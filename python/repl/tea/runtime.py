"""
repl/tea/runtime.py

ReplRuntime: owns the prompt_toolkit event loop and executes Cmd side effects.
This is the only file allowed to do I/O, call gRPC, or touch prompt_toolkit.
"""
from __future__ import annotations

import json
import sys
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, Optional

import grpc
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.validation import ValidationError, Validator

from google.protobuf.json_format import MessageToDict

from repl.completer import HeaderAwareCompleter, ReplCompleter
from repl.schema.builder import UISpecBuilder
from repl.schema.dsl import coerce_value, expand_variables, proto_type_label
from repl.schema.parser import CmdParser
from repl.schema.projection import project_candidates
from repl.schema.ui_spec import (
    CompletionCache, CompletionSource, FieldSpec, REFLECTION_SERVICE, UISpec,
)
from repl.tea.model import Model, ReplMode, WizardState, initial_model
from repl.tea.msg import (
    BootstrapDone, CompletionLoaded, RpcError, RpcSuccess,
    StreamChunk, StreamEnd, WizardAborted, WizardFieldDone,
)
from repl.tea.update import (
    ExitRepl, FetchCompletion, InvokeRpc, RenderError, RenderHeaderList,
    RenderHelp, RenderResponse, RenderStreamChunk, RenderStreamEnd,
    SetVariable, StartWizard, WriteHistory, update,
)
from repl.view.response import (
    view_banner, view_error, view_help, view_prompt_tokens,
    view_response, view_stream_chunk, view_stream_header,
)
from repl.view.style import build_key_bindings, repl_style


class ReplRuntime:
    """
    Outer shell of the TEA loop.
    Responsibilities (only this class):
      - Run prompt_toolkit event loop
      - Execute Cmd side effects (gRPC calls, file I/O)
      - Feed results back as Msg via _dispatch
    """

    def __init__(
        self,
        client: Any,               # GrpcReflectionClient
        server: str,
        auth_header: Optional[str] = None,
        initial_service: Optional[str] = None,
    ) -> None:
        self._client     = client
        self._server     = server
        self._cache      = CompletionCache()
        self._exit_flag  = False
        self._stream_cancel = threading.Event()

        print("  🔍  Discovering services …")
        ui_spec = UISpecBuilder().build(client)

        self._model = initial_model(
            server=server,
            ui_spec=ui_spec,
            auth_header=auth_header,
        )

        inner    = ReplCompleter(ui_spec, self._cache)
        completer = HeaderAwareCompleter(inner)
        self._inner_completer = inner

        self._session = PromptSession(
            completer=completer,
            history=FileHistory(str(Path.home() / ".grpc_repl_history")),
            auto_suggest=AutoSuggestFromHistory(),
            style=repl_style,
            key_bindings=build_key_bindings(completer),
            bottom_toolbar=self._bottom_toolbar,
            refresh_interval=0.5,
            multiline=False,
            # Tab-only completions: don't pop the menu on every keystroke.
            # This also ensures Tab on an empty buffer triggers the full
            # top-level command list rather than doing nothing.
            complete_while_typing=False,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Public
    # ──────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        n_cmds = len(self._model.ui_spec.user_commands())
        print(view_banner(self._server, n_cmds))
        print()
        self._bootstrap()

        while not self._exit_flag:
            try:
                text = self._session.prompt(
                    lambda: FormattedText(
                        view_prompt_tokens(self._server, self._prompt_mode())
                    )
                )
                if text and text.strip():
                    self._dispatch_text(text.strip())

            except KeyboardInterrupt:
                if self._model.stream_active:
                    self._stream_cancel.set()
                else:
                    print()  # clear line
            except EOFError:
                print("\n  Bye.")
                break

    # ──────────────────────────────────────────────────────────────────────
    # Bootstrap
    # ──────────────────────────────────────────────────────────────────────

    def _bootstrap(self) -> None:
        for node in self._model.ui_spec.bootstrap_nodes():
            try:
                req_json = "{}"
                result   = self._client.invoke_rpc(
                    f"{node.service}/{node.method}",
                    req_json,
                    self._model.auth_header,
                )
                # Seed completion caches from bootstrap results
                for cs in node.completions:
                    if cs.source_rpc.split("/")[-1] == node.method or \
                       cs.source_rpc.split(".")[-1] == node.method:
                        self._fill_cache_from_result(cs, result, node)
            except Exception as exc:
                print(view_error(f"Bootstrap failed for {node.cmd}: {exc}"))
            finally:
                print(f"bootstrapped {node.cmd}")

        # Also pre-fetch all completion sources that have no live flag
        for cs in self._model.ui_spec.all_completion_sources():
            if not self._cache.has(cs.source_rpc) and not cs.live:
                self._fetch_completion(cs)

        self._dispatch(BootstrapDone())

    # ──────────────────────────────────────────────────────────────────────
    # Dispatch loop
    # ──────────────────────────────────────────────────────────────────────

    def _dispatch_text(self, text: str) -> None:
        from repl.tea.msg import UserInput
        self._dispatch(UserInput(text=text))

    def _dispatch(self, msg: Any) -> None:
        new_model, cmds = update(self._model, msg)
        self._model = new_model
        for cmd in cmds:
            self._execute(cmd)

    # ──────────────────────────────────────────────────────────────────────
    # Cmd executor
    # ──────────────────────────────────────────────────────────────────────

    def _execute(self, cmd: Any) -> None:
        # ── RPC ─────────────────────────────────────────────────────────────
        if isinstance(cmd, InvokeRpc):
            if cmd.streaming:
                self._invoke_streaming(cmd)
            else:
                self._invoke_unary(cmd)
            return

        # ── Completion fetch ─────────────────────────────────────────────────
        if isinstance(cmd, FetchCompletion):
            cs = self._model.ui_spec.completion_source_by_rpc(cmd.source_rpc)
            if cs:
                threading.Thread(
                    target=self._fetch_completion,
                    args=(cs,),
                    daemon=True,
                ).start()
            return

        # ── Render response ──────────────────────────────────────────────────
        if isinstance(cmd, RenderResponse):
            node   = self._model.ui_spec.find_cmd(cmd.cmd_path)
            output = view_response(node, cmd.result)
            print(output)
            return

        # ── Stream chunk / end ───────────────────────────────────────────────
        if isinstance(cmd, RenderStreamChunk):
            node   = self._model.ui_spec.find_cmd(cmd.cmd_path)
            output = view_stream_chunk(node, cmd.chunk, cmd.index)
            with patch_stdout():
                print(output)
            return

        if isinstance(cmd, RenderStreamEnd):
            with patch_stdout():
                print(f"\n  ⏹  Stream ended — {cmd.total} message(s).")
            return

        # ── Error ────────────────────────────────────────────────────────────
        if isinstance(cmd, RenderError):
            print(view_error(cmd.message))
            return

        # ── Help ─────────────────────────────────────────────────────────────
        if isinstance(cmd, RenderHelp):
            print(view_help(self._model.ui_spec, cmd.prefix))
            return

        # ── Header list ──────────────────────────────────────────────────────
        if isinstance(cmd, RenderHeaderList):
            if not cmd.headers:
                print("  (no headers set)")
            else:
                for k, v in cmd.headers:
                    print(f"  {k}: {v}")
            return

        # ── Wizard ───────────────────────────────────────────────────────────
        if isinstance(cmd, StartWizard):
            self._run_wizard(cmd.cmd_path)
            return

        # ── Variable ────────────────────────────────────────────────────────
        if isinstance(cmd, SetVariable):
            print(f"  ✔  ${cmd.name} = {cmd.value}")
            return

        # ── Write history ────────────────────────────────────────────────────
        if isinstance(cmd, WriteHistory):
            return  # prompt_toolkit handles history via FileHistory

        # ── Exit ─────────────────────────────────────────────────────────────
        if isinstance(cmd, ExitRepl):
            print("  Bye.")
            self._exit_flag = True
            return

    # ──────────────────────────────────────────────────────────────────────
    # gRPC invocation
    # ──────────────────────────────────────────────────────────────────────

    def _invoke_unary(self, cmd: InvokeRpc) -> None:
        # ── Reflection-backed commands — never touch the wire RPC ────────
        if cmd.service == REFLECTION_SERVICE:
            self._invoke_reflection(cmd)
            return

        try:
            all_headers = dict(self._model.headers)
            if self._model.auth_header:
                all_headers.setdefault("authorization", self._model.auth_header)

            result = self._client.invoke_rpc(
                f"{cmd.service}/{cmd.method}",
                json.dumps(cmd.request),
                next(
                    (v for k, v in all_headers.items()
                     if k.lower() == "authorization"),
                    None,
                ),
            )
            self._dispatch(RpcSuccess(cmd_path=cmd.cmd_path, result=result))

        except grpc.RpcError as e:
            self._dispatch(RpcError(
                cmd_path=cmd.cmd_path, code=e.code(), detail=e.details()
            ))
        except Exception as e:
            self._dispatch(RpcError(
                cmd_path=cmd.cmd_path,
                code=grpc.StatusCode.INTERNAL,
                detail=str(e),
            ))

    def _invoke_reflection(self, cmd: InvokeRpc) -> None:
        """
        Handle commands backed by gRPC reflection rather than a real RPC.
        Currently only 'list_services' is registered this way.
        The result is shaped to match what the view layer expects:
        a dict with a repeated field whose name matches the CmdNode's
        output display columns.
        """
        try:
            if cmd.method == "list_services":
                raw      = self._client.list_services()
                # Shape: {"name": [...]} — single column table
                # Wrap each string as a row dict so render_table works uniformly
                result   = [{"name": svc} for svc in raw]
                self._dispatch(RpcSuccess(cmd_path=cmd.cmd_path, result=result))
            else:
                self._dispatch(RpcError(
                    cmd_path=cmd.cmd_path,
                    code=grpc.StatusCode.UNIMPLEMENTED,
                    detail=f"Unknown reflection command: {cmd.method}",
                ))
        except Exception as exc:
            self._dispatch(RpcError(
                cmd_path=cmd.cmd_path,
                code=grpc.StatusCode.INTERNAL,
                detail=str(exc),
            ))

    def _invoke_streaming(self, cmd: InvokeRpc) -> None:
        """Run streaming RPC in a background thread, print with patch_stdout."""
        self._stream_cancel.clear()

        node = self._model.ui_spec.find_cmd(cmd.cmd_path)

        def _run() -> None:
            auth = next(
                (v for k, v in self._model.headers_dict().items()
                 if k.lower() == "authorization"),
                self._model.auth_header,
            )

            with patch_stdout():
                print(f"\n  📡  Streaming  (Ctrl-C to stop)\n")
                # Print stream header if available
                header = view_stream_header(node)
                if header:
                    print(header)
                    print("  " + "─" * 60)

            count = 0
            try:
                for chunk in self._client.invoke_rpc_streaming(
                    f"{cmd.service}/{cmd.method}",
                    json.dumps(cmd.request),
                    auth,
                ):
                    if self._stream_cancel.is_set():
                        break
                    count += 1
                    self._dispatch(
                        StreamChunk(cmd_path=cmd.cmd_path, chunk=chunk, index=count)
                    )
                self._dispatch(StreamEnd(cmd_path=cmd.cmd_path, total=count))

            except grpc.RpcError as e:
                if e.code() not in (
                    grpc.StatusCode.CANCELLED,
                    grpc.StatusCode.DEADLINE_EXCEEDED,
                ):
                    with patch_stdout():
                        print(view_error(f"Stream error: {e.code()} — {e.details()}"))
                self._dispatch(StreamEnd(cmd_path=cmd.cmd_path, total=count))

        self._model = replace(self._model, stream_active=True)
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

    # ──────────────────────────────────────────────────────────────────────
    # Completion fetch
    # ──────────────────────────────────────────────────────────────────────

    def _fetch_completion(self, cs: CompletionSource) -> None:
        try:
            req_json = expand_variables(cs.source_request, self._model.variables_dict())
            result   = self._client.invoke_rpc(
                cs.source_rpc.replace(".", "/", cs.source_rpc.count(".") - 1),
                req_json,
                self._model.auth_header,
            )
            self._fill_cache_from_result(cs, result, node=None)
        except Exception as exc:
            pass  # completion fetch failure is non-fatal

    def _fill_cache_from_result(
        self, cs: CompletionSource, result: Any, node: Any
    ) -> None:
        # Build field_specs for the source RPC's output (best-effort)
        field_specs: dict[str, Any] = {}
        try:
            src_parts = cs.source_rpc.rsplit(".", 1)
            if len(src_parts) == 2:
                src_svc, src_method = src_parts
                svc_desc = self._client.pool.FindServiceByName(src_svc)
                m_desc   = svc_desc.FindMethodByName(src_method)
                from repl.schema.builder import UISpecBuilder
                bld = UISpecBuilder()
                for f in bld._build_fields(self._client, m_desc.output_type):
                    field_specs[f.name] = f
        except Exception:
            pass

        rows = project_candidates(result, cs, field_specs)
        self._cache.store(cs.source_rpc, rows)
        self._dispatch(CompletionLoaded(source_rpc=cs.source_rpc))

    # ──────────────────────────────────────────────────────────────────────
    # Wizard
    # ──────────────────────────────────────────────────────────────────────

    def _run_wizard(self, cmd_path: str) -> None:
        node = self._model.ui_spec.find_cmd(cmd_path)
        if node is None:
            return

        visible = [f for f in node.input_fields if not f.hidden]
        if not visible:
            self._dispatch_text(cmd_path)
            return

        ws = WizardState(
            node=node,
            fields_pending=tuple(visible),
            collected=(),
        )
        self._model = replace(self._model, mode=ReplMode.WIZARD, wizard_state=ws)

        print(f"  ✏️  Entering wizard for '{cmd_path}' — press Ctrl-C to abort\n")

        for field in visible:
            value = self._wizard_prompt_field(field, node)
            if value is None:
                self._dispatch(WizardAborted())
                print("  ↩  Wizard aborted.")
                return
            self._dispatch(WizardFieldDone(field_name=field.name, value=value))

    def _wizard_prompt_field(
        self, field: FieldSpec, node: Any
    ) -> Optional[str]:
        """Prompt for a single wizard field with inline completion."""
        from prompt_toolkit.completion import WordCompleter
        from prompt_toolkit.validation import Validator

        label    = field.display_label()
        type_str = proto_type_label(field)

        # Build a mini completer for this field
        comp_src = node.completion_for_field(field.name)
        if field.enum_values:
            mini_completer = WordCompleter(list(field.enum_values), ignore_case=True)
        elif field.proto_type == 8:  # BOOL
            mini_completer = WordCompleter(["true", "false"])
        elif comp_src and self._cache.has(comp_src.source_rpc):
            candidates = [r.insert_value for r in self._cache.get(comp_src.source_rpc)]
            mini_completer = WordCompleter(candidates)
        else:
            mini_completer = None

        # Validator
        def _validate(text: str) -> None:
            if not text.strip():
                return  # allow empty (optional fields)
            try:
                coerce_value(text.strip(), field)
            except (ValueError, TypeError, Exception) as e:
                raise ValidationError(cursor_position=len(text), message=str(e))

        validator = Validator.from_callable(
            lambda t: True,
            error_message="",
        )

        prompt_str = FormattedText([
            ("class:wizard-field", f"  {label}"),
            ("class:wizard-type",  f" [{type_str}]"),
            ("",                   ": "),
        ])

        try:
            mini_session = PromptSession(
                completer=mini_completer,
                style=repl_style,
                complete_while_typing=True,
                validate_while_typing=False,
            )
            value = mini_session.prompt(prompt_str)
            return value
        except KeyboardInterrupt:
            return None
        except EOFError:
            return None

    # ──────────────────────────────────────────────────────────────────────
    # Prompt accessories
    # ──────────────────────────────────────────────────────────────────────

    def _prompt_mode(self) -> str:
        if self._model.mode == ReplMode.WIZARD:
            return "wizard"
        return ""

    def _bottom_toolbar(self) -> FormattedText:
        parts: list[tuple[str, str]] = []
        if self._model.stream_active:
            parts.append(("class:bottom-toolbar.text", " 📡 streaming  Ctrl-C to stop  "))
        elif self._model.error:
            err = self._model.error[:60]
            parts.append(("class:error", f" ❌ {err} "))
        else:
            svc_count = len(self._model.ui_spec.services)
            cmd_count = len(self._model.ui_spec.user_commands())
            parts.append((
                "class:bottom-toolbar.text",
                f" {self._server}  {svc_count} service(s)  {cmd_count} commands  F1=help ",
            ))
        return FormattedText(parts)