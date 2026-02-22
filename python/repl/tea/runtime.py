"""
repl/tea/runtime.py

ReplRuntime: owns the prompt_toolkit event loop and executes Cmd side effects.
This is the only file allowed to do I/O, call gRPC, or touch prompt_toolkit.
"""

from __future__ import annotations

import json
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
from prompt_toolkit.validation import ValidationError

from repl.completer import HeaderAwareCompleter, ReplCompleter
from repl.schema.builder import UISpecBuilder
from repl.schema.dsl import coerce_value, expand_variables, proto_type_label
from repl.schema.parser import CmdParser
from repl.schema.projection import project_candidates
from repl.schema.ui_spec import (
    CompletionCache,
    CompletionSource,
    FieldSpec,
    REFLECTION_SERVICE,
    UISpec,
)
from repl.tea.model import Model, ReplMode, WizardState, initial_model
from repl.tea.msg import (
    BootstrapDone,
    CompletionLoaded,
    RpcError,
    RpcSuccess,
    StreamChunk,
    StreamEnd,
    WizardAborted,
    WizardFieldDone,
)
from repl.tea.update import (
    CancelActiveStream,
    ExitRepl,
    FetchCompletion,
    InvokeRpc,
    RenderError,
    RenderHeaderList,
    RenderHelp,
    RenderInfo,
    RenderResponse,
    RenderStreamChunk,
    RenderStreamEnd,
    SetVariable,
    StartWizard,
    WriteHistory,
    update,
)
from repl.view.response import (
    view_banner,
    view_error,
    view_help,
    view_prompt_tokens,
    view_response,
    view_stream_chunk,
    view_stream_header,
)
from repl.view.style import build_key_bindings, repl_style


class ReplRuntime:
    def __init__(
        self,
        client: Any,
        server: str,
        initial_service: Optional[str] = None,
    ) -> None:
        self._client = client
        self._server = server
        self._cache = CompletionCache()
        self._exit_flag = False
        self._stream_cancel = threading.Event()

        print("  🔍  Discovering services …")
        ui_spec = UISpecBuilder().build(client)

        self._model = initial_model(
            server=server,
            ui_spec=ui_spec,
        )

        # Give the completer a live view of the namespace via a lambda so it
        # always reads the current model state without holding a stale copy.
        inner = ReplCompleter(
            ui_spec,
            self._cache,
            namespace_fn=lambda: self._model.namespace,
            live_fetch_fn=self._live_fetch_sync,
        )
        completer = HeaderAwareCompleter(inner)
        self._inner_completer = inner

        self._session = PromptSession(
            completer=completer,
            history=FileHistory(str(Path.home() / ".grpc_repl_history")),
            auto_suggest=AutoSuggestFromHistory(),
            style=repl_style,
            key_bindings=build_key_bindings(
                completer,
                is_streaming_fn=lambda: self._model.stream_active,
                cancel_fn=self._stream_cancel.set,
            ),
            bottom_toolbar=self._bottom_toolbar,
            refresh_interval=0.5,
            multiline=False,
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
                        view_prompt_tokens(
                            self._server,
                            namespace=self._model.namespace,
                            wizard=self._model.mode == ReplMode.WIZARD,
                        )
                    )
                )
                if text and text.strip():
                    self._dispatch_text(text.strip())

            except KeyboardInterrupt:
                if self._model.stream_active:
                    self._stream_cancel.set()
                else:
                    print()
            except EOFError:
                print("\n  Bye.")
                break

    # ──────────────────────────────────────────────────────────────────────
    # Bootstrap
    # ──────────────────────────────────────────────────────────────────────

    def _bootstrap(self) -> None:
        """
        1. Call every bootstrap RPC synchronously.
        2. For any CompletionSource whose source_rpc matches the bootstrap
           node, fill the cache from the result we already have rather than
           making a second round-trip.
        3. Fetch any remaining completion sources that weren't covered above.
        """
        # Index source_rpc → CompletionSource for quick lookup
        all_sources = self._model.ui_spec.all_completion_sources()
        sources_by_rpc: dict[str, CompletionSource] = {
            cs.source_rpc: cs for cs in all_sources
        }

        for node in self._model.ui_spec.bootstrap_nodes():
            try:
                result = self._client.invoke_rpc(
                    f"{node.service}/{node.method}",
                    "{}",
                )
                # Build the FQN that other commands would reference as source_rpc
                # e.g.  "kv.v1.KVService/ListKeys" → "kv.v1.KVService.ListKeys"
                fqn = f"{node.service}.{node.method}"
                if fqn in sources_by_rpc:
                    cs = sources_by_rpc[fqn]
                    self._fill_cache_from_result(cs, result)
            except Exception as exc:
                print(view_error(f"Bootstrap {node.method}: {exc}"))

        # Fetch any completion sources not yet populated
        for cs in all_sources:
            if not self._cache.has(cs.source_rpc):
                self._fetch_completion(cs)

        self._dispatch(BootstrapDone())

    # ──────────────────────────────────────────────────────────────────────
    # Dispatch
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

        if isinstance(cmd, InvokeRpc):
            if cmd.streaming:
                self._invoke_streaming(cmd)
            else:
                self._invoke_unary(cmd)
            return

        if isinstance(cmd, FetchCompletion):
            cs = self._model.ui_spec.completion_source_by_rpc(cmd.source_rpc)
            if cs:
                threading.Thread(
                    target=self._fetch_completion, args=(cs,), daemon=True
                ).start()
            return

        if isinstance(cmd, RenderResponse):
            node = self._model.ui_spec.find_cmd(cmd.cmd_path)
            print(view_response(node, cmd.result))
            return

        if isinstance(cmd, RenderStreamChunk):
            node = self._model.ui_spec.find_cmd(cmd.cmd_path)
            with patch_stdout():
                print(view_stream_chunk(node, cmd.chunk, cmd.index))
            return

        if isinstance(cmd, RenderStreamEnd):
            with patch_stdout():
                print(f"\n  ⏹  Stream ended — {cmd.total} message(s).")
            return

        if isinstance(cmd, RenderError):
            print(view_error(cmd.message))
            return

        if isinstance(cmd, RenderInfo):
            print(cmd.message)
            return

        if isinstance(cmd, RenderHelp):
            # Scope help to the given prefix; fall back to full list
            print(view_help(self._model.ui_spec, cmd.prefix))
            return

        if isinstance(cmd, RenderHeaderList):
            if not cmd.headers:
                print("  (no headers set)")
            else:
                for k, v in cmd.headers:
                    print(f"  {k}: {v}")
            return

        if isinstance(cmd, StartWizard):
            self._run_wizard(cmd.cmd_path)
            return

        if isinstance(cmd, SetVariable):
            print(f"  ✔  ${cmd.name} = {cmd.value}")
            return

        if isinstance(cmd, WriteHistory):
            return  # FileHistory handles this

        if isinstance(cmd, ExitRepl):
            print("  Bye.")
            self._exit_flag = True
            return

        if isinstance(cmd, CancelActiveStream):
            self._stream_cancel.set()
            return

    # ──────────────────────────────────────────────────────────────────────
    # gRPC — reflection intercept
    # ──────────────────────────────────────────────────────────────────────

    def _invoke_unary(self, cmd: InvokeRpc) -> None:
        if cmd.service == REFLECTION_SERVICE:
            self._invoke_reflection(cmd)
            return
        try:
            result = self._client.invoke_rpc(
                f"{cmd.service}/{cmd.method}",
                json.dumps(cmd.request),
            )
            self._dispatch(RpcSuccess(cmd_path=cmd.cmd_path, result=result))
        except grpc.RpcError as e:
            self._dispatch(
                RpcError(cmd_path=cmd.cmd_path, code=e.code(), detail=e.details())
            )
        except Exception as e:
            self._dispatch(
                RpcError(
                    cmd_path=cmd.cmd_path,
                    code=grpc.StatusCode.INTERNAL,
                    detail=str(e),
                )
            )

    def _invoke_reflection(self, cmd: InvokeRpc) -> None:
        try:
            if cmd.method == "list_services":
                raw = self._client.list_services()
                result = [{"name": svc} for svc in raw]
                self._dispatch(RpcSuccess(cmd_path=cmd.cmd_path, result=result))
            else:
                self._dispatch(
                    RpcError(
                        cmd_path=cmd.cmd_path,
                        code=grpc.StatusCode.UNIMPLEMENTED,
                        detail=f"Unknown reflection command: {cmd.method}",
                    )
                )
        except Exception as exc:
            self._dispatch(
                RpcError(
                    cmd_path=cmd.cmd_path,
                    code=grpc.StatusCode.INTERNAL,
                    detail=str(exc),
                )
            )

    def _invoke_streaming(self, cmd: InvokeRpc) -> None:
        self._stream_cancel.clear()
        node = self._model.ui_spec.find_cmd(cmd.cmd_path)

        def _run() -> None:
            count = 0
            with patch_stdout():
                print(f"\n  📡  Streaming  (Ctrl-C to stop)\n")
                hdr = view_stream_header(node)
                if hdr:
                    print(hdr)
                    print("  " + "─" * 60)
            try:
                for chunk in self._client.invoke_rpc_streaming(
                    f"{cmd.service}/{cmd.method}",
                    json.dumps(cmd.request),
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
        threading.Thread(target=_run, daemon=True).start()

    # ──────────────────────────────────────────────────────────────────────
    # Completion
    # ──────────────────────────────────────────────────────────────────────

    def _fetch_completion(self, cs: CompletionSource) -> None:
        """
        Invoke the source RPC and populate the completion cache.

        Path conversion: "pkg.Svc.Method" → "pkg.Svc/Method"
        Using rsplit(".", 1) is correct regardless of package depth.
        """
        try:
            svc, method = cs.source_rpc.rsplit(".", 1)
            req_json = expand_variables(cs.source_request, self._model.variables_dict())
            result = self._client.invoke_rpc(
                f"{svc}/{method}",
                req_json,
            )
            self._fill_cache_from_result(cs, result)
        except Exception:
            pass  # completion fetch failure is non-fatal

    def _field_specs_for_source(
        self, svc_name: str, method_name: str, source_field: str
    ) -> dict[str, Any]:
        """
        Build field_specs for the ELEMENT type of source_field.

        Example:
          ListKeysResponse  { repeated ConfigEntry keys = 1 }
          source_field = "keys"
          → ConfigEntry descriptor → FieldSpec for key, value

          ListUsersResponse { repeated User users = 1 }
          source_field = "users"
          → User descriptor → FieldSpec for id, username, role, active, ...

          ListKeysResponse  { repeated string keys = 1 }  (scalar)
          source_field = "keys"
          → no message_type → fall back to ListKeysResponse top-level fields
        """
        field_specs: dict[str, Any] = {}
        try:
            svc_desc = self._client.pool.FindServiceByName(svc_name)
            m_desc = svc_desc.FindMethodByName(method_name)
            bld = UISpecBuilder()

            # Walk the dot-path of source_field through the output type to find
            # the field descriptor whose message_type is the element type.
            desc = m_desc.output_type
            for part in source_field.split("."):
                f_desc = desc.fields_by_name.get(part) if desc else None
                if f_desc is None:
                    break
                if f_desc.message_type:
                    desc = f_desc.message_type
                else:
                    # Scalar repeated field (e.g. repeated string) — no element
                    # message type; projection handles scalars via is_scalar branch.
                    desc = None
                    break

            # desc is now the element message descriptor (or None for scalars)
            if desc and desc != m_desc.output_type:
                for f in bld._build_fields(self._client, desc):
                    field_specs[f.name] = f
            else:
                # Fall back to top-level output type fields
                for f in bld._build_fields(self._client, m_desc.output_type):
                    field_specs[f.name] = f
        except Exception:
            pass
        return field_specs

    def _fill_cache_from_result(self, cs: CompletionSource, result: Any) -> None:
        try:
            svc_name, method_name = cs.source_rpc.rsplit(".", 1)
        except ValueError:
            return

        field_specs = self._field_specs_for_source(
            svc_name, method_name, cs.source_field
        )

        rows = project_candidates(result, cs, field_specs)

        self._cache.store(cs.source_rpc, rows)
        self._dispatch(CompletionLoaded(source_rpc=cs.source_rpc))

    def _live_fetch_sync(
        self,
        cs: CompletionSource,
        extra_vars: dict[str, str] | None = None,
    ) -> None:
        """
        Synchronous live completion fetch called from the completer on Tab.

        Expands ${VAR} in source_request using:
          1. REPL variables (set ENV prod)
          2. extra_vars — flag values already typed in the buffer
             e.g. --env dev → {"env": "dev", "ENV": "dev"}
        """
        try:
            svc, method = cs.source_rpc.rsplit(".", 1)
            vars_merged = {**self._model.variables_dict(), **(extra_vars or {})}
            req_json = expand_variables(cs.source_request, vars_merged)
            result = self._client.invoke_rpc(
                f"{svc}/{method}",
                req_json,
            )
            field_specs = self._field_specs_for_source(svc, method, cs.source_field)
            rows = project_candidates(result, cs, field_specs)
            self._cache.store(cs.source_rpc, rows)
        except Exception:
            pass

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

        ws = WizardState(node=node, fields_pending=tuple(visible), collected=())
        self._model = replace(self._model, mode=ReplMode.WIZARD, wizard_state=ws)
        print(f"  ✏️  Entering wizard for '{cmd_path}' — Ctrl-C to abort\n")

        for f in visible:
            value = self._wizard_prompt_field(f, node)
            if value is None:
                self._dispatch(WizardAborted())
                print("  ↩  Wizard aborted.")
                return
            self._dispatch(WizardFieldDone(field_name=f.name, value=value))

    def _wizard_prompt_field(self, field: FieldSpec, node: Any) -> Optional[str]:
        from prompt_toolkit.completion import WordCompleter

        label = field.display_label()
        type_str = proto_type_label(field)

        comp_src = node.completion_for_field(field.name)
        if field.enum_values:
            mini_completer = WordCompleter(list(field.enum_values), ignore_case=True)
        elif field.proto_type == 8:
            mini_completer = WordCompleter(["true", "false"])
        elif comp_src and self._cache.has(comp_src.source_rpc):
            mini_completer = WordCompleter(
                [r.insert_value for r in self._cache.get(comp_src.source_rpc)]
            )
        else:
            mini_completer = None

        prompt_str = FormattedText(
            [
                ("class:wizard-field", f"  {label}"),
                ("class:wizard-type", f" [{type_str}]"),
                ("", ": "),
            ]
        )
        try:
            return PromptSession(
                completer=mini_completer, style=repl_style, validate_while_typing=False
            ).prompt(prompt_str)
        except (KeyboardInterrupt, EOFError):
            return None

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────
    def _bottom_toolbar(self) -> FormattedText:
        if self._model.stream_active:
            return FormattedText(
                [("class:bottom-toolbar.text", " 📡 streaming  Ctrl-C to stop  ")]
            )
        if self._model.error:
            return FormattedText([("class:error", f" ❌ {self._model.error[:60]} ")])
        ns = self._model.namespace_prefix()
        ns_s = f"  ns: {ns}" if ns else ""
        return FormattedText(
            [
                (
                    "class:bottom-toolbar.text",
                    f" {self._server}  "
                    f"{len(self._model.ui_spec.services)} service(s)  "
                    f"{len(self._model.ui_spec.user_commands())} commands"
                    f"{ns_s}  F1=help ",
                )
            ]
        )
