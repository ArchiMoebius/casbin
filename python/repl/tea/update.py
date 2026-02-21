"""
repl/tea/update.py

Pure update function: (Model, Msg) → (Model, list[Cmd])
No I/O. No grpc calls. No rendering. 100% unit-testable.
"""

from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import replace, dataclass
from typing import Any

import grpc

from repl.schema.dsl import coerce_value, expand_request, expand_template
from repl.schema.parser import CmdParser, namespace_to_request
from repl.schema.ui_spec import CmdNode, FieldSpec, UISpec
from repl.tea.model import Model, ReplMode, RpcResult, WizardState
from repl.tea.msg import (
    BootstrapDone,
    CancelStream,
    CompletionLoaded,
    Interrupt,
    Msg,
    RpcError,
    RpcSuccess,
    StreamChunk,
    StreamEnd,
    UserInput,
    WizardAborted,
    WizardFieldDone,
)
from repl.tea.msg import (
    CancelActiveStream,
    ExitRepl,
    FetchCompletion,
    InvokeRpc,
    SetVariable,
    StartWizard,
    WriteHistory,
)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────


def update(model: Model, msg: Msg) -> tuple[Model, list]:
    if isinstance(msg, UserInput):
        return _handle_user_input(model, msg.text)
    if isinstance(msg, WizardFieldDone):
        return _handle_wizard_field(model, msg.field_name, msg.value)
    if isinstance(msg, WizardAborted):
        return replace(model, mode=ReplMode.NORMAL, wizard_state=None, error=None), []
    if isinstance(msg, RpcSuccess):
        return _handle_rpc_success(model, msg)
    if isinstance(msg, (StreamChunk, StreamEnd)):
        return _handle_stream(model, msg)
    if isinstance(msg, RpcError):
        return _handle_rpc_error(model, msg)
    if isinstance(msg, CompletionLoaded):
        return model, []
    if isinstance(msg, BootstrapDone):
        return replace(model, connected=True), []
    if isinstance(msg, (Interrupt, CancelStream)):
        if model.stream_active:
            return replace(model, stream_active=False), [CancelActiveStream()]
        return model, []
    return model, []


# ──────────────────────────────────────────────────────────────────────────────
# UserInput dispatch
# ──────────────────────────────────────────────────────────────────────────────


def _handle_user_input(model: Model, raw: str) -> tuple[Model, list]:
    text = raw.strip()
    if not text:
        return model, []

    # ── Built-ins ────────────────────────────────────────────────────────────
    if text in ("exit", "quit", "q"):
        return model, [ExitRepl()]

    if text in ("..", "back", "unuse"):
        return _handle_pop_namespace(model)

    if text.startswith("use ") or text == "use":
        return _handle_use(model, text[4:].strip() if text.startswith("use ") else "")

    if text.startswith("set "):
        return _handle_set(model, text[4:].strip())

    if text.startswith("header "):
        return _handle_header(model, text[7:].strip())

    if text in ("help", "?") or text.startswith("help ") or text.startswith("? "):
        return model, [_help_cmd(text, model.namespace_prefix())]

    # ── Resolve command — namespace-aware ────────────────────────────────────
    try:
        words = shlex.split(text)
    except ValueError as exc:
        return _error(model, f"Parse error: {exc}")

    if not words:
        return model, []

    cmd_word = words[0]

    # Resolve: try absolute first, then namespace-prefixed
    canonical = model.resolve_cmd(cmd_word)

    if canonical is None:
        # Give a helpful suggestion
        prefix = model.namespace_prefix()
        subtree_key = (prefix + "." + cmd_word) if prefix else cmd_word
        suggestions = model.ui_spec.subtree(subtree_key)
        if not suggestions and prefix:
            suggestions = model.ui_spec.subtree(cmd_word)
        if suggestions:
            names = ", ".join(n.cmd for n in suggestions[:4])
            return _error(
                model, f"Unknown command '{cmd_word}'. Did you mean: {names}?"
            )
        return _error(
            model, f"Unknown command: '{cmd_word}'. Type 'help' for available commands."
        )

    node = model.ui_spec.find_cmd(canonical)

    # ── Parse flags ───────────────────────────────────────────────────────────
    arg_words = words[1:]

    if not arg_words and any(not f.hidden for f in node.input_fields):
        new_model = replace(model, history=model.history + (text,), error=None)
        return new_model, [WriteHistory(text), StartWizard(node.cmd)]

    parser = CmdParser.instance().build(node)
    try:
        namespace = parser.parse_args(arg_words)
    except (argparse.ArgumentError, SystemExit) as exc:
        return _error(model, str(exc))

    request = namespace_to_request(namespace, node)
    request = expand_request(request, model.variables_dict())

    new_model = replace(model, history=model.history + (text,), error=None)
    return new_model, [
        WriteHistory(text),
        InvokeRpc(
            service=node.service,
            method=node.method,
            request=request,
            streaming=node.server_streaming,
            cmd_path=node.cmd,
        ),
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Namespace: use / ..
# ──────────────────────────────────────────────────────────────────────────────


def _handle_use(model: Model, segment: str) -> tuple[Model, list]:
    if not segment:
        # "use" with no argument → show current namespace or go to root
        prefix = model.namespace_prefix()
        msg = f"  namespace: {prefix!r}" if prefix else "  namespace: (root)"
        return model, [RenderInfo(message=msg)]

    # Validate: there must be at least one command under the new prefix
    current_prefix = model.namespace_prefix()
    new_prefix = (current_prefix + "." + segment) if current_prefix else segment
    cmds_under = model.ui_spec.subtree(new_prefix)

    if not cmds_under:
        # Also try the segment as an absolute path
        cmds_under = model.ui_spec.subtree(segment)
        if cmds_under:
            # Treat as absolute jump
            new_prefix = segment
            # Rebuild namespace tuple from absolute path
            new_namespace = tuple(new_prefix.split("."))
        else:
            return _error(
                model,
                f"No commands under '{new_prefix}'. Available: "
                + ", ".join(
                    sorted({c.cmd.split(".")[0] for c in model.ui_spec.user_commands()})
                ),
            )
    else:
        new_namespace = tuple(new_prefix.split("."))

    new_model = replace(model, namespace=new_namespace, error=None)
    return new_model, [RenderInfo(message=f"  → {new_prefix}")]


def _handle_pop_namespace(model: Model) -> tuple[Model, list]:
    if not model.namespace:
        return model, [RenderInfo(message="  already at root")]
    new_model = model.pop_namespace()
    prefix = new_model.namespace_prefix()
    label = prefix if prefix else "(root)"
    return new_model, [RenderInfo(message=f"  ← {label}")]


# ──────────────────────────────────────────────────────────────────────────────
# RPC result handlers
# ──────────────────────────────────────────────────────────────────────────────


def _handle_rpc_success(model: Model, msg: RpcSuccess) -> tuple[Model, list]:
    node = model.ui_spec.find_cmd(msg.cmd_path)
    new_model = replace(
        model,
        last_response=RpcResult(msg.cmd_path, msg.result),
        error=None,
        stream_active=False,
    )
    cmds: list = []

    if node and node.refresh_after_mutation:
        for cs in model.ui_spec.all_completion_sources():
            cmds.append(
                FetchCompletion(
                    source_rpc=cs.source_rpc,
                    request=json.loads(cs.source_request),
                    live=True,
                )
            )

    cmds.append(_render_response_cmd(msg.cmd_path, msg.result, success=True))
    return new_model, cmds


def _handle_stream(model: Model, msg: StreamChunk | StreamEnd) -> tuple[Model, list]:
    if isinstance(msg, StreamEnd):
        return replace(model, stream_active=False), [
            _stream_end_cmd(msg.cmd_path, msg.total)
        ]
    return replace(model, stream_active=True), [
        _stream_chunk_cmd(msg.cmd_path, msg.chunk, msg.index)
    ]


def _handle_rpc_error(model: Model, msg: RpcError) -> tuple[Model, list]:
    detail = f"{msg.code.name}: {msg.detail}"
    return replace(model, error=detail, stream_active=False), [_error_cmd(detail)]


# ──────────────────────────────────────────────────────────────────────────────
# Wizard
# ──────────────────────────────────────────────────────────────────────────────


def _handle_wizard_field(
    model: Model, field_name: str, value: str
) -> tuple[Model, list]:
    ws = model.wizard_state
    if ws is None:
        return model, []

    new_collected = ws.collected + ((field_name, value),)
    remaining = tuple(f for f in ws.fields_pending if f.name != field_name)

    if remaining:
        new_ws = WizardState(
            node=ws.node, fields_pending=remaining, collected=new_collected
        )
        return replace(model, wizard_state=new_ws), []

    raw_dict = dict(new_collected)
    request = {
        k: coerce_value(v, _find_field(ws.node, k))
        for k, v in raw_dict.items()
        if _find_field(ws.node, k) is not None
    }
    request = expand_request(request, model.variables_dict())
    new_model = replace(model, mode=ReplMode.NORMAL, wizard_state=None, error=None)
    return new_model, [
        InvokeRpc(
            service=ws.node.service,
            method=ws.node.method,
            request=request,
            streaming=ws.node.server_streaming,
            cmd_path=ws.node.cmd,
        )
    ]


def _find_field(node: CmdNode, name: str) -> FieldSpec | None:
    return next((f for f in node.input_fields if f.name == name), None)


# ──────────────────────────────────────────────────────────────────────────────
# Built-in: set / header
# ──────────────────────────────────────────────────────────────────────────────


def _handle_set(model: Model, rest: str) -> tuple[Model, list]:
    parts = rest.split(None, 1)
    if len(parts) != 2:
        return _error(model, "Usage: set <name> <value>")
    name, value = parts
    return model.with_variable(name, value), [SetVariable(name, value)]


def _handle_header(model: Model, rest: str) -> tuple[Model, list]:
    words = rest.split(None, 2)
    if not words:
        return _error(model, "Usage: header set|list|clear [name] [value]")
    sub = words[0]
    if sub == "list":
        return model, [RenderHeaderList(headers=model.headers)]
    if sub == "clear":
        new_model = (
            replace(model, headers=())
            if len(words) == 1
            else model.without_header(words[1])
        )
        return new_model, []
    if sub == "set":
        if len(words) < 3:
            return _error(model, "Usage: header set <name> <value>")
        return model.with_header(words[1], words[2]), []
    return _error(model, f"Unknown header sub-command: {sub}")


# ──────────────────────────────────────────────────────────────────────────────
# Render-cmd sentinels
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RenderResponse:
    cmd_path: str
    result: Any
    success: bool


@dataclass(frozen=True)
class RenderStreamChunk:
    cmd_path: str
    chunk: Any
    index: int


@dataclass(frozen=True)
class RenderStreamEnd:
    cmd_path: str
    total: int


@dataclass(frozen=True)
class RenderError:
    message: str


@dataclass(frozen=True)
class RenderInfo:
    message: str


@dataclass(frozen=True)
class RenderHelp:
    prefix: str
    namespace: str  # active namespace when help was requested


@dataclass(frozen=True)
class RenderHeaderList:
    headers: tuple[tuple[str, str], ...]


def _render_response_cmd(cmd_path, result, success=True):
    return RenderResponse(cmd_path=cmd_path, result=result, success=success)


def _stream_chunk_cmd(cmd_path, chunk, index):
    return RenderStreamChunk(cmd_path=cmd_path, chunk=chunk, index=index)


def _stream_end_cmd(cmd_path, total):
    return RenderStreamEnd(cmd_path=cmd_path, total=total)


def _error_cmd(msg):
    return RenderError(message=msg)


def _help_cmd(text, namespace_prefix=""):
    parts = text.split(None, 1)
    raw_prefix = parts[1].strip() if len(parts) > 1 else ""
    # If no explicit prefix given and namespace active, scope help to namespace
    effective = raw_prefix or namespace_prefix
    return RenderHelp(prefix=effective, namespace=namespace_prefix)


def _error(model: Model, msg: str) -> tuple[Model, list]:
    return replace(model, error=msg), [RenderError(message=msg)]
