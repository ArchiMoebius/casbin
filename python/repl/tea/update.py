"""
repl/tea/update.py

Pure update function: (Model, Msg) → (Model, list[Cmd])

No I/O. No grpc calls. No rendering. 100% unit-testable.
"""
from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import replace
from typing import Any

import grpc

from repl.schema.dsl import coerce_value, expand_request, expand_template
from repl.schema.parser import CmdParser, namespace_to_request
from repl.schema.ui_spec import CmdNode, FieldSpec, UISpec
from repl.tea.model import Model, ReplMode, RpcResult, WizardState
from repl.tea.msg import (
    BootstrapDone, CancelStream, CompletionLoaded, Interrupt, Msg,
    RpcError, RpcSuccess, StreamChunk, StreamEnd, UserInput,
    WizardAborted, WizardFieldDone,
)
from repl.tea.msg import (
    CancelActiveStream, ExitRepl, FetchCompletion, InvokeRpc,
    SetVariable, StartWizard, WriteHistory,
)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def update(model: Model, msg: Msg) -> tuple[Model, list]:
    """
    Pure state transition.  Returns (new_model, list[Cmd]).
    The runtime executes the Cmd list as side effects.
    """
    if isinstance(msg, UserInput):
        return _handle_user_input(model, msg.text)

    if isinstance(msg, WizardFieldDone):
        return _handle_wizard_field(model, msg.field_name, msg.value)

    if isinstance(msg, WizardAborted):
        return (
            replace(model, mode=ReplMode.NORMAL, wizard_state=None, error=None),
            [],
        )

    if isinstance(msg, RpcSuccess):
        return _handle_rpc_success(model, msg)

    if isinstance(msg, (StreamChunk, StreamEnd)):
        return _handle_stream(model, msg)

    if isinstance(msg, RpcError):
        return _handle_rpc_error(model, msg)

    if isinstance(msg, CompletionLoaded):
        # Cache update is done by runtime; model doesn't hold cache
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

    if text.startswith("set "):
        return _handle_set(model, text[4:].strip())

    if text.startswith("header "):
        return _handle_header(model, text[7:].strip())

    if text in ("help", "?") or text.startswith("help ") or text.startswith("? "):
        # Help is rendered by the runtime using the UISpec
        return model, [_help_cmd(text)]

    # ── Resolve command ───────────────────────────────────────────────────────
    try:
        words = shlex.split(text)
    except ValueError as exc:
        return _error(model, f"Parse error: {exc}")

    if not words:
        return model, []

    cmd_word = words[0]
    node     = model.ui_spec.find_cmd(cmd_word)

    if node is None:
        suggestions = model.ui_spec.subtree(cmd_word)
        if suggestions:
            names = ", ".join(n.cmd for n in suggestions[:4])
            return _error(model, f"Unknown command '{cmd_word}'. Did you mean: {names}?")
        return _error(model, f"Unknown command: '{cmd_word}'. Type 'help' for available commands.")

    # ── Parse flags ───────────────────────────────────────────────────────────
    arg_words = words[1:]

    # No args + node has visible input fields → wizard mode
    if not arg_words and any(not f.hidden for f in node.input_fields):
        new_model = replace(model,
            history=model.history + (text,),
            error=None,
        )
        return new_model, [WriteHistory(text), StartWizard(node.cmd)]

    parser = CmdParser.instance().build(node)
    try:
        namespace = parser.parse_args(arg_words)
    except (argparse.ArgumentError, SystemExit) as exc:
        return _error(model, str(exc))

    request = namespace_to_request(namespace, node)
    request = expand_request(request, model.variables_dict())

    new_model = replace(model,
        history=model.history + (text,),
        error=None,
    )
    cmds: list = [WriteHistory(text)]
    cmds.append(InvokeRpc(
        service=node.service,
        method=node.method,
        request=request,
        streaming=node.server_streaming,
        cmd_path=node.cmd,
    ))
    return new_model, cmds


# ──────────────────────────────────────────────────────────────────────────────
# RPC result handlers
# ──────────────────────────────────────────────────────────────────────────────

def _handle_rpc_success(model: Model, msg: RpcSuccess) -> tuple[Model, list]:
    node      = model.ui_spec.find_cmd(msg.cmd_path)
    new_model = replace(
        model,
        last_response=RpcResult(msg.cmd_path, msg.result),
        error=None,
        stream_active=False,
    )
    cmds: list = []

    # Trigger completion refresh if this was a mutating method
    if node and node.refresh_after_mutation:
        for cs in model.ui_spec.all_completion_sources():
            cmds.append(FetchCompletion(
                source_rpc=cs.source_rpc,
                request=json.loads(cs.source_request),
                live=True,
            ))

    # Runtime will render using node + result; pass a sentinel
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
    return replace(model, error=detail, stream_active=False), [
        _error_cmd(detail)
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Wizard
# ──────────────────────────────────────────────────────────────────────────────

def _handle_wizard_field(
    model: Model,
    field_name: str,
    value: str,
) -> tuple[Model, list]:
    ws = model.wizard_state
    if ws is None:
        return model, []

    new_collected = ws.collected + ((field_name, value),)
    remaining     = tuple(f for f in ws.fields_pending if f.name != field_name)

    if remaining:
        new_ws = WizardState(
            node=ws.node,
            fields_pending=remaining,
            collected=new_collected,
        )
        return replace(model, wizard_state=new_ws), []

    # All fields collected → fire the RPC
    raw_dict = dict(new_collected)
    request  = {
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
    return (
        model.with_variable(name, value),
        [SetVariable(name, value)],
    )


def _handle_header(model: Model, rest: str) -> tuple[Model, list]:
    words = rest.split(None, 2)
    if not words:
        return _error(model, "Usage: header set|list|clear [name] [value]")

    sub = words[0]
    if sub == "list":
        return model, [_header_list_cmd(model)]
    if sub == "clear":
        if len(words) == 1:
            new_model = replace(model, headers=())
        else:
            new_model = model.without_header(words[1])
        return new_model, []
    if sub == "set":
        if len(words) < 3:
            return _error(model, "Usage: header set <name> <value>")
        new_model = model.with_header(words[1], words[2])
        return new_model, []

    return _error(model, f"Unknown header sub-command: {sub}")


# ──────────────────────────────────────────────────────────────────────────────
# Sentinel Cmd factories  (runtime pattern-matches on class + attributes)
# ──────────────────────────────────────────────────────────────────────────────
# We reuse the existing Cmd dataclasses but add a thin layer of "render cmds"
# that the runtime interprets to print output.

from dataclasses import dataclass

@dataclass(frozen=True)
class RenderResponse:
    cmd_path: str
    result:   Any
    success:  bool

@dataclass(frozen=True)
class RenderStreamChunk:
    cmd_path: str
    chunk:    Any
    index:    int

@dataclass(frozen=True)
class RenderStreamEnd:
    cmd_path: str
    total:    int

@dataclass(frozen=True)
class RenderError:
    message: str

@dataclass(frozen=True)
class RenderHelp:
    prefix: str

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

def _help_cmd(text):
    prefix = ""
    parts  = text.split(None, 1)
    if len(parts) > 1:
        prefix = parts[1].strip()
    return RenderHelp(prefix=prefix)

def _header_list_cmd(model):
    return RenderHeaderList(headers=model.headers)


def _error(model: Model, msg: str) -> tuple[Model, list]:
    return replace(model, error=msg), [RenderError(message=msg)]
