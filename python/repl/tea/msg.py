"""
repl/tea/msg.py  — all Msg variants (inputs / async results)
repl/tea/cmd.py  — all Cmd variants (effect descriptors)

Kept in one file to avoid circular imports; split into msg/cmd sections.
"""
from __future__ import annotations

import grpc
from dataclasses import dataclass, field
from typing import Any, Optional


# ══════════════════════════════════════════════════════════════════════════════
# MSG — events flowing INTO Update
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class UserInput:
    text: str

@dataclass(frozen=True)
class WizardFieldDone:
    field_name: str
    value:      str

@dataclass(frozen=True)
class WizardAborted:
    pass

@dataclass(frozen=True)
class CancelStream:
    pass

@dataclass(frozen=True)
class Interrupt:
    pass

@dataclass(frozen=True)
class RpcSuccess:
    cmd_path: str
    result:   Any   # dict

@dataclass(frozen=True)
class StreamChunk:
    cmd_path: str
    chunk:    Any   # dict
    index:    int

@dataclass(frozen=True)
class StreamEnd:
    cmd_path: str
    total:    int

@dataclass(frozen=True)
class RpcError:
    cmd_path: str
    code:     grpc.StatusCode
    detail:   str

@dataclass(frozen=True)
class CompletionLoaded:
    source_rpc: str     # notification only — data stored in runtime cache

@dataclass(frozen=True)
class BootstrapDone:
    pass


Msg = (
    UserInput | WizardFieldDone | WizardAborted | CancelStream | Interrupt |
    RpcSuccess | StreamChunk | StreamEnd | RpcError |
    CompletionLoaded | BootstrapDone
)


# ══════════════════════════════════════════════════════════════════════════════
# CMD — side-effect descriptors returned by Update
# ══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class InvokeRpc:
    service:   str
    method:    str
    request:   dict
    streaming: bool      = False
    cmd_path:  str       = ""   # used to correlate back to CmdNode

@dataclass(frozen=True)
class FetchCompletion:
    source_rpc:      str
    request:         dict
    live:            bool = False   # if true, skip cache check

@dataclass(frozen=True)
class CancelActiveStream:
    pass

@dataclass(frozen=True)
class StartWizard:
    cmd_path: str

@dataclass(frozen=True)
class WriteHistory:
    line: str

@dataclass(frozen=True)
class SetVariable:
    name:  str
    value: str

@dataclass(frozen=True)
class ExitRepl:
    pass


Cmd = (
    InvokeRpc | FetchCompletion | CancelActiveStream | StartWizard |
    WriteHistory | SetVariable | ExitRepl
)
