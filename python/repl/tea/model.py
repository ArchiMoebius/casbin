"""
repl/tea/model.py

Immutable Model + auxiliary state types.
No I/O. No grpc. No proto imports.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum, auto
from typing import Any, Optional

from repl.schema.ui_spec import CmdNode, FieldSpec, UISpec


class ReplMode(Enum):
    NORMAL  = "normal"
    WIZARD  = "wizard"   # field-by-field guided input
    CONFIRM = "confirm"  # awaiting yes/no


@dataclass(frozen=True)
class WizardState:
    node:           CmdNode
    # Fields still needing input (ordered)
    fields_pending: tuple[FieldSpec, ...]
    # Already-collected field values (field.name → raw string)
    collected:      tuple[tuple[str, str], ...]   # ordered pairs for hashing

    def next_field(self) -> Optional[FieldSpec]:
        return self.fields_pending[0] if self.fields_pending else None

    def remaining_count(self) -> int:
        return len(self.fields_pending)

    def as_dict(self) -> dict[str, str]:
        return dict(self.collected)


@dataclass(frozen=True)
class RpcResult:
    cmd_path: str
    result:   Any   # dict or list[dict]


@dataclass(frozen=True)
class Model:
    # ── Connection ──────────────────────────────────────────────────────────
    server:       str
    connected:    bool

    # ── Schema (compiled once at bootstrap) ─────────────────────────────────
    ui_spec:      UISpec

    # ── Session ─────────────────────────────────────────────────────────────
    headers:      tuple[tuple[str, str], ...]   # ordered (name, value) pairs
    variables:    tuple[tuple[str, str], ...]   # ordered (name, value) pairs
    history:      tuple[str, ...]

    # ── UI state ────────────────────────────────────────────────────────────
    last_response:  Optional[RpcResult]
    error:          Optional[str]
    mode:           ReplMode
    wizard_state:   Optional[WizardState]
    stream_active:  bool

    # ── Auth (initialised from CLI flags) ────────────────────────────────────
    auth_header:    Optional[str]

    def headers_dict(self) -> dict[str, str]:
        return dict(self.headers)

    def variables_dict(self) -> dict[str, str]:
        return dict(self.variables)

    def with_header(self, name: str, value: str) -> "Model":
        existing = dict(self.headers)
        existing[name.lower()] = value
        return replace(self, headers=tuple(existing.items()))

    def without_header(self, name: str) -> "Model":
        existing = dict(self.headers)
        existing.pop(name.lower(), None)
        return replace(self, headers=tuple(existing.items()))

    def with_variable(self, name: str, value: str) -> "Model":
        existing = dict(self.variables)
        existing[name] = value
        return replace(self, variables=tuple(existing.items()))


def initial_model(
    server: str,
    ui_spec: UISpec,
    auth_header: Optional[str] = None,
) -> Model:
    return Model(
        server=server,
        connected=True,
        ui_spec=ui_spec,
        headers=(),
        variables=(),
        history=(),
        last_response=None,
        error=None,
        mode=ReplMode.NORMAL,
        wizard_state=None,
        stream_active=False,
        auth_header=auth_header,
    )
