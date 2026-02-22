"""
repl/tea/model.py

Immutable Model + auxiliary state types.
No I/O. No grpc. No proto imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Optional

from repl.schema.ui_spec import CmdNode, FieldSpec, UISpec


class ReplMode(Enum):
    NORMAL = "normal"
    WIZARD = "wizard"
    CONFIRM = "confirm"


@dataclass(frozen=True)
class WizardState:
    node: CmdNode
    fields_pending: tuple[FieldSpec, ...]
    collected: tuple[tuple[str, str], ...]

    def next_field(self) -> Optional[FieldSpec]:
        return self.fields_pending[0] if self.fields_pending else None

    def remaining_count(self) -> int:
        return len(self.fields_pending)

    def as_dict(self) -> dict[str, str]:
        return dict(self.collected)


@dataclass(frozen=True)
class RpcResult:
    cmd_path: str
    result: Any


@dataclass(frozen=True)
class Model:
    # ── Connection ──────────────────────────────────────────────────────────
    server: str
    connected: bool

    # ── Schema ──────────────────────────────────────────────────────────────
    ui_spec: UISpec

    # ── Session ─────────────────────────────────────────────────────────────
    headers: tuple[tuple[str, str], ...]
    variables: tuple[tuple[str, str], ...]
    history: tuple[str, ...]

    # ── Namespace (use stack) ────────────────────────────────────────────────
    # Each element is one dot-path segment, e.g. ("key",) or ("key", "search").
    # The active prefix is ".".join(namespace).
    namespace: tuple[str, ...]

    # ── UI state ────────────────────────────────────────────────────────────
    last_response: Optional[RpcResult]
    error: Optional[str]
    mode: ReplMode
    wizard_state: Optional[WizardState]
    stream_active: bool

    # ── Derived helpers ──────────────────────────────────────────────────────

    def namespace_prefix(self) -> str:
        """Active dot-path prefix, e.g. 'key' or 'key.search'. Empty = root."""
        return ".".join(self.namespace)

    def push_namespace(self, segment: str) -> "Model":
        return replace(self, namespace=self.namespace + (segment,))

    def pop_namespace(self) -> "Model":
        if not self.namespace:
            return self
        return replace(self, namespace=self.namespace[:-1])

    def resolve_cmd(self, word: str) -> Optional[str]:
        """
        Return the canonical cmd path for `word`, respecting the active
        namespace prefix.

        Resolution order (when namespace is active):
          1. namespace_prefix + "." + word  (relative — highest priority)
          2. Absolute path or alias

        At root (no namespace): alias/absolute only.

        Always returns the canonical path (alias → resolved), never the
        raw alias string.
        """
        # 1. Namespace-prefixed — try first when inside a namespace
        if self.namespace:
            prefixed = self.namespace_prefix() + "." + word
            if self.ui_spec.commands.get(prefixed) is not None:
                return prefixed

        # 2. Absolute path or alias
        canonical = self.ui_spec.alias_map.get(word, word)
        if self.ui_spec.commands.get(canonical) is not None:
            return canonical

        return None

    def headers_dict(self) -> dict[str, str]:
        return dict(self.headers)

    def variables_dict(self) -> dict[str, str]:
        return dict(self.variables)

    def with_header(self, name: str, value: str) -> "Model":
        d = dict(self.headers)
        d[name.lower()] = value
        return replace(self, headers=tuple(d.items()))

    def without_header(self, name: str) -> "Model":
        d = dict(self.headers)
        d.pop(name.lower(), None)
        return replace(self, headers=tuple(d.items()))

    def with_variable(self, name: str, value: str) -> "Model":
        d = dict(self.variables)
        d[name] = value
        return replace(self, variables=tuple(d.items()))


def initial_model(
    server: str,
    ui_spec: UISpec,
) -> Model:
    return Model(
        server=server,
        connected=True,
        ui_spec=ui_spec,
        headers=(),
        variables=(),
        history=(),
        namespace=(),
        last_response=None,
        error=None,
        mode=ReplMode.NORMAL,
        wizard_state=None,
        stream_active=False,
    )
