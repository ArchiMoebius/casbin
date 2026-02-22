"""
repl/schema/ui_spec.py

All frozen dataclasses that describe the compiled UI specification.
Nothing in this module imports grpc, prompt_toolkit, or any I/O library.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional, Union


# ──────────────────────────────────────────────────────────────────────────────
# Reflection sentinel
# Commands backed by gRPC reflection rather than a real RPC use this as their
# service name.  The runtime intercepts it before calling invoke_rpc().
# ──────────────────────────────────────────────────────────────────────────────
REFLECTION_SERVICE = "__reflection__"
# ──────────────────────────────────────────────────────────────────────────────
# Enums (mirror repl.proto)
# ──────────────────────────────────────────────────────────────────────────────


class DisplayMode(Enum):
    AUTO = 0
    TABLE = 1
    KV = 2
    JSON = 3
    STREAM = 4
    SILENT = 5
    RAW = 6


class CellRenderer(Enum):
    DEFAULT = 0
    TIMESTAMP = 1
    BYTES = 2
    BOOL_ICON = 3
    JSON = 4


class FilterOp(Enum):
    EQ = "eq"
    NEQ = "neq"
    PRESENT = "present"
    IN_SET = "in_set"


# ──────────────────────────────────────────────────────────────────────────────
# Display
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DisplaySpec:
    mode: DisplayMode = DisplayMode.AUTO
    columns: tuple[str, ...] = ()
    stream_separator: str = "─"
    title_field: Optional[str] = None
    success_message: str = "✔"
    expand_nested: bool = False


# ──────────────────────────────────────────────────────────────────────────────
# Field
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FieldSpec:
    name: str
    proto_type: int  # FieldDescriptorProto.TYPE_*
    is_repeated: bool = False
    enum_values: tuple[str, ...] = ()
    label: str = ""
    hidden: bool = False
    renderer: CellRenderer = CellRenderer.DEFAULT
    column_width: int = 0
    message_type: Optional[str] = None  # fqn for TYPE_MESSAGE fields
    # Integer range completion (int_completion_end > 0 enables it)
    int_completion_start: int = 0
    int_completion_end: int = 0
    int_completion_step: int = 1

    def display_label(self) -> str:
        return self.label or self.name


# ──────────────────────────────────────────────────────────────────────────────
# Completion
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CompletionDisplayFieldSpec:
    path: str
    label: str = ""
    width: int = 0
    renderer: CellRenderer = CellRenderer.DEFAULT
    join_separator: str = ", "
    style: str = "class:completion-meta"

    def display_label(self) -> str:
        return self.label or self.path.split(".")[-1]


@dataclass(frozen=True)
class CompletionFilterSpec:
    field: str
    op: FilterOp
    value: Union[str, tuple[str, ...], None]  # None for PRESENT

    def test(self, candidate: dict) -> bool:
        from repl.schema.projection import resolve_path

        raw = resolve_path(candidate, self.field)
        # Normalise Python booleans so "true"/"false" filter strings match.
        if raw is None:
            actual = ""
        elif isinstance(raw, bool):
            actual = str(raw).lower()  # True→"true", False→"false"
        else:
            actual = str(raw)
        if self.op == FilterOp.EQ:
            return actual == self.value
        if self.op == FilterOp.NEQ:
            return actual != self.value
        if self.op == FilterOp.PRESENT:
            return bool(resolve_path(candidate, self.field))
        if self.op == FilterOp.IN_SET:
            return actual in self.value
        return True


@dataclass(frozen=True)
class CompletionSource:
    field: str
    source_rpc: str
    source_field: str
    source_request: str = "{}"
    live: bool = False
    # Projection
    value_field: Optional[str] = None
    display_fields: tuple[CompletionDisplayFieldSpec, ...] = ()
    filters: tuple[CompletionFilterSpec, ...] = ()
    show_headers: bool = False
    column_separator: str = "  "


# ──────────────────────────────────────────────────────────────────────────────
# Command node
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CmdNode:
    cmd: str
    aliases: tuple[str, ...]
    description: str
    service: str
    method: str
    server_streaming: bool
    client_streaming: bool
    input_fields: tuple[FieldSpec, ...]
    output_fields: tuple[FieldSpec, ...]
    display: DisplaySpec
    completions: tuple[CompletionSource, ...]
    bootstrap: bool = False
    refresh_after_mutation: bool = False

    def completion_for_field(self, field_name: str) -> Optional[CompletionSource]:
        for cs in self.completions:
            if cs.field == field_name:
                return cs
        return None


# ──────────────────────────────────────────────────────────────────────────────
# UISpec — the compiled, immutable schema
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class UISpec:
    services: tuple[str, ...]
    # All commands keyed by canonical dot-path
    commands: dict[str, CmdNode]  # mutable dict in frozen dataclass is fine —
    alias_map: dict[str, str]  # we never reassign these references

    def find_cmd(self, path_or_alias: str) -> Optional[CmdNode]:
        canonical = self.alias_map.get(path_or_alias, path_or_alias)
        return self.commands.get(canonical)

    def subtree(self, prefix: str) -> list[CmdNode]:
        """All non-bootstrap commands whose cmd starts with prefix."""
        return [
            n
            for k, n in self.commands.items()
            if k.startswith(prefix) and not n.bootstrap
        ]

    def user_commands(self) -> list[CmdNode]:
        return [n for n in self.commands.values() if not n.bootstrap]

    def bootstrap_nodes(self) -> list[CmdNode]:
        return [n for n in self.commands.values() if n.bootstrap]

    def all_completion_sources(self) -> list[CompletionSource]:
        seen: set[str] = set()
        out: list[CompletionSource] = []
        for node in self.commands.values():
            for cs in node.completions:
                if cs.source_rpc not in seen:
                    seen.add(cs.source_rpc)
                    out.append(cs)
        return out

    def completion_source_by_rpc(self, rpc: str) -> Optional[CompletionSource]:
        for node in self.commands.values():
            for cs in node.completions:
                if cs.source_rpc == rpc:
                    return cs
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Completion cache entry
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class CandidateRow:
    """One projected completion candidate."""

    insert_value: str
    display_cols: tuple[tuple[str, str], ...]  # (text, style_class)
    raw: dict[str, Any]


class CompletionCache:
    """Mutable cache owned by the runtime, read by the completer."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[CandidateRow, ...]] = {}

    def store(self, source_rpc: str, rows: tuple[CandidateRow, ...]) -> None:
        self._store[source_rpc] = rows

    def get(self, source_rpc: str) -> tuple[CandidateRow, ...]:
        return self._store.get(source_rpc, ())

    def invalidate(self, source_rpc: str) -> None:
        self._store.pop(source_rpc, None)

    def has(self, source_rpc: str) -> bool:
        return source_rpc in self._store
