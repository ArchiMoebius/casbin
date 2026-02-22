"""
repl/schema/parser.py

CmdParser: builds (and caches) an argparse.ArgumentParser for each CmdNode.
"""

from __future__ import annotations

import argparse
import base64
import json
from typing import Any

from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from repl.schema.ui_spec import CmdNode, FieldSpec


class _SilentParser(argparse.ArgumentParser):
    """ArgumentParser that raises instead of calling sys.exit."""

    def error(self, message: str):  # type: ignore[override]
        raise argparse.ArgumentError(None, message)

    def exit(self, status: int = 0, message: str = ""):  # type: ignore[override]
        raise SystemExit(status)


class CmdParser:
    """
    Lazily builds and caches one ArgumentParser per CmdNode.
    Thread-safe reads (parsers are never mutated after creation).
    """

    _instance: "CmdParser | None" = None

    def __init__(self) -> None:
        self._cache: dict[str, _SilentParser] = {}

    @classmethod
    def instance(cls) -> "CmdParser":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def build(self, node: CmdNode) -> _SilentParser:
        if node.cmd in self._cache:
            return self._cache[node.cmd]

        p = _SilentParser(
            prog=node.cmd,
            description=node.description,
            add_help=False,
            exit_on_error=False,
        )

        for field in node.input_fields:
            if field.hidden:
                continue
            kwargs = _field_to_kwargs(field)
            p.add_argument(f"--{field.name}", **kwargs)

        self._cache[node.cmd] = p
        return p

    def invalidate(self, cmd: str) -> None:
        self._cache.pop(cmd, None)


def namespace_to_request(ns: argparse.Namespace, node: CmdNode) -> dict[str, Any]:
    """Convert parsed argparse Namespace → request dict, omitting unset fields."""
    result: dict[str, Any] = {}
    for field in node.input_fields:
        val = getattr(ns, field.name, None)
        if val is None:
            continue
        # For repeated fields argparse returns a list; empty list means not set
        if field.is_repeated and isinstance(val, list) and not val:
            continue
        result[field.name] = val
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Field → argparse kwargs
# ──────────────────────────────────────────────────────────────────────────────

_BOOL_TYPE = lambda s: s.lower() not in ("false", "0", "no")  # noqa: E731
_BYTES_TYPE = base64.b64decode
_JSON_TYPE = json.loads


def _field_to_kwargs(field: FieldSpec) -> dict[str, Any]:
    nargs = "*" if field.is_repeated else "?"

    # Enum
    if field.enum_values:
        return dict(
            type=str,
            choices=list(field.enum_values),
            default=[] if field.is_repeated else None,
            nargs=nargs,
            metavar=field.name.upper(),
            help=f"enum: {', '.join(field.enum_values[:5])}{'...' if len(field.enum_values) > 5 else ''}",
        )

    type_map = {
        FieldDescriptorProto.TYPE_STRING: str,
        FieldDescriptorProto.TYPE_INT32: int,
        FieldDescriptorProto.TYPE_INT64: int,
        FieldDescriptorProto.TYPE_UINT32: int,
        FieldDescriptorProto.TYPE_UINT64: int,
        FieldDescriptorProto.TYPE_SINT32: int,
        FieldDescriptorProto.TYPE_SINT64: int,
        FieldDescriptorProto.TYPE_FIXED32: int,
        FieldDescriptorProto.TYPE_FIXED64: int,
        FieldDescriptorProto.TYPE_SFIXED32: int,
        FieldDescriptorProto.TYPE_SFIXED64: int,
        FieldDescriptorProto.TYPE_FLOAT: float,
        FieldDescriptorProto.TYPE_DOUBLE: float,
        FieldDescriptorProto.TYPE_BOOL: _BOOL_TYPE,
        FieldDescriptorProto.TYPE_BYTES: _BYTES_TYPE,
        FieldDescriptorProto.TYPE_MESSAGE: _JSON_TYPE,
    }

    py_type = type_map.get(field.proto_type, str)
    label = _proto_type_label(field)

    return dict(
        type=py_type,
        default=[] if field.is_repeated else None,
        nargs=nargs,
        metavar=field.name.upper(),
        help=label,
    )


def _proto_type_label(field: FieldSpec) -> str:
    from google.protobuf.descriptor_pb2 import FieldDescriptorProto as FDP

    names = {
        FDP.TYPE_DOUBLE: "double",
        FDP.TYPE_FLOAT: "float",
        FDP.TYPE_INT64: "int64",
        FDP.TYPE_UINT64: "uint64",
        FDP.TYPE_INT32: "int32",
        FDP.TYPE_FIXED64: "fixed64",
        FDP.TYPE_FIXED32: "fixed32",
        FDP.TYPE_BOOL: "bool",
        FDP.TYPE_STRING: "string",
        FDP.TYPE_GROUP: "group",
        FDP.TYPE_MESSAGE: f"message({field.message_type or '?'})",
        FDP.TYPE_BYTES: "bytes(base64)",
        FDP.TYPE_UINT32: "uint32",
        FDP.TYPE_ENUM: "enum",
        FDP.TYPE_SFIXED32: "sfixed32",
        FDP.TYPE_SFIXED64: "sfixed64",
        FDP.TYPE_SINT32: "sint32",
        FDP.TYPE_SINT64: "sint64",
    }
    label = names.get(field.proto_type, f"type_{field.proto_type}")
    if field.is_repeated:
        label = f"[]{label}"
    return label
