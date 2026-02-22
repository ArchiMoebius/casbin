"""
repl/schema/builder.py

UISpecBuilder: reads a GrpcReflectionClient and compiles a UISpec.
Single Responsibility: descriptor → UISpec only. No I/O, no rendering.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Optional

from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from repl.schema.ui_spec import (
    CandidateRow,
    CellRenderer,
    CmdNode,
    CompletionDisplayFieldSpec,
    CompletionFilterSpec,
    CompletionSource,
    DisplayMode,
    DisplaySpec,
    FieldSpec,
    FilterOp,
    REFLECTION_SERVICE,
    UISpec,
)

if TYPE_CHECKING:
    pass  # avoid circular imports with GrpcReflectionClient

log = logging.getLogger(__name__)

# Proto extension field numbers (must match repl.proto)
_METHOD_EXT_NUM = 50100
_MESSAGE_EXT_NUM = 50101
_FIELD_EXT_NUM = 50102
_METHOD_EXT_FQN = "repl.v1.method_opts"
_MESSAGE_EXT_FQN = "repl.v1.message_opts"
_FIELD_EXT_FQN = "repl.v1.field_opts"

# DisplayMode int → enum
_DISPLAY_MODE_MAP = {
    0: DisplayMode.AUTO,
    1: DisplayMode.TABLE,
    2: DisplayMode.KV,
    3: DisplayMode.JSON,
    4: DisplayMode.STREAM,
    5: DisplayMode.SILENT,
    6: DisplayMode.RAW,
}

# CellRenderer int → enum
_CELL_RENDERER_MAP = {
    0: CellRenderer.DEFAULT,
    1: CellRenderer.TIMESTAMP,
    2: CellRenderer.BYTES,
    3: CellRenderer.BOOL_ICON,
    4: CellRenderer.JSON,
}


def _opt_value(opts_msg, fqn: str, field_num: int) -> Optional[Any]:
    """
    Extract an extension from a proto Options message.

    Tries two strategies:
      1. Iterate ListFields() and match by full_name (works when extension is
         registered in the descriptor pool used by GetOptions()).
      2. Access via _extensions_by_number on the options class (fallback).
    """
    if opts_msg is None:
        return None
    try:
        for fd, val in opts_msg.ListFields():
            if fd.full_name == fqn or fd.number == field_num:
                return val
    except Exception:
        pass
    return None


def _msg_field(msg: Any, name: str, default=None):
    """Safely read a field from a proto message, returning default if absent."""
    try:
        val = getattr(msg, name)
        # For strings/repeated, proto3 returns "" / [] as default — that's fine
        return val
    except (AttributeError, ValueError):
        return default


def _repeated_strings(msg: Any, name: str) -> tuple[str, ...]:
    try:
        return tuple(getattr(msg, name))
    except Exception:
        return ()


class UISpecBuilder:
    """
    Compiles reflection output + repl.v1 options into an immutable UISpec.

    Open/Closed: add new DSL features by adding _build_* methods, not editing
    existing ones.
    """

    def build(self, client: Any) -> UISpec:  # client: GrpcReflectionClient
        services = client.list_services()
        commands: dict[str, CmdNode] = {}
        alias_map: dict[str, str] = {}

        for svc in services:
            try:
                self._load_service(client, svc, commands, alias_map)
            except Exception as exc:
                log.warning("Could not load service %s: %s", svc, exc)

        # Inject the reflection-backed 'services' command.  It has no
        # corresponding RPC — the runtime intercepts REFLECTION_SERVICE and
        # calls client.list_services() directly.
        svc_node = self._build_services_node(services)
        commands[svc_node.cmd] = svc_node
        for alias in svc_node.aliases:
            alias_map[alias] = svc_node.cmd

        return UISpec(
            services=tuple(services),
            commands=commands,
            alias_map=alias_map,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Service loading
    # ──────────────────────────────────────────────────────────────────────

    def _load_service(
        self,
        client: Any,
        svc: str,
        commands: dict[str, CmdNode],
        alias_map: dict[str, str],
    ) -> None:
        client._resolve_service(svc)
        svc_desc = client.pool.FindServiceByName(svc)

        for method_desc in svc_desc.methods:
            try:
                node = self._build_node(client, svc, method_desc)
            except Exception as exc:
                log.warning("Skipping method %s.%s: %s", svc, method_desc.name, exc)
                continue

            if node is None:
                continue  # No repl.v1.method option → invisible

            if node.cmd in commands:
                log.warning(
                    "Duplicate cmd path '%s' — skipping %s.%s",
                    node.cmd,
                    svc,
                    method_desc.name,
                )
                continue

            commands[node.cmd] = node
            for alias in node.aliases:
                alias_map[alias] = node.cmd

    # ──────────────────────────────────────────────────────────────────────
    # CmdNode
    # ──────────────────────────────────────────────────────────────────────

    def _build_node(
        self,
        client: Any,
        svc: str,
        method_desc: Any,
    ) -> Optional[CmdNode]:
        opts = method_desc.GetOptions()
        repl_opt = _opt_value(opts, _METHOD_EXT_FQN, _METHOD_EXT_NUM)

        if repl_opt is None:
            return None

        cmd = _msg_field(repl_opt, "cmd", "").strip()
        if not cmd:
            return None

        input_fields = self._build_fields(client, method_desc.input_type)
        output_fields = self._build_fields(client, method_desc.output_type)
        display = self._build_display(
            _msg_field(repl_opt, "display"),
            method_desc.output_type,
            method_desc.server_streaming,
        )
        completions = tuple(
            self._build_completion_source(c)
            for c in _msg_field(repl_opt, "completions") or []
        )

        return CmdNode(
            cmd=cmd,
            aliases=_repeated_strings(repl_opt, "aliases"),
            description=_msg_field(repl_opt, "description", ""),
            service=svc,
            method=method_desc.name,
            server_streaming=method_desc.server_streaming,
            client_streaming=method_desc.client_streaming,
            input_fields=input_fields,
            output_fields=output_fields,
            display=display,
            completions=completions,
            bootstrap=_msg_field(repl_opt, "bootstrap", False),
            refresh_after_mutation=_msg_field(
                repl_opt, "refresh_after_mutation", False
            ),
        )

    # ──────────────────────────────────────────────────────────────────────
    # Fields
    # ──────────────────────────────────────────────────────────────────────

    def _build_fields(
        self,
        client: Any,
        msg_descriptor: Any,
    ) -> tuple[FieldSpec, ...]:
        result: list[FieldSpec] = []
        for f in msg_descriptor.fields:
            result.append(self._build_field(f))
        return tuple(result)

    def _build_field(self, f: Any) -> FieldSpec:
        field_opt = _opt_value(f.GetOptions(), _FIELD_EXT_FQN, _FIELD_EXT_NUM)

        label = ""
        hidden = False
        renderer = CellRenderer.DEFAULT
        col_w = 0
        int_start = 0
        int_end = 0
        int_step = 1

        if field_opt is not None:
            label = _msg_field(field_opt, "label", "")
            hidden = _msg_field(field_opt, "hidden", False)
            renderer = _CELL_RENDERER_MAP.get(
                int(_msg_field(field_opt, "renderer", 0)), CellRenderer.DEFAULT
            )
            col_w = int(_msg_field(field_opt, "column_width", 0))
            int_start = int(_msg_field(field_opt, "int_completion_start", 0))
            int_end = int(_msg_field(field_opt, "int_completion_end", 0))
            int_step = int(_msg_field(field_opt, "int_completion_step", 1)) or 1

        enum_values: tuple[str, ...] = ()
        if f.type == FieldDescriptorProto.TYPE_ENUM and f.enum_type:
            enum_values = tuple(v.name for v in f.enum_type.values)

        message_type: Optional[str] = None
        if f.type == FieldDescriptorProto.TYPE_MESSAGE and f.message_type:
            message_type = f.message_type.full_name

        return FieldSpec(
            name=f.name,
            proto_type=f.type,
            is_repeated=(f.label == FieldDescriptorProto.LABEL_REPEATED),
            enum_values=enum_values,
            label=label,
            hidden=hidden,
            renderer=renderer,
            column_width=col_w,
            message_type=message_type,
            int_completion_start=int_start,
            int_completion_end=int_end,
            int_completion_step=int_step,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Display
    # ──────────────────────────────────────────────────────────────────────

    def _build_display(
        self,
        display_msg: Optional[Any],
        output_desc: Any,
        is_streaming: bool,
    ) -> DisplaySpec:
        # Try message-level default
        msg_opts = output_desc.GetOptions()
        msg_repl_opt = _opt_value(msg_opts, _MESSAGE_EXT_FQN, _MESSAGE_EXT_NUM)
        msg_disp_msg = _msg_field(msg_repl_opt, "display") if msg_repl_opt else None

        # Priority: method option > message option > auto-infer
        effective = display_msg or msg_disp_msg
        if effective is not None:
            mode = _DISPLAY_MODE_MAP.get(
                int(_msg_field(effective, "mode", 0)), DisplayMode.AUTO
            )
            if mode != DisplayMode.AUTO:
                return DisplaySpec(
                    mode=mode,
                    columns=_repeated_strings(effective, "columns"),
                    stream_separator=_msg_field(effective, "stream_separator", "─"),
                    title_field=_msg_field(effective, "title_field", None) or None,
                    success_message=_msg_field(effective, "success_message", "✔")
                    or "✔",
                    expand_nested=_msg_field(effective, "expand_nested", False),
                )

        # Auto-infer
        if is_streaming:
            return DisplaySpec(mode=DisplayMode.STREAM)
        has_repeated = any(
            f.label == FieldDescriptorProto.LABEL_REPEATED for f in output_desc.fields
        )
        if has_repeated:
            return DisplaySpec(mode=DisplayMode.TABLE)
        return DisplaySpec(mode=DisplayMode.KV)

    # ──────────────────────────────────────────────────────────────────────
    # Completion sources
    # ──────────────────────────────────────────────────────────────────────

    def _build_completion_source(self, fc: Any) -> CompletionSource:
        display_fields = tuple(
            self._build_display_field(df)
            for df in (_msg_field(fc, "display_fields") or [])
        )
        filters = tuple(
            self._build_filter(f) for f in (_msg_field(fc, "filters") or [])
        )
        return CompletionSource(
            field=_msg_field(fc, "field", ""),
            source_rpc=_msg_field(fc, "source_rpc", ""),
            source_field=_msg_field(fc, "source_field", ""),
            source_request=_msg_field(fc, "source_request", "{}") or "{}",
            live=_msg_field(fc, "live", False),
            value_field=_msg_field(fc, "value_field", None) or None,
            display_fields=display_fields,
            filters=filters,
            show_headers=_msg_field(fc, "show_headers", False),
            column_separator=_msg_field(fc, "column_separator", "  ") or "  ",
        )

    def _build_display_field(self, df: Any) -> CompletionDisplayFieldSpec:
        return CompletionDisplayFieldSpec(
            path=_msg_field(df, "path", ""),
            label=_msg_field(df, "label", ""),
            width=int(_msg_field(df, "width", 0)),
            renderer=_CELL_RENDERER_MAP.get(
                int(_msg_field(df, "renderer", 0)), CellRenderer.DEFAULT
            ),
            join_separator=_msg_field(df, "join_separator", ", ") or ", ",
            style=_msg_field(df, "style", "") or "class:completion-meta",
        )

    def _build_filter(self, f: Any) -> CompletionFilterSpec:
        which = None
        value: Any = None

        for cond in ("eq", "neq", "present", "in_set"):
            try:
                which_check = f.WhichOneof("condition")
                if which_check:
                    which = which_check
                    break
            except Exception:
                pass

        if which is None:
            # Fallback: check each field manually
            for cond in ("eq", "neq", "present", "in_set"):
                v = _msg_field(f, cond)
                if v is not None and v != "" and v != False:  # noqa: E712
                    which = cond
                    break

        field_name = _msg_field(f, "field", "")

        if which == "eq":
            op, value = FilterOp.EQ, str(_msg_field(f, "eq", ""))
        elif which == "neq":
            op, value = FilterOp.NEQ, str(_msg_field(f, "neq", ""))
        elif which == "present":
            op, value = FilterOp.PRESENT, None
        elif which == "in_set":
            in_set_msg = _msg_field(f, "in_set")
            vals = tuple(_repeated_strings(in_set_msg, "values")) if in_set_msg else ()
            op, value = FilterOp.IN_SET, vals
        else:
            op, value = FilterOp.PRESENT, None

        return CompletionFilterSpec(field=field_name, op=op, value=value)

    # ──────────────────────────────────────────────────────────────────────
    # Synthetic reflection-backed commands
    # ──────────────────────────────────────────────────────────────────────

    def _build_services_node(self, services: list[str]) -> CmdNode:
        """
        Synthesise a CmdNode for the 'services' command backed by gRPC
        reflection rather than an RPC.  Output fields are derived from the
        live service list so the table renderer works without a proto schema.
        """
        # Single output field: "name" (the service FQN string)
        name_field = FieldSpec(
            name="name",
            proto_type=9,  # TYPE_STRING
            is_repeated=False,
            label="service",
            column_width=60,
        )
        return CmdNode(
            cmd="services",
            aliases=("ls", "svc"),
            description="List all gRPC services (via reflection)",
            service=REFLECTION_SERVICE,
            method="list_services",
            server_streaming=False,
            client_streaming=False,
            input_fields=(),
            output_fields=(name_field,),
            display=DisplaySpec(
                mode=DisplayMode.TABLE,
                columns=("name",),
            ),
            completions=(),
            bootstrap=False,
            refresh_after_mutation=False,
        )
