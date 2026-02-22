"""
repl/view/response.py

Pure render functions. Return strings or token lists. No I/O.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from repl.schema.projection import cell_value, resolve_path
from repl.schema.ui_spec import (
    CellRenderer,
    CmdNode,
    DisplayMode,
    DisplaySpec,
    FieldSpec,
    UISpec,
)


# ──────────────────────────────────────────────────────────────────────────────
# Top-level dispatcher
# ──────────────────────────────────────────────────────────────────────────────


def view_response(node: Optional[CmdNode], result: Any) -> str:
    if node is None:
        return _json_block(result)

    spec = node.display
    fields = [f for f in node.output_fields if not f.hidden]
    mode = spec.mode

    if mode == DisplayMode.SILENT:
        return _render_silent(spec, result)
    if mode == DisplayMode.KV:
        return _render_kv(fields, result)
    if mode == DisplayMode.TABLE:
        return _render_table(fields, spec, result)
    if mode == DisplayMode.JSON:
        return _json_block(result)
    if mode == DisplayMode.RAW:
        return json.dumps(result, separators=(",", ":"))
    if mode == DisplayMode.STREAM:
        return _render_kv(fields, result)

    # AUTO fallback
    if isinstance(result, list):
        return _render_table(fields, spec, result)
    if isinstance(result, dict):
        return _render_kv(fields, result)
    return str(result)


def view_stream_chunk(node: Optional[CmdNode], chunk: Any, index: int) -> str:
    fields = [f for f in node.output_fields if not f.hidden] if node else []
    spec = node.display if node else DisplaySpec(mode=DisplayMode.STREAM)
    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    field_map = {f.name: f for f in fields}

    if not col_names:
        return f"[{index}] {json.dumps(chunk)}"

    parts = []
    for col in col_names:
        fs = field_map.get(col)
        raw = chunk.get(col, "")
        parts.append(cell_value(raw, fs.renderer if fs else CellRenderer.DEFAULT))

    ts = datetime.now().strftime("%H:%M:%S")
    return f"[{index:>4}] {ts}  {'  │  '.join(parts)}"


def view_stream_header(node: Optional[CmdNode]) -> Optional[str]:
    if node is None:
        return None
    spec = node.display
    fields = [f for f in node.output_fields if not f.hidden]
    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    if not col_names:
        return None
    field_map = {f.name: f for f in fields}
    headers = []
    for col in col_names:
        fs = field_map.get(col)
        label = fs.display_label() if fs else col
        w = fs.column_width if fs else 0
        headers.append(label.ljust(w) if w else label)
    return "       " + "  │  ".join(headers)


def view_help(ui_spec: UISpec, prefix: str = "") -> str:
    cmds = ui_spec.user_commands()
    if prefix:
        cmds = [c for c in cmds if c.cmd == prefix or c.cmd.startswith(prefix + ".")]

    if not cmds:
        return f"  No commands matching '{prefix}'."

    # Single exact command match → show detailed argparse-style help
    if len(cmds) == 1 and cmds[0].cmd == prefix:
        return _render_cmd_detail(cmds[0])

    # Multiple commands → tree listing
    lines = ["  Commands:\n"]
    max_cmd = max(len(c.cmd) for c in cmds)

    for node in sorted(cmds, key=lambda n: n.cmd):
        aliases = f"  ({', '.join(node.aliases)})" if node.aliases else ""
        stream = " [streaming]" if node.server_streaming else ""
        lines.append(
            f"  {node.cmd:<{max_cmd}}  {aliases:<20}{node.description or ''}{stream}"
        )
        # Show flags inline
        flags = [f"--{f.name}" for f in node.input_fields if not f.hidden]
        if flags:
            lines.append(f"  {'':>{max_cmd}}  flags: {'  '.join(flags)}")
        lines.append("")

    return "\n".join(lines)


def _render_cmd_detail(node: CmdNode) -> str:
    """Argparse-style detail for a single command."""
    from repl.schema.parser import CmdParser, _proto_type_label

    lines = [
        f"  {node.cmd}",
        f"  {'─' * len(node.cmd)}",
        f"  {node.description}" if node.description else "",
        "",
    ]
    if node.aliases:
        lines.append(f"  aliases:  {', '.join(node.aliases)}")
        lines.append("")
    if node.server_streaming:
        lines.append("  [server-streaming]")
        lines.append("")

    visible = [f for f in node.input_fields if not f.hidden]
    if visible:
        lines.append("  Flags:")
        max_flag = max(len(f.name) for f in visible)
        for f in visible:
            type_label = _proto_type_label(f)
            comp = "  (tab-complete)" if node.completion_for_field(f.name) else ""
            int_hint = ""
            if f.int_completion_end > 0:
                int_hint = (
                    f"  [{f.int_completion_start}…{f.int_completion_end}"
                    f" step {f.int_completion_step}]"
                )
            lines.append(f"    --{f.name:<{max_flag}}  <{type_label}>{comp}{int_hint}")
        lines.append("")

    if node.output_fields:
        visible_out = [f for f in node.output_fields if not f.hidden]
        lines.append("  Output fields:")
        for f in visible_out:
            lines.append(f"    {f.display_label()}")
        lines.append("")

    return "\n".join(l for l in lines if l is not None)


def view_error(msg: str) -> str:
    return f"  ❌  {msg}"


def view_banner(server: str, n_cmds: int) -> str:
    inner = 50
    lines = [
        "  ┌" + "─" * inner + "┐",
        f"  │  {'grpc-repl':^{inner - 2}}  │",
        f"  │  {server:^{inner - 2}}  │",
        f"  │  {f'{n_cmds} commands discovered':^{inner - 2}}  │",
        f"  │  {'type  help  or press Tab':^{inner - 2}}  │",
        "  └" + "─" * inner + "┘",
    ]
    return "\n".join(lines)


def view_prompt_tokens(
    server: str,
    namespace: tuple[str, ...] = (),
    wizard: bool = False,
):
    """
    Build prompt_toolkit (style, text) token list.

    Root:              [server]>
    use key:           [server] key>
    use key.search:    [server] key.search>
    wizard mode:       [server] (wizard)>
    """
    short = server.split(":")[0] if ":" in server else server
    tokens = [("class:server", f"[{short}]")]

    if wizard:
        tokens.append(("class:service", " (wizard)"))
    elif namespace:
        ns = ".".join(namespace)
        tokens.append(("class:service", f" {ns}"))

    tokens.append(("class:arrow", "> "))
    return tokens


# ──────────────────────────────────────────────────────────────────────────────
# KV (single message)
# ──────────────────────────────────────────────────────────────────────────────


def _render_kv(fields: list[FieldSpec], result: Any) -> str:
    if not isinstance(result, dict):
        return str(result)
    if not fields:
        return "\n".join(f"  {k}: {v}" for k, v in result.items())

    col_w = max((len(f.display_label()) for f in fields), default=8)
    lines = []
    for f in fields:
        raw = result.get(f.name)
        if raw is None:
            continue
        lines.append(f"  {f.display_label():<{col_w}}  {cell_value(raw, f.renderer)}")
    return "\n".join(lines) if lines else "(empty)"


# ──────────────────────────────────────────────────────────────────────────────
# Table (repeated message)
# ──────────────────────────────────────────────────────────────────────────────


def _render_table(fields: list[FieldSpec], spec: DisplaySpec, result: Any) -> str:
    rows: list[dict] = []
    if isinstance(result, list):
        rows = result
    elif isinstance(result, dict):
        for v in result.values():
            if isinstance(v, list):
                rows = v
                break
        if not rows:
            return _render_kv(fields, result)

    if not rows:
        return "  (no results)"

    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    field_map = {f.name: f for f in fields}

    header_row = [
        (field_map[c].display_label() if c in field_map else c) for c in col_names
    ]
    col_widths = [
        max(
            len(header_row[i]),
            max(
                (
                    len(
                        cell_value(
                            row.get(col, ""),
                            field_map[col].renderer
                            if col in field_map
                            else CellRenderer.DEFAULT,
                        )
                    )
                    for row in rows
                ),
                default=0,
            ),
        )
        for i, col in enumerate(col_names)
    ]

    sep = "  "
    lines = [
        "  " + sep.join(h.ljust(w) for h, w in zip(header_row, col_widths)),
        "  " + sep.join("─" * w for w in col_widths),
    ]
    for row in rows:
        cells = []
        for col, w in zip(col_names, col_widths):
            raw = resolve_path(row, col) if "." in col else row.get(col, "")
            r = field_map[col].renderer if col in field_map else CellRenderer.DEFAULT
            cells.append(cell_value(raw, r).ljust(w))
        lines.append("  " + sep.join(cells))

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _render_silent(spec: DisplaySpec, result: Any) -> str:
    if spec.success_message and spec.success_message != "✔":
        if isinstance(result, dict):
            from repl.schema.dsl import expand_template

            return "  " + expand_template(spec.success_message, result)
    return "  ✔"


def _json_block(result: Any) -> str:
    try:
        return json.dumps(result, indent=2)
    except (TypeError, ValueError):
        return str(result)
