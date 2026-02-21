"""
repl/view/response.py  (and sub-modules inlined for brevity)

Pure render functions. Return strings. No I/O, no grpc, no proto imports.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from repl.schema.projection import cell_value, resolve_path
from repl.schema.ui_spec import (
    CandidateRow, CellRenderer, CmdNode, DisplayMode, DisplaySpec,
    FieldSpec, UISpec,
)


# ──────────────────────────────────────────────────────────────────────────────
# Top-level dispatcher
# ──────────────────────────────────────────────────────────────────────────────

def view_response(node: Optional[CmdNode], result: Any) -> str:
    if node is None:
        return _json_block(result)

    spec   = node.display
    fields = [f for f in node.output_fields if not f.hidden]
    mode   = spec.mode

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
        # Single-message path (used by bootstrap)
        return _render_kv(fields, result)

    # AUTO fallback
    if isinstance(result, list):
        return _render_table(fields, spec, result)
    if isinstance(result, dict):
        return _render_kv(fields, result)
    return str(result)


def view_stream_chunk(node: Optional[CmdNode], chunk: Any, index: int) -> str:
    fields = [f for f in node.output_fields if not f.hidden] if node else []
    spec   = node.display if node else DisplaySpec(mode=DisplayMode.STREAM)

    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    field_map  = {f.name: f for f in fields}

    if not col_names:
        return f"[{index}] {json.dumps(chunk)}"

    parts = []
    for col in col_names:
        fs  = field_map.get(col)
        raw = chunk.get(col, "")
        val = cell_value(raw, fs.renderer if fs else CellRenderer.DEFAULT)
        parts.append(val)

    sep   = spec.stream_separator or "│"
    row   = f"  {sep}  ".join(parts)
    ts    = datetime.now().strftime("%H:%M:%S")
    return f"[{index:>4}] {ts}  {row}"


def view_stream_header(node: Optional[CmdNode]) -> Optional[str]:
    """Column header line for streaming output."""
    if node is None:
        return None
    spec  = node.display
    fields = [f for f in node.output_fields if not f.hidden]
    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    if not col_names:
        return None
    field_map = {f.name: f for f in fields}
    headers   = []
    for col in col_names:
        fs = field_map.get(col)
        label = fs.display_label() if fs else col
        w     = fs.column_width if fs else 0
        headers.append(label.ljust(w) if w else label)
    return "  │  ".join(f"{'':6}  " + "  ".join(h for h in headers))


def view_help(ui_spec: UISpec, prefix: str = "") -> str:
    cmds = ui_spec.user_commands()
    if prefix:
        cmds = [c for c in cmds if c.cmd.startswith(prefix)]

    if not cmds:
        return f"  No commands matching '{prefix}'."

    lines = ["  Commands:\n"]
    max_cmd = max(len(c.cmd) for c in cmds)

    for node in sorted(cmds, key=lambda n: n.cmd):
        aliases = f"  ({', '.join(node.aliases)})" if node.aliases else ""
        flags   = "  ".join(f"--{f.name}" for f in node.input_fields if not f.hidden)
        stream  = " [streaming]" if node.server_streaming else ""
        desc    = node.description or ""
        lines.append(
            f"  {node.cmd:<{max_cmd}}  {aliases:<20}{desc}{stream}"
        )
        if flags:
            lines.append(f"  {'':>{max_cmd}}  flags: {flags}")
        lines.append("")

    return "\n".join(lines)


def view_error(msg: str) -> str:
    return f"  ❌  {msg}"


def view_banner(server: str, n_cmds: int) -> str:
    width = 54
    inner = width - 2
    lines = [
        "  ┌" + "─" * inner + "┐",
        f"  │  {'grpc-repl':^{inner - 2}}  │",
        f"  │  {server:^{inner - 2}}  │",
        f"  │  {f'{n_cmds} commands discovered':^{inner - 2}}  │",
        f"  │  {'type  help  to get started':^{inner - 2}}  │",
        "  └" + "─" * inner + "┘",
    ]
    return "\n".join(lines)


def view_prompt_text(server: str, mode: str = "") -> str:
    """Plain-text prompt (used when FormattedText unavailable)."""
    short = server.split(":")[0] if ":" in server else server
    if mode:
        return f"[{short}|{mode}]> "
    return f"[{short}]> "


def view_prompt_tokens(server: str, mode: str = ""):
    """prompt_toolkit (style, text) token list for the prompt."""
    tokens = [("class:server", f"[{server}]")]
    if mode:
        tokens.append(("class:service", f" {mode}"))
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
        val = cell_value(raw, f.renderer)
        lines.append(f"  {f.display_label():<{col_w}}  {val}")
    return "\n".join(lines) if lines else "(empty)"


# ──────────────────────────────────────────────────────────────────────────────
# Table (repeated message)
# ──────────────────────────────────────────────────────────────────────────────

def _render_table(fields: list[FieldSpec], spec: DisplaySpec, result: Any) -> str:
    # Find the repeated field
    rows: list[dict] = []
    if isinstance(result, list):
        rows = result
    elif isinstance(result, dict):
        # Find first repeated value
        for v in result.values():
            if isinstance(v, list):
                rows = v
                break
        if not rows:
            return _render_kv(fields, result)

    if not rows:
        return "  (no results)"

    col_names = list(spec.columns) if spec.columns else [f.name for f in fields]
    field_map  = {f.name: f for f in fields}

    # Compute column widths
    header_row = [
        (field_map[c].display_label() if c in field_map else c)
        for c in col_names
    ]
    col_widths = [
        max(
            len(header_row[i]),
            max(
                (len(cell_value(
                    row.get(col, ""),
                    field_map[col].renderer if col in field_map else CellRenderer.DEFAULT
                )) for row in rows),
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
            renderer = field_map[col].renderer if col in field_map else CellRenderer.DEFAULT
            cells.append(cell_value(raw, renderer).ljust(w))
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
