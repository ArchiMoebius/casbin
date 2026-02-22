"""
repl/schema/projection.py

Pure projection engine: raw RPC response + CompletionSource → CandidateRow[].
No I/O. No grpc. No prompt_toolkit.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from repl.schema.ui_spec import (
    CandidateRow,
    CellRenderer,
    CompletionDisplayFieldSpec,
    CompletionSource,
    FieldSpec,
)


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────


def project_candidates(
    response: dict,
    source: CompletionSource,
    candidate_field_specs: dict[str, FieldSpec],  # field_name → FieldSpec
) -> tuple[CandidateRow, ...]:
    """
    1. Walk source_field dot-path to the repeated value.
    2. Normalise: scalar list → list[dict]; message list → list[dict].
    3. Apply client-side filters.
    4. Project each item to CandidateRow.
    """
    raw_list = resolve_path(response, source.source_field)

    if raw_list is None:
        raw_list = []
    if not isinstance(raw_list, list):
        raw_list = [raw_list]

    # Normalise scalars → {"_value": scalar}
    is_scalar = raw_list and not isinstance(raw_list[0], dict)
    items: list[dict] = (
        [{"_value": v} for v in raw_list] if is_scalar else list(raw_list)
    )

    # Apply client-side filters
    for filt in source.filters:
        items = [item for item in items if filt.test(item)]

    # Resolve display columns
    if source.display_fields:
        disp_specs = source.display_fields
    elif not is_scalar and candidate_field_specs:
        visible = [fs for fs in candidate_field_specs.values() if not fs.hidden]
        disp_specs = tuple(
            CompletionDisplayFieldSpec(
                path=fs.name,
                label=fs.display_label(),
                width=fs.column_width,
                renderer=fs.renderer,
            )
            for fs in visible
        )
    else:
        disp_specs = ()

    rows: list[CandidateRow] = []
    for item in items:
        insert = _resolve_insert(item, source.value_field, is_scalar)
        cols = _render_display_cols(item, disp_specs)
        rows.append(CandidateRow(insert_value=insert, display_cols=cols, raw=item))

    return tuple(rows)


# ──────────────────────────────────────────────────────────────────────────────
# Dot-path traversal
# ──────────────────────────────────────────────────────────────────────────────


def resolve_path(obj: Any, path: str) -> Any:
    """
    Traverse a dot-path through nested dicts/lists.
    Examples:
      resolve_path({"a": {"b": 1}}, "a.b")  → 1
      resolve_path({"entries": [...]}, "entries") → [...]
    """
    if not path or obj is None:
        return obj

    parts = path.split(".", 1)
    head = parts[0]
    tail = parts[1] if len(parts) > 1 else ""

    if isinstance(obj, dict):
        child = obj.get(head)
    elif isinstance(obj, list):
        # If the current node is a list, apply path to each element and flatten
        return [resolve_path(item, path) for item in obj]
    else:
        return None

    if not tail:
        return child
    return resolve_path(child, tail)


# ──────────────────────────────────────────────────────────────────────────────
# Cell rendering
# ──────────────────────────────────────────────────────────────────────────────


def cell_value(raw: Any, renderer: CellRenderer = CellRenderer.DEFAULT) -> str:
    if raw is None:
        return ""

    if renderer == CellRenderer.TIMESTAMP:
        try:
            return datetime.fromtimestamp(int(raw)).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError, OSError):
            return str(raw)

    if renderer == CellRenderer.BOOL_ICON:
        if isinstance(raw, bool):
            return "✔" if raw else "✘"
        return "✔" if str(raw).lower() in ("true", "1", "yes") else "✘"

    if renderer == CellRenderer.BYTES:
        if isinstance(raw, (bytes, bytearray)):
            return raw.hex()
        return str(raw)

    if renderer == CellRenderer.JSON:
        try:
            return json.dumps(raw, separators=(",", ":"))
        except (TypeError, ValueError):
            return str(raw)

    # DEFAULT
    if isinstance(raw, bool):
        return str(raw).lower()
    return str(raw)


# ──────────────────────────────────────────────────────────────────────────────
# Internals
# ──────────────────────────────────────────────────────────────────────────────


def _resolve_insert(item: dict, value_field: Optional[str], is_scalar: bool) -> str:
    if is_scalar:
        return str(item.get("_value", ""))
    if value_field:
        return str(resolve_path(item, value_field) or "")
    # Fallback: first non-None value
    for v in item.values():
        if v is not None:
            return str(v)
    return ""


def _render_display_cols(
    item: dict,
    specs: tuple[CompletionDisplayFieldSpec, ...],
) -> tuple[tuple[str, str], ...]:
    cols: list[tuple[str, str]] = []
    for spec in specs:
        raw = resolve_path(item, spec.path)

        # Repeated field → join
        if isinstance(raw, list):
            text = spec.join_separator.join(cell_value(v) for v in raw)
        else:
            text = cell_value(raw, spec.renderer)

        # Pad/truncate to declared width
        if spec.width:
            text = text[: spec.width].ljust(spec.width)

        cols.append((text, spec.style or "class:completion-meta"))
    return tuple(cols)
