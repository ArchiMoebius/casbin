"""
repl/schema/dsl.py

DSL helpers: variable expansion, template rendering, value coercion.
Pure functions. No I/O.
"""
from __future__ import annotations

import re
import json
from typing import Any

from google.protobuf.descriptor_pb2 import FieldDescriptorProto

from repl.schema.ui_spec import FieldSpec

_VAR_RE = re.compile(r"\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


def expand_variables(text: str, variables: dict[str, str]) -> str:
    """Replace $VAR and ${VAR} with values from the variables dict."""
    def _replace(m: re.Match) -> str:
        name = m.group(1) or m.group(2)
        return variables.get(name, m.group(0))
    return _VAR_RE.sub(_replace, text)


def expand_request(request: dict[str, Any], variables: dict[str, str]) -> dict[str, Any]:
    """Recursively expand variables in all string values of a request dict."""
    result: dict[str, Any] = {}
    for k, v in request.items():
        if isinstance(v, str):
            result[k] = expand_variables(v, variables)
        elif isinstance(v, list):
            result[k] = [
                expand_variables(item, variables) if isinstance(item, str) else item
                for item in v
            ]
        else:
            result[k] = v
    return result


def expand_template(template: str, data: dict[str, Any]) -> str:
    """
    Replace {field_name} tokens in a template with values from data.
    E.g. "✔ {key} = {value}" with {"key": "foo", "value": "bar"} → "✔ foo = bar"
    """
    def _replace(m: re.Match) -> str:
        name = m.group(1)
        val  = data.get(name)
        if val is None:
            return m.group(0)
        return str(val)
    return re.sub(r"\{([^}]+)\}", _replace, template)


def coerce_value(raw_str: str, field: FieldSpec) -> Any:
    """Coerce a string (from wizard input) into the proper Python type."""
    from google.protobuf.descriptor_pb2 import FieldDescriptorProto as FDP

    if field.proto_type == FDP.TYPE_BOOL:
        return raw_str.strip().lower() not in ("false", "0", "no", "")

    if field.proto_type in (
        FDP.TYPE_INT32, FDP.TYPE_INT64, FDP.TYPE_UINT32, FDP.TYPE_UINT64,
        FDP.TYPE_SINT32, FDP.TYPE_SINT64, FDP.TYPE_FIXED32, FDP.TYPE_FIXED64,
        FDP.TYPE_SFIXED32, FDP.TYPE_SFIXED64,
    ):
        return int(raw_str.strip())

    if field.proto_type in (FDP.TYPE_FLOAT, FDP.TYPE_DOUBLE):
        return float(raw_str.strip())

    if field.proto_type == FDP.TYPE_BYTES:
        import base64
        return base64.b64decode(raw_str.strip())

    if field.proto_type == FDP.TYPE_MESSAGE:
        return json.loads(raw_str.strip())

    if field.is_repeated:
        # Split on commas for repeated strings
        return [v.strip() for v in raw_str.split(",") if v.strip()]

    return raw_str


def proto_type_label(field: FieldSpec) -> str:
    """Human-readable type label for a field, used in wizard prompts."""
    from repl.schema.parser import _proto_type_label
    return _proto_type_label(field)
