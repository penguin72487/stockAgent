"""Optional private-cache decoder with Python's exact integer semantics.

Public wire bytes keep their canonical owners. Untyped msgspec decoding retains
arbitrary Python integers. General encoding stays with stdlib; the guarded
metadata path excludes floats, large integers and custom value/key semantics.
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

try:
    import msgspec as _native
except ImportError:
    _native = None


def native_available() -> bool:
    return _native is not None


def loads(body: bytes) -> Any:
    if _native is not None:
        try:
            return _native.json.decode(body)
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    return json.loads(body)


@lru_cache(maxsize=32)
def _object_projection(fields: tuple[str, ...]):
    # Any retains Python ints, float bits and legacy value types. Projection
    # avoids allocating unused row fields; it never coerces financial values.
    shape = _native.defstruct("PrivateJSONProjection", [(name, Any, None) for name in fields])
    return _native.json.Decoder(shape)


def projected_object_values(body: bytes | str, fields: tuple[str, ...]) -> tuple | None:
    """Read named values from an object, with the same legacy fallback as loads.

    Missing fields are None and non-object documents are ignored. The caller
    supplies the complete projection; source documents and wire bytes stay intact.
    """
    if _native is not None:
        try:
            return _native.structs.astuple(_object_projection(fields).decode(body))
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    value = loads(body)
    return tuple(value.get(field) for field in fields) if isinstance(value, dict) else None


def projected_object_rows(lines: list[str], fields: tuple[str, ...]):
    """Project a bounded group of strict JSONL records in one native call.

    ``decode_lines`` also accepts several documents on one line and multiline
    documents. Check object-line boundaries and record count before using it;
    legacy/non-object/invalid groups retain the exact single-line fallback.
    The caller owns the bounded read size. No numeric field is coerced.
    """
    if _native is not None:
        # Strip JSON whitespace only. str.strip() would also remove e.g. NBSP
        # or form feed and turn a legacy-invalid record into a valid object.
        populated = [value for line in lines if (value := line.strip(" \t\r\n"))]
        if populated and all(line.startswith("{") and line.endswith("}") for line in populated):
            try:
                values = _object_projection(fields).decode_lines("\n".join(populated))
            except (ValueError, TypeError, OverflowError, RecursionError):
                pass
            else:
                if len(values) == len(populated):
                    yield from (_native.structs.astuple(value) for value in values)
                    return
    for line in lines:
        if line.strip():
            values = projected_object_values(line, fields)
            if values is not None:
                yield values


def dumps(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def _plain_metadata(value: Any, depth: int = 0) -> bool:
    kind = type(value)
    if kind is str or kind is bool or value is None:
        return True
    if kind is int:
        return -(2**63) <= value <= 2**64 - 1
    if depth >= 64:
        return False
    if kind is list:
        return all(_plain_metadata(item, depth + 1) for item in value)
    if kind is dict:
        return all(type(key) is str and _plain_metadata(item, depth + 1)
                   for key, item in value.items())
    return False


def pretty_metadata_bytes(value: Any) -> bytes:
    """Keep stdlib indent-2 UTF-8 bytes for guarded native metadata encoding.

    Only exact builtin strings, bounded integers, booleans and null enter the
    native encoder. Large integers, custom containers, floats, deep trees and
    legacy inputs retain the original pretty encoder and its errors. No trailing newline
    is added; the artifact owner controls publication and durability.
    """
    if _native is not None:
        try:
            if _plain_metadata(value):
                compact = _native.json.encode(value)
                return _native.json.format(compact, indent=2)
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    return json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
