"""Optional private-cache decoder with Python's exact integer semantics.

Public wire bytes and canonical hashes keep their existing owners. Untyped
msgspec decoding retains arbitrary Python integers. Only guarded metadata and
private ledger encoding use native output; general encoding stays with stdlib.
"""

from __future__ import annotations

import json
import math
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


def _plain_string_keys(value: dict) -> bool:
    """Exact str keys, without a Python generator step for every key.

    Type identities, rather than the type objects themselves, avoid invoking
    a user-defined metaclass hash/equality while rejecting legacy custom keys.
    This is the same predicate as all(type(key) is str for key in value).
    """
    return set(map(id, map(type, value))).issubset({id(str)})


def _plain_finite_json(value: Any, depth: int = 0) -> bool:
    """Exclude native conversions, nonfinite-to-null and custom container rules."""
    # Most ledger fields are scalars. Do not allocate a recursive generator
    # and call this Python function for every string/bool/float in thousands
    # of rows; stack only containers while checking scalar leaves inline.
    # The depth boundary and exact-builtin rule remain the same as before.
    pending = [(value, depth)]
    while pending:
        current, level = pending.pop()
        kind = type(current)
        if current is None or kind in (str, bool, int):
            continue
        if kind is float:
            if not math.isfinite(current):
                return False
            continue
        if level >= 64:
            return False
        if kind is dict:
            if not _plain_string_keys(current):
                return False
            children = current.values()
        elif kind in (list, tuple):
            children = current
        else:
            return False
        for item in children:
            item_kind = type(item)
            if item_kind is float:
                if not math.isfinite(item):
                    return False
            elif item is not None and item_kind not in (str, bool, int):
                pending.append((item, level + 1))
    return True


def ledger_bytes(value: Any) -> bytes:
    """Compact sorted private paper JSON, preserving decoded financial values.

    This is not a public-wire or canonical-hash encoder: finite float spelling
    may differ from stdlib, but integers, float bits, strings and order survive
    readback. Only exact builtin trees enter the native encoder. Nonfinite
    numbers, custom types/keys, cycles and legacy values retain stdlib's
    ``default=str`` behavior and errors. The caller still owns append/fsync and
    atomic publication; old ledger prefixes are never re-encoded.
    """
    if _native is not None:
        try:
            if _plain_finite_json(value):
                return _native.json.encode(value, order="sorted")
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str,
                      separators=(",", ":")).encode("utf-8")


def ledger_lines_bytes(values) -> bytes:
    """Encode a complete private JSONL batch before its writer touches a file.

    Native encode_lines retains the per-record sorted codec, but uses one
    encoder/buffer rather than allocating bytes and a Python call per row.
    Guard rows at their ORIGINAL depth; wrapping them in an array would change
    the legacy depth boundary. Mixed/custom/nonfinite or unsupported native
    input uses the exact existing individual encoder, including its errors.
    This helper owns no file, transaction, durability or public-wire contract.
    """
    rows = tuple(values)
    if _native is not None and all(_plain_finite_json(row) for row in rows):
        try:
            encoder = _native.json.Encoder(order="sorted")
            if hasattr(encoder, "encode_lines"):
                return encoder.encode_lines(rows)
        except (ValueError, TypeError, OverflowError, RecursionError):
            pass
    return b"".join(ledger_bytes(row) + b"\n" for row in rows)


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
