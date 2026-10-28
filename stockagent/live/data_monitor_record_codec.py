"""Lossless native encoding for the private, integer/string footer cache only.

This is not the public wire encoder or a replacement for ``private_json``.
The native encoder would turn NaN/Inf into null and supports more Python types
than stdlib JSON. Prove the complete known metadata schema first; any legacy
or unknown shape keeps the original stdlib behavior and errors. Encode the
original object, not the converted validation copy, so no defaults are added,
unknown keys dropped, or dictionary order changed. UTF-8 JSON is reader-
compatible with the former ASCII-escaped cache; source files are not changed.
"""

from __future__ import annotations

import json
from typing import Any

from stockagent import private_json

try:
    import msgspec as _native
except ImportError:
    _native = None


if _native is not None:
    class _FooterStats(_native.Struct, forbid_unknown_fields=True, gc=False):
        count: int
        first: str | None
        last: str | None
        time_column: str | None
        schema_id: str
        non_null: list[int | None]

    class _FileEntry(_native.Struct, forbid_unknown_fields=True):
        size: int
        mtime_ns: int
        file_identity: list[int] | None = None
        stats: _FooterStats | None = None
        error: str | None = None

    class _Dataset(_native.Struct, forbid_unknown_fields=True):
        count: int | None
        verified_partial_count: int | None
        first: str | None
        last: str | None
        time_column: str | None
        bounds_state: str
        files_inspected: int
        files_total: int
        invalid_files: int
        state: str
        basis: str
        bounds_precision: str | None = None
        missing_families: int | None = None

    class _RecordCache(_native.Struct, forbid_unknown_fields=True):
        version: int
        files: dict[str, _FileEntry]
        schemas: dict[str, list[list[str]]]
        datasets: dict[str, _Dataset]
        selection_membership: str
        dataset_membership_version: int
        dataset_memberships: dict[str, str]
        source_observation_root: str
        source_observation_matches_cache: bool
        recently_refreshed_files: list[str]
        feature_revision: str


def encode_record_cache(
    payload: Any, *, observation: dict[str, Any] | None = None,
) -> bytes:
    """Encode compatible metadata fast, retaining legacy JSON on any mismatch.

    Strict validation has no float target and forbids unknown fields at every
    object boundary. Native conversion is only a type proof, never an input
    coercion or a data-quality/completeness verdict. Missing dependency and
    validation/encoding errors leave the existing writer available. Memory or
    I/O failures are not hidden, and stdlib's own errors still propagate.
    """
    reason = "native_unavailable"
    if _native is not None:
        try:
            _native.convert(payload, type=_RecordCache, strict=True)
            encoded = _native.json.encode(payload)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
            reason = type(exc).__name__
        else:
            if observation is not None:
                observation.update(
                    backend="msgspec_validated_metadata", fallback_reason=None,
                    temporary_struct_gc_tracking=_RecordCache.__struct_config__.gc,
                )
            return encoded
    if observation is not None:
        observation.update(backend="stdlib", fallback_reason=reason)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


_FOOTER_KEYS = ("count", "first", "last", "time_column", "schema_id", "non_null")


def decode_schema_references(body: bytes, codecs: dict[str, int]) -> tuple[dict, bool]:
    """Validate all intern references in C, retaining legacy section values."""
    if _native is not None and all(codec == 1 for codec in codecs.values()):
        return _native.json.decode(body, type=dict[str, list[int]]), True
    return private_json.loads(body), False


def encode_footer_stats(value: dict) -> tuple[bytes, int]:
    """Encode a witnessed stats block once, retaining legacy values exactly.

    Codec 1 is a type proof for scalar metadata and an integer/null vector.
    It allows readers to copy a vector in C rather than decoding its cells
    again for every referring file. Other shapes use the original codec.
    """
    if _native is not None and type(value) is dict:
        try:
            proven = _native.convert(value, type=_FooterStats, strict=True)
            return _native.json.encode(proven), 1
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
            pass
    return private_json.dumps(value), 0


def decode_footer_stats(body: bytes, codec: int) -> dict:
    """Read a storage block; neither codec coerces missing or legacy values."""
    if codec == 0:
        value = private_json.loads(body)
        if type(value) is not dict:
            raise ValueError("invalid footer stats block")
        return value
    if codec != 1:
        raise ValueError("unsupported footer stats codec")
    if _native is not None:
        value = _native.json.decode(body, type=_FooterStats)
        return _native.structs.asdict(value)
    value = private_json.loads(body)
    if (type(value) is not dict or set(value) != set(_FOOTER_KEYS)
            or type(value["count"]) is not int or type(value["schema_id"]) is not str
            or any(value[key] is not None and type(value[key]) is not str for key in ("first", "last", "time_column"))
            or type(value["non_null"]) is not list
            or any(item is not None and type(item) is not int for item in value["non_null"])):
        raise ValueError("invalid typed footer stats block")
    return value


def decode_footer_stats_pool(body: bytes, codecs: dict[str, int], *, typed_decode=False) -> dict:
    """Batch one complete stats pool, retaining the per-block type proofs."""
    if (typed_decode and _native is not None and codecs
            and all(type(codec) is int and codec == 1 for codec in codecs.values())):
        try:
            decoded = _native.json.decode(body, type=dict[str, _FooterStats])
        except (ValueError, TypeError, OverflowError, RecursionError):
            # Keep arbitrary-size integer and legacy error behavior. Direct
            # typed decoding may have a narrower integer range than convert.
            pass
        else:
            if decoded.keys() != codecs.keys():
                raise ValueError("invalid footer stats pool")
            return {key: _native.structs.asdict(value) for key, value in decoded.items()}
    values = private_json.loads(body)
    if type(values) is not dict or values.keys() != codecs.keys() or any(type(value) is not dict for value in values.values()):
        raise ValueError("invalid footer stats pool")
    typed = {key: values[key] for key, codec in codecs.items() if codec == 1}
    if any(type(codec) is not int or codec not in (0, 1) for codec in codecs.values()):
        raise ValueError("unsupported footer stats codec")
    if _native is not None:
        # One C call validates every unique vector. Do not create a decoder
        # call and temporary Struct/dict for each of tens of thousands of rows.
        _native.convert(typed, type=dict[str, _FooterStats], strict=True)
    else:
        for value in typed.values():
            if (set(value) != set(_FOOTER_KEYS) or type(value["count"]) is not int
                    or type(value["schema_id"]) is not str
                    or any(value[key] is not None and type(value[key]) is not str for key in ("first", "last", "time_column"))
                    or type(value["non_null"]) is not list
                    or any(item is not None and type(item) is not int for item in value["non_null"])):
                raise ValueError("invalid typed footer stats block")
    return values
