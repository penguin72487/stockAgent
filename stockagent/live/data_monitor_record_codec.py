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

try:
    import msgspec as _native
except ImportError:
    _native = None


if _native is not None:
    class _FooterStats(_native.Struct, forbid_unknown_fields=True):
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
                observation.update(backend="msgspec_validated_metadata", fallback_reason=None)
            return encoded
    if observation is not None:
        observation.update(backend="stdlib", fallback_reason=reason)
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")
