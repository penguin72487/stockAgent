"""Private footer-cache prototype: validate counts once, expand only on demand.

The on-disk JSON and its canonical schema remain owned by record_codec. Raw
fragments are never trusted input: every count vector is strictly decoded and
canonically re-encoded before it becomes an immutable, internal value. Unknown
or legacy metadata falls back to the original decoder/writer, without coercion.
Rejected in the complete replay; this benchmark-only module is not a live reader.
"""

from __future__ import annotations

import json
from typing import Any

from stockagent import private_json
from stockagent.live import data_monitor_record_codec as codec


_native = codec._native
_legacy_loads = private_json.loads


class DeferredCounts:
    """An immutable, fully validated JSON integer/null vector, not an estimate.

    Iteration expands once per iterator. FeatureDataset's zip consumes one
    iterator per member, so this does not decode once for every field or retain
    expanded counts across unrelated datasets. Equality of two canonical
    vectors needs neither expansion nor floating-point arithmetic.
    """

    __slots__ = ("_encoded", "_length")

    def __init__(self, raw: bytes) -> None:
        if _native is None:
            raise TypeError("native count validation is unavailable")
        values = _native.json.decode(raw, type=list[int | None], strict=True)
        object.__setattr__(self, "_encoded", _native.json.encode(values))
        object.__setattr__(self, "_length", len(values))

    def __setattr__(self, name: str, value: Any) -> None:
        raise AttributeError("validated count vectors are immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("validated count vectors are immutable")

    def __len__(self) -> int:
        return self._length

    def to_list(self) -> list[int | None]:
        return (json.loads(self._encoded) if _native is None else
                _native.json.decode(self._encoded, type=list[int | None], strict=True))

    def __iter__(self):
        return iter(self.to_list())

    def __eq__(self, other: object) -> bool:
        if type(other) is DeferredCounts:
            return self._encoded == other._encoded
        if isinstance(other, list):
            return self.to_list() == other
        return NotImplemented

    __hash__ = None


def is_count_vector(value: Any) -> bool:
    """Keep the legacy list guard; admit only our validated internal type."""
    return isinstance(value, list) or type(value) is DeferredCounts


def _replace_fields(struct: type, name: str, replacements: dict[str, Any]) -> type:
    fields = []
    for field in _native.structs.fields(struct):
        item = (field.name, replacements.get(field.name, field.type))
        if field.default is not _native.NODEFAULT:
            item += (field.default,)
        fields.append(item)
    return _native.defstruct(name, fields, forbid_unknown_fields=True, module=__name__)


if _native is not None:
    # Derive the proof from the single canonical schema, not a second hand-
    # maintained contract. The proof is never the object that gets persisted.
    _DeferredFooterStats = _replace_fields(codec._FooterStats, "_DeferredFooterStats", {"non_null": DeferredCounts})
    _DeferredFileEntry = _replace_fields(codec._FileEntry, "_DeferredFileEntry", {"stats": _DeferredFooterStats | None})
    _DeferredRecordCache = _replace_fields(codec._RecordCache, "_DeferredRecordCache", {"files": dict[str, _DeferredFileEntry]})


def _validate_vector(expected: type, value: Any) -> DeferredCounts:
    if expected is not DeferredCounts:
        raise TypeError("unknown private metadata type")
    if type(value) is DeferredCounts:
        return value
    values = _native.convert(value, type=list[int | None], strict=True)
    return DeferredCounts(_native.json.encode(values))


def decode_record_cache(body: bytes, *, observation: dict[str, Any] | None = None) -> Any:
    """Preserve object key order/omissions, and never drop unknown fields."""
    reason = "native_unavailable"
    if _native is not None:
        try:
            root = _native.json.decode(body, type=dict[str, _native.Raw], strict=True)
            raw_files = root.get("files")
            if raw_files is None:
                raise ValueError("not a record cache")
            file_fields = _native.json.decode(raw_files, type=dict[str, dict[str, _native.Raw]], strict=True)
            files = {}
            for path, fields in file_fields.items():
                entry = {}
                for name, raw in fields.items():
                    if name == "stats" and bytes(raw) != b"null":
                        stats = _native.json.decode(raw, type=dict[str, _native.Raw], strict=True)
                        entry[name] = {key: DeferredCounts(value) if key == "non_null" else _native.json.decode(value)
                                       for key, value in stats.items()}
                    else:
                        entry[name] = _native.json.decode(raw)
                files[path] = entry
            payload = {key: files if key == "files" else _native.json.decode(value)
                       for key, value in root.items()}
            _native.convert(payload, type=_DeferredRecordCache, strict=True, dec_hook=_validate_vector)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
            reason = type(exc).__name__
        else:
            if observation is not None:
                observation.update(backend="msgspec_deferred_counts", fallback_reason=None)
            return payload
    if observation is not None:
        observation.update(backend="private_json", fallback_reason=reason)
    return _legacy_loads(body)


def _raw_vector(value: Any):
    if type(value) is DeferredCounts:
        return _native.Raw(value._encoded)
    raise TypeError("unsupported private metadata type")


def _legacy_vector(value: Any):
    if type(value) is DeferredCounts:
        return value.to_list()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def encode_record_cache(payload: Any, *, observation: dict[str, Any] | None = None) -> bytes:
    """Use validated raw vectors; legacy/unknown types retain stdlib behavior."""
    reason = "native_unavailable"
    if _native is not None:
        try:
            _native.convert(payload, type=_DeferredRecordCache, strict=True, dec_hook=_validate_vector)
            body = _native.json.encode(payload, enc_hook=_raw_vector)
        except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
            reason = type(exc).__name__
        else:
            if observation is not None:
                observation.update(backend="msgspec_validated_deferred_counts", fallback_reason=None)
            return body
    if observation is not None:
        observation.update(backend="stdlib", fallback_reason=reason)
    return json.dumps(payload, separators=(",", ":"), default=_legacy_vector).encode("utf-8")
