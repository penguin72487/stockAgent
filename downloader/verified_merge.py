"""Reuse a derived merge only after the caller verifies all source receipts.

This is an optimization of a mutable projection, not a source receipt or a
publication gate. Full output hashing is intentional: size/mtime alone cannot
detect a replaced file. Commit the small checkpoint after the output exists.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa

from downloader.artifact_io import atomic_write_json, sha256_file


def pandas_timestamp_schema(schema: pa.Schema) -> pa.Schema:
    """Match the former pandas round trip's timestamp metadata, without rows.

    The canonical Parquet writer uses millisecond timestamps. Older shards can
    still describe their original seconds unit in the pandas extension metadata.
    Arrow -> pandas -> Arrow formerly regenerated those descriptors from the
    physical timestamp types. Preserve that compatibility for columnar merges.
    """
    metadata = schema.metadata
    if not metadata or b"pandas" not in metadata:
        return schema
    pandas_metadata = json.loads(metadata[b"pandas"])
    changed = False
    for column in pandas_metadata.get("columns", []):
        index = schema.get_field_index(column["field_name"])
        if index < 0:
            continue
        dtype = schema.field(index).type
        if pa.types.is_timestamp(dtype):
            value = f"datetime64[{dtype.unit}]"
            changed |= column.get("numpy_type") != value
            column["numpy_type"] = value
    if not changed:
        return schema
    return schema.with_metadata({
        **metadata, b"pandas": json.dumps(pandas_metadata).encode(),
    })


class VerifiedMergeProjection:
    """Caller owns validation/locking; cache failure always means recompute."""

    def __init__(self, checkpoint: Path, target: Path, *, inputs: Any) -> None:
        self.checkpoint = checkpoint
        self.target = target
        encoded = json.dumps(inputs, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode()
        self.fingerprint = hashlib.sha256(encoded).hexdigest()

    def load(self) -> dict[str, Any] | None:
        try:
            saved = json.loads(self.checkpoint.read_bytes())
            result = saved["result"]
            before = self.target.stat()
            if (saved["schema_version"] != 1
                    or saved["inputs_sha256"] != self.fingerprint
                    or saved["target"] != str(self.target.resolve())
                    or before.st_size != result["output_bytes"]
                    or sha256_file(self.target) != result["output_sha256"]):
                return None
            after = self.target.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                return None
            return {**result, "merge_reused": True}
        except (OSError, ValueError, TypeError, KeyError):
            return None

    def save(self, result: dict[str, Any]) -> dict[str, Any]:
        # The writer already hashed its output. No observation/source is erased
        # on an interrupted checkpoint write; the next run simply recomputes.
        result = {**result, "merge_reused": False}
        try:
            atomic_write_json(self.checkpoint, {
                "schema_version": 1, "inputs_sha256": self.fingerprint,
                "target": str(self.target.resolve()), "result": result,
            })
        except OSError as exc:
            # The verified output remains usable. An optional optimization
            # checkpoint must not turn a completed acquisition into a failure.
            return {**result, "merge_cache_state": "unavailable", "merge_cache_error": type(exc).__name__}
        return result
