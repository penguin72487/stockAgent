"""Integrity receipt for the trusted producer's public feature snapshot.

This is not an authentication token.  It only permits the read-only gateway to
skip reparsing a large JSON file when the root-owned producer's completed
snapshot and sidecar still match byte for byte.  Missing or mismatched proofs
must fall back to the gateway's ordinary JSON contract validation.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping


def feature_source_signature(stat: os.stat_result) -> list[int]:
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def feature_reuse_checksum(signature: list[int], digest: str, fields: int) -> str:
    encoded = json.dumps([3, signature, digest, fields], separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def feature_reuse_receipt_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.reuse.json")


def trusted_feature_snapshot(
    path: Path, *, source_stat: os.stat_result, body: bytes
) -> bool:
    """Accept only a same-owner, non-writable sidecar for these exact bytes.

    The producer constructs the public DTO before writing the sidecar.  A
    missing/tampered sidecar never makes an invalid JSON body publicly valid.
    """

    try:
        receipt_path = feature_reuse_receipt_path(path)
        with receipt_path.open("rb") as stream:
            receipt_stat = os.fstat(stream.fileno())
            if (
                receipt_stat.st_uid != source_stat.st_uid
                or source_stat.st_mode & 0o022
                or receipt_stat.st_mode & 0o022
            ):
                return False
            receipt: Any = json.loads(stream.read())
        if not isinstance(receipt, Mapping):
            return False
        signature = feature_source_signature(source_stat)
        digest = receipt.get("source_sha256")
        fields = receipt.get("fields")
        if (
            receipt.get("schema_version") != 3
            or receipt.get("read_only") is not True
            or receipt.get("production_control_possible") is not False
            or receipt.get("source_signature") != signature
            or not isinstance(digest, str)
            or len(digest) != 64
            or not isinstance(fields, int)
            or isinstance(fields, bool)
            or not 0 <= fields <= 10_000_000
            or receipt.get("receipt_sha256")
            != feature_reuse_checksum(signature, digest, fields)
        ):
            return False
        return hashlib.sha256(body).hexdigest() == digest
    except (OSError, ValueError, UnicodeError, TypeError):
        return False


__all__ = [
    "feature_reuse_checksum",
    "feature_reuse_receipt_path",
    "feature_source_signature",
    "trusted_feature_snapshot",
]
