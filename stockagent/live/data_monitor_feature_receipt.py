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

from stockagent.live.data_monitor_feature_pages import (
    valid_feature_page_preview,
    valid_feature_source_pages,
)


FEATURE_SOURCE_PAGES_MAX_BYTES = 8_000_000


def feature_source_signature(stat: os.stat_result) -> list[int]:
    return [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]


def feature_reuse_checksum(signature: list[int], digest: str, fields: int) -> str:
    encoded = json.dumps([3, signature, digest, fields], separators=(",", ":")).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def feature_observation_binding(signature: list[int], digest: str, fields: int, root: str) -> str | None:
    """Additive producer coherence proof; never changes the public v1 snapshot."""
    if (
        not isinstance(signature, list) or len(signature) != 5
        or any(type(value) is not int or value < 0 for value in signature)
        or not isinstance(digest, str) or len(digest) != 64
        or any(char not in "0123456789abcdef" for char in digest)
        or type(fields) is not int or not 0 <= fields <= 10_000_000
        or not isinstance(root, str) or len(root) != 32
        or any(char not in "0123456789abcdef" for char in root)
    ):
        return None
    return hashlib.sha256(json.dumps(
        ["source-observation-v1", signature, digest, fields, root], separators=(",", ":"),
    ).encode("ascii")).hexdigest()


def feature_revision_binding(
    signature: list[int], source_digest: str, fields: int,
    revision: str | None, source_metadata_sha256: str | None,
) -> str | None:
    """Bind a completed feature generation to its source and public labels."""

    if not isinstance(revision, str) or len(revision) != 32:
        return None
    values = [signature, source_digest, fields, revision]
    if source_metadata_sha256 is not None:
        values.append(source_metadata_sha256)
    return hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def feature_reuse_receipt_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.reuse.json")


def feature_source_pages_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.source_pages.json")


def feature_preview_checksum(source_digest: str, preview: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        [source_digest, preview], ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def trusted_feature_preview(
    path: Path, *, source_stat: os.stat_result,
) -> Mapping[str, Any] | None:
    """Read a bounded first page only after verifying its full source digest.

    The producer's optional preview never authorizes serving an unverified or
    changed full snapshot.  Missing proof falls back to the regular full parse.
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
                return None
            receipt_body = stream.read(2_000_001)
        if len(receipt_body) > 2_000_000:
            return None

        def reject_nonfinite(value: str) -> None:
            raise ValueError(f"non-finite preview JSON: {value}")

        receipt: Any = json.loads(receipt_body, parse_constant=reject_nonfinite)
        if not isinstance(receipt, Mapping):
            return None
        signature = feature_source_signature(source_stat)
        digest = receipt.get("source_sha256")
        fields = receipt.get("fields")
        preview = receipt.get("first_page_preview")
        if (
            receipt.get("schema_version") != 3
            or receipt.get("read_only") is not True
            or receipt.get("production_control_possible") is not False
            or receipt.get("source_signature") != signature
            or not isinstance(digest, str)
            or len(digest) != 64
            or type(fields) is not int
            or not 0 <= fields <= 10_000_000
            or receipt.get("receipt_sha256") != feature_reuse_checksum(signature, digest, fields)
            or not valid_feature_page_preview(preview, fields=fields)
            or receipt.get("first_page_preview_sha256") != feature_preview_checksum(digest, preview)
        ):
            return None
        source_hash = hashlib.sha256()
        with path.open("rb") as stream:
            if feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                source_hash.update(chunk)
            if feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
        if feature_source_signature(path.stat()) != signature or source_hash.hexdigest() != digest:
            return None
        return preview
    except (OSError, ValueError, UnicodeError, TypeError):
        return None


def trusted_feature_source_pages(
    path: Path, *, source_stat: os.stat_result,
) -> tuple[Mapping[str, list[Mapping[str, Any]]], Mapping[str, Any]] | None:
    """Accept bounded source rows only with the completed full-source proof.

    The optional projection never substitutes for the authoritative snapshot.
    Any changed, missing, or malformed member falls back to its full parser.
    """

    try:
        signature = feature_source_signature(source_stat)
        receipt_path = feature_reuse_receipt_path(path)
        with receipt_path.open("rb") as stream:
            receipt_stat = os.fstat(stream.fileno())
            if (
                receipt_stat.st_uid != source_stat.st_uid
                or source_stat.st_mode & 0o022
                or receipt_stat.st_mode & 0o022
            ):
                return None
            receipt_body = stream.read(2_000_001)
        if len(receipt_body) > 2_000_000:
            return None

        def reject_nonfinite(value: str) -> None:
            raise ValueError(f"non-finite feature source page JSON: {value}")

        receipt: Any = json.loads(receipt_body, parse_constant=reject_nonfinite)
        if not isinstance(receipt, Mapping):
            return None
        digest = receipt.get("source_sha256")
        fields = receipt.get("fields")
        preview = receipt.get("first_page_preview")
        pages_digest = receipt.get("source_pages_sha256")
        if (
            receipt.get("schema_version") != 3
            or receipt.get("read_only") is not True
            or receipt.get("production_control_possible") is not False
            or receipt.get("source_signature") != signature
            or not isinstance(digest, str)
            or len(digest) != 64
            or type(fields) is not int
            or not 0 <= fields <= 10_000_000
            or receipt.get("receipt_sha256") != feature_reuse_checksum(signature, digest, fields)
            or not valid_feature_page_preview(preview, fields=fields)
            or receipt.get("first_page_preview_sha256") != feature_preview_checksum(digest, preview)
            or not isinstance(pages_digest, str)
            or len(pages_digest) != 64
        ):
            return None
        pages_path = feature_source_pages_path(path)
        with pages_path.open("rb") as stream:
            pages_stat = os.fstat(stream.fileno())
            if pages_stat.st_uid != source_stat.st_uid or pages_stat.st_mode & 0o022:
                return None
            pages_body = stream.read(FEATURE_SOURCE_PAGES_MAX_BYTES + 1)
            if (
                len(pages_body) > FEATURE_SOURCE_PAGES_MAX_BYTES
                or feature_source_signature(pages_stat)
                != feature_source_signature(os.fstat(stream.fileno()))
            ):
                return None
        if (
            feature_source_signature(pages_path.stat()) != feature_source_signature(pages_stat)
            or hashlib.sha256(pages_body).hexdigest() != pages_digest
        ):
            return None
        projection: Any = json.loads(pages_body, parse_constant=reject_nonfinite)
        if (
            not isinstance(projection, Mapping)
            or projection.get("schema_version") != 1
            or projection.get("read_only") is not True
            or projection.get("production_control_possible") is not False
            or projection.get("source_signature") != signature
            or projection.get("source_sha256") != digest
            or projection.get("fields") != fields
            or not valid_feature_source_pages(projection.get("pages"), fields=fields)
        ):
            return None
        source_hash = hashlib.sha256()
        with path.open("rb") as stream:
            if feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                source_hash.update(chunk)
            if feature_source_signature(os.fstat(stream.fileno())) != signature:
                return None
        if feature_source_signature(path.stat()) != signature or source_hash.hexdigest() != digest:
            return None
        return projection["pages"], preview
    except (OSError, ValueError, UnicodeError, TypeError):
        return None


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
    "FEATURE_SOURCE_PAGES_MAX_BYTES",
    "feature_preview_checksum",
    "feature_revision_binding",
    "feature_reuse_checksum",
    "feature_reuse_receipt_path",
    "feature_source_pages_path",
    "feature_source_signature",
    "trusted_feature_preview",
    "trusted_feature_source_pages",
    "trusted_feature_snapshot",
]
