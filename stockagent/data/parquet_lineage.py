"""Byte-exact lineage for legacy or explicit partitioned Parquet sources.

An ordered member inventory is not a file hash. New receipts have a named
source field; existing SHA fields always retain their original byte meaning.
Paths are source-parent-relative so a fixed cold release can be relocated.
"""
from __future__ import annotations

from pathlib import Path, PurePosixPath
import re

from stockagent.data.partitioned_parquet import CONTRACT as PARTITION_CONTRACT, ParquetSource, logical_name, resolve_source
from stockagent.data_sync.immutable_cache_files import stable_payload_hash

CONTRACT = "parquet_byte_lineage_v1"


class ParquetLineage:
    def __init__(self, path):
        self.source = ParquetSource(path)
        source = self.source
        self.identity = {"contract": CONTRACT, "kind": "file" if source.manifest is None else "partitioned",
                         "logical_name": logical_name(source.path), "rows": source.rows}
        if source.manifest is None:
            self.identity.update(bytes=source.before["size"], sha256=stable_payload_hash(source.path, source.before))
        else:
            self.identity.update(manifest_contract=PARTITION_CONTRACT,
                files=[{"path": Path(path).relative_to(source.path.parent).as_posix(), "bytes": size, "sha256": digest}
                       for path, size, digest in source.fingerprints()])
        self.verify()

    def verify(self):
        if resolve_source(self.source.path) != self.source.path:
            raise ValueError("Parquet source ABI changed during lineage read")
        self.source.verify()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.verify()


def receipt_fields(identity, *, hash_key="sha256", source_key="source"):
    """Preserve old receipt ABI for an actual legacy byte file only."""
    if identity["kind"] == "file":
        return {hash_key: identity["sha256"]}
    return {source_key: identity}


def assert_receipt(identity, receipt, *, hash_key="sha256", source_key="source"):
    expected = receipt_fields(identity, hash_key=hash_key, source_key=source_key)
    if (not isinstance(receipt, dict) or any(receipt.get(key) != value for key, value in expected.items())
            or (identity["kind"] == "partitioned" and hash_key in receipt)
            or (identity["kind"] == "file" and source_key in receipt)):
        raise RuntimeError("Parquet source lineage mismatch; raw SHA cannot represent a partitioned source")


def inventory_hashes(identity, logical_relative):
    """Expand only exact lineage members, never a glob over old generations."""
    if not isinstance(identity, dict):
        raise ValueError("invalid Parquet byte lineage")
    logical = PurePosixPath(logical_relative)
    if logical.is_absolute() or ".." in logical.parts or logical.name != identity.get("logical_name"):
        raise ValueError("lineage logical path is unsafe or differs")
    if identity.get("contract") != CONTRACT or identity.get("kind") not in {"file", "partitioned"}:
        raise ValueError("unsupported Parquet byte lineage contract")
    if identity["kind"] == "file":
        entries = [{"path": logical.name, "sha256": identity["sha256"]}]
    else:
        if identity.get("manifest_contract") != PARTITION_CONTRACT:
            raise ValueError("unsupported partition lineage contract")
        entries = identity.get("files")
        if not isinstance(entries, list) or not 1 <= len(entries) <= 4097:
            raise ValueError("partition lineage member bound")
    result = {}
    pointer = logical.name + ".manifest.json"
    for index, entry in enumerate(entries):
        name, digest = entry.get("path"), entry.get("sha256")
        if not isinstance(name, str) or not re.fullmatch(r"[a-f0-9]{64}", str(digest)):
            raise ValueError("invalid lineage member/hash")
        if identity["kind"] == "partitioned":
            if ((index == 0 and name != pointer) or (index > 0 and not re.fullmatch(
                    re.escape(pointer) + r"\.parts/p_[a-f0-9]{64}_[a-f0-9]{32}\.parquet", name))):
                raise ValueError("lineage member escaped its source namespace")
        relative = (logical.parent / name).as_posix()
        if relative in result:
            raise ValueError("duplicate lineage member")
        result[relative] = digest
    return result


def release_inventory(receipt, *, logical_relative="features/tw_public_stock_daily.parquet"):
    """Read new source lineage, or the unchanged old byte-SHA release ABI."""
    identity = receipt.get("base_feature_source")
    if identity is not None:
        if (not isinstance(identity, dict) or "base_feature_sha256" in receipt
                or identity.get("kind") != "partitioned"):
            raise ValueError("ambiguous release lineage")
        return inventory_hashes(identity, logical_relative)
    digest = receipt.get("base_feature_sha256")
    if not re.fullmatch(r"[a-f0-9]{64}", str(digest)):
        raise ValueError("missing legacy base byte SHA")
    return {logical_relative: digest}
