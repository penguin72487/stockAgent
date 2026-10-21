"""Reuse row-local canonical parsers on exact original Parquet groups.

This is a derived cache, not another collector, source clock or public dataset.
The source owner still verifies its full source receipts before/after use. Only
row-local transforms may opt in; rolling/aggregate/lifecycle logic stays with
the original whole-frame builder. Changed physical layouts are safe misses.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_public_incremental import _encoded_row_groups, _generation, table_content_sha256
from stockagent.data_sync.cache_capacity import GenerationWriteBudget, reserve_cache_write
from stockagent.data_sync.factorized_projection_reuse import seal_new_file
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash

CONTRACT = "canonical_row_local_parquet_group_projection_v1"
MAX_INDEX_BYTES = 16 * 1024**2
MAX_INDEX_MEMBERS = 32_768


class ParquetGroupProjection:
    def __init__(self, root, implementation, *, capacity_policy=None):
        self.root = Path(root).absolute()
        if self.root.resolve() != self.root:
            raise ValueError("redirected source projection cache")
        self.implementation = implementation
        self.capacity_policy = capacity_policy
        self.work = {"contract": CONTRACT, "groups_reused": 0, "groups_built": 0,
            "reused_source_rows": 0, "parsed_source_rows": 0, "rejected_members": 0,
            "compressed_proof_bytes": 0, "unsupported_sources": 0,
            "full_source_receipts_required": True, "source_paths_removed": 0}

    def _index(self):
        try:
            with (self.root / "group_index.json").open("rb") as stream:
                raw = stream.read(MAX_INDEX_BYTES + 1)
            if len(raw) > MAX_INDEX_BYTES:
                return {}
            index = json.loads(raw)
            members = index["members"]
            if (index["contract"] == CONTRACT and isinstance(members, dict)
                    and len(members) <= MAX_INDEX_MEMBERS and index["index_sha256"] == hashlib.sha256(
                        json.dumps({"contract": CONTRACT, "members": members},
                            sort_keys=True, separators=(",", ":")).encode()).hexdigest()):
                return members
        except (OSError, ValueError, KeyError, TypeError):
            pass
        return {}

    def _read(self, proof, rows, key):
        try:
            name = proof["path"]
            if not isinstance(name, str) or not re.fullmatch(r"g_[a-f0-9]{64}_[a-f0-9]{32}\.parquet", name):
                raise ValueError("invalid projected member name")
            path = self.root / name
            before = payload_info(path)
            if (proof["input_key"] != key or before["mode"] & ~0o444 or before["uid"] != os.geteuid()
                    or before["size"] != proof["bytes"] or proof["rows"] != rows
                    or stable_payload_hash(path, before) != proof["sha256"]):
                raise ValueError("projected member proof changed")
            frame = pl.read_parquet(path)
            if payload_info(path) != before or frame.height != rows:
                raise ValueError("projected member changed during read")
            return frame
        except (OSError, ValueError, KeyError, TypeError, pl.exceptions.PolarsError):
            self.work["rejected_members"] += 1
            return None

    def project(self, path, transform, *, parser):
        """Owner supplies its exact row-local transform and implementation ID."""
        path = Path(path)
        before = _generation(path)
        parquet = pq.ParquetFile(path)
        try:
            groups, proof_bytes = _encoded_row_groups(path, parquet)
        except (ValueError, pa.ArrowException):
            self.work["unsupported_sources"] += 1
            frame = transform(pl.read_parquet(path))
            if before != _generation(path):
                raise OSError("source changed during uncached projection")
            return frame
        self.work["compressed_proof_bytes"] += proof_bytes
        context = {"contract": CONTRACT, "parser": parser, "implementation": self.implementation,
                   "polars": pl.__version__, "arrow": pa.__version__,
                   "schema": hashlib.sha256(parquet.schema_arrow.remove_metadata().serialize()).hexdigest()}
        keys = [hashlib.sha256(json.dumps({**context, "group": group},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest() for group in groups]
        # This is a tiny writer-control directory, never the source tree.
        # All payload/index growth below is admitted before its computation.
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "projection.lock").open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            members = self._index()
            frames, missing = {}, []
            for index, (key, group) in enumerate(zip(keys, groups)):
                frame = self._read(members[key], group["rows"], key) if key in members else None
                if frame is None:
                    missing.append(index)
                else:
                    frames[index] = frame
                    self.work["groups_reused"] += 1
                    self.work["reused_source_rows"] += group["rows"]
            if missing:
                if len(members) + len(set(keys[index] for index in missing)) > MAX_INDEX_MEMBERS:
                    raise ValueError("projection cache inventory bound; no automatic eviction")
                growth = (sum(parquet.metadata.row_group(index).total_byte_size for index in missing) * 4
                          + len(missing) * 4 * 1024**2 + 2 * MAX_INDEX_BYTES)
                with reserve_cache_write(self.root, key=hashlib.sha256(str(path).encode()).hexdigest(),
                        maximum_growth_bytes=growth, family="panel", policy=self.capacity_policy), GenerationWriteBudget(growth) as budget:
                    for index in missing:
                        frame = transform(pl.from_arrow(parquet.read_row_group(index)))
                        if frame.height != groups[index]["rows"]:
                            raise ValueError("source projection must be row-local and preserve row count")
                        target = self.root / f"g_{keys[index]}_{uuid.uuid4().hex}.parquet"
                        with budget.open(target) as stream:
                            frame.write_parquet(stream, compression="zstd")
                        values = table_content_sha256(frame.to_arrow())
                        if table_content_sha256(pl.read_parquet(target).to_arrow()) != values:
                            raise ValueError("source projection changed typed values on roundtrip")
                        seal_new_file(target)
                        members[keys[index]] = {"path": target.name, "sha256": sha256_file(target),
                            "bytes": target.stat().st_size, "rows": frame.height, "values_sha256": values,
                            "input_key": keys[index]}
                        frames[index] = frame
                        self.work["groups_built"] += 1
                        self.work["parsed_source_rows"] += groups[index]["rows"]
                    if before != _generation(path):
                        raise OSError("source changed during group projection")
                    index = {"contract": CONTRACT, "members": members}
                    index["index_sha256"] = hashlib.sha256(json.dumps(index,
                        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                    if len(json.dumps(index, ensure_ascii=True, indent=2).encode()) > MAX_INDEX_BYTES:
                        raise ValueError("projection index byte bound; last index retained")
                    budget.charge_json(index)
                    atomic_write_json(self.root / "group_index.json", index)
            if before != _generation(path):
                raise OSError("source changed during verified group reuse")
            if not groups:
                return transform(pl.from_arrow(parquet.read()))
            return pl.concat([frames[index] for index in range(len(groups))], rechunk=False)

    def stats(self):
        return dict(self.work)
