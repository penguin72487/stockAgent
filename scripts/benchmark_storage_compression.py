#!/usr/bin/env python3
"""Bounded Parquet codec trial on complete files; original bytes stay untouched.

Compare identical Arrow encoding options at ZSTD levels 3/6/12/19. Validate all
values, schema, nulls and original SHA; retain receipts, remove only disposable
outputs created inside this invocation. Warm read times are not cold I/O proof.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.desync_snapshots import atomic_write_json


def signature(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("benchmark requires a regular source file")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def benchmark(paths, output, *, max_input_mib=16, max_decoded_mib=64):
    import pyarrow.parquet as pq
    import polars as pl
    if output.exists() or len(paths) > 8:
        raise ValueError("retain a fresh bounded benchmark receipt")
    output.parent.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(output.parent).free < 25 * 1024**3 + 2 * max_decoded_mib * 1024**2:
        raise ValueError("defer benchmark below the existing 25 GiB filesystem reserve")
    results = []
    for path in paths:
        path = path.absolute()
        before = signature(path)
        row = {"source": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else path.name,
               "source_bytes": before[2], "source_sha256": None, "candidates": []}
        if before[2] > max_input_mib * 1024**2:
            row["state"] = "deferred_input_limit_no_payload_read"
            results.append(row)
            continue
        metadata = pq.read_metadata(path)
        decoded = sum(metadata.row_group(i).column(j).total_uncompressed_size
                      for i in range(metadata.num_row_groups) for j in range(metadata.num_columns))
        codecs = sorted({metadata.row_group(i).column(j).compression
                         for i in range(metadata.num_row_groups) for j in range(metadata.num_columns)})
        row.update(decoded_bytes=decoded, original_codecs=codecs, rows=metadata.num_rows, columns=metadata.num_columns)
        if decoded > max_decoded_mib * 1024**2:
            row["state"] = "deferred_bounded_memory_trial"
            results.append(row)
            continue
        source_sha = digest(path)
        row["source_sha256"] = source_sha
        table = pq.read_table(path)
        original = pl.from_arrow(table)
        with tempfile.TemporaryDirectory(prefix="storage-codec-", dir=output.parent) as scratch:
            for level in (3, 6, 12, 19):
                candidate = Path(scratch) / f"zstd-{level}.parquet"
                started = time.perf_counter()
                pq.write_table(table, candidate, compression="zstd", compression_level=level, write_statistics=True,
                               use_dictionary=True, row_group_size=128 * 1024)
                with candidate.open("rb") as stream:
                    os.fsync(stream.fileno())
                write_seconds = time.perf_counter() - started
                started = time.perf_counter()
                reconstructed = pq.read_table(candidate)
                read_seconds = time.perf_counter() - started
                if (not table.schema.equals(reconstructed.schema, check_metadata=True)
                        or not original.equals(pl.from_arrow(reconstructed))):
                    raise ValueError("codec trial changed schema, values or null semantics")
                info = candidate.stat()
                row["candidates"].append({"codec": "zstd", "level": level, "bytes": info.st_size,
                    "allocated_bytes": info.st_blocks * 512, "write_fsync_seconds": write_seconds,
                    "warm_read_decode_seconds": read_seconds, "full_value_roundtrip_verified": True})
        if signature(path) != before or digest(path) != source_sha:
            raise ValueError("source changed during compression benchmark")
        row["state"] = "roundtrip_verified_source_unchanged"
        results.append(row)
        atomic_write_json(output, {"state": "in_progress", "files": results, "source_files_modified": 0})
    result = {"state": "completed", "files": results, "source_files_modified": 0, "gpu_used": False,
              "timing_basis": "complete bounded files, encode+fsync and warm read+decode; not a full-provider or cold-I/O benchmark",
              "production_codec_changed": False}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = benchmark(args.parquet, args.output)
    print(json.dumps({"state": result["state"], "files": len(result["files"]), "production_codec_changed": False}))


if __name__ == "__main__":
    main()
