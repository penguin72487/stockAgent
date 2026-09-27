#!/usr/bin/env python3
"""Read-only A/B benchmark of the complete OpenBB L1 source-contract scan.

Both variants use one SQLite read transaction, the production predicate and
the same row digest. This does not mark segments stale or mutate the archive.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import tempfile
import time

import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.compact_openbb_l1 import (  # noqa: E402
    _lexical_absolute_path,
    _stale_source_contract_sql,
)


def _process_read_counters() -> dict[str, int] | None:
    """Measure this benchmark's reads, not other host or SQLite writer I/O."""

    try:
        lines = Path("/proc/self/io").read_text(encoding="ascii").splitlines()
        values = dict(line.split(": ", 1) for line in lines if ": " in line)
        return {name: int(values[name]) for name in ("rchar", "read_bytes")}
    except (OSError, KeyError, ValueError):
        return None


def _source_variant_sequence(requested: str) -> tuple[bool, ...]:
    """Return member-first flags; reversed orders expose cache/order effects."""

    if requested == "both":
        return (True, False)
    if requested == "abba":
        return (False, True, True, False)
    if requested == "baab":
        return (True, False, False, True)
    if requested == "none":
        return ()
    if requested in {"segment_first", "member_first"}:
        return (requested == "member_first",)
    raise ValueError(f"unknown source benchmark variant: {requested}")


def _run_variant(
    connection: sqlite3.Connection, *, member_first: bool, prefix: str
) -> dict[str, object]:
    query = _stale_source_contract_sql("", member_first=member_first)
    parameters = (prefix, prefix)
    plan = [
        str(row[3])
        for row in connection.execute("EXPLAIN QUERY PLAN " + query, parameters)
    ]
    row_hashes: list[bytes] = []
    io_before = _process_read_counters()
    started = time.perf_counter()
    for row in connection.execute(query, parameters):
        row_hashes.append(
            hashlib.sha256(
                json.dumps(
                    tuple(row), ensure_ascii=False, separators=(",", ":")
                ).encode()
            ).digest()
        )
    query_elapsed = time.perf_counter() - started
    io_after = _process_read_counters()
    digest = hashlib.sha256()
    for row_hash in sorted(row_hashes):
        digest.update(row_hash)
    return {
        "variant": "member_first" if member_first else "segment_first",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "query_elapsed_seconds": round(query_elapsed, 3),
        "process_rchar_bytes": (
            io_after["rchar"] - io_before["rchar"]
            if io_before is not None and io_after is not None else None
        ),
        "process_read_bytes": (
            io_after["read_bytes"] - io_before["read_bytes"]
            if io_before is not None and io_after is not None else None
        ),
        "stale_rows": len(row_hashes),
        "stale_rows_sha256": digest.hexdigest(),
        "query_plan": plan,
    }


def _metadata_result(
    row: tuple[str, str, int, str], *, use_read_metadata: bool
) -> tuple[str, int | str, str]:
    segment_id, output_path, _expected_rows, _expected_schema = row
    try:
        if use_read_metadata:
            metadata = pq.read_metadata(output_path)
            observed_rows = int(metadata.num_rows)
            schema = metadata.schema.to_arrow_schema()
        else:
            parquet_file = pq.ParquetFile(output_path)
            observed_rows = int(parquet_file.metadata.num_rows)
            schema = parquet_file.schema_arrow
        fingerprint = hashlib.sha256(
            schema.remove_metadata().serialize().to_pybytes()
        ).hexdigest()
        return segment_id, observed_rows, fingerprint
    except Exception as exc:
        return segment_id, type(exc).__name__, "unreadable"


def _run_metadata_variant(
    rows: list[tuple[str, str, int, str]],
    *,
    use_read_metadata: bool,
    workers: int,
) -> dict[str, object]:
    """Read exact output-row/schema evidence without altering L1 state."""

    started = time.perf_counter()
    if workers == 1:
        observed = [
            _metadata_result(row, use_read_metadata=use_read_metadata)
            for row in rows
        ]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            observed = list(executor.map(
                lambda row: _metadata_result(
                    row, use_read_metadata=use_read_metadata
                ),
                rows,
            ))
    digest = hashlib.sha256()
    mismatches = 0
    for source, current in zip(rows, observed, strict=True):
        digest.update(json.dumps(current, separators=(",", ":")).encode())
        if current[1:] != (source[2], source[3]):
            mismatches += 1
    return {
        "variant": ("read_metadata" if use_read_metadata else "parquetfile")
        + f"_workers_{workers}",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "files": len(rows),
        "mismatches": mismatches,
        "observed_sha256": digest.hexdigest(),
    }


def _run_source_metadata_variant(
    rows: list[tuple[str, str, int, int]], *, single_open: bool,
) -> dict[str, object]:
    """Measure the exact deep-audit row/uncompressed-size source check."""

    started = time.perf_counter()
    digest = hashlib.sha256()
    mismatches = 0
    for task_id, source_path, expected_rows, expected_uncompressed in rows:
        if single_open:
            metadata = pq.ParquetFile(source_path).metadata
            observed_rows = int(metadata.num_rows)
        else:
            observed_rows = int(pq.ParquetFile(source_path).metadata.num_rows)
            metadata = pq.ParquetFile(source_path).metadata
        uncompressed = sum(
            int(metadata.row_group(index).total_byte_size)
            for index in range(metadata.num_row_groups)
        )
        digest.update(
            json.dumps(
                (task_id, observed_rows, uncompressed), separators=(",", ":")
            ).encode()
        )
        mismatches += (
            observed_rows != expected_rows
            or (expected_uncompressed > 0 and uncompressed != expected_uncompressed)
        )
    return {
        "variant": "single_open" if single_open else "double_open",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "files": len(rows),
        "mismatches": mismatches,
        "observed_sha256": digest.hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path,
        default=REPO_ROOT / "data_openBB/_state/openbb_archive.sqlite3",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--cache-mib", type=int, default=0)
    parser.add_argument(
        "--variant", choices=("both", "abba", "baab", "segment_first", "member_first", "none"),
        default="both",
    )
    parser.add_argument(
        "--metadata-sample", type=int, default=0,
        help="Compare exact Parquet row/schema checks on up to 50,000 segments",
    )
    parser.add_argument("--metadata-workers", type=int, default=4)
    parser.add_argument(
        "--source-metadata-sample", type=int, default=0,
        help="Compare one versus two Parquet metadata opens on up to 50,000 L0 members",
    )
    args = parser.parse_args()
    if args.cache_mib < 0 or args.cache_mib > 2048:
        parser.error("--cache-mib must be 0..2048")
    if not 0 <= args.metadata_sample <= 50_000:
        parser.error("--metadata-sample must be 0..50000")
    if not 2 <= args.metadata_workers <= 16:
        parser.error("--metadata-workers must be 2..16")
    if not 0 <= args.source_metadata_sample <= 50_000:
        parser.error("--source-metadata-sample must be 0..50000")
    if args.variant == "none" and args.metadata_sample == 0 and args.source_metadata_sample == 0:
        parser.error("--variant none requires a metadata sample")
    manifest = args.manifest.resolve(strict=True)
    os.chdir(REPO_ROOT)
    connection = sqlite3.connect(
        f"file:{manifest}?mode=ro", uri=True, timeout=30.0
    )
    try:
        connection.create_function(
            "stockagent_resolve_path", 1, _lexical_absolute_path,
            deterministic=True,
        )
        if args.cache_mib:
            connection.execute(f"PRAGMA cache_size={-args.cache_mib * 1024}")
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        prefix = str(REPO_ROOT) + os.sep
        variants = _source_variant_sequence(args.variant)
        results = [
            _run_variant(connection, member_first=member_first, prefix=prefix)
            for member_first in variants
        ]
        connection.rollback()
        if len({
            (result["stale_rows"], result["stale_rows_sha256"])
            for result in results
        }) > 1:
            raise RuntimeError("OpenBB source-contract scan variants disagree")
        metadata_probe = None
        if args.metadata_sample:
            source_rows = [
                (str(row[0]), str(row[1]), int(row[2]), str(row[3]))
                for row in connection.execute(
                    "SELECT segment_id, output_path, output_rows, "
                    "schema_fingerprint FROM l1_compaction_segments "
                    "WHERE status='success' ORDER BY segment_id LIMIT ?",
                    (args.metadata_sample,),
                )
            ]
            if not source_rows:
                raise RuntimeError("no successful L1 segments for metadata probe")
            sequence = (
                (False, 1), (True, 1), (False, args.metadata_workers),
                (False, args.metadata_workers), (True, 1), (False, 1),
            )
            samples = [
                _run_metadata_variant(
                    source_rows, use_read_metadata=use_metadata, workers=workers
                )
                for use_metadata, workers in sequence
            ]
            if len({sample["observed_sha256"] for sample in samples}) != 1:
                raise RuntimeError("L1 metadata variants disagree or files changed")
            metadata_probe = {
                "selection": "first_successful_segment_ids_ascending",
                "source_rows": len(source_rows),
                "samples": samples,
                "claim_boundary": (
                    "Same selected files and complete row/schema observations, "
                    "read-only; filesystem cache and other service load can "
                    "still vary. Never infer whole-service speed from a sample."
                ),
            }
        source_metadata_probe = None
        if args.source_metadata_sample:
            source_rows = [
                (str(row[0]), str(row[1]), int(row[2]), int(row[3] or 0))
                for row in connection.execute(
                    "SELECT task_id, source_path, source_rows, "
                    "source_uncompressed_bytes FROM l1_compaction_members "
                    "ORDER BY segment_id, task_id LIMIT ?",
                    (args.source_metadata_sample,),
                )
            ]
            if not source_rows:
                raise RuntimeError("no L0 members for source metadata probe")
            samples = [
                _run_source_metadata_variant(source_rows, single_open=single)
                for single in (False, True, True, False)
            ]
            if len({sample["observed_sha256"] for sample in samples}) != 1:
                raise RuntimeError("L0 source metadata changed between variants")
            source_metadata_probe = {
                "selection": "first_member_segment_and_task_ids_ascending",
                "source_rows": len(source_rows),
                "samples": samples,
                "claim_boundary": (
                    "Read-only metadata check of selected L0 files. Does not "
                    "measure the whole deep audit or normal compaction service."
                ),
            }
        payload = {
            "schema_version": 1,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "manifest": str(manifest),
            "manifest_bytes": manifest.stat().st_size,
            "cache_mib": args.cache_mib,
            "requested_variant": args.variant,
            "claim_boundary": (
                "One read-only SQLite snapshot for selected source queries, "
                "L1 segments and/or L0 members. Source order is recorded in "
                "results; abba and baab reverse the warm-cache order. The "
                "L1 metadata probe uses A-B-C-C-B-A order; the L0 metadata "
                "probe uses A-B-B-A order. "
                "Filesystem cache and concurrent host load may vary; these "
                "are not p95, complete-service, or write-cost measurements."
            ),
            "results": results,
            "metadata_probe": metadata_probe,
            "source_metadata_probe": source_metadata_probe,
            "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
        if args.output is not None:
            output = args.output.resolve()
            output.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=output.parent,
                prefix=f".{output.name}.", suffix=".tmp", delete=False,
            ) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
            os.replace(temporary, output)
        print(json.dumps(payload, ensure_ascii=False))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
