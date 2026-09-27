"""Benchmark a task-contract covering index without modifying the OpenBB archive.

The source manifest is opened read-only. A bounded, exact sample of successful
L1 members and their full task/segment rows is copied into a disposable SQLite
database. Both query plans run against that *same* database in ABBA order.
This is a query-plan and sampled-cost experiment, not a production speed claim.
"""

from __future__ import annotations

import argparse
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


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.compact_openbb_l1 import (  # noqa: E402
    MEMBER_TABLE,
    SEGMENT_TABLE,
    _lexical_absolute_path,
    _stale_source_contract_sql,
)


COVERING_INDEX = "idx_l1_tasks_source_contract_cover"
TASK_PRIMARY_INDEX = "sqlite_autoindex_tasks_1"
TASK_COLUMNS = "task_id, active, status, endpoint, output_path, rows, updated_at"


def _process_io() -> dict[str, int] | None:
    try:
        values = dict(
            line.split(": ", 1)
            for line in Path("/proc/self/io").read_text(encoding="ascii").splitlines()
            if ": " in line
        )
        return {
            key: int(values[key])
            for key in ("rchar", "read_bytes", "wchar", "write_bytes")
        }
    except (OSError, ValueError, KeyError):
        return None


def _schema(source: sqlite3.Connection, table: str) -> tuple[str, int]:
    row = source.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if row is None or not row[0]:
        raise RuntimeError(f"source table missing: {table}")
    columns = source.execute(f"PRAGMA table_info({table})").fetchall()
    return str(row[0]), len(columns)


def _copy_sample(
    source: sqlite3.Connection, sample: sqlite3.Connection, *, limit: int
) -> int:
    table_widths: list[int] = []
    for table in (MEMBER_TABLE, "tasks", SEGMENT_TABLE):
        ddl, width = _schema(source, table)
        sample.execute(ddl)
        table_widths.append(width)
    sample.execute(
        f"CREATE INDEX idx_l1_members_segment ON {MEMBER_TABLE}(segment_id)"
    )
    sample.commit()
    inserts = [
        f"INSERT {'OR IGNORE ' if table == SEGMENT_TABLE else ''}INTO {table} "
        f"VALUES ({','.join('?' for _ in range(width))})"
        for table, width in zip(
            (MEMBER_TABLE, "tasks", SEGMENT_TABLE), table_widths, strict=True
        )
    ]
    query = (
        f"SELECT m.*, t.*, s.* FROM {MEMBER_TABLE} AS m "
        "JOIN tasks AS t ON t.task_id=m.task_id "
        f"JOIN {SEGMENT_TABLE} AS s ON s.segment_id=m.segment_id "
        "WHERE s.status='success' ORDER BY m.task_id LIMIT ?"
    )
    cursor = source.execute(query, (limit,))
    copied = 0
    while rows := cursor.fetchmany(512):
        for row in rows:
            values = tuple(row)
            first = 0
            for statement, width in zip(inserts, table_widths, strict=True):
                sample.execute(statement, values[first:first + width])
                first += width
        copied += len(rows)
        sample.commit()
    if copied == 0:
        raise RuntimeError("no successful L1 members to sample")
    return copied


def _indexed_query(index_name: str) -> str:
    query = _stale_source_contract_sql("", member_first=True)
    old = "LEFT JOIN tasks AS t ON"
    if query.count(old) != 1:
        raise RuntimeError("OpenBB source-contract query shape changed")
    return query.replace(old, f"LEFT JOIN tasks AS t INDEXED BY {index_name} ON")


def _discard_sample_cache(database: Path) -> None:
    """Request eviction of this disposable file only; never touch source cache."""

    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("per-file cold-cache measurement requires posix_fadvise")
    fd = os.open(database, os.O_RDONLY)
    try:
        os.fsync(fd)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
    finally:
        os.close(fd)


def _query_sample(
    database: Path, index_name: str, *, cold_cache: bool
) -> dict[str, object]:
    if cold_cache:
        _discard_sample_cache(database)
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA cache_size=-2048")
        connection.create_function(
            "stockagent_resolve_path", 1, _lexical_absolute_path,
            deterministic=True,
        )
        query = _indexed_query(index_name)
        prefix = str(REPO_ROOT) + os.sep
        parameters = (prefix, prefix)
        plan = [
            str(row[3]) for row in connection.execute(
                "EXPLAIN QUERY PLAN " + query, parameters
            )
        ]
        io_before = _process_io()
        started = time.perf_counter()
        digest = hashlib.sha256()
        stale_rows = 0
        for row in connection.execute(query, parameters):
            digest.update(json.dumps(tuple(row), separators=(",", ":")).encode())
            digest.update(b"\n")
            stale_rows += 1
        elapsed = time.perf_counter() - started
        io_after = _process_io()
        return {
            "index": index_name,
            "cold_cache_requested": cold_cache,
            "elapsed_seconds": round(elapsed, 3),
            "stale_rows": stale_rows,
            "stale_rows_sha256": digest.hexdigest(),
            "process_rchar_bytes": (
                io_after["rchar"] - io_before["rchar"]
                if io_before is not None and io_after is not None else None
            ),
            "process_read_bytes": (
                io_after["read_bytes"] - io_before["read_bytes"]
                if io_before is not None and io_after is not None else None
            ),
            "query_plan": plan,
        }
    finally:
        connection.close()


def _write_trial(
    database: Path, task_ids: list[str], *, variant: str, new_value: str
) -> dict[str, object]:
    """Time a committed update in one disposable sampled database."""

    connection = sqlite3.connect(database)
    try:
        connection.execute("PRAGMA cache_size=-2048")
        io_before = _process_io()
        started = time.perf_counter()
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.executemany(
                "UPDATE tasks SET updated_at=? WHERE task_id=?",
                ((new_value, task_id) for task_id in task_ids),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        elapsed = time.perf_counter() - started
        io_after = _process_io()
        if connection.execute(
            "SELECT updated_at FROM tasks WHERE task_id=?", (task_ids[0],)
        ).fetchone()[0] != new_value:
            raise RuntimeError("committed task update did not change the sample")
        return {
            "variant": variant,
            "updated_rows": len(task_ids),
            "elapsed_seconds": round(elapsed, 3),
            "process_write_bytes": (
                io_after["write_bytes"] - io_before["write_bytes"]
                if io_before is not None and io_after is not None else None
            ),
        }
    finally:
        connection.close()


def benchmark(
    manifest: Path, *, sample_members: int,
    temp_root: Path | None = None, cold_cache: bool = False,
) -> dict[str, object]:
    if not 1 <= sample_members <= 100_000:
        raise ValueError("sample_members must be 1..100000")
    manifest = manifest.resolve(strict=True)
    temp_root = temp_root or REPO_ROOT / "artifacts/benchmarks"
    temp_root.mkdir(parents=True, exist_ok=True)
    if cold_cache and os.stat(temp_root).st_dev != os.stat(manifest).st_dev:
        raise ValueError("cold-cache sample must use the manifest filesystem")
    source = sqlite3.connect(f"file:{manifest}?mode=ro", uri=True)
    try:
        source.execute("PRAGMA query_only=ON")
        with tempfile.TemporaryDirectory(
            prefix="stockagent-openbb-cover-", dir=temp_root
        ) as root:
            database = Path(root) / "sample.sqlite3"
            baseline_database = Path(root) / "baseline.sqlite3"
            sample = sqlite3.connect(database)
            try:
                started = time.perf_counter()
                copied = _copy_sample(source, sample, limit=sample_members)
                sample_seconds = round(time.perf_counter() - started, 3)
                task_ids = [
                    str(row[0]) for row in sample.execute(
                        "SELECT task_id FROM tasks ORDER BY task_id LIMIT ?",
                        (min(1_000, copied),),
                    )
                ]
                baseline = sqlite3.connect(baseline_database)
                try:
                    sample.backup(baseline)
                finally:
                    baseline.close()
                page_size = int(sample.execute("PRAGMA page_size").fetchone()[0])
                before_pages = int(sample.execute("PRAGMA page_count").fetchone()[0])
                started = time.perf_counter()
                sample.execute(
                    f"CREATE INDEX {COVERING_INDEX} ON tasks({TASK_COLUMNS})"
                )
                sample.commit()
                index_seconds = round(time.perf_counter() - started, 3)
                after_pages = int(sample.execute("PRAGMA page_count").fetchone()[0])
            finally:
                sample.close()
            sequence = (
                TASK_PRIMARY_INDEX, COVERING_INDEX,
                COVERING_INDEX, TASK_PRIMARY_INDEX,
            )
            results = [
                _query_sample(database, name, cold_cache=cold_cache)
                for name in sequence
            ]
            if len({
                (result["stale_rows"], result["stale_rows_sha256"])
                for result in results
            }) != 1:
                raise RuntimeError("sample source-contract variants disagree")
            if not all(
                any("COVERING INDEX" in step and COVERING_INDEX in step
                    for step in result["query_plan"])
                for result in results if result["index"] == COVERING_INDEX
            ):
                raise RuntimeError("candidate index did not cover task lookups")
            write_sequence = (
                ("primary", baseline_database, "2026-09-26T00:00:00+00:00"),
                ("covering", database, "2026-09-26T00:00:00+00:00"),
                ("covering", database, "2026-09-26T00:00:01+00:00"),
                ("primary", baseline_database, "2026-09-26T00:00:01+00:00"),
            )
            write_results = [
                _write_trial(path, task_ids, variant=name, new_value=value)
                for name, path, value in write_sequence
            ]
            return {
                "schema_version": 1,
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "manifest": str(manifest),
                "source_open_mode": "read_only",
                "temporary_sample_root": str(temp_root.resolve()),
                "cold_cache_requested": cold_cache,
                "sample_selection": "first successful L1 member task IDs ascending",
                "requested_members": sample_members,
                "sampled_members": copied,
                "sample_copy_seconds": sample_seconds,
                "sample_database_bytes": after_pages * page_size,
                "covering_index_bytes_approx": (after_pages - before_pages) * page_size,
                "covering_index_build_seconds": index_seconds,
                "results": results,
                "write_results": write_results,
                "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "claim_boundary": (
                    "Read-only source; exact task/member/segment rows copied to a "
                    "disposable bounded sample. ABBA compares forced plans on the "
                    "same temporary database. Per-file cold-cache eviction is "
                    "requested only for the disposable sample when selected. "
                    "Production cold I/O, "
                    "whole-manifest index size, and concurrent service load are "
                    "not proven. Committed updates affect disposable sample "
                    "databases only and measure local write overhead. "
                    "Do not add this index to the live manifest from this receipt alone."
                ),
            }
    finally:
        source.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest", type=Path,
        default=REPO_ROOT / "data_openBB/_state/openbb_archive.sqlite3",
    )
    parser.add_argument("--sample-members", type=int, default=20_000)
    parser.add_argument("--temp-root", type=Path)
    parser.add_argument("--cold-cache", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = benchmark(
        args.manifest, sample_members=args.sample_members,
        temp_root=args.temp_root, cold_cache=args.cold_cache,
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(f".{args.output.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(payload, indent=2) + "\n")
            os.replace(temporary, args.output)
        finally:
            temporary.unlink(missing_ok=True)
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
