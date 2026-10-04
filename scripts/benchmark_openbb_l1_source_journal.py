"""Measure SQLite task-change trigger overhead on a disposable OpenBB sample.

The canonical manifest is opened read-only. Exact successful L1 task/member/
segment rows are copied to two temporary databases on the same filesystem.
One receives the proposed triggers; neither can publish or alter L0/L1 data.
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

from scripts.benchmark_openbb_l1_covering_index import (  # noqa: E402
    _copy_sample,
    _process_io,
    _schema,
)
from scripts.openbb_l1_source_journal import (  # noqa: E402
    JOURNAL_TABLE,
    _install_journal,
)


def _update_trial(
    path: Path,
    task_ids: list[str],
    *,
    variant: str,
    value: str,
    batch_size: int,
) -> dict[str, object]:
    connection = sqlite3.connect(path)
    try:
        io_before = _process_io()
        started = time.perf_counter()
        for offset in range(0, len(task_ids), batch_size):
            batch = task_ids[offset:offset + batch_size]
            connection.executemany(
                "UPDATE tasks SET updated_at=? WHERE task_id=?",
                ((value, task_id) for task_id in batch),
            )
            connection.commit()
        elapsed = time.perf_counter() - started
        io_after = _process_io()
        if connection.execute(
            "SELECT updated_at FROM tasks WHERE task_id=?", (task_ids[0],)
        ).fetchone()[0] != value:
            raise RuntimeError("sample task update was not committed")
        journal_rows = (
            int(connection.execute(
                f"SELECT COUNT(*) FROM {JOURNAL_TABLE}"
            ).fetchone()[0])
            if variant == "journal" else None
        )
        return {
            "variant": variant,
            "updated_tasks": len(task_ids),
            "batch_size": batch_size,
            "elapsed_seconds": round(elapsed, 3),
            "journal_rows_after": journal_rows,
            "process_rchar_bytes": (
                io_after["rchar"] - io_before["rchar"]
                if io_before is not None and io_after is not None else None
            ),
            "process_read_bytes": (
                io_after["read_bytes"] - io_before["read_bytes"]
                if io_before is not None and io_after is not None else None
            ),
            "process_write_bytes": (
                io_after["write_bytes"] - io_before["write_bytes"]
                if io_before is not None and io_after is not None else None
            ),
        }
    finally:
        connection.close()


def _insert_trial(
    path: Path,
    *,
    variant: str,
    wave: int,
    count: int,
    batch_size: int,
) -> dict[str, object]:
    """Insert exact-schema new tasks, distinct from already-compacted members."""

    connection = sqlite3.connect(path)
    try:
        columns = [
            str(row[1]) for row in connection.execute("PRAGMA table_info(tasks)")
        ]
        template = list(connection.execute("SELECT * FROM tasks LIMIT 1").fetchone())
        task_index = columns.index("task_id")
        output_index = columns.index("output_path")
        insert_sql = "INSERT INTO tasks VALUES (" + ",".join(
            "?" for _ in columns
        ) + ")"
        io_before = _process_io()
        started = time.perf_counter()
        for offset in range(0, count, batch_size):
            batch: list[tuple[object, ...]] = []
            for number in range(offset, min(offset + batch_size, count)):
                row = template.copy()
                task_id = hashlib.sha256(
                    f"journal-benchmark-{wave}-{number}".encode()
                ).hexdigest()
                row[task_index] = task_id
                row[output_index] = f"journal-probe/{task_id}.parquet"
                batch.append(tuple(row))
            connection.executemany(insert_sql, batch)
            connection.commit()
        elapsed = time.perf_counter() - started
        io_after = _process_io()
        journal_rows = (
            int(connection.execute(
                f"SELECT COUNT(*) FROM {JOURNAL_TABLE}"
            ).fetchone()[0])
            if variant == "journal" else None
        )
        return {
            "variant": variant,
            "inserted_tasks": count,
            "batch_size": batch_size,
            "elapsed_seconds": round(elapsed, 3),
            "journal_rows_after": journal_rows,
            "process_read_bytes": (
                io_after["read_bytes"] - io_before["read_bytes"]
                if io_before is not None and io_after is not None else None
            ),
            "process_write_bytes": (
                io_after["write_bytes"] - io_before["write_bytes"]
                if io_before is not None and io_after is not None else None
            ),
        }
    finally:
        connection.close()


def benchmark(
    manifest: Path,
    *,
    sample_members: int,
    update_tasks: int,
    batch_size: int,
    temp_root: Path | None = None,
) -> dict[str, object]:
    if not 1 <= sample_members <= 100_000:
        raise ValueError("sample_members must be 1..100000")
    if not 1 <= update_tasks <= sample_members:
        raise ValueError("update_tasks must be 1..sample_members")
    if not 1 <= batch_size <= 10_000:
        raise ValueError("batch_size must be 1..10000")
    manifest = manifest.resolve(strict=True)
    temp_root = temp_root or REPO_ROOT / "artifacts/benchmarks"
    temp_root.mkdir(parents=True, exist_ok=True)
    if os.stat(temp_root).st_dev != os.stat(manifest).st_dev:
        raise ValueError("temporary database must use the manifest filesystem")
    source = sqlite3.connect(f"file:{manifest}?mode=ro", uri=True)
    try:
        source.execute("PRAGMA query_only=ON")
        with tempfile.TemporaryDirectory(
            prefix="stockagent-openbb-journal-", dir=temp_root
        ) as root:
            baseline_path = Path(root) / "baseline.sqlite3"
            journal_path = Path(root) / "journal.sqlite3"
            baseline = sqlite3.connect(baseline_path)
            try:
                started = time.perf_counter()
                copied = _copy_sample(source, baseline, limit=sample_members)
                archive_meta_sql, _ = _schema(source, "archive_meta")
                baseline.execute(archive_meta_sql)
                baseline.commit()
                copied_seconds = round(time.perf_counter() - started, 3)
                task_ids = [
                    str(row[0]) for row in baseline.execute(
                        "SELECT task_id FROM tasks ORDER BY task_id LIMIT ?",
                        (update_tasks,),
                    )
                ]
                journal = sqlite3.connect(journal_path)
                try:
                    baseline.backup(journal)
                    _install_journal(journal)
                finally:
                    journal.close()
            finally:
                baseline.close()
            sequence = (
                ("baseline", baseline_path, "2026-09-26T00:00:00+00:00"),
                ("journal", journal_path, "2026-09-26T00:00:00+00:00"),
                ("journal", journal_path, "2026-09-26T00:00:01+00:00"),
                ("baseline", baseline_path, "2026-09-26T00:00:01+00:00"),
            )
            results = [
                _update_trial(
                    path, task_ids, variant=name, value=value,
                    batch_size=batch_size,
                )
                for name, path, value in sequence
            ]
            insert_sequence = (
                ("baseline", baseline_path, 0),
                ("journal", journal_path, 0),
                ("journal", journal_path, 1),
                ("baseline", baseline_path, 1),
            )
            insert_results = [
                _insert_trial(
                    path, variant=name, wave=wave,
                    count=len(task_ids), batch_size=batch_size,
                )
                for name, path, wave in insert_sequence
            ]
            return {
                "schema_version": 1,
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "manifest": str(manifest),
                "source_open_mode": "read_only",
                "temporary_sample_root": str(temp_root.resolve()),
                "sampled_members": copied,
                "sample_copy_seconds": copied_seconds,
                "updated_tasks_per_trial": len(task_ids),
                "batch_size": batch_size,
                "baseline_database_bytes": baseline_path.stat().st_size,
                "journal_database_bytes": journal_path.stat().st_size,
                "results": results,
                "insert_results": insert_results,
                "process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "claim_boundary": (
                    "Exact sampled task/member/segment rows; source read-only. "
                    "Only disposable copies receive updates or triggers. ABBA "
                    "uses the same task IDs and fixed-size timestamp values. "
                    "The sampled writer cost is not a production downloader "
                    "latency, concurrent-write, or full-manifest storage proof."
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
    parser.add_argument("--sample-members", type=int, default=100_000)
    parser.add_argument("--update-tasks", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--temp-root", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = benchmark(
        args.manifest,
        sample_members=args.sample_members,
        update_tasks=args.update_tasks,
        batch_size=args.batch_size,
        temp_root=args.temp_root,
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
