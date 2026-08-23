from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import sqlite3

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from downloader.download_openbb_archive import DownloadTask, Manifest, TaskResult
from scripts.compact_openbb_l1 import (
    MAX_QUERY_VIEW_SCHEMA_VARIANTS,
    TaskShard,
    _archive_compaction_allowed,
    _lexical_absolute_path,
    _query_view_deferred_reason,
    _segment_batches,
    run,
)


ENDPOINT = "equity.price.historical"


def test_query_view_deferral_has_an_explicit_schema_complexity_boundary() -> None:
    assert _query_view_deferred_reason(MAX_QUERY_VIEW_SCHEMA_VARIANTS) is None
    reason = _query_view_deferred_reason(MAX_QUERY_VIEW_SCHEMA_VARIANTS + 1)
    assert reason is not None
    assert "long_form_normalization_required" in reason


def test_manifest_path_normalization_is_lexical_and_absolute(
    tmp_path: Path, monkeypatch
) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    monkeypatch.chdir(tmp_path)

    assert _lexical_absolute_path("link/source.parquet") == str(link / "source.parquet")


def test_segment_batches_enforce_rows_and_uncompressed_bytes_before_file_minimum() -> (
    None
):
    shards = [
        TaskShard(
            task_id=f"task-{index}",
            endpoint=ENDPOINT,
            output_path=f"/{index}.parquet",
            rows=6,
            task_updated_at="2026-01-01T00:00:00+00:00",
            bytes=10,
            uncompressed_bytes=60,
            mtime_ns=index,
        )
        for index in range(3)
    ]
    batches = list(
        _segment_batches(
            shards,
            max_files=100,
            max_bytes=1_000,
            max_uncompressed_bytes=100,
            max_rows=10,
            min_files=128,
            include_tail=False,
            flush_endpoints={ENDPOINT},
        )
    )
    assert [[item.task_id for item in batch] for batch in batches] == [
        ["task-0"],
        ["task-1"],
        ["task-2"],
    ]


def test_segment_batches_never_union_different_parquet_schemas() -> None:
    shards = [
        TaskShard(
            task_id=f"task-{index}",
            endpoint=ENDPOINT,
            output_path=f"/{index}.parquet",
            rows=1,
            task_updated_at="2026-01-01T00:00:00+00:00",
            bytes=10,
            uncompressed_bytes=20,
            mtime_ns=index,
            schema_fingerprint="schema-a" if index % 2 == 0 else "schema-b",
        )
        for index in range(4)
    ]
    batches = list(
        _segment_batches(
            shards,
            max_files=100,
            max_bytes=1_000,
            max_uncompressed_bytes=1_000,
            max_rows=100,
            min_files=1,
            include_tail=True,
        )
    )
    assert [[item.task_id for item in batch] for batch in batches] == [
        ["task-0", "task-2"],
        ["task-1", "task-3"],
    ]


def test_archive_idle_guard_prioritizes_downloader_work(tmp_path: Path) -> None:
    state = tmp_path / "_state"
    state.mkdir()
    (state / "downloader.pid").write_text(str(os.getpid()), encoding="utf-8")
    phase_path = state / "downloader_phase.json"
    scheduler_path = state / "provider_scheduler.json"

    phase_path.write_text(json.dumps({"phase": "planning"}), encoding="utf-8")
    assert _archive_compaction_allowed(state) == (False, "archive_phase_planning")

    phase_path.write_text(json.dumps({"phase": "download"}), encoding="utf-8")
    scheduler_path.write_text(
        json.dumps(
            {
                "phase": "waiting",
                "active_total": 0,
                "buffered_total": 0,
                "completed_pending_total": 0,
            }
        ),
        encoding="utf-8",
    )
    phase_mtime = phase_path.stat().st_mtime_ns
    os.utime(scheduler_path, ns=(phase_mtime + 1, phase_mtime + 1))
    assert _archive_compaction_allowed(state) == (
        True,
        "archive_waiting_for_provider_quota",
    )

    scheduler_path.write_text(json.dumps({"phase": "running"}), encoding="utf-8")
    os.utime(scheduler_path, ns=(phase_mtime + 2, phase_mtime + 2))
    assert _archive_compaction_allowed(state) == (
        False,
        "archive_scheduler_running",
    )


def _task(root: Path, task_id: str) -> DownloadTask:
    return DownloadTask(
        task_id=task_id,
        endpoint=ENDPOINT,
        category="equity",
        scope_key=task_id,
        kwargs={"symbol": task_id},
        providers=("yfinance",),
        output_path=str(root / "data" / "equity" / f"{task_id}.parquet"),
    )


def _publish_tasks(root: Path, task_ids: list[str]) -> list[DownloadTask]:
    manifest = Manifest(root / "_state" / "openbb_archive.sqlite3")
    tasks = [_task(root, task_id) for task_id in task_ids]
    try:
        manifest.upsert_tasks(tasks, plan_token="test-plan")
        manifest.set_meta_value("active_plan_token", "test-plan")
        for index, task in enumerate(tasks):
            output = Path(task.output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                pa.Table.from_pylist(
                    [{"date": f"2024-01-{index + 1:02d}", "close": float(index)}]
                ),
                output,
                compression="zstd",
            )
            manifest.claim([task])
            manifest.complete(
                TaskResult(task, "success", "yfinance", 1, str(output), 1)
            )
    finally:
        manifest.close()
    return tasks


def _args(root: Path) -> list[str]:
    return [
        "--output-dir",
        str(root),
        "--endpoint",
        ENDPOINT,
        "--max-source-files",
        "100",
        "--max-files-per-segment",
        "2",
        "--min-files-per-segment",
        "1",
        "--include-tail",
        "--threads",
        "1",
        "--memory-limit",
        "512MB",
        "--no-progress",
    ]


def _segment_counts(root: Path) -> tuple[int, int]:
    with sqlite3.connect(root / "_state" / "openbb_archive.sqlite3") as connection:
        return tuple(
            int(value)
            for value in connection.execute(
                "SELECT "
                "SUM(status='success'), SUM(status!='success') "
                "FROM l1_compaction_segments"
            ).fetchone()
        )


def test_l1_compaction_is_incremental_queryable_and_self_healing(
    tmp_path: Path,
) -> None:
    tasks = _publish_tasks(tmp_path, ["aa", "bb", "cc", "dd", "ee"])

    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (3, 0)
    with duckdb.connect(
        str(tmp_path / "openbb_l1.duckdb"), read_only=True
    ) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
            ).fetchone()[0]
            == 5
        )
        assert connection.execute("SELECT COUNT(*) FROM l1_catalog").fetchone()[0] == 1

    status = pl.read_parquet(tmp_path / "catalog" / "l1_compaction_status.parquet")
    assert status["success_files"].to_list() == [5]
    assert status["compacted_files"].to_list() == [5]
    assert status["pending_files"].to_list() == [0]

    # Idempotent reruns publish the same views without duplicating source rows.
    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (3, 0)

    _publish_tasks(tmp_path, ["ff"])
    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (4, 0)
    with duckdb.connect(
        str(tmp_path / "openbb_l1.duckdb"), read_only=True
    ) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
            ).fetchone()[0]
            == 6
        )

    # Replacing one successful L0 task invalidates only its containing segment;
    # the old derivative is quarantined and rebuilt from current L0 truth.
    replacement = Path(tasks[0].output_path)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": "2024-01-01", "close": 10.0},
                {"date": "2024-01-02", "close": 11.0},
            ]
        ),
        replacement,
        compression="zstd",
    )
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        connection.execute(
            "UPDATE tasks SET rows=2, updated_at=? WHERE task_id='aa'",
            (datetime.now(timezone.utc).isoformat(),),
        )
        connection.commit()
    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (4, 1)
    assert list((tmp_path / "compact_l1" / "_stale").rglob("*.parquet"))
    with duckdb.connect(
        str(tmp_path / "openbb_l1.duckdb"), read_only=True
    ) as connection:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
            ).fetchone()[0]
            == 7
        )

    assert run([*_args(tmp_path), "--audit-only"]) == 0
    audit = pl.read_parquet(tmp_path / "catalog" / "l1_compaction_audit.parquet")
    assert set(audit["status"].to_list()) == {"passed"}

    # A readable but truncated derivative fails audit, then the normal run
    # detects metadata drift and reconstructs it without touching L0.
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        segment_path_value, segment_rows = connection.execute(
            "SELECT output_path, output_rows FROM l1_compaction_segments "
            "WHERE status='success' ORDER BY segment_id LIMIT 1"
        ).fetchone()
        segment_path = Path(segment_path_value)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": f"bad-{index}", "close": 0.0}
                for index in range(int(segment_rows) + 1)
            ]
        ),
        segment_path,
    )
    assert run([*_args(tmp_path), "--audit-only"]) == 2
    assert run(_args(tmp_path)) == 0
    assert run([*_args(tmp_path), "--audit-only"]) == 0


def test_l1_tail_is_left_pending_until_threshold_or_explicit_flush(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    args = [
        "--output-dir",
        str(tmp_path),
        "--endpoint",
        ENDPOINT,
        "--max-source-files",
        "100",
        "--max-files-per-segment",
        "100",
        "--min-files-per-segment",
        "3",
        "--threads",
        "1",
        "--memory-limit",
        "512MB",
        "--no-progress",
    ]
    assert run(args) == 0
    status = pl.read_parquet(tmp_path / "catalog" / "l1_compaction_status.parquet")
    assert status["compacted_files"].to_list() == [0]
    assert status["pending_files"].to_list() == [2]
    assert run([*args, "--include-tail"]) == 0
    status = pl.read_parquet(tmp_path / "catalog" / "l1_compaction_status.parquet")
    assert status["compacted_files"].to_list() == [2]
    assert status["pending_files"].to_list() == [0]
