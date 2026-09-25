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
from scripts.benchmark_openbb_l1_stale_scan import _run_variant
from scripts.compact_openbb_l1 import (
    MAX_QUERY_VIEW_SCHEMA_VARIANTS,
    TASK_COMPACTION_INDEX,
    TaskShard,
    _archive_compaction_allowed,
    _lexical_absolute_path,
    _load_unassigned_shards,
    _mark_stale_segments,
    _open_manifest,
    _query_view_deferred_reason,
    _segment_batches,
    _stale_source_contract_sql,
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


def test_member_first_stale_scan_preserves_each_source_contract_reason(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb", "cc"])
    args = _args(tmp_path)
    args[args.index("--max-files-per-segment") + 1] = "1"
    assert run(args) == 0
    connection = _open_manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        connection.execute(
            "UPDATE tasks SET rows=rows+1 WHERE task_id='aa'"
        )
        connection.execute(
            "UPDATE tasks SET active=0 WHERE task_id='bb'"
        )
        connection.execute(
            "UPDATE tasks SET output_path='changed-path' WHERE task_id='cc'"
        )
        connection.commit()
        prefix = os.path.abspath(os.curdir) + os.sep
        parameters = (prefix, prefix)
        baseline = [
            tuple(row) for row in connection.execute(
                _stale_source_contract_sql(""), parameters
            )
        ]
        candidate = [
            tuple(row) for row in connection.execute(
                _stale_source_contract_sql("", member_first=True), parameters
            )
        ]
        assert candidate == baseline
        assert {row[3] for row in candidate} == {
            "source row contract changed",
            "source task retired from active plan",
            "source path changed",
        }
    finally:
        connection.close()


def test_stale_scan_benchmark_hashes_multiple_reasons_per_segment(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    connection = _open_manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        connection.execute("UPDATE tasks SET rows=rows+1 WHERE task_id='aa'")
        connection.execute("UPDATE tasks SET active=0 WHERE task_id='bb'")
        connection.commit()
        prefix = os.path.abspath(os.curdir) + os.sep
        baseline = _run_variant(connection, member_first=False, prefix=prefix)
        candidate = _run_variant(connection, member_first=True, prefix=prefix)
        assert baseline["stale_rows"] == candidate["stale_rows"] == 2
        assert baseline["stale_rows_sha256"] == candidate["stale_rows_sha256"]
    finally:
        connection.close()


def test_l1_source_path_fast_comparison_keeps_noncanonical_fallback(
    tmp_path: Path,
) -> None:
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    manifest_path = tmp_path / "_state" / "openbb_archive.sqlite3"
    connection = _open_manifest(manifest_path)
    try:
        relative = os.path.relpath(tasks[0].output_path)
        for spelling in (relative, f"./{relative}", tasks[0].output_path):
            connection.execute(
                "UPDATE tasks SET output_path=? WHERE task_id='aa'", (spelling,)
            )
            assert _mark_stale_segments(connection, ()) == (set(), 0)

        connection.execute(
            "UPDATE tasks SET output_path=? WHERE task_id='aa'",
            (str(tmp_path / "different.parquet"),),
        )
        assert _mark_stale_segments(connection, ()) == ({ENDPOINT}, 1)
        reason = connection.execute(
            "SELECT stale_reason FROM l1_compaction_segments WHERE status='stale'"
        ).fetchone()[0]
        assert reason == "source path changed"
    finally:
        connection.close()


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


def _task(root: Path, task_id: str, *, endpoint: str = ENDPOINT) -> DownloadTask:
    return DownloadTask(
        task_id=task_id,
        endpoint=endpoint,
        category="equity",
        scope_key=task_id,
        kwargs={"symbol": task_id},
        providers=("yfinance",),
        output_path=str(root / "data" / "equity" / f"{task_id}.parquet"),
    )


def _publish_tasks(
    root: Path, task_ids: list[str], *, endpoint: str = ENDPOINT
) -> list[DownloadTask]:
    manifest = Manifest(root / "_state" / "openbb_archive.sqlite3")
    tasks = [_task(root, task_id, endpoint=endpoint) for task_id in task_ids]
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


def test_unassigned_source_fast_path_preserves_global_task_order(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["cc", "bb", "aa"])
    state_path = tmp_path / "_state" / "openbb_archive.sqlite3"
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE tasks SET endpoint='zeta.example' WHERE task_id='cc'"
        )
        connection.execute(f"DROP INDEX {TASK_COMPACTION_INDEX}")
    connection = _open_manifest(state_path)
    try:
        fast_timing: dict[str, float] = {}
        fast = _load_unassigned_shards(
            connection, (), limit=2, show_progress=False, timing=fast_timing
        )
        assert [row.task_id for row in fast] == ["aa", "bb"]
        assert "unassigned_source_endpoint_sort" in fast_timing
        assert "unassigned_source_global_fallback" not in fast_timing

        short_timing: dict[str, float] = {}
        short = _load_unassigned_shards(
            connection, (), limit=3, show_progress=False, timing=short_timing
        )
        assert [row.task_id for row in short] == ["aa", "bb", "cc"]
        assert "unassigned_source_global_fallback" in short_timing

        connection.execute("DROP INDEX idx_tasks_active_plan")
        missing_index_timing: dict[str, float] = {}
        missing_index = _load_unassigned_shards(
            connection, (), limit=2, show_progress=False,
            timing=missing_index_timing,
        )
        assert [row.task_id for row in missing_index] == ["aa", "bb"]
        assert "unassigned_source_global_fallback" in missing_index_timing
    finally:
        connection.close()


def test_covering_compaction_task_index_preserves_global_order(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["cc", "bb", "aa"])
    state_path = tmp_path / "_state" / "openbb_archive.sqlite3"
    with sqlite3.connect(state_path) as connection:
        connection.execute(
            "UPDATE tasks SET endpoint='zeta.example' WHERE task_id='cc'"
        )
        connection.execute(f"DROP INDEX {TASK_COMPACTION_INDEX}")
    connection = _open_manifest(state_path)
    try:
        baseline = _load_unassigned_shards(
            connection, (), limit=2, show_progress=False
        )
        connection.execute(
            f"CREATE INDEX {TASK_COMPACTION_INDEX} "
            "ON tasks(plan_token, endpoint, task_id, rows) "
            "WHERE active=1 AND status='success'"
        )
        timing: dict[str, float] = {}
        indexed = _load_unassigned_shards(
            connection, (), limit=2, show_progress=False, timing=timing
        )
        assert [row.task_id for row in indexed] == [row.task_id for row in baseline]
        assert [row.task_id for row in indexed] == ["aa", "bb"]
        assert "unassigned_source_index_order" in timing
        assert "unassigned_source_endpoint_sort" not in timing
        selection_plan = connection.execute(
            f"EXPLAIN QUERY PLAN SELECT t.task_id FROM tasks AS t "
            f"INDEXED BY {TASK_COMPACTION_INDEX} "
            "LEFT JOIN l1_compaction_members AS m ON m.task_id=t.task_id "
            "WHERE t.active=1 AND t.plan_token=? AND t.status='success' "
            "AND m.task_id IS NULL ORDER BY t.endpoint, t.task_id LIMIT ?",
            ("test-plan", 2),
        ).fetchall()
        assert not any("TEMP B-TREE" in str(row[3]) for row in selection_plan)
        count_plan = connection.execute(
            f"EXPLAIN QUERY PLAN SELECT endpoint, COUNT(*), SUM(rows) "
            f"FROM tasks INDEXED BY {TASK_COMPACTION_INDEX} "
            "WHERE active=1 AND status='success' AND plan_token=? "
            "GROUP BY endpoint",
            ("test-plan",),
        ).fetchall()
        assert any("COVERING INDEX" in str(row[3]) for row in count_plan)
    finally:
        connection.close()


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
    status_receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert all(
        status_receipt["stage_seconds"][name] >= 0
        for name in (
            "stale_contract_audit", "stale_source_contract_query",
            "stale_existing_status_query", "stale_source_join_query",
            "stale_derivative_metadata_scan", "stale_state_apply",
            "unassigned_source_load", "batch_planning",
            "unassigned_source_query", "unassigned_source_metadata",
            "segment_build", "query_view_publish", "stale_output_quarantine",
            "status_task_count", "status_member_count", "status_projection_write",
        )
    )
    assert status_receipt["view_endpoint_seconds"][ENDPOINT] >= 0
    assert status_receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
    assert set(status_receipt["stage_resources"]) == {
        "before_stale_contract_audit", "after_stale_contract_audit",
        "after_unassigned_source_load",
        "after_segment_build", "after_query_view_publish",
    }
    before = status_receipt["stage_resources"]["before_stale_contract_audit"]
    after = status_receipt["stage_resources"]["after_stale_contract_audit"]
    if before["process_read_bytes"] is not None:
        assert after["process_read_bytes"] >= before["process_read_bytes"]

    # Idempotent reruns publish the same views without duplicating source rows.
    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (3, 0)
    status_receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert status_receipt["view_endpoint_actions"][ENDPOINT] == "reused_verified_paths"

    _publish_tasks(tmp_path, ["ff"])
    assert run(_args(tmp_path)) == 0
    assert _segment_counts(tmp_path) == (4, 0)
    status_receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert status_receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
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

    # A derivative with unchanged row count but a different schema must also
    # become stale; the single-open metadata path checks both contracts.
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        segment_path_value, segment_rows = connection.execute(
            "SELECT output_path, output_rows FROM l1_compaction_segments "
            "WHERE status='success' ORDER BY segment_id LIMIT 1"
        ).fetchone()
        segment_path = Path(segment_path_value)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": f"bad-{index}", "close": 0.0, "unexpected": 1}
                for index in range(int(segment_rows))
            ]
        ),
        segment_path,
    )
    assert run([*_args(tmp_path), "--audit-only"]) == 2
    assert run(_args(tmp_path)) == 0
    assert run([*_args(tmp_path), "--audit-only"]) == 0


def test_l1_view_catalog_without_signatures_rebuilds_before_reuse(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    with duckdb.connect(str(database_path)) as database:
        database.execute("ALTER TABLE l1_catalog DROP COLUMN view_input_signature")
    assert run(_args(tmp_path)) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
    with duckdb.connect(str(database_path), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 2
        columns = {row[1] for row in database.execute(
            "PRAGMA table_info('l1_catalog')"
        ).fetchall()}
        assert "view_input_signature" in columns
    assert run(_args(tmp_path)) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "reused_verified_paths"


def test_l1_view_sql_drift_rebuilds_from_verified_segments(tmp_path: Path) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    with duckdb.connect(str(database_path)) as database:
        database.execute(
            "CREATE OR REPLACE VIEW openbb_l1_equity_price_historical "
            "AS SELECT -1 AS close"
        )
    assert run(_args(tmp_path)) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
    with duckdb.connect(str(database_path), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 2


def test_l1_only_rebuilds_endpoint_with_changed_segment_paths(tmp_path: Path) -> None:
    other_endpoint = "equity.test.alternative"
    _publish_tasks(tmp_path, ["aa", "bb"])
    _publish_tasks(tmp_path, ["cc", "dd"], endpoint=other_endpoint)
    args = _args(tmp_path)
    del args[args.index("--endpoint"):args.index("--endpoint") + 2]
    assert run(args) == 0
    _publish_tasks(tmp_path, ["ee"])
    assert run(args) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"] == {
        ENDPOINT: "rebuilt", other_endpoint: "reused_verified_paths"
    }
    with duckdb.connect(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 3
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_test_alternative"
        ).fetchone()[0] == 2


def test_l1_deferred_endpoint_removes_old_view_until_queryable_again(
    tmp_path: Path, monkeypatch
) -> None:
    from scripts import compact_openbb_l1 as compactor

    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    monkeypatch.setattr(compactor, "MAX_QUERY_VIEW_SCHEMA_VARIANTS", 0)
    assert run(_args(tmp_path)) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "deferred"
    with duckdb.connect(str(database_path), read_only=True) as database:
        assert database.execute(
            "SELECT view_name FROM duckdb_views() "
            "WHERE view_name='openbb_l1_equity_price_historical'"
        ).fetchone() is None
    monkeypatch.setattr(
        compactor, "MAX_QUERY_VIEW_SCHEMA_VARIANTS",
        MAX_QUERY_VIEW_SCHEMA_VARIANTS,
    )
    assert run(_args(tmp_path)) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
    with duckdb.connect(str(database_path), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 2


def test_missing_l1_derivative_is_detected_and_rebuilt(tmp_path: Path) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    manifest_path = tmp_path / "_state" / "openbb_archive.sqlite3"
    with sqlite3.connect(manifest_path) as connection:
        missing = Path(connection.execute(
            "SELECT output_path FROM l1_compaction_segments "
            "WHERE status='success' ORDER BY segment_id LIMIT 1"
        ).fetchone()[0])
    missing.unlink()
    assert run([*_args(tmp_path), "--audit-only"]) == 2
    assert run(_args(tmp_path)) == 0
    with duckdb.connect(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 2
    with sqlite3.connect(manifest_path) as connection:
        assert connection.execute(
            "SELECT stale_reason FROM l1_compaction_segments "
            "WHERE status='quarantined' ORDER BY segment_id LIMIT 1"
        ).fetchone()[0] == "L1 output file is missing"


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
