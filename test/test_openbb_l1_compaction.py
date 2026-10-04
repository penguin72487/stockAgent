from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import sqlite3

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.download_openbb_archive import DownloadTask, Manifest, TaskResult
from scripts import compact_openbb_l1 as compactor
from scripts.benchmark_openbb_l1_stale_scan import (
    _run_metadata_variant,
    _run_source_metadata_variant,
    _run_variant,
    _source_variant_sequence,
)
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
    _write_status,
    parse_args,
    run,
)


ENDPOINT = "equity.price.historical"


@pytest.mark.parametrize("optional_state", ("available", "missing", "malformed"))
def test_l1_stage_resources_preserves_cgroup_memory_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, optional_state: str,
) -> None:
    group = "sys/fs/cgroup/system.slice/stockagent-openbb-l1-compaction.service"
    files = {
        "proc/self/status": "VmRSS:\t123 kB\nVmSwap:\t2 kB\n",
        "proc/self/io": "read_bytes: 31\nwrite_bytes: 47\n",
        "proc/self/cgroup": "0::/system.slice/stockagent-openbb-l1-compaction.service\n",
        f"{group}/memory.current": "900\n",
        f"{group}/memory.swap.current": "3\n",
        f"{group}/memory.events": "low 0\nhigh 4\n",
        f"{group}/memory.pressure": "some total=9\nfull avg10=0.00 total=11\n",
        f"{group}/io.pressure": "full avg10=0.00 total=13\n",
    }
    if optional_state != "missing":
        files[f"{group}/memory.peak"] = "1200\n"
        files[f"{group}/memory.stat"] = "anon 500\nfile 300\nfile_mapped 50\n"
        files[f"{group}/memory.events"] += "max 7\noom 2\noom_kill 1\noom_group_kill 0\n"
    if optional_state == "malformed":
        files[f"{group}/memory.peak"] = "unknown\n"
        files[f"{group}/memory.stat"] = "anon unknown\nfile 300\n"
        files[f"{group}/memory.events"] = "high 4\nmax unknown\noom 2\noom_kill 1\n"
    for relative, content in files.items():
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    monkeypatch.setattr(compactor, "Path", lambda path: tmp_path / path.lstrip("/"))

    observed = compactor._stage_resources()

    assert observed["process_rss_bytes"] == 123 * 1024
    assert observed["process_swap_bytes"] == 2 * 1024
    assert observed["process_read_bytes"] == 31
    assert observed["process_write_bytes"] == 47
    assert observed["cgroup_memory_bytes"] == 900
    assert observed["cgroup_swap_bytes"] == 3
    assert observed["cgroup_memory_high_events"] == 4
    assert observed["cgroup_memory_full_stall_us"] == 11
    assert observed["cgroup_io_full_stall_us"] == 13
    complete = optional_state == "available"
    present = optional_state != "missing"
    assert observed["cgroup_memory_peak_bytes"] == (1200 if complete else None)
    assert observed["cgroup_memory_anon_bytes"] == (500 if complete else None)
    assert observed["cgroup_memory_file_bytes"] == (300 if present else None)
    assert observed["cgroup_memory_max_events"] == (7 if complete else None)
    assert observed["cgroup_memory_oom_events"] == (2 if present else None)
    assert observed["cgroup_memory_oom_kill_events"] == (1 if present else None)


def test_l1_deep_audit_reads_each_source_metadata_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    source_opens = {Path(task.output_path).resolve(): 0 for task in tasks}
    original = compactor.pq.ParquetFile

    def tracked(path: str | Path, *args: object, **kwargs: object):
        selected = Path(path).resolve()
        if selected in source_opens:
            source_opens[selected] += 1
        return original(path, *args, **kwargs)

    monkeypatch.setattr(compactor.pq, "ParquetFile", tracked)
    assert run([*_args(tmp_path), "--audit-only"]) == 0
    assert source_opens and set(source_opens.values()) == {1}


def test_l1_source_metadata_benchmark_variants_match(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pq.write_table(pa.Table.from_pylist([{"value": 1}, {"value": 2}]), source)
    metadata = pq.ParquetFile(source).metadata
    uncompressed = sum(
        metadata.row_group(index).total_byte_size
        for index in range(metadata.num_row_groups)
    )
    rows = [("task-a", str(source), 2, uncompressed)]
    double = _run_source_metadata_variant(rows, single_open=False)
    single = _run_source_metadata_variant(rows, single_open=True)
    assert double["mismatches"] == single["mismatches"] == 0
    assert double["observed_sha256"] == single["observed_sha256"]


def test_l1_journal_fails_closed_if_source_sql_gains_untracked_task_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compactor._stale_source_contract_sql

    def changed_sql(filter_clause: str, *, member_first: bool = False) -> str:
        return original(filter_clause, member_first=member_first).replace(
            "t.rows", "t.untracked_rows"
        )

    monkeypatch.setattr(compactor, "_stale_source_contract_sql", changed_sql)
    with pytest.raises(RuntimeError, match="untracked task columns"):
        compactor._source_audit_fingerprint()


def test_l1_journal_rejects_filtered_global_watermark(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        parse_args([*_args(tmp_path), "--incremental-source-audit"])


@pytest.mark.parametrize("corruption", ("delete", "endpoint"))
def test_l1_status_rejects_member_corruption_before_replacing_receipt(
    tmp_path: Path, corruption: str
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    receipt_path = tmp_path / "_state" / "l1_compaction_latest.json"
    status_path = tmp_path / "catalog" / "l1_compaction_status.parquet"
    old_receipt = receipt_path.read_bytes()
    old_status = status_path.read_bytes()
    connection = _open_manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        task_id = str(connection.execute(
            "SELECT task_id FROM l1_compaction_members LIMIT 1"
        ).fetchone()[0])
        if corruption == "delete":
            connection.execute(
                "DELETE FROM l1_compaction_members WHERE task_id=?", (task_id,)
            )
        else:
            connection.execute(
                "UPDATE l1_compaction_members SET endpoint=? WHERE task_id=?",
                ("wrong.endpoint", task_id),
            )
        connection.commit()
        with pytest.raises(RuntimeError, match="member/segment count mismatch"):
            _write_status(
                connection, tmp_path, (), stale_segments=0, new_segments=0,
                deferred_query_views={},
            )
    finally:
        connection.close()
    assert receipt_path.read_bytes() == old_receipt
    assert status_path.read_bytes() == old_status


def test_l1_source_benchmark_abba_preserves_both_query_orders() -> None:
    assert _source_variant_sequence("abba") == (False, True, True, False)
    assert _source_variant_sequence("baab") == (True, False, False, True)
    assert _source_variant_sequence("both") == (True, False)
    assert _source_variant_sequence("none") == ()


def test_l1_journal_incremental_source_audit_preserves_rebuild_contract(
    tmp_path: Path,
) -> None:
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    args = _incremental_args(tmp_path)
    assert run(args) == 0
    receipt_path = tmp_path / "_state" / "l1_compaction_latest.json"
    first = json.loads(receipt_path.read_text())
    assert first["source_audit"]["mode"] == "full"
    assert first["source_audit"]["reason"] == "checkpoint_missing"
    assert first["stale_source_scan_order"] == "member_first"

    assert run(args) == 0
    quiet = json.loads(receipt_path.read_text())
    assert quiet["source_audit"]["mode"] == "incremental"
    assert quiet["source_audit"]["changed_task_ids"] == 0
    assert quiet["stage_seconds"]["stale_source_join_query"] == 0
    assert quiet["compacted_files"] == 2

    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": "2024-01-01", "close": 10.0},
                {"date": "2024-01-02", "close": 11.0},
            ]
        ),
        Path(tasks[0].output_path),
    )
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        connection.execute(
            "UPDATE tasks SET rows=2, updated_at=? WHERE task_id='aa'",
            (datetime.now(timezone.utc).isoformat(),),
        )
        connection.commit()
    assert run(args) == 0
    changed = json.loads(receipt_path.read_text())
    assert changed["source_audit"]["mode"] == "incremental"
    assert changed["source_audit"]["changed_task_ids"] == 1
    assert changed["stale_source_scan_order"] == "journal_task_id"
    assert changed["stale_segments"] == 1
    assert changed["compacted_rows"] == 3
    assert changed["l0_deleted"] is False

    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        connection.execute("DELETE FROM tasks WHERE task_id='bb'")
        connection.commit()
    assert run(args) == 0
    deleted = json.loads(receipt_path.read_text())
    assert deleted["source_audit"]["mode"] == "incremental"
    assert deleted["source_audit"]["changed_task_ids"] == 1
    assert deleted["stale_segments"] == 1
    assert deleted["success_files"] == deleted["compacted_files"] == 1
    assert run([*args, "--force-full-source-audit"]) == 0
    forced = json.loads(receipt_path.read_text())
    assert forced["source_audit"]["mode"] == "full"
    assert forced["source_audit"]["reason"] == "force_full"


def test_l1_journal_missing_trigger_forces_full_before_source_rebuild(
    tmp_path: Path,
) -> None:
    tasks = _publish_tasks(tmp_path, ["aa", "bb"])
    args = _incremental_args(tmp_path)
    assert run(args) == 0
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"date": "2024-01-01", "close": 10.0},
                {"date": "2024-01-02", "close": 11.0},
            ]
        ),
        Path(tasks[0].output_path),
    )
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        connection.execute("DROP TRIGGER l1_source_task_update")
        connection.execute(
            "UPDATE tasks SET rows=2, updated_at=? WHERE task_id='aa'",
            (datetime.now(timezone.utc).isoformat(),),
        )
        connection.commit()
    assert run(args) == 0
    receipt = json.loads(
        (tmp_path / "_state" / "l1_compaction_latest.json").read_text()
    )
    assert receipt["source_audit"]["mode"] == "full"
    assert receipt["source_audit"]["reason"] == "schema_changed"
    assert receipt["stale_segments"] == 1
    assert receipt["compacted_rows"] == 3


def test_l1_journal_quiet_source_still_audits_derivative_metadata(
    tmp_path: Path,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    args = _incremental_args(tmp_path)
    assert run(args) == 0
    with sqlite3.connect(tmp_path / "_state" / "openbb_archive.sqlite3") as connection:
        output = Path(str(connection.execute(
            "SELECT output_path FROM l1_compaction_segments "
            "WHERE status='success' LIMIT 1"
        ).fetchone()[0]))
    pq.write_table(
        pa.Table.from_pylist(
            [{"date": f"bad-{i}", "close": 0.0} for i in range(3)]
        ),
        output,
    )
    assert run(args) == 0
    receipt = json.loads(
        (tmp_path / "_state" / "l1_compaction_latest.json").read_text()
    )
    assert receipt["source_audit"]["mode"] == "incremental"
    assert receipt["source_audit"]["changed_task_ids"] == 0
    assert receipt["stale_segments"] == 1
    assert receipt["compacted_files"] == 2
    assert receipt["l0_deleted"] is False


def test_l1_systemd_memory_relief_keeps_hard_limit_and_audit() -> None:
    unit = Path(
        "deploy/systemd/stockagent-openbb-l1-compaction.service.in"
    ).read_text(encoding="utf-8")
    assert "MemoryHigh=2816M" in unit
    assert "MemoryMax=3G" in unit
    assert "--incremental-source-audit" in unit
    assert "--max-source-files 32768" in unit
    assert "--archive-idle-only" in unit


def test_l1_metadata_probe_compares_complete_rows_and_schema(tmp_path: Path) -> None:
    path = tmp_path / "segment.parquet"
    table = pa.table({"date": ["2026-09-24", "2026-09-25"], "close": [1.0, 2.0]})
    pq.write_table(table, path)
    fingerprint = hashlib.sha256(
        table.schema.remove_metadata().serialize().to_pybytes()
    ).hexdigest()
    rows = [("segment-1", str(path), 2, fingerprint)]
    variants = [
        _run_metadata_variant(rows, use_read_metadata=method, workers=workers)
        for method, workers in ((False, 1), (True, 1), (False, 4))
    ]
    assert {variant["observed_sha256"] for variant in variants} == {
        variants[0]["observed_sha256"]
    }
    assert all(variant["mismatches"] == 0 for variant in variants)

    path.write_bytes(b"not a parquet file")
    broken = _run_metadata_variant(rows, use_read_metadata=False, workers=1)
    assert broken["mismatches"] == 1
    assert broken["observed_sha256"] != variants[0]["observed_sha256"]


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
    receipt = json.loads(
        (tmp_path / "_state" / "l1_compaction_latest.json").read_text()
    )
    assert receipt["stale_source_scan_order"] == "member_first"
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
        filtered_baseline = [
            tuple(row) for row in connection.execute(
                _stale_source_contract_sql(" AND s.endpoint=?"),
                (prefix, ENDPOINT, prefix),
            )
        ]
        filtered_candidate = [
            tuple(row) for row in connection.execute(
                _stale_source_contract_sql(
                    " AND s.endpoint=?", member_first=True
                ),
                (prefix, ENDPOINT, prefix),
            )
        ]
        assert filtered_candidate == filtered_baseline
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
        assert baseline["process_rchar_bytes"] is None or baseline[
            "process_rchar_bytes"
        ] >= 0
        assert candidate["process_read_bytes"] is None or candidate[
            "process_read_bytes"
        ] >= 0
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


def _incremental_args(root: Path) -> list[str]:
    args = _args(root)
    start = args.index("--endpoint")
    del args[start:start + 2]
    return [*args, "--incremental-source-audit"]


def _sec_grouped_args(root: Path) -> list[str]:
    args = _args(root)
    args[args.index("--endpoint") + 1] = compactor.SCHEMA_GROUPED_ENDPOINT
    args[args.index("--max-files-per-segment") + 1] = "1"
    return [*args, "--schema-grouped-sec-views"]


def _sec_segment_paths(root: Path) -> list[Path]:
    with sqlite3.connect(root / "_state/openbb_archive.sqlite3") as connection:
        return [Path(row[0]) for row in connection.execute(
            "SELECT output_path FROM l1_compaction_segments WHERE status='success' "
            "ORDER BY segment_id"
        )]


def _sec_action(root: Path) -> str:
    receipt = json.loads((root / "_state/l1_compaction_latest.json").read_text())
    return receipt["view_endpoint_actions"][compactor.SCHEMA_GROUPED_ENDPOINT]


def test_l1_sec_grouping_is_opt_in_reuses_audit_and_fences_reuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVOCATION_ID", "a" * 32)
    assert parse_args([]).schema_grouped_sec_views is False
    _publish_tasks(tmp_path, ["aa", "bb", "cc"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    assert _sec_action(tmp_path) == "rebuilt_schema_groups"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    receipt = json.loads(receipt_path.read_text())
    assert receipt["systemd_invocation_id"] == "a" * 32
    assert receipt["started_at_utc"] <= receipt["finished_at_utc"]
    assert receipt["state"] == "completed"
    assert json.loads(receipt_path.read_text())["query_view_policy"] == {
        "schema_grouped_sec_requested": True,
        "schema_grouped_policy_revision": compactor.SCHEMA_GROUPED_POLICY_REVISION,
        "physical_identity_revision": compactor.SCHEMA_IDENTITY_REVISION,
    }
    paths = _sec_segment_paths(tmp_path)
    original = compactor.pq.ParquetFile
    opens: list[Path] = []

    def tracked(path, *positional, **kwargs):
        opens.append(Path(path))
        return original(path, *positional, **kwargs)

    monkeypatch.setattr(compactor.pq, "ParquetFile", tracked)
    assert run(args) == 0
    assert _sec_action(tmp_path) == "reused_verified_schema_groups"
    assert sorted(opens) == sorted(paths)
    with duckdb.connect(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        assert database.execute(
            "SELECT * FROM openbb_l1_regulators_sec_filing_headers ORDER BY date"
        ).fetchall() == [("2024-01-01", 0.0), ("2024-01-02", 1.0), ("2024-01-03", 2.0)]
        sql = database.execute("SELECT sql FROM duckdb_views() WHERE view_name=?", (
            compactor._view_name(compactor.SCHEMA_GROUPED_ENDPOINT),
        )).fetchone()[0]
        assert "union_by_name = CAST('f' AS BOOLEAN)" in sql
    assert run(args[:-1]) == 0
    assert _sec_action(tmp_path) == "rebuilt"
    assert json.loads(receipt_path.read_text())["query_view_policy"]["schema_grouped_sec_requested"] is False


def test_l1_multischema_group_sql_survives_reopen_and_reuses_other_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    tasks = _publish_tasks(
        tmp_path, [f"sec-{index:02}" for index in range(12)],
        endpoint=compactor.SCHEMA_GROUPED_ENDPOINT,
    )
    for index, task in enumerate(tasks):
        pq.write_table(pa.table({
            "date": [f"2024-01-{index + 1:02}"], "close": [float(index)],
            f"extra{index}": [index],
        }), task.output_path)
    _publish_tasks(tmp_path, ["other"])
    args = _sec_grouped_args(tmp_path)
    del args[args.index("--endpoint"):args.index("--endpoint") + 2]
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    with sqlite3.connect(tmp_path / "_state/openbb_archive.sqlite3") as manifest:
        paths = [Path(row[0]) for row in manifest.execute(
            "SELECT output_path FROM l1_compaction_segments "
            "WHERE status='success' AND endpoint=? ORDER BY segment_id",
            (compactor.SCHEMA_GROUPED_ENDPOINT,),
        )]
    with duckdb.connect(str(database_path), read_only=True) as database:
        baseline = database.execute(
            compactor._parquet_view_query(paths) + " ORDER BY date"
        ).fetchall()
        assert len(baseline) == 12
    all_paths = _sec_segment_paths(tmp_path)
    original = compactor.pq.ParquetFile
    opened: list[Path] = []

    def tracked(path, *positional, **kwargs):
        opened.append(Path(path))
        return original(path, *positional, **kwargs)

    monkeypatch.setattr(compactor.pq, "ParquetFile", tracked)
    sql_signatures = []
    for _ in range(3):
        opened.clear()
        assert run(args) == 0
        receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
        assert receipt["view_endpoint_actions"] == {
            compactor.SCHEMA_GROUPED_ENDPOINT: "reused_verified_schema_groups",
            ENDPOINT: "reused_verified_paths",
        }
        # Every old footer is still audited on every reuse, including non-SEC.
        assert sorted(opened) == sorted(all_paths)
        catalog = compactor._reusable_view_catalog(
            database_path, config={"threads": 1, "memory_limit": "512MB"},
        )
        assert catalog is not None
        sql_signatures.append(catalog[compactor.SCHEMA_GROUPED_ENDPOINT][3])
        with duckdb.connect(str(database_path), read_only=True) as database:
            assert database.execute(
                "SELECT * FROM openbb_l1_regulators_sec_filing_headers ORDER BY date"
            ).fetchall() == baseline
    assert len(set(sql_signatures)) == 1


@pytest.mark.parametrize(("failed", "deferred", "state"), [
    (0, 0, "completed"),
    (2, 1, "completed_with_failures"),
    (0, 1, "completed_with_deferred_segments"),
])
def test_l1_status_retains_job_failure_separate_from_source_backlog(
    tmp_path: Path, failed: int, deferred: int, state: str,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    with compactor._open_manifest(tmp_path / "_state/openbb_archive.sqlite3") as connection:
        result = compactor._write_status(
            connection, tmp_path, (), stale_segments=0, new_segments=0,
            deferred_query_views={}, failed_segments=failed,
            deferred_failed_segments=deferred, started_at_utc=compactor._now(),
        )
    assert result["state"] == state
    assert result["failed_segments"] == failed
    assert result["deferred_failed_segments"] == deferred
    assert result["l0_deleted"] is False


def test_l1_grouping_never_opts_other_endpoints_in(tmp_path: Path) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run([*_args(tmp_path), "--schema-grouped-sec-views"]) == 0
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "rebuilt"
    with duckdb.connect(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        sql = database.execute("SELECT sql FROM duckdb_views() WHERE view_name=?", (
            compactor._view_name(ENDPOINT),
        )).fetchone()[0]
        assert "union_by_name = CAST('t' AS BOOLEAN)" in sql


def test_l1_sec_unknown_physical_proof_falls_back_to_true(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    original_identities = compactor.schema_identities
    monkeypatch.setattr(compactor, "schema_identities", lambda pf: (original_identities(pf)[0], None))
    assert run(_sec_grouped_args(tmp_path)) == 0
    assert _sec_action(tmp_path) == "rebuilt"


@pytest.mark.parametrize("error_type", (MemoryError, pa.ArrowMemoryError))
@pytest.mark.parametrize("grouped", (False, True))
def test_footer_memory_pressure_never_marks_healthy_segments_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_type, grouped: bool,
) -> None:
    _publish_tasks(
        tmp_path, ["aa", "bb"],
        endpoint=compactor.SCHEMA_GROUPED_ENDPOINT if grouped else ENDPOINT,
    )
    args = _sec_grouped_args(tmp_path) if grouped else _args(tmp_path)
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    previous_database, previous_receipt = database_path.read_bytes(), receipt_path.read_bytes()
    counts = _segment_counts(tmp_path)

    def out_of_memory(*args, **kwargs):
        raise error_type("test resource pressure")

    monkeypatch.setattr(compactor.pq, "ParquetFile", out_of_memory)
    with pytest.raises(error_type, match="test resource pressure"):
        run(args)
    assert _segment_counts(tmp_path) == counts
    assert database_path.read_bytes() == previous_database
    assert receipt_path.read_bytes() == previous_receipt


@pytest.mark.parametrize("error_type", (MemoryError, duckdb.OutOfMemoryException))
def test_build_memory_pressure_does_not_create_source_failure_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_type,
) -> None:
    _publish_tasks(tmp_path, ["aa"])
    args = _args(tmp_path)
    assert run(args) == 0
    previous = (tmp_path / "_state/l1_compaction_latest.json").read_bytes()
    counts = _segment_counts(tmp_path)
    _publish_tasks(tmp_path, ["bb"])

    def out_of_memory(*args, **kwargs):
        raise error_type("test process budget")

    monkeypatch.setattr(compactor, "compact_parquet_files", out_of_memory)
    with pytest.raises(error_type, match="test process budget"):
        run(args)
    assert _segment_counts(tmp_path) == counts
    assert (tmp_path / "_state/l1_compaction_latest.json").read_bytes() == previous
    with sqlite3.connect(tmp_path / "_state/openbb_archive.sqlite3") as connection:
        assert connection.execute(f"SELECT COUNT(*) FROM {compactor.FAILURE_TABLE}").fetchone()[0] == 0


@pytest.mark.parametrize("mutation", ("missing", "corrupt", "touch", "replace"))
@pytest.mark.parametrize("stage", ("before_publish", "after_bind", "after_persisted_validation"))
def test_l1_sec_nonfirst_source_drift_keeps_previous_catalog_and_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str, stage: str,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb", "cc"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    old_database, old_receipt = database_path.read_bytes(), receipt_path.read_bytes()
    target = _sec_segment_paths(tmp_path)[-1]

    def mutate():
        if mutation == "missing":
            target.unlink()
        elif mutation == "corrupt":
            target.write_bytes(b"invalid parquet footer")
        elif mutation == "touch":
            info = target.stat()
            os.utime(target, ns=(info.st_atime_ns, info.st_mtime_ns + 1))
        else:
            replacement = target.with_suffix(".replacement")
            replacement.write_bytes(target.read_bytes())
            os.replace(replacement, target)

    if stage == "before_publish":
        original = compactor._publish_views

        def before_publish(*positional, **kwargs):
            mutate()
            return original(*positional, **kwargs)

        monkeypatch.setattr(compactor, "_publish_views", before_publish)
    elif stage == "after_bind":
        original = compactor._schema_grouped_view

        def after_bind(*positional, **kwargs):
            result = original(*positional, **kwargs)
            mutate()
            return result

        monkeypatch.setattr(compactor, "_schema_grouped_view", after_bind)
    else:
        original = compactor._reusable_view_catalog

        def after_persisted_validation(path, **kwargs):
            result = original(path, **kwargs)
            if path.suffix == ".tmp":
                mutate()
            return result

        monkeypatch.setattr(compactor, "_reusable_view_catalog", after_persisted_validation)
    with pytest.raises((RuntimeError, FileNotFoundError), match="changed|No such file"):
        run(args)
    assert database_path.read_bytes() == old_database
    assert receipt_path.read_bytes() == old_receipt
    assert not list(tmp_path.glob(".openbb_l1.duckdb.*.tmp"))


@pytest.mark.parametrize("policy_change", ("revision", "reader", "physical"))
def test_l1_sec_grouping_policy_invalidates_reuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, policy_change: str,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    if policy_change == "revision":
        monkeypatch.setattr(compactor, "SCHEMA_GROUPED_POLICY_REVISION", 999)
    elif policy_change == "reader":
        original = compactor._schema_group_reader_policy

        def changed_reader(database):
            return {**original(database), "reader": "different-reader-version"}

        monkeypatch.setattr(compactor, "_schema_group_reader_policy", changed_reader)
    else:
        original = compactor.schema_identities

        def changed_identity(parquet_file):
            arrow, physical = original(parquet_file)
            return arrow, "new:" + physical

        monkeypatch.setattr(compactor, "schema_identities", changed_identity)
    assert run(args) == 0
    assert _sec_action(tmp_path) == "rebuilt_schema_groups"


@pytest.mark.parametrize("relocation", ("hive", "alias", "symlink"))
def test_l1_sec_noncanonical_paths_keep_original_reader_semantics(
    tmp_path: Path, relocation: str,
) -> None:
    _publish_tasks(tmp_path, ["aa"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    original = _sec_segment_paths(tmp_path)[0]
    target = tmp_path / ("year=2026" if relocation == "hive" else "external") / original.name
    target.parent.mkdir()
    original.rename(target)
    if relocation == "symlink":
        original.symlink_to(target)
    else:
        with sqlite3.connect(tmp_path / "_state/openbb_archive.sqlite3") as connection:
            connection.execute(
                "UPDATE l1_compaction_segments SET output_path=?", (str(target),)
            )
    assert run(args) == 0
    assert _sec_action(tmp_path) == "rebuilt"
    with duckdb.connect(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        query = "SELECT * FROM openbb_l1_regulators_sec_filing_headers"
        actual = database.execute(query).fetchall()
        expected = database.execute(compactor._parquet_view_query(
            [original if relocation == "symlink" else target]
        )).fetchall()
        assert actual == expected
        if relocation == "hive":
            assert actual == [("2024-01-01", 0.0, 2026)]


def test_l1_sec_representative_schema_disagreement_keeps_original_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    original = compactor._schema_grouped_view

    class WrongSchema:
        def __init__(self, database):
            self.database = database

        def execute(self, query, *parameters):
            if query.startswith("DESCRIBE") and "union_by_name=false" in query:
                return SimpleNamespace(fetchall=lambda: [("wrong", "INTEGER")])
            return self.database.execute(query, *parameters)

    def mismatched(database, *positional, **kwargs):
        return original(WrongSchema(database), *positional, **kwargs)

    monkeypatch.setattr(compactor, "_schema_grouped_view", mismatched)
    assert run(_sec_grouped_args(tmp_path)) == 0
    assert _sec_action(tmp_path) == "rebuilt"


@pytest.mark.parametrize("field,value", (
    ("output_rows", 99), ("output_bytes", 99), ("schema_fingerprint", "changed"),
    ("segment_id", "a" * 24),
))
def test_l1_sec_manifest_receipt_drift_cannot_reuse_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: object,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    original = compactor._publish_views
    old_database = (tmp_path / "openbb_l1.duckdb").read_bytes()

    def changed_manifest(connection, *positional, **kwargs):
        # Deliberately violate the single-writer contract after metadata audit.
        connection.execute(
            f"UPDATE l1_compaction_segments SET {field}=? "
            "WHERE segment_id=(SELECT MAX(segment_id) FROM l1_compaction_segments)",
            (value,),
        )
        connection.commit()
        return original(connection, *positional, **kwargs)

    monkeypatch.setattr(compactor, "_publish_views", changed_manifest)
    with pytest.raises(RuntimeError, match="manifest changed"):
        run(args)
    assert (tmp_path / "openbb_l1.duckdb").read_bytes() == old_database


def test_l1_sec_reader_policy_drift_during_publish_keeps_previous_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    old_database = (tmp_path / "openbb_l1.duckdb").read_bytes()
    original = compactor._schema_group_reader_policy
    calls = 0

    def drifting_policy(database):
        nonlocal calls
        calls += 1
        return {**original(database), "policy_changed": calls > 1}

    monkeypatch.setattr(compactor, "_schema_group_reader_policy", drifting_policy)
    with pytest.raises(RuntimeError, match="reader policy changed"):
        run(args)
    assert (tmp_path / "openbb_l1.duckdb").read_bytes() == old_database
    assert not list(tmp_path.glob(".openbb_l1.duckdb.*.tmp"))


def test_l1_sec_unknown_proof_fallback_still_fences_nonfirst_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"], endpoint=compactor.SCHEMA_GROUPED_ENDPOINT)
    args = _sec_grouped_args(tmp_path)
    assert run(args) == 0
    original_identities = compactor.schema_identities
    monkeypatch.setattr(compactor, "schema_identities", lambda pf: (original_identities(pf)[0], None))
    original = compactor._schema_grouped_view
    target = _sec_segment_paths(tmp_path)[-1]
    old_database = (tmp_path / "openbb_l1.duckdb").read_bytes()

    def mutate_unsupported(*positional, **kwargs):
        assert original(*positional, **kwargs) is None
        target.write_bytes(b"broken nonfirst file")
        return None

    monkeypatch.setattr(compactor, "_schema_grouped_view", mutate_unsupported)
    with pytest.raises((RuntimeError, duckdb.Error)):
        run(args)
    assert (tmp_path / "openbb_l1.duckdb").read_bytes() == old_database


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
    assert after["process_cpu_ns"] >= before["process_cpu_ns"] > 0
    assert after["monotonic_ns"] >= before["monotonic_ns"] > 0
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


def test_l1_view_connections_share_configured_resource_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    args = _args(tmp_path)
    args[args.index("--threads") + 1] = "2"
    args[args.index("--memory-limit") + 1] = "128MB"
    original = compactor.duckdb.connect
    observed: list[tuple[bool, dict[str, object], tuple[object, ...]]] = []

    def tracked(database: str, *positional: object, **kwargs: object):
        connection = original(database, *positional, **kwargs)
        if "openbb_l1.duckdb" in str(database):
            settings = connection.execute(
                "SELECT current_setting('threads'), current_setting('memory_limit')"
            ).fetchone()
            observed.append((bool(kwargs.get("read_only")), kwargs["config"], settings))
        return connection

    monkeypatch.setattr(compactor.duckdb, "connect", tracked)
    assert run(args) == 0
    assert run(args) == 0
    assert [read_only for read_only, _, _ in observed] == [
        False, False, True, True, False, True,
    ]
    for _, config, settings in observed:
        assert config == {"threads": 2, "memory_limit": "128MB"}
        assert settings == (2, "122.0 MiB")
    receipt = json.loads((tmp_path / "_state/l1_compaction_latest.json").read_text())
    assert receipt["view_endpoint_actions"][ENDPOINT] == "reused_verified_paths"
    with original(str(tmp_path / "openbb_l1.duckdb"), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 2


def test_l1_view_resource_failure_preserves_previous_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    args = _args(tmp_path)
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    old_database = database_path.read_bytes()
    old_receipt = receipt_path.read_bytes()
    _publish_tasks(tmp_path, ["cc"])
    original = compactor.duckdb.connect
    closed: list[bool] = []

    class FailingPublisher:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query, *parameters):
            if query.startswith("CREATE OR REPLACE VIEW"):
                raise duckdb.OutOfMemoryException("injected query-view memory limit")
            return self.connection.execute(query, *parameters)

        def close(self):
            self.connection.close()
            closed.append(True)

    def failing_connect(database: str, *positional: object, **kwargs: object):
        connection = original(database, *positional, **kwargs)
        if "openbb_l1.duckdb" in str(database) and not kwargs.get("read_only"):
            return FailingPublisher(connection)
        return connection

    with monkeypatch.context() as patch:
        patch.setattr(compactor.duckdb, "connect", failing_connect)
        with pytest.raises(duckdb.OutOfMemoryException, match="injected query-view"):
            run(args)
    assert closed == [True]
    assert database_path.read_bytes() == old_database
    assert receipt_path.read_bytes() == old_receipt
    assert not list(tmp_path.glob(".openbb_l1.duckdb.*.tmp"))
    assert run(args) == 0
    with original(str(database_path), read_only=True) as database:
        assert database.execute(
            "SELECT COUNT(*) FROM openbb_l1_equity_price_historical"
        ).fetchone()[0] == 3


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


@pytest.mark.parametrize("stage", ("connect", "metadata"))
def test_l1_reusable_catalog_memory_error_does_not_trigger_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str,
) -> None:
    _publish_tasks(tmp_path, ["aa"])
    args = _args(tmp_path)
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    old_database, old_receipt = database_path.read_bytes(), receipt_path.read_bytes()
    original = compactor.duckdb.connect
    closed = []

    class MemoryErrorCatalog:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, *args, **kwargs):
            raise duckdb.OutOfMemoryException("injected catalog metadata memory limit")

        def close(self):
            self.connection.close()
            closed.append(True)

    def failed_connect(database, *positional, **kwargs):
        if Path(database) == database_path:
            if stage == "connect":
                raise duckdb.OutOfMemoryException("injected catalog connect memory limit")
            return MemoryErrorCatalog(original(database, *positional, **kwargs))
        return original(database, *positional, **kwargs)

    monkeypatch.setattr(compactor.duckdb, "connect", failed_connect)
    with pytest.raises(duckdb.OutOfMemoryException, match="injected catalog"):
        compactor._reusable_view_catalog(
            database_path, config={"threads": 1, "memory_limit": "512MB"},
        )
    with pytest.raises(duckdb.OutOfMemoryException, match="injected catalog"):
        run(args)
    assert closed == ([True, True] if stage == "metadata" else [])
    assert database_path.read_bytes() == old_database
    assert receipt_path.read_bytes() == old_receipt
    assert not list(tmp_path.glob(".openbb_l1.duckdb.*.tmp*"))


@pytest.mark.parametrize("failure", ("checkpoint", "validation", "reused_sql", "catalog_rows"))
def test_l1_persisted_view_signature_failure_preserves_previous_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    other_endpoint = "equity.test.alternative"
    _publish_tasks(tmp_path, ["aa"])
    _publish_tasks(tmp_path, ["other"], endpoint=other_endpoint)
    args = _args(tmp_path)
    del args[args.index("--endpoint"):args.index("--endpoint") + 2]
    assert run(args) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    receipt_path = tmp_path / "_state/l1_compaction_latest.json"
    old_database, old_receipt = database_path.read_bytes(), receipt_path.read_bytes()
    _publish_tasks(tmp_path, ["bb"])
    original_connect = compactor.duckdb.connect
    original_validate = compactor._reusable_view_catalog
    writable_opens = 0
    closed = []

    class FailingCheckpoint:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, query, *parameters):
            if query == "CHECKPOINT":
                raise RuntimeError("injected persisted signature checkpoint failure")
            return self.connection.execute(query, *parameters)

        def close(self):
            self.connection.close()
            closed.append(True)

    def intercepted_connect(database, *positional, **kwargs):
        nonlocal writable_opens
        connection = original_connect(database, *positional, **kwargs)
        if str(database).endswith(".tmp") and not kwargs.get("read_only"):
            writable_opens += 1
            if writable_opens == 2:
                if failure == "checkpoint":
                    return FailingCheckpoint(connection)
                if failure in {"reused_sql", "catalog_rows"}:
                    name = compactor._view_name(other_endpoint)
                    if failure == "reused_sql":
                        connection.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT -1 AS close")
                    else:
                        # Even an internally consistent but incomplete catalog
                        # must not pass the final expected-publication check.
                        connection.execute(f"DROP VIEW {name}")
                        connection.execute("DELETE FROM l1_catalog WHERE endpoint=?", (other_endpoint,))
        return connection

    def failed_validate(path, **kwargs):
        if failure == "validation" and path.suffix == ".tmp":
            return None
        return original_validate(path, **kwargs)

    monkeypatch.setattr(compactor.duckdb, "connect", intercepted_connect)
    monkeypatch.setattr(compactor, "_reusable_view_catalog", failed_validate)
    with pytest.raises(RuntimeError, match="signature"):
        run(args)
    assert writable_opens == 2
    if failure == "checkpoint":
        assert closed == [True]
    assert database_path.read_bytes() == old_database
    assert receipt_path.read_bytes() == old_receipt
    assert not list(tmp_path.glob(".openbb_l1.duckdb.*.tmp*"))


def test_l1_view_sql_drift_rebuilds_from_verified_segments(tmp_path: Path) -> None:
    _publish_tasks(tmp_path, ["aa", "bb"])
    assert run(_args(tmp_path)) == 0
    database_path = tmp_path / "openbb_l1.duckdb"
    with duckdb.connect(str(database_path)) as database:
        database.execute(
            "CREATE OR REPLACE VIEW openbb_l1_equity_price_historical "
            "AS SELECT -1 AS close"
        )
    assert compactor._reusable_view_catalog(
        database_path, config={"threads": 1, "memory_limit": "512MB"},
    ) is None
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
    counts = {
        row["endpoint"]: (row["compacted_files"], row["compacted_rows"])
        for row in receipt["endpoint_status"]
    }
    assert counts == {ENDPOINT: (3, 3), other_endpoint: (2, 2)}
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
