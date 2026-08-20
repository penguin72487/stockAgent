from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import scripts.monitor_openbb_archive as monitor
from downloader.download_openbb_archive import DownloadTask, Manifest, TaskResult
from scripts.monitor_openbb_archive import (
    _daily_quota_projection,
    _proc_runtime_metrics,
    collect_status,
    run,
)


def test_daily_quota_projection_exposes_optimistic_lower_bound() -> None:
    projection = _daily_quota_projection(
        "fmp", "basic", 2_133_079, 250, evidence_tasks=38
    )
    assert projection == {
        "provider": "fmp",
        "detected_tier": "basic",
        "provider_only_backlog_tasks": 2_133_079,
        "daily_call_cap": 250,
        "minimum_days_at_call_cap": 8_533,
        "minimum_years_at_call_cap": 23.36,
        "evidence_tasks": 38,
        "lower_bound_only": True,
    }
    assert _daily_quota_projection("fmp", "basic", 10, 250, 0) is None


def test_monitor_excludes_active_bls_bulk_route_from_api_quota_floor(
    tmp_path: Path, monkeypatch
) -> None:
    task = DownloadTask(
        task_id="bls-local-bulk",
        endpoint="economy.survey.bls_series",
        category="economy",
        scope_key="series=INUS0001",
        kwargs={"symbol": "INUS0001"},
        providers=("bls",),
        output_path=str(tmp_path / "data" / "bls.parquet"),
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task])
        manifest.connection.execute(
            "UPDATE tasks SET error=? WHERE task_id=?",
            ("bls: daily threshold reached; quota resets tomorrow", task.task_id),
        )
        manifest.connection.commit()
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "rate_limits_rps": {"bls": 5.0},
                "concurrency": {"bls": 16},
                "rate_activity": {
                    "providers": {
                        "bls": {
                            "declared_daily_request_cap": 500,
                            "declared_daily_requests_remaining": 0,
                        }
                    }
                },
                "providers": {
                    "bls": {
                        "blocked_until": time.time() + 3600,
                        "kind": "quota",
                        "reason": "daily threshold reached",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        monitor,
        "_active_local_cooldown_bypass_endpoints",
        lambda _state_dir: {("bls", "economy.survey.bls_series")},
    )

    status = monitor.collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["fully_local_bypass_providers"] == ["bls"]
    assert status["provider_quota_feasibility"] == {}
    projection = next(
        row for row in status["provider_eta_projections"] if row["provider"] == "bls"
    )
    assert projection["local_cooldown_bypass"] is True
    assert projection["state"] == "stalled"
    assert projection["optimistic_additional_daily_resets_required"] is None
    assert status["provider_progress_stalls"] == [
        {"provider": "bls", "backlog_tasks": 1, "recent_accepted_tasks": 0}
    ]
    assert "provider_quota_deferred" not in {
        alert["code"] for alert in status["alerts"]
    }


def test_process_runtime_metrics_are_machine_readable() -> None:
    metrics = _proc_runtime_metrics(os.getpid())
    assert metrics["rss_bytes"] > 0
    assert metrics["rss_peak_bytes"] >= metrics["rss_bytes"]
    assert metrics["virtual_bytes"] >= metrics["rss_bytes"]
    assert metrics["threads"] >= 1
    assert metrics["open_fds"] >= 1


def test_monitor_distinguishes_buffered_from_executing_tasks(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "execution-state")
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        buffered = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert buffered["execution_tracking_available"] is True
        assert buffered["buffered_running_tasks"] == 1
        assert buffered["executing_tasks"] == 0

        manifest.mark_executing([task])
        executing = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert executing["execution_tracking_available"] is True
        assert executing["buffered_running_tasks"] == 0
        assert executing["executing_tasks"] == 1
    finally:
        manifest.close()


def test_monitor_reports_durable_task_retry_backoff_as_deferred(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "http-500-deferred")
    deadline = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "pending",
                "yfinance",
                0,
                None,
                42,
                error="yfinance: HTTP Error 500",
                retry_not_before=deadline,
                transient_failures=10,
            )
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["retry_tracking_available"] is True
    assert status["pending_eligible"] == 0
    assert status["pending_retry_deferred"] == 1
    assert status["next_task_retry_at"] == deadline
    assert status["runnable_retryable_tasks"] == 0
    assert status["retryable_tasks"] == 1


def test_monitor_keeps_repair_queue_visible_but_outside_main_scheduler(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="congress-repair",
        endpoint="uscongress.bill_info",
        category="uscongress",
        scope_key="https://api.congress.gov/v3/bill/111/s/3605?format=json",
        kwargs={"bill_url": "https://api.congress.gov/v3/bill/111/s/3605?format=json"},
        providers=("congress_gov",),
        output_path=str(tmp_path / "data" / "congress-repair.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "repair",
                "congress_gov",
                0,
                None,
                1,
                error="congress_gov: HTTP Error 500: Internal Server Error",
            )
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["status_counts"]["repair"] == 1
    assert status["repair_queue_tasks"] == 1
    assert status["retryable_tasks"] == 0
    assert status["actionable_unresolved_tasks"] == 0
    assert status["complete"] is False
    assert any(alert["code"] == "repair_queue_tasks" for alert in status["alerts"])


def _task(root: Path, task_id: str) -> DownloadTask:
    return DownloadTask(
        task_id=task_id,
        endpoint="equity.price.historical",
        category="equity",
        scope_key=task_id,
        kwargs={"symbol": task_id},
        providers=("yfinance",),
        output_path=str(root / "data" / f"{task_id}.parquet"),
    )


def _fred_release_task(
    root: Path,
    task_id: str,
    scope_key: str,
    *,
    release_id: int,
    offset: int = 0,
) -> DownloadTask:
    kwargs = {
        "query": "",
        "release_id": release_id,
        "search_type": "release",
        "limit": 1000,
    }
    if offset:
        kwargs["offset"] = offset
    return DownloadTask(
        task_id=task_id,
        endpoint="economy.fred_search",
        category="economy",
        scope_key=scope_key,
        kwargs=kwargs,
        providers=("fred",),
        output_path=str(root / "data" / f"{task_id}.parquet"),
    )


def _fmp_page_task(
    root: Path,
    task_id: str,
    scope_key: str,
    *,
    page: int,
) -> DownloadTask:
    return DownloadTask(
        task_id=task_id,
        endpoint="news.company",
        category="news",
        scope_key=scope_key,
        kwargs={
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2000-12-31",
            "page": page,
            "limit": 100,
        },
        providers=("fmp",),
        output_path=str(root / "data" / f"{task_id}.parquet"),
    )


def test_monitor_counts_and_full_file_audit(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    success = _task(tmp_path, "success")
    pending = _task(tmp_path, "pending")
    try:
        manifest.upsert_tasks([success, pending])
        output = Path(success.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "date": "2000-01-03",
                        "close": 1.0,
                        "_openbb_endpoint": success.endpoint,
                        "_provider": "yfinance",
                        "_scope_key": success.scope_key,
                        "_retrieved_at": "2026-01-01T00:00:00+00:00",
                        "_query_json": '{"symbol":"success"}',
                    }
                ]
            ),
            output,
        )
        manifest.claim([success])
        manifest.complete(TaskResult(success, "success", "yfinance", 1, str(output), 1))
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["total_tasks"] == 2
    assert status["accepted_tasks"] == 1
    assert status["pending_eligible"] == 1
    assert status["pending_with_error"] == 0
    assert status["pending_cooldown"] == 0
    assert status["pending_rate_limited"] == 0
    assert status["pending_attempted"] == 0
    assert status["recent_progress"] == {
        "by_category": [{"category": "equity", "tasks": 1, "rows": 1}],
        "by_provider": [{"provider": "yfinance", "tasks": 1, "rows": 1}],
    }
    assert status["category_progress"] == [
        {
            "category": "equity",
            "total_tasks": 2,
            "accepted_tasks": 1,
            "success_tasks": 1,
            "empty_tasks": 0,
            "unavailable_tasks": 0,
            "pending_tasks": 1,
            "running_tasks": 0,
            "failed_tasks": 0,
            "unresolved_tasks": 1,
            "success_rows": 1,
            "completion_percent": 50.0,
        }
    ]
    assert status["provider_progress"] == [
        {
            "provider": "yfinance",
            "accepted_tasks": 1,
            "success_tasks": 1,
            "empty_tasks": 0,
            "rows": 1,
        }
    ]
    assert status["last_accepted_update"] is not None
    assert status["accepted_progress_stalled"] is False
    assert status["retryable_tasks"] == 1
    assert status["file_audit"]["passed"] is True
    assert status["file_audit"]["success_zero_row_tasks"] == 0
    assert status["file_audit"]["success_missing_provider_tasks"] == 0
    assert status["file_audit"]["success_missing_output_path_tasks"] == 0
    assert status["file_audit"]["duplicate_output_paths"] == 0
    assert status["complete"] is False
    assert (
        run(["--output-dir", str(tmp_path), "--fail-on-incomplete", "--no-progress"])
        == 2
    )


def test_full_audit_rejects_parquet_owned_by_non_success_task(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "stale-pending")
    output = Path(task.output_path)
    try:
        manifest.upsert_tasks([task])
        output.parent.mkdir(parents=True)
        pq.write_table(pa.table({"value": [1]}), output)
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["file_audit"]["non_success_parquet_files"] == 1
    assert status["file_audit"]["passed"] is False
    assert status["file_audit"]["issue_samples"][0] == {
        "task_id": "",
        "path": str(output),
        "issue": "parquet_exists_for_non_success_task",
    }


def test_full_audit_snapshot_is_preserved_separately(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "audited")
    output = Path(task.output_path)
    output.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "date": "2000-01-03",
                    "close": 1.0,
                    "_openbb_endpoint": task.endpoint,
                    "_provider": "yfinance",
                    "_scope_key": task.scope_key,
                    "_retrieved_at": "2026-01-01T00:00:00+00:00",
                    "_query_json": '{"symbol":"audited"}',
                }
            ]
        ),
        output,
    )
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "yfinance", 1, str(output), 1))
    finally:
        manifest.close()

    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--audit-files",
                "--write-snapshot",
                "--no-progress",
            ]
        )
        == 0
    )
    audit = json.loads(
        (tmp_path / "_state" / "audit_latest.json").read_text(encoding="utf-8")
    )
    assert audit["file_audit"]["checked_files"] == 1
    assert audit["file_audit"]["passed"] is True
    assert audit["catalog_followup_audit"]["passed"] is True


def test_monitor_snapshot_records_net_manifest_progress(tmp_path: Path) -> None:
    task = _task(tmp_path, "delta")
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task])
    finally:
        manifest.close()

    assert (
        run(["--output-dir", str(tmp_path), "--write-snapshot", "--no-progress"]) == 0
    )
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.complete(TaskResult(task, "empty", "yfinance", 0, None, 1))
    finally:
        manifest.close()
    assert (
        run(["--output-dir", str(tmp_path), "--write-snapshot", "--no-progress"]) == 0
    )

    status = json.loads(
        (tmp_path / "_state" / "monitor_latest.json").read_text(encoding="utf-8")
    )
    provider_eta_path = tmp_path / "catalog" / "provider_eta.parquet"
    category_eta_path = tmp_path / "catalog" / "category_eta.parquet"
    endpoint_eta_path = tmp_path / "catalog" / "provider_endpoint_eta.parquet"
    assert provider_eta_path.is_file()
    assert category_eta_path.is_file()
    assert endpoint_eta_path.is_file()
    assert pq.read_table(provider_eta_path).to_pylist()[0]["provider"] == "yfinance"
    assert pq.read_table(category_eta_path).to_pylist()[0]["category"] == "equity"
    assert pq.read_table(endpoint_eta_path).to_pylist()[0]["endpoint"] == task.endpoint
    delta = status["snapshot_delta"]
    assert delta["elapsed_seconds"] > 0
    assert delta["total_tasks_delta"] == 0
    assert delta["accepted_tasks_delta"] == 1
    assert delta["success_rows_delta"] == 0
    assert delta["retryable_tasks_delta"] == -1
    assert delta["zero_accepted_endpoints_delta"] == -1
    assert delta["by_category"] == [
        {
            "category": "equity",
            "total_tasks_delta": 0,
            "accepted_tasks_delta": 1,
            "rows_delta": 0,
        }
    ]
    assert delta["by_provider"] == [
        {"provider": "yfinance", "accepted_tasks_delta": 1, "rows_delta": 0}
    ]


def test_monitor_field_reads_fresh_atomic_snapshot(tmp_path: Path, capsys) -> None:
    state_dir = tmp_path / "_state"
    state_dir.mkdir(parents=True)
    (state_dir / "monitor_latest.json").write_text(
        json.dumps(
            {
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "retryable_tasks": 123,
            }
        ),
        encoding="utf-8",
    )

    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--field",
                "retryable_tasks",
                "--no-progress",
            ]
        )
        == 0
    )
    assert capsys.readouterr().out.strip() == "123"


def test_monitor_detects_archive_metadata_mismatch(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "metadata")
    try:
        manifest.upsert_tasks([task])
        output = Path(task.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([{"date": "2000-01-03"}]), output)
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "yfinance", 1, str(output), 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, audit_files=True, show_progress=False)
    assert status["file_audit"]["metadata_mismatch_files"] == 1
    assert status["file_audit"]["passed"] is False


def test_monitor_detects_json_encoded_result_wrapper(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "wrapped-result")
    try:
        manifest.upsert_tasks([task])
        output = Path(task.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "result": '[{"date":"2000-01-03","value":1}]',
                        "metadata": '{"SERIES":{"units":"USD"}}',
                        "_openbb_endpoint": task.endpoint,
                        "_provider": "yfinance",
                        "_scope_key": task.scope_key,
                        "_retrieved_at": "2026-01-01T00:00:00+00:00",
                        "_query_json": '{"symbol":"wrapped-result"}',
                    }
                ]
            ),
            output,
        )
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "yfinance", 1, str(output), 1))
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["file_audit"]["encoded_wrapper_files"] == 1
    assert status["file_audit"]["passed"] is False
    assert status["file_audit"]["issue_samples"][0]["issue"] == (
        "encoded_result_wrapper_not_row_normalized"
    )


def test_monitor_detects_duplicate_success_output_paths(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    first = _task(tmp_path, "duplicate-a")
    second = DownloadTask(
        task_id="duplicate-b",
        endpoint=first.endpoint,
        category=first.category,
        scope_key="duplicate-b",
        kwargs={"symbol": "duplicate-b"},
        providers=first.providers,
        output_path=first.output_path,
    )
    try:
        manifest.upsert_tasks([first, second])
        output = Path(first.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "date": "2000-01-03",
                        "_openbb_endpoint": first.endpoint,
                        "_provider": "yfinance",
                        "_scope_key": first.scope_key,
                        "_retrieved_at": "2026-01-01T00:00:00+00:00",
                        "_query_json": '{"symbol":"duplicate-a"}',
                    }
                ]
            ),
            output,
        )
        for task in (first, second):
            manifest.claim([task])
            manifest.complete(
                TaskResult(task, "success", "yfinance", 1, str(output), 1)
            )
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["file_audit"]["duplicate_output_paths"] == 1
    assert status["file_audit"]["passed"] is False
    assert any(
        sample["issue"] == "duplicate_output_path tasks=2"
        for sample in status["file_audit"]["issue_samples"]
    )


def test_monitor_reconciles_catalog_rows_to_followup_tasks(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    parent = DownloadTask(
        task_id="indicator-catalog",
        endpoint="economy.available_indicators",
        category="economy",
        scope_key="all",
        kwargs={"query": ""},
        providers=("econdb",),
        output_path=str(tmp_path / "data" / "indicator-catalog.parquet"),
    )

    def child(symbol: str) -> DownloadTask:
        return DownloadTask(
            task_id=f"indicator-{symbol}",
            endpoint="economy.indicators",
            category="economy",
            scope_key=symbol,
            kwargs={"symbol": f"{symbol}~"},
            providers=("econdb",),
            output_path=str(tmp_path / "data" / f"indicator-{symbol}.parquet"),
        )

    try:
        manifest.upsert_tasks([parent, child("GDP")])
        output = Path(parent.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "symbol": symbol,
                        "_openbb_endpoint": parent.endpoint,
                        "_provider": "econdb",
                        "_scope_key": parent.scope_key,
                        "_retrieved_at": "2026-01-01T00:00:00+00:00",
                        "_query_json": '{"query":""}',
                    }
                    for symbol in ("GDP", "CPI")
                ]
            ),
            output,
        )
        manifest.claim([parent])
        manifest.complete(TaskResult(parent, "success", "econdb", 2, str(output), 1))
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    followups = status["catalog_followup_audit"]
    assert followups["required_unique_followups"] == 2
    assert followups["missing_followups"] == 1
    assert followups["missing_by_endpoint"] == {"economy.indicators": 1}
    assert followups["passed"] is False
    assert "catalog_followup_audit_failed" in {
        alert["code"] for alert in status["alerts"]
    }

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([child("CPI")])
    finally:
        manifest.close()
    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["catalog_followup_audit"]["missing_followups"] == 0
    assert status["catalog_followup_audit"]["passed"] is True


def test_monitor_requires_next_full_major_holders_page(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    kwargs = {
        "symbol": "AAPL",
        "year": 2025,
        "quarter": 4,
        "page": 0,
        "limit": 100,
    }
    parent = DownloadTask(
        task_id="holders-page-0",
        endpoint="equity.ownership.major_holders",
        category="equity",
        scope_key="AAPL/year=2025/quarter=4/page=0",
        kwargs=kwargs,
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "holders-page-0.parquet"),
    )
    child = DownloadTask(
        task_id="holders-page-1",
        endpoint=parent.endpoint,
        category=parent.category,
        scope_key="AAPL/year=2025/quarter=4/page=1",
        kwargs={**kwargs, "page": 1},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "holders-page-1.parquet"),
    )
    try:
        manifest.upsert_tasks([parent])
        output = Path(parent.output_path)
        output.parent.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "holder": f"holder-{index}",
                        "_openbb_endpoint": parent.endpoint,
                        "_provider": "fmp",
                        "_scope_key": parent.scope_key,
                        "_retrieved_at": "2026-01-01T00:00:00+00:00",
                        "_query_json": json.dumps(
                            kwargs, sort_keys=True, separators=(",", ":")
                        ),
                    }
                    for index in range(100)
                ]
            ),
            output,
        )
        manifest.claim([parent])
        manifest.complete(TaskResult(parent, "success", "fmp", 100, str(output), 1))
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    audit = status["catalog_followup_audit"]
    assert audit["required_by_endpoint"] == {"equity.ownership.major_holders": 1}
    assert audit["missing_by_endpoint"] == {"equity.ownership.major_holders": 1}
    assert audit["passed"] is False

    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([child])
    finally:
        manifest.close()
    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["catalog_followup_audit"]["missing_followups"] == 0
    assert status["catalog_followup_audit"]["passed"] is True


def test_monitor_detects_missing_success_file(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "missing")
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(
            TaskResult(task, "success", "yfinance", 1, task.output_path, 1)
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, audit_files=True, show_progress=False)
    assert status["file_audit"]["missing_files"] == 1
    assert status["file_audit"]["passed"] is False
    assert status["complete"] is False


def test_monitor_detects_missing_fred_release_continuation(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    page = _fred_release_task(
        tmp_path,
        "fred-148-page-0",
        "release=148",
        release_id=148,
    )
    continuation = _fred_release_task(
        tmp_path,
        "fred-148-page-1000",
        "release=148/offset=0001000",
        release_id=148,
        offset=1000,
    )
    try:
        manifest.upsert_tasks([page])
        manifest.connection.execute(
            "UPDATE tasks SET status='success',rows=1000 WHERE task_id=?",
            (page.task_id,),
        )
        manifest.connection.commit()
        status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert status["fred_release_pagination_gaps"] == 1
        assert status["fred_release_pagination_gap_samples"] == [
            {
                "scope_key": "release=148",
                "rows": 1000,
                "expected_scope_key": "release=148/offset=0001000",
            }
        ]
        assert status["complete"] is False

        manifest.upsert_tasks([continuation])
        manifest.connection.execute(
            "UPDATE tasks SET status='empty' WHERE task_id=?",
            (continuation.task_id,),
        )
        manifest.connection.commit()
        status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert status["fred_release_pagination_gaps"] == 0
        assert status["complete"] is True
    finally:
        manifest.close()


def test_monitor_detects_missing_fmp_manifest_continuation(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    page = _fmp_page_task(
        tmp_path,
        "fmp-aapl-page-0",
        "AAPL/year=2000/page=0",
        page=0,
    )
    continuation = _fmp_page_task(
        tmp_path,
        "fmp-aapl-page-1",
        "AAPL/year=2000/page=1",
        page=1,
    )
    try:
        manifest.upsert_tasks([page])
        manifest.connection.execute(
            """
            UPDATE tasks
            SET status='success',rows=100,selected_provider='fmp'
            WHERE task_id=?
            """,
            (page.task_id,),
        )
        manifest.connection.commit()
        status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert status["fmp_manifest_pagination_gaps"] == 1
        assert status["pagination_gaps"] == 1
        assert status["fmp_manifest_pagination_gap_samples"] == [
            {
                "endpoint": "news.company",
                "scope_key": "AAPL/year=2000/page=0",
                "rows": 100,
                "limit": 100,
                "expected_scope_key": "AAPL/year=2000/page=1",
            }
        ]
        assert status["complete"] is False

        manifest.upsert_tasks([continuation])
        manifest.connection.execute(
            "UPDATE tasks SET status='empty' WHERE task_id=?",
            (continuation.task_id,),
        )
        manifest.connection.commit()
        status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
        assert status["fmp_manifest_pagination_gaps"] == 0
        assert status["pagination_gaps"] == 0
        assert status["complete"] is True
    finally:
        manifest.close()


def test_monitor_surfaces_systematic_failure_cluster_before_exhaustion(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    tasks = [_task(tmp_path, f"validation-{index}") for index in range(3)]
    error = "provider: validation error: required date missing"
    try:
        manifest.upsert_tasks(tasks)
        for task in tasks:
            manifest.claim([task])
            manifest.complete(
                TaskResult(task, "failed", "provider", 0, None, 1, error=error)
            )
    finally:
        manifest.close()

    status = collect_status(tmp_path, max_total_attempts=20, show_progress=False)
    assert status["systematic_retryable_failure_tasks"] == 3
    assert status["systematic_failure_clusters"] == [
        {
            "endpoint": "equity.price.historical",
            "provider": "provider",
            "tasks": 3,
            "min_attempts": 1,
            "max_attempts": 1,
            "error": error,
        }
    ]
    assert any(
        alert["code"] == "systematic_retryable_failures" for alert in status["alerts"]
    )


def test_monitor_keeps_high_attempt_transient_task_retryable(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "repeated-timeout")
    try:
        manifest.upsert_tasks([task])
        manifest.connection.execute(
            "UPDATE tasks SET status='pending',attempts=20,"
            "error='yfinance: TimeoutError: request timed out' WHERE task_id=?",
            (task.task_id,),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    status = collect_status(tmp_path, max_total_attempts=20, show_progress=False)
    assert status["pending_eligible"] == 1
    assert status["retryable_tasks"] == 1
    assert status["exhausted_tasks"] == 0
    assert status["high_attempt_tasks"] == 1
    assert any(alert["code"] == "high_attempt_tasks" for alert in status["alerts"])


def test_monitor_rejects_terminal_empty_with_retryable_final_provider(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="false-empty",
        endpoint="currency.price.historical",
        category="currency",
        scope_key="AEDJOD",
        kwargs={"symbol": "AEDJOD", "start_date": "2000-01-01"},
        providers=("yfinance", "tiingo"),
        output_path=str(tmp_path / "data" / "false-empty.parquet"),
    )
    error = (
        "yfinance: EmptyDataError: No results | "
        "tiingo: You have run over your hourly request allocation."
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(TaskResult(task, "empty", "tiingo", 0, None, 1, error=error))
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["non_authoritative_empty_tasks"] == 1
    assert status["non_authoritative_empty_clusters"] == [
        {
            "endpoint": "currency.price.historical",
            "provider": "tiingo",
            "tasks": 1,
            "final_error": (
                "tiingo: You have run over your hourly request allocation."
            ),
        }
    ]
    assert any(
        alert["code"] == "non_authoritative_terminal_empty"
        for alert in status["alerts"]
    )
    assert status["complete"] is False


def test_monitor_rejects_unavailable_with_adaptable_limit_evidence(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="false-unavailable",
        endpoint="equity.fundamental.metrics",
        category="equity",
        scope_key="AAPL/period=annual",
        kwargs={"symbol": "AAPL", "limit": 1000},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "false-unavailable.parquet"),
    )
    error = (
        "fmp: Premium Query Parameter: The values for 'limit' must be between 0 and 5"
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(task, "unavailable", "fmp", 0, None, 1, error=error)
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["non_authoritative_unavailable_tasks"] == 1
    assert status["non_authoritative_unavailable_clusters"] == [
        {
            "endpoint": "equity.fundamental.metrics",
            "provider": "fmp",
            "tasks": 1,
            "final_error": error,
        }
    ]
    assert any(
        alert["code"] == "non_authoritative_terminal_unavailable"
        for alert in status["alerts"]
    )
    assert status["complete"] is False


def test_monitor_requires_positive_evidence_for_terminal_unavailable(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="unproven-unavailable",
        endpoint="news.company",
        category="news",
        scope_key="AAPL/year=2020/page=0",
        kwargs={"symbol": "AAPL", "start_date": "2020-01-01"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "unproven-unavailable.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(
                task,
                "unavailable",
                "fmp",
                0,
                None,
                1,
                error="all configured providers unavailable for task capability",
                provider_outcomes={"fmp": "unavailable"},
            )
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["unproven_unavailable_tasks"] == 1
    assert status["unproven_unavailable_clusters"][0]["endpoint"] == "news.company"
    assert any(
        alert["code"] == "unproven_terminal_unavailable" for alert in status["alerts"]
    )

    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "unavailable_providers": {},
                "unavailable_domains": [],
                "unavailable_routes": [
                    {
                        "provider": "fmp",
                        "endpoint": "news.company",
                        "reason": (
                            "HTTP 402 Restricted Endpoint: This endpoint is not "
                            "available under your current subscription"
                        ),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    proven = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert proven["unproven_unavailable_tasks"] == 0
    assert proven["unproven_unavailable_clusters"] == []
    assert proven["unproven_provider_capability_constraints"] == []


def test_monitor_proves_mixed_fallback_from_unavailable_provider_evidence(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="mixed-provider-evidence",
        endpoint="equity.estimates.consensus",
        category="equity",
        scope_key="DYNB",
        kwargs={"symbol": "DYNB"},
        providers=("fmp", "yfinance"),
        output_path=str(tmp_path / "data" / "mixed-provider-evidence.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(
                task,
                "unavailable",
                "yfinance",
                0,
                None,
                1,
                error="yfinance: EmptyDataError: No data was returned",
                provider_outcomes={"fmp": "unavailable", "yfinance": "empty"},
                provider_evidence={
                    "fmp": (
                        "HTTP 402 Restricted Endpoint: not available under "
                        "your current subscription"
                    )
                },
            )
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["unproven_unavailable_tasks"] == 0
    assert status["unproven_unavailable_clusters"] == []


def test_monitor_accepts_explicit_legacy_route_entitlement_evidence(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="legacy-calendar-route",
        endpoint="equity.calendar.events",
        category="equity",
        scope_key="year=2025",
        kwargs={"start_date": "2025-01-01", "end_date": "2025-12-31"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "legacy-calendar-route.parquet"),
    )
    reason = (
        "FMP 403 Legacy Endpoint: only available for legacy users with valid "
        "subscriptions prior August 31, 2025"
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(
                task,
                "unavailable",
                "fmp",
                0,
                None,
                1,
                error=reason,
                provider_outcomes={"fmp": "unavailable"},
                provider_evidence={"fmp": reason},
            )
        )
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "unavailable_routes": [
                    {
                        "provider": "fmp",
                        "endpoint": "equity.calendar.events",
                        "reason": reason,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["unproven_unavailable_tasks"] == 0
    assert status["unproven_provider_capability_constraints"] == []


def test_monitor_rejects_transient_provider_capability_constraint(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="transient-capability",
        endpoint="equity.fundamental.employee_count",
        category="equity",
        scope_key="2330.TW/year=2020",
        kwargs={"symbol": "2330.TW", "start_date": "2020-01-01"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "transient-capability.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(
                task,
                "unavailable",
                "fmp",
                0,
                None,
                1,
                error="all configured providers unavailable for task capability",
                provider_outcomes={"fmp": "unavailable"},
            )
        )
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "unavailable_providers": {},
                "unavailable_routes": [],
                "unavailable_domains": [
                    {
                        "provider": "fmp",
                        "endpoint": "equity.fundamental.employee_count",
                        "domain": "tw",
                        "reason": "TimeoutError: upstream timed out",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["unproven_unavailable_tasks"] == 1
    assert len(status["unproven_provider_capability_constraints"]) == 1
    assert status["unproven_provider_capability_constraints"][0]["domain"] == "tw"


def test_monitor_surfaces_permanent_provider_outcomes(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "schema-drift")
    error = "YFinanceEtfInfoData name Field required"
    try:
        manifest.upsert_tasks([task])
        manifest.complete(
            TaskResult(
                task,
                "pending",
                "yfinance",
                0,
                None,
                1,
                error=error,
                provider_outcomes={"yfinance": "permanent"},
            )
        )
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["schema_version"] == 15
    assert status["zero_accepted_categories"] == [
        {
            "category": "equity",
            "total_tasks": 1,
            "accepted_tasks": 0,
            "success_tasks": 0,
            "empty_tasks": 0,
            "unavailable_tasks": 0,
            "pending_tasks": 1,
            "running_tasks": 0,
            "failed_tasks": 0,
            "unresolved_tasks": 1,
            "success_rows": 0,
            "completion_percent": 0.0,
        }
    ]
    assert any(
        alert["code"] == "categories_without_accepted_data"
        for alert in status["alerts"]
    )
    assert status["pending_attempted"] == 1
    assert status["provider_outcomes"] == {
        "tasks_with_any": 1,
        "tasks_with_empty": 0,
        "tasks_with_unavailable": 0,
        "tasks_with_permanent": 1,
        "permanent_endpoint_count": 1,
        "permanent_by_endpoint": [{"endpoint": "equity.price.historical", "tasks": 1}],
        "permanent_samples": [
            {
                "endpoint": "equity.price.historical",
                "scope_key": "schema-drift",
                "status": "pending",
                "providers": ["yfinance"],
                "selected_provider": "yfinance",
                "attempts": 1,
                "error": error,
                "updated_at": status["last_task_update"],
            }
        ],
    }
    outcome_alert = next(
        item
        for item in status["alerts"]
        if item["code"] == "permanent_provider_outcomes"
    )
    assert outcome_alert["severity"] == "warning"
    assert "1 tasks" in outcome_alert["message"]


def test_monitor_rejects_success_partition_that_saturates_source_cap(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="etf-search-nasdaq",
        endpoint="etf.search",
        category="etf",
        scope_key="exchange=nasdaq",
        kwargs={"exchange": "nasdaq", "country": "all"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "etf-search-nasdaq.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "fmp", 10_000, None, 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["source_cap_saturation_count"] == 1
    assert status["source_cap_saturations"] == [
        {
            "endpoint": "etf.search",
            "scope_key": "exchange=nasdaq",
            "provider": "fmp",
            "rows": 10_000,
            "cap": 10_000,
        }
    ]
    assert status["complete"] is False
    assert any(alert["code"] == "source_cap_saturated" for alert in status["alerts"])


def test_monitor_rejects_bounded_result_at_declared_limit(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="fmp-balance-at-limit",
        endpoint="equity.fundamental.balance",
        category="equity",
        scope_key="AAPL/period=annual",
        kwargs={"symbol": "AAPL", "period": "annual", "limit": 1000},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "fmp-balance-at-limit.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "fmp", 1000, None, 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["declared_limit_saturation_count"] == 1
    assert status["declared_limit_saturations"] == [
        {
            "endpoint": "equity.fundamental.balance",
            "scope_key": "AAPL/period=annual",
            "provider": "fmp",
            "rows": 1000,
            "cap": 1000,
        }
    ]
    assert status["complete"] is False
    assert any(
        alert["code"] == "declared_limit_saturated" for alert in status["alerts"]
    )


def test_monitor_records_proven_nonpageable_entitlement_cap_without_retry_loop(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="fmp-employee-entitlement-cap",
        endpoint="equity.fundamental.employee_count",
        category="equity",
        scope_key="AAPL",
        kwargs={
            "symbol": "AAPL",
            "start_date": "2000-01-01",
            "end_date": "2026-07-18",
            "limit": 5,
        },
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "fmp-employee-cap.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(TaskResult(task, "success", "fmp", 5, None, 1))
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "parameter_maximums": [
                    {
                        "provider": "fmp",
                        "endpoint": "equity.fundamental.employee_count",
                        "parameter": "limit",
                        "maximum": 5,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["source_cap_saturation_count"] == 0
    assert status["declared_limit_saturation_count"] == 0
    assert status["entitlement_limit_saturation_count"] == 1
    assert status["entitlement_limit_saturations"] == [
        {
            "endpoint": "equity.fundamental.employee_count",
            "scope_key": "AAPL",
            "provider": "fmp",
            "rows": 5,
            "cap": 5,
            "constraint": "nonpageable_current_entitlement_limit",
        }
    ]
    assert status["complete"] is True
    assert any(
        alert["code"] == "entitlement_history_capped" for alert in status["alerts"]
    )


def test_monitor_reports_endpoint_level_zero_accepted_progress(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    pending = _task(tmp_path, "pending-price")
    success = DownloadTask(
        task_id="economy-success",
        endpoint="economy.pce",
        category="economy",
        scope_key="all",
        kwargs={},
        providers=("fred",),
        output_path=str(tmp_path / "data" / "economy-success.parquet"),
    )
    try:
        manifest.upsert_tasks([pending, success])
        manifest.claim([success])
        manifest.complete(TaskResult(success, "success", "fred", 1, None, 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, show_progress=False)
    assert status["endpoint_progress_summary"] == {
        "endpoint_count": 2,
        "started_endpoint_count": 1,
        "zero_accepted_endpoint_count": 1,
        "resolved_endpoint_count": 1,
        "unresolved_endpoint_count": 1,
        "zero_quota_blocked_endpoint_count": 0,
        "zero_capacity_blocked_endpoint_count": 0,
        "zero_inflight_endpoint_count": 0,
        "zero_without_recorded_blocker_count": 1,
        "high_empty_endpoint_count": 0,
        "all_empty_endpoint_count": 0,
    }
    assert status["zero_accepted_endpoints"] == [
        {
            "endpoint": "equity.price.historical",
            "total_tasks": 1,
            "accepted_tasks": 0,
            "success_tasks": 0,
            "empty_tasks": 0,
            "empty_ratio": None,
            "unavailable_tasks": 0,
            "unresolved_tasks": 1,
            "running_tasks": 0,
            "failed_tasks": 0,
            "last_accepted_update": None,
            "provider_capacity_deferred": 0,
            "fmp_only_pending": 0,
            "fmp_quota_deferred": 0,
            "bls_quota_deferred": 0,
            "sec_rate_limit_deferred": 0,
            "tiingo_news_permission_pending": 0,
            "runtime_provider_cooldown_deferred": 0,
        }
    ]
    assert any(
        alert["code"] == "endpoints_without_accepted_data" for alert in status["alerts"]
    )


def test_monitor_warns_when_endpoint_has_only_authoritative_empty_results(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    tasks = [_task(tmp_path, f"empty-{index}") for index in range(10)]
    try:
        manifest.upsert_tasks(tasks)
        manifest.claim(tasks)
        for task in tasks:
            manifest.complete(TaskResult(task, "empty", "yfinance", 0, None, 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, show_progress=False)
    summary = status["endpoint_progress_summary"]
    assert summary["high_empty_endpoint_count"] == 1
    assert summary["all_empty_endpoint_count"] == 1
    assert status["high_empty_endpoints"][0]["endpoint"] == ("equity.price.historical")
    assert status["high_empty_endpoints"][0]["empty_ratio"] == 1.0
    assert any(alert["code"] == "endpoints_all_empty" for alert in status["alerts"])
    assert status["health"] == "critical"
    assert status["complete"] is False


def test_monitor_reports_subscription_unavailable_as_resolved_but_not_downloaded(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = _task(tmp_path, "premium-route")
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(
            TaskResult(
                task,
                "unavailable",
                "fmp",
                0,
                None,
                1,
                error="fmp: not available under current subscription",
            )
        )
    finally:
        manifest.close()

    status = collect_status(
        tmp_path, audit_files=True, min_free_gib=0, show_progress=False
    )
    assert status["accepted_tasks"] == 0
    assert status["unavailable_tasks"] == 1
    assert status["resolved_tasks"] == 1
    assert status["unresolved_tasks"] == 1
    assert status["actionable_unresolved_tasks"] == 0
    assert status["health"] == "ok"
    assert status["alerts"] == []
    assert status["complete"] is True
    assert (
        run(
            [
                "--output-dir",
                str(tmp_path),
                "--fail-on-incomplete",
                "--no-progress",
                "--min-free-gib",
                "0",
            ]
        )
        == 0
    )


def test_monitor_distinguishes_cooldown_churn_from_accepted_progress(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    success = _task(tmp_path, "old-success")
    cooldown = _task(tmp_path, "cooldown")
    try:
        manifest.upsert_tasks([success, cooldown])
        manifest.connection.execute(
            "UPDATE tasks SET status='success',rows=1,updated_at=? WHERE task_id=?",
            ("2000-01-01T00:00:00+00:00", success.task_id),
        )
        manifest.connection.execute(
            "UPDATE tasks SET status='pending',attempts=0,error=? WHERE task_id=?",
            (
                "fmp: skipped (cooldown until 2099-01-01T00:00:00+00:00)",
                cooldown.task_id,
            ),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    status = collect_status(
        tmp_path,
        accepted_stall_minutes=1,
        min_free_gib=0,
        show_progress=False,
    )
    assert status["pending_with_error"] == 1
    assert status["pending_cooldown"] == 1
    assert status["pending_rate_limited"] == 1
    assert status["pending_attempted"] == 0
    assert status["accepted_progress_stalled"] is True
    assert "accepted_progress_stalled" in {alert["code"] for alert in status["alerts"]}


def test_monitor_reports_provider_quota_and_permission_constraints(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    fmp_only = DownloadTask(
        task_id="fmp-only",
        endpoint="equity.estimates.forward_eps",
        category="equity",
        scope_key="AAPL/period=annual",
        kwargs={"symbol": "AAPL", "period": "annual"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "fmp-only.parquet"),
    )
    bls = DownloadTask(
        task_id="bls-quota",
        endpoint="economy.survey.bls_series",
        category="economy",
        scope_key="batch=1",
        kwargs={"series_ids": "A"},
        providers=("bls",),
        output_path=str(tmp_path / "data" / "bls-quota.parquet"),
    )
    news = DownloadTask(
        task_id="news-permission",
        endpoint="news.company",
        category="news",
        scope_key="AAPL/year=2025/page=0",
        kwargs={"symbol": "AAPL"},
        providers=("fmp", "tiingo"),
        output_path=str(tmp_path / "data" / "news-permission.parquet"),
    )
    sec = DownloadTask(
        task_id="sec-rate-limit",
        endpoint="regulators.sec.filing_headers",
        category="regulators",
        scope_key="accession=1",
        kwargs={"url": "https://www.sec.gov/Archives/example"},
        providers=("sec",),
        output_path=str(tmp_path / "data" / "sec-rate-limit.parquet"),
    )
    try:
        manifest.upsert_tasks([fmp_only, bls, news, sec])
        manifest.connection.execute(
            "UPDATE tasks SET error=? WHERE task_id=?",
            (
                "fmp: UnauthorizedError: 429 Limit Reach",
                fmp_only.task_id,
            ),
        )
        manifest.connection.execute(
            "UPDATE tasks SET error=? WHERE task_id=?",
            (
                "bls: daily threshold reached; quota resets tomorrow",
                bls.task_id,
            ),
        )
        manifest.connection.execute(
            "UPDATE tasks SET error=? WHERE task_id=?",
            (
                "fmp: skipped (cooldown until tomorrow) | tiingo: You do not "
                "have permission to access the News API",
                news.task_id,
            ),
        )
        manifest.connection.execute(
            "UPDATE tasks SET error=? WHERE task_id=?",
            (
                "sec: OpenBBError: 429 Too Many Requests; cooldown until tomorrow",
                sec.task_id,
            ),
        )
        manifest.connection.commit()
    finally:
        manifest.close()

    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "rate_limits_rps": {"fmp": 8.0, "bls": 5.0},
                "concurrency": {"fmp": 8, "bls": 5},
                "rate_activity": {
                    "providers": {
                        "fmp": {
                            "declared_daily_request_cap": 250,
                            "declared_daily_requests_remaining": 250,
                            "endpoint_request_costs": {
                                "equity.estimates.forward_eps": {
                                    "requests": 10,
                                    "claiming_attempts": 5,
                                    "max_requests_per_attempt": 2,
                                    "average_requests_per_claiming_attempt": 2.0,
                                }
                            },
                        },
                        "bls": {
                            "declared_daily_request_cap": 500,
                            "declared_daily_requests_remaining": 500,
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert status["provider_constraints"] == {
        "fmp_only_pending": 1,
        "provider_capacity_deferred": 0,
        "fmp_quota_deferred": 2,
        "bls_quota_deferred": 1,
        "sec_rate_limit_deferred": 1,
        "tiingo_news_permission_pending": 1,
    }
    assert status["provider_quota_feasibility"] == {
        "bls": [
            {
                "window": "provider_day",
                "exclusive_backlog_tasks": 1,
                "estimated_exclusive_http_requests": 1,
                "request_cap": 500,
                "requests_remaining_in_current_window": 500,
                "minimum_quota_windows": 1,
                "additional_reset_boundaries_required": 0,
                "lower_bound_only": True,
            }
        ],
        "fmp": [
            {
                "window": "provider_day",
                "exclusive_backlog_tasks": 1,
                "estimated_exclusive_http_requests": 2,
                "request_cap": 250,
                "requests_remaining_in_current_window": 250,
                "minimum_quota_windows": 1,
                "additional_reset_boundaries_required": 0,
                "lower_bound_only": False,
            }
        ],
    }
    assert status["provider_progress_stalls"] == [
        # FMP is unresolved for both its provider-only task and the
        # FMP/Tiingo fallback-chain task; generic stall accounting must
        # include both instead of substituting the provider-only counter.
        {"provider": "fmp", "backlog_tasks": 2, "recent_accepted_tasks": 0},
        {"provider": "bls", "backlog_tasks": 1, "recent_accepted_tasks": 0},
        {"provider": "sec", "backlog_tasks": 1, "recent_accepted_tasks": 0},
    ]
    assert status["exclusive_provider_backlogs"] == {
        "bls": 1,
        "fmp": 1,
        "sec": 1,
        "tiingo": 0,
    }
    fmp_eta = next(
        row for row in status["provider_eta_projections"] if row["provider"] == "fmp"
    )
    assert fmp_eta["eligible_backlog_tasks"] == 2
    assert fmp_eta["exclusive_backlog_tasks"] == 1
    assert fmp_eta["estimated_exclusive_http_requests"] == 2
    assert fmp_eta["unobserved_exclusive_request_cost_tasks"] == 0
    assert fmp_eta["state"] == "stalled"
    assert fmp_eta["daily_quota_lower_bound"]["minimum_days_at_call_cap"] == 1
    assert any(item["code"] == "provider_progress_stalled" for item in status["alerts"])
    quota_alert = next(
        item for item in status["alerts"] if item["code"] == "provider_quota_deferred"
    )
    assert quota_alert["severity"] == "warning"
    assert "fmp=2" in quota_alert["message"]
    assert "bls=1" in quota_alert["message"]
    assert "sec=1" in quota_alert["message"]


def test_monitor_classifies_pristine_fmp_backlog_from_runtime_cooldown(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    task = DownloadTask(
        task_id="fmp-pristine",
        endpoint="equity.fundamental.metrics",
        category="equity",
        scope_key="AAPL/period=quarter",
        kwargs={"symbol": "AAPL", "period": "quarter"},
        providers=("fmp",),
        output_path=str(tmp_path / "data" / "fmp-pristine.parquet"),
    )
    try:
        manifest.upsert_tasks([task])
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "providers": {
                    "fmp": {
                        "blocked_until": time.time() + 120,
                        "kind": "quota",
                        "reason": "daily limit reached",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["endpoint_progress_summary"]["zero_quota_blocked_endpoint_count"] == 1
    assert (
        status["endpoint_progress_summary"]["zero_without_recorded_blocker_count"] == 0
    )
    assert (
        status["zero_accepted_endpoints"][0]["runtime_provider_cooldown_deferred"] == 1
    )


def test_monitor_classifies_any_pristine_provider_cooldown_but_not_live_fallback(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    bls_only = DownloadTask(
        task_id="bls-pristine",
        endpoint="economy.survey.bls_search",
        category="economy",
        scope_key="labor-force",
        kwargs={"query": "labor force"},
        providers=("bls",),
        output_path=str(tmp_path / "data" / "bls-pristine.parquet"),
    )
    fallback = DownloadTask(
        task_id="bls-with-fred-fallback",
        endpoint="economy.fred_series",
        category="economy",
        scope_key="fallback",
        kwargs={"symbol": "fallback"},
        providers=("bls", "fred"),
        output_path=str(tmp_path / "data" / "bls-with-fred-fallback.parquet"),
    )
    try:
        manifest.upsert_tasks([bls_only, fallback])
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "providers": {
                    "bls": {
                        "blocked_until": time.time() + 120,
                        "kind": "quota",
                        "reason": "daily threshold reached",
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    by_endpoint = {row["endpoint"]: row for row in status["zero_accepted_endpoints"]}

    assert (
        by_endpoint["economy.survey.bls_search"]["runtime_provider_cooldown_deferred"]
        == 1
    )
    assert by_endpoint["economy.fred_series"]["runtime_provider_cooldown_deferred"] == 0
    summary = status["endpoint_progress_summary"]
    assert summary["zero_quota_blocked_endpoint_count"] == 1
    assert summary["zero_without_recorded_blocker_count"] == 1


def test_monitor_reports_all_active_provider_cooldowns(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([_task(tmp_path, "pending")])
    finally:
        manifest.close()
    deadline = time.time() + 120
    (tmp_path / "_state" / "provider_cooldowns.json").write_text(
        json.dumps(
            {
                "schema_version": 3,
                "rate_limits_rps": {"fmp": 8.0, "sec": 10.0},
                "concurrency": {"fmp": 8, "sec": 1},
                "rate_activity": {
                    "providers": {
                        "fmp": {
                            "active_calls": 7,
                            "ticket_waiters": 2,
                            "effective_concurrency": 12,
                            "adaptive_concurrency_cap": 64,
                            "concurrency_expansions": 3,
                            "limiter_claims_total": 24,
                            "limiter_observed_claims_total": 19,
                            "pending_claim_observations": 5,
                            "limiter_claims_last_60s": 12,
                            "observed_claims_per_second": 0.2,
                            "utilization_percent": 2.5,
                        }
                    }
                },
                "providers": {
                    "fmp": {
                        "blocked_until": deadline,
                        "kind": "quota",
                        "reason": "daily limit reached",
                    },
                    "tiingo": deadline + 10,
                    "expired": time.time() - 1,
                },
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)
    assert set(status["active_provider_cooldowns"]) == {"fmp", "tiingo"}
    assert status["active_provider_cooldowns"]["fmp"]["remaining_seconds"] > 0
    assert status["active_provider_cooldowns"]["fmp"]["kind"] == "quota"
    assert status["active_provider_cooldowns"]["fmp"]["reason"] == "daily limit reached"
    assert status["active_provider_cooldowns"]["tiingo"]["kind"] == "legacy"
    assert status["active_provider_cooldowns"]["tiingo"]["until"].endswith("+00:00")
    limits = status["provider_runtime_limits"]
    assert set(limits) == {"fmp", "sec"}
    assert limits["fmp"]["requests_per_second"] == 8.0
    assert limits["fmp"]["effective_concurrency"] == 12
    assert limits["fmp"]["limiter_claims_total"] == 24
    assert limits["fmp"]["limiter_observed_claims_total"] == 19
    assert limits["fmp"]["pending_claim_observations"] == 5
    assert limits["fmp"]["limiter_claims_current_provider_day"] == 0
    assert limits["fmp"]["declared_daily_request_cap"] is None
    assert limits["fmp"]["observed_quota_limit"] is None
    assert limits["sec"]["requests_per_second"] == 10.0
    assert limits["sec"]["effective_concurrency"] == 1


def test_monitor_reports_independent_provider_scheduler_pools(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([_task(tmp_path, "pending")])
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_scheduler.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "phase": "running",
                "pid": os.getpid(),
                "plan_token": "active",
                "wave": 7,
                "attempted_this_run": 12,
                "global_worker_limit": 96,
                "global_queue_limit": 384,
                "active_total": 3,
                "completed_pending_total": 2,
                "buffered_total": 5,
                "completion_persistence_batch_size": 256,
                "completion_backpressure_limit": 512,
                "completion_backpressure_active": True,
                "providers": {
                    "yfinance": {
                        "requests_per_second": 4.0,
                        "execution_limit": 24,
                        "queue_limit": 96,
                        "active": 3,
                        "buffered": 5,
                        "reservations": 8,
                        "refill_threshold": 48,
                        "seed_route_count": 9,
                        "cooldown": False,
                        "unavailable": False,
                    }
                },
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert status["provider_scheduler"]["phase"] == "running"
    assert status["provider_scheduler"]["completed_pending_total"] == 2
    assert status["provider_scheduler"]["completion_persistence_batch_size"] == 256
    assert status["provider_scheduler"]["completion_backpressure_limit"] == 512
    assert status["provider_scheduler"]["completion_backpressure_active"] is True
    assert status["scheduler_invariant_violations"] == []
    assert status["provider_scheduler"]["providers"]["yfinance"] == {
        "requests_per_second": 4.0,
        "execution_limit": 24,
        "queue_limit": 96,
        "active": 3,
        "buffered": 5,
        "reservations": 8,
        "refill_threshold": 48,
        "seed_route_count": 9,
        "cooldown": False,
        "unavailable": False,
    }


def test_monitor_rejects_shared_scheduler_queue_invariant_violations(
    tmp_path: Path,
) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([_task(tmp_path, "pending")])
    finally:
        manifest.close()
    (tmp_path / "_state" / "provider_scheduler.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "phase": "running",
                "pid": os.getpid(),
                "global_worker_limit": 24,
                "global_queue_limit": 96,
                "active_total": 97,
                "completed_pending_total": 0,
                "buffered_total": 0,
                "providers": {
                    "sec": {
                        "execution_limit": 24,
                        "queue_limit": 96,
                        "active": 97,
                        "reservations": 97,
                        "refill_threshold": 97,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    assert {
        violation["invariant"] for violation in status["scheduler_invariant_violations"]
    } == {
        "refill_threshold_lte_queue_limit",
        "live_reservations_lte_queue_limit",
        "live_handoff_lte_global_queue_limit",
    }
    assert "provider_scheduler_invariant_failed" in {
        alert["code"] for alert in status["alerts"]
    }


def test_monitor_never_marks_a_critical_snapshot_complete(tmp_path: Path) -> None:
    task = _task(tmp_path, "complete-but-low-disk")
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([task])
        manifest.claim([task])
        manifest.complete(TaskResult(task, "empty", "yfinance", 0, None, 1))
    finally:
        manifest.close()

    status = collect_status(tmp_path, min_free_gib=10**9, show_progress=False)

    assert "low_disk" in {alert["code"] for alert in status["alerts"]}
    assert status["health"] == "critical"
    assert status["complete"] is False


def test_monitor_reports_request_level_resume_checkpoints(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    try:
        manifest.upsert_tasks([_task(tmp_path, "checkpoint-monitor")])
    finally:
        manifest.close()
    task_dir = tmp_path / "_state" / "request_checkpoints" / "congress_gov" / "task-id"
    task_dir.mkdir(parents=True)
    (task_dir / "page.json").write_text('{"payload":{}}', encoding="utf-8")
    (task_dir / "page.json.corrupt.1").write_text("broken", encoding="utf-8")

    status = collect_status(tmp_path, min_free_gib=0, show_progress=False)

    summary = status["request_checkpoints"]
    assert summary["task_count"] == 1
    assert summary["file_count"] == 2
    assert summary["bytes"] > 0
    assert summary["corrupt_file_count"] == 1
    assert summary["oldest_checkpoint_at"]
    assert summary["newest_checkpoint_at"]
    assert summary["by_provider"] == {
        "congress_gov": {
            "task_count": 1,
            "file_count": 2,
            "bytes": summary["bytes"],
        }
    }
    assert any(
        item["code"] == "request_checkpoint_corrupt" for item in status["alerts"]
    )


def test_monitor_uses_active_plan_and_ignores_obsolete_tasks(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    current = _task(tmp_path, "current")
    obsolete = _task(tmp_path, "obsolete")
    try:
        manifest.upsert_tasks(
            [current], plan_token="archive", plan_generation="current"
        )
        manifest.upsert_tasks([obsolete], plan_token="archive", plan_generation="old")
        manifest.reconcile_initial_plan("archive", "current")
    finally:
        manifest.close()

    status = collect_status(tmp_path, show_progress=False)
    assert status["active_plan_token"] == "archive"
    assert status["total_tasks"] == 1
    assert status["inactive_tasks"] == 1


def test_monitor_rejects_active_tasks_from_another_plan(tmp_path: Path) -> None:
    manifest = Manifest(tmp_path / "_state" / "openbb_archive.sqlite3")
    current = _task(tmp_path, "current-plan-complete")
    orphan = _task(tmp_path, "orphan-other-plan")
    try:
        manifest.upsert_tasks([current], plan_token="current", plan_generation="now")
        manifest.reconcile_initial_plan("current", "now")
        manifest.connection.execute(
            "UPDATE tasks SET status='empty' WHERE task_id=?", (current.task_id,)
        )
        manifest.upsert_tasks([orphan], plan_token="orphan", plan_generation="old")
        manifest.connection.commit()
    finally:
        manifest.close()

    status = collect_status(tmp_path, show_progress=False)
    assert status["active_plan_token"] == "current"
    assert status["total_tasks"] == 1
    assert status["active_other_plan_tasks"] == 1
    assert status["complete"] is False
