from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess

import pytest

from stockagent.live.shioaji_api_dashboard import (
    CAPTURE_UNIT,
    HISTORY_UNIT,
    TOP200_UNIT,
    ShioajiMonitorPaths,
    _backfill_status,
    _capture_status,
    _latest_capture_mtime,
    build_shioaji_public_status,
)


def _keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(map(str, value)) | {
            key for item in value.values() for key in _keys(item)
        }
    if isinstance(value, list):
        return {key for item in value for key in _keys(item)}
    return set()


def _journal_line(message: str, timestamp: datetime, invocation: str) -> str:
    return json.dumps(
        {
            "MESSAGE": message,
            "__REALTIME_TIMESTAMP": str(int(timestamp.timestamp() * 1_000_000)),
            "_SYSTEMD_INVOCATION_ID": invocation,
        }
    )


def test_backfill_new_progress_supersedes_old_wait_without_invocation_id(
    tmp_path: Path,
) -> None:
    inventory = tmp_path / "contracts.csv"
    inventory.write_text("contract,priority\nMXFR1,1\n", encoding="utf-8")
    target = tmp_path / "target.txt"
    target.write_text("2026-08-10\n", encoding="utf-8")
    paths = ShioajiMonitorPaths(
        alias_inventory=inventory,
        txfr1_manifest=tmp_path / "missing-txfr1.json",
        futures_history_root=tmp_path / "history",
        target_end_date=target,
        capture_root=tmp_path / "capture",
    )
    entries = [
        {
            "MESSAGE": "[shioaji-futures-history-runner] waiting_seconds=256925 "
            "reason=next_quota_window contract=MXFR1"
        },
        {
            "MESSAGE": "[shioaji-futures-history-runner] "
            "runner_started=2026-08-15T10:00:42+08:00"
        },
        {
            "MESSAGE": "[shioaji-futures-history-runner] "
            "contract_start=MXFR1 output=history/MXFR1"
        },
        {
            "MESSAGE": "[shioaji-futures-history] 1/1105 contract=MXFR1 "
            "date=2024-09-30 rows=214490 traffic=2033611/2147483648"
        },
    ]
    status = _backfill_status(
        paths,
        [
            {
                "status": "partial",
                "contract": "MXFR1",
                "resolved_trading_dates": 449,
                "expected_trading_dates": 1554,
                "rows": 70_747_492,
                "bytes": 1_519_979_221,
            }
        ],
        entries,
        {"active": True, "state": "running", "invocation_id": None},
    )

    assert status["state"] == "downloading"
    assert status["waiting_reason"] is None
    assert status["waiting_seconds_at_observation"] is None


def test_latest_capture_mtime_uses_write_time_not_hour_name(tmp_path: Path) -> None:
    root = tmp_path / "capture"
    older_night = root / "ticks" / "trade_date=2026-08-19" / "hour=23"
    newer_day = root / "ticks" / "trade_date=2026-08-19" / "hour=13"
    older_night.mkdir(parents=True)
    newer_day.mkdir(parents=True)
    os.utime(older_night, (100.0, 100.0))
    os.utime(newer_day, (200.0, 200.0))

    assert _latest_capture_mtime(root) == 200.0


def test_capture_waits_between_sessions_instead_of_reporting_stale(
    tmp_path: Path,
) -> None:
    observed = datetime(2026, 8, 19, 6, 15, tzinfo=UTC)
    capture_root = tmp_path / "capture"
    latest_hour = capture_root / "ticks" / "trade_date=2026-08-19" / "hour=13"
    latest_hour.mkdir(parents=True)
    os.utime(
        latest_hour,
        (observed.timestamp() - 30 * 60, observed.timestamp() - 30 * 60),
    )
    missing = tmp_path / "missing"
    paths = ShioajiMonitorPaths(
        alias_inventory=missing,
        txfr1_manifest=missing,
        futures_history_root=missing,
        target_end_date=missing,
        capture_root=capture_root,
    )

    status = _capture_status(
        paths,
        [],
        {"active": True, "state": "running", "restarts": 0},
        now=observed,
    )

    assert status["state"] == "waiting"


def test_shioaji_public_status_reconciles_quota_progress_and_capture(
    tmp_path: Path,
) -> None:
    observed = datetime(2026, 8, 13, 8, 0, tzinfo=UTC)
    inventory = tmp_path / "contracts.csv"
    inventory.write_text(
        "contract,priority\nTXFR1,1\nTXFR2,2\nMXFR1,3\n", encoding="utf-8"
    )
    tx_manifest = tmp_path / "txfr1.json"
    tx_manifest.write_text(
        json.dumps(
            {
                "status": "complete",
                "contract": "TXFR1",
                "resolved_trading_dates": 20,
                "expected_trading_dates": 20,
                "rows": 1_000,
                "bytes": 2_000,
                "traffic_used_bytes": 1_600_000_000,
                "traffic_limit_bytes": 2_000_000_000,
            }
        ),
        encoding="utf-8",
    )
    futures_root = tmp_path / "history"
    mx_root = futures_root / "MXFR1"
    mx_root.mkdir(parents=True)
    (mx_root / "manifest.json").write_text(
        json.dumps(
            {
                "status": "partial",
                "contract": "MXFR1",
                "resolved_trading_dates": 5,
                "expected_trading_dates": 20,
                "rows": 500,
                "bytes": 1_000,
                "traffic_used_bytes": 1_790_000_000,
                "traffic_limit_bytes": 2_000_000_000,
                "stopped_for_traffic": True,
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "target.txt"
    target.write_text("2026-08-12\n", encoding="utf-8")
    capture_root = tmp_path / "capture"
    latest_hour = capture_root / "ticks" / "trade_date=2026-08-14" / "hour=15"
    latest_hour.mkdir(parents=True)
    os.utime(latest_hour, (observed.timestamp() - 5, observed.timestamp() - 5))
    os.utime(tx_manifest, (observed.timestamp() - 120, observed.timestamp() - 120))
    os.utime(
        mx_root / "manifest.json",
        (observed.timestamp() - 10, observed.timestamp() - 10),
    )

    history_journal = "\n".join(
        [
            _journal_line(
                "[shioaji-futures-history] 4/15 contract=MXFR1 "
                "traffic=1,700,000,000/2,000,000,000",
                observed - timedelta(seconds=60),
                "history-invocation",
            ),
            _journal_line(
                "[shioaji-futures-history] 5/15 contract=MXFR1 "
                "traffic=1,790,000,000/2,000,000,000",
                observed,
                "history-invocation",
            ),
            _journal_line(
                "[shioaji-futures-history-runner] waiting_seconds=600 "
                "reason=next_quota_window contract=MXFR1",
                observed,
                "history-invocation",
            ),
        ]
    )
    capture_journal = "\n".join(
        [
            _journal_line(
                "[shioaji-taifex] capture_start=2026-08-13T06:55:00+08:00 "
                "capture_id=private session=night trade_date=2026-08-14 "
                "stop_at=2026-08-14T05:00:05+08:00",
                observed,
                "capture-invocation",
            ),
            _journal_line(
                "[shioaji-taifex] worker=0/2 contracts=100 subscriptions=200",
                observed,
                "capture-invocation",
            ),
            _journal_line(
                "[shioaji-taifex] worker=1/2 contracts=100 subscriptions=200",
                observed,
                "capture-invocation",
            ),
        ]
    )

    observed_commands: list[list[str]] = []

    def runner(args: list[str] | tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        command = list(args)
        observed_commands.append(command)
        unit = (
            command[command.index("--unit") + 1] if "--unit" in command else command[2]
        )
        if command[0] == "systemctl":
            invocation = (
                "history-invocation" if unit == HISTORY_UNIT else "capture-invocation"
            )
            stdout = (
                "ActiveState=active\nSubState=running\nNRestarts=0\n"
                f"InvocationID={invocation}\n"
            )
        else:
            stdout = history_journal if unit == HISTORY_UNIT else capture_journal
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    payload = build_shioaji_public_status(
        tmp_path,
        now=observed,
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=inventory,
            txfr1_manifest=tx_manifest,
            futures_history_root=futures_root,
            target_end_date=target,
            capture_root=capture_root,
        ),
    )

    assert payload["health"] == "waiting"
    assert payload["read_only"] is True
    assert payload["production_order_possible"] is False
    assert payload["traffic"]["remaining_bytes"] == 210_000_000
    assert payload["traffic"]["safe_remaining_bytes"] == 10_000_000
    assert payload["backfill"]["progress_ratio"] == pytest.approx(25 / 60)
    assert payload["backfill"]["completed_contracts"] == 1
    assert payload["backfill"]["current_contract"] == "MXFR1"
    assert payload["capture"]["state"] == "capturing"
    assert payload["capture"]["workers"] == 2
    assert payload["capture"]["subscriptions"] == 400
    assert payload["dashboard_schema_version"] == 5
    journal_commands = [
        command for command in observed_commands if command[0] == "journalctl"
    ]
    assert journal_commands
    assert all(
        "--output-fields=MESSAGE,__REALTIME_TIMESTAMP,_SYSTEMD_INVOCATION_ID" in command
        for command in journal_commands
    )
    assert {item["id"] for item in payload["pipelines"]} == {
        "contract_catalog",
        "fop_stream",
        "futures_history",
        "hft_dataset",
        "minute_research",
        "on_demand_snapshots",
        "stock_daily",
        "stock_minute",
        "top200_stream",
    }
    assert payload["pipeline_summary"]["total"] == 9
    assert all(
        item["quota"] in {"historical", "realtime", "none"}
        for item in payload["pipelines"]
    )
    assert all(isinstance(item["fields"], list) for item in payload["pipelines"])
    assert all(isinstance(item["eta"], dict) for item in payload["pipelines"])
    history_eta = payload["backfill"]["eta"]
    assert history_eta["state"] == "waiting_quota"
    assert history_eta["remaining_seconds"] >= 2 * 86400
    assert history_eta["quota_windows_remaining"] == 2
    assert history_eta["assumption"] == "one_equivalent_quota_window_per_24h_scenario"
    assert history_eta["confidence"] == "low"
    assert len(payload["traffic_breakdown"]) == 9
    assert "實際費用依永豐最新帳戶契約" in payload["traffic"]["pricing_policy"]
    assert "08:00 僅為預期政策" in payload["traffic"]["reset_policy"]
    assert payload["storage"]["status"] == "collecting"
    forbidden = {
        "api_key",
        "secret",
        "account_id",
        "capture_id",
        "invocation_id",
        "path",
    }
    assert not (_keys(payload) & forbidden)
    assert str(tmp_path) not in json.dumps(payload)


def test_shioaji_dashboard_labels_only_observed_counter_drop_as_reset(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "traffic_summary.json"
    ledger.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "ledger_date": "2026-08-15",
                "observation_date": "2026-08-15",
                "quota_epoch": {
                    "id": "observed_counter_drop:2026-08-15T01:00:00Z",
                    "started_at_utc": "2026-08-15T01:00:00Z",
                    "boundary_kind": "observed_counter_drop",
                    "reset_observed": True,
                },
                "latest_reset": {
                    "kind": "observed_counter_drop",
                    "observed_at_utc": "2026-08-15T01:00:00Z",
                    "previous_used_bytes": 1_900_000_000,
                    "new_used_bytes": 0,
                    "previous_limit_bytes": 2_000_000_000,
                    "new_limit_bytes": 2_000_000_000,
                    "consumer": "strategy",
                    "method": "snapshots",
                },
                "latest_usage": {
                    "observed_at_utc": "2026-08-15T01:00:30Z",
                    "used_bytes": 25,
                    "limit_bytes": 2_000_000_000,
                    "consumer": "strategy",
                    "method": "snapshots",
                },
                "totals": {},
                "by_consumer": {},
                "by_method": {},
                "by_asset_class": {},
            }
        ),
        encoding="utf-8",
    )

    def runner(args):
        command = list(args)
        stdout = (
            "ActiveState=inactive\nSubState=dead\nNRestarts=0\nInvocationID=\n"
            if command[0] == "systemctl"
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    missing = tmp_path / "missing"
    payload = build_shioaji_public_status(
        tmp_path,
        now=datetime(2026, 8, 15, 1, 1, tzinfo=UTC),
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=missing,
            txfr1_manifest=missing,
            futures_history_root=missing,
            target_end_date=missing,
            capture_root=missing,
            traffic_ledger_summary=ledger,
        ),
    )

    assert payload["traffic"]["used_bytes"] == 25
    assert payload["traffic"]["reset_observed_at_utc"] == "2026-08-15T01:00:00Z"
    assert "計數器下降才認定重置" in payload["traffic"]["reset_policy"]
    assert payload["traffic_ledger"]["quota_epoch"]["reset_observed"] is True


def test_inactive_timer_driven_history_job_is_scheduled_not_failed(
    tmp_path: Path,
) -> None:
    inventory = tmp_path / "contracts.csv"
    inventory.write_text("contract,priority\nTXFR1,0\n", encoding="utf-8")
    target = tmp_path / "target.txt"
    target.write_text("2026-08-21\n", encoding="utf-8")

    def runner(args):
        command = list(args)
        stdout = (
            "ActiveState=inactive\nSubState=dead\nNRestarts=0\nInvocationID=\n"
            if command[0] == "systemctl"
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    missing = tmp_path / "missing"
    payload = build_shioaji_public_status(
        tmp_path,
        now=datetime(2026, 8, 23, 2, 0, tzinfo=UTC),
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=inventory,
            txfr1_manifest=missing,
            futures_history_root=missing,
            target_end_date=target,
            capture_root=missing,
        ),
    )

    history = next(
        item for item in payload["pipelines"] if item["id"] == "futures_history"
    )
    assert payload["backfill"]["state"] == "scheduled"
    assert history["status"] == "waiting"
    assert history["status_label"] == "等待排程／上次成功"


def test_shioaji_dashboard_is_local_read_only_and_source_backed() -> None:
    root = Path(__file__).resolve().parents[1]
    static_root = root / "services" / "shioaji_api_dashboard"
    html = (static_root / "index.html").read_text(encoding="utf-8")
    javascript = (static_root / "app.js").read_text(encoding="utf-8")
    assert 'fetchWithTimeout("api/status"' in javascript
    assert "traffic-chart" in html
    assert 'id="traffic-legend"' in html
    assert 'data-series="usage"' in html
    assert 'data-series="guard"' in html
    assert "ledger-body" in html
    assert "traffic-breakdown-body" in html
    assert "storage-growth-chart" in html
    assert "storage-body" in html
    assert "fleet-progress-bar" in html
    assert "fleet-eta-label" in html
    assert "pipeline-grid" in html
    assert 'data-filter="historical"' in html
    assert 'data-filter="realtime"' in html
    assert "renderPipelines" in javascript
    assert "pipeline-eta" in javascript
    assert "durationLabel" in javascript
    assert "renderTrafficLedger" in javascript
    assert "renderTrafficBreakdown" in javascript
    assert "renderStorage" in javascript
    assert "HIDDEN_TRAFFIC_SERIES_STORAGE_KEY" in javascript
    assert "button[data-series]" in javascript
    assert "syncTrafficLegend" in javascript
    assert "API Key、Secret" in html
    assert "http://" not in html and "https://" not in html
    assert "textContent" in javascript
    assert "innerHTML" not in javascript


def test_daily_pipeline_is_zero_quota_only_after_local_lineage_audit(
    tmp_path: Path,
) -> None:
    daily_summary = tmp_path / "daily_summary.json"
    dataset_summary = tmp_path / "dataset_summary.json"
    daily_audit = tmp_path / "daily_audit.json"
    daily_summary.write_text(
        json.dumps(
            {
                "universe_coverage_complete": True,
                "materialization_mode": "verified_local_minute",
                "api_requests_started": 0,
                "selected_symbols": 2746,
                "reported_symbols": 2746,
            }
        ),
        encoding="utf-8",
    )
    dataset_summary.write_text(
        json.dumps({"source": "tw_public_before_shioaji_after"}),
        encoding="utf-8",
    )
    daily_audit.write_text(
        json.dumps(
            {
                "status": "ok",
                "materialization_mode": "verified_local_minute",
                "api_requests_started": 0,
                "source_minute_summary_receipt_verified": True,
                "symbols": 2746,
                "hybrid_symbols": 2329,
                "public_source_gap_fallback_rows": 105,
                "public_only_contract_unavailable_symbols": 124,
                "public_only_outside_source_window_symbols": 291,
                "public_only_not_yet_listed_symbols": 2,
            }
        ),
        encoding="utf-8",
    )

    def runner(args):
        command = list(args)
        stdout = (
            "ActiveState=inactive\nSubState=dead\nNRestarts=0\nInvocationID=\n"
            if command[0] == "systemctl"
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    missing = tmp_path / "missing"
    payload = build_shioaji_public_status(
        tmp_path,
        now=datetime(2026, 8, 15, 1, 1, tzinfo=UTC),
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=missing,
            txfr1_manifest=missing,
            futures_history_root=missing,
            target_end_date=missing,
            capture_root=missing,
            daily_summary=daily_summary,
            daily_dataset_summary=dataset_summary,
            daily_audit=daily_audit,
        ),
    )
    daily = next(item for item in payload["pipelines"] if item["id"] == "stock_daily")
    assert daily["status"] == "ready"
    assert daily["quota"] == "none"
    assert daily["metrics"][-1]["value"] == 0
    assert daily["coverage"]["current"] == 2746


def test_minute_pipeline_separates_research_usability_from_latest_freshness(
    tmp_path: Path,
) -> None:
    minute_summary = tmp_path / "download_summary.json"
    minute_run_summary = tmp_path / "latest_run_summary.json"
    minute_manifest = tmp_path / "research_manifest.json"
    minute_audit = tmp_path / "full_audit.json"
    minute_summary.write_text(
        json.dumps({"selected_symbols": 2746, "reported_symbols": 2746}),
        encoding="utf-8",
    )
    minute_run_summary.write_text(
        json.dumps(
            {
                "selected_symbols": 2747,
                "reported_symbols": 4,
                "end_date": "2026-08-21",
                "resumable_collection_complete": False,
                "selected_coverage_complete": False,
                "stopped_for_traffic": True,
                "written_at_utc": "2026-08-22T19:35:46Z",
            }
        ),
        encoding="utf-8",
    )
    minute_manifest.write_text(json.dumps({"research_ready": True}), encoding="utf-8")
    minute_audit.write_text(
        json.dumps(
            {
                "status": "research_ready",
                "last_date": "2026-08-20",
                "available_source_symbols": 2621,
                "source_gap_symbols": 89,
                "contract_unavailable_symbols": 125,
            }
        ),
        encoding="utf-8",
    )

    def runner(args):
        command = list(args)
        stdout = (
            "ActiveState=inactive\nSubState=dead\nNRestarts=0\nInvocationID=\n"
            if command[0] == "systemctl"
            else ""
        )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    missing = tmp_path / "missing"
    payload = build_shioaji_public_status(
        tmp_path,
        now=datetime(2026, 8, 22, 20, 0, tzinfo=UTC),
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=missing,
            txfr1_manifest=missing,
            futures_history_root=missing,
            target_end_date=missing,
            capture_root=missing,
            minute_summary=minute_summary,
            minute_run_summary=minute_run_summary,
            minute_manifest=minute_manifest,
            minute_audit=minute_audit,
        ),
    )

    stock = next(item for item in payload["pipelines"] if item["id"] == "stock_minute")
    research = next(
        item for item in payload["pipelines"] if item["id"] == "minute_research"
    )
    assert stock["status"] == "waiting"
    assert stock["status_label"] == "流量保護暫停"
    assert stock["data_through"] == "2026-08-20"
    assert stock["target_date"] == "2026-08-21"
    assert stock["eta"]["state"] == "waiting_quota"
    assert "89 檔" in stock["warnings"][0]
    assert "125 檔" in stock["warnings"][0]
    assert research["status"] == "partial"
    assert research["data_through"] == "2026-08-20"


def test_shioaji_public_status_allowlists_storage_snapshot(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)
    inventory = tmp_path / "contracts.csv"
    inventory.write_text("contract,priority\n", encoding="utf-8")
    target = tmp_path / "target.txt"
    target.write_text("2026-08-13\n", encoding="utf-8")
    storage = tmp_path / "storage.json"
    storage.write_text(
        json.dumps(
            {
                "generated_at_utc": "2026-08-14T07:59:00Z",
                "scan_seconds": 12.5,
                "summary": {
                    "datasets": 1,
                    "files": 4,
                    "total_bytes": 1000,
                    "source_bytes": 1000,
                    "derived_bytes": 0,
                    "operations_bytes": 0,
                    "growth_window_days": 30,
                    "growth_window_bytes": 300,
                    "average_daily_growth_bytes": 10,
                    "disk_total_bytes": 10000,
                    "disk_used_bytes": 4000,
                    "disk_free_bytes": 6000,
                    "disk_used_ratio": 0.4,
                    "estimated_days_remaining": 600,
                },
                "datasets": [
                    {
                        "id": "ticks",
                        "title": "Tick",
                        "storage_class": "source",
                        "quota_class": "historical",
                        "description": "tick files",
                        "bytes": 1000,
                        "files": 4,
                        "growth_window_days": 30,
                        "growth_window_bytes": 300,
                        "average_daily_growth_bytes": 10,
                        "average_active_day_growth_bytes": 100,
                        "active_growth_days": 3,
                        "growth_source": "file_mtime_estimate",
                        "private_path": "/secret/data",
                    }
                ],
                "daily_growth": [{"date": "2026-08-13", "bytes": 300}],
            }
        ),
        encoding="utf-8",
    )

    def runner(args: list[str] | tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        command = list(args)
        if command[0] == "systemctl":
            stdout = (
                "ActiveState=active\nSubState=running\nNRestarts=0\nInvocationID=test\n"
            )
        else:
            stdout = ""
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    payload = build_shioaji_public_status(
        tmp_path,
        now=observed,
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=inventory,
            txfr1_manifest=tmp_path / "tx.json",
            futures_history_root=tmp_path / "history",
            target_end_date=target,
            capture_root=tmp_path / "capture",
            storage_summary=storage,
        ),
    )
    assert payload["storage"]["status"] == "ready"
    assert payload["storage"]["age_seconds"] == 60
    assert payload["storage"]["summary"]["total_bytes"] == 1000
    assert payload["storage"]["datasets"][0]["average_daily_growth_bytes"] == 10
    assert "private_path" not in json.dumps(payload)


def test_top200_connection_budget_is_intentional_wait_not_failure(
    tmp_path: Path,
) -> None:
    inventory = tmp_path / "contracts.csv"
    inventory.write_text("contract,priority\n", encoding="utf-8")
    target = tmp_path / "target.txt"
    target.write_text("2026-08-13\n", encoding="utf-8")

    def runner(args: list[str] | tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        command = list(args)
        if command[0] == "systemctl":
            unit = command[2]
            state = "inactive" if unit == TOP200_UNIT else "active"
            stdout = (
                f"ActiveState={state}\n"
                f"SubState={'dead' if state == 'inactive' else 'running'}\n"
                "NRestarts=0\nInvocationID=test\n"
            )
        else:
            unit = command[command.index("--unit") + 1]
            stdout = (
                _journal_line(
                    "capture_skipped reason=connection_budget",
                    datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
                    "test",
                )
                if unit == TOP200_UNIT
                else ""
            )
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    payload = build_shioaji_public_status(
        tmp_path,
        now=datetime(2026, 8, 14, 8, 0, tzinfo=UTC),
        runner=runner,
        paths=ShioajiMonitorPaths(
            alias_inventory=inventory,
            txfr1_manifest=tmp_path / "tx.json",
            futures_history_root=tmp_path / "history",
            target_end_date=target,
            capture_root=tmp_path / "capture",
            top200_capture_root=tmp_path / "top200",
        ),
    )
    top200 = next(
        item for item in payload["pipelines"] if item["id"] == "top200_stream"
    )
    assert top200["status"] == "waiting"
    assert top200["status_label"] == "期權優先暫停"
    assert not any(item["status"] == "failed" for item in payload["pipelines"])
