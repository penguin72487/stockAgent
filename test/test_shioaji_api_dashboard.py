from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess

import pytest

from stockagent.live.shioaji_api_dashboard import (
    CAPTURE_UNIT,
    HISTORY_UNIT,
    ShioajiMonitorPaths,
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

    def runner(args: list[str] | tuple[str, ...]) -> subprocess.CompletedProcess[str]:
        command = list(args)
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
    assert payload["dashboard_schema_version"] == 4
    assert {item["id"] for item in payload["pipelines"]} == {
        "contract_catalog",
        "fop_stream",
        "futures_history",
        "hft_dataset",
        "on_demand_snapshots",
        "stock_daily",
        "stock_minute",
        "top200_stream",
    }
    assert payload["pipeline_summary"]["total"] == 8
    assert all(
        item["quota"] in {"historical", "realtime", "none"}
        for item in payload["pipelines"]
    )
    assert all(isinstance(item["fields"], list) for item in payload["pipelines"])
    assert len(payload["traffic_breakdown"]) == 8
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


def test_shioaji_dashboard_is_local_read_only_and_source_backed() -> None:
    root = Path(__file__).resolve().parents[1]
    static_root = root / "services" / "shioaji_api_dashboard"
    html = (static_root / "index.html").read_text(encoding="utf-8")
    javascript = (static_root / "app.js").read_text(encoding="utf-8")
    assert 'fetch("api/status"' in javascript
    assert "traffic-chart" in html
    assert "ledger-body" in html
    assert "traffic-breakdown-body" in html
    assert "storage-growth-chart" in html
    assert "storage-body" in html
    assert "fleet-progress-bar" in html
    assert "pipeline-grid" in html
    assert 'data-filter="historical"' in html
    assert 'data-filter="realtime"' in html
    assert "renderPipelines" in javascript
    assert "renderTrafficLedger" in javascript
    assert "renderTrafficBreakdown" in javascript
    assert "renderStorage" in javascript
    assert "API Key、Secret" in html
    assert "http://" not in html and "https://" not in html
    assert "textContent" in javascript
    assert "innerHTML" not in javascript


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
            stdout = "ActiveState=active\nSubState=running\nNRestarts=0\nInvocationID=test\n"
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
