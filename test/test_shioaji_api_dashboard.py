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
    assert "fleet-progress-bar" in html
    assert "API Key、Secret" in html
    assert "http://" not in html and "https://" not in html
    assert "textContent" in javascript
