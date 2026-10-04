from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from downloader import download_finmind_free as worker
from stockagent.live.finmind_dashboard import _network_time_projection, build_finmind_public_status


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_finmind_page_uses_receipts_and_worker_only_quota(tmp_path: Path, monkeypatch) -> None:
    now = datetime(2026, 9, 25, 8, 0, tzinfo=UTC)
    root = tmp_path / "data_finmind"
    root.mkdir()
    _write(root / "status.json", {
        "state": "backfilling", "observed_at_utc": (now - timedelta(seconds=20)).isoformat(),
        "official_requests_per_hour": 300,
        "total_session_day_tasks": 6, "complete_session_day_tasks": 2,
        "retry_deferred_tasks": 1, "token_configured": False,
        "last_task": {"dataset": worker.SESSION_DATASETS[0], "date": "2026-09-24", "status": "complete", "rows": 271, "api_key": "SECRET"},
        "series": {
            worker.SESSION_DATASETS[0]: {"total": 3, "complete": 2, "deferred": 0, "rows": 542,
                                         "bytes": 100, "first_complete_date": "2005-01-03", "last_complete_date": "2026-09-24", "observed_grains": {"1m": 2}},
            worker.SESSION_DATASETS[1]: {"total": 3, "complete": 0, "deferred": 1, "rows": 0, "bytes": 0},
        },
        "news": "disabled_by_user", "secret": "SECRET",
    })
    _write(root / "calendar.json", {
        "source_dataset": worker.CALENDAR_DATASET,
        "observed_at_utc": now.isoformat(),
        "dates": ["2005-01-03", "2026-09-24"],
    })
    _write(root / "complement/status.json", {
        "state": "running", "observed_at_utc": now.isoformat(),
        "series": {"TaiwanExchangeRate": {
            "target": 2, "complete": 0, "observed_empty": 0,
            "failed": 0, "not_entitled": 2, "rows": 0, "bytes": 0,
        }},
        "last_task": {"dataset": "TaiwanExchangeRate", "status": "not_entitled", "token": "SECRET"},
    })
    master_file = root / "snapshots" / worker.MASTER_DATASET / "snapshot=2026-09-25-full.parquet"
    master_file.parent.mkdir(parents=True)
    master_file.write_bytes(b"parquet-test")
    _write(root / "receipts" / worker.MASTER_DATASET / "2026-09-25.json", {
        "status": "complete", "query_scope": "full_table_snapshot", "rows": 200976,
        "parquet_path": str(master_file.relative_to(root)), "parquet_size_bytes": 12,
        "source_first_date": "2021-01-04", "source_last_date": "2026-09-25",
        "snapshot_date_taipei": "2026-09-25", "fetched_at_utc": now.isoformat(),
        "token": "SECRET",
    })
    monkeypatch.setattr(worker, "_utc_now", lambda: now - timedelta(minutes=70))
    worker._record_request_start(root, worker.CALENDAR_DATASET)
    monkeypatch.setattr(worker, "_utc_now", lambda: now - timedelta(minutes=20))
    worker._record_request_start(root, worker.SESSION_DATASETS[0])

    result = build_finmind_public_status(tmp_path, now=now)
    assert result["read_only"] is True
    assert result["health"] == "updating"
    assert result["acquisition"]["complete_session_day_tasks"] == 2
    assert result["acquisition"]["total_tasks"] == 10
    assert result["acquisition"]["not_entitled_tasks"] == 2
    assert result["quota"]["official_requests_per_hour"] == 300
    assert result["quota"]["observed_requests_60m"] == 1
    assert result["quota"]["worker_headroom_60m"] == 299
    assert next(row for row in result["datasets"] if row['id'] == worker.SESSION_DATASETS[0])["rows"] == 542
    assert next(row for row in result["datasets"] if row['id'] == worker.CALENDAR_DATASET)["rows"] == 2
    assert next(row for row in result["datasets"] if row['id'] == worker.MASTER_DATASET)["rows"] == 200976
    assert "TaiwanStockNews" not in result["scope"]["excluded"]
    assert result['acquisition']['news'] == 'enabled_whole_market_calendar_day'
    assert "TaiwanStockPriceTick" in result["scope"]["scheduled_datasets"]
    assert "taiwan_stock_tick_snapshot" in result["scope"]["scheduled_datasets"]
    from downloader.download_finmind_sponsor import SOURCES, UNSCHEDULED
    from downloader.download_finmind_complement import ALL_DATASETS
    assert len(result["datasets"]) == 4 + len(ALL_DATASETS) + len(SOURCES) + len(UNSCHEDULED)
    assert next(row for row in result["datasets"] if row["id"] == "TaiwanExchangeRate")["state"] == "unavailable"
    assert "SECRET" not in json.dumps(result)
    assert "parquet_path" not in json.dumps(result)


def test_finmind_page_rejects_partial_master_and_missing_status(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind"
    _write(root / "receipts" / worker.MASTER_DATASET / "2026-09-25.json", {
        "status": "complete", "rows": 10, "parquet_path": "missing.parquet",
        "parquet_size_bytes": 10,
    })
    result = build_finmind_public_status(tmp_path, now=datetime(2026, 9, 25, 8, tzinfo=UTC))
    assert result["health"] == "unavailable"
    assert next(row for row in result["datasets"] if row['id'] == worker.MASTER_DATASET)["state"] == "pending"
    assert result["quota"]["state"] == "not_started"
    assert result["acquisition"]["total_session_day_tasks"] is None


def test_session_grain_breakdown_preserves_all_historical_cadences(tmp_path: Path) -> None:
    now = datetime(2026, 9, 27, tzinfo=UTC)
    grains = {"1m": 1503, "15s": 764, "10s": 214, "5s": 2862}
    _write(tmp_path / "data_finmind/status.json", {
        "state": "current", "observed_at_utc": now.isoformat(),
        "series": {worker.SESSION_DATASETS[0]: {
            "complete": 5343, "total": 5343,
            "observed_grains": {**grains, "secret": "do-not-export"},
        }},
    })
    result = build_finmind_public_status(tmp_path, now=now)
    row = next(row for row in result["datasets"] if row["id"] == worker.SESSION_DATASETS[0])
    assert row["observed_grains"] == grains
    assert sum(row["observed_grains"].values()) == row["complete_partitions"]
    assert "do-not-export" not in json.dumps(result)


def test_finmind_page_exposes_protected_incremental_budget_without_token(tmp_path: Path) -> None:
    now = datetime(2026, 9, 26, 4, tzinfo=UTC)
    root = tmp_path / "data_finmind"
    _write(root / "account_status.json", {
        "observed_at_utc": now.isoformat(), "tier": "Sponsor",
        "official_requests_per_hour": 6000, "provider_used_in_hour": 100,
    })
    result = build_finmind_public_status(tmp_path, now=now)
    allocation = result["quota"]["backfill_allocation"]
    assert allocation["basis"] == "provider_observation_plus_local_starts"
    assert allocation["reserve"] == 3  # One missing calendar check + two in-flight slots.
    assert allocation['ready_incremental_requests'] == 1
    assert allocation['priority_wait']
    assert allocation["remaining"] == 5900
    assert "token" not in json.dumps(allocation).lower()


def test_sparse_global_history_counts_verified_empty_as_checked_not_rows(tmp_path: Path) -> None:
    root = tmp_path / "data_finmind"
    _write(root / "complement/status.json", {
        "state": "current_queue", "observed_at_utc": datetime(2026, 9, 25, 8, tzinfo=UTC).isoformat(),
        "series": {"TaiwanStockSplitPrice": {
            "target": 3, "complete": 1, "observed_empty": 2,
            "failed": 0, "not_entitled": 0, "rows": 4, "bytes": 100,
        }},
    })
    result = build_finmind_public_status(tmp_path, now=datetime(2026, 9, 25, 8, tzinfo=UTC))
    item = next(row for row in result["datasets"] if row["id"] == "TaiwanStockSplitPrice")
    assert item["state"] == "complete"
    assert item["checked_partitions"] == 3
    assert item["complete_partitions"] == 1
    assert item["observed_empty_partitions"] == 2
    assert item["rows"] == 4


@pytest.mark.parametrize("pending", [None, 0, 120])
def test_partition_projection_never_claims_eta_or_minimum(pending) -> None:
    result = _network_time_projection(pending, 6000)
    assert result["minimum_network_seconds_remaining"] is None
    assert "request_count_unknown" in result["network_time_basis"]
    projection = result["unbatched_task_projection"]
    if pending is None:
        assert projection is None
    else:
        assert projection["seconds"] == round(pending * 3600 / 6000)
        assert projection["pending_partitions"] == pending
        assert projection["is_eta"] is False
        assert projection["is_lower_bound"] is False
        assert "hypothetical_one_call" in projection["basis"]
        assert {"batching", "unknown_identifiers", "refreshes", "retries"} <= set(projection["excludes"])


def test_batched_sources_and_legacy_owners_do_not_publish_request_based_minimum(tmp_path: Path) -> None:
    now = datetime(2026, 9, 27, tzinfo=UTC)
    root = tmp_path / "data_finmind"
    _write(root / "status.json", {
        "observed_at_utc": now.isoformat(), "official_requests_per_hour": 6000,
        "total_session_day_tasks": 0, "complete_session_day_tasks": 0,
    })
    _write(root / "complement/status.json", {
        "observed_at_utc": now.isoformat(),
        "series": {
            "TaiwanStockSplitPrice": {"target": 80, "complete": 10, "observed_empty": 20},
            "TaiwanFuturesFinalSettlementPrice": {"target": 3, "complete": 1},
            "TaiwanStockInstitutionalInvestorsBuySellWide": {"target": 10, "complete": 0},
        },
    })
    _write(root / "sponsor/status.json", {
        "observed_at_utc": now.isoformat(),
        "series": {
            "TaiwanBusinessIndicator": {"target": 45, "complete": 5},
            # Stale pre-v6 receipt: preserve alias observations, not totals.
            "TaiwanFuturesFinalSettlementPrice": {"target": 29, "observed_empty": 27, "failed": 2},
        },
    })
    result = build_finmind_public_status(tmp_path, now=now)
    rows = {row["id"]: row for row in result["datasets"]}
    assert all(row["minimum_network_seconds_remaining"] is None for row in rows.values())
    assert all(row["network_time_basis"] for row in rows.values())
    assert result["acquisition"]["minimum_network_seconds_remaining"] is None
    assert rows["TaiwanBusinessIndicator:all_market"]["unbatched_task_projection"]["pending_partitions"] == 40
    assert rows["TaiwanStockSplitPrice"]["unbatched_task_projection"]["pending_partitions"] == 50
    assert rows["TaiwanStockInstitutionalInvestorsBuySellWide"]["unbatched_task_projection"] is None
    alias = rows["TaiwanFuturesFinalSettlementPrice:all_market"]
    assert alias["state"] == "delegated"
    assert alias["source_status"] == "delegated_to_complement_product_history"
    assert alias["unbatched_task_projection"] is None
    assert alias["observed_empty_partitions"] == 27
    assert result["acquisition"]["total_tasks"] == 2 + 80 + 3 + 10 + 45
    assert result["acquisition"]["observed_empty_tasks"] == 20
    assert result["acquisition"]["failed_tasks"] == 0


def test_finmind_ui_does_not_consume_legacy_partition_based_eta() -> None:
    root = Path(__file__).resolve().parents[1] / "services/finmind_dashboard"
    javascript = (root / "app.js").read_text(encoding="utf-8")
    html = (root / "index.html").read_text(encoding="utf-8")
    assert "minimum_network_seconds_remaining" not in javascript
    assert "最少剩餘時間" not in javascript + html
    assert "目前已知任務最少仍需" not in html
    assert "分割數不等於請求數" in javascript + html
    assert 'unbatched_task_projection' not in javascript
    assert 'id="download-global-eta-basis"' in html
    assert 'app.js?v=21' in html


def test_finmind_ui_legacy_numeric_values_still_render_unknown() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("Node runtime not installed")
    path = Path(__file__).resolve().parents[1] / "services/finmind_dashboard/app.js"
    script = """
const fs = require('fs'), vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8').split('function svgNode')[0];
const context = {window: {StockAgentDashboard: {createJsonFetcher: () => () => {}, byId: () => {}}}};
vm.createContext(context);
vm.runInContext(source + `
globalThis.result = {
  legacy: networkTimeLabel({state: 'backfilling', minimum_network_seconds_remaining: 1200}),
  complete: networkTimeLabel({state: 'complete'}),
  delegated: datasetStatus({state: 'delegated', source_status: 'delegated_to_complement_product_history'}),
  basis: networkTimeBasis({unbatched_task_projection: {
    seconds: 1200, basis: 'hypothetical_one_call_per_known_pending_partition_at_full_shared_quota',
    is_eta: false, is_lower_bound: false
  }})
};`, context);
process.stdout.write(JSON.stringify(context.result));
"""
    result = subprocess.run([node, "-e", script, str(path)], capture_output=True, text=True, check=True, timeout=10)
    rendered = json.loads(result.stdout)
    assert rendered["legacy"] == "未知（分割數不等於請求數）"
    assert rendered["complete"] == "已查驗目前任務"
    assert rendered["delegated"] == "由 Complement 主責"
    assert "沒有可信倒數" in rendered['basis']
    assert "20 分鐘" not in rendered['basis']
    assert "至少" not in rendered["basis"]
