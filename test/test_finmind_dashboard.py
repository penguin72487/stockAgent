from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from downloader import download_finmind_free as worker
from stockagent.live.finmind_dashboard import build_finmind_public_status


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
    assert result["datasets"][0]["rows"] == 542
    assert result["datasets"][2]["rows"] == 2
    assert result["datasets"][3]["rows"] == 200976
    assert "TaiwanStockNews" in result["scope"]["excluded"]
    assert "TaiwanStockPriceTick" in result["scope"]["excluded"]
    from downloader.download_finmind_sponsor import SOURCES, UNSCHEDULED
    assert len(result["datasets"]) == 52 + len(SOURCES) + len(UNSCHEDULED)
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
    assert result["datasets"][3]["state"] == "pending"
    assert result["quota"]["state"] == "not_started"
    assert result["acquisition"]["total_session_day_tasks"] is None


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
