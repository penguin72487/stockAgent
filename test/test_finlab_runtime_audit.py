from datetime import UTC, date, datetime
import sqlite3
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest

from scripts.audit_finlab_runtime import measure_pipeline, systemd_properties, verify_recent_partitions
from scripts.download_finlab_intraday import fetch_partition
from scripts.download_finlab_market_intraday import _open_index, _record


def test_systemd_timer_properties_keep_every_repeated_schedule():
    properties = systemd_properties(
        "ActiveState=active\n"
        "TimersMonotonic={ OnUnitInactiveUSec=1min ; next_elapse=1min }\n"
        "TimersMonotonic={ OnActiveUSec=1min ; next_elapse=2min }\n"
        "TimersCalendar={ OnCalendar=*-*-* 08:00:05 Asia/Taipei }\n"
        "TimersCalendar={ OnCalendar=*-*-* 07:00:05 Asia/Taipei }\n")
    assert properties["ActiveState"] == "active"
    assert "OnUnitInactiveUSec=1min" in properties["TimersMonotonic"]
    assert "OnActiveUSec=1min" in properties["TimersMonotonic"]
    assert "08:00:05 Asia/Taipei" in properties["TimersCalendar"]
    assert "07:00:05 Asia/Taipei" in properties["TimersCalendar"]


@pytest.fixture
def source_partition(tmp_path):
    source = tmp_path / "data_finlab"
    day = date(2026, 6, 1)
    frame = pd.DataFrame({
        "stock_id": ["2330"], "trade_date": [day.isoformat()],
        "timestamp": pd.to_datetime(["2026-06-01T09:00:00+08:00"]),
        "open": [100.0], "high": [101.0], "low": [99.0],
        "close": [100.5], "volume": [1000],
    })
    with patch("finlab.data.get", return_value=frame):
        receipt = fetch_partition(source, "tw_minute:2330", day, now=datetime(2026, 9, 25, tzinfo=UTC))
    db = _open_index(source / "intraday/market_index.sqlite3")
    _record(db, "tw_minute", "2330", day, receipt["status"], receipt["rows"], receipt["parquet_size_bytes"])
    db.close()
    return tmp_path, source, receipt


def test_recent_object_audit_uses_existing_source_semantics_without_api(source_partition):
    root, source, receipt = source_partition
    with patch("finlab.data.get", side_effect=AssertionError("audit must stay offline")):
        result = verify_recent_partitions(root, 1)
    assert len(result) == 1 and result[0]["accepted"]
    assert result[0]["dataset"] == "tw_minute:2330"
    assert all(result[0]["checks"].values())
    assert (source / receipt["parquet_path"]).is_file()


def test_recent_object_audit_rejects_missing_object(source_partition):
    root, source, receipt = source_partition
    (source / receipt["parquet_path"]).unlink()
    assert verify_recent_partitions(root, 1)[0]["accepted"] is False


def test_recent_object_audit_rejects_index_row_mismatch(source_partition):
    root, source, _ = source_partition
    with sqlite3.connect(source / "intraday/market_index.sqlite3") as db:
        db.execute("UPDATE partitions SET rows=999 WHERE kind='tw_minute'")
    result = verify_recent_partitions(root, 1)[0]
    assert result["checks"]["rows"] is False and result["accepted"] is False


def test_recent_object_audit_never_creates_a_missing_index(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        verify_recent_partitions(tmp_path, 1)
    assert not (tmp_path / "data_finlab").exists()


def test_recent_tick_audit_checks_its_own_receipt_not_derived_minutes(source_partition):
    root, source, _ = source_partition
    day = date(2026, 6, 1)
    frame = pl.DataFrame({
        "stock_id": ["2330"], "trade_date": [day],
        "timestamp": [datetime(2026, 6, 1, 1, tzinfo=UTC)],
        "price": [100.5], "volume": [1000], "sequence": [1], "session": ["regular"],
    })
    with patch("finlab.data.get", return_value=frame):
        receipt = fetch_partition(source, "tw_tick:2330", day, now=datetime(2026, 9, 25, tzinfo=UTC))
    db = _open_index(source / "intraday/market_index.sqlite3")
    _record(db, "tw_tick", "2330", day, receipt["status"], receipt["rows"], receipt["parquet_size_bytes"])
    db.close()
    with patch("finlab.data.get", side_effect=AssertionError("audit must stay offline")):
        minute = verify_recent_partitions(root, 1)
        tick = verify_recent_partitions(root, 1, kind="tw_tick")
    assert minute[0]["dataset"] == "tw_minute:2330"
    assert tick[0]["dataset"] == "tw_tick:2330"
    assert all(tick[0]["checks"].values()) and tick[0]["accepted"]
    (source / receipt["parquet_path"]).unlink()
    assert verify_recent_partitions(root, 1, kind="tw_tick")[0]["accepted"] is False
    assert verify_recent_partitions(root, 1)[0]["accepted"]


@pytest.mark.parametrize("count,kind", [(1, "tw_day"), (-1, "tw_tick"), (33, "tw_tick"), (True, "tw_minute")])
def test_recent_audit_rejects_unbounded_or_wrong_family_before_opening_index(tmp_path, count, kind):
    with pytest.raises(ValueError, match="verification scope"):
        verify_recent_partitions(tmp_path, count, kind=kind)
    assert not (tmp_path / "data_finlab").exists()


def test_pipeline_measurement_never_sums_overlapping_worker_timings_or_claims_official_max():
    def batch(seconds, sdk_calls):
        return {"kind": "tw_minute", "state": "batch_limit", "attempted": 100,
                "completed_attempts": 100, "new_receipted_partitions": 100,
                "elapsed_seconds": seconds, "source_seconds": seconds * 4,
                "sdk_calls": sdk_calls, "started_at_utc": "2026-10-09T12:00:00+00:00",
                "finished_at_utc": "2026-10-09T12:05:00+00:00"}

    result = measure_pipeline([batch(300, 100)], [batch(100, 5), {**batch(1, 1), "state": "interrupted"}])
    assert result["observed_rate_ratio"] == 3
    assert result["after"]["whole_owner_seconds"] == 100
    assert result["after"]["sdk_calls"] == 5
    assert not result["matched_source_benchmark"] and not result["official_maximum_throughput_proven"]


def test_partial_or_timed_out_batches_cannot_calibrate_full_pipeline_throughput():
    full = {"kind": "tw_minute", "state": "batch_limit", "attempted": 100, "attempt_limit": 100,
            "completed_attempts": 100, "new_receipted_partitions": 100, "elapsed_seconds": 300,
            "started_at_utc": "2026-10-09T12:00:00+00:00", "finished_at_utc": "2026-10-09T12:05:00+00:00"}
    invalid = [{**full, "state": "window_time_budget_reached", "elapsed_seconds": 1},
               {**full, "state": "waiting_required_general_data", "elapsed_seconds": 1},
               {**full, "attempted": 20, "completed_attempts": 20, "new_receipted_partitions": 20}]
    result = measure_pipeline([full, *invalid], [full, *invalid])
    assert result["contract_version"] == 2 and result["observed_rate_ratio"] == 1
    assert result["before"]["batches"] == result["after"]["batches"] == 1
