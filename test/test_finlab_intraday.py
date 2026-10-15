from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
from finlab.exceptions import DataError

from scripts.download_finlab_intraday import (
    _checked_this_cycle, _partition_paths, _stored_receipt, _task_days, _validate_frame,
    fetch_partition, record_failed_partition, sync,
)
from scripts.download_finlab_history import safe_stem


def _tick_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "stock_id": ["2330", "2330"],
        "trade_date": ["2026-06-01", "2026-06-01"],
        "timestamp": pd.to_datetime([
            "2026-06-01T09:00:00+08:00", "2026-06-01T09:00:00+08:00",
        ]),
        "sequence": [1, 2], "close": [100.0, 101.0], "volume": [1, 2],
    })


def _minute_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "stock_id": ["2330"], "trade_date": ["2026-06-01"],
        "timestamp": pd.to_datetime(["2026-06-01T09:00:00+08:00"]),
        "open": [100.0], "high": [101.0], "low": [99.0],
        "close": [100.5], "volume": [1000],
    })


def test_source_cooldown_rechecks_when_expired_not_next_account_day(tmp_path):
    day, key = date(2026, 6, 1), "tw_minute:2330"
    with patch("scripts.download_finlab_intraday._transport_metrics", return_value={}):
        assert record_failed_partition(tmp_path, key, day, DataError("rate_limited: fixture")) == "source_rate_limited"
    path = _partition_paths(tmp_path, key, day)[1]
    payload = json.loads(path.read_text())
    before = datetime.fromisoformat(payload["attempted_at_utc"])
    retry = datetime.fromisoformat(payload["retry_at_utc"])
    assert _checked_this_cycle(path, "attempted_at_utc", before)
    assert not _checked_this_cycle(path, "attempted_at_utc", retry)
    assert retry-before == timedelta(seconds=60)


def test_conflict_attempts_stop_at_three_but_resume_after_next_reset(tmp_path):
    key, day = "tw_minute:2330", date(2026, 6, 1)
    for _ in range(3):
        record_failed_partition(tmp_path, key, day, DataError("request_conflict: fixture"))
    path = _partition_paths(tmp_path, key, day)[1]
    payload = json.loads(path.read_text())
    assert payload["cycle_attempts"] == 3 and payload["retry_exhausted"] is True
    retry = datetime.fromisoformat(payload["retry_at_utc"])
    assert _checked_this_cycle(path, "attempted_at_utc", retry)
    assert not _checked_this_cycle(path, "attempted_at_utc", retry+timedelta(days=1))


@pytest.mark.parametrize("column,value", [("open", float("nan")), ("close", float("inf")), ("low", -1)])
def test_minutes_reject_non_finite_or_non_positive_prices(column, value):
    frame = _minute_frame()
    frame.loc[0, column] = value
    with pytest.raises(ValueError, match="minute prices"):
        _validate_frame(frame, "tw_minute:2330", date(2026, 6, 1))


def test_minute_timestamp_day_and_alignment_must_match_the_partition():
    frame = _minute_frame()
    frame["timestamp"] = pd.to_datetime(["2026-06-02T09:00:00+08:00"])
    with pytest.raises(ValueError, match="Taipei trade date"):
        _validate_frame(frame, "tw_minute:2330", date(2026, 6, 1))
    frame["timestamp"] = pd.to_datetime(["2026-06-01T09:00:01+08:00"])
    with pytest.raises(ValueError, match="minute aligned"):
        _validate_frame(frame, "tw_minute:2330", date(2026, 6, 1))


def test_tick_same_timestamp_keeps_both_provider_rows():
    day = date(2026, 6, 1)
    frame = _tick_frame()
    assert _validate_frame(frame, "tw_tick:2330", day) == "downloaded_unverified_for_pit"
    with tempfile.TemporaryDirectory() as directory, patch(
        "finlab.data.get", return_value=frame,
    ) as get:
        root = Path(directory)
        receipt = fetch_partition(root, "tw_tick:2330", day,
                                  now=datetime(2026, 9, 25, tzinfo=UTC))
        assert receipt["rows"] == 2
        assert pd.read_parquet(root / receipt["parquet_path"])["sequence"].tolist() == [1, 2]
        assert _stored_receipt(
            root / "intraday/receipts" / safe_stem("tw_tick:2330") / "2026-06-01.json",
            root, "tw_tick:2330", day,
        ) is not None
        assert get.call_args.kwargs["start"] == "2026-06-01"
        assert get.call_args.kwargs["end"] == "2026-06-01"


def test_empty_day_requires_provider_closed_or_no_trade_evidence():
    day = date(2026, 6, 1)
    with pytest.raises(ValueError, match="lacks provider"):
        _validate_frame(pd.DataFrame(), "tw_minute:2330", day)
    closed = pd.DataFrame()
    closed.attrs["closed_dates"] = ["2026-06-01"]
    assert _validate_frame(closed, "tw_minute:2330", day) == "verified_closed_date"


def test_sync_accounts_for_unpublished_dates_without_inventing_rows():
    day = date(2026, 6, 1)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        catalog = root / "catalog/discovery.json"
        catalog.parent.mkdir()
        catalog.write_text(json.dumps({"keys": ["tw_minute:2330", "tw_tick:2330"]}))
        with patch("scripts.download_finlab_intraday.credential_available", return_value=True), patch(
            "scripts.download_finlab_intraday.quota_room_mb", return_value=(1000.0, 5000.0),
        ), patch("finlab.data.get", side_effect=DataError("not_ready: unpublished")):
            result = sync(root, start=day, end=day, limit=2,
                          reserve_mb=50, now=datetime(2026, 9, 25, tzinfo=UTC))
        assert result["requested_weekday_partitions"] == 1
        assert result["receipted_partitions"] == 0
        assert result["remaining_partitions"] == 1
        assert result["by_key"]["tw_tick:2330"]["provider_not_ready_partitions"] == 1
        assert len(_task_days(day, day)) == 1
