from datetime import UTC, date, datetime
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest
from finlab.exceptions import DataError

from scripts.download_finlab_intraday import (
    _stored_receipt, _task_days, _validate_frame, fetch_partition, sync,
)
from scripts.download_finlab_history import safe_stem


def _tick_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "stock_id": ["2330", "2330"],
        "trade_date": ["2026-06-01", "2026-06-01"],
        "timestamp": pd.to_datetime([
            "2026-06-01T09:00:00+08:00", "2026-06-01T09:00:00+08:00",
        ]),
        "sequence": [1, 2], "close": [100.0, 101.0],
    })


def _minute_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "stock_id": ["2330"], "trade_date": ["2026-06-01"],
        "timestamp": pd.to_datetime(["2026-06-01T09:00:00+08:00"]),
        "open": [100.0], "high": [101.0], "low": [99.0],
        "close": [100.5], "volume": [1000],
    })


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
