from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from downloader.materialize_ohlcv_daily import _daily_frame


def _minute_frame(timestamps: list[datetime]) -> pl.DataFrame:
    size = len(timestamps)
    return pl.DataFrame(
        {
            "date": timestamps,
            "open": [float(index + 1) for index in range(size)],
            "max": [float(index + 2) for index in range(size)],
            "min": [float(index) for index in range(size)],
            "close": [float(index + 1.5) for index in range(size)],
            "Trading_Volume": [1.0] * size,
        }
    )


def test_daily_materializer_aggregates_utc_days_and_reports_partial_grid(
    tmp_path: Path,
) -> None:
    timestamps = pl.datetime_range(
        datetime(2026, 8, 13, tzinfo=timezone.utc),
        datetime(2026, 8, 14, 0, 1, tzinfo=timezone.utc),
        interval="1m",
        eager=True,
        time_zone="UTC",
    ).to_list()
    source = tmp_path / "BTC_USDT_features.parquet"
    _minute_frame(timestamps).write_parquet(source)

    result = _daily_frame(source)

    assert result["date"].to_list() == ["2026-08-13", "2026-08-14"]
    assert result["source_minute_rows"].to_list() == [1440, 2]
    assert result["minute_grid_complete"].to_list() == [True, False]
    assert result["Trading_Volume"].to_list() == [1440.0, 2.0]
    assert result["open"].to_list()[0] == 1.0
    assert result["close"].to_list()[0] == 1440.5

    tail = _daily_frame(source, start_day=date(2026, 8, 14))
    assert tail["date"].to_list() == ["2026-08-14"]
    assert tail["source_minute_rows"].to_list() == [2]


def test_daily_materializer_rejects_duplicate_minute_volume(tmp_path: Path) -> None:
    duplicate = datetime(2026, 8, 13, tzinfo=timezone.utc)
    source = tmp_path / "duplicate_features.parquet"
    _minute_frame([duplicate, duplicate]).write_parquet(source)

    with pytest.raises(ValueError, match="duplicate one-minute timestamps"):
        _daily_frame(source)


def test_daily_materializer_rejects_off_grid_timestamp(tmp_path: Path) -> None:
    source = tmp_path / "off_grid_features.parquet"
    _minute_frame(
        [datetime(2026, 8, 13, 0, 0, 1, tzinfo=timezone.utc)]
    ).write_parquet(source)

    with pytest.raises(ValueError, match="not aligned"):
        _daily_frame(source)
