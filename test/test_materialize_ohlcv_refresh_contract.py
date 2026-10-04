"""Exercise the real CLI/main against tiny, private canonical Parquet fixtures."""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from downloader import materialize_ohlcv_daily as materializer
from downloader.ohlcv_hot_tail import hot_tail_path


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = cls(2026, 8, 20, 12, tzinfo=timezone.utc)
        return fixed.replace(tzinfo=None) if tz is None else fixed.astimezone(tz)


@pytest.fixture(autouse=True)
def fixed_completed_day_boundary(monkeypatch):
    monkeypatch.setattr(materializer, "datetime", FixedDateTime)


def minute_frame(days, *, values=None):
    timestamps = [datetime(2026, 8, day, tzinfo=timezone.utc) for day in days]
    amounts = [float(day) for day in days] if values is None else list(values)
    return pl.DataFrame({
        "date": timestamps,
        "open": amounts,
        "max": [value + 1 for value in amounts],
        "min": [value - 1 for value in amounts],
        "close": [value + 0.5 for value in amounts],
        "adjclose": [999.0] * len(days),  # Daily adjclose comes from canonical close.
        "Trading_Volume": amounts,
    })


def seed_target(tmp_path, original):
    source = tmp_path / "1m" / "X_features.parquet"
    target = tmp_path / "daily" / source.name
    source.parent.mkdir()
    target.parent.mkdir()
    original.write_parquet(source)
    materializer._daily_frame(source).write_parquet(target)
    return source, target


def run_main(monkeypatch, source, target, *, refresh):
    argv = ["materialize_ohlcv_daily", "--input-dir", str(source.parent),
            "--output-dir", str(target.parent), "--provider", "fixture", "--workers", "1"]
    if refresh:
        argv.append("--refresh")
    monkeypatch.setattr(sys, "argv", argv)
    materializer.main()
    summary = json.loads((target.parent / "download_summary.json").read_text())
    report = pl.read_csv(target.parent / "download_report.csv")
    return summary, report


@pytest.mark.parametrize("change", ["new_head", "old_middle_correction", "removed_source_day"])
def test_refresh_rebuilds_all_completed_source_days_despite_newer_target_mtime(
    tmp_path, monkeypatch, change,
):
    original = minute_frame([2, 3, 4, 5]) if change == "new_head" else minute_frame([1, 2, 3, 4, 5])
    source, target = seed_target(tmp_path, original)
    fresh = (minute_frame([1, 2, 3, 4, 5]) if change == "new_head" else
             minute_frame([1, 2, 3, 4, 5], values=[1.0, 200.0, 3.0, 4.0, 5.0])
             if change == "old_middle_correction" else minute_frame([2, 3, 4, 5]))
    fresh.write_parquet(source)
    expected = materializer._daily_frame(source)
    newer = source.stat().st_mtime_ns + 10_000_000_000
    os.utime(target, ns=(newer, newer))
    source_bytes = source.read_bytes()

    summary, report = run_main(monkeypatch, source, target, refresh=True)

    assert_frame_equal(pl.read_parquet(target), expected)
    assert source.read_bytes() == source_bytes
    assert summary["status_counts"] == {"updated": 1}
    assert summary["requested_reconciliation_scope"] == "full_source"
    assert summary["row_count"] == expected.height
    assert report["status"].to_list() == ["updated"]


def test_nonrefresh_keeps_incremental_tail_window_and_existing_older_days(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([1, 2, 3, 4, 5]))
    original_daily = pl.read_parquet(target)
    minute_frame([1, 2, 3, 4, 5, 6], values=[1.0, 200.0, 3.0, 40.0, 50.0, 60.0]).write_parquet(source)
    expected = pl.concat([
        original_daily.filter(pl.col("date") < "2026-08-04"),
        materializer._daily_frame(source).filter(pl.col("date") >= "2026-08-04"),
    ])
    older = source.stat().st_mtime_ns - 1_000_000_000
    os.utime(target, ns=(older, older))
    reads = []
    original_read = materializer.read_logical_parquet

    def read(*args, **kwargs):
        reads.append(kwargs.get("filters"))
        return original_read(*args, **kwargs)

    monkeypatch.setattr(materializer, "read_logical_parquet", read)
    summary, _ = run_main(monkeypatch, source, target, refresh=False)

    assert_frame_equal(pl.read_parquet(target), expected)
    assert len(reads) == 1 and str(reads[0][0][2])[:10] == "2026-08-04"
    assert summary["status_counts"] == {"updated": 1}
    assert summary["requested_reconciliation_scope"] == "incremental"
    assert pl.read_parquet(target).filter(pl.col("date") == "2026-08-02")["open"].item() == 2.0


def test_nonrefresh_retains_existing_up_to_date_skip(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([1, 2, 3]))
    newer = source.stat().st_mtime_ns + 10_000_000_000
    os.utime(target, ns=(newer, newer))
    before = target.read_bytes()

    def unexpected_read(*args, **kwargs):
        raise AssertionError("up-to-date nonrefresh must not decode minute rows")

    monkeypatch.setattr(materializer, "read_logical_parquet", unexpected_read)
    summary, _ = run_main(monkeypatch, source, target, refresh=False)

    assert target.read_bytes() == before
    assert summary["status_counts"] == {"skipped_up_to_date": 1}


def test_refresh_preserves_full_partial_grid_and_closed_utc_day_contract(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([18]))
    timestamps = [datetime(2026, 8, 18, tzinfo=timezone.utc) + timedelta(minutes=i)
                  for i in range(1440)] + [
                      datetime(2026, 8, 19, tzinfo=timezone.utc),
                      datetime(2026, 8, 19, 0, 1, tzinfo=timezone.utc),
                      datetime(2026, 8, 20, tzinfo=timezone.utc),
                  ]
    source_frame = minute_frame([18] * len(timestamps)).with_columns(
        pl.Series("date", timestamps), pl.lit(1.0).alias("Trading_Volume"),
    )
    source_frame.write_parquet(source)
    summary, _ = run_main(monkeypatch, source, target, refresh=True)
    result = pl.read_parquet(target)

    assert result["date"].to_list() == ["2026-08-18", "2026-08-19"]
    assert result["source_minute_rows"].to_list() == [1440, 2]
    assert result["Trading_Volume"].to_list() == [1440.0, 2.0]
    assert result["expected_minute_rows"].to_list() == [1440, 1440]
    assert result["minute_grid_complete"].to_list() == [True, False]
    assert result["minute_coverage_ratio"].to_list() == [1.0, 2 / 1440]
    assert result["adjclose"].to_list() == result["close"].to_list()
    assert summary["partial_day_count"] == 1


def test_refresh_keeps_logical_hot_tail_override_without_deleting_sources(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([2, 3]))
    minute_frame([1, 2, 3]).write_parquet(source)
    tail = hot_tail_path(source)
    tail.parent.mkdir()
    minute_frame([3, 4], values=[30.0, 40.0]).write_parquet(tail)
    expected = materializer._daily_frame(source)
    source_bytes, tail_bytes = source.read_bytes(), tail.read_bytes()

    run_main(monkeypatch, source, target, refresh=True)

    assert_frame_equal(pl.read_parquet(target), expected)
    assert source.read_bytes() == source_bytes and tail.read_bytes() == tail_bytes
    assert expected.filter(pl.col("date") == "2026-08-03")["Trading_Volume"].item() == 30.0


def test_refresh_repairs_corrupt_projection_without_reading_unused_adjclose(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([1, 2, 3]))
    expected = materializer._daily_frame(source)
    target.write_bytes(b"broken derived parquet")
    original = materializer.read_logical_parquet
    selected = []

    def read(*args, **kwargs):
        selected.append(kwargs["columns"])
        return original(*args, **kwargs)

    monkeypatch.setattr(materializer, "read_logical_parquet", read)
    summary, _ = run_main(monkeypatch, source, target, refresh=True)

    assert_frame_equal(pl.read_parquet(target), expected)
    assert selected == [["date", "open", "max", "min", "close", "Trading_Volume"]]
    assert summary["status_counts"] == {"updated": 1}


def test_refresh_with_only_current_day_does_not_erase_completed_daily(tmp_path, monkeypatch):
    source, target = seed_target(tmp_path, minute_frame([1, 2, 3]))
    before = target.read_bytes()
    minute_frame([20]).write_parquet(source)

    summary, _ = run_main(monkeypatch, source, target, refresh=True)

    assert target.read_bytes() == before
    assert summary["status_counts"] == {"skipped_no_completed_day": 1}
    assert summary["row_count"] == 3


@pytest.mark.parametrize("damage", [
    "old_duplicate", "old_off_grid", "missing_column", "read_error", "write_error", "replace_error",
])
def test_refresh_failure_keeps_prior_daily_bytes_and_persists_failed_report(
    tmp_path, monkeypatch, damage,
):
    source, target = seed_target(tmp_path, minute_frame([1, 2, 3, 4, 5]))
    before = target.read_bytes()
    fresh = minute_frame([1, 2, 3, 4, 5], values=[1.0, 200.0, 3.0, 4.0, 5.0])
    if damage == "old_duplicate":
        fresh = pl.concat([fresh, fresh.head(1)])
    elif damage == "old_off_grid":
        fresh = fresh.with_columns(pl.when(pl.col("date").dt.day() == 1)
                                   .then(pl.col("date") + pl.duration(seconds=1))
                                   .otherwise(pl.col("date")).alias("date"))
    elif damage == "missing_column":
        fresh = fresh.drop("open")
    fresh.write_parquet(source)
    if damage in {"read_error", "write_error"}:
        def fail(*args, **kwargs):
            raise OSError(f"fixture {damage}")
        monkeypatch.setattr(materializer, "read_logical_parquet" if damage == "read_error"
                            else "_write_parquet_atomic", fail)
    elif damage == "replace_error":
        original_replace = os.replace

        def fail_target_replace(src, dst, *args, **kwargs):
            if Path(dst) == target:
                raise OSError("fixture replace_error")
            return original_replace(src, dst, *args, **kwargs)

        monkeypatch.setattr(os, "replace", fail_target_replace)

    with pytest.raises(RuntimeError, match="daily materialization incomplete"):
        run_main(monkeypatch, source, target, refresh=True)

    assert target.read_bytes() == before
    assert not list(target.parent.glob(f".{target.name}.*.tmp"))
    summary = json.loads((target.parent / "download_summary.json").read_text())
    report = pl.read_csv(target.parent / "download_report.csv")
    assert summary["status_counts"] == {"failed": 1}
    assert report["status"].to_list() == ["failed"]
    message = report["message"].item()
    expected = {"old_duplicate": "duplicate", "old_off_grid": "aligned",
                "missing_column": "missing canonical", "read_error": "fixture read_error",
                "write_error": "fixture write_error", "replace_error": "fixture replace_error"}[damage]
    assert expected in message
