from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from io import BytesIO
from pathlib import Path
import shutil
from zipfile import ZIP_DEFLATED, ZipFile

import polars as pl
import pytest

from downloader.download_binance_public_archive import (
    ArchiveObject,
    _canonical_merge,
    _capacity_receipt,
    _download_states,
    _is_requested_symbol,
    _parse_kline_zip,
    _parse_listing,
    _promote_monthly_repairs,
    _record_state,
    _completed_etags,
)


def _object(*, granularity: str = "monthly", size: int = 100) -> ArchiveObject:
    period = "2025-01" if granularity == "monthly" else "2025-01-01"
    return ArchiveObject(
        market="spot",
        symbol="BTCUSDT",
        archive_granularity=granularity,
        period=period,
        partition="2025-01",
        key=(
            f"data/spot/{granularity}/klines/BTCUSDT/1m/"
            f"BTCUSDT-1m-{period}.zip"
        ),
        etag="etag",
        compressed_bytes=size,
        archive_last_modified_utc="2025-02-03T00:00:00Z",
    )


def _zip_csv(text: str) -> bytes:
    payload = BytesIO()
    with ZipFile(payload, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("BTCUSDT-1m-2025-01.csv", text)
    return payload.getvalue()


def _kline_row(*, open_time: int = 1_735_689_600_000, close: str = "2") -> str:
    return (
        f"{open_time},1,3,0.5,{close},10,{open_time + 59_999},"
        "20,5,4,8,0\n"
    )


def test_parse_s3_listing_with_namespace_and_pagination() -> None:
    payload = b"""<?xml version='1.0' encoding='UTF-8'?>
    <ListBucketResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>
      <Contents><Key>data/a.zip</Key><LastModified>2025-01-02T00:00:00Z</LastModified><ETag>&quot;abc&quot;</ETag><Size>123</Size></Contents>
      <CommonPrefixes><Prefix>data/BTCUSDT/</Prefix></CommonPrefixes>
      <NextContinuationToken>next</NextContinuationToken>
    </ListBucketResult>"""

    objects, prefixes, token = _parse_listing(payload)

    assert objects == [
        {
            "key": "data/a.zip",
            "etag": "abc",
            "size": 123,
            "last_modified": "2025-01-02T00:00:00Z",
        }
    ]
    assert prefixes == ["data/BTCUSDT/"]
    assert token == "next"


def test_spot_microsecond_archive_is_normalized_to_milliseconds() -> None:
    row = (
        "1735689600000000,1,3,0.5,2,10,1735689659999999,20,5,4,8,0\n"
    )

    frame = _parse_kline_zip(_zip_csv(row), _object())

    assert frame["open_time"].item() == 1_735_689_600_000
    assert frame["close_time"].item() == 1_735_689_659_999
    assert frame["source_timestamp_unit"].item() == "microseconds"


def test_exact_official_duplicate_rows_are_losslessly_deduplicated() -> None:
    row = _kline_row()

    frame = _parse_kline_zip(_zip_csv(row + row), _object())

    assert frame.height == 1
    assert frame["source_exact_duplicate_rows_removed"].item() == 1


def test_conflicting_official_duplicate_rows_fail_closed() -> None:
    payload = _zip_csv(_kline_row(close="2") + _kline_row(close="2.5"))

    with pytest.raises(ValueError, match="conflicting duplicate"):
        _parse_kline_zip(payload, _object())


def test_daily_row_overrides_monthly_row_for_same_open_time() -> None:
    monthly = pl.DataFrame(
        {
            "market": ["spot"],
            "symbol": ["BTCUSDT"],
            "open_time": [1_000],
            "close": [10.0],
            "source_priority": [1],
            "available_at_utc": ["2025-02-01T00:00:00Z"],
            "archive_key": ["monthly.zip"],
        }
    )
    daily = monthly.with_columns(
        pl.lit(11.0).alias("close"),
        pl.lit(2).alias("source_priority"),
        pl.lit("2025-01-02T00:00:00Z").alias("available_at_utc"),
        pl.lit("daily.zip").alias("archive_key"),
    )

    merged = _canonical_merge(monthly, [daily])

    assert merged.height == 1
    assert merged["close"].item() == 11.0
    assert merged["archive_key"].item() == "daily.zip"


def test_derivatives_scope_only_accepts_dated_futures() -> None:
    assert _is_requested_symbol("spot", "BTCUSDT")
    assert _is_requested_symbol("um", "BTCUSDT_250627")
    assert _is_requested_symbol("cm", "BTCUSD_250627")
    assert not _is_requested_symbol("um", "BTCUSDT")
    assert not _is_requested_symbol("cm", "BTCUSD_PERP")


def test_capacity_gate_fails_before_download(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _: shutil._ntuple_diskusage(total=1_000, used=100, free=900),
    )

    receipt = _capacity_receipt(
        tmp_path,
        [_object(size=400)],
        reserve_gib=0,
        max_download_bytes=0,
    )

    assert receipt["accepted"] is False
    assert receipt["estimated_peak_new_bytes"] == 1_000
    assert receipt["reasons"] == [
        "estimated_peak_bytes_exceeds_free_space_after_reserve"
    ]


def test_invalid_monthly_object_is_quarantined_for_daily_rebuild(
    tmp_path: Path,
) -> None:
    state = tmp_path / "state.sqlite3"
    item = _object()
    _record_state(
        state,
        item,
        status="failed",
        error=f"{item.key}: 3 invalid OHLCV rows",
    )

    repairs = _promote_monthly_repairs(state)

    assert repairs == {("spot", "BTCUSDT", "2025-01")}
    assert _completed_etags(state) == {item.key: item.etag}


def test_parallel_state_writes_are_serialized(tmp_path: Path) -> None:
    state = tmp_path / "state.sqlite3"
    items = [
        replace(
            _object(),
            key=f"data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-01-{day:02d}.zip",
            etag=f"etag-{day}",
        )
        for day in range(1, 33)
    ]

    with ThreadPoolExecutor(max_workers=32) as executor:
        list(
            executor.map(
                lambda item: _record_state(state, item, status="complete"),
                items,
            )
        )

    completed = _completed_etags(state)
    assert len(completed) == len(items)
    assert set(completed) == {item.key for item in items}


def test_durable_source_quarantine_keeps_data_partial_without_failing_clean_cycle() -> None:
    counts = {
        "failed": 0,
        "quarantined_repair_required": 0,
        "quarantined_source_invalid": 0,
    }
    durable_counts = {"complete": 10, "quarantined_source_invalid": 1}

    dataset_state, cycle_state = _download_states(counts, durable_counts)

    assert dataset_state == "partial"
    assert cycle_state == "complete"


@pytest.mark.parametrize(
    "failure_name",
    ("failed", "quarantined_repair_required", "quarantined_source_invalid"),
)
def test_new_archive_failure_fails_current_cycle(failure_name: str) -> None:
    counts = {
        "failed": 0,
        "quarantined_repair_required": 0,
        "quarantined_source_invalid": 0,
    }
    counts[failure_name] = 1

    _, cycle_state = _download_states(counts, {})

    assert cycle_state == "failed"
