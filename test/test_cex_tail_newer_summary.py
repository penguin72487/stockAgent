from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import asdict
from unittest.mock import patch

import polars as pl
import pytest

from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx
from downloader.ohlcv_hot_tail import tail_newer_summary
from scripts.benchmark_cex_tail_overlap import benchmark


BASE_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def reference(frame, normalize, base_ms=BASE_MS):
    dates = normalize(frame).select(pl.col("date").str.to_datetime(strict=False).alias("__ts"))
    cutoff = datetime.fromtimestamp(base_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
    newer = dates.filter(pl.col("__ts") > cutoff)
    return newer.height, newer.select(pl.col("__ts").min()).item() if newer.height else None


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("values", [
    ["2026-01-01 00:02:00", "2026-01-01 00:00:00", "2026-01-01 00:01:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
    ["2025-12-31 23:59:00"],
    ["2026-01-01 00:00:00"],
    ["2026-01-01 00:00:59.999999", "2026-01-01 00:01:00.100000"],
    ["2026-01-01T00:01:00", "2026-01-01T00:02:00"],
    ["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
    ["2026-01-01T08:01:00+08:00"],
    ["2026-01-01 00:01:00", None],
    ["2026-01-01 00:01:00", "2026-01-01 00:99:00"],
    ["2026-01-01 00:01:00", "bad"],
    ["bad"],
    [None],
    [],
])
def test_same_result_or_exception_as_complete_legacy_path(module, values):
    frame = pl.DataFrame({"date": pl.Series(values, dtype=pl.String)})
    before = frame.clone()
    normalize = module._normalize_date_frame
    try:
        expected = reference(frame, normalize)
    except Exception as error:
        with pytest.raises(type(error)):
            tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=normalize)
    else:
        assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=normalize) == expected
    assert frame.equals(before)


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("dtype", [pl.Datetime("us"), pl.Datetime("ns"), pl.Date, pl.Int64])
def test_non_string_keeps_legacy_cast_and_precision(module, dtype):
    dates = pl.Series([datetime(2026, 1, 1, 0, 1, 0, 999999), datetime(2025, 12, 31, 23, 59)])
    frame = pl.DataFrame({"date": dates.cast(dtype)})
    assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=module._normalize_date_frame) == reference(
        frame, module._normalize_date_frame)


def test_canonical_writer_dates_use_native_path_without_sort_or_string_format():
    frame = pl.DataFrame({"date": ["2026-01-01 00:02:00", "2026-01-01 00:01:00"]})

    def forbidden(_):
        raise AssertionError("complete normalization is unused for exact writer strings")

    assert tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=forbidden) == (
        2, datetime(2026, 1, 1, 0, 1))


def test_subsecond_base_cutoff_and_duplicate_count_are_not_rounded_or_deduplicated():
    frame = pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:00:01", "2026-01-01 00:00:01"]})
    assert tail_newer_summary(frame, base_latest_ms=BASE_MS + 999, normalize=bybit._normalize_date_frame) == (
        2, datetime(2026, 1, 1, 0, 0, 1))


def test_missing_column_keeps_reference_exception():
    frame = pl.DataFrame({"other": [1]})
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        tail_newer_summary(frame, base_latest_ms=BASE_MS, normalize=bybit._normalize_date_frame)


@pytest.mark.parametrize("module", [binance, bybit, okx])
@pytest.mark.parametrize("tail_values", [
    ["2026-01-01 00:01:00", "2026-01-01 00:02:00"],
    ["2026-01-01 00:04:00"],
    ["2026-01-01 00:01:00", "2026-01-01 00:01:00"],
    ["2025-12-31 23:59:00"],
    ["2026-01-01 00:02:00", None],
    ["2026-01-01 00:02:00", "bad"],
    ["2026-01-01T00:02:00Z"],
    ["2026-01-01 00:00:00.123456"],
    ["bad"],
    [],
])
def test_complete_information_loader_matches_legacy_overlap(module, tail_values, tmp_path):
    base = tmp_path / "TEST_features.parquet"
    pl.DataFrame({"date": ["2026-01-01 00:00:00", "2026-01-01 00:01:00"]}).write_parquet(
        base, statistics=True)
    tail = tmp_path / "_hot_tail" / base.name
    tail.parent.mkdir()
    pl.DataFrame({"date": pl.Series(tail_values, dtype=pl.String)}).write_parquet(tail, statistics=False)
    name = "_load_logical_existing_info" if module is binance else "_load_logical_existing_candle_info"
    loader = getattr(module, name)

    def legacy_overlap(frame, *, base_latest_ms, normalize):
        return reference(frame, normalize, base_ms=base_latest_ms)

    before = base.read_bytes(), tail.read_bytes()
    kwargs = {"require_contiguous": False} if module is bybit else {}
    try:
        with patch.object(module, "tail_newer_summary", legacy_overlap):
            expected = asdict(loader(base, **kwargs))
    except Exception as error:
        with pytest.raises(type(error)):
            loader(base, **kwargs)
    else:
        assert asdict(loader(base, **kwargs)) == expected
    assert (base.read_bytes(), tail.read_bytes()) == before


def test_benchmark_rejects_existing_output_without_source_reads(tmp_path):
    with pytest.raises(ValueError, match="fresh output"):
        benchmark(tmp_path, tmp_path)


def test_benchmark_rejects_source_overlap_before_creating_private_files(tmp_path, monkeypatch):
    from scripts import benchmark_cex_tail_overlap as helper

    monkeypatch.setattr(helper, "ROOT", tmp_path)
    root = tmp_path / "data_bybit" / "1m"
    root.mkdir(parents=True)
    output = root / "candidate"
    with pytest.raises(ValueError, match="overlap source"):
        benchmark(tmp_path, output)
    assert not output.exists()
