from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
import pytest

from downloader.candle_frame_buffer import CandleFrameBuffer
from downloader import download_binance_perp_15m as binance
from downloader import download_bybit_perp_daily as bybit
from downloader import download_okx_perp_daily as okx


_START_MS = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


def _binance_row(ts: int) -> list[object]:
    return [ts, "100", "102", "99", "101", "12", ts + 59_999, "1212", 2, "6", "606"]


def _bybit_row(ts: int) -> list[str]:
    return [str(ts), "100", "102", "99", "101", "12", "1212"]


def _okx_row(ts: int) -> list[str]:
    return [str(ts), "100", "102", "99", "101", "12", "12", "1212", "1"]


@pytest.mark.parametrize(
    ("normalize", "make_row"),
    [
        (binance._normalize_candles, _binance_row),
        (bybit._normalize_candles, _bybit_row),
        (okx._normalize_candles, _okx_row),
    ],
)
def test_bounded_candle_buffer_preserves_provider_output_across_batches(
    normalize, make_row
) -> None:
    rows = [make_row(_START_MS + minute * 60_000) for minute in range(9)]
    expected = normalize(rows)
    buffer = CandleFrameBuffer(normalize, max_pending_rows=3)
    buffer.extend(rows[:4])
    assert len(buffer._pending) <= 3
    buffer.extend(rows[4:])

    actual = buffer.finish()

    assert buffer._frames == []
    assert actual.columns == expected.columns
    assert actual.to_dicts() == expected.to_dicts()


def test_bounded_candle_buffer_deduplicates_across_batch_boundaries() -> None:
    first = _binance_row(_START_MS)
    last = _binance_row(_START_MS)
    last[4] = "101.5"
    buffer = CandleFrameBuffer(binance._normalize_candles, max_pending_rows=1)
    buffer.extend([first, last])

    actual = buffer.finish()

    assert actual.height == 1
    assert actual["close"].item() == 101.5


def test_bounded_candle_buffer_empty_schema_and_invalid_limit() -> None:
    with pytest.raises(ValueError, match="positive"):
        CandleFrameBuffer(bybit._normalize_candles, max_pending_rows=0)
    assert isinstance(CandleFrameBuffer(bybit._normalize_candles).finish(), pl.DataFrame)


@pytest.mark.parametrize("make_row,normalize", [
    (_binance_row, binance._normalize_candles),
    (_bybit_row, bybit._normalize_candles),
    (_okx_row, okx._normalize_candles),
])
def test_spilled_candles_preserve_reverse_pages_revisions_and_cleanup(tmp_path, make_row, normalize):
    rows = [make_row(_START_MS + minute * 60_000) for minute in range(11)]
    rows = rows[6:] + rows[:6] + [make_row(_START_MS)]
    rows[-1][4] = "101.5"
    expected = normalize(rows)
    buffer = CandleFrameBuffer(normalize, max_pending_rows=3, max_resident_bytes=1, scratch_dir=tmp_path)
    buffer.extend(rows)
    assert buffer.resident_columnar_bytes == 0
    assert buffer.spilled_parts == 4
    actual = buffer.finish()
    assert actual.equals(expected)
    assert actual.schema == expected.schema
    assert not buffer.used_disjoint_fast_path
    assert not list(tmp_path.iterdir())
    assert buffer.finish().is_empty()


def test_disjoint_reverse_pages_use_linear_concat_without_global_sort(tmp_path, monkeypatch):
    buffer = CandleFrameBuffer(binance._normalize_candles, max_pending_rows=2,
                               max_resident_bytes=1, scratch_dir=tmp_path)
    rows = [_binance_row(_START_MS + minute * 60_000) for minute in (6, 7, 4, 5, 2, 3, 0, 1)]
    expected = binance._normalize_candles(rows)
    buffer.extend(rows)
    def forbidden(*args, **kwargs):
        raise AssertionError("disjoint batches need no global sort/hash")
    monkeypatch.setattr(pl.DataFrame, "unique", forbidden)
    monkeypatch.setattr(pl.DataFrame, "sort", forbidden)
    assert buffer.finish().equals(expected)
    assert buffer.used_disjoint_fast_path
    assert not list(tmp_path.iterdir())


def test_spill_cleanup_on_normalizer_failure(tmp_path):
    buffer = CandleFrameBuffer(binance._normalize_candles, max_pending_rows=2,
                               max_resident_bytes=1, scratch_dir=tmp_path)
    buffer.extend([_binance_row(_START_MS), _binance_row(_START_MS + 60_000)])
    buffer.extend([_binance_row(_START_MS + 120_000)])
    def fail(rows):
        raise ValueError("source parse failed")
    buffer._normalize = fail
    with pytest.raises(ValueError, match="source parse failed"):
        buffer.finish()
    assert not list(tmp_path.iterdir())


def test_invalid_resident_budget():
    with pytest.raises(ValueError, match="max_resident_bytes"):
        CandleFrameBuffer(bybit._normalize_candles, max_resident_bytes=0)


def test_spill_failure_during_extend_reclaims_scratch_and_does_not_keep_partial_prefix(tmp_path, monkeypatch):
    buffer = CandleFrameBuffer(binance._normalize_candles, max_pending_rows=2,
                               max_resident_bytes=1, scratch_dir=tmp_path)
    def unavailable(*args, **kwargs):
        raise OSError("scratch is unavailable")
    monkeypatch.setattr(pl.DataFrame, "write_ipc", unavailable)
    with pytest.raises(OSError, match="scratch"):
        buffer.extend([_binance_row(_START_MS), _binance_row(_START_MS + 60_000)])
    assert not list(tmp_path.iterdir())
    assert not buffer._pending
    assert not buffer._frames
