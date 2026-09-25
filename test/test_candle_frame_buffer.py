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
