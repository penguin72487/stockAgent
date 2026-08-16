"""Compressed historical minute-K execution tape for the daily day-trade model.

The model still makes exactly one decision per exchange session.  This module
extracts only the causally later bars needed by the executor, so the resulting
tensor is an execution label and must never be appended to model features.
"""

from __future__ import annotations

from enum import IntEnum
from pathlib import Path
import hashlib
import os

import numpy as np

try:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - validated by the public loader
    pc = None
    pq = None


DAY_TRADE_MINUTE_EXECUTION_CONTRACT_VERSION = 1


class DayTradeExecutionField(IntEnum):
    OFFICIAL_OPEN = 0
    ENTRY_VWAP_0901 = 1
    ENTRY_VOLUME_0901 = 2
    LIMIT_PRICE_1320 = 3
    HIGH_1321 = 4
    LOW_1321 = 5
    VOLUME_1321 = 6
    HIGH_1322 = 7
    LOW_1322 = 8
    VOLUME_1322 = 9
    HIGH_1323 = 10
    LOW_1323 = 11
    VOLUME_1323 = 12
    MARKET_VWAP_1324 = 13
    MARKET_VOLUME_1324 = 14
    AUCTION_PRICE_1330 = 15
    AUCTION_VOLUME_1330 = 16


DAY_TRADE_EXECUTION_FIELD_COUNT = len(DayTradeExecutionField)
_EVENT_MINUTES = (1, 260, 261, 262, 263, 264, 270)


def _vwap(amount: float, volume_shares: float, fallback: float) -> float:
    if np.isfinite(amount) and np.isfinite(volume_shares) and volume_shares > 0.0:
        value = amount / volume_shares
        if np.isfinite(value) and value > 0.0:
            return float(value)
    return float(fallback) if np.isfinite(fallback) and fallback > 0.0 else np.nan


def load_tw_day_trade_execution_tape(
    root: str | Path,
    *,
    panel_dates: np.ndarray,
    panel_symbols: list[str],
    official_open_prices: np.ndarray,
    cache_dir: str | Path | None = None,
) -> np.ndarray:
    """Align the fixed daily execution events to a daily panel ``[T,S,C]``.

    Missing partitions, symbols, bars, prices, or volume remain fail-closed:
    prices are NaN and capacity is zero.  ``Amount / volume_shares`` is the
    observable minute VWAP used for historical market-order execution.
    """

    if pq is None or pc is None:
        raise RuntimeError("PyArrow is required for day-trade minute execution")
    dates = np.asarray(panel_dates, dtype="datetime64[D]").reshape(-1)
    opens = np.asarray(official_open_prices, dtype=np.float64)
    expected = (int(dates.size), len(panel_symbols))
    if opens.shape != expected:
        raise ValueError("official_open_prices must align with panel [T,S]")
    cache_path: Path | None = None
    if cache_dir is not None:
        digest = hashlib.sha256()
        digest.update(str(Path(root).resolve()).encode("utf-8"))
        manifest = Path(root) / "manifest.json"
        if manifest.is_file():
            digest.update(manifest.read_bytes())
        digest.update(dates.astype("datetime64[D]").astype(np.int64).tobytes())
        digest.update("\0".join(map(str, panel_symbols)).encode("utf-8"))
        digest.update(np.ascontiguousarray(opens, dtype=np.float32).tobytes())
        resolved_cache = Path(cache_dir)
        resolved_cache.mkdir(parents=True, exist_ok=True)
        cache_path = resolved_cache / f"tape-{digest.hexdigest()}.npy"
        if cache_path.is_file():
            cached = np.load(cache_path, allow_pickle=False)
            if cached.shape != (*expected, DAY_TRADE_EXECUTION_FIELD_COUNT):
                raise RuntimeError(f"invalid cached day-trade execution tape: {cache_path}")
            return np.asarray(cached, dtype=np.float32)
    tape = np.full((*expected, DAY_TRADE_EXECUTION_FIELD_COUNT), np.nan, dtype=np.float32)
    tape[:, :, DayTradeExecutionField.ENTRY_VOLUME_0901] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1321] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1322] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1323] = 0.0
    tape[:, :, DayTradeExecutionField.MARKET_VOLUME_1324] = 0.0
    tape[:, :, DayTradeExecutionField.AUCTION_VOLUME_1330] = 0.0
    tape[:, :, DayTradeExecutionField.OFFICIAL_OPEN] = opens.astype(np.float32)
    symbol_index = {str(symbol): idx for idx, symbol in enumerate(panel_symbols)}
    root_path = Path(root)
    columns = [
        "symbol", "minutes_from_open", "High", "Low", "Close", "Amount",
        "volume_shares",
    ]
    for date_idx, day in enumerate(dates):
        day_text = np.datetime_as_string(day, unit="D")
        path = root_path / f"trade_date={day_text}" / "data.parquet"
        if not path.is_file():
            continue
        table = pq.read_table(
            path,
            columns=columns,
            filters=[("minutes_from_open", "in", list(_EVENT_MINUTES))],
        )
        payload = table.to_pydict()
        for row in range(table.num_rows):
            sym_idx = symbol_index.get(str(payload["symbol"][row]))
            if sym_idx is None:
                continue
            minute = int(payload["minutes_from_open"][row])
            high = float(payload["High"][row])
            low = float(payload["Low"][row])
            close = float(payload["Close"][row])
            volume = float(payload["volume_shares"][row])
            amount = float(payload["Amount"][row])
            if minute == 1:
                tape[date_idx, sym_idx, DayTradeExecutionField.ENTRY_VWAP_0901] = _vwap(amount, volume, close)
                tape[date_idx, sym_idx, DayTradeExecutionField.ENTRY_VOLUME_0901] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 260:
                tape[date_idx, sym_idx, DayTradeExecutionField.LIMIT_PRICE_1320] = close
            elif minute in (261, 262, 263):
                base = {
                    261: DayTradeExecutionField.HIGH_1321,
                    262: DayTradeExecutionField.HIGH_1322,
                    263: DayTradeExecutionField.HIGH_1323,
                }[minute]
                tape[date_idx, sym_idx, base] = high
                tape[date_idx, sym_idx, int(base) + 1] = low
                tape[date_idx, sym_idx, int(base) + 2] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 264:
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VWAP_1324] = _vwap(amount, volume, close)
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VOLUME_1324] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 270:
                tape[date_idx, sym_idx, DayTradeExecutionField.AUCTION_PRICE_1330] = close
                tape[date_idx, sym_idx, DayTradeExecutionField.AUCTION_VOLUME_1330] = max(volume, 0.0) if np.isfinite(volume) else 0.0
    if cache_path is not None:
        temporary = cache_path.with_name(
            f".{cache_path.name}.{os.getpid()}.tmp"
        )
        with temporary.open("wb") as handle:
            np.save(handle, tape, allow_pickle=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, cache_path)
    return tape


__all__ = [
    "DAY_TRADE_EXECUTION_FIELD_COUNT",
    "DAY_TRADE_MINUTE_EXECUTION_CONTRACT_VERSION",
    "DayTradeExecutionField",
    "load_tw_day_trade_execution_tape",
]
