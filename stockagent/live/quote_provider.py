from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np


@dataclass(slots=True)
class PriceSnapshot:
    prices: np.ndarray
    source: str
    timestamp: str | None = None
    available_count: int = 0


def load_symbol_yahoo_map(parquet_root: str | Path) -> dict[str, str]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.exists():
        return {}
    try:
        import csv

        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = csv.DictReader(handle)
            return {
                str(row.get("code", "")).strip(): str(row.get("yahoo_symbol", "")).strip()
                for row in rows
                if str(row.get("code", "")).strip() and str(row.get("yahoo_symbol", "")).strip()
            }
    except Exception:
        return {}


def load_symbol_name_map(parquet_root: str | Path) -> dict[str, str]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.exists():
        return {}
    try:
        import csv

        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = csv.DictReader(handle)
            return {
                str(row.get("code", "")).strip(): str(row.get("name", "")).strip()
                for row in rows
                if str(row.get("code", "")).strip() and str(row.get("name", "")).strip()
            }
    except Exception:
        return {}


def load_prices_csv(path: str | Path, symbols: list[str], fallback_prices: np.ndarray) -> PriceSnapshot:
    import polars as pl

    frame = pl.read_csv(path)
    columns = {name.lower(): name for name in frame.columns}
    symbol_col = columns.get("symbol") or columns.get("code") or columns.get("ticker")
    price_col = columns.get("price") or columns.get("close") or columns.get("last") or columns.get("current_price")
    if symbol_col is None or price_col is None:
        raise ValueError("prices CSV must contain symbol/code/ticker and price/close/last/current_price columns")

    lookup = {
        str(row[symbol_col]).strip(): float(row[price_col])
        for row in frame.select([symbol_col, price_col]).iter_rows(named=True)
        if str(row[symbol_col]).strip()
    }
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    count = 0
    for idx, symbol in enumerate(symbols):
        value = lookup.get(str(symbol))
        if value is None or not np.isfinite(value) or value <= 0.0:
            continue
        prices[idx] = value
        count += 1
    return PriceSnapshot(prices=prices, source=f"csv:{Path(path)}", available_count=count)


def fetch_yahoo_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
    period: str = "1d",
    interval: str = "1m",
) -> PriceSnapshot:
    """Fetch latest Yahoo prices with yfinance and align them to panel symbols."""
    import pandas as pd
    import yfinance as yf

    yahoo_map = load_symbol_yahoo_map(parquet_root)
    tickers = [yahoo_map.get(symbol, symbol) for symbol in symbols]
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    count = 0
    last_timestamp: str | None = None

    for start in range(0, len(symbols), max(1, int(chunk_size))):
        sym_chunk = symbols[start : start + max(1, int(chunk_size))]
        ticker_chunk = tickers[start : start + max(1, int(chunk_size))]
        data = yf.download(
            tickers=" ".join(ticker_chunk),
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
        if data is None or len(data) == 0:
            continue
        if isinstance(data.columns, pd.MultiIndex):
            for offset, ticker in enumerate(ticker_chunk):
                try:
                    close = data[(ticker, "Close")].dropna()
                except Exception:
                    continue
                if close.empty:
                    continue
                value = float(close.iloc[-1])
                if np.isfinite(value) and value > 0.0:
                    prices[start + offset] = value
                    count += 1
                    last_timestamp = str(close.index[-1])
        else:
            close = data.get("Close")
            if close is None:
                continue
            close = close.dropna()
            if close.empty:
                continue
            value = float(close.iloc[-1])
            if np.isfinite(value) and value > 0.0 and sym_chunk:
                prices[start] = value
                count += 1
                last_timestamp = str(close.index[-1])

    return PriceSnapshot(prices=prices, source=f"yahoo:{period}/{interval}", timestamp=last_timestamp, available_count=count)
