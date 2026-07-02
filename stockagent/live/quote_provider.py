from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import numpy as np
import requests


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
    """Fetch latest Yahoo prices from the chart API and align them to panel symbols."""
    yahoo_map = load_symbol_yahoo_map(parquet_root)
    tickers = [yahoo_map.get(symbol, symbol) for symbol in symbols]
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    count = 0
    last_timestamp: str | None = None

    for start in range(0, len(symbols), max(1, int(chunk_size))):
        ticker_chunk = tickers[start : start + max(1, int(chunk_size))]
        for offset, ticker in enumerate(ticker_chunk):
            encoded = quote(str(ticker), safe="")
            url = (
                f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
                f"?range={period}&interval={interval}&includePrePost=false"
            )
            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                payload = response.json()
                result = (payload.get("chart", {}).get("result") or [None])[0]
                if not result:
                    continue
                timestamps = list(result.get("timestamp") or [])
                quote_rows = result.get("indicators", {}).get("quote") or []
                close_values = list((quote_rows[0] if quote_rows else {}).get("close") or [])
            except Exception:
                continue

            for idx in range(min(len(timestamps), len(close_values)) - 1, -1, -1):
                value = float(close_values[idx]) if close_values[idx] is not None else float("nan")
                if not (np.isfinite(value) and value > 0.0):
                    continue
                prices[start + offset] = value
                count += 1
                try:
                    last_timestamp = datetime.fromtimestamp(
                        int(timestamps[idx]),
                        tz=timezone.utc,
                    ).isoformat()
                except Exception:
                    last_timestamp = str(timestamps[idx])
                break

    return PriceSnapshot(prices=prices, source=f"yahoo:{period}/{interval}", timestamp=last_timestamp, available_count=count)
