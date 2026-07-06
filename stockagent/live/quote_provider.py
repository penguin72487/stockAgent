from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    """Fetch latest Yahoo prices from the quote API and align them to panel symbols."""
    yahoo_map = load_symbol_yahoo_map(parquet_root)
    tickers = [yahoo_map.get(symbol, symbol) for symbol in symbols]
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    count = 0
    last_timestamp_s: int | None = None
    chunk_len = max(1, int(chunk_size))
    chunks = [(start, tickers[start : start + chunk_len]) for start in range(0, len(tickers), chunk_len)]
    max_parallel = int(os.getenv("STOCKAGENT_YAHOO_PARALLEL_REQUESTS", "32") or "32")
    workers = max(1, min(len(chunks) or 1, max_parallel))

    def fetch_chunk(start: int, ticker_chunk: list[str]) -> list[tuple[int, float, int | None]]:
        encoded = quote(",".join(str(ticker) for ticker in ticker_chunk), safe=",")
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}"
        try:
            response = requests.get(
                url,
                timeout=8,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
                    )
                },
            )
            response.raise_for_status()
            payload = response.json()
            result_rows = payload.get("quoteResponse", {}).get("result") or []
        except Exception:
            return []

        local_index: dict[str, list[int]] = {}
        for offset, ticker in enumerate(ticker_chunk):
            local_index.setdefault(str(ticker), []).append(start + offset)
        rows: list[tuple[int, float, int | None]] = []
        for item in result_rows:
            ticker = str(item.get("symbol") or "").strip()
            indices = local_index.get(ticker)
            if not indices:
                continue
            raw_value = (
                item.get("regularMarketPrice")
                or item.get("postMarketPrice")
                or item.get("preMarketPrice")
                or item.get("bid")
                or item.get("ask")
            )
            try:
                value = float(raw_value)
            except Exception:
                continue
            if not (np.isfinite(value) and value > 0.0):
                continue
            raw_time = item.get("regularMarketTime") or item.get("postMarketTime") or item.get("preMarketTime")
            try:
                timestamp_s = int(raw_time)
            except Exception:
                timestamp_s = None
            for index in indices:
                rows.append((index, value, timestamp_s))
        return rows

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="yahoo-quote") as executor:
        futures = [executor.submit(fetch_chunk, start, ticker_chunk) for start, ticker_chunk in chunks]
        for future in as_completed(futures):
            for idx, value, timestamp_s in future.result():
                prices[idx] = value
                count += 1
                if timestamp_s is not None and (last_timestamp_s is None or timestamp_s > last_timestamp_s):
                    last_timestamp_s = timestamp_s

    last_timestamp = (
        datetime.fromtimestamp(last_timestamp_s, tz=timezone.utc).isoformat() if last_timestamp_s is not None else None
    )
    return PriceSnapshot(prices=prices, source=f"yahoo:quote", timestamp=last_timestamp, available_count=count)
