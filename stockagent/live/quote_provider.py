from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import numpy as np
import requests


_YAHOO_SESSION_LOCK = threading.Lock()
_YAHOO_SESSION: requests.Session | None = None
_YAHOO_CRUMB: str | None = None
_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    )
}


@dataclass(slots=True)
class PriceSnapshot:
    prices: np.ndarray
    source: str
    timestamp: str | None = None
    available_count: int = 0
    available_mask: np.ndarray | None = None
    open_prices: np.ndarray | None = None
    high_prices: np.ndarray | None = None
    low_prices: np.ndarray | None = None
    volumes: np.ndarray | None = None
    upper_limit_prices: np.ndarray | None = None
    lower_limit_prices: np.ndarray | None = None


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
    open_col = columns.get("open_price") or columns.get("open")
    high_col = columns.get("high_price") or columns.get("high")
    low_col = columns.get("low_price") or columns.get("low")
    volume_col = columns.get("volume") or columns.get("trading_volume")
    upper_col = columns.get("upper_limit_price") or columns.get("upper_limit")
    lower_col = columns.get("lower_limit_price") or columns.get("lower_limit")
    if symbol_col is None or price_col is None:
        raise ValueError("prices CSV must contain symbol/code/ticker and price/close/last/current_price columns")

    value_columns = [
        column
        for column in (
            symbol_col,
            price_col,
            open_col,
            high_col,
            low_col,
            volume_col,
            upper_col,
            lower_col,
        )
        if column is not None
    ]
    lookup = {
        str(row[symbol_col]).strip(): row
        for row in frame.select(value_columns).iter_rows(named=True)
        if str(row[symbol_col]).strip()
    }
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    available = np.zeros((len(symbols),), dtype=bool)
    open_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    high_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    low_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    upper_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    lower_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    count = 0
    for idx, symbol in enumerate(symbols):
        row = lookup.get(str(symbol))
        if row is None:
            continue
        value = _float_or_none(row.get(price_col))
        if value is None:
            continue
        prices[idx] = value
        available[idx] = True
        count += 1
        for column, target in (
            (open_col, open_prices),
            (high_col, high_prices),
            (low_col, low_prices),
            (volume_col, volumes),
            (upper_col, upper_limit_prices),
            (lower_col, lower_limit_prices),
        ):
            if column is None:
                continue
            observed = _float_or_none(row.get(column))
            if observed is not None:
                target[idx] = observed
    return PriceSnapshot(
        prices=prices,
        source=f"csv:{Path(path)}",
        available_count=count,
        available_mask=available,
        open_prices=open_prices,
        high_prices=high_prices,
        low_prices=low_prices,
        volumes=volumes,
        upper_limit_prices=upper_limit_prices,
        lower_limit_prices=lower_limit_prices,
    )


def _float_or_none(value: object) -> float | None:
    try:
        text = str(value).strip()
        if not text or text in {"-", "--", "null", "None"}:
            return None
        parsed = float(text.replace(",", ""))
    except Exception:
        return None
    if not (np.isfinite(parsed) and parsed > 0.0):
        return None
    return parsed


def _first_book_price(value: object) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    for part in text.split("_"):
        parsed = _float_or_none(part)
        if parsed is not None:
            return parsed
    return None


def _tw_mis_candidates(symbol: str, yahoo_symbol: str | None) -> list[str]:
    code = str(symbol).strip()
    if not code:
        return []
    raw_yahoo = str(yahoo_symbol or "").strip()
    markets: list[str] = []
    for item in raw_yahoo.split(","):
        ticker = item.strip().upper()
        if ticker.endswith(".TW") and "tse" not in markets:
            markets.append("tse")
        elif ticker.endswith(".TWO") and "otc" not in markets:
            markets.append("otc")
    if not markets:
        markets = ["tse", "otc"]
    return [f"{market}_{code}.tw" for market in markets]


def _tw_mis_price(row: dict) -> float | None:
    for key in ("z", "pz"):
        value = _float_or_none(row.get(key))
        if value is not None:
            return value
    bid = _first_book_price(row.get("b"))
    ask = _first_book_price(row.get("a"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid or ask or _float_or_none(row.get("y"))


def fetch_tw_mis_last_prices(
    symbols: list[str],
    fallback_prices: np.ndarray,
    *,
    parquet_root: str | Path,
    chunk_size: int = 80,
) -> PriceSnapshot:
    """Fetch Taiwan intraday prices from TWSE MIS and align them to panel symbols."""
    yahoo_map = load_symbol_yahoo_map(parquet_root)
    prices = np.asarray(fallback_prices, dtype=np.float64).copy()
    filled = np.zeros((len(symbols),), dtype=bool)
    open_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    high_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    low_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    volumes = np.full((len(symbols),), np.nan, dtype=np.float64)
    upper_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    lower_limit_prices = np.full((len(symbols),), np.nan, dtype=np.float64)
    last_timestamp_ms: int | None = None

    ex_channels: list[tuple[int, str]] = []
    for idx, symbol in enumerate(symbols):
        for ex_ch in _tw_mis_candidates(str(symbol), yahoo_map.get(str(symbol))):
            ex_channels.append((idx, ex_ch))
    # MIS silently returns partial/empty payloads when ex_ch grows too large.
    # Keep the public caller knob for smaller probes, but never exceed the
    # stable endpoint batch used by the full-universe fetcher.
    chunk_len = max(1, min(int(chunk_size), 80))
    chunks = [ex_channels[start : start + chunk_len] for start in range(0, len(ex_channels), chunk_len)]
    max_parallel = int(os.getenv("STOCKAGENT_TW_MIS_PARALLEL_REQUESTS", "4") or "4")
    workers = max(1, min(len(chunks) or 1, max_parallel))
    retry_attempts = max(
        0,
        int(os.getenv("STOCKAGENT_TW_MIS_RETRY_ATTEMPTS", "3") or "3"),
    )
    retry_delay_seconds = max(
        0.0,
        float(os.getenv("STOCKAGENT_TW_MIS_RETRY_DELAY_SECONDS", "0.35") or "0.35"),
    )
    session_local = threading.local()

    def session() -> requests.Session:
        sess = getattr(session_local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update(_YAHOO_HEADERS)
            try:
                sess.get("https://mis.twse.com.tw/stock/index.jsp", timeout=8)
            except Exception:
                pass
            session_local.session = sess
        return sess

    def fetch_chunk(
        items: list[tuple[int, str]],
    ) -> list[tuple[int, float, int | None, float | None, float | None, float | None, float | None, float | None, float | None]]:
        if not items:
            return []
        ex_ch = "|".join(ex for _, ex in items)
        code_to_indices: dict[str, list[int]] = {}
        for idx, _ex in items:
            code_to_indices.setdefault(str(symbols[idx]), []).append(idx)
        try:
            response = session().get(
                "https://mis.twse.com.tw/stock/api/getStockInfo.jsp",
                params={"ex_ch": ex_ch, "json": "1", "delay": "0", "_": str(int(datetime.now().timestamp() * 1000))},
                headers={"Referer": "https://mis.twse.com.tw/stock/index.jsp", **_YAHOO_HEADERS},
                timeout=8,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return []
        rows: list[
            tuple[
                int,
                float,
                int | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ] = []
        for item in payload.get("msgArray") or []:
            code = str(item.get("c") or "").strip()
            if not code:
                continue
            value = _tw_mis_price(item)
            if value is None:
                continue
            try:
                timestamp_ms = int(item.get("tlong") or 0) or None
            except Exception:
                timestamp_ms = None
            for idx in code_to_indices.get(code, []):
                rows.append(
                    (
                        idx,
                        value,
                        timestamp_ms,
                        _float_or_none(item.get("o")),
                        _float_or_none(item.get("h")),
                        _float_or_none(item.get("l")),
                        _float_or_none(item.get("v")),
                        _float_or_none(item.get("u")),
                        _float_or_none(item.get("w")),
                    )
                )
        return rows

    chunk_results: list[
        list[
            tuple[
                int,
                float,
                int | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
                float | None,
            ]
        ]
    ] = [[] for _ in chunks]
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tw-mis-quote") as executor:
        futures = {
            executor.submit(fetch_chunk, chunk): chunk_index
            for chunk_index, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_results[futures[future]] = future.result()

    # MIS intermittently closes full-universe connections. Empty chunks are not
    # usable evidence that no symbol traded, so retry only those chunks in a
    # paced single-thread pass. Never substitute the intraday last price for a
    # missing opening price; callers receive NaN in open_prices and fail closed.
    for chunk_index, rows in enumerate(chunk_results):
        if rows or retry_attempts <= 0:
            continue
        for attempt in range(retry_attempts):
            if retry_delay_seconds > 0.0:
                time.sleep(retry_delay_seconds * (attempt + 1))
            rows = fetch_chunk(chunks[chunk_index])
            if rows:
                chunk_results[chunk_index] = rows
                break

    for rows in chunk_results:
        for idx, value, timestamp_ms, open_px, high_px, low_px, volume, upper_px, lower_px in rows:
            prices[idx] = value
            filled[idx] = True
            for target, observed in (
                (open_prices, open_px),
                (high_prices, high_px),
                (low_prices, low_px),
                (volumes, volume),
                (upper_limit_prices, upper_px),
                (lower_limit_prices, lower_px),
            ):
                if observed is not None:
                    target[idx] = observed
            if timestamp_ms is not None and (last_timestamp_ms is None or timestamp_ms > last_timestamp_ms):
                last_timestamp_ms = timestamp_ms

    timestamp = None
    if last_timestamp_ms is not None:
        timestamp = datetime.fromtimestamp(last_timestamp_ms / 1000.0, tz=timezone.utc).isoformat()
    return PriceSnapshot(
        prices=prices,
        source="twse_tpex:mis",
        timestamp=timestamp,
        available_count=int(filled.sum()),
        available_mask=filled,
        open_prices=open_prices,
        high_prices=high_prices,
        low_prices=low_prices,
        volumes=volumes,
        upper_limit_prices=upper_limit_prices,
        lower_limit_prices=lower_limit_prices,
    )


def _yahoo_session_and_crumb() -> tuple[requests.Session, str | None]:
    global _YAHOO_SESSION, _YAHOO_CRUMB
    with _YAHOO_SESSION_LOCK:
        if _YAHOO_SESSION is None:
            _YAHOO_SESSION = requests.Session()
        if not _YAHOO_CRUMB:
            try:
                _YAHOO_SESSION.get("https://fc.yahoo.com", timeout=8, headers=_YAHOO_HEADERS)
                response = _YAHOO_SESSION.get(
                    "https://query1.finance.yahoo.com/v1/test/getcrumb",
                    timeout=8,
                    headers=_YAHOO_HEADERS,
                )
                response.raise_for_status()
                crumb = response.text.strip()
                if crumb and "Too Many Requests" not in crumb:
                    _YAHOO_CRUMB = crumb
            except Exception:
                _YAHOO_CRUMB = None
        return _YAHOO_SESSION, _YAHOO_CRUMB


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
    filled = np.zeros((len(symbols),), dtype=bool)
    last_timestamp_s: int | None = None
    chunk_len = max(1, int(chunk_size))
    chunks = [(start, tickers[start : start + chunk_len]) for start in range(0, len(tickers), chunk_len)]
    max_parallel = int(os.getenv("STOCKAGENT_YAHOO_PARALLEL_REQUESTS", "32") or "32")
    workers = max(1, min(len(chunks) or 1, max_parallel))

    def fetch_chunk(
        start: int,
        ticker_chunk: list[str],
        *,
        session: requests.Session | None = None,
        crumb: str | None = None,
    ) -> list[tuple[int, float, int | None]]:
        encoded = quote(",".join(str(ticker) for ticker in ticker_chunk), safe=",")
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={encoded}"
        params = {"crumb": crumb} if crumb else None
        request_get = session.get if session is not None else requests.get
        try:
            response = request_get(
                url,
                params=params,
                timeout=8,
                headers=_YAHOO_HEADERS,
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

    def run_quote_pass(*, session: requests.Session | None = None, crumb: str | None = None) -> None:
        nonlocal last_timestamp_s
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="yahoo-quote") as executor:
            futures = [
                executor.submit(fetch_chunk, start, ticker_chunk, session=session, crumb=crumb)
                for start, ticker_chunk in chunks
            ]
            for future in as_completed(futures):
                for idx, value, timestamp_s in future.result():
                    prices[idx] = value
                    filled[idx] = True
                    if timestamp_s is not None and (last_timestamp_s is None or timestamp_s > last_timestamp_s):
                        last_timestamp_s = timestamp_s

    run_quote_pass()
    quote_count = int(filled.sum())
    min_quote_retry_count = min(len(symbols), max(1, int(len(symbols) * 0.8)))
    used_crumb = False
    if quote_count < min_quote_retry_count:
        session, crumb = _yahoo_session_and_crumb()
        if crumb:
            run_quote_pass(session=session, crumb=crumb)
            used_crumb = True

    used_chart = False
    if (
        not str(os.getenv("STOCKAGENT_YAHOO_CHART_FALLBACK", "1") or "1").strip().lower()
        in {"0", "false", "no", "off"}
        and not bool(filled.all())
    ):
        missing = [idx for idx, ok in enumerate(filled) if not ok]
        fallback_cap = int(os.getenv("STOCKAGENT_YAHOO_CHART_FALLBACK_MAX_SYMBOLS", "200") or "200")
        if fallback_cap >= 0:
            missing = missing[:fallback_cap]

        def fetch_chart(idx: int) -> tuple[int, float, int | None] | None:
            ticker = str(tickers[idx]).strip()
            if not ticker:
                return None
            encoded = quote(ticker, safe="")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}?range={period}&interval={interval}"
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
                result_rows = payload.get("chart", {}).get("result") or []
                if not result_rows:
                    return None
                row = result_rows[0]
                meta = row.get("meta") or {}
                raw_value = meta.get("regularMarketPrice") or meta.get("chartPreviousClose")
                raw_time = meta.get("regularMarketTime")
                indicators = row.get("indicators", {}).get("quote") or []
                timestamps = row.get("timestamp") or []
                if indicators:
                    closes = indicators[0].get("close") or []
                    for pos in range(len(closes) - 1, -1, -1):
                        try:
                            close_value = float(closes[pos])
                        except Exception:
                            continue
                        if np.isfinite(close_value) and close_value > 0.0:
                            raw_value = close_value
                            if pos < len(timestamps):
                                raw_time = timestamps[pos]
                            break
                value = float(raw_value)
                if not (np.isfinite(value) and value > 0.0):
                    return None
                try:
                    timestamp_s = int(raw_time)
                except Exception:
                    timestamp_s = None
                return idx, value, timestamp_s
            except Exception:
                return None

        chart_workers = max(1, min(len(missing) or 1, max_parallel))
        with ThreadPoolExecutor(max_workers=chart_workers, thread_name_prefix="yahoo-chart") as executor:
            futures = [executor.submit(fetch_chart, idx) for idx in missing]
            for future in as_completed(futures):
                result = future.result()
                if result is None:
                    continue
                idx, value, timestamp_s = result
                prices[idx] = value
                filled[idx] = True
                used_chart = True
                if timestamp_s is not None and (last_timestamp_s is None or timestamp_s > last_timestamp_s):
                    last_timestamp_s = timestamp_s

    last_timestamp = (
        datetime.fromtimestamp(last_timestamp_s, tz=timezone.utc).isoformat() if last_timestamp_s is not None else None
    )
    source_parts = ["yahoo:quote"]
    if used_crumb:
        source_parts.append("crumb")
    if used_chart:
        source_parts.append("chart")
    source = "+".join(source_parts)
    return PriceSnapshot(prices=prices, source=source, timestamp=last_timestamp, available_count=int(filled.sum()))
