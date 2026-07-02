#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
import math
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests


PRICE_COLUMNS = ("open", "max", "min", "close", "adjclose")
OHLC_COLUMNS = ("open", "max", "min", "close")
VOLUME_COLUMN = "Trading_Volume"
FINMIND_URL = "https://api.finmindtrade.com/api/v4/data"
TWSE_STOCK_DAY_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"
TPEX_DAILY_QUOTES_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; stockAgent-data-audit/1.0)"}


@dataclass(frozen=True)
class ReturnEvent:
    symbol: str
    event_date: str
    next_date: str
    log_return: float
    simple_return: float
    current_close: float
    current_adjclose: float
    next_close: float
    next_adjclose: float
    current_volume: float
    next_volume: float
    current_dividends: float
    current_splits: float
    next_dividends: float
    next_splits: float


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _finite_positive(value: Any) -> bool:
    value_f = _safe_float(value)
    return bool(np.isfinite(value_f) and value_f > 0.0)


def _date_key(value: Any) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return pd.Timestamp(value).strftime("%Y-%m-%d")


def _parse_date_key(value: str) -> date:
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _parse_roc_date_key(value: Any) -> str:
    text = str(value or "").strip()
    parts = text.split("/")
    if len(parts) != 3:
        return ""
    try:
        year = int(parts[0]) + 1911
        month = int(parts[1])
        day = int(parts[2])
        return date(year, month, day).isoformat()
    except Exception:
        return ""


def _expand_date(value: str, days: int) -> str:
    return (_parse_date_key(value) + timedelta(days=int(days))).isoformat()


def _read_parquet(path: Path) -> tuple[pd.DataFrame, dict[bytes, bytes] | None]:
    table = pq.read_table(path)
    frame = table.to_pandas()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame, table.schema.metadata


def _write_parquet(frame: pd.DataFrame, path: Path, metadata: dict[bytes, bytes] | None) -> None:
    out = pa.Table.from_pandas(frame, preserve_index=False)
    if metadata:
        out = out.replace_schema_metadata(metadata)
    pq.write_table(out, path)


def _fetch_finmind(dataset: str, symbol: str, start_date: str, end_date: str, *, timeout_s: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params = {
        "dataset": dataset,
        "data_id": symbol,
        "start_date": start_date,
        "end_date": end_date,
    }
    response = requests.get(FINMIND_URL, params=params, headers=HTTP_HEADERS, timeout=timeout_s)
    info = {
        "dataset": dataset,
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "url": response.url,
        "status_code": response.status_code,
    }
    response.raise_for_status()
    payload = response.json()
    info["payload_status"] = payload.get("status")
    info["payload_msg"] = payload.get("msg")
    data = payload.get("data") or []
    info["rows"] = len(data)
    return list(data), info


def _parse_market_number(value: Any) -> float:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "--", "---", "----", "除息", "除權"}:
        return float("nan")
    try:
        return float(text)
    except Exception:
        return float("nan")


def _month_keys_for_intervals(intervals: list[tuple[str, str]]) -> list[date]:
    months: set[date] = set()
    for start_key, end_key in intervals:
        current = _parse_date_key(start_key).replace(day=1)
        end = _parse_date_key(end_key).replace(day=1)
        while current <= end:
            months.add(current)
            year = current.year + int(current.month == 12)
            month = 1 if current.month == 12 else current.month + 1
            current = date(year, month, 1)
    return sorted(months)


def _local_dates_for_intervals(frame: pd.DataFrame, intervals: list[tuple[str, str]]) -> list[str]:
    if "date" not in frame.columns:
        return []
    dates = []
    for value in frame["date"]:
        key = _date_key(value)
        if _date_in_intervals(key, intervals):
            dates.append(key)
    return sorted(set(dates))


def _source_row(
    *,
    date_key: str,
    open_px: float,
    high_px: float,
    low_px: float,
    close_px: float,
    volume: float,
    source: str,
    source_url: str,
) -> dict[str, Any]:
    return {
        "date": date_key,
        "open": open_px,
        "max": high_px,
        "min": low_px,
        "close": close_px,
        "Trading_Volume": volume,
        "_source": source,
        "_source_url": source_url,
    }


def _fetch_twse_stock_month(symbol: str, month: date, *, timeout_s: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    query_date = month.strftime("%Y%m%d")
    params = {"date": query_date, "stockNo": symbol, "response": "json"}
    response = requests.get(TWSE_STOCK_DAY_URL, params=params, headers=HTTP_HEADERS, timeout=timeout_s)
    info = {
        "dataset": "TWSE_STOCK_DAY",
        "symbol": symbol,
        "start_date": month.isoformat(),
        "end_date": date(month.year, month.month, calendar.monthrange(month.year, month.month)[1]).isoformat(),
        "url": response.url,
        "status_code": response.status_code,
    }
    response.raise_for_status()
    payload = response.json()
    info["payload_status"] = payload.get("stat")
    info["payload_msg"] = payload.get("title", "")
    fields = list(payload.get("fields") or [])
    rows = list(payload.get("data") or [])
    info["rows"] = len(rows)
    idx = {name: i for i, name in enumerate(fields)}
    parsed: list[dict[str, Any]] = []
    for row in rows:
        key = _parse_roc_date_key(row[idx["日期"]]) if "日期" in idx and idx["日期"] < len(row) else ""
        if not key:
            continue
        parsed.append(
            _source_row(
                date_key=key,
                open_px=_parse_market_number(row[idx["開盤價"]]) if "開盤價" in idx and idx["開盤價"] < len(row) else float("nan"),
                high_px=_parse_market_number(row[idx["最高價"]]) if "最高價" in idx and idx["最高價"] < len(row) else float("nan"),
                low_px=_parse_market_number(row[idx["最低價"]]) if "最低價" in idx and idx["最低價"] < len(row) else float("nan"),
                close_px=_parse_market_number(row[idx["收盤價"]]) if "收盤價" in idx and idx["收盤價"] < len(row) else float("nan"),
                volume=_parse_market_number(row[idx["成交股數"]]) if "成交股數" in idx and idx["成交股數"] < len(row) else float("nan"),
                source="TWSE STOCK_DAY",
                source_url=response.url,
            )
        )
    return parsed, info


def _fetch_tpex_daily_quote(symbol: str, date_key: str, *, timeout_s: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    d = _parse_date_key(date_key)
    params = {"date": d.strftime("%Y/%m/%d"), "id": symbol, "response": "json"}
    response = requests.get(TPEX_DAILY_QUOTES_URL, params=params, headers=HTTP_HEADERS, timeout=timeout_s)
    info = {
        "dataset": "TPEX_DAILY_QUOTES",
        "symbol": symbol,
        "start_date": date_key,
        "end_date": date_key,
        "url": response.url,
        "status_code": response.status_code,
    }
    response.raise_for_status()
    payload = response.json()
    info["payload_status"] = payload.get("stat")
    info["payload_msg"] = ""
    parsed: list[dict[str, Any]] = []
    for table in payload.get("tables") or []:
        fields = list(table.get("fields") or [])
        idx = {name: i for i, name in enumerate(fields)}
        for row in table.get("data") or []:
            code = str(row[idx["代號"]]).strip() if "代號" in idx and idx["代號"] < len(row) else ""
            if code != symbol:
                continue
            parsed.append(
                _source_row(
                    date_key=date_key,
                    open_px=_parse_market_number(row[idx["開盤"]]) if "開盤" in idx and idx["開盤"] < len(row) else float("nan"),
                    high_px=_parse_market_number(row[idx["最高"]]) if "最高" in idx and idx["最高"] < len(row) else float("nan"),
                    low_px=_parse_market_number(row[idx["最低"]]) if "最低" in idx and idx["最低"] < len(row) else float("nan"),
                    close_px=_parse_market_number(row[idx["收盤"]]) if "收盤" in idx and idx["收盤"] < len(row) else float("nan"),
                    volume=_parse_market_number(row[idx["成交股數"]]) if "成交股數" in idx and idx["成交股數"] < len(row) else float("nan"),
                    source="TPEX dailyQuotes",
                    source_url=response.url,
                )
            )
    info["rows"] = len(parsed)
    return parsed, info


def _fetch_tpex_daily_quotes_all(date_key: str, *, timeout_s: int) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    d = _parse_date_key(date_key)
    params = {"date": d.strftime("%Y/%m/%d"), "response": "json"}
    response = requests.get(TPEX_DAILY_QUOTES_URL, params=params, headers=HTTP_HEADERS, timeout=timeout_s)
    info = {
        "dataset": "TPEX_DAILY_QUOTES",
        "symbol": "*",
        "start_date": date_key,
        "end_date": date_key,
        "url": response.url,
        "status_code": response.status_code,
    }
    response.raise_for_status()
    payload = response.json()
    info["payload_status"] = payload.get("stat")
    info["payload_msg"] = ""
    parsed: dict[str, dict[str, Any]] = {}
    for table in payload.get("tables") or []:
        fields = list(table.get("fields") or [])
        idx = {name: i for i, name in enumerate(fields)}
        for row in table.get("data") or []:
            code = str(row[idx["代號"]]).strip() if "代號" in idx and idx["代號"] < len(row) else ""
            if not code:
                continue
            parsed[code] = _source_row(
                date_key=date_key,
                open_px=_parse_market_number(row[idx["開盤"]]) if "開盤" in idx and idx["開盤"] < len(row) else float("nan"),
                high_px=_parse_market_number(row[idx["最高"]]) if "最高" in idx and idx["最高"] < len(row) else float("nan"),
                low_px=_parse_market_number(row[idx["最低"]]) if "最低" in idx and idx["最低"] < len(row) else float("nan"),
                close_px=_parse_market_number(row[idx["收盤"]]) if "收盤" in idx and idx["收盤"] < len(row) else float("nan"),
                volume=_parse_market_number(row[idx["成交股數"]]) if "成交股數" in idx and idx["成交股數"] < len(row) else float("nan"),
                source="TPEX dailyQuotes",
                source_url=response.url,
            )
    info["rows"] = len(parsed)
    return parsed, info


def _load_yahoo_symbol_map(data_root: Path) -> dict[str, str]:
    path = data_root / "symbols.csv"
    if not path.exists():
        return {}
    try:
        frame = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return {}
    if "code" not in frame.columns or "yahoo_symbol" not in frame.columns:
        return {}
    return {str(row["code"]).strip(): str(row["yahoo_symbol"]).strip() for _, row in frame.iterrows()}


def _fetch_official_price_rows(
    *,
    symbol: str,
    yahoo_symbol: str,
    frame: pd.DataFrame,
    intervals: list[tuple[str, str]],
    timeout_s: int,
    request_sleep_s: float,
    twse_cache: dict[tuple[str, date], tuple[list[dict[str, Any]], dict[str, Any]]],
    tpex_cache: dict[str, tuple[dict[str, dict[str, Any]], dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    suffix = yahoo_symbol.rsplit(".", 1)[-1].upper() if "." in yahoo_symbol else ""
    rows: list[dict[str, Any]] = []
    query_records: list[dict[str, Any]] = []

    def sleep_if_needed() -> None:
        if request_sleep_s > 0:
            time.sleep(float(request_sleep_s))

    if suffix in {"TW", ""}:
        for month in _month_keys_for_intervals(intervals):
            try:
                cache_key = (symbol, month)
                if cache_key in twse_cache:
                    month_rows, _info = twse_cache[cache_key]
                else:
                    month_rows, info = _fetch_twse_stock_month(symbol, month, timeout_s=timeout_s)
                    twse_cache[cache_key] = (month_rows, info)
                    query_records.append(info)
                    sleep_if_needed()
                rows.extend(month_rows)
            except Exception as exc:
                query_records.append(
                    {
                        "dataset": "TWSE_STOCK_DAY",
                        "symbol": symbol,
                        "start_date": month.isoformat(),
                        "end_date": date(month.year, month.month, calendar.monthrange(month.year, month.month)[1]).isoformat(),
                        "url": "",
                        "status_code": "",
                        "payload_status": "",
                        "payload_msg": repr(exc),
                        "rows": 0,
                    }
                )
                sleep_if_needed()
        if rows or suffix == "TW":
            return rows, query_records

    for key in _local_dates_for_intervals(frame, intervals):
        try:
            if key in tpex_cache:
                day_rows_by_symbol, _info = tpex_cache[key]
            else:
                day_rows_by_symbol, info = _fetch_tpex_daily_quotes_all(key, timeout_s=timeout_s)
                tpex_cache[key] = (day_rows_by_symbol, info)
                query_records.append(info)
                sleep_if_needed()
            row = day_rows_by_symbol.get(symbol)
            if row is not None:
                rows.append(row)
        except Exception as exc:
            query_records.append(
                {
                    "dataset": "TPEX_DAILY_QUOTES",
                    "symbol": symbol,
                    "start_date": key,
                    "end_date": key,
                    "url": "",
                    "status_code": "",
                    "payload_status": "",
                    "payload_msg": repr(exc),
                    "rows": 0,
                }
            )
            sleep_if_needed()
    return rows, query_records


def _collect_events(frame: pd.DataFrame, symbol: str, min_abs_log_return: float) -> list[ReturnEvent]:
    if "date" not in frame.columns or "adjclose" not in frame.columns:
        return []
    frame = frame.sort_values("date").reset_index(drop=True)
    adj = pd.to_numeric(frame["adjclose"], errors="coerce").to_numpy(dtype=np.float64)
    close = pd.to_numeric(frame.get("close", np.nan), errors="coerce").to_numpy(dtype=np.float64)
    volume = pd.to_numeric(frame.get(VOLUME_COLUMN, np.nan), errors="coerce").to_numpy(dtype=np.float64)
    dividends = pd.to_numeric(frame.get("Dividends", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    splits = pd.to_numeric(frame.get("Stock Splits", 0.0), errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    dates = [_date_key(value) for value in frame["date"]]
    out: list[ReturnEvent] = []
    for idx in range(len(frame) - 1):
        a = adj[idx]
        b = adj[idx + 1]
        if not (np.isfinite(a) and np.isfinite(b) and a > 0.0 and b > 0.0):
            continue
        log_ret = float(math.log(b / a))
        if abs(log_ret) < float(min_abs_log_return):
            continue
        out.append(
            ReturnEvent(
                symbol=symbol,
                event_date=dates[idx],
                next_date=dates[idx + 1],
                log_return=log_ret,
                simple_return=float(math.exp(log_ret) - 1.0),
                current_close=float(close[idx]) if np.isfinite(close[idx]) else float("nan"),
                current_adjclose=float(adj[idx]),
                next_close=float(close[idx + 1]) if np.isfinite(close[idx + 1]) else float("nan"),
                next_adjclose=float(adj[idx + 1]),
                current_volume=float(volume[idx]) if np.isfinite(volume[idx]) else float("nan"),
                next_volume=float(volume[idx + 1]) if np.isfinite(volume[idx + 1]) else float("nan"),
                current_dividends=float(dividends[idx]),
                current_splits=float(splits[idx]),
                next_dividends=float(dividends[idx + 1]),
                next_splits=float(splits[idx + 1]),
            )
        )
    return out


def _merge_intervals(intervals: list[tuple[str, str]]) -> list[tuple[str, str]]:
    if not intervals:
        return []
    parsed = sorted((_parse_date_key(start), _parse_date_key(end)) for start, end in intervals)
    merged: list[tuple[date, date]] = []
    for start, end in parsed:
        if not merged or start > merged[-1][1] + timedelta(days=1):
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return [(start.isoformat(), end.isoformat()) for start, end in merged]


def _date_in_intervals(key: str, intervals: list[tuple[str, str]]) -> bool:
    if not key:
        return False
    d = _parse_date_key(key)
    for start, end in intervals:
        if _parse_date_key(start) <= d <= _parse_date_key(end):
            return True
    return False


def _finmind_price_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row.get("date", ""))[:10]
        if key:
            out[key] = row
    return out


def _finmind_row_values(row: dict[str, Any]) -> dict[str, float]:
    return {
        "open": _safe_float(row.get("open")),
        "max": _safe_float(row.get("max")),
        "min": _safe_float(row.get("min")),
        "close": _safe_float(row.get("close")),
        "adjclose": _safe_float(row.get("close")),
        VOLUME_COLUMN: _safe_float(row.get(VOLUME_COLUMN)),
    }


def _relative_diff(a: float, b: float) -> float:
    if not (np.isfinite(a) and np.isfinite(b)):
        return float("inf")
    scale = max(abs(a), abs(b), 1e-12)
    return abs(a - b) / scale


def _is_etf_like_symbol(symbol: str) -> bool:
    return str(symbol).startswith("00")


def _event_verified_by_exchange(
    *,
    repaired: pd.DataFrame,
    event: ReturnEvent,
    price_by_date: dict[str, dict[str, Any]],
    date_to_idx: dict[str, int],
    mismatch_ratio: float,
) -> bool:
    for key in (event.event_date, event.next_date):
        source_row = price_by_date.get(key)
        idx = date_to_idx.get(key)
        if source_row is None or idx is None:
            return False
        source_values = _finmind_row_values(source_row)
        source_close = source_values["close"]
        source_volume = source_values[VOLUME_COLUMN]
        local_close = _safe_float(repaired.iloc[idx].get("close"))
        if _relative_diff(local_close, source_close) >= mismatch_ratio:
            return False
        if np.isfinite(source_volume) and source_volume <= 0.0:
            return False
    return True


def _record_row_change(
    *,
    symbol: str,
    key: str,
    method: str,
    reason: str,
    old_row: pd.Series,
    new_values: dict[str, float],
    source: str,
    source_url: str | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "symbol": symbol,
        "date": key,
        "repair_method": method,
        "repair_reason": reason,
        "source": source,
        "source_url": source_url or "",
    }
    for col in (*PRICE_COLUMNS, VOLUME_COLUMN, "Dividends", "Stock Splits"):
        if col in old_row.index:
            record[f"old_{col}"] = old_row.get(col)
    for col, value in new_values.items():
        record[f"new_{col}"] = value
    return record


def _apply_external_price_repairs(
    *,
    frame: pd.DataFrame,
    symbol: str,
    price_rows: list[dict[str, Any]],
    source_url: str,
    intervals: list[tuple[str, str]],
    mismatch_ratio: float,
    apply: bool,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if not price_rows or "date" not in frame.columns:
        return frame, []
    price_by_date = _finmind_price_map(price_rows)
    records: list[dict[str, Any]] = []
    repaired = frame.copy()
    for idx, row in frame.iterrows():
        key = _date_key(row.get("date"))
        source_row = price_by_date.get(key)
        if not source_row:
            continue
        values = _finmind_row_values(source_row)
        if not all(_finite_positive(values.get(col)) for col in OHLC_COLUMNS):
            continue
        local_close = _safe_float(row.get("close"))
        local_adjclose = _safe_float(row.get("adjclose"))
        source_close = values["close"]
        if _relative_diff(local_close, source_close) < mismatch_ratio:
            continue
        new_values = {col: values[col] for col in PRICE_COLUMNS if col in repaired.columns}
        if "adjclose" in new_values and np.isfinite(local_close) and local_close > 0.0 and np.isfinite(local_adjclose) and local_adjclose > 0.0:
            new_values["adjclose"] = source_close * (local_adjclose / local_close)
        if VOLUME_COLUMN in repaired.columns and np.isfinite(values[VOLUME_COLUMN]):
            new_values[VOLUME_COLUMN] = values[VOLUME_COLUMN]
        records.append(
            _record_row_change(
                symbol=symbol,
                key=key,
                method="replace_yahoo_ohlc_with_exchange",
                reason=(
                    "Local Yahoo OHLC/adjclose is materially inconsistent with official exchange OHLC "
                    f"inside an anomalous adjusted-return window; close relative diff={_relative_diff(local_close, source_close):.4f}."
                ),
                old_row=row,
                new_values=new_values,
                source=str(source_row.get("_source") or "official exchange price"),
                source_url=str(source_row.get("_source_url") or source_url),
            )
        )
        for col, value in new_values.items():
            repaired.at[idx, col] = value
    return repaired, records


def _mask_stale_no_source_rows(
    *,
    frame: pd.DataFrame,
    symbol: str,
    price_rows: list[dict[str, Any]],
    source_url: str,
    intervals: list[tuple[str, str]],
    endpoint_dates: set[str],
    apply: bool,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    price_dates = set(_finmind_price_map(price_rows))
    if not price_dates:
        return frame, []
    repaired = frame.copy()
    records: list[dict[str, Any]] = []
    for idx, row in frame.iterrows():
        key = _date_key(row.get("date"))
        if not _date_in_intervals(key, intervals) or key in price_dates:
            continue
        if key not in endpoint_dates and not price_dates:
            continue
        volume = _safe_float(row.get(VOLUME_COLUMN))
        prices = [_safe_float(row.get(col)) for col in OHLC_COLUMNS if col in row.index]
        finite_prices = [value for value in prices if np.isfinite(value)]
        all_equal = len(finite_prices) >= 2 and (max(finite_prices) - min(finite_prices)) <= max(1e-8, abs(finite_prices[0]) * 1e-8)
        if not ((np.isfinite(volume) and volume <= 0.0) or all_equal):
            continue
        new_values = {col: float("nan") for col in PRICE_COLUMNS if col in repaired.columns}
        records.append(
            _record_row_change(
                symbol=symbol,
                key=key,
                method="mask_no_external_trade_stale_row",
                reason="No official exchange price exists for this local stale/zero-volume row inside an anomalous window.",
                old_row=row,
                new_values=new_values,
                source="official exchange price",
                source_url=source_url,
            )
        )
        for col, value in new_values.items():
            repaired.at[idx, col] = value
    return repaired, records


def _residual_extreme_records(
    *,
    frame: pd.DataFrame,
    symbol: str,
    price_rows: list[dict[str, Any]],
    min_abs_log_return: float,
    intervals: list[tuple[str, str]],
    mismatch_ratio: float,
    source_note: str,
    source_url: str,
    apply: bool,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    repaired = frame.copy()
    events = _collect_events(repaired, symbol, min_abs_log_return)
    records: list[dict[str, Any]] = []
    if not events:
        return repaired, records
    date_to_idx = {_date_key(value): idx for idx, value in enumerate(repaired["date"])}
    price_by_date = _finmind_price_map(price_rows)
    if not price_by_date:
        return repaired, records
    masked_keys: set[str] = set()
    for event in events:
        if not (_date_in_intervals(event.event_date, intervals) or _date_in_intervals(event.next_date, intervals)):
            continue
        endpoint_source_rows = [price_by_date.get(event.event_date), price_by_date.get(event.next_date)]
        if any(row is None for row in endpoint_source_rows):
            continue
        if _is_etf_like_symbol(symbol) and _event_verified_by_exchange(
            repaired=repaired,
            event=event,
            price_by_date=price_by_date,
            date_to_idx=date_to_idx,
            mismatch_ratio=mismatch_ratio,
        ):
            continue
        for key in (event.event_date, event.next_date):
            if key in masked_keys:
                continue
            idx = date_to_idx.get(key)
            if idx is None:
                continue
            masked_keys.add(key)
            row = repaired.iloc[idx]
            new_values = {col: float("nan") for col in PRICE_COLUMNS if col in repaired.columns}
            event_source_url = " | ".join(
                sorted(
                    {
                        str(source_row.get("_source_url") or "")
                        for source_row in endpoint_source_rows
                        if source_row is not None and source_row.get("_source_url")
                    }
                )
            )
            records.append(
                _record_row_change(
                    symbol=symbol,
                    key=key,
                    method="mask_residual_extreme_boundary",
                    reason=(
                        "Adjusted-return boundary remains extreme after external price repair/check "
                        f"(event {event.event_date}->{event.next_date}, log_return={event.log_return:.6f}); "
                        "mask both endpoint price rows to prevent unverified corporate-action artifacts from training/backtest."
                    ),
                    old_row=row,
                    new_values=new_values,
                    source=source_note,
                    source_url=event_source_url or source_url,
                )
            )
            for col, value in new_values.items():
                repaired.at[idx, col] = value
    return repaired, records


def _events_to_frame(events: list[ReturnEvent]) -> pd.DataFrame:
    return pd.DataFrame([event.__dict__ for event in events])


def _method_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        method = str(record.get("repair_method", "unknown"))
        counts[method] = counts.get(method, 0) + 1
    return counts


def _ledger_source_covers_date(source_url: Any, key: str) -> bool:
    text = str(source_url or "")
    if not text or not key:
        return False
    d = _parse_date_key(key)
    month_token = f"date={d:%Y%m}01"
    day_token = f"date={d:%Y/%m/%d}"
    day_token_encoded = f"date={d:%Y}%2F{d:%m}%2F{d:%d}"
    return month_token in text or day_token in text or day_token_encoded in text


def _apply_ledger_record(frame: pd.DataFrame, row_idx: int, record: pd.Series) -> list[str]:
    method = str(record.get("repair_method") or "")
    changed: list[str] = []
    if method == "replace_yahoo_ohlc_with_exchange":
        for col in PRICE_COLUMNS:
            new_col = f"new_{col}"
            if col in frame.columns and new_col in record.index and pd.notna(record.get(new_col)):
                value = float(record.get(new_col))
                if col == "adjclose":
                    old_close = _safe_float(record.get("old_close"))
                    old_adjclose = _safe_float(record.get("old_adjclose"))
                    new_close = _safe_float(record.get("new_close"))
                    if np.isfinite(old_close) and old_close > 0.0 and np.isfinite(old_adjclose) and old_adjclose > 0.0 and np.isfinite(new_close):
                        value = new_close * (old_adjclose / old_close)
                frame.at[row_idx, col] = value
                changed.append(col)
        new_volume = record.get(f"new_{VOLUME_COLUMN}") if f"new_{VOLUME_COLUMN}" in record.index else np.nan
        if VOLUME_COLUMN in frame.columns and pd.notna(new_volume):
            frame.at[row_idx, VOLUME_COLUMN] = float(new_volume)
            changed.append(VOLUME_COLUMN)
    elif method == "mask_no_external_trade_stale_row":
        for col in PRICE_COLUMNS:
            if col in frame.columns:
                frame.at[row_idx, col] = float("nan")
                changed.append(col)
    return changed


def apply_ledger(
    *,
    ledger_path: Path,
    data_root: Path,
    output_dir: Path,
    backup_dir: Path,
    methods: set[str],
) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_target = backup_dir / timestamp
    apply_path = output_dir / f"tw_return_anomaly_apply_ledger_{timestamp}.csv"
    summary_path = output_dir / f"tw_return_anomaly_apply_ledger_{timestamp}.md"
    ledger = pd.read_csv(ledger_path)
    applied_records: list[dict[str, Any]] = []
    touched_symbols: list[str] = []

    for symbol, symbol_records in ledger.groupby(ledger["symbol"].astype(str), sort=True):
        parquet_path = data_root / f"{symbol}_features.parquet"
        if not parquet_path.exists():
            for _, record in symbol_records.iterrows():
                out = record.to_dict()
                out["applied"] = False
                out["apply_skip_reason"] = "missing_parquet"
                applied_records.append(out)
            continue
        frame, metadata = _read_parquet(parquet_path)
        date_to_idx = {_date_key(value): idx for idx, value in enumerate(frame["date"])}
        changed_symbol = False
        for _, record in symbol_records.iterrows():
            out = record.to_dict()
            method = str(record.get("repair_method") or "")
            key = str(record.get("date") or "")
            if method not in methods:
                out["applied"] = False
                out["apply_skip_reason"] = "method_not_selected"
                applied_records.append(out)
                continue
            if method == "replace_yahoo_ohlc_with_exchange" and not str(record.get("source_url") or ""):
                out["applied"] = False
                out["apply_skip_reason"] = "replace_without_source_url"
                applied_records.append(out)
                continue
            if method == "mask_no_external_trade_stale_row" and not _ledger_source_covers_date(record.get("source_url"), key):
                out["applied"] = False
                out["apply_skip_reason"] = "stale_mask_source_does_not_cover_date"
                applied_records.append(out)
                continue
            row_idx = date_to_idx.get(key)
            if row_idx is None:
                out["applied"] = False
                out["apply_skip_reason"] = "date_not_found"
                applied_records.append(out)
                continue
            changed_cols = _apply_ledger_record(frame, row_idx, record)
            if not changed_cols:
                out["applied"] = False
                out["apply_skip_reason"] = "no_columns_changed"
                applied_records.append(out)
                continue
            out["applied"] = True
            out["apply_skip_reason"] = ""
            out["applied_columns"] = ",".join(changed_cols)
            applied_records.append(out)
            changed_symbol = True

        if changed_symbol:
            backup_target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_path, backup_target / parquet_path.name)
            _write_parquet(frame, parquet_path, metadata)
            touched_symbols.append(symbol)

    applied = pd.DataFrame(applied_records)
    applied.to_csv(apply_path, index=False)
    applied_counts = applied["applied"].value_counts(dropna=False).to_dict() if not applied.empty else {}
    method_counts = (
        applied[applied["applied"] == True]["repair_method"].value_counts().to_dict()  # noqa: E712
        if not applied.empty
        else {}
    )
    skip_counts = (
        applied[applied["applied"] != True]["apply_skip_reason"].value_counts().to_dict()  # noqa: E712
        if not applied.empty
        else {}
    )
    lines = [
        "# TW Return Anomaly Apply Ledger",
        "",
        f"- source_ledger: `{ledger_path}`",
        f"- data_root: `{data_root}`",
        f"- selected_methods: `{','.join(sorted(methods))}`",
        f"- records_in_ledger: `{len(ledger)}`",
        f"- applied_records: `{int((applied['applied'] == True).sum()) if not applied.empty else 0}`",  # noqa: E712
        f"- touched_symbols: `{len(set(touched_symbols))}`",
        f"- backup_dir: `{backup_target}`",
        "",
        "## Applied Method Counts",
        "",
    ]
    for method, count in sorted(method_counts.items()):
        lines.append(f"- `{method}`: `{count}`")
    if not method_counts:
        lines.append("(none)")
    lines.extend(["", "## Skip Counts", ""])
    for reason, count in sorted(skip_counts.items()):
        lines.append(f"- `{reason}`: `{count}`")
    if not skip_counts:
        lines.append("(none)")
    lines.extend(["", "## Outputs", "", f"- apply_ledger: `{apply_path}`"])
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"apply_ledger={apply_path}")
    print(f"summary={summary_path}")
    print(json.dumps({"applied": applied_counts, "methods": method_counts, "skips": skip_counts}, ensure_ascii=False, sort_keys=True))
    return apply_path


def repair(
    *,
    data_root: Path,
    output_dir: Path,
    backup_dir: Path,
    min_abs_log_return: float,
    expand_days: int,
    mismatch_ratio: float,
    request_sleep_s: float,
    timeout_s: int,
    apply: bool,
    max_symbols: int | None,
    symbols_filter: set[str] | None,
    progress_interval: int,
) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_target = backup_dir / timestamp
    ledger_path = output_dir / f"tw_return_anomaly_repairs_{timestamp}.csv"
    events_path = output_dir / f"tw_return_anomaly_events_{timestamp}.csv"
    query_path = output_dir / f"tw_return_anomaly_external_queries_{timestamp}.csv"
    summary_path = output_dir / f"tw_return_anomaly_repairs_{timestamp}.md"

    parquet_paths = sorted(path for path in data_root.glob("*_features.parquet") if path.is_file())
    all_events: list[ReturnEvent] = []
    frames: dict[str, tuple[pd.DataFrame, dict[bytes, bytes] | None, Path]] = {}
    for path in parquet_paths:
        symbol = path.name.removesuffix("_features.parquet")
        if symbols_filter is not None and symbol not in symbols_filter:
            continue
        frame, metadata = _read_parquet(path)
        events = _collect_events(frame, symbol, min_abs_log_return)
        if not events:
            continue
        frames[symbol] = (frame, metadata, path)
        all_events.extend(events)

    events_frame = _events_to_frame(all_events)
    events_frame.to_csv(events_path, index=False)
    if not all_events:
        summary_path.write_text("# TW Return Anomaly Repairs\n\nNo events found.\n", encoding="utf-8")
        print(f"events={events_path}")
        print(f"summary={summary_path}")
        return ledger_path

    symbols = sorted(frames)
    if max_symbols is not None:
        symbols = symbols[: max(0, int(max_symbols))]
    yahoo_symbol_map = _load_yahoo_symbol_map(data_root)

    all_records: list[dict[str, Any]] = []
    query_records: list[dict[str, Any]] = []
    touched_symbols: list[str] = []
    external_checked_symbols: list[str] = []
    twse_cache: dict[tuple[str, date], tuple[list[dict[str, Any]], dict[str, Any]]] = {}
    tpex_cache: dict[str, tuple[dict[str, dict[str, Any]], dict[str, Any]]] = {}

    for symbol_idx, symbol in enumerate(symbols, start=1):
        frame, metadata, parquet_path = frames[symbol]
        symbol_events = [event for event in all_events if event.symbol == symbol]
        endpoint_dates = {event.event_date for event in symbol_events} | {event.next_date for event in symbol_events}
        intervals = _merge_intervals(
            [
                (_expand_date(event.event_date, -expand_days), _expand_date(event.next_date, expand_days))
                for event in symbol_events
            ]
        )
        price_rows, symbol_query_records = _fetch_official_price_rows(
            symbol=symbol,
            yahoo_symbol=yahoo_symbol_map.get(symbol, ""),
            frame=frame,
            intervals=intervals,
            timeout_s=timeout_s,
            request_sleep_s=request_sleep_s,
            twse_cache=twse_cache,
            tpex_cache=tpex_cache,
        )
        query_records.extend(symbol_query_records)
        price_url = " | ".join(sorted({str(row.get("_source_url") or "") for row in price_rows if row.get("_source_url")}))

        external_checked_symbols.append(symbol)
        repaired = frame.copy()
        records: list[dict[str, Any]] = []
        repaired, new_records = _apply_external_price_repairs(
            frame=repaired,
            symbol=symbol,
            price_rows=price_rows,
            source_url=price_url,
            intervals=intervals,
            mismatch_ratio=mismatch_ratio,
            apply=apply,
        )
        records.extend(new_records)
        repaired, new_records = _mask_stale_no_source_rows(
            frame=repaired,
            symbol=symbol,
            price_rows=price_rows,
            source_url=price_url,
            intervals=intervals,
            endpoint_dates=endpoint_dates,
            apply=apply,
        )
        records.extend(new_records)
        repaired, new_records = _residual_extreme_records(
            frame=repaired,
            symbol=symbol,
            price_rows=price_rows,
            min_abs_log_return=min_abs_log_return,
            intervals=intervals,
            mismatch_ratio=mismatch_ratio,
            source_note="official exchange OHLC plus local corporate-action boundary check",
            source_url=price_url,
            apply=apply,
        )
        records.extend(new_records)

        all_records.extend(records)
        if apply and records:
            backup_target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_path, backup_target / parquet_path.name)
            _write_parquet(repaired, parquet_path, metadata)
            touched_symbols.append(symbol)
        if progress_interval > 0 and (symbol_idx == 1 or symbol_idx % progress_interval == 0 or symbol_idx == len(symbols)):
            print(
                f"[{symbol_idx}/{len(symbols)}] symbol={symbol} "
                f"events={len(symbol_events)} repairs={len(records)} apply={apply}",
                flush=True,
            )

    ledger = pd.DataFrame(all_records)
    if ledger.empty:
        ledger = pd.DataFrame(columns=["symbol", "date", "repair_method", "repair_reason", "source", "source_url"])
    ledger.to_csv(ledger_path, index=False)
    pd.DataFrame(query_records).to_csv(query_path, index=False)

    method_counts = _method_counts(all_records)
    lines = [
        "# TW Return Anomaly Repair Ledger",
        "",
        f"- apply: `{apply}`",
        f"- data_root: `{data_root}`",
        f"- min_abs_log_return: `{min_abs_log_return:.6f}`",
        f"- equivalent positive simple threshold: `{math.exp(min_abs_log_return) - 1.0:.2%}`",
        f"- candidate_events: `{len(all_events)}`",
        f"- candidate_symbols: `{len(frames)}`",
        f"- external_checked_symbols: `{len(set(external_checked_symbols))}`",
        f"- repair_records: `{len(all_records)}`",
        f"- touched_symbols: `{len(set(touched_symbols))}`",
        f"- backup_dir: `{backup_target}`",
        "",
        "## Method Counts",
        "",
    ]
    for method, count in sorted(method_counts.items()):
        lines.append(f"- `{method}`: `{count}`")
    if not method_counts:
        lines.append("(none)")
    lines.extend(
        [
            "",
            "## Outputs",
            "",
            f"- events: `{events_path}`",
            f"- external_queries: `{query_path}`",
            f"- ledger: `{ledger_path}`",
            "",
            "## Method",
            "",
            "Each anomalous adjusted-return boundary is checked against official exchange daily OHLC where available "
            "(TWSE STOCK_DAY for listed .TW symbols, TPEX dailyQuotes for .TWO symbols). "
            "Rows where local Yahoo OHLC materially disagrees with official exchange OHLC are replaced with official OHLC "
            "and official close as a conservative adjusted close for the repaired row. Stale local rows with no external trade are masked. "
            "ETF-like symbols whose extreme moves are verified by official exchange OHLC on both endpoints are kept as real market moves. "
            "If an adjusted-return boundary remains extreme after those checks, both endpoint price rows are masked "
            "so the unverified corporate-action boundary cannot become a train/test label or feature artifact.",
        ]
    )
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"events={events_path}")
    print(f"queries={query_path}")
    print(f"ledger={ledger_path}")
    print(f"summary={summary_path}")
    print(json.dumps({k: method_counts[k] for k in sorted(method_counts)}, ensure_ascii=False, sort_keys=True))
    return ledger_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit and repair TW adjusted-return anomalies with external evidence.")
    parser.add_argument("--data-root", type=Path, default=Path("data_yahoo/tw_stocks"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_logs"))
    parser.add_argument("--backup-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_backups/tw_return_anomalies"))
    parser.add_argument("--apply-ledger", type=Path, default=None, help="Apply a previously generated repair ledger without re-querying sources.")
    parser.add_argument(
        "--ledger-methods",
        type=str,
        default="replace_yahoo_ohlc_with_exchange,mask_no_external_trade_stale_row",
        help="Comma-separated methods to apply when --apply-ledger is used.",
    )
    parser.add_argument("--min-abs-log-return", type=float, default=math.log(1.35))
    parser.add_argument("--expand-days", type=int, default=30)
    parser.add_argument("--mismatch-ratio", type=float, default=0.02)
    parser.add_argument("--request-sleep-s", type=float, default=0.05)
    parser.add_argument("--timeout-s", type=int, default=30)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--symbols", type=str, default="", help="Comma-separated symbol allowlist, e.g. 6669,2380.")
    parser.add_argument("--progress-interval", type=int, default=25)
    parser.add_argument("--apply", action="store_true", help="Mutate parquet files. Without this flag only writes audit ledgers.")
    args = parser.parse_args()
    if args.apply_ledger is not None:
        methods = {part.strip() for part in str(args.ledger_methods).split(",") if part.strip()}
        apply_ledger(
            ledger_path=args.apply_ledger,
            data_root=args.data_root,
            output_dir=args.output_dir,
            backup_dir=args.backup_dir,
            methods=methods,
        )
        return
    symbols_filter = {part.strip() for part in str(args.symbols).split(",") if part.strip()} or None
    repair(
        data_root=args.data_root,
        output_dir=args.output_dir,
        backup_dir=args.backup_dir,
        min_abs_log_return=float(args.min_abs_log_return),
        expand_days=int(args.expand_days),
        mismatch_ratio=float(args.mismatch_ratio),
        request_sleep_s=float(args.request_sleep_s),
        timeout_s=int(args.timeout_s),
        apply=bool(args.apply),
        max_symbols=args.max_symbols,
        symbols_filter=symbols_filter,
        progress_interval=int(args.progress_interval),
    )


if __name__ == "__main__":
    main()
