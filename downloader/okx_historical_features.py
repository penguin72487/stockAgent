from __future__ import annotations

import csv
import io
import json
import os
import threading
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm


KLINE_BAR = "15m"
CANDLE_INTERVAL_MS = 15 * 60 * 1000
FEATURE_SCHEMA_VERSION = 1

MARK_PRICE_HISTORY_ENDPOINT = "/api/v5/market/history-mark-price-candles"
INDEX_PRICE_HISTORY_ENDPOINT = "/api/v5/market/history-index-candles"
FUNDING_RATE_HISTORY_ENDPOINT = "/api/v5/public/funding-rate-history"
MARKET_DATA_HISTORY_ENDPOINT = "/api/v5/public/market-data-history"
OPEN_INTEREST_HISTORY_ENDPOINT = "/api/v5/rubik/stat/contracts/open-interest-history"
TAKER_VOLUME_HISTORY_ENDPOINT = "/api/v5/rubik/stat/taker-volume-contract"
LONG_SHORT_ACCOUNT_RATIO_ENDPOINT = (
    "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract"
)
TOP_TRADER_ACCOUNT_RATIO_ENDPOINT = (
    "/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader"
)
TOP_TRADER_POSITION_RATIO_ENDPOINT = (
    "/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader"
)

FEATURE_STAGE_IDS = (
    "mark_price",
    "index_price",
    "funding_rate",
    "open_interest",
    "taker_volume",
    "long_short_account_ratio",
    "top_trader_account_ratio",
    "top_trader_position_ratio",
)

RAW_FEATURE_COLUMNS = (
    "okx_mark_open",
    "okx_mark_high",
    "okx_mark_low",
    "okx_mark_close",
    "okx_index_open",
    "okx_index_high",
    "okx_index_low",
    "okx_index_close",
    "okx_funding_rate_at_settlement",
    "okx_funding_realized_rate",
    "okx_funding_interval_hours",
    "okx_funding_age_hours",
    "okx_funding_formula_with_rate",
    "okx_funding_method_current_period",
    "okx_open_interest_contracts",
    "okx_open_interest_ccy",
    "okx_open_interest_usd",
    "okx_taker_sell_volume_contracts",
    "okx_taker_buy_volume_contracts",
    "okx_long_short_account_ratio",
    "okx_top_trader_account_ratio",
    "okx_top_trader_position_ratio",
)

DERIVED_FEATURE_COLUMNS = (
    "okx_mark_log_return_15m",
    "okx_index_log_return_15m",
    "okx_mark_range_log",
    "okx_index_range_log",
    "okx_contract_mark_basis_log",
    "okx_mark_index_basis_log",
    "okx_funding_realized_annualized",
    "okx_open_interest_contracts_log_change_15m",
    "okx_open_interest_usd_log_change_15m",
    "okx_open_interest_usd_to_volume_log",
    "okx_taker_imbalance",
    "okx_long_short_account_ratio_log",
    "okx_top_trader_account_ratio_log",
    "okx_top_trader_position_ratio_log",
)

ALL_FEATURE_COLUMNS = (*RAW_FEATURE_COLUMNS, *DERIVED_FEATURE_COLUMNS)

FAMILY_PRIMARY_COLUMN = {
    "mark_price": "okx_mark_close",
    "index_price": "okx_index_close",
    "funding_rate": "okx_funding_realized_rate",
    "open_interest": "okx_open_interest_usd",
    "taker_volume": "okx_taker_buy_volume_contracts",
    "long_short_account_ratio": "okx_long_short_account_ratio",
    "top_trader_account_ratio": "okx_top_trader_account_ratio",
    "top_trader_position_ratio": "okx_top_trader_position_ratio",
}


HISTORICAL_FEATURE_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "id": "trade_candles_15m",
        "category": "price_volume",
        "download_status": "included_existing",
        "endpoint": "/api/v5/market/history-candles",
        "history_contract": "recent years; 15m completed bars",
        "grain": "instrument_15m",
        "model_role": "OHLCV source of truth",
        "reason": "Canonical price/volume series already downloaded by the parent downloader.",
    },
    {
        "id": "mark_price_candles_15m",
        "category": "fair_value",
        "download_status": "included",
        "endpoint": MARK_PRICE_HISTORY_ENDPOINT,
        "history_contract": "recent years",
        "grain": "instrument_15m",
        "model_role": "mark return, mark range, contract-mark basis",
        "reason": "Historical candles are point-in-time reconstructable.",
    },
    {
        "id": "index_price_candles_15m",
        "category": "fair_value",
        "download_status": "included",
        "endpoint": INDEX_PRICE_HISTORY_ENDPOINT,
        "history_contract": "recent years",
        "grain": "instrument_family_15m",
        "model_role": "index return, index range, mark-index and contract-index basis",
        "reason": "Historical candles are point-in-time reconstructable.",
    },
    {
        "id": "funding_rate",
        "category": "carry_crowding",
        "download_status": "included",
        "endpoint": (
            f"{MARKET_DATA_HISTORY_ENDPOINT} module=3 plus "
            f"{FUNDING_RATE_HISTORY_ENDPOINT}"
        ),
        "history_contract": (
            "archive coverage varies and is backfilled by OKX; REST history is limited "
            "to three months"
        ),
        "grain": "instrument_settlement_event_asof_15m",
        "model_role": "realized funding, settlement prediction, interval, age, annualized carry",
        "reason": "Monthly archive reconstructs old realized rates; REST supplies recent metadata.",
    },
    {
        "id": "open_interest_15m",
        "category": "leverage",
        "download_status": "included_limited_history",
        "endpoint": OPEN_INTEREST_HISTORY_ENDPOINT,
        "history_contract": "latest 1,440 entries; about 15 days at 15m",
        "grain": "instrument_15m",
        "model_role": "OI contracts/coin/USD and causal changes",
        "reason": "Historical endpoint exists, but the short coverage boundary is retained.",
    },
    {
        "id": "contract_taker_volume_15m",
        "category": "order_flow",
        "download_status": "included_limited_history",
        "endpoint": TAKER_VOLUME_HISTORY_ENDPOINT,
        "history_contract": "latest 1,440 entries; about 15 days at 15m",
        "grain": "instrument_15m",
        "model_role": "taker buy/sell contracts and imbalance",
        "reason": "Historical aggregate is reconstructable without a live trade snapshot.",
    },
    {
        "id": "contract_long_short_account_ratio_15m",
        "category": "positioning",
        "download_status": "included_limited_history",
        "endpoint": LONG_SHORT_ACCOUNT_RATIO_ENDPOINT,
        "history_contract": "latest 1,440 entries; about 15 days at 15m",
        "grain": "instrument_15m",
        "model_role": "all-trader account-count long/short ratio",
        "reason": "Historical endpoint exists; account ratio is not position notional.",
    },
    {
        "id": "top_trader_account_ratio_15m",
        "category": "positioning",
        "download_status": "included_limited_history",
        "endpoint": TOP_TRADER_ACCOUNT_RATIO_ENDPOINT,
        "history_contract": "latest 1,440 entries",
        "grain": "instrument_15m",
        "model_role": "top-5%-trader account-count long/short ratio",
        "reason": "Historical endpoint exists and is distinct from position ratio.",
    },
    {
        "id": "top_trader_position_ratio_15m",
        "category": "positioning",
        "download_status": "included_limited_history",
        "endpoint": TOP_TRADER_POSITION_RATIO_ENDPOINT,
        "history_contract": "latest 1,440 entries",
        "grain": "instrument_15m",
        "model_role": "top-5%-trader position-notional long/short ratio",
        "reason": "Historical endpoint exists and is distinct from account ratio.",
    },
    {
        "id": "premium_history_raw",
        "category": "fair_value",
        "download_status": "excluded_redundant",
        "endpoint": "/api/v5/public/premium-history",
        "history_contract": "past six months; high-frequency samples",
        "grain": "instrument_event",
        "model_role": "premium index",
        "reason": (
            "The same causal mechanism is reconstructed at 15m from historical mark and "
            "index candles; downloading every raw premium sample would multiply requests."
        ),
    },
    {
        "id": "tick_trade_archive",
        "category": "microstructure_archive",
        "download_status": "separate_large_archive",
        "endpoint": f"{MARKET_DATA_HISTORY_ENDPOINT} module=1",
        "history_contract": "T+2; OKX backfill coverage varies",
        "grain": "tick",
        "model_role": "trade count, size distribution, exact CVD",
        "reason": (
            "Historically reconstructable but materially larger than the compact 15m "
            "feature layer; do not silently download it for every live SWAP."
        ),
    },
    {
        "id": "orderbook_400_5000_archive",
        "category": "microstructure_archive",
        "download_status": "separate_extreme_archive",
        "endpoint": f"{MARKET_DATA_HISTORY_ENDPOINT} module=4/5",
        "history_contract": "T+3; OKX backfill coverage varies",
        "grain": "L2 event",
        "model_role": "spread, depth, imbalance, slope, replenishment",
        "reason": (
            "Historically reconstructable, but a live BTC-USDT sample was about 1,973 MB "
            "for six 400-level days; it requires an independently budgeted L2 pipeline."
        ),
    },
    {
        "id": "one_minute_candle_archive",
        "category": "duplicate_archive",
        "download_status": "excluded_duplicate",
        "endpoint": f"{MARKET_DATA_HISTORY_ENDPOINT} module=2",
        "history_contract": "T+2; OKX backfill coverage varies",
        "grain": "instrument_1m",
        "model_role": "none in the current 15m source contract",
        "reason": "The canonical downloader already obtains official completed 15m candles.",
    },
    {
        "id": "borrowing_rate_archive",
        "category": "non_swap_archive",
        "download_status": "excluded_non_swap_grain",
        "endpoint": f"{MARKET_DATA_HISTORY_ENDPOINT} module=11",
        "history_contract": "T+2; OKX backfill coverage varies",
        "grain": "margin_currency",
        "model_role": "possible slower market-level context",
        "reason": "Margin borrowing is not measured at the current SWAP instrument grain.",
    },
    {
        "id": "margin_loan_and_currency_ratios",
        "category": "non_swap_market_context",
        "download_status": "excluded_non_swap_grain",
        "endpoint": (
            "/api/v5/rubik/stat/margin/loan-ratio and currency-level "
            "taker/long-short endpoints"
        ),
        "history_contract": "historical Rubik series with endpoint-specific limits",
        "grain": "currency_market",
        "model_role": "possible slower cross-market context",
        "reason": "Not the same unit of observation as a per-contract SWAP row.",
    },
    {
        "id": "option_trading_statistics",
        "category": "non_swap_market_context",
        "download_status": "excluded_non_swap_grain",
        "endpoint": "OKX Rubik option OI/volume, put-call, expiry, strike and taker-flow",
        "history_contract": "historical Rubik series with endpoint-specific limits",
        "grain": "BTC_ETH_option_market",
        "model_role": "possible BTC/ETH market-level regime",
        "reason": "These are not defined for every SWAP and require a separate market-level join.",
    },
    {
        "id": "security_fund_history",
        "category": "exchange_risk_context",
        "download_status": "excluded_semantic_instability",
        "endpoint": "/api/v5/public/insurance-fund",
        "history_contract": "paginated events; no stable history range documented",
        "grain": "instrument_family_event",
        "model_role": "possible exchange stress context",
        "reason": (
            "The documentation says regular_update was removed while the 2026-07-30 "
            "live response still returned it; do not train on an unstable definition."
        ),
    },
    {
        "id": "delivery_and_settlement_history",
        "category": "non_swap_contract",
        "download_status": "excluded_not_applicable",
        "endpoint": (
            "/api/v5/public/delivery-exercise-history and "
            "/api/v5/public/settlement-history"
        ),
        "history_contract": "last three months",
        "grain": "FUTURES_OPTION_event",
        "model_role": "none for perpetual SWAP",
        "reason": "Perpetual swaps do not have expiry delivery or futures settlement.",
    },
    {
        "id": "economic_calendar",
        "category": "authenticated_macro",
        "download_status": "excluded_auth_and_revision",
        "endpoint": "/api/v5/public/economic-calendar",
        "history_contract": "authentication required; older than three months requires VIP1",
        "grain": "macro_event",
        "model_role": "possible market-level regime",
        "reason": (
            "It is not an anonymous public-history source and revised values need a "
            "separate point-in-time vintage contract."
        ),
    },
    {
        "id": "current_open_interest",
        "category": "snapshot_only",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/public/open-interest",
        "history_contract": "current snapshot only",
        "grain": "instrument_snapshot",
        "model_role": "none",
        "reason": "The current value cannot reconstruct historical decision-time observations.",
    },
    {
        "id": "current_funding_estimate",
        "category": "snapshot_only",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/public/funding-rate",
        "history_contract": "current estimate only",
        "grain": "instrument_snapshot",
        "model_role": "none",
        "reason": (
            "Final historical funding must not be substituted for the estimate visible "
            "at an earlier decision time."
        ),
    },
    {
        "id": "ticker_and_rolling_24h",
        "category": "snapshot_only",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/market/ticker and /api/v5/market/platform-24-volume",
        "history_contract": "current rolling snapshot only",
        "grain": "instrument_snapshot",
        "model_role": "none",
        "reason": "Historical rolling-window values cannot be reconstructed from the endpoint.",
    },
    {
        "id": "rest_orderbook",
        "category": "snapshot_only",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/market/books and /api/v5/market/books-full",
        "history_contract": "current snapshot only",
        "grain": "instrument_snapshot",
        "model_role": "none",
        "reason": "Use the separately budgeted historical L2 archive, not current REST books.",
    },
    {
        "id": "liquidation_orders",
        "category": "stream_only",
        "download_status": "excluded_unreconstructable",
        "endpoint": "WebSocket liquidation-orders",
        "history_contract": "real-time stream; historical REST removed",
        "grain": "liquidation_event",
        "model_role": "none",
        "reason": "OKX removed the historical liquidation REST endpoint in April 2023.",
    },
    {
        "id": "index_components",
        "category": "snapshot_only",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/market/index-components",
        "history_contract": "current composition only",
        "grain": "index_snapshot",
        "model_role": "none",
        "reason": "Today’s index composition must not be projected into historical rows.",
    },
    {
        "id": "option_market_snapshot",
        "category": "snapshot_only_non_swap",
        "download_status": "excluded_snapshot",
        "endpoint": "/api/v5/public/opt-summary",
        "history_contract": "current option summary",
        "grain": "option_snapshot",
        "model_role": "none",
        "reason": "Not historically reconstructable and not at the current SWAP instrument grain.",
    },
)


class OkxHistoricalClient(Protocol):
    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]: ...

    def get_bytes(self, url: str) -> bytes: ...


@dataclass(slots=True)
class HistoricalFeatureResult:
    code: str
    okx_symbol: str
    status: str
    rows: int
    output_path: str
    changed: bool
    stage_status_json: str
    coverage_json: str
    errors_json: str


def feature_catalog_payload() -> dict[str, Any]:
    return {
        "schema_version": FEATURE_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bar": KLINE_BAR,
        "decision_contract": (
            "A row is available only after its 15-minute bar closes. Event series use "
            "event_ts <= bar_close_ts; execution must occur at or after the next bar open."
        ),
        "catalog": list(HISTORICAL_FEATURE_CATALOG),
    }


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.features.tmp")
    try:
        pq.write_table(frame.to_arrow(), tmp_path, compression="snappy", write_statistics=True)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _datetime_ms_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.to_datetime(strict=False)
        .cast(pl.Int64)
        .floordiv(1_000)
    )


def _latest_non_null_ms(
    frame: pl.DataFrame,
    column: str,
    fallback_start_ms: int,
) -> int:
    if column not in frame.columns or "date" not in frame.columns:
        return fallback_start_ms
    available = frame.filter(pl.col(column).is_not_null())
    if available.is_empty():
        return fallback_start_ms
    latest = available.select(_datetime_ms_expr("date").max()).item()
    if latest is None:
        return fallback_start_ms
    return max(fallback_start_ms, int(latest) - CANDLE_INTERVAL_MS)


def _fetch_array_history(
    client: OkxHistoricalClient,
    path: str,
    base_params: dict[str, Any],
    *,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> list[list[str]]:
    rows: list[list[str]] = []
    cursor_after = end_ms + CANDLE_INTERVAL_MS
    seen: set[int] = set()
    while True:
        params = {**base_params, "after": str(cursor_after), "limit": str(limit)}
        chunk = client.get(path, params).get("data", [])
        if not chunk:
            break
        timestamps = [int(row[0]) for row in chunk if row]
        if not timestamps:
            break
        rows.extend(chunk)
        oldest = min(timestamps)
        if oldest <= start_ms or len(chunk) < limit or oldest in seen:
            break
        seen.add(oldest)
        cursor_after = oldest
    return rows


def _fetch_object_history(
    client: OkxHistoricalClient,
    path: str,
    base_params: dict[str, Any],
    *,
    timestamp_field: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor_after = end_ms + 1
    seen: set[int] = set()
    while True:
        params = {**base_params, "after": str(cursor_after), "limit": str(limit)}
        chunk = client.get(path, params).get("data", [])
        if not chunk:
            break
        timestamps = [
            int(item[timestamp_field])
            for item in chunk
            if item.get(timestamp_field) not in {None, ""}
        ]
        if not timestamps:
            break
        rows.extend(chunk)
        oldest = min(timestamps)
        if oldest <= start_ms or len(chunk) < limit or oldest in seen:
            break
        seen.add(oldest)
        cursor_after = oldest
    return rows


def _fetch_rubik_history(
    client: OkxHistoricalClient,
    path: str,
    base_params: dict[str, Any],
    *,
    start_ms: int,
    end_ms: int,
    limit: int = 100,
) -> list[list[str]]:
    rows: list[list[str]] = []
    cursor_end = end_ms
    seen: set[int] = set()
    while True:
        params = {
            **base_params,
            "begin": str(start_ms),
            "end": str(cursor_end),
            "limit": str(limit),
        }
        chunk = client.get(path, params).get("data", [])
        if not chunk:
            break
        timestamps = [int(row[0]) for row in chunk if row]
        if not timestamps:
            break
        rows.extend(chunk)
        oldest = min(timestamps)
        if oldest <= start_ms or len(chunk) < limit or oldest in seen:
            break
        seen.add(oldest)
        cursor_end = oldest - 1
    return rows


def _normalize_price_candles(
    raw_rows: list[list[str]],
    *,
    prefix: str,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < 6:
            continue
        ts = int(row[0])
        if not (start_ms <= ts <= end_ms) or str(row[5]) != "1":
            continue
        rows.append(
            {
                "date": _ms_to_date_string(ts),
                f"{prefix}_open": float(row[1]),
                f"{prefix}_high": float(row[2]),
                f"{prefix}_low": float(row[3]),
                f"{prefix}_close": float(row[4]),
            }
        )
    columns = [
        "date",
        f"{prefix}_open",
        f"{prefix}_high",
        f"{prefix}_low",
        f"{prefix}_close",
    ]
    if not rows:
        return pl.DataFrame({column: [] for column in columns})
    return (
        pl.DataFrame(rows)
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
    )


def _normalize_rubik_rows(
    raw_rows: list[list[str]],
    *,
    value_columns: tuple[str, ...],
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if len(row) < len(value_columns) + 1:
            continue
        ts = int(row[0])
        if not (start_ms <= ts <= end_ms):
            continue
        item: dict[str, Any] = {"date": _ms_to_date_string(ts)}
        for idx, column in enumerate(value_columns, start=1):
            item[column] = float(row[idx]) if row[idx] not in {None, ""} else None
        rows.append(item)
    columns = ["date", *value_columns]
    if not rows:
        return pl.DataFrame({column: [] for column in columns})
    return (
        pl.DataFrame(rows)
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
    )


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.year * 12 + (value.month - 1) + months
    year, zero_based_month = divmod(month_index, 12)
    return datetime(year, zero_based_month + 1, 1, tzinfo=timezone.utc)


def _monthly_query_ranges(start_ms: int, end_ms: int) -> list[tuple[int, int]]:
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)
    cursor = datetime(start_dt.year, start_dt.month, 1, tzinfo=timezone.utc)
    ranges: list[tuple[int, int]] = []
    while cursor <= end_dt:
        next_cursor = _add_months(cursor, 20)
        chunk_end = min(end_dt, next_cursor)
        ranges.append((int(cursor.timestamp() * 1000), int(chunk_end.timestamp() * 1000)))
        cursor = next_cursor
    return ranges


def _fetch_funding_archive(
    client: OkxHistoricalClient,
    *,
    inst_family: str,
    okx_symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    urls: dict[str, str] = {}
    for query_start, query_end in _monthly_query_ranges(start_ms, end_ms):
        payload = client.get(
            MARKET_DATA_HISTORY_ENDPOINT,
            {
                "module": "3",
                "instType": "SWAP",
                "instFamilyList": inst_family,
                "dateAggrType": "monthly",
                "begin": str(query_start),
                "end": str(query_end),
            },
        )
        for block in payload.get("data", []):
            for detail in block.get("details", []):
                for group in detail.get("groupDetails", []):
                    filename = str(group.get("filename") or "")
                    url = str(group.get("url") or "")
                    if filename and url and okx_symbol in filename:
                        urls[filename] = url

    rows: list[dict[str, Any]] = []
    for filename, url in sorted(urls.items()):
        payload = client.get_bytes(url)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            for member in archive.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                with archive.open(member) as handle:
                    reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig"))
                    for row in reader:
                        symbol = str(
                            row.get("instrument_name")
                            or row.get("instId")
                            or row.get("instrument")
                            or ""
                        ).strip()
                        if symbol != okx_symbol:
                            continue
                        ts_text = (
                            row.get("funding_time")
                            or row.get("fundingTime")
                            or row.get("ts")
                        )
                        rate_text = row.get("funding_rate") or row.get("fundingRate")
                        if not ts_text or rate_text in {None, ""}:
                            continue
                        ts = int(ts_text)
                        if start_ms <= ts <= end_ms:
                            rows.append(
                                {
                                    "fundingTime": str(ts),
                                    "fundingRate": None,
                                    "realizedRate": str(rate_text),
                                    "formulaType": None,
                                    "method": None,
                                    "_archive_file": filename,
                                }
                            )
    return rows


def _normalize_funding_events(
    archive_rows: list[dict[str, Any]],
    rest_rows: list[dict[str, Any]],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    by_timestamp: dict[int, dict[str, Any]] = {}
    for item in [*archive_rows, *rest_rows]:
        if item.get("fundingTime") in {None, ""}:
            continue
        ts = int(item["fundingTime"])
        if not (start_ms <= ts <= end_ms):
            continue
        predicted = item.get("fundingRate")
        realized = item.get("realizedRate")
        by_timestamp[ts] = {
            "ts": ts,
            "okx_funding_rate_at_settlement": (
                float(predicted) if predicted not in {None, ""} else None
            ),
            "okx_funding_realized_rate": (
                float(realized) if realized not in {None, ""} else None
            ),
            "okx_funding_formula_with_rate": (
                1.0
                if item.get("formulaType") == "withRate"
                else 0.0
                if item.get("formulaType") == "noRate"
                else None
            ),
            "okx_funding_method_current_period": (
                1.0
                if item.get("method") == "current_period"
                else 0.0
                if item.get("method") == "next_period"
                else None
            ),
        }

    events = [by_timestamp[key] for key in sorted(by_timestamp)]
    previous_ts: int | None = None
    for event in events:
        ts = int(event["ts"])
        event["okx_funding_interval_hours"] = (
            (ts - previous_ts) / (60 * 60 * 1000) if previous_ts is not None else None
        )
        previous_ts = ts
    return events


def _materialize_funding_asof(
    frame: pl.DataFrame,
    events: list[dict[str, Any]],
    *,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    columns = [
        "date",
        "okx_funding_rate_at_settlement",
        "okx_funding_realized_rate",
        "okx_funding_interval_hours",
        "okx_funding_age_hours",
        "okx_funding_formula_with_rate",
        "okx_funding_method_current_period",
    ]
    if not events:
        return pl.DataFrame({column: [] for column in columns})

    selected = (
        frame.select(["date", _datetime_ms_expr("date").alias("_bar_open_ms")])
        .filter(
            (pl.col("_bar_open_ms") >= start_ms)
            & (pl.col("_bar_open_ms") <= end_ms)
        )
        .sort("_bar_open_ms")
    )
    if selected.is_empty():
        return pl.DataFrame({column: [] for column in columns})

    bar_open_ms = selected["_bar_open_ms"].to_numpy()
    bar_close_ms = bar_open_ms + CANDLE_INTERVAL_MS
    event_ts = np.asarray([int(event["ts"]) for event in events], dtype=np.int64)
    indices = np.searchsorted(event_ts, bar_close_ms, side="right") - 1
    valid = indices >= 0

    def values(name: str) -> list[float | None]:
        source = [event.get(name) for event in events]
        output: list[float | None] = []
        for is_valid, idx in zip(valid.tolist(), indices.tolist(), strict=True):
            output.append(source[idx] if is_valid else None)
        return output

    age_hours: list[float | None] = []
    for is_valid, idx, close_ms in zip(
        valid.tolist(),
        indices.tolist(),
        bar_close_ms.tolist(),
        strict=True,
    ):
        age_hours.append(
            (int(close_ms) - int(event_ts[idx])) / (60 * 60 * 1000)
            if is_valid
            else None
        )

    return pl.DataFrame(
        {
            "date": selected["date"],
            "okx_funding_rate_at_settlement": values(
                "okx_funding_rate_at_settlement"
            ),
            "okx_funding_realized_rate": values("okx_funding_realized_rate"),
            "okx_funding_interval_hours": values("okx_funding_interval_hours"),
            "okx_funding_age_hours": age_hours,
            "okx_funding_formula_with_rate": values(
                "okx_funding_formula_with_rate"
            ),
            "okx_funding_method_current_period": values(
                "okx_funding_method_current_period"
            ),
        }
    )


def _overlay_feature_frame(
    base: pl.DataFrame,
    fresh: pl.DataFrame,
    columns: tuple[str, ...],
) -> pl.DataFrame:
    if fresh.is_empty():
        return base
    fresh_columns = [column for column in columns if column in fresh.columns]
    if not fresh_columns:
        return base
    renamed = fresh.select(["date", *fresh_columns]).rename(
        {column: f"__fresh_{column}" for column in fresh_columns}
    )
    joined = base.join(renamed, on="date", how="left")
    expressions: list[pl.Expr] = []
    for column in fresh_columns:
        incoming = pl.col(f"__fresh_{column}")
        if column in joined.columns:
            expressions.append(pl.coalesce([incoming, pl.col(column)]).alias(column))
        else:
            expressions.append(incoming.alias(column))
    return joined.with_columns(expressions).drop(
        [f"__fresh_{column}" for column in fresh_columns]
    )


def _ensure_raw_columns(frame: pl.DataFrame) -> pl.DataFrame:
    missing = [
        pl.lit(None, dtype=pl.Float64).alias(column)
        for column in RAW_FEATURE_COLUMNS
        if column not in frame.columns
    ]
    return frame.with_columns(missing) if missing else frame


def _positive_log_ratio(numerator: str, denominator: str, alias: str) -> pl.Expr:
    return (
        pl.when(
            (pl.col(numerator) > 0)
            & (pl.col(denominator) > 0)
            & pl.col(numerator).is_finite()
            & pl.col(denominator).is_finite()
        )
        .then((pl.col(numerator) / pl.col(denominator)).log())
        .otherwise(None)
        .alias(alias)
    )


def _contiguous_log_change(column: str, alias: str) -> pl.Expr:
    contiguous = _datetime_ms_expr("date").diff() == CANDLE_INTERVAL_MS
    return (
        pl.when(
            contiguous
            & (pl.col(column) > 0)
            & (pl.col(column).shift(1) > 0)
        )
        .then(pl.col(column).log() - pl.col(column).shift(1).log())
        .otherwise(None)
        .alias(alias)
    )


def _add_derived_features(frame: pl.DataFrame) -> pl.DataFrame:
    frame = _ensure_raw_columns(frame).sort("date")
    derived = frame.with_columns(
        [
            _contiguous_log_change(
                "okx_mark_close", "okx_mark_log_return_15m"
            ),
            _contiguous_log_change(
                "okx_index_close", "okx_index_log_return_15m"
            ),
            _positive_log_ratio(
                "okx_mark_high", "okx_mark_low", "okx_mark_range_log"
            ),
            _positive_log_ratio(
                "okx_index_high", "okx_index_low", "okx_index_range_log"
            ),
            _positive_log_ratio(
                "close", "okx_mark_close", "okx_contract_mark_basis_log"
            ),
            _positive_log_ratio(
                "okx_mark_close",
                "okx_index_close",
                "okx_mark_index_basis_log",
            ),
            pl.when(
                (pl.col("okx_funding_interval_hours") > 0)
                & pl.col("okx_funding_realized_rate").is_not_null()
            )
            .then(
                pl.col("okx_funding_realized_rate")
                * (365.0 * 24.0)
                / pl.col("okx_funding_interval_hours")
            )
            .otherwise(None)
            .alias("okx_funding_realized_annualized"),
            _contiguous_log_change(
                "okx_open_interest_contracts",
                "okx_open_interest_contracts_log_change_15m",
            ),
            _contiguous_log_change(
                "okx_open_interest_usd",
                "okx_open_interest_usd_log_change_15m",
            ),
            pl.when(
                (pl.col("okx_open_interest_usd") >= 0)
                & (pl.col("Trading_Volume") >= 0)
            )
            .then(
                pl.col("okx_open_interest_usd").log1p()
                - pl.col("Trading_Volume").log1p()
            )
            .otherwise(None)
            .alias("okx_open_interest_usd_to_volume_log"),
            pl.when(
                (
                    pl.col("okx_taker_buy_volume_contracts")
                    + pl.col("okx_taker_sell_volume_contracts")
                )
                > 0
            )
            .then(
                (
                    pl.col("okx_taker_buy_volume_contracts")
                    - pl.col("okx_taker_sell_volume_contracts")
                )
                / (
                    pl.col("okx_taker_buy_volume_contracts")
                    + pl.col("okx_taker_sell_volume_contracts")
                )
            )
            .otherwise(None)
            .alias("okx_taker_imbalance"),
            pl.when(pl.col("okx_long_short_account_ratio") > 0)
            .then(pl.col("okx_long_short_account_ratio").log())
            .otherwise(None)
            .alias("okx_long_short_account_ratio_log"),
            pl.when(pl.col("okx_top_trader_account_ratio") > 0)
            .then(pl.col("okx_top_trader_account_ratio").log())
            .otherwise(None)
            .alias("okx_top_trader_account_ratio_log"),
            pl.when(pl.col("okx_top_trader_position_ratio") > 0)
            .then(pl.col("okx_top_trader_position_ratio").log())
            .otherwise(None)
            .alias("okx_top_trader_position_ratio_log"),
        ]
    )
    return derived


def _frames_equal(left: pl.DataFrame, right: pl.DataFrame) -> bool:
    if left.height != right.height or left.columns != right.columns:
        return False
    return left.equals(right)


def _coverage_summary(frame: pl.DataFrame) -> dict[str, Any]:
    coverage: dict[str, Any] = {}
    for family, column in FAMILY_PRIMARY_COLUMN.items():
        if column not in frame.columns:
            coverage[family] = {"rows": 0, "start": None, "end": None}
            continue
        selected = frame.filter(pl.col(column).is_not_null())
        coverage[family] = {
            "rows": selected.height,
            "start": selected["date"].min() if not selected.is_empty() else None,
            "end": selected["date"].max() if not selected.is_empty() else None,
        }
    return coverage


def enrich_symbol_historical_features(
    client: OkxHistoricalClient,
    record: Any,
    output_path: Path,
    *,
    start_ms: int,
    end_ms: int,
    include_funding_archive: bool,
    stage_callback: Callable[[str, str], None] | None = None,
) -> HistoricalFeatureResult:
    original = _read_parquet(output_path)
    frame = original
    stage_status: dict[str, str] = {}
    errors: dict[str, str] = {}

    def run_stage(stage: str, fn: Callable[[], None]) -> None:
        status = "ok"
        try:
            fn()
        except Exception as exc:
            status = "failed"
            errors[stage] = f"{type(exc).__name__}: {exc}"
        stage_status[stage] = status
        if stage_callback is not None:
            stage_callback(stage, status)

    base_start_value = original.select(_datetime_ms_expr("date").min()).item()
    base_end_value = original.select(_datetime_ms_expr("date").max()).item()
    if base_start_value is None or base_end_value is None:
        raise ValueError(f"{output_path.name} has no valid candle timestamps")
    dataset_start_ms = max(start_ms, int(base_start_value))
    closed_end_ms = min(end_ms, int(base_end_value))
    inst_family = str(
        getattr(record, "inst_family", None)
        or getattr(record, "uly", None)
        or ""
    ).strip()
    if not inst_family:
        inst_family = str(record.okx_symbol).removesuffix("-SWAP")

    def mark_stage() -> None:
        nonlocal frame
        feature_start = _latest_non_null_ms(
            frame, "okx_mark_close", dataset_start_ms
        )
        rows = _fetch_array_history(
            client,
            MARK_PRICE_HISTORY_ENDPOINT,
            {"instId": record.okx_symbol, "bar": KLINE_BAR},
            start_ms=feature_start,
            end_ms=closed_end_ms,
            limit=100,
        )
        fresh = _normalize_price_candles(
            rows,
            prefix="okx_mark",
            start_ms=feature_start,
            end_ms=closed_end_ms,
        )
        frame = _overlay_feature_frame(
            frame,
            fresh,
            (
                "okx_mark_open",
                "okx_mark_high",
                "okx_mark_low",
                "okx_mark_close",
            ),
        )

    def index_stage() -> None:
        nonlocal frame
        feature_start = _latest_non_null_ms(
            frame, "okx_index_close", dataset_start_ms
        )
        rows = _fetch_array_history(
            client,
            INDEX_PRICE_HISTORY_ENDPOINT,
            {"instId": inst_family, "bar": KLINE_BAR},
            start_ms=feature_start,
            end_ms=closed_end_ms,
            limit=100,
        )
        fresh = _normalize_price_candles(
            rows,
            prefix="okx_index",
            start_ms=feature_start,
            end_ms=closed_end_ms,
        )
        frame = _overlay_feature_frame(
            frame,
            fresh,
            (
                "okx_index_open",
                "okx_index_high",
                "okx_index_low",
                "okx_index_close",
            ),
        )

    def funding_stage() -> None:
        nonlocal frame
        feature_start = _latest_non_null_ms(
            frame, "okx_funding_realized_rate", dataset_start_ms
        )
        if {
            "okx_funding_realized_rate",
            "okx_funding_interval_hours",
        }.issubset(frame.columns):
            incomplete = frame.filter(
                pl.col("okx_funding_realized_rate").is_not_null()
                & pl.col("okx_funding_interval_hours").is_null()
            )
            if not incomplete.is_empty():
                earliest_incomplete = incomplete.select(
                    _datetime_ms_expr("date").min()
                ).item()
                if earliest_incomplete is not None:
                    feature_start = min(feature_start, int(earliest_incomplete))
        # Pull the preceding settlements as causal context. Without this
        # lookback, the first event inside a requested window has no observable
        # prior interval and early bars cannot inherit the latest settled rate.
        event_start = feature_start - 24 * 60 * 60 * 1000
        archive_rows: list[dict[str, Any]] = []
        if include_funding_archive:
            try:
                archive_rows = _fetch_funding_archive(
                    client,
                    inst_family=inst_family,
                    okx_symbol=record.okx_symbol,
                    start_ms=event_start,
                    end_ms=closed_end_ms,
                )
            except Exception as exc:
                # The archive is explicitly documented as still being
                # backfilled. Preserve that gap in the report, but continue
                # with the recent REST history instead of discarding all
                # funding data for the symbol.
                errors["funding_rate_archive"] = f"{type(exc).__name__}: {exc}"
        rest_rows = _fetch_object_history(
            client,
            FUNDING_RATE_HISTORY_ENDPOINT,
            {"instId": record.okx_symbol},
            timestamp_field="fundingTime",
            start_ms=event_start,
            end_ms=closed_end_ms,
            limit=400,
        )
        events = _normalize_funding_events(
            archive_rows,
            rest_rows,
            start_ms=event_start,
            end_ms=closed_end_ms,
        )
        fresh = _materialize_funding_asof(
            frame,
            events,
            start_ms=feature_start,
            end_ms=closed_end_ms,
        )
        frame = _overlay_feature_frame(
            frame,
            fresh,
            (
                "okx_funding_rate_at_settlement",
                "okx_funding_realized_rate",
                "okx_funding_interval_hours",
                "okx_funding_age_hours",
                "okx_funding_formula_with_rate",
                "okx_funding_method_current_period",
            ),
        )

    def rubik_stage(
        stage: str,
        path: str,
        columns: tuple[str, ...],
        extra_params: dict[str, Any] | None = None,
    ) -> Callable[[], None]:
        def execute() -> None:
            nonlocal frame
            feature_start = _latest_non_null_ms(
                frame, FAMILY_PRIMARY_COLUMN[stage], dataset_start_ms
            )
            rows = _fetch_rubik_history(
                client,
                path,
                {
                    "instId": record.okx_symbol,
                    "period": KLINE_BAR,
                    **(extra_params or {}),
                },
                start_ms=feature_start,
                end_ms=closed_end_ms,
            )
            fresh = _normalize_rubik_rows(
                rows,
                value_columns=columns,
                start_ms=feature_start,
                end_ms=closed_end_ms,
            )
            frame = _overlay_feature_frame(frame, fresh, columns)

        return execute

    run_stage("mark_price", mark_stage)
    run_stage("index_price", index_stage)
    run_stage("funding_rate", funding_stage)
    run_stage(
        "open_interest",
        rubik_stage(
            "open_interest",
            OPEN_INTEREST_HISTORY_ENDPOINT,
            (
                "okx_open_interest_contracts",
                "okx_open_interest_ccy",
                "okx_open_interest_usd",
            ),
        ),
    )
    run_stage(
        "taker_volume",
        rubik_stage(
            "taker_volume",
            TAKER_VOLUME_HISTORY_ENDPOINT,
            (
                "okx_taker_sell_volume_contracts",
                "okx_taker_buy_volume_contracts",
            ),
            {"unit": "1"},
        ),
    )
    run_stage(
        "long_short_account_ratio",
        rubik_stage(
            "long_short_account_ratio",
            LONG_SHORT_ACCOUNT_RATIO_ENDPOINT,
            ("okx_long_short_account_ratio",),
        ),
    )
    run_stage(
        "top_trader_account_ratio",
        rubik_stage(
            "top_trader_account_ratio",
            TOP_TRADER_ACCOUNT_RATIO_ENDPOINT,
            ("okx_top_trader_account_ratio",),
        ),
    )
    run_stage(
        "top_trader_position_ratio",
        rubik_stage(
            "top_trader_position_ratio",
            TOP_TRADER_POSITION_RATIO_ENDPOINT,
            ("okx_top_trader_position_ratio",),
        ),
    )

    frame = _add_derived_features(frame)
    changed = not _frames_equal(original, frame)
    if changed:
        _write_parquet(frame, output_path)

    coverage = _coverage_summary(frame)
    failed = len(errors)
    status = "partial" if failed else "updated" if changed else "unchanged"
    if failed == len(FEATURE_STAGE_IDS):
        status = "failed"
    return HistoricalFeatureResult(
        code=str(record.code),
        okx_symbol=str(record.okx_symbol),
        status=status,
        rows=frame.height,
        output_path=str(output_path),
        changed=changed,
        stage_status_json=json.dumps(stage_status, ensure_ascii=False, sort_keys=True),
        coverage_json=json.dumps(coverage, ensure_ascii=False, sort_keys=True),
        errors_json=json.dumps(errors, ensure_ascii=False, sort_keys=True),
    )


class _StageProgress:
    def __init__(self, symbol_count: int) -> None:
        self._lock = threading.Lock()
        self._seen: set[tuple[str, str]] = set()
        self._counts: Counter[str] = Counter()
        self._bar = tqdm(
            total=symbol_count * len(FEATURE_STAGE_IDS),
            desc="download:okx-history",
            unit="dataset",
        )

    def update(self, code: str, stage: str, status: str) -> None:
        key = (code, stage)
        with self._lock:
            if key in self._seen:
                return
            self._seen.add(key)
            self._counts[status] += 1
            self._bar.update(1)
            self._bar.set_postfix(
                ok=self._counts["ok"],
                failed=self._counts["failed"],
                refresh=False,
            )

    def fill_missing(self, code: str, status: str) -> None:
        for stage in FEATURE_STAGE_IDS:
            self.update(code, stage, status)

    def close(self) -> None:
        self._bar.close()


def run_historical_feature_downloads(
    client: OkxHistoricalClient,
    records: list[Any],
    output_dir: Path,
    *,
    start_ms: int,
    end_ms: int,
    workers: int,
    include_funding_archive: bool,
) -> list[HistoricalFeatureResult]:
    eligible = [
        record
        for record in records
        if (output_dir / f"{record.code}_features.parquet").is_file()
    ]
    if not eligible:
        return []

    progress = _StageProgress(len(eligible))
    results: list[HistoricalFeatureResult] = []

    def worker(record: Any) -> HistoricalFeatureResult:
        output_path = output_dir / f"{record.code}_features.parquet"
        return enrich_symbol_historical_features(
            client,
            record,
            output_path,
            start_ms=start_ms,
            end_ms=end_ms,
            include_funding_archive=include_funding_archive,
            stage_callback=lambda stage, status: progress.update(
                str(record.code), stage, status
            ),
        )

    try:
        with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
            futures = {executor.submit(worker, record): record for record in eligible}
            for future in as_completed(futures):
                record = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    progress.fill_missing(str(record.code), "failed")
                    results.append(
                        HistoricalFeatureResult(
                            code=str(record.code),
                            okx_symbol=str(record.okx_symbol),
                            status="failed",
                            rows=0,
                            output_path=str(
                                output_dir / f"{record.code}_features.parquet"
                            ),
                            changed=False,
                            stage_status_json="{}",
                            coverage_json="{}",
                            errors_json=json.dumps(
                                {"worker": f"{type(exc).__name__}: {exc}"},
                                ensure_ascii=False,
                            ),
                        )
                    )
    finally:
        progress.close()

    return sorted(results, key=lambda result: result.okx_symbol)


def result_rows(results: list[HistoricalFeatureResult]) -> list[dict[str, Any]]:
    return [asdict(result) for result in results]
