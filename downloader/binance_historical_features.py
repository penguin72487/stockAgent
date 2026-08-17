from __future__ import annotations

import json
import os
import threading
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


KLINE_BAR = "1m"
STATISTICS_PERIOD = "5m"
CANDLE_INTERVAL_MS = 60 * 1000
STATISTICS_INTERVAL_MS = 5 * 60 * 1000
FEATURE_SCHEMA_VERSION = 1

MARK_PRICE_KLINES_ENDPOINT = "/fapi/v1/markPriceKlines"
INDEX_PRICE_KLINES_ENDPOINT = "/fapi/v1/indexPriceKlines"
PREMIUM_INDEX_KLINES_ENDPOINT = "/fapi/v1/premiumIndexKlines"
FUNDING_RATE_ENDPOINT = "/fapi/v1/fundingRate"
OPEN_INTEREST_ENDPOINT = "/futures/data/openInterestHist"
GLOBAL_RATIO_ENDPOINT = "/futures/data/globalLongShortAccountRatio"
TOP_ACCOUNT_RATIO_ENDPOINT = "/futures/data/topLongShortAccountRatio"
TOP_POSITION_RATIO_ENDPOINT = "/futures/data/topLongShortPositionRatio"
TAKER_RATIO_ENDPOINT = "/futures/data/takerlongshortRatio"
BASIS_ENDPOINT = "/futures/data/basis"

KLINE_LIMIT = 499
KLINE_REQUEST_WEIGHT = 2.0
FUNDING_LIMIT = 1000
SHORT_HISTORY_LIMIT = 500
# Binance rejects queries older than its rolling 30-day statistics window.
# One hour of guard avoids a race at the exact moving boundary.
SHORT_HISTORY_RETENTION_MS = 30 * 24 * 60 * 60 * 1000 - 60 * 60 * 1000

FEATURE_STAGE_IDS = (
    "mark_price",
    "index_price",
    "premium_index",
    "funding_rate",
    "open_interest",
    "global_long_short_account_ratio",
    "top_trader_account_ratio",
    "top_trader_position_ratio",
    "taker_buy_sell_volume",
    "basis",
)

RAW_FEATURE_COLUMNS = (
    "binance_mark_open",
    "binance_mark_high",
    "binance_mark_low",
    "binance_mark_close",
    "binance_index_open",
    "binance_index_high",
    "binance_index_low",
    "binance_index_close",
    "binance_premium_open",
    "binance_premium_high",
    "binance_premium_low",
    "binance_premium_close",
    "binance_funding_rate",
    "binance_funding_mark_price",
    "binance_funding_interval_hours",
    "binance_funding_age_hours",
    "binance_open_interest_contracts",
    "binance_open_interest_value_usd",
    "binance_cmc_circulating_supply",
    "binance_global_long_account_share",
    "binance_global_short_account_share",
    "binance_global_long_short_account_ratio",
    "binance_top_long_account_share",
    "binance_top_short_account_share",
    "binance_top_long_short_account_ratio",
    "binance_top_long_position_share",
    "binance_top_short_position_share",
    "binance_top_long_short_position_ratio",
    "binance_taker_buy_volume",
    "binance_taker_sell_volume",
    "binance_taker_buy_sell_ratio",
    "binance_basis_index_price",
    "binance_basis_futures_price",
    "binance_basis_value",
    "binance_basis_rate",
    "binance_basis_annualized_rate",
)

DERIVED_FEATURE_COLUMNS = (
    "binance_mark_log_return_1m",
    "binance_index_log_return_1m",
    "binance_mark_range_log",
    "binance_index_range_log",
    "binance_contract_mark_basis_log",
    "binance_mark_index_basis_log",
    "binance_funding_annualized",
    "binance_open_interest_contracts_log_change_5m",
    "binance_open_interest_value_log_change_5m",
    "binance_open_interest_value_to_quote_volume_log",
    "binance_global_long_short_account_ratio_log",
    "binance_top_long_short_account_ratio_log",
    "binance_top_long_short_position_ratio_log",
    "binance_taker_imbalance",
)

ALL_FEATURE_COLUMNS = (*RAW_FEATURE_COLUMNS, *DERIVED_FEATURE_COLUMNS)

FAMILY_PRIMARY_COLUMN = {
    "mark_price": "binance_mark_close",
    "index_price": "binance_index_close",
    "premium_index": "binance_premium_close",
    "funding_rate": "binance_funding_rate",
    "open_interest": "binance_open_interest_value_usd",
    "global_long_short_account_ratio": ("binance_global_long_short_account_ratio"),
    "top_trader_account_ratio": "binance_top_long_short_account_ratio",
    "top_trader_position_ratio": "binance_top_long_short_position_ratio",
    "taker_buy_sell_volume": "binance_taker_buy_volume",
    "basis": "binance_basis_rate",
}


HISTORICAL_FEATURE_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "id": "trade_candles_1m",
        "download_status": "included_existing",
        "endpoint": "/fapi/v1/klines",
        "history_contract": "full available history; completed one-minute bars",
        "grain": "instrument_1m",
        "model_role": "OHLCV, trade count and taker-buy volume source of truth",
    },
    {
        "id": "mark_price_candles_1m",
        "download_status": "included",
        "endpoint": MARK_PRICE_KLINES_ENDPOINT,
        "history_contract": "full available history",
        "grain": "instrument_1m",
        "model_role": "mark return, range and contract-mark basis",
    },
    {
        "id": "index_price_candles_1m",
        "download_status": "included",
        "endpoint": INDEX_PRICE_KLINES_ENDPOINT,
        "history_contract": "full available history",
        "grain": "pair_1m",
        "model_role": "index return, range and mark-index basis",
    },
    {
        "id": "premium_index_candles_1m",
        "download_status": "included",
        "endpoint": PREMIUM_INDEX_KLINES_ENDPOINT,
        "history_contract": "full available history",
        "grain": "instrument_1m",
        "model_role": "official premium-index path",
    },
    {
        "id": "funding_rate_history",
        "download_status": "included",
        "endpoint": FUNDING_RATE_ENDPOINT,
        "history_contract": "full available settlement history",
        "grain": "instrument_settlement_event_asof_1m",
        "model_role": "realized funding, interval, age and annualized carry",
    },
    {
        "id": "open_interest_5m",
        "download_status": "included_rolling_30d",
        "endpoint": OPEN_INTEREST_ENDPOINT,
        "history_contract": "rolling latest 30 days; append-only capture preserves future vintages",
        "grain": "instrument_native_5m_aligned_to_1m",
        "model_role": "OI contracts/value, supply and causal changes",
    },
    {
        "id": "global_long_short_account_ratio_5m",
        "download_status": "included_rolling_30d",
        "endpoint": GLOBAL_RATIO_ENDPOINT,
        "history_contract": "rolling latest 30 days",
        "grain": "instrument_native_5m_aligned_to_1m",
        "model_role": "all-trader account-count positioning",
    },
    {
        "id": "top_trader_account_ratio_5m",
        "download_status": "included_rolling_30d",
        "endpoint": TOP_ACCOUNT_RATIO_ENDPOINT,
        "history_contract": "rolling latest 30 days",
        "grain": "instrument_native_5m_aligned_to_1m",
        "model_role": "top-trader account-count positioning",
    },
    {
        "id": "top_trader_position_ratio_5m",
        "download_status": "included_rolling_30d",
        "endpoint": TOP_POSITION_RATIO_ENDPOINT,
        "history_contract": "rolling latest 30 days",
        "grain": "instrument_native_5m_aligned_to_1m",
        "model_role": "top-trader position-notional positioning",
    },
    {
        "id": "taker_buy_sell_volume_5m",
        "download_status": "included_rolling_30d",
        "endpoint": TAKER_RATIO_ENDPOINT,
        "history_contract": "rolling latest 30 days",
        "grain": "instrument_native_5m_aligned_to_1m",
        "model_role": "aggressive order-flow volume and imbalance",
    },
    {
        "id": "basis_5m",
        "download_status": "included_rolling_30d",
        "endpoint": BASIS_ENDPOINT,
        "history_contract": "rolling latest 30 days",
        "grain": "pair_contract_type_native_5m_aligned_to_1m",
        "model_role": "official index/futures basis",
    },
    {
        "id": "current_snapshots_and_order_book",
        "download_status": "separate_prospective_capture",
        "endpoint": "/fapi/v1/premiumIndex, /fapi/v1/openInterest, /fapi/v1/depth",
        "history_contract": "current snapshot only",
        "grain": "instrument_observation",
        "model_role": "not back-projected; usable only after local observed_at",
    },
    {
        "id": "agg_trades_and_websocket_liquidations",
        "download_status": "separate_high_volume_stream",
        "endpoint": "/fapi/v1/aggTrades and forceOrder stream",
        "history_contract": "recent REST / prospective stream",
        "grain": "event",
        "model_role": "microstructure archive; independently storage-budgeted",
    },
)


class BinanceHistoricalClient(Protocol):
    def get(self, path: str, params: dict[str, Any], *, weight: float) -> Any: ...


@dataclass(slots=True)
class HistoricalFeatureResult:
    code: str
    binance_symbol: str
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
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "bar": KLINE_BAR,
        "statistics_period": STATISTICS_PERIOD,
        "causal_clock": {
            "event_ts": "timestamp assigned by Binance to the source observation",
            "published_at": "source publication time when supplied; otherwise event boundary",
            "observed_at": "first successful local capture time, retained in run receipts",
            "available_at": "max(published_at, observed_at) for snapshots; historical series use the documented event boundary",
            "decision_rule": "event_ts <= completed bar close; model execution is no earlier than the next bar open",
            "missing_rule": "retain null and mask; never forward-project a present snapshot into history",
        },
        "catalog": list(HISTORICAL_FEATURE_CATALOG),
    }


def _ms_to_date_string(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _datetime_ms_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.to_datetime(strict=False)
        .cast(pl.Int64)
        .floordiv(1_000)
    )


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path, memory_map=True))


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.features.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), temporary, compression="snappy", write_statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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


def _timestamp_from_row(row: Any, field: str | int) -> int | None:
    try:
        value = row[field] if isinstance(field, int) else row.get(field)
        return int(value)
    except (AttributeError, IndexError, KeyError, TypeError, ValueError):
        return None


def _fetch_forward(
    client: BinanceHistoricalClient,
    path: str,
    base_params: dict[str, Any],
    *,
    start_ms: int,
    end_ms: int,
    limit: int,
    timestamp_field: str | int,
    weight: float,
) -> list[Any]:
    if start_ms > end_ms:
        return []
    rows: list[Any] = []
    cursor = start_ms
    seen: set[int] = set()
    while cursor <= end_ms:
        payload = client.get(
            path,
            {
                **base_params,
                "startTime": str(cursor),
                "endTime": str(end_ms),
                "limit": str(limit),
            },
            weight=weight,
        )
        if not isinstance(payload, list) or not payload:
            break
        timestamps = [
            timestamp
            for row in payload
            if (timestamp := _timestamp_from_row(row, timestamp_field)) is not None
        ]
        if not timestamps:
            break
        rows.extend(payload)
        latest = max(timestamps)
        if latest in seen or latest < cursor:
            raise RuntimeError(f"Binance pagination made no progress for {path}")
        seen.add(latest)
        cursor = latest + 1
        if len(payload) < limit:
            break
    return rows


def _normalize_price_klines(
    raw_rows: list[Any],
    *,
    prefix: str,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        ts = int(row[0])
        if not (start_ms <= ts <= end_ms):
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


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_object_rows(
    raw_rows: list[Any],
    *,
    timestamp_field: str,
    field_map: dict[str, str],
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, dict) or raw.get(timestamp_field) in {None, ""}:
            continue
        ts = int(raw[timestamp_field])
        if not (start_ms <= ts <= end_ms):
            continue
        item: dict[str, Any] = {"date": _ms_to_date_string(ts)}
        for source, target in field_map.items():
            item[target] = _float_or_none(raw.get(source))
        rows.append(item)
    columns = ["date", *field_map.values()]
    if not rows:
        return pl.DataFrame({column: [] for column in columns})
    return (
        pl.DataFrame(rows)
        .sort("date")
        .unique(subset=["date"], keep="last", maintain_order=True)
    )


def _normalize_funding_events(
    raw_rows: list[Any],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    by_timestamp: dict[int, dict[str, Any]] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict) or raw.get("fundingTime") in {None, ""}:
            continue
        ts = int(raw["fundingTime"])
        if start_ms <= ts <= end_ms:
            by_timestamp[ts] = {
                "ts": ts,
                "binance_funding_rate": _float_or_none(raw.get("fundingRate")),
                "binance_funding_mark_price": _float_or_none(raw.get("markPrice")),
            }
    events = [by_timestamp[key] for key in sorted(by_timestamp)]
    previous_ts: int | None = None
    for event in events:
        ts = int(event["ts"])
        event["binance_funding_interval_hours"] = (
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
        "binance_funding_rate",
        "binance_funding_mark_price",
        "binance_funding_interval_hours",
        "binance_funding_age_hours",
    ]
    if not events:
        return pl.DataFrame({column: [] for column in columns})
    selected = (
        frame.select(["date", _datetime_ms_expr("date").alias("_bar_open_ms")])
        .filter(
            (pl.col("_bar_open_ms") >= start_ms) & (pl.col("_bar_open_ms") <= end_ms)
        )
        .sort("_bar_open_ms")
    )
    if selected.is_empty():
        return pl.DataFrame({column: [] for column in columns})

    bar_close_ms = selected["_bar_open_ms"].to_numpy() + CANDLE_INTERVAL_MS
    event_ts = np.asarray([int(event["ts"]) for event in events], dtype=np.int64)
    indices = np.searchsorted(event_ts, bar_close_ms, side="right") - 1
    valid = indices >= 0

    def values(name: str) -> list[float | None]:
        source = [event.get(name) for event in events]
        return [
            source[index] if is_valid else None
            for is_valid, index in zip(valid.tolist(), indices.tolist(), strict=True)
        ]

    age_hours = [
        (int(close_ms) - int(event_ts[index])) / (60 * 60 * 1000) if is_valid else None
        for is_valid, index, close_ms in zip(
            valid.tolist(), indices.tolist(), bar_close_ms.tolist(), strict=True
        )
    ]
    return pl.DataFrame(
        {
            "date": selected["date"],
            "binance_funding_rate": values("binance_funding_rate"),
            "binance_funding_mark_price": values("binance_funding_mark_price"),
            "binance_funding_interval_hours": values("binance_funding_interval_hours"),
            "binance_funding_age_hours": age_hours,
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
        pl.when(contiguous & (pl.col(column) > 0) & (pl.col(column).shift(1) > 0))
        .then(pl.col(column).log() - pl.col(column).shift(1).log())
        .otherwise(None)
        .alias(alias)
    )


def _observation_log_change(column: str, alias: str) -> pl.Expr:
    """Change between native sparse observations, emitted only on observation rows."""

    previous_observation = pl.col(column).forward_fill().shift(1)
    observation_ts = pl.when(pl.col(column).is_not_null()).then(
        _datetime_ms_expr("date")
    )
    previous_observation_ts = observation_ts.forward_fill().shift(1)
    return (
        pl.when(
            pl.col(column).is_not_null()
            & (pl.col(column) > 0)
            & (previous_observation > 0)
            & (
                (_datetime_ms_expr("date") - previous_observation_ts)
                <= STATISTICS_INTERVAL_MS
            )
        )
        .then(pl.col(column).log() - previous_observation.log())
        .otherwise(None)
        .alias(alias)
    )


def _add_derived_features(frame: pl.DataFrame) -> pl.DataFrame:
    frame = _ensure_raw_columns(frame).sort("date")
    return frame.with_columns(
        [
            _contiguous_log_change("binance_mark_close", "binance_mark_log_return_1m"),
            _contiguous_log_change(
                "binance_index_close", "binance_index_log_return_1m"
            ),
            _positive_log_ratio(
                "binance_mark_high", "binance_mark_low", "binance_mark_range_log"
            ),
            _positive_log_ratio(
                "binance_index_high",
                "binance_index_low",
                "binance_index_range_log",
            ),
            _positive_log_ratio(
                "close", "binance_mark_close", "binance_contract_mark_basis_log"
            ),
            _positive_log_ratio(
                "binance_mark_close",
                "binance_index_close",
                "binance_mark_index_basis_log",
            ),
            pl.when(
                (pl.col("binance_funding_interval_hours") > 0)
                & pl.col("binance_funding_rate").is_not_null()
            )
            .then(
                pl.col("binance_funding_rate")
                * (365.0 * 24.0)
                / pl.col("binance_funding_interval_hours")
            )
            .otherwise(None)
            .alias("binance_funding_annualized"),
            _observation_log_change(
                "binance_open_interest_contracts",
                "binance_open_interest_contracts_log_change_5m",
            ),
            _observation_log_change(
                "binance_open_interest_value_usd",
                "binance_open_interest_value_log_change_5m",
            ),
            pl.when(
                (pl.col("binance_open_interest_value_usd") >= 0)
                & (pl.col("binance_volume_quote") >= 0)
            )
            .then(
                pl.col("binance_open_interest_value_usd").log1p()
                - pl.col("binance_volume_quote").log1p()
            )
            .otherwise(None)
            .alias("binance_open_interest_value_to_quote_volume_log"),
            pl.when(pl.col("binance_global_long_short_account_ratio") > 0)
            .then(pl.col("binance_global_long_short_account_ratio").log())
            .otherwise(None)
            .alias("binance_global_long_short_account_ratio_log"),
            pl.when(pl.col("binance_top_long_short_account_ratio") > 0)
            .then(pl.col("binance_top_long_short_account_ratio").log())
            .otherwise(None)
            .alias("binance_top_long_short_account_ratio_log"),
            pl.when(pl.col("binance_top_long_short_position_ratio") > 0)
            .then(pl.col("binance_top_long_short_position_ratio").log())
            .otherwise(None)
            .alias("binance_top_long_short_position_ratio_log"),
            pl.when(
                (
                    pl.col("binance_taker_buy_volume")
                    + pl.col("binance_taker_sell_volume")
                )
                > 0
            )
            .then(
                (
                    pl.col("binance_taker_buy_volume")
                    - pl.col("binance_taker_sell_volume")
                )
                / (
                    pl.col("binance_taker_buy_volume")
                    + pl.col("binance_taker_sell_volume")
                )
            )
            .otherwise(None)
            .alias("binance_taker_imbalance"),
        ]
    )


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
    client: BinanceHistoricalClient,
    record: Any,
    output_path: Path,
    *,
    start_ms: int,
    end_ms: int,
    observed_at_ms: int | None = None,
    stage_callback: Callable[[str, str], None] | None = None,
) -> HistoricalFeatureResult:
    original = _read_parquet(output_path)
    frame = original
    stage_status: dict[str, str] = {}
    errors: dict[str, str] = {}

    base_start = original.select(_datetime_ms_expr("date").min()).item()
    base_end = original.select(_datetime_ms_expr("date").max()).item()
    if base_start is None or base_end is None:
        raise ValueError(f"{output_path.name} has no valid candle timestamps")
    dataset_start_ms = max(start_ms, int(base_start))
    closed_end_ms = min(end_ms, int(base_end))
    observation_ms = observed_at_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    short_start_floor = observation_ms - SHORT_HISTORY_RETENTION_MS

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

    def price_stage(
        stage: str,
        path: str,
        prefix: str,
        base_params: dict[str, Any],
    ) -> Callable[[], None]:
        def execute() -> None:
            nonlocal frame
            feature_start = _latest_non_null_ms(
                frame, FAMILY_PRIMARY_COLUMN[stage], dataset_start_ms
            )
            rows = _fetch_forward(
                client,
                path,
                {**base_params, "interval": KLINE_BAR},
                start_ms=feature_start,
                end_ms=closed_end_ms,
                limit=KLINE_LIMIT,
                timestamp_field=0,
                weight=KLINE_REQUEST_WEIGHT,
            )
            fresh = _normalize_price_klines(
                rows,
                prefix=prefix,
                start_ms=feature_start,
                end_ms=closed_end_ms,
            )
            frame = _overlay_feature_frame(
                frame,
                fresh,
                (
                    f"{prefix}_open",
                    f"{prefix}_high",
                    f"{prefix}_low",
                    f"{prefix}_close",
                ),
            )

        return execute

    def funding_stage() -> None:
        nonlocal frame
        feature_start = _latest_non_null_ms(
            frame, FAMILY_PRIMARY_COLUMN["funding_rate"], dataset_start_ms
        )
        # Pull one day of context so the first selected bar receives the latest
        # settlement and the first in-window event can infer its interval.
        event_start = max(dataset_start_ms, feature_start - 24 * 60 * 60 * 1000)
        rows = _fetch_forward(
            client,
            FUNDING_RATE_ENDPOINT,
            {"symbol": record.binance_symbol},
            start_ms=event_start,
            end_ms=closed_end_ms + CANDLE_INTERVAL_MS,
            limit=FUNDING_LIMIT,
            timestamp_field="fundingTime",
            weight=1.0,
        )
        events = _normalize_funding_events(
            rows,
            start_ms=event_start,
            end_ms=closed_end_ms + CANDLE_INTERVAL_MS,
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
                "binance_funding_rate",
                "binance_funding_mark_price",
                "binance_funding_interval_hours",
                "binance_funding_age_hours",
            ),
        )

    def rolling_stage(
        stage: str,
        path: str,
        field_map: dict[str, str],
        base_params: dict[str, Any],
    ) -> Callable[[], None]:
        def execute() -> None:
            nonlocal frame
            feature_start = max(
                short_start_floor,
                _latest_non_null_ms(
                    frame, FAMILY_PRIMARY_COLUMN[stage], dataset_start_ms
                ),
            )
            if feature_start > closed_end_ms:
                return
            rows = _fetch_forward(
                client,
                path,
                {**base_params, "period": STATISTICS_PERIOD},
                start_ms=feature_start,
                end_ms=closed_end_ms,
                limit=SHORT_HISTORY_LIMIT,
                timestamp_field="timestamp",
                # The futures/data routes historically reported zero request
                # weight, but the shared limiter requires a positive cost and
                # a conservative unit also protects against documentation drift.
                weight=1.0,
            )
            fresh = _normalize_object_rows(
                rows,
                timestamp_field="timestamp",
                field_map=field_map,
                start_ms=feature_start,
                end_ms=closed_end_ms,
            )
            frame = _overlay_feature_frame(frame, fresh, tuple(field_map.values()))

        return execute

    symbol_params = {"symbol": record.binance_symbol}
    pair = str(getattr(record, "pair", None) or record.binance_symbol)
    run_stage(
        "mark_price",
        price_stage(
            "mark_price",
            MARK_PRICE_KLINES_ENDPOINT,
            "binance_mark",
            symbol_params,
        ),
    )
    run_stage(
        "index_price",
        price_stage(
            "index_price",
            INDEX_PRICE_KLINES_ENDPOINT,
            "binance_index",
            {"pair": pair},
        ),
    )
    run_stage(
        "premium_index",
        price_stage(
            "premium_index",
            PREMIUM_INDEX_KLINES_ENDPOINT,
            "binance_premium",
            symbol_params,
        ),
    )
    run_stage("funding_rate", funding_stage)
    run_stage(
        "open_interest",
        rolling_stage(
            "open_interest",
            OPEN_INTEREST_ENDPOINT,
            {
                "sumOpenInterest": "binance_open_interest_contracts",
                "sumOpenInterestValue": "binance_open_interest_value_usd",
                "CMCCirculatingSupply": "binance_cmc_circulating_supply",
            },
            symbol_params,
        ),
    )
    run_stage(
        "global_long_short_account_ratio",
        rolling_stage(
            "global_long_short_account_ratio",
            GLOBAL_RATIO_ENDPOINT,
            {
                "longAccount": "binance_global_long_account_share",
                "shortAccount": "binance_global_short_account_share",
                "longShortRatio": "binance_global_long_short_account_ratio",
            },
            symbol_params,
        ),
    )
    run_stage(
        "top_trader_account_ratio",
        rolling_stage(
            "top_trader_account_ratio",
            TOP_ACCOUNT_RATIO_ENDPOINT,
            {
                "longAccount": "binance_top_long_account_share",
                "shortAccount": "binance_top_short_account_share",
                "longShortRatio": "binance_top_long_short_account_ratio",
            },
            symbol_params,
        ),
    )
    run_stage(
        "top_trader_position_ratio",
        rolling_stage(
            "top_trader_position_ratio",
            TOP_POSITION_RATIO_ENDPOINT,
            {
                "longAccount": "binance_top_long_position_share",
                "shortAccount": "binance_top_short_position_share",
                "longShortRatio": "binance_top_long_short_position_ratio",
            },
            symbol_params,
        ),
    )
    run_stage(
        "taker_buy_sell_volume",
        rolling_stage(
            "taker_buy_sell_volume",
            TAKER_RATIO_ENDPOINT,
            {
                "buyVol": "binance_taker_buy_volume",
                "sellVol": "binance_taker_sell_volume",
                "buySellRatio": "binance_taker_buy_sell_ratio",
            },
            symbol_params,
        ),
    )
    run_stage(
        "basis",
        rolling_stage(
            "basis",
            BASIS_ENDPOINT,
            {
                "indexPrice": "binance_basis_index_price",
                "futuresPrice": "binance_basis_futures_price",
                "basis": "binance_basis_value",
                "basisRate": "binance_basis_rate",
                "annualizedBasisRate": "binance_basis_annualized_rate",
            },
            {"pair": pair, "contractType": "PERPETUAL"},
        ),
    )

    frame = _add_derived_features(frame)
    changed = not original.equals(frame)
    if changed:
        _write_parquet_atomic(frame, output_path)
    coverage = _coverage_summary(frame)
    failed = len(errors)
    status = "partial" if failed else "updated" if changed else "unchanged"
    if failed == len(FEATURE_STAGE_IDS):
        status = "failed"
    return HistoricalFeatureResult(
        code=str(record.code),
        binance_symbol=str(record.binance_symbol),
        status=status,
        rows=frame.height,
        output_path=str(output_path),
        changed=changed,
        stage_status_json=json.dumps(stage_status, ensure_ascii=False, sort_keys=True),
        coverage_json=json.dumps(coverage, ensure_ascii=False, sort_keys=True),
        errors_json=json.dumps(errors, ensure_ascii=False, sort_keys=True),
    )


class _StageProgress:
    def __init__(
        self,
        symbol_count: int,
        external_callback: Callable[[str, str, str], None] | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._seen: set[tuple[str, str]] = set()
        self._counts: Counter[str] = Counter()
        self._external_callback = external_callback
        self._bar = tqdm(
            total=symbol_count * len(FEATURE_STAGE_IDS),
            desc="download:binance-history",
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
        if self._external_callback is not None:
            self._external_callback(code, stage, status)

    def fill_missing(self, code: str, status: str) -> None:
        for stage in FEATURE_STAGE_IDS:
            self.update(code, stage, status)

    def close(self) -> None:
        self._bar.close()


def run_historical_feature_downloads(
    client: BinanceHistoricalClient,
    records: list[Any],
    output_dir: Path,
    *,
    start_ms: int,
    end_ms: int,
    workers: int,
    observed_at_ms: int | None = None,
    stage_progress_callback: Callable[[str, str, str], None] | None = None,
) -> list[HistoricalFeatureResult]:
    eligible = [
        record
        for record in records
        if (output_dir / f"{record.code}_features.parquet").is_file()
    ]
    if not eligible:
        return []
    progress = _StageProgress(len(eligible), external_callback=stage_progress_callback)
    results: list[HistoricalFeatureResult] = []

    def worker(record: Any) -> HistoricalFeatureResult:
        return enrich_symbol_historical_features(
            client,
            record,
            output_dir / f"{record.code}_features.parquet",
            start_ms=start_ms,
            end_ms=end_ms,
            observed_at_ms=observed_at_ms,
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
                            binance_symbol=str(record.binance_symbol),
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
    return sorted(results, key=lambda result: result.binance_symbol)


def result_rows(results: list[HistoricalFeatureResult]) -> list[dict[str, Any]]:
    return [asdict(result) for result in results]
