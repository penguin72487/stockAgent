"""Causal dataset for recent TAIFEX TX and TXO transaction files.

The public source labels trades only to whole seconds and has no unique trade
identifier.  This module therefore aggregates every completed second, emits a
decision on a configurable second grid, and records the first TX trade from a
strictly later second as the execution price.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Final, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl

from stockagent.data.shioaji_capture_parts import (
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)


TICK_DATASET_SCHEMA_VERSION: Final[int] = 5
TICK_FEATURE_CONTRACT_VERSION: Final[int] = 2
TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
DEPTH_LEVELS: Final[int] = 5
SHIOAJI_BIDASK_SOURCE: Final[str] = "shioaji_bidask"
TAIFEX_TRADE_PROXY_SOURCE: Final[str] = "taifex_next_trade_proxy"
TICK_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "tx_log_return_1s",
    "tx_log_return_5s",
    "tx_log_return_60s",
    "tx_range_bps_1s",
    "tx_log_volume_1s",
    "tx_log_volume_60s",
    "tx_log_trade_count_1s",
    "option_call_log_volume_1s",
    "option_put_log_volume_1s",
    "option_call_put_volume_imbalance_60s",
    "option_atm_call_put_volume_imbalance_60s",
    "option_log_premium_notional_60s",
    "option_call_premium_share_60s",
    "option_atm_volume_share_60s",
    "option_volume_weighted_abs_moneyness_60s",
    "seconds_from_open_sin",
    "seconds_from_open_cos",
    "seconds_to_close_fraction",
)
OPTION_LEG_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "option_leg_log_premium",
    "option_leg_signed_otm_fraction",
    "option_leg_intrinsic_fraction",
    "option_leg_is_call",
    "option_leg_is_put",
    "option_leg_days_to_expiry_scaled",
)
OPTION_TICK_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *TICK_FEATURE_COLUMNS,
    *OPTION_LEG_FEATURE_COLUMNS,
)
_OPTION_SERIES_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<year>\d{4})(?P<month>\d{2})(?:(?P<weekday>[WF])(?P<ordinal>[1-5]))?$"
)


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _third_wednesday(year: int, month: int) -> date:
    first = date(year, month, 1)
    first_wednesday = first + timedelta(days=(2 - first.weekday()) % 7)
    return first_wednesday + timedelta(days=14)


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    first = date(year, month, 1)
    first_match = first + timedelta(days=(weekday - first.weekday()) % 7)
    result = first_match + timedelta(days=7 * (ordinal - 1))
    if result.month != month:
        raise ValueError(
            f"month {year:04d}-{month:02d} has no weekday={weekday} ordinal={ordinal}"
        )
    return result


def taifex_option_expiry(series: str) -> date:
    """Resolve regular, Wednesday-weekly, and Friday-weekly TXO codes."""

    match = _OPTION_SERIES_RE.fullmatch(str(series).strip().upper())
    if match is None:
        raise ValueError(f"unsupported TXO delivery month/week code: {series!r}")
    year = int(match.group("year"))
    month = int(match.group("month"))
    weekday_code = match.group("weekday")
    if weekday_code is None:
        return _third_wednesday(year, month)
    weekday = 2 if weekday_code == "W" else 4
    return _nth_weekday(year, month, weekday, int(match.group("ordinal")))


def taifex_front_month(trading_date: date) -> str:
    """Select the nearest unexpired regular monthly TX contract causally."""

    year = trading_date.year
    month = trading_date.month
    if trading_date > _third_wednesday(year, month):
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return f"{year:04d}{month:02d}"


def _validate_builder_parameters(
    *,
    session: str,
    decision_interval_seconds: int,
    rolling_window_seconds: int,
    warmup_seconds: int,
) -> None:
    if session != "day":
        raise ValueError(
            "tw_index_derivatives_tick v1 supports session='day' only; "
            "night-session boundaries require a separate cross-calendar clock"
        )
    if decision_interval_seconds < 1:
        raise ValueError("decision_interval_seconds must be positive")
    if rolling_window_seconds < 1:
        raise ValueError("rolling_window_seconds must be positive")
    if warmup_seconds < rolling_window_seconds:
        raise ValueError("warmup_seconds must cover rolling_window_seconds")


def _sum_when(column: str, predicate: pl.Expr) -> pl.Expr:
    return pl.when(predicate).then(pl.col(column)).otherwise(0.0).sum()


def _select_causal_atm_option_pair(
    option_rows: pl.DataFrame,
    *,
    trading_date: date,
    selection_ts: datetime,
    underlying_price: float,
) -> tuple[str, date, float]:
    """Select one Call/Put pair using only trades observed by selection_ts."""

    observed = option_rows.filter(pl.col("event_ts") <= selection_ts)
    if observed.is_empty():
        raise ValueError(f"{trading_date}: no TXO trade observed by {selection_ts}")
    expiries: list[tuple[date, str]] = []
    for raw_series in observed["delivery_month_week"].unique().to_list():
        series = str(raw_series)
        try:
            expiry = taifex_option_expiry(series)
        except ValueError:
            continue
        if expiry >= trading_date:
            expiries.append((expiry, series))
    for expiry, series in sorted(expiries):
        paired_strikes = (
            observed.filter(pl.col("delivery_month_week") == series)
            .group_by("strike_price")
            .agg(pl.col("option_right").n_unique().alias("rights"))
            .filter(pl.col("rights") == 2)["strike_price"]
            .to_list()
        )
        if paired_strikes:
            strike = min(
                (float(value) for value in paired_strikes),
                key=lambda value: (abs(value - underlying_price), value),
            )
            return series, expiry, strike
    raise ValueError(
        f"{trading_date}: no unexpired Call/Put strike pair observed by {selection_ts}"
    )


def _attach_option_pair_prices(
    grid: pl.DataFrame,
    option_rows: pl.DataFrame,
    *,
    series: str,
    strike: float,
) -> pl.DataFrame:
    selected = option_rows.filter(
        (pl.col("delivery_month_week") == series)
        & (pl.col("strike_price") == float(strike))
    )
    call = pl.col("option_right") == "C"
    put = pl.col("option_right") == "P"
    seconds = (
        selected.group_by("event_ts")
        .agg(
            _sum_when("premium_notional", call).alias("call_premium_num"),
            _sum_when("matched_quantity_equivalent", call).alias("call_qty"),
            _sum_when("premium_notional", put).alias("put_premium_num"),
            _sum_when("matched_quantity_equivalent", put).alias("put_qty"),
        )
        .with_columns(
            pl.when(pl.col("call_qty") > 0.0)
            .then(pl.col("call_premium_num") / pl.col("call_qty"))
            .alias("call_trade_price"),
            pl.when(pl.col("put_qty") > 0.0)
            .then(pl.col("put_premium_num") / pl.col("put_qty"))
            .alias("put_trade_price"),
        )
        .with_columns(
            pl.when(pl.col("call_trade_price").is_not_null())
            .then(pl.col("event_ts"))
            .alias("call_trade_event_ts"),
            pl.when(pl.col("put_trade_price").is_not_null())
            .then(pl.col("event_ts"))
            .alias("put_trade_event_ts"),
        )
        .select(
            [
                "event_ts",
                "call_trade_price",
                "put_trade_price",
                "call_trade_event_ts",
                "put_trade_event_ts",
            ]
        )
    )
    return (
        grid.join(seconds, on="event_ts", how="left")
        .with_columns(
            pl.col("call_trade_price")
            .fill_null(strategy="forward")
            .alias("call_last_price"),
            pl.col("put_trade_price")
            .fill_null(strategy="forward")
            .alias("put_last_price"),
        )
        .with_columns(
            pl.col("call_trade_price")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("call_execution_price"),
            pl.col("put_trade_price")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("put_execution_price"),
            pl.col("call_trade_event_ts")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("call_execution_event_ts"),
            pl.col("put_trade_event_ts")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("put_execution_event_ts"),
        )
    )


def _book_depth_columns(prefix: str) -> tuple[str, ...]:
    return tuple(
        f"{prefix}_{side}_{kind}_{level}"
        for side in ("bid", "ask")
        for kind in ("price", "volume")
        for level in range(1, DEPTH_LEVELS + 1)
    )


def _valid_book_arrays(
    frame: pl.DataFrame,
    *,
    max_transport_delay_ms: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    required = {
        "event_seq",
        "code",
        "exchange_ts_ns",
        "receive_ts_ns",
        "simtrade",
        *{
            f"{side}_{kind}_{level}"
            for side in ("bid", "ask")
            for kind in ("price", "volume")
            for level in range(1, DEPTH_LEVELS + 1)
        },
    }
    if missing := required.difference(frame.columns):
        raise ValueError(f"captured BidAsk is missing columns: {sorted(missing)}")
    ordered = frame.sort(["receive_ts_ns", "event_seq"])
    receive = ordered["receive_ts_ns"].to_numpy().astype(np.int64, copy=False)
    exchange = ordered["exchange_ts_ns"].to_numpy().astype(np.int64, copy=False)
    bids = np.stack(
        [
            ordered[f"bid_price_{level}"].fill_null(0.0).to_numpy()
            for level in range(1, DEPTH_LEVELS + 1)
        ],
        axis=1,
    ).astype(np.float64, copy=False)
    asks = np.stack(
        [
            ordered[f"ask_price_{level}"].fill_null(0.0).to_numpy()
            for level in range(1, DEPTH_LEVELS + 1)
        ],
        axis=1,
    ).astype(np.float64, copy=False)
    bid_volumes = np.stack(
        [
            ordered[f"bid_volume_{level}"].fill_null(0).to_numpy()
            for level in range(1, DEPTH_LEVELS + 1)
        ],
        axis=1,
    ).astype(np.int64, copy=False)
    ask_volumes = np.stack(
        [
            ordered[f"ask_volume_{level}"].fill_null(0).to_numpy()
            for level in range(1, DEPTH_LEVELS + 1)
        ],
        axis=1,
    ).astype(np.int64, copy=False)
    transport_ns = receive - exchange
    maximum_ns = int(round(float(max_transport_delay_ms) * 1_000_000.0))
    valid = (
        (transport_ns >= -100_000_000)
        & (transport_ns <= maximum_ns)
        & (~ordered["simtrade"].fill_null(False).to_numpy().astype(bool))
        & np.isfinite(bids).all(axis=1)
        & np.isfinite(asks).all(axis=1)
        & (bid_volumes >= 0).all(axis=1)
        & (ask_volumes >= 0).all(axis=1)
        & (bids[:, 0] > 0.0)
        & (asks[:, 0] > 0.0)
        & (bid_volumes[:, 0] > 0)
        & (ask_volumes[:, 0] > 0)
        & (bids[:, 0] <= asks[:, 0])
    )
    for level in range(1, DEPTH_LEVELS):
        valid &= (bid_volumes[:, level] == 0) | (
            (bids[:, level] > 0.0) & (bids[:, level] <= bids[:, level - 1])
        )
        valid &= (ask_volumes[:, level] == 0) | (
            (asks[:, level] > 0.0) & (asks[:, level] >= asks[:, level - 1])
        )
    return (
        receive[valid],
        exchange[valid],
        bids[valid],
        bid_volumes[valid],
        asks[valid],
        ask_volumes[valid],
    )


def _first_later_books(
    decisions: pl.DataFrame,
    books: pl.DataFrame,
    *,
    prefix: str,
    execution_latency_ms: float,
    execution_max_wait_ms: float,
    max_transport_delay_ms: float,
) -> dict[str, np.ndarray]:
    decision_ns = np.asarray(
        [int(value.timestamp() * 1_000_000_000) for value in decisions["event_ts"]],
        dtype=np.int64,
    )
    decision_available_ns = decision_ns + 1_000_000_000
    earliest = decision_available_ns + int(
        round(float(execution_latency_ms) * 1_000_000.0)
    )
    deadline = earliest + int(round(float(execution_max_wait_ms) * 1_000_000.0))
    receive, exchange, bids, bid_volumes, asks, ask_volumes = _valid_book_arrays(
        books,
        max_transport_delay_ms=max_transport_delay_ms,
    )
    indices = np.searchsorted(receive, earliest, side="left")
    in_bounds = indices < len(receive)
    safe_indices = np.minimum(indices, max(0, len(receive) - 1))
    valid = in_bounds.copy()
    if len(receive):
        valid &= receive[safe_indices] <= deadline
    else:
        valid[:] = False
    rows = len(decisions)
    output: dict[str, np.ndarray] = {
        f"{prefix}_execution_receive_ts_ns": np.where(
            valid, receive[safe_indices] if len(receive) else 0, 0
        ).astype(np.int64),
        f"{prefix}_book_exchange_ts_ns": np.where(
            valid, exchange[safe_indices] if len(exchange) else 0, 0
        ).astype(np.int64),
        f"{prefix}_book_valid": valid,
    }
    for side, prices, volumes in (
        ("bid", bids, bid_volumes),
        ("ask", asks, ask_volumes),
    ):
        for level in range(DEPTH_LEVELS):
            selected_prices = (
                prices[safe_indices, level] if len(prices) else np.zeros(rows)
            )
            selected_volumes = (
                volumes[safe_indices, level] if len(volumes) else np.zeros(rows)
            )
            output[f"{prefix}_{side}_price_{level + 1}"] = np.where(
                valid, selected_prices, np.nan
            ).astype(np.float64)
            output[f"{prefix}_{side}_volume_{level + 1}"] = np.where(
                valid, selected_volumes, 0
            ).astype(np.int64)
    return output


def _normalize_security_type(value: object) -> str:
    normalized = str(value).strip().upper()
    if normalized.endswith(".FUTURE") or normalized == "FUTURE":
        return "FUT"
    if normalized.endswith(".OPTION") or normalized == "OPTION":
        return "OPT"
    return normalized


def _capture_codes(
    metadata: list[dict[str, Any]],
    *,
    security_type: str,
    delivery_date: date | None = None,
    delivery_month: str | None = None,
    strike: float | None = None,
    option_right: str | None = None,
) -> list[str]:
    matches: list[str] = []
    for row in metadata:
        if _normalize_security_type(row.get("security_type")) != security_type:
            continue
        if (
            delivery_date is not None
            and row.get("delivery_date") != delivery_date.isoformat()
        ):
            continue
        if delivery_month is not None:
            raw_month = re.sub(r"\D", "", str(row.get("delivery_month", "")))[:6]
            if raw_month != delivery_month:
                continue
        if strike is not None and not math.isclose(
            float(row.get("strike_price", float("nan"))),
            float(strike),
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            continue
        if option_right is not None and row.get("option_right") != option_right:
            continue
        for key in ("code", "target_code"):
            value = row.get(key)
            if value and str(value) not in matches:
                matches.append(str(value))
    return matches


def attach_captured_bidask_execution(
    decisions: pl.DataFrame,
    books: pl.DataFrame,
    contract_metadata: list[dict[str, Any]],
    *,
    execution_latency_ms: float = 250.0,
    execution_max_wait_ms: float = 1_000.0,
    max_transport_delay_ms: float = 2_000.0,
) -> pl.DataFrame:
    """Attach first observable post-decision five-level books without fallback."""

    for name, value in (
        ("execution_latency_ms", execution_latency_ms),
        ("execution_max_wait_ms", execution_max_wait_ms),
        ("max_transport_delay_ms", max_transport_delay_ms),
    ):
        if not math.isfinite(float(value)) or float(value) < 0.0:
            raise ValueError(f"{name} must be finite and non-negative")
    if execution_max_wait_ms <= 0.0 or max_transport_delay_ms <= 0.0:
        raise ValueError("execution wait and transport-delay gates must be positive")
    if decisions.is_empty():
        return decisions
    trading_date = decisions["trading_date"][0]
    option_expiry = decisions["option_expiry_date"][0]
    option_strike = float(decisions["option_strike"][0])
    tx_month = str(decisions["tx_contract_month"][0])
    if isinstance(trading_date, datetime):
        trading_date = trading_date.date()
    if isinstance(option_expiry, datetime):
        option_expiry = option_expiry.date()
    leg_specs = {
        "tx": _capture_codes(
            contract_metadata,
            security_type="FUT",
            delivery_month=tx_month,
        ),
        "call": _capture_codes(
            contract_metadata,
            security_type="OPT",
            delivery_date=option_expiry,
            strike=option_strike,
            option_right="C",
        ),
        "put": _capture_codes(
            contract_metadata,
            security_type="OPT",
            delivery_date=option_expiry,
            strike=option_strike,
            option_right="P",
        ),
    }
    if missing := [name for name, codes in leg_specs.items() if not codes]:
        raise ValueError(
            f"captured contract metadata lacks selected legs {missing} for "
            f"date={trading_date}, expiry={option_expiry}, strike={option_strike}"
        )
    attached: dict[str, np.ndarray] = {}
    for prefix, codes in leg_specs.items():
        selected_books = books.filter(pl.col("code").is_in(codes))
        if selected_books.is_empty():
            raise ValueError(
                f"captured BidAsk has no events for {prefix} codes={codes}"
            )
        attached.update(
            _first_later_books(
                decisions,
                selected_books,
                prefix=prefix,
                execution_latency_ms=execution_latency_ms,
                execution_max_wait_ms=execution_max_wait_ms,
                max_transport_delay_ms=max_transport_delay_ms,
            )
        )
    output = decisions.with_columns(
        [pl.Series(name, values) for name, values in attached.items()]
    )
    tx_valid = output["tx_book_valid"].to_numpy().astype(bool)
    option_valid = (
        tx_valid
        & output["call_book_valid"].to_numpy().astype(bool)
        & output["put_book_valid"].to_numpy().astype(bool)
    )
    tx_terminal = np.zeros(output.height, dtype=bool)
    option_terminal = np.zeros(output.height, dtype=bool)
    if tx_valid.any():
        tx_terminal[np.flatnonzero(tx_valid)[-1]] = True
    if option_valid.any():
        option_terminal[np.flatnonzero(option_valid)[-1]] = True
    tx_mid = (
        output["tx_bid_price_1"].to_numpy() + output["tx_ask_price_1"].to_numpy()
    ) / 2.0
    call_mid = (
        output["call_bid_price_1"].to_numpy() + output["call_ask_price_1"].to_numpy()
    ) / 2.0
    put_mid = (
        output["put_bid_price_1"].to_numpy() + output["put_ask_price_1"].to_numpy()
    ) / 2.0

    def interval_log_return(values: np.ndarray, valid: np.ndarray) -> np.ndarray:
        result = np.zeros(len(values), dtype=np.float64)
        indices = np.flatnonzero(valid)
        if len(indices) > 1:
            result[indices[:-1]] = np.log(values[indices[1:]] / values[indices[:-1]])
        return result

    return output.with_columns(
        pl.Series("execution_price", tx_mid),
        pl.Series("interval_log_return", interval_log_return(tx_mid, tx_valid)),
        pl.Series("call_execution_price", call_mid),
        pl.Series("put_execution_price", put_mid),
        pl.Series(
            "call_interval_log_return",
            interval_log_return(call_mid, option_valid),
        ),
        pl.Series(
            "put_interval_log_return",
            interval_log_return(put_mid, option_valid),
        ),
        pl.Series("option_executable", option_valid),
        pl.Series("option_is_terminal", option_terminal),
        pl.Series("is_terminal", tx_terminal),
    )


def build_tick_day_frame(
    tx: pl.DataFrame,
    txo: pl.DataFrame,
    *,
    trading_date: date,
    session: str = "day",
    decision_interval_seconds: int = 5,
    rolling_window_seconds: int = 60,
    warmup_seconds: int = 300,
    atm_moneyness_fraction: float = 0.01,
) -> pl.DataFrame:
    """Build one decision/execution partition without within-second ordering."""

    _validate_builder_parameters(
        session=session,
        decision_interval_seconds=decision_interval_seconds,
        rolling_window_seconds=rolling_window_seconds,
        warmup_seconds=warmup_seconds,
    )
    if not math.isfinite(atm_moneyness_fraction) or not (
        0.0 < atm_moneyness_fraction <= 0.25
    ):
        raise ValueError("atm_moneyness_fraction must be in (0, 0.25]")
    required_tx = {
        "event_ts",
        "session",
        "delivery_month_week",
        "price",
        "matched_quantity",
        "source_row_number",
    }
    required_txo = {
        "delivery_month_week",
        "event_ts",
        "session",
        "strike_price",
        "option_right",
        "price",
        "matched_quantity_equivalent",
    }
    if missing := required_tx.difference(tx.columns):
        raise ValueError(f"TX partition is missing columns: {sorted(missing)}")
    if missing := required_txo.difference(txo.columns):
        raise ValueError(f"TXO partition is missing columns: {sorted(missing)}")

    front_month = taifex_front_month(trading_date)
    selected_tx = (
        tx.filter(
            (pl.col("session") == session)
            & (pl.col("delivery_month_week") == front_month)
        )
        .sort(["event_ts", "source_row_number"])
        .group_by("event_ts", maintain_order=True)
        .agg(
            pl.col("price").first().alias("tx_first_price"),
            pl.col("price").last().alias("tx_last_trade_price"),
            pl.col("price").max().alias("tx_high_price"),
            pl.col("price").min().alias("tx_low_price"),
            pl.col("matched_quantity").sum().alias("tx_volume_1s"),
            pl.len().alias("tx_trade_count_1s"),
        )
        .with_columns(pl.col("event_ts").alias("tx_trade_event_ts"))
    )
    if selected_tx.is_empty():
        raise ValueError(
            f"{trading_date}: no {session} TX rows for causal front month {front_month}"
        )

    session_start = datetime.combine(trading_date, time(8, 45), tzinfo=TAIPEI)
    session_end = datetime.combine(trading_date, time(13, 45), tzinfo=TAIPEI)
    grid = pl.DataFrame(
        {
            "event_ts": pl.datetime_range(
                session_start,
                session_end,
                interval="1s",
                closed="left",
                eager=True,
                time_zone="Asia/Taipei",
            )
        }
    ).with_row_index("second_index")
    grid = (
        grid.join(selected_tx, on="event_ts", how="left")
        .with_columns(
            pl.col("tx_last_trade_price")
            .fill_null(strategy="forward")
            .alias("tx_last_price"),
            pl.col("tx_volume_1s").fill_null(0.0),
            pl.col("tx_trade_count_1s").fill_null(0),
        )
        .with_columns(
            pl.col("tx_first_price")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("execution_price"),
            pl.col("tx_trade_event_ts")
            .shift(-1)
            .fill_null(strategy="backward")
            .alias("execution_event_ts"),
        )
    )

    option_rows = (
        txo.filter(pl.col("session") == session)
        .join(
            grid.select(["event_ts", "tx_last_price"]),
            on="event_ts",
            how="inner",
        )
        .filter(pl.col("tx_last_price").is_not_null())
        .with_columns(
            (
                (pl.col("strike_price") - pl.col("tx_last_price")).abs()
                / pl.col("tx_last_price")
            ).alias("abs_moneyness"),
            (pl.col("price") * pl.col("matched_quantity_equivalent")).alias(
                "premium_notional"
            ),
        )
    )
    if option_rows.is_empty():
        raise ValueError(f"{trading_date}: no {session} TXO rows with a TX reference")
    call = pl.col("option_right") == "C"
    put = pl.col("option_right") == "P"
    atm = pl.col("abs_moneyness") <= float(atm_moneyness_fraction)
    option_seconds = option_rows.group_by("event_ts").agg(
        _sum_when("matched_quantity_equivalent", call).alias("call_volume_1s"),
        _sum_when("matched_quantity_equivalent", put).alias("put_volume_1s"),
        _sum_when("matched_quantity_equivalent", call & atm).alias(
            "atm_call_volume_1s"
        ),
        _sum_when("matched_quantity_equivalent", put & atm).alias("atm_put_volume_1s"),
        pl.col("premium_notional").sum().alias("premium_notional_1s"),
        _sum_when("premium_notional", call).alias("call_premium_notional_1s"),
        (pl.col("abs_moneyness") * pl.col("matched_quantity_equivalent"))
        .sum()
        .alias("abs_moneyness_weighted_1s"),
    )
    option_value_columns = [
        "call_volume_1s",
        "put_volume_1s",
        "atm_call_volume_1s",
        "atm_put_volume_1s",
        "premium_notional_1s",
        "call_premium_notional_1s",
        "abs_moneyness_weighted_1s",
    ]
    grid = grid.join(option_seconds, on="event_ts", how="left").with_columns(
        [pl.col(name).fill_null(0.0) for name in option_value_columns]
    )
    rolling_columns = [
        "tx_volume_1s",
        *option_value_columns,
    ]
    grid = grid.with_columns(
        [
            pl.col(name)
            .rolling_sum(window_size=rolling_window_seconds, min_samples=1)
            .alias(name.removesuffix("_1s") + "_rolling")
            for name in rolling_columns
        ]
    )
    total_option_rolling = pl.col("call_volume_rolling") + pl.col("put_volume_rolling")
    total_atm_rolling = pl.col("atm_call_volume_rolling") + pl.col(
        "atm_put_volume_rolling"
    )
    seconds_per_session = int((session_end - session_start).total_seconds())
    grid = grid.with_columns(
        (pl.col("tx_last_price") / pl.col("tx_last_price").shift(1))
        .log()
        .fill_null(0.0)
        .alias("tx_log_return_1s"),
        (pl.col("tx_last_price") / pl.col("tx_last_price").shift(5))
        .log()
        .fill_null(0.0)
        .alias("tx_log_return_5s"),
        (pl.col("tx_last_price") / pl.col("tx_last_price").shift(60))
        .log()
        .fill_null(0.0)
        .alias("tx_log_return_60s"),
        (
            (pl.col("tx_high_price") - pl.col("tx_low_price"))
            / pl.col("tx_last_price")
            * 10_000.0
        )
        .fill_null(0.0)
        .alias("tx_range_bps_1s"),
        pl.col("tx_volume_1s").log1p().alias("tx_log_volume_1s"),
        pl.col("tx_volume_rolling").log1p().alias("tx_log_volume_60s"),
        pl.col("tx_trade_count_1s")
        .cast(pl.Float64)
        .log1p()
        .alias("tx_log_trade_count_1s"),
        pl.col("call_volume_1s").log1p().alias("option_call_log_volume_1s"),
        pl.col("put_volume_1s").log1p().alias("option_put_log_volume_1s"),
        (
            (pl.col("call_volume_rolling") - pl.col("put_volume_rolling"))
            / total_option_rolling.clip(lower_bound=1.0)
        ).alias("option_call_put_volume_imbalance_60s"),
        (
            (pl.col("atm_call_volume_rolling") - pl.col("atm_put_volume_rolling"))
            / total_atm_rolling.clip(lower_bound=1.0)
        ).alias("option_atm_call_put_volume_imbalance_60s"),
        pl.col("premium_notional_rolling")
        .log1p()
        .alias("option_log_premium_notional_60s"),
        (
            pl.col("call_premium_notional_rolling")
            / pl.col("premium_notional_rolling").clip(lower_bound=1.0)
        ).alias("option_call_premium_share_60s"),
        (total_atm_rolling / total_option_rolling.clip(lower_bound=1.0)).alias(
            "option_atm_volume_share_60s"
        ),
        (
            pl.col("abs_moneyness_weighted_rolling")
            / total_option_rolling.clip(lower_bound=1.0)
        ).alias("option_volume_weighted_abs_moneyness_60s"),
        (pl.col("second_index") * (2.0 * math.pi / seconds_per_session))
        .sin()
        .alias("seconds_from_open_sin"),
        (pl.col("second_index") * (2.0 * math.pi / seconds_per_session))
        .cos()
        .alias("seconds_from_open_cos"),
        (
            (seconds_per_session - pl.col("second_index")) / float(seconds_per_session)
        ).alias("seconds_to_close_fraction"),
    )
    selection_ts = session_start + timedelta(seconds=warmup_seconds)
    selection_prices = grid.filter(pl.col("event_ts") <= selection_ts)[
        "tx_last_price"
    ].drop_nulls()
    if selection_prices.is_empty():
        raise ValueError(f"{trading_date}: no TX price available at option selection")
    option_series, option_expiry, option_strike = _select_causal_atm_option_pair(
        option_rows,
        trading_date=trading_date,
        selection_ts=selection_ts,
        underlying_price=float(selection_prices[-1]),
    )
    grid = _attach_option_pair_prices(
        grid,
        option_rows,
        series=option_series,
        strike=option_strike,
    )
    decisions = grid.filter(
        (pl.col("second_index") >= warmup_seconds)
        & (
            pl.col("second_index")
            <= seconds_per_session - decision_interval_seconds - 1
        )
        & ((pl.col("second_index") % decision_interval_seconds) == 0)
        & pl.col("tx_last_price").is_not_null()
        & pl.col("execution_price").is_not_null()
        & (pl.col("execution_event_ts") > pl.col("event_ts"))
    ).unique(
        subset=["execution_event_ts"],
        keep="first",
        maintain_order=True,
    )
    option_decisions = (
        decisions.filter(
            pl.col("call_last_price").is_not_null()
            & pl.col("put_last_price").is_not_null()
            & pl.col("call_execution_price").is_not_null()
            & pl.col("put_execution_price").is_not_null()
            & (pl.col("call_execution_event_ts") > pl.col("event_ts"))
            & (pl.col("put_execution_event_ts") > pl.col("event_ts"))
        )
        .unique(
            subset=[
                "execution_event_ts",
                "call_execution_event_ts",
                "put_execution_event_ts",
            ],
            keep="first",
            maintain_order=True,
        )
        .filter(
            (
                pl.col("call_execution_event_ts").shift(1).is_null()
                | (
                    pl.col("call_execution_event_ts")
                    > pl.col("call_execution_event_ts").shift(1)
                )
            )
            & (
                pl.col("put_execution_event_ts").shift(1).is_null()
                | (
                    pl.col("put_execution_event_ts")
                    > pl.col("put_execution_event_ts").shift(1)
                )
            )
        )
        .with_columns(
            (pl.col("call_execution_price").shift(-1) / pl.col("call_execution_price"))
            .log()
            .fill_null(0.0)
            .alias("call_interval_log_return"),
            (pl.col("put_execution_price").shift(-1) / pl.col("put_execution_price"))
            .log()
            .fill_null(0.0)
            .alias("put_interval_log_return"),
            pl.lit(True).alias("option_executable"),
            (pl.int_range(0, pl.len()) == pl.len() - 1).alias("option_is_terminal"),
        )
        .select(
            "event_ts",
            "option_executable",
            "option_is_terminal",
            "call_interval_log_return",
            "put_interval_log_return",
        )
    )
    if decisions.height < 2:
        raise ValueError(f"{trading_date}: fewer than two causal decision rows")
    if option_decisions.height < 2:
        raise ValueError(f"{trading_date}: fewer than two causal option decision rows")
    decisions = (
        decisions.join(option_decisions, on="event_ts", how="left")
        .with_columns(
            pl.col("option_executable").fill_null(False),
            pl.col("option_is_terminal").fill_null(False),
            pl.col("call_interval_log_return").fill_null(0.0),
            pl.col("put_interval_log_return").fill_null(0.0),
            pl.lit(trading_date).cast(pl.Date).alias("trading_date"),
            pl.lit(front_month).alias("tx_contract_month"),
            pl.lit(option_series).alias("option_series"),
            pl.lit(option_expiry).cast(pl.Date).alias("option_expiry_date"),
            pl.lit(option_strike).alias("option_strike"),
            (pl.col("execution_price").shift(-1) / pl.col("execution_price"))
            .log()
            .fill_null(0.0)
            .alias("interval_log_return"),
            (pl.int_range(0, pl.len()) == pl.len() - 1).alias("is_terminal"),
        )
        .with_columns(
            [
                pl.col(name).cast(pl.Float32).fill_nan(0.0).fill_null(0.0).alias(name)
                for name in TICK_FEATURE_COLUMNS
            ]
        )
    )
    return decisions.select(
        [
            "trading_date",
            "event_ts",
            "execution_event_ts",
            "call_execution_event_ts",
            "put_execution_event_ts",
            "tx_contract_month",
            "option_series",
            "option_expiry_date",
            "option_strike",
            "tx_last_price",
            "call_last_price",
            "put_last_price",
            "execution_price",
            "call_execution_price",
            "put_execution_price",
            "interval_log_return",
            "call_interval_log_return",
            "put_interval_log_return",
            "option_executable",
            "option_is_terminal",
            "is_terminal",
            *TICK_FEATURE_COLUMNS,
        ]
    )


def _partition_path(root: Path, trading_date: date) -> Path:
    return root / f"trading_date={trading_date.isoformat()}" / "decisions.parquet"


def _load_bidask_capture_day(
    capture_root: Path,
    trading_date: date,
) -> tuple[pl.DataFrame, list[dict[str, Any]], dict[str, Any]]:
    manifests = read_capture_manifests(capture_root, trading_date.isoformat())
    if not manifests:
        raise ValueError(f"FOP capture manifests are missing for {trading_date}")
    manifest_dir = (
        capture_root / "manifests" / f"trade_date={trading_date.isoformat()}"
    )
    for manifest in manifests:
        if manifest.get("source") != "shioaji_taifex_tick_bidask_v1":
            raise ValueError(f"unexpected FOP capture source: {manifest_dir}")
        if manifest.get("status") != "complete":
            raise ValueError(f"FOP capture is not complete: {manifest_dir}")
        if int(manifest.get("dropped_events", -1)) != 0:
            raise ValueError(f"FOP capture recorded dropped events: {manifest_dir}")
    metadata = [
        row
        for manifest in manifests
        for row in manifest.get("contract_metadata", [])
        if isinstance(row, dict)
    ]
    if not metadata:
        raise ValueError(f"FOP capture has no contract metadata: {manifest_dir}")
    paths = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_events",
        trade_date=trading_date.isoformat(),
        manifests=manifests,
    )
    part_hashes = {str(path): _sha256_path(path) for path in paths}
    books = pl.concat([pl.read_parquet(path) for path in paths], how="vertical_relaxed")
    manifest_paths = sorted(manifest_dir.glob("worker=*.json"))
    evidence = {
        "manifest_path": str(manifest_paths[0]),
        "manifest_sha256": _sha256_path(manifest_paths[0]),
        "manifest_paths": [
            str(manifest_dir / f"worker={int(item['worker_index']):02d}.json")
            for item in manifests
        ],
        "manifest_sha256_by_path": {
            str(path): _sha256_path(path)
            for path in manifest_paths
        },
        "capture_id": str(shared_capture_id(manifests)),
        "book_part_sha256": part_hashes,
        "book_rows": books.height,
    }
    return books, metadata, evidence


def build_index_derivatives_tick_dataset(
    raw_root: str | Path,
    output_root: str | Path,
    *,
    execution_price_source: str = TAIFEX_TRADE_PROXY_SOURCE,
    bidask_capture_root: str | Path = (
        "data_tw_index_derivatives_ticks/shioaji_fop_captures"
    ),
    execution_latency_ms: float = 250.0,
    execution_max_wait_ms: float = 1_000.0,
    max_transport_delay_ms: float = 2_000.0,
    session: str = "day",
    decision_interval_seconds: int = 5,
    rolling_window_seconds: int = 60,
    warmup_seconds: int = 300,
    atm_moneyness_fraction: float = 0.01,
) -> dict[str, Any]:
    """Materialize receipt-backed daily strategy partitions from raw TX/TXO."""

    _validate_builder_parameters(
        session=session,
        decision_interval_seconds=decision_interval_seconds,
        rolling_window_seconds=rolling_window_seconds,
        warmup_seconds=warmup_seconds,
    )
    source_root = Path(raw_root).expanduser().resolve()
    target_root = Path(output_root).expanduser().resolve()
    quote_source = str(execution_price_source).strip().lower()
    if quote_source not in {SHIOAJI_BIDASK_SOURCE, TAIFEX_TRADE_PROXY_SOURCE}:
        raise ValueError(
            "execution_price_source must be 'shioaji_bidask' or "
            "'taifex_next_trade_proxy'"
        )
    capture_root = Path(bidask_capture_root).expanduser().resolve()
    source_manifest_path = source_root / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text(encoding="utf-8"))
    if source_manifest.get("status") != "complete":
        raise ValueError("TAIFEX TX/TXO source manifest is not complete")
    raw_dates = source_manifest.get("trading_dates")
    if not isinstance(raw_dates, list) or not raw_dates:
        raise ValueError("TAIFEX TX/TXO source manifest has no trading_dates")
    source_manifest_sha256 = _sha256_path(source_manifest_path)
    parameters = {
        "execution_price_source": quote_source,
        "bidask_capture_root": str(capture_root),
        "execution_latency_ms": float(execution_latency_ms),
        "execution_max_wait_ms": float(execution_max_wait_ms),
        "max_transport_delay_ms": float(max_transport_delay_ms),
        "session": session,
        "decision_interval_seconds": int(decision_interval_seconds),
        "rolling_window_seconds": int(rolling_window_seconds),
        "warmup_seconds": int(warmup_seconds),
        "atm_moneyness_fraction": float(atm_moneyness_fraction),
    }
    partitions: list[dict[str, Any]] = []
    unavailable_bidask_dates: list[dict[str, str]] = []
    capture_evidence: list[dict[str, Any]] = []
    for raw_date in raw_dates:
        trade_date = date.fromisoformat(str(raw_date))
        books: pl.DataFrame | None = None
        contract_metadata: list[dict[str, Any]] | None = None
        capture_receipt: dict[str, Any] | None = None
        if quote_source == SHIOAJI_BIDASK_SOURCE:
            try:
                books, contract_metadata, capture_receipt = _load_bidask_capture_day(
                    capture_root,
                    trade_date,
                )
            except (FileNotFoundError, RuntimeError, ValueError) as exc:
                unavailable_bidask_dates.append(
                    {"trading_date": trade_date.isoformat(), "reason": str(exc)}
                )
                continue
        tx_path = (
            source_root
            / "tx"
            / f"trading_date={trade_date.isoformat()}"
            / "transactions.parquet"
        )
        txo_path = (
            source_root
            / "txo"
            / f"trading_date={trade_date.isoformat()}"
            / "transactions.parquet"
        )
        if not tx_path.is_file() or not txo_path.is_file():
            raise FileNotFoundError(f"missing TX/TXO partition for {trade_date}")
        tx_sha256 = _sha256_path(tx_path)
        txo_sha256 = _sha256_path(txo_path)
        output_path = _partition_path(target_root, trade_date)
        receipt_path = output_path.with_suffix(".receipt.json")
        receipt: dict[str, Any] | None = None
        if output_path.is_file() and receipt_path.is_file():
            candidate = json.loads(receipt_path.read_text(encoding="utf-8"))
            if (
                candidate.get("schema_version") == TICK_DATASET_SCHEMA_VERSION
                and candidate.get("feature_contract_version")
                == TICK_FEATURE_CONTRACT_VERSION
                and candidate.get("tx_sha256") == tx_sha256
                and candidate.get("txo_sha256") == txo_sha256
                and candidate.get("parameters") == parameters
                and candidate.get("capture_receipt") == capture_receipt
                and candidate.get("output_sha256") == _sha256_path(output_path)
            ):
                receipt = candidate
        if receipt is None:
            frame = build_tick_day_frame(
                pl.read_parquet(tx_path),
                pl.read_parquet(txo_path),
                trading_date=trade_date,
                session=session,
                decision_interval_seconds=decision_interval_seconds,
                rolling_window_seconds=rolling_window_seconds,
                warmup_seconds=warmup_seconds,
                atm_moneyness_fraction=atm_moneyness_fraction,
            )
            if quote_source == SHIOAJI_BIDASK_SOURCE:
                assert books is not None
                assert contract_metadata is not None
                try:
                    frame = attach_captured_bidask_execution(
                        frame,
                        books,
                        contract_metadata,
                        execution_latency_ms=execution_latency_ms,
                        execution_max_wait_ms=execution_max_wait_ms,
                        max_transport_delay_ms=max_transport_delay_ms,
                    )
                except ValueError as exc:
                    unavailable_bidask_dates.append(
                        {"trading_date": trade_date.isoformat(), "reason": str(exc)}
                    )
                    continue
                if int(frame["is_terminal"].sum()) != 1:
                    unavailable_bidask_dates.append(
                        {
                            "trading_date": trade_date.isoformat(),
                            "reason": "fewer than one executable TX BidAsk row",
                        }
                    )
                    continue
                if int(frame["option_executable"].sum()) < 2:
                    unavailable_bidask_dates.append(
                        {
                            "trading_date": trade_date.isoformat(),
                            "reason": "fewer than two executable Call/Put BidAsk rows",
                        }
                    )
                    continue
            _atomic_parquet(frame, output_path)
            receipt = {
                "schema_version": TICK_DATASET_SCHEMA_VERSION,
                "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
                "trading_date": trade_date.isoformat(),
                "parameters": parameters,
                "capture_receipt": capture_receipt,
                "tx_path": str(tx_path),
                "tx_sha256": tx_sha256,
                "txo_path": str(txo_path),
                "txo_sha256": txo_sha256,
                "output_path": str(output_path),
                "output_sha256": _sha256_path(output_path),
                "rows": (
                    int(frame["tx_book_valid"].sum())
                    if quote_source == SHIOAJI_BIDASK_SOURCE
                    else frame.height
                ),
                "option_rows": int(frame["option_executable"].sum()),
                "event_ts_min": frame["event_ts"].min().isoformat(),
                "event_ts_max": frame["event_ts"].max().isoformat(),
                "execution_ts_min": frame["execution_event_ts"].min().isoformat(),
                "execution_ts_max": frame["execution_event_ts"].max().isoformat(),
                "tx_contract_month": frame["tx_contract_month"][0],
                "option_series": frame["option_series"][0],
                "option_expiry_date": frame["option_expiry_date"][0].isoformat(),
                "option_strike": float(frame["option_strike"][0]),
            }
            _atomic_json(receipt_path, receipt)
        partitions.append(receipt)
        if capture_receipt is not None:
            capture_evidence.append(capture_receipt)
    if not partitions:
        raise ValueError(
            "no strategy dates could be materialized for execution_price_source="
            f"{quote_source}; unavailable={unavailable_bidask_dates[:3]}"
        )
    source_fingerprint_payload = {
        "taifex_source_manifest_sha256": source_manifest_sha256,
        "execution_price_source": quote_source,
        "capture_manifests": [row["manifest_sha256"] for row in capture_evidence],
    }
    combined_source_sha256 = hashlib.sha256(
        json.dumps(
            source_fingerprint_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    selected_dates = [str(row["trading_date"]) for row in partitions]
    manifest = {
        "schema_version": TICK_DATASET_SCHEMA_VERSION,
        "feature_contract_version": TICK_FEATURE_CONTRACT_VERSION,
        "status": "complete",
        "source_manifest_path": str(source_manifest_path),
        "source_manifest_sha256": combined_source_sha256,
        "taifex_source_manifest_sha256": source_manifest_sha256,
        "execution_price_source": quote_source,
        "parameters": parameters,
        "decision_clock": (
            "after every completed public-source second on the configured grid"
        ),
        "execution_clock": (
            "first valid captured five-level BidAsk whose local receive time is "
            "after completed-second decision availability plus configured latency"
            if quote_source == SHIOAJI_BIDASK_SOURCE
            else "first later whole-second TX trade and first later whole-second "
            "matched-volume-weighted trade price for each fixed TXO leg"
        ),
        "position_contract": (
            "shared data supports one signed TX exposure or a fixed causal ATM "
            "Call/Put pair; execution mode owns the position policy"
        ),
        "feature_names": list(TICK_FEATURE_COLUMNS),
        "option_feature_names": list(OPTION_TICK_FEATURE_COLUMNS),
        "trading_dates": selected_dates,
        "partitions": partitions,
        "coverage": {
            "raw_dates": [str(value) for value in raw_dates],
            "selected_dates": selected_dates,
            "unavailable_bidask_dates": unavailable_bidask_dates,
            "coverage_complete": not unavailable_bidask_dates,
        },
        "summary": {
            "dates": len(partitions),
            "rows": int(sum(int(row["rows"]) for row in partitions)),
            "option_rows": int(sum(int(row["option_rows"]) for row in partitions)),
        },
    }
    _atomic_json(target_root / "manifest.json", manifest)
    return manifest


@dataclass(frozen=True, slots=True)
class TickFeatureNormalizer:
    mean: np.ndarray
    scale: np.ndarray
    counts: np.ndarray

    def transform(self, values: np.ndarray) -> np.ndarray:
        normalized = (values.astype(np.float32, copy=False) - self.mean) / self.scale
        return np.nan_to_num(normalized, nan=0.0, posinf=0.0, neginf=0.0)


@dataclass(frozen=True, slots=True)
class IndexDerivativesTickDay:
    trading_date: np.datetime64
    event_ts: np.ndarray
    execution_event_ts: np.ndarray
    tx_contract_month: str
    features: np.ndarray
    interval_log_returns: np.ndarray
    execution_prices: np.ndarray
    tradable_mask: np.ndarray
    terminal_mask: np.ndarray
    execution_bid_prices: np.ndarray | None = None
    execution_bid_volumes: np.ndarray | None = None
    execution_ask_prices: np.ndarray | None = None
    execution_ask_volumes: np.ndarray | None = None
    next_execution_bid_prices: np.ndarray | None = None
    next_execution_ask_prices: np.ndarray | None = None
    option_series: str | None = None
    option_expiry_date: np.datetime64 | None = None
    option_strike: float | None = None
    option_execution_event_ts: np.ndarray | None = None
    option_execution_prices: np.ndarray | None = None
    option_interval_log_returns: np.ndarray | None = None
    underlying_execution_prices: np.ndarray | None = None
    option_execution_bid_prices: np.ndarray | None = None
    option_execution_bid_volumes: np.ndarray | None = None
    option_execution_ask_prices: np.ndarray | None = None
    option_execution_ask_volumes: np.ndarray | None = None
    option_next_execution_bid_prices: np.ndarray | None = None
    option_next_execution_ask_prices: np.ndarray | None = None

    @property
    def rows(self) -> int:
        return int(self.features.shape[0])


@dataclass(slots=True)
class IndexDerivativesTickDataset:
    root: Path
    manifest: dict[str, Any]
    dates: np.ndarray
    partition_paths: tuple[Path, ...]
    verify_partition_sha256: bool = True

    @property
    def num_features(self) -> int:
        return len(TICK_FEATURE_COLUMNS)

    @staticmethod
    def feature_names(*, option_mode: bool = False) -> tuple[str, ...]:
        return OPTION_TICK_FEATURE_COLUMNS if option_mode else TICK_FEATURE_COLUMNS

    @staticmethod
    def _raw_features(frame: pl.DataFrame, *, option_mode: bool) -> np.ndarray:
        shared = (
            frame.select(TICK_FEATURE_COLUMNS).to_numpy().astype(np.float32, copy=False)
        )
        if not option_mode:
            return shared[:, None, :]
        underlying = frame["tx_last_price"].to_numpy().astype(np.float32)
        strike = frame["option_strike"].to_numpy().astype(np.float32)
        premiums = np.stack(
            [
                frame["call_last_price"].to_numpy(),
                frame["put_last_price"].to_numpy(),
            ],
            axis=1,
        ).astype(np.float32)
        denominator = np.maximum(underlying, np.finfo(np.float32).tiny)
        signed_otm = np.stack(
            [
                (strike - underlying) / denominator,
                (underlying - strike) / denominator,
            ],
            axis=1,
        )
        intrinsic = np.maximum(-signed_otm, 0.0)
        expiry = frame["option_expiry_date"].to_numpy().astype("datetime64[D]")
        trade_date = frame["trading_date"].to_numpy().astype("datetime64[D]")
        days = np.maximum((expiry - trade_date).astype(np.int64), 0).astype(np.float32)
        leg = np.stack(
            [
                np.log1p(np.maximum(premiums, 0.0)),
                signed_otm,
                intrinsic,
                np.broadcast_to(
                    np.asarray([1.0, 0.0], dtype=np.float32), premiums.shape
                ),
                np.broadcast_to(
                    np.asarray([0.0, 1.0], dtype=np.float32), premiums.shape
                ),
                np.broadcast_to((days / 30.0)[:, None], premiums.shape),
            ],
            axis=-1,
        )
        shared_legs = np.repeat(shared[:, None, :], 2, axis=1)
        return np.concatenate([shared_legs, leg], axis=-1).astype(
            np.float32, copy=False
        )

    def fit_normalizer(
        self,
        indices: Iterable[int],
        *,
        option_mode: bool = False,
    ) -> TickFeatureNormalizer:
        feature_names = self.feature_names(option_mode=option_mode)
        total = np.zeros(len(feature_names), dtype=np.float64)
        total_sq = np.zeros(len(feature_names), dtype=np.float64)
        counts = np.zeros(len(feature_names), dtype=np.int64)
        for raw_index in indices:
            frame = pl.read_parquet(self.partition_paths[int(raw_index)])
            if option_mode:
                frame = frame.filter(pl.col("option_executable"))
            values = (
                self._raw_features(frame, option_mode=option_mode)
                .reshape(-1, len(feature_names))
                .astype(np.float64, copy=False)
            )
            finite = np.isfinite(values)
            total += np.where(finite, values, 0.0).sum(axis=0)
            total_sq += np.where(finite, values * values, 0.0).sum(axis=0)
            counts += finite.sum(axis=0)
        if np.any(counts == 0):
            missing = [
                name
                for name, count in zip(feature_names, counts, strict=True)
                if count == 0
            ]
            raise ValueError(f"training split has no finite values for {missing}")
        mean = total / counts
        variance = np.maximum(total_sq / counts - mean * mean, 0.0)
        scale = np.sqrt(variance)
        scale = np.where(scale > 1e-6, scale, 1.0)
        return TickFeatureNormalizer(
            mean=mean.astype(np.float32),
            scale=scale.astype(np.float32),
            counts=counts,
        )

    def load_day(
        self,
        index: int,
        *,
        normalizer: TickFeatureNormalizer,
        option_mode: bool = False,
    ) -> IndexDerivativesTickDay:
        path = self.partition_paths[int(index)]
        if self.verify_partition_sha256:
            expected = self.manifest["partitions"][int(index)]["output_sha256"]
            if _sha256_path(path) != expected:
                raise ValueError(f"tick dataset partition SHA256 mismatch: {path}")
        frame = pl.read_parquet(path)
        quote_source = self.manifest.get(
            "execution_price_source", TAIFEX_TRADE_PROXY_SOURCE
        )
        bidask_mode = quote_source == SHIOAJI_BIDASK_SOURCE
        if option_mode:
            frame = frame.filter(pl.col("option_executable"))
        elif bidask_mode:
            frame = frame.filter(pl.col("tx_book_valid"))
        values = self._raw_features(frame, option_mode=option_mode)
        features = normalizer.transform(values)
        event_ts = frame["event_ts"].to_numpy()
        if bidask_mode:
            execution_event_ts = frame["tx_execution_receive_ts_ns"].to_numpy()
            decision_ns = np.asarray(
                [int(value.timestamp() * 1_000_000_000) for value in frame["event_ts"]],
                dtype=np.int64,
            )
        else:
            execution_event_ts = frame["execution_event_ts"].to_numpy()
            decision_ns = event_ts
        if not np.all(execution_event_ts > decision_ns):
            raise ValueError(f"non-causal execution timestamp in {path}")
        if (
            not option_mode
            and len(execution_event_ts) > 1
            and not np.all(execution_event_ts[1:] > execution_event_ts[:-1])
        ):
            raise ValueError(
                f"execution timestamps are not strictly increasing in {path}"
            )
        prices = frame["execution_price"].to_numpy().astype(np.float32)
        returns = frame["interval_log_return"].to_numpy().astype(np.float32)
        terminal_column = "option_is_terminal" if option_mode else "is_terminal"
        terminal = frame[terminal_column].to_numpy().astype(bool)
        valid = np.isfinite(prices) & (prices > 0.0) & np.isfinite(returns)
        if terminal.sum() != 1 or not terminal[-1]:
            raise ValueError(f"tick dataset terminal row contract failed: {path}")
        day = IndexDerivativesTickDay(
            trading_date=self.dates[int(index)],
            event_ts=event_ts,
            execution_event_ts=execution_event_ts,
            tx_contract_month=str(frame["tx_contract_month"][0]),
            features=features,
            interval_log_returns=returns,
            execution_prices=prices,
            tradable_mask=valid,
            terminal_mask=terminal,
        )
        if bidask_mode:

            def depth(prefix: str, side: str, kind: str) -> np.ndarray:
                dtype = np.float32 if kind == "price" else np.float32
                return np.stack(
                    [
                        frame[f"{prefix}_{side}_{kind}_{level}"].to_numpy()
                        for level in range(1, DEPTH_LEVELS + 1)
                    ],
                    axis=-1,
                ).astype(dtype)

            tx_bid = depth("tx", "bid", "price")
            tx_ask = depth("tx", "ask", "price")
            tx_bid_volume = depth("tx", "bid", "volume")
            tx_ask_volume = depth("tx", "ask", "volume")
            next_tx_bid = np.concatenate([tx_bid[1:], tx_bid[-1:]], axis=0)
            next_tx_ask = np.concatenate([tx_ask[1:], tx_ask[-1:]], axis=0)
            day = replace(
                day,
                execution_bid_prices=tx_bid,
                execution_bid_volumes=tx_bid_volume,
                execution_ask_prices=tx_ask,
                execution_ask_volumes=tx_ask_volume,
                next_execution_bid_prices=next_tx_bid,
                next_execution_ask_prices=next_tx_ask,
            )
        if not option_mode:
            return day
        if bidask_mode:
            option_execution_event_ts = np.stack(
                [
                    frame["call_execution_receive_ts_ns"].to_numpy(),
                    frame["put_execution_receive_ts_ns"].to_numpy(),
                ],
                axis=1,
            )
            option_decision_ns = decision_ns[:, None]
        else:
            option_execution_event_ts = np.stack(
                [
                    frame["call_execution_event_ts"].to_numpy(),
                    frame["put_execution_event_ts"].to_numpy(),
                ],
                axis=1,
            )
            option_decision_ns = event_ts[:, None]
        if not np.all(option_execution_event_ts > option_decision_ns):
            raise ValueError(f"non-causal option execution timestamp in {path}")
        if len(option_execution_event_ts) > 1 and not np.all(
            option_execution_event_ts[1:] > option_execution_event_ts[:-1]
        ):
            raise ValueError(
                f"option execution timestamps are not strictly increasing in {path}"
            )
        option_prices = np.stack(
            [
                frame["call_execution_price"].to_numpy(),
                frame["put_execution_price"].to_numpy(),
            ],
            axis=1,
        ).astype(np.float32)
        option_returns = np.stack(
            [
                frame["call_interval_log_return"].to_numpy(),
                frame["put_interval_log_return"].to_numpy(),
            ],
            axis=1,
        ).astype(np.float32)
        option_valid = (
            np.isfinite(option_prices)
            & (option_prices > 0.0)
            & np.isfinite(option_returns)
        )
        kwargs: dict[str, Any] = {}
        if bidask_mode:
            option_bid = np.stack(
                [depth("call", "bid", "price"), depth("put", "bid", "price")],
                axis=1,
            )
            option_ask = np.stack(
                [depth("call", "ask", "price"), depth("put", "ask", "price")],
                axis=1,
            )
            kwargs = {
                "option_execution_bid_prices": option_bid,
                "option_execution_bid_volumes": np.stack(
                    [
                        depth("call", "bid", "volume"),
                        depth("put", "bid", "volume"),
                    ],
                    axis=1,
                ),
                "option_execution_ask_prices": option_ask,
                "option_execution_ask_volumes": np.stack(
                    [
                        depth("call", "ask", "volume"),
                        depth("put", "ask", "volume"),
                    ],
                    axis=1,
                ),
                "option_next_execution_bid_prices": np.concatenate(
                    [option_bid[1:], option_bid[-1:]], axis=0
                ),
                "option_next_execution_ask_prices": np.concatenate(
                    [option_ask[1:], option_ask[-1:]], axis=0
                ),
            }
        return IndexDerivativesTickDay(
            trading_date=day.trading_date,
            event_ts=day.event_ts,
            execution_event_ts=day.execution_event_ts,
            tx_contract_month=day.tx_contract_month,
            features=features,
            interval_log_returns=day.interval_log_returns,
            execution_prices=day.execution_prices,
            tradable_mask=option_valid,
            terminal_mask=day.terminal_mask,
            option_series=str(frame["option_series"][0]),
            option_expiry_date=np.datetime64(frame["option_expiry_date"][0], "D"),
            option_strike=float(frame["option_strike"][0]),
            option_execution_event_ts=option_execution_event_ts,
            option_execution_prices=option_prices,
            option_interval_log_returns=option_returns,
            underlying_execution_prices=prices,
            execution_bid_prices=day.execution_bid_prices,
            execution_bid_volumes=day.execution_bid_volumes,
            execution_ask_prices=day.execution_ask_prices,
            execution_ask_volumes=day.execution_ask_volumes,
            next_execution_bid_prices=day.next_execution_bid_prices,
            next_execution_ask_prices=day.next_execution_ask_prices,
            **kwargs,
        )


def load_index_derivatives_tick_dataset(
    root: str | Path,
    *,
    verify_partition_sha256: bool = True,
) -> IndexDerivativesTickDataset:
    dataset_root = Path(root).expanduser().resolve()
    manifest = json.loads((dataset_root / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError("tick strategy dataset manifest is not complete")
    if manifest.get("schema_version") != TICK_DATASET_SCHEMA_VERSION:
        raise ValueError("unsupported tick strategy dataset schema")
    if manifest.get("feature_contract_version") != TICK_FEATURE_CONTRACT_VERSION:
        raise ValueError("unsupported tick feature contract")
    if manifest.get("feature_names") != list(TICK_FEATURE_COLUMNS):
        raise ValueError("tick strategy feature schema mismatch")
    if manifest.get("option_feature_names") != list(OPTION_TICK_FEATURE_COLUMNS):
        raise ValueError("tick option strategy feature schema mismatch")
    dates = np.asarray(manifest["trading_dates"], dtype="datetime64[D]")
    paths = tuple(
        _partition_path(dataset_root, date.fromisoformat(str(raw_date)))
        for raw_date in manifest["trading_dates"]
    )
    if any(not path.is_file() for path in paths):
        raise FileNotFoundError("tick strategy dataset has missing partitions")
    if len(manifest.get("partitions", [])) != len(paths):
        raise ValueError("tick strategy manifest partition count mismatch")
    return IndexDerivativesTickDataset(
        root=dataset_root,
        manifest=manifest,
        dates=dates,
        partition_paths=paths,
        verify_partition_sha256=bool(verify_partition_sha256),
    )


__all__ = [
    "DEPTH_LEVELS",
    "IndexDerivativesTickDataset",
    "IndexDerivativesTickDay",
    "OPTION_LEG_FEATURE_COLUMNS",
    "OPTION_TICK_FEATURE_COLUMNS",
    "TICK_DATASET_SCHEMA_VERSION",
    "TICK_FEATURE_COLUMNS",
    "TICK_FEATURE_CONTRACT_VERSION",
    "TickFeatureNormalizer",
    "attach_captured_bidask_execution",
    "build_index_derivatives_tick_dataset",
    "build_tick_day_frame",
    "load_index_derivatives_tick_dataset",
    "taifex_front_month",
    "taifex_option_expiry",
]
