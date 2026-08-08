#!/usr/bin/env python3
"""Run causal one-lot TXO benchmark controls on receipt-backed TAIFEX ticks.

All decisions use completed whole-second observations and every non-terminal
trade uses the first strictly later transaction print.  Historical books are
not available, so the suite deliberately implements the user's optimistic
one-lot market-fill assumption without quantity/depth caps or artificial
slippage.  Terminal exits remain last-trade mark proxies and are labelled as
such.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Final, Iterable, Mapping, Sequence

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import (  # noqa: E402
    CLASSIC_NO_ROLL_THRESHOLD,
    STRATEGY_CLASSIC,
    STRATEGY_ROLL_ITM,
    STRATEGY_ROLL_OTM,
    DayMarket,
    Fill,
    OptionContract,
    _build_day_market,
    _datetime_from_ns,
    _ns,
    _parse_time,
    _sha256_path,
    _simulate_variant,
    _summarize_trade_rows,
    _trade_row,
    _verify_manifest,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIFEX_TRADE_PROXY_SOURCE,
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 1
OPTION_PRODUCT: Final[str] = "TXO"
OPTION_MULTIPLIER: Final[float] = 50.0
FUTURES_MULTIPLIERS: Final[dict[str, float]] = {
    "TX": 200.0,
    "MTX": 50.0,
    "TMF": 10.0,
}
FIXED_FEES_PER_CONTRACT_SIDE: Final[dict[str, float]] = {
    "TX": 60.0,
    "MTX": 24.0,
    "TXO": 22.0,
    "TMF": 16.0,
}

FAMILY_CLASSIC: Final[str] = "buy_hold_atm_straddle"
FAMILY_CANDIDATE: Final[str] = "single_leg_roll_candidate"
FAMILY_TP_SL: Final[str] = "atm_straddle_fixed_tp_sl"
FAMILY_FULL_RECENTER: Final[str] = "full_recenter_straddle"
FAMILY_STRANGLE: Final[str] = "long_strangle"
FAMILY_GAMMA: Final[str] = "gamma_scalping"
FAMILY_DELTA_BAND: Final[str] = "delta_band_gamma_scalping"
FAMILY_TIME_RECENTER: Final[str] = "time_based_recenter"
FAMILY_RATCHET: Final[str] = "trailing_ratchet_roll"
FAMILY_UNDERLYING: Final[str] = "no_option_underlying"
FAMILY_RANDOM: Final[str] = "random_roll_control"

BENCHMARK_CATALOG: Final[tuple[tuple[str, str, str], ...]] = (
    (
        FAMILY_CLASSIC,
        "Buy & Hold ATM Straddle",
        "Open one ATM Call and Put, then flatten at the daily terminal mark.",
    ),
    (
        FAMILY_TP_SL,
        "ATM Straddle + fixed TP/SL",
        "No rolling; exit both legs after a fixed opening-premium TP or SL.",
    ),
    (
        FAMILY_FULL_RECENTER,
        "Full Re-center Straddle",
        "When absolute TX movement reaches N points, replace both legs with ATM.",
    ),
    (
        FAMILY_STRANGLE,
        "Long Strangle",
        "Open equidistant OTM Call and Put and hold to the daily terminal mark.",
    ),
    (
        FAMILY_GAMMA,
        "Gamma Scalping",
        "Hold the opening ATM straddle and periodically delta-hedge with futures.",
    ),
    (
        FAMILY_DELTA_BAND,
        "Delta-band Gamma Scalping",
        "Hedge only when absolute net delta exceeds the configured band.",
    ),
    (
        FAMILY_TIME_RECENTER,
        "Time-based Re-center",
        "Replace both legs with ATM every 15, 30, or 60 minutes.",
    ),
    (
        FAMILY_RATCHET,
        "Trailing / Ratchet Roll",
        "Roll Put only at ratcheted highs and Call only at ratcheted lows.",
    ),
    (
        FAMILY_UNDERLYING,
        "No-option underlying",
        "Hold one long or short TX, MTX, or TMF future without options.",
    ),
    (
        FAMILY_RANDOM,
        "Random-roll / shuffled trigger",
        "Random causal roll times with the exact candidate roll count per day.",
    ),
)


@dataclass(frozen=True, slots=True)
class Variant:
    family: str
    variant_id: str
    role: str
    parameters: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class OpenOptions:
    series: str
    expiry: date
    positions: dict[str, OptionContract]
    position_open_ns: dict[str, int]
    opening_underlying: float
    opening_premium_twd: float
    opening_strikes: dict[str, float]
    available_after_ns: int


def _variant(
    family: str,
    variant_id: str,
    *,
    role: str = "benchmark",
    **parameters: Any,
) -> Variant:
    return Variant(
        family=family,
        variant_id=variant_id,
        role=role,
        parameters=dict(parameters),
    )


def _parameter_json(variant: Variant) -> str:
    return json.dumps(
        dict(variant.parameters), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def _primary_parameter(variant: Variant) -> int:
    for key in (
        "rolling_points",
        "recenter_points",
        "strangle_distance_points",
        "interval_minutes",
        "tp_percent",
    ):
        value = variant.parameters.get(key)
        if value is not None:
            return int(round(float(value)))
    return 0


def _tag_trade(row: dict[str, Any], variant: Variant) -> dict[str, Any]:
    tagged = dict(row)
    tagged.update(
        {
            "benchmark_family": variant.family,
            "variant_id": variant.variant_id,
            "variant_role": variant.role,
            "parameters_json": _parameter_json(variant),
        }
    )
    return tagged


def _option_trade(
    *,
    market: DayMarket,
    variant: Variant,
    contract: OptionContract,
    fill: Fill,
    delta_contracts: int,
    reason: str,
    decision_ns: int,
    tax_rate: float,
    terminal_mark_proxy: bool = False,
) -> dict[str, Any]:
    row = _trade_row(
        trading_date=market.trading_date,
        strategy=variant.variant_id,
        rolling_points=_primary_parameter(variant),
        contract=contract,
        fill=fill,
        delta_contracts=delta_contracts,
        reason=reason,
        decision_ns=decision_ns,
        multiplier=OPTION_MULTIPLIER,
        fee_per_side=FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT],
        # Apply the shared statutory per-contract rounding below.  The legacy
        # helper multiplies first and therefore cannot represent the official
        # whole-TWD per-contract rule for quantities greater than one.
        tax_rate=0.0,
        slippage_points=0.0,
        terminal_mark_proxy=terminal_mark_proxy,
    )
    row.update(
        {
            "instrument_type": "option",
            "product": OPTION_PRODUCT,
            "contract_multiplier_twd_per_point": OPTION_MULTIPLIER,
        }
    )
    quantity = abs(int(delta_contracts))
    transaction_tax = quantity * option_premium_transaction_tax_twd(
        fill.price,
        multiplier_twd_per_point=OPTION_MULTIPLIER,
        tax_rate=tax_rate,
    )
    row["transaction_tax_twd"] = transaction_tax
    row["net_cash_flow_twd"] = (
        float(row["gross_cash_flow_twd"])
        - float(row["fixed_fee_twd"])
        - transaction_tax
        - float(row["slippage_cost_twd"])
    )
    return _tag_trade(row, variant)


def _future_trade(
    *,
    market: DayMarket,
    variant: Variant,
    product: str,
    fill: Fill,
    delta_contracts: int,
    reason: str,
    decision_ns: int,
    terminal_mark_proxy: bool = False,
) -> dict[str, Any]:
    normalized = str(product).strip().upper()
    if normalized not in FUTURES_MULTIPLIERS:
        raise ValueError(f"unsupported futures product: {product}")
    quantity = abs(int(delta_contracts))
    multiplier = FUTURES_MULTIPLIERS[normalized]
    fixed_fee = quantity * FIXED_FEES_PER_CONTRACT_SIDE[normalized]
    gross_cash_flow = -float(delta_contracts) * fill.price * multiplier
    transaction_tax = quantity * taifex_tax_per_contract_twd(
        fill.price,
        multiplier_twd_per_point=multiplier,
        tax_rate=stock_index_futures_tax_rate(market.trading_date),
    )
    row: dict[str, Any] = {
        "trading_date": market.trading_date,
        "strategy": variant.variant_id,
        "rolling_points": _primary_parameter(variant),
        "series": f"{normalized}_front_month",
        "strike": None,
        "option_right": None,
        "decision_ts": _datetime_from_ns(decision_ns),
        "fill_ts": _datetime_from_ns(fill.event_ns),
        "fill_delay_seconds": (fill.event_ns - decision_ns) / 1_000_000_000.0,
        "price_points": fill.price,
        "delta_contracts": int(delta_contracts),
        "reason": reason,
        "terminal_mark_proxy": terminal_mark_proxy,
        "gross_cash_flow_twd": gross_cash_flow,
        "fixed_fee_twd": fixed_fee,
        "transaction_tax_twd": transaction_tax,
        "slippage_cost_twd": 0.0,
        "net_cash_flow_twd": gross_cash_flow - fixed_fee - transaction_tax,
        "instrument_type": "future",
        "product": normalized,
        "contract_multiplier_twd_per_point": multiplier,
    }
    return _tag_trade(row, variant)


def _open_option_contracts(
    market: DayMarket,
    *,
    variant: Variant,
    decision_ns: int,
    close_decision_ns: int,
    contracts: Mapping[str, OptionContract],
    series: str,
    expiry: date,
    opening_underlying: float,
    tax_rate: float,
    reason: str,
) -> tuple[OpenOptions, list[dict[str, Any]]]:
    fills = {
        right: market.first_option_trade_after(
            contract, decision_ns, before_ns=close_decision_ns
        )
        for right, contract in contracts.items()
    }
    if any(fill is None for fill in fills.values()):
        raise ValueError(f"{market.trading_date}: opening option pair could not fill")
    rows: list[dict[str, Any]] = []
    for right in ("C", "P"):
        fill = fills[right]
        assert fill is not None
        rows.append(
            _option_trade(
                market=market,
                variant=variant,
                contract=contracts[right],
                fill=fill,
                delta_contracts=1,
                reason=reason,
                decision_ns=decision_ns,
                tax_rate=tax_rate,
            )
        )
    position_open_ns = {
        right: int(fills[right].event_ns)  # type: ignore[union-attr]
        for right in ("C", "P")
    }
    opening_premium = sum(
        float(fills[right].price) * OPTION_MULTIPLIER  # type: ignore[union-attr]
        for right in ("C", "P")
    )
    return (
        OpenOptions(
            series=series,
            expiry=expiry,
            positions=dict(contracts),
            position_open_ns=position_open_ns,
            opening_underlying=opening_underlying,
            opening_premium_twd=opening_premium,
            opening_strikes={right: contracts[right].strike for right in ("C", "P")},
            available_after_ns=max(position_open_ns.values()),
        ),
        rows,
    )


def _open_atm(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    tax_rate: float,
) -> tuple[OpenOptions, list[dict[str, Any]]]:
    underlying = market.underlying_at_or_before(selection_ns)
    if underlying is None:
        raise ValueError(f"{market.trading_date}: no TX by opening selection")
    series, expiry, strike = market.select_atm_pair(
        decision_ns=selection_ns, underlying_price=underlying
    )
    contracts = {
        right: OptionContract(series, strike, right) for right in ("C", "P")
    }
    return _open_option_contracts(
        market,
        variant=variant,
        decision_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        contracts=contracts,
        series=series,
        expiry=expiry,
        opening_underlying=underlying,
        tax_rate=tax_rate,
        reason="open_atm_straddle",
    )


def _terminal_option_rows(
    market: DayMarket,
    *,
    variant: Variant,
    positions: Mapping[str, OptionContract],
    position_open_ns: Mapping[str, int],
    session_end_ns: int,
    tax_rate: float,
) -> tuple[list[dict[str, Any]], float]:
    rows: list[dict[str, Any]] = []
    staleness: list[float] = []
    for right in ("C", "P"):
        contract = positions[right]
        fill = market.last_option_trade_before(contract, session_end_ns)
        if fill is None or fill.event_ns < int(position_open_ns[right]):
            raise ValueError(
                f"{market.trading_date}: terminal option mark unavailable after open"
            )
        staleness.append((session_end_ns - fill.event_ns) / 1_000_000_000.0)
        rows.append(
            _option_trade(
                market=market,
                variant=variant,
                contract=contract,
                fill=fill,
                delta_contracts=-1,
                reason="daily_terminal_last_trade_mark",
                decision_ns=session_end_ns,
                tax_rate=tax_rate,
                terminal_mark_proxy=True,
            )
        )
    return rows, max(staleness)


def _daily_outcome(
    *,
    market: DayMarket,
    variant: Variant,
    trades: Sequence[dict[str, Any]],
    opening_premium_twd: float,
    opening_underlying: float | None,
    opening_strikes: Mapping[str, float] | None,
    option_series: str | None,
    option_expiry: date | None,
    roll_count: int = 0,
    recenter_count: int = 0,
    hedge_count: int = 0,
    exit_reason: str = "daily_terminal",
    terminal_staleness_seconds: float | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    summary = _summarize_trade_rows(trades)
    option_sides = sum(
        abs(int(row["delta_contracts"]))
        for row in trades
        if row["instrument_type"] == "option"
    )
    futures_sides = sum(
        abs(int(row["delta_contracts"]))
        for row in trades
        if row["instrument_type"] == "future"
    )
    payload: dict[str, Any] = {
        "trading_date": market.trading_date,
        "benchmark_family": variant.family,
        "variant_id": variant.variant_id,
        "variant_role": variant.role,
        "parameters_json": _parameter_json(variant),
        "option_series": option_series,
        "option_expiry": option_expiry,
        "opening_underlying_price": opening_underlying,
        "opening_call_strike": (
            float(opening_strikes["C"]) if opening_strikes is not None else None
        ),
        "opening_put_strike": (
            float(opening_strikes["P"]) if opening_strikes is not None else None
        ),
        "opening_premium_twd": float(opening_premium_twd),
        "roll_count": int(roll_count),
        "recenter_count": int(recenter_count),
        "hedge_count": int(hedge_count),
        "trade_sides": int(option_sides + futures_sides),
        "option_trade_sides": int(option_sides),
        "futures_trade_sides": int(futures_sides),
        "exit_reason": exit_reason,
        "terminal_mark_max_staleness_seconds": terminal_staleness_seconds,
        "return_on_opening_premium": (
            summary["net_after_fee_twd"] / opening_premium_twd
            if opening_premium_twd > 0.0
            else None
        ),
        **summary,
        "diagnostics_json": json.dumps(
            dict(diagnostics or {}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    }
    return payload


def _convert_existing_variant(
    market: DayMarket,
    *,
    variant: Variant,
    strategy: str,
    rolling_points: int,
    selection_time: time,
    close_decision_time: time,
    session_end_time: time,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    daily, rows = _simulate_variant(
        market,
        strategy=strategy,
        rolling_points=rolling_points,
        selection_time=selection_time,
        close_decision_time=close_decision_time,
        session_end_time=session_end_time,
        multiplier=OPTION_MULTIPLIER,
        fee_per_side=FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT],
        tax_rate=tax_rate,
        slippage_points=0.0,
    )
    trades: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        row.update(
            {
                "instrument_type": "option",
                "product": OPTION_PRODUCT,
                "contract_multiplier_twd_per_point": OPTION_MULTIPLIER,
            }
        )
        trades.append(_tag_trade(row, variant))
    outcome = _daily_outcome(
        market=market,
        variant=variant,
        trades=trades,
        opening_premium_twd=float(daily["opening_premium_twd"]),
        opening_underlying=float(daily["opening_underlying_price"]),
        opening_strikes={
            "C": float(daily["opening_strike"]),
            "P": float(daily["opening_strike"]),
        },
        option_series=str(daily["option_series"]),
        option_expiry=daily["option_expiry"],
        roll_count=int(daily["roll_count"]),
        terminal_staleness_seconds=float(
            daily["terminal_mark_max_staleness_seconds"]
        ),
        diagnostics={
            "same_strike_signals": int(daily["same_strike_signals"]),
            "unfilled_roll_signals": int(daily["unfilled_roll_signals"]),
        },
    )
    return outcome, trades


def _replace_option_rights(
    market: DayMarket,
    *,
    variant: Variant,
    positions: dict[str, OptionContract],
    position_open_ns: dict[str, int],
    rights: Sequence[str],
    target_strike: float,
    decision_ns: int,
    close_decision_ns: int,
    tax_rate: float,
    reason_prefix: str,
) -> tuple[list[dict[str, Any]], int] | None:
    targets = {
        right: OptionContract(positions[right].series, target_strike, right)
        for right in rights
    }
    if all(
        math.isclose(
            positions[right].strike,
            targets[right].strike,
            rel_tol=0.0,
            abs_tol=1e-9,
        )
        for right in rights
    ):
        return [], decision_ns
    close_fills = {
        right: market.first_option_trade_after(
            positions[right], decision_ns, before_ns=close_decision_ns
        )
        for right in rights
    }
    open_fills = {
        right: market.first_option_trade_after(
            targets[right], decision_ns, before_ns=close_decision_ns
        )
        for right in rights
    }
    if any(fill is None for fill in (*close_fills.values(), *open_fills.values())):
        return None
    rows: list[dict[str, Any]] = []
    for right in rights:
        close_fill = close_fills[right]
        open_fill = open_fills[right]
        assert close_fill is not None and open_fill is not None
        rows.append(
            _option_trade(
                market=market,
                variant=variant,
                contract=positions[right],
                fill=close_fill,
                delta_contracts=-1,
                reason=f"{reason_prefix}_close_{right}",
                decision_ns=decision_ns,
                tax_rate=tax_rate,
            )
        )
        rows.append(
            _option_trade(
                market=market,
                variant=variant,
                contract=targets[right],
                fill=open_fill,
                delta_contracts=1,
                reason=f"{reason_prefix}_open_atm_{right}",
                decision_ns=decision_ns,
                tax_rate=tax_rate,
            )
        )
        positions[right] = targets[right]
        position_open_ns[right] = open_fill.event_ns
    return rows, max(
        max(fill.event_ns for fill in close_fills.values() if fill is not None),
        max(fill.event_ns for fill in open_fills.values() if fill is not None),
    )


def _simulate_fixed_tp_sl(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    tp = float(variant.parameters["tp_percent"]) / 100.0
    sl = float(variant.parameters["sl_percent"]) / 100.0
    opening_points = opened.opening_premium_twd / OPTION_MULTIPLIER
    exit_reason = "daily_terminal"
    exited = False
    start = int(
        np.searchsorted(market.tx_times_ns, opened.available_after_ns, side="right")
    )
    stop = int(
        np.searchsorted(market.tx_times_ns, close_decision_ns, side="left")
    )
    for decision_ns in market.tx_times_ns[start:stop]:
        marks = {
            right: market.last_option_trade_at_or_before(
                opened.positions[right], int(decision_ns)
            )
            for right in ("C", "P")
        }
        if any(fill is None for fill in marks.values()):
            continue
        marked_points = sum(float(fill.price) for fill in marks.values() if fill)
        marked_return = marked_points / opening_points - 1.0
        reason = None
        if marked_return >= tp:
            reason = "fixed_take_profit"
        elif marked_return <= -sl:
            reason = "fixed_stop_loss"
        if reason is None:
            continue
        close_fills = {
            right: market.first_option_trade_after(
                opened.positions[right],
                int(decision_ns),
                before_ns=close_decision_ns,
            )
            for right in ("C", "P")
        }
        if any(fill is None for fill in close_fills.values()):
            continue
        for right in ("C", "P"):
            fill = close_fills[right]
            assert fill is not None
            trades.append(
                _option_trade(
                    market=market,
                    variant=variant,
                    contract=opened.positions[right],
                    fill=fill,
                    delta_contracts=-1,
                    reason=reason,
                    decision_ns=int(decision_ns),
                    tax_rate=tax_rate,
                )
            )
        exit_reason = reason
        exited = True
        break
    terminal_staleness = None
    if not exited:
        terminal, terminal_staleness = _terminal_option_rows(
            market,
            variant=variant,
            positions=opened.positions,
            position_open_ns=opened.position_open_ns,
            session_end_ns=session_end_ns,
            tax_rate=tax_rate,
        )
        trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            exit_reason=exit_reason,
            terminal_staleness_seconds=terminal_staleness,
        ),
        trades,
    )


def _simulate_full_recenter(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    positions = dict(opened.positions)
    position_open_ns = dict(opened.position_open_ns)
    threshold = float(variant.parameters["recenter_points"])
    anchor = opened.opening_underlying
    search_after = opened.available_after_ns
    recenter_count = 0
    same_strike = 0
    while recenter_count < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(
            np.searchsorted(market.tx_times_ns, close_decision_ns, side="left")
        )
        if start >= stop:
            break
        offsets = np.abs(market.tx_prices[start:stop] - anchor)
        hits = np.flatnonzero(offsets >= threshold)
        if not len(hits):
            break
        index = start + int(hits[0])
        decision_ns = int(market.tx_times_ns[index])
        trigger = float(market.tx_prices[index])
        _series, _expiry, strike = market.select_atm_pair(
            decision_ns=decision_ns,
            underlying_price=trigger,
            required_series=opened.series,
        )
        replaced = _replace_option_rights(
            market,
            variant=variant,
            positions=positions,
            position_open_ns=position_open_ns,
            rights=("C", "P"),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=close_decision_ns,
            tax_rate=tax_rate,
            reason_prefix="full_recenter",
        )
        if replaced is None:
            break
        rows, available_after = replaced
        if not rows:
            same_strike += 1
            search_after = decision_ns
            continue
        trades.extend(rows)
        recenter_count += 1
        anchor = trigger
        search_after = available_after
    terminal, staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=positions,
        position_open_ns=position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            recenter_count=recenter_count,
            terminal_staleness_seconds=staleness,
            diagnostics={"same_strike_signals": same_strike},
        ),
        trades,
    )


def _simulate_long_strangle(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    underlying = market.underlying_at_or_before(selection_ns)
    if underlying is None:
        raise ValueError(f"{market.trading_date}: no TX by opening selection")
    series, expiry, _atm = market.select_atm_pair(
        decision_ns=selection_ns, underlying_price=underlying
    )
    distance = float(variant.parameters["strangle_distance_points"])
    contracts = {
        "C": market.select_option_contract(
            decision_ns=selection_ns,
            series=series,
            right="C",
            target_strike=underlying + distance,
        ),
        "P": market.select_option_contract(
            decision_ns=selection_ns,
            series=series,
            right="P",
            target_strike=underlying - distance,
        ),
    }
    opened, trades = _open_option_contracts(
        market,
        variant=variant,
        decision_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        contracts=contracts,
        series=series,
        expiry=expiry,
        opening_underlying=underlying,
        tax_rate=tax_rate,
        reason="open_long_strangle",
    )
    terminal, staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=opened.positions,
        position_open_ns=opened.position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            terminal_staleness_seconds=staleness,
        ),
        trades,
    )


def _simulate_time_recenter(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    positions = dict(opened.positions)
    position_open_ns = dict(opened.position_open_ns)
    minutes = int(variant.parameters["interval_minutes"])
    interval_ns = minutes * 60 * 1_000_000_000
    decision_ns = selection_ns + interval_ns
    available_after = opened.available_after_ns
    recenter_count = 0
    same_strike = 0
    while decision_ns < close_decision_ns:
        if decision_ns <= available_after:
            decision_ns += interval_ns
            continue
        underlying = market.underlying_at_or_before(decision_ns)
        if underlying is None:
            decision_ns += interval_ns
            continue
        _series, _expiry, strike = market.select_atm_pair(
            decision_ns=decision_ns,
            underlying_price=underlying,
            required_series=opened.series,
        )
        replaced = _replace_option_rights(
            market,
            variant=variant,
            positions=positions,
            position_open_ns=position_open_ns,
            rights=("C", "P"),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=close_decision_ns,
            tax_rate=tax_rate,
            reason_prefix=f"time_recenter_{minutes}m",
        )
        if replaced is None:
            decision_ns += interval_ns
            continue
        rows, available_after = replaced
        if rows:
            trades.extend(rows)
            recenter_count += 1
        else:
            same_strike += 1
        decision_ns += interval_ns
    terminal, staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=positions,
        position_open_ns=position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            recenter_count=recenter_count,
            terminal_staleness_seconds=staleness,
            diagnostics={"same_strike_signals": same_strike},
        ),
        trades,
    )


def _simulate_ratchet_roll(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    positions = dict(opened.positions)
    position_open_ns = dict(opened.position_open_ns)
    threshold = float(variant.parameters["rolling_points"])
    high_ratchet = opened.opening_underlying
    low_ratchet = opened.opening_underlying
    search_after = opened.available_after_ns
    roll_count = 0
    same_strike = 0
    while roll_count < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(
            np.searchsorted(market.tx_times_ns, close_decision_ns, side="left")
        )
        if start >= stop:
            break
        prices = market.tx_prices[start:stop]
        up_hits = np.flatnonzero(prices >= high_ratchet + threshold)
        down_hits = np.flatnonzero(prices <= low_ratchet - threshold)
        if not len(up_hits) and not len(down_hits):
            break
        up_offset = int(up_hits[0]) if len(up_hits) else len(prices) + 1
        down_offset = int(down_hits[0]) if len(down_hits) else len(prices) + 1
        is_up = up_offset <= down_offset
        index = start + (up_offset if is_up else down_offset)
        decision_ns = int(market.tx_times_ns[index])
        trigger = float(market.tx_prices[index])
        right = "P" if is_up else "C"
        _series, _expiry, strike = market.select_atm_pair(
            decision_ns=decision_ns,
            underlying_price=trigger,
            required_series=opened.series,
        )
        replaced = _replace_option_rights(
            market,
            variant=variant,
            positions=positions,
            position_open_ns=position_open_ns,
            rights=(right,),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=close_decision_ns,
            tax_rate=tax_rate,
            reason_prefix="ratchet_high" if is_up else "ratchet_low",
        )
        if replaced is None:
            break
        rows, available_after = replaced
        if not rows:
            same_strike += 1
            search_after = decision_ns
            continue
        trades.extend(rows)
        roll_count += 1
        if is_up:
            high_ratchet = trigger
        else:
            low_ratchet = trigger
        search_after = available_after
    terminal, staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=positions,
        position_open_ns=position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            roll_count=roll_count,
            terminal_staleness_seconds=staleness,
            diagnostics={
                "same_strike_signals": same_strike,
                "high_ratchet": high_ratchet,
                "low_ratchet": low_ratchet,
            },
        ),
        trades,
    )


def _random_seed(*values: object) -> int:
    payload = "|".join(str(value) for value in values).encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def _simulate_random_roll_once(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
    rolled_right: str,
    target_roll_count: int,
    candidate_indices: np.ndarray,
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    positions = dict(opened.positions)
    position_open_ns = dict(opened.position_open_ns)
    available_after = opened.available_after_ns
    roll_count = 0
    same_strike = 0
    for index in np.sort(candidate_indices):
        if roll_count >= target_roll_count:
            break
        decision_ns = int(market.tx_times_ns[int(index)])
        if decision_ns <= available_after:
            continue
        trigger = float(market.tx_prices[int(index)])
        _series, _expiry, strike = market.select_atm_pair(
            decision_ns=decision_ns,
            underlying_price=trigger,
            required_series=opened.series,
        )
        replaced = _replace_option_rights(
            market,
            variant=variant,
            positions=positions,
            position_open_ns=position_open_ns,
            rights=(rolled_right,),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=close_decision_ns,
            tax_rate=tax_rate,
            reason_prefix=f"random_roll_{rolled_right}",
        )
        if replaced is None:
            continue
        rows, available_after = replaced
        if not rows:
            same_strike += 1
            continue
        trades.extend(rows)
        roll_count += 1
    if roll_count != target_roll_count:
        return None
    terminal, staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=positions,
        position_open_ns=position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            roll_count=roll_count,
            terminal_staleness_seconds=staleness,
            diagnostics={
                "matched_candidate_roll_count": target_roll_count,
                "same_strike_signals": same_strike,
                "research_control_uses_realized_trade_count": True,
            },
        ),
        trades,
    )


def _simulate_random_roll(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
    rolled_right: str,
    target_roll_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    start = int(np.searchsorted(market.tx_times_ns, selection_ns, side="right"))
    stop = int(
        np.searchsorted(market.tx_times_ns, close_decision_ns, side="left")
    )
    all_indices = np.arange(start, stop, dtype=np.int64)
    if target_roll_count == 0:
        selected = np.empty(0, dtype=np.int64)
        outcome = _simulate_random_roll_once(
            market,
            variant=variant,
            selection_ns=selection_ns,
            close_decision_ns=close_decision_ns,
            session_end_ns=session_end_ns,
            tax_rate=tax_rate,
            rolled_right=rolled_right,
            target_roll_count=0,
            candidate_indices=selected,
        )
        assert outcome is not None
        return outcome
    if len(all_indices) < target_roll_count:
        raise ValueError(f"{market.trading_date}: insufficient random trigger seconds")
    rng = np.random.default_rng(
        _random_seed(
            market.trading_date,
            variant.variant_id,
            variant.parameters.get("random_seed", 20260807),
        )
    )
    order = rng.permutation(all_indices)
    sample_size = min(len(order), max(target_roll_count * 8, target_roll_count + 32))
    while True:
        outcome = _simulate_random_roll_once(
            market,
            variant=variant,
            selection_ns=selection_ns,
            close_decision_ns=close_decision_ns,
            session_end_ns=session_end_ns,
            tax_rate=tax_rate,
            rolled_right=rolled_right,
            target_roll_count=target_roll_count,
            candidate_indices=order[:sample_size],
        )
        if outcome is not None:
            return outcome
        if sample_size >= len(order):
            raise ValueError(
                f"{market.trading_date}: random control could not match "
                f"{target_roll_count} successful rolls"
            )
        sample_size = min(len(order), sample_size * 2)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _straddle_price_and_delta(
    *,
    spot: float,
    strike: float,
    years_to_expiry: float,
    volatility: float,
) -> tuple[float, float]:
    if spot <= 0.0 or strike <= 0.0 or years_to_expiry <= 0.0:
        raise ValueError("Black-Scholes inputs must be positive")
    sigma_sqrt_t = volatility * math.sqrt(years_to_expiry)
    if sigma_sqrt_t <= 1e-12:
        intrinsic = abs(spot - strike)
        delta = 1.0 if spot > strike else (-1.0 if spot < strike else 0.0)
        return intrinsic, delta
    d1 = (
        math.log(spot / strike) + 0.5 * volatility * volatility * years_to_expiry
    ) / sigma_sqrt_t
    d2 = d1 - sigma_sqrt_t
    call = spot * _normal_cdf(d1) - strike * _normal_cdf(d2)
    put = strike * _normal_cdf(-d2) - spot * _normal_cdf(-d1)
    return call + put, 2.0 * _normal_cdf(d1) - 1.0


def _causal_straddle_delta(
    *,
    spot: float,
    strike: float,
    years_to_expiry: float,
    observed_straddle_price: float,
) -> tuple[float, float] | None:
    if (
        not math.isfinite(spot)
        or not math.isfinite(strike)
        or not math.isfinite(years_to_expiry)
        or not math.isfinite(observed_straddle_price)
        or spot <= 0.0
        or strike <= 0.0
        or years_to_expiry <= 0.0
        or observed_straddle_price <= 0.0
    ):
        return None
    intrinsic = abs(spot - strike)
    if observed_straddle_price < intrinsic - 1e-6:
        return None
    low = 1e-4
    high = 10.0
    low_price, low_delta = _straddle_price_and_delta(
        spot=spot,
        strike=strike,
        years_to_expiry=years_to_expiry,
        volatility=low,
    )
    high_price, _high_delta = _straddle_price_and_delta(
        spot=spot,
        strike=strike,
        years_to_expiry=years_to_expiry,
        volatility=high,
    )
    if observed_straddle_price <= low_price + 1e-8:
        return low_delta, low
    if observed_straddle_price > high_price + 1e-6:
        return None
    for _ in range(64):
        mid = (low + high) / 2.0
        price, _delta = _straddle_price_and_delta(
            spot=spot,
            strike=strike,
            years_to_expiry=years_to_expiry,
            volatility=mid,
        )
        if price < observed_straddle_price:
            low = mid
        else:
            high = mid
    volatility = (low + high) / 2.0
    _price, delta = _straddle_price_and_delta(
        spot=spot,
        strike=strike,
        years_to_expiry=years_to_expiry,
        volatility=volatility,
    )
    return delta, volatility


def _round_nearest_contract(value: float) -> int:
    if value >= 0.0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def _simulate_gamma_scalping(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
    tax_rate: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    opened, trades = _open_atm(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=close_decision_ns,
        tax_rate=tax_rate,
    )
    product = str(variant.parameters["futures_product"]).upper()
    interval_seconds = int(variant.parameters.get("hedge_interval_seconds", 60))
    delta_band_raw = variant.parameters.get("delta_band")
    delta_band = None if delta_band_raw is None else float(delta_band_raw)
    future_delta_unit = FUTURES_MULTIPLIERS[product] / OPTION_MULTIPLIER
    future_position = 0
    future_position_open_ns: int | None = None
    hedge_count = 0
    delta_observations = 0
    missing_delta_observations = 0
    abs_net_delta_before: list[float] = []
    implied_vols: list[float] = []
    interval_ns = interval_seconds * 1_000_000_000
    decision_ns = selection_ns + interval_ns
    available_after = opened.available_after_ns
    expiry_dt = datetime.combine(opened.expiry, time(13, 30), tzinfo=TAIPEI)
    while decision_ns < close_decision_ns:
        if decision_ns <= available_after:
            decision_ns += interval_ns
            continue
        spot = market.underlying_at_or_before(decision_ns)
        marks = {
            right: market.last_option_trade_at_or_before(
                opened.positions[right], decision_ns
            )
            for right in ("C", "P")
        }
        if spot is None or any(fill is None for fill in marks.values()):
            missing_delta_observations += 1
            decision_ns += interval_ns
            continue
        years = max(
            (expiry_dt - _datetime_from_ns(decision_ns)).total_seconds(), 1.0
        ) / (365.0 * 24.0 * 60.0 * 60.0)
        observed = sum(float(fill.price) for fill in marks.values() if fill)
        estimate = _causal_straddle_delta(
            spot=spot,
            strike=opened.positions["C"].strike,
            years_to_expiry=years,
            observed_straddle_price=observed,
        )
        if estimate is None:
            missing_delta_observations += 1
            decision_ns += interval_ns
            continue
        option_delta, implied_vol = estimate
        net_delta = option_delta + future_position * future_delta_unit
        delta_observations += 1
        abs_net_delta_before.append(abs(net_delta))
        implied_vols.append(implied_vol)
        should_hedge = delta_band is None or abs(net_delta) > delta_band
        desired_position = _round_nearest_contract(-option_delta / future_delta_unit)
        delta_contracts = desired_position - future_position
        if should_hedge and delta_contracts:
            fill = market.first_future_trade_after(
                product, decision_ns, before_ns=close_decision_ns
            )
            if fill is not None:
                trades.append(
                    _future_trade(
                        market=market,
                        variant=variant,
                        product=product,
                        fill=fill,
                        delta_contracts=delta_contracts,
                        reason=(
                            "periodic_delta_hedge"
                            if delta_band is None
                            else "delta_band_hedge"
                        ),
                        decision_ns=decision_ns,
                    )
                )
                future_position = desired_position
                future_position_open_ns = fill.event_ns
                available_after = fill.event_ns
                hedge_count += 1
        decision_ns += interval_ns
    terminal, option_staleness = _terminal_option_rows(
        market,
        variant=variant,
        positions=opened.positions,
        position_open_ns=opened.position_open_ns,
        session_end_ns=session_end_ns,
        tax_rate=tax_rate,
    )
    trades.extend(terminal)
    terminal_staleness = option_staleness
    if future_position:
        fill = market.last_future_trade_before(product, session_end_ns)
        if (
            fill is None
            or future_position_open_ns is None
            or fill.event_ns < future_position_open_ns
        ):
            raise ValueError(
                f"{market.trading_date}: terminal {product} mark unavailable"
            )
        trades.append(
            _future_trade(
                market=market,
                variant=variant,
                product=product,
                fill=fill,
                delta_contracts=-future_position,
                reason="daily_terminal_future_last_trade_mark",
                decision_ns=session_end_ns,
                terminal_mark_proxy=True,
            )
        )
        terminal_staleness = max(
            terminal_staleness,
            (session_end_ns - fill.event_ns) / 1_000_000_000.0,
        )
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=opened.opening_premium_twd,
            opening_underlying=opened.opening_underlying,
            opening_strikes=opened.opening_strikes,
            option_series=opened.series,
            option_expiry=opened.expiry,
            hedge_count=hedge_count,
            terminal_staleness_seconds=terminal_staleness,
            diagnostics={
                "delta_model": "Black-Scholes zero-rate common IV from causal C+P marks",
                "delta_observations": delta_observations,
                "missing_delta_observations": missing_delta_observations,
                "mean_abs_net_delta_before_hedge": (
                    float(np.mean(abs_net_delta_before))
                    if abs_net_delta_before
                    else None
                ),
                "median_implied_volatility": (
                    float(np.median(implied_vols)) if implied_vols else None
                ),
                "future_delta_unit": future_delta_unit,
            },
        ),
        trades,
    )


def _simulate_underlying(
    market: DayMarket,
    *,
    variant: Variant,
    selection_ns: int,
    close_decision_ns: int,
    session_end_ns: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    product = str(variant.parameters["futures_product"]).upper()
    direction = str(variant.parameters["direction"])
    delta = 1 if direction == "long" else -1
    entry = market.first_future_trade_after(
        product, selection_ns, before_ns=close_decision_ns
    )
    if entry is None:
        raise ValueError(f"{market.trading_date}: {product} entry unavailable")
    terminal = market.last_future_trade_before(product, session_end_ns)
    if terminal is None or terminal.event_ns < entry.event_ns:
        raise ValueError(f"{market.trading_date}: {product} terminal mark unavailable")
    trades = [
        _future_trade(
            market=market,
            variant=variant,
            product=product,
            fill=entry,
            delta_contracts=delta,
            reason=f"open_underlying_{direction}",
            decision_ns=selection_ns,
        ),
        _future_trade(
            market=market,
            variant=variant,
            product=product,
            fill=terminal,
            delta_contracts=-delta,
            reason="daily_terminal_future_last_trade_mark",
            decision_ns=session_end_ns,
            terminal_mark_proxy=True,
        ),
    ]
    opening_underlying = market.underlying_at_or_before(selection_ns)
    staleness = (session_end_ns - terminal.event_ns) / 1_000_000_000.0
    return (
        _daily_outcome(
            market=market,
            variant=variant,
            trades=trades,
            opening_premium_twd=0.0,
            opening_underlying=opening_underlying,
            opening_strikes=None,
            option_series=None,
            option_expiry=None,
            terminal_staleness_seconds=staleness,
            diagnostics={
                "notional_multiplier_twd_per_point": FUTURES_MULTIPLIERS[product]
            },
        ),
        trades,
    )


def _build_variants(
    *,
    rolling_points: Sequence[int],
    tp_percent: Sequence[float],
    sl_percent: Sequence[float],
    strangle_distances: Sequence[int],
    time_recenter_minutes: Sequence[int],
    delta_bands: Sequence[float],
    random_seed: int,
) -> list[Variant]:
    variants = [
        _variant(
            FAMILY_CLASSIC,
            STRATEGY_CLASSIC,
            rolling_points=CLASSIC_NO_ROLL_THRESHOLD,
        )
    ]
    for strategy in (STRATEGY_ROLL_OTM, STRATEGY_ROLL_ITM):
        for threshold in rolling_points:
            variants.append(
                _variant(
                    FAMILY_CANDIDATE,
                    f"{strategy}__{threshold:04d}",
                    role="candidate",
                    source_strategy=strategy,
                    rolling_points=threshold,
                )
            )
    for tp in tp_percent:
        for sl in sl_percent:
            variants.append(
                _variant(
                    FAMILY_TP_SL,
                    f"fixed_tp_sl__tp{int(round(tp)):03d}_sl{int(round(sl)):03d}",
                    tp_percent=float(tp),
                    sl_percent=float(sl),
                )
            )
    for threshold in rolling_points:
        variants.append(
            _variant(
                FAMILY_FULL_RECENTER,
                f"full_recenter__{threshold:04d}",
                recenter_points=threshold,
            )
        )
    for distance in strangle_distances:
        variants.append(
            _variant(
                FAMILY_STRANGLE,
                f"long_strangle__{distance:04d}",
                strangle_distance_points=distance,
            )
        )
    for product in ("MTX", "TMF"):
        variants.append(
            _variant(
                FAMILY_GAMMA,
                f"gamma_scalping__{product.lower()}__60s",
                futures_product=product,
                hedge_interval_seconds=60,
                delta_band=None,
            )
        )
    for band in delta_bands:
        variants.append(
            _variant(
                FAMILY_DELTA_BAND,
                f"delta_band_gamma__tmf__{int(round(band * 100)):02d}",
                futures_product="TMF",
                hedge_interval_seconds=60,
                delta_band=float(band),
            )
        )
    for minutes in time_recenter_minutes:
        variants.append(
            _variant(
                FAMILY_TIME_RECENTER,
                f"time_recenter__{minutes:03d}m",
                interval_minutes=minutes,
            )
        )
    for threshold in rolling_points:
        variants.append(
            _variant(
                FAMILY_RATCHET,
                f"ratchet_roll__{threshold:04d}",
                rolling_points=threshold,
            )
        )
    for product in ("TX", "MTX", "TMF"):
        for direction in ("long", "short"):
            variants.append(
                _variant(
                    FAMILY_UNDERLYING,
                    f"underlying__{product.lower()}__{direction}",
                    futures_product=product,
                    direction=direction,
                )
            )
    for strategy in (STRATEGY_ROLL_OTM, STRATEGY_ROLL_ITM):
        rolled_right = "P" if strategy == STRATEGY_ROLL_OTM else "C"
        for threshold in rolling_points:
            variants.append(
                _variant(
                    FAMILY_RANDOM,
                    f"random_control__{strategy}__{threshold:04d}",
                    source_strategy=strategy,
                    rolled_right=rolled_right,
                    rolling_points=threshold,
                    random_seed=random_seed,
                )
            )
    if len({variant.variant_id for variant in variants}) != len(variants):
        raise ValueError("benchmark variant IDs are not unique")
    return variants


def _aggregate_results(daily: pl.DataFrame) -> list[dict[str, Any]]:
    classic = daily.filter(pl.col("benchmark_family") == FAMILY_CLASSIC).select(
        ["trading_date", pl.col("net_after_fee_twd").alias("classic_fee_pnl")]
    )
    results: list[dict[str, Any]] = []
    for key, frame in daily.partition_by("variant_id", as_dict=True).items():
        (variant_id,) = key
        frame = frame.sort("trading_date")
        fee_pnl = frame["net_after_fee_twd"].to_numpy().astype(np.float64)
        fee_tax_pnl = frame["net_after_fee_tax_twd"].to_numpy().astype(np.float64)
        cumulative = np.cumsum(fee_pnl)
        peak = np.maximum.accumulate(np.r_[0.0, cumulative])
        drawdown = np.r_[0.0, cumulative] - peak
        positive = fee_pnl[fee_pnl > 0.0].sum()
        negative = -fee_pnl[fee_pnl < 0.0].sum()
        paired = frame.join(classic, on="trading_date", how="inner")
        delta = paired["net_after_fee_twd"] - paired["classic_fee_pnl"]
        results.append(
            {
                "benchmark_family": str(frame["benchmark_family"][0]),
                "variant_id": str(variant_id),
                "variant_role": str(frame["variant_role"][0]),
                "parameters": json.loads(str(frame["parameters_json"][0])),
                "days": frame.height,
                "gross_pnl_twd": float(frame["gross_pnl_twd"].sum()),
                "fixed_fees_twd": float(frame["fixed_fees_twd"].sum()),
                "transaction_tax_twd": float(frame["transaction_tax_twd"].sum()),
                "slippage_cost_twd": float(frame["slippage_cost_twd"].sum()),
                "net_after_fee_twd": float(fee_pnl.sum()),
                "net_after_fee_tax_twd": float(fee_tax_pnl.sum()),
                "average_daily_after_fee_twd": float(fee_pnl.mean()),
                "median_daily_after_fee_twd": float(np.median(fee_pnl)),
                "win_rate_after_fee": float(np.mean(fee_pnl > 0.0)),
                "profit_factor_after_fee": (
                    float(positive / negative) if negative > 0.0 else None
                ),
                "maximum_drawdown_after_fee_twd": float(drawdown.min()),
                "best_day_after_fee_twd": float(fee_pnl.max()),
                "worst_day_after_fee_twd": float(fee_pnl.min()),
                "total_trade_sides": int(frame["trade_sides"].sum()),
                "total_option_trade_sides": int(
                    frame["option_trade_sides"].sum()
                ),
                "total_futures_trade_sides": int(
                    frame["futures_trade_sides"].sum()
                ),
                "total_rolls": int(frame["roll_count"].sum()),
                "total_recenters": int(frame["recenter_count"].sum()),
                "total_hedges": int(frame["hedge_count"].sum()),
                "paired_delta_vs_classic_twd": float(delta.sum()),
                "paired_positive_days_vs_classic": int((delta > 0.0).sum()),
                "paired_negative_days_vs_classic": int((delta < 0.0).sum()),
                "terminal_mark_staleness_median_seconds": (
                    float(frame["terminal_mark_max_staleness_seconds"].median())
                    if frame["terminal_mark_max_staleness_seconds"].drop_nulls().len()
                    else None
                ),
                "terminal_mark_staleness_max_seconds": (
                    float(frame["terminal_mark_max_staleness_seconds"].max())
                    if frame["terminal_mark_max_staleness_seconds"].drop_nulls().len()
                    else None
                ),
            }
        )
    return sorted(
        results, key=lambda row: (row["benchmark_family"], row["variant_id"])
    )


def _validate_outputs(
    *,
    daily: pl.DataFrame,
    trades: pl.DataFrame,
    trading_days: int,
    variants: Sequence[Variant],
) -> None:
    expected_daily = trading_days * len(variants)
    if daily.height != expected_daily:
        raise ValueError(f"daily coverage mismatch: {daily.height}/{expected_daily}")
    if daily.select("variant_id").n_unique() != len(variants):
        raise ValueError("daily variant coverage mismatch")
    if daily.select("trading_date").n_unique() != trading_days:
        raise ValueError("daily trading-date coverage mismatch")
    if float(trades["slippage_cost_twd"].abs().max()) != 0.0:
        raise ValueError("artificial slippage must remain zero")
    causal = trades.filter(~pl.col("terminal_mark_proxy"))
    if causal.filter(pl.col("fill_ts") <= pl.col("decision_ts")).height:
        raise ValueError("non-terminal trade is not strictly causal")
    expected_fee = [
        abs(int(quantity)) * FIXED_FEES_PER_CONTRACT_SIDE[str(product)]
        for product, quantity in trades.select(["product", "delta_contracts"]).iter_rows()
    ]
    if not np.allclose(expected_fee, trades["fixed_fee_twd"].to_numpy()):
        raise ValueError("product-specific fixed fees do not reconcile")
    open_positions = (
        trades.group_by(
            [
                "trading_date",
                "variant_id",
                "instrument_type",
                "product",
                "series",
                "strike",
                "option_right",
            ]
        )
        .agg(pl.col("delta_contracts").sum().alias("contracts"))
        .filter(pl.col("contracts") != 0)
    )
    if open_positions.height:
        raise ValueError(f"terminal positions are not flat: {open_positions.head(5)}")
    random_daily = daily.filter(pl.col("benchmark_family") == FAMILY_RANDOM)
    candidate_daily = daily.filter(pl.col("benchmark_family") == FAMILY_CANDIDATE)
    candidate_counts = {
        (
            row[0],
            str(json.loads(row[1])["source_strategy"]),
            int(json.loads(row[1])["rolling_points"]),
        ): int(row[2])
        for row in candidate_daily.select(
            ["trading_date", "parameters_json", "roll_count"]
        ).iter_rows()
    }
    for trading_date, parameters_json, roll_count in random_daily.select(
        ["trading_date", "parameters_json", "roll_count"]
    ).iter_rows():
        parameters = json.loads(str(parameters_json))
        key = (
            trading_date,
            str(parameters["source_strategy"]),
            int(parameters["rolling_points"]),
        )
        if int(roll_count) != candidate_counts[key]:
            raise ValueError(f"random control roll-count mismatch: {key}")


def run_benchmarks(
    *,
    raw_root: Path,
    output_dir: Path,
    rolling_points: Iterable[int],
    tp_percent: Iterable[float],
    sl_percent: Iterable[float],
    strangle_distances: Iterable[int],
    time_recenter_minutes: Iterable[int],
    delta_bands: Iterable[float],
    selection_time: time,
    close_decision_time: time,
    session_end_time: time,
    option_tax_rate: float,
    random_seed: int,
) -> dict[str, Any]:
    source_manifest, source_sha, trading_dates = _verify_manifest(raw_root)
    thresholds = tuple(sorted(set(int(value) for value in rolling_points)))
    strangles = tuple(sorted(set(int(value) for value in strangle_distances)))
    tp_values = tuple(sorted(set(float(value) for value in tp_percent)))
    sl_values = tuple(sorted(set(float(value) for value in sl_percent)))
    time_values = tuple(sorted(set(int(value) for value in time_recenter_minutes)))
    band_values = tuple(sorted(set(float(value) for value in delta_bands)))
    if not thresholds or any(value <= 0 for value in thresholds):
        raise ValueError("rolling_points must contain positive integers")
    if not strangles or any(value <= 0 for value in strangles):
        raise ValueError("strangle_distances must contain positive integers")
    if not tp_values or not sl_values or any(
        value <= 0.0 for value in (*tp_values, *sl_values)
    ):
        raise ValueError("TP/SL percentages must be positive")
    if set(time_values) != {15, 30, 60}:
        raise ValueError("time recenter benchmark must contain 15, 30, and 60 minutes")
    if set(band_values) != {0.2, 0.3}:
        raise ValueError("delta-band benchmark must contain 0.2 and 0.3")
    if not math.isfinite(option_tax_rate) or option_tax_rate < 0.0:
        raise ValueError("option_tax_rate must be finite and non-negative")
    variants = _build_variants(
        rolling_points=thresholds,
        tp_percent=tp_values,
        sl_percent=sl_values,
        strangle_distances=strangles,
        time_recenter_minutes=time_values,
        delta_bands=band_values,
        random_seed=random_seed,
    )
    daily_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for day_index, trading_date in enumerate(trading_dates, start=1):
        print(
            f"[option-benchmarks] date={trading_date} "
            f"progress={day_index}/{len(trading_dates)} variants={len(variants)}",
            flush=True,
        )
        market = _build_day_market(
            raw_root,
            trading_date,
            futures_products=("TX", "MTX", "TMF"),
        )
        selection_ns = _ns(
            datetime.combine(trading_date, selection_time, tzinfo=TAIPEI)
        )
        close_decision_ns = _ns(
            datetime.combine(trading_date, close_decision_time, tzinfo=TAIPEI)
        )
        session_end_ns = _ns(
            datetime.combine(trading_date, session_end_time, tzinfo=TAIPEI)
        )
        candidate_roll_counts: dict[tuple[str, int], int] = {}
        for variant in variants:
            try:
                if variant.family == FAMILY_CLASSIC:
                    outcome = _convert_existing_variant(
                        market,
                        variant=variant,
                        strategy=STRATEGY_CLASSIC,
                        rolling_points=0,
                        selection_time=selection_time,
                        close_decision_time=close_decision_time,
                        session_end_time=session_end_time,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_CANDIDATE:
                    strategy = str(variant.parameters["source_strategy"])
                    threshold = int(variant.parameters["rolling_points"])
                    outcome = _convert_existing_variant(
                        market,
                        variant=variant,
                        strategy=strategy,
                        rolling_points=threshold,
                        selection_time=selection_time,
                        close_decision_time=close_decision_time,
                        session_end_time=session_end_time,
                        tax_rate=option_tax_rate,
                    )
                    candidate_roll_counts[(strategy, threshold)] = int(
                        outcome[0]["roll_count"]
                    )
                elif variant.family == FAMILY_TP_SL:
                    outcome = _simulate_fixed_tp_sl(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_FULL_RECENTER:
                    outcome = _simulate_full_recenter(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_STRANGLE:
                    outcome = _simulate_long_strangle(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family in {FAMILY_GAMMA, FAMILY_DELTA_BAND}:
                    outcome = _simulate_gamma_scalping(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_TIME_RECENTER:
                    outcome = _simulate_time_recenter(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_RATCHET:
                    outcome = _simulate_ratchet_roll(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                    )
                elif variant.family == FAMILY_UNDERLYING:
                    outcome = _simulate_underlying(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                    )
                elif variant.family == FAMILY_RANDOM:
                    strategy = str(variant.parameters["source_strategy"])
                    threshold = int(variant.parameters["rolling_points"])
                    outcome = _simulate_random_roll(
                        market,
                        variant=variant,
                        selection_ns=selection_ns,
                        close_decision_ns=close_decision_ns,
                        session_end_ns=session_end_ns,
                        tax_rate=option_tax_rate,
                        rolled_right=str(variant.parameters["rolled_right"]),
                        target_roll_count=candidate_roll_counts[(strategy, threshold)],
                    )
                else:
                    raise ValueError(f"unsupported benchmark family: {variant.family}")
            except Exception as exc:
                failures.append(
                    {
                        "trading_date": trading_date.isoformat(),
                        "variant_id": variant.variant_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            daily, trades = outcome
            daily_rows.append(daily)
            trade_rows.extend(trades)
    if failures:
        failure_path = output_dir / "failures.json"
        output_dir.mkdir(parents=True, exist_ok=True)
        _atomic_json(failure_path, failures)
        raise RuntimeError(
            f"{len(failures)} benchmark-days failed; first={failures[:5]}"
        )
    daily = pl.DataFrame(daily_rows).sort(["benchmark_family", "variant_id", "trading_date"])
    trades = pl.DataFrame(trade_rows).sort(
        ["benchmark_family", "variant_id", "trading_date", "fill_ts", "product"]
    )
    _validate_outputs(
        daily=daily,
        trades=trades,
        trading_days=len(trading_dates),
        variants=variants,
    )
    results = _aggregate_results(daily)
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "daily_benchmarks.parquet"
    trades_path = output_dir / "trades.parquet"
    _atomic_parquet(daily, daily_path)
    _atomic_parquet(trades, trades_path)
    catalog = []
    for family, name, purpose in BENCHMARK_CATALOG:
        family_variants = [variant for variant in variants if variant.family == family]
        catalog.append(
            {
                "family": family,
                "name": name,
                "purpose": purpose,
                "variant_count": len(family_variants),
                "variant_ids": [variant.variant_id for variant in family_variants],
            }
        )
    summary: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "source": TAIFEX_TRADE_PROXY_SOURCE,
        "source_manifest": str(raw_root / "manifest.json"),
        "source_manifest_sha256": source_sha,
        "source_parser_contract_version": source_manifest["parser_contract_version"],
        "date_start": trading_dates[0].isoformat(),
        "date_end": trading_dates[-1].isoformat(),
        "trading_days": len(trading_dates),
        "variant_count": len(variants),
        "benchmark_family_count": len(BENCHMARK_CATALOG),
        "daily_result_rows": daily.height,
        "trade_rows": trades.height,
        "fixed_fees_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE,
        "contract_multipliers_twd_per_point": {
            **FUTURES_MULTIPLIERS,
            OPTION_PRODUCT: OPTION_MULTIPLIER,
        },
        "parameters": {
            "rolling_points": list(thresholds),
            "tp_percent": list(tp_values),
            "sl_percent": list(sl_values),
            "strangle_distance_points": list(strangles),
            "time_recenter_minutes": list(time_values),
            "delta_bands": list(band_values),
            "selection_time": selection_time.isoformat(),
            "close_decision_time": close_decision_time.isoformat(),
            "session_end_time": session_end_time.isoformat(),
            "option_transaction_tax_rate_secondary_layer": option_tax_rate,
            "artificial_slippage_points_per_side": 0.0,
            "contracts_per_option_leg": 1,
            "random_seed": random_seed,
        },
        "benchmark_catalog": catalog,
        "execution_contract": {
            "decision_clock": "completed TAIFEX whole-second event",
            "non_terminal_execution": "first strictly later product transaction print",
            "terminal_execution": "last product transaction before 13:45 mark proxy",
            "historical_bidask_available": False,
            "one_lot_fill_assumption": "guaranteed; no historical quantity or depth cap",
            "matched_quantity_usage": "price aggregation only; never fill capacity",
            "artificial_slippage": False,
            "daily_flattened": True,
            "futures_price_source": "actual TX/MTX/TMF front-month transaction rows from receipt-backed official ZIP",
            "gamma_delta_model": "zero-rate Black-Scholes common IV solved from causal held Call+Put marks",
            "random_control": "uses candidate realized daily roll count by design; trigger times are deterministic shuffled controls, not a deployable causal signal",
        },
        "evaluation_stages": {
            "primary": "fixed fees only",
            "secondary": "option statutory transaction tax only; no futures tax layer yet",
            "pressure_tests": "not run",
        },
        "artifacts": {
            "daily_benchmarks": str(daily_path),
            "daily_benchmarks_sha256": _sha256_path(daily_path),
            "trades": str(trades_path),
            "trades_sha256": _sha256_path(trades_path),
        },
        "results": results,
    }
    _atomic_json(output_dir / "summary.json", summary)
    print(
        json.dumps(
            {
                "status": "complete",
                "trading_days": len(trading_dates),
                "variants": len(variants),
                "daily_rows": daily.height,
                "trade_rows": trades.height,
                "benchmark_families": len(BENCHMARK_CATALOG),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root", type=Path, default=Path("data_tw_index_derivatives_ticks")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/research/taifex_option_benchmarks"),
    )
    parser.add_argument(
        "--rolling-points",
        nargs="+",
        type=int,
        default=list(range(50, 1001, 50)),
    )
    parser.add_argument(
        "--tp-percent", nargs="+", type=float, default=[25.0, 50.0, 100.0]
    )
    parser.add_argument(
        "--sl-percent", nargs="+", type=float, default=[25.0, 50.0]
    )
    parser.add_argument(
        "--strangle-distances",
        nargs="+",
        type=int,
        default=list(range(50, 1001, 50)),
    )
    parser.add_argument(
        "--time-recenter-minutes", nargs="+", type=int, default=[15, 30, 60]
    )
    parser.add_argument(
        "--delta-bands", nargs="+", type=float, default=[0.2, 0.3]
    )
    parser.add_argument("--selection-time", default="08:50:00")
    parser.add_argument("--close-decision-time", default="13:40:00")
    parser.add_argument("--session-end-time", default="13:45:00")
    parser.add_argument("--option-tax-rate", type=float, default=0.001)
    parser.add_argument("--random-seed", type=int, default=20260807)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_benchmarks(
        raw_root=args.raw_root,
        output_dir=args.output_dir,
        rolling_points=args.rolling_points,
        tp_percent=args.tp_percent,
        sl_percent=args.sl_percent,
        strangle_distances=args.strangle_distances,
        time_recenter_minutes=args.time_recenter_minutes,
        delta_bands=args.delta_bands,
        selection_time=_parse_time(args.selection_time),
        close_decision_time=_parse_time(args.close_decision_time),
        session_end_time=_parse_time(args.session_end_time),
        option_tax_rate=float(args.option_tax_rate),
        random_seed=int(args.random_seed),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
