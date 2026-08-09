#!/usr/bin/env python3
"""Run all 160 TXO controls with positions carried to official expiry.

The variant catalog and execution helpers come from
``backtest_taifex_option_benchmarks``.  This runner changes only the lifecycle:
option and futures positions remain open across sessions, daily settlement is a
non-trading mark, and the next series is entered only after the held series has
reached its official TAIFEX final-settlement date.
"""

from __future__ import annotations

import argparse
import copy
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
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
    Fill,
    OptionContract,
    STRATEGY_ROLL_ITM,
    STRATEGY_ROLL_OTM,
    _build_day_market,
    _datetime_from_ns,
    _ns,
    _parse_time,
    _sha256_path,
    _summarize_trade_rows,
    _verify_manifest,
)
from scripts.backtest_taifex_opening_straddle_hold_to_expiry import (  # noqa: E402
    OFFICIAL_FINAL_SETTLEMENT_URL,
    _load_official_final_settlements,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    BENCHMARK_CATALOG,
    FAMILY_CANDIDATE,
    FAMILY_CLASSIC,
    FAMILY_DELTA_BAND,
    FAMILY_FULL_RECENTER,
    FAMILY_GAMMA,
    FAMILY_RANDOM,
    FAMILY_RATCHET,
    FAMILY_STRANGLE,
    FAMILY_TIME_RECENTER,
    FAMILY_TP_SL,
    FAMILY_UNDERLYING,
    FIXED_FEES_PER_CONTRACT_SIDE,
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
    OPTION_PRODUCT,
    Variant,
    _build_variants,
    _causal_straddle_delta,
    _future_trade,
    _open_atm,
    _open_option_contracts,
    _option_trade,
    _parameter_json,
    _random_seed,
    _replace_option_rights,
    _round_nearest_contract,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIFEX_TRADE_PROXY_SOURCE,
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
    load_taifex_option_daily_contract_rows,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    TAIFEX_OPTION_PREMIUM_TAX_RATE,
    option_cash_settlement_transaction_tax_twd,
    stock_index_futures_tax_rate,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 2
HOLDING_POLICY: Final[str] = "official_expiry_carry"
OFFICIAL_EXPIRY_TIME: Final[time] = time(13, 30)
FUTURE_EXPIRY_CLOSE_TIME: Final[time] = time(13, 25)
DEFAULT_FINAL_SETTLEMENT_PATH: Final[Path] = Path(
    "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)
DEFAULT_OPTION_DAILY_ROOT: Final[Path] = Path(
    "data_tw_index_options_daily/raw/ranges"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_option_benchmarks_expiry_carry"
)


@dataclass(slots=True)
class CarryState:
    variant: Variant
    cycle_id: str
    series: str
    expiry: date
    opening_date: date
    opening_underlying: float
    opening_premium_twd: float
    opening_strikes: dict[str, float]
    positions: dict[str, OptionContract] = field(default_factory=dict)
    position_open_ns: dict[str, int] = field(default_factory=dict)
    anchor: float = 0.0
    high_ratchet: float = 0.0
    low_ratchet: float = 0.0
    future_product: str | None = None
    future_position: int = 0
    future_position_open_ns: int | None = None
    exited_early: bool = False
    random_pending_rolls: int = 0


def _settlement_by_series(
    settlements: Mapping[tuple[date, str], Mapping[str, object]],
) -> dict[str, tuple[date, Mapping[str, object]]]:
    output: dict[str, tuple[date, Mapping[str, object]]] = {}
    for (settlement_date, series), payload in settlements.items():
        previous = output.get(series)
        if previous is not None and previous[0] != settlement_date:
            raise ValueError(f"multiple official settlement dates for {series}")
        output[series] = (settlement_date, payload)
    return output


def _daily_source_paths(root: Path, start: date, end: date) -> list[Path]:
    paths: list[Path] = []
    cursor = date(start.year, start.month, 1)
    while cursor <= end:
        prefix = cursor.strftime("%Y-%m-")
        matches = sorted(root.glob(f"{prefix}*_TXO.csv"))
        if len(matches) != 1:
            raise FileNotFoundError(
                f"expected one official TXO daily file for {cursor:%Y-%m}: {matches}"
            )
        paths.extend(matches)
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
    return paths


def _select_complete_cycle(
    market: Any,
    *,
    selection_ns: int,
    settlement_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
    verified_dates: set[date],
) -> tuple[str, date, float, float] | None:
    underlying = market.underlying_at_or_before(selection_ns)
    if underlying is None:
        raise ValueError(f"{market.trading_date}: no TX price at cycle selection")
    series, _calendar_expiry, strike = market.select_atm_pair(
        decision_ns=selection_ns,
        underlying_price=underlying,
    )
    official = settlement_by_series.get(series)
    if official is None or official[0] not in verified_dates:
        return None
    if official[0] < market.trading_date:
        raise ValueError(f"{market.trading_date}: selected expired series {series}")
    return series, official[0], strike, underlying


def _enter_cycle(
    market: Any,
    *,
    variant: Variant,
    cycle: tuple[str, date, float, float],
    selection_ns: int,
    trade_deadline_ns: int,
    option_tax_rate: float,
) -> tuple[CarryState, list[dict[str, Any]]]:
    series, expiry, atm_strike, underlying = cycle
    cycle_id = f"{variant.variant_id}__{market.trading_date}__{series}"
    if variant.family == FAMILY_UNDERLYING:
        product = str(variant.parameters["futures_product"]).upper()
        direction = 1 if str(variant.parameters["direction"]) == "long" else -1
        fill = market.first_future_trade_after(
            product, selection_ns, before_ns=trade_deadline_ns
        )
        if fill is None:
            raise ValueError(f"{market.trading_date}: no {product} cycle entry")
        row = _future_trade(
            market=market,
            variant=variant,
            product=product,
            fill=fill,
            delta_contracts=direction,
            reason="open_underlying_expiry_cycle",
            decision_ns=selection_ns,
        )
        return (
            CarryState(
                variant=variant,
                cycle_id=cycle_id,
                series=series,
                expiry=expiry,
                opening_date=market.trading_date,
                opening_underlying=underlying,
                opening_premium_twd=0.0,
                opening_strikes={},
                anchor=underlying,
                high_ratchet=underlying,
                low_ratchet=underlying,
                future_product=product,
                future_position=direction,
                future_position_open_ns=fill.event_ns,
            ),
            [row],
        )

    if variant.family == FAMILY_STRANGLE:
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
        opened, rows = _open_option_contracts(
            market,
            variant=variant,
            decision_ns=selection_ns,
            close_decision_ns=trade_deadline_ns,
            contracts=contracts,
            series=series,
            expiry=expiry,
            opening_underlying=underlying,
            tax_rate=option_tax_rate,
            reason="open_long_strangle_expiry_cycle",
        )
    else:
        opened, rows = _open_atm(
            market,
            variant=variant,
            selection_ns=selection_ns,
            close_decision_ns=trade_deadline_ns,
            tax_rate=option_tax_rate,
        )
        if opened.series != series:
            raise ValueError("cycle selector diverged from shared ATM opener")
        opened = type(opened)(
            series=opened.series,
            expiry=expiry,
            positions=opened.positions,
            position_open_ns=opened.position_open_ns,
            opening_underlying=opened.opening_underlying,
            opening_premium_twd=opened.opening_premium_twd,
            opening_strikes=opened.opening_strikes,
            available_after_ns=opened.available_after_ns,
        )
    product = None
    if variant.family in {FAMILY_GAMMA, FAMILY_DELTA_BAND}:
        product = str(variant.parameters["futures_product"]).upper()
    return (
        CarryState(
            variant=variant,
            cycle_id=cycle_id,
            series=series,
            expiry=expiry,
            opening_date=market.trading_date,
            opening_underlying=underlying,
            opening_premium_twd=opened.opening_premium_twd,
            opening_strikes=opened.opening_strikes,
            positions=dict(opened.positions),
            position_open_ns=dict(opened.position_open_ns),
            anchor=underlying,
            high_ratchet=underlying,
            low_ratchet=underlying,
            future_product=product,
        ),
        rows,
    )


def _close_options(
    market: Any,
    *,
    state: CarryState,
    decision_ns: int,
    trade_deadline_ns: int,
    option_tax_rate: float,
    reason: str,
) -> list[dict[str, Any]] | None:
    fills = {
        right: market.first_option_trade_after(
            contract, decision_ns, before_ns=trade_deadline_ns
        )
        for right, contract in state.positions.items()
    }
    if any(fill is None for fill in fills.values()):
        return None
    rows: list[dict[str, Any]] = []
    for right, contract in tuple(state.positions.items()):
        fill = fills[right]
        assert fill is not None
        rows.append(
            _option_trade(
                market=market,
                variant=state.variant,
                contract=contract,
                fill=fill,
                delta_contracts=-1,
                reason=reason,
                decision_ns=decision_ns,
                tax_rate=option_tax_rate,
            )
        )
    state.positions.clear()
    state.position_open_ns.clear()
    return rows


def _process_candidate(
    market: Any,
    *,
    state: CarryState,
    start_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    strategy = str(state.variant.parameters["source_strategy"])
    rolled_right = "P" if strategy == STRATEGY_ROLL_OTM else "C"
    threshold = float(state.variant.parameters["rolling_points"])
    rows: list[dict[str, Any]] = []
    rolls = same = unfilled = 0
    search_after = start_ns
    while rolls < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(np.searchsorted(market.tx_times_ns, deadline_ns, side="left"))
        if start >= stop:
            break
        hits = np.flatnonzero(
            market.tx_prices[start:stop] >= state.anchor + threshold
        )
        if not len(hits):
            break
        index = start + int(hits[0])
        decision_ns = int(market.tx_times_ns[index])
        trigger = float(market.tx_prices[index])
        try:
            _series, _expiry, strike = market.select_atm_pair(
                decision_ns=decision_ns,
                underlying_price=trigger,
                required_series=state.series,
            )
        except ValueError:
            search_after = decision_ns
            continue
        replaced = _replace_option_rights(
            market,
            variant=state.variant,
            positions=state.positions,
            position_open_ns=state.position_open_ns,
            rights=(rolled_right,),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=deadline_ns,
            tax_rate=option_tax_rate,
            reason_prefix=f"expiry_carry_roll_{rolled_right}",
        )
        if replaced is None:
            unfilled += 1
            break
        replacement_rows, available_after = replaced
        if not replacement_rows:
            same += 1
            search_after = decision_ns
            continue
        rows.extend(replacement_rows)
        rolls += 1
        state.anchor = trigger
        search_after = available_after
    return rows, rolls, {"same_strike_signals": same, "unfilled_roll_signals": unfilled}


def _process_full_recenter(
    market: Any,
    *,
    state: CarryState,
    start_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    threshold = float(state.variant.parameters["recenter_points"])
    rows: list[dict[str, Any]] = []
    count = same = 0
    search_after = start_ns
    while count < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(np.searchsorted(market.tx_times_ns, deadline_ns, side="left"))
        if start >= stop:
            break
        hits = np.flatnonzero(
            np.abs(market.tx_prices[start:stop] - state.anchor) >= threshold
        )
        if not len(hits):
            break
        index = start + int(hits[0])
        decision_ns = int(market.tx_times_ns[index])
        trigger = float(market.tx_prices[index])
        try:
            _series, _expiry, strike = market.select_atm_pair(
                decision_ns=decision_ns,
                underlying_price=trigger,
                required_series=state.series,
            )
        except ValueError:
            search_after = decision_ns
            continue
        replaced = _replace_option_rights(
            market,
            variant=state.variant,
            positions=state.positions,
            position_open_ns=state.position_open_ns,
            rights=("C", "P"),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=deadline_ns,
            tax_rate=option_tax_rate,
            reason_prefix="expiry_carry_full_recenter",
        )
        if replaced is None:
            break
        replacement_rows, available_after = replaced
        if not replacement_rows:
            same += 1
            search_after = decision_ns
            continue
        rows.extend(replacement_rows)
        count += 1
        state.anchor = trigger
        search_after = available_after
    return rows, count, {"same_strike_signals": same}


def _process_ratchet(
    market: Any,
    *,
    state: CarryState,
    start_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    threshold = float(state.variant.parameters["rolling_points"])
    rows: list[dict[str, Any]] = []
    count = same = 0
    search_after = start_ns
    while count < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(np.searchsorted(market.tx_times_ns, deadline_ns, side="left"))
        if start >= stop:
            break
        prices = market.tx_prices[start:stop]
        up = np.flatnonzero(prices >= state.high_ratchet + threshold)
        down = np.flatnonzero(prices <= state.low_ratchet - threshold)
        if not len(up) and not len(down):
            break
        up_offset = int(up[0]) if len(up) else len(prices) + 1
        down_offset = int(down[0]) if len(down) else len(prices) + 1
        is_up = up_offset <= down_offset
        index = start + (up_offset if is_up else down_offset)
        decision_ns = int(market.tx_times_ns[index])
        trigger = float(market.tx_prices[index])
        right = "P" if is_up else "C"
        try:
            _series, _expiry, strike = market.select_atm_pair(
                decision_ns=decision_ns,
                underlying_price=trigger,
                required_series=state.series,
            )
        except ValueError:
            search_after = decision_ns
            continue
        replaced = _replace_option_rights(
            market,
            variant=state.variant,
            positions=state.positions,
            position_open_ns=state.position_open_ns,
            rights=(right,),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=deadline_ns,
            tax_rate=option_tax_rate,
            reason_prefix="expiry_carry_ratchet_high" if is_up else "expiry_carry_ratchet_low",
        )
        if replaced is None:
            break
        replacement_rows, available_after = replaced
        if not replacement_rows:
            same += 1
            search_after = decision_ns
            continue
        rows.extend(replacement_rows)
        count += 1
        if is_up:
            state.high_ratchet = trigger
        else:
            state.low_ratchet = trigger
        search_after = available_after
    return rows, count, {"same_strike_signals": same}


def _process_time_recenter(
    market: Any,
    *,
    state: CarryState,
    selection_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    minutes = int(state.variant.parameters["interval_minutes"])
    interval_ns = minutes * 60 * 1_000_000_000
    decision_ns = selection_ns + interval_ns
    rows: list[dict[str, Any]] = []
    count = same = 0
    available_after = selection_ns
    while decision_ns < deadline_ns:
        if decision_ns <= available_after:
            decision_ns += interval_ns
            continue
        underlying = market.underlying_at_or_before(decision_ns)
        if underlying is None:
            decision_ns += interval_ns
            continue
        try:
            _series, _expiry, strike = market.select_atm_pair(
                decision_ns=decision_ns,
                underlying_price=underlying,
                required_series=state.series,
            )
        except ValueError:
            decision_ns += interval_ns
            continue
        replaced = _replace_option_rights(
            market,
            variant=state.variant,
            positions=state.positions,
            position_open_ns=state.position_open_ns,
            rights=("C", "P"),
            target_strike=strike,
            decision_ns=decision_ns,
            close_decision_ns=deadline_ns,
            tax_rate=option_tax_rate,
            reason_prefix=f"expiry_carry_time_recenter_{minutes}m",
        )
        if replaced is not None:
            replacement_rows, available_after = replaced
            if replacement_rows:
                rows.extend(replacement_rows)
                count += 1
            else:
                same += 1
        decision_ns += interval_ns
    return rows, count, {"same_strike_signals": same}


def _process_tp_sl(
    market: Any,
    *,
    state: CarryState,
    start_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
) -> tuple[list[dict[str, Any]], str | None]:
    if not state.positions or state.exited_early:
        return [], None
    tp = float(state.variant.parameters["tp_percent"]) / 100.0
    sl = float(state.variant.parameters["sl_percent"]) / 100.0
    opening_points = state.opening_premium_twd / OPTION_MULTIPLIER
    start = int(np.searchsorted(market.tx_times_ns, start_ns, side="right"))
    stop = int(np.searchsorted(market.tx_times_ns, deadline_ns, side="left"))
    for raw_ns in market.tx_times_ns[start:stop]:
        decision_ns = int(raw_ns)
        marks = {
            right: market.last_option_trade_at_or_before(contract, decision_ns)
            for right, contract in state.positions.items()
        }
        if any(mark is None for mark in marks.values()):
            continue
        marked_return = (
            sum(float(mark.price) for mark in marks.values() if mark) / opening_points
            - 1.0
        )
        reason = (
            "fixed_take_profit"
            if marked_return >= tp
            else ("fixed_stop_loss" if marked_return <= -sl else None)
        )
        if reason is None:
            continue
        rows = _close_options(
            market,
            state=state,
            decision_ns=decision_ns,
            trade_deadline_ns=deadline_ns,
            option_tax_rate=option_tax_rate,
            reason=reason,
        )
        if rows is not None:
            state.exited_early = True
            return rows, reason
    return [], None


def _process_gamma(
    market: Any,
    *,
    state: CarryState,
    selection_ns: int,
    deadline_ns: int,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    product = str(state.future_product)
    interval_seconds = int(state.variant.parameters.get("hedge_interval_seconds", 60))
    band_raw = state.variant.parameters.get("delta_band")
    band = None if band_raw is None else float(band_raw)
    future_delta_unit = FUTURES_MULTIPLIERS[product] / OPTION_MULTIPLIER
    interval_ns = interval_seconds * 1_000_000_000
    decision_ns = selection_ns + interval_ns
    expiry_dt = datetime.combine(state.expiry, OFFICIAL_EXPIRY_TIME, tzinfo=TAIPEI)
    rows: list[dict[str, Any]] = []
    hedges = observations = missing = 0
    abs_deltas: list[float] = []
    implied_vols: list[float] = []
    while decision_ns < deadline_ns:
        spot = market.underlying_at_or_before(decision_ns)
        marks = {
            right: market.last_option_trade_at_or_before(contract, decision_ns)
            for right, contract in state.positions.items()
        }
        if spot is None or any(mark is None for mark in marks.values()):
            missing += 1
            decision_ns += interval_ns
            continue
        years = max(
            (expiry_dt - _datetime_from_ns(decision_ns)).total_seconds(), 1.0
        ) / (365.0 * 24.0 * 60.0 * 60.0)
        estimate = _causal_straddle_delta(
            spot=spot,
            strike=state.positions["C"].strike,
            years_to_expiry=years,
            observed_straddle_price=sum(float(mark.price) for mark in marks.values() if mark),
        )
        if estimate is None:
            missing += 1
            decision_ns += interval_ns
            continue
        option_delta, implied_vol = estimate
        net_delta = option_delta + state.future_position * future_delta_unit
        observations += 1
        abs_deltas.append(abs(net_delta))
        implied_vols.append(implied_vol)
        desired = _round_nearest_contract(-option_delta / future_delta_unit)
        delta_contracts = desired - state.future_position
        if (band is None or abs(net_delta) > band) and delta_contracts:
            fill = market.first_future_trade_after(
                product, decision_ns, before_ns=deadline_ns
            )
            if fill is not None:
                rows.append(
                    _future_trade(
                        market=market,
                        variant=state.variant,
                        product=product,
                        fill=fill,
                        delta_contracts=delta_contracts,
                        reason="expiry_carry_delta_hedge",
                        decision_ns=decision_ns,
                    )
                )
                state.future_position = desired
                state.future_position_open_ns = fill.event_ns
                hedges += 1
        decision_ns += interval_ns
    return rows, hedges, {
        "delta_observations": observations,
        "missing_delta_observations": missing,
        "mean_abs_net_delta_before_hedge": float(np.mean(abs_deltas)) if abs_deltas else None,
        "median_implied_volatility": float(np.median(implied_vols)) if implied_vols else None,
    }


def _process_random(
    market: Any,
    *,
    state: CarryState,
    selection_ns: int,
    deadline_ns: int,
    option_tax_rate: float,
    target_roll_count: int,
) -> tuple[list[dict[str, Any]], int, dict[str, Any]]:
    strategy = str(state.variant.parameters["source_strategy"])
    right = "P" if strategy == STRATEGY_ROLL_OTM else "C"
    start = int(np.searchsorted(market.tx_times_ns, selection_ns, side="right"))
    stop = int(np.searchsorted(market.tx_times_ns, deadline_ns, side="left"))
    indices = np.arange(start, stop, dtype=np.int64)
    rng = np.random.default_rng(
        _random_seed(
            market.trading_date,
            state.variant.variant_id,
            state.variant.parameters.get("random_seed", 20260807),
        )
    )
    if target_roll_count == 0:
        return [], 0, {
            "matched_candidate_roll_count": 0,
            "same_strike_signals": 0,
        }
    order = rng.permutation(indices)
    sample_size = min(
        len(order), max(target_roll_count * 8, target_roll_count + 32)
    )
    while True:
        trial = copy.deepcopy(state)
        rows: list[dict[str, Any]] = []
        count = same = 0
        available_after = selection_ns
        # Randomize which candidate seconds are retained, then restore causal
        # time order before applying them.  A shuffled execution order would
        # use future events before past events and is not a valid control.
        for index in np.sort(order[:sample_size]):
            if count >= target_roll_count:
                break
            decision_ns = int(market.tx_times_ns[int(index)])
            if decision_ns <= available_after:
                continue
            trigger = float(market.tx_prices[int(index)])
            try:
                _series, _expiry, strike = market.select_atm_pair(
                    decision_ns=decision_ns,
                    underlying_price=trigger,
                    required_series=trial.series,
                )
            except ValueError:
                continue
            replaced = _replace_option_rights(
                market,
                variant=trial.variant,
                positions=trial.positions,
                position_open_ns=trial.position_open_ns,
                rights=(right,),
                target_strike=strike,
                decision_ns=decision_ns,
                close_decision_ns=deadline_ns,
                tax_rate=option_tax_rate,
                reason_prefix=f"expiry_carry_random_{right}",
            )
            replacement_rows: list[dict[str, Any]] = []
            if replaced is not None:
                replacement_rows, available_after = replaced
            if replaced is None or not replacement_rows:
                # A carried random-control leg can diverge far enough from the
                # candidate that the randomly selected ATM is unchanged or no
                # longer executable.  The control's defining contract is equal
                # trade count at random causal times, so fall back to a random
                # different, causally observed and subsequently tradable strike
                # in the same series/right.  This is explicitly disclosed and
                # never changes the candidate strategy itself.
                current = trial.positions[right]
                alternatives: list[float] = []
                for contract, (times, _prices) in market.option_events.items():
                    if (
                        contract.series != trial.series
                        or contract.right != right
                        or math.isclose(
                            contract.strike,
                            current.strike,
                            rel_tol=0.0,
                            abs_tol=1e-9,
                        )
                    ):
                        continue
                    before_index = int(
                        np.searchsorted(times, decision_ns, side="right")
                    )
                    if (
                        before_index > 0
                        and before_index < len(times)
                        and int(times[before_index]) < deadline_ns
                    ):
                        alternatives.append(contract.strike)
                for alternative_index in sorted(
                    range(len(alternatives)),
                    key=lambda item: (
                        abs(alternatives[item] - trigger),
                        alternatives[item],
                    ),
                ):
                    fallback = _replace_option_rights(
                        market,
                        variant=trial.variant,
                        positions=trial.positions,
                        position_open_ns=trial.position_open_ns,
                        rights=(right,),
                        target_strike=alternatives[int(alternative_index)],
                        decision_ns=decision_ns,
                        close_decision_ns=deadline_ns,
                        tax_rate=option_tax_rate,
                        reason_prefix=f"expiry_carry_random_fallback_{right}",
                    )
                    if fallback is None or not fallback[0]:
                        continue
                    replacement_rows, available_after = fallback
                    break
            if not replacement_rows:
                same += 1
                continue
            rows.extend(replacement_rows)
            count += 1
        if count == target_roll_count:
            state.positions = trial.positions
            state.position_open_ns = trial.position_open_ns
            return rows, count, {
                "matched_candidate_roll_count": target_roll_count,
                "same_strike_signals": same,
                "random_target_contract": (
                    "causal ATM, with random causally tradable same-series strike fallback"
                ),
            }
        if sample_size >= len(order):
            state.positions = trial.positions
            state.position_open_ns = trial.position_open_ns
            return rows, count, {
                "matched_candidate_roll_count_requested": target_roll_count,
                "matched_candidate_roll_count_executed": count,
                "same_strike_signals": same,
                "random_target_contract": (
                    "causal ATM, with nearest causally tradable same-series strike fallback"
                ),
            }
        sample_size = min(len(order), sample_size * 2)


def _settle_expiry(
    market: Any,
    *,
    state: CarryState,
    settlement_payload: Mapping[str, object],
    session_end_ns: int,
) -> list[dict[str, Any]]:
    settlement = float(settlement_payload["settlement_price"])
    settlement_ns = _ns(
        datetime.combine(market.trading_date, OFFICIAL_EXPIRY_TIME, tzinfo=TAIPEI)
    )
    rows: list[dict[str, Any]] = []
    if state.future_position:
        product = str(state.future_product)
        future_close_ns = _ns(
            datetime.combine(
                market.trading_date, FUTURE_EXPIRY_CLOSE_TIME, tzinfo=TAIPEI
            )
        )
        fill = market.first_future_trade_after(
            product, future_close_ns, before_ns=settlement_ns
        )
        if fill is None:
            raise ValueError(f"{market.trading_date}: no {product} expiry hedge close")
        rows.append(
            _future_trade(
                market=market,
                variant=state.variant,
                product=product,
                fill=fill,
                delta_contracts=-state.future_position,
                reason="close_future_before_option_expiry",
                decision_ns=future_close_ns,
            )
        )
        state.future_position = 0
        state.future_position_open_ns = None
    if state.positions:
        for right, contract in tuple(state.positions.items()):
            intrinsic = (
                max(settlement - contract.strike, 0.0)
                if right == "C"
                else max(contract.strike - settlement, 0.0)
            )
            row = _option_trade(
                market=market,
                variant=state.variant,
                contract=contract,
                fill=Fill(event_ns=settlement_ns, price=intrinsic),
                delta_contracts=-1,
                reason="official_expiry_cash_settlement",
                decision_ns=settlement_ns,
                tax_rate=0.0,
            )
            row["fixed_fee_twd"] = 0.0
            row["fill_delay_seconds"] = 0.0
            row["execution_kind"] = "official_cash_settlement"
            rows.append(row)
        settlement_tax = option_cash_settlement_transaction_tax_twd(
            settlement,
            settlement_date=market.trading_date,
            multiplier_twd_per_point=OPTION_MULTIPLIER,
        )
        for row in rows:
            if row.get("instrument_type") != "option":
                continue
            # A rolled straddle can finish with both legs in the money when
            # their strikes cross.  Cash-settlement tax is assessed per
            # exercised contract, so every positive-intrinsic leg is taxed.
            tax = settlement_tax if float(row["price_points"]) > 0.0 else 0.0
            row["transaction_tax_twd"] = tax
            row["net_cash_flow_twd"] = float(row["gross_cash_flow_twd"]) - tax
        state.positions.clear()
        state.position_open_ns.clear()
    return rows


def _mark_targets(
    snapshots: Sequence[dict[str, Any]],
) -> set[tuple[date, str, float, str]]:
    return {
        (row["trading_date"], contract.series, contract.strike, right)
        for row in snapshots
        for right, contract in row["positions"].items()
    }


def _load_daily_marks(
    *,
    source_paths: Sequence[Path],
    targets: set[tuple[date, str, float, str]],
) -> dict[tuple[date, str, float, str], dict[str, object]]:
    monthly = {target for target in targets if len(target[1]) == 6}
    weekly = targets - monthly
    output: dict[tuple[date, str, float, str], dict[str, object]] = {}
    if monthly:
        output.update(
            load_taifex_option_daily_contract_rows(
                source_paths, monthly, series_scope="monthly"
            )
        )
    if weekly:
        output.update(
            load_taifex_option_daily_contract_rows(
                source_paths, weekly, series_scope="weekly"
            )
        )
    return output


def _build_daily_rows(
    *,
    snapshots: Sequence[dict[str, Any]],
    daily_marks: Mapping[tuple[date, str, float, str], Mapping[str, object]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_variant: dict[str, list[dict[str, Any]]] = {}
    for snapshot in snapshots:
        by_variant.setdefault(snapshot["variant"].variant_id, []).append(snapshot)
    for variant_id, variant_snapshots in by_variant.items():
        gross_cash = fees = taxes = slippage = 0.0
        previous = {"gross": 0.0, "fee": 0.0, "tax": 0.0, "net": 0.0}
        for snapshot in sorted(variant_snapshots, key=lambda item: item["trading_date"]):
            trades = snapshot["trades"]
            gross_cash += sum(float(row["gross_cash_flow_twd"]) for row in trades)
            fees += sum(float(row["fixed_fee_twd"]) for row in trades)
            taxes += sum(float(row["transaction_tax_twd"]) for row in trades)
            slippage += sum(float(row["slippage_cost_twd"]) for row in trades)
            option_value = 0.0
            mark_sources: dict[str, str] = {}
            staleness: list[float] = []
            for right, contract in snapshot["positions"].items():
                key = (
                    snapshot["trading_date"],
                    contract.series,
                    contract.strike,
                    right,
                )
                mark = daily_marks.get(key)
                value = None
                source = None
                if mark is not None:
                    for field, label in (
                        ("settlement", "official_daily_settlement"),
                        ("close", "official_daily_last_trade_fallback"),
                    ):
                        raw = mark.get(field)
                        if raw is not None and math.isfinite(float(raw)) and float(raw) >= 0.0:
                            value = float(raw)
                            source = label
                            break
                if value is None:
                    fallback = snapshot["fallback_option_marks"].get(right)
                    if fallback is None:
                        raise ValueError(f"missing EOD mark for {key}")
                    value = float(fallback.price)
                    source = "tick_last_trade_fallback"
                    staleness.append(
                        (snapshot["session_end_ns"] - fallback.event_ns) / 1_000_000_000.0
                    )
                option_value += value * OPTION_MULTIPLIER
                mark_sources[right] = str(source)
            futures_value = 0.0
            if snapshot["future_position"]:
                future_mark = snapshot["future_mark"]
                if future_mark is None:
                    raise ValueError(
                        f"missing future EOD mark: {snapshot['trading_date']}/{variant_id}"
                    )
                product = str(snapshot["future_product"])
                futures_value = (
                    int(snapshot["future_position"])
                    * float(future_mark.price)
                    * FUTURES_MULTIPLIERS[product]
                )
                staleness.append(
                    (snapshot["session_end_ns"] - future_mark.event_ns) / 1_000_000_000.0
                )
            gross_equity = gross_cash + option_value + futures_value
            fee_equity = gross_equity - fees
            tax_equity = fee_equity - taxes
            net_equity = tax_equity - slippage
            daily_gross = gross_equity - previous["gross"]
            daily_fee = fee_equity - previous["fee"]
            daily_tax = tax_equity - previous["tax"]
            daily_net = net_equity - previous["net"]
            previous = {
                "gross": gross_equity,
                "fee": fee_equity,
                "tax": tax_equity,
                "net": net_equity,
            }
            state = snapshot["state"]
            variant = snapshot["variant"]
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
            rows.append(
                {
                    "trading_date": snapshot["trading_date"],
                    "benchmark_family": variant.family,
                    "variant_id": variant.variant_id,
                    "variant_role": variant.role,
                    "parameters_json": _parameter_json(variant),
                    "holding_policy": HOLDING_POLICY,
                    "cycle_id": state.cycle_id if state is not None else None,
                    "option_series": state.series if state is not None else None,
                    "option_expiry": state.expiry if state is not None else None,
                    "opening_underlying_price": state.opening_underlying if state else None,
                    "opening_call_strike": state.opening_strikes.get("C") if state else None,
                    "opening_put_strike": state.opening_strikes.get("P") if state else None,
                    "opening_premium_twd": (
                        state.opening_premium_twd
                        if state is not None and state.opening_date == snapshot["trading_date"]
                        else 0.0
                    ),
                    "is_cycle_entry": bool(snapshot["is_cycle_entry"]),
                    "is_expiry_session": bool(snapshot["is_expiry_session"]),
                    "position_carried_overnight": bool(
                        snapshot["positions"] or snapshot["future_position"]
                    ),
                    "roll_count": int(snapshot["roll_count"]),
                    "recenter_count": int(snapshot["recenter_count"]),
                    "hedge_count": int(snapshot["hedge_count"]),
                    "trade_sides": int(option_sides + futures_sides),
                    "option_trade_sides": int(option_sides),
                    "futures_trade_sides": int(futures_sides),
                    "exit_reason": snapshot["exit_reason"],
                    "terminal_mark_max_staleness_seconds": max(staleness) if staleness else None,
                    "gross_pnl_twd": daily_gross,
                    "fixed_fees_twd": sum(float(row["fixed_fee_twd"]) for row in trades),
                    "transaction_tax_twd": sum(float(row["transaction_tax_twd"]) for row in trades),
                    "slippage_cost_twd": sum(float(row["slippage_cost_twd"]) for row in trades),
                    "net_after_fee_twd": daily_fee,
                    "net_after_fee_tax_twd": daily_tax,
                    "net_pnl_twd": daily_net,
                    "marked_equity_twd": net_equity,
                    "diagnostics_json": json.dumps(
                        {**snapshot["diagnostics"], "mark_sources": mark_sources},
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                }
            )
    return rows


def _aggregate(daily: pl.DataFrame) -> list[dict[str, Any]]:
    classic = daily.filter(pl.col("benchmark_family") == FAMILY_CLASSIC).select(
        "trading_date", pl.col("net_pnl_twd").alias("classic_net_pnl")
    )
    output: list[dict[str, Any]] = []
    for frame in daily.partition_by("variant_id", maintain_order=True):
        frame = frame.sort("trading_date")
        pnl = frame["net_pnl_twd"].to_numpy().astype(np.float64)
        cumulative = np.cumsum(pnl)
        peak = np.maximum.accumulate(np.r_[0.0, cumulative])
        drawdown = np.r_[0.0, cumulative] - peak
        positive = pnl[pnl > 0.0].sum()
        negative = -pnl[pnl < 0.0].sum()
        paired = frame.join(classic, on="trading_date", how="inner")
        delta = paired["net_pnl_twd"] - paired["classic_net_pnl"]
        output.append(
            {
                "benchmark_family": str(frame["benchmark_family"][0]),
                "variant_id": str(frame["variant_id"][0]),
                "variant_role": str(frame["variant_role"][0]),
                "parameters": json.loads(str(frame["parameters_json"][0])),
                "days": frame.height,
                "gross_pnl_twd": float(frame["gross_pnl_twd"].sum()),
                "fixed_fees_twd": float(frame["fixed_fees_twd"].sum()),
                "transaction_tax_twd": float(frame["transaction_tax_twd"].sum()),
                "slippage_cost_twd": float(frame["slippage_cost_twd"].sum()),
                "net_after_fee_twd": float(frame["net_after_fee_twd"].sum()),
                "net_after_fee_tax_twd": float(frame["net_after_fee_tax_twd"].sum()),
                "net_pnl_twd": float(pnl.sum()),
                "average_daily_net_twd": float(pnl.mean()),
                "median_daily_net_twd": float(np.median(pnl)),
                "win_rate_net": float(np.mean(pnl > 0.0)),
                "profit_factor_net": float(positive / negative) if negative > 0.0 else None,
                "maximum_drawdown_net_twd": float(drawdown.min()),
                "best_day_net_twd": float(pnl.max()),
                "worst_day_net_twd": float(pnl.min()),
                "total_trade_sides": int(frame["trade_sides"].sum()),
                "total_option_trade_sides": int(frame["option_trade_sides"].sum()),
                "total_futures_trade_sides": int(frame["futures_trade_sides"].sum()),
                "total_rolls": int(frame["roll_count"].sum()),
                "total_recenters": int(frame["recenter_count"].sum()),
                "total_hedges": int(frame["hedge_count"].sum()),
                "paired_delta_vs_classic_twd": float(delta.sum()),
                "paired_positive_days_vs_classic": int((delta > 0.0).sum()),
                "paired_negative_days_vs_classic": int((delta < 0.0).sum()),
            }
        )
    return sorted(output, key=lambda row: (row["benchmark_family"], row["variant_id"]))


def _validate(
    *,
    daily: pl.DataFrame,
    trades: pl.DataFrame,
    variants: Sequence[Variant],
    trading_dates: Sequence[date],
) -> None:
    expected = len(variants) * len(trading_dates)
    if daily.height != expected:
        raise ValueError(f"daily coverage mismatch: {daily.height}/{expected}")
    if daily.select(pl.struct(["trading_date", "variant_id"]).n_unique()).item() != expected:
        raise ValueError("duplicate or missing variant/day rows")
    if daily.select("variant_id").n_unique() != len(variants):
        raise ValueError("variant coverage mismatch")
    if float(trades["slippage_cost_twd"].abs().max()) != 0.0:
        raise ValueError("artificial slippage must remain zero")
    causal = trades.filter(pl.col("reason") != "official_expiry_cash_settlement")
    if causal.filter(pl.col("fill_ts") <= pl.col("decision_ts")).height:
        raise ValueError("non-settlement execution is not strictly causal")
    positions = (
        trades.group_by(
            ["variant_id", "instrument_type", "product", "series", "strike", "option_right"]
        )
        .agg(pl.col("delta_contracts").sum().alias("contracts"))
        .filter(pl.col("contracts") != 0)
    )
    if positions.height:
        raise ValueError(f"sample-end positions are not flat: {positions.head(5)}")
    for frame in daily.partition_by("variant_id", maintain_order=True):
        variant_id = str(frame["variant_id"][0])
        ledger = trades.filter(pl.col("variant_id") == variant_id)
        expected_net = float(
            ledger["gross_cash_flow_twd"].sum()
            - ledger["fixed_fee_twd"].sum()
            - ledger["transaction_tax_twd"].sum()
            - ledger["slippage_cost_twd"].sum()
        )
        if not math.isclose(float(frame["net_pnl_twd"].sum()), expected_net, abs_tol=1e-6):
            raise ValueError(f"daily/ledger endpoint mismatch: {variant_id}")
    candidates = daily.filter(pl.col("benchmark_family") == FAMILY_CANDIDATE)
    candidate_counts: dict[tuple[str, int], int] = {}
    for raw_parameters, roll_count in candidates.select(
        "parameters_json", "roll_count"
    ).iter_rows():
        parameters = json.loads(str(raw_parameters))
        key = (
            str(parameters["source_strategy"]),
            int(parameters["rolling_points"]),
        )
        candidate_counts[key] = candidate_counts.get(key, 0) + int(roll_count)
    random_rows = daily.filter(pl.col("benchmark_family") == FAMILY_RANDOM)
    random_counts: dict[tuple[str, int], int] = {}
    for raw_parameters, roll_count in random_rows.select(
        "parameters_json", "roll_count"
    ).iter_rows():
        parameters = json.loads(str(raw_parameters))
        key = (
            str(parameters["source_strategy"]),
            int(parameters["rolling_points"]),
        )
        random_counts[key] = random_counts.get(key, 0) + int(roll_count)
    if random_counts != candidate_counts:
        raise ValueError(
            f"random/candidate sample trade-count mismatch: "
            f"candidate={candidate_counts}, random={random_counts}"
        )


def run_benchmarks(
    *,
    raw_root: Path,
    option_daily_root: Path,
    final_settlement_path: Path,
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
    verified_dates = set(trading_dates)
    settlements = _load_official_final_settlements(final_settlement_path)
    by_series = _settlement_by_series(settlements)
    thresholds = tuple(sorted(set(int(value) for value in rolling_points)))
    variants = _build_variants(
        rolling_points=thresholds,
        tp_percent=tuple(sorted(set(float(value) for value in tp_percent))),
        sl_percent=tuple(sorted(set(float(value) for value in sl_percent))),
        strangle_distances=tuple(sorted(set(int(value) for value in strangle_distances))),
        time_recenter_minutes=tuple(sorted(set(int(value) for value in time_recenter_minutes))),
        delta_bands=tuple(sorted(set(float(value) for value in delta_bands))),
        random_seed=random_seed,
    )
    if len(variants) != 160:
        raise ValueError(f"canonical benchmark matrix must have 160 variants, got {len(variants)}")
    states: dict[str, CarryState | None] = {variant.variant_id: None for variant in variants}
    snapshots: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    completed_cycles: dict[str, int] = {variant.variant_id: 0 for variant in variants}
    for day_index, trading_date in enumerate(trading_dates, start=1):
        print(
            f"[expiry-carry-160] date={trading_date} progress={day_index}/{len(trading_dates)} variants=160",
            flush=True,
        )
        market = _build_day_market(
            raw_root, trading_date, futures_products=("TX", "MTX", "TMF")
        )
        selection_ns = _ns(datetime.combine(trading_date, selection_time, tzinfo=TAIPEI))
        close_ns = _ns(datetime.combine(trading_date, close_decision_time, tzinfo=TAIPEI))
        session_end_ns = _ns(datetime.combine(trading_date, session_end_time, tzinfo=TAIPEI))
        cycle = _select_complete_cycle(
            market,
            selection_ns=selection_ns,
            settlement_by_series=by_series,
            verified_dates=verified_dates,
        )
        candidate_counts: dict[tuple[str, int], int] = {}
        for variant in variants:
            state = states[variant.variant_id]
            is_entry = False
            day_trades: list[dict[str, Any]] = []
            roll_count = recenter_count = hedge_count = 0
            exit_reason = "carry_mark"
            diagnostics: dict[str, Any] = {}
            try:
                if state is None and cycle is not None:
                    trade_deadline = min(
                        close_ns,
                        _ns(datetime.combine(trading_date, OFFICIAL_EXPIRY_TIME, tzinfo=TAIPEI))
                        if cycle[1] == trading_date
                        else close_ns,
                    )
                    state, opened = _enter_cycle(
                        market,
                        variant=variant,
                        cycle=cycle,
                        selection_ns=selection_ns,
                        trade_deadline_ns=trade_deadline,
                        option_tax_rate=option_tax_rate,
                    )
                    states[variant.variant_id] = state
                    day_trades.extend(opened)
                    is_entry = True
                if state is not None:
                    is_expiry = state.expiry == trading_date
                    deadline_ns = (
                        min(
                            close_ns,
                            _ns(
                                datetime.combine(
                                    trading_date,
                                    (
                                        FUTURE_EXPIRY_CLOSE_TIME
                                        if variant.family
                                        in {FAMILY_GAMMA, FAMILY_DELTA_BAND}
                                        else OFFICIAL_EXPIRY_TIME
                                    ),
                                    tzinfo=TAIPEI,
                                )
                            ),
                        )
                        if is_expiry
                        else close_ns
                    )
                    if variant.family == FAMILY_CANDIDATE and state.positions:
                        rows, roll_count, diagnostics = _process_candidate(
                            market,
                            state=state,
                            start_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                        )
                        day_trades.extend(rows)
                        candidate_counts[(
                            str(variant.parameters["source_strategy"]),
                            int(variant.parameters["rolling_points"]),
                        )] = roll_count
                    elif variant.family == FAMILY_TP_SL:
                        rows, reason = _process_tp_sl(
                            market,
                            state=state,
                            start_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                        )
                        day_trades.extend(rows)
                        if reason is not None:
                            exit_reason = reason
                    elif variant.family == FAMILY_FULL_RECENTER and state.positions:
                        rows, recenter_count, diagnostics = _process_full_recenter(
                            market,
                            state=state,
                            start_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                        )
                        day_trades.extend(rows)
                    elif variant.family == FAMILY_TIME_RECENTER and state.positions:
                        rows, recenter_count, diagnostics = _process_time_recenter(
                            market,
                            state=state,
                            selection_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                        )
                        day_trades.extend(rows)
                    elif variant.family == FAMILY_RATCHET and state.positions:
                        rows, roll_count, diagnostics = _process_ratchet(
                            market,
                            state=state,
                            start_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                        )
                        day_trades.extend(rows)
                    elif variant.family in {FAMILY_GAMMA, FAMILY_DELTA_BAND}:
                        rows, hedge_count, diagnostics = _process_gamma(
                            market,
                            state=state,
                            selection_ns=selection_ns,
                            deadline_ns=deadline_ns,
                        )
                        day_trades.extend(rows)
                    elif variant.family == FAMILY_RANDOM and state.positions:
                        strategy = str(variant.parameters["source_strategy"])
                        threshold = int(variant.parameters["rolling_points"])
                        requested_rolls = (
                            state.random_pending_rolls
                            + candidate_counts[(strategy, threshold)]
                        )
                        rows, roll_count, diagnostics = _process_random(
                            market,
                            state=state,
                            selection_ns=selection_ns,
                            deadline_ns=deadline_ns,
                            option_tax_rate=option_tax_rate,
                            target_roll_count=requested_rolls,
                        )
                        state.random_pending_rolls = requested_rolls - roll_count
                        diagnostics["pending_matched_rolls_after_session"] = (
                            state.random_pending_rolls
                        )
                        day_trades.extend(rows)
                    if is_expiry:
                        if (
                            variant.family == FAMILY_RANDOM
                            and state.random_pending_rolls != 0
                        ):
                            raise ValueError(
                                f"{trading_date}: random control has "
                                f"{state.random_pending_rolls} unmatched cycle rolls"
                            )
                        settlement_payload = settlements[(state.expiry, state.series)]
                        day_trades.extend(
                            _settle_expiry(
                                market,
                                state=state,
                                settlement_payload=settlement_payload,
                                session_end_ns=session_end_ns,
                            )
                        )
                        completed_cycles[variant.variant_id] += 1
                        exit_reason = "official_expiry_cash_settlement"
                else:
                    is_expiry = False
                    exit_reason = "no_complete_next_expiry_in_sample"
            except Exception as exc:
                failures.append(
                    {
                        "trading_date": trading_date.isoformat(),
                        "variant_id": variant.variant_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            trade_rows.extend(day_trades)
            positions = dict(state.positions) if state is not None else {}
            fallback_marks = {
                right: market.last_option_trade_before(contract, session_end_ns)
                for right, contract in positions.items()
            }
            future_position = state.future_position if state is not None else 0
            future_product = state.future_product if state is not None else None
            future_mark = (
                market.last_future_trade_before(str(future_product), session_end_ns)
                if future_position
                else None
            )
            snapshots.append(
                {
                    "trading_date": trading_date,
                    "variant": variant,
                    "state": state,
                    "trades": day_trades,
                    "positions": positions,
                    "future_position": future_position,
                    "future_product": future_product,
                    "fallback_option_marks": fallback_marks,
                    "future_mark": future_mark,
                    "session_end_ns": session_end_ns,
                    "is_cycle_entry": is_entry,
                    "is_expiry_session": bool(state is not None and state.expiry == trading_date),
                    "roll_count": roll_count,
                    "recenter_count": recenter_count,
                    "hedge_count": hedge_count,
                    "exit_reason": exit_reason,
                    "diagnostics": diagnostics,
                }
            )
            if state is not None and state.expiry == trading_date:
                if state.positions or state.future_position:
                    raise ValueError(f"expiry state did not flatten: {variant.variant_id}")
                states[variant.variant_id] = None
    if failures:
        output_dir.mkdir(parents=True, exist_ok=True)
        _atomic_json(output_dir / "failures.json", failures)
        raise RuntimeError(f"{len(failures)} variant-days failed; first={failures[:5]}")
    open_states = [variant_id for variant_id, state in states.items() if state is not None]
    if open_states:
        raise ValueError(f"incomplete sample-end carry states: {open_states[:5]}")
    source_paths = _daily_source_paths(
        option_daily_root, trading_dates[0], trading_dates[-1]
    )
    daily_marks = _load_daily_marks(
        source_paths=source_paths,
        targets=_mark_targets(snapshots),
    )
    daily = pl.DataFrame(
        _build_daily_rows(snapshots=snapshots, daily_marks=daily_marks),
        infer_schema_length=None,
    ).sort(["benchmark_family", "variant_id", "trading_date"])
    trades = pl.DataFrame(trade_rows, infer_schema_length=None).sort(
        ["benchmark_family", "variant_id", "trading_date", "fill_ts", "product"]
    )
    _validate(
        daily=daily,
        trades=trades,
        variants=variants,
        trading_dates=trading_dates,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "daily_benchmarks.parquet"
    trades_path = output_dir / "trades.parquet"
    _atomic_parquet(daily, daily_path)
    _atomic_parquet(trades, trades_path)
    catalog = []
    for family, name, purpose in BENCHMARK_CATALOG:
        members = [variant.variant_id for variant in variants if variant.family == family]
        catalog.append(
            {
                "family": family,
                "name": name,
                "purpose": purpose,
                "variant_count": len(members),
                "variant_ids": members,
            }
        )
    summary: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": TAIFEX_TRADE_PROXY_SOURCE,
        "source_manifest": str(raw_root / "manifest.json"),
        "source_manifest_sha256": source_sha,
        "source_parser_contract_version": source_manifest["parser_contract_version"],
        "daily_mark_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE,
        "daily_mark_contract_version": TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
        "official_final_settlement_source": OFFICIAL_FINAL_SETTLEMENT_URL,
        "official_final_settlement_path": str(final_settlement_path),
        "official_final_settlement_sha256": _sha256_path(final_settlement_path),
        "date_start": trading_dates[0].isoformat(),
        "date_end": trading_dates[-1].isoformat(),
        "trading_days": len(trading_dates),
        "variant_count": len(variants),
        "benchmark_family_count": len(BENCHMARK_CATALOG),
        "daily_result_rows": daily.height,
        "trade_rows": trades.height,
        "completed_cycles_per_variant_min": min(completed_cycles.values()),
        "completed_cycles_per_variant_max": max(completed_cycles.values()),
        "fixed_fees_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE,
        "contract_multipliers_twd_per_point": {**FUTURES_MULTIPLIERS, OPTION_PRODUCT: OPTION_MULTIPLIER},
        "parameters": {
            "rolling_points": list(thresholds),
            "tp_percent": sorted(set(float(value) for value in tp_percent)),
            "sl_percent": sorted(set(float(value) for value in sl_percent)),
            "strangle_distance_points": sorted(set(int(value) for value in strangle_distances)),
            "time_recenter_minutes": sorted(set(int(value) for value in time_recenter_minutes)),
            "delta_bands": sorted(set(float(value) for value in delta_bands)),
            "selection_time": selection_time.isoformat(),
            "close_decision_time": close_decision_time.isoformat(),
            "session_end_time": session_end_time.isoformat(),
            "official_expiry_time": OFFICIAL_EXPIRY_TIME.isoformat(),
            "option_transaction_tax_rate": option_tax_rate,
            "contracts_per_option_leg": 1,
            "random_seed": random_seed,
        },
        "benchmark_catalog": catalog,
        "execution_contract": {
            "holding_policy": HOLDING_POLICY,
            "decision_clock": "completed TAIFEX whole-second event",
            "non_terminal_execution": "first strictly later product transaction print",
            "daily_mark": "official daily settlement, then official last trade fallback; never a trade",
            "expiry_execution": "official final settlement intrinsic; next series starts on a later session",
            "historical_bidask_available": False,
            "one_lot_fill_assumption": "guaranteed; no historical quantity or depth cap",
            "option_positions_daily_flattened": False,
            "futures_positions_daily_flattened": False,
            "tp_sl_reentry_same_expiry": False,
            "random_control": (
                "matches the candidate realized roll count over each completed expiry cycle; "
                "an unfilled random-session quota carries forward within that cycle"
            ),
        },
        "evaluation_stages": {
            "primary": "fixed fees plus statutory option and stock-index-futures transaction taxes",
            "pressure_tests": "not run",
        },
        "artifacts": {
            "daily_benchmarks": str(daily_path),
            "daily_benchmarks_sha256": _sha256_path(daily_path),
            "trades": str(trades_path),
            "trades_sha256": _sha256_path(trades_path),
        },
        "results": _aggregate(daily),
    }
    _atomic_json(output_dir / "summary.json", summary)
    print(
        json.dumps(
            {
                "status": "complete",
                "variants": len(variants),
                "days": len(trading_dates),
                "daily_rows": daily.height,
                "trade_rows": trades.height,
                "completed_cycles_per_variant": min(completed_cycles.values()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=Path("data_tw_index_derivatives_ticks"))
    parser.add_argument("--option-daily-root", type=Path, default=DEFAULT_OPTION_DAILY_ROOT)
    parser.add_argument("--final-settlement-path", type=Path, default=DEFAULT_FINAL_SETTLEMENT_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--rolling-points", nargs="+", type=int, default=list(range(50, 1001, 50)))
    parser.add_argument("--tp-percent", nargs="+", type=float, default=[25.0, 50.0, 100.0])
    parser.add_argument("--sl-percent", nargs="+", type=float, default=[25.0, 50.0])
    parser.add_argument("--strangle-distances", nargs="+", type=int, default=list(range(50, 1001, 50)))
    parser.add_argument("--time-recenter-minutes", nargs="+", type=int, default=[15, 30, 60])
    parser.add_argument("--delta-bands", nargs="+", type=float, default=[0.2, 0.3])
    parser.add_argument("--selection-time", default="08:50:00")
    parser.add_argument("--close-decision-time", default="13:40:00")
    parser.add_argument("--session-end-time", default="13:45:00")
    parser.add_argument("--option-tax-rate", type=float, default=TAIFEX_OPTION_PREMIUM_TAX_RATE)
    parser.add_argument("--random-seed", type=int, default=20260807)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_benchmarks(
        raw_root=args.raw_root,
        option_daily_root=args.option_daily_root,
        final_settlement_path=args.final_settlement_path,
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
