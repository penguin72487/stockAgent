#!/usr/bin/env python3
"""Causal one-package TAIFEX index-derivatives arbitrage research.

This scanner deliberately separates model-free expiry relationships from a
tradeable quote claim.  The historical TAIFEX transaction archive contains
whole-second trades, not bid/ask books.  A signal therefore uses only completed
seconds and every entry leg fills from the first strictly later transaction in
that contract.  An open package exits when its fixed relationship first returns
to the no-arbitrage interval; every exit leg again uses the first strictly later
transaction and pays another commission and transaction tax.  Official final
settlement is only the terminal fallback when convergence never occurs.

The implementation reuses the repository's receipt verification, day-market
parser, official settlement loader, statutory tax helpers, option daily marks,
contract multipliers, fixed fees, and official futures initial-margin schedule.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Final, Iterable, Mapping, Sequence

import numpy as np
import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import (  # noqa: E402
    DayMarket,
    Fill,
    OptionContract,
    _build_day_market,
    _datetime_from_ns,
    _ns,
    _parse_time,
    _sha256_path,
    _verify_manifest,
)
from scripts.backtest_taifex_opening_straddle_hold_to_expiry import (  # noqa: E402
    _load_official_final_settlements,
)
from scripts.backtest_taifex_option_benchmarks import (  # noqa: E402
    FIXED_FEES_PER_CONTRACT_SIDE,
    FUTURES_MULTIPLIERS,
    OPTION_MULTIPLIER,
    OPTION_PRODUCT,
)
from scripts.backtest_taifex_option_benchmarks_expiry_carry import (  # noqa: E402
    _daily_source_paths,
)
from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIFEX_TRADE_PROXY_SOURCE,
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
    taifex_front_month,
    taifex_option_expiry,
)
from stockagent.data.tw_index_options_daily import (  # noqa: E402
    load_taifex_option_daily_contract_rows,
)
from stockagent.research.taifex_capital_returns import (  # noqa: E402
    TAIFEX_INITIAL_MARGIN_TWD,
    TAIFEX_MARGIN_VERIFIED_THROUGH,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    TAIFEX_OPTION_PREMIUM_TAX_RATE,
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


OUTPUT_SCHEMA_VERSION: Final[int] = 3
DEFAULT_RAW_ROOT: Final[Path] = Path("data_tw_index_derivatives_ticks")
DEFAULT_OPTION_DAILY_ROOT: Final[Path] = Path("data_tw_index_options_daily/raw/ranges")
DEFAULT_SETTLEMENT_PATH: Final[Path] = Path(
    "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)
DEFAULT_OUTPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_index_arbitrage_all_expiries"
)
DEFAULT_SCAN_START: Final[time] = time(8, 50)
DEFAULT_ENTRY_DEADLINE: Final[time] = time(13, 20)
SESSION_END: Final[time] = time(13, 45)
EXPIRY_TIME: Final[time] = time(13, 30)
TRADING_DAYS_PER_YEAR: Final[float] = 252.0

# Equal capital prevents a TX-sized package from looking better only because it
# posts more gross margin.  This equals the conservative, no-offset margin for
# one long and one short TX-equivalent package in the verified sample window.
COMMON_CAPITAL_TWD: Final[float] = 2.0 * TAIFEX_INITIAL_MARGIN_TWD["TX"]

FUTURES_PACKAGE_SPECS: Final[dict[str, tuple[str, int, str, int]]] = {
    "futures_tx_vs_4_mtx": ("TX", 1, "MTX", 4),
    "futures_tx_vs_20_tmf": ("TX", 1, "TMF", 20),
    "futures_4_mtx_vs_20_tmf": ("MTX", 4, "TMF", 20),
}
OPTION_STRUCTURE_FAMILIES: Final[tuple[str, ...]] = (
    "call_vertical_bounds",
    "put_vertical_bounds",
    "call_butterfly_bounds",
    "put_butterfly_bounds",
    "box_spread",
)
CROSS_EXPIRY_VARIANT: Final[str] = "cross_expiry_calendar_box"
VARIANT_IDS: Final[tuple[str, ...]] = (
    *FUTURES_PACKAGE_SPECS.keys(),
    "monthly_put_call_parity_mtx",
    *OPTION_STRUCTURE_FAMILIES,
    CROSS_EXPIRY_VARIANT,
)


@dataclass(frozen=True, slots=True)
class LegIntent:
    instrument_type: str
    product: str
    quantity: int
    series: str | None = None
    strike: float | None = None
    option_right: str | None = None

    @property
    def option_contract(self) -> OptionContract:
        if self.instrument_type != "option":
            raise ValueError("futures intent has no option contract")
        if self.series is None or self.strike is None or self.option_right is None:
            raise ValueError("incomplete option intent")
        return OptionContract(self.series, self.strike, self.option_right)


@dataclass(frozen=True, slots=True)
class FilledLeg:
    intent: LegIntent
    fill: Fill
    fixed_fee_twd: float
    entry_tax_twd: float


@dataclass(frozen=True, slots=True)
class SignalCandidate:
    variant_id: str
    family: str
    decision_ns: int
    series: str
    expiry: date
    intents: tuple[LegIntent, ...]
    observed_edge_twd: float
    estimated_cost_twd: float
    relationship_value_points: float
    lower_bound_points: float
    upper_bound_points: float
    direction: str
    strikes: tuple[float, ...]

    @property
    def observed_net_edge_twd(self) -> float:
        return self.observed_edge_twd - self.estimated_cost_twd


@dataclass(frozen=True, slots=True)
class CycleState:
    cycle_id: str
    variant_id: str
    family: str
    series: str
    expiry: date
    entry_date: date
    decision_ns: int
    completion_ns: int
    legs: tuple[FilledLeg, ...]
    observed_edge_twd: float
    estimated_cost_twd: float
    relationship_value_points: float
    lower_bound_points: float
    upper_bound_points: float
    direction: str
    strikes: tuple[float, ...]

    @property
    def entry_fees_twd(self) -> float:
        return float(sum(leg.fixed_fee_twd for leg in self.legs))

    @property
    def entry_taxes_twd(self) -> float:
        return float(sum(leg.entry_tax_twd for leg in self.legs))

    @property
    def observed_net_edge_twd(self) -> float:
        return self.observed_edge_twd - self.estimated_cost_twd


@dataclass(frozen=True, slots=True)
class CycleExit:
    trading_date: date
    reason: str
    decision_ns: int
    completion_ns: int
    legs: tuple[FilledLeg, ...]
    settlement_price: float | None = None
    settlement_prices: Mapping[str, float] | None = None

    @property
    def exit_fees_twd(self) -> float:
        return float(sum(leg.fixed_fee_twd for leg in self.legs))

    @property
    def exit_taxes_twd(self) -> float:
        return float(sum(leg.entry_tax_twd for leg in self.legs))


@dataclass(frozen=True, slots=True)
class DailySnapshot:
    trading_date: date
    state: CycleState | None
    realized_cycle_ids: tuple[str, ...]
    option_fallback_marks: Mapping[tuple[str, float, str], Fill]
    futures_marks: Mapping[str, Fill]


@dataclass(frozen=True, slots=True)
class DayEntryScan:
    trading_date: date
    market: DayMarket
    selected: Mapping[str, CycleState | None]
    diagnostics: Mapping[str, Mapping[str, Any]]


def _family(variant_id: str) -> str:
    if variant_id.startswith("futures_"):
        return "equivalent_futures"
    if variant_id == "monthly_put_call_parity_mtx":
        return "put_call_parity"
    if "vertical" in variant_id:
        return "vertical_bounds"
    if "butterfly" in variant_id:
        return "butterfly_bounds"
    if variant_id == "box_spread":
        return "box_spread"
    if variant_id == CROSS_EXPIRY_VARIANT:
        return "calendar_box_spread"
    raise ValueError(f"unknown variant: {variant_id}")


def _option_entry_cost(
    intents: Sequence[LegIntent], prices: Mapping[OptionContract, float]
) -> tuple[float, float]:
    fees = 0.0
    taxes = 0.0
    for intent in intents:
        quantity = abs(intent.quantity)
        contract = intent.option_contract
        price = float(prices[contract])
        fees += quantity * FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT]
        taxes += quantity * option_premium_transaction_tax_twd(
            price,
            multiplier_twd_per_point=OPTION_MULTIPLIER,
            tax_rate=TAIFEX_OPTION_PREMIUM_TAX_RATE,
        )
    return fees, taxes


def _future_entry_cost(
    *, trading_date: date, product: str, quantity: int, price: float
) -> tuple[float, float]:
    normalized = product.upper()
    absolute = abs(int(quantity))
    multiplier = FUTURES_MULTIPLIERS[normalized]
    fee = absolute * FIXED_FEES_PER_CONTRACT_SIDE[normalized]
    tax = absolute * taifex_tax_per_contract_twd(
        price,
        multiplier_twd_per_point=multiplier,
        tax_rate=stock_index_futures_tax_rate(trading_date),
    )
    return fee, tax


def _estimated_settlement_cost(
    intents: Sequence[LegIntent], *, expiry: date, settlement_proxy: float
) -> float:
    total = 0.0
    option_tax = option_cash_settlement_transaction_tax_twd(
        settlement_proxy,
        settlement_date=expiry,
        multiplier_twd_per_point=OPTION_MULTIPLIER,
    )
    for intent in intents:
        quantity = abs(intent.quantity)
        if intent.instrument_type == "option":
            # A conservative trigger reserve: assume every option quantity is
            # exercised.  Exact settlement later taxes only positive intrinsic.
            total += quantity * option_tax
        else:
            total += quantity * taifex_tax_per_contract_twd(
                settlement_proxy,
                multiplier_twd_per_point=FUTURES_MULTIPLIERS[intent.product],
                tax_rate=stock_index_futures_tax_rate(expiry),
            )
    return total


def _estimated_candidate_cost(
    *,
    market: DayMarket,
    intents: Sequence[LegIntent],
    latest_options: Mapping[OptionContract, float],
    latest_futures: Mapping[str, float],
    expiry: date,
    decision_ns: int,
) -> float:
    """Estimate both entry and convergence-exit trading costs at signal prices.

    The estimate is deliberately round-trip rather than expiry based.  The
    realized ledger later replaces the second side with actual convergence
    fills, or with official settlement tax if the relationship never converges.
    """

    fees = taxes = 0.0
    for intent in intents:
        if intent.instrument_type == "option":
            price = latest_options[intent.option_contract]
            fee, tax = _option_entry_cost((intent,), {intent.option_contract: price})
        else:
            price = latest_futures[intent.product]
            fee, tax = _future_entry_cost(
                trading_date=market.trading_date,
                product=intent.product,
                quantity=intent.quantity,
                price=price,
            )
        fees += fee
        taxes += tax
    del market, expiry, decision_ns
    return 2.0 * (fees + taxes)


def _structure_candidate(
    *,
    variant_id: str,
    decision_ns: int,
    series: str,
    expiry: date,
    latest: Mapping[OptionContract, float],
    market: DayMarket,
) -> SignalCandidate | None:
    right = "C" if variant_id.startswith("call_") else "P"
    by_right: dict[str, dict[float, float]] = {"C": {}, "P": {}}
    for contract, price in latest.items():
        if contract.series == series and math.isfinite(price) and price >= 0.0:
            by_right[contract.right][contract.strike] = float(price)

    best: (
        tuple[
            float,
            float,
            tuple[LegIntent, ...],
            float,
            float,
            str,
            tuple[float, ...],
        ]
        | None
    ) = None

    def consider(
        *,
        edge_points: float,
        relationship: float,
        intents: tuple[LegIntent, ...],
        lower: float,
        upper: float,
        direction: str,
        strikes: tuple[float, ...],
    ) -> None:
        nonlocal best
        if edge_points <= 0.0:
            return
        edge_twd = edge_points * OPTION_MULTIPLIER
        payload = (
            edge_twd,
            edge_points,
            intents,
            relationship,
            upper,
            direction,
            strikes,
        )
        if best is None or payload[0] > best[0]:
            best = payload

    if "vertical" in variant_id:
        prices = by_right[right]
        strikes = sorted(prices)
        for low, high in zip(strikes, strikes[1:]):
            width = high - low
            if width <= 0.0:
                continue
            if right == "C":
                relationship = prices[low] - prices[high]
                long_intents = (
                    LegIntent("option", OPTION_PRODUCT, +1, series, low, "C"),
                    LegIntent("option", OPTION_PRODUCT, -1, series, high, "C"),
                )
            else:
                relationship = prices[high] - prices[low]
                long_intents = (
                    LegIntent("option", OPTION_PRODUCT, +1, series, high, "P"),
                    LegIntent("option", OPTION_PRODUCT, -1, series, low, "P"),
                )
            if relationship < 0.0:
                consider(
                    edge_points=-relationship,
                    relationship=relationship,
                    intents=long_intents,
                    lower=0.0,
                    upper=width,
                    direction="buy_underpriced_vertical",
                    strikes=(low, high),
                )
            elif relationship > width:
                consider(
                    edge_points=relationship - width,
                    relationship=relationship,
                    intents=tuple(
                        replace(intent, quantity=-intent.quantity)
                        for intent in long_intents
                    ),
                    lower=0.0,
                    upper=width,
                    direction="sell_overpriced_vertical",
                    strikes=(low, high),
                )
    elif "butterfly" in variant_id:
        prices = by_right[right]
        strikes = sorted(prices)
        for low, middle, high in zip(strikes, strikes[1:], strikes[2:]):
            left = middle - low
            right_width = high - middle
            if left <= 0.0 or not math.isclose(left, right_width, abs_tol=1e-9):
                continue
            relationship = prices[low] - 2.0 * prices[middle] + prices[high]
            long_intents = (
                LegIntent("option", OPTION_PRODUCT, +1, series, low, right),
                LegIntent("option", OPTION_PRODUCT, -2, series, middle, right),
                LegIntent("option", OPTION_PRODUCT, +1, series, high, right),
            )
            if relationship < 0.0:
                consider(
                    edge_points=-relationship,
                    relationship=relationship,
                    intents=long_intents,
                    lower=0.0,
                    upper=left,
                    direction="buy_negative_price_butterfly",
                    strikes=(low, middle, high),
                )
            elif relationship > left:
                consider(
                    edge_points=relationship - left,
                    relationship=relationship,
                    intents=tuple(
                        replace(intent, quantity=-intent.quantity)
                        for intent in long_intents
                    ),
                    lower=0.0,
                    upper=left,
                    direction="sell_above_max_payoff_butterfly",
                    strikes=(low, middle, high),
                )
    elif variant_id == "box_spread":
        strikes = sorted(set(by_right["C"]) & set(by_right["P"]))
        for low, high in zip(strikes, strikes[1:]):
            width = high - low
            if width <= 0.0:
                continue
            relationship = (
                by_right["C"][low]
                - by_right["C"][high]
                + by_right["P"][high]
                - by_right["P"][low]
            )
            long_intents = (
                LegIntent("option", OPTION_PRODUCT, +1, series, low, "C"),
                LegIntent("option", OPTION_PRODUCT, -1, series, high, "C"),
                LegIntent("option", OPTION_PRODUCT, +1, series, high, "P"),
                LegIntent("option", OPTION_PRODUCT, -1, series, low, "P"),
            )
            deviation = relationship - width
            if deviation < 0.0:
                consider(
                    edge_points=-deviation,
                    relationship=relationship,
                    intents=long_intents,
                    lower=width,
                    upper=width,
                    direction="buy_underpriced_box",
                    strikes=(low, high),
                )
            elif deviation > 0.0:
                consider(
                    edge_points=deviation,
                    relationship=relationship,
                    intents=tuple(
                        replace(intent, quantity=-intent.quantity)
                        for intent in long_intents
                    ),
                    lower=width,
                    upper=width,
                    direction="sell_overpriced_box",
                    strikes=(low, high),
                )
    else:
        raise ValueError(f"unsupported option structure: {variant_id}")

    if best is None or best[0] <= 0.0:
        return None
    edge_twd, _edge_points, intents, relationship, _bound, direction, strikes = best
    estimated = _estimated_candidate_cost(
        market=market,
        intents=intents,
        latest_options=latest,
        latest_futures={},
        expiry=expiry,
        decision_ns=decision_ns,
    )
    if edge_twd <= estimated:
        return None
    if "vertical" in variant_id:
        upper = strikes[-1] - strikes[0]
        lower = 0.0
    elif "butterfly" in variant_id:
        upper = strikes[1] - strikes[0]
        lower = 0.0
    else:
        upper = lower = strikes[-1] - strikes[0]
    return SignalCandidate(
        variant_id=variant_id,
        family=_family(variant_id),
        decision_ns=decision_ns,
        series=series,
        expiry=expiry,
        intents=intents,
        observed_edge_twd=edge_twd,
        estimated_cost_twd=estimated,
        relationship_value_points=relationship,
        lower_bound_points=lower,
        upper_bound_points=upper,
        direction=direction,
        strikes=strikes,
    )


def _parity_candidate(
    *,
    decision_ns: int,
    series: str,
    expiry: date,
    latest: Mapping[OptionContract, float],
    market: DayMarket,
) -> SignalCandidate | None:
    future = market.future_at_or_before("MTX", decision_ns)
    if future is None:
        return None
    calls = {
        contract.strike: price
        for contract, price in latest.items()
        if contract.series == series and contract.right == "C"
    }
    puts = {
        contract.strike: price
        for contract, price in latest.items()
        if contract.series == series and contract.right == "P"
    }
    common = set(calls) & set(puts)
    if not common:
        return None
    strike, parity_gap = max(
        (
            (strike, calls[strike] - puts[strike] - (future.price - strike))
            for strike in common
        ),
        key=lambda item: abs(item[1]),
    )
    if parity_gap == 0.0:
        return None
    direction_sign = 1 if parity_gap > 0.0 else -1
    # Positive gap: short Call, long Put, long MTX.  Negative gap reverses.
    intents = (
        LegIntent("option", OPTION_PRODUCT, -direction_sign, series, strike, "C"),
        LegIntent("option", OPTION_PRODUCT, +direction_sign, series, strike, "P"),
        LegIntent("future", "MTX", +direction_sign, series),
    )
    estimated = _estimated_candidate_cost(
        market=market,
        intents=intents,
        latest_options=latest,
        latest_futures={"MTX": future.price},
        expiry=expiry,
        decision_ns=decision_ns,
    )
    candidate = SignalCandidate(
        variant_id="monthly_put_call_parity_mtx",
        family="put_call_parity",
        decision_ns=decision_ns,
        series=series,
        expiry=expiry,
        intents=intents,
        observed_edge_twd=abs(parity_gap) * OPTION_MULTIPLIER,
        estimated_cost_twd=estimated,
        relationship_value_points=parity_gap,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction=(
            "short_call_long_put_long_mtx"
            if direction_sign > 0
            else "long_call_short_put_short_mtx"
        ),
        strikes=(strike,),
    )
    return candidate if candidate.observed_net_edge_twd > 0.0 else None


def _all_structure_candidates(
    *,
    decision_ns: int,
    series: str,
    expiry: date,
    latest: Mapping[OptionContract, float],
    market: DayMarket,
) -> dict[str, SignalCandidate]:
    """Vectorize the five cross-strike bounds over one option-chain snapshot."""

    by_right: dict[str, dict[float, float]] = {"C": {}, "P": {}}
    for contract, price in latest.items():
        if contract.series == series and math.isfinite(price) and price >= 0.0:
            by_right[contract.right][contract.strike] = float(price)
    output: dict[str, SignalCandidate] = {}

    def emit(
        variant_id: str,
        *,
        relationship: float,
        lower: float,
        upper: float,
        intents: tuple[LegIntent, ...],
        direction: str,
        strikes: tuple[float, ...],
    ) -> None:
        edge_points = (
            lower - relationship
            if relationship < lower
            else (relationship - upper if relationship > upper else 0.0)
        )
        if edge_points <= 0.0:
            return
        edge_twd = edge_points * OPTION_MULTIPLIER
        estimated = _estimated_candidate_cost(
            market=market,
            intents=intents,
            latest_options=latest,
            latest_futures={},
            expiry=expiry,
            decision_ns=decision_ns,
        )
        if edge_twd <= estimated:
            return
        output[variant_id] = SignalCandidate(
            variant_id=variant_id,
            family=_family(variant_id),
            decision_ns=decision_ns,
            series=series,
            expiry=expiry,
            intents=intents,
            observed_edge_twd=edge_twd,
            estimated_cost_twd=estimated,
            relationship_value_points=relationship,
            lower_bound_points=lower,
            upper_bound_points=upper,
            direction=direction,
            strikes=strikes,
        )

    for option_right, variant_id in (
        ("C", "call_vertical_bounds"),
        ("P", "put_vertical_bounds"),
    ):
        mapping = by_right[option_right]
        strikes = np.asarray(sorted(mapping), dtype=np.float64)
        if strikes.size >= 2:
            prices = np.asarray([mapping[float(k)] for k in strikes])
            widths = np.diff(strikes)
            relationship = (
                prices[:-1] - prices[1:]
                if option_right == "C"
                else prices[1:] - prices[:-1]
            )
            violations = np.maximum(-relationship, relationship - widths)
            index = int(np.argmax(violations))
            if violations[index] > 0.0:
                low = float(strikes[index])
                high = float(strikes[index + 1])
                value = float(relationship[index])
                width = float(widths[index])
                if option_right == "C":
                    long_intents = (
                        LegIntent("option", OPTION_PRODUCT, +1, series, low, "C"),
                        LegIntent("option", OPTION_PRODUCT, -1, series, high, "C"),
                    )
                else:
                    long_intents = (
                        LegIntent("option", OPTION_PRODUCT, +1, series, high, "P"),
                        LegIntent("option", OPTION_PRODUCT, -1, series, low, "P"),
                    )
                lower_violation = value < 0.0
                intents = (
                    long_intents
                    if lower_violation
                    else tuple(
                        replace(intent, quantity=-intent.quantity)
                        for intent in long_intents
                    )
                )
                emit(
                    variant_id,
                    relationship=value,
                    lower=0.0,
                    upper=width,
                    intents=intents,
                    direction=(
                        "buy_underpriced_vertical"
                        if lower_violation
                        else "sell_overpriced_vertical"
                    ),
                    strikes=(low, high),
                )

    for option_right, variant_id in (
        ("C", "call_butterfly_bounds"),
        ("P", "put_butterfly_bounds"),
    ):
        mapping = by_right[option_right]
        strikes = np.asarray(sorted(mapping), dtype=np.float64)
        if strikes.size >= 3:
            prices = np.asarray([mapping[float(k)] for k in strikes])
            left = strikes[1:-1] - strikes[:-2]
            right = strikes[2:] - strikes[1:-1]
            equal = np.isclose(left, right, atol=1e-9, rtol=0.0) & (left > 0.0)
            relationship = prices[:-2] - 2.0 * prices[1:-1] + prices[2:]
            violations = np.where(
                equal,
                np.maximum(-relationship, relationship - left),
                -math.inf,
            )
            index = int(np.argmax(violations))
            if violations[index] > 0.0:
                low = float(strikes[index])
                middle = float(strikes[index + 1])
                high = float(strikes[index + 2])
                value = float(relationship[index])
                width = float(left[index])
                long_intents = (
                    LegIntent("option", OPTION_PRODUCT, +1, series, low, option_right),
                    LegIntent(
                        "option", OPTION_PRODUCT, -2, series, middle, option_right
                    ),
                    LegIntent("option", OPTION_PRODUCT, +1, series, high, option_right),
                )
                lower_violation = value < 0.0
                intents = (
                    long_intents
                    if lower_violation
                    else tuple(
                        replace(intent, quantity=-intent.quantity)
                        for intent in long_intents
                    )
                )
                emit(
                    variant_id,
                    relationship=value,
                    lower=0.0,
                    upper=width,
                    intents=intents,
                    direction=(
                        "buy_negative_price_butterfly"
                        if lower_violation
                        else "sell_above_max_payoff_butterfly"
                    ),
                    strikes=(low, middle, high),
                )

    common = np.asarray(
        sorted(set(by_right["C"]) & set(by_right["P"])), dtype=np.float64
    )
    if common.size >= 2:
        calls = np.asarray([by_right["C"][float(k)] for k in common])
        puts = np.asarray([by_right["P"][float(k)] for k in common])
        widths = np.diff(common)
        relationship = calls[:-1] - calls[1:] + puts[1:] - puts[:-1]
        deviation = relationship - widths
        index = int(np.argmax(np.abs(deviation)))
        if deviation[index] != 0.0:
            low = float(common[index])
            high = float(common[index + 1])
            width = float(widths[index])
            value = float(relationship[index])
            long_intents = (
                LegIntent("option", OPTION_PRODUCT, +1, series, low, "C"),
                LegIntent("option", OPTION_PRODUCT, -1, series, high, "C"),
                LegIntent("option", OPTION_PRODUCT, +1, series, high, "P"),
                LegIntent("option", OPTION_PRODUCT, -1, series, low, "P"),
            )
            lower_violation = deviation[index] < 0.0
            intents = (
                long_intents
                if lower_violation
                else tuple(
                    replace(intent, quantity=-intent.quantity)
                    for intent in long_intents
                )
            )
            emit(
                "box_spread",
                relationship=value,
                lower=width,
                upper=width,
                intents=intents,
                direction=(
                    "buy_underpriced_box" if lower_violation else "sell_overpriced_box"
                ),
                strikes=(low, high),
            )
    return output


def _future_candidate(
    *,
    variant_id: str,
    decision_ns: int,
    series: str,
    expiry: date,
    market: DayMarket,
) -> SignalCandidate | None:
    product_a, count_a, product_b, count_b = FUTURES_PACKAGE_SPECS[variant_id]
    fill_a = market.future_at_or_before(product_a, decision_ns)
    fill_b = market.future_at_or_before(product_b, decision_ns)
    if fill_a is None or fill_b is None:
        return None
    value_a = count_a * fill_a.price * FUTURES_MULTIPLIERS[product_a]
    value_b = count_b * fill_b.price * FUTURES_MULTIPLIERS[product_b]
    spread_twd = value_a - value_b
    if spread_twd == 0.0:
        return None
    sign = 1 if spread_twd > 0.0 else -1
    intents = (
        LegIntent("future", product_a, -sign * count_a, series),
        LegIntent("future", product_b, +sign * count_b, series),
    )
    estimated = _estimated_candidate_cost(
        market=market,
        intents=intents,
        latest_options={},
        latest_futures={product_a: fill_a.price, product_b: fill_b.price},
        expiry=expiry,
        decision_ns=decision_ns,
    )
    candidate = SignalCandidate(
        variant_id=variant_id,
        family="equivalent_futures",
        decision_ns=decision_ns,
        series=series,
        expiry=expiry,
        intents=intents,
        observed_edge_twd=abs(spread_twd),
        estimated_cost_twd=estimated,
        relationship_value_points=spread_twd / 200.0,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction=(
            f"short_{product_a}_long_{product_b}"
            if sign > 0
            else f"long_{product_a}_short_{product_b}"
        ),
        strikes=(),
    )
    return candidate if candidate.observed_net_edge_twd > 0.0 else None


def _fill_candidate(
    *, market: DayMarket, candidate: SignalCandidate, deadline_ns: int
) -> CycleState | None:
    legs: list[FilledLeg] = []
    for intent in candidate.intents:
        if intent.instrument_type == "option":
            fill = market.first_option_trade_after(
                intent.option_contract,
                candidate.decision_ns,
                before_ns=deadline_ns,
            )
            if fill is None:
                return None
            fee = abs(intent.quantity) * FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT]
            tax = abs(intent.quantity) * option_premium_transaction_tax_twd(
                fill.price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
                tax_rate=TAIFEX_OPTION_PREMIUM_TAX_RATE,
            )
        else:
            fill = market.first_future_trade_after(
                intent.product,
                candidate.decision_ns,
                before_ns=deadline_ns,
            )
            if fill is None:
                return None
            fee, tax = _future_entry_cost(
                trading_date=market.trading_date,
                product=intent.product,
                quantity=intent.quantity,
                price=fill.price,
            )
        legs.append(FilledLeg(intent, fill, fee, tax))
    completion_ns = max(leg.fill.event_ns for leg in legs)
    cycle_id = (
        f"{candidate.variant_id}__{candidate.series}__"
        f"{market.trading_date.isoformat()}__{candidate.decision_ns}"
    )
    return CycleState(
        cycle_id=cycle_id,
        variant_id=candidate.variant_id,
        family=candidate.family,
        series=candidate.series,
        expiry=candidate.expiry,
        entry_date=market.trading_date,
        decision_ns=candidate.decision_ns,
        completion_ns=completion_ns,
        legs=tuple(legs),
        observed_edge_twd=candidate.observed_edge_twd,
        estimated_cost_twd=candidate.estimated_cost_twd,
        relationship_value_points=candidate.relationship_value_points,
        lower_bound_points=candidate.lower_bound_points,
        upper_bound_points=candidate.upper_bound_points,
        direction=candidate.direction,
        strikes=candidate.strikes,
    )


def _leg_price_key(intent: LegIntent) -> tuple[object, ...]:
    if intent.instrument_type == "option":
        contract = intent.option_contract
        return ("option", contract.series, contract.strike, contract.right)
    return ("future", intent.product)


def _relationship_value_from_prices(
    state: CycleState, prices: Mapping[tuple[object, ...], float]
) -> float:
    def option_price(strike: float, right: str, *, series: str | None = None) -> float:
        return float(prices[("option", series or state.series, strike, right)])

    if state.variant_id in FUTURES_PACKAGE_SPECS:
        product_a, count_a, product_b, count_b = FUTURES_PACKAGE_SPECS[state.variant_id]
        spread_twd = (
            count_a
            * float(prices[("future", product_a)])
            * FUTURES_MULTIPLIERS[product_a]
            - count_b
            * float(prices[("future", product_b)])
            * FUTURES_MULTIPLIERS[product_b]
        )
        return spread_twd / 200.0
    if state.variant_id == "monthly_put_call_parity_mtx":
        strike = state.strikes[0]
        return (
            option_price(strike, "C")
            - option_price(strike, "P")
            - (float(prices[("future", "MTX")]) - strike)
        )
    if "vertical" in state.variant_id:
        low, high = state.strikes
        right = "C" if state.variant_id.startswith("call_") else "P"
        return (
            option_price(low, right) - option_price(high, right)
            if right == "C"
            else option_price(high, right) - option_price(low, right)
        )
    if "butterfly" in state.variant_id:
        low, middle, high = state.strikes
        right = "C" if state.variant_id.startswith("call_") else "P"
        return (
            option_price(low, right)
            - 2.0 * option_price(middle, right)
            + option_price(high, right)
        )
    if state.variant_id == "box_spread":
        low, high = state.strikes
        return (
            option_price(low, "C")
            - option_price(high, "C")
            + option_price(high, "P")
            - option_price(low, "P")
        )
    if state.variant_id == CROSS_EXPIRY_VARIANT:
        near_series, far_series = state.series.split("|", maxsplit=1)
        low, high = state.strikes

        def box_value(series: str) -> float:
            return (
                option_price(low, "C", series=series)
                - option_price(high, "C", series=series)
                + option_price(high, "P", series=series)
                - option_price(low, "P", series=series)
            )

        return box_value(near_series) - box_value(far_series)
    raise ValueError(f"unsupported convergence relationship: {state.variant_id}")


def _relationship_has_converged(state: CycleState, current_value: float) -> bool:
    if state.relationship_value_points < state.lower_bound_points:
        return current_value >= state.lower_bound_points
    if state.relationship_value_points > state.upper_bound_points:
        return current_value <= state.upper_bound_points
    raise ValueError(f"entry relationship was not outside bounds: {state.cycle_id}")


def _fill_convergence_exit(
    *,
    market: DayMarket,
    state: CycleState,
    decision_ns: int,
    deadline_ns: int,
) -> CycleExit | None:
    legs: list[FilledLeg] = []
    for entry_leg in state.legs:
        close_intent = replace(entry_leg.intent, quantity=-entry_leg.intent.quantity)
        if close_intent.instrument_type == "option":
            fill = market.first_option_trade_after(
                close_intent.option_contract,
                decision_ns,
                before_ns=deadline_ns,
            )
            if fill is None:
                return None
            fee = (
                abs(close_intent.quantity)
                * FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT]
            )
            tax = abs(close_intent.quantity) * option_premium_transaction_tax_twd(
                fill.price,
                multiplier_twd_per_point=OPTION_MULTIPLIER,
                tax_rate=TAIFEX_OPTION_PREMIUM_TAX_RATE,
            )
        else:
            fill = market.first_future_trade_after(
                close_intent.product,
                decision_ns,
                before_ns=deadline_ns,
            )
            if fill is None:
                return None
            fee, tax = _future_entry_cost(
                trading_date=market.trading_date,
                product=close_intent.product,
                quantity=close_intent.quantity,
                price=fill.price,
            )
        legs.append(FilledLeg(close_intent, fill, fee, tax))
    return CycleExit(
        trading_date=market.trading_date,
        reason="relationship_convergence",
        decision_ns=decision_ns,
        completion_ns=max(leg.fill.event_ns for leg in legs),
        legs=tuple(legs),
    )


def _try_convergence_exit(
    *,
    market: DayMarket,
    state: CycleState,
    start_ns: int,
    deadline_ns: int,
) -> tuple[CycleExit | None, int]:
    """Exit at the first completed second that returns inside the bound.

    A carried position must observe every fixed leg at least once during the
    current day before testing convergence.  A same-day entry seeds each leg
    with its latest transaction at or before the final entry fill.
    """

    required = {_leg_price_key(leg.intent) for leg in state.legs}
    latest: dict[tuple[object, ...], float] = {}
    same_day_entry = state.entry_date == market.trading_date
    scan_after_ns = max(start_ns, state.completion_ns) if same_day_entry else start_ns
    if same_day_entry:
        for leg in state.legs:
            intent = leg.intent
            if intent.instrument_type == "option":
                fill = market.last_option_trade_at_or_before(
                    intent.option_contract, scan_after_ns
                )
            else:
                fill = market.future_at_or_before(intent.product, scan_after_ns)
            if fill is None:
                return None, 0
            latest[_leg_price_key(intent)] = fill.price

    failed_fill_attempts = 0

    def try_exit(decision_ns: int) -> CycleExit | None:
        nonlocal failed_fill_attempts
        if set(latest) != required:
            return None
        relationship = _relationship_value_from_prices(state, latest)
        if not _relationship_has_converged(state, relationship):
            return None
        result = _fill_convergence_exit(
            market=market,
            state=state,
            decision_ns=decision_ns,
            deadline_ns=deadline_ns,
        )
        if result is None:
            failed_fill_attempts += 1
        return result

    if same_day_entry:
        immediate = try_exit(scan_after_ns)
        if immediate is not None:
            return immediate, failed_fill_attempts

    rows: list[tuple[int, tuple[object, ...], float]] = []
    for leg in state.legs:
        intent = leg.intent
        key = _leg_price_key(intent)
        if intent.instrument_type == "option":
            payload = market.option_events.get(intent.option_contract)
        else:
            payload = market._future_payload(intent.product)
        if payload is None:
            continue
        times, values = payload
        side = "right" if same_day_entry else "left"
        begin = int(np.searchsorted(times, scan_after_ns, side=side))
        stop = int(np.searchsorted(times, deadline_ns, side="left"))
        rows.extend(
            (int(event_ns), key, float(value))
            for event_ns, value in zip(times[begin:stop], values[begin:stop])
        )
    rows.sort(key=lambda item: (item[0], item[1]))
    index = 0
    while index < len(rows):
        decision_ns = rows[index][0]
        while index < len(rows) and rows[index][0] == decision_ns:
            _event_ns, key, value = rows[index]
            latest[key] = value
            index += 1
        result = try_exit(decision_ns)
        if result is not None:
            return result, failed_fill_attempts
    return None, failed_fill_attempts


def _series_events(
    market: DayMarket, *, series: str, start_ns: int, end_ns: int
) -> list[tuple[int, OptionContract, float]]:
    rows: list[tuple[int, OptionContract, float]] = []
    for contract, (times, prices) in market.option_events.items():
        if contract.series != series:
            continue
        start = int(np.searchsorted(times, start_ns, side="left"))
        stop = int(np.searchsorted(times, end_ns, side="left"))
        rows.extend(
            (int(event_ns), contract, float(price))
            for event_ns, price in zip(times[start:stop], prices[start:stop])
        )
    rows.sort(key=lambda row: (row[0], row[1].strike, row[1].right))
    return rows


def _scan_option_day(
    *,
    market: DayMarket,
    variant_id: str,
    series: str,
    expiry: date,
    start_ns: int,
    deadline_ns: int,
    execute: bool,
) -> tuple[CycleState | None, dict[str, Any]]:
    latest: dict[OptionContract, float] = {}
    rows = _series_events(market, series=series, start_ns=start_ns, end_ns=deadline_ns)
    signal_seconds = 0
    max_net_edge = -math.inf
    max_gross_edge = 0.0
    fill_failures = 0
    selected: CycleState | None = None
    index = 0
    while index < len(rows):
        decision_ns = rows[index][0]
        while index < len(rows) and rows[index][0] == decision_ns:
            _event_ns, contract, price = rows[index]
            latest[contract] = price
            index += 1
        candidate = (
            _parity_candidate(
                decision_ns=decision_ns,
                series=series,
                expiry=expiry,
                latest=latest,
                market=market,
            )
            if variant_id == "monthly_put_call_parity_mtx"
            else _structure_candidate(
                variant_id=variant_id,
                decision_ns=decision_ns,
                series=series,
                expiry=expiry,
                latest=latest,
                market=market,
            )
        )
        if candidate is None:
            continue
        signal_seconds += 1
        max_net_edge = max(max_net_edge, candidate.observed_net_edge_twd)
        max_gross_edge = max(max_gross_edge, candidate.observed_edge_twd)
        if execute and selected is None:
            selected = _fill_candidate(
                market=market, candidate=candidate, deadline_ns=deadline_ns
            )
            if selected is None:
                fill_failures += 1
    return selected, {
        "signal_seconds": signal_seconds,
        "max_observed_gross_edge_twd": max_gross_edge,
        "max_observed_net_edge_twd": (
            None if max_net_edge == -math.inf else max_net_edge
        ),
        "unfillable_signal_attempts": fill_failures,
        "series": series,
        "series_count": 1,
        "series_pair_count": 0,
    }


def _scan_option_structures_day(
    *,
    market: DayMarket,
    series: str,
    expiry: date,
    start_ns: int,
    deadline_ns: int,
    execute_by_variant: Mapping[str, bool],
) -> tuple[dict[str, CycleState | None], dict[str, dict[str, Any]]]:
    """Scan all five cross-strike structures in one chronological chain pass."""

    latest: dict[OptionContract, float] = {}
    rows = _series_events(market, series=series, start_ns=start_ns, end_ns=deadline_ns)
    selected: dict[str, CycleState | None] = {
        variant_id: None for variant_id in OPTION_STRUCTURE_FAMILIES
    }
    diagnostics: dict[str, dict[str, Any]] = {
        variant_id: {
            "signal_seconds": 0,
            "max_observed_gross_edge_twd": 0.0,
            "max_observed_net_edge_twd": None,
            "unfillable_signal_attempts": 0,
            "series": series,
        }
        for variant_id in OPTION_STRUCTURE_FAMILIES
    }
    index = 0
    while index < len(rows):
        decision_ns = rows[index][0]
        while index < len(rows) and rows[index][0] == decision_ns:
            _event_ns, contract, price = rows[index]
            latest[contract] = price
            index += 1
        candidates = _all_structure_candidates(
            decision_ns=decision_ns,
            series=series,
            expiry=expiry,
            latest=latest,
            market=market,
        )
        for variant_id, candidate in candidates.items():
            row = diagnostics[variant_id]
            row["signal_seconds"] += 1
            row["max_observed_gross_edge_twd"] = max(
                float(row["max_observed_gross_edge_twd"]),
                candidate.observed_edge_twd,
            )
            previous = row["max_observed_net_edge_twd"]
            row["max_observed_net_edge_twd"] = (
                candidate.observed_net_edge_twd
                if previous is None
                else max(float(previous), candidate.observed_net_edge_twd)
            )
            if (
                execute_by_variant.get(variant_id, False)
                and selected[variant_id] is None
            ):
                filled = _fill_candidate(
                    market=market, candidate=candidate, deadline_ns=deadline_ns
                )
                if filled is None:
                    row["unfillable_signal_attempts"] += 1
                else:
                    selected[variant_id] = filled
    return selected, diagnostics


def _aggregate_structure_series_scans(
    scans: Sequence[
        tuple[
            Mapping[str, CycleState | None],
            Mapping[str, Mapping[str, Any]],
        ]
    ],
    *,
    series_count: int,
) -> tuple[dict[str, CycleState | None], dict[str, dict[str, Any]]]:
    selected: dict[str, CycleState | None] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    for variant_id in OPTION_STRUCTURE_FAMILIES:
        candidates = [
            states[variant_id]
            for states, _rows in scans
            if states.get(variant_id) is not None
        ]
        selected[variant_id] = (
            min(
                candidates,
                key=lambda state: (
                    state.decision_ns,
                    -state.observed_net_edge_twd,
                    state.completion_ns,
                    state.series,
                ),
            )
            if candidates
            else None
        )
        rows = [row_map[variant_id] for _states, row_map in scans]
        net_edges = [
            float(row["max_observed_net_edge_twd"])
            for row in rows
            if row["max_observed_net_edge_twd"] is not None
        ]
        diagnostics[variant_id] = {
            "signal_seconds": sum(int(row["signal_seconds"]) for row in rows),
            "max_observed_gross_edge_twd": max(
                (float(row["max_observed_gross_edge_twd"]) for row in rows),
                default=0.0,
            ),
            "max_observed_net_edge_twd": max(net_edges) if net_edges else None,
            "unfillable_signal_attempts": sum(
                int(row["unfillable_signal_attempts"]) for row in rows
            ),
            "series": "all_complete_series",
            "series_count": series_count,
            "series_pair_count": 0,
        }
    return selected, diagnostics


def _long_box_intents(series: str, low: float, high: float) -> tuple[LegIntent, ...]:
    return (
        LegIntent("option", OPTION_PRODUCT, +1, series, low, "C"),
        LegIntent("option", OPTION_PRODUCT, -1, series, high, "C"),
        LegIntent("option", OPTION_PRODUCT, +1, series, high, "P"),
        LegIntent("option", OPTION_PRODUCT, -1, series, low, "P"),
    )


def _calendar_box_pair_candidate(
    *,
    decision_ns: int,
    near_series: str,
    near_expiry: date,
    far_series: str,
    far_expiry: date,
    latest: Mapping[OptionContract, float],
    market: DayMarket,
) -> SignalCandidate | None:
    maps: dict[tuple[str, str], dict[float, float]] = {}
    for series in (near_series, far_series):
        for right in ("C", "P"):
            maps[(series, right)] = {
                contract.strike: float(price)
                for contract, price in latest.items()
                if contract.series == series
                and contract.right == right
                and math.isfinite(price)
                and price >= 0.0
            }
    common = np.asarray(
        sorted(
            set(maps[(near_series, "C")])
            & set(maps[(near_series, "P")])
            & set(maps[(far_series, "C")])
            & set(maps[(far_series, "P")])
        ),
        dtype=np.float64,
    )
    if common.size < 2:
        return None

    price_arrays: dict[tuple[str, str], np.ndarray] = {
        key: np.asarray([mapping[float(strike)] for strike in common])
        for key, mapping in maps.items()
    }

    def boxes(series: str) -> np.ndarray:
        calls = price_arrays[(series, "C")]
        puts = price_arrays[(series, "P")]
        return calls[:-1] - calls[1:] + puts[1:] - puts[:-1]

    gaps = boxes(near_series) - boxes(far_series)
    all_leg_prices = np.vstack(
        [
            price_arrays[(near_series, "C")][:-1],
            price_arrays[(near_series, "C")][1:],
            price_arrays[(near_series, "P")][1:],
            price_arrays[(near_series, "P")][:-1],
            price_arrays[(far_series, "C")][:-1],
            price_arrays[(far_series, "C")][1:],
            price_arrays[(far_series, "P")][1:],
            price_arrays[(far_series, "P")][:-1],
        ]
    )
    per_leg_tax = np.floor(
        all_leg_prices * OPTION_MULTIPLIER * TAIFEX_OPTION_PREMIUM_TAX_RATE + 0.5
    )
    estimated_costs = 2.0 * (
        8.0 * FIXED_FEES_PER_CONTRACT_SIDE[OPTION_PRODUCT] + per_leg_tax.sum(axis=0)
    )
    net_edges = np.abs(gaps) * OPTION_MULTIPLIER - estimated_costs
    index = int(np.argmax(net_edges))
    if net_edges[index] <= 0.0 or gaps[index] == 0.0:
        return None

    low = float(common[index])
    high = float(common[index + 1])
    gap = float(gaps[index])
    near_long = _long_box_intents(near_series, low, high)
    far_long = _long_box_intents(far_series, low, high)
    if gap > 0.0:
        intents = (
            tuple(replace(intent, quantity=-intent.quantity) for intent in near_long)
            + far_long
        )
        direction = "short_near_box_long_far_box"
    else:
        intents = near_long + tuple(
            replace(intent, quantity=-intent.quantity) for intent in far_long
        )
        direction = "long_near_box_short_far_box"
    estimated = _estimated_candidate_cost(
        market=market,
        intents=intents,
        latest_options=latest,
        latest_futures={},
        expiry=far_expiry,
        decision_ns=decision_ns,
    )
    observed = abs(gap) * OPTION_MULTIPLIER
    if observed <= estimated:
        return None
    return SignalCandidate(
        variant_id=CROSS_EXPIRY_VARIANT,
        family=_family(CROSS_EXPIRY_VARIANT),
        decision_ns=decision_ns,
        series=f"{near_series}|{far_series}",
        expiry=far_expiry,
        intents=intents,
        observed_edge_twd=observed,
        estimated_cost_twd=estimated,
        relationship_value_points=gap,
        lower_bound_points=0.0,
        upper_bound_points=0.0,
        direction=direction,
        strikes=(low, high),
    )


def _scan_cross_expiry_calendar_box_day(
    *,
    market: DayMarket,
    series_expiries: Sequence[tuple[str, date]],
    start_ns: int,
    deadline_ns: int,
    execute: bool,
) -> tuple[CycleState | None, dict[str, Any]]:
    eligible = [
        (series, expiry)
        for series, expiry in series_expiries
        if expiry > market.trading_date
    ]
    pairs = [
        (near_series, near_expiry, far_series, far_expiry)
        for index, (near_series, near_expiry) in enumerate(eligible)
        for far_series, far_expiry in eligible[index + 1 :]
        if near_expiry < far_expiry
    ]
    pair_keys = [(near, far) for near, _near_expiry, far, _far_expiry in pairs]
    pair_payload = {
        (near, far): (near_expiry, far_expiry)
        for near, near_expiry, far, far_expiry in pairs
    }
    pairs_by_series: dict[str, set[tuple[str, str]]] = {
        series: set() for series, _expiry in eligible
    }
    for near, far in pair_keys:
        pairs_by_series[near].add((near, far))
        pairs_by_series[far].add((near, far))

    eligible_series = set(pairs_by_series)
    rows = [
        row
        for series in eligible_series
        for row in _series_events(
            market,
            series=series,
            start_ns=start_ns,
            end_ns=deadline_ns,
        )
    ]
    rows.sort(key=lambda row: (row[0], row[1].series, row[1].strike, row[1].right))
    latest_by_series: dict[str, dict[OptionContract, float]] = {
        series: {} for series in eligible_series
    }
    pair_candidates: dict[tuple[str, str], SignalCandidate | None] = {
        key: None for key in pair_keys
    }
    selected: CycleState | None = None
    signal_seconds = 0
    max_net_edge = -math.inf
    max_gross_edge = 0.0
    fill_failures = 0
    index = 0
    while index < len(rows):
        decision_ns = rows[index][0]
        updated_series: set[str] = set()
        while index < len(rows) and rows[index][0] == decision_ns:
            _event_ns, contract, price = rows[index]
            latest_by_series[contract.series][contract] = price
            updated_series.add(contract.series)
            index += 1
        affected_pairs = {
            pair
            for series in updated_series
            for pair in pairs_by_series.get(series, ())
        }
        for near_series, far_series in affected_pairs:
            near_expiry, far_expiry = pair_payload[(near_series, far_series)]
            pair_candidates[(near_series, far_series)] = _calendar_box_pair_candidate(
                decision_ns=decision_ns,
                near_series=near_series,
                near_expiry=near_expiry,
                far_series=far_series,
                far_expiry=far_expiry,
                latest={
                    **latest_by_series[near_series],
                    **latest_by_series[far_series],
                },
                market=market,
            )
        candidates = [candidate for candidate in pair_candidates.values() if candidate]
        if not candidates:
            continue
        candidate = max(candidates, key=lambda item: item.observed_net_edge_twd)
        signal_seconds += 1
        max_net_edge = max(max_net_edge, candidate.observed_net_edge_twd)
        max_gross_edge = max(max_gross_edge, candidate.observed_edge_twd)
        if execute and selected is None:
            selected = _fill_candidate(
                market=market,
                candidate=candidate,
                deadline_ns=deadline_ns,
            )
            if selected is None:
                fill_failures += 1
    return selected, {
        "signal_seconds": signal_seconds,
        "max_observed_gross_edge_twd": max_gross_edge,
        "max_observed_net_edge_twd": (
            None if max_net_edge == -math.inf else max_net_edge
        ),
        "unfillable_signal_attempts": fill_failures,
        "series": "cross_expiry_pairs",
        "series_count": len(eligible),
        "series_pair_count": len(pairs),
    }


def _scan_future_day(
    *,
    market: DayMarket,
    variant_id: str,
    series: str,
    expiry: date,
    start_ns: int,
    deadline_ns: int,
    execute: bool,
) -> tuple[CycleState | None, dict[str, Any]]:
    product_a, _count_a, product_b, _count_b = FUTURES_PACKAGE_SPECS[variant_id]
    times = np.unique(
        np.concatenate(
            [market._future_payload(product_a)[0], market._future_payload(product_b)[0]]
        )
    )
    start = int(np.searchsorted(times, start_ns, side="left"))
    stop = int(np.searchsorted(times, deadline_ns, side="left"))
    signal_seconds = 0
    max_net_edge = -math.inf
    max_gross_edge = 0.0
    fill_failures = 0
    selected: CycleState | None = None
    for raw_ns in times[start:stop]:
        candidate = _future_candidate(
            variant_id=variant_id,
            decision_ns=int(raw_ns),
            series=series,
            expiry=expiry,
            market=market,
        )
        if candidate is None:
            continue
        signal_seconds += 1
        max_net_edge = max(max_net_edge, candidate.observed_net_edge_twd)
        max_gross_edge = max(max_gross_edge, candidate.observed_edge_twd)
        if execute and selected is None:
            selected = _fill_candidate(
                market=market, candidate=candidate, deadline_ns=deadline_ns
            )
            if selected is None:
                fill_failures += 1
    return selected, {
        "signal_seconds": signal_seconds,
        "max_observed_gross_edge_twd": max_gross_edge,
        "max_observed_net_edge_twd": (
            None if max_net_edge == -math.inf else max_net_edge
        ),
        "unfillable_signal_attempts": fill_failures,
        "series": series,
        "series_count": 1,
        "series_pair_count": 0,
    }


def _complete_series(
    *,
    market: DayMarket,
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
    verified_dates: set[date],
    monthly_only: bool = False,
) -> list[tuple[str, date]]:
    present = {contract.series for contract in market.option_events}
    candidates: list[tuple[date, str]] = []
    for series in present:
        payload = settlements_by_series.get(series)
        if payload is None:
            continue
        expiry, _settlement = payload
        if expiry < market.trading_date or expiry not in verified_dates:
            continue
        if monthly_only and len(series) != 6:
            continue
        candidates.append((expiry, series))
    return [(series, expiry) for expiry, series in sorted(candidates)]


def _nearest_complete_series(
    *,
    market: DayMarket,
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
    verified_dates: set[date],
    monthly_only: bool = False,
) -> tuple[str, date] | None:
    candidates = _complete_series(
        market=market,
        settlements_by_series=settlements_by_series,
        verified_dates=verified_dates,
        monthly_only=monthly_only,
    )
    return candidates[0] if candidates else None


def _settlements_by_series(
    settlements: Mapping[tuple[date, str], Mapping[str, object]],
) -> dict[str, tuple[date, Mapping[str, object]]]:
    output: dict[str, tuple[date, Mapping[str, object]]] = {}
    for (settlement_date, series), payload in settlements.items():
        previous = output.get(series)
        if previous is not None and previous[0] != settlement_date:
            raise ValueError(f"multiple settlement dates for {series}")
        output[series] = (settlement_date, payload)
    return output


def _intrinsic(right: str, strike: float, settlement: float) -> float:
    return (
        max(settlement - strike, 0.0) if right == "C" else max(strike - settlement, 0.0)
    )


def _state_value(
    state: CycleState,
    *,
    trading_date: date,
    option_marks: Mapping[tuple[date, str, float, str], Mapping[str, object]],
    snapshot: DailySnapshot,
    settlement_price: float | None,
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
) -> tuple[float, float, float, float, str, float | None]:
    gross = 0.0
    settlement_tax = 0.0
    mark_sources: set[str] = set()
    staleness: list[float] = []
    for leg in state.legs:
        intent = leg.intent
        if intent.instrument_type == "option":
            assert intent.series is not None
            assert intent.strike is not None
            assert intent.option_right is not None
            component_settlement_price = settlement_price
            component_settlement_date = state.expiry
            component = settlements_by_series.get(intent.series)
            if (
                component_settlement_price is None
                and component is not None
                and trading_date >= component[0]
            ):
                component_settlement_date = component[0]
                component_settlement_price = float(component[1]["settlement_price"])
            if component_settlement_price is not None:
                mark = _intrinsic(
                    intent.option_right,
                    intent.strike,
                    component_settlement_price,
                )
                mark_sources.add("official_final_settlement")
                if mark > 0.0:
                    settlement_tax += abs(intent.quantity) * (
                        option_cash_settlement_transaction_tax_twd(
                            component_settlement_price,
                            settlement_date=component_settlement_date,
                            multiplier_twd_per_point=OPTION_MULTIPLIER,
                        )
                    )
            else:
                key = (
                    trading_date,
                    intent.series,
                    intent.strike,
                    intent.option_right,
                )
                daily = option_marks.get(key)
                mark = None
                if daily is not None:
                    for field, label in (
                        ("settlement", "official_daily_settlement"),
                        ("close", "official_daily_last_trade"),
                    ):
                        value = daily.get(field)
                        if value is not None and math.isfinite(float(value)):
                            mark = float(value)
                            mark_sources.add(label)
                            break
                if mark is None:
                    fallback = snapshot.option_fallback_marks.get(
                        (intent.series, intent.strike, intent.option_right)
                    )
                    if fallback is None:
                        raise ValueError(
                            f"missing mark {trading_date}/{intent.series}/"
                            f"{intent.strike}/{intent.option_right}"
                        )
                    mark = fallback.price
                    mark_sources.add("tick_last_trade_fallback")
                    session_end_ns = _ns(
                        datetime.combine(trading_date, SESSION_END, tzinfo=TAIPEI)
                    )
                    staleness.append(
                        (session_end_ns - fallback.event_ns) / 1_000_000_000.0
                    )
            gross += intent.quantity * (mark - leg.fill.price) * OPTION_MULTIPLIER
        else:
            if settlement_price is not None:
                mark = settlement_price
                mark_sources.add("official_final_settlement")
                settlement_tax += abs(intent.quantity) * taifex_tax_per_contract_twd(
                    settlement_price,
                    multiplier_twd_per_point=FUTURES_MULTIPLIERS[intent.product],
                    tax_rate=stock_index_futures_tax_rate(state.expiry),
                )
            else:
                future_mark = snapshot.futures_marks.get(intent.product)
                if future_mark is None:
                    raise ValueError(
                        f"missing futures mark {trading_date}/{intent.product}"
                    )
                mark = future_mark.price
                mark_sources.add("tick_last_trade")
                session_end_ns = _ns(
                    datetime.combine(trading_date, SESSION_END, tzinfo=TAIPEI)
                )
                staleness.append(
                    (session_end_ns - future_mark.event_ns) / 1_000_000_000.0
                )
            gross += (
                intent.quantity
                * (mark - leg.fill.price)
                * FUTURES_MULTIPLIERS[intent.product]
            )
    net = gross - state.entry_fees_twd - state.entry_taxes_twd - settlement_tax
    return (
        gross,
        state.entry_fees_twd,
        state.entry_taxes_twd,
        settlement_tax,
        ",".join(sorted(mark_sources)),
        max(staleness) if staleness else None,
    )


def _performance_metrics(returns: np.ndarray) -> dict[str, float | None]:
    if returns.ndim != 1 or returns.size == 0 or not np.isfinite(returns).all():
        raise ValueError("returns must be a finite one-dimensional array")
    wealth = np.cumprod(1.0 + returns)
    cumulative = float(wealth[-1] - 1.0)
    peaks = np.maximum.accumulate(np.concatenate(([1.0], wealth)))[:-1]
    drawdowns = wealth / peaks - 1.0
    mdd = float(drawdowns.min(initial=0.0))
    std = float(np.std(returns, ddof=1)) if returns.size > 1 else 0.0
    sharpe = (
        float(np.mean(returns) / std * math.sqrt(TRADING_DAYS_PER_YEAR))
        if std > 0.0
        else None
    )
    downside = np.minimum(returns, 0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside))))
    sortino = (
        float(np.mean(returns) / downside_deviation * math.sqrt(TRADING_DAYS_PER_YEAR))
        if downside_deviation > 0.0
        else None
    )
    annualized = (
        float((1.0 + cumulative) ** (TRADING_DAYS_PER_YEAR / returns.size) - 1.0)
        if cumulative > -1.0
        else -1.0
    )
    calmar = annualized / abs(mdd) if mdd < 0.0 else None
    return {
        "cumulative_compounded_return": cumulative,
        "annualized_return": annualized,
        "annualized_sharpe": sharpe,
        "annualized_sortino": sortino,
        "maximum_drawdown": mdd,
        "annualized_calmar": calmar,
    }


def _write_csv(frame: pl.DataFrame, path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_csv(temporary)
    temporary.replace(path)


def _cycle_actual_result(
    state: CycleState, settlement_price: float
) -> dict[str, float]:
    gross = 0.0
    settlement_tax = 0.0
    for leg in state.legs:
        intent = leg.intent
        if intent.instrument_type == "option":
            assert intent.strike is not None and intent.option_right is not None
            terminal = _intrinsic(intent.option_right, intent.strike, settlement_price)
            gross += intent.quantity * (terminal - leg.fill.price) * OPTION_MULTIPLIER
            if terminal > 0.0:
                settlement_tax += abs(intent.quantity) * (
                    option_cash_settlement_transaction_tax_twd(
                        settlement_price,
                        settlement_date=state.expiry,
                        multiplier_twd_per_point=OPTION_MULTIPLIER,
                    )
                )
        else:
            gross += (
                intent.quantity
                * (settlement_price - leg.fill.price)
                * FUTURES_MULTIPLIERS[intent.product]
            )
            settlement_tax += abs(intent.quantity) * taifex_tax_per_contract_twd(
                settlement_price,
                multiplier_twd_per_point=FUTURES_MULTIPLIERS[intent.product],
                tax_rate=stock_index_futures_tax_rate(state.expiry),
            )
    net = gross - state.entry_fees_twd - state.entry_taxes_twd - settlement_tax
    return {
        "gross_pnl_twd": gross,
        "entry_fees_twd": state.entry_fees_twd,
        "exit_fees_twd": 0.0,
        "total_fixed_fees_twd": state.entry_fees_twd,
        "entry_taxes_twd": state.entry_taxes_twd,
        "exit_taxes_twd": 0.0,
        "settlement_taxes_twd": settlement_tax,
        "net_pnl_twd": net,
    }


def _cycle_multi_expiry_result(
    state: CycleState,
    settlement_prices: Mapping[str, float],
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
) -> dict[str, float]:
    if state.variant_id != CROSS_EXPIRY_VARIANT:
        raise ValueError("multi-expiry settlement is only valid for calendar boxes")
    gross = 0.0
    settlement_tax = 0.0
    for leg in state.legs:
        intent = leg.intent
        if intent.instrument_type != "option" or intent.series is None:
            raise ValueError("calendar box contains a non-option leg")
        if intent.strike is None or intent.option_right is None:
            raise ValueError("calendar box contains an incomplete option leg")
        settlement_price = float(settlement_prices[intent.series])
        terminal = _intrinsic(intent.option_right, intent.strike, settlement_price)
        gross += intent.quantity * (terminal - leg.fill.price) * OPTION_MULTIPLIER
        if terminal > 0.0:
            settlement_date = settlements_by_series[intent.series][0]
            settlement_tax += abs(intent.quantity) * (
                option_cash_settlement_transaction_tax_twd(
                    settlement_price,
                    settlement_date=settlement_date,
                    multiplier_twd_per_point=OPTION_MULTIPLIER,
                )
            )
    net = gross - state.entry_fees_twd - state.entry_taxes_twd - settlement_tax
    return {
        "gross_pnl_twd": gross,
        "entry_fees_twd": state.entry_fees_twd,
        "exit_fees_twd": 0.0,
        "total_fixed_fees_twd": state.entry_fees_twd,
        "entry_taxes_twd": state.entry_taxes_twd,
        "exit_taxes_twd": 0.0,
        "settlement_taxes_twd": settlement_tax,
        "net_pnl_twd": net,
    }


def _cycle_exit_result(
    state: CycleState,
    exit_state: CycleExit,
    *,
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]]
    | None = None,
) -> dict[str, float]:
    if exit_state.reason == "expiry_settlement":
        if exit_state.settlement_price is None:
            raise ValueError(f"missing settlement price: {state.cycle_id}")
        return _cycle_actual_result(state, exit_state.settlement_price)
    if exit_state.reason == "multi_expiry_settlement":
        if exit_state.settlement_prices is None or settlements_by_series is None:
            raise ValueError(
                "multi-expiry result requires prices and settlement-series mapping"
            )
        return _cycle_multi_expiry_result(
            state,
            exit_state.settlement_prices,
            settlements_by_series,
        )
    if exit_state.reason != "relationship_convergence":
        raise ValueError(f"unsupported exit reason: {exit_state.reason}")
    entry_by_key = {_leg_price_key(leg.intent): leg for leg in state.legs}
    exit_by_key = {_leg_price_key(leg.intent): leg for leg in exit_state.legs}
    if set(entry_by_key) != set(exit_by_key):
        raise ValueError(f"entry/exit leg mismatch: {state.cycle_id}")
    gross = 0.0
    for key, entry_leg in entry_by_key.items():
        exit_leg = exit_by_key[key]
        intent = entry_leg.intent
        multiplier = (
            OPTION_MULTIPLIER
            if intent.instrument_type == "option"
            else FUTURES_MULTIPLIERS[intent.product]
        )
        gross += (
            intent.quantity * (exit_leg.fill.price - entry_leg.fill.price) * multiplier
        )
    total_fees = state.entry_fees_twd + exit_state.exit_fees_twd
    net = gross - total_fees - state.entry_taxes_twd - exit_state.exit_taxes_twd
    return {
        "gross_pnl_twd": gross,
        "entry_fees_twd": state.entry_fees_twd,
        "exit_fees_twd": exit_state.exit_fees_twd,
        "total_fixed_fees_twd": total_fees,
        "entry_taxes_twd": state.entry_taxes_twd,
        "exit_taxes_twd": exit_state.exit_taxes_twd,
        "settlement_taxes_twd": 0.0,
        "net_pnl_twd": net,
    }


def _trade_rows_for_legs(
    *,
    state: CycleState,
    trading_date: date,
    event_kind: str,
    decision_ns: int,
    legs: Sequence[FilledLeg],
) -> list[dict[str, Any]]:
    return [
        {
            "cycle_id": state.cycle_id,
            "variant_id": state.variant_id,
            "family": state.family,
            "trading_date": trading_date,
            "event_kind": event_kind,
            "leg_index": leg_index,
            "decision_ts": _datetime_from_ns(decision_ns),
            "fill_ts": _datetime_from_ns(leg.fill.event_ns),
            "fill_delay_seconds": (leg.fill.event_ns - decision_ns) / 1_000_000_000.0,
            "instrument_type": leg.intent.instrument_type,
            "product": leg.intent.product,
            "series": leg.intent.series,
            "strike": leg.intent.strike,
            "option_right": leg.intent.option_right,
            "quantity": leg.intent.quantity,
            "price_points": leg.fill.price,
            "fixed_fee_twd": leg.fixed_fee_twd,
            "transaction_tax_twd": leg.entry_tax_twd,
        }
        for leg_index, leg in enumerate(legs)
    ]


def _scan_entry_day(
    *,
    raw_root: Path,
    trading_date: date,
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
    verified_dates: frozenset[date],
    scan_start: time,
    entry_deadline: time,
) -> DayEntryScan:
    """Compute the state-independent entry scan for one day.

    This is the expensive full-chain pass and is safe to execute in a worker
    process because it neither reads nor mutates a cross-day position.
    """

    market = _build_day_market(
        raw_root,
        trading_date,
        futures_products=("TX", "MTX", "TMF"),
    )
    start_ns = _ns(datetime.combine(trading_date, scan_start, tzinfo=TAIPEI))
    deadline_ns = _ns(
        datetime.combine(
            trading_date,
            min(entry_deadline, time(13, 20)),
            tzinfo=TAIPEI,
        )
    )
    selected: dict[str, CycleState | None] = {
        variant_id: None for variant_id in VARIANT_IDS
    }
    diagnostics: dict[str, Mapping[str, Any]] = {
        variant_id: {
            "signal_seconds": 0,
            "max_observed_gross_edge_twd": 0.0,
            "max_observed_net_edge_twd": None,
            "unfillable_signal_attempts": 0,
            "series": None,
            "series_count": 0,
            "series_pair_count": 0,
        }
        for variant_id in VARIANT_IDS
    }
    complete_series = _complete_series(
        market=market,
        settlements_by_series=settlements_by_series,
        verified_dates=set(verified_dates),
    )
    if complete_series:
        structure_scans = [
            _scan_option_structures_day(
                market=market,
                series=series,
                expiry=expiry,
                start_ns=start_ns,
                deadline_ns=deadline_ns,
                execute_by_variant={
                    variant_id: True for variant_id in OPTION_STRUCTURE_FAMILIES
                },
            )
            for series, expiry in complete_series
        ]
        structure_selected, structure_diagnostics = _aggregate_structure_series_scans(
            structure_scans,
            series_count=len(complete_series),
        )
        selected.update(structure_selected)
        diagnostics.update(structure_diagnostics)
        selected[CROSS_EXPIRY_VARIANT], diagnostics[CROSS_EXPIRY_VARIANT] = (
            _scan_cross_expiry_calendar_box_day(
                market=market,
                series_expiries=complete_series,
                start_ns=start_ns,
                deadline_ns=deadline_ns,
                execute=True,
            )
        )
    front_month = taifex_front_month(trading_date)
    monthly = next(
        (payload for payload in complete_series if payload[0] == front_month),
        None,
    )
    if monthly is not None:
        series, expiry = monthly
        (
            selected["monthly_put_call_parity_mtx"],
            diagnostics["monthly_put_call_parity_mtx"],
        ) = _scan_option_day(
            market=market,
            variant_id="monthly_put_call_parity_mtx",
            series=series,
            expiry=expiry,
            start_ns=start_ns,
            deadline_ns=deadline_ns,
            execute=True,
        )
    front_payload = settlements_by_series.get(front_month)
    if front_payload is not None and front_payload[0] in verified_dates:
        expiry = front_payload[0]
        for variant_id in FUTURES_PACKAGE_SPECS:
            selected[variant_id], diagnostics[variant_id] = _scan_future_day(
                market=market,
                variant_id=variant_id,
                series=front_month,
                expiry=expiry,
                start_ns=start_ns,
                deadline_ns=deadline_ns,
                execute=True,
            )
    return DayEntryScan(trading_date, market, selected, diagnostics)


def _scan_entry_day_task(payload: tuple[Any, ...]) -> DayEntryScan:
    (
        raw_root,
        trading_date,
        settlements_by_series,
        verified_dates,
        scan_start,
        entry_deadline,
    ) = payload
    return _scan_entry_day(
        raw_root=raw_root,
        trading_date=trading_date,
        settlements_by_series=settlements_by_series,
        verified_dates=verified_dates,
        scan_start=scan_start,
        entry_deadline=entry_deadline,
    )


def _scan_entries_parallel(
    *,
    raw_root: Path,
    trading_dates: Sequence[date],
    settlements_by_series: Mapping[str, tuple[date, Mapping[str, object]]],
    scan_start: time,
    entry_deadline: time,
    workers: int,
) -> dict[date, DayEntryScan]:
    if workers <= 0:
        raise ValueError("scan workers must be positive")
    verified_dates = frozenset(trading_dates)
    tasks = [
        (
            raw_root,
            trading_date,
            settlements_by_series,
            verified_dates,
            scan_start,
            entry_deadline,
        )
        for trading_date in trading_dates
    ]
    results: dict[date, DayEntryScan] = {}
    if workers == 1:
        for index, task in enumerate(tasks, start=1):
            scan = _scan_entry_day_task(task)
            results[scan.trading_date] = scan
            print(
                f"[taifex-arbitrage-scan] date={scan.trading_date} "
                f"progress={index}/{len(tasks)} workers=1",
                flush=True,
            )
        return results
    with ProcessPoolExecutor(max_workers=workers) as pool:
        future_to_date = {
            pool.submit(_scan_entry_day_task, task): task[1] for task in tasks
        }
        completed = 0
        for future in as_completed(future_to_date):
            scan = future.result()
            results[scan.trading_date] = scan
            completed += 1
            print(
                f"[taifex-arbitrage-scan] date={scan.trading_date} "
                f"progress={completed}/{len(tasks)} workers={workers}",
                flush=True,
            )
    if set(results) != set(trading_dates):
        raise ValueError("parallel entry scan did not return every trading date")
    return results


def run_backtest(
    *,
    raw_root: Path,
    option_daily_root: Path,
    settlement_path: Path,
    output_dir: Path,
    scan_start: time,
    entry_deadline: time,
    scan_workers: int,
) -> dict[str, Any]:
    manifest, manifest_sha, trading_dates = _verify_manifest(raw_root)
    if trading_dates[-1] > TAIFEX_MARGIN_VERIFIED_THROUGH:
        raise ValueError(
            "futures initial-margin schedule is not verified through sample end: "
            f"{trading_dates[-1]} > {TAIFEX_MARGIN_VERIFIED_THROUGH}"
        )
    settlements = _load_official_final_settlements(settlement_path)
    settlements_by_series = _settlements_by_series(settlements)
    entry_scans = _scan_entries_parallel(
        raw_root=raw_root,
        trading_dates=trading_dates,
        settlements_by_series=settlements_by_series,
        scan_start=scan_start,
        entry_deadline=entry_deadline,
        workers=scan_workers,
    )

    states: dict[str, CycleState | None] = {
        variant_id: None for variant_id in VARIANT_IDS
    }
    snapshots: dict[str, list[DailySnapshot]] = {
        variant_id: [] for variant_id in VARIANT_IDS
    }
    opportunity_rows: list[dict[str, Any]] = []
    cycles: list[CycleState] = []
    cycle_exits: dict[str, CycleExit] = {}
    trade_rows: list[dict[str, Any]] = []

    for day_number, trading_date in enumerate(trading_dates, start=1):
        print(
            f"[taifex-arbitrage-simulate] date={trading_date} "
            f"progress={day_number}/{len(trading_dates)} variants={len(VARIANT_IDS)}",
            flush=True,
        )
        entry_scan = entry_scans[trading_date]
        market = entry_scan.market
        start_ns = _ns(datetime.combine(trading_date, scan_start, tzinfo=TAIPEI))
        session_end_ns = _ns(datetime.combine(trading_date, SESSION_END, tzinfo=TAIPEI))
        realized_today: dict[str, list[str]] = {
            variant_id: [] for variant_id in VARIANT_IDS
        }
        blocked_reentry_today: set[str] = set()
        convergence_fill_failures: dict[str, int] = {
            variant_id: 0 for variant_id in VARIANT_IDS
        }

        # Existing packages get the first chance to exit.  A strategy that
        # closes today waits until the next trading day before taking a new
        # package, preventing repeated stale-print churn inside one session.
        for variant_id, state in tuple(states.items()):
            if state is None:
                continue
            if variant_id == CROSS_EXPIRY_VARIANT:
                component_series = state.series.split("|")
                earliest_expiry = min(
                    settlements_by_series[series][0] for series in component_series
                )
                if trading_date > earliest_expiry:
                    continue
                deadline_time = (
                    EXPIRY_TIME if trading_date == earliest_expiry else SESSION_END
                )
            else:
                deadline_time = (
                    EXPIRY_TIME if state.expiry == trading_date else SESSION_END
                )
            exit_deadline_ns = _ns(
                datetime.combine(trading_date, deadline_time, tzinfo=TAIPEI)
            )
            exit_state, failed_attempts = _try_convergence_exit(
                market=market,
                state=state,
                start_ns=start_ns,
                deadline_ns=exit_deadline_ns,
            )
            convergence_fill_failures[variant_id] += failed_attempts
            if exit_state is None:
                continue
            cycle_exits[state.cycle_id] = exit_state
            realized_today[variant_id].append(state.cycle_id)
            blocked_reentry_today.add(variant_id)
            states[variant_id] = None
            trade_rows.extend(
                _trade_rows_for_legs(
                    state=state,
                    trading_date=trading_date,
                    event_kind="exit_convergence_next_strictly_later_trade",
                    decision_ns=exit_state.decision_ns,
                    legs=exit_state.legs,
                )
            )
        for variant_id in VARIANT_IDS:
            state = states[variant_id]
            execute = state is None and variant_id not in blocked_reentry_today
            selected = entry_scan.selected[variant_id] if execute else None
            diagnostics = dict(entry_scan.diagnostics[variant_id])
            diagnostics["convergence_exit_fill_failures"] = convergence_fill_failures[
                variant_id
            ]
            opportunity_rows.append(
                {
                    "trading_date": trading_date,
                    "variant_id": variant_id,
                    "family": _family(variant_id),
                    "position_already_open": state is not None,
                    **diagnostics,
                }
            )
            if selected is not None:
                states[variant_id] = selected
                state = selected
                cycles.append(selected)
                trade_rows.extend(
                    _trade_rows_for_legs(
                        state=selected,
                        trading_date=trading_date,
                        event_kind="entry_next_strictly_later_trade",
                        decision_ns=selected.decision_ns,
                        legs=selected.legs,
                    )
                )
                exit_deadline_ns = _ns(
                    datetime.combine(
                        trading_date,
                        EXPIRY_TIME if selected.expiry == trading_date else SESSION_END,
                        tzinfo=TAIPEI,
                    )
                )
                exit_state, failed_attempts = _try_convergence_exit(
                    market=market,
                    state=selected,
                    start_ns=selected.completion_ns,
                    deadline_ns=exit_deadline_ns,
                )
                convergence_fill_failures[variant_id] += failed_attempts
                diagnostics["convergence_exit_fill_failures"] = (
                    convergence_fill_failures[variant_id]
                )
                if exit_state is not None:
                    cycle_exits[selected.cycle_id] = exit_state
                    realized_today[variant_id].append(selected.cycle_id)
                    states[variant_id] = None
                    state = None
                    trade_rows.extend(
                        _trade_rows_for_legs(
                            state=selected,
                            trading_date=trading_date,
                            event_kind=("exit_convergence_next_strictly_later_trade"),
                            decision_ns=exit_state.decision_ns,
                            legs=exit_state.legs,
                        )
                    )

            # Convergence is preferred; official settlement is the mandatory
            # terminal close only when a package is still open on expiry.
            state = states[variant_id]
            if state is not None and state.expiry == trading_date:
                settlement_ns = _ns(
                    datetime.combine(trading_date, EXPIRY_TIME, tzinfo=TAIPEI)
                )
                if variant_id == CROSS_EXPIRY_VARIANT:
                    component_prices = {
                        series: float(
                            settlements_by_series[series][1]["settlement_price"]
                        )
                        for series in state.series.split("|")
                    }
                    cycle_exits[state.cycle_id] = CycleExit(
                        trading_date=trading_date,
                        reason="multi_expiry_settlement",
                        decision_ns=settlement_ns,
                        completion_ns=settlement_ns,
                        legs=(),
                        settlement_prices=component_prices,
                    )
                else:
                    settlement_price = float(
                        settlements[(state.expiry, state.series)]["settlement_price"]
                    )
                    cycle_exits[state.cycle_id] = CycleExit(
                        trading_date=trading_date,
                        reason="expiry_settlement",
                        decision_ns=settlement_ns,
                        completion_ns=settlement_ns,
                        legs=(),
                        settlement_price=settlement_price,
                    )
                realized_today[variant_id].append(state.cycle_id)
                states[variant_id] = None
                state = None

            opportunity_rows[-1]["convergence_exit_fill_failures"] = (
                convergence_fill_failures[variant_id]
            )

            fallback: dict[tuple[str, float, str], Fill] = {}
            future_marks: dict[str, Fill] = {}
            if state is not None:
                for leg in state.legs:
                    intent = leg.intent
                    if intent.instrument_type == "option":
                        contract = intent.option_contract
                        mark = market.last_option_trade_before(contract, session_end_ns)
                        if mark is not None:
                            fallback[
                                (
                                    contract.series,
                                    contract.strike,
                                    contract.right,
                                )
                            ] = mark
                    else:
                        mark = market.last_future_trade_before(
                            intent.product, session_end_ns
                        )
                        if mark is not None:
                            future_marks[intent.product] = mark
            snapshots[variant_id].append(
                DailySnapshot(
                    trading_date=trading_date,
                    state=state,
                    realized_cycle_ids=tuple(realized_today[variant_id]),
                    option_fallback_marks=fallback,
                    futures_marks=future_marks,
                )
            )

    if any(state is not None for state in states.values()):
        raise ValueError("complete-cycle selector left open sample-end positions")

    option_targets: set[tuple[date, str, float, str]] = set()
    for variant_snapshots in snapshots.values():
        for snapshot in variant_snapshots:
            if snapshot.state is None:
                continue
            for leg in snapshot.state.legs:
                intent = leg.intent
                if intent.instrument_type == "option":
                    assert intent.series is not None
                    assert intent.strike is not None
                    assert intent.option_right is not None
                    component = settlements_by_series.get(intent.series)
                    if component is not None and snapshot.trading_date >= component[0]:
                        continue
                    option_targets.add(
                        (
                            snapshot.trading_date,
                            intent.series,
                            intent.strike,
                            intent.option_right,
                        )
                    )
    source_paths = _daily_source_paths(
        option_daily_root, trading_dates[0], trading_dates[-1]
    )
    monthly_targets = {target for target in option_targets if len(target[1]) == 6}
    weekly_targets = option_targets - monthly_targets
    option_marks: dict[tuple[date, str, float, str], dict[str, object]] = {}
    if monthly_targets:
        option_marks.update(
            load_taifex_option_daily_contract_rows(
                source_paths, monthly_targets, series_scope="monthly"
            )
        )
    if weekly_targets:
        option_marks.update(
            load_taifex_option_daily_contract_rows(
                source_paths, weekly_targets, series_scope="weekly"
            )
        )

    cycle_rows: list[dict[str, Any]] = []
    state_results: dict[str, dict[str, float]] = {}
    for state in cycles:
        exit_state = cycle_exits.get(state.cycle_id)
        if exit_state is None:
            raise ValueError(f"cycle has no terminal exit: {state.cycle_id}")
        result = _cycle_exit_result(
            state,
            exit_state,
            settlements_by_series=settlements_by_series,
        )
        state_results[state.cycle_id] = result
        cycle_rows.append(
            {
                "cycle_id": state.cycle_id,
                "variant_id": state.variant_id,
                "family": state.family,
                "series": state.series,
                "expiry": state.expiry,
                "entry_date": state.entry_date,
                "decision_ts": _datetime_from_ns(state.decision_ns),
                "completion_ts": _datetime_from_ns(state.completion_ns),
                "max_fill_delay_seconds": (state.completion_ns - state.decision_ns)
                / 1_000_000_000.0,
                "direction": state.direction,
                "strikes_json": json.dumps(state.strikes),
                "relationship_value_points": state.relationship_value_points,
                "lower_bound_points": state.lower_bound_points,
                "upper_bound_points": state.upper_bound_points,
                "observed_gross_edge_twd": state.observed_edge_twd,
                "estimated_cost_at_signal_twd": state.estimated_cost_twd,
                "exit_date": exit_state.trading_date,
                "exit_reason": exit_state.reason,
                "exit_decision_ts": _datetime_from_ns(exit_state.decision_ns),
                "exit_completion_ts": _datetime_from_ns(exit_state.completion_ns),
                "exit_fill_delay_seconds": (
                    exit_state.completion_ns - exit_state.decision_ns
                )
                / 1_000_000_000.0,
                "holding_seconds": (exit_state.completion_ns - state.completion_ns)
                / 1_000_000_000.0,
                "official_final_settlement_price": exit_state.settlement_price,
                "official_final_settlement_prices_json": (
                    json.dumps(exit_state.settlement_prices, sort_keys=True)
                    if exit_state.settlement_prices is not None
                    else None
                ),
                **result,
                "return_on_common_capital": (
                    result["net_pnl_twd"] / COMMON_CAPITAL_TWD
                ),
            }
        )

    daily_rows: list[dict[str, Any]] = []
    metric_rows: list[dict[str, Any]] = []
    for variant_id in VARIANT_IDS:
        realized = 0.0
        previous_equity = 0.0
        returns: list[float] = []
        variant_daily: list[dict[str, Any]] = []
        for snapshot in snapshots[variant_id]:
            for cycle_id in snapshot.realized_cycle_ids:
                realized += state_results[cycle_id]["net_pnl_twd"]
            state = snapshot.state
            active_value = 0.0
            gross = fees = entry_taxes = settlement_taxes = 0.0
            sources = "none"
            staleness = None
            if state is not None:
                (
                    gross,
                    fees,
                    entry_taxes,
                    settlement_taxes,
                    sources,
                    staleness,
                ) = _state_value(
                    state,
                    trading_date=snapshot.trading_date,
                    option_marks=option_marks,
                    snapshot=snapshot,
                    settlement_price=None,
                    settlements_by_series=settlements_by_series,
                )
                active_value = gross - fees - entry_taxes - settlement_taxes
            equity = realized + active_value
            daily_pnl = equity - previous_equity
            daily_return = daily_pnl / COMMON_CAPITAL_TWD
            returns.append(daily_return)
            previous_equity = equity
            wealth = float(np.prod(1.0 + np.asarray(returns, dtype=np.float64)))
            variant_daily.append(
                {
                    "trading_date": snapshot.trading_date,
                    "variant_id": variant_id,
                    "family": _family(variant_id),
                    "cycle_id": state.cycle_id if state is not None else None,
                    "realized_cycle_ids_json": json.dumps(snapshot.realized_cycle_ids),
                    "position_open": state is not None,
                    "had_convergence_exit": any(
                        cycle_exits[cycle_id].reason == "relationship_convergence"
                        for cycle_id in snapshot.realized_cycle_ids
                    ),
                    "had_expiry_settlement": any(
                        cycle_exits[cycle_id].reason == "expiry_settlement"
                        for cycle_id in snapshot.realized_cycle_ids
                    ),
                    "had_multi_expiry_settlement": any(
                        cycle_exits[cycle_id].reason == "multi_expiry_settlement"
                        for cycle_id in snapshot.realized_cycle_ids
                    ),
                    "gross_marked_pnl_twd": gross,
                    "marked_equity_net_twd": equity,
                    "daily_net_pnl_twd": daily_pnl,
                    "common_capital_twd": COMMON_CAPITAL_TWD,
                    "daily_return": daily_return,
                    "cumulative_compounded_return": wealth - 1.0,
                    "mark_sources": sources,
                    "tick_mark_max_staleness_seconds": staleness,
                }
            )
        frame_returns = np.asarray(returns, dtype=np.float64)
        metrics = _performance_metrics(frame_returns)
        total_cycles = [row for row in cycle_rows if row["variant_id"] == variant_id]
        total_net = float(sum(float(row["net_pnl_twd"]) for row in total_cycles))
        if not math.isclose(total_net, realized, abs_tol=1e-7):
            raise ValueError(
                f"daily/cycle PnL mismatch {variant_id}: {realized} != {total_net}"
            )
        daily_rows.extend(variant_daily)
        metric_rows.append(
            {
                "variant_id": variant_id,
                "family": _family(variant_id),
                "trading_days": len(returns),
                "completed_cycles": len(total_cycles),
                "winning_cycles": sum(
                    float(row["net_pnl_twd"]) > 0.0 for row in total_cycles
                ),
                "total_gross_pnl_twd": float(
                    sum(float(row["gross_pnl_twd"]) for row in total_cycles)
                ),
                "total_fixed_fees_twd": float(
                    sum(float(row["total_fixed_fees_twd"]) for row in total_cycles)
                ),
                "total_entry_taxes_twd": float(
                    sum(float(row["entry_taxes_twd"]) for row in total_cycles)
                ),
                "total_exit_taxes_twd": float(
                    sum(float(row["exit_taxes_twd"]) for row in total_cycles)
                ),
                "total_settlement_taxes_twd": float(
                    sum(float(row["settlement_taxes_twd"]) for row in total_cycles)
                ),
                "total_net_pnl_twd": total_net,
                "convergence_exits": sum(
                    row["exit_reason"] == "relationship_convergence"
                    for row in total_cycles
                ),
                "expiry_settlements": sum(
                    row["exit_reason"] == "expiry_settlement" for row in total_cycles
                ),
                "multi_expiry_settlements": sum(
                    row["exit_reason"] == "multi_expiry_settlement"
                    for row in total_cycles
                ),
                "common_capital_twd": COMMON_CAPITAL_TWD,
                "simple_return_on_common_capital": total_net / COMMON_CAPITAL_TWD,
                **metrics,
            }
        )

    daily = pl.DataFrame(daily_rows, infer_schema_length=None).sort(
        ["variant_id", "trading_date"]
    )
    metrics = pl.DataFrame(metric_rows, infer_schema_length=None).sort(
        "total_net_pnl_twd", descending=True
    )
    cycles_frame = (
        pl.DataFrame(cycle_rows, infer_schema_length=None).sort(
            ["variant_id", "entry_date"]
        )
        if cycle_rows
        else pl.DataFrame()
    )
    opportunities = pl.DataFrame(opportunity_rows, infer_schema_length=None).sort(
        ["variant_id", "trading_date"]
    )
    trades = (
        pl.DataFrame(trade_rows, infer_schema_length=None).sort(
            ["variant_id", "trading_date", "fill_ts", "leg_index"]
        )
        if trade_rows
        else pl.DataFrame()
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "daily_returns": output_dir / "daily_returns.parquet",
        "metrics": output_dir / "metrics.parquet",
        "cycles": output_dir / "cycles.parquet",
        "opportunities": output_dir / "opportunities.parquet",
        "trades": output_dir / "trades.parquet",
    }
    for label, frame in (
        ("daily_returns", daily),
        ("metrics", metrics),
        ("cycles", cycles_frame),
        ("opportunities", opportunities),
        ("trades", trades),
    ):
        if frame.is_empty():
            continue
        _atomic_parquet(frame, outputs[label])
        _write_csv(frame, output_dir / f"{label}.csv")

    capture_audit = raw_root / "shioaji_fop_captures/audits/2026-08-10.json"
    capture_payload = (
        json.loads(capture_audit.read_text(encoding="utf-8"))
        if capture_audit.is_file()
        else None
    )
    data_quality = {
        "status": "usable_with_execution_proxy_limitations",
        "historical_tick_manifest_status": manifest["status"],
        "trading_dates": {
            "first": trading_dates[0].isoformat(),
            "last": trading_dates[-1].isoformat(),
            "count": len(trading_dates),
        },
        "historical_quote_type": "TAIFEX transaction prints at whole-second timestamps",
        "historical_bid_ask_available": False,
        "execution_proxy": TAIFEX_TRADE_PROXY_SOURCE,
        "same_second_execution_allowed": False,
        "volume_capacity_modeled": False,
        "package_unit": "one strategy package; butterflies require two center contracts",
        "spot_taiex_tick_available": False,
        "spot_index_tradeable_directly": False,
        "shioaji_book_capture_outside_backtest": (
            {
                "trade_date": capture_payload.get("trade_date"),
                "status": capture_payload.get("status"),
                "book_rows": capture_payload.get("book_rows"),
                "valid_book_fraction": capture_payload.get("valid_book_fraction"),
                "simulation_account_environment": capture_payload.get(
                    "simulation_account_environment"
                ),
            }
            if capture_payload is not None
            else None
        ),
        "analysis_gate": (
            "same-expiry bounds and boxes have fixed payoff relationships; the "
            "cross-expiry calendar box assumes zero financing interest between its "
            "two fixed settlement cash flows; convergence fills remain next-trade "
            "proxies and are not executable bid/ask evidence"
        ),
    }
    _atomic_json(output_dir / "data_quality.json", data_quality)

    aggregate_opportunities = (
        opportunities.group_by(["variant_id", "family"])
        .agg(
            pl.col("signal_seconds").sum(),
            pl.col("max_observed_gross_edge_twd").max(),
            pl.col("max_observed_net_edge_twd").max(),
            pl.col("unfillable_signal_attempts").sum(),
            pl.col("convergence_exit_fill_failures").sum(),
        )
        .sort("variant_id")
    )
    _atomic_parquet(aggregate_opportunities, output_dir / "opportunity_summary.parquet")
    _write_csv(aggregate_opportunities, output_dir / "opportunity_summary.csv")

    summary = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "manifest_path": str(raw_root / "manifest.json"),
            "manifest_sha256": manifest_sha,
            "final_settlement_path": str(settlement_path),
            "final_settlement_sha256": _sha256_path(settlement_path),
            "option_daily_paths": [str(path) for path in source_paths],
        },
        "sample": {
            "first_trading_date": trading_dates[0].isoformat(),
            "last_trading_date": trading_dates[-1].isoformat(),
            "trading_days": len(trading_dates),
        },
        "execution": {
            "signal_clock": "after completed whole second",
            "fill_clock": "first strictly later transaction per leg",
            "historical_bid_ask": False,
            "quantity": "one package",
            "position_exit": (
                "first completed-second return to the no-arbitrage interval, "
                "filled at the first strictly later transaction per leg; "
                "same-expiry packages use official final settlement only if "
                "convergence does not occur; each cross-expiry box component uses "
                "its own official final settlement if convergence does not occur "
                "before the earlier expiry"
            ),
            "same_day_reentry_after_exit": False,
            "entry_scan_workers": scan_workers,
            "estimated_signal_cost": (
                "round-trip fixed fees and transaction taxes at signal prices"
            ),
            "fixed_fees_per_contract_side_twd": FIXED_FEES_PER_CONTRACT_SIDE,
            "option_premium_tax_rate": TAIFEX_OPTION_PREMIUM_TAX_RATE,
            "futures_tax_rate": stock_index_futures_tax_rate(trading_dates[-1]),
            "financing_interest_rate": 0.0,
            "option_series_scope": (
                "all receipt-verified monthly, Wednesday-weekly W, and "
                "Friday-weekly F series whose expiries are inside the sample"
            ),
        },
        "capital": {
            "common_capital_twd": COMMON_CAPITAL_TWD,
            "definition": (
                "two gross TX initial margins; identical denominator for every variant"
            ),
            "portfolio_margin_offsets_modeled": False,
        },
        "variant_count": len(VARIANT_IDS),
        "completed_cycles": len(cycles),
        "metrics": metrics.to_dicts(),
        "opportunity_summary": aggregate_opportunities.to_dicts(),
        "unsupported_or_non_model_free": [
            {
                "method": "TAIEX spot vs futures cash-and-carry",
                "status": "not_backtested",
                "reason": (
                    "no synchronized intraday constituent-basket execution data; "
                    "the published TAIEX level is not directly tradeable"
                ),
            },
            {
                "method": "weekly Put-Call parity hedged by monthly MTX",
                "status": "excluded",
                "reason": (
                    "the weekly option and monthly future settle on different dates, "
                    "so their index exposure does not cancel"
                ),
            },
            {
                "method": "cross-expiry synthetic-forward convergence",
                "status": "excluded_from_arbitrage_total",
                "reason": (
                    "different settlement dates retain index forward-curve, carry, "
                    "and dividend exposure unless another dated hedge is supplied"
                ),
            },
        ],
        "data_quality": data_quality,
    }
    _atomic_json(output_dir / "summary.json", summary)
    receipt_paths = [
        output_dir / name
        for name in (
            "cycles.csv",
            "cycles.parquet",
            "daily_returns.csv",
            "daily_returns.parquet",
            "data_quality.json",
            "metrics.csv",
            "metrics.parquet",
            "opportunities.csv",
            "opportunities.parquet",
            "opportunity_summary.csv",
            "opportunity_summary.parquet",
            "summary.json",
            "trades.csv",
            "trades.parquet",
        )
    ]
    missing_receipt_paths = [str(path) for path in receipt_paths if not path.is_file()]
    if missing_receipt_paths:
        raise ValueError(f"missing receipt outputs: {missing_receipt_paths}")
    receipt = {
        "schema_version": 1,
        "status": "complete",
        "outputs": {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in receipt_paths
        },
    }
    _atomic_json(output_dir / "receipt.json", receipt)
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument(
        "--option-daily-root", type=Path, default=DEFAULT_OPTION_DAILY_ROOT
    )
    parser.add_argument("--settlement-path", type=Path, default=DEFAULT_SETTLEMENT_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--scan-start", default=DEFAULT_SCAN_START.strftime("%H:%M"))
    parser.add_argument(
        "--entry-deadline", default=DEFAULT_ENTRY_DEADLINE.strftime("%H:%M")
    )
    parser.add_argument(
        "--scan-workers",
        type=int,
        default=min(8, os.cpu_count() or 1),
        help="Worker processes for independent per-day full-chain entry scans.",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    summary = run_backtest(
        raw_root=args.raw_root,
        option_daily_root=args.option_daily_root,
        settlement_path=args.settlement_path,
        output_dir=args.output_dir,
        scan_start=_parse_time(args.scan_start),
        entry_deadline=_parse_time(args.entry_deadline),
        scan_workers=args.scan_workers,
    )
    print(
        json.dumps(
            {
                "status": summary["status"],
                "output_dir": str(args.output_dir),
                "variant_count": summary["variant_count"],
                "completed_cycles": summary["completed_cycles"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
