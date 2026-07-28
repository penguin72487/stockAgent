"""Exact integer-share oracles for Taiwan open/close execution.

The differentiable dual-session ledger operates in normalized currency units.
This module is its discrete CPU audit counterpart:

* every position and order is an integer number of symbol-specific lots;
* OPEN and CLOSE use their exact auction prices;
* commission minimums and whole-TWD rounding apply independently to each
  symbol-side aggregate order at each event;
* both events share one daily volume, turnover, and short-inventory budget;
* settlement queues age once at the start of an exchange session and the two
  events enqueue one account-level net T+2 claim after the close.

``tw_cash`` accepts signed post-event target weights in ``actions[T,2,S]``.
``tw_overnight`` accepts ``[exit_at_open_fraction, entry_at_open,
entry_at_close]`` in ``actions[T,3,S]``.  Every close position in the latter is
today's cohort and is a mandatory exit no later than the following active
session's close.

This is an eager semantic oracle, not a differentiable training kernel.
Runtime is O(T*S), apart from an exact O((S+K) log S) lot-endpoint search for
non-monotone reduction funding and a fixed 56-step scalar affordability search
for openings when nonlinear fixed fees make the requested order unaffordable.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Literal

import numpy as np

from stockagent.backtest.tw_execution import normalize_fee_rounding
from stockagent.backtest.tw_integer_execution import (
    TaiwanIntegerState,
    _as_advance_mask,
    _as_commission_rebate_calendar,
    _as_commission_rebate_rates,
    _as_execution_price_matrix,
    _as_lot_vector,
    _as_minimum_commission,
    _as_nonnegative_matrix,
    _as_nonnegative_ratio,
    _as_short_maintenance_ratio,
    _apply_commission_rebate_at_close,
    _cash_dividend_inputs,
    _commission_fees_by_symbol,
    _commission_rebates_by_symbol,
    _copy_validate_state,
    _capacity_for_new_payable,
    _empty_result_arrays,
    _enqueue_net_claim,
    _limit_signed_transition_by_quantity,
    _log_and_simple_return,
    _record_ruin,
    _record_state,
    _round_quantity_down_to_lots,
    _round_short_margin_up_to_hundreds,
    _safe_execution_price_row,
    _scaled_opening_holdings,
    _separate_reductions_and_openings,
    _settle_start_of_session,
    _tax_fees_by_symbol,
)


OPEN = 0
CLOSE = 1

OVERNIGHT_EXIT_AT_OPEN = 0
OVERNIGHT_ENTRY_AT_OPEN = 1
OVERNIGHT_ENTRY_AT_CLOSE = 2

DualSessionIntegerMode = Literal["tw_cash", "tw_overnight"]


@dataclass(frozen=True, slots=True)
class _DescendingLotBreakpoint:
    """One exact rational lot-scale breakpoint, largest scale first."""

    remaining_lots: int
    total_lots: int
    symbol_index: int

    def __lt__(self, other: "_DescendingLotBreakpoint") -> bool:
        left = int(self.remaining_lots) * int(other.total_lots)
        right = int(other.remaining_lots) * int(self.total_lots)
        if left != right:
            # ``heapq`` is a min-heap. Reverse the rational comparison so its
            # root is the largest scale still represented by any symbol.
            return left > right
        return int(self.symbol_index) < int(other.symbol_index)

    def same_scale(self, other: "_DescendingLotBreakpoint") -> bool:
        return (
            int(self.remaining_lots) * int(other.total_lots)
            == int(other.remaining_lots) * int(self.total_lots)
        )


@dataclass(frozen=True, slots=True)
class TaiwanDualSessionIntegerBacktestResult:
    """Audit-complete output from a dual-session integer oracle."""

    mode: DualSessionIntegerMode
    strategy_returns: np.ndarray
    simple_returns: np.ndarray
    turnover: np.ndarray
    event_turnovers: np.ndarray
    positions: np.ndarray
    open_positions: np.ndarray
    weights: np.ndarray
    open_weights: np.ndarray
    settled_cash_history: np.ndarray
    payable_history: np.ndarray
    receivable_history: np.ndarray
    payable_queue_history: np.ndarray
    receivable_queue_history: np.ndarray
    execution_payable_history: np.ndarray
    execution_receivable_history: np.ndarray
    execution_nav_history: np.ndarray
    nav_history: np.ndarray
    settlement_net_history: np.ndarray
    fee_history: np.ndarray
    event_fee_history: np.ndarray
    commission_rebate_accrued_history: np.ndarray
    commission_rebate_paid_history: np.ndarray
    commission_rebate_current_history: np.ndarray
    commission_rebate_due_history: np.ndarray
    event_commission_rebate_accrued: np.ndarray
    default_mask: np.ndarray
    executed_buy_shares: np.ndarray
    executed_sell_shares: np.ndarray
    executed_long_buy_shares: np.ndarray
    executed_long_sell_shares: np.ndarray
    executed_short_open_shares: np.ndarray
    executed_short_cover_shares: np.ndarray
    executed_buy_weights: np.ndarray
    executed_sell_weights: np.ndarray
    executed_long_buy_weights: np.ndarray
    executed_long_sell_weights: np.ndarray
    executed_short_open_weights: np.ndarray
    executed_short_cover_weights: np.ndarray
    short_sale_collateral_history: np.ndarray
    short_margin_collateral_history: np.ndarray
    due_positions_history: np.ndarray | None
    final_due_positions: np.ndarray | None
    final_weights: np.ndarray
    final_state: TaiwanIntegerState

    @property
    def close_positions(self) -> np.ndarray:
        return self.positions

    @property
    def close_weights(self) -> np.ndarray:
        return self.weights

    @property
    def final_commission_rebate_current(self) -> float:
        return float(self.final_state.commission_rebate_current)

    @property
    def final_commission_rebate_due(self) -> float:
        return float(self.final_state.commission_rebate_due)

    @property
    def final_commission_rebate_month_id(self) -> int:
        return int(self.final_state.commission_rebate_month_id)


@dataclass(frozen=True, slots=True)
class _IntegerEventResult:
    holdings: np.ndarray
    short_sale_collateral: np.ndarray
    short_margin_collateral: np.ndarray
    settlement_net: float
    fee: float
    commission_rebate: float
    turnover_notional: float
    remaining_volume: np.ndarray | None
    remaining_turnover: float | None
    remaining_short_capacity: np.ndarray | None
    long_buy: np.ndarray
    long_sell: np.ndarray
    short_open: np.ndarray
    short_cover: np.ndarray
    default_now: bool = False


def _as_actions(
    value: object,
    *,
    mode: DualSessionIntegerMode,
) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    phases = 2 if mode == "tw_cash" else 3
    if (
        result.ndim != 3
        or result.shape[0] <= 0
        or result.shape[1] != phases
        or result.shape[2] <= 0
    ):
        raise ValueError(
            f"{mode} actions must be non-empty with shape [T,{phases},S]"
        )
    if not np.all(np.isfinite(result)):
        raise ValueError("actions must contain only finite values")
    if mode == "tw_cash":
        gross = np.sum(np.abs(result), axis=2)
        if np.any(gross > 1.0 + 1e-12):
            raise ValueError("each tw_cash event gross exposure cannot exceed 1.0")
    else:
        exit_fraction = result[:, OVERNIGHT_EXIT_AT_OPEN]
        if np.any((exit_fraction < 0.0) | (exit_fraction > 1.0)):
            raise ValueError(
                "tw_overnight actions[:,0,:] exit fractions must be within [0,1]"
            )
        entries = result[:, 1:]
        if np.any(np.sum(np.abs(entries), axis=(1, 2)) > 1.0 + 1e-12):
            raise ValueError(
                "tw_overnight open/close entries share one gross exposure budget"
            )
        entry_open = result[:, OVERNIGHT_ENTRY_AT_OPEN]
        entry_close = result[:, OVERNIGHT_ENTRY_AT_CLOSE]
        opposite = (
            (entry_open != 0.0)
            & (entry_close != 0.0)
            & ((entry_open > 0.0) != (entry_close > 0.0))
        )
        if np.any(opposite):
            raise ValueError(
                "tw_overnight open-entry and close-entry allocations may not "
                "reverse the same symbol within one session"
            )
    return result


def _as_phase_bool(
    name: str,
    value: object | None,
    shape: tuple[int, int, int],
    *,
    default: bool,
) -> np.ndarray:
    if value is None:
        return np.full(shape, default, dtype=np.bool_)
    result = np.asarray(value)
    if result.shape != shape:
        raise ValueError(f"{name} must have shape {shape}")
    if result.dtype != np.bool_:
        raise ValueError(f"{name} must contain booleans")
    return result.astype(np.bool_, copy=False)


def _as_phase_rate(
    name: str,
    value: object,
    shape: tuple[int, int, int],
) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    time, phases, symbols = shape
    if result.ndim == 0:
        result = np.broadcast_to(result, shape)
    elif result.shape == (symbols,):
        result = np.broadcast_to(result.reshape(1, 1, symbols), shape)
    elif result.shape == (phases, symbols):
        result = np.broadcast_to(result.reshape(1, phases, symbols), shape)
    elif result.shape == (time, symbols):
        result = np.broadcast_to(result[:, None, :], shape)
    elif result.shape != shape:
        raise ValueError(
            f"{name} must be scalar, [S], [2,S], [T,S], or [T,2,S]"
        )
    if not np.all(np.isfinite(result)) or np.any(result < 0.0):
        raise ValueError(f"{name} must contain finite non-negative rates")
    return result


def _split_phase_fee_rates(
    buy_fee_rates: object,
    sell_fee_rates: object,
    shape: tuple[int, int, int],
) -> tuple[np.ndarray, np.ndarray]:
    commission = _as_phase_rate("buy_fee_rates", buy_fee_rates, shape)
    sell_all_in = _as_phase_rate("sell_fee_rates", sell_fee_rates, shape)
    sell_tax = sell_all_in - commission
    tolerance = np.maximum(1.0, np.abs(sell_all_in)) * 1e-15
    if np.any(sell_tax < -tolerance):
        raise ValueError(
            "sell_fee_rates must be at least buy_fee_rates because the exact "
            "Taiwan profile assumes symmetric commission plus sell-only tax"
        )
    return commission, np.maximum(sell_tax, 0.0)


def _initial_carry_marks(
    state: TaiwanIntegerState,
    symbols: int,
) -> np.ndarray:
    marks = np.ones(symbols, dtype=np.float64)
    held = np.asarray(state.holdings, dtype=np.int64) != 0
    if not np.any(held):
        return marks
    if state.last_weights is None:
        raise ValueError(
            "dual-session initial_state with holdings requires last_weights "
            "to reconstruct the preceding close marks"
        )
    weights = np.asarray(state.last_weights, dtype=np.float64)
    reconstructed = (
        weights[held]
        * float(state.last_nav)
        / np.asarray(state.holdings, dtype=np.float64)[held]
    )
    if not np.all(np.isfinite(reconstructed)) or np.any(reconstructed <= 0.0):
        raise ValueError(
            "initial_state.last_weights cannot reconstruct finite positive "
            "preceding close marks"
        )
    marks[held] = reconstructed
    return marks


def _valuation_price_row(
    raw_prices: np.ndarray,
    index: int,
    *,
    holdings: np.ndarray,
    carry_marks: np.ndarray,
    name: str,
) -> np.ndarray:
    raw = raw_prices[index]
    valid = np.isfinite(raw) & (raw > 0.0)
    result = np.where(valid, raw, 1.0)
    held_without_quote = (holdings != 0) & ~valid
    if np.any(held_without_quote):
        carry = carry_marks[held_without_quote]
        if not np.all(np.isfinite(carry)) or np.any(carry <= 0.0):
            raise ValueError(
                f"{name} cannot value held symbols without a raw quote or "
                "finite positive carried mark"
            )
        result[held_without_quote] = carry
    return result.astype(np.float64, copy=False)


def _ledger_nav(
    *,
    cash: float,
    holdings: np.ndarray,
    prices: np.ndarray,
    payable: np.ndarray,
    receivable: np.ndarray,
    short_sale_collateral: np.ndarray,
    short_margin_collateral: np.ndarray,
    pending_net_claim: float = 0.0,
    commission_rebate_current: float = 0.0,
    commission_rebate_due: float = 0.0,
) -> float:
    return float(
        cash
        + np.dot(holdings.astype(np.float64), prices)
        + np.sum(short_sale_collateral)
        + np.sum(short_margin_collateral)
        + np.sum(receivable)
        + commission_rebate_current
        + commission_rebate_due
        - np.sum(payable)
        + pending_net_claim
    )


def _target_holdings_from_weights(
    weights: np.ndarray,
    *,
    nav: float,
    prices: np.ndarray,
    long_lot_sizes: np.ndarray,
    short_lot_sizes: np.ndarray,
) -> np.ndarray:
    targets = np.zeros(weights.shape, dtype=np.int64)
    positive = weights > 0.0
    negative = weights < 0.0
    if np.any(positive):
        targets[positive] = _round_quantity_down_to_lots(
            weights[positive] * nav / prices[positive],
            long_lot_sizes[positive],
        )
    if np.any(negative):
        targets[negative] = -_round_quantity_down_to_lots(
            -weights[negative] * nav / prices[negative],
            short_lot_sizes[negative],
        )
    return targets


def _fractional_due_exit_target(
    due: np.ndarray,
    fraction: np.ndarray,
    *,
    long_lot_sizes: np.ndarray,
    short_lot_sizes: np.ndarray,
) -> np.ndarray:
    target = np.asarray(due, dtype=np.int64).copy()
    positive = due > 0
    negative = due < 0
    if np.any(positive):
        quantities = _round_quantity_down_to_lots(
            due[positive].astype(np.float64) * fraction[positive],
            long_lot_sizes[positive],
        )
        quantities[fraction[positive] >= 1.0] = due[positive][
            fraction[positive] >= 1.0
        ]
        target[positive] -= quantities
    if np.any(negative):
        absolute_due = -due[negative]
        quantities = _round_quantity_down_to_lots(
            absolute_due.astype(np.float64) * fraction[negative],
            short_lot_sizes[negative],
        )
        quantities[fraction[negative] >= 1.0] = absolute_due[
            fraction[negative] >= 1.0
        ]
        target[negative] += quantities
    return target


def _empty_event(
    holdings: np.ndarray,
    short_sale_collateral: np.ndarray,
    short_margin_collateral: np.ndarray,
    remaining_volume: np.ndarray | None,
    remaining_turnover: float | None,
    remaining_short_capacity: np.ndarray | None,
    *,
    default_now: bool,
) -> _IntegerEventResult:
    zeros = np.zeros_like(holdings, dtype=np.int64)
    return _IntegerEventResult(
        holdings=holdings.copy(),
        short_sale_collateral=short_sale_collateral.copy(),
        short_margin_collateral=short_margin_collateral.copy(),
        settlement_net=0.0,
        fee=0.0,
        commission_rebate=0.0,
        turnover_notional=0.0,
        remaining_volume=(
            None if remaining_volume is None else remaining_volume.copy()
        ),
        remaining_turnover=remaining_turnover,
        remaining_short_capacity=(
            None
            if remaining_short_capacity is None
            else remaining_short_capacity.copy()
        ),
        long_buy=zeros.copy(),
        long_sell=zeros.copy(),
        short_open=zeros.copy(),
        short_cover=zeros.copy(),
        default_now=default_now,
    )


def _execute_signed_integer_event(
    *,
    holdings: np.ndarray,
    desired_holdings: np.ndarray,
    mandatory_long_sell: np.ndarray,
    mandatory_short_cover: np.ndarray,
    prices: np.ndarray,
    raw_price_matrix: np.ndarray,
    time_index: int,
    price_name: str,
    can_buy: np.ndarray,
    can_sell: np.ndarray,
    can_short_open: np.ndarray,
    commission_rates: np.ndarray,
    sell_tax_rates: np.ndarray,
    minimum_commission: float,
    commission_rounding: str,
    tax_rounding: str,
    long_lot_sizes: np.ndarray,
    short_lot_sizes: np.ndarray,
    short_margin_rates: np.ndarray,
    short_handling_rates: np.ndarray,
    short_sale_collateral: np.ndarray,
    short_margin_collateral: np.ndarray,
    short_maintenance_ratio: float,
    cash: float,
    payable: np.ndarray,
    receivable: np.ndarray,
    pending_net_claim: float,
    remaining_volume: np.ndarray | None,
    remaining_turnover: float | None,
    remaining_short_capacity: np.ndarray | None,
    event_nav: float,
    gross_budget: float,
    commission_rebate_rates: np.ndarray | None = None,
) -> _IntegerEventResult:
    """Execute one exact signed event without aging or enqueueing settlement."""

    rebate_rates = (
        np.zeros_like(commission_rates, dtype=np.float64)
        if commission_rebate_rates is None
        else np.asarray(commission_rebate_rates, dtype=np.float64)
    )
    if rebate_rates.shape != commission_rates.shape:
        raise ValueError(
            "commission_rebate_rates must match event commission_rates"
        )
    if not np.all(np.isfinite(rebate_rates)) or np.any(rebate_rates < 0.0):
        raise ValueError(
            "commission_rebate_rates must contain finite non-negative rates"
        )
    gross_rates = np.asarray(commission_rates, dtype=np.float64)
    tolerance = np.maximum(1.0, np.abs(gross_rates)) * 1e-15
    if np.any(rebate_rates > gross_rates + tolerance):
        raise ValueError(
            "commission_rebate_rates cannot exceed gross commission rates"
        )
    current_long = np.maximum(holdings, 0)
    current_short = np.maximum(-holdings, 0)
    mandatory_sell = np.minimum(
        np.asarray(mandatory_long_sell, dtype=np.int64),
        current_long,
    )
    mandatory_cover = np.minimum(
        np.asarray(mandatory_short_cover, dtype=np.int64),
        current_short,
    )
    blocked_mandatory = (
        ((mandatory_sell > 0) & ~can_sell)
        | ((mandatory_cover > 0) & ~can_buy)
    )
    if np.any(blocked_mandatory):
        return _empty_event(
            holdings,
            short_sale_collateral,
            short_margin_collateral,
            remaining_volume,
            remaining_turnover,
            remaining_short_capacity,
            default_now=True,
        )

    base_holdings = holdings - mandatory_sell + mandatory_cover
    proposed_holdings = np.asarray(desired_holdings, dtype=np.int64).copy()
    increasing = proposed_holdings > base_holdings
    proposed_holdings[increasing & ~can_buy] = base_holdings[
        increasing & ~can_buy
    ]
    decreasing = proposed_holdings < base_holdings
    proposed_holdings[decreasing & ~can_sell] = base_holdings[
        decreasing & ~can_sell
    ]
    decreasing = proposed_holdings < base_holdings
    cannot_add_short = decreasing & ~can_short_open
    proposed_holdings[cannot_add_short] = np.maximum(
        proposed_holdings[cannot_add_short],
        np.minimum(base_holdings[cannot_add_short], 0),
    )

    requested_change = proposed_holdings != base_holdings
    required_price = (mandatory_sell > 0) | (mandatory_cover > 0) | (
        requested_change & (can_buy | can_sell)
    )
    _safe_execution_price_row(
        price_name,
        raw_price_matrix,
        time_index,
        required_price,
    )

    if remaining_volume is not None:
        volume_limits = np.maximum(remaining_volume, 0).astype(np.int64)
        proposed_holdings = _limit_signed_transition_by_quantity(
            base_holdings,
            proposed_holdings,
            volume_limits,
            long_lot_sizes,
            short_lot_sizes,
        )

    ordinary_delta = proposed_holdings - base_holdings
    if remaining_turnover is not None:
        ordinary_notional = float(
            np.dot(np.abs(ordinary_delta).astype(np.float64), prices)
        )
        if ordinary_notional > remaining_turnover + 1e-9:
            scaled_quantities = np.floor(
                np.abs(ordinary_delta).astype(np.float64)
                * max(remaining_turnover, 0.0)
                / ordinary_notional
                + 1e-12
            )
            proposed_holdings = _limit_signed_transition_by_quantity(
                base_holdings,
                proposed_holdings,
                np.minimum(
                    scaled_quantities,
                    np.iinfo(np.int64).max,
                ).astype(np.int64),
                long_lot_sizes,
                short_lot_sizes,
            )

    reduced_holdings, opening_holdings = _separate_reductions_and_openings(
        base_holdings,
        proposed_holdings,
    )
    reduced_gross = float(
        np.dot(np.abs(reduced_holdings).astype(np.float64), prices)
    )
    opening_gross = float(
        np.dot(np.abs(opening_holdings).astype(np.float64), prices)
    )
    gross_open_budget = max(event_nav * gross_budget - reduced_gross, 0.0)
    if opening_gross > gross_open_budget + 1e-9:
        proposed_holdings = _scaled_opening_holdings(
            reduced_holdings,
            opening_holdings,
            gross_open_budget / opening_gross if opening_gross > 0.0 else 0.0,
            long_lot_sizes,
            short_lot_sizes,
        )

    if remaining_short_capacity is not None:
        old_short = np.maximum(-base_holdings, 0)
        proposed_short = np.maximum(-proposed_holdings, 0)
        requested_open = np.maximum(proposed_short - old_short, 0)
        admitted_open = np.minimum(
            requested_open,
            np.maximum(remaining_short_capacity, 0).astype(np.int64),
        )
        capped_short = old_short + admitted_open
        needs_cap = requested_open > admitted_open
        proposed_holdings[needs_cap] = -capped_short[needs_cap]

    short_collateral_before_trade = float(
        np.sum(short_sale_collateral) + np.sum(short_margin_collateral)
    )

    def trade_details(new_holdings: np.ndarray) -> dict[str, object]:
        # Mandatory exits are gross physical orders.  Classify only the
        # discretionary transition relative to the post-mandatory base, then
        # add the mandatory legs back.  Comparing original holdings directly
        # with the final target would net a required exit against a same-side
        # re-entry and understate fees, turnover, collateral, and T+2 claims.
        voluntary_delta = new_holdings - base_holdings
        voluntary_buys = np.maximum(voluntary_delta, 0)
        voluntary_sells = np.maximum(-voluntary_delta, 0)
        base_long = np.maximum(base_holdings, 0)
        base_short = np.maximum(-base_holdings, 0)
        voluntary_cover = np.minimum(voluntary_buys, base_short)
        long_buy = voluntary_buys - voluntary_cover
        voluntary_long_sell = np.minimum(voluntary_sells, base_long)
        short_open = voluntary_sells - voluntary_long_sell
        long_sell = mandatory_sell + voluntary_long_sell
        cover = mandatory_cover + voluntary_cover
        buys = long_buy + cover
        sells = long_sell + short_open
        old_short = np.maximum(-holdings, 0)

        sell_notional = sells.astype(np.float64) * prices
        sell_commission = _commission_fees_by_symbol(
            sell_notional,
            commission_rates,
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
        )
        sell_tax = _tax_fees_by_symbol(
            sell_notional,
            sell_tax_rates,
            rounding=tax_rounding,
        )
        sell_fees = sell_commission + sell_tax
        sell_rebate = _commission_rebates_by_symbol(
            sell_notional,
            commission_rates,
            rebate_rates,
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
            gross_commissions=sell_commission,
        )
        short_sell_fraction = np.divide(
            short_open,
            sells,
            out=np.zeros_like(prices),
            where=sells > 0,
        )
        short_sell_fees = sell_fees * short_sell_fraction
        short_handling_fees = (
            short_open.astype(np.float64) * prices * short_handling_rates
        )
        long_sell_fees = sell_fees - short_sell_fees
        short_sale_net = (
            short_open.astype(np.float64) * prices
            - short_sell_fees
            - short_handling_fees
        )
        added_sale_collateral = np.maximum(short_sale_net, 0.0)
        short_sale_fee_deficit = np.minimum(short_sale_net, 0.0)
        added_margin = _round_short_margin_up_to_hundreds(
            short_open.astype(np.float64) * prices * short_margin_rates,
            short_open > 0,
        )

        buy_notional = buys.astype(np.float64) * prices
        buy_fees = _commission_fees_by_symbol(
            buy_notional,
            commission_rates,
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
        )
        buy_rebate = _commission_rebates_by_symbol(
            buy_notional,
            commission_rates,
            rebate_rates,
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
            gross_commissions=buy_fees,
        )
        cover_fraction_of_buy = np.divide(
            cover,
            buys,
            out=np.zeros_like(prices),
            where=buys > 0,
        )
        cover_fees = buy_fees * cover_fraction_of_buy
        long_buy_fees = buy_fees - cover_fees

        cover_fraction = np.divide(
            cover,
            old_short,
            out=np.zeros_like(prices),
            where=old_short > 0,
        )
        released_sale = short_sale_collateral * cover_fraction
        released_margin = short_margin_collateral * cover_fraction
        fully_covered = (old_short > 0) & (cover == old_short)
        released_sale[fully_covered] = short_sale_collateral[fully_covered]
        released_margin[fully_covered] = short_margin_collateral[fully_covered]

        required_collateral: float | None = None
        collateral_before_release = 0.0
        if np.any(cover):
            remaining_short = np.maximum(-new_holdings, 0)
            if not np.any(remaining_short):
                released_sale = short_sale_collateral.copy()
                released_margin = short_margin_collateral.copy()
            else:
                remaining_liability = float(
                    np.dot(remaining_short.astype(np.float64), prices)
                )
                required_collateral = (
                    short_maintenance_ratio * remaining_liability
                )
                collateral_before_release = float(
                    short_collateral_before_trade
                    + np.sum(added_sale_collateral)
                    + np.sum(added_margin)
                )
                maximum_release = max(
                    collateral_before_release - required_collateral,
                    0.0,
                )
                proportional_release = float(
                    np.sum(released_sale) + np.sum(released_margin)
                )
                if proportional_release > maximum_release:
                    release_scale = maximum_release / proportional_release
                    released_sale *= release_scale
                    released_margin *= release_scale

        sale_collateral_after = (
            short_sale_collateral - released_sale + added_sale_collateral
        )
        margin_collateral_after = (
            short_margin_collateral - released_margin + added_margin
        )
        if (
            required_collateral is not None
            and np.sum(released_sale) + np.sum(released_margin) > 0.0
            and collateral_before_release >= required_collateral
        ):
            for _ in range(8):
                represented = float(
                    np.sum(sale_collateral_after)
                    + np.sum(margin_collateral_after)
                )
                if represented >= required_collateral:
                    break
                release_total = float(
                    np.sum(released_sale) + np.sum(released_margin)
                )
                if release_total <= 0.0:
                    break
                deficit = required_collateral - represented
                correction_scale = np.nextafter(
                    max(release_total - deficit, 0.0) / release_total,
                    0.0,
                )
                released_sale *= correction_scale
                released_margin *= correction_scale
                sale_collateral_after = (
                    short_sale_collateral
                    - released_sale
                    + added_sale_collateral
                )
                margin_collateral_after = (
                    short_margin_collateral - released_margin + added_margin
                )
            else:
                released_sale.fill(0.0)
                released_margin.fill(0.0)
                sale_collateral_after = (
                    short_sale_collateral + added_sale_collateral
                )
                margin_collateral_after = (
                    short_margin_collateral + added_margin
                )

        remaining_short_shares = np.maximum(-new_holdings, 0)
        remaining_short_liability = (
            remaining_short_shares.astype(np.float64) * prices
        )
        orphaned_collateral = (
            (remaining_short_shares == 0)
            & (
                sale_collateral_after + margin_collateral_after
                > 1.0e-12
            )
        )
        if np.any(orphaned_collateral):
            total_remaining_liability = float(
                np.sum(remaining_short_liability)
            )
            if total_remaining_liability <= 0.0:
                raise RuntimeError(
                    "short collateral remained after every short was covered"
                )
            allocation = (
                remaining_short_liability / total_remaining_liability
            )
            sale_collateral_after = (
                allocation * float(np.sum(sale_collateral_after))
            )
            margin_collateral_after = (
                allocation * float(np.sum(margin_collateral_after))
            )

        long_sell_net = (
            long_sell.astype(np.float64) * prices - long_sell_fees
        )
        long_buy_cost = (
            long_buy.astype(np.float64) * prices + long_buy_fees
        )
        cover_cost = cover.astype(np.float64) * prices + cover_fees
        settlement_net = float(
            np.sum(long_sell_net)
            - np.sum(long_buy_cost)
            + np.sum(released_sale)
            + np.sum(released_margin)
            - np.sum(cover_cost)
            + np.sum(short_sale_fee_deficit)
            - np.sum(added_margin)
        )
        return {
            "buys": buys,
            "sells": sells,
            "long_buy": long_buy,
            "long_sell": long_sell,
            "short_open": short_open,
            "short_cover": cover,
            "buy_notional": buy_notional,
            "sell_notional": sell_notional,
            "fees": float(
                np.sum(buy_fees)
                + np.sum(sell_fees)
                + np.sum(short_handling_fees)
            ),
            "commission_rebate": float(
                np.sum(buy_rebate) + np.sum(sell_rebate)
            ),
            "settlement_net": settlement_net,
            "sale_collateral": sale_collateral_after,
            "margin_collateral": margin_collateral_after,
        }

    free_settled_cash = _capacity_for_new_payable(cash, payable, receivable)
    tolerance = max(1e-9, abs(free_settled_cash) * 1e-12)

    def payable_for(details: dict[str, object]) -> float:
        combined_net = pending_net_claim + float(details["settlement_net"])
        return max(-combined_net, 0.0)

    reduced_target, opening_target = _separate_reductions_and_openings(
        base_holdings,
        proposed_holdings,
    )

    # Voluntary risk reductions are not all cash producers.  A deeply
    # underwater short cover can consume more settled buying power than its
    # released collateral.  Mandatory covers still bypass this gate and are
    # allowed to create a future T+2 default; voluntary consuming reductions
    # are scaled before any opposite-side opening is considered.
    base_long = np.maximum(base_holdings, 0)
    base_short = np.maximum(-base_holdings, 0)
    reduced_long = np.maximum(reduced_target, 0)
    reduced_short = np.maximum(-reduced_target, 0)
    voluntary_long_sell = np.maximum(base_long - reduced_long, 0)
    voluntary_short_cover = np.maximum(base_short - reduced_short, 0)

    mandatory_sell_notional = mandatory_sell.astype(np.float64) * prices
    full_sell_notional = (
        mandatory_sell + voluntary_long_sell
    ).astype(np.float64) * prices
    mandatory_sell_fees = _commission_fees_by_symbol(
        mandatory_sell_notional,
        commission_rates,
        minimum_commission=minimum_commission,
        rounding=commission_rounding,
    ) + _tax_fees_by_symbol(
        mandatory_sell_notional,
        sell_tax_rates,
        rounding=tax_rounding,
    )
    full_sell_fees = _commission_fees_by_symbol(
        full_sell_notional,
        commission_rates,
        minimum_commission=minimum_commission,
        rounding=commission_rounding,
    ) + _tax_fees_by_symbol(
        full_sell_notional,
        sell_tax_rates,
        rounding=tax_rounding,
    )
    voluntary_long_sell_net = (
        voluntary_long_sell.astype(np.float64) * prices
        - np.maximum(full_sell_fees - mandatory_sell_fees, 0.0)
    )

    mandatory_cover_notional = mandatory_cover.astype(np.float64) * prices
    full_cover_notional = (
        mandatory_cover + voluntary_short_cover
    ).astype(np.float64) * prices
    mandatory_cover_fees = _commission_fees_by_symbol(
        mandatory_cover_notional,
        commission_rates,
        minimum_commission=minimum_commission,
        rounding=commission_rounding,
    )
    full_cover_fees = _commission_fees_by_symbol(
        full_cover_notional,
        commission_rates,
        minimum_commission=minimum_commission,
        rounding=commission_rounding,
    )
    raw_release_per_share = np.divide(
        short_sale_collateral + short_margin_collateral,
        current_short,
        out=np.zeros_like(prices),
        where=current_short > 0,
    )
    voluntary_cover_net = (
        voluntary_short_cover.astype(np.float64)
        * (raw_release_per_share - prices)
        - np.maximum(full_cover_fees - mandatory_cover_fees, 0.0)
    )

    producer_long_sell = np.where(
        voluntary_long_sell_net >= -1.0e-12,
        voluntary_long_sell,
        0,
    ).astype(np.int64)
    producer_short_cover = np.where(
        voluntary_cover_net >= -1.0e-12,
        voluntary_short_cover,
        0,
    ).astype(np.int64)
    long_sale_producer_holdings = base_holdings - producer_long_sell
    producer_holdings = (
        long_sale_producer_holdings + producer_short_cover
    )
    consuming_reduction_delta = reduced_target - producer_holdings

    reduction_lot_sizes = np.where(
        consuming_reduction_delta < 0,
        long_lot_sizes,
        short_lot_sizes,
    )
    consuming_magnitude = np.abs(consuming_reduction_delta)
    if np.any(consuming_magnitude % reduction_lot_sizes != 0):
        raise RuntimeError(
            "consumer reduction delta is not aligned to its execution lot"
        )
    total_consumer_lots = np.floor_divide(
        consuming_magnitude,
        reduction_lot_sizes,
    ).astype(np.int64)

    # Funding along the producer-first ray is not necessarily prefix-feasible.
    # When maintenance locks collateral, an initially unaffordable cover can
    # become affordable only after enough additional liability is removed,
    # then become unaffordable again once proportional release binds.  Enumerate
    # every distinct lot endpoint from scale=1 downward and stop at the first
    # affordable endpoint.  Exact rational heap ordering merges simultaneous
    # symbol breakpoints without a floating-point scale comparison.
    candidate_holdings = reduced_target.copy()
    all_indices = np.arange(candidate_holdings.size, dtype=np.int64)

    def reduction_contributions(
        indices: np.ndarray,
    ) -> tuple[
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
    ]:
        candidate = candidate_holdings[indices]
        base_subset = base_holdings[indices]
        voluntary_delta = candidate - base_subset
        voluntary_buys = np.maximum(voluntary_delta, 0)
        voluntary_sells = np.maximum(-voluntary_delta, 0)
        base_long_subset = np.maximum(base_subset, 0)
        base_short_subset = np.maximum(-base_subset, 0)
        voluntary_cover_subset = np.minimum(
            voluntary_buys,
            base_short_subset,
        )
        voluntary_long_sell_subset = np.minimum(
            voluntary_sells,
            base_long_subset,
        )
        if np.any(voluntary_buys != voluntary_cover_subset) or np.any(
            voluntary_sells != voluntary_long_sell_subset
        ):
            raise RuntimeError(
                "funding-ray endpoint unexpectedly contains an opening order"
            )

        long_sell_quantity = (
            mandatory_sell[indices] + voluntary_long_sell_subset
        )
        cover_quantity = mandatory_cover[indices] + voluntary_cover_subset
        sell_notional = (
            long_sell_quantity.astype(np.float64) * prices[indices]
        )
        sell_fees = _commission_fees_by_symbol(
            sell_notional,
            commission_rates[indices],
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
        ) + _tax_fees_by_symbol(
            sell_notional,
            sell_tax_rates[indices],
            rounding=tax_rounding,
        )
        long_sell_net = sell_notional - sell_fees

        cover_notional = cover_quantity.astype(np.float64) * prices[indices]
        cover_fees = _commission_fees_by_symbol(
            cover_notional,
            commission_rates[indices],
            minimum_commission=minimum_commission,
            rounding=commission_rounding,
        )
        cover_cost = cover_notional + cover_fees

        old_short_subset = current_short[indices]
        cover_fraction = np.divide(
            cover_quantity,
            old_short_subset,
            out=np.zeros_like(cover_notional),
            where=old_short_subset > 0,
        )
        proportional_release = (
            short_sale_collateral[indices]
            + short_margin_collateral[indices]
        ) * cover_fraction
        remaining_short = np.maximum(
            old_short_subset - cover_quantity,
            0,
        )
        remaining_liability = (
            remaining_short.astype(np.float64) * prices[indices]
        )
        return (
            long_sell_net,
            cover_cost,
            proportional_release,
            remaining_liability,
            cover_quantity,
        )

    (
        long_sell_net_by_symbol,
        cover_cost_by_symbol,
        proportional_release_by_symbol,
        remaining_liability_by_symbol,
        cover_quantity_by_symbol,
    ) = reduction_contributions(all_indices)
    total_long_sell_net = float(np.sum(long_sell_net_by_symbol))
    total_cover_cost = float(np.sum(cover_cost_by_symbol))
    total_proportional_release = float(
        np.sum(proportional_release_by_symbol)
    )
    total_remaining_liability = float(
        np.sum(remaining_liability_by_symbol)
    )
    total_cover_quantity = int(np.sum(cover_quantity_by_symbol))

    def estimated_reduction_payable() -> float:
        if total_cover_quantity <= 0:
            released_collateral = 0.0
        elif total_remaining_liability <= 0.0:
            released_collateral = short_collateral_before_trade
        else:
            maximum_release = max(
                short_collateral_before_trade
                - short_maintenance_ratio * total_remaining_liability,
                0.0,
            )
            released_collateral = min(
                total_proportional_release,
                maximum_release,
            )
        settlement_net = (
            total_long_sell_net
            + released_collateral
            - total_cover_cost
        )
        return max(-(pending_net_claim + settlement_net), 0.0)

    selected_affordable_ray_endpoint = (
        estimated_reduction_payable()
        <= free_settled_cash + tolerance
    )
    breakpoint_heap = [
        _DescendingLotBreakpoint(
            remaining_lots=int(lot_count),
            total_lots=int(lot_count),
            symbol_index=int(symbol_index),
        )
        for symbol_index, lot_count in enumerate(total_consumer_lots.tolist())
        if int(lot_count) > 0
    ]
    heapq.heapify(breakpoint_heap)

    while not selected_affordable_ray_endpoint and breakpoint_heap:
        breakpoint = heapq.heappop(breakpoint_heap)
        tied_breakpoints = [breakpoint]
        while (
            breakpoint_heap
            and breakpoint.same_scale(breakpoint_heap[0])
        ):
            tied_breakpoints.append(heapq.heappop(breakpoint_heap))

        changed_indices = np.fromiter(
            (entry.symbol_index for entry in tied_breakpoints),
            dtype=np.int64,
            count=len(tied_breakpoints),
        )
        total_long_sell_net -= float(
            np.sum(long_sell_net_by_symbol[changed_indices])
        )
        total_cover_cost -= float(
            np.sum(cover_cost_by_symbol[changed_indices])
        )
        total_proportional_release -= float(
            np.sum(proportional_release_by_symbol[changed_indices])
        )
        total_remaining_liability -= float(
            np.sum(remaining_liability_by_symbol[changed_indices])
        )
        total_cover_quantity -= int(
            np.sum(cover_quantity_by_symbol[changed_indices])
        )

        candidate_holdings[changed_indices] -= (
            np.sign(consuming_reduction_delta[changed_indices]).astype(
                np.int64
            )
            * reduction_lot_sizes[changed_indices]
        )
        (
            next_long_sell_net,
            next_cover_cost,
            next_proportional_release,
            next_remaining_liability,
            next_cover_quantity,
        ) = reduction_contributions(changed_indices)
        long_sell_net_by_symbol[changed_indices] = next_long_sell_net
        cover_cost_by_symbol[changed_indices] = next_cover_cost
        proportional_release_by_symbol[
            changed_indices
        ] = next_proportional_release
        remaining_liability_by_symbol[
            changed_indices
        ] = next_remaining_liability
        cover_quantity_by_symbol[changed_indices] = next_cover_quantity

        total_long_sell_net += float(np.sum(next_long_sell_net))
        total_cover_cost += float(np.sum(next_cover_cost))
        total_proportional_release += float(
            np.sum(next_proportional_release)
        )
        total_remaining_liability += float(
            np.sum(next_remaining_liability)
        )
        total_cover_quantity += int(np.sum(next_cover_quantity))

        for entry in tied_breakpoints:
            next_remaining_lots = int(entry.remaining_lots) - 1
            if next_remaining_lots > 0:
                heapq.heappush(
                    breakpoint_heap,
                    _DescendingLotBreakpoint(
                        remaining_lots=next_remaining_lots,
                        total_lots=int(entry.total_lots),
                        symbol_index=int(entry.symbol_index),
                    ),
                )

        selected_affordable_ray_endpoint = (
            estimated_reduction_payable()
            <= free_settled_cash + tolerance
        )

    if selected_affordable_ray_endpoint:
        funded_reduced = candidate_holdings
    else:
        # The fixed raw-producer anchor can itself be unaffordable while no
        # consumer endpoint unlocks enough collateral.  Keep only voluntary
        # long-sale producers; mandatory exits remain gross and may still
        # create the explicitly modelled future T+2 default.
        funded_reduced = long_sale_producer_holdings
    funded_reduced_details = trade_details(funded_reduced)
    if (
        selected_affordable_ray_endpoint
        and payable_for(funded_reduced_details)
        > free_settled_cash + tolerance
    ):
        raise RuntimeError(
            "incremental reduction funding oracle selected an unaffordable "
            "lot endpoint"
        )

    # An opposite-side opening is physical only after that symbol reaches
    # zero.  Same-side additions remain eligible.  Recompute the gross budget
    # from the actually funded reduction endpoint rather than from a target
    # that the account could not execute.
    opening_holdings = opening_target.copy()
    opening_holdings[
        ((opening_holdings > 0) & (funded_reduced < 0))
        | ((opening_holdings < 0) & (funded_reduced > 0))
    ] = 0
    funded_reduced_gross = float(
        np.dot(np.abs(funded_reduced).astype(np.float64), prices)
    )
    opening_gross = float(
        np.dot(np.abs(opening_holdings).astype(np.float64), prices)
    )
    gross_open_budget = max(
        event_nav * gross_budget - funded_reduced_gross,
        0.0,
    )
    opening_scale = (
        min(1.0, gross_open_budget / opening_gross)
        if opening_gross > 0.0
        else 0.0
    )
    proposed_holdings = _scaled_opening_holdings(
        funded_reduced,
        opening_holdings,
        opening_scale,
        long_lot_sizes,
        short_lot_sizes,
    )
    details = trade_details(proposed_holdings)

    if (
        payable_for(details) > free_settled_cash + tolerance
        and np.any(opening_holdings)
    ):
        if (
            payable_for(funded_reduced_details)
            > free_settled_cash + tolerance
        ):
            proposed_holdings = funded_reduced
            details = funded_reduced_details
        else:
            low = 0.0
            high = opening_scale
            best_holdings = funded_reduced
            best_details = funded_reduced_details
            for _ in range(56):
                midpoint = (low + high) * 0.5
                candidate = _scaled_opening_holdings(
                    funded_reduced,
                    opening_holdings,
                    midpoint,
                    long_lot_sizes,
                    short_lot_sizes,
                )
                candidate_details = trade_details(candidate)
                if (
                    payable_for(candidate_details)
                    <= free_settled_cash + tolerance
                ):
                    low = midpoint
                    best_holdings = candidate
                    best_details = candidate_details
                else:
                    high = midpoint
            proposed_holdings = best_holdings
            details = best_details

    voluntary_quantity = np.abs(proposed_holdings - base_holdings)
    voluntary_notional = float(
        np.dot(voluntary_quantity.astype(np.float64), prices)
    )
    next_volume = (
        None
        if remaining_volume is None
        else np.maximum(
            remaining_volume - voluntary_quantity,
            0,
        ).astype(np.int64)
    )
    next_turnover = (
        None
        if remaining_turnover is None
        else max(remaining_turnover - voluntary_notional, 0.0)
    )
    actual_short_open = np.asarray(details["short_open"], dtype=np.int64)
    next_short_capacity = (
        None
        if remaining_short_capacity is None
        else np.maximum(
            remaining_short_capacity - actual_short_open,
            0,
        ).astype(np.int64)
    )
    sale_collateral_after = np.asarray(
        details["sale_collateral"],
        dtype=np.float64,
    )
    margin_collateral_after = np.asarray(
        details["margin_collateral"],
        dtype=np.float64,
    )
    sale_collateral_after[np.abs(sale_collateral_after) <= 1e-12] = 0.0
    margin_collateral_after[np.abs(margin_collateral_after) <= 1e-12] = 0.0
    return _IntegerEventResult(
        holdings=proposed_holdings,
        short_sale_collateral=sale_collateral_after,
        short_margin_collateral=margin_collateral_after,
        settlement_net=float(details["settlement_net"]),
        fee=float(details["fees"]),
        commission_rebate=float(details["commission_rebate"]),
        turnover_notional=float(
            np.sum(details["buy_notional"])
            + np.sum(details["sell_notional"])
        ),
        remaining_volume=next_volume,
        remaining_turnover=next_turnover,
        remaining_short_capacity=next_short_capacity,
        long_buy=np.asarray(details["long_buy"], dtype=np.int64),
        long_sell=np.asarray(details["long_sell"], dtype=np.int64),
        short_open=actual_short_open,
        short_cover=np.asarray(details["short_cover"], dtype=np.int64),
    )


def _run_dual_session_integer(
    *,
    mode: DualSessionIntegerMode,
    actions: object,
    open_prices: object,
    close_prices: object,
    tradable_mask: object,
    can_buy_mask: object,
    can_sell_mask: object,
    buy_fee_rates: object,
    sell_fee_rates: object,
    commission_rebate_rates: object,
    commission_rebate_timing: str,
    session_month_ids: object | None,
    commission_rebate_payment_eligible_mask: object | None,
    can_short_open_mask: object | None,
    force_short_cover_mask: object | None,
    short_margin_rate: object | None,
    short_capacity_shares: object | None,
    short_lot_sizes: object | None,
    short_handling_fee_rate: object,
    short_maintenance_ratio: float,
    unresolved_corporate_action_mask: object | None,
    cash_dividend_yield: object | None,
    cash_dividend_payment_delay_sessions: object | None,
    claim_queue_sessions: int | None,
    force_exit_mask: object | None,
    minimum_commission: float,
    commission_rounding: str,
    tax_rounding: str,
    lot_sizes: object | None,
    daily_volumes: object | None,
    max_volume_participation: float,
    max_turnover_ratio: float,
    gross_budget: float,
    state_advance_mask: object | None,
    settlement_lag_sessions: int,
    initial_cash: float,
    initial_state: TaiwanIntegerState | None,
    initial_commission_rebate_current: object | None,
    initial_commission_rebate_due: object | None,
    initial_commission_rebate_month_id: object | None,
) -> TaiwanDualSessionIntegerBacktestResult:
    action_values = _as_actions(actions, mode=mode)
    time, _, symbols = action_values.shape
    daily_shape = (time, symbols)
    phase_shape = (time, 2, symbols)
    raw_opens = _as_execution_price_matrix("open_prices", open_prices)
    raw_closes = _as_execution_price_matrix("close_prices", close_prices)
    if raw_opens.shape != daily_shape or raw_closes.shape != daily_shape:
        raise ValueError(
            "open_prices and close_prices must have shape [T,S] matching actions"
        )

    selection = _as_phase_bool(
        "tradable_mask",
        tradable_mask,
        phase_shape,
        default=False,
    )
    can_buy = _as_phase_bool(
        "can_buy_mask",
        can_buy_mask,
        phase_shape,
        default=False,
    )
    can_sell = _as_phase_bool(
        "can_sell_mask",
        can_sell_mask,
        phase_shape,
        default=False,
    )
    can_short_open = (
        _as_phase_bool(
            "can_short_open_mask",
            can_short_open_mask,
            phase_shape,
            default=False,
        )
        & can_sell
    )
    forced_cover = _as_phase_bool(
        "force_short_cover_mask",
        force_short_cover_mask,
        phase_shape,
        default=False,
    )
    if unresolved_corporate_action_mask is None:
        raise ValueError(
            f"{mode} exact integer execution requires an explicit official "
            "corporate-action avoidance mask"
        )
    unresolved_action = _as_phase_bool(
        "unresolved_corporate_action_mask",
        unresolved_corporate_action_mask,
        phase_shape,
        default=False,
    )
    terminal = _as_phase_bool(
        "force_exit_mask",
        force_exit_mask,
        phase_shape,
        default=False,
    )

    commission_rates, sell_tax_rates = _split_phase_fee_rates(
        buy_fee_rates,
        sell_fee_rates,
        phase_shape,
    )
    rebate_rates = _as_commission_rebate_rates(
        commission_rebate_rates,
        shape=phase_shape,
        gross_commission_rates=commission_rates,
    )
    min_commission = _as_minimum_commission(minimum_commission)
    commission_rounding_rule = normalize_fee_rounding(
        commission_rounding,
        name="commission_rounding",
    )
    tax_rounding_rule = normalize_fee_rounding(
        tax_rounding,
        name="tax_rounding",
    )
    long_lots = _as_lot_vector(lot_sizes, symbols, default=1)
    short_lots = _as_lot_vector(short_lot_sizes, symbols, default=1000)
    maintenance_ratio = _as_short_maintenance_ratio(short_maintenance_ratio)
    handling_rates = _as_phase_rate(
        "short_handling_fee_rate",
        short_handling_fee_rate,
        phase_shape,
    )

    requested_negative = (
        np.any(action_values < 0.0)
        if mode == "tw_cash"
        else np.any(action_values[:, 1:] < 0.0)
    )
    initial_has_short = (
        initial_state is not None
        and np.any(np.asarray(initial_state.holdings, dtype=np.int64) < 0)
    )
    signed_contract = bool(
        requested_negative
        or initial_has_short
        or np.any(forced_cover)
        or can_short_open_mask is not None
    )
    if requested_negative and can_short_open_mask is None:
        raise ValueError(
            f"negative {mode} entries require explicit two-phase "
            "can_short_open_mask"
        )
    if signed_contract and short_margin_rate is None:
        raise ValueError(
            f"signed {mode} requires explicit point-in-time short_margin_rate"
        )
    if signed_contract and short_capacity_shares is None:
        raise ValueError(
            f"signed {mode} requires explicit shared daily "
            "short_capacity_shares"
        )
    margin_rates = _as_phase_rate(
        "short_margin_rate",
        0.9 if short_margin_rate is None else short_margin_rate,
        phase_shape,
    )
    short_capacity = _as_nonnegative_matrix(
        "short_capacity_shares",
        short_capacity_shares,
        daily_shape,
    )

    volume_reference = _as_nonnegative_matrix(
        "daily_volumes",
        daily_volumes,
        daily_shape,
    )
    volume_ratio = _as_nonnegative_ratio(
        "max_volume_participation",
        max_volume_participation,
    )
    turnover_ratio = _as_nonnegative_ratio(
        "max_turnover_ratio",
        max_turnover_ratio,
    )
    gross_ratio = _as_nonnegative_ratio("gross_budget", gross_budget)
    if gross_ratio > 1.0:
        raise ValueError("gross_budget must be between 0 and 1 inclusive")
    if volume_ratio > 0.0 and volume_reference is None:
        raise ValueError(
            "daily_volumes is required when max_volume_participation is positive"
        )

    if (
        isinstance(settlement_lag_sessions, (bool, np.bool_))
        or not isinstance(settlement_lag_sessions, (int, np.integer))
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    lag = int(settlement_lag_sessions)
    dividend_yields, dividend_delays, claim_queue = _cash_dividend_inputs(
        daily_shape,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue_sessions,
    )
    advance = _as_advance_mask(state_advance_mask, time)
    rebate_timing, rebate_month_ids, rebate_payment_eligible = (
        _as_commission_rebate_calendar(
            time=time,
            timing=commission_rebate_timing,
            session_month_ids=session_month_ids,
            payment_eligible_mask=(
                commission_rebate_payment_eligible_mask
            ),
            state_advance_mask=advance,
        )
    )
    state = _copy_validate_state(
        initial_state,
        mode=mode,
        symbols=symbols,
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue,
        initial_cash=initial_cash,
        initial_commission_rebate_current=(
            initial_commission_rebate_current
        ),
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=(
            initial_commission_rebate_month_id
        ),
    )
    holdings = state.holdings.copy()
    positive = holdings > 0
    negative = holdings < 0
    if np.any(holdings[positive] % long_lots[positive] != 0):
        raise ValueError(
            "positive initial_state.holdings must align with lot_sizes"
        )
    if np.any((-holdings[negative]) % short_lots[negative] != 0):
        raise ValueError(
            "negative initial_state.holdings must align with short_lot_sizes"
        )
    if state.last_weights is not None:
        initial_weights = np.asarray(state.last_weights, dtype=np.float64)
        if np.any(initial_weights[holdings == 0] != 0.0) or np.any(
            initial_weights * holdings < 0.0
        ):
            raise ValueError(
                "dual-session initial_state.last_weights must align with "
                "signed holdings"
            )

    cash = float(state.settled_cash)
    payable = state.payable_queue.copy()
    receivable = state.receivable_queue.copy()
    rebate_current = float(state.commission_rebate_current)
    rebate_due = float(state.commission_rebate_due)
    rebate_month_id = int(state.commission_rebate_month_id)
    if rebate_timing == "daily_close" and (
        rebate_current != 0.0
        or rebate_due != 0.0
        or rebate_month_id != 0
    ):
        raise ValueError(
            "daily_close commission rebates cannot start with monthly pending "
            "rebate state"
        )
    short_sale_collateral = np.asarray(
        state.short_sale_collateral,
        dtype=np.float64,
    ).copy()
    short_margin_collateral = np.asarray(
        state.short_margin_collateral,
        dtype=np.float64,
    ).copy()
    last_nav = float(state.last_nav)
    alive = bool(state.alive)
    final_weights = (
        np.zeros(symbols, dtype=np.float64)
        if state.last_weights is None
        else np.asarray(state.last_weights, dtype=np.float64).copy()
    )
    carry_marks = _initial_carry_marks(state, symbols)

    arrays = _empty_result_arrays(time, symbols, lag, claim_queue)
    arrays.update(
        event_turnovers=np.zeros((time, 2), dtype=np.float64),
        event_turnover_notionals=np.zeros((time, 2), dtype=np.float64),
        turnover_denominator_history=np.zeros(time, dtype=np.float64),
        open_positions=np.zeros((time, symbols), dtype=np.int64),
        open_weights=np.zeros((time, symbols), dtype=np.float64),
        event_fee_history=np.zeros((time, 2), dtype=np.float64),
        event_commission_rebate_accrued=np.zeros(
            (time, 2),
            dtype=np.float64,
        ),
        executed_long_buy_shares=np.zeros(
            (time, 2, symbols),
            dtype=np.int64,
        ),
        executed_long_sell_shares=np.zeros(
            (time, 2, symbols),
            dtype=np.int64,
        ),
        executed_short_open_shares=np.zeros(
            (time, 2, symbols),
            dtype=np.int64,
        ),
        executed_short_cover_shares=np.zeros(
            (time, 2, symbols),
            dtype=np.int64,
        ),
        due_positions_history=np.zeros(
            (time, symbols),
            dtype=np.int64,
        ),
    )
    current_turnover_anchor = 0.0
    current_pending_claim = 0.0

    def record_default(index: int) -> None:
        nonlocal cash, holdings, last_nav, alive, final_weights
        nonlocal rebate_current, rebate_due, rebate_month_id
        alive = False
        cash = 0.0
        last_nav = 0.0
        holdings.fill(0)
        final_weights.fill(0.0)
        short_sale_collateral.fill(0.0)
        short_margin_collateral.fill(0.0)
        rebate_current = 0.0
        rebate_due = 0.0
        rebate_month_id = 0
        if current_turnover_anchor > 0.0:
            arrays["event_turnovers"][index] = (
                arrays["event_turnover_notionals"][index]
                / current_turnover_anchor
            )
            arrays["turnover"][index] = float(
                np.sum(arrays["event_turnover_notionals"][index])
            ) / current_turnover_anchor
        arrays["fee_history"][index] = float(
            np.sum(arrays["event_fee_history"][index])
        )
        arrays["commission_rebate_accrued_history"][index] = float(
            np.sum(arrays["event_commission_rebate_accrued"][index])
        )
        arrays["settlement_net_history"][index] = current_pending_claim
        _record_ruin(
            arrays,
            index,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
        )

    def apply_event_audit(
        index: int,
        phase: int,
        result: _IntegerEventResult,
    ) -> None:
        arrays["event_turnover_notionals"][
            index,
            phase,
        ] += result.turnover_notional
        arrays["event_fee_history"][index, phase] += result.fee
        arrays["event_commission_rebate_accrued"][
            index,
            phase,
        ] += result.commission_rebate
        arrays["executed_long_buy_shares"][index, phase] += result.long_buy
        arrays["executed_long_sell_shares"][index, phase] += result.long_sell
        arrays["executed_short_open_shares"][index, phase] += result.short_open
        arrays["executed_short_cover_shares"][index, phase] += result.short_cover

    for t in range(time):
        current_turnover_anchor = 0.0
        current_pending_claim = 0.0
        if not alive:
            _record_state(
                arrays,
                t,
                cash=0.0,
                holdings=holdings,
                weights=final_weights,
                payable=payable,
                receivable=receivable,
                nav=0.0,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
            )
            continue
        if not advance[t]:
            arrays["open_positions"][t] = holdings
            arrays["open_weights"][t] = final_weights
            if mode == "tw_overnight":
                arrays["due_positions_history"][t] = holdings
            _record_state(
                arrays,
                t,
                cash=cash,
                holdings=holdings,
                weights=final_weights,
                payable=payable,
                receivable=receivable,
                nav=last_nav,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                commission_rebate_current=rebate_current,
                commission_rebate_due=rebate_due,
            )
            continue

        cash, settlement_ok = _settle_start_of_session(
            cash,
            payable,
            receivable,
        )
        if not settlement_ok:
            record_default(t)
            continue

        nav_start = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=carry_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if nav_start <= 0.0 or not np.isfinite(nav_start):
            record_default(t)
            continue

        open_marks = _valuation_price_row(
            raw_opens,
            t,
            holdings=holdings,
            carry_marks=carry_marks,
            name="open_prices",
        )
        nav_open = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=open_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if nav_open <= 0.0 or not np.isfinite(nav_open):
            record_default(t)
            continue
        current_turnover_anchor = nav_open
        arrays["turnover_denominator_history"][t] = nav_open

        if volume_ratio > 0.0:
            assert volume_reference is not None
            remaining_volume = np.zeros(symbols, dtype=np.int64)
            valid_volume = np.isfinite(volume_reference[t])
            raw_capacity = np.floor(
                np.maximum(volume_reference[t, valid_volume], 0.0)
                * volume_ratio
                + 1e-12
            )
            remaining_volume[valid_volume] = np.minimum(
                raw_capacity,
                np.iinfo(np.int64).max,
            ).astype(np.int64)
        else:
            remaining_volume = None
        remaining_turnover = (
            None if turnover_ratio <= 0.0 else nav_open * turnover_ratio
        )
        if short_capacity is None:
            remaining_short_capacity = None
        else:
            remaining_short_capacity = np.zeros(symbols, dtype=np.int64)
            finite_capacity = np.isfinite(short_capacity[t])
            if np.any(finite_capacity):
                remaining_short_capacity[finite_capacity] = (
                    _round_quantity_down_to_lots(
                        short_capacity[t, finite_capacity],
                        short_lots[finite_capacity],
                    )
                )

        pending_net_claim = 0.0
        due = holdings.copy() if mode == "tw_overnight" else None
        new_cohort = np.zeros(symbols, dtype=np.int64)

        open_mandatory_exit = unresolved_action[t, OPEN] | terminal[t, OPEN]
        open_forced_cover = forced_cover[t, OPEN]
        current_long = np.maximum(holdings, 0)
        current_short = np.maximum(-holdings, 0)
        mandatory_long = np.where(
            open_mandatory_exit,
            current_long,
            0,
        ).astype(np.int64)
        mandatory_cover = np.where(
            open_mandatory_exit | open_forced_cover,
            current_short,
            0,
        ).astype(np.int64)

        if mode == "tw_cash":
            open_target_weights = np.where(
                selection[t, OPEN] & ~open_mandatory_exit,
                action_values[t, OPEN],
                0.0,
            )
            open_target_weights = np.where(
                open_forced_cover,
                np.maximum(open_target_weights, 0.0),
                open_target_weights,
            )
            desired_open = _target_holdings_from_weights(
                open_target_weights,
                nav=nav_open,
                prices=open_marks,
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
            )
        else:
            assert due is not None
            desired_open = _fractional_due_exit_target(
                due,
                action_values[t, OVERNIGHT_EXIT_AT_OPEN],
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
            )
            desired_open[open_mandatory_exit] = 0
            desired_open[open_forced_cover] = np.maximum(
                desired_open[open_forced_cover],
                0,
            )

        nav_before_event = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=open_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        open_event = _execute_signed_integer_event(
            holdings=holdings,
            desired_holdings=desired_open,
            mandatory_long_sell=mandatory_long,
            mandatory_short_cover=mandatory_cover,
            prices=open_marks,
            raw_price_matrix=raw_opens,
            time_index=t,
            price_name="open_prices",
            can_buy=can_buy[t, OPEN],
            can_sell=can_sell[t, OPEN],
            can_short_open=can_short_open[t, OPEN],
            commission_rates=commission_rates[t, OPEN],
            sell_tax_rates=sell_tax_rates[t, OPEN],
            minimum_commission=min_commission,
            commission_rounding=commission_rounding_rule,
            tax_rounding=tax_rounding_rule,
            long_lot_sizes=long_lots,
            short_lot_sizes=short_lots,
            short_margin_rates=margin_rates[t, OPEN],
            short_handling_rates=handling_rates[t, OPEN],
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            short_maintenance_ratio=maintenance_ratio,
            cash=cash,
            payable=payable,
            receivable=receivable,
            pending_net_claim=pending_net_claim,
            remaining_volume=remaining_volume,
            remaining_turnover=remaining_turnover,
            remaining_short_capacity=remaining_short_capacity,
            event_nav=nav_open,
            gross_budget=gross_ratio,
            commission_rebate_rates=rebate_rates[t, OPEN],
        )
        apply_event_audit(t, OPEN, open_event)
        if open_event.default_now:
            record_default(t)
            continue
        holdings = open_event.holdings
        short_sale_collateral = open_event.short_sale_collateral
        short_margin_collateral = open_event.short_margin_collateral
        pending_net_claim += open_event.settlement_net
        current_pending_claim = pending_net_claim
        remaining_volume = open_event.remaining_volume
        remaining_turnover = open_event.remaining_turnover
        remaining_short_capacity = open_event.remaining_short_capacity
        nav_after_event = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=open_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if not np.isclose(
            nav_after_event,
            nav_before_event - open_event.fee,
            rtol=2e-12,
            atol=1e-7,
        ):
            raise RuntimeError("dual-session OPEN ledger identity violated")

        if mode == "tw_overnight":
            assert due is not None
            due = holdings.copy()
            open_entry_weights = np.where(
                selection[t, OPEN] & ~open_mandatory_exit,
                action_values[t, OVERNIGHT_ENTRY_AT_OPEN],
                0.0,
            )
            open_entry_weights = np.where(
                open_forced_cover & (open_entry_weights < 0.0),
                0.0,
                open_entry_weights,
            )
            opposing_due = (
                (due != 0)
                & (open_entry_weights != 0.0)
                & ((due > 0) != (open_entry_weights > 0.0))
            )
            open_entry_weights[opposing_due] = 0.0
            entry_holdings = _target_holdings_from_weights(
                open_entry_weights,
                nav=nav_open,
                prices=open_marks,
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
            )
            desired_with_entry = holdings + entry_holdings
            zeros = np.zeros(symbols, dtype=np.int64)
            nav_before_event = _ledger_nav(
                cash=cash,
                holdings=holdings,
                prices=open_marks,
                payable=payable,
                receivable=receivable,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                pending_net_claim=pending_net_claim,
                commission_rebate_current=rebate_current,
                commission_rebate_due=rebate_due,
            )
            entry_event = _execute_signed_integer_event(
                holdings=holdings,
                desired_holdings=desired_with_entry,
                mandatory_long_sell=zeros,
                mandatory_short_cover=zeros,
                prices=open_marks,
                raw_price_matrix=raw_opens,
                time_index=t,
                price_name="open_prices",
                can_buy=can_buy[t, OPEN],
                can_sell=can_sell[t, OPEN],
                can_short_open=can_short_open[t, OPEN],
                commission_rates=commission_rates[t, OPEN],
                sell_tax_rates=sell_tax_rates[t, OPEN],
                minimum_commission=min_commission,
                commission_rounding=commission_rounding_rule,
                tax_rounding=tax_rounding_rule,
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
                short_margin_rates=margin_rates[t, OPEN],
                short_handling_rates=handling_rates[t, OPEN],
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                short_maintenance_ratio=maintenance_ratio,
                cash=cash,
                payable=payable,
                receivable=receivable,
                pending_net_claim=pending_net_claim,
                remaining_volume=remaining_volume,
                remaining_turnover=remaining_turnover,
                remaining_short_capacity=remaining_short_capacity,
                event_nav=nav_open,
                gross_budget=gross_ratio,
                commission_rebate_rates=rebate_rates[t, OPEN],
            )
            apply_event_audit(t, OPEN, entry_event)
            if entry_event.default_now:
                record_default(t)
                continue
            holdings = entry_event.holdings
            short_sale_collateral = entry_event.short_sale_collateral
            short_margin_collateral = entry_event.short_margin_collateral
            pending_net_claim += entry_event.settlement_net
            current_pending_claim = pending_net_claim
            remaining_volume = entry_event.remaining_volume
            remaining_turnover = entry_event.remaining_turnover
            remaining_short_capacity = entry_event.remaining_short_capacity
            new_cohort = holdings - due
            nav_after_event = _ledger_nav(
                cash=cash,
                holdings=holdings,
                prices=open_marks,
                payable=payable,
                receivable=receivable,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                pending_net_claim=pending_net_claim,
                commission_rebate_current=rebate_current,
                commission_rebate_due=rebate_due,
            )
            if not np.isclose(
                nav_after_event,
                nav_before_event - entry_event.fee,
                rtol=2e-12,
                atol=1e-7,
            ):
                raise RuntimeError(
                    "tw_overnight OPEN-entry ledger identity violated"
                )

        nav_after_open = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=open_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if nav_after_open <= 0.0 or not np.isfinite(nav_after_open):
            record_default(t)
            continue
        arrays["open_positions"][t] = holdings
        arrays["open_weights"][t] = (
            holdings.astype(np.float64) * open_marks / nav_after_open
        )

        close_marks = _valuation_price_row(
            raw_closes,
            t,
            holdings=holdings,
            carry_marks=open_marks,
            name="close_prices",
        )
        nav_close = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=close_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if nav_close <= 0.0 or not np.isfinite(nav_close):
            record_default(t)
            continue

        close_mandatory_exit = (
            unresolved_action[t, CLOSE] | terminal[t, CLOSE]
        )
        close_forced_cover = forced_cover[t, CLOSE]
        current_long = np.maximum(holdings, 0)
        current_short = np.maximum(-holdings, 0)
        if mode == "tw_overnight":
            assert due is not None
            due_long = np.maximum(due, 0)
            due_short = np.maximum(-due, 0)
        else:
            due_long = np.zeros(symbols, dtype=np.int64)
            due_short = np.zeros(symbols, dtype=np.int64)
        mandatory_long = np.maximum(
            due_long,
            np.where(close_mandatory_exit, current_long, 0),
        ).astype(np.int64)
        mandatory_cover = np.maximum(
            due_short,
            np.where(
                close_mandatory_exit | close_forced_cover,
                current_short,
                0,
            ),
        ).astype(np.int64)

        if mode == "tw_cash":
            close_target_weights = np.where(
                selection[t, CLOSE] & ~close_mandatory_exit,
                action_values[t, CLOSE],
                0.0,
            )
            close_target_weights = np.where(
                close_forced_cover,
                np.maximum(close_target_weights, 0.0),
                close_target_weights,
            )
            desired_close = _target_holdings_from_weights(
                close_target_weights,
                nav=nav_close,
                prices=close_marks,
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
            )
        else:
            close_entry_weights = np.where(
                selection[t, CLOSE] & ~close_mandatory_exit,
                action_values[t, OVERNIGHT_ENTRY_AT_CLOSE],
                0.0,
            )
            close_entry_weights = np.where(
                close_forced_cover & (close_entry_weights < 0.0),
                0.0,
                close_entry_weights,
            )
            close_entry_holdings = _target_holdings_from_weights(
                close_entry_weights,
                nav=nav_close,
                prices=close_marks,
                long_lot_sizes=long_lots,
                short_lot_sizes=short_lots,
            )
            desired_close = new_cohort + close_entry_holdings
            desired_close[close_mandatory_exit] = 0
            desired_close[close_forced_cover] = np.maximum(
                desired_close[close_forced_cover],
                0,
            )

        nav_before_event = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=close_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        close_event = _execute_signed_integer_event(
            holdings=holdings,
            desired_holdings=desired_close,
            mandatory_long_sell=mandatory_long,
            mandatory_short_cover=mandatory_cover,
            prices=close_marks,
            raw_price_matrix=raw_closes,
            time_index=t,
            price_name="close_prices",
            can_buy=can_buy[t, CLOSE],
            can_sell=can_sell[t, CLOSE],
            can_short_open=can_short_open[t, CLOSE],
            commission_rates=commission_rates[t, CLOSE],
            sell_tax_rates=sell_tax_rates[t, CLOSE],
            minimum_commission=min_commission,
            commission_rounding=commission_rounding_rule,
            tax_rounding=tax_rounding_rule,
            long_lot_sizes=long_lots,
            short_lot_sizes=short_lots,
            short_margin_rates=margin_rates[t, CLOSE],
            short_handling_rates=handling_rates[t, CLOSE],
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            short_maintenance_ratio=maintenance_ratio,
            cash=cash,
            payable=payable,
            receivable=receivable,
            pending_net_claim=pending_net_claim,
            remaining_volume=remaining_volume,
            remaining_turnover=remaining_turnover,
            remaining_short_capacity=remaining_short_capacity,
            event_nav=nav_close,
            gross_budget=gross_ratio,
            commission_rebate_rates=rebate_rates[t, CLOSE],
        )
        apply_event_audit(t, CLOSE, close_event)
        if close_event.default_now:
            record_default(t)
            continue
        holdings = close_event.holdings
        short_sale_collateral = close_event.short_sale_collateral
        short_margin_collateral = close_event.short_margin_collateral
        pending_net_claim += close_event.settlement_net
        current_pending_claim = pending_net_claim
        nav_after_close_event = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=close_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if not np.isclose(
            nav_after_close_event,
            nav_before_event - close_event.fee,
            rtol=2e-12,
            atol=1e-7,
        ):
            raise RuntimeError("dual-session CLOSE ledger identity violated")

        if mode == "tw_overnight":
            # Every share from yesterday's due cohort was a mandatory close
            # input. A blocked physical side already defaulted above.
            due = np.zeros(symbols, dtype=np.int64)

        _enqueue_net_claim(
            pending_net_claim,
            payable,
            receivable,
            settlement_lag_sessions=lag,
        )
        execution_nav = nav_after_close_event
        execution_payable = float(np.sum(payable))
        execution_receivable = float(np.sum(receivable))
        rebate_accrued = float(
            np.sum(arrays["event_commission_rebate_accrued"][t])
        )
        (
            cash,
            rebate_current,
            rebate_due,
            rebate_month_id,
            rebate_paid,
        ) = _apply_commission_rebate_at_close(
            earned=rebate_accrued,
            cash=cash,
            current=rebate_current,
            due=rebate_due,
            accrual_month_id=rebate_month_id,
            session_month_id=rebate_month_ids[t],
            payment_eligible=rebate_payment_eligible[t],
            timing=rebate_timing,
        )
        arrays["commission_rebate_accrued_history"][t] = rebate_accrued
        arrays["commission_rebate_paid_history"][t] = rebate_paid
        nav_after_rebate = _ledger_nav(
            cash=cash,
            holdings=holdings,
            prices=close_marks,
            payable=payable,
            receivable=receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if not np.isclose(
            nav_after_rebate,
            execution_nav + rebate_accrued,
            rtol=2e-12,
            atol=1e-7,
        ):
            raise RuntimeError("commission rebate ledger identity violated")
        dividend_claim_by_symbol = (
            np.maximum(holdings, 0).astype(np.float64)
            * close_marks
            * dividend_yields[t]
        )
        for delay in np.unique(
            dividend_delays[t][dividend_claim_by_symbol > 0.0]
        ):
            due_mask = dividend_delays[t] == delay
            receivable[int(delay) - 1] += float(
                np.sum(dividend_claim_by_symbol[due_mask])
            )
        nav = nav_after_rebate + float(np.sum(dividend_claim_by_symbol))
        if nav <= 0.0 or not np.isfinite(nav):
            record_default(t)
            continue

        log_return, simple_return = _log_and_simple_return(nav, last_nav)
        arrays["strategy_returns"][t] = log_return
        arrays["simple_returns"][t] = simple_return
        event_turnover_notional = arrays["event_turnover_notionals"][t]
        arrays["event_turnovers"][t] = event_turnover_notional / nav_open
        arrays["turnover"][t] = float(
            np.sum(event_turnover_notional)
        ) / nav_open
        arrays["settlement_net_history"][t] = pending_net_claim
        arrays["fee_history"][t] = float(
            np.sum(arrays["event_fee_history"][t])
        )
        arrays["commission_rebate_accrued_history"][t] = rebate_accrued
        arrays["commission_rebate_paid_history"][t] = rebate_paid
        close_weights = (
            holdings.astype(np.float64) * close_marks / execution_nav
        )
        final_weights = holdings.astype(np.float64) * close_marks / nav
        _record_state(
            arrays,
            t,
            cash=cash,
            holdings=holdings,
            weights=close_weights,
            payable=payable,
            receivable=receivable,
            nav=nav,
            execution_nav=execution_nav,
            execution_payable=execution_payable,
            execution_receivable=execution_receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            commission_rebate_current=rebate_current,
            commission_rebate_due=rebate_due,
        )
        if mode == "tw_overnight":
            arrays["due_positions_history"][t] = holdings
        last_nav = nav
        carry_marks = close_marks

    final_state = TaiwanIntegerState(
        mode=mode,
        settled_cash=float(cash),
        holdings=holdings.astype(np.int64, copy=True),
        payable_queue=payable.astype(np.float64, copy=True),
        receivable_queue=receivable.astype(np.float64, copy=True),
        last_nav=float(last_nav),
        alive=bool(alive),
        last_weights=final_weights.astype(np.float64, copy=True),
        short_sale_collateral=short_sale_collateral.astype(
            np.float64,
            copy=True,
        ),
        short_margin_collateral=short_margin_collateral.astype(
            np.float64,
            copy=True,
        ),
        commission_rebate_current=float(rebate_current),
        commission_rebate_due=float(rebate_due),
        commission_rebate_month_id=int(rebate_month_id),
    )
    long_buy = arrays["executed_long_buy_shares"]
    long_sell = arrays["executed_long_sell_shares"]
    short_open_history = arrays["executed_short_open_shares"]
    short_cover_history = arrays["executed_short_cover_shares"]
    phase_prices = np.stack((raw_opens, raw_closes), axis=1)
    safe_phase_prices = np.where(
        np.isfinite(phase_prices) & (phase_prices > 0.0),
        phase_prices,
        0.0,
    )
    turnover_denominators = arrays["turnover_denominator_history"][:, None, None]

    def execution_weights(shares: np.ndarray) -> np.ndarray:
        notionals = shares.astype(np.float64) * safe_phase_prices
        return np.divide(
            notionals,
            turnover_denominators,
            out=np.zeros_like(notionals),
            where=turnover_denominators > 0.0,
        )

    long_buy_weights = execution_weights(long_buy)
    long_sell_weights = execution_weights(long_sell)
    short_open_weights = execution_weights(short_open_history)
    short_cover_weights = execution_weights(short_cover_history)
    buy_weights = long_buy_weights + short_cover_weights
    sell_weights = long_sell_weights + short_open_weights
    reconstructed_event_turnovers = (buy_weights + sell_weights).sum(axis=2)
    if not np.allclose(
        arrays["event_turnovers"],
        reconstructed_event_turnovers,
        rtol=2e-12,
        atol=1e-12,
    ):
        raise RuntimeError(
            "dual-session event turnover does not equal exact executed "
            "buy/sell weights"
        )
    return TaiwanDualSessionIntegerBacktestResult(
        mode=mode,
        strategy_returns=arrays["strategy_returns"],
        simple_returns=arrays["simple_returns"],
        turnover=arrays["turnover"],
        event_turnovers=arrays["event_turnovers"],
        positions=arrays["positions"],
        open_positions=arrays["open_positions"],
        weights=arrays["weights"],
        open_weights=arrays["open_weights"],
        settled_cash_history=arrays["settled_cash_history"],
        payable_history=arrays["payable_history"],
        receivable_history=arrays["receivable_history"],
        payable_queue_history=arrays["payable_queue_history"],
        receivable_queue_history=arrays["receivable_queue_history"],
        execution_payable_history=arrays["execution_payable_history"],
        execution_receivable_history=arrays["execution_receivable_history"],
        execution_nav_history=arrays["execution_nav_history"],
        nav_history=arrays["nav_history"],
        settlement_net_history=arrays["settlement_net_history"],
        fee_history=arrays["fee_history"],
        event_fee_history=arrays["event_fee_history"],
        commission_rebate_accrued_history=arrays[
            "commission_rebate_accrued_history"
        ],
        commission_rebate_paid_history=arrays[
            "commission_rebate_paid_history"
        ],
        commission_rebate_current_history=arrays[
            "commission_rebate_current_history"
        ],
        commission_rebate_due_history=arrays[
            "commission_rebate_due_history"
        ],
        event_commission_rebate_accrued=arrays[
            "event_commission_rebate_accrued"
        ],
        default_mask=arrays["default_mask"],
        executed_buy_shares=long_buy + short_cover_history,
        executed_sell_shares=long_sell + short_open_history,
        executed_long_buy_shares=long_buy,
        executed_long_sell_shares=long_sell,
        executed_short_open_shares=short_open_history,
        executed_short_cover_shares=short_cover_history,
        executed_buy_weights=buy_weights,
        executed_sell_weights=sell_weights,
        executed_long_buy_weights=long_buy_weights,
        executed_long_sell_weights=long_sell_weights,
        executed_short_open_weights=short_open_weights,
        executed_short_cover_weights=short_cover_weights,
        short_sale_collateral_history=arrays[
            "short_sale_collateral_history"
        ],
        short_margin_collateral_history=arrays[
            "short_margin_collateral_history"
        ],
        due_positions_history=(
            arrays["due_positions_history"]
            if mode == "tw_overnight"
            else None
        ),
        final_due_positions=(
            holdings.astype(np.int64, copy=True)
            if mode == "tw_overnight"
            else None
        ),
        final_weights=final_weights.astype(np.float64, copy=True),
        final_state=final_state,
    )


def run_tw_cash_dual_session_integer(
    actions: object,
    open_prices: object,
    close_prices: object,
    tradable_mask: object,
    can_buy_mask: object,
    can_sell_mask: object,
    buy_fee_rates: object,
    sell_fee_rates: object,
    *,
    commission_rebate_rates: object = 0.0,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: object | None = None,
    commission_rebate_payment_eligible_mask: object | None = None,
    can_short_open_mask: object | None = None,
    force_short_cover_mask: object | None = None,
    short_margin_rate: object | None = None,
    short_capacity_shares: object | None = None,
    short_lot_sizes: object | None = None,
    short_handling_fee_rate: object = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: object | None = None,
    cash_dividend_yield: object | None = None,
    cash_dividend_payment_delay_sessions: object | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: object | None = None,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: object | None = None,
    daily_volumes: object | None = None,
    max_volume_participation: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_budget: float = 1.0,
    state_advance_mask: object | None = None,
    settlement_lag_sessions: int = 2,
    initial_cash: float = 1_000_000.0,
    initial_state: TaiwanIntegerState | None = None,
    initial_commission_rebate_current: object | None = None,
    initial_commission_rebate_due: object | None = None,
    initial_commission_rebate_month_id: object | None = None,
) -> TaiwanDualSessionIntegerBacktestResult:
    """Execute signed OPEN/CLOSE target weights with one daily T+2 event."""

    return _run_dual_session_integer(
        mode="tw_cash",
        actions=actions,
        open_prices=open_prices,
        close_prices=close_prices,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_shares=short_capacity_shares,
        short_lot_sizes=short_lot_sizes,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        minimum_commission=minimum_commission,
        commission_rounding=commission_rounding,
        tax_rounding=tax_rounding,
        lot_sizes=lot_sizes,
        daily_volumes=daily_volumes,
        max_volume_participation=max_volume_participation,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        state_advance_mask=state_advance_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        initial_cash=initial_cash,
        initial_state=initial_state,
        initial_commission_rebate_current=(
            initial_commission_rebate_current
        ),
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=(
            initial_commission_rebate_month_id
        ),
    )


def run_tw_overnight_dual_session_integer(
    actions: object,
    open_prices: object,
    close_prices: object,
    tradable_mask: object,
    can_buy_mask: object,
    can_sell_mask: object,
    buy_fee_rates: object,
    sell_fee_rates: object,
    *,
    commission_rebate_rates: object = 0.0,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: object | None = None,
    commission_rebate_payment_eligible_mask: object | None = None,
    can_short_open_mask: object | None = None,
    force_short_cover_mask: object | None = None,
    short_margin_rate: object | None = None,
    short_capacity_shares: object | None = None,
    short_lot_sizes: object | None = None,
    short_handling_fee_rate: object = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: object | None = None,
    cash_dividend_yield: object | None = None,
    cash_dividend_payment_delay_sessions: object | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: object | None = None,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: object | None = None,
    daily_volumes: object | None = None,
    max_volume_participation: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_budget: float = 1.0,
    state_advance_mask: object | None = None,
    settlement_lag_sessions: int = 2,
    initial_cash: float = 1_000_000.0,
    initial_state: TaiwanIntegerState | None = None,
    initial_commission_rebate_current: object | None = None,
    initial_commission_rebate_due: object | None = None,
    initial_commission_rebate_month_id: object | None = None,
) -> TaiwanDualSessionIntegerBacktestResult:
    """Execute strict one-session cohorts with exact OPEN/CLOSE prices."""

    return _run_dual_session_integer(
        mode="tw_overnight",
        actions=actions,
        open_prices=open_prices,
        close_prices=close_prices,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_shares=short_capacity_shares,
        short_lot_sizes=short_lot_sizes,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        minimum_commission=minimum_commission,
        commission_rounding=commission_rounding,
        tax_rounding=tax_rounding,
        lot_sizes=lot_sizes,
        daily_volumes=daily_volumes,
        max_volume_participation=max_volume_participation,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        state_advance_mask=state_advance_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        initial_cash=initial_cash,
        initial_state=initial_state,
        initial_commission_rebate_current=(
            initial_commission_rebate_current
        ),
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=(
            initial_commission_rebate_month_id
        ),
    )
