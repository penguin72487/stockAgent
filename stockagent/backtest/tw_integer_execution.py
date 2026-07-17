"""Exact, CPU reference ledgers for Taiwan cash and day-trade execution.

The canonical tensor simulator is optimized for training.  This module instead
provides a deliberately small NumPy oracle for validating discrete execution:

* positions are integer shares and every order is rounded down to its lot size;
* settlement is delayed by *exchange sessions*, not calendar days;
* each trade date creates one account-level net balance after buys and sells are
  offset, matching Taiwan's cash-stock settlement rule and day-trade difference
  settlement;
* a payable due before the session must be funded by already-settled cash -- a
  receivable due later in the same session cannot cure that settlement default;
* the economic value of a claim is recognized on trade date, while its later
  conversion to/from cash has zero NAV impact.

Both executors are O(T*S): time is inherently recurrent, while all symbols in a
session are processed by NumPy vector operations.  They are reference/oracle
implementations, not differentiable training kernels.
"""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Real
from typing import Literal

import numpy as np

from stockagent.backtest.tw_execution import normalize_fee_rounding


ExecutionMode = Literal["tw_cash", "tw_day_trade"]


@dataclass(frozen=True, slots=True)
class TaiwanIntegerState:
    """Chunk-continuation state for the exact Taiwan settlement ledger.

    Queue index zero is payable/receivable at the *next active session's*
    pre-trade settlement event.  ``last_nav`` is the preceding active session's
    closing ledger value and therefore makes chunked return calculation exact.
    """

    mode: ExecutionMode
    settled_cash: float
    holdings: np.ndarray
    payable_queue: np.ndarray
    receivable_queue: np.ndarray
    last_nav: float
    alive: bool = True
    # Drifted allocation aligned with ``last_nav``.  It is needed to make an
    # inactive/padding row a true identity operation when a chunk starts with
    # an existing holding and therefore has no new quote to revalue it.  None
    # remains accepted for old callers/artifacts; an active row can recover it,
    # but leading padding with nonzero holdings then fails closed.
    last_weights: np.ndarray | None = None
    # A Taiwan margin short is not a naked negative cash position.  The net
    # short-sale proceeds and the investor's margin deposit are segregated
    # assets until the corresponding shares are bought back.  Optional values
    # preserve construction compatibility with pre-margin-short states.
    short_sale_collateral: np.ndarray | None = None
    short_margin_collateral: np.ndarray | None = None


@dataclass(frozen=True, slots=True)
class TaiwanIntegerBacktestResult:
    """Audit-complete output from an integer Taiwan execution oracle."""

    # ``strategy_returns`` is log return for compatibility with the project's
    # canonical return convention.  The default event is represented by -inf.
    strategy_returns: np.ndarray
    simple_returns: np.ndarray
    turnover: np.ndarray
    positions: np.ndarray
    weights: np.ndarray
    settled_cash_history: np.ndarray
    payable_history: np.ndarray
    receivable_history: np.ndarray
    payable_queue_history: np.ndarray
    receivable_queue_history: np.ndarray
    # Queue totals at the execution/mark boundary.  End-of-row queue history
    # may additionally contain a cash-dividend entitlement earned after that
    # boundary, so holdings reports must not mix the two accounting times.
    execution_payable_history: np.ndarray
    execution_receivable_history: np.ndarray
    # NAV at the row's execution/mark boundary, before the supplied forward
    # valuation return.  This keeps exact holdings reports reconstructable even
    # when a halted symbol has no raw execution quote on that row.
    execution_nav_history: np.ndarray
    nav_history: np.ndarray
    settlement_net_history: np.ndarray
    fee_history: np.ndarray
    default_mask: np.ndarray
    # Per-symbol restricted assets carried by Taiwan margin shorts.  These are
    # zero for cash-long-only and day-trade executions.
    short_sale_collateral_history: np.ndarray
    short_margin_collateral_history: np.ndarray
    # Drifted end-of-horizon allocation, aligned with final_state.last_nav.
    # ``weights`` above remains the execution-time audit history.
    final_weights: np.ndarray
    final_state: TaiwanIntegerState


def _as_matrix(name: str, value: object) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.ndim != 2:
        raise ValueError(f"{name} must have shape [time, symbols]")
    if not np.all(np.isfinite(result)):
        raise ValueError(f"{name} must contain only finite values")
    return result


def _as_execution_price_matrix(name: str, value: object) -> np.ndarray:
    """Coerce prices while deferring validity checks to their use sites.

    A full-universe panel legitimately has no quote before listing or after a
    completed terminal exit.  Such cells are harmless only while the symbol is
    inactive and unheld; the recurrent executor therefore cannot decide price
    validity globally before it knows the current holdings and lifecycle masks.
    """

    result = np.asarray(value, dtype=np.float64)
    if result.ndim != 2:
        raise ValueError(f"{name} must have shape [time, symbols]")
    return result


def _safe_execution_price_row(
    name: str,
    prices: np.ndarray,
    index: int,
    required_mask: np.ndarray,
) -> np.ndarray:
    """Return a finite row, rejecting every invalid price that affects state.

    Invalid inactive cells receive an internal unit sentinel solely so NumPy
    vector operations cannot turn masked ``0 * NaN`` terms into NaNs.  The
    sentinel is never executable or observable: any held, tradable, or terminal
    symbol is included in ``required_mask`` and fails closed instead.
    """

    row = prices[index]
    valid = np.isfinite(row) & (row > 0.0)
    invalid_required = np.flatnonzero(np.asarray(required_mask, dtype=np.bool_) & ~valid)
    if invalid_required.size:
        preview = invalid_required[:8].tolist()
        suffix = "" if invalid_required.size <= 8 else "..."
        raise ValueError(
            f"{name} has non-finite or non-positive required prices at "
            f"time index {index}, symbol indices {preview}{suffix}"
        )
    if bool(valid.all()):
        return row
    safe = row.copy()
    safe[~valid] = 1.0
    return safe


def _cash_valuation_price_row(
    prices: np.ndarray,
    index: int,
    *,
    holdings: np.ndarray,
    last_nav: float,
    last_weights: np.ndarray | None,
) -> np.ndarray:
    """Return current raw marks, carrying only already-held missing quotes.

    ``last_weights`` is the drifted allocation at this exact valuation
    boundary.  Therefore ``weight * NAV / shares`` reconstructs the prior
    official close used to mark a position through a trading halt without
    storing another dense price panel in recurrent state.  Unheld invalid cells
    retain a private unit sentinel and can never become an execution price.
    """

    row = prices[index]
    raw_valid = np.isfinite(row) & (row > 0.0)
    if bool(raw_valid.all()):
        return row
    valuation = row.copy()
    valuation[~raw_valid] = 1.0
    held_without_quote = (holdings != 0) & ~raw_valid
    if not np.any(held_without_quote):
        return valuation
    if last_weights is None:
        raise ValueError(
            "tw_cash cannot value an existing holding without a raw close or "
            "initial_state.last_weights"
        )
    reconstructed = (
        np.asarray(last_weights[held_without_quote], dtype=np.float64)
        * float(last_nav)
        / holdings[held_without_quote].astype(np.float64)
    )
    if not np.all(np.isfinite(reconstructed)) or np.any(reconstructed <= 0.0):
        invalid_symbols = np.flatnonzero(held_without_quote)[
            ~np.isfinite(reconstructed) | (reconstructed <= 0.0)
        ]
        raise ValueError(
            "tw_cash carried valuation is non-finite/non-positive for held "
            f"symbols at time index {index}: {invalid_symbols.tolist()}"
        )
    valuation[held_without_quote] = reconstructed
    return valuation


def _as_rate_vector(name: str, value: object, symbols: int) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.ndim == 0:
        result = np.full(symbols, float(result), dtype=np.float64)
    if result.shape != (symbols,):
        raise ValueError(f"{name} must be scalar or have shape [{symbols}]")
    if not np.all(np.isfinite(result)) or np.any(result < 0.0):
        raise ValueError(f"{name} must contain finite non-negative rates")
    return result


def _as_time_symbol_rate(
    name: str,
    value: object,
    shape: tuple[int, int],
) -> np.ndarray:
    """Normalize a scalar, symbol vector, or point-in-time rate matrix.

    Scalar and ``[S]`` inputs retain the historical API and are exposed as
    broadcast views.  ``[T,S]`` permits statutory or security-specific rates
    to change by session without splitting the accounting horizon.
    """

    result = np.asarray(value, dtype=np.float64)
    if result.ndim == 0:
        result = np.broadcast_to(result, shape)
    elif result.shape == (shape[1],):
        result = np.broadcast_to(result.reshape(1, -1), shape)
    elif result.shape != shape:
        raise ValueError(
            f"{name} must be scalar or have shape [{shape[1]}] or "
            f"[{shape[0]},{shape[1]}]"
        )
    if not np.all(np.isfinite(result)) or np.any(result < 0.0):
        raise ValueError(f"{name} must contain finite non-negative rates")
    return result


def _as_minimum_commission(value: object) -> float:
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        raise ValueError("minimum_commission must be finite and non-negative")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("minimum_commission must be finite and non-negative") from exc
    if not np.isfinite(result) or result < 0.0:
        raise ValueError("minimum_commission must be finite and non-negative")
    return result


def _split_commission_and_sell_tax_rates(
    buy_fee_rates: object,
    sell_fee_rates: object,
    symbols: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return symmetric commission and sell-only tax rate vectors.

    The public schedule keeps backward-compatible all-in fee vectors:
    ``buy_fee_rates`` is discounted commission and ``sell_fee_rates`` is the
    same commission plus product tax.  Splitting them here lets the exact
    integer ledger apply distinct currency rounding to commission and tax.
    """

    commission_rates = _as_rate_vector("buy_fee_rates", buy_fee_rates, symbols)
    sell_all_in_rates = _as_rate_vector("sell_fee_rates", sell_fee_rates, symbols)
    sell_tax_rates = sell_all_in_rates - commission_rates
    tolerance = np.maximum(1.0, np.abs(sell_all_in_rates)) * 1e-15
    if np.any(sell_tax_rates < -tolerance):
        raise ValueError(
            "sell_fee_rates must be at least buy_fee_rates because the exact "
            "Taiwan fee profile assumes symmetric commission plus sell-only tax"
        )
    return commission_rates, np.maximum(sell_tax_rates, 0.0)


def _round_currency_fees(values: np.ndarray, rule: str) -> np.ndarray:
    """Round non-negative fee amounts to whole TWD under an explicit rule."""

    if rule == "none":
        return values
    if rule == "floor":
        return np.floor(values)
    if rule == "half_up":
        # Fee amounts are non-negative, so floor(x + 0.5) is decimal half-up.
        return np.floor(values + 0.5)
    raise RuntimeError(f"unreachable fee rounding rule: {rule!r}")


def _round_short_margin_up_to_hundreds(
    values: np.ndarray,
    active_orders: np.ndarray,
) -> np.ndarray:
    """Round each symbol's new Taiwan short margin up to TWD 100.

    The statutory initial margin is assessed per symbol/order rather than on
    an account-level aggregate.  Snap only machine-noise-close integer
    quotients before ``ceil`` so an economically exact TWD-100 boundary cannot
    become TWD 200 because of binary floating multiplication, while a genuine
    amount above the boundary still rounds upward.
    """

    raw = np.asarray(values, dtype=np.float64)
    active = np.asarray(active_orders, dtype=np.bool_)
    quotients = raw / 100.0
    nearest = np.rint(quotients)
    tolerance = (
        8.0
        * np.finfo(np.float64).eps
        * np.maximum(1.0, np.abs(quotients))
    )
    stable_quotients = np.where(
        np.abs(quotients - nearest) <= tolerance,
        nearest,
        quotients,
    )
    return np.where(active, np.ceil(stable_quotients) * 100.0, 0.0)


def _commission_fees_by_symbol(
    notionals: np.ndarray,
    commission_rates: np.ndarray,
    *,
    minimum_commission: float,
    rounding: str,
) -> np.ndarray:
    """Charge commission once per nonzero symbol-side aggregate order."""

    notionals = np.asarray(notionals, dtype=np.float64)
    proportional = _round_currency_fees(notionals * commission_rates, rounding)
    return np.where(
        notionals > 0.0,
        np.maximum(proportional, minimum_commission),
        0.0,
    )


def _tax_fees_by_symbol(
    sell_notionals: np.ndarray,
    sell_tax_rates: np.ndarray,
    *,
    rounding: str,
) -> np.ndarray:
    return _round_currency_fees(
        np.asarray(sell_notionals, dtype=np.float64) * sell_tax_rates,
        rounding,
    )


def _buy_costs_by_symbol(
    quantities: np.ndarray,
    prices: np.ndarray,
    commission_rates: np.ndarray,
    *,
    minimum_commission: float,
    commission_rounding: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    notionals = np.asarray(quantities, dtype=np.float64) * prices
    commissions = _commission_fees_by_symbol(
        notionals,
        commission_rates,
        minimum_commission=minimum_commission,
        rounding=commission_rounding,
    )
    return notionals, commissions, notionals + commissions


def _scale_lot_buys_to_budget_with_fixed_fees(
    desired_buys: np.ndarray,
    prices: np.ndarray,
    lot_sizes: np.ndarray,
    commission_rates: np.ndarray,
    *,
    budget: float,
    minimum_commission: float,
    commission_rounding: str,
) -> np.ndarray:
    """Find a conservative scalar lot allocation under nonlinear buy fees.

    Minimum commissions and whole-TWD rounding make buy cost discontinuous.
    Two vectorized candidates are sufficient: first scale by the full desired
    cost; if fixed fees leave that candidate over budget, reserve its fees and
    rescale the original desired notional.  Fee monotonicity guarantees the
    second candidate is affordable because it can only have smaller notionals
    and fees.  This is constant-pass O(S), matching the per-session lower bound.
    """

    desired = np.asarray(desired_buys, dtype=np.int64)
    budget = max(float(budget), 0.0)

    def candidate(scale: float) -> tuple[np.ndarray, float, float]:
        quantities = _round_quantity_down_to_lots(
            desired.astype(np.float64) * scale,
            lot_sizes,
        )
        notionals, commissions, costs = _buy_costs_by_symbol(
            quantities,
            prices,
            commission_rates,
            minimum_commission=minimum_commission,
            commission_rounding=commission_rounding,
        )
        return quantities, float(np.sum(costs)), float(np.sum(commissions))

    full, full_cost, _ = candidate(1.0)
    tolerance = max(1e-9, abs(budget) * 1e-12)
    if full_cost <= budget + tolerance:
        return full

    first, first_cost, first_fees = candidate(budget / full_cost)
    if first_cost <= budget + tolerance:
        return first

    desired_notional = float(np.dot(desired.astype(np.float64), prices))
    second_scale = (
        max(budget - first_fees, 0.0) / desired_notional
        if desired_notional > 0.0
        else 0.0
    )
    second, second_cost, _ = candidate(second_scale)
    if second_cost > budget + tolerance:
        raise RuntimeError("internal fixed-fee affordability invariant violated")
    return second


def _as_lot_vector(value: object, symbols: int, *, default: int) -> np.ndarray:
    raw = np.asarray(default if value is None else value)
    if raw.ndim == 0:
        raw = np.full(symbols, raw.item())
    if raw.shape != (symbols,):
        raise ValueError(f"lot_sizes must be scalar or have shape [{symbols}]")
    if not np.issubdtype(raw.dtype, np.integer):
        numeric = np.asarray(raw, dtype=np.float64)
        if not np.all(np.isfinite(numeric)) or not np.all(numeric == np.floor(numeric)):
            raise ValueError("lot_sizes must contain positive integers")
        raw = numeric.astype(np.int64)
    else:
        raw = raw.astype(np.int64, copy=False)
    if np.any(raw <= 0):
        raise ValueError("lot_sizes must contain positive integers")
    return raw


def _as_bool_matrix(name: str, value: object | None, shape: tuple[int, int]) -> np.ndarray:
    if value is None:
        return np.ones(shape, dtype=np.bool_)
    result = np.asarray(value)
    if result.shape != shape:
        raise ValueError(f"{name} must have shape {shape}")
    if result.dtype != np.bool_:
        raise ValueError(f"{name} must contain booleans")
    return result.astype(np.bool_, copy=False)


def _as_nonnegative_matrix(
    name: str,
    value: object | None,
    shape: tuple[int, int],
) -> np.ndarray | None:
    if value is None:
        return None
    result = np.asarray(value, dtype=np.float64)
    if result.shape != shape:
        raise ValueError(f"{name} must have shape {shape}")
    if np.any(np.isinf(result)) or np.any(result < 0.0):
        raise ValueError(f"{name} must contain non-negative finite values or NaN")
    return result


def _as_nonnegative_ratio(name: str, value: object) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite and non-negative") from exc
    if not np.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be finite and non-negative")
    return result


def _as_short_maintenance_ratio(value: object) -> float:
    """Validate the account-level short collateral maintenance threshold."""

    raw = np.asarray(value)
    if raw.ndim != 0 or np.issubdtype(raw.dtype, np.bool_):
        raise ValueError(
            "short_maintenance_ratio must be a finite scalar at least 1.0"
        )
    result = _as_nonnegative_ratio("short_maintenance_ratio", value)
    if result < 1.0:
        raise ValueError(
            "short_maintenance_ratio must be a finite scalar at least 1.0"
        )
    return result


def _round_quantity_down_to_lots(
    quantities: np.ndarray,
    lot_sizes: np.ndarray,
) -> np.ndarray:
    """Floor non-negative share quantities to symbol-specific whole lots."""

    numeric = np.asarray(quantities, dtype=np.float64)
    if not np.all(np.isfinite(numeric)) or np.any(numeric < 0.0):
        raise ValueError("share quantities must be finite and non-negative")
    lot_counts = np.floor(numeric / lot_sizes + 1e-12)
    max_lot_counts = np.floor_divide(np.iinfo(np.int64).max, lot_sizes)
    if np.any(lot_counts > max_lot_counts):
        raise OverflowError("share quantity exceeds int64 lot storage")
    return lot_counts.astype(np.int64) * lot_sizes


def _scale_signed_lot_orders(
    deltas: np.ndarray,
    scale: float,
    lot_sizes: np.ndarray,
) -> np.ndarray:
    """Scale signed orders without ever increasing their absolute quantity."""

    magnitude = _round_quantity_down_to_lots(
        np.abs(deltas).astype(np.float64) * max(0.0, min(1.0, float(scale))),
        lot_sizes,
    )
    return np.sign(deltas).astype(np.int64) * magnitude


def _limit_signed_transition_by_quantity(
    base_holdings: np.ndarray,
    target_holdings: np.ndarray,
    max_quantities: np.ndarray,
    long_lot_sizes: np.ndarray,
    short_lot_sizes: np.ndarray,
) -> np.ndarray:
    """Limit a signed holding transition while preserving both lot regimes.

    A long-to-short (or short-to-long) order has two economically distinct
    legs.  The closing leg uses the existing position's lot regime and only
    the residual capacity may open the opposite position.  This routine is
    O(S), which is the lower bound for heterogeneous per-symbol lot sizes.
    """

    base = np.asarray(base_holdings, dtype=np.int64)
    target = np.asarray(target_holdings, dtype=np.int64)
    limits = np.asarray(max_quantities, dtype=np.int64)
    result = base.copy()
    for symbol in range(base.size):
        old = int(base[symbol])
        desired = int(target[symbol])
        remaining = max(int(limits[symbol]), 0)
        if desired == old or remaining == 0:
            continue
        if desired > old:
            if old < 0:
                cover_wanted = min(-old, desired - old)
                cover = min(cover_wanted, remaining)
                cover = (cover // int(short_lot_sizes[symbol])) * int(
                    short_lot_sizes[symbol]
                )
                result[symbol] += cover
                remaining -= cover
            if result[symbol] == 0 and desired > 0 and remaining > 0:
                buy = min(desired, remaining)
                buy = (buy // int(long_lot_sizes[symbol])) * int(
                    long_lot_sizes[symbol]
                )
                result[symbol] = buy
            elif result[symbol] >= 0:
                buy = min(desired - int(result[symbol]), remaining)
                buy = (buy // int(long_lot_sizes[symbol])) * int(
                    long_lot_sizes[symbol]
                )
                result[symbol] += buy
        else:
            if old > 0:
                sell_wanted = min(old, old - desired)
                sell = min(sell_wanted, remaining)
                sell = (sell // int(long_lot_sizes[symbol])) * int(
                    long_lot_sizes[symbol]
                )
                result[symbol] -= sell
                remaining -= sell
            if result[symbol] == 0 and desired < 0 and remaining > 0:
                short = min(-desired, remaining)
                short = (short // int(short_lot_sizes[symbol])) * int(
                    short_lot_sizes[symbol]
                )
                result[symbol] = -short
            elif result[symbol] <= 0:
                short = min(int(result[symbol]) - desired, remaining)
                short = (short // int(short_lot_sizes[symbol])) * int(
                    short_lot_sizes[symbol]
                )
                result[symbol] -= short
    return result


def _separate_reductions_and_openings(
    old_holdings: np.ndarray,
    proposed_holdings: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Split a signed transition into unavoidable reductions and new risk."""

    old = np.asarray(old_holdings, dtype=np.int64)
    proposed = np.asarray(proposed_holdings, dtype=np.int64)
    reduced = proposed.copy()
    opening = np.zeros_like(proposed)

    add_long = (old >= 0) & (proposed > old)
    add_short = (old <= 0) & (proposed < old)
    cross_to_long = (old < 0) & (proposed > 0)
    cross_to_short = (old > 0) & (proposed < 0)
    opening[add_long] = proposed[add_long] - old[add_long]
    reduced[add_long] = old[add_long]
    opening[add_short] = proposed[add_short] - old[add_short]
    reduced[add_short] = old[add_short]
    opening[cross_to_long | cross_to_short] = proposed[
        cross_to_long | cross_to_short
    ]
    reduced[cross_to_long | cross_to_short] = 0
    return reduced, opening


def _scaled_opening_holdings(
    reduced_holdings: np.ndarray,
    opening_holdings: np.ndarray,
    scale: float,
    long_lot_sizes: np.ndarray,
    short_lot_sizes: np.ndarray,
) -> np.ndarray:
    result = np.asarray(reduced_holdings, dtype=np.int64).copy()
    opening = np.asarray(opening_holdings, dtype=np.int64)
    clipped_scale = max(0.0, min(1.0, float(scale)))
    positive = opening > 0
    negative = opening < 0
    if np.any(positive):
        result[positive] += _round_quantity_down_to_lots(
            opening[positive].astype(np.float64) * clipped_scale,
            long_lot_sizes[positive],
        )
    if np.any(negative):
        result[negative] -= _round_quantity_down_to_lots(
            (-opening[negative]).astype(np.float64) * clipped_scale,
            short_lot_sizes[negative],
        )
    return result


def _as_advance_mask(value: object | None, time: int) -> np.ndarray:
    if value is None:
        return np.ones(time, dtype=np.bool_)
    result = np.asarray(value)
    if result.shape != (time,):
        raise ValueError(f"state_advance_mask must have shape [{time}]")
    if result.dtype != np.bool_:
        raise ValueError("state_advance_mask must contain booleans")
    return result.astype(np.bool_, copy=False)


def _cash_dividend_inputs(
    shape: tuple[int, int],
    *,
    cash_dividend_yield: object | None,
    cash_dividend_payment_delay_sessions: object | None,
    settlement_lag_sessions: int,
    claim_queue_sessions: int | None,
) -> tuple[np.ndarray, np.ndarray, int]:
    """Validate exact cash-entitlement terms without inventing missing dates."""

    has_yield = cash_dividend_yield is not None
    if has_yield != (cash_dividend_payment_delay_sessions is not None):
        raise ValueError(
            "cash_dividend_yield and cash_dividend_payment_delay_sessions "
            "must be supplied together"
        )
    if claim_queue_sessions is None:
        queue_sessions = int(settlement_lag_sessions)
    else:
        if isinstance(claim_queue_sessions, (bool, np.bool_)) or not isinstance(
            claim_queue_sessions, (int, np.integer)
        ):
            raise ValueError(
                "claim_queue_sessions must be an integer at least as large as "
                "settlement_lag_sessions"
            )
        queue_sessions = int(claim_queue_sessions)
    if queue_sessions < int(settlement_lag_sessions):
        raise ValueError(
            "claim_queue_sessions must be an integer at least as large as "
            "settlement_lag_sessions"
        )
    if not has_yield:
        return (
            np.zeros(shape, dtype=np.float64),
            np.zeros(shape, dtype=np.int64),
            queue_sessions,
        )
    yields = np.asarray(cash_dividend_yield, dtype=np.float64)
    raw_delays = np.asarray(cash_dividend_payment_delay_sessions)
    if yields.shape != shape or raw_delays.shape != shape:
        raise ValueError("cash-dividend tensors must match target_weights")
    if not np.all(np.isfinite(yields)) or np.any(yields < 0.0):
        raise ValueError("cash_dividend_yield must be finite and non-negative")
    if not np.issubdtype(raw_delays.dtype, np.integer):
        if not np.all(np.isfinite(raw_delays)) or not np.all(
            raw_delays == np.floor(raw_delays)
        ):
            raise ValueError(
                "cash_dividend_payment_delay_sessions must contain integers"
            )
    delays = raw_delays.astype(np.int64, copy=False)
    events = yields > 0.0
    if np.any(events & ((delays < 1) | (delays > queue_sessions))):
        raise ValueError(
            "cash-dividend payment delay exceeds the configured claim queue"
        )
    if np.any((~events) & (delays != 0)):
        raise ValueError(
            "cash-dividend payment delay must be zero when yield is zero"
        )
    return yields, delays, queue_sessions


def _new_state(
    mode: ExecutionMode,
    symbols: int,
    settlement_lag_sessions: int,
    claim_queue_sessions: int,
    initial_cash: float,
) -> TaiwanIntegerState:
    if not np.isfinite(initial_cash) or initial_cash <= 0.0:
        raise ValueError("initial_cash must be finite and positive")
    return TaiwanIntegerState(
        mode=mode,
        settled_cash=float(initial_cash),
        holdings=np.zeros(symbols, dtype=np.int64),
        payable_queue=np.zeros(settlement_lag_sessions, dtype=np.float64),
        receivable_queue=np.zeros(claim_queue_sessions, dtype=np.float64),
        last_nav=float(initial_cash),
        alive=True,
        last_weights=np.zeros(symbols, dtype=np.float64),
        short_sale_collateral=np.zeros(symbols, dtype=np.float64),
        short_margin_collateral=np.zeros(symbols, dtype=np.float64),
    )


def _copy_validate_state(
    state: TaiwanIntegerState | None,
    *,
    mode: ExecutionMode,
    symbols: int,
    settlement_lag_sessions: int,
    claim_queue_sessions: int,
    initial_cash: float,
) -> TaiwanIntegerState:
    if state is None:
        return _new_state(
            mode,
            symbols,
            settlement_lag_sessions,
            claim_queue_sessions,
            initial_cash,
        )
    if not isinstance(state, TaiwanIntegerState):
        raise ValueError("initial_state must be a TaiwanIntegerState")
    if state.mode != mode:
        raise ValueError(f"initial_state mode must be {mode!r}")

    holdings_raw = np.asarray(state.holdings)
    if holdings_raw.shape != (symbols,) or not np.issubdtype(holdings_raw.dtype, np.integer):
        raise ValueError(f"initial_state.holdings must be integer shape [{symbols}]")
    holdings = holdings_raw.astype(np.int64, copy=True)
    if mode == "tw_day_trade" and np.any(holdings != 0):
        raise ValueError("tw_day_trade initial_state must be flat")

    collateral_values: list[np.ndarray] = []
    for field_name in ("short_sale_collateral", "short_margin_collateral"):
        raw_value = getattr(state, field_name)
        if raw_value is None:
            if mode == "tw_cash" and np.any(holdings < 0):
                raise ValueError(
                    f"initial_state.{field_name} is required for negative holdings"
                )
            value = np.zeros(symbols, dtype=np.float64)
        else:
            value = np.asarray(raw_value, dtype=np.float64)
            if value.shape != (symbols,):
                raise ValueError(
                    f"initial_state.{field_name} must have shape [{symbols}]"
                )
            if not np.all(np.isfinite(value)) or np.any(value < 0.0):
                raise ValueError(
                    f"initial_state.{field_name} must be finite and non-negative"
                )
            value = value.copy()
        # A partial cover can be prevented from releasing all of that
        # symbol's proportional collateral by the account-wide maintenance
        # test.  The retained excess deliberately stays in its original
        # per-symbol pool, including after that one symbol reaches zero, until
        # the account has no short liability left.
        if not np.any(holdings < 0) and np.any(value != 0.0):
            raise ValueError(
                f"initial_state.{field_name} must be zero when the account has "
                "no short holding"
            )
        collateral_values.append(value)
    short_sale_collateral, short_margin_collateral = collateral_values
    short_symbols = holdings < 0
    if np.any(short_sale_collateral[short_symbols] <= 0.0):
        raise ValueError(
            "initial_state.short_sale_collateral must be positive for every "
            "negative holding"
        )
    if np.any(short_margin_collateral[short_symbols] <= 0.0):
        raise ValueError(
            "initial_state.short_margin_collateral must be positive for every "
            "negative holding"
        )

    payable = np.asarray(state.payable_queue, dtype=np.float64)
    receivable = np.asarray(state.receivable_queue, dtype=np.float64)
    if payable.shape != (settlement_lag_sessions,) or receivable.shape != (
        claim_queue_sessions,
    ):
        raise ValueError(
            "initial_state payable queue must match settlement_lag_sessions and "
            "receivable queue must match claim_queue_sessions"
        )
    if (
        not np.all(np.isfinite(payable))
        or not np.all(np.isfinite(receivable))
        or np.any(payable < 0.0)
        or np.any(receivable < 0.0)
    ):
        raise ValueError("initial_state settlement queues must be finite and non-negative")
    if not np.isfinite(state.settled_cash) or state.settled_cash < 0.0:
        raise ValueError("initial_state.settled_cash must be finite and non-negative")
    if not np.isfinite(state.last_nav) or (state.alive and state.last_nav <= 0.0):
        raise ValueError("a live initial_state.last_nav must be finite and positive")
    if not state.alive and (
        state.settled_cash != 0.0
        or state.last_nav != 0.0
        or np.any(holdings)
        or np.any(payable)
        or np.any(receivable)
        or np.any(short_sale_collateral)
        or np.any(short_margin_collateral)
    ):
        raise ValueError("an absorbing dead initial_state must have a cleared zero ledger")

    last_weights: np.ndarray | None
    if state.last_weights is None:
        last_weights = None
    else:
        last_weights = np.asarray(state.last_weights, dtype=np.float64)
        if last_weights.shape != (symbols,):
            raise ValueError(
                f"initial_state.last_weights must have shape [{symbols}]"
            )
        if not np.all(np.isfinite(last_weights)):
            raise ValueError("initial_state.last_weights must be finite")
        if mode == "tw_cash" and np.any(last_weights[holdings == 0] != 0.0):
            raise ValueError(
                "tw_cash initial_state.last_weights cannot allocate an unheld symbol"
            )
        if mode == "tw_cash" and np.any(last_weights * holdings < 0.0):
            raise ValueError(
                "tw_cash initial_state.last_weights must have the holding's sign"
            )
        if mode == "tw_day_trade" and np.any(last_weights != 0.0):
            raise ValueError("tw_day_trade initial_state.last_weights must be zero")
        if not state.alive and np.any(last_weights != 0.0):
            raise ValueError(
                "an absorbing dead initial_state must have zero last_weights"
            )
        last_weights = last_weights.copy()

    return TaiwanIntegerState(
        mode=mode,
        settled_cash=float(state.settled_cash),
        holdings=holdings,
        payable_queue=payable.copy(),
        receivable_queue=receivable.copy(),
        last_nav=float(state.last_nav),
        alive=bool(state.alive),
        last_weights=last_weights,
        short_sale_collateral=short_sale_collateral,
        short_margin_collateral=short_margin_collateral,
    )


def _validate_common(
    exposures: np.ndarray,
    prices: np.ndarray,
    *,
    settlement_lag_sessions: int,
    long_only: bool,
) -> None:
    if exposures.shape != prices.shape:
        raise ValueError("exposures and prices must have the same shape")
    if not isinstance(settlement_lag_sessions, (int, np.integer)) or isinstance(
        settlement_lag_sessions, (bool, np.bool_)
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    if int(settlement_lag_sessions) <= 0:
        raise ValueError("settlement_lag_sessions must be a positive integer")
    # Price validity is recurrent: a NaN before listing is harmless, while the
    # same NaN with an existing holding is an unpriceable ledger.  Callers check
    # each row after masks and current holdings are known.
    if long_only and np.any(exposures < 0.0):
        raise ValueError("tw_cash target_weights must be non-negative")
    gross = np.sum(np.abs(exposures), axis=1)
    if np.any(gross > 1.0 + 1e-12):
        raise ValueError("per-session gross exposure cannot exceed 1.0")


def _empty_result_arrays(
    time: int,
    symbols: int,
    lag: int,
    claim_queue_sessions: int | None = None,
) -> dict[str, np.ndarray]:
    receivable_lag = lag if claim_queue_sessions is None else int(claim_queue_sessions)
    return {
        "strategy_returns": np.zeros(time, dtype=np.float64),
        "simple_returns": np.zeros(time, dtype=np.float64),
        "turnover": np.zeros(time, dtype=np.float64),
        "positions": np.zeros((time, symbols), dtype=np.int64),
        "weights": np.zeros((time, symbols), dtype=np.float64),
        "settled_cash_history": np.zeros(time, dtype=np.float64),
        "payable_history": np.zeros(time, dtype=np.float64),
        "receivable_history": np.zeros(time, dtype=np.float64),
        "payable_queue_history": np.zeros((time, lag), dtype=np.float64),
        "receivable_queue_history": np.zeros(
            (time, receivable_lag), dtype=np.float64
        ),
        "execution_payable_history": np.zeros(time, dtype=np.float64),
        "execution_receivable_history": np.zeros(time, dtype=np.float64),
        "execution_nav_history": np.zeros(time, dtype=np.float64),
        "nav_history": np.zeros(time, dtype=np.float64),
        "settlement_net_history": np.zeros(time, dtype=np.float64),
        "fee_history": np.zeros(time, dtype=np.float64),
        "default_mask": np.zeros(time, dtype=np.bool_),
        "short_sale_collateral_history": np.zeros(
            (time, symbols), dtype=np.float64
        ),
        "short_margin_collateral_history": np.zeros(
            (time, symbols), dtype=np.float64
        ),
    }


def _record_state(
    arrays: dict[str, np.ndarray],
    index: int,
    *,
    cash: float,
    holdings: np.ndarray,
    weights: np.ndarray,
    payable: np.ndarray,
    receivable: np.ndarray,
    nav: float,
    execution_nav: float | None = None,
    execution_payable: float | None = None,
    execution_receivable: float | None = None,
    short_sale_collateral: np.ndarray | None = None,
    short_margin_collateral: np.ndarray | None = None,
) -> None:
    arrays["positions"][index] = holdings
    arrays["weights"][index] = weights
    arrays["settled_cash_history"][index] = cash
    arrays["payable_history"][index] = float(np.sum(payable))
    arrays["receivable_history"][index] = float(np.sum(receivable))
    arrays["payable_queue_history"][index] = payable
    arrays["receivable_queue_history"][index] = receivable
    arrays["execution_payable_history"][index] = (
        float(np.sum(payable))
        if execution_payable is None
        else float(execution_payable)
    )
    arrays["execution_receivable_history"][index] = (
        float(np.sum(receivable))
        if execution_receivable is None
        else float(execution_receivable)
    )
    arrays["execution_nav_history"][index] = (
        float(nav) if execution_nav is None else float(execution_nav)
    )
    arrays["nav_history"][index] = nav
    if short_sale_collateral is not None:
        arrays["short_sale_collateral_history"][index] = short_sale_collateral
    if short_margin_collateral is not None:
        arrays["short_margin_collateral_history"][index] = short_margin_collateral


def _record_ruin(
    arrays: dict[str, np.ndarray],
    index: int,
    *,
    payable: np.ndarray,
    receivable: np.ndarray,
    short_sale_collateral: np.ndarray | None = None,
    short_margin_collateral: np.ndarray | None = None,
) -> None:
    """Record an absorbing synthetic close-out after settlement default.

    Once delivery fails, ordinary mark-to-market accounting no longer describes
    an executable portfolio.  The oracle therefore closes the modeled strategy
    at zero and clears its ledger; this preserves an explicit zero-value identity
    rather than pretending the strategy can resume after default.
    """

    arrays["strategy_returns"][index] = -np.inf
    arrays["simple_returns"][index] = -1.0
    arrays["default_mask"][index] = True
    payable.fill(0.0)
    receivable.fill(0.0)
    _record_state(
        arrays,
        index,
        cash=0.0,
        holdings=np.zeros(arrays["positions"].shape[1], dtype=np.int64),
        weights=np.zeros(arrays["weights"].shape[1], dtype=np.float64),
        payable=payable,
        receivable=receivable,
        nav=0.0,
        short_sale_collateral=short_sale_collateral,
        short_margin_collateral=short_margin_collateral,
    )


def _settle_start_of_session(
    cash: float,
    payable: np.ndarray,
    receivable: np.ndarray,
) -> tuple[float, bool]:
    """Settle payable first, then receivable, and advance both fixed queues."""

    due_payable = float(payable[0])
    due_receivable = float(receivable[0])
    if due_payable > cash + max(1e-9, abs(cash) * 1e-12):
        return cash, False
    cash -= due_payable
    # The receivable is deliberately credited only after the payable test.
    cash += due_receivable
    payable[:-1] = payable[1:]
    payable[-1] = 0.0
    receivable[:-1] = receivable[1:]
    receivable[-1] = 0.0
    return cash, True


def _enqueue_net_claim(
    settlement_net: float,
    payable: np.ndarray,
    receivable: np.ndarray,
    *,
    settlement_lag_sessions: int | None = None,
) -> None:
    # One trade date produces one account-level net claim, never gross claims on
    # both sides.  Small floating noise is removed to make invariants auditable.
    if abs(settlement_net) <= 1e-12:
        return
    if settlement_net > 0.0:
        lag = payable.size if settlement_lag_sessions is None else int(
            settlement_lag_sessions
        )
        if lag <= 0 or lag > receivable.size:
            raise ValueError(
                "settlement_lag_sessions must address the receivable queue"
            )
        receivable[lag - 1] += settlement_net
    else:
        payable[-1] += -settlement_net


def _capacity_for_new_payable(
    cash: float,
    payable: np.ndarray,
    receivable: np.ndarray,
) -> float:
    """Exact settled cash available when a claim created today becomes due.

    Existing receivables due on an *earlier* session may fund the new payable.
    A receivable due on the same session may not, because the payable delivery
    test occurs first.  Returning zero when an earlier existing payable already
    cannot be met is fail-closed: today's later-dated claim cannot cure it.
    """

    projected_cash = float(cash)
    for index in range(max(int(payable.size) - 1, 0)):
        due = float(payable[index])
        tolerance = max(1e-9, abs(projected_cash) * 1e-12)
        if due > projected_cash + tolerance:
            return 0.0
        projected_cash = projected_cash - due + float(receivable[index])
    return max(projected_cash - float(payable[-1]), 0.0)


def _log_and_simple_return(nav: float, previous_nav: float) -> tuple[float, float]:
    if nav <= 0.0 or previous_nav <= 0.0:
        return -np.inf, -1.0
    ratio = nav / previous_nav
    return float(np.log(ratio)), float(ratio - 1.0)


def _build_result(
    arrays: dict[str, np.ndarray],
    *,
    mode: ExecutionMode,
    cash: float,
    holdings: np.ndarray,
    payable: np.ndarray,
    receivable: np.ndarray,
    last_nav: float,
    alive: bool,
    final_weights: np.ndarray,
    short_sale_collateral: np.ndarray | None = None,
    short_margin_collateral: np.ndarray | None = None,
) -> TaiwanIntegerBacktestResult:
    sale_collateral = (
        np.zeros(holdings.shape, dtype=np.float64)
        if short_sale_collateral is None
        else np.asarray(short_sale_collateral, dtype=np.float64)
    )
    margin_collateral = (
        np.zeros(holdings.shape, dtype=np.float64)
        if short_margin_collateral is None
        else np.asarray(short_margin_collateral, dtype=np.float64)
    )
    state = TaiwanIntegerState(
        mode=mode,
        settled_cash=float(cash),
        holdings=holdings.astype(np.int64, copy=True),
        payable_queue=payable.astype(np.float64, copy=True),
        receivable_queue=receivable.astype(np.float64, copy=True),
        last_nav=float(last_nav),
        alive=bool(alive),
        last_weights=np.asarray(final_weights, dtype=np.float64).copy(),
        short_sale_collateral=sale_collateral.copy(),
        short_margin_collateral=margin_collateral.copy(),
    )
    return TaiwanIntegerBacktestResult(
        final_state=state,
        final_weights=np.asarray(final_weights, dtype=np.float64).copy(),
        **arrays,
    )


def _run_tw_cash_integer_long_only(
    target_weights: object,
    close_prices: object,
    *,
    future_log_returns: object | None = None,
    unresolved_corporate_action_mask: object | None = None,
    cash_dividend_yield: object | None = None,
    cash_dividend_payment_delay_sessions: object | None = None,
    claim_queue_sessions: int | None = None,
    buy_fee_rates: object,
    sell_fee_rates: object,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: object | None = None,
    tradable_mask: object | None = None,
    can_buy_mask: object | None = None,
    can_sell_mask: object | None = None,
    force_exit_mask: object | None = None,
    daily_volumes: object | None = None,
    max_volume_participation: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_budget: float = 1.0,
    state_advance_mask: object | None = None,
    settlement_lag_sessions: int = 2,
    initial_cash: float = 1_000_000.0,
    initial_state: TaiwanIntegerState | None = None,
) -> TaiwanIntegerBacktestResult:
    """Execute long-only Taiwan cash-stock targets in integer shares.

    ``target_weights[t]`` is the desired closing gross allocation as a fraction
    of pre-trade ledger NAV.  Untradable or side-blocked positions are held
    unchanged.  Participation limits are applied before the portfolio turnover
    cap; an explicit ``force_exit_mask`` bypasses both, matching the canonical
    lifecycle contract.  Sells are fixed first; if desired purchases would
    create a T+2 payable larger than settled cash after reserving older
    payables, all desired buy quantities are scaled proportionally and rounded
    down to lots.  This deterministic O(S) rule is conservative; it is not a
    combinatorial knapsack optimizer.  Broker-specific minimum commission and
    rounding apply once to each nonzero symbol-side aggregate order.  The
    default ``0``/``none`` profile preserves the user's proportional schedule.
    When participation limiting is enabled, ``daily_volumes`` is the explicit
    causal share-volume reference supplied by the public dispatcher (normally
    completed session t-1), not the eventual volume of the execution session.
    """

    targets = _as_matrix("target_weights", target_weights)
    raw_prices = _as_execution_price_matrix("close_prices", close_prices)
    forward_returns = (
        np.zeros_like(raw_prices)
        if future_log_returns is None
        # Missing labels are legitimate outside a held position.  Validate
        # state-dependent finiteness after execution instead of globally
        # fabricating zero return or rejecting an inactive panel cell.
        else _as_execution_price_matrix("future_log_returns", future_log_returns)
    )
    if unresolved_corporate_action_mask is None:
        raise ValueError(
            "tw_cash requires the official corporate-action avoidance mask "
            "for exact real-share accounting"
        )
    unresolved_actions = _as_bool_matrix(
        "unresolved_corporate_action_mask",
        unresolved_corporate_action_mask,
        targets.shape,
    )
    if forward_returns.shape != raw_prices.shape:
        raise ValueError("future_log_returns must have the same shape as close_prices")
    _validate_common(
        targets,
        raw_prices,
        settlement_lag_sessions=settlement_lag_sessions,
        long_only=True,
    )
    time, symbols = targets.shape
    lag = int(settlement_lag_sessions)
    dividend_yields, dividend_delays, claim_queue = _cash_dividend_inputs(
        targets.shape,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue_sessions,
    )
    commission_rates, sell_tax_rates = _split_commission_and_sell_tax_rates(
        buy_fee_rates,
        sell_fee_rates,
        symbols,
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
    lots = _as_lot_vector(lot_sizes, symbols, default=1)
    tradable = _as_bool_matrix("tradable_mask", tradable_mask, targets.shape)
    # ``tradable`` is the causal selection universe; current-close side masks
    # independently govern execution of existing holdings.
    can_buy = _as_bool_matrix("can_buy_mask", can_buy_mask, targets.shape)
    can_sell = _as_bool_matrix("can_sell_mask", can_sell_mask, targets.shape)
    force_exit = (
        np.zeros(targets.shape, dtype=np.bool_)
        if force_exit_mask is None
        else _as_bool_matrix("force_exit_mask", force_exit_mask, targets.shape)
    )
    volumes = _as_nonnegative_matrix("daily_volumes", daily_volumes, targets.shape)
    volume_ratio = _as_nonnegative_ratio(
        "max_volume_participation", max_volume_participation
    )
    turnover_ratio = _as_nonnegative_ratio("max_turnover_ratio", max_turnover_ratio)
    gross_ratio = _as_nonnegative_ratio("gross_budget", gross_budget)
    if gross_ratio > 1.0:
        raise ValueError("gross_budget must be between 0 and 1 inclusive")
    if volume_ratio > 0.0 and volumes is None:
        raise ValueError(
            "daily_volumes is required when max_volume_participation is positive"
        )
    advance = _as_advance_mask(state_advance_mask, time)
    state = _copy_validate_state(
        initial_state,
        mode="tw_cash",
        symbols=symbols,
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue,
        initial_cash=initial_cash,
    )

    cash = state.settled_cash
    holdings = state.holdings.copy()
    payable = state.payable_queue.copy()
    receivable = state.receivable_queue.copy()
    last_nav = state.last_nav
    alive = state.alive
    final_weights = (
        np.zeros(symbols, dtype=np.float64)
        if state.last_weights is None
        else state.last_weights.copy()
    )
    final_weights_known = state.last_weights is not None or not np.any(holdings)
    if np.any(holdings % lots != 0):
        raise ValueError("initial_state.holdings must align with lot_sizes")
    arrays = _empty_result_arrays(time, symbols, lag, claim_queue)

    for t in range(time):
        if not alive:
            final_weights.fill(0.0)
            _record_state(
                arrays,
                t,
                cash=0.0,
                holdings=np.zeros(symbols, dtype=np.int64),
                weights=np.zeros(symbols, dtype=np.float64),
                payable=payable,
                receivable=receivable,
                nav=0.0,
            )
            continue
        if not advance[t]:
            # Padding/non-session rows are a pure identity: no quote is
            # observed, no claim ages, and NAV/weights remain exactly the last
            # active close.  A legacy state with holdings but no carried
            # weights cannot reconstruct that identity without fabricating a
            # valuation, so fail rather than read a padding-row quote.
            if not final_weights_known:
                raise ValueError(
                    "tw_cash leading padding with existing holdings requires "
                    "initial_state.last_weights"
                )
            _record_state(
                arrays,
                t,
                cash=cash,
                holdings=holdings,
                weights=final_weights,
                payable=payable,
                receivable=receivable,
                nav=last_nav,
            )
            continue

        # T+2 delivery is a start-of-session event.  It must happen before any
        # close quote is consulted: a failed morning payable defaults even if
        # the later session has no usable valuation quote.
        cash, settlement_ok = _settle_start_of_session(cash, payable, receivable)
        if not settlement_ok:
            alive = False
            holdings.fill(0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        # The source-derived action mask is aligned to the exchange session
        # immediately before an official ex-date.  Without entitlement payment
        # dates, the exact no-fabrication policy is to liquidate at this close
        # and block a new entry.  This action exit bypasses voluntary volume and
        # turnover caps, but unlike a terminal lifecycle exit it may not bypass
        # an unavailable sell side.
        action_avoidance = unresolved_actions[t]
        action_exit = action_avoidance & ~force_exit[t] & (holdings != 0)
        blocked_action_exit = action_exit & ~can_sell[t]
        if np.any(blocked_action_exit):
            raise RuntimeError(
                "tw_cash cannot liquidate a held position before an official "
                "corporate action because the sell side is unavailable; "
                f"time_index={t}, "
                f"symbol_indices={np.flatnonzero(blocked_action_exit).tolist()}"
            )
        mandatory_exit = force_exit[t] | action_avoidance
        exiting_holdings = mandatory_exit & (holdings != 0)
        base_holdings = np.where(exiting_holdings, 0, holdings)
        held = holdings != 0
        side_available = can_buy[t] | can_sell[t]
        # Reading an execution quote is justified only when this row can
        # actually submit an order.  In particular, the model's causal outer
        # universe may include a previously-alive but currently halted symbol;
        # an unheld zero/blocked target is a no-op and must not turn its missing
        # close into a false ledger error.  Existing halted holdings are valued
        # separately below from recurrent state.
        target_enabled = ~mandatory_exit & (
            (held & side_available)
            | (~held & (targets[t] > 0.0) & can_buy[t])
        )
        required_execution_price = target_enabled | exiting_holdings
        execution_price_row = _safe_execution_price_row(
            "close_prices",
            raw_prices,
            t,
            required_execution_price,
        )
        valuation_price_row = _cash_valuation_price_row(
            raw_prices,
            t,
            holdings=holdings,
            last_nav=last_nav,
            last_weights=(final_weights if final_weights_known else None),
        )

        nav_before_trade = (
            cash
            + float(np.dot(holdings, valuation_price_row))
            + float(np.sum(receivable))
            - float(np.sum(payable))
        )
        if nav_before_trade <= 0.0:
            alive = False
            holdings.fill(0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        # Convert desired value to int64 shares only for symbols that can
        # actually target a position.  Inactive cells may legitimately have no
        # quote and receive an internal unit sentinel above; rounding their
        # unused request could otherwise overflow before the execution mask is
        # applied.
        raw_target_shares = np.zeros(symbols, dtype=np.int64)
        if np.any(target_enabled):
            raw_target_shares[target_enabled] = _round_quantity_down_to_lots(
                targets[t, target_enabled]
                * nav_before_trade
                / execution_price_row[target_enabled],
                lots[target_enabled],
            )
        target_shares = np.where(target_enabled, raw_target_shares, base_holdings)
        ordinary_delta = target_shares - base_holdings
        ordinary_delta[(ordinary_delta > 0) & ~can_buy[t]] = 0
        ordinary_delta[(ordinary_delta < 0) & ~can_sell[t]] = 0

        # Match the canonical constraint order.  Missing causal volume is no
        # demonstrated fill capacity, not an unlimited order.  Finite
        # non-negative values cap the ordinary order and are rounded down to
        # complete lots.
        if volume_ratio > 0.0:
            assert volumes is not None
            volume_row = volumes[t]
            valid_volume = np.isfinite(volume_row)
            max_quantity = np.zeros(symbols, dtype=np.int64)
            max_quantity[valid_volume] = _round_quantity_down_to_lots(
                volume_row[valid_volume] * volume_ratio,
                lots[valid_volume],
            )
            clipped_quantity = np.minimum(np.abs(ordinary_delta), max_quantity)
            ordinary_delta = np.sign(ordinary_delta).astype(np.int64) * clipped_quantity

        if turnover_ratio > 0.0:
            ordinary_notional = float(
                np.dot(
                    np.abs(ordinary_delta).astype(np.float64),
                    execution_price_row,
                )
            )
            ordinary_budget = nav_before_trade * turnover_ratio
            if ordinary_notional > ordinary_budget + 1e-9 and ordinary_notional > 0.0:
                ordinary_delta = _scale_signed_lot_orders(
                    ordinary_delta,
                    ordinary_budget / ordinary_notional,
                    lots,
                )

        tentative_holdings = base_holdings + ordinary_delta
        total_delta = tentative_holdings - holdings
        sells = np.maximum(-total_delta, 0)
        desired_buys = np.maximum(total_delta, 0)

        # A constrained sale may leave more old holdings than the target
        # assumes.  Recompute buy capacity from the actually executable sells
        # so a portfolio rotation cannot exceed the configured gross budget.
        holdings_after_sells = holdings - sells
        gross_buy_budget = max(
            nav_before_trade * gross_ratio
            - float(np.dot(holdings_after_sells, valuation_price_row)),
            0.0,
        )
        desired_buy_notional = float(
            np.dot(desired_buys, execution_price_row)
        )
        if desired_buy_notional > gross_buy_budget + 1e-9:
            desired_buys = _round_quantity_down_to_lots(
                desired_buys
                * (
                    gross_buy_budget / desired_buy_notional
                    if desired_buy_notional > 0.0
                    else 0.0
                ),
                lots,
            )

        sell_notional_by_symbol = sells * execution_price_row
        sell_commissions_by_symbol = _commission_fees_by_symbol(
            sell_notional_by_symbol,
            commission_rates,
            minimum_commission=min_commission,
            rounding=commission_rounding_rule,
        )
        sell_taxes_by_symbol = _tax_fees_by_symbol(
            sell_notional_by_symbol,
            sell_tax_rates,
            rounding=tax_rounding_rule,
        )
        sell_fees_by_symbol = sell_commissions_by_symbol + sell_taxes_by_symbol
        sell_net = float(np.sum(sell_notional_by_symbol - sell_fees_by_symbol))
        _, _, desired_buy_cost_by_symbol = _buy_costs_by_symbol(
            desired_buys,
            execution_price_row,
            commission_rates,
            minimum_commission=min_commission,
            commission_rounding=commission_rounding_rule,
        )
        desired_buy_cost = float(np.sum(desired_buy_cost_by_symbol))

        # Project the existing queue session by session.  Receivables arriving
        # before today's new claim is due become settled cash; a receivable due
        # on that same session remains unavailable for its pre-receipt payable.
        free_settled_cash = _capacity_for_new_payable(cash, payable, receivable)
        # All buys and sells sharing this trade date are legally offset into one
        # T+2 balance, so net sale proceeds reduce the new payable before the
        # account's settlement capacity is checked.
        buy_budget = max(free_settled_cash + sell_net, 0.0)
        buys = desired_buys
        if desired_buy_cost > buy_budget + max(1e-9, abs(buy_budget) * 1e-12):
            if min_commission == 0.0 and commission_rounding_rule == "none":
                scale = (
                    max(buy_budget, 0.0) / desired_buy_cost
                    if desired_buy_cost > 0.0
                    else 0.0
                )
                buys = (
                    np.floor((desired_buys * scale) / lots + 1e-12).astype(np.int64)
                    * lots
                )
            else:
                buys = _scale_lot_buys_to_budget_with_fixed_fees(
                    desired_buys,
                    execution_price_row,
                    lots,
                    commission_rates,
                    budget=buy_budget,
                    minimum_commission=min_commission,
                    commission_rounding=commission_rounding_rule,
                )

        (
            buy_notional_by_symbol,
            buy_fees_by_symbol,
            buy_cost_by_symbol,
        ) = _buy_costs_by_symbol(
            buys,
            execution_price_row,
            commission_rates,
            minimum_commission=min_commission,
            commission_rounding=commission_rounding_rule,
        )
        buy_cost = float(np.sum(buy_cost_by_symbol))
        sell_notional = float(np.sum(sell_notional_by_symbol))
        fees = float(np.sum(sell_fees_by_symbol) + np.sum(buy_fees_by_symbol))
        settlement_net = sell_net - buy_cost

        # Only voluntary purchases are admission-controlled.  A mandatory or
        # ordinary tiny sale whose fixed fee exceeds its proceeds creates an
        # unavoidable net payable and may default when due; it must not be
        # erased or mislabeled as an internal affordability error on trade date.
        admitted_buy_budget = max(free_settled_cash + sell_net, 0.0)
        if buy_cost > admitted_buy_budget + max(
            1e-8, abs(admitted_buy_budget) * 1e-12
        ):
            raise RuntimeError("internal cash affordability invariant violated")

        holdings = holdings - sells + buys
        residual_action_position = (holdings != 0) & action_avoidance
        if np.any(residual_action_position):
            raise RuntimeError(
                "tw_cash official corporate-action avoidance liquidation did "
                "not finish at the preceding close; "
                f"time_index={t}, "
                "symbol_indices="
                f"{np.flatnonzero(residual_action_position).tolist()}"
            )
        _enqueue_net_claim(
            settlement_net,
            payable,
            receivable,
            settlement_lag_sessions=lag,
        )
        nav_at_execution = (
            cash
            + float(np.dot(holdings, valuation_price_row))
            + float(np.sum(receivable))
            - float(np.sum(payable))
        )
        execution_payable = float(np.sum(payable))
        execution_receivable = float(np.sum(receivable))
        expected_nav_at_execution = nav_before_trade - fees
        if not np.isclose(
            nav_at_execution,
            expected_nav_at_execution,
            rtol=2e-12,
            atol=1e-7,
        ):
            raise RuntimeError("cash ledger asset-liability identity violated")
        # The canonical sample at row t executes at close[t] and realizes the
        # supplied close-to-next-session total return before producing row t's
        # strategy return.  Using close[t] for both execution and valuation
        # would shift every PnL one row and silently drop the final label.
        active_return = holdings != 0
        with np.errstate(over="ignore", invalid="ignore"):
            growth = np.exp(forward_returns[t])
        invalid_active_return = active_return & (
            ~np.isfinite(growth) | (growth <= 0.0)
        )
        if np.any(invalid_active_return):
            raise ValueError(
                "future_log_returns produces a non-finite/non-positive "
                "valuation for an executed holding; "
                f"time_index={t}, "
                "symbol_indices="
                f"{np.flatnonzero(invalid_active_return).tolist()}"
            )
        safe_growth = np.where(active_return, growth, 1.0)
        next_valuation_prices = valuation_price_row * safe_growth
        dividend_claim_by_symbol = (
            np.maximum(holdings, 0).astype(np.float64)
            * valuation_price_row
            * dividend_yields[t]
        )
        for delay in np.unique(dividend_delays[t][dividend_claim_by_symbol > 0.0]):
            due = dividend_delays[t] == delay
            receivable[int(delay) - 1] += float(
                np.sum(dividend_claim_by_symbol[due])
            )
        dividend_claim = float(np.sum(dividend_claim_by_symbol))
        nav = nav_at_execution + dividend_claim + float(
            np.dot(holdings, next_valuation_prices - valuation_price_row)
        )
        if nav <= 0.0:
            alive = False
            holdings.fill(0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        log_return, simple_return = _log_and_simple_return(nav, last_nav)
        arrays["strategy_returns"][t] = log_return
        arrays["simple_returns"][t] = simple_return
        arrays["turnover"][t] = (
            float(np.sum(buy_notional_by_symbol)) + sell_notional
        ) / nav_before_trade
        arrays["settlement_net_history"][t] = settlement_net
        arrays["fee_history"][t] = fees
        weights = holdings * valuation_price_row / nav_at_execution
        final_weights = holdings * next_valuation_prices / nav
        final_weights_known = True
        _record_state(
            arrays,
            t,
            cash=cash,
            holdings=holdings,
            weights=weights,
            payable=payable,
            receivable=receivable,
            nav=nav,
            execution_nav=nav_at_execution,
            execution_payable=execution_payable,
            execution_receivable=execution_receivable,
        )
        last_nav = nav

    return _build_result(
        arrays,
        mode="tw_cash",
        cash=cash,
        holdings=holdings,
        payable=payable,
        receivable=receivable,
        last_nav=last_nav,
        alive=alive,
        final_weights=final_weights,
    )


def _run_tw_cash_integer_signed(
    target_weights: object,
    close_prices: object,
    *,
    future_log_returns: object | None,
    unresolved_corporate_action_mask: object | None,
    cash_dividend_yield: object | None,
    cash_dividend_payment_delay_sessions: object | None,
    claim_queue_sessions: int | None,
    buy_fee_rates: object,
    sell_fee_rates: object,
    minimum_commission: float,
    commission_rounding: str,
    tax_rounding: str,
    lot_sizes: object | None,
    short_lot_sizes: object | None,
    tradable_mask: object | None,
    can_buy_mask: object | None,
    can_sell_mask: object | None,
    can_short_open_mask: object | None,
    force_exit_mask: object | None,
    force_short_cover_mask: object | None,
    short_margin_rate: object,
    short_maintenance_ratio: float,
    short_handling_fee_rate: object,
    short_capacity_shares: object,
    daily_volumes: object | None,
    max_volume_participation: float,
    max_turnover_ratio: float,
    gross_budget: float,
    state_advance_mask: object | None,
    settlement_lag_sessions: int,
    initial_cash: float,
    initial_state: TaiwanIntegerState | None,
) -> TaiwanIntegerBacktestResult:
    """Signed Taiwan cash ledger with fully collateralized margin shorts."""

    targets = _as_matrix("target_weights", target_weights)
    raw_prices = _as_execution_price_matrix("close_prices", close_prices)
    forward_returns = (
        np.zeros_like(raw_prices)
        if future_log_returns is None
        else _as_execution_price_matrix("future_log_returns", future_log_returns)
    )
    if unresolved_corporate_action_mask is None:
        raise ValueError(
            "tw_cash requires the official corporate-action avoidance mask "
            "for exact real-share accounting"
        )
    unresolved_actions = _as_bool_matrix(
        "unresolved_corporate_action_mask",
        unresolved_corporate_action_mask,
        targets.shape,
    )
    if forward_returns.shape != raw_prices.shape:
        raise ValueError("future_log_returns must have the same shape as close_prices")
    _validate_common(
        targets,
        raw_prices,
        settlement_lag_sessions=settlement_lag_sessions,
        long_only=False,
    )
    time, symbols = targets.shape
    lag = int(settlement_lag_sessions)
    dividend_yields, dividend_delays, claim_queue = _cash_dividend_inputs(
        targets.shape,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue_sessions,
    )
    commission_rates, sell_tax_rates = _split_commission_and_sell_tax_rates(
        buy_fee_rates,
        sell_fee_rates,
        symbols,
    )
    margin_rates = _as_time_symbol_rate(
        "short_margin_rate", short_margin_rate, targets.shape
    )
    maintenance_ratio = _as_short_maintenance_ratio(short_maintenance_ratio)
    short_handling_rates = _as_rate_vector(
        "short_handling_fee_rate", short_handling_fee_rate, symbols
    )
    min_commission = _as_minimum_commission(minimum_commission)
    commission_rounding_rule = normalize_fee_rounding(
        commission_rounding,
        name="commission_rounding",
    )
    tax_rounding_rule = normalize_fee_rounding(tax_rounding, name="tax_rounding")
    lots = _as_lot_vector(lot_sizes, symbols, default=1)
    short_lots = _as_lot_vector(short_lot_sizes, symbols, default=1000)
    tradable = _as_bool_matrix("tradable_mask", tradable_mask, targets.shape)
    # Keep current-close execution facts separate from the prior-alive model
    # selection mask, matching the canonical tensor TW cash ledger.
    can_buy = _as_bool_matrix("can_buy_mask", can_buy_mask, targets.shape)
    can_sell = _as_bool_matrix("can_sell_mask", can_sell_mask, targets.shape)
    if can_short_open_mask is None:
        can_short_open = np.zeros(targets.shape, dtype=np.bool_)
    else:
        can_short_open = (
            _as_bool_matrix(
                "can_short_open_mask", can_short_open_mask, targets.shape
            )
            & can_sell
        )
    force_exit = (
        np.zeros(targets.shape, dtype=np.bool_)
        if force_exit_mask is None
        else _as_bool_matrix("force_exit_mask", force_exit_mask, targets.shape)
    )
    force_short_cover = (
        np.zeros(targets.shape, dtype=np.bool_)
        if force_short_cover_mask is None
        else _as_bool_matrix(
            "force_short_cover_mask", force_short_cover_mask, targets.shape
        )
    )
    short_capacity = _as_nonnegative_matrix(
        "short_capacity_shares", short_capacity_shares, targets.shape
    )
    assert short_capacity is not None
    volumes = _as_nonnegative_matrix("daily_volumes", daily_volumes, targets.shape)
    volume_ratio = _as_nonnegative_ratio(
        "max_volume_participation", max_volume_participation
    )
    turnover_ratio = _as_nonnegative_ratio("max_turnover_ratio", max_turnover_ratio)
    gross_ratio = _as_nonnegative_ratio("gross_budget", gross_budget)
    if gross_ratio > 1.0:
        raise ValueError("gross_budget must be between 0 and 1 inclusive")
    if volume_ratio > 0.0 and volumes is None:
        raise ValueError(
            "daily_volumes is required when max_volume_participation is positive"
        )
    advance = _as_advance_mask(state_advance_mask, time)
    state = _copy_validate_state(
        initial_state,
        mode="tw_cash",
        symbols=symbols,
        settlement_lag_sessions=lag,
        claim_queue_sessions=claim_queue,
        initial_cash=initial_cash,
    )
    if can_short_open_mask is None and np.any(targets < 0.0):
        raise ValueError("negative tw_cash targets require can_short_open_mask")

    cash = state.settled_cash
    holdings = state.holdings.copy()
    payable = state.payable_queue.copy()
    receivable = state.receivable_queue.copy()
    short_sale_collateral = np.asarray(
        state.short_sale_collateral, dtype=np.float64
    ).copy()
    short_margin_collateral = np.asarray(
        state.short_margin_collateral, dtype=np.float64
    ).copy()
    last_nav = state.last_nav
    alive = state.alive
    final_weights = (
        np.zeros(symbols, dtype=np.float64)
        if state.last_weights is None
        else state.last_weights.copy()
    )
    final_weights_known = state.last_weights is not None or not np.any(holdings)
    if np.any(holdings[holdings >= 0] % lots[holdings >= 0] != 0):
        raise ValueError("positive initial_state.holdings must align with lot_sizes")
    short_held = holdings < 0
    if np.any((-holdings[short_held]) % short_lots[short_held] != 0):
        raise ValueError(
            "negative initial_state.holdings must align with short_lot_sizes"
        )
    arrays = _empty_result_arrays(time, symbols, lag, claim_queue)

    for t in range(time):
        if not alive:
            final_weights.fill(0.0)
            _record_state(
                arrays,
                t,
                cash=0.0,
                holdings=np.zeros(symbols, dtype=np.int64),
                weights=np.zeros(symbols, dtype=np.float64),
                payable=payable,
                receivable=receivable,
                nav=0.0,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
            )
            continue
        if not advance[t]:
            if not final_weights_known:
                raise ValueError(
                    "tw_cash leading padding with existing holdings requires "
                    "initial_state.last_weights"
                )
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
            )
            continue

        cash, settlement_ok = _settle_start_of_session(cash, payable, receivable)
        if not settlement_ok:
            alive = False
            holdings.fill(0)
            short_sale_collateral.fill(0.0)
            short_margin_collateral.fill(0.0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(
                arrays,
                t,
                payable=payable,
                receivable=receivable,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
            )
            continue

        action_avoidance = unresolved_actions[t]
        action_exit = action_avoidance & ~force_exit[t] & (holdings != 0)
        blocked_long_action = action_exit & (holdings > 0) & ~can_sell[t]
        blocked_short_cover = (
            (action_exit | force_short_cover[t] | (force_exit[t] & (holdings < 0)))
            & (holdings < 0)
            & ~can_buy[t]
        )
        if np.any(blocked_long_action):
            raise RuntimeError(
                "tw_cash cannot liquidate a long position before an official "
                "corporate action because the sell side is unavailable; "
                f"time_index={t}, symbol_indices="
                f"{np.flatnonzero(blocked_long_action).tolist()}"
            )
        if np.any(blocked_short_cover):
            raise RuntimeError(
                "tw_cash cannot cover a mandatory margin short because the "
                f"buy side is unavailable; time_index={t}, symbol_indices="
                f"{np.flatnonzero(blocked_short_cover).tolist()}"
            )

        mandatory_flat = force_exit[t] | action_avoidance
        forced_cover = force_short_cover[t] & (holdings < 0)
        base_holdings = holdings.copy()
        base_holdings[mandatory_flat] = 0
        base_holdings[forced_cover] = 0
        held = holdings != 0
        may_adjust = can_buy[t] | can_sell[t]
        wants_long = targets[t] > 0.0
        wants_short = targets[t] < 0.0
        target_enabled = ~mandatory_flat & (
            (held & may_adjust)
            | (~held & wants_long & can_buy[t])
            | (~held & wants_short & can_short_open[t])
            | (forced_cover & wants_long & can_buy[t])
        )
        exiting_holdings = mandatory_flat & held
        required_execution_price = target_enabled | exiting_holdings | forced_cover
        execution_price_row = _safe_execution_price_row(
            "close_prices", raw_prices, t, required_execution_price
        )
        valuation_price_row = _cash_valuation_price_row(
            raw_prices,
            t,
            holdings=holdings,
            last_nav=last_nav,
            last_weights=(final_weights if final_weights_known else None),
        )

        short_collateral_before_trade = float(
            np.sum(short_sale_collateral) + np.sum(short_margin_collateral)
        )
        nav_before_trade = (
            cash
            + float(np.dot(holdings, valuation_price_row))
            + short_collateral_before_trade
            + float(np.sum(receivable))
            - float(np.sum(payable))
        )
        if nav_before_trade <= 0.0:
            alive = False
            holdings.fill(0)
            short_sale_collateral.fill(0.0)
            short_margin_collateral.fill(0.0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(
                arrays,
                t,
                payable=payable,
                receivable=receivable,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
            )
            continue

        desired_holdings = base_holdings.copy()
        positive_target = target_enabled & (targets[t] >= 0.0)
        negative_target = target_enabled & (targets[t] < 0.0)
        if np.any(positive_target):
            desired_holdings[positive_target] = _round_quantity_down_to_lots(
                targets[t, positive_target]
                * nav_before_trade
                / execution_price_row[positive_target],
                lots[positive_target],
            )
        if np.any(negative_target):
            desired_holdings[negative_target] = -_round_quantity_down_to_lots(
                -targets[t, negative_target]
                * nav_before_trade
                / execution_price_row[negative_target],
                short_lots[negative_target],
            )
        # A forced cover may cross zero into a discretionary long, but may not
        # re-establish a short on the same session.
        desired_holdings[forced_cover] = np.maximum(
            desired_holdings[forced_cover], 0
        )

        increasing = desired_holdings > base_holdings
        desired_holdings[increasing & ~can_buy[t]] = base_holdings[
            increasing & ~can_buy[t]
        ]
        decreasing = desired_holdings < base_holdings
        desired_holdings[decreasing & ~can_sell[t]] = base_holdings[
            decreasing & ~can_sell[t]
        ]
        # The short-open mask blocks only new/increased negative exposure; it
        # never blocks selling an existing cash long down to zero.
        decreasing = desired_holdings < base_holdings
        cannot_add_short = decreasing & ~can_short_open[t]
        desired_holdings[cannot_add_short] = np.maximum(
            desired_holdings[cannot_add_short],
            np.minimum(base_holdings[cannot_add_short], 0),
        )

        if volume_ratio > 0.0:
            assert volumes is not None
            valid_volume = np.isfinite(volumes[t])
            max_quantity = np.zeros(symbols, dtype=np.int64)
            finite_capacity = np.floor(
                np.maximum(volumes[t, valid_volume], 0.0) * volume_ratio + 1e-12
            )
            max_quantity[valid_volume] = np.minimum(
                finite_capacity,
                np.iinfo(np.int64).max,
            ).astype(np.int64)
            desired_holdings = _limit_signed_transition_by_quantity(
                base_holdings,
                desired_holdings,
                max_quantity,
                lots,
                short_lots,
            )

        ordinary_delta = desired_holdings - base_holdings
        if turnover_ratio > 0.0:
            ordinary_notional = float(
                np.dot(np.abs(ordinary_delta).astype(np.float64), execution_price_row)
            )
            ordinary_budget = nav_before_trade * turnover_ratio
            if ordinary_notional > ordinary_budget + 1e-9:
                scaled_quantities = np.floor(
                    np.abs(ordinary_delta).astype(np.float64)
                    * ordinary_budget
                    / ordinary_notional
                    + 1e-12
                )
                desired_holdings = _limit_signed_transition_by_quantity(
                    base_holdings,
                    desired_holdings,
                    np.minimum(scaled_quantities, np.iinfo(np.int64).max).astype(
                        np.int64
                    ),
                    lots,
                    short_lots,
                )

        proposed_holdings = desired_holdings
        reduced_holdings, opening_holdings = _separate_reductions_and_openings(
            holdings, proposed_holdings
        )
        reduced_gross = float(
            np.dot(np.abs(reduced_holdings).astype(np.float64), valuation_price_row)
        )
        opening_gross = float(
            np.dot(np.abs(opening_holdings).astype(np.float64), execution_price_row)
        )
        gross_open_budget = max(nav_before_trade * gross_ratio - reduced_gross, 0.0)
        if opening_gross > gross_open_budget + 1e-9:
            proposed_holdings = _scaled_opening_holdings(
                reduced_holdings,
                opening_holdings,
                gross_open_budget / opening_gross if opening_gross > 0.0 else 0.0,
                lots,
                short_lots,
            )

        if short_capacity is not None:
            old_short = np.maximum(-holdings, 0)
            proposed_short = np.maximum(-proposed_holdings, 0)
            requested_open = np.maximum(proposed_short - old_short, 0)
            demonstrated = np.where(
                np.isfinite(short_capacity[t]), short_capacity[t], 0.0
            )
            capacity_lots = _round_quantity_down_to_lots(
                demonstrated, short_lots
            )
            admitted_open = np.minimum(requested_open, capacity_lots)
            capped_short = old_short + admitted_open
            needs_cap = requested_open > admitted_open
            proposed_holdings[needs_cap] = -capped_short[needs_cap]

        def trade_details(new_holdings: np.ndarray) -> dict[str, object]:
            delta = new_holdings - holdings
            buys = np.maximum(delta, 0)
            sells = np.maximum(-delta, 0)
            old_long = np.maximum(holdings, 0)
            old_short = np.maximum(-holdings, 0)
            cover = np.minimum(buys, old_short)
            long_buy = buys - cover
            long_sell = np.minimum(sells, old_long)
            short_open = sells - long_sell

            sell_notional = sells.astype(np.float64) * execution_price_row
            sell_commission = _commission_fees_by_symbol(
                sell_notional,
                commission_rates,
                minimum_commission=min_commission,
                rounding=commission_rounding_rule,
            )
            sell_tax = _tax_fees_by_symbol(
                sell_notional,
                sell_tax_rates,
                rounding=tax_rounding_rule,
            )
            sell_fees = sell_commission + sell_tax
            short_sell_fraction = np.divide(
                short_open,
                sells,
                out=np.zeros(symbols, dtype=np.float64),
                where=sells > 0,
            )
            short_sell_fees = sell_fees * short_sell_fraction
            short_handling_fees = (
                short_open.astype(np.float64)
                * execution_price_row
                * short_handling_rates
            )
            long_sell_fees = sell_fees - short_sell_fees
            short_sale_net = (
                short_open.astype(np.float64) * execution_price_row
                - short_sell_fees
                - short_handling_fees
            )
            added_sale_collateral = np.maximum(short_sale_net, 0.0)
            short_sale_fee_deficit = np.minimum(short_sale_net, 0.0)
            added_margin = _round_short_margin_up_to_hundreds(
                short_open.astype(np.float64)
                * execution_price_row
                * margin_rates[t],
                short_open > 0,
            )

            buy_notional = buys.astype(np.float64) * execution_price_row
            buy_fees = _commission_fees_by_symbol(
                buy_notional,
                commission_rates,
                minimum_commission=min_commission,
                rounding=commission_rounding_rule,
            )
            cover_fraction_of_buy = np.divide(
                cover,
                buys,
                out=np.zeros(symbols, dtype=np.float64),
                where=buys > 0,
            )
            cover_fees = buy_fees * cover_fraction_of_buy
            long_buy_fees = buy_fees - cover_fees

            cover_fraction = np.divide(
                cover,
                old_short,
                out=np.zeros(symbols, dtype=np.float64),
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
                    # Once every short liability is extinguished, all old
                    # collateral is releasable, including excess retained in
                    # a previously covered symbol's original per-symbol pool.
                    released_sale = short_sale_collateral.copy()
                    released_margin = short_margin_collateral.copy()
                else:
                    remaining_liability = float(
                        np.dot(
                            remaining_short.astype(np.float64),
                            valuation_price_row,
                        )
                    )
                    required_collateral = maintenance_ratio * remaining_liability
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
            release_total = float(
                np.sum(released_sale) + np.sum(released_margin)
            )
            if (
                required_collateral is not None
                and release_total > 0.0
                and collateral_before_release >= required_collateral
            ):
                # Vector summation and per-pool subtraction can round in
                # opposite directions at an exact maintenance boundary.  If
                # that happens, reduce every release by the same next-lower
                # scalar until the represented ledger is conservatively on
                # the legal side.  This path is normally skipped and changes
                # a binding release by only a few float64 ulps.
                for _ in range(8):
                    represented_collateral = float(
                        np.sum(sale_collateral_after)
                        + np.sum(margin_collateral_after)
                    )
                    if represented_collateral >= required_collateral:
                        break
                    release_total = float(
                        np.sum(released_sale) + np.sum(released_margin)
                    )
                    if release_total <= 0.0:
                        break
                    deficit = required_collateral - represented_collateral
                    corrected_total = max(release_total - deficit, 0.0)
                    correction_scale = np.nextafter(
                        corrected_total / release_total,
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
                    # A zero release is always conservative and prevents an
                    # exotic floating-point shape from violating the contract.
                    released_sale.fill(0.0)
                    released_margin.fill(0.0)
                    sale_collateral_after = (
                        short_sale_collateral + added_sale_collateral
                    )
                    margin_collateral_after = (
                        short_margin_collateral + added_margin
                    )

            long_sell_net = (
                long_sell.astype(np.float64) * execution_price_row
                - long_sell_fees
            )
            long_buy_cost = (
                long_buy.astype(np.float64) * execution_price_row + long_buy_fees
            )
            cover_cost = (
                cover.astype(np.float64) * execution_price_row + cover_fees
            )
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
                "buy_notional": buy_notional,
                "sell_notional": sell_notional,
                "fees": float(
                    np.sum(buy_fees)
                    + np.sum(sell_fees)
                    + np.sum(short_handling_fees)
                ),
                "settlement_net": settlement_net,
                "sale_collateral": sale_collateral_after,
                "margin_collateral": margin_collateral_after,
            }

        details = trade_details(proposed_holdings)
        free_settled_cash = _capacity_for_new_payable(cash, payable, receivable)
        new_payable = max(-float(details["settlement_net"]), 0.0)
        reduced_holdings, opening_holdings = _separate_reductions_and_openings(
            holdings, proposed_holdings
        )
        if new_payable > free_settled_cash + max(
            1e-9, abs(free_settled_cash) * 1e-12
        ) and np.any(opening_holdings):
            reduced_details = trade_details(reduced_holdings)
            reduced_payable = max(-float(reduced_details["settlement_net"]), 0.0)
            if reduced_payable > free_settled_cash + max(
                1e-9, abs(free_settled_cash) * 1e-12
            ):
                proposed_holdings = reduced_holdings
                details = reduced_details
            else:
                low = 0.0
                high = 1.0
                best_holdings = reduced_holdings
                best_details = reduced_details
                # A fixed number of iterations keeps runtime O(T*S) while
                # resolving every possible float64 scale boundary.
                for _ in range(56):
                    midpoint = (low + high) * 0.5
                    candidate = _scaled_opening_holdings(
                        reduced_holdings,
                        opening_holdings,
                        midpoint,
                        lots,
                        short_lots,
                    )
                    candidate_details = trade_details(candidate)
                    candidate_payable = max(
                        -float(candidate_details["settlement_net"]), 0.0
                    )
                    if candidate_payable <= free_settled_cash + max(
                        1e-9, abs(free_settled_cash) * 1e-12
                    ):
                        low = midpoint
                        best_holdings = candidate
                        best_details = candidate_details
                    else:
                        high = midpoint
                proposed_holdings = best_holdings
                details = best_details

        holdings = proposed_holdings
        short_sale_collateral = np.asarray(
            details["sale_collateral"], dtype=np.float64
        )
        short_margin_collateral = np.asarray(
            details["margin_collateral"], dtype=np.float64
        )
        short_sale_collateral[np.abs(short_sale_collateral) <= 1e-12] = 0.0
        short_margin_collateral[np.abs(short_margin_collateral) <= 1e-12] = 0.0
        settlement_net = float(details["settlement_net"])
        fees = float(details["fees"])
        residual_action = (holdings != 0) & action_avoidance
        if np.any(residual_action):
            raise RuntimeError(
                "tw_cash official corporate-action avoidance liquidation did "
                f"not finish; time_index={t}, symbol_indices="
                f"{np.flatnonzero(residual_action).tolist()}"
            )
        residual_forced_short = forced_cover & (holdings < 0)
        if np.any(residual_forced_short):
            raise RuntimeError("tw_cash mandatory short cover did not finish")

        _enqueue_net_claim(
            settlement_net,
            payable,
            receivable,
            settlement_lag_sessions=lag,
        )
        nav_at_execution = (
            cash
            + float(np.dot(holdings, valuation_price_row))
            + float(np.sum(short_sale_collateral))
            + float(np.sum(short_margin_collateral))
            + float(np.sum(receivable))
            - float(np.sum(payable))
        )
        execution_payable = float(np.sum(payable))
        execution_receivable = float(np.sum(receivable))
        expected_nav_at_execution = nav_before_trade - fees
        if not np.isclose(
            nav_at_execution,
            expected_nav_at_execution,
            rtol=2e-12,
            atol=1e-7,
        ):
            raise RuntimeError("cash margin-short ledger identity violated")

        active_return = holdings != 0
        with np.errstate(over="ignore", invalid="ignore"):
            growth = np.exp(forward_returns[t])
        invalid_active_return = active_return & (
            ~np.isfinite(growth) | (growth <= 0.0)
        )
        if np.any(invalid_active_return):
            raise ValueError(
                "future_log_returns produces a non-finite/non-positive "
                "valuation for an executed holding; "
                f"time_index={t}, symbol_indices="
                f"{np.flatnonzero(invalid_active_return).tolist()}"
            )
        safe_growth = np.where(active_return, growth, 1.0)
        next_valuation_prices = valuation_price_row * safe_growth
        dividend_claim_by_symbol = (
            np.maximum(holdings, 0).astype(np.float64)
            * valuation_price_row
            * dividend_yields[t]
        )
        for delay in np.unique(dividend_delays[t][dividend_claim_by_symbol > 0.0]):
            due = dividend_delays[t] == delay
            receivable[int(delay) - 1] += float(
                np.sum(dividend_claim_by_symbol[due])
            )
        dividend_claim = float(np.sum(dividend_claim_by_symbol))
        nav = nav_at_execution + dividend_claim + float(
            np.dot(holdings, next_valuation_prices - valuation_price_row)
        )
        if nav <= 0.0:
            alive = False
            holdings.fill(0)
            short_sale_collateral.fill(0.0)
            short_margin_collateral.fill(0.0)
            cash = 0.0
            last_nav = 0.0
            final_weights.fill(0.0)
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        log_return, simple_return = _log_and_simple_return(nav, last_nav)
        arrays["strategy_returns"][t] = log_return
        arrays["simple_returns"][t] = simple_return
        arrays["turnover"][t] = (
            float(np.sum(details["buy_notional"]))
            + float(np.sum(details["sell_notional"]))
        ) / nav_before_trade
        arrays["settlement_net_history"][t] = settlement_net
        arrays["fee_history"][t] = fees
        weights = holdings * valuation_price_row / nav_at_execution
        final_weights = holdings * next_valuation_prices / nav
        final_weights_known = True
        _record_state(
            arrays,
            t,
            cash=cash,
            holdings=holdings,
            weights=weights,
            payable=payable,
            receivable=receivable,
            nav=nav,
            execution_nav=nav_at_execution,
            execution_payable=execution_payable,
            execution_receivable=execution_receivable,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
        )
        last_nav = nav

    return _build_result(
        arrays,
        mode="tw_cash",
        cash=cash,
        holdings=holdings,
        payable=payable,
        receivable=receivable,
        last_nav=last_nav,
        alive=alive,
        final_weights=final_weights,
        short_sale_collateral=short_sale_collateral,
        short_margin_collateral=short_margin_collateral,
    )


def run_tw_cash_integer(
    target_weights: object,
    close_prices: object,
    *,
    future_log_returns: object | None = None,
    unresolved_corporate_action_mask: object | None = None,
    cash_dividend_yield: object | None = None,
    cash_dividend_payment_delay_sessions: object | None = None,
    claim_queue_sessions: int | None = None,
    buy_fee_rates: object,
    sell_fee_rates: object,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: object | None = None,
    short_lot_sizes: object | None = None,
    tradable_mask: object | None = None,
    can_buy_mask: object | None = None,
    can_sell_mask: object | None = None,
    can_short_open_mask: object | None = None,
    force_exit_mask: object | None = None,
    force_short_cover_mask: object | None = None,
    short_margin_rate: object | None = None,
    short_margin_ratio: object | None = None,
    short_maintenance_ratio: float = 1.30,
    short_handling_fee_rate: object = 0.0,
    short_capacity_shares: object | None = None,
    daily_volumes: object | None = None,
    max_volume_participation: float = 0.0,
    max_turnover_ratio: float = 0.0,
    gross_budget: float = 1.0,
    state_advance_mask: object | None = None,
    settlement_lag_sessions: int = 2,
    initial_cash: float = 1_000_000.0,
    initial_state: TaiwanIntegerState | None = None,
) -> TaiwanIntegerBacktestResult:
    """Execute signed Taiwan cash targets; negatives are margin shorts.

    The historical non-negative path is dispatched unchanged.  Negative
    targets use Taiwan margin-short accounting: net sale proceeds and the
    margin deposit remain locked, while each symbol/order's initial margin is
    rounded upward to TWD 100 and becomes a T+2 payable.  Covering releases
    both collateral pools pro rata into the same account-level T+2 net claim,
    capped so remaining account collateral stays above the configured short
    maintenance ratio.  Clearing every short releases all old collateral.
    """

    targets = _as_matrix("target_weights", target_weights)
    state_has_short = (
        isinstance(initial_state, TaiwanIntegerState)
        and np.any(np.asarray(initial_state.holdings, dtype=np.int64) < 0)
    )
    force_cover_active = (
        force_short_cover_mask is not None
        and np.any(np.asarray(force_short_cover_mask, dtype=np.bool_))
    )
    # An explicit borrow-availability mask selects the signed contract even if
    # this particular chunk happens to contain only positive targets.  Without
    # that signal a positive prefix would take the historical long-only path
    # while the full horizon takes the signed path, breaking exact chunk
    # continuation before the first short appears.
    signed_path = bool(
        np.any(targets < 0.0)
        or state_has_short
        or force_cover_active
        or can_short_open_mask is not None
    )
    if not signed_path:
        return _run_tw_cash_integer_long_only(
            targets,
            close_prices,
            future_log_returns=future_log_returns,
            unresolved_corporate_action_mask=unresolved_corporate_action_mask,
            cash_dividend_yield=cash_dividend_yield,
            cash_dividend_payment_delay_sessions=(
                cash_dividend_payment_delay_sessions
            ),
            claim_queue_sessions=claim_queue_sessions,
            buy_fee_rates=buy_fee_rates,
            sell_fee_rates=sell_fee_rates,
            minimum_commission=minimum_commission,
            commission_rounding=commission_rounding,
            tax_rounding=tax_rounding,
            lot_sizes=lot_sizes,
            tradable_mask=tradable_mask,
            can_buy_mask=can_buy_mask,
            can_sell_mask=can_sell_mask,
            force_exit_mask=force_exit_mask,
            daily_volumes=daily_volumes,
            max_volume_participation=max_volume_participation,
            max_turnover_ratio=max_turnover_ratio,
            gross_budget=gross_budget,
            state_advance_mask=state_advance_mask,
            settlement_lag_sessions=settlement_lag_sessions,
            initial_cash=initial_cash,
            initial_state=initial_state,
        )

    if short_margin_rate is None:
        raise ValueError(
            "signed tw_cash requires short_margin_rate to be supplied explicitly"
        )
    if short_capacity_shares is None:
        raise ValueError(
            "signed tw_cash requires short_capacity_shares to be supplied "
            "explicitly"
        )
    resolved_margin_rate: object = short_margin_rate
    if short_margin_ratio is not None:
        left = _as_time_symbol_rate(
            "short_margin_rate", short_margin_rate, targets.shape
        )
        right = _as_time_symbol_rate(
            "short_margin_ratio", short_margin_ratio, targets.shape
        )
        if not np.array_equal(left, right):
            raise ValueError(
                "short_margin_rate and short_margin_ratio aliases disagree"
            )
        resolved_margin_rate = left

    return _run_tw_cash_integer_signed(
        targets,
        close_prices,
        future_log_returns=future_log_returns,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(
            cash_dividend_payment_delay_sessions
        ),
        claim_queue_sessions=claim_queue_sessions,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        minimum_commission=minimum_commission,
        commission_rounding=commission_rounding,
        tax_rounding=tax_rounding,
        lot_sizes=lot_sizes,
        short_lot_sizes=short_lot_sizes,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        can_short_open_mask=can_short_open_mask,
        force_exit_mask=force_exit_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=resolved_margin_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        short_handling_fee_rate=short_handling_fee_rate,
        short_capacity_shares=short_capacity_shares,
        daily_volumes=daily_volumes,
        max_volume_participation=max_volume_participation,
        max_turnover_ratio=max_turnover_ratio,
        gross_budget=gross_budget,
        state_advance_mask=state_advance_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        initial_cash=initial_cash,
        initial_state=initial_state,
    )


def run_tw_day_trade_integer(
    target_weights: object,
    open_prices: object,
    close_prices: object,
    *,
    buy_fee_rates: object,
    sell_fee_rates: object,
    eligibility_mask: object,
    minimum_commission: float = 0.0,
    commission_rounding: str = "none",
    tax_rounding: str = "none",
    lot_sizes: object | None = None,
    tradable_mask: object | None = None,
    can_buy_mask: object | None = None,
    can_sell_mask: object | None = None,
    can_short_open_mask: object | None = None,
    day_trade_can_buy_open_mask: object | None = None,
    day_trade_can_sell_open_mask: object | None = None,
    force_short_cover_mask: object | None = None,
    force_exit_mask: object | None = None,
    daily_volumes: object | None = None,
    max_volume_participation: float = 0.0,
    max_turnover_ratio: float = 0.0,
    state_advance_mask: object | None = None,
    settlement_lag_sessions: int = 2,
    initial_cash: float = 1_000_000.0,
    initial_state: TaiwanIntegerState | None = None,
) -> TaiwanIntegerBacktestResult:
    """Execute paired buy/sell day trades and finish every session flat.

    Positive targets buy at the open and sell at the close; negative targets
    sell at the open and buy at the close.  ``eligibility_mask`` is mandatory:
    omitting historical point-in-time eligibility must fail rather than silently
    treating today's eligible list as valid for the past.  Profit and loss are
    one account-level T+2 net claim.  A loss may exceed current settled cash and
    causes settlement default when due (or immediate ruin if NAV is non-positive).
    Minimum commission and rounding apply once per nonzero symbol-side aggregate
    order; a day trade therefore has one buy-side and one sell-side commission.
    """

    targets = _as_matrix("target_weights", target_weights)
    raw_opens = _as_execution_price_matrix("open_prices", open_prices)
    raw_closes = _as_execution_price_matrix("close_prices", close_prices)
    _validate_common(
        targets,
        raw_opens,
        settlement_lag_sessions=settlement_lag_sessions,
        long_only=False,
    )
    if raw_closes.shape != targets.shape:
        raise ValueError("close_prices must match target_weights")
    time, symbols = targets.shape
    lag = int(settlement_lag_sessions)
    commission_rates, sell_tax_rates = _split_commission_and_sell_tax_rates(
        buy_fee_rates,
        sell_fee_rates,
        symbols,
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
    lots = _as_lot_vector(lot_sizes, symbols, default=1000)
    eligible = _as_bool_matrix("eligibility_mask", eligibility_mask, targets.shape)
    tradable = _as_bool_matrix("tradable_mask", tradable_mask, targets.shape)
    can_buy = _as_bool_matrix("can_buy_mask", can_buy_mask, targets.shape) & tradable
    can_sell = _as_bool_matrix("can_sell_mask", can_sell_mask, targets.shape) & tradable
    if day_trade_can_buy_open_mask is None or day_trade_can_sell_open_mask is None:
        raise ValueError(
            "tw_day_trade requires explicit open-side buy and sell masks; "
            "close masks cannot be reused without look-ahead"
        )
    can_buy_open = _as_bool_matrix(
        "day_trade_can_buy_open_mask",
        day_trade_can_buy_open_mask,
        targets.shape,
    )
    can_sell_open = _as_bool_matrix(
        "day_trade_can_sell_open_mask",
        day_trade_can_sell_open_mask,
        targets.shape,
    )
    if np.any(targets < 0.0) and can_short_open_mask is None:
        raise ValueError(
            "negative tw_day_trade targets require an explicit can_short_open_mask"
        )
    can_short_open = (
        can_sell_open
        if can_short_open_mask is None
        else _as_bool_matrix(
            "can_short_open_mask", can_short_open_mask, targets.shape
        )
        & can_sell_open
    )
    force_short_cover = (
        np.zeros(targets.shape, dtype=np.bool_)
        if force_short_cover_mask is None
        else _as_bool_matrix(
            "force_short_cover_mask", force_short_cover_mask, targets.shape
        )
    )
    force_exit = (
        np.zeros(targets.shape, dtype=np.bool_)
        if force_exit_mask is None
        else _as_bool_matrix("force_exit_mask", force_exit_mask, targets.shape)
    )
    volumes = _as_nonnegative_matrix("daily_volumes", daily_volumes, targets.shape)
    volume_ratio = _as_nonnegative_ratio(
        "max_volume_participation", max_volume_participation
    )
    turnover_ratio = _as_nonnegative_ratio("max_turnover_ratio", max_turnover_ratio)
    if volume_ratio > 0.0 and volumes is None:
        raise ValueError(
            "daily_volumes is required when max_volume_participation is positive"
        )
    advance = _as_advance_mask(state_advance_mask, time)
    state = _copy_validate_state(
        initial_state,
        mode="tw_day_trade",
        symbols=symbols,
        settlement_lag_sessions=lag,
        claim_queue_sessions=lag,
        initial_cash=initial_cash,
    )

    cash = state.settled_cash
    holdings = state.holdings.copy()  # validated flat; retained for common state shape
    payable = state.payable_queue.copy()
    receivable = state.receivable_queue.copy()
    last_nav = state.last_nav
    alive = state.alive
    final_weights = np.zeros(symbols, dtype=np.float64)
    arrays = _empty_result_arrays(time, symbols, lag)

    for t in range(time):
        if not alive:
            _record_state(
                arrays,
                t,
                cash=0.0,
                holdings=holdings,
                weights=np.zeros(symbols, dtype=np.float64),
                payable=payable,
                receivable=receivable,
                nav=0.0,
            )
            continue
        if not advance[t]:
            nav = cash + float(np.sum(receivable)) - float(np.sum(payable))
            _record_state(
                arrays,
                t,
                cash=cash,
                holdings=holdings,
                weights=np.zeros(symbols, dtype=np.float64),
                payable=payable,
                receivable=receivable,
                nav=nav,
            )
            continue

        cash, settlement_ok = _settle_start_of_session(cash, payable, receivable)
        if not settlement_ok:
            alive = False
            cash = 0.0
            last_nav = 0.0
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        nav_before_trade = cash + float(np.sum(receivable)) - float(np.sum(payable))
        if nav_before_trade <= 0.0:
            alive = False
            cash = 0.0
            last_nav = 0.0
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        # The account is flat at both session boundaries: terminal exits block
        # all new entries, while a forced-cover event blocks only sell-first.
        entry_enabled = eligible[t] & ~force_exit[t]
        long_enabled = entry_enabled & can_buy_open[t]
        short_enabled = (
            entry_enabled
            & can_sell_open[t]
            & can_short_open[t]
            & ~force_short_cover[t]
        )
        enabled = np.where(targets[t] < 0.0, short_enabled, long_enabled)
        # Only a direction that can actually enter at the open requires an open
        # quote.  Empty lifecycle masks must not make an otherwise inactive,
        # unquoted symbol observable.  The close is deliberately not read here:
        # opening quantity must be invariant to information revealed later in
        # the session.
        open_required = enabled & (targets[t] != 0.0)
        open_row = _safe_execution_price_row(
            "open_prices", raw_opens, t, open_required
        )
        # As with the cash oracle, never convert a masked-out request through an
        # inactive quote sentinel.  Only a direction that can really submit a
        # nonzero opening order participates in share rounding and overflow
        # validation.
        quantities = np.zeros(symbols, dtype=np.int64)
        if np.any(open_required):
            quantities[open_required] = _round_quantity_down_to_lots(
                np.abs(targets[t, open_required])
                * nav_before_trade
                / open_row[open_required],
                lots[open_required],
            )

        if volume_ratio > 0.0:
            assert volumes is not None
            volume_row = volumes[t]
            valid_volume = np.isfinite(volume_row)
            # Missing prior-session volume is absence of demonstrated opening
            # capacity, not permission for an unlimited fill.
            max_quantity = np.zeros(symbols, dtype=np.int64)
            max_quantity[valid_volume] = _round_quantity_down_to_lots(
                # A forced same-day round trip executes the same quantity on
                # both sides.  The participation budget applies to aggregate
                # traded shares, so entry quantity may consume at most half.
                volume_row[valid_volume] * volume_ratio * 0.5,
                lots[valid_volume],
            )
            quantities = np.minimum(quantities, max_quantity)

        # This is an ex-ante turnover budget.  A round trip has two equal share
        # legs, so its only causal opening-time notional is 2*q*open.  Using the
        # eventual close here would leak the label into order sizing.  Realized
        # turnover below is still reported from both actual execution prices.
        if turnover_ratio > 0.0:
            opening_round_trip_notional = 2.0 * float(
                np.dot(quantities.astype(np.float64), open_row)
            )
            turnover_budget = nav_before_trade * turnover_ratio
            if (
                opening_round_trip_notional > turnover_budget + 1e-9
                and opening_round_trip_notional > 0.0
            ):
                quantities = _round_quantity_down_to_lots(
                    quantities.astype(np.float64)
                    * (turnover_budget / opening_round_trip_notional),
                    lots,
                )
        signed_positions = np.sign(targets[t]).astype(np.int64) * quantities

        long_mask = signed_positions > 0
        short_mask = signed_positions < 0
        absolute_quantities = np.abs(signed_positions)
        blocked_close = (long_mask & ~can_sell[t]) | (short_mask & ~can_buy[t])
        if np.any(blocked_close):
            blocked_symbols = np.flatnonzero(blocked_close).tolist()
            raise RuntimeError(
                "tw_day_trade mandatory close leg is unavailable after open entry; "
                f"time_index={t}, symbol_indices={blocked_symbols}. Daily data "
                "cannot carry or price the unresolved position exactly."
            )
        close_required = signed_positions != 0
        close_row = _safe_execution_price_row(
            "close_prices", raw_closes, t, close_required
        )
        # Long: buy at open, sell at close.  Short: sell at open, buy at close.
        buy_notional_by_symbol = np.where(
            long_mask,
            absolute_quantities * open_row,
            np.where(short_mask, absolute_quantities * close_row, 0.0),
        )
        sell_notional_by_symbol = np.where(
            long_mask,
            absolute_quantities * close_row,
            np.where(short_mask, absolute_quantities * open_row, 0.0),
        )
        buy_fees_by_symbol = _commission_fees_by_symbol(
            buy_notional_by_symbol,
            commission_rates,
            minimum_commission=min_commission,
            rounding=commission_rounding_rule,
        )
        sell_fees_by_symbol = _commission_fees_by_symbol(
            sell_notional_by_symbol,
            commission_rates,
            minimum_commission=min_commission,
            rounding=commission_rounding_rule,
        ) + _tax_fees_by_symbol(
            sell_notional_by_symbol,
            sell_tax_rates,
            rounding=tax_rounding_rule,
        )
        buy_cost = float(np.sum(buy_notional_by_symbol + buy_fees_by_symbol))
        sell_net = float(np.sum(sell_notional_by_symbol - sell_fees_by_symbol))
        settlement_net = sell_net - buy_cost
        fees = float(np.sum(buy_fees_by_symbol) + np.sum(sell_fees_by_symbol))
        _enqueue_net_claim(settlement_net, payable, receivable)

        nav = cash + float(np.sum(receivable)) - float(np.sum(payable))
        expected_nav = nav_before_trade + settlement_net
        if not np.isclose(nav, expected_nav, rtol=2e-12, atol=1e-7):
            raise RuntimeError("day-trade ledger asset-liability identity violated")
        if nav <= 0.0:
            alive = False
            cash = 0.0
            last_nav = 0.0
            _record_ruin(arrays, t, payable=payable, receivable=receivable)
            continue

        log_return, simple_return = _log_and_simple_return(nav, last_nav)
        arrays["strategy_returns"][t] = log_return
        arrays["simple_returns"][t] = simple_return
        arrays["turnover"][t] = (
            float(np.sum(buy_notional_by_symbol) + np.sum(sell_notional_by_symbol))
            / nav_before_trade
        )
        arrays["positions"][t] = signed_positions
        arrays["weights"][t] = signed_positions * open_row / nav_before_trade
        arrays["settlement_net_history"][t] = settlement_net
        arrays["fee_history"][t] = fees
        arrays["settled_cash_history"][t] = cash
        arrays["payable_history"][t] = float(np.sum(payable))
        arrays["receivable_history"][t] = float(np.sum(receivable))
        arrays["payable_queue_history"][t] = payable
        arrays["receivable_queue_history"][t] = receivable
        arrays["execution_nav_history"][t] = nav_before_trade
        arrays["nav_history"][t] = nav
        # End-of-session holdings remain zero by construction.
        last_nav = nav

    return _build_result(
        arrays,
        mode="tw_day_trade",
        cash=cash,
        holdings=holdings,
        payable=payable,
        receivable=receivable,
        last_nav=last_nav,
        alive=alive,
        final_weights=final_weights,
    )
