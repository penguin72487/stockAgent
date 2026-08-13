"""Canonical day-session execution for TX, MTX, and TMF.

The strategy owns one signed Taiwan-index exposure.  The model does not choose
three independent directions for products with the same underlying.  The
continuous path is used by the differentiable objective; the integer path
turns that exposure into a same-direction basket of TX/MTX/TMF contracts and
flattens the complete basket in the same general trading session.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real
from typing import Final

import numpy as np
import torch

from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    TaiwanIndexFuturesDaySession,
)


# v4 charges the configured transaction tax on the sale leg only.  A long
# round trip sells at the close; a short round trip sells at the open.  v3 and
# earlier incorrectly charged both the buy and sell legs.
TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION: Final[int] = 4
TW_INDEX_FUTURES_SELL_TAX_RATE: Final[float] = 0.0002
# Compatibility export.  The value is a sell-side rate, not a two-sided rate.
TW_INDEX_FUTURES_TAX_RATE: Final[float] = TW_INDEX_FUTURES_SELL_TAX_RATE
# Current TAIFEX exchange + clearing charges per contract per side.  A broker's
# negotiated commission is separate and defaults to zero below.
TAIFEX_FIXED_FEES_PER_SIDE_TWD: Final[dict[str, float]] = {
    "TX": 20.0,
    "MTX": 12.5,
    "TMF": 8.0,
}


@dataclass(frozen=True, slots=True)
class FuturesCostSchedule:
    # Tax applies only when a contract is sold.  Fixed fees and slippage remain
    # per side and are therefore incurred twice for a daily-flat round trip.
    tax_rate: float = TW_INDEX_FUTURES_SELL_TAX_RATE
    exchange_and_clearing_fee_per_side_twd: tuple[float, ...] = (20.0, 12.5, 8.0)
    broker_fee_per_side_twd: tuple[float, ...] = (0.0, 0.0, 0.0)
    slippage_points_per_side: tuple[float, ...] = (0.0, 0.0, 0.0)
    basket_fee_penalty: float = 1.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tax_rate, Real)
            or isinstance(self.tax_rate, bool)
            or not math.isfinite(float(self.tax_rate))
            or float(self.tax_rate) < 0.0
        ):
            raise ValueError("tax_rate must be a finite non-negative real")
        for name in (
            "exchange_and_clearing_fee_per_side_twd",
            "broker_fee_per_side_twd",
            "slippage_points_per_side",
        ):
            values = tuple(getattr(self, name))
            if len(values) != len(TAIFEX_INDEX_FUTURES_PRODUCTS):
                raise ValueError(f"{name} must contain TX, MTX, and TMF values")
            if any(
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(value) < 0.0
                for value in values
            ):
                raise ValueError(f"{name} must contain finite non-negative values")
        if (
            not isinstance(self.basket_fee_penalty, Real)
            or isinstance(self.basket_fee_penalty, bool)
            or not math.isfinite(float(self.basket_fee_penalty))
            or float(self.basket_fee_penalty) < 0.0
        ):
            raise ValueError(
                "basket_fee_penalty must be a finite non-negative real"
            )

    @property
    def fixed_fee_per_side_twd(self) -> np.ndarray:
        return np.asarray(
            self.exchange_and_clearing_fee_per_side_twd,
            dtype=np.float64,
        ) + np.asarray(self.broker_fee_per_side_twd, dtype=np.float64)


@dataclass(frozen=True, slots=True)
class FuturesContinuousBacktest:
    strategy_returns: torch.Tensor
    gross_returns: torch.Tensor
    cost_returns: torch.Tensor
    requested_exposure: torch.Tensor
    executed_exposure: torch.Tensor
    turnovers: torch.Tensor
    tradable_mask: torch.Tensor


@dataclass(frozen=True, slots=True)
class FuturesContractBasket:
    quantities: np.ndarray
    target_notional: float
    actual_notional: float
    tracking_error: float
    estimated_round_trip_cost: float


@dataclass(frozen=True, slots=True)
class FuturesIntegerBacktest:
    dates: np.ndarray
    products: tuple[str, ...]
    requested_exposure: np.ndarray
    executed_exposure: np.ndarray
    contract_quantities: np.ndarray
    gross_pnl_twd: np.ndarray
    fees_twd: np.ndarray
    tax_twd: np.ndarray
    slippage_twd: np.ndarray
    net_pnl_twd: np.ndarray
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    equity: np.ndarray
    alive: np.ndarray
    contract_months: np.ndarray


def _finite_nonnegative(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite non-negative real")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be a finite non-negative real")
    return result


def run_tw_index_futures_day_continuous(
    exposure: torch.Tensor,
    reference_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    round_trip_cost_rate: float = 0.00004,
    max_abs_exposure: float = 1.0,
) -> FuturesContinuousBacktest:
    """Run the differentiable same-session exposure contract.

    A row is flat before the open and after the close, so turnover is twice the
    absolute executed exposure.  Missing or non-finite reference returns are an
    executor-only no-trade fact and never become a zero-return open position.
    """

    cost_rate = _finite_nonnegative(
        "round_trip_cost_rate",
        round_trip_cost_rate,
    )
    exposure_limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if exposure_limit <= 0.0:
        raise ValueError("max_abs_exposure must be positive")
    if exposure.ndim != 1:
        raise ValueError(f"exposure must have shape [T], got {tuple(exposure.shape)}")
    if tuple(reference_log_returns.shape) != tuple(exposure.shape):
        raise ValueError("reference_log_returns must have shape [T]")
    if tuple(tradable_mask.shape) != tuple(exposure.shape):
        raise ValueError("tradable_mask must have shape [T]")

    requested = torch.nan_to_num(
        exposure,
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    ).clamp(min=-exposure_limit, max=exposure_limit)
    valid = (
        tradable_mask.to(device=exposure.device, dtype=torch.bool)
        & torch.isfinite(reference_log_returns)
    )
    executed = torch.where(valid, requested, torch.zeros_like(requested))
    simple_returns = torch.expm1(
        torch.where(
            valid,
            reference_log_returns.to(
                device=exposure.device,
                dtype=torch.float32,
            ),
            torch.zeros_like(executed, dtype=torch.float32),
        )
    )
    executed_f32 = executed.to(dtype=torch.float32)
    gross = executed_f32 * simple_returns
    costs = executed_f32.abs() * float(cost_rate)
    strategy = gross - costs
    return FuturesContinuousBacktest(
        strategy_returns=strategy,
        gross_returns=gross,
        cost_returns=costs,
        requested_exposure=requested,
        executed_exposure=executed,
        turnovers=executed_f32.abs() * 2.0,
        tradable_mask=valid,
    )


def tw_index_futures_log_utility_loss(
    exposure: torch.Tensor,
    reference_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    round_trip_cost_rate: float = 0.00004,
    max_abs_exposure: float = 1.0,
) -> torch.Tensor:
    result = run_tw_index_futures_day_continuous(
        exposure,
        reference_log_returns,
        tradable_mask,
        round_trip_cost_rate=round_trip_cost_rate,
        max_abs_exposure=max_abs_exposure,
    )
    valid = result.tradable_mask
    if not bool(valid.any().detach().cpu().item()):
        return exposure.sum() * 0.0
    safe_returns = torch.clamp(
        result.strategy_returns[valid],
        min=-1.0 + 1e-7,
    )
    return -torch.log1p(safe_returns).mean()


def _candidate_counts(
    target_notional: float,
    contract_notionals: np.ndarray,
    available: np.ndarray,
) -> list[tuple[int, ...]]:
    """Enumerate a bounded exact-sizing neighborhood from large to small."""

    notionals = np.asarray(contract_notionals, dtype=np.float64)
    tradable = np.asarray(available, dtype=bool)
    if notionals.shape != (3,) or tradable.shape != (3,):
        raise ValueError("contract_notionals and available must have shape [3]")
    candidates: list[tuple[int, ...]] = []

    def around(value: float, radius: int) -> list[int]:
        floor_value = max(0, int(math.floor(value)))
        values = {0}
        for candidate in range(
            max(0, floor_value - radius),
            floor_value + radius + 2,
        ):
            values.add(candidate)
        return sorted(values)

    tx_counts = (
        around(target_notional / notionals[0], 2)
        if tradable[0]
        else [0]
    )
    for tx_count in tx_counts:
        tx_notional = tx_count * notionals[0]
        residual_after_tx = max(0.0, target_notional - tx_notional)
        mtx_counts = (
            around(residual_after_tx / notionals[1], 5)
            if tradable[1]
            else [0]
        )
        for mtx_count in mtx_counts:
            used = tx_notional + mtx_count * notionals[1]
            residual = max(0.0, target_notional - used)
            tmf_counts = (
                around(residual / notionals[2], 3)
                if tradable[2]
                else [0]
            )
            for tmf_count in tmf_counts:
                candidates.append((tx_count, mtx_count, tmf_count))
    return candidates


def select_tw_index_futures_contract_basket(
    target_notional: float,
    open_prices: np.ndarray,
    tradable_mask: np.ndarray,
    *,
    cost_schedule: FuturesCostSchedule | None = None,
    max_notional: float | None = None,
) -> FuturesContractBasket:
    """Choose non-negative TX/MTX/TMF counts for an absolute target notional."""

    target = _finite_nonnegative("target_notional", target_notional)
    maximum = target if max_notional is None else _finite_nonnegative(
        "max_notional",
        max_notional,
    )
    schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
    opens = np.asarray(open_prices, dtype=np.float64)
    tradable = np.asarray(tradable_mask, dtype=bool)
    if opens.shape != (3,) or tradable.shape != (3,):
        raise ValueError("open_prices and tradable_mask must have shape [3]")
    multipliers = np.asarray(
        [
            TAIFEX_INDEX_FUTURES_MULTIPLIERS[product]
            for product in TAIFEX_INDEX_FUTURES_PRODUCTS
        ],
        dtype=np.float64,
    )
    valid = tradable & np.isfinite(opens) & (opens > 0.0)
    notionals = opens * multipliers
    if target == 0.0 or not bool(valid.any()):
        return FuturesContractBasket(
            quantities=np.zeros(3, dtype=np.int64),
            target_notional=target,
            actual_notional=0.0,
            tracking_error=target,
            estimated_round_trip_cost=0.0,
        )
    # Invalid products are already excluded by ``valid`` in the enumerator.
    # Give them a finite zero placeholder rather than infinity: the unavailable
    # branch has an exact count of zero, and ``0 * inf`` would otherwise emit a
    # warning/NaN before that candidate is rejected.
    safe_notionals = np.where(valid, notionals, 0.0)
    fixed = schedule.fixed_fee_per_side_twd
    slippage = np.asarray(
        schedule.slippage_points_per_side,
        dtype=np.float64,
    )

    best_key: tuple[float, float, int, int, int] | None = None
    best_counts = np.zeros(3, dtype=np.int64)
    best_notional = 0.0
    best_cost = 0.0
    for raw_counts in _candidate_counts(target, safe_notionals, valid):
        counts = np.asarray(raw_counts, dtype=np.int64)
        if bool(np.any((counts > 0) & ~valid)):
            continue
        actual = float(np.dot(counts, np.where(valid, notionals, 0.0)))
        if actual > maximum + 1e-9:
            continue
        fixed_round_trip = float(np.dot(counts, fixed * 2.0))
        # Direction and closing prices are not known while choosing the basket
        # at the open.  Use one opening sale-notional tax as the symmetric
        # sizing estimate; the ledger below recomputes the exact long/short
        # sale price.
        tax_round_trip = float(
            np.dot(
                counts,
                np.where(valid, notionals, 0.0) * float(schedule.tax_rate),
            )
        )
        slippage_round_trip = float(np.dot(counts, slippage * multipliers * 2.0))
        estimated_cost = (
            fixed_round_trip + tax_round_trip + slippage_round_trip
        )
        tracking_error = abs(actual - target)
        objective = tracking_error + float(
            schedule.basket_fee_penalty
        ) * estimated_cost
        # Ties prefer lower costs/fewer contracts, then larger contracts.
        key = (
            objective,
            estimated_cost,
            int(counts.sum()),
            -int(counts[0]),
            -int(counts[1]),
        )
        if best_key is None or key < best_key:
            best_key = key
            best_counts = counts
            best_notional = actual
            best_cost = estimated_cost
    return FuturesContractBasket(
        quantities=best_counts,
        target_notional=target,
        actual_notional=best_notional,
        tracking_error=abs(best_notional - target),
        estimated_round_trip_cost=best_cost,
    )


def run_tw_index_futures_day_integer(
    exposure: np.ndarray,
    market: TaiwanIndexFuturesDaySession,
    *,
    initial_capital: float,
    max_abs_exposure: float = 1.0,
    cost_schedule: FuturesCostSchedule | None = None,
) -> FuturesIntegerBacktest:
    """Execute and flatten an integer TX/MTX/TMF basket every day."""

    initial_equity = _finite_nonnegative("initial_capital", initial_capital)
    if initial_equity <= 0.0:
        raise ValueError("initial_capital must be positive")
    exposure_limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if exposure_limit <= 0.0:
        raise ValueError("max_abs_exposure must be positive")
    requested = np.nan_to_num(
        np.asarray(exposure, dtype=np.float64),
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    )
    requested = np.clip(requested, -exposure_limit, exposure_limit)
    rows = int(market.dates.size)
    if requested.shape != (rows,):
        raise ValueError(f"exposure must have shape [{rows}]")
    if market.products != TAIFEX_INDEX_FUTURES_PRODUCTS:
        raise ValueError(
            "integer execution requires products ordered as TX, MTX, TMF"
        )
    schedule = FuturesCostSchedule() if cost_schedule is None else cost_schedule
    multipliers = market.multipliers.astype(np.float64, copy=False)
    if tuple(multipliers.tolist()) != (200.0, 50.0, 10.0):
        raise ValueError("market multipliers must be TX=200, MTX=50, TMF=10")

    quantities = np.zeros((rows, 3), dtype=np.int64)
    executed = np.zeros(rows, dtype=np.float64)
    gross_pnl = np.zeros(rows, dtype=np.float64)
    fixed_fees = np.zeros(rows, dtype=np.float64)
    taxes = np.zeros(rows, dtype=np.float64)
    slippage_costs = np.zeros(rows, dtype=np.float64)
    net_pnl = np.zeros(rows, dtype=np.float64)
    strategy_returns = np.zeros(rows, dtype=np.float64)
    turnovers = np.zeros(rows, dtype=np.float64)
    equity = np.zeros(rows, dtype=np.float64)
    alive = np.ones(rows, dtype=bool)
    current_equity = float(initial_equity)
    still_alive = True
    fixed_per_side = schedule.fixed_fee_per_side_twd
    slip_points = np.asarray(
        schedule.slippage_points_per_side,
        dtype=np.float64,
    )

    for row in range(rows):
        if not still_alive or requested[row] == 0.0:
            equity[row] = current_equity
            alive[row] = still_alive
            continue
        valid_products = np.asarray(market.tradable_mask[row], dtype=bool)
        if not bool(valid_products.any()):
            equity[row] = current_equity
            alive[row] = still_alive
            continue
        target_abs = abs(float(requested[row])) * current_equity
        basket = select_tw_index_futures_contract_basket(
            target_abs,
            market.open_prices[row],
            valid_products,
            cost_schedule=schedule,
            max_notional=exposure_limit * current_equity,
        )
        direction = 1 if requested[row] > 0.0 else -1
        signed_counts = basket.quantities * direction
        quantities[row] = signed_counts
        if not bool(basket.quantities.any()):
            equity[row] = current_equity
            alive[row] = still_alive
            continue

        opens = market.open_prices[row]
        closes = market.close_prices[row]
        # A product with zero selected contracts has no cash flow. Restrict
        # every price-dependent calculation to active legs so unavailable
        # products cannot leak NaN through IEEE ``0 * NaN`` arithmetic. An
        # active leg with a broken price is instead a hard data-contract error;
        # silently converting it to zero PnL would fabricate an executable
        # round trip.
        active = signed_counts != 0
        active_prices_valid = (
            np.isfinite(opens[active])
            & np.isfinite(closes[active])
            & (opens[active] > 0.0)
            & (closes[active] > 0.0)
        )
        if not bool(active_prices_valid.all()):
            invalid_products = [
                market.products[index]
                for index in np.flatnonzero(active)
                if not (
                    math.isfinite(float(opens[index]))
                    and math.isfinite(float(closes[index]))
                    and float(opens[index]) > 0.0
                    and float(closes[index]) > 0.0
                )
            ]
            raise ValueError(
                "selected futures contracts require finite positive open/close "
                f"prices on {market.dates[row]}: {', '.join(invalid_products)}"
            )
        active_counts = signed_counts[active].astype(np.float64)
        active_absolute_counts = np.abs(active_counts)
        active_multipliers = multipliers[active]
        active_opens = opens[active]
        active_closes = closes[active]
        gross = float(
            np.dot(
                active_counts * active_multipliers,
                active_closes - active_opens,
            )
        )
        fees = float(
            np.dot(active_absolute_counts, fixed_per_side[active] * 2.0)
        )
        # Long: buy open, sell close. Short: sell open, buy close. Transaction
        # tax is charged exactly once, on the actual sale leg.
        sale_prices = np.where(active_counts > 0.0, active_closes, active_opens)
        tax = float(
            np.dot(
                active_absolute_counts * active_multipliers,
                sale_prices * float(schedule.tax_rate),
            )
        )
        slip = float(
            np.dot(
                active_absolute_counts,
                slip_points[active] * active_multipliers * 2.0,
            )
        )
        net = gross - fees - tax - slip
        gross_pnl[row] = gross
        fixed_fees[row] = fees
        taxes[row] = tax
        slippage_costs[row] = slip
        net_pnl[row] = net
        strategy_returns[row] = net / current_equity
        executed[row] = direction * basket.actual_notional / current_equity
        turnovers[row] = 2.0 * abs(executed[row])
        next_equity = current_equity + net
        if not math.isfinite(next_equity) or next_equity <= 0.0:
            current_equity = 0.0
            still_alive = False
        else:
            current_equity = next_equity
        equity[row] = current_equity
        alive[row] = still_alive

    return FuturesIntegerBacktest(
        dates=np.asarray(market.dates, dtype="datetime64[D]").copy(),
        products=market.products,
        requested_exposure=requested,
        executed_exposure=executed,
        contract_quantities=quantities,
        gross_pnl_twd=gross_pnl,
        fees_twd=fixed_fees,
        tax_twd=taxes,
        slippage_twd=slippage_costs,
        net_pnl_twd=net_pnl,
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        equity=equity,
        alive=alive,
        contract_months=market.contract_months.copy(),
    )


__all__ = [
    "FuturesContinuousBacktest",
    "FuturesContractBasket",
    "FuturesCostSchedule",
    "FuturesIntegerBacktest",
    "TAIFEX_FIXED_FEES_PER_SIDE_TWD",
    "TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION",
    "TW_INDEX_FUTURES_SELL_TAX_RATE",
    "TW_INDEX_FUTURES_TAX_RATE",
    "run_tw_index_futures_day_continuous",
    "run_tw_index_futures_day_integer",
    "select_tw_index_futures_contract_basket",
    "tw_index_futures_log_utility_loss",
]
