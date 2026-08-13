"""Daily-flat E1..E6 futures plus causal signed-TXO candidate execution."""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real
from typing import Final

import numpy as np
import torch

from stockagent.backtest.tw_index_futures import (
    FuturesCostSchedule,
    select_tw_index_futures_contract_basket,
)
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    TAIFEX_OPTION_CANDIDATE_CAPACITY,
    TaiwanIndexDerivativeDayCandidates,
)
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    TaiwanIndexFuturesDaySession,
)


TW_INDEX_DERIVATIVES_DAY_BACKTEST_CONTRACT_VERSION: Final[int] = 5


@dataclass(frozen=True, slots=True)
class OptionDayCostSchedule:
    fixed_fee_per_contract_per_side_twd: float = 22.0
    transaction_tax_rate: float = 0.0002
    slippage_points_per_side: float = 0.5

    def __post_init__(self) -> None:
        for name in (
            "fixed_fee_per_contract_per_side_twd",
            "transaction_tax_rate",
            "slippage_points_per_side",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(value) < 0.0
            ):
                raise ValueError(f"{name} must be finite and non-negative")


@dataclass(frozen=True, slots=True)
class DerivativesDayContinuousBacktest:
    strategy_returns: torch.Tensor
    requested_actions: torch.Tensor
    executed_actions: torch.Tensor
    turnovers: torch.Tensor
    alive: torch.Tensor


@dataclass(frozen=True, slots=True)
class DerivativesDayIntegerBacktest:
    dates: np.ndarray
    requested_actions: np.ndarray
    executed_actions: np.ndarray
    futures_contract_months: np.ndarray
    futures_contract_quantities: np.ndarray
    option_contract_quantities: np.ndarray
    option_sparse_indices: np.ndarray
    gross_pnl_twd: np.ndarray
    fees_twd: np.ndarray
    tax_twd: np.ndarray
    slippage_twd: np.ndarray
    net_pnl_twd: np.ndarray
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    equity: np.ndarray
    alive: np.ndarray
    terminal_flat: np.ndarray


def _normalize_actions_torch(
    actions: torch.Tensor,
    maximum_capital_fraction: float,
    *,
    allow_option_short: bool,
) -> torch.Tensor:
    if (
        actions.ndim != 2
        or int(actions.size(1)) != TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4
    ):
        raise ValueError(
            "derivative actions must have shape "
            f"[T,{TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4}], got {tuple(actions.shape)}"
        )
    maximum = float(maximum_capital_fraction)
    if not math.isfinite(maximum) or not 0.0 < maximum <= 1.0:
        raise ValueError("maximum_capital_fraction must be in (0, 1]")
    clean = torch.nan_to_num(actions, nan=0.0, posinf=maximum, neginf=-maximum)
    futures = clean[:, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS]
    options = clean[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:]
    if not bool(allow_option_short):
        options = options.clamp_min(0.0)
    clean = torch.cat((futures, options), dim=-1)
    gross = futures.abs().sum(dim=-1, keepdim=True) + options.abs().sum(
        dim=-1, keepdim=True
    )
    scale = torch.clamp(
        maximum / gross.clamp_min(torch.finfo(clean.dtype).eps), max=1.0
    )
    return clean * scale


def run_tw_index_derivatives_day_continuous(
    actions: torch.Tensor,
    derivative_simple_returns: torch.Tensor,
    *,
    futures_round_trip_cost_rate: float,
    maximum_capital_fraction: float = 0.98,
) -> DerivativesDayContinuousBacktest:
    """Differentiable same-day P&L with ruin handled only at portfolio NAV.

    Option inputs are exact unbounded simple returns after option costs.  They
    are never transformed or clipped per leg; only the aggregate portfolio is
    floored at -100% when the account is ruined.
    """

    directional = derivative_simple_returns.dim() == 3
    requested = _normalize_actions_torch(
        actions,
        maximum_capital_fraction,
        allow_option_short=directional,
    )
    if tuple(derivative_simple_returns.shape) not in {
        tuple(requested.shape),
        (*tuple(requested.shape), 2),
    }:
        raise ValueError(
            "derivative_simple_returns must have shape [T,4102] or "
            "directional [T,4102,2]"
        )
    cost = float(futures_round_trip_cost_rate)
    if not math.isfinite(cost) or cost < 0.0:
        raise ValueError("futures_round_trip_cost_rate must be non-negative")

    raw_returns = derivative_simple_returns.to(
        device=actions.device, dtype=torch.float32
    )
    if directional:
        long_returns = raw_returns[..., 0]
        short_returns = raw_returns[..., 1]
        valid = torch.where(
            requested >= 0.0,
            torch.isfinite(long_returns),
            torch.isfinite(short_returns),
        )
        executed = torch.where(valid, requested, torch.zeros_like(requested))
        safe_long = torch.where(
            torch.isfinite(long_returns), long_returns, torch.zeros_like(long_returns)
        )
        safe_short = torch.where(
            torch.isfinite(short_returns),
            short_returns,
            torch.zeros_like(short_returns),
        )
        capital_returns = (
            executed.clamp_min(0.0) * safe_long
            + (-executed).clamp_min(0.0) * safe_short
        )
    else:
        valid = torch.isfinite(raw_returns)
        executed = torch.where(valid, requested, torch.zeros_like(requested))
        safe_returns = torch.where(valid, raw_returns, torch.zeros_like(raw_returns))
        capital_returns = executed * safe_returns
    futures = executed[:, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS].float()
    simple_raw = (
        capital_returns[:, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS].sum(dim=-1)
        - futures.abs().sum(dim=-1) * cost
        + capital_returns[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:].sum(dim=-1)
    )
    rows = int(requested.size(0))
    if rows:
        alive_before = torch.cat(
            (
                torch.ones(1, dtype=torch.bool, device=actions.device),
                torch.cumprod((simple_raw[:-1] > -1.0).to(torch.int64), dim=0).bool(),
            )
        )
        executed = torch.where(
            alive_before.unsqueeze(-1), executed, torch.zeros_like(executed)
        )
        strategy = torch.where(
            alive_before,
            simple_raw.clamp_min(-1.0),
            torch.zeros_like(simple_raw),
        )
        alive = torch.cumprod((strategy > -1.0).to(torch.int64), dim=0).bool()
    else:
        strategy = simple_raw
        alive = torch.empty(0, dtype=torch.bool, device=actions.device)
    gross_exposure = executed.abs().sum(dim=-1)
    return DerivativesDayContinuousBacktest(
        strategy_returns=strategy,
        requested_actions=requested,
        executed_actions=executed,
        turnovers=2.0 * gross_exposure,
        alive=alive,
    )


def run_tw_index_derivatives_day_integer(
    actions: np.ndarray,
    futures_market: TaiwanIndexFuturesDaySession,
    candidates: TaiwanIndexDerivativeDayCandidates,
    *,
    initial_capital: float,
    maximum_capital_fraction: float = 0.98,
    futures_cost_schedule: FuturesCostSchedule | None = None,
    option_cost_schedule: OptionDayCostSchedule | None = None,
) -> DerivativesDayIntegerBacktest:
    """Resolve every relative slot to a concrete contract and finish flat."""

    capital = float(initial_capital)
    maximum = float(maximum_capital_fraction)
    if not math.isfinite(capital) or capital <= 0.0:
        raise ValueError("initial_capital must be finite and positive")
    if not math.isfinite(maximum) or not 0.0 < maximum <= 1.0:
        raise ValueError("maximum_capital_fraction must be in (0, 1]")
    if not np.array_equal(futures_market.dates, candidates.dates):
        raise ValueError("futures and derivative candidate calendars must align")
    requested = np.nan_to_num(
        np.asarray(actions, dtype=np.float64),
        nan=0.0,
        posinf=maximum,
        neginf=-maximum,
    )
    rows = int(futures_market.dates.size)
    if requested.shape != (rows, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4):
        raise ValueError(
            f"actions must have shape [{rows},{TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4}]"
        )
    if not bool(candidates.allow_option_short):
        requested[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:] = np.maximum(
            requested[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:], 0.0
        )
    gross = np.abs(requested).sum(axis=-1)
    scale = np.minimum(1.0, maximum / np.maximum(gross, np.finfo(np.float64).eps))
    requested *= scale[:, None]
    candidate_mask = candidates.candidate_mask()
    requested = np.where(candidate_mask, requested, 0.0)

    future_schedule = futures_cost_schedule or FuturesCostSchedule()
    option_schedule = option_cost_schedule or OptionDayCostSchedule()
    executed = np.zeros_like(requested)
    future_counts = np.zeros(
        (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS, len(futures_market.products)),
        dtype=np.int64,
    )
    option_counts = np.zeros((rows, TAIFEX_OPTION_CANDIDATE_CAPACITY), dtype=np.int64)
    option_sparse_out = np.asarray(candidates.option_sparse_indices, dtype=np.int32).copy()
    gross_pnl = np.zeros(rows)
    fees = np.zeros(rows)
    taxes = np.zeros(rows)
    slippage = np.zeros(rows)
    net_pnl = np.zeros(rows)
    returns = np.zeros(rows)
    turnovers = np.zeros(rows)
    equity = np.zeros(rows)
    alive = np.ones(rows, dtype=bool)
    terminal_flat = np.ones(rows, dtype=bool)
    still_alive = True
    current_equity = capital

    (
        market_tenor_months,
        tenor_open,
        _tenor_high,
        _tenor_low,
        tenor_close,
        _tenor_volume,
        _tenor_returns,
        tenor_tradable,
    ) = futures_market.require_tenor_panel()
    multipliers = futures_market.multipliers.astype(np.float64, copy=False)
    future_fixed = future_schedule.fixed_fee_per_side_twd
    future_slip = np.asarray(
        future_schedule.slippage_points_per_side, dtype=np.float64
    )
    chain = candidates.source_chain
    option_multiplier = float(chain.multiplier)
    option_open = np.asarray(chain.open_prices, dtype=np.float64)
    option_close = np.asarray(chain.close_prices, dtype=np.float64)
    option_executable = np.asarray(chain.executable, dtype=bool)
    option_simple = np.asarray(candidates.option_simple_returns, dtype=np.float64)
    option_short_simple = np.asarray(
        candidates.option_short_simple_returns, dtype=np.float64
    )
    option_short_margin = np.asarray(
        candidates.option_short_initial_margins, dtype=np.float64
    )

    for row in range(rows):
        if not still_alive:
            equity[row] = current_equity
            alive[row] = False
            continue
        current_month_to_tenor = {
            str(month): tenor
            for tenor, month in enumerate(np.asarray(market_tenor_months[row]))
            if str(month)
        }
        for slot in range(TAIFEX_INDEX_FUTURES_TENOR_SLOTS):
            target_weight = float(requested[row, slot])
            month = str(candidates.futures_contract_months[row, slot])
            tenor = current_month_to_tenor.get(month)
            if tenor is None or target_weight == 0.0:
                continue
            target_notional = abs(target_weight) * current_equity
            basket = select_tw_index_futures_contract_basket(
                target_notional,
                tenor_open[row, tenor],
                tenor_tradable[row, tenor],
                cost_schedule=future_schedule,
                max_notional=target_notional,
            )
            direction = 1 if target_weight > 0.0 else -1
            signed_counts = basket.quantities.astype(np.int64) * direction
            future_counts[row, slot] = signed_counts
            executed[row, slot] = direction * basket.actual_notional / current_equity
            active = signed_counts != 0
            if not bool(active.any()):
                continue
            opens = np.asarray(tenor_open[row, tenor, active], dtype=np.float64)
            closes = np.asarray(tenor_close[row, tenor, active], dtype=np.float64)
            if not bool(
                (np.isfinite(opens) & np.isfinite(closes) & (opens > 0.0) & (closes > 0.0)).all()
            ):
                raise ValueError(
                    f"active futures leg has invalid price on {futures_market.dates[row]}"
                )
            signed = signed_counts[active].astype(np.float64)
            absolute = np.abs(signed)
            active_mult = multipliers[active]
            gross_pnl[row] += float(np.dot(signed * active_mult, closes - opens))
            fees[row] += float(np.dot(absolute, future_fixed[active] * 2.0))
            sale_prices = np.where(signed > 0.0, closes, opens)
            taxes[row] += float(
                np.dot(absolute * active_mult, sale_prices * future_schedule.tax_rate)
            )
            slippage[row] += float(
                np.dot(absolute, future_slip[active] * active_mult * 2.0)
            )

        active_option_slots = np.flatnonzero(
            (requested[row, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:] != 0.0)
            & (option_sparse_out[row] >= 0)
        )
        for slot_value in active_option_slots:
            slot = int(slot_value)
            target_weight = float(
                requested[row, TAIFEX_INDEX_FUTURES_TENOR_SLOTS + slot]
            )
            sparse_index = int(option_sparse_out[row, slot])
            short_side = target_weight < 0.0
            directional_return = (
                float(option_short_simple[row, slot])
                if short_side
                else float(option_simple[row, slot])
            )
            if (
                sparse_index < 0
                or not math.isfinite(directional_return)
                or not bool(option_executable[sparse_index])
            ):
                continue
            open_price = float(option_open[sparse_index])
            close_price = float(option_close[sparse_index])
            if not (
                math.isfinite(open_price)
                and open_price > 0.0
                and math.isfinite(close_price)
                and close_price > 0.0
            ):
                raise ValueError("executable option candidate has invalid prices")
            premium = open_price * option_multiplier
            capital_per_contract = (
                float(option_short_margin[row, slot])
                if short_side
                else premium
            )
            if not math.isfinite(capital_per_contract) or capital_per_contract <= 0.0:
                raise ValueError("active option leg has invalid capital requirement")
            absolute_count = int(
                math.floor(abs(target_weight) * current_equity / capital_per_contract)
            )
            if absolute_count <= 0:
                continue
            count = -absolute_count if short_side else absolute_count
            option_counts[row, slot] = count
            executed[row, TAIFEX_INDEX_FUTURES_TENOR_SLOTS + slot] = (
                count * capital_per_contract / current_equity
            )
            count_f = float(count)
            absolute_count_f = abs(count_f)
            gross_pnl[row] += count_f * option_multiplier * (close_price - open_price)
            fees[row] += (
                absolute_count_f
                * option_schedule.fixed_fee_per_contract_per_side_twd
                * 2.0
            )
            taxes[row] += (
                absolute_count_f
                * option_multiplier
                * (open_price if short_side else close_price)
                * option_schedule.transaction_tax_rate
            )
            slippage[row] += (
                absolute_count_f
                * option_multiplier
                * option_schedule.slippage_points_per_side
                * 2.0
            )

        net_pnl[row] = gross_pnl[row] - fees[row] - taxes[row] - slippage[row]
        returns[row] = max(net_pnl[row] / current_equity, -1.0)
        turnovers[row] = 2.0 * np.abs(executed[row]).sum()
        current_equity += net_pnl[row]
        if not math.isfinite(current_equity) or current_equity <= 0.0:
            current_equity = 0.0
            still_alive = False
        equity[row] = current_equity
        alive[row] = still_alive
        # Positions are local loop variables only: both entry and exit cash
        # flows were booked above, so no contract can survive a slot remap.
        terminal_flat[row] = True

    return DerivativesDayIntegerBacktest(
        dates=np.asarray(futures_market.dates, dtype="datetime64[D]").copy(),
        requested_actions=requested,
        executed_actions=executed,
        futures_contract_months=np.asarray(candidates.futures_contract_months).copy(),
        futures_contract_quantities=future_counts,
        option_contract_quantities=option_counts,
        option_sparse_indices=option_sparse_out,
        gross_pnl_twd=gross_pnl,
        fees_twd=fees,
        tax_twd=taxes,
        slippage_twd=slippage,
        net_pnl_twd=net_pnl,
        strategy_returns=returns,
        turnovers=turnovers,
        equity=equity,
        alive=alive,
        terminal_flat=terminal_flat,
    )


__all__ = [
    "DerivativesDayContinuousBacktest",
    "DerivativesDayIntegerBacktest",
    "OptionDayCostSchedule",
    "TW_INDEX_DERIVATIVES_DAY_BACKTEST_CONTRACT_VERSION",
    "run_tw_index_derivatives_day_continuous",
    "run_tw_index_derivatives_day_integer",
]
