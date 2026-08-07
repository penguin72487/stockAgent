"""Causal completed-second execution for the TX/TXO tick strategy.

The official TAIFEX public transaction files expose whole-second timestamps
without a trade identifier. A decision therefore owns only information from a
completed second and executes at a trade in a strictly later second. The shared
module keeps the one-TX executor and the Call/Put premium-and-margin ledger.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real
from typing import Final

import torch

from stockagent.models.normalization import masked_signed_action_weights


TW_INDEX_DERIVATIVES_TICK_BACKTEST_CONTRACT_VERSION: Final[int] = 2
TW_INDEX_OPTIONS_TICK_BACKTEST_CONTRACT_VERSION: Final[int] = 2


@dataclass(frozen=True, slots=True)
class TickContinuousBacktest:
    strategy_returns: torch.Tensor
    gross_returns: torch.Tensor
    cost_returns: torch.Tensor
    requested_exposure: torch.Tensor
    executed_exposure: torch.Tensor
    turnovers: torch.Tensor
    tradable_mask: torch.Tensor
    final_exposure: torch.Tensor


@dataclass(frozen=True, slots=True)
class OptionTickState:
    cash: torch.Tensor
    equity: torch.Tensor
    contracts: torch.Tensor
    alive: torch.Tensor

    def detached(self) -> OptionTickState:
        return OptionTickState(
            cash=self.cash.detach().clone(),
            equity=self.equity.detach().clone(),
            contracts=self.contracts.detach().clone(),
            alive=self.alive.detach().clone(),
        )


@dataclass(frozen=True, slots=True)
class OptionTickStep:
    state: OptionTickState
    net_return: torch.Tensor
    gross_pnl: torch.Tensor
    fixed_fees: torch.Tensor
    transaction_tax: torch.Tensor
    slippage_cost: torch.Tensor
    premium_cash_flow: torch.Tensor
    turnover_contracts: torch.Tensor
    initial_margin_required: torch.Tensor
    unfilled_contracts: torch.Tensor


def _finite_nonnegative(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite non-negative real")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise ValueError(f"{name} must be a finite non-negative real")
    return result


def sweep_five_level_depth(
    requested_contracts: torch.Tensor,
    prices: torch.Tensor,
    volumes: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Return filled contracts and price-point notional for one marketable order."""

    if requested_contracts.numel() != 1:
        raise ValueError("requested_contracts must be scalar")
    if tuple(prices.shape) != (5,) or tuple(volumes.shape) != (5,):
        raise ValueError("five-level prices and volumes must have shape [5]")
    requested = torch.nan_to_num(
        requested_contracts.float().reshape(()), nan=0.0
    ).clamp_min(0.0)
    clean_prices = torch.nan_to_num(prices.float(), nan=0.0).clamp_min(0.0)
    clean_volumes = torch.nan_to_num(volumes.float(), nan=0.0).clamp_min(0.0)
    remaining = requested
    filled = requested * 0.0
    point_notional = requested * 0.0
    for level in range(5):
        valid = (clean_prices[level] > 0.0) & (clean_volumes[level] > 0.0)
        level_fill = torch.minimum(
            remaining,
            torch.where(valid, clean_volumes[level], clean_volumes[level] * 0.0),
        )
        filled = filled + level_fill
        point_notional = point_notional + level_fill * clean_prices[level]
        remaining = (remaining - level_fill).clamp_min(0.0)
    return filled, point_notional


def option_target_weights_from_logits(
    logits: torch.Tensor,
    mask: torch.Tensor,
    *,
    allow_option_short: bool,
    maximum_capital_fraction: float,
) -> torch.Tensor:
    """Reuse the canonical long/short/cash action allocator for Call and Put."""

    capital_fraction = _finite_nonnegative(
        "maximum_capital_fraction", maximum_capital_fraction
    )
    if capital_fraction > 1.0:
        raise ValueError("maximum_capital_fraction must not exceed 1")
    weights = masked_signed_action_weights(
        logits,
        mask,
        transform="softmax",
        long_only=not bool(allow_option_short),
    )
    return weights * float(capital_fraction)


def txo_short_initial_margin_per_contract(
    option_prices: torch.Tensor,
    underlying_price: torch.Tensor | float,
    strike_price: torch.Tensor | float,
    *,
    contract_multiplier: float = 50.0,
    risk_margin_a_twd: float = 169_000.0,
    risk_margin_b_twd: float = 85_000.0,
) -> torch.Tensor:
    """Return naked Call/Put initial margin using the official single-leg rule."""

    if option_prices.shape != (2,):
        raise ValueError("option_prices must have shape [2] in [Call, Put] order")
    multiplier = _finite_nonnegative("contract_multiplier", contract_multiplier)
    margin_a = _finite_nonnegative("risk_margin_a_twd", risk_margin_a_twd)
    margin_b = _finite_nonnegative("risk_margin_b_twd", risk_margin_b_twd)
    if multiplier <= 0.0 or margin_a <= 0.0 or margin_b <= 0.0:
        raise ValueError("option multiplier and A/B margins must be positive")
    if margin_b > margin_a:
        raise ValueError("risk_margin_b_twd must not exceed risk_margin_a_twd")
    prices = torch.nan_to_num(option_prices.float(), nan=0.0).clamp_min(0.0)
    underlying = torch.as_tensor(
        underlying_price, device=prices.device, dtype=torch.float32
    ).reshape(())
    strike = torch.as_tensor(
        strike_price, device=prices.device, dtype=torch.float32
    ).reshape(())
    call_otm = torch.clamp(strike - underlying, min=0.0) * float(multiplier)
    put_otm = torch.clamp(underlying - strike, min=0.0) * float(multiplier)
    otm = torch.stack([call_otm, put_otm])
    premium_value = prices * float(multiplier)
    risk = torch.maximum(
        prices.new_tensor(float(margin_a)) - otm,
        prices.new_tensor(float(margin_b)),
    )
    return premium_value + risk


def initialize_option_tick_state(
    *,
    initial_equity: float,
    device: torch.device | str,
) -> OptionTickState:
    equity = _finite_nonnegative("initial_equity", initial_equity)
    if equity <= 0.0:
        raise ValueError("initial_equity must be positive")
    value = torch.tensor(equity, device=device, dtype=torch.float32)
    return OptionTickState(
        cash=value.clone(),
        equity=value.clone(),
        contracts=torch.zeros(2, device=device, dtype=torch.float32),
        alive=torch.ones((), device=device, dtype=torch.bool),
    )


def execute_option_tick_target(
    state: OptionTickState,
    *,
    target_weights: torch.Tensor,
    tradable_mask: torch.Tensor,
    bid_prices: torch.Tensor,
    bid_volumes: torch.Tensor,
    ask_prices: torch.Tensor,
    ask_volumes: torch.Tensor,
    next_bid_prices: torch.Tensor,
    next_ask_prices: torch.Tensor,
    underlying_price: torch.Tensor | float,
    strike_price: torch.Tensor | float,
    allow_option_short: bool,
    maximum_capital_fraction: float = 0.8,
    contract_multiplier: float = 50.0,
    risk_margin_a_twd: float = 169_000.0,
    risk_margin_b_twd: float = 85_000.0,
    fixed_fee_per_contract_per_side_twd: float = 20.0,
    transaction_tax_rate: float = 0.001,
    slippage_points_per_side: float = 0.5,
    force_flat: bool = False,
) -> OptionTickStep:
    """Rebalance TXO at marketable five-level prices and conservative marks."""

    for name, value in (
        ("target_weights", target_weights),
        ("tradable_mask", tradable_mask),
    ):
        if tuple(value.shape) != (2,):
            raise ValueError(f"{name} must have shape [2] in [Call, Put] order")
    for name, value in (
        ("bid_prices", bid_prices),
        ("bid_volumes", bid_volumes),
        ("ask_prices", ask_prices),
        ("ask_volumes", ask_volumes),
        ("next_bid_prices", next_bid_prices),
        ("next_ask_prices", next_ask_prices),
    ):
        if tuple(value.shape) != (2, 5):
            raise ValueError(f"{name} must have shape [2,5] in [Call, Put] order")
    multiplier = _finite_nonnegative("contract_multiplier", contract_multiplier)
    fee = _finite_nonnegative(
        "fixed_fee_per_contract_per_side_twd",
        fixed_fee_per_contract_per_side_twd,
    )
    tax_rate = _finite_nonnegative("transaction_tax_rate", transaction_tax_rate)
    slippage = _finite_nonnegative("slippage_points_per_side", slippage_points_per_side)
    capital_fraction = _finite_nonnegative(
        "maximum_capital_fraction", maximum_capital_fraction
    )
    if multiplier <= 0.0 or not 0.0 < capital_fraction <= 1.0:
        raise ValueError("multiplier must be positive and capital fraction in (0,1]")
    bids = torch.nan_to_num(bid_prices.float(), nan=0.0).clamp_min(0.0)
    asks = torch.nan_to_num(ask_prices.float(), nan=0.0).clamp_min(0.0)
    next_bids = torch.nan_to_num(next_bid_prices.float(), nan=0.0).clamp_min(0.0)
    next_asks = torch.nan_to_num(next_ask_prices.float(), nan=0.0).clamp_min(0.0)
    valid = (
        tradable_mask.to(device=bids.device, dtype=torch.bool)
        & torch.isfinite(bid_prices).all(dim=1)
        & torch.isfinite(ask_prices).all(dim=1)
        & torch.isfinite(next_bid_prices).all(dim=1)
        & torch.isfinite(next_ask_prices).all(dim=1)
        & (bids[:, 0] > 0.0)
        & (asks[:, 0] >= bids[:, 0])
        & (next_bids[:, 0] > 0.0)
        & (next_asks[:, 0] >= next_bids[:, 0])
        & (bid_volumes[:, 0] > 0.0)
        & (ask_volumes[:, 0] > 0.0)
    )
    alive = state.alive.to(device=bids.device, dtype=torch.bool)
    current_contracts = state.contracts.to(device=bids.device, dtype=torch.float32)
    current_cash = state.cash.to(device=bids.device, dtype=torch.float32)
    current_marks = torch.where(current_contracts >= 0.0, bids[:, 0], asks[:, 0])
    marked_equity = current_cash + torch.sum(
        current_contracts * current_marks * float(multiplier)
    )
    alive = alive & torch.isfinite(marked_equity) & (marked_equity > 0.0)
    safe_equity = torch.where(alive, marked_equity, torch.zeros_like(marked_equity))

    clean_weights = torch.nan_to_num(target_weights.float(), nan=0.0)
    if not allow_option_short:
        clean_weights = clean_weights.clamp_min(0.0)
    gross = clean_weights.abs().sum()
    clean_weights = torch.where(
        gross > capital_fraction,
        clean_weights * (float(capital_fraction) / gross.clamp_min(1e-8)),
        clean_weights,
    )
    if force_flat:
        clean_weights = clean_weights * 0.0
    premium_value = asks[:, 0] * float(multiplier)
    short_margin = txo_short_initial_margin_per_contract(
        asks[:, 0],
        underlying_price,
        strike_price,
        contract_multiplier=multiplier,
        risk_margin_a_twd=risk_margin_a_twd,
        risk_margin_b_twd=risk_margin_b_twd,
    )
    long_contracts = (
        clean_weights.clamp_min(0.0)
        * safe_equity
        / premium_value.clamp_min(torch.finfo(torch.float32).tiny)
    )
    short_contracts = (
        (-clean_weights).clamp_min(0.0)
        * safe_equity
        / short_margin.clamp_min(torch.finfo(torch.float32).tiny)
    )
    requested_contracts = long_contracts - short_contracts
    requested_contracts = torch.where(
        alive & valid,
        requested_contracts,
        current_contracts,
    )
    if not bool(alive.detach().cpu().item()):
        requested_contracts = current_contracts * 0.0
    requested_delta = requested_contracts - current_contracts
    filled_delta_rows: list[torch.Tensor] = []
    execution_point_notional_rows: list[torch.Tensor] = []
    for leg in range(2):
        buy_filled, buy_notional = sweep_five_level_depth(
            requested_delta[leg].clamp_min(0.0),
            asks[leg],
            ask_volumes[leg],
        )
        sell_filled, sell_notional = sweep_five_level_depth(
            (-requested_delta[leg]).clamp_min(0.0),
            bids[leg],
            bid_volumes[leg],
        )
        can_trade = alive & valid[leg]
        filled_delta_rows.append(
            torch.where(can_trade, buy_filled - sell_filled, buy_filled * 0.0)
        )
        execution_point_notional_rows.append(
            torch.where(can_trade, buy_notional + sell_notional, buy_notional * 0.0)
        )
    filled_delta = torch.stack(filled_delta_rows)
    execution_point_notional = torch.stack(execution_point_notional_rows)
    executed_contracts = current_contracts + filled_delta
    turnover_contracts = filled_delta.abs()
    unfilled_contracts = (requested_delta - filled_delta).abs().sum()
    fixed_fees = turnover_contracts.sum() * float(fee)
    slippage_cost = turnover_contracts.sum() * float(slippage * multiplier)
    execution_notional = execution_point_notional * float(multiplier)
    transaction_tax = execution_notional.sum() * float(tax_rate)
    transaction_cost = fixed_fees + slippage_cost + transaction_tax
    premium_cash_flow = -torch.sum(torch.sign(filled_delta) * execution_notional)
    cash_after_trade = current_cash + premium_cash_flow - transaction_cost
    post_trade_marks = torch.where(executed_contracts >= 0.0, bids[:, 0], asks[:, 0])
    equity_after_trade = cash_after_trade + torch.sum(
        executed_contracts * post_trade_marks * float(multiplier)
    )
    margin_required = torch.sum((-executed_contracts).clamp_min(0.0) * short_margin)
    margin_ok = margin_required <= (equity_after_trade.clamp_min(0.0) + 1e-4)
    if not bool(margin_ok.detach().cpu().item()):
        raise RuntimeError(
            "option target exceeded configured naked-short margin capacity"
        )
    next_marks = torch.where(
        executed_contracts >= 0.0, next_bids[:, 0], next_asks[:, 0]
    )
    final_equity = cash_after_trade + torch.sum(
        executed_contracts * next_marks * float(multiplier)
    )
    gross_pnl = (
        current_cash
        + premium_cash_flow
        + torch.sum(executed_contracts * next_marks * float(multiplier))
        - marked_equity
    )
    final_alive = alive & torch.isfinite(final_equity) & (final_equity > 0.0)
    final_contracts = torch.where(
        final_alive,
        executed_contracts,
        torch.zeros_like(executed_contracts),
    )
    final_cash = torch.where(final_alive, cash_after_trade, final_equity)
    net_return = torch.where(
        alive,
        final_equity / marked_equity.clamp_min(torch.finfo(torch.float32).tiny) - 1.0,
        torch.zeros_like(marked_equity),
    )
    return OptionTickStep(
        state=OptionTickState(
            cash=final_cash,
            equity=final_equity,
            contracts=final_contracts,
            alive=final_alive,
        ),
        net_return=net_return,
        gross_pnl=gross_pnl,
        fixed_fees=fixed_fees,
        transaction_tax=transaction_tax,
        slippage_cost=slippage_cost,
        premium_cash_flow=premium_cash_flow,
        turnover_contracts=turnover_contracts.sum(),
        initial_margin_required=margin_required,
        unfilled_contracts=unfilled_contracts,
    )


def run_tw_index_derivatives_tick_continuous(
    exposure: torch.Tensor,
    interval_log_returns: torch.Tensor,
    execution_prices: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    initial_exposure: torch.Tensor | float = 0.0,
    fee_rate_per_side: float = 0.000045,
    slippage_points_per_side: float = 1.0,
    max_abs_exposure: float = 1.0,
    force_final_flat: bool = False,
) -> TickContinuousBacktest:
    """Rebalance one signed TX exposure and charge one-way turnover costs.

    Row ``t`` is a completed-second decision whose target is established at
    ``execution_prices[t]``, the first later TX trade.  That target earns the
    price move to the next decision's execution price.  An untradable row keeps
    the prior exposure rather than pretending the existing position vanished.
    """

    if exposure.ndim != 1:
        raise ValueError(f"exposure must have shape [T], got {tuple(exposure.shape)}")
    for name, value in (
        ("interval_log_returns", interval_log_returns),
        ("execution_prices", execution_prices),
        ("tradable_mask", tradable_mask),
    ):
        if tuple(value.shape) != tuple(exposure.shape):
            raise ValueError(f"{name} must have shape [T]")
    fee_rate = _finite_nonnegative("fee_rate_per_side", fee_rate_per_side)
    slippage_points = _finite_nonnegative(
        "slippage_points_per_side", slippage_points_per_side
    )
    exposure_limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if exposure_limit <= 0.0:
        raise ValueError("max_abs_exposure must be positive")

    requested = torch.nan_to_num(
        exposure,
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    ).clamp(min=-exposure_limit, max=exposure_limit)
    valid = (
        tradable_mask.to(device=exposure.device, dtype=torch.bool)
        & torch.isfinite(interval_log_returns)
        & torch.isfinite(execution_prices)
        & (execution_prices > 0.0)
    )
    if isinstance(initial_exposure, torch.Tensor):
        if initial_exposure.numel() != 1:
            raise ValueError("initial_exposure must be scalar")
        previous = initial_exposure.to(device=exposure.device, dtype=torch.float32)
    else:
        previous = exposure.new_tensor(float(initial_exposure), dtype=torch.float32)
    previous = torch.nan_to_num(
        previous,
        nan=0.0,
        posinf=exposure_limit,
        neginf=-exposure_limit,
    ).clamp(min=-exposure_limit, max=exposure_limit)

    executed_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    gross_rows: list[torch.Tensor] = []
    cost_rows: list[torch.Tensor] = []
    for index in range(int(exposure.numel())):
        target = requested[index].to(dtype=torch.float32)
        if force_final_flat and index == exposure.numel() - 1:
            target = target * 0.0
        executed = torch.where(valid[index], target, previous)
        turnover = torch.abs(executed - previous)
        simple_return = torch.expm1(
            torch.where(
                valid[index],
                interval_log_returns[index].to(dtype=torch.float32),
                interval_log_returns.new_zeros((), dtype=torch.float32),
            )
        )
        gross = executed * simple_return
        slippage_rate = slippage_points / execution_prices[index].to(
            dtype=torch.float32
        ).clamp_min(torch.finfo(torch.float32).tiny)
        cost = turnover * (float(fee_rate) + slippage_rate)
        executed_rows.append(executed)
        turnover_rows.append(turnover)
        gross_rows.append(gross)
        cost_rows.append(cost)
        previous = executed

    if executed_rows:
        executed_tensor = torch.stack(executed_rows)
        turnover_tensor = torch.stack(turnover_rows)
        gross_tensor = torch.stack(gross_rows)
        cost_tensor = torch.stack(cost_rows)
    else:
        empty = exposure.new_empty((0,), dtype=torch.float32)
        executed_tensor = empty
        turnover_tensor = empty
        gross_tensor = empty
        cost_tensor = empty
    return TickContinuousBacktest(
        strategy_returns=gross_tensor - cost_tensor,
        gross_returns=gross_tensor,
        cost_returns=cost_tensor,
        requested_exposure=requested,
        executed_exposure=executed_tensor,
        turnovers=turnover_tensor,
        tradable_mask=valid,
        final_exposure=previous,
    )


def run_tw_index_derivatives_tick_bidask_continuous(
    exposure: torch.Tensor,
    bid_prices: torch.Tensor,
    bid_volumes: torch.Tensor,
    ask_prices: torch.Tensor,
    ask_volumes: torch.Tensor,
    next_bid_prices: torch.Tensor,
    next_ask_prices: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    initial_exposure: torch.Tensor | float = 0.0,
    initial_equity: float = 1_000_000.0,
    contract_multiplier: float = 200.0,
    fee_rate_per_side: float = 0.000045,
    slippage_points_per_side: float = 0.0,
    max_abs_exposure: float = 1.0,
    force_final_flat: bool = False,
) -> TickContinuousBacktest:
    """Execute a signed TX exposure by sweeping the captured five-level book."""

    if exposure.ndim != 1:
        raise ValueError("exposure must have shape [T]")
    rows = int(exposure.numel())
    for name, value in (
        ("bid_prices", bid_prices),
        ("bid_volumes", bid_volumes),
        ("ask_prices", ask_prices),
        ("ask_volumes", ask_volumes),
        ("next_bid_prices", next_bid_prices),
        ("next_ask_prices", next_ask_prices),
    ):
        if tuple(value.shape) != (rows, 5):
            raise ValueError(f"{name} must have shape [T,5]")
    if tuple(tradable_mask.shape) != (rows,):
        raise ValueError("tradable_mask must have shape [T]")
    equity = _finite_nonnegative("initial_equity", initial_equity)
    multiplier = _finite_nonnegative("contract_multiplier", contract_multiplier)
    fee_rate = _finite_nonnegative("fee_rate_per_side", fee_rate_per_side)
    slippage = _finite_nonnegative("slippage_points_per_side", slippage_points_per_side)
    limit = _finite_nonnegative("max_abs_exposure", max_abs_exposure)
    if equity <= 0.0 or multiplier <= 0.0 or limit <= 0.0:
        raise ValueError("equity, multiplier, and exposure limit must be positive")
    requested = torch.nan_to_num(
        exposure.float(), nan=0.0, posinf=limit, neginf=-limit
    ).clamp(min=-limit, max=limit)
    if isinstance(initial_exposure, torch.Tensor):
        previous = initial_exposure.to(device=exposure.device, dtype=torch.float32)
    else:
        previous = exposure.new_tensor(float(initial_exposure), dtype=torch.float32)
    previous = previous.reshape(()).clamp(min=-limit, max=limit)

    executed_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    gross_rows: list[torch.Tensor] = []
    cost_rows: list[torch.Tensor] = []
    valid_rows: list[torch.Tensor] = []
    for index in range(rows):
        bids = torch.nan_to_num(bid_prices[index].float(), nan=0.0).clamp_min(0.0)
        asks = torch.nan_to_num(ask_prices[index].float(), nan=0.0).clamp_min(0.0)
        next_bids = torch.nan_to_num(next_bid_prices[index].float(), nan=0.0).clamp_min(
            0.0
        )
        next_asks = torch.nan_to_num(next_ask_prices[index].float(), nan=0.0).clamp_min(
            0.0
        )
        valid = (
            tradable_mask[index].to(dtype=torch.bool)
            & (bids[0] > 0.0)
            & (asks[0] >= bids[0])
            & (next_bids[0] > 0.0)
            & (next_asks[0] >= next_bids[0])
            & (bid_volumes[index, 0] > 0.0)
            & (ask_volumes[index, 0] > 0.0)
        )
        mid = (bids[0] + asks[0]) * 0.5
        next_mid = (next_bids[0] + next_asks[0]) * 0.5
        target = requested[index]
        if force_final_flat and index == rows - 1:
            target = target * 0.0
        delta = target - previous
        requested_contracts = (
            delta.abs() * float(equity) / (mid * float(multiplier)).clamp_min(1e-8)
        )
        buy_filled, buy_notional = sweep_five_level_depth(
            torch.where(delta > 0.0, requested_contracts, requested_contracts * 0.0),
            asks,
            ask_volumes[index],
        )
        sell_filled, sell_notional = sweep_five_level_depth(
            torch.where(delta < 0.0, requested_contracts, requested_contracts * 0.0),
            bids,
            bid_volumes[index],
        )
        filled_contracts = torch.where(
            valid, buy_filled + sell_filled, buy_filled * 0.0
        )
        filled_exposure = filled_contracts * mid * float(multiplier) / float(equity)
        executed = previous + torch.sign(delta) * filled_exposure
        executed = executed.clamp(min=-limit, max=limit)
        turnover = (executed - previous).abs()
        gross = torch.where(valid, executed * (next_mid / mid - 1.0), mid * 0.0)
        adverse_points = torch.where(
            delta > 0.0,
            buy_notional - buy_filled * mid,
            sell_filled * mid - sell_notional,
        ).clamp_min(0.0)
        traded_notional = (buy_notional + sell_notional) * float(multiplier)
        cost = torch.where(
            valid,
            (
                adverse_points * float(multiplier)
                + filled_contracts * float(slippage * multiplier)
                + traded_notional * float(fee_rate)
            )
            / float(equity),
            mid * 0.0,
        )
        executed_rows.append(executed)
        turnover_rows.append(turnover)
        gross_rows.append(gross)
        cost_rows.append(cost)
        valid_rows.append(valid)
        previous = executed

    empty = exposure.new_empty((0,), dtype=torch.float32)
    return TickContinuousBacktest(
        strategy_returns=(torch.stack(gross_rows) - torch.stack(cost_rows))
        if rows
        else empty,
        gross_returns=torch.stack(gross_rows) if rows else empty,
        cost_returns=torch.stack(cost_rows) if rows else empty,
        requested_exposure=requested,
        executed_exposure=torch.stack(executed_rows) if rows else empty,
        turnovers=torch.stack(turnover_rows) if rows else empty,
        tradable_mask=torch.stack(valid_rows)
        if rows
        else exposure.new_empty((0,), dtype=torch.bool),
        final_exposure=previous,
    )


def tw_index_derivatives_tick_log_utility_loss(
    exposure: torch.Tensor,
    interval_log_returns: torch.Tensor,
    execution_prices: torch.Tensor,
    tradable_mask: torch.Tensor,
    **kwargs: object,
) -> torch.Tensor:
    result = run_tw_index_derivatives_tick_continuous(
        exposure,
        interval_log_returns,
        execution_prices,
        tradable_mask,
        **kwargs,
    )
    if result.strategy_returns.numel() == 0:
        return exposure.sum() * 0.0
    safe = torch.clamp(result.strategy_returns, min=-1.0 + 1e-7)
    return -torch.log1p(safe).mean()


__all__ = [
    "TW_INDEX_DERIVATIVES_TICK_BACKTEST_CONTRACT_VERSION",
    "TW_INDEX_OPTIONS_TICK_BACKTEST_CONTRACT_VERSION",
    "OptionTickState",
    "OptionTickStep",
    "TickContinuousBacktest",
    "execute_option_tick_target",
    "initialize_option_tick_state",
    "option_target_weights_from_logits",
    "run_tw_index_derivatives_tick_bidask_continuous",
    "run_tw_index_derivatives_tick_continuous",
    "sweep_five_level_depth",
    "txo_short_initial_margin_per_contract",
    "tw_index_derivatives_tick_log_utility_loss",
]
