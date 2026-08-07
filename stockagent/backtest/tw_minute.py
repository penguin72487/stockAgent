from __future__ import annotations

from dataclasses import dataclass
import math

import torch


@dataclass(frozen=True, slots=True)
class MinuteExecutionConfig:
    initial_equity: float
    gross_exposure: float
    maximum_name_weight: float
    maximum_volume_participation: float
    maximum_order_notional: float
    slippage_bps_per_side: float

    def __post_init__(self) -> None:
        positive = {
            "initial_equity": self.initial_equity,
            "maximum_order_notional": self.maximum_order_notional,
        }
        for name, value in positive.items():
            if not math.isfinite(float(value)) or float(value) <= 0.0:
                raise ValueError(f"{name} must be positive and finite")
        fractions = {
            "gross_exposure": self.gross_exposure,
            "maximum_name_weight": self.maximum_name_weight,
            "maximum_volume_participation": self.maximum_volume_participation,
        }
        for name, value in fractions.items():
            if not math.isfinite(float(value)) or not 0.0 < float(value) <= 1.0:
                raise ValueError(f"{name} must be in (0, 1]")
        if (
            not math.isfinite(float(self.slippage_bps_per_side))
            or float(self.slippage_bps_per_side) < 0.0
        ):
            raise ValueError("slippage_bps_per_side must be finite and nonnegative")


@dataclass(slots=True)
class MinuteExecutionState:
    cash: torch.Tensor
    shares: torch.Tensor
    last_prices: torch.Tensor
    equity: torch.Tensor

    def detached(self) -> "MinuteExecutionState":
        return MinuteExecutionState(
            cash=self.cash.detach().clone(),
            shares=self.shares.detach().clone(),
            last_prices=self.last_prices.detach().clone(),
            equity=self.equity.detach().clone(),
        )


@dataclass(frozen=True, slots=True)
class MinuteStepResult:
    state: MinuteExecutionState
    net_return: torch.Tensor
    turnover_notional: torch.Tensor
    explicit_fees: torch.Tensor
    slippage_cost: torch.Tensor


@dataclass(frozen=True, slots=True)
class MinuteCloseResult:
    state: MinuteExecutionState
    net_return: torch.Tensor
    turnover_notional: torch.Tensor
    explicit_fees: torch.Tensor
    slippage_cost: torch.Tensor
    forced_exit_over_capacity_notional: torch.Tensor


def initialize_minute_execution_state(
    *,
    num_symbols: int,
    config: MinuteExecutionConfig,
    device: torch.device,
    dtype: torch.dtype = torch.float32,
    batch_size: int | None = None,
) -> MinuteExecutionState:
    if int(num_symbols) <= 0:
        raise ValueError("num_symbols must be positive")
    if batch_size is not None and int(batch_size) <= 0:
        raise ValueError("batch_size must be positive when provided")
    batch_shape = () if batch_size is None else (int(batch_size),)
    cash = torch.full(
        batch_shape,
        float(config.initial_equity),
        device=device,
        dtype=dtype,
    )
    return MinuteExecutionState(
        cash=cash,
        shares=torch.zeros(
            (*batch_shape, int(num_symbols)), device=device, dtype=dtype
        ),
        last_prices=torch.zeros(
            (*batch_shape, int(num_symbols)), device=device, dtype=dtype
        ),
        equity=cash.clone(),
    )


def minute_target_weights_from_logits(
    logits: torch.Tensor,
    tradable_mask: torch.Tensor,
    *,
    gross_exposure: float,
    maximum_name_weight: float,
) -> torch.Tensor:
    if logits.ndim != 2 or tradable_mask.shape != logits.shape:
        raise ValueError("minute logits and tradable mask must both have shape [B,S]")
    mask = tradable_mask.to(dtype=torch.bool, device=logits.device)
    safe_logits = logits.float().masked_fill(~mask, torch.finfo(torch.float32).min)
    any_valid = mask.any(dim=1, keepdim=True)
    safe_logits = torch.where(any_valid, safe_logits, torch.zeros_like(safe_logits))
    weights = torch.softmax(safe_logits, dim=1)
    weights = weights.masked_fill(~mask, 0.0)
    weights = weights * float(gross_exposure)
    # Clamping leaves unused exposure as cash. Renormalizing here would violate
    # the configured per-name ceiling for a concentrated cross-section.
    weights = torch.clamp(weights, min=0.0, max=float(maximum_name_weight))
    return torch.where(any_valid, weights, torch.zeros_like(weights))


def execute_minute_target(
    state: MinuteExecutionState,
    *,
    target_weights: torch.Tensor,
    execution_mask: torch.Tensor,
    execution_open: torch.Tensor,
    exit_close: torch.Tensor,
    future_volume_shares: torch.Tensor,
    buy_fee_rates: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    config: MinuteExecutionConfig,
    allow_new_entries: bool | torch.Tensor,
) -> MinuteStepResult:
    """Execute one completed-bar decision at the next bar's open proxy."""

    if state.shares.ndim < 1:
        raise ValueError("minute executor shares must end in a symbol axis")
    symbols = int(state.shares.size(-1))
    batch_shape = tuple(state.shares.shape[:-1])
    if tuple(state.cash.shape) != batch_shape or tuple(state.equity.shape) != batch_shape:
        raise ValueError("minute executor cash/equity batch axes are inconsistent")
    if tuple(state.last_prices.shape) != tuple(state.shares.shape):
        raise ValueError("minute executor last_prices must match shares")
    aligned = (
        target_weights,
        execution_mask,
        execution_open,
        exit_close,
        future_volume_shares,
    )
    if any(tuple(value.shape) != tuple(state.shares.shape) for value in aligned):
        raise ValueError(
            "minute executor market inputs must match shares on [...,S]"
        )
    for rates in (buy_fee_rates, sell_fee_rates):
        if tuple(rates.shape) not in {(symbols,), tuple(state.shares.shape)}:
            raise ValueError("minute fee rates must have shape [S] or [...,S]")
    mask = execution_mask.to(device=state.shares.device, dtype=torch.bool)
    opens = execution_open.float().to(state.shares.device)
    closes = exit_close.float().to(state.shares.device)
    volumes = future_volume_shares.float().to(state.shares.device)
    valid = (
        mask
        & torch.isfinite(opens)
        & torch.isfinite(closes)
        & torch.isfinite(volumes)
        & (opens > 0.0)
        & (closes > 0.0)
        & (volumes > 0.0)
    )
    valuation_open = torch.where(valid, opens, state.last_prices)
    equity_open = state.cash + torch.sum(state.shares * valuation_open, dim=-1)
    # Avoid a device synchronization on every minute. CPU oracle/tests still
    # fail at the exact event; CUDA training is guarded by the finite log-loss
    # check before every backward pass.
    if state.shares.device.type == "cpu" and (
        not bool(torch.all(torch.isfinite(equity_open)).item())
        or bool(torch.any(equity_open.detach() <= 0.0).item())
    ):
        raise RuntimeError("minute account equity became nonpositive before execution")

    clean_target = torch.where(
        valid,
        torch.nan_to_num(target_weights.float(), nan=0.0, posinf=0.0, neginf=0.0),
        torch.zeros_like(target_weights, dtype=torch.float32),
    )
    target_shares = (
        clean_target
        * equity_open.unsqueeze(-1)
        / torch.clamp_min(opens, 1e-12)
    )
    if isinstance(allow_new_entries, bool):
        if not allow_new_entries:
            target_shares = torch.minimum(target_shares, state.shares)
    else:
        entry_flag = allow_new_entries.to(
            device=state.shares.device, dtype=torch.bool
        )
        target_shares = torch.where(
            entry_flag.unsqueeze(-1),
            target_shares,
            torch.minimum(target_shares, state.shares),
        )
    delta = target_shares - state.shares
    volume_capacity = torch.where(
        valid,
        volumes * float(config.maximum_volume_participation),
        torch.zeros_like(volumes),
    )
    notional_capacity = torch.where(
        valid,
        float(config.maximum_order_notional) / torch.clamp_min(opens, 1e-12),
        torch.zeros_like(opens),
    )
    capacity = torch.minimum(volume_capacity, notional_capacity)
    slippage = float(config.slippage_bps_per_side) / 10_000.0
    buy_rates = buy_fee_rates.float().to(state.shares.device)
    sell_rates = sell_fee_rates.float().to(state.shares.device)

    sell_quantity = torch.minimum(torch.clamp_min(-delta, 0.0), capacity)
    sell_quantity = torch.minimum(sell_quantity, state.shares)
    sell_execution = opens * (1.0 - slippage)
    sell_fees = sell_quantity * sell_execution * sell_rates
    cash_after_sell = state.cash + torch.sum(
        sell_quantity * sell_execution - sell_fees, dim=-1
    )
    shares_after_sell = state.shares - sell_quantity

    buy_quantity = torch.minimum(torch.clamp_min(delta, 0.0), capacity)
    buy_execution = opens * (1.0 + slippage)
    buy_unit_cost = buy_execution * (1.0 + buy_rates)
    requested_buy_cost = torch.sum(buy_quantity * buy_unit_cost, dim=-1)
    affordable_scale = torch.where(
        requested_buy_cost > 0.0,
        torch.clamp(cash_after_sell / torch.clamp_min(requested_buy_cost, 1e-12), 0.0, 1.0),
        torch.zeros_like(requested_buy_cost),
    )
    buy_quantity = buy_quantity * affordable_scale.unsqueeze(-1)
    buy_fees = buy_quantity * buy_execution * buy_rates
    cash = cash_after_sell - torch.sum(
        buy_quantity * buy_execution + buy_fees, dim=-1
    )
    shares = shares_after_sell + buy_quantity
    last_prices = torch.where(valid, closes, state.last_prices)
    equity = cash + torch.sum(shares * last_prices, dim=-1)
    net_return = equity / torch.clamp_min(state.equity, 1e-12) - 1.0
    turnover = torch.sum((sell_quantity + buy_quantity) * opens, dim=-1)
    fees = torch.sum(sell_fees + buy_fees, dim=-1)
    slippage_cost = turnover * slippage
    return MinuteStepResult(
        state=MinuteExecutionState(
            cash=cash,
            shares=shares,
            last_prices=last_prices,
            equity=equity,
        ),
        net_return=net_return,
        turnover_notional=turnover,
        explicit_fees=fees,
        slippage_cost=slippage_cost,
    )


def force_close_minute_session(
    state: MinuteExecutionState,
    *,
    session_close: torch.Tensor,
    session_exit_mask: torch.Tensor,
    last_minute_volume_shares: torch.Tensor,
    sell_fee_rates: torch.Tensor,
    config: MinuteExecutionConfig,
) -> MinuteCloseResult:
    if state.shares.ndim < 1:
        raise ValueError("minute force-close shares must end in a symbol axis")
    symbols = int(state.shares.size(-1))
    aligned_shape = tuple(state.shares.shape)
    if any(
        tuple(value.shape) != aligned_shape
        for value in (session_close, session_exit_mask, last_minute_volume_shares)
    ):
        raise ValueError("minute force-close market inputs must match shares on [...,S]")
    if tuple(sell_fee_rates.shape) not in {(symbols,), aligned_shape}:
        raise ValueError("minute force-close fee rates must have shape [S] or [...,S]")
    held = state.shares > 0.0
    valid_exit = (
        session_exit_mask.to(device=state.shares.device, dtype=torch.bool)
        & torch.isfinite(session_close)
        & (session_close > 0.0)
    )
    if bool(torch.any(held & ~valid_exit).item()):
        raise RuntimeError("minute session cannot force-close held symbols without a close")
    reference = session_close.float().to(state.shares.device)
    quantity = torch.where(held, state.shares, torch.zeros_like(state.shares))
    slippage = float(config.slippage_bps_per_side) / 10_000.0
    execution = reference * (1.0 - slippage)
    fees = quantity * execution * sell_fee_rates.float().to(state.shares.device)
    proceeds = quantity * execution - fees
    cash = state.cash + torch.sum(proceeds, dim=-1)
    turnover = torch.sum(quantity * reference, dim=-1)
    capacity = (
        last_minute_volume_shares.float().to(state.shares.device)
        * float(config.maximum_volume_participation)
    )
    over_capacity = torch.sum(
        torch.clamp_min(quantity - capacity, 0.0) * reference, dim=-1
    )
    equity = cash
    net_return = equity / torch.clamp_min(state.equity, 1e-12) - 1.0
    return MinuteCloseResult(
        state=MinuteExecutionState(
            cash=cash,
            shares=torch.zeros_like(state.shares),
            last_prices=reference,
            equity=equity,
        ),
        net_return=net_return,
        turnover_notional=turnover,
        explicit_fees=torch.sum(fees, dim=-1),
        slippage_cost=turnover * slippage,
        forced_exit_over_capacity_notional=over_capacity,
    )


__all__ = [
    "MinuteCloseResult",
    "MinuteExecutionConfig",
    "MinuteExecutionState",
    "MinuteStepResult",
    "execute_minute_target",
    "force_close_minute_session",
    "initialize_minute_execution_state",
    "minute_target_weights_from_logits",
]
