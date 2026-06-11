from __future__ import annotations

import torch
from torch import Tensor

from stockagent.backtest.simulator import _normalize_target_weights_torch, _resolve_exposure_budget


def _fused_log_utility_long_only_scan(
    target_weights: Tensor,
    returns: Tensor,
    tradable: Tensor,
    can_buy: Tensor,
    can_sell: Tensor,
    valid_mask: Tensor,
    initial_weights: Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
) -> tuple[Tensor, Tensor, Tensor, Tensor]:
    t_len = int(target_weights.size(0))
    dtype = target_weights.dtype
    device = target_weights.device
    prev = initial_weights.to(device=device, dtype=dtype)
    cap = torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
    one = torch.ones((), device=device, dtype=dtype)
    return_sum = torch.zeros((), device=device, dtype=torch.float32)
    turnover_sum = torch.zeros((), device=device, dtype=torch.float32)
    valid_count = torch.zeros((), device=device, dtype=torch.float32)

    for idx in range(t_len):
        target_t = torch.where(tradable[idx], target_weights[idx], prev)
        delta = target_t - prev
        buy_delta = delta.clamp_min(0.0) * can_buy[idx].to(dtype=dtype)
        sell_delta = delta.clamp_max(0.0) * can_sell[idx].to(dtype=dtype)

        base_after_sells = prev + sell_delta
        buy_sum = buy_delta.sum()
        buy_capacity = (one - base_after_sells.sum()).clamp_min(0.0)
        buy_scale = torch.minimum(torch.ones_like(buy_sum), buy_capacity / buy_sum.clamp_min(1e-12))
        delta = sell_delta + buy_delta * buy_scale

        next_weights = prev + delta
        if max_turnover_ratio > 0.0:
            turnover_raw = delta.abs().sum()
            turnover_scale = torch.minimum(torch.ones_like(turnover_raw), cap / turnover_raw.clamp_min(1e-12))
            next_weights = prev + delta * turnover_scale
            delta = next_weights - prev

        buy_turnover = delta.clamp_min(0.0).sum()
        sell_turnover = (-delta).clamp_min(0.0).sum()
        turnover = buy_turnover + sell_turnover
        gross_return = (next_weights * returns[idx]).sum()
        strategy_return = gross_return - float(buy_fee_rate) * buy_turnover - float(sell_fee_rate) * sell_turnover

        valid_f = valid_mask[idx].to(dtype=torch.float32)
        return_sum = return_sum + torch.nan_to_num(strategy_return.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
        turnover_sum = turnover_sum + torch.nan_to_num(turnover.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
        valid_count = valid_count + valid_f
        prev = next_weights

    return return_sum, turnover_sum, valid_count, prev


def _fused_log_utility_long_short_scan(
    target_weights: Tensor,
    returns: Tensor,
    tradable: Tensor,
    can_buy: Tensor,
    can_sell: Tensor,
    valid_mask: Tensor,
    initial_weights: Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_budget: float,
) -> tuple[Tensor, Tensor, Tensor, Tensor]:
    t_len = int(target_weights.size(0))
    dtype = target_weights.dtype
    device = target_weights.device
    prev = initial_weights.to(device=device, dtype=dtype)
    cap = torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
    gross_cap = torch.as_tensor(gross_budget, device=device, dtype=dtype)
    return_sum = torch.zeros((), device=device, dtype=torch.float32)
    turnover_sum = torch.zeros((), device=device, dtype=torch.float32)
    valid_count = torch.zeros((), device=device, dtype=torch.float32)

    for idx in range(t_len):
        target_t = torch.where(tradable[idx], target_weights[idx], prev)
        delta = target_t - prev
        buy_delta = delta.clamp_min(0.0) * can_buy[idx].to(dtype=dtype)
        sell_delta = delta.clamp_max(0.0) * can_sell[idx].to(dtype=dtype)
        delta = sell_delta + buy_delta

        next_weights = prev + delta
        if max_turnover_ratio > 0.0:
            turnover_raw = delta.abs().sum()
            turnover_scale = torch.minimum(torch.ones_like(turnover_raw), cap / turnover_raw.clamp_min(1e-12))
            next_weights = prev + delta * turnover_scale
            delta = next_weights - prev

        gross_next = next_weights.abs().sum()
        gross_scale = torch.minimum(torch.ones_like(gross_next), gross_cap / gross_next.clamp_min(1e-12))
        next_weights = next_weights * gross_scale
        delta = next_weights - prev

        buy_turnover = delta.clamp_min(0.0).sum()
        sell_turnover = (-delta).clamp_min(0.0).sum()
        turnover = buy_turnover + sell_turnover
        gross_return = (next_weights * returns[idx]).sum()
        strategy_return = gross_return - float(buy_fee_rate) * buy_turnover - float(sell_fee_rate) * sell_turnover

        valid_f = valid_mask[idx].to(dtype=torch.float32)
        return_sum = return_sum + torch.nan_to_num(strategy_return.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
        turnover_sum = turnover_sum + torch.nan_to_num(turnover.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_f
        valid_count = valid_count + valid_f
        prev = next_weights

    return return_sum, turnover_sum, valid_count, prev


def fused_log_utility_loss_tensor(
    weights: Tensor,
    future_returns: Tensor,
    tradable_mask: Tensor,
    can_buy_mask: Tensor,
    can_sell_mask: Tensor,
    sample_mask: Tensor,
    initial_weights: Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    long_only: bool,
    max_turnover_ratio: float,
    gross_leverage: float,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.0,
    concentration_weight: float = 0.0,
) -> tuple[Tensor, Tensor]:
    """Compile-friendly canonical log-utility loss with tensor state I/O.

    This mirrors the canonical tensor backtest trading rules but reduces return
    and turnover inside the recurrent scan instead of materializing full curves.
    """
    compute_dtype = weights.dtype
    device = weights.device
    gross_budget = _resolve_exposure_budget(gross_leverage)
    returns = torch.nan_to_num(
        future_returns.to(device=device, dtype=compute_dtype),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    tradable = tradable_mask.to(device=device, dtype=torch.bool)
    can_buy = can_buy_mask.to(device=device, dtype=torch.bool)
    can_sell = can_sell_mask.to(device=device, dtype=torch.bool)
    valid_mask = sample_mask.to(device=device, dtype=torch.bool)
    target_weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    target_weights = _normalize_target_weights_torch(
        target_weights,
        long_only=long_only,
        gross_budget=gross_budget,
    )

    if long_only:
        return_sum, turnover_sum, valid_count, final_weights = _fused_log_utility_long_only_scan(
            target_weights,
            returns,
            tradable,
            can_buy,
            can_sell,
            valid_mask,
            initial_weights,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
        )
    else:
        return_sum, turnover_sum, valid_count, final_weights = _fused_log_utility_long_short_scan(
            target_weights,
            returns,
            tradable,
            can_buy,
            can_sell,
            valid_mask,
            initial_weights,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            max_turnover_ratio=max_turnover_ratio,
            gross_budget=gross_budget,
        )

    denom = valid_count.clamp_min(1.0)
    mean_return = return_sum / denom
    loss = -float(gamma_sharpe) * (mean_return * torch.as_tensor(252.0, device=device, dtype=torch.float32))
    if float(gamma_turnover) != 0.0:
        loss = loss + float(gamma_turnover) * (turnover_sum / denom)
    if float(concentration_weight) > 0.0:
        tradable_f = tradable.to(dtype=target_weights.dtype)
        active_count = tradable_f.sum(dim=1).clamp_min(1.0)
        concentration = ((target_weights.pow(2) * tradable_f).sum(dim=1) * active_count).mean()
        loss = loss + float(concentration_weight) * concentration.float()
    return loss, final_weights.detach().clone(memory_format=torch.contiguous_format)
