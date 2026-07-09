from __future__ import annotations

import torch
from torch import Tensor

from stockagent.backtest.simulator import (
    _apply_min_trade_weight_torch,
    _asset_log_returns_to_simple_torch,
    _normalize_target_weights_torch,
    _portfolio_simple_returns_to_log_torch,
)


def _resolve_fused_loss_static_scalars(
    gross_leverage: float,
    max_turnover_ratio: float,
) -> tuple[float, float]:
    gross_budget = min(1.0, max(0.0, float(gross_leverage)))
    max_possible_turnover = 2.0 * float(gross_budget)
    raw_turnover_cap = max(0.0, float(max_turnover_ratio))
    effective_turnover_cap = (
        raw_turnover_cap
        if 0.0 < raw_turnover_cap < max_possible_turnover
        else 0.0
    )
    return float(gross_budget), float(effective_turnover_cap)


def _torch_dynamo_is_compiling() -> bool:
    try:
        import torch._dynamo as torch_dynamo  # type: ignore

        return bool(torch_dynamo.is_compiling())
    except Exception:
        return False


class _LongShortLogUtilityNoTurnoverCap(torch.autograd.Function):
    @staticmethod
    def forward(
        ctx,
        target_weights: Tensor,
        returns: Tensor,
        tradable: Tensor,
        can_buy: Tensor,
        can_sell: Tensor,
        valid_mask: Tensor,
        initial_weights: Tensor,
        buy_fee_rate: float,
        sell_fee_rate: float,
        gamma_sharpe: float,
        gamma_turnover: float,
        gross_budget: float,
    ) -> tuple[Tensor, Tensor]:
        t_len = int(target_weights.size(0))
        dtype = target_weights.dtype
        device = target_weights.device
        tradable_b = tradable.to(device=device, dtype=torch.bool)
        can_buy_b = can_buy.to(device=device, dtype=torch.bool)
        can_sell_b = can_sell.to(device=device, dtype=torch.bool)
        valid_f = valid_mask.to(device=device, dtype=torch.float32)
        prev = initial_weights.to(device=device, dtype=dtype)
        gross_cap = torch.as_tensor(float(gross_budget), device=device, dtype=dtype)
        floor = torch.as_tensor(-0.999999, device=device, dtype=dtype)

        pre_history = torch.empty_like(target_weights)
        allowed_history = torch.empty_like(target_weights, dtype=torch.bool)
        gross_scale_history = torch.empty((t_len,), device=device, dtype=dtype)
        gross_raw_history = torch.empty((t_len,), device=device, dtype=dtype)
        delta_sign_history = torch.empty_like(target_weights)
        net_simple_history = torch.empty((t_len,), device=device, dtype=dtype)

        return_sum = torch.zeros((), device=device, dtype=torch.float32)
        turnover_sum = torch.zeros((), device=device, dtype=torch.float32)
        valid_count = torch.zeros((), device=device, dtype=torch.float32)
        one = torch.ones((), device=device, dtype=dtype)

        for idx in range(t_len):
            target_t = torch.where(tradable_b[idx], target_weights[idx], prev)
            delta_raw = target_t - prev
            allowed = ((delta_raw > 0.0) & can_buy_b[idx]) | ((delta_raw < 0.0) & can_sell_b[idx])
            pre = prev + torch.where(allowed, delta_raw, torch.zeros_like(delta_raw))
            gross_raw = pre.abs().sum()
            gross_scale = torch.minimum(one, gross_cap / gross_raw.clamp_min(1e-12))
            next_weights = pre * gross_scale
            delta_final = next_weights - prev
            buy_turnover = delta_final.clamp_min(0.0).sum()
            sell_turnover = (-delta_final).clamp_min(0.0).sum()
            turnover = buy_turnover + sell_turnover
            gross_return = (next_weights * returns[idx]).sum()
            net_simple = gross_return - float(buy_fee_rate) * buy_turnover - float(sell_fee_rate) * sell_turnover
            strategy_return = torch.log1p(torch.nan_to_num(net_simple, nan=0.0, posinf=torch.finfo(dtype).max, neginf=-0.999999).clamp_min(floor))

            valid_t = valid_f[idx]
            return_sum = return_sum + torch.nan_to_num(strategy_return.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_t
            turnover_sum = turnover_sum + torch.nan_to_num(turnover.float(), nan=0.0, posinf=0.0, neginf=0.0) * valid_t
            valid_count = valid_count + valid_t

            pre_history[idx].copy_(pre)
            allowed_history[idx].copy_(allowed)
            gross_scale_history[idx].copy_(gross_scale)
            gross_raw_history[idx].copy_(gross_raw)
            delta_sign_history[idx].copy_(torch.sign(delta_final))
            net_simple_history[idx].copy_(net_simple)
            prev = next_weights

        denom = valid_count.clamp_min(1.0)
        loss = -float(gamma_sharpe) * (return_sum / denom * torch.as_tensor(252.0, device=device, dtype=torch.float32))
        if float(gamma_turnover) != 0.0:
            loss = loss + float(gamma_turnover) * (turnover_sum / denom)

        ctx.save_for_backward(
            returns,
            valid_f,
            pre_history,
            allowed_history,
            gross_scale_history,
            gross_raw_history,
            delta_sign_history,
            net_simple_history,
        )
        ctx.buy_fee_rate = float(buy_fee_rate)
        ctx.sell_fee_rate = float(sell_fee_rate)
        ctx.gamma_sharpe = float(gamma_sharpe)
        ctx.gamma_turnover = float(gamma_turnover)
        ctx.gross_budget = float(gross_budget)
        ctx.denom = denom
        ctx.target_dtype = dtype
        ctx.mark_non_differentiable(prev)
        return loss, prev

    @staticmethod
    def backward(ctx, grad_loss: Tensor, grad_final: Tensor | None = None):
        del grad_final
        (
            returns,
            valid_f,
            pre_history,
            allowed_history,
            gross_scale_history,
            gross_raw_history,
            delta_sign_history,
            net_simple_history,
        ) = ctx.saved_tensors
        t_len = int(returns.size(0))
        grad_target = torch.zeros_like(pre_history)
        grad_state = torch.zeros_like(pre_history[0])
        denom = ctx.denom.to(device=returns.device, dtype=torch.float32)
        loss_return_scale = -float(ctx.gamma_sharpe) * 252.0 / denom
        loss_turnover_scale = float(ctx.gamma_turnover) / denom
        floor = torch.as_tensor(-0.999999, device=returns.device, dtype=net_simple_history.dtype)

        for idx in range(t_len - 1, -1, -1):
            valid_t = valid_f[idx]
            net_simple = net_simple_history[idx]
            finite = torch.isfinite(net_simple)
            unclamped = net_simple >= floor
            dlog = torch.where(finite & unclamped, 1.0 / (net_simple.clamp_min(floor) + 1.0), torch.zeros_like(net_simple))
            grad_net_coeff = (loss_return_scale * valid_t).to(dtype=returns.dtype) * dlog.to(dtype=returns.dtype)
            grad_turn_coeff = (loss_turnover_scale * valid_t).to(dtype=returns.dtype)

            sign_delta = delta_sign_history[idx]
            pos = sign_delta > 0.0
            neg = sign_delta < 0.0
            grad_next_current = grad_net_coeff * (
                returns[idx]
                - float(ctx.buy_fee_rate) * pos.to(dtype=returns.dtype)
                + float(ctx.sell_fee_rate) * neg.to(dtype=returns.dtype)
            )
            if float(ctx.gamma_turnover) != 0.0:
                grad_next_current = grad_next_current + grad_turn_coeff * sign_delta.to(dtype=returns.dtype)

            grad_prev_direct = grad_net_coeff * (
                float(ctx.buy_fee_rate) * pos.to(dtype=returns.dtype)
                - float(ctx.sell_fee_rate) * neg.to(dtype=returns.dtype)
            )
            if float(ctx.gamma_turnover) != 0.0:
                grad_prev_direct = grad_prev_direct - grad_turn_coeff * sign_delta.to(dtype=returns.dtype)

            grad_next = grad_state.to(dtype=returns.dtype) + grad_next_current
            pre = pre_history[idx]
            gross_scale = gross_scale_history[idx]
            gross_raw = gross_raw_history[idx]
            grad_next_pre = grad_next.to(dtype=pre.dtype)
            dot = (grad_next_pre * pre).sum()
            grad_pre_scaled = gross_scale * grad_next_pre
            grad_pre_scaled = grad_pre_scaled - (
                gross_scale / gross_raw.clamp_min(1e-12)
            ) * torch.sign(pre) * dot
            grad_pre = torch.where(gross_scale < 1.0, grad_pre_scaled, grad_next_pre)

            allowed_f = allowed_history[idx].to(dtype=grad_pre.dtype)
            grad_target[idx].copy_(grad_pre * allowed_f)
            grad_state = grad_prev_direct.to(dtype=grad_pre.dtype) + grad_pre * (1.0 - allowed_f)

        grad_target = grad_target * grad_loss.to(device=grad_target.device, dtype=grad_target.dtype)
        return (
            grad_target.to(dtype=ctx.target_dtype),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )


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
    volume_limit_weights: Tensor | None = None,
) -> tuple[Tensor, Tensor, Tensor, Tensor]:
    t_len = int(target_weights.size(0))
    dtype = target_weights.dtype
    device = target_weights.device
    prev = initial_weights.to(device=device, dtype=dtype)
    cap = (
        torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
        if max_turnover_ratio > 0.0
        else None
    )
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
        if volume_limit_weights is not None:
            volume_cap = volume_limit_weights[idx].to(device=device, dtype=dtype)
            abs_delta = delta.abs()
            cap_safe = torch.where(
                torch.isfinite(volume_cap) & (volume_cap >= 0.0),
                volume_cap.clamp_min(0.0),
                abs_delta,
            )
            delta = torch.sign(delta) * torch.minimum(abs_delta, cap_safe)
            next_weights = prev + delta
        if cap is not None:
            turnover_raw = delta.abs().sum()
            turnover_scale = torch.minimum(torch.ones_like(turnover_raw), cap / turnover_raw.clamp_min(1e-12))
            next_weights = prev + delta * turnover_scale
            delta = next_weights - prev

        buy_turnover = delta.clamp_min(0.0).sum()
        sell_turnover = (-delta).clamp_min(0.0).sum()
        turnover = buy_turnover + sell_turnover
        gross_return = (next_weights * returns[idx]).sum()
        net_simple_return = gross_return - float(buy_fee_rate) * buy_turnover - float(sell_fee_rate) * sell_turnover
        strategy_return = _portfolio_simple_returns_to_log_torch(net_simple_return)

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
    volume_limit_weights: Tensor | None = None,
) -> tuple[Tensor, Tensor, Tensor, Tensor]:
    t_len = int(target_weights.size(0))
    dtype = target_weights.dtype
    device = target_weights.device
    prev = initial_weights.to(device=device, dtype=dtype)
    cap = (
        torch.as_tensor(max_turnover_ratio, device=device, dtype=dtype)
        if max_turnover_ratio > 0.0
        else None
    )
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
        if volume_limit_weights is not None:
            volume_cap = volume_limit_weights[idx].to(device=device, dtype=dtype)
            abs_delta = delta.abs()
            cap_safe = torch.where(
                torch.isfinite(volume_cap) & (volume_cap >= 0.0),
                volume_cap.clamp_min(0.0),
                abs_delta,
            )
            delta = torch.sign(delta) * torch.minimum(abs_delta, cap_safe)
            next_weights = prev + delta
        if cap is not None:
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
        net_simple_return = gross_return - float(buy_fee_rate) * buy_turnover - float(sell_fee_rate) * sell_turnover
        strategy_return = _portfolio_simple_returns_to_log_torch(net_simple_return)

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
    min_trade_weight: float = 0.0,
    portfolio_activation: str = "identity",
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.0,
    concentration_weight: float = 0.0,
    manual_backward: bool = False,
    volume_limit_weights: Tensor | None = None,
) -> tuple[Tensor, Tensor]:
    """Compile-friendly canonical log-utility loss with tensor state I/O.

    This mirrors the canonical tensor backtest trading rules but reduces return
    and turnover inside the recurrent scan instead of materializing full curves.
    """
    compute_dtype = weights.dtype
    device = weights.device
    (
        gross_budget,
        effective_turnover_cap,
    ) = _resolve_fused_loss_static_scalars(
        gross_leverage,
        max_turnover_ratio,
    )
    returns = _asset_log_returns_to_simple_torch(future_returns, device=device, dtype=compute_dtype)
    tradable = tradable_mask.to(device=device, dtype=torch.bool)
    can_buy = can_buy_mask.to(device=device, dtype=torch.bool)
    can_sell = can_sell_mask.to(device=device, dtype=torch.bool)
    valid_mask = sample_mask.to(device=device, dtype=torch.bool)
    volume_limits = (
        None
        if volume_limit_weights is None
        else volume_limit_weights.to(device=device, dtype=compute_dtype)
    )
    target_weights = torch.nan_to_num(weights, nan=0.0, posinf=0.0, neginf=0.0)
    target_weights = _normalize_target_weights_torch(
        target_weights,
        long_only=long_only,
        gross_budget=gross_budget,
        portfolio_activation=portfolio_activation,
    )
    target_weights = _apply_min_trade_weight_torch(target_weights, min_trade_weight)

    use_manual_backward = (
        bool(manual_backward)
        and not bool(long_only)
        and float(effective_turnover_cap) == 0.0
        and volume_limits is None
        and target_weights.requires_grad
        and not _torch_dynamo_is_compiling()
    )

    if use_manual_backward:
        return_sum_loss, final_weights = _LongShortLogUtilityNoTurnoverCap.apply(
            target_weights,
            returns,
            tradable,
            can_buy,
            can_sell,
            valid_mask,
            initial_weights,
            float(buy_fee_rate),
            float(sell_fee_rate),
            float(gamma_sharpe),
            float(gamma_turnover),
            float(gross_budget),
        )
        loss = return_sum_loss
    elif long_only:
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
            max_turnover_ratio=effective_turnover_cap,
            volume_limit_weights=volume_limits,
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
            max_turnover_ratio=effective_turnover_cap,
            gross_budget=gross_budget,
            volume_limit_weights=volume_limits,
        )

    if not use_manual_backward:
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
