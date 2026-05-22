from __future__ import annotations

import torch
from torch import Tensor


def masked_mse_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    mask_f = mask.to(dtype=predictions.dtype)
    error = (predictions - targets).pow(2) * mask_f
    return error.sum() / mask_f.sum().clamp_min(1.0)


def masked_ic_loss(predictions: Tensor, targets: Tensor, mask: Tensor) -> Tensor:
    ics: list[Tensor] = []
    for i in range(predictions.size(0)):
        m = mask[i]
        if int(m.sum().item()) < 2:
            continue
        p = predictions[i][m].float()
        t = targets[i][m].float()
        p_rank = p.argsort().argsort().float()
        t_rank = t.argsort().argsort().float()
        p_c = p_rank - p_rank.mean()
        t_c = t_rank - t_rank.mean()
        denom = (p_c.norm() * t_c.norm()).clamp_min(1e-8)
        ics.append((p_c * t_c).sum() / denom)
    if not ics:
        return predictions.sum() * 0.0
    return -torch.stack(ics).mean()


def sharpe_aware_loss(
    weights: Tensor,
    future_log_returns: Tensor,
    tradable_mask: Tensor,
    sample_mask: Tensor | None = None,
    fee_per_side: float = 0.0,
    buy_fee_rate: float | None = None,
    sell_fee_rate: float | None = None,
    gamma_sharpe: float = 1.0,
    gamma_turnover: float = 0.1,
    cash_symbol_mask: Tensor | None = None,
) -> Tensor:
    """Vectorized Sharpe-aware loss with transaction costs on GPU.

    This path avoids creating backtest container objects in each train step.
    """
    if buy_fee_rate is None:
        buy_fee_rate = fee_per_side
    if sell_fee_rate is None:
        sell_fee_rate = fee_per_side

    weights_f = weights.float()
    tradable_bool = tradable_mask.bool()

    if sample_mask is None:
        sample_mask_f = torch.ones(weights.size(0), device=weights.device, dtype=weights_f.dtype)
    else:
        sample_mask_f = sample_mask.to(device=weights.device, dtype=weights_f.dtype)

    returns = torch.nan_to_num(future_log_returns.float(), nan=0.0, posinf=0.0, neginf=0.0)

    weights_history = weights_f.masked_fill(~tradable_bool, 0.0)
    denom = weights_history.abs().sum(dim=1, keepdim=True).clamp_min(1e-12)
    weights_history = weights_history / denom

    prev = torch.cat([torch.zeros_like(weights_history[:1]), weights_history[:-1]], dim=0)
    delta = weights_history - prev
    buy_turnover = torch.relu(delta)
    sell_turnover = torch.relu(-delta)

    if cash_symbol_mask is not None:
        cash_mask = cash_symbol_mask.to(device=weights_history.device, dtype=torch.bool)
        if cash_mask.ndim != 1 or cash_mask.numel() != weights_history.size(1):
            raise ValueError(
                "cash_symbol_mask shape must match num_symbols: "
                f"expected ({weights_history.size(1)},), got {tuple(cash_mask.shape)}"
            )
        mask_view = cash_mask.unsqueeze(0)
        buy_turnover = buy_turnover.masked_fill(mask_view, 0.0)
        sell_turnover = sell_turnover.masked_fill(mask_view, 0.0)

    turnovers = (buy_turnover + sell_turnover).sum(dim=1)
    gross = (weights_history * returns).sum(dim=1)
    net_returns = gross - buy_fee_rate * buy_turnover.sum(dim=1) - sell_fee_rate * sell_turnover.sum(dim=1)

    valid_count = sample_mask_f.sum().clamp_min(1.0)

    turnover_penalty = ((turnovers * sample_mask_f).sum()) / valid_count

    masked_returns = net_returns * sample_mask_f
    mean_return = masked_returns.sum() / valid_count
    centered = (net_returns - mean_return) * sample_mask_f
    variance = (centered ** 2).sum() / valid_count

    eps = 1e-8
    std_return = torch.sqrt(variance + eps)
    annualizer = torch.sqrt(torch.as_tensor(252.0, device=weights.device, dtype=weights_f.dtype))

    sharpe = mean_return / std_return * annualizer

    loss = -gamma_sharpe * sharpe + gamma_turnover * turnover_penalty
    return loss