"""Continuous notional ledger for TAIFEX delivery-month-slot positions."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import torch

from stockagent.data.tw_futures_portfolio_daily import (
    TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION,
)


@dataclass(slots=True)
class FuturesPortfolioTensorResult:
    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    weights_history: torch.Tensor
    final_weights: torch.Tensor
    final_alive: torch.Tensor


@dataclass(slots=True)
class FuturesPortfolioNumpyResult:
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    weights_history: np.ndarray
    final_weights: np.ndarray
    final_alive: np.ndarray


def _validate_shapes(
    weights_shape: tuple[int, ...],
    returns_shape: tuple[int, ...],
    tradable_shape: tuple[int, ...],
    liquidation_shape: tuple[int, ...],
) -> None:
    if len(weights_shape) != 2 or weights_shape[0] <= 0 or weights_shape[1] <= 0:
        raise ValueError("TAIFEX futures portfolio weights must have shape [T,S]")
    if returns_shape != weights_shape:
        raise ValueError("holding_log_returns must match weights [T,S]")
    if tradable_shape != weights_shape:
        raise ValueError("tradable_mask must match weights [T,S]")
    if liquidation_shape != weights_shape:
        raise ValueError("must_liquidate_mask must match weights [T,S]")


def run_tw_futures_portfolio_continuous_torch(
    target_weights: torch.Tensor,
    holding_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    must_liquidate_mask: torch.Tensor,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
) -> FuturesPortfolioTensorResult:
    """Run the no-roll-gap cross-session futures ledger.

    ``must_liquidate_mask[t,s]`` is an after-return close.  The position earns
    either open[t] -> open[t+1] on the same physical contract or open[t] ->
    close[t] before being flattened.  It is intentionally different from the
    cash-market pre-row ``force_exit_mask`` convention.
    """

    _validate_shapes(
        tuple(target_weights.shape),
        tuple(holding_log_returns.shape),
        tuple(tradable_mask.shape),
        tuple(must_liquidate_mask.shape),
    )
    if buy_fee_rate < 0.0 or sell_fee_rate < 0.0:
        raise ValueError("futures portfolio fee rates must be non-negative")
    if max_turnover_ratio < 0.0:
        raise ValueError("max_turnover_ratio must be non-negative")
    weights = target_weights.to(dtype=torch.float32)
    returns = holding_log_returns.to(device=weights.device, dtype=torch.float32)
    tradable = tradable_mask.to(device=weights.device, dtype=torch.bool)
    liquidate = must_liquidate_mask.to(device=weights.device, dtype=torch.bool)
    simple_returns = torch.expm1(returns)
    t_len, n_symbols = weights.shape
    history = (
        torch.empty((t_len, n_symbols), device=weights.device, dtype=torch.float32)
        if return_weights_history
        else torch.empty((0, n_symbols), device=weights.device, dtype=torch.float32)
    )
    strategy = torch.empty((t_len,), device=weights.device, dtype=torch.float32)
    turnovers = torch.empty_like(strategy)
    prev = (
        torch.zeros((n_symbols,), device=weights.device, dtype=torch.float32)
        if initial_weights is None
        else torch.nan_to_num(
            initial_weights.detach().clone(memory_format=torch.contiguous_format).to(
                device=weights.device, dtype=torch.float32
            ),
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
    )
    alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().clone().to(device=weights.device, dtype=torch.bool).reshape(())
    )
    if volume_limit_weights is not None:
        volume_limits = volume_limit_weights.to(device=weights.device, dtype=torch.float32)
        if tuple(volume_limits.shape) != tuple(weights.shape):
            raise ValueError("volume_limit_weights must match weights [T,S]")
    else:
        volume_limits = None
    advance = (
        torch.ones((t_len,), device=weights.device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=weights.device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (t_len,):
        raise ValueError("state_advance_mask must have shape [T]")

    for row in range(t_len):
        prev = torch.where(alive, prev, torch.zeros_like(prev))
        row_advances = advance[row]
        desired = torch.where(
            alive & row_advances & tradable[row],
            torch.nan_to_num(weights[row], nan=0.0, posinf=0.0, neginf=0.0),
            prev,
        )
        delta = desired - prev
        if volume_limits is not None:
            cap = torch.where(
                torch.isfinite(volume_limits[row]) & (volume_limits[row] >= 0.0),
                volume_limits[row],
                delta.abs(),
            )
            delta = torch.sign(delta) * torch.minimum(delta.abs(), cap)
        if max_turnover_ratio > 0.0:
            turnover = delta.abs().sum()
            scale = torch.minimum(
                torch.ones_like(turnover),
                torch.as_tensor(max_turnover_ratio, device=weights.device)
                / turnover.clamp_min(1.0e-12),
            )
            delta = delta * scale
        held = prev + delta
        opening_buy = delta.clamp_min(0.0).sum()
        opening_sell = (-delta).clamp_min(0.0).sum()
        valid_return = torch.isfinite(simple_returns[row]) | (held.abs() <= 1.0e-8)
        invalid_active = ~valid_return.all()
        clean_asset_return = torch.where(
            row_advances & torch.isfinite(simple_returns[row]),
            simple_returns[row],
            torch.zeros_like(simple_returns[row]),
        )
        ending_notional = held * (1.0 + clean_asset_return)
        closing_notional = torch.where(
            row_advances & liquidate[row],
            ending_notional,
            torch.zeros_like(ending_notional),
        )
        forced_buy = (-closing_notional).clamp_min(0.0).sum()
        forced_sell = closing_notional.clamp_min(0.0).sum()
        gross_simple = torch.where(
            row_advances,
            (held * clean_asset_return).sum(),
            torch.zeros((), device=weights.device, dtype=torch.float32),
        )
        net_simple = (
            gross_simple
            - float(buy_fee_rate) * (opening_buy + forced_buy)
            - float(sell_fee_rate) * (opening_sell + forced_sell)
        )
        survived = (
            alive
            & ~invalid_active
            & torch.isfinite(net_simple)
            & (net_simple > -1.0)
        )
        safe_net = torch.where(
            survived,
            net_simple,
            torch.full_like(net_simple, -1.0 + 1.0e-7),
        )
        strategy[row] = torch.log1p(safe_net)
        turnovers[row] = opening_buy + opening_sell + forced_buy + forced_sell
        if return_weights_history:
            history[row] = held
        denominator = (1.0 + safe_net).clamp_min(1.0e-7)
        prev = ending_notional / denominator
        prev = torch.where(
            row_advances & liquidate[row], torch.zeros_like(prev), prev
        )
        alive = survived
        prev = torch.where(alive, prev, torch.zeros_like(prev))
    return FuturesPortfolioTensorResult(
        strategy_returns=strategy,
        turnovers=turnovers,
        weights_history=history,
        final_weights=prev,
        final_alive=alive,
    )


def run_tw_futures_portfolio_continuous_numpy(
    target_weights: np.ndarray,
    holding_log_returns: np.ndarray,
    tradable_mask: np.ndarray,
    must_liquidate_mask: np.ndarray,
    *,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float = 0.0,
) -> FuturesPortfolioNumpyResult:
    _validate_shapes(
        tuple(np.shape(target_weights)),
        tuple(np.shape(holding_log_returns)),
        tuple(np.shape(tradable_mask)),
        tuple(np.shape(must_liquidate_mask)),
    )
    weights = np.nan_to_num(np.asarray(target_weights, dtype=np.float64))
    returns = np.expm1(np.asarray(holding_log_returns, dtype=np.float64))
    tradable = np.asarray(tradable_mask, dtype=bool)
    liquidate = np.asarray(must_liquidate_mask, dtype=bool)
    t_len, n_symbols = weights.shape
    history = np.zeros((t_len, n_symbols), dtype=np.float32)
    strategy = np.zeros(t_len, dtype=np.float32)
    turnovers = np.zeros(t_len, dtype=np.float32)
    prev = np.zeros(n_symbols, dtype=np.float64)
    alive = True
    for row in range(t_len):
        if not alive:
            prev.fill(0.0)
        desired = np.where(alive & tradable[row], weights[row], prev)
        delta = desired - prev
        if max_turnover_ratio > 0.0:
            turnover = float(np.abs(delta).sum())
            if turnover > max_turnover_ratio:
                delta *= max_turnover_ratio / turnover
        held = prev + delta
        opening_buy = float(np.clip(delta, 0.0, None).sum())
        opening_sell = float(np.clip(-delta, 0.0, None).sum())
        valid_return = np.isfinite(returns[row]) | (np.abs(held) <= 1.0e-8)
        clean_return = np.where(np.isfinite(returns[row]), returns[row], 0.0)
        ending_notional = held * (1.0 + clean_return)
        closing = np.where(liquidate[row], ending_notional, 0.0)
        forced_buy = float(np.clip(-closing, 0.0, None).sum())
        forced_sell = float(np.clip(closing, 0.0, None).sum())
        gross = float(np.sum(held * clean_return))
        net = (
            gross
            - buy_fee_rate * (opening_buy + forced_buy)
            - sell_fee_rate * (opening_sell + forced_sell)
        )
        survived = bool(
            alive
            and valid_return.all()
            and np.isfinite(net)
            and net > -1.0
        )
        safe_net = net if survived else -1.0 + 1.0e-7
        strategy[row] = np.log1p(safe_net)
        turnovers[row] = opening_buy + opening_sell + forced_buy + forced_sell
        history[row] = held.astype(np.float32)
        prev = ending_notional / max(1.0 + safe_net, 1.0e-7)
        prev[liquidate[row]] = 0.0
        alive = survived
        if not alive:
            prev.fill(0.0)
    return FuturesPortfolioNumpyResult(
        strategy_returns=strategy,
        turnovers=turnovers,
        weights_history=history,
        final_weights=prev.astype(np.float32),
        final_alive=np.asarray(alive, dtype=bool),
    )


__all__ = [
    "FuturesPortfolioNumpyResult",
    "FuturesPortfolioTensorResult",
    "TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION",
    "run_tw_futures_portfolio_continuous_numpy",
    "run_tw_futures_portfolio_continuous_torch",
]
