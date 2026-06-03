from __future__ import annotations

import os
from pathlib import Path

import torch
from torch.utils.cpp_extension import load

_CPP_EXT = None
_CPP_EXT_ERROR: str | None = None


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "on", "yes"}


def cpp_long_short_enabled() -> bool:
    return _env_truthy("STOCKAGENT_USE_CPP_BACKTEST_EXT", "0")


def _load_cpp_ext():
    global _CPP_EXT, _CPP_EXT_ERROR
    if _CPP_EXT is not None:
        return _CPP_EXT
    if _CPP_EXT_ERROR is not None:
        return None

    source = Path(__file__).resolve().parent / "csrc" / "backtest_long_short_ext.cpp"
    if not source.exists():
        _CPP_EXT_ERROR = f"missing source file: {source}"
        return None

    try:
        _CPP_EXT = load(
            name="stockagent_backtest_long_short_ext",
            sources=[str(source)],
            extra_cflags=["-O3"],
            verbose=_env_truthy("STOCKAGENT_CPP_EXT_VERBOSE", "0"),
        )
    except Exception as exc:
        _CPP_EXT_ERROR = str(exc)
        return None
    return _CPP_EXT


class _LongShortBacktestFunction(torch.autograd.Function):
    @staticmethod
    def forward(
        ctx,
        weights: torch.Tensor,
        future_returns: torch.Tensor,
        tradable_mask: torch.Tensor,
        can_buy_mask: torch.Tensor,
        can_sell_mask: torch.Tensor,
        buy_fee_rate: float,
        sell_fee_rate: float,
        max_turnover_ratio: float,
        gross_budget: float,
    ):
        ext = _load_cpp_ext()
        if ext is None:
            raise RuntimeError(f"C++ extension unavailable: {_CPP_EXT_ERROR}")

        (
            strategy_returns,
            turnovers,
            weights_history,
            allowed_mask,
            turnover_scales,
            gross_scales,
            delta_sign,
        ) = ext.long_short_forward(
            weights,
            future_returns,
            tradable_mask,
            can_buy_mask,
            can_sell_mask,
            float(buy_fee_rate),
            float(sell_fee_rate),
            float(max_turnover_ratio),
            float(gross_budget),
        )

        # Save only tensors needed for custom reverse scan; masks/fees are treated as constants.
        ctx.save_for_backward(
            future_returns,
            allowed_mask,
            turnover_scales,
            gross_scales,
            delta_sign,
        )
        return strategy_returns, turnovers, weights_history

    @staticmethod
    def backward(ctx, grad_strategy_returns, grad_turnovers, grad_weights_history):
        del grad_weights_history  # weights history gradients are intentionally ignored in this path

        future_returns, allowed_mask, turnover_scales, gross_scales, delta_sign = ctx.saved_tensors
        ext = _load_cpp_ext()
        if ext is None:
            raise RuntimeError(f"C++ extension unavailable during backward: {_CPP_EXT_ERROR}")

        if grad_strategy_returns is None:
            grad_strategy_returns = torch.zeros(
                future_returns.size(0), device=future_returns.device, dtype=future_returns.dtype
            )
        if grad_turnovers is None:
            grad_turnovers = torch.zeros(
                future_returns.size(0), device=future_returns.device, dtype=future_returns.dtype
            )

        grad_weights = ext.long_short_backward(
            grad_strategy_returns.contiguous(),
            grad_turnovers.contiguous(),
            future_returns,
            allowed_mask,
            turnover_scales,
            gross_scales,
            delta_sign,
        )

        return (
            grad_weights,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )


def run_long_short_cpp_autograd(
    weights: torch.Tensor,
    future_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rate: float,
    sell_fee_rate: float,
    max_turnover_ratio: float,
    gross_budget: float,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    return _LongShortBacktestFunction.apply(
        weights,
        future_returns,
        tradable_mask,
        can_buy_mask,
        can_sell_mask,
        float(buy_fee_rate),
        float(sell_fee_rate),
        float(max_turnover_ratio),
        float(gross_budget),
    )
