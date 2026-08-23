"""Shared differentiable portfolio-allocation operators."""

from __future__ import annotations

import math

import torch


def stateful_proximal_target_weights(
    requested_weights: torch.Tensor,
    previous_executed_weights: torch.Tensor,
    *,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    cost_multiplier: float,
    long_only: bool,
) -> torch.Tensor:
    """Resolve a cost-aware target around the actual pre-trade portfolio.

    This is the proximal map for a quadratic pull toward the model proposal and
    an asymmetric L1 switching cost.  It creates an endogenous no-trade region:
    the model remains free to choose direction, gross exposure, concentration,
    and cash, while proposed changes smaller than the applicable one-way cost
    stay at the carried position.

    The last tensor axis is the asset axis.  Leading axes, when present, are
    independent portfolios.  A completely flat portfolio keeps the raw model
    proposal so the real entry fee supplies a usable learning gradient instead
    of trapping the policy forever at the zero solution.
    """

    if requested_weights.shape != previous_executed_weights.shape:
        raise ValueError("requested and previous weights must have one shape")
    if requested_weights.dim() < 1:
        raise ValueError("stateful proximal allocator expects an asset axis")
    multiplier = float(cost_multiplier)
    if not math.isfinite(multiplier) or multiplier < 0.0:
        raise ValueError("proximal cost multiplier must be finite and nonnegative")

    requested = requested_weights.float()
    previous = previous_executed_weights.float()
    if long_only:
        requested = requested.clamp_min(0.0)
        previous = previous.clamp_min(0.0)

    symbols = int(requested.size(-1))

    def fee_vector(value: torch.Tensor | float, name: str) -> torch.Tensor:
        fee = torch.as_tensor(value, device=requested.device, dtype=torch.float32)
        if fee.dim() == 0:
            fee = fee.expand(symbols)
        if tuple(fee.shape) != (symbols,):
            raise ValueError(f"{name} must be scalar or have shape [S]")
        return fee

    buy = fee_vector(buy_fee_rates, "buy_fee_rates")
    sell = fee_vector(sell_fee_rates, "sell_fee_rates")
    delta = requested - previous
    leading_dims = (1,) * (delta.dim() - 1)
    fee_threshold = (
        torch.where(
            delta >= 0.0,
            buy.view(*leading_dims, symbols),
            sell.view(*leading_dims, symbols),
        )
        * multiplier
    )
    shrunk_delta = torch.sign(delta) * torch.relu(delta.abs() - fee_threshold)
    changed = shrunk_delta.abs().sum(dim=-1, keepdim=True) > 0.0
    candidate = previous + shrunk_delta
    if long_only:
        candidate = candidate.clamp_min(0.0)

    # Do not L1-normalize this candidate.  Each coordinate lies between the
    # previous and requested endpoints, so two feasible L1 portfolios already
    # imply a feasible candidate.  Re-normalization would recreate turnover
    # removed by the no-trade region and prevent a deliberate move into cash.
    previous_gross = previous.abs().sum(dim=-1, keepdim=True)
    initial_entry = previous_gross <= 1.0e-12
    return torch.where(
        initial_entry,
        requested,
        torch.where(changed, candidate, previous),
    )


__all__ = ["stateful_proximal_target_weights"]
