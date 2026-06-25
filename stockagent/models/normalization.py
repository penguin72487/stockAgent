from __future__ import annotations

import math

import torch


DEFAULT_PORTFOLIO_ACTIVATION = "identity"


def normalize_portfolio_activation(activation: str | None) -> str:
    normalized = str(activation or DEFAULT_PORTFOLIO_ACTIVATION).strip().lower().replace("-", "_")
    aliases = {
        "arc_tan": "atan",
        "arctan": "atan",
        "erf_scaled": "erf",
        "gd": "gudermannian",
        "inverse_square_root_unit": "isru",
        "inverse_sqrt": "isru",
        "inverse_sqrt_unit": "isru",
        "isr": "isru",
        "isru1": "isru",
        "soft_sign": "softsign",
        "x_over_1_abs_x": "softsign",
        "x_over_sqrt_1_x2": "isru",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in {"identity", "linear", "none", "raw"}:
        return "identity"
    valid = {"tanh", "softsign", "isru", "erf", "atan", "gudermannian"}
    if normalized not in valid:
        raise ValueError(
            "portfolio activation must be one of "
            "'identity', 'tanh', 'softsign', 'isru', 'erf', 'atan', or 'gd'"
        )
    return normalized


def apply_portfolio_activation(logits: torch.Tensor, activation: str | None = None) -> torch.Tensor:
    activation_name = normalize_portfolio_activation(activation)
    if activation_name == "identity":
        return torch.where(torch.isfinite(logits), logits, torch.zeros_like(logits))
    logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    if activation_name == "tanh":
        return torch.tanh(logits)
    if activation_name == "softsign":
        return logits / (1.0 + logits.abs())
    if activation_name == "isru":
        return logits * torch.rsqrt(1.0 + logits.square())
    if activation_name == "erf":
        return torch.erf(logits * (math.sqrt(math.pi) / 2.0))
    if activation_name == "atan":
        return (2.0 / math.pi) * torch.atan(logits * (math.pi / 2.0))
    if activation_name == "gudermannian":
        return (2.0 / math.pi) * torch.atan(torch.sinh(logits * (math.pi / 2.0)))
    raise AssertionError(f"Unhandled portfolio activation: {activation_name}")


def finite_mask_fill_value(values: torch.Tensor) -> float:
    if not values.dtype.is_floating_point:
        return -1e9
    return float(torch.finfo(values.dtype).min)


def masked_cross_sectional_mean(values: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    values = torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    if mask is None:
        return values.mean(dim=1, keepdim=True)

    mask_f = mask.to(dtype=values.dtype)
    denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    return (values * mask_f).sum(dim=1, keepdim=True) / denom


def masked_softmax(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    activation: str | None = None,
) -> torch.Tensor:
    return masked_activation_l1_weights(logits, mask, long_only=True, activation=activation)


def masked_activation_l1_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    activation: str | None = None,
    eps: float = 1e-8,
) -> torch.Tensor:
    """Convert scores to portfolio weights via optional activation + L1 normalization.

    With the default identity activation, raw scores are normalized directly.
    Other activations can be selected as post-processing transforms.  The L1
    denominator controls gross exposure and keeps the row sum of absolute
    weights at 1 for non-empty active rows.  For long-only callers, negative
    activated outputs are clipped out.
    """
    weights = apply_portfolio_activation(logits, activation)
    if long_only:
        weights = weights.clamp_min(0.0)
    if mask is None:
        denom = weights.abs().sum(dim=1, keepdim=True)
        return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))

    mask_bool = mask.bool()
    weights = torch.where(mask_bool, weights, torch.zeros_like(weights))
    denom = weights.abs().sum(dim=1, keepdim=True)
    return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))


def masked_softsign_l1_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    eps: float = 1e-8,
) -> torch.Tensor:
    """Backward-compatible explicit softsign + L1 normalizer."""
    return masked_activation_l1_weights(
        logits,
        mask,
        long_only=long_only,
        activation="softsign",
        eps=eps,
    )


def masked_tanh_l1_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    eps: float = 1e-8,
) -> torch.Tensor:
    """Backward-compatible explicit tanh + L1 normalizer."""
    return masked_activation_l1_weights(
        logits,
        mask,
        long_only=long_only,
        activation="tanh",
        eps=eps,
    )


def dual_branch_softmax(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    activation: str | None = None,
) -> torch.Tensor:
    return masked_activation_l1_weights(logits, mask, long_only=False, activation=activation)
