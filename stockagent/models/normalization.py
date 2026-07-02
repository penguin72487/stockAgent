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
        "already_normalized": "pre_normalized",
        "inverse_square_root_unit": "isru",
        "inverse_sqrt": "isru",
        "inverse_sqrt_unit": "isru",
        "isr": "isru",
        "isru1": "isru",
        "pre_normalized_weights": "pre_normalized",
        "preserve": "pre_normalized",
        "preserve_weights": "pre_normalized",
        "soft_sign": "softsign",
        "weights": "pre_normalized",
        "x_over_1_abs_x": "softsign",
        "x_over_sqrt_1_x2": "isru",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized in {"identity", "linear", "none", "raw"}:
        return "identity"
    valid = {"tanh", "softsign", "isru", "erf", "atan", "gudermannian", "pre_normalized"}
    if normalized not in valid:
        raise ValueError(
            "portfolio activation must be one of "
            "'identity', 'tanh', 'softsign', 'isru', 'erf', 'atan', 'gd', or 'pre_normalized'"
        )
    return normalized


def apply_portfolio_activation(logits: torch.Tensor, activation: str | None = None) -> torch.Tensor:
    activation_name = normalize_portfolio_activation(activation)
    if activation_name in {"identity", "pre_normalized"}:
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


def _masked_distribution(
    logits: torch.Tensor,
    mask: torch.Tensor,
    *,
    transform: str,
    eps: float = 1e-12,
) -> torch.Tensor:
    transform_name = str(transform).strip().lower().replace("-", "_")
    if transform_name in {"softmax", "action_softmax"}:
        mask_fill = finite_mask_fill_value(logits)
        safe_logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
        safe_logits = safe_logits.masked_fill(~mask, mask_fill)
        probs = torch.softmax(safe_logits.float(), dim=1).to(dtype=logits.dtype)
        return probs.masked_fill(~mask, 0.0)
    if transform_name in {"sparsemax", "action_sparsemax"}:
        return _masked_sparsemax(logits, mask, eps=eps)
    if transform_name in {"entmax", "entmax15", "entmax_15", "action_entmax", "action_entmax15"}:
        return _masked_entmax15(logits, mask, eps=eps)
    raise ValueError("signed action transform must be 'softmax', 'sparsemax', or 'entmax15'")


def _masked_sparsemax(
    logits: torch.Tensor,
    mask: torch.Tensor,
    *,
    eps: float = 1e-12,
) -> torch.Tensor:
    """Masked sparsemax over dim=1."""
    mask_bool = mask.bool()
    logits_f = torch.nan_to_num(logits.float(), nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    valid_count = mask_bool.sum(dim=1, keepdim=True)
    safe_logits = logits_f.masked_fill(~mask_bool, -1e9)
    sorted_logits = torch.sort(safe_logits, dim=1, descending=True).values
    cssv = sorted_logits.cumsum(dim=1)
    k = torch.arange(1, logits.size(1) + 1, device=logits.device, dtype=logits_f.dtype).view(1, -1)
    support = 1.0 + k * sorted_logits > cssv
    k_z = support.sum(dim=1, keepdim=True).clamp_min(1)
    tau = (cssv.gather(1, k_z.long() - 1) - 1.0) / k_z.to(dtype=logits_f.dtype)
    probs = (safe_logits - tau).clamp_min(0.0).masked_fill(~mask_bool, 0.0)
    denom = probs.sum(dim=1, keepdim=True)
    probs = torch.where(
        (valid_count > 0) & (denom > 0.0),
        probs / denom.clamp_min(float(eps)),
        torch.zeros_like(probs),
    )
    return probs.to(dtype=logits.dtype)


def _masked_entmax15(
    logits: torch.Tensor,
    mask: torch.Tensor,
    *,
    eps: float = 1e-12,
    n_iter: int = 32,
) -> torch.Tensor:
    """Masked alpha=1.5 entmax over dim=1.

    For alpha=1.5, p_i = relu((z_i - tau) / 2)^2.  Bisection finds tau so
    each valid row sums to one.  The operation is differentiable almost
    everywhere, and exact zeros are the useful sparse case.
    """
    mask_bool = mask.bool()
    logits_f = torch.nan_to_num(logits.float(), nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    valid_count = mask_bool.sum(dim=1, keepdim=True)
    safe_logits = logits_f.masked_fill(~mask_bool, -1e9)
    max_val = safe_logits.max(dim=1, keepdim=True).values
    tau_lo = max_val - 2.0 * valid_count.clamp_min(1).to(dtype=logits_f.dtype).sqrt()
    tau_hi = max_val
    for _ in range(int(n_iter)):
        tau_mid = (tau_lo + tau_hi) * 0.5
        probs_mid = ((logits_f - tau_mid).clamp_min(0.0) * 0.5).square().masked_fill(~mask_bool, 0.0)
        too_large = probs_mid.sum(dim=1, keepdim=True) > 1.0
        tau_lo = torch.where(too_large, tau_mid, tau_lo)
        tau_hi = torch.where(too_large, tau_hi, tau_mid)

    tau = (tau_lo + tau_hi) * 0.5
    probs = ((logits_f - tau).clamp_min(0.0) * 0.5).square().masked_fill(~mask_bool, 0.0)
    denom = probs.sum(dim=1, keepdim=True)
    probs = torch.where(
        (valid_count > 0) & (denom > 0.0),
        probs / denom.clamp_min(float(eps)),
        torch.zeros_like(probs),
    )
    return probs.to(dtype=logits.dtype)


def masked_signed_action_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    transform: str = "softmax",
    long_only: bool = False,
    cash_logit: float = 0.0,
    return_parts: bool = False,
) -> torch.Tensor | tuple[torch.Tensor, dict[str, torch.Tensor]]:
    """Allocate over long/short/cash actions and return net portfolio weights.

    Long/short mode creates a single action distribution over ``[long_i]``,
    ``[short_i]``, and one cash action.  Net holding is ``long_i - short_i``.
    This keeps the long/short ratio free and leaves unused gross exposure as
    implicit cash.  ``sparsemax`` and ``entmax15`` use the same action set but
    can produce exact zero actions.
    """
    if mask is None:
        mask_bool = torch.ones_like(logits, dtype=torch.bool)
    else:
        mask_bool = mask.to(device=logits.device, dtype=torch.bool)
    clean_logits = torch.nan_to_num(logits, nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    clean_logits = clean_logits.masked_fill(~mask_bool, 0.0)
    cash = torch.full(
        (clean_logits.size(0), 1),
        float(cash_logit),
        device=clean_logits.device,
        dtype=clean_logits.dtype,
    )
    cash_mask = torch.ones((clean_logits.size(0), 1), device=clean_logits.device, dtype=torch.bool)

    if long_only:
        action_logits = torch.cat([clean_logits, cash], dim=1)
        action_mask = torch.cat([mask_bool, cash_mask], dim=1)
        probs = _masked_distribution(action_logits, action_mask, transform=transform)
        long_alloc = probs[:, : clean_logits.size(1)].masked_fill(~mask_bool, 0.0)
        short_alloc = torch.zeros_like(long_alloc)
        cash_alloc = probs[:, clean_logits.size(1)]
        weights = long_alloc
    else:
        action_logits = torch.cat([clean_logits, -clean_logits, cash], dim=1)
        action_mask = torch.cat([mask_bool, mask_bool, cash_mask], dim=1)
        probs = _masked_distribution(action_logits, action_mask, transform=transform)
        width = clean_logits.size(1)
        long_alloc = probs[:, :width].masked_fill(~mask_bool, 0.0)
        short_alloc = probs[:, width : 2 * width].masked_fill(~mask_bool, 0.0)
        cash_alloc = probs[:, 2 * width]
        weights = long_alloc - short_alloc

    weights = weights.masked_fill(~mask_bool, 0.0)
    if not return_parts:
        return weights
    parts = {
        "action_long_alloc": long_alloc,
        "action_short_alloc": short_alloc,
        "action_cash_alloc": cash_alloc,
        "implicit_cash_weight": (1.0 - weights.abs().sum(dim=1)).clamp_min(0.0),
    }
    return weights, parts


def masked_l1_projection_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    radius: float = 1.0,
    eps: float = 1e-12,
) -> torch.Tensor:
    """Project raw holdings onto the masked L1 ball ``sum(abs(w)) <= radius``.

    Rows already inside the legal set are preserved, so cash remains possible.
    Rows outside the set are soft-thresholded; this gives exact zeros and keeps
    the long/short ratio unconstrained apart from the gross exposure cap.
    """
    if mask is None:
        mask_bool = torch.ones_like(logits, dtype=torch.bool)
    else:
        mask_bool = mask.to(device=logits.device, dtype=torch.bool)
    clean = torch.nan_to_num(logits.float(), nan=0.0, posinf=20.0, neginf=-20.0).clamp(min=-20.0, max=20.0)
    clean = clean.masked_fill(~mask_bool, 0.0)
    if long_only:
        clean = clean.clamp_min(0.0)

    abs_clean = clean.abs()
    l1 = abs_clean.sum(dim=1, keepdim=True)
    radius_t = clean.new_tensor(max(0.0, float(radius)))
    inside = l1 <= (radius_t + float(eps))

    sorted_abs = torch.sort(abs_clean, dim=1, descending=True).values
    cssv = sorted_abs.cumsum(dim=1)
    idx = torch.arange(1, clean.size(1) + 1, device=clean.device, dtype=clean.dtype).view(1, -1)
    support = sorted_abs * idx > (cssv - radius_t)
    rho = support.sum(dim=1, keepdim=True).clamp_min(1)
    theta = (cssv.gather(1, rho.long() - 1) - radius_t) / rho.to(dtype=clean.dtype)
    projected = clean.sign() * (abs_clean - theta).clamp_min(0.0)
    projected = torch.where(inside, clean, projected)
    projected = projected.masked_fill(~mask_bool, 0.0)
    return projected.to(dtype=logits.dtype)


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
