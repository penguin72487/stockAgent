from __future__ import annotations

import math

import torch
import torch.nn.functional as F

from stockagent.portfolio_contract import (
    DEFAULT_PORTFOLIO_ACTIVATION,
    normalize_portfolio_activation,
)
from stockagent.profiling import profile_range

PORTFOLIO_L1_EPS = 1.0e-8


def apply_portfolio_activation(logits: torch.Tensor, activation: str | None = None) -> torch.Tensor:
    with profile_range("portfolio.activation"):
        activation_name = normalize_portfolio_activation(activation)
        if activation_name in {"identity", "pre_normalized"}:
            return torch.nan_to_num(logits, nan=0.0)
        logits = torch.nan_to_num(logits, nan=0.0)
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


def masked_cross_sectional_mean_finite(
    values: torch.Tensor,
    mask: torch.Tensor | None,
) -> torch.Tensor:
    """Masked cross-sectional mean for callers that already guarantee finite values."""
    if mask is None:
        return values.mean(dim=1, keepdim=True)

    mask_f = mask.to(dtype=values.dtype)
    denom = mask_f.sum(dim=1, keepdim=True).clamp_min(1.0)
    return (values * mask_f).sum(dim=1, keepdim=True) / denom


def masked_cross_sectional_mean(values: torch.Tensor, mask: torch.Tensor | None) -> torch.Tensor:
    values = torch.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    return masked_cross_sectional_mean_finite(values, mask)


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
    eps: float = PORTFOLIO_L1_EPS,
) -> torch.Tensor:
    """Convert scores to portfolio weights via optional activation + L1 normalization.

    With the default identity activation, raw scores are normalized directly.
    Other activations can be selected as post-processing transforms.  The L1
    denominator controls gross exposure and keeps the row sum of absolute
    weights at 1 for non-empty active rows.  For long-only callers, negative
    activated outputs are clipped out.
    """
    with profile_range("portfolio.activation_l1"):
        weights = apply_portfolio_activation(logits, activation)
        if long_only:
            weights = weights.clamp_min(0.0)
        if mask is None:
            with profile_range("portfolio.normalize_l1"):
                denom = weights.abs().sum(dim=1, keepdim=True)
                return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))

        mask_bool = mask.bool()
        with profile_range("portfolio.mask_where"):
            weights = torch.where(mask_bool, weights, torch.zeros_like(weights))
        with profile_range("portfolio.normalize_l1"):
            denom = weights.abs().sum(dim=1, keepdim=True)
            return torch.where(denom > 0.0, weights / denom.clamp_min(float(eps)), torch.zeros_like(weights))


def masked_cash_asset_l1_weights(
    stock_logits: torch.Tensor,
    cash_logit: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    activation: str | None = "identity",
    eps: float = PORTFOLIO_L1_EPS,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Jointly allocate one explicit cash action and signed stock actions.

    The cash action is a model score, not residual buying power.  Its positive
    score competes in the same L1 denominator as every stock score, so every
    row satisfies ``sum(abs(stock_weights)) + cash_weight == 1`` (up to
    floating-point rounding).  Cash cannot be short, hence the smooth
    ``softplus`` mapping from its unconstrained model logit.
    """

    if stock_logits.ndim != 2:
        raise ValueError("cash-L1 stock logits must have shape [B,S]")
    if cash_logit.ndim == 1:
        cash_logit = cash_logit.unsqueeze(1)
    if tuple(cash_logit.shape) != (int(stock_logits.size(0)), 1):
        raise ValueError("cash-L1 cash logit must have shape [B] or [B,1]")

    stock_scores = apply_portfolio_activation(stock_logits, activation).float()
    if long_only:
        stock_scores = stock_scores.clamp_min(0.0)
    if mask is not None:
        mask_bool = mask.to(device=stock_logits.device, dtype=torch.bool)
        if tuple(mask_bool.shape) != tuple(stock_logits.shape):
            raise ValueError("cash-L1 mask must match stock logits")
        stock_scores = stock_scores.masked_fill(~mask_bool, 0.0)

    clean_cash_logit = torch.nan_to_num(
        cash_logit.float(), nan=0.0, posinf=0.0, neginf=0.0
    )
    positive_cash_score = F.softplus(clean_cash_logit)
    denominator = stock_scores.abs().sum(dim=1, keepdim=True) + positive_cash_score
    denominator = denominator.clamp_min(float(eps))
    stock_weights = stock_scores / denominator
    cash_weight = positive_cash_score / denominator
    return stock_weights, cash_weight.squeeze(1), positive_cash_score.squeeze(1)


def _masked_distribution(
    logits: torch.Tensor,
    mask: torch.Tensor,
    *,
    transform: str,
    eps: float = 1e-12,
) -> torch.Tensor:
    with profile_range(f"portfolio.distribution.{transform}"):
        transform_name = str(transform).strip().lower().replace("-", "_")
        if transform_name in {"softmax", "action_softmax"}:
            mask_fill = finite_mask_fill_value(logits)
            safe_logits = torch.nan_to_num(logits, nan=0.0)
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
    with profile_range("portfolio.sparsemax"):
        mask_bool = mask.bool()
        logits_f = torch.nan_to_num(logits.float(), nan=0.0)
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
    with profile_range("portfolio.entmax15"):
        mask_bool = mask.bool()
        logits_f = torch.nan_to_num(logits.float(), nan=0.0)
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
    short_mask: torch.Tensor | None = None,
    cash_logit: float = 0.0,
    return_parts: bool = False,
) -> torch.Tensor | tuple[torch.Tensor, dict[str, torch.Tensor]]:
    """Allocate over long/short/cash actions and return net portfolio weights.

    Long/short mode creates a single action distribution over ``[long_i]``,
    ``[short_i]``, and one cash action.  Net holding is ``long_i - short_i``.
    ``short_mask`` may further restrict which otherwise tradable instruments
    have a short action; this supports mixed universes such as signed futures
    plus long-only options without inventing an executable short-option leg.
    This keeps the permitted long/short ratio free and leaves unused gross
    exposure as implicit cash.  ``sparsemax`` and ``entmax15`` use the same
    action set but can produce exact zero actions.
    """
    with profile_range("portfolio.action_head"):
        if mask is None:
            mask_bool = torch.ones_like(logits, dtype=torch.bool)
        else:
            mask_bool = mask.to(device=logits.device, dtype=torch.bool)
        if short_mask is None:
            short_mask_bool = mask_bool
        else:
            short_mask_bool = short_mask.to(
                device=logits.device, dtype=torch.bool
            )
            if tuple(short_mask_bool.shape) != tuple(logits.shape):
                raise ValueError("short_mask must match logits")
            short_mask_bool = short_mask_bool & mask_bool
        clean_logits = torch.nan_to_num(logits, nan=0.0)
        with profile_range("portfolio.action_mask_where"):
            clean_logits = clean_logits.masked_fill(~mask_bool, 0.0)
        cash = torch.full(
            (clean_logits.size(0), 1),
            float(cash_logit),
            device=clean_logits.device,
            dtype=clean_logits.dtype,
        )
        cash_mask = torch.ones((clean_logits.size(0), 1), device=clean_logits.device, dtype=torch.bool)

        with profile_range("portfolio.action_allocation"):
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
                action_mask = torch.cat(
                    [mask_bool, short_mask_bool, cash_mask], dim=1
                )
                probs = _masked_distribution(action_logits, action_mask, transform=transform)
                width = clean_logits.size(1)
                long_alloc = probs[:, :width].masked_fill(~mask_bool, 0.0)
                short_alloc = probs[:, width : 2 * width].masked_fill(
                    ~short_mask_bool, 0.0
                )
                cash_alloc = probs[:, 2 * width]
                weights = long_alloc - short_alloc

        with profile_range("portfolio.action_final_mask"):
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


def masked_cash_entmax15_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    short_mask: torch.Tensor | None = None,
    radius: float = 1.0,
    eps: float = PORTFOLIO_L1_EPS,
    return_parts: bool = False,
) -> torch.Tensor | tuple[torch.Tensor, dict[str, torch.Tensor]]:
    """Sparse signed allocation with dimension-invariant residual cash.

    Entmax-1.5 chooses relative legs from legal directional evidence. Each
    selected leg keeps its own parameter-free conviction
    ``score / (1 + abs(score))``; unused gross remains cash. Therefore zero
    evidence is all cash, one strong sparse leg is not averaged over thousands
    of weak candidates, and gross stays below ``radius`` without normalizing
    every nonzero row to the boundary.
    """

    if logits.ndim != 2:
        raise ValueError("cash-entmax logits must have shape [B,S]")
    radius_value = float(radius)
    if not math.isfinite(radius_value) or not 0.0 < radius_value <= 1.0:
        raise ValueError("cash-entmax radius must be in (0,1]")
    mask_bool = (
        torch.ones_like(logits, dtype=torch.bool)
        if mask is None
        else mask.to(device=logits.device, dtype=torch.bool)
    )
    if tuple(mask_bool.shape) != tuple(logits.shape):
        raise ValueError("cash-entmax mask must match logits")
    if short_mask is None:
        short_mask_bool = mask_bool
    else:
        short_mask_bool = short_mask.to(device=logits.device, dtype=torch.bool)
        if tuple(short_mask_bool.shape) != tuple(logits.shape):
            raise ValueError("short_mask must match logits")
        short_mask_bool = short_mask_bool & mask_bool

    clean = torch.nan_to_num(logits.float(), nan=0.0, posinf=0.0, neginf=0.0)
    legal_evidence = torch.where(
        short_mask_bool,
        clean.abs(),
        clean.clamp_min(0.0),
    ).masked_fill(~mask_bool, 0.0)
    direction = torch.where(
        short_mask_bool,
        clean.sign(),
        torch.ones_like(clean),
    )
    relative = _masked_entmax15(legal_evidence, mask_bool, eps=eps).float()
    conviction = legal_evidence / (1.0 + legal_evidence)
    weights = (
        relative * direction * conviction * radius_value
    ).masked_fill(~mask_bool, 0.0)
    weights = weights.to(dtype=logits.dtype)
    if not return_parts:
        return weights
    gross = weights.float().abs().sum(dim=1)
    return weights, {
        "cash_entmax_relative_alloc": relative.to(dtype=logits.dtype),
        "cash_entmax_conviction": conviction.to(dtype=logits.dtype),
        "cash_entmax_risk_fraction": (gross / radius_value).clamp(0.0, 1.0),
        "cash_entmax_cash_fraction": (1.0 - gross).clamp_min(0.0),
        "implicit_cash_weight": (1.0 - gross).clamp_min(0.0),
    }


def masked_l1_projection_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    radius: float = 1.0,
    eps: float = 1e-12,
    scale_by_active_count: bool = False,
) -> torch.Tensor:
    """Project raw holdings onto the masked L1 ball ``sum(abs(w)) <= radius``.

    Rows already inside the legal set are preserved, so cash remains possible.
    Rows outside the set are soft-thresholded; this gives exact zeros and keeps
    the long/short ratio unconstrained apart from the gross exposure cap.

    ``scale_by_active_count`` gives each raw score an average-position meaning:
    before projection, score ``1`` on every active asset is one unit of gross
    exposure, independent of universe width.  This is essential for large,
    time-varying universes.  Without it, an O(1) score head produces O(S) raw
    gross exposure, so almost every row is pinned to the L1 boundary and the
    sparse projection active set supplies a discontinuous, mostly-zero
    gradient.  The option is explicit and disabled by default to preserve
    historical checkpoint semantics.
    """
    with profile_range("portfolio.projection_l1"):
        if mask is None:
            mask_bool = torch.ones_like(logits, dtype=torch.bool)
        else:
            mask_bool = mask.to(device=logits.device, dtype=torch.bool)
        clean = torch.nan_to_num(logits.float(), nan=0.0)
        clean = clean.masked_fill(~mask_bool, 0.0)
        if long_only:
            clean = clean.clamp_min(0.0)
        if scale_by_active_count:
            active_count = mask_bool.sum(dim=1, keepdim=True).clamp_min(1)
            clean = clean / active_count.to(dtype=clean.dtype)

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
        # The sort/threshold projection is evaluated in FP32 for the recurrent
        # finance boundary.  With extremely large but finite logits, subtracting
        # two O(1e15) values can leave an O(1e7) cancellation residual.  Enforce
        # the public L1-ball invariant after projection so one pathological row
        # can never create an unbounded order request or misleading artifact.
        projected_l1 = projected.abs().sum(dim=1, keepdim=True)
        legal_scale = torch.minimum(
            torch.ones_like(projected_l1),
            radius_t / projected_l1.clamp_min(float(eps)),
        )
        projected = projected * legal_scale
        # Portfolio weights feed recurrent fee, funding, turnover, and NAV
        # accounting.  Keep that public boundary in FP32 under FP16/BF16 AMP;
        # casting the already-FP32 projection back to an autocast dtype loses
        # small holdings before the ledger sees them.  Preserve the caller's
        # dtype for ordinary FP32/FP64 use so non-AMP numerical contracts do
        # not change.
        if logits.dtype in {torch.float16, torch.bfloat16}:
            return projected
        return projected.to(dtype=logits.dtype)


def masked_softsign_l1_weights(
    logits: torch.Tensor,
    mask: torch.Tensor | None,
    *,
    long_only: bool = False,
    eps: float = PORTFOLIO_L1_EPS,
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
    eps: float = PORTFOLIO_L1_EPS,
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
