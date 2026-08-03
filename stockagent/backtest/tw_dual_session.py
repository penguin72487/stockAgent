"""Canonical eager Taiwan open/close execution ledgers.

This module is the eager semantic oracle dispatched by
:mod:`stockagent.backtest.simulator` for two decisions per exchange session:

``tw_cash``
    ``actions[T, 2, S]`` contains signed post-event target weights for
    ``OPEN`` and ``CLOSE``.

``tw_overnight``
    ``actions[T, 3, S]`` contains an *already resolved* exit-at-open fraction
    in ``[0, 1]``, a signed open-entry allocation, and a signed close-entry
    allocation.  Entries form today's cohort.  Yesterday's cohort must be
    completely closed by today's close or the account enters absorbing
    default.

The two price inputs are log returns from previous close to current open and
from current open to current close.  One exchange session settles its T+2
queues exactly once, charges each phase's gross orders independently, and
enqueues one account-level net claim after the close.

All finance state uses FP32 unless the caller deliberately supplies FP64.
Lower-precision model actions are promoted before entering the ledger.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import torch

from stockagent.backtest.tw_commission_rebate import (
    apply_commission_rebate_at_close,
    mask_commission_rebate_state,
    normalize_commission_rebate_state,
    prepare_commission_rebate_calendar_tensors,
    prepare_commission_rebate_state,
)
from stockagent.backtest.tw_continuous import (
    _MIN_WEALTH_FACTOR,
    _advance_equity_scale,
    _capacity_for_new_payable,
    _enqueue_net_claim,
    _finalize_cash_day,
    _prepare_cash_short_state,
    _prepare_equity_scale,
    _resolve_short_maintenance_ratio,
    _settle_open_phase,
)


OPEN = 0
CLOSE = 1

OVERNIGHT_EXIT_AT_OPEN = 0
OVERNIGHT_ENTRY_AT_OPEN = 1
OVERNIGHT_ENTRY_AT_CLOSE = 2

DualSessionMode = Literal["tw_cash", "tw_overnight"]

_POSITION_TOLERANCE = 1.0e-10


@dataclass(slots=True)
class TaiwanDualSessionResult:
    """Audit-oriented result from the eager dual-session ledger."""

    mode: DualSessionMode
    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    event_turnovers: torch.Tensor
    weights_history: torch.Tensor
    open_weights_history: torch.Tensor
    close_weights_history: torch.Tensor
    executed_buy_weights: torch.Tensor
    executed_sell_weights: torch.Tensor
    executed_long_buy_weights: torch.Tensor
    executed_long_sell_weights: torch.Tensor
    executed_short_open_weights: torch.Tensor
    executed_short_cover_weights: torch.Tensor
    cash_history: torch.Tensor
    payables_history: torch.Tensor
    receivables_history: torch.Tensor
    settlement_default: torch.Tensor
    equity_scale_history: torch.Tensor
    short_sale_collateral_history: torch.Tensor
    short_margin_collateral_history: torch.Tensor
    final_weights: torch.Tensor
    final_cash: torch.Tensor
    final_payables: torch.Tensor
    final_receivables: torch.Tensor
    final_alive: torch.Tensor
    final_equity_scale: torch.Tensor
    final_short_sale_collateral: torch.Tensor
    final_short_margin_collateral: torch.Tensor
    commission_rebate_accrued_history: torch.Tensor
    commission_rebate_paid_history: torch.Tensor
    commission_rebate_current_history: torch.Tensor
    commission_rebate_due_history: torch.Tensor
    event_commission_rebate_accrued: torch.Tensor
    final_commission_rebate_current: torch.Tensor
    final_commission_rebate_due: torch.Tensor
    final_commission_rebate_month_id: torch.Tensor
    due_weights_history: torch.Tensor | None = None
    final_due_weights: torch.Tensor | None = None


@dataclass(slots=True)
class _EventResult:
    risky: torch.Tensor
    short_sale_collateral: torch.Tensor
    short_margin_collateral: torch.Tensor
    pending_net_claim: torch.Tensor
    remaining_volume: torch.Tensor | None
    remaining_turnover: torch.Tensor | None
    remaining_short_capacity: torch.Tensor | None
    turnover: torch.Tensor
    long_buy: torch.Tensor
    long_sell: torch.Tensor
    short_open: torch.Tensor
    short_cover: torch.Tensor
    default_now: torch.Tensor


def _finance_dtype(value: torch.Tensor) -> torch.dtype:
    if value.dtype == torch.float64:
        return torch.float64
    return torch.float32


def _validate_actions(
    actions: torch.Tensor,
    *,
    phases: int,
    name: str,
) -> torch.Tensor:
    if not isinstance(actions, torch.Tensor) or not actions.is_floating_point():
        raise ValueError(f"{name} must be a floating tensor")
    if (
        actions.dim() != 3
        or int(actions.size(0)) <= 0
        or int(actions.size(1)) != phases
    ):
        raise ValueError(
            f"{name} must be non-empty with shape [T,{phases},S], "
            f"got {tuple(actions.shape)}"
        )
    if int(actions.size(2)) <= 0:
        raise ValueError(f"{name} must contain at least one symbol")
    result = actions.to(dtype=_finance_dtype(actions))
    if not torch.compiler.is_compiling() and not bool(
        torch.isfinite(result).all().item()
    ):
        raise ValueError(f"{name} must be finite")
    return result


def _as_return_matrix(
    value: torch.Tensor,
    *,
    reference: torch.Tensor,
    name: str,
) -> tuple[torch.Tensor, torch.Tensor]:
    result = torch.as_tensor(
        value,
        device=reference.device,
        dtype=reference.dtype,
    )
    if tuple(result.shape) != tuple(reference.shape):
        raise ValueError(
            f"{name} must have shape [T,S]={tuple(reference.shape)}, "
            f"got {tuple(result.shape)}"
        )
    finite_log = torch.isfinite(result)
    simple_unchecked = torch.expm1(
        torch.where(finite_log, result, torch.zeros_like(result))
    )
    valid = finite_log & torch.isfinite(simple_unchecked)
    simple = torch.where(valid, simple_unchecked, torch.zeros_like(simple_unchecked))
    return simple, valid


def _as_phase_mask(
    value: torch.Tensor | None,
    *,
    reference: torch.Tensor,
    name: str,
    default: bool,
) -> torch.Tensor:
    if value is None:
        return torch.full_like(reference, default, dtype=torch.bool)
    result = torch.as_tensor(value, device=reference.device, dtype=torch.bool)
    if tuple(result.shape) != tuple(reference.shape):
        raise ValueError(
            f"{name} must have shape [T,2,S]={tuple(reference.shape)}, "
            f"got {tuple(result.shape)}"
        )
    return result


def _as_phase_rate(
    value: torch.Tensor | float,
    *,
    reference: torch.Tensor,
    name: str,
) -> torch.Tensor:
    """Return a broadcast view with shape ``[T,2,S]``."""

    result = torch.as_tensor(
        value,
        device=reference.device,
        dtype=reference.dtype,
    )
    t_len, phases, symbols = reference.shape
    if result.dim() == 0:
        result = result.expand_as(reference)
    elif tuple(result.shape) == (symbols,):
        result = result.view(1, 1, symbols).expand_as(reference)
    elif tuple(result.shape) == (phases, symbols):
        result = result.unsqueeze(0).expand_as(reference)
    elif tuple(result.shape) != tuple(reference.shape):
        raise ValueError(
            f"{name} must be scalar, [S], [2,S], or [T,2,S]; got {tuple(result.shape)}"
        )
    if not torch.compiler.is_compiling() and (
        not bool(torch.isfinite(result).all().item())
        or bool((result < 0.0).any().item())
    ):
        raise ValueError(f"{name} must be finite and non-negative")
    return result


def _as_daily_nonnegative(
    value: torch.Tensor | float | None,
    *,
    reference: torch.Tensor,
    name: str,
    allow_positive_inf: bool = False,
) -> torch.Tensor | None:
    if value is None:
        return None
    result = torch.as_tensor(
        value,
        device=reference.device,
        dtype=reference.dtype,
    )
    rows, symbols = reference.shape
    if result.dim() == 0:
        result = result.expand_as(reference)
    elif tuple(result.shape) == (symbols,):
        result = result.view(1, symbols).expand_as(reference)
    elif tuple(result.shape) != tuple(reference.shape):
        raise ValueError(
            f"{name} must be scalar, [S], or [T,S]; got {tuple(result.shape)}"
        )
    valid = torch.isfinite(result)
    if allow_positive_inf:
        valid = valid | torch.isposinf(result)
    if not torch.compiler.is_compiling() and (
        not bool(valid.all().item())
        or bool((result < 0.0).any().item())
    ):
        qualifier = "finite or positive-infinity" if allow_positive_inf else "finite"
        raise ValueError(f"{name} must be {qualifier} and non-negative")
    return result


def _cash_margin_nav(
    *,
    cash: torch.Tensor,
    risky: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    short_sale_collateral: torch.Tensor,
    short_margin_collateral: torch.Tensor,
    pending_net_claim: torch.Tensor,
) -> torch.Tensor:
    return (
        cash
        + risky.sum()
        + short_sale_collateral.sum()
        + short_margin_collateral.sum()
        + receivables.sum()
        - payables.sum()
        + pending_net_claim
    )


def _kill_finance_state(
    *,
    default_now: torch.Tensor,
    alive: torch.Tensor,
    risky: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    short_sale_collateral: torch.Tensor,
    short_margin_collateral: torch.Tensor,
    pending_net_claim: torch.Tensor,
) -> tuple[
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
    torch.Tensor,
]:
    survived = alive & ~default_now
    risky = torch.where(survived, risky, torch.zeros_like(risky))
    cash = torch.where(survived, cash, torch.zeros_like(cash))
    payables = torch.where(survived, payables, torch.zeros_like(payables))
    receivables = torch.where(survived, receivables, torch.zeros_like(receivables))
    short_sale_collateral = torch.where(
        survived,
        short_sale_collateral,
        torch.zeros_like(short_sale_collateral),
    )
    short_margin_collateral = torch.where(
        survived,
        short_margin_collateral,
        torch.zeros_like(short_margin_collateral),
    )
    pending_net_claim = torch.where(
        survived,
        pending_net_claim,
        torch.zeros_like(pending_net_claim),
    )
    return (
        survived,
        risky,
        cash,
        payables,
        receivables,
        short_sale_collateral,
        short_margin_collateral,
        pending_net_claim,
    )


def _cap_vector(
    requested: torch.Tensor,
    remaining: torch.Tensor | None,
) -> tuple[torch.Tensor, torch.Tensor | None]:
    if remaining is None:
        return requested, None
    total = requested
    unbounded = torch.isposinf(remaining)
    effective_remaining = torch.where(unbounded, total, remaining)
    finite_scale = torch.minimum(
        torch.ones_like(total),
        effective_remaining / total.clamp_min(1.0e-12),
    )
    scale = torch.where(
        unbounded,
        torch.ones_like(finite_scale),
        finite_scale,
    )
    executed = requested * scale
    finite_start = torch.where(unbounded, executed, remaining)
    finite_next = (finite_start - executed).clamp_min(0.0)
    next_remaining = torch.where(
        unbounded,
        remaining.detach(),
        finite_next,
    )
    return executed, next_remaining


def _cap_group_by_vector(
    values: tuple[torch.Tensor, ...],
    remaining: torch.Tensor | None,
) -> tuple[tuple[torch.Tensor, ...], torch.Tensor | None]:
    if remaining is None:
        return values, None
    total = torch.zeros_like(values[0])
    for value in values:
        total = total + value
    unbounded = torch.isposinf(remaining)
    effective_remaining = torch.where(unbounded, total, remaining)
    finite_scale = torch.minimum(
        torch.ones_like(total),
        effective_remaining / total.clamp_min(1.0e-12),
    )
    scale = torch.where(
        unbounded,
        torch.ones_like(finite_scale),
        finite_scale,
    )
    capped = tuple(value * scale for value in values)
    used = torch.zeros_like(total)
    for value in capped:
        used = used + value
    finite_start = torch.where(unbounded, used, remaining)
    finite_next = (finite_start - used).clamp_min(0.0)
    next_remaining = torch.where(
        unbounded,
        remaining.detach(),
        finite_next,
    )
    return capped, next_remaining


def _cap_group_by_scalar(
    values: tuple[torch.Tensor, ...],
    remaining: torch.Tensor | None,
) -> tuple[tuple[torch.Tensor, ...], torch.Tensor | None]:
    if remaining is None:
        return values, None
    total = torch.zeros_like(remaining)
    for value in values:
        total = total + value.sum()
    scale = torch.minimum(
        torch.ones_like(total),
        remaining / total.clamp_min(1.0e-12),
    )
    capped = tuple(value * scale for value in values)
    used = torch.zeros_like(total)
    for value in capped:
        used = used + value.sum()
    return capped, (remaining - used).clamp_min(0.0)


def _unit_interval_linear_root(
    intercept: torch.Tensor,
    slope: torch.Tensor,
) -> torch.Tensor:
    """Return the clipped root of ``intercept + slope * x`` without NaNs."""

    nonzero = slope != 0.0
    safe_slope = torch.where(nonzero, slope, torch.ones_like(slope))
    root = -intercept / safe_slope
    return torch.where(
        nonzero,
        root.clamp(0.0, 1.0),
        torch.zeros_like(root),
    )


def _max_feasible_cover_scale(
    *,
    remaining_intercept: torch.Tensor,
    remaining_slope: torch.Tensor,
    proportional_intercept: torch.Tensor,
    proportional_slope: torch.Tensor,
    cost_intercept: torch.Tensor,
    cost_slope: torch.Tensor,
    collateral_total: torch.Tensor,
    maintenance_ratio: torch.Tensor,
    fixed_funding_margin: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Return the globally largest affordable scale from affine ray terms.

    ``cover(s) = anchor_cover + s * tail_cover`` for ``0 <= s <= 1``.
    Maintenance can make affordability non-monotone: a small cover may release
    no collateral, a larger one can unlock a pooled maintenance surplus, and a
    still larger underwater cover can consume cash again.  A prefix bisection
    is therefore invalid.

    Along this ray, remaining liability, proportional release, cover cost, and
    each maintenance-tolerance branch are affine.  The funding margin can only
    change formula where maintenance crosses one, releasable collateral crosses
    zero/proportional release, or every short is covered.  Its global feasible
    maximum is consequently either one of those critical points or a zero of
    one of the five affine margin formulas.  Evaluate that fixed candidate set
    with the exact tensor ledger formula and select the largest feasible scale.
    This scalar helper is shared by the producer-first and proportional rays;
    each ray derives its own FP32 coefficients before entering it.
    """

    required_intercept = maintenance_ratio * remaining_intercept
    required_slope = maintenance_ratio * remaining_slope
    margin_intercept = fixed_funding_margin - cost_intercept

    finance_epsilon = fixed_funding_margin.new_tensor(
        torch.finfo(fixed_funding_margin.dtype).eps * 16.0
    )
    high_headroom_intercept = (
        collateral_total
        - (1.0 + finance_epsilon) * required_intercept
    )
    high_headroom_slope = (
        (1.0 + finance_epsilon) * required_slope
    )
    low_headroom_intercept = (
        collateral_total - required_intercept - finance_epsilon
    )
    low_headroom_slope = required_slope

    def margin_at(scale: torch.Tensor) -> torch.Tensor:
        remaining = (
            remaining_intercept - remaining_slope * scale
        ).clamp_min(0.0)
        required = maintenance_ratio * remaining
        release_tolerance = finance_epsilon * torch.maximum(
            torch.ones_like(required),
            required.abs(),
        )
        maximum_release = (
            collateral_total - required - release_tolerance
        ).clamp_min(0.0)
        proportional_release = (
            proportional_intercept + proportional_slope * scale
        )
        released = torch.minimum(proportional_release, maximum_release)
        released = torch.where(
            remaining <= _POSITION_TOLERANCE,
            collateral_total.expand_as(released),
            released,
        )
        return margin_intercept + released - cost_slope * scale

    critical_points = torch.stack(
        (
            torch.zeros_like(margin_intercept),
            torch.ones_like(margin_intercept),
            _unit_interval_linear_root(
                required_intercept - 1.0,
                -required_slope,
            ),
            _unit_interval_linear_root(
                high_headroom_intercept,
                high_headroom_slope,
            ),
            _unit_interval_linear_root(
                low_headroom_intercept,
                low_headroom_slope,
            ),
            _unit_interval_linear_root(
                high_headroom_intercept - proportional_intercept,
                high_headroom_slope - proportional_slope,
            ),
            _unit_interval_linear_root(
                low_headroom_intercept - proportional_intercept,
                low_headroom_slope - proportional_slope,
            ),
            _unit_interval_linear_root(
                remaining_intercept
                - fixed_funding_margin.new_tensor(_POSITION_TOLERANCE),
                -remaining_slope,
            ),
        )
    )
    affine_roots = torch.stack(
        (
            _unit_interval_linear_root(
                margin_intercept,
                -cost_slope,
            ),
            _unit_interval_linear_root(
                margin_intercept + high_headroom_intercept,
                high_headroom_slope - cost_slope,
            ),
            _unit_interval_linear_root(
                margin_intercept + low_headroom_intercept,
                low_headroom_slope - cost_slope,
            ),
            _unit_interval_linear_root(
                margin_intercept + proportional_intercept,
                proportional_slope - cost_slope,
            ),
            _unit_interval_linear_root(
                margin_intercept + collateral_total,
                -cost_slope,
            ),
        )
    )
    conservative_roots = (
        affine_roots
        - finance_epsilon
        * torch.maximum(torch.ones_like(affine_roots), affine_roots.abs())
    ).clamp(0.0, 1.0)
    candidates = torch.cat(
        (critical_points, affine_roots, conservative_roots)
    )
    candidate_margins = margin_at(candidates)
    feasible_candidates = torch.where(
        candidate_margins >= 0.0,
        candidates,
        -torch.ones_like(candidates),
    )
    best_scale = feasible_candidates.amax()
    has_feasible = best_scale >= 0.0
    safe_scale = torch.where(
        has_feasible,
        best_scale,
        torch.zeros_like(best_scale),
    )
    return safe_scale, has_feasible, margin_at(safe_scale)


def _max_feasible_cover_on_ray(
    *,
    short_after_mandatory: torch.Tensor,
    anchor_cover: torch.Tensor,
    scalable_cover: torch.Tensor,
    collateral_per_short: torch.Tensor,
    cover_cost_rates: torch.Tensor,
    mandatory_release: torch.Tensor,
    collateral_total: torch.Tensor,
    maintenance_ratio: torch.Tensor,
    fixed_funding_margin: torch.Tensor,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Reduce one cover ray to affine coefficients and solve it globally."""

    remaining_after_anchor = (
        short_after_mandatory - anchor_cover
    ).clamp_min(0.0)
    return _max_feasible_cover_scale(
        remaining_intercept=remaining_after_anchor.sum(),
        remaining_slope=scalable_cover.sum(),
        proportional_intercept=(
            mandatory_release
            + (anchor_cover * collateral_per_short).sum()
        ),
        proportional_slope=(
            scalable_cover * collateral_per_short
        ).sum(),
        cost_intercept=(anchor_cover * cover_cost_rates).sum(),
        cost_slope=(scalable_cover * cover_cost_rates).sum(),
        collateral_total=collateral_total,
        maintenance_ratio=maintenance_ratio,
        fixed_funding_margin=fixed_funding_margin,
    )


def _execute_signed_event(
    *,
    risky_before: torch.Tensor,
    desired_risky: torch.Tensor,
    mandatory_long_sell: torch.Tensor,
    mandatory_short_cover: torch.Tensor,
    can_buy: torch.Tensor,
    can_sell: torch.Tensor,
    can_short_open: torch.Tensor,
    buy_fees: torch.Tensor,
    sell_fees: torch.Tensor,
    margin_rates: torch.Tensor,
    short_handling_rates: torch.Tensor,
    short_sale_collateral: torch.Tensor,
    short_margin_collateral: torch.Tensor,
    cash: torch.Tensor,
    payables: torch.Tensor,
    receivables: torch.Tensor,
    pending_net_claim: torch.Tensor,
    remaining_volume: torch.Tensor | None,
    remaining_turnover: torch.Tensor | None,
    remaining_short_capacity: torch.Tensor | None,
    event_nav: torch.Tensor,
    gross_budget: float,
    maintenance_ratio: torch.Tensor,
    alive: torch.Tensor,
    advance: torch.Tensor,
) -> _EventResult:
    """Execute one signed target transition without aging or enqueueing T+2."""

    starting_volume = remaining_volume
    starting_turnover = remaining_turnover
    starting_short_capacity = remaining_short_capacity
    active = alive & advance
    current_long = risky_before.clamp_min(0.0)
    current_short = (-risky_before).clamp_min(0.0)
    mandatory_long_sell = torch.minimum(
        mandatory_long_sell.clamp_min(0.0),
        current_long,
    )
    mandatory_short_cover = torch.minimum(
        mandatory_short_cover.clamp_min(0.0),
        current_short,
    )
    blocked_mandatory = (
        ((mandatory_long_sell > _POSITION_TOLERANCE) & ~can_sell)
        | ((mandatory_short_cover > _POSITION_TOLERANCE) & ~can_buy)
    ).any()
    event_default = active & blocked_mandatory
    event_alive = alive & ~event_default
    active = event_alive & advance

    mandatory_long_sell = torch.where(
        active,
        mandatory_long_sell,
        torch.zeros_like(mandatory_long_sell),
    )
    mandatory_short_cover = torch.where(
        active,
        mandatory_short_cover,
        torch.zeros_like(mandatory_short_cover),
    )
    base = risky_before - mandatory_long_sell + mandatory_short_cover
    # Apply phase permissions to the requested *signed transition* before
    # decomposing it into physical legs.  In particular, a blocked reduction
    # may not be bypassed by opening the opposite side: long -> short requires
    # the long sale first, and short -> long requires the short cover first.
    desired_risky = torch.where(
        (desired_risky > base) & ~can_buy,
        base,
        desired_risky,
    )
    desired_risky = torch.where(
        (desired_risky < base) & ~can_sell,
        base,
        desired_risky,
    )
    short_floor = torch.minimum(base, torch.zeros_like(base))
    desired_risky = torch.where(
        (desired_risky < base) & ~can_short_open,
        torch.maximum(desired_risky, short_floor),
        desired_risky,
    )
    requested_delta = desired_risky - base
    requested_magnitude = requested_delta.abs()
    capped_magnitude, _ = _cap_vector(
        requested_magnitude,
        remaining_volume,
    )
    (capped_magnitude,), _ = _cap_group_by_scalar(
        (capped_magnitude,),
        remaining_turnover,
    )
    liquidity_target = base + requested_delta.sign() * capped_magnitude

    base_long = base.clamp_min(0.0)
    base_short = (-base).clamp_min(0.0)
    desired_long = liquidity_target.clamp_min(0.0)
    desired_short = (-liquidity_target).clamp_min(0.0)

    voluntary_long_sell = (base_long - desired_long).clamp_min(0.0)
    voluntary_long_buy = (desired_long - base_long).clamp_min(0.0)
    voluntary_short_cover = (base_short - desired_short).clamp_min(0.0)
    voluntary_short_open = (desired_short - base_short).clamp_min(0.0)

    free_cash_at_new_due = _capacity_for_new_payable(
        cash,
        payables,
        receivables,
    )
    collateral_total = (
        short_sale_collateral + short_margin_collateral
    ).sum()
    collateral_per_short = torch.where(
        current_short > _POSITION_TOLERANCE,
        (short_sale_collateral + short_margin_collateral)
        / current_short.clamp_min(1.0e-12),
        torch.zeros_like(current_short),
    )
    mandatory_release = (
        mandatory_short_cover * collateral_per_short
    ).sum()
    short_after_mandatory = (
        current_short - mandatory_short_cover
    ).clamp_min(0.0)
    mandatory_cover_cost = (
        mandatory_short_cover * (1.0 + buy_fees)
    ).sum()
    long_sale_claim = (
        (mandatory_long_sell + voluntary_long_sell) * (1.0 - sell_fees)
    ).sum()

    requested_short_cover = voluntary_short_cover
    # A profitable cover can release more collateral than its buy cost and is
    # therefore a funding source for a different, underwater cover.  Executing
    # every cover under one common scale incorrectly discards that source when
    # the requested basket is net cash-consuming.  Mirror the integer oracle:
    # keep raw cash-producing covers first, then scale only the consuming tail.
    raw_cover_net = requested_short_cover * (
        collateral_per_short - (1.0 + buy_fees)
    )
    producer_cover = torch.where(
        raw_cover_net >= -1.0e-12,
        requested_short_cover,
        torch.zeros_like(requested_short_cover),
    )
    consuming_cover = (
        requested_short_cover - producer_cover
    ).clamp_min(0.0)
    cover_cost_rates = 1.0 + buy_fees
    fixed_funding_margin = (
        free_cash_at_new_due
        + pending_net_claim
        + long_sale_claim
        - mandatory_cover_cost
    )
    producer_first_scale, producer_first_feasible, _ = (
        _max_feasible_cover_on_ray(
            short_after_mandatory=short_after_mandatory,
            anchor_cover=producer_cover,
            scalable_cover=consuming_cover,
            collateral_per_short=collateral_per_short,
            cover_cost_rates=cover_cost_rates,
            mandatory_release=mandatory_release,
            collateral_total=collateral_total,
            maintenance_ratio=maintenance_ratio,
            fixed_funding_margin=fixed_funding_margin,
        )
    )
    # A raw producer can itself be maintenance-locked.  Preserve the historical
    # proportional fallback only when no point on the producer-first ray is
    # affordable; otherwise a consumer may legitimately unlock the producer's
    # trapped collateral and fund a larger cover basket.
    proportional_scale, _, _ = _max_feasible_cover_on_ray(
        short_after_mandatory=short_after_mandatory,
        anchor_cover=torch.zeros_like(requested_short_cover),
        scalable_cover=requested_short_cover,
        collateral_per_short=collateral_per_short,
        cover_cost_rates=cover_cost_rates,
        mandatory_release=mandatory_release,
        collateral_total=collateral_total,
        maintenance_ratio=maintenance_ratio,
        fixed_funding_margin=fixed_funding_margin,
    )
    producer_first_cover = (
        producer_cover + producer_first_scale * consuming_cover
    )
    proportional_cover = proportional_scale * requested_short_cover
    voluntary_short_cover = torch.where(
        producer_first_feasible,
        producer_first_cover,
        proportional_cover,
    )

    # Recompute the selected reduction through the canonical vector ledger.
    # The scalar affine margin is sufficient to choose the global endpoint,
    # but using it to infer the T+2 claim changes FP32 reduction association
    # under Inductor.  This one post-selection O(S) pass makes funding capacity
    # use the exact same collateral-release and cover-cost formula as the
    # executed legs.
    remaining_short_after_reduction = (
        short_after_mandatory - voluntary_short_cover
    ).clamp_min(0.0)
    remaining_short_after_reduction_total = (
        remaining_short_after_reduction.sum()
    )
    required_after_reduction = (
        maintenance_ratio * remaining_short_after_reduction_total
    )
    reduction_release_tolerance = risky_before.new_tensor(
        torch.finfo(risky_before.dtype).eps * 16.0
    ) * torch.maximum(
        torch.ones_like(required_after_reduction),
        required_after_reduction.abs(),
    )
    maximum_reduction_release = (
        collateral_total
        - required_after_reduction
        - reduction_release_tolerance
    ).clamp_min(0.0)
    proportional_reduction_release = mandatory_release + (
        voluntary_short_cover * collateral_per_short
    ).sum()
    released_for_reduction = torch.minimum(
        proportional_reduction_release,
        maximum_reduction_release,
    )
    released_for_reduction = torch.where(
        remaining_short_after_reduction_total <= _POSITION_TOLERANCE,
        collateral_total,
        released_for_reduction,
    )
    funded_reduction_claim = (
        pending_net_claim
        + long_sale_claim
        + released_for_reduction
        - mandatory_cover_cost
        - (voluntary_short_cover * cover_cost_rates).sum()
    )

    # If funding cannot finish a short cover, the same symbol cannot cross
    # through zero into a synthetic long.  Liquidity caps have already been
    # allocated to the signed transition, so unused capacity is deliberately
    # not reassigned to a different requested endpoint.
    remaining_short_after_cover = (
        base_short - voluntary_short_cover
    ).clamp_min(0.0)
    voluntary_long_buy = torch.where(
        remaining_short_after_cover <= _POSITION_TOLERANCE,
        voluntary_long_buy,
        torch.zeros_like(voluntary_long_buy),
    )

    exposure_after_reductions = (
        (base_long - voluntary_long_sell).clamp_min(0.0)
        + (base_short - voluntary_short_cover).clamp_min(0.0)
    ).sum()
    requested_increases = (voluntary_long_buy + voluntary_short_open).sum()
    gross_limit = event_nav.clamp_min(0.0) * float(max(0.0, gross_budget))
    gross_scale = torch.minimum(
        torch.ones_like(requested_increases),
        (gross_limit - exposure_after_reductions).clamp_min(0.0)
        / requested_increases.clamp_min(1.0e-12),
    )
    voluntary_long_buy = voluntary_long_buy * gross_scale
    voluntary_short_open = voluntary_short_open * gross_scale

    voluntary_short_open, _ = _cap_vector(
        voluntary_short_open,
        remaining_short_capacity,
    )

    opening_cost = (
        (voluntary_long_buy * (1.0 + buy_fees)).sum()
        + (voluntary_short_open * margin_rates).sum()
    )
    opening_capacity = (
        free_cash_at_new_due + funded_reduction_claim
    ).clamp_min(0.0)
    opening_funding_scale = torch.minimum(
        torch.ones_like(opening_cost),
        opening_capacity / opening_cost.clamp_min(1.0e-12),
    )
    voluntary_long_buy = voluntary_long_buy * opening_funding_scale
    voluntary_short_open = voluntary_short_open * opening_funding_scale

    long_sell = mandatory_long_sell + voluntary_long_sell
    long_buy = voluntary_long_buy
    short_cover = mandatory_short_cover + voluntary_short_cover
    short_open = voluntary_short_open
    executed = risky_before - long_sell + long_buy + short_cover - short_open

    voluntary_volume_used = (
        voluntary_long_sell
        + voluntary_long_buy
        + voluntary_short_cover
        + voluntary_short_open
    )
    next_volume = (
        None
        if starting_volume is None
        else (starting_volume - voluntary_volume_used).clamp_min(0.0)
    )
    next_turnover = (
        None
        if starting_turnover is None
        else (starting_turnover - voluntary_volume_used.sum()).clamp_min(0.0)
    )
    next_short_capacity = (
        None
        if starting_short_capacity is None
        else (starting_short_capacity - voluntary_short_open).clamp_min(0.0)
    )

    cover_fraction = torch.where(
        current_short > _POSITION_TOLERANCE,
        (short_cover / current_short.clamp_min(1.0e-12)).clamp(0.0, 1.0),
        torch.zeros_like(current_short),
    )
    candidate_released_sale = short_sale_collateral * cover_fraction
    candidate_released_margin = short_margin_collateral * cover_fraction
    opened_sale_collateral = short_open * (1.0 - sell_fees - short_handling_rates)
    opened_margin_collateral = short_open * margin_rates
    remaining_short = (-executed).clamp_min(0.0)
    remaining_short_total = remaining_short.sum()
    candidate_release_total = (
        candidate_released_sale.sum() + candidate_released_margin.sum()
    )
    collateral_before_release = (
        short_sale_collateral.sum()
        + short_margin_collateral.sum()
        + opened_sale_collateral.sum()
        + opened_margin_collateral.sum()
    )
    required_remaining_collateral = maintenance_ratio * remaining_short_total
    maintenance_tolerance = risky_before.new_tensor(
        torch.finfo(risky_before.dtype).eps * 16.0
    ) * torch.maximum(
        torch.ones_like(required_remaining_collateral),
        required_remaining_collateral.abs(),
    )
    max_maintenance_release = (
        collateral_before_release
        - required_remaining_collateral
        - maintenance_tolerance
    ).clamp_min(0.0)
    release_scale = torch.minimum(
        torch.ones_like(candidate_release_total),
        max_maintenance_release / candidate_release_total.clamp_min(1.0e-12),
    )
    all_shorts_covered = remaining_short_total <= _POSITION_TOLERANCE
    release_scale = torch.where(
        all_shorts_covered,
        torch.ones_like(release_scale),
        release_scale,
    )
    released_sale = torch.where(
        all_shorts_covered,
        short_sale_collateral,
        candidate_released_sale * release_scale,
    )
    released_margin = torch.where(
        all_shorts_covered,
        short_margin_collateral,
        candidate_released_margin * release_scale,
    )
    raw_next_short_sale = short_sale_collateral - released_sale + opened_sale_collateral
    raw_next_short_margin = (
        short_margin_collateral - released_margin + opened_margin_collateral
    )
    remaining_allocation = remaining_short / remaining_short_total.clamp_min(1.0e-12)
    allocated_sale = remaining_allocation * raw_next_short_sale.sum()
    allocated_margin = remaining_allocation * raw_next_short_margin.sum()
    orphaned_collateral = (
        (remaining_short <= _POSITION_TOLERANCE)
        & ((raw_next_short_sale + raw_next_short_margin) > _POSITION_TOLERANCE)
    ).any()
    next_short_sale = torch.where(
        orphaned_collateral,
        allocated_sale,
        raw_next_short_sale,
    )
    next_short_margin = torch.where(
        orphaned_collateral,
        allocated_margin,
        raw_next_short_margin,
    )

    event_net_claim = (
        (long_sell * (1.0 - sell_fees)).sum()
        - (long_buy * (1.0 + buy_fees)).sum()
        + released_sale.sum()
        + released_margin.sum()
        - (short_cover * (1.0 + buy_fees)).sum()
        - opened_margin_collateral.sum()
    )
    next_pending = pending_net_claim + event_net_claim
    event_turnover = (long_sell + long_buy + short_cover + short_open).sum()

    active_float = active.to(dtype=risky_before.dtype)
    executed = torch.where(active, executed, risky_before)
    next_short_sale = torch.where(
        active,
        next_short_sale,
        short_sale_collateral,
    )
    next_short_margin = torch.where(
        active,
        next_short_margin,
        short_margin_collateral,
    )
    next_pending = torch.where(active, next_pending, pending_net_claim)
    event_turnover = event_turnover * active_float
    long_buy = long_buy * active_float
    long_sell = long_sell * active_float
    short_open = short_open * active_float
    short_cover = short_cover * active_float
    if next_volume is not None:
        next_volume = torch.where(active, next_volume, remaining_volume)
    if next_turnover is not None:
        next_turnover = torch.where(active, next_turnover, remaining_turnover)
    if next_short_capacity is not None:
        next_short_capacity = torch.where(
            active,
            next_short_capacity,
            remaining_short_capacity,
        )

    executed = torch.where(event_alive, executed, torch.zeros_like(executed))
    next_short_sale = torch.where(
        event_alive,
        next_short_sale,
        torch.zeros_like(next_short_sale),
    )
    next_short_margin = torch.where(
        event_alive,
        next_short_margin,
        torch.zeros_like(next_short_margin),
    )
    next_pending = torch.where(
        event_alive,
        next_pending,
        torch.zeros_like(next_pending),
    )
    return _EventResult(
        risky=executed,
        short_sale_collateral=next_short_sale,
        short_margin_collateral=next_short_margin,
        pending_net_claim=next_pending,
        remaining_volume=next_volume,
        remaining_turnover=next_turnover,
        remaining_short_capacity=next_short_capacity,
        turnover=event_turnover,
        long_buy=long_buy,
        long_sell=long_sell,
        short_open=short_open,
        short_cover=short_cover,
        default_now=event_default,
    )


def _event_weight(
    risky: torch.Tensor, nav: torch.Tensor, alive: torch.Tensor
) -> torch.Tensor:
    valid = alive & torch.isfinite(nav) & (nav > _MIN_WEALTH_FACTOR)
    safe_nav = torch.where(
        valid,
        nav,
        torch.ones_like(nav),
    )
    return torch.where(valid, risky / safe_nav, torch.zeros_like(risky))


def _convert_reference_cap_to_ledger(
    cap: torch.Tensor | None,
    *,
    equity_scale: torch.Tensor,
    alive: torch.Tensor,
) -> torch.Tensor | None:
    if cap is None:
        return None
    finite = torch.isfinite(cap) & (cap >= 0.0)
    unbounded = torch.isposinf(cap)
    safe_scale = torch.where(
        alive & (equity_scale > 0.0),
        equity_scale,
        torch.ones_like(equity_scale),
    )
    # Positive infinity is the explicit disabled-capacity sentinel. It is
    # invariant to account scale, so it must not enter ``inf / equity_scale``:
    # that forward value remains inf but its backward derivative is -inf and
    # later zero use produces ``0 * inf = NaN`` through the recurrent state.
    # Mask before the division so the unbounded branch has exactly zero
    # derivative while finite capacity retains the intended scale conversion.
    finite_cap = torch.where(finite, cap, torch.zeros_like(cap))
    converted = finite_cap / safe_scale
    return torch.where(
        alive & unbounded,
        cap.detach(),
        torch.where(
            alive & finite,
            converted,
            torch.zeros_like(converted),
        ),
    )


def _reference_cap_to_event_notional(
    reference_cap: torch.Tensor | None,
    *,
    event_price_ratio: torch.Tensor,
) -> torch.Tensor | None:
    """Value a shared share-equivalent cap at one auction's price.

    ``reference_cap`` is stored as prior-close notional in normalized ledger
    units.  Multiplying by the causal event/prior-close price ratio produces
    the current auction notional for exactly the same number of shares.
    """

    if reference_cap is None:
        return None
    valid_ratio = torch.isfinite(event_price_ratio) & (event_price_ratio > 0.0)
    finite = torch.isfinite(reference_cap) & (reference_cap >= 0.0)
    unbounded = torch.isposinf(reference_cap)
    finite_cap = torch.where(
        finite,
        reference_cap,
        torch.zeros_like(reference_cap),
    )
    safe_ratio = torch.where(
        valid_ratio,
        event_price_ratio,
        torch.ones_like(event_price_ratio),
    )
    converted = finite_cap * safe_ratio
    return torch.where(
        valid_ratio & unbounded,
        reference_cap.detach(),
        torch.where(
            valid_ratio & finite,
            converted,
            torch.zeros_like(converted),
        ),
    )


def _event_notional_cap_to_reference(
    event_cap: torch.Tensor | None,
    *,
    event_price_ratio: torch.Tensor,
) -> torch.Tensor | None:
    """Convert an auction's unspent notional back to shared share units."""

    if event_cap is None:
        return None
    valid_ratio = torch.isfinite(event_price_ratio) & (event_price_ratio > 0.0)
    finite = torch.isfinite(event_cap) & (event_cap >= 0.0)
    unbounded = torch.isposinf(event_cap)
    safe_ratio = torch.where(
        valid_ratio,
        event_price_ratio,
        torch.ones_like(event_price_ratio),
    )
    finite_cap = torch.where(finite, event_cap, torch.zeros_like(event_cap))
    converted = finite_cap / safe_ratio
    return torch.where(
        valid_ratio & unbounded,
        event_cap.detach(),
        torch.where(
            valid_ratio & finite,
            converted,
            torch.zeros_like(converted),
        ),
    )


def _run_dual_session(
    *,
    mode: DualSessionMode,
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    commission_rebate_rates: torch.Tensor | float,
    commission_rebate_timing: str,
    session_month_ids: torch.Tensor | None,
    commission_rebate_payment_eligible_mask: torch.Tensor | None,
    can_short_open_mask: torch.Tensor | None,
    force_short_cover_mask: torch.Tensor | None,
    short_margin_rate: torch.Tensor | float | None,
    short_capacity_weights: torch.Tensor | float | None,
    short_handling_fee_rate: torch.Tensor | float,
    short_maintenance_ratio: float,
    unresolved_corporate_action_mask: torch.Tensor | None,
    cash_dividend_yield: torch.Tensor | None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None,
    claim_queue_sessions: int | None,
    force_exit_mask: torch.Tensor | None,
    settlement_lag_sessions: int,
    gross_budget: float,
    max_turnover_ratio: float,
    volume_limit_weights: torch.Tensor | float | None,
    state_advance_mask: torch.Tensor | None,
    return_weights_history: bool,
    initial_weights: torch.Tensor | None,
    initial_cash: torch.Tensor | None,
    initial_payables: torch.Tensor | None,
    initial_receivables: torch.Tensor | None,
    initial_commission_rebate_current: torch.Tensor | None,
    initial_commission_rebate_due: torch.Tensor | None,
    initial_commission_rebate_month_id: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    initial_equity_scale: torch.Tensor | None,
    initial_short_sale_collateral: torch.Tensor | None,
    initial_short_margin_collateral: torch.Tensor | None,
    detach_initial_state: bool,
) -> TaiwanDualSessionResult:
    phases = 2 if mode == "tw_cash" else 3
    action_values = _validate_actions(actions, phases=phases, name="actions")
    t_len, _, symbols = action_values.shape
    phase_reference = action_values.new_zeros((t_len, 2, symbols))
    daily_reference = action_values[:, 0]

    if mode == "tw_overnight":
        exit_fraction = action_values[:, OVERNIGHT_EXIT_AT_OPEN]
        if not torch.compiler.is_compiling() and bool(
            ((exit_fraction < 0.0) | (exit_fraction > 1.0)).any().item()
        ):
            raise ValueError(
                "tw_overnight actions[:,0,:] is an already-resolved "
                "exit-at-open fraction and must be within [0,1]"
            )
        entry_open = action_values[:, OVERNIGHT_ENTRY_AT_OPEN]
        entry_close = action_values[:, OVERNIGHT_ENTRY_AT_CLOSE]
        opposite_same_day_entry = (
            (entry_open.abs() > _POSITION_TOLERANCE)
            & (entry_close.abs() > _POSITION_TOLERANCE)
            & ((entry_open > 0.0) != (entry_close > 0.0))
        )
        if not torch.compiler.is_compiling() and bool(
            opposite_same_day_entry.any().item()
        ):
            raise ValueError(
                "tw_overnight open-entry and close-entry allocations may not "
                "reverse the same symbol within one session"
            )

    overnight_returns, overnight_return_valid = _as_return_matrix(
        overnight_log_returns,
        reference=daily_reference,
        name="overnight_log_returns",
    )
    intraday_returns, intraday_return_valid = _as_return_matrix(
        intraday_log_returns,
        reference=daily_reference,
        name="intraday_log_returns",
    )
    selection = _as_phase_mask(
        tradable_mask,
        reference=phase_reference,
        name="tradable_mask",
        default=False,
    )
    can_buy = _as_phase_mask(
        can_buy_mask,
        reference=phase_reference,
        name="can_buy_mask",
        default=False,
    )
    can_sell = _as_phase_mask(
        can_sell_mask,
        reference=phase_reference,
        name="can_sell_mask",
        default=False,
    )
    short_open = (
        _as_phase_mask(
            can_short_open_mask,
            reference=phase_reference,
            name="can_short_open_mask",
            default=False,
        )
        & can_sell
    )
    forced_cover = _as_phase_mask(
        force_short_cover_mask,
        reference=phase_reference,
        name="force_short_cover_mask",
        default=False,
    )
    unresolved_action = _as_phase_mask(
        unresolved_corporate_action_mask,
        reference=phase_reference,
        name="unresolved_corporate_action_mask",
        default=False,
    )
    terminal = _as_phase_mask(
        force_exit_mask,
        reference=phase_reference,
        name="force_exit_mask",
        default=False,
    )
    buy_fees = _as_phase_rate(
        buy_fee_rates,
        reference=phase_reference,
        name="buy_fee_rates",
    )
    sell_fees = _as_phase_rate(
        sell_fee_rates,
        reference=phase_reference,
        name="sell_fee_rates",
    )
    rebate_rates = _as_phase_rate(
        commission_rebate_rates,
        reference=phase_reference,
        name="commission_rebate_rates",
    )
    if not torch.compiler.is_compiling() and bool((sell_fees >= 1.0).any().item()):
        raise ValueError("sell_fee_rates must be less than 1")
    if not torch.compiler.is_compiling() and bool(
        ((rebate_rates > buy_fees) | (rebate_rates > sell_fees)).any().item()
    ):
        raise ValueError(
            "commission_rebate_rates cannot exceed gross buy or sell fee rates"
        )
    (
        rebate_timing,
        rebate_month_ids,
        rebate_payment_eligible,
    ) = prepare_commission_rebate_calendar_tensors(
        reference=daily_reference,
        timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        payment_eligible_mask=commission_rebate_payment_eligible_mask,
        state_advance_mask=state_advance_mask,
    )
    if rebate_timing == "monthly_15th" and not torch.compiler.is_compiling():
        rebate_active_rows = (
            torch.ones(
                (t_len,),
                device=action_values.device,
                dtype=torch.bool,
            )
            if state_advance_mask is None
            else torch.as_tensor(
                state_advance_mask,
                device=action_values.device,
                dtype=torch.bool,
            )
        )
        active_month_ids = rebate_month_ids[rebate_active_rows]
        if int(active_month_ids.numel()) > 1 and bool(
            (active_month_ids[1:] < active_month_ids[:-1]).any().item()
        ):
            raise ValueError("active session_month_ids must be non-decreasing")
    if can_short_open_mask is not None:
        if short_margin_rate is None:
            raise ValueError(
                "can_short_open_mask requires an explicit short_margin_rate"
            )
        if short_capacity_weights is None:
            raise ValueError(
                "can_short_open_mask requires explicit short_capacity_weights"
            )
    margin_rates = _as_phase_rate(
        0.90 if short_margin_rate is None else short_margin_rate,
        reference=phase_reference,
        name="short_margin_rate",
    )
    handling_rates = _as_phase_rate(
        short_handling_fee_rate,
        reference=phase_reference,
        name="short_handling_fee_rate",
    )
    if not torch.compiler.is_compiling() and bool(
        (sell_fees + handling_rates >= 1.0).any().item()
    ):
        raise ValueError(
            "sell fees plus short handling fees must leave positive sale collateral"
        )
    maintenance_ratio = action_values.new_tensor(
        _resolve_short_maintenance_ratio(short_maintenance_ratio)
    )
    daily_volume_cap = _as_daily_nonnegative(
        volume_limit_weights,
        reference=daily_reference,
        name="volume_limit_weights",
    )
    daily_short_cap = _as_daily_nonnegative(
        short_capacity_weights,
        reference=daily_reference,
        name="short_capacity_weights",
        allow_positive_inf=True,
    )

    if (
        isinstance(settlement_lag_sessions, bool)
        or int(settlement_lag_sessions) != settlement_lag_sessions
        or int(settlement_lag_sessions) <= 0
    ):
        raise ValueError("settlement_lag_sessions must be a positive integer")
    settlement_lag = int(settlement_lag_sessions)
    if claim_queue_sessions is None:
        resolved_claim_queue_sessions = settlement_lag
    elif (
        isinstance(claim_queue_sessions, bool)
        or not isinstance(claim_queue_sessions, int)
        or int(claim_queue_sessions) < settlement_lag
    ):
        raise ValueError(
            "claim_queue_sessions must be an integer at least as large as "
            "settlement_lag_sessions"
        )
    else:
        resolved_claim_queue_sessions = int(claim_queue_sessions)

    has_dividends = cash_dividend_yield is not None
    if has_dividends != (cash_dividend_payment_delay_sessions is not None):
        raise ValueError(
            "cash_dividend_yield and cash_dividend_payment_delay_sessions "
            "must be supplied together"
        )
    if has_dividends:
        dividend_yields = torch.as_tensor(
            cash_dividend_yield,
            device=action_values.device,
            dtype=action_values.dtype,
        )
        raw_dividend_delays = torch.as_tensor(
            cash_dividend_payment_delay_sessions,
            device=action_values.device,
        )
        if tuple(dividend_yields.shape) != tuple(daily_reference.shape):
            raise ValueError("cash_dividend_yield must have shape [T,S]")
        if tuple(raw_dividend_delays.shape) != tuple(daily_reference.shape):
            raise ValueError(
                "cash_dividend_payment_delay_sessions must have shape [T,S]"
            )
        if not torch.compiler.is_compiling() and (
            not bool(torch.isfinite(dividend_yields).all().item())
            or bool((dividend_yields < 0.0).any().item())
        ):
            raise ValueError("cash_dividend_yield must be finite and non-negative")
        dividend_delays = raw_dividend_delays.to(dtype=torch.int64)
        if (
            not torch.compiler.is_compiling()
            and raw_dividend_delays.is_floating_point()
            and not bool(
                (
                    raw_dividend_delays
                    == dividend_delays.to(dtype=raw_dividend_delays.dtype)
                )
                .all()
                .item()
            )
        ):
            raise ValueError(
                "cash_dividend_payment_delay_sessions must contain integers"
            )
        event_cells = dividend_yields > 0.0
        invalid_delay = event_cells & (
            (dividend_delays < 1) | (dividend_delays > resolved_claim_queue_sessions)
        )
        if not torch.compiler.is_compiling() and bool(invalid_delay.any().item()):
            raise ValueError(
                "cash-dividend payment delay exceeds the configured claim queue"
            )
        if not torch.compiler.is_compiling() and bool(
            ((~event_cells) & (dividend_delays != 0)).any().item()
        ):
            raise ValueError(
                "cash-dividend payment delay must be zero when yield is zero"
            )
    else:
        dividend_yields = torch.zeros_like(daily_reference)
        dividend_delays = torch.zeros_like(
            daily_reference,
            dtype=torch.int64,
        )

    if initial_alive is None:
        rebate_alive = torch.ones(
            (),
            device=action_values.device,
            dtype=torch.bool,
        )
    else:
        rebate_alive_source = (
            initial_alive.detach() if detach_initial_state else initial_alive
        )
        if rebate_alive_source.dim() != 0:
            raise ValueError("initial_alive must be a scalar tensor")
        rebate_alive = rebate_alive_source.clone(
            memory_format=torch.contiguous_format
        ).to(device=action_values.device, dtype=torch.bool)
    (
        commission_rebate_current,
        commission_rebate_due,
        commission_rebate_month_id,
    ) = prepare_commission_rebate_state(
        reference=daily_reference,
        initial_current=initial_commission_rebate_current,
        initial_due=initial_commission_rebate_due,
        initial_month_id=initial_commission_rebate_month_id,
        alive=rebate_alive,
        detach=detach_initial_state,
    )
    if (
        rebate_timing == "monthly_15th"
        and not torch.compiler.is_compiling()
        and int(active_month_ids.numel()) > 0
        and bool(
            (
                (commission_rebate_month_id > 0)
                & (commission_rebate_month_id > active_month_ids[0])
            ).item()
        )
    ):
        raise ValueError(
            "initial_commission_rebate_month_id cannot be later than the "
            "first active session month"
        )
    if rebate_timing == "daily_close" and not torch.compiler.is_compiling():
        has_carried_rebate = (
            (commission_rebate_current > 0.0)
            | (commission_rebate_due > 0.0)
            | (commission_rebate_month_id != 0)
        )
        if bool(has_carried_rebate.item()):
            raise ValueError(
                "daily_close commission rebates cannot carry an initial "
                "monthly rebate state"
            )
    (
        risky,
        cash,
        payables,
        receivables,
        short_sale_collateral,
        short_margin_collateral,
        alive,
        advance_mask,
    ) = _prepare_cash_short_state(
        daily_reference,
        settlement_lag_sessions=settlement_lag,
        claim_queue_sessions=resolved_claim_queue_sessions,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_alive=initial_alive,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        commission_rebate_current=commission_rebate_current,
        commission_rebate_due=commission_rebate_due,
        state_advance_mask=state_advance_mask,
        detach_initial_state=detach_initial_state,
    )
    (
        commission_rebate_current,
        commission_rebate_due,
        commission_rebate_month_id,
    ) = mask_commission_rebate_state(
        commission_rebate_current,
        commission_rebate_due,
        commission_rebate_month_id,
        alive,
    )
    equity_scale = _prepare_equity_scale(
        daily_reference,
        initial_equity_scale=initial_equity_scale,
        alive=alive,
        detach_initial_state=detach_initial_state,
    )

    strategy_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    event_turnover_rows: list[torch.Tensor] = []
    weights_rows: list[torch.Tensor] = []
    open_weights_rows: list[torch.Tensor] = []
    close_weights_rows: list[torch.Tensor] = []
    event_long_buy_rows: list[torch.Tensor] = []
    event_long_sell_rows: list[torch.Tensor] = []
    event_short_open_rows: list[torch.Tensor] = []
    event_short_cover_rows: list[torch.Tensor] = []
    cash_rows: list[torch.Tensor] = []
    payable_rows: list[torch.Tensor] = []
    receivable_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    short_sale_rows: list[torch.Tensor] = []
    short_margin_rows: list[torch.Tensor] = []
    rebate_accrued_rows: list[torch.Tensor] = []
    rebate_paid_rows: list[torch.Tensor] = []
    rebate_current_rows: list[torch.Tensor] = []
    rebate_due_rows: list[torch.Tensor] = []
    event_rebate_accrued_rows: list[torch.Tensor] = []
    due_rows: list[torch.Tensor] = []

    for idx in range(int(t_len)):
        advance = advance_mask[idx]
        default_now = torch.zeros_like(alive)
        pending_net_claim = torch.zeros_like(cash)
        (
            cash,
            payables,
            receivables,
            alive_after_settlement,
            settlement_default,
        ) = _settle_open_phase(
            cash,
            payables,
            receivables,
            alive,
            advance,
        )
        default_now = default_now | settlement_default
        risky = torch.where(
            alive_after_settlement,
            risky,
            torch.zeros_like(risky),
        )
        short_sale_collateral = torch.where(
            alive_after_settlement,
            short_sale_collateral,
            torch.zeros_like(short_sale_collateral),
        )
        short_margin_collateral = torch.where(
            alive_after_settlement,
            short_margin_collateral,
            torch.zeros_like(short_margin_collateral),
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_after_settlement,
        )
        base_nav_start = _cash_margin_nav(
            cash=cash,
            risky=risky,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        nav_start = (
            base_nav_start
            + commission_rebate_current
            + commission_rebate_due
        )

        # All carried overnight positions are the due cohort in tw_overnight.
        due = risky if mode == "tw_overnight" else torch.zeros_like(risky)
        active_gap = (
            advance & alive_after_settlement & (risky.abs() > _POSITION_TOLERANCE)
        )
        gap_default = (active_gap & ~overnight_return_valid[idx]).any()
        gap_default = advance & alive_after_settlement & gap_default
        default_now = default_now | gap_default
        (
            alive_after_gap,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=gap_default,
            alive=alive_after_settlement,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_after_gap,
        )
        effective_gap = torch.where(
            advance & alive_after_gap,
            overnight_returns[idx],
            torch.zeros_like(overnight_returns[idx]),
        )
        risky = risky * (1.0 + effective_gap)
        due = torch.where(
            alive_after_gap,
            due * (1.0 + effective_gap),
            torch.zeros_like(due),
        )
        base_nav_open = _cash_margin_nav(
            cash=cash,
            risky=risky,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        nav_open = (
            base_nav_open
            + commission_rebate_current
            + commission_rebate_due
        )
        open_nav_default = (
            advance
            & alive_after_gap
            & (~torch.isfinite(nav_open) | (nav_open <= _MIN_WEALTH_FACTOR))
        )
        default_now = default_now | open_nav_default
        (
            alive_phase,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=open_nav_default,
            alive=alive_after_gap,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_phase,
        )
        due = torch.where(alive_phase, due, torch.zeros_like(due))
        # ``nav_open`` remains the raw accounting mark used to classify
        # default and compute the day's return.  Once that mark has defaulted,
        # however, it may be +/-inf after an otherwise finite FP32 position
        # overflows on the price move.  Never multiply a model action by that
        # dead-account value: even an ultimately unselected ``0 * inf`` branch
        # poisons reverse-mode AD.  Event sizing is defined only for an active,
        # finite, positive-NAV account, so its exact fail-closed value is zero.
        open_event_nav = torch.where(
            advance
            & alive_phase
            & torch.isfinite(base_nav_open)
            & (base_nav_open > _MIN_WEALTH_FACTOR),
            base_nav_open,
            torch.zeros_like(base_nav_open),
        )

        remaining_volume_reference = _convert_reference_cap_to_ledger(
            None if daily_volume_cap is None else daily_volume_cap[idx],
            equity_scale=equity_scale,
            alive=alive_phase,
        )
        remaining_short_capacity_reference = _convert_reference_cap_to_ledger(
            None if daily_short_cap is None else daily_short_cap[idx],
            equity_scale=equity_scale,
            alive=alive_phase,
        )
        open_price_ratio = torch.where(
            overnight_return_valid[idx],
            1.0 + overnight_returns[idx],
            torch.zeros_like(overnight_returns[idx]),
        )
        remaining_volume = _reference_cap_to_event_notional(
            remaining_volume_reference,
            event_price_ratio=open_price_ratio,
        )
        remaining_short_capacity = _reference_cap_to_event_notional(
            remaining_short_capacity_reference,
            event_price_ratio=open_price_ratio,
        )
        remaining_turnover = (
            None
            if max_turnover_ratio <= 0.0
            else open_event_nav * float(max_turnover_ratio)
        )

        event_turnovers = [torch.zeros_like(cash), torch.zeros_like(cash)]
        event_long_buy = [
            torch.zeros_like(risky),
            torch.zeros_like(risky),
        ]
        event_long_sell = [
            torch.zeros_like(risky),
            torch.zeros_like(risky),
        ]
        event_short_open = [
            torch.zeros_like(risky),
            torch.zeros_like(risky),
        ]
        event_short_cover = [
            torch.zeros_like(risky),
            torch.zeros_like(risky),
        ]

        open_mandatory_exit = unresolved_action[idx, OPEN] | terminal[idx, OPEN]
        open_forced_cover = forced_cover[idx, OPEN]
        current_long = risky.clamp_min(0.0)
        current_short = (-risky).clamp_min(0.0)
        mandatory_long = torch.where(
            open_mandatory_exit,
            current_long,
            torch.zeros_like(current_long),
        )
        mandatory_cover = torch.where(
            open_mandatory_exit | open_forced_cover,
            current_short,
            torch.zeros_like(current_short),
        )
        if mode == "tw_cash":
            desired_open = torch.where(
                selection[idx, OPEN] & ~open_mandatory_exit,
                action_values[idx, OPEN] * open_event_nav,
                torch.zeros_like(risky),
            )
            desired_open = torch.where(
                open_forced_cover,
                desired_open.clamp_min(0.0),
                desired_open,
            )
        else:
            requested_due_after_open = due * (
                1.0 - action_values[idx, OVERNIGHT_EXIT_AT_OPEN]
            )
            desired_open = torch.where(
                open_mandatory_exit,
                torch.zeros_like(risky),
                requested_due_after_open,
            )
            desired_open = torch.where(
                open_forced_cover,
                desired_open.clamp_min(0.0),
                desired_open,
            )

        open_event = _execute_signed_event(
            risky_before=risky,
            desired_risky=desired_open,
            mandatory_long_sell=mandatory_long,
            mandatory_short_cover=mandatory_cover,
            can_buy=can_buy[idx, OPEN],
            can_sell=can_sell[idx, OPEN],
            can_short_open=short_open[idx, OPEN],
            buy_fees=buy_fees[idx, OPEN],
            sell_fees=sell_fees[idx, OPEN],
            margin_rates=margin_rates[idx, OPEN],
            short_handling_rates=handling_rates[idx, OPEN],
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            cash=cash,
            payables=payables,
            receivables=receivables,
            pending_net_claim=pending_net_claim,
            remaining_volume=remaining_volume,
            remaining_turnover=remaining_turnover,
            remaining_short_capacity=remaining_short_capacity,
            event_nav=open_event_nav,
            gross_budget=gross_budget,
            maintenance_ratio=maintenance_ratio,
            alive=alive_phase,
            advance=advance,
        )
        risky = open_event.risky
        short_sale_collateral = open_event.short_sale_collateral
        short_margin_collateral = open_event.short_margin_collateral
        pending_net_claim = open_event.pending_net_claim
        remaining_volume = open_event.remaining_volume
        remaining_turnover = open_event.remaining_turnover
        remaining_short_capacity = open_event.remaining_short_capacity
        event_turnovers[OPEN] = event_turnovers[OPEN] + open_event.turnover
        event_long_buy[OPEN] = event_long_buy[OPEN] + open_event.long_buy
        event_long_sell[OPEN] = event_long_sell[OPEN] + open_event.long_sell
        event_short_open[OPEN] = event_short_open[OPEN] + open_event.short_open
        event_short_cover[OPEN] = event_short_cover[OPEN] + open_event.short_cover
        default_now = default_now | open_event.default_now
        (
            alive_phase,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=open_event.default_now,
            alive=alive_phase,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_phase,
        )
        if mode == "tw_overnight":
            # No new cohort existed before this transition, so the residual is
            # exactly the due cohort.
            due = torch.where(alive_phase, risky, torch.zeros_like(risky))
            open_allocation = torch.where(
                selection[idx, OPEN]
                & ~open_mandatory_exit
                & ~(
                    open_forced_cover
                    & (action_values[idx, OVERNIGHT_ENTRY_AT_OPEN] < 0.0)
                ),
                action_values[idx, OVERNIGHT_ENTRY_AT_OPEN] * open_event_nav,
                torch.zeros_like(risky),
            )
            opposing_due = (
                (due.abs() > _POSITION_TOLERANCE)
                & (open_allocation.abs() > _POSITION_TOLERANCE)
                & ((due > 0.0) != (open_allocation > 0.0))
            )
            open_allocation = torch.where(
                opposing_due,
                torch.zeros_like(open_allocation),
                open_allocation,
            )
            before_entry = risky
            entry_event = _execute_signed_event(
                risky_before=risky,
                desired_risky=risky + open_allocation,
                mandatory_long_sell=torch.zeros_like(risky),
                mandatory_short_cover=torch.zeros_like(risky),
                can_buy=can_buy[idx, OPEN],
                can_sell=can_sell[idx, OPEN],
                can_short_open=short_open[idx, OPEN],
                buy_fees=buy_fees[idx, OPEN],
                sell_fees=sell_fees[idx, OPEN],
                margin_rates=margin_rates[idx, OPEN],
                short_handling_rates=handling_rates[idx, OPEN],
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                cash=cash,
                payables=payables,
                receivables=receivables,
                pending_net_claim=pending_net_claim,
                remaining_volume=remaining_volume,
                remaining_turnover=remaining_turnover,
                remaining_short_capacity=remaining_short_capacity,
                event_nav=open_event_nav,
                gross_budget=gross_budget,
                maintenance_ratio=maintenance_ratio,
                alive=alive_phase,
                advance=advance,
            )
            risky = entry_event.risky
            new_cohort = risky - before_entry
            short_sale_collateral = entry_event.short_sale_collateral
            short_margin_collateral = entry_event.short_margin_collateral
            pending_net_claim = entry_event.pending_net_claim
            remaining_volume = entry_event.remaining_volume
            remaining_turnover = entry_event.remaining_turnover
            remaining_short_capacity = entry_event.remaining_short_capacity
            event_turnovers[OPEN] = event_turnovers[OPEN] + entry_event.turnover
            event_long_buy[OPEN] = event_long_buy[OPEN] + entry_event.long_buy
            event_long_sell[OPEN] = event_long_sell[OPEN] + entry_event.long_sell
            event_short_open[OPEN] = event_short_open[OPEN] + entry_event.short_open
            event_short_cover[OPEN] = event_short_cover[OPEN] + entry_event.short_cover
            default_now = default_now | entry_event.default_now
            (
                alive_phase,
                risky,
                cash,
                payables,
                receivables,
                short_sale_collateral,
                short_margin_collateral,
                pending_net_claim,
            ) = _kill_finance_state(
                default_now=entry_event.default_now,
                alive=alive_phase,
                risky=risky,
                cash=cash,
                payables=payables,
                receivables=receivables,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                pending_net_claim=pending_net_claim,
            )
            (
                commission_rebate_current,
                commission_rebate_due,
                commission_rebate_month_id,
            ) = mask_commission_rebate_state(
                commission_rebate_current,
                commission_rebate_due,
                commission_rebate_month_id,
                alive_phase,
            )
            due = torch.where(alive_phase, due, torch.zeros_like(due))
            new_cohort = torch.where(
                alive_phase,
                new_cohort,
                torch.zeros_like(new_cohort),
            )
        else:
            new_cohort = torch.zeros_like(risky)

        remaining_volume_reference = _event_notional_cap_to_reference(
            remaining_volume,
            event_price_ratio=open_price_ratio,
        )
        remaining_short_capacity_reference = _event_notional_cap_to_reference(
            remaining_short_capacity,
            event_price_ratio=open_price_ratio,
        )
        if return_weights_history:
            nav_after_open = _cash_margin_nav(
                cash=cash,
                risky=risky,
                payables=payables,
                receivables=receivables,
                short_sale_collateral=short_sale_collateral,
                short_margin_collateral=short_margin_collateral,
                pending_net_claim=pending_net_claim,
            )
            open_execution_weights = _event_weight(
                risky,
                nav_after_open,
                alive_phase,
            )

        active_intraday = advance & alive_phase & (risky.abs() > _POSITION_TOLERANCE)
        intraday_default = (active_intraday & ~intraday_return_valid[idx]).any()
        intraday_default = advance & alive_phase & intraday_default
        default_now = default_now | intraday_default
        (
            alive_phase,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=intraday_default,
            alive=alive_phase,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_phase,
        )
        effective_intraday = torch.where(
            advance & alive_phase,
            intraday_returns[idx],
            torch.zeros_like(intraday_returns[idx]),
        )
        risky = risky * (1.0 + effective_intraday)
        if mode == "tw_overnight":
            due = due * (1.0 + effective_intraday)
            new_cohort = new_cohort * (1.0 + effective_intraday)
            due = torch.where(alive_phase, due, torch.zeros_like(due))
            new_cohort = torch.where(
                alive_phase,
                new_cohort,
                torch.zeros_like(new_cohort),
            )

        base_nav_close = _cash_margin_nav(
            cash=cash,
            risky=risky,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        nav_close = (
            base_nav_close
            + commission_rebate_current
            + commission_rebate_due
        )
        close_nav_default = (
            advance
            & alive_phase
            & (~torch.isfinite(nav_close) | (nav_close <= _MIN_WEALTH_FACTOR))
        )
        default_now = default_now | close_nav_default
        (
            alive_phase,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=close_nav_default,
            alive=alive_phase,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_phase,
        )
        if mode == "tw_overnight":
            due = torch.where(alive_phase, due, torch.zeros_like(due))
            new_cohort = torch.where(
                alive_phase,
                new_cohort,
                torch.zeros_like(new_cohort),
            )
        # Keep the raw close mark for accounting/default semantics, but expose
        # only a finite active-account NAV to action sizing and the event
        # executor.  This is the close-side analogue of ``open_event_nav``.
        close_event_nav = torch.where(
            advance
            & alive_phase
            & torch.isfinite(base_nav_close)
            & (base_nav_close > _MIN_WEALTH_FACTOR),
            base_nav_close,
            torch.zeros_like(base_nav_close),
        )

        close_mandatory_exit = unresolved_action[idx, CLOSE] | terminal[idx, CLOSE]
        close_forced_cover = forced_cover[idx, CLOSE]
        current_long = risky.clamp_min(0.0)
        current_short = (-risky).clamp_min(0.0)
        if mode == "tw_overnight":
            due_long = due.clamp_min(0.0)
            due_short = (-due).clamp_min(0.0)
        else:
            due_long = torch.zeros_like(current_long)
            due_short = torch.zeros_like(current_short)
        mandatory_long = torch.maximum(
            due_long,
            torch.where(
                close_mandatory_exit,
                current_long,
                torch.zeros_like(current_long),
            ),
        )
        mandatory_cover = torch.maximum(
            due_short,
            torch.where(
                close_mandatory_exit | close_forced_cover,
                current_short,
                torch.zeros_like(current_short),
            ),
        )
        if mode == "tw_cash":
            desired_close = torch.where(
                selection[idx, CLOSE] & ~close_mandatory_exit,
                action_values[idx, CLOSE] * close_event_nav,
                torch.zeros_like(risky),
            )
        else:
            close_allocation = torch.where(
                selection[idx, CLOSE] & ~close_mandatory_exit,
                action_values[idx, OVERNIGHT_ENTRY_AT_CLOSE] * close_event_nav,
                torch.zeros_like(risky),
            )
            close_allocation = torch.where(
                close_forced_cover & (close_allocation < 0.0),
                torch.zeros_like(close_allocation),
                close_allocation,
            )
            desired_close = new_cohort + close_allocation
            desired_close = torch.where(
                close_mandatory_exit,
                torch.zeros_like(desired_close),
                desired_close,
            )
        desired_close = torch.where(
            close_forced_cover,
            desired_close.clamp_min(0.0),
            desired_close,
        )
        close_price_ratio = torch.where(
            overnight_return_valid[idx] & intraday_return_valid[idx],
            open_price_ratio * (1.0 + intraday_returns[idx]),
            torch.zeros_like(intraday_returns[idx]),
        )
        remaining_volume = _reference_cap_to_event_notional(
            remaining_volume_reference,
            event_price_ratio=close_price_ratio,
        )
        remaining_short_capacity = _reference_cap_to_event_notional(
            remaining_short_capacity_reference,
            event_price_ratio=close_price_ratio,
        )

        close_event = _execute_signed_event(
            risky_before=risky,
            desired_risky=desired_close,
            mandatory_long_sell=mandatory_long,
            mandatory_short_cover=mandatory_cover,
            can_buy=can_buy[idx, CLOSE],
            can_sell=can_sell[idx, CLOSE],
            can_short_open=short_open[idx, CLOSE],
            buy_fees=buy_fees[idx, CLOSE],
            sell_fees=sell_fees[idx, CLOSE],
            margin_rates=margin_rates[idx, CLOSE],
            short_handling_rates=handling_rates[idx, CLOSE],
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            cash=cash,
            payables=payables,
            receivables=receivables,
            pending_net_claim=pending_net_claim,
            remaining_volume=remaining_volume,
            remaining_turnover=remaining_turnover,
            remaining_short_capacity=remaining_short_capacity,
            event_nav=close_event_nav,
            gross_budget=gross_budget,
            maintenance_ratio=maintenance_ratio,
            alive=alive_phase,
            advance=advance,
        )
        risky = close_event.risky
        short_sale_collateral = close_event.short_sale_collateral
        short_margin_collateral = close_event.short_margin_collateral
        pending_net_claim = close_event.pending_net_claim
        event_turnovers[CLOSE] = close_event.turnover
        event_long_buy[CLOSE] = close_event.long_buy
        event_long_sell[CLOSE] = close_event.long_sell
        event_short_open[CLOSE] = close_event.short_open
        event_short_cover[CLOSE] = close_event.short_cover
        default_now = default_now | close_event.default_now
        (
            alive_phase,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            pending_net_claim,
        ) = _kill_finance_state(
            default_now=close_event.default_now,
            alive=alive_phase,
            risky=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = mask_commission_rebate_state(
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            alive_phase,
        )

        if mode == "tw_overnight":
            # The due amounts were mandatory inputs to the close event.  The
            # event either executed them in full or already entered absorbing
            # default when a physical side was unavailable.
            due = torch.zeros_like(due)

        next_payables, next_receivables = _enqueue_net_claim(
            payables,
            receivables,
            pending_net_claim,
            settlement_lag_sessions=settlement_lag,
        )
        active_close = advance & alive_phase
        payables = torch.where(active_close, next_payables, payables)
        receivables = torch.where(active_close, next_receivables, receivables)
        pending_net_claim = torch.where(
            active_close,
            torch.zeros_like(pending_net_claim),
            pending_net_claim,
        )
        # This is both the exact CLOSE execution denominator and the one
        # unavoidable O(S) end-of-day ledger reduction.  Add the subsequently
        # earned entitlement to this stable pre-dividend value instead of
        # recovering it by subtracting two potentially huge nearby numbers.
        close_execution_nav = _cash_margin_nav(
            cash=cash,
            risky=risky,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            pending_net_claim=pending_net_claim,
        )
        event_commission_rebate_accrued = torch.stack(
            [
                (
                    (
                        event_long_buy[phase]
                        + event_long_sell[phase]
                        + event_short_open[phase]
                        + event_short_cover[phase]
                    )
                    * rebate_rates[idx, phase]
                ).sum()
                for phase in (OPEN, CLOSE)
            ]
        )
        commission_rebate_accrued = event_commission_rebate_accrued.sum()
        (
            cash,
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
            commission_rebate_paid,
        ) = apply_commission_rebate_at_close(
            earned=commission_rebate_accrued,
            cash=cash,
            current=commission_rebate_current,
            due=commission_rebate_due,
            accrual_month_id=commission_rebate_month_id,
            session_month_id=rebate_month_ids[idx],
            payment_eligible=rebate_payment_eligible[idx],
            timing=rebate_timing,
            advance=advance,
            alive=alive_phase,
        )
        dividend_claims = (
            risky.clamp_min(0.0)
            * dividend_yields[idx]
            * active_close.to(dtype=action_values.dtype)
        )
        dividend_indices = (dividend_delays[idx] - 1).clamp(
            min=0,
            max=resolved_claim_queue_sessions - 1,
        )
        scheduled_dividends = torch.zeros_like(receivables).scatter_add(
            0,
            dividend_indices,
            dividend_claims,
        )
        receivables = receivables + scheduled_dividends
        nav_end = (
            close_execution_nav
            + commission_rebate_paid
            + commission_rebate_current
            + commission_rebate_due
            + scheduled_dividends.sum()
        )
        if return_weights_history:
            # Phase histories are execution-boundary audits.  The just-created
            # same-close entitlement therefore cannot retroactively change the
            # weight at which the closing auction executed.
            close_execution_weights = _event_weight(
                risky,
                close_execution_nav,
                alive_phase,
            )
        (
            simple_return,
            risky,
            cash,
            payables,
            receivables,
            short_sale_collateral,
            short_margin_collateral,
            alive,
            default_event,
        ) = _finalize_cash_day(
            nav_start=nav_start,
            nav_end=nav_end,
            risky_end=risky,
            cash=cash,
            payables=payables,
            receivables=receivables,
            short_sale_collateral=short_sale_collateral,
            short_margin_collateral=short_margin_collateral,
            alive=alive_phase,
            advance=advance,
            default_now=default_now,
        )
        (
            commission_rebate_current,
            commission_rebate_due,
            commission_rebate_month_id,
        ) = normalize_commission_rebate_state(
            current=commission_rebate_current,
            due=commission_rebate_due,
            month_id=commission_rebate_month_id,
            nav_end=nav_end,
            advance=advance,
            alive=alive,
        )
        equity_scale = _advance_equity_scale(
            equity_scale,
            simple_return,
            advance=advance,
            alive=alive,
        )
        if return_weights_history:
            close_execution_weights = torch.where(
                alive,
                close_execution_weights,
                torch.zeros_like(close_execution_weights),
            )
            open_execution_weights = torch.where(
                alive,
                open_execution_weights,
                torch.zeros_like(open_execution_weights),
            )
        # Internal event quantities stay in ledger-notional units so fees,
        # cash, shared turnover capacity, and settlement remain exact. Public
        # turnover and executed-weight audits use the common liquid
        # start-of-day ``base_nav_open`` denominator.  Pending monthly rebates
        # are owned NAV but cannot fund or size either auction before payment.
        safe_turnover_nav = torch.where(
            torch.isfinite(base_nav_open)
            & (base_nav_open > _MIN_WEALTH_FACTOR),
            base_nav_open,
            torch.ones_like(base_nav_open),
        )
        normalized_event_turnovers = torch.stack(event_turnovers) / safe_turnover_nav
        daily_turnover = normalized_event_turnovers.sum()

        strategy_rows.append(
            torch.log1p(simple_return.clamp_min(-1.0 + _MIN_WEALTH_FACTOR))
        )
        turnover_rows.append(daily_turnover)
        if return_weights_history:
            event_turnover_rows.append(normalized_event_turnovers)
            # Preserve the established reporting contract: history is the
            # realised portfolio at the close execution point. ``risky`` is
            # the recurrent end-of-day ledger state and can differ after a
            # same-close dividend entitlement increases receivables.
            weights_rows.append(close_execution_weights)
            open_weights_rows.append(open_execution_weights)
            close_weights_rows.append(close_execution_weights)
        if return_weights_history:
            event_long_buy_rows.append(
                torch.stack(event_long_buy) / safe_turnover_nav
            )
            event_long_sell_rows.append(
                torch.stack(event_long_sell) / safe_turnover_nav
            )
            event_short_open_rows.append(
                torch.stack(event_short_open) / safe_turnover_nav
            )
            event_short_cover_rows.append(
                torch.stack(event_short_cover) / safe_turnover_nav
            )
        cash_rows.append(cash)
        payable_rows.append(payables)
        receivable_rows.append(receivables)
        default_rows.append(default_event)
        equity_scale_rows.append(equity_scale)
        short_sale_rows.append(short_sale_collateral)
        short_margin_rows.append(short_margin_collateral)
        rebate_accrued_rows.append(commission_rebate_accrued)
        rebate_paid_rows.append(commission_rebate_paid)
        rebate_current_rows.append(commission_rebate_current)
        rebate_due_rows.append(commission_rebate_due)
        event_rebate_accrued_rows.append(event_commission_rebate_accrued)
        if mode == "tw_overnight" and return_weights_history:
            # Today's close cohort is tomorrow's due portfolio.  Store it in
            # execution-weight units, matching the field name and artifacts.
            due_rows.append(close_execution_weights)

    empty_history = action_values.new_empty((0, symbols))
    weights_history = (
        torch.stack(weights_rows) if return_weights_history else empty_history
    )
    open_weights_history = (
        torch.stack(open_weights_rows) if return_weights_history else empty_history
    )
    close_weights_history = (
        torch.stack(close_weights_rows) if return_weights_history else empty_history
    )
    empty_phase_history = action_values.new_empty((0, 2, symbols))
    long_buy_history = (
        torch.stack(event_long_buy_rows)
        if return_weights_history
        else empty_phase_history
    )
    long_sell_history = (
        torch.stack(event_long_sell_rows)
        if return_weights_history
        else empty_phase_history
    )
    short_open_history = (
        torch.stack(event_short_open_rows)
        if return_weights_history
        else empty_phase_history
    )
    short_cover_history = (
        torch.stack(event_short_cover_rows)
        if return_weights_history
        else empty_phase_history
    )
    due_history = (
        torch.stack(due_rows)
        if mode == "tw_overnight" and return_weights_history
        else None
    )
    return TaiwanDualSessionResult(
        mode=mode,
        strategy_returns=torch.stack(strategy_rows),
        turnovers=torch.stack(turnover_rows),
        event_turnovers=(
            torch.stack(event_turnover_rows)
            if return_weights_history
            else action_values.new_empty((0, 2))
        ),
        weights_history=weights_history,
        open_weights_history=open_weights_history,
        close_weights_history=close_weights_history,
        executed_buy_weights=long_buy_history + short_cover_history,
        executed_sell_weights=long_sell_history + short_open_history,
        executed_long_buy_weights=long_buy_history,
        executed_long_sell_weights=long_sell_history,
        executed_short_open_weights=short_open_history,
        executed_short_cover_weights=short_cover_history,
        cash_history=torch.stack(cash_rows),
        payables_history=torch.stack(payable_rows),
        receivables_history=torch.stack(receivable_rows),
        settlement_default=torch.stack(default_rows),
        equity_scale_history=torch.stack(equity_scale_rows),
        short_sale_collateral_history=torch.stack(short_sale_rows),
        short_margin_collateral_history=torch.stack(short_margin_rows),
        final_weights=risky,
        final_cash=cash,
        final_payables=payables,
        final_receivables=receivables,
        final_alive=alive,
        final_equity_scale=equity_scale,
        final_short_sale_collateral=short_sale_collateral,
        final_short_margin_collateral=short_margin_collateral,
        commission_rebate_accrued_history=torch.stack(rebate_accrued_rows),
        commission_rebate_paid_history=torch.stack(rebate_paid_rows),
        commission_rebate_current_history=torch.stack(rebate_current_rows),
        commission_rebate_due_history=torch.stack(rebate_due_rows),
        event_commission_rebate_accrued=torch.stack(
            event_rebate_accrued_rows
        ),
        final_commission_rebate_current=commission_rebate_current,
        final_commission_rebate_due=commission_rebate_due,
        final_commission_rebate_month_id=commission_rebate_month_id,
        due_weights_history=due_history,
        final_due_weights=(risky if mode == "tw_overnight" else None),
    )


def run_tw_cash_dual_session(
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    *,
    commission_rebate_rates: torch.Tensor | float = 0.0,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: torch.Tensor | None = None,
    commission_rebate_payment_eligible_mask: torch.Tensor | None = None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | float | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | float | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_commission_rebate_current: torch.Tensor | None = None,
    initial_commission_rebate_due: torch.Tensor | None = None,
    initial_commission_rebate_month_id: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    _detach_initial_state: bool = True,
) -> TaiwanDualSessionResult:
    """Run signed OPEN/CLOSE post-event targets with one daily T+2 ledger."""

    return _run_dual_session(
        mode="tw_cash",
        actions=actions,
        overnight_log_returns=overnight_log_returns,
        intraday_log_returns=intraday_log_returns,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        gross_budget=gross_budget,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        state_advance_mask=state_advance_mask,
        return_weights_history=return_weights_history,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
        initial_alive=initial_alive,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        detach_initial_state=_detach_initial_state,
    )


def run_tw_overnight_dual_session(
    actions: torch.Tensor,
    overnight_log_returns: torch.Tensor,
    intraday_log_returns: torch.Tensor,
    tradable_mask: torch.Tensor,
    can_buy_mask: torch.Tensor,
    can_sell_mask: torch.Tensor,
    buy_fee_rates: torch.Tensor | float,
    sell_fee_rates: torch.Tensor | float,
    *,
    commission_rebate_rates: torch.Tensor | float = 0.0,
    commission_rebate_timing: str = "daily_close",
    session_month_ids: torch.Tensor | None = None,
    commission_rebate_payment_eligible_mask: torch.Tensor | None = None,
    can_short_open_mask: torch.Tensor | None = None,
    force_short_cover_mask: torch.Tensor | None = None,
    short_margin_rate: torch.Tensor | float | None = None,
    short_capacity_weights: torch.Tensor | float | None = None,
    short_handling_fee_rate: torch.Tensor | float = 0.0,
    short_maintenance_ratio: float = 1.30,
    unresolved_corporate_action_mask: torch.Tensor | None = None,
    cash_dividend_yield: torch.Tensor | None = None,
    cash_dividend_payment_delay_sessions: torch.Tensor | None = None,
    claim_queue_sessions: int | None = None,
    force_exit_mask: torch.Tensor | None = None,
    settlement_lag_sessions: int = 2,
    gross_budget: float = 1.0,
    max_turnover_ratio: float = 0.0,
    volume_limit_weights: torch.Tensor | float | None = None,
    state_advance_mask: torch.Tensor | None = None,
    return_weights_history: bool = True,
    initial_weights: torch.Tensor | None = None,
    initial_cash: torch.Tensor | None = None,
    initial_payables: torch.Tensor | None = None,
    initial_receivables: torch.Tensor | None = None,
    initial_commission_rebate_current: torch.Tensor | None = None,
    initial_commission_rebate_due: torch.Tensor | None = None,
    initial_commission_rebate_month_id: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_short_sale_collateral: torch.Tensor | None = None,
    initial_short_margin_collateral: torch.Tensor | None = None,
    _detach_initial_state: bool = True,
) -> TaiwanDualSessionResult:
    """Run strict one-session cohorts with open/close entry and exit timing.

    ``actions[:, 0, :]`` is an already-resolved fraction in ``[0,1]``.  The
    executor deliberately does not apply sigmoid, so policy/model and execution
    contracts cannot disagree about logits versus fractions.
    """

    return _run_dual_session(
        mode="tw_overnight",
        actions=actions,
        overnight_log_returns=overnight_log_returns,
        intraday_log_returns=intraday_log_returns,
        tradable_mask=tradable_mask,
        can_buy_mask=can_buy_mask,
        can_sell_mask=can_sell_mask,
        buy_fee_rates=buy_fee_rates,
        sell_fee_rates=sell_fee_rates,
        commission_rebate_rates=commission_rebate_rates,
        commission_rebate_timing=commission_rebate_timing,
        session_month_ids=session_month_ids,
        commission_rebate_payment_eligible_mask=(
            commission_rebate_payment_eligible_mask
        ),
        can_short_open_mask=can_short_open_mask,
        force_short_cover_mask=force_short_cover_mask,
        short_margin_rate=short_margin_rate,
        short_capacity_weights=short_capacity_weights,
        short_handling_fee_rate=short_handling_fee_rate,
        short_maintenance_ratio=short_maintenance_ratio,
        unresolved_corporate_action_mask=unresolved_corporate_action_mask,
        cash_dividend_yield=cash_dividend_yield,
        cash_dividend_payment_delay_sessions=(cash_dividend_payment_delay_sessions),
        claim_queue_sessions=claim_queue_sessions,
        force_exit_mask=force_exit_mask,
        settlement_lag_sessions=settlement_lag_sessions,
        gross_budget=gross_budget,
        max_turnover_ratio=max_turnover_ratio,
        volume_limit_weights=volume_limit_weights,
        state_advance_mask=state_advance_mask,
        return_weights_history=return_weights_history,
        initial_weights=initial_weights,
        initial_cash=initial_cash,
        initial_payables=initial_payables,
        initial_receivables=initial_receivables,
        initial_commission_rebate_current=initial_commission_rebate_current,
        initial_commission_rebate_due=initial_commission_rebate_due,
        initial_commission_rebate_month_id=initial_commission_rebate_month_id,
        initial_alive=initial_alive,
        initial_equity_scale=initial_equity_scale,
        initial_short_sale_collateral=initial_short_sale_collateral,
        initial_short_margin_collateral=initial_short_margin_collateral,
        detach_initial_state=_detach_initial_state,
    )


__all__ = [
    "CLOSE",
    "OPEN",
    "OVERNIGHT_ENTRY_AT_CLOSE",
    "OVERNIGHT_ENTRY_AT_OPEN",
    "OVERNIGHT_EXIT_AT_OPEN",
    "TaiwanDualSessionResult",
    "run_tw_cash_dual_session",
    "run_tw_overnight_dual_session",
]
