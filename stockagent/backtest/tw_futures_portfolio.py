"""Continuous notional ledger for TAIFEX delivery-month-slot positions."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import math
import os
from typing import Callable, Final

import numpy as np
import torch
import torch.nn.functional as F

from stockagent.data.tw_futures_portfolio_daily import (
    TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION,
)
from stockagent.data import tw_futures_margin as margin
from stockagent.backtest.futures_cuda_graph import (
    futures_cuda_graph_enabled, run_futures_cuda_graph,
)


# Bump whenever the exact execution or backward-only relaxation changes.
# Partial-capacity fills, strict funding and cost/zero-action gradients changed
# together in this revision. Old optimizer states must not silently resume.
TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE = (
    "grouped_fake_floor_cash_surrogate_v5"
)
TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE = (
    "grouped_fake_floor_cash_solvency_recovery_surrogate_v7"
)
TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_FORWARD = "exact_integer_account_v3"

TW_FUTURES_PORTFOLIO_DEFAULT_NONE = 0
TW_FUTURES_PORTFOLIO_DEFAULT_FUNDING = 1
TW_FUTURES_PORTFOLIO_DEFAULT_NONFINITE_EQUITY = 2
TW_FUTURES_PORTFOLIO_DEFAULT_NONPOSITIVE_EQUITY = 3
TW_FUTURES_PORTFOLIO_DEFAULT_MARGIN_LIQUIDATION = 4

# The exact integer account and its backward-only shadow are both recurrent.
# Compiling a whole global batch unrolls a large graph, while eager execution
# launches hundreds of tiny scatter/reduction kernels per batch.  A fixed
# power-of-two block amortizes launch overhead without changing either state
# machine.  Sixteen rows is the measured dual-RTX-5090 throughput point; an
# environment override remains available for hardware re-benchmarking.
TW_FUTURES_PORTFOLIO_INTEGER_COMPILED_BLOCK_ROWS: Final[int] = 16
_COMPILED_INTEGER_BLOCKS: dict[
    tuple[object, ...], Callable[..., tuple[torch.Tensor, ...]]
] = {}
_FAILED_INTEGER_BLOCKS: set[tuple[object, ...]] = set()
_INTEGER_COMPILE_STATS: dict[str, int] = {
    "compile_constructors": 0,
    "compiled_block_calls": 0,
    "compiled_day_calls": 0,
    "compiled_tail_calls": 0,
    "eager_fallback_calls": 0,
}


@dataclass(slots=True)
class FuturesPortfolioTensorResult:
    strategy_returns: torch.Tensor
    turnovers: torch.Tensor
    weights_history: torch.Tensor
    final_weights: torch.Tensor
    final_alive: torch.Tensor
    equity_scale_history: torch.Tensor | None = None
    final_equity_scale: torch.Tensor | None = None
    contract_quantities_history: torch.Tensor | None = None
    default_history: torch.Tensor | None = None
    default_reason_history: torch.Tensor | None = None
    margin_audit_history: torch.Tensor | None = None
    residual_contract_quantities_history: torch.Tensor | None = None
    # Internal continuation state for fixed-block compilation.  Public exact
    # accounting continues to use ``final_weights`` (whole contracts),
    # ``final_equity_scale``, and ``final_alive``.  These shadow fields never
    # replace forward values or enter validation/inference artifacts.
    _surrogate_final_weights: torch.Tensor | None = None
    _surrogate_final_equity_scale: torch.Tensor | None = None
    _surrogate_final_alive: torch.Tensor | None = None


@dataclass(slots=True)
class FuturesPortfolioNumpyResult:
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    weights_history: np.ndarray
    final_weights: np.ndarray
    final_alive: np.ndarray


def _env_truthy(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
        "",
    }


def _strict_no_fallback_enabled() -> bool:
    return _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0")


def get_tw_futures_portfolio_integer_compile_stats(
    *, reset: bool = False,
) -> dict[str, int]:
    """Return process-local fixed-block usage counters."""

    snapshot = dict(_INTEGER_COMPILE_STATS)
    if reset:
        for name in _INTEGER_COMPILE_STATS:
            _INTEGER_COMPILE_STATS[name] = 0
    return snapshot


def resolve_tw_futures_portfolio_integer_compiled_block_rows() -> int:
    """Resolve the one effective fixed-block length used by all callers."""

    try:
        return int(
            os.environ.get(
                "STOCKAGENT_TW_FUTURES_PORTFOLIO_COMPILE_BLOCK_ROWS",
                str(TW_FUTURES_PORTFOLIO_INTEGER_COMPILED_BLOCK_ROWS),
            )
        )
    except ValueError:
        return 0


def _integer_group_candidate_baskets(
    target_cash: torch.Tensor,
    reserved_cash: torch.Tensor,
    maximum_contracts: torch.Tensor,
    valid: torch.Tensor,
) -> torch.Tensor:
    """Return bounded standard-first, residual-refill, mini-only baskets."""

    groups = int(target_cash.numel())
    expected = (groups, 2)
    for name, value in (
        ("reserved_cash", reserved_cash),
        ("maximum_contracts", maximum_contracts),
        ("valid", valid),
    ):
        if tuple(value.shape) != expected:
            raise ValueError(f"{name} must have shape [G,2]")
    safe_cash = torch.where(
        valid,
        reserved_cash,
        torch.full_like(reserved_cash, float("inf")),
    )
    caps = torch.where(
        valid,
        torch.floor(maximum_contracts.clamp_min(0.0)),
        torch.zeros_like(maximum_contracts),
    )
    eps = (
        torch.finfo(target_cash.dtype).eps
        * target_cash.abs().clamp_min(1.0)
        * 8.0
    )
    standard_max = torch.minimum(
        torch.floor((target_cash + eps) / safe_cash[:, 0]),
        caps[:, 0],
    ).clamp_min(0.0)
    # Together with the two safety baskets appended by the executor, this makes
    # a power-of-two 32-column compile shape: 28 descending 1/32 budget levels,
    # then mini-only, cash, unchanged-current, and maximum-close. The denser
    # frontier avoids a large exposure jump during account-wide de-risking.
    numerators = torch.arange(
        32,
        4,
        -1,
        device=target_cash.device,
        dtype=target_cash.dtype,
    )
    standard_candidates = torch.floor(
        standard_max[:, None] * numerators[None, :] / 32.0
    )
    standard_candidates = torch.cat(
        (
            standard_candidates,
            torch.zeros((groups, 2), device=target_cash.device, dtype=target_cash.dtype),
        ),
        dim=1,
    )
    used = standard_candidates * torch.where(
        valid[:, 0], reserved_cash[:, 0], torch.zeros_like(reserved_cash[:, 0])
    )[:, None]
    residual = (target_cash[:, None] - used).clamp_min(0.0)
    mini = torch.minimum(
        torch.floor((residual + eps[:, None]) / safe_cash[:, 1, None]),
        caps[:, 1, None],
    ).clamp_min(0.0)
    # The final explicit all-cash candidate makes zero exposure selectable even
    # when a mini denomination happens to fit the target sleeve.
    mini[:, -1] = 0.0
    return torch.stack((standard_candidates, mini), dim=-1).to(torch.int64)


@torch.library.custom_op("stockagent::futures_funding_sum", mutates_args=())
def _native_funding_sum(value: torch.Tensor) -> torch.Tensor:
    # Funding is a discrete decision at the FP32 boundary. Preserve ATen's
    # reduction tree instead of letting Inductor reassociate the global sum.
    return value.contiguous().sum()


@_native_funding_sum.register_fake
def _fake_funding_sum(value: torch.Tensor) -> torch.Tensor:
    return value.new_empty(())


def _funding_sum(value: torch.Tensor) -> torch.Tensor:
    if torch.compiler.is_compiling():
        return _native_funding_sum(value)
    return value.sum()


def _globally_funded_group_candidate_indices_impl(
    *,
    cash_required: torch.Tensor,
    candidate_exposure: torch.Tensor,
    target_exposure: torch.Tensor,
    capacity_ok: torch.Tensor,
    target_cash: torch.Tensor,
    equity: torch.Tensor,
    target_candidate_count: int,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Project discrete group choices onto the account-wide funding set.

    Target baskets may spend only their model-requested sleeve. The terminal
    current/max-close safety baskets may exceed that sleeve because a blocked
    carried position is a pre-existing liability, not a new allocation.

    When independent tracking-optimal choices overfund the account, a common
    radial scale contracts every model-requested exposure by the same factor.
    This keeps relative model conviction intact and introduces neither top-K,
    a long/short quota, nor freed-cash reassignment. If even every group's
    minimum-cash capacity-feasible basket exceeds equity, ``fundable`` is false
    and the caller records a real funding default.
    """

    if cash_required.ndim != 2 or candidate_exposure.shape != cash_required.shape:
        raise ValueError("candidate cash and exposure must have shape [G,K]")
    if capacity_ok.shape != cash_required.shape:
        raise ValueError("candidate capacity mask must have shape [G,K]")
    groups, candidates = tuple(cash_required.shape)
    target_count = int(target_candidate_count)
    if not 0 < target_count <= candidates:
        raise ValueError("target_candidate_count must be within the candidate axis")
    if tuple(target_cash.shape) != (groups,):
        raise ValueError("target_cash must have shape [G]")
    if tuple(target_exposure.shape) != (groups,):
        raise ValueError("target_exposure must have shape [G]")
    if equity.numel() != 1:
        raise ValueError("equity must be scalar")

    dtype = cash_required.dtype
    finite_candidate = torch.isfinite(cash_required) & torch.isfinite(
        candidate_exposure
    )
    target_allowed = (
        capacity_ok[:, :target_count]
        & finite_candidate[:, :target_count]
        & (
            cash_required[:, :target_count]
            <= target_cash[:, None]
        )
    )
    safety_allowed = (
        capacity_ok[:, target_count:] & finite_candidate[:, target_count:]
    )
    allowed = torch.cat((target_allowed, safety_allowed), dim=1)

    # The unchanged-current basket is normally capacity-feasible. Keep a tensor
    # fallback for malformed metadata so the final exact funding check fails
    # closed instead of choosing an arbitrary disallowed candidate.
    fallback = torch.zeros_like(allowed)
    fallback[:, -2] = True
    allowed = torch.where(allowed.any(dim=1, keepdim=True), allowed, fallback)

    scale = equity.detach().abs().clamp_min(1.0)
    cash_norm = cash_required / scale
    large = (
        cash_norm.detach().amax(dim=1, keepdim=True)
        + (candidate_exposure.detach().abs() / scale).amax(dim=1, keepdim=True)
        + (target_exposure.detach().abs() / scale)[:, None]
        + 2.0
    )

    def choose(radial_scale: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        scaled_target = target_exposure[:, None] * radial_scale
        score = (candidate_exposure - scaled_target).abs() / scale
        # Exact tracking-error ties prefer lower cash.
        score = score + torch.finfo(dtype).eps * 32.0 * cash_norm
        score = torch.where(allowed, score, large + score.abs())
        selected = score.argmin(dim=1)
        selected_cash = cash_required.gather(1, selected[:, None])[:, 0]
        return selected, _funding_sum(selected_cash)

    zero = cash_required.new_zeros(())
    one = cash_required.new_ones(())
    unconstrained, unconstrained_cash = choose(one)
    # These are spendable-cash limits, not approximate numerical equalities.
    # An epsilon here can select an unaffordable basket and turn a perfectly
    # feasible cash account into a funding default at the final ledger gate.
    unconstrained_fits = unconstrained_cash <= equity

    min_cash_score = torch.where(allowed, cash_required, large * scale)
    min_cash = min_cash_score.argmin(dim=1)
    min_cash_total = _funding_sum(cash_required.gather(1, min_cash[:, None])[:, 0])
    fundable = torch.isfinite(min_cash_total) & (
        min_cash_total <= equity
    )

    low = zero
    high = one
    best = min_cash
    # Eight iterations resolve the 1/32 candidate frontier with extra room for
    # fee/tax discontinuities, while keeping compiled block cost bounded.
    for _ in range(8):
        midpoint = (low + high) * 0.5
        midpoint_choice, midpoint_cash = choose(midpoint)
        midpoint_fits = midpoint_cash <= equity
        low = torch.where(midpoint_fits, midpoint, low)
        high = torch.where(midpoint_fits, high, midpoint)
        best = torch.where(midpoint_fits, midpoint_choice, best)

    selected = torch.where(unconstrained_fits, unconstrained, best)
    selected_cash_total = _funding_sum(cash_required.gather(1, selected[:, None])[:, 0])
    # Reversing a carried position and fixed per-contract costs can make the
    # discrete frontier locally non-monotone.  The radial search is therefore
    # an optimization only: when its final choice misses the funding boundary,
    # fall back to the independently proven minimum-cash feasible state.  A
    # real funding default is recorded only when that state is also impossible.
    selected = torch.where(
        selected_cash_total <= equity,
        selected,
        min_cash,
    )
    selected = torch.where(fundable, selected, min_cash)
    return selected, fundable


@lru_cache(maxsize=1)
def _compiled_funding_selector():
    # Fuse only the discrete funding search. Retain FP32 rounding, division
    # and subnormals; the chronological ledger and surrogate remain unchanged.
    return torch.compile(
        _globally_funded_group_candidate_indices_impl, fullgraph=True, dynamic=False,
        options={"triton.cudagraphs": False, "emulate_precision_casts": True,
                 "eager_numerics.division_rounding": True,
                 "eager_numerics.disable_ftz": True, "force_same_precision": True},
    )


def _globally_funded_group_candidate_indices(
    *,
    cash_required: torch.Tensor,
    candidate_exposure: torch.Tensor,
    target_exposure: torch.Tensor,
    capacity_ok: torch.Tensor,
    target_cash: torch.Tensor,
    equity: torch.Tensor,
    target_candidate_count: int,
) -> tuple[torch.Tensor, torch.Tensor]:
    function = _globally_funded_group_candidate_indices_impl
    if (cash_required.device.type == "cuda"
            and _env_truthy("STOCKAGENT_FUTURES_FUNDING_COMPILE", "0")
            and not torch.compiler.is_compiling()):
        function = _compiled_funding_selector()
    return function(
        cash_required=cash_required, candidate_exposure=candidate_exposure,
        target_exposure=target_exposure, capacity_ok=capacity_ok,
        target_cash=target_cash, equity=equity,
        target_candidate_count=target_candidate_count,
    )


def _absolute_cost_with_intent(
    executed: torch.Tensor, continuous_intent: torch.Tensor,
) -> torch.Tensor:
    """Keep absolute executed cost, including its sub-contract shadow slope.

    Quantization can make the forward exposure/delta exactly zero despite a
    nonzero continuous request. Ordinary abs then drops the fee derivative
    while the straight-through PnL still rewards that request. At that one
    boundary, use the request's direction for the local cost slope. Nonzero
    execution retains its usual derivative and exactly flat intent stays flat.
    """

    return torch.where(
        executed == 0,
        executed * torch.sign(continuous_intent.detach()),
        executed.abs(),
    )


def _project_margin_position_limits_impl(
    proposed: torch.Tensor,
    previous: torch.Tensor,
    *,
    position_group: torch.Tensor,
    position_units: torch.Tensor,
    group_limits: torch.Tensor,
    close_capacity: torch.Tensor,
    whole_contracts: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Share the legal position geometry between execution and its relaxation.

    Preserve reductions, allocate remaining group capacity to increases, then
    close only a dated limit's remaining excess through permitted physical
    capacity. The relaxation omits integer rounding, never the legal boundary.
    """
    pg, units, limits = position_group, position_units, group_limits
    same_sign = proposed * previous > 0
    reduced = torch.where(same_sign, previous.sign() * torch.minimum(
        previous.abs(), proposed.abs()), torch.zeros_like(previous))
    increases = proposed - reduced
    used = torch.zeros_like(units).scatter_add(0, pg, reduced.abs() * units)
    wanted = torch.zeros_like(units).scatter_add(0, pg, increases.abs() * units)
    scale = torch.minimum(torch.ones_like(used), (limits - used).clamp_min(0) / wanted.clamp_min(1e-12))
    scaled = increases * scale[pg]
    selected = reduced + (torch.trunc(scaled).long() if whole_contracts else scaled)
    selected_units = torch.zeros_like(units).scatter_add(0, pg, selected.abs() * units)
    excess = (selected_units - limits).clamp_min(0.)
    minimum_residual = (previous.abs() - close_capacity).clamp_min(0)
    available = torch.where(selected * previous > 0,
        (selected.abs() - minimum_residual).clamp_min(0), torch.zeros_like(previous))
    available_units = torch.zeros_like(units).scatter_add(0, pg, available * units)
    fraction = torch.minimum(torch.ones_like(excess), excess / available_units.clamp_min(1e-12))
    additional = available * fraction[pg]
    if whole_contracts:
        additional = torch.ceil(additional).long()
    additional = torch.minimum(available, additional)
    selected = selected - selected.sign() * additional
    remaining = torch.zeros_like(units).scatter_add(0, pg, selected.abs() * units)
    return selected, remaining > limits, (additional != 0).any()


@lru_cache(maxsize=1)
def _compiled_margin_position_projection():
    # Fuse the detached integer projection only. Autograd's accumulation order
    # in the continuous relaxation remains the original eager implementation.
    return torch.compile(
        _project_margin_position_limits_impl, fullgraph=True, dynamic=False,
        options={"triton.cudagraphs": False, "emulate_precision_casts": True,
                 "eager_numerics.division_rounding": True,
                 "eager_numerics.disable_ftz": True, "force_same_precision": True},
    )


def _project_margin_position_limits(proposed, previous, **kwargs):
    function = _project_margin_position_limits_impl
    if (kwargs['whole_contracts'] and proposed.device.type == 'cuda'
            and _env_truthy('STOCKAGENT_FUTURES_FUNDING_COMPILE', '0')
            and _env_truthy('STOCKAGENT_FUTURES_POSITION_COMPILE', '1')
            and not torch.compiler.is_compiling()):
        function = _compiled_margin_position_projection()
    return function(proposed, previous, **kwargs)


def _grandfather_position_limit(proposed, previous, groups, units, limits, permission):
    """Keep only reductions while already above a dated lower limit.

    No rolling to another month, sign flip or new slot can consume the old
    holding permission. Old inventory expires through the ordinary terminal
    ledger. Restricting additions until the next account decision is explicit
    and conservative; it does not model intradecision sell-then-buy sequences.
    """
    held=torch.zeros_like(limits).scatter_add(0,groups,previous.abs()*units)
    allowed=torch.zeros_like(limits).scatter_reduce(0,groups,permission,reduce='amax')>.5
    legacy=allowed & (held>limits)
    reducing=torch.where(proposed*previous>0,
        previous.sign()*torch.minimum(previous.abs(),proposed.abs()),torch.zeros_like(proposed))
    return torch.where(legacy[groups],reducing,proposed),torch.where(legacy,held,limits)


def _positive_inventory_half_slope(quantity: torch.Tensor) -> torch.Tensor:
    """Split one net position without doubling its tangent at zero.

    clamp_min has slope one at zero. Using it for both q+ and q- makes
    d(q+ - q-)/dq equal two, so an unconstrained zero-position carry can
    amplify its gradient as 2**days. The symmetric half slope preserves the
    exact forward inventory and makes their recombination the identity.
    """
    if not quantity.is_floating_point():
        return quantity.clamp_min(0)
    return torch.where(quantity == 0, quantity * 0.5, quantity.clamp_min(0))


def _project_margin_position_limit_axes(proposed, previous, *, execution_row,
                                       position_group, position_units, group_limits,
                                       close_capacity, whole_contracts):
    """Apply schema-5 same-direction caps on independent long/short inventories.

    Splitting the two directions does not create a second tradable position:
    the request was already bounded by the physical net order's capacity and
    each physical slot has only one signed ending quantity.
    """
    if execution_row.shape[-1] < margin.MARGIN_GRANDFATHER_EXECUTION_WIDTH:
        return _project_margin_position_limit_axes_impl(proposed,previous,
            execution_row=execution_row,position_group=position_group,
            position_units=position_units,group_limits=group_limits,
            close_capacity=close_capacity,whole_contracts=whole_contracts)
    n=len(previous)
    second_group=execution_row[:,margin.SECOND_POSITION_GROUP].nan_to_num().long().clamp(0,n-1)
    second_limits=torch.zeros_like(group_limits).scatter_reduce(0,second_group,
        execution_row[:,margin.SECOND_POSITION_LIMIT].nan_to_num(),reduce='amax')
    for groups,units,limits,permission in (
        (position_group,position_units,group_limits,execution_row[:,margin.POSITION_GRANDFATHER]),
        (second_group,execution_row[:,margin.SECOND_POSITION_UNIT],second_limits,
         execution_row[:,margin.SECOND_POSITION_GRANDFATHER])):
        long_used=torch.zeros_like(limits).scatter_add(0,groups,previous.clamp_min(0)*units)
        short_used=torch.zeros_like(limits).scatter_add(0,groups,(-previous).clamp_min(0)*units)
        permitted=torch.zeros_like(limits).scatter_reduce(0,groups,permission,reduce='amax')>.5
        locked=permitted & (torch.maximum(long_used,short_used)>limits)
        reduction=torch.where(proposed*previous>0,
            previous.sign()*torch.minimum(previous.abs(),proposed.abs()),torch.zeros_like(proposed))
        proposed=torch.where(locked[groups],reduction,proposed)
    ex=torch.cat((execution_row,execution_row),dim=0).clone()
    ex[n:,margin.SECOND_POSITION_GROUP]+=n
    q,failed,reduced=_project_margin_position_limit_axes_impl(
        torch.cat((_positive_inventory_half_slope(proposed),
                   _positive_inventory_half_slope(-proposed))),
        torch.cat((_positive_inventory_half_slope(previous),
                   _positive_inventory_half_slope(-previous))),
        execution_row=ex,position_group=torch.cat((position_group,position_group+n)),
        position_units=position_units.repeat(2),group_limits=group_limits.repeat(2),
        close_capacity=close_capacity.repeat(2),whole_contracts=whole_contracts)
    return q[:n]-q[n:],failed[:n]|failed[n:],reduced


def _project_margin_position_limit_axes_impl(proposed, previous, *, execution_row,
                                       position_group, position_units, group_limits,
                                       close_capacity, whole_contracts):
    """Intersect dated monthly and aggregate caps without doubling liquidity.

    Each projection preserves feasible reductions and only shrinks increases.
    Both use the same original inventory and cumulative close capacity, so a
    second constraint cannot spend the same close volume twice. Recheck both
    axes at the end: the second can also resolve a first-axis violation.
    """
    grandfather=execution_row.shape[-1] >= margin.MARGIN_GRANDFATHER_EXECUTION_WIDTH
    if grandfather:
        proposed,group_limits=_grandfather_position_limit(proposed,previous,position_group,
            position_units,group_limits,execution_row[:,margin.POSITION_GRANDFATHER])
    selected, failed, reduced = _project_margin_position_limits(
        proposed, previous, position_group=position_group, position_units=position_units,
        group_limits=group_limits, close_capacity=close_capacity, whole_contracts=whole_contracts)
    if execution_row.shape[-1] < margin.MARGIN_MULTI_LIMIT_EXECUTION_WIDTH:
        return selected, failed[position_group], reduced
    second_group=execution_row[:,margin.SECOND_POSITION_GROUP].nan_to_num().long().clamp(0,len(previous)-1)
    second_units=execution_row[:,margin.SECOND_POSITION_UNIT].nan_to_num()
    second_limits=torch.zeros_like(group_limits).scatter_reduce(0,second_group,
        execution_row[:,margin.SECOND_POSITION_LIMIT].nan_to_num(),reduce='amax')
    if grandfather:
        selected,second_limits=_grandfather_position_limit(selected,previous,second_group,
            second_units,second_limits,execution_row[:,margin.SECOND_POSITION_GRANDFATHER])
    selected, _, reduced_second = _project_margin_position_limits(
        selected, previous, position_group=second_group, position_units=second_units,
        group_limits=second_limits, close_capacity=close_capacity, whole_contracts=whole_contracts)
    used=torch.zeros_like(group_limits).scatter_add(0,position_group,selected.abs()*position_units)
    used_second=torch.zeros_like(second_limits).scatter_add(0,second_group,selected.abs()*second_units)
    failed_slots=(used>group_limits)[position_group] | (used_second>second_limits)[second_group]
    return selected, failed_slots, reduced | reduced_second


def _margin_position_slack(held,previous,groups,units,limits,permission=None):
    if permission is None:
        used=torch.zeros_like(limits).scatter_add(0,groups,held.abs()*units)
        return torch.where(limits>0,1.-used/limits.clamp_min(1e-12),
                           torch.full_like(limits,float('inf'))).min()
    sides=[]
    for sign in (1.,-1.):
        inventory=(sign*held).clamp_min(0)
        _,allowed=_grandfather_position_limit(inventory,(sign*previous).clamp_min(0),
                                               groups,units,limits,permission)
        sides.append(_margin_position_slack(inventory,previous,groups,units,allowed))
    return torch.minimum(*sides)


def _transfer_margin_inventory(quantities: torch.Tensor, execution: torch.Tensor,
                               *, whole_contracts: bool):
    """Apply a dated legal transfer without a trade, fee, or invented position.

    The map is destination -> old slot, so a renamed contract can coexist with
    a freshly listed standard contract. A rational split cannot round away
    residual contracts. Missing/duplicated old inventory fails the account.
    """
    slots = quantities.shape[0]
    raw = execution[:, margin.CARRY_SOURCE_SLOT]
    valid_index = torch.isfinite(raw) & (raw == raw.round()) & (raw >= 0) & (raw <= slots)
    incoming = valid_index & (raw > 0)
    origin = (raw.nan_to_num().long() - 1).clamp(0, slots - 1)
    num = execution[:, margin.CARRY_QUANTITY_NUMERATOR]
    den = execution[:, margin.CARRY_QUANTITY_DENOMINATOR]
    valid_ratio = (torch.isfinite(num) & torch.isfinite(den)
                   & (num > 0) & (den > 0) & (num <= 1_000_000) & (den <= 1_000_000)
                   & (num == num.round()) & (den == den.round()))
    cash = execution[:, margin.CARRY_CASH]
    counts = torch.zeros(slots, device=quantities.device, dtype=torch.int64).scatter_add(
        0, origin, incoming.long())
    source = torch.where(incoming, quantities.gather(0, origin), torch.zeros_like(quantities))
    metadata_ok = (valid_index & (~incoming | (valid_ratio & torch.isfinite(cash)))).all()
    metadata_ok &= ((quantities == 0) | (counts == 1)).all() & (counts <= 1).all()
    safe_num = torch.where(valid_ratio, num, 1.).long()
    safe_den = torch.where(valid_ratio, den, 1.).long()
    if whole_contracts:
        scaled = source * safe_num
        metadata_ok &= ((scaled % safe_den) == 0).all()
        moved = torch.div(scaled, safe_den, rounding_mode="trunc")
    else:
        moved = source * safe_num / safe_den
    cash_flow = (source.to(execution.dtype) * torch.where(incoming, cash, 0.).nan_to_num()).sum()
    return moved, cash_flow, metadata_ok


def _margin_physical_backward(
    weights: torch.Tensor,
    execution: torch.Tensor,
    *,
    initial_capital: float,
    initial_quantities: torch.Tensor,
    advance: torch.Tensor,
    active_metadata: torch.Tensor,
    trace: list[tuple[torch.Tensor, ...]],
    recover: bool,
    return_weights_history: bool,
    return_turnovers: bool,
) -> FuturesPortfolioTensorResult:
    """Linearize the executed physical account, never a fungible group book.

    Basket selection is discrete. Its local STE distributes a group's target
    over its actually selected denominations; at cash it uses the cheapest
    legally tradable denomination (equal ties). Every fill/carry/mark is then
    anchored to the exact trace. Only discrete basket/lot selection uses an
    STE: capacity and position constraints retain their feasible derivative.
    Mandatory closes retain the residual-position derivative. A denomination
    with no permitted capacity cannot close an untradeable physical contract.
    A default is absorbing in both paths: recovery differentiates the failed
    boundary while it can still credit prior actions, never a re-funded future.
    This is an explicit biased estimator, not an integer function derivative.
    """
    capital = float(initial_capital)
    slots = weights.shape[1]
    q = initial_quantities.to(weights.dtype).round()
    position_groups = execution[..., margin.POSITION_GROUP].nan_to_num().long().clamp(0, slots - 1)
    position_limits = torch.zeros_like(weights).scatter_reduce(
        1, position_groups, execution[..., margin.POSITION_LIMIT].nan_to_num(), reduce='amax')
    # Immutable physical tape fields have no recurrent dependencies. Prepare
    # their exact pointwise values once, rather than launch the same kernels
    # independently on every date of the captured recurrence.
    initial_margins = torch.where(active_metadata, execution[..., margin.INITIAL], 1.)
    groups = execution[..., 9].round().long().clamp(0, slots - 1)
    gaps = torch.where(active_metadata, execution[..., 3] - execution[..., margin.PREVIOUS_MARK], 0.).nan_to_num()
    moves = torch.where(active_metadata, execution[..., 4] - execution[..., 3], 0.).nan_to_num()
    terminal_moves = torch.where(active_metadata, execution[..., margin.TERMINAL_MARK] - execution[..., 4], 0.).nan_to_num()
    entry_fees = (execution[..., 5] + execution[..., 6]).nan_to_num()
    exit_fees = (execution[..., 5] + execution[..., 7]).nan_to_num()
    capacities = execution[..., 8].clamp_min(0.).floor()
    position_units = execution[..., margin.POSITION_UNIT].nan_to_num()
    terminal_capacities = execution[..., margin.TERMINAL_CAPACITY].nan_to_num()
    reserves = initial_margins + 2 * entry_fees
    nav = trace[0][0]
    returns, turnovers, histories, equities = [], [], [], []
    for row, (start_nav, start_alive, chosen, closed, next_q, next_nav, force_close, position_failed) in enumerate(trace):
        x = execution[row]
        active = active_metadata[row]
        im = initial_margins[row]
        group = groups[row]
        do_row = advance[row] & start_alive
        # Retain the self-financing derivative until (and including) default.
        # Later returns are identically zero in this episode; resurrecting the
        # shadow would optimize unavailable future profits against survival.
        # The positive placeholder only makes inactive arithmetic well-defined.
        nav = torch.where(start_alive, nav + (start_nav - nav).detach(), weights.new_full((), capital))
        q = torch.where(start_alive, q, torch.zeros_like(q))
        denominator = nav.clamp_min(1.0e-12)
        event_cash = weights.new_zeros(())
        if execution.shape[-1] >= margin.MARGIN_CORPORATE_EXECUTION_WIDTH:
            moved, transferred_cash, _ = _transfer_margin_inventory(q, x, whole_contracts=False)
            q = torch.where(do_row, moved, q)
            event_cash = torch.where(do_row, transferred_cash, event_cash)
        gap = (q * gaps[row]).sum()
        marked_nav = nav + event_cash + gap
        allocation_nav = torch.minimum(nav, marked_nav).clamp_min(0.)
        requested = torch.zeros_like(q).scatter_add(0, group, torch.where(active, weights[row], 0.))
        mandatory_failed = force_close | position_failed
        requested = torch.where(mandatory_failed, torch.zeros_like(requested), requested)
        capacity = torch.where(active & (x[:, 1] > .5) & do_row, capacities[row], 0.)
        buy_cap = torch.where(x[:, margin.CAN_BUY] > .5, capacity, 0.)
        sell_cap = torch.where(x[:, margin.CAN_SELL] > .5, capacity, 0.)

        allocated = chosen.abs().to(weights.dtype) * im
        group_allocated = torch.zeros_like(q).scatter_add(0, group, allocated)
        sign_request = requested.gather(0, group)
        can_enter = torch.where(sign_request > 0, buy_cap > 0,
                               torch.where(sign_request < 0, sell_cap > 0, (buy_cap + sell_cap) > 0))
        reserve = reserves[row]
        cost = torch.where(can_enter, reserve, torch.full_like(q, float('inf')))
        cheapest = torch.full_like(q, float('inf')).scatter_reduce(0, group, cost, reduce='amin')
        fallback = (can_enter & (cost == cheapest.gather(0, group))).to(weights.dtype)
        count = torch.zeros_like(q).scatter_add(0, group, fallback).gather(0, group)
        share = torch.where(group_allocated.gather(0, group) > 0,
                            allocated / group_allocated.gather(0, group).clamp_min(1.0e-12),
                            fallback / count.clamp_min(1.))
        desired = sign_request * allocation_nav * share / im
        delta = desired - q
        # At an identically forbidden interval PyTorch min/max tie derivatives
        # are not an executable direction; explicitly give it zero sensitivity.
        bounded = torch.minimum(torch.maximum(delta, -sell_cap), buy_cap)
        bounded = torch.where((buy_cap + sell_cap) > 0, bounded, torch.zeros_like(bounded))
        proposed = q + bounded
        proposed, _, _ = _project_margin_position_limit_axes(
            proposed, q, execution_row=x, position_group=position_groups[row],
            position_units=position_units[row],
            group_limits=position_limits[row],
            close_capacity=torch.where(q < 0, buy_cap, sell_cap),
            whole_contracts=False,
        )
        held = proposed + (chosen.to(weights.dtype) - proposed).detach()
        traded = held - q
        entry_cost = (_absolute_cost_with_intent(traded, desired - q) * entry_fees[row]).sum()
        terminal_capacity = (terminal_capacities[row] - traded.abs()).clamp_min(0.).floor()
        terminal_allowed = torch.where(held < 0, x[:, margin.TERMINAL_CAN_BUY] > .5,
                                       x[:, margin.TERMINAL_CAN_SELL] > .5)
        closable = torch.where(terminal_allowed, torch.minimum(held.abs(), terminal_capacity), 0.)
        close_proposal = torch.where(x[:, margin.CASH_SETTLEMENT] > .5, held,
                                     held.sign() * closable)
        close_proposal = torch.where((x[:, 2] > .5) & do_row, close_proposal, 0.)
        closing = close_proposal + (closed.to(weights.dtype) - close_proposal).detach()
        exit_cost = (_absolute_cost_with_intent(closing, desired) * exit_fees[row]).sum()
        end_nav = marked_nav - entry_cost - exit_cost + (
            held * moves[row] + closing * terminal_moves[row]
        ).sum()
        wealth = end_nav / denominator
        if recover:
            # A capacity-limited risk reduction carries its actual residual.
            # Its return (and gradient) is the retained position's marked P&L;
            # an unfilled order is neither a fee nor economic insolvency.
            # Terminal delivery and dated position permissions remain separate
            # obligations; they cannot be relaxed into an invented fill.
            dated_hold=execution.shape[-1]>=margin.MARGIN_GRANDFATHER_EXECUTION_WIDTH
            position_slack=_margin_position_slack(held,q,position_groups[row],position_units[row],
                position_limits[row],x[:,margin.POSITION_GRANDFATHER] if dated_hold else None)
            if execution.shape[-1] >= margin.MARGIN_MULTI_LIMIT_EXECUTION_WIDTH:
                second_group=x[:,margin.SECOND_POSITION_GROUP].nan_to_num().long().clamp(0,slots-1)
                second_limits=torch.zeros_like(q).scatter_reduce(0,second_group,
                    x[:,margin.SECOND_POSITION_LIMIT].nan_to_num(),reduce='amax')
                second_slack=_margin_position_slack(held,q,second_group,
                    x[:,margin.SECOND_POSITION_UNIT].nan_to_num(),second_limits,
                    x[:,margin.SECOND_POSITION_GRANDFATHER] if dated_hold else None)
                position_slack=torch.minimum(position_slack,second_slack)
            wealth = torch.where(position_failed, position_slack, wealth)
            terminal_obligation = (x[:, 2] > .5) & do_row
            terminal_failed = (terminal_obligation & (chosen != closed)).any()
            # Only the unfulfilled terminal contracts own this boundary.
            # Ordinary overnight inventory is neither delivery debt nor an
            # extra penalty when a different physical contract cannot close.
            residual = torch.where(terminal_obligation, held - closing, 0.)
            wealth = torch.where(terminal_failed, -(residual.abs() * im).sum() / denominator, wealth)
            soft_wealth = F.softplus(torch.nan_to_num(wealth, nan=-1., posinf=3., neginf=-1.) / .10) * .10 + 1.0e-7
            log_return = soft_wealth.log()
        else:
            log_return = wealth.clamp_min(1.0e-7).log()
        returns.append(torch.where(do_row, log_return, 0.))
        turnovers.append(torch.where(do_row, (traded.abs() * x[:, 3].nan_to_num()
                         + closing.abs() * x[:, 4].nan_to_num()).sum() / denominator, 0.)
                         if return_turnovers else weights.new_zeros(()))
        if return_weights_history:
            histories.append(held * torch.where(active, x[:, 3], 0.) / marked_nav.clamp_min(1.0e-12))
        q = held - closing
        q = q + (next_q.to(weights.dtype) - q).detach()
        nav = end_nav + (next_nav - end_nav).detach()
        equities.append(nav / capital)
    return FuturesPortfolioTensorResult(
        strategy_returns=torch.stack(returns), turnovers=torch.stack(turnovers),
        weights_history=torch.stack(histories) if histories else weights.new_empty((0, slots)),
        final_weights=q, final_alive=trace[-1][5] > 0,
        equity_scale_history=torch.stack(equities), final_equity_scale=nav / capital,
    )


def run_tw_futures_portfolio_integer_surrogate_torch(
    target_weights: torch.Tensor,
    integer_execution: torch.Tensor,
    *,
    initial_capital: float,
    state_advance_mask: torch.Tensor | None = None,
    initial_quantities: torch.Tensor | None = None,
    initial_weights: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
    return_turnovers: bool = True,
    recover_after_default_for_backward: bool = False,
    _detach_initial_weights: bool = True,
) -> FuturesPortfolioTensorResult:
    """Differentiable grouped relaxation for the exact integer account.

    Integer contract choice is piecewise constant and therefore has a zero
    derivative almost everywhere.  Training needs an explicit relaxation, not
    an accidental gradient through ``argmin`` and integer casts.  This kernel
    aggregates every standard/mini basket to the executor's exposure group,
    moves the prior group position toward the requested group target subject
    to causal contract-volume capacity, charges fractional-contract fee/tax
    rates, and carries the resulting fully-collateralized notional state.

    A straight-through floor maps each group request to whole units of its
    cheapest causally tradable contract.  Its forward value is zero below one
    contract and an integer number of contract-cash units above it, while its
    backward derivative remains continuous.  Training therefore cannot earn
    returns from thousands of fractional positions that exact execution must
    leave in cash.

    The exact executor may use this result only for its backward path.  During
    training it can also be selected directly, with continuous ``initial_weights``
    carried between batches; exact integer execution remains authoritative for
    validation, test, reports, and deployment artifacts.

    Dated-margin exact training uses ``_margin_physical_backward`` instead;
    this unanchored grouped model remains a legacy research relaxation.
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError("integer futures target_weights must have shape [T,S]")
    if (
        integer_execution.ndim != 3
        or tuple(integer_execution.shape[:2]) != tuple(target_weights.shape)
        or int(integer_execution.size(-1)) not in {11, *margin.MARGIN_EXECUTION_WIDTHS}
    ):
        raise ValueError("integer_execution must have shape [T,S,11]")
    capital = float(initial_capital)
    if not math.isfinite(capital) or capital <= 0.0:
        raise ValueError("initial_capital must be finite and positive")

    weights = torch.nan_to_num(
        target_weights.to(dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    execution = integer_execution.to(device=weights.device, dtype=torch.float32)
    margin_mode = int(execution.size(-1)) in margin.MARGIN_EXECUTION_WIDTHS
    if execution.size(-1) >= margin.MARGIN_CORPORATE_EXECUTION_WIDTH:
        raise ValueError("corporate margin carry requires the physical exact account and its anchored backward")
    rows, slots = tuple(weights.shape)
    advance = (
        torch.ones((rows,), device=weights.device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=weights.device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (rows,):
        raise ValueError("state_advance_mask must have shape [T]")

    holding_log_returns = execution[..., 0]
    executable = execution[..., 1] > 0.5
    must_liquidate = execution[..., 2] > 0.5
    opening_notional = execution[..., 3]
    ending_notional = execution[..., 4]
    collateral = execution[..., margin.INITIAL] if margin_mode else opening_notional
    fixed_fee = execution[..., 5]
    opening_tax = execution[..., 6]
    ending_tax = execution[..., 7]
    maximum_trade = torch.floor(execution[..., 8].clamp_min(0.0))
    group_index = torch.round(execution[..., 9]).to(torch.int64).clamp(0, slots - 1)
    active = (
        torch.isfinite(holding_log_returns)
        & torch.isfinite(opening_notional)
        & (opening_notional > 0.0)
        & torch.isfinite(ending_notional)
        & (ending_notional > 0.0)
        & torch.isfinite(fixed_fee)
        & (fixed_fee >= 0.0)
        & torch.isfinite(opening_tax)
        & (opening_tax >= 0.0)
        & torch.isfinite(ending_tax)
        & (ending_tax >= 0.0)
    )
    simple_asset_returns = torch.where(
        active,
        ((ending_notional - opening_notional) / collateral.clamp_min(1.0e-12)
         if margin_mode else ending_notional / opening_notional.clamp_min(1.0e-12) - 1.0),
        torch.zeros_like(opening_notional),
    )
    if margin_mode:
        active &= torch.isfinite(collateral) & (collateral > 0)
        # Cash-settled expiry is valued at the official final mark, which can
        # differ from the ordinary daily settlement. The backward payoff must
        # price that same endpoint; it must not learn from a discarded mark.
        cash_settled = must_liquidate & (execution[..., margin.CASH_SETTLEMENT] > 0.5)
        simple_asset_returns = torch.where(
            cash_settled,
            (execution[..., margin.TERMINAL_MARK] - opening_notional)
            / collateral.clamp_min(1.0e-12),
            simple_asset_returns,
        )
        simple_asset_returns = torch.where(active, simple_asset_returns, torch.zeros_like(simple_asset_returns))
    active_f = active.to(dtype=weights.dtype)
    group_count = torch.zeros_like(weights).scatter_add(1, group_index, active_f)
    requested_group = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        torch.where(active & advance[:, None], weights, torch.zeros_like(weights)),
    )
    group_simple_return = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        simple_asset_returns * active_f,
    ) / group_count.clamp_min(1.0)
    group_must_liquidate = (
        torch.zeros_like(group_index).scatter_add(
            1,
            group_index,
            (must_liquidate & active & advance[:, None]).to(torch.int64),
        )
        > 0
    )
    can_trade = (
        active
        & executable
        & advance[:, None]
        & (maximum_trade > 0.0)
    )
    slot_capacity_cash = torch.where(
        can_trade,
        maximum_trade * collateral,
        torch.zeros_like(opening_notional),
    )
    group_capacity_cash = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        slot_capacity_cash,
    )
    if margin_mode:
        group_buy_capacity_cash = torch.zeros_like(weights).scatter_add(
            1, group_index, slot_capacity_cash * (execution[..., margin.CAN_BUY] > 0.5))
        group_sell_capacity_cash = torch.zeros_like(weights).scatter_add(
            1, group_index, slot_capacity_cash * (execution[..., margin.CAN_SELL] > 0.5))
    entry_cost_rate = torch.where(
        active,
        (fixed_fee + opening_tax) / collateral.clamp_min(1.0e-12),
        torch.zeros_like(opening_notional),
    )
    exit_cost_rate = torch.where(
        active,
        (fixed_fee + ending_tax) / collateral.clamp_min(1.0e-12),
        torch.zeros_like(opening_notional),
    )
    group_entry_cost_rate = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        entry_cost_rate * active_f,
    ) / group_count.clamp_min(1.0)
    group_exit_cost_rate = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        exit_cost_rate * active_f,
    ) / group_count.clamp_min(1.0)
    new_contract_cash = torch.where(
        can_trade,
        collateral
        + (2.0 * fixed_fee)
        + (2.0 * opening_tax),
        torch.full_like(opening_notional, float("inf")),
    )
    group_minimum_contract_cash = torch.full_like(
        weights,
        float("inf"),
    ).scatter_reduce(
        1,
        group_index,
        new_contract_cash,
        reduce="amin",
        include_self=True,
    )

    supplied_alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.to(device=weights.device, dtype=torch.bool).reshape(())
    )
    starting_scale = (
        weights.new_ones(())
        if initial_equity_scale is None
        else initial_equity_scale.to(
            device=weights.device,
            dtype=weights.dtype,
        ).reshape(())
    )
    if recover_after_default_for_backward:
        valid_start = (
            supplied_alive
            & torch.isfinite(starting_scale)
            & (starting_scale > 0.0)
        )
        starting_scale = torch.where(
            valid_start,
            starting_scale,
            weights.new_ones(()),
        )
    equity = weights.new_full((), capital) * starting_scale
    alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if recover_after_default_for_backward
        else supplied_alive
    )
    if initial_quantities is not None and initial_weights is not None:
        raise ValueError("provide initial_quantities or initial_weights, not both")
    if initial_weights is not None:
        carried_initial_weights = initial_weights.to(
            device=weights.device,
            dtype=weights.dtype,
        )
        if _detach_initial_weights:
            carried_initial_weights = carried_initial_weights.detach()
        previous_slot_weights = torch.nan_to_num(
            carried_initial_weights,
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        )
        if tuple(previous_slot_weights.shape) != (slots,):
            raise ValueError("initial_weights must have shape [S]")
    else:
        initial_q = (
            torch.zeros((slots,), device=weights.device, dtype=torch.int64)
            if initial_quantities is None
            else torch.round(
                initial_quantities.detach().to(
                    device=weights.device,
                    dtype=torch.float32,
                )
            ).to(torch.int64)
        )
        if tuple(initial_q.shape) != (slots,):
            raise ValueError("initial_quantities must have shape [S]")
        first_active = active[0]
        previous_slot_weights = (
            initial_q.to(weights.dtype)
            * torch.where(
                first_active,
                (execution[0, :, margin.PREVIOUS_INITIAL].nan_to_num()
                 if margin_mode else opening_notional[0]),
                torch.zeros_like(opening_notional[0]),
            )
            / equity.detach().clamp_min(1.0e-12)
        )

    return_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    equity_rows: list[torch.Tensor] = []
    for row in range(rows):
        row_advances = (
            advance[row]
            if recover_after_default_for_backward
            else advance[row] & alive
        )
        gap_simple = weights.new_zeros(())
        allocation_scale = weights.new_ones(())
        force_margin_close = torch.zeros((), device=weights.device, dtype=torch.bool)
        if margin_mode:
            previous_collateral = execution[row, :, margin.PREVIOUS_INITIAL]
            prior_valid = active[row] & torch.isfinite(previous_collateral) & (previous_collateral > 0)
            gap_return = torch.where(prior_valid,
                (opening_notional[row] - execution[row, :, margin.PREVIOUS_MARK])
                / previous_collateral.clamp_min(1e-12), torch.zeros_like(previous_collateral))
            gap_simple = (previous_slot_weights * gap_return.nan_to_num()).sum() * row_advances
            # All shadow weights/PnL below use the row's starting NAV. Rebase
            # new margin requests onto min(start NAV, marked opening NAV), as
            # the exact account does. Gains cannot finance opening orders;
            # losses must reduce their funding. Do not detach the shadow's
            # overnight sensitivity inside a truncated-BPTT batch.
            allocation_scale = (1.0 + gap_simple).clamp(min=0.0, max=1.0)
            previous_maintenance = (previous_slot_weights.abs() * torch.where(
                prior_valid,
                execution[row, :, margin.PREVIOUS_MAINTENANCE]
                / previous_collateral.clamp_min(1e-12),
                torch.zeros_like(previous_collateral))).sum()
            rescaled_prior = previous_slot_weights * torch.where(
                prior_valid, collateral[row] / previous_collateral.clamp_min(1e-12),
                torch.ones_like(previous_collateral))
            previous_slot_weights = torch.where(row_advances, rescaled_prior, previous_slot_weights)
            force_margin_close = row_advances & (
                (previous_maintenance > 1.0)
                | ((1.0 + gap_simple) < rescaled_prior.abs().sum()
                   * execution[row, :, margin.LIQUIDATION_RATIO].max()))
        current_group = torch.zeros_like(previous_slot_weights).scatter_add(
            0,
            group_index[row],
            torch.where(
                active[row],
                previous_slot_weights,
                torch.zeros_like(previous_slot_weights),
            ),
        )
        capacity_weight = (
            group_capacity_cash[row] / equity.detach().clamp_min(1.0e-12)
        )
        raw_request = requested_group[row]
        if margin_mode:
            raw_request = torch.where(force_margin_close, torch.zeros_like(raw_request),
                                      raw_request * allocation_scale)
        minimum_cash = group_minimum_contract_cash[row]
        valid_minimum = torch.isfinite(minimum_cash) & (minimum_cash > 0.0)
        safe_minimum = torch.where(
            valid_minimum,
            minimum_cash,
            torch.ones_like(minimum_cash),
        )
        requested_contract_units = torch.where(
            valid_minimum,
            raw_request.abs() * equity.detach().clamp_min(0.0) / safe_minimum,
            torch.zeros_like(raw_request),
        )
        hard_contract_units = torch.floor(requested_contract_units)
        straight_through_units = requested_contract_units + (
            hard_contract_units - requested_contract_units
        ).detach()
        quantized_request_abs = (
            straight_through_units
            * safe_minimum
            / equity.detach().clamp_min(1.0e-12)
        )
        hard_signed_request = torch.sign(raw_request) * quantized_request_abs
        # Differentiate the signed request directly. sign(w) * STE(abs(w))
        # loses every derivative at w == 0 and traps a trainable cash-reset
        # head there permanently. The identity STE has no directional bias;
        # only the observed return supplies the signed first-order signal.
        executable_request = raw_request + (
            hard_signed_request - raw_request
        ).detach()
        requested_delta = executable_request - current_group
        if margin_mode:
            capacity_weight = torch.where(
                requested_delta < 0, group_sell_capacity_cash[row], group_buy_capacity_cash[row]
            ) / equity.detach().clamp_min(1.0e-12)
        capacity_enabled_delta = torch.where(
            capacity_weight > 0.0,
            requested_delta,
            torch.zeros_like(requested_delta),
        )
        hard_bounded_delta = torch.sign(capacity_enabled_delta) * torch.minimum(
            capacity_enabled_delta.abs(),
            capacity_weight,
        )
        bounded_delta = capacity_enabled_delta + (
            hard_bounded_delta - capacity_enabled_delta
        ).detach()
        proposed_group = current_group + torch.where(
            row_advances,
            bounded_delta,
            torch.zeros_like(bounded_delta),
        )

        # A locked position can coexist with a new request and otherwise push
        # gross above cash.  Fund reductions first, then scale only exposure
        # increases into the remaining cash; no failed sleeve is reassigned to
        # another group.
        current_abs = current_group.abs()
        proposed_abs = proposed_group.abs()
        reductions = (current_abs - proposed_abs).clamp_min(0.0)
        increases = (proposed_abs - current_abs).clamp_min(0.0)
        gross_after_reductions = (
            current_abs.sum() - reductions.sum()
        ).clamp_min(0.0)
        available_increase = (allocation_scale - gross_after_reductions).clamp_min(0.0)
        increase_scale = torch.minimum(
            torch.ones_like(available_increase),
            available_increase / increases.sum().clamp_min(1.0e-12),
        )
        funded_abs = torch.where(
            proposed_abs > current_abs,
            current_abs + increases * increase_scale,
            proposed_abs,
        )
        funded_group = torch.sign(proposed_group) * funded_abs
        # Funding is a hard account constraint in the forward path. Preserve
        # the proposal derivative through its zero/fully-funded boundary so a
        # sub-contract request that floors to cash can still learn toward an
        # executable whole contract.
        held_group = proposed_group + (funded_group - proposed_group).detach()
        group_delta = held_group - current_group

        gross_simple = (
            held_group * group_simple_return[row]
        ).sum() * row_advances.to(weights.dtype)
        cost_delta_abs = _absolute_cost_with_intent(
            group_delta, raw_request - current_group,
        )
        cost_held_abs = _absolute_cost_with_intent(held_group, raw_request)
        entry_cost = (cost_delta_abs * group_entry_cost_rate[row]).sum()
        close_cost = torch.where(
            group_must_liquidate[row] & row_advances,
            cost_held_abs * group_exit_cost_rate[row],
            torch.zeros_like(held_group),
        ).sum()
        net_simple = gross_simple - entry_cost - close_cost + gap_simple
        row_survived = ~advance[row] | (
            torch.isfinite(net_simple) & (net_simple > -1.0)
        )
        survived = row_survived if recover_after_default_for_backward else (
            alive & row_survived
        )
        safe_net = torch.where(
            survived,
            net_simple,
            torch.full_like(net_simple, -1.0 + 1.0e-7),
        )
        if recover_after_default_for_backward:
            # Exact forward ruin remains absorbing in the integer executor. This
            # shadow account exists only in backward: use a smooth positive
            # wealth map at/through the -100% boundary, while preserving the
            # hard ruin sentinel as the forward value. Resetting the shadow
            # account after ruin lets later batches provide a learning signal
            # instead of turning every subsequent optimizer step into zero.
            finite_net = torch.nan_to_num(
                net_simple,
                nan=-2.0,
                posinf=2.0,
                neginf=-2.0,
            )
            soft_wealth = F.softplus((1.0 + finite_net) / 0.10) * 0.10 + 1.0e-7
            hard_log_return = torch.log1p(safe_net)
            soft_log_return = torch.log(soft_wealth)
            recoverable_log_return = soft_log_return + (
                hard_log_return - soft_log_return
            ).detach()

            # This zero-forward barrier is the differentiable form of the exact
            # account's full-notional-plus-cost funding test. It does not add a
            # top-K, target leverage, or portfolio redistribution heuristic.
            funding_required = (
                held_group.abs().sum()
                + entry_cost
                + (cost_held_abs * group_exit_cost_rate[row]).sum()
            )
            funding_excess = funding_required - allocation_scale
            # The constraint is inactive for a funded account. A softplus
            # barrier has a nonzero slope even far below the funding limit:
            # at 75% gross it can reverse a profitable, fee-adjusted utility
            # gradient. Penalize only actual shadow funding excess; preserve
            # the exact forward ledger and recovery after a genuine breach.
            funding_barrier = F.relu(funding_excess)
            recoverable_log_return = recoverable_log_return - (
                funding_barrier - funding_barrier.detach()
            )
            log_return = torch.where(
                row_advances,
                recoverable_log_return,
                torch.zeros_like(recoverable_log_return),
            )
        else:
            log_return = torch.where(
                row_advances,
                torch.log1p(safe_net),
                torch.zeros_like(safe_net),
            )
        if return_turnovers:
            forced_close_turnover = torch.where(
                group_must_liquidate[row] & row_advances,
                (held_group * (1.0 + group_simple_return[row])).abs(),
                torch.zeros_like(held_group),
            ).sum()
            turnover = torch.where(
                row_advances,
                group_delta.abs().sum() + forced_close_turnover,
                torch.zeros_like(net_simple),
            )
        else:
            turnover = torch.zeros_like(net_simple)
        denominator = (1.0 + safe_net).clamp_min(1.0e-7)
        if margin_mode:
            collateral_change = torch.where(active[row],
                execution[row, :, margin.END_INITIAL] / collateral[row].clamp_min(1e-12),
                torch.zeros_like(collateral[row]))
            group_change = torch.zeros_like(held_group).scatter_add(
                0, group_index[row], collateral_change) / group_count[row].clamp_min(1)
            next_group = held_group * group_change / denominator
        else:
            next_group = held_group * (1.0 + group_simple_return[row]) / denominator
        next_group = torch.where(
            group_must_liquidate[row] & row_advances,
            torch.zeros_like(next_group),
            next_group,
        )
        count_by_slot = group_count[row].gather(0, group_index[row]).clamp_min(1.0)
        held_slot = torch.where(
            active[row],
            held_group.gather(0, group_index[row]) / count_by_slot,
            torch.zeros_like(previous_slot_weights),
        )
        next_slot = torch.where(
            active[row],
            next_group.gather(0, group_index[row]) / count_by_slot,
            torch.zeros_like(previous_slot_weights),
        )
        previous_slot_weights = torch.where(
            advance[row] & survived,
            next_slot,
            previous_slot_weights,
        )
        previous_slot_weights = torch.where(
            survived,
            previous_slot_weights,
            torch.zeros_like(previous_slot_weights),
        )
        equity = torch.where(
            advance[row] & survived,
            equity * (1.0 + safe_net),
            equity,
        )
        if recover_after_default_for_backward:
            previous_slot_weights = torch.where(
                survived,
                previous_slot_weights,
                torch.zeros_like(previous_slot_weights),
            )
            equity = torch.where(
                survived,
                equity,
                weights.new_full((), capital),
            )
            alive = torch.ones_like(alive)
        else:
            alive = survived
        return_rows.append(log_return)
        turnover_rows.append(turnover)
        if return_weights_history:
            weight_rows.append(held_slot)
        equity_rows.append(equity / capital)

    strategy_returns = torch.stack(return_rows)
    turnovers = torch.stack(turnover_rows)
    equity_history = torch.stack(equity_rows)
    history = (
        torch.stack(weight_rows)
        if return_weights_history
        else weights.new_empty((0, slots))
    )
    return FuturesPortfolioTensorResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=history,
        final_weights=previous_slot_weights,
        final_alive=alive,
        equity_scale_history=equity_history,
        final_equity_scale=(
            equity_history[-1] if equity_history.numel() else starting_scale
        ),
    )


def _run_tw_futures_portfolio_integer_torch_impl(
    target_weights: torch.Tensor,
    integer_execution: torch.Tensor,
    *,
    initial_capital: float,
    state_advance_mask: torch.Tensor | None = None,
    initial_quantities: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
    return_turnovers: bool = True,
    return_margin_audit: bool = True,
    recoverable_backward: bool = False,
    _initial_surrogate_weights: torch.Tensor | None = None,
    _initial_surrogate_equity_scale: torch.Tensor | None = None,
    _initial_surrogate_alive: torch.Tensor | None = None,
) -> FuturesPortfolioTensorResult:
    """Run the exact notional-funded or dated-margin futures carrying account.

    The forward account uses signed integer contract quantities.  Standard and
    mini stock/ETF contracts sharing an underlying and delivery month receive
    one aggregate model exposure and are packed together.  Every other slot is
    a singleton group.  Unused sleeve cash is never reassigned to another
    group. The eleven-channel contract reserves full absolute notional; the
    margin contract reserves initial margin for new risk and checks maintenance
    for carried risk. Actual PnL,
    per-side fixed fees, rounded transaction tax, and expiry closes update the
    next session's equity.  The differentiable path is a straight-through
    continuous surrogate; reported quantities and cash are always exact.
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError("integer futures target_weights must have shape [T,S]")
    if (
        integer_execution.ndim != 3
        or tuple(integer_execution.shape[:2]) != tuple(target_weights.shape)
        or int(integer_execution.size(-1)) not in {11, *margin.MARGIN_EXECUTION_WIDTHS}
    ):
        raise ValueError("integer_execution must have shape [T,S,11]")
    capital = float(initial_capital)
    if not math.isfinite(capital) or capital <= 0.0:
        raise ValueError("initial_capital must be finite and positive")

    surrogate_weights = torch.nan_to_num(
        target_weights.to(dtype=torch.float32),
        nan=0.0,
        posinf=0.0,
        neginf=0.0,
    )
    # The exact discrete account must not accidentally expose gradients from
    # basket scoring, argmin, or integer casts.  Its value/state is detached;
    # the explicit grouped relaxation below owns the complete backward path.
    weights = surrogate_weights.detach()
    execution = integer_execution.to(device=weights.device, dtype=torch.float32)
    margin_mode = int(execution.size(-1)) in margin.MARGIN_EXECUTION_WIDTHS
    rows, slots = tuple(weights.shape)
    advance = (
        torch.ones((rows,), device=weights.device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=weights.device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (rows,):
        raise ValueError("state_advance_mask must have shape [T]")
    quantities = (
        torch.zeros((slots,), device=weights.device, dtype=torch.int64)
        if initial_quantities is None
        else torch.round(
            initial_quantities.detach().to(device=weights.device, dtype=torch.float32)
        ).to(torch.int64)
    )
    if tuple(quantities.shape) != (slots,):
        raise ValueError("initial_quantities must have shape [S]")
    alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().to(device=weights.device, dtype=torch.bool).reshape(())
    )
    starting_scale = (
        weights.new_ones(())
        if initial_equity_scale is None
        else initial_equity_scale.detach().to(
            device=weights.device,
            dtype=weights.dtype,
        ).reshape(())
    )
    equity = weights.new_full((), capital) * starting_scale

    # Every field below is exogenous execution metadata.  Grouping it inside
    # the recurrent loop repeated seven 1,936-slot scatter kernels per day,
    # even though only quantities/equity/alive are recurrent.  Aggregate the
    # complete fixed-shape block once across its row dimension; the loop then
    # contains only the state-dependent integer basket choice and account
    # update.  This is an algebraic reassociation of the same scatter sums.
    holding_log_returns_all = execution[..., 0]
    executable_all = execution[..., 1] > 0.5
    must_liquidate_all = execution[..., 2] > 0.5
    opening_notional_all = execution[..., 3]
    ending_notional_all = execution[..., 4]
    collateral_all = execution[..., margin.INITIAL] if margin_mode else opening_notional_all
    fixed_fee_all = execution[..., 5]
    opening_tax_all = execution[..., 6]
    ending_tax_all = execution[..., 7]
    maximum_trade_all = torch.floor(
        execution[..., 8].clamp_min(0.0)
    ).to(torch.int64)
    group_index_all = torch.round(execution[..., 9]).to(torch.int64).clamp(
        0, slots - 1
    )
    candidate_tier_all = torch.round(execution[..., 10]).to(
        torch.int64
    ).clamp(0, 1)
    active_metadata_all = (
        torch.isfinite(holding_log_returns_all)
        & torch.isfinite(opening_notional_all)
        & (opening_notional_all > 0.0)
        & torch.isfinite(ending_notional_all)
        & (ending_notional_all > 0.0)
        & torch.isfinite(fixed_fee_all)
        & (fixed_fee_all >= 0.0)
        & torch.isfinite(opening_tax_all)
        & (opening_tax_all >= 0.0)
        & torch.isfinite(ending_tax_all)
        & (ending_tax_all >= 0.0)
    )
    if margin_mode:
        active_metadata_all &= torch.isfinite(collateral_all) & (collateral_all > 0)
    flat_candidate_index_all = group_index_all * 2 + candidate_tier_all

    def grouped_all(values: torch.Tensor) -> torch.Tensor:
        source = torch.where(
            active_metadata_all,
            values,
            torch.zeros_like(values),
        )
        return torch.zeros(
            (rows, slots * 2),
            device=weights.device,
            dtype=weights.dtype,
        ).scatter_add(1, flat_candidate_index_all, source).reshape(rows, slots, 2)

    valid_candidates_all = (
        torch.zeros(
            (rows, slots * 2),
            device=weights.device,
            dtype=torch.int64,
        ).scatter_add(
            1,
            flat_candidate_index_all,
            active_metadata_all.to(torch.int64),
        ).reshape(rows, slots, 2)
        > 0
    )
    can_trade_candidates_all = (
        torch.zeros(
            (rows, slots * 2),
            device=weights.device,
            dtype=torch.int64,
        ).scatter_add(
            1,
            flat_candidate_index_all,
            (
                active_metadata_all
                & executable_all
                & advance[:, None]
            ).to(torch.int64),
        ).reshape(rows, slots, 2)
        > 0
    )
    notionals_all = grouped_all(opening_notional_all)
    collateral_group_all = grouped_all(collateral_all)
    fees_all = grouped_all(fixed_fee_all)
    entry_taxes_all = grouped_all(opening_tax_all)
    trade_caps_all = grouped_all(maximum_trade_all.to(weights.dtype)).floor().to(
        torch.int64
    )
    if margin_mode:
        buy_allowed_all = grouped_all(execution[..., margin.CAN_BUY]) > 0.5
        sell_allowed_all = grouped_all(execution[..., margin.CAN_SELL]) > 0.5
    close_reserve_all = fees_all + entry_taxes_all
    base_reserved_cash_all = collateral_group_all + close_reserve_all
    requested_group_all = torch.zeros_like(weights).scatter_add(
        1,
        group_index_all,
        torch.where(
            advance[:, None],
            weights,
            torch.zeros_like(weights),
        ),
    )

    return_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    quantity_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    default_reason_rows: list[torch.Tensor] = []
    margin_backward_trace: list[tuple[torch.Tensor, ...]] = []
    margin_audit_rows: list[torch.Tensor] = []
    residual_rows: list[torch.Tensor] = []
    for row in range(rows):
        must_liquidate = must_liquidate_all[row]
        opening_notional = opening_notional_all[row]
        ending_notional = ending_notional_all[row]
        fixed_fee = fixed_fee_all[row]
        opening_tax = opening_tax_all[row]
        ending_tax = ending_tax_all[row]
        group_index = group_index_all[row]
        active_metadata = active_metadata_all[row]
        flat_candidate_index = flat_candidate_index_all[row]
        row_start_equity = equity
        row_start_alive = alive
        allocation_equity = equity
        gap_pnl = weights.new_zeros(())
        force_margin_close = torch.zeros_like(alive)
        margin_metadata_ok = torch.ones_like(alive)
        if margin_mode:
            event_cash = weights.new_zeros(())
            if execution.shape[-1] >= margin.MARGIN_CORPORATE_EXECUTION_WIDTH:
                moved, transferred_cash, transfer_ok = _transfer_margin_inventory(
                    quantities, execution[row], whole_contracts=True)
                quantities = torch.where(advance[row], moved, quantities)
                event_cash = torch.where(advance[row], transferred_cash, event_cash)
                margin_metadata_ok &= ~advance[row] | transfer_ok
            previous_mark = execution[row, :, margin.PREVIOUS_MARK]
            previous_mm = execution[row, :, margin.PREVIOUS_MAINTENANCE]
            carried = quantities != 0
            margin_metadata_ok &= (~carried | (active_metadata & torch.isfinite(previous_mark)
                & (previous_mark > 0) & torch.isfinite(previous_mm) & (previous_mm > 0))).all()
            gap_pnl = torch.where(carried & active_metadata,
                quantities.to(weights.dtype) * (opening_notional - previous_mark).nan_to_num(),
                torch.zeros_like(opening_notional)).sum() * advance[row]
            gap_pnl = gap_pnl + event_cash
            equity = equity + gap_pnl
            # Unsettled gains on retained positions do not finance new orders.
            allocation_equity = torch.minimum(row_start_equity, equity)
            previous_requirement = (quantities.abs() * previous_mm.nan_to_num()).sum()
            opening_requirement = (quantities.abs() * collateral_all[row].nan_to_num()).sum()
            risk_ratio = execution[row, :, margin.LIQUIDATION_RATIO].max()
            force_margin_close = advance[row] & alive & (
                (row_start_equity < previous_requirement)
                | (equity < opening_requirement * risk_ratio))
            position_group = execution[row, :, margin.POSITION_GROUP].nan_to_num().long().clamp(0, slots - 1)
            position_units = execution[row, :, margin.POSITION_UNIT].nan_to_num()
            group_limits = torch.zeros_like(opening_notional).scatter_reduce(0, position_group,
                execution[row, :, margin.POSITION_LIMIT].nan_to_num(), reduce="amax")
        group_target_weight = torch.where(
            alive,
            requested_group_all[row],
            torch.zeros_like(requested_group_all[row]),
        )
        group_target_weight = torch.where(force_margin_close, torch.zeros_like(group_target_weight), group_target_weight)
        target_cash = group_target_weight.abs() * allocation_equity.detach().clamp_min(0.0)
        group_sign = torch.sign(group_target_weight).to(torch.int64)
        valid_candidates = valid_candidates_all[row]
        can_trade_candidates = can_trade_candidates_all[row] & alive
        notionals = notionals_all[row]
        collateral = collateral_group_all[row]
        fees = fees_all[row]
        entry_taxes = entry_taxes_all[row]
        trade_caps = trade_caps_all[row]
        prior_group_quantities = torch.zeros(
            (slots * 2,), device=weights.device, dtype=torch.int64
        ).scatter_add(
            0,
            flat_candidate_index,
            torch.where(active_metadata, quantities, torch.zeros_like(quantities)),
        ).reshape(slots, 2)

        close_reserve = close_reserve_all[row]
        base_reserved_cash = base_reserved_cash_all[row]
        maximum_target_contracts = torch.where(
            valid_candidates,
            torch.floor(
                target_cash[:, None]
                / base_reserved_cash.clamp_min(1.0e-12)
            ),
            torch.zeros_like(base_reserved_cash),
        )
        unsigned_baskets = _integer_group_candidate_baskets(
            target_cash,
            base_reserved_cash,
            maximum_target_contracts,
            valid_candidates,
        )
        target_baskets = unsigned_baskets * group_sign[:, None, None]
        current_basket = prior_group_quantities[:, None, :]
        # A requested endpoint is an order target, not an all-or-none basket.
        # Project its signed delta into the observed opening capacity before
        # funding selection. Otherwise the sparse 1/32 target frontier can
        # contain no small feasible fill (mini-only groups have just full/zero
        # targets), even though one or more whole contracts can be executed.
        # Existing feasible targets remain identical; no exit data enter this
        # projection and a reversal consumes capacity closing the old side.
        requested_delta = target_baskets - current_basket
        permitted_delta = can_trade_candidates[:, None, :]
        if margin_mode:
            buy_allowed = buy_allowed_all[row]
            sell_allowed = sell_allowed_all[row]
            permitted_delta = permitted_delta & torch.where(
                requested_delta > 0,
                buy_allowed[:, None, :],
                sell_allowed[:, None, :],
            )
        executed_delta = torch.where(
            permitted_delta,
            torch.sign(requested_delta) * torch.minimum(
                requested_delta.abs(), trade_caps[:, None, :],
            ),
            torch.zeros_like(requested_delta),
        )
        target_baskets = current_basket + executed_delta
        close_delta = torch.minimum(prior_group_quantities.abs(), trade_caps)
        if margin_mode:
            buy_allowed = buy_allowed_all[row]
            sell_allowed = sell_allowed_all[row]
            close_allowed = can_trade_candidates & torch.where(prior_group_quantities < 0, buy_allowed, sell_allowed)
            close_delta = torch.where(close_allowed, close_delta, torch.zeros_like(close_delta))
        close_basket = (
            prior_group_quantities
            - torch.sign(prior_group_quantities) * close_delta
        )[:, None, :]
        candidate_baskets = torch.cat(
            (target_baskets, current_basket, close_basket), dim=1
        )
        deltas = candidate_baskets - prior_group_quantities[:, None, :]
        capacity_ok = (
            (deltas == 0)
            | (
                can_trade_candidates[:, None, :]
                & (deltas.abs() <= trade_caps[:, None, :])
            )
        ).all(dim=-1)
        if margin_mode:
            direction_allowed = (deltas == 0) | torch.where(deltas > 0, buy_allowed[:, None, :], sell_allowed[:, None, :])
            capacity_ok &= direction_allowed.all(-1)
            capacity_ok &= ~force_margin_close | (candidate_baskets == close_basket).all(-1)
        candidate_trade_cost = (
            deltas.abs().to(weights.dtype)
            * (fees + entry_taxes)[:, None, :]
        ).sum(dim=-1)
        candidate_reserved = (
            candidate_baskets.abs().to(weights.dtype)
            * (collateral + close_reserve)[:, None, :]
        ).sum(dim=-1)
        cash_required = candidate_reserved + candidate_trade_cost
        signed_exposure = (
            candidate_baskets.to(weights.dtype) * collateral[:, None, :]
        ).sum(dim=-1)
        target_exposure = group_target_weight * allocation_equity.detach().clamp_min(0.0)
        best, globally_fundable = _globally_funded_group_candidate_indices(
            cash_required=cash_required,
            candidate_exposure=signed_exposure,
            target_exposure=target_exposure,
            capacity_ok=capacity_ok,
            target_cash=target_cash,
            equity=allocation_equity.detach().clamp_min(0.0),
            target_candidate_count=int(target_baskets.size(1)),
        )
        chosen_group_quantities = candidate_baskets.gather(
            1, best[:, None, None].expand(-1, 1, 2)
        )[:, 0, :]
        if margin_mode:
            # An existing position below initial margin but above maintenance
            # is not bankrupt. If fully funded reallocation is impossible,
            # allow only holding/reducing old contracts; no new risk can use
            # the collateral deficit as an order budget.
            reduction_only = ((candidate_baskets == 0) | (
                (candidate_baskets * prior_group_quantities[:, None, :] > 0)
                & (candidate_baskets.abs() <= prior_group_quantities[:, None, :].abs())
            )).all(-1)
            reduction_score = torch.where(capacity_ok & reduction_only,
                (signed_exposure - target_exposure[:, None]).abs(),
                torch.full_like(signed_exposure, float("inf")))
            reduction_best = reduction_score.argmin(-1)
            reduction_basket = candidate_baskets.gather(
                1, reduction_best[:, None, None].expand(-1, 1, 2))[:, 0, :]
            chosen_group_quantities = torch.where(
                globally_fundable, chosen_group_quantities, reduction_basket)
            # Closing an insolvent account may itself create debt. Lack of
            # collateral must never suppress an otherwise executable close.
            chosen_group_quantities = torch.where(force_margin_close,
                close_basket[:, 0, :], chosen_group_quantities)
        chosen_by_slot = chosen_group_quantities.reshape(-1).gather(
            0, flat_candidate_index
        )
        chosen_by_slot = torch.where(
            active_metadata, chosen_by_slot, quantities
        )
        chosen_by_slot = torch.where(advance[row], chosen_by_slot, quantities)
        if margin_mode:
            physical_capacity = torch.where(
                active_metadata & executable_all[row] & alive & advance[row],
                maximum_trade_all[row],
                torch.zeros_like(quantities))
            close_permitted = torch.where(quantities < 0,
                execution[row, :, margin.CAN_BUY] > .5,
                execution[row, :, margin.CAN_SELL] > .5)
            chosen_by_slot, position_limit_failed_slots, position_reduction = _project_margin_position_limit_axes(
                chosen_by_slot, quantities, execution_row=execution[row], position_group=position_group,
                position_units=position_units, group_limits=group_limits,
                close_capacity=torch.where(close_permitted, physical_capacity, torch.zeros_like(physical_capacity)),
                whole_contracts=True,
            )
            position_limit_failed_slots &= advance[row] & alive
            position_limit_failed = position_limit_failed_slots.any()
        delta_by_slot = chosen_by_slot - quantities
        trade_cost = (
            delta_by_slot.abs().to(weights.dtype)
            * torch.where(
                active_metadata,
                fixed_fee + opening_tax,
                torch.zeros_like(fixed_fee),
            )
        ).sum()
        # Reuse the selector's cash expression and reduction tree. Summing
        # slot reserves and trade costs separately can round to a different
        # side of the same FP32 funding boundary. Margin position limits may
        # change the selected basket, so recompute that basket in group units.
        if margin_mode:
            final_group_quantities = torch.zeros_like(
                prior_group_quantities.reshape(-1),
            ).scatter_add(
                0, flat_candidate_index,
                torch.where(active_metadata, chosen_by_slot, torch.zeros_like(chosen_by_slot)),
            ).reshape(slots, 2)
            final_group_cash = (
                final_group_quantities.abs().to(weights.dtype)
                * (collateral + close_reserve)
            ).sum(dim=-1) + (
                (final_group_quantities - prior_group_quantities).abs().to(weights.dtype)
                * (fees + entry_taxes)
            ).sum(dim=-1)
        else:
            final_group_cash = cash_required.gather(1, best[:, None])[:, 0]
        final_cash_required = _funding_sum(final_group_cash)
        funded = (
            ~advance[row]
            | (
                globally_fundable
                & torch.isfinite(final_cash_required)
                & torch.isfinite(trade_cost)
                & (final_cash_required <= allocation_equity)
                & margin_metadata_ok
            )
        )
        if margin_mode:
            no_increase = ((chosen_by_slot == 0) | (
                (chosen_by_slot * quantities > 0) & (chosen_by_slot.abs() <= quantities.abs())
            )).all()
            funded |= no_increase & margin_metadata_ok & torch.isfinite(trade_cost)
        gross_pnl = (
            chosen_by_slot.to(weights.dtype)
            * torch.where(
                active_metadata,
                ending_notional - opening_notional,
                torch.zeros_like(opening_notional),
            )
        ).sum() * advance[row].to(weights.dtype)
        closed_by_slot = torch.where(must_liquidate & advance[row], chosen_by_slot, torch.zeros_like(chosen_by_slot))
        liquidation_failed = torch.zeros_like(alive)
        if margin_mode:
            expiry_cash = execution[row, :, margin.CASH_SETTLEMENT] > 0.5
            terminal_allowed = torch.where(chosen_by_slot < 0,
                execution[row, :, margin.TERMINAL_CAN_BUY] > 0.5,
                execution[row, :, margin.TERMINAL_CAN_SELL] > 0.5)
            terminal_capacity = (execution[row, :, margin.TERMINAL_CAPACITY].nan_to_num()
                                 - delta_by_slot.abs()).clamp_min(0).floor().long()
            closable = torch.where(terminal_allowed, torch.minimum(chosen_by_slot.abs(), terminal_capacity),
                                   torch.zeros_like(chosen_by_slot))
            closable = torch.where(expiry_cash, chosen_by_slot.abs(), closable)
            closed_by_slot = torch.where(must_liquidate & advance[row], torch.sign(chosen_by_slot) * closable,
                                         torch.zeros_like(chosen_by_slot))
            gross_pnl = gross_pnl + (closed_by_slot * torch.where(active_metadata,
                execution[row, :, margin.TERMINAL_MARK] - ending_notional,
                torch.zeros_like(ending_notional))).sum()
            # Risk liquidation is an order, not a guaranteed execution. Keep
            # unfilled whole contracts and their marked equity; the next
            # session re-evaluates margin and can continue reducing them.
            # A terminal obligation has no automatic next tradable session.
            liquidation_failed = position_limit_failed | (
                (must_liquidate & advance[row] & (chosen_by_slot != closed_by_slot)).any())
        forced_close_cost = (
            torch.where(
                must_liquidate & active_metadata & advance[row],
                closed_by_slot.abs().to(weights.dtype) * (fixed_fee + ending_tax),
                torch.zeros_like(opening_notional),
            )
        ).sum()
        exact_next_equity = torch.where(
            advance[row],
            equity - trade_cost + gross_pnl - forced_close_cost,
            equity,
        )
        net_simple = (
            exact_next_equity / row_start_equity.detach().clamp_min(1.0e-12) - 1.0
        )
        next_equity = exact_next_equity if margin_mode else equity * (1.0 + net_simple)
        row_alive = alive & (
            ~advance[row]
            | (
                funded
                & ~liquidation_failed
                & torch.isfinite(next_equity)
                & (next_equity > 0.0)
            )
        )
        row_default = alive & advance[row] & ~row_alive
        default_reason = torch.where(
            row_default & ~funded,
            torch.full_like(
                net_simple,
                TW_FUTURES_PORTFOLIO_DEFAULT_FUNDING,
                dtype=torch.int64,
            ),
            torch.where(
                row_default & ~torch.isfinite(next_equity),
                torch.full_like(
                    net_simple,
                    TW_FUTURES_PORTFOLIO_DEFAULT_NONFINITE_EQUITY,
                    dtype=torch.int64,
                ),
                torch.where(
                    row_default & (next_equity <= 0.0),
                    torch.full_like(
                        net_simple,
                        TW_FUTURES_PORTFOLIO_DEFAULT_NONPOSITIVE_EQUITY,
                        dtype=torch.int64,
                    ),
                    torch.full_like(
                        net_simple,
                        TW_FUTURES_PORTFOLIO_DEFAULT_NONE,
                        dtype=torch.int64,
                    ),
                ),
            ),
        )
        default_reason = torch.where(row_default & liquidation_failed,
            torch.full_like(default_reason, TW_FUTURES_PORTFOLIO_DEFAULT_MARGIN_LIQUIDATION), default_reason)
        safe_net = torch.where(
            row_alive,
            net_simple,
            torch.full_like(net_simple, -1.0 + 1.0e-7),
        )
        log_return = torch.where(
            advance[row] & alive,
            torch.log1p(safe_net),
            torch.zeros_like(safe_net),
        )
        if return_weights_history:
            exact_weight = (
                chosen_by_slot.to(weights.dtype)
                * torch.where(
                    active_metadata,
                    opening_notional,
                    torch.zeros_like(opening_notional),
                )
                / equity.detach().clamp_min(1.0e-12)
            )
        if return_turnovers:
            turnover = (
                delta_by_slot.abs().to(weights.dtype)
                * torch.where(
                    active_metadata,
                    opening_notional,
                    torch.zeros_like(opening_notional),
                )
                + torch.where(
                    must_liquidate & active_metadata,
                    closed_by_slot.abs().to(weights.dtype) * (execution[row, :, margin.TERMINAL_MARK] if margin_mode else ending_notional),
                    torch.zeros_like(ending_notional),
                )
            ).sum() / equity.detach().clamp_min(1.0e-12)
        else:
            turnover = torch.zeros_like(log_return)

        quantities = chosen_by_slot - closed_by_slot
        if margin_mode and return_margin_audit:
            end_im = (quantities.abs() * execution[row, :, margin.END_INITIAL].nan_to_num()).sum()
            end_mm = (quantities.abs() * execution[row, :, margin.END_MAINTENANCE].nan_to_num()).sum()
            used_im = (chosen_by_slot.abs() * collateral_all[row].nan_to_num()).sum()
            unfilled = torch.where(must_liquidate | force_margin_close
                | position_limit_failed_slots, quantities.abs(), torch.zeros_like(quantities)).sum()
            margin_audit_rows.append(torch.stack((row_start_equity, equity, exact_next_equity,
                used_im, end_im, end_mm, allocation_equity - trade_cost - used_im,
                (chosen_by_slot.abs() * opening_notional.nan_to_num()).sum() / equity.clamp_min(1e-12),
                (exact_next_equity < end_mm).to(weights.dtype),
                (force_margin_close | position_reduction).to(weights.dtype),
                unfilled.to(weights.dtype), gap_pnl)))
        if margin_mode and return_weights_history:
            residual_rows.append(quantities.clone())
        quantities = torch.where(row_alive, quantities, torch.zeros_like(quantities))
        equity = torch.where(row_alive, next_equity, torch.zeros_like(next_equity))
        alive = alive & row_alive
        if margin_mode and surrogate_weights.requires_grad and torch.is_grad_enabled():
            margin_backward_trace.append((row_start_equity, row_start_alive,
                chosen_by_slot, closed_by_slot, quantities, equity, force_margin_close, position_limit_failed))
        return_rows.append(log_return)
        turnover_rows.append(
            torch.where(advance[row] & (row_start_alive if margin_mode else alive), turnover, torch.zeros_like(turnover))
        )
        if return_weights_history:
            weight_rows.append(exact_weight)
            quantity_rows.append(chosen_by_slot)
        equity_scale_rows.append(equity / capital)
        default_rows.append(row_default)
        default_reason_rows.append(default_reason)

    exact_strategy_returns = torch.stack(return_rows)
    exact_turnovers = torch.stack(turnover_rows)
    equity_scales = torch.stack(equity_scale_rows)
    exact_history = (
        torch.stack(weight_rows)
        if return_weights_history
        else weights.new_empty((0, slots))
    )
    quantity_history = (
        torch.stack(quantity_rows)
        if return_weights_history
        else torch.empty((0, slots), device=weights.device, dtype=torch.int64)
    )
    if margin_mode and surrogate_weights.requires_grad and torch.is_grad_enabled():
        surrogate = _margin_physical_backward(
            surrogate_weights, execution, initial_capital=capital,
            initial_quantities=(torch.zeros(slots, device=weights.device)
                                if initial_quantities is None else initial_quantities.detach()),
            advance=advance, active_metadata=active_metadata_all,
            trace=margin_backward_trace, recover=recoverable_backward,
            return_weights_history=return_weights_history, return_turnovers=return_turnovers,
        )
    elif surrogate_weights.requires_grad and torch.is_grad_enabled():
        surrogate = run_tw_futures_portfolio_integer_surrogate_torch(
            surrogate_weights,
            integer_execution,
            initial_capital=capital,
            state_advance_mask=state_advance_mask,
            initial_quantities=(
                initial_quantities
                if _initial_surrogate_weights is None
                else None
            ),
            initial_weights=_initial_surrogate_weights,
            initial_equity_scale=(
                initial_equity_scale
                if _initial_surrogate_equity_scale is None
                else _initial_surrogate_equity_scale
            ),
            initial_alive=(
                initial_alive
                if _initial_surrogate_alive is None
                else _initial_surrogate_alive
            ),
            return_weights_history=return_weights_history,
            return_turnovers=return_turnovers,
            recover_after_default_for_backward=recoverable_backward,
            # A public batch boundary intentionally detaches recurrent state.
            # Internal compiled blocks must retain the graph across their
            # artificial boundary to remain gradient-identical to one eager
            # full-batch call.
            _detach_initial_weights=(_initial_surrogate_weights is None),
        )
    if surrogate_weights.requires_grad and torch.is_grad_enabled():
        # Add an exactly zero forward tangent rather than subtracting the
        # differently sized exact/shadow values. This preserves the integer
        # account bit for bit under grad/no-grad while retaining the shadow
        # derivative (including its recoverable-default protection).
        strategy_returns = exact_strategy_returns + (
            surrogate.strategy_returns - surrogate.strategy_returns.detach()
        )
        turnovers = exact_turnovers + (
            surrogate.turnovers - surrogate.turnovers.detach()
        )
        history = (
            exact_history
            + (surrogate.weights_history - surrogate.weights_history.detach())
            if return_weights_history
            else exact_history
        )
    else:
        strategy_returns = exact_strategy_returns
        turnovers = exact_turnovers
        history = exact_history
    return FuturesPortfolioTensorResult(
        strategy_returns=strategy_returns,
        turnovers=turnovers,
        weights_history=history,
        final_weights=quantities.to(dtype=torch.float32),
        final_alive=alive,
        equity_scale_history=equity_scales,
        final_equity_scale=(
            equity_scales[-1] if equity_scales.numel() else starting_scale
        ),
        contract_quantities_history=quantity_history,
        default_history=torch.stack(default_rows),
        default_reason_history=torch.stack(default_reason_rows),
        margin_audit_history=(torch.stack(margin_audit_rows)
                              if margin_mode and return_margin_audit else None),
        residual_contract_quantities_history=torch.stack(residual_rows) if residual_rows else None,
        _surrogate_final_weights=(
            surrogate.final_weights
            if surrogate_weights.requires_grad and torch.is_grad_enabled()
            else None
        ),
        _surrogate_final_equity_scale=(
            surrogate.final_equity_scale
            if surrogate_weights.requires_grad and torch.is_grad_enabled()
            else None
        ),
        _surrogate_final_alive=(
            surrogate.final_alive
            if surrogate_weights.requires_grad and torch.is_grad_enabled()
            else None
        ),
    )


def _integer_result_tuple(
    result: FuturesPortfolioTensorResult,
) -> tuple[torch.Tensor, ...]:
    """Flatten one integer-account result into a compile-safe tensor tuple."""

    if (
        result.equity_scale_history is None
        or result.final_equity_scale is None
        or result.contract_quantities_history is None
        or result.default_history is None
        or result.default_reason_history is None
    ):
        raise RuntimeError("integer futures result omitted exact account state")
    shadow_weights = (
        result.final_weights
        if result._surrogate_final_weights is None
        else result._surrogate_final_weights
    )
    shadow_scale = (
        result.final_equity_scale
        if result._surrogate_final_equity_scale is None
        else result._surrogate_final_equity_scale
    )
    shadow_alive = (
        result.final_alive
        if result._surrogate_final_alive is None
        else result._surrogate_final_alive
    )
    return (
        result.strategy_returns,
        result.turnovers,
        result.weights_history,
        result.final_weights,
        result.final_alive,
        result.equity_scale_history,
        result.final_equity_scale,
        result.contract_quantities_history,
        result.default_history,
        result.default_reason_history,
        shadow_weights,
        shadow_scale,
        shadow_alive,
    )


def _initial_integer_surrogate_state(
    integer_execution: torch.Tensor,
    *,
    initial_capital: float,
    initial_quantities: torch.Tensor | None,
    initial_equity_scale: torch.Tensor | None,
    initial_alive: torch.Tensor | None,
    recoverable_backward: bool,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Construct the shadow state exactly as the eager surrogate does."""

    execution = integer_execution.to(dtype=torch.float32)
    slots = int(execution.size(1))
    quantities = (
        torch.zeros((slots,), device=execution.device, dtype=torch.int64)
        if initial_quantities is None
        else torch.round(
            initial_quantities.detach().to(
                device=execution.device,
                dtype=torch.float32,
            )
        ).to(torch.int64)
    )
    supplied_alive = (
        torch.ones((), device=execution.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().to(
            device=execution.device,
            dtype=torch.bool,
        ).reshape(())
    )
    supplied_scale = (
        execution.new_ones(())
        if initial_equity_scale is None
        else initial_equity_scale.detach().to(
            device=execution.device,
            dtype=torch.float32,
        ).reshape(())
    )
    if recoverable_backward:
        valid_start = (
            supplied_alive
            & torch.isfinite(supplied_scale)
            & (supplied_scale > 0.0)
        )
        shadow_scale = torch.where(
            valid_start,
            supplied_scale,
            execution.new_ones(()),
        )
        shadow_alive = torch.ones_like(supplied_alive)
    else:
        shadow_scale = supplied_scale
        shadow_alive = supplied_alive

    first = execution[0]
    holding_log_returns = first[:, 0]
    opening_notional = first[:, 3]
    ending_notional = first[:, 4]
    fixed_fee = first[:, 5]
    opening_tax = first[:, 6]
    ending_tax = first[:, 7]
    active = (
        torch.isfinite(holding_log_returns)
        & torch.isfinite(opening_notional)
        & (opening_notional > 0.0)
        & torch.isfinite(ending_notional)
        & (ending_notional > 0.0)
        & torch.isfinite(fixed_fee)
        & (fixed_fee >= 0.0)
        & torch.isfinite(opening_tax)
        & (opening_tax >= 0.0)
        & torch.isfinite(ending_tax)
        & (ending_tax >= 0.0)
    )
    shadow_equity = execution.new_full((), float(initial_capital)) * shadow_scale
    shadow_weights = (
        quantities.to(dtype=torch.float32)
        * torch.where(active, opening_notional, torch.zeros_like(opening_notional))
        / shadow_equity.clamp_min(1.0e-12)
    )
    return shadow_weights, shadow_scale, shadow_alive


def _compiled_integer_block(
    target_weights: torch.Tensor,
    *,
    block_rows: int,
    initial_capital: float,
    return_weights_history: bool,
    return_turnovers: bool,
    recoverable_backward: bool,
) -> tuple[
    tuple[object, ...],
    Callable[..., tuple[torch.Tensor, ...]],
]:
    device_index = (
        target_weights.device.index
        if target_weights.device.index is not None
        else torch.cuda.current_device()
    )
    training_shadow = bool(
        torch.is_grad_enabled() and target_weights.requires_grad
    )
    key: tuple[object, ...] = (
        int(device_index),
        str(target_weights.dtype),
        int(target_weights.size(1)),
        int(block_rows),
        float(initial_capital),
        bool(return_weights_history),
        bool(return_turnovers),
        bool(recoverable_backward),
        training_shadow,
    )
    compiled = _COMPILED_INTEGER_BLOCKS.get(key)
    if compiled is not None:
        return key, compiled

    def block(
        block_weights: torch.Tensor,
        block_execution: torch.Tensor,
        block_advance: torch.Tensor,
        exact_quantities: torch.Tensor,
        exact_equity_scale: torch.Tensor,
        exact_alive: torch.Tensor,
        shadow_weights: torch.Tensor,
        shadow_equity_scale: torch.Tensor,
        shadow_alive: torch.Tensor,
    ) -> tuple[torch.Tensor, ...]:
        return _integer_result_tuple(
            _run_tw_futures_portfolio_integer_torch_impl(
                block_weights,
                block_execution,
                initial_capital=initial_capital,
                state_advance_mask=block_advance,
                initial_quantities=exact_quantities,
                initial_equity_scale=exact_equity_scale,
                initial_alive=exact_alive,
                return_weights_history=return_weights_history,
                return_turnovers=return_turnovers,
                recoverable_backward=recoverable_backward,
                _initial_surrogate_weights=(
                    shadow_weights if training_shadow else None
                ),
                _initial_surrogate_equity_scale=(
                    shadow_equity_scale if training_shadow else None
                ),
                _initial_surrogate_alive=(
                    shadow_alive if training_shadow else None
                ),
            )
        )

    compiled = torch.compile(
        block,
        fullgraph=True,
        dynamic=False,
        options={"triton.cudagraphs": False},
    )
    _COMPILED_INTEGER_BLOCKS[key] = compiled
    _INTEGER_COMPILE_STATS["compile_constructors"] += 1
    return key, compiled


def _pad_integer_compile_tail(
    target_weights: torch.Tensor,
    integer_execution: torch.Tensor,
    state_advance_mask: torch.Tensor,
    *,
    block_rows: int,
) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Pad a short terminal block with inert rows for the fixed graph.

    An eager terminal recurrence is disproportionately expensive: even one
    real row launches the full 1,936-slot grouped-account kernel sequence.  A
    zero execution row with ``state_advance_mask=False`` is an identity step
    for both the exact integer ledger and its differentiable shadow, so the
    fixed compiled graph can process the tail without changing any public or
    recurrent result.  Concatenation keeps only the real prefix.
    """

    rows, slots = tuple(target_weights.shape)
    if not 0 < rows < int(block_rows):
        raise ValueError("integer compile tail must contain 1..block_rows-1 rows")
    if tuple(integer_execution.shape) != (rows, slots, 11):
        raise ValueError("integer compile tail execution must have shape [T,S,11]")
    if tuple(state_advance_mask.shape) != (rows,):
        raise ValueError("integer compile tail advance mask must have shape [T]")
    padding = int(block_rows) - rows
    return (
        torch.cat(
            (
                target_weights,
                target_weights.new_zeros((padding, slots)),
            ),
            dim=0,
        ),
        torch.cat(
            (
                integer_execution,
                integer_execution.new_zeros((padding, slots, 11)),
            ),
            dim=0,
        ),
        torch.cat(
            (
                state_advance_mask,
                torch.zeros(
                    (padding,),
                    device=state_advance_mask.device,
                    dtype=torch.bool,
                ),
            ),
            dim=0,
        ),
    )


def run_tw_futures_portfolio_integer_torch(
    target_weights: torch.Tensor,
    integer_execution: torch.Tensor,
    *,
    initial_capital: float,
    state_advance_mask: torch.Tensor | None = None,
    initial_quantities: torch.Tensor | None = None,
    initial_equity_scale: torch.Tensor | None = None,
    initial_alive: torch.Tensor | None = None,
    return_weights_history: bool = True,
    return_turnovers: bool = True,
    return_margin_audit: bool = True,
    recoverable_backward: bool = False,
) -> FuturesPortfolioTensorResult:
    """Run the exact account through reusable, gradient-identical blocks."""

    if futures_cuda_graph_enabled(target_weights):
        return run_futures_cuda_graph(
            _run_tw_futures_portfolio_integer_torch_impl,
            target_weights, integer_execution,
            initial_capital=initial_capital,
            state_advance_mask=state_advance_mask,
            initial_quantities=initial_quantities,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
            return_turnovers=return_turnovers,
            return_margin_audit=return_margin_audit,
            recoverable_backward=recoverable_backward,
        )
    block_rows = resolve_tw_futures_portfolio_integer_compiled_block_rows()
    compile_blocks = bool(
        block_rows > 0
        and int(integer_execution.size(-1)) == 11
        and target_weights.ndim == 2
        and int(target_weights.size(0)) >= block_rows
        and target_weights.device.type == "cuda"
        and hasattr(torch, "compile")
        and _env_truthy("STOCKAGENT_BACKTEST_COMPILE", "1")
        and not torch.compiler.is_compiling()
    )
    if not compile_blocks:
        return _run_tw_futures_portfolio_integer_torch_impl(
            target_weights,
            integer_execution,
            initial_capital=initial_capital,
            state_advance_mask=state_advance_mask,
            initial_quantities=initial_quantities,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
            return_turnovers=return_turnovers,
            return_margin_audit=return_margin_audit,
            recoverable_backward=recoverable_backward,
        )

    rows, slots = tuple(target_weights.shape)
    if (
        integer_execution.ndim != 3
        or tuple(integer_execution.shape[:2]) != (rows, slots)
        or int(integer_execution.size(-1)) != 11
    ):
        raise ValueError("integer_execution must have shape [T,S,11]")
    advance = (
        torch.ones((rows,), device=target_weights.device, dtype=torch.bool)
        if state_advance_mask is None
        else state_advance_mask.to(device=target_weights.device, dtype=torch.bool)
    )
    if tuple(advance.shape) != (rows,):
        raise ValueError("state_advance_mask must have shape [T]")
    exact_quantities = (
        torch.zeros((slots,), device=target_weights.device, dtype=torch.int64)
        if initial_quantities is None
        else torch.round(
            initial_quantities.detach().to(
                device=target_weights.device,
                dtype=torch.float32,
            )
        ).to(torch.int64)
    )
    exact_equity_scale = (
        target_weights.new_ones((), dtype=torch.float32)
        if initial_equity_scale is None
        else initial_equity_scale.detach().to(
            device=target_weights.device,
            dtype=torch.float32,
        ).reshape(())
    )
    exact_alive = (
        torch.ones((), device=target_weights.device, dtype=torch.bool)
        if initial_alive is None
        else initial_alive.detach().to(
            device=target_weights.device,
            dtype=torch.bool,
        ).reshape(())
    )
    training_shadow = bool(
        torch.is_grad_enabled() and target_weights.requires_grad
    )
    if training_shadow:
        shadow_weights, shadow_equity_scale, shadow_alive = (
            _initial_integer_surrogate_state(
                integer_execution,
                initial_capital=initial_capital,
                initial_quantities=exact_quantities,
                initial_equity_scale=exact_equity_scale,
                initial_alive=exact_alive,
                recoverable_backward=recoverable_backward,
            )
        )
    else:
        shadow_weights = exact_quantities.to(dtype=torch.float32)
        shadow_equity_scale = exact_equity_scale
        shadow_alive = exact_alive

    try:
        compiled_key, compiled_block = _compiled_integer_block(
            target_weights,
            block_rows=block_rows,
            initial_capital=float(initial_capital),
            return_weights_history=return_weights_history,
            return_turnovers=return_turnovers,
            recoverable_backward=recoverable_backward,
        )
    except Exception:
        if _strict_no_fallback_enabled():
            raise
        _INTEGER_COMPILE_STATS["eager_fallback_calls"] += 1
        return _run_tw_futures_portfolio_integer_torch_impl(
            target_weights,
            integer_execution,
            initial_capital=initial_capital,
            state_advance_mask=advance,
            initial_quantities=exact_quantities,
            initial_equity_scale=exact_equity_scale,
            initial_alive=exact_alive,
            return_weights_history=return_weights_history,
            recoverable_backward=recoverable_backward,
        )

    outputs: list[tuple[torch.Tensor, ...]] = []
    full_stop = rows - rows % block_rows
    for start in range(0, full_stop, block_rows):
        stop = start + block_rows
        try:
            values = compiled_block(
                target_weights[start:stop],
                integer_execution[start:stop],
                advance[start:stop],
                exact_quantities,
                exact_equity_scale,
                exact_alive,
                shadow_weights,
                shadow_equity_scale,
                shadow_alive,
            )
        except Exception:
            _COMPILED_INTEGER_BLOCKS.pop(compiled_key, None)
            _FAILED_INTEGER_BLOCKS.add(compiled_key)
            if _strict_no_fallback_enabled():
                raise
            _INTEGER_COMPILE_STATS["eager_fallback_calls"] += 1
            values = _integer_result_tuple(
                _run_tw_futures_portfolio_integer_torch_impl(
                    target_weights[start:stop],
                    integer_execution[start:stop],
                    initial_capital=initial_capital,
                    state_advance_mask=advance[start:stop],
                    initial_quantities=exact_quantities,
                    initial_equity_scale=exact_equity_scale,
                    initial_alive=exact_alive,
                    return_weights_history=return_weights_history,
                    return_turnovers=return_turnovers,
                    recoverable_backward=recoverable_backward,
                    _initial_surrogate_weights=(
                        shadow_weights if training_shadow else None
                    ),
                    _initial_surrogate_equity_scale=(
                        shadow_equity_scale if training_shadow else None
                    ),
                    _initial_surrogate_alive=(
                        shadow_alive if training_shadow else None
                    ),
                )
            )
        else:
            _INTEGER_COMPILE_STATS["compiled_block_calls"] += 1
            _INTEGER_COMPILE_STATS["compiled_day_calls"] += block_rows
        outputs.append(values)
        exact_quantities = values[3]
        exact_alive = values[4]
        exact_equity_scale = values[6]
        if training_shadow:
            shadow_weights = values[10]
            shadow_equity_scale = values[11]
            shadow_alive = values[12]

    if full_stop < rows:
        valid_tail_rows = rows - full_stop
        padded_weights, padded_execution, padded_advance = (
            _pad_integer_compile_tail(
                target_weights[full_stop:],
                integer_execution[full_stop:],
                advance[full_stop:],
                block_rows=block_rows,
            )
        )
        try:
            tail = compiled_block(
                padded_weights,
                padded_execution,
                padded_advance,
                exact_quantities,
                exact_equity_scale,
                exact_alive,
                shadow_weights,
                shadow_equity_scale,
                shadow_alive,
            )
        except Exception:
            _COMPILED_INTEGER_BLOCKS.pop(compiled_key, None)
            _FAILED_INTEGER_BLOCKS.add(compiled_key)
            if _strict_no_fallback_enabled():
                raise
            _INTEGER_COMPILE_STATS["eager_fallback_calls"] += 1
            tail = _integer_result_tuple(
                _run_tw_futures_portfolio_integer_torch_impl(
                    target_weights[full_stop:],
                    integer_execution[full_stop:],
                    initial_capital=initial_capital,
                    state_advance_mask=advance[full_stop:],
                    initial_quantities=exact_quantities,
                    initial_equity_scale=exact_equity_scale,
                    initial_alive=exact_alive,
                    return_weights_history=return_weights_history,
                    return_turnovers=return_turnovers,
                    recoverable_backward=recoverable_backward,
                    _initial_surrogate_weights=(
                        shadow_weights if training_shadow else None
                    ),
                    _initial_surrogate_equity_scale=(
                        shadow_equity_scale if training_shadow else None
                    ),
                    _initial_surrogate_alive=(
                        shadow_alive if training_shadow else None
                    ),
                )
            )
        else:
            _INTEGER_COMPILE_STATS["compiled_block_calls"] += 1
            _INTEGER_COMPILE_STATS["compiled_day_calls"] += valid_tail_rows
            _INTEGER_COMPILE_STATS["compiled_tail_calls"] += 1

            # Row-shaped histories include inert padding.  Terminal states are
            # deliberately left unsliced because every padded row is an
            # identity transition.
            tail = tuple(
                value[:valid_tail_rows]
                if index in {0, 1, 2, 5, 7, 8, 9}
                else value
                for index, value in enumerate(tail)
            )
        outputs.append(tail)
        exact_quantities = tail[3]
        exact_alive = tail[4]
        exact_equity_scale = tail[6]
        if training_shadow:
            shadow_weights = tail[10]
            shadow_equity_scale = tail[11]
            shadow_alive = tail[12]

    def concatenate(index: int) -> torch.Tensor:
        return torch.cat([values[index] for values in outputs], dim=0)

    equity_scale_history = concatenate(5)
    return FuturesPortfolioTensorResult(
        strategy_returns=concatenate(0),
        turnovers=concatenate(1),
        weights_history=concatenate(2),
        final_weights=exact_quantities,
        final_alive=exact_alive,
        equity_scale_history=equity_scale_history,
        final_equity_scale=exact_equity_scale,
        contract_quantities_history=concatenate(7),
        default_history=concatenate(8),
        default_reason_history=concatenate(9),
        _surrogate_final_weights=(shadow_weights if training_shadow else None),
        _surrogate_final_equity_scale=(
            shadow_equity_scale if training_shadow else None
        ),
        _surrogate_final_alive=(shadow_alive if training_shadow else None),
    )


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
    fee_rate_per_open_notional: torch.Tensor,
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
    if max_turnover_ratio < 0.0:
        raise ValueError("max_turnover_ratio must be non-negative")
    weights = target_weights.to(dtype=torch.float32)
    returns = holding_log_returns.to(device=weights.device, dtype=torch.float32)
    tradable = tradable_mask.to(device=weights.device, dtype=torch.bool)
    liquidate = must_liquidate_mask.to(device=weights.device, dtype=torch.bool)
    fee_rates = fee_rate_per_open_notional.to(
        device=weights.device,
        dtype=torch.float32,
    )
    if tuple(fee_rates.shape) != tuple(weights.shape):
        raise ValueError(
            "fee_rate_per_open_notional must match weights [T,S]"
        )
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
        row_fee_rates = fee_rates[row]
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
        fee_active = (delta.abs() > 1.0e-8) | (
            row_advances & liquidate[row] & (held.abs() > 1.0e-8)
        )
        invalid_fee = fee_active & (
            ~torch.isfinite(row_fee_rates) | (row_fee_rates < 0.0)
        )
        clean_fee_rates = torch.where(
            row_advances
            & torch.isfinite(row_fee_rates)
            & (row_fee_rates >= 0.0),
            row_fee_rates,
            torch.zeros_like(row_fee_rates),
        )
        # delta/held are opening-NAV notional weights.  Dividing the fixed
        # per-contract fee by open*multiplier converts each fractional
        # contract trade directly to the same NAV denominator.  A forced
        # close charges the number of held contracts, not ending notional.
        fixed_commission = (
            delta.abs() * clean_fee_rates
            + torch.where(liquidate[row], held.abs(), torch.zeros_like(held))
            * clean_fee_rates
        ).sum()
        gross_simple = torch.where(
            row_advances,
            (held * clean_asset_return).sum(),
            torch.zeros((), device=weights.device, dtype=torch.float32),
        )
        net_simple = (
            gross_simple - fixed_commission
        )
        survived = (
            alive
            & ~invalid_active
            & ~invalid_fee.any()
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
    fee_rate_per_open_notional: np.ndarray,
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
    fee_rates = np.asarray(fee_rate_per_open_notional, dtype=np.float64)
    if fee_rates.shape != weights.shape:
        raise ValueError(
            "fee_rate_per_open_notional must match weights [T,S]"
        )
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
        fee_active = (np.abs(delta) > 1.0e-8) | (
            liquidate[row] & (np.abs(held) > 1.0e-8)
        )
        valid_fee = np.isfinite(fee_rates[row]) & (fee_rates[row] >= 0.0)
        clean_fee = np.where(valid_fee, fee_rates[row], 0.0)
        fixed_commission = float(
            np.sum(
                np.abs(delta) * clean_fee
                + np.where(liquidate[row], np.abs(held), 0.0) * clean_fee
            )
        )
        gross = float(np.sum(held * clean_return))
        net = (
            gross - fixed_commission
        )
        survived = bool(
            alive
            and valid_return.all()
            and valid_fee[fee_active].all()
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
    "TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_FORWARD",
    "TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE",
    "TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE",
    "TW_FUTURES_PORTFOLIO_INTEGER_COMPILED_BLOCK_ROWS",
    "get_tw_futures_portfolio_integer_compile_stats",
    "resolve_tw_futures_portfolio_integer_compiled_block_rows",
    "run_tw_futures_portfolio_continuous_numpy",
    "run_tw_futures_portfolio_continuous_torch",
    "run_tw_futures_portfolio_integer_surrogate_torch",
    "run_tw_futures_portfolio_integer_torch",
]
