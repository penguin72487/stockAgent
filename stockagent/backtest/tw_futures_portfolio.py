"""Continuous notional ledger for TAIFEX delivery-month-slot positions."""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import torch
import torch.nn.functional as F

from stockagent.data.tw_futures_portfolio_daily import (
    TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION,
)


# Bump whenever the explicit backward-only relaxation changes.  The exact
# integer forward account is stable, but checkpoints trained under a different
# surrogate objective must not be resumed as if their gradients were identical.
TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_SURROGATE = (
    "grouped_fake_floor_cash_surrogate_v4"
)
TW_FUTURES_PORTFOLIO_INTEGER_RECOVERABLE_TRAINING_SURROGATE = (
    "grouped_fake_floor_cash_solvency_recovery_surrogate_v5"
)
TW_FUTURES_PORTFOLIO_INTEGER_TRAINING_FORWARD = "exact_integer_account_v1"

TW_FUTURES_PORTFOLIO_DEFAULT_NONE = 0
TW_FUTURES_PORTFOLIO_DEFAULT_FUNDING = 1
TW_FUTURES_PORTFOLIO_DEFAULT_NONFINITE_EQUITY = 2
TW_FUTURES_PORTFOLIO_DEFAULT_NONPOSITIVE_EQUITY = 3


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


@dataclass(slots=True)
class FuturesPortfolioNumpyResult:
    strategy_returns: np.ndarray
    turnovers: np.ndarray
    weights_history: np.ndarray
    final_weights: np.ndarray
    final_alive: np.ndarray


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
    standard_candidates = torch.stack(
        (
            standard_max,
            (standard_max - 1.0).clamp_min(0.0),
            (standard_max - 2.0).clamp_min(0.0),
            torch.zeros_like(standard_max),
            torch.zeros_like(standard_max),
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
    recover_after_default_for_backward: bool = False,
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
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError("integer futures target_weights must have shape [T,S]")
    if (
        integer_execution.ndim != 3
        or tuple(integer_execution.shape[:2]) != tuple(target_weights.shape)
        or int(integer_execution.size(-1)) != 11
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
        ending_notional / opening_notional.clamp_min(1.0e-12) - 1.0,
        torch.zeros_like(opening_notional),
    )
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
        maximum_trade * opening_notional,
        torch.zeros_like(opening_notional),
    )
    group_capacity_cash = torch.zeros_like(weights).scatter_add(
        1,
        group_index,
        slot_capacity_cash,
    )
    entry_cost_rate = torch.where(
        active,
        (fixed_fee + opening_tax) / opening_notional.clamp_min(1.0e-12),
        torch.zeros_like(opening_notional),
    )
    exit_cost_rate = torch.where(
        active,
        (fixed_fee + ending_tax) / opening_notional.clamp_min(1.0e-12),
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
        opening_notional
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
    equity = weights.new_tensor(capital) * starting_scale
    alive = (
        torch.ones((), device=weights.device, dtype=torch.bool)
        if recover_after_default_for_backward
        else supplied_alive
    )
    if initial_quantities is not None and initial_weights is not None:
        raise ValueError("provide initial_quantities or initial_weights, not both")
    if initial_weights is not None:
        previous_slot_weights = torch.nan_to_num(
            initial_weights.detach().to(device=weights.device, dtype=weights.dtype),
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
                opening_notional[0],
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
        executable_request = torch.sign(raw_request) * quantized_request_abs
        requested_delta = executable_request - current_group
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
        available_increase = (1.0 - gross_after_reductions).clamp_min(0.0)
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
        entry_cost = (
            group_delta.abs() * group_entry_cost_rate[row]
        ).sum()
        close_cost = torch.where(
            group_must_liquidate[row] & row_advances,
            held_group.abs() * group_exit_cost_rate[row],
            torch.zeros_like(held_group),
        ).sum()
        net_simple = gross_simple - entry_cost - close_cost
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
                + (held_group.abs() * group_exit_cost_rate[row]).sum()
            )
            funding_excess = funding_required - 1.0
            funding_barrier = F.softplus(funding_excess / 0.05) * 0.05
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
        denominator = (1.0 + safe_net).clamp_min(1.0e-7)
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
                weights.new_tensor(capital),
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
    recoverable_backward: bool = False,
) -> FuturesPortfolioTensorResult:
    """Run the exact fully-collateralized all-futures carrying account.

    The forward account uses signed integer contract quantities.  Standard and
    mini stock/ETF contracts sharing an underlying and delivery month receive
    one aggregate model exposure and are packed together.  Every other slot is
    a singleton group.  Unused sleeve cash is never reassigned to another
    group, both long and short reserve full absolute notional, and actual PnL,
    per-side fixed fees, rounded transaction tax, and expiry closes update the
    next session's equity.  The differentiable path is a straight-through
    continuous surrogate; reported quantities and cash are always exact.
    """

    if target_weights.ndim != 2 or target_weights.numel() == 0:
        raise ValueError("integer futures target_weights must have shape [T,S]")
    if (
        integer_execution.ndim != 3
        or tuple(integer_execution.shape[:2]) != tuple(target_weights.shape)
        or int(integer_execution.size(-1)) != 11
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
    equity = weights.new_tensor(capital) * starting_scale

    return_rows: list[torch.Tensor] = []
    turnover_rows: list[torch.Tensor] = []
    weight_rows: list[torch.Tensor] = []
    quantity_rows: list[torch.Tensor] = []
    equity_scale_rows: list[torch.Tensor] = []
    default_rows: list[torch.Tensor] = []
    default_reason_rows: list[torch.Tensor] = []
    for row in range(rows):
        row_exec = execution[row]
        holding_log_returns = row_exec[:, 0]
        executable = row_exec[:, 1] > 0.5
        must_liquidate = row_exec[:, 2] > 0.5
        opening_notional = row_exec[:, 3]
        ending_notional = row_exec[:, 4]
        fixed_fee = row_exec[:, 5]
        opening_tax = row_exec[:, 6]
        ending_tax = row_exec[:, 7]
        maximum_trade = torch.floor(row_exec[:, 8].clamp_min(0.0)).to(torch.int64)
        group_index = torch.round(row_exec[:, 9]).to(torch.int64).clamp(0, slots - 1)
        candidate_tier = torch.round(row_exec[:, 10]).to(torch.int64).clamp(0, 1)
        active_metadata = (
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
        requested = torch.where(
            advance[row] & alive,
            weights[row],
            torch.zeros_like(weights[row]),
        )
        group_target_weight = torch.zeros_like(requested).scatter_add(
            0, group_index, requested
        )
        target_cash = group_target_weight.abs() * equity.detach().clamp_min(0.0)
        group_sign = torch.sign(group_target_weight).to(torch.int64)

        flat_candidate_index = group_index * 2 + candidate_tier

        def grouped(values: torch.Tensor, *, default: float = 0.0) -> torch.Tensor:
            out = torch.full(
                (slots * 2,),
                float(default),
                device=weights.device,
                dtype=weights.dtype,
            )
            source = torch.where(active_metadata, values, torch.zeros_like(values))
            return out.scatter_add(0, flat_candidate_index, source).reshape(slots, 2)

        valid_candidates = torch.zeros(
            (slots * 2,), device=weights.device, dtype=torch.int64
        ).scatter_add(
            0, flat_candidate_index, active_metadata.to(torch.int64)
        ).reshape(slots, 2) > 0
        can_trade_candidates = torch.zeros(
            (slots * 2,), device=weights.device, dtype=torch.int64
        ).scatter_add(
            0,
            flat_candidate_index,
            (active_metadata & executable & advance[row] & alive).to(torch.int64),
        ).reshape(slots, 2) > 0
        notionals = grouped(opening_notional)
        fees = grouped(fixed_fee)
        entry_taxes = grouped(opening_tax)
        trade_caps = grouped(maximum_trade.to(weights.dtype)).floor().to(torch.int64)
        prior_group_quantities = torch.zeros(
            (slots * 2,), device=weights.device, dtype=torch.int64
        ).scatter_add(
            0,
            flat_candidate_index,
            torch.where(active_metadata, quantities, torch.zeros_like(quantities)),
        ).reshape(slots, 2)

        close_reserve = fees + entry_taxes
        base_reserved_cash = notionals + close_reserve
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
        close_delta = torch.minimum(prior_group_quantities.abs(), trade_caps)
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
        candidate_trade_cost = (
            deltas.abs().to(weights.dtype)
            * (fees + entry_taxes)[:, None, :]
        ).sum(dim=-1)
        candidate_reserved = (
            candidate_baskets.abs().to(weights.dtype)
            * (notionals + close_reserve)[:, None, :]
        ).sum(dim=-1)
        cash_required = candidate_reserved + candidate_trade_cost
        eps = (
            torch.finfo(weights.dtype).eps
            * target_cash.abs().clamp_min(1.0)
            * 16.0
        )
        cash_ok = cash_required <= target_cash[:, None] + eps[:, None]
        signed_exposure = (
            candidate_baskets.to(weights.dtype) * notionals[:, None, :]
        ).sum(dim=-1)
        target_exposure = group_target_weight * equity.detach().clamp_min(0.0)
        tracking_error = (signed_exposure - target_exposure[:, None]).abs()
        feasible = capacity_ok & cash_ok
        any_feasible = feasible.any(dim=1, keepdim=True)
        de_risk_score = cash_required + tracking_error
        large = (
            cash_required.detach().amax(dim=1, keepdim=True)
            + tracking_error.detach().amax(dim=1, keepdim=True)
            + equity.detach().abs()
            + 1.0
        )
        score = torch.where(
            any_feasible,
            torch.where(feasible, tracking_error, large + de_risk_score),
            torch.where(capacity_ok, de_risk_score, large + de_risk_score),
        )
        best = score.argmin(dim=1)
        chosen_group_quantities = candidate_baskets.gather(
            1, best[:, None, None].expand(-1, 1, 2)
        )[:, 0, :]
        chosen_by_slot = chosen_group_quantities.reshape(-1).gather(
            0, flat_candidate_index
        )
        chosen_by_slot = torch.where(
            active_metadata, chosen_by_slot, quantities
        )
        chosen_by_slot = torch.where(advance[row], chosen_by_slot, quantities)
        delta_by_slot = chosen_by_slot - quantities
        trade_cost = (
            delta_by_slot.abs().to(weights.dtype)
            * torch.where(
                active_metadata,
                fixed_fee + opening_tax,
                torch.zeros_like(fixed_fee),
            )
        ).sum()
        collateral_and_exit_reserve = (
            chosen_by_slot.abs().to(weights.dtype)
            * torch.where(
                active_metadata,
                opening_notional + fixed_fee + opening_tax,
                torch.zeros_like(opening_notional),
            )
        ).sum()
        funded = (
            ~advance[row]
            | (
                torch.isfinite(collateral_and_exit_reserve)
                & torch.isfinite(trade_cost)
                & (collateral_and_exit_reserve + trade_cost <= equity + 1.0e-3)
            )
        )
        gross_pnl = (
            chosen_by_slot.to(weights.dtype)
            * torch.where(
                active_metadata,
                ending_notional - opening_notional,
                torch.zeros_like(opening_notional),
            )
        ).sum() * advance[row].to(weights.dtype)
        forced_close_cost = (
            torch.where(
                must_liquidate & active_metadata & advance[row],
                chosen_by_slot.abs().to(weights.dtype) * (fixed_fee + ending_tax),
                torch.zeros_like(opening_notional),
            )
        ).sum()
        exact_next_equity = torch.where(
            advance[row],
            equity - trade_cost + gross_pnl - forced_close_cost,
            equity,
        )
        net_simple = (
            exact_next_equity / equity.detach().clamp_min(1.0e-12) - 1.0
        )
        next_equity = equity * (1.0 + net_simple)
        row_alive = alive & (
            ~advance[row]
            | (
                funded
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
        exact_weight = (
            chosen_by_slot.to(weights.dtype)
            * torch.where(active_metadata, opening_notional, torch.zeros_like(opening_notional))
            / equity.detach().clamp_min(1.0e-12)
        )
        turnover = (
            delta_by_slot.abs().to(weights.dtype)
            * torch.where(active_metadata, opening_notional, torch.zeros_like(opening_notional))
            + torch.where(
                must_liquidate & active_metadata,
                chosen_by_slot.abs().to(weights.dtype) * ending_notional,
                torch.zeros_like(ending_notional),
            )
        ).sum() / equity.detach().clamp_min(1.0e-12)

        quantities = torch.where(
            advance[row] & must_liquidate,
            torch.zeros_like(chosen_by_slot),
            chosen_by_slot,
        )
        quantities = torch.where(row_alive, quantities, torch.zeros_like(quantities))
        equity = torch.where(row_alive, next_equity, torch.zeros_like(next_equity))
        alive = alive & row_alive
        return_rows.append(log_return)
        turnover_rows.append(
            torch.where(advance[row] & alive, turnover, torch.zeros_like(turnover))
        )
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
    if surrogate_weights.requires_grad and torch.is_grad_enabled():
        surrogate = run_tw_futures_portfolio_integer_surrogate_torch(
            surrogate_weights,
            integer_execution,
            initial_capital=capital,
            state_advance_mask=state_advance_mask,
            initial_quantities=initial_quantities,
            initial_equity_scale=initial_equity_scale,
            initial_alive=initial_alive,
            return_weights_history=return_weights_history,
            recover_after_default_for_backward=recoverable_backward,
        )
        strategy_returns = surrogate.strategy_returns + (
            exact_strategy_returns - surrogate.strategy_returns
        ).detach()
        turnovers = surrogate.turnovers + (
            exact_turnovers - surrogate.turnovers
        ).detach()
        history = (
            surrogate.weights_history
            + (exact_history - surrogate.weights_history).detach()
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
    "run_tw_futures_portfolio_continuous_numpy",
    "run_tw_futures_portfolio_continuous_torch",
    "run_tw_futures_portfolio_integer_surrogate_torch",
    "run_tw_futures_portfolio_integer_torch",
]
