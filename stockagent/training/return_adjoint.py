"""Whole-account metrics and bounded-memory return adjoints.

Single-metric objectives are independent experiments. Complete configured annual
accounts retain their resets and date weights; batches are never treated as
independent Sharpe or drawdown observations. Only return vectors cross ranks.
The existing physical ledger and its declared truncated surrogate VJP remain
the authoritative forward/backward implementation.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import torch
from torch import Tensor
import torch.distributed as dist

from stockagent.training.multiobjective import (
    MULTIOBJECTIVE_OBJECTIVES, TASK_OBJECTIVES, multiobjective_contract,
)


RETURN_ADJOINT_ABI = "annual_single_metric_return_adjoint_v3"
TRAJECTORY_OBJECTIVES = frozenset({
    "trajectory_sharpe", "trajectory_return", "trajectory_sortino", "trajectory_mdd",
    "trajectory_calmar",
})
RETURN_ADJOINT_OBJECTIVES = TRAJECTORY_OBJECTIVES | MULTIOBJECTIVE_OBJECTIVES
RATIO_VARIANCE_EPS = 1e-8
CALMAR_DRAWDOWN_EPS = 1e-8
REPLAY_ABSOLUTE_TOLERANCE = 1e-9


def return_adjoint_contract(objective: str) -> dict:
    if objective in MULTIOBJECTIVE_OBJECTIVES:
        return {**multiobjective_contract(objective),
                "ratio_variance_epsilon": RATIO_VARIANCE_EPS,
                "calmar_task": return_adjoint_contract("trajectory_calmar"),
                "backward": "canonical_block_detached_physical_surrogate_vjp",
                "model_forward_passes": 5 if objective.endswith("_parameter") else 2,
                "replay_absolute_tolerance": REPLAY_ABSOLUTE_TOLERANCE,
                "update_cadence": "one_optimizer_step_after_all_verified_replays"}
    if objective not in TRAJECTORY_OBJECTIVES:
        raise ValueError("return-adjoint contract requires one single trajectory metric")
    contract = {
        "abi": RETURN_ADJOINT_ABI,
        "objective": objective,
        "scope": "valid_date_weighted_complete_independently_reset_accounts",
        "return": "compound_per_account",
        "ratio_returns": "daily_log_returns_ddof0_252_periods",
        "ratio_variance_epsilon": RATIO_VARIANCE_EPS,
        "drawdown": "positive_magnitude_initial_nav_included",
        "backward": "canonical_block_detached_physical_surrogate_vjp",
        "replay_absolute_tolerance": REPLAY_ABSOLUTE_TOLERANCE,
        "update_cadence": ("one_optimizer_step_after_complete_return_tape"
                           if objective == "trajectory_return" else "one_optimizer_step_after_verified_replay"),
        "engine": ("single_pass_account_compound_gradient_rescale_v1"
                   if objective == "trajectory_return" else "two_pass_no_grad_ledger_record_verified_vjp_v2"),
    }
    if objective == "trajectory_calmar":
        contract.update(
            abi="annual_calmar_return_adjoint_v1",
            calmar_numerator="expm1_252_times_mean_daily_net_log_return",
            calmar_denominator="maximum_drawdown_magnitude_clamped_minimum",
            calmar_drawdown_epsilon=CALMAR_DRAWDOWN_EPS,
        )
    return contract


def account_offsets(accounts_by_rank, rows: int) -> tuple[int, ...]:
    """Reconstruct reset boundaries, rejecting gaps/duplicate owned accounts."""
    accounts = sorted((account for rank in accounts_by_rank for account in rank),
                      key=lambda account: account[1])
    if (not accounts or accounts[0][1] != 0 or accounts[-1][2] != rows
            or any(a[2] != b[1] for a, b in zip(accounts, accounts[1:]))
            or any(a[1] >= a[2] for a in accounts)
            or len({a[0] for a in accounts}) != len(accounts)):
        raise ValueError("return objective accounts must cover every physical date exactly once")
    return (0, *(int(account[2]) for account in accounts))


def trajectory_metric_loss(
    log_returns: Tensor, objective: str, episode_offsets: Sequence[int] | None = None,
) -> Tensor:
    """Negative Sharpe/compound return/Sortino/Calmar, or positive MDD magnitude.

    Each complete account starts at NAV 1. Multiple independently reset
    accounts contribute in proportion to valid dates. Ratios use daily log
    returns, ddof=0 and 252 annualization. The fixed variance epsilon follows
    the existing ratio-training contract, including finite cash gradients.
    Reported financial metrics keep their existing unsmoothed definitions.
    Calmar uses annualized compound net return divided by positive MDD, with
    only a fixed denominator floor for zero or near-zero drawdown accounts.
    """
    if objective in MULTIOBJECTIVE_OBJECTIVES:
        # Surgery is a vector field, not the derivative of a scalar aggregate.
        # A declared pure-log validation criterion owns checkpoint selection.
        objective = "log_utility"
    if objective not in TRAJECTORY_OBJECTIVES | {"log_utility"}:
        raise ValueError(f"unsupported return-adjoint objective: {objective}")
    if log_returns.ndim != 1 or not log_returns.is_floating_point():
        raise ValueError("return objective requires a one-dimensional floating tensor")
    if not bool(torch.isfinite(log_returns).all()):
        raise FloatingPointError("non-finite physical return cannot become a zero sample")
    n = log_returns.numel()
    if n == 0:
        return log_returns.double().sum() * 0.
    offsets = tuple(episode_offsets) if episode_offsets is not None else (0, n)
    if (len(offsets) < 2 or offsets[0] != 0 or offsets[-1] != n
            or any(a >= b for a, b in zip(offsets, offsets[1:]))):
        raise ValueError("episode offsets must cover every return exactly once")
    r = log_returns.double()
    lengths = torch.tensor([b - a for a, b in zip(offsets, offsets[1:])],
                           device=r.device, dtype=torch.long)
    starts = torch.tensor(offsets[:-1], device=r.device, dtype=torch.long)
    columns = torch.arange(max(b - a for a, b in zip(offsets, offsets[1:])), device=r.device)
    valid = columns[None, :] < lengths[:, None]
    indices = (starts[:, None] + columns[None, :]).clamp_max(n - 1)
    values = r[indices].masked_fill(~valid, 0.)
    count = lengths.to(r.dtype)
    mean = values.sum(1) / count
    if objective == "log_utility":
        per_account = -252. * mean
    elif objective == "trajectory_return":
        # Capital is reset between accounts, so do not compound across them.
        per_account = -torch.expm1(values.sum(1))
    elif objective == "trajectory_sharpe":
        variance = ((values - mean[:, None]).masked_fill(~valid, 0.)).square().sum(1) / count
        per_account = -(252. ** .5) * mean / (variance + RATIO_VARIANCE_EPS).sqrt()
    elif objective == "trajectory_sortino":
        downside = values.clamp_max(0.).square().sum(1) / count
        per_account = -(252. ** .5) * mean / (downside + RATIO_VARIANCE_EPS).sqrt()
    else:
        cumulative = values.cumsum(1)
        with_initial = torch.cat((values.new_zeros((len(offsets) - 1, 1)), cumulative), 1)
        peaks = with_initial.cummax(1).values[:, 1:]
        depth = -torch.expm1((cumulative - peaks).clamp_max(0.))
        per_account = depth.masked_fill(~valid, 0.).amax(1)
        if objective == "trajectory_calmar":
            annualized_return = torch.expm1(252. * mean)
            per_account = -annualized_return / per_account.clamp_min(CALMAR_DRAWDOWN_EPS)
    return (per_account * count).sum() / n


def return_adjoint_weights(
    log_returns: Tensor, objective: str, episode_offsets: Sequence[int],
) -> tuple[Tensor, Tensor]:
    """Factor dL/dtheta into whole-account dL/dr and the canonical ledger VJP.

    Existing trajectory reduction is -252/N * sum(weight_i * r_i).
    Therefore weight_i = -N/252 * dL/dr_i. The DDP loop already compensates
    its gradient mean by world_size for independent account ownership.
    """
    if objective in MULTIOBJECTIVE_OBJECTIVES:
        raise ValueError("multiobjective surgery requires all four task return gradients")
    with torch.enable_grad():
        leaf = log_returns.detach().double().clone().requires_grad_(True)
        value = trajectory_metric_loss(leaf, objective, episode_offsets)
        gradient, = torch.autograd.grad(value, leaf)
    if not bool(torch.isfinite(value) & torch.isfinite(gradient).all()):
        raise FloatingPointError("non-finite whole-account metric or return adjoint")
    return value.detach(), gradient.detach() * (-leaf.numel() / 252.)


def task_return_gradients(log_returns: Tensor, episode_offsets: Sequence[int]) -> tuple[Tensor, Tensor]:
    """Four raw dL/dr rows at one fixed policy, including annual log utility.

    The second objective is LOG return, independently of the compound-return
    single-loss experiment. No model/ledger graph is retained here.
    """
    values, gradients = [], []
    with torch.enable_grad():
        leaf = log_returns.detach().double().clone().requires_grad_(True)
        for objective in TASK_OBJECTIVES:
            value = trajectory_metric_loss(leaf, objective, episode_offsets)
            gradient, = torch.autograd.grad(value, leaf)
            values.append(value.detach())
            gradients.append(gradient.detach())
    values, gradients = torch.stack(values), torch.stack(gradients)
    if not bool(torch.isfinite(values).all() & torch.isfinite(gradients).all()):
        raise FloatingPointError("non-finite multiobjective return adjoints")
    return values, gradients


class CompoundReturnAdjoint:
    """Accumulate -exp(sum(r)) d(sum(r)) once per independent account.

    The physical trainer already accumulates -252/N * sum(r) gradients,
    including DDP's world-size compensation. At each account boundary multiply
    by N_account/252 * exp(sum(r)), then add to the epoch gradient. No model
    forward/ledger replay or retained activation graph is needed. Unused
    parameters stay absent so weight decay cannot silently activate them.
    """

    def __init__(self, parameters):
        self.parameters = tuple(p for p in parameters if p.requires_grad)
        self.total = {}

    @torch.no_grad()
    def finish_account(self, log_return_sum: Tensor, rows: int):
        if rows <= 0 or log_return_sum.numel() != 1:
            raise ValueError("compound adjoint needs one complete account")
        scale = log_return_sum.double().exp() * (rows / 252.)
        from stockagent.backtest.tw_day_trade_graph import ledger_predicate
        if not ledger_predicate(torch.isfinite(scale), 'compound_return_gradient_scale'):
            raise FloatingPointError("compound account return adjoint is non-finite")
        active = [p for p in self.parameters if p.grad is not None]
        grads = [p.grad for p in active]
        if grads:
            torch._foreach_mul_(grads, scale)
        existing = [p for p in active if id(p) in self.total]
        if existing:
            torch._foreach_add_([self.total[id(p)] for p in existing], [p.grad for p in existing])
        for p in active:
            if id(p) not in self.total:
                self.total[id(p)] = p.grad
            p.grad = None

    @torch.no_grad()
    def restore_epoch_gradients(self):
        for p in self.parameters:
            if p.grad is not None:
                raise RuntimeError("unfinished account gradients cannot enter the epoch update")
            p.grad = self.total.get(id(p))


@dataclass
class ReturnAdjointTape:
    """Date-indexed return collection; no retained NN/ledger activation graph."""

    values: Tensor
    counts: Tensor
    expected: Tensor | None = None
    maximum_replay_error: float = 0.
    actions: Tensor | None = None
    expected_actions: Tensor | None = None

    @classmethod
    def create(cls, rows: int, device: torch.device, *, expected: Tensor | None = None,
               expected_actions: Tensor | None = None):
        if rows <= 0:
            raise ValueError("return tape needs valid training dates")
        return cls(torch.zeros(rows, dtype=torch.float64, device=device),
                   torch.zeros(rows, dtype=torch.int32, device=device), expected,
                   expected_actions=expected_actions)

    @torch.no_grad()
    def reset_recording(self):
        """Discard a failed speculative pass while preserving its replay oracle."""
        self.values.zero_()
        self.counts.zero_()
        self.actions = None
        self.maximum_replay_error = 0.

    @torch.no_grad()
    def record_actions(self, start: int, actions: Tensor):
        if actions.ndim != 2 or start < 0 or start + actions.shape[0] > self.values.numel():
            raise ValueError("return tape actions do not align with physical dates")
        if self.actions is None:
            self.actions = actions.new_zeros((self.values.numel(), actions.shape[1]))
        self.actions[start:start + actions.shape[0]].copy_(actions.detach())

    @torch.no_grad()
    def record(self, start: int, returns: Tensor, sample_mask: Tensor):
        stop = min(self.values.numel(), start + returns.numel())
        count = stop - start
        if (start < 0 or returns.ndim != 1 or count <= 0
                or sample_mask.ndim != 1 or sample_mask.numel() != returns.numel()):
            raise ValueError("unaligned or out-of-scope return tape rows")
        mask = sample_mask[:count].bool()
        self.values[start:stop].add_(returns[:count].detach().double().masked_fill(~mask, 0.))
        self.counts[start:stop].add_(mask.to(torch.int32))

    @torch.no_grad()
    def finish(self):
        if dist.is_available() and dist.is_initialized():
            dist.all_reduce(self.values, op=dist.ReduceOp.SUM)
            dist.all_reduce(self.counts, op=dist.ReduceOp.SUM)
        if not bool((self.counts == 1).all() & torch.isfinite(self.values).all()):
            raise RuntimeError("return tape has missing, duplicated or non-finite dates")
        if self.expected is not None:
            if self.expected.shape != self.values.shape:
                raise ValueError("replay return tape shape changed")
            error = (self.values - self.expected).abs().amax()
            self.maximum_replay_error = float(error)
            if self.maximum_replay_error > REPLAY_ABSOLUTE_TOLERANCE:
                policy_error = self.values.new_zeros(())
                if self.expected_actions is not None and self.actions is not None:
                    policy_error = (self.actions - self.expected_actions).abs().amax().double()
                    if dist.is_available() and dist.is_initialized():
                        dist.all_reduce(policy_error, op=dist.ReduceOp.MAX)
                row = int((self.values - self.expected).abs().argmax())
                raise RuntimeError(f"fixed-policy return replay changed before optimizer step: {float(error):.3g}; "
                                   f"date_row={row}; policy_action_max_abs_error={float(policy_error):.9g}")
        return self.values
