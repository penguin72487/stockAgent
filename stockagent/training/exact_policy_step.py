"""Backtrack a proposed optimizer update against the actual training account.

This is an optimizer acceptance rule, not a different executor or smoothed loss.
It cannot establish a future drawdown bound. The evaluator must replay the whole
training split at fixed parameters without backward recovery or future data.
"""
from copy import deepcopy
from dataclasses import asdict, dataclass
import math
from typing import Callable

import torch


@dataclass(frozen=True)
class PolicyEvaluation:
    loss: float
    max_drawdown: float
    defaulted: bool
    drawdown_start: int = 0
    drawdown_end: int = 0
    benchmark_max_drawdown: float | None = None

    def feasible(self, budget: float) -> bool:
        return (math.isfinite(self.loss) and math.isfinite(self.max_drawdown)
                and -budget <= self.max_drawdown <= 0. and not self.defaulted)


def maximum_drawdown_span(log_returns: torch.Tensor) -> tuple[int, int]:
    """Return [start,end) of the worst drawdown, including initial capital."""
    r = log_returns.detach().double()
    if r.ndim != 1 or not bool(torch.isfinite(r).all()):
        raise ValueError('drawdown span requires finite one-dimensional log returns')
    if not r.numel():
        return 0, 0
    cumulative = torch.cat((r.new_zeros(1), r.cumsum(0)))
    peaks, peak_indices = cumulative.cummax(0)
    end = int((cumulative - peaks).argmin().item())
    return int(peak_indices[end].item()), end


def bind_return_weighted_loss(loss_fn, weights, start, rows):
    """Bind chronological VJP weights; padding never advances the account."""
    if weights is None:
        return loss_fn
    from functools import partial
    local = weights[start:start + rows]
    if local.numel() < rows:
        local = torch.nn.functional.pad(local, (0, rows - local.numel()))
    return partial(loss_fn, log_return_weights=local)


def resolve_training_drawdown_budget(setting, baseline: PolicyEvaluation) -> float:
    """A relative risk budget uses only the benchmark in the training replay."""
    if setting == 'benchmark':
        measured = baseline.benchmark_max_drawdown
        if measured is None or not math.isfinite(measured) or not -1. < measured < 0.:
            raise ValueError('training benchmark must have a finite nonzero drawdown')
        return -measured
    return float(setting)


class ExactPolicyStep:
    """Backtrack Adam, optionally projecting its outward risk component away."""

    def __init__(self, model, optimizer, scheduler, baseline: PolicyEvaluation, budget: float):
        if not 0. < budget < 1. or not baseline.feasible(budget):
            raise ValueError('exact policy step requires a feasible initial training account')
        self.parameters = [p for p in model.parameters() if p.requires_grad]
        self.before = [p.detach().clone() for p in self.parameters]
        self.optimizer = optimizer
        self.scheduler = scheduler
        self.optimizer_before = deepcopy(optimizer.state_dict())
        self.scheduler_before = None if scheduler is None else deepcopy(scheduler.state_dict())
        self.baseline = baseline
        self.budget = budget

    def restore(self):
        with torch.no_grad():
            for p, old in zip(self.parameters, self.before, strict=True):
                p.copy_(old)
        self.optimizer.load_state_dict(self.optimizer_before)
        if self.scheduler is not None:
            self.scheduler.load_state_dict(self.scheduler_before)
        self.optimizer.zero_grad(set_to_none=True)

    def resolve(self, evaluate: Callable[[], PolicyEvaluation], risk_gradient=None, *, risk_interior=False):
        proposal = [p.detach().clone() for p in self.parameters]
        trials = []
        # Numerical tolerance, not an economic loss or drawdown allowance.
        tolerance = 8 * torch.finfo(torch.float32).eps * max(1., abs(self.baseline.loss))
        try:
            direction = 'adam'
            projection = {}
            interior_proposal = None
            backtracks = 0
            while backtracks < 9:
                fraction = 2. ** -backtracks
                with torch.no_grad():
                    for p, old, new in zip(self.parameters, self.before, proposal, strict=True):
                        p.copy_(torch.lerp(old, new, fraction))
                candidate = evaluate()
                accepted = candidate.feasible(self.budget) and candidate.loss <= self.baseline.loss + tolerance
                measured = {key: (None if isinstance(value, float) and not math.isfinite(value) else value)
                            for key, value in asdict(candidate).items()}
                trials.append(dict(direction=direction, fraction=fraction, accepted=accepted, **measured))
                if accepted:
                    return candidate, dict(accepted=True, fraction=fraction, direction=direction, trials=trials, **projection)
                if (risk_interior and direction == 'risk_tangent' and backtracks == 0
                        and interior_proposal is not None and not candidate.feasible(self.budget)):
                    # A tangent of a curved/discrete feasible set can still
                    # point outside it at every finite step. Reflect Adam's
                    # outward normal component inward: a - 2 (g.a/g.g) g.
                    # This is a search candidate, never a claimed risk bound.
                    with torch.no_grad():
                        for p, new in zip(self.parameters, interior_proposal, strict=True):
                            p.copy_(new)
                    inner = evaluate()
                    inner_accepted = inner.feasible(self.budget) and inner.loss <= self.baseline.loss + tolerance
                    measured = {key: (None if isinstance(value, float) and not math.isfinite(value) else value)
                                for key, value in asdict(inner).items()}
                    trials.append(dict(direction='risk_interior', fraction=1., accepted=inner_accepted, **measured))
                    if inner_accepted:
                        return inner, dict(accepted=True, fraction=1., direction='risk_interior', trials=trials, **projection)
                if (risk_gradient is not None and direction == 'adam' and backtracks == 0
                        and not candidate.feasible(self.budget)):
                    # Compute a risk normal at the OLD feasible policy. The
                    # callback reuses the chronological full-trajectory VJP;
                    # Adam's proposed moments/scheduler are not advanced again.
                    with torch.no_grad():
                        for p, old in zip(self.parameters, self.before, strict=True):
                            p.copy_(old)
                    gradients = risk_gradient(self.baseline)
                    with torch.no_grad():
                        dots = [(new - old).double().mul(g.double()).sum()
                                for old, new, g in zip(self.before, proposal, gradients, strict=True)]
                        norms = [g.double().square().sum() for g in gradients]
                        dot, norm2 = torch.stack(dots).sum(), torch.stack(norms).sum()
                        if not bool(torch.isfinite(dot) & torch.isfinite(norm2)):
                            raise FloatingPointError('non-finite drawdown projection')
                        if float(dot) > 0 and float(norm2) > 0:
                            coefficient = dot / norm2
                            proposal = [new - coefficient.to(g.dtype) * g
                                        for new, g in zip(proposal, gradients, strict=True)]
                            if risk_interior:
                                interior_proposal = [new - coefficient.to(g.dtype) * g
                                                     for new, g in zip(proposal, gradients, strict=True)]
                            projection = {'risk_dot_before': float(dot), 'risk_normal_norm': float(norm2.sqrt())}
                            direction = 'risk_tangent'
                            backtracks = 0
                            continue
                backtracks += 1
        except BaseException:
            self.restore()
            raise
        self.restore()
        return self.baseline, dict(accepted=False, fraction=0., direction=direction, trials=trials, **projection)
