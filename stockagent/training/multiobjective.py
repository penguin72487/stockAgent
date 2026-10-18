"""Small-Gram PCGrad/MGDA with bounded-memory parameter-gradient capture.

Only the four-by-four Gram matrix crosses to the host. Surgery is performed in
coefficient space, so the large gradients need a single final linear combination.
The raw objective units are retained: no task is dropped, renormalized or given
a hidden return reward. A zero MGDA direction is a legitimate result.
"""
from __future__ import annotations

import itertools
import random

import numpy as np
import torch


# v2 replaces the standalone drawdown task with Calmar. This changes the
# gradient field and must reject continuation from the old four-task ABI.
MULTIOBJECTIVE_ABI = "annual_four_metric_calmar_raw_gram_surgery_v2"
TASK_NAMES = ("sharpe", "log_return", "sortino", "calmar")
TASK_OBJECTIVES = ("trajectory_sharpe", "log_utility", "trajectory_sortino", "trajectory_calmar")
MULTIOBJECTIVE_OBJECTIVES = frozenset(
    f"trajectory_{method}_{space}"
    for method in ("pcgrad", "mgda") for space in ("return", "parameter")
)


def surgery_spec(objective: str) -> tuple[str, str]:
    if objective not in MULTIOBJECTIVE_OBJECTIVES:
        raise ValueError(f"unsupported multiobjective method: {objective}")
    _, method, space = objective.split("_")
    return method, space


def multiobjective_contract(objective: str) -> dict:
    method, space = surgery_spec(objective)
    return dict(
        abi=MULTIOBJECTIVE_ABI, objective=objective, method=method, space=space,
        tasks=list(TASK_OBJECTIVES), scaling="raw_objective_units_no_normalization",
        log_return="negative_252_mean_daily_log_return",
        account_reduction="valid_date_weighted_complete_independently_reset_accounts",
        pcgrad="sum_projected_tasks_against_unmodified_references_seed_plus_epoch_v1",
        mgda="all_nonempty_simplex_faces_minimum_norm_kkt_certified_fp64_v1",
        zero_gradient="retain_all_tasks_allow_pareto_stationary_zero_direction",
        validation_selection="pure_log_utility_no_test_selection",
        parameter_gradients="globally_mean_reduced_before_gram_no_individual_clipping",
        optimizer="configured_optimizer_after_one_combination_and_one_clip_per_epoch",
        guarantee="declared_surrogate_gradient_geometry_not_discrete_fills_adamw_or_finite_step_performance",
    )


def pcgrad_coefficients(gram: np.ndarray, *, seed: int, step: int) -> np.ndarray:
    """Algorithm 1: each task visits the other ORIGINAL gradients randomly."""
    count = len(gram)
    projected = np.eye(count, dtype=np.float64)
    # Local RNG leaves training/dropout RNG and exact resume state untouched.
    rng = random.Random((int(seed) << 32) + int(step))
    for i in range(count):
        others = [j for j in range(count) if j != i]
        rng.shuffle(others)
        for j in others:
            dot = projected[i] @ gram[:, j]
            if dot < 0. and gram[j, j] > 0.:
                projected[i, j] -= dot / gram[j, j]
    return projected.sum(axis=0)


def mgda_coefficients(gram: np.ndarray) -> np.ndarray:
    """Solve the four-task simplex QP, including singular/zero-gradient faces.

    A convex quadratic's optimum is the interior stationary point of at least
    one face. Enumerating 15 faces avoids an iterative device solver and gives
    a checkable global first-order certificate. No ridge changes the objective.
    """
    count = len(gram)
    scale = float(np.max(np.abs(gram)))
    if scale == 0.:
        return np.full(count, 1. / count)
    q = gram / scale
    candidates = []
    for size in range(1, count + 1):
        for indices in itertools.combinations(range(count), size):
            ix = np.array(indices)
            system = np.zeros((size + 1, size + 1), dtype=np.float64)
            system[:size, :size] = q[np.ix_(ix, ix)]
            system[:size, size] = system[size, :size] = 1.
            rhs = np.zeros(size + 1)
            rhs[size] = 1.
            try:
                solution = np.linalg.solve(system, rhs)
            except np.linalg.LinAlgError:
                solution = np.linalg.lstsq(system, rhs, rcond=1e-14)[0]
            if (not np.isfinite(solution).all() or solution[:size].min() < -1e-10
                    or np.max(np.abs(system @ solution - rhs)) > 1e-9):
                continue
            alpha = np.zeros(count)
            alpha[ix] = np.maximum(solution[:size], 0.)
            alpha /= alpha.sum()
            candidates.append((float(alpha @ q @ alpha), alpha))
    if not candidates:
        raise FloatingPointError("MGDA has no feasible simplex candidate")
    _, alpha = min(candidates, key=lambda candidate: candidate[0])
    products = gram @ alpha
    norm2 = float(alpha @ products)
    # This certificate is in original units, not the globally rescaled system.
    tolerance = 1e-9 * max(1., abs(norm2), float(np.max(np.abs(products))))
    if float(np.min(products) - norm2) < -tolerance:
        raise FloatingPointError("MGDA simplex solution failed its global KKT certificate")
    return alpha


@torch.no_grad()
def combine_task_gradients(
    gradients: torch.Tensor, method: str, *, seed: int = 42, step: int = 0,
) -> tuple[torch.Tensor, dict[str, float]]:
    if (gradients.ndim != 2 or gradients.shape[0] != len(TASK_NAMES)
            or gradients.shape[1] == 0 or not gradients.is_floating_point()):
        raise ValueError("surgery requires four nonempty floating gradient rows")
    if method not in {"pcgrad", "mgda"}:
        raise ValueError("surgery method must be pcgrad or mgda")
    # FP64 retains the raw, potentially very different objective scales.
    rows = gradients.double()
    gram = (rows @ rows.T).cpu().numpy()
    if not np.isfinite(gram).all():
        raise FloatingPointError("non-finite task gradient Gram matrix")
    gram = (gram + gram.T) * .5
    coefficients = (pcgrad_coefficients(gram, seed=seed, step=step)
                    if method == "pcgrad" else mgda_coefficients(gram))
    mixed = torch.as_tensor(coefficients, device=rows.device) @ rows
    products = gram @ coefficients
    norm2 = max(0., float(coefficients @ products))
    norms = np.sqrt(np.maximum(np.diag(gram), 0.))
    diagnostics = dict(combined_norm=norm2 ** .5,
                       conflict_pairs=float(sum(gram[i, j] < 0. for i in range(4) for j in range(i))),
                       zero_gradient_tasks=float(sum(norms == 0.)),
                       mgda_kkt_margin=float(np.min(products) - norm2) if method == "mgda" else 0.)
    for i, name in enumerate(TASK_NAMES):
        diagnostics[f"gradient_norm_{name}"] = float(norms[i])
        diagnostics[f"coefficient_{name}"] = float(coefficients[i])
        diagnostics[f"raw_descent_derivative_{name}"] = float(-products[i])
        for j in range(i):
            denominator = norms[i] * norms[j]
            diagnostics[f"cosine_{TASK_NAMES[j]}_{name}"] = float(
                gram[i, j] / denominator if denominator > 0. else 0.)
    return mixed.to(gradients.dtype), diagnostics


class ParameterGradientBank:
    """Four synchronized unclipped VJPs; globally unused parameters stay None."""

    def __init__(self, parameters):
        self.parameters = tuple(p for p in parameters if p.requires_grad)
        if not self.parameters or len({(p.device, p.dtype) for p in self.parameters}) != 1:
            raise ValueError("parameter surgery requires one device/dtype of trainable parameters")
        self.sizes = tuple(p.numel() for p in self.parameters)
        self.rows = self.parameters[0].new_empty((4, sum(self.sizes)))
        self.present = [False] * len(self.parameters)
        self.count = 0
        self.before = None

    @torch.no_grad()
    def record(self):
        if self.count >= 4:
            raise RuntimeError("parameter surgery already has four tasks")
        offset = 0
        for index, (p, size) in enumerate(zip(self.parameters, self.sizes, strict=True)):
            destination = self.rows[self.count, offset:offset + size]
            if p.grad is None:
                destination.zero_()
            else:
                if p.grad.is_sparse:
                    raise ValueError("parameter surgery requires dense gradients")
                destination.copy_(p.grad.reshape(-1))
                self.present[index] = True
            offset += size
        self.count += 1

    @torch.no_grad()
    def combine(self, method: str, *, seed: int, step: int) -> dict[str, float]:
        self.record()
        if self.count != 4:
            raise RuntimeError("combine requires four complete synchronized task VJPs")
        mixed, diagnostics = combine_task_gradients(self.rows, method, seed=seed, step=step)
        offset = 0
        for p, size, present in zip(self.parameters, self.sizes, self.present, strict=True):
            p.grad = mixed[offset:offset + size].view_as(p) if present else None
            offset += size
        self.before = torch.cat([p.detach().reshape(-1) for p in self.parameters])
        return diagnostics

    @torch.no_grad()
    def observe_update(self) -> dict[str, float]:
        if self.before is None:
            raise RuntimeError("no combined update to observe")
        delta = torch.cat([p.detach().reshape(-1) for p in self.parameters]) - self.before
        products = (self.rows.double() @ delta.double()).cpu().tolist()
        return {f"actual_update_derivative_{name}": float(value)
                for name, value in zip(TASK_NAMES, products, strict=True)}
