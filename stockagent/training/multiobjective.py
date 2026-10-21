"""Small-Gram PCGrad/MGDA with bounded-memory parameter-gradient capture.

Only the four-by-four Gram matrix crosses to the host. Surgery is performed in
coefficient space, so the large gradients need a single final linear combination.
Raw objective units remain the historical default. The explicit mgda_l2 variant
uses unit-norm task directions without dropping zero gradients. A zero MGDA
direction is a legitimate result in either geometry.
"""
from __future__ import annotations

import itertools
import random

import numpy as np
import torch


# v2 replaces the standalone drawdown task with Calmar. This changes the
# gradient field and must reject continuation from the old four-task ABI.
MULTIOBJECTIVE_ABI = "annual_four_metric_calmar_raw_gram_surgery_v2"
MGDA_L2_ABI = "annual_four_metric_calmar_unit_l2_gram_surgery_v1"
FRONTIER_L2_ABI = "annual_four_metric_calmar_unit_l2_direction_mgda_length_v2"
FRONTIER_L2_METHODS = ("config_l2", "dibs_l2_1", "dibs_l2_5", "upgrad_log_l2")
TASK_NAMES = ("sharpe", "log_return", "sortino", "calmar")
TASK_OBJECTIVES = ("trajectory_sharpe", "log_utility", "trajectory_sortino", "trajectory_calmar")
MULTIOBJECTIVE_OBJECTIVES = frozenset(
    f"trajectory_{method}_{space}"
    for method in ("pcgrad", "mgda") for space in ("return", "parameter")
) | {"trajectory_mgda_l2_parameter"} | {
    f"trajectory_{method}_parameter" for method in FRONTIER_L2_METHODS
}


def surgery_spec(objective: str) -> tuple[str, str]:
    if objective not in MULTIOBJECTIVE_OBJECTIVES:
        raise ValueError(f"unsupported multiobjective method: {objective}")
    method, space = objective.removeprefix("trajectory_").rsplit("_", 1)
    return method, space


def multiobjective_contract(objective: str) -> dict:
    method, space = surgery_spec(objective)
    contract = dict(
        abi=MGDA_L2_ABI if method == "mgda_l2" else MULTIOBJECTIVE_ABI,
        objective=objective, method=method, space=space,
        tasks=list(TASK_OBJECTIVES), scaling=(
            "unit_global_l2_task_gradients_zero_tasks_retained"
            if method == "mgda_l2" else "raw_objective_units_no_normalization"),
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
    if method in FRONTIER_L2_METHODS:
        contract.update(
            abi=FRONTIER_L2_ABI,
            scaling="unit_global_l2_task_gradients_zero_tasks_retained",
            gradient_length="match_current_unit_mgda_norm_before_single_global_clip",
            stationary="unit_mgda_squared_gram_norm_at_most_64eps_scale_returns_unmodified_mgda_direction",
            direction=("equal_alignment_unit_gram_pseudoinverse_rcond_1e-12_mgda_fallback_on_conflict"
                       if method == "config_l2" else
                       "upgrad_preference_0_1_0_0_exact_dual_cone_projection_all_active_faces_kkt"
                       if method == "upgrad_log_l2" else
                       f"dibs_frozen_linear_oracles_radius1_alpha0.2_steps{method.rsplit('_', 1)[1]}"),
            common_descent=("certified_nonnegative_unit_products_or_mgda_fallback"
                            if method in {"config_l2", "upgrad_log_l2"} else
                            "not_guaranteed_per_step_record_all_actual_task_derivatives"),
        )
    return contract


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


def frontier_l2_coefficients(gram: np.ndarray, method: str) -> tuple[np.ndarray, dict[str, float]]:
    """Direction-only ConFIG/DiBS/UPGrad adaptations of the four-row Gram.

    ConFIG uses the least-norm equal-alignment solution U^+ 1 = U^T Q^+ 1.
    An inconsistent singular system can conflict; certify before accepting it.
    DiBS follows Algorithm 1 with epsilon=1 and alpha=.2<1/4. Its temporary
    displacement is -beta @ U, so all five inner steps require only Q=UU^T.
    Log-preferred UPGrad projects the log-return direction onto the dual cone.
    Neither loop evaluates another model or changes the four financial losses.

    Both directions are rescaled to the contemporaneous unit-MGDA norm. This
    is an explicit experimental length policy, not the papers' default length
    or a transfer of their convergence results to discrete financial fills.
    """
    reference = mgda_coefficients(gram)
    reference_norm2 = max(0., float(reference @ gram @ reference))
    info = dict(reference_mgda_norm=reference_norm2 ** .5,
                reference_mgda_kkt_margin=float(np.min(gram @ reference) - reference_norm2),
                direction_fallback_mgda=0., direction_near_stationary=0.)
    # Near cancellation, alpha^T Q alpha has absolute roundoff O(eps*|Q|)
    # even when (alpha @ U) is exactly zero. Taking its square root and using
    # it as a target length can manufacture a ~sqrt(eps) update. Keep the
    # original MGDA vector in this unresolved region; do not round it to zero
    # and do not relax the independent scale-invariance oracle.
    precision_floor = 64 * np.finfo(np.float64).eps * max(1., float(np.max(np.abs(gram))))
    info["reference_gram_precision_floor"] = precision_floor
    if reference_norm2 <= precision_floor:
        info["direction_near_stationary"] = 1.
        return reference, info
    if method == "config_l2":
        beta = np.linalg.pinv(gram, rcond=1e-12, hermitian=True) @ np.ones(len(gram))
        products = gram @ beta
        info["config_equal_alignment_residual"] = float(np.max(np.abs(products - 1.)))
        if (not np.isfinite(beta).all() or float(np.min(products)) < -1e-10):
            info["direction_fallback_mgda"] = 1.
            return reference, info
    elif method == "upgrad_log_l2":
        beta, margin = project_log_gradient_coefficients(gram)
        info["projection_kkt_residual"] = margin
    elif method in {"dibs_l2_1", "dibs_l2_5"}:
        steps = int(method.rsplit("_", 1)[1])
        beta = np.zeros(len(gram))
        for _ in range(steps):
            products = gram @ beta
            distances2 = float(beta @ products) - 2. * products + np.diag(gram)
            beta += .2 * np.sqrt(np.maximum(distances2, 0.))
        info["dibs_inner_steps"] = float(steps)
    else:
        raise ValueError(f"unsupported frontier method: {method}")
    norm2 = float(beta @ gram @ beta)
    if not np.isfinite(norm2) or norm2 <= 1e-24:
        info["direction_fallback_mgda"] = 1.
        return reference, info
    beta *= (reference_norm2 / norm2) ** .5
    if not np.isfinite(beta).all():
        raise FloatingPointError("non-finite normalized frontier coefficients")
    info["direction_min_unit_product"] = float(np.min(gram @ beta))
    info["direction_cosine_mgda"] = float(reference @ gram @ beta / reference_norm2)
    return beta, info


def project_log_gradient_coefficients(gram: np.ndarray) -> tuple[np.ndarray, float]:
    """Project the unit log gradient onto {d: U d >= 0}; 16 active faces.

    UPGrad with preference [0,1,0,0] projects just the log-return row. Its dual
    is min_{lambda>=0} .5 lambda^T Q lambda + lambda^T Q e_log.
    There is no ridge and no repeated projection-order dependence. Checking
    primal/dual feasibility and complementarity certifies the true projection.
    """
    anchor = np.eye(len(gram))[TASK_NAMES.index("log_return")]
    candidates = []
    for size in range(len(gram) + 1):
        for active in itertools.combinations(range(len(gram)), size):
            multipliers = np.zeros(len(gram))
            if active:
                ix = np.asarray(active)
                solution = np.linalg.lstsq(gram[np.ix_(ix, ix)], -(gram @ anchor)[ix], rcond=1e-12)[0]
                if np.min(solution) < -1e-9:
                    continue
                multipliers[ix] = np.maximum(solution, 0.)
            beta = anchor + multipliers
            products = gram @ beta
            residual = max(0., -float(np.min(products)),
                           float(np.max(np.abs(multipliers * products))))
            if np.isfinite(beta).all() and residual <= 1e-9:
                candidates.append((float(multipliers @ gram @ multipliers), beta, residual))
    if not candidates:
        raise FloatingPointError("log-anchor cone projection failed its KKT certificate")
    _, beta, residual = min(candidates, key=lambda row: row[0])
    return beta, residual


@torch.no_grad()
def combine_task_gradients(
    gradients: torch.Tensor, method: str, *, seed: int = 42, step: int = 0,
) -> tuple[torch.Tensor, dict[str, float]]:
    if (gradients.ndim != 2 or gradients.shape[0] != len(TASK_NAMES)
            or gradients.shape[1] == 0 or not gradients.is_floating_point()):
        raise ValueError("surgery requires four nonempty floating gradient rows")
    if method not in {"pcgrad", "mgda", "mgda_l2", *FRONTIER_L2_METHODS}:
        raise ValueError("unsupported gradient surgery method")
    # FP64 retains the raw, potentially very different objective scales.
    rows = gradients.double()
    gram = (rows @ rows.T).cpu().numpy()
    if not np.isfinite(gram).all():
        raise FloatingPointError("non-finite task gradient Gram matrix")
    gram = (gram + gram.T) * .5
    norms = np.sqrt(np.maximum(np.diag(gram), 0.))
    # Normalize only the tiny Gram and final coefficients. Do not allocate four
    # extra parameter vectors or transmit them to the host. An exactly zero
    # task stays a simplex vertex, preserving its stationary-point semantics.
    inverse_norms = np.ones_like(norms)
    solve_gram = gram
    normalized = method == "mgda_l2" or method in FRONTIER_L2_METHODS
    if normalized:
        np.divide(1., norms, out=inverse_norms, where=norms > 0.)
        inverse_norms[norms == 0.] = 0.
        solve_gram = gram * inverse_norms[:, None] * inverse_norms[None, :]
    frontier_info = {}
    if method in FRONTIER_L2_METHODS:
        simplex, frontier_info = frontier_l2_coefficients(solve_gram, method)
    else:
        simplex = (pcgrad_coefficients(solve_gram, seed=seed, step=step)
                   if method == "pcgrad" else mgda_coefficients(solve_gram))
    coefficients = simplex * inverse_norms
    mixed = torch.as_tensor(coefficients, device=rows.device) @ rows
    products = gram @ coefficients
    norm2 = max(0., float(coefficients @ products))
    solve_products = solve_gram @ simplex
    solve_norm2 = max(0., float(simplex @ solve_products))
    diagnostics = dict(combined_norm=norm2 ** .5,
                       conflict_pairs=float(sum(gram[i, j] < 0. for i in range(4) for j in range(i))),
                       zero_gradient_tasks=float(sum(norms == 0.)), **frontier_info)
    if method not in FRONTIER_L2_METHODS:
        diagnostics["mgda_kkt_margin"] = (float(np.min(solve_products) - solve_norm2)
                                           if method != "pcgrad" else 0.)
    for i, name in enumerate(TASK_NAMES):
        diagnostics[f"gradient_norm_{name}"] = float(norms[i])
        diagnostics[f"coefficient_{name}"] = float(simplex[i])
        if normalized:
            diagnostics[f"raw_coefficient_{name}"] = float(coefficients[i])
            diagnostics[f"gradient_scale_{name}"] = float(inverse_norms[i])
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
        self.mark_update_start()
        return diagnostics

    @torch.no_grad()
    def mark_update_start(self):
        """Snapshot the restored master parameters at the actual step boundary."""
        self.before = torch.cat([p.detach().reshape(-1) for p in self.parameters])

    @torch.no_grad()
    def observe_update(self) -> dict[str, float]:
        if self.before is None:
            raise RuntimeError("no combined update to observe")
        delta = torch.cat([p.detach().reshape(-1) for p in self.parameters]) - self.before
        products = (self.rows.double() @ delta.double()).cpu().tolist()
        return {f"actual_update_derivative_{name}": float(value)
                for name, value in zip(TASK_NAMES, products, strict=True)}
