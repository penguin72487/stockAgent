"""Two complete annual passes around the canonical gradient/optimizer boundary.

SAM/ASAM use the perturbed gradient. Fixed-alpha XSAM interpolates (or
extrapolates) between the actual ascent displacement and that gradient. There
is no line search, extra optimizer, persistent state or alternative ledger.
"""
from __future__ import annotations

import time
from dataclasses import fields

import torch

from stockagent.training.optimizer_config import normalized_sharpness_options


class SharpnessPerturbation:
    """Keep an exact master-parameter backup, including parameters unused later."""

    def __init__(self, parameters, *, mode: str, radius: float, alpha: float = 1.):
        self.options = normalized_sharpness_options(mode, radius, alpha)
        if self.options["mode"] == "none":
            raise ValueError("a disabled method must bypass the perturbation context")
        self.parameters = tuple(p for p in parameters if p.requires_grad)
        if not self.parameters or len({(p.device, p.dtype) for p in self.parameters}) != 1:
            raise ValueError("sharpness requires one device/dtype of master parameters")
        self.sizes = tuple(p.numel() for p in self.parameters)
        self.original = torch.cat([p.detach().reshape(-1) for p in self.parameters])
        self.restored = True
        self.finished = False
        self.diagnostics = {}

    def _views(self, flat):
        return [part.view_as(p) for part, p in zip(flat.split(self.sizes), self.parameters, strict=True)]

    def _gradient(self):
        if any(p.grad is not None and p.grad.is_sparse for p in self.parameters):
            raise ValueError("sharpness requires dense gradients")
        return torch.cat([torch.zeros_like(p).reshape(-1) if p.grad is None
                          else p.grad.detach().reshape(-1) for p in self.parameters])

    @torch.no_grad()
    def perturb(self):
        gradient = self._gradient()
        if not bool(torch.isfinite(gradient).all()):
            raise FloatingPointError("non-finite unperturbed sharpness gradient")
        self.first_present = tuple(p.grad is not None for p in self.parameters)
        scale = self.original.abs().add(.01) if self.options["mode"] == "asam" else 1.
        norm = torch.linalg.vector_norm(gradient * scale)
        if not bool(torch.isfinite(norm)):
            raise FloatingPointError("non-finite sharpness perturbation norm")
        safe_norm = norm.clamp_min(torch.finfo(gradient.dtype).tiny)
        factor = torch.where(norm > 0., self.options["radius"] / safe_norm, 0.)
        self.displacement = (gradient * scale * scale) * factor
        if not bool(torch.isfinite(self.displacement).all()):
            raise FloatingPointError("non-finite sharpness displacement")
        self.restored = False
        torch._foreach_add_(list(self.parameters), self._views(self.displacement))
        self.diagnostics.update(
            sharpness_base_gradient_norm=float(torch.linalg.vector_norm(gradient)),
            sharpness_metric_gradient_norm=float(norm),
            sharpness_displacement_l2=float(torch.linalg.vector_norm(self.displacement)),
            sharpness_zero_direction=float(norm == 0.))

    @torch.no_grad()
    def restore(self):
        if not self.restored:
            torch._foreach_copy_(list(self.parameters), self._views(self.original))
            self.restored = True

    @torch.no_grad()
    def before_update(self):
        if self.finished:
            raise RuntimeError("sharpness restoration callback ran more than once")
        gradient = self._gradient()
        if not bool(torch.isfinite(gradient).all()):
            raise FloatingPointError("non-finite perturbed sharpness gradient")
        norm = torch.linalg.vector_norm(gradient)
        displacement_norm = torch.linalg.vector_norm(self.displacement)
        if not bool(torch.isfinite(norm)):
            raise FloatingPointError("non-finite perturbed sharpness gradient norm")
        tiny = torch.finfo(gradient.dtype).tiny
        v0 = self.displacement / displacement_norm.clamp_min(tiny)
        v1 = gradient / norm.clamp_min(tiny)
        cosine = torch.dot(v0, v1).clamp(-1., 1.)
        self.diagnostics.update(sharpness_perturbed_gradient_norm=float(norm),
                                sharpness_direction_cosine=float(cosine))
        if self.options["mode"] == "xsam":
            # The fixed-alpha ablation of XSAM, not its expensive dynamic
            # alpha search. Near parallel/antipodal vectors have no stable
            # spherical interpolation plane; retain the perturbed gradient.
            theta = torch.acos(cosine)
            sine = torch.sin(theta)
            degenerate = (sine.abs() <= 1e-6) | (norm == 0.) | (displacement_norm == 0.)
            denominator = sine.abs().clamp_min(1e-6)
            alpha = self.options["alpha"]
            mixed = norm * ((torch.sin((1. - alpha) * theta) / denominator) * v0
                            + (torch.sin(alpha * theta) / denominator) * v1)
            mixed = torch.where(degenerate, gradient, mixed)
            present = [first or p.grad is not None
                       for first, p in zip(self.first_present, self.parameters, strict=True)]
            for p, part, active in zip(self.parameters, self._views(mixed), present, strict=True):
                p.grad = part if active else None
            self.diagnostics["sharpness_xsam_degenerate"] = float(degenerate)
        self.restore()
        self.finished = True


def run_sharpness_aware_epoch(model, run_epoch, *, mode, radius, alpha,
                             capture_rng_state, restore_rng_state):
    """One real optimizer/scheduler step; the first pass captures gradients only.

    ``run_epoch`` owns the unchanged complete financial forward/backward and
    accepts gradient_only / gradient_transform at its existing final boundary.
    The second pass replays the rank-local RNG so stochastic masks agree.
    """
    options = normalized_sharpness_options(mode, radius, alpha)
    if options["mode"] == "none":
        return run_epoch()
    started = time.perf_counter()
    perturbation = SharpnessPerturbation(model.parameters(), **options)
    rng = capture_rng_state()
    try:
        base_loss, base_timing = run_epoch(gradient_only=True)
        perturbation.perturb()
        if not restore_rng_state(rng, rank_local=True):
            raise RuntimeError("sharpness could not restore its rank-local RNG")
        shifted_loss, timing = run_epoch(gradient_transform=perturbation.before_update)
        if not perturbation.finished:
            raise RuntimeError("sharpness epoch did not restore before the optimizer boundary")
    finally:
        perturbation.restore()
    # Include both complete passes in all stage times and actual batch counts.
    # Gradient/optimizer diagnostics belong to the one accepted second pass.
    for item in fields(timing):
        if item.name.endswith("_s"):
            setattr(timing, item.name, getattr(timing, item.name) + getattr(base_timing, item.name))
    timing.cuda_events.extend(base_timing.cuda_events)
    for name in ("batches", "deferred_validation_checks", "deferred_validation_retries",
                 "resident_input_cache_hits", "resident_input_cache_misses"):
        setattr(timing, name, getattr(timing, name) + getattr(base_timing, name))
    timing.return_adjoint_replay_max_abs_error = max(
        timing.return_adjoint_replay_max_abs_error, base_timing.return_adjoint_replay_max_abs_error)
    timing.multiobjective_diagnostics.update(perturbation.diagnostics,
        sharpness_radius=options["radius"], sharpness_alpha=options["alpha"],
        sharpness_base_loss=float(base_loss), sharpness_perturbed_loss=float(shifted_loss),
        sharpness_base_vjp_passes=base_timing.multiobjective_diagnostics.get("vjp_passes", 1.),
        sharpness_total_vjp_passes=(base_timing.multiobjective_diagnostics.get("vjp_passes", 1.)
                                   + timing.multiobjective_diagnostics.get("vjp_passes", 1.)))
    timing.total_s = time.perf_counter() - started
    return base_loss, timing
