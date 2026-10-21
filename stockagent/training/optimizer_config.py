"""Serializable optimizer identities; importing this module does not load CUDA."""
from __future__ import annotations

from copy import deepcopy
import math
from typing import Any, Mapping

DION_REVISION = "7692479288f928a3d61d1cc58137b4dbe4f71000"
SOAP_REVISION = "a1e553530fde97d0e6b307d7c82ac6d38b072340"
ADEMAMIX_REVISION = "eb517ed3bbc09227e5d062b682723543f6c71f1a"
OPTIMIZER_CONTRACT = "hidden_linear_optimizer_v1"
_AUX = {"aux_optimizer": "adamw", "aux_lr_scale": 1.0}
_DEFAULTS: dict[str, dict[str, Any]] = {
    "adamw": {},
    "sgd": {"momentum": 0.0, "dampening": 0.0, "nesterov": False},
    "muon": {**_AUX, "matrix_lr_scale": 1.0, "momentum": 0.95,
             "nesterov": True, "ns_steps": 5, "adjust_lr_fn": "match_rms_adamw"},
    "normuon": {**_AUX, "matrix_lr_scale": 1.0, "mu": 0.95,
                "muon_beta2": 0.95, "nesterov": True, "adjust_lr": "rms_norm",
                "use_polar_express": False},
    "dion3": {**_AUX, "matrix_lr_scale": 1.0, "mu": 0.95,
              "muon_beta2": 0.95, "fraction": 0.25, "adjust_lr": "rms_norm",
              "use_polar_express": False},
    "soap": {**_AUX, "matrix_lr_scale": 1.0, "betas": [0.95, 0.95],
             "precondition_frequency": 10, "max_precond_dim": 512},
    "ademamix": {"betas": [0.9, 0.999, 0.9999], "alpha": 2.0,
                 "beta3_warmup": None, "alpha_warmup": None},
}
_AUX_MIX = {"aux_betas": [0.9, 0.999, 0.9999], "aux_alpha": 2.0,
            "aux_beta3_warmup": None, "aux_alpha_warmup": None}


def normalized_optimizer_options(name: str, options: Mapping[str, Any]) -> dict[str, Any]:
    name = str(name).strip().lower()
    if name not in _DEFAULTS:
        raise ValueError(f"unsupported training.optimizer_name: {name!r}")
    if not isinstance(options, Mapping):
        raise ValueError("training.optimizer_options must be a mapping")
    defaults = deepcopy(_DEFAULTS[name])
    if options.get("aux_optimizer") == "ademamix":
        if name in {"adamw", "ademamix", "normuon", "dion3"}:
            raise ValueError(f"{name} does not support an AdEMAMix auxiliary optimizer")
        defaults.update(deepcopy(_AUX_MIX))
    unknown = set(options) - set(defaults)
    if unknown:
        raise ValueError(f"unsupported {name} optimizer options: {sorted(unknown)}")
    defaults.update(deepcopy(dict(options)))
    if defaults.get("aux_optimizer", "adamw") not in {"adamw", "ademamix"}:
        raise ValueError("aux_optimizer must be adamw or ademamix")
    for key, value in defaults.items():
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"optimizer option {key} must be finite")
        if key.endswith("lr_scale") and (isinstance(value, bool) or float(value) <= 0):
            raise ValueError(f"optimizer option {key} must be positive")
        if key in {"ns_steps", "precondition_frequency", "max_precond_dim"}:
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"optimizer option {key} must be a positive integer")
        if key in {"momentum", "mu", "muon_beta2"} and not 0 <= float(value) < 1:
            raise ValueError(f"optimizer option {key} must be in [0, 1)")
        if key == "dampening" and not 0 <= float(value) <= 1:
            raise ValueError("optimizer option dampening must be in [0, 1]")
        if key in {"betas", "aux_betas"}:
            expected = 3 if name == "ademamix" or key == "aux_betas" else 2
            if len(value) != expected or any(not 0 <= float(v) < 1 for v in value):
                raise ValueError(f"optimizer option {key} must contain {expected} betas in [0, 1)")
    if "fraction" in defaults and not 0 < float(defaults["fraction"]) <= 1:
        raise ValueError("Dion3 fraction must be in (0, 1]")
    if name == "sgd" and defaults["nesterov"] and (
            defaults["momentum"] <= 0 or defaults["dampening"] != 0):
        raise ValueError("SGD Nesterov requires positive momentum and zero dampening")
    return defaults


def optimizer_checkpoint_fields(training: Any) -> dict[str, Any]:
    name = str(getattr(training, "optimizer_name", "adamw")).lower()
    options = normalized_optimizer_options(name, getattr(training, "optimizer_options", {}))
    if name == "adamw":
        return {"name": "AdamW"}  # Preserve historical fingerprints exactly.
    revisions = {}
    if name in {"normuon", "dion3"}:
        revisions["microsoft/dion"] = DION_REVISION
    if name == "soap":
        revisions["nikhilvyas/SOAP"] = SOAP_REVISION
    if name == "ademamix" or options.get("aux_optimizer") == "ademamix":
        revisions["apple-aiml-research/ml-ademamix"] = ADEMAMIX_REVISION
    return {"name": name, "options": options, "contract": OPTIMIZER_CONTRACT,
            "implementation_revisions": revisions,
            "parameter_partition": "all_trainable" if name in {"ademamix", "sgd"} else "hidden_linear_modules"}


def normalized_sharpness_options(mode: str, radius: float, alpha: float) -> dict[str, Any]:
    mode = str(mode).strip().lower()
    if mode not in {"none", "sam", "asam", "xsam"}:
        raise ValueError("training.sharpness_mode must be none, sam, asam or xsam")
    if isinstance(radius, bool) or not math.isfinite(float(radius)) or float(radius) < 0:
        raise ValueError("training.sharpness_radius must be finite and nonnegative")
    if isinstance(alpha, bool) or not math.isfinite(float(alpha)) or not 0 <= float(alpha) <= 2:
        raise ValueError("training.sharpness_alpha must be finite and in [0, 2]")
    if (mode == "none") != (float(radius) == 0.):
        raise ValueError("sharpness requires positive radius; disabled sharpness requires radius zero")
    if mode != "xsam" and float(alpha) != 1.:
        raise ValueError("sharpness_alpha is consumed only by fixed-coefficient XSAM")
    return dict(mode=mode, radius=float(radius), alpha=float(alpha))


def sharpness_checkpoint_fields(training: Any) -> dict[str, Any]:
    options = normalized_sharpness_options(
        getattr(training, "sharpness_mode", "none"),
        getattr(training, "sharpness_radius", 0.),
        getattr(training, "sharpness_alpha", 1.))
    if options["mode"] == "none":
        return {}  # Preserve every historical optimizer fingerprint.
    return {"sharpness": dict(
        abi="complete_annual_two_pass_sharpness_v1", **options,
        adaptive_scale="abs_original_parameter_plus_0.01_all_coordinates",
        perturbation="global_unclipped_synchronized_gradient",
        update="restore_original_parameters_before_one_canonical_clip_and_optimizer_step",
        rng="replay_rank_local_pre_epoch_state_for_second_pass",
        xsam="fixed_alpha_spherical_interpolation_displacement_to_perturbed_gradient",
        xsam_degenerate="perturbed_gradient_for_parallel_or_antipodal_directions",
        multiobjective="perturb_along_combined_direction_then_recompute_all_tasks_at_perturbed_parameters",
        validation="unperturbed_exact_log_utility_no_test_selection")}
