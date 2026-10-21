"""Opt-in research optimizers behind the canonical trainer's single step."""
from __future__ import annotations

import importlib.metadata
import json
from collections import defaultdict
from typing import Any

import torch
from torch import nn

from stockagent.training.optimizer_config import DION_REVISION, normalized_optimizer_options

# Select module roles, not tensor rank: queries, embeddings, input projections
# and final action/cash heads stay on the auxiliary optimizer. Packed attention
# QKV and SwiGLU matrices retain their existing grouping in this v1 experiment.
_HIDDEN_ROOTS = (
    "temporal_blocks", "cross_blocks", "joint_blocks", "latent_blocks",
    "market_blocks", "stock_read_latent_blocks", "stock_read_market_blocks",
    "daily_context_blocks",
)


def partition_hidden_linear_parameters(model: nn.Module) -> tuple[list[nn.Parameter], list[nn.Parameter], dict[str, Any]]:
    while hasattr(model, "module") or hasattr(model, "_orig_mod"):
        model = getattr(model, "module", getattr(model, "_orig_mod", model))
    selected: set[int] = set()
    for path, module in model.named_modules():
        if path.split(".", 1)[0] in _HIDDEN_ROOTS and isinstance(module, nn.Linear):
            if module.weight.requires_grad and module.weight.ndim == 2:
                selected.add(id(module.weight))
    matrix, aux, manifest = [], [], {"matrix": [], "auxiliary": []}
    for name, parameter in model.named_parameters():
        if not parameter.requires_grad:
            continue
        group = "matrix" if id(parameter) in selected else "auxiliary"
        (matrix if group == "matrix" else aux).append(parameter)
        manifest[group].append({"name": name, "shape": list(parameter.shape), "numel": parameter.numel()})
    if not matrix or not aux:
        raise ValueError("hidden-linear optimizer requires nonempty matrix and auxiliary groups")
    return matrix, aux, manifest


class CombinedOptimizer(torch.optim.Optimizer):
    """One scheduler/scaler/checkpoint owner for disjoint child optimizers.

    Every child uses the same state mapping and group objects. Scheduler LR
    mutations and device migration therefore reach the actual step owners.
    """
    def __init__(self, children: list[torch.optim.Optimizer]):
        self.children = children
        self._group_counts = [len(child.param_groups) for child in children]
        groups = [group for child in children for group in child.param_groups]
        ids = [id(p) for group in groups for p in group["params"]]
        if len(ids) != len(set(ids)):
            raise ValueError("combined optimizer parameter groups overlap")
        super().__init__(groups, {"lr": groups[0]["lr"]})
        self.state = defaultdict(dict)
        for child in children:
            self.state.update(child.state)
        self._bind_children()

    def _bind_children(self) -> None:
        offset = 0
        for child, count in zip(self.children, self._group_counts):
            child.param_groups = self.param_groups[offset:offset + count]
            child.state = self.state
            offset += count

    @torch.no_grad()
    def step(self, closure=None):
        loss = None
        if closure is not None:
            with torch.enable_grad():
                loss = closure()
        for child in self.children:
            child.step()
        return loss

    def state_dict(self):
        result = super().state_dict()
        result["combined_optimizer_group_counts"] = self._group_counts
        return result

    def load_state_dict(self, state_dict):
        if state_dict.get("combined_optimizer_group_counts") != self._group_counts:
            raise ValueError("incompatible combined optimizer partition")
        super().load_state_dict(state_dict)
        self._bind_children()


def _adamw(parameters, *, lr: float, weight_decay: float, device: torch.device):
    if device.type == "cuda":
        try:
            return torch.optim.AdamW(parameters, lr=lr, weight_decay=weight_decay, fused=True)
        except TypeError:
            pass
    return torch.optim.AdamW(parameters, lr=lr, weight_decay=weight_decay)


def _ademamix(parameters, *, lr: float, weight_decay: float, options: dict[str, Any], prefix: str = ""):
    from stockagent.training.vendor.ademamix import AdEMAMix
    return AdEMAMix(parameters, lr=lr, weight_decay=weight_decay,
                    betas=tuple(options[prefix + "betas"]), alpha=options[prefix + "alpha"],
                    beta3_warmup=options[prefix + "beta3_warmup"],
                    alpha_warmup=options[prefix + "alpha_warmup"])


def create_frontier_optimizer(model: nn.Module, training: Any, device: torch.device) -> torch.optim.Optimizer:
    name = training.optimizer_name
    opts = normalized_optimizer_options(name, training.optimizer_options)
    lr, wd = float(training.learning_rate), float(training.weight_decay)
    if name == "sgd":
        parameters = [p for p in model.parameters() if p.requires_grad]
        return torch.optim.SGD(parameters, lr=lr, weight_decay=wd, **opts,
                               **({"fused": True} if device.type == "cuda" else {"foreach": True}))
    if name == "ademamix":
        return _ademamix([p for p in model.parameters() if p.requires_grad], lr=lr, weight_decay=wd, options=opts)
    matrix, aux, manifest = partition_hidden_linear_parameters(model)
    matrix_lr, aux_lr = lr * opts["matrix_lr_scale"], lr * opts["aux_lr_scale"]
    if name in {"normuon", "dion3"}:
        distribution = importlib.metadata.distribution("dion")
        provenance = json.loads(distribution.read_text("direct_url.json") or "{}")
        if provenance.get("vcs_info", {}).get("commit_id") != DION_REVISION:
            raise RuntimeError("Dion optimizer package differs from the pinned research revision")
        from dion import Dion3, NorMuon
        cls = NorMuon if name == "normuon" else Dion3
        algorithm = "normuon" if name == "normuon" else "nordion2"
        groups = [{"params": matrix, "algorithm": algorithm, "lr": matrix_lr},
                  {"params": aux, "algorithm": "adamw", "lr": aux_lr, "beta1": 0.9, "beta2": 0.999}]
        kwargs = {k: opts[k] for k in ("mu", "muon_beta2", "adjust_lr", "use_polar_express")}
        kwargs.update({"nesterov": opts["nesterov"]} if name == "normuon" else {"fraction": opts["fraction"]})
        optimizer = cls(groups, distributed_mesh=None, lr=lr, weight_decay=wd,
                        betas=(0.9, 0.999), use_triton=False, **kwargs)
    else:
        if name == "muon":
            cls = getattr(torch.optim, "Muon", None)
            if cls is None:
                raise RuntimeError("this experiment requires torch.optim.Muon")
            primary = cls(matrix, lr=matrix_lr, weight_decay=wd,
                          **{k: opts[k] for k in ("momentum", "nesterov", "ns_steps", "adjust_lr_fn")})
        elif name == "soap":
            from stockagent.training.vendor.soap import SOAP
            primary = SOAP(matrix, lr=matrix_lr, weight_decay=wd, betas=tuple(opts["betas"]),
                           precondition_frequency=opts["precondition_frequency"],
                           max_precond_dim=opts["max_precond_dim"])
        else:
            raise ValueError(f"unsupported frontier optimizer: {name}")
        auxiliary = (_ademamix(aux, lr=aux_lr, weight_decay=wd, options=opts, prefix="aux_")
                     if opts["aux_optimizer"] == "ademamix"
                     else _adamw(aux, lr=aux_lr, weight_decay=wd, device=device))
        optimizer = CombinedOptimizer([primary, auxiliary])
    optimizer.parameter_partition_manifest = manifest
    return optimizer
