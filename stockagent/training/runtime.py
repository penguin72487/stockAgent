"""Shared model runtime and safe checkpoint I/O.

This module is deliberately independent of the full trainer.  Training,
specialized runners, explainability, and live inference can therefore share the
same device, precision, model-call, and state-loading contracts without making
deployment import the complete training orchestration graph.
"""

from __future__ import annotations

from contextlib import nullcontext
import inspect
import os
from pathlib import Path, PosixPath
from typing import Any, Callable, Mapping

import torch
from torch import nn
from torch.amp import autocast
from torch.nn.parallel import DistributedDataParallel

from stockagent.config import ExperimentConfig


ProgressReporter = Callable[[str], None]


def resolve_device(config: ExperimentConfig) -> torch.device:
    """Resolve the configured device without silently changing execution."""
    requested = config.environment.device
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA was requested in config (environment.device=cuda), "
            "but torch.cuda.is_available() is False. "
            "Training is aborted to avoid silently falling back to CPU."
        )
    return torch.device(requested)


def resolve_amp_dtype(amp_dtype: str) -> torch.dtype | None:
    """Map the configured AMP mode to its compute dtype."""
    if amp_dtype == "bf16":
        return torch.bfloat16
    if amp_dtype == "fp16":
        return torch.float16
    if amp_dtype == "tf32":
        return None
    raise ValueError(f"Unsupported amp dtype: {amp_dtype}")


def autocast_context(device: torch.device, amp_dtype: torch.dtype | None):
    """Return the canonical CUDA autocast context for the resolved precision."""
    if device.type != "cuda" or amp_dtype is None:
        return nullcontext()
    return autocast(device_type="cuda", enabled=True, dtype=amp_dtype)


def unwrap_model(model: nn.Module) -> nn.Module:
    """Unwrap compile, StockAgent slab, and DDP wrappers exactly once."""
    target = model
    seen: set[int] = set()
    while True:
        if id(target) in seen:
            return target
        seen.add(id(target))
        next_target = getattr(target, "_orig_mod", None)
        if next_target is None and bool(
            getattr(target, "_stockagent_panel_slab_forward_wrapper", False)
        ):
            next_target = getattr(target, "model", None)
        if next_target is None and bool(
            getattr(target, "_stockagent_dynamic_symbol_panel_slab_wrapper", False)
        ):
            next_target = getattr(target, "model", None)
        if next_target is None and isinstance(target, DistributedDataParallel):
            next_target = getattr(target, "module", None)
        if next_target is None or next_target is target:
            return target
        target = next_target


def extract_weights_and_aux(
    model_output: torch.Tensor | dict[str, torch.Tensor] | tuple[Any, ...],
) -> tuple[torch.Tensor, dict[str, torch.Tensor] | None]:
    """Normalize supported model outputs to weights plus optional aux tensors."""
    if isinstance(model_output, dict):
        if "weights" not in model_output:
            raise ValueError("Model output dict must include 'weights'")
        return model_output["weights"], model_output
    if isinstance(model_output, tuple):
        weights = model_output[0]
        aux = (
            model_output[2]
            if len(model_output) >= 3 and isinstance(model_output[2], dict)
            else None
        )
        return weights, aux
    return model_output, None


def model_attention_mask(mask: torch.Tensor) -> torch.Tensor:
    """Give all-empty rows one model-only attention token.

    Callers retain the original trading mask for allocation, loss, and
    execution, so the dummy visibility cannot create a position or return.
    """
    mask_bool = mask.to(dtype=torch.bool)
    empty_rows = ~mask_bool.any(dim=-1, keepdim=True)
    return torch.cat(
        (mask_bool[..., :1] | empty_rows, mask_bool[..., 1:]),
        dim=-1,
    )


def _model_accepts_return_aux(model: nn.Module) -> bool:
    target = unwrap_model(model)
    cached = getattr(target, "_stockagent_accepts_return_aux", None)
    if cached is not None:
        return bool(cached)
    try:
        accepts = "return_aux" in inspect.signature(target.forward).parameters
    except (TypeError, ValueError):
        accepts = hasattr(target, "return_aux")
    try:
        setattr(target, "_stockagent_accepts_return_aux", bool(accepts))
    except Exception:
        pass
    return bool(accepts)


def _model_accepts_portfolio_context(model: nn.Module) -> bool:
    target = unwrap_model(model)
    cached = getattr(target, "_stockagent_accepts_portfolio_context", None)
    if cached is not None:
        return bool(cached)
    try:
        accepts = "portfolio_context" in inspect.signature(target.forward).parameters
    except (TypeError, ValueError):
        accepts = False
    try:
        setattr(target, "_stockagent_accepts_portfolio_context", bool(accepts))
    except Exception:
        pass
    return bool(accepts)


def call_model(
    model: nn.Module,
    x: torch.Tensor,
    mask: torch.Tensor,
    *,
    return_aux: bool | None = None,
    symbol_indices: torch.Tensor | None = None,
    portfolio_context: dict[str, torch.Tensor] | None = None,
) -> Any:
    """Call a StockAgent model through the canonical mask/aux ABI."""
    model_mask = model_attention_mask(mask)
    kwargs: dict[str, Any] = {}
    if symbol_indices is not None:
        kwargs["symbol_indices"] = symbol_indices
    if return_aux is not None and _model_accepts_return_aux(model):
        kwargs["return_aux"] = return_aux
    if portfolio_context is not None:
        if not _model_accepts_portfolio_context(model):
            raise ValueError(
                "model does not accept required derivative portfolio_context"
            )
        kwargs["portfolio_context"] = portfolio_context
    return model(x, model_mask, **kwargs)


def load_checkpoint(checkpoint_path: Path) -> dict[str, Any]:
    """Load a tensor checkpoint without enabling arbitrary pickle globals."""
    with torch.serialization.safe_globals([PosixPath]):
        checkpoint = torch.load(
            checkpoint_path,
            map_location="cpu",
            weights_only=True,
        )
    if not isinstance(checkpoint, dict):
        raise TypeError(
            f"Checkpoint must contain a mapping, got {type(checkpoint).__name__}: "
            f"{checkpoint_path}"
        )
    return checkpoint


def _env_truthy(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in {
        "1",
        "true",
        "on",
        "yes",
    }


def load_model_state_dict(
    model: nn.Module,
    state_dict: Mapping[str, Any],
    *,
    strict_no_fallback: bool | None = None,
    progress: ProgressReporter | None = None,
) -> None:
    """Load model state with wrapper cleanup and explicit legacy compatibility."""

    def strip_wrapper_prefixes(key: str) -> str:
        cleaned = str(key)
        while True:
            previous = cleaned
            for prefix in ("module.", "_orig_mod."):
                if cleaned.startswith(prefix):
                    cleaned = cleaned[len(prefix) :]
                    break
            if cleaned == previous:
                return cleaned

    cleaned_state_dict = {
        strip_wrapper_prefixes(key): value for key, value in state_dict.items()
    }
    target = unwrap_model(model)
    legacy_dynamic_keys = any(
        key.startswith(("dynamic_latent_generator.", "dynamic_market_generator."))
        for key in cleaned_state_dict
    )
    if legacy_dynamic_keys:
        compatibility_loader = getattr(
            target,
            "enable_legacy_dynamic_token_checkpoint_compatibility",
            None,
        )
        if compatibility_loader is None:
            raise RuntimeError(
                "Checkpoint contains legacy dynamic-token weights, but the target model "
                "does not support their strict inference reconstruction."
            )
        compatibility_loader(cleaned_state_dict)
    try:
        target.load_state_dict(cleaned_state_dict)
        return
    except RuntimeError as exc:
        message = str(exc)
        if not (
            hasattr(target, "forward_from_panel") and "Unexpected key(s)" in message
        ):
            raise
        strict = (
            _env_truthy("STOCKAGENT_STRICT_NO_FALLBACK", "0")
            if strict_no_fallback is None
            else bool(strict_no_fallback)
        )
        if strict:
            raise RuntimeError(
                "Checkpoint state_dict is not strictly compatible with the model; "
                "strict_no_fallback=true so strict=False checkpoint loading is disabled."
            ) from exc

    incompatible = target.load_state_dict(cleaned_state_dict, strict=False)
    allowed_prefixes = (
        "cross_blocks.",
        "joint_blocks.",
        "latent_queries",
        "market_queries",
        "temporal_pool_score.",
        "latent_blocks.",
        "market_blocks.",
        "stock_read_latent_blocks.",
        "stock_read_market_blocks.",
    )
    unexpected = list(getattr(incompatible, "unexpected_keys", []))
    missing = list(getattr(incompatible, "missing_keys", []))
    disallowed_unexpected = [
        key
        for key in unexpected
        if not any(key.startswith(prefix) for prefix in allowed_prefixes)
    ]
    if missing or disallowed_unexpected:
        details = []
        if missing:
            details.append(f"missing={missing[:8]}")
        if disallowed_unexpected:
            details.append(f"unexpected={disallowed_unexpected[:8]}")
        raise RuntimeError(
            "Checkpoint is incompatible with model state_dict: " + ", ".join(details)
        )
    if unexpected:
        reporter = print if progress is None else progress
        reporter(
            "Loaded checkpoint with strict=False; ignored unused "
            "TransformerBasePortfolioModel keys: "
            + ", ".join(unexpected[:8])
            + (" ..." if len(unexpected) > 8 else "")
        )


__all__ = [
    "autocast_context",
    "call_model",
    "extract_weights_and_aux",
    "load_checkpoint",
    "load_model_state_dict",
    "model_attention_mask",
    "resolve_amp_dtype",
    "resolve_device",
    "unwrap_model",
]
