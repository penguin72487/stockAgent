#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import math
import os
import sys
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import torch
from torch import nn
from torch.amp import GradScaler, autocast
import torch.nn.functional as F


def _ensure_transformer_engine_cuda_include_env() -> None:
    if os.environ.get("NVTE_CUDA_INCLUDE_DIR"):
        return
    env_root = Path(sys.executable).resolve().parents[1]
    candidates = [
        env_root / "targets" / "x86_64-linux" / "include",
        env_root / "include",
        Path("/usr/local/cuda/include"),
        Path("/usr/include"),
    ]
    for candidate in candidates:
        if (candidate / "cuda_runtime.h").exists():
            os.environ.setdefault("NVTE_CUDA_INCLUDE_DIR", str(candidate))
            return


_ensure_transformer_engine_cuda_include_env()

try:
    import transformer_engine.pytorch as te
    from transformer_engine.common.recipe import Float8CurrentScaling, NVFP4BlockScaling
except Exception:  # noqa: BLE001 - optional precision backend.
    te = None
    Float8CurrentScaling = None
    NVFP4BlockScaling = None

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.models.transformer_base_portfolio import TransformerBasePortfolioModel
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import _extract_weights_and_aux


RUNNABLE_TRAIN_MODES = {"fp32", "tf32", "fp16", "bf16", "fp8", "fp4", "nf4", "int8"}
DEFAULT_MODES = ["fp32", "tf32", "fp16", "bf16", "fp8", "fp4", "nf4", "int8"]


@dataclass(frozen=True)
class PrecisionPlan:
    name: str
    runnable: bool
    autocast_dtype: torch.dtype | None
    allow_tf32: bool
    use_grad_scaler: bool
    transformer_engine: bool
    native_backend: str | None
    compute_label: str
    effective_weight_bits: float
    native_amp: bool
    reason: str


def precision_plan(mode: str) -> PrecisionPlan:
    normalized = mode.strip().lower()
    if normalized == "fp32":
        return PrecisionPlan(
            name="fp32",
            runnable=True,
            autocast_dtype=None,
            allow_tf32=False,
            use_grad_scaler=False,
            transformer_engine=False,
            native_backend=None,
            compute_label="fp32",
            effective_weight_bits=32.0,
            native_amp=True,
            reason="FP32 baseline; TF32 disabled for the strict reference path.",
        )
    if normalized == "tf32":
        return PrecisionPlan(
            name="tf32",
            runnable=True,
            autocast_dtype=None,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=False,
            native_backend=None,
            compute_label="tf32 matmul with fp32 storage",
            effective_weight_bits=32.0,
            native_amp=True,
            reason="FP32 storage with CUDA TF32 Tensor Core matmul enabled.",
        )
    if normalized == "fp16":
        return PrecisionPlan(
            name="fp16",
            runnable=True,
            autocast_dtype=torch.float16,
            allow_tf32=True,
            use_grad_scaler=True,
            transformer_engine=False,
            native_backend=None,
            compute_label="fp16 amp",
            effective_weight_bits=16.0,
            native_amp=True,
            reason="CUDA AMP FP16; GradScaler is enabled.",
        )
    if normalized == "bf16":
        return PrecisionPlan(
            name="bf16",
            runnable=True,
            autocast_dtype=torch.bfloat16,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=False,
            native_backend=None,
            compute_label="bf16 amp",
            effective_weight_bits=16.0,
            native_amp=True,
            reason="CUDA AMP BF16; GradScaler remains disabled.",
        )
    if normalized == "fp8":
        return PrecisionPlan(
            name="fp8",
            runnable=True,
            autocast_dtype=torch.bfloat16,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=True,
            native_backend="transformer_engine_fp8",
            compute_label="Transformer Engine native FP8 Linear where shape-supported; BF16 fallback otherwise",
            effective_weight_bits=8.0,
            native_amp=False,
            reason=(
                "Native mode uses Transformer Engine FP8 kernels with dequantized backward for supported Linear shapes. "
                "Layers with unsupported TE shape constraints remain high precision and are counted as fallback calls."
            ),
        )
    if normalized == "fp4":
        return PrecisionPlan(
            name="fp4",
            runnable=True,
            autocast_dtype=torch.bfloat16,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=True,
            native_backend="transformer_engine_nvfp4",
            compute_label="Transformer Engine native NVFP4 Linear where shape-supported; BF16 fallback otherwise",
            effective_weight_bits=4.0,
            native_amp=False,
            reason=(
                "Native mode uses Transformer Engine NVFP4 kernels for supported Linear shapes. "
                "Layers with unsupported TE shape constraints remain high precision and are counted as fallback calls."
            ),
        )
    if normalized == "nf4":
        return PrecisionPlan(
            name="nf4",
            runnable=False,
            autocast_dtype=None,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=False,
            native_backend=None,
            compute_label="unsupported native training",
            effective_weight_bits=4.0,
            native_amp=False,
            reason=(
                "NF4 packages can be installed, but this benchmark requires a native CUDA autograd "
                "training path for the full project hotpath. bitsandbytes NF4 is a quantized Linear "
                "path commonly used for inference or adapter fine-tuning, and PyTorch/Transformer Engine "
                "do not provide NF4 full-parameter autograd training here."
            ),
        )
    if normalized == "int8":
        return PrecisionPlan(
            name="int8",
            runnable=False,
            autocast_dtype=None,
            allow_tf32=True,
            use_grad_scaler=False,
            transformer_engine=False,
            native_backend=None,
            compute_label="unsupported native training",
            effective_weight_bits=8.0,
            native_amp=False,
            reason=(
                "PyTorch native INT8 Linear is an inference quantized op, not a CUDA autograd training path for this model."
            ),
        )
    raise ValueError(f"Unknown precision mode: {mode}")


def _parse_modes(value: str) -> list[str]:
    modes = [part.strip().lower() for part in value.split(",") if part.strip()]
    return modes or list(DEFAULT_MODES)


def _sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _gb(num_bytes: int) -> float:
    return float(num_bytes) / 1024**3


def _autocast_context(device: torch.device, dtype: torch.dtype | None):
    if device.type != "cuda" or dtype is None:
        return nullcontext()
    return autocast(device_type="cuda", enabled=True, dtype=dtype)


class TransformerEngineLinearCompat(nn.Module):
    def __init__(self, source: nn.Linear, backend: str) -> None:
        super().__init__()
        if te is None:
            raise RuntimeError("transformer_engine is not available")
        self.in_features = int(source.in_features)
        self.out_features = int(source.out_features)
        self.backend = str(backend)
        self.te_linear = te.Linear(
            self.in_features,
            self.out_features,
            bias=source.bias is not None,
            params_dtype=source.weight.dtype,
            device=source.weight.device,
        )
        with torch.no_grad():
            self.te_linear.weight.copy_(source.weight)
            if source.bias is not None and self.te_linear.bias is not None:
                self.te_linear.bias.copy_(source.bias)
        self.native_calls = 0
        self.fallback_calls = 0

    @property
    def weight(self) -> torch.nn.Parameter:
        return self.te_linear.weight

    @property
    def bias(self) -> torch.nn.Parameter | None:
        return self.te_linear.bias

    def _shape_can_use_native(self, x: torch.Tensor) -> bool:
        if x.dim() == 0:
            return False
        leading = int(x.numel() // max(1, x.shape[-1]))
        if not x.is_cuda or x.shape[-1] % 16 != 0:
            return False
        if self.backend == "transformer_engine_nvfp4":
            return leading % 32 == 0 and self.out_features % 32 == 0
        return leading % 8 == 0 and self.out_features % 8 == 0

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if self._shape_can_use_native(x):
            try:
                self.native_calls += 1
                return self.te_linear(x)
            except Exception:  # noqa: BLE001 - runtime shape/backend fallback.
                self.fallback_calls += 1
        else:
            self.fallback_calls += 1
        return F.linear(x, self.te_linear.weight, self.te_linear.bias)


def _replace_linear_modules(module: nn.Module, plan: PrecisionPlan) -> dict[str, int]:
    stats = {"te_linear": 0, "linear_total": 0}
    for name, child in list(module.named_children()):
        if isinstance(child, nn.Linear):
            stats["linear_total"] += 1
            if plan.transformer_engine and te is not None and child.in_features % 16 == 0:
                setattr(module, name, TransformerEngineLinearCompat(child, plan.native_backend or "transformer_engine"))
                stats["te_linear"] += 1
                continue
        else:
            child_stats = _replace_linear_modules(child, plan)
            for key, value in child_stats.items():
                stats[key] = stats.get(key, 0) + int(value)
    return stats


def _precision_module_stats(module: nn.Module) -> dict[str, int]:
    stats = {
        "te_linear": 0,
        "native_low_precision_calls": 0,
        "high_precision_fallback_calls": 0,
        "linear_total": 0,
    }
    for child in module.modules():
        if isinstance(child, TransformerEngineLinearCompat):
            stats["te_linear"] += 1
            stats["linear_total"] += 1
            stats["native_low_precision_calls"] += int(child.native_calls)
            stats["high_precision_fallback_calls"] += int(child.fallback_calls)
        elif isinstance(child, nn.Linear):
            stats["linear_total"] += 1
    return stats


def _parameter_stats(module: nn.Module, plan: PrecisionPlan) -> dict[str, float | int]:
    total_params = sum(int(p.numel()) for p in module.parameters())
    train_storage_bytes = sum(int(p.numel() * p.element_size()) for p in module.parameters())
    packed_bits = float(plan.effective_weight_bits)
    packed_weight_bytes = total_params * packed_bits / 8.0
    return {
        "parameter_count": total_params,
        "training_parameter_storage_mb": round(train_storage_bytes / 1024**2, 4),
        "estimated_packed_weight_storage_mb": round(packed_weight_bytes / 1024**2, 4),
        "effective_weight_bits": packed_bits,
    }


def _te_recipe(plan: PrecisionPlan):
    if plan.native_backend == "transformer_engine_fp8":
        if Float8CurrentScaling is None:
            raise RuntimeError("Transformer Engine FP8 recipe is unavailable")
        return Float8CurrentScaling(backward_override="dequantized")
    if plan.native_backend == "transformer_engine_nvfp4":
        if NVFP4BlockScaling is None:
            raise RuntimeError("Transformer Engine NVFP4 recipe is unavailable")
        return NVFP4BlockScaling(disable_rht=True, disable_stochastic_rounding=True)
    return None


def _precision_context(device: torch.device, plan: PrecisionPlan):
    fp8_ctx = (
        te.fp8_autocast(enabled=True, fp8_recipe=_te_recipe(plan))
        if plan.transformer_engine and te is not None and device.type == "cuda"
        else nullcontext()
    )
    autocast_ctx = _autocast_context(device, plan.autocast_dtype)
    return _NestedContext(fp8_ctx, autocast_ctx)


class _NestedContext:
    def __init__(self, outer, inner) -> None:
        self.outer = outer
        self.inner = inner

    def __enter__(self):
        self.outer.__enter__()
        return self.inner.__enter__()

    def __exit__(self, exc_type, exc, tb):
        inner_result = self.inner.__exit__(exc_type, exc, tb)
        outer_result = self.outer.__exit__(exc_type, exc, tb)
        return bool(inner_result or outer_result)


def _make_model(args: argparse.Namespace, device: torch.device) -> TransformerBasePortfolioModel:
    return TransformerBasePortfolioModel(
        lookback=int(args.lookback),
        num_features=int(args.features),
        num_symbols=int(args.symbols),
        d_model=int(args.d_model),
        attention_mode=str(args.attention_mode),
        use_flash_attention=True,
        use_time_pos=True,
        use_symbol_pos=True,
        input_dropout=0.0,
        sdpa_batch_limit=int(args.sdpa_batch_limit),
        norm_type="rmsnorm",
        ffn_type="swiglu",
        qk_norm=True,
        rope_temporal=True,
        rope_base=10000.0,
        temporal_layers=int(args.temporal_layers),
        temporal_heads=int(args.heads),
        temporal_ffn_mult=2,
        temporal_pooling=str(args.temporal_pooling),
        cross_layers=1,
        cross_heads=int(args.heads),
        cross_ffn_mult=2,
        joint_layers=2,
        joint_heads=int(args.heads),
        joint_ffn_mult=2,
        latent_layers=1,
        num_latent_factors=16,
        num_market_tokens=4,
        market_layers=1,
        dynamic_latent_tokens=True,
        dynamic_market_tokens=True,
        dynamic_token_hidden_mult=2,
        dynamic_token_gate_init=0.1,
        dynamic_token_dropout=0.1,
        head_hidden_dim=int(args.d_model),
        head_layers=1,
        dropout=0.2,
        default_temperature=1.0,
        portfolio_mode="long_short",
        max_full_tokens=16384,
        checkpoint_blocks=False,
        return_aux=True,
        return_aux_details=False,
        runtime_shape_check=True,
        allow_dynamic_symbols=False,
    ).to(device)


def _make_batch(args: argparse.Namespace, device: torch.device) -> dict[str, torch.Tensor]:
    generator = torch.Generator(device="cpu").manual_seed(int(args.seed) + 17)
    batch = int(args.batch_size)
    lookback = int(args.lookback)
    symbols = int(args.symbols)
    features = int(args.features)
    x = torch.randn(batch, lookback, symbols, features, generator=generator, dtype=torch.float32)
    future_log_returns = torch.randn(batch, symbols, generator=generator, dtype=torch.float32) * 0.015
    tradable_mask = torch.ones(batch, symbols, dtype=torch.bool)
    if symbols >= 8:
        tradable_mask[1::2, -max(1, symbols // 16) :] = False
    benchmark = (future_log_returns * tradable_mask.to(dtype=future_log_returns.dtype)).sum(dim=1)
    benchmark = benchmark / tradable_mask.sum(dim=1).clamp_min(1).to(dtype=future_log_returns.dtype)
    can_buy_mask = tradable_mask.clone()
    can_sell_mask = tradable_mask.clone()
    sample_mask = torch.ones(batch, dtype=torch.bool)
    return {
        "x": x.to(device),
        "future_log_returns": future_log_returns.to(device),
        "tradable_mask": tradable_mask.to(device),
        "benchmark": benchmark.to(device),
        "can_buy_mask": can_buy_mask.to(device),
        "can_sell_mask": can_sell_mask.to(device),
        "sample_mask": sample_mask.to(device),
    }


def _loss_kwargs() -> dict[str, Any]:
    return {
        "long_only": False,
        "buy_fee_rate": 0.001425,
        "sell_fee_rate": 0.004425,
        "max_turnover_ratio": 0.0,
        "gross_leverage": 1.0,
        "gamma_sharpe": 1.0,
        "gamma_excess": 1.0,
        "gamma_cvar": 1.0,
        "cvar_alpha": 0.95,
        "gamma_drawdown": 0.0,
        "drawdown_target": 0.2,
        "gamma_turnover": 0.0,
        "gamma_underperformance": 1.0,
        "excess_target": 0.0,
        "cvar_budget": 0.03,
        "drawdown_budget": 0.2,
        "turnover_budget": 0.3,
        "gamma_cvar_budget": 1.0,
        "gamma_drawdown_budget": 1.0,
        "gamma_turnover_budget": 0.0,
        "objective": "log_utility",
        "rank_ic_weight": 1.0,
        "direction_weight": 0.05,
        "volatility_regime_weight": 0.05,
        "concentration_weight": 0.005,
        "regime_up_threshold": 0.002,
        "regime_down_threshold": -0.002,
    }


def _forward_loss(
    *,
    model: torch.nn.Module,
    batch: dict[str, torch.Tensor],
    device: torch.device,
    plan: PrecisionPlan,
    initial_weights: torch.Tensor | None,
) -> tuple[torch.Tensor, torch.Tensor, dict[str, torch.Tensor]]:
    with _precision_context(device, plan):
        output = model(batch["x"], batch["tradable_mask"])
        weights, aux_outputs = _extract_weights_and_aux(output)
        aux_outputs = dict(aux_outputs or {})
        aux_outputs["initial_weights"] = initial_weights
        loss = risk_aware_loss(
            weights,
            batch["future_log_returns"],
            batch["tradable_mask"],
            benchmark_returns=batch["benchmark"],
            can_buy_mask=batch["can_buy_mask"],
            can_sell_mask=batch["can_sell_mask"],
            sample_mask=batch["sample_mask"],
            aux_outputs=aux_outputs,
            **_loss_kwargs(),
        )
    return loss, weights, aux_outputs


def _run_supported_mode(
    *,
    args: argparse.Namespace,
    mode: str,
    plan: PrecisionPlan,
    device: torch.device,
    batch: dict[str, torch.Tensor],
    base_state: dict[str, torch.Tensor],
    reference_weights: torch.Tensor | None,
    reference_loss: float | None,
) -> dict[str, Any]:
    old_matmul_tf32 = torch.backends.cuda.matmul.allow_tf32
    old_cudnn_tf32 = torch.backends.cudnn.allow_tf32
    torch.backends.cuda.matmul.allow_tf32 = bool(plan.allow_tf32)
    torch.backends.cudnn.allow_tf32 = bool(plan.allow_tf32)
    try:
        if plan.transformer_engine and te is None:
            return {
                "mode": mode,
                "status": "unsupported_native",
                "runnable": False,
                "reason": f"transformer_engine is not installed; {mode.upper()} native mode cannot run.",
                "error_type": "MissingOptionalDependency",
                "error": "transformer_engine.pytorch import failed",
            }
        model = _make_model(args, device)
        model.load_state_dict(base_state)
        conversion_stats = _replace_linear_modules(model, plan)
        model.train()
        optimizer = torch.optim.AdamW(model.parameters(), lr=float(args.lr), weight_decay=float(args.weight_decay))
        scaler = GradScaler(device="cuda", enabled=(device.type == "cuda" and plan.use_grad_scaler))

        if device.type == "cuda":
            torch.cuda.empty_cache()
            torch.cuda.reset_peak_memory_stats(device)

        with torch.no_grad():
            eval_model = _make_model(args, device)
            eval_model.load_state_dict(base_state)
            _replace_linear_modules(eval_model, plan)
            eval_model.eval()
            initial_loss_t, initial_weights, _ = _forward_loss(
                model=eval_model,
                batch=batch,
                device=device,
                plan=plan,
                initial_weights=None,
            )
            initial_loss = float(initial_loss_t.detach().float().cpu().item())
            initial_finite = bool(torch.isfinite(initial_loss_t.detach()).item())
            if reference_weights is not None:
                reference_weights_for_diff = reference_weights.to(device=initial_weights.device, dtype=torch.float32)
                weight_diff = float(
                    (initial_weights.detach().float() - reference_weights_for_diff).abs().max().cpu().item()
                )
            else:
                weight_diff = None
            loss_diff = abs(initial_loss - reference_loss) if reference_loss is not None else None
            del eval_model

        _sync(device)
        timed_steps = int(args.steps)
        warmup = int(args.warmup)
        total_steps = warmup + timed_steps
        previous_weights: torch.Tensor | None = None
        final_loss = float("nan")
        started = None
        for step in range(total_steps):
            if step == warmup:
                _sync(device)
                if device.type == "cuda":
                    torch.cuda.reset_peak_memory_stats(device)
                started = time.perf_counter()
            optimizer.zero_grad(set_to_none=True)
            loss, _, aux_outputs = _forward_loss(
                model=model,
                batch=batch,
                device=device,
                plan=plan,
                initial_weights=previous_weights,
            )
            if scaler.is_enabled():
                scaler.scale(loss).backward()
                scaler.step(optimizer)
                scaler.update()
            else:
                loss.backward()
                optimizer.step()
            next_weights = aux_outputs.get("_final_weights")
            previous_weights = (
                next_weights.detach().clone(memory_format=torch.contiguous_format)
                if isinstance(next_weights, torch.Tensor)
                else None
            )
            final_loss = float(loss.detach().float().cpu().item())

        _sync(device)
        elapsed = time.perf_counter() - float(started if started is not None else time.perf_counter())
        peak_bytes = torch.cuda.max_memory_allocated(device) if device.type == "cuda" else 0
        module_stats = _precision_module_stats(model)
        parameter_stats = _parameter_stats(model, plan)
        return {
            "mode": mode,
            "status": "ok",
            "runnable": True,
            "reason": plan.reason,
            "autocast_dtype": str(plan.autocast_dtype).replace("torch.", "") if plan.autocast_dtype is not None else None,
            "compute_label": plan.compute_label,
            "native_amp": bool(plan.native_amp),
            "native_backend": plan.native_backend,
            "compatibility_mode": plan.native_backend or "native",
            "allow_tf32": bool(plan.allow_tf32),
            "grad_scaler": bool(scaler.is_enabled()),
            **parameter_stats,
            **module_stats,
            "conversion_stats": conversion_stats,
            "device": str(device),
            "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
            "batch_size": int(args.batch_size),
            "lookback": int(args.lookback),
            "symbols": int(args.symbols),
            "features": int(args.features),
            "attention_mode": str(args.attention_mode),
            "temporal_pooling": str(args.temporal_pooling),
            "steps": timed_steps,
            "warmup": warmup,
            "elapsed_s": round(float(elapsed), 6),
            "s_per_step": round(float(elapsed) / max(1, timed_steps), 6),
            "samples_per_s": round(float(timed_steps * int(args.batch_size)) / max(float(elapsed), 1e-12), 3),
            "peak_vram_gb": round(_gb(int(peak_bytes)), 4),
            "initial_loss": round(initial_loss, 10),
            "final_loss": round(final_loss, 10),
            "initial_loss_finite": initial_finite,
            "final_loss_finite": bool(math.isfinite(final_loss)),
            "initial_loss_abs_diff_vs_fp32": round(float(loss_diff), 10) if loss_diff is not None else None,
            "initial_weight_max_abs_diff_vs_fp32": round(float(weight_diff), 10) if weight_diff is not None else None,
        }
    except Exception as exc:  # noqa: BLE001 - benchmark should report failures per mode.
        return {
            "mode": mode,
            "status": "error",
            "runnable": True,
            "reason": plan.reason,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    finally:
        torch.backends.cuda.matmul.allow_tf32 = old_matmul_tf32
        torch.backends.cudnn.allow_tf32 = old_cudnn_tf32


def _probe_unsupported_mode(mode: str, plan: PrecisionPlan, device: torch.device) -> dict[str, Any]:
    result: dict[str, Any] = {
        "mode": mode,
        "status": "unsupported_native",
        "runnable": False,
        "reason": plan.reason,
        "compute_label": plan.compute_label,
        "native_backend": plan.native_backend,
        "device": str(device),
    }
    if mode == "fp8":
        result["torch_has_float8_e4m3fn"] = hasattr(torch, "float8_e4m3fn")
        result["torch_has_float8_e5m2"] = hasattr(torch, "float8_e5m2")
        if device.type == "cuda" and plan.autocast_dtype is not None:
            try:
                with autocast(device_type="cuda", dtype=plan.autocast_dtype):
                    a = torch.randn(16, 16, device=device)
                    b = torch.randn(16, 16, device=device)
                    _ = a @ b
                result["autocast_probe"] = "unexpected_ok"
            except Exception as exc:  # noqa: BLE001
                result["autocast_probe"] = "failed_as_expected"
                result["autocast_error_type"] = type(exc).__name__
                result["autocast_error"] = str(exc)
    return result


def _write_markdown(path: Path, results: list[dict[str, Any]]) -> None:
    runnable_rows = [r for r in results if r.get("status") == "ok"]
    lines = [
        "# Precision Mode Benchmark",
        "",
        "| mode | status | compat mode | s/step | samples/s | peak VRAM GB | train param MB | packed weight MB | initial loss | loss diff vs FP32 | weight diff vs FP32 | notes |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in results:
        lines.append(
            "| {mode} | {status} | {compat} | {s_per_step} | {samples_per_s} | {peak_vram_gb} | {train_mb} | {packed_mb} | {initial_loss} | {loss_diff} | {weight_diff} | {notes} |".format(
                mode=row.get("mode"),
                status=row.get("status"),
                compat=row.get("compatibility_mode", ""),
                s_per_step=row.get("s_per_step", ""),
                samples_per_s=row.get("samples_per_s", ""),
                peak_vram_gb=row.get("peak_vram_gb", ""),
                train_mb=row.get("training_parameter_storage_mb", ""),
                packed_mb=row.get("estimated_packed_weight_storage_mb", ""),
                initial_loss=row.get("initial_loss", ""),
                loss_diff=row.get("initial_loss_abs_diff_vs_fp32", ""),
                weight_diff=row.get("initial_weight_max_abs_diff_vs_fp32", ""),
                notes=str(row.get("reason", row.get("error", ""))).replace("|", "\\|"),
            )
        )
    if runnable_rows:
        fastest = min(runnable_rows, key=lambda r: float(r.get("s_per_step", float("inf"))))
        lines.extend(["", f"Fastest runnable mode in this run: `{fastest.get('mode')}`."])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark project precision modes on transformer_base_portfolio forward + "
            "log_utility loss + backward."
        )
    )
    parser.add_argument("--modes", default=",".join(DEFAULT_MODES))
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--batch-size", type=int, default=4)
    parser.add_argument("--lookback", type=int, default=32)
    parser.add_argument("--symbols", type=int, default=512)
    parser.add_argument("--features", type=int, default=24)
    parser.add_argument("--d-model", type=int, default=32)
    parser.add_argument("--heads", type=int, default=4)
    parser.add_argument("--temporal-layers", type=int, default=2)
    parser.add_argument("--attention-mode", default="market_token")
    parser.add_argument("--temporal-pooling", default="last")
    parser.add_argument("--sdpa-batch-limit", type=int, default=16384)
    parser.add_argument("--steps", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--lr", type=float, default=1e-4)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--output-jsonl", default="artifacts/precision_modes/benchmark_precision_modes.jsonl")
    parser.add_argument("--output-md", default="artifacts/precision_modes/benchmark_precision_modes.md")
    args = parser.parse_args()

    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is false")

    torch.manual_seed(int(args.seed))
    if device.type == "cuda":
        torch.cuda.manual_seed_all(int(args.seed))

    batch = _make_batch(args, device)
    base_model = _make_model(args, device)
    base_state = copy.deepcopy(base_model.state_dict())
    del base_model

    jsonl_path = Path(args.output_jsonl)
    md_path = Path(args.output_md)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    reference_weights: torch.Tensor | None = None
    reference_loss: float | None = None
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for mode in _parse_modes(args.modes):
            plan = precision_plan(mode)
            if not plan.runnable:
                result = _probe_unsupported_mode(plan.name, plan, device)
            else:
                result = _run_supported_mode(
                    args=args,
                    mode=plan.name,
                    plan=plan,
                    device=device,
                    batch=batch,
                    base_state=base_state,
                    reference_weights=reference_weights,
                    reference_loss=reference_loss,
                )
                if plan.name == "fp32" and result.get("status") == "ok":
                    eval_model = _make_model(args, device)
                    eval_model.load_state_dict(base_state)
                    eval_model.eval()
                    old_tf32 = torch.backends.cuda.matmul.allow_tf32
                    torch.backends.cuda.matmul.allow_tf32 = False
                    with torch.no_grad():
                        loss_t, weights_t, _ = _forward_loss(
                            model=eval_model,
                            batch=batch,
                            device=device,
                            plan=plan,
                            initial_weights=None,
                        )
                    torch.backends.cuda.matmul.allow_tf32 = old_tf32
                    reference_weights = weights_t.detach().float().cpu()
                    reference_loss = float(loss_t.detach().float().cpu().item())
                    del eval_model
            results.append(result)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
            handle.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            handle.flush()
    _write_markdown(md_path, results)


if __name__ == "__main__":
    main()
