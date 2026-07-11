#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from functools import partial
from pathlib import Path

import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import (
    _PanelSlabForwardWrapper,
    _autocast_context,
    _can_enable_torch_compile,
    _call_model,
    _configure_backtest_runtime_from_config,
    _detach_portfolio_state,
    _densify_windowed_training_split_for_panel_slab,
    _extract_weights_and_aux,
    _is_log_utility_objective,
    _maybe_compact_train_windowed_symbols,
    _model_supports_panel_slab_forward,
    _normalize_risk_objective,
    _pad_windowed_training_split,
    _resolve_amp_dtype,
    _training_loss_min_trade_weight,
    _training_loss_portfolio_activation,
    _training_needs_aux,
    _torch_compile_options,
    _volume_limit_weights_from_notional,
    _volume_participation_enabled,
)
from stockagent.training.windowed import dataset_to_windowed_tensors


def _parse_int_list(value: str) -> list[int]:
    return [int(part.strip()) for part in str(value).split(",") if part.strip()]


def _sync(device: torch.device) -> None:
    if device.type == "cuda":
        torch.cuda.synchronize(device)


def _gb(num_bytes: int) -> float:
    return float(num_bytes) / 1024**3


def _config_bool(value: bool | None, default: bool) -> bool:
    return bool(default if value is None else value)


def _loss_kwargs(config) -> dict:
    return {
        "long_only": bool(config.trading.long_only),
        "buy_fee_rate": float(config.trading.buy_fee_rate),
        "sell_fee_rate": float(config.trading.sell_fee_rate),
        "max_turnover_ratio": float(config.trading.max_turnover_ratio),
        "gross_leverage": 1.0,
        "min_trade_weight": _training_loss_min_trade_weight(config),
        "portfolio_activation": _training_loss_portfolio_activation(config),
        "gamma_sharpe": float(config.evaluation.gamma_sharpe),
        "gamma_excess": float(config.evaluation.gamma_excess),
        "gamma_cvar": float(config.evaluation.gamma_cvar),
        "cvar_alpha": float(config.evaluation.cvar_alpha),
        "gamma_drawdown": float(config.evaluation.gamma_drawdown),
        "drawdown_target": float(config.evaluation.drawdown_target),
        "gamma_turnover": float(config.evaluation.gamma_turnover),
        "gamma_underperformance": float(config.evaluation.gamma_underperformance),
        "excess_target": float(config.evaluation.excess_target),
        "cvar_budget": float(config.evaluation.cvar_budget),
        "drawdown_budget": float(config.evaluation.drawdown_budget),
        "turnover_budget": float(config.evaluation.turnover_budget),
        "gamma_cvar_budget": float(config.evaluation.gamma_cvar_budget),
        "gamma_drawdown_budget": float(config.evaluation.gamma_drawdown_budget),
        "gamma_turnover_budget": float(config.evaluation.gamma_turnover_budget),
        "objective": str(config.training.loss_type),
        "rank_ic_weight": float(config.training.multitask_loss.rank_ic_weight),
        "return_rank_ic_weight": float(config.training.multitask_loss.return_rank_ic_weight),
        "direction_weight": float(config.training.multitask_loss.direction_weight),
        "volatility_regime_weight": float(config.training.multitask_loss.volatility_regime_weight),
        "concentration_weight": float(config.training.multitask_loss.concentration_weight),
        "regime_up_threshold": float(config.training.multitask_loss.regime_up_threshold),
        "regime_down_threshold": float(config.training.multitask_loss.regime_down_threshold),
    }


def _run_case(
    *,
    config,
    panel,
    fold,
    lookback: int,
    batch_size: int,
    batches: int,
    warmup: int,
    device: torch.device,
    cache_on_device: bool,
    compile_model: bool,
    compile_loss: bool,
    attention_mode: str | None,
    temporal_pooling: str | None,
) -> dict:
    config.training.lookback = int(lookback)
    if attention_mode:
        config.training.transformer_base_portfolio.attention_mode = attention_mode
    if temporal_pooling:
        config.training.transformer_base_portfolio.temporal_pooling = temporal_pooling

    include_volume_notional = _volume_participation_enabled(
        config.trading.max_volume_participation,
        config.trading.volume_participation_equity,
    )
    dataset = CrossSectionalDataset(
        panel,
        fold.train_indices,
        int(lookback),
        include_volume_notional=include_volume_notional,
    )
    real_rows = len(dataset)
    batch_size = max(1, min(int(batch_size), real_rows))
    objective = _normalize_risk_objective(config.training.loss_type)
    if _training_needs_aux(
        objective,
        direction_weight=float(config.training.multitask_loss.direction_weight),
        volatility_regime_weight=float(config.training.multitask_loss.volatility_regime_weight),
    ):
        raise ValueError(
            "benchmark_transformer_hotpath measures the production no-aux panel-slab executor; "
            f"objective={objective!r} requires the research aux path"
        )
    split = _maybe_compact_train_windowed_symbols(
        dataset_to_windowed_tensors(dataset),
        config,
        label="hotpath benchmark",
    )
    split = _densify_windowed_training_split_for_panel_slab(split, fold.train_indices)
    split = _pad_windowed_training_split(split, batch_size)
    rows = len(split)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    if cache_on_device and device.type != "cpu":
        split = split.to_device_cache(device)
        if (
            bool(getattr(config.training, "cache_train_features_in_amp_dtype", False))
            and amp_dtype in {torch.float16, torch.bfloat16}
        ):
            split.features = split.features.to(dtype=amp_dtype)

    model = build_model(
        config=config,
        lookback=int(lookback),
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
        feature_names=panel.feature_names,
    ).to(device)
    parameter_count = sum(int(parameter.numel()) for parameter in model.parameters())
    trainable_parameter_count = sum(
        int(parameter.numel())
        for parameter in model.parameters()
        if parameter.requires_grad
    )
    if compile_model or compile_loss:
        can_compile, reason = _can_enable_torch_compile(device)
        if not can_compile:
            raise RuntimeError(f"torch.compile requested but unavailable: {reason}")
    forward_model: torch.nn.Module = model
    use_panel_slab = _model_supports_panel_slab_forward(model)
    if use_panel_slab:
        forward_model = _PanelSlabForwardWrapper(model)
    if compile_model:
        compile_mode = str(getattr(config.training, "torch_compile_mode", "default") or "default")
        forward_model = torch.compile(
            forward_model,
            fullgraph=True,
            dynamic=False,
            options=_torch_compile_options(compile_mode, cudagraphs=False),
        )
    optimizer_kwargs = {
        "lr": float(config.training.learning_rate),
        "weight_decay": float(config.training.weight_decay),
    }
    if device.type == "cuda":
        optimizer_kwargs["fused"] = True
    try:
        optimizer = torch.optim.AdamW(model.parameters(), **optimizer_kwargs)
    except TypeError:
        optimizer_kwargs.pop("fused", None)
        optimizer = torch.optim.AdamW(model.parameters(), **optimizer_kwargs)
    _configure_backtest_runtime_from_config(config)
    loss_fn = partial(risk_aware_loss, **_loss_kwargs(config))
    if compile_loss:
        loss_fn = torch.compile(
            loss_fn,
            fullgraph=_is_log_utility_objective(objective),
            dynamic=False,
            options={"triton.cudagraphs": False},
        )

    if device.type == "cuda":
        torch.cuda.reset_peak_memory_stats(device)

    total_batches = max(1, rows // batch_size)
    total_steps = int(warmup) + int(batches)
    portfolio_prev_weights = torch.zeros(split.num_symbols, device=device, dtype=torch.float32)
    portfolio_prev_alive = torch.ones((), device=device, dtype=torch.bool)
    last_loss_tensor: torch.Tensor | None = None

    _sync(device)
    start_timed: float | None = None
    for step in range(total_steps):
        if step == int(warmup):
            _sync(device)
            start_timed = time.perf_counter()
            if device.type == "cuda":
                torch.cuda.reset_peak_memory_stats(device)
        batch_idx = step % total_batches
        start = batch_idx * batch_size
        end = start + batch_size
        if use_panel_slab:
            batch = split.panel_slab_batch_by_rows(
                start,
                end,
                device=device,
                non_blocking=(device.type == "cuda"),
            )
            if batch is None:
                raise RuntimeError(f"production panel-slab batch unavailable for rows=[{start},{end})")
        else:
            batch = split.batch_by_rows(start, end, device=device, non_blocking=(device.type == "cuda"))

        optimizer.zero_grad(set_to_none=True)
        with _autocast_context(device, amp_dtype):
            if use_panel_slab:
                model_output = forward_model(
                    batch["feature_slab"],
                    batch["tradable_mask"],
                    batch.get("symbol_indices"),
                )
            else:
                model_output = _call_model(
                    forward_model,
                    batch["x"],
                    batch["tradable_mask"],
                    return_aux=False,
                    symbol_indices=batch.get("symbol_indices"),
                )
            weights, aux_outputs = _extract_weights_and_aux(model_output)
            aux_outputs = dict(aux_outputs or {})
            aux_outputs["initial_weights"] = portfolio_prev_weights
            aux_outputs["initial_alive"] = portfolio_prev_alive
            loss = loss_fn(
                weights,
                batch["future_log_returns"],
                batch["tradable_mask"],
                benchmark_returns=batch["benchmark"],
                can_buy_mask=batch["can_buy_mask"],
                can_sell_mask=batch["can_sell_mask"],
                can_short_open_mask=batch["can_short_open_mask"],
                force_short_cover_mask=batch["force_short_cover_mask"],
                force_exit_mask=batch["force_exit_mask"],
                volume_limit_weights=_volume_limit_weights_from_notional(
                    batch.get("volume_notional"),
                    max_volume_participation=float(config.trading.max_volume_participation),
                    volume_participation_equity=float(config.trading.volume_participation_equity),
                    device=device,
                    dtype=weights.dtype,
                ),
                sample_mask=batch["sample_mask"],
                aux_outputs=aux_outputs,
            )
        loss.backward()
        optimizer.step()
        next_prev = aux_outputs.get("_final_weights")
        next_alive = aux_outputs.get("_final_alive")
        portfolio_prev_weights = _detach_portfolio_state(next_prev) if next_prev is not None else None
        if next_alive is not None:
            portfolio_prev_alive = _detach_portfolio_state(next_alive)
        # Do not synchronize CUDA inside the timed loop. Production training
        # consumes the scalar only after the epoch, so the benchmark should too.
        last_loss_tensor = loss.detach()

    _sync(device)
    elapsed = time.perf_counter() - float(start_timed if start_timed is not None else time.perf_counter())
    last_loss = (
        float(last_loss_tensor.float().cpu().item())
        if last_loss_tensor is not None
        else float("nan")
    )
    peak_bytes = torch.cuda.max_memory_allocated(device) if device.type == "cuda" else 0
    return {
        "fold_id": int(fold.fold_id),
        "lookback": int(lookback),
        "rows": int(real_rows),
        "padded_rows": int(rows),
        "symbols": int(split.num_symbols),
        "universe_symbols": int(panel.num_symbols),
        "features": int(len(panel.feature_names)),
        "batch_size": int(batch_size),
        "timed_batches": int(batches),
        "device": str(device),
        "cache_on_device": bool(cache_on_device and device.type != "cpu"),
        "compile_model": bool(compile_model),
        "compile_loss": bool(compile_loss),
        "torch_compile_mode": str(config.training.torch_compile_mode),
        "attention_mode": str(config.training.transformer_base_portfolio.attention_mode),
        "temporal_pooling": str(config.training.transformer_base_portfolio.temporal_pooling),
        "temporal_query_mode": str(config.training.transformer_base_portfolio.temporal_query_mode),
        "sanitize_inputs": bool(config.training.transformer_base_portfolio.sanitize_inputs),
        "amp_native_position_add": bool(
            config.training.transformer_base_portfolio.amp_native_position_add
        ),
        "temporal_self_attention_fast_path": bool(
            config.training.transformer_base_portfolio.temporal_self_attention_fast_path
        ),
        "compiled_cross_attention_backend": str(
            config.training.transformer_base_portfolio.compiled_cross_attention_backend
        ),
        "cache_train_features_in_amp_dtype": bool(
            config.training.cache_train_features_in_amp_dtype
        ),
        "parameter_count": int(parameter_count),
        "trainable_parameter_count": int(trainable_parameter_count),
        "loss_type": str(config.training.loss_type),
        "elapsed_s": round(float(elapsed), 6),
        "s_per_batch": round(float(elapsed) / max(1, int(batches)), 6),
        "samples_per_s": round((int(batches) * int(batch_size)) / max(float(elapsed), 1e-12), 3),
        "peak_vram_gb": round(_gb(int(peak_bytes)), 4),
        "last_loss": round(last_loss, 8),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark real transformer train hotpath on panel/windowed tensors.")
    parser.add_argument("--config", default="configs/experiment_baseline.yaml")
    parser.add_argument("--parquet-root", type=Path, default=None)
    parser.add_argument("--public-feature-path", type=Path, default=None)
    parser.add_argument("--fold-index", type=int, default=0)
    parser.add_argument("--lookbacks", default="8,32")
    parser.add_argument("--batch-sizes", default=None)
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--cache-on-device", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--compile-model", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--compile-loss", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--attention-mode", default=None)
    parser.add_argument("--temporal-pooling", default=None)
    parser.add_argument(
        "--temporal-query-mode",
        choices=("full_then_last", "last_only"),
        default=None,
    )
    parser.add_argument("--torch-compile-mode", default=None)
    parser.add_argument(
        "--cache-train-features-in-amp-dtype",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument("--sanitize-inputs", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--amp-native-position-add", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument(
        "--temporal-self-attention-fast-path",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    parser.add_argument(
        "--compiled-cross-attention-backend",
        choices=("auto", "manual", "sdpa"),
        default=None,
    )
    args = parser.parse_args()

    config = load_config(args.config)
    if args.parquet_root is not None:
        config.data.parquet_root = str(args.parquet_root)
    if args.public_feature_path is not None:
        config.data.tw_public_feature_path = str(args.public_feature_path)
    if args.torch_compile_mode is not None:
        config.training.torch_compile_mode = str(args.torch_compile_mode)
    if args.cache_train_features_in_amp_dtype is not None:
        config.training.cache_train_features_in_amp_dtype = bool(
            args.cache_train_features_in_amp_dtype
        )
    transformer_config = config.training.transformer_base_portfolio
    if args.temporal_query_mode is not None:
        transformer_config.temporal_query_mode = str(args.temporal_query_mode)
    if args.sanitize_inputs is not None:
        transformer_config.sanitize_inputs = bool(args.sanitize_inputs)
    if args.amp_native_position_add is not None:
        transformer_config.amp_native_position_add = bool(args.amp_native_position_add)
    if args.temporal_self_attention_fast_path is not None:
        transformer_config.temporal_self_attention_fast_path = bool(
            args.temporal_self_attention_fast_path
        )
    if args.compiled_cross_attention_backend is not None:
        transformer_config.compiled_cross_attention_backend = str(
            args.compiled_cross_attention_backend
        )
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is false")
    torch.manual_seed(int(config.training.seed))
    if device.type == "cuda":
        torch.cuda.manual_seed_all(int(config.training.seed))

    panel = build_panel(
        config.data.parquet_root,
        benchmark_name=config.data.benchmark_name,
        usd_only_trading_pairs=config.data.usd_only_trading_pairs,
        tradable_mode=config.data.tradable_mode,
        trading_volume_policy=config.data.trading_volume_policy,
        security_filter=config.data.security_filter,
        strict_no_fallback=config.training.strict_no_fallback,
        panel_backend=config.data.panel_backend,
        panel_load_workers=config.data.panel_load_workers,
        external_feature_path=(
            config.data.tw_public_feature_path
            if config.data.use_tw_public_features or config.data.use_tw_public_rules
            else None
        ),
        external_market_symbol=config.data.tw_public_market_symbol,
        external_include_features=config.data.use_tw_public_features,
        external_include_rules=config.data.use_tw_public_rules,
        external_data_required=config.data.use_tw_public_features or config.data.use_tw_public_rules,
        feature_include=config.data.feature_include,
        feature_exclude=config.data.feature_exclude,
        feature_zero_fill=config.data.feature_zero_fill,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    fold = folds[min(args.fold_index, len(folds) - 1)]
    lookbacks = _parse_int_list(args.lookbacks)
    batch_sizes = (
        _parse_int_list(args.batch_sizes)
        if args.batch_sizes
        else [int(config.training.batch_size_train)]
    )
    cache_on_device = _config_bool(args.cache_on_device, bool(config.training.cache_train_tensors_on_gpu))
    compile_model = _config_bool(args.compile_model, bool(config.training.enable_torch_compile))
    compile_loss = _config_bool(args.compile_loss, bool(config.training.compile_loss))

    for lookback in lookbacks:
        for batch_size in batch_sizes:
            result = _run_case(
                config=config,
                panel=panel,
                fold=fold,
                lookback=lookback,
                batch_size=batch_size,
                batches=max(1, int(args.batches)),
                warmup=max(0, int(args.warmup)),
                device=device,
                cache_on_device=cache_on_device,
                compile_model=compile_model,
                compile_loss=compile_loss,
                attention_mode=args.attention_mode,
                temporal_pooling=args.temporal_pooling,
            )
            print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
