#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from pathlib import Path

import numpy as np
import torch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_execution import require_naive_execution_for_tool
from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.walkforward import build_expanding_year_folds
from stockagent.models.factory import build_model
from stockagent.training.dataset import CrossSectionalDataset
from stockagent.training.loss import risk_aware_loss
from stockagent.training.trainer import (
    _autocast_context,
    _extract_weights_and_aux,
    _resolve_amp_dtype,
    _training_loss_min_trade_weight,
    _volume_limit_weights_from_notional,
)
from stockagent.training.windowed import dataset_to_windowed_tensors


def _parse_ints(value: str) -> list[int]:
    return [int(part.strip()) for part in str(value).split(",") if part.strip()]


def _set_seed(seed: int) -> None:
    random.seed(int(seed))
    np.random.seed(int(seed))
    torch.manual_seed(int(seed))
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(int(seed))


def _loss_kwargs(config) -> dict:
    return {
        "long_only": bool(config.trading.long_only),
        "buy_fee_rate": float(config.trading.buy_fee_rate),
        "sell_fee_rate": float(config.trading.sell_fee_rate),
        "max_turnover_ratio": float(config.trading.max_turnover_ratio),
        "gross_leverage": 1.0,
        "min_trade_weight": _training_loss_min_trade_weight(config),
        "portfolio_activation": str(config.training.loss_portfolio_activation),
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


def _probe_seed(config, panel, fold, seed: int, device: torch.device, batch_size: int, min_grad_norm: float) -> dict:
    _set_seed(seed)
    config.training.seed = int(seed)
    dataset = CrossSectionalDataset(
        panel,
        fold.train_indices,
        int(config.training.lookback),
        execution_mode=config.trading.execution_mode,
        short_capacity_limit_enabled=config.trading.tw_short_capacity_limit_enabled,
        tw_corporate_action_mode=config.trading.tw_corporate_action_mode,
    )
    split = dataset_to_windowed_tensors(dataset)
    batch_size = max(1, min(int(batch_size), len(split)))
    batch = split.batch_by_rows(0, batch_size, device=device, non_blocking=(device.type == "cuda"))

    model = build_model(
        config=config,
        lookback=int(config.training.lookback),
        num_features=len(panel.feature_names),
        num_symbols=panel.num_symbols,
        feature_names=panel.feature_names,
    ).to(device)
    model.train()
    model.zero_grad(set_to_none=True)
    amp_dtype = _resolve_amp_dtype(config.environment.amp_dtype)
    with _autocast_context(device, amp_dtype):
        output = model(batch["x"], batch["tradable_mask"])
        weights, aux_outputs = _extract_weights_and_aux(output)
        loss = risk_aware_loss(
            weights,
            batch["future_log_returns"],
            batch["tradable_mask"],
            benchmark_returns=batch.get("benchmark"),
            can_buy_mask=batch["can_buy_mask"],
            can_sell_mask=batch["can_sell_mask"],
            can_short_open_mask=batch["can_short_open_mask"],
            force_short_cover_mask=batch["force_short_cover_mask"],
            force_exit_mask=batch["force_exit_mask"],
            volume_limit_weights=_volume_limit_weights_from_notional(
                batch.get("volume_notional"),
                max_volume_participation=float(config.trading.max_volume_participation),
                volume_participation_equity=float(config.trading.volume_participation_equity),
                device=weights.device,
                dtype=weights.dtype,
            ),
            sample_mask=batch.get("sample_mask"),
            aux_outputs=aux_outputs,
            **_loss_kwargs(config),
        )
    loss.backward()
    total_sq = 0.0
    nonzero_tensors = 0
    finite = bool(torch.isfinite(loss.detach()).item())
    first_nonfinite_grad = None
    for name, param in model.named_parameters():
        grad = param.grad
        if grad is None:
            continue
        grad_detached = grad.detach()
        if not torch.isfinite(grad_detached).all():
            finite = False
            first_nonfinite_grad = name
            break
        norm = float(grad_detached.float().norm().detach().cpu().item())
        total_sq += norm * norm
        if norm > 0.0:
            nonzero_tensors += 1
    grad_norm = total_sq ** 0.5
    ok = finite and grad_norm >= float(min_grad_norm) and nonzero_tensors > 0
    if device.type == "cuda":
        torch.cuda.synchronize(device)
        torch.cuda.empty_cache()
    return {
        "seed": int(seed),
        "ok": bool(ok),
        "loss": float(loss.detach().float().cpu().item()) if torch.isfinite(loss.detach()) else None,
        "grad_norm": grad_norm,
        "nonzero_grad_tensors": int(nonzero_tensors),
        "first_nonfinite_grad": first_nonfinite_grad,
        "batch_size": int(batch_size),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Find a tw_boost seed with finite non-zero gradients.")
    parser.add_argument("--config", default="configs/markets/tw_boost.yaml")
    parser.add_argument("--seeds", default="1,2,3,4,5,7,11,13,17,19,23,29,31,37,41,43,47,53")
    parser.add_argument("--fold-index", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--device", default="cuda")
    parser.add_argument("--min-grad-norm", type=float, default=1e-10)
    parser.add_argument("--output-jsonl", default="artifacts/benchmarks/tw_boost_seed_gradients.jsonl")
    parser.add_argument("--write-config", action="store_true")
    args = parser.parse_args()

    config_path = (REPO_ROOT / args.config).resolve()
    config = load_config(config_path)
    require_naive_execution_for_tool(
        config.trading.execution_mode,
        tool_name="find_tw_boost_seed_with_gradients.py",
    )
    device = torch.device(args.device)
    if device.type == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA requested but torch.cuda.is_available() is false")

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
        panel_start_date=config.data.panel_start_date,
    )
    folds = build_expanding_year_folds(
        dates=panel.dates,
        min_train_years=config.walk_forward.min_train_years,
        val_years=config.walk_forward.val_years,
        require_future_test_year=config.walk_forward.require_future_test_year,
    )
    fold = folds[min(max(0, int(args.fold_index)), len(folds) - 1)]
    batch_size = int(args.batch_size) if args.batch_size is not None else int(config.training.batch_size_train)

    output_path = (REPO_ROOT / args.output_jsonl).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []
    best: dict | None = None
    for seed in _parse_ints(args.seeds):
        row = _probe_seed(config, panel, fold, seed, device, batch_size, float(args.min_grad_norm))
        rows.append(row)
        output_path.write_text("\n".join(json.dumps(item, sort_keys=True) for item in rows) + "\n")
        print(json.dumps(row, ensure_ascii=False, sort_keys=True), flush=True)
        if row["ok"]:
            best = row
            break

    if best is None:
        raise RuntimeError(f"No seed produced finite non-zero gradients. See {output_path}")

    if args.write_config:
        text = config_path.read_text()
        seed_line = f"  seed: {int(best['seed'])}"
        if re.search(r"(?m)^training:\n(?:  .*\n)*?  seed: .*$", text):
            text = re.sub(r"(?m)^  seed: .*$", seed_line, text, count=1)
        else:
            text = text.replace("  non_blocking_transfer: true\n", f"  non_blocking_transfer: true\n{seed_line}\n", 1)
        config_path.write_text(text)
        print(f"[seed] wrote training.seed={best['seed']} to {config_path}", flush=True)


if __name__ == "__main__":
    main()
