#!/usr/bin/env python3
"""Train the stock-cross-section -> Taiwan-index-futures day strategy."""

from __future__ import annotations

import argparse
from contextlib import nullcontext
from dataclasses import asdict, replace
import hashlib
import json
import math
from pathlib import Path
import random
import sys
from typing import Any

import numpy as np
import torch
from torch import nn

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.backtest.tw_index_futures import (  # noqa: E402
    TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION,
    run_tw_index_futures_day_integer,
    tw_index_futures_log_utility_loss,
)
from stockagent.data.panel import (  # noqa: E402
    DAY_TRADE_OPEN_GAP_FEATURE,
    PanelData,
    build_panel,
)
from stockagent.data.tw_index_futures import (  # noqa: E402
    TAIFEX_FUTURES_DATA_CONTRACT_VERSION,
    TAIFEX_INDEX_FUTURES_PRODUCTS,
    TaiwanIndexFuturesDaySession,
    load_taifex_index_futures_day_session,
)
from stockagent.data.walkforward import build_expanding_year_folds  # noqa: E402
from stockagent.strategies.tw_index_futures_day import (  # noqa: E402
    build_index_futures_model,
    decision_indices_for_futures_day_session,
    decision_stock_mask,
    load_tw_index_futures_day_strategy_config,
    model_feature_end_indices,
)


class _PanelSlabForward(nn.Module):
    def __init__(self, model: nn.Module) -> None:
        super().__init__()
        self.model = model

    def forward(
        self,
        feature_slab: torch.Tensor,
        stock_mask: torch.Tensor,
    ) -> torch.Tensor:
        return self.model.forward_from_panel_slab(
            feature_slab,
            stock_mask,
        )


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_device(device_name: str) -> torch.device:
    normalized = str(device_name).strip().lower()
    if normalized == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA is required by the selected base config")
        return torch.device("cuda")
    return torch.device(normalized)


def _resolve_amp_dtype(name: str) -> torch.dtype | None:
    normalized = str(name).strip().lower()
    if normalized in {"", "none", "off", "fp32", "float32"}:
        return None
    if normalized in {"bf16", "bfloat16"}:
        return torch.bfloat16
    if normalized in {"fp16", "float16", "half"}:
        return torch.float16
    raise ValueError(f"unsupported AMP dtype: {name!r}")


def _autocast(device: torch.device, dtype: torch.dtype | None):
    if dtype is None:
        return nullcontext()
    return torch.autocast(device_type=device.type, dtype=dtype)


def _build_stock_panel(base_config) -> PanelData:
    if bool(base_config.data.day_trade_open_feature) or (
        DAY_TRADE_OPEN_GAP_FEATURE in base_config.data.feature_include
    ):
        raise ValueError(
            "08:45 futures decisions cannot consume the stock-market "
            "next-session opening quote; use a close-complete base config"
        )
    use_external = bool(
        base_config.data.use_tw_public_features
        or base_config.data.use_tw_public_rules
    )
    return build_panel(
        base_config.data.parquet_root,
        benchmark_name=base_config.data.benchmark_name,
        usd_only_trading_pairs=base_config.data.usd_only_trading_pairs,
        tradable_mode=base_config.data.tradable_mode,
        trading_volume_policy=base_config.data.trading_volume_policy,
        security_filter=base_config.data.security_filter,
        strict_no_fallback=base_config.training.strict_no_fallback,
        panel_backend=base_config.data.panel_backend,
        panel_load_workers=base_config.data.panel_load_workers,
        external_feature_path=(
            base_config.data.tw_public_feature_path if use_external else None
        ),
        external_market_symbol=base_config.data.tw_public_market_symbol,
        external_include_features=base_config.data.use_tw_public_features,
        external_include_rules=base_config.data.use_tw_public_rules,
        external_data_required=use_external,
        feature_include=base_config.data.feature_include,
        feature_exclude=base_config.data.feature_exclude,
        feature_zero_fill=base_config.data.feature_zero_fill,
        panel_start_date=base_config.data.panel_start_date,
    )


def _iter_contiguous_batches(
    indices: np.ndarray,
    batch_size: int,
    *,
    shuffle: bool,
    rng: np.random.Generator,
):
    values = np.asarray(indices, dtype=np.int64)
    if values.ndim != 1:
        raise ValueError("indices must be one-dimensional")
    if values.size and bool(np.any(values[1:] <= values[:-1])):
        raise ValueError("indices must be strictly increasing")
    split_points = np.flatnonzero(np.diff(values) != 1) + 1
    runs = np.split(values, split_points)
    batches = [
        run[start : start + batch_size]
        for run in runs
        for start in range(0, run.size, batch_size)
        if run.size
    ]
    if shuffle:
        order = rng.permutation(len(batches))
        batches = [batches[int(index)] for index in order]
    yield from batches


def _forward_batch(
    forward: nn.Module,
    features: torch.Tensor,
    panel: PanelData,
    decision_indices: np.ndarray,
    *,
    lookback: int,
    device: torch.device,
) -> torch.Tensor:
    ends = model_feature_end_indices(decision_indices)
    if ends.size > 1 and bool(np.any(np.diff(ends) != 1)):
        raise ValueError("panel-slab batches must contain contiguous dates")
    slab_start = int(ends[0]) - int(lookback) + 1
    slab_rows = int(ends[-1]) - slab_start + 1
    feature_slab = features.narrow(0, slab_start, slab_rows)
    mask = torch.as_tensor(
        decision_stock_mask(panel, decision_indices),
        device=device,
        dtype=torch.bool,
    )
    return forward(feature_slab, mask)


def _evaluate_exposures(
    forward: nn.Module,
    features: torch.Tensor,
    panel: PanelData,
    market: TaiwanIndexFuturesDaySession,
    decision_indices: np.ndarray,
    *,
    reference_product: str,
    batch_size: int,
    lookback: int,
    device: torch.device,
    amp_dtype: torch.dtype | None,
    round_trip_cost_rate: float,
    max_abs_exposure: float,
) -> tuple[np.ndarray, float]:
    reference_returns = torch.as_tensor(
        market.reference_log_returns(reference_product),
        device=device,
        dtype=torch.float32,
    )
    reference_valid = torch.as_tensor(
        market.reference_tradable_mask(reference_product),
        device=device,
        dtype=torch.bool,
    )
    outputs: list[np.ndarray] = []
    loss_sum = 0.0
    rows = 0
    forward.eval()
    rng = np.random.default_rng(0)
    with torch.no_grad():
        for batch_indices in _iter_contiguous_batches(
            decision_indices,
            batch_size,
            shuffle=False,
            rng=rng,
        ):
            with _autocast(device, amp_dtype):
                exposure = _forward_batch(
                    forward,
                    features,
                    panel,
                    batch_indices,
                    lookback=lookback,
                    device=device,
                )
            selected = torch.as_tensor(
                batch_indices,
                device=device,
                dtype=torch.long,
            )
            loss = tw_index_futures_log_utility_loss(
                exposure.float(),
                reference_returns.index_select(0, selected),
                reference_valid.index_select(0, selected),
                round_trip_cost_rate=round_trip_cost_rate,
                max_abs_exposure=max_abs_exposure,
            )
            outputs.append(exposure.float().cpu().numpy())
            loss_sum += float(loss.cpu().item()) * len(batch_indices)
            rows += len(batch_indices)
    if rows == 0:
        raise ValueError("evaluation split has no executable futures decisions")
    return np.concatenate(outputs), loss_sum / rows


def _subset_market(
    market: TaiwanIndexFuturesDaySession,
    indices: np.ndarray,
) -> TaiwanIndexFuturesDaySession:
    selected = np.asarray(indices, dtype=np.int64)
    return replace(
        market,
        dates=market.dates[selected],
        contract_months=market.contract_months[selected],
        open_prices=market.open_prices[selected],
        high_prices=market.high_prices[selected],
        low_prices=market.low_prices[selected],
        close_prices=market.close_prices[selected],
        volumes=market.volumes[selected],
        log_returns=market.log_returns[selected],
        tradable_mask=market.tradable_mask[selected],
    )


def _annualized_metrics(returns: np.ndarray) -> dict[str, float]:
    values = np.asarray(returns, dtype=np.float64)
    if values.size == 0:
        return {}
    equity = np.cumprod(np.clip(1.0 + values, 1e-12, None))
    peak = np.maximum.accumulate(equity)
    drawdown = equity / peak - 1.0
    mean = float(values.mean())
    std = float(values.std(ddof=1)) if values.size > 1 else 0.0
    downside = values[values < 0.0]
    downside_std = (
        float(downside.std(ddof=1)) if downside.size > 1 else 0.0
    )
    return {
        "total_return": float(equity[-1] - 1.0),
        "annualized_return": float(equity[-1] ** (252.0 / values.size) - 1.0),
        "annualized_volatility": std * math.sqrt(252.0),
        "sharpe": 0.0 if std <= 0.0 else mean / std * math.sqrt(252.0),
        "sortino": (
            0.0
            if downside_std <= 0.0
            else mean / downside_std * math.sqrt(252.0)
        ),
        "max_drawdown": float(drawdown.min(initial=0.0)),
    }


def _save_fold_artifacts(
    fold_dir: Path,
    *,
    checkpoint: dict[str, Any],
    market: TaiwanIndexFuturesDaySession,
    integer_result,
    val_loss: float,
    test_loss: float,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    fold_dir.mkdir(parents=True, exist_ok=True)
    torch.save(checkpoint, fold_dir / "checkpoint_best.pt")
    metrics = {
        "validation_log_utility_loss": float(val_loss),
        "test_log_utility_loss": float(test_loss),
        "integer_backtest": _annualized_metrics(integer_result.strategy_returns),
        "final_equity_twd": float(integer_result.equity[-1]),
        "traded_sessions": int(
            np.count_nonzero(
                np.any(integer_result.contract_quantities != 0, axis=1)
            )
        ),
        "total_fees_twd": float(integer_result.fees_twd.sum()),
        "total_tax_twd": float(integer_result.tax_twd.sum()),
        "total_slippage_twd": float(integer_result.slippage_twd.sum()),
    }
    (fold_dir / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    payload: dict[str, Any] = {
        "date": pa.array(
            np.asarray(integer_result.dates, dtype="datetime64[D]").astype(
                np.int32
            ),
            type=pa.date32(),
        ),
        "requested_exposure": integer_result.requested_exposure,
        "executed_exposure": integer_result.executed_exposure,
        "strategy_return": integer_result.strategy_returns,
        "turnover": integer_result.turnovers,
        "gross_pnl_twd": integer_result.gross_pnl_twd,
        "fees_twd": integer_result.fees_twd,
        "tax_twd": integer_result.tax_twd,
        "slippage_twd": integer_result.slippage_twd,
        "net_pnl_twd": integer_result.net_pnl_twd,
        "equity_twd": integer_result.equity,
        "alive": integer_result.alive,
    }
    for col, product in enumerate(integer_result.products):
        payload[f"{product}_contract_month"] = integer_result.contract_months[
            :, col
        ].tolist()
        payload[f"{product}_contracts"] = integer_result.contract_quantities[
            :, col
        ]
        payload[f"{product}_open"] = market.open_prices[:, col]
        payload[f"{product}_close"] = market.close_prices[:, col]
    pq.write_table(
        pa.table(payload),
        fold_dir / "daily_futures_execution.parquet",
        compression="zstd",
    )


def train(config_path: Path) -> None:
    base_config, strategy = load_tw_index_futures_day_strategy_config(config_path)
    random.seed(strategy.seed)
    np.random.seed(strategy.seed)
    torch.manual_seed(strategy.seed)
    panel = _build_stock_panel(base_config)
    market = load_taifex_index_futures_day_session(
        strategy.futures_data_path,
        panel_dates=panel.dates,
        products=TAIFEX_INDEX_FUTURES_PRODUCTS,
    )
    folds = build_expanding_year_folds(
        panel.dates,
        min_train_years=base_config.walk_forward.min_train_years,
        val_years=base_config.walk_forward.val_years,
        require_future_test_year=(
            base_config.walk_forward.require_future_test_year
        ),
        split_start_year=base_config.walk_forward.split_start_year,
    )
    selected_folds = [
        fold for fold in folds if fold.fold_id >= strategy.start_fold
    ]
    if strategy.max_folds is not None:
        selected_folds = selected_folds[: strategy.max_folds]
    if not selected_folds:
        raise ValueError("no walk-forward folds selected")

    device = _resolve_device(base_config.environment.device)
    amp_dtype = _resolve_amp_dtype(base_config.environment.amp_dtype)
    if device.type == "cuda":
        torch.backends.cuda.matmul.allow_tf32 = bool(
            base_config.environment.use_tensor_cores
        )
    features = torch.as_tensor(
        panel.features,
        device=device,
        dtype=torch.float32,
    )
    reference_returns = torch.as_tensor(
        market.reference_log_returns(strategy.reference_product),
        device=device,
        dtype=torch.float32,
    )
    reference_valid = torch.as_tensor(
        market.reference_tradable_mask(strategy.reference_product),
        device=device,
        dtype=torch.bool,
    )
    rng = np.random.default_rng(strategy.seed)

    for fold in selected_folds:
        train_indices = decision_indices_for_futures_day_session(
            panel,
            market,
            fold.train_indices,
            lookback=base_config.training.lookback,
            lookback_context=base_config.walk_forward.lookback_context,
            reference_product=strategy.reference_product,
        )
        val_indices = decision_indices_for_futures_day_session(
            panel,
            market,
            fold.val_indices,
            lookback=base_config.training.lookback,
            lookback_context=base_config.walk_forward.lookback_context,
            reference_product=strategy.reference_product,
        )
        test_indices = decision_indices_for_futures_day_session(
            panel,
            market,
            fold.test_indices,
            lookback=base_config.training.lookback,
            lookback_context=base_config.walk_forward.lookback_context,
            reference_product=strategy.reference_product,
        )
        if min(train_indices.size, val_indices.size, test_indices.size) == 0:
            raise ValueError(
                f"fold {fold.fold_id} has no executable train/val/test futures rows"
            )
        model = build_index_futures_model(base_config, panel, strategy).to(device)
        forward: nn.Module = _PanelSlabForward(model).to(device)
        if strategy.enable_torch_compile:
            forward = torch.compile(
                forward,
                dynamic=False,
                options={"triton.cudagraphs": False},
            )
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=strategy.learning_rate,
            weight_decay=strategy.weight_decay,
        )
        scaler = torch.amp.GradScaler(
            "cuda",
            enabled=(device.type == "cuda" and amp_dtype == torch.float16),
        )
        best_val = float("inf")
        best_state: dict[str, torch.Tensor] | None = None
        for epoch in range(1, strategy.epochs + 1):
            model.train()
            train_loss_sum = 0.0
            train_rows = 0
            for batch_indices in _iter_contiguous_batches(
                train_indices,
                strategy.batch_size,
                shuffle=True,
                rng=rng,
            ):
                optimizer.zero_grad(set_to_none=True)
                with _autocast(device, amp_dtype):
                    exposure = _forward_batch(
                        forward,
                        features,
                        panel,
                        batch_indices,
                        lookback=int(base_config.training.lookback),
                        device=device,
                    )
                selected = torch.as_tensor(
                    batch_indices,
                    device=device,
                    dtype=torch.long,
                )
                loss = tw_index_futures_log_utility_loss(
                    exposure.float(),
                    reference_returns.index_select(0, selected),
                    reference_valid.index_select(0, selected),
                    round_trip_cost_rate=(
                        strategy.continuous_round_trip_cost_rate
                    ),
                    max_abs_exposure=strategy.max_abs_exposure,
                )
                scaler.scale(loss).backward()
                scaler.step(optimizer)
                scaler.update()
                train_loss_sum += float(loss.detach().cpu().item()) * len(
                    batch_indices
                )
                train_rows += len(batch_indices)
            _val_exposure, val_loss = _evaluate_exposures(
                forward,
                features,
                panel,
                market,
                val_indices,
                reference_product=strategy.reference_product,
                batch_size=strategy.eval_batch_size,
                lookback=int(base_config.training.lookback),
                device=device,
                amp_dtype=amp_dtype,
                round_trip_cost_rate=(
                    strategy.continuous_round_trip_cost_rate
                ),
                max_abs_exposure=strategy.max_abs_exposure,
            )
            print(
                f"fold={fold.fold_id} epoch={epoch} "
                f"train_loss={train_loss_sum / max(1, train_rows):.8f} "
                f"val_loss={val_loss:.8f}",
                flush=True,
            )
            if val_loss < best_val:
                best_val = val_loss
                best_state = {
                    name: value.detach().cpu().clone()
                    for name, value in model.state_dict().items()
                }
        if best_state is None:
            raise RuntimeError(f"fold {fold.fold_id} produced no checkpoint")
        model.load_state_dict(best_state)
        test_exposure, test_loss = _evaluate_exposures(
            forward,
            features,
            panel,
            market,
            test_indices,
            reference_product=strategy.reference_product,
            batch_size=strategy.eval_batch_size,
            lookback=int(base_config.training.lookback),
            device=device,
            amp_dtype=amp_dtype,
            round_trip_cost_rate=strategy.continuous_round_trip_cost_rate,
            max_abs_exposure=strategy.max_abs_exposure,
        )
        test_market = _subset_market(market, test_indices)
        integer_result = run_tw_index_futures_day_integer(
            test_exposure,
            test_market,
            initial_capital=strategy.initial_capital,
            max_abs_exposure=strategy.max_abs_exposure,
            cost_schedule=strategy.cost_schedule,
        )
        fold_dir = strategy.output_dir / f"fold_{fold.fold_id:02d}"
        _save_fold_artifacts(
            fold_dir,
            checkpoint={
                "model_state_dict": best_state,
                "fold_id": fold.fold_id,
                "train_years": fold.train_years,
                "val_years": fold.val_years,
                "test_years": fold.test_years,
                "feature_names": panel.feature_names,
                "symbols": panel.symbols,
                "model_config": asdict(
                    base_config.training.transformer_base_portfolio
                ),
                "strategy": asdict(strategy),
                "base_experiment_config": str(
                    strategy.base_experiment_config
                ),
                "base_experiment_config_sha256": _sha256_path(
                    strategy.base_experiment_config
                ),
                "futures_data_path": str(strategy.futures_data_path),
                "futures_data_sha256": _sha256_path(
                    strategy.futures_data_path
                ),
                "taifex_futures_data_contract_version": (
                    TAIFEX_FUTURES_DATA_CONTRACT_VERSION
                ),
                "tw_index_futures_day_backtest_contract_version": (
                    TW_INDEX_FUTURES_DAY_BACKTEST_CONTRACT_VERSION
                ),
            },
            market=test_market,
            integer_result=integer_result,
            val_loss=best_val,
            test_loss=test_loss,
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        default="configs/markets/tw_index_futures_day.yaml",
    )
    args = parser.parse_args()
    train(Path(args.config).expanduser().resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
