from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Iterable

import json
import numpy as np

from stockagent.backtest.report import (
    compute_god_mode_returns,
    compute_metrics,
    generate_annual_report,
    plot_annual_performance,
    plot_equity_curve,
    plot_equity_curve_log,
)
from stockagent.backtest.simulator import run_backtest, run_backtest_cupy, run_backtest_integer_shares
from stockagent.config import ExperimentConfig
from stockagent.data.panel import PanelData
from stockagent.data.walkforward import WalkForwardFold
from stockagent.evaluation.metrics import compute_ic_series, compute_ic_series_cupy, ic_summary
from stockagent.training.trainer import FoldResult, _refresh_walkforward_artifacts, _save_holdings_csv

try:
    import cupy as cp
except Exception:  # pragma: no cover - optional GPU dependency
    cp = None


def _valid_indices(date_indices: np.ndarray, lookback: int) -> np.ndarray:
    sorted_idx = np.array(sorted(date_indices.tolist()), dtype=np.int64)
    fold_start_idx = int(sorted_idx[0])
    min_valid_idx = fold_start_idx + lookback - 1
    valid = sorted_idx[sorted_idx > min_valid_idx]
    if valid.size == 0:
        raise ValueError(f"Fold has insufficient data for lookback={lookback}.")
    return valid


def _is_gpu_enabled(config: ExperimentConfig) -> bool:
    return config.environment.device == "cuda" and cp is not None


def _masked_softmax(scores, mask, use_gpu: bool):
    xp = cp if use_gpu else np
    masked_scores = xp.where(mask, scores, -1e30)
    row_max = xp.max(masked_scores, axis=1, keepdims=True)
    stable = masked_scores - row_max
    exp_scores = xp.exp(stable, dtype=xp.float64) * mask.astype(xp.float64)
    denom = exp_scores.sum(axis=1, keepdims=True)
    safe = exp_scores / xp.clip(denom, 1e-12, None)
    safe = xp.where(denom > 0, safe, 0.0)
    safe = xp.where(xp.isfinite(safe), safe, 0.0)
    return safe.astype(xp.float32)


def _flatten_windows_vectorized(features, valid_indices: np.ndarray, lookback: int, use_gpu: bool):
    xp = cp if use_gpu else np
    idx = xp.asarray(valid_indices, dtype=xp.int64)
    offsets = xp.arange(lookback, dtype=xp.int64)
    gather_idx = idx[:, None] - (lookback - 1) + offsets[None, :]
    windows = features[gather_idx]  # [N, L, S, F]
    return windows.transpose(0, 2, 1, 3).reshape(windows.shape[0] * windows.shape[2], lookback * windows.shape[3])


def _prepare_fold_arrays(
    panel_arrays: dict[str, object],
    date_indices: np.ndarray,
    lookback: int,
    cache: dict[tuple[tuple[int, ...], int, bool], dict[str, object]],
    use_gpu: bool,
) -> dict[str, object]:
    valid = _valid_indices(date_indices, lookback)
    key = (tuple(valid.tolist()), lookback, use_gpu)
    cached = cache.get(key)
    if cached is not None:
        return cached

    features = panel_arrays["features"]
    returns = panel_arrays["returns"]
    tradable = panel_arrays["tradable"]
    benchmark = panel_arrays["benchmark"]
    dates = panel_arrays["dates"]

    x_flat = _flatten_windows_vectorized(features, valid, lookback, use_gpu)
    idx = (cp.asarray(valid) if use_gpu else valid)
    future_returns = returns[idx]
    tradable_mask = tradable[idx]
    benchmark_eval = benchmark[idx]
    fold_view = {
        "valid": valid,
        "x_flat": x_flat,
        "future_returns": future_returns,
        "tradable_mask": tradable_mask,
        "benchmark": benchmark_eval,
        "dates": dates[valid],
    }
    cache[key] = fold_view
    return fold_view


def _build_xgb_params(config: ExperimentConfig, use_gpu: bool) -> dict[str, object]:
    params: dict[str, object] = {
        "max_depth": int(config.training.xgb_max_depth),
        "learning_rate": float(config.training.xgb_learning_rate),
        "subsample": float(config.training.xgb_subsample),
        "colsample_bytree": float(config.training.xgb_colsample_bytree),
        "lambda": float(config.training.xgb_reg_lambda),
        "objective": "reg:squarederror",
        "seed": 42,
    }
    if use_gpu:
        params["tree_method"] = "hist"
        params["device"] = "cuda"
    else:
        params["tree_method"] = "hist"
    return params


def _build_xgb_booster(config: ExperimentConfig, x_train, y_train, use_gpu: bool):
    try:
        import xgboost as xgb
    except ImportError as exc:
        raise ImportError(
            "XGBoost is required for model_name=xgboost. Install with: pip install xgboost"
        ) from exc

    params = _build_xgb_params(config, use_gpu)
    dtrain = xgb.QuantileDMatrix(x_train, label=y_train)
    num_boost_round = int(config.training.xgb_n_estimators)
    return xgb.train(params=params, dtrain=dtrain, num_boost_round=num_boost_round)


def _predict_scores(booster, x_eval, out_shape: tuple[int, int], use_gpu: bool):
    raw_pred = booster.inplace_predict(x_eval)
    if use_gpu:
        pred = cp.asarray(raw_pred, dtype=cp.float32).reshape(out_shape)
    else:
        pred = np.asarray(raw_pred, dtype=np.float32).reshape(out_shape)
    return pred


def _write_summary(results: list[FoldResult], output_path: Path) -> None:
    summary_path = output_path / "summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump([asdict(result) for result in results], handle, indent=2)


def run_training_xgboost(
    panel: PanelData,
    folds: Iterable[WalkForwardFold],
    config: ExperimentConfig,
    output_dir: str | Path,
) -> list[FoldResult]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    results: list[FoldResult] = []
    lookback = int(config.training.lookback)
    use_gpu = _is_gpu_enabled(config)

    features_np = np.asarray(panel.features, dtype=np.float32)
    returns_np = np.nan_to_num(panel.returns_1d, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32, copy=False)
    tradable_np = panel.tradable_mask.astype(bool)
    benchmark_np = np.nan_to_num(panel.benchmark_returns, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32, copy=False)

    panel_arrays: dict[str, object] = {
        "features": cp.asarray(features_np) if use_gpu else features_np,
        "returns": cp.asarray(returns_np) if use_gpu else returns_np,
        "tradable": cp.asarray(tradable_np) if use_gpu else tradable_np,
        "benchmark": cp.asarray(benchmark_np) if use_gpu else benchmark_np,
        "dates": panel.dates,
    }
    fold_cache: dict[tuple[tuple[int, ...], int, bool], dict[str, object]] = {}

    fold_list = list(folds)
    for fold in fold_list:
        print(
            f"[XGBoost] Fold {fold.fold_id}: train={fold.train_years} "
            f"val={fold.val_years} test={fold.test_years} gpu={use_gpu}"
        )

        train_view = _prepare_fold_arrays(panel_arrays, fold.train_indices, lookback, fold_cache, use_gpu)
        val_view = _prepare_fold_arrays(panel_arrays, fold.val_indices, lookback, fold_cache, use_gpu)
        test_view = _prepare_fold_arrays(panel_arrays, fold.test_indices, lookback, fold_cache, use_gpu)

        train_mask_flat = train_view["tradable_mask"].reshape(-1)
        x_train = train_view["x_flat"][train_mask_flat]
        y_train = train_view["future_returns"].reshape(-1)[train_mask_flat]

        booster = _build_xgb_booster(config, x_train, y_train, use_gpu)

        val_shape = tuple(val_view["future_returns"].shape)
        test_shape = tuple(test_view["future_returns"].shape)
        val_scores = _predict_scores(booster, val_view["x_flat"], val_shape, use_gpu)
        test_scores = _predict_scores(booster, test_view["x_flat"], test_shape, use_gpu)

        val_weights = _masked_softmax(val_scores, val_view["tradable_mask"], use_gpu)
        test_weights = _masked_softmax(test_scores, test_view["tradable_mask"], use_gpu)

        if use_gpu:
            val_backtest = run_backtest_cupy(
                val_weights,
                val_view["future_returns"],
                val_view["tradable_mask"],
                val_view["benchmark"],
                fee_per_side=config.trading.fee_per_side,
            )
            test_backtest = run_backtest_cupy(
                test_weights,
                test_view["future_returns"],
                test_view["tradable_mask"],
                test_view["benchmark"],
                fee_per_side=config.trading.fee_per_side,
            )
            val_ic = ic_summary(
                compute_ic_series_cupy(
                    val_scores,
                    val_view["future_returns"],
                    val_view["tradable_mask"],
                )
            )
            test_ic = ic_summary(
                compute_ic_series_cupy(
                    test_scores,
                    test_view["future_returns"],
                    test_view["tradable_mask"],
                )
            )
        else:
            val_backtest = run_backtest(
                val_weights,
                val_view["future_returns"],
                val_view["tradable_mask"],
                val_view["benchmark"],
                fee_per_side=config.trading.fee_per_side,
            )
            test_backtest = run_backtest(
                test_weights,
                test_view["future_returns"],
                test_view["tradable_mask"],
                test_view["benchmark"],
                fee_per_side=config.trading.fee_per_side,
            )
            val_ic = ic_summary(compute_ic_series(val_scores, val_view["future_returns"], val_view["tradable_mask"]))
            test_ic = ic_summary(compute_ic_series(test_scores, test_view["future_returns"], test_view["tradable_mask"]))

        test_dates = np.asarray(test_view["dates"])
        valid_idx = test_view["valid"]
        test_open_prices = panel.open_prices[valid_idx]
        test_close_prices = panel.close_prices[valid_idx]
        execution_mode = config.trading.execution_mode
        if execution_mode == "intraday_next_open":
            buy_fee_rate = config.trading.intraday_buy_fee_rate
            sell_fee_rate = config.trading.intraday_sell_fee_rate
        else:
            buy_fee_rate = config.trading.overnight_buy_fee_rate
            sell_fee_rate = config.trading.overnight_sell_fee_rate

        test_weights_np = cp.asnumpy(test_weights) if use_gpu else np.asarray(test_weights)
        test_returns_np = cp.asnumpy(test_view["future_returns"]) if use_gpu else np.asarray(test_view["future_returns"])
        test_mask_np = cp.asnumpy(test_view["tradable_mask"]) if use_gpu else np.asarray(test_view["tradable_mask"])
        test_bench_np = cp.asnumpy(test_view["benchmark"]) if use_gpu else np.asarray(test_view["benchmark"])

        test_bt, holdings_records = run_backtest_integer_shares(
            weights=test_weights_np,
            future_returns=test_returns_np,
            tradable_mask=test_mask_np,
            benchmark_returns=test_bench_np,
            initial_capital=1_000_000.0,
            buy_fee_rate=buy_fee_rate,
            sell_fee_rate=sell_fee_rate,
            min_fee=config.trading.min_fee,
            execution_mode=execution_mode,
            lot_size=config.trading.lot_size,
            settlement_delay_days=config.trading.settlement_delay_days,
            open_prices=test_open_prices,
            close_prices=test_close_prices,
            symbols=panel.symbols,
            dates=test_dates,
        )

        val_metrics = compute_metrics(val_backtest)
        test_metrics = compute_metrics(test_bt)

        fold_result = FoldResult(
            fold_id=fold.fold_id,
            train_years=fold.train_years,
            val_years=fold.val_years,
            test_years=fold.test_years,
            best_val_loss=float(-val_metrics.get("sharpe", 0.0)),
            val_ic=val_ic,
            val_metrics=val_metrics,
            test_ic=test_ic,
            test_metrics=test_metrics,
        )
        results.append(fold_result)

        fold_dir = output_path / f"fold_{fold.fold_id:02d}"
        fold_dir.mkdir(parents=True, exist_ok=True)
        booster.save_model(str(fold_dir / "xgboost_model.json"))
        with (fold_dir / "metrics.json").open("w", encoding="utf-8") as handle:
            json.dump(asdict(fold_result), handle, indent=2)
        np.savez_compressed(
            fold_dir / "test_backtest.npz",
            strategy_returns=test_bt.strategy_returns,
            benchmark_returns=test_bt.benchmark_returns,
            turnovers=test_bt.turnovers,
            weights_history=test_bt.weights_history,
            dates=test_dates,
        )

        god_returns = compute_god_mode_returns(test_returns_np, test_mask_np)
        report = generate_annual_report(test_bt, test_dates, god_returns=god_returns)
        with (fold_dir / "annual_report.txt").open("w", encoding="utf-8") as handle:
            handle.write(report)

        plot_equity_curve(test_bt, test_dates, fold_dir / "equity_curve.png", god_returns=god_returns)
        plot_equity_curve_log(test_bt, test_dates, fold_dir / "equity_curve_log.png", god_returns=god_returns)
        plot_annual_performance(test_bt, test_dates, fold_dir / "annual_performance.png")
        _save_holdings_csv(fold_dir / "holdings.csv", holdings_records)

        _refresh_walkforward_artifacts(output_path, results)

    _write_summary(results, output_path)
    return results
