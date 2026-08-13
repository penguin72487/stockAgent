#!/usr/bin/env python3
"""First-principles audit of the daily TX/MTX/TMF + long-TXO strategy.

This script deliberately starts from saved fold requests and the canonical
integer executor.  It does not infer performance from generic stock-weight
tables, because a daily-flat derivative strategy has zero carried stock
weights by construction.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from datetime import date, datetime, timezone
import json
import math
from pathlib import Path
import sys
from typing import Any, Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from scipy import stats

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import train
from stockagent.backtest.tw_index_derivatives_day import (
    OptionDayCostSchedule,
    run_tw_index_derivatives_day_integer,
)
from stockagent.backtest.tw_index_futures import FuturesCostSchedule
from stockagent.config import load_config
from stockagent.data.panel import build_panel
from stockagent.data.tw_index_derivatives_day import (
    TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4,
    TAIFEX_OPTION_CANDIDATE_CAPACITY,
    build_causal_derivative_day_candidates,
)
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    TaiwanIndexFuturesDaySession,
    load_taifex_index_futures_day_session,
)
from stockagent.data.tw_index_options_daily import (
    combine_taifex_option_chains,
    load_taifex_option_full_chain,
)
from stockagent.training.trainer import (
    _build_execution_runtime,
    _load_completed_fold_result,
    _refresh_walkforward_artifacts,
)


ROOT_DEFAULT = Path(
    "artifacts/markets/"
    "tw_index_derivatives_day_multi_basis_100m_relative_tenor_v5_dual5090"
)
OUTPUT_DEFAULT = Path(
    "artifacts/analysis/"
    "tw_index_derivatives_day_v5_first_principles_20260813"
)
CONFIG_DEFAULT = Path(
    "configs/markets/tw_index_derivatives_day_multi_basis_dual_5090.yaml"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG_DEFAULT)
    parser.add_argument("--run-root", type=Path, default=ROOT_DEFAULT)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DEFAULT)
    parser.add_argument(
        "--skip-walkforward-repair",
        action="store_true",
        help="Do not rebuild the root stitched report from every isolated fold.",
    )
    return parser.parse_args()


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return _jsonable(value.tolist())
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    columns = list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(_jsonable(rows))


def _load_npz(path: Path) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as payload:
        return {key: payload[key].copy() for key in payload.files}


def _safe_simple_to_log(simple: np.ndarray) -> np.ndarray:
    values = np.asarray(simple, dtype=np.float64)
    return np.log1p(np.clip(values, -0.999999999999, None))


def _return_metrics_from_log(log_returns: np.ndarray) -> dict[str, float]:
    values = np.nan_to_num(
        np.asarray(log_returns, dtype=np.float64),
        nan=0.0,
        posinf=0.0,
        neginf=math.log(1e-12),
    )
    rows = int(values.size)
    if rows == 0:
        return {
            "rows": 0,
            "cumulative_return": 0.0,
            "cagr": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "daily_hit_rate": 0.0,
        }
    total_log = float(values.sum())
    cumulative = float(math.expm1(max(total_log, math.log(1e-12))))
    years = rows / 252.0
    cagr = float(math.expm1(total_log / max(years, 1e-12)))
    standard_deviation = float(values.std(ddof=1)) if rows > 1 else 0.0
    sharpe = (
        float(values.mean() / standard_deviation * math.sqrt(252.0))
        if standard_deviation > 0.0
        else 0.0
    )
    nav = np.exp(np.clip(np.cumsum(values), math.log(1e-12), 700.0))
    running_high = np.maximum.accumulate(np.concatenate(([1.0], nav)))
    nav_with_start = np.concatenate(([1.0], nav))
    drawdown = nav_with_start / running_high - 1.0
    return {
        "rows": rows,
        "cumulative_return": cumulative,
        "cagr": cagr,
        "sharpe": sharpe,
        "max_drawdown": float(drawdown.min()),
        "daily_hit_rate": float((values > 0.0).mean()),
    }


def _selected_futures_market(
    market: TaiwanIndexFuturesDaySession,
    requested_dates: np.ndarray,
) -> TaiwanIndexFuturesDaySession:
    market_dates = np.asarray(market.dates, dtype="datetime64[D]")
    requested = np.asarray(requested_dates, dtype="datetime64[D]")
    indices = np.searchsorted(market_dates, requested)
    if bool(np.any(indices >= market_dates.size)) or not np.array_equal(
        market_dates[indices], requested
    ):
        raise ValueError("futures market does not cover every deployment date")

    def selected(value: np.ndarray | None) -> np.ndarray | None:
        return None if value is None else np.asarray(value)[indices].copy()

    return TaiwanIndexFuturesDaySession(
        dates=market_dates[indices].copy(),
        products=market.products,
        contract_months=np.asarray(market.contract_months)[indices].copy(),
        open_prices=np.asarray(market.open_prices)[indices].copy(),
        high_prices=np.asarray(market.high_prices)[indices].copy(),
        low_prices=np.asarray(market.low_prices)[indices].copy(),
        close_prices=np.asarray(market.close_prices)[indices].copy(),
        volumes=np.asarray(market.volumes)[indices].copy(),
        log_returns=np.asarray(market.log_returns)[indices].copy(),
        tradable_mask=np.asarray(market.tradable_mask)[indices].copy(),
        multipliers=np.asarray(market.multipliers).copy(),
        rolling_buy_hold_log_returns=selected(
            market.rolling_buy_hold_log_returns
        ),
        rolling_buy_hold_tradable_mask=selected(
            market.rolling_buy_hold_tradable_mask
        ),
        front_month_roll_mask=selected(market.front_month_roll_mask),
        tenor_contract_months=selected(market.tenor_contract_months),
        tenor_open_prices=selected(market.tenor_open_prices),
        tenor_high_prices=selected(market.tenor_high_prices),
        tenor_low_prices=selected(market.tenor_low_prices),
        tenor_close_prices=selected(market.tenor_close_prices),
        tenor_volumes=selected(market.tenor_volumes),
        tenor_log_returns=selected(market.tenor_log_returns),
        tenor_tradable_mask=selected(market.tenor_tradable_mask),
    )


def _load_and_attach_market(config: Any) -> tuple[Any, Any, Any]:
    panel = build_panel(config.data.parquet_root, **train._build_panel_kwargs(config))
    futures = load_taifex_index_futures_day_session(
        config.trading.tw_index_futures_data_path,
        panel_dates=panel.dates,
    )
    monthly = load_taifex_option_full_chain(
        config.trading.tw_index_options_monthly_data_path,
        expected_series_scope="monthly",
        panel_dates=panel.dates,
    )
    weekly = load_taifex_option_full_chain(
        config.trading.tw_index_options_weekly_data_path,
        expected_series_scope="weekly",
        panel_dates=panel.dates,
    )
    chain = combine_taifex_option_chains(monthly, weekly)
    candidates = build_causal_derivative_day_candidates(
        futures,
        chain,
        reference_product=config.trading.tw_index_futures_reference_product,
        fixed_fee_per_contract_per_side_twd=(
            config.trading.tw_index_derivatives_day_option_fixed_fee_per_contract_per_side_twd
        ),
        transaction_tax_rate=(
            config.trading.tw_index_derivatives_day_option_transaction_tax_rate
        ),
        slippage_points_per_side=(
            config.trading.tw_index_derivatives_day_option_slippage_points_per_side
        ),
    )
    panel.index_futures_day_session = futures
    panel.index_futures_reference_product = (
        config.trading.tw_index_futures_reference_product
    )
    panel.index_options_monthly_day_session = monthly
    panel.index_options_weekly_day_session = weekly
    panel.index_options_chain_day_session = chain
    panel.index_derivatives_day_candidates = candidates
    panel.index_derivatives_candidate_features = (
        candidates.option_candidate_features
    )
    panel.index_derivatives_candidate_mask = candidates.candidate_mask()
    panel.index_derivatives_simple_returns = candidates.simple_returns()
    return panel, futures, candidates


def _run_scenario(
    name: str,
    actions: np.ndarray,
    market: TaiwanIndexFuturesDaySession,
    candidates: Any,
    *,
    initial_capital: float,
    maximum_capital_fraction: float,
    futures_schedule: FuturesCostSchedule,
    option_schedule: OptionDayCostSchedule,
) -> tuple[dict[str, Any], Any]:
    result = run_tw_index_derivatives_day_integer(
        actions,
        market,
        candidates,
        initial_capital=initial_capital,
        maximum_capital_fraction=maximum_capital_fraction,
        futures_cost_schedule=futures_schedule,
        option_cost_schedule=option_schedule,
    )
    metrics = _return_metrics_from_log(_safe_simple_to_log(result.strategy_returns))
    prior_equity = np.concatenate(
        ([float(initial_capital)], np.asarray(result.equity[:-1], dtype=np.float64))
    )
    safe_prior = np.maximum(prior_equity, 1e-12)
    costs = result.fees_twd + result.tax_twd + result.slippage_twd
    metrics.update(
        {
            "scenario": name,
            "mean_turnover": float(np.mean(result.turnovers)),
            "mean_requested_gross": float(
                np.mean(
                    np.abs(result.requested_actions[:, :6]).sum(axis=1)
                    + result.requested_actions[:, 6:].sum(axis=1)
                )
            ),
            "mean_executed_gross": float(
                np.mean(
                    np.abs(result.executed_actions[:, :6]).sum(axis=1)
                    + result.executed_actions[:, 6:].sum(axis=1)
                )
            ),
            "gross_pnl_twd": float(result.gross_pnl_twd.sum()),
            "fees_twd": float(result.fees_twd.sum()),
            "tax_twd": float(result.tax_twd.sum()),
            "slippage_twd": float(result.slippage_twd.sum()),
            "total_cost_twd": float(costs.sum()),
            "mean_daily_cost_over_nav": float(np.mean(costs / safe_prior)),
            "annualized_cost_over_nav": float(np.mean(costs / safe_prior) * 252.0),
            "terminal_equity_twd": float(result.equity[-1]),
            "terminal_alive": bool(result.alive[-1]),
        }
    )
    return metrics, result


def _top_k_without_renormalization(actions: np.ndarray, k: int) -> np.ndarray:
    output = np.zeros_like(actions)
    count = min(int(k), int(actions.shape[1]))
    if count <= 0:
        return output
    indices = np.argpartition(np.abs(actions), -count, axis=1)[:, -count:]
    rows = np.arange(actions.shape[0])[:, None]
    output[rows, indices] = actions[rows, indices]
    return output


def _action_diagnostics(
    actions: np.ndarray,
    candidates: Any,
    exact: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    futures = actions[:, :TAIFEX_INDEX_FUTURES_TENOR_SLOTS]
    options = np.maximum(actions[:, TAIFEX_INDEX_FUTURES_TENOR_SLOTS:], 0.0)
    abs_all = np.concatenate((np.abs(futures), options), axis=1)
    gross = abs_all.sum(axis=1)
    option_gross = options.sum(axis=1)
    future_gross = np.abs(futures).sum(axis=1)
    shares = np.divide(
        abs_all,
        np.maximum(gross[:, None], 1e-12),
        out=np.zeros_like(abs_all),
    )
    hhi = np.square(shares).sum(axis=1)
    top_share = shares.max(axis=1)
    active_1e8 = (abs_all > 1e-8).sum(axis=1)
    active_1e4 = (abs_all > 1e-4).sum(axis=1)
    visible_options = np.asarray(candidates.option_candidate_mask, dtype=bool).sum(
        axis=1
    )
    executable_options = np.isfinite(candidates.option_simple_returns).sum(axis=1)
    invalid_requested = (
        (options > 1e-8) & ~np.isfinite(candidates.option_simple_returns)
    ).sum(axis=1)
    option_days = option_gross > 1e-8
    majority_option_days = option_gross > 0.5

    supports = abs_all > 1e-4
    intersections = np.logical_and(supports[1:], supports[:-1]).sum(axis=1)
    unions = np.logical_or(supports[1:], supports[:-1]).sum(axis=1)
    jaccard = np.divide(
        intersections,
        np.maximum(unions, 1),
        dtype=np.float64,
    )
    diagnostics = {
        "rows": int(actions.shape[0]),
        "action_width": int(actions.shape[1]),
        "mean_gross": float(gross.mean()),
        "median_gross": float(np.median(gross)),
        "boundary_fraction_gross_ge_0_979": float((gross >= 0.979).mean()),
        "mean_futures_gross": float(future_gross.mean()),
        "mean_options_gross": float(option_gross.mean()),
        "days_with_any_option_exposure": int(option_days.sum()),
        "fraction_days_with_any_option_exposure": float(option_days.mean()),
        "days_with_majority_option_exposure": int(majority_option_days.sum()),
        "maximum_option_gross": float(option_gross.max()),
        "conditional_mean_option_gross": float(
            option_gross[option_days].mean() if option_days.any() else 0.0
        ),
        "conditional_mean_return_on_option_days": float(
            exact.strategy_returns[option_days].mean() if option_days.any() else 0.0
        ),
        "conditional_compounded_return_on_option_days": float(
            np.prod(1.0 + exact.strategy_returns[option_days]) - 1.0
            if option_days.any()
            else 0.0
        ),
        "gross_pnl_twd_on_option_days": float(
            exact.gross_pnl_twd[option_days].sum() if option_days.any() else 0.0
        ),
        "mean_cash_fraction": float(np.maximum(1.0 - gross, 0.0).mean()),
        "mean_active_gt_1e8": float(active_1e8.mean()),
        "mean_active_gt_1e4": float(active_1e4.mean()),
        "mean_hhi": float(hhi.mean()),
        "effective_bets_inverse_hhi": float(np.mean(1.0 / np.maximum(hhi, 1e-12))),
        "mean_largest_action_share": float(top_share.mean()),
        "mean_support_jaccard_gt_1e4": float(jaccard.mean()),
        "mean_visible_options": float(visible_options.mean()),
        "mean_executable_options": float(executable_options.mean()),
        "mean_nonexecuted_requested_option_slots": float(invalid_requested.mean()),
        "mean_integer_executed_gross": float(
            (
                np.abs(exact.executed_actions[:, :6]).sum(axis=1)
                + exact.executed_actions[:, 6:].sum(axis=1)
            ).mean()
        ),
    }
    day_rows: list[dict[str, Any]] = []
    for row, date in enumerate(candidates.dates):
        day_rows.append(
            {
                "date": str(date),
                "requested_gross": float(gross[row]),
                "futures_gross": float(future_gross[row]),
                "options_gross": float(option_gross[row]),
                "active_gt_1e4": int(active_1e4[row]),
                "hhi": float(hhi[row]),
                "largest_action_share": float(top_share[row]),
                "visible_options": int(visible_options[row]),
                "executable_options": int(executable_options[row]),
                "integer_turnover": float(exact.turnovers[row]),
                "integer_simple_return": float(exact.strategy_returns[row]),
                "gross_pnl_twd": float(exact.gross_pnl_twd[row]),
                "fees_twd": float(exact.fees_twd[row]),
                "tax_twd": float(exact.tax_twd[row]),
                "slippage_twd": float(exact.slippage_twd[row]),
            }
        )
    return diagnostics, day_rows


def _source_option_quality(
    monthly_path: Path,
    weekly_path: Path,
    *,
    fee: float,
    tax: float,
    slippage: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    scan = pl.scan_parquet([monthly_path, weekly_path])
    key = ["date", "option_series", "strike", "option_right"]
    quality = scan.select(
        pl.len().alias("rows"),
        pl.col("date").min().alias("min_date"),
        pl.col("date").max().alias("max_date"),
        pl.struct(key).n_unique().alias("unique_contract_days"),
        pl.col("executable").sum().alias("executable_rows"),
        (pl.col("open").is_null() | (pl.col("open") <= 0)).sum().alias(
            "invalid_open_rows"
        ),
        (pl.col("close").is_null() | (pl.col("close") <= 0)).sum().alias(
            "invalid_close_rows"
        ),
        (pl.col("volume") <= 0).sum().alias("nonpositive_volume_rows"),
        (pl.col("last_bid").is_not_null() & pl.col("last_ask").is_not_null()).sum().alias(
            "rows_with_last_bid_and_ask"
        ),
    ).collect()
    summary = quality.to_dicts()[0]
    summary["duplicate_key_rows"] = int(summary["rows"] - summary["unique_contract_days"])
    summary["executable_fraction"] = float(
        summary["executable_rows"] / max(summary["rows"], 1)
    )
    summary["last_quote_pair_fraction"] = float(
        summary["rows_with_last_bid_and_ask"] / max(summary["rows"], 1)
    )

    executable_quotes = scan.filter(pl.col("executable")).select(
        pl.len().alias("executable_rows"),
        (
            pl.col("last_bid").is_not_null()
            & pl.col("last_ask").is_not_null()
        ).sum().alias("executable_rows_with_last_bid_and_ask"),
    ).collect().to_dicts()[0]
    summary.update(executable_quotes)
    summary["executable_last_quote_pair_fraction"] = float(
        executable_quotes["executable_rows_with_last_bid_and_ask"]
        / max(executable_quotes["executable_rows"], 1)
    )

    valid = scan.filter(
        pl.col("executable")
        & pl.col("open").is_finite()
        & (pl.col("open") > 0)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0)
    ).with_columns(
        (pl.col("close") / pl.col("open") - 1.0).alias("gross_return"),
        (
            pl.col("close") / pl.col("open")
            - 1.0
            - (2.0 * fee) / (pl.col("open") * 50.0)
            - tax * pl.col("close") / pl.col("open")
            - (2.0 * slippage) / pl.col("open")
        ).alias("net_return"),
        pl.when(pl.col("open") < 1.0)
        .then(pl.lit("<1"))
        .when(pl.col("open") < 5.0)
        .then(pl.lit("1-5"))
        .when(pl.col("open") < 20.0)
        .then(pl.lit("5-20"))
        .when(pl.col("open") < 100.0)
        .then(pl.lit("20-100"))
        .otherwise(pl.lit(">=100"))
        .alias("premium_bucket"),
    )
    grouped = (
        valid.group_by("premium_bucket")
        .agg(
            pl.len().alias("rows"),
            pl.col("open").median().alias("median_open"),
            pl.col("gross_return").mean().alias("mean_gross_return"),
            pl.col("net_return").mean().alias("mean_net_return"),
            pl.col("net_return").median().alias("median_net_return"),
            pl.col("net_return").quantile(0.01).alias("p01_net_return"),
            pl.col("net_return").quantile(0.99).alias("p99_net_return"),
            (pl.col("net_return") > 0).mean().alias("positive_fraction"),
        )
        .collect()
    )
    order = {"<1": 0, "1-5": 1, "5-20": 2, "20-100": 3, ">=100": 4}
    rows = sorted(grouped.to_dicts(), key=lambda row: order[row["premium_bucket"]])
    return summary, rows


def _training_diagnostics(
    run_root: Path,
    summary: list[dict[str, Any]],
    *,
    panel_dates: np.ndarray,
    model_parameters: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[int, list[dict[str, Any]]]]:
    curve_by_fold: dict[int, list[dict[str, Any]]] = {}
    rows: list[dict[str, Any]] = []
    dates = np.asarray(panel_dates, dtype="datetime64[D]")
    years = dates.astype("datetime64[Y]").astype(np.int64) + 1970
    for result in summary:
        fold_id = int(result["fold_id"])
        train_years = [int(year) for year in result["train_years"]]
        directory = run_root / ("train_" + "-".join(map(str, train_years)))
        curve_path = directory / "epoch_curve.jsonl"
        curve = [json.loads(line) for line in curve_path.read_text().splitlines() if line]
        curve_by_fold[fold_id] = curve
        val_values = np.asarray([entry["val_mean"] for entry in curve], dtype=np.float64)
        train_values = np.asarray([entry["train_loss"] for entry in curve], dtype=np.float64)
        best_index = int(np.argmin(val_values))
        target_rows = max(int(np.isin(years, train_years).sum()) - 31, 0)
        rows.append(
            {
                "fold_id": fold_id,
                "train_years": ",".join(map(str, train_years)),
                "train_target_days": target_rows,
                "epochs_ran": len(curve),
                "best_epoch": best_index + 1,
                "best_val_loss_curve": float(val_values[best_index]),
                "last_val_loss": float(val_values[-1]),
                "val_loss_std": float(val_values.std(ddof=1)),
                "first_train_loss": float(train_values[0]),
                "best_train_loss": float(train_values.min()),
                "last_train_loss": float(train_values[-1]),
                "model_parameters": int(model_parameters),
                "parameters_per_train_day": float(
                    model_parameters / max(target_rows, 1)
                ),
            }
        )
    best_epoch_fraction = np.asarray(
        [row["best_epoch"] / row["epochs_ran"] for row in rows], dtype=np.float64
    )
    aggregate = {
        "model_parameters": int(model_parameters),
        "feature_count": 98,
        "temporal_basis_families": 18,
        "temporal_basis_components": 4,
        "effective_projected_input_columns": 98 * (1 + 18 * 4),
        "median_train_target_days": float(
            np.median([row["train_target_days"] for row in rows])
        ),
        "fold1_parameters_per_train_day": float(rows[0]["parameters_per_train_day"]),
        "median_best_epoch_fraction": float(np.median(best_epoch_fraction)),
        "folds_best_in_first_10_epochs": int(
            sum(row["best_epoch"] <= 10 for row in rows)
        ),
        "median_val_loss_std": float(np.median([row["val_loss_std"] for row in rows])),
    }
    return aggregate, rows, curve_by_fold


def _fold_performance(
    run_root: Path,
    summary: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    for item in summary:
        fold_id = int(item["fold_id"])
        artifact = _load_npz(
            run_root / f"fold_{fold_id:02d}" / "deployment_test_backtest.npz"
        )
        strategy = _return_metrics_from_log(artifact["strategy_returns"])
        benchmark = _return_metrics_from_log(artifact["benchmark_returns"])
        owned_dates = np.asarray(artifact["dates"], dtype="datetime64[D]")
        rows.append(
            {
                "fold_id": fold_id,
                "val_year": int(item["val_years"][0]),
                "owned_test_year": (
                    int(str(owned_dates[0])[:4]) if owned_dates.size else None
                ),
                "owned_rows": int(owned_dates.size),
                "best_val_loss": float(item["best_val_loss"]),
                "val_cumulative_return": float(
                    item["val_metrics"]["cumulative_return"]
                ),
                "val_sharpe": float(item["val_metrics"]["sharpe"]),
                "deployment_cumulative_return": float(
                    strategy["cumulative_return"]
                ),
                "deployment_sharpe": float(strategy["sharpe"]),
                "deployment_max_drawdown": float(strategy["max_drawdown"]),
                "benchmark_cumulative_return": float(
                    benchmark["cumulative_return"]
                ),
                "mean_turnover": (
                    float(np.mean(artifact["turnovers"]))
                    if owned_dates.size
                    else 0.0
                ),
                "full_horizon_test_cumulative_return": float(
                    item["test_metrics"]["cumulative_return"]
                ),
                "full_horizon_continuous_cumulative_return": float(
                    item["test_continuous_surrogate_metrics"]["cumulative_return"]
                ),
            }
        )
    honest = [row for row in rows if row["fold_id"] <= 11 and row["owned_rows"] > 0]
    val = np.asarray([row["val_cumulative_return"] for row in honest])
    deployment = np.asarray(
        [row["deployment_cumulative_return"] for row in honest]
    )
    pearson = stats.pearsonr(val, deployment) if len(val) >= 3 else (math.nan, math.nan)
    spearman = stats.spearmanr(val, deployment) if len(val) >= 3 else (math.nan, math.nan)
    positive = np.asarray([row["deployment_cumulative_return"] > 0 for row in honest])
    aggregate = {
        "honest_owned_years": len(honest),
        "positive_owned_years": int(positive.sum()),
        "negative_owned_years": int((~positive).sum()),
        "val_to_next_year_pearson_r": float(pearson.statistic),
        "val_to_next_year_pearson_p": float(pearson.pvalue),
        "val_to_next_year_spearman_rho": float(spearman.statistic),
        "val_to_next_year_spearman_p": float(spearman.pvalue),
        "fold12_is_overlapping_latest_year_experiment": True,
    }
    return aggregate, rows


def _version_comparison(run_root: Path) -> list[dict[str, Any]]:
    markets_root = run_root.parent
    versions = {
        "v3_full_chain_attention": (
            markets_root
            / "tw_index_derivatives_day_multi_basis_100m_full_chain_v3_dual5090"
        ),
        "v4_relative_tenor_attention": (
            markets_root
            / "tw_index_derivatives_day_multi_basis_100m_relative_tenor_v4_dual5090"
        ),
        "v5_relative_tenor_last_only": run_root,
    }
    rows: list[dict[str, Any]] = []
    for version, root in versions.items():
        parts: list[np.ndarray] = []
        years: list[int] = []
        annual_returns: list[float] = []
        for path in sorted(root.glob("fold_*/deployment_test_backtest.npz")):
            payload = _load_npz(path)
            dates = np.asarray(payload["dates"], dtype="datetime64[D]")
            if dates.size == 0:
                continue
            log_returns = np.asarray(payload["strategy_returns"], dtype=np.float64)
            parts.append(log_returns)
            years.append(int(str(dates[0])[:4]))
            annual_returns.append(float(math.expm1(log_returns.sum())))
        combined = _return_metrics_from_log(np.concatenate(parts))
        rows.append(
            {
                "version": version,
                "years": len(years),
                "standalone_fold_compounded_return": combined["cumulative_return"],
                "median_owned_year_return": float(np.median(annual_returns)),
                "positive_owned_years": int(np.sum(np.asarray(annual_returns) > 0)),
                "worst_owned_year_return": float(np.min(annual_returns)),
                "best_owned_year_return": float(np.max(annual_returns)),
            }
        )
    return rows


def _model_parameter_count(run_root: Path) -> tuple[int, int]:
    import torch

    checkpoint = torch.load(
        run_root / "fold_01" / "checkpoint_best.pt",
        map_location="cpu",
        weights_only=False,
    )
    state = checkpoint["model_state_dict"]
    return int(sum(tensor.numel() for tensor in state.values())), int(
        checkpoint["epoch"]
    )


def _plot_equity_scenarios(
    path: Path,
    dates: np.ndarray,
    benchmark_log: np.ndarray,
    scenario_results: dict[str, Any],
) -> None:
    fig, ax = plt.subplots(figsize=(13, 6.5))
    benchmark_nav = np.exp(np.cumsum(np.nan_to_num(benchmark_log, nan=0.0)))
    ax.plot(dates, benchmark_nav, label="TX rolling buy-and-hold", linewidth=2.2)
    for name, color in [
        ("actual_user_tax_0.0002", "tab:red"),
        ("zero_cost", "tab:green"),
        ("causal_prior_close_ge_20", "tab:blue"),
        ("futures_only_no_renorm", "tab:purple"),
    ]:
        result = scenario_results[name]
        nav = np.cumprod(1.0 + np.asarray(result.strategy_returns))
        ax.plot(dates, np.maximum(nav, 1e-8), label=name, linewidth=1.6)
    ax.set_yscale("log")
    ax.set_title("Canonical stitched deployment: execution and cost counterfactuals")
    ax.set_ylabel("NAV (log scale, start=1)")
    ax.grid(alpha=0.25, which="both")
    ax.legend(loc="best", fontsize=8)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def _plot_fold_generalization(path: Path, fold_rows: list[dict[str, Any]]) -> None:
    honest = [row for row in fold_rows if row["fold_id"] <= 11 and row["owned_rows"]]
    labels = [str(row["owned_test_year"]) for row in honest]
    x = np.arange(len(honest))
    val = np.asarray([row["val_cumulative_return"] for row in honest])
    test = np.asarray([row["deployment_cumulative_return"] for row in honest])
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
    width = 0.38
    axes[0].bar(x - width / 2, val, width, label="selected validation")
    axes[0].bar(x + width / 2, test, width, label="next owned year")
    axes[0].axhline(0, color="black", linewidth=0.8)
    axes[0].set_xticks(x, labels, rotation=45)
    axes[0].set_title("Validation does not transfer to the next deployment year")
    axes[0].set_ylabel("Cumulative return")
    axes[0].legend()
    axes[0].grid(axis="y", alpha=0.25)
    axes[1].scatter(val, test, s=48)
    for row in honest:
        axes[1].annotate(
            str(row["owned_test_year"]),
            (row["val_cumulative_return"], row["deployment_cumulative_return"]),
            fontsize=8,
        )
    axes[1].axhline(0, color="black", linewidth=0.8)
    axes[1].axvline(0, color="black", linewidth=0.8)
    axes[1].set_xlabel("Validation cumulative return")
    axes[1].set_ylabel("Next-year deployment return")
    axes[1].set_title("Out-of-sample transfer scatter")
    axes[1].grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def _plot_action_geometry(path: Path, day_rows: list[dict[str, Any]]) -> None:
    gross = np.asarray([row["requested_gross"] for row in day_rows])
    active = np.asarray([row["active_gt_1e4"] for row in day_rows])
    turnover = np.asarray([row["integer_turnover"] for row in day_rows])
    hhi = np.asarray([row["hhi"] for row in day_rows])
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.2))
    axes[0, 0].hist(gross, bins=40, color="tab:red")
    axes[0, 0].axvline(0.98, color="black", linestyle="--")
    axes[0, 0].set_title("Requested gross is pinned to the L1 boundary")
    axes[0, 0].set_xlabel("Gross exposure")
    axes[0, 1].hist(turnover, bins=40, color="tab:orange")
    axes[0, 1].axvline(1.96, color="black", linestyle="--")
    axes[0, 1].set_title("Daily-flat turnover is approximately 2 × gross")
    axes[0, 1].set_xlabel("Turnover")
    axes[1, 0].hist(active, bins=40, color="tab:blue")
    axes[1, 0].set_title("Active derivative legs (>1e-4)")
    axes[1, 0].set_xlabel("Count")
    axes[1, 1].hist(1.0 / np.maximum(hhi, 1e-12), bins=40, color="tab:purple")
    axes[1, 1].set_title("Effective number of bets (1/HHI)")
    axes[1, 1].set_xlabel("Effective bets")
    for ax in axes.ravel():
        ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def _plot_option_quality(path: Path, rows: list[dict[str, Any]]) -> None:
    labels = [row["premium_bucket"] for row in rows]
    gross = np.asarray([row["mean_gross_return"] for row in rows])
    net = np.asarray([row["mean_net_return"] for row in rows])
    positive = np.asarray([row["positive_fraction"] for row in rows])
    x = np.arange(len(rows))
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 4.8))
    width = 0.38
    axes[0].bar(x - width / 2, gross, width, label="gross first-to-last trade")
    axes[0].bar(x + width / 2, net, width, label="after configured costs")
    axes[0].axhline(0, color="black", linewidth=0.8)
    axes[0].set_xticks(x, labels)
    axes[0].set_title("Option return by opening premium")
    axes[0].set_ylabel("Mean simple return")
    axes[0].legend(fontsize=8)
    axes[1].bar(x, positive, color="tab:green")
    axes[1].axhline(0.5, color="black", linestyle="--")
    axes[1].set_xticks(x, labels)
    axes[1].set_ylim(0, 1)
    axes[1].set_title("Fraction of fee-adjusted positive option rows")
    axes[1].set_ylabel("Fraction")
    for ax in axes:
        ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(path, dpi=170)
    plt.close(fig)


def _plot_training_curves(
    path: Path,
    curves: dict[int, list[dict[str, Any]]],
) -> None:
    fig, axes = plt.subplots(3, 4, figsize=(15, 10), sharex=False)
    for fold_id, ax in zip(sorted(curves), axes.ravel(), strict=True):
        curve = curves[fold_id]
        epochs = np.asarray([row["epoch"] for row in curve])
        train_loss = np.asarray([row["train_loss"] for row in curve])
        val_loss = np.asarray([row["val_mean"] for row in curve])
        ax.plot(epochs, train_loss, label="train", linewidth=1.0)
        ax.plot(epochs, val_loss, label="validation", linewidth=1.0)
        best = int(np.argmin(val_loss))
        ax.axvline(epochs[best], color="black", linestyle="--", linewidth=0.8)
        ax.set_title(f"Fold {fold_id}: best epoch {epochs[best]}")
        ax.grid(alpha=0.2)
    axes[0, 0].legend(fontsize=8)
    fig.suptitle("Loss curves: noisy validation minima and rapid selection saturation")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(path, dpi=170)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    run_root = args.run_root.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    config = load_config(args.config)
    summary = json.loads((run_root / "summary.json").read_text(encoding="utf-8"))
    if len(summary) != 12:
        raise RuntimeError(f"expected 12 completed folds, found {len(summary)}")

    before = _load_npz(run_root / "walkforward_deployment_backtest.npz")
    before_dates = np.asarray(before["dates"], dtype="datetime64[D]")
    panel, futures, candidates = _load_and_attach_market(config)
    results = [
        _load_completed_fold_result(run_root, int(item["fold_id"]))
        for item in summary
    ]
    if any(result is None for result in results):
        raise RuntimeError("at least one completed fold could not be loaded")
    if not args.skip_walkforward_repair:
        _refresh_walkforward_artifacts(
            run_root,
            list(results),  # type: ignore[arg-type]
            panel=panel,
            config=config,
        )

    stitched = _load_npz(run_root / "walkforward_deployment_backtest.npz")
    dates = np.asarray(stitched["dates"], dtype="datetime64[D]")
    actions = np.asarray(stitched["requested_weights_history"], dtype=np.float64)
    if actions.shape != (dates.size, TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4):
        raise RuntimeError(f"unexpected stitched request shape: {actions.shape}")

    selected_market = _selected_futures_market(futures, dates)
    selected_candidates = candidates.select_dates(dates)
    runtime = _build_execution_runtime(panel, config, device="cpu")  # type: ignore[arg-type]
    if runtime.futures_cost_schedule is None or runtime.option_day_cost_schedule is None:
        raise RuntimeError("derivative execution schedules were not resolved")
    future_schedule = runtime.futures_cost_schedule
    option_schedule = runtime.option_day_cost_schedule
    initial_capital = float(config.trading.tw_index_futures_initial_capital)
    maximum = float(config.trading.tw_index_derivatives_day_maximum_capital_fraction)

    scenario_specs: list[tuple[str, np.ndarray, FuturesCostSchedule, OptionDayCostSchedule]] = [
        ("actual_user_tax_0.0002", actions, future_schedule, option_schedule),
        (
            "official_option_tax_0.001",
            actions,
            future_schedule,
            OptionDayCostSchedule(
                fixed_fee_per_contract_per_side_twd=option_schedule.fixed_fee_per_contract_per_side_twd,
                transaction_tax_rate=0.001,
                slippage_points_per_side=option_schedule.slippage_points_per_side,
            ),
        ),
        (
            "zero_cost",
            actions,
            FuturesCostSchedule(
                tax_rate=0.0,
                exchange_and_clearing_fee_per_side_twd=(0.0, 0.0, 0.0),
                broker_fee_per_side_twd=(0.0, 0.0, 0.0),
                slippage_points_per_side=(0.0, 0.0, 0.0),
                basket_fee_penalty=future_schedule.basket_fee_penalty,
            ),
            OptionDayCostSchedule(
                fixed_fee_per_contract_per_side_twd=0.0,
                transaction_tax_rate=0.0,
                slippage_points_per_side=0.0,
            ),
        ),
        ("gross_scaled_50pct", actions * 0.5, future_schedule, option_schedule),
        ("gross_scaled_25pct", actions * 0.25, future_schedule, option_schedule),
        (
            "futures_only_no_renorm",
            np.concatenate((actions[:, :6], np.zeros_like(actions[:, 6:])), axis=1),
            future_schedule,
            option_schedule,
        ),
        (
            "options_only_no_renorm",
            np.concatenate((np.zeros_like(actions[:, :6]), actions[:, 6:]), axis=1),
            future_schedule,
            option_schedule,
        ),
    ]
    for k in (1, 4, 16, 64):
        scenario_specs.append(
            (f"top_{k}_keep_cash", _top_k_without_renormalization(actions, k), future_schedule, option_schedule)
        )
    prior_close = np.expm1(
        np.asarray(selected_candidates.option_candidate_features[:, :, 7], dtype=np.float64)
        * 10.0
    )
    for threshold in (5.0, 10.0, 20.0):
        filtered = actions.copy()
        filtered[:, 6:] = np.where(prior_close >= threshold, filtered[:, 6:], 0.0)
        scenario_specs.append(
            (f"causal_prior_close_ge_{int(threshold)}", filtered, future_schedule, option_schedule)
        )

    scenario_rows: list[dict[str, Any]] = []
    scenario_results: dict[str, Any] = {}
    for name, scenario_actions, scenario_futures, scenario_options in scenario_specs:
        row, result = _run_scenario(
            name,
            scenario_actions,
            selected_market,
            selected_candidates,
            initial_capital=initial_capital,
            maximum_capital_fraction=maximum,
            futures_schedule=scenario_futures,
            option_schedule=scenario_options,
        )
        scenario_rows.append(row)
        scenario_results[name] = result

    actual = scenario_results["actual_user_tax_0.0002"]
    saved_log = np.asarray(stitched["strategy_returns"], dtype=np.float64)
    recomputed_log = _safe_simple_to_log(actual.strategy_returns)
    max_log_return_difference = float(np.max(np.abs(saved_log - recomputed_log)))
    if max_log_return_difference > 2e-5:
        raise RuntimeError(
            "canonical stitched artifact does not match direct integer replay: "
            f"max_abs_diff={max_log_return_difference}"
        )

    action_summary, action_days = _action_diagnostics(
        actions, selected_candidates, actual
    )
    model_parameters, fold1_checkpoint_epoch = _model_parameter_count(run_root)
    training_summary, training_rows, curves = _training_diagnostics(
        run_root,
        summary,
        panel_dates=panel.dates,
        model_parameters=model_parameters,
    )
    fold_summary, fold_rows = _fold_performance(run_root, summary)
    version_rows = _version_comparison(run_root)
    source_quality, premium_rows = _source_option_quality(
        Path(config.trading.tw_index_options_monthly_data_path),
        Path(config.trading.tw_index_options_weekly_data_path),
        fee=float(option_schedule.fixed_fee_per_contract_per_side_twd),
        tax=float(option_schedule.transaction_tax_rate),
        slippage=float(option_schedule.slippage_points_per_side),
    )

    root_repair = {
        "before_rows": int(before_dates.size),
        "before_min_date": str(before_dates.min()) if before_dates.size else None,
        "before_max_date": str(before_dates.max()) if before_dates.size else None,
        "after_rows": int(dates.size),
        "after_min_date": str(dates.min()),
        "after_max_date": str(dates.max()),
        "direct_integer_replay_max_abs_log_return_diff": max_log_return_difference,
        "cause": (
            "isolated fold children overwrote root artifacts; parent now rebuilds "
            "one stitched account after every fold completes"
        ),
    }
    repair_observation_path = output_dir / "walkforward_repair_observation.json"
    if repair_observation_path.exists():
        root_repair["observed_initial_bug_state"] = json.loads(
            repair_observation_path.read_text(encoding="utf-8")
        )
    benchmark_metrics = _return_metrics_from_log(stitched["benchmark_returns"])
    tx_intraday = np.where(
        selected_market.reference_tradable_mask("TX"),
        selected_market.reference_log_returns("TX"),
        0.0,
    )
    tx_intraday_metrics = _return_metrics_from_log(tx_intraday)
    actual_metrics = next(
        row for row in scenario_rows if row["scenario"] == "actual_user_tax_0.0002"
    )
    worst_indices = np.argsort(actual.strategy_returns)[:10]
    worst_days = [
        {
            "date": str(dates[index]),
            "simple_return": float(actual.strategy_returns[index]),
            "gross_pnl_twd": float(actual.gross_pnl_twd[index]),
            "fees_twd": float(actual.fees_twd[index]),
            "tax_twd": float(actual.tax_twd[index]),
            "slippage_twd": float(actual.slippage_twd[index]),
            "turnover": float(actual.turnovers[index]),
            "option_contracts": int(actual.option_contract_quantities[index].sum()),
            "futures_contracts": int(
                np.abs(actual.futures_contract_quantities[index]).sum()
            ),
        }
        for index in worst_indices
    ]

    findings = [
        {
            "rank": 1,
            "status": "verified",
            "driver": "L1 boundary forces near-full daily deployment and cost",
            "evidence": (
                f"gross>=0.979 on {action_summary['boundary_fraction_gross_ge_0_979']:.1%} "
                f"of days; mean turnover={action_summary['mean_gross'] * 2:.3f} before integer rounding"
            ),
        },
        {
            "rank": 2,
            "status": "verified",
            "driver": "validation model selection does not generalize",
            "evidence": (
                f"Pearson r={fold_summary['val_to_next_year_pearson_r']:.3f}; "
                f"positive years={fold_summary['positive_owned_years']}/{fold_summary['honest_owned_years']}"
            ),
        },
        {
            "rank": 3,
            "status": "verified",
            "driver": "rare near-full option bets create the catastrophic left tail",
            "evidence": (
                f"options were selected on only {action_summary['days_with_any_option_exposure']} days, "
                f"but compounded {action_summary['conditional_compounded_return_on_option_days']:.1%} on those days"
            ),
        },
        {
            "rank": 4,
            "status": "verified",
            "driver": "daily option prices are first/last trades, not executable synchronized quotes",
            "evidence": (
                f"source contract={source_quality['rows']} rows; terminal quote fields cannot establish "
                "an executable opening ask and closing bid"
            ),
        },
        {
            "rank": 5,
            "status": "likely",
            "driver": "supervision is statistically weak relative to model capacity",
            "evidence": (
                f"{model_parameters:,} parameters; fold-1 has "
                f"{training_rows[0]['train_target_days']} target days, while derivative legs "
                "share a small set of index/volatility shocks"
            ),
        },
        {
            "rank": 6,
            "status": "verified",
            "driver": "reported root walk-forward was incomplete",
            "evidence": (
                "the observed pre-repair root held only 140 rows from 2026; "
                f"the repaired account contains {root_repair['after_rows']} rows"
            ),
        },
        {
            "rank": 7,
            "status": "verified",
            "driver": "option tax assumption conflicts with current TAIFEX public schedule",
            "evidence": "configured 0.0002 on closing sale; TAIFEX publishes 0.001 per premium transaction",
        },
        {
            "rank": 8,
            "status": "likely",
            "driver": "stock close t-1 cannot observe overnight information before derivative open t",
            "evidence": "the information clock intentionally excludes current futures/option open, IV surface, and synchronized bid/ask",
        },
    ]

    results_payload = {
        "generated_at_utc": datetime.now(timezone.utc),
        "config": str(args.config.resolve()),
        "run_root": str(run_root),
        "output_dir": str(output_dir),
        "root_repair": root_repair,
        "canonical_actual_metrics": actual_metrics,
        "benchmark_metrics": benchmark_metrics,
        "tx_intraday_long_metrics": tx_intraday_metrics,
        "action_geometry": action_summary,
        "fold_generalization": fold_summary,
        "training_capacity": training_summary,
        "source_option_quality": source_quality,
        "fold1_best_checkpoint_epoch": fold1_checkpoint_epoch,
        "worst_days": worst_days,
        "findings": findings,
    }
    _write_json(output_dir / "analysis_results.json", results_payload)
    _write_csv(output_dir / "scenario_counterfactuals.csv", scenario_rows)
    _write_csv(output_dir / "fold_generalization.csv", fold_rows)
    _write_csv(output_dir / "training_curve_summary.csv", training_rows)
    _write_csv(output_dir / "option_quality_by_premium.csv", premium_rows)
    _write_csv(output_dir / "model_version_comparison.csv", version_rows)
    _write_csv(output_dir / "daily_action_diagnostics.csv", action_days)
    _write_csv(output_dir / "worst_days.csv", worst_days)

    _plot_equity_scenarios(
        output_dir / "equity_scenarios.png",
        dates,
        stitched["benchmark_returns"],
        scenario_results,
    )
    _plot_fold_generalization(
        output_dir / "fold_generalization.png",
        fold_rows,
    )
    _plot_action_geometry(
        output_dir / "action_geometry.png",
        action_days,
    )
    _plot_option_quality(
        output_dir / "option_quality_by_premium.png",
        premium_rows,
    )
    _plot_training_curves(
        output_dir / "training_curves.png",
        curves,
    )
    print(json.dumps(_jsonable(results_payload), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
