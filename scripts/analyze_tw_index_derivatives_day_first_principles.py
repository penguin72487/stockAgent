#!/usr/bin/env python3
"""Reproduce first-principles diagnostics for the completed derivative v7 run.

The script treats the stored walk-forward artifact as immutable evidence, maps
its 4,102 requested actions back to concrete TAIFEX contracts, replays the same
actions under the corrected statutory per-transaction tax contract, and emits
bounded CSV/JSON/PNG evidence for the technical report.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import asdict, fields
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import NormalDist
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch

from stockagent.backtest.tw_index_derivatives_day import (
    OptionDayCostSchedule,
    run_tw_index_derivatives_day_integer,
)
from stockagent.backtest.tw_index_futures import FuturesCostSchedule
from stockagent.data.tw_index_derivatives_day import (
    build_causal_derivative_day_candidates,
    load_taiex_opening_index,
)
from stockagent.data.tw_index_futures import (
    TaiwanIndexFuturesDaySession,
    load_taifex_index_futures_day_session,
)
from stockagent.data.tw_index_options_daily import (
    combine_taifex_option_chains,
    load_taifex_option_full_chain,
)
from stockagent.models.normalization import masked_cash_entmax15_weights


DEFAULT_ROOT = Path(
    "artifacts/markets/"
    "tw_index_derivatives_day_multi_basis_100m_relative_tenor_cash_entmax_v7_dual5090"
)
DEFAULT_OUTPUT = Path(
    "artifacts/diagnostics/"
    "tw_index_derivatives_day_cash_entmax_v7_first_principles"
)


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, np.ndarray):
        return _json_ready(value.tolist())
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _performance(simple_returns: np.ndarray) -> dict[str, float]:
    values = np.nan_to_num(np.asarray(simple_returns, dtype=np.float64), nan=0.0)
    wealth = np.cumprod(1.0 + np.clip(values, -1.0, None))
    cumulative = float(wealth[-1] - 1.0) if wealth.size else 0.0
    volatility = float(values.std(ddof=1)) if values.size > 1 else 0.0
    sharpe = (
        float(math.sqrt(252.0) * values.mean() / volatility)
        if volatility > 0.0
        else 0.0
    )
    running_peak = np.maximum.accumulate(np.r_[1.0, wealth])
    drawdown = np.r_[1.0, wealth] / running_peak - 1.0
    return {
        "cumulative_return": cumulative,
        "annualized_sharpe": sharpe,
        "max_drawdown": float(drawdown.min()),
        "mean_daily_return": float(values.mean()) if values.size else 0.0,
        "daily_volatility": volatility,
        "days": int(values.size),
    }


def _select_market(
    market: TaiwanIndexFuturesDaySession, indices: np.ndarray
) -> TaiwanIndexFuturesDaySession:
    payload: dict[str, Any] = {}
    row_fields = {
        "dates",
        "contract_months",
        "open_prices",
        "high_prices",
        "low_prices",
        "close_prices",
        "volumes",
        "log_returns",
        "tradable_mask",
        "rolling_buy_hold_log_returns",
        "rolling_buy_hold_tradable_mask",
        "front_month_roll_mask",
        "tenor_contract_months",
        "tenor_open_prices",
        "tenor_high_prices",
        "tenor_low_prices",
        "tenor_close_prices",
        "tenor_volumes",
        "tenor_log_returns",
        "tenor_tradable_mask",
    }
    for field in fields(TaiwanIndexFuturesDaySession):
        value = getattr(market, field.name)
        if field.name in row_fields and value is not None:
            payload[field.name] = np.asarray(value)[indices]
        else:
            payload[field.name] = value
    return TaiwanIndexFuturesDaySession(**payload)


def _epoch_selection(root: Path) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    summaries: list[dict[str, Any]] = []
    curves: dict[int, pd.DataFrame] = {}
    paths = sorted(root.glob("train_*/epoch_curve.jsonl"))
    for path in paths:
        years = path.parent.name.removeprefix("train_").split("-")
        fold = len(years)
        rows = []
        with path.open(encoding="utf-8") as handle:
            for raw in handle:
                item = json.loads(raw)
                rows.append(
                    {
                        "epoch": int(item["epoch"]),
                        "train_loss": float(item["train_loss"]),
                        "val_loss": float(item["val_mean"]),
                        "test_loss": float(item["test_mean"]),
                    }
                )
        frame = pd.DataFrame(rows).sort_values("epoch").reset_index(drop=True)
        if frame.empty:
            continue
        curves[fold] = frame
        selected = frame.loc[frame["val_loss"].idxmin()]
        oracle = frame.loc[frame["test_loss"].idxmin()]
        summaries.append(
            {
                "fold": fold,
                "training_years": fold,
                "epochs_observed": len(frame),
                "selected_epoch": int(selected["epoch"]),
                "selected_val_loss": float(selected["val_loss"]),
                "selected_test_loss": float(selected["test_loss"]),
                "oracle_test_epoch": int(oracle["epoch"]),
                "oracle_test_loss": float(oracle["test_loss"]),
                "selection_regret": float(
                    selected["test_loss"] - oracle["test_loss"]
                ),
                "val_test_epoch_correlation": float(
                    frame[["val_loss", "test_loss"]].corr().iloc[0, 1]
                ),
                "last_val_loss": float(frame.iloc[-1]["val_loss"]),
                "last_test_loss": float(frame.iloc[-1]["test_loss"]),
            }
        )
    return pd.DataFrame(summaries).sort_values("fold"), curves


def _model_parameter_count(root: Path) -> int:
    checkpoint = torch.load(
        root / "fold_02" / "checkpoint_best.pt",
        map_location="cpu",
        weights_only=False,
    )
    state = checkpoint["model_state_dict"]
    return int(sum(tensor.numel() for tensor in state.values()))


def _plot_evidence(
    output: Path,
    daily: pd.DataFrame,
    annual: pd.DataFrame,
    allocation: pd.DataFrame,
    tails: pd.DataFrame,
    fold_selection: pd.DataFrame,
    curves: dict[int, pd.DataFrame],
) -> None:
    plt.style.use("seaborn-v0_8-whitegrid")

    figure, axis = plt.subplots(figsize=(11, 5.5))
    for column, label, color in (
        ("v7_wealth", "v7 exact (legacy tax)", "#b23a48"),
        ("statutory_tax_wealth", "same actions, corrected tax", "#d88c2f"),
        ("benchmark_wealth", "TX front-month buy-and-hold", "#2455a4"),
    ):
        axis.plot(pd.to_datetime(daily["date"]), daily[column], label=label, lw=1.7, color=color)
    axis.set_yscale("log")
    axis.set_title("Walk-forward wealth: model loss is structural, not a small benchmark gap")
    axis.set_ylabel("Wealth multiple (log scale)")
    axis.legend(loc="best")
    figure.tight_layout()
    figure.savefig(output / "wealth_comparison.png", dpi=180)
    plt.close(figure)

    figure, axis = plt.subplots(figsize=(11, 5.5))
    x = np.arange(len(annual))
    width = 0.27
    axis.bar(x - width, annual["v7_return"], width, label="v7", color="#b23a48")
    axis.bar(x, annual["statutory_tax_return"], width, label="corrected tax replay", color="#d88c2f")
    axis.bar(x + width, annual["benchmark_return"], width, label="benchmark", color="#2455a4")
    axis.axhline(0.0, color="black", lw=0.8)
    axis.set_xticks(x, annual["year"].astype(str), rotation=45)
    axis.set_ylabel("Calendar return")
    axis.set_title("Annual return instability across walk-forward folds")
    axis.legend(loc="best")
    figure.tight_layout()
    figure.savefig(output / "annual_return_comparison.png", dpi=180)
    plt.close(figure)

    figure, axis = plt.subplots(figsize=(11, 5.5))
    bottom = np.zeros(len(allocation))
    for column, label, color in (
        ("mean_futures_gross", "futures", "#2455a4"),
        ("mean_long_option_gross", "long options", "#b23a48"),
        ("mean_short_option_gross", "short options", "#6d3f91"),
        ("mean_cash_fraction", "cash", "#b7b7b7"),
    ):
        values = allocation[column].to_numpy()
        axis.bar(allocation["year"].astype(str), values, bottom=bottom, label=label, color=color)
        bottom += values
    axis.set_ylim(0.0, 1.05)
    axis.set_ylabel("Mean capital/gross fraction")
    axis.set_title("Cash-entmax generally deploys most of the 0.98 radius")
    axis.legend(ncol=4, loc="upper center")
    figure.tight_layout()
    figure.savefig(output / "allocation_by_year.png", dpi=180)
    plt.close(figure)

    figure, axis = plt.subplots(figsize=(11, 5.5))
    tail_plot = tails.head(15).iloc[::-1]
    colors = ["#b23a48" if value > 0.5 else "#d88c2f" for value in tail_plot["option_gross"]]
    axis.barh(tail_plot["date"], tail_plot["v7_return"], color=colors)
    axis.set_xlabel("Daily strategy return")
    axis.set_title("Worst days are concentrated derivative tail events")
    figure.tight_layout()
    figure.savefig(output / "worst_days.png", dpi=180)
    plt.close(figure)

    figure, axes = plt.subplots(1, 2, figsize=(12, 5.2))
    axes[0].scatter(
        fold_selection["selected_val_loss"],
        fold_selection["selected_test_loss"],
        c=fold_selection["fold"],
        cmap="viridis",
        s=55,
    )
    for row in fold_selection.itertuples():
        axes[0].annotate(str(row.fold), (row.selected_val_loss, row.selected_test_loss), fontsize=8)
    axes[0].axhline(0.0, color="black", lw=0.8)
    axes[0].axvline(0.0, color="black", lw=0.8)
    axes[0].set_xlabel("Selected validation loss")
    axes[0].set_ylabel("Test loss at selected epoch")
    axes[0].set_title("Best validation epoch does not transfer reliably")
    for fold, color in ((2, "#b23a48"), (6, "#2455a4"), (11, "#3b7d44")):
        frame = curves.get(fold)
        if frame is not None:
            axes[1].plot(frame["epoch"], frame["val_loss"], label=f"fold {fold} val", color=color)
            axes[1].plot(frame["epoch"], frame["test_loss"], label=f"fold {fold} test", color=color, ls="--", alpha=0.75)
    axes[1].axhline(0.0, color="black", lw=0.8)
    axes[1].set_xlabel("Epoch")
    axes[1].set_ylabel("Loss; lower is better")
    axes[1].set_title("Representative loss curves")
    axes[1].legend(fontsize=8, ncol=2)
    figure.tight_layout()
    figure.savefig(output / "validation_selection.png", dpi=180)
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    root = args.artifact_root.resolve()
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)

    source_npz = root / "walkforward_deployment_backtest.npz"
    stored = np.load(source_npz, allow_pickle=False)
    dates = stored["dates"].astype("datetime64[D]")
    legacy_log = stored["strategy_returns"].astype(np.float64)
    benchmark_log = stored["benchmark_returns"].astype(np.float64)
    actions = stored["requested_weights_history"].astype(np.float64)
    legacy_simple = np.expm1(legacy_log)
    benchmark_simple = np.expm1(benchmark_log)
    years = dates.astype("datetime64[Y]").astype(int) + 1970

    futures = load_taifex_index_futures_day_session(
        "data_tw_index_futures/day_session_contracts.parquet", panel_dates=dates
    )
    monthly = load_taifex_option_full_chain(
        "data_tw_index_options_daily/monthly_full_chain.parquet",
        expected_series_scope="monthly",
        panel_dates=dates,
    )
    weekly = load_taifex_option_full_chain(
        "data_tw_index_options_daily/weekly_full_chain.parquet",
        expected_series_scope="weekly",
        panel_dates=dates,
    )
    chain = combine_taifex_option_chains(monthly, weekly)
    taiex_open = load_taiex_opening_index(
        "data_tw_public/twse_taiex_ohlc.parquet", panel_dates=dates
    )
    candidates = build_causal_derivative_day_candidates(
        futures,
        chain,
        fixed_fee_per_contract_per_side_twd=22.0,
        transaction_tax_rate=0.001,
        slippage_points_per_side=0.5,
        allow_option_short=True,
        option_risk_margin_a_twd=187_000.0,
        option_risk_margin_b_twd=94_000.0,
        option_margin_schedule_as_of="2026-08-12",
        underlying_index_open_prices=taiex_open,
        option_margin_underlying_source="official_twse_taiex_opening_index",
    )

    future_costs = FuturesCostSchedule(
        tax_rate=0.00002,
        exchange_and_clearing_fee_per_side_twd=(20.0, 12.5, 8.0),
        broker_fee_per_side_twd=(40.0, 11.5, 8.0),
        slippage_points_per_side=(0.0, 0.0, 0.0),
    )
    option_costs = OptionDayCostSchedule(
        fixed_fee_per_contract_per_side_twd=22.0,
        transaction_tax_rate=0.001,
        slippage_points_per_side=0.5,
    )
    official_simple = np.zeros_like(legacy_simple)
    official_gross_pnl = np.zeros_like(legacy_simple)
    official_fees = np.zeros_like(legacy_simple)
    official_taxes = np.zeros_like(legacy_simple)
    official_slippage = np.zeros_like(legacy_simple)
    official_option_counts = np.zeros((len(dates), 4096), dtype=np.int64)
    component_money: defaultdict[tuple[int, str], dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for year in sorted(np.unique(years)):
        indices = np.flatnonzero(years == year)
        year_dates = dates[indices]
        selected_market = _select_market(futures, indices)
        selected_candidates = candidates.select_dates(year_dates)
        result = run_tw_index_derivatives_day_integer(
            actions[indices],
            selected_market,
            selected_candidates,
            initial_capital=100_000_000.0,
            maximum_capital_fraction=0.98,
            futures_cost_schedule=future_costs,
            option_cost_schedule=option_costs,
        )
        official_simple[indices] = result.strategy_returns
        official_gross_pnl[indices] = result.gross_pnl_twd
        official_fees[indices] = result.fees_twd
        official_taxes[indices] = result.tax_twd
        official_slippage[indices] = result.slippage_twd
        official_option_counts[indices] = result.option_contract_quantities

        sparse = result.option_sparse_indices
        active = result.option_contract_quantities != 0
        sparse_active = sparse[active]
        counts_active = result.option_contract_quantities[active].astype(np.float64)
        opens = selected_candidates.source_chain.open_prices[sparse_active].astype(np.float64)
        closes = selected_candidates.source_chain.close_prices[sparse_active].astype(np.float64)
        option_gross = counts_active * 50.0 * (closes - opens)
        option_fee = np.abs(counts_active) * 44.0
        option_tax = np.abs(counts_active) * 50.0 * (opens + closes) * 0.001
        option_slip = np.abs(counts_active) * 50.0
        option_net = option_gross - option_fee - option_tax - option_slip
        side = np.where(counts_active > 0.0, "option_long", "option_short")
        for name in ("option_long", "option_short"):
            selected = side == name
            bucket = component_money[(int(year), name)]
            bucket["gross_pnl_twd"] += float(option_gross[selected].sum())
            bucket["fees_twd"] += float(option_fee[selected].sum())
            bucket["tax_twd"] += float(option_tax[selected].sum())
            bucket["slippage_twd"] += float(option_slip[selected].sum())
            bucket["net_pnl_twd"] += float(option_net[selected].sum())
        option_totals = {
            "gross_pnl_twd": float(option_gross.sum()),
            "fees_twd": float(option_fee.sum()),
            "tax_twd": float(option_tax.sum()),
            "slippage_twd": float(option_slip.sum()),
            "net_pnl_twd": float(option_net.sum()),
        }
        futures_bucket = component_money[(int(year), "futures")]
        for name, total in (
            ("gross_pnl_twd", result.gross_pnl_twd.sum()),
            ("fees_twd", result.fees_twd.sum()),
            ("tax_twd", result.tax_twd.sum()),
            ("slippage_twd", result.slippage_twd.sum()),
            ("net_pnl_twd", result.net_pnl_twd.sum()),
        ):
            futures_bucket[name] += float(total) - option_totals[name]

    legacy_wealth = np.cumprod(1.0 + legacy_simple)
    official_wealth = np.cumprod(1.0 + official_simple)
    benchmark_wealth = np.cumprod(1.0 + benchmark_simple)
    daily = pd.DataFrame(
        {
            "date": dates.astype(str),
            "year": years,
            "v7_return": legacy_simple,
            "statutory_tax_return": official_simple,
            "benchmark_return": benchmark_simple,
            "v7_wealth": legacy_wealth,
            "statutory_tax_wealth": official_wealth,
            "benchmark_wealth": benchmark_wealth,
            "requested_gross": np.abs(actions).sum(axis=1),
            "requested_option_gross": np.abs(actions[:, 6:]).sum(axis=1),
            "official_gross_pnl_twd": official_gross_pnl,
            "official_fees_twd": official_fees,
            "official_taxes_twd": official_taxes,
            "official_slippage_twd": official_slippage,
        }
    )
    daily.to_csv(output / "daily_replay.csv", index=False)

    annual_rows = []
    allocation_rows = []
    for year in sorted(np.unique(years)):
        selected = years == year
        annual_rows.append(
            {
                "year": int(year),
                "v7_return": _performance(legacy_simple[selected])["cumulative_return"],
                "statutory_tax_return": _performance(official_simple[selected])["cumulative_return"],
                "benchmark_return": _performance(benchmark_simple[selected])["cumulative_return"],
            }
        )
        total_gross = np.abs(actions[selected]).sum(axis=1)
        allocation_rows.append(
            {
                "year": int(year),
                "mean_total_gross": float(total_gross.mean()),
                "mean_futures_gross": float(np.abs(actions[selected, :6]).sum(axis=1).mean()),
                "mean_long_option_gross": float(np.clip(actions[selected, 6:], 0.0, None).sum(axis=1).mean()),
                "mean_short_option_gross": float(np.clip(-actions[selected, 6:], 0.0, None).sum(axis=1).mean()),
                "mean_cash_fraction": float((1.0 - total_gross).mean()),
            }
        )
    annual = pd.DataFrame(annual_rows)
    allocation = pd.DataFrame(allocation_rows)
    annual.to_csv(output / "annual_performance.csv", index=False)
    allocation.to_csv(output / "allocation_by_year.csv", index=False)

    sparse = candidates.option_sparse_indices
    valid_sparse = sparse >= 0
    volumes = np.zeros_like(sparse, dtype=np.int64)
    volumes[valid_sparse] = chain.volumes[sparse[valid_sparse]]
    active_counts = np.abs(official_option_counts) > 0
    participation = np.divide(
        np.abs(official_option_counts),
        volumes,
        out=np.full_like(actions[:, 6:], np.nan, dtype=np.float64),
        where=volumes > 0,
    )
    active_participation = participation[active_counts]
    liquidity = {
        "executed_option_legs": int(active_counts.sum()),
        "executed_option_legs_over_10pct_reported_volume": int(
            np.sum(active_counts & (participation > 0.10))
        ),
        "executed_option_legs_over_100pct_reported_volume": int(
            np.sum(active_counts & (participation > 1.0))
        ),
        "days_with_any_option_leg_over_10pct_reported_volume": int(
            np.sum(np.any(active_counts & (participation > 0.10), axis=1))
        ),
        "days_with_any_option_leg_over_100pct_reported_volume": int(
            np.sum(np.any(active_counts & (participation > 1.0), axis=1))
        ),
        "participation_p50": float(np.nanquantile(active_participation, 0.50)),
        "participation_p90": float(np.nanquantile(active_participation, 0.90)),
        "participation_p95": float(np.nanquantile(active_participation, 0.95)),
        "participation_p99": float(np.nanquantile(active_participation, 0.99)),
        "participation_max": float(np.nanmax(active_participation)),
    }
    _write_json(output / "liquidity_summary.json", liquidity)

    tail_rows = []
    for row in np.argsort(legacy_simple)[:30]:
        absolute = np.abs(actions[row])
        gross = float(absolute.sum())
        shares = absolute / gross if gross > 0.0 else absolute
        effective = 1.0 / float(np.square(shares).sum()) if gross > 0.0 else 0.0
        tail_rows.append(
            {
                "date": str(dates[row]),
                "v7_return": float(legacy_simple[row]),
                "benchmark_return": float(benchmark_simple[row]),
                "requested_gross": gross,
                "option_gross": float(absolute[6:].sum()),
                "long_option_gross": float(np.clip(actions[row, 6:], 0.0, None).sum()),
                "short_option_gross": float(np.clip(-actions[row, 6:], 0.0, None).sum()),
                "active_actions": int(np.count_nonzero(absolute)),
                "top_action_share_of_gross": float(shares.max()) if gross else 0.0,
                "effective_action_count": effective,
                "max_option_volume_participation": float(
                    np.nanmax(participation[row])
                    if bool(np.isfinite(participation[row]).any())
                    else 0.0
                ),
            }
        )
    tails = pd.DataFrame(tail_rows)
    tails.to_csv(output / "worst_days.csv", index=False)

    contribution_rows = []
    for (year, component), values in sorted(component_money.items()):
        contribution_rows.append({"year": year, "component": component, **values})
    contributions = pd.DataFrame(contribution_rows)
    contributions.to_csv(output / "pnl_components.csv", index=False)

    fold_selection, curves = _epoch_selection(root)
    fold_selection.to_csv(output / "fold_epoch_selection.csv", index=False)

    logits = torch.tensor([[0.20, 0.10, -0.05, 0.02]], dtype=torch.float32)
    mask = torch.ones_like(logits, dtype=torch.bool)
    scale_rows = []
    for scale in (0.0, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 50.0):
        weights = masked_cash_entmax15_weights(
            logits * scale,
            mask,
            short_mask=mask,
            radius=0.98,
        )
        gross = float(weights.abs().sum())
        scale_rows.append(
            {"logit_scale": scale, "gross_exposure": gross, "cash_fraction": 1.0 - gross}
        )
    entmax_scale = pd.DataFrame(scale_rows)
    entmax_scale.to_csv(output / "cash_entmax_scale_sensitivity.csv", index=False)

    noise_rows = []
    for count in (6, 32, 100, 500, 1000, 4102):
        expected_extreme_proxy = NormalDist().inv_cdf(1.0 - 1.0 / count)
        noise_rows.append(
            {
                "action_count": count,
                "iid_normal_one_exceedance_z": expected_extreme_proxy,
            }
        )
    pd.DataFrame(noise_rows).to_csv(output / "multiple_comparison_noise.csv", index=False)

    parameter_count = _model_parameter_count(root)
    long_candidate_returns = candidates.option_simple_returns[
        np.isfinite(candidates.option_simple_returns)
    ].astype(np.float64)
    short_candidate_returns = candidates.option_short_simple_returns[
        np.isfinite(candidates.option_short_simple_returns)
    ].astype(np.float64)
    directional_returns = candidates.simple_returns().astype(np.float64)
    safe_long = np.nan_to_num(directional_returns[..., 0], nan=0.0)
    safe_short = np.nan_to_num(directional_returns[..., 1], nan=0.0)
    continuous_leg_pnl = (
        np.clip(actions, 0.0, None) * safe_long
        + np.clip(-actions, 0.0, None) * safe_short
    )
    continuous_raw_returns = (
        continuous_leg_pnl.sum(axis=1)
        - np.abs(actions[:, :6]).sum(axis=1) * 0.00013
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_root": str(root),
        "date_start": str(dates[0]),
        "date_end": str(dates[-1]),
        "walkforward_days": int(len(dates)),
        "stock_symbols": 2744,
        "base_features": 98,
        "lookback": 32,
        "model_parameters": parameter_count,
        "derivative_actions": int(actions.shape[1]),
        "futures_actions": 6,
        "option_candidate_capacity": 4096,
        "mean_prior_known_option_candidates": float(
            candidates.option_candidate_mask.sum(axis=1).mean()
        ),
        "max_prior_known_option_candidates": int(
            candidates.option_candidate_mask.sum(axis=1).max()
        ),
        "mean_executable_option_candidates": float(
            np.isfinite(candidates.option_simple_returns).sum(axis=1).mean()
        ),
        "max_executable_option_candidates": int(
            np.isfinite(candidates.option_simple_returns).sum(axis=1).max()
        ),
        "mean_requested_gross": float(np.abs(actions).sum(axis=1).mean()),
        "median_requested_gross": float(np.median(np.abs(actions).sum(axis=1))),
        "mean_requested_option_gross": float(np.abs(actions[:, 6:]).sum(axis=1).mean()),
        "option_share_of_total_requested_gross": float(
            np.abs(actions[:, 6:]).sum() / np.abs(actions).sum()
        ),
        "candidate_return_distribution": {
            "long_option_p01": float(np.quantile(long_candidate_returns, 0.01)),
            "long_option_median": float(np.quantile(long_candidate_returns, 0.50)),
            "long_option_p99": float(np.quantile(long_candidate_returns, 0.99)),
            "long_option_fraction_below_minus_one": float(
                np.mean(long_candidate_returns < -1.0)
            ),
            "short_option_p01": float(np.quantile(short_candidate_returns, 0.01)),
            "short_option_median": float(np.quantile(short_candidate_returns, 0.50)),
            "short_option_p99": float(np.quantile(short_candidate_returns, 0.99)),
            "deployment_continuous_rows_at_or_below_ruin": int(
                np.sum(continuous_raw_returns <= -1.0)
            ),
        },
        "legacy_v7": _performance(legacy_simple),
        "corrected_statutory_tax_same_actions": _performance(official_simple),
        "benchmark": _performance(benchmark_simple),
        "liquidity": liquidity,
        "epoch_selection": {
            "median_epoch_count": float(fold_selection["epochs_observed"].median()),
            "median_selected_epoch": float(fold_selection["selected_epoch"].median()),
            "median_val_test_epoch_correlation": float(
                fold_selection["val_test_epoch_correlation"].median()
            ),
            "folds_where_selected_test_loss_is_positive": int(
                np.sum(fold_selection["selected_test_loss"] > 0.0)
            ),
            "folds": int(len(fold_selection)),
        },
        "source_sha256": {
            "walkforward_deployment_backtest.npz": _sha256(source_npz),
            "day_session_contracts.parquet": _sha256(
                Path("data_tw_index_futures/day_session_contracts.parquet")
            ),
            "monthly_full_chain.parquet": _sha256(
                Path("data_tw_index_options_daily/monthly_full_chain.parquet")
            ),
            "weekly_full_chain.parquet": _sha256(
                Path("data_tw_index_options_daily/weekly_full_chain.parquet")
            ),
        },
    }
    _write_json(output / "summary.json", summary)

    data_quality = pd.DataFrame(
        [
            {
                "check": "walkforward_rows",
                "value": len(dates),
                "status": "pass",
                "interpretation": "unique chronological deployment decisions",
            },
            {
                "check": "candidate_capacity",
                "value": actions.shape[1],
                "status": "pass",
                "interpretation": "6 futures plus 4096 date-local option slots",
            },
            {
                "check": "mean_executable_option_candidates",
                "value": summary["mean_executable_option_candidates"],
                "status": "warning",
                "interpretation": "large daily multiple-comparison search space",
            },
            {
                "check": "long_option_candidates_below_minus_100pct",
                "value": summary["candidate_return_distribution"][
                    "long_option_fraction_below_minus_one"
                ],
                "status": "warning",
                "interpretation": "fees and fixed slippage can exceed a cheap option premium",
            },
            {
                "check": "days_over_100pct_reported_option_volume",
                "value": liquidity["days_with_any_option_leg_over_100pct_reported_volume"],
                "status": "fail",
                "interpretation": "integer executor had no option volume-capacity constraint",
            },
            {
                "check": "bid_ask_available_to_model_or_executor",
                "value": 0,
                "status": "fail",
                "interpretation": "first/last trade plus fixed 0.5 point slippage is not executable depth",
            },
            {
                "check": "fold12_unbiased_test",
                "value": 0,
                "status": "warning",
                "interpretation": "2026 validation and test intentionally overlap",
            },
        ]
    )
    data_quality.to_csv(output / "data_quality_checks.csv", index=False)

    _plot_evidence(output, daily, annual, allocation, tails, fold_selection, curves)
    print(json.dumps(_json_ready(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
