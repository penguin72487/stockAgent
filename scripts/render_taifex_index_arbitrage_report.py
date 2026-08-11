#!/usr/bin/env python3
"""Build the canonical portable-report artifact for TAIFEX arbitrage research."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Final

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_derivatives_tick import _atomic_json  # noqa: E402


DEFAULT_INPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_index_arbitrage_all_expiries"
)
LABELS: Final[dict[str, str]] = {
    "futures_tx_vs_4_mtx": "TX vs 4 MTX",
    "futures_tx_vs_20_tmf": "TX vs 20 TMF",
    "futures_4_mtx_vs_20_tmf": "4 MTX vs 20 TMF",
    "monthly_put_call_parity_mtx": "月選 Put–Call parity + MTX",
    "call_vertical_bounds": "Call vertical bounds",
    "put_vertical_bounds": "Put vertical bounds",
    "call_butterfly_bounds": "Call butterfly bounds",
    "put_butterfly_bounds": "Put butterfly bounds",
    "box_spread": "Box spread",
    "cross_expiry_calendar_box": "跨到期 Calendar box",
}


def _finite(value: Any) -> Any:
    if value is None:
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


def _pct(value: float) -> str:
    return f"{value * 100.0:+.3f}%"


def _twd(value: float) -> str:
    return f"{value:,.0f} 元"


def build_artifact(input_dir: Path) -> dict[str, Any]:
    metrics = pl.read_parquet(input_dir / "metrics.parquet")
    daily = pl.read_parquet(input_dir / "daily_returns.parquet")
    cycles = pl.read_parquet(input_dir / "cycles.parquet")
    opportunities = pl.read_parquet(input_dir / "opportunity_summary.parquet")
    summary = json.loads((input_dir / "summary.json").read_text(encoding="utf-8"))
    quality = json.loads((input_dir / "data_quality.json").read_text(encoding="utf-8"))
    generated_at = datetime.now(timezone.utc).isoformat()
    source_prefix = input_dir.as_posix()

    metric_rows: list[dict[str, Any]] = []
    for rank, row in enumerate(metrics.iter_rows(named=True), start=1):
        metric_rows.append(
            {
                "rank": rank,
                "variant_id": row["variant_id"],
                "strategy": LABELS[row["variant_id"]],
                "family": row["family"],
                "trading_days": row["trading_days"],
                "completed_cycles": row["completed_cycles"],
                "winning_cycles": row["winning_cycles"],
                "win_rate": (
                    row["winning_cycles"] / row["completed_cycles"]
                    if row["completed_cycles"]
                    else None
                ),
                "gross_pnl_twd": row["total_gross_pnl_twd"],
                "fees_twd": row["total_fixed_fees_twd"],
                "entry_taxes_twd": row["total_entry_taxes_twd"],
                "exit_taxes_twd": row["total_exit_taxes_twd"],
                "settlement_taxes_twd": row["total_settlement_taxes_twd"],
                "convergence_exits": row["convergence_exits"],
                "expiry_settlements": row["expiry_settlements"],
                "multi_expiry_settlements": row["multi_expiry_settlements"],
                "net_pnl_twd": row["total_net_pnl_twd"],
                "simple_return": row["simple_return_on_common_capital"],
                "compounded_return": row["cumulative_compounded_return"],
                "annualized_sharpe": _finite(row["annualized_sharpe"]),
                "annualized_sortino": _finite(row["annualized_sortino"]),
                "maximum_drawdown": row["maximum_drawdown"],
                "annualized_calmar": _finite(row["annualized_calmar"]),
            }
        )
    by_id = {row["variant_id"]: row for row in metric_rows}
    best = metric_rows[0]
    second_best = metric_rows[1]
    worst = metric_rows[-1]
    profitable = sum(row["net_pnl_twd"] > 0.0 for row in metric_rows)

    best_observed = opportunities.sort(
        "max_observed_net_edge_twd", descending=True
    ).row(0, named=True)
    best_cycle = cycles.sort("net_pnl_twd", descending=True).row(0, named=True)
    total_cost_positive_signal_seconds = int(opportunities["signal_seconds"].sum())

    daily_rows = [
        {
            "date": row["trading_date"].isoformat(),
            "strategy": LABELS[row["variant_id"]],
            "variant_id": row["variant_id"],
            "daily_net_pnl_twd": row["daily_net_pnl_twd"],
            "daily_return": row["daily_return"],
            "cumulative_return": row["cumulative_compounded_return"],
        }
        for row in daily.iter_rows(named=True)
    ]
    ratio_rows = [
        {
            "strategy": row["strategy"],
            "metric": label,
            "value": row[field],
        }
        for row in metric_rows
        for field, label in (
            ("annualized_sharpe", "Sharpe"),
            ("annualized_sortino", "Sortino"),
            ("annualized_calmar", "Calmar"),
        )
        if row[field] is not None
    ]
    cycle_rows = [
        {
            "entry_date": row["entry_date"].isoformat(),
            "exit_date": row["exit_date"].isoformat(),
            "expiry": row["expiry"].isoformat(),
            "exit_reason": row["exit_reason"],
            "strategy": LABELS[row["variant_id"]],
            "series": row["series"],
            "direction": row["direction"],
            "strikes": row["strikes_json"],
            "signal_edge_twd": row["observed_gross_edge_twd"],
            "estimated_cost_twd": row["estimated_cost_at_signal_twd"],
            "fill_delay_seconds": row["max_fill_delay_seconds"],
            "exit_fill_delay_seconds": row["exit_fill_delay_seconds"],
            "holding_seconds": row["holding_seconds"],
            "gross_pnl_twd": row["gross_pnl_twd"],
            "fees_twd": row["total_fixed_fees_twd"],
            "entry_taxes_twd": row["entry_taxes_twd"],
            "exit_taxes_twd": row["exit_taxes_twd"],
            "settlement_taxes_twd": row["settlement_taxes_twd"],
            "settlement_prices": row["official_final_settlement_prices_json"],
            "net_pnl_twd": row["net_pnl_twd"],
        }
        for row in cycles.iter_rows(named=True)
    ]
    latency_rows = [
        {
            "strategy": LABELS[row["variant_id"]],
            "variant_id": row["variant_id"],
            "cycles": row["cycles"],
            "median_entry_fill_delay_seconds": row["median_entry_fill_delay_seconds"],
            "max_entry_fill_delay_seconds": row["max_entry_fill_delay_seconds"],
            "median_exit_fill_delay_seconds": row["median_exit_fill_delay_seconds"],
            "max_exit_fill_delay_seconds": row["max_exit_fill_delay_seconds"],
            "median_holding_seconds": row["median_holding_seconds"],
            "max_holding_seconds": row["max_holding_seconds"],
            "net_pnl_twd": row["net_pnl_twd"],
        }
        for row in (
            cycles.group_by("variant_id")
            .agg(
                pl.len().alias("cycles"),
                pl.col("max_fill_delay_seconds")
                .median()
                .alias("median_entry_fill_delay_seconds"),
                pl.col("max_fill_delay_seconds")
                .max()
                .alias("max_entry_fill_delay_seconds"),
                pl.col("exit_fill_delay_seconds")
                .median()
                .alias("median_exit_fill_delay_seconds"),
                pl.col("exit_fill_delay_seconds")
                .max()
                .alias("max_exit_fill_delay_seconds"),
                pl.col("holding_seconds").median().alias("median_holding_seconds"),
                pl.col("holding_seconds").max().alias("max_holding_seconds"),
                pl.col("net_pnl_twd").sum().alias("net_pnl_twd"),
            )
            .sort("net_pnl_twd", descending=True)
            .iter_rows(named=True)
        )
    ]
    latency_by_id = {row["variant_id"]: row for row in latency_rows}
    parity_latency = latency_by_id["monthly_put_call_parity_mtx"]
    convergence_count = int(
        cycles.filter(pl.col("exit_reason") == "relationship_convergence").height
    )
    expiry_count = int(
        cycles.filter(pl.col("exit_reason") == "expiry_settlement").height
    )
    multi_expiry_count = int(
        cycles.filter(pl.col("exit_reason") == "multi_expiry_settlement").height
    )
    same_day_cycles = int(
        cycles.filter(pl.col("entry_date") == pl.col("exit_date")).height
    )
    scope_frame = (
        cycles.with_columns(
            pl.when(pl.col("series").str.contains(r"\|"))
            .then(pl.lit("跨到期"))
            .when(pl.col("series").str.len_chars() == 6)
            .then(pl.lit("月選"))
            .when(pl.col("series").str.contains("W"))
            .then(pl.lit("週三週選"))
            .otherwise(pl.lit("週五週選"))
            .alias("series_scope")
        )
        .group_by(["variant_id", "series_scope"])
        .agg(
            pl.len().alias("cycles"),
            (pl.col("net_pnl_twd") > 0.0).sum().alias("winning_cycles"),
            pl.col("gross_pnl_twd").sum().alias("gross_pnl_twd"),
            pl.col("total_fixed_fees_twd").sum().alias("fees_twd"),
            (
                pl.col("entry_taxes_twd")
                + pl.col("exit_taxes_twd")
                + pl.col("settlement_taxes_twd")
            )
            .sum()
            .alias("taxes_twd"),
            pl.col("net_pnl_twd").sum().alias("net_pnl_twd"),
        )
        .sort(["series_scope", "net_pnl_twd"], descending=[False, True])
    )
    series_scope_rows = [
        {
            "strategy": LABELS[row["variant_id"]],
            **row,
        }
        for row in scope_frame.iter_rows(named=True)
    ]
    weekly_scope = scope_frame.filter(
        pl.col("series_scope").is_in(["週三週選", "週五週選"])
    )
    weekly_cycles = int(weekly_scope["cycles"].sum())
    weekly_net_pnl = float(weekly_scope["net_pnl_twd"].sum())
    opportunity_rows = [
        {
            "strategy": LABELS[row["variant_id"]],
            "signal_seconds": row["signal_seconds"],
            "max_observed_gross_edge_twd": row["max_observed_gross_edge_twd"],
            "max_observed_net_edge_twd": row["max_observed_net_edge_twd"],
            "unfillable_attempts": row["unfillable_signal_attempts"],
            "unfillable_exit_attempts": row["convergence_exit_fill_failures"],
        }
        for row in opportunities.iter_rows(named=True)
    ]
    evidence_layer_rows = [
        {
            "layer_order": 1,
            "layer": "L1",
            "status": "可計算",
            "coverage": "30/30 交易日",
            "price_evidence": "完成秒後，各腿最近一筆成交價（各腿不必在同一秒成交）",
            "costs_included": "估計雙邊固定手續費 + 雙邊交易稅",
            "headline_metric": "單一組合最大成本後表面錯價",
            "headline_value_twd": float(best_observed["max_observed_net_edge_twd"]),
            "secondary_result": (
                f"{total_cost_positive_signal_seconds:,} 個成本後正值秒；"
                "同一錯價可跨秒重複，不能加總為獲利"
            ),
            "best_strategy": LABELS[best_observed["variant_id"]],
            "interpretation": "理想觀察上限，不是可成交獲利",
        },
        {
            "layer_order": 2,
            "layer": "L2",
            "status": "可計算（成交價代理）",
            "coverage": "30/30 交易日",
            "price_evidence": "進場與收斂平倉皆用各腿第一筆嚴格較晚成交；跨到期未收斂時各系列按自己的到期日結算",
            "costs_included": "實際雙邊手續費與交易稅；未交易平倉的腿改計結算稅",
            "headline_metric": "最佳策略全月淨利",
            "headline_value_twd": float(best["net_pnl_twd"]),
            "secondary_result": (
                f"最佳單一完成週期 {best_cycle['net_pnl_twd']:,.0f} 元；"
                f"{profitable}/{len(metric_rows)} 策略為正、共 {cycles.height} 個週期"
            ),
            "best_strategy": LABELS[best_cycle["variant_id"]],
            "interpretation": "因跨腿成交可相隔很久，這是因果成交代理，不是同步可成交套利",
        },
        {
            "layer_order": 3,
            "layer": "L3",
            "status": "不可計算",
            "coverage": "0/30 交易日",
            "price_evidence": "各腿同時有效的主動買／賣價與顯示量",
            "costs_included": "雙邊固定手續費 + 交易稅；必要時到期結算稅",
            "headline_metric": "歷史 Bid/Ask 可成交套利",
            "headline_value_twd": None,
            "secondary_result": "月內沒有歷史 Bid/Ask；2026-08-10 單日簿在樣本外",
            "best_strategy": None,
            "interpretation": "缺少證據，不能用成交價或樣本外簿資料代填",
        },
    ]
    summary_rows = [
        {
            "best_net_pnl_twd": best["net_pnl_twd"],
            "best_simple_return": best["simple_return"],
            "profitable_variants": profitable,
            "tested_variants": len(metric_rows),
            "completed_cycles": int(cycles.height),
            "trading_days": int(summary["sample"]["trading_days"]),
            "common_capital_twd": float(summary["capital"]["common_capital_twd"]),
            "l1_max_cost_adjusted_edge_twd": float(
                best_observed["max_observed_net_edge_twd"]
            ),
            "l2_best_variant_month_net_pnl_twd": float(best["net_pnl_twd"]),
            "l3_bidask_coverage_days": 0,
        }
    ]

    sources = [
        {
            "id": "evidence_layers_source",
            "label": "Monthly cost-aware evidence layers",
            "path": f"{source_prefix}/evidence_layers.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "Load the three evidence layers for the receipt-verified 30-day window.",
                "sql": f"SELECT * FROM read_parquet('{source_prefix}/evidence_layers.parquet') ORDER BY layer_order",
                "tables_used": [f"{source_prefix}/evidence_layers.parquet"],
                "filters": [
                    "2026-06-25 through 2026-08-06",
                    "layer 1 already includes estimated round-trip fees and transaction taxes",
                    "out-of-window 2026-08-10 BidAsk capture excluded",
                ],
                "metric_definitions": {
                    "L1": "Maximum completed-second as-of transaction relationship after estimated round-trip fixed fees and transaction taxes.",
                    "L2": "First strictly later transaction per entry leg; close on convergence using first strictly later transactions again, or settle each series on its own official expiry when a cross-expiry box remains open.",
                    "L3": "Simultaneous active Bid/Ask and displayed size; unavailable for all 30 in-window dates.",
                },
            },
        },
        {
            "id": "metrics_source",
            "label": "Capital-normalized convergence-arbitrage metrics",
            "path": f"{source_prefix}/metrics.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "Load the reconciled strategy metric artifact produced by the causal backtest.",
                "sql": f"SELECT * FROM read_parquet('{source_prefix}/metrics.parquet') ORDER BY total_net_pnl_twd DESC",
                "tables_used": [f"{source_prefix}/metrics.parquet"],
                "filters": [
                    "trading_date between 2026-06-25 and 2026-08-06",
                    "one independent strategy package",
                    "common capital TWD 1272000",
                ],
                "metric_definitions": {
                    "net_pnl_twd": "Convergence-exit or single/multi-expiry settlement gross P&L less entry and exit fixed fees, entry and exit transaction taxes, and any applicable settlement tax.",
                    "simple_return": "Total net P&L divided by the common TWD 1,272,000 capital denominator.",
                    "annualized_sharpe": "Mean daily common-capital return divided by sample standard deviation and multiplied by sqrt(252).",
                    "annualized_sortino": "Mean daily return divided by zero-target downside deviation and multiplied by sqrt(252).",
                    "maximum_drawdown": "Minimum peak-to-trough drawdown of compounded daily common-capital wealth.",
                    "annualized_calmar": "Annualized compounded return divided by absolute maximum drawdown.",
                },
            },
        },
        {
            "id": "daily_source",
            "label": "Daily marked returns",
            "path": f"{source_prefix}/daily_returns.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "Load all 30 daily marks for every strategy variant.",
                "sql": f"SELECT * FROM read_parquet('{source_prefix}/daily_returns.parquet') ORDER BY variant_id, trading_date",
                "tables_used": [f"{source_prefix}/daily_returns.parquet"],
                "filters": [
                    "30 receipt-verified trading dates",
                    "realized convergence exits plus any end-of-day open marks",
                    "each option series uses its own official final settlement when required",
                ],
            },
        },
        {
            "id": "cycles_source",
            "label": "Causal all-series entry, convergence, and settlement cycles",
            "path": f"{source_prefix}/cycles.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "Load every completed monthly, weekly, and cross-expiry causal cycle.",
                "sql": f"SELECT * FROM read_parquet('{source_prefix}/cycles.parquet') ORDER BY variant_id, entry_date",
                "tables_used": [f"{source_prefix}/cycles.parquet"],
                "filters": [
                    "first strictly later transaction per leg",
                    "first convergence after completed seconds",
                    "all complete monthly, Wednesday-weekly, and Friday-weekly TXO series",
                    "cross-expiry boxes settle each component on its own expiry at zero financing interest",
                    "same-day re-entry after an exit disabled",
                ],
            },
        },
        {
            "id": "opportunity_source",
            "label": "Completed-second violation scan",
            "path": f"{source_prefix}/opportunity_summary.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "Aggregate completed-second model-free bound violations by strategy.",
                "sql": f"SELECT * FROM read_parquet('{source_prefix}/opportunity_summary.parquet') ORDER BY variant_id",
                "tables_used": [f"{source_prefix}/opportunity_summary.parquet"],
                "filters": [
                    "signals use completed whole seconds",
                    "displayed violations are not summed as P&L",
                ],
            },
        },
        {
            "id": "quality_source",
            "label": "Data-quality and execution-proxy audit",
            "path": f"{source_prefix}/data_quality.json",
        },
        {
            "id": "tick_source",
            "label": "Receipt-verified TAIFEX transaction archive",
            "path": "data_tw_index_derivatives_ticks/manifest.json",
        },
        {
            "id": "settlement_source",
            "label": "Official TXO final settlement history",
            "path": "data_tw_index_options_daily/txo_final_settlement_history.parquet",
        },
        {
            "id": "code_source",
            "label": "Reproducible arbitrage scanner",
            "path": "scripts/backtest_taifex_index_arbitrage.py",
        },
    ]

    cards = [
        {
            "id": "layer_1_result",
            "description": "L1 starts after estimated round-trip fixed fees and transaction taxes.",
            "dataset": "summary",
            "sourceId": "evidence_layers_source",
            "metrics": [
                {
                    "label": "L1 max cost-adjusted edge",
                    "field": "l1_max_cost_adjusted_edge_twd",
                    "format": "number",
                    "unit": "TWD",
                },
            ],
        },
        {
            "id": "layer_2_result",
            "description": "Best full-month strategy total under the causal next-trade proxy after exact modeled fees and taxes.",
            "dataset": "summary",
            "sourceId": "evidence_layers_source",
            "metrics": [
                {
                    "label": "L2 best full-month result",
                    "field": "l2_best_variant_month_net_pnl_twd",
                    "format": "number",
                    "unit": "TWD",
                },
            ],
        },
        {
            "id": "layer_3_coverage",
            "description": "No simultaneous historical Bid/Ask coverage exists inside the month.",
            "dataset": "summary",
            "sourceId": "evidence_layers_source",
            "metrics": [
                {
                    "label": "L3 Bid/Ask days",
                    "field": "l3_bidask_coverage_days",
                    "format": "number",
                },
            ],
        },
        {
            "id": "best_result",
            "description": "The highest net P&L among the nine independent one-package variants.",
            "dataset": "summary",
            "sourceId": "metrics_source",
            "metrics": [
                {
                    "label": "Best net P&L",
                    "field": "best_net_pnl_twd",
                    "format": "number",
                    "unit": "TWD",
                },
                {
                    "label": "Best simple return",
                    "field": "best_simple_return",
                    "format": "percent",
                    "signed": True,
                },
            ],
        },
        {
            "id": "coverage",
            "description": "Completed convergence or single/multi-expiry settlement cycles in the receipt-verified window.",
            "dataset": "summary",
            "sourceId": "metrics_source",
            "metrics": [
                {
                    "label": "Profitable variants",
                    "field": "profitable_variants",
                    "format": "number",
                },
                {
                    "label": "Tested variants",
                    "field": "tested_variants",
                    "format": "number",
                },
                {
                    "label": "Completed cycles",
                    "field": "completed_cycles",
                    "format": "number",
                },
                {"label": "Trading days", "field": "trading_days", "format": "number"},
            ],
        },
        {
            "id": "capital",
            "description": "Every strategy uses exactly the same return denominator.",
            "dataset": "summary",
            "sourceId": "metrics_source",
            "metrics": [
                {
                    "label": "Common capital",
                    "field": "common_capital_twd",
                    "format": "number",
                    "unit": "TWD",
                },
            ],
        },
    ]
    charts = [
        {
            "id": "net_pnl",
            "title": "Net P&L by one-package strategy",
            "subtitle": "Convergence-exit P&L after entry and exit commissions and transaction taxes.",
            "type": "bar",
            "dataset": "metrics",
            "sourceId": "metrics_source",
            "encodings": {
                "x": {"field": "strategy", "type": "nominal", "label": "Strategy"},
                "y": {
                    "field": "net_pnl_twd",
                    "type": "quantitative",
                    "label": "Net P&L",
                    "format": "number",
                    "unit": "TWD",
                },
            },
            "valueFormat": "number",
            "unit": "TWD",
            "layout": "full",
        },
        {
            "id": "daily_curves",
            "title": "Daily compounded return curves",
            "subtitle": "Daily realized convergence P&L and any active end-of-day marks on a common TWD 1.272 million capital base.",
            "type": "line",
            "dataset": "daily",
            "sourceId": "daily_source",
            "encodings": {
                "x": {"field": "date", "type": "temporal", "label": "Date"},
                "y": {
                    "field": "cumulative_return",
                    "type": "quantitative",
                    "label": "Cumulative return",
                    "format": "percent",
                },
                "color": {"field": "strategy", "type": "nominal", "label": "Strategy"},
            },
            "valueFormat": "percent",
            "layout": "full",
        },
        {
            "id": "risk_ratios",
            "title": "Annualized risk ratios",
            "subtitle": "Sharpe, Sortino and Calmar from 30 daily marked returns; short samples require caution.",
            "type": "bar",
            "dataset": "ratios",
            "sourceId": "metrics_source",
            "encodings": {
                "x": {"field": "strategy", "type": "nominal", "label": "Strategy"},
                "y": {
                    "field": "value",
                    "type": "quantitative",
                    "label": "Ratio",
                    "format": "number",
                },
                "color": {"field": "metric", "type": "nominal", "label": "Metric"},
            },
            "valueFormat": "number",
            "layout": "full",
        },
        {
            "id": "series_scope_pnl",
            "title": "Net P&L by option-series scope",
            "subtitle": "Executed cycles split into monthly, Wednesday weekly, Friday weekly, and cross-expiry packages.",
            "type": "bar",
            "dataset": "series_scope",
            "sourceId": "cycles_source",
            "encodings": {
                "x": {"field": "strategy", "type": "nominal", "label": "Strategy"},
                "y": {
                    "field": "net_pnl_twd",
                    "type": "quantitative",
                    "label": "Net P&L",
                    "format": "number",
                    "unit": "TWD",
                },
                "color": {
                    "field": "series_scope",
                    "type": "nominal",
                    "label": "Series scope",
                },
            },
            "valueFormat": "number",
            "unit": "TWD",
            "layout": "full",
        },
        {
            "id": "execution_latency",
            "title": "Maximum cross-leg entry fill delay",
            "subtitle": "Elapsed seconds from the completed-second decision to the slowest first strictly later transaction; lower is better.",
            "type": "bar",
            "dataset": "latency",
            "sourceId": "cycles_source",
            "encodings": {
                "x": {"field": "strategy", "type": "nominal", "label": "Strategy"},
                "y": {
                    "field": "max_entry_fill_delay_seconds",
                    "type": "quantitative",
                    "label": "Maximum delay",
                    "format": "number",
                    "unit": "seconds",
                },
            },
            "valueFormat": "number",
            "unit": "seconds",
            "layout": "full",
        },
    ]
    tables = [
        {
            "id": "evidence_layers_table",
            "title": "30-day cost-aware evidence layers",
            "subtitle": "L1 already includes fees and taxes; L2 changes the pricing clock; L3 is unavailable rather than imputed.",
            "dataset": "evidence_layers",
            "sourceId": "evidence_layers_source",
            "defaultSort": {"field": "layer", "direction": "asc"},
            "columns": [
                {"field": "layer", "label": "Layer", "type": "text"},
                {"field": "status", "label": "Status", "type": "text"},
                {"field": "coverage", "label": "Coverage", "type": "text"},
                {"field": "price_evidence", "label": "Price evidence", "type": "text"},
                {"field": "costs_included", "label": "Costs included", "type": "text"},
                {"field": "headline_metric", "label": "Metric", "type": "text"},
                {
                    "field": "headline_value_twd",
                    "label": "Value (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "secondary_result",
                    "label": "Monthly result",
                    "type": "text",
                },
                {"field": "interpretation", "label": "Meaning", "type": "text"},
            ],
        },
        {
            "id": "metrics_table",
            "title": "Full strategy metrics",
            "dataset": "metrics",
            "sourceId": "metrics_source",
            "defaultSort": {"field": "net_pnl_twd", "direction": "desc"},
            "columns": [
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {"field": "completed_cycles", "label": "Cycles", "format": "number"},
                {"field": "winning_cycles", "label": "Wins", "format": "number"},
                {
                    "field": "convergence_exits",
                    "label": "Convergence exits",
                    "format": "number",
                },
                {
                    "field": "expiry_settlements",
                    "label": "Expiry fallbacks",
                    "format": "number",
                },
                {
                    "field": "multi_expiry_settlements",
                    "label": "Multi-expiry settlements",
                    "format": "number",
                },
                {"field": "fees_twd", "label": "Fees (TWD)", "format": "number"},
                {
                    "field": "net_pnl_twd",
                    "label": "Net P&L (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "simple_return",
                    "label": "Simple return",
                    "format": "percent",
                    "movement": True,
                },
                {"field": "annualized_sharpe", "label": "Sharpe", "format": "number"},
                {"field": "annualized_sortino", "label": "Sortino", "format": "number"},
                {"field": "annualized_calmar", "label": "Calmar", "format": "number"},
                {
                    "field": "maximum_drawdown",
                    "label": "MDD",
                    "format": "percent",
                    "movement": True,
                },
            ],
        },
        {
            "id": "series_scope_table",
            "title": "Executed results by monthly and weekly series",
            "subtitle": "Weekly means actual W/F TXO series; cross-expiry packages are shown separately.",
            "dataset": "series_scope",
            "sourceId": "cycles_source",
            "defaultSort": {"field": "net_pnl_twd", "direction": "desc"},
            "columns": [
                {"field": "series_scope", "label": "Series scope", "type": "text"},
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {"field": "cycles", "label": "Cycles", "format": "number"},
                {
                    "field": "winning_cycles",
                    "label": "Wins",
                    "format": "number",
                },
                {
                    "field": "gross_pnl_twd",
                    "label": "Gross P&L (TWD)",
                    "format": "number",
                },
                {"field": "fees_twd", "label": "Fees (TWD)", "format": "number"},
                {"field": "taxes_twd", "label": "Taxes (TWD)", "format": "number"},
                {
                    "field": "net_pnl_twd",
                    "label": "Net P&L (TWD)",
                    "format": "number",
                    "movement": True,
                },
            ],
        },
        {
            "id": "opportunity_table",
            "title": "Observed violation seconds versus executable evidence",
            "dataset": "opportunities",
            "sourceId": "opportunity_source",
            "defaultSort": {"field": "signal_seconds", "direction": "desc"},
            "columns": [
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {
                    "field": "signal_seconds",
                    "label": "Violation seconds",
                    "format": "number",
                },
                {
                    "field": "max_observed_net_edge_twd",
                    "label": "L1 max after costs (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "max_observed_gross_edge_twd",
                    "label": "Max before costs (audit only, TWD)",
                    "format": "number",
                },
                {
                    "field": "unfillable_attempts",
                    "label": "No entry fill",
                    "format": "number",
                },
                {
                    "field": "unfillable_exit_attempts",
                    "label": "No exit fill",
                    "format": "number",
                },
            ],
        },
        {
            "id": "cycles_table",
            "title": "All completed convergence and single/multi-expiry settlement cycles",
            "dataset": "cycles",
            "sourceId": "cycles_source",
            "defaultSort": {"field": "entry_date", "direction": "asc"},
            "columns": [
                {"field": "entry_date", "label": "Entry", "type": "date"},
                {"field": "exit_date", "label": "Exit", "type": "date"},
                {"field": "expiry", "label": "Expiry", "type": "date"},
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {"field": "series", "label": "Series", "type": "text"},
                {"field": "exit_reason", "label": "Exit reason", "type": "text"},
                {
                    "field": "fill_delay_seconds",
                    "label": "Entry delay (s)",
                    "format": "number",
                },
                {
                    "field": "exit_fill_delay_seconds",
                    "label": "Exit delay (s)",
                    "format": "number",
                },
                {
                    "field": "holding_seconds",
                    "label": "Holding (s)",
                    "format": "number",
                },
                {
                    "field": "signal_edge_twd",
                    "label": "Signal edge (TWD)",
                    "format": "number",
                },
                {
                    "field": "net_pnl_twd",
                    "label": "Net P&L (TWD)",
                    "format": "number",
                    "movement": True,
                },
            ],
        },
        {
            "id": "latency_table",
            "title": "Cross-leg execution latency by strategy",
            "subtitle": "Transaction-print proxy; these are not simultaneous Bid/Ask fills.",
            "dataset": "latency",
            "sourceId": "cycles_source",
            "defaultSort": {
                "field": "max_entry_fill_delay_seconds",
                "direction": "desc",
            },
            "columns": [
                {"field": "strategy", "label": "Strategy", "type": "text"},
                {"field": "cycles", "label": "Cycles", "format": "number"},
                {
                    "field": "median_entry_fill_delay_seconds",
                    "label": "Median entry delay (s)",
                    "format": "number",
                },
                {
                    "field": "max_entry_fill_delay_seconds",
                    "label": "Max entry delay (s)",
                    "format": "number",
                },
                {
                    "field": "median_exit_fill_delay_seconds",
                    "label": "Median exit delay (s)",
                    "format": "number",
                },
                {
                    "field": "max_exit_fill_delay_seconds",
                    "label": "Max exit delay (s)",
                    "format": "number",
                },
                {
                    "field": "median_holding_seconds",
                    "label": "Median holding (s)",
                    "format": "number",
                },
                {
                    "field": "max_holding_seconds",
                    "label": "Max holding (s)",
                    "format": "number",
                },
            ],
        },
    ]

    parity = by_id["monthly_put_call_parity_mtx"]
    calendar_box = by_id["cross_expiry_calendar_box"]
    blocks = [
        {
            "id": "title",
            "type": "markdown",
            "body": "# 台指期／台指選擇權一個月收斂套利實測",
        },
        {
            "id": "summary_text",
            "type": "markdown",
            "body": (
                "## 結論先講\n\n"
                "第一層已扣估計雙邊手續費與雙邊交易稅，不把零成本理論價當第一層。"
                f"在 2026-06-25 至 2026-08-06 的 30 個交易日中，L1 最大成本後表面錯價為 **{_twd(best_observed['max_observed_net_edge_twd'])}**，來自 {LABELS[best_observed['variant_id']]}，但各腿只是截至該秒的最近成交，並非同時可成交價。\n\n"
                "本版已掃描樣本內可完整結算的全部月選、週三週選 W 與週五週選 F，不再只選最近一個系列。"
                f"L2 在完成秒後用各腿第一筆嚴格較晚成交進場，關係回到無套利區間後再用嚴格較晚成交平倉；{len(metric_rows)} 組一口策略只有 **{profitable}/{len(metric_rows)}** 全月淨利為正。"
                f"唯一正值是月選 Put–Call parity + MTX：{parity['completed_cycles']} 個週期、{parity['winning_cycles']} 勝，淨利 **{_twd(parity['net_pnl_twd'])}**，共同資本簡單報酬 **{_pct(parity['simple_return'])}**，Sharpe **{parity['annualized_sharpe']:.2f}**、Sortino **{parity['annualized_sortino']:.2f}**、Calmar **{parity['annualized_calmar']:.2f}**、MDD **{_pct(parity['maximum_drawdown'])}**。"
                f"跨到期 Calendar box 為 {calendar_box['completed_cycles']} 個週期、{calendar_box['winning_cycles']} 勝，淨損 **{_twd(abs(calendar_box['net_pnl_twd']))}**。全部 {cycles.height} 個週期中，{convergence_count} 個收斂平倉、{expiry_count} 個單一到期備援、{multi_expiry_count} 個按兩個系列各自到期結算。\n\n"
                f"但正值策略的最慢進場跨腿成交延遲達 **{parity_latency['max_entry_fill_delay_seconds']:,.0f} 秒**，最慢出場延遲 **{parity_latency['max_exit_fill_delay_seconds']:,.0f} 秒**。"
                "月內 30/30 日都沒有歷史 Bid/Ask，因此這是因果成交價代理的研究正值，不是已證明可同步下單的無風險獲利。"
            ),
        },
        {
            "id": "evidence_layers_heading",
            "type": "markdown",
            "body": (
                "## 第一層就扣成本：三層結果不能混為同一種獲利\n\n"
                "L1 是成本後的**表面錯價上限**；L2 用之後真正出現的成交進場與收斂平倉，所以是因果成交代理；L3 才是同時 Bid/Ask 可成交證據。"
                "由於 L2 更換了價格時點，它不是從同一批 L1 報價逐項扣除的嚴格單調漏斗，因此以下用證據表呈現，不畫會誤導的漏斗圖。"
            ),
        },
        {
            "id": "layer_cards",
            "type": "metric-strip",
            "cardIds": ["layer_1_result", "layer_2_result", "layer_3_coverage"],
        },
        {
            "id": "evidence_layers_detail",
            "type": "table",
            "tableId": "evidence_layers_table",
            "layout": "full",
        },
        {
            "id": "evidence_layers_interpretation",
            "type": "markdown",
            "sourceId": "evidence_layers_source",
            "body": (
                f"L1 共出現 **{total_cost_positive_signal_seconds:,} 個成本後為正的秒**，但同一組陳舊成交價可連續存在很多秒，不能把秒數或每秒最大值加總成月獲利。"
                f"L2 的最佳單一完成週期是 {best_cycle['entry_date'].isoformat()} 的 {LABELS[best_cycle['variant_id']]}，成本後 **{_twd(best_cycle['net_pnl_twd'])}**；最佳策略的全月合計仍只有 **{_twd(best['net_pnl_twd'])}**。"
                f"L2 共 {convergence_count} 次收斂平倉、{expiry_count} 次單一到期備援、{multi_expiry_count} 次跨到期雙結算。"
                "L3 因 0/30 日有歷史 Bid/Ask 而留白，不以成交價冒充可成交價。"
            ),
        },
        {
            "id": "headline_cards",
            "type": "metric-strip",
            "cardIds": ["best_result", "coverage", "capital"],
        },
        {"id": "pnl_chart", "type": "chart", "chartId": "net_pnl", "layout": "full"},
        {
            "id": "pnl_interpretation",
            "type": "markdown",
            "sourceId": "metrics_source",
            "body": (
                f"淨利圖使用每組彼此獨立的一個策略單位，並統一除以 127.2 萬元；因此原始 TWD 與報酬可比較，但不可把 {len(metric_rows)} 組損益直接相加成同一帳戶績效。"
                f"只有 {best['strategy']} 為正；次佳的 {second_best['strategy']} 仍淨損 **{_twd(abs(second_best['net_pnl_twd']))}**，最差的 {worst['strategy']} 淨損 **{_twd(abs(worst['net_pnl_twd']))}**。"
            ),
        },
        {
            "id": "series_scope_chart",
            "type": "chart",
            "chartId": "series_scope_pnl",
            "layout": "full",
        },
        {
            "id": "series_scope_detail",
            "type": "table",
            "tableId": "series_scope_table",
            "layout": "full",
        },
        {
            "id": "series_scope_interpretation",
            "type": "markdown",
            "sourceId": "cycles_source",
            "body": (
                f"同到期價界策略實際執行了 **{weekly_cycles} 個週選週期**（週三 W 與週五 F），合計淨損 **{_twd(abs(weekly_net_pnl))}**。"
                "表格同時保留月選拆分，證明週選不是只出現在掃描候選，而是真的進入成交與費稅帳本。跨到期組合另列，避免把它重複歸入任一單一期別。"
            ),
        },
        {
            "id": "curve_chart",
            "type": "chart",
            "chartId": "daily_curves",
            "layout": "full",
        },
        {
            "id": "curve_interpretation",
            "type": "markdown",
            "sourceId": "daily_source",
            "body": (
                f"本樣本 {cycles.height} 個完成週期中有 {same_day_cycles} 個同日平倉，因此曲線主要反映每天已實現的收斂損益。"
                "若日末仍有持倉，使用官方日結算價、缺值時退回最後成交作標記；跨到期 box 的近端與遠端各在自己的官方結算日認列固定現金流與結算稅。"
            ),
        },
        {
            "id": "ratio_chart",
            "type": "chart",
            "chartId": "risk_ratios",
            "layout": "full",
        },
        {
            "id": "ratio_interpretation",
            "type": "markdown",
            "sourceId": "metrics_source",
            "body": (
                "Sharpe、Sortino、Calmar 均按 252 日年化；MDD 是共同資本複利曲線的峰谷跌幅。"
                "只有 30 個每日觀測，年化比率只供描述，不具穩健推論力；月選 parity 雖有 14 次收斂週期，仍不足以單獨形成投資結論。"
            ),
        },
        {
            "id": "latency_chart",
            "type": "chart",
            "chartId": "execution_latency",
            "layout": "full",
        },
        {
            "id": "latency_detail",
            "type": "table",
            "tableId": "latency_table",
            "layout": "full",
        },
        {
            "id": "latency_interpretation",
            "type": "markdown",
            "sourceId": "cycles_source",
            "body": (
                f"月選 Put–Call parity 的進場延遲中位數為 **{parity_latency['median_entry_fill_delay_seconds']:,.1f} 秒**、最大值 **{parity_latency['max_entry_fill_delay_seconds']:,.0f} 秒**；出場延遲中位數 **{parity_latency['median_exit_fill_delay_seconds']:,.1f} 秒**、最大值 **{parity_latency['max_exit_fill_delay_seconds']:,.0f} 秒**。"
                "這種跨腿不同步會把陳舊成交拼成看似獲利的 package，是目前正值結果最重要的可執行性限制。"
            ),
        },
        {
            "id": "metrics_detail",
            "type": "table",
            "tableId": "metrics_table",
            "layout": "full",
        },
        {
            "id": "execution_heading",
            "type": "markdown",
            "body": "## 為什麼看起來脫鉤，仍不能直接視為套利？\n\nTAIFEX 公開逐筆檔只有整秒成交，沒有歷史買一／賣一。不同商品與履約價的最後成交時間不一致，陳舊價格拼在一起會產生很大的表面違反；等待各腿之後成交則可能跨越數分鐘甚至數小時。",
        },
        {
            "id": "opportunity_detail",
            "type": "table",
            "tableId": "opportunity_table",
            "layout": "full",
        },
        {
            "id": "opportunity_interpretation",
            "type": "markdown",
            "sourceId": "opportunity_source",
            "body": (
                "表中的 L1 已扣估計雙邊固定手續費與雙邊交易稅。對同到期策略，Violation seconds 是各系列分別在完成秒後出現成本後正值的 series-seconds；跨到期策略則是配對掃描秒。兩者都不是交易次數，也不能加總為獲利。"
                "回測在沒有持倉時從全部合格系列採用最早可完成的一個成本後訊號，逐腿等待嚴格較晚成交；關係回到無套利區間後再逐腿等待嚴格較晚成交平倉。同日平倉後不再次進場，避免反覆收割同一組陳舊成交。"
            ),
        },
        {
            "id": "methods",
            "type": "markdown",
            "sourceId": "code_source",
            "body": (
                "## 方法與套利方向\n\n"
                "- **等值期貨**：TX 對 4 MTX、TX 對 20 TMF、4 MTX 對 20 TMF；同月份與同最終結算值使點值相消。\n"
                "- **Put–Call parity**：同履約價月選 Call／Put 加 1 口 MTX，讓到期指數曝險相消。\n"
                "- **Vertical bounds**：對每個月選、週三 W 與週五 F 系列，Call／Put 垂直價差必須介於 0 與履約價寬度。\n"
                "- **Butterfly bounds**：對每個完整系列，等距蝶式價差必須介於 0 與翼寬。\n"
                "- **Box spread**：每個同到期 box 的固定到期給付應等於履約價寬度。\n"
                "- **跨到期 Calendar box**：買低估的一個到期 box、賣高估的另一個到期 box；兩邊各自只有固定履約現金流。以 0% 融資率比較，未在近端到期前收斂時，各系列按自己的官方結算日入帳。\n\n"
                "不同到期的裸 synthetic forward 並未加入，因為沒有相同日期的期貨／現貨籃子去抵消兩個結算時點的指數與股息風險；把它直接叫套利會重複引入方向風險。每個訊號在整秒結束後決策；每腿只接受嚴格較晚成交。"
                "手續費為 TX 60、MTX 24、TMF 16、TXO 22 元／口／邊，進場與交易平倉皆另計期交稅；只有到期備援才改計選擇權現金結算稅。Butterfly 的一個策略單位在中間履約價交易 2 口。"
            ),
        },
        {
            "id": "cycle_detail",
            "type": "table",
            "tableId": "cycles_table",
            "layout": "full",
        },
        {
            "id": "limitations",
            "type": "markdown",
            "sourceId": "quality_source",
            "body": (
                "## 資料限制與不可計入項目\n\n"
                f"歷史 30 日逐筆交易檔完整，但沒有 bid/ask。永豐只有 2026-08-10 的單日簿資料（366,370 列、有效簿比例 {quality['shioaji_book_capture_outside_backtest']['valid_book_fraction']:.2%}），在本樣本之外且為模擬帳號環境，因此沒有混入回測。\n\n"
                "成交價代理沒有主動買進 ask、主動賣出 bid、掛單量與 package 同時成交保證；尤其月選 parity 與八腿跨到期 box 的長延遲會高估可執行性。跨到期 box 的確有兩個固定履約現金流，但目前採 0% 融資率，未建模現金提領限制與組合保證金。台指現貨指數本身不能直接買賣，工作區也沒有同步的成分股籃子逐筆可成交成本、股息與融資資料。"
            ),
        },
        {
            "id": "next_steps",
            "type": "markdown",
            "body": (
                "## 下一步\n\n"
                f"只有月選 Put–Call parity + MTX 在成交價代理下呈正，月淨利 **{_twd(parity['net_pnl_twd'])}**。依你的要求，現在不額外加入壓力測試；下一個必要的真實限制只有前瞻 Bid/Ask 驗證：所有腿同時以可成交 ask/bid 重估並記錄顯示量。"
                "在同時可成交價結果仍為正以前，不把這筆代理獲利視為可重複實現的套利收益。"
            ),
        },
    ]

    return {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "台指期／台指選擇權一個月收斂套利實測",
            "description": "30-day cost-aware monthly, Wednesday-weekly, Friday-weekly, and cross-expiry convergence study across ten strategy families.",
            "generatedAt": generated_at,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "sources": sources,
            "blocks": blocks,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "summary": summary_rows,
                "metrics": metric_rows,
                "daily": daily_rows,
                "ratios": ratio_rows,
                "latency": latency_rows,
                "series_scope": series_scope_rows,
                "cycles": cycle_rows,
                "opportunities": opportunity_rows,
                "evidence_layers": evidence_layer_rows,
            },
        },
        "sources": sources,
        "package_info": {
            "root": source_prefix,
            "manifestPath": "artifact.json",
            "snapshotPath": "artifact.json",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or args.input_dir / "artifact.json"
    artifact = build_artifact(args.input_dir)
    evidence_layers = pl.DataFrame(
        artifact["snapshot"]["datasets"]["evidence_layers"],
        infer_schema_length=None,
    )
    evidence_layers.write_parquet(args.input_dir / "evidence_layers.parquet")
    evidence_layers.write_csv(args.input_dir / "evidence_layers.csv")
    _atomic_json(output, artifact)
    print(json.dumps({"status": "complete", "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
