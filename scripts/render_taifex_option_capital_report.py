#!/usr/bin/env python3
"""Render capital-normalized daily curves and Sharpe for all TXO benchmarks."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Final

import matplotlib

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.ticker import PercentFormatter  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import _sha256_path  # noqa: E402
from stockagent.data.tw_index_derivatives_tick import _atomic_json  # noqa: E402
from stockagent.research.taifex_capital_returns import (  # noqa: E402
    TAIFEX_INITIAL_MARGIN_TWD,
    TAIFEX_MARGIN_ANNOUNCEMENT_URL,
    TAIFEX_MARGIN_CSV_SHA256,
    TAIFEX_MARGIN_CSV_URL,
    TAIFEX_MARGIN_FIRST_TRADING_DATE,
    TAIFEX_MARGIN_PDF_URL,
    TAIFEX_MARGIN_VERIFIED_THROUGH,
    build_capital_normalized_returns,
)


CLASSIC: Final[str] = "classic_opening_straddle"
FAMILY_LABELS: Final[dict[str, str]] = {
    "buy_hold_atm_straddle": "Classic ATM straddle",
    "atm_straddle_fixed_tp_sl": "Fixed TP/SL",
    "full_recenter_straddle": "Full re-center",
    "long_strangle": "Long strangle",
    "gamma_scalping": "Gamma scalping",
    "delta_band_gamma_scalping": "Delta-band gamma",
    "time_based_recenter": "Time re-center",
    "trailing_ratchet_roll": "Ratchet roll",
    "no_option_underlying": "Underlying only",
    "random_roll_control": "Random-roll control",
    "single_leg_roll_candidate": "Single-leg roll candidate",
}
CURVE_GROUPS: Final[tuple[tuple[str, str], ...]] = (
    ("classic", "Classic opening ATM straddle"),
    ("fixed_tp_sl", "ATM straddle with fixed TP/SL"),
    ("full_recenter", "Full re-center straddle: 50-1,000 points"),
    ("long_strangle", "Long strangle: 50-1,000 OTM points"),
    ("gamma", "Gamma scalping"),
    ("delta_band_gamma", "Delta-band gamma scalping"),
    ("time_recenter", "Time-based re-center"),
    ("ratchet", "Trailing / ratchet roll: 50-1,000 points"),
    ("underlying", "No-option underlying controls"),
    ("candidate_keep_otm", "Candidate: roll ITM call, keep OTM put"),
    ("candidate_keep_itm", "Candidate: roll OTM put, keep ITM call"),
    ("random_keep_otm", "Random control: matched keep-OTM roll counts"),
    ("random_keep_itm", "Random control: matched keep-ITM roll counts"),
)
PALETTE_ROOTS: Final[tuple[str, ...]] = (
    "#2F6F9F",  # blue
    "#C79322",  # gold
    "#D97732",  # orange
    "#778A35",  # olive
    "#B85C8A",  # pink
)
CONTEXT_LINE: Final[str] = "#A7B0B8"
TOP_LINE_STYLES: Final[tuple[str, ...]] = ("-", "--", "-.")
FAMILY_MARKERS: Final[tuple[str, ...]] = ("o", "s", "^", "D", "P", "X")


def curve_group_for_variant(*, family: str, variant_id: str) -> str:
    """Map every one of the 160 variants to exactly one readable curve panel."""

    direct = {
        "buy_hold_atm_straddle": "classic",
        "atm_straddle_fixed_tp_sl": "fixed_tp_sl",
        "full_recenter_straddle": "full_recenter",
        "long_strangle": "long_strangle",
        "gamma_scalping": "gamma",
        "delta_band_gamma_scalping": "delta_band_gamma",
        "time_based_recenter": "time_recenter",
        "trailing_ratchet_roll": "ratchet",
        "no_option_underlying": "underlying",
    }
    if family in direct:
        return direct[family]
    if family == "single_leg_roll_candidate":
        if variant_id.startswith("roll_itm_call_keep_otm_put__"):
            return "candidate_keep_otm"
        if variant_id.startswith("roll_otm_put_keep_itm_call__"):
            return "candidate_keep_itm"
    if family == "random_roll_control":
        if variant_id.startswith("random_control__roll_itm_call_keep_otm_put__"):
            return "random_keep_otm"
        if variant_id.startswith("random_control__roll_otm_put_keep_itm_call__"):
            return "random_keep_itm"
    raise ValueError(f"unmapped benchmark variant: family={family}, id={variant_id}")


def _variant_label(variant_id: str) -> str:
    replacements = (
        ("classic_opening_straddle", "Classic ATM"),
        ("fixed_tp_sl__tp", "TP"),
        ("_sl", "/SL"),
        ("full_recenter__", "Re-center "),
        ("long_strangle__", "Strangle "),
        ("gamma_scalping__", "Gamma "),
        ("delta_band_gamma__tmf__", "Delta band 0."),
        ("time_recenter__", "Every "),
        ("ratchet_roll__", "Ratchet "),
        ("underlying__", "Underlying "),
        ("random_control__roll_itm_call_keep_otm_put__", "Random keep OTM "),
        ("random_control__roll_otm_put_keep_itm_call__", "Random keep ITM "),
        ("roll_itm_call_keep_otm_put__", "Keep OTM "),
        ("roll_otm_put_keep_itm_call__", "Keep ITM "),
    )
    label = variant_id
    for source, target in replacements:
        label = label.replace(source, target)
    label = label.replace("__", " ").replace("_", " ")
    if label.endswith("m"):
        return label
    tokens = label.split()
    if tokens and tokens[-1].isdigit() and len(tokens[-1]) == 4:
        tokens[-1] = str(int(tokens[-1]))
    return " ".join(tokens)


def _pct(value: float) -> str:
    return f"{value * 100.0:+.2f}%"


def _twd(value: float) -> str:
    return f"{value:,.0f}"


def _markdown_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    header = "| " + " | ".join(label for _, label in columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row[field]) for field, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, separator, *body])


def _plot_curve_group(
    *,
    group_id: str,
    title: str,
    normalized_daily: pl.DataFrame,
    metrics: pl.DataFrame,
    output_path: Path,
    carry_mode: bool = False,
) -> dict[str, Any]:
    members = metrics.filter(pl.col("curve_group") == group_id).sort(
        "cumulative_return_on_capital", descending=True
    )
    if members.is_empty():
        raise ValueError(f"curve group has no members: {group_id}")
    variant_ids = members.get_column("variant_id").to_list()
    top_ids = set(variant_ids[:3])
    classic_frame = normalized_daily.filter(pl.col("variant_id") == CLASSIC).sort(
        "trading_date"
    )

    fig, axes = plt.subplots(2, 1, figsize=(16, 11.5), sharex=True)
    top_rank = {variant_id: rank for rank, variant_id in enumerate(variant_ids[:3])}
    for index, variant_id in enumerate(variant_ids):
        frame = normalized_daily.filter(pl.col("variant_id") == variant_id).sort(
            "trading_date"
        )
        dates = frame.get_column("trading_date").to_list()
        label = _variant_label(variant_id)
        is_top = variant_id in top_ids
        is_classic = variant_id == CLASSIC
        line_width = 2.4 if is_classic else (2.2 if is_top else 1.0)
        alpha = 0.95 if is_classic or is_top else 0.38
        color = (
            "#202124"
            if is_classic
            else (
                PALETTE_ROOTS[top_rank[variant_id]]
                if is_top
                else CONTEXT_LINE
            )
        )
        line_style = (
            "--"
            if is_classic
            else (
                TOP_LINE_STYLES[top_rank[variant_id]]
                if is_top
                else "-"
            )
        )
        if is_classic:
            label = "Classic ATM benchmark"
        axes[0].plot(
            dates,
            frame.get_column("cumulative_return_on_capital").to_numpy(),
            label=label,
            color=color,
            linewidth=line_width,
            alpha=alpha,
            linestyle=line_style,
        )
        axes[1].plot(
            dates,
            frame.get_column(
                "fixed_capital_drawdown_return"
                if carry_mode
                else "cumulative_compounded_return"
            ).to_numpy(),
            label=label,
            color=color,
            linewidth=line_width,
            alpha=alpha,
            linestyle=line_style,
        )

    if CLASSIC not in variant_ids:
        dates = classic_frame.get_column("trading_date").to_list()
        for axis, field in zip(
            axes,
            (
                "cumulative_return_on_capital",
                (
                    "fixed_capital_drawdown_return"
                    if carry_mode
                    else "cumulative_compounded_return"
                ),
            ),
        ):
            axis.plot(
                dates,
                classic_frame.get_column(field).to_numpy(),
                label="Classic ATM benchmark",
                color="black",
                linestyle="--",
                linewidth=2.4,
                alpha=0.95,
            )

    for axis in axes:
        axis.axhline(0.0, color="#666666", linewidth=0.8, alpha=0.7)
        axis.grid(True, axis="both", alpha=0.18)
        axis.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    axes[0].set_ylabel("Cumulative P&L / fixed capital")
    axes[1].set_ylabel(
        "Drawdown from marked-equity peak"
        if carry_mode
        else "Daily-compounded return"
    )
    axes[1].set_xlabel("Trading date")
    axes[0].set_title("Fixed one-lot capital return", loc="left", fontsize=11)
    axes[1].set_title(
        (
            "Fixed-capital marked-equity drawdown"
            if carry_mode
            else "Compounded statistical view (positions were not resized)"
        ),
        loc="left",
        fontsize=11,
    )
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="center left",
        bbox_to_anchor=(0.80, 0.5),
        frameon=False,
        fontsize=7.5,
        ncol=1,
    )
    fig.suptitle(title, x=0.06, y=0.985, ha="left", fontsize=16, fontweight="bold")
    fig.text(
        0.06,
        0.957,
        f"{members.height} variants; top three in this panel are emphasized; "
        f"{normalized_daily['trading_date'].min()} to {normalized_daily['trading_date'].max()}; "
        "fixed one-lot P&L / fixed capital",
        ha="left",
        fontsize=9,
        color="#444444",
    )
    fig.tight_layout(rect=(0.04, 0.035, 0.79, 0.94))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return {
        "group_id": group_id,
        "title": title,
        "variants": members.height,
        "best_variant": str(members.item(0, "variant_id")),
        "best_cumulative_return_on_capital": float(
            members.item(0, "cumulative_return_on_capital")
        ),
        "path": str(output_path),
        "sha256": _sha256_path(output_path),
    }


def _family_colors(families: list[str]) -> dict[str, Any]:
    return {
        family: PALETTE_ROOTS[index % len(PALETTE_ROOTS)]
        for index, family in enumerate(families)
    }


def _family_line_styles(families: list[str]) -> dict[str, str]:
    styles = ("-", "--", "-.", ":")
    return {family: styles[index % len(styles)] for index, family in enumerate(families)}


def _set_overview_header(fig: Any, *, title: str, subtitle: str) -> None:
    """Keep the overview title and audit subtitle in separate figure bands."""
    fig.suptitle(title, x=0.06, y=0.975, ha="left", fontsize=15, fontweight="bold")
    fig.text(0.06, 0.938, subtitle, ha="left", fontsize=9, color="#555555")


def _plot_family_best_curves(
    family_best: pl.DataFrame,
    normalized_daily: pl.DataFrame,
    output_path: Path,
) -> None:
    fig, axis = plt.subplots(figsize=(15, 7.5))
    families = family_best.get_column("benchmark_family").to_list()
    colors = _family_colors(families)
    line_styles = _family_line_styles(families)
    for row in family_best.iter_rows(named=True):
        variant_id = str(row["variant_id"])
        family = str(row["benchmark_family"])
        frame = normalized_daily.filter(pl.col("variant_id") == variant_id).sort(
            "trading_date"
        )
        axis.plot(
            frame.get_column("trading_date").to_list(),
            frame.get_column("cumulative_return_on_capital").to_numpy(),
            label=FAMILY_LABELS[family],
            linewidth=2.0,
            color=colors[family],
            linestyle=line_styles[family],
        )
    axis.axhline(0.0, color="#666666", linewidth=0.8)
    axis.grid(True, alpha=0.18)
    axis.yaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    axis.set_xlabel("Trading date")
    axis.set_ylabel("Cumulative P&L / fixed capital")
    _set_overview_header(
        fig,
        title="Capital-normalized daily curves: best in-sample variant per family",
        subtitle=(
            f"{normalized_daily['trading_date'].min()} to {normalized_daily['trading_date'].max()}; "
            "one selected variant per family; fixed one-lot P&L / fixed capital"
        ),
    )
    axis.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False)
    fig.tight_layout(rect=(0.04, 0.03, 0.98, 0.90))
    fig.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _plot_family_best_bar(family_best: pl.DataFrame, output_path: Path) -> None:
    ordered = family_best.sort("cumulative_return_on_capital")
    labels = [FAMILY_LABELS[str(value)] for value in ordered["benchmark_family"]]
    values = ordered["cumulative_return_on_capital"].to_numpy()
    colors = ["#2F6F9F" if value >= 0.0 else "#D97732" for value in values]
    fig, axis = plt.subplots(figsize=(13.5, 7.5))
    bars = axis.barh(labels, values, color=colors, alpha=0.9)
    axis.axvline(0.0, color="#444444", linewidth=0.8)
    axis.xaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    axis.grid(True, axis="x", alpha=0.18)
    axis.set_xlabel("30-day cumulative P&L / fixed capital (not annualized)")
    _set_overview_header(
        fig,
        title="Family-best return after required-capital normalization",
        subtitle="30 trading days; best in-sample variant per family; return is not annualized",
    )
    for bar, value in zip(bars, values):
        axis.text(
            value,
            bar.get_y() + bar.get_height() / 2.0,
            f" {_pct(float(value))}",
            va="center",
            ha="left" if value >= 0.0 else "right",
            fontsize=8,
        )
    fig.tight_layout(rect=(0.04, 0.03, 0.98, 0.90))
    fig.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _plot_return_sharpe(metrics: pl.DataFrame, output_path: Path) -> None:
    families = sorted(metrics.get_column("benchmark_family").unique().to_list())
    colors = _family_colors(families)
    markers = {
        family: FAMILY_MARKERS[index % len(FAMILY_MARKERS)]
        for index, family in enumerate(families)
    }
    fig, axis = plt.subplots(figsize=(13.5, 8.0))
    for family in families:
        frame = metrics.filter(pl.col("benchmark_family") == family)
        axis.scatter(
            frame.get_column("cumulative_return_on_capital").to_numpy(),
            frame.get_column("annualized_sharpe").to_numpy(),
            label=FAMILY_LABELS[family],
            color=colors[family],
            alpha=0.72,
            s=32,
            marker=markers[family],
        )
    classic = metrics.filter(pl.col("variant_id") == CLASSIC)
    axis.scatter(
        classic["cumulative_return_on_capital"],
        classic["annualized_sharpe"],
        marker="*",
        color="black",
        s=190,
        label="Classic benchmark",
        zorder=10,
    )
    axis.axvline(0.0, color="#666666", linewidth=0.8)
    axis.axhline(0.0, color="#666666", linewidth=0.8)
    axis.grid(True, alpha=0.18)
    axis.xaxis.set_major_formatter(PercentFormatter(xmax=1.0, decimals=0))
    axis.set_xlabel("Cumulative P&L / fixed capital (not annualized)")
    axis.set_ylabel("Annualized Sharpe from daily returns (x sqrt(252))")
    _set_overview_header(
        fig,
        title="All 160 variants: capital return versus Sharpe",
        subtitle=(
            "30 trading days; x = fixed-capital cumulative return; "
            "y = daily Sharpe x sqrt(252)"
        ),
    )
    axis.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False)
    fig.tight_layout(rect=(0.04, 0.03, 0.98, 0.90))
    fig.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _plot_capital_vs_pnl(metrics: pl.DataFrame, output_path: Path) -> None:
    families = sorted(metrics.get_column("benchmark_family").unique().to_list())
    colors = _family_colors(families)
    fig, axis = plt.subplots(figsize=(13.5, 8.0))
    for family in families:
        frame = metrics.filter(pl.col("benchmark_family") == family)
        axis.scatter(
            frame.get_column("capital_base_twd").to_numpy(),
            frame.get_column("total_net_after_fee_twd").to_numpy(),
            label=FAMILY_LABELS[family],
            color=colors[family],
            alpha=0.72,
            s=32,
        )
    axis.axhline(0.0, color="#666666", linewidth=0.8)
    axis.grid(True, alpha=0.18)
    axis.set_xlabel("Fixed capital base (TWD)")
    axis.set_ylabel("30-day one-lot net P&L after modeled costs (TWD)")
    _set_overview_header(
        fig,
        title="Why raw TWD P&L is not a fair ranking",
        subtitle="30 trading days; one-lot after-cost P&L versus fixed funding requirement",
    )
    axis.legend(loc="center left", bbox_to_anchor=(1.01, 0.5), frameon=False)
    fig.tight_layout(rect=(0.04, 0.03, 0.98, 0.90))
    fig.savefig(output_path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _select_family_best(metrics: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for frame in metrics.partition_by("benchmark_family", maintain_order=True):
        rows.append(
            frame.sort("cumulative_return_on_capital", descending=True).row(
                0, named=True
            )
        )
    return pl.DataFrame(rows).sort(
        "cumulative_return_on_capital", descending=True
    )


def _result_table_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(
        frame.sort("cumulative_return_on_capital", descending=True).iter_rows(
            named=True
        ),
        start=1,
    ):
        rows.append(
            {
                "rank": rank,
                "family": FAMILY_LABELS[str(row["benchmark_family"])],
                "variant": str(row["variant_id"]),
                "capital": _twd(float(row["capital_base_twd"])),
                "one_lot_return": _pct(float(row["cumulative_return_on_capital"])),
                "compounded": _pct(float(row["cumulative_compounded_return"])),
                "sharpe": f"{float(row['annualized_sharpe']):+.3f}",
                "max_dd": _pct(float(row["maximum_drawdown_compounded_return"])),
                "pnl": _twd(float(row["total_net_after_fee_twd"])),
                "fees": _twd(float(row["fixed_fees_twd"])),
                "taxes": _twd(float(row["transaction_tax_twd"])),
                "sides": int(row["total_trade_sides"]),
            }
        )
    return rows


def build_report(*, input_dir: Path, output_path: Path) -> dict[str, Any]:
    summary_path = input_dir / "summary.json"
    daily_path = input_dir / "daily_benchmarks.parquet"
    trades_path = input_dir / "trades.parquet"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("status") != "complete" or summary.get("variant_count") != 160:
        raise ValueError("the complete 160-variant benchmark summary is required")
    for key, source_path in (
        ("daily_benchmarks_sha256", daily_path),
        ("trades_sha256", trades_path),
    ):
        if _sha256_path(source_path) != summary["artifacts"][key]:
            raise ValueError(f"source artifact hash mismatch: {source_path}")

    daily = pl.read_parquet(daily_path)
    trades = pl.read_parquet(trades_path)
    carry_mode = (
        summary.get("execution_contract", {}).get("holding_policy")
        == "official_expiry_carry"
    )
    pnl_column = "net_pnl_twd" if carry_mode else "net_after_fee_twd"
    fee_comparison: dict[str, float] | None = None
    if carry_mode:
        prior_summary_path = input_dir.parent / "taifex_option_benchmarks" / "summary.json"
        if prior_summary_path.is_file():
            prior_summary = json.loads(prior_summary_path.read_text(encoding="utf-8"))
            if prior_summary.get("status") == "complete" and prior_summary.get("variant_count") == 160:
                prior_classic = next(
                    row
                    for row in prior_summary["results"]
                    if row["variant_id"] == CLASSIC
                )
                current_classic = next(
                    row for row in summary["results"] if row["variant_id"] == CLASSIC
                )
                prior_total = sum(
                    float(row["fixed_fees_twd"])
                    for row in prior_summary["results"]
                )
                current_total = sum(
                    float(row["fixed_fees_twd"]) for row in summary["results"]
                )
                fee_comparison = {
                    "prior_total_fixed_fees_twd": prior_total,
                    "current_total_fixed_fees_twd": current_total,
                    "total_fixed_fee_reduction_fraction": 1.0 - current_total / prior_total,
                    "prior_classic_fixed_fees_twd": float(prior_classic["fixed_fees_twd"]),
                    "current_classic_fixed_fees_twd": float(current_classic["fixed_fees_twd"]),
                    "classic_fixed_fee_reduction_fraction": (
                        1.0
                        - float(current_classic["fixed_fees_twd"])
                        / float(prior_classic["fixed_fees_twd"])
                    ),
                    "prior_summary_sha256": _sha256_path(prior_summary_path),
                }
    random_fallback_rows = trades.filter(
        pl.col("reason").cast(pl.Utf8).str.contains("random_fallback")
    )
    random_fallback_rolls = random_fallback_rows.height // 2
    random_fallback_variants = random_fallback_rows.select("variant_id").n_unique()
    official_daily_mark_legs = 0
    non_settlement_mark_legs = 0
    for raw in daily.get_column("diagnostics_json"):
        for source in json.loads(str(raw)).get("mark_sources", {}).values():
            if source == "official_daily_settlement":
                official_daily_mark_legs += 1
            else:
                non_settlement_mark_legs += 1
    normalized_daily, capital_metrics = build_capital_normalized_returns(
        daily,
        trades,
        carry_across_sessions=carry_mode,
        pnl_column=pnl_column,
    )
    result_lookup = pl.DataFrame(
        [
            {
                "variant_id": str(row["variant_id"]),
                "fixed_fees_twd": float(row["fixed_fees_twd"]),
                "transaction_tax_twd": float(row["transaction_tax_twd"]),
                "total_trade_sides": int(row["total_trade_sides"]),
                "total_rolls": int(row["total_rolls"]),
                "total_recenters": int(row["total_recenters"]),
                "total_hedges": int(row["total_hedges"]),
            }
            for row in summary["results"]
        ]
    )
    metrics = capital_metrics.join(
        result_lookup, on="variant_id", how="left", validate="1:1"
    )
    if metrics.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError("capital metrics did not fully join benchmark summary")
    for row in metrics.iter_rows(named=True):
        if abs(
            float(row["total_net_after_fee_twd"])
            - float(
                next(
                    result[pnl_column]
                    for result in summary["results"]
                    if result["variant_id"] == row["variant_id"]
                )
            )
        ) > 1e-6:
            raise ValueError(f"P&L reconciliation failed: {row['variant_id']}")

    group_rows = [
        {
            "variant_id": str(row["variant_id"]),
            "curve_group": curve_group_for_variant(
                family=str(row["benchmark_family"]),
                variant_id=str(row["variant_id"]),
            ),
        }
        for row in metrics.iter_rows(named=True)
    ]
    metrics = metrics.join(
        pl.DataFrame(group_rows), on="variant_id", how="left", validate="1:1"
    ).sort("cumulative_return_on_capital", descending=True)
    normalized_daily = normalized_daily.join(
        pl.DataFrame(group_rows), on="variant_id", how="left", validate="m:1"
    )
    expected_groups = {group_id for group_id, _ in CURVE_GROUPS}
    observed_groups = set(metrics.get_column("curve_group").to_list())
    if observed_groups != expected_groups:
        raise ValueError(
            f"curve groups are incomplete: expected={expected_groups}, observed={observed_groups}"
        )

    normalized_daily_path = input_dir / "capital_normalized_daily.parquet"
    normalized_daily_csv_path = input_dir / "capital_normalized_daily.csv"
    metrics_path = input_dir / "capital_normalized_metrics.csv"
    normalized_daily.write_parquet(normalized_daily_path, compression="zstd")
    normalized_daily.write_csv(normalized_daily_csv_path)
    metrics.write_csv(metrics_path)

    plots_dir = input_dir / "capital_return_plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    chart_receipts: list[dict[str, Any]] = []
    for group_id, title in CURVE_GROUPS:
        chart_receipts.append(
            _plot_curve_group(
                group_id=group_id,
                title=title,
                normalized_daily=normalized_daily,
                metrics=metrics,
                output_path=plots_dir / f"daily_curves_{group_id}.png",
                carry_mode=carry_mode,
            )
        )

    family_best = _select_family_best(metrics)
    overview_paths = {
        "family_best_curves": plots_dir / "overview_family_best_daily_curves.png",
        "family_best_bar": plots_dir / "overview_family_best_returns.png",
        "return_sharpe": plots_dir / "overview_return_vs_sharpe.png",
        "capital_pnl": plots_dir / "overview_capital_vs_raw_pnl.png",
    }
    _plot_family_best_curves(
        family_best, normalized_daily, overview_paths["family_best_curves"]
    )
    _plot_family_best_bar(family_best, overview_paths["family_best_bar"])
    _plot_return_sharpe(metrics, overview_paths["return_sharpe"])
    _plot_capital_vs_pnl(metrics, overview_paths["capital_pnl"])

    family_rows = _result_table_rows(family_best)
    all_rows = _result_table_rows(metrics)
    classic = metrics.filter(pl.col("variant_id") == CLASSIC).row(0, named=True)
    candidate = (
        metrics.filter(pl.col("benchmark_family") == "single_leg_roll_candidate")
        .sort("cumulative_return_on_capital", descending=True)
        .row(0, named=True)
    )
    best_random = (
        metrics.filter(pl.col("benchmark_family") == "random_roll_control")
        .sort("cumulative_return_on_capital", descending=True)
        .row(0, named=True)
    )
    overall = metrics.row(0, named=True)
    profitable_count = metrics.filter(
        pl.col("total_net_after_fee_twd") > 0.0
    ).height
    capital_exhausted_count = (
        normalized_daily.group_by("variant_id")
        .agg(pl.col("cumulative_return_on_capital").min().alias("minimum_return"))
        .filter(pl.col("minimum_return") < -1.0)
        .height
    )

    contract = {
        "version": 1,
        "scope": (
            "one-lot official-expiry carry benchmark with fees and statutory taxes; no stress add-ons"
            if carry_mode
            else "one-lot intraday fixed-fee benchmark; no stress add-ons"
        ),
        "capital_base": (
            "maximum observed funding requirement across the complete carried-position ledger"
            if carry_mode
            else "maximum observed daily peak required capital for each variant"
        ),
        "option_requirement": (
            "peak cumulative net option premium cash debit including fixed fees and statutory taxes; long-option positions only"
            if carry_mode
            else "peak cumulative net option premium cash debit including fixed fees; long-option positions only"
        ),
        "futures_requirement": "simultaneous absolute contracts times official TAIFEX initial margin, plus cumulative futures fixed fees",
        "mixed_requirement": "option cash requirement plus futures initial margin at each atomic whole-second event",
        "daily_return_on_capital": (
            "daily marked-to-settlement net TWD P&L after fees and statutory taxes divided by the fixed capital base"
            if carry_mode
            else "daily net-after-fixed-fee TWD P&L divided by the variant fixed capital base"
        ),
        "cumulative_return_on_capital": "cumulative one-lot TWD P&L divided by the same fixed capital base; not annualized",
        "cumulative_compounded_return": (
            "compatibility alias of fixed-capital marked-equity return; daily fixed-denominator compounding is intentionally disabled for carried positions"
            if carry_mode
            else "product(1 + daily return) - 1; statistical view only, because actual positions remain one lot"
        ),
        "annualized_sharpe": "mean daily return / sample daily standard deviation * sqrt(252), zero risk-free rate",
        "futures_initial_margin_twd": TAIFEX_INITIAL_MARGIN_TWD,
        "margin_first_trading_date": TAIFEX_MARGIN_FIRST_TRADING_DATE.isoformat(),
        "margin_verified_through": TAIFEX_MARGIN_VERIFIED_THROUGH.isoformat(),
        "official_sources": {
            "announcement": TAIFEX_MARGIN_ANNOUNCEMENT_URL,
            "pdf": TAIFEX_MARGIN_PDF_URL,
            "csv": TAIFEX_MARGIN_CSV_URL,
            "csv_sha256": TAIFEX_MARGIN_CSV_SHA256,
        },
    }
    contract_path = input_dir / "capital_normalization_contract.json"
    _atomic_json(contract_path, contract)

    family_columns = [
            ("rank", "Rank"),
            ("family", "Benchmark family"),
            ("variant", "Best variant"),
            ("capital", "Capital TWD"),
            ("one_lot_return", "30d return"),
            ("sharpe", "Sharpe x sqrt(252)"),
            ("max_dd", "Max DD"),
            ("pnl", "P&L TWD"),
    ]
    complete_columns = [
            ("rank", "Rank"),
            ("family", "Family"),
            ("variant", "Variant"),
            ("capital", "Capital TWD"),
            ("one_lot_return", "30d return"),
            ("sharpe", "Sharpe x sqrt(252)"),
            ("max_dd", "Max DD"),
            ("pnl", "P&L TWD"),
            ("fees", "Fees TWD"),
            ("taxes", "Taxes TWD"),
            ("sides", "Sides"),
    ]
    if not carry_mode:
        family_columns.insert(5, ("compounded", "Compounded"))
        complete_columns.insert(5, ("compounded", "Compounded"))
    family_table = _markdown_table(family_rows, family_columns)
    complete_table = _markdown_table(all_rows, complete_columns)
    chart_sections: list[str] = []
    for receipt in chart_receipts:
        relative_path = Path(receipt["path"]).relative_to(input_dir)
        chart_sections.append(
            f"## {receipt['title']}\n\n"
            f"這張圖包含 {receipt['variants']} 個變體；上半部是固定一口的資金報酬，"
            + (
                "下半部是固定資金權益高點回撤。"
                if carry_mode
                else "下半部是每日複利統計視圖。"
            )
            + "黑色虛線是經典 ATM straddle。\n\n"
            f"![{receipt['title']}]({relative_path.as_posix()})"
        )

    classic_secondary = (
        ""
        if carry_mode
        else f"，複利統計值 `{_pct(float(classic['cumulative_compounded_return']))}`"
    )
    return_definition_secondary = (
        "2. `Max DD`：固定資金權益曲線相對先前高點的回撤；跨日部位不做不合理的每日固定分母複利。"
        if carry_mode
        else "2. `Compounded`：把每日百分比做 `product(1+r)-1`；只是統計視圖，實際回測沒有隨權益增減口數。"
    )
    fee_optimization_bullet = ""
    if fee_comparison is not None:
        fee_optimization_bullet = (
            "- 相對舊的每日平倉／重開帳本，經典 ATM 的固定手續費從 "
            f"`{_twd(fee_comparison['prior_classic_fixed_fees_twd'])}` 降到 "
            f"`{_twd(fee_comparison['current_classic_fixed_fees_twd'])}` TWD（"
            f"`{fee_comparison['classic_fixed_fee_reduction_fraction'] * 100.0:.1f}%`）；"
            "160 組合計固定費下降 "
            f"`{fee_comparison['total_fixed_fee_reduction_fraction'] * 100.0:.1f}%`。"
            "這是持倉週期改變的描述，不把損益差異歸因為只有手續費。\n"
        )
    daily_file_description = (
        "160 組 × 30 日的每日損益、資金需求、固定資金累積報酬與回撤。"
        if carry_mode
        else "160 組 × 30 日的每日損益、資金需求與兩種累積曲線。"
    )
    metrics_file_description = (
        "全部 160 組的資金、30 日固定資金報酬、Sharpe 與最大回撤。"
        if carry_mode
        else "全部 160 組的資金、30 日報酬、複利報酬、Sharpe 與最大回撤。"
    )

    report = f"""# 台指選擇權逐筆策略：資金正規化每日曲線與 Sharpe

## 先看結論

- 這裡沒有年化報酬率。主要排名是 **30 日累積一口損益 / 各策略固定資金基準**。
- Sharpe 才依日報酬乘上 `sqrt(252)`，表格明確標成 `Sharpe x sqrt(252)`。
- 經典 ATM straddle：資金 `{_twd(float(classic['capital_base_twd']))}` TWD，30 日報酬 `{_pct(float(classic['cumulative_return_on_capital']))}`{classic_secondary}，Sharpe `{float(classic['annualized_sharpe']):+.3f}`。
- 最佳單腿 rolling：`{candidate['variant_id']}`，資金 `{_twd(float(candidate['capital_base_twd']))}` TWD，30 日報酬 `{_pct(float(candidate['cumulative_return_on_capital']))}`，Sharpe `{float(candidate['annualized_sharpe']):+.3f}`。
- 全部 160 組中，`{profitable_count}` 組淨損益為正、`{160 - profitable_count}` 組為負；按資金報酬排名第一是 `{overall['variant_id']}`，30 日報酬 `{_pct(float(overall['cumulative_return_on_capital']))}`。
- 第一名的原始淨利 `{_twd(float(overall['total_net_after_fee_twd']))}` TWD 低於經典 ATM 的 `{_twd(float(classic['total_net_after_fee_twd']))}` TWD；它的資金報酬較高，主要是固定資金分母只有 `{_twd(float(overall['capital_base_twd']))}` TWD。這正是本次要求做資金正規化後才會看見的差異。
- 最佳 matched-count random control 為 `{best_random['variant_id']}`，報酬 `{_pct(float(best_random['cumulative_return_on_capital']))}`；它高於最佳單腿 rolling，因此這 30 日樣本**沒有證明指定 rolling trigger 本身具有優勢**。
- 以上仍是同一個月內事後比較，不是樣本外營利保證。
{fee_optimization_bullet}

## 公平資金分母

- 每個變體使用樣本期間 **最高資金需求** 當固定分母，不因某天保證金較低就放大報酬。
- 買方選擇權：盤中累積淨權利金現金支出的最高值，包含每口每邊 22 TWD 固定費。
- 期貨：同時持有口數乘以官方原始保證金，TX `{_twd(TAIFEX_INITIAL_MARGIN_TWD['TX'])}`、MTX `{_twd(TAIFEX_INITIAL_MARGIN_TWD['MTX'])}`、TMF `{_twd(TAIFEX_INITIAL_MARGIN_TWD['TMF'])}` TWD，再加累積固定費。
- Gamma：在每個逐筆事件同時計算選擇權現金需求與期貨保證金。
- 同一整秒的成交視為原子事件；買方策略不得出現裸賣部位，跨日模式只要求官方到期後與樣本結尾歸零。
- 本輪已逐筆計入選擇權權利金交易稅、指數期貨交易稅與到期現金結算稅；依要求沒有壓力滑價、深度、等待上限或額外保證金 buffer。
- 160 組都完成 12 個到期週期；跨日部位共使用 `{official_daily_mark_legs:,}` 個官方每日結算腿次估值，非結算價 fallback 為 `{non_settlement_mark_legs}`。

## 兩條報酬定義不要混用

1. `30d return`：固定一口每日損益累加後除以固定資金；這是最貼近本次一口測試的公平比較。
{return_definition_secondary}

## 每個 benchmark family 的最佳值

{family_table}

## Overview：各 family 最佳每日曲線

每條線仍是固定一口的累積資金報酬，方便先看路徑是否只靠少數幾天拉開。

![Family best daily curves]({overview_paths['family_best_curves'].relative_to(input_dir).as_posix()})

## Overview：資金正規化後排名

![Family best capital returns]({overview_paths['family_best_bar'].relative_to(input_dir).as_posix()})

## Overview：全部 160 組的報酬與 Sharpe

這張圖的橫軸不是年化報酬；只有縱軸 Sharpe 使用 `sqrt(252)`。

![Return versus Sharpe]({overview_paths['return_sharpe'].relative_to(input_dir).as_posix()})

## Overview：為什麼不能只比台幣損益

高保證金或高權利金策略可能自然產生較大的台幣損益，這張圖直接畫出資金需求與原始損益的關係。

![Capital versus raw P&L]({overview_paths['capital_pnl'].relative_to(input_dir).as_posix()})

{"\n\n".join(chart_sections)}

## 全部 160 組結果

以下完整列出每一組的固定資金、非年化 30 日報酬、Sharpe 與最大回撤。每日 4,800 筆明細另存於 `capital_normalized_daily.csv`。

{complete_table}

## 可重現檔案

- `capital_normalized_daily.parquet` / `.csv`：{daily_file_description}
- `capital_normalized_metrics.csv`：{metrics_file_description}
- `capital_normalization_contract.json`：資金公式、保證金版本與官方來源。
- `capital_return_plots/`：13 組完整每日曲線與 4 張 overview 圖。

## 限制

歷史資料沒有 Bid/Ask；非到期成交仍採一口可由下一筆實際成交價保證成交的代理。跨日部位以官方每日結算價估值，到期以官方最終結算價清算，估值不是成交。Random control 為了維持同一到期週的相同 roll 次數，有 `{random_fallback_rolls}` 次（`{random_fallback_rows.height}` 筆成交列、`{random_fallback_variants}` 個 variant）在隨機時點 ATM 無法切換時，改選當時可觀察且下一筆可成交的同到期／同權別鄰近履約價；它是研究 control，不是可直接部署的策略。以官方原始保證金作固定分母時，有 `{capital_exhausted_count}` 組的盤中／跨日累積損失曾超過該分母，實務上會需要追繳或更多資金；依要求本輪沒有自行加入強制平倉或壓力 buffer。報告只驗證這 30 個交易日內的相對結果，160 組參數存在明顯事後挑選風險。依要求，本輪沒有加入壓力測試；若資金正規化後仍有營利，再逐步增加真實限制。
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")

    receipt = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "report": str(output_path),
        "report_sha256": _sha256_path(output_path),
        "normalized_daily_rows": normalized_daily.height,
        "variant_metrics_rows": metrics.height,
        "curve_groups": len(chart_receipts),
        "overview_charts": len(overview_paths),
        "source_hashes": {
            "summary": _sha256_path(summary_path),
            "daily": _sha256_path(daily_path),
            "trades": _sha256_path(trades_path),
        },
        "output_hashes": {
            "normalized_daily_parquet": _sha256_path(normalized_daily_path),
            "normalized_daily_csv": _sha256_path(normalized_daily_csv_path),
            "metrics_csv": _sha256_path(metrics_path),
            "contract": _sha256_path(contract_path),
        },
        "chart_receipts": chart_receipts,
        "overview_chart_hashes": {
            key: _sha256_path(path) for key, path in overview_paths.items()
        },
        "validation": {
            "all_160_variants_present": metrics.height == 160,
            "all_4800_daily_rows_present": normalized_daily.height == 4_800,
            "all_variants_flat_and_long_option_only": True,
            "carry_mode": carry_mode,
            "pnl_column": pnl_column,
            "no_annualized_return_reported": True,
            "annualized_sharpe_only": True,
            "stress_tests_added": False,
            "official_daily_mark_legs": official_daily_mark_legs,
            "non_settlement_mark_legs": non_settlement_mark_legs,
            "random_fallback_rolls": random_fallback_rolls,
        },
        "fee_comparison": fee_comparison,
    }
    _atomic_json(input_dir / "capital_report_receipt.json", receipt)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("artifacts/research/taifex_option_benchmarks"),
    )
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = args.output or (args.input_dir / "capital_normalized_report.md")
    receipt = build_report(input_dir=args.input_dir, output_path=output)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
