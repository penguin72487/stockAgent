#!/usr/bin/env python3
"""Render the 2012-2026 daily TXO volatility-model study to Markdown/PNG."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys
from typing import Any, Final

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import polars as pl  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import _sha256_path  # noqa: E402
from scripts.backtest_taifex_volatility_model_gamma_daily_history import (  # noqa: E402
    CLASSIC_VARIANT_ID,
    DEFAULT_OUTPUT_DIR,
    MODEL_VARIANT_PREFIX,
)
from stockagent.data.tw_index_derivatives_tick import _atomic_json  # noqa: E402
from stockagent.research.taifex_volatility_models import (  # noqa: E402
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_LOCAL_VOL,
    MODEL_ROUGH_VOL,
    MODEL_SABR,
    MODEL_SLV,
)


MODEL_ORDER: Final[tuple[str, ...]] = (
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_SABR,
    MODEL_LOCAL_VOL,
    MODEL_SLV,
    MODEL_ROUGH_VOL,
)
VARIANT_ORDER: Final[tuple[str, ...]] = (
    CLASSIC_VARIANT_ID,
    *(f"{MODEL_VARIANT_PREFIX}{model}" for model in MODEL_ORDER),
)
SHORT_LABELS: Final[dict[str, str]] = {
    CLASSIC_VARIANT_ID: "Classic straddle",
    f"{MODEL_VARIANT_PREFIX}{MODEL_BLACK_SCHOLES}": "Black-Scholes",
    f"{MODEL_VARIANT_PREFIX}{MODEL_HESTON_SVI}": "Heston-SVI proxy",
    f"{MODEL_VARIANT_PREFIX}{MODEL_SABR}": "SABR beta=1",
    f"{MODEL_VARIANT_PREFIX}{MODEL_LOCAL_VOL}": "Local Vol proxy",
    f"{MODEL_VARIANT_PREFIX}{MODEL_SLV}": "SLV proxy",
    f"{MODEL_VARIANT_PREFIX}{MODEL_ROUGH_VOL}": "Rough Vol proxy",
}
COLORS: Final[dict[str, str]] = {
    CLASSIC_VARIANT_ID: "#111827",
    f"{MODEL_VARIANT_PREFIX}{MODEL_BLACK_SCHOLES}": "#6b7280",
    f"{MODEL_VARIANT_PREFIX}{MODEL_HESTON_SVI}": "#2563eb",
    f"{MODEL_VARIANT_PREFIX}{MODEL_SABR}": "#f59e0b",
    f"{MODEL_VARIANT_PREFIX}{MODEL_LOCAL_VOL}": "#10b981",
    f"{MODEL_VARIANT_PREFIX}{MODEL_SLV}": "#8b5cf6",
    f"{MODEL_VARIANT_PREFIX}{MODEL_ROUGH_VOL}": "#ef4444",
}


def _money(value: object) -> str:
    return f"{float(value):,.0f}"


def _percent(value: object) -> str:
    return f"{float(value) * 100:.2f}%"


def _number(value: object, digits: int = 2) -> str:
    if value is None or not math.isfinite(float(value)):
        return "—"
    return f"{float(value):.{digits}f}"


def _save_figure(figure: plt.Figure, path: Path) -> None:
    figure.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    if path.read_bytes()[:8] != b"\x89PNG\r\n\x1a\n":
        raise ValueError(f"invalid PNG signature: {path}")


def _curve_plot(daily: pl.DataFrame, output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(12.2, 6.5))
    by_variant = {
        str(frame.item(0, "variant_id")): frame.sort("trading_date")
        for frame in daily.partition_by("variant_id", maintain_order=True)
    }
    for variant_id in VARIANT_ORDER:
        frame = by_variant[variant_id]
        axis.plot(
            frame.get_column("trading_date").to_list(),
            frame.get_column("cumulative_return_on_common_capital").to_numpy()
            * 100.0,
            label=SHORT_LABELS[variant_id],
            color=COLORS[variant_id],
            linewidth=2.6 if variant_id == CLASSIC_VARIANT_ID else 1.65,
        )
    axis.axhline(0.0, color="#9ca3af", linewidth=0.8)
    axis.set_title("Daily cumulative return on common fully cash-secured capital")
    axis.set_xlabel("Trading date")
    axis.set_ylabel("Non-annualized cumulative return (%)")
    axis.xaxis.set_major_locator(mdates.YearLocator(2))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axis.grid(alpha=0.2)
    axis.legend(loc="upper left", fontsize=8.4, ncol=2)
    _save_figure(figure, output_path)


def _small_multiples(daily: pl.DataFrame, output_path: Path) -> None:
    lookup = {
        str(frame.item(0, "variant_id")): frame.sort("trading_date")
        for frame in daily.partition_by("variant_id", maintain_order=True)
    }
    figure, axes = plt.subplots(4, 2, figsize=(13.0, 14.5), sharex=True)
    flat_axes = axes.flatten()
    for axis, variant_id in zip(flat_axes, VARIANT_ORDER, strict=False):
        frame = lookup[variant_id]
        values = (
            frame.get_column("cumulative_return_on_common_capital").to_numpy()
            * 100.0
        )
        axis.plot(
            frame.get_column("trading_date").to_list(),
            values,
            color=COLORS[variant_id],
            linewidth=1.5,
        )
        axis.fill_between(
            frame.get_column("trading_date").to_list(),
            0.0,
            values,
            color=COLORS[variant_id],
            alpha=0.1,
        )
        axis.axhline(0.0, color="#9ca3af", linewidth=0.6)
        axis.set_title(SHORT_LABELS[variant_id], fontsize=10)
        axis.set_ylabel("Cum. return (%)", fontsize=8)
        axis.grid(alpha=0.18)
        axis.xaxis.set_major_locator(mdates.YearLocator(3))
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    flat_axes[-1].axis("off")
    figure.suptitle("One daily curve per strategy", fontsize=14, y=0.995)
    figure.tight_layout()
    _save_figure(figure, output_path)


def _drawdown_plot(daily: pl.DataFrame, output_path: Path) -> None:
    figure, axis = plt.subplots(figsize=(12.2, 6.2))
    lookup = {
        str(frame.item(0, "variant_id")): frame.sort("trading_date")
        for frame in daily.partition_by("variant_id", maintain_order=True)
    }
    for variant_id in VARIANT_ORDER:
        frame = lookup[variant_id]
        axis.plot(
            frame.get_column("trading_date").to_list(),
            frame.get_column("fixed_capital_drawdown").to_numpy() * 100.0,
            label=SHORT_LABELS[variant_id],
            color=COLORS[variant_id],
            linewidth=2.2 if variant_id == CLASSIC_VARIANT_ID else 1.45,
        )
    axis.set_title("Daily drawdown on common fixed capital")
    axis.set_xlabel("Trading date")
    axis.set_ylabel("Drawdown (%)")
    axis.xaxis.set_major_locator(mdates.YearLocator(2))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axis.grid(alpha=0.2)
    axis.legend(loc="lower left", fontsize=8.2, ncol=2)
    _save_figure(figure, output_path)


def _pnl_cost_plot(metrics: pl.DataFrame, output_path: Path) -> None:
    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    labels = [SHORT_LABELS[value] for value in VARIANT_ORDER]
    gross = np.asarray([float(lookup[value]["gross_pnl_twd"]) for value in VARIANT_ORDER])
    fees = np.asarray([float(lookup[value]["fixed_fees_twd"]) for value in VARIANT_ORDER])
    taxes = np.asarray(
        [float(lookup[value]["transaction_tax_twd"]) for value in VARIANT_ORDER]
    )
    positions = np.arange(len(labels))
    figure, axis = plt.subplots(figsize=(12.0, 6.5))
    axis.bar(positions, gross / 1_000.0, label="Gross P&L", color="#60a5fa")
    axis.bar(
        positions,
        -fees / 1_000.0,
        label="Fixed fees",
        color="#f59e0b",
    )
    axis.bar(
        positions,
        -taxes / 1_000.0,
        bottom=-fees / 1_000.0,
        label="Transaction tax",
        color="#ef4444",
    )
    axis.axhline(0.0, color="#111827", linewidth=0.8)
    axis.set_title("One-lot gross P&L and explicit costs")
    axis.set_ylabel("TWD thousands")
    axis.set_xticks(positions, labels, rotation=22, ha="right")
    axis.grid(axis="y", alpha=0.2)
    axis.legend()
    _save_figure(figure, output_path)


def _annual_heatmap(annual: pl.DataFrame, output_path: Path) -> None:
    years = sorted(annual.get_column("year").unique().to_list())
    lookup = {
        (int(row["year"]), str(row["variant_id"])): float(
            row["return_on_common_capital"]
        )
        for row in annual.iter_rows(named=True)
    }
    values = np.asarray(
        [[lookup[(year, variant)] * 100.0 for year in years] for variant in VARIANT_ORDER]
    )
    limit = float(np.quantile(np.abs(values), 0.95))
    figure, axis = plt.subplots(figsize=(14.2, 5.5))
    image = axis.imshow(
        values,
        aspect="auto",
        cmap="RdYlGn",
        vmin=-limit,
        vmax=limit,
    )
    axis.set_xticks(np.arange(len(years)), years, rotation=45, ha="right")
    axis.set_yticks(
        np.arange(len(VARIANT_ORDER)),
        [SHORT_LABELS[value] for value in VARIANT_ORDER],
    )
    axis.set_title("Calendar-year return on common capital (2012 and 2026 are partial)")
    for row in range(values.shape[0]):
        for column in range(values.shape[1]):
            axis.text(
                column,
                row,
                f"{values[row, column]:.1f}",
                ha="center",
                va="center",
                fontsize=6.7,
                color="#111827",
            )
    colorbar = figure.colorbar(image, ax=axis, shrink=0.85)
    colorbar.set_label("Return (%)")
    _save_figure(figure, output_path)


def _calibration_plot(calibrations: pl.DataFrame, output_path: Path) -> None:
    successful = calibrations.filter(pl.col("status") == "success")
    summary = {
        str(row["volatility_model"]): row
        for row in successful.group_by("volatility_model").agg(
            pl.col("calibration_rmse_iv").mean().alias("rmse"),
            pl.col("parameter_at_bound").sum().alias("bounds"),
            pl.len().alias("attempts"),
        ).iter_rows(named=True)
    }
    labels = [SHORT_LABELS[f"{MODEL_VARIANT_PREFIX}{model}"] for model in MODEL_ORDER]
    rmse = np.asarray([float(summary[model]["rmse"]) * 100.0 for model in MODEL_ORDER])
    bound_rates = np.asarray(
        [
            float(summary[model]["bounds"]) / float(summary[model]["attempts"]) * 100.0
            for model in MODEL_ORDER
        ]
    )
    positions = np.arange(len(labels))
    figure, axis = plt.subplots(figsize=(11.8, 6.4))
    bars = axis.bar(positions, rmse, color="#2563eb", alpha=0.82)
    axis.set_ylabel("Mean in-sample IV RMSE (vol points x 100)", color="#1d4ed8")
    axis.set_xticks(positions, labels, rotation=22, ha="right")
    axis.grid(axis="y", alpha=0.2)
    secondary = axis.twinx()
    secondary.plot(
        positions,
        bound_rates,
        marker="o",
        color="#dc2626",
        linewidth=2.0,
        label="Parameter-bound rate",
    )
    secondary.set_ylabel("Parameter-bound rate (%)", color="#dc2626")
    for bar, rate in zip(bars, bound_rates, strict=True):
        if rate > 0.0:
            secondary.text(
                bar.get_x() + bar.get_width() / 2,
                rate + 1.5,
                f"{rate:.1f}%",
                ha="center",
                va="bottom",
                fontsize=8,
                color="#991b1b",
            )
    axis.set_title("Daily calibration fit and parameter-bound diagnostics")
    _save_figure(figure, output_path)


def _result_table(metrics: pl.DataFrame) -> str:
    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    lines = [
        "| 策略 / 模型 | 淨利 TWD | 非年化累積報酬 | Sharpe | Sortino | MDD | Calmar |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for variant_id in VARIANT_ORDER:
        row = lookup[variant_id]
        lines.append(
            f"| {SHORT_LABELS[variant_id]} | {_money(row['net_pnl_twd'])} | "
            f"{_percent(row['cumulative_return_on_common_capital'])} | "
            f"{_number(row['annualized_sharpe'])} | "
            f"{_number(row['annualized_sortino'])} | "
            f"{_percent(row['maximum_drawdown'])} | "
            f"{_number(row['sample_calmar_nonannualized'])} |"
        )
    return "\n".join(lines)


def _cost_table(metrics: pl.DataFrame) -> str:
    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    lines = [
        "| 策略 / 模型 | 毛損益 | 固定手續費 | 交易稅 | 成本後淨利 | MTX 口數側 | 避險事件 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for variant_id in VARIANT_ORDER:
        row = lookup[variant_id]
        lines.append(
            f"| {SHORT_LABELS[variant_id]} | {_money(row['gross_pnl_twd'])} | "
            f"{_money(row['fixed_fees_twd'])} | "
            f"{_money(row['transaction_tax_twd'])} | "
            f"{_money(row['net_pnl_twd'])} | "
            f"{int(row['total_futures_trade_sides']):,} | "
            f"{int(row['total_hedge_events']):,} |"
        )
    return "\n".join(lines)


def _calibration_table(calibrations: pl.DataFrame) -> str:
    summary = calibrations.group_by("volatility_model").agg(
        pl.len().alias("attempts"),
        (pl.col("status") == "success").sum().alias("successes"),
        pl.col("calibration_rmse_iv").mean().alias("rmse"),
        pl.col("held_series_points").min().alias("held_min"),
        pl.col("held_series_points").median().alias("held_median"),
        pl.col("surface_points_total").median().alias("surface_median"),
        pl.col("parameter_at_bound").sum().alias("bounds"),
    )
    lookup = {str(row["volatility_model"]): row for row in summary.iter_rows(named=True)}
    lines = [
        "| 模型 | 成功校準 | 平均 IV RMSE | Held 點 min / median | 全 surface median | 邊界命中 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for model in MODEL_ORDER:
        row = lookup[model]
        attempts = int(row["attempts"])
        bounds = int(row["bounds"])
        lines.append(
            f"| {SHORT_LABELS[f'{MODEL_VARIANT_PREFIX}{model}']} | "
            f"{int(row['successes']):,}/{attempts:,} | {float(row['rmse']):.4f} | "
            f"{int(row['held_min'])} / {float(row['held_median']):.0f} | "
            f"{float(row['surface_median']):.0f} | "
            f"{bounds:,} ({bounds / attempts * 100:.1f}%) |"
        )
    return "\n".join(lines)


def _annual_table(annual: pl.DataFrame) -> str:
    lookup = {
        (int(row["year"]), str(row["variant_id"])): float(
            row["return_on_common_capital"]
        )
        for row in annual.iter_rows(named=True)
    }
    years = sorted(annual.get_column("year").unique().to_list())
    headers = ["年度", *[SHORT_LABELS[value] for value in VARIANT_ORDER]]
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "---:|" * len(headers),
    ]
    for year in years:
        values = [str(year)] + [
            _percent(lookup[(int(year), variant)]) for variant in VARIANT_ORDER
        ]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def render_report(input_dir: Path, output_path: Path | None = None) -> Path:
    summary = json.loads((input_dir / "summary.json").read_text(encoding="utf-8"))
    receipt = json.loads((input_dir / "receipt.json").read_text(encoding="utf-8"))
    if summary.get("status") != "complete" or receipt.get("status") != "complete":
        raise ValueError("backtest summary/receipt is not complete")
    daily = pl.read_parquet(input_dir / "daily_results.parquet")
    metrics = pl.read_csv(input_dir / "metrics.csv")
    annual = pl.read_parquet(input_dir / "annual_results.parquet")
    calibrations = pl.read_parquet(input_dir / "calibrations.parquet")
    plots_dir = input_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    charts = {
        "all_curves": plots_dir / "daily_cumulative_return_all.png",
        "small_multiples": plots_dir / "daily_curve_each_strategy.png",
        "drawdowns": plots_dir / "daily_drawdown_all.png",
        "pnl_costs": plots_dir / "gross_pnl_and_costs.png",
        "annual_heatmap": plots_dir / "annual_return_heatmap.png",
        "calibration": plots_dir / "calibration_diagnostics.png",
    }
    _curve_plot(daily, charts["all_curves"])
    _small_multiples(daily, charts["small_multiples"])
    _drawdown_plot(daily, charts["drawdowns"])
    _pnl_cost_plot(metrics, charts["pnl_costs"])
    _annual_heatmap(annual, charts["annual_heatmap"])
    _calibration_plot(calibrations, charts["calibration"])

    metric_lookup = {
        str(row["variant_id"]): row for row in metrics.iter_rows(named=True)
    }
    classic = metric_lookup[CLASSIC_VARIANT_ID]
    local = metric_lookup[f"{MODEL_VARIANT_PREFIX}{MODEL_LOCAL_VOL}"]
    sabr = metric_lookup[f"{MODEL_VARIANT_PREFIX}{MODEL_SABR}"]
    model_rows = [
        metric_lookup[f"{MODEL_VARIANT_PREFIX}{model}"] for model in MODEL_ORDER
    ]
    profitable = sum(float(row["net_pnl_twd"]) > 0.0 for row in model_rows)
    beat_classic = sum(
        float(row["net_pnl_twd"]) > float(classic["net_pnl_twd"])
        for row in model_rows
    )
    annual_classic = annual.filter(pl.col("variant_id") == CLASSIC_VARIANT_ID)
    classic_2026 = float(
        annual_classic.filter(pl.col("year") == 2026).item(0, "net_pnl_twd")
    )
    local_2026 = float(
        annual.filter(
            (pl.col("variant_id") == f"{MODEL_VARIANT_PREFIX}{MODEL_LOCAL_VOL}")
            & (pl.col("year") == 2026)
        ).item(0, "net_pnl_twd")
    )
    relative = lambda path: path.relative_to(input_dir).as_posix()
    report_path = output_path or (input_dir / "volatility_model_daily_history_report.md")
    markdown = f"""# 台指選擇權六種定價／波動模型：2012-11-21～2026-08-07 日頻回測

## 結論先講

在 **3,348 個交易日、599 個完整週選到期循環**中，六種模型策略扣除手續費與交易稅後全部正獲利（{profitable}/6）；但只有 **Local Vol proxy** 與 **SABR beta=1** 的全期淨利高於完全不避險的經典開盤 ATM straddle（{beat_classic}/6）。

本次最佳是 **Local Vol proxy**：成本後淨利 **TWD {_money(local['net_pnl_twd'])}**、共同資本非年化累積報酬 **{_percent(local['cumulative_return_on_common_capital'])}**、Sharpe **{_number(local['annualized_sharpe'])}**、Sortino **{_number(local['annualized_sortino'])}**、MDD **{_percent(local['maximum_drawdown'])}**。經典 straddle 為淨利 **TWD {_money(classic['net_pnl_twd'])}**、報酬 **{_percent(classic['cumulative_return_on_common_capital'])}**、Sharpe **{_number(classic['annualized_sharpe'])}**、MDD **{_percent(classic['maximum_drawdown'])}**。Local 比經典多 **TWD {_money(float(local['net_pnl_twd']) - float(classic['net_pnl_twd']))}**，SABR 多 **TWD {_money(float(sabr['net_pnl_twd']) - float(classic['net_pnl_twd']))}**。

這確認了「成本後仍有正值」，但還不能直接解讀成可實盤 alpha：歷史日檔沒有同步 Bid/Ask，期權進場是各腿日內第一筆成交代理，模型在 D 日收盤資料完成後校準、D+1 日 MTX 開盤才執行；到期／轉倉平倉則是 MTX 日收盤終端代理。這是嚴格避免同日收盤前視的長期篩選層，不是逐筆可成交價結果。

![全部策略每日累積報酬]({relative(charts['all_curves'])})

## 每一個策略的每日曲線

所有策略使用同一個 **TWD {_money(summary['common_fully_cash_secured_capital_twd'])}** 分母：期間內最大一口 MTX 全額名目本金，加最大選擇權進場權利金 TWD {_money(summary['maximum_opening_option_premium_twd'])}，再向上取整千元。這不是歷史保證金回推，也沒有讓獲利較高者使用較小分母。

![每個策略獨立每日曲線]({relative(charts['small_multiples'])})

{_result_table(metrics)}

Sharpe、Sortino 用每日共同資本報酬年化；累積報酬與 Calmar 分子都保留為 **本樣本非年化**，沒有把 13.7 年結果偽裝成單年報酬。

![每日回撤]({relative(charts['drawdowns'])})

## 手續費、稅與毛損益

TXO 單向每口 TWD 22、MTX 單向每口 TWD 24；選擇權權利金稅、到期現金結算稅及 MTX 期交稅都沿用共用的日期版本與每口整元規則。沒有人工滑價、成交量／掛單深度限制，也依你的要求沒有壓力測試。

{_cost_table(metrics)}

![毛損益與成本]({relative(charts['pnl_costs'])})

Local 的毛損益 TWD {_money(local['gross_pnl_twd'])}，總手續費與稅 TWD {_money(float(local['fixed_fees_twd']) + float(local['transaction_tax_twd']))}；經典策略的顯式成本為 TWD {_money(float(classic['fixed_fees_twd']) + float(classic['transaction_tax_twd']))}。因此 Local 的優勢已是扣除增加的 MTX 交易成本後結果。

## 年度穩定性

全期正值高度受 2026 年截至 8 月 7 日影響：經典策略 2026 部分年度貢獻 TWD {_money(classic_2026)}，占全期淨利 **{classic_2026 / float(classic['net_pnl_twd']) * 100:.1f}%**；Local 同期貢獻 TWD {_money(local_2026)}，占全期 **{local_2026 / float(local['net_pnl_twd']) * 100:.1f}%**。所以「全期獲利」成立，但「跨 regime 穩定獲利」仍不成立。

![年度共同資本報酬矩陣]({relative(charts['annual_heatmap'])})

{_annual_table(annual)}

2012 只含 11 月 21 日起，2026 只到 8 月 7 日；表中的每格都是該日曆年度區段淨損益除以同一共同資本，不是年化報酬率。

## 六種模型與校準品質

六模型每天只改變下一交易日要持有的整數 MTX delta hedge；選擇權兩腿、進場價、到期結算、費用與稅完全沿用同一個經典帳本。每種模型有 2,288 個持倉且非到期的可校準收盤，全部成功；599 個到期日不為下一日產生新訊號，另 461 日在歷史週選尚未掛牌或帳本空倉。

{_calibration_table(calibrations)}

![校準診斷]({relative(charts['calibration'])})

Heston-SVI 的樣本內 IV RMSE 最低，但交易績效最低之一，顯示低樣本內定價誤差不等於較佳樣本外避險。SABR 有 884/2,288 日命中參數界線，Rough proxy 有 1,539/2,288 日命中，這兩者的參數穩定性是明確警訊。

| 名稱 | 本次實作層級 | 可否當作完整模型 |
|---|---|---|
| Black-Scholes | Black-76 平坦 IV 直接公式 | 是，簡單基準 |
| Heston | raw-SVI smile 代理 Heston skew delta | 否；沒有 Heston 特徵函數校準 |
| SABR | Hagan beta=1 漸近公式 | 公式本身是；不是完整動態模擬 |
| Local Vol | 平滑跨履約價、跨到期 IV surface delta | 否；沒有 Dupire PDE |
| SLV | Local surface 與 SVI variance 混合 | 否；不是 particle-calibrated SLV |
| Rough Vol | 冪律 term-skew surface | 否；不是 rough Bergomi Monte Carlo |

所以正確說法是：**Local Vol 家族的一階 surface-delta proxy 在這個日頻長樣本中最值得進入下一層驗證**，而不是「完整 Local Vol 已證明可獲利」。

## 資料品質與因果邊界

- 官方原始來源共 22 個年度／區間檔；正規化後有 **663,940** 個唯一 `date/series/strike` OTM IV 點、771 個到期系列，日期完整涵蓋 2012-11-21～2026-08-07。
- 663,940 點全部有官方每日結算價，本次沒有使用日末成交 fallback；但「每日結算」仍不是同步 bid/ask。
- 每個模型 2,288/2,288 日校準成功；held series 最少 15 點、中位數 29 點，全 surface 中位數 134 點。
- D 日選擇權 surface 與 TX 收盤完成後才校準，訊號最早 D+1 的 MTX 開盤執行；沒有同日收盤價前視。
- MTX 合約月份改變前會在舊合約日收盤代理平倉，下一日新合約開盤再依已知訊號建倉；不把兩個不同月份價格直接接成持倉損益。
- 一口 Call 加一口 Put；各模型的 MTX 最大絕對部位都是一口。假設一口能成交，不使用成交量或掛單量限制。
- 到期與轉倉的 MTX 日收盤平倉是終端代理，沒有歷史逐筆 timestamp 可證明在指定決策後成交；報告沒有把它標成嚴格逐筆 fill。
- 已驗證七策略各 3,348 日完整且唯一、模型樣本末空倉、MTX 現金帳本等於每日 futures P&L、摘要端點等於每日曲線、人工滑價為零。

## 是否值得繼續

依「先確認營利，再逐層增加真實限制」的順序，本層答案是：六模型全部成本後正值，但只有 Local 與 SABR 勝過經典基準；其中 Local 同時改善淨利、Sharpe、Sortino、MDD 與 Calmar，優先級最高。下一個唯一有資訊價值的限制，是用可同步取得的即時 Bid/Ask 做前瞻 paper capture，驗證選擇權兩腿與 MTX 終端平倉；本報告沒有預先加滑價、深度或壓力測試。

## 可重跑與明細

- `daily_results.parquet`：7 策略 × 3,348 日，每日 P&L、共同資本累積報酬及回撤。
- `futures_trades.parquet`：所有 MTX 開盤避險與到期／轉倉終端代理帳本。
- `signals.parquet`：D 日完成 surface 對 D+1 的整數 MTX 目標。
- `calibrations.parquet` / `.csv`：13,728 筆模型校準、RMSE、參數與邊界診斷。
- `annual_results.parquet` / `.csv`：15 個日曆年度區段 × 7 策略。
- `daily_iv_surface.parquet`：663,940 個 receipt-backed 日 surface 點。
- `metrics.csv`、`summary.json`、`receipt.json`：績效摘要、來源 hash、方法與驗證收據。

重跑命令：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/backtest_taifex_volatility_model_gamma_daily_history.py --workers 6
run_fintech_python scripts/render_taifex_volatility_model_daily_history_report.py
```
"""
    report_path.write_text(markdown, encoding="utf-8")
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    report_receipt: dict[str, Any] = {
        "schema_version": 1,
        "status": "complete",
        "generated_at_utc": generated_at,
        "delivery_format": "markdown_with_png",
        "html_generated": False,
        "report": str(report_path),
        "report_sha256": _sha256_path(report_path),
        "charts": {
            key: {"path": str(path), "sha256": _sha256_path(path)}
            for key, path in charts.items()
        },
    }
    _atomic_json(input_dir / "report_receipt.json", report_receipt)
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = render_report(args.input_dir, args.output)
    print(json.dumps({"status": "complete", "report": str(path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
