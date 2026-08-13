#!/usr/bin/env python3
"""Render the TXO volatility-model comparison as Markdown plus PNG charts."""

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
from stockagent.data.tw_index_derivatives_tick import _atomic_json  # noqa: E402
from stockagent.research.taifex_volatility_models import (  # noqa: E402
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_LOCAL_VOL,
    MODEL_ROUGH_VOL,
    MODEL_SABR,
    MODEL_SLV,
)


DEFAULT_INPUT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_volatility_model_gamma"
)
CLASSIC: Final[str] = "classic_opening_straddle"
MODEL_ORDER: Final[tuple[str, ...]] = (
    MODEL_BLACK_SCHOLES,
    MODEL_HESTON_SVI,
    MODEL_SABR,
    MODEL_LOCAL_VOL,
    MODEL_SLV,
    MODEL_ROUGH_VOL,
)
VARIANT_TO_SHORT: Final[dict[str, str]] = {
    CLASSIC: "Classic straddle",
    f"vol_model_gamma__{MODEL_BLACK_SCHOLES}": "Black-Scholes",
    f"vol_model_gamma__{MODEL_HESTON_SVI}": "Heston-SVI proxy",
    f"vol_model_gamma__{MODEL_SABR}": "SABR beta=1",
    f"vol_model_gamma__{MODEL_LOCAL_VOL}": "Local Vol proxy",
    f"vol_model_gamma__{MODEL_SLV}": "SLV proxy",
    f"vol_model_gamma__{MODEL_ROUGH_VOL}": "Rough Vol proxy",
}
COLORS: Final[dict[str, str]] = {
    CLASSIC: "#111827",
    f"vol_model_gamma__{MODEL_BLACK_SCHOLES}": "#6b7280",
    f"vol_model_gamma__{MODEL_HESTON_SVI}": "#2563eb",
    f"vol_model_gamma__{MODEL_SABR}": "#f59e0b",
    f"vol_model_gamma__{MODEL_LOCAL_VOL}": "#10b981",
    f"vol_model_gamma__{MODEL_SLV}": "#8b5cf6",
    f"vol_model_gamma__{MODEL_ROUGH_VOL}": "#ef4444",
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


def _curve_plot(
    normalized_daily: pl.DataFrame,
    *,
    output_path: Path,
    models_only: bool,
) -> None:
    figure, axis = plt.subplots(figsize=(11.5, 6.2))
    for frame in normalized_daily.partition_by("variant_id", maintain_order=True):
        variant_id = str(frame.item(0, "variant_id"))
        if models_only and variant_id == CLASSIC:
            continue
        frame = frame.sort("trading_date")
        axis.plot(
            frame.get_column("trading_date").to_list(),
            frame.get_column("cumulative_return_on_capital").to_numpy() * 100.0,
            label=VARIANT_TO_SHORT[variant_id],
            color=COLORS[variant_id],
            linewidth=2.6 if variant_id == CLASSIC else 1.8,
        )
    axis.axhline(0.0, color="#9ca3af", linewidth=0.8)
    axis.set_title(
        "Daily cumulative return - six delta models"
        if models_only
        else "Daily cumulative return - classic and six delta models"
    )
    axis.set_xlabel("Trading date")
    axis.set_ylabel("Cumulative return on fixed capital (%)")
    axis.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=5, maxticks=9))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    axis.grid(alpha=0.22)
    axis.legend(loc="best", fontsize=8.5, ncol=2)
    figure.autofmt_xdate(rotation=0)
    _save_figure(figure, output_path)


def _pnl_cost_plot(metrics: pl.DataFrame, output_path: Path) -> None:
    ordered_ids = [CLASSIC] + [f"vol_model_gamma__{model}" for model in MODEL_ORDER]
    rows = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    labels = [VARIANT_TO_SHORT[variant_id] for variant_id in ordered_ids]
    net = np.asarray([float(rows[value]["net_pnl_twd"]) for value in ordered_ids])
    costs = np.asarray(
        [
            float(rows[value]["fixed_fees_twd"])
            + float(rows[value]["transaction_tax_twd"])
            for value in ordered_ids
        ]
    )
    positions = np.arange(len(labels))
    width = 0.38
    figure, axis = plt.subplots(figsize=(11.5, 6.2))
    axis.bar(positions - width / 2, net / 1_000.0, width, label="Net P&L", color="#2563eb")
    axis.bar(positions + width / 2, costs / 1_000.0, width, label="Fees + tax", color="#f59e0b")
    axis.set_title("One-lot net P&L and explicit costs")
    axis.set_ylabel("TWD thousands")
    axis.set_xticks(positions, labels, rotation=22, ha="right")
    axis.grid(axis="y", alpha=0.22)
    axis.legend()
    _save_figure(figure, output_path)


def _calibration_plot(calibrations: pl.DataFrame, output_path: Path) -> None:
    summary = {
        str(row["volatility_model"]): row
        for row in calibrations.group_by("volatility_model").agg(
            pl.col("calibration_rmse_iv").mean().alias("mean_rmse"),
            pl.col("parameter_at_bound").sum().alias("bound_hits"),
        ).iter_rows(named=True)
    }
    labels = [VARIANT_TO_SHORT[f"vol_model_gamma__{model}"] for model in MODEL_ORDER]
    rmse = np.asarray([float(summary[model]["mean_rmse"]) * 100.0 for model in MODEL_ORDER])
    hits = np.asarray([int(summary[model]["bound_hits"]) for model in MODEL_ORDER])
    positions = np.arange(len(labels))
    figure, axis = plt.subplots(figsize=(11.5, 6.2))
    bars = axis.bar(positions, rmse, color=[COLORS[f"vol_model_gamma__{m}"] for m in MODEL_ORDER])
    axis.set_title("Causal daily calibration diagnostics")
    axis.set_ylabel("Mean IV RMSE (volatility points x 100)")
    axis.set_xticks(positions, labels, rotation=22, ha="right")
    axis.grid(axis="y", alpha=0.22)
    for bar, hit_count in zip(bars, hits, strict=True):
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"bounds: {hit_count}/29",
            ha="center",
            va="bottom",
            fontsize=8,
        )
    _save_figure(figure, output_path)


def _result_rows(metrics: pl.DataFrame) -> str:
    order = [CLASSIC] + [f"vol_model_gamma__{model}" for model in MODEL_ORDER]
    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    lines = [
        "| 策略 / 模型 | 固定資金 TWD | 成本後淨利 TWD | 非年化累積報酬 | Sharpe | Sortino | MDD | Calmar |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for variant_id in order:
        row = lookup[variant_id]
        lines.append(
            "| {label} | {capital} | {net} | {ret} | {sharpe} | {sortino} | {mdd} | {calmar} |".format(
                label=VARIANT_TO_SHORT[variant_id],
                capital=_money(row["capital_base_twd"]),
                net=_money(row["net_pnl_twd"]),
                ret=_percent(row["cumulative_return_on_capital"]),
                sharpe=_number(row["annualized_sharpe"]),
                sortino=_number(row["annualized_sortino"]),
                mdd=_percent(row["maximum_drawdown_compounded_return"]),
                calmar=_number(row["sample_calmar_nonannualized"]),
            )
        )
    return "\n".join(lines)


def _cost_rows(metrics: pl.DataFrame) -> str:
    order = [CLASSIC] + [f"vol_model_gamma__{model}" for model in MODEL_ORDER]
    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    lines = [
        "| 策略 / 模型 | 毛損益 | 手續費 | 交易稅 | 成本後淨利 | 期貨口數側 | 避險事件 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for variant_id in order:
        row = lookup[variant_id]
        lines.append(
            f"| {VARIANT_TO_SHORT[variant_id]} | {_money(row['gross_pnl_twd'])} | "
            f"{_money(row['fixed_fees_twd'])} | {_money(row['transaction_tax_twd'])} | "
            f"{_money(row['net_pnl_twd'])} | {int(row['total_futures_trade_sides']):,} | "
            f"{int(row['total_hedge_events']):,} |"
        )
    return "\n".join(lines)


def _calibration_rows(calibrations: pl.DataFrame) -> str:
    summary = calibrations.group_by("volatility_model").agg(
        pl.len().alias("sessions"),
        pl.col("calibration_rmse_iv").mean().alias("rmse"),
        pl.col("calibration_points").min().alias("points_min"),
        pl.col("calibration_points").median().alias("points_median"),
        pl.col("parameter_at_bound").sum().alias("bound_hits"),
        pl.col("maximum_calibration_staleness_seconds").max().alias("max_stale"),
    )
    lookup = {str(row["volatility_model"]): row for row in summary.iter_rows(named=True)}
    lines = [
        "| 模型 | 有效校準日 | 平均 IV RMSE | 點數 min / median | 邊界命中 | 最大成交陳舊秒數 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for model in MODEL_ORDER:
        row = lookup[model]
        lines.append(
            f"| {VARIANT_TO_SHORT[f'vol_model_gamma__{model}']} | {int(row['sessions'])} | "
            f"{float(row['rmse']):.4f} | {int(row['points_min'])} / "
            f"{float(row['points_median']):.0f} | {int(row['bound_hits'])}/29 | "
            f"{float(row['max_stale']):.0f} |"
        )
    return "\n".join(lines)


def render_report(input_dir: Path, output_path: Path | None = None) -> Path:
    summary = json.loads((input_dir / "summary.json").read_text(encoding="utf-8"))
    receipt = json.loads((input_dir / "receipt.json").read_text(encoding="utf-8"))
    if summary.get("status") != "complete" or receipt.get("status") != "complete":
        raise ValueError("backtest summary/receipt is not complete")
    metrics = pl.read_csv(input_dir / "metrics.csv")
    normalized_daily = pl.read_parquet(input_dir / "capital_normalized_daily.parquet")
    calibrations = pl.read_parquet(input_dir / "calibrations.parquet")
    trades = pl.read_parquet(input_dir / "trades.parquet")
    plots_dir = input_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    chart_paths = {
        "all_curves": plots_dir / "daily_cumulative_return_all.png",
        "model_curves": plots_dir / "daily_cumulative_return_models.png",
        "pnl_costs": plots_dir / "net_pnl_and_costs.png",
        "calibration": plots_dir / "calibration_diagnostics.png",
    }
    _curve_plot(normalized_daily, output_path=chart_paths["all_curves"], models_only=False)
    _curve_plot(normalized_daily, output_path=chart_paths["model_curves"], models_only=True)
    _pnl_cost_plot(metrics, chart_paths["pnl_costs"])
    _calibration_plot(calibrations, chart_paths["calibration"])

    lookup = {str(row["variant_id"]): row for row in metrics.iter_rows(named=True)}
    classic = lookup[CLASSIC]
    model_rows = [row for key, row in lookup.items() if key != CLASSIC]
    best = max(model_rows, key=lambda row: float(row["cumulative_return_on_capital"]))
    profitable = sum(float(row["net_pnl_twd"]) > 0.0 for row in model_rows)
    beat_classic = sum(
        float(row["cumulative_return_on_capital"])
        > float(classic["cumulative_return_on_capital"])
        for row in model_rows
    )
    futures = trades.filter(pl.col("instrument_type") == "future")
    p50_delay = float(futures.get_column("fill_delay_seconds").quantile(0.50))
    p95_delay = float(futures.get_column("fill_delay_seconds").quantile(0.95))
    max_delay = float(futures.get_column("fill_delay_seconds").max())
    report_path = output_path or (input_dir / "volatility_model_report.md")
    relative = lambda path: path.relative_to(input_dir).as_posix()
    markdown = f"""# 台指選擇權六種波動模型：一個月成本後逐筆回測

## 結論先講

在 2026-06-25 至 2026-08-06 的 30 個交易日、12 個完整到期循環中，六種模型策略扣除手續費與交易稅後全部正獲利（{profitable}/6），但用各策略固定資金需求正規化後，沒有任何一種（{beat_classic}/6）勝過不做 delta hedge 的經典開盤 ATM straddle。

模型組最佳是 **{VARIANT_TO_SHORT[str(best['variant_id'])]}**：成本後淨利 **TWD {_money(best['net_pnl_twd'])}**，非年化累積報酬 **{_percent(best['cumulative_return_on_capital'])}**，Sharpe **{_number(best['annualized_sharpe'])}**。經典 straddle 為淨利 **TWD {_money(classic['net_pnl_twd'])}**、非年化累積報酬 **{_percent(classic['cumulative_return_on_capital'])}**、Sharpe **{_number(classic['annualized_sharpe'])}**。

這不能解讀成「Heston 比其他模型有穩定 alpha」。這個月長波動本身非常有利；每分鐘 delta hedge 壓低方向曝險，也增加 TMF 保證金、手續費與交易稅，因此六種避險版都大幅落後未避險基準。樣本只有一個月，足以做第一層可行性篩選，不足以做投資結論。

![全部策略每日累積報酬]({relative(chart_paths['all_curves'])})

## 成本後績效

報酬是「固定一口所需最大資金」分母下的非年化累積報酬，不是年報酬率。Sharpe、Sortino 依每日報酬年化；Calmar 使用本樣本非年化累積報酬除以 MDD，欄位意義刻意分開。

{_result_rows(metrics)}

模型曲線放大如下；上圖因經典策略報酬很高，會壓縮六模型之間差異。

![六模型每日累積報酬]({relative(chart_paths['model_curves'])})

## 手續費與交易稅拆解

TXO 單向每口 TWD 22；TMF 單向每口 TWD 16。交易稅沿用共用、按日期版本化的法定計算與每口整元規則。沒有另加滑價，也沒有壓力測試。

{_cost_rows(metrics)}

![成本與淨利]({relative(chart_paths['pnl_costs'])})

## 六種模型實際做了什麼

六模型共用完全相同的選擇權部位：開盤後買一口 ATM Call 與一口 ATM Put，持有至官方到期結算；下一個完整可觀測到期系列才重新買入。模型只決定每分鐘要持有幾口 TMF 來抵銷 straddle delta，因此比較不會混入不同選擇權進場價或到期結算方式。

| 名稱 | 本次實作層級 | 可否視為完整模型 |
|---|---|---|
| Black-Scholes | Black-76 平坦 IV 直接公式 | 是，作為簡單基準 |
| Heston | raw-SVI smile 代理 Heston skew delta | 否；不是 Heston 特徵函數校準 |
| SABR | Hagan beta=1 漸近公式 | 是公式本身；但不是完整動態模擬 |
| Local Vol | 平滑跨履約價、跨到期 IV surface delta | 否；不是 Dupire PDE |
| SLV | Local surface 與 SVI variance 混合 | 否；不是 particle-calibrated SLV |
| Rough Vol | 冪律 term-skew surface | 否；不是 rough Bergomi Monte Carlo |

因此，本報告回答的是「這六個模型家族的一階、可因果 delta hedge 是否值得繼續投入完整實作」，不是把代理結果冒充完整 Heston、Dupire、SLV 或 rough Bergomi。模型原始方法可參考 [Heston (1993)](https://ideas.repec.org/a/oup/rfinst/v6y1993i2p327-43.html)、[SABR](http://www.wilmott.com/pdfs/021118_smile.pdf)、[SLV particle calibration](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1885032) 與 [rough Bergomi](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2554754)。

## 校準品質

每天 08:55 只校準一次，參數整天固定；當日進場若晚於 08:55，則等兩腿成交後下一個完整分鐘才校準。資料只允許看到決策前一個已完成整秒。最後一天沒有下一個能在樣本內完整到期的部位，所以每個模型有 29 個有效校準日。

{_calibration_rows(calibrations)}

![校準診斷]({relative(chart_paths['calibration'])})

SABR 有 14/29 日參數命中界線，Rough proxy 有 17/29 日 Hurst 命中界線；這是明確的不穩定警訊。Heston-SVI 的平均樣本內 IV RMSE 最低，但低校準誤差不等於高樣本外交易獲利。

## 成交與因果邊界

- 資料是 TAIFEX 歷史逐筆成交，不含可重建的歷史 Bid/Ask；所以不能宣稱使用 bid/ask 可成交價。
- 每次非終端交易都取決策後第一筆 TMF 成交，且已驗證 `fill_ts > decision_ts`。成交延遲中位數 {p50_delay:.0f} 秒、p95 {p95_delay:.0f} 秒、最大 {max_delay:.0f} 秒。
- 只測一口，不使用成交量或掛單深度限制，假設可主動成交；人工滑價為零。
- TXO 到期以官方最終結算價現金結算；TMF 在到期日 13:25 決策後、13:30 前平倉。
- `202607F2` 原排定 2026-07-10，但官方延至 2026-07-13 結算；校準到期時間已跟官方帳本對齊，沒有使用名稱推導的過期日期。
- 帳本已核對：30 日完整覆蓋、模型選擇權交易與經典基準逐欄相同、非終端成交嚴格晚於決策、樣本末空倉、每日損益等於逐筆帳本、資本曲線端點等於摘要。

## 如何解讀與下一步門檻

目前第一層結果是「全部成本後獲利，但沒有一個模型提高相對經典 straddle 的資本報酬」。依你先前指定的順序，這裡沒有加入額外滑價、深度、成交率或壓力測試。若要決定是否值得做完整 Heston/Local Vol/SLV/Rough Vol，合理門檻應是先補歷史 Bid/Ask 或真實即時報價，再確認模型代理至少能在多月份、跨行情下穩定勝過 Black-Scholes delta 基準；本月結果本身不支持直接增加更複雜限制或上實盤。

## 可重跑檔案

- `daily_results.parquet`：7 個策略 × 30 日每日損益。
- `capital_normalized_daily.parquet` / `.csv`：每日固定資金報酬與累積曲線。
- `metrics.csv`：成本、Sharpe、Sortino、MDD、Calmar 等摘要。
- `trades.parquet`：全部 3,961 筆選擇權與 TMF 帳本列。
- `calibrations.parquet` / `.csv`：174 筆每日模型校準診斷。
- `summary.json` / `receipt.json`：來源 hash、方法合約與驗證收據。

重跑命令：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/backtest_taifex_volatility_model_gamma.py
run_fintech_python scripts/render_taifex_volatility_model_report.py
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
            for key, path in chart_paths.items()
        },
    }
    _atomic_json(input_dir / "report_receipt.json", report_receipt)
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = render_report(args.input_dir, args.output)
    print(json.dumps({"status": "complete", "report": str(path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
