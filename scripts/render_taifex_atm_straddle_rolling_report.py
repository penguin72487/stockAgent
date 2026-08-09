#!/usr/bin/env python3
"""Render the one-lot TXO rolling sweep and classic-straddle comparison."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Final

import matplotlib

matplotlib.use("Agg", force=True)
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DEFAULT_ARTIFACT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_atm_straddle_rolling"
)
CLASSIC: Final[str] = "classic_opening_straddle"
ROLL_OTM: Final[str] = "roll_otm_put_keep_itm_call"
ROLL_ITM: Final[str] = "roll_itm_call_keep_otm_put"
ROLLING_STRATEGIES: Final[tuple[str, str]] = (ROLL_OTM, ROLL_ITM)
LABELS: Final[dict[str, str]] = {
    CLASSIC: "Classic opening straddle",
    ROLL_OTM: "Roll OTM Put / Keep ITM Call",
    ROLL_ITM: "Roll ITM Call / Keep OTM Put",
}
LABELS_ZH: Final[dict[str, str]] = {
    CLASSIC: "經典開盤 Straddle（不換倉）",
    ROLL_OTM: "Roll OTM Put、留下 ITM Call",
    ROLL_ITM: "Roll ITM Call、留下 OTM Put",
}

BLUE: Final[str] = "#3569A8"
GOLD: Final[str] = "#D09A23"
ORANGE: Final[str] = "#C86632"
INK: Final[str] = "#252A31"
MUTED: Final[str] = "#68717D"
GRID: Final[str] = "#D9DEE5"
OPEN_BLUE: Final[str] = "#A9C5E3"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _atomic_json(path: Path, payload: Any) -> None:
    _atomic_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def _verify_and_load(
    artifact_dir: Path,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    summary = json.loads((artifact_dir / "summary.json").read_text(encoding="utf-8"))
    daily_path = artifact_dir / "daily_results.parquet"
    trades_path = artifact_dir / "trades.parquet"
    for path, expected in (
        (daily_path, summary["artifacts"]["daily_results_sha256"]),
        (trades_path, summary["artifacts"]["trades_sha256"]),
    ):
        actual = _sha256(path)
        if actual != expected:
            raise ValueError(
                f"artifact hash mismatch for {path}: expected {expected}, got {actual}"
            )
    daily = pd.read_parquet(daily_path)
    trades = pd.read_parquet(trades_path)
    daily["trading_date"] = pd.to_datetime(daily["trading_date"])
    if len(daily) != int(summary["daily_result_rows"]):
        raise ValueError("daily row count does not match summary")
    if len(trades) != int(summary["trade_rows"]):
        raise ValueError("trade row count does not match summary")
    if float(summary["parameters"]["slippage_points_per_side"]) != 0.0:
        raise ValueError("this report requires artificial slippage to be disabled")
    return summary, daily, trades


def _result_frame(summary: dict[str, Any]) -> pd.DataFrame:
    results = pd.DataFrame(summary["results"])
    classic = results.loc[results["strategy"] == CLASSIC]
    if len(classic) != 1 or int(classic.iloc[0]["rolling_points"]) != 0:
        raise ValueError("expected one classic no-roll baseline")
    thresholds = tuple(int(value) for value in summary["parameters"]["rolling_points"])
    if thresholds != tuple(range(50, 1001, 50)):
        raise ValueError(f"unexpected rolling threshold grid: {thresholds}")
    for strategy in ROLLING_STRATEGIES:
        observed = tuple(
            sorted(
                int(value)
                for value in results.loc[
                    results["strategy"] == strategy, "rolling_points"
                ]
            )
        )
        if observed != thresholds:
            raise ValueError(f"incomplete threshold sweep for {strategy}: {observed}")
    classic_fee = float(classic.iloc[0]["net_after_fee_twd"])
    classic_tax = float(classic.iloc[0]["net_after_fee_tax_twd"])
    results["delta_vs_classic_after_fee_twd"] = (
        results["net_after_fee_twd"] - classic_fee
    )
    results["delta_vs_classic_after_fee_tax_twd"] = (
        results["net_after_fee_tax_twd"] - classic_tax
    )
    results["strategy_label"] = results["strategy"].map(LABELS)
    return results


def _style() -> None:
    plt.rcParams.update(
        {
            "font.family": "Lato",
            "font.size": 10,
            "axes.labelcolor": INK,
            "axes.edgecolor": MUTED,
            "axes.titlecolor": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "xtick.color": MUTED,
            "ytick.color": MUTED,
            "text.color": INK,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
            "savefig.bbox": "tight",
        }
    )


def _finish_axes(ax: plt.Axes, *, x_grid: bool = False, y_grid: bool = False) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    if x_grid:
        ax.grid(axis="x", color=GRID, linewidth=0.8, alpha=0.75)
    if y_grid:
        ax.grid(axis="y", color=GRID, linewidth=0.8, alpha=0.75)
    ax.set_axisbelow(True)


def _money_k(value: float) -> str:
    return f"{value / 1000:+.1f}k"


def _plot_threshold_delta(results: pd.DataFrame, output: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 8.2), sharex=True, sharey=True)
    for ax, strategy, color in zip(
        axes, ROLLING_STRATEGIES, (BLUE, ORANGE), strict=True
    ):
        rows = results.loc[results["strategy"] == strategy].sort_values(
            "rolling_points"
        )
        ax.plot(
            rows["rolling_points"],
            rows["delta_vs_classic_after_fee_twd"],
            color=color,
            linewidth=2.2,
            marker="o",
            markersize=4.5,
        )
        ax.axhline(0, color=INK, linewidth=1.2, linestyle="--")
        best = rows.loc[rows["delta_vs_classic_after_fee_twd"].idxmax()]
        ax.annotate(
            f"best {int(best['rolling_points'])} pts: "
            f"{_money_k(float(best['delta_vs_classic_after_fee_twd']))}",
            xy=(best["rolling_points"], best["delta_vs_classic_after_fee_twd"]),
            xytext=(-120, 18),
            textcoords="offset points",
            arrowprops={"arrowstyle": "->", "color": MUTED, "linewidth": 1},
            fontsize=9,
            color=INK,
        )
        ax.set_title(LABELS[strategy], loc="left")
        ax.set_ylabel("Incremental P&L vs classic (TWD)")
        _finish_axes(ax, y_grid=True)
    axes[-1].set_xlabel("Rolling threshold (TX points)")
    axes[-1].set_xticks(np.arange(50, 1001, 50))
    axes[-1].tick_params(axis="x", rotation=45)
    fig.suptitle(
        "Incremental 30-day P&L versus classic opening straddle",
        x=0.09,
        y=0.98,
        ha="left",
        fontsize=14,
        fontweight="bold",
    )
    fig.text(
        0.09,
        0.945,
        "One contract per leg; fixed TWD 22 fee per contract-side only; zero means equal to classic",
        color=MUTED,
        fontsize=10,
    )
    fig.subplots_adjust(left=0.11, right=0.97, top=0.88, bottom=0.12, hspace=0.28)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def _best_rows(results: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    classic = results.loc[results["strategy"] == CLASSIC].iloc[0]
    best_otm = results.loc[
        results.loc[results["strategy"] == ROLL_OTM, "net_after_fee_twd"].idxmax()
    ]
    best_itm = results.loc[
        results.loc[results["strategy"] == ROLL_ITM, "net_after_fee_twd"].idxmax()
    ]
    return classic, best_otm, best_itm


def _plot_best_totals(results: pd.DataFrame, output: Path) -> None:
    classic, best_otm, best_itm = _best_rows(results)
    rows = [classic, best_otm, best_itm]
    labels = [
        LABELS[CLASSIC],
        f"{LABELS[ROLL_OTM]} ({int(best_otm['rolling_points'])} pts)",
        f"{LABELS[ROLL_ITM]} ({int(best_itm['rolling_points'])} pts)",
    ]
    values = [float(row["net_after_fee_twd"]) for row in rows]
    colors = [MUTED, BLUE, ORANGE]
    y = np.arange(3)
    fig, ax = plt.subplots(figsize=(12, 5.4))
    bars = ax.barh(y, values, color=colors, edgecolor=INK, linewidth=0.7)
    for bar, value in zip(bars, values, strict=True):
        ax.text(
            value + 1100,
            bar.get_y() + bar.get_height() / 2,
            _money_k(value),
            ha="left",
            va="center",
            fontsize=10,
            fontweight="bold",
        )
    ax.set_yticks(y, labels)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values) * 1.14)
    ax.set_xlabel("30-day P&L after fixed fees (TWD)")
    fig.suptitle(
        "Classic straddle and best threshold from each rolling policy",
        x=0.35,
        y=0.97,
        ha="left",
        fontsize=14,
        fontweight="bold",
    )
    fig.text(
        0.35,
        0.91,
        "June 25–August 6, 2026; simulated one-lot market fills; no artificial slippage",
        color=MUTED,
        fontsize=10,
    )
    _finish_axes(ax, x_grid=True)
    fig.subplots_adjust(left=0.35, right=0.96, top=0.82, bottom=0.16)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def _plot_cumulative_best(
    daily: pd.DataFrame, results: pd.DataFrame, output: Path
) -> None:
    classic, best_otm, best_itm = _best_rows(results)
    variants = (
        (CLASSIC, 0, MUTED, "-", LABELS[CLASSIC]),
        (
            ROLL_OTM,
            int(best_otm["rolling_points"]),
            BLUE,
            "--",
            f"Roll OTM ({int(best_otm['rolling_points'])} pts)",
        ),
        (
            ROLL_ITM,
            int(best_itm["rolling_points"]),
            ORANGE,
            "-.",
            f"Roll ITM ({int(best_itm['rolling_points'])} pts)",
        ),
    )
    fig, ax = plt.subplots(figsize=(12, 6.5))
    for strategy, threshold, color, linestyle, label in variants:
        rows = daily.loc[
            (daily["strategy"] == strategy)
            & (daily["rolling_points"] == threshold)
        ].sort_values("trading_date")
        ax.plot(
            rows["trading_date"],
            rows["net_after_fee_twd"].cumsum(),
            color=color,
            linestyle=linestyle,
            linewidth=2.3,
            marker="o",
            markersize=3.6,
            label=label,
        )
    ax.axhline(0, color=INK, linewidth=1)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=9))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.set_xlabel("Trading date")
    ax.set_ylabel("Cumulative P&L after fixed fees (TWD)")
    ax.legend(loc="upper left", frameon=False, ncol=3)
    fig.suptitle(
        "Daily cumulative P&L for classic and best rolling variants",
        x=0.1,
        y=0.98,
        ha="left",
        fontsize=14,
        fontweight="bold",
    )
    fig.text(
        0.1,
        0.935,
        "One contract per leg, daily reset, TWD 22 fixed fee per contract-side",
        color=MUTED,
        fontsize=10,
    )
    _finish_axes(ax, y_grid=True)
    fig.subplots_adjust(left=0.1, right=0.97, top=0.86, bottom=0.12)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def _plot_best_increment(daily: pd.DataFrame, results: pd.DataFrame, output: Path) -> None:
    classic, _best_otm, best_itm = _best_rows(results)
    threshold = int(best_itm["rolling_points"])
    base = daily.loc[
        daily["strategy"] == CLASSIC, ["trading_date", "net_after_fee_twd"]
    ].rename(columns={"net_after_fee_twd": "classic_pnl"})
    selected = daily.loc[
        (daily["strategy"] == ROLL_ITM)
        & (daily["rolling_points"] == threshold),
        ["trading_date", "net_after_fee_twd", "roll_count"],
    ].merge(base, on="trading_date", validate="one_to_one")
    selected["increment"] = selected["net_after_fee_twd"] - selected["classic_pnl"]
    colors = np.where(selected["increment"] >= 0.0, BLUE, OPEN_BLUE)
    fig, ax = plt.subplots(figsize=(12, 5.8))
    ax.bar(selected["trading_date"], selected["increment"], color=colors, width=0.75)
    ax.axhline(0, color=INK, linewidth=1)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=9))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.set_xlabel("Trading date")
    ax.set_ylabel("Daily incremental P&L vs classic (TWD)")
    fig.suptitle(
        f"Daily incremental P&L of the {threshold}-point Roll ITM policy",
        x=0.1,
        y=0.98,
        ha="left",
        fontsize=14,
        fontweight="bold",
    )
    fig.text(
        0.1,
        0.935,
        f"Only {int((selected['roll_count'] > 0).sum())} of 30 days rolled; monthly increment {_money_k(float(best_itm['delta_vs_classic_after_fee_twd']))}",
        color=MUTED,
        fontsize=10,
    )
    _finish_axes(ax, y_grid=True)
    fig.subplots_adjust(left=0.1, right=0.97, top=0.86, bottom=0.12)
    fig.savefig(output, dpi=180)
    plt.close(fig)


def _fmt_twd(value: float) -> str:
    return f"{value:+,.0f}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def _sweep_table(results: pd.DataFrame) -> str:
    classic = results.loc[results["strategy"] == CLASSIC].iloc[0]
    rows = [
        "| 門檻 | Roll OTM 固定費後 | 相對經典 | 換倉次數 | Roll ITM 固定費後 | 相對經典 | 換倉次數 |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for threshold in range(50, 1001, 50):
        otm = results.loc[
            (results["strategy"] == ROLL_OTM)
            & (results["rolling_points"] == threshold)
        ].iloc[0]
        itm = results.loc[
            (results["strategy"] == ROLL_ITM)
            & (results["rolling_points"] == threshold)
        ].iloc[0]
        rows.append(
            "| "
            + " | ".join(
                [
                    str(threshold),
                    _fmt_twd(float(otm["net_after_fee_twd"])),
                    _fmt_twd(float(otm["delta_vs_classic_after_fee_twd"])),
                    str(int(otm["total_rolls"])),
                    _fmt_twd(float(itm["net_after_fee_twd"])),
                    _fmt_twd(float(itm["delta_vs_classic_after_fee_twd"])),
                    str(int(itm["total_rolls"])),
                ]
            )
            + " |"
        )
    return (
        f"> 經典開盤 Straddle 基準：固定費後 {_fmt_twd(float(classic['net_after_fee_twd']))} 元；"
        f"30 天共 {int(classic['total_trade_sides'])} 個買賣方向。\n\n"
        + "\n".join(rows)
    )


def _strict_common_lookup(summary: dict[str, Any], strategy: str, threshold: int) -> pd.Series:
    strict = pd.DataFrame(summary["validation"]["common_strict_terminal_results"])
    return strict.loc[
        (strict["strategy"] == strategy)
        & (strict["rolling_points"] == threshold)
    ].iloc[0]


def _comparison_csv(results: pd.DataFrame, output: Path) -> None:
    fields = [
        "strategy",
        "strategy_label",
        "rolling_points",
        "days",
        "total_rolls",
        "total_trade_sides",
        "net_after_fee_twd",
        "delta_vs_classic_after_fee_twd",
        "average_daily_after_fee_twd",
        "median_daily_after_fee_twd",
        "win_rate_after_fee",
        "maximum_drawdown_after_fee_twd",
        "net_after_fee_tax_twd",
        "delta_vs_classic_after_fee_tax_twd",
    ]
    temporary = output.with_suffix(output.suffix + ".tmp")
    results.loc[:, fields].sort_values(
        ["strategy", "rolling_points"]
    ).to_csv(temporary, index=False)
    temporary.replace(output)


def _render_markdown(
    summary: dict[str, Any], daily: pd.DataFrame, results: pd.DataFrame
) -> str:
    classic, best_otm, best_itm = _best_rows(results)
    best_threshold = int(best_itm["rolling_points"])
    best_daily = daily.loc[
        (daily["strategy"] == ROLL_ITM)
        & (daily["rolling_points"] == best_threshold)
    ]
    classic_daily = daily.loc[
        daily["strategy"] == CLASSIC,
        ["trading_date", "net_after_fee_twd"],
    ].rename(columns={"net_after_fee_twd": "classic_after_fee_twd"})
    best_comparison = best_daily.merge(
        classic_daily, on="trading_date", validate="one_to_one"
    )
    best_comparison["increment_vs_classic_twd"] = (
        best_comparison["net_after_fee_twd"]
        - best_comparison["classic_after_fee_twd"]
    )
    rolled = best_comparison.loc[best_comparison["roll_count"] > 0]
    roll_days = int((best_daily["roll_count"] > 0).sum())
    positive_roll_days = int((rolled["increment_vs_classic_twd"] > 0).sum())
    negative_roll_days = int((rolled["increment_vs_classic_twd"] < 0).sum())
    classic_strict = _strict_common_lookup(summary, CLASSIC, 0)
    best_strict = _strict_common_lookup(summary, ROLL_ITM, best_threshold)
    start = pd.Timestamp(summary["date_start"]).strftime("%Y 年 %m 月 %d 日")
    end = pd.Timestamp(summary["date_end"]).strftime("%Y 年 %m 月 %d 日")
    sweep = _sweep_table(results)

    return f"""# 台指選擇權 Rolling 與經典 Straddle 比較報告

## Executive Summary

- **第一階段的經典開盤 Straddle 確認為正。** {start}至 {end}共 30 個交易日、Call 與 Put 各一口、只扣每個買賣方向 22 元時，不換倉基準獲利 {_fmt_twd(float(classic['net_after_fee_twd']))} 元，每日中位數 {_fmt_twd(float(classic['median_daily_after_fee_twd']))} 元，勝率 {_fmt_pct(float(classic['win_rate_after_fee']))}。
- **40 組 rolling 設定中，只有 Roll ITM 的 950 與 1,000 點門檻超過經典基準。** 950 點最佳，固定費後 {_fmt_twd(float(best_itm['net_after_fee_twd']))} 元，比經典多 {_fmt_twd(float(best_itm['delta_vs_classic_after_fee_twd']))} 元；1,000 點多 {_fmt_twd(float(results.loc[(results['strategy'] == ROLL_ITM) & (results['rolling_points'] == 1000), 'delta_vs_classic_after_fee_twd'].iloc[0]))} 元。
- **Rolling 的增量優勢很小。** 950 點只在 {roll_days} 天各換倉一次，{positive_roll_days} 天優於經典、{negative_roll_days} 天落後；月增量約為經典基準的 {float(best_itm['delta_vs_classic_after_fee_twd']) / float(classic['net_after_fee_twd']) * 100:.1f}%，同時最大回撤由 {_fmt_twd(float(classic['maximum_drawdown_after_fee_twd']))} 擴大至 {_fmt_twd(float(best_itm['maximum_drawdown_after_fee_twd']))} 元。
- **依你的階段式規則，人工滑價完全未加入。** 第一階段為正後，才查看下一層法定交易稅；950 點 Roll ITM 在稅後仍比經典多 {_fmt_twd(float(best_itm['delta_vs_classic_after_fee_tax_twd']))} 元。歷史 bid/ask 不存在，因此本報告仍是保證一口市價成交的價格代理模擬，不是可成交性證明。

## 只有高門檻 Roll ITM 略勝經典 Straddle

這張圖直接顯示每個 rolling 門檻相對經典不換倉 Straddle 的增量；零線代表兩者相同。Roll OTM 的 20 組設定沒有任何一組超過經典基準，最佳 750 點仍少 {abs(float(best_otm['delta_vs_classic_after_fee_twd'])):,.0f} 元。Roll ITM 只有 950、1,000 點略為領先。

![各門檻相對經典 Straddle 的增量](./figures/threshold_sweep_vs_classic.png)

圖 1：固定費後的 30 日增量損益；不含人工滑價與 bid/ask 深度限制。負值表示 rolling 不如完全不換倉。

最佳 Roll OTM、最佳 Roll ITM 與經典基準的總損益非常接近。950 點 Roll ITM 雖然最高，但差距只有 3,678 元，遠小於每組策略約 4.5～5.2 萬元的最大回撤。

![經典與各方向最佳門檻](./figures/best_variants_total_pnl.png)

圖 2：所有長條均從零開始，只扣每個合約買賣方向 22 元。

## 完整 50 至 1,000 點結果

以下每 50 點一組，共 20 個門檻。相對經典欄位是 rolling 固定費後損益減去經典 Straddle 的 {_fmt_twd(float(classic['net_after_fee_twd']))} 元；換倉次數是 30 天合計。

{sweep}

**解讀：** 低門檻產生大量換倉與固定費，且大多沒有換來足夠的權利金改善。高門檻逐漸接近經典 Straddle；950 點 Roll ITM 的小幅領先來自少數幾個實際換倉日，而不是每天穩定增加收益。

## 最佳 Rolling 與經典基準的每日路徑

三條累積曲線大部分時間重疊，因為 750／950 點門檻在多數日期不會觸發。950 點 Roll ITM 的月度領先是在少數日期正負跳動後留下的小額淨差，不是持續性的平滑優勢。

![每日累積損益](./figures/daily_cumulative_best_vs_classic.png)

圖 3：經典 Straddle、最佳 Roll OTM 750 點及最佳 Roll ITM 950 點；全部採每日重設、一口模擬市價與固定費。

逐日增量圖顯示，950 點策略只在 7 天和經典基準不同。其中最大單日改善與最大單日惡化都遠大於整月最後的 3,678 元增量，因此一個月樣本不足以確認 rolling 本身有穩定優勢。

![950 點 Roll ITM 的逐日增量](./figures/daily_increment_best_vs_classic.png)

圖 4：深色為相對經典增加收益，淺色為相對經典減少收益；未換倉日增量為零。

## 第一階段為正後，才加入下一層真實成本

依指定流程，主排名只使用固定費。因經典與 950／1,000 點 Roll ITM 在第一階段皆為正，才進入法定交易稅層：

| 策略 | 固定費後 | 再扣 0.1% 交易稅 | 稅後相對經典 |
|---|---:|---:|---:|
| 經典開盤 Straddle | {_fmt_twd(float(classic['net_after_fee_twd']))} | {_fmt_twd(float(classic['net_after_fee_tax_twd']))} | — |
| Roll ITM 950 點 | {_fmt_twd(float(best_itm['net_after_fee_twd']))} | {_fmt_twd(float(best_itm['net_after_fee_tax_twd']))} | {_fmt_twd(float(best_itm['delta_vs_classic_after_fee_tax_twd']))} |
| Roll ITM 1,000 點 | {_fmt_twd(float(results.loc[(results['strategy'] == ROLL_ITM) & (results['rolling_points'] == 1000), 'net_after_fee_twd'].iloc[0]))} | {_fmt_twd(float(results.loc[(results['strategy'] == ROLL_ITM) & (results['rolling_points'] == 1000), 'net_after_fee_tax_twd'].iloc[0]))} | {_fmt_twd(float(results.loc[(results['strategy'] == ROLL_ITM) & (results['rolling_points'] == 1000), 'delta_vs_classic_after_fee_tax_twd'].iloc[0]))} |

人工滑價仍為 0，沒有加入 spread、深度、成交量上限或等待時間淘汰條件。這些必須等策略在更長期間仍能維持相對經典的增量後，再逐層加入。

## 模擬市價與 bid/ask 的界線

- 過去這一個月的 TAIFEX 檔案只有逐筆成交，沒有可回補的歷史 bid/ask；Shioaji `BidAskFOPv1` 只能從現在開始保存。
- 本次依指定假設每腿只有一口，所有訂單視為保證成交，不使用成交量、掛單量或五檔深度做容量限制；同秒成交量只用來形成該秒代表價格，不影響是否成交。
- 價格仍需有歷史依據：進場與換倉使用決策後第一個完整秒的成交價代理，收盤使用 13:45 前最後成交價代理；它們不是實際的 ask 買入價或 bid 賣出價。
- 在 6 個所有設定都能於 13:40 後找到雙腿後續成交的共同日期，經典固定費後為 {_fmt_twd(float(classic_strict['net_after_fee_twd']))} 元，950 點 Roll ITM 為 {_fmt_twd(float(best_strict['net_after_fee_twd']))} 元；樣本太少，只能作方向檢查。

## 建議的下一步

1. **先延長同一個一口市價模擬期間。** 目前 950 點只觸發 7 次，無法判斷 3,678 元增量是策略還是偶然。
2. **主要比較相對經典 Straddle 的 paired daily P&L。** Rolling 本身為正不夠，必須持續超過不換倉基準。
3. **若較長期間仍為正，再依順序加入限制。** 建議順序為法定交易稅 → 實際 bid/ask 買賣價 → stale/max-wait → 五檔深度；不先加入人工壓力滑價。
4. **同步保存 Shioaji BidAskFOPv1。** 等累積足夠完整日盤後，用買 ask、賣 bid 重新驗證，而不是嘗試從成交檔還原不存在的歷史委買委賣。

## 重要假設與限制

- 開倉時間為 08:50，因 08:45 當下常沒有雙腿完整且因果可用的 ATM 成交；每腿一口、點值 50 元、每天平倉且不隔夜。
- Rolling 只在 TX 向上越過門檻時觸發，從上一次成功換倉的 TX 價格重新起算；本次未測試下跌時的對稱 rolling。
- 主結果只扣每個合約買賣方向 22 元；人工滑價為 0。交易稅只在第一階段確認為正後的表格中呈現。
- 模擬不使用成交量與掛單量限制，但仍依賴歷史成交價作為市價代理。這項假設使結果偏向研究用途，不能當成歷史 bid/ask 實際成交績效。
- 30 日內 950 點 Roll ITM 只有 7 個換倉日，月增量由 4 個正貢獻日與 3 個負貢獻日組成；樣本不足以宣稱穩定超額收益。

---

報告由已驗證的 `daily_results.parquet`、`trades.parquet` 與 `summary.json` 直接產生；完整 41 組比較另存於 `threshold_comparison.csv`。
"""


def _chart_map() -> dict[str, Any]:
    return {
        "charts": [
            {
                "file": "figures/threshold_sweep_vs_classic.png",
                "family": "Uncertainty & Benchmark",
                "type": "faceted line with zero reference",
                "question": "Which rolling thresholds add P&L versus classic straddle?",
                "fields": ["strategy", "rolling_points", "delta_vs_classic_after_fee_twd"],
            },
            {
                "file": "figures/best_variants_total_pnl.png",
                "family": "Comparison & Ranking",
                "type": "horizontal bar",
                "question": "How do classic and each policy's best threshold compare in total P&L?",
                "fields": ["strategy", "rolling_points", "net_after_fee_twd"],
            },
            {
                "file": "figures/daily_cumulative_best_vs_classic.png",
                "family": "Trend",
                "type": "multi-series line",
                "question": "How do the daily cumulative paths differ?",
                "fields": ["trading_date", "strategy", "rolling_points", "net_after_fee_twd"],
            },
            {
                "file": "figures/daily_increment_best_vs_classic.png",
                "family": "Uncertainty & Benchmark",
                "type": "signed daily bar",
                "question": "Which days create the best rolling policy's incremental P&L?",
                "fields": ["trading_date", "roll_count", "increment_vs_classic_twd"],
            },
        ],
        "primary_metric": "net_after_fee_twd",
        "palette_policy": "hard two-root cap plus neutrals",
        "renderer": "matplotlib static PNG",
    }


def render_report(artifact_dir: Path) -> dict[str, Any]:
    summary, daily, trades = _verify_and_load(artifact_dir)
    results = _result_frame(summary)
    figures = artifact_dir / "figures"
    figures.mkdir(parents=True, exist_ok=True)
    _style()
    outputs = {
        "figures/threshold_sweep_vs_classic.png": figures
        / "threshold_sweep_vs_classic.png",
        "figures/best_variants_total_pnl.png": figures
        / "best_variants_total_pnl.png",
        "figures/daily_cumulative_best_vs_classic.png": figures
        / "daily_cumulative_best_vs_classic.png",
        "figures/daily_increment_best_vs_classic.png": figures
        / "daily_increment_best_vs_classic.png",
    }
    _plot_threshold_delta(results, outputs["figures/threshold_sweep_vs_classic.png"])
    _plot_best_totals(results, outputs["figures/best_variants_total_pnl.png"])
    _plot_cumulative_best(
        daily, results, outputs["figures/daily_cumulative_best_vs_classic.png"]
    )
    _plot_best_increment(
        daily, results, outputs["figures/daily_increment_best_vs_classic.png"]
    )

    report_path = artifact_dir / "REPORT.md"
    comparison_path = artifact_dir / "threshold_comparison.csv"
    chart_map_path = artifact_dir / "chart_map.json"
    _atomic_text(report_path, _render_markdown(summary, daily, results))
    _comparison_csv(results, comparison_path)
    _atomic_json(chart_map_path, _chart_map())

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "summary_json_sha256": _sha256(artifact_dir / "summary.json"),
            "daily_results_sha256": _sha256(artifact_dir / "daily_results.parquet"),
            "trades_sha256": _sha256(artifact_dir / "trades.parquet"),
        },
        "outputs": {
            "REPORT.md": _sha256(report_path),
            "threshold_comparison.csv": _sha256(comparison_path),
            "chart_map.json": _sha256(chart_map_path),
            **{name: _sha256(path) for name, path in outputs.items()},
        },
        "validation": {
            "daily_rows": len(daily),
            "trade_rows": len(trades),
            "result_groups": len(results),
            "source_hashes_verified": True,
            "artificial_slippage_points_per_side": 0.0,
        },
    }
    _atomic_json(artifact_dir / "report_manifest.json", manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    args = parser.parse_args()
    manifest = render_report(args.artifact_dir.resolve())
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
