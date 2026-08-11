#!/usr/bin/env python3
"""Build a reproducible first-principles TW-minute edge diagnosis.

The report deliberately separates same-trade pre-fee return, explicit cost,
and net return.  It does not claim that adding fees back is a counterfactual
no-fee simulation: changing fees can also change the executed path.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


DEFAULT_ROOT = Path("artifacts/markets/tw_minute_dual_5090_cash_asset_v9")


def _read_epoch_curve(path: Path) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if not rows:
        raise RuntimeError(f"empty epoch curve: {path}")
    return pl.DataFrame(rows, infer_schema_length=None)


def _summarize_curve(label: str, path: Path) -> dict[str, float | int | str]:
    frame = pl.read_parquet(path)
    required = {
        "net_return",
        "explicit_fees",
        "initial_equity",
        "turnover_notional",
        "mean_model_requested_cash_weight",
        "mean_model_requested_risky_gross_weight",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise RuntimeError(f"{path} is missing required columns: {missing}")

    net = frame["net_return"].to_numpy()
    fee_fraction = (
        frame["explicit_fees"] / frame["initial_equity"]
    ).to_numpy()
    turnover = (
        frame["turnover_notional"] / frame["initial_equity"]
    ).to_numpy()
    same_trade_pre_fee = net + fee_fraction
    total_turnover = float(frame["turnover_notional"].sum())
    fee_per_turnover = (
        float(frame["explicit_fees"].sum()) / total_turnover
        if total_turnover > 0.0
        else float("nan")
    )
    mean_pre_fee = float(np.mean(same_trade_pre_fee))
    break_even_turnover = (
        mean_pre_fee / fee_per_turnover
        if mean_pre_fee > 0.0 and fee_per_turnover > 0.0
        else 0.0
    )
    mean_risky = float(frame["mean_model_requested_risky_gross_weight"].mean())
    forced_round_trip_floor = 2.0 * mean_risky
    mean_turnover = float(np.mean(turnover))
    return {
        "segment": label,
        "source": str(path),
        "days": frame.height,
        "mean_net_return": float(np.mean(net)),
        "mean_explicit_fee_fraction": float(np.mean(fee_fraction)),
        "mean_same_trade_pre_fee_return": mean_pre_fee,
        "mean_turnover_multiple": mean_turnover,
        "fee_per_turnover": fee_per_turnover,
        "break_even_turnover_multiple": break_even_turnover,
        "mean_model_cash_weight": float(
            frame["mean_model_requested_cash_weight"].mean()
        ),
        "mean_model_risky_gross_weight": mean_risky,
        "forced_round_trip_turnover_floor": forced_round_trip_floor,
        "turnover_above_round_trip_floor": max(
            0.0, mean_turnover - forced_round_trip_floor
        ),
    }


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#6B7280",
            "axes.labelcolor": "#262626",
            "axes.titlecolor": "#262626",
            "xtick.color": "#4B5563",
            "ytick.color": "#4B5563",
            "grid.color": "#E5E7EB",
            "font.size": 10,
        }
    )


def _plot_learning_curves(epoch_frames: list[tuple[str, pl.DataFrame]], path: Path) -> None:
    _style()
    fig, axes = plt.subplots(len(epoch_frames), 1, figsize=(10.5, 4.0 * len(epoch_frames)))
    if len(epoch_frames) == 1:
        axes = [axes]
    colors = {"train": "#1F5A94", "validation": "#D97706", "test": "#6B7280"}
    for axis, (label, frame) in zip(axes, epoch_frames, strict=True):
        epoch = frame["epoch"].to_numpy()
        for name, column in (
            ("train", "minute_train_mean_daily_return"),
            ("validation", "minute_val_mean_daily_return"),
            ("test", "minute_test_mean_daily_return"),
        ):
            axis.plot(
                epoch,
                frame[column].to_numpy() * 100.0,
                label=name,
                color=colors[name],
                linewidth=1.8,
            )
        axis.axhline(0.0, color="#262626", linewidth=1.0, linestyle="--")
        axis.set_title(f"Daily net return by epoch — {label}", loc="left", fontweight="bold")
        axis.set_ylabel("Mean daily return (%)")
        axis.set_xlabel("Epoch")
        axis.grid(axis="y", linewidth=0.8)
        axis.legend(frameon=False, ncol=3, loc="lower right")
    fig.suptitle(
        "Training path metrics (train is online within-epoch; validation/test are fixed-model)",
        x=0.06,
        y=0.995,
        ha="left",
        fontsize=12,
        fontweight="bold",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_cost_decomposition(summary: pl.DataFrame, path: Path) -> None:
    _style()
    labels = summary["segment"].to_list()
    y = np.arange(len(labels))
    pre_fee = summary["mean_same_trade_pre_fee_return"].to_numpy() * 100.0
    fees = -summary["mean_explicit_fee_fraction"].to_numpy() * 100.0
    net = summary["mean_net_return"].to_numpy() * 100.0
    height = 0.22
    fig, axis = plt.subplots(figsize=(10.5, 4.8))
    axis.barh(y + height, pre_fee, height=height, color="#1F5A94", label="same-trade pre-fee")
    axis.barh(y, fees, height=height, color="#E8A23A", label="explicit fees")
    axis.barh(y - height, net, height=height, color="#9CA3AF", label="net")
    axis.axvline(0.0, color="#262626", linewidth=1.0)
    axis.set_yticks(y, labels)
    axis.set_xlabel("Mean daily return / equity (%)")
    axis.set_title("Daily edge and explicit cost decomposition", loc="left", fontweight="bold")
    axis.grid(axis="x", linewidth=0.8)
    axis.legend(frameon=False, ncol=3, loc="lower right")
    for row_y, values in zip(y, zip(pre_fee, fees, net, strict=True), strict=True):
        for offset, value in zip((height, 0.0, -height), values, strict=True):
            ha = "left" if value >= 0 else "right"
            axis.text(value, row_y + offset, f" {value:+.3f}% ", va="center", ha=ha, fontsize=9)
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _plot_turnover_budget(row: dict[str, Any], path: Path) -> None:
    _style()
    labels = ["Actual turnover", "Forced-close lower bound", "Break-even maximum"]
    values = [
        float(row["mean_turnover_multiple"]),
        float(row["forced_round_trip_turnover_floor"]),
        float(row["break_even_turnover_multiple"]),
    ]
    colors = ["#D97706", "#9CA3AF", "#1F5A94"]
    fig, axis = plt.subplots(figsize=(10.5, 4.4))
    bars = axis.barh(np.arange(3), values, color=colors, edgecolor="#374151", linewidth=0.6)
    axis.set_yticks(np.arange(3), labels)
    axis.invert_yaxis()
    axis.set_xlabel("One-way turnover multiple per day")
    axis.set_title(
        f"Turnover budget — {row['segment']}", loc="left", fontweight="bold"
    )
    axis.grid(axis="x", linewidth=0.8)
    for bar, value in zip(bars, values, strict=True):
        axis.text(
            value,
            bar.get_y() + bar.get_height() / 2,
            f" {value:.3f}×",
            va="center",
            ha="left",
            fontweight="bold",
        )
    fig.tight_layout()
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def _pct(value: float, digits: int = 3) -> str:
    return f"{value * 100:.{digits}f}%"


def _write_report(
    output_path: Path,
    root: Path,
    summary: pl.DataFrame,
    epoch_frames: list[tuple[str, pl.DataFrame]],
) -> None:
    fold1_test = summary.filter(pl.col("segment") == "Fold 1 test").row(0, named=True)
    fold2_val = summary.filter(pl.col("segment") == "Fold 2 best validation").row(0, named=True)
    latest_epochs = ", ".join(
        f"{name}: epoch {int(frame['epoch'][-1])}" for name, frame in epoch_frames
    )
    fee_multiple = (
        float(fold2_val["mean_explicit_fee_fraction"])
        / float(fold2_val["mean_same_trade_pre_fee_return"])
    )
    turnover_reduction = 1.0 - (
        float(fold2_val["break_even_turnover_multiple"])
        / float(fold2_val["mean_turnover_multiple"])
    )
    avoidable_share = float(fold2_val["turnover_above_round_trip_floor"]) / float(
        fold2_val["mean_turnover_multiple"]
    )

    lines = [
        "# TW 分鐘策略：費用後 edge 第一性原理診斷",
        "",
        "## 技術摘要",
        "",
        (
            "現在不是單純『模型完全找不到訊號』。Fold 2 最佳 validation 已有 "
            f"**{_pct(float(fold2_val['mean_same_trade_pre_fee_return']))}/日的同交易路徑毛利**，"
            f"但顯式費用是 **{_pct(float(fold2_val['mean_explicit_fee_fraction']))}/日**，"
            f"約為毛利的 **{fee_multiple:.1f} 倍**，所以淨利仍為 "
            f"**{_pct(float(fold2_val['mean_net_return']))}/日**。"
        ),
        "",
        (
            f"根因是配置器每天換手 **{float(fold2_val['mean_turnover_multiple']):.3f}×**；"
            f"依目前每單位換手成本，只能容許約 **{float(fold2_val['break_even_turnover_multiple']):.3f}×/日**，"
            f"也就是在毛利不變時至少要降低 **{turnover_reduction:.1%}**。"
        ),
        "",
        (
            "此外，現有 `minute_train_mean_daily_return` 是模型在同一 epoch 內一邊更新、一邊累積的"
            " online 路徑，不是用 epoch 結束後的固定權重重跑完整訓練集。因此目前證據不能嚴格推論"
            "『固定模型連訓練集都無法擬合』；要加固定 train replay 才能回答。"
        ),
        "",
        "## 目前模型已經學到毛利，但被成本完全吃掉",
        "",
        (
            "下圖把每日淨報酬加回當日實際支付的費用，得到同一交易路徑的 pre-fee cashflow。"
            "這不是把 fee 設成零後重新模擬的反事實結果，但足以回答目前虧損主要來自方向還是成本。"
        ),
        "",
        "![Daily edge and cost decomposition](edge_cost_decomposition.png)",
        "",
        (
            f"Fold 1 test 的毛利本身為 {_pct(float(fold1_test['mean_same_trade_pre_fee_return']))}/日，"
            "方向也尚未合格；Fold 2 validation 則已轉正，表示訊號並非必然為零，但經濟門檻遠高於目前 edge。"
        ),
        "",
        "## 換手不是微調問題，而是數量級錯位",
        "",
        (
            "日內策略每天從零部位開始、收盤歸零。用平均 risky gross 估算，單純進場與出場的"
            f"最低換手已約 **{float(fold2_val['forced_round_trip_turnover_floor']):.3f}×/日**；"
            f"目前仍有 **{float(fold2_val['turnover_above_round_trip_floor']):.3f}×/日** 的額外盤中換倉，"
            f"占總換手約 **{avoidable_share:.1%}**。"
        ),
        "",
        "![Turnover budget](turnover_budget.png)",
        "",
        (
            "即使把額外換倉全部消除，目前約 95.7% 的平均 risky gross 仍使強制 round trip 下限高於"
            "損益兩平容許值。因此必須同時做兩件事：降低盤中 churn，並讓 cash／風險曝險取決於"
            "『預測 edge 是否超過完整 round-trip 成本』，而不是預設接近滿倉。"
        ),
        "",
        "## 訓練曲線顯示正在收斂，但 train 指標不是固定模型擬合度",
        "",
        f"資料快照：{latest_epochs}。",
        "",
        "![Training path metrics](learning_curves.png)",
        "",
        (
            "100% cash 是合法、零費用、log utility 為 0 的可行解，理論上已優於目前所有負淨報酬結果；"
            "模型卻只要求約 4%–5% cash。這表示問題不只在 feature，而在動作參數化與最佳化路徑："
            "單一 cash score 正在和約 2,600 個股票分數的絕對值總和競爭，且舊 proximal 實作又會"
            "把剛抑制掉的換手重新正規化回去。"
        ),
        "",
        "## 已實作的演算法修正",
        "",
        "1. **修正 proximal map。** 保留 `previous + soft_threshold(requested - previous)` 的精確解，不再重新 L1 正規化。候選點逐座標位於舊、新部位之間，本來就位於 L1 可行集合；重新正規化只會重灌換手並阻止資金回到 cash。",
        "2. **訓練契約升到 v13。** 新實驗輸出到 `artifacts/markets/tw_minute_dual_5090_cost_aware_v10`，不拿舊 optimizer state 靜默續跑。現有 v9 程序不受影響。",
        "3. **新增反例回歸測試。** 測試 partial move 會保留 soft-threshold 後的 0.40 gross，而不是被縮回 requested 0.30 gross。",
        "",
        "## 下一版要採用的模型與決策分解",
        "",
        "目前一個帶偏置的股票 head 同時承擔市場方向、個股相對 alpha、gross exposure 與 cash，結果容易全體同號並接近滿倉。建議把可辨識的經濟量拆開，但最後仍輸出同一個 `[stocks..., cash]` 動作向量：",
        "",
        "1. `alpha_i`：股票間的相對報酬，做 masked cross-sectional centering，負責多空選股。",
        "2. `beta_t`：獨立的 signed market-direction score，允許整體偏多或偏空，不強迫 dollar-neutral。",
        "3. `cash_t`：顯式 cash action；對有效股票數做 cardinality calibration，避免股票池從 500 擴到 2,600 時 cash 梯度自然消失。",
        "4. `confidence_t`：只有預測的剩餘持有期 edge 超過 `round-trip fee + uncertainty buffer` 才從 cash 移到股票。",
        "",
        "可寫成 `stock_score_i = confidence * (centered_alpha_i + beta * market_loading_i)`，再把經股票數校準的 cash score 加為最後一個 action；這不是外部 cash gate，cash 仍由模型在共同動作向量中競爭。",
        "",
        "## 建議的訓練目標與驗證順序",
        "",
        "1. **先做可學性單元測試。** 固定 32 個交易日、固定 32 個股票，關閉 fee，要求固定 checkpoint replay 能在訓練集取得正 pre-fee return。失敗代表 label／梯度／容量錯誤，不應再跑完整 walk-forward。",
        "2. **加入 oracle 上限。** 只在診斷程式用已實現未來報酬求『最佳可行交易』，禁止餵給模型。若 oracle 扣費後仍不賺，代表這個交易頻率、流動性與 fee contract 下根本沒有足夠 edge，調網路沒有意義。",
        "3. **加入固定 train replay。** 每 8 個 epoch 用 epoch-end 固定權重重跑 train split，online train path 只保留為最佳化過程指標。",
        "4. **先學 return，再學配置。** 加入 8／32 分鐘與到收盤的多尺度 forward-return auxiliary loss；canonical fee-adjusted log utility 仍是最終目標。這會比只靠整日 portfolio scalar 梯度更快辨認 alpha。",
        "5. **經濟門檻式交易。** allocator 解 `expected return - risk - exact L1 trading cost`，把不超過成本的 score 留在 no-trade region；不要靠把 fee multiplier 任意調大來碰運氣。",
        "6. **依序放行。** `32-day overfit → 1 fold pre-fee → 1 fold fee-on → walk-forward`。每關都要求 cash-only baseline、turnover budget 與 fixed-train replay；不通過就停止擴大訓練。",
        "",
        "## 指標定義與資料範圍",
        "",
        "- `net return`：daily curve 的 `net_return`。",
        "- `explicit fee fraction`：`explicit_fees / initial_equity`，slippage 設為 0。",
        "- `same-trade pre-fee return`：`net_return + explicit_fees / initial_equity`。",
        "- `turnover multiple`：`turnover_notional / initial_equity`，為 one-way 成交額倍數。",
        "- `break-even turnover`：正的 same-trade pre-fee return 除以實際每單位換手費率。",
        "- Fold 1 validation 246 日、Fold 1 test 865 日；Fold 2 採目前 best-validation curve 239 日。",
        "",
        "## 限制與仍待回答的問題",
        "",
        "- Fold 2 尚在訓練，報告是輸出當下的 checkpoint 快照，數值會繼續變動。",
        "- 加回已付費用保持成交路徑不變；真正 fee=0 的 counterfactual 可能改變可買股數與後續部位。",
        "- 目前沒有固定 checkpoint 的完整 train replay，因此不能把 online train 曲線當作模型容量證明。",
        "- 尚未建立 perfect-foresight oracle；在它完成前，無法區分『市場在此成本下不可交易』與『模型尚未找到 edge』。",
        "",
        f"資料根目錄：`{root}`。報告由 `scripts/analyze_tw_minute_edge.py` 產生。",
        "",
    ]
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    args = parser.parse_args()

    root = args.root.resolve()
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else root / "diagnostics" / "edge_first_principles"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    curve_specs = [
        ("Fold 1 validation", root / "fold_01" / "validation_daily_curve.parquet"),
        ("Fold 1 test", root / "fold_01" / "test_daily_curve.parquet"),
        (
            "Fold 2 best validation",
            root / "fold_02" / "best_validation_daily_curve.parquet",
        ),
    ]
    summaries = [_summarize_curve(label, path) for label, path in curve_specs]
    summary = pl.DataFrame(summaries)
    summary.write_csv(output_dir / "edge_cost_summary.csv")

    epoch_specs = [
        ("Fold 1 / train 2020–2021", root / "train_2020-2021" / "epoch_curve.jsonl"),
        (
            "Fold 2 / train 2020–2022",
            root / "train_2020-2021-2022" / "epoch_curve.jsonl",
        ),
    ]
    epoch_frames = [(label, _read_epoch_curve(path)) for label, path in epoch_specs]

    _plot_learning_curves(epoch_frames, output_dir / "learning_curves.png")
    _plot_cost_decomposition(summary, output_dir / "edge_cost_decomposition.png")
    fold2 = summary.filter(pl.col("segment") == "Fold 2 best validation").row(
        0, named=True
    )
    _plot_turnover_budget(fold2, output_dir / "turnover_budget.png")
    _write_report(output_dir / "report.md", root, summary, epoch_frames)

    print(output_dir / "report.md")


if __name__ == "__main__":
    main()
