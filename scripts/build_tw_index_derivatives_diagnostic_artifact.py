#!/usr/bin/env python3
"""Build the canonical Data Analytics artifact.json for the v5 diagnosis."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import numpy as np


DEFAULT_OUTPUT = Path(
    "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813"
)
DEFAULT_RUN = Path(
    "artifacts/markets/"
    "tw_index_derivatives_day_multi_basis_100m_relative_tenor_v5_dual5090"
)


def _csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _number(row: dict[str, str], field: str) -> float:
    return float(row[field])


def _source(source_id: str, label: str, description: str, path: str) -> dict:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": "duckdb",
            "language": "sql",
            "id": source_id,
            "description": description,
            "sql": f"SELECT * FROM read_csv_auto('{path}', header = true)",
            "tables_used": [path],
            "filters": [
                "Canonical stitched deployment dates 2016-01-04 through 2026-08-04",
                "Twelve expanding walk-forward folds; Fold 12 is latest-year overlap",
                "Initial capital TWD 100,000,000",
            ],
            "metric_definitions": {
                "cumulative_return": "exp(sum daily log return) - 1 over the stated date window",
                "turnover": "gross opening plus closing derivative notional divided by opening NAV",
                "deployment_year": "the first non-overlapping test year owned by each fold",
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN)
    args = parser.parse_args()
    analysis_dir = args.analysis_dir
    result = json.loads((analysis_dir / "analysis_results.json").read_text())
    scenarios = _csv(analysis_dir / "scenario_counterfactuals.csv")
    folds = _csv(analysis_dir / "fold_generalization.csv")
    premiums = _csv(analysis_dir / "option_quality_by_premium.csv")
    versions = _csv(analysis_dir / "model_version_comparison.csv")
    training = _csv(analysis_dir / "training_curve_summary.csv")

    scenario_keep = {
        "actual_user_tax_0.0002",
        "zero_cost",
        "gross_scaled_50pct",
        "gross_scaled_25pct",
        "futures_only_no_renorm",
        "options_only_no_renorm",
        "top_1_keep_cash",
    }
    scenario_rows = [
        {
            "scenario": row["scenario"],
            "cumulative_return": _number(row, "cumulative_return"),
            "cagr": _number(row, "cagr"),
            "sharpe": _number(row, "sharpe"),
            "max_drawdown": _number(row, "max_drawdown"),
            "mean_turnover": _number(row, "mean_turnover"),
            "total_cost_twd_m": _number(row, "total_cost_twd") / 1_000_000.0,
            "terminal_equity_twd_m": _number(row, "terminal_equity_twd") / 1_000_000.0,
        }
        for row in scenarios
        if row["scenario"] in scenario_keep
    ]
    scenario_order = {name: index for index, name in enumerate(scenario_keep)}
    scenario_rows.sort(key=lambda row: scenario_order[row["scenario"]])

    fold_rows = []
    for row in folds:
        if not row["owned_test_year"] or int(row["fold_id"]) > 10:
            continue
        fold_rows.extend(
            [
                {
                    "year": int(row["owned_test_year"]),
                    "sample": "selected validation year",
                    "cumulative_return": _number(row, "val_cumulative_return"),
                },
                {
                    "year": int(row["owned_test_year"]),
                    "sample": "next owned deployment year",
                    "cumulative_return": _number(
                        row, "deployment_cumulative_return"
                    ),
                },
            ]
        )

    premium_rows = []
    for row in premiums:
        premium_rows.extend(
            [
                {
                    "premium_bucket": row["premium_bucket"],
                    "return_basis": "gross first-to-last trade",
                    "mean_return": _number(row, "mean_gross_return"),
                    "rows": int(row["rows"]),
                },
                {
                    "premium_bucket": row["premium_bucket"],
                    "return_basis": "after configured cost proxy",
                    "mean_return": _number(row, "mean_net_return"),
                    "rows": int(row["rows"]),
                },
            ]
        )

    version_rows = [
        {
            "version": row["version"],
            "standalone_fold_compounded_return": _number(
                row, "standalone_fold_compounded_return"
            ),
            "median_owned_year_return": _number(row, "median_owned_year_return"),
            "positive_owned_years": int(row["positive_owned_years"]),
        }
        for row in versions
    ]
    training_rows = [
        {
            "fold_id": int(row["fold_id"]),
            "train_target_days": int(row["train_target_days"]),
            "epochs_ran": int(row["epochs_ran"]),
            "best_epoch": int(row["best_epoch"]),
            "val_loss_std": _number(row, "val_loss_std"),
            "parameters_per_train_day": _number(row, "parameters_per_train_day"),
        }
        for row in training
    ]

    with np.load(args.run_root / "walkforward_deployment_backtest.npz") as payload:
        dates = np.asarray(payload["dates"], dtype="datetime64[D]")
        strategy = np.asarray(payload["strategy_returns"], dtype=np.float64)
        benchmark = np.asarray(payload["benchmark_returns"], dtype=np.float64)
    years = dates.astype("datetime64[Y]").astype(np.int64) + 1970
    annual_rows = []
    for year in np.unique(years):
        mask = years == year
        annual_rows.extend(
            [
                {
                    "year": int(year),
                    "series": "strategy",
                    "cumulative_return": float(math.expm1(strategy[mask].sum())),
                },
                {
                    "year": int(year),
                    "series": "TX rolling buy-and-hold",
                    "cumulative_return": float(math.expm1(benchmark[mask].sum())),
                },
            ]
        )

    generated_at = datetime.now(timezone.utc).isoformat()
    headline_row = {
        "actual_return": result["canonical_actual_metrics"]["cumulative_return"],
        "zero_cost_return": next(
            row["cumulative_return"]
            for row in scenario_rows
            if row["scenario"] == "zero_cost"
        ),
        "benchmark_return": result["benchmark_metrics"]["cumulative_return"],
        "boundary_days": result["action_geometry"]["boundary_fraction_gross_ge_0_979"],
        "option_days": result["action_geometry"]["days_with_any_option_exposure"],
    }
    generated_csvs = {
        "headline_metrics.csv": [headline_row],
        "annual_returns_report.csv": annual_rows,
    }
    for file_name, rows in generated_csvs.items():
        with (analysis_dir / file_name).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    headline_source = _source(
        "headline_metrics",
        "Canonical replay headline metrics",
        "Headline returns and allocation geometry derived by the reproducible diagnostic script.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/headline_metrics.csv",
    )
    annual_source = _source(
        "annual_returns_source", "Canonical stitched annual returns",
        "Annual strategy and rolling TX benchmark returns from the repaired root backtest.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/annual_returns_report.csv",
    )
    scenario_source = _source(
        "scenario_source", "Executor counterfactuals",
        "Cost, risk-budget, and instrument-slot counterfactuals using saved model requests.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/scenario_counterfactuals.csv",
    )
    fold_source = _source(
        "fold_source", "Fold generalization diagnostics",
        "Selected validation metrics and the next independent owned deployment year.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/fold_generalization.csv",
    )
    option_source = _source(
        "option_source", "TXO source quality by opening premium",
        "TAIFEX daily first/last trade proxy quality and configured-cost return bins.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/option_quality_by_premium.csv",
    )
    version_source = _source(
        "version_source", "Model version comparison",
        "Non-overlapping owned-year results for v3, v4, and v5 artifacts.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/model_version_comparison.csv",
    )
    training_source = _source(
        "training_source", "Fold training curve summary",
        "Per-fold epochs, selected epoch, validation volatility, and capacity ratios.",
        "artifacts/analysis/tw_index_derivatives_day_v5_first_principles_20260813/training_curve_summary.csv",
    )
    all_sources = [headline_source, annual_source, scenario_source, fold_source, option_source, version_source, training_source]
    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "台指期衍生日內策略：第一性原理失效診斷",
            "description": (
                "v5 多基底、last-only、projection_l1 策略的資料、數學、成本、選模與執行契約審計"
            ),
            "generatedAt": generated_at,
            "cards": [
                {
                    "id": "actual_return",
                    "dataset": "headline",
                    "sourceId": "headline_metrics",
                    "metrics": [
                        {
                            "label": "完整拼接策略報酬",
                            "field": "actual_return",
                            "format": "percent",
                        }
                    ],
                },
                {
                    "id": "zero_cost_return",
                    "dataset": "headline",
                    "sourceId": "headline_metrics",
                    "metrics": [
                        {
                            "label": "零成本反證報酬",
                            "field": "zero_cost_return",
                            "format": "percent",
                        }
                    ],
                },
                {
                    "id": "benchmark_return",
                    "dataset": "headline",
                    "sourceId": "headline_metrics",
                    "metrics": [
                        {
                            "label": "TX 滾動買進持有",
                            "field": "benchmark_return",
                            "format": "percent",
                        }
                    ],
                },
                {
                    "id": "boundary_days",
                    "dataset": "headline",
                    "sourceId": "headline_metrics",
                    "metrics": [
                        {
                            "label": "貼住 0.98 L1 邊界的日數比例",
                            "field": "boundary_days",
                            "format": "percent",
                        }
                    ],
                },
                {
                    "id": "option_days",
                    "dataset": "headline",
                    "sourceId": "headline_metrics",
                    "metrics": [
                        {
                            "label": "實際選到期權的交易日",
                            "field": "option_days",
                            "format": "number",
                        }
                    ],
                },
            ],
            "charts": [
                {
                    "id": "annual_returns",
                    "title": "逐年策略與 TX 滾動 benchmark 報酬",
                    "subtitle": "2016–2026 canonical stitched deployment；每年獨立複合",
                    "type": "bar",
                    "dataset": "annual_returns",
                    "sourceId": "annual_returns_source",
                    "encodings": {
                        "x": {"field": "year", "type": "ordinal"},
                        "y": {"field": "cumulative_return", "type": "quantitative", "format": "percent"},
                        "color": {"field": "series", "type": "nominal"},
                    },
                },
                {
                    "id": "scenario_returns",
                    "title": "成本、槓桿與資產槽反證",
                    "subtitle": "保留已訓練請求，只改 executor 成本或事後風險預算；不代表重新訓練績效",
                    "type": "bar",
                    "dataset": "scenarios",
                    "sourceId": "scenario_source",
                    "encodings": {
                        "x": {"field": "scenario", "type": "nominal", "sort": "-y"},
                        "y": {"field": "cumulative_return", "type": "quantitative", "format": "percent"},
                    },
                },
                {
                    "id": "fold_transfer",
                    "title": "被選中的 validation 與下一部署年報酬",
                    "subtitle": "Fold 1–10；Fold 11 無獨立年度，Fold 12 與 validation 重疊而排除",
                    "type": "bar",
                    "dataset": "fold_transfer",
                    "sourceId": "fold_source",
                    "encodings": {
                        "x": {"field": "year", "type": "ordinal"},
                        "y": {"field": "cumulative_return", "type": "quantitative", "format": "percent"},
                        "color": {"field": "sample", "type": "nominal"},
                    },
                },
                {
                    "id": "option_premium",
                    "title": "TXO 每日 first-to-last 報酬依開盤權利金分桶",
                    "subtitle": "全資料 executable rows；成本代理使用每邊 22 元、每邊 0.5 點及設定稅率",
                    "type": "bar",
                    "dataset": "option_premium",
                    "sourceId": "option_source",
                    "encodings": {
                        "x": {"field": "premium_bucket", "type": "ordinal"},
                        "y": {"field": "mean_return", "type": "quantitative", "format": "percent"},
                        "color": {"field": "return_basis", "type": "nominal"},
                    },
                },
                {
                    "id": "version_comparison",
                    "title": "v3、v4、v5 非重疊年度拼接報酬",
                    "subtitle": "v5 將 attention/full_then_last 改為 last/last_only；其餘版本仍非完全控制實驗",
                    "type": "bar",
                    "dataset": "versions",
                    "sourceId": "version_source",
                    "encodings": {
                        "x": {"field": "version", "type": "nominal"},
                        "y": {"field": "standalone_fold_compounded_return", "type": "quantitative", "format": "percent"},
                    },
                },
            ],
            "tables": [
                {
                    "id": "scenario_table",
                    "title": "反證實驗精確數值",
                    "dataset": "scenarios",
                    "sourceId": "scenario_source",
                    "defaultSort": {"field": "cumulative_return", "direction": "desc"},
                    "columns": [
                        {"field": "scenario", "label": "情境"},
                        {"field": "cumulative_return", "label": "累積報酬", "format": "percent"},
                        {"field": "cagr", "label": "CAGR", "format": "percent"},
                        {"field": "sharpe", "label": "Sharpe", "format": "number"},
                        {"field": "max_drawdown", "label": "Max DD", "format": "percent"},
                        {"field": "mean_turnover", "label": "日均 turnover", "format": "number"},
                        {"field": "total_cost_twd_m", "label": "總成本 (TWD M)", "format": "number"},
                        {"field": "terminal_equity_twd_m", "label": "期末權益 (TWD M)", "format": "number"},
                    ],
                },
                {
                    "id": "training_table",
                    "title": "各 fold 訓練與 validation 曲線摘要",
                    "dataset": "training",
                    "sourceId": "training_source",
                    "defaultSort": {"field": "fold_id", "direction": "asc"},
                    "columns": [
                        {"field": "fold_id", "label": "Fold"},
                        {"field": "train_target_days", "label": "Train target days"},
                        {"field": "epochs_ran", "label": "Epochs"},
                        {"field": "best_epoch", "label": "Best epoch"},
                        {"field": "val_loss_std", "label": "Val loss std", "format": "number"},
                        {"field": "parameters_per_train_day", "label": "Params / train day", "format": "number"},
                    ],
                },
            ],
            "blocks": [
                {"id": "title", "type": "markdown", "body": "# 台指期衍生日內策略：第一性原理失效診斷"},
                {
                    "id": "summary",
                    "type": "markdown",
                    "body": (
                        "## 技術摘要\n\n"
                        "策略表現差的第一原因不是手續費，而是 out-of-sample 訊號沒有正期望：完整 2016–2026 單一帳戶報酬為 **-98.49%**；把所有費用、稅與滑價設為零仍為 **-96.66%**。現行成交路徑的毛損益為 -61.92M TWD，成本再扣 36.57M TWD。成本很重要，但無法把負 alpha 解釋成正 alpha。\n\n"
                        "第二原因是 allocation 幾何：4,102 個分數經 Euclidean L1 projection 後，100% 日子都貼住 0.98 gross 邊界，平均只留下 1.62 腿，最大一腿平均占 87.4%。這是近似 winner-take-all，不是分散投資。期權只在 15 天被選中，卻在那些日子複利 -93.9%。\n\n"
                        "第三原因是選模失真：Fold 1–10 的 validation 報酬與下一個獨立部署年報酬 Pearson r=-0.931；10 個獨立部署年全部為負。2024 validation +88.0% 的 checkpoint 在 2025 部署 -95.2%，是最直接的 winner's curse 證據。"
                    ),
                },
                {"id": "metrics", "type": "metric-strip", "cardIds": ["actual_return", "zero_cost_return", "benchmark_return", "boundary_days", "option_days"]},
                {
                    "id": "annual_interpretation",
                    "type": "markdown",
                    "sourceId": "annual_returns_source",
                    "body": (
                        "## 真正的 walk-forward 是每年失血，2025 發生尾端崩潰\n\n"
                        "修正逐-fold 子程序覆寫根報表的缺陷後，canonical stitched account 包含 2,578 個交易日，而不是原先只顯示的 2026 年 140 日。2016–2025 每個獨立部署年都虧損；2026 為 Fold 12 的 validation=test 最新年實驗，不能當成無偏模型選擇證據。"
                    ),
                },
                {"id": "annual_chart_block", "type": "chart", "chartId": "annual_returns"},
                {
                    "id": "counterfactual_interpretation",
                    "type": "markdown",
                    "sourceId": "scenario_source",
                    "body": (
                        "## 零成本仍失敗，表示要先修訊號與選模，再談費率微調\n\n"
                        "零成本反證仍虧 96.66%，只做期貨也虧 76.66%，只保留期權原權重則虧 94.08%。把 gross 降為 25% 可把損失縮到 58.27%，證明風險預算能減少傷害，但沒有創造 alpha。Top-1 留現金同樣仍虧 79.73%。所有反證都沿用既有模型請求，因此只回答損失機制，不是 v6 重新訓練結果。"
                    ),
                },
                {"id": "scenario_chart_block", "type": "chart", "chartId": "scenario_returns"},
                {"id": "scenario_table_block", "type": "table", "tableId": "scenario_table"},
                {
                    "id": "selection_interpretation",
                    "type": "markdown",
                    "sourceId": "fold_source",
                    "body": (
                        "## 單一年 validation 的最佳 epoch 是反向指標，不是部署保證\n\n"
                        "每個 fold 都從最多 1,000 epochs 中挑 validation 最小值，等同對高噪聲的一年樣本反覆做多重比較。627,796 個參數不是單獨的定罪證據；但經濟上獨立的市場狀態仍主要沿時間軸增加，Fold 1 只有 217 個 train target days。觀察到的 validation→下一年負相關，才是過度選模已實際發生的證據。"
                    ),
                },
                {"id": "fold_chart_block", "type": "chart", "chartId": "fold_transfer"},
                {"id": "training_table_block", "type": "table", "tableId": "training_table"},
                {
                    "id": "option_interpretation",
                    "type": "markdown",
                    "sourceId": "option_source",
                    "body": (
                        "## 低價期權的成本是非線性的，daily open/last trade 又不是可成交 bid/ask\n\n"
                        "固定成本率為 44/(50×open) 加上 1/open 點滑價，因此權利金越低，成本占 premium 越大。全資料中 open<1 的 executable rows 平均毛報酬約 +9.4%，但成本代理後平均 -744%。此外 TAIFEX daily open 與最後成交價只是第一筆與最後一筆成交；終端 bid/ask 欄位不能證明策略能在開盤 ask 買入並於收盤 bid 賣出。此資料只能當研究 proxy，不能當可交易性驗證。"
                    ),
                },
                {"id": "premium_chart_block", "type": "chart", "chartId": "option_premium"},
                {
                    "id": "benchmark_interpretation",
                    "type": "markdown",
                    "body": (
                        "## Benchmark 本身正確，但與策略承擔的時段不同\n\n"
                        "依需求保留 TX front-month 滾動買進持有：2016–2026 累積 +692.85%。同期間單純每天做多 TX open→close 只有 +14.19%。差異主要來自 close→next close 的持有時段與長期 beta，而策略每天收盤前歸零。因此 `excess vs benchmark` 是資本配置比較，不是同風險時段的純 alpha 歸因；它解釋巨大 excess 落差，但不能解釋策略自身 -98.49%。"
                    ),
                },
                {
                    "id": "architecture_interpretation",
                    "type": "markdown",
                    "sourceId": "version_source",
                    "body": (
                        "## `last_only` 加快計算，但這批實驗沒有改善泛化\n\n"
                        "非完全控制的歷史版本比較中，v4 relative-tenor + attention/full_then_last 的非重疊年度拼接約 -35.3%，v5 last/last_only 約 -98.5%。這不能把差異全歸因於 pooling，卻足以否定『更快所以至少不傷表現』。若維持 last_only，必須把它視為速度限制條件，而不是已驗證的預測結構。"
                    ),
                },
                {"id": "version_chart_block", "type": "chart", "chartId": "version_comparison"},
                {
                    "id": "definitions",
                    "type": "markdown",
                    "body": (
                        "## 範圍、資料與指標定義\n\n"
                        "- 決策資訊：股票與公共特徵完成至 t-1；lookback=32；98 原始特徵與 18×4 基底係數共用 CandleEncoder 投影。\n"
                        "- 執行軸：6 個 E1–E6 期貨 tenor 加 4,096 個前一交易日已知 TXO 候選；股票只提供訊號，不能成交。\n"
                        "- 報酬：期貨與長期權均以一般交易時段 open→close 日內平倉；整數口數、固定費、稅與滑價逐日更新一億元帳戶。\n"
                        "- Honest deployment：Fold 1–10 各自擁有下一個不重疊年度；Fold 11 的 2026 由 Fold 12 接管，因此無 owned rows；Fold 12 是 latest-year validation=test 實驗。"
                    ),
                },
                {
                    "id": "methods",
                    "type": "markdown",
                    "body": (
                        "## 方法與反證設計\n\n"
                        "分析直接讀取每 fold 的 `requested_weights_history`，重新拼接日期後送入同一 canonical integer executor；保存檔與直接 replay 的最大單日 log-return 差為 8.75e-8。成本反證將費、稅、滑價歸零；槓桿、top-k、期貨/期權槽反證不重新正規化，未使用額度留現金。資料品質檢查涵蓋 2,690,738 個 TXO contract-day keys，重複 key 為 0，49.37% 有正 open/close 且被標記 executable。"
                    ),
                },
                {
                    "id": "external_evidence",
                    "type": "markdown",
                    "body": (
                        "## 外部一手證據如何改變判讀\n\n"
                        "TAIFEX [費率表](https://www.taifex.com.tw/cht/4/feeSchedules) 列示 TXO 權利金交易稅率 0.001，而現行研究設定依先前指定使用 0.0002 且只在 closing sale 計算；這是明確的 realism mismatch，但本資料中只把既有 close-only 稅率改為 0.001 對總結果影響很小。TAIFEX [每日行情](https://www.taifex.com.tw/cht/3/optDailyMarketReport) 明確分列開盤、最後成交與最後最佳買賣價，支持『daily first/last trade 不是同步可成交 quote』的限制。\n\n"
                        "White 的 [Reality Check](https://doi.org/10.1111/1468-0262.00152) 將重複用同一歷史做規格搜尋視為 data snooping；Bailey 等人的 [PBO 論文](https://scholarworks.wmich.edu/math_pubs/42/) 說明單一 hold-out 在投資回測選模中可能不可靠。這些文獻支持改用 nested / combinatorial temporal validation，但本報告的根因排序仍以本專案的直接反證為主。"
                    ),
                },
                {
                    "id": "limitations",
                    "type": "markdown",
                    "body": (
                        "## 限制、不確定性與不能宣稱的事\n\n"
                        "本報告證明 v5 儲存模型在現有 proxy 與 executor 下失敗，不能證明任何特定 v6 一定獲利。零成本、降槓桿與 top-k 是反事實 executor 測試，不是重新訓練。v3/v4/v5 比較同時含資料與程式版本差異，僅能作方向性 evidence。Daily option 資料無法重建開盤 ask、收盤 bid、排隊、價量深度與實際成交時間；在 tick/bid-ask dataset 完成前，所有 TXO 結論都只能標為研究 proxy。"
                    ),
                },
                {
                    "id": "implemented",
                    "type": "markdown",
                    "body": (
                        "## 已實作的修正與下一個嚴格實驗\n\n"
                        "1. Parent 在 isolated folds 全部完成後重建全期 canonical stitched account，並用 2,578 日 replay 取代 140 日錯誤根報表。\n"
                        "2. 衍生日內集中度改讀 4,102 維 requested actions，不再讀收盤後必為零的 stock carried weights。\n"
                        "3. 新增 `gated_v6` fresh-root config：保留 last + last_only + projection_l1 作方向與稀疏選腿；投影後乘可學習 capital gate，初始 sigmoid(-2)=11.9%；長期權另設 5% NAV cap，未使用額度留現金。舊 v5 預設與 checkpoint 參數完全不變。\n"
                        "4. v6 只有在 nested temporal selection、cash baseline、官方稅務語意與 tick bid/ask executor 全部通過後，才值得跑完整 12 folds。首要 acceptance gate 應是每個獨立部署年的 worst-year、median-year、PBO/selection stability，而不是單一年 best validation。"
                    ),
                },
                {
                    "id": "questions",
                    "type": "markdown",
                    "body": (
                        "## 尚待回答的問題\n\n"
                        "- 使用 synchronized bid/ask tick 重建同一 15 個 option days 後，極端損失是否更差？\n"
                        "- v6 capital gate 在完全不做交易的 cash baseline 下，是否能於多個 validation blocks 穩定選擇低曝險？\n"
                        "- 恢復 attention/full_then_last 的額外計算成本，是否換回足以覆蓋成本的 out-of-sample 改善？\n"
                        "- 若 benchmark 仍固定為 TX rolling buy-and-hold，策略是否應允許隔夜 beta，而不是只交易 open→close？"
                    ),
                },
            ],
            "sources": all_sources,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "headline": [
                    headline_row
                ],
                "annual_returns": annual_rows,
                "scenarios": scenario_rows,
                "fold_transfer": fold_rows,
                "option_premium": premium_rows,
                "versions": version_rows,
                "training": training_rows,
            },
        },
        "sources": all_sources,
    }
    output = analysis_dir / "artifact.json"
    output.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(output)


if __name__ == "__main__":
    main()
