#!/usr/bin/env python3
"""Build the canonical portable-report artifact for TXO benchmark controls."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Final

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.backtest_taifex_atm_straddle_rolling import _sha256_path  # noqa: E402
from stockagent.data.tw_index_derivatives_tick import _atomic_json  # noqa: E402


CLASSIC: Final[str] = "classic_opening_straddle"
CANDIDATE_FAMILY: Final[str] = "single_leg_roll_candidate"
RANDOM_FAMILY: Final[str] = "random_roll_control"
UNDERLYING_FAMILY: Final[str] = "no_option_underlying"
OPTION_FAMILIES: Final[tuple[str, ...]] = (
    "buy_hold_atm_straddle",
    "atm_straddle_fixed_tp_sl",
    "full_recenter_straddle",
    "long_strangle",
    "gamma_scalping",
    "delta_band_gamma_scalping",
    "time_based_recenter",
    "trailing_ratchet_roll",
    "random_roll_control",
    "single_leg_roll_candidate",
)
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
    "single_leg_roll_candidate": "Single-leg roll",
}


def _source(
    source_id: str,
    label: str,
    path: str,
    *,
    description: str,
    executed_at: str,
    sql: str,
) -> dict[str, Any]:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": "python-polars",
            "language": "python",
            "sql": sql,
            "description": description,
            "executed_at": executed_at,
            "tables_used": [path],
            "filters": {
                "date_window": "2026-06-25 through 2026-08-06",
                "session": "TAIFEX day session",
                "execution": "one-lot guaranteed transaction-print proxy",
            },
            "metric_definitions": {
                "net_after_fee_twd": "Gross intraday cash flow minus product-specific fixed contract-side fees.",
                "paired_delta_vs_classic_twd": "Same-date net-after-fee P&L minus classic opening ATM straddle P&L.",
                "maximum_drawdown_after_fee_twd": "Minimum drawdown of cumulative daily net-after-fee P&L from its prior peak, including a zero starting point.",
            },
        },
    }


def _signed_twd(value: float) -> str:
    return f"{value:+,.0f} TWD"


def _result_map(results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row["variant_id"]): row for row in results}


def _best(
    results: list[dict[str, Any]], family: str, *, predicate: Any | None = None
) -> dict[str, Any]:
    candidates = [row for row in results if row["benchmark_family"] == family]
    if predicate is not None:
        candidates = [row for row in candidates if predicate(row)]
    if not candidates:
        raise ValueError(f"no result rows for family={family}")
    return max(candidates, key=lambda row: float(row["net_after_fee_twd"]))


def _table_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "family": FAMILY_LABELS.get(
            str(row["benchmark_family"]), str(row["benchmark_family"])
        ),
        "variant_id": str(row["variant_id"]),
        "parameters": json.dumps(
            row.get("parameters", {}),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "net_after_fee_twd": float(row["net_after_fee_twd"]),
        "delta_vs_classic_twd": float(row["paired_delta_vs_classic_twd"]),
        "max_drawdown_twd": float(row["maximum_drawdown_after_fee_twd"]),
        "win_rate": float(row["win_rate_after_fee"]),
        "fixed_fees_twd": float(row["fixed_fees_twd"]),
        "trade_sides": int(row["total_trade_sides"]),
        "rolls": int(row["total_rolls"]),
        "recenters": int(row["total_recenters"]),
        "hedges": int(row["total_hedges"]),
    }


def build_artifact(*, input_dir: Path, artifact_path: Path) -> dict[str, Any]:
    summary_path = input_dir / "summary.json"
    daily_path = input_dir / "daily_benchmarks.parquet"
    trades_path = input_dir / "trades.parquet"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("status") != "complete":
        raise ValueError("benchmark summary is not complete")
    if summary.get("benchmark_family_count") != 10:
        raise ValueError("the required 10 benchmark families are not complete")
    if summary.get("variant_count") != 160:
        raise ValueError("unexpected benchmark variant count")
    if summary.get("fixed_fees_per_contract_side_twd") != {
        "TX": 60.0,
        "MTX": 24.0,
        "TXO": 22.0,
        "TMF": 16.0,
    }:
        raise ValueError("product-specific fixed fee schedule mismatch")
    if summary["parameters"]["artificial_slippage_points_per_side"] != 0.0:
        raise ValueError("artificial slippage must remain disabled")
    for key, path in (
        ("daily_benchmarks_sha256", daily_path),
        ("trades_sha256", trades_path),
    ):
        if _sha256_path(path) != summary["artifacts"][key]:
            raise ValueError(f"source artifact hash mismatch: {path}")

    results = list(summary["results"])
    by_id = _result_map(results)
    classic = by_id[CLASSIC]
    best_candidate = _best(results, CANDIDATE_FAMILY)
    best_gamma = _best(results, "gamma_scalping")
    best_strangle = _best(results, "long_strangle")
    best_random = _best(results, RANDOM_FAMILY)
    best_ratchet = _best(results, "trailing_ratchet_roll")
    underlying_mtx_short = by_id["underlying__mtx__short"]
    underlying_mtx_long = by_id["underlying__mtx__long"]
    candidate_parameters = best_candidate["parameters"]
    matched_random_id = (
        "random_control__"
        f"{candidate_parameters['source_strategy']}__"
        f"{int(candidate_parameters['rolling_points']):04d}"
    )
    matched_random = by_id[matched_random_id]
    candidate_vs_random = float(best_candidate["net_after_fee_twd"]) - float(
        matched_random["net_after_fee_twd"]
    )

    comparable_rows: list[dict[str, Any]] = []
    for family in OPTION_FAMILIES:
        comparable_rows.append(_table_row(_best(results, family)))
    comparable_rows.extend(
        [_table_row(underlying_mtx_short), _table_row(underlying_mtx_long)]
    )
    comparable_rows.sort(key=lambda row: row["net_after_fee_twd"], reverse=True)
    for rank, row in enumerate(comparable_rows, start=1):
        row["rank"] = rank

    full_rows = [_table_row(row) for row in results]
    full_rows.sort(key=lambda row: row["net_after_fee_twd"], reverse=True)
    for rank, row in enumerate(full_rows, start=1):
        row["rank"] = rank
    summary_sql = "SELECT * FROM benchmark_summary"
    summary_context = pl.SQLContext(
        benchmark_summary=pl.DataFrame(full_rows), eager=True
    )
    full_rows = summary_context.execute(summary_sql).to_dicts()

    daily_sql = (
        "SELECT trading_date, variant_id, net_after_fee_twd "
        "FROM daily_benchmarks"
    )
    daily_context = pl.SQLContext(
        daily_benchmarks=pl.scan_parquet(daily_path), eager=True
    )
    daily = daily_context.execute(daily_sql)
    chosen = [
        (CLASSIC, "Classic ATM"),
        (str(best_candidate["variant_id"]), "Best single-leg roll"),
        (str(best_gamma["variant_id"]), "Gamma MTX"),
        (str(best_strangle["variant_id"]), "Strangle 150"),
        (matched_random_id, "Matched random control"),
    ]
    cumulative_rows: list[dict[str, Any]] = []
    for variant_id, label in chosen:
        frame = daily.filter(pl.col("variant_id") == variant_id).sort("trading_date")
        cumulative = frame["net_after_fee_twd"].cum_sum().to_list()
        for trading_date, value in zip(frame["trading_date"].to_list(), cumulative):
            cumulative_rows.append(
                {
                    "trading_date": trading_date.isoformat(),
                    "series": label,
                    "cumulative_pnl_twd": float(value),
                    "variant_id": variant_id,
                }
            )

    threshold_rows: list[dict[str, Any]] = []
    for strategy, label in (
        ("roll_otm_put_keep_itm_call", "Roll OTM candidate"),
        ("roll_itm_call_keep_otm_put", "Roll ITM candidate"),
    ):
        for threshold in range(50, 1001, 50):
            candidate = by_id[f"{strategy}__{threshold:04d}"]
            random = by_id[f"random_control__{strategy}__{threshold:04d}"]
            threshold_rows.extend(
                [
                    {
                        "threshold": threshold,
                        "series": label,
                        "net_after_fee_twd": float(candidate["net_after_fee_twd"]),
                        "rolls": int(candidate["total_rolls"]),
                    },
                    {
                        "threshold": threshold,
                        "series": label.replace("candidate", "random control"),
                        "net_after_fee_twd": float(random["net_after_fee_twd"]),
                        "rolls": int(random["total_rolls"]),
                    },
                ]
            )

    generated_at = str(summary["generated_at_utc"])
    summary_source = _source(
        "benchmark_summary",
        "Benchmark summary",
        "summary.json",
        description="Aggregated fixed-fee results for all 160 option benchmark variants.",
        executed_at=generated_at,
        sql=summary_sql,
    )
    daily_source = _source(
        "benchmark_daily",
        "Daily benchmark results",
        "daily_benchmarks.parquet",
        description="Daily one-lot fixed-fee P&L used for paired comparisons and cumulative paths.",
        executed_at=generated_at,
        sql=daily_sql,
    )
    title = "台指選擇權 Benchmark 完整比較"
    summary_body = (
        "## Technical summary\n\n"
        f"- **10 類 benchmark 已完整落地。** 30 個交易日、160 個變體、"
        f"{summary['daily_result_rows']:,} 筆每日結果與 {summary['trade_rows']:,} 筆交易均完成對帳。\n"
        f"- **原策略不是這個月唯一或最強的解釋。** 最佳單腿 rolling 為 "
        f"`{best_candidate['variant_id']}`，固定費後 {_signed_twd(float(best_candidate['net_after_fee_twd']))}，"
        f"僅比經典 ATM straddle 多 {_signed_twd(float(best_candidate['paired_delta_vs_classic_twd']))}。\n"
        f"- **較強的可交易控制包括 MTX Gamma scalping 與 150 點 strangle。** "
        f"兩者分別比經典多 {_signed_twd(float(best_gamma['paired_delta_vs_classic_twd']))} 與 "
        f"{_signed_twd(float(best_strangle['paired_delta_vs_classic_twd']))}；但都是 30 日內的 in-sample 最佳值。\n"
        f"- **Random-roll 會大幅改變結論。** 與最佳 rolling 相同 7 次換倉的隨機控制為 "
        f"{_signed_twd(float(matched_random['net_after_fee_twd']))}；原訊號只多 "
        f"{_signed_twd(candidate_vs_random)}。某些其他門檻的 random control 甚至明顯勝過原訊號，"
        "顯示換倉時機優勢尚未被確認。"
    )
    findings_body = (
        "## 較強控制顯示 rolling 優勢仍不唯一\n\n"
        f"經典 ATM straddle 固定費後為 **{_signed_twd(float(classic['net_after_fee_twd']))}**。"
        f"最佳候選 rolling 只增加 **{_signed_twd(float(best_candidate['paired_delta_vs_classic_twd']))}**，"
        f"但 Gamma MTX 增加 **{_signed_twd(float(best_gamma['paired_delta_vs_classic_twd']))}**，"
        f"150 點 long strangle 增加 **{_signed_twd(float(best_strangle['paired_delta_vs_classic_twd']))}**。"
        "下圖的 family 值都是各自參數網格中的事後最佳，因此適合當研究篩選，不是樣本外績效預測。"
    )
    random_body = (
        "## 相同交易次數的隨機控制削弱 price-trigger 證據\n\n"
        f"最佳 rolling 與其 matched random control 都只有 **{int(best_candidate['total_rolls'])} 次**換倉；"
        f"兩者 30 日差額只有 **{_signed_twd(candidate_vs_random)}**。"
        f"全體 random control 的事後最佳值為 **{_signed_twd(float(best_random['net_after_fee_twd']))}**，"
        "但它使用候選策略當日實現後的換倉次數，只是研究控制、不可部署。"
        "若 rolling 規則真的有資訊，應在更長期間持續勝過相同交易次數的隨機觸發，而不只是勝過不換倉。"
    )
    direction_body = (
        "## 方向曝險不是可忽略的替代解釋\n\n"
        f"與 TXO 點值同為 50 TWD 的 MTX，一口放空為 **{_signed_twd(float(underlying_mtx_short['net_after_fee_twd']))}**，"
        f"一口做多為 **{_signed_twd(float(underlying_mtx_long['net_after_fee_twd']))}**。"
        "這個大幅正負對稱顯示樣本期間具有明顯方向路徑；TX 一口點值為 200 TWD，名目是 TXO/MTX 的四倍，"
        "因此完整表保留 TX 結果，但主比較不把 TX 的絕對損益當成同名目排名。"
    )
    definitions_body = (
        "## Scope, data, and metric definitions\n\n"
        "- **期間與時區：** 2026-06-25 至 2026-08-06，共 30 個 TAIFEX 日盤交易日，Asia/Taipei。\n"
        "- **主指標：** `net_after_fee_twd`，即逐筆現金流扣除每口每邊固定費；TX 60、MTX 24、TXO 22、TMF 16 TWD。\n"
        "- **名目：** TXO 與 MTX 均為每點 50 TWD；TMF 為 10，TX 為 200。\n"
        "- **主基準：** 開盤各買一口 ATM Call/Put，收盤平倉。所有選擇權策略每日歸零。\n"
        "- **成本階段：** 本報告主排名只扣固定費，人工滑價為 0；選擇權交易稅保留在次要欄位，未加入期貨稅層。"
    )
    method_body = (
        "## Methodology and benchmark specification\n\n"
        "所有決策只使用已完成整秒；非終端成交使用同商品下一個嚴格較晚的逐筆成交秒。"
        "歷史 Bid/Ask 不存在，因此每口視為保證以成交價代理成交，不使用成交量或掛單深度。"
        "Full Re-center 與 Ratchet 使用 50–1,000 點；Strangle 使用 50–1,000 點 OTM 距離；"
        "TP/SL 使用 TP 25/50/100% × SL 25/50%；時間重設為 15/30/60 分。"
        "Gamma 每 60 秒以當時因果 C+P 價格解零利率共同隱含波動率，再用 Black–Scholes Delta 配置 MTX 或 TMF；"
        "Delta-band 使用 TMF 並測試 0.2/0.3。Random-roll 每日精確匹配對應候選的實現換倉次數。"
    )
    limitations_body = (
        "## Limitations, uncertainty, and robustness status\n\n"
        "**Overall assessment: Share with caveats.** 逐筆費用、產品乘數、每日歸零與 random roll 次數已驗證；"
        "但歷史終端價仍是 13:45 前最後成交 mark，不是可賣 Bid。Gamma 的 Delta 依賴共同 IV、零利率與非同步成交 mark。"
        "160 個 in-sample 變體只有 30 日，存在嚴重多重比較與事後選參數風險。Random control 又刻意使用當日實現交易次數，"
        "只能檢驗規則是否優於隨機時機，不能當成可交易策略。本輪依使用者要求沒有壓力滑價、深度、等待時間或期貨稅測試。"
    )
    next_body = (
        "## Recommended next steps\n\n"
        "1. 先把完全相同的 160 變體延長到更多月份，參數與 random seed 固定不改。\n"
        "2. 主要判定改成 paired daily P&L：候選 rolling 同時必須勝過 classic、matched random、Gamma MTX 與 long strangle。\n"
        "3. 只有較長期間仍有增量時，才依既定順序加入交易稅、實際 Bid/Ask、等待上限與深度；不先加人工壓力滑價。\n"
        "4. Gamma 應另外保存每次 hedge 前後 Delta、IV 與期貨部位，確認正收益不是離散合約取整或方向部位造成。"
    )
    questions_body = (
        "## Further questions\n\n"
        "- 950 點 rolling 在更長樣本是否仍勝過同交易次數 random control？\n"
        "- Gamma MTX 的增量是否集中在少數方向日，還是來自可重複的 gamma capture？\n"
        "- 150 點 strangle 的優勢能否在固定選參數後維持，而不是事後距離最佳化？\n"
        "- 累積到足夠 Shioaji BidAskFOPv1 後，以上排序是否會因 spread 與終端可成交性改變？"
    )

    headline = {
        "classic_pnl": float(classic["net_after_fee_twd"]),
        "candidate_pnl": float(best_candidate["net_after_fee_twd"]),
        "candidate_delta": float(best_candidate["paired_delta_vs_classic_twd"]),
        "gamma_pnl": float(best_gamma["net_after_fee_twd"]),
        "gamma_delta": float(best_gamma["paired_delta_vs_classic_twd"]),
        "candidate_vs_random": candidate_vs_random,
    }
    charts = [
        {
            "id": "family_comparison",
            "title": "Best fixed-fee P&L by benchmark family",
            "subtitle": "30 trading days; each row is the in-sample best variant in its family, with MTX used for comparable underlying notional.",
            "type": "bar",
            "dataset": "family_comparison",
            "sourceId": "benchmark_summary",
            "encodings": {
                "x": {"field": "family", "type": "nominal", "label": "Benchmark family"},
                "y": {"field": "net_after_fee_twd", "type": "quantitative", "label": "Net P&L (TWD)", "format": "number"},
            },
            "valueFormat": "number",
            "layout": "full",
        },
        {
            "id": "cumulative_paths",
            "title": "Daily cumulative fixed-fee P&L",
            "subtitle": "Classic, leading option controls, best single-leg rolling, and its matched random control across 30 sessions.",
            "type": "line",
            "dataset": "cumulative_paths",
            "sourceId": "benchmark_daily",
            "encodings": {
                "x": {"field": "trading_date", "type": "temporal", "label": "Trading date"},
                "y": {"field": "cumulative_pnl_twd", "type": "quantitative", "label": "Cumulative P&L (TWD)", "format": "number"},
                "color": {"field": "series", "type": "nominal", "label": "Strategy"},
            },
            "valueFormat": "number",
            "layout": "full",
        },
        {
            "id": "candidate_random_thresholds",
            "title": "Single-leg rolling and matched random controls by threshold",
            "subtitle": "Fixed-fee 30-day P&L; every random control exactly matches its candidate's realized daily roll count.",
            "type": "line",
            "dataset": "candidate_random_thresholds",
            "sourceId": "benchmark_summary",
            "encodings": {
                "x": {"field": "threshold", "type": "quantitative", "label": "Rolling threshold (TX points)"},
                "y": {"field": "net_after_fee_twd", "type": "quantitative", "label": "Net P&L (TWD)", "format": "number"},
                "color": {"field": "series", "type": "nominal", "label": "Policy/control"},
            },
            "valueFormat": "number",
            "layout": "full",
        },
    ]
    tables = [
        {
            "id": "family_table",
            "title": "Representative and family-best comparisons",
            "dataset": "family_comparison",
            "sourceId": "benchmark_summary",
            "defaultSort": {"field": "net_after_fee_twd", "direction": "desc"},
            "columns": [
                {"field": "family", "label": "Family", "type": "text"},
                {"field": "variant_id", "label": "Variant", "type": "text"},
                {"field": "net_after_fee_twd", "label": "Fixed-fee P&L (TWD)", "format": "number"},
                {"field": "delta_vs_classic_twd", "label": "Vs classic (TWD)", "format": "number", "movement": True},
                {"field": "max_drawdown_twd", "label": "Max drawdown (TWD)", "format": "number", "movement": True},
                {"field": "trade_sides", "label": "Contract-sides", "format": "number"},
            ],
        },
        {
            "id": "full_results",
            "title": "All 160 benchmark variants",
            "dataset": "full_results",
            "sourceId": "benchmark_summary",
            "defaultSort": {"field": "net_after_fee_twd", "direction": "desc"},
            "columns": [
                {"field": "rank", "label": "Rank", "format": "number"},
                {"field": "family", "label": "Family", "type": "text"},
                {"field": "variant_id", "label": "Variant", "type": "text"},
                {"field": "parameters", "label": "Parameters", "type": "text"},
                {"field": "net_after_fee_twd", "label": "Fixed-fee P&L (TWD)", "format": "number"},
                {"field": "delta_vs_classic_twd", "label": "Vs classic (TWD)", "format": "number", "movement": True},
                {"field": "max_drawdown_twd", "label": "Max drawdown (TWD)", "format": "number", "movement": True},
                {"field": "fixed_fees_twd", "label": "Fixed fees (TWD)", "format": "number"},
                {"field": "trade_sides", "label": "Contract-sides", "format": "number"},
            ],
        },
    ]
    cards = [
        {
            "id": "classic_card",
            "description": "Opening ATM Call+Put held to the daily terminal mark.",
            "dataset": "headline",
            "sourceId": "benchmark_summary",
            "metrics": [
                {"label": "Classic ATM straddle P&L (TWD)", "field": "classic_pnl", "format": "number"}
            ],
        },
        {
            "id": "candidate_card",
            "description": "Best of the 40 single-leg rolling candidates.",
            "dataset": "headline",
            "sourceId": "benchmark_summary",
            "metrics": [
                {"label": "Best single-leg rolling P&L (TWD)", "field": "candidate_pnl", "format": "number"},
                {"label": "Delta versus classic (TWD)", "field": "candidate_delta", "format": "number", "signed": True},
            ],
        },
        {
            "id": "gamma_card",
            "description": "Best 60-second delta-hedged ATM straddle control.",
            "dataset": "headline",
            "sourceId": "benchmark_summary",
            "metrics": [
                {"label": "Gamma MTX P&L (TWD)", "field": "gamma_pnl", "format": "number"},
                {"label": "Delta versus classic (TWD)", "field": "gamma_delta", "format": "number", "signed": True},
            ],
        },
        {
            "id": "random_card",
            "description": "Best rolling minus its same-count matched random control.",
            "dataset": "headline",
            "sourceId": "benchmark_summary",
            "metrics": [
                {"label": "Rolling edge over matched random (TWD)", "field": "candidate_vs_random", "format": "number", "signed": True}
            ],
        },
    ]
    blocks = [
        {"id": "title", "type": "markdown", "body": f"# {title}"},
        {"id": "technical_summary", "type": "markdown", "body": summary_body, "sourceId": "benchmark_summary"},
        {"id": "headline_metrics", "type": "metric-strip", "cardIds": ["classic_card", "candidate_card", "gamma_card", "random_card"]},
        {"id": "findings", "type": "markdown", "body": findings_body, "sourceId": "benchmark_summary"},
        {"id": "family_chart", "type": "chart", "chartId": "family_comparison", "layout": "full"},
        {"id": "paths_explanation", "type": "markdown", "body": "## 少數路徑差異造成月末排名\n\n累積線可辨識結果是在多數日穩定分離，還是被少數大幅跳動主導。這個月各領先策略路徑多次交叉，月末排序不能解讀為穩定優勢。", "sourceId": "benchmark_daily"},
        {"id": "paths_chart", "type": "chart", "chartId": "cumulative_paths", "layout": "full"},
        {"id": "random_finding", "type": "markdown", "body": random_body, "sourceId": "benchmark_summary"},
        {"id": "random_chart", "type": "chart", "chartId": "candidate_random_thresholds", "layout": "full"},
        {"id": "direction", "type": "markdown", "body": direction_body, "sourceId": "benchmark_summary"},
        {"id": "family_table_intro", "type": "markdown", "body": "## Family-level exact values\n\n主表保留每個選擇權 family 的事後最佳設定，以及名目相符的 MTX 多空；完整 160 組另列於報告末端。", "sourceId": "benchmark_summary"},
        {"id": "family_table_block", "type": "table", "tableId": "family_table", "layout": "full"},
        {"id": "definitions", "type": "markdown", "body": definitions_body, "sourceId": "benchmark_summary"},
        {"id": "methodology", "type": "markdown", "body": method_body, "sourceId": "benchmark_summary"},
        {"id": "limitations", "type": "markdown", "body": limitations_body, "sourceId": "benchmark_summary"},
        {"id": "next_steps", "type": "markdown", "body": next_body},
        {"id": "questions", "type": "markdown", "body": questions_body},
        {"id": "full_results_intro", "type": "markdown", "body": "## Complete variant inventory\n\n以下表格保留全部 160 個候選與控制，供精確查找；排序僅為本月固定費後結果，不代表樣本外排名。", "sourceId": "benchmark_summary"},
        {"id": "full_results_block", "type": "table", "tableId": "full_results", "layout": "full"},
    ]
    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": title,
            "description": "Technical benchmark comparison for causal one-lot TXO intraday strategies.",
            "generatedAt": generated_at,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "sources": [
                {"id": "benchmark_summary", "label": "Benchmark summary", "path": "summary.json"},
                {"id": "benchmark_daily", "label": "Daily benchmark results", "path": "daily_benchmarks.parquet"},
            ],
            "blocks": blocks,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "ready",
            "datasets": {
                "headline": [headline],
                "family_comparison": comparable_rows,
                "cumulative_paths": cumulative_rows,
                "candidate_random_thresholds": threshold_rows,
                "full_results": full_rows,
            },
        },
        "sources": [summary_source, daily_source],
    }
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(artifact_path, artifact)

    pl.DataFrame(comparable_rows).write_csv(input_dir / "benchmark_family_comparison.csv")
    pl.DataFrame(full_rows).write_csv(input_dir / "all_benchmark_results.csv")
    pl.DataFrame(threshold_rows).write_csv(input_dir / "candidate_random_comparison.csv")
    chart_map = {
        "family_comparison": {
            "question": "Which benchmark families outperform the classic opening ATM straddle?",
            "type": "bar",
            "dataset": "family_comparison",
            "claim": "The best single-leg roll is not the only in-sample control above classic.",
        },
        "cumulative_paths": {
            "question": "Are the leading controls persistently separated over time?",
            "type": "line",
            "dataset": "cumulative_paths",
            "claim": "Paths cross and the month-end ranking is sensitive to a few sessions.",
        },
        "candidate_random_thresholds": {
            "question": "Do price-trigger candidates beat matched random roll timing?",
            "type": "line",
            "dataset": "candidate_random_thresholds",
            "claim": "Random timing can match or beat many threshold candidates at identical roll counts.",
        },
    }
    _atomic_json(input_dir / "chart_map.json", chart_map)
    receipt = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_json": str(artifact_path),
        "artifact_json_sha256": _sha256_path(artifact_path),
        "sources": {
            "summary_json_sha256": _sha256_path(summary_path),
            "daily_benchmarks_sha256": _sha256_path(daily_path),
            "trades_sha256": _sha256_path(trades_path),
        },
        "validation": {
            "benchmark_families": 10,
            "variants": 160,
            "daily_rows": int(summary["daily_result_rows"]),
            "trade_rows": int(summary["trade_rows"]),
            "fixed_fee_schedule_verified": True,
            "artificial_slippage_zero": True,
        },
    }
    _atomic_json(input_dir / "report_source_receipt.json", receipt)
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("artifacts/research/taifex_option_benchmarks"),
    )
    parser.add_argument("--artifact", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact = args.artifact or (args.input_dir / "artifact.json")
    receipt = build_artifact(input_dir=args.input_dir, artifact_path=artifact)
    print(json.dumps(receipt, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
