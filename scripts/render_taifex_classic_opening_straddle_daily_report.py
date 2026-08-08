#!/usr/bin/env python3
"""Build the canonical Data Analytics artifact for a TXO straddle benchmark."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Final

import pandas as pd


DEFAULT_ARTIFACT_DIR: Final[Path] = Path(
    "artifacts/research/taifex_classic_opening_straddle_daily_long_history"
)
OFFICIAL_OPTIONS_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/optDailyMarketView"
)
OFFICIAL_FUTURES_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/3/futDailyMarketView"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _money(value: float) -> str:
    return f"TWD {value:,.0f}"


def _load(artifact_dir: Path):
    summary_path = artifact_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    frames: dict[str, pd.DataFrame] = {}
    for key, filename in (
        ("annual", "annual_results.csv"),
        ("monthly", "monthly_results.csv"),
        ("excluded", "excluded_sessions.csv"),
    ):
        expected = summary["artifacts"][
            {
                "annual": "annual_results",
                "monthly": "monthly_results",
                "excluded": "excluded_sessions",
            }[key]
        ]["sha256"]
        path = artifact_dir / filename
        if _sha256(path) != expected:
            raise ValueError(f"artifact hash mismatch: {path}")
        frames[key] = pd.read_csv(path)
    daily_path = artifact_dir / "daily_results.parquet"
    if _sha256(daily_path) != summary["artifacts"]["daily_results"]["sha256"]:
        raise ValueError(f"artifact hash mismatch: {daily_path}")
    return summary, frames["annual"], frames["monthly"], frames["excluded"]


def _artifact(
    summary: dict[str, Any],
    annual: pd.DataFrame,
    monthly: pd.DataFrame,
    excluded: pd.DataFrame,
    *,
    generated_at: str,
) -> dict[str, Any]:
    results = summary["results"]
    coverage = summary["coverage"]
    parameters = summary["parameters"]
    series_scope = str(parameters.get("series_scope") or "monthly")
    position_side = str(parameters.get("position_side") or "long")
    is_weekly = series_scope == "weekly"
    is_short = position_side == "short"
    strategy_label = "Short Straddle 損益初篩" if is_short else "Straddle 長期回測"
    report_title = (
        f"期交所最快到期週選 Classic Opening ATM {strategy_label}"
        if is_weekly
        else f"期交所月選 Classic Opening ATM {strategy_label}"
    )
    series_label = "最快到期 TXO 週選" if is_weekly else "最近未到期 TXO 月選"
    series_filter = (
        "到期月份(週別) 僅 YYYYMMWn／YYYYMMFn 週選，依實際到期日取最快到期契約"
        if is_weekly
        else "到期月份(週別) 僅 YYYYMM 月選"
    )
    no_weekly_listing_days = int(
        coverage.get("exclusion_reason_counts", {}).get("no_weekly_txo_listing", 0)
    )
    weekly_availability_note = (
        f"- 另有 {no_weekly_listing_days:,} 個 TX 交易日的官方日檔完全沒有週選列；"
        "這些日期不以月選或較遠月份替代。\n"
        if is_weekly and no_weekly_listing_days
        else ""
    )
    entry_action = "賣出" if is_short else "買進"
    exit_action = "買回" if is_short else "賣出"
    direction_formula = "-1" if is_short else "+1"
    candidate_first = int(str(coverage["candidate_first_date"])[:4])
    candidate_last = int(str(coverage["candidate_last_date"])[:4])
    full_years = annual.loc[
        (annual["year"] > candidate_first) & (annual["year"] < candidate_last)
    ]
    profitable_full_years = int((full_years["net_pnl_twd"] > 0.0).sum())
    full_year_count = int(len(full_years))
    comparison_years = full_years if not full_years.empty else annual
    best_annual = comparison_years.loc[comparison_years["net_pnl_twd"].idxmax()]
    worst_annual = comparison_years.loc[comparison_years["net_pnl_twd"].idxmin()]

    annual = annual.copy()
    annual["period"] = annual["year"].astype(str)
    annual.loc[annual["year"] == candidate_first, "period"] += "（部分）"
    annual.loc[annual["year"] == candidate_last, "period"] += "（部分）"
    annual["net_result"] = annual["net_pnl_twd"].map(
        lambda value: "獲利" if value > 0 else "虧損" if value < 0 else "持平"
    )
    annual["coverage_pct"] = annual["coverage_share"] * 100.0

    monthly = monthly.copy()
    monthly["month"] = monthly["month"].astype(str) + "-01"
    opposite = results["opposite_position_same_sample"]
    current_direction = "開盤賣 Call + Put" if is_short else "開盤買 Call + Put"
    opposite_direction = "開盤買 Call + Put" if is_short else "開盤賣 Call + Put"
    direction_comparison = pd.DataFrame(
        [
            {
                "position_side": position_side,
                "direction": current_direction,
                "gross_pnl_twd": results["gross_pnl_twd"],
                "fee_twd": results["fees_twd"],
                "net_pnl_twd": results["net_pnl_twd"],
            },
            {
                "position_side": opposite["position_side"],
                "direction": opposite_direction,
                "gross_pnl_twd": opposite["gross_pnl_twd"],
                "fee_twd": opposite["fees_twd"],
                "net_pnl_twd": opposite["net_pnl_twd"],
            },
        ]
    ).sort_values("position_side", kind="stable")
    headline = pd.DataFrame(
        [
            {
                "net_pnl_twd": results["net_pnl_twd"],
                "gross_pnl_twd": results["gross_pnl_twd"],
                "fees_twd": results["fees_twd"],
                "maximum_drawdown_twd": results["maximum_drawdown_twd"],
                "executable_sessions": coverage["executable_sessions"],
                "executable_share": coverage["executable_share"],
                "profitable_full_year_share": (
                    profitable_full_years / full_year_count if full_year_count else 0.0
                ),
            }
        ]
    )

    options_source = {
        "id": "taifex_option_daily",
        "label": "臺灣期貨交易所－選擇權每日交易行情",
        "href": OFFICIAL_OPTIONS_URL,
        "query": {
            "engine": "TAIFEX official download",
            "url": OFFICIAL_OPTIONS_URL,
            "description": "期交所年度 ZIP 與當年度逐月 TXO 日行情下載。",
            "executed_at": generated_at,
            "filters": [
                "契約=TXO",
                "交易時段=一般（舊格式無交易時段欄者視為日盤）",
                series_filter,
                f"交易日 {coverage['candidate_first_date']} 至 {coverage['candidate_last_date']}",
            ],
            "metric_definitions": [
                "開盤價為該契約該日第一筆成交價；收盤價為該日最後一筆成交價。",
                "成交量只用於確認選中 ATM 腿當日有成交，不做容量或滑價模型。",
            ],
            "tables_used": ["TAIFEX 選擇權每日交易行情年度 ZIP／日期區間 CSV"],
        },
    }
    futures_source = {
        "id": "taifex_futures_daily",
        "label": "臺灣期貨交易所－期貨每日交易行情",
        "href": OFFICIAL_FUTURES_URL,
        "query": {
            "engine": "TAIFEX official download",
            "url": OFFICIAL_FUTURES_URL,
            "description": "官方 TX 日盤近月開盤價，用於每日 ATM 履約價選擇。",
            "executed_at": generated_at,
            "filters": ["契約=TX", "交易時段=一般", "最近未到期月份"],
            "metric_definitions": [
                f"ATM 為與 TX 日盤開盤價絕對距離最小的成對{series_label}履約價；平手取較低履約價。"
            ],
            "tables_used": ["TAIFEX 期貨每日交易行情年度 ZIP／日期區間 CSV"],
        },
    }
    derived_source = {
        "id": "classic_straddle_results",
        "label": f"StockAgent Classic Opening ATM {position_side.title()} Straddle 回測輸出",
        "path": "daily_results.parquet",
        "query": {
            "engine": "duckdb",
            "language": "sql",
            "sql": "SELECT * FROM read_parquet('daily_results.parquet') ORDER BY date",
            "description": "載入逐日回測明細；報告中的月、年與全期間數字均由這些逐日列彙總。",
            "executed_at": generated_at,
            "filters": [
                "只含通過 ATM Call/Put 開盤、收盤與成交量完整性檢查的日期",
                f"每日{entry_action}一口 Call 加一口 Put，收盤{exit_action}並歸零",
            ],
            "metric_definitions": [
                f"毛損益={direction_formula}*((Call收盤+Put收盤)-(Call開盤+Put開盤))*50。",
                "手續費=每日4個contract-side*22元；淨損益=毛損益-手續費。",
                "開盤權利金與收盤買賣回補均按現金流入為正、流出為負記錄。",
                "月、年與全期間數字為逐日損益的時間彙總。",
            ],
            "tables_used": ["daily_results.parquet"],
        },
    }
    sources = [options_source, futures_source, derived_source]

    net = float(results["net_pnl_twd"])
    gross = float(results["gross_pnl_twd"])
    fees = float(results["fees_twd"])
    opposite_net = float(opposite["net_pnl_twd"])
    gross_readout = (
        f"零手續費毛損益為 **{_money(gross)}**，但仍低於固定手續費 **{_money(fees)}**；"
        f"扣費後反而剩 **{_money(net)}**。"
        if gross > 0.0 and net <= 0.0
        else f"零手續費毛損益為 **{_money(gross)}**；固定手續費合計 **{_money(fees)}**。"
    )
    capital_boundary = (
        "這只是裸賣的損益初篩；開盤收進的權利金不是資金母數，本報告不計裸賣保證金、"
        "盤中追繳／強平或資金報酬率。"
        if is_short
        else ""
    )
    gross_bullet = (
        f"- **手續費吃掉全部毛利：**{gross_readout}\n"
        if gross > 0.0 and net <= 0.0
        else f"- **毛損益本身已不利：**{gross_readout}\n"
    )
    executive = (
        "## Executive Summary\n\n"
        f"- **答案是否定的：**依這個日行情代理定義，{coverage['executable_sessions']:,} 個交易日的"
        f"累積淨損益為 **{_money(net)}**；策略沒有確認營利，因此本次不再疊加交易稅、滑價或容量壓力。\n"
        + gross_bullet
        + f"- **年度穩定性不足：**完整年度中只有 {profitable_full_years}/{full_year_count} 年淨獲利；"
        + f"最佳年度為 {int(best_annual['year'])}（{_money(float(best_annual['net_pnl_twd']))}），"
        + f"最差年度為 {int(worst_annual['year'])}（{_money(float(worst_annual['net_pnl_twd']))}）。\n"
        + "- **這是長期價格代理，不是可成交報價回測：**Call 與 Put 的日開盤／收盤可能發生在不同時間，"
        + "也不是同步可成交的 bid/ask。"
        + (f"\n- **裸賣邊界：**{capital_boundary}" if is_short else "")
    )

    cards = [
        {
            "id": "net_card",
            "dataset": "headline",
            "sourceId": "classic_straddle_results",
            "description": f"每日{entry_action}一口 Call + 一口 Put，收盤{exit_action}，已扣固定手續費。",
            "metrics": [
                {"label": "累積淨損益（TWD）", "field": "net_pnl_twd", "format": "compact", "signed": True},
                {"label": "零費用毛損益", "field": "gross_pnl_twd", "format": "compact", "signed": True},
            ],
        },
        {
            "id": "drawdown_card",
            "dataset": "headline",
            "sourceId": "classic_straddle_results",
            "description": "由累積淨損益歷史高點計算。",
            "metrics": [
                {"label": "最大回撤（TWD）", "field": "maximum_drawdown_twd", "format": "compact", "signed": True}
            ],
        },
        {
            "id": "coverage_card",
            "dataset": "headline",
            "sourceId": "classic_straddle_results",
            "description": f"候選 {coverage['candidate_sessions']:,} 日中，排除 {coverage['excluded_sessions']} 日。",
            "metrics": [
                {"label": "可回測交易日", "field": "executable_sessions", "format": "number"},
                {"label": "資料覆蓋率", "field": "executable_share", "format": "percent"},
            ],
        },
        {
            "id": "year_card",
            "dataset": "headline",
            "sourceId": "classic_straddle_results",
            "description": f"完整年度指 {candidate_first + 1}–{candidate_last - 1}。",
            "metrics": [
                {"label": "完整年度獲利率", "field": "profitable_full_year_share", "format": "percent"}
            ],
        },
    ]
    charts = [
        {
            "id": "cumulative_chart",
            "title": "每月月底累積淨損益",
            "subtitle": (
                f"固定手續費後，TWD；{coverage['candidate_first_date']} "
                f"至 {coverage['candidate_last_date']}"
            ),
            "intent": "trend",
            "question": "策略的長期累積損益路徑是否穩定向上？",
            "rationale": "時間序列折線最直接顯示長期方向、回升與回撤區段。",
            "type": "line",
            "dataset": "monthly",
            "sourceId": "classic_straddle_results",
            "encodings": {
                "x": {"field": "month", "type": "temporal", "label": "月份"},
                "y": {"field": "month_end_cumulative_net_pnl_twd", "type": "quantitative", "label": "累積淨損益", "unit": "TWD"},
                "tooltip": [
                    {"field": "month", "type": "temporal", "label": "月份"},
                    {"field": "net_pnl_twd", "type": "quantitative", "label": "當月淨損益", "unit": "TWD"},
                    {"field": "month_worst_drawdown_twd", "type": "quantitative", "label": "截至當月回撤", "unit": "TWD"},
                ],
            },
            "valueFormat": "compact",
            "unit": "TWD",
            "referenceLines": [{"axis": "y", "value": 0, "label": "損益兩平", "color": "neutral", "lineStyle": "dashed"}],
            "maxRows": 400,
            "surface": {"viewMode": "both", "showControls": True},
        },
        {
            "id": "annual_chart",
            "title": "年度淨損益",
            "subtitle": (
                f"{candidate_first} 與 {candidate_last} 為部分年度；其餘為完整年度"
            ),
            "intent": "comparison",
            "question": "長期虧損是否由少數年份造成，或跨年度普遍存在？",
            "rationale": "逐年柱狀比較可以辨識獲利年度比例及損益集中度。",
            "type": "bar",
            "dataset": "annual",
            "sourceId": "classic_straddle_results",
            "encodings": {
                "x": {"field": "period", "type": "ordinal", "label": "年度"},
                "y": {"field": "net_pnl_twd", "type": "quantitative", "label": "年度淨損益", "unit": "TWD"},
                "tooltip": [
                    {"field": "executable_sessions", "type": "quantitative", "label": "交易日"},
                    {"field": "gross_pnl_twd", "type": "quantitative", "label": "毛損益", "unit": "TWD"},
                    {"field": "fee_twd", "type": "quantitative", "label": "手續費", "unit": "TWD"},
                    {"field": "win_rate", "type": "quantitative", "format": "percent", "label": "勝率"},
                ],
            },
            "valueFormat": "compact",
            "unit": "TWD",
            "referenceLines": [{"axis": "y", "value": 0, "label": "損益兩平", "color": "neutral", "lineStyle": "solid"}],
            "maxRows": 40,
            "surface": {"viewMode": "both", "showControls": True},
        },
    ]
    tables = [
        {
            "id": "direction_comparison_table",
            "title": "同樣本長短方向損益",
            "subtitle": (
                f"相同 {coverage['executable_sessions']:,} 個交易日、每腿一口，"
                "只扣固定手續費"
            ),
            "dataset": "direction_comparison",
            "sourceId": "classic_straddle_results",
            "defaultSort": {"field": "position_side", "direction": "asc"},
            "density": "spacious",
            "columns": [
                {"field": "position_side", "label": "模式", "type": "text"},
                {"field": "direction", "label": "每日方向", "type": "text"},
                {"field": "gross_pnl_twd", "label": "毛損益 TWD", "format": "number", "movement": True},
                {"field": "fee_twd", "label": "手續費 TWD", "format": "number"},
                {"field": "net_pnl_twd", "label": "淨損益 TWD", "format": "number", "movement": True},
            ],
        },
        {
            "id": "annual_table",
            "title": "年度結果與資料覆蓋",
            "subtitle": "精確值；由舊到新排列",
            "dataset": "annual",
            "sourceId": "classic_straddle_results",
            "defaultSort": {"field": "period", "direction": "asc"},
            "density": "dense",
            "columns": [
                {"field": "period", "label": "年度", "type": "text"},
                {"field": "executable_sessions", "label": "交易日", "format": "number"},
                {"field": "coverage_share", "label": "覆蓋率", "format": "percent"},
                {"field": "gross_pnl_twd", "label": "毛損益 TWD", "format": "number", "movement": True},
                {"field": "fee_twd", "label": "手續費 TWD", "format": "number"},
                {"field": "net_pnl_twd", "label": "淨損益 TWD", "format": "number", "movement": True},
                {"field": "win_rate", "label": "勝率", "format": "percent"},
            ],
        }
    ]

    blocks = [
        {"id": "title", "type": "markdown", "body": f"# {report_title}"},
        {"id": "executive_summary", "type": "markdown", "body": executive, "sourceId": "classic_straddle_results"},
        {"id": "headline_metrics", "type": "metric-strip", "cardIds": [card["id"] for card in cards]},
        {
            "id": "interpretation",
            "type": "markdown",
            "sourceId": "classic_straddle_results",
            "body": (
                "## 核心判讀\n\n"
                f"**這個 Classic {position_side} 版本在目前定義下不值得加碼真實限制。**{gross_readout}"
                "依你的研究順序，到這裡便停止增加交易稅、滑價、容量"
                + ("、裸賣保證金與強平" if is_short else "")
                + "等限制。固定單邊 22 元後，平均每個可回測交易日"
                + f"淨損益為 {_money(float(results['average_net_pnl_twd']))}，中位數為 {_money(float(results['median_net_pnl_twd']))}。"
            ),
        },
        {
            "id": "direction_comparison_readout",
            "type": "markdown",
            "sourceId": "classic_straddle_results",
            "body": (
                "## 反向操作只翻轉毛損益，不會翻轉成本\n\n"
                f"同一批日開盤／收盤價格下，long 與 short 的毛損益互為相反數；"
                f"但兩邊都要支付 {_money(fees)} 手續費。這使目前 {position_side} 方向淨損益為 "
                f"**{_money(net)}**，反方向為 **{_money(opposite_net)}**。因此不能把毛損益鏡像直接當成淨獲利鏡像。"
            ),
        },
        {
            "id": "direction_comparison_block",
            "type": "table",
            "tableId": "direction_comparison_table",
        },
        {"id": "cumulative_block", "type": "chart", "chartId": "cumulative_chart"},
        {
            "id": "cumulative_readout",
            "type": "markdown",
            "sourceId": "classic_straddle_results",
            "body": (
                "## 固定手續費後沒有形成穩定向上的路徑\n\n"
                f"累積淨損益的最大回撤為 **{_money(float(results['maximum_drawdown_twd']))}**。"
                "少數反彈年度不足以證明跨制度、跨波動環境的穩定優勢。"
            ),
        },
        {"id": "annual_block", "type": "chart", "chartId": "annual_chart"},
        {
            "id": "annual_readout",
            "type": "markdown",
            "sourceId": "classic_straddle_results",
            "body": (
                "## 少數獲利年無法抵銷多數虧損年\n\n"
                f"完整的 {full_year_count} 個年度中，只有 **{profitable_full_years} 年**在固定手續費後為正。"
                f"最佳完整年度是 {int(best_annual['year'])}，最差年度是 {int(worst_annual['year'])}；"
                "這不是單一極端年度造成的孤立虧損。"
            ),
        },
        {"id": "annual_table_block", "type": "table", "tableId": "annual_table"},
        {
            "id": "methodology",
            "type": "markdown",
            "body": (
                "## 回測定義與資料品質\n\n"
                f"- 官方資料範圍：**{coverage['candidate_first_date']}–{coverage['candidate_last_date']}**。\n"
                f"- 候選交易日 {coverage['candidate_sessions']:,}；可回測 {coverage['executable_sessions']:,}；"
                f"排除 {coverage['excluded_sessions']}，覆蓋率 {coverage['executable_share']:.2%}。\n"
                f"{weekly_availability_note}"
                f"- 合約固定為**{series_label}**，不以全日成交量挑選到期別。\n"
                "- ATM 由官方 TX 日盤近月開盤價決定；若理論 ATM Call/Put 任一腿缺有效日開盤、日收盤或成交量，整日排除。\n"
                f"- 每日開盤{entry_action} Call/Put 各一口，收盤{exit_action}；逐日檔驗證兩腿最終部位均為 0。\n"
                f"- 每腿一口、乘數 50 元／點；每日四個 contract-side，固定手續費 {parameters['fee_per_session_twd']:.0f} 元。\n"
                "- 未使用成交量挑履約價，也未套用深度、滑價、交易稅或資金報酬率假設。"
                + (f"\n- {capital_boundary}" if is_short else "")
            ),
        },
        {
            "id": "caveats",
            "type": "markdown",
            "body": (
                "## 重要限制\n\n"
                "1. **不是同步開盤成交。** 每個契約的日開盤價是它自己的第一筆成交；Call 與 Put 可能不同時。\n"
                "2. **不是 Bid/Ask 可成交價。** 日收盤價同樣只是最後成交，不能視為收盤 bid。\n"
                "3. **不推回歷史委託簿。** 官方日檔沒有逐筆五檔深度，不能由成交量重建。\n"
                "4. **未算歷史交易稅。** 主結果已虧損，依你的規則不再增加更多限制；也避免用單一現行稅率錯套多年資料。\n"
                + (
                    "5. **不是完整裸賣回測。** 未建模逐日保證金、盤中權益、追繳、強制平倉與破產；不能據此判定可執行性或資金報酬率。\n"
                    if is_short
                    else ""
                )
                + ("6." if is_short else "5.")
                + " **不是投資結論。** 這只回答固定定義的每日價格代理是否曾有長期統計獲利。"
            ),
        },
        {
            "id": "next_steps",
            "type": "markdown",
            "body": (
                "## 下一步\n\n"
                "目前不建議繼續替這個 Classic 日頻版本疊加真實限制。若要延伸，應改變研究問題，而不是替虧損結果做壓力測試："
                "可用近期實際擷取的同步 Bid/Ask 驗證開盤成交落差，或另行比較不同出場時間；兩者都應作為獨立策略版本。"
            ),
        },
        {
            "id": "questions",
            "type": "markdown",
            "body": "## Further Questions\n\n"
            + (
                "- 要不要只做同日期 long/short 損益來源分解，不增加保證金或滑價限制？\n"
                if is_short
                else "- 要不要把相同 Classic 定義改用近一個月的逐筆 Bid/Ask 做可成交價校準？\n"
            )
            + (
                "- 要不要把最快到期週選與先前月選結果做同期間的逐日差異分解？\n"
                if is_weekly
                else "- 要不要另做最快到期週選版本，保持與月選結果分離？\n"
            )
            + "- 要不要比較固定時間（例如 13:30）出場，而不是各腿最後成交價？",
        },
    ]

    return {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": report_title,
            "description": (
                f"{coverage['candidate_first_date']} 起的官方{series_label}每日行情"
                f" Classic {position_side} 損益代理回測。"
            ),
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
                "headline": _records(headline),
                "direction_comparison": _records(direction_comparison),
                "monthly": _records(monthly),
                "annual": _records(annual),
                "excluded": _records(excluded),
            },
        },
        "sources": sources,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", type=Path, default=DEFAULT_ARTIFACT_DIR)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    artifact_dir = args.artifact_dir.expanduser().resolve()
    output = (args.output or artifact_dir / "artifact.json").expanduser().resolve()
    summary, annual, monthly, excluded = _load(artifact_dir)
    generated_at = datetime.now(timezone.utc).isoformat()
    payload = _artifact(
        summary,
        annual,
        monthly,
        excluded,
        generated_at=generated_at,
    )
    _atomic_json(output, payload)
    print(f"wrote canonical report artifact {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
