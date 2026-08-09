#!/usr/bin/env python3
"""Build a portable technical report for the hold-to-expiry TXO study."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Final

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.render_taifex_classic_opening_straddle_daily_report import (  # noqa: E402
    _atomic_json,
    _money,
    _records,
    _sha256,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    TAIFEX_OPTION_PREMIUM_TAX_RATE,
    option_premium_transaction_tax_twd,
)


DEFAULT_HOLD_DIR: Final[Path] = Path(
    "artifacts/research/taifex_opening_straddle_hold_to_weekly_expiry"
)
DEFAULT_SHORT_HOLD_DIR: Final[Path] = Path(
    "artifacts/research/taifex_opening_short_straddle_hold_to_weekly_expiry"
)
DEFAULT_CLASSIC_DIR: Final[Path] = Path(
    "artifacts/research/taifex_classic_opening_straddle_weekly_nearest_expiry"
)
OPTIONS_URL: Final[str] = "https://www.taifex.com.tw/cht/3/optDailyMarketView"
FUTURES_URL: Final[str] = "https://www.taifex.com.tw/cht/3/futDailyMarketView"
SETTLEMENT_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/5/optIndxFSP"
)
TRANSACTION_TAX_URL: Final[str] = (
    "https://www.taifex.com.tw/cht/9/tradersQAProducts"
)
FOCUS_START: Final[pd.Timestamp] = pd.Timestamp("2026-05-01")
FOCUS_END: Final[pd.Timestamp] = pd.Timestamp("2026-07-31")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _verify(path: Path, expected_sha256: str) -> None:
    if _sha256(path) != expected_sha256:
        raise ValueError(f"artifact hash mismatch: {path}")


def _sources(generated_at: str, coverage: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "id": "combined_research_evidence",
            "label": "StockAgent 買賣跨式持有到期與 Classic 基準整合證據",
            "path": "report_evidence.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": (
                    "SELECT * FROM read_json_auto('report_evidence.json', "
                    "maximum_depth=-1)"
                ),
                "description": "由買方、賣方與Classic回測摘要、逐期結果及資金帳務輸出組成報告快照。",
                "executed_at": generated_at,
                "filters": [
                    "TXO 日盤最快到期 W/F 週選",
                    "每腿一口，單一到期週期不換履約價",
                    "單邊手續費 22 元",
                    "法定期貨交易稅，逐口元以下四捨五入",
                    f"資料範圍 {coverage['source_first_date']} 至 {coverage['source_last_date']}",
                ],
                "metric_definitions": [
                    "持有到期手續費=完整週期數*2個開倉contract-side*22元；買賣方向相同。",
                    "進場稅=Call與Put各自權利金金額*0.1%，逐口元以下四捨五入。",
                    "到期稅=自動履約價內腿的結算指數契約金額*股價類期貨歷史稅率，逐口元以下四捨五入。",
                    "淨損益=到期價值-開倉權利金-固定手續費-進場與到期交易稅。",
                    "一口累積資金報酬=累積稅後淨損益/全樣本最大單期開倉權利金含手續費與進場稅；不是複利CAGR。",
                    "Classic 每天開平Call/Put各一口，共4個contract-side。",
                ],
                "tables_used": [
                    "cycles.parquet",
                    "daily_results.parquet",
                    "short cycles.parquet",
                    "short daily_results.parquet",
                    "daily_metrics.csv",
                    "annual_results.csv",
                    "monthly_results.csv",
                    "capital_metrics.csv",
                    "Classic daily_results.parquet",
                ],
            },
        },
        {
            "id": "taifex_transaction_tax",
            "label": "臺灣期貨交易所－期貨與選擇權交易稅率與計算",
            "href": TRANSACTION_TAX_URL,
            "query": {
                "engine": "TAIFEX official rules",
                "url": TRANSACTION_TAX_URL,
                "description": "選擇權成交按權利金0.1%；到期現金結算按股價類期貨契約金額稅率；每口稅額四捨五入至元。",
                "executed_at": generated_at,
                "filters": [
                    "TXO股價指數選擇權",
                    "買方一口",
                    "樣本跨2013-04-01股價類期貨降稅日",
                ],
                "metric_definitions": [
                    "2008-10-06至2013-03-31股價類期貨稅率=0.004%。",
                    "2013-04-01起股價類期貨稅率=0.002%。",
                    "選擇權一般交易稅率=權利金金額0.1%。",
                ],
                "tables_used": ["期交所商品面Q&A與財政部法規"],
            },
        },
        {
            "id": "taifex_options_daily",
            "label": "臺灣期貨交易所－選擇權每日交易行情",
            "href": OPTIONS_URL,
            "query": {
                "engine": "TAIFEX official download",
                "url": OPTIONS_URL,
                "description": "年度ZIP與日期區間CSV中的TXO日盤第一筆與最後一筆成交。",
                "executed_at": generated_at,
                "filters": [
                    "契約=TXO",
                    "交易時段=一般",
                    "週選=YYYYMMWn或YYYYMMFn，依實際到期日取最快到期",
                ],
                "tables_used": ["TAIFEX 選擇權每日交易行情原始回條"],
            },
        },
        {
            "id": "taifex_futures_daily",
            "label": "臺灣期貨交易所－期貨每日交易行情",
            "href": FUTURES_URL,
            "query": {
                "engine": "TAIFEX official download",
                "url": FUTURES_URL,
                "description": "TX日盤近月開盤價只用來決定開倉ATM履約價。",
                "executed_at": generated_at,
                "filters": ["契約=TX", "交易時段=一般", "最近未到期月份"],
                "tables_used": ["TAIFEX 期貨每日交易行情原始回條"],
            },
        },
        {
            "id": "taifex_final_settlement",
            "label": "臺灣期貨交易所－指數選擇權最後結算價",
            "href": SETTLEMENT_URL,
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "sql": (
                    "SELECT * FROM read_parquet("
                    "'data_tw_index_options_daily/txo_final_settlement_history.parquet') "
                    "WHERE product = 'TXO'"
                ),
                "url": SETTLEMENT_URL,
                "executed_at": generated_at,
                "filters": ["product=TXO", "週選", "日期與契約代碼完全相符"],
                "description": (
                    f"完整歷史檔涵蓋本研究的 {coverage['official_final_settlement_cycles']} 個TXO週選到期週期。"
                ),
                "metric_definitions": ["正式到期價值=abs(最後結算價-履約價)。"],
                "tables_used": ["txo_final_settlement_history.parquet"],
            },
        },
    ]


def _cards(*, complete_cycles: int) -> list[dict[str, Any]]:
    source_id = "combined_research_evidence"
    return [
        {
            "id": "net_card",
            "dataset": "headline",
            "sourceId": source_id,
            "description": f"{complete_cycles}個完整到期週期的一口固定部位累積值。",
            "metrics": [
                {
                    "label": "開盤買C+P淨損益（TWD）",
                    "field": "net_pnl_twd",
                    "format": "compact",
                    "signed": True,
                },
                {
                    "label": "開盤賣C+P（保證金前）",
                    "field": "short_net_pnl_twd",
                    "format": "compact",
                    "signed": True,
                },
            ],
        },
        {
            "id": "cost_card",
            "dataset": "headline",
            "sourceId": source_id,
            "description": "手續費加法定期交稅；相對Classic只做成本比較。",
            "metrics": [
                {
                    "label": "持有到期總交易成本（TWD）",
                    "field": "trading_costs_twd",
                    "format": "compact",
                },
                {
                    "label": "其中交易稅",
                    "field": "transaction_taxes_twd",
                    "format": "compact",
                },
                {
                    "label": "總成本較Classic節省",
                    "field": "cost_savings_share",
                    "format": "percent",
                    "signed": True,
                },
            ],
        },
        {
            "id": "win_card",
            "dataset": "headline",
            "sourceId": source_id,
            "description": "勝率與中位數顯示總獲利依賴少數大波動週期。",
            "metrics": [
                {"label": "週期勝率", "field": "win_rate", "format": "percent"},
                {
                    "label": "賣方週期勝率",
                    "field": "short_win_rate",
                    "format": "percent",
                },
                {
                    "label": "買方中位數損益",
                    "field": "median_net_pnl_twd",
                    "format": "compact",
                    "signed": True,
                },
                {
                    "label": "賣方中位數損益",
                    "field": "short_median_net_pnl_twd",
                    "format": "compact",
                    "signed": True,
                },
            ],
        },
        {
            "id": "capital_card",
            "dataset": "headline",
            "sourceId": source_id,
            "description": "只對買方計固定一口累積報酬；Sharpe用完整交易日日終估值。",
            "metrics": [
                {
                    "label": "一口累積資金報酬",
                    "field": "one_lot_cumulative_return_on_capital",
                    "format": "percent",
                },
                {
                    "label": "日結年化Sharpe",
                    "field": "annualized_sharpe",
                    "format": "number",
                },
            ],
        },
    ]


def _charts() -> list[dict[str, Any]]:
    source_id = "combined_research_evidence"
    charts = [
        {
            "id": "cumulative_early_chart",
            "title": "開盤買跨式每日累積資金報酬（2012至2020）",
            "subtitle": "固定一口稅後損益除以全樣本最大開倉資金 TWD 83,128；不是CAGR或動態加碼",
            "intent": "trend",
            "question": "正的全期總損益是否來自穩定向上的長期路徑？",
            "rationale": "用完整每日日終曲線呈現長期路徑；因單資料集上限2000列，依時間分成兩張連續日頻圖。",
            "type": "line",
            "dataset": "daily_early",
            "sourceId": source_id,
            "encodings": {
                "x": {
                    "field": "trading_date",
                    "type": "temporal",
                    "label": "交易日",
                },
                "y": {
                    "field": "買方固定一口累積資金報酬",
                    "type": "quantitative",
                    "label": "累積資金報酬",
                    "unit": "%",
                },
                "tooltip": [
                    {
                        "field": "買方當日淨損益",
                        "type": "quantitative",
                        "label": "買方當日淨損益",
                        "unit": "TWD",
                    },
                    {"field": "買方累積淨損益", "type": "quantitative", "label": "買方累積淨損益", "unit": "TWD"},
                    {"field": "option_series", "type": "nominal", "label": "持有契約"},
                    {"field": "is_entry_session", "type": "nominal", "label": "進場日"},
                    {"field": "is_expiry_session", "type": "nominal", "label": "到期日"},
                ],
            },
            "labels": {"values": "endpoints"},
            "valueFormat": "percent",
            "unit": "%",
            "referenceLines": [
                {
                    "axis": "y",
                    "value": 0,
                    "label": "報酬兩平",
                    "color": "neutral",
                    "lineStyle": "dashed",
                }
            ],
            "maxRows": 2000,
            "surface": {"viewMode": "both", "showControls": True},
        },
        {
            "id": "daily_focus_chart",
            "title": "2026年5至7月開盤買／賣跨式每日淨損益",
            "subtitle": "同一批合約的每日日終標記；手續費與法定期交稅在發生日扣除，TWD",
            "intent": "trend",
            "question": "5至7月的獲利是否來自每日穩定收益，還是少數大波動日？",
            "rationale": "以63個完整交易日柱狀值呈現獲利與虧損的日別集中度。",
            "type": "bar",
            "dataset": "daily_focus",
            "sourceId": source_id,
            "encodings": {
                "x": {
                    "field": "trading_date",
                    "type": "temporal",
                    "label": "交易日",
                },
                "y": {
                    "fields": [
                        "買方當日淨損益",
                        "賣方當日淨損益_保證金前",
                    ],
                    "type": "quantitative",
                    "label": "當日淨損益",
                    "unit": "TWD",
                },
                "tooltip": [
                    {"field": "option_series", "type": "nominal", "label": "持有契約"},
                    {
                        "field": "position_mark_value_twd",
                        "type": "quantitative",
                        "label": "日終部位價值",
                        "unit": "TWD",
                    },
                    {
                        "field": "買方累積淨損益",
                        "type": "quantitative",
                        "label": "買方累積淨損益",
                        "unit": "TWD",
                    },
                    {
                        "field": "賣方累積淨損益_保證金前",
                        "type": "quantitative",
                        "label": "賣方累積淨損益",
                        "unit": "TWD",
                    },
                    {"field": "is_entry_session", "type": "nominal", "label": "進場日"},
                    {"field": "is_expiry_session", "type": "nominal", "label": "到期日"},
                ],
            },
            "palette": {"kind": "categorical", "name": "position-direction"},
            "legend": {"position": "bottom", "interactive": True},
            "valueFormat": "compact",
            "unit": "TWD",
            "referenceLines": [
                {
                    "axis": "y",
                    "value": 0,
                    "label": "損益兩平",
                    "color": "neutral",
                    "lineStyle": "solid",
                }
            ],
            "maxRows": 100,
            "surface": {"viewMode": "both", "showControls": True},
        },
    ]
    recent = deepcopy(charts[0])
    recent.update(
        {
            "id": "cumulative_recent_chart",
            "title": "開盤買跨式每日累積資金報酬（2021至2026）",
            "dataset": "daily_recent",
            "maxRows": 1500,
        }
    )
    return [charts[0], recent, charts[1]]


def _tables() -> list[dict[str, Any]]:
    return [
        {
            "id": "focus_cycle_table",
            "title": "2026年5至7月買方到期週期損益明細",
            "subtitle": "依稅後淨損益排序；到期移動=|正式最後結算價-履約價|",
            "dataset": "focus_cycles",
            "sourceId": "combined_research_evidence",
            "defaultSort": {"field": "net_pnl_twd", "direction": "desc"},
            "density": "dense",
            "columns": [
                {"field": "entry_date", "label": "進場日", "type": "date"},
                {"field": "expiry_date", "label": "到期日", "type": "date"},
                {"field": "option_series", "label": "契約", "type": "text"},
                {"field": "strike", "label": "履約價", "format": "number"},
                {
                    "field": "opening_premium_points",
                    "label": "開倉雙腿權利金 點",
                    "format": "number",
                },
                {
                    "field": "terminal_value_points",
                    "label": "到期移動 點",
                    "format": "number",
                },
                {
                    "field": "official_final_settlement_points",
                    "label": "最後結算價",
                    "format": "number",
                },
                {
                    "field": "net_pnl_twd",
                    "label": "稅後淨損益 TWD",
                    "format": "number",
                    "movement": True,
                },
            ],
        },
        {
            "id": "cost_comparison_table",
            "title": "換倉頻率與交易成本比較",
            "subtitle": "手續費與法定期交稅均納入；持有期間不同，損益差不可只歸因於成本",
            "dataset": "cost_comparison",
            "sourceId": "combined_research_evidence",
            "defaultSort": {"field": "trading_costs_twd", "direction": "asc"},
            "density": "spacious",
            "columns": [
                {"field": "strategy", "label": "策略", "type": "text"},
                {"field": "observations", "label": "觀測數", "format": "number"},
                {"field": "observation_unit", "label": "單位", "type": "text"},
                {
                    "field": "commissioned_sides",
                    "label": "計費contract-side",
                    "format": "number",
                },
                {
                    "field": "gross_pnl_twd",
                    "label": "毛損益 TWD",
                    "format": "number",
                    "movement": True,
                },
                {"field": "fee_twd", "label": "手續費 TWD", "format": "number"},
                {
                    "field": "transaction_tax_twd",
                    "label": "交易稅 TWD",
                    "format": "number",
                },
                {
                    "field": "trading_costs_twd",
                    "label": "總交易成本 TWD",
                    "format": "number",
                },
                {
                    "field": "net_pnl_twd",
                    "label": "淨損益 TWD",
                    "format": "number",
                    "movement": True,
                },
            ],
        },
        {
            "id": "settlement_validation_table",
            "title": "正式最後結算價對照",
            "subtitle": "表列稅後損益代理誤差絕對值最大的20期；全部策略損益均使用正式結算",
            "dataset": "settlement_validation",
            "sourceId": "combined_research_evidence",
            "defaultSort": {"field": "expiry_date", "direction": "asc"},
            "density": "dense",
            "columns": [
                {"field": "expiry_date", "label": "到期日", "type": "date"},
                {"field": "option_series", "label": "契約", "type": "text"},
                {"field": "strike", "label": "履約價", "format": "number"},
                {
                    "field": "official_final_settlement_points",
                    "label": "正式結算指數",
                    "format": "number",
                },
                {
                    "field": "expiry_last_trade_proxy_terminal_points",
                    "label": "最後成交代理價值",
                    "format": "number",
                },
                {
                    "field": "terminal_value_points",
                    "label": "正式到期價值",
                    "format": "number",
                },
                {
                    "field": "proxy_minus_settlement_terminal_twd",
                    "label": "代理誤差 TWD",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "settlement_transaction_tax_twd",
                    "label": "正式結算稅 TWD",
                    "format": "number",
                },
                {
                    "field": "expiry_last_trade_proxy_settlement_transaction_tax_twd",
                    "label": "代理結算稅 TWD",
                    "format": "number",
                },
            ],
        },
    ]


def _build(
    hold_dir: Path,
    short_hold_dir: Path,
    classic_dir: Path,
    generated_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    hold = _load_json(hold_dir / "summary.json")
    short_hold = _load_json(short_hold_dir / "summary.json")
    classic = _load_json(classic_dir / "summary.json")
    for key, filename in (
        ("cycles", "cycles.parquet"),
        ("annual_results", "annual_results.csv"),
        ("monthly_results", "monthly_results.csv"),
        ("capital_metrics", "capital_metrics.csv"),
        ("daily_results", "daily_results.parquet"),
        ("daily_metrics", "daily_metrics.csv"),
    ):
        _verify(hold_dir / filename, hold["artifacts"][key]["sha256"])
    _verify(
        classic_dir / "daily_results.parquet",
        classic["artifacts"]["daily_results"]["sha256"],
    )
    for key, filename in (
        ("cycles", "cycles.parquet"),
        ("annual_results", "annual_results.csv"),
        ("daily_results", "daily_results.parquet"),
        ("daily_metrics", "daily_metrics.csv"),
    ):
        _verify(short_hold_dir / filename, short_hold["artifacts"][key]["sha256"])

    cycles = pd.read_parquet(hold_dir / "cycles.parquet")
    short_cycles = pd.read_parquet(short_hold_dir / "cycles.parquet")
    annual = pd.read_csv(hold_dir / "annual_results.csv")
    monthly = pd.read_csv(hold_dir / "monthly_results.csv")
    daily = pd.read_parquet(hold_dir / "daily_results.parquet")
    short_daily = pd.read_parquet(short_hold_dir / "daily_results.parquet")
    daily_metrics = pd.read_csv(hold_dir / "daily_metrics.csv").iloc[0]
    classic_daily = pd.read_parquet(classic_dir / "daily_results.parquet")
    results = hold["results"]
    short_results = short_hold["results"]
    coverage = hold["coverage"]
    classic_results = classic["results"]
    classic_coverage = classic["coverage"]
    cycle_keys = ["entry_date", "expiry_date", "option_series", "strike"]
    if not cycles[cycle_keys].equals(short_cycles[cycle_keys]):
        raise ValueError("long/short hold cycles do not use identical contracts")
    if not (
        cycles["gross_pnl_twd"] + short_cycles["gross_pnl_twd"]
    ).abs().le(1e-9).all():
        raise ValueError("long/short cycle gross P&L is not exactly opposite")
    for field in ("fee_twd", "transaction_tax_twd"):
        if not cycles[field].equals(short_cycles[field]):
            raise ValueError(f"long/short cycle {field} differs")
    annual["period"] = annual["year"].astype(str)
    annual.loc[annual["year"].eq(2012), "period"] += "（部分）"
    annual.loc[annual["year"].eq(2026), "period"] += "（部分）"
    daily["trading_date"] = pd.to_datetime(daily["trading_date"])
    if daily.empty:
        raise ValueError("daily hold-to-expiry results are empty")
    if daily["trading_date"].duplicated().any():
        duplicates = daily.loc[
            daily["trading_date"].duplicated(keep=False), "trading_date"
        ]
        raise ValueError(
            "daily hold-to-expiry results contain duplicate sessions: "
            f"{duplicates.dt.strftime('%Y-%m-%d').head(10).tolist()}"
        )
    if not daily["trading_date"].is_monotonic_increasing:
        raise ValueError("daily hold-to-expiry results are not chronological")
    if len(daily) != int(coverage["source_sessions"]):
        raise ValueError(
            "daily result/session coverage mismatch: "
            f"{len(daily)} != {coverage['source_sessions']}"
        )
    if abs(
        float(daily["net_after_fee_and_tax_twd"].sum())
        - float(results["net_pnl_twd"])
    ) > 1e-6:
        raise ValueError("daily taxed P&L does not reconcile to cycle results")
    if abs(
        float(daily.iloc[-1]["cumulative_net_pnl_twd"])
        - float(results["net_pnl_twd"])
    ) > 1e-6:
        raise ValueError("daily cumulative endpoint does not match summary")
    short_daily["trading_date"] = pd.to_datetime(short_daily["trading_date"])
    if short_daily["trading_date"].duplicated().any():
        raise ValueError("short daily results contain duplicate sessions")
    if not daily["trading_date"].equals(short_daily["trading_date"]):
        raise ValueError("long/short daily results do not use identical sessions")
    if abs(
        float(short_daily["net_after_fee_and_tax_twd"].sum())
        - float(short_results["net_pnl_twd"])
    ) > 1e-6:
        raise ValueError("short daily P&L does not reconcile to summary")
    if abs(
        float(short_daily.iloc[-1]["cumulative_net_pnl_twd"])
        - float(short_results["net_pnl_twd"])
    ) > 1e-6:
        raise ValueError("short daily cumulative endpoint does not match summary")

    daily_chart = daily[
        [
            "trading_date",
            "option_series",
            "is_entry_session",
            "is_expiry_session",
            "position_mark_value_twd",
            "commission_twd",
            "transaction_tax_twd",
            "net_after_fee_and_tax_twd",
            "cumulative_net_pnl_twd",
            "drawdown_twd",
            "cumulative_return_on_capital",
        ]
    ].copy()
    daily_chart = daily_chart.rename(
        columns={
            "net_after_fee_and_tax_twd": "long_net_after_fee_and_tax_twd",
            "cumulative_net_pnl_twd": "long_cumulative_net_pnl_twd",
            "drawdown_twd": "long_drawdown_twd",
        }
    )
    daily_chart["short_net_after_fee_and_tax_twd"] = short_daily[
        "net_after_fee_and_tax_twd"
    ].to_numpy()
    daily_chart["short_cumulative_net_pnl_twd"] = short_daily[
        "cumulative_net_pnl_twd"
    ].to_numpy()
    daily_chart["short_drawdown_twd"] = short_daily["drawdown_twd"].to_numpy()
    # Preserve the existing long-only research fields for the established
    # May-July concentration analysis while adding explicit comparison fields.
    daily_chart["net_after_fee_and_tax_twd"] = daily_chart[
        "long_net_after_fee_and_tax_twd"
    ]
    daily_chart["cumulative_net_pnl_twd"] = daily_chart[
        "long_cumulative_net_pnl_twd"
    ]
    daily_chart["drawdown_twd"] = daily_chart["long_drawdown_twd"]
    daily_chart["買方當日淨損益"] = daily_chart[
        "long_net_after_fee_and_tax_twd"
    ]
    daily_chart["賣方當日淨損益_保證金前"] = daily_chart[
        "short_net_after_fee_and_tax_twd"
    ]
    daily_chart["買方累積淨損益"] = daily_chart[
        "long_cumulative_net_pnl_twd"
    ]
    daily_chart["買方固定一口累積資金報酬"] = daily_chart[
        "cumulative_return_on_capital"
    ]
    daily_chart["賣方累積淨損益_保證金前"] = daily_chart[
        "short_cumulative_net_pnl_twd"
    ]
    daily_early = daily_chart.loc[
        daily_chart["trading_date"].lt(pd.Timestamp("2021-01-01"))
    ].copy()
    daily_recent = daily_chart.loc[
        daily_chart["trading_date"].ge(pd.Timestamp("2021-01-01"))
    ].copy()
    if len(daily_early) > 2000 or len(daily_recent) > 2000:
        raise ValueError(
            "daily chart partitions exceed the portable artifact 2000-row limit: "
            f"early={len(daily_early)}, recent={len(daily_recent)}"
        )
    daily_focus = daily_chart.loc[
        daily_chart["trading_date"].between(FOCUS_START, FOCUS_END)
    ].copy()
    if daily_focus.empty:
        raise ValueError(
            f"no daily results in focus window {FOCUS_START.date()} to {FOCUS_END.date()}"
        )
    daily_focus["calendar_month"] = (
        daily_focus["trading_date"].dt.to_period("M").astype(str)
    )
    focus_daily_months = daily_focus.groupby("calendar_month", as_index=False).agg(
        trading_days=("trading_date", "size"),
        net_pnl_twd=("net_after_fee_and_tax_twd", "sum"),
        short_net_pnl_twd=("short_net_after_fee_and_tax_twd", "sum"),
        positive_days=("net_after_fee_and_tax_twd", lambda values: int((values > 0).sum())),
        negative_days=("net_after_fee_and_tax_twd", lambda values: int((values < 0).sum())),
        largest_gain_twd=("net_after_fee_and_tax_twd", "max"),
        largest_loss_twd=("net_after_fee_and_tax_twd", "min"),
    )
    focus_cycles = cycles.copy()
    focus_cycles["entry_date"] = pd.to_datetime(focus_cycles["entry_date"])
    focus_cycles["expiry_date"] = pd.to_datetime(focus_cycles["expiry_date"])
    focus_cycles = focus_cycles.loc[
        focus_cycles["expiry_date"].between(FOCUS_START, FOCUS_END)
    ].copy()
    if focus_cycles.empty:
        raise ValueError(
            f"no completed cycles in focus window {FOCUS_START.date()} to {FOCUS_END.date()}"
        )
    focus_cycle_net = float(focus_cycles["net_pnl_twd"].sum())
    focus_cycles["expiry_month"] = (
        focus_cycles["expiry_date"].dt.to_period("M").astype(str)
    )
    focus_cycle_months = focus_cycles.groupby("expiry_month", as_index=False).agg(
        cycles=("option_series", "size"),
        net_pnl_twd=("net_pnl_twd", "sum"),
    )
    top_focus_cycles = focus_cycles.nlargest(6, "net_pnl_twd")
    top_focus_cycle_net = float(top_focus_cycles["net_pnl_twd"].sum())
    top_focus_days = daily_focus.nlargest(5, "net_after_fee_and_tax_twd")
    top_focus_day_net = float(top_focus_days["net_after_fee_and_tax_twd"].sum())
    focus_daily_net = float(daily_focus["net_after_fee_and_tax_twd"].sum())
    expected_focus_months = {"2026-05", "2026-06", "2026-07"}
    if set(focus_daily_months["calendar_month"]) != expected_focus_months:
        raise ValueError("focus daily results do not cover 2026-05 through 2026-07")
    if set(focus_cycle_months["expiry_month"]) != expected_focus_months:
        raise ValueError("focus cycles do not cover 2026-05 through 2026-07")
    focus_daily_by_month = focus_daily_months.set_index("calendar_month")
    focus_cycle_by_month = focus_cycle_months.set_index("expiry_month")
    top_focus_day_lines = "\n".join(
        f"- {row.trading_date:%Y-%m-%d}: **{_money(float(row.net_after_fee_and_tax_twd))}**"
        for row in top_focus_days.itertuples(index=False)
    )
    daily["month"] = daily["trading_date"].dt.to_period("M").astype(str)
    daily_monthly = daily.groupby("month", as_index=False).agg(
        period_end_cumulative_net_pnl_twd=("cumulative_net_pnl_twd", "last"),
        daily_worst_drawdown_twd=("drawdown_twd", "min"),
    )
    monthly = monthly.drop(
        columns=["period_end_cumulative_net_pnl_twd", "worst_drawdown_twd"]
    ).merge(daily_monthly, on="month", how="left", validate="one_to_one")
    monthly = monthly.rename(
        columns={"daily_worst_drawdown_twd": "worst_drawdown_twd"}
    )
    monthly["month"] = monthly["month"].astype(str) + "-01"

    through_2025 = float(annual.loc[annual["year"].le(2025), "net_pnl_twd"].sum())
    pnl_2026 = float(annual.loc[annual["year"].eq(2026), "net_pnl_twd"].sum())
    classic_executable = classic_daily.loc[classic_daily["executable"]].copy()
    classic_transaction_tax_twd = float(
        sum(
            option_premium_transaction_tax_twd(
                premium,
                multiplier_twd_per_point=50.0,
                tax_rate=TAIFEX_OPTION_PREMIUM_TAX_RATE,
            )
            for row in classic_executable[
                ["call_open", "put_open", "call_close", "put_close"]
            ].itertuples(index=False, name=None)
            for premium in row
        )
    )
    classic_net_after_fee_tax_twd = (
        float(classic_results["net_pnl_twd"]) - classic_transaction_tax_twd
    )
    hold_trading_costs_twd = float(results["fees_twd"]) + float(
        results["transaction_taxes_twd"]
    )
    short_hold_trading_costs_twd = float(short_results["fees_twd"]) + float(
        short_results["transaction_taxes_twd"]
    )
    if abs(short_hold_trading_costs_twd - hold_trading_costs_twd) > 1e-9:
        raise ValueError("long/short hold trading costs differ")
    classic_trading_costs_twd = float(
        classic_results["fees_twd"]
    ) + classic_transaction_tax_twd
    cost_savings_twd = classic_trading_costs_twd - hold_trading_costs_twd
    cost_savings_share = 1.0 - hold_trading_costs_twd / classic_trading_costs_twd
    commissioned_sides = int(coverage["complete_cycles"]) * 2
    classic_sides = int(classic_coverage["executable_sessions"]) * 4

    validation = cycles.loc[
        cycles["terminal_value_source"].eq(
            "official_taifex_final_settlement_price"
        )
    ].copy()
    validation["proxy_net_pnl_twd"] = (
        validation["net_pnl_twd"]
        + validation["proxy_minus_settlement_terminal_twd"]
        - validation["proxy_minus_settlement_transaction_tax_twd"]
    )
    validation = validation[
        [
            "entry_date",
            "expiry_date",
            "option_series",
            "strike",
            "official_final_settlement_points",
            "expiry_last_trade_proxy_terminal_points",
            "terminal_value_points",
            "proxy_minus_settlement_terminal_twd",
            "settlement_transaction_tax_twd",
            "expiry_last_trade_proxy_settlement_transaction_tax_twd",
            "proxy_minus_settlement_transaction_tax_twd",
            "proxy_net_pnl_twd",
            "net_pnl_twd",
        ]
    ]
    validation_comparable = validation.loc[
        validation["proxy_net_pnl_twd"].notna()
    ].copy()
    validation_proxy_net = float(
        validation_comparable["proxy_net_pnl_twd"].sum()
    )
    validation_official_net = float(validation_comparable["net_pnl_twd"].sum())
    validation_difference = validation_proxy_net - validation_official_net
    validation_cycle_difference = (
        validation_comparable["proxy_net_pnl_twd"]
        - validation_comparable["net_pnl_twd"]
    )
    validation_detail = (
        validation_comparable.assign(
            _absolute_taxed_net_difference=validation_cycle_difference.abs()
        )
        .sort_values("_absolute_taxed_net_difference", ascending=False)
        .head(20)
        .drop(columns="_absolute_taxed_net_difference")
    )

    cost_comparison = pd.DataFrame(
        [
            {
                "strategy": "開盤買Call＋Put，持有到期",
                "observations": int(coverage["complete_cycles"]),
                "observation_unit": "到期週期",
                "commissioned_sides": commissioned_sides,
                "fee_twd": float(results["fees_twd"]),
                "transaction_tax_twd": float(results["transaction_taxes_twd"]),
                "trading_costs_twd": hold_trading_costs_twd,
                "gross_pnl_twd": float(results["gross_pnl_twd"]),
                "net_pnl_twd": float(results["net_pnl_twd"]),
            },
            {
                "strategy": "開盤賣Call＋Put，持有到期（保證金前）",
                "observations": int(coverage["complete_cycles"]),
                "observation_unit": "到期週期",
                "commissioned_sides": commissioned_sides,
                "fee_twd": float(short_results["fees_twd"]),
                "transaction_tax_twd": float(
                    short_results["transaction_taxes_twd"]
                ),
                "trading_costs_twd": short_hold_trading_costs_twd,
                "gross_pnl_twd": float(short_results["gross_pnl_twd"]),
                "net_pnl_twd": float(short_results["net_pnl_twd"]),
            },
            {
                "strategy": "每日開盤／收盤 Classic",
                "observations": int(classic_coverage["executable_sessions"]),
                "observation_unit": "交易日",
                "commissioned_sides": classic_sides,
                "fee_twd": float(classic_results["fees_twd"]),
                "transaction_tax_twd": classic_transaction_tax_twd,
                "trading_costs_twd": classic_trading_costs_twd,
                "gross_pnl_twd": float(classic_results["gross_pnl_twd"]),
                "net_pnl_twd": classic_net_after_fee_tax_twd,
            },
        ]
    )
    headline = pd.DataFrame(
        [
            {
                "net_pnl_twd": float(results["net_pnl_twd"]),
                "short_net_pnl_twd": float(short_results["net_pnl_twd"]),
                "fees_twd": float(results["fees_twd"]),
                "transaction_taxes_twd": float(
                    results["transaction_taxes_twd"]
                ),
                "trading_costs_twd": hold_trading_costs_twd,
                "cost_savings_share": cost_savings_share,
                "win_rate": float(results["win_rate"]),
                "short_win_rate": float(short_results["win_rate"]),
                "median_net_pnl_twd": float(results["median_net_pnl_twd"]),
                "short_median_net_pnl_twd": float(
                    short_results["median_net_pnl_twd"]
                ),
                "one_lot_cumulative_return_on_capital": float(
                    results["one_lot_cumulative_return_on_capital"]
                ),
                "annualized_sharpe": float(daily_metrics["annualized_sharpe"]),
                "through_2025_net_pnl_twd": through_2025,
            }
        ]
    )
    evidence = {
        "generated_at": generated_at,
        "hold_summary": hold,
        "short_hold_summary": short_hold,
        "classic_summary": classic,
        "headline": _records(headline),
        "cost_comparison": _records(cost_comparison),
        "official_settlement_validation": _records(validation_comparable),
        "focus_daily_months": _records(focus_daily_months),
        "focus_cycles": _records(focus_cycles),
        "focus_top_days": _records(top_focus_days),
        "chart_map": [
            {
                "section": "long_run_path_2012_2020",
                "question": "稅後總損益在2012至2020的每個交易日如何累積",
                "family": "Trend",
                "type": "line",
                "fields": [
                    "trading_date",
                    "cumulative_return_on_capital",
                ],
                "palette_policy": "single-root preferred",
                "delivery": "report.html",
            },
            {
                "section": "long_run_path_2021_2026",
                "question": "稅後總損益在2021至2026的每個交易日如何累積",
                "family": "Trend",
                "type": "line",
                "fields": [
                    "trading_date",
                    "cumulative_return_on_capital",
                ],
                "palette_policy": "single-root preferred",
                "delivery": "report.html",
            },
            {
                "section": "may_july_daily_concentration",
                "question": "2026年5至7月獲利是否集中在少數交易日",
                "family": "Trend",
                "type": "bar",
                "fields": [
                    "trading_date",
                    "long_net_after_fee_and_tax_twd",
                    "short_net_after_fee_and_tax_twd",
                ],
                "palette_policy": "hard two-root cap with zero reference",
                "delivery": "report.html",
            },
        ],
    }
    sources = _sources(generated_at, coverage)
    cards = _cards(complete_cycles=int(coverage["complete_cycles"]))
    charts = _charts()
    tables = _tables()
    title = "最快到期週選 ATM Straddle：開盤買 vs 開盤賣持有到期"
    official_phrase = (
        f"少 {_money(-validation_difference)}"
        if validation_difference < 0.0
        else f"多 {_money(validation_difference)}"
    )
    blocks = [
        {"id": "title", "type": "markdown", "body": f"# {title}"},
        {
            "id": "technical_summary",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                "## Executive Summary\n\n"
                f"- **開盤買 Call＋Put 有利，反向全賣沒有：**同一批 {coverage['complete_cycles']} 個完整週期，買方稅後淨損益 **{_money(float(results['net_pnl_twd']))}**；賣方為 **{_money(float(short_results['net_pnl_twd']))}**。\n"
                f"- **毛損益完全對稱，成本不會反向：**買／賣毛損益分別 {_money(float(results['gross_pnl_twd']))} 與 {_money(float(short_results['gross_pnl_twd']))}；兩邊各付手續費 {_money(float(results['fees_twd']))}、交易稅 {_money(float(results['transaction_taxes_twd']))}。\n"
                f"- **賣方高勝率仍被尾部虧損壓過：**賣方 {int(short_results['winning_cycles'])}/{coverage['complete_cycles']} 期獲利、週期中位數 {_money(float(short_results['median_net_pnl_twd']))}，但全期仍虧 {_money(-float(short_results['net_pnl_twd']))}。這是保證金前損益，不報賣方報酬率。\n"
                f"- **到期端已改為完整正式結算：**{coverage['official_final_settlement_cycles']} 期全部使用期交所最後結算價；其中 {coverage['expiry_last_trade_validation_cycles']} 期另保留最後成交代理，只作誤差驗證，不進策略損益。"
            ),
        },
        {
            "id": "headline_metrics",
            "type": "metric-strip",
            "cardIds": [card["id"] for card in cards],
        },
        {
            "id": "cost_finding",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                "## 同一批週選中，開盤買C＋P為正、開盤賣C＋P為負\n\n"
                f"買方與賣方使用完全相同的 {coverage['complete_cycles']} 組進場日、契約、履約價及正式最後結算價。"
                f"毛損益互為相反數：買方 {_money(float(results['gross_pnl_twd']))}、賣方 {_money(float(short_results['gross_pnl_twd']))}；"
                f"手續費與交易稅各自都要支付 {_money(hold_trading_costs_twd)}，因此稅後結果分別為 **{_money(float(results['net_pnl_twd']))}** 與 **{_money(float(short_results['net_pnl_twd']))}**。"
                f"相較每天開平的Classic，持有到期的每個方向少付 {_money(cost_savings_twd)}（{cost_savings_share:.1%}）成本；但持有區間不同，不能把策略損益差全歸因於成本。"
            ),
        },
        {"id": "cost_table", "type": "table", "tableId": "cost_comparison_table"},
        {
            "id": "path_finding",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                "## 每日曲線顯示買方靠少數大波動週上升，賣方承受對稱尾部虧損\n\n"
                f"2012起到2025年底仍為 **{_money(through_2025)}**；2026截至8月7日為 **{_money(pnl_2026)}**。"
                "下圖是3,348個交易日的買方固定一口累積資金報酬；分母固定為全樣本最大單期開倉資金，因此不會因高權利金週期投入較多就放大報酬。賣方缺少歷史保證金契約，不畫資金報酬。"
            ),
        },
        {
            "id": "cumulative_early",
            "type": "chart",
            "chartId": "cumulative_early_chart",
        },
        {
            "id": "recent_path_finding",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                "## 2021至2026的分歧主要發生在2026年6、7月\n\n"
                "下圖延續上圖的同一固定資金累積報酬尺度，不是重置績效。"
                "2025底累積僅為 "
                f"**{_money(through_2025)}**，之後買方的大部分上升與賣方的大額下滑，都來自2026年6、7月少數日的對稱標記變動與雙方各自成本。"
            ),
        },
        {
            "id": "cumulative_recent",
            "type": "chart",
            "chartId": "cumulative_recent_chart",
        },
        {
            "id": "focus_finding",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                "## 6、7月的買方獲利對應賣方大額虧損，5月兩邊都接近持平\n\n"
                f"買方按日曆月份加總，5月 **{_money(float(focus_daily_by_month.loc['2026-05', 'net_pnl_twd']))}**、6月 **{_money(float(focus_daily_by_month.loc['2026-06', 'net_pnl_twd']))}**、7月 **{_money(float(focus_daily_by_month.loc['2026-07', 'net_pnl_twd']))}**；"
                f"賣方同期為5月 **{_money(float(focus_daily_by_month.loc['2026-05', 'short_net_pnl_twd']))}**、6月 **{_money(float(focus_daily_by_month.loc['2026-06', 'short_net_pnl_twd']))}**、7月 **{_money(float(focus_daily_by_month.loc['2026-07', 'short_net_pnl_twd']))}**。"
                f"最大5個獲利日就貢獻 **{_money(top_focus_day_net)}**，占這三個月淨利 **{top_focus_day_net / focus_daily_net:.1%}**：\n\n"
                f"{top_focus_day_lines}\n\n"
                f"以到期週期歸屬月份則是5月 {_money(float(focus_cycle_by_month.loc['2026-05', 'net_pnl_twd']))}、6月 {_money(float(focus_cycle_by_month.loc['2026-06', 'net_pnl_twd']))}、7月 {_money(float(focus_cycle_by_month.loc['2026-07', 'net_pnl_twd']))}；"
                "與日曆月份不同，是因為4/30進場的週期在5月到期，6/29進場的週期則在7月到期。"
                f"前6大獲利週期合計 **{_money(top_focus_cycle_net)}**，占5至7月到期週期淨利 **{top_focus_cycle_net / focus_cycle_net:.1%}**。"
            ),
        },
        {"id": "daily_focus", "type": "chart", "chartId": "daily_focus_chart"},
        {"id": "focus_cycles", "type": "table", "tableId": "focus_cycle_table"},
        {
            "id": "definitions",
            "type": "markdown",
            "body": (
                "## 樣本、價格與資金指標定義\n\n"
                f"- 範圍：{coverage['source_first_date']}–{coverage['source_last_date']}，{coverage['complete_cycles']}期；平均持有 {coverage['mean_holding_sessions']:.2f} 個交易日。\n"
                "- 開倉：空手時用TX近月日盤開盤決定最快到期W/F週選ATM；買方各買一口Call/Put，賣方對同一組合約各賣一口。\n"
                "- 持有：到期前不換月份或履約價；到期後下一個符合條件交易日才買下一期。\n"
                "- 每日結算：持有期間優先用官方每日選擇權結算價；欄位缺漏時才用同契約日盤最後成交作EOD標記。\n"
                "- 到期：全部週期都用期交所正式最後結算價計算 `abs(S-K)`，最後成交只留作對照。\n"
                "- 費用：兩個開倉contract-side各22元；現金結算不另列到期手續費。買賣方向都支付相同費用與交易稅。\n"
                "- 進場稅：Call與Put各按權利金金額0.1%，逐口元以下四捨五入。\n"
                "- 到期稅：只對自動履約的價內腿，按結算指數契約金額課股價類期貨稅；2013-03-31前0.004%，2013-04-01起0.002%，逐口四捨五入。\n"
                f"- 買方資金基準為最大單期權利金、手續費與進場稅合計 {_money(float(results['capital_base_twd']))}；一口累積報酬 {float(results['one_lot_cumulative_return_on_capital']):.1%} 不是CAGR。賣方因未建歷史保證金，不計報酬率。"
            ),
        },
        {
            "id": "methodology",
            "type": "markdown",
            "body": (
                "## 方法與帳務驗證\n\n"
                "每個方向每期建立四列不可變交易帳：Call與Put各一列開倉、各一列到期結算。逐期驗證合約與履約價固定、下一期晚於前一期到期、兩腿最終口數為零，且淨損益等於權利金現金流減手續費與交易稅。"
                "另逐期檢查買賣雙方毛損益互為相反數、成本完全相同，並把3,348個交易日逐日加總回各自總帳。"
                "一般選擇權交易與到期現金結算使用不同稅基；每口先四捨五入，再加總。"
                "資金報酬重用既有TAIFEX一口長選擇權資金帳務，沒有另建報酬公式。"
            ),
        },
        {
            "id": "settlement_finding",
            "type": "markdown",
            "sourceId": "combined_research_evidence",
            "body": (
                f"## {coverage['official_final_settlement_cycles']}期已全用正式結算，最後成交代理不再決定損益\n\n"
                f"可對照的 {coverage['expiry_last_trade_validation_cycles']} 期中，最後成交代理相對正式結算的稅後損益合計{official_phrase}；單期誤差介於 {_money(float(validation_cycle_difference.min()))} 到 {_money(float(validation_cycle_difference.max()))}。"
                "這個比較現在只量化舊代理偏差；正式策略曲線與到期稅基都不再依賴代理。"
            ),
        },
        {
            "id": "settlement_table",
            "type": "table",
            "tableId": "settlement_validation_table",
        },
        {
            "id": "limitations",
            "type": "markdown",
            "body": (
                "## 限制、不確定性與本次沒有加入的條件\n\n"
                "1. **開倉不是Bid/Ask可成交價。** Call與Put日開盤成交可能不同時。\n"
                f"2. **持有曲線只在日終估值。** {int(daily_metrics['official_daily_settlement_leg_marks'])} 個持倉腿日全部使用官方每日結算價，最後成交fallback為 {int(daily_metrics['daily_last_trade_fallback_leg_marks'])}；仍看不到盤中浮動。\n"
                "3. **沒有逐筆深度歷史。** 官方免費日檔不能重建委託簿。\n"
                "4. **沒有額外壓力測試。** 本次只加入法定稅費；沒有另加滑價、容量或人為價差。\n"
                f"5. **回撤為日終估值。** 買方每日最大回撤 {_money(float(daily_metrics['maximum_drawdown_twd']))}，仍不含盤中浮虧。\n"
                "6. **賣方不是正式可執行回測。** 目前只有每日按市價的保證金前台幣損益；歷史保證金、盤中追繳、強制平倉與帳戶歸零尚未建模，因此不呈現賣方Sharpe或報酬率。"
            ),
        },
        {
            "id": "next_steps",
            "type": "markdown",
            "body": (
                "## 建議下一步\n\n"
                "1. 賣方在保證金前已虧損，因此依你的規則不再對賣方疊加保證金壓力、滑價或容量限制。\n"
                "2. 買方在日開盤成交代理下為正；下一個必要資料校驗是用近期同步Ask估計兩腿開倉偏差。\n"
                "3. 官方免費長期日檔沒有歷史Bid/Ask，因此只能以前瞻同步收集校準，不能回造舊掛單簿。\n"
                "4. 只有Ask校準後仍為正，才評估你先前暫停的其他真實限制。"
            ),
        },
        {
            "id": "questions",
            "type": "markdown",
            "body": (
                "## 尚待回答\n\n"
                "- 近期同步Ask相對日開盤成交的偏差有多大？\n"
                "- 2026的極端日開盤後，Call與Put兩腿實際可同時成交的價格會不會吃掉主要利潤？"
            ),
        },
    ]
    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": title,
            "description": "最快到期W/F週選ATM Straddle開盤買入與開盤賣出、固定持有至到期的稅後比較。",
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
                "cost_comparison": _records(cost_comparison),
                "daily_early": _records(daily_early),
                "daily_recent": _records(daily_recent),
                "daily_focus": _records(daily_focus),
                "focus_cycles": _records(focus_cycles),
                "monthly": _records(monthly),
                "annual": _records(annual),
                "settlement_validation": _records(validation_detail),
            },
        },
        "sources": sources,
    }
    return artifact, evidence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hold-dir", type=Path, default=DEFAULT_HOLD_DIR)
    parser.add_argument(
        "--short-hold-dir", type=Path, default=DEFAULT_SHORT_HOLD_DIR
    )
    parser.add_argument("--classic-dir", type=Path, default=DEFAULT_CLASSIC_DIR)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    hold_dir = args.hold_dir.expanduser().resolve()
    short_hold_dir = args.short_hold_dir.expanduser().resolve()
    classic_dir = args.classic_dir.expanduser().resolve()
    output = (args.output or hold_dir / "artifact.json").expanduser().resolve()
    generated_at = datetime.now(timezone.utc).isoformat()
    artifact, evidence = _build(
        hold_dir, short_hold_dir, classic_dir, generated_at
    )
    _atomic_json(hold_dir / "report_evidence.json", evidence)
    _atomic_json(output, artifact)
    print(f"wrote canonical report artifact {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
