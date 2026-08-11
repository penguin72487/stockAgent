#!/usr/bin/env python3
"""Build the canonical portable report for captured Bid/Ask arbitrage ceilings."""

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
    "artifacts/research/taifex_bidask_arbitrage"
)
LABELS: Final[dict[str, str]] = {
    "put_call_parity_tx": "Put–Call parity + TX",
    "call_vertical_bounds": "Call vertical",
    "put_vertical_bounds": "Put vertical",
    "call_butterfly_bounds": "Call butterfly",
    "put_butterfly_bounds": "Put butterfly",
    "box_spread": "Box spread",
}


def _twd(value: float) -> str:
    return f"{value:,.0f} 元"


def build_artifact(input_dir: Path) -> dict[str, Any]:
    summary = pl.read_parquet(input_dir / "summary.parquet")
    layer_overall = pl.read_parquet(input_dir / "layer_overall_summary.parquet")
    fixed_waterfall = pl.read_parquet(
        input_dir / "layer_fixed_package_waterfall.parquet"
    )
    trade_print_summary = pl.read_parquet(
        input_dir / "same_second_trade_summary.parquet"
    )
    layer_meta = json.loads(
        (input_dir / "layer_summary.json").read_text(encoding="utf-8")
    )
    quality = json.loads(
        (input_dir / "data_quality.json").read_text(encoding="utf-8")
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for row in summary.iter_rows(named=True):
        label = LABELS[row["variant_id"]]
        rows.append(
            {
                "delivery_month": row["delivery_month"],
                "expiry": row["expiry"].isoformat(),
                "variant_id": row["variant_id"],
                "strategy": f"{row['delivery_month']} {label}",
                "method": label,
                "evaluable_seconds": row["evaluable_snapshot_seconds"],
                "positive_seconds": row["positive_snapshot_seconds"],
                "positive_fraction": row["positive_snapshot_fraction"],
                "best_gross_edge_twd": row["best_gross_locked_edge_twd"],
                "best_entry_fees_twd": row["best_entry_fixed_fees_twd"],
                "best_entry_tax_twd": row["best_entry_transaction_tax_twd"],
                "best_settlement_tax_twd": row[
                    "best_estimated_settlement_tax_twd"
                ],
                "best_net_before_settlement_tax_twd": row[
                    "best_net_before_settlement_tax_twd"
                ],
                "best_net_after_all_costs_twd": row[
                    "best_net_after_estimated_settlement_tax_twd"
                ],
                "best_time": row["best_snapshot_ts"],
                "best_direction": row["best_direction"],
                "best_strikes": row["best_strikes_json"],
                "best_max_book_age_ms": row["best_max_book_age_ms"],
                "best_legs": row["best_legs_json"],
            }
        )
    rows.sort(key=lambda item: item["best_net_after_all_costs_twd"], reverse=True)
    layer_rows = [
        {
            **row,
            "best_expiry": (
                row["best_expiry"].isoformat()
                if hasattr(row.get("best_expiry"), "isoformat")
                else row.get("best_expiry")
            ),
            "best_method": LABELS[row["best_variant_id"]],
        }
        for row in layer_overall.iter_rows(named=True)
    ]
    fixed_rows = [
        {
            **row,
            "expiry": (
                row["expiry"].isoformat()
                if hasattr(row.get("expiry"), "isoformat")
                else row.get("expiry")
            ),
        }
        for row in fixed_waterfall.iter_rows(named=True)
    ]
    trade_rows = [
        {
            **row,
            "expiry": row["expiry"].isoformat(),
            "method": LABELS[row["variant_id"]],
            "strategy": f"{row['delivery_month']} {LABELS[row['variant_id']]}",
        }
        for row in trade_print_summary.iter_rows(named=True)
    ]
    layer_by_order = {row["layer_order"]: row for row in layer_rows}
    l0 = layer_by_order[0]
    l1 = layer_by_order[1]
    l2 = layer_by_order[2]
    l5 = layer_by_order[5]
    print_control = layer_meta["same_second_trade_print_control"]
    headline = [
        {
            "final_executable_ceiling_twd": l5["max_layer_value_twd"],
            "active_no_depth_ceiling_twd": l1["max_layer_value_twd"],
            "active_no_depth_positive_seconds": l1["positive_method_seconds"],
            "level1_ceiling_twd": l2["max_layer_value_twd"],
            "level1_positive_seconds": l2["positive_method_seconds"],
            "tested_method_expiries": len(rows),
            "snapshot_seconds": quality["snapshot_seconds"],
            "valid_book_fraction": quality["valid_book_fraction"],
        }
    ]

    sources = [
        {
            "id": "layer_source",
            "label": "六層套利漏斗摘要",
            "path": "artifacts/research/taifex_bidask_arbitrage/layer_overall_summary.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "同一 capture 中，每層重新搜尋所有同步秒、方法、到期月、履約價與方向後的全域理想上限。",
                "sql": "SELECT * FROM read_parquet('artifacts/research/taifex_bidask_arbitrage/layer_overall_summary.parquet') ORDER BY layer_order",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/layer_overall_summary.parquet"
                ],
                "filters": [
                    "trade_date = 2026-08-10",
                    "capture_id = 8775e9d020044cf881261622a803e9e1",
                    "same valid non-stale uncrossed books in every strict layer",
                    "zero added latency and slippage",
                ],
                "metric_definitions": {
                    "max_layer_value_twd": "該層允許的全部時間、方法、到期月、履約價和方向中，最高的無模型到期鎖定損益。",
                    "positive_method_seconds": "以方法與到期月為單位，每個同步秒的最佳 package 高於零的筆數。",
                    "change_from_previous_layer_twd": "該層重新最佳化後的最高值減前一層重新最佳化後的最高值；不是固定組合的成本歸因。",
                },
            },
        },
        {
            "id": "fixed_waterfall_source",
            "label": "固定 L5 最佳組合的逐層成本瀑布",
            "path": "artifacts/research/taifex_bidask_arbitrage/layer_fixed_package_waterfall.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "固定最終層全域最佳 package，以同一秒和同一履約價逐項加入價差、量、手續費與稅。",
                "sql": "SELECT * FROM read_parquet('artifacts/research/taifex_bidask_arbitrage/layer_fixed_package_waterfall.parquet') ORDER BY layer_order",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/layer_fixed_package_waterfall.parquet"
                ],
                "filters": ["freeze globally best L5 package"],
            },
        },
        {
            "id": "trade_print_source",
            "label": "同一本地秒逐筆成交旁證",
            "path": "artifacts/research/taifex_bidask_arbitrage/same_second_trade_summary.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "每條腿在同一本地接收秒都有逐筆成交時，以該秒最後一筆價格形成的觀察控制。",
                "sql": "SELECT * FROM read_parquet('artifacts/research/taifex_bidask_arbitrage/same_second_trade_summary.parquet') ORDER BY delivery_month, variant_id",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/same_second_trade_summary.parquet"
                ],
                "filters": [
                    "last print per contract per local receive second",
                    "all package legs must print in that second",
                    "observation only; no simultaneous fill claim",
                ],
            },
        },
        {
            "id": "summary_source",
            "label": "同秒 Bid/Ask 套利上限摘要",
            "path": "artifacts/research/taifex_bidask_arbitrage/summary.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "每一到期月與方法在所有同步秒、履約價和可行方向中的最佳一組主動成交報價。",
                "sql": "SELECT * FROM read_parquet('artifacts/research/taifex_bidask_arbitrage/summary.parquet') ORDER BY best_net_after_estimated_settlement_tax_twd DESC",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/summary.parquet"
                ],
                "filters": [
                    "2026-08-10 capture id 8775e9d020044cf881261622a803e9e1",
                    "buy at best ask and sell at best bid",
                    "all legs fit displayed level-one quantity",
                    "non-stale uncrossed non-simtrade books",
                    "same expiry required for put-call parity",
                ],
                "metric_definitions": {
                    "best_gross_edge_twd": "同秒可成交 Bid/Ask 建倉後，依無模型到期損益下界計算的最高毛套利金額。",
                    "best_net_after_all_costs_twd": "毛套利減進場固定手續費、進場交易稅及估計到期結算稅。",
                    "positive_seconds": "該秒所有履約價與方向中，至少一組完整 package 在全部已建模成本後仍大於零。",
                },
            },
        },
        {
            "id": "snapshot_source",
            "label": "逐秒最佳可成交 package",
            "path": "artifacts/research/taifex_bidask_arbitrage/snapshot_best.parquet",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "每秒、每到期月、每個無模型套利方法的最佳完整一檔 package。",
                "sql": "SELECT * FROM read_parquet('artifacts/research/taifex_bidask_arbitrage/snapshot_best.parquet') ORDER BY delivery_month, variant_id, snapshot_ts_ns",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/snapshot_best.parquet"
                ],
                "filters": [
                    "zero added latency",
                    "zero added slippage",
                    "passive fill not assumed",
                ],
            },
        },
        {
            "id": "quality_source",
            "label": "Shioaji Bid/Ask 捕捉品質稽核",
            "path": "artifacts/research/taifex_bidask_arbitrage/data_quality.json",
            "query": {
                "engine": "duckdb",
                "language": "sql",
                "description": "讀取捕捉涵蓋、有效簿比例、掉事件與執行價格契約。",
                "sql": "SELECT * FROM read_json_auto('artifacts/research/taifex_bidask_arbitrage/data_quality.json')",
                "tables_used": [
                    "artifacts/research/taifex_bidask_arbitrage/data_quality.json"
                ],
                "filters": ["trade_date = 2026-08-10"],
            },
        },
        {
            "id": "capture_source",
            "label": "Shioaji TX/TXO 捕捉清單",
            "path": "data_tw_index_derivatives_ticks/shioaji_fop_captures/manifests/trade_date=2026-08-10/worker=00.json",
        },
        {
            "id": "code_source",
            "label": "可重現六層套利分析器",
            "path": "scripts/analyze_taifex_arbitrage_layers.py",
        },
    ]
    cards = [
        {
            "id": "final_ceiling",
            "description": "第一檔量足夠，並扣除手續費、進場稅及估計到期稅後的全域最高值。",
            "dataset": "headline",
            "sourceId": "layer_source",
            "metrics": [
                {
                    "label": "L5 最終上限",
                    "field": "final_executable_ceiling_twd",
                    "format": "number",
                    "unit": "TWD",
                    "signed": True,
                }
            ],
        },
        {
            "id": "active_no_depth",
            "description": "已用 Ask 買、Bid 賣，但暫時忽略第一檔顯示量。",
            "dataset": "headline",
            "sourceId": "layer_source",
            "metrics": [
                {
                    "label": "L1 最高值",
                    "field": "active_no_depth_ceiling_twd",
                    "format": "number",
                    "unit": "TWD",
                    "signed": True,
                },
                {
                    "label": "正值方法秒",
                    "field": "active_no_depth_positive_seconds",
                    "format": "number",
                },
            ],
        },
        {
            "id": "level1_depth",
            "description": "完整 package 必須由同秒第一檔顯示量覆蓋。",
            "dataset": "headline",
            "sourceId": "layer_source",
            "metrics": [
                {
                    "label": "L2 最高值",
                    "field": "level1_ceiling_twd",
                    "format": "number",
                    "unit": "TWD",
                    "signed": True,
                },
                {
                    "label": "正值方法秒",
                    "field": "level1_positive_seconds",
                    "format": "number",
                },
            ],
        },
        {
            "id": "coverage",
            "description": "完整 capture 中的本地同秒簿快照；有效率以全部契約秒為分母。",
            "dataset": "headline",
            "sourceId": "quality_source",
            "metrics": [
                {
                    "label": "同步秒數",
                    "field": "snapshot_seconds",
                    "format": "number",
                },
                {
                    "label": "有效契約秒比例",
                    "field": "valid_book_fraction",
                    "format": "percent",
                },
            ],
        },
    ]
    charts = [
        {
            "id": "fixed_package_layer_chart",
            "title": "固定同一組合的逐層損益",
            "subtitle": "固定 2026 年 8 月 Put butterfly；從中間價假設逐層加入主動價差、第一檔量與費稅，單位為 TWD。",
            "type": "bar",
            "dataset": "fixed_package_waterfall",
            "sourceId": "fixed_waterfall_source",
            "encodings": {
                "x": {
                    "field": "layer_label_zh",
                    "type": "nominal",
                    "label": "限制層級",
                },
                "y": {
                    "field": "fixed_package_value_twd",
                    "type": "quantitative",
                    "label": "固定組合鎖定損益",
                    "format": "number",
                    "unit": "TWD",
                },
            },
            "valueFormat": "number",
            "unit": "TWD",
            "layout": "full",
        },
        {
            "id": "best_net_chart",
            "title": "每種方法的最佳費稅後套利上限",
            "subtitle": "2026 年 8 月 10 日；每個方法取所有同步秒與履約價中的最高一組，單位為新台幣。",
            "type": "bar",
            "dataset": "method_summary",
            "sourceId": "summary_source",
            "encodings": {
                "x": {
                    "field": "strategy",
                    "type": "nominal",
                    "label": "方法與到期月",
                },
                "y": {
                    "field": "best_net_after_all_costs_twd",
                    "type": "quantitative",
                    "label": "費稅後套利上限",
                    "format": "number",
                    "unit": "TWD",
                },
            },
            "valueFormat": "number",
            "unit": "TWD",
            "layout": "full",
        },
    ]
    tables = [
        {
            "id": "layer_table",
            "title": "六層重新最佳化的理想套利上限",
            "subtitle": "每層都重新搜尋全部同步秒、方法、到期月、履約價與方向；變化欄不是固定組合的成本歸因。",
            "dataset": "layer_overall",
            "sourceId": "layer_source",
            "defaultSort": {"field": "layer_label_zh", "direction": "asc"},
            "columns": [
                {"field": "layer_label_zh", "label": "層級", "type": "text"},
                {"field": "added_factor_zh", "label": "新增因素", "type": "text"},
                {
                    "field": "max_layer_value_twd",
                    "label": "該層最高值 (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "change_from_previous_layer_twd",
                    "label": "相較前層",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "positive_method_seconds",
                    "label": "正值方法秒",
                    "format": "number",
                },
                {"field": "best_method", "label": "最佳方法", "type": "text"},
                {
                    "field": "best_delivery_month",
                    "label": "到期月",
                    "type": "text",
                },
                {
                    "field": "best_snapshot_ts",
                    "label": "最佳時間",
                    "type": "text",
                },
                {
                    "field": "best_strikes_json",
                    "label": "履約價",
                    "type": "text",
                },
            ],
        },
        {
            "id": "fixed_waterfall_table",
            "title": "固定 L5 最佳 package 的可加總成本分解",
            "subtitle": "固定 2026-08-10 11:55:40、44150/44200/44250 Put butterfly；每列只多加入一項因素。",
            "dataset": "fixed_package_waterfall",
            "sourceId": "fixed_waterfall_source",
            "defaultSort": {"field": "layer_label_zh", "direction": "asc"},
            "columns": [
                {"field": "layer_label_zh", "label": "層級", "type": "text"},
                {
                    "field": "fixed_package_value_twd",
                    "label": "固定組合損益 (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "change_from_previous_layer_twd",
                    "label": "本層影響",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "depth_sufficient",
                    "label": "第一檔量足夠",
                    "type": "boolean",
                },
            ],
        },
        {
            "id": "trade_print_table",
            "title": "同一本地秒逐筆成交觀察",
            "subtitle": "所有腿都需在同一本地接收秒出現成交；這不是可同時成交的證明。",
            "dataset": "trade_print_summary",
            "sourceId": "trade_print_source",
            "defaultSort": {
                "field": "max_gross_print_edge_twd",
                "direction": "desc",
            },
            "columns": [
                {"field": "strategy", "label": "方法", "type": "text"},
                {
                    "field": "evaluable_same_second_prints",
                    "label": "完整同秒觀察",
                    "format": "number",
                },
                {
                    "field": "positive_same_second_prints",
                    "label": "毛利為正",
                    "format": "number",
                },
                {
                    "field": "max_gross_print_edge_twd",
                    "label": "最高毛利 (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "max_after_estimated_settlement_tax_twd",
                    "label": "含費稅後最高值",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "positive_after_all_modeled_costs",
                    "label": "費稅後正值觀察",
                    "format": "number",
                },
                {
                    "field": "positive_after_costs_and_print_volume",
                    "label": "費稅及成交量後正值",
                    "format": "number",
                },
                {
                    "field": "best_local_second",
                    "label": "最佳同秒",
                    "type": "text",
                },
                {
                    "field": "best_strikes_json",
                    "label": "履約價",
                    "type": "text",
                },
            ],
        },
        {
            "id": "method_table",
            "title": "所有方法的最佳一組報價",
            "subtitle": "精確費稅分解；負值表示即使零延遲、零滑價也沒有鎖定獲利。",
            "dataset": "method_summary",
            "sourceId": "summary_source",
            "defaultSort": {
                "field": "best_net_after_all_costs_twd",
                "direction": "desc",
            },
            "columns": [
                {"field": "strategy", "label": "方法", "type": "text"},
                {
                    "field": "positive_seconds",
                    "label": "正套利秒數",
                    "format": "number",
                },
                {
                    "field": "best_gross_edge_twd",
                    "label": "毛套利 (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {
                    "field": "best_entry_fees_twd",
                    "label": "手續費",
                    "format": "number",
                },
                {
                    "field": "best_entry_tax_twd",
                    "label": "進場稅",
                    "format": "number",
                },
                {
                    "field": "best_settlement_tax_twd",
                    "label": "估計到期稅",
                    "format": "number",
                },
                {
                    "field": "best_net_after_all_costs_twd",
                    "label": "費稅後上限 (TWD)",
                    "format": "number",
                    "movement": True,
                },
                {"field": "best_time", "label": "最佳時間", "type": "text"},
                {"field": "best_strikes", "label": "履約價", "type": "text"},
                {
                    "field": "best_max_book_age_ms",
                    "label": "最舊腿 (ms)",
                    "format": "number",
                },
            ],
        }
    ]

    blocks = [
        {
            "id": "title",
            "type": "markdown",
            "body": "# TX／TXO 六層理想套利上限",
        },
        {
            "id": "technical_summary",
            "type": "markdown",
            "sourceId": "layer_source",
            "body": (
                "## 技術摘要\n\n"
                f"- **完全不考慮摩擦的中間價上限為 {_twd(l0['max_layer_value_twd'])}，但它不是可成交價格。** 最佳點使用極寬買賣價差的中間值，因此只代表錯把報價中心當流動性的數學上限。\n"
                f"- **改用可主動成交的 Ask／Bid 後，上限只剩 {_twd(l1['max_layer_value_twd'])}，共 {l1['positive_method_seconds']} 個正值方法秒。** 這些都是 TX parity，但 Call／Put 需要各 4 口，第一檔只有 1–2 口。\n"
                f"- **加入第一檔量後，上限立刻變成 {_twd(l2['max_layer_value_twd'])}；再加手續費與稅後為 {_twd(l5['max_layer_value_twd'])}。** 所以這一天沒有一口完整 package 能鎖定正套利。"
            ),
        },
        {
            "id": "headline_cards",
            "type": "metric-strip",
            "cardIds": [
                "final_ceiling",
                "active_no_depth",
                "level1_depth",
                "coverage",
            ],
        },
        {
            "id": "layer_definition",
            "type": "markdown",
            "sourceId": "layer_source",
            "body": (
                "## 六層漏斗把假流動性逐步拿掉\n\n"
                "主漏斗固定同一天與同一組非陳舊、非鎖價報價。L0 假設所有腿都能成交在中間價；L1 改成買 Ask、賣 Bid；L2 再要求完整 package 放得進第一檔；L3、L4、L5 依序扣手續費、進場稅與估計到期稅。每層都重新搜尋最佳時間、方法、到期月、履約價和方向，因此是該層最寬鬆的全域上限。"
            ),
        },
        {
            "id": "layer_table_block",
            "type": "table",
            "tableId": "layer_table",
            "layout": "full",
        },
        {
            "id": "fixed_package_heading",
            "type": "markdown",
            "sourceId": "fixed_waterfall_source",
            "body": (
                "## 固定同一組合後，買賣價差是最大成本\n\n"
                f"固定最終層最佳的 8 月 Put butterfly，同一秒的中間價假設看似可賺 {_twd(fixed_rows[0]['fixed_package_value_twd'])}；一改成主動成交價就變成 {_twd(fixed_rows[1]['fixed_package_value_twd'])}，價差單獨吃掉 {_twd(-fixed_rows[1]['change_from_previous_layer_twd'])}。第一檔量剛好足夠，所以量的金額影響為 0；之後手續費、進場稅與估計到期稅再分別扣 88、124、180 元。"
            ),
        },
        {
            "id": "fixed_package_layer_chart_block",
            "type": "chart",
            "chartId": "fixed_package_layer_chart",
            "layout": "full",
        },
        {
            "id": "fixed_waterfall_table_block",
            "type": "table",
            "tableId": "fixed_waterfall_table",
            "layout": "full",
        },
        {
            "id": "trade_print_heading",
            "type": "markdown",
            "sourceId": "trade_print_source",
            "body": (
                "## 同秒逐筆成交看見正值，但不能當成成交證據\n\n"
                f"11,618 筆原始 ticks 經有效性與同秒聚合後，只有 {print_control['evaluable_method_seconds']} 個方法秒能讓完整 package 的每一腿都在同一本地秒出現成交。最高觀察毛利是 {_twd(print_control['max_gross_print_edge_twd'])}；即使扣除同樣費稅後，最高仍為 {_twd(print_control['max_after_all_modeled_costs_twd'])}。但兩筆正值 parity 的 Call 與 Put 成交量都只有 1 口，低於配 1 口 TX 所需的各 4 口；加上成交量後正值為 {print_control['positive_after_costs_and_print_volume_rows']} 筆，而且歷史成交仍不代表可同時回補。"
            ),
        },
        {
            "id": "trade_print_table_block",
            "type": "table",
            "tableId": "trade_print_table",
            "layout": "full",
        },
        {
            "id": "all_methods_heading",
            "type": "markdown",
            "body": (
                "## 含全部費稅後，所有方法都停在零以下\n\n"
                "下圖比較每個方法在整段捕捉中最有利的一秒。這已經事後挑選最佳秒與履約價；若最高值仍小於零，當天沒有可由主動吃單鎖定的套利。"
            ),
        },
        {
            "id": "best_net_chart_block",
            "type": "chart",
            "chartId": "best_net_chart",
            "layout": "full",
        },
        {
            "id": "chart_interpretation",
            "type": "markdown",
            "sourceId": "summary_source",
            "body": (
                "最接近正值的 8 月 Put butterfly 毛套利為 -500 元；加入 88 元手續費、124 元進場稅與約 180 元到期稅後，上限為 -892 元。其他方法更差。"
            ),
        },
        {
            "id": "detail_table_block",
            "type": "table",
            "tableId": "method_table",
            "layout": "full",
        },
        {
            "id": "execution_definition",
            "type": "markdown",
            "sourceId": "code_source",
            "body": (
                "## 計算方法保持偏向套利者的理想上限\n\n"
                "- 買進腿用當秒最佳 Ask，賣出腿用最佳 Bid；marketable limit 與第一檔量足夠的市價單視為同價。\n"
                "- 蝶式中間腿需 2 口；TX parity 需 4 口 Call、4 口 Put 配 1 口 TX。L2 起完整數量必須放得進第一檔。\n"
                "- 不加滑價、不加下單延遲、資金利率為零，持有到同到期結算；沒有假設被動掛單成交。\n"
                "- 手續費為 TX 60 元、TXO 22 元／口／單邊；進場交易稅按各腿價計算，到期稅以當秒 TX 中間價與最大同時履約腿數估計。"
            ),
        },
        {
            "id": "caveats",
            "type": "markdown",
            "sourceId": "quality_source",
            "body": (
                "## 一天模擬環境資料只能證明當日上限\n\n"
                f"目前只有 2026 年 8 月 10 日一天、{quality['snapshot_seconds']:,} 個同步秒，而且 capture 標示為模擬帳戶環境。全部契約秒的有效簿比例為 {quality['valid_book_fraction']:.2%}；無效部分已直接排除。交換所時間與本地接收時間存在時鐘偏移，因此只用本地接收時間建快照。\n\n"
                "一天資料不能產生有意義的日 Sharpe、Sortino、MDD 或 Calmar。9 月 TXO 沒有同到期 TX，所以不做 9 月 parity；TAIEX 現貨籃子、MTX、TMF 也因缺同步可成交簿而不列入。"
            ),
        },
        {
            "id": "next_steps",
            "type": "markdown",
            "body": (
                "## 下一步先擴充同步報價，再決定是否做實單\n\n"
                "1. 用正式環境連續捕捉至少數週，並同步訂閱同到期 TX、MTX、TMF 與更完整 TXO 履約價。\n"
                "2. 即時監控只在 `Bid/Ask 毛套利 − 手續費 − 進場稅 − 到期稅 > 0` 且所有腿第一檔量足夠時告警。\n"
                "3. 目前不應把同秒逐筆成交正值當成可下單訊號；那個控制只適合找待驗證的報價時段。"
            ),
        },
        {
            "id": "further_questions",
            "type": "markdown",
            "body": (
                "## 還需要回答的問題\n\n"
                "正式帳戶環境的簿是否與模擬環境一致？加入 MTX／TMF 同到期簿後，等值期貨與 TXO parity 是否出現第一檔量足夠且費稅後為正的秒？這兩項都需要新的同步 capture。"
            ),
        },
    ]
    return {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "TX／TXO 六層理想套利上限",
            "description": "2026-08-10 six-layer synchronized TX/TXO model-free arbitrage funnel from midpoint to active Bid/Ask, displayed depth, fees, and taxes.",
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
                "headline": headline,
                "layer_overall": layer_rows,
                "fixed_package_waterfall": fixed_rows,
                "trade_print_summary": trade_rows,
                "method_summary": rows,
            },
        },
        "sources": sources,
        "package_info": {
            "root": "artifacts/research/taifex_bidask_arbitrage",
            "manifestPath": "artifact.json",
            "snapshotPath": "artifact.json",
        },
    }


def _write_layer_markdown(input_dir: Path) -> Path:
    overall = pl.read_parquet(input_dir / "layer_overall_summary.parquet")
    fixed = pl.read_parquet(input_dir / "layer_fixed_package_waterfall.parquet")
    trades = pl.read_parquet(input_dir / "same_second_trade_summary.parquet")
    meta = json.loads(
        (input_dir / "layer_summary.json").read_text(encoding="utf-8")
    )
    lines = [
        "# TX／TXO 六層理想套利上限",
        "",
        "結論：2026-08-10 的同步 capture 中，中間價會製造很大的假套利；改用可主動成交 Bid/Ask 後只剩微小正值，但第一檔量不足。加入第一檔量、手續費與稅後，最佳完整 package 為 -892 元。",
        "",
        "## 每層重新搜尋後的全域上限",
        "",
        "| 層級 | 新增因素 | 最高值 (TWD) | 相較前層 | 正值方法秒 | 最佳方法 | 時間 |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for row in overall.iter_rows(named=True):
        lines.append(
            f"| {row['layer_label_zh']} | {row['added_factor_zh']} | "
            f"{row['max_layer_value_twd']:,.0f} | "
            f"{row['change_from_previous_layer_twd']:,.0f} | "
            f"{row['positive_method_seconds']:,} | "
            f"{row['best_delivery_month']} {LABELS[row['best_variant_id']]} | "
            f"{row['best_snapshot_ts']} |"
        )
    lines.extend(
        [
            "",
            "## 固定同一組合的可加總成本分解",
            "",
            "固定 2026-08-10 11:55:40、202608 Put butterfly，履約價 44150/44200/44250。",
            "",
            "| 層級 | 固定組合損益 (TWD) | 本層影響 |",
            "|---|---:|---:|",
        ]
    )
    for row in fixed.iter_rows(named=True):
        lines.append(
            f"| {row['layer_label_zh']} | {row['fixed_package_value_twd']:,.0f} | "
            f"{row['change_from_previous_layer_twd']:,.0f} |"
        )
    control = meta["same_second_trade_print_control"]
    lines.extend(
        [
            "",
            "## 同秒逐筆成交旁證",
            "",
            f"共有 {control['evaluable_method_seconds']} 個完整方法秒；最高觀察毛利為 {control['max_gross_print_edge_twd']:,.0f} 元，扣同樣費稅後最高仍為 {control['max_after_all_modeled_costs_twd']:,.0f} 元。但兩筆正值 parity 的 Call 與 Put 成交量都只有 1 口，低於所需各 4 口；加入成交量後正值為 {control['positive_after_costs_and_print_volume_rows']} 筆。逐筆成交仍不代表你的多腿單能同時以那些歷史價格成交。",
            "",
            "| 方法 | 完整同秒觀察 | 毛利為正 | 最高毛利 | 含費稅後最高值 | 費稅及成交量後正值 |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in trades.iter_rows(named=True):
        lines.append(
            f"| {row['delivery_month']} {LABELS[row['variant_id']]} | "
            f"{row['evaluable_same_second_prints']:,} | "
            f"{row['positive_same_second_prints']:,} | "
            f"{row['max_gross_print_edge_twd']:,.0f} | "
            f"{row['max_after_estimated_settlement_tax_twd']:,.0f} | "
            f"{row['positive_after_costs_and_print_volume']:,} |"
        )
    lines.extend(
        [
            "",
            "## 邊界",
            "",
            "- 只涵蓋一天、模擬帳戶環境；不能據此估計 Sharpe、Sortino、MDD 或 Calmar。",
            "- 主漏斗零滑價、零下單延遲、零資金成本，且沒有被動成交假設。",
            "- L5 的到期稅以當秒 TX 中間價和最大同時履約腿數估計。",
            "- 9 月 TXO 沒有同到期 TX；MTX、TMF 與現貨籃子未在本 capture 中同步訂閱。",
            "",
        ]
    )
    path = input_dir / "layer_report.md"
    temporary = path.with_suffix(".md.tmp")
    temporary.write_text("\n".join(lines), encoding="utf-8")
    temporary.replace(path)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    output = args.output or args.input_dir / "artifact.json"
    _atomic_json(output, build_artifact(args.input_dir))
    markdown = _write_layer_markdown(args.input_dir)
    print(
        json.dumps(
            {
                "status": "complete",
                "output": str(output),
                "markdown": str(markdown),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
