#!/usr/bin/env python3
"""Write every selected Taiwan-stock feature and source feature into Markdown.

This is an inventory, not a PIT or checkpoint acceptance test.  Source-table
counts describe finite observations before panel carry, shift, and zero fill.
"""

from __future__ import annotations

import argparse
import csv
from fnmatch import fnmatchcase
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config
from stockagent.data.tw_overnight import OVERNIGHT_1325_FEATURE, overnight_minute_manifest
from scripts.classify_tw_public_2014_features import classify, research_acceptance


CONFIGS = {
    "live_day_trade_fold11": "configs/deployments/tw_day_trade_multi_basis_22_fold11.yaml",
    "formal_day_trade": "configs/markets/tw_day_trade_daily_multi_basis_projection_l1_tplus2_close_capital10m.yaml",
    "overnight_1325_research": "configs/markets/tw_overnight_1325_multi_basis_22_capital10m.yaml",
    "preopen_strict": "configs/markets/tw_public_preopen_pit.yaml",
    "preopen_raw_2014": "configs/markets/tw_public_preopen_raw_2014_v1.yaml",
    "preopen_wide_research_2014": "configs/markets/tw_public_preopen_wide_research_2014_v1.yaml",
    "same_close_research": "configs/markets/tw_public.yaml",
}

MODE_LABELS = {
    "live_day_trade_fold11": "線上日當沖 fold 11",
    "formal_day_trade": "正式日當沖訓練",
    "overnight_1325_research": "13:25 隔夜研究",
    "preopen_strict": "嚴格 09:00 開盤前",
    "preopen_raw_2014": "2014 原值開盤前",
    "preopen_wide_research_2014": "2014 寬覆蓋研究",
    "same_close_research": "同日收盤研究",
}

FAMILY_LABELS = {
    "stock_ohlcv_or_panel": "股票行情／面板",
    "official_stock_daily": "官方日行情",
    "valuation": "估值",
    "dividend": "股利",
    "corporate_action": "除權息／公司行動",
    "margin_short": "資券",
    "institutional": "法人",
    "attention": "注意股票",
    "disposal": "處置股票",
    "material_event": "重大訊息",
    "taiex": "加權指數",
    "fx": "匯率",
    "cbc_rate": "央行利率",
    "cbc_reserves": "央行外匯存底",
    "cbc_money": "央行貨幣",
    "dgbas_cpi": "主計總處物價",
    "dgbas_unemployment": "主計總處失業",
    "dgbas_gdp": "主計總處 GDP",
    "mof_tax_trade": "財政部貿易／稅收",
    "mops_xbrl": "MOPS XBRL 財報",
    "taifex": "期交所",
    "availability": "可用性旗標",
    "overnight_1325_minute": "13:25 分鐘價差",
    "other_public": "其他公開來源",
}

CADENCE_LABELS = {
    "each trading session": "每交易日",
    "each trading session after close": "每交易日盤後",
    "each trading session when source is present": "有分鐘來源的交易日",
    "event or daily valuation": "事件／每日估值",
    "event or trading session": "事件／交易日",
    "event": "不定期事件",
    "monthly": "每月",
    "quarterly": "每季",
    "quarterly filing": "逐公司季報／年報",
    "source dependent": "依來源",
}

STATUS_LABELS = {
    "source_observed": "有來源值；仍須驗 PIT",
    "observed_from_2026_only": "2026 才有來源值",
    "all_null": "全空",
    "configured_zero_fill": "配置遮蔽為零",
    "computed_from_stock_panel": "股票面板推導；未逐欄核數",
    "computed_from_1325_minute": "13:25 分鐘衍生；有效格未核數",
    "derived_availability": "可用性旗標；非原始來源值",
    "missing_source_column": "來源欄缺失",
}

DECISION_LABELS = {
    "selected_original_pit": "2014 原值配置已選",
    "snapshot_no_old_vintage": "快照缺舊版本",
    "derived_or_duplicate_not_selected": "轉換／重複；原值配置未選",
    "late_taifex_capture": "期交所本機晚近捕捉",
    "bulk_no_original_values": "整包值缺當年原稿",
    "all_null": "全空",
    "redundant_raw": "與面板原值重複",
    "partial_market_raw": "只覆蓋部分市場",
}

ACCEPTANCE_LABELS = {
    "yes": "接受原稿／官方日值",
    "yes_from_first_observation": "僅首次觀測後接受",
    "yes_after_date_mapping": "日期映射後接受現修值",
    "yes_for_covered_market": "僅已覆蓋市場接受",
    "no_raw_only_request": "原值配置排除衍生值",
    "no_duplicate_input": "排除重複輸入",
    "no": "無值，不能啟用",
}

DECISION_REASON_LABELS = {
    "selected_original_pit": "原值；嚴格用途另需來源公告時鐘和血緣收據。",
    "snapshot_no_old_vintage": "早年版本未封存；2026 快照不得倒填。",
    "derived_or_duplicate_not_selected": "可作衍生候選，但不符合原值配置的選欄規則。",
    "late_taifex_capture": "本機期交所歷史起點晚於 2014 訓練窗。",
    "bulk_no_original_values": "目前整包值可能修訂；原始逐期數值仍待核對。",
    "all_null": "目前來源表無有限值。",
    "redundant_raw": "股票面板已含成交量原值。",
    "partial_market_raw": "原始每日值只涵蓋部分上市櫃市場。",
}

BEGIN_MARKER = "<!-- BEGIN GENERATED FEATURE INVENTORY -->"
END_MARKER = "<!-- END GENERATED FEATURE INVENTORY -->"


def family(name: str) -> tuple[str, str]:
    if name == OVERNIGHT_1325_FEATURE:
        return "overnight_1325_minute", "each trading session when source is present"
    if name.endswith("__available"):
        return "availability", "each trading session"
    if not name.startswith("twpub_"):
        return "stock_ohlcv_or_panel", "each trading session"
    for prefix, label, cadence in (
        ("twpub_official_", "official_stock_daily", "each trading session"),
        ("twpub_pe", "valuation", "each trading session"),
        ("twpub_pb", "valuation", "each trading session"),
        ("twpub_dividend_", "dividend", "event or daily valuation"),
        ("twpub_exdiv_", "corporate_action", "event"),
        ("twpub_margin_", "margin_short", "each trading session after close"),
        ("twpub_short_", "margin_short", "each trading session after close"),
        ("twpub_foreign_", "institutional", "each trading session after close"),
        ("twpub_investment_trust_", "institutional", "each trading session after close"),
        ("twpub_dealer_", "institutional", "each trading session after close"),
        ("twpub_institutional_", "institutional", "each trading session after close"),
        ("twpub_attention_", "attention", "event or trading session"),
        ("twpub_disposal_", "disposal", "event or trading session"),
        ("twpub_material_", "material_event", "event"),
        ("twpub_twse_taiex_", "taiex", "each trading session"),
        ("twpub_usdtwd_", "fx", "each trading session"),
        ("twpub_cbc_overnight_", "cbc_rate", "each trading session"),
        ("twpub_cbc_fx_reserves_", "cbc_reserves", "monthly"),
        ("twpub_cbc_m1b_", "cbc_money", "monthly"),
        ("twpub_cbc_m2_", "cbc_money", "monthly"),
        ("twpub_dgbas_cpi_", "dgbas_cpi", "monthly"),
        ("twpub_dgbas_unemployment_", "dgbas_unemployment", "monthly"),
        ("twpub_dgbas_gdp_", "dgbas_gdp", "quarterly"),
        ("twpub_mof_", "mof_tax_trade", "monthly"),
        ("twpub_xbrl_", "mops_xbrl", "quarterly filing"),
        ("twpub_taifex_", "taifex", "each trading session"),
    ):
        if name.startswith(prefix):
            return label, cadence
    return "other_public", "source dependent"


def transform(name: str) -> str:
    base_transforms = {
        "open_logret_1d": "ln(今開／前開)",
        "max_logret_1d": "ln(今高／前高)",
        "min_logret_1d": "ln(今低／前低)",
        "close_logret_1d": "ln(今收／前收)",
        "trading_volume_logret_1d": "ln(今量／前量)",
        "signed_vol": "當日收開報酬符號 × 成交量對數變動",
        "body_ratio": "實體長度／全日高低幅，限制在 0–1",
        "signed_body_ratio": "收開差／全日高低幅，限制在 -1–1",
        "delta_body_ratio": "實體比率與前交易日之差",
        "clv": "收盤於全日高低幅中的位置，限制在 0–1",
        "clv_centered": "CLV − 0.5",
        "delta_clv": "CLV 與前交易日之差",
        "upper_shadow": "上影線／全日高低幅，限制在 0–1",
        "lower_shadow": "下影線／全日高低幅，限制在 0–1",
        "shadow_imbalance": "上影線比率 − 下影線比率",
        "next_session_open_gap_logret": "ln(次日開盤／今日收盤)，供次日開盤決策",
    }
    if name in base_transforms:
        return base_transforms[name]
    if name == OVERNIGHT_1325_FEATURE:
        return "ln(當日 13:25 價／前日收盤)，缺價可用同日最終收盤代理"
    if name.endswith("__available"):
        return "缺值填零前計算來源可用性 0／1"
    if name.endswith("_raw"):
        return "原單位；季流量可能由年累計差分"
    if "_asinh" in name:
        return "asinh 雙曲反正弦縮放"
    if "_logret" in name or name.endswith("_chg"):
        return "前期對數報酬／變動；以建表函式為準"
    if name.endswith("_log"):
        return "對數或 log1p 縮放；以建表函式為準"
    if "ratio" in name or "margin" in name or "yield" in name:
        return "比率／百分比；分母無效時留缺值"
    if "flag" in name or "covered" in name or "known" in name:
        return "事件或來源覆蓋 0／1 旗標"
    return "來源值計算衍生；公式依特徵建表函式"


def read_inventory(path: Path) -> dict[str, dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["feature"]: row for row in csv.DictReader(handle)}


def check_inventory_snapshot(directory: Path) -> None:
    summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
    path = Path(summary["feature_path"])
    stat = path.stat()
    expected = summary["file_signature"]
    if (stat.st_ino, stat.st_size, stat.st_mtime_ns) != (
        expected["inode"], expected["size"], expected["mtime_ns"]
    ):
        raise RuntimeError(f"Inventory is stale for {path}; rerun inventory_tw_public_feature_table.py")


def read_annual(path: Path) -> dict[str, int]:
    totals: dict[str, int] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if 2014 <= int(row["year"]) <= 2026:
                name = row["feature"]
                totals[name] = totals.get(name, 0) + int(row["raw_non_null"])
    return totals


def matches(name: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatchcase(name, pattern) for pattern in patterns)


def _cell(value: object) -> str:
    if value is None or value == "":
        return "—"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value).replace("|", "\\|").replace("\n", " ")


def _count(value: object) -> str:
    return _cell(int(value)) if value != "" and value is not None else "—"


def _render_markdown(
    rows: list[dict[str, object]],
    configs: list[dict[str, object]],
    strict: dict[str, dict[str, str]],
    wide: dict[str, dict[str, str]],
    strict_selected: set[str],
) -> str:
    lines = [
        "## 完整逐欄清單",
        "",
        "以下逐一列出 7 個主線配置的 525 個輸入通道；相同欄位在不同配置重複列出，以顯示各配置的位移、遮蔽與來源表差異。其後另列正式來源表 143 欄及研究來源表 122 欄，包含未選入上述模型的欄位。",
        "",
        "**計數口徑：**「有限值格」及「2014–2026 格」都是建面板前、來源表非空且有限的儲存格數。市場月／季欄通常每公告期只有 1 個 `__MARKET__` 值；面板廣播或沿用後的股票格數沒有冒充新公告。`—` 表示未逐欄核算。可用性旗標的日期是其所依附值欄的來源起訖，並非旗標自身的歷史筆數。來源更新頻率是資料發布節奏；本機輪詢時間見上文。",
        "",
    ]
    for config in configs:
        mode = str(config["mode"])
        selected = [row for row in rows if row["mode"] == mode]
        lines.extend([
            f"### {MODE_LABELS[mode]}（{len(selected)} 通道）",
            "",
            f"配置：`{config['config']}`。面板從 `{selected[0]['panel_start']}` 開始；來源首末日可能早於面板，也不等於實際決策可用日。",
            "",
            "| # | 特徵 | 家族 | 來源首日 | 來源最新 | 有限值格 | 2014–2026 格 | 來源代號 | 更新節奏 | 前處理 | 品質與配置 |",
            "|---:|---|---|---|---|---:|---:|---:|---|---|---|",
        ])
        for number, row in enumerate(selected, 1):
            flags = [STATUS_LABELS[str(row["coverage_status"])]]
            if row["configured_next_session_shift"]:
                flags.append("面板次交易日位移")
            if row["configured_zero_fill"] and row["coverage_status"] != "configured_zero_fill":
                flags.append("配置遮蔽為零")
            lines.append("| " + " | ".join([
                str(number), f"`{row['feature']}`", FAMILY_LABELS.get(str(row["family"]), _cell(row["family"])),
                _cell(row["source_first"]), _cell(row["source_last"]),
                _count(row["source_finite_cells"]), _count(row["source_finite_cells_2014_2026"]),
                _count(row["source_symbols"]),
                CADENCE_LABELS.get(str(row["source_cadence"]), _cell(row["source_cadence"])),
                _cell(row["preprocessing"]), "；".join(flags),
            ]) + " |")
        lines.append("")

    for label, inventory in (("正式公開特徵來源表：全部 143 欄", strict), ("寬覆蓋研究來源表：全部 122 欄", wide)):
        is_strict = inventory is strict
        lines.extend([
            f"### {label}",
            "",
            "這是來源表欄位盤點，含上述配置未選用的欄位；來源有值不等於可用於嚴格 PIT 訓練。每列的筆數是該欄有限值格數。正式表的 2014 原值決策按本次最新有限值重算，接受欄反映使用者允許目前修訂值／推定公告日的研究政策。" if is_strict else "這是來源表欄位盤點，含上述配置未選用的欄位；來源有值不等於可用於嚴格 PIT 訓練。每列的筆數是該欄有限值格數。",
            "",
            ("| # | 來源特徵 | 家族 | 首日 | 最新 | 有限值格 | 代號數 | 更新節奏 | 前處理 | 品質 | 2014 原值決策 | 研究接受 | 決策原因 |"
             if is_strict else
             "| # | 來源特徵 | 家族 | 首日 | 最新 | 有限值格 | 代號數 | 更新節奏 | 前處理 | 品質 |"),
            ("|---:|---|---|---|---|---:|---:|---|---|---|---|---|---|"
             if is_strict else
             "|---:|---|---|---|---|---:|---:|---|---|---|"),
        ])
        for number, (name, item) in enumerate(inventory.items(), 1):
            group, cadence = family(name)
            first = item.get("first", "")
            status = "全空" if not int(item.get("count") or 0) else "2026 才有值" if first >= "2026-01-01" else "有值；PIT 另驗"
            cells = [
                str(number), f"`{name}`", FAMILY_LABELS.get(group, group), _cell(first), _cell(item.get("last", "")),
                _count(item.get("count", "")), _count(item.get("symbols", "")),
                CADENCE_LABELS.get(cadence, cadence), transform(name), status,
            ]
            if is_strict:
                decision, _reason = classify(name, int(item.get("count") or 0), strict_selected)
                _value_basis, _publication_basis, acceptance = research_acceptance(
                    name, int(item.get("count") or 0), decision
                )
                cells.extend([
                    DECISION_LABELS[decision], ACCEPTANCE_LABELS[acceptance],
                    DECISION_REASON_LABELS[decision],
                ])
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _replace_report_section(path: Path, markdown: str) -> None:
    original = path.read_text(encoding="utf-8")
    if original.count(BEGIN_MARKER) != 1 or original.count(END_MARKER) != 1:
        raise RuntimeError(f"Expected one generated section in {path}")
    before, remainder = original.split(BEGIN_MARKER, 1)
    _old, after = remainder.split(END_MARKER, 1)
    updated = before + BEGIN_MARKER + "\n" + markdown + END_MARKER + after
    if updated != original:
        path.write_text(updated, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="artifacts/data_quality/tw_stock_training_features_current")
    parser.add_argument("--report-path", default="docs/tw_stock_training_feature_status_2026-09-18.md")
    args = parser.parse_args()
    output = Path(args.output_dir)
    strict_dir = Path("artifacts/data_quality/tw_public_raw_2014_v1")
    wide_dir = Path("artifacts/data_quality/tw_public_research_2014_v1")
    check_inventory_snapshot(strict_dir)
    check_inventory_snapshot(wide_dir)
    strict = read_inventory(strict_dir / "feature_inventory.csv")
    wide = read_inventory(wide_dir / "feature_inventory.csv")
    strict_annual = read_annual(strict_dir / "annual_feature_inventory.csv")
    wide_annual = read_annual(wide_dir / "annual_feature_inventory.csv")

    rows: list[dict[str, object]] = []
    configs: list[dict[str, object]] = []
    for mode, path in CONFIGS.items():
        config = load_config(ROOT / path)
        data = config.data
        is_wide = "research_wide" in str(data.tw_public_feature_path)
        inventory = wide if is_wide else strict
        annual = wide_annual if is_wide else strict_annual
        values = list(data.feature_include)
        extra = [OVERNIGHT_1325_FEATURE] if mode == "overnight_1325_research" else []
        minute_dates: list[str] = []
        if extra and data.overnight_1325_root:
            minute_root = Path(data.overnight_1325_root)
            if (minute_root / "manifest.json").is_file():
                minute_dates = overnight_minute_manifest(minute_root)["dates"]
        indicators = [
            f"{name}__available" for name in values
            if matches(name, data.feature_availability_indicators)
        ]
        for name in [*values, *extra, *indicators]:
            indicator = name.endswith("__available")
            base = name.removesuffix("__available")
            item = inventory.get(base, {})
            group, cadence = family(name)
            count = int(item["count"]) if item.get("count") else 0
            if indicator:
                status = "derived_availability"
            elif name == OVERNIGHT_1325_FEATURE:
                status = "computed_from_1325_minute"
            elif base.startswith("twpub_") and not item:
                status = "missing_source_column"
            elif item and count == 0:
                status = "all_null"
            elif matches(base, data.feature_zero_fill):
                status = "configured_zero_fill"
            elif item and item.get("first", "") >= "2026-01-01":
                status = "observed_from_2026_only"
            elif item:
                status = "source_observed"
            else:
                status = "computed_from_stock_panel"
            rows.append({
                "mode": mode,
                "config": path,
                "feature": name,
                "family": group,
                "source_cadence": cadence,
                "source_table": (
                    str(data.overnight_1325_root) if name == OVERNIGHT_1325_FEATURE
                    else str(data.tw_public_feature_path) if base.startswith("twpub_")
                    else str(data.parquet_root)
                ),
                "source_finite_cells": item.get("count", "") if not indicator else "",
                "source_finite_cells_2014_2026": annual.get(base, "") if not indicator else "",
                "source_symbols": item.get("symbols", "") if not indicator else "",
                "source_first": minute_dates[0] if extra and name == OVERNIGHT_1325_FEATURE and minute_dates else item.get("first", ""),
                "source_last": minute_dates[-1] if extra and name == OVERNIGHT_1325_FEATURE and minute_dates else item.get("last", ""),
                "source_nonfinite_cells": item.get("nonfinite", "") if not indicator else "",
                "panel_start": str(data.panel_start_date),
                "configured_next_session_shift": matches(base, data.feature_shift_next_session),
                "configured_zero_fill": matches(base, data.feature_zero_fill),
                "preprocessing": transform(name),
                "coverage_status": status,
            })
        configs.append({
            "mode": mode, "config": path, "configured_values": len(values),
            "extra_model_channels": len(extra),
            "derived_availability_indicators": len(indicators),
            "total_model_channels": len(values) + len(extra) + len(indicators),
            "all_null_source_features": [r["feature"] for r in rows if r["mode"] == mode and r["coverage_status"] == "all_null"],
            "observed_from_2026_only": [r["feature"] for r in rows if r["mode"] == mode and r["coverage_status"] == "observed_from_2026_only"],
        })
    output.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report_path)
    strict_selected = set(load_config(ROOT / CONFIGS["preopen_raw_2014"]).data.feature_include)
    markdown = _render_markdown(rows, configs, strict, wide, strict_selected)
    _replace_report_section(report_path, markdown)
    (output / "summary.json").write_text(
        json.dumps({"configs": configs, "rows": len(rows), "report_path": str(report_path), "strict_source_columns": len(strict), "research_source_columns": len(wide), "measurement": "finite source cells, before panel carry/shift/zero fill; not PIT acceptance"}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"configs": configs, "rows": len(rows)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
