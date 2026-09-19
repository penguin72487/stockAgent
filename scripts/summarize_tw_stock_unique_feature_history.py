#!/usr/bin/env python3
"""Build one-row-per-name Markdown inventory of Taiwan stock feature history.

Counts are finite source cells before projection, carry, shift, or zero fill.
Zero event cells do not prove that an event source had no historical events.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.summarize_tw_stock_training_features import (
    CONFIGS,
    FAMILY_LABELS,
    OVERNIGHT_1325_FEATURE,
    check_inventory_snapshot,
    family,
    matches,
    read_inventory,
)
from stockagent.config import load_config


STRICT_DIR = Path("artifacts/data_quality/tw_public_raw_2014_v1")
RESEARCH_DIR = Path("artifacts/data_quality/tw_public_research_2014_v1")
OUTPUT = Path("docs/tw_stock_unique_feature_history_2014.md")
BEGIN = "<!-- BEGIN GENERATED UNIQUE FEATURE HISTORY -->"
END = "<!-- END GENERATED UNIQUE FEATURE HISTORY -->"

# These are transformation inputs, not proof that the derived formal feature
# already has a valid 2014 vintage or matching feature-builder semantics.
RAW_CANDIDATES = {
    "twpub_cbc_m1b_log": "twpub_cbc_m1b_raw",
    "twpub_cbc_m2_log": "twpub_cbc_m2_raw",
    "twpub_cbc_overnight_rate": "twpub_cbc_overnight_pct_raw",
    "twpub_cbc_overnight_rate_chg": "twpub_cbc_overnight_pct_raw",
    "twpub_dgbas_cpi_log": "twpub_dgbas_cpi_raw",
    "twpub_dgbas_gdp_log": "twpub_dgbas_gdp_raw",
    "twpub_usdtwd_log": "twpub_usdtwd_raw",
    "twpub_usdtwd_logret_1d": "twpub_usdtwd_raw",
    "twpub_mof_business_tax_log": "twpub_mof_business_tax_raw",
    "twpub_mof_export_log": "twpub_mof_export_raw",
    "twpub_mof_futures_tax_log": "twpub_mof_futures_tax_raw",
    "twpub_mof_import_log": "twpub_mof_import_raw",
    "twpub_mof_securities_tax_log": "twpub_mof_securities_tax_raw",
    "twpub_mof_tax_total_log": "twpub_mof_tax_total_raw",
    "twpub_mof_trade_balance_asinh": "twpub_mof_trade_balance_raw",
}


def annual_counts(path: Path) -> dict[str, dict[int, int]]:
    result: dict[str, dict[int, int]] = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            year = int(row["year"])
            if 2014 <= year <= 2025:
                result[row["feature"]][year] = int(row["raw_non_null"])
    return result


def source_route(name: str) -> str:
    if name.startswith("twpub_taifex_"):
        return "T2" if any(part in name for part in ("foreign", "dealer", "trust", "large_oi", "top5", "top10")) else "T1"
    if name.startswith("twpub_tdcc_"):
        return "D1"
    if name.startswith(("twpub_attention_", "twpub_disposal_")):
        return "E1"
    if name.startswith(("twpub_dividend_", "twpub_exdiv_")):
        return "E2"
    if name.startswith(("twpub_material_", "twpub_monthly_", "twpub_cumulative_")):
        return "M1"
    if name.startswith(("twpub_xbrl_", "twpub_financial_")):
        return "M2"
    if name.startswith(("twpub_company_", "twpub_insider_")):
        return "M3"
    if name.startswith(("twpub_sbl_", "twpub_borrow_", "twpub_short_sale_")):
        return "S1"
    if name.startswith(("twpub_usdtwd_", "twpub_cbc_")):
        return "C1"
    if name.startswith("twpub_dgbas_"):
        return "G1"
    if name.startswith("twpub_mof_"):
        return "F1"
    return "P1" if name.startswith("twpub_") else "P2"


def escaped(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def year_ranges(years: list[int]) -> str:
    if not years:
        return "無整年零值"
    ranges: list[str] = []
    start = end = years[0]
    for year in years[1:]:
        if year == end + 1:
            end = year
            continue
        ranges.append(str(start) if start == end else f"{start}–{end}")
        start = end = year
    ranges.append(str(start) if start == end else f"{start}–{end}")
    return "、".join(ranges)


def build() -> str:
    for directory in (STRICT_DIR, RESEARCH_DIR):
        check_inventory_snapshot(directory)
    inventories = {
        "strict": read_inventory(STRICT_DIR / "feature_inventory.csv"),
        "research": read_inventory(RESEARCH_DIR / "feature_inventory.csv"),
    }
    annual = {
        "strict": annual_counts(STRICT_DIR / "annual_feature_inventory.csv"),
        "research": annual_counts(RESEARCH_DIR / "annual_feature_inventory.csv"),
    }
    selected: dict[str, list[str]] = defaultdict(list)
    for mode, config_path in CONFIGS.items():
        data = load_config(ROOT / config_path).data
        names = list(data.feature_include)
        names += [f"{name}__available" for name in data.feature_include if matches(name, data.feature_availability_indicators)]
        if mode == "overnight_1325_research":
            names.append(OVERNIGHT_1325_FEATURE)
        for name in names:
            if mode not in selected[name]:
                selected[name].append(mode)
    all_names = set(selected) | set(inventories["strict"]) | set(inventories["research"])
    groups: dict[str, list[dict[str, object]]] = defaultdict(list)
    for name in sorted(all_names):
        indicator = name.endswith("__available")
        base = name.removesuffix("__available")
        strict_2014 = annual["strict"].get(base, {}).get(2014, 0)
        research_2014 = annual["research"].get(base, {}).get(2014, 0)
        source = inventories["strict"].get(base) or inventories["research"].get(base)
        candidate = RAW_CANDIDATES.get(name)
        candidate_2014 = annual["research"].get(candidate or "", {}).get(2014, 0)
        if indicator:
            group = "旗標"
            state = "基礎值有 2014" if strict_2014 or research_2014 else "基礎值缺 2014"
            first = last = "衍生"
        elif name == OVERNIGHT_1325_FEATURE:
            group, state = "待回補", "分鐘價自 2020-03-02；2014 缺"
            first, last = "2020-03-02", "2026-09-18"
        elif not name.startswith("twpub_"):
            group, state = "股票面板", "2014 未逐欄核數"
            first = last = "面板計算"
        else:
            if strict_2014:
                group, state = "正式表已有 2014", "正式表數值有 2014"
            elif research_2014:
                group, state = "僅研究表已有 2014", "研究現值有 2014；正式版未納"
            elif candidate_2014:
                group, state = "原值可重算", f"候選原值有 2014（{candidate}）；待驗公式"
            else:
                group, state = "待回補", "兩表均無 2014 觀測"
            # Prefer the table containing the 2014 observation when both have
            # this name but different first-observation policies.
            preferred = inventories["research"] if research_2014 and not strict_2014 else inventories["strict"]
            item = preferred.get(name) or source or {}
            first = item.get("first") or "—"
            last = item.get("last") or "—"
        missing_years = [year for year in range(2014, 2026)
                         if not annual["strict"].get(base, {}).get(year, 0)
                         and not annual["research"].get(base, {}).get(year, 0)]
        if group in ("股票面板", "旗標"):
            missing_label = "由基礎值決定" if indicator else "未逐欄核"
        elif name == OVERNIGHT_1325_FEATURE:
            missing_label = "2014–2019；2020 部分"
        else:
            missing_label = year_ranges(missing_years)
        group_name, _ = family(name)
        family_label = FAMILY_LABELS.get(group_name, group_name)
        if group_name == "other_public":
            family_label = {
                "D1": "集保股權分散", "M1": "MOPS 月營收／重大訊息",
                "M2": "財報", "M3": "公司／內部人",
                "S1": "借券／賣空",
            }.get(source_route(base), family_label)
        groups[group].append({
            "name": name, "family": family_label, "modes": selected.get(name, []),
            "first": first, "last": last, "strict_2014": strict_2014,
            "research_2014": research_2014, "missing_years": missing_label,
            "state": state, "route": source_route(base) if name != OVERNIGHT_1325_FEATURE else "I1",
        })
    order = ["正式表已有 2014", "僅研究表已有 2014", "原值可重算", "待回補", "股票面板", "旗標"]
    lines = [
        f"資料快照合併：**{len(all_names)} 個不重複名稱**；七個主線配置選入 {len(selected)} 個不重複通道，另有 {len(all_names)-len(selected)} 個僅來源表欄位。",
        "",
        "| 分類 | 名稱數 | 判讀 |",
        "|---|---:|---|",
    ]
    labels = {
        "正式表已有 2014": "正式來源表 2014 有至少一格有限值",
        "僅研究表已有 2014": "正式表 2014 零格，研究表有歷史現值",
        "原值可重算": "同名衍生欄無 2014，但研究表另一原值欄可作候選",
        "待回補": "兩張來源表及候選原值均未證明 2014 有值",
        "股票面板": "由股票面板原值或公式產生，未逐欄核 2014",
        "旗標": "可用性指標是衍生通道，歷史性取決於基礎值",
    }
    for key in order:
        lines.append(f"| {key} | {len(groups[key])} | {labels[key]} |")
    route_counts = defaultdict(int)
    for row in groups["待回補"]:
        route_counts[str(row["route"])] += 1
    lines.extend([
        "",
        "真正待回補的 78 個名稱依來源路線分布：" + "、".join(
            f"{route} {route_counts[route]}" for route in sorted(route_counts)
        ) + "。一份原始資料可供多個衍生特徵，因此名稱數不是下載檔案數。",
    ])
    lines.extend([
        "",
        "**口徑：**2014 數字是當年來源表有限值格數，不是完整交易日、公司覆蓋、舊版原稿或決策時刻證明。稀疏事件的整年零值表示本機沒有觀測，不能直接解讀成全年沒有事件。`2015–2025 零值年`只抓整年零格；逐日、逐公司和月季缺口仍需來源稽核。兩表同名只列一次，正式與研究數字放在同一列。",
        "",
        "模式代碼：D=線上日當沖 fold 11／正式日當沖，O=13:25 隔夜研究，S=嚴格開盤前，R=2014 原值開盤前，W=2014 寬覆蓋研究，C=同日收盤研究；D 合併兩個 ABI 相同的模式，但該欄如只在其中一個配置出現仍會分別標記。`—` 代表來源表中沒有此欄。",
        "",
    ])
    mode_codes = {
        "live_day_trade_fold11": "D線", "formal_day_trade": "D訓", "overnight_1325_research": "O",
        "preopen_strict": "S", "preopen_raw_2014": "R", "preopen_wide_research_2014": "W", "same_close_research": "C",
    }
    seen: set[str] = set()
    for key in order:
        lines += [f"### {key}（{len(groups[key])}）", "",
                  "| 特徵 | 家族 | 配置 | 首日 | 最新 | 正式表 2014 格 | 研究表 2014 格 | 2015–2025 零值年 | 判讀 | 路線 |",
                  "|---|---|---|---|---|---:|---:|---|---|---|"]
        for row in sorted(groups[key], key=lambda r: (str(r["family"]), str(r["name"]))):
            name = str(row["name"])
            if name in seen:
                raise RuntimeError(f"Duplicate feature name: {name}")
            seen.add(name)
            modes = "、".join(mode_codes[mode] for mode in row["modes"]) or "僅來源"
            values = [f"`{name}`", row["family"], modes, row["first"], row["last"],
                      f"{row['strict_2014']:,}" if name in inventories["strict"] else "—",
                      f"{row['research_2014']:,}" if name in inventories["research"] else "—",
                      row["missing_years"], row["state"], row["route"]]
            lines.append("| " + " | ".join(escaped(value) for value in values) + " |")
        lines.append("")
    if seen != all_names:
        raise RuntimeError("Generated feature set is incomplete")
    assert sum(len(groups[key]) for key in order) == len(all_names)
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-path", type=Path, default=OUTPUT)
    args = parser.parse_args()
    original = args.report_path.read_text(encoding="utf-8")
    if original.count(BEGIN) != 1 or original.count(END) != 1:
        raise RuntimeError("Expected exactly one generated section")
    before, remainder = original.split(BEGIN, 1)
    _, after = remainder.split(END, 1)
    updated = before + BEGIN + "\n" + build() + END + after
    if updated != original:
        args.report_path.write_text(updated, encoding="utf-8")
    print(f"Updated {args.report_path}")


if __name__ == "__main__":
    main()
