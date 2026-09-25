#!/usr/bin/env python3
"""Write a Markdown-only inventory for the complete observed TW research ABI."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

import polars as pl
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data.tw_public_research_all_features import DERIVED_SOURCES, _sha256


DEFAULT_TABLE = Path("artifacts/research_features/tw_public_research_all_2014_v3.parquet")
DEFAULT_OLD_INVENTORY = Path("docs/tw_stock_unique_feature_history_2014.md")
DEFAULT_REPORT = Path("docs/tw_stock_all_observed_training_features_2014_v3.md")


def _old_rows(path: Path) -> dict[str, dict[str, str]]:
    rows = {}
    group = ""
    for line in path.read_text().splitlines():
        if line.startswith("### "):
            group = re.sub(r"（\d+）.*$", "", line[4:])
        if not line.startswith("| `"):
            continue
        cells = [part.strip().strip("`") for part in line.strip().strip("|").split("|")]
        if len(cells) < 10:
            continue
        rows[cells[0]] = {
            "group": group, "first": cells[3], "last": cells[4], "route": cells[9],
        }
    return rows


def _total_non_nulls(file: pq.ParquetFile, name: str) -> int:
    index = file.schema.names.index(name)
    count = 0
    for group in range(file.metadata.num_row_groups):
        column = file.metadata.row_group(group).column(index)
        if column.statistics is None or column.statistics.null_count is None:
            raise ValueError(f"Parquet lacks null statistics for {name}")
        count += column.num_values - column.statistics.null_count
    return count


def build_report(table_path: Path, old_path: Path) -> str:
    receipt_path = table_path.with_suffix(".all_features.json")
    receipt = json.loads(receipt_path.read_text())
    output_sha256 = _sha256(table_path)
    if receipt.get("output_sha256") != output_sha256:
        raise ValueError("research table does not match its source receipt")
    source = pq.ParquetFile(table_path)
    value_names = sorted(name for name in source.schema.names if name.startswith("twpub_"))
    if len(value_names) != 204:
        raise ValueError(f"expected 204 observed public columns, got {len(value_names)}")
    old = _old_rows(old_path)
    old_names = set(old)
    if len(old_names) != 266:
        raise ValueError(f"expected 266 prior unique names, got {len(old_names)}")
    old_values = {name for name in old_names if name.startswith("twpub_") and not name.endswith("__available")}
    if len(old_values) != 194 or not old_values <= set(value_names):
        raise ValueError("prior public features are not all represented in the new table")
    new_values = set(value_names) - old_values
    if len(new_values) != 10:
        raise ValueError("expected exactly ten new TAIFEX values")

    totals = {name: _total_non_nulls(source, name) for name in value_names}
    if not all(totals.values()):
        raise ValueError("an all-null source column would create a fake training input")
    frame = pl.scan_parquet(table_path)
    bounds = frame.select(
        pl.col("date").min().alias("first"),
        pl.col("date").max().alias("last"),
        pl.col("symbol").n_unique().alias("symbols"),
    ).collect().row(0, named=True)
    counts_2014 = frame.filter(pl.col("date").dt.year() == 2014).select(
        pl.len().alias("rows"),
        *[pl.col(name).is_finite().sum().alias(name) for name in value_names],
    ).collect(engine="streaming").row(0, named=True)
    rows_2014 = counts_2014.pop("rows")
    later_only = [name for name in value_names if counts_2014[name] == 0]
    later_routes = Counter(old.get(name, {}).get("route", "新增") for name in later_only)
    research_2014 = [name for name, item in old.items() if item["group"] == "僅研究表已有 2014"]
    rebuilt_2014 = [name for name, item in old.items() if item["group"] == "原值可重算"]
    if len(research_2014) != 44 or len(rebuilt_2014) != 15:
        raise ValueError("the 44 research and 15 rebuilt feature inventory changed")
    if any(counts_2014[name] == 0 for name in research_2014 + rebuilt_2014):
        raise ValueError("a required 2014 research or reconstructed feature lost all observations")
    observed = pl.scan_parquet(table_path).filter(pl.col("symbol") == "__MARKET__").select(
        "date", *[name for name in value_names if name in DERIVED_SOURCES or name in new_values]
    ).collect()
    fresh_dates = {}
    for name in set(DERIVED_SOURCES) | new_values:
        dates = observed.filter(pl.col(name).is_finite()).get_column("date")
        if dates.is_empty():
            raise ValueError(f"new or rebuilt feature has no market observations: {name}")
        fresh_dates[name] = (str(dates.min()), str(dates.max()))

    panel_names = sorted(name for name in old_names if not name.startswith("twpub_"))
    all_names = sorted(set(value_names) | {name + "__available" for name in value_names} | set(panel_names))
    if len(all_names) != 430:
        raise ValueError(f"expected 430 deduplicated names including availability, got {len(all_names)}")
    categories = Counter()
    lines = [
        "# 台股完整已觀測研究訓練特徵（v3）",
        "",
        "資料表：`artifacts/research_features/tw_public_research_all_2014_v3.parquet`；研究配置：`configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml`。",
        "本表將既有 266 個去重名稱、10 個期交所新增欄，以及模型需要的 154 個新增可用性旗標合併；所有名稱只列一次。",
        "",
        f"- 來源列數：{source.metadata.num_rows:,}，2014 年列數：{rows_2014:,}；來源鍵為 `(date, symbol)`。",
        f"- 建表契約：v{receipt['contract_version']}；輸出 SHA-256：`{output_sha256}`；輸入 SHA-256 與路徑見同名 `.all_features.json` 收據。",
        f"- 來源日期：{bounds['first']}～{bounds['last']}；來源含 {bounds['symbols']:,} 個代號（含 `__MARKET__`），模型面板的實際股票數以預檢輸出為準。",
        f"- 公開資料值欄：{len(value_names)}；2014 年有實值：{sum(counts_2014[name] > 0 for name in value_names)}；2014 年尚無實值但後續有值：{sum(counts_2014[name] == 0 for name in value_names)}。",
        "- 晚起始 77 欄按原來源路線：" + "、".join(
            f"{route} {count}" for route, count in sorted(later_routes.items())
        ) + "；本 ABI 保留它們的後續實值與缺值旗標。",
        "- 2014 年的 127 個值欄分解為：原正式表 58、僅研究表 44、由原值重算 15、期交所新補 10。",
        "- 模型欄：204 個公開值 + 204 個可用性旗標 + 20 個股票面板欄 = 428。`next_session_open_gap_logret` 與 `next_session_1325_gap_logret` 分屬不同決策時鐘，不屬此開盤前研究 ABI。",
        "- 15 個舊衍生欄已從研究原值重算，44 個原本僅研究表有 2014 值的欄位亦保留。原始缺值維持 null，模型在產生可用性旗標後才轉零；較晚開始的欄位在先前日期的旗標為 0。",
        "- 本配置接受現修宏觀值、推測公告日與僅有捕獲日期的快照作研究輸入。這只證明資料可讀，不證明歷史 PIT、完整來源覆蓋或可執行報酬。快照值不得倒填到捕獲日之前。",
        "- 在 09:00 研究時鐘，沒有可靠盤中公告時間的當期快照再向後移一個交易日。僅從 2026 才開始觀測的欄位在先前訓練 fold 未見過，不應把其已列入配置誤說成模型已學到該訊號。",
        "- XBRL 舊 `tifrs` 概念在來源後僅延續 400 曆日。期交所 v2 官方值已對齊下個可用交易日；模型不可再把這些欄當同日 09:00 可見。",
        "- 下列「2014 原始格」與「全期非空」計數是來源表的觀測格，不是前向保留後的模型張量格，也不是逐公司完整率。來源日期多為估計可用日，詳見原始來源收據及 `docs/tw_public_wide_research_2014.md`。",
        "- 原清單欄位的首次／最新日期沿用 2026-09-18 來源盤點；15 個重算欄與 10 個期交所新欄則直接取 v3 的市場觀測日期。回補後若來源新增新日期，需重跑來源盤點與本報告。",
        "- v3 目前在本機 `artifacts/research_features` 工作區；本頁的建表與資料預檢不等於冷庫發布、遠端同步或 GPU 訓練完成。",
        "",
        "## 15 個原值重算欄",
        "",
        "GDP、CPI、M1B、M2、美元匯率用正值自然對數；各稅、進出口用正值 `log(1+x)`；貿易出超用 `asinh(x/1,000,000)`；美元匯率報酬用相鄰有效觀測值的對數比；隔夜利率直接使用已是小數利率的研究原值，變動量取相鄰有效觀測差。非有限值或對數定義域外維持缺值。",
        "",
        "| 衍生特徵 | 原值 | 2014 有效格 | 全期非空格 |",
        "|---|---|---:|---:|",
    ]
    for name, raw in DERIVED_SOURCES.items():
        lines.append(f"| `{name}` | `{raw}` | {counts_2014[name]:,} | {totals[name]:,} |")
    lines += ["", "## 所有去重名稱", "", "| 特徵 | 來源/角色 | 首次來源日期 | 最新來源日期 | 2014 原始格 | 全期非空格 | 此 ABI |", "|---|---|---|---|---:|---:|---|"]
    for name in all_names:
        base = name.removesuffix("__available")
        prior = (old.get(base) if name.endswith("__available") else old.get(name)) or {}
        if name in fresh_dates:
            first, last = fresh_dates[name]
        elif base in fresh_dates:
            first, last = fresh_dates[base]
        else:
            first, last = prior.get("first", "—"), prior.get("last", "—")
        if name.endswith("__available"):
            role = "可用性旗標"
            training = "使用；基礎值缺時 0"
            raw_count = counts_2014[base]
            total = totals[base]
        elif name in DERIVED_SOURCES:
            role = "原值重算"
            training = "使用"
            raw_count, total = counts_2014[name], totals[name]
        elif name in new_values:
            role = "期交所新增"
            training = "使用"
            raw_count, total = counts_2014[name], totals[name]
        elif name.startswith("twpub_"):
            role = prior.get("group", "公開資料")
            training = "使用"
            raw_count, total = counts_2014[name], totals[name]
        else:
            role = "模式專用" if name.startswith("next_session_") else "股票面板"
            training = "僅專用時鐘" if role == "模式專用" else "使用"
            raw_count = total = None
        categories[role] += 1
        count_text = "—" if raw_count is None else f"{raw_count:,}"
        total_text = "—" if total is None else f"{total:,}"
        lines.append(f"| `{name}` | {role} | {first} | {last} | {count_text} | {total_text} | {training} |")
    lines += [
        "", "## 重建與增量更新", "",
        "1. 先以既有下載器更新正式來源表、2014 寬研究表與期交所 v2 表，各自保留原始檔與收據。",
        "2. 執行 `source scripts/runtime_env.sh && run_fintech_python scripts/build_tw_public_research_all_features.py`。輸入雜湊與輸出雜湊相同時原樣重用；變更時原子重建。研究表值優先，同鍵研究空值由正式表補。",
        "3. 執行 `source scripts/runtime_env.sh && run_fintech_python scripts/report_tw_public_research_all_features.py` 更新本頁，再以 `train.py --config configs/markets/tw_public_preopen_all_observed_research_2014_v3.yaml --check-data-only` 核對模型資料。",
        "4. 開始完整訓練前，需確認顯示的 428 通道、折數、來源簽章、記憶體與輸出目錄；訓練結果只可稱為研究實驗。",
        "",
        "資料血緣：`tw_public_research_wide_2014_taifex_v2.parquet` + `tw_public_stock_daily.parquet` → 15 個原值公式 + 同鍵空值補齊 → v3；精確輸入／輸出 SHA-256 在同名 `.all_features.json` 收據。",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=Path, default=DEFAULT_TABLE)
    parser.add_argument("--old-inventory", type=Path, default=DEFAULT_OLD_INVENTORY)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    report = build_report(args.table, args.old_inventory)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    print(f"[tw-public-research-all] markdown={args.output} lines={report.count(chr(10)) + 1}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
