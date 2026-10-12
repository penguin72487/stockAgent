"""Render source ranges, merge decisions and reviewed aliases as Markdown.

This reads receipts, not moving provider APIs or training tensors. Detailed
minute lineage/conflicts remain separate sidecars, never model channels.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_text, atomic_write_json, sha256_file
from scripts.merge_tw_minute_sources import _digest


def _cell(value):
    return str(value).replace("|", "\\|").replace("\n", " ")


def _table(headers, rows):
    return ["| " + " | ".join(map(_cell, headers)) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
            *["| " + " | ".join(map(_cell, row)) + " |" for row in rows], ""]


def _interval_pages(report_path: Path, report: dict, output: Path):
    """Every exact interval for explicitly selected symbols, not a sample."""
    symbols = report["selected_symbols"]
    if not symbols:
        return []
    import polars as pl
    paths = [report_path.parent / "sessions" / p["day"] / "lineage_segments.parquet"
             for p in report["partitions"]]
    if not paths:
        return []
    table = pl.scan_parquet(paths).filter(pl.col("symbol").is_in(symbols)).collect(engine="streaming")
    pages = []
    for symbol in sorted(set(symbols)):
        if not symbol.isalnum():
            raise ValueError("unsafe source lineage symbol")
        selected = table.filter(pl.col("symbol") == symbol).sort(["first_ts", "phase", "_provider"])
        destination = output.parent / "lineage" / report_path.parent.name / f"{symbol}.md"
        lines = [f"# {symbol}：完整來源分鐘區段", "",
            f"固定來源：`{report['source_inventory_sha256']}`。", "",
            "以下是所有已選取區段，不是 top-K。只有實際連續分鐘才形成一段；",
            "同時點不同 phase 分開，首末時間不能取代列數、缺口或成交容量驗收。", ""]
        rows = []
        for row in selected.iter_rows(named=True):
            day = row["first_ts"].strftime("%Y-%m-%d")
            source = report_path.parent / "sessions" / day / "source_refs.json"
            reference = f"[{row['_source_ref'][:12]}]({os.path.relpath(source.resolve(), destination.parent.resolve())})"
            rows.append([day, row["first_ts"].strftime("%H:%M"), row["last_ts"].strftime("%H:%M"),
                row["phase"], row["_provider"], row["rows"], row["selection_reason"], reference])
        lines += _table(["日期", "開始", "結束", "機制", "Provider", "列數", "原因", "來源收據"], rows)
        atomic_write_text(destination, "\n".join(lines))
        pages.append(destination)
    return pages


def render(inventory_path: Path, report_paths: list[Path], output: Path):
    pinned = json.loads(inventory_path.read_text())
    if _digest(pinned["records"]) != pinned["source_inventory_sha256"]:
        raise ValueError("source inventory identity mismatch")
    def link(path, title):
        return f"[{title}]({os.path.relpath(Path(path).resolve(), output.parent.resolve())})"
    lines = ["# 同義原始觀測：來源區段、補缺與合併驗收", "",
        f"清冊時間：`{pinned['observed_at_utc']}`。本頁由收據產生，不代表 provider 現在已更新。", "",
        f"固定來源清冊：{link(inventory_path, 'source_inventory.json')}；",
        f"SHA：`{pinned['source_inventory_sha256']}`。", "",
        "原始觀測不刪除；試組合只留下方法、來源與差異證據。模型數值表不含 provider、SHA 或選取原因。", "",
        "## 目前固定來源", ""]
    if pinned.get("unit_evidence_inputs"):
        if _digest(pinned["unit_evidence_inputs"]) != pinned["unit_evidence_inventory_sha256"]:
            raise ValueError("unit evidence inventory identity mismatch")
        lines += [f"同日官方市場單位證據清冊 SHA：`{pinned['unit_evidence_inventory_sha256']}`；",
            f"固定 {len(pinned['unit_evidence_inputs'])} 份交易所原始報表；僅驗證實際試驗涉及的日期／商品。", ""]
    groups = defaultdict(list)
    for row in pinned["records"]:
        groups[row["provider"], row["scope"]].append(row)
    lines += _table(["Provider", "來源角色", "首日", "末日", "分區", "來源列"], [
        [provider, scope, min(r["day"] for r in rows), max(r["day"] for r in rows), len(rows),
         sum(r["rows"] for r in rows)] for (provider, scope), rows in sorted(groups.items())])
    lines += ["以上首末日期只界定觀測範圍，不保證其中每個商品／分鐘都有值；同義重疊尚未扣除。", "",
        "FinMind 2005 年起的是 TAIEX 指數，個股從 2019 年起；兩者不可互補成同一可交易股票。", "",
        "## 收據狀態（不混入退休查詢／空回應）", ""]
    lines += _table(["角色", "狀態", "工作數", "回應列", "查詢首日", "查詢末日"], [
        [r["scope"], r["state"], r["tasks"], r["rows"], r["first_query"], r["last_query"]]
        for r in pinned["finmind_states"]])
    lines += ["`deprecated_query_shape` 不代表目前缺下載，也不把舊逐股與新版全市場物件一起計入合併。",
        "`observed_empty` 只是 provider 回應空，不冒充完整交易日。", ""]
    for path in report_paths:
        report = json.loads(path.read_text())
        if report["source_inventory_sha256"] != pinned["source_inventory_sha256"]:
            raise ValueError(f"trial uses another selected source version: {path}")
        scope = ", ".join(report["selected_symbols"]) or "所選日期全部原生商品"
        lines += [f"## 已完成試合併：{scope}", "",
            f"本節執行契約：`{report['contract']}`。v1 是原試驗基線；v2 單位修補不覆寫舊收據。", "",
            f"{link(path, '完整試驗收據')}；{report['sessions']} 個 session；{report['wall_seconds']:.2f} 秒。", ""]
        counts = report["counts"]
        if report.get("repair_comparison"):
            repair = report["repair_comparison"]
            if report.get("unit_evidence_inventory_sha256") != pinned.get("unit_evidence_inventory_sha256"):
                raise ValueError("repair used different dated unit evidence")
            lines += ["本節是單位修補的前後逐列驗收，不是另造價格或重跑未受影響的歷史。", ""]
            lines += _table(["修補驗證", "結果"], list(repair.items()))
        lines += _table(["驗證項目", "結果"], [
            [name, counts.get(name, 0)] for name in ("selected_input_files", "source_rows", "canonical_rows",
             "duplicate_rows", "rejected_rows", "conflicting_valid_rows", "rejected_input_files",
             "sessions_with_no_accepted_rows", "equity_rows_without_share_quantity",
             "rejected_unit_evidence_files", "conflicting_unit_proof_rows")])
        lines += _table(["選取／補缺原因", "列數"], sorted(report["selection"].items()))
        lines += [f"模型准入：`training_ready={report['training_ready']}`；"
            f"全歷史完整聲明：`{report['all_history_complete_claim']}`；"
            f"試組合保留：`{report.get('assembled_dataset_retained', '未宣告')}`。", "",
            "valid provider 差異保留主來源與雙方原值，不因不一致就宣告其中一方損壞。",
            "未知股份單位仍是 NULL，不計為 50% 分鐘成交容量；TAIEX 的零量不是股票可成交量。", "",
            f"分日精確區段／conflicts／rejected／source_refs 在 {link(path.parent / 'sessions', 'sessions')}。", ""]
        coverage = report["coverage_by_symbol_provider"]
        if len(coverage) <= 20:
            lines += _table(["商品類別:代號:provider", "首個選取日", "最後選取日"], [
                [key, value["first"], value["last"]] for key, value in sorted(coverage.items())])
        else:
            lines += [f"此範圍共 {len(coverage)} 個「商品類別×代號×選取 provider」條目，完整首末範圍在上述 JSON。", ""]
        lines += ["首末範圍仍非連續供應證明；`lineage_segments.parquet` 遇缺分鐘或 provider 改變就拆段。", ""]
        for page in _interval_pages(path, report, output):
            lines += [link(page, f"{page.stem} 全部精確來源區段（Markdown）"), ""]
    lines += ["## 所有已明確定義的財務等價映射", "",
        "以下沿用 `tw_public_cross_source_fill.MAPPINGS`，不是僅靠欄位同名的新映射，也不是全部都已通過合併。",
        "FinLab 為相應主來源、FinMind 為候選；只有既有原生期別／單位與重疊 QA 通過才做 missing-only。",
        "現有主來源真實零值及非空值不被覆寫。公式值必須有完整原始期別操作數；不可把期別當公布日。", ""]
    lines += _table(["FinLab 經濟量", "FinMind dataset", "原生欄位", "期別", "倍率", "轉換"], [
        [name, spec["dataset"], spec["field"], spec["kind"], spec["factor"], spec["transform"]]
        for name, spec in sorted(pinned["reviewed_financial_aliases"].items())])
    lines += ["季度現金流累計→單季需相鄰原始季度；營收需正確 subject month；TDCC 不含總計／差異調整。",
        "未列出的同名或相似科目不會猜著合併；保留原始來源，等待 entity/unit/report-basis/vintage 對照。", "",
        "## 合併方法與重建入口", "",
        link(ROOT / "docs/tw_raw_source_merge_2026-10-07.md", "完整方法、錯誤分類及重建指令"), "",
        "來源 inventory 固定 receipts／bytes；merge receipt 另固定實際 normalizer／merger 程式 SHA。",
        "FinLab／FinMind 限受許可的私人研究。沒有公開發布、沒有更動正在訓練的資料與 optimizer。", ""]
    atomic_write_text(output, "\n".join(lines))
    atomic_write_json(output.with_suffix(".receipt.json"), {
        "contract": "tw_raw_merge_markdown_v1", "output_sha256": sha256_file(output),
        "source_inventory_file_sha256": sha256_file(inventory_path),
        "unit_evidence_inventory_sha256": pinned.get("unit_evidence_inventory_sha256"),
        "trial_report_sha256": {str(p): sha256_file(p) for p in report_paths},
        "renderer_code_sha256": sha256_file(Path(__file__)),
        "model_provider_channels_added": False})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--trial-report", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    render(args.inventory, args.trial_report, args.output)
    print(json.dumps({"markdown": str(args.output), "trial_reports": len(args.trial_report)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
