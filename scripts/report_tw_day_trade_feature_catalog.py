#!/usr/bin/env python3
"""Render the local admission bundle as Markdown, CSV and an executed notebook."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_text
from scripts.build_tw_feature_admission_bundle import csv_rows
from scripts.prepare_tw_day_trade_feature_catalog import read_json, write_csv

CORE_UNITS = {
    "open_raw": "TWD/share", "high_raw": "TWD/share", "low_raw": "TWD/share", "close_raw": "TWD/share",
    "trading_volume_raw": "shares", "twpub_official_trading_value_raw": "TWD", "twpub_official_trades_raw": "trades",
    "twpub_pe_raw": "ratio", "twpub_pb_raw": "ratio", "twpub_dividend_yield_pct_raw": "percent",
    "twpub_twse_taiex_raw": "index_points", "twpub_cbc_fx_reserves_usd_billion_raw": "USD_billion",
}


def units(name: str) -> str:
    if name in CORE_UNITS: return CORE_UNITS[name]
    if name.endswith("_lots_raw"): return "official_lots"
    if name.endswith("_shares_raw"): return "shares"
    if name.endswith("_pct_raw"): return "percent"
    raise ValueError(f"unregistered core unit: {name}")


def source_admission(row: dict) -> dict:
    name = row["id"]
    result = {**row, "training_admission": "needs_feature_adapter_and_timing_evidence",
              "training_reason": "download_or_service_status_does_not_certify_a_model_input"}
    if name.startswith("credential:") or name.startswith("group:"):
        result.update(training_admission="operational_or_aggregate_not_feature",
                      training_reason="credentials_and_overlapping_groups_are_not_independent_signals")
    elif name.startswith("keyed-public:"):
        result.update(training_admission="catalog_or_recent_snapshot_not_history",
                      training_reason="endpoint_catalog_or_current_weather_air_quality_or_symbol_snapshot; needs_actual_history_and_available_at")
    elif row.get("status") in {"blocked", "failed", "degraded", "partial", "waiting", "deferred"}:
        result.update(training_admission="missing_or_restricted_source",
                      training_reason="inspect_scope_clock_and_coverage; degraded_can_mean_unverified_PIT_not_missing_bytes")
    return result


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dataset-dir", type=Path, required=True)
    p.add_argument("--audit-dir", type=Path, required=True)
    p.add_argument("--report-path", type=Path, required=True)
    args = p.parse_args()
    out, audit = args.dataset_dir.resolve(), args.audit_dir.resolve()
    manifest = read_json(out / "dataset_manifest.json")
    catalog = read_json(audit / "catalog_summary.json")
    sources = csv_rows(audit / "source_inventory.csv")
    source_rows = [source_admission(row) for row in sources]
    accepted_ids = {"tw-public:" + row["source"] for row in csv_rows(audit / "accepted_core_audit/source_profiles.csv")}
    accepted_ids |= {"tw-public:cbc_fx_reserve_release_vintages", "tw-public:cbc_money_release_vintages",
                     "tw-public:dgbas_release_vintages"}
    research_receipt = read_json(out / "research_features.finlab_research.json")
    finlab_ids = {"finlab:" + name for name in research_receipt["inputs"]["sources"]}
    for row in source_rows:
        if row["id"] in accepted_ids:
            row.update(training_admission="accepted_for_listed_core_features_2014_plus",
                       training_reason="accepted_core_audit; only selected columns and audited horizon, not all provider fields")
        elif row["id"] in finlab_ids:
            row.update(training_admission="research_adapter_available_not_strict_PIT",
                       training_reason="FinLab research overlay source; provider revision and historical clock unverified")
    write_csv(audit / "source_training_admission.csv", source_rows)
    source_ids = {row["id"] for row in source_rows}
    unmapped = [{"expected_source_id": name, "status": "independently_audited_without_exact_registry_id",
                 "evidence": "accepted_core_audit/source_profiles.csv or release-vintage audit"}
                for name in sorted(accepted_ids - source_ids)]
    write_csv(audit / "core_source_registry_gaps.csv", unmapped,
              ["expected_source_id", "status", "evidence"])
    features = csv_rows(audit / "model_feature_admission.csv")
    queue = csv_rows(audit / "finmind_partition_states.csv")
    queue_counts = Counter()
    for row in queue: queue_counts[row["state"]] += int(row["partitions"])
    core = manifest["core"]
    research = manifest["research"]
    annual_missing = Counter(int(row["year"]) for row in csv_rows(audit / "research_missing_feature_years.csv"))
    write_csv(audit / "core_feature_units.csv", [{"feature": r["feature"], "units": units(r["feature"])}
                                               for r in features if r["layer"] == "core"])
    recheck = read_json(audit / "source_failure_recheck.json")
    broken = csv_rows(audit / "source_failure_recheck.csv")
    minute = next(r for r in sources if r["id"] == "group:tw-minute-train")
    accepted = read_json(audit / "accepted_core_audit/summary.json")
    lines = [
        "# 台股當沖訓練特徵清點與資料集 — 2026-09-28", "",
        "## 1. 執行結果與適用邊界", "",
        f"- 清冊快照：{catalog['monitor_generated_at_utc']}；補充讀取到 {catalog['finished_at_utc']}。下載器持續運作，這是有時間標記的盤點，不是所有來源同時凍結的快照。",
        f"- 已清點 **{catalog['registered_sources']:,} 個註冊來源項目**，以及監控的 **{catalog['physical_field_rows']:,} 個實體 dataset×field**；後者包含股票代碼欄、主鍵、文字與不同版本，不是同等數量的模型特徵。",
        f"- 補查 **{catalog['finlab_source_keys']:,} 個 FinLab key、{catalog['finmind_current_receipts_inspected']:,} 個 FinMind 當前分區憑證、{catalog['economic_current_files_inspected']:,} 個 BEA/Census/MOI 當前 normalized 檔**。舊 content-addressed 版本沒有重複加總。",
        f"- 核心：{core['value_channels']} 個數值 + {core['availability_channels']} 個缺值標記；{core['first_session']}～{core['last_session']}，{core['sessions']:,} 個交易日、{core['symbols']:,} 檔股票／ETF 的歷史 universe、{core['rows']:,} 個 alive date×symbol 列。包含已下市標的，並非今天可交易股票數。",
        "- 日期截至 9/24 並非漏掉 9/28：本地官方休市表列明 9/25 中秋節、9/28 教師節休市，中間是週末。",
        f"- 研究：{research['external_value_channels']} 個外部欄位（204 TW public/TAIFEX/MOPS + 104 FinLab），共 {research['rows']:,} 個稀疏 date×symbol 列；既有研究 selector 加 20 股價衍生欄、308 availability 後是 **{research['configured_total_channels']} 通道**。這是配置展開數，不是本次 GPU 訓練／完整研究 tensor 壓測結果。",
        f"- 嚴格核心驗收：model_safe={accepted['model_safe']}，finding_counts={accepted['finding_counts']}。此處 model_safe 僅限已選特徵、日資料和稽核契約。",
        "- **特徵資料就緒，不等於整個當沖成交訓練已驗收。** `core_feature_audit.yaml` / `research_feature_audit.yaml` 僅為特徵建置與檢查配置；其繼承的 naive close-proxy labels 不是 09:01 成交。不可把這兩個配置的回測當成當沖績效。正式當沖模式仍須接既有 phase-aware/分鐘執行契約並獨立檢查，不可再把已對齊 09:00 的資料多移一天。",
        "- 沒有啟動訓練、修改服務、切換模型、送單、傳送 FinLab 授權原始資料或更動冷庫。", "",
        "## 2. 可以用、研究限定、不能直接用", "",
        "| 類別 | 本次判定 | 原因／處理 |", "|---|---|---|",
        "| 官方 OHLCV、成交值／筆數、估值、融資券、法人、TAIEX、CBC／DGBAS 原始發布值 | 27 個核心數值通過盤前特徵門檻 | 11 個當日完整資訊欄往下一交易日；10 個籌碼欄已由來源建置器延後；6 個總經欄按原始發布版本 |",
        "| 308 欄寬研究表 | 可做明示近似的研究，不能當成嚴格歷史實盤資料 | 保留現修值、理論財報公布日、晚近快照的限制；禁止反向補到歷史 |",
        "| FinLab 其他已下載 key、FinMind 其他欄位 | 有資料但不自動批准接入模型 | 尚缺 entity/unit/available_at/vintage 的逐來源轉換與驗證；股價重複來源先對帳，不能盲接或重複算成獨立資訊 |",
        "| BEA／Census／MOI | 原始研究候選 | receipt 的 historical_point_in_time=false；觀測期不等於公布時點，數字字串及單位需按資料集解析 |",
        "| NOAA／CWA／MOENV／Finnhub 等 catalog 或近期快照 | 不能直接充當歷史特徵 | 目錄是可下載項目的清單；近期天氣／空品觀測與股票清單不是多年歷史 |",
        "| 海外股／外匯／加密／FRED／CFTC／OpenBB | 清冊已納入；尚未接入台股 feature ABI | 需驗證時區、發布延遲、native grain、實際 vintage、單位和交易日對齊 |",
        "| source/date/symbol/id/path/hash 等 | 不是數值 alpha | 作主鍵或來源證據，不當成連續模型訊號 |", "",
        "以上不是說只有 27 個特徵永遠合格：其他同源衍生式可能可用，但本次只對列出的核心選集完成端到端盤前驗收。", "",
        "## 3. 具體缺口與不合格項目", "",
        f"- 研究表有 **{research['features_starting_2026']} 欄直到 2026 年才有第一筆值**。TDCC 官方快照、近期營收／財務／公司基本資料、若干舊期交所摘要等，不能拿來宣稱 2014 年起同樣完整。逐欄逐年缺口見 `research_missing_feature_years.csv`。",
        f"- 年度實測：2014 年有 {research['external_value_channels'] - annual_missing[2014]}/308 欄至少一筆值；2026 年截至資料截止日有 {research['external_value_channels'] - annual_missing[2026]}/308 欄至少一筆值，另 {annual_missing[2026]} 欄整年尚無值。『至少一筆』不是全市場每日完整。",
        f"- 在本次盤點所選的檔案中，有 **{catalog['admission_counts'].get('missing_values', 0)} 個 dataset×field 為 NULL**、{catalog['admission_counts'].get('incomplete_source', 0)} 個欄位的來源驗證不完整；不代表全 provider 的現行資料皆空，也不是 core 的 27 個數值欄。OpenBB 舊 compact 必須另外對照較新 L1。",
        f"- 主監控 schema 證據仍為 {catalog['monitor_schema_coverage']['files_with_schema']:,}/{catalog['monitor_schema_coverage']['files_total']:,} 個檔，不完整項不冒充全量驗證。catalog 補充檢查也只驗 footer/receipt；只有採入 core/research 的來源另有相應 bytes/hash 驗證。",
        f"- FinMind 佇列：pending={queue_counts['pending']:,}、inflight={queue_counts['inflight']}、failed={queue_counts['failed']}、invalid_request={queue_counts['invalid_request']}、calendar_wait={queue_counts['calendar_wait']}。observed_empty={queue_counts['observed_empty']:,} 只表示查詢回應空，不能一概當成『缺資料』或『歷史完整』。",
        f"- FinMind 失敗分區已再次只讀檢查 {recheck['current_complete_partitions_inspected']:,} 個分區，仍有 {recheck['failures']} 個檔失敗；其中 {sum(r.get('bytes') == '0' for r in broken)} 檔為 0 bytes。原始完成 receipt 與空檔不一致，不能只信 queue 的 complete。未重寫正在運作的下載器或原檔。",
        "", "| 來源 | 標的／分區 | 問題 |", "|---|---|---|",
    ]
    lines.extend(f"| {r['dataset']} | {r['data_id'] or '全市場'} / {r['partition']} | {r.get('bytes', 'unknown')}-byte Parquet；receipt rows={r['receipt_rows']} |" for r in broken)
    lines += ["", f"- 台股分鐘來源監控：{minute['coverage_current']}/{minute['coverage_total']} 標的；底層表 {minute['record_count']} 列，{minute['first_observed']}～{minute['last_observed']}。標的數覆蓋不代表每個股票日都有 270 個可執行分鐘；本次沒有重新驗證所有分鐘成交容量或用日 K 假冒分鐘。",
              "- FinLab 分鐘／tick 目前仍是有限分區，不可替代全市場歷史；兩個 FinLab key 在清冊標為 degraded（`dividend_otc:權息`、`management_change_events:變更交易開始日`），未採入核心。",
        "- MOPS 原始欄位清冊有 `1911-00-07` 等非法日期界線；它是原始 mixed-format 欄位統計，不是可直接使用的公布日期。XBRL 在研究表走既有解析與理論日期標記，不以這個 min/max 認證 PIT。",
              "- `twse_taiex_ohlc` 沒有相符的獨立註冊來源 ID（可能由集合項目代表）；本次已直接驗收正本與交易日，不把集合狀態當其獨立證據。詳見 `core_source_registry_gaps.csv`，這不是資料檔缺失。",
              "- 核心仍保留一項 medium 限制：並非所有下市事件都有對應的歷史強制回補公告；沒有自行猜補公告時間。", "",
              "## 4. 缺值、時間與年度切分", "",
              "- `preopen_core_features.parquet` 只含 date、symbol、27 數值與27布林 available；沒有未來報酬、下一日收盤或成交 labels。原生未知仍為 NULL，真正觀測的 0 仍是 0。",
              "- canonical panel 內未知值的 0 只是張量容器；必須一起使用 availability，不能單看填零後的 finite rate 宣稱 100% 資料覆蓋。",
              "- 研究表市場總體欄在 `__MARKET__` 列，個股欄在各股列，不能用全表 stock_rows 當總經覆蓋率分母；月／季資料的 event 稀疏不代表應補成每日新觀測。",
              "- 沿用年度 walk-forward，2014～2026 無缺年；2026 是截至 9/24 的部分年度。標準化／特徵篩選只能在該 fold 的 train years fit，不得用驗證或測試報酬挑 feature。",
              "- 只做已登記的狀態 carry；事件不可盲目 forward-fill，也沒有插值、把今日快照倒灌歷史或臆造缺價。", "",
              "## 5. 已修正與測試", "",
              "- 使用既有官方股票建置器及 TW public 建置器，在獨立目錄重建股票、143 欄原表與憑證。初始 2 個 critical stale receipt 已由重新稽核驗收；重建前後特徵 parquet SHA 相同，代表這次主要是來源版本憑證落後，而非擅改數值。",
              "- 原特徵盤點工具只掃 `twpub_*`，會漏掉 `twfl_*`。已補上 FinLab namespace，並測試缺欄、duplicate grain、NULL 與有效零的區別。",
              "- masked core 的27個數值逐塊比對已驗收 panel，完全相同；新增 availability 不改變既有數值。",
              "- 測試結果見 `test_results.txt`；原始／修復後完整 audit 各自保留。`verification.ipynb` 供重跑核心驗證。若本機沒有 ipykernel，產生器用同一個 runtime Python 依序執行自建 cell 並保存輸出，不宣稱通過 Jupyter kernel 驗收；實際方式在 notebook metadata。未另外做 Notebook viewer 視覺驗收，可在 IDE 的 Jupyter 檢視器開啟核對排版。", "",
              "## 6. 產物索引", "",
              f"資料集目錄：`{out.relative_to(ROOT)}`。清冊目錄：`{audit.relative_to(ROOT)}`。", "",
              "| 檔案 | 用途 |", "|---|---|",
              "| dataset_manifest.json | 狀態、範圍、SHA、執行限制 |",
              "| preopen_core_features.parquet | 54 通道的明確缺值、09:00 對齊 feature-only 表 |",
              "| features/tw_public_stock_daily.parquet + stocks/ | 重建後的 canonical 輸入 |",
              "| research_features.parquet + .finlab_research.json | 308 外部欄位研究表與映射／來源憑證 |",
              "| source_inventory.csv / source_training_admission.csv | 全部 1,869 註冊來源、下載與訓練資格分開 |",
              "| feature_candidates.csv / physical_fields.csv | 實體欄位及來源候選清單，不是模型維度 |",
              "| model_feature_admission.csv / core_feature_units.csv | 每個已建模型特徵的資格、日期、單位 |",
              "| research_feature_profile/annual_feature_inventory.csv | 每年、每個 feature 的實際 non-null 數 |",
              "| research_missing_feature_years.csv | 2014 年起整年無觀測值的研究欄位 |",
              "| finmind_partition_states.csv / source_failure_recheck.csv | 下載欠帳、損壞檔與可追查 receipt 路徑 |", "",
              "## 7. 來源與證據", "",
              "- 本文數量來自本地已留存的 monitor 快照、SQLite queue 唯讀快照、當前 receipt、Parquet footer／全表 profile、canonical panel audit；來源 first/last 與實際可知時間分開。",
              "- FinLab 官方亦區分實際公告日與統一截止日；因此不能把季度代碼直接當可交易日期。[FinLab 官方 FAQ](https://finlab.finance/docs/faq/)",
              "- FinMind 官方技術面資料文件提供各端點欄位與粒度；同名成交量仍須按端點單位契約對帳。[FinMind 官方文件](https://finmind.github.io/tutor/TaiwanMarket/Technical/)", "",
              "## 附錄：本次核心數值欄位", "", "| feature | 單位 | 未知 cells | 實際為零 cells |", "|---|---|---:|---:|"]
    lines.extend(f"| {r['feature']} | {units(r['feature'])} | {r['missing_cells']} | {r['observed_zero_cells']} |"
                 for r in features if r["layer"] == "core")
    lines += ["", "## 重跑檢查", "",
              "以下只重跑清冊／特徵驗證，不啟動模型訓練；保留現有產物時請使用新的 dated output 目錄。核心資料更新必須先按本次使用的 canonical `build_tw_official_symbol_parquets.py`、`build_tw_public_training_features.py` 重建，再通過 `audit_tw_public_data_layer.py --strict --require-live-selected-features`，不可只手改 receipt。", "",
              "```bash", "cd /root/stockAgent", "source scripts/runtime_env.sh",
              "run_fintech_python scripts/prepare_tw_day_trade_feature_catalog.py \\",
              "  --output-dir artifacts/data_quality/tw_day_trade_feature_catalog_NEW --workers 4",
              "run_fintech_python -m pytest -q \\",
              "  test/test_tw_public_data_layer_tools.py \\",
              "  test/test_tw_public_training_features.py \\",
              "  test/test_tw_public_research_all_features.py \\",
              "  test/test_finlab_research_overlay.py \\",
              "  test/test_tw_day_trade_feature_catalog.py", "```", ""]
    followup = ROOT / "docs/downloader_null_integrity_repair_2026-09-28.md"
    if followup.is_file():
        lines += ["", "## 後續更正", "",
                  "以上保留初次盤點時點；後續的 5 個 FinMind 空檔修復與 OpenBB L1 空欄位更正，"
                  "請見 [下載器修復紀錄](downloader_null_integrity_repair_2026-09-28.md)。"
                  "最新修正清冊：`artifacts/data_quality/downloader_integrity_20260928/feature_candidates_corrected.csv`。"]
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(args.report_path, "\n".join(lines) + "\n")
    import nbformat
    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_markdown_cell(
        "# 台股 feature bundle 驗證\n\n## 摘要\n核心27值+27 availability；研究308外部值，不能當成歷史 PIT。\n\n"
        "## 方法與資料\n來源：本次本地清冊與資料集。只驗證特徵，不驗證當沖成交或模型盈利。\n\n"
        "### 假設\n不用全表 finite 率代替原始觀測覆蓋。只讀取已建資料，不即時重新抓取來源。\n\n### 1. 載入與 SHA／列數檢查"),
        nbformat.v4.new_code_cell(f"""from pathlib import Path
import json
import pandas as pd
import pyarrow.parquet as pq
from scripts.prepare_tw_day_trade_feature_catalog import sha256
bundle = Path({str(out)!r})
audit = Path({str(audit)!r})
manifest = json.loads((bundle / 'dataset_manifest.json').read_text())
assert manifest['core_feature_ready'] is True
assert manifest['day_trade_execution_training_ready'] is False
core = manifest['core']
assert sha256(Path(core['path'])) == core['sha256']
assert pq.ParquetFile(core['path']).metadata.num_rows == core['rows']
print({{k: core[k] for k in ['rows','sessions','symbols','value_channels','availability_channels','first_session','last_session']}})
"""), nbformat.v4.new_markdown_cell("### 2. 資格、品質閘門與逐年缺口"), nbformat.v4.new_code_cell("""features = pd.read_csv(audit / 'model_feature_admission.csv')
assert not features.duplicated(['layer', 'feature']).any()
print(features.groupby(['layer', 'admission']).size().to_string())
accepted = json.loads((audit / 'accepted_core_audit/summary.json').read_text())
assert accepted['model_safe']
print(accepted['finding_counts'])
missing = pd.read_csv(audit / 'research_missing_feature_years.csv')
assert (missing.raw_non_null == 0).all()
print(missing.groupby('year').size().to_string())
"""), nbformat.v4.new_markdown_cell(
        "## 結論\n核心已做來源／時鐘／數值比對；晚近欄位仍是研究限定，缺年不能假補。"
        "完整當沖分鐘執行、GPU 訓練及此 Notebook 的視覺排版不在本次驗收範圍。")]
    nb.metadata["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    if importlib.util.find_spec("ipykernel") is not None:
        from nbclient import NotebookClient
        NotebookClient(nb, timeout=180, resources={"metadata": {"path": str(ROOT)}}).execute()
        execution = "nbclient_kernel"
    else:
        # Execute only cells authored immediately above, not uploaded notebook
        # code. Do not modify the user's training environment to add a viewer.
        namespace = {}
        for count, cell in enumerate((c for c in nb.cells if c.cell_type == "code"), 1):
            stream = io.StringIO()
            with redirect_stdout(stream):
                exec(compile(cell.source, f"verification-cell-{count}", "exec"), namespace)
            cell.execution_count = count
            cell.outputs = [nbformat.v4.new_output("stream", name="stdout", text=stream.getvalue())]
        execution = "runtime_python_sequential_cells; ipykernel_not_installed; kernel_validation_not_run"
    nb.metadata["execution_contract"] = execution
    nbformat.validate(nb)
    atomic_write_text(audit / "verification.ipynb", nbformat.writes(nb))
    print(json.dumps({"report": str(args.report_path), "source_rows": len(source_rows),
                      "model_feature_rows": len(features), "notebook_executed": True}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
