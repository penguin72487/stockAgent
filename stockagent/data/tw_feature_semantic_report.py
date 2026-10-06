"""Economic meaning and acquisition evidence, independent of model admission.

This is a report contract, not a new training allowlist. Source identities and
native dimensions survive; identical labels do not establish equivalent facts.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unicodedata

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.data.tw_day_trade_feature_admission import (
    CATEGORY_LABELS, category_for, housing_reason,
)

CONTRACT = "tw_all_source_economic_feature_report_v1"
FACTORIZED_REPORT_CONTRACT = "tw_nullable_factorized_feature_report_v4_channel_policy_named_coverage"
MEANINGFUL_ROLES = frozenset({"economic_measure", "economic_category", "economic_text"})
ROLE_LABELS = {
    "economic_measure": "有經濟意義的數值／比率／旗標候選",
    "economic_category": "有經濟意義的類別，需要明確編碼",
    "economic_text": "有經濟意義的事件／敘述，需要事件或文字編碼",
    "measure_container": "多概念數值容器，不能直接算成獨立特徵",
    "dimension_key": "實體／期間／單位／版本軸，不直接作數值訊號",
    "provenance": "來源與管理證據，不作模型訊號",
    "execution_or_label": "執行規則／目標標籤，不混入決策特徵",
    "unknown_definition": "語意尚未辨識，保留待核對",
}
SCOPE_LABELS = {
    "tw_equity": "台股／公司層候選（實體映射仍需驗證）", "market_context": "跨市場／總經背景候選",
    "housing_excluded": "房屋相關：沿用使用者排除要求",
    "warrant_excluded": "權證：沿用不交易權證要求",
    "not_model_signal": "鍵／證據／執行層，不計入訊號數",
}
STATE_LABELS = {
    "research_value_wired": "已接入 251 值的私人研究表（不等於正式訓練驗收）",
    "no_variation_in_selected_view": "有觀測，但所選研究期間未通過變異門檻",
    "needs_adapter_clock_units": "有意義候選；尚需來源轉換、單位與可知時間接線",
    "source_quality_review": "保留候選；需核對來源／衍生層品質",
    "no_observed_value_in_selected_source": "所選檔未有觀測值；不是全來源無資料證明",
    "tej_values_acquired_unwired": "TEJ 已有非空匯出值；尚未接入台股訓練",
    "tej_catalog_only_or_not_yet_acquired": "TEJ 目錄可見；本次未證明取得非空值",
    "not_selected_input": "非本次模型輸入；保留其來源角色",
    "needs_semantic_definition": "原生概念語意／型別待確認，不等於無意義或來源壞掉",
    "needs_text_or_parse_adapter": "原檔有概念但數值投影無有效數字；需核對文字／日期／parse 狀態",
}


def write_factorized_feature_report(manifest_path: Path, output: Path, *, page_size: int = 100) -> dict:
    """Detailed Markdown companion for the canonical nullable panel builder.

    Existing source-wide inventories remain authoritative for unadapted source
    families. This report enumerates this exact view's selected quantities,
    missingness channels, approved aliases and every quarantine decision.
    """
    if output.exists():raise FileExistsError("feature report requires a fresh output directory")
    if not 1<=page_size<=1000:raise ValueError("bounded Markdown page size required")
    manifest=json.loads(manifest_path.read_text())
    dictionary_path=manifest_path.parent/"feature_dictionary.json"
    if digest(dictionary_path)!=manifest["feature_dictionary_sha256"]:
        raise ValueError("feature dictionary differs from panel manifest")
    dictionary=json.loads(dictionary_path.read_text());output.mkdir(parents=True)
    coverage_path=manifest_path.parent/"feature_coverage.csv"
    coverage={}
    if coverage_path.exists():
        with coverage_path.open(encoding="utf-8",newline="") as stream:
            for row in csv.DictReader(stream):
                name=row["feature"]
                if name in coverage:raise ValueError("duplicate feature coverage identity")
                coverage[name]=row
    pages=[]
    def safe(value):
        return str(value).replace("|","\\|").replace("\n"," ")
    def feature_scope(row):
        scope=row.get("scope",row["rule"]["scope"])
        if scope not in {"stock","market"} or scope!=row["rule"]["scope"]:
            raise ValueError("feature scope disagrees with its registered release rule")
        return scope
    for scope in ("stock","market"):
        rows=[r for r in dictionary["features"] if feature_scope(r)==scope]
        for begin in range(0,len(rows),page_size):
            path=output/f"{scope}_{begin//page_size+1:04d}.md"
            channel_description = ("模型只輸入每項數量的 value，不輸入可用性、年齡或更新旗標。" if manifest.get("model_channel_policy") == "value_only" else "模型每項數量使用 value、available、age_days、updated 四個通道。")
            lines=[f"# {'個別' if scope=='stock' else '共同'}特徵 {begin+1}–{min(begin+page_size,len(rows))}","",
                "原始未觀測座標為 NULL；"+channel_description+"來源鍵、SHA 與單位標籤只作證據，沒有編成數值訊號。",""]
            for row in rows[begin:begin+page_size]:
                label=(row.get("dataset") or ", ".join(row.get("labels",[]))
                    or ", ".join(row.get("original_concepts",[]))
                    or row.get("source_column") or row.get("feature") or str(row.get("identity",{})))
                rule=row["rule"]
                lines.extend([f"## {safe(label)}","",f"- 模型 quantity ID：`{row['feature']}`。",
                    f"- 來源：{safe(row['source'])}；範圍：{scope}；時鐘：{safe(row.get('clock',''))}。",
                    f"- 更新規則：{safe(rule['kind'])}；最多延續 {rule['carry_days']} 日；年齡按台北日曆日計。",
                    f"- NULL 事件語意：{safe(row.get('null_event_policy','released_null_barrier'))}。",
                    f"- 規則依據：{safe(rule['rationale'])}。",
                    "- 身分、單位、原科目與時鐘完整定義：","","```json",json.dumps(row,ensure_ascii=False,sort_keys=True,indent=2),"```",""])
                if row["feature"] in coverage:
                    record=coverage[row["feature"]]
                    if record["scope"]!=scope or record["source"]!=row["source"]:
                        raise ValueError("feature coverage scope/source differs from dictionary")
                    count=int(record["available_panel_cells"])
                    denominator=len(manifest["dates"])*(len(manifest["symbols"]) if scope=="stock" else 1)
                    if not 0<=count<=denominator:raise ValueError("invalid feature availability count")
                    lines.extend([f"- 訓練 view 可用{'股票×日期座標' if scope=='stock' else '日期'}：{count:,}／{denominator:,}。",
                        "- 此分母包含尚未上市、商品不適用與無歷史觀測的座標；不是合格交易目標的缺資料率。",""])
            atomic_write_text(path,"\n".join(lines));pages.append(path.name)
    exclusions=dictionary.get("excluded",[])
    for begin in range(0,len(exclusions),page_size):
        path=output/f"excluded_{begin//page_size+1:04d}.md"
        lines=[f"# 未接入／待處理來源 {begin+1}–{min(begin+page_size,len(exclusions))}","",
            "排除不等於來源損壞：可能沒有訓練期歷史、需要文字／維度編碼、單位未驗證或時鐘尚未接線。原始來源不刪除。",""]
        for row in exclusions[begin:begin+page_size]:lines.extend(["```json",json.dumps(row,ensure_ascii=False,sort_keys=True,indent=2),"```",""])
        atomic_write_text(path,"\n".join(lines));pages.append(path.name)
    alias_path=output/"aliases.md"
    atomic_write_text(alias_path,"# 同義合併與缺值聯集證據\n\n"+"\n\n".join("```json\n"+json.dumps(row,ensure_ascii=False,sort_keys=True,indent=2)+"\n```" for row in dictionary.get("aliases",[])))
    lines=["# 固定版本 Nullable Panel 特徵清冊","",
        f"- 原始資料版本：`{manifest['source_snapshot_id']}`。",
        f"- 日期：{manifest['dates'][0]} 至 {manifest['dates'][-1]}；可交易實體軸：{len(manifest['symbols']):,}。",
        f"- 個別數量：{manifest['individual_quantities']:,}；共同數量：{manifest['shared_quantities']:,}；模型通道：{manifest['logical_model_channels']:,}。",
        f"- 模型通道規則：{manifest.get('model_channel_policy','value_available_age_updated')}；公布時鐘、NULL／TTL／生命週期屏障仍屬資料規則，不是模型輸入。",
        f"- 基礎欄：{', '.join(manifest['base_feature_names'])}。",
        f"- 訓練所有權篩選：{manifest.get('training_owned_admission_cutoff','')}；逐 fold 正規化仍只使用該 fold 訓練窗口。",
        "- 這是含估計公布時鐘／現行修訂值的私人研究表，不宣稱已重建歷史原始 vintage，也不是實盘訊號輸入。",
        "- 資料 view 已建立不等於完整訓練通過；嚴格 data gate、CUDA、雙卡訓練與產物驗收分別記錄。", "",
        "## 全部明細分冊","",*[f"- [{p}]({p})" for p in pages],f"- [同義合併證據]({alias_path.name})",""]
    atomic_write_text(output/"index.md","\n".join(lines))
    proof={"contract":FACTORIZED_REPORT_CONTRACT,
        "model_channel_policy":manifest.get("model_channel_policy","value_available_age_updated"),
        "manifest_sha256":digest(manifest_path),"dictionary_sha256":digest(dictionary_path),
        "coverage_sha256":digest(coverage_path) if coverage_path.exists() else None,
        "selected_quantities":len(dictionary["features"]),"excluded_records":len(exclusions),
        "markdown_pages":len(pages)+2,"report_only":True}
    atomic_write_json(output/"report_receipt.json",proof)
    return proof
TEJ_CATEGORIES = {
    "financial": "fundamentals", "monthly_revenue": "fundamentals",
    "segments_subsidiaries": "fundamentals", "audit_report": "company_profile",
    "company_lifecycle": "company_profile", "governance_investments": "company_profile",
    "corporate_actions": "corporate_events", "events": "corporate_events",
    "stock_daily": "price_technical", "stock_adjusted": "price_technical",
    "stock_daytrade": "liquidity", "stock_flow": "institutional",
    "stock_lending": "margin_lending", "stock_margin": "margin_lending",
    "futures": "derivatives", "options": "derivatives", "fx": "fx_rates",
    "macro_banking": "macro", "funds_bonds": "funds", "global_indices": "cross_market",
}
FINANCIAL_DATASETS = frozenset({
    "TaiwanStockBalanceSheet", "TaiwanStockFinancialStatements", "TaiwanStockCashFlowsStatement",
})
PROVENANCE = re.compile(
    r"(?:^|[ _:/])(?:sha\d*|md5|hash|checksum|path|url|uri|filename|receipt|"
    r"retrieved|fetched|downloaded|ingested|schema|payload|context_ref|dimensions_json|"
    r"raw_sha256|archive_sha256|document_sha256|source_member|value_parse_status|"
    r"point_in_time_state)(?:$|[ _:/])|資料來源|下載|網址|檔案|住址|地址|電話|傳真|email|fax",
    re.I,
)
DIMENSIONS = frozenset({
    "id", "code", "symbol", "stock_id", "data_id", "ticker", "date", "time", "timestamp",
    "datetime", "source_index", "type", "origin_name", "concept", "metric", "series_id",
    "seriescode", "linenumber", "table_name", "tablename", "timeperiod", "category_code",
    "data_type_code", "geo_level_code", "seasonally_adj", "error_data", "time_slot_id",
    "time_slot_date", "time_slot_name", "unit", "unit_ref", "cl_unit", "unit_mult", "scale",
    "sign", "decimals", "period", "year", "month", "quarter", "season", "currency",
    "market", "exchange", "country", "contract", "futures_id", "option_id", "entity",
    "entity_identifier", "document_type", "holdingshareslevel", "call_put", "name",
    "consolidation", "f/s_type", "company_name", "companyname", "stock_name", "provider",
    "source", "dataset", "source_id", "archive_name", "raw_value",
})
ECONOMIC_CATEGORIES = re.compile(r"industry|sector|產業|行業|business.*type|信用.*評等|credit.*rating", re.I)
EVENT_TEXT = re.compile(r"headline|title|subject|content|summary|opinion|reason|公告內容|主旨|重大訊息|裁罰|審計意見", re.I)
EXECUTION = frozenset({
    "return_1d", "tradable", "alive_mask", "force_exit_mask", "can_buy_mask", "can_sell_mask",
    "can_short_open_mask", "target", "label", "adjustment_reference_price", "raw_ohlc_scale_factor",
})
CONTAINERS = {
    "tw-public:mops_xbrl_quarterly": {"decimal_value", "raw_value"},
    "physical:fred:observations": {"value"},
    "physical:free-public:observations": {"value_float", "value_text"},
    "physical:etf:issuer-metrics": {"value"},
    "physical:openbb:archives": {"value"},
}


def read_csv(path: Path) -> list[dict]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def truth(value) -> bool:
    return str(value).lower() in {"true", "1"}


def field_label(row: dict) -> str:
    name = str(row.get("field", ""))
    if row.get("provider") == "FinLab" and ":" in name:
        return name.rsplit(":", 1)[-1]
    if row.get("expansion_kind") == "mops_xbrl_concept":
        return name.rsplit("}", 1)[-1].rsplit(":", 1)[-1]
    return name


def economic_category(row: dict) -> str:
    """Source family precedes ambiguous words such as loan, value and equity."""
    provider, dataset, field = (str(row.get(k, "")) for k in ("provider", "dataset_id", "field"))
    if field == "next_session_open_gap_logret":
        return "price_technical"
    if provider == "TEJ":
        category = TEJ_CATEGORIES.get(row.get("tej_category"))
        if category:
            return category
    text = (dataset + " " + field).lower()
    if provider == "FinMind":
        if "holdingsharesper" in text:
            return "shareholding"
        if "taiwanoption" in text or "taiwanfutures" in text or "futopt" in text:
            return "derivatives"
        if any(s in dataset.casefold() for s in ("taiwanstockinfo", "industrychain")):
            return "company_profile"
    if provider in {"bea", "census"}:
        return "fundamentals" if dataset.endswith(":qfr") else "macro"
    if any(s in text for s in ("financial_statement", "fundamental_features", "balancesheet",
                              "financialstatements", "cashflowsstatement", "mops_xbrl", "twpub_xbrl")):
        return "fundamentals"
    if housing_reason(row):
        return "housing"
    if "monthly_revenue" in text or "monthrevenue" in text:
        return "fundamentals"
    if re.search(r"tpex.*(?:index|ceil|ipo_no_limit)|tpex50", dataset, re.I):
        return "microstructure" if "ceil" in dataset else "price_technical"
    if re.search(r"tpex.*cmode|eligibility", dataset, re.I):
        return "execution_rules"
    return category_for(dataset, field, provider)


def financial_subcategory(row: dict) -> str:
    text = (str(row.get("dataset_id", "")) + " " + field_label(row)).casefold()
    rules = (
        (r"monthly.*(?:revenue|sales)|monthrevenue|當月營收|累計營收", "月營收與成長"),
        (r"cash.?flow|cashprovided|cashfrom|現金流|營業活動|投資活動|籌資活動", "現金流量"),
        (r"loan|bank|bis ratio|impairment|overdue|銀行|放款|貸款|呆帳", "銀行、放款與信用風險"),
        (r"eps|earnings.?per.?share|roa|roe|margin|turnover|growth|ratio|率|每股|週轉", "財務比率、每股與成長"),
        (r"asset|liabilit|balance.?sheet|equity|cashhand|capital|資產|負債|權益|股本", "資產負債與資本結構"),
        (r"revenue|income|profit|loss|expense|cost|營收|收益|費用|利潤|損益|淨利", "損益與營運"),
        (r"segment|subsidiar|department|部門|子公司", "部門與子公司"),
    )
    if row.get("category") != "fundamentals":
        return CATEGORY_LABELS.get(row.get("category"), "待辨識")
    return next((label for pattern, label in rules if re.search(pattern, text)), "其他財務科目（保留來源定義）")


def semantic_role(row: dict) -> tuple[str, str]:
    name, dataset = field_label(row).strip(), str(row.get("dataset_id", ""))
    lower = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).casefold()
    expansion = row.get("expansion_kind", "")
    if expansion and expansion != "finlab_table_field":
        # These names came from a measurement's actual concept axis, not schema keys.
        if expansion == "free_public_metric" and str(row.get("source_unit", "")).lower() in {"id", "url", "code"}:
            return "dimension_key", "原始 metric 的單位明確是識別碼／連結，不因儲存為數字變成量測。"
        if PROVENANCE.search(name):
            return "provenance", "概念名顯示為來源／管理資訊；保留證據，不作數值輸入。"
        if lower in DIMENSIONS or lower.endswith(("_id", "_code", "_name")):
            return "dimension_key", "展開概念本身仍是實體／識別／單位軸；不因來自金融表就當數值特徵。"
        if expansion == "mops_xbrl_concept" and re.search(r"(?:^|_)(?:name|address|telephone|fax|identifier)(?:$|_|\d)|(?:date|year|month|quarter)\d*$", lower):
            return "dimension_key", "XBRL 實體姓名／名稱／地址／日期等 context，不當連續金融量測。"
        if expansion == "mops_xbrl_concept" and re.search(r"(?:Axis|Member|Domain|Abstract|Table|LineItems)$", name):
            return "dimension_key", "XBRL taxonomy 的維度／成員／結構節點，不當作獨立數值量測。"
        if expansion == "mops_xbrl_concept" and name.endswith("TextBlock"):
            return "economic_text", "財務揭露文字區塊；先驗內容、版本與公布時鐘，不能當數值欄。"
        if expansion == "mops_xbrl_concept" and row.get("numeric_observations") == 0:
            if re.search(r"note|report|disclosure|description|audit|opinion", name, re.I):
                return "economic_text", "財務揭露／審計／附註候選；原生數值投影沒有數字，不能誤算成數值財報維度。"
            return "unknown_definition", "原檔有此 taxonomy 概念，但無數字投影證據；需查原文／parse 狀態，既不當數值特徵也不判壞檔。"
        if EVENT_TEXT.search(name) or (row.get("numeric_observations") == 0 and row.get("non_null_count", 0) > 0):
            return "economic_text", "原生概念為文字／事件；需要內容與公布時間編碼，不直接轉成數字。"
        return "economic_measure", "由實際長表的概念／系列軸展開；不是把股票或日期軸重複當特徵。"
    if name == "next_session_open_gap_logret":
        return "economic_measure", "09:00 收到當日開盤報價後才可用；不是盤前可知，也不是未來收盤標籤。"
    if name.startswith(("_twpub_", "target_", "label_", "execution_", "exit_")) or lower in EXECUTION:
        return "execution_or_label", "執行／結算／目標層資料；須與模型決策輸入隔離。"
    if PROVENANCE.search(name) or name.startswith("_"):
        return "provenance", "描述來源、檔案、擷取與管理過程，不是經濟量測。"
    if lower in {"remark", "remarks", "comment", "comments", "note", "notes", "note_ref", "noteref"} or name in {"備註", "註記", "說明", "附註"}:
        return "provenance", "來源附註／說明欄不是數值量測；保留作來源解讀，不任意編碼成 alpha。"
    if name in CONTAINERS.get(dataset, set()) or (
        row.get("provider") == "FinMind" and dataset.rsplit(":", 1)[-1] in FINANCIAL_DATASETS and name == "value"
    ) or (row.get("provider") == "bea" and name == "DataValue") or (
        row.get("provider") == "census" and name.casefold() == "cell_value"
    ) or (row.get("provider") == "FinLab" and ":" not in dataset and not expansion):
        return "measure_container", "一個表／value 欄可能承載多項概念；應查看展開清冊，不把容器與子概念重複計數。"
    if ECONOMIC_CATEGORIES.search(name):
        return "economic_category", "產業／評等等具有經濟意義，但必須採可追溯的歷史類別編碼。"
    if EVENT_TEXT.search(name) and row.get("category") in {"corporate_events", "company_profile", "fundamentals"}:
        return "economic_text", "事件或財務敘述有意義；尚需公布時鐘與文字／事件編碼。"
    # Count measures such as investors, number of traders and option strike are NOT identifiers.
    if lower in DIMENSIONS or lower.endswith(("_id", "_code", "_name")) or (
        re.search(r"(?:^|_)date(?:$|_)|(?:^|_)period(?:$|_)|_at_utc$", lower)
    ) or re.fullmatch(r".*(?:日期|年月|年度|季別|代號|編號|名稱|姓名|幣別|市場別|持股分級)|(?:單位|計價單位)", name):
        return "dimension_key", "保留實體、報告期間、單位或版本軸；不能直接視為連續 alpha，衍生編碼另驗。"
    if row.get("tej_role") == "date_or_availability_metadata":
        return "dimension_key", "TEJ 的日期／可用性軸；來源 query period 不是公告日期。"
    if row.get("tej_role") == "categorical_or_context_verify_type":
        return "economic_category", "TEJ 類別／脈絡欄；需核對型別、歷史變更及合理編碼。"
    if row.get("category") == "unclassified" and not re.search(
        r"value|price|amount|count|ratio|rate|volume|shares|investors|traders|金額|數|率|量|價", name, re.I
    ):
        return "unknown_definition", "目前欄名／家族不足以給出經濟定義；不把未知視為無意義或資料損壞。"
    return "economic_measure", "來源家族與科目名對應經濟數量／比率／旗標；型別、單位與時鐘仍需各自驗收。"


def classify(row: dict, admission: dict | None = None, selected: dict | None = None) -> dict:
    result = dict(row)
    result["category"] = economic_category(result)
    result["category_zh"] = CATEGORY_LABELS[result["category"]]
    role, reason = semantic_role(result)
    result.update(semantic_role=role, semantic_reason=reason)
    text = (result.get("dataset_id", "") + " " + field_label(result)).casefold()
    if housing_reason(result) and result["category"] != "fundamentals":
        scope = "housing_excluded"
    elif re.search(r"warrant|認購權證|認售權證|權證", text):
        scope = "warrant_excluded"
    elif role not in MEANINGFUL_ROLES:
        scope = "not_model_signal"
    elif result.get("provider") in {"bea", "census", "FRED", "CFTC"} or result["category"] in {"cross_market", "crypto", "macro", "fx_rates"}:
        scope = "market_context"
    else:
        scope = "tw_equity"
    result["scope"] = scope
    result["meaningful_in_selected_scope"] = role in MEANINGFUL_ROLES and scope not in {"housing_excluded", "warrant_excluded"}
    result["subcategory"] = financial_subcategory(result)
    admission = admission or {}
    selected = selected or {}
    result["prior_admission_decision"] = admission.get("decision", result.get("admission", "not_in_previous_catalog"))
    result["prior_admission_reason"] = admission.get("reason", result.get("reason", ""))
    if result.get("provider") == "TEJ":
        cells = result.get("tej_exported_non_null_cells")
        state = "tej_values_acquired_unwired" if cells and int(cells) > 0 else "tej_catalog_only_or_not_yet_acquired"
    elif truth(admission.get("admitted_as_model_value")):
        state = "research_value_wired"
    elif admission.get("decision") == "excluded_no_variable_observation":
        state = "no_variation_in_selected_view"
    elif result.get("admission") in {"missing_values", "upstream_all_null"}:
        state = "no_observed_value_in_selected_source"
    elif result.get("admission") in {"incomplete_source", "legacy_derived_table_needs_scoped_rebuild"}:
        state = "source_quality_review"
    elif role == "unknown_definition":
        state = "needs_semantic_definition"
    elif result.get("numeric_observations") == 0 and role in MEANINGFUL_ROLES:
        state = "needs_text_or_parse_adapter"
    else:
        state = "needs_adapter_clock_units" if result["meaningful_in_selected_scope"] else "not_selected_input"
    result["training_state"] = state
    result["selected_model_value"] = admission.get("selected_canonical_feature", "")
    result["update_frequency"] = selected.get("update_frequency", admission.get("update_frequency", result.get("update_frequency", "unknown"))) or "unknown"
    result["frequency_basis"] = result.get("frequency_basis") or (
        "registered_research_rule_not_original_release_proof" if selected else "source_family_or_native_period_label_not_release_proof"
    )
    for name in ("carry_days", "clock", "publication_time_estimated", "observations", "coverage_basis",
                 "available_stock_decision_cells", "decision_stock_rows_denominator",
                 "availability_ratio_over_all_stock_rows", "quality_barrier_observation_rows", "unmapped_release_periods"):
        result[name] = selected.get(name, admission.get(name, result.get(name, "")))
    if result.get("provider") in {"FinLab", "FinMind"}:
        result["rights"] = "僅本人 vastai1T 私人非商業研究已獲使用者確認；來源限制保留，不公開、不含帳密。"
    elif result.get("provider") == "TEJ":
        result["rights"] = "本機已授權取得的目錄／匯出證據；不代表全部表可查或可跨主機發布；raw_source_publish=false。"
    else:
        result["rights"] = result.get("redistribution") or "沿用來源權限；公開可見不等於所有歷史／再發布／訓練權限已逐項驗證。"
    result["next_action_detail"] = next_action(result)
    return result


def next_action(row: dict) -> str:
    if row["scope"] in {"housing_excluded", "warrant_excluded"}:
        return "排除目前當沖候選集合；原始資料與此明細保留，不執行刪除。"
    role, state = row["semantic_role"], row["training_state"]
    if role == "provenance":
        return "僅留來源收據與追溯關係，不當模型通道。"
    if role == "dimension_key":
        return "保留作 join、實體、時間、單位或版本鍵；需要訊號編碼時另定義因果衍生式。"
    if role == "execution_or_label":
        return "保持在執行／標籤層，核對決策時間，避免未來資訊混入輸入。"
    if role == "measure_container":
        return "以實際 concept/type/series/metric 展開；未展開部分列為明示缺口，不能以一個 value 冒充全部特徵。"
    if role == "unknown_definition":
        return "核對官方科目／原生型別／parse 狀態與維度；保留在完整待辨識清冊，不當作已知數值，也不列為壞資料。"
    if state == "research_value_wired":
        return "沿已固定研究時鐘使用 value/available/age/update；正式 data gate、容量與 DDP 尚須獨立通過。"
    if state == "no_variation_in_selected_view":
        return "原始科目保留；不加到該期間張量，換研究期間時重查變異，不宣稱概念無意義。"
    if state == "tej_catalog_only_or_not_yet_acquired":
        return "沿 TEJ 既有 owner 取得實際授權非空值；驗單位、請求網格／原生日期與公布版本後接線。"
    if role in {"economic_category", "economic_text"}:
        return "保留原值，補歷史狀態／事件的 available_at 及可解釋編碼，不用任意整數或任意補零。"
    return "對齊 issuer/instrument、原生期間、單位、公布／版本時鐘；保留缺值及合法稀疏，驗重疊與衝突再接入研究。"


def load_tej_snapshot(root: Path, inventory: Path) -> tuple[list[dict], dict]:
    """One bounded read transaction; never call the writer-side connect helper."""
    definitions = {r["feature_id"]: r for r in read_csv(inventory / "all_fields.csv")}
    path = root.resolve() / "queue.sqlite3"
    con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        con.execute("BEGIN")
        rows = [dict(r) for r in con.execute(
            "SELECT f.*,t.name AS table_name,t.smart_id,t.category AS tej_category,t.frequency,"
            "t.query_type,t.state AS table_state,t.last_error_code,t.schema_sha256,"
            "t.first_available_query_period,t.last_available_query_period,t.axis_profile_basis "
            "FROM features f JOIN tables t USING(table_id) ORDER BY t.table_id,f.field_index"
        )]
        meta = {r["key"]: r["value"] for r in con.execute("SELECT key,value FROM meta")}
    finally:
        con.close()
    output = []
    for row in rows:
        definition = definitions.get(row["feature_id"])
        if not definition or definition["table_id"] != row["table_id"] or definition["field"] != row["name"]:
            raise ValueError("TEJ live feature identity no longer matches supplied catalog")
        output.append({
            "catalog_id": "tej:" + row["feature_id"], "provider": "TEJ",
            "dataset_id": "tej:" + row["table_id"] + ":" + row["table_name"], "field": row["name"],
            "tej_table_id": row["table_id"], "tej_smart_id": row["smart_id"], "tej_category": row["tej_category"],
            "tej_role": row["role"], "tej_table_state": row["table_state"],
            "tej_error": row["last_error_code"], "tej_exported_non_null_cells": row["exported_non_null_cells"],
            "non_null_count": row["exported_non_null_cells"], "source_first": row["first_query_period"],
            "source_last": row["last_query_period"], "source_unit": definition["source_unit"],
            "unit_normalization": definition["normalization"], "reported_unit": row["unit"],
            "rows": "", "bounds_basis": "requested_smart_wizard_query_grid_not_native_observation_or_publication_dates",
            "count_basis": "cumulative_exported_non_null_cells_not_distinct_economic_keys_or_full_history",
            "source_path": str(path), "receipt_path": str(inventory / "all_fields.csv"),
            "schema_verified": "catalog_schema_identity_matched_not_full_value_type_validation",
            "sha256_verified": False, "publication_clock": "not_verified_by_export_grid",
            "value_vintage": "vendor_display_strings_current_provider_version_not_original_release_proof",
            "update_frequency": row["frequency"], "frequency_basis": definition["cadence_basis"],
            "context_review": definition["context_check"], "available_types": definition["available_types"],
            "table_schema_sha256": row["schema_sha256"], "local_match_ids": definition["local_match_ids"],
            "same_family_cadence_match_ids": definition["same_family_cadence_match_ids"],
        })
    return output, {"read_at_utc": datetime.now(UTC).isoformat(), "mode": "sqlite_read_transaction",
                    "database": str(path), "catalog_sha256": meta.get("catalog_sha256"),
                    "cutoff_requested_in_queue": meta.get("cutoff"), "fields": len(output),
                    "tables": len({r["tej_table_id"] for r in output}),
                    "catalog_file_sha256": digest(inventory / "all_fields.csv")}


def native_frequency(labels: set[str]) -> str:
    kinds = set()
    for label in labels:
        if re.fullmatch(r"\d{4}[- ]?Q[1-4]|\d{4}-Q[1-4]", label, re.I):
            kinds.add("quarterly")
        elif re.fullmatch(r"\d{4}", label):
            kinds.add("yearly")
        elif re.fullmatch(r"\d{4}[- ](?:M\d{1,2}|\d{2})", label):
            kinds.add("monthly")
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", label):
            kinds.add("dated_observations_native_cadence_unverified")
        else:
            kinds.add("unknown")
    return "|".join(sorted(kinds)) or "unknown"


def aggregate_long_files(jobs: list[dict], batch_size: int = 131072) -> tuple[list[dict], list[dict]]:
    """O(rows) column projections, bounded batches; no imputation or provider calls.

    Only small grouped sufficient statistics leave each batch. Values and
    arbitrary entity IDs are not copied into the human-facing report.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    groups, evidence = {}, []
    for index, job in enumerate(jobs):
        path = Path(job["source_path"]).resolve(strict=True)
        signature = path.stat()
        before = (signature.st_dev, signature.st_ino, signature.st_size, signature.st_mtime_ns)
        sha = digest(path)
        expected = job.get("declared_sha256")
        if expected and sha != expected:
            raise ValueError(f"expanded source SHA does not match selected receipt: {path}")
        file = pq.ParquetFile(path)
        dims = job["dimensions"]
        measure, date = job["value_column"], job["date_column"]
        columns = list(dict.fromkeys([*dims, measure, date, *job.get("extra_columns", [])]))
        if not set(columns) <= set(file.schema_arrow.names):
            raise ValueError(f"long-table dimensions changed: {path}")
        file_groups = set()
        for batch in file.iter_batches(columns=columns, batch_size=batch_size, use_threads=False):
            table = pa.Table.from_batches([batch])
            values = table[measure]
            count = pc.is_valid(values).cast(pa.int64())
            if pa.types.is_floating(values.type) or pa.types.is_integer(values.type):
                array = values.to_numpy(zero_copy_only=False)
                finite = np.isfinite(array)
                zero = finite & (array == 0)
                numeric = pa.array(finite.astype(np.int64()))
                zeros = pa.array(zero.astype(np.int64()))
            elif job.get("expansion_kind") == "mops_xbrl_concept":
                numeric = pc.fill_null(pc.equal(table["value_parse_status"], "parsed"), False).cast(pa.int64())
                zeros = pc.fill_null(pc.match_substring_regex(values, r"^[+-]?0+(?:\.0+)?(?:[eE][+-]?\d+)?$"), False).cast(pa.int64())
            else:
                strings = pc.replace_substring(values.cast(pa.string()), ",", "")
                valid_number = pc.match_substring_regex(strings, r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
                numeric = pc.fill_null(valid_number, False).cast(pa.int64())
                zeros = pc.fill_null(pc.match_substring_regex(strings, r"^[+-]?0+(?:\.0+)?(?:[eE][+-]?\d+)?$"), False).cast(pa.int64())
            table = table.append_column("_source_rows", pa.array(np.ones(batch.num_rows, dtype=np.int64)))
            table = table.append_column("_observed", count).append_column("_numeric", numeric).append_column("_zero", zeros)
            aggregations = [("_source_rows", "sum"), ("_observed", "sum"), ("_numeric", "sum"), ("_zero", "sum"), (date, "min"), (date, "max")]
            aggregate = table.group_by(dims, use_threads=False).aggregate(aggregations).to_pylist()
            for item in aggregate:
                axis = tuple(item[d] for d in dims)
                identity = (job["provider"], job["dataset_id"], job["expansion_kind"], axis)
                group = groups.setdefault(identity, {
                    "job": job, "axis": axis, "rows": 0, "observed": 0, "numeric": 0, "zeros": 0,
                    "first": None, "last": None, "files": 0,
                })
                group["rows"] += item["_source_rows_sum"]
                group["observed"] += item["_observed_sum"]
                group["numeric"] += item["_numeric_sum"]
                group["zeros"] += item["_zero_sum"]
                lower, upper = item[date + "_min"], item[date + "_max"]
                if lower is not None:
                    lower = str(lower)
                    group["first"] = min(group["first"], lower) if group["first"] else lower
                if upper is not None:
                    upper = str(upper)
                    group["last"] = max(group["last"], upper) if group["last"] else upper
                file_groups.add(identity)
        for identity in file_groups:
            groups[identity]["files"] += 1
        after_stat = path.stat()
        if before != (after_stat.st_dev, after_stat.st_ino, after_stat.st_size, after_stat.st_mtime_ns):
            raise ValueError(f"expanded source changed during scan; do not certify mixed versions: {path}")
        evidence.append({"path": str(path), "sha256": sha, "expected_sha256_matched": bool(expected),
                         "bytes": signature.st_size, "source_rows": file.metadata.num_rows,
                         "projected_columns": columns, "concept_groups": len(file_groups),
                         "dataset_id": job["dataset_id"], "expansion_kind": job["expansion_kind"],
                         "source_version_stable": True})
        if index % 20 == 0 or index + 1 == len(jobs):
            print(f"semantic source scan {index + 1}/{len(jobs)}; groups={len(groups)}; {job['dataset_id']}", flush=True)
    output = []
    for identity, group in groups.items():
        job, axis = group["job"], group["axis"]
        dimensions = dict(zip(job["dimensions"], axis, strict=True))
        if job["provider"] == "census":
            dimensions = {k.casefold(): v for k, v in dimensions.items()}
        concept = dimensions.get("concept") or dimensions.get("origin_name") or dimensions.get("LineDescription") or dimensions.get("metric") or dimensions.get("series_id")
        code = dimensions.get("type") or dimensions.get("SeriesCode") or dimensions.get("data_type_code")
        field = str(concept or code or axis)
        semantic_hash = hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:24]
        label_set = {s for s in (group["first"], group["last"]) if s}
        unit = dimensions.get("unit") or dimensions.get("unit_ref") or job.get("unit", "未由來源明示；不能猜測元／千元／%")
        if job["provider"] == "bea":
            unit = f"CL_UNIT={dimensions.get('CL_UNIT')}; METRIC_NAME={dimensions.get('METRIC_NAME')}; UNIT_MULT={dimensions.get('UNIT_MULT')}（原尺度保留）"
        elif job["provider"] == "census":
            unit = "依 program/data_type_code 原單位；代码未對照時不得當美元／百分比"
        output.append({
            "catalog_id": "expanded:" + semantic_hash, "provider": job["provider"],
            "dataset_id": job["dataset_id"], "field": field, "concept_code": code,
            "concept_dimensions": dimensions, "expansion_kind": job["expansion_kind"],
            "parent_catalog_ids": job.get("parent_catalog_ids", []), "rows": group["rows"],
            "non_null_count": group["observed"], "numeric_observations": group["numeric"],
            "observed_zero_count": group["zeros"], "files": group["files"],
            "source_first": group["first"], "source_last": group["last"],
            "bounds_basis": job.get("bounds_basis", "per_concept_native_report_or_observation_period_not_publication_date"),
            "count_basis": "all_rows_in_selected_current_source_versions; not_distinct_issuer_period_keys_or_expected_population",
            "schema_verified": True, "sha256_verified": True, "source_unit": unit,
            "source_path": job["source_path"], "receipt_path": job.get("receipt_path", ""),
            "types": "numeric_string_preserved" if job["provider"] in {"bea", "census", "MOPS / 公開資訊觀測站"} else "source_value_dtype",
            "publication_clock": job.get("publication_clock", "source_period_not_verified_publication_time"),
            "value_vintage": job.get("value_vintage", "current_stored_source_version_not_original_release_proof"),
            "update_frequency": "quarterly" if job["expansion_kind"] in {"finmind_financial_code", "mops_xbrl_concept"} else native_frequency(label_set),
            "frequency_basis": "native_source_period_or_financial_family; release_schedule_not_certified",
        })
    return sorted(output, key=lambda r: r["catalog_id"]), evidence


def expansion_jobs(catalog: Path, repo: Path, candidates: list[dict]) -> list[dict]:
    jobs = []
    parents = defaultdict(list)
    for row in candidates:
        parents[row["dataset_id"]].append(row["catalog_id"])
    # Complete current-receipt histories of the three FinMind statement families.
    with (catalog / "finmind_source_files.csv").open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["dataset"] not in FINANCIAL_DATASETS:
                continue
            dataset = row["lane"] + ":" + row["dataset"]
            jobs.append({**row, "provider": "FinMind", "dataset_id": dataset,
                         "dimensions": ["type", "origin_name"], "value_column": "value", "date_column": "date",
                         "expansion_kind": "finmind_financial_code", "parent_catalog_ids": parents[dataset]})
    with (catalog / "economic_source_files.csv").open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["provider"] not in {"bea", "census"}:
                continue
            dataset = "public_economic:" + row["dataset"]
            if row["provider"] == "bea":
                dimensions = ["SeriesCode", "LineNumber", "LineDescription", "CL_UNIT", "METRIC_NAME", "UNIT_MULT"]
                measure, date, kind = "DataValue", "TimePeriod", "bea_series"
            else:
                dimensions = ["category_code", "data_type_code", "geo_level_code", "seasonally_adj", "error_data"]
                measure, date, kind = "cell_value", "time", "census_series"
                import pyarrow.parquet as pq
                names = {name.casefold(): name for name in pq.ParquetFile(row["source_path"]).schema_arrow.names}
                dimensions = [names[name] for name in dimensions]
                measure = names[measure]
            jobs.append({**row, "dataset_id": dataset, "dimensions": dimensions, "value_column": measure,
                         "date_column": date, "expansion_kind": kind, "parent_catalog_ids": parents[dataset]})
    # Raw official taxonomy concepts, including older concepts not selected in the research projection.
    for path in sorted((repo / "data_tw_public/mops_xbrl/normalized").glob("*/*/*/facts.parquet")):
        dataset = "tw-public:mops_xbrl_quarterly"
        jobs.append({"provider": "MOPS / 公開資訊觀測站", "dataset_id": dataset, "source_path": str(path),
                     "dimensions": ["concept", "unit_ref"], "value_column": "decimal_value",
                     "date_column": "report_period_end", "extra_columns": ["value_parse_status"],
                     "expansion_kind": "mops_xbrl_concept", "parent_catalog_ids": parents[dataset],
                     "bounds_basis": "raw_report_period_end_not_context_period_or_filing_timestamp"})
    for dataset, path, dimensions, measure, date, kind in (
        ("physical:fred:observations", "data_fred_crypto_macro/observations.parquet", ["series_id"], "value", "observation_date", "fred_series"),
        ("physical:free-public:observations", "data_free_public/observations.parquet", ["source", "dataset", "metric", "unit"], "value_float", "event_ts_utc", "free_public_metric"),
        ("physical:etf:issuer-metrics", "data_crypto_etf/normalized/issuer_daily_fund_metrics.parquet", ["metric", "unit"], "value", "event_date", "etf_metric"),
    ):
        if (repo / path).is_file():
            jobs.append({"provider": "FRED" if kind == "fred_series" else "公開資料" if kind == "free_public_metric" else "SEC EDGAR / ETF 發行商",
                         "dataset_id": dataset, "source_path": str(repo / path), "dimensions": dimensions,
                         "value_column": measure, "date_column": date, "expansion_kind": kind,
                         "parent_catalog_ids": parents[dataset],
                         "publication_clock": "原表有 available_at／PIT state；此清冊未逐版本認證時鐘"})
    return jobs


def finlab_table_fields(candidates: list[dict]) -> tuple[list[dict], list[dict]]:
    """Expand named event-table fields, never instrument-code columns in wide measures."""
    import pyarrow.parquet as pq
    from stockagent.live.data_monitor_inventory import parquet_footer_stats

    output, evidence = [], []
    for parent in candidates:
        if parent.get("provider") != "FinLab" or ":" in parent["dataset_id"]:
            continue
        path = Path(parent["source_path"])
        stats = parquet_footer_stats(path)
        if not stats:
            raise ValueError(f"FinLab table schema unavailable: {path}")
        # source_index is the native event/date index, not a distinct economic measure.
        for (field, dtype), count in zip(stats["fields"], stats["non_null"], strict=True):
            identity = parent["catalog_id"] + "::" + field
            output.append({**parent, "catalog_id": identity, "field": field, "types": [dtype],
                           "expansion_kind": "finlab_table_field", "parent_catalog_ids": [parent["catalog_id"]],
                           "non_null_count": count, "rows": stats["count"],
                           "count_basis": "Parquet_footer_field_statistics_not_full_value_domain_validation",
                           "schema_verified": True, "sha256_verified": False})
        evidence.append({"path": str(path), "fields": len(stats["fields"]), "rows": stats["count"],
                         "mode": "footer_only; not full SHA or type-domain validation"})
    return output, evidence


def label_unit(field: str) -> str:
    """Report only explicit scales; an old registry's 'TWD' may omit x1000."""
    for pattern, unit in (
        (r"1000\s*(?:ntd|twd)|(?:ntd|twd)\s*1000|千元", "thousand_TWD（×1000 才是 TWD）"),
        (r"(?:ntd|twd)\s*(?:mn|million)|百萬元", "million_TWD（×1000000 才是 TWD）"),
        (r"1000\s*s\b|千股", "thousand_shares（×1000 才是股）"),
        (r"%|百分比|百分率", "percent（百分點原尺度；不要自動再 ×100）"),
        (r"ntd|twd|\(元\)|（元）", "TWD（是否每股／總額須依原科目）"),
    ):
        if re.search(pattern, field, re.I):
            return unit
    return "未由欄名明示；保留來源單位，轉換前核對定義"


def enrich_report_rows(rows: list[dict]) -> None:
    same_label = defaultdict(list)
    for row in rows:
        label = unicodedata.normalize("NFKC", field_label(row)).strip().casefold()
        if row["meaningful_in_selected_scope"]:
            same_label[(row["category"], label)].append(row)
        row["source_unit_label_evidence"] = label_unit(field_label(row))
        row["definition_status"] = (
            "code_only_needs_source_dictionary" if row.get("expansion_kind") == "census_series"
            else "named_source_concept_context_and_formula_still_require_validation"
        )
        lower, upper = str(row.get("source_first") or ""), str(row.get("source_last") or "")
        if row["update_frequency"] == "unknown":
            inferred = native_frequency({s for s in (lower, upper) if s})
            if inferred in {"quarterly", "yearly", "monthly"}:
                row["update_frequency"] = inferred
                row["frequency_basis"] = "source_period_label_inferred_not_publication_schedule"
        row["bounds_review"] = "未提供逐欄有效日期界線" if not lower and not upper else "原生觀測／報告／請求期間；不是公布或盤前可知日期"
        for value in (lower, upper):
            if re.match(r"^\d{4}-\d{2}-\d{2}(?:$|[ T])", value):
                try:
                    datetime.strptime(value[:10], "%Y-%m-%d")
                except ValueError:
                    row["bounds_review"] = "存在非法日期標記；只保留原標記，不作歷史有效／公布界線"
        row["missingness_interpretation"] = (
            "未建 expected issuer×native-period 分母；低頻、上市前、科目不適用、事件稀疏、公式域與未下載要分開，不能把 NULL 統稱來源損壞。"
        )
        if row.get("numeric_observations") is not None:
            row["missingness_interpretation"] += " 本次只統計選定原檔的非空／數字／有效零，並非對應市場的完整率。"
        if row.get("source_value_columns"):
            row["missingness_interpretation"] += " FinLab 非空數為寬表儲存 cells，列數是日期／事件列，不可相除當缺值率。"
    for (category, label), values in same_label.items():
        group_id = hashlib.sha256((category + "\0" + label).encode()).hexdigest()[:20]
        for row in values:
            row["same_label_group"] = group_id
            row["same_label_occurrences"] = len(values)
            row["equivalence_status"] = "同名只作核對線索；合併／個別、累計／單季、幣別、調整與版本未一致前不合併或刪除。"
    for row in rows:
        row.setdefault("same_label_group", "")
        row.setdefault("same_label_occurrences", 0)
        row.setdefault("equivalence_status", "非訊號或語意尚待核對，不作相等宣告。")


def md(value) -> str:
    if value is None or value == "":
        return "未提供／未驗證"
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = str(value).replace("\r", " ").replace("\n", " ")
    for char in ("\\", "`", "*", "_", "[", "]", "<", ">", "|"):
        text = text.replace(char, "\\" + char)
    return text


def anchor(catalog_id: str) -> str:
    return "f-" + hashlib.sha256(catalog_id.encode()).hexdigest()[:24]


def count_text(value) -> str:
    if value is None or value == "":
        return "未知（不是 0）"
    return f"{int(value):,}"


def feature_detail(row: dict) -> list[str]:
    source_first, source_last = row.get("source_first"), row.get("source_last")
    content = [f'<a id="{anchor(row["catalog_id"])}"></a>', "",
               f'### {md(field_label(row))}', "", "| 項目 | 詳細狀況 |", "| --- | --- |"]
    properties = [
        ("清冊 ID", row["catalog_id"]),
        ("來源／表／原欄名", f'{row["provider"]}；{row["dataset_id"]}；{row["field"]}'),
        ("分類／子類／經濟角色", f'{row["category_zh"]}／{row["subcategory"]}；{ROLE_LABELS[row["semantic_role"]]}'),
        ("定義與意義", row["semantic_reason"] + " 科目原名保留；經濟可解釋不等於已證明有預測力。"),
        ("台股使用範圍", SCOPE_LABELS[row["scope"]]),
        ("原生概念與軸", row.get("concept_dimensions") or row.get("available_types") or "原來源實體／期間軸保留；未展開市場內每個股票為不同特徵"),
        ("原生首末標記", f'{source_first or "未知"} → {source_last or "未知"}；{row.get("bounds_basis", "未提供")}'),
        ("日期審查", row["bounds_review"]),
        ("儲存列／非空數／分檔", f'{count_text(row.get("rows"))} 列；{count_text(row.get("non_null_count"))} 非空；{count_text(row.get("files"))} 檔；{row.get("count_basis") or "原清冊來源欄統計，不是完整市場覆蓋率"}'),
        ("數字／有效零／型別", f'{count_text(row.get("numeric_observations"))} 數字；{count_text(row.get("observed_zero_count"))} 有效零；{row.get("types") or "原來源型別尚未逐值驗證"}'),
        ("缺值判讀", row["missingness_interpretation"]),
        ("單位與尺度", f'{row.get("source_unit") or "來源未在此清冊明示"}；欄名證據：{row["source_unit_label_evidence"]}；舊註冊單位：{row.get("reported_unit") or "未提供"}'),
        ("更新頻率／證據", f'{row["update_frequency"]}；{row["frequency_basis"]}；carry_days={row.get("carry_days") or "未登記，不擅自填值"}'),
        ("公布與版本", f'{row.get("clock") or row.get("publication_clock") or "未驗證"}；publication_time_estimated={row.get("publication_time_estimated", "未提供")}；{row.get("value_vintage") or "原始發布版本未驗證"}'),
        ("來源檢查界線", f'schema={row.get("schema_verified", "未驗證")}；內容 SHA={row.get("sha256_verified", "未驗證")}；footer／SHA 不代表歷史 PIT 或全歷史完整'),
        ("目前訓練接線", STATE_LABELS[row["training_state"]] + f'；對應值={row.get("selected_model_value") or "未接入"}'),
        ("原閘門判定（不覆寫）", f'{row["prior_admission_decision"]}；{row["prior_admission_reason"]}'),
        ("同名群組與重複", f'{row["same_label_group"] or "無"}；同名 {row["same_label_occurrences"]} 筆；{row["equivalence_status"]}'),
        ("權限與發布", row["rights"]),
        ("下一步", row["next_action_detail"]),
        ("來源與收據位置", f'{row.get("source_path") or "未提供"}；{row.get("receipt_path") or "未提供"}'),
    ]
    if row.get("expansion_kind"):
        properties.append(("概念展開與父清冊", f'{row["expansion_kind"]}；{row.get("parent_catalog_ids", [])}'))
    if row.get("provider") == "TEJ":
        properties.append(("TEJ 即時取得證據", f'table_state={row.get("tej_table_state")}；error={row.get("tej_error") or "未列出錯誤（不是歷史完整證明）"}；{row.get("context_review")}'))
    if row.get("availability_ratio_over_all_stock_rows") not in (None, ""):
        ratio = float(row["availability_ratio_over_all_stock_rows"])
        properties.append(("已接線研究表的覆蓋", f'{count_text(row["available_stock_decision_cells"])} / {count_text(row["decision_stock_rows_denominator"])} = {ratio:.4%}；{row.get("coverage_basis")}；不是適用 issuer 的必要觀測完整率'))
        properties.append(("已知品質 barrier／未映射期間", f'{row.get("quality_barrier_observation_rows", "未知")}／{row.get("unmapped_release_periods", "未知")}；僅限已接線研究規則'))
    content.extend(f"| {label} | {md(value)} |" for label, value in properties)
    return content + [""]


def write_feature_pages(rows: list[dict], root: Path, page_size: int) -> list[dict]:
    pages, by_category = [], defaultdict(list)
    for row in rows:
        by_category[row["category"]].append(row)
    categories = list(CATEGORY_LABELS)
    for category in categories:
        members = sorted(by_category.get(category, []), key=lambda r: (r["subcategory"], r["provider"], r["dataset_id"], r["field"], r["catalog_id"]))
        if not members:
            continue
        directory = root / category
        directory.mkdir(parents=True)
        index = [f"# {CATEGORY_LABELS[category]}：完整清冊", "",
                 f"共 {len(members):,} 筆來源表示，全部列出、不作 Top-K 篩選。經濟角色和訓練接線是兩個獨立欄位。", "",
                 "[回總報告](../README.md)", "", "| 分冊 | 項數 | 主要來源與內容 |", "| --- | ---: | --- |"]
        for offset in range(0, len(members), page_size):
            part = members[offset:offset + page_size]
            name = f"features-{offset // page_size + 1:03d}.md"
            filename = category + "/" + name
            text = [f"# {CATEGORY_LABELS[category]}：第 {offset // page_size + 1} 冊", "",
                    f"本冊 {len(part):,} 筆；詳細表保留所有原欄、限制與未知狀態。", "",
                    "[分類索引](index.md) · [總報告](../README.md)", "",
                    "## 本冊完整項目", "", "| 特徵／原科目 | 來源 | 經濟角色 | 目前接線 |", "| --- | --- | --- | --- |"]
            for row in part:
                row["report_file"] = filename
                row["report_anchor"] = anchor(row["catalog_id"])
                text.append(f'| [{md(field_label(row))}](#{row["report_anchor"]}) | {md(row["provider"])} | {ROLE_LABELS[row["semantic_role"]]} | {STATE_LABELS[row["training_state"]]} |')
            text += ["", "## 每項詳細狀況", ""]
            for row in part:
                text.extend(feature_detail(row))
            atomic_write_text(directory / name, "\n".join(text) + "\n")
            providers = "、".join(sorted({r["provider"] for r in part}))
            index.append(f"| [{name}]({name}) | {len(part):,} | {md(providers)}；{md(part[0]['subcategory'])} → {md(part[-1]['subcategory'])} |")
            pages.append({"file": filename, "category": category, "features": len(part), "sha256": digest(directory / name)})
        atomic_write_text(directory / "index.md", "\n".join(index) + "\n")
    return pages


def write_source_pages(sources: list[dict], root: Path, page_size: int) -> list[str]:
    files = []
    directory = root / "source_status"
    directory.mkdir(parents=True)
    index = ["# 全部註冊來源狀況", "", f"原盤點共有 {len(sources):,} 個註冊項目；包含群組、別名、credentials／服務宣告，不是独立 feature 或可相加的資料量。", "",
             "[回總報告](../README.md)", ""]
    for offset in range(0, len(sources), page_size):
        part = sources[offset:offset + page_size]
        name = f"sources-{offset // page_size + 1:03d}.md"
        content = ["# 註冊來源逐項狀況", "", "[來源索引](index.md) · [總報告](../README.md)", ""]
        for row in part:
            content += [f'## {md(row.get("title") or row.get("id"))}', "", "| 項目 | 狀況 |", "| --- | --- |"]
            for label, keys in (
                ("身分／來源／範圍", ["id", "provider", "scope", "market_category"]),
                ("服務與資料状态", ["status", "status_label", "operation_state", "acquisition_blocker"]),
                ("原生首末與筆數證據", ["first_observed", "last_observed", "record_count", "record_count_basis", "record_evidence"]),
                ("更新排程（不是原生頻率）", ["cadence", "expected_release_at_utc", "expected_release_basis", "next_run_at_utc", "next_run_basis"]),
                ("範圍覆蓋（不是 feature 完整率）", ["coverage_current", "coverage_total", "coverage_unit"]),
                ("時鐘／權限／快照", ["publication_clock_kind", "publishable", "snapshot_at_utc", "receipt_evidence"]),
                ("既有 owner", ["update_owner", "service_keys"]),
            ):
                values = "; ".join(f"{key}={row.get(key) or '未提供'}" for key in keys)
                content.append(f"| {label} | {md(values)} |")
            content.append("")
        atomic_write_text(directory / name, "\n".join(content) + "\n")
        index.append(f"- [{name}]({name})：{len(part)} 項。")
        files.append("source_status/" + name)
    atomic_write_text(directory / "index.md", "\n".join(index) + "\n")
    return files


def report_summary(rows: list[dict], base_count: int, tej_count: int, source_count: int) -> dict:
    meaningful = [r for r in rows if r["meaningful_in_selected_scope"]]
    financial = [r for r in meaningful if r["category"] == "fundamentals"]
    wired = [r for r in rows if r["training_state"] == "research_value_wired"]
    names = {r["selected_model_value"] for r in wired}
    if len(names) != len(wired) or "" in names:
        raise ValueError("wired value identity must be unique; do not double count source projections")
    return {
        "contract": CONTRACT, "feature_entries": len(rows), "base_catalog_entries": base_count,
        "tej_catalog_entries": tej_count, "expanded_entries": len(rows) - base_count - tej_count,
        "registered_source_rows": source_count, "meaningful_candidate_representations": len(meaningful),
        "meaningful_measure_representations": sum(r["semantic_role"] == "economic_measure" for r in meaningful),
        "meaningful_category_or_text_representations": sum(r["semantic_role"] != "economic_measure" for r in meaningful),
        "financial_candidate_representations": len(financial),
        "financial_distinct_exact_field_labels": len({field_label(r).strip() for r in financial}),
        "meaningful_same_label_groups": len({r["same_label_group"] for r in meaningful}),
        "wired_research_values": len(names),
        "by_category": dict(sorted(Counter(r["category"] for r in rows).items())),
        "meaningful_by_category": dict(sorted(Counter(r["category"] for r in meaningful).items())),
        "financial_by_subcategory": dict(sorted(Counter(r["subcategory"] for r in financial).items())),
        "by_provider": dict(sorted(Counter(r["provider"] for r in rows).items())),
        "meaningful_by_provider": dict(sorted(Counter(r["provider"] for r in meaningful).items())),
        "by_semantic_role": dict(sorted(Counter(r["semantic_role"] for r in rows).items())),
        "by_training_state": dict(sorted(Counter(r["training_state"] for r in rows).items())),
        "by_scope": dict(sorted(Counter(r["scope"] for r in rows).items())),
        "by_expansion_kind": dict(sorted(Counter(r.get("expansion_kind") for r in rows if r.get("expansion_kind")).items())),
        "strict_historical_PIT_all_features_verified": False, "formal_training_ready": False,
        "independent_nonredundant_feature_count_verified": False, "predictive_value_measured": False,
        "report_complete_for_named_catalogs_and_expansions": True,
    }


def write_overview(summary: dict, root: Path, catalog_summary: dict, inputs: list[dict], pages: list[dict],
                   tej_proof: dict, unexpanded: list[dict]) -> None:
    s = summary
    text = ["# 有意義的財務與市場特徵確實超過幾千項，251 只是已接線的研究選集", "",
            "## 1. 執行結果", "",
            f'- 已逐項整理 **{s["feature_entries"]:,} 筆來源特徵／概念表示**，每一項都有 Markdown 詳細表。包含原清冊 {s["base_catalog_entries"]:,} 筆、TEJ {s["tej_catalog_entries"]:,} 筆，以及長表／事件表展開 {s["expanded_entries"]:,} 筆；沒有 Top-K 截斷。',
            f'- 排除目前指定的房屋／權證用途、鍵、來源管理資訊與執行／標籤容器後，有 **{s["meaningful_candidate_representations"]:,} 筆具經濟意義的候選表示**；其中數值／比率／旗標 {s["meaningful_measure_representations"]:,} 筆，類別／事件／文字 {s["meaningful_category_or_text_representations"]:,} 筆。這是「經濟可解釋候選」，不是型別、全部歷史、單位或因果時鐘都已通過的模型維度。',
            f'- 財報與營收類有 **{s["financial_candidate_representations"]:,} 筆候選表示、{s["financial_distinct_exact_field_labels"]:,} 個不同原始科目名稱**。同名、不同年度／合併層級／原生單位／稅則與版本不能直接合併；不同名也可能同概念，所以兩種數量都不等於最終獨立資訊量。',
            f'- 前一輪 **{s["wired_research_values"]} 個數值**只表示「目前已實作公布時鐘並建入私人混合頻率研究表」；四種 value／available／age／update 通道不能另算為同等數量的經濟概念。',
            "- 本次交付是完整語意清冊與狀況報告：沒有增加模型維度、修改訓練 allowlist、啟動 GPU、更新正式資料包、下載付費資料或改動服務。沒有重寫／刪除原始來源。", "",
            "## 2. 為什麼原先的數量偏小", "",
            f'- TEJ 先前只列在註冊來源列表，未逐欄加入 12,446 筆清冊。本次從原目錄與目前 SQLite 一次唯讀交易核對 **{tej_proof["tables"]} 張表、{tej_proof["fields"]:,} 個表格 × 欄位**；目錄可見與已下載值分開記錄。',
            "- FinMind 財報的 type／origin_name 才指向科目，value 是數值容器；BEA 的 SeriesCode／LineDescription、Census 的 item/industry/geo/seasonal 軸、MOPS 的 taxonomy QName 也不能僅用欄位數代表所有概念。本次保留原容器並另列實際觀測到的子概念，不雙算容器。",
            "- 尚需 adapter、時鐘或單位驗收，是工程／證據狀態，不是經濟無意義判決。反過來，某欄已是 Float64、非空或 HTTP 200，也不能證明可以直接訓練。",
            "- 財務表的 loan/equity/租賃等科目按財報來源家族分類；不因關鍵字 loan 就全部丟到融資券，也不把公司的房產／租賃會計科目等同住宅成交資料。僅更正報告分類；既有訓練排除規則未被繞過。", "",
            "## 3. 全部分類與詳細 Markdown", "",
            "點分類即可依序閱讀所有分冊；每冊有完整索引及每項詳細狀況，不只連到 CSV。", "",
            "| 分類 | 全部表示 | 有意義且在用途內 | 詳細清冊 |", "| --- | ---: | ---: | --- |"]
    for category in CATEGORY_LABELS:
        if category not in s["by_category"]:
            continue
        text.append(f'| {CATEGORY_LABELS[category]} | {s["by_category"][category]:,} | {s["meaningful_by_category"].get(category, 0):,} | [完整分冊]({category}/index.md) |')
    text += ["", "### 財務科目子類", "", "| 子類 | 用途內有意義候選表示 |", "| --- | ---: |"]
    text.extend(f"| {md(k)} | {v:,} |" for k, v in s["financial_by_subcategory"].items())
    text += ["", "## 4. 目前有哪些能用，還差什麼", "", "| 狀態 | 項數 | 解讀 |", "| --- | ---: | --- |"]
    for key, value in s["by_training_state"].items():
        text.append(f"| {md(key)} | {value:,} | {STATE_LABELS[key]} |")
    text += ["",
             "- **已接線研究表 ≠ 整個訓練已就緒。** 正式 data gate／容量／雙卡 DDP 仍是另一項驗收；本報告不升格聲稱已通過。",
             "- **TEJ 非空匯出數 ≠ 唯一有效觀測數。** 累積 cells、first/last query period 是請求網格／顯示匯出證據；不可當作原生事件、股票 × 季度完整率或歷史公告日期。沒有值的項目寫未知，不擅自寫 0。",
             "- **更新頻率 ≠ 公布日期 ≠ 服務檢查頻率。** 季報值可在已知公布後沿用；沿用不是製造新值。事件不能盲目 forward-fill。每項標示頻率證據和是否已登記 carry，不從今日更新時間倒灌歷史。",
             "- **缺值 ≠ 損壞。** 低頻、上市前／下市後、不適用科目、公式域、有效零、未下載／未映射、修訂與真壞檔各自判讀；沒建立預期 issuer×period 分母時不報虛假完整率。",
             "- **原始數值精度保留。** 不把百分比／報表金額套股票 tick，不假設數字字串是元；1000 NTD 明示千元時標出 ×1000，即使舊單位欄曾寫 TWD。",
             "- **授權不混淆。** FinLab／FinMind 維持本人 vastai1T 私人非商業用途；TEJ 可見目錄不是所有表查詢／跨主機發布許可；不公開受限資料或帳密。", "",
             "## 5. 盤點範圍與尚未拆開的部分", "",
             f'- 原清冊來自 {catalog_summary.get("started_at_utc")}～{catalog_summary.get("finished_at_utc")}；TEJ 唯讀快照 {tej_proof["read_at_utc"]}。下載服務仍運作，不是所有來源同時凍結。',
             f'- 原監控涵蓋 {catalog_summary["physical_field_rows"]:,} 個實體 field；footer/schema 覆蓋 {catalog_summary["monitor_schema_coverage"]["files_with_schema"]:,}/{catalog_summary["monitor_schema_coverage"]["files_total"]:,} 檔，仍為 partial。股票代碼寬表軸不是特徵；[實體欄位粒度](physical_grain.md)對每個 dataset 對帳。',
             f'- 全部 {s["registered_source_rows"]:,} 個註冊來源狀況已寫成 [Markdown 分冊](source_status/index.md)。群組／別名／credential 不得與來源子項重複加總。',
             "- FinMind 三種財報、BEA／Census、MOPS、FRED、免費公開 metric、ETF metric 展開按實際原檔投影讀取、逐檔內容 hash 和讀取穩定性核對。Hash證明檔案身分，不證明欄位語義或原始發布版。其他來源沿 footer／原收據界線，不冒稱全來源逐值驗收。",
             "- Census opaque code 保留完整軸並標示 code_only_needs_source_dictionary，不杜撰代碼定義、單位或公布日期。error_data=yes 屬另一統計量測軸，不當成壞檔或混進主估計。",
             "- MOPS 以原 taxonomy QName × unit 列概念，保留歷史命名空間；未把所有合併／維度／期間 context 統一成一個模型輸入。概念清冊不是已完成 economic-key 對齊。",
             f'- 下列 **{len(unexpanded)} 個容器／目錄項**仍須後續對齊，全部已列明細。不能宣稱窮盡 provider API 所有可能指標：', "",
             "| 清冊 ID | 原容器 | 已展開子項 | 待完成 |", "| --- | --- | ---: | --- |"]
    for row in unexpanded:
        text.append(f'| {md(row["catalog_id"])} | {md(row["dataset_id"] + " / " + row["field"])} | {row["expanded_children"]:,} | {md(row["remaining"])} |')
    text += ["", "## 6. 從原始觀測到當沖輸入", "",
             "原始科目／量測 → issuer/instrument/期間/單位/版本鍵 → 實際或已授權估計的 available_at → 09:00 決策網格 → value/available/age/update → 僅在 train years fit 的標準化 → 訓練。執行價、容量、標籤及費稅另走既有執行層。", "",
             "這個順序使低頻資料可以用，不需每天虛構財報。後續按可取得值、單位及時鐘證據分批接入；不按測試集報酬先挑贏家，不宣稱特徵多一定更賺，不靠刪難處理欄位製造假完整。", "",
             "## 7. 原始證據與重現", "", "| 證據 | 身分 |", "| --- | --- |"]
    text.extend(f'| {md(r["path"])} | SHA256 {r["sha256"]} |' for r in inputs)
    text += ["",
             "機器可核對附件：feature_catalog.jsonl（全部特徵與 Markdown 定位）、source_projection_evidence.json（每個展開原檔 SHA／範圍）、report_manifest.json（總數與分冊 hash）、report_acceptance.json（逐項覆蓋驗收）。附件不代替上述完整 Markdown 明細。", "",
             "```bash", "cd /root/stockAgent", "source scripts/runtime_env.sh",
             "run_fintech_python scripts/report_tw_day_trade_feature_catalog.py \\",
             "  --all-source-catalog artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/catalog \\",
             "  --admission-dir artifacts/data_quality/tw_daytrade_mixed_frequency_20261004/admission_v3 \\",
             "  --tej-root data_tej \\",
             "  --tej-inventory artifacts/data_quality/tej_smart_wizard_inventory_2026-10-02/plan_v3 \\",
             "  --output-dir docs/tw_daytrade_feature_catalog_NEW", "```", "",
             "必須使用新輸出目錄；只產生清冊和報告，不下載、不修改 queue、不訓練。", "",
             "### 核對過的來源定義", "",
             "- FinMind 官方財報 schema 以 type/value/origin_name 保存科目與數值，不應只算 value 一欄。[官方文件](https://github.com/FinMind/FinMind-Doc/blob/master/docs/tutor/TaiwanMarket/Fundamental.md)。",
             "- FinLab 區分 actual disclosure date 與 deadline，報告期間與兩種轉換不可混用。[官方 FAQ](https://finlab.finance/docs/en/faq/)。",
             "- Census data_type_code 為 item type，category_code 為 Industry list；保留複合軸，不杜撰代碼含義。[item 定義](https://api.census.gov/data/timeseries/eits/qfr/variables/data_type_code.json)、[industry 定義](https://api.census.gov/data/timeseries/eits/qfr/variables/category_code.json)。", ""]
    atomic_write_text(root / "README.md", "\n".join(text) + "\n")


def verify_report(root: Path, expected_ids: set[str] | None = None) -> dict:
    """Reconcile every input ID with one Markdown detail and working anchor."""
    manifest = json.loads((root / "report_manifest.json").read_text())
    catalog_path = root / "feature_catalog.jsonl"
    if digest(catalog_path) != manifest["feature_catalog_sha256"]:
        raise ValueError("feature catalog changed after rendering")
    rows = [json.loads(line) for line in catalog_path.read_text().splitlines()]
    ids = [r["catalog_id"] for r in rows]
    if len(ids) != len(set(ids)) or len(ids) != manifest["summary"]["feature_entries"]:
        raise ValueError("duplicate or missing catalog identity")
    if expected_ids is not None and set(ids) != expected_ids:
        raise ValueError("input identities do not reconcile with output")
    expected_by_file = defaultdict(set)
    for row in rows:
        expected_by_file[row["report_file"]].add(row["report_anchor"])
        if not set(("semantic_role", "category", "scope", "training_state", "rights", "next_action_detail")) <= set(row):
            raise ValueError("a feature detail is missing status axes")
    actual_anchors, texts, anchor_sets = [], {}, {}
    for path in root.rglob("*.md"):
        text = path.read_text()
        if "\ufffd" in text:
            raise ValueError("malformed Unicode in report")
        texts[path.resolve()] = text
        anchor_sets[path.resolve()] = set(re.findall(r'<a id="(f-[0-9a-f]{24})"></a>', text))
    for page in manifest["feature_pages"]:
        path = root / page["file"]
        if digest(path) != page["sha256"]:
            raise ValueError("feature page hash changed")
        anchors = re.findall(r'<a id="(f-[0-9a-f]{24})"></a>', texts[path.resolve()])
        if len(anchors) != page["features"] or set(anchors) != expected_by_file[page["file"]]:
            raise ValueError("a Markdown page omitted or duplicated a feature")
        actual_anchors.extend(anchors)
    if len(actual_anchors) != len(set(actual_anchors)) or len(actual_anchors) != len(ids):
        raise ValueError("Markdown feature coverage does not reconcile")
    links = 0
    for path, text in texts.items():
        for match in re.finditer(r'(?<!!)\[[^\n]*?\]\(([^\s)]+)\)', text):
            target = match.group(1)
            if target.startswith(("https://", "http://")):
                continue
            name, _, fragment = target.partition("#")
            resolved = (path.parent / name).resolve() if name else path.resolve()
            if not resolved.is_relative_to(root.resolve()) or resolved not in texts:
                raise ValueError(f"broken or escaping report link: {path}: {target}")
            if fragment and fragment not in anchor_sets[resolved]:
                raise ValueError("broken feature anchor link")
            links += 1
    recounted = Counter(r["category"] for r in rows)
    if dict(sorted(recounted.items())) != manifest["summary"]["by_category"]:
        raise ValueError("category totals do not reconcile")
    return {"contract": CONTRACT, "state": "accepted_report_scope", "feature_entries": len(rows),
            "markdown_feature_details": len(actual_anchors), "all_ids_unique": True,
            "category_totals_reconciled": True, "local_links_verified": links,
            "feature_pages": len(manifest["feature_pages"]), "all_source_values_validated": False,
            "formal_training_ready": False, "verified_at_utc": datetime.now(UTC).isoformat(),
            "report_manifest_sha256": digest(root / "report_manifest.json")}


def reuse_source_snapshot(root: Path, inputs: list[dict]) -> tuple[list[dict], dict, list[dict], dict]:
    """Re-render the same accepted source observations, never pretend they are new.

    Raw snapshots in early reports were not separately hashed. Every raw key
    must therefore match the already hash-bound final catalog before reuse.
    This avoids rereading millions of values for a report classification edit.
    """
    manifest_path = root / "report_manifest.json"
    manifest_sha = digest(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    acceptance = json.loads((root / "report_acceptance.json").read_text())
    if manifest.get("contract") != CONTRACT or acceptance.get("state") != "accepted_report_scope":
        raise ValueError("source report contract or acceptance is incompatible")
    if acceptance.get("report_manifest_sha256") != manifest_sha:
        raise ValueError("source report manifest SHA does not match its acceptance")
    if {r["path"]: r["sha256"] for r in inputs} != {r["path"]: r["sha256"] for r in manifest["inputs"]}:
        raise ValueError("source report input identities changed")
    catalog_path = root / "feature_catalog.jsonl"
    projection_path = root / "source_projection_evidence.json"
    if digest(catalog_path) != manifest["feature_catalog_sha256"]:
        raise ValueError("source report catalog SHA changed")
    if digest(projection_path) != manifest["source_projection_evidence_sha256"]:
        raise ValueError("source report projection SHA changed")
    raw_paths = [root / "tej_feature_read_snapshot.json", root / "expanded_source_snapshot.jsonl"]
    raw_hashes = {str(p): digest(p) for p in raw_paths}
    tej_snapshot = json.loads(raw_paths[0].read_text())
    with raw_paths[1].open() as handle:
        expanded = [json.loads(line) for line in handle]
    raw_rows = [*tej_snapshot["features"], *expanded]
    raw_by_id = {r["catalog_id"]: r for r in raw_rows}
    if len(raw_by_id) != len(raw_rows):
        raise ValueError("source report raw identities collide")
    found, all_ids = set(), set()
    with catalog_path.open() as handle:
        for line in handle:
            final = json.loads(line)
            cid = final["catalog_id"]
            if cid in all_ids:
                raise ValueError("source report catalog identities collide")
            all_ids.add(cid)
            raw = raw_by_id.get(cid)
            if raw is not None:
                if any(key not in final or final[key] != value for key, value in raw.items()):
                    raise ValueError("raw source snapshot differs from hash-bound catalog")
                found.add(cid)
    if found != set(raw_by_id) or len(all_ids) != manifest["summary"]["feature_entries"]:
        raise ValueError("source report raw identities do not reconcile")
    if tej_snapshot["proof"] != manifest["summary"]["tej_read_snapshot"]:
        raise ValueError("source report TEJ read proof changed")
    for path in raw_paths:
        if digest(path) != raw_hashes[str(path)]:
            raise ValueError("source report raw snapshot changed during reuse")
    if digest(manifest_path) != manifest_sha or digest(catalog_path) != manifest["feature_catalog_sha256"]:
        raise ValueError("source report changed during reuse")
    projection = json.loads(projection_path.read_text())
    if digest(projection_path) != manifest["source_projection_evidence_sha256"]:
        raise ValueError("source report projection changed during reuse")
    reuse_proof = {"source_report": str(root), "source_manifest_sha256": manifest_sha,
                   "source_feature_catalog_sha256": manifest["feature_catalog_sha256"],
                   "source_projection_evidence_sha256": manifest["source_projection_evidence_sha256"],
                   "raw_snapshot_sha256": raw_hashes, "raw_source_fields_reconciled": len(raw_rows),
                   "not_a_new_source_download_or_freshness_check": True}
    return tej_snapshot["features"], tej_snapshot["proof"], expanded, {**projection, "reuse_proof": reuse_proof}


def build_report(catalog: Path, admission: Path, tej_root: Path, tej_inventory: Path,
                 output: Path, *, repo: Path, page_size: int = 250,
                 reuse_source_report: Path | None = None) -> dict:
    if output.exists():
        raise FileExistsError("report requires a fresh versioned output directory")
    if page_size < 1 or page_size > 1000:
        raise ValueError("Markdown page size must be 1..1000")
    code_sha = digest(Path(__file__))
    output.mkdir(parents=True)
    input_paths = [catalog / "feature_candidates.csv", catalog / "catalog_summary.json",
                   catalog / "physical_fields.csv", catalog / "source_inventory.csv",
                   catalog / "finmind_source_files.csv", catalog / "economic_source_files.csv",
                   admission / "feature_classification.csv", admission / "selected_source_features.csv",
                   tej_inventory / "all_fields.csv", tej_inventory / "summary.json"]
    inputs = [{"path": str(p), "sha256": digest(p)} for p in input_paths]
    base = read_csv(catalog / "feature_candidates.csv")
    admissions = {r["catalog_id"]: r for r in read_csv(admission / "feature_classification.csv")}
    if set(admissions) != {r["catalog_id"] for r in base}:
        raise ValueError("previous admission identities do not match input catalog")
    selected = {r["feature"]: r for r in read_csv(admission / "selected_source_features.csv")}
    reused = {}
    if reuse_source_report is not None:
        tej, tej_proof, expanded_all, projection = reuse_source_snapshot(reuse_source_report, inputs)
        expanded = [r for r in expanded_all if r["expansion_kind"] != "finlab_table_field"]
        finlab = [r for r in expanded_all if r["expansion_kind"] == "finlab_table_field"]
        evidence, finlab_evidence = projection["full_projection"], projection["footer_only"]
        reused = projection["reuse_proof"]
    else:
        tej, tej_proof = load_tej_snapshot(tej_root, tej_inventory)
        jobs = expansion_jobs(catalog, repo, base)
        expanded, evidence = aggregate_long_files(jobs)
        finlab, finlab_evidence = finlab_table_fields(base)
    atomic_write_json(output / "tej_feature_read_snapshot.json", {"proof": tej_proof, "features": tej})
    atomic_write_text(output / "expanded_source_snapshot.jsonl", "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in [*expanded, *finlab]) + "\n")
    all_inputs = [*base, *tej, *expanded, *finlab]
    if len(all_inputs) != len({r["catalog_id"] for r in all_inputs}):
        raise ValueError("report source identities collide")
    rows = []
    for row in all_inputs:
        prior = admissions.get(row["catalog_id"], {})
        rows.append(classify(row, prior, selected.get(prior.get("selected_canonical_feature"))))
    enrich_report_rows(rows)
    children = Counter(parent for row in rows for parent in row.get("parent_catalog_ids", []))
    unexpanded = []
    for row in rows:
        if row["semantic_role"] == "measure_container":
            unexpanded.append({"catalog_id": row["catalog_id"], "dataset_id": row["dataset_id"],
                               "field": row["field"], "expanded_children": children[row["catalog_id"]],
                               "remaining": "子項已列；歷史 context／版本與單位對齊仍須各別驗收" if children[row["catalog_id"]]
                               else "未取得此容器全部原生概念軸；需來源專用 schema／字典，不猜測"})
    pages = write_feature_pages(rows, output, page_size)
    sources = read_csv(catalog / "source_inventory.csv")
    source_pages = write_source_pages(sources, output, page_size)
    summary = report_summary(rows, len(base), len(tej), len(sources))
    summary["unexpanded_container_rows"] = sum(r["expanded_children"] == 0 for r in unexpanded)
    summary["tej_read_snapshot"] = tej_proof
    atomic_write_json(output / "source_projection_evidence.json", {"full_projection": evidence, "footer_only": finlab_evidence})
    atomic_write_text(output / "feature_catalog.jsonl", "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows) + "\n")
    catalog_summary = json.loads((catalog / "catalog_summary.json").read_text())
    physical = read_csv(catalog / "physical_fields.csv")
    physical_groups = Counter(r["dataset_id"] for r in physical)
    lines = ["# 實體欄位不是同等數量的模型特徵", "",
             f"共 {len(physical):,} 個 dataset×field。股票／券商／到期別寬表軸保留，但不把每個股票代碼當獨立財務 feature。", "",
             "[總報告](README.md)包含所有語意候選與展開概念。此表對所有實體 dataset 對帳，不把維度軸計入訊號。完整原欄清單由 input hash 固定的 physical_fields.csv 保留，並不宣稱所有實體欄可訓練。", "",
             "| 實體 dataset | field 數 |", "| --- | ---: |"]
    lines.extend(f"| {md(name)} | {count:,} |" for name, count in sorted(physical_groups.items()))
    atomic_write_text(output / "physical_grain.md", "\n".join(lines) + "\n")
    write_overview(summary, output, catalog_summary, inputs, pages, tej_proof, unexpanded)
    if reused:
        overview = output / "README.md"
        atomic_write_text(overview, overview.read_text() + "\n## 8. 本版重用的原檔驗收\n\n"
                          + "本版只更新語意分類與報告，原始觀測、TEJ 唯讀時間及下載界線維持原驗收；不冒稱重新下載或更新。"
                          + f'原報告：{md(reused["source_report"])}；manifest SHA256={reused["source_manifest_sha256"]}。'
                          + f'逐鍵核對 {reused["raw_source_fields_reconciled"]:,} 筆原始 snapshot 與 hash 固定的清冊後才重用。\n')
    for p, recorded in zip(input_paths, inputs, strict=True):
        if digest(p) != recorded["sha256"]:
            raise ValueError("report input changed during build; reject mixed evidence")
    if digest(Path(__file__)) != code_sha:
        raise ValueError("report implementation changed during build")
    manifest = {"contract": CONTRACT, "generated_at_utc": datetime.now(UTC).isoformat(),
                "summary": summary, "inputs": inputs, "feature_pages": pages, "source_pages": source_pages,
                "code_sha256": code_sha, "source_projection_evidence_sha256": digest(output / "source_projection_evidence.json"),
                "reused_source_report": reused,
                "expanded_source_snapshot_sha256": digest(output / "expanded_source_snapshot.jsonl"),
                "tej_feature_read_snapshot_sha256": digest(output / "tej_feature_read_snapshot.json"),
                "feature_catalog_sha256": digest(output / "feature_catalog.jsonl"), "unexpanded_containers": unexpanded,
                "format": "Markdown_only_requested; no web app or public publication",
                "limits": ["economic_meaning_is_not_predictive_value", "source_labels_not_independent_information_dimension",
                           "not_all_provider_possible_series_expanded", "not_all_sources_full_value_or_PIT_validated",
                           "no_download_no_remote_write_no_training_no_model_admission_change"]}
    atomic_write_json(output / "report_manifest.json", manifest)
    acceptance = verify_report(output, {r["catalog_id"] for r in all_inputs})
    atomic_write_json(output / "report_acceptance.json", acceptance)
    return {"summary": summary, "acceptance": acceptance, "report": str(output / "README.md")}
