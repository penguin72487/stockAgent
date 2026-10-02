"""Build an evidence-backed TEJ catalog and two acquisition phases plus validation.

No API, GUI, training, queue or source-data mutation. English/Chinese aliases
are comparison candidates, not permission to merge independent observations.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import csv
from datetime import date, datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import sqlite3
import sys
import unicodedata
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, atomic_write_text

CONTRACT_VERSION = 2
CATALOG_CONTRACT_VERSION = 1

# Explicit concept hints. A match does NOT assert same currency, consolidation,
# revision, accounting basis or publication time. Unknown concepts stay visible.
ALIASES = {
    "Open(NTD)": ["開盤價", "open"],
    "High(NTD)": ["最高價", "max", "high"],
    "Low(NTD)": ["最低價", "min", "low"],
    "Close(NTD)": ["收盤價", "close"],
    "Volume(1000S)": ["成交股數", "Trading_Volume", "twpub_official_trading_volume_raw"],
    "Amount(NTD1000)": ["成交金額", "Trading_money", "twpub_official_trading_value_raw"],
    "Transaction": ["成交筆數", "Trading_turnover", "twpub_official_trades_raw"],
    "Last Bid(NTD)": ["最後揭示買價"], "Last Offer(NTD)": ["最後揭示賣價"],
    "P/E-TSE": ["本益比", "PER", "twpub_pe_raw"],
    "P/B-TSE": ["股價淨值比", "PBR", "twpub_pb_raw"],
    "Dividend_Yield%": ["殖利率(%)", "dividend_yield", "twpub_dividend_yield_pct_raw"],
    "Shares(1000S)": ["發行股數", "發行股數(股)", "發行股數(千股)"],
    "Market Cap.(NTD MN)": ["市值", "market_value"],
    "Sale-Monthly": ["當月營收", "revenue", "twpub_monthly_revenue_raw"],
    "Sale-Monthly,Last Year": ["去年當月營收"],
    "YoY%-Monthly Sale": ["去年同月增減(%)", "twpub_monthly_revenue_yoy"],
    "MoM%-Monthly Sale": ["上月比較增減(%)", "twpub_monthly_revenue_mom"],
    "Sale-Accumulated": ["當月累計營收", "twpub_cumulative_revenue_raw"],
    "Sale-Accu.,L.Y.": ["去年累計營收"],
    "YoY%-Acc. Sales": ["前期比較增減(%)", "twpub_cumulative_revenue_yoy"],
    "Total assets": ["資產總額", "資產總計", "Assets"],
    "Total current assets": ["流動資產", "流動資產合計", "CurrentAssets"],
    "Total non-current assets": ["非流動資產", "非流動資產合計", "NoncurrentAssets"],
    "Total liabilities": ["負債總額", "負債總計", "Liabilities"],
    "Total current liabilities": ["流動負債", "流動負債合計", "CurrentLiabilities"],
    "Total non-current liabilities": ["非流動負債", "NoncurrentLiabilities"],
    "Total equity": ["權益總額", "權益總計", "Equity"],
    "Total cash and cash equivalents": ["現金及約當現金", "CashAndCashEquivalents"],
    "Total inventories": ["存貨", "Inventories"],
    "Total property, plant and equipment": ["不動產廠房及設備", "PropertyAndPlantAndEquipment"],
    "Accounts & notes receivable": ["應收帳款及票據"],
    "Goodwill and intangible assets": ["商譽及無形資產合計"],
    "Deferred tax assets": ["遞延所得稅資產", "DeferredTaxAssets"],
    "Deferred tax liabilities": ["遞延所得稅負債", "DeferredTaxLiabilities"],
    "Right of use assets": ["使用權資產", "RightOfUseAssets"],
    "Investment property, net": ["投資性不動產淨額", "InvestmentProperty"],
    "Total operating revenue": ["營業收入淨額", "營業收入", "Revenue"],
    "Operating revenue": ["營業收入淨額", "營業收入", "Revenue"],
    "Total operating costs": ["營業成本", "CostOfGoodsSold"],
    "Operating income": ["營業利益", "OperatingIncome"],
    "Gross profit": ["營業毛利", "GrossProfit"],
    "Total operating expenses": ["營業費用", "OperatingExpenses"],
    "Total income tax expense": ["所得稅費用", "Tax"],
    "Interest income": ["利息收入", "InterestIncome"],
    "Interest expense": ["利息費用", "InterestExpense"],
    "Depreciation": ["折舊費用", "折舊", "Depreciation"],
    "Amortization": ["攤銷費用", "AmortizationExpense"],
    "Current ratio": ["流動比率"], "Quick ratio": ["速動比率"],
    "Operating profit margin": ["營業利益率"], "Gross profit margin": ["營業毛利率"],
    "Debt ratio": ["負債比率"], "EBITDA": ["EBITDA"],
    "EPS": ["每股盈餘", "基本每股盈餘", "EPS"],
    "Qfii Buy(1000S)": ["外資買進股數", "Foreign_Investor_buy"],
    "Qfii Sell(1000S)": ["外資賣出股數", "Foreign_Investor_sell"],
    "Fund Buy(1000S)": ["投信買進股數", "Investment_Trust_buy"],
    "Fund Sell(1000S)": ["投信賣出股數", "Investment_Trust_sell"],
    "Dealer Buy(1000S)": ["自營商買進股數", "Dealer_buy"],
    "Dealer Sell(1000S)": ["自營商賣出股數", "Dealer_sell"],
    "Qfii Net S/B(1000S)": ["外資買賣超", "twpub_foreign_net_buy_raw"],
    "Fund Net S/B(1000S)": ["投信買賣超", "twpub_trust_net_buy_raw"],
    "Dealer Net S/B(1000S)": ["自營商買賣超", "twpub_dealer_net_buy_raw"],
    "Foreign Invest.%": ["全體外資及陸資持股比率", "ForeignInvestmentSharesRatio"],
    "Day Trading Volume(1000S)": ["當日沖銷交易成交股數", "Volume"],
    "Day Trading Value of Buys(NTD1000)": ["當日沖銷交易買進成交金額", "BuyAfterSale"],
    "Day Trading Value of Sells(NTD1000)": ["當日沖銷交易賣出成交金額", "SellAfterBuy"],
    "Open": ["open", "Open"], "High": ["max", "high", "High"],
    "Low": ["min", "low", "Low"], "Close": ["close", "Close"],
    "OI": ["open_interest", "未平倉口數"],
    "DSP": ["settlement_price", "結算價"],
}


def normalize(value: str) -> str:
    return re.sub(r"[^\w]", "", unicodedata.normalize("NFKC", value).casefold()).replace("_", "")


def cadence(table: str, category: str) -> tuple[str, str]:
    for pattern, name in ((r"daily|day trading|event_daily", "daily"),
                          (r"weekly|event_weekly", "weekly"),
                          (r"monthly|month|event_monthly", "monthly"),
                          (r"yearly|annual", "yearly"), (r"qly|quarter|\(acc\)", "quarterly")):
        if re.search(pattern, table, re.I):
            return name, "inferred_from_table_label_not_release_schedule"
    if category in {"stock_daily", "stock_adjusted", "stock_flow", "stock_margin", "stock_lending", "stock_daytrade", "futures", "options"}:
        return "daily", "inferred_from_family_not_release_schedule"
    return "unknown_or_event", "requires_provider_definition"


def category(table: str, type_name: str = "", smart: str = "") -> str:
    text = f"{table} {smart}".casefold()
    if "auditor" in text:
        return "audit_report"
    if "subsidiary monthly" in text or "department information" in text:
        return "segments_subsidiaries"
    if "holding groups" in text:
        return "governance_investments"
    if re.search(r"ifrs|financial_|financial holding|assets/liability|aging analysis|fair value|sensitivity analysis|quality of assets|bis ratio", text):
        return "financial"
    if "monthly sales" in text:
        return "monthly_revenue"
    if re.search(r"remuneration|directors|long-term investment|invest transfer|investment in china|holding groups|shares structure", text):
        return "governance_investments"
    if re.search(r"treasury|dividend|shareholder|ex_right|preferred stock|finance events", text):
        return "corporate_actions"
    if "company attribute" in text or "pre ipo" in text:
        return "company_lifecycle"
    if "unadjusted_price" in text:
        return "stock_daily"
    if "adjusted_price" in text or "roi description" in text:
        return "stock_adjusted"
    if "qfii" in text or "equity view" in text:
        return "stock_flow"
    if "margin trading" in text:
        return "stock_margin"
    if re.search(r"borrowing|sbl information", text):
        return "stock_lending"
    if "day trading" in text:
        return "stock_daytrade"
    if "option" in text or type_name.startswith("Option"):
        return "options"
    if "future" in text or type_name.startswith("Future"):
        return "futures"
    if "exchange rate" in text or "forex" in text:
        return "fx"
    if "macro" in text or type_name == "Bankstat":
        return "macro_banking"
    if re.search(r"fund|bond|yield", text):
        return "funds_bonds"
    if "stock index" in text:
        return "global_indices"
    if "event" in text or "calendar" in text or "suspended" in text:
        return "events"
    return "other"


def local_category(dataset: str, field: str) -> str:
    text = f"{dataset} {field}".casefold()
    if "financial_statement" in text or "fundamental_features" in text or re.search(r"balancesheet|cashflowsstatement|financialstatements|xbrl", text):
        return "financial"
    if "monthly_revenue" in text or "monthrevenue" in text:
        return "monthly_revenue"
    if "daytrading" in text or "day_trading" in text or "day_trade" in text:
        return "stock_daytrade"
    if "margin" in text and "future" not in text:
        return "stock_margin"
    if "lending" in text or "short_sale" in text or "borrow" in text:
        return "stock_lending"
    if "institutional" in text or "shareholding" in text or "foreign_investor" in text:
        return "stock_flow"
    if "future" in text or "期貨" in text:
        return "futures"
    if "option" in text or "選擇權" in text:
        return "options"
    if "forex" in text or "exchangerate" in text or "frankfurter" in text:
        return "fx"
    if "price_adj" in text or "priceadj" in text or "adjclose" in text:
        return "stock_adjusted"
    if "price:" in text or "stockprice" in text or "stock-features" in text or "stock-daily" in text:
        return "stock_daily"
    if "dividend" in text or "treasury" in text or "capital_reduction" in text:
        return "corporate_actions"
    if re.search(r"business|pmi|nmi|macro|gov_bond|interest|money|m2|gdp|cpi|ppi|bank", text):
        return "macro_banking"
    if "company" in text or "stockinfo" in text or "delist" in text:
        return "company_lifecycle"
    if "cb_" in text or "etf" in text or "fund" in text or "bond" in text:
        return "funds_bonds"
    if re.search(r"announcement|attention|disposal|event|conference|calendar", text):
        return "events"
    return "other"


def safe_date(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        result = date.fromisoformat(value[:10])
    except ValueError:
        return None
    return result.isoformat() if result <= datetime.now(ZoneInfo("Asia/Taipei")).date() else None


def local_cadence(dataset: str, family: str) -> str:
    """Conservative metadata inference; a minute source is not daily coverage."""
    name = dataset.casefold()
    if re.search(r"(?:^|[^a-z])tick(?:[^a-z]|$)|逐筆", name):
        return "tick"
    if re.search(r"minute|(?:^|[^0-9])(?:1m|5m|15m)(?:[^a-z]|$)", name):
        return "minute"
    if re.search(r"monthly|monthrevenue|monthly_revenue", name):
        return "monthly"
    if re.search(r"weekly|week", name):
        return "weekly"
    if re.search(r"yearly|annual", name):
        return "yearly"
    if re.search(r"financial_statement:|balancesheet|cashflowsstatement|financialstatements", name):
        return "quarterly"
    if re.search(r"price:|stockprice|stock-daily|future.*daily|option.*daily|daily", name):
        return "daily"
    # Some feature views carry quarterly state onto a daily decision grid.
    # Without a field profile this grid is not a new native observation.
    return "unknown_or_event"


def field_role(field: str) -> str:
    if re.search(r"announcement.*date|forecast.*date|\bdate\b|\bym\b", field, re.I):
        return "date_or_availability_metadata"
    if re.search(r"currency|consolidation|\bquarter\b|\bmonth\b|\bmarket\b|remark|description|name|\bcode\b|industry|auditor|address", field, re.I):
        return "categorical_or_context_verify_type"
    return "value_or_flag_verify_provider_type"


def read_json(path: Path) -> tuple[dict | list, str]:
    raw = path.read_bytes()
    return json.loads(raw.decode("utf-8-sig")), hashlib.sha256(raw).hexdigest()


def dump_csv(path: Path, rows: list[dict]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row)) if rows else ["feature_id"]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: json.dumps(value, ensure_ascii=False, sort_keys=True) if isinstance(value, (list, dict)) else value for key, value in row.items()})
    atomic_write_text(path, "\ufeff" + buffer.getvalue(), durable=True)


def catalog(paths: list[Path]) -> tuple[list[dict], dict]:
    bindings, seen, types, covered, inputs, schemas, empty_types = [], set(), [], set(), [], {}, set()
    if not paths:
        raise ValueError("At least one catalog capture is required")
    for path in paths:
        raw = path.read_bytes()
        records = [json.loads(line) for line in raw.decode("utf-8-sig").splitlines() if line.strip()]
        if not records or records[0].get("contract_version") != CATALOG_CONTRACT_VERSION or records[0].get("provider") != "tej_smart_wizard":
            raise ValueError("Unrecognized catalog evidence")
        header = records[0]
        if (header.get("record_kind") != "header" or not isinstance(header.get("types"), list)
                or not header["types"] or any(not isinstance(t, str) or not t for t in header["types"])
                or len(set(header["types"])) != len(header["types"])):
            raise ValueError("Invalid catalog type scope")
        if types and header["types"] != types:
            raise ValueError("Catalog type list changed between captures")
        types = header["types"]
        finish = records[-1]
        inputs.append({"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "completion": finish if finish.get("record_kind") == "completion" else None})
        completed = {r["type"] for r in records if r.get("record_kind") == "type_completion" and r.get("complete") is True}
        if finish.get("record_kind") == "completion" and finish.get("complete") is True:
            completed.update(header.get("selected_types", types))
        if not completed.issubset(set(types)):
            raise ValueError("Completion references an unknown catalog type")
        covered.update(completed)
        empty_types.update(r["type"] for r in records if r.get("record_kind") == "empty_type" and r.get("type") in types)
        for record in records:
            if record.get("record_kind") != "binding":
                continue
            fields = record.get("fields")
            if (not isinstance(fields, list) or record.get("field_count") != len(fields)
                    or any(not isinstance(f, str) or not f for f in fields)
                    or record.get("type") not in types or not record.get("smart_id") or not record.get("table")):
                raise ValueError("Inconsistent catalog binding")
            scope = record["type"], record["smart_id"], record["table"]
            if scope in schemas and schemas[scope] != fields:
                raise ValueError("Catalog schema changed; reconcile captures before planning")
            schemas[scope] = fields
            key = (record["type"], record["smart_id"], record["table"], tuple(record["fields"]))
            if key not in seen:
                bindings.append(record)
                seen.add(key)
    observed = {r["type"] for r in bindings}
    return bindings, {"inputs": inputs, "types": types, "types_verified_complete": sorted(covered),
                      "types_with_observed_bindings": sorted(observed), "types_with_observed_empty_menu": sorted(empty_types),
                      "types_without_binding_or_empty_menu_evidence": [t for t in types if t not in observed | empty_types],
                      "types_remaining": [t for t in types if t not in covered],
                      "catalog_scan_complete": bool(types) and set(types) == covered}


def local_features(root: Path) -> tuple[list[dict], list[dict]]:
    """Read current receipts/schemas, not all source arrays or model masks."""
    import pyarrow.parquet as pq

    rows, evidence = [], []
    for path in sorted((root / "data_finlab/receipts").glob("*.json")):
        rec, digest = read_json(path)
        key = rec.get("dataset")
        if not key or not rec.get("parquet_path"):
            continue
        base = root / "data_finlab"
        source = (base / rec["parquet_path"]).resolve()
        if not source.is_relative_to(base.resolve()) or not source.is_file():
            continue
        names = [key.split(":", 1)[1]] if ":" in key else [n for n in pq.ParquetFile(source).schema.names if n not in rec.get("source_index_columns", [])]
        for field in names:
            count = rec.get("non_null_values") if ":" in key else None
            rows.append({"local_id": f"FinLab|{key}|{field}", "provider": "FinLab", "dataset": key,
                         "field": field, "category": local_category(key, field),
                         "cadence": local_cadence(key, local_category(key, field)),
                         "non_null_count": count, "count_basis": "receipt_current_dataset_non_null_cells" if count is not None else "event_field_not_profiled",
                         "first": safe_date(rec.get("first_non_null_source_index")), "last": safe_date(rec.get("last_non_null_source_index")),
                         "first_source_label": rec.get("first_non_null_source_index"), "last_source_label": rec.get("last_non_null_source_index"),
                         "bounds_basis": "receipt_source_index_not_publication_or_complete_key_coverage",
                         "source_path": str(path.relative_to(root)), "source_sha256": digest,
                         "snapshot_at_utc": rec.get("source_checked_at_utc", rec.get("fetched_at_utc"))})
    monitor = root / "artifacts/live/data_monitor/feature_inventory.json"
    if monitor.is_file():
        doc, digest = read_json(monitor)
        evidence.append({"path": str(monitor.relative_to(root)), "sha256": digest, "generated_at_utc": doc.get("generated_at_utc"),
                         "excluded_finlab_instrument_axis_rows": sum("FinLab" in r.get("provider", "") for r in doc["rows"])})
        for rec in doc["rows"]:
            if "FinLab" in rec.get("provider", "") or not rec.get("field"):
                continue  # Stock-code columns in wide files are not separate features.
            dataset, field = rec["dataset_id"], rec["field"]
            rows.append({"local_id": f"{rec['provider']}|{dataset}|{field}", "provider": rec["provider"], "dataset": dataset,
                         "field": field, "category": local_category(dataset, field),
                         "cadence": local_cadence(dataset, local_category(dataset, field)),
                         "non_null_count": rec.get("non_null_count"), "count_basis": "monitor_field_profile_not_unique_economic_key_audit",
                         "first": safe_date(rec.get("dataset_first")), "last": safe_date(rec.get("dataset_last")),
                         "first_source_label": rec.get("dataset_first"), "last_source_label": rec.get("dataset_last"),
                         "bounds_basis": "dataset_bounds_not_per_field_bounds_or_publication",
                         "source_path": str(monitor.relative_to(root)), "source_sha256": digest,
                         "snapshot_at_utc": doc.get("generated_at_utc")})
    for lane in ("sponsor", "complement"):
        base = root / "data_finmind" / lane
        queue = base / "queue.sqlite3"
        if not queue.is_file():
            continue
        with closing(sqlite3.connect(f"file:{queue}?mode=ro", uri=True)) as con:
            # A read transaction sees a consistent task snapshot, without locks
            # that block the downloader's WAL writes.
            con.execute("BEGIN")
            coverage = {ds: (first, last) for ds, first, last in con.execute(
                "SELECT dataset,MIN(first_data_date),MAX(last_data_date) FROM tasks WHERE state='complete' GROUP BY dataset")}
            samples = con.execute("""SELECT dataset,receipt_path FROM (
                SELECT dataset,receipt_path,ROW_NUMBER() OVER(PARTITION BY dataset ORDER BY last_data_date DESC,last_attempt_at_utc DESC) AS rank
                FROM tasks WHERE state='complete' AND receipt_path IS NOT NULL) WHERE rank=1""").fetchall()
        for dataset, relative in samples:
            path = (base / relative).resolve()
            if not path.is_relative_to(base.resolve()) or not path.is_file():
                continue
            rec, digest = read_json(path)
            if not rec.get("parquet_path"):
                continue
            source = (base / rec["parquet_path"]).resolve()
            if not source.is_relative_to(base.resolve()) or not source.is_file():
                continue
            parquet = pq.ParquetFile(source)
            fields = parquet.schema.names
            financial = "type" in fields and "origin_name" in fields and "value" in fields
            if financial:
                columns = parquet.read(columns=["type", "origin_name"], use_threads=False).to_pydict()
                field_pairs = sorted(set(zip(columns["type"], columns["origin_name"])), key=str)
            else:
                field_pairs = [(field, field) for field in fields if field not in {"date", "stock_id"}]
            first, last = coverage[dataset]
            for code, field in field_pairs:
                if not isinstance(field, str):
                    continue
                rows.append({"local_id": f"FinMind-{lane}|{dataset}|{code}|{field}", "provider": f"FinMind-{lane}", "dataset": dataset,
                             "field": field, "category": local_category(dataset, field),
                             "cadence": local_cadence(dataset, local_category(dataset, field)),
                             "non_null_count": None if financial else rec.get("field_non_null_counts", {}).get(code),
                             "count_basis": "latest_receipt_field_sample_not_full_history" if not financial else "latest_report_code_catalog_not_full_history_profile",
                             "first": safe_date(first), "last": safe_date(last),
                             "first_source_label": first, "last_source_label": last,
                             "bounds_basis": "dataset_complete_task_bounds_not_per_field_or_full_key_coverage",
                             "source_path": str(path.relative_to(root)), "source_sha256": digest,
                             "snapshot_at_utc": rec.get("fetched_at_utc"), "_alias_code": code})
        evidence.append({"path": str(queue.relative_to(root)), "mode": "sqlite_read_transaction", "datasets": len(coverage), "full_field_history_profile": False})
    return rows, evidence


def unit(field: str) -> tuple[str, str]:
    if "1000S" in field:
        return "thousand_shares", "shares_after_multiply_1000"
    if "NTD1000" in field:
        return "thousand_TWD", "TWD_after_multiply_1000"
    if re.search(r"NTD\s*MN", field):
        return "million_TWD", "TWD_after_multiply_1000000"
    if "NTD" in field:
        return "TWD", "retain_source_precision"
    if "%" in field:
        return "percentage_points", "retain_original_ratio_scale"
    return "not_declared_in_field_label", "verify_table_UOM_before_ingestion"


def table_identity(smart: str, table: str, fields: list[str]) -> tuple[str, str]:
    signature = hashlib.sha256(json.dumps(fields, ensure_ascii=False).encode()).hexdigest()
    identity = hashlib.sha256(json.dumps([smart, table, signature], ensure_ascii=False).encode()).hexdigest()[:24]
    return identity, signature


def worklist(bindings: list[dict], local: list[dict], *, start: str) -> list[dict]:
    date.fromisoformat(start)
    aliases = {normalize(key): values for key, values in ALIASES.items()}
    index = defaultdict(list)
    for rec in local:
        index[(rec["category"], normalize(rec["field"]))].append(rec)
        if rec.get("_alias_code"):
            index[(rec["category"], normalize(rec["_alias_code"]))].append(rec)
    grouped = {}
    for binding in bindings:
        _, signature = table_identity(binding["smart_id"], binding["table"], binding["fields"])
        key = binding["smart_id"], binding["table"], signature
        if key not in grouped:
            grouped[key] = {**binding, "available_types": []}
        grouped[key]["available_types"].append(binding["type"])
    output = []
    for (smart, table, signature), binding in grouped.items():
        table_id, _ = table_identity(smart, table, binding["fields"])
        family = category(table, binding["type"], smart)
        grain, grain_basis = cadence(table, family)
        for field_index, field in enumerate(binding["fields"]):
            targets = [field, *aliases.get(normalize(field), [])]
            possible = {}
            local_families = [family]
            if family == "stock_daily":
                local_families += ["events", "stock_flow", "company_lifecycle"]
            if family == "stock_daytrade" and field in {"Close(NTD)", "Volume(1000S)", "Amount(NTD1000)"}:
                local_families += ["stock_daily"]
            if family == "stock_adjusted":
                local_families += ["stock_daily"]
            for local_family in local_families:
                for target in targets:
                    for rec in index[(local_family, normalize(target))]:
                        possible[rec["local_id"]] = rec
            matches = list(possible.values())
            comparable = [rec for rec in matches if rec.get("cadence") == grain
                          and grain != "unknown_or_event" and rec["category"] == family]
            reported_counts = [rec["non_null_count"] for rec in comparable if rec["provider"] == "FinLab" and rec["non_null_count"] is not None]
            count = max(reported_counts) if reported_counts else None
            starts = [r["first"] for r in comparable if r["first"]]
            ends = [r["last"] for r in comparable if r["last"]]
            first, last = min(starts) if starts else None, max(ends) if ends else None
            if not matches:
                stage, reason = "P1", "未找到同概念已取資料的明確對照；候選新增，仍須排除未映射的同義欄位"
            elif not comparable:
                stage, reason = "P2", "已有同名／概念線索但頻率或還原／表義不符或未知；先核對，不當成完整或重複"
            elif (count is not None and count == 0) or not first or first > start or count is None:
                stage, reason = "P2", "已有概念對照但首期不足／非空筆數或逐欄覆蓋未證明；先查缺口，再補歷史"
            else:
                stage, reason = "P3", "已有概念對照且報告首末範圍覆蓋目標起點；作校驗候選，不宣稱鍵／歷史已完整"
            source_unit, conversion = unit(field)
            fingerprint = hashlib.sha256(json.dumps([smart, table, signature, field_index, field], ensure_ascii=False).encode()).hexdigest()[:24]
            output.append({"feature_id": fingerprint, "phase": stage, "category": family,
                           "table_id": table_id, "schema_sha256": signature,
                           "smart_id": smart, "table": table, "field_index": field_index, "field": field,
                           "available_types": sorted(set(binding["available_types"])), "cadence": grain, "cadence_basis": grain_basis,
                           "field_role": field_role(field),
                           "source_unit": source_unit, "normalization": conversion,
                           "original_format": "source_log_derived_do_not_use_as_raw_input" if re.search(r"ROI%-Ln|logarithm|\blog\b", field, re.I) else "original_source_values_ratios_and_labels_retained",
                           "entitlement": "catalog_visible_query_unverified", "tej_history_first": None, "tej_history_last": None,
                           "tej_history_count": None, "local_match_ids": sorted(possible), "match_basis": "concept_hint_not_proven_equivalence" if matches else "no_explicit_match_not_global_absence_proof",
                           "same_family_cadence_match_ids": sorted(r["local_id"] for r in comparable),
                           "context_check": "instrument_universe_currency_consolidation_accumulation_revision_release_time_still_unverified",
                           "local_non_null_count": count, "local_count_basis": "FinLab_current_key_non_null_cells_only" if count is not None else "unknown_not_zero",
                           "local_first_reported": first, "local_last_reported": last, "reason": reason,
                           "publication_clock": "catalog_inventory_does_not_verify_release_time",
                           "download_started": False})
    # Fewer observations are meaningful only within comparable native cadences.
    output.sort(key=lambda r: (r["phase"], r["category"], r["cadence"], r["local_non_null_count"] is None,
                               r["local_non_null_count"] if r["local_non_null_count"] is not None else 0,
                               r["table"], r["field_index"]))
    return [{"order": i, **row} for i, row in enumerate(output, 1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, nargs="+", required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output-dir", type=Path, required=True, help="New directory; evidence is never overwritten")
    parser.add_argument("--target-start", default="2014-01-01")
    args = parser.parse_args(argv)
    date.fromisoformat(args.target_start)
    if args.output_dir.exists():
        raise FileExistsError("Refusing to overwrite existing inventory evidence")
    bindings, scan = catalog(args.catalog)
    local, sources = local_features(args.root.resolve())
    rows = worklist(bindings, local, start=args.target_start)
    args.output_dir.mkdir(parents=True, exist_ok=False)
    dump_csv(args.output_dir / "all_fields.csv", rows)
    for stage, name in (("P1", "phase1_new_feature_candidates.csv"), ("P2", "phase2_history_gap_candidates.csv"), ("P3", "phase3_cross_source_validation.csv")):
        dump_csv(args.output_dir / name, [r for r in rows if r["phase"] == stage])
    dump_csv(args.output_dir / "local_feature_evidence.csv", [{k: v for k, v in r.items() if not k.startswith("_")} for r in local])
    table_rows = []
    groups = defaultdict(list)
    for row in rows:
        groups[row["table_id"]].append(row)
    table_bindings = {}
    for binding in bindings:
        table_id, signature = table_identity(binding["smart_id"], binding["table"], binding["fields"])
        entry = table_bindings.setdefault(table_id, {**binding, "schema_sha256": signature, "available_types": set()})
        entry["available_types"].add(binding["type"])
    # Empty field menus are still catalog tables, not silently missing entries
    # or evidence that a historical query was denied.
    for table_id, binding in sorted(table_bindings.items(), key=lambda pair: (pair[1]["smart_id"], pair[1]["table"], pair[0])):
        smart, table = binding["smart_id"], binding["table"]
        family = category(table, binding["type"], smart)
        members = groups[table_id]
        table_rows.append({"table_id": table_id, "schema_sha256": binding["schema_sha256"],
                           "category": family, "smart_id": smart, "table": table, "fields": len(members),
                           "catalog_state": "visible_fields" if members else "empty_field_menu_query_unverified",
                           "phase1": sum(r["phase"] == "P1" for r in members), "phase2": sum(r["phase"] == "P2" for r in members),
                           "phase3": sum(r["phase"] == "P3" for r in members), "available_types": sorted(binding["available_types"])})
    dump_csv(args.output_dir / "tables.csv", table_rows)
    summary = {"contract_version": CONTRACT_VERSION, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
               "target_start_for_comparison": args.target_start, "catalog": scan, "menu_bindings": len(bindings),
               "menu_field_occurrences": sum(len(r["fields"]) for r in bindings),
               "unique_table_schema_fields": len(rows), "tables": len(table_rows), "local_feature_evidence_rows": len(local),
              "phase_counts": dict(Counter(r["phase"] for r in rows)), "categories": dict(Counter(r["category"] for r in rows)),
               "phase2_known_local_count": sum(r["phase"] == "P2" and r["local_non_null_count"] is not None for r in rows),
               "phase2_unknown_local_count": sum(r["phase"] == "P2" and r["local_non_null_count"] is None for r in rows),
               "source_evidence": sources, "limits": {"all_download_entitlements_verified": False, "historical_field_bounds_verified": False,
               "all_other_sources_absence_proven": False, "aliases_prove_equivalence": False, "full_key_completeness_verified": False,
               "downloads_or_schedulers_started": False, "guessed_history_bounds_or_observations_created": False}}
    catalog_scopes = []
    for type_name in scan["types"]:
        members = [r for r in bindings if r["type"] == type_name]
        catalog_scopes.append({"type": type_name, "observed_tables": len(members),
                               "observed_field_occurrences": sum(r["field_count"] for r in members),
                               "has_empty_menu_receipt": type_name in scan["types_with_observed_empty_menu"],
                               "scan_explicitly_complete": type_name in scan["types_verified_complete"],
                               "status": "verified_catalog_complete" if type_name in scan["types_verified_complete"] else
                               "observed_bindings_completion_unverified" if members else
                               "empty_menu_not_entitlement_denial" if type_name in scan["types_with_observed_empty_menu"] else "not_yet_observed",
                               "historical_download_entitlement_verified": False})
    dump_csv(args.output_dir / "catalog_scopes.csv", catalog_scopes)
    atomic_write_json(args.output_dir / "summary.json", summary)
    lines = ["# TEJ Smart Wizard 欄位及下載順序清冊", "",
             f"觀察時間：{summary['generated_at_utc']}。共 {len(rows):,} 個資料表欄位項目、{len(table_rows):,} 個 SmartID／表名；已讀到的欄位沒有 Top-K 截斷。", "",
             "目錄可見不等於每張表已通過歷史下載測試；首末日與筆數未知時留空，不填成零或推測日期。", "",
             "## 下載順序", "", "| 階段 | 欄位候選數 | 動作 |", "| --- | ---: | --- |",
             f"| P1 未找到明確對照 | {summary['phase_counts'].get('P1', 0):,} | 先確認同義欄位與權限，再取得真正新增特徵 |",
             f"| P2 已有對照但覆蓋待補 | {summary['phase_counts'].get('P2', 0):,} | 同類型／頻率按非空筆數與缺口排序；未知筆數不當零 |",
             f"| P3 既有概念校驗候選 | {summary['phase_counts'].get('P3', 0):,} | 前兩階段後才排全量獨立校驗；不直接覆蓋主來源 |", "",
             "這是兩個取得階段加最後的校驗階段。P1 是未找到映射的候選，不是證明其他程式絕對沒有；P2 包含仍需逐欄查明的覆蓋。", "",
             "## 完整清單", "", "- [逐欄明細](all_fields.csv)", "- [P1 新特徵候選](phase1_new_feature_candidates.csv)",
             "- [P2 歷史覆蓋候選](phase2_history_gap_candidates.csv)", "- [P3 多源校驗候選](phase3_cross_source_validation.csv)",
             "- [既有特徵證據](local_feature_evidence.csv)", "- [機器可讀摘要](summary.json)", "",
             "- [全部 30 類目錄的讀取狀態](catalog_scopes.csv)", "",
             "## 資料表", "", "| 分類 | SmartID | 資料表 | 欄位 | P1 | P2 | P3 |", "| --- | --- | --- | ---: | ---: | ---: | ---: |"]
    for rec in table_rows:
        cells = [rec['category'], rec['smart_id'], rec['table'], rec['fields'], rec['phase1'], rec['phase2'], rec['phase3']]
        lines.append("| " + " | ".join(str(v).replace("|", "\\|") for v in cells) + " |")
    lines += ["", "## 驗收與限制", "", f"已確認逐類完成：{len(scan['types_verified_complete'])}/{len(scan['types'])}。未確認的分類：{', '.join(scan['types_remaining']) or '無'}。",
              "原始目錄與輸入 SHA256 在 summary.json。來源收據與 SQLite 讀交易留有出處；未對全部原始檔重做內容雜湊或鍵覆蓋掃描。",
              "FinLab 寬表的股票代號欄不是獨立 feature，已排除面板中這種軸欄，改用 dataset key 收據。FinMind 的最新財報代碼樣本不是所有年代代碼聯集。",
              "欄位比對只供排程規劃，需另核對商品／市場、頻率、母公司／合併、累計／單季、還原／原值、單位、版本及發布時刻，才能合併或校驗。",
              "合併的僅是跨 Type 完全相同的 SmartID／表名／欄位清單，available_types 全數保留；不同的財報、還原價格、時間粒度或表名不相互刪除。",
              "總經、外匯及指數可能以公司／標的代號表示系列。欄位項目數不是系列數，也不是已通過下載的權限數；本次未將這些代號軸誤算成 feature。",
              "每個 TEJ 欄位的最早／最新日期、非空筆數、實際下載權限、發布時程及精確缺口須後續按來源查驗；本次沒有啟動市場資料下載或新排程。", ""]
    atomic_write_text(args.output_dir / "inventory.md", "\n".join(lines))
    print(json.dumps({k: summary[k] for k in ("unique_table_schema_fields", "tables", "phase_counts")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
