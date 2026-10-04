"""Exact, receipt-backed exclusion of native API facts from Wizard queries.

No table-name-only delegation. The reviewed unadjusted daily-price mapping is
eligible only for a daily two-key grid. Other candidate table pairs stay visible
for semantic/key/unit review; a trial's global min/max is not per-symbol proof.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json

CONTRACT = "tej_exact_native_fact_delegation_v2"
API_TABLE = "TRAIL/TAPRCD"
WIZARD_SMART = "TEJ Equity"
WIZARD_TABLE = "TSE/OTC Unadjusted_Price(Daily)"
# Same vendor, reviewed economic definitions and scale. Not adjusted prices,
# monthly grids, other markets, vendor snapshots or third-key event records.
FIELD_PAIRS = (
    ("open_d", "Open(NTD)"), ("high_d", "High(NTD)"), ("low_d", "Low(NTD)"),
    ("close_d", "Close(NTD)"), ("volume", "Volume(1000S)"), ("amount", "Amount(NTD1000)"),
    ("roi", "ROI%"), ("turnover", "Turn Over%"), ("outstanding", "Shares(1000S)"),
    ("mv", "Market Cap.(NTD MN)"), ("bid", "Last Bid(NTD)"), ("offer", "Last Offer(NTD)"),
    ("roib", "ROI%-Ln"), ("mv_pct", "MV%"), ("amt_pct", "Amount%"),
    ("trn_d", "Transaction"), ("per_tse", "P/E-TSE"), ("per_tej", "P/E-TEJ"),
    ("pbr_tse", "P/B-TSE"), ("pbr_tej", "P/B-TEJ"), ("limit", "LIMIT"),
    ("tej_psr", "P/S-TEJ"), ("div_yid", "Dividend_Yield%"), ("tej_cdiv", "Cash_Dividend%"),
    ("clschg", "Price_Change(NTD)"), ("hmlpct", "High minus Low %"),
    ("refprc", "Next Ref. Price"), ("u_limit", "Next Limit Up"), ("d_limit", "Next Limit Down"),
    ("xattn1", "Attention (A)"), ("xattn2", "Disposition (D)"),
    ("xstat1", "Full delivery"), ("pmkt", "Market"),
)
# TAPRCD.mv is labelled "百萬元" but its native unit is NTD,T. Do not
# silently resolve that scale conflict using a matching Wizard field name.
UNIT_AMBIGUOUS_FIELDS = frozenset({'mv'})
EXPECTED_UNITS = {
    'open_d': 'NTD', 'high_d': 'NTD', 'low_d': 'NTD', 'close_d': 'NTD',
    'volume': 'T', 'amount': 'NTD,T', 'outstanding': 'T', 'bid': 'NTD', 'offer': 'NTD',
    'roi': '%', 'turnover': '%', 'mv_pct': '%', 'amt_pct': '%', 'div_yid': '%',
    'tej_cdiv': '%', 'hmlpct': '%',
}

# These are table-level candidates only. A pair never grants whole-table
# exclusion: financial long/wide, third keys and snapshot ages differ.
CANDIDATE_PAIRS = {
    "AIND": ("TEJ Company DB", "TSE/OTC Company Attribute"),
    "TAMT": ("TEJ Company DB", "Shareholder Meeting"),
    "TASALE": ("TEJ Company DB", "Monthly Sales & Earnings"),
    "TAGIN": ("TEJ Trading analyze", "Margin Trading"),
    "TAPRCD": (WIZARD_SMART, WIZARD_TABLE),
    "TATINST1": ("TEJ Trading analyze", "Qfii/Dealer/Invest. Buy/Sell"),
    "TAATT": ("TEJ FUND DB", "Fund's Attribute"),
    "TANAV": ("TEJ FUND DB", "Fund's NAV(ID)"),
    "TAOFATT": ("TEJ FUND DB", "Offshore Fund's Attribute"),
    "TAOFCAN": ("TEJ FUND DB", "Offshore Fund's Dividends"),
    "TAOFIVA": ("TEJ FUND DB", "Offshore Fund's Holding-Area"),
    "TAOFMNV": ("TEJ FUND DB", "Offshore Fund's NAV(By Ranking)"),
    "TAOFNAV": ("TEJ FUND DB", "Offshore Fund's Nav"),
    "TAOFSUSP": ("TEJ FUND DB", "Offshore Fund's Attribute Suspended Records"),
    "TAOFUNDS": ("TEJ FUND DB", "Offshore Fund's Ranking"),
    "TAIM1A": ("TEJ IFRS Finance-New Accounting Principle", "IFRS_TEJ Consolidated First(Acc)-ALL"),
    "TAIM1AA": ("TEJ IFRS Finance-New Accounting Principle", "IFRS_TEJ Consolidated First(Acc)-ALL"),
    "TAIM1AQ": ("TEJ IFRS Finance-New Accounting Principle", "IFRS_TEJ Consolidated First(Qly)-ALL"),
    "TAIM1AQA": ("TEJ IFRS Finance-New Accounting Principle", "IFRS_TEJ Consolidated First(Qly)-ALL"),
    "TAFUTR": ("TEJ Derivatives", "Future DB"),
    "TAOPBAS": ("TEJ Derivatives", "Option Attribute"),
    "TAOPTION": ("TEJ Derivatives", "Option"),
}


def write_allocation_plan(catalog: dict, wizard_root: Path, output: Path) -> dict:
    from downloader.tej_api_catalog import write_csv
    with sqlite3.connect((wizard_root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True) as con:
        definitions = {(smart, name): (identifier, json.loads(fields)) for identifier, smart, name, fields in
                       con.execute("SELECT table_id,smart_id,name,fields_json FROM tables")}
    rows = []
    for table in catalog["tables"]:
        candidate = CANDIDATE_PAIRS.get(table["tableId"].split("/")[1])
        found = definitions.get(candidate) if candidate else None
        reviewed = table["tableId"] == API_TABLE and found and all(pair[1] in found[1] for pair in FIELD_PAIRS)
        rows.append({"api_table": table["tableId"], "api_name": str(table["cName"]).strip(),
                     "api_owner": "native_trial_API", "actual_first": table["actual_first"],
                     "actual_last": table["actual_last"], "api_access_state": table["access_state"],
                     "wizard_candidate_smart_id": candidate[0] if candidate else "",
                     "wizard_candidate_table": candidate[1] if candidate else "",
                     "wizard_candidate_table_id": found[0] if found else "",
                     "exclusion_contract": CONTRACT if reviewed else "semantic_key_unit_review_required",
                     "reviewed_field_pairs": len(FIELD_PAIRS) if reviewed else 0,
                     "unit_safe_candidate_fields": len(FIELD_PAIRS) - len(UNIT_AMBIGUOUS_FIELDS) if reviewed else 0,
                     "ambiguous_unit_fields_retained_on_wizard": ','.join(sorted(UNIT_AMBIGUOUS_FIELDS)) if reviewed else '',
                     "exclusion_scope": "receipt_proven_nonnull_symbol_date_fields_only" if reviewed else "none_unverified",
                     "wizard_older_newer_uncovered_history_retained": True})
    write_csv(output / "source_allocation_plan.csv", rows)
    write_csv(output / "reviewed_daily_price_field_pairs.csv", [{"api_table": API_TABLE, "api_field": code,
        "wizard_smart_id": WIZARD_SMART, "wizard_table": WIZARD_TABLE, "wizard_field": name,
        "contract": CONTRACT, 'exclusion_enabled': code not in UNIT_AMBIGUOUS_FIELDS,
        'unit_review_state': 'conflicting_scale_requires_value_verification' if code in UNIT_AMBIGUOUS_FIELDS
                             else 'source_metadata_and_wizard_definition_reviewed'} for code, name in FIELD_PAIRS])
    return {"api_tables": len(rows), "wizard_candidate_tables_found": sum(bool(r["wizard_candidate_table_id"]) for r in rows),
            "reviewed_native_daily_price_fields": len(FIELD_PAIRS),
            "unit_safe_daily_price_fields": len(FIELD_PAIRS) - len(UNIT_AMBIGUOUS_FIELDS),
            "whole_table_exclusions": 0}


def reindex_price_receipts(api_root: Path) -> dict:
    from decimal import Decimal
    from downloader.tej_api import connect
    with closing(connect(api_root)) as con:
        pages = list(con.execute("SELECT receipt_path FROM pages WHERE table_id=?", (API_TABLE,)))
    rows = 0
    for page in pages:
        receipt = json.loads((api_root / page[0]).read_text())
        body = (api_root / receipt["raw_path"]).read_bytes()
        if hashlib.sha256(body).hexdigest() != receipt["raw_sha256"]:
            raise ValueError("Native API receipt hash mismatch")
        payload = json.loads(body, parse_float=Decimal)
        item = payload["datatable"]
        rows += record_price_coverage(api_root, receipt, item["data"], item["columns"])
    return {"price_pages_audited": len(pages), "price_rows_indexed_including_duplicate_probes": rows,
            "provider_requests_sent": 0}


def enable_exact_allocation(wizard_root: Path) -> dict:
    api_root = wizard_root / "api_trial_v1"
    indexed = reindex_price_receipts(api_root)
    if not indexed["price_rows_indexed_including_duplicate_probes"]:
        raise ValueError("No verified API price facts; preserve all Wizard work")
    policy = {"contract": CONTRACT, "enabled": True, "whole_table_exclusions": False,
              "excluded_scope": "receipt_proven_nonnull_symbol_date_field_facts_only",
              "reviewed_api_table": API_TABLE, "reviewed_field_pairs": len(FIELD_PAIRS),
              "unit_safe_daily_price_fields": len(FIELD_PAIRS) - len(UNIT_AMBIGUOUS_FIELDS),
              "ambiguous_unit_fields_retained_on_wizard": sorted(UNIT_AMBIGUOUS_FIELDS),
              "observed_at_utc": datetime.now(UTC).isoformat(), "unknown_coverage_fallback": "Smart Wizard"}
    atomic_write_json(wizard_root / "api_source_allocation.json", policy)
    return policy


def record_price_coverage(api_root: Path, receipt: dict, rows: list, columns: list) -> int:
    if receipt.get("table_id") != API_TABLE:
        return 0
    names = [c["name"] for c in columns]
    positions = {name: index for index, name in enumerate(names)}
    if not {"coid", "mdate", *(pair[0] for pair in FIELD_PAIRS)} <= set(names):
        return 0  # A scope probe / projected subset is not all-field proof.
    # Field-code identity alone cannot silently survive vendor unit changes.
    units = {c["name"]: c.get("unit") for c in columns}
    eligible = {code for code, _ in FIELD_PAIRS if code not in UNIT_AMBIGUOUS_FIELDS
                and units.get(code) == EXPECTED_UNITS.get(code, '-')}
    from downloader.tej_api import connect
    coverage = []
    for row in rows:
        symbol, period = row[positions["coid"]], row[positions["mdate"]]
        if not isinstance(symbol, str) or not isinstance(period, str) or len(period) < 10:
            continue
        try:
            from datetime import date
            period = date.fromisoformat(period[:10]).isoformat()
        except ValueError:
            continue
        mask = sum(1 << index for index, pair in enumerate(FIELD_PAIRS)
                   if pair[0] in eligible and row[positions[pair[0]]] is not None
                   and pair[0] not in receipt.get("physical_string_fallback_fields", []))
        digest = hashlib.sha256(json.dumps(row, default=str, ensure_ascii=False).encode()).hexdigest()
        coverage.append((API_TABLE, symbol, period, mask, receipt["request_id"], digest))
    with closing(connect(api_root)) as con, con:
        con.executescript("""
          CREATE TABLE IF NOT EXISTS native_coverage(
            api_table TEXT,coid TEXT,period TEXT,field_mask INTEGER,request_id TEXT,row_sha256 TEXT,
            PRIMARY KEY(api_table,coid,period));
        """)
        saved = con.execute("SELECT value FROM meta WHERE key='native_coverage_contract'").fetchone()
        if not saved or saved[0] != CONTRACT:
            # Derived index only; source observations and receipts remain.
            # Clear stale eligibility under one transaction before rebuilding.
            con.execute('UPDATE native_coverage SET field_mask=0')
            con.execute("INSERT OR REPLACE INTO meta VALUES('native_coverage_contract',?)", (CONTRACT,))
        for item in coverage:
            # Conflicting revisions are not falsely called the same value.
            con.execute("INSERT INTO native_coverage VALUES(?,?,?,?,?,?) ON CONFLICT(api_table,coid,period) "
                        "DO UPDATE SET field_mask=CASE WHEN row_sha256=excluded.row_sha256 "
                        "THEN field_mask|excluded.field_mask ELSE 0 END, "
                        "row_sha256=CASE WHEN row_sha256=excluded.row_sha256 THEN row_sha256 ELSE NULL END", item)
    return len(coverage)


def remaining_rectangles(request: dict, facts: dict[tuple[str, str], int]) -> list[dict] | None:
    """Pure bounded scope subtraction. None means no change, [] fully delegated."""
    from downloader.tej_history import company_code, normalize_period
    from downloader.tej_key_layout import key_count
    if (request.get("smart_id") != WIZARD_SMART or request.get("table") != WIZARD_TABLE
            or request.get("source_key_mode", 2) != 2
            or request.get("frequency") != "daily" or key_count(request) != 2):
        return None
    fields = request["fields"]
    field_indices = {name: index for index, (_, name) in enumerate(FIELD_PAIRS)}
    companies, dates = request["company_labels"], request["date_labels"]
    codes, periods = [company_code(c) for c in companies], [normalize_period(d) for d in dates]
    if any(len(p) != 10 for p in periods):
        return None
    patterns = []
    excluded = 0
    for field in fields:
        bit = 1 << field_indices[field] if field in field_indices else 0
        pattern = tuple(tuple(index for index, period in enumerate(periods)
                              if not facts.get((code, period), 0) & bit) for code in codes)
        excluded += len(companies) * len(dates) - sum(len(p) for p in pattern)
        patterns.append(pattern)
    if not excluded:
        return None
    results = []
    begin = 0
    while begin < len(fields):
        end = begin + 1
        while end < len(fields) and patterns[end] == patterns[begin]:
            end += 1
        groups = defaultdict(list)
        for index, pattern in enumerate(patterns[begin]):
            if pattern:
                groups[pattern].append(companies[index])
        for indices, company_labels in groups.items():
            selected = [dates[index] for index in indices]
            full = request.get("catalog_fields", fields)
            base = full.index(fields[begin])
            if full[base:base + end - begin] != fields[begin:end]:
                raise ValueError("Non-contiguous Wizard field scope")
            from downloader.tej_planning import CONTRACT as PLAN
            results.append({**request, "fields": fields[begin:end], "company_labels": company_labels,
                            "date_labels": selected,
                            "field_partition": {"contract": PLAN, "field_range": [base, base + end - begin],
                                                "catalog_fields": len(full)}})
        begin = end
    return results


def repartition_pending(con, root: Path, row) -> bool:
    """Inside the canonical queue transaction, before any desktop submission."""
    policy_path = root / "api_source_allocation.json"
    if row["kind"] != "download" or not policy_path.is_file():
        return False
    policy = json.loads(policy_path.read_text())
    if policy.get("contract") != CONTRACT or policy.get("enabled") is not True:
        return False
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, expand_request, compact_request, stable_id
    if row["state"] != "pending" or row["scope_contract"] != SOURCE_SCOPE_CONTRACT:
        return False
    definition = dict(con.execute("SELECT * FROM tables WHERE table_id=?", (row["table_id"],)).fetchone())
    if definition["smart_id"] != WIZARD_SMART or definition["name"] != WIZARD_TABLE:
        return False
    request = expand_request(json.loads(row["request_json"]), definition, row["table_id"])
    from downloader.tej_history import company_code, normalize_period
    companies = [company_code(label) for label in request["company_labels"]]
    periods = [normalize_period(label) for label in request["date_labels"]]
    if not companies or not periods:
        return False
    database = root / "api_trial_v1/queue.sqlite3"
    if not database.is_file():
        return False
    try:
        with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True, timeout=1) as source:
            if not source.execute("SELECT 1 FROM sqlite_master WHERE name='native_coverage'").fetchone():
                return False
            placeholders = ",".join("?" for _ in companies)
            facts = {(symbol, period): mask for symbol, period, mask in source.execute(
                "SELECT coid,period,field_mask FROM native_coverage WHERE api_table=? AND coid IN (" + placeholders + ") "
                "AND period>=? AND period<=?", (API_TABLE, *companies, min(periods), max(periods)))}
    except sqlite3.Error:
        return False  # No reliable API proof means preserve the Wizard request.
    parts = remaining_rectangles(request, facts)
    if parts is None:
        return False
    audit = {"contract": CONTRACT, "original_task_id": row["task_id"],
             "api_table": API_TABLE, "observed_at_utc": datetime.now(UTC).isoformat(),
             "original_request": request, "remaining_requests": parts,
             "api_source_representation": "native_api_values", "fake_wizard_receipts": False}
    previous_work = row["work_expected_rows"] or row["expected_rows"]
    remaining_work = sum(len(part["company_labels"]) * len(part["date_labels"]) for part in parts)
    audit.update(original_work_rows=previous_work, remaining_work_rows=remaining_work,
                 excluded_native_cells=len(request["fields"]) * len(request["company_labels"]) * len(request["date_labels"])
                    - sum(len(p["fields"]) * len(p["company_labels"]) * len(p["date_labels"]) for p in parts))
    audit_path = root / "source_allocation" / (row["task_id"] + ".json")
    atomic_write_json(audit_path, audit)
    con.execute("UPDATE tasks SET state='superseded_exact_api_scope_v1' WHERE task_id=?", (row["task_id"],))
    # Adjust only the desktop-work denominator, never count API rows as Wizard
    # successes. Splitting field groups can increase the number of Preview rows.
    con.execute("UPDATE tables SET work_grid_rows=MAX(0,work_grid_rows+?) WHERE table_id=?",
                (remaining_work - previous_work, row["table_id"]))
    for part in parts:
        identifier = stable_id([row["table_id"], part])
        expected = len(part["company_labels"]) * len(part["date_labels"])
        con.execute("INSERT OR IGNORE INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract,work_expected_rows) "
                    "VALUES(?,?,'download',?,?,?,?,?,?)", (identifier, row["table_id"], row["phase"], row["priority"],
                        json.dumps(compact_request(part, definition), ensure_ascii=False), expected, SOURCE_SCOPE_CONTRACT, expected))
    return True
