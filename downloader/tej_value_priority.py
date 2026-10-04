"""Local-gap-first acquisition value on the two existing TEJ queues.

Only ordering changes. Concept aliases and date bounds are priority hints, NOT
key/unit equivalence, download exclusions, PIT eligibility or proven alpha.
Exact API/Wizard fact subtraction remains in tej_api_ownership. No source API,
desktop query, imputation or source-state reset is performed by this module.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file

CONTRACT = "tej_local_gap_value_priority_v1"
API_CATEGORIES = {
    "AIND": "company_lifecycle", "TAMT": "corporate_actions", "TASALE": "monthly_revenue",
    "TAGIN": "stock_margin", "TAPRCD": "stock_daily", "TAQFII": "stock_flow",
    "TATINST1": "stock_flow", "TAIACC": "reference", "TAFUTR": "futures",
    "TAOPBAS": "options", "TAOPTION": "options",
    "TAIM1A": "financial", "TAIM1AQ": "financial",
    "TAIM1AA": "financial", "TAIM1AQA": "financial",
    "TAAPRRENT": "real_estate", "TAAPRTRAN": "real_estate", "TALANDTR": "real_estate",
}


def validate_policy(policy: dict) -> None:
    date.fromisoformat(policy["target_start"])
    if (policy.get("contract") != CONTRACT or type(policy.get("refresh_seconds")) is not int
            or not 60 <= policy["refresh_seconds"] <= 86400
            or not isinstance(policy.get("family_values"), dict)
            or not policy["family_values"]
            or any(type(v) is not int or not 0 <= v <= 100 for v in policy["family_values"].values())):
        raise ValueError("Unreviewed TEJ acquisition value policy")


def feature_value(row: dict, policy: dict) -> tuple[int, str]:
    """Transparent ordinal engineering utility; not statistical feature importance."""
    family, table, field = row["category"], row["table"], row["field"]
    text = f"{table} {field}".casefold()
    value = policy["family_values"].get(family, policy["family_values"].get("other", 38))
    reason = "family_research_relevance"
    # A fund dividend/portfolio is not a cash-stock action/futures contract.
    if "fund" in table.casefold() and family != "macro_banking":
        value, reason = min(value, 45), "fund_auxiliary_not_stock_or_futures_rule"
    elif family in {"futures", "options", "stock_daily", "corporate_actions", "events", "company_lifecycle"}:
        if re.search(r"contract adjustment|margin|position.*limit|limit.*position|契約調整|保證金|部位上限", text):
            value, reason = 100, "execution_risk_or_contract_adjustment"
        elif re.search(r"suspend|delist|listing.*date|listed.*date|停牌|復牌|下市|上市日", text):
            value, reason = 98, "dated_lifecycle_or_tradability"
        elif family == "corporate_actions" and re.search(r"ex_right|cash dividend|treasury|除權|除息|減資", text):
            value, reason = 96, "corporate_action_or_share_supply"
        elif family in {"futures", "options"} and re.search(r"attribute|interest|settle|\boi\b|\bdsp\b|volatil|delta|gamma|vega|theta|basis", text):
            value, reason = 95, "derivative_specific_state_or_contract_context"
    if re.search(r"announc|publication|publish|release.*date|申報|公告日|發布日", field, re.I):
        value, reason = min(96, value + 8), "availability_clock_with_family_relevance"
    if family == "financial" and re.search(r"cash flow|cashflow|operating income|\beps\b|revenue|現金流|營業利益|每股盈餘", field, re.I):
        value, reason = max(value, 90), "financial_cashflow_earnings_or_revenue"
    if (row.get("source_field") in {"coid", "mdate", "id", "name", "mname"}
            or re.fullmatch(r"(?:company |stock |security )?(?:code|name)|YYYY/MM|YYYY/MM/DD|公司代碼|證券代碼|證券名稱|期貨名稱|基金碼|日期", field, re.I)):
        value, reason = min(value, 25), "join_key_or_period_bundled_with_observations"
    closed_range = re.search(r"\((\d{4})~(\d{4})\)|before (\d{4})", table, re.I)
    if closed_range:
        upper = int(closed_range[2]) if closed_range[2] else int(closed_range[3]) - 1
        if upper < int(policy["target_start"][:4]):
            value, reason = min(value, 50), "outside_primary_research_horizon_retained"
    # Retain every source field, including derived logs; raw-input preference
    # changes acquisition order only, not source values or the training ABI.
    if re.search(r"ROI%-Ln|logarithm|\blog\b", field, re.I):
        value, reason = min(value, 20), "source_log_derived_raw_input_not_preferred"
    return value, reason


def coverage_hint(row: dict, local_by_id: dict[str, dict], *, start: str,
                  end: str | None = None) -> tuple[int, str]:
    """Unknown scope is work, not proof of absence or permission to skip it.

    Require ONE same-family/cadence observation to span the target; never union
    unrelated providers' min/max, count an all-null schema as acquired, or fill
    a gap with another market/grain. Even the covered bucket is a candidate.
    """
    ids = row.get("local_match_ids", [])
    if not ids:
        return 0, "not_observed_in_local_catalog_candidate"
    comparable = [local_by_id[i] for i in row.get("same_family_cadence_match_ids", [])
                  if i in local_by_id]
    if not comparable:
        return 0, "concept_seen_grain_or_scope_unverified"
    positive = [r for r in comparable if type(r.get("non_null_count")) is int and r["non_null_count"] > 0]
    if not positive:
        return 0, "null_or_unprofiled_history_requires_work"
    for item in positive:
        if item.get("first") and item["first"] <= start and item.get("last") and (end is None or item["last"] >= end):
            return 1, "observed_concept_bounds_crosscheck_candidate_not_complete"
    return 0, "observed_concept_history_or_field_bounds_gap"


def rank_feature(row: dict, local_by_id: dict[str, dict], policy: dict,
                 *, end: str | None = None) -> dict:
    # A trial slice cannot repair years outside its actual scope. Do not
    # promote a duplicate 2025 query merely because another source lacks 2014.
    start = max(policy["target_start"], (row.get("actual_first") or "")[:10])
    bucket, coverage = coverage_hint(row, local_by_id, start=start, end=end)
    score, reason = feature_value(row, policy)
    # All gap work precedes all cross-check candidates, even a cheap small
    # auxiliary table. Value precedes names, row volume and request cost.
    priority = bucket * 20000 + (100 - score) * 100
    return {**row, "priority": priority, "value_score": score, "value_basis": reason,
            "local_coverage_state": coverage, "acquisition_stage": "missing_or_gap" if bucket == 0 else "crosscheck_candidate",
            "coverage_is_complete": False, "exclusion_enabled_by_ranking": False}


def summarize_table(channel: str, table: dict, fields: list[dict]) -> dict:
    best = min(fields, key=lambda r: (r["priority"], r["field_index"])) if fields else None
    return {"channel": channel, "table_id": table["table_id"], "table": table["table"],
            "category": table["category"], "fields": len(fields),
            "priority": best["priority"] if best else 40000,
            "value_score": best["value_score"] if best else 0,
            "value_basis": best["value_basis"] if best else "empty_schema",
            "priority_field": best["field"] if best else "",
            "missing_or_gap_fields": sum(f["acquisition_stage"] == "missing_or_gap" for f in fields),
            "crosscheck_candidate_fields": sum(f["acquisition_stage"] == "crosscheck_candidate" for f in fields),
            "local_coverage_state": best["local_coverage_state"] if best else "empty_schema",
            "batching": "native_table_fields_bundled_no_second_request_for_context",
            "whole_table_exclusion": False}


def build_ranking(repo: Path, root: Path, policy: dict) -> tuple[dict, list[dict], list[dict]]:
    """Reuse canonical receipt/schema inventory; never scan historical arrays."""
    from scripts.build_tej_smart_wizard_inventory import local_features, worklist
    from downloader.tej_api_ownership import FIELD_PAIRS

    validate_policy(policy)
    local, evidence = local_features(repo)
    # Keep global macro/FX context, but a US stock or crypto candle is not
    # Taiwan stock/TAIFEX coverage merely because it has a column called close.
    excluded_markets = [r for r in local if r['category'] in {
        'stock_daily', 'stock_adjusted', 'stock_flow', 'stock_margin', 'stock_lending',
        'stock_daytrade', 'financial', 'monthly_revenue', 'futures', 'options'} and re.search(
        r'crypto|binance|bybit|okx|usstock|us_stock|us_stocks|hkstock|hk_stock',
        r['dataset'] + ' ' + r['provider'], re.I)]
    excluded_ids = {r['local_id'] for r in excluded_markets}
    comparable_local = [r for r in local if r['local_id'] not in excluded_ids]
    local_by_id = {r["local_id"]: r for r in local}
    with sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True) as con:
        con.row_factory = sqlite3.Row
        con.execute("BEGIN")
        definitions = [dict(r) for r in con.execute("SELECT * FROM tables")]
        own = {r["feature_id"]: dict(r) for r in con.execute("SELECT feature_id,exported_non_null_cells,first_query_period,last_query_period FROM features")}
    bindings = [{"type": t["query_type"], "smart_id": t["smart_id"], "table": t["name"],
                 "fields": json.loads(t["fields_json"])} for t in definitions if json.loads(t["fields_json"])]
    candidates = worklist(bindings, comparable_local, start=policy["target_start"])
    fields, grouped = [], defaultdict(list)
    for row in candidates:
        ranked = rank_feature(row, local_by_id, policy)
        ranked.update(channel="smart_wizard", **own.get(row["feature_id"], {}))
        # Partial own exports are visible, not upgraded to all-symbol history.
        if (ranked.get('exported_non_null_cells') or 0) > 0 and ranked['acquisition_stage'] == 'missing_or_gap':
            ranked['local_coverage_state'] = 'own_tej_export_observed_remaining_scope_unverified'
        fields.append(ranked)
        grouped[row["table_id"]].append(ranked)
    tables = [summarize_table("smart_wizard", {"table_id": t["table_id"], "table": t["name"],
              "category": t["category"]}, grouped[t["table_id"]]) for t in definitions]

    catalog_path = (repo / policy["api_catalog"]).resolve()
    if not catalog_path.is_relative_to(repo.resolve()):
        raise ValueError("API catalog escapes repository")
    catalog = json.loads(catalog_path.read_text())
    price_names = dict(FIELD_PAIRS)
    financial_concepts = []
    # Long-form value rows contain accounting concepts, not one generic value
    # feature. Reuse the complete native reference dictionary (no API call).
    reference = catalog_path.parent / "financial_account_fields.csv"
    if reference.is_file():
        from downloader.tej_history import read_csv
        financial_concepts = read_csv(reference)
        evidence.append({"path": str(reference.relative_to(repo)), "sha256": sha256_file(reference)})
    from scripts.build_tej_smart_wizard_inventory import normalize, ALIASES
    local_index = defaultdict(list)
    for item in comparable_local:
        local_index[(item["category"], normalize(item["field"]))].append(item)
        if item.get("_alias_code"):
            local_index[(item["category"], normalize(item["_alias_code"]))].append(item)
    for table in catalog["tables"]:
        tid, code = table["tableId"], table["tableId"].split("/")[1]
        family = API_CATEGORIES.get(code, "funds_bonds")
        labels = [price_names.get(c["name"], c.get("cname") or c["name"]) if code == "TAPRCD"
                  else c.get("cname") or c["name"] for c in table["columns"]]
        # worklist's existing alias engine requires its native family/cadence.
        members = []
        source_fields = [(c["name"], label, c.get("unit"), "native_schema") for c, label in zip(table["columns"], labels)]
        source_fields += [(c["acc_code"], c["label_en"] or c["label_zh"], c["source_unit_code"], "accounting_concept")
                          for c in financial_concepts if c["financial_table"] == tid]
        for index, (source_code, label, unit, kind) in enumerate(source_fields):
            targets = [label, *ALIASES.get(label, [])]
            matches = {r["local_id"]: r for target in targets for r in local_index[(family, normalize(target))]}
            frequency = table["frequency"]
            grain = "quarterly" if frequency.startswith("quarterly") else frequency
            grain = "snapshot" if grain == "current_snapshot" else grain
            row = {"channel": "api", "table_id": tid, "table": str(table["cName"]).strip(),
                   "category": family, "field": label, "source_field": source_code, "field_index": index,
                   "field_kind": kind, "cadence": grain, "source_unit": unit,
                   "local_match_ids": sorted(matches),
                   "same_family_cadence_match_ids": sorted(k for k, v in matches.items() if v["cadence"] == grain),
                   "actual_first": table.get("actual_first"), "actual_last": table.get("actual_last"),
                   "access_state": table.get("access_state")}
            ranked = rank_feature(row, local_by_id, policy, end=(table.get("actual_last") or "")[:10] or None)
            members.append(ranked)
            fields.append(ranked)
        item = summarize_table("api", {"table_id": tid, "table": str(table["cName"]).strip(), "category": family}, members)
        # Snapshot metadata and dictionary rows are useful join context, not
        # primary history. An empty verified query cannot create features.
        if frequency in {"current_snapshot", "reference"}:
            item["priority"] = max(item["priority"], 15000)
            item["value_basis"] = "reference_or_snapshot_not_historical_observations"
        if table.get("access_state") == "verified_empty_query":
            item["priority"] = 40000
            item["value_basis"] = "verified_empty_scope_no_observations"
        tables.append(item)
    fields.sort(key=lambda r: (r["priority"], r["channel"], r["table"], r["field_index"]))
    fields = [{"rank": i, **r} for i, r in enumerate(fields, 1)]
    tables.sort(key=lambda r: (r["priority"], r["channel"], r["table"]))
    tables = [{"rank": i, **r} for i, r in enumerate(tables, 1)]
    observed = datetime.now(UTC)
    snapshot = {"contract": CONTRACT, "generated_at_utc": observed.isoformat(), "policy": policy,
                "next_refresh_at_utc": (observed + timedelta(seconds=policy["refresh_seconds"])).isoformat(),
                "local_features_examined": len(local), "local_providers": dict(Counter(r["provider"] for r in local)),
                "other_market_feature_hints_excluded": len(excluded_markets),
                "source_evidence": evidence, "api_catalog_sha256": sha256_file(catalog_path),
                "field_stage_counts": dict(Counter(r["acquisition_stage"] for r in fields)),
                "tables": tables, "scope_changed": False, "provider_requests_sent": 0,
                "value_is_measured_alpha": False, "local_bounds_certify_completeness": False}
    snapshot["fingerprint"] = hashlib.sha256(json.dumps(snapshot, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return snapshot, fields, local


def installed_priority(con, table_id: str, default: int, *, kind: str = "download") -> int:
    """Indexed lookup for newly discovered/lazy tasks; old queues remain valid."""
    if default < 0:
        return default  # Explicit finite operand repair keeps its scoped priority.
    try:
        row = con.execute("SELECT priority FROM acquisition_value_priorities WHERE table_id=?", (table_id,)).fetchone()
    except sqlite3.OperationalError as exc:
        if "no such table" not in str(exc):
            raise
        return default
    return row[0] + (kind == "discover") if row else default


def install_priorities(con, snapshot: dict, channel: str) -> dict:
    """One metadata transaction. Completed/running/blocked source states stay intact."""
    if snapshot.get("contract") != CONTRACT or channel not in {"smart_wizard", "api"}:
        raise ValueError("Invalid value ranking snapshot")
    scope_columns = ('task_id,table_id,kind,state,request_json,scope_contract' if channel == 'smart_wizard'
                     else 'id,table_id,state,params_json,cursor,query_rows,actual_rows,page_size')
    def scope_identity():
        digest, count = hashlib.sha256(), 0
        order_key = 'task_id' if channel == 'smart_wizard' else 'id'
        for row in con.execute('SELECT ' + scope_columns + " FROM tasks WHERE state IN ('pending','retry','running','complete','blocked') ORDER BY " + order_key):
            digest.update(json.dumps(tuple(row), ensure_ascii=False, separators=(',', ':')).encode())
            digest.update(b'\n')
            count += 1
        return digest.hexdigest(), count
    before_scope = scope_identity()
    con.execute("CREATE TABLE IF NOT EXISTS acquisition_value_priorities (table_id TEXT PRIMARY KEY,priority INTEGER NOT NULL,rank INTEGER NOT NULL,value_score INTEGER NOT NULL,missing_fields INTEGER NOT NULL,crosscheck_fields INTEGER NOT NULL,reason TEXT NOT NULL)")
    own = [r for r in snapshot["tables"] if r["channel"] == channel]
    con.executemany("INSERT OR REPLACE INTO acquisition_value_priorities VALUES(?,?,?,?,?,?,?)",
        [(r["table_id"], r["priority"], r["rank"], r["value_score"], r["missing_or_gap_fields"],
          r["crosscheck_candidate_fields"], r["value_basis"]) for r in own])
    if channel == "smart_wizard":
        changed = con.execute("UPDATE tasks SET priority=(SELECT priority FROM acquisition_value_priorities p WHERE p.table_id=tasks.table_id)+(kind='discover') WHERE state='pending' AND priority>=0 AND table_id IN (SELECT table_id FROM acquisition_value_priorities)").rowcount
        con.execute("UPDATE download_plans SET priority=(SELECT priority FROM acquisition_value_priorities p WHERE p.table_id=download_plans.table_id) WHERE table_id IN (SELECT table_id FROM acquisition_value_priorities)")
    else:
        changed = con.execute("UPDATE tasks SET priority=(SELECT priority FROM acquisition_value_priorities p WHERE p.table_id=tasks.table_id) WHERE state IN ('pending','retry') AND priority>=0 AND table_id IN (SELECT table_id FROM acquisition_value_priorities)").rowcount
    meta = {"contract": CONTRACT, "fingerprint": snapshot["fingerprint"],
            "generated_at_utc": snapshot["generated_at_utc"], "next_refresh_at_utc": snapshot["next_refresh_at_utc"],
            "channel": channel, "tables": len(own), "states_or_scope_changed": False}
    if scope_identity() != before_scope:
        raise ValueError('Ordering changed source scope, saved cursor or lifecycle; rollback required')
    con.execute("INSERT OR REPLACE INTO meta VALUES('acquisition_value_priority',?)", (json.dumps(meta),))
    return {**meta, "pending_priorities_updated": changed, "audited_source_tasks": before_scope[1],
            "preserved_source_identity_sha256": before_scope[0], "source_identity_verified_unchanged": True}


def apply_snapshot(root: Path, snapshot: dict) -> list[dict]:
    """Apply serial, short SQLite writes; no second GUI/API request owner."""
    results = []
    for channel, db in (("smart_wizard", root / "queue.sqlite3"), ("api", root / "api_trial_v1/queue.sqlite3")):
        if not db.is_file():
            continue
        with closing(sqlite3.connect(db, timeout=5)) as con:
            con.execute("BEGIN IMMEDIATE")
            results.append(install_priorities(con, snapshot, channel))
            con.commit()
    atomic_write_json(root / "value_priority.json", snapshot, durable=True)
    return results


def refresh_if_due(root: Path, *, now: datetime | None = None) -> dict | None:
    """Opt-in, local metadata refresh at task boundaries; no per-call full scan."""
    path = root / "value_priority.json"
    if not path.is_file():
        return None
    now = now or datetime.now(UTC)
    status_path = root / "value_priority_refresh_status.json"
    if status_path.is_file():
        try:
            previous = json.loads(status_path.read_text())
            retry = previous.get("next_retry_at_utc")
            if retry and now < datetime.fromisoformat(retry):
                return None
        except (OSError, ValueError, TypeError, AttributeError):
            pass  # A diagnostic file cannot become a source admission gate.
    try:
        snapshot = json.loads(path.read_text())
        if snapshot.get("contract") != CONTRACT:
            raise ValueError("Incompatible acquisition value snapshot")
        if now < datetime.fromisoformat(snapshot["next_refresh_at_utc"]):
            return None
        fresh, _, _ = build_ranking(root.parent, root, snapshot["policy"])
        result = {"state": "local_value_priorities_refreshed", "channels": apply_snapshot(root, fresh),
                  "provider_requests_sent": 0}
    except (OSError, ValueError, KeyError, TypeError, AttributeError, sqlite3.Error):
        # A ranking metadata failure must not stop the healthy downloader or
        # turn into a per-query catalog scan. Retain the last approved order.
        result = {"state": "local_priority_refresh_deferred", "existing_order_retained": True,
                  "provider_requests_sent": 0, "error_code": "local_inventory_unavailable",
                  "next_retry_at_utc": (now + timedelta(minutes=15)).isoformat()}
    try:
        atomic_write_json(status_path, result)
    except OSError:
        pass  # The canonical collector's own disk/headroom gate stays binding.
    return result
