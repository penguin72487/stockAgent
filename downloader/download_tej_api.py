"""Canonical trial-API CLI; licensed rows remain under data_tej, never public."""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path

from downloader.artifact_io import atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from downloader.tej_api import TejAPIError, TejTrialAPI, connect
from downloader.tej_api_catalog import collect_catalog, publish_catalog, write_csv


def inventory(api: TejTrialAPI, output: Path, *, refresh: bool = False) -> dict:
    account = api.authenticate()
    path = output / "catalog.json"
    catalog = json.loads(path.read_text()) if path.exists() and not refresh else collect_catalog(
        entitled_table_ids=tuple(account["tables"]))
    if set(t["tableId"] for t in catalog["tables"]) != set(account["tables"]):
        raise TejAPIError("catalog_and_current_entitlements_differ")
    catalog.update(account_entitlements_verified=True, licensed_data_requested=True,
                   account_scope_verified_at_utc=api.now().isoformat())
    scopes = []
    for table in catalog["tables"]:
        identifier = table["tableId"]
        metadata_path = api.root / "metadata" / (identifier.split("/")[1] + ".json")
        meta = json.loads(metadata_path.read_text()) if metadata_path.exists() and not refresh else api.metadata(identifier)
        has_date = "mdate" in meta.get("filters", [])
        pages = [api.page(identifier, {"opts.sort": "mdate." + order} if has_date else {}, wanted_rows=1)
                 for order in (("asc", "desc") if has_date else ("asc",))]
        first, last = pages[0]["first_date"], pages[-1]["last_date"]
        state = "verified_nonempty" if all(page["rows"] for page in pages) else "verified_empty_query"
        table.update(actual_first=first, actual_last=last, access_state=state)
        if identifier in {"TRAIL/TAAPRRENT", "TRAIL/TAAPRTRAN", "TRAIL/TALANDTR"}:
            table["frequency"] = "event"
        grant = account["tables"][identifier]
        scopes.append({"table_id": identifier, "name": str(table["cName"]).strip(),
                       "access_state": state, "permission_start_year": grant.get("dataStartYear"),
                       "permission_end_year": grant.get("dataEndYear"),
                       "first_observed": first, "last_observed": last,
                       "primary_keys_declared": json.dumps(meta.get("primaryKey", [])),
                       "native_fields": len(meta.get("columns", [])),
                       "frequency": table["frequency"], "rows_estimate_catalog": table["rowCount"],
                       "history_complete": False, "last_verified_utc": api.now().isoformat()})
        print(json.dumps({"table_id": identifier, "access_state": state, "first": first,
                          "last": last, "licensed_rows_observed": sum(p["rows"] for p in pages)}, ensure_ascii=False), flush=True)
    report = publish_catalog(catalog, output)
    write_csv(output / "account_table_scopes.csv", scopes)
    atomic_write_json(api.root / "scope_inventory.json", {"observed_at_utc": api.now().isoformat(), "tables": scopes})
    report.update(entitled_tables=len(account["tables"]), verified_nonempty=sum(
        s["access_state"] == "verified_nonempty" for s in scopes), licensed_rows_observed_this_inventory=sum(
        2 if s["first_observed"] else 1 for s in scopes if s["access_state"] == "verified_nonempty"))
    atomic_write_json(output / "summary.json", report)
    return report


def _task_id(table: str, params: dict) -> str:
    return hashlib.sha256(json.dumps([table, params], sort_keys=True).encode()).hexdigest()[:32]


def seed_queue(api: TejTrialAPI, catalog: dict) -> dict:
    # Every authorized table gets work, including empty catalogue estimates.
    # Date partitions reduce pagination-cap risk without excluding entities.
    inserted = 0
    from downloader.tej_value_priority import installed_priority
    with closing(connect(api.root)) as con, con:
        for table in catalog["tables"]:
            identifier = table["tableId"]
            if table["access_state"] not in {"verified_nonempty", "verified_empty_query"}:
                raise TejAPIError("actual_table_scope_probe_required")
            first, last = table.get("actual_first"), table.get("actual_last")
            count = int(table.get("rowCount") or 0)
            params_list = [{}]
            if first and last and table["frequency"] != "current_snapshot" and count > 10_000:
                lower, upper = date.fromisoformat(first[:10]), date.fromisoformat(last[:10])
                span = (upper - lower).days + 1
                width = max(1, min(31, 25_000 * span // max(1, count)))
                params_list = []
                while lower <= upper:
                    if table["frequency"].startswith("quarterly") or table["frequency"] == "monthly":
                        jump = 3 if table["frequency"].startswith("quarterly") else 1
                        month_index = lower.year * 12 + lower.month - 1 + jump
                        next_month = date(month_index // 12, month_index % 12 + 1, 1)
                        end = min(upper, next_month - timedelta(days=1))
                    else:
                        end = min(upper, lower + timedelta(days=width - 1))
                    params_list.append({"mdate.gte": lower.isoformat(), "mdate.lte": end.isoformat(),
                                        "opts.sort": "coid.asc"})
                    lower = end + timedelta(days=1)
                if identifier in {"TRAIL/TAIM1A", "TRAIL/TAIM1AQ"}:
                    # Quarter boundaries cluster long-form accounting rows;
                    # shorter day windows alone cannot reduce these bursts.
                    bounds = ["0", *(str(x) for x in range(10, 100))]
                    intervals = [{"coid.lt": bounds[0]},
                                 *({"coid.gte": lo, "coid.lt": hi} for lo, hi in zip(bounds, bounds[1:])),
                                 {"coid.gte": bounds[-1]}]
                    params_list = [{**params, **interval} for params in params_list for interval in intervals]
            priority = 0 if table["frequency"] in {"reference", "current_snapshot"} else 1 if count <= 50_000 else 2
            priority = installed_priority(con, identifier, priority)
            for params in params_list:
                inserted += con.execute("INSERT OR IGNORE INTO tasks(id,table_id,params_json,priority,state) VALUES(?,?,?,?,'pending')",
                    (_task_id(identifier, params), identifier, json.dumps(params, sort_keys=True), priority)).rowcount
    return {"tasks_inserted": inserted, "all_entitled_tables_queued": len(catalog["tables"])}


def _partition_at_cap(con, task: dict, receipt: dict, api: TejTrialAPI) -> str:
    # A capped query is not complete. Partition into explicit disjoint filters;
    # never reset an opaque cursor to evade the per-query limit.
    params = json.loads(task["params_json"])
    low, high = params.get("mdate.gte"), params.get("mdate.lte")
    if low and high and low < high:
        middle = date.fromisoformat(low) + (date.fromisoformat(high) - date.fromisoformat(low)) // 2
        parts = [{**params, "mdate.lte": middle.isoformat()},
                 {**params, "mdate.gte": (middle + timedelta(days=1)).isoformat()}]
    else:
        # Use a returned actual code as the lexicographic boundary. Both
        # halves cover the parent entity interval; no current-stock list can
        # silently drop delisted entities. A single huge entity stays visible.
        import pyarrow.parquet as pq
        if not receipt.get("parquet_path"):
            return "requires_entity_partition"
        frame = pq.read_table(api.root / receipt["parquet_path"], columns=["coid"])
        codes = sorted({value for value in frame.column(0).to_pylist() if isinstance(value, str)})
        if len(codes) < 2:
            return "requires_entity_partition"
        boundary = codes[len(codes) // 2]
        parts = [{**params, "coid.lt": boundary}, {**params, "coid.gte": boundary}]
    for part in parts:
        con.execute("INSERT OR IGNORE INTO tasks(id,table_id,params_json,priority,state) VALUES(?,?,?,?,'pending')",
                    (_task_id(task["table_id"], part), task["table_id"], json.dumps(part, sort_keys=True), task["priority"]))
    return "superseded_disjoint_partition"


def run_queue(api: TejTrialAPI, *, max_pages: int = 500) -> dict:
    from downloader.tej_value_priority import refresh_if_due
    refresh_if_due(api.root.parent)
    api.recover_saved_pages()
    with closing(connect(api.root)) as con:
        before = api.budget(con, api.now())
        cooldown = api.cooldown_until(con)
    if cooldown and cooldown > api.now():
        result = {**status(api), "pages_received_this_run": 0, "reason": "provider_cooldown",
                  "next_attempt_utc": cooldown.isoformat()}
        atomic_write_json(api.root / "worker_status.json", result)
        return result
    if not before["rows_remaining"] or not before["calls_remaining"]:
        result = {**status(api), "pages_received_this_run": 0, "reason": "waiting_quota"}
        atomic_write_json(api.root / "worker_status.json", result)
        return result
    try:
        api.authenticate()
    except TejAPIError as exc:
        result = {**status(api), "pages_received_this_run": 0, "reason": str(exc)}
        atomic_write_json(api.root / "worker_status.json", result)
        return result
    attempted = 0
    reason = "bounded_batch_finished"
    for _ in range(max_pages):
        now = api.now()
        with closing(connect(api.root)) as con:
            # Stored source receipts from an interrupted local commit can be
            # adopted locally; do not repeat a successful provider response.
            for row in con.execute("SELECT * FROM tasks WHERE state='running' OR "
                    "(state='blocked' AND error_code IN ('local_materialization_failed',"
                    "'received_response_requires_local_recovery','pagination_cursor_did_not_advance'))").fetchall():
                page = con.execute("SELECT * FROM pages WHERE request_id=?", (row["active_request_id"],)).fetchone()
                if page:
                    receipt = json.loads((api.root / page["receipt_path"]).read_text())
                    _advance(con, dict(row), receipt, api)
                    con.commit()
                else:
                    received = con.execute("SELECT state FROM requests WHERE id=?", (row["active_request_id"],)).fetchone()
                    if received and received[0] == 'received':
                        con.execute("UPDATE tasks SET state='blocked',error_code='received_response_requires_local_recovery' WHERE id=?", (row["id"],))
                    elif row["state"] == 'running':
                        con.execute("UPDATE tasks SET state='pending' WHERE id=?", (row["id"],))
                    con.commit()
            budget = api.budget(con, now)
            task = con.execute("SELECT * FROM tasks WHERE state IN ('pending','retry') AND "
                "(next_attempt_utc IS NULL OR next_attempt_utc<=?) AND "
                "(cursor IS NULL OR COALESCE(page_size,10000)<=?) ORDER BY priority,table_id,id LIMIT 1",
                (now.isoformat(), budget["rows_remaining"])).fetchone()
            if not task:
                reason = "queue_idle_or_waiting_retry"; break
            task = dict(task)
            if not budget["rows_remaining"] or not budget["calls_remaining"]:
                reason = "waiting_quota"; break
            con.execute("UPDATE tasks SET state='running' WHERE id=?", (task["id"],)); con.commit()
        params = json.loads(task["params_json"])
        if task["cursor"]:
            params["opts.cursor_id"] = task["cursor"]
        try:
            receipt = api.page(task["table_id"], params,
                wanted_rows=min(task['page_size'] or 10_000, 50_000 - task["query_rows"]), task_id=task["id"])
            with closing(connect(api.root)) as con, con:
                _advance(con, task, receipt, api)
            attempted += 1
        except (TejAPIError, OSError, ValueError, ArithmeticError) as exc:
            code = str(exc) if isinstance(exc, TejAPIError) else "local_materialization_failed"
            retryable = code in {"transient_provider_error", "unknown_transport_outcome", "waiting_quota", "provider_cooldown"}
            with closing(connect(api.root)) as con, con:
                retry_at = now + timedelta(minutes=10 if code not in {"waiting_quota", "provider_cooldown"} else 60)
                cooldown = api.cooldown_until(con)
                if cooldown and cooldown > now and (code in {"waiting_quota", "provider_cooldown"} or cooldown > retry_at):
                    retry_at = cooldown
                con.execute("UPDATE tasks SET state=?,error_code=?,next_attempt_utc=? WHERE id=?",
                    ("retry" if retryable else "blocked", code, retry_at.isoformat(), task["id"]))
            reason = code
            break
    result = status(api)
    result.update(pages_received_this_run=attempted, reason=reason)
    atomic_write_json(api.root / "worker_status.json", result)
    return result


def _advance(con, task: dict, receipt: dict, api: TejTrialAPI) -> None:
    if not con.execute("INSERT OR IGNORE INTO applied_pages VALUES(?)", (receipt["request_id"],)).rowcount:
        return
    query_rows = task["query_rows"] + receipt["rows"]
    page_size = task.get('page_size') or receipt.get('page_bound', 10_000)
    state = "pending" if receipt["next_cursor"] else "complete"
    if query_rows >= 50_000 or receipt['next_cursor'] and query_rows + page_size > 50_000:
        state = _partition_at_cap(con, task, receipt, api)
    con.execute("UPDATE tasks SET state=?,cursor=?,query_rows=?,actual_rows=actual_rows+?,page_size=?,error_code=NULL,next_attempt_utc=NULL WHERE id=?",
        (state, receipt["next_cursor"], query_rows, receipt["rows"], page_size, task["id"]))


def status(api: TejTrialAPI) -> dict:
    with closing(connect(api.root)) as con:
        budget = api.budget(con, api.now())
        states = dict(con.execute("SELECT state,COUNT(*) FROM tasks GROUP BY state"))
        totals = con.execute("SELECT COUNT(*),COALESCE(SUM(rows),0) FROM pages").fetchone()
        priority = con.execute("SELECT value FROM meta WHERE key='acquisition_value_priority'").fetchone()
    return {"contract": "tej_trial_native_pages_v1", "observed_at_utc": api.now().isoformat(),
            "quota": budget, "task_states": states, "stored_pages": totals[0], "stored_rows": totals[1],
            "history_complete": False, "licensed_rows_public": False,
            "acquisition_priority": json.loads(priority[0]) if priority else None}


def publish_financial_reference_fields(api: TejTrialAPI, output: Path) -> dict:
    """Expose field definitions, not financial observations or account identity.

    The financial API uses acc_code/acc_value long rows. Counting just those
    schema columns hides its available accounting concepts. A complete source
    dictionary still does not prove that every concept has nonnull history.
    """
    import pyarrow.parquet as pq
    with closing(connect(api.root)) as con:
        pages = list(con.execute("SELECT p.parquet_path FROM pages p JOIN tasks t ON p.task_id=t.id "
            "WHERE t.table_id='TRAIL/TAIACC' AND t.state='complete' AND t.params_json='{}'"))
    if not pages:
        return {"account_dictionary_state": "waiting_complete_native_query", "provider_requests_sent": 0}
    definitions = {}
    for page in pages:
        for row in pq.read_table(api.root / page[0], columns=['id', 'code', 'cname', 'ename', 'unit']).to_pylist():
            identity = (row['id'], row['code'])
            if identity in definitions and definitions[identity] != row:
                raise TejAPIError('account_dictionary_conflicting_definitions')
            definitions[identity] = row
    rows = [{"financial_table": table, "dictionary_table": "TRAIL/TAIACC",
             "dictionary_id": row['id'], "acc_code": row['code'], "label_zh": row['cname'],
             "label_en": row['ename'], "source_unit_code": row['unit'],
             "reference_state": "source_dictionary_definition_not_value_history",
             "historical_nonnull_values_verified": False,
             "wizard_exclusion": "none_long_wide_key_unit_review_required"}
            for table in ('TRAIL/TAIM1A', 'TRAIL/TAIM1AQ')
            for identity, row in sorted(definitions.items()) if identity[0] == 'TAIM1A']
    if not rows:
        raise TejAPIError('account_dictionary_expected_family_missing')
    write_csv(output / 'financial_account_fields.csv', rows)
    return {"account_dictionary_state": "complete_native_query", "accounting_concepts": len(rows) // 2,
            "financial_series_definitions": len(rows), "provider_requests_sent": 0,
            "all_concept_histories_complete": False}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("inventory", "seed", "run", "status", "allocate", "reindex", "enable-allocation", "recover", "reference-fields"))
    parser.add_argument("--root", type=Path, default=Path("data_tej/api_trial_v1"))
    parser.add_argument("--env", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/data_quality/tej_trial_api_inventory_2026-10-04"))
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--max-pages", type=int, default=500)
    args = parser.parse_args()
    api = TejTrialAPI.from_env(args.root, args.env)
    with exclusive_dataset_lock(api.root / ".api.lock", provider="tej-api", timeout_seconds=5):
        if args.action == "inventory":
            result = inventory(api, args.output, refresh=args.refresh)
        elif args.action == "seed":
            result = seed_queue(api, json.loads((args.output / "catalog.json").read_text()))
        elif args.action == "run":
            if not 1 <= args.max_pages <= 500:
                parser.error("max-pages must be 1..500")
            result = run_queue(api, max_pages=args.max_pages)
        elif args.action == "allocate":
            from downloader.tej_api_ownership import write_allocation_plan
            result = write_allocation_plan(json.loads((args.output / "catalog.json").read_text()), args.root.parent, args.output)
        elif args.action == "reindex":
            from downloader.tej_api_ownership import reindex_price_receipts
            result = reindex_price_receipts(api.root)
        elif args.action == "enable-allocation":
            from downloader.tej_api_ownership import enable_exact_allocation
            result = enable_exact_allocation(args.root.parent)
        elif args.action == "recover":
            result = api.recover_saved_pages()
        elif args.action == "reference-fields":
            result = publish_financial_reference_fields(api, args.output)
        else:
            result = status(api)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
