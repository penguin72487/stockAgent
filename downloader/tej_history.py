"""TEJ desktop history contracts, durable queue and bounded source ingestion.

Smart Wizard's date axis is an analytical query grid. It may repeat a latest
period value, so exported row/cell counts must not be sold as native observations
or point-in-time history. Licensed values stay in data_tej; the public projection
only reads metadata. This is the one canonical TEJ acquisition workspace.
"""
from __future__ import annotations

from contextlib import closing
from calendar import monthrange
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import time
from typing import Any
import uuid

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet

CONTRACT_VERSION = 4
SOURCE_SCOPE_CONTRACT = "editable_source_scope_v1"
DATE_AXIS = "requested_smart_wizard_grid_not_native_observation_dates"
MAX_CELLS = 400_000
NORMALIZATION_CONTRACT = "explicit_field_units_v2_exact_power10_with_scale_guard"
PHASE_LABELS = {"P1": "候選新增特徵", "P2": "補歷史／口徑缺口", "P3": "多源校驗候選"}
METADATA_RECOVERY_CONTRACT = "verified_metadata_only_isolation_v1"
BATCH_SCHEDULER_CONTRACT = "phase_preserving_discovery_download_interleave_v1"
PREVIEW_SUBMISSION_CONTRACT = 'owned_msaa_default_action_once_no_foreground_result_transition_v2'
DESKTOP_INPUT_CONTRACT = 'native_acknowledged_date_model_commit_blank_mask_no_mouse_v5'
PREQUERY_FAILURES = {
    'desktop_foreground_unavailable_before_preview':'desktop_unavailable',
    'desktop_context_unavailable_before_preview':'desktop_unavailable',
    'local_date_input_failed_before_preview':'date_input_prequery_needs_review',
    'local_list_selection_failed_before_preview':'list_selection_prequery_needs_review',
    'local_query_activation_failed_before_preview':'query_activation_prequery_needs_review',
}
# Reviewed bridge used by the one legacy DateText foreground failure. This
# compatibility proof accepts neither arbitrary error text nor current code
# substituted for the exact script which actually ran.
LEGACY_DATE_INPUT_BRIDGE_SHA256 = "64843cabb273595ffff5b6259ed974a0bf7e9d0c8f25647a8b6d2428c62e1d0e"
LEGACY_FIELD_SELECTION_BRIDGE_SHA256 = "40e487a0545bad69499386d4b16f77552a2ab8d91abfcb915960f66003252e2a"
LEGACY_DATE_GRID_BRIDGE_SHA256 = "7eb744b077eaca844904e3d55850c7c1a9849bf76ba011cc4be86b81de65c797"
LEGACY_DATE_LAYOUT_BRIDGE_SHA256 = "f16f9bc5be060da3965e5b671ae76e1a4d53cb097a65dc9ef63d59d213ec86fc"


class BeforeDataQueryError(RuntimeError):
    """Exact guarded local-input failure; no Preview submission possible."""
    def __init__(self,error_code: str='local_date_input_failed_before_preview'):
        if error_code not in PREQUERY_FAILURES:
            raise ValueError('Unreviewed local-input failure code')
        super().__init__(error_code);self.error_code=error_code


class MetadataPreparationError(RuntimeError):
    """Bridge attested a metadata-only failure, not an unknown data Preview."""
    def __init__(self, prepared_request: Path):
        super().__init__("metadata_preparation_failed_before_data_preview")
        self.prepared_request = prepared_request


class SourceKeyLayoutError(RuntimeError):
    """Exact source-reported layout mismatch before any Preview was possible."""
    def __init__(self, message: str, source_key_mode: int | None = None):
        super().__init__(message)
        self.source_key_mode=source_key_mode


def stable_id(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:24]


def connect(root: Path) -> sqlite3.Connection:
    root = root.resolve()
    if root == root.parent or root in {Path.home().resolve(), Path.cwd().resolve()}:
        raise ValueError("TEJ requires a dedicated acquisition directory, not a home or workspace root")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    con = sqlite3.connect(root / "queue.sqlite3", timeout=5)
    con.row_factory = sqlite3.Row
    # One serial desktop writer and short read transactions do not need WAL.
    # Rollback journaling lets the hardened gateway read on its read-only
    # mount without creating a -shm file or granting write access to sources.
    con.execute("PRAGMA journal_mode=DELETE")
    con.executescript("""
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS tables (
        table_id TEXT PRIMARY KEY, smart_id TEXT NOT NULL, name TEXT NOT NULL,
        category TEXT NOT NULL, phase TEXT NOT NULL, frequency TEXT NOT NULL,
        query_type TEXT NOT NULL, schema_sha256 TEXT NOT NULL, fields_json TEXT NOT NULL,
        state TEXT NOT NULL, universe_count INTEGER, grid_dates INTEGER, grid_rows INTEGER,
        discovery_path TEXT, next_check_at_utc TEXT, last_error_code TEXT);
      CREATE TABLE IF NOT EXISTS features (
        feature_id TEXT PRIMARY KEY, table_id TEXT NOT NULL, field_index INTEGER NOT NULL,
        name TEXT NOT NULL, phase TEXT NOT NULL, unit TEXT NOT NULL, role TEXT NOT NULL,
        raw_input_policy TEXT NOT NULL, local_count INTEGER, local_first TEXT, local_last TEXT,
        exported_non_null_cells INTEGER, first_query_period TEXT, last_query_period TEXT);
      CREATE INDEX IF NOT EXISTS features_table ON features(table_id,field_index);
      CREATE INDEX IF NOT EXISTS features_named ON features(table_id,name);
      CREATE TABLE IF NOT EXISTS tasks (
        task_id TEXT PRIMARY KEY, table_id TEXT NOT NULL, kind TEXT NOT NULL,
        phase TEXT NOT NULL, priority INTEGER NOT NULL, request_json TEXT NOT NULL,
        state TEXT NOT NULL DEFAULT 'pending', expected_rows INTEGER,
        actual_rows INTEGER, actual_bytes INTEGER, seconds REAL,
        attempted_at_utc TEXT, completed_at_utc TEXT, receipt_path TEXT,
        output_path TEXT, last_error_code TEXT, next_attempt_at_utc TEXT);
      CREATE INDEX IF NOT EXISTS tasks_ready ON tasks(state,priority,next_attempt_at_utc);
      CREATE INDEX IF NOT EXISTS tasks_ready_kind ON tasks(state,kind,priority,next_attempt_at_utc);
      CREATE TABLE IF NOT EXISTS traffic (
        event_id TEXT PRIMARY KEY, action TEXT NOT NULL, started_at_utc TEXT NOT NULL,
        completed_at_utc TEXT, state TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS traffic_started ON traffic(started_at_utc);
      CREATE TABLE IF NOT EXISTS download_plans (
        table_id TEXT PRIMARY KEY,contract TEXT NOT NULL,plan_json TEXT NOT NULL,
        next_query INTEGER NOT NULL,total_queries INTEGER NOT NULL,
        phase TEXT NOT NULL,priority INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS desktop_attempts (
        attempt_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,request_path TEXT NOT NULL,
        raw_path TEXT NOT NULL,started_at_utc TEXT NOT NULL,state TEXT NOT NULL,
        previous_attempt_id TEXT,finished_at_utc TEXT);
      CREATE INDEX IF NOT EXISTS desktop_attempts_task ON desktop_attempts(task_id,started_at_utc);
      CREATE TABLE IF NOT EXISTS desktop_replays (
        authorization_id TEXT PRIMARY KEY,task_id TEXT NOT NULL,original_attempted_at_utc TEXT,
        audit_path TEXT NOT NULL,audit_sha256 TEXT NOT NULL,consumed_at_utc TEXT,outcome TEXT);
    """)
    if 'active_attempt_id' not in {r[1] for r in con.execute('PRAGMA table_info(tasks)')}:
        con.execute('ALTER TABLE tasks ADD COLUMN active_attempt_id TEXT')
    if "timing_basis" not in {r[1] for r in con.execute("PRAGMA table_info(tasks)")}:
        # Scope/source evidence stays unchanged. Old durations may include
        # operator recovery, so do not retroactively call them fresh queries.
        con.execute("ALTER TABLE tasks ADD COLUMN timing_basis TEXT")
    if "safe_prequery_retries" not in {r[1] for r in con.execute("PRAGMA table_info(tasks)")}:
        con.execute("ALTER TABLE tasks ADD COLUMN safe_prequery_retries INTEGER NOT NULL DEFAULT 0")
    if "scope_contract" not in {r[1] for r in con.execute("PRAGMA table_info(tasks)")}:
        # No default that could silently certify existing tasks. New inserts
        # explicitly name their reviewed acquisition scope below.
        con.execute("ALTER TABLE tasks ADD COLUMN scope_contract TEXT")
    if "work_expected_rows" not in {r[1] for r in con.execute("PRAGMA table_info(tasks)")}:
        con.execute("ALTER TABLE tasks ADD COLUMN work_expected_rows INTEGER")
    table_columns = {r[1] for r in con.execute("PRAGMA table_info(tables)")}
    for column in ("first_available_query_period", "last_available_query_period", "axis_profile_basis"):
        if column not in table_columns:
            con.execute(f"ALTER TABLE tables ADD COLUMN {column} TEXT")
    for column in ('work_grid_rows','field_batches'):
        if column not in table_columns:
            con.execute(f'ALTER TABLE tables ADD COLUMN {column} INTEGER')
    if 'source_key_mode' not in table_columns:
        con.execute('ALTER TABLE tables ADD COLUMN source_key_mode INTEGER')
    con.execute("CREATE INDEX IF NOT EXISTS tasks_timing_sample ON tasks(state,kind,timing_basis,completed_at_utc DESC)")
    con.execute("CREATE INDEX IF NOT EXISTS tasks_scope_metadata ON tasks(scope_contract,table_id,kind,state,expected_rows,actual_rows,actual_bytes)")
    con.execute("CREATE INDEX IF NOT EXISTS tasks_scope_work_metadata ON tasks(scope_contract,state,table_id,kind,work_expected_rows,expected_rows,actual_rows,actual_bytes)")
    con.execute("CREATE INDEX IF NOT EXISTS tasks_scope_timing ON tasks(scope_contract,state,kind,timing_basis,completed_at_utc DESC,table_id,expected_rows,actual_rows,actual_bytes,seconds)")
    con.execute("CREATE INDEX IF NOT EXISTS tasks_ready_order ON tasks(state,priority,table_id,task_id,next_attempt_at_utc)")
    con.execute("CREATE INDEX IF NOT EXISTS tasks_ready_kind_order ON tasks(state,kind,priority,table_id,task_id,next_attempt_at_utc)")
    if not con.execute("SELECT 1 FROM meta WHERE key='full_lifecycle_timing_v1'").fetchone():
        with con:
            con.execute("UPDATE tasks SET timing_basis='legacy_bridge_only' WHERE timing_basis='fresh_end_to_end'")
            con.execute("INSERT INTO meta VALUES ('full_lifecycle_timing_v1','1')")
    return con


def read_csv(path: Path) -> list[dict]:
    import csv
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def compact_request(request: dict, definition: dict) -> dict:
    """Losslessly reference the immutable schema; never repeat 800 names per task."""
    names = json.loads(definition["fields_json"])
    partition=request.get('field_partition')
    if partition is not None:
        from downloader.tej_planning import CONTRACT
        from downloader.tej_key_layout import key_count
        bounds=partition.get('field_range') if isinstance(partition,dict) else None
        if (not isinstance(bounds,list) or len(bounds)!=2 or any(not isinstance(x,int) or isinstance(x,bool) for x in bounds)
                or not 0 <= bounds[0] < bounds[1] <= len(names) or bounds[1]-bounds[0]>30-key_count(request)
                or partition.get('contract')!=CONTRACT or partition.get('catalog_fields')!=len(names)
                or request.get('fields')!=names[bounds[0]:bounds[1]] or request.get('catalog_fields')!=names):
            raise ValueError('Invalid field partition/schema reference')
        return {**{k:v for k,v in request.items() if k not in {'fields','catalog_fields'}},
                '_fields_ref':{'version':2,'table_id':definition['table_id'],'schema_sha256':definition['schema_sha256'],
                               'field_range':bounds}}
    if request.get("fields") != names or request.get("catalog_fields") != names:
        return request
    return {**{k: v for k, v in request.items() if k not in {"fields", "catalog_fields"}},
            "_fields_ref": {"version": 1, "table_id": definition["table_id"],
                            "schema_sha256": definition["schema_sha256"]}}


def expand_request(request: dict, definition: dict, table_id: str) -> dict:
    ref = request.get("_fields_ref")
    if ref is None:
        return request
    expected={"version":1,"table_id":table_id,"schema_sha256":definition["schema_sha256"]}
    if isinstance(ref,dict) and ref.get('version')==2:
        expected={**expected,'version':2,'field_range':ref.get('field_range')}
    if (ref != expected
            or definition["table_id"] != table_id
            or any(request.get(k) != definition[v] for k, v in (("smart_id", "smart_id"), ("table", "name")))
            or "fields" in request or "catalog_fields" in request):
        raise ValueError("Queue schema reference mismatch")
    names = json.loads(definition["fields_json"])
    from scripts.build_tej_smart_wizard_inventory import table_identity
    if table_identity(definition["smart_id"], definition["name"], names) != (table_id, definition["schema_sha256"]):
        raise ValueError("Registered schema content differs from its fingerprint")
    full={**{k:v for k,v in request.items() if k!='_fields_ref'},'fields':names,'catalog_fields':names}
    if ref['version']==2:
        bounds=ref['field_range']
        if not isinstance(bounds,list) or len(bounds)!=2 or any(not isinstance(x,int) or isinstance(x,bool) for x in bounds) or not 0 <= bounds[0] < bounds[1] <= len(names):
            raise ValueError('Invalid field reference bounds')
        full['fields']=names[bounds[0]:bounds[1]]
        if compact_request(full,definition)!=request:
            raise ValueError('Field reference is not an exact partition')
    return full


def task_request(root: Path, task: dict) -> dict:
    request = json.loads(task["request_json"])
    if "_fields_ref" not in request:
        return request
    with closing(connect(root)) as con:
        definition = con.execute("SELECT * FROM tables WHERE table_id=?", (task["table_id"],)).fetchone()
    if definition is None:
        raise ValueError("Missing registered schema definition")
    return expand_request(request, dict(definition), task["table_id"])


def compact_queue(root: Path) -> dict:
    """Verify exact reconstruction under the caller's dataset lock; no source edits."""
    before = after = changed = 0
    with closing(connect(root)) as con, con:
        definitions = {r["table_id"]: dict(r) for r in con.execute("SELECT * FROM tables")}
        # A bounded cursor, not a list of every historical request in RAM.
        rows = con.execute("SELECT task_id,table_id,request_json FROM tasks WHERE state NOT LIKE 'superseded_%'")
        for row in rows:
            text = row["request_json"]
            old = json.loads(text)
            definition = definitions[row["table_id"]]
            new = compact_request(old, definition)
            if new == old:
                continue
            if expand_request(new, definition, row["table_id"]) != old:
                raise ValueError("Lossless request reconstruction failed")
            encoded = json.dumps(new, ensure_ascii=False, separators=(",", ":"))
            con.execute("UPDATE tasks SET request_json=? WHERE task_id=?", (encoded, row["task_id"]))
            before += len(text.encode()); after += len(encoded.encode()); changed += 1
        con.execute("INSERT OR REPLACE INTO meta VALUES ('queue_encoding','schema_reference_v1')")
    receipt = {"queue_encoding": "schema_reference_v1", "changed_tasks": changed,
               "request_bytes_before": before, "request_bytes_after": after,
               "exact_request_reconstruction_verified": True, "task_ids_or_scopes_changed": False,
               "source_data_or_receipts_deleted": False,
               "note": "SQLite freed pages are retained/reused; this does not claim physical file shrinkage"}
    atomic_write_json(root / "diagnostics" / ("queue_encoding_upgrade-" + uuid.uuid4().hex + ".json"), receipt)
    return receipt


def revalidate_source_scopes(root: Path) -> dict:
    """Version the acquisition scope, preserving every old source and receipt.

    A disabled/memory-erroring vendor UI exposed cached company/date menus.
    The old Cartesian scopes therefore cannot be completeness denominators.
    Called explicitly under the dataset lock; never from a read-only monitor.
    """
    with closing(connect(root)) as con:
        meta = {r[0]: r[1] for r in con.execute("SELECT key,value FROM meta")}
        if meta.get("editable_scope_contract") == SOURCE_SCOPE_CONTRACT:
            return {"state": "already_revalidated_queue_registered", "source_files_deleted": False}
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError("Unresolved running query or unknown Preview requires recovery before scope migration")
        definitions = [dict(r) for r in con.execute("SELECT * FROM tables ORDER BY table_id")]
        feature_metadata = [dict(r) for r in con.execute("SELECT feature_id,exported_non_null_cells,first_query_period,last_query_period FROM features WHERE exported_non_null_cells IS NOT NULL")]
        legacy = dict(con.execute("SELECT COUNT(*) AS completed_download_tasks,COALESCE(SUM(actual_rows),0) AS exported_rows,COALESCE(SUM(actual_bytes),0) AS recorded_bytes FROM tasks WHERE kind='download' AND state='complete'").fetchone())
        legacy["exported_non_null_cells"] = sum(f["exported_non_null_cells"] or 0 for f in feature_metadata)
        audit = {"contract_version": CONTRACT_VERSION, "scope_contract": SOURCE_SCOPE_CONTRACT,
                 "observed_at_utc": datetime.now(UTC).isoformat(), "legacy_exports": legacy,
                 "previous_table_metadata": definitions, "previous_feature_metadata": feature_metadata,
                 "reason": "cached_axes_possible_when_vendor_mysql_allocation_error_disabled_company_date_groups",
                 "source_files_deleted": False, "source_receipts_overwritten": False,
                 "provider_data_queries_sent": 0}
        audit_path = root / "diagnostics" / ("editable_scope_upgrade_v4-" + uuid.uuid4().hex + ".json")
        atomic_write_json(audit_path, audit)
        cutoff = meta["cutoff"]
        config = json.loads(meta["config"])
        with con:
            # Receipts/task counts remain in the ledger; only their active
            # planning status changes. Do not resume an old unverified scope.
            con.execute("UPDATE tasks SET scope_contract='legacy_scope_unverified_v3' WHERE scope_contract IS NULL")
            con.execute("UPDATE tasks SET state='superseded_scope_unverified_v3' WHERE state NOT LIKE 'superseded_%' AND state!='complete'")
            con.execute("UPDATE tables SET state=CASE WHEN fields_json='[]' THEN 'empty_field_menu' ELSE 'pending_discovery' END,universe_count=NULL,grid_dates=NULL,grid_rows=NULL,discovery_path=NULL,first_available_query_period=NULL,last_available_query_period=NULL,axis_profile_basis=NULL,last_error_code=NULL")
            con.execute("UPDATE features SET exported_non_null_cells=NULL,first_query_period=NULL,last_query_period=NULL")
            for table in definitions:
                names = json.loads(table["fields_json"])
                if not names:
                    continue
                request = {"contract_version": CONTRACT_VERSION, "action": "plan", "type": table["query_type"],
                           "smart_id": table["smart_id"], "table": table["name"], "fields": names,
                           "catalog_fields": names, "frequency": table["frequency"],
                           "start": config["history_search_start"], "end": cutoff,
                           "max_cells": config["max_cells_per_export"], "max_rows": config["max_rows_per_export"]}
                con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,scope_contract) VALUES(?,?,?,?,?,?,?)",
                            (stable_id([table["table_id"], "discover", cutoff, CONTRACT_VERSION]),table["table_id"],
                             "discover",table["phase"],int(table["phase"][1])*100+1,
                             json.dumps(compact_request(request,table),ensure_ascii=False,separators=(",", ":")),SOURCE_SCOPE_CONTRACT))
            for key,value in {"editable_scope_contract":SOURCE_SCOPE_CONTRACT,
                              "legacy_unverified_exports":json.dumps(legacy),
                              "scope_upgrade_audit":str(audit_path.relative_to(root))}.items():
                con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)",(key,value))
    atomic_write_json(root / "worker_status.json", {"contract_version":CONTRACT_VERSION,
                       "state":"source_scopes_require_revalidation","observed_at_utc":datetime.now(UTC).isoformat()})
    return {"state":"scope_revalidation_registered","tables_requeued":sum(bool(json.loads(t["fields_json"])) for t in definitions),
            "legacy_exports_preserved":legacy,"source_files_deleted":False,"provider_data_queries_sent":0}


def register_inventory(root: Path, inventory: Path, config: dict, *, cutoff: str) -> dict:
    """Register every field, including empty tables; do not infer entitlement."""
    from scripts.build_tej_smart_wizard_inventory import table_identity
    date.fromisoformat(cutoff)
    summary = json.loads((inventory / "summary.json").read_text())
    if not summary.get("catalog", {}).get("catalog_scan_complete"):
        raise ValueError("Full catalog type scan is required before registration")
    fields, tables = read_csv(inventory / "all_fields.csv"), read_csv(inventory / "tables.csv")
    groups: dict[str, list[dict]] = {}
    for field in fields:
        groups.setdefault(field["table_id"], []).append(field)
    fingerprint = hashlib.sha256((inventory / "all_fields.csv").read_bytes() +
                                 (inventory / "tables.csv").read_bytes()).hexdigest()
    with closing(connect(root)) as con, con:
        old = con.execute("SELECT value FROM meta WHERE key='catalog_sha256'").fetchone()
        if old and old[0] != fingerprint:
            raise ValueError("Catalog changed; versioned workspace migration is required")
        old_cutoff = con.execute("SELECT value FROM meta WHERE key='cutoff'").fetchone()
        if old_cutoff and old_cutoff[0] != cutoff:
            raise ValueError("Initial history cutoff is immutable; use incremental scheduling, not a second full history")
        scope = con.execute("SELECT value FROM meta WHERE key='editable_scope_contract'").fetchone()
        if old and (scope is None or scope[0] != SOURCE_SCOPE_CONTRACT):
            raise ValueError("Existing queue requires explicit revalidate-scopes before registration")
        for key, value in {"catalog_sha256": fingerprint, "cutoff": cutoff,
                           "editable_scope_contract": SOURCE_SCOPE_CONTRACT,
                           "catalog_summary": json.dumps({key: summary[key] for key in
                              ("generated_at_utc", "tables", "unique_table_schema_fields", "phase_counts", "catalog")}),
                           "config": json.dumps(config)}.items():
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, value))
        for table in tables:
            members = sorted(groups.get(table["table_id"], []), key=lambda f: int(f["field_index"]))
            names = [f["field"] for f in members]
            identity, schema = table_identity(table["smart_id"], table["table"], names)
            if identity != table["table_id"] or schema != table["schema_sha256"]:
                raise ValueError("Inventory schema identity mismatch")
            phase = min((f["phase"] for f in members), default="P1")
            grain = members[0]["cadence"] if members else "unknown_or_event"
            # This is an explicit query-grid choice, not an inferred native cadence.
            frequency = grain if grain in {"daily", "weekly", "monthly", "quarterly", "yearly"} else (
                "quarterly" if table["category"] in {"financial", "audit_report"} else "daily")
            types = json.loads(table["available_types"])
            if len(types) != 1:
                raise ValueError("Multiple universe aliases need an explicit verified universe plan")
            query_type = types[0]
            con.execute("INSERT OR IGNORE INTO tables(table_id,smart_id,name,category,phase,frequency,query_type,schema_sha256,fields_json,state) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (identity, table["smart_id"], table["table"], table["category"], phase,
                         frequency, query_type, schema, json.dumps(names, ensure_ascii=False),
                         "pending_discovery" if names else "empty_field_menu"))
            for f in members:
                con.execute("INSERT OR IGNORE INTO features(feature_id,table_id,field_index,name,phase,unit,role,raw_input_policy,local_count,local_first,local_last) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                            (f["feature_id"], identity, int(f["field_index"]), f["field"], f["phase"],
                             f["source_unit"], f["field_role"], f["original_format"],
                             int(f["local_non_null_count"]) if f["local_non_null_count"] else None,
                             f["local_first_reported"] or None, f["local_last_reported"] or None))
            if names:
                request = {"contract_version": CONTRACT_VERSION, "action": "plan", "type": query_type,
                           "smart_id": table["smart_id"], "table": table["table"], "fields": names,
                           "catalog_fields": names, "frequency": frequency,
                           "start": config["history_search_start"], "end": cutoff,
                           "max_cells": config["max_cells_per_export"], "max_rows": config["max_rows_per_export"]}
                con.execute("INSERT OR IGNORE INTO tasks(task_id,table_id,kind,phase,priority,request_json,scope_contract) VALUES(?,?,?,?,?,?,?)",
                            (stable_id([identity, "discover", cutoff, CONTRACT_VERSION]), identity, "discover", phase,
                             int(phase[1]) * 100 + 1, json.dumps(request, ensure_ascii=False),SOURCE_SCOPE_CONTRACT))
    return {"tables": len(tables), "fields": len(fields), "catalog_sha256": fingerprint}


def normalize_period(value: Any, *, system: str | None = None) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isfinite(value) and int(value) == value and 100001 <= value <= 999912:
            return normalize_period(str(int(value)))  # Unambiguous compact month, not an Excel serial.
        if system not in {"excel_1900", "excel_1904"} or not math.isfinite(value) or int(value) != value or not 1 <= value <= 73050:
            raise ValueError("Invalid source Excel date")
        if system == "excel_1900" and value == 60:
            raise ValueError("Excel phantom leap day")
        base = date(1904, 1, 1) if system == "excel_1904" else date(1899, 12, 30)
        if system == "excel_1900" and value < 60:
            base = date(1899, 12, 31)
        return (base + timedelta(days=int(value))).isoformat()
    text = str(value).strip()
    if re.fullmatch(r"\d{6}", text):
        year, month = int(text[:4]), int(text[4:])
        date(year, month, 1)
        return f"{year:04d}-{month:02d}"
    if re.fullmatch(r"\d{4}[/\-]\d{2}[/\-]\d{2}", text):
        return date.fromisoformat(text.replace("/", "-")).isoformat()
    if re.fullmatch(r"\d{8}", text):
        return date.fromisoformat(f"{text[:4]}-{text[4:6]}-{text[6:]}").isoformat()
    if re.fullmatch(r"\d{4}[/\-]\d{2}", text) and 1 <= int(text[-2:]) <= 12:
        return text.replace("/", "-")
    if re.fullmatch(r"\d{4}", text):
        return text
    raise ValueError("Unverified query-period label")


def company_code(label: str) -> str:
    # Smart Wizard 4.1 uses "code==>name"; old verified samples used "=>".
    # Splitting on "=>" leaves an extra '=' in current account company keys.
    match = re.fullmatch(r"([^\s=>]+)={1,2}>.+", label)
    if match is None:
        raise ValueError("Unverified company label syntax")
    return match[1]


def display_decimal(value: Any) -> Decimal:
    """Lossless numeric interpretation only; original display cells stay raw."""
    if isinstance(value, bool):
        raise ValueError("Boolean is not a numeric financial value")
    if isinstance(value, (int, float)):
        value = str(value)
    if not isinstance(value, str) or not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value.strip()):
        raise ValueError("Unknown display number/locale/sentinel; do not guess")
    number = Decimal(value.strip())
    if not number.is_finite():
        raise ValueError("Non-finite display number")
    return number


def scale_decimal_power10(value: Decimal, exponent: int) -> Decimal:
    """Move the exponent without the default Decimal context's 28-digit rounding."""
    if exponent not in {3, 6} or not value.is_finite():
        raise ValueError("Unreviewed unit scale")
    parts = value.as_tuple()
    return Decimal((parts.sign, parts.digits, parts.exponent + exponent))


def validate_source_scope(request: dict, payload: dict, action: str) -> None:
    from downloader.tej_key_layout import KEY1_CONTRACT, SNAPSHOT_DATE_AXIS
    snapshot=(request.get('source_key_mode')==1 and request.get('key_layout_contract')==KEY1_CONTRACT
              or action=='plan' and payload.get('source_key_mode')==1 and payload.get('key_layout_contract')==KEY1_CONTRACT)
    # First Key=1 captures reported the old false flag for an UNUSED setting.
    # Keep their raw evidence; this is not proof of a calendar checkbox state.
    calendar_ok=payload.get('calendar_date_mode') is False or snapshot and payload.get('calendar_date_mode') is None
    if (payload.get("contract_version") not in {2, 3, CONTRACT_VERSION} or payload.get("provider") != "tej_smart_wizard"
            or payload.get("action") != action or payload.get("date_axis") != (SNAPSHOT_DATE_AXIS if snapshot else DATE_AXIS)
            or payload.get("credentials_read") is not False
            or payload.get("universe_scope") != "exact_type_smart_id_all_sectors"
            or not calendar_ok
            or payload.get("checkbox_verification_method") != "msaa_role44_state_flags"
            or any(payload.get(key) != request.get(key) or not request.get(key) for key in ("type", "smart_id", "table"))
            or payload.get("fields") != request.get("fields")):
        raise ValueError("Source binding, schema or universe scope mismatch")
    # Legacy captures remain interpretable for explicit recovery/audit, but a
    # current task can never adopt a cached menu from a disabled/erroring UI.
    if request.get("contract_version") == CONTRACT_VERSION or payload.get("contract_version") == CONTRACT_VERSION:
        proof = payload.get("source_scope_proof")
        if (payload.get("contract_version") != CONTRACT_VERSION or not isinstance(proof, dict)
                or proof.get("contract") != SOURCE_SCOPE_CONTRACT
                or any(proof.get(k) is not True for k in
                       (("company_group_enabled", "vendor_notices_absent", "binding_readback_verified") if snapshot else
                        ("company_group_enabled", "date_group_enabled", "vendor_notices_absent", "binding_readback_verified")))
                or snapshot and (proof.get('source_key_mode')!=1 or proof.get('date_axis_not_used') is not True)):
            raise ValueError("Editable source scope proof missing or invalid")


def discovery_tasks(root: Path, task: dict, payload: dict, output: Path) -> int:
    """Partition the real vendor axes, never a guessed historical universe."""
    request = task_request(root, task)
    validate_source_scope(request, payload, "plan")
    if payload.get("task_id") != task["task_id"]:
        raise ValueError("Discovery evidence belongs to another task")
    companies, dates = payload.get("company_labels"), payload.get("date_labels")
    if (not isinstance(companies, list) or not isinstance(dates, list)
            or any(not isinstance(x, str) or not x for x in companies + dates)
            or len(set(companies)) != len(companies) or len(set(dates)) != len(dates)):
        raise ValueError("Discovery scope or axes mismatch")
    from downloader.tej_period_keys import MONTH_CONTRACTS
    monthly_keys = request.get('source_period_key_contract') in MONTH_CONTRACTS
    if monthly_keys and (payload.get('frequency') != 'monthly' or
                         payload.get('frequency_selection_readback_contract') != 'owned_checked_frequency_button_v1'):
        raise ValueError('Monthly source control readback required')
    for label in dates:
        if monthly_keys:
            from downloader.tej_period_keys import month_key
            period = month_key(label)
        else:
            period = normalize_period(label)
        if period < request["start"][:len(period)] or period > request["end"][:len(period)]:
            raise ValueError("Vendor returned dates outside requested search interval")
    with closing(connect(root)) as con:
        planning=con.execute("SELECT value FROM meta WHERE key='preview_planning_contract'").fetchone()
    if planning:
        from downloader.tej_planning import CONTRACT, install_plan
        if planning[0]!=CONTRACT:
            raise ValueError('Incompatible Preview planner; do not enqueue')
        with closing(connect(root)) as con,con:
            config=json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
            definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
            periods=[month_key(label) if monthly_keys else normalize_period(label) for label in dates]
            if payload.get('source_key_mode') in (1,2,3):
                con.execute('UPDATE tables SET source_key_mode=? WHERE table_id=?',(payload['source_key_mode'],task['table_id']))
            con.execute('UPDATE tables SET universe_count=?,grid_dates=?,grid_rows=?,discovery_path=?,state=?,first_available_query_period=?,last_available_query_period=?,axis_profile_basis=? WHERE table_id=?',
                (len(companies),len(dates),len(companies)*len(dates),str(output.relative_to(root)),
                 'backfilling' if companies and dates else 'empty_query_axis_unverified',min(periods,default=None),max(periods,default=None),
                 'source_menu_bounds_not_native_history_or_release_times',task['table_id']))
            installed=install_plan(con,definition,request,payload,config)
            if payload.get('source_key_mode')==1:
                con.execute("UPDATE tables SET frequency='snapshot',grid_rows=universe_count,grid_dates=0,first_available_query_period=NULL,last_available_query_period=NULL,axis_profile_basis='native_key1_current_snapshot_no_history_axis',state=CASE WHEN universe_count>0 THEN 'backfilling' ELSE 'empty_query_axis_unverified' END WHERE table_id=?",(task['table_id'],))
            return installed['logical_queries']
    columns = len(request["fields"]) + 2
    max_rows = min(request["max_rows"], request["max_cells"] // columns - 1)
    if max_rows < 1:
        raise ValueError("Schema exceeds configured output bound")
    with closing(connect(root)) as con, con:
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        definition = dict(con.execute("SELECT * FROM tables WHERE table_id=?", (task["table_id"],)).fetchone())
        # Use the largest bounded date range. Company batching limits UI clicks,
        # not vendor HTTP rate; the vendor's internal request count is unknown.
        per_company = min(config["max_companies_per_export"], max_rows)
        generated = 0
        for c in range(0, len(companies), per_company):
            group = companies[c:c + per_company]
            dates_per_task = max(1, max_rows // len(group))
            for d in range(0, len(dates), dates_per_task):
                selection = dates[d:d + dates_per_task]
                query = {**request, "action": "download", "company_labels": group, "date_labels": selection}
                # Date search is bounded to the selected group rather than
                # repeatedly loading a century-long calendar every export.
                periods = [normalize_period(p) for p in selection]
                if all(len(p) == 10 for p in periods):
                    query.update(start=min(periods), end=max(periods))
                elif all(len(p) == 7 for p in periods):
                    first, last = min(periods), max(periods)
                    year, month = map(int, last.split('-'))
                    # The vendor's native-month search excludes the lower
                    # boundary month when given its first day. Expand only
                    # the MENU search by one day, then verify/select exactly
                    # the planned labels before any data request.
                    lower = (date.fromisoformat(first + '-01') - timedelta(days=1)).isoformat()
                    query.update(start=max(request['start'], lower),
                                 end=min(request['end'], f"{last}-{monthrange(year, month)[1]:02d}"))
                key = stable_id([task["table_id"], query])
                con.execute("INSERT OR IGNORE INTO tasks(task_id,table_id,kind,phase,priority,request_json,expected_rows,scope_contract) VALUES(?,?,?,?,?,?,?,?)",
                            (key, task["table_id"], "download", task["phase"], int(task["phase"][1]) * 100,
                             json.dumps(compact_request(query, definition), ensure_ascii=False, separators=(",", ":")), len(group) * len(selection),
                             SOURCE_SCOPE_CONTRACT if request.get('contract_version') == CONTRACT_VERSION else 'legacy_scope_unverified_v3'))
                generated += 1
        periods = [normalize_period(label) for label in dates]
        con.execute("UPDATE tables SET universe_count=?,grid_dates=?,grid_rows=?,discovery_path=?,state=?,first_available_query_period=?,last_available_query_period=?,axis_profile_basis=? WHERE table_id=?",
                    (len(companies), len(dates), len(companies) * len(dates), str(output.relative_to(root)),
                     "backfilling" if generated else "empty_query_axis_unverified",
                     min(periods,default=None), max(periods,default=None),
                     "source_menu_bounds_not_native_history_or_release_times", task["table_id"]))
    return generated


def refresh_axis_profiles(root: Path) -> dict:
    """One bounded writer-side metadata upgrade, never called by the dashboard."""
    profiled = rejected = 0
    with closing(connect(root)) as con, con:
        definitions = [dict(row) for row in con.execute("SELECT * FROM tables WHERE discovery_path IS NOT NULL AND axis_profile_basis IS NULL")]
        for definition in definitions:
            try:
                output = (root / definition['discovery_path']).resolve()
                if output.parent != (root/'raw').resolve() or output.suffix != '.json' or output.stat().st_size > 16*1024**2:
                    raise ValueError('Unreviewed axis evidence scope or size')
                payload = json.loads(output.read_text(encoding='utf-8-sig'))
                row = con.execute("SELECT * FROM tasks WHERE task_id=? AND table_id=? AND kind='discover' AND state='complete'",
                                  (payload.get('task_id'),definition['table_id'])).fetchone()
                if row is None or row['output_path'] != definition['discovery_path']:
                    raise ValueError('Unverified axis evidence identity')
                req = expand_request(json.loads(row['request_json']),definition,definition['table_id'])
                validate_source_scope(req,payload,'plan')
                labels = payload['date_labels'];companies = payload['company_labels']
                if (not isinstance(labels,list) or not isinstance(companies,list)
                        or len(labels)!=definition['grid_dates'] or len(companies)!=definition['universe_count']
                        or any(not isinstance(v,str) or not v for v in labels+companies)
                        or len(set(labels))!=len(labels) or len(set(companies))!=len(companies)):
                    raise ValueError('Axis evidence differs from the recorded query scope')
                periods = [normalize_period(label) for label in labels]
                if any(p<req['start'][:len(p)] or p>req['end'][:len(p)] for p in periods):
                    raise ValueError('Axis outside search interval')
                con.execute("UPDATE tables SET first_available_query_period=?,last_available_query_period=?,axis_profile_basis=? WHERE table_id=?",
                            (min(periods,default=None),max(periods,default=None),
                             'source_menu_bounds_not_native_history_or_release_times',definition['table_id']))
                profiled += 1
            except (ValueError,TypeError,KeyError,OSError):
                con.execute("UPDATE tables SET axis_profile_basis='local_axis_evidence_requires_review' WHERE table_id=?",(definition['table_id'],))
                rejected += 1
    return {'profiled_tables':profiled,'local_axis_evidence_requires_review':rejected,'provider_queries_sent':0}


def validate_preview(headers: list[str], cells: list[list], preview: Any, *, date_system: str, keys_count: int = 2) -> int:
    """Verify source header and bounded leading/trailing MSAA samples.

    This catches stale/wrong Excel exports without pretending to have compared
    every cell of a large query. The full query keys and shape are checked below.
    """
    if not isinstance(preview, list) or len(preview) != 2 or not isinstance(preview[0], int) or not isinstance(preview[1], list):
        raise ValueError("Missing pre-export Preview evidence")
    child_count, samples = preview
    if child_count not in {len(cells), len(cells) + 1}:
        raise ValueError("Preview/export row count mismatch")
    by_key = {(str(r[0]), *([] if keys_count==1 else [normalize_period(r[1], system=date_system)]), *([str(r[2])] if keys_count==3 else [])): r
              for r in cells[1:]}
    seen_indices, sampled = set(), 0
    for sample in samples:
        if not isinstance(sample, list) or len(sample) != 2 or not isinstance(sample[0], int) or not isinstance(sample[1], list):
            raise ValueError("Unrecognized Preview sample")
        index, values = sample
        if index in seen_indices or not 1 <= index <= child_count:
            raise ValueError("Duplicate or out-of-scope Preview sample")
        seen_indices.add(index)
        if len(values) == len(headers) + 1 and values[0] in (None, "", "Top Left Header Cell"):
            values = values[1:]  # MSAA exposes the DataGridView row-header cell.
        if len(values) != len(headers):
            raise ValueError("Preview column count mismatch")
        if index == 1:
            if values != headers:
                raise ValueError("Preview schema mismatch")
            continue
        if index == len(cells) + 1 and all(v in (None, "") for v in values):
            continue  # Empty DataGridView new-row affordance, not source data.
        key = (str(values[0]), *([] if keys_count==1 else [normalize_period(values[1])]), *([str(values[2])] if keys_count==3 else []))
        row = by_key.get(key)
        if row is None:
            raise ValueError("Preview/export query key mismatch")
        for actual, source in zip(row[keys_count:], values[keys_count:]):
            if actual is None or actual == "":
                equal = source is None or source == ""
            elif isinstance(actual, (int, float)) and not isinstance(actual, bool):
                try:
                    equal = Decimal(str(actual)) == Decimal(str(source))
                except Exception:
                    equal = False
            else:
                equal = str(actual) == str(source)
            if not equal:
                raise ValueError("Preview/Excel value mismatch; source is retained, not repaired by filling")
        sampled += 1
    expected_indices = {i for i in range(1, child_count + 1) if i <= 4 or i >= child_count - 2}
    if seen_indices != expected_indices or sampled < min(3, len(cells) - 1):
        raise ValueError("Incomplete bounded Preview samples")
    return sampled


def validate_export(request: dict, payload: dict) -> tuple[list[str], list[list], dict]:
    from downloader.tej_key_layout import key_count, KEY3_CONTRACT
    keys_count=key_count(request)
    if keys_count==1:
        from downloader.tej_snapshot import validate_snapshot
        return validate_snapshot(request,payload)
    validate_source_scope(request, payload, "download")
    cells = payload.get("cells")
    if payload.get("contract_version") in {3, CONTRACT_VERSION} and (
            payload.get("capture_method") != "native_msaa_preview_full"
            or payload.get("source_value_representation") != "vendor_display_strings_not_underlying_excel_values"
            or payload.get("source_grid_rows") != len(cells or []) - 1
            or payload.get("source_grid_columns") != len(request["fields"]) + keys_count):
        raise ValueError("Unverified full source-grid capture contract")
    if (payload.get("provider") != "tej_smart_wizard" or payload.get("action") != "download"
            or payload.get("date_axis") != DATE_AXIS or payload.get("credentials_read") is not False
            or payload.get("query_comments_read") is not False or not isinstance(cells, list) or len(cells) < 2):
        raise ValueError("Unrecognized bounded export contract")
    key_headers = payload.get("source_key_headers", ["CO_ID", "Date"])
    if keys_count==3 and (payload.get('source_key_mode')!=3 or payload.get('key_layout_contract')!=KEY3_CONTRACT):
        raise ValueError('Unverified native record key layout')
    if (not isinstance(key_headers, list) or len(key_headers) != keys_count
            or any(not isinstance(h, str) or not h or len(h) > 128 for h in key_headers)
            or len(set(key_headers + request["fields"])) != len(key_headers) + len(request["fields"])):
        raise ValueError("Unverified source key headers")
    from downloader.tej_header_mapping import validate_headers
    headers, header_contract = validate_headers(request,payload,key_headers)
    if any(not isinstance(r, list) or len(r) != len(headers) for r in cells):
        raise ValueError("Export schema mismatch")
    if len(cells) * len(headers) > min(MAX_CELLS, request["max_cells"]) or len(cells) - 1 > request["max_rows"]:
        raise ValueError("Export exceeds memory bounds")
    symbols = [company_code(c) for c in request["company_labels"]]
    if len(set(symbols)) != len(symbols):
        raise ValueError("Unverified company key syntax")
    from downloader.tej_period_keys import period_scope, month_key
    periods, period_profile = period_scope(request, payload)
    expected = {(s, p) for s in symbols for p in periods}
    found = set()
    counts = [0] * len(request["fields"])
    first, last = [None] * len(counts), [None] * len(counts)
    keys = []
    for row in cells[1:]:
        if not isinstance(row[0], str):
            raise ValueError("Missing source company key")
        code = row[0].split()[0]
        actual = (month_key(row[1], system=payload.get("date_system")) if period_profile else
                  normalize_period(row[1], system=payload.get("date_system")))
        # At most three calendar precisions exist; do not scan every selected
        # date for every exported row. Mixed/ambiguous precisions still fail.
        candidates = list(dict.fromkeys(actual[:width] for width in (10, 7, 4)
                                        if actual[:width] in periods))
        if len(candidates) != 1 or (code, candidates[0]) not in expected:
            raise ValueError("Source key outside requested query grid")
        key = (code, candidates[0])
        row_key=key
        if keys_count==3:
            record=row[2]
            if (not isinstance(record,(str,int,float)) or isinstance(record,bool) or str(record)==''
                    or isinstance(record,float) and not math.isfinite(record)):
                raise ValueError('Missing/non-finite native record key')
            row_key=(*key,str(record))
        if row_key in found:
            raise ValueError("Duplicate source query-grid key")
        found.add(row_key); keys.append(key)
        for i, value in enumerate(row[keys_count:]):
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("Non-finite source value")
            if value is not None and value != "":
                counts[i] += 1
                first[i] = min(first[i], key[1]) if first[i] else key[1]
                last[i] = max(last[i], key[1]) if last[i] else key[1]
    # Native-period mode omits company/period pairs for which TEJ has no row.
    # The source Preview row count must agree with the bounded Excel export;
    # do not manufacture a Cartesian row or confuse sparse source output with
    # a lost export. Out-of-scope and duplicate keys were rejected above.
    sampled = validate_preview(cells[0], cells, payload.get("preview"), date_system=payload.get("date_system"), keys_count=keys_count)
    requested_rows = len(symbols) * len({normalize_period(d) for d in request["date_labels"]})
    return headers, cells[1:], {"keys": keys, "non_null_counts": counts, "first": first, "last": last,
                               "requested_query_rows": requested_rows,
                               "omitted_query_grid_rows": None if period_profile else len(expected - set(keys)),
                               **period_profile,
                               **({"source_period_scope_rows": len(expected),
                                   "omitted_source_period_scope_rows": len(expected - set(keys))} if period_profile else {}),
                               "preview_sample_rows_verified": sampled,
                               "preview_header_mapping_contract":header_contract,
                               "native_observation_completeness_verified": False, "publication_verified": False}


def validate_download_evidence(request: dict, payload: dict) -> tuple[list[str], list[list], dict]:
    """A verified empty response resolves a query, not any observed data rows."""
    from downloader.tej_key_layout import key_count, KEY1_CONTRACT, KEY3_CONTRACT
    keys_count=key_count(request)
    if key_count(request)==3 and (payload.get('source_key_mode')!=3 or payload.get('key_layout_contract')!=KEY3_CONTRACT):
        raise ValueError('Exact Key=3 source layout required even for an empty reply')
    outcome = payload.get("source_outcome")
    if outcome != "explicit_empty_scope":
        if outcome not in {None, "exported_query_grid"}:
            raise ValueError("Unrecognized source outcome")
        return validate_export(request, payload)
    validate_source_scope(request, payload, "download")
    if keys_count==1 and (payload.get('source_key_mode')!=1 or payload.get('key_layout_contract')!=KEY1_CONTRACT
                         or request.get('date_labels')!=[]):
        raise ValueError('Exact empty Key=1 snapshot contract required')
    if (payload.get("capture_method") != "owned_vendor_empty_dialog"
            or payload.get("query_comments_read") is not False
            or payload.get("cells") not in (None, [])
            or not re.fullmatch(r"ERROR1:No data !!\([a-zA-Z0-9_]{1,32}\)", str(payload.get("source_message")))):
        raise ValueError("Unverified provider empty response")
    # The bridge verifies the actual source/field/company/date selections before
    # acknowledging this exact normal response. Unknown notices are not empty.
    empty_headers=['CO_ID'] if keys_count==1 else ['CO_ID','Date']+(['Source_Record_Key'] if keys_count==3 else [])
    requested_rows=len(request['company_labels'])*(1 if keys_count==1 else len(request['date_labels']))
    return [*empty_headers, *request["fields"]], [], {
        "keys": [], "non_null_counts": [0] * len(request["fields"]),
        "first": [None] * len(request["fields"]), "last": [None] * len(request["fields"]),
        "preview_sample_rows_verified": 0,
        "requested_query_rows": requested_rows,
        "omitted_query_grid_rows": requested_rows,
    }


def ingest_export(root: Path, task: dict, payload: dict, output: Path) -> dict:
    import pyarrow as pa
    from downloader.tej_key_layout import key_count, parquet_relative, KEY1_CONTRACT, KEY3_CONTRACT, SNAPSHOT_DATE_AXIS
    request = task_request(root, task)
    keys_count=key_count(request)
    if payload.get("task_id") != task["task_id"]:
        raise ValueError("Export evidence belongs to another task")
    empty = payload.get("source_outcome") == "explicit_empty_scope"
    headers, rows, profile = validate_download_evidence(request, payload)
    arrays = {}
    schema_notes = {}
    for i, name in enumerate(headers):
        values = [r[i] for r in rows]
        if all(v is None or isinstance(v, (float, int)) and not isinstance(v, bool) for v in values):
            arrays[name] = pa.array(values)
        else:
            # Mixed/text columns remain source strings; the lossless raw JSON
            # retains original cell types and empty/null distinction.
            arrays[name] = pa.array([None if v is None else str(v) for v in values])
            schema_notes[name] = "text_or_mixed_source_cells_original_types_retained_in_raw"
    arrays["_query_symbol"] = pa.array([k[0] for k in profile["keys"]])
    arrays["_query_period"] = pa.array([k[1] for k in profile["keys"]],type=pa.string())
    if keys_count==1:
        arrays['_snapshot_observed_at_utc']=pa.array([payload['observed_at_utc']]*len(rows),type=pa.string())
    if keys_count==3:
        arrays['_native_record_key']=pa.array([str(r[2]) for r in rows],type=pa.string())
    normalized = {}
    scale_selection = payload.get("vendor_numeric_scale_selection")
    for i, field in enumerate(request["fields"], keys_count):
        values = [r[i] for r in rows]
        try:
            decimals = [None if v is None or v == "" else display_decimal(v) for v in values]
        except ValueError:
            continue  # Keep unknown locale/text/sentinels raw; never fill zero.
        suffix = stable_id(field)
        if payload.get("capture_method") == "native_msaa_preview_full":
            try:
                arrays[f"_numeric_{suffix}"] = pa.array(decimals)
            except (pa.ArrowInvalid, OverflowError):
                schema_notes[field] = "display_decimal_exceeds_arrow_bound_raw_preserved"
        if "1000S" not in field and "NTD1000" not in field and not re.search(r"NTD\s*MN", field):
            continue
        if ((isinstance(scale_selection, str) and scale_selection.startswith("vendor_scale_"))
                or payload.get('vendor_numeric_scale_readback_basis') == 'current_ui_only_original_query_setting_unverified'):
            # An explicit display conversion may already have rescaled these
            # values. Never multiply the header's original unit a second time.
            schema_notes[field] = "display_scale_conversion_requires_source_unit_review_raw_preserved"
            continue
        scale = 1_000_000 if re.search(r"NTD\s*MN", field) else 1000
        decimals = [scale_decimal_power10(v, 6 if scale == 1_000_000 else 3) if v is not None else None for v in decimals]
        try:
            arrays[f"_normalized_{'shares' if '1000S' in field else 'twd'}_{suffix}"] = pa.array(decimals)
            normalized[field] = {"factor": scale, "unit": "shares" if "1000S" in field else "TWD"}
        except (pa.ArrowInvalid, OverflowError):
            schema_notes[field] = "normalization_precision_exceeds_arrow_decimal_bound_raw_preserved"
    method = payload.get("capture_method", "legacy_excel_value2_with_bounded_preview_samples")
    date_axis=SNAPSHOT_DATE_AXIS if keys_count==1 else DATE_AXIS
    frame = pa.table(arrays).replace_schema_metadata({b"stockagent.date_axis": date_axis.encode(),
                                                     b"stockagent.capture_method": method.encode(),
                                                     b"stockagent.preview_header_mapping_contract": (profile.get('preview_header_mapping_contract') or 'exact_legacy_headers').encode(),
                                                     b"stockagent.normalization_contract": NORMALIZATION_CONTRACT.encode(),
                                                     b"stockagent.publication_verified": b"false"})
    if profile.get('source_period_key_contract'):
        frame = frame.replace_schema_metadata({**frame.schema.metadata,
            b'stockagent.source_period_key_contract': profile['source_period_key_contract'].encode(),
            b'stockagent.source_period_grain': profile['source_period_grain'].encode()})
    parquet = root / parquet_relative(task,payload)
    atomic_write_parquet(parquet, frame, compression="zstd", durable=True)
    receipt = {"contract_version": payload["contract_version"], "provider": "tej_smart_wizard", "task_id": task["task_id"],
               "table_id": task["table_id"], "observed_at_utc": payload["observed_at_utc"], "date_axis": date_axis,
               "capture_method": method, "source_value_representation": payload.get("source_value_representation", "excel_value2"),
               "source_key_headers": headers[:keys_count], "requested_query_rows": profile["requested_query_rows"],
               "preview_header_mapping_contract":profile.get('preview_header_mapping_contract'),
               "native_source_headers":payload['cells'][0] if not empty else None,
               "omitted_query_grid_rows": profile["omitted_query_grid_rows"],
               "exported_rows": len(rows), "exported_non_null_cells": sum(profile["non_null_counts"]),
               "field_non_null_counts": dict(zip(request["fields"], profile["non_null_counts"])),
               "field_first_query_period": dict(zip(request["fields"], profile["first"])),
               "field_last_query_period": dict(zip(request["fields"], profile["last"])),
               "first_query_period": min((p for _, p in profile["keys"] if p is not None),default=None), "last_query_period": max((p for _, p in profile["keys"] if p is not None),default=None),
               "raw_path": str(output.relative_to(root)), "raw_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
               "raw_bytes": output.stat().st_size, "parquet_path": str(parquet.relative_to(root)),
               "parquet_sha256": hashlib.sha256(parquet.read_bytes()).hexdigest(), "parquet_bytes": parquet.stat().st_size,
               "normalization": normalized, "schema_notes": schema_notes, "source_values_retained": True,
               "source_outcome": 'explicit_empty_scope' if empty else 'exported_query_grid',
               "msaa_value_read_path": payload.get("msaa_value_read_path"),
               "msaa_object_fallback_cells": payload.get("msaa_object_fallback_cells"),
               "vendor_numeric_scale_selection": scale_selection,
               "vendor_numeric_scale_readback_basis": payload.get('vendor_numeric_scale_readback_basis'),
               "sector_filter_applicable": payload.get("sector_filter_applicable"),
               "field_partition": request.get('field_partition'),
               "source_scope_proof": payload.get("source_scope_proof"),
               "all_source_units_verified": False,
               "normalization_contract": NORMALIZATION_CONTRACT,
               "normalization_basis": "explicit_field_name_units_not_full_source_unit_certification",
               "preview_parity": "not_applicable_provider_empty" if empty else "full_source_grid_with_bounded_samples_not_independent_excel_parity" if method == "native_msaa_preview_full" else "bounded_leading_trailing_samples", "preview_sample_rows_verified": profile["preview_sample_rows_verified"],
               "native_observation_completeness_verified": False, "publication_verified": False,
               "training_auto_injected": False, "raw_source_publish": False}
    receipt.update({key: value for key, value in profile.items() if key in {
        'source_period_key_contract', 'source_period_grain', 'source_period_basis',
        'requested_grid_periods', 'source_period_scope_periods', 'source_period_scope_rows',
        'omitted_source_period_scope_rows', 'grid_to_source_period_mapping', 'source_observation_cadence_verified'}})
    if payload.get('source_period_interpretation_path'):
        receipt.update({key:payload[key] for key in ('source_period_interpretation_path','source_period_interpretation_sha256')})
    if keys_count==3:
        receipt.update(key_layout_contract=KEY3_CONTRACT,source_key_mode=3,
                       row_grain='native_company_period_record_key_not_collapsed_to_company_day',
                       source_key_headers_basis='empty_schema_placeholders_not_observed_source_headers' if empty else 'captured_native_grid_headers')
    if keys_count==1:
        receipt.update(key_layout_contract=KEY1_CONTRACT,source_key_mode=1,
                       row_grain='native_company_current_snapshot_not_historical_observation',
                       historical_values_reconstructed=False,
                       source_key_headers_basis='empty_schema_placeholders_not_observed_source_headers' if empty else 'captured_native_grid_headers')
    from downloader.tej_desktop_attempts import receipt_attempt_evidence
    receipt.update(receipt_attempt_evidence(root,task,request,payload))
    receipt_path = root / "receipts" / (task["task_id"] + ".json")
    atomic_write_json(receipt_path, receipt)
    return receipt


class DesktopBridge:
    """Call the reviewed script without relaxing Windows execution policy."""
    def __init__(self, repo: Path, session: dict):
        self.repo, self.session = repo, session

    @staticmethod
    def quote(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def windows_path(path: Path) -> str:
        return subprocess.check_output(["wslpath", "-w", str(path.resolve())], text=True).strip()

    def execute(self, root: Path, task: dict) -> tuple[dict, Path, float]:
        name = task["task_id"] + "-" + uuid.uuid4().hex
        request = root / "requests" / (name + ".json")
        output = root / "raw" / (name + ".json")
        output.parent.mkdir(parents=True, exist_ok=True)
        wire_request = task_request(root, task)
        is_preview = wire_request.get('action') == 'download'
        if is_preview:
            wire_request = {**wire_request, 'query_attempt_id':name}
        atomic_write_json(request, {**wire_request, "contract_version": CONTRACT_VERSION,
                                    "task_id": task["task_id"], "desktop_input_contract": DESKTOP_INPUT_CONTRACT})
        source = self.quote(self.windows_path(self.repo / "scripts/tej_smart_wizard_bridge.ps1"))
        args = " ".join(f"-{key} {self.quote(str(value))}" for key, value in self.session.items()
                        if key in {"TejProcessId", "ExpectedWindow", "ExpectedTitle", "ExpectedWorkbook", "ExpectedExcelWindow"})
        if set(self.session) != {"TejProcessId", "ExpectedWindow", "ExpectedTitle", "ExpectedWorkbook", "ExpectedExcelWindow"}:
            raise ValueError("Unreviewed desktop session scope")
        if is_preview:
            from downloader.tej_desktop_attempts import begin_attempt
            begin_attempt(root,task,name,request,output)
        command = ("$ErrorActionPreference='Stop';$s=" + source + ";"
                   "$d=Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) ('StockAgent\\TEJSmartWizard\\worker-'+[Guid]::NewGuid().ToString('N'));"
                   "[void](New-Item -ItemType Directory -Path $d);$p=Join-Path $d 'bridge.ps1';Copy-Item -LiteralPath $s -Destination $p;"
                   "if((Get-FileHash -LiteralPath $s).Hash -cne (Get-FileHash -LiteralPath $p).Hash){throw 'Script copy mismatch'};"
                   f"try{{& $p {args} -Request {self.quote(self.windows_path(request))} -Output {self.quote(self.windows_path(output))}}}"
                   "catch{[Console]::Error.WriteLine($_.Exception.Message);[Console]::Error.WriteLine($_.ScriptStackTrace);exit 1}")
        started = time.monotonic()
        try:
            result = subprocess.run(["/mnt/c/WINDOWS/System32/WindowsPowerShell/v1.0/powershell.exe",
                                 "-NoProfile", "-NonInteractive", "-Command", command],
                                # A complete bounded MSAA grid is read across
                                # processes cell by cell. The former four-minute
                                # deadline killed valid large readbacks after
                                # the single Preview was already submitted.
                                # Still finite; a timeout never resends it.
                                capture_output=True, timeout=900)
        except subprocess.TimeoutExpired as exc:
            if is_preview:
                from downloader.tej_desktop_attempts import finish_attempt
                finish_attempt(root,name,'unknown_outcome')
            diagnostic = root / "diagnostics" / (name + ".txt")
            atomic_write_bytes(diagnostic, (exc.stderr or b"")[:32768] +
                               b"\nfinite_desktop_deadline_exceeded; no data query retry\n")
            diagnostic.chmod(0o600)
            # TimeoutExpired renders its command line; keep session paths and
            # vendor diagnostics out of public/logged operator error messages.
            raise RuntimeError("desktop_bridge_deadline_exceeded_requires_recovery") from None
        if result.returncode or not output.is_file():
            if is_preview:
                from downloader.tej_desktop_attempts import finish_attempt
                finish_attempt(root,name,'unknown_outcome')
            # Vendor/COM errors can contain account names, settings or paths.
            # Preserve no such strings in public status or console output.
            diagnostic = root / "diagnostics" / (name + ".txt")
            atomic_write_bytes(diagnostic, result.stderr[:32768])
            diagnostic.chmod(0o600)
            outcome_path = output.with_suffix(output.suffix + ".outcome.json")
            if outcome_path.is_file() and outcome_path.stat().st_size <= 4096:
                try:
                    outcome = json.loads(outcome_path.read_text(encoding="utf-8-sig"))
                    expected = wire_request
                    if (expected.get('action')=='download' and outcome.get('action')=='download'
                            and outcome.get('provider')=='tej_smart_wizard' and outcome.get('contract_version')==CONTRACT_VERSION
                            and outcome.get('task_id')==task['task_id']
                            and outcome.get('market_data_query_submission_possible') is False
                            and outcome.get('error_code')=='source_key_layout_failed_before_preview'
                            and outcome.get('source_key_mode') in (1,3)
                            and all(outcome.get(k)==expected.get(k) for k in ('type','smart_id','table'))):
                        finish_attempt(root,name,'proven_not_submitted')
                        raise SourceKeyLayoutError('source_key_layout_replan_required',outcome['source_key_mode'])
                    if (expected.get("action") == "download" and outcome.get("action") == "download"
                            and outcome.get("provider") == "tej_smart_wizard" and outcome.get("contract_version") == CONTRACT_VERSION
                            and outcome.get("task_id") == task["task_id"]
                            and outcome.get("market_data_query_submission_possible") is False
                            and outcome.get("error_code") in PREQUERY_FAILURES
                            and all(outcome.get(k) == expected.get(k) for k in ("type", "smart_id", "table"))):
                        finish_attempt(root,name,'proven_not_submitted')
                        raise BeforeDataQueryError(outcome['error_code'])
                    if (expected.get("action") == "plan" and outcome.get("action") == "plan"
                            and outcome.get("provider") == "tej_smart_wizard" and outcome.get("contract_version") == CONTRACT_VERSION
                            and outcome.get("task_id") == task["task_id"]
                            and outcome.get("market_data_query_submission_possible") is False
                            and outcome.get("error_code") == "metadata_preparation_failed_before_preview"
                            and all(outcome.get(k) == expected.get(k) for k in ("type", "smart_id", "table"))):
                        raise MetadataPreparationError(request)
                except (ValueError, OSError):
                    pass
            raise RuntimeError("desktop_bridge_failed_requires_scoped_diagnostic")
        payload = json.loads(output.read_text(encoding="utf-8-sig"))
        if is_preview:
            from downloader.tej_desktop_attempts import finish_attempt
            if payload.get('query_attempt_id') != name:
                raise ValueError('Desktop response does not match this query attempt')
            finish_attempt(root,name,'response_received_not_yet_adopted')
        return payload, output, time.monotonic() - started


def verify_desktop_input(root: Path, task_id: str, bridge: DesktopBridge) -> dict:
    """Explicit same-value UI acceptance, never a query/recovery or scheduler action.

    The CLI must hold the canonical acquisition lock. Preserve every queue state
    including an unresolved Preview; save the private input proof separately.
    """
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None or row['kind'] != 'download':
        raise ValueError('Input acceptance requires one exact registered download task')
    task = dict(row)
    request = task_request(root, task)
    payload, output, seconds = bridge.execute(root, {**task, 'request_json':json.dumps({**request, 'action':'probe_date_input'})})
    if (payload.get('contract_version') != CONTRACT_VERSION or payload.get('provider') != 'tej_smart_wizard'
            or payload.get('task_id') != task_id or payload.get('action') != 'probe_date_input'
            or payload.get('desktop_input_contract') != DESKTOP_INPUT_CONTRACT
            or any(payload.get(k) != request.get(k) for k in ('type', 'smart_id', 'table'))
            or payload.get('market_data_query_submitted') is not False
            or payload.get('early_noop_path_used') is not False
            or any(payload.get(k) is not True for k in ('date_text_input_sent', 'binding_matches_failed_plan',
                 'query_axes_unchanged', 'source_binding_unchanged', 'field_selection_unchanged'))):
        raise ValueError('Unverified mouse-free input acceptance; queue unchanged')
    results = payload.get('date_input_results')
    if not isinstance(results, list) or len(results) != 2:
        raise ValueError('Both exact date input results are required; queue unchanged')
    for result, key in zip(results, ('start', 'end'), strict=True):
        expected = request[key].replace('-', '/')
        trace = result.get('input_trace') if isinstance(result, dict) else None
        if (not isinstance(trace, list) or len(trace) != 4 or not all(isinstance(v, str) for v in trace)
                or trace[0] != expected or trace[1] == trace[0] or trace[2].replace('/', '') != expected.replace('/', '')
                or trace[3] != expected or result.get('target_text_before') != expected
                or result.get('target_text_after') != expected
                or any(result.get(k) is not True for k in ('same_value_write', 'verified_exact_box', 'intermediate_edit_observed'))):
            raise ValueError('Actual date edit and exact original readback required; queue unchanged')
    with closing(connect(root)) as con:
        current = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
    if current is None or dict(current) != task:
        raise ValueError('Queue changed during input acceptance; no recovery performed')
    audit = {'contract_version':1, 'desktop_input_contract':DESKTOP_INPUT_CONTRACT, 'task_id':task_id,
             'observed_at_utc':datetime.now(UTC).isoformat(), 'source_evidence_path':str(output.resolve()),
             'source_evidence_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
             'seconds':seconds, 'both_date_writes_verified':True, 'intermediate_edits_observed':True,
             'source_binding_unchanged':True, 'field_selection_unchanged':True, 'query_axes_unchanged':True,
             'queue_unchanged':True, 'data_query_repeated':False, 'source_rows_adopted':False,
             'state_before':task['state'], 'state_after':current['state'], 'unknown_outcome_barrier_reset':False}
    atomic_write_json(root/'input_acceptance'/(output.stem+'.json'), audit)
    return {k:audit[k] for k in ('desktop_input_contract', 'both_date_writes_verified', 'intermediate_edits_observed',
                               'queue_unchanged', 'data_query_repeated', 'source_rows_adopted', 'unknown_outcome_barrier_reset')}


def commit_evidence(root: Path, task: dict, payload: dict, output: Path, seconds: float | None,
                    *, event_id: str | None = None) -> None:
    """Adopt durable evidence once, without repeating any vendor query."""
    if payload.get("task_id") != task["task_id"]:
        raise ValueError("Evidence belongs to another task")
    if task['kind'] == 'download':
        from downloader.tej_period_keys import interpreted_payload
        payload = interpreted_payload(root, task, payload)
    if task["kind"] == "discover":
        discovery_tasks(root, task, payload, output)
        receipt_path, rows, size, receipt = None, None, output.stat().st_size, None
    else:
        from downloader.tej_desktop_attempts import validate_active_attempt
        validate_active_attempt(root,task,payload)
        existing = root / "receipts" / (task["task_id"] + ".json")
        if existing.is_file():
            receipt = json.loads(existing.read_text())
            if receipt.get("task_id") != task["task_id"] or receipt.get("raw_sha256") != hashlib.sha256(output.read_bytes()).hexdigest():
                raise ValueError("Existing receipt differs; refusing to overwrite source evidence")
            parquet = root / receipt["parquet_path"]
            if not parquet.is_file() or hashlib.sha256(parquet.read_bytes()).hexdigest() != receipt.get("parquet_sha256"):
                raise ValueError("Receipt artifact digest mismatch")
            validate_download_evidence(task_request(root, task), payload)
        else:
            receipt = ingest_export(root, task, payload, output)
        receipt_path = str(Path("receipts") / (task["task_id"] + ".json"))
        rows, size = receipt["exported_rows"], receipt["raw_bytes"] + receipt["parquet_bytes"]
    complete = datetime.now(UTC).isoformat()
    with closing(connect(root)) as con, con:
        already = con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0]
        if task["kind"] == "download" and already != "complete":
            for field, count in receipt["field_non_null_counts"].items():
                first, last = receipt["field_first_query_period"][field], receipt["field_last_query_period"][field]
                con.execute("UPDATE features SET exported_non_null_cells=COALESCE(exported_non_null_cells,0)+?,first_query_period=CASE WHEN ? IS NULL THEN first_query_period WHEN first_query_period IS NULL THEN ? ELSE MIN(first_query_period,?) END,last_query_period=CASE WHEN ? IS NULL THEN last_query_period WHEN last_query_period IS NULL THEN ? ELSE MAX(last_query_period,?) END WHERE table_id=? AND name=?",
                            (count, first, first, first, last, last, last, task["table_id"], field))
        if already != "complete":
            timing_basis = "fresh_bridge_only" if event_id and seconds is not None else "recovery_or_unmeasured"
            con.execute("UPDATE tasks SET state='complete',completed_at_utc=?,actual_rows=?,actual_bytes=?,seconds=?,receipt_path=?,output_path=?,timing_basis=?,last_error_code=NULL WHERE task_id=?",
                        (complete, rows, size, seconds, receipt_path, str(output.relative_to(root)), timing_basis, task["task_id"]))
        if not con.execute("SELECT 1 FROM tasks WHERE table_id=? AND state='blocked' LIMIT 1", (task["table_id"],)).fetchone():
            con.execute("UPDATE tables SET last_error_code=NULL,state=CASE WHEN grid_rows>0 THEN 'backfilling' ELSE state END WHERE table_id=?", (task["table_id"],))
        if event_id:
            con.execute("UPDATE traffic SET completed_at_utc=?,state='complete' WHERE event_id=?", (complete, event_id))
        if task['kind']=='download':
            con.execute("UPDATE desktop_attempts SET state='adopted',finished_at_utc=? WHERE attempt_id="
                        "(SELECT active_attempt_id FROM tasks WHERE task_id=?)",(complete,task['task_id']))


def recover_evidence(root: Path, task_id: str, output: Path) -> None:
    raw = root.resolve() / "raw"
    output = output.resolve()
    if output.parent != raw or output.suffix != ".json" or not output.name.startswith(task_id + "-"):
        raise ValueError("Recovery requires the exact private raw evidence file for this task")
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("Unknown task")
    task = dict(row)
    payload = json.loads(output.read_text(encoding="utf-8-sig"))
    if payload.get("task_id") != task_id:
        raise ValueError("Recovery task identity mismatch")
    commit_evidence(root, task, payload, output, None)
    atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION,
                       "state": "evidence_recovered", "observed_at_utc": datetime.now(UTC).isoformat()})


def settle_metadata_failure(root: Path, task_id: str, bridge: Any, evidence: Path) -> dict:
    """Defer one proved failed metadata lookup, never an unknown data Preview.

    Operator-only after an exact owned runtime notice was inspected/acknowledged.
    No scope is adopted and the failed table remains visibly blocked. Other
    tables can proceed only after the shared UI is verified usable again.
    """
    with closing(connect(root)) as con:
        row=con.execute("SELECT * FROM tasks WHERE task_id=?",(task_id,)).fetchone()
    if row is None or row['kind']!='discover' or row['state']!='blocked':
        raise ValueError('Settlement requires one exact blocked discovery, never a data query')
    task=dict(row);req=task_request(root,task)
    if req.get('action')!='plan' or req.get('contract_version')!=CONTRACT_VERSION:
        raise ValueError('Only a reviewed metadata-only plan can be deferred')
    evidence=evidence.resolve()
    if evidence.parent!=(root/'raw').resolve() or evidence.stat().st_size>4096:
        raise ValueError('Exact private runtime acknowledgement evidence required')
    ack=json.loads(evidence.read_text(encoding='utf-8-sig'))
    if (ack.get('contract_version')!=CONTRACT_VERSION or ack.get('provider')!='tej_smart_wizard'
            or ack.get('task_id')!=task_id or ack.get('action')!='ack_source_memory_error'
            or ack.get('error_code') not in {'vendor_mysql_allocation_failed','vendor_source_table_unresolved'}
            or ack.get('data_query_repeated') is not False or ack.get('credentials_read') is not False
            or any(ack.get(k)!=req.get(k) for k in ('type','smart_id','table'))):
        raise ValueError('Runtime acknowledgement does not match the failed metadata scope')
    req['action']='confirm_metadata_error_cleared'
    payload,output,_=bridge.execute(root,{**task,'request_json':json.dumps(req)})
    if (payload.get('contract_version')!=CONTRACT_VERSION or payload.get('provider')!='tej_smart_wizard'
            or payload.get('action')!='confirm_metadata_error_cleared' or payload.get('task_id')!=task_id
            or any(payload.get(k)!=req.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in ('vendor_notices_absent','source_selectors_enabled'))
            or payload.get('market_data_query_submitted') is not False or payload.get('credentials_read') is not False):
        raise ValueError('Shared source interface is not verified usable; keep the barrier')
    code='vendor_metadata_allocation_failed_deferred'
    audit={'contract_version':CONTRACT_VERSION,'task_id':task_id,'state':code,
           'source_error_code':ack['error_code'],'metadata_only_plan':True,'data_query_repeated':False,
           'source_axes_adopted':False,'source_files_deleted':False,
           'ack_evidence_sha256':hashlib.sha256(evidence.read_bytes()).hexdigest(),
           'interface_readback_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
           'observed_at_utc':datetime.now(UTC).isoformat()}
    atomic_write_json(root/'diagnostics'/(task_id+'-metadata_failure_deferred.json'),audit)
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET last_error_code=? WHERE task_id=? AND kind='discover' AND state='blocked'",(code,task_id))
        con.execute("UPDATE tables SET state='needs_review',last_error_code=? WHERE table_id=?",(code,task['table_id']))
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':code,
                                               'observed_at_utc':audit['observed_at_utc'],'task_id':task_id})
    return {'state':code,'failed_table_still_blocked':True,'data_query_repeated':False,'source_axes_adopted':False}


def isolate_failed_metadata(root: Path, task_id: str, bridge: Any, prepared_request: Path,
                            *, acknowledge_known_notices: bool = False) -> dict:
    """Isolate one exact plan; never retry it or adopt its axes.

    The original private request must be the complete v4 metadata-only plan.
    Unknown data downloads, foreign notices and permission/quota/authentication
    dialogs remain hard barriers. Only two independently inspected, exact
    owned runtime notices may be acknowledged when explicitly enabled.
    """
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if row is None or row['kind'] != 'discover' or row['state'] != 'blocked':
        raise ValueError('Isolation requires one exact blocked metadata plan, never a data query')
    task = dict(row); req = task_request(root, task)
    prepared_request = prepared_request.resolve()
    if (prepared_request.parent != (root/'requests').resolve()
            or not prepared_request.name.startswith(task_id+'-')
            or prepared_request.suffix != '.json' or prepared_request.stat().st_size > 2*1024**2):
        raise ValueError('Exact private original plan request required')
    original = json.loads(prepared_request.read_text(encoding='utf-8-sig'))
    from downloader.tej_desktop_attempts import prepared_request_matches
    if (req.get('action') != 'plan' or req.get('contract_version') != CONTRACT_VERSION
            or not prepared_request_matches(original,req,task)):
        raise ValueError('Original request is not the exact reviewed metadata-only plan')
    known = {'MySql:malloc':'vendor_mysql_allocation_failed',
             'Cannot find table 0.':'vendor_source_table_unresolved'}
    evidence_paths, codes = [], []
    for attempt in range(3):
        inspection = {**req, 'action':'inspect_notices'}
        payload, output, _ = bridge.execute(root, {**task, 'request_json':json.dumps(inspection)})
        if (payload.get('action') != 'inspect_notices' or payload.get('contract_version') != CONTRACT_VERSION
                or payload.get('task_id') != task_id or payload.get('notice_acknowledged') is not False
                or payload.get('data_query_repeated') is not False or payload.get('credentials_read') is not False
                or not isinstance(payload.get('notices'),list)):
            raise ValueError('Unverified owned-notice inspection; keep the barrier')
        evidence_paths.append(output)
        notices = payload['notices']
        if not notices:
            if payload.get('root_enabled') is False or payload.get('root_automation_enabled') is False:
                from downloader.tej_query_lifecycle import mark_interface_barrier
                mark_interface_barrier(root,reason='vendor_query_disabled_without_known_notice',evidence=output)
                raise RuntimeError('desktop_interface_recovery_required')
            break
        if not acknowledge_known_notices or attempt >= 2 or len(notices) != 1:
            raise RuntimeError('metadata_interface_not_restored_no_auto_retry')
        notice = notices[0]; nodes = notice.get('controls',[])
        texts = [n for n in nodes if n.get('class') == 'Static' and n.get('name')]
        buttons = [n for n in nodes if n.get('class') == 'Button']
        code = known.get(texts[0].get('name')) if len(texts) == 1 else None
        owner = getattr(bridge,'session',{}).get('ExpectedWindow')
        handle = notice.get('handle')
        if (not code or not isinstance(handle,int) or isinstance(handle,bool) or handle <= 0
                or owner is None or notice.get('owner') != owner or len(buttons) != 1
                or buttons[0].get('name') != 'OK' or buttons[0].get('enabled') is not True):
            raise RuntimeError('unreviewed_metadata_notice_not_acknowledged')
        ack = {**req, 'action':'ack_source_memory_error','error_window':handle,'error_code':code}
        payload, output, _ = bridge.execute(root, {**task, 'request_json':json.dumps(ack)})
        if (payload.get('action') != ack['action'] or payload.get('contract_version') != CONTRACT_VERSION
                or payload.get('provider') != 'tej_smart_wizard' or payload.get('task_id') != task_id
                or any(payload.get(k) != req.get(k) for k in ('type','smart_id','table'))
                or payload.get('error_code') != code or payload.get('data_query_repeated') is not False
                or payload.get('credentials_read') is not False):
            raise ValueError('Exact runtime acknowledgement was not verified')
        evidence_paths.append(output); codes.append(code)
    check = {**req, 'action':'confirm_metadata_error_cleared'}
    payload, output, _ = bridge.execute(root, {**task, 'request_json':json.dumps(check)})
    if (payload.get('contract_version') != CONTRACT_VERSION or payload.get('provider') != 'tej_smart_wizard'
            or payload.get('action') != check['action'] or payload.get('task_id') != task_id
            or any(payload.get(k) != req.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in
                   ('vendor_notices_absent','source_selectors_enabled','source_binding_stable'))
            or payload.get('market_data_query_submitted') is not False or payload.get('credentials_read') is not False):
        raise ValueError('Shared source interface not verified stable; keep the barrier')
    evidence_paths.append(output)
    code = 'vendor_metadata_allocation_failed_deferred' if codes else 'metadata_preparation_failed_deferred'
    audit = {'contract':METADATA_RECOVERY_CONTRACT,'contract_version':CONTRACT_VERSION,'task_id':task_id,
             'state':code,'observed_at_utc':datetime.now(UTC).isoformat(),
             'source_error_codes':codes,'metadata_only_plan':True,'failed_plan_retried':False,
             'data_query_repeated':False,'source_axes_adopted':False,'failed_table_still_blocked':True,
             'prepared_request_sha256':hashlib.sha256(prepared_request.read_bytes()).hexdigest(),
             'readback_evidence_sha256':[hashlib.sha256(p.read_bytes()).hexdigest() for p in evidence_paths]}
    atomic_write_json(root/'diagnostics'/(task_id+'-metadata_isolation-'+uuid.uuid4().hex+'.json'),audit)
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET last_error_code=? WHERE task_id=? AND kind='discover' AND state='blocked'",(code,task_id))
        con.execute("UPDATE tables SET state='needs_review',last_error_code=? WHERE table_id=?",(code,task['table_id']))
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':code,
                                               'observed_at_utc':audit['observed_at_utc'],'task_id':task_id})
    return {'state':code,'failed_table_still_blocked':True,'data_query_repeated':False,'source_axes_adopted':False}


def recover_desktop_response(root: Path, task_id: str, bridge: Any, *, response: str,
                             error_window: int | None = None) -> dict:
    """Explicit operator-only adoption; never sends another Preview query."""
    actions = {"preview": "recover_preview", "empty": "resolve_empty", "excel_error": "ack_excel_error",
               "plan": "recover_plan", "plan-empty-fields": "resume_plan_empty_fields", "plan-preparation": "plan"}
    if response not in actions:
        raise ValueError("Unknown desktop recovery response")
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        expected_kind = "discover" if response in {"plan", "plan-empty-fields", "plan-preparation"} else "download"
        if row is None or row["kind"] != expected_kind or row["state"] not in {"blocked", "running"}:
            raise ValueError("Recovery requires one exact unresolved task of the matching kind")
    task = dict(row)
    request = task_request(root, task)
    request["action"] = actions[response]
    if response in {"preview", "empty"} and request.get("contract_version") == CONTRACT_VERSION:
        from downloader.tej_desktop_attempts import query_stage
        stage, _ = query_stage(root,task,request)
        request["original_query_scope_proof"] = stage.get("source_scope_proof")
        if task.get('active_attempt_id'):
            request['query_attempt_id'] = task['active_attempt_id']
            request['original_query_stage'] = stage
    if response == "excel_error":
        if not isinstance(error_window, int) or isinstance(error_window, bool) or error_window <= 0:
            raise ValueError("Operator acknowledgement requires the exact inspected error HWND")
        request["error_window"] = error_window
    elif error_window is not None:
        raise ValueError("Error HWND is only accepted for the explicit Excel error response")
    if expected_kind=='discover':
        mark_worker_wait(root,kind='discover',seconds=900,metadata_recovery=True)
    payload, output, seconds = bridge.execute(root, {**task, "request_json": json.dumps(request)})
    if response == "excel_error":
        if (payload.get("action") != "ack_excel_error" or payload.get("task_id") != task_id
                or payload.get("data_query_repeated") is not False
                or payload.get("error_code") != "vendor_excel_com_initialization_failed"
                or payload.get("credentials_read") is not False):
            raise ValueError("Unverified operator acknowledgement")
        return {"state": "known_excel_error_acknowledged", "data_query_repeated": False}
    # Recovery does not repeat the query, so its readback-only duration is not
    # a sample of end-to-end fresh acquisition throughput.
    commit_evidence(root, task, payload, output, None)
    atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION,
                       "state": "evidence_recovered", "observed_at_utc": datetime.now(UTC).isoformat()})
    return {"state": "evidence_recovered", "data_query_repeated": False}


def configure_preview_planning(root: Path, config: dict) -> dict:
    """Upgrade pending encoding, never discard/re-query completed source scopes."""
    from downloader.tej_planning import CONTRACT, TILING_CONTRACT, install_plan
    if config.get('download_planning_contract')!=CONTRACT or config.get('preview_max_columns')!=30:
        raise ValueError('Explicit reviewed 30-column planner configuration required')
    if config.get('query_tiling_contract') not in (None, TILING_CONTRACT):
        raise ValueError('Unreviewed company/date tiling contract')
    # The one-time queue-state rewrite can journal database pages. Leave room
    # for that bounded operation; no VACUUM/deletion or source retirement.
    with closing(connect(root)) as con,con:
        old=con.execute("SELECT value FROM meta WHERE key='preview_planning_contract'").fetchone()
        if old:
            if old[0]!=CONTRACT:
                raise ValueError('Incompatible planner version')
            tiling = config.get('query_tiling_contract')
            saved_tiling = con.execute("SELECT value FROM meta WHERE key='query_tiling_contract'").fetchone()
            if tiling and (saved_tiling is None or saved_tiling[0] != tiling):
                raise ValueError('Run explicit upgrade-query-tiling before using a new tiling policy')
            return {'contract':CONTRACT,'already_installed':True}
        if shutil.disk_usage(root).free < config.get('minimum_free_disk_bytes',5*1024**3)+2*(root/'queue.sqlite3').stat().st_size:
            raise OSError('Insufficient headroom for non-destructive queue upgrade')
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone():
            raise ValueError('Unresolved source action blocks planner migration')
        rows=con.execute("SELECT * FROM tasks WHERE kind='discover' AND state='complete' AND scope_contract=?",(SOURCE_SCOPE_CONTRACT,)).fetchall()
        installed=[]
        for row in rows:
            task=dict(row); definition=dict(con.execute('SELECT * FROM tables WHERE table_id=?',(task['table_id'],)).fetchone())
            req=expand_request(json.loads(task['request_json']),definition,task['table_id'])
            raw=(root/task['output_path']).resolve()
            if raw.parent != (root/'raw').resolve() or raw.stat().st_size > 16*1024**2:
                raise ValueError('Exact verified discovery artifact required')
            payload=json.loads(raw.read_text(encoding='utf-8-sig')); validate_source_scope(req,payload,'plan')
            if payload.get('task_id')!=task['task_id'] or req['fields']!=json.loads(definition['fields_json']):
                raise ValueError('Discovery identity/schema mismatch')
            installed.append({'table_id':task['table_id'],**install_plan(con,definition,req,payload,config)})
        con.execute("INSERT INTO meta VALUES('preview_planning_contract',?)",(CONTRACT,))
        if config.get('query_tiling_contract'):
            con.execute("INSERT OR REPLACE INTO meta VALUES('query_tiling_contract',?)",(TILING_CONTRACT,))
    audit={'contract':CONTRACT,'observed_at_utc':datetime.now(UTC).isoformat(),
           'tables':installed,'source_files_deleted':False,'completed_scopes_preserved':True,
           'provider_queries_sent':0,'preview_max_columns':30,'max_fields_per_query':28}
    atomic_write_json(root/'diagnostics'/('preview_planning_upgrade-'+uuid.uuid4().hex+'.json'),audit)
    return {'contract':CONTRACT,'already_installed':False,'tables':len(installed),
            'logical_queries_remaining':sum(r['logical_queries'] for r in installed)}


def settle_preview_column_limit(root: Path, task_id: str, bridge: Any, prepared: Path, error_window: int) -> dict:
    """Resolve the exact E8033 rejection, never an unknown source error."""
    with closing(connect(root)) as con:
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
    if row is None or row['kind']!='download' or row['state']!='blocked' or row['last_error_code']!='unknown_outcome_no_auto_retry':
        raise ValueError('Exact unresolved Preview download required')
    task=dict(row); req=task_request(root,task); prepared=prepared.resolve()
    if (prepared.parent!=(root/'requests').resolve() or not prepared.name.startswith(task_id+'-')
            or prepared.suffix!='.json' or prepared.stat().st_size>2*1024**2
            or not isinstance(error_window,int) or isinstance(error_window,bool) or error_window<=0):
        raise ValueError('Exact rejected request and inspected HWND required')
    if task.get('active_attempt_id'):
        from downloader.tej_desktop_attempts import query_stage
        query_stage(root,task,req,prepared)
    elif json.loads(prepared.read_text(encoding='utf-8-sig'))!={**req,'contract_version':CONTRACT_VERSION,'task_id':task_id}:
        raise ValueError('Exact original rejected prepared request required')
    stage_path=root/'raw'/(prepared.name+'.stage.json')
    if stage_path.stat().st_size>2*1024**2:
        raise ValueError('Unreviewed query stage bound')
    stage=json.loads(stage_path.read_text(encoding='utf-8-sig'))
    proof=stage.get('source_scope_proof')
    if (stage.get('task_id')!=task_id or stage.get('contract_version')!=CONTRACT_VERSION or stage.get('stage')!='prepreview_verified'
            or any(stage.get(k)!=req.get(k) for k in ('type','smart_id','table','fields','company_labels','date_labels'))
            or not isinstance(proof,dict) or proof.get('contract')!=SOURCE_SCOPE_CONTRACT
            or any(proof.get(k) is not True for k in ('company_group_enabled','date_group_enabled','vendor_notices_absent','binding_readback_verified'))):
        raise ValueError('Original rejected query stage differs')
    ack={**req,'action':'ack_preview_column_limit','error_window':error_window}
    payload,output,_=bridge.execute(root,{**task,'request_json':json.dumps(ack)})
    if (payload.get('provider')!='tej_smart_wizard' or payload.get('contract_version')!=CONTRACT_VERSION
            or payload.get('task_id')!=task_id or payload.get('action')!=ack['action']
            or any(payload.get(k)!=req.get(k) for k in ('type','smart_id','table'))
            or payload.get('error_code')!='vendor_preview_30_column_limit' or payload.get('source_query_rejected') is not True
            or payload.get('data_query_repeated') is not False or payload.get('credentials_read') is not False):
        raise ValueError('Exact native Preview rejection was not acknowledged')
    check={**req,'action':'confirm_metadata_error_cleared'}
    verified,readback,_=bridge.execute(root,{**task,'request_json':json.dumps(check)})
    if (verified.get('action')!=check['action'] or verified.get('task_id')!=task_id
            or verified.get('contract_version')!=CONTRACT_VERSION or verified.get('provider')!='tej_smart_wizard'
            or any(verified.get(k)!=req.get(k) for k in ('type','smart_id','table'))
            or any(verified.get(k) is not True for k in ('source_binding_stable','vendor_notices_absent','source_selectors_enabled'))
            or verified.get('market_data_query_submitted') is not False or verified.get('credentials_read') is not False):
        raise ValueError('Shared interface is not verified restored; keep the barrier')
    code='preview_column_limit_repartition_required'
    audit={'contract':'exact_vendor_preview_limit_rejection_v1','task_id':task_id,'state':code,
           'original_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
           'original_stage_sha256':hashlib.sha256(stage_path.read_bytes()).hexdigest(),
           'ack_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'readback_sha256':hashlib.sha256(readback.read_bytes()).hexdigest(),
           'source_rows_adopted':False,'data_query_repeated':False,'observed_at_utc':datetime.now(UTC).isoformat()}
    atomic_write_json(root/'diagnostics'/(task_id+'-preview_limit-'+uuid.uuid4().hex+'.json'),audit)
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET last_error_code=? WHERE task_id=?',(code,task_id))
        con.execute('UPDATE tables SET last_error_code=? WHERE table_id=?',(code,task['table_id']))
        con.execute("UPDATE traffic SET state='rejected_preview_column_limit' WHERE action='download' AND started_at_utc=? AND state='failed'",(task['attempted_at_utc'],))
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':code,
                                              'observed_at_utc':audit['observed_at_utc'],'task_id':task_id})
    return {'state':code,'data_query_repeated':False,'source_rows_adopted':False}


def _mark_prequery_failure(con: sqlite3.Connection, task: dict, error_code: str='local_date_input_failed_before_preview') -> str:
    """One shared, durable two-retry budget; never change result counters."""
    if error_code in {'desktop_foreground_unavailable_before_preview','desktop_context_unavailable_before_preview'}:
        deadline = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
        con.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=?,last_error_code=? WHERE task_id=?",
                    (deadline,error_code,task['task_id']))
        return 'desktop_unavailable'
    retries = con.execute("SELECT safe_prequery_retries FROM tasks WHERE task_id=?", (task['task_id'],)).fetchone()[0]
    if retries < 2:
        deadline = (datetime.now(UTC) + timedelta(seconds=5)).isoformat()
        con.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=?,safe_prequery_retries=safe_prequery_retries+1,last_error_code=? WHERE task_id=?", (deadline,error_code,task['task_id']))
        if not con.execute("SELECT 1 FROM tasks WHERE table_id=? AND state='blocked' LIMIT 1", (task['table_id'],)).fetchone():
            con.execute("UPDATE tables SET state='backfilling',last_error_code=NULL WHERE table_id=?", (task['table_id'],))
        return 'prequery_retry_scheduled'
    code=PREQUERY_FAILURES[error_code]
    con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?", (code,task['task_id']))
    con.execute("UPDATE tables SET last_error_code=?,state='needs_review' WHERE table_id=?", (code,task['table_id']))
    return code


def recover_legacy_local_input(root: Path, task_id: str, bridge: Any, prepared_request: Path) -> dict:
    """Operator-only migration of reviewed legacy local-input failures.

    Check the exact private request/diagnostic and original Windows bridge
    copy, not today's source. Only reviewed hashes and exact before-Preview
    DateText/field-selection callsites qualify. Read the UI; never query here.
    New bridges emit structured pre-Preview evidence instead.
    """
    with closing(connect(root)) as con:
        row = con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
    if (row is None or row['kind'] != 'download' or row['state'] != 'blocked'
            or row['last_error_code'] != 'unknown_outcome_no_auto_retry'):
        raise ValueError('Requires one exact legacy unresolved download, not completed data')
    task = dict(row); req = task_request(root, task); original = prepared_request.resolve()
    from downloader.tej_desktop_attempts import prepared_request_matches
    if (req.get('action') != 'download' or req.get('contract_version') != CONTRACT_VERSION
            or original.parent != (root/'requests').resolve() or original.suffix != '.json'
            or not re.fullmatch(re.escape(task_id)+r'-[0-9a-f]{32}\.json', original.name)
            or original.stat().st_size > 2*1024**2
            or not prepared_request_matches(json.loads(original.read_text(encoding='utf-8-sig')), req, task)
            or task.get('active_attempt_id') and original.stem != task['active_attempt_id']):
        raise ValueError('Exact original v4 prepared download required')
    output = root/'raw'/original.name
    if output.exists() or output.with_suffix('.json.stage.json').exists() or output.with_suffix('.json.outcome.json').exists():
        raise ValueError('Structured source/stage evidence exists; use scoped evidence recovery')
    diagnostic = root/'diagnostics'/original.with_suffix('.txt').name
    if diagnostic.stat().st_size > 32768:
        raise ValueError('Unreviewed diagnostic bound')
    lines = diagnostic.read_text(encoding='utf-8-sig').splitlines()
    date_error='Exception calling "DateText" with "4" argument(s): "Foreground unavailable; no input sent"'
    field_error='Selected fields differ; no query'
    grid_error='Date disappeared from grid'
    layout_error='Stable outer date boxes unavailable; no date input'
    error_code='local_list_selection_failed_before_preview' if lines and lines[0] in {field_error,grid_error} else 'local_date_input_failed_before_preview'
    stack = (re.fullmatch(r'at <ScriptBlock>, (C:\\Users\\[^\\\r\n]+\\AppData\\Local\\StockAgent\\TEJSmartWizard\\worker-[0-9a-f]{32}\\bridge\.ps1): line ([0-9]+)',lines[1])
             if len(lines) == 3 and lines[0] in {date_error,field_error,grid_error,layout_error} and lines[2] == 'at <ScriptBlock>, <No file>: line 1' else None)
    if stack is None:
        raise ValueError('Unknown diagnostic is not proof of an unsent query')
    source = Path(subprocess.check_output(['wslpath','-u',stack[1]],text=True).strip())
    if source.stat().st_size > 256*1024:
        raise ValueError('Unreviewed original bridge bound')
    script = source.read_bytes(); source_lines = script.decode('utf-8-sig').splitlines()
    line = int(stack[2])-1
    expected_hash=(LEGACY_FIELD_SELECTION_BRIDGE_SHA256 if lines[0]==field_error else
                   LEGACY_DATE_GRID_BRIDGE_SHA256 if lines[0]==grid_error else
                   LEGACY_DATE_LAYOUT_BRIDGE_SHA256 if lines[0]==layout_error else LEGACY_DATE_INPUT_BRIDGE_SHA256)
    call_matches=0 <= line < len(source_lines) and ("{throw 'Selected fields differ; no query'}" in source_lines[line] if lines[0]==field_error else
        "if($index -lt 0){throw 'Date disappeared from grid'}" in source_lines[line] if lines[0]==grid_error else
        "{throw 'Stable outer date boxes unavailable; no date input'}" in source_lines[line] if lines[0]==layout_error else
        source_lines[line].strip().startswith('[TejBridgeNative]::DateText('))
    if hashlib.sha256(script).hexdigest()!=expected_hash or not call_matches:
        raise ValueError('Original script or exception callsite differs from reviewed pre-query boundary')
    check = {**req,'action':'confirm_metadata_error_cleared'}
    payload, readback, _ = bridge.execute(root,{**task,'request_json':json.dumps(check)})
    if (payload.get('provider') != 'tej_smart_wizard' or payload.get('contract_version') != CONTRACT_VERSION
            or payload.get('task_id') != task_id or payload.get('action') != check['action']
            or any(payload.get(k) != req.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in ('vendor_notices_absent','source_selectors_enabled','source_binding_stable'))
            or payload.get('market_data_query_submitted') is not False or payload.get('credentials_read') is not False):
        raise ValueError('Interface is not independently verified usable; keep the barrier')
    audit = {'contract':'verified_legacy_local_input_recovery_v1','task_id':task_id,'error_code':error_code,
             'original_request_sha256':hashlib.sha256(original.read_bytes()).hexdigest(),
             'original_diagnostic_sha256':hashlib.sha256(diagnostic.read_bytes()).hexdigest(),
             'original_bridge_sha256':hashlib.sha256(script).hexdigest(),
             'readback_sha256':hashlib.sha256(readback.read_bytes()).hexdigest(),
             'market_data_query_repeated':False,'source_rows_adopted':False,
             'observed_at_utc':datetime.now(UTC).isoformat()}
    atomic_write_json(root/'diagnostics'/(task_id+'-date_input_recovery-'+uuid.uuid4().hex+'.json'),audit)
    with closing(connect(root)) as con,con:
        state = _mark_prequery_failure(con,task,error_code)
        con.execute("UPDATE traffic SET state='failed_before_preview' WHERE action='download' AND started_at_utc=? AND state='failed'", (task['attempted_at_utc'],))
    if task.get('active_attempt_id'):
        from downloader.tej_desktop_attempts import finish_attempt
        finish_attempt(root, task['active_attempt_id'], 'proven_not_submitted')
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':state,
                                               'observed_at_utc':audit['observed_at_utc'],'task_id':task_id})
    return {'state':state,'market_data_query_repeated':False,'source_rows_adopted':False}


# Backward-compatible operator/test entrypoint; both use the same exact proof.
recover_legacy_date_input=recover_legacy_local_input


def recover_verified_input(root: Path, task_id: str, bridge: Any, prepared_request: Path) -> dict:
    """Explicit operator recovery after a local-input implementation fix.

    Not used by the automatic loop. The last structured before-Preview proof
    must match the exact attempt. A new bounded retry budget is audited;
    permission/quota/unknown/submitted queries cannot enter this path.
    """
    with closing(connect(root)) as con:
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
    codes={blocked:internal for internal,blocked in PREQUERY_FAILURES.items()}
    if row is None or row['kind']!='download' or row['state']!='blocked' or row['last_error_code'] not in codes:
        raise ValueError('Only a proved exhausted local-input task can be resumed by the operator')
    task=dict(row); req=task_request(root,task); original=prepared_request.resolve()
    from downloader.tej_desktop_attempts import prepared_request_matches
    if (original.parent!=(root/'requests').resolve() or original.suffix!='.json'
            or not re.fullmatch(re.escape(task_id)+r'-[0-9a-f]{32}\.json',original.name)
            or original.stat().st_size>2*1024**2
            or not prepared_request_matches(json.loads(original.read_text(encoding='utf-8-sig')),req,task)):
        raise ValueError('Exact original prepared request required')
    output=root/'raw'/original.name; proof_path=output.with_suffix('.json.outcome.json')
    if output.exists() or output.with_suffix('.json.stage.json').exists() or proof_path.stat().st_size>4096:
        raise ValueError('Unreviewed source/query-stage evidence; do not reset input retries')
    proof=json.loads(proof_path.read_text(encoding='utf-8-sig'))
    if (proof.get('provider')!='tej_smart_wizard' or proof.get('contract_version')!=CONTRACT_VERSION
            or proof.get('action')!='download' or proof.get('task_id')!=task_id
            or proof.get('error_code')!=codes[row['last_error_code']]
            or proof.get('market_data_query_submission_possible') is not False
            or any(proof.get(k)!=req.get(k) for k in ('type','smart_id','table'))):
        raise ValueError('Proof is not an exact unsent local-input attempt')
    observed=datetime.fromisoformat(proof['observed_at_utc']); attempted=datetime.fromisoformat(task['attempted_at_utc'])
    if not 0 <= (observed-attempted).total_seconds() <= 900:
        raise ValueError('Source proof does not cover the latest attempt')
    check={**req,'action':'confirm_metadata_error_cleared'}
    payload,readback,_=bridge.execute(root,{**task,'request_json':json.dumps(check)})
    if (payload.get('provider')!='tej_smart_wizard' or payload.get('contract_version')!=CONTRACT_VERSION
            or payload.get('action')!=check['action'] or payload.get('task_id')!=task_id
            or any(payload.get(k)!=req.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in ('source_binding_stable','vendor_notices_absent','source_selectors_enabled'))
            or payload.get('market_data_query_submitted') is not False or payload.get('credentials_read') is not False):
        raise ValueError('Shared interface is not independently verified usable')
    audit={'contract':'verified_local_input_operator_recovery_v1','task_id':task_id,
           'prior_safe_prequery_retries':task['safe_prequery_retries'],'automatic_recovery':False,
           'request_sha256':hashlib.sha256(original.read_bytes()).hexdigest(),
           'proof_sha256':hashlib.sha256(proof_path.read_bytes()).hexdigest(),
           'readback_sha256':hashlib.sha256(readback.read_bytes()).hexdigest(),
           'data_query_repeated':False,'source_rows_adopted':False,'observed_at_utc':datetime.now(UTC).isoformat()}
    atomic_write_json(root/'diagnostics'/(task_id+'-verified_input_recovery-'+uuid.uuid4().hex+'.json'),audit)
    with closing(connect(root)) as con,con:
        con.execute('UPDATE tasks SET safe_prequery_retries=0 WHERE task_id=?',(task_id,))
        state=_mark_prequery_failure(con,task,proof['error_code'])
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':state,
                                              'observed_at_utc':audit['observed_at_utc'],'task_id':task_id})
    return {'state':state,'data_query_repeated':False,'source_rows_adopted':False}


def configure_runtime_policy(root: Path, config: dict) -> dict:
    """Version execution policy separately from immutable source requests."""
    policy = {'contract':BATCH_SCHEDULER_CONTRACT,
              'preview_submission_contract':PREVIEW_SUBMISSION_CONTRACT,
              'desktop_input_contract':DESKTOP_INPUT_CONTRACT,
              'download_burst':config.get('download_burst_between_discoveries',4),
              'auto_key3_replanning':config.get('auto_key3_replanning',False),
              'auto_snapshot_replanning':config.get('auto_snapshot_replanning',False),
              'auto_month_period_replanning':config.get('auto_month_period_replanning',False),
              'metadata_failure_isolation':config.get('metadata_failure_isolation',False),
              'acknowledge_known_metadata_runtime_notices':config.get('acknowledge_known_metadata_runtime_notices',False)}
    if (isinstance(policy['download_burst'],bool) or not isinstance(policy['download_burst'],int)
            or not 1 <= policy['download_burst'] <= 20
            or any(not isinstance(policy[k],bool) for k in
                   ('metadata_failure_isolation','acknowledge_known_metadata_runtime_notices','auto_key3_replanning','auto_snapshot_replanning','auto_month_period_replanning'))):
        raise ValueError('Unreviewed desktop runtime policy')
    with closing(connect(root)) as con,con:
        con.execute("INSERT OR REPLACE INTO meta VALUES ('runtime_policy',?)",(json.dumps(policy),))
    return policy


def _ready_task(con: sqlite3.Connection, now: str, kind: str | None, policy: dict, table_id: str | None=None) -> sqlite3.Row | None:
    """Interleave within the earliest phase; rotate tables without sorting blobs."""
    def candidate(selected: str):
        if table_id is not None:
            return con.execute("SELECT * FROM tasks WHERE state='pending' AND kind=? AND table_id=? AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?) ORDER BY priority,task_id LIMIT 1",(selected,table_id,now)).fetchone()
        return con.execute("SELECT * FROM tasks WHERE state='pending' AND kind=? AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?) ORDER BY priority,table_id,task_id LIMIT 1",(selected,now)).fetchone()
    if kind is not None:
        return candidate(kind)
    discovery,download = candidate('discover'),candidate('download')
    values = dict(con.execute("SELECT key,value FROM meta WHERE key IN ('scheduler_consecutive_downloads','scheduler_last_download_table')"))
    burst = int(values.get('scheduler_consecutive_downloads','0'))
    choose_discovery = discovery is not None and (download is None or
        int(discovery['priority'])//100 < int(download['priority'])//100 or
        (int(discovery['priority'])//100 == int(download['priority'])//100 and burst >= policy.get('download_burst',4)))
    if choose_discovery:
        return discovery
    if download is None:
        return None
    last_table = values.get('scheduler_last_download_table')
    if last_table and table_id is None:
        rotated = con.execute("SELECT * FROM tasks WHERE state='pending' AND kind='download' AND priority=? AND table_id>? AND (next_attempt_at_utc IS NULL OR next_attempt_at_utc<=?) ORDER BY table_id,task_id LIMIT 1",(download['priority'],last_table,now)).fetchone()
        if rotated is not None:
            return rotated
    return download


def run_one(root: Path, bridge: Any, *, kind: str | None = None, table_id: str | None=None,
            retry_authorization_id: str | None=None) -> str:
    if kind not in {None, "discover", "download"}:
        raise ValueError("Unknown acquisition task kind")
    if table_id is not None and (kind!='download' or not re.fullmatch(r'[0-9a-f]{24}',table_id)):
        raise ValueError('Targeted acceptance requires one exact registered table and download-only mode')
    now = datetime.now(UTC).isoformat()
    with closing(connect(root)) as con, con:
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        saved_policy = con.execute("SELECT value FROM meta WHERE key='runtime_policy'").fetchone()
        policy = json.loads(saved_policy[0]) if saved_policy else {}
        if con.execute("SELECT 1 FROM meta WHERE key='desktop_interface_recovery_required'").fetchone():
            return 'desktop_interface_recovery_required'
        if con.execute("SELECT 1 FROM meta WHERE key='source_period_replan_required'").fetchone():
            return 'source_period_replan_required'
        if shutil.disk_usage(root).free < config.get("minimum_free_disk_bytes", 5 * 1024 ** 3):
            atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION,
                              "state": "local_disk_headroom_low", "observed_at_utc": now})
            return "local_disk_headroom_low"
        replay = None
        if retry_authorization_id is not None:
            from downloader.tej_desktop_attempts import OPERATOR_REPLAY_CONTRACT
            if kind is not None or table_id is not None or not re.fullmatch(r'[0-9a-f]{32}',retry_authorization_id):
                raise ValueError('One exact operator replay authorization required')
            replay = con.execute('SELECT * FROM desktop_replays WHERE authorization_id=?', (retry_authorization_id,)).fetchone()
            if replay is None or replay['consumed_at_utc'] is not None:
                raise ValueError('Operator replay authorization missing or already consumed')
            audit = root / replay['audit_path']
            if (audit.resolve().parent != (root/'operator_replays').resolve() or not audit.is_file()
                    or audit.stat().st_size>16384 or hashlib.sha256(audit.read_bytes()).hexdigest()!=replay['audit_sha256']):
                raise ValueError('Exact immutable operator replay audit required')
            proof = json.loads(audit.read_text())
            if (proof.get('contract')!=OPERATOR_REPLAY_CONTRACT or proof.get('authorization_id')!=retry_authorization_id
                    or proof.get('task_id')!=replay['task_id'] or proof.get('automatic_retry') is not False):
                raise ValueError('Invalid operator replay audit')
            row = con.execute('SELECT * FROM tasks WHERE task_id=?',(replay['task_id'],)).fetchone()
            if (row is None or row['kind']!='download' or row['state']!='blocked'
                    or row['last_error_code']!='unknown_outcome_no_auto_retry'
                    or row['attempted_at_utc']!=replay['original_attempted_at_utc']):
                raise ValueError('Unresolved task changed since operator replay inspection')
        # A crashed/timed-out process may already have exported a sheet. Never
        # silently move on or repeat it until local evidence is reconciled.
        if con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND last_error_code='unknown_outcome_no_auto_retry' AND task_id!=?) LIMIT 1",
                       (replay['task_id'] if replay else '',)).fetchone():
            return "inflight_requires_recovery"
        if kind!='discover' and con.execute("SELECT 1 FROM meta WHERE key='preview_planning_contract'").fetchone():
            from downloader.tej_planning import refill_ready_plans
            refill_ready_plans(con)
        if replay is None:
            row = _ready_task(con,now,kind,policy,table_id)
        if not row:
            return "idle"
        task = dict(row)
        if replay is not None:
            con.execute('UPDATE desktop_replays SET consumed_at_utc=? WHERE authorization_id=?', (now,retry_authorization_id))
        if task['kind']=='discover':
            con.execute("INSERT OR REPLACE INTO meta VALUES ('scheduler_consecutive_downloads','0')")
        else:
            old = con.execute("SELECT value FROM meta WHERE key='scheduler_consecutive_downloads'").fetchone()
            con.execute("INSERT OR REPLACE INTO meta VALUES ('scheduler_consecutive_downloads',?)",(str(min(20,int(old[0]) + 1) if old else 1),))
            con.execute("INSERT OR REPLACE INTO meta VALUES ('scheduler_last_download_table',?)",(task['table_id'],))
        con.execute("UPDATE tasks SET state='running',attempted_at_utc=? WHERE task_id=?", (now, task["task_id"]))
        event = uuid.uuid4().hex
        con.execute("INSERT INTO traffic VALUES (?,?,?,NULL,'running')", (event, task["kind"], now))
    atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION, "state": "running",
                       "observed_at_utc": now, "task_id": task["task_id"], "kind": task["kind"],
                       "owner_pid": os.getpid(),
                       "owner_start_ticks": Path(f"/proc/{os.getpid()}/stat").read_text().rsplit(") ", 1)[1].split()[19],
                       "deadline_at_utc": (datetime.fromisoformat(now) + timedelta(seconds=900)).isoformat()})
    try:
        lifecycle_started = time.monotonic()
        # Bridges (including test adapters) always receive full original
        # requests. The compact form belongs to the durable queue only.
        task["request_json"] = json.dumps(task_request(root, task), ensure_ascii=False)
        payload, output, seconds = bridge.execute(root, task)
        commit_evidence(root, task, payload, output, seconds, event_id=event)
        total_seconds = time.monotonic() - lifecycle_started
        with closing(connect(root)) as con, con:
            con.execute("UPDATE tasks SET seconds=?,timing_basis='fresh_end_to_end' WHERE task_id=? AND state='complete'",
                        (total_seconds, task["task_id"]))
        state = "completed_task"
        if task['kind']=='download' and policy.get('auto_month_period_replanning') is True:
            from downloader.tej_period_keys import source_month_keys, replan_month_periods, month_header_contract
            req = json.loads(task['request_json'])
            if (source_month_keys(payload) and req.get('frequency')=='daily'
                    and req.get('source_period_key_contract') != month_header_contract(payload)):
                try:
                    replan_month_periods(root, task['task_id'], bridge)
                except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
                    with closing(connect(root)) as con, con:
                        con.execute("INSERT OR REPLACE INTO meta VALUES ('source_period_replan_required',?)", (task['task_id'],))
                        con.execute("UPDATE tables SET state='needs_review',last_error_code='source_period_replan_required' WHERE table_id=?",(task['table_id'],))
                    atomic_write_json(root/'diagnostics'/(event+'-period_replan.json'), {
                        'task_id':task['task_id'],'state':'source_period_replan_required',
                        'exception_type':type(exc).__name__,'private_diagnostic':str(exc)[:4096],
                        'source_download_already_adopted':True,'data_query_repeated':False,
                        'observed_at_utc':datetime.now(UTC).isoformat()})
                    state='source_period_replan_required'
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        if isinstance(exc, BeforeDataQueryError):
            with closing(connect(root)) as con, con:
                state = _mark_prequery_failure(con,task,exc.error_code)
                con.execute("UPDATE traffic SET completed_at_utc=?,state='failed_before_preview' WHERE event_id=?", (datetime.now(UTC).isoformat(), event))
            atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION, "state": state,
                               "observed_at_utc": datetime.now(UTC).isoformat(), "task_id": task["task_id"]})
            return state
        code = ('source_key_layout_replan_required' if isinstance(exc,SourceKeyLayoutError)
                else "unknown_outcome_no_auto_retry" if isinstance(exc, (RuntimeError, subprocess.SubprocessError))
                else "local_storage_failed" if isinstance(exc, OSError) else "source_validation_failed")
        diagnostic = root/'diagnostics'/(event+'-adoption.json')
        atomic_write_json(diagnostic, {'task_id':task['task_id'],'state':code,
            'exception_type':type(exc).__name__,'private_diagnostic':str(exc)[:4096],
            'observed_at_utc':datetime.now(UTC).isoformat(),'data_query_repeated':False})
        diagnostic.chmod(0o600)
        with closing(connect(root)) as con, con:
            con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?", (code, task["task_id"]))
            con.execute("UPDATE tables SET last_error_code=?,state='needs_review' WHERE table_id=?", (code, task["table_id"]))
            con.execute("UPDATE traffic SET completed_at_utc=?,state='failed' WHERE event_id=?", (datetime.now(UTC).isoformat(), event))
        state = code
        if (isinstance(exc,SourceKeyLayoutError) and
                (exc.source_key_mode==3 and policy.get('auto_key3_replanning') is True or
                 exc.source_key_mode==1 and policy.get('auto_snapshot_replanning') is True)):
            from downloader.tej_key_layout import repair_source_key_plan
            try:
                state=repair_source_key_plan(root,task['task_id'],bridge,only_mode=exc.source_key_mode)['state']
            except (RuntimeError,ValueError,OSError,subprocess.SubprocessError) as replan_exc:
                atomic_write_json(root/'diagnostics'/(event+'-key_layout_replan.json'), {
                    'task_id':task['task_id'],'state':code,'exception_type':type(replan_exc).__name__,
                    'private_diagnostic':str(replan_exc)[:4096], 'data_query_repeated':False,
                    'observed_at_utc':datetime.now(UTC).isoformat()})
                state=code
        if isinstance(exc,MetadataPreparationError) and policy.get('metadata_failure_isolation') is True:
            # No result is adopted. The original failed task stays blocked.
            # A notice or unstable interface keeps the global barrier intact.
            mark_worker_wait(root,kind='discover',seconds=900,metadata_recovery=True)
            try:
                result = isolate_failed_metadata(root,task['task_id'],bridge,exc.prepared_request,
                    acknowledge_known_notices=policy.get('acknowledge_known_metadata_runtime_notices') is True)
                state = result['state']
            except (RuntimeError,ValueError,OSError,subprocess.SubprocessError):
                state = code
    atomic_write_json(root / "worker_status.json", {"contract_version": CONTRACT_VERSION, "state": state,
                       "observed_at_utc": datetime.now(UTC).isoformat(), "task_id": task["task_id"]})
    return state


def mark_worker_wait(root: Path, *, kind: str, seconds: float, local_retry: bool = False,
                     metadata_recovery: bool = False) -> None:
    """Prove the live batch's short wait without pretending a query is running."""
    now = datetime.now(UTC)
    atomic_write_json(root/'worker_status.json',{
        'contract_version':CONTRACT_VERSION,
        'state':'recovering_metadata' if metadata_recovery else 'waiting_local_retry' if local_retry else 'between_tasks',
        'kind':kind,'observed_at_utc':now.isoformat(),
        'owner_pid':os.getpid(),
        'owner_start_ticks':Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19],
        'deadline_at_utc':(now+timedelta(seconds=min(900,max(10,seconds+5)))).isoformat()})


def mark_batch_finished(root: Path, *, reason: str, attempted: int, mode: str) -> None:
    """Finite batch end is not proof that the remaining history is complete."""
    atomic_write_json(root/'worker_status.json',{
        'contract_version':CONTRACT_VERSION,'scheduler_contract':BATCH_SCHEDULER_CONTRACT,
        'state':'batch_finished','reason':reason,'attempted_tasks':attempted,'batch_mode':mode,
        'observed_at_utc':datetime.now(UTC).isoformat(),'history_complete':False})
