"""Credential/value-free TEJ projection. Indexed local metadata only; no GUI/API."""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import re
import shutil
import sqlite3
from statistics import median
from typing import Any

SCHEMA_VERSION = 15
STATUS_CACHE_SECONDS = 4.0
STATUS_REFRESH_SECONDS = 5
PROGRESS_CONTRACT = 'tej_native_readback_progress_v1'
PROGRESS_STAGES = frozenset({'preparing_scope', 'awaiting_preview', 'reading_preview',
                            'response_saved', 'validating_and_saving'})
SCHEDULER_STATES = frozenset({'starting', 'executing', 'between_tasks', 'waiting_local_retry', 'waiting_desktop',
    'waiting_queue', 'waiting_storage', 'waiting_owner', 'waiting_metadata', 'waiting_recovery', 'recovering_response',
    'replaying_authorized', 'stopped'})
SCHEDULER_REASONS = frozenset({'desktop_interface_recovery_required', 'inflight_requires_recovery',
    'unknown_outcome_no_auto_retry', 'source_validation_failed', 'local_storage_failed',
    'source_key_layout_replan_required', 'date_input_prequery_needs_review',
    'source_period_replan_required',
    'source_capacity_requires_review',
    'list_selection_prequery_needs_review', 'query_activation_prequery_needs_review',
    'source_binding_prequery_needs_review', 'query_preparation_prequery_needs_review'})
METHOD = "Smart Wizard 查詢可能沿用最近一期。v4 另驗證公司／日期介面可用及無錯誤視窗；保存的是來源顯示字串，不是 Excel 底層精度、原生歷史或發布時點證明。舊範圍需重新驗證。"
TABLE_COLUMNS = ("table_id", "smart_id", "name", "category", "phase", "frequency", "state",
                 "universe_count", "grid_dates", "grid_rows", "last_error_code",
                 "work_grid_rows", "field_batches",
                 "query_type", "source_key_mode",
                 "first_available_query_period", "last_available_query_period", "axis_profile_basis")
FEATURE_COLUMNS = ("feature_id", "table_id", "field_index", "name", "phase", "unit", "role",
                   "raw_input_policy", "local_count", "local_first", "local_last",
                   "exported_non_null_cells", "first_query_period", "last_query_period")


def _read(root: Path) -> sqlite3.Connection:
    con = sqlite3.connect((root / "queue.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=.25)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    return con


def _json(path: Path) -> dict:
    try:
        with path.open("rb") as stream:
            body = stream.read(65537)
        value = json.loads(body) if len(body) <= 65536 else {}
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _stamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else None
    except (TypeError, ValueError):
        return None


def _owner_alive(worker: dict, observed: datetime, states: set | frozenset, max_seconds: float) -> bool:
    clock, deadline = _stamp(worker.get("observed_at_utc")), _stamp(worker.get("deadline_at_utc"))
    pid = worker.get("owner_pid")
    if (worker.get("state") not in states or not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0
            or not clock or not deadline or not clock - timedelta(seconds=5) <= observed <= deadline
            or not 0 < (deadline-clock).total_seconds() <= max_seconds):
        return False
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
        return stat[0] not in {"Z", "X"} and stat[19] == worker.get("owner_start_ticks")
    except (OSError, IndexError):
        return False


def _worker_alive(worker: dict, observed: datetime) -> bool:
    return _owner_alive(worker, observed, {'running', 'between_tasks', 'waiting_local_retry', 'recovering_metadata'}, 900)


def _automatic_execution_state(state: str | None) -> str:
    """A live supervisor waiting for resources is not continuous acquisition."""
    if state in {'waiting_recovery','recovering_response','replaying_authorized'}:
        return 'automatic_waiting_recovery'
    return 'automatic_running' if state in {'executing','between_tasks'} else 'automatic_waiting'


def _scheduler_public(root: Path, observed: datetime) -> dict:
    value = _json(root / 'scheduler_status.json')
    valid = (value.get('contract') == 'persistent_serial_evidence_preserving_supervision_v1'
             and value.get('continuous') is True)
    state = value.get('state') if valid and value.get('state') in SCHEDULER_STATES else None
    alive = valid and _owner_alive(value, observed, SCHEDULER_STATES - {'stopped'},
                                 1800 if state in {'executing','recovering_response','replaying_authorized'} else 300)

    def stamp(key):
        parsed = _stamp(value.get(key)) if valid else None
        return parsed.isoformat() if parsed else None

    def counter(key):
        count = value.get(key) if valid else None
        return count if isinstance(count, int) and not isinstance(count, bool) and 0 <= count <= 10**12 else None

    grant = value.get('authorized_unknown_replay') if valid else None
    enabled = (isinstance(grant, dict) and grant.get('enabled') is True
               and grant.get('contract') == 'exact_unknown_download_user_authorized_replay_v1')
    recovery = _json(root / 'authorized_replay_status.json') if enabled else {}
    recovery_valid = recovery.get('contract') == grant.get('contract') if enabled else False
    replay_states = {'authorized_replay_cooldown', 'authorized_replay_task_budget',
                     'authorized_replay_global_budget', 'checking_authorized_replay',
                     'authorized_replay_context_unverified', 'completed_task', 'unknown_outcome_no_auto_retry',
                     'local_disk_headroom_low', 'local_metadata_busy', 'prequery_retry_scheduled',
                     'original_response_recovered'}
    replay = {'enabled': bool(enabled), 'unconditional_retry': False}
    if enabled:
        for key in ('cooldown_base_seconds', 'cooldown_max_seconds', 'max_replays_per_task_hour', 'max_replays_per_hour'):
            limit = grant.get(key)
            replay[key] = limit if type(limit) is int and 0 < limit <= 300 else None
        clock = _stamp(recovery.get('observed_at_utc')) if recovery_valid else None
        fresh = bool(clock and clock <= observed + timedelta(seconds=5) and (observed-clock).total_seconds() <= 3600)
        next_check = _stamp(recovery.get('next_check_at_utc')) if fresh else None
        recovery_state = recovery.get('state')
        replay.update(state=recovery_state if fresh and isinstance(recovery_state, str) and recovery_state in replay_states else None,
                      next_check_at_utc=next_check.isoformat() if next_check else None)
        for key in ('task_replays_in_hour', 'replays_in_hour'):
            count = recovery.get(key) if fresh else None
            replay[key] = count if type(count) is int and 0 <= count <= 10**12 else None

    # This is an independent process heartbeat. Never expose the PID, owner
    # identity, raw exceptions or private desktop/session paths to the browser.
    return {'alive': bool(alive), 'continuous': valid, 'state': state,
            'observed_at_utc': stamp('observed_at_utc'), 'started_at_utc': stamp('started_at_utc'),
            'next_check_at_utc': stamp('next_check_at_utc') if alive else None,
            'last_completed_at_utc': stamp('last_completed_at_utc'),
            'completed_tasks': counter('completed_tasks'), 'cycles': counter('cycles'),
            'paused_reason': value.get('paused_reason') if valid and value.get('paused_reason') in SCHEDULER_REASONS else None,
            'query_deadline_renewed_by_heartbeat': False, 'unknown_outcome_auto_retry': False,
            'authorized_unknown_replay': replay}


def _current_task_public(root: Path, task: dict | None, worker: dict, alive: bool,
                         observed: datetime) -> dict | None:
    """Progress is display-only: never a receipt, retry proof or owner heartbeat."""
    if not task or not alive or worker.get('state') != 'running':
        return None
    started = _stamp(task.get('attempted_at_utc'))
    value = {'task_id': task['task_id'], 'table_id': task['table_id'],
             'table_name': task['table_name'], 'kind': task['kind'], 'phase': task['phase'],
             'started_at_utc': started.isoformat() if started else None,
             'elapsed_seconds': max(0, (observed-started).total_seconds()) if started else None,
             'query_work_rows': task['work_rows'], 'stage': 'awaiting_progress',
             'progress_observed_at_utc': None, 'readback_scanned_row_slots': None,
             'readback_total_row_slots': None, 'readback_ratio': None,
             'adopted': False, 'progress_stale': False}
    attempt = worker.get('bridge_attempt_id')
    if (not isinstance(attempt, str) or
            not re.fullmatch(re.escape(task['task_id']) + r'-[0-9a-f]{32}', attempt) or
            task.get('active_attempt_id') and task['active_attempt_id'] != attempt):
        return value
    path = root / 'progress' / (attempt + '.json.progress.json')
    # Derive one exact attempt path; no glob, source file read or symlink escape.
    if path.resolve().parent != (root / 'progress').resolve():
        return value
    progress = _json(path)
    clock = _stamp(progress.get('observed_at_utc'))
    if (progress.get('contract') != PROGRESS_CONTRACT or progress.get('task_id') != task['task_id']
            or progress.get('attempt_id') != attempt or progress.get('stage') not in PROGRESS_STAGES
            or not clock or not started or not started <= clock <= observed + timedelta(seconds=5)):
        return value
    value.update(stage=progress['stage'], progress_observed_at_utc=clock.isoformat(),
                 progress_stale=(observed-clock).total_seconds() > 30)
    scanned, total = progress.get('scanned_row_slots'), progress.get('total_row_slots')
    if (type(scanned) is int and type(total) is int and 0 <= scanned <= total <= 10001
            and progress['stage'] in {'reading_preview', 'response_saved', 'validating_and_saving'}):
        value.update(readback_scanned_row_slots=scanned, readback_total_row_slots=total,
                     readback_ratio=scanned/total if total else None)
    return value


def build_tej_public_status(repo_root: Path, *, now: datetime | None = None) -> dict:
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    root = repo_root / "data_tej"
    base = {"schema_version": SCHEMA_VERSION, "provider": "TEJ", "observed_at_utc": observed.isoformat(),
            "read_only": True, "raw_values_exposed": False, "method": METHOD,
            "native_observation_completeness_verified": False, "publication_verified": False,
            "raw_source_publish": False, "tables": []}
    # Local receipts only. Never reveal workbook/PID/handle/session/path,
    # launcher details or private vendor errors to the public browser.
    from downloader.tej_startup import CONTRACT as STARTUP_CONTRACT, desktop_ready
    startup = _json(root/'desktop_startup_status.json')
    startup_config = _json(repo_root/'configs/tej_history.json').get('automation', {}).get('desktop_startup', {})
    startup_states = {'desktop_ready','interactive_windows_transport_unavailable','interactive_session_required',
        'desktop_writer_active','owned_query_needs_reconciliation','retired_addin_still_present',
        'startup_launcher_outcome_unresolved','desktop_restart_cooldown','tej_addin_not_ready','desktop_waiting_login_or_addin',
        'desktop_startup_unverified','owned_workbook_context_unavailable','owned_workbook_identity_changed',
        'owned_query_identity_unverified'}
    enabled = startup_config.get('enabled') is True and startup_config.get('contract') == STARTUP_CONTRACT
    base['desktop_startup'] = {'contract': STARTUP_CONTRACT, 'enabled': enabled,
        'state': startup.get('state') if startup.get('contract') == STARTUP_CONTRACT and startup.get('state') in startup_states else 'desktop_startup_unverified',
        'observed_at_utc': startup.get('observed_at_utc'), 'ready': desktop_ready(root, now=observed),
        'check_interval_seconds': 60, 'requires_windows_user_logon': True,
        'source_receipts_preserved': True, 'windows_autologin_changed': False}
    worker = _json(root / 'worker_status.json')
    if not (root / "queue.sqlite3").is_file():
        return {**base, "state": "not_registered", "catalog": {"tables": None, "fields": None}}
    try:
        with closing(_read(root)) as con:
            con.execute("BEGIN")
            meta = {r[0]: r[1] for r in con.execute("SELECT key,value FROM meta")}
            task_columns = {r[1] for r in con.execute("PRAGMA table_info(tasks)")}
            current_scope = (" AND scope_contract='editable_source_scope_v1'" if
                             'scope_contract' in task_columns and meta.get('editable_scope_contract') == 'editable_source_scope_v1' else "")
            columns = {r[1] for r in con.execute("PRAGMA table_info(tables)")}
            work_rows='COALESCE(work_expected_rows,expected_rows)' if 'work_expected_rows' in task_columns else 'expected_rows'
            # A writer upgrades the catalog only at a safe acquisition boundary.
            # Older live writers remain readable; the monitor never migrates it.
            select = [key if key in columns else 'NULL AS '+key for key in TABLE_COLUMNS]
            tables = [dict(r) for r in con.execute("SELECT " + ",".join(select) + " FROM tables ORDER BY phase,category,name")]
            per_table = {r["table_id"]: dict(r) for r in con.execute("SELECT table_id,COUNT(*) AS fields,SUM(exported_non_null_cells) AS exported_non_null_cells,MIN(first_query_period) AS first_query_period,MAX(last_query_period) AS last_query_period,SUM(phase='P1') AS p1_fields,SUM(phase='P2') AS p2_fields,SUM(phase='P3') AS p3_fields FROM features GROUP BY table_id")}
            tasks = [dict(r) for r in con.execute("SELECT table_id,kind,state,COUNT(*) AS tasks,SUM("+work_rows+") AS target_rows,SUM(actual_rows) AS exported_rows,SUM(actual_bytes) AS recorded_bytes,SUM(CASE WHEN kind='download' AND state='complete' AND actual_rows=0 THEN 1 ELSE 0 END) AS empty_tasks FROM tasks WHERE state IN ('pending','running','complete','blocked')"+current_scope+" GROUP BY table_id,kind,state")]
            plans={r['table_id']:dict(r) for r in con.execute('SELECT table_id,total_queries,next_query FROM download_plans')} if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='download_plans'").fetchone() else {}
            value_priorities = {r['table_id']: dict(r) for r in con.execute(
                'SELECT table_id,priority,rank,value_score,missing_fields,crosscheck_fields,reason FROM acquisition_value_priorities')
                } if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='acquisition_value_priorities'").fetchone() else {}
            # A bounded timing sample, never a scan of source Parquet files.
            samples = []
            for kind in ("download", "discover"):
                samples.extend(dict(r) for r in con.execute(
                    "SELECT table_id,kind,expected_rows,actual_rows,actual_bytes,seconds,timing_basis,completed_at_utc "
                    "FROM tasks WHERE state='complete' AND kind=? AND timing_basis='fresh_end_to_end' AND seconds>0"
                    + current_scope + " ORDER BY completed_at_utc DESC LIMIT 1000", (kind,)))
            total_features = con.execute("SELECT COUNT(*) FROM features").fetchone()[0]
            features_obtained = con.execute("SELECT COUNT(*) FROM features WHERE exported_non_null_cells>0").fetchone()[0]
            minute = observed.replace(second=0, microsecond=0)
            traffic = con.execute("SELECT COUNT(*) FROM traffic WHERE started_at_utc>? AND started_at_utc<=?", ((minute - timedelta(hours=1)).isoformat(), minute.isoformat())).fetchone()[0]
            # Indexed current/success records; never decode giant lazy plans or
            # read licensed values just to display download activity.
            active_column = 'k.active_attempt_id' if 'active_attempt_id' in task_columns else 'NULL AS active_attempt_id'
            active_work_rows = ('COALESCE(k.work_expected_rows,k.expected_rows)' if
                                'work_expected_rows' in task_columns else 'k.expected_rows')
            current_row = con.execute("SELECT k.task_id,k.table_id,t.name AS table_name,k.kind,k.phase,"
                "k.attempted_at_utc," + active_column + "," + active_work_rows
                + " AS work_rows FROM tasks k JOIN tables t ON t.table_id=k.table_id "
                "WHERE k.task_id=? AND k.state='running'" + current_scope,
                (worker.get('task_id') if isinstance(worker.get('task_id'), str) else '',)).fetchone()
            current = dict(current_row) if current_row else None
            latest_row = con.execute("SELECT k.task_id,k.table_id,t.name AS table_name,k.completed_at_utc,"
                "k.actual_rows,k.actual_bytes FROM tasks k JOIN tables t ON t.table_id=k.table_id "
                "WHERE k.state='complete' AND k.kind='download'" + current_scope
                + " ORDER BY k.completed_at_utc DESC LIMIT 1").fetchone()
            latest = dict(latest_row) if latest_row else None
            blocked = [dict(r) for r in con.execute("SELECT k.task_id,k.table_id,t.name AS table_name,"
                "k.kind,k.last_error_code,k.attempted_at_utc FROM tasks k JOIN tables t ON t.table_id=k.table_id "
                "WHERE k.state='blocked'" + current_scope + " ORDER BY k.priority,k.task_id LIMIT 20")]
            retry_windows = {}
            if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='desktop_retry_windows'").fetchone():
                for row in con.execute("SELECT r.table_id,r.task_id,t.name AS table_name,r.consecutive_failures,"
                        "r.next_attempt_at_utc,r.error_code,k.state AS task_state FROM desktop_retry_windows r "
                        "JOIN tasks k ON k.task_id=r.task_id JOIN tables t ON t.table_id=r.table_id "
                        "WHERE k.state IN ('pending','running') ORDER BY r.next_attempt_at_utc,r.table_id"):
                    record=dict(row)
                    if record['error_code'] not in SCHEDULER_REASONS:
                        record['error_code']='unclassified_requires_review'
                    due=_stamp(record['next_attempt_at_utc'])
                    record['retry_after_seconds']=max(0,(due-observed).total_seconds()) if due else None
                    retry_windows[record['table_id']]=record
    except (sqlite3.Error, OSError, ValueError):
        return {**base, "state": "metadata_unavailable", "catalog": {"tables": None, "fields": None}}
    grouped: dict[str, list[dict]] = {}
    try:
        registered = json.loads(meta.get("catalog_summary", "{}"))
        config = json.loads(meta.get("config", "{}"))
        legacy = json.loads(meta.get('legacy_unverified_exports','{}'))
    except (TypeError, ValueError):
        return {**base, "state": "metadata_unavailable", "catalog": {"tables": None, "fields": None}}
    alive = _worker_alive(worker, observed)
    scheduler = _scheduler_public(root, observed)
    for task in tasks:
        grouped.setdefault(task["table_id"], []).append(task)
    table_samples: dict[str, list[dict]] = {}
    for sample in samples:
        if sample['kind'] == 'download' and sample['expected_rows']:
            table_samples.setdefault(sample["table_id"], []).append(sample)
    totals = {"exported_rows": 0, "resolved_grid_rows": 0, "source_empty_tasks": 0,
              "recorded_bytes": 0, "exported_non_null_cells": 0,
              "known_grid_rows": 0, "discovered_tables": 0, "blocked_tasks": 0, "running_tasks": 0}
    for table in tables:
        identifier = table["table_id"]
        ranking = value_priorities.get(identifier)
        if ranking:
            table.update(collection_priority=ranking['priority'], collection_rank=ranking['rank'],
                         acquisition_value_score=ranking['value_score'], acquisition_value_basis=ranking['reason'],
                         local_gap_candidate_fields=ranking['missing_fields'],
                         local_crosscheck_candidate_fields=ranking['crosscheck_fields'])
        # One company/date pair may have several disjoint field batches.
        # Keep the unique menu grid separately; progress measures query work.
        table['company_period_grid_rows']=table['grid_rows']
        if table['work_grid_rows'] is not None:
            table['grid_rows']=table['work_grid_rows']
        plan=plans.get(identifier)
        deferred_queries=max(0,plan['total_queries']-plan['next_query']) if plan else 0
        table['unmaterialized_queries']=deferred_queries if plan else None
        table.update(per_table.get(identifier, {"fields": 0, "exported_non_null_cells": None,
                                                "first_query_period": None, "last_query_period": None}))
        table["field_phase_counts"] = {phase: table.pop(phase.lower() + "_fields", 0) for phase in ("P1", "P2", "P3")}
        work = grouped.get(identifier, [])
        downloads = [w for w in work if w["kind"] == "download"]
        table["tasks"] = sum(w["tasks"] for w in downloads)+deferred_queries
        table["remaining_download_queries"] = sum(w["tasks"] for w in downloads if w['state'] != 'complete')+deferred_queries
        table["remaining_discovery_tasks"] = sum(w["tasks"] for w in work if w['kind'] == 'discover' and w['state'] != 'complete')
        table["blocked_tasks"] = sum(w["tasks"] for w in work if w["state"] == "blocked")
        table["running_tasks"] = sum(w["tasks"] for w in work if w["state"] == "running")
        table['local_retry_window']=retry_windows.get(identifier)
        table['deferred_tasks']=int(bool(table['local_retry_window'] and
                                       table['local_retry_window']['task_state']=='pending'))
        table["active_work_kind"] = next((w['kind'] for w in work if w['state']=='running'),None)
        table["exported_rows"] = sum(w["exported_rows"] or 0 for w in downloads if w["state"] == "complete")
        table["resolved_grid_rows"] = sum(w["target_rows"] or 0 for w in downloads if w["state"] == "complete")
        table["source_empty_tasks"] = sum(w["empty_tasks"] for w in downloads)
        table["recorded_bytes"] = sum(w["recorded_bytes"] or 0 for w in work if w["state"] == "complete")
        table["query_grid_ratio"] = table["exported_rows"] / table["grid_rows"] if table["grid_rows"] else None
        table["query_scope_ratio"] = table["resolved_grid_rows"] / table["grid_rows"] if table["grid_rows"] else None
        unfinished = sum(w["tasks"] for w in work if w["state"] != "complete")+deferred_queries
        checked = bool(table["grid_rows"] and downloads and not unfinished and table["resolved_grid_rows"] == table["grid_rows"])
        table["state"] = ("needs_review" if table["blocked_tasks"] else "running" if table["running_tasks"] and alive else
                          "stalled_requires_recovery" if table["running_tasks"] else
                          "waiting_local_retry" if table['deferred_tasks'] else
                          "query_grid_exported" if checked and table["exported_rows"] == table["grid_rows"] else
                          "query_scope_checked" if checked else table["state"])
        measured = table_samples.get(identifier, [])
        # Work denominator = planned company/period scope, even for a sparse or
        # empty source response. Result rows are a different unit and cannot
        # be divided into remaining Cartesian scope to estimate elapsed work.
        overhead = config.get("minimum_export_interval_seconds", 0)
        rate = median(s["expected_rows"] / (s["seconds"] + overhead) for s in measured) if len(measured) >= 2 else None
        bytes_per_row = median(s["actual_bytes"] / s["expected_rows"] for s in measured) if len(measured) >= 2 else None
        remaining = max(0, (table["grid_rows"] or 0) - table["resolved_grid_rows"])
        table["estimated_total_bytes"] = (table["recorded_bytes"] if checked else
                                          table["recorded_bytes"] + round(bytes_per_row * remaining) if bytes_per_row is not None and table["grid_rows"] else None)
        density = median(s["actual_rows"] / s["expected_rows"] for s in measured) if len(measured) >= 2 else None
        table["estimated_total_export_rows"] = (table["exported_rows"] if checked else
                                                table["exported_rows"] + round(density * remaining) if density is not None and table["grid_rows"] else None)
        table["remaining_seconds"] = (0 if checked else
                                      remaining / rate if rate and not table["blocked_tasks"] and not table['local_retry_window'] and table["grid_rows"] is not None else None)
        table["eta_basis"] = "同表近期完整工作之格點處理速度／稀疏密度；條件式，不含未核實配額、其他表優先工作及桌面等待" if rate else "至少兩次同表完整工作後估算；恢復讀回不當抓取速度，欄位數不當流量／原生筆數"
        for key in ("exported_rows", "resolved_grid_rows", "source_empty_tasks", "recorded_bytes", "blocked_tasks", "running_tasks"):
            totals[key] += table[key]
        totals["exported_non_null_cells"] += table["exported_non_null_cells"] or 0
        if table["grid_rows"] is not None:
            totals["known_grid_rows"] += table["grid_rows"]
            totals["discovered_tables"] += 1
    quotas = config.get("quota", {})
    states = Counter(t["state"] for t in tables)
    nonempty = sum(t["fields"] > 0 for t in tables)
    axes_complete = totals["discovered_tables"] == nonempty
    all_known_exported = axes_complete and not totals["blocked_tasks"] and all(t["state"] in {"query_grid_exported", "query_scope_checked", "empty_field_menu"} for t in tables)
    phase_rows = []
    for phase in ("P1", "P2", "P3"):
        selected = [t for t in tables if t["phase"] == phase]
        phase_rows.append({"phase": phase, "tables": len(selected), "exported_rows": sum(t["exported_rows"] for t in selected),
                           "candidate_fields": registered.get("phase_counts", {}).get(phase),
                           "tables_combined_into_prior_phase": sum(t["phase"] < phase and t["field_phase_counts"].get(phase, 0) > 0 for t in tables),
                           "resolved_grid_rows": sum(t["resolved_grid_rows"] for t in selected),
                           "known_grid_rows": sum(t["grid_rows"] or 0 for t in selected),
                           "undiscovered_tables": sum(t["grid_rows"] is None and t["fields"] > 0 for t in selected),
                           "remaining_seconds": sum(t["remaining_seconds"] or 0 for t in selected) if selected and all(t["remaining_seconds"] is not None for t in selected) else None})
    catalog_proof = registered.get("catalog", {})
    completed_state = ("query_grid_exported" if all(t["state"] in {"query_grid_exported", "empty_field_menu"} for t in tables)
                       else "query_scope_checked")
    from downloader.tej_eta import build_staged_eta

    # Forecasting shares this consistent, read-only metadata transaction.
    # No plan JSON, source files, GUI calls, or speculative queue writes.
    definitions = {t['table_id']: t for t in tables}
    for sample in samples:
        definition = definitions.get(sample['table_id'], {})
        sample['frequency'] = definition.get('frequency')
    forecast = build_staged_eta(tables, samples, config, observed=observed,
        cutoff=meta.get('cutoff'), alive=alive,
        interface_blocked='desktop_interface_recovery_required' in meta)
    if scheduler['alive']:
        forecast['execution_state'] = _automatic_execution_state(scheduler['state'])
        forecast['assumptions'][0] = '常駐單一桌面持有者；下列為連續 24 小時情境，安全等待與故障修復時間仍未知'
    try:
        free = shutil.disk_usage(root).free
    except OSError:
        free = None
    reserve = config.get('minimum_free_disk_bytes', 5*1024**3)
    remaining_bytes = forecast['global_scenarios']['middle']['remaining_local_bytes']
    budget = max(0, free-reserve) if free is not None else None
    forecast['local_storage'] = {'free_bytes':free,'minimum_reserve_bytes':reserve,'available_for_download_bytes':budget,
                                'middle_scenario_fits_current_budget':remaining_bytes <= budget
                                    if budget is not None and remaining_bytes is not None else None}
    table_predictions = {t['table_id']: t for t in forecast.pop('table_forecasts')}
    for table in tables:
        predicted = table_predictions[table['table_id']]
        table['forecast'] = {key: predicted[key] for key in ('remaining_queries', 'remaining_seconds',
            'remaining_export_rows', 'remaining_local_bytes', 'geometry_basis', 'timing_basis', 'timing_samples')}
        # The cohort input is an implementation detail, not source data to
        # expose or a browser request parameter.
        table.pop('query_type', None)
    automatic_wait = ('needs_review' if scheduler['state'] == 'waiting_recovery' else
                      'paused_for_storage' if scheduler['state'] == 'waiting_storage' else 'automatic_waiting')
    for task in blocked:
        if task['last_error_code'] not in SCHEDULER_REASONS | {'vendor_metadata_allocation_failed_deferred',
                'metadata_preparation_failed_deferred','source_validation_failed_deferred'}:
            task['last_error_code'] = 'unclassified_requires_review'
    completed_downloads = sum(w['tasks'] for w in tasks if w['kind'] == 'download' and w['state'] == 'complete')
    completed_discoveries = sum(w['tasks'] for w in tasks if w['kind'] == 'discover' and w['state'] == 'complete')
    return {**base, "state": "running" if alive else automatic_wait if scheduler['alive'] else "needs_review" if totals["blocked_tasks"] or totals["running_tasks"] else "paused_for_storage" if worker.get("state") == "local_disk_headroom_low" else completed_state if all_known_exported else "queued",
            "activity": {'contract': 'tej_download_activity_v1', 'refresh_seconds': STATUS_REFRESH_SECONDS,
                         'completed_download_tasks': completed_downloads,
                         'completed_discovery_tasks': completed_discoveries,
                         'last_successful_download': latest,
                         'current_task': _current_task_public(root, current, worker, alive, observed),
                         'blocked_tasks': blocked, 'blocked_tasks_total': totals['blocked_tasks'],
                         'deferred_tasks': [r for r in retry_windows.values() if r['task_state']=='pending'][:20],
                         'deferred_tasks_total': sum(r['task_state']=='pending' for r in retry_windows.values()),
                         'completion_revision': f"{latest['task_id'] if latest else ''}:{completed_downloads}:"
                             f"{completed_discoveries}:{totals['exported_non_null_cells']}:{total_features}"},
            "catalog": {"tables": len(tables), "fields": total_features, "types_scanned": len(catalog_proof.get("types_verified_complete", [])) or None,
                        "catalog_scan_complete": catalog_proof.get("catalog_scan_complete") is True, "empty_field_menus": states["empty_field_menu"],
                        "fields_with_non_null_exports": features_obtained,
                        "field_phase_counts": registered.get("phase_counts"), "catalog_observed_at_utc": registered.get("generated_at_utc")},
            "workload": {**totals, "total_rows": totals["known_grid_rows"] if axes_complete else None,
                         "global_row_ratio": totals["exported_rows"] / totals["known_grid_rows"] if axes_complete and totals["known_grid_rows"] else None,
                         "global_query_scope_ratio": totals["resolved_grid_rows"] / totals["known_grid_rows"] if axes_complete and totals["known_grid_rows"] else None,
                         "scope": "已發現公司／日期／欄位分片工作格點，不等於原生歷史；未發現的表不填零"},
            "planning": {"contract":meta.get('preview_planning_contract'),
                         "acquisition_priority":json.loads(meta.get('acquisition_value_priority','null')),
                         "query_tiling_contract":meta.get('query_tiling_contract'),
                         "query_capacity_contract":meta.get('query_capacity_contract'),
                         "local_max_rows":config.get('max_rows_per_export'),
                         "local_max_cells":config.get('max_cells_per_export'),
                         "local_max_companies":config.get('max_companies_per_export'),
                         "query_interval_contract":config.get('query_interval_contract','minimum_completion_gap_v1'),
                         "minimum_query_interval_seconds":config.get('minimum_export_interval_seconds'),
                         "preview_max_columns":30 if meta.get('preview_planning_contract')=='preview_30_columns_lazy_fields_v1' else None,
                         "buffered_download_tasks":sum(w['tasks'] for w in tasks if w['kind']=='download' and w['state'] in {'pending','running'}),
                         "unmaterialized_queries":sum(p['total_queries']-p['next_query'] for p in plans.values()) if plans else None},
            "legacy_exports": {key:legacy.get(key) for key in ('exported_rows','exported_non_null_cells','recorded_bytes','completed_download_tasks')},
            "scheduler": scheduler,
            "worker": {"alive": alive, "state": worker.get("state"), "observed_at_utc": worker.get("observed_at_utc"),
                       "kind": worker.get("kind"), "finite_desktop_ownership": True,
                       "batch_mode": worker.get('batch_mode') if worker.get('batch_mode') in {'discover','download','interleaved'} else None,
                       "batch_end_reason": worker.get('reason') if worker.get('reason') in {'finite_task_limit_reached','no_ready_tasks_in_selected_mode'} else None,
                       "attempted_tasks": worker.get('attempted_tasks') if isinstance(worker.get('attempted_tasks'),int) and not isinstance(worker.get('attempted_tasks'),bool) and 0 <= worker['attempted_tasks'] <= 1000 else None},
            "quota": {key: quotas.get(key) for key in ("smart_wizard_requests_per_second", "smart_wizard_requests_per_day", "smart_wizard_rows_per_day", "smart_wizard_reset_timezone")},
            "traffic": {"sampled_at_utc": minute.isoformat(), "local_desktop_operations_60m": traffic,
                        "official_http_requests_used": None, "official_account_usage": None,
                        "basis": "每分鐘按佇列工作開始收據取樣，不含手動診斷；一次桌面查詢可能含多個內部請求，不能當 API 流量"},
            "eta": {"global_remaining_seconds": None, "estimated_complete_at_utc": None,
                    "reason": "尚未核實所有歷史軸、配額及執行時段；下列為連續執行情境，不是已排定完成日",
                    "phases": phase_rows, "forecast": forecast},
            "tables": tables}


def build_tej_feature_page(repo_root: Path, *, offset: int = 0, limit: int = 50,
                           search: str = "", table_id: str = "", phase: str = "all") -> dict:
    if not 0 <= offset <= 1_000_000 or not 1 <= limit <= 100 or len(search) > 120 or phase not in {"all", "P1", "P2", "P3"}:
        raise ValueError("Invalid TEJ feature page parameters")
    import re
    if table_id and not re.fullmatch(r"[a-f0-9]{24}", table_id):
        raise ValueError("Invalid table identity")
    clauses, args = [], []
    if table_id:
        clauses.append("f.table_id=?"); args.append(table_id)
    if phase != "all":
        clauses.append("f.phase=?"); args.append(phase)
    if search:
        needle = "%" + search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        clauses.append("(f.name LIKE ? ESCAPE '\\' OR t.name LIKE ? ESCAPE '\\')"); args.extend((needle, needle))
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    with closing(_read(repo_root / "data_tej")) as con:
        count = con.execute("SELECT COUNT(*) FROM features f JOIN tables t ON t.table_id=f.table_id" + where, args).fetchone()[0]
        rows = con.execute("SELECT " + ",".join("f." + key for key in FEATURE_COLUMNS) + ",t.name AS table_name,t.category,t.state AS table_state FROM features f JOIN tables t ON t.table_id=f.table_id" + where + " ORDER BY f.phase,t.category,t.name,f.field_index LIMIT ? OFFSET ?", (*args, limit, offset)).fetchall()
    return {"schema_version": SCHEMA_VERSION, "read_only": True, "raw_values_exposed": False, "method": METHOD,
            "page": {"offset": offset, "limit": limit, "matched_total": count, "has_more": offset + len(rows) < count},
            "features": [dict(r) for r in rows]}
