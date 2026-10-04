"""Attempt-scoped desktop evidence; explicit replay never means safe auto retry.

The canonical task/source ABI is unchanged. Each actual Preview has its own
private prepared request/stage, including a failed attempt retained by an
operator replay. Recovery must not choose an arbitrary historical stage.
"""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import uuid

from downloader.artifact_io import atomic_write_json

ATTEMPT_CONTRACT = 'attempt_scoped_preview_v1'
OPERATOR_REPLAY_CONTRACT = 'exact_unknown_download_operator_replay_v1'


def recover_unlaunched_metadata_claim(root: Path, task_id: str, prepared: Path | None = None) -> bool:
    """Reconcile a local claim that never passed canonical begin_attempt.

    The v4 DesktopBridge commits begin_attempt before starting PowerShell.
    Its exact prepared request plus a current metadata failure, and no attempt
    registration or source artifact, prove a failed local launch. A bare NULL
    attempt, dead process, or missing response alone is never sufficient.
    Caller owns .download.lock; this function never invokes the desktop.
    """
    from downloader.tej_history import (
        CONTRACT_VERSION, DESKTOP_INPUT_CONTRACT, SOURCE_SCOPE_CONTRACT, DesktopBridge, connect, task_request,
    )
    from downloader.artifact_io import sha256_file
    import inspect
    worker_path = root / 'worker_status.json'
    if not worker_path.is_file():
        return False
    worker = json.loads(worker_path.read_text())
    if worker.get('state') != 'waiting_metadata':
        return False
    with closing(connect(root)) as con, con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if row is None:
            return False
        task = dict(row)
        if (task['state'] != 'running' or task['kind'] != 'download'
                or task['scope_contract'] != SOURCE_SCOPE_CONTRACT or task.get('active_attempt_id') is not None
                or any(task.get(k) is not None for k in ('actual_rows', 'receipt_path', 'completed_at_utc'))):
            return False
        if con.execute("SELECT count(*) FROM tasks WHERE state='running'").fetchone()[0] != 1:
            raise ValueError('Metadata failure has ambiguous queue ownership')
        if con.execute('SELECT 1 FROM desktop_attempts WHERE task_id=? LIMIT 1', (task_id,)).fetchone():
            raise ValueError('A registered desktop action must use attempt recovery')
        started = datetime.fromisoformat(task['attempted_at_utc'])
        failed = datetime.fromisoformat(worker['observed_at_utc'])
        if (started.tzinfo is None or failed.tzinfo is None or failed > datetime.now(UTC)
                or not 0 <= (failed - started).total_seconds() <= 60):
            raise ValueError('Exact local metadata failure clock required')
        candidates = [p for p in (root / 'requests').glob(task_id + '-*.json')
                      if started.timestamp() <= p.stat().st_mtime <= failed.timestamp()]
        if len(candidates) != 1 or prepared is not None and prepared.resolve() != candidates[0].resolve():
            raise ValueError('Exactly one current canonical prepared request required')
        prepared = candidates[0]
        original = _read_private(prepared, root / 'requests', task_id)
        attempt = prepared.stem
        if (not re.fullmatch(re.escape(task_id) + r'-[0-9a-f]{32}', attempt)
                or original.get('query_attempt_id') != attempt
                or original.get('desktop_input_contract') != DESKTOP_INPUT_CONTRACT):
            raise ValueError('Exact v4 prelaunch request identity required')
        request = task_request(root, task)
        if (request.get('action') != 'download' or request.get('contract_version') != CONTRACT_VERSION
                or not prepared_request_matches(original, request, {**task, 'active_attempt_id': attempt})):
            raise ValueError('Unlaunched claim source scope differs')
        # Any older stage/response/progress for this task makes the claim
        # ambiguous too. Retain it for explicit reconciliation, never delete it.
        if (list((root / 'raw').glob(task_id + '-*')) or list((root / 'progress').glob(task_id + '-*'))
                or con.execute("SELECT 1 FROM desktop_replays WHERE task_id=? LIMIT 1", (task_id,)).fetchone()):
            raise ValueError('Source stage, response, progress or operator replay prevents local requeue')
        traffic = con.execute("SELECT event_id FROM traffic WHERE action='download' AND state='running' "
                              "AND started_at_utc=?", (task['attempted_at_utc'],)).fetchall()
        if len(traffic) != 1:
            raise ValueError('Exact unlaunched claim traffic event required')
        audit = {'contract': 'canonical_v4_metadata_failure_before_desktop_launch_v1',
                 'task_id': task_id, 'unlaunched_attempt_id': attempt,
                 'claim_started_at_utc': task['attempted_at_utc'], 'metadata_failed_at_utc': worker['observed_at_utc'],
                 'prepared_request_path': str(prepared.relative_to(root)), 'prepared_request_sha256': sha256_file(prepared),
                 'bridge_launch_order_sha256': hashlib.sha256(inspect.getsource(DesktopBridge.execute).encode()).hexdigest(),
                 'proof': 'canonical_begin_attempt_commits_before_subprocess_no_registration_or_source_artifacts',
                 'provider_queries_sent': 0, 'unknown_source_actions_retried': 0, 'source_rows_adopted': False,
                 'observed_at_utc': datetime.now(UTC).isoformat()}
        path = root / 'operator_replays' / (attempt + '-unlaunched-metadata.json')
        if path.exists():
            raise ValueError('Prior unlaunched recovery requires reconciliation')
        atomic_write_json(path, audit)
        con.execute("UPDATE tasks SET state='pending',last_error_code='local_metadata_before_launch_recovered' "
                    "WHERE task_id=? AND state='running' AND active_attempt_id IS NULL", (task_id,))
        con.execute("UPDATE traffic SET state='failed_before_desktop_launch',completed_at_utc=? WHERE event_id=?",
                    (audit['observed_at_utc'], traffic[0]['event_id']))
    atomic_write_json(worker_path, {'contract_version': CONTRACT_VERSION, 'state': 'prequery_retry_scheduled',
                                   'task_id': task_id, 'observed_at_utc': audit['observed_at_utc']})
    return True


def prepared_request_matches(original: dict, request: dict, task: dict) -> bool:
    """Only reviewed execution metadata may supplement the exact source scope."""
    from downloader.tej_history import DESKTOP_INPUT_CONTRACT
    expected={**request,'contract_version':4,'task_id':task['task_id']}
    candidate=dict(original)
    contract=candidate.pop('desktop_input_contract',None)
    if contract not in (None, DESKTOP_INPUT_CONTRACT, 'native_control_events_guarded_date_focus_no_mouse_v1',
                        'native_control_events_guarded_date_focus_acknowledged_chars_no_mouse_v2',
                        'owned_control_date_events_exact_readback_no_global_input_v3',
                        'native_acknowledged_date_model_commit_no_mouse_v4',
                        'native_acknowledged_date_model_commit_blank_mask_no_mouse_v5'):
        return False
    attempt=candidate.pop('query_attempt_id',None)
    if attempt is not None and attempt!=task.get('active_attempt_id'):
        return False
    return candidate==expected


def _connect(root):
    from downloader.tej_history import connect
    return connect(root)


def _read_private(path: Path, parent: Path, task_id: str) -> dict:
    path = path.resolve()
    if (path.parent != parent.resolve() or not path.name.startswith(task_id + '-')
            or not path.is_file() or path.stat().st_size > 2 * 1024 ** 2):
        raise ValueError('Exact bounded private task evidence required')
    return json.loads(path.read_text(encoding='utf-8-sig'))


def query_stage(root: Path, task: dict, request: dict, prepared: Path | None = None) -> tuple[dict, Path]:
    """Exact active attempt, or the single unambiguous pre-attempt legacy stage."""
    active = task.get('active_attempt_id')
    if active:
        if not re.fullmatch(re.escape(task['task_id']) + r'-[0-9a-f]{32}', active):
            raise ValueError('Invalid active desktop attempt identity')
        paths = [root / 'raw' / (active + '.json.stage.json')]
        if prepared is not None and prepared.resolve() != (root / 'requests' / (active + '.json')).resolve():
            raise ValueError('Operator evidence is not the current unresolved attempt')
    else:
        paths = list((root / 'raw').glob(task['task_id'] + '-*.json.stage.json'))
        if len(paths) != 1:
            raise ValueError('Exact original prepreview scope proof required; no new query')
        if prepared is not None and paths[0].name != prepared.name + '.stage.json':
            raise ValueError('Operator evidence is not the unique original stage')
    stage = _read_private(paths[0], root / 'raw', task['task_id'])
    if (stage.get('contract_version') != 4 or stage.get('task_id') != task['task_id']
            or stage.get('stage') != 'prepreview_verified'
            or any(stage.get(k) != request.get(k) for k in ('type', 'smart_id', 'table', 'fields'))
            or any(not isinstance(stage.get(k), list) or len(stage[k]) != len(request[k])
                   or set(stage[k]) != set(request[k]) for k in ('company_labels', 'date_labels'))
            or active and stage.get('query_attempt_id') != active):
        raise ValueError('Original query-stage scope differs from recovery')
    proof = stage.get('source_scope_proof', {})
    snapshot = request.get('source_key_mode')==1
    if (proof.get('contract') != 'editable_source_scope_v1'
            or any(proof.get(k) is not True for k in (('company_group_enabled', 'vendor_notices_absent', 'binding_readback_verified')
                                                      if snapshot else ('company_group_enabled', 'date_group_enabled', 'vendor_notices_absent', 'binding_readback_verified')))
            or snapshot and (proof.get('source_key_mode')!=1 or proof.get('date_axis_not_used') is not True)):
        raise ValueError('Exact original editable query proof required')
    if prepared is not None:
        original = _read_private(prepared, root / 'requests', task['task_id'])
        if (original.get('contract_version') != 4 or original.get('action') != 'download'
                or original.get('task_id') != task['task_id']
                or active and original.get('query_attempt_id')!=active
                or any(original.get(k) != request.get(k) for k in
                       ('type', 'smart_id', 'table', 'fields', 'company_labels', 'date_labels', 'start', 'end',
                        'source_key_mode', 'key_layout_contract'))):
            raise ValueError('Exact original prepared download scope required')
    return stage, paths[0]


def begin_attempt(root: Path, task: dict, attempt_id: str, request_path: Path, output: Path) -> None:
    with closing(_connect(root)) as con, con:
        row = con.execute('SELECT active_attempt_id FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
        if row is None:
            raise ValueError('Desktop acquisition requires a registered task')
        con.execute('INSERT INTO desktop_attempts VALUES(?,?,?,?,?,?,?,?)',
                    (attempt_id, task['task_id'], str(request_path.relative_to(root)), str(output.relative_to(root)),
                     datetime.now(UTC).isoformat(), 'prepared', row[0], None))
        con.execute('UPDATE tasks SET active_attempt_id=? WHERE task_id=?', (attempt_id, task['task_id']))


def finish_attempt(root: Path, attempt_id: str, state: str) -> None:
    try:
        with closing(_connect(root)) as con, con:
            con.execute('UPDATE desktop_attempts SET state=?,finished_at_utc=? WHERE attempt_id=?',
                        (state, datetime.now(UTC).isoformat(), attempt_id))
    except sqlite3.OperationalError as exc:
        if not metadata_busy(exc):
            raise
        # The task/active attempt remains unresolved. The already durable raw
        # response or exact negative prequery outcome drives reconciliation;
        # this secondary DB annotation must not hide that stronger evidence.
        atomic_write_json(root/'diagnostics'/(attempt_id+'-attempt_state_deferred.json'),{
            'contract':ATTEMPT_CONTRACT,'attempt_id':attempt_id,'requested_state':state,
            'state':'metadata_busy_annotation_deferred','observed_at_utc':datetime.now(UTC).isoformat(),
            'provider_queries_sent':0,'automatic_query_replay':False})


def metadata_busy(exc: sqlite3.OperationalError) -> bool:
    code = getattr(exc,'sqlite_errorcode',None)
    return ((code & 255) in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED) if type(code) is int else
            str(exc) in ('database is locked','database table is locked'))


def proved_unsent_prequery(root: Path, con, task: dict, *, finished: bool = False) -> dict:
    """One exact negative-proof validator shared by reconciliation and retries."""
    from downloader.tej_history import CONTRACT_VERSION, PREQUERY_FAILURES, SOURCE_SCOPE_CONTRACT, task_request
    task_id=task['task_id'];attempt=task.get('active_attempt_id')
    if (task.get('kind')!='download' or task.get('scope_contract')!=SOURCE_SCOPE_CONTRACT
            or not isinstance(attempt,str) or not re.fullmatch(re.escape(task_id)+r'-[0-9a-f]{32}',attempt)):
        raise ValueError('Exact active editable download attempt required')
    response=root/'raw'/(attempt+'.json');outcome=root/'raw'/(attempt+'.json.outcome.json')
    prepared=root/'requests'/(attempt+'.json')
    if (response.exists() or response.with_suffix('.json.stage.json').exists()
            or outcome.resolve().parent!=(root/'raw').resolve() or not outcome.is_file()
            or outcome.stat().st_size>4096
            or any(task.get(k) is not None for k in ('actual_rows','receipt_path','completed_at_utc'))):
        raise ValueError('Ambiguous/adopted prequery outcome; no automatic recovery')
    body=outcome.read_bytes();proof=json.loads(body.decode('utf-8-sig'))
    original=_read_private(prepared,root/'requests',task_id);request=task_request(root,task)
    if (request.get('action')!='download' or request.get('contract_version')!=CONTRACT_VERSION
            or original.get('query_attempt_id')!=attempt or not prepared_request_matches(original,request,task)
            or proof.get('contract_version')!=CONTRACT_VERSION or proof.get('provider')!='tej_smart_wizard'
            or proof.get('action')!='download' or proof.get('task_id')!=task_id
            or proof.get('market_data_query_submission_possible') is not False
            or proof.get('error_code') not in PREQUERY_FAILURES
            or any(proof.get(k)!=request.get(k) for k in ('type','smart_id','table'))):
        raise ValueError('Exact current-attempt proved-unsent prequery scope required')
    registered=con.execute('SELECT * FROM desktop_attempts WHERE attempt_id=?',(attempt,)).fetchone()
    if (registered is None or registered['task_id']!=task_id
            or registered['request_path']!=str(prepared.relative_to(root))
            or registered['raw_path']!=str(response.relative_to(root))
            or finished and (registered['state']!='proven_not_submitted' or registered['finished_at_utc'] is None)):
        raise ValueError('Registered active prequery attempt differs or is unfinished')
    if proof['error_code'] == 'desktop_transport_unavailable_before_preview':
        from downloader.tej_windows_transport import validate_unpermitted
        evidence = root/'launches'/(attempt+'-unpermitted.json')
        if proof.get('launch_admission_path') != str(evidence.relative_to(root)):
            raise ValueError('Exact launch admission evidence required')
        validate_unpermitted(root, prepared, evidence)
    try:
        clocks=[datetime.fromisoformat(value) for value in (registered['started_at_utc'],proof['observed_at_utc'])]
        if (any(clock.tzinfo is None for clock in clocks)
                or not 0 <= (clocks[1]-clocks[0]).total_seconds() <= 900):
            raise ValueError('Prequery outcome is outside the exact attempt deadline')
        if finished:
            end=datetime.fromisoformat(registered['finished_at_utc'])
            if end.tzinfo is None or not clocks[1] <= end <= clocks[0]+timedelta(seconds=900):
                raise ValueError('Prequery attempt finished outside its exact deadline')
    except (KeyError,TypeError) as exc:
        raise ValueError('Exact aware prequery outcome clock required') from exc
    return {'attempt_id':attempt,'request':request,'outcome':proof,
            'prepared_path':prepared,'outcome_path':outcome,
            'prepared_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
            'prequery_outcome_sha256':hashlib.sha256(body).hexdigest()}


def recover_prequery_outcome(root: Path, task_id: str) -> bool:
    """Reconcile a DB interruption only from the exact proved-unsent attempt.

    Called under .download.lock; no desktop interaction or provider request.
    Unknown/foreign/changed proof remains a barrier. This uses the same bounded
    local-input retry budget as run_one, not a second retry/scheduling policy.
    """
    from downloader.tej_history import CONTRACT_VERSION, SOURCE_SCOPE_CONTRACT, _mark_prequery_failure

    with closing(_connect(root)) as con,con:
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if (row is None or row['kind']!='download' or row['scope_contract']!=SOURCE_SCOPE_CONTRACT
                or not (row['state']=='running' or row['state']=='blocked' and row['last_error_code']=='unknown_outcome_no_auto_retry')):
            return False
        task=dict(row);attempt=task['active_attempt_id']
        if not isinstance(attempt,str) or not re.fullmatch(re.escape(task_id)+r'-[0-9a-f]{32}',attempt):
            return False
        outcome=root/'raw'/(attempt+'.json.outcome.json')
        if not outcome.is_file():
            return False
        verified=proved_unsent_prequery(root,con,task)
        prepared=verified['prepared_path'];proof=verified['outcome']
        audit_path=root/'local_prequery_recovery'/(attempt+'.json')
        audit={'contract':'exact_unsent_prequery_db_reconciliation_v1','state':'prepared','task_id':task_id,
               'query_attempt_id':attempt,'observed_at_utc':datetime.now(UTC).isoformat(),
               'prepared_request_path':str(prepared.relative_to(root)),
               'prepared_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
               'prequery_outcome_path':str(outcome.relative_to(root)),
               'prequery_outcome_sha256':hashlib.sha256(outcome.read_bytes()).hexdigest(),
               'provider_queries_sent':0,'source_rows_adopted':False,'automatic_unknown_replay':False,
               'original_attempt_preserved':True}
        atomic_write_json(audit_path,audit,durable=True)
        state=_mark_prequery_failure(con,task,proof['error_code'])
        con.execute("UPDATE desktop_attempts SET state='proven_not_submitted',finished_at_utc=? WHERE attempt_id=?",
                    (audit['observed_at_utc'],attempt))
        con.execute("UPDATE traffic SET state='failed_before_preview',completed_at_utc=? WHERE state='running' "
                    "AND action='download' AND started_at_utc=?",(audit['observed_at_utc'],task['attempted_at_utc']))
    audit.update(state='committed',worker_state=state)
    try:
        atomic_write_json(audit_path,audit,durable=True)
    except OSError:
        pass  # Durable intent + committed DB suffice; never repeat the retry-budget mutation.
    atomic_write_json(root/'worker_status.json',{'contract_version':CONTRACT_VERSION,'state':state,
        'task_id':task_id,'observed_at_utc':datetime.now(UTC).isoformat()})
    return True


PREQUERY_RETRY_CONTRACT='exact_unsent_table_retry_window_v1'
PREQUERY_DEFERRED='prequery_failure_deferred'
EXHAUSTED_PREQUERY_ERRORS=frozenset({'date_input_prequery_needs_review',
    'list_selection_prequery_needs_review','query_activation_prequery_needs_review'})


def defer_unsent_prequery(root: Path, task_id: str, bridge, *, base_seconds: int = 60,
                         max_seconds: int = 900) -> dict:
    """Retry one proved-unsent fragment after a durable per-table backoff.

    Caller holds .download.lock. Only a finished exact negative proof and a
    fresh normal read-only interface check authorize this state transition.
    The two immediate retries stay exhausted; subsequent probes are bounded
    by this one shared table window, including after service/host restarts.
    """
    from downloader.tej_history import CONTRACT_VERSION
    if type(base_seconds) is not int or type(max_seconds) is not int or not 5 <= base_seconds <= max_seconds <= 3600:
        raise ValueError('Invalid bounded proved-unsent backoff')

    def verified_task(con):
        row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task_id,)).fetchone()
        if (row is None or row['state']!='blocked' or row['safe_prequery_retries']<2
                or row['last_error_code'] not in EXHAUSTED_PREQUERY_ERRORS):
            raise ValueError('Only an exhausted proved-unsent local input task may be deferred')
        if (con.execute("SELECT 1 FROM tasks WHERE state='running' OR "
                "(state='blocked' AND last_error_code='unknown_outcome_no_auto_retry') LIMIT 1").fetchone()
                or con.execute("SELECT 1 FROM meta WHERE key IN ('desktop_interface_recovery_required',"
                    "'source_period_replan_required')").fetchone()):
            raise ValueError('Unresolved shared source action remains a barrier')
        task=dict(row);proof=proved_unsent_prequery(root,con,task,finished=True)
        from downloader.tej_history import PREQUERY_FAILURES
        if PREQUERY_FAILURES[proof['outcome']['error_code']]!=task['last_error_code']:
            raise ValueError('Current task classification differs from exact negative proof')
        return task,proof

    with closing(_connect(root)) as con:
        task,verified=verified_task(con)
    request={**verified['request'],'action':'confirm_metadata_error_cleared'}
    check_started=datetime.now(UTC)
    payload,readback,_=bridge.execute(root,{**task,'request_json':json.dumps(request)})
    try:
        observed=datetime.fromisoformat(payload['observed_at_utc'].replace('Z','+00:00'))
        if observed.tzinfo is None or not check_started <= observed <= datetime.now(UTC)+timedelta(seconds=5):
            raise ValueError('Shared interface readback is not fresh')
    except (KeyError,TypeError,AttributeError) as exc:
        raise ValueError('Fresh aware interface readback required') from exc
    if (payload.get('contract_version')!=CONTRACT_VERSION or payload.get('provider')!='tej_smart_wizard'
            or payload.get('action')!=request['action'] or payload.get('task_id')!=task_id
            or any(payload.get(k)!=request.get(k) for k in ('type','smart_id','table'))
            or any(payload.get(k) is not True for k in
                ('source_binding_stable','vendor_notices_absent','source_selectors_enabled','company_group_enabled'))
            or any(payload.get(k) is not False for k in ('market_data_query_submitted','source_axes_adopted','credentials_read'))
            or readback.resolve().parent!=(root/'raw').resolve() or not readback.name.startswith(task_id+'-')
            or not readback.is_file() or readback.stat().st_size>2*1024**2
            or json.loads(readback.read_text(encoding='utf-8-sig'))!=payload):
        raise ValueError('Fresh shared interface is not independently verified usable')
    with closing(_connect(root)) as con,con:
        current,rechecked=verified_task(con)
        if (current!=task or any(rechecked[k]!=verified[k] for k in
                ('attempt_id','prepared_request_sha256','prequery_outcome_sha256'))):
            raise ValueError('Current unsent task/evidence changed during interface verification')
        previous=con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?',(task['table_id'],)).fetchone()
        failures=previous['consecutive_failures']+1 if previous else 1
        seconds=min(max_seconds,base_seconds*2**min(failures-1,12))
        now=datetime.now(UTC);due=(now+timedelta(seconds=seconds)).isoformat()
        audit_path=root/'local_prequery_recovery'/(verified['attempt_id']+'-table_retry.json')
        result={'state':PREQUERY_DEFERRED,'task_id':task_id,'table_id':task['table_id'],
                'next_attempt_at_utc':due,'backoff_seconds':seconds,'consecutive_failures':failures,
                'provider_queries_sent':0,'source_rows_adopted':False,'data_query_repeated':False,
                'audit':str(audit_path.relative_to(root))}
        audit={'contract':PREQUERY_RETRY_CONTRACT,'state':'prepared','observed_at_utc':now.isoformat(),
               'query_attempt_id':verified['attempt_id'],'original_error':task['last_error_code'],
               'prepared_request_sha256':verified['prepared_request_sha256'],
               'prequery_outcome_sha256':verified['prequery_outcome_sha256'],
               'interface_readback_path':str(readback.relative_to(root)),
               'interface_readback_sha256':hashlib.sha256(readback.read_bytes()).hexdigest(),
               'original_evidence_preserved':True,'unknown_outcome_auto_retry':False,'result':result}
        atomic_write_json(audit_path,audit,durable=True)
        con.execute('INSERT OR REPLACE INTO desktop_retry_windows VALUES(?,?,?,?,?,?,?)',
            (task['table_id'],task_id,verified['attempt_id'],failures,due,task['last_error_code'],str(audit_path.relative_to(root))))
        con.execute("UPDATE tasks SET state='pending',last_error_code=?,next_attempt_at_utc=? WHERE task_id=?",
            (PREQUERY_DEFERRED,due,task_id))
        con.execute("UPDATE tables SET state='waiting_local_retry',last_error_code=? WHERE table_id=?",
            (PREQUERY_DEFERRED,task['table_id']))
    audit['state']='committed'
    try:
        atomic_write_json(audit_path,audit,durable=True)
    except OSError:
        result={**result,'audit_commit_marker_pending':True}
    return result


def validate_active_attempt(root: Path, task: dict, payload: dict) -> None:
    with closing(_connect(root)) as con:
        row = con.execute('SELECT active_attempt_id FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
    active = row[0] if row else None
    if not active:
        return  # Legacy source receipts retain their actual original contract.
    from downloader.tej_history import task_request
    task = {**task, 'active_attempt_id':active}
    request = task_request(root, task)
    stage, _ = query_stage(root, task, request)
    if payload.get('query_attempt_id') != active:
        raise ValueError('Source response belongs to a different desktop attempt')
    if (stage.get('preview_submission_contract') != payload.get('preview_submission_contract')
            or payload.get('source_outcome') != 'explicit_empty_scope'
            and payload.get('fresh_preview_transition_verified') is not True):
        raise ValueError('Fresh desktop query outcome proof required')


def receipt_attempt_evidence(root: Path, task: dict, request: dict, payload: dict) -> dict:
    """Bind new receipts to immutable prepared/stage bytes; legacy stays legacy."""
    active = payload.get('query_attempt_id')
    if not active:
        return {}
    prepared = root / 'requests' / (active + '.json')
    _, stage_path = query_stage(root, {**task, 'active_attempt_id': active}, request, prepared)
    return {'desktop_attempt_contract': ATTEMPT_CONTRACT, 'query_attempt_id': active,
            'preview_submission_contract': payload.get('preview_submission_contract'),
            'prepared_request_path': str(prepared.relative_to(root)),
            'prepared_request_sha256': hashlib.sha256(prepared.read_bytes()).hexdigest(),
            'query_stage_path': str(stage_path.relative_to(root)),
            'query_stage_sha256': hashlib.sha256(stage_path.read_bytes()).hexdigest()}


def unstaged_interop_diagnostic(root: Path, task: dict, request: dict, prepared: Path) -> Path:
    """Bind a known WSL launch diagnostic; NOT evidence the query was unsent.

    Only the explicit operator replay path uses this. Missing stage/response
    never enables automatic recovery or lets us adopt an old Preview.
    """
    active = task.get('active_attempt_id')
    if (not isinstance(active, str) or not re.fullmatch(re.escape(task['task_id']) + r'-[0-9a-f]{32}', active)
            or prepared.resolve() != (root / 'requests' / (active + '.json')).resolve()):
        raise ValueError('Exact active interop attempt required')
    original = _read_private(prepared, root / 'requests', task['task_id'])
    if not prepared_request_matches(original, request, task):
        raise ValueError('Original interop request scope differs')
    raw = root / 'raw' / (active + '.json')
    if any(p.exists() for p in (raw, raw.with_suffix('.json.stage.json'), raw.with_suffix('.json.outcome.json'),
                                root / 'progress' / (active + '.json.progress.json'))):
        raise ValueError('Source/stage/progress evidence exists; use exact original result recovery')
    diagnostic = root / 'diagnostics' / (active + '.txt')
    if diagnostic.is_symlink() or not diagnostic.is_file() or diagnostic.stat().st_size > 4096:
        raise ValueError('Bounded original WSL diagnostic required')
    if not re.fullmatch(r'<3>WSL \([0-9]+ - \) ERROR: UtilAcceptVsock:[0-9]+: accept4 failed 110\s*',
                        diagnostic.read_text(encoding='utf-8-sig')):
        raise ValueError('Unreviewed unstaged error; operator replay refused')
    with closing(_connect(root)) as con:
        attempt = con.execute('SELECT * FROM desktop_attempts WHERE attempt_id=?', (active,)).fetchone()
    if (attempt is None or attempt['task_id'] != task['task_id'] or attempt['state'] != 'unknown_outcome'
            or attempt['request_path'] != str(prepared.resolve().relative_to(root.resolve()))
            or attempt['raw_path'] != str(raw.relative_to(root))):
        raise ValueError('Exact registered unknown interop attempt required')
    return diagnostic


def retry_unknown_download(root: Path, task_id: str, bridge, prepared: Path, *,
                           allow_unstaged_interop: bool = False) -> dict:
    """Explicit one-shot operator replay. Preserves unknown evidence and cost risk.

    Called ONLY by the operator CLI, under its canonical dataset lock; not by
    the scheduler or automatic recovery. A stable UI does not prove that the
    original request was never charged. The new outcome requires new evidence.
    """
    from downloader.tej_history import connect, run_one, task_request
    with closing(connect(root)) as con:
        row = con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone()
        if (row is None or row['kind'] != 'download' or row['state'] != 'blocked'
                or row['last_error_code'] != 'unknown_outcome_no_auto_retry'
                or con.execute("SELECT 1 FROM tasks WHERE state='running' OR (state='blocked' AND "
                               "last_error_code='unknown_outcome_no_auto_retry' AND task_id!=?) LIMIT 1",
                               (task_id,)).fetchone()):
            raise ValueError('One exact quiescent unknown download required')
    task = dict(row)
    request = task_request(root, task)
    if type(allow_unstaged_interop) is not bool:
        raise ValueError('Explicit operator replay mode required')
    diagnostic = unstaged_interop_diagnostic(root, task, request, prepared) if allow_unstaged_interop else None
    stage_path = None
    if not allow_unstaged_interop:
        _, stage_path = query_stage(root, task, request, prepared)
    payload, output, _ = bridge.execute(root, {**task, 'request_json':json.dumps({**request, 'action':'inspect_query_runtime'})})
    expected_true = ('vendor_notices_absent', 'source_binding_stable', 'source_selectors_enabled',
                     'company_group_enabled', 'date_group_enabled', 'source_binding_unchanged')
    if not allow_unstaged_interop:
        expected_true += ('binding_matches_failed_plan',)
    expected_false = ('market_data_query_submitted', 'date_text_input_sent', 'query_button_invoked',
                      'source_rows_adopted', 'credentials_read')
    if (payload.get('contract_version') != 4 or payload.get('action') != 'inspect_query_runtime'
            or payload.get('task_id') != task_id or payload.get('provider') != 'tej_smart_wizard'
            or any(payload.get(k) != request.get(k) for k in ('type', 'smart_id', 'table'))
            or any(payload.get(k) is not True for k in expected_true)
            or any(payload.get(k) is not False for k in expected_false)):
        raise ValueError('Unverified current desktop context; unknown barrier preserved')
    fields = payload.get('current_field_lists', [])
    axes = payload.get('current_query_axes', [])
    button = payload.get('preview_button', {})
    role_state = button.get('accessible_role_state')
    texts = [x.get('native_text') for x in payload.get('date_input_controls', [])
             if re.fullmatch(r'\d{4}/\d{2}/\d{2}', str(x.get('native_text')))]
    if not allow_unstaged_interop and (len(fields) != 2 or fields[-1].get('items') != request['fields'] or len(axes) != 6
            or axes[3].get('items') != request['company_labels'] or axes[-1].get('items') != request['date_labels']
            or sorted(texts) != sorted([request['start'].replace('-', '/'), request['end'].replace('-', '/')])
            or button.get('enabled') is not True or button.get('visible') is not True
            or not isinstance(role_state, list) or len(role_state) != 2
            or role_state[0] != 43 or button.get('default_action') != 'Press'):
        raise ValueError('Exact current query axes/button required; unknown barrier preserved')
    if allow_unstaged_interop and (button.get('enabled') is not True or button.get('visible') is not True
            or not isinstance(role_state, list) or len(role_state) != 2 or role_state[0] != 43
            or button.get('default_action') != 'Press' or payload.get('source_key_mode') not in (1, 2, 3)):
        raise ValueError('Exact idle owned query button required; unknown barrier preserved')
    authorization = uuid.uuid4().hex
    audit_path = root / 'operator_replays' / (task_id + '-' + authorization + '.json')
    audit = {'contract':OPERATOR_REPLAY_CONTRACT, 'authorization_id':authorization, 'task_id':task_id,
             'observed_at_utc':datetime.now(UTC).isoformat(), 'original_attempted_at_utc':task['attempted_at_utc'],
             'original_request_path':str(prepared.resolve().relative_to(root.resolve())),
             'original_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
             'original_stage_path':str(stage_path.relative_to(root)) if stage_path else None,
             'original_stage_sha256':hashlib.sha256(stage_path.read_bytes()).hexdigest() if stage_path else None,
             'unstaged_interop_replay_explicitly_authorized':allow_unstaged_interop,
             'original_launch_diagnostic_path':str(diagnostic.relative_to(root)) if diagnostic else None,
             'original_launch_diagnostic_sha256':hashlib.sha256(diagnostic.read_bytes()).hexdigest() if diagnostic else None,
             'readback_path':str(output.relative_to(root)), 'readback_sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
             'original_outcome':'unknown_retained_not_claimed_unsent', 'automatic_retry':False,
             'possible_additional_provider_usage':True, 'source_rows_adopted':False}
    atomic_write_json(audit_path, audit)
    with closing(connect(root)) as con, con:
        current = dict(con.execute('SELECT * FROM tasks WHERE task_id=?', (task_id,)).fetchone())
        if current != task:
            raise ValueError('Task changed during operator inspection; no new query')
        con.execute('INSERT INTO desktop_replays VALUES(?,?,?,?,?,?,?)',
                    (authorization, task_id, task['attempted_at_utc'], str(audit_path.relative_to(root)),
                     hashlib.sha256(audit_path.read_bytes()).hexdigest(), None, None))
    result = run_one(root, bridge, retry_authorization_id=authorization)
    with closing(connect(root)) as con, con:
        con.execute('UPDATE desktop_replays SET outcome=? WHERE authorization_id=?', (result, authorization))
    return {'state':result, 'operator_replay':True, 'automatic_retry':False,
            'original_unknown_evidence_retained':True, 'authorization_id':authorization}
