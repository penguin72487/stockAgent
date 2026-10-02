"""Attempt-scoped desktop evidence; explicit replay never means safe auto retry.

The canonical task/source ABI is unchanged. Each actual Preview has its own
private prepared request/stage, including a failed attempt retained by an
operator replay. Recovery must not choose an arbitrary historical stage.
"""
from contextlib import closing
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import re
import uuid

from downloader.artifact_io import atomic_write_json

ATTEMPT_CONTRACT = 'attempt_scoped_preview_v1'
OPERATOR_REPLAY_CONTRACT = 'exact_unknown_download_operator_replay_v1'


def prepared_request_matches(original: dict, request: dict, task: dict) -> bool:
    """Only reviewed execution metadata may supplement the exact source scope."""
    from downloader.tej_history import DESKTOP_INPUT_CONTRACT
    expected={**request,'contract_version':4,'task_id':task['task_id']}
    candidate=dict(original)
    contract=candidate.pop('desktop_input_contract',None)
    if contract not in (None, DESKTOP_INPUT_CONTRACT, 'native_control_events_guarded_date_focus_no_mouse_v1',
                        'native_control_events_guarded_date_focus_acknowledged_chars_no_mouse_v2',
                        'owned_control_date_events_exact_readback_no_global_input_v3',
                        'native_acknowledged_date_model_commit_no_mouse_v4'):
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
    with closing(_connect(root)) as con, con:
        con.execute('UPDATE desktop_attempts SET state=?,finished_at_utc=? WHERE attempt_id=?',
                    (state, datetime.now(UTC).isoformat(), attempt_id))


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


def retry_unknown_download(root: Path, task_id: str, bridge, prepared: Path) -> dict:
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
    stage, stage_path = query_stage(root, task, request, prepared)
    payload, output, _ = bridge.execute(root, {**task, 'request_json':json.dumps({**request, 'action':'inspect_query_runtime'})})
    expected_true = ('vendor_notices_absent', 'source_binding_stable', 'source_selectors_enabled',
                     'binding_matches_failed_plan', 'company_group_enabled', 'date_group_enabled', 'source_binding_unchanged')
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
    if (len(fields) != 2 or fields[-1].get('items') != request['fields'] or len(axes) != 6
            or axes[3].get('items') != request['company_labels'] or axes[-1].get('items') != request['date_labels']
            or sorted(texts) != sorted([request['start'].replace('-', '/'), request['end'].replace('-', '/')])
            or button.get('enabled') is not True or button.get('visible') is not True
            or not isinstance(role_state, list) or len(role_state) != 2
            or role_state[0] != 43 or button.get('default_action') != 'Press'):
        raise ValueError('Exact current query axes/button required; unknown barrier preserved')
    authorization = uuid.uuid4().hex
    audit_path = root / 'operator_replays' / (task_id + '-' + authorization + '.json')
    audit = {'contract':OPERATOR_REPLAY_CONTRACT, 'authorization_id':authorization, 'task_id':task_id,
             'observed_at_utc':datetime.now(UTC).isoformat(), 'original_attempted_at_utc':task['attempted_at_utc'],
             'original_request_path':str(prepared.resolve().relative_to(root.resolve())),
             'original_request_sha256':hashlib.sha256(prepared.read_bytes()).hexdigest(),
             'original_stage_path':str(stage_path.relative_to(root)),
             'original_stage_sha256':hashlib.sha256(stage_path.read_bytes()).hexdigest(),
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
