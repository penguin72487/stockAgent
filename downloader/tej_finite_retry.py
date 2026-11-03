"""Admit verified finite TEJ work past a metadata-only table retry.

This never clears the full discovery retry, relaxes quarantine, or touches the
desktop.  The original scheduler remains the sole source owner.  A partial
company/date menu is evidence for its exact registered rectangle only.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import uuid

from downloader.artifact_io import sha256_file

PREFIX = 'verified_finite_metadata_retry_v1:'
REJECTION_PREFIX = 'rejected_finite_metadata_retry_v1:'
CONTRACT = 'source_bound_finite_work_during_metadata_retry_v1'


def retry_exception_sql() -> str:
    """Candidate prefilter only; the owner validates all evidence before claim."""
    return ("retry.error_code='metadata_retry_scheduled' AND tasks.kind='download' "
            "AND EXISTS (SELECT 1 FROM meta AS finite_retry WHERE "
            "finite_retry.key='" + PREFIX + "'||tasks.task_id)")


def _object(path: Path, limit: int) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('Bounded retained finite-work evidence required')
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('Finite-work evidence must be an object')
    return value


def _source_plan(con, root: Path, plan_path: Path, receipt_path: Path) -> tuple[dict, dict]:
    from downloader.tej_priority import CONTRACT as PRIORITY_CONTRACT
    plan, receipt = _object(plan_path, 2 * 1024**2), _object(receipt_path, 1024**2)
    digest = sha256_file(plan_path)
    saved = con.execute('SELECT value FROM meta WHERE key=?', ('finite_priority_plan:' + digest,)).fetchone()
    if (plan.get('contract') != PRIORITY_CONTRACT or plan.get('finite_axis_only') is not True
            or plan.get('provider_queries_by_planning') != 0
            or plan.get('full_axis_admitted_to_plan') is not False
            or plan.get('training_input_admitted') is not False
            or receipt.get('plan_sha256') != digest or receipt.get('global_plan_changed') is not False
            or Path(receipt.get('plan_path', '')).resolve() != plan_path.resolve()
            or saved is None or json.loads(saved[0]) != receipt
            or len(receipt.get('tasks', [])) != len(plan.get('requests', []))):
        raise ValueError('Finite plan and committed source receipt differ')
    sources = plan.get('sources')
    if not isinstance(sources, list) or not sources:
        raise ValueError('Pinned finite source dependencies required')
    for source in sources:
        path = Path(source['path'])
        if path.is_symlink() or not path.is_file() or sha256_file(path) != source['sha256']:
            raise ValueError('Finite source dependency changed')
    return plan, receipt


def _retry_proof(con, root: Path, table_id: str) -> dict:
    """Recheck the exact latest metadata failure and the owned UI recovery."""
    from downloader.tej_metadata_recovery import CONTRACT as RETRY_CONTRACT, failed_plan_proof
    from downloader.tej_history import CONTRACT_VERSION, METADATA_RECOVERY_CONTRACT
    retry = con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?', (table_id,)).fetchone()
    if retry is None or retry['error_code'] != 'metadata_retry_scheduled':
        raise ValueError('Exact metadata-only table retry required')
    owner = con.execute('SELECT * FROM tasks WHERE task_id=?', (retry['task_id'],)).fetchone()
    if (owner is None or owner['kind'] != 'discover' or owner['state'] != 'pending'
            or owner['last_error_code'] != 'metadata_retry_scheduled' or owner['table_id'] != table_id):
        raise ValueError('Metadata retry discovery owner changed')
    audit_path = root / retry['audit_path']
    if audit_path.parent.resolve() != (root / 'diagnostics').resolve():
        raise ValueError('Metadata retry audit escaped its owner')
    audit = _object(audit_path, 16384)
    isolation_path = root / audit.get('isolation_audit_path', '')
    if isolation_path.parent.resolve() != (root / 'diagnostics').resolve():
        raise ValueError('Metadata isolation audit escaped its owner')
    isolation = _object(isolation_path, 16384)
    proof = failed_plan_proof(root, {**dict(owner), 'state': 'blocked',
                                   'last_error_code': 'metadata_preparation_requires_recovery'})
    if (audit.get('contract') != RETRY_CONTRACT or audit.get('state') != 'metadata_retry_scheduled'
            or audit.get('task_id') != owner['task_id'] or audit.get('attempt_id') != retry['attempt_id']
            or audit.get('data_query_repeated') is not False or audit.get('source_rows_adopted') is not False
            or audit.get('original_evidence_preserved') is not True
            or isolation.get('contract') != METADATA_RECOVERY_CONTRACT
            or isolation.get('contract_version') != CONTRACT_VERSION
            or isolation.get('task_id') != owner['task_id'] or isolation.get('metadata_only_plan') is not True
            or isolation.get('data_query_repeated') is not False or isolation.get('source_axes_adopted') is not False
            or isolation.get('failed_plan_retried') is not False or isolation.get('failed_table_still_blocked') is not True
            or isolation.get('prepared_request_sha256') != proof['prepared_request_sha256']
            or not isinstance(isolation.get('readback_evidence_sha256'), list)
            or len(isolation['readback_evidence_sha256']) < 2
            or any(not isinstance(value, str) or len(value) != 64 for value in isolation['readback_evidence_sha256'])
            or any(audit.get(k) != proof[k] for k in ('attempt_id', 'prepared_request_sha256',
                   'prequery_outcome_sha256', 'bridge_script_sha256', 'failed_event_id'))):
        raise ValueError('Metadata retry has no exact independently isolated no-Preview proof')
    return dict(retry)


def enroll(root: Path, plan_path: Path, receipt_path: Path) -> dict:
    """Install task-specific admissions, retaining all prior queue evidence."""
    from downloader.tej_history import SOURCE_SCOPE_CONTRACT, connect, task_request
    from scripts.audit_tej_finite_option_axis_capture import audit
    root, plan_path, receipt_path = root.resolve(strict=True), plan_path.absolute(), receipt_path.absolute()
    with closing(connect(root)) as con:
        plan, receipt = _source_plan(con, root, plan_path, receipt_path)
        observation = plan.get('source_observation', {})
        proof = audit(root, Path(observation.get('path', '')))
        if (proof.get('observation_sha256') != observation.get('sha256')
                or proof.get('source_key_mode') != 2 or proof.get('date_menu_contains_day') is not True
                or proof.get('market_observations_obtained') is not False
                or proof.get('full_axis_admitted_to_plan') is not False
                or proof.get('training_input_admitted') is not False):
            raise ValueError('An independently verified finite native menu is required')
        discovery = con.execute('SELECT * FROM tasks WHERE task_id=?', (proof['task_id'],)).fetchone()
        if discovery is None or discovery['kind'] != 'discover' or discovery['scope_contract'] != SOURCE_SCOPE_CONTRACT:
            raise ValueError('Finite menu discovery identity changed')
        table_id = discovery['table_id']
        retry = _retry_proof(con, root, table_id)
        if retry['task_id'] != proof['task_id']:
            raise ValueError('Finite menu and metadata retry belong to different discoveries')
        records = []
        completed = 0
        for item, registered in zip(plan['requests'], receipt['tasks'], strict=True):
            row = con.execute('SELECT * FROM tasks WHERE task_id=?', (registered['task_id'],)).fetchone()
            if (row is None or row['kind'] != 'download' or row['table_id'] != table_id
                    or row['state'] not in ('pending', 'complete') or row['scope_contract'] != SOURCE_SCOPE_CONTRACT
                    or item.get('table_id') != table_id or item.get('finite_axis_observation') != observation
                    or task_request(root, dict(row), connection=con) != item.get('request')):
                raise ValueError('Registered finite task or exact source scope changed')
            if row['state'] == 'complete':
                completed += 1  # Never reopen or certify the completed source response.
                continue
            records.append({'contract': CONTRACT, 'task_id': row['task_id'], 'table_id': table_id,
                'discovery_task_id': proof['task_id'], 'request_sha256': hashlib.sha256(row['request_json'].encode()).hexdigest(),
                'plan_path': str(plan_path), 'plan_sha256': sha256_file(plan_path),
                'receipt_path': str(receipt_path), 'receipt_sha256': sha256_file(receipt_path),
                'observation_sha256': proof['observation_sha256']})
        con.execute('BEGIN IMMEDIATE')
        current = con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?', (table_id,)).fetchone()
        if current is None or dict(current) != retry:
            raise ValueError('Metadata retry changed during finite admission')
        for record in records:
            row = con.execute('SELECT * FROM tasks WHERE task_id=?', (record['task_id'],)).fetchone()
            if (row is None or row['state'] != 'pending'
                    or hashlib.sha256(row['request_json'].encode()).hexdigest() != record['request_sha256']):
                raise ValueError('Finite task changed during admission')
            key = PREFIX + record['task_id']
            prior = con.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
            if prior is not None and json.loads(prior[0]) != record:
                raise ValueError('Existing finite admission differs')
            con.execute('INSERT OR IGNORE INTO meta VALUES (?,?)', (key, json.dumps(record, sort_keys=True)))
        con.commit()
    return {'contract': CONTRACT, 'state': 'registered', 'tasks': len(records), 'already_complete': completed,
            'table_id': table_id, 'provider_queries': 0, 'metadata_retry_preserved': True,
            'full_axis_admitted': False, 'training_data_admitted': False}


def validate_before_claim(con, root: Path, task) -> None:
    """Fail closed if the task, source pins, or current retry proof changed."""
    from downloader.tej_history import task_request
    retry = con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?', (task['table_id'],)).fetchone()
    if retry is None or retry['task_id'] == task['task_id']:
        return
    if retry['error_code'] != 'metadata_retry_scheduled':
        return  # The normal selector does not admit other retry windows.
    marker = con.execute('SELECT value FROM meta WHERE key=?', (PREFIX + task['task_id'],)).fetchone()
    if marker is None:
        raise ValueError('No exact finite metadata retry admission')
    record = json.loads(marker[0])
    if (record.get('contract') != CONTRACT or record.get('task_id') != task['task_id']
            or record.get('table_id') != task['table_id'] or record.get('discovery_task_id') != retry['task_id']
            or record.get('request_sha256') != hashlib.sha256(task['request_json'].encode()).hexdigest()):
        raise ValueError('Finite admission identity changed')
    plan_path, receipt_path = Path(record['plan_path']), Path(record['receipt_path'])
    if sha256_file(plan_path) != record['plan_sha256'] or sha256_file(receipt_path) != record['receipt_sha256']:
        raise ValueError('Finite admission plan or receipt changed')
    plan, receipt = _source_plan(con, root, plan_path, receipt_path)
    matches = [(item, saved) for item, saved in zip(plan['requests'], receipt['tasks'], strict=True)
               if saved.get('task_id') == task['task_id']]
    if (len(matches) != 1 or matches[0][0].get('finite_axis_observation') != plan.get('source_observation')
            or plan.get('source_observation', {}).get('sha256') != record['observation_sha256']
            or task_request(root, dict(task), connection=con) != matches[0][0].get('request')):
        raise ValueError('Finite admission no longer matches its original rectangle')
    _retry_proof(con, root, task['table_id'])


def reject_invalid_admission(con, task, error: Exception, observed_at_utc: str) -> None:
    """Isolate one unsent finite admission without changing its source task.

    This is called only when the candidate prefilter selected an admitted task
    and its before-claim validation failed. Keep the original marker and the
    failure in durable metadata, remove that exact marker from selection, and
    leave the original pending task and whole-table retry untouched. A later
    re-admission must independently verify the complete source plan again.
    """
    key = PREFIX + task['task_id']
    marker = con.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    current = con.execute('SELECT * FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
    retry = con.execute('SELECT * FROM desktop_retry_windows WHERE table_id=?', (task['table_id'],)).fetchone()
    if (marker is None or current is None or current['state'] != 'pending'
            or current['kind'] != 'download' or current['table_id'] != task['table_id']
            or current['request_json'] != task['request_json'] or retry is None
            or retry['error_code'] != 'metadata_retry_scheduled' or retry['task_id'] == task['task_id']):
        raise ValueError('Finite admission changed before isolation') from error
    record = {'contract': 'finite_metadata_retry_rejected_before_query_v1',
              'task_id': task['task_id'], 'table_id': task['table_id'],
              'metadata_retry_task_id': retry['task_id'], 'observed_at_utc': observed_at_utc,
              'exception_type': type(error).__name__, 'reason': str(error)[:1024],
              'original_admission': marker[0], 'provider_query_submitted': False}
    con.execute('INSERT INTO meta VALUES (?,?)',
                (REJECTION_PREFIX + task['task_id'] + ':' + uuid.uuid4().hex, json.dumps(record, sort_keys=True)))
    removed = con.execute('DELETE FROM meta WHERE key=? AND value=?', (key, marker[0]))
    if removed.rowcount != 1:
        raise ValueError('Finite admission changed during isolation') from error
