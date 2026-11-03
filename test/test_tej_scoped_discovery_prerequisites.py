from contextlib import closing
import json
import sqlite3
from types import SimpleNamespace

import pytest

from downloader.tej_history import configure_runtime_policy, connect
from scripts.run_tej_scoped_discovery_prerequisites import choice, recoverable_metadata_task, selected
from scripts import run_tej_scoped_discovery_prerequisites as supervisor
from test_tej_history import registry as registry


def _status(*states, active=True):
    return {'provider': 'tej', 'priority_active': active,
            'items': [{'key': str(i), 'state': state} for i, state in enumerate(states)]}


def test_scope_choice_respects_native_rotation_and_other_requests():
    status = _status('pending', 'complete')
    assert choice({'task_id': 'due', 'kind': 'download', 'priority': -1000}, status) == 'prerequisite_incremental'
    assert choice({'task_id': '0', 'kind': 'discover', 'priority': 100}, status) == 'requested_discovery'
    assert choice({'task_id': 'other', 'kind': 'discover', 'priority': 100}, status) == 'wait_for_other_global_work'
    assert choice({'task_id': 'history', 'kind': 'download', 'priority': 100}, status) == 'wait_for_other_global_work'
    assert choice({'task_id': '', 'kind': 'inflight_or_unresolved', 'priority': 0}, status) == 'wait_for_other_global_work'
    assert choice(None, status) == 'wait'


def test_no_owner_work_after_completion_expiry_or_blocked_result():
    due = {'task_id': 'due', 'kind': 'download', 'priority': -1000}
    assert choice(due, _status('complete')) == 'complete'
    assert choice(due, _status('pending', active=False)) == 'priority_expired_or_invalid'
    assert choice(due, _status('blocked')) == 'requested_source_blocked'
    assert choice(due, _status('blocked'), metadata_recoverable=True) == 'recover_requested_metadata'
    assert choice(due, _status()) == 'invalid_empty_request'


def test_only_registered_pending_source_can_advance_through_native_selector():
    requested = _status('pending')
    source = {'task_id': 'source', 'kind': 'download', 'priority': -40}
    assert choice(source, requested, source_states={'source': 'pending'}) == 'registered_source_prerequisite'
    assert choice(source, requested, source_states={'different': 'pending'}) == 'wait_for_other_global_work'
    assert choice(source, requested, source_states={'source': 'blocked'}) == 'registered_source_blocked'
    assert choice(source, _status('complete'), source_states={'source': 'pending'}) == 'registered_source_prerequisite'
    assert choice(source, _status('complete'), source_states={'source': 'complete'}) == 'complete'
    assert choice(source, _status('complete'), source_states={'source': 'running'}) == 'wait_for_other_global_work'
    assert choice(source, _status('pending', active=False), source_states={'source': 'pending'}) == 'priority_expired_or_invalid'


def test_finite_source_receipt_requires_exact_plan_and_committed_queue_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(supervisor, 'REPO', tmp_path)
    artifacts = tmp_path / 'artifacts/markets'
    artifacts.mkdir(parents=True)
    root = tmp_path / 'data_tej'
    root.mkdir()
    plan = artifacts / 'plan.json'
    plan.write_text('{"sources": []}')
    receipt = {'contract': 'finite_source_bound_priority_requests_v1',
               'plan_path': 'artifacts/markets/plan.json', 'plan_sha256': supervisor.sha256_file(plan),
               'global_plan_changed': False, 'desktop_actions': 0, 'unknown_outcomes_retried': 0,
               'tasks': [{'task_id': 'task', 'table_id': 'table'}]}
    receipt_path = artifacts / 'receipt.json'
    receipt_path.write_text(json.dumps(receipt))
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute('CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT)')
        con.execute('CREATE TABLE tasks(task_id TEXT PRIMARY KEY,table_id TEXT,kind TEXT,scope_contract TEXT,state TEXT)')
        con.execute('INSERT INTO meta VALUES(?,?)', ('finite_priority_plan:' + receipt['plan_sha256'], json.dumps(receipt)))
        con.execute('INSERT INTO tasks VALUES(?,?,?,?,?)',
                    ('task', 'table', 'download', supervisor.SOURCE_SCOPE_CONTRACT, 'pending'))
    parsed, pinned_plan = supervisor.verified_source_receipt(root, receipt_path)
    assert parsed == receipt and pinned_plan == plan
    assert supervisor.source_task_states(root, parsed) == {'task': 'pending'}
    with sqlite3.connect(root / 'queue.sqlite3') as con:
        con.execute("UPDATE tasks SET table_id='other'")
    with pytest.raises(ValueError, match='task identity changed'):
        supervisor.source_task_states(root, parsed)
    receipt_path.write_text(json.dumps({**receipt, 'desktop_actions': 1}))
    with pytest.raises(ValueError, match='invalid finite source receipt'):
        supervisor.verified_source_receipt(root, receipt_path)
    receipt_path.write_text(json.dumps(receipt))
    plan.write_text('{"sources": ["changed"]}')
    with pytest.raises(ValueError, match='fingerprint changed'):
        supervisor.verified_source_receipt(root, receipt_path)


def test_selection_reads_native_queue_without_claiming_and_waits_for_inflight(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, 'finite_query_after_incremental_attempts': 4})
    row = selected(root)
    assert row['kind'] == 'discover'
    with closing(connect(root)) as con, con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (row['task_id'],)).fetchone()[0] == 'pending'
        con.execute("UPDATE tasks SET state='running' WHERE task_id=?", (row['task_id'],))
    assert selected(root)['kind'] == 'inflight_or_unresolved'


def test_metadata_recovery_candidate_is_only_the_exact_requested_failure(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, 'finite_query_after_incremental_attempts': 4})
    task_id = selected(root)['task_id']
    status = {'provider': 'tej', 'priority_active': True,
              'items': [{'key': task_id, 'state': 'blocked'}]}
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?",
                    ('metadata_preparation_requires_recovery', task_id))
    assert recoverable_metadata_task(root, status) == task_id
    assert recoverable_metadata_task(root, {**status, 'priority_active': False}) is None
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?", (task_id,))
    assert recoverable_metadata_task(root, status) is None


def test_metadata_recovery_requires_ready_desktop_and_preserves_native_recovery(registry, monkeypatch, tmp_path):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, 'finite_query_after_incremental_attempts': 4})
    task_id = selected(root)['task_id']
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?",
                    ('metadata_preparation_requires_recovery', task_id))
    status = {'provider': 'tej', 'priority_active': True,
              'items': [{'key': task_id, 'state': 'blocked'}]}
    monkeypatch.setattr(supervisor, 'inspect', lambda *_: status)
    monkeypatch.setattr(supervisor, 'desktop_ready', lambda *_a, **_kw: False)
    monkeypatch.setattr(supervisor, 'recover_failed_metadata', lambda *_a, **_kw: pytest.fail('not ready'))
    assert supervisor.recover_requested_metadata(root, 'request', task_id) == {'state': 'metadata_desktop_not_ready'}

    (tmp_path / 'configs').mkdir()
    (tmp_path / 'configs/tej_history.json').write_text('{}')
    (root / 'desktop_session.json').write_text('{}')
    monkeypatch.setattr(supervisor, 'REPO', tmp_path)
    monkeypatch.setattr(supervisor, 'desktop_ready', lambda *_a, **_kw: True)
    monkeypatch.setattr(supervisor, 'metadata_recovery_policy', lambda _config: {'contract': 'native'})
    monkeypatch.setattr(supervisor, 'DesktopBridge', lambda *_args: object())
    seen = []
    monkeypatch.setattr(supervisor, 'recover_failed_metadata',
                        lambda _root, _bridge, **kwargs: seen.append(kwargs) or {'state': 'waiting_metadata_interface'})
    assert supervisor.recover_requested_metadata(root, 'request', task_id)['state'] == 'waiting_metadata_interface'
    assert seen == [{'policy': {'contract': 'native'}, 'task_id': task_id}]


def test_owner_metadata_error_reenters_recovery_only_with_exact_requested_block(registry, monkeypatch):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, 'finite_query_after_incremental_attempts': 4})
    task_id = selected(root)['task_id']
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',last_error_code=? WHERE task_id=?",
                    ('metadata_preparation_requires_recovery', task_id))
    status = {'provider': 'tej', 'priority_active': True,
              'items': [{'key': task_id, 'state': 'blocked'}]}
    monkeypatch.setattr(supervisor, 'inspect', lambda *_: status)
    stdout = '{"state": "metadata_preparation_requires_recovery"}\n'
    assert supervisor.recoverable_owner_error(root, 'request', 'requested_discovery', stdout) == task_id
    assert supervisor.recoverable_owner_error(root, 'request', 'prerequisite_incremental', stdout) is None
    assert supervisor.recoverable_owner_error(root, 'request', 'requested_discovery', '{"state":"other"}') is None
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?", (task_id,))
    assert supervisor.recoverable_owner_error(root, 'request', 'requested_discovery', stdout) is None


@pytest.mark.parametrize(('code', 'stdout', 'expected'), [
    (1, '{"state": "desktop_unavailable"}\n', True),
    (0, '{"state": "desktop_unavailable"}\n', False),
    (75, '{"state": "desktop_unavailable"}\n', False),
    (1, '{"state": "unknown_outcome_no_auto_retry"}\n', False),
    (1, '{"state": "desktop_unavailable", "extra": true}\n', False),
    (1, '{"state": "desktop_unavailable"}\nnot-json', False),
    (1, '', False),
])
def test_owner_retries_only_proved_before_preview_desktop_failure(code, stdout, expected):
    assert supervisor.owner_desktop_unavailable_before_query(code, stdout) is expected


def test_relay_waits_after_desktop_failure_but_stops_for_unknown_outcome(monkeypatch, tmp_path):
    monkeypatch.setattr(supervisor, 'ROOT', tmp_path)
    monkeypatch.setattr(supervisor, 'sha256_file', lambda _path: 'same-fingerprint')
    monkeypatch.setattr(supervisor, 'inspect', lambda *_: _status('pending'))
    monkeypatch.setattr(supervisor, 'recoverable_metadata_task', lambda *_: None)
    monkeypatch.setattr(supervisor, 'selected', lambda *_: {
        'task_id': 'due', 'kind': 'download', 'priority': -1000,
    })
    monkeypatch.setattr(supervisor, 'acquisition_dispatch_status', lambda *_: {'state': 'admitted'})
    monkeypatch.setattr(supervisor.sys, 'argv', [
        'supervisor', '--request-id', 'request', '--plan', str(tmp_path / 'plan.json'),
        '--max-owner-attempts', '2',
    ])
    replies = iter(['desktop_unavailable', 'unknown_outcome_no_auto_retry'])
    owner_states = []

    def run(cmd, **_kwargs):
        if cmd[0] == 'systemctl':
            return SimpleNamespace(returncode=3)
        state = next(replies)
        owner_states.append(state)
        return SimpleNamespace(returncode=1, stdout='{"state": "' + state + '"}\n', stderr='')

    waits = []
    monkeypatch.setattr(supervisor.subprocess, 'run', run)
    monkeypatch.setattr(supervisor.time, 'sleep', waits.append)
    assert supervisor.main() == 1
    assert owner_states == ['desktop_unavailable', 'unknown_outcome_no_auto_retry']
    assert waits == [60]
