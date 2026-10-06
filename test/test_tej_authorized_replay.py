"""Standing user authority is bounded and audited, not proof of non-submission."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
from threading import Event

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_desktop_attempts import (AUTHORIZED_REPLAY_CONTRACT, authorized_replay_policy,
    authorized_replay_window, retry_unknown_download)
from downloader.tej_history import configure_runtime_policy, connect, run_one
from downloader.tej_scheduler import replay_authorized_unknown, WatchPolicy, watch_queue
from test_tej_desktop_attempts import ReplayBridge, unknown as unknown, registry as registry
from test_tej_desktop_attempts import _activate
from test_tej_self_healing import active_unknown, RetainedBridge, unstaged_script_copy as unstaged_script_copy
from test_tej_scheduler import InstantWait, emit


def grant_config():
    return {'automation': {'enabled': True, 'authorized_unknown_replay': {
        'enabled': True, 'contract': AUTHORIZED_REPLAY_CONTRACT,
        'authorization_basis': 'explicit_user_permission_to_requeue_unfinished_tej',
        'authorized_at_utc': (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        'cooldown_base_seconds': 60, 'cooldown_max_seconds': 300,
        'max_replays_per_task_hour': 2, 'max_replays_per_hour': 6}}}


def age_original(root, attempt):
    with closing(connect(root)) as con, con:
        con.execute('UPDATE desktop_attempts SET started_at_utc=?,finished_at_utc=? WHERE attempt_id=?',
                    ((datetime.now(UTC)-timedelta(minutes=11)).isoformat(),
                     (datetime.now(UTC)-timedelta(minutes=10)).isoformat(), attempt))


@pytest.fixture
def standing(unknown):
    root, task, attempt, prepared = active_unknown(unknown)
    age_original(root, attempt)
    config = grant_config()
    configure_runtime_policy(root, config)
    return root, task, prepared, config


class FreshReplayBridge(ReplayBridge):
    def __init__(self, mutation=None, *, stale=False, revoke=False):
        super().__init__(mutation)
        self.stale, self.revoke = stale, revoke

    def execute(self, root, task):
        payload, output, seconds = super().execute(root, task)
        if payload.get('action') == 'inspect_query_runtime':
            observed = datetime.now(UTC) - timedelta(seconds=60 if self.stale else 0)
            payload['observed_at_utc'] = observed.isoformat()
            atomic_write_json(output, payload)
            if self.revoke:
                configure_runtime_policy(root, {})
        return payload, output, seconds


def test_standing_replay_retains_unknown_and_adopts_a_new_actual_result(standing):
    root, task, prepared, config = standing
    before = prepared.read_bytes()
    bridge = FreshReplayBridge()
    result = replay_authorized_unknown(root, bridge, config)
    assert result['performed'] and result['state'] == 'completed_task'
    assert result['replays_in_hour'] == result['task_replays_in_hour'] == 1
    assert bridge.calls == ['inspect_query_runtime', 'download']
    assert prepared.read_bytes() == before
    with closing(connect(root)) as con:
        original = con.execute('SELECT state FROM desktop_attempts WHERE attempt_id=?',
                               (task['active_attempt_id'],)).fetchone()
        replay = dict(con.execute('SELECT * FROM desktop_replays').fetchone())
        current = con.execute('SELECT state,actual_rows FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()
    assert original['state'] == 'unknown_outcome' and tuple(current) == ('complete', 4)
    audit = json.loads((root/replay['audit_path']).read_text())
    assert audit['contract'] == AUTHORIZED_REPLAY_CONTRACT and audit['automatic_retry'] is True
    assert audit['standing_authorization'] == authorized_replay_policy(config)
    assert audit['original_outcome'] == 'unknown_retained_not_claimed_unsent'
    assert audit['possible_additional_provider_usage'] is True
    with pytest.raises(ValueError, match='already consumed'):
        run_one(root, bridge, retry_authorization_id=replay['authorization_id'])
    assert replay_authorized_unknown(root, bridge, config) is None
    assert len(bridge.calls) == 2


def test_standing_permission_also_covers_exact_old_script_copy_failure(unstaged_script_copy):
    root, task, prepared, diagnostic = unstaged_script_copy
    age_original(root, task['active_attempt_id'])
    before = prepared.read_bytes(), diagnostic.read_bytes()
    config = grant_config()
    configure_runtime_policy(root, config)
    bridge = FreshReplayBridge('fields')  # Idle UI still on previous query; next query prepares exact scope.
    result = replay_authorized_unknown(root, bridge, config)
    assert result['performed'] and result['state'] == 'completed_task'
    assert before == (prepared.read_bytes(), diagnostic.read_bytes())
    audit = json.loads(next((root/'operator_replays').glob('*.json')).read_text())
    assert audit['automatic_retry'] and audit['unstaged_script_preparation_replay_explicitly_authorized']


@pytest.mark.parametrize('case', ['disabled', 'no_stage', 'unfinished', 'running', 'other_unknown', 'wrong_scope', 'saved_raw'])
def test_grant_is_not_a_reset_all_or_missing_evidence_escape(standing, case):
    root, task, prepared, config = standing
    bridge = FreshReplayBridge()
    if case == 'disabled':
        config = {}
    elif case == 'no_stage':
        (root/'raw'/(prepared.name+'.stage.json')).unlink()
    elif case == 'wrong_scope':
        original = json.loads(prepared.read_text()); original['start'] = '1900-01-01'
        atomic_write_json(prepared, original)
    elif case == 'saved_raw':
        atomic_write_json(root/'raw'/prepared.name, {'original_response': 'must reconcile'})
    else:
        with closing(connect(root)) as con, con:
            if case == 'unfinished':
                con.execute('UPDATE desktop_attempts SET finished_at_utc=NULL')
            elif case == 'running':
                con.execute("UPDATE tasks SET state='running' WHERE task_id=?", (task['task_id'],))
            else:
                con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) "
                            "VALUES('other','other','download','P1',100,'{}','blocked','unknown_outcome_no_auto_retry')")
    result = replay_authorized_unknown(root, bridge, config)
    assert result is None or not result['performed']
    assert bridge.calls == []
    with closing(connect(root)) as con:
        assert con.execute('SELECT count(*) FROM desktop_replays').fetchone()[0] == 0


@pytest.mark.parametrize('mutation', ['vendor_notices_absent', 'source_binding_stable', 'source_binding_unchanged',
    'fields', 'axes', 'dates', 'button', 'credentials_read', 'market_data_query_submitted'])
def test_idle_owned_fresh_exact_context_required_even_with_authority(standing, mutation):
    root, _, _, config = standing
    bridge = FreshReplayBridge(mutation)
    first = replay_authorized_unknown(root, bridge, config)
    assert not first['performed'] and first['state'] == 'authorized_replay_context_unverified'
    second = replay_authorized_unknown(root, bridge, config)
    assert not second['performed']
    assert bridge.calls == ['inspect_query_runtime']  # Durable backoff survives a new helper invocation.
    with closing(connect(root)) as con:
        assert con.execute('SELECT count(*) FROM desktop_replays').fetchone()[0] == 0


def test_stale_readback_does_not_license_standing_replay(standing):
    root, _, _, config = standing
    bridge = FreshReplayBridge(stale=True)
    assert replay_authorized_unknown(root, bridge, config)['state'] == 'authorized_replay_context_unverified'
    assert bridge.calls == ['inspect_query_runtime']


def test_snapshot_replay_does_not_require_or_invent_a_historical_date_axis(standing):
    from downloader.tej_history import task_request, PREVIEW_SUBMISSION_CONTRACT
    from test_tej_snapshot import snapshot_export, snapshot_proof
    from downloader.tej_key_layout import KEY1_CONTRACT
    root, task, prepared, config = standing
    request = task_request(root, task)
    request.update(source_key_mode=1, key_layout_contract=KEY1_CONTRACT, date_labels=[], frequency='snapshot')
    with closing(connect(root)) as con, con:
        con.execute('UPDATE tasks SET request_json=?,expected_rows=2,work_expected_rows=2 WHERE task_id=?',
                    (json.dumps(request), task['task_id']))
    atomic_write_json(prepared, {**request, 'task_id': task['task_id'], 'query_attempt_id': task['active_attempt_id']})
    path = root/'raw'/(prepared.name+'.stage.json')
    stage = json.loads(path.read_text()); stage.update(date_labels=[], source_scope_proof=snapshot_proof())
    atomic_write_json(path, stage)
    class SnapshotReplay(FreshReplayBridge):
        def execute(self, root, task):
            req = json.loads(task['request_json'])
            if req['action'] == 'inspect_query_runtime':
                payload, output, seconds = super().execute(root, task)
                payload.update(source_key_mode=1, date_group_enabled=False, date_input_controls=[])
                atomic_write_json(output, payload)
                return payload, output, seconds
            self.calls.append(req['action'])
            task, attempt, _ = _activate(root, task, req)
            stage_path = root/'raw'/(attempt+'.json.stage.json')
            new_stage = json.loads(stage_path.read_text()); new_stage['source_scope_proof'] = snapshot_proof()
            atomic_write_json(stage_path, new_stage)
            payload = {**snapshot_export(), 'task_id': task['task_id'], 'query_attempt_id': attempt,
                       'observed_at_utc': datetime.now(UTC).isoformat(), 'fresh_preview_transition_verified': True,
                       'preview_submission_contract': PREVIEW_SUBMISSION_CONTRACT}
            output = root/'raw'/(attempt+'.json')
            atomic_write_json(output, payload)
            return payload, output, 1.0
    bridge = SnapshotReplay()
    result = replay_authorized_unknown(root, bridge, config)
    assert result['performed'] and result['state'] == 'completed_task'
    assert bridge.calls == ['inspect_query_runtime', 'download']
    receipt = json.loads((root/'receipts'/(task['task_id']+'.json')).read_text())
    assert receipt['historical_values_reconstructed'] is False
    assert receipt['first_query_period'] is receipt['last_query_period'] is None


def test_revocation_is_rechecked_at_once_only_consumption_boundary(standing):
    root, task, prepared, config = standing
    bridge = FreshReplayBridge(revoke=True)
    with pytest.raises(ValueError, match='revoked'):
        retry_unknown_download(root, task['task_id'], bridge, prepared,
                               standing_authorization=authorized_replay_policy(config))
    assert bridge.calls == ['inspect_query_runtime']
    with closing(connect(root)) as con:
        assert con.execute('SELECT consumed_at_utc FROM desktop_replays').fetchone()[0] is None
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task['task_id'],)).fetchone()[0] == 'blocked'


def add_consumed(con, task_id, index, stamp):
    # Budget-only fixture: an immutable production audit is tested above.
    con.execute('INSERT INTO desktop_replays VALUES(?,?,?,?,?,?,?)',
                (f'{index:032x}', task_id, stamp.isoformat(), 'fixture', 'fixture', stamp.isoformat(), 'unknown_outcome_no_auto_retry'))


@pytest.mark.parametrize('case', ['recent_finish', 'one_consumed_manual', 'task_budget', 'global_budget', 'expired_budget'])
def test_indexed_rolling_budget_and_cooldown_do_not_reset_on_restart(standing, case):
    root, task, _, config = standing
    now = datetime.now(UTC); policy = authorized_replay_policy(config)
    with closing(connect(root)) as con, con:
        if case == 'recent_finish':
            con.execute('UPDATE desktop_attempts SET finished_at_utc=? WHERE attempt_id=?',
                        (now.isoformat(), task['active_attempt_id']))
        elif case == 'one_consumed_manual':
            add_consumed(con, task['task_id'], 1, now-timedelta(seconds=10))
        else:
            for i in range(6 if case == 'global_budget' else 2):
                add_consumed(con, 'other' if case == 'global_budget' else task['task_id'], i,
                             now-timedelta(minutes=61 if case == 'expired_budget' else 10+i))
        first = authorized_replay_window(con, task, policy, now)
    with closing(connect(root)) as con:
        second = authorized_replay_window(con, task, policy, now)
    assert first == second
    assert first['allowed'] == (case == 'expired_budget')
    if case != 'expired_budget':
        bridge = FreshReplayBridge()
        result = replay_authorized_unknown(root, bridge, config)
        assert not result['performed'] and bridge.calls == []
        assert datetime.fromisoformat(result['next_check_at_utc']) > now


@pytest.mark.parametrize('key,value', [('enabled', 'yes'), ('contract', 'unreviewed'),
    ('authorization_basis', 'implied'), ('authorized_at_utc', '2026-10-04'),
    ('authorized_at_utc', '2999-01-01T00:00:00+00:00'), ('cooldown_base_seconds', True),
    ('cooldown_base_seconds', 0), ('cooldown_max_seconds', 3601), ('max_replays_per_task_hour', 7),
    ('max_replays_per_hour', 0)])
def test_invalid_grant_rejected_before_source_operations(key, value):
    config = grant_config(); config['automation']['authorized_unknown_replay'][key] = value
    with pytest.raises(ValueError):
        WatchPolicy.from_config(config)


def test_watch_counts_one_authorized_replay_once_not_two_queries_in_one_cycle(standing, monkeypatch):
    root, _, _, config = standing
    monkeypatch.setattr('downloader.tej_scheduler.recover_retained_desktop_response', lambda *_, **__: False)
    bridge = FreshReplayBridge()
    def no_second_query(*_):
        pytest.fail('Authorized replay already executed this cycle')
    watch_queue(root, bridge, config, runner=no_second_query, max_cycles=1, stop=InstantWait(), emit=emit)
    status = json.loads((root/'scheduler_status.json').read_text())
    assert status['cycles'] == status['completed_tasks'] == 1
    assert status['authorized_unknown_replay']['enabled'] is True
    assert status['unknown_outcome_auto_retry'] is False  # Unconditional retry is still off.
    assert bridge.calls == ['inspect_query_runtime', 'download']


def test_watch_recovers_original_response_before_considering_authorized_resubmission(standing):
    from downloader.tej_scheduler import RESPONSE_RECOVERY_CONTRACT
    root, _, _, config = standing
    config['automation']['response_recovery_contract'] = RESPONSE_RECOVERY_CONTRACT
    stop = Event()
    def no_source(*_):
        stop.set()
        return 'idle'
    bridge = RetainedBridge()
    watch_queue(root, bridge, config, stop=stop, runner=no_source, emit=emit)
    assert bridge.calls == ['inspect_notices', 'recover_preview']
    with closing(connect(root)) as con:
        assert con.execute('SELECT count(*) FROM desktop_replays').fetchone()[0] == 0


def test_due_authorized_replay_rechecks_a_late_result_even_during_readback_backoff(standing):
    from downloader.tej_scheduler import RESPONSE_RECOVERY_CONTRACT
    root, task, _, config = standing
    config['automation']['response_recovery_contract'] = RESPONSE_RECOVERY_CONTRACT
    now = datetime.now(UTC)
    atomic_write_json(root/'desktop_response_recovery.json', {
        'contract': RESPONSE_RECOVERY_CONTRACT, 'query_attempt_id': task['active_attempt_id'],
        'observed_at_utc': (now-timedelta(seconds=61)).isoformat(), 'failures': 2,
        'next_check_at_utc': (now+timedelta(seconds=239)).isoformat()})
    bridge = RetainedBridge()
    result = replay_authorized_unknown(root, bridge, config)
    assert result['state'] == 'original_response_recovered' and not result['performed']
    assert bridge.calls == ['inspect_notices', 'recover_preview']
    with closing(connect(root)) as con:
        assert con.execute('SELECT count(*) FROM desktop_replays').fetchone()[0] == 0


def test_public_projection_exposes_limits_not_grant_paths_or_private_evidence(standing):
    from stockagent.live.tej_dashboard import _scheduler_public, _automatic_execution_state
    root, _, _, config = standing
    now = datetime.now(UTC)
    ticks = Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ', 1)[1].split()[19]
    policy = {**authorized_replay_policy(config), 'private_path': 'SECRET'}
    atomic_write_json(root/'scheduler_status.json', {
        'contract': 'persistent_serial_evidence_preserving_supervision_v1', 'continuous': True,
        'state': 'replaying_authorized', 'observed_at_utc': now.isoformat(),
        'deadline_at_utc': (now+timedelta(seconds=1800)).isoformat(),
        'owner_pid': os.getpid(), 'owner_start_ticks': ticks, 'authorized_unknown_replay': policy})
    atomic_write_json(root/'authorized_replay_status.json', {
        'contract': AUTHORIZED_REPLAY_CONTRACT, 'state': 'authorized_replay_task_budget',
        'observed_at_utc': now.isoformat(), 'task_replays_in_hour': 2,
        'next_check_at_utc': (now+timedelta(minutes=30)).isoformat(), 'private_path': 'SECRET'})
    public = _scheduler_public(root, now)
    assert public['alive'] and 'SECRET' not in json.dumps(public)
    assert public['authorized_unknown_replay']['state'] == 'authorized_replay_task_budget'
    assert public['authorized_unknown_replay']['max_replays_per_task_hour'] == 2
    assert public['authorized_unknown_replay']['unconditional_retry'] is False
    assert _automatic_execution_state('replaying_authorized') == 'automatic_waiting_recovery'
