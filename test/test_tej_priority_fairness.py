"""Finite provider work shares the original TEJ owner without starving refreshes."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.provider_priority import CONTRACT, register
from downloader.tej_history import (_ready_task, _record_incremental_priority_claim, _record_finite_priority_claim,
                                    configure_runtime_policy, connect, run_one)
from test_tej_history import FakeBridge, registry as registry

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
POLICY = {'finite_query_after_incremental_attempts': 4}


def add(con, task_id, table, priority, *, kind='download'):
    con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,scope_contract) "
                "VALUES(?,?,?,'P1',?,'{}','editable_source_scope_v1')", (task_id, table, kind, priority))


def test_native_priority_gets_one_turn_after_four_committed_incremental_claims(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        for i in range(8):
            add(con, f'due-{i}', f'due-{i}', -1000)
        for i in range(2):
            add(con, f'finite-{i}', f'finite-{i}', -72)
        chosen = []
        for _ in range(10):
            # Merely inspecting the queue must not advance the streak.
            row = _ready_task(con, NOW.isoformat(), None, POLICY)
            assert _ready_task(con, NOW.isoformat(), None, POLICY)['task_id'] == row['task_id']
            chosen.append(row['task_id'])
            _record_incremental_priority_claim(con, dict(row))
            con.execute("UPDATE tasks SET state='complete' WHERE task_id=?", (row['task_id'],))
        assert chosen == ['due-0', 'due-1', 'due-2', 'due-3', 'finite-0',
                          'due-4', 'due-5', 'due-6', 'due-7', 'finite-1']
    with closing(connect(tmp_path)) as con:
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_incremental_attempt_streak'").fetchone()[0] == '0'


def test_generic_request_rotation_respects_expiry_and_original_due_priority(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'due', 'a', -1000)
        add(con, 'requested', 'b', 200)
    request = {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite research read',
               'created_at_utc': NOW.isoformat(), 'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
               'items': ['requested']}
    register(tmp_path, request, now=NOW)
    with closing(connect(tmp_path)) as con, con:
        con.execute("INSERT INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'due'
        assert _ready_task(con, NOW.isoformat(), None, POLICY)['task_id'] == 'requested'
        assert _ready_task(con, (NOW + timedelta(hours=3)).isoformat(), None, POLICY)['task_id'] == 'due'


def test_requested_tej_tasks_rotate_by_oldest_native_attempt_without_weakening_gates(tmp_path):
    """A safely requeued early ID must not repeatedly hide the rest of a request."""
    with closing(connect(tmp_path)) as con, con:
        for name in ('a', 'b', 'c'):
            add(con, name, name, 200, kind='discover')
    register(tmp_path, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite metadata',
                        'created_at_utc': NOW.isoformat(),
                        'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
                        'items': ['a', 'b', 'c']}, now=NOW)
    with closing(connect(tmp_path)) as con, con:
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'a'
        # Only the native owner records a committed claim. A known-unsent
        # failure may be requeued, but selection alone changes nothing.
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='a'", (NOW.isoformat(),))
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'b'
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'b'
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='b'",
                    ((NOW + timedelta(seconds=1)).isoformat(),))
        con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE task_id='c'",
                    ((NOW + timedelta(hours=1)).isoformat(),))
        assert _ready_task(con, NOW.isoformat(), None, {})['task_id'] == 'a'
        assert _ready_task(con, (NOW + timedelta(hours=1)).isoformat(), None, {})['task_id'] == 'c'


def test_requested_tej_rotation_preserves_older_request_precedence(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'old', 'a', 200, kind='discover')
        add(con, 'new', 'b', 200, kind='discover')
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='old'", (NOW.isoformat(),))
    for item, offset in [('old', 0), ('new', 1)]:
        created = NOW + timedelta(seconds=offset)
        register(tmp_path, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite metadata',
                            'created_at_utc': created.isoformat(),
                            'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
                            'items': [item]}, now=created)
    with closing(connect(tmp_path)) as con:
        assert _ready_task(con, (NOW + timedelta(seconds=2)).isoformat(), None, {})['task_id'] == 'old'


def test_finite_retry_rotation_prevents_cross_request_starvation_and_reserves_retry(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'old', 'old-table', 201, kind='discover')
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='old'", (NOW.isoformat(),))
        for i in range(5):
            add(con, f'new-{i}', f'new-table-{i}', -30)
        add(con, 'due', 'incremental-table', -1000)
    for items, offset in [(['old'], 0), ([f'new-{i}' for i in range(5)], 1)]:
        created = NOW + timedelta(seconds=offset)
        register(tmp_path, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite source scope',
                            'created_at_utc': created.isoformat(),
                            'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
                            'items': items}, now=created)
    policy = {**POLICY, 'finite_retry_after_fresh_attempts': 4}
    now = (NOW + timedelta(seconds=2)).isoformat()
    with closing(connect(tmp_path)) as con, con:
        chosen = []
        for _ in range(5):
            # Four due incremental claims still precede each finite turn.
            con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
            row = _ready_task(con, now, None, policy)
            assert _ready_task(con, now, None, policy)['task_id'] == row['task_id']
            chosen.append(row['task_id'])
            _record_finite_priority_claim(con, dict(row), now, policy)
            if row['task_id'] != 'old':
                con.execute("UPDATE tasks SET state='complete' WHERE task_id=?", (row['task_id'],))
        assert chosen == ['new-0', 'new-1', 'new-2', 'new-3', 'old']
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_finite_fresh_claim_streak'").fetchone()[0] == '0'
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_incremental_attempt_streak','0')")
        assert _ready_task(con, now, None, policy)['task_id'] == 'due'


def test_finite_retry_rotation_preserves_gates_and_only_counts_real_registered_claims(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'old', 'old-table', 200, kind='discover')
        add(con, 'fresh', 'fresh-table', -30)
        add(con, 'unregistered', 'other-table', -30)
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='old'", (NOW.isoformat(),))
    for item, offset in [('old', 0), ('fresh', 1)]:
        created = NOW + timedelta(seconds=offset)
        register(tmp_path, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite source scope',
                            'created_at_utc': created.isoformat(),
                            'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
                            'items': [item]}, now=created)
    policy = {'finite_retry_after_fresh_attempts': 4}
    now = (NOW + timedelta(seconds=2)).isoformat()
    with closing(connect(tmp_path)) as con, con:
        assert _ready_task(con, now, None, policy)['task_id'] == 'fresh'
        # Merely inspecting the selector or claiming unrelated work changes no counter.
        _record_finite_priority_claim(con, dict(con.execute("SELECT * FROM tasks WHERE task_id='unregistered'").fetchone()), now, policy)
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_finite_fresh_claim_streak'").fetchone() is None
        con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE task_id='fresh'", ((NOW + timedelta(hours=1)).isoformat(),))
        assert _ready_task(con, now, None, policy)['task_id'] == 'old'
        con.execute("UPDATE tasks SET next_attempt_at_utc=NULL WHERE task_id='fresh'")
        con.execute("INSERT INTO desktop_retry_windows VALUES('fresh-table','other','attempt',1,?,'source_input_failure','audit')",
                    ((NOW + timedelta(hours=1)).isoformat(),))
        assert _ready_task(con, now, None, policy)['task_id'] == 'old'
        # Once both priority intents expire, native priority selection resumes.
        assert _ready_task(con, (NOW + timedelta(hours=3)).isoformat(), None, policy)['task_id'] == 'unregistered'


def test_finite_retry_lane_rotates_across_requests_by_real_attempt_time(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        for name in ('old', 'new', 'fresh'):
            add(con, name, name + '-table', 200, kind='discover')
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='old'", ((NOW + timedelta(minutes=10)).isoformat(),))
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='new'", ((NOW + timedelta(minutes=1)).isoformat(),))
    for item, offset in [('fresh', 0), ('old', 1), ('new', 2)]:
        created = NOW + timedelta(seconds=offset)
        register(tmp_path, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'finite source scope',
                            'created_at_utc': created.isoformat(),
                            'expires_at_utc': (NOW + timedelta(hours=2)).isoformat(),
                            'items': [item]}, now=created)
    policy = {'finite_retry_after_fresh_attempts': 4}
    now = (NOW + timedelta(minutes=11)).isoformat()
    with closing(connect(tmp_path)) as con, con:
        assert _ready_task(con, now, None, policy)['task_id'] == 'fresh'
        # The reserved retry turn also applies if the oldest request is fresh.
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_finite_fresh_claim_streak','4')")
        assert _ready_task(con, now, None, policy)['task_id'] == 'new'
        task = dict(con.execute("SELECT * FROM tasks WHERE task_id='new'").fetchone())
        _record_finite_priority_claim(con, task, now, policy)
        con.execute("UPDATE tasks SET attempted_at_utc=? WHERE task_id='new'", (now,))
        con.execute("UPDATE tasks SET state='complete' WHERE task_id='fresh'")
        assert _ready_task(con, now, None, policy)['task_id'] == 'old'


@pytest.mark.parametrize('value', [True, 0, -1, 101, 1.5, '4'])
def test_finite_retry_rotation_requires_explicit_bounded_policy(tmp_path, value):
    with pytest.raises(ValueError, match='retry rotation'):
        configure_runtime_policy(tmp_path, {'finite_retry_after_fresh_attempts': value})
    assert configure_runtime_policy(tmp_path, {})['finite_retry_after_fresh_attempts'] is None


def test_finite_retry_counter_is_written_by_the_original_claim_lifecycle(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **POLICY, 'finite_retry_after_fresh_attempts': 4})
    now = datetime.now(UTC)
    with closing(connect(root)) as con:
        target = con.execute("SELECT task_id FROM tasks WHERE kind='discover'").fetchone()[0]
    register(root, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'bounded source scope',
                    'created_at_utc': now.isoformat(), 'expires_at_utc': (now + timedelta(hours=1)).isoformat(),
                    'items': [target]}, now=now)
    assert run_one(root, FakeBridge()) == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (target,)).fetchone()[0] == 'complete'
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_finite_fresh_claim_streak'").fetchone()[0] == '1'


def test_finite_retry_policy_works_before_any_priority_intent_exists(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **POLICY, 'finite_retry_after_fresh_attempts': 4})
    assert run_one(root, FakeBridge()) == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_finite_fresh_claim_streak'").fetchone() is None


@pytest.mark.parametrize('barrier', ['quarantine', 'retry_window', 'not_due'])
def test_rotation_never_bypasses_native_source_barriers(tmp_path, barrier):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'due', 'a', -1000)
        add(con, 'finite', 'b', -72)
        con.execute("INSERT INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
        if barrier == 'quarantine':
            add(con, 'blocked', 'b', 100)
            con.execute("UPDATE tasks SET state='blocked',last_error_code='source_validation_failed_deferred' WHERE task_id='blocked'")
        elif barrier == 'retry_window':
            con.execute("INSERT INTO desktop_retry_windows VALUES('b','other','attempt',1,?,'source_input_failure','audit')",
                        ((NOW + timedelta(hours=1)).isoformat(),))
        else:
            con.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE task_id='finite'", ((NOW + timedelta(hours=1)).isoformat(),))
        assert _ready_task(con, NOW.isoformat(), None, POLICY)['task_id'] == 'due'
        assert _ready_task(con, NOW.isoformat(), 'download', POLICY, 'b') is None


def test_rotation_respects_requested_kind_and_table(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        add(con, 'due', 'a', -1000, kind='discover')
        add(con, 'finite', 'b', -72)
        con.execute("INSERT INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
        assert _ready_task(con, NOW.isoformat(), 'discover', POLICY)['task_id'] == 'due'
        assert _ready_task(con, NOW.isoformat(), 'download', POLICY, 'b')['task_id'] == 'finite'
        assert _ready_task(con, NOW.isoformat(), 'download', POLICY, 'a') is None


@pytest.mark.parametrize('value', [True, 0, -1, 101, 1.5, '4'])
def test_rotation_requires_explicit_bounded_policy(tmp_path, value):
    with pytest.raises(ValueError, match='bounded'):
        configure_runtime_policy(tmp_path, {'finite_query_after_incremental_attempts': value})
    assert configure_runtime_policy(tmp_path, {})['finite_query_after_incremental_attempts'] is None


def test_native_claim_updates_counter_in_same_real_lifecycle(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **POLICY})
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET priority=-1000 WHERE kind='discover'")
    assert run_one(root, FakeBridge()) == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_incremental_attempt_streak'").fetchone()[0] == '1'
        assert con.execute("SELECT state FROM tasks WHERE kind='discover'").fetchone()[0] == 'complete'


def test_finite_discovery_obeys_global_rotation_and_never_runs_other_work(registry):
    _, root, _, config = registry
    configure_runtime_policy(root, {**config, **POLICY})
    now = datetime.now(UTC)
    with closing(connect(root)) as con, con:
        target = con.execute("SELECT task_id FROM tasks WHERE kind='discover'").fetchone()[0]
        add(con, 'protected-release', 'other-table', -1000)
    receipt = register(root, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'bounded metadata',
                             'created_at_utc': now.isoformat(),
                             'expires_at_utc': (now + timedelta(hours=1)).isoformat(),
                             'items': [target]}, now=now)

    class NoCalls:
        def execute(self, *_args):
            raise AssertionError('No unrelated provider work may run')

    request_id = receipt['request_id']
    assert run_one(root, NoCalls(), finite_discovery_request_id=request_id) == 'finite_priority_not_ready'
    with closing(connect(root)) as con, con:
        assert con.execute("SELECT state FROM tasks WHERE task_id='protected-release'").fetchone()[0] == 'pending'
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (target,)).fetchone()[0] == 'pending'
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
    assert run_one(root, FakeBridge(), finite_discovery_request_id=request_id) == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id='protected-release'").fetchone()[0] == 'pending'
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (target,)).fetchone()[0] == 'complete'
        assert con.execute("SELECT value FROM meta WHERE key='scheduler_incremental_attempt_streak'").fetchone()[0] == '0'


def test_finite_discovery_rejects_expired_request_and_data_download(registry):
    _, root, _, _ = registry
    current = datetime.now(UTC)
    past = current - timedelta(hours=2)
    with closing(connect(root)) as con:
        target = con.execute("SELECT task_id FROM tasks WHERE kind='discover'").fetchone()[0]
    expired = register(root, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'expired metadata',
                              'created_at_utc': past.isoformat(),
                              'expires_at_utc': (past + timedelta(hours=1)).isoformat(),
                              'items': [target]}, now=past)

    class NoCalls:
        def execute(self, *_args):
            raise AssertionError('Expired or non-discovery work must not contact the provider')

    assert run_one(root, NoCalls(), finite_discovery_request_id=expired['request_id']) == 'finite_priority_not_ready'
    assert run_one(root, FakeBridge()) == 'completed_task'
    with closing(connect(root)) as con:
        download = con.execute("SELECT task_id FROM tasks WHERE kind='download' AND state='pending'").fetchone()[0]
    live = register(root, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'data download',
                           'created_at_utc': current.isoformat(),
                           'expires_at_utc': (current + timedelta(hours=1)).isoformat(),
                           'items': [download]}, now=current)
    assert run_one(root, NoCalls(), finite_discovery_request_id=live['request_id']) == 'finite_priority_not_ready'
    with pytest.raises(ValueError, match='global native selector'):
        run_one(root, NoCalls(), kind='discover', finite_discovery_request_id=live['request_id'])


def test_finite_discovery_cli_uses_native_owner_and_global_gate(registry, monkeypatch):
    from downloader import download_tej_history as cli
    from stockagent.data_sync import cache_capacity

    tmp, root, _, config = registry
    config_path, session_path = tmp / 'runtime.json', tmp / 'session.json'
    config_path.write_text(json.dumps({**config, **POLICY}))
    session_path.write_text('{}')
    monkeypatch.setattr(cli, 'DesktopBridge', lambda *_args: FakeBridge())
    monkeypatch.setattr(cache_capacity, 'acquisition_dispatch_status', lambda _root: {'state': 'admitted'})
    now = datetime.now(UTC)
    with closing(connect(root)) as con, con:
        target = con.execute("SELECT task_id FROM tasks WHERE kind='discover'").fetchone()[0]
        add(con, 'protected-release', 'other-table', -1000)
    receipt = register(root, {'contract': CONTRACT, 'provider': 'tej', 'reason': 'bounded metadata',
                             'created_at_utc': now.isoformat(),
                             'expires_at_utc': (now + timedelta(hours=1)).isoformat(),
                             'items': [target]}, now=now)
    argv = ['run', '--root', str(root), '--config', str(config_path), '--session', str(session_path),
            '--finite-discovery-request-id', receipt['request_id']]
    assert cli.main(argv) == 75
    with closing(connect(root)) as con, con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (target,)).fetchone()[0] == 'pending'
        con.execute("INSERT OR REPLACE INTO meta VALUES('scheduler_incremental_attempt_streak','4')")
    assert cli.main(argv) == 0
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (target,)).fetchone()[0] == 'complete'
        assert con.execute("SELECT state FROM tasks WHERE task_id='protected-release'").fetchone()[0] == 'pending'

    with pytest.raises(SystemExit):
        cli.main(argv + ['--discover-only'])


@pytest.mark.parametrize('state', ['deferred', 'not_enrolled', 'unknown'])
def test_finite_discovery_cli_requires_measured_capacity_before_desktop(registry, monkeypatch, state):
    from downloader import download_tej_history as cli
    from stockagent.data_sync import cache_capacity

    _, root, _, _ = registry
    monkeypatch.setattr(cache_capacity, 'acquisition_dispatch_status', lambda _root: {'state': state})
    monkeypatch.setattr(cli, 'DesktopBridge', lambda *_args: pytest.fail('desktop opened before admission'))
    assert cli.main(['run', '--root', str(root), '--finite-discovery-request-id', 'a' * 64]) == 75
