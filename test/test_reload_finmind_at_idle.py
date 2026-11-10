from datetime import UTC, datetime, timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from downloader import download_finmind_complement as worker
from scripts import reload_finmind_at_idle as reload


def preparation_threads(*extra):
    return [{'thread_name': 'MainThread', 'frames': [
        {'name': '_populate', 'filename': '/repo/downloader/download_finmind_complement.py'},
        {'name': 'run_once', 'filename': '/repo/downloader/download_finmind_complement.py'},
        *({'name': name} for name in extra)]}]


def test_preparation_stack_is_not_an_idle_flag_or_live_batch():
    assert reload.preparation_stack(preparation_threads())
    assert not reload.preparation_stack([])
    assert not reload.preparation_stack(preparation_threads('run_batch'))
    assert not reload.preparation_stack(preparation_threads() + [
        {'thread_name': 'fetch', 'frames': [{'name': '_request_independent'}]}])
    foreign = preparation_threads()
    foreign[0]['frames'][1]['filename'] = '/foreign.py'
    assert not reload.preparation_stack(foreign)


@pytest.mark.parametrize('provider_query', [False, True])
def test_product_query_adoption_requires_pinned_passing_semantics_not_a_speed_claim(tmp_path, provider_query):
    import xml.etree.ElementTree as ET
    required = [
        'test_exact_verification_shape_is_not_hidden_by_dataset_delegation',
        'test_due_refresh_precedes_the_source_check_and_scope_is_exact',
        'test_source_check_does_not_reopen_terminal_empty_or_running_key',
        'test_registered_verification_retains_cooldown',
        'test_changed_plan_cannot_authorize_a_new_query',
        'test_changed_source_pin_rejects_registration_without_task_mutation',
        'test_finite_source_priority_preempts_due_foreign_history_without_changing_tasks',
        'test_finite_priority_obeys_real_release_quota_and_future_reservations',
        'test_canonical_owner_executes_one_exact_check_before_accepted_renewal',
    ]
    if provider_query:
        required += [
            'test_finmind_exact_priority_is_idempotent_finite_and_preserves_due_refreshes',
            'test_finite_read_yields_only_foreign_history_reservations_and_respects_dated_releases',
            'test_unknown_batch_is_rejected_atomically',
            'test_request_registry_never_reopens_completed_or_unknown_tasks',
            'test_api_native_split_retains_finite_priority_and_parent_receipt',
            'test_sponsor_priority_uses_native_release_and_dependency_predicate',
        ]
    suite = ET.Element('testsuite')
    for name in required + [f'other_control_{i}' for i in range(9)]:
        ET.SubElement(suite, 'testcase', name=name)
    path = tmp_path / 'semantic.xml'
    def write():
        ET.ElementTree(suite).write(path)
        return reload.file_sha(path)
    proof = dict(measurement_contract=reload.PRODUCT_VERIFICATION_ACCEPTANCE,
        state='finite_product_query_semantics_accepted', source_only_no_training_injection=True,
        exact_output_parity=True, provider_calls=0, production_queue_mutations=0, source_writes=0,
        semantic_junit_path=str(path), semantic_junit_sha256=write())
    if provider_query:
        proof.update(measurement_contract=reload.PROVIDER_QUERY_ACCEPTANCE,
                     state='provider_priority_semantics_accepted')
    # The production pytest document wraps suites; accept that exact format.
    wrapper = ET.Element('testsuites'); wrapper.append(suite)
    ET.ElementTree(wrapper).write(path); proof['semantic_junit_sha256'] = reload.file_sha(path)
    assert reload.accepted_local_measurement(proof)
    for key, bad in [('source_only_no_training_injection',False), ('provider_calls',1),
                     ('production_queue_mutations',1), ('semantic_junit_sha256','0'*64)]:
        assert not reload.accepted_local_measurement({**proof,key:bad})
    for tag in ('failure','error','skipped'):
        child = ET.SubElement(suite[0],tag)
        proof['semantic_junit_sha256'] = write()
        assert not reload.accepted_local_measurement(proof)
        suite[0].remove(child)
    suite[0].set('name','missing_required_control')
    proof['semantic_junit_sha256'] = write()
    assert not reload.accepted_local_measurement(proof)


def test_store_fence_adoption_requires_full_exact_store_trials_and_unchanged_final_files():
    proof = {'measurement_contract': reload.STORE_MEASUREMENT,
             'state': 'accepted_bounded_store_replay',
             'scope': 'canonical_store_first_publish_and_same_value_refresh_with_full_footer_and_sha_readback',
             'exact_output_parity': True, 'source_parquet_preserved': True, 'code_stable': True,
             'provider_calls': 0, 'production_queue_mutations': 0, 'production_source_writes': 0,
             'records': {'tasks': 48, 'stores_per_trial': 96, 'rows_per_payload': 389},
             'trials': [{'variant': name, 'elapsed_seconds': cost, 'cpu_seconds': cost,
                         'stores': 96, 'exact_existing_final_inode_preserved': True,
                         'output_sha256': 'a' * 64}
                        for name, cost in [('baseline', 2), ('candidate', 1),
                                           ('candidate', 1), ('baseline', 2)] * 2],
             'medians': {name: {'elapsed_seconds': cost, 'cpu_seconds': cost}
                         for name, cost in [('baseline', 2), ('candidate', 1)]}}
    assert reload.accepted_local_measurement(proof)
    for key in ('source_parquet_preserved', 'code_stable', 'exact_output_parity'):
        assert not reload.accepted_local_measurement({**proof, key: False})
    assert not reload.accepted_local_measurement({**proof, 'production_source_writes': 1})
    assert not reload.accepted_local_measurement({**proof, 'trials': proof['trials'][:4]})
    assert not reload.accepted_local_measurement({**proof, 'medians': {}})
    for field, bad in [('output_sha256', 'b' * 64), ('elapsed_seconds', float('nan')),
                       ('stores', 48), ('exact_existing_final_inode_preserved', False)]:
        trials = [{**row} for row in proof['trials']]
        trials[1][field] = bad
        assert not reload.accepted_local_measurement({**proof, 'trials': trials})


def test_parallel_publication_adoption_requires_full_pipeline_parity_and_original_bounds():
    import copy
    expected = {'calls': 48, 'accepted_requests': 48, 'nonempty_requests': 48, 'failed_tasks': 0}
    proof = {'measurement_contract': reload.PUBLICATION_MEASUREMENT,
             'state': 'eight_full_pipeline_trials_verified', 'configuration_sha256': 'a'*64,
             'exact_output_parity': True, 'outputs_identical': True, 'source_preserved': True, 'code_stable': True,
             'provider_calls': 0, 'production_queue_mutations': 0, 'source_writes': 0,
             'tasks_per_pass': 48, 'passes_per_trial': 2, 'status_writes_per_trial': 5, 'source_rows': 240,
             'benchmark_device': 7, 'target_queue_device': 7, 'boot_id': 'test-boot',
             'medians': {'baseline': 2, 'candidate': 1},
             'trials': [{'variant': label, 'wall_seconds': 1 if label=='candidate' else 2, 'cpu_seconds': 1,
                         'output_sha256': 'b'*64, 'passes': [expected.copy(), expected.copy()],
                         'existing_parquet_inodes_preserved': True, 'full_source_readback': True,
                         'queue_writer_thread_verified': True, 'max_response_slots': 16,
                         'same_filesystem_as_target_queue': True,
                         'max_claimed_responses': 16, 'max_publication_workers': 2 if label=='candidate' else 1}
                        for label in ['baseline', 'candidate', 'candidate', 'baseline']*2]}
    assert reload.accepted_local_measurement(proof)
    for field, bad in [('source_preserved', False), ('source_writes', 1), ('code_stable', False),
                       ('source_rows', 0), ('configuration_sha256', None), ('status_writes_per_trial', 0),
                       ('target_queue_device', 8), ('benchmark_device', None), ('boot_id', ''),
                       ('trials', proof['trials'][:4]), ('medians', {'baseline': 4, 'candidate': 1})]:
        assert not reload.accepted_local_measurement({**proof, field: bad})
    for field, bad in [('wall_seconds', float('nan')), ('max_claimed_responses', 17),
                       ('max_publication_workers', 16), ('max_response_slots', 32),
                       ('existing_parquet_inodes_preserved', False), ('queue_writer_thread_verified', False),
                       ('same_filesystem_as_target_queue', False),
                       ('output_sha256', 'c'*64), ('passes', [expected])]:
        broken = copy.deepcopy(proof); broken['trials'][1][field] = bad
        assert not reload.accepted_local_measurement(broken)


def test_reservation_reload_requires_balanced_full_local_cycle_and_exact_parity():
    proof = {'measurement_contract': 'finmind_live_reservation_readers_local_dispatch_v1',
             'state': 'measurement_complete', 'snapshot_preserved': True, 'exact_output_parity': True,
             'code_stable': True, 'status_connection': 'native_StatusConnection_fresh_memo_per_trial',
             'local_dispatch_wall_improvement_observed': True, 'production_queue_mutations': 0,
             'provider_calls': 0, 'medians_seconds': {'baseline': 2.0, 'candidate': 1.0},
             'trials': [{'label': label, 'admissions': 120, 'status_writes': 5,
                         'wall_seconds': 2.0 if label == 'baseline' else 1.0, 'output_sha256': 'a'*64}
                        for label in ('baseline', 'candidate', 'candidate', 'baseline')]}
    assert reload.accepted_local_measurement(proof)
    for key in ('snapshot_preserved', 'exact_output_parity', 'code_stable', 'local_dispatch_wall_improvement_observed'):
        assert not reload.accepted_local_measurement({**proof, key: False})
    assert not reload.accepted_local_measurement({**proof, 'medians_seconds': {'baseline': 3, 'candidate': 1}})
    assert not reload.accepted_local_measurement({**proof, 'provider_calls': 1})
    assert not reload.accepted_local_measurement({**proof, 'trials': proof['trials'][:2]})
    changed = [{**row, 'output_sha256': 'b'*64} if index == 0 else row
               for index, row in enumerate(proof['trials'])]
    assert not reload.accepted_local_measurement({**proof, 'trials': changed})

def test_eta_index_reload_requires_full_eta_and_exact_durable_write_trials():
    proof = {'measurement_contract': 'finmind_full_eta_queue_indexes_v1',
             'state': 'accepted_bounded_replay', 'scope': 'full_offline_finmind_eta_snapshot',
             'exact_full_output_parity': True, 'read_snapshot_preserved': True,
             'production_queue_writes': 0, 'provider_calls': 0,
             'queue_index_setup': {name: {'task_changes': 0} for name in ('sponsor', 'complement')},
             'medians': {name: {'elapsed_seconds': cost, 'cpu_seconds': cost, 'peak_rss_bytes': 100}
                         for name, cost in (('baseline', 2.0), ('candidate', 1.0))},
             'trials': [{'variant': name, 'elapsed_seconds': 2.0 if name == 'baseline' else 1.0,
                         'cpu_seconds': 2.0 if name == 'baseline' else 1.0, 'peak_rss_bytes': 100,
                         'output_sha256': 'a' * 64}
                        for name in ('baseline', 'candidate', 'candidate', 'baseline') * 2],
             'claim_release_trials': [{'variant': name, 'wall_seconds': 0.3, 'cpu_seconds': 0.2,
                                      'tasks': 32, 'transactions': 64, 'exact_task_rows_restored': True}
                                     for name in ('baseline', 'candidate', 'candidate', 'baseline')]}
    assert reload.accepted_local_measurement(proof)
    for key in ('exact_full_output_parity', 'read_snapshot_preserved'):
        assert not reload.accepted_local_measurement({**proof, key: False})
    assert not reload.accepted_local_measurement({**proof, 'production_queue_writes': 1})
    assert not reload.accepted_local_measurement({**proof, 'trials': proof['trials'][:4]})
    assert not reload.accepted_local_measurement({**proof, 'claim_release_trials': []})
    assert not reload.accepted_local_measurement({**proof, 'queue_index_setup': {'complement': {'task_changes': 0}}})
    changed = [dict(row) for row in proof['trials']]
    changed[1]['output_sha256'] = 'b' * 64
    assert not reload.accepted_local_measurement({**proof, 'trials': changed})
    with_wake = {**proof, 'measurement_contract': 'finmind_full_eta_queue_indexes_v2',
                 'scope': 'full_offline_finmind_eta_and_wake_snapshot',
                 'trials': [{**row, 'wake_delays': {'sponsor': 60.0, 'complement': 30.0}}
                            for row in proof['trials']]}
    assert reload.accepted_local_measurement(with_wake)
    assert not reload.accepted_local_measurement({**with_wake, 'trials': proof['trials']})


def test_preparation_guard_rechecks_frozen_claims_and_always_thaws(tmp_path, monkeypatch):
    import sqlite3
    root = tmp_path
    (root / 'status.json').write_text(json.dumps({'last_task': {'rows': 390}}))
    with sqlite3.connect(root / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE finmind_object_prefetch_claims(id)')
        conn.execute('CREATE TABLE complement_year_batch_claims(id)')
        conn.execute('CREATE TABLE tasks(state)')
    calls = []
    monkeypatch.setattr(reload, '_preparation_pid', lambda: 123)
    monkeypatch.setattr(reload, '_stacks', lambda _: preparation_threads())
    monkeypatch.setattr(reload, 'identity', lambda _: {'InvocationID': 'one'})
    monkeypatch.setattr(reload.subprocess, 'run', lambda args, **kw: calls.append(args))
    with pytest.raises(RuntimeError, match='fixture'):
        with reload.preparation_boundary(root, {'InvocationID': 'one'}) as proof:
            assert proof['mode'] == 'frozen_drained_preparation'
            assert set(proof['claims'].values()) == {0}
            raise RuntimeError('fixture')
    assert [args[1] for args in calls] == ['freeze', 'thaw']
    with sqlite3.connect(root / 'queue.sqlite3') as conn:
        conn.execute('INSERT INTO finmind_object_prefetch_claims VALUES (1)')
    calls.clear()
    with reload.preparation_boundary(root, {'InvocationID': 'one'}) as proof:
        assert proof is None
        assert [args[1] for args in calls] == ['freeze', 'thaw']
    with sqlite3.connect(root / 'queue.sqlite3') as conn:
        conn.execute('DELETE FROM finmind_object_prefetch_claims')
        conn.execute("INSERT INTO tasks VALUES ('inflight')")
    calls.clear()
    with reload.preparation_boundary(root, {'InvocationID': 'one'}) as proof:
        assert proof is None
        assert [args[1] for args in calls] == ['freeze', 'thaw']


def test_failed_freeze_is_still_thawed(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(reload, '_preparation_pid', lambda: 123)
    monkeypatch.setattr(reload, '_stacks', lambda _: preparation_threads())
    def run(args, **kw):
        calls.append(args[1])
        if args[1] == 'freeze':
            raise subprocess.TimeoutExpired(args, 5)
    monkeypatch.setattr(reload.subprocess, 'run', run)
    with pytest.raises(subprocess.TimeoutExpired):
        with reload.preparation_boundary(tmp_path, {}):
            pytest.fail('No proof on freeze failure')
    assert calls == ['freeze', 'thaw']


@pytest.mark.parametrize('owner', ['complement', 'sponsor'])
def test_reserve_boundary_requires_frozen_waiter_and_zero_claims(tmp_path, monkeypatch, owner):
    import sqlite3
    stack = [{'thread_name': 'MainThread', 'frames': [
        {'name': 'wait_for_next_cycle', 'filename': '/repo/downloader/finmind_runtime.py'},
        {'name': 'main', 'filename': f'/repo/downloader/download_finmind_{owner}.py'}]}]
    assert reload.reserve_wait_stack(stack, owner)
    assert not reload.reserve_wait_stack(stack, 'free')
    assert not reload.reserve_wait_stack(stack + [
        {'thread_name': 'fetch', 'frames': [{'name': '_fetch'}]}], owner)
    assert not reload.reserve_wait_stack(preparation_threads(), owner)
    stamp = datetime.now(UTC).isoformat()
    status = {'state': 'incremental_reserve', 'observed_at_utc': stamp, 'active_task': None}
    (tmp_path / 'status.json').write_text(json.dumps(status))
    with sqlite3.connect(tmp_path / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE tasks(state)')
        conn.execute('CREATE TABLE finmind_object_prefetch_claims(id)')
    calls = []
    monkeypatch.setattr(reload, '_preparation_pid', lambda *_: 123)
    monkeypatch.setattr(reload, '_stacks', lambda _: stack)
    monkeypatch.setattr(reload, 'identity', lambda _: {'InvocationID': 'one'})
    monkeypatch.setattr(reload, 'idle_heartbeat', lambda *_: {'state': 'waiting'})
    monkeypatch.setattr(reload.subprocess, 'run', lambda args, **kw: calls.append(args))
    unit = f'stockagent-finmind-{owner}.service'
    with reload.reserve_boundary(tmp_path, {'InvocationID': 'one'}, owner) as proof:
        assert proof['mode'] == 'frozen_drained_reserve' and proof['unit'] == unit
        assert not any(proof['claims'].values())
        assert [args[1] for args in calls] == ['freeze']
    assert calls[-1] == ['systemctl', 'thaw', unit]
    for failure in ('task', 'object', 'changed-generation', 'stale-heartbeat'):
        with sqlite3.connect(tmp_path / 'queue.sqlite3') as conn:
            conn.execute('DELETE FROM tasks'); conn.execute('DELETE FROM finmind_object_prefetch_claims')
            if failure == 'task': conn.execute("INSERT INTO tasks VALUES ('inflight')")
            if failure == 'object': conn.execute('INSERT INTO finmind_object_prefetch_claims VALUES (1)')
        monkeypatch.setattr(reload, 'identity', lambda _: {'InvocationID': 'other' if failure == 'changed-generation' else 'one'})
        monkeypatch.setattr(reload, 'idle_heartbeat', lambda *_: {'state': 'unavailable' if failure == 'stale-heartbeat' else 'waiting'})
        calls.clear()
        with reload.reserve_boundary(tmp_path, {'InvocationID': 'one'}, owner) as proof:
            assert proof is None
        assert [args[1] for args in calls] == ['freeze', 'thaw']


@pytest.mark.parametrize('mode,unit', [('frozen_drained_preparation', reload.UNIT),
                                      ('frozen_drained_reserve', 'stockagent-finmind-sponsor.service')])
def test_frozen_termination_pins_process_and_queues_signal_before_thaw(tmp_path, monkeypatch, mode, unit):
    import signal
    calls = []
    monkeypatch.setattr(reload, '_pidfd_open', lambda pid: calls.append(('pin', pid)) or 99)
    monkeypatch.setattr(reload.os, 'close', lambda fd: calls.append(('close', fd)))
    monkeypatch.setattr(reload, '_pidfd_signal', lambda fd, sig: calls.append(('signal', fd, sig)))
    monkeypatch.setattr(reload.subprocess, 'run', lambda args, **kw: calls.append(('systemctl', args[1])))
    monkeypatch.setattr(Path, 'read_text', lambda *_a, **_kw: 'SigIgn:\t0\nSigCgt:\t2\n')
    proof = {'mode': mode, 'unit': unit, 'pid': 123, 'claims': {'object': 0, 'year': 0}}
    reload.terminate_frozen_preparation(proof)
    assert calls == [('pin', 123), ('signal', 99, signal.SIGTERM), ('close', 99), ('systemctl', 'thaw')]
    monkeypatch.setattr(Path, 'read_text', lambda *_a, **_kw: 'SigIgn:\t0\nSigCgt:\t4000\n')
    calls.clear()
    with pytest.raises(ValueError, match='nondefault_sigterm'):
        reload.terminate_frozen_preparation(proof)
    assert calls == [('pin', 123), ('close', 99)]


def test_pidfd_native_or_libc_binding_checks_own_process_without_signalling():
    descriptor = reload._pidfd_open(os.getpid())
    try:
        reload._pidfd_signal(descriptor, 0)  # Existence/permission check only.
    finally:
        os.close(descriptor)


def test_direct_cli_help_works_outside_repository_without_pythonpath(tmp_path):
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run([sys.executable, str(Path(reload.__file__).resolve()), "--help"],
                            cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "--apply" in result.stdout and "No provider requests" in " ".join(result.stdout.split())


def setup_idle(root, now, *, reason="batch_complete", seconds=5):
    root.mkdir()
    with worker._db(root / "queue.sqlite3") as connection:
        worker.object_pipeline.recover_claims(connection)
        connection.commit()
    status = {"state": reason, "observed_at_utc": now.isoformat(), "active_task": None}
    (root / "status.json").write_text(json.dumps(status))
    waiting = {"schema_version": 1, "phase": "waiting", "reason": reason,
               "observed_at_utc": now.isoformat(), "next_check_at_utc": (now + timedelta(seconds=seconds)).isoformat(),
               "cycle_observed_at_utc": now.isoformat()}
    (root / "worker_status.json").write_text(json.dumps(waiting))
    return status, waiting


def test_idle_reload_requires_nonreactive_window_deadline_and_empty_claims(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    root = tmp_path / "owner"
    setup_idle(root, now)
    assert reload.idle_proof(root, now)["reason"] == "batch_complete"
    assert reload.idle_proof(root, now + timedelta(seconds=4)) is None
    with worker._db(root / "queue.sqlite3") as connection:
        connection.execute("INSERT INTO finmind_object_prefetch_claims VALUES ('GoldPrice','','2026','pending')")
        connection.commit()
    assert reload.idle_proof(root, now) is None
    other = tmp_path / "reactive"
    setup_idle(other, now, reason="incremental_reserve", seconds=60)
    assert reload.idle_proof(other, now) is None


def test_idle_disk_guard_requires_explicit_complement_scope_and_no_claims(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    root = tmp_path / 'owner'
    setup_idle(root, now, reason='disk_guard', seconds=60)
    assert reload.idle_proof(root, now) is None
    assert reload.idle_proof(root, now, allow_disk_guard=True)['reason'] == 'disk_guard'
    with worker._db(root / 'queue.sqlite3') as connection:
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES ('GoldPrice','','2026','year',1,'inflight')")
        connection.commit()
    assert reload.idle_proof(root, now, allow_disk_guard=True) is None


def test_capacity_resume_adoption_checks_semantic_cases_and_source_scope(tmp_path):
    import xml.etree.ElementTree as ET
    names = [
        'test_capacity_wait_rechecks_each_minute_and_reuses_sampler',
        'test_capacity_wait_resumes_promptly_after_the_inflight_probe',
        'test_background_startup_keeps_summary_clock_and_does_not_scan',
        'test_background_capacity_wait_never_overrides_a_real_reserve',
        'test_wait_for_refresh_reuses_inflight_probe_and_returns_only_new_result',
        'test_pending_and_expired_samples_fail_closed_with_only_one_probe',
        'test_probe_failure_and_invalid_result_are_not_admission',
        'test_idle_disk_guard_requires_explicit_complement_scope_and_no_claims',
    ]
    suite = ET.Element('testsuite')
    for name in names:
        ET.SubElement(suite, 'testcase', name=name)
    path = tmp_path / 'semantic.xml'
    ET.ElementTree(suite).write(path)
    proof = {'measurement_contract': reload.CAPACITY_RESUME_ACCEPTANCE,
             'state': 'capacity_resume_semantics_accepted',
             'source_only_no_training_injection': True, 'source_state_preserved': True,
             'provider_calls': 0, 'production_queue_mutations': 0, 'source_writes': 0,
             'semantic_junit_path': str(path), 'semantic_junit_sha256': reload.file_sha(path)}
    assert reload.accepted_local_measurement(proof)
    assert not reload.accepted_local_measurement({**proof, 'source_state_preserved': False})
    assert not reload.accepted_local_measurement({**proof, 'provider_calls': 1})
    assert not reload.accepted_local_measurement({**proof, 'semantic_junit_sha256': '0' * 64})
    ET.SubElement(suite[0], 'failure')
    ET.ElementTree(suite).write(path)
    proof['semantic_junit_sha256'] = reload.file_sha(path)
    assert not reload.accepted_local_measurement(proof)


def test_capacity_adoption_readback_allows_bounded_queue_preparation_time():
    assert reload._owner_readback_timeout({'measurement_contract': reload.CAPACITY_RESUME_ACCEPTANCE}) == 300
    assert reload._owner_readback_timeout({'measurement_contract': 'unrelated'}) == 90
    for contract in reload.ETA_MEASUREMENTS:
        assert reload._owner_readback_timeout({'measurement_contract': contract}) == 300


def test_idle_without_claim_table_entry_still_rejects_an_inflight_task(tmp_path):
    now = datetime.now(UTC)
    root = tmp_path / 'sponsor'
    setup_idle(root, now, reason='current_queue', seconds=60)
    with worker._db(root / 'queue.sqlite3') as connection:
        connection.execute("INSERT INTO tasks(dataset,data_id,partition,kind,priority,state) "
                           "VALUES ('GoldPrice','','2026','year',1,'inflight')")
        connection.commit()
    assert reload.idle_proof(root, now) is None


def test_sponsor_reload_is_bound_to_its_measured_owner_and_never_uses_preparation(tmp_path, monkeypatch):
    root = tmp_path / 'data_finmind/sponsor'
    root.parent.mkdir()
    setup_idle(root, datetime.now(UTC), reason='current_queue', seconds=60)
    monkeypatch.setattr(reload, 'REPO', tmp_path)
    monkeypatch.setattr(reload, 'code_matches', lambda _: True)
    monkeypatch.setattr(reload, 'accepted_local_measurement', lambda _: True)
    monkeypatch.setattr(reload, 'identity', lambda _: {'ActiveState': 'active', 'MainPID': '1', 'InvocationID': 'same'})
    monkeypatch.setattr(reload.shutil, 'disk_usage', lambda _: type('Disk', (), {'free': 64 * 2**30})())
    monkeypatch.setattr(reload.subprocess, 'run', lambda *_a, **_k: pytest.fail('dry run dispatched an action'))
    acceptance = tmp_path / 'acceptance.json'
    payload = {'exact_output_parity': True, 'code_stable': True,
               'measurement_contract': 'finmind_full_eta_queue_indexes_v2',
               'source_manifest': {'source': str(root.parent), 'files': {'data/sponsor/queue.sqlite3': 'a' * 64}}}
    acceptance.write_text(json.dumps(payload))
    output = tmp_path / 'sponsor-readback.json'
    argv = ['--owner', 'sponsor', '--acceptance', str(acceptance), '--output', str(output), '--wait-seconds', '0']
    assert reload.main(argv) == 0
    result = json.loads(output.read_bytes())
    assert result['unit'] == 'stockagent-finmind-sponsor.service'
    assert result['state'] == 'idle_observed_no_action'
    assert result['idle_proof']['claims']['inflight_tasks'] == 0
    output.unlink()
    with pytest.raises(SystemExit):
        reload.main(argv + ['--allow-preparation-boundary'])
    payload['source_manifest']['files'].clear()
    acceptance.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match='sponsor_queue_requires_its_full_measured_snapshot'):
        reload.main(argv)


def test_mismatched_cycle_and_active_task_never_authorize_restart(tmp_path):
    now = datetime(2026, 10, 14, tzinfo=UTC)
    root = tmp_path / "owner"
    status, waiting = setup_idle(root, now)
    waiting["cycle_observed_at_utc"] = (now - timedelta(seconds=1)).isoformat()
    (root / "worker_status.json").write_text(json.dumps(waiting))
    assert reload.idle_proof(root, now) is None
    waiting["cycle_observed_at_utc"] = now.isoformat()
    (root / "worker_status.json").write_text(json.dumps(waiting))
    status["active_task"] = {"dataset": "GoldPrice"}
    (root / "status.json").write_text(json.dumps(status))
    assert reload.idle_proof(root, now) is None


@pytest.mark.parametrize("protected", [False, True])
def test_calendar_protected_completed_wait_is_idle_only_while_gate_remains_active(tmp_path, monkeypatch, protected):
    now = datetime(2026, 10, 14, 0, 25, tzinfo=UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="protected_opening", seconds=300)
    observed = []
    def gate(stamp):
        observed.append(stamp)
        return protected
    monkeypatch.setattr(reload, "protected_stock_opening", gate)
    proof = reload.idle_proof(root, now)
    assert bool(proof) == protected
    assert observed == [now]
    if proof:
        assert proof["reason"] == "protected_opening" and not any(proof["claims"].values())
    assert reload.idle_proof(root, now + timedelta(seconds=299)) is None


def test_protected_wait_never_overrides_inflight_claims_or_unknown_live_status(tmp_path, monkeypatch):
    now = datetime(2026, 10, 14, 0, 25, tzinfo=UTC)
    root = tmp_path / "owner"
    status, _waiting = setup_idle(root, now, reason="protected_opening", seconds=300)
    monkeypatch.setattr(reload, "protected_stock_opening", lambda stamp: True)
    status["active_task"] = {"dataset": "UKStockPrice"}
    (root / "status.json").write_text(json.dumps(status))
    assert reload.idle_proof(root, now) is None
    status["active_task"] = None
    (root / "status.json").write_text(json.dumps(status))
    with worker._db(root / "queue.sqlite3") as connection:
        connection.execute("INSERT INTO finmind_object_prefetch_claims VALUES ('GoldPrice','','2026','pending')")
        connection.commit()
    assert reload.idle_proof(root, now) is None


def test_reload_rechecks_the_actual_idle_cycle_after_preservation_readback(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="current_queue", seconds=60)
    monkeypatch.setattr(reload, "ROOT", root)
    monkeypatch.setattr(reload, "code_matches", lambda accepted: True)
    monkeypatch.setattr(reload, "identity", lambda unit: {"ActiveState": "active", "InvocationID": "same", "MainPID": "123"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("not idle anymore")))
    def preservation(root, last):
        (root / "worker_status.json").write_text(json.dumps({"schema_version": 1, "phase": "running"}))
        return []
    monkeypatch.setattr(reload, "prior_files", preservation)
    acceptance = tmp_path / "acceptance.json"
    acceptance.write_text(json.dumps({"exact_output_parity": True, "code_stable": True,
                                     "status_wall_improvement_observed": True, "source_queue": str(root / "queue.sqlite3")}))
    output = tmp_path / "observed.json"
    assert reload.main(["--acceptance", str(acceptance), "--output", str(output), "--apply", "--wait-seconds", "0"]) == 0
    observed = json.loads(output.read_bytes())
    assert observed["state"] == "deferred_generation_or_deadline_changed"
    assert not observed["restart_requested"]


def test_refused_frozen_signal_is_reported_without_claiming_a_restart(tmp_path, monkeypatch):
    from contextlib import contextmanager
    now = datetime.now(UTC)
    root = tmp_path / 'owner'
    setup_idle(root, now, reason='current_queue', seconds=60)
    monkeypatch.setattr(reload, 'ROOT', root)
    monkeypatch.setattr(reload, 'code_matches', lambda _: True)
    monkeypatch.setattr(reload, 'identity', lambda _: {'ActiveState': 'active', 'InvocationID': 'same', 'MainPID': '123'})
    monkeypatch.setattr(reload, 'idle_proof', lambda *_a, **_kw: None)
    monkeypatch.setattr(reload, 'prior_files', lambda *_: [])
    @contextmanager
    def boundary(*_args):
        yield {'mode': 'frozen_drained_preparation', 'claims': {'inflight': 0}, 'last_task': None}
    monkeypatch.setattr(reload, 'preparation_boundary', boundary)
    monkeypatch.setattr(reload, 'terminate_frozen_preparation',
                        lambda _: (_ for _ in ()).throw(ValueError('preparation_owner_has_nondefault_sigterm')))
    monkeypatch.setattr(reload.subprocess, 'run', lambda *_a, **_kw: pytest.fail('systemd restart must not run'))
    acceptance = tmp_path / 'acceptance.json'
    acceptance.write_text(json.dumps({'exact_output_parity': True, 'code_stable': True,
                                      'status_wall_improvement_observed': True, 'source_queue': str(root/'queue.sqlite3')}))
    output = tmp_path / 'observed.json'
    assert reload.main(['--acceptance', str(acceptance), '--output', str(output), '--apply',
                        '--wait-seconds', '0', '--allow-preparation-boundary']) == 0
    result = json.loads(output.read_bytes())
    assert result['state'] == 'deferred_frozen_owner_not_safe_to_stop'
    assert result['restart_requested'] is False and 'restart_at_utc' not in result


def test_observation_default_never_restarts_a_service(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    root = tmp_path / "owner"
    setup_idle(root, now, reason="current_queue", seconds=60)
    monkeypatch.setattr(reload, "ROOT", root)
    monkeypatch.setattr(reload, "code_matches", lambda accepted: True)
    monkeypatch.setattr(reload, "identity", lambda unit: {"ActiveState": "active", "InvocationID": "generation", "MainPID": "123"})
    monkeypatch.setattr(reload.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("restart not authorized")))
    acceptance = tmp_path / "acceptance.json"
    acceptance.write_text(json.dumps({"exact_output_parity": True, "code_stable": True,
                                     "status_wall_improvement_observed": True, "source_queue": str(root / "queue.sqlite3")}))
    output = tmp_path / "observed.json"
    assert reload.main(["--acceptance", str(acceptance), "--output", str(output)]) == 0
    observed = json.loads(output.read_bytes())
    assert observed["state"] == "idle_observed_no_action"
    assert not observed["restart_requested"]
