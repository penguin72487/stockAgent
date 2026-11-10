import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import transport_placement as placement
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_stale_bind_detection_deduplicates_namespaces_and_ignores_E(tmp_path, monkeypatch):
    from stockagent.data_sync import transport_relocation as relocation
    monkeypatch.setattr(placement, 'CANONICAL', Path('/srv/transport'))
    monkeypatch.setattr(placement, 'D_BACKING', Path('/srv/D/old'))
    old = '21 1 0:171 /old.relocated-to-E-v1 /srv/transport rw - 9p D: rw\n'
    new = '22 1 8:80 /workspace/transport /srv/transport rw - ext4 /dev/sdf rw\n'
    for pid, namespace, mounts in [('1', 'mnt:[1]', old), ('2', 'mnt:[1]', old),
                                    ('3', 'mnt:[3]', new)]:
        p = tmp_path / pid
        (p / 'ns').mkdir(parents=True)
        os.symlink(namespace, p / 'ns/mnt')
        (p / 'mountinfo').write_text(mounts)
    assert relocation.stale_transport_namespaces(tmp_path) == {
        'count': 1, 'reachable': 0, 'inspection_complete': True}
    # A differently named bind of the same D root is also a live reference.
    (tmp_path / '1/mountinfo').write_text(old.replace('/srv/transport', '/elsewhere/old'))
    assert relocation.stale_transport_namespaces(tmp_path)['count'] == 1
    (tmp_path / '1/mountinfo').write_text(new)
    (tmp_path / '2/mountinfo').write_text(new)
    assert relocation.stale_transport_namespaces(tmp_path) == {
        'count': 0, 'reachable': 0, 'inspection_complete': True}


@pytest.mark.parametrize('recovers', [False, True])
def test_namespace_resampling_never_accepts_an_incomplete_pass(monkeypatch, recovers):
    from stockagent.data_sync import transport_relocation as relocation
    calls = []
    def observe():
        calls.append(True)
        return {'count': 0, 'reachable': 0, 'inspection_complete': recovers and len(calls) == 2}
    monkeypatch.setattr(relocation, 'stale_transport_namespaces', observe)
    monkeypatch.setattr(relocation.time, 'sleep', lambda _: None)
    result = relocation.complete_namespace_observation()
    assert result['inspection_complete'] == recovers
    assert len(calls) == (2 if recovers else 3)


def test_legacy_completion_reconciles_without_unlink_or_rewriting_receipt(tmp_path, monkeypatch):
    from stockagent.data_sync import transport_relocation as relocation
    source = tmp_path / 'D' / 'old'
    source.parent.mkdir()
    source.with_name('old.relocated-to-E-v1').mkdir()
    (source.with_name('old.relocated-to-E-v1') / 'payload').write_bytes(b'original')
    monkeypatch.setattr(placement, 'D_BACKING', source)
    monkeypatch.setattr(relocation, 'stale_transport_namespaces',
                        lambda: {'count': 2, 'inspection_complete': True})
    monkeypatch.setattr(relocation, 'syncthing_alias_matches', lambda _: True)
    evidence = tmp_path / 'evidence'
    evidence.mkdir()
    original = {'state': 'completed', 'copy': {'verified': True}}
    (evidence / 'handoff.json').write_text(json.dumps(original))
    result = relocation.previous_completion_reconciliation(evidence, original, tmp_path / 'E')
    assert result['state'] == 'waiting_verified_old_D_retirement'
    assert result['reason_code'] == 'transport_stale_mount_namespace'
    assert result['old_D_candidate_lookups_present'] >= 1
    assert json.loads((evidence / 'handoff.json').read_text()) == original
    assert (source.with_name('old.relocated-to-E-v1') / 'payload').read_bytes() == b'original'
    assert json.loads((evidence / 'reconciliation.json').read_text())['stale_mount_namespaces'] == 2


def test_legacy_completion_cannot_become_proof_even_when_all_lookups_absent(tmp_path, monkeypatch):
    from stockagent.data_sync import transport_relocation as relocation
    monkeypatch.setattr(placement, 'D_BACKING', tmp_path / 'D' / 'uniquely-absent')
    monkeypatch.setattr(relocation, 'stale_transport_namespaces',
                        lambda: {'count': 0, 'inspection_complete': True})
    monkeypatch.setattr(relocation, 'syncthing_alias_matches', lambda _: True)
    value = relocation.previous_completion_reconciliation(tmp_path, {'state': 'completed'}, tmp_path / 'E')
    assert value['state'] == 'waiting_verified_old_D_retirement'
    assert value['reason_code'] == 'transport_retirement_unverified'
    assert 'old_D_copy_retirement_verified' not in value


@pytest.mark.parametrize('stale_count,pending_handoffs,expected_calls,unchanged_hold',
    [(1, 1, 0, False), (0, 1, 0, False), (0, 0, 1, False), (0, 0, 0, True)])
def test_resume_checks_namespaces_and_acquires_original_locks(tmp_path, monkeypatch, stale_count,
                                                             pending_handoffs, expected_calls, unchanged_hold):
    import fcntl
    from stockagent.data_sync import transport_relocation as relocation
    from stockagent.data_sync import transport_relocation_audit as audit
    from stockagent.data_sync import transport_relocation_delta as delta
    from stockagent.data_sync import transport_relocation_retirement as retirement
    from stockagent.data_sync import workspace_volume as volume
    from stockagent.data_sync import transport_namespace_handoff as namespace_handoff
    conf = tmp_path / 'config'
    state, archive, evidence = tmp_path/'state', tmp_path/'archive', tmp_path/'evidence'
    for path in (conf, state, archive, evidence):
        path.mkdir()
    (conf/'backup_stream.json').write_text(json.dumps({'state_root': str(state),
        'immutable_archive_state_root': str(archive),
        'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}))
    (evidence/'handoff.json').write_text(json.dumps({'state': 'completed'}))
    monkeypatch.setattr(placement, 'CONFIG', conf/'backup_transport_volume.json')
    monkeypatch.setattr(placement, 'D_BACKING', tmp_path/'old')
    monkeypatch.setattr(placement, 'check_transport', lambda **kw: None)
    def workspace(kind, *, create=False):
        path = tmp_path/kind
        path.mkdir(exist_ok=True)
        return path
    monkeypatch.setattr(volume, 'path', workspace)
    monkeypatch.setattr(relocation, 'previous_completion_reconciliation', lambda *args: {
        'state': 'waiting_verified_old_D_retirement', 'namespace_inspection_complete': True,
        'stale_mount_namespaces': stale_count, 'syncthing_alias_verified': True})
    monkeypatch.setattr(audit, 'compare_fixed_copy', lambda *args: {'identity_sha256': 'a'*64, 'files_total': 1})
    monkeypatch.setattr(delta, 'preserve_differences', lambda *args, **kw: {'state': 'verified'})
    monkeypatch.setattr(retirement, 'rehearse_delta_restore', lambda *args, **kw: {'state': 'member_restore_verified'})
    monkeypatch.setattr(retirement, 'observe_unchanged_held_copy', lambda *args, **kw:
                        {'state': 'waiting_held_members'} if unchanged_hold else None)
    if unchanged_hold:
        monkeypatch.setattr(relocation, 'sender_boundary', lambda *args, **kw:
                            pytest.fail('unchanged held queue must not wait for a sender owner'))
    def handoff(*args):
        for path in (state/'cycle.lock', state/'owner.lock', archive/'source-replication-owner.lock'):
            with path.open('a') as another_owner:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(another_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return {'waiting': pending_handoffs}
    monkeypatch.setattr(namespace_handoff, 'run', handoff)
    called = []
    def guarded(*args, **kwargs):
        for path in (state/'cycle.lock', state/'owner.lock', archive/'source-replication-owner.lock'):
            with path.open('a') as another_owner:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(another_owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        called.append(True)
        return {'state': 'waiting_held_members'}
    monkeypatch.setattr(retirement, 'retire_fixed_copy', guarded)
    value = relocation.run(evidence)
    assert len(called) == expected_calls
    assert ('retirement' in value) == bool(expected_calls or unchanged_hold)
    assert value.get('held_queue_observation_without_sender_lock', False) == unchanged_hold


def test_transport_E_has_no_D_native_path_but_cold_remains_D(monkeypatch):
    monkeypatch.setattr(placement,'on_e',lambda:True)
    assert native.windows_path(Path('/srv/stockagent-backup-ingress-lab203/.staging/batch/object')) is None
    assert native.windows_path(Path('/srv/stockagent-packed/objects/aa/closed')) == 'D:\\stockagent-cold-primary\\packed\\objects\\aa\\closed'


def test_fixed_partial_wait_rechecks_trading_priority_before_any_mutation(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from stockagent.data_sync import transport_relocation as relocation
    from stockagent.data_sync import transport_relocation_audit as audit
    from stockagent.data_sync import transport_relocation_delta as delta
    from stockagent.data_sync import transport_relocation_retirement as retirement
    from stockagent.data_sync import transport_restore_held_partials as partial
    from stockagent.data_sync import workspace_source_handoff as handoff
    from stockagent.data_sync import workspace_volume as volume
    conf, state, archive, evidence = (tmp_path / x for x in ('config', 'state', 'archive', 'evidence'))
    for path in (conf, state, archive, evidence):
        path.mkdir()
    (conf / 'backup_stream.json').write_text(json.dumps({'state_root': str(state),
        'immutable_archive_state_root': str(archive),
        'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}))
    (evidence / 'handoff.json').write_text(json.dumps({'state': 'completed'}))
    monkeypatch.setattr(placement, 'CONFIG', conf / 'backup_transport_volume.json')
    monkeypatch.setattr(placement, 'D_BACKING', tmp_path / 'old')
    monkeypatch.setattr(placement, 'check_transport', lambda **kw: None)
    monkeypatch.setattr(volume, 'path', lambda kind, **kw: tmp_path)
    monkeypatch.setattr(relocation, 'previous_completion_reconciliation', lambda *args: {
        'state': 'waiting_verified_old_D_retirement', 'namespace_inspection_complete': True,
        'stale_mount_namespaces': 0, 'syncthing_alias_verified': True})
    monkeypatch.setattr(audit, 'compare_fixed_copy', lambda *args: {'identity_sha256': 'a' * 64})
    monkeypatch.setattr(delta, 'preserve_differences', lambda *args, **kw: {'state': 'verified'})
    monkeypatch.setattr(retirement, 'rehearse_delta_restore', lambda *args, **kw: {'state': 'member_restore_verified'})
    monkeypatch.setattr(partial, 'needs_restore', lambda *args: True)
    monkeypatch.setattr(partial, 'restore', lambda *args, **kw: pytest.fail('must defer at trading boundary'))
    decisions = iter((True, False))
    monkeypatch.setattr(handoff, 'binance_handoff_window_allowed', lambda **kw: next(decisions))
    @contextmanager
    def sender(*args, **kwargs):
        assert kwargs['recheck_seconds'] == 600 and kwargs['wait']
        yield ()
    monkeypatch.setattr(relocation, 'sender_boundary', sender)
    result = relocation.run(evidence)
    assert result['partial_name_restore']['state'] == 'waiting_trading_priority'
    assert result['partial_name_restore']['source_files_deleted'] == 0
    assert 'retirement' not in result


def test_queued_handoff_releases_partial_locks_and_rechecks_all_owners(tmp_path, monkeypatch):
    import fcntl
    from stockagent.data_sync import transport_relocation as relocation
    state, archive = tmp_path/'state', tmp_path/'archive'
    state.mkdir()
    archive.mkdir()
    config = {'state_root': str(state), 'immutable_archive_state_root': str(archive)}
    paths = (state/'cycle.lock', state/'owner.lock', archive/'source-replication-owner.lock')
    busy = paths[-1].open('a')
    fcntl.flock(busy, fcntl.LOCK_EX | fcntl.LOCK_NB)
    waited = []
    def release(seconds):
        # A busy final owner must not reserve either of the other writers.
        for path in paths[:-1]:
            with path.open('a') as probe:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        waited.append(seconds)
        busy.close()
    monkeypatch.setattr(relocation.time, 'sleep', release)
    try:
        with relocation.sender_boundary(tmp_path, config, wait=True):
            for path in paths:
                with path.open('a') as probe:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert waited == [0.25]
        assert json.loads((tmp_path/'attempt.json').read_bytes())['reason_code'] == 'original_owner'
    finally:
        busy.close()


@pytest.mark.parametrize('purpose', ['namespace_handoff', 'old_D_transport_retirement'])
def test_sender_wait_returns_to_timer_without_reserving_partial_owners(tmp_path, monkeypatch, purpose):
    import fcntl
    from stockagent.data_sync import transport_relocation as relocation
    state, archive = tmp_path / 'state', tmp_path / 'archive'
    state.mkdir(); archive.mkdir()
    config = {'state_root': str(state), 'immutable_archive_state_root': str(archive)}
    paths = (state / 'cycle.lock', state / 'owner.lock', archive / 'source-replication-owner.lock')
    elapsed = [0.0]
    monkeypatch.setattr(relocation.time, 'monotonic', lambda: elapsed[0])
    monkeypatch.setattr(relocation.time, 'sleep', lambda _: elapsed.__setitem__(0, 61.0))
    with paths[-1].open('a') as busy:
        fcntl.flock(busy, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError, match='reevaluation'):
            with relocation.sender_boundary(tmp_path, config, wait=True, purpose=purpose):
                pytest.fail('busy sender boundary was admitted')
        for path in paths[:-1]:
            with path.open('a') as probe:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
    observation = json.loads((tmp_path / 'attempt.json').read_bytes())
    assert observation['reason_code'] == 'original_owner' and observation['retryable']
    assert observation['purpose'] == purpose


@pytest.mark.parametrize('purpose,expected', [
    ('namespace_handoff', True), ('old_D_transport_retirement', False), (None, True)])
def test_timer_exception_preserves_wait_type_through_backup_admission(tmp_path, purpose, expected):
    from scripts.install_backup_stream import record_placement_wait
    from stockagent.data_sync import backup_stream, transport_relocation
    error = transport_relocation.SenderBoundaryBusy(purpose) if purpose else BlockingIOError('busy')
    result = record_placement_wait(tmp_path, error, now=1000)
    persisted = json.loads((tmp_path / 'attempt.json').read_bytes())
    assert result.get('purpose') == persisted.get('purpose') == purpose
    config = {'transport_namespace_handoff_contract': 'stockagent.transport_namespace_handoff.v1'}
    assert backup_stream.transport_placement_pending(config, evidence=tmp_path, now=1000) is expected
    assert 'private_wait_error' not in result


def test_transport_marker_enrolls_one_exact_E_role(tmp_path,monkeypatch):
    config=tmp_path/'config.json'
    c={'schema_version':2,'authority_node_id':'penguin','purpose':'lab203-backup-transport-only',
       'physical_volume':'E:','backing':'enrolled_ext4_workspace','relative_root':'transport/lab203',
       'independent_disaster_recovery_copy':False}
    config.write_text(json.dumps(c))
    monkeypatch.setattr(placement,'CONFIG',config)
    placement._read.cache_clear()
    assert placement.on_e()
    c['relative_root']='downloads'
    config.write_text(json.dumps(c))
    with pytest.raises(SnapshotError,match='placement'):
        placement.placement()
    placement._read.cache_clear()


@pytest.mark.parametrize('restart', [False, True])
def test_post_placement_rollout_keeps_original_owner_and_runs_once(tmp_path,monkeypatch,restart):
    from types import SimpleNamespace
    from scripts import manage_lakehouse
    from stockagent.data_sync import transport_relocation as relocation
    (tmp_path/'handoff.json').write_text(json.dumps({'state':'completed'}))
    monkeypatch.setattr(manage_lakehouse,'worker_code_identity',lambda:'fixed-E-code')
    monkeypatch.setattr(placement,'check_transport',lambda **kw:None)
    calls=[]
    def run(argv,**kwargs):
        calls.append(argv)
        assert '--reload-code-only' in argv
        assert ('--restart-active-owner' in argv) is restart
        assert argv[argv.index('--deadline-seconds') + 1] == '600'
        assert Path(argv[argv.index('--handoff-status') + 1]).name == 'worker-reload.json'
        waiting = json.loads((tmp_path / 'worker-reload.json').read_bytes())
        assert waiting['state'] == 'waiting_for_original_owner'
        assert waiting['publication_progress_renewed'] is False
        Path(argv[argv.index('--output')+1]).write_text(json.dumps({
            'state':'accepted','loaded_code_identity_sha256':'fixed-E-code',
            'authorized_active_owner_restart': restart,
            'workflow_chains_preserved':True,'postgres_and_temporal_server_not_restarted':True,
            'publication_owner_boundary_verified':True}))
        return SimpleNamespace(returncode=0,stdout=b'',stderr=b'')
    monkeypatch.setattr(relocation.subprocess,'run',run)
    assert relocation.finish_worker_handoff(tmp_path, restart_active_owner=restart)['state']=='accepted'
    assert relocation.finish_worker_handoff(tmp_path, restart_active_owner=restart)['state']=='accepted'
    assert len(calls)==1


def test_busy_rollout_does_not_spend_failure_budget_and_errors_are_bounded(tmp_path,monkeypatch):
    from types import SimpleNamespace
    from scripts import manage_lakehouse
    from stockagent.data_sync import transport_relocation as relocation
    (tmp_path/'handoff.json').write_text(json.dumps({'state':'completed'}))
    monkeypatch.setattr(manage_lakehouse,'worker_code_identity',lambda:'fixed-E-code')
    monkeypatch.setattr(placement,'check_transport',lambda **kw:None)
    response=SimpleNamespace(returncode=1,stdout=b'',stderr=
        b'PublicationOwnerBusy: publication owner has not reached its handoff boundary')
    calls=[]
    def run(*args,**kwargs):
        calls.append(args)
        return response
    monkeypatch.setattr(relocation.subprocess,'run',run)
    assert relocation.finish_worker_handoff(tmp_path)['failures']==0
    response.stderr=b'ValueError: retained validation failure'
    for i in range(1,4):
        assert relocation.finish_worker_handoff(tmp_path)['failures']==i
    assert relocation.finish_worker_handoff(tmp_path)['failures']==3
    assert len(calls)==4
    assert not json.loads((tmp_path/'worker-reload.json').read_bytes())['original_active_owner_forced']


def test_old_accepted_rollout_does_not_mask_a_changed_candidate(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import manage_lakehouse
    from stockagent.data_sync import transport_relocation as relocation
    (tmp_path/'handoff.json').write_text(json.dumps({'state': 'completed'}))
    (tmp_path/'worker-reload.json').write_text(json.dumps({
        'state': 'accepted', 'loaded_code_identity_sha256': 'previous-code', 'failures': 0}))
    monkeypatch.setattr(manage_lakehouse, 'worker_code_identity', lambda: 'current-code')
    monkeypatch.setattr(placement, 'check_transport', lambda **kw: None)
    called = []
    def run(argv, **kwargs):
        called.append(argv)
        return SimpleNamespace(returncode=1, stdout=b'', stderr=
            b'PublicationOwnerBusy: publication owner has not reached its handoff boundary')
    monkeypatch.setattr(relocation.subprocess, 'run', run)
    result = relocation.finish_worker_handoff(tmp_path)
    assert len(called) == 1 and result['state'] == 'waiting_for_original_owner'
    assert result['candidate_code_identity_sha256'] == 'current-code'


@pytest.mark.parametrize('error', [b'TimeoutError: worker never became ready',
                                  b'BlockingIOError: publication body failed'])
def test_worker_admission_errors_are_not_unlimited_owner_waits(tmp_path, monkeypatch, error):
    from types import SimpleNamespace
    from scripts import manage_lakehouse
    from stockagent.data_sync import transport_relocation as relocation
    (tmp_path / 'handoff.json').write_text(json.dumps({'state': 'completed'}))
    monkeypatch.setattr(manage_lakehouse, 'worker_code_identity', lambda: 'current-code')
    monkeypatch.setattr(placement, 'check_transport', lambda **kw: None)
    monkeypatch.setattr(relocation.subprocess, 'run', lambda *a, **kw:
                        SimpleNamespace(returncode=1, stdout=b'', stderr=error))
    result = relocation.finish_worker_handoff(tmp_path)
    assert result['state'] == 'worker_reload_failed' and result['failures'] == 1


@pytest.mark.parametrize('pending', [False, True])
def test_ready_E_worker_handoff_precedes_unrelated_sender_gc_wait(tmp_path, monkeypatch, pending):
    from stockagent.data_sync import transport_relocation as relocation
    (tmp_path / 'handoff.json').write_text(json.dumps({'state': 'completed'}))
    monkeypatch.setattr(placement, 'check_transport', lambda **kw: None)
    monkeypatch.setattr(relocation, 'stale_transport_namespaces', lambda: {
        'inspection_complete': True, 'count': 0, 'reachable': 0})
    monkeypatch.setattr(relocation, 'syncthing_alias_matches', lambda root: True)
    monkeypatch.setattr(relocation.volume, 'path', lambda *a, **kw: tmp_path)
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    plans = tmp_path / 'namespace-handoff' / boot
    plans.mkdir(parents=True)
    (plans / '123.json').write_text(json.dumps({'phase': 'old_mount_removed' if pending else 'alias_verified'}))
    calls = []
    def handoff(root):
        calls.append('reload')
        return {'state': 'waiting_for_original_owner'}
    def retire(root):
        calls.append('retirement')
        return {'state': 'waiting'}
    monkeypatch.setattr(relocation, 'finish_worker_handoff', handoff)
    monkeypatch.setattr(relocation, 'run', retire)
    result = relocation.advance(tmp_path)
    assert calls == (['retirement'] if pending else ['reload'])
    assert result['state'] == ('waiting' if pending else 'waiting_worker_handoff')
