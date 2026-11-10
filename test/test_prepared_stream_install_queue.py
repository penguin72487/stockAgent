import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_prepared_stream_install_queue as queue
from stockagent.data_sync import packed_snapshots as packed, stream_repack, temporary_e_authority
from stockagent.data_sync.desync_snapshots import SnapshotError


def seed(workspace, name, size, *, state='retrying', sha=None):
    sha = sha or ('a' if name == 'first' else 'b')*64
    stage = workspace/'migration/stream-source-repack-v1'/name/sha
    stage.mkdir(parents=True)
    package = stage/('pack-'+'c'*64+'.zip')
    inventory = stage/('inventory-'+'d'*64+'.jsonl.gz')
    package.write_bytes(b'x'*size)
    inventory.write_bytes(b'x')
    prepared = {'contract': queue.stream_repack.CONTRACT+'.prepared', 'dataset': name,
        'old_manifest_sha256': sha, 'destination_independent_readback': False,
        'source_member_sha_streamed': True, 'raw_extraction_tree_created': False,
        'packages': [{'bytes': size, 'sha256': 'c'*64, 'name': package.name,
                      'E_signature': queue.stream_repack._signature(package)}],
        'inventory': {'bytes': 1, 'sha256': 'd'*64, 'name': inventory.name,
                      'E_signature': queue.stream_repack._signature(inventory)}}
    prepared['identity_sha256'] = queue.stream_repack._identity(prepared)
    (stage/'prepared.json').write_text(json.dumps(prepared))
    return {'dataset': name, 'expected_manifest_sha256': sha, 'origin': 'cold',
            'state': state, 'E_prepared': True}


def upstream(workspace, rows):
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True, exist_ok=True)
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.q.CONTRACT+'.status', 'entries': rows}))
    return root


def test_prepared_install_is_independent_and_uses_the_exact_sealed_generation(tmp_path):
    row = seed(tmp_path, 'first', 20)
    root = upstream(tmp_path, [row])
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'first'
        assert command[command.index('--expected-manifest-sha256')+1] == row['expected_manifest_sha256']
        assert command[command.index('--progress-file')+1].endswith('/prepared-install-first.progress.json')
        on_heartbeat()
        directory = root/'first'
        directory.mkdir()
        (directory/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'first',
            'old_manifest_sha256': row['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'completed'


def test_foreign_head_wait_does_not_block_an_independent_prepared_install(tmp_path):
    first = seed(tmp_path, 'first', 10)
    first.update(reason='repack_foreign_head_wait', retry_after_epoch=1300)
    second = seed(tmp_path, 'second', 20)
    root = upstream(tmp_path, [first, second])
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'second'
        (root/'second').mkdir()
        (root/'second'/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'second',
            'old_manifest_sha256': second['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                          runner=runner, now=lambda: 1000, code_sha='test')
    by_name = {row['dataset']: row for row in rows}
    assert by_name['first']['state'] == 'waiting_head_transition'
    assert by_name['second']['state'] == 'completed'


def test_foreign_head_wait_exposes_sealed_E_without_deleting_or_switching_D(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.stream_cli, '_handoff_timer_active', lambda _bucket: False)
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'data.txt').write_bytes(b'fixed historical data')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    def wait_for_head(*_a):
        raise SnapshotError('foreign head wait')
    with pytest.raises(SnapshotError, match='foreign head wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_install=wait_for_head)
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'state': 'retrying', 'E_prepared': True,
           'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300}
    upstream(workspace, [row])
    guards = []
    values = queue.run_once(workspace, cold, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
        runner=lambda *_a, **_kw: pytest.fail('foreign head must not launch a D writer'),
        now=lambda: 1000, code_sha='test', enable_relief=True, guard=lambda: guards.append(True))
    assert values[0]['state'] == 'waiting_head_transition'
    assert values[0]['E_temporary_authority_ready'] and guards
    with temporary_e_authority.borrow(stage, 'fixture', old.manifest_sha256) as view:
        assert view.manifest['source'] == old.manifest['source']
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert old.manifest_path.is_file()
    # A second scheduled cycle reuses the same E view rather than recopying.
    again = queue.run_once(workspace, cold, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
        runner=lambda *_a, **_kw: pytest.fail('foreign head must not launch a D writer'),
        now=lambda: 1001, code_sha='test', enable_relief=True)
    assert again[0]['E_temporary_authority_ready'] is True


def test_foreign_head_wait_resumes_interrupted_E_view_publication(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.stream_cli, '_handoff_timer_active', lambda _bucket: False)
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'data.txt').write_bytes(b'fixed historical data')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    def wait_for_head(*_a):
        raise SnapshotError('foreign head wait')
    with pytest.raises(SnapshotError, match='foreign head wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_install=wait_for_head)
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    (stage/temporary_e_authority.MARKER).write_text(json.dumps({
        'contract': temporary_e_authority.CONTRACT, 'state': 'preparing', 'dataset': 'fixture',
        'old_manifest_sha256': old.manifest_sha256, 'prepared_sha256': prepared['identity_sha256'],
        'created_ns': 1_000_000_000, 'local_only': True, 'D_retirement_authorized': False}))
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'state': 'retrying', 'E_prepared': True,
           'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300}
    upstream(workspace, [row])
    values = queue.run_once(workspace, cold, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
        runner=lambda *_a, **_kw: pytest.fail('foreign head must not launch a D writer'),
        now=lambda: 1000, code_sha='test', enable_relief=True)
    assert values[0]['E_temporary_authority_ready'] is True
    with temporary_e_authority.borrow(stage, 'fixture', old.manifest_sha256) as view:
        assert view.manifest['source'] == old.manifest['source']
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_lab203_head_wait_keeps_exact_owner_and_capacity_during_upstream_cooldown(tmp_path, monkeypatch):
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir(); (source/'sample').write_bytes(b'exact source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='lab203')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'state': 'retrying', 'E_prepared': True,
           'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300}
    upstream(workspace, [row])
    result, = queue.run_once(workspace, cold, free_bytes=0, enable_relief=True,
        runner=lambda *_a, **_kw: pytest.fail('another publisher must not be retired locally'),
        now=lambda: 1000, code_sha='test')
    assert result['state'] == 'waiting_head_transition' and result['relief_head_owner'] == 'lab203'
    assert result['E_temporary_authority_ready'] and result['D_shortfall_bytes'] > 0
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_enrolled_vast_handoff_wakes_sealed_successor_without_waiting_for_old_cooldown(tmp_path, monkeypatch):
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'data.txt').write_bytes(b'fixed source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue.stream_cli, '_handoff_timer_active', lambda _bucket: True)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='previous wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('previous wait')))
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'state': 'retrying', 'E_prepared': True,
           'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300}
    root = upstream(workspace, [row])
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'fixture'
        on_heartbeat()
        (root/'fixture').mkdir()
        (root/'fixture'/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
            'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(workspace, cold, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+1024**2,
                          runner=runner, now=lambda: 1000, code_sha='test')
    assert rows[0]['state'] == 'completed'
    assert old.head_path.exists() and old.manifest_path.exists()


def test_install_selects_new_generation_without_installing_the_blocked_old_one(tmp_path):
    old = seed(tmp_path, 'first', 10, state='blocked_mismatch', sha='a'*64)
    old['reason'] = 'source_head_advanced'
    new = seed(tmp_path, 'first', 20, sha='b'*64)
    root = upstream(tmp_path, [old, new])
    def runner(command, on_heartbeat):
        assert command[command.index('--expected-manifest-sha256')+1] == 'b'*64
        (root/'first').mkdir()
        (root/'first'/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'first',
            'old_manifest_sha256': 'b'*64, 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                          runner=runner, now=lambda: 1000, code_sha='test')
    assert len(rows) == 1 and rows[0]['expected_manifest_sha256'] == 'b'*64
    assert rows[0]['state'] == 'completed'
    assert (tmp_path/'migration/stream-source-repack-v1/first'/('a'*64)/'prepared.json').is_file()


def test_foreign_head_wait_never_delays_an_e_only_recovery(tmp_path, monkeypatch):
    row = seed(tmp_path, 'first', 20)
    row.update(reason='repack_foreign_head_wait', retry_after_epoch=1300)
    root = upstream(tmp_path, [row])
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations',
                        lambda _workspace: {('first', row['expected_manifest_sha256'])})
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'first'
        (root/'first').mkdir()
        (root/'first'/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'first',
            'old_manifest_sha256': row['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    rows = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                          runner=runner, now=lambda: 1000, code_sha='test')
    assert rows[0]['state'] == 'completed'


def test_committed_head_cleanup_restarts_even_after_some_E_packs_were_removed(tmp_path, monkeypatch):
    workspace, cold, source = tmp_path/'workspace', tmp_path/'cold', tmp_path/'source'
    source.mkdir()
    (source/'sample').write_bytes(b'fixed original content')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='penguin')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    published = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': published['new_manifest_sha256'],
               'new_snapshot_id': published['snapshot_id']}
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    blocked = stage/prepared['inventory']['name']
    unlink = Path.unlink
    def fail_inventory(path, *args, **kwargs):
        if path == blocked:
            raise OSError('interrupted cleanup')
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', fail_inventory)
    with pytest.raises(OSError, match='interrupted cleanup'):
        stream_repack.cleanup_stage(cold, stage, receipt)
    monkeypatch.setattr(Path, 'unlink', unlink)
    assert not (stage/prepared['packages'][0]['name']).exists()
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'state': 'retrying', 'E_prepared': True}
    root = upstream(workspace, [row])
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'fixture'
        assert stream_repack.cleanup_stage(cold, stage, receipt)['state'] == 'completed'
        directory = root/'fixture'
        directory.mkdir()
        (directory/(published['new_manifest_sha256']+'.json')).write_text(json.dumps(receipt))
        return SimpleNamespace(returncode=0, stderr='')
    value, = queue.run_once(workspace, cold, free_bytes=0, runner=runner, now=lambda: 1000, code_sha='test')
    assert value['state'] == 'completed' and value['recover_committed_head'] is True


def test_private_prepared_install_stays_private_and_uses_its_own_authority(tmp_path):
    name = 'local-tw-index-derivatives-ticks'
    row = seed(tmp_path, name, 20)
    row['origin'] = 'private_cold'
    root = upstream(tmp_path, [row])
    def runner(command, on_heartbeat):
        assert '--private-source' in command
        assert command[command.index('--dataset')+1] == name
        on_heartbeat()
        directory = root/name
        directory.mkdir()
        (directory/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1', 'private': True,
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': name,
            'old_manifest_sha256': row['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'completed' and values[0]['origin'] == 'private_cold'


def test_private_capacity_shortage_schedules_original_publisher_relief(tmp_path, monkeypatch):
    name = 'local-tw-index-derivatives-ticks'
    row = seed(tmp_path, name, 20)
    row['origin'] = 'private_cold'
    upstream(tmp_path, [row])
    monkeypatch.setattr(queue.p, 'resolve_latest_packed',
        lambda *_a, **_kw: SimpleNamespace(manifest_sha256=row['expected_manifest_sha256'],
            manifest={'publisher': {'node_id': 'private-owner'}, 'archive': {'objects': [{'bytes': 100}]}}))
    monkeypatch.setattr(queue, 'ensure_local_E_view', lambda *_a, **_kw: None)
    monkeypatch.setattr(queue.stream_cli, '_source_head_nodes',
                        lambda *_a: {'private-owner': row['expected_manifest_sha256']})
    calls = []
    def runner(command, on_heartbeat):
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'contract': queue.staged_head_capacity_relief.CONTRACT, 'dataset': name,
            'state': 'completed', 'private': True}))
    result, = queue.run_once(tmp_path, tmp_path, free_bytes=0, enable_relief=True,
                             runner=runner, now=lambda: 1000, code_sha='test')
    assert result['state'] == 'relief_completed' and result['origin'] == 'private_cold'
    assert calls[0][1].endswith('run_staged_head_capacity_relief.py')
    assert '--private-source' in calls[0] and '--apply' in calls[0]


def test_capacity_and_active_owner_are_distinct_and_do_not_start_a_child(tmp_path):
    rows = [seed(tmp_path, 'first', 20), seed(tmp_path, 'second', 30, state='running')]
    upstream(tmp_path, rows)
    def no_child(*_args, **_kwargs):
        raise AssertionError('capacity or existing owner must defer')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+10,
                            runner=no_child, now=lambda: 1000, code_sha='test')
    assert [row['state'] for row in values] == ['waiting_capacity', 'waiting_original_owner']
    assert values[0]['D_shortfall_bytes'] == 11


def test_capacity_wait_exposes_an_exact_local_E_view_without_launching_a_D_writer(tmp_path, monkeypatch):
    row = seed(tmp_path, 'first', 20)
    upstream(tmp_path, [row])
    # This fixture has no real D catalog; provide the fixed D head that the
    # production capacity branch requires before it exposes an E view.
    monkeypatch.setattr(queue.p, 'resolve_latest_packed',
        lambda *_args, **_kwargs: SimpleNamespace(manifest_sha256=row['expected_manifest_sha256'],
                                                   manifest={'archive': {'objects': []}}))
    seen = []
    def expose(cold, stage, name, expected, **_kwargs):
        seen.append((cold, stage, name, expected))
        return {'state': 'ready'}
    monkeypatch.setattr(queue.temporary_e_authority, 'expose', expose)
    def no_child(*_args, **_kwargs):
        raise AssertionError('D capacity wait cannot launch a writer')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+10,
                            runner=no_child, now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'waiting_capacity' and values[0]['E_temporary_authority_ready'] is True
    assert seen[0][2:] == ('first', row['expected_manifest_sha256'])


def test_insufficient_exclusive_relief_is_recorded_and_wakes_when_capacity_improves(tmp_path, monkeypatch):
    row = seed(tmp_path, 'first', 20)
    upstream(tmp_path, [row])
    monkeypatch.setattr(queue.p, 'resolve_latest_packed',
        lambda *_a, **_kw: SimpleNamespace(manifest_sha256=row['expected_manifest_sha256'],
                                            manifest={'archive': {'objects': [{'bytes': 21}]}}))
    monkeypatch.setattr(queue.stream_cli, '_source_head_nodes',
                        lambda *_a: {'penguin': row['expected_manifest_sha256']})
    monkeypatch.setattr(queue, 'ensure_local_E_view', lambda *_a, **_kw: None)
    reserve = queue.stream_cli.D_INSTALL_RESERVE
    exclusive = 8*1024**3
    calls = []
    def runner(command, on_heartbeat):
        calls.append(command)
        state = 'insufficient_exclusive_space' if len(calls) == 1 else 'completed'
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'contract': queue.staged_head_capacity_relief.CONTRACT, 'dataset': 'first', 'state': state,
            'exclusive_allocated_bytes': exclusive,
            'D_shortfall_bytes': reserve + 21 + queue.staged_head_capacity_relief.HEADROOM}))
    first, = queue.run_once(tmp_path, tmp_path, free_bytes=0, now=lambda: 1000,
                            runner=runner, code_sha='test', enable_relief=True)
    assert first['error'] == 'insufficient_exclusive_space' and first['retry_after_epoch'] == 1300
    assert first['D_exclusive_allocated_bytes'] == exclusive and first['relief_observed_at_epoch'] == 1000
    second, = queue.run_once(tmp_path, tmp_path, free_bytes=0, now=lambda: 1001,
                             runner=runner, code_sha='test', enable_relief=True)
    assert second['state'] == 'retrying' and second['E_temporary_authority_ready']
    assert second['D_exclusive_allocated_bytes'] == exclusive and len(calls) == 1
    shortage = exclusive - queue.staged_head_capacity_relief.HEADROOM
    free = reserve + 21 - shortage
    third, = queue.run_once(tmp_path, tmp_path, free_bytes=free, now=lambda: 1002,
                            runner=runner, code_sha='test', enable_relief=True)
    assert third['state'] == 'relief_completed' and len(calls) == 2  # No five-minute wait.
    assert calls[-1][1].endswith('run_staged_head_capacity_relief.py')


def test_relief_capacity_wakeup_rejects_unknown_or_malformed_evidence():
    eligible = queue.relief_capacity_may_now_fit
    assert eligible(0, {'error': 'insufficient_exclusive_space'})
    assert not eligible(0, {'error': 'owner_busy'})
    assert not eligible(1, {'error': 'insufficient_exclusive_space', 'D_exclusive_allocated_bytes': True})
    assert not eligible(1, {'error': 'insufficient_exclusive_space', 'D_exclusive_allocated_bytes': -1})


def test_capacity_relief_foreign_owner_waits_without_starting_a_guaranteed_failure(tmp_path, monkeypatch):
    row = seed(tmp_path, 'first', 20)
    upstream(tmp_path, [row])
    monkeypatch.setattr(queue.p, 'resolve_latest_packed',
        lambda *_args, **_kwargs: SimpleNamespace(manifest_sha256=row['expected_manifest_sha256'],
                                                   manifest={'archive': {'objects': []}}))
    monkeypatch.setattr(queue.temporary_e_authority, 'expose', lambda *_a, **_kw: {'state': 'ready'})
    monkeypatch.setattr(queue.stream_cli, '_source_head_nodes',
                        lambda *_a: {'lab203': row['expected_manifest_sha256']})
    result = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+10,
                            runner=lambda *_a, **_kw: pytest.fail('foreign owner cannot be retired locally'),
                            now=lambda: 1000, code_sha='test', enable_relief=True)
    assert result[0]['state'] == 'waiting_relief_head_owner'
    assert result[0]['relief_head_owner'] == 'lab203'
    assert result[0]['E_temporary_authority_ready'] is True
    assert result[0]['D_shortfall_bytes'] == 11


def test_capacity_wait_reuses_ready_E_view_even_while_unrelated_D_owner_is_busy(tmp_path, monkeypatch):
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'sample').write_bytes(b'an exact source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='lab203')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_a: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    temporary_e_authority.expose(cold, stage, 'fixture', old.manifest_sha256)
    upstream(workspace, [{'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
                           'origin': 'cold', 'state': 'retrying', 'E_prepared': True}])
    with packed._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock'):
        rows = queue.run_once(workspace, cold, free_bytes=0,
            runner=lambda *_a, **_kw: pytest.fail('foreign owner cannot be retired locally'),
            now=lambda: 1000, code_sha='test', enable_relief=True)
    assert rows[0]['state'] == 'waiting_relief_head_owner'
    assert rows[0]['relief_head_owner'] == 'lab203' and rows[0]['E_temporary_authority_ready']
    assert 'E_temporary_authority_error' not in rows[0]


def test_capacity_wait_does_not_expose_E_when_D_head_changed(tmp_path, monkeypatch):
    row = seed(tmp_path, 'first', 20)
    upstream(tmp_path, [row])
    monkeypatch.setattr(queue.p, 'resolve_latest_packed',
        lambda *_args, **_kwargs: SimpleNamespace(manifest_sha256='f'*64,
                                                   manifest={'archive': {'objects': []}}))
    monkeypatch.setattr(queue.temporary_e_authority, 'expose',
        lambda *_args: pytest.fail('a changed D head must not expose the stale E view'))
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+10,
                            runner=lambda *_a, **_kw: pytest.fail('must not launch a writer'),
                            now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'waiting_capacity'
    assert values[0]['E_temporary_authority_error'] == 'SnapshotError'


def test_capacity_shortage_still_allows_owned_partial_reconciliation(tmp_path):
    row = seed(tmp_path, 'first', 20)
    upstream(tmp_path, [row])
    stage = tmp_path/'migration/stream-source-repack-v1'/'first'/row['expected_manifest_sha256']
    prepared = json.loads((stage/'prepared.json').read_text())
    (stage/'install.json').write_text(json.dumps({
        'contract': queue.stream_repack.CONTRACT+'.install', 'dataset': 'first',
        'old_manifest_sha256': row['expected_manifest_sha256'],
        'prepared_sha256': prepared['identity_sha256'], 'installed': [],
        'pending': {'sha256': 'c'*64, 'target_absent_before': True,
                    'incoming': {'name': 'bound'}}}))
    called = []
    def runner(command, on_heartbeat):
        called.append(command)
        return SimpleNamespace(returncode=1, stderr='D needs space')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+10,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert len(called) == 1 and values[0]['interrupted_incoming_reconciliation'] is True
    assert values[0]['state'] == 'retrying' and values[0]['D_shortfall_bytes'] == 11


def test_fixed_public_generation_borrows_only_its_bounded_backup_reserve(tmp_path):
    cli = queue.stream_cli
    name = 'binance-public-archive'
    sha = '5288d22ed33fc707497278c7ce218b56b67a35b98277b7cab69542f01445cf35'
    reserve = 12 * 1024**3
    assert cli.install_reserve_bytes(name, sha, private=False) == reserve
    assert cli.install_reserve_bytes(name, sha, private=True) == cli.D_INSTALL_RESERVE
    assert cli.install_reserve_bytes(name, 'f'*64, private=False) == cli.D_INSTALL_RESERVE
    private_name = 'local-tw-index-derivatives-ticks'
    private_sha = '5e015fa56b850933a460a020095e837d8953e8307f9824afe1709a43fdcc0a21'
    assert cli.install_reserve_bytes(private_name, private_sha, private=True) == reserve
    assert cli.install_reserve_bytes(private_name, private_sha, private=False) == cli.D_INSTALL_RESERVE
    assert cli.install_reserve_bytes(private_name, 'f'*64, private=True) == cli.D_INSTALL_RESERVE
    row = seed(tmp_path, name, 20, sha=sha)
    upstream(tmp_path, [row])
    ran = []
    def runner(command, on_heartbeat):
        ran.append(command)
        return SimpleNamespace(returncode=1, stderr='D reserve reached')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=reserve+21,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert len(ran) == 1 and values[0]['D_reserve_bytes'] == reserve
    assert values[0]['D_shortfall_bytes'] == 0 and values[0]['state'] == 'retrying'


def test_E_only_binance_generation_has_a_narrow_eight_GiB_floor():
    name = 'binance'
    sha = 'ce0f8747ca4da7b6f18184711a702d2a67e0fd81549f4384fc86e56aa77e28b8'
    assert queue.stream_cli.install_reserve_bytes(name, sha, private=False) == 8*1024**3
    assert queue.stream_cli.install_reserve_bytes(name, sha, private=True) == queue.stream_cli.D_INSTALL_RESERVE
    assert queue.stream_cli.install_reserve_bytes(name, 'f'*64, private=False) == queue.stream_cli.D_INSTALL_RESERVE


def test_a_busy_dataset_does_not_block_an_independent_ready_one(tmp_path):
    rows = [seed(tmp_path, 'first', 20, state='running'), seed(tmp_path, 'second', 30)]
    root = upstream(tmp_path, rows)
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'second'
        on_heartbeat()
        directory = root/'second'
        directory.mkdir()
        (directory/('e'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'second',
            'old_manifest_sha256': rows[1]['expected_manifest_sha256'], 'new_manifest_sha256': 'e'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert [row['state'] for row in values] == ['waiting_original_owner', 'completed']


def test_invalid_candidate_does_not_block_an_independent_ready_dataset(tmp_path):
    valid = seed(tmp_path, 'first', 20)
    root = upstream(tmp_path, [
        {'dataset': '../invalid', 'expected_manifest_sha256': 'bad',
         'origin': 'cold', 'state': 'retrying', 'E_prepared': True}, valid])
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT, 'implementation_sha256': 'test',
        'entries': [{'dataset': [], 'expected_manifest_sha256': {}}]}))

    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'first'
        on_heartbeat()
        directory = root/'first'
        directory.mkdir()
        (directory/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'first',
            'old_manifest_sha256': valid['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')

    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert [row['state'] for row in values] == ['invalid_candidate', 'completed']
    assert '../invalid' not in json.dumps(values)


def test_sealed_generation_is_discovered_after_unflagged_owner_contention(tmp_path):
    row = seed(tmp_path, 'first', 20)
    row.pop('E_prepared')
    row['reason'] = 'd_e_owner_busy'
    root = upstream(tmp_path, [row])
    def runner(command, on_heartbeat):
        assert command[command.index('--dataset')+1] == 'first'
        on_heartbeat()
        directory = root/'first'
        directory.mkdir()
        (directory/('d'*64+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'first',
            'old_manifest_sha256': row['expected_manifest_sha256'], 'new_manifest_sha256': 'd'*64}))
        return SimpleNamespace(returncode=0, stderr='')
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'completed'


@pytest.mark.parametrize('lock_name', ['source-local-retirement-turn.lock', 'source-local-retirement-intent.lock'])
def test_prepared_install_retries_a_retirement_turn_without_losing_stage(tmp_path, lock_name):
    row = seed(tmp_path, 'first', 20)
    root = upstream(tmp_path, [row])
    def runner(_command, on_heartbeat):
        on_heartbeat()
        return SimpleNamespace(returncode=1,
            stderr='PackedPublicationBusy: publication owner busy: '+lock_name)
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=runner, now=lambda: 1000, code_sha='test')
    assert values[0]['state'] == 'retrying'
    assert values[0]['error'] == 'retirement_turn' and values[0]['retry_after_epoch'] == 1030
    assert (tmp_path/'migration/stream-source-repack-v1'/'first'/row['expected_manifest_sha256']/'prepared.json').is_file()


def test_blocked_integrity_mismatch_is_never_discovered_as_ready(tmp_path):
    row = seed(tmp_path, 'first', 20, state='blocked_mismatch')
    upstream(tmp_path, [row])
    values = queue.run_once(tmp_path, tmp_path, free_bytes=queue.stream_cli.D_INSTALL_RESERVE+100,
                            runner=lambda *_a, **_kw: (_ for _ in ()).throw(AssertionError('must not run')),
                            now=lambda: 1000, code_sha='test')
    assert values == []
