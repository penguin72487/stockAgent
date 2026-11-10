import fcntl
import hashlib
import json
import os
import subprocess
import threading
from types import SimpleNamespace

import pytest

from scripts import run_source_local_repack_queue as queue
from stockagent.data_sync import storage_progress
from stockagent.data_sync.packed_snapshots import _canonical_json_bytes
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('outcome', [None, 'retrying', 'blocked_mismatch'])
def test_ready_batch_yields_after_idle_or_unsuccessful_attempt(outcome):
    seen = []
    def run(*, on_result):
        seen.append('run')
        if outcome is not None:
            on_result(outcome)
        return 0
    result = queue.run_ready_batch(run=run, monotonic=lambda: 0)
    assert result == {'attempted': 1, 'published': 0, 'reason': 'no_publication', 'exit_code': 0}
    assert seen == ['run']


def test_ready_batch_continues_after_recorded_publication_and_has_a_bound():
    seen = []
    def run(*, on_result):
        seen.append('run')
        on_result('published')
        return 0
    result = queue.run_ready_batch(run=run, guard=lambda: seen.append('guard'), monotonic=lambda: 0)
    assert result == {'attempted': 4, 'published': 4, 'reason': 'batch_limit', 'exit_code': 0}
    assert seen == ['guard', 'run'] * 4


def test_missing_inventory_does_not_stop_unrelated_bounded_work():
    outcomes = iter(['source_inventory_missing', 'published', None])
    def run(*, on_result):
        value = next(outcomes)
        if value:
            on_result(value)
        return 0
    result = queue.run_ready_batch(run=run, monotonic=lambda: 0)
    assert result == {'attempted': 3, 'published': 1, 'reason': 'no_publication', 'exit_code': 0}


def test_foreign_head_staging_allows_the_next_dataset_to_run():
    outcomes = iter(['foreign_head_staged', 'published', None])
    def run(*, on_result):
        value = next(outcomes)
        if value:
            on_result(value)
        return 0
    assert queue.run_ready_batch(run=run, monotonic=lambda: 0) == {
        'attempted': 3, 'published': 1, 'reason': 'no_publication', 'exit_code': 0}


@pytest.mark.parametrize(('observation', 'selected'), [('held', 'other'), ('released', 'fixture')])
def test_sealed_foreign_head_only_reruns_after_transition(tmp_path, monkeypatch, observation, selected):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    first = {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1/fixture'/('a'*64)),
             'files': 2, 'logical_bytes': 100}
    other = {'dataset': 'other', 'expected_manifest_sha256': 'b'*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1/other'/('b'*64)),
             'files': 2, 'logical_bytes': 100}
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *_args: [first, other])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_args: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda *_args: False)
    monkeypatch.setattr(queue, 'foreign_head_wait_observation',
                        lambda row, _prior, _cold: observation if row['dataset'] == 'fixture' else 'not_applicable')
    now = queue.time.time()
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': []}))
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'queue_identity': 'previous',
        'entries': [{**first, 'state': 'retrying', 'reason': 'repack_foreign_head_wait',
                     'E_prepared': True, 'retry_after_epoch': now+3600}]}))
    commands = []
    def busy(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1, stderr='owner busy', stdout='')
    monkeypatch.setattr(queue, 'run_observed', busy)
    assert queue.main() == 0
    assert commands[0][commands[0].index('--dataset')+1] == selected
    entries = json.loads((root/'queue-status.json').read_text())['entries']
    row, = [item for item in entries if item['dataset'] == 'fixture']
    if observation == 'held':
        assert row['reason'] == 'repack_foreign_head_wait' and row['E_prepared'] is True
        assert 59 <= row['retry_after_epoch']-now <= 61


def test_foreign_head_hint_rechecks_fixed_E_generation_and_falls_back_when_unknown(tmp_path, monkeypatch):
    from stockagent.data_sync import packed_snapshots as packed
    source, cold, workspace = tmp_path/'source', tmp_path/'cold', tmp_path/'workspace'
    source.mkdir()
    (source/'data.txt').write_bytes(b'fixed historical data')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kwargs: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    def wait_for_head(*_args):
        raise SnapshotError('foreign head wait')
    with pytest.raises(SnapshotError, match='foreign head wait'):
        queue.stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_install=wait_for_head)
    row = {'dataset': 'fixture', 'expected_manifest_sha256': old.manifest_sha256,
           'origin': 'cold', 'source': str(stage)}
    prior = {'state': 'retrying', 'reason': 'repack_foreign_head_wait', 'E_prepared': True}
    assert queue.foreign_head_wait_observation(row, prior, cold) == 'held'
    head = cold/'heads/fixture/vastai1T.json'
    value = json.loads(head.read_text())
    head.unlink()
    value['node_id'] = 'penguin'
    (head.parent/'penguin.json').write_text(json.dumps(value))
    queue._FOREIGN_HEAD_STILL_HELD.clear()
    assert queue.foreign_head_wait_observation(row, prior, cold) == 'released'
    (stage/'prepared.json').unlink()
    assert queue.foreign_head_wait_observation(row, prior, cold) == 'unknown'


@pytest.mark.parametrize(('message', 'matches'), [
    ('PackedPublicationBusy: publication owner busy: publish-fixture.lock', True),
    ('PackedPublicationBusy: publication owner busy: publish-another.lock', False),
    ('PackedPublicationBusy: publication owner busy: publish-retention-global.lock', False),
    ('PackedPublicationBusy: publication owner busy: publish-fixture.lock.extra', False),
    ('another error: publication owner busy: publish-fixture.lock', False),
    ('PackedPublicationBusy: publication owner busy: publish-retention-global.lock\n'
     'PackedPublicationBusy: publication owner busy: publish-fixture.lock', False),
])
def test_dataset_publication_busy_requires_the_exact_scoped_owner(message, matches):
    assert queue.dataset_publication_owner_busy(message, 'fixture') is matches
    assert not queue.dataset_publication_owner_busy(message, '../fixture')
    assert not queue.dataset_publication_owner_busy(message, 'retention-global')


def test_dataset_owner_busy_allows_the_next_dataset_to_run():
    outcomes = iter(['dataset_owner_busy', 'published', None])
    def run(*, on_result):
        value = next(outcomes)
        if value:
            on_result(value)
        return 0
    assert queue.run_ready_batch(run=run, monotonic=lambda: 0) == {
        'attempted': 3, 'published': 1, 'reason': 'no_publication', 'exit_code': 0}


@pytest.mark.parametrize('shared_lock', [False, True])
def test_real_queue_yields_only_dataset_lock_and_preserves_shared_lock_boundary(tmp_path, monkeypatch, shared_lock):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *args, **kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *args: [])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda _: False)
    rows = [{'dataset': name, 'expected_manifest_sha256': digest*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1'/name/(digest*64)),
             'files': 1, 'logical_bytes': 100} for name, digest in [('first', 'a'), ('second', 'b')]]
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': queue.time.time()+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': rows,
    }))
    published, calls = set(), []
    monkeypatch.setattr(queue, 'prior_published', lambda _root, row: row['dataset'] in published)
    def run(command, **kwargs):
        name = command[command.index('--dataset')+1]
        calls.append(name)
        if name == 'first':
            lock = 'publish-retention-global.lock' if shared_lock else 'publish-first.lock'
            return SimpleNamespace(returncode=1,
                stderr=f'PackedPublicationBusy: publication owner busy: {lock}', stdout='')
        published.add(name)
        return SimpleNamespace(returncode=0, stderr='', stdout='')
    monkeypatch.setattr(queue, 'run_observed', run)

    result = queue.run_ready_batch(run=queue.main, monotonic=lambda: 0, max_items=3)
    assert calls == (['first'] if shared_lock else ['first', 'second'])
    assert result['published'] == (0 if shared_lock else 1)
    entries = json.loads((root/'queue-status.json').read_text())['entries']
    first, = [entry for entry in entries if entry['dataset'] == 'first']
    assert first['reason'] == 'd_e_owner_busy' and first.get('owner_lock') == (
        None if shared_lock else 'dataset_publication')


def test_prepared_e_stage_survives_discovery_rotation_and_preserves_e_only_priority(tmp_path):
    old = {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
           'state': 'retrying', 'reason': 'repack_foreign_head_wait', 'E_prepared': True,
           'E_foreign_head_staged': True, 'files': 2, 'logical_bytes': 100}
    newer = {'dataset': 'fixture', 'expected_manifest_sha256': 'b'*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1/fixture'/('b'*64)),
             'files': 3, 'logical_bytes': 150}
    carried, = queue.carry_prepared_stages([], [old], tmp_path, set())
    assert carried['expected_manifest_sha256'] == 'a'*64
    assert carried['source'] == str(tmp_path/'migration/stream-source-repack-v1/fixture'/('a'*64))
    both = queue.carry_prepared_stages([newer], [old], tmp_path, set())
    assert both == [newer, {**carried, 'superseded_by_manifest_sha256': 'b'*64}]
    # An urgent E-only generation has priority without hiding another sealed
    # generation of the same dataset from the queue.
    assert queue.carry_prepared_stages([newer], [old], tmp_path, {('fixture', 'b'*64)}) == [newer, carried]
    assert queue.carry_prepared_stages([], [{**old, 'state': 'published'}], tmp_path, set()) == []
    assert queue.carry_prepared_stages([], [{**old, 'dataset': '../bad'}], tmp_path, set()) == []
    busy = {**old, 'reason': 'd_e_owner_busy', 'E_foreign_head_staged': False}
    assert queue.carry_prepared_stages([], [busy], tmp_path, set()) == [carried]
    planned = {**old, 'E_prepared': False, 'E_dedup_planned': True, 'reason': 'source_duplicate_bloat'}
    assert queue.carry_prepared_stages([], [planned], tmp_path, set()) == [carried]
    interrupted = {**old, 'state': 'running', 'E_prepared': False}
    interrupted.pop('E_foreign_head_staged')
    assert queue.carry_prepared_stages([], [interrupted, {'dataset': [], 'expected_manifest_sha256': {}}],
        tmp_path, set(), holds={('fixture', 'a'*64)}) == [carried]


def test_queue_retains_sealed_e_generation_after_discovery_moves_on(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *args: [])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda _: False)
    now = queue.time.time()
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [],
    }))
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'queue_identity': 'old-cursor',
        'entries': [{'dataset': ['malformed'], 'expected_manifest_sha256': {}, 'E_prepared': True},
                    {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
                     'state': 'retrying', 'reason': 'd_e_owner_busy', 'E_prepared': True,
                     'prepared_files': 2, 'prepared_logical_bytes': 100,
                     'files': 2, 'logical_bytes': 100, 'retry_after_epoch': now+300}],
    }))

    assert queue.main() == 0
    status = json.loads((root/'queue-status.json').read_text())
    entry, = status['entries']
    assert (entry['dataset'], entry['expected_manifest_sha256']) == ('fixture', 'a'*64)
    assert entry['E_prepared'] is True
    assert entry['state'] == 'retrying'
    assert entry['retry_after_epoch'] == now+300
    assert status['next_scan_epoch'] == now+300


def test_abandoned_running_queue_retains_sealed_e_across_repeated_interruptions(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *args, **kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *args: [])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda _: False)
    now = queue.time.time()
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [],
    }))
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'queue_identity': 'old-cursor',
        'entries': [{'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
                     'state': 'running', 'E_prepared': True, 'E_foreign_head_staged': True,
                     'prepared_files': 2, 'prepared_logical_bytes': 100,
                     'files': 2, 'logical_bytes': 100}],
    }))
    def interrupted(*args, **kwargs):
        raise RuntimeError('simulated interrupted child')
    monkeypatch.setattr(queue, 'run_observed', interrupted)

    for _ in range(2):
        with pytest.raises(RuntimeError, match='simulated interrupted child'):
            queue.main()
        entry, = json.loads((root/'queue-status.json').read_text())['entries']
        assert entry['state'] == 'running'
        assert entry['E_prepared'] is True and entry['E_foreign_head_staged'] is True
        assert entry['prepared_files'] == 2 and entry['prepared_logical_bytes'] == 100
        assert entry['expected_manifest_sha256'] == 'a'*64


def test_new_head_does_not_hide_previous_sealed_generation_or_inherit_its_retry(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *args, **kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *args: [])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda _: False)
    now = queue.time.time()
    newer = {'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'b'*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1/tw-index-futures'/('b'*64)),
             'files': 3, 'logical_bytes': 150}
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [newer],
    }))
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'queue_identity': 'old-cursor',
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
                     'state': 'retrying', 'reason': 'repack_foreign_head_wait', 'E_prepared': True,
                     'prepared_files': 2, 'prepared_logical_bytes': 100,
                     'files': 2, 'logical_bytes': 100, 'retry_after_epoch': now+300}],
    }))
    commands = []
    def busy(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1, stderr='owner busy', stdout='')
    monkeypatch.setattr(queue, 'run_observed', busy)

    assert queue.main() == 0
    entries = json.loads((root/'queue-status.json').read_text())['entries']
    by_sha = {item['expected_manifest_sha256']: item for item in entries}
    assert set(by_sha) == {'a'*64, 'b'*64}
    assert 'b'*64 in commands[0] and 'a'*64 not in commands[0]
    assert by_sha['b'*64]['reason'] == 'd_e_owner_busy'
    assert by_sha['a'*64]['state'] == 'blocked_mismatch'
    assert by_sha['a'*64]['reason'] == 'source_head_advanced'
    assert by_sha['a'*64]['E_prepared'] is True
    assert by_sha['a'*64]['superseded_by_manifest_sha256'] == 'b'*64

    # Both versions remain separately identifiable in the public monitor.
    visible = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert len(visible) == 2 and len({item['id'] for item in visible}) == 2
    old, = [item for item in visible if item['reason'] == 'source_head_advanced']
    assert old['state'] == 'blocked' and old['phase'] == 'source_local_E_prepared_ready'
    assert old['completed_bytes'] == 100 and old['total_bytes'] == 100
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(now), 'tasks': visible}, now=now)
    assert len(public['tasks']) == 2 and len({item['id'] for item in public['tasks']}) == 2


@pytest.mark.parametrize('private', [False, True])
def test_e_only_old_generation_is_selected_over_new_head_of_same_dataset(tmp_path, monkeypatch, private):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *args, **kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *args: [])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations',
                        lambda *_: {('fixture', 'a'*64)})
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'load', lambda *_: {'private': private})
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda *_args: {'files': 2, 'logical_bytes': 100})
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda _: False)
    now = queue.time.time()
    origin = 'private_cold' if private else 'cold'
    newer = {'dataset': 'fixture', 'expected_manifest_sha256': 'b'*64, 'origin': origin,
             'source': str(tmp_path/'migration/stream-source-repack-v1/fixture'/('b'*64)),
             'files': 3, 'logical_bytes': 150}
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [newer],
    }))
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'entries': [
            {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': origin,
             'state': 'retrying', 'reason': 'repack_foreign_head_wait', 'E_prepared': True,
             'prepared_files': 2, 'prepared_logical_bytes': 100,
             'files': 2, 'logical_bytes': 100, 'retry_after_epoch': now+300}],
    }))
    commands = []
    def busy(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1,
            stderr='PackedPublicationBusy: publication owner busy: publish-fixture.lock', stdout='')
    monkeypatch.setattr(queue, 'run_observed', busy)

    outcomes = []
    assert queue.main(on_result=outcomes.append) == 0
    assert outcomes == ['retrying']  # Sole-source recovery must not be overtaken.
    by_sha = {item['expected_manifest_sha256']: item for item in
              json.loads((root/'queue-status.json').read_text())['entries']}
    assert set(by_sha) == {'a'*64, 'b'*64}
    assert 'a'*64 in commands[0] and 'b'*64 not in commands[0]
    assert ('--private-source' in commands[0]) is private
    assert by_sha['a'*64]['reason'] == 'd_e_owner_busy' and by_sha['a'*64]['E_prepared'] is True
    assert by_sha['b'*64]['state'] == 'waiting'


def test_e_only_generation_rejects_discovered_wrong_authority(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    row = {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
           'source': str(tmp_path/'migration/stream-source-repack-v1/fixture'/('a'*64)),
           'files': 2, 'logical_bytes': 100}
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *_args: [row])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations',
                        lambda *_args: {('fixture', 'a'*64)})
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'load', lambda *_args: {'private': True})
    monkeypatch.setattr(queue.stream_repack, '_prepared_metadata',
                        lambda *_args: {'files': 2, 'logical_bytes': 100})
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': queue.time.time()+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': [],
    }))
    monkeypatch.setattr(queue, 'run_observed', lambda *_args, **_kwargs: pytest.fail('no writer may start'))
    with pytest.raises(SnapshotError, match='authority differs from queue'):
        queue.main()


def test_missing_inventory_recognizes_only_the_exact_d_object_scope():
    digest = '61' + 'a'*62
    error = ("FileNotFoundError: [Errno 2] No such file or directory: "
             f"'/srv/stockagent-packed/objects/inventories/61/{digest}.jsonl.gz'")
    assert queue.missing_cold_inventory(error)
    assert not queue.missing_cold_inventory(error.replace('/61/', '/fc/'))
    assert not queue.missing_cold_inventory(error.replace('/inventories/', '/packs/'))
    assert not queue.missing_cold_inventory(error.replace('/srv/stockagent-packed/', '/tmp/'))
    assert not queue.missing_cold_inventory(error.replace('FileNotFoundError', 'PermissionError'))


def test_ready_batch_stops_between_items_for_trading_priority():
    checks = []
    def guard():
        checks.append(True)
        if len(checks) > 1:
            raise SnapshotError('Taiwan day-trade priority window; maintenance deferred')
    def run(*, on_result):
        on_result('published')
        return 0
    result = queue.run_ready_batch(run=run, guard=guard, monotonic=lambda: 0)
    assert result == {'attempted': 1, 'published': 1, 'reason': 'trading_priority', 'exit_code': 0}


def test_ready_batch_time_bound_never_interrupts_an_inflight_dataset():
    clock = [0]
    def run(*, on_result):
        clock[0] += 181
        on_result('published')
        return 0
    result = queue.run_ready_batch(run=run, monotonic=lambda: clock[0])
    assert result == {'attempted': 1, 'published': 1, 'reason': 'time_limit', 'exit_code': 0}


def test_ready_batch_propagates_owner_failure():
    result = queue.run_ready_batch(run=lambda **_: 7, monotonic=lambda: 0)
    assert result == {'attempted': 1, 'published': 0, 'reason': 'owner_error', 'exit_code': 7}


@pytest.mark.parametrize('advance,should_finish', [(True, True), (False, False)])
def test_observed_child_deadline_follows_real_progress_only(monkeypatch, advance, should_finish):
    clock = [0.0]
    heartbeats = []
    class Process:
        returncode = 0
        killed = False
        calls = 0
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def kill(self): self.killed = True
        def communicate(self, timeout=None):
            if self.killed:
                return '', ''
            self.calls += 1
            clock[0] += timeout
            if self.calls < 3:
                raise subprocess.TimeoutExpired('child', timeout)
            return 'done', ''
    process = Process()
    monkeypatch.setattr(queue, 'time', SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(queue.subprocess, 'Popen', lambda *_args, **_kwargs: process)
    probe = (lambda: process.calls) if advance else (lambda: 0)
    if should_finish:
        result = queue.run_observed(['child'], on_heartbeat=lambda: heartbeats.append(1),
                                    timeout=3, progress_probe=probe)
        assert result.stdout == 'done' and clock[0] > 3 and not process.killed
    else:
        with pytest.raises(subprocess.TimeoutExpired):
            queue.run_observed(['child'], on_heartbeat=lambda: heartbeats.append(1),
                               timeout=3, progress_probe=probe)
        assert process.killed


def test_fixed_generation_hold_yields_to_other_work_and_releases_by_identity(tmp_path):
    config = tmp_path/'holds.json'
    row = {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold'}
    assert queue.load_generation_holds(config) == set()
    config.write_text(json.dumps({'contract': queue.CONTRACT+'.generation-holds-v1', 'entries': [
        {'dataset': row['dataset'], 'expected_manifest_sha256': row['expected_manifest_sha256'],
         'reason': 'source_duplicate_bloat'}]}))
    holds = queue.load_generation_holds(config)
    held = queue.generation_hold(row, holds, now=100)
    assert held['reason'] == 'source_duplicate_bloat' and held['retry_after_epoch'] == 400
    assert queue.generation_hold({**row, 'dataset': 'other'}, holds, now=100) is None
    assert queue.generation_hold({**row, 'expected_manifest_sha256': 'b'*64}, holds, now=100) is None
    assert queue.generation_hold({**row, 'origin': 'workspace'}, holds, now=100) is None
    config.write_text(json.dumps({'contract': queue.CONTRACT+'.generation-holds-v1', 'entries': []}))
    assert queue.generation_hold(row, queue.load_generation_holds(config), now=101) is None


def test_generation_hold_rejects_redirected_or_malformed_policy(tmp_path):
    config = tmp_path/'holds.json'
    config.write_text(json.dumps({'contract': queue.CONTRACT+'.generation-holds-v1', 'entries': [
        {'dataset': '../outside', 'expected_manifest_sha256': 'a'*64, 'reason': 'source_duplicate_bloat'}]}))
    with pytest.raises(SnapshotError, match='invalid source-local generation hold'):
        queue.load_generation_holds(config)
    config.unlink()
    config.symlink_to(tmp_path/'outside')
    with pytest.raises(SnapshotError, match='redirected'):
        queue.load_generation_holds(config)


def test_generation_hold_is_visible_as_blocked_without_a_fake_eta(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 100,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'reason': 'source_duplicate_bloat',
                     'retry_after_epoch': 400, 'logical_bytes': 500, 'files': 20}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'source_duplicate_bloat'
    assert row['next_attempt_at_utc'] is None


def test_dedup_candidate_progress_uses_unique_content_counts_not_all_logical_files(tmp_path):
    from scripts.run_dedup_candidate_queue import process_identity
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 100,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'reason': 'source_duplicate_bloat',
                     'logical_bytes': 500, 'files': 20}]}))
    dedup = tmp_path/'migration/source-local-dedup-candidate-v1'
    dedup.mkdir()
    status = {'contract': 'stockagent.dedup_candidate_queue.v1', 'dataset': 'tw-index-futures',
              'expected_manifest_sha256': 'a'*64, 'state': 'running', 'observed_at_epoch': 110,
              'owner_identity': process_identity(), 'publishable': False, 'retirement_authorized': False,
              'unique_contents': 5, 'unique_bytes': 100, 'fallback_count': 2,
              'progress': {'stage': 'streaming_source_into_E_packs', 'files_completed': 2,
                           'logical_bytes_processed': 40}}
    path = dedup/'tw-index-futures.status.json'
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_dedup_packing'
    assert row['_dedup_candidate_owner'] is True
    assert row['metrics']['dedup_native_fallbacks'] == 2
    assert (row['completed_items'], row['total_items'], row['completed_bytes'], row['total_bytes']) == (2, 5, 40, 100)
    status.update(state='sealed_waiting_reader', unique_contents=5)
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'dedup_candidate_reader_pending'
    assert row['phase'] == 'source_local_dedup_sealed' and not row['complete_evidence']
    status.update(state='validating_v3_metadata', files=20,
                  progress={'stage': 'validate-packed-inventory', 'completed_items': 11, 'total_items': 25})
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_dedup_v3_validating'
    assert (row['completed_items'], row['total_items']) == (11, 25)
    status.update(state='v3_metadata_prepared_waiting_handoff')
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'dedup_candidate_handoff_pending'
    assert row['phase'] == 'source_local_dedup_v3_ready' and not row['complete_evidence']


def test_completed_old_E_checkpoint_cleanup_is_separate_from_D_and_WSL_reclamation(tmp_path):
    root = tmp_path/'migration/source-local-dedup-candidate-v1/tw-index-futures'
    root.mkdir(parents=True)
    plan = [{'name': 'pack-a.zip', 'allocated_bytes': 100}, {'name': 'pack-b.partial', 'allocated_bytes': 20}]
    sha = hashlib.sha256(_canonical_json_bytes(plan)).hexdigest()
    record = {'contract': 'stockagent.dedup_stream_stage_handoff.v1.legacy-prune',
              'dataset': 'tw-index-futures', 'state': 'completed', 'old_manifest_sha256': 'a'*64,
              'completed_at_epoch': 100, 'plan': plan, 'plan_sha256': sha,
              'removed': [item['name'] for item in plan], 'reclaimed_allocated_bytes': 120}
    path = root/'tw-index-futures.legacy-prune.json'
    path.write_text(json.dumps(record))
    row, = storage_progress.source_local_dedup_legacy_cleanup_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['metrics'] == {'E_legacy_reclaimed_allocated_bytes': 120}
    assert row['completed_items'] == 2 and row['total_items'] == 2
    record['reclaimed_allocated_bytes'] = 121
    path.write_text(json.dumps(record))
    assert storage_progress.source_local_dedup_legacy_cleanup_tasks(tmp_path, workspace_checked=True) == []


def test_old_dedup_writer_reports_only_real_compression_observation(tmp_path):
    from scripts.run_dedup_candidate_queue import process_identity
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 100,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'reason': 'source_duplicate_bloat'}]}))
    dedup = tmp_path/'migration/source-local-dedup-candidate-v1'
    stage = dedup/'tw-index-futures'/('a'*64)
    stage.mkdir(parents=True)
    status = {'contract': 'stockagent.dedup_candidate_queue.v1', 'dataset': 'tw-index-futures',
              'expected_manifest_sha256': 'a'*64, 'state': 'running', 'observed_at_epoch': 110,
              'owner_identity': process_identity(), 'publishable': False, 'retirement_authorized': False,
              'progress': {'stage': 'writing_candidate_inventory', 'items_completed': 9, 'items_total': 10}}
    (dedup/'tw-index-futures.status.json').write_text(json.dumps(status))
    partial = stage/('inventory-'+'b'*32+'.partial')
    partial.write_bytes(b'partial')
    os.utime(partial, (120, 120))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_dedup_compressing'
    assert row['completed_items'] is None and row['total_items'] is None
    assert row['observed_at_utc'] == storage_progress.iso(120)
    partial.unlink()
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_dedup_inventory'
    assert (row['completed_items'], row['total_items']) == (9, 10)


def test_dedup_status_from_a_dead_process_does_not_claim_running(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 100,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'reason': 'source_duplicate_bloat'}]}))
    dedup = tmp_path/'migration/source-local-dedup-candidate-v1'
    dedup.mkdir()
    (dedup/'tw-index-futures.status.json').write_text(json.dumps({
        'contract': 'stockagent.dedup_candidate_queue.v1', 'dataset': 'tw-index-futures',
        'expected_manifest_sha256': 'a'*64, 'state': 'running', 'observed_at_epoch': 110,
        'owner_identity': {'pid': 999999999, 'start_ticks': 1, 'boot_id': 'old'},
        'publishable': False, 'retirement_authorized': False}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'dedup_candidate_interrupted'


def test_dedup_retry_uses_its_own_clock_not_the_parent_queue_clock(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 100,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'reason': 'source_duplicate_bloat',
                     'retry_after_epoch': 900}]}))
    dedup = tmp_path/'migration/source-local-dedup-candidate-v1'
    dedup.mkdir()
    (dedup/'tw-index-futures.status.json').write_text(json.dumps({
        'contract': 'stockagent.dedup_candidate_queue.v1', 'dataset': 'tw-index-futures',
        'expected_manifest_sha256': 'a'*64, 'state': 'waiting_retry', 'observed_at_epoch': 110,
        'retry_after_epoch': 400, 'error_type': 'PackedPublicationBusy',
        'publishable': False, 'retirement_authorized': False}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['reason'] == 'd_e_owner_busy'
    assert row['next_attempt_at_utc'] == storage_progress.iso(400)


def test_sealed_sources_yield_only_to_the_existing_live_install_owner(tmp_path):
    lock = tmp_path/'prepared-install-queue-owner.lock'
    assert not queue.prepared_installer_busy(tmp_path)
    lock.touch()
    row = {'dataset': 'binance-public-archive', 'origin': 'cold'}
    prior = {'state': 'retrying', 'reason': 'repack_D_capacity', 'E_prepared': True,
             'prepared_files': 10}
    with lock.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert queue.prepared_installer_busy(tmp_path)
        deferred = queue.defer_sealed_install(row, prior, installer_busy=True, now=100)
        assert deferred['state'] == 'retrying' and deferred['reason'] == 'd_e_owner_busy'
        assert deferred['retry_after_epoch'] == 160 and deferred['prepared_files'] == 10
        assert queue.defer_sealed_install(row, {**prior, 'E_prepared': False},
                                          installer_busy=True, now=100) is None
        assert queue.defer_sealed_install({**row, 'origin': 'workspace'}, prior,
                                          installer_busy=True, now=100) is None
        assert queue.defer_sealed_install(row, {**prior, 'reason': 'repack_source_integrity'},
                                          installer_busy=True, now=100) is None
    assert not queue.prepared_installer_busy(tmp_path)
    assert queue.defer_sealed_install(row, prior, installer_busy=False, now=100) is None
    lock.unlink()
    lock.symlink_to(tmp_path/'elsewhere')
    with pytest.raises(OSError):
        queue.prepared_installer_busy(tmp_path)


def test_queue_enforces_scoped_source_and_unique_datasets(tmp_path):
    config = tmp_path/'queue.json'
    row = {'dataset': 'fixture', 'source': str(tmp_path/'downloads/data_fixture'),
           'expected_manifest_sha256': 'a'*64}
    config.write_text(json.dumps({'contract': queue.CONTRACT, 'entries': [row]}))
    assert queue.load_queue(config, tmp_path) == [row]
    config.write_text(json.dumps({'contract': queue.CONTRACT, 'entries': [row, row]}))
    with pytest.raises(SnapshotError, match='queue member'):
        queue.load_queue(config, tmp_path)
    second = {**row, 'expected_manifest_sha256': 'b'*64}
    with pytest.raises(SnapshotError, match='queue member'):
        queue.validate_entries([row, second], tmp_path)
    assert queue.validate_entries([row, second], tmp_path, allow_generations=True) == [row, second]
    with pytest.raises(SnapshotError, match='queue member'):
        queue.validate_entries([row, row], tmp_path, allow_generations=True)
    row['source'] = '/outside/data_fixture'
    config.write_text(json.dumps({'contract': queue.CONTRACT, 'entries': [row]}))
    with pytest.raises(SnapshotError, match='queue member'):
        queue.load_queue(config, tmp_path)


def test_private_retirement_uses_the_private_owner_and_does_not_sweep_public_history(tmp_path, monkeypatch):
    name, old, new = 'local-tw-index-derivatives-ticks', 'a'*64, 'b'*64
    directory = tmp_path/name
    directory.mkdir()
    path = directory/(new+'.json')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'private': True,
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': name,
        'source_mode': 'D_streamed_to_E_dataset_pack', 'old_manifest_sha256': old,
        'new_manifest_sha256': new, 'old_release_removed': False, 'logical_bytes': 100}
    path.write_text(json.dumps(receipt))
    assert queue.discover_pending_retirements(tmp_path)[0]['private'] is True
    def run(command, *, on_heartbeat, timeout):
        assert '--private-source' in command and command[command.index('--dataset')+1] == name
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'completed', 'object_bytes': 1, 'object_count': 1}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_one_retirement(tmp_path)
    assert json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries'][0]['state'] == 'completed'
    receipt['old_release_removed'] = True
    path.write_text(json.dumps(receipt))
    row, = queue.discover_pending_retirements(tmp_path)
    assert row['private'] is True and row['mode'] == 'history'
    def run_history(command, *, on_heartbeat, timeout):
        assert '--private-source' in command and '--history' in command
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'no_unpinned_history', 'object_bytes': 0}))
    monkeypatch.setattr(queue, 'run_observed', run_history)
    assert queue.run_one_retirement(tmp_path)
    assert json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries'][0]['state'] == 'completed'


def test_private_busy_owner_does_not_block_public_retirement_and_wakes_immediately(tmp_path, monkeypatch):
    private = {'dataset': 'private-fixture', 'old_manifest_sha256': 'a'*64,
               'logical_bytes': 100, 'mode': 'direct', 'private': True}
    public = {'dataset': 'public-fixture', 'old_manifest_sha256': 'b'*64,
              'logical_bytes': 90, 'mode': 'direct'}
    pending = [private, public]
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _: list(pending))
    for name in ('discover_latest_retirements', 'discover_latest_shared_objects',
                 'discover_semantic_retirements', 'discover_prepared_history_retirements'):
        monkeypatch.setattr(queue, name, lambda _: [])
    called = []
    def run(command, *, on_heartbeat, timeout):
        name = command[command.index('--dataset')+1]
        called.append(name)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'completed', 'object_bytes': 1, 'object_count': 1}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_one_retirement(tmp_path, busy_publication_scopes={'private'})
    assert called == ['public-fixture']
    rows = json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries']
    deferred, = [row for row in rows if row['dataset'] == 'private-fixture']
    assert deferred['reason'] == 'd_e_owner_busy' and deferred['owner_lock'] == 'publication_scope_hint'
    pending.remove(public)
    assert queue.run_one_retirement(tmp_path, busy_publication_scopes=set())
    assert called == ['public-fixture', 'private-fixture']


def test_semantic_retirement_precedes_other_work_and_is_not_repeated(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    receipt_path = tmp_path/'migration/handoff-index-archive-v1/binance-public-archive'/('semantic-promotion-'+'a'*64+'.json')
    receipt_path.parent.mkdir(parents=True)
    receipt = {'contract': 'stockagent.semantic_link_promotion.v1', 'state': 'committed',
               'dataset': 'binance-public-archive', 'comparison_identity_sha256': 'a'*64,
               'old_manifest_sha256': 'b'*64, 'old_sources_deleted': False,
               'manifest': {'source': {'logical_bytes': 900}}}
    receipt_path.write_text(json.dumps(receipt))
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _: [
        {'dataset': 'fixture', 'old_manifest_sha256': 'c'*64, 'logical_bytes': 1, 'mode': 'direct'}])
    monkeypatch.setattr(queue, 'discover_prepared_history_retirements', lambda _: [])
    seen = []
    def run(command, *, on_heartbeat, timeout):
        seen.append(command)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': 'binance-public-archive', 'state': 'completed', 'apply': True,
            'old_manifest_sha256': 'b'*64, 'object_bytes': 500, 'object_count': 2}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_one_retirement(root)
    assert seen[0][1].endswith('retire_binance_archive_semantic.py')
    assert seen[0][seen[0].index('--old-manifest-sha256')+1] == 'b'*64
    row = json.loads((root/'retirement-queue-status.json').read_text())['entries'][0]
    assert row['mode'] == 'semantic' and row['state'] == 'completed'
    receipt['old_sources_deleted'] = True
    receipt_path.write_text(json.dumps(receipt))
    assert queue.discover_semantic_retirements(root) == []


def test_prefetch_receives_selected_generation_and_failure_keeps_original_worker(tmp_path, monkeypatch):
    name, old, new = 'history-fixture', 'a'*64, 'b'*64
    directory = tmp_path/name
    directory.mkdir()
    (directory/(new+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': name,
        'source_mode': 'D_streamed_to_E_dataset_pack', 'old_manifest_sha256': old,
        'new_manifest_sha256': new, 'old_release_removed': True, 'logical_bytes': 100}))
    events = []
    def prefetch(row):
        events.append(('prefetch', row['dataset'], row['old_manifest_sha256'], row['mode']))
        row['dataset'] = 'mutated-copy'
        raise SnapshotError('cache hint unavailable')
    def run(command, *, on_heartbeat, timeout):
        selected = command[command.index('--dataset')+1]
        events.append(('worker', selected))
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'no_unpinned_history', 'object_bytes': 0}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_one_retirement(tmp_path, on_selected=prefetch)
    assert events == [('prefetch', name, old, 'history'), ('worker', name)]


def test_prefetch_heartbeat_runs_without_mutating_the_selected_generation():
    observed = []
    pulsed = threading.Event()
    original = {'dataset': 'fixed', 'mode': 'history'}
    def save():
        observed.append('heartbeat')
        pulsed.set()
    def prefetch(row):
        row['dataset'] = 'changed-copy'
        assert pulsed.wait(timeout=2)
    queue._selected_prefetch_with_heartbeat(prefetch, original, save, interval=0.01)
    assert observed and original['dataset'] == 'fixed'


@pytest.mark.parametrize('dataset', ['binance', 'yahoo-market'])
def test_latest_fixed_retirement_records_uncompared_history_and_has_priority(tmp_path, monkeypatch, dataset):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    receipt_path = tmp_path/'migration/handoff-index-archive-v1'/dataset/('latest-promotion-'+'a'*64+'.json')
    receipt_path.parent.mkdir(parents=True)
    receipt = {'contract': 'stockagent.latest_fixed_link_promotion.v1', 'state': 'committed',
        'dataset': dataset, 'comparison_identity_sha256': 'a'*64, 'old_manifest_sha256': 'b'*64,
        'old_sources_deleted': False, 'changed_member_values_compared': False,
        'old_historical_value_coverage': 'not_compared', 'uncompared_changed_members': 12,
        'manifest': {'source': {'logical_bytes': 900}}}
    receipt_path.write_text(json.dumps(receipt))
    monkeypatch.setattr(queue, 'discover_semantic_retirements', lambda _: [])
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _: [])
    monkeypatch.setattr(queue, 'discover_prepared_history_retirements', lambda _: [])
    def runner(command, *, on_heartbeat, timeout):
        assert command[1].endswith('retire_latest_fixed_generation.py')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': dataset, 'state': 'completed', 'apply': True, 'old_manifest_sha256': 'b'*64,
            'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
            'object_bytes': 800, 'object_count': 2}))
    monkeypatch.setattr(queue, 'run_observed', runner)
    assert queue.run_one_retirement(root)
    row, = json.loads((root/'retirement-queue-status.json').read_text())['entries']
    assert row['mode'] == 'latest' and row['state'] == 'completed' and row['uncompared_changed_members'] == 12
    receipt['old_sources_deleted'] = True
    receipt_path.write_text(json.dumps(receipt))
    assert queue.discover_latest_retirements(root) == []


def test_fixed_object_sharing_is_scheduled_and_an_interrupted_receipt_is_resumed(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    directory = root.parent/'handoff-index-archive-v1/okx'; directory.mkdir(parents=True)
    identity, old = 'a'*64, 'b'*64
    path = directory/('latest-promotion-'+identity+'.json')
    receipt = {'contract': 'stockagent.latest_fixed_link_promotion.v1', 'state': 'committed',
        'dataset': 'okx', 'comparison_identity_sha256': identity, 'old_manifest_sha256': old,
        'old_sources_deleted': True, 'duplicate_encoded_bytes_pending': 99}
    path.write_text(json.dumps(receipt))
    for name in ('discover_latest_retirements', 'discover_semantic_retirements',
                 'discover_pending_retirements', 'discover_prepared_history_retirements'):
        monkeypatch.setattr(queue, name, lambda _: [])
    def runner(command, *, on_heartbeat, timeout):
        assert command[1].endswith('deduplicate_latest_fixed_generation.py')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': 'okx', 'state': 'completed', 'apply': True, 'old_manifest_sha256': old,
            'object_bytes': 99, 'object_count': 1, 'estimated_allocated_bytes_reclaimed': 100}))
    monkeypatch.setattr(queue, 'run_observed', runner)
    assert queue.run_one_retirement(root)
    row, = json.loads((root/'retirement-queue-status.json').read_text())['entries']
    assert row['mode'] == 'dedup' and row['state'] == 'completed'
    receipt['duplicate_encoded_bytes_pending'] = 0
    path.write_text(json.dumps(receipt))
    assert queue.discover_latest_shared_objects(root) == []
    journal = directory/('shared-objects-'+identity+'.json')
    journal.write_text(json.dumps({'state': 'sharing'}))
    assert queue.discover_latest_shared_objects(root)[0]['mode'] == 'dedup'
    journal.write_text(json.dumps({'state': 'completed'}))
    assert queue.discover_latest_shared_objects(root) == []


def test_busy_shared_objects_do_not_starve_unrelated_retirement(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    common = {'old_manifest_sha256': 'b'*64, 'logical_bytes': 1}
    monkeypatch.setattr(queue, 'discover_latest_retirements', lambda _: [])
    monkeypatch.setattr(queue, 'discover_latest_shared_objects', lambda _: [{'dataset': 'okx', **common, 'mode': 'dedup'}])
    monkeypatch.setattr(queue, 'discover_semantic_retirements', lambda _: [])
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _: [{'dataset': 'other', **common, 'mode': 'direct'}])
    monkeypatch.setattr(queue, 'discover_prepared_history_retirements', lambda _: [])
    clock = [1000.0]
    monkeypatch.setattr(queue.time, 'time', lambda: clock[0])
    commands = []
    def runner(command, *, on_heartbeat, timeout):
        commands.append(command[1])
        if len(commands) == 1:
            return SimpleNamespace(returncode=1, stderr='PackedPublicationBusy: publication owner busy', stdout='')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': 'other', 'state': 'completed', 'object_bytes': 1}))
    monkeypatch.setattr(queue, 'run_observed', runner)
    assert queue.run_one_retirement(root)
    clock[0] += 61
    assert queue.run_one_retirement(root)
    assert commands[0].endswith('deduplicate_latest_fixed_generation.py')
    assert commands[1].endswith('retire_stream_repacked_release.py')


def test_other_publisher_head_is_reported_without_blocking_other_retirements(tmp_path, monkeypatch):
    for name, old, new, removed in (('a-source', 'a'*64, 'b'*64, False),
                                     ('b-source', 'c'*64, 'd'*64, True)):
        directory = tmp_path/name
        directory.mkdir()
        (directory/(new+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
            'dataset': name, 'old_manifest_sha256': old, 'new_manifest_sha256': new,
            'logical_bytes': 1, 'old_release_removed': removed}))
    seen = []
    def run(command, *, on_heartbeat, timeout):
        name = command[command.index('--dataset')+1]
        seen.append(name)
        if name == 'a-source':
            return SimpleNamespace(returncode=1, stderr='old release is still a current head', stdout='')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'no_unpinned_history', 'object_bytes': 0}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_one_retirement(tmp_path)
    first = json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries'][0]
    assert first['reason'] == 'retirement_current_head' and first['state'] == 'waiting_retry'
    assert queue.run_one_retirement(tmp_path) and seen == ['a-source', 'b-source']


def test_current_head_cooldown_wakes_when_head_transition_is_observed(tmp_path, monkeypatch):
    name, old, new = 'fixture', 'a'*64, 'b'*64
    directory = tmp_path/name
    directory.mkdir()
    (directory/(new+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': name, 'old_manifest_sha256': old, 'new_manifest_sha256': new,
        'logical_bytes': 1, 'old_release_removed': False}))
    clock, released, calls = [1000.0], [False], []
    monkeypatch.setattr(queue.time, 'time', lambda: clock[0])
    monkeypatch.setattr(queue, 'retirement_head_released_hint', lambda *_args: released[0])
    def runner(command, *, on_heartbeat, timeout):
        calls.append(command)
        if len(calls) == 1:
            return SimpleNamespace(returncode=1, stderr='old release is still a current head', stdout='')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'completed', 'object_bytes': 1, 'object_count': 1}))
    monkeypatch.setattr(queue, 'run_observed', runner)
    assert queue.run_one_retirement(tmp_path)
    clock[0] += 60
    assert not queue.run_one_retirement(tmp_path) and len(calls) == 1
    released[0] = True
    assert queue.run_one_retirement(tmp_path) and len(calls) == 2
    assert json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries'][0]['state'] == 'completed'


def test_completed_handoff_wakes_a_pinned_retirement_only_once(tmp_path, monkeypatch):
    name, old, new = 'fixture', 'a'*64, 'b'*64
    directory = tmp_path/name
    directory.mkdir()
    (directory/(new+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': name, 'old_manifest_sha256': old, 'new_manifest_sha256': new,
        'logical_bytes': 1, 'old_release_removed': False}))
    clock, identity, calls = [1000.0], [None], []
    monkeypatch.setattr(queue.time, 'time', lambda: clock[0])
    monkeypatch.setattr(queue, 'handoff_completed_identity', lambda *_args: identity[0])
    def runner(command, *, on_heartbeat, timeout):
        calls.append(command)
        return SimpleNamespace(returncode=1, stderr='old release is pinned', stdout='')
    monkeypatch.setattr(queue, 'run_observed', runner)
    assert queue.run_one_retirement(tmp_path)
    clock[0] += 60
    assert not queue.run_one_retirement(tmp_path) and len(calls) == 1
    identity[0] = 'c'*64
    assert queue.run_one_retirement(tmp_path) and len(calls) == 2
    assert not queue.run_one_retirement(tmp_path) and len(calls) == 2
    row = json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries'][0]
    assert row['handoff_recheck_identity'] == identity[0]


def test_fresh_positive_pin_hint_defers_only_exact_candidate_and_other_work_advances(tmp_path, monkeypatch):
    old_a, old_b, new = 'a'*64, 'b'*64, 'c'*64
    for name, old in [('pinned', old_a), ('ready', old_b)]:
        directory = tmp_path/name
        directory.mkdir()
        (directory/(new+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
            'dataset': name, 'old_manifest_sha256': old, 'new_manifest_sha256': new,
            'logical_bytes': 1, 'old_release_removed': False}))
    (tmp_path/'retirement-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.retirement-status', 'entries': [
            {'dataset': 'pinned', 'old_manifest_sha256': old_a, 'mode': 'direct',
             'state': 'waiting_retry', 'reason': 'retirement_pinned', 'retry_after_epoch': 999,
             'execution_sha256': queue.RETIRE_IMPLEMENTATION_SHA256}]}))
    clock, seen = [1000.0], []
    monkeypatch.setattr(queue.time, 'time', lambda: clock[0])
    def runner(command, *, on_heartbeat, timeout):
        name = command[command.index('--dataset')+1]
        seen.append(name)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': name, 'state': 'completed', 'object_bytes': 1, 'object_count': 1}))
    monkeypatch.setattr(queue, 'run_observed', runner)
    hint = {'observed_at_epoch': 1000.0, 'keys': frozenset({('pinned', old_a)})}
    assert queue.run_one_retirement(tmp_path, positive_pin_hints=hint)
    assert seen == ['ready']
    rows = json.loads((tmp_path/'retirement-queue-status.json').read_text())['entries']
    pinned = next(row for row in rows if row['dataset'] == 'pinned')
    assert pinned['state'] == 'waiting_retry' and pinned['reason'] == 'retirement_pinned'
    assert pinned['retry_after_epoch'] == 1300.0
    clock[0] = 1301.0
    assert queue.run_one_retirement(tmp_path, positive_pin_hints=hint)
    assert seen[-1] == 'pinned'  # Expired hints never authorize or suppress the original child.


def test_positive_pin_hints_reject_wrong_scope_and_untrusted_clocks():
    row = {'dataset': 'fixture', 'old_manifest_sha256': 'a'*64, 'mode': 'direct'}
    keys = frozenset({('fixture', 'a'*64)})
    assert queue.positive_pin_hint_applies({'observed_at_epoch': 1000, 'keys': keys}, row, 1000)
    for stamp in (float('nan'), float('inf'), 10**400, 1121, 879):
        assert not queue.positive_pin_hint_applies({'observed_at_epoch': stamp, 'keys': keys}, row, 1000)
    assert not queue.positive_pin_hint_applies({'observed_at_epoch': 1000, 'keys': keys},
                                               {**row, 'private': True}, 1000)
    assert not queue.positive_pin_hint_applies({'observed_at_epoch': 1000, 'keys': keys},
                                               {**row, 'mode': 'history'}, 1000)
    assert not queue.positive_pin_hint_applies({'observed_at_epoch': 1000, 'keys': set(keys)}, row, 1000)


def test_head_transition_hint_requires_the_fixed_stage_and_no_matching_head(tmp_path):
    workspace, cold = tmp_path/'workspace', tmp_path/'cold'
    name, old, old_id = 'fixture', 'a'*64, 'fixture-old'
    stage = workspace/'migration/stream-source-repack-v1'/name/old
    stage.mkdir(parents=True)
    prepared = {'contract': queue.stream_repack.CONTRACT+'.prepared', 'dataset': name,
                'old_manifest_sha256': old, 'old_snapshot_id': old_id,
                'destination_independent_readback': False, 'raw_extraction_tree_created': False,
                'source_member_sha_streamed': True, 'packages': []}
    prepared['identity_sha256'] = queue.stream_repack._identity(prepared)
    (stage/'prepared.json').write_text(json.dumps(prepared))
    head = cold/'heads'/name/'vastai1T.json'
    head.parent.mkdir(parents=True)
    head.write_text(json.dumps({'dataset': name, 'snapshot_id': old_id, 'manifest_sha256': old}))
    hint = queue.retirement_head_released_hint
    assert not hint(str(workspace), str(cold), name, old)
    head.write_text(json.dumps({'dataset': name, 'snapshot_id': 'fixture-new', 'manifest_sha256': 'b'*64}))
    queue._RETIREMENT_HEAD_BLOCKED_HINTS.clear()
    assert hint(str(workspace), str(cold), name, old)
    head.write_text(json.dumps({'dataset': name, 'snapshot_id': old_id, 'manifest_sha256': 'b'*64}))
    assert not hint(str(workspace), str(cold), name, old)
    head.unlink()
    queue._RETIREMENT_HEAD_BLOCKED_HINTS.clear()
    assert not hint(str(workspace), str(cold), name, old)


def test_continuous_direct_releases_cannot_starve_ready_history(tmp_path, monkeypatch):
    common = {'logical_bytes': 1}
    rows = [{'dataset': f'direct-{i}', 'old_manifest_sha256': str(i)*64, 'mode': 'direct', **common}
            for i in range(1, 6)]
    rows.append({'dataset': 'history', 'old_manifest_sha256': 'a'*64, 'mode': 'history', **common})
    for name in ('discover_latest_retirements', 'discover_latest_shared_objects',
                 'discover_semantic_retirements', 'discover_prepared_history_retirements'):
        monkeypatch.setattr(queue, name, lambda _: [])
    monkeypatch.setattr(queue, 'discover_pending_retirements', lambda _: rows)
    selected = []
    def run(command, *, on_heartbeat, timeout):
        name = command[command.index('--dataset')+1]
        selected.append(name)
        if name != 'history':
            return SimpleNamespace(returncode=1, stderr='old release is still a current head', stdout='')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps(
            {'dataset': name, 'state': 'no_unpinned_history', 'object_bytes': 0}))
    monkeypatch.setattr(queue, 'run_observed', run)
    for _ in range(5):
        assert queue.run_one_retirement(tmp_path)
    assert selected == ['direct-1', 'direct-2', 'direct-3', 'direct-4', 'history']
    status = json.loads((tmp_path/'retirement-queue-status.json').read_text())
    assert status['consecutive_direct_attempts'] == 0
    assert queue.run_one_retirement(tmp_path)
    assert selected[-1] == 'direct-5'


def test_receipt_must_match_exact_old_and_new_identity(tmp_path):
    directory = tmp_path/'fixture'
    directory.mkdir()
    row = {'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64}
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64}
    path = directory/('b'*64+'.json')
    path.write_text(json.dumps(receipt))
    assert queue.prior_published(tmp_path, row)
    receipt['old_manifest_sha256'] = 'c'*64
    path.write_text(json.dumps(receipt))
    assert not queue.prior_published(tmp_path, row)


def test_queue_monitor_reports_missing_source_inventory_without_private_paths(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'blocked_mismatch',
                     'reason': 'repack_source_inventory_missing', 'retry_after_epoch': 1300}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_source_inventory_missing'
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(1000), 'tasks': [row]}, now=1000)
    exposed = public['tasks'][0]
    assert exposed['reason_code'] == 'repack_source_inventory_missing'
    assert '缺少清冊' in exposed['reason'] and '/srv/' not in json.dumps(exposed, ensure_ascii=False)


def test_queue_monitor_reports_e_staged_foreign_head_wait(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'origin': 'cold', 'state': 'retrying', 'E_prepared': True,
                     'reason': 'repack_foreign_head_wait', 'retry_after_epoch': 1300,
                     'logical_bytes': 200, 'files': 2, 'prepared_logical_bytes': 200, 'prepared_files': 2}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_foreign_head_wait'
    assert row['completed_bytes'] == 200 and row['total_bytes'] == 200
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(1000), 'tasks': [row]}, now=1000)
    assert '另一發布節點' in public['tasks'][0]['reason']


def test_queue_monitor_explains_measured_exclusive_space_shortage(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
              'origin': 'cold', 'state': 'retrying', 'reason': 'repack_D_capacity',
              'E_prepared': True, 'prepared_logical_bytes': 200, 'prepared_files': 2}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
        'observed_at_epoch': now, 'entries': [source]}))
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'state': 'retrying', 'error': 'insufficient_exclusive_space',
                     'E_temporary_authority_ready': True, 'encoded_bytes_remaining': 150,
                     'D_free_bytes': 100, 'D_reserve_bytes': 20, 'D_shortfall_bytes': 70,
                     'D_exclusive_allocated_bytes': 10, 'D_relief_shortfall_bytes': 75,
                     'relief_observed_at_epoch': int(now)-5, 'retry_after_epoch': now+300}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_exclusive_space'
    assert row['phase'] == 'source_local_E_local_ready'
    assert row['metrics']['D_exclusive_allocated_bytes'] == 10
    assert row['metrics']['D_relief_shortfall_bytes'] == 75
    assert row['next_attempt_at_utc'] == storage_progress.iso(now+300)
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(now), 'tasks': [row]}, now=now)
    assert '共用' in public['tasks'][0]['reason'] and not public['tasks'][0]['complete_evidence']
    assert public['tasks'][0]['metrics']['D_exclusive_allocated_bytes'] == 10
    assert public['tasks'][0]['metrics']['relief_observed_at_epoch'] == int(now)-5


def test_queue_monitor_explains_the_retirement_turn(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
              'origin': 'cold', 'state': 'retrying', 'reason': 'd_e_owner_busy',
              'E_prepared': True, 'prepared_logical_bytes': 200, 'prepared_files': 2}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
        'observed_at_epoch': now, 'entries': [source]}))
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'state': 'retrying', 'error': 'retirement_turn'}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['reason'] == 'repack_retirement_turn'
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(now), 'tasks': [row]}, now=now)
    assert '舊版本回收' in public['tasks'][0]['reason']


def test_monitor_uses_independent_installer_foreign_head_result_only_for_exact_generation(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
              'origin': 'cold', 'state': 'retrying', 'reason': 'd_e_owner_busy',
              'E_prepared': True, 'logical_bytes': 200, 'files': 2,
              'prepared_logical_bytes': 200, 'prepared_files': 2}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    priority = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
                'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                             'state': 'retrying', 'error': 'foreign_head_wait', 'retry_after_epoch': now+300}]}
    path = root/'prepared-install-queue-status.json'; path.write_text(json.dumps(priority))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_foreign_head_wait'
    assert row['phase'] == 'source_local_E_prepared_ready'
    assert row['next_attempt_at_utc'] == storage_progress.iso(now+300)
    priority['entries'][0]['expected_manifest_sha256'] = 'b'*64
    path.write_text(json.dumps(priority))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['reason'] == 'd_e_owner_busy' and row['state'] == 'waiting'


def test_monitor_reobserves_fixed_retired_peer_wait_without_claiming_completion(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'stockagent-migration-core', 'expected_manifest_sha256': 'a'*64,
              'origin': 'cold', 'state': 'retrying', 'reason': 'repack_foreign_head_wait',
              'E_prepared': True, 'logical_bytes': 200, 'files': 2,
              'prepared_logical_bytes': 200, 'prepared_files': 2}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now-7200, 'entries': [source]}))
    outcome = {'state': 'waiting', 'encoded_bytes_remaining': 150, 'D_free_bytes': 1000,
               'D_reserve_bytes': 20, 'D_shortfall_bytes': 0}
    monkeypatch.setattr(storage_progress, '_retired_lab203_install_observation', lambda *_a: outcome)
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'source_local_E_prepared_ready'
    assert row['reason'] == 'staging_queued' and not row['complete_evidence']
    outcome.update(state='waiting_capacity', D_free_bytes=100, D_shortfall_bytes=70)
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_D_capacity'
    assert row['metrics']['D_shortfall_bytes'] == 70
    monkeypatch.setattr(storage_progress, '_retired_lab203_install_observation', lambda *_a: None)
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_foreign_head_wait'


@pytest.mark.parametrize('priority_state', ['waiting_relief_head_owner', 'waiting_head_transition'])
def test_monitor_reports_lab203_head_and_capacity_as_distinct_blockers(tmp_path, priority_state):
    root = tmp_path/'migration/source-local-exact-repack'; root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
              'origin': 'cold', 'state': 'retrying', 'reason': 'repack_foreign_head_wait',
              'E_prepared': True, 'logical_bytes': 200, 'files': 2,
              'prepared_logical_bytes': 200, 'prepared_files': 2}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
        'entries': [{'dataset': 'tw-index-futures', 'expected_manifest_sha256': 'a'*64,
                     'state': priority_state, 'relief_head_owner': 'lab203',
                     'E_temporary_authority_ready': True, 'encoded_bytes_remaining': 150,
                     'D_free_bytes': 100, 'D_reserve_bytes': 20, 'D_shortfall_bytes': 70}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_lab203_head_wait'
    assert row['phase'] == 'source_local_E_local_ready'
    assert row['metrics']['D_shortfall_bytes'] == 70
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(now), 'tasks': [row]}, now=now)
    assert 'lab203' in public['tasks'][0]['reason']
    assert not public['tasks'][0]['complete_evidence']


def test_queue_monitor_shows_active_and_skipped_items(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    sha = 'a'*64
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [
            {'dataset': 'tw-microstructure-train', 'expected_manifest_sha256': sha, 'state': 'running'},
            {'dataset': 'tw-futures-margin-sources', 'expected_manifest_sha256': 'b'*64,
             'state': 'blocked_mismatch', 'reason': 'source_or_head_mismatch'}]}))
    (root/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_repack_progress.v1', 'observed_at_epoch': 1001,
        'dataset': 'tw-microstructure-train', 'expected_manifest_sha256': sha,
        'progress': {'files_completed': 3, 'files_total': 10,
                     'logical_bytes_processed': 40, 'logical_bytes_total': 100}}))
    active, blocked = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert active['state'] == 'running' and active['completed_bytes'] == 40
    assert blocked['state'] == 'blocked' and blocked['reason'] == 'source_or_head_mismatch'
    active.update(completed_bytes=100, total_bytes=100, completed_items=10, total_items=10)
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(1001), 'tasks': [active]}, now=1001)
    assert public['tasks'][0]['progress']['percent'] == 99.99


def test_private_source_inventory_phase_and_worker_heartbeat_are_separate(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    stream = tmp_path/'migration/stream-source-repack-v1'
    root.mkdir(parents=True)
    stream.mkdir(parents=True)
    name, sha = 'local-tw-index-derivatives-ticks', 'a'*64
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
        'observed_at_epoch': 1020, 'entries': [{'dataset': name, 'origin': 'private_cold',
        'expected_manifest_sha256': sha, 'state': 'running'}]}))
    (stream/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': name,
        'expected_manifest_sha256': sha, 'observed_at_epoch': 1010,
        'progress': {'stage': 'indexing_source_inventory', 'inventory_items_completed': 30,
                     'inventory_items_total': 100}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_source_indexing'
    assert row['completed_items'] == 30 and row['total_items'] == 100 and row['completed_bytes'] is None
    assert row['observed_at_utc'] == storage_progress.iso(1010)
    assert row['activity_at_utc'] == storage_progress.iso(1020)
    (stream/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': name,
        'expected_manifest_sha256': sha, 'observed_at_epoch': 1021,
        'progress': {'stage': 'writing_E_inventory', 'inventory_items_completed': 80,
                     'inventory_items_total': 100}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_inventory'
    assert row['completed_items'] == 80 and row['total_items'] == 100 and not row['complete_evidence']
    (stream/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': name,
        'expected_manifest_sha256': sha, 'observed_at_epoch': 1021,
        'progress': {'stage': 'retiring_prepared_history'}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_prepared_history'
    assert row['completed_items'] is None and row['completed_bytes'] is None


def test_old_stream_worker_inventory_write_has_no_invented_row_count(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    stream = tmp_path/'migration/stream-source-repack-v1'
    name, sha = 'local-tw-index-derivatives-ticks', 'a'*64
    stage = stream/name/sha
    root.mkdir(parents=True)
    stage.mkdir(parents=True)
    now = queue.time.time()
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
        'observed_at_epoch': now, 'entries': [{'dataset': name, 'origin': 'private_cold',
        'expected_manifest_sha256': sha, 'state': 'running'}]}))
    (stream/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': name,
        'expected_manifest_sha256': sha, 'observed_at_epoch': now-10, 'files_total': 10,
        'logical_bytes_total': 100, 'progress': {'stage': 'streaming_source_into_E_packs',
        'files_completed': 9, 'logical_bytes_processed': 90}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_packing'
    partial = stage/('inventory-'+'c'*32+'.partial')
    partial.write_bytes(b'in progress')
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_inventory' and not row['complete_evidence']
    assert row['completed_items'] is None and row['total_items'] is None
    assert storage_progress.timestamp(row['observed_at_utc']) >= now
    os.utime(partial, (now-100, now-100))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_packing'
    partial.unlink()
    partial.symlink_to(stage/'absent')
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_packing'


def test_discovery_selects_latest_complete_copy_and_skips_other_owner(tmp_path, monkeypatch):
    workspace, cold = tmp_path/'E', tmp_path/'D'
    cold.mkdir()
    names = ['legacy-parquet', 'legacy-parquet', 'finlab']
    for i, name in enumerate(names):
        destination = workspace/f'downloads/copy-{i}'
        destination.mkdir(parents=True)
        root = workspace/f'migration/copy-{i}'
        root.mkdir(parents=True)
        (root/'intent.json').write_text(json.dumps({'source': f'/C/{name}', 'destination': str(destination)}))
        (root/'copy-result.json').write_text(json.dumps({
            'contract': 'workspace_exact_relocation_v1', 'state': 'exact_destination_readback_verified',
            'source_namespace_unchanged': True, 'files': 10, 'logical_bytes': 42, 'observed_at_epoch': i+1}))
    catalog = [{'dataset': 'legacy-parquet', 'source': '/C/legacy-parquet'},
               {'dataset': 'finlab', 'source': '/C/finlab'}]
    monkeypatch.setattr(queue, '_source_path', lambda row: __import__('pathlib').Path(row['source']))
    monkeypatch.setattr(queue, 'resolve_latest_packed', lambda *_, **_kw: SimpleNamespace(
        manifest={'source': {'files': 10, 'logical_bytes': 42}, 'archive': {}}, manifest_sha256='a'*64))
    rows = queue.discover_fixed_sources(workspace, cold, catalog=catalog)
    assert len(rows) == 1 and rows[0]['dataset'] == 'legacy-parquet'
    assert rows[0]['source'].endswith('copy-1')


def test_cold_discovery_is_bounded_to_heads_and_other_owners_are_excluded(tmp_path, monkeypatch):
    calls = []
    def resolve(_cold, name, *, require_objects):
        calls.append((name, require_objects))
        if name == 'missing':
            raise SnapshotError('not available')
        size = {'small': 3, 'large': 40, 'already': 2}[name]
        return SimpleNamespace(manifest_sha256='a'*64, manifest={
            'source': {'logical_bytes': size, 'files': 1},
            'archive': {'new_pack_layout': 'source-dataset-v1' if name == 'already' else 'old',
                        'reused_files': 0}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', resolve)
    catalog = [{'dataset': name, 'publish': True} for name in
               ('large', 'small', 'finmind', 'tej', 'already', 'missing')]
    rows = queue.discover_cold_sources(tmp_path/'E', tmp_path/'D', catalog=catalog)
    assert [row['dataset'] for row in rows] == ['small', 'large']
    assert all(not require for _, require in calls)
    assert all(name not in {'finmind', 'tej'} for name, _ in calls)
    assert queue.validate_entries(rows, tmp_path/'E') == rows
    rows[0]['source'] = str(tmp_path/'E/unrelated')
    with pytest.raises(SnapshotError, match='streaming-repack'):
        queue.validate_entries(rows, tmp_path/'E')


def test_source_local_compaction_schedules_only_materially_pinned_tails(tmp_path, monkeypatch):
    mib = 1024**2
    manifest = {'source': {'logical_bytes': 220*mib, 'files': 1747},
                'archive': {'new_pack_layout': 'source-dataset-v1', 'stored_bytes': 440*mib,
                            'reused_files': 5}}
    assert queue.source_local_compaction_worthwhile(manifest)
    assert not queue.source_local_compaction_worthwhile({**manifest, 'archive': {
        **manifest['archive'], 'stored_bytes': 230*mib}})
    assert not queue.source_local_compaction_worthwhile({**manifest, 'archive': {
        **manifest['archive'], 'reused_files': 1000}})
    assert not queue.source_local_compaction_worthwhile({**manifest, 'archive': {
        **manifest['archive'], 'stored_bytes': None}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', lambda *a, **k:
                        SimpleNamespace(manifest_sha256='a'*64, manifest=manifest))
    rows = queue.discover_cold_sources(tmp_path/'E', tmp_path/'D',
                                       catalog=[{'dataset': 'forex', 'publish': True}])
    assert len(rows) == 1 and rows[0]['expected_manifest_sha256'] == 'a'*64


def test_layout_policy_does_not_hide_reused_legacy_objects(tmp_path, monkeypatch):
    current = {'source': {}, 'archive': {'new_pack_layout': 'source-dataset-v1', 'reused_files': 3,
        'base_snapshot_id': 'prior', 'objects': [{'sha256': 'a'*64}, {'sha256': 'b'*64}]}}
    previous = {'source': {}, 'archive': {'new_pack_layout': 'path-hash-v1', 'reused_files': 0,
        'objects': [{'sha256': 'a'*64}]}}
    monkeypatch.setattr(queue, 'resolve_packed_snapshot_id',
                        lambda *_args, **_kwargs: SimpleNamespace(manifest=previous))
    assert not queue.source_layout_confirmed(tmp_path, 'fixture', current)
    previous['archive']['new_pack_layout'] = 'source-dataset-v1'
    assert queue.source_layout_confirmed(tmp_path, 'fixture', current)
    previous['archive']['new_pack_layout'] = 'path-hash-v1'
    previous['archive']['objects'] = [{'sha256': 'c'*64}]
    assert queue.source_layout_confirmed(tmp_path, 'fixture', current)
    monkeypatch.setattr(queue, 'resolve_packed_snapshot_id', lambda *_args, **_kwargs:
                        (_ for _ in ()).throw(FileNotFoundError('missing old base')))
    assert not queue.source_layout_confirmed(tmp_path, 'fixture', current)
    missing_counter = {'source': {}, 'archive': {'new_pack_layout': 'source-dataset-v1'}}
    assert not queue.source_layout_confirmed(tmp_path, 'fixture', missing_counter)
    retained = {'source': {'selection': {'retained_source_contract': 'bounded_raw_history_v1'}},
                'archive': {'new_pack_layout': 'source-dataset-v1', 'reused_files': 0,
                            'objects': [{'sha256': 'a'*64}]}}
    assert not queue.source_layout_confirmed(tmp_path, 'fixture', retained)


def test_private_discovery_enrolls_only_explicit_local_histories(tmp_path, monkeypatch):
    names = ['local-tw-index-derivatives-ticks', 'public-economic-history']
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset(names))
    calls = []
    def resolve(root, dataset, *, require_objects):
        calls.append((root, dataset, require_objects))
        return SimpleNamespace(manifest_sha256='a'*64, manifest={
            'source': {'logical_bytes': 100, 'files': 8}, 'archive': {}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', resolve)
    rows = queue.discover_private_sources(tmp_path/'E', tmp_path/'D', private=tmp_path/'private')
    assert [row['dataset'] for row in rows] == names
    assert all(row['origin'] == 'private_cold' for row in rows) and queue.validate_entries(rows, tmp_path/'E') == rows
    assert calls == [(tmp_path/'private', name, False) for name in names]
    rows[0]['dataset'] = 'some-other-private-data'
    with pytest.raises(SnapshotError, match='streaming-repack'):
        queue.validate_entries(rows, tmp_path/'E')


def test_private_discovery_rotates_existing_heads_and_excludes_other_owners(tmp_path, monkeypatch):
    private = tmp_path/'private-history'
    heads = private/'heads'
    for name in ('a', 'b', 'c', 'finmind-free', 'factorized-model', 'tw-daytrade-ledger', 'z-alias'):
        (heads/name).mkdir(parents=True)
    (heads/'redirected').symlink_to(heads/'a', target_is_directory=True)
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_ROOT', private)
    def resolve(_root, dataset, *, require_objects):
        metadata = {'source_relative': 'data_finlab/example'} if dataset == 'z-alias' else {}
        return SimpleNamespace(manifest_sha256='a'*64, manifest={
            'source': {'logical_bytes': 100, 'files': 8}, 'archive': {}, 'metadata': metadata})
    monkeypatch.setattr(queue, 'resolve_latest_packed', resolve)
    first, cursor, more = queue.discover_private_sources(tmp_path/'E', tmp_path/'D', private=private,
                                                         limit=2, with_cursor=True)
    assert [row['dataset'] for row in first] == ['a', 'b'] and cursor == 'b' and more
    second, cursor, more = queue.discover_private_sources(tmp_path/'E', tmp_path/'D', private=private,
                                                          cursor=cursor, limit=2, with_cursor=True)
    assert [row['dataset'] for row in second] == ['c', 'a']
    assert cursor == 'a' and more
    assert 'redirected' not in set(queue.stream_repack.PRIVATE_REPACK_DATASETS)
    assert 'finmind-free' not in queue.stream_repack.PRIVATE_REPACK_DATASETS
    assert not queue.stream_repack.private_repack_source_allowed(
        'z-alias', {'metadata': {'source_relative': 'data_finlab/example'}})
    assert not queue.stream_repack.private_repack_source_allowed(
        'z-alias', {'metadata': {'source_relative': 'data_tw_day_trade/service'}})


def test_private_new_head_invalidates_cached_discovery_without_manual_enrollment(tmp_path, monkeypatch):
    private = tmp_path/'private-history'
    (private/'heads'/'first').mkdir(parents=True)
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_ROOT', private)
    original = queue.private_source_enrollment_sha()
    (private/'heads'/'second').mkdir()
    assert queue.private_source_enrollment_sha() != original
    assert 'second' in queue.stream_repack.PRIVATE_REPACK_DATASETS


def test_private_discovery_reports_bad_head_and_continues(tmp_path, monkeypatch):
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'bad', 'good'}))
    def resolve(_root, name, *, require_objects):
        if name == 'bad':
            raise SnapshotError('unsupported private dependency metadata: unreviewed_binding')
        return SimpleNamespace(manifest_sha256='a'*64, manifest={
            'source': {'logical_bytes': 100, 'files': 8}, 'archive': {}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', resolve)
    errors = []
    rows = queue.discover_private_sources(tmp_path/'E', tmp_path/'D', private=tmp_path/'private',
                                          on_error=errors.append)
    assert [row['dataset'] for row in rows] == ['good']
    assert errors == [{'dataset': 'bad', 'reason': 'private_dependency_contract_unknown'}]


def test_private_discovery_error_is_visible_without_exposing_raw_name(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now = queue.time.time()
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': now, 'entries': [],
        'private_discovery_next_scan_epoch': now+900,
        'private_discovery_errors': [{'dataset': 'sensitive-private-name',
                                      'reason': 'private_dependency_contract_unknown'}]}))
    rows = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert len(rows) == 1 and rows[0]['state'] == 'blocked'
    assert rows[0]['reason'] == 'private_dependency_contract_unknown'
    assert rows[0]['next_attempt_at_utc'] == storage_progress.iso(now+900)
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(now), 'tasks': rows}, now=now)
    assert 'sensitive-private-name' not in json.dumps(public)
    assert public['tasks'][0]['reason_code'] == 'private_dependency_contract_unknown'


def test_private_state_pin_hint_is_exact_and_only_delays(tmp_path, monkeypatch):
    digest = 'a'*64
    name = 'parent-state-123456789abc'
    latest = SimpleNamespace(manifest_sha256=digest, manifest={'snapshot_id': 'state-v1'})
    binding = {'dataset': name, 'snapshot_id': 'state-v1', 'manifest_sha256': digest}
    parent = SimpleNamespace(manifest={'metadata': {'consistent_sqlite_bindings': json.dumps([binding])}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', lambda *_args, **_kwargs: parent)
    assert queue.private_state_pin_hint(tmp_path, name, latest, {})
    binding['manifest_sha256'] = 'b'*64
    parent.manifest['metadata']['consistent_sqlite_bindings'] = json.dumps([binding])
    assert not queue.private_state_pin_hint(tmp_path, name, latest, {})
    assert not queue.private_state_pin_hint(tmp_path, 'parent', latest, {})


def test_private_pinned_state_does_not_block_another_dataset(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    first = {'dataset': 'parent-state-123456789abc', 'expected_manifest_sha256': 'a'*64,
             'origin': 'private_cold', 'dependency_pin_hint': True,
             'source': str(tmp_path/'migration/stream-source-repack-v1/parent-state-123456789abc'/('a'*64)),
             'files': 2, 'logical_bytes': 100}
    other = {'dataset': 'other', 'expected_manifest_sha256': 'b'*64, 'origin': 'cold',
             'source': str(tmp_path/'migration/stream-source-repack-v1/other'/('b'*64)),
             'files': 2, 'logical_bytes': 100}
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({first['dataset']}))
    monkeypatch.setattr(queue.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(queue.workspace_volume, 'check', lambda **_kwargs: None)
    monkeypatch.setattr(queue.workspace_volume, 'admit', lambda *_args, **_kwargs: None)
    monkeypatch.setattr(queue, 'load_queue', lambda *_args: [first, other])
    monkeypatch.setattr(queue, 'testing_disposition', lambda: None)
    monkeypatch.setattr(queue, 'private_source_enrollment_sha', lambda: 'f'*64)
    monkeypatch.setattr(queue.staged_head_capacity_relief, 'E_only_pending_generations', lambda *_args: set())
    monkeypatch.setattr(queue, 'load_generation_holds', lambda: set())
    monkeypatch.setattr(queue, 'prepared_installer_busy', lambda *_args: False)
    now = queue.time.time()
    (root/'discovered-sources.json').write_text(json.dumps({
        'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': now+600,
        'private_source_enrollment_sha256': 'f'*64, 'testing_disposition_identity_sha256': None,
        'extra_has_more': False, 'entries': []}))
    commands = []
    def busy(command, **_kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=1, stderr='owner busy', stdout='')
    monkeypatch.setattr(queue, 'run_observed', busy)
    assert queue.main() == 0
    assert commands[0][commands[0].index('--dataset')+1] == 'other'
    row, = [item for item in json.loads((root/'queue-status.json').read_text())['entries']
            if item['dataset'] == first['dataset']]
    assert row['reason'] == 'relief_pinned' and row['state'] == 'retrying'


def test_private_enrollment_change_invalidates_cached_discovery(monkeypatch):
    first = queue.private_source_enrollment_sha()
    disposition = {'identity_sha256': 'a'*64}
    cache = {'contract': queue.DISCOVERY_CONTRACT, 'next_scan_epoch': 2000,
             'private_source_enrollment_sha256': first, 'testing_disposition_identity_sha256': 'a'*64}
    assert queue.discovery_is_fresh(cache, 1000, disposition=disposition)
    assert not queue.discovery_is_fresh(cache, 1000, disposition={'identity_sha256': 'b'*64})
    assert not queue.discovery_is_fresh({**cache, 'next_scan_epoch': float('nan')}, 1000, disposition=disposition)
    assert not queue.discovery_is_fresh(cache, 2000, disposition=disposition)
    monkeypatch.setattr(queue.stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    assert queue.private_source_enrollment_sha() != first
    assert not queue.discovery_is_fresh(cache, 1000, disposition=disposition)


def test_discard_policy_skips_only_reviewed_test_heads(tmp_path, monkeypatch):
    cold, workspace = tmp_path/'D', tmp_path/'E'
    for name in ('a-benchmark', 'b-research'):
        (cold/'heads'/name).mkdir(parents=True)
    def resolve(_cold, name, **_kwargs):
        source = 'markets/benchmarks/speed' if name == 'a-benchmark' else 'markets/research/run'
        return SimpleNamespace(manifest_sha256='a'*64, manifest={
            'source': {'logical_bytes': 10, 'files': 1}, 'archive': {},
            'metadata': {'artifact_relative_root': source}})
    monkeypatch.setattr(queue, 'resolve_latest_packed', resolve)
    policy = {'contract': 'explicit-testing-artifact-discard-v1',
              'targets': [{'base': 'artifact', 'relative': 'markets/benchmarks/speed'}]}
    rows, _ = queue.discover_other_cold_sources(workspace, cold, catalog=[], disposition=policy)
    assert [row['dataset'] for row in rows] == ['b-research']
    rows, _ = queue.discover_other_cold_sources(workspace, cold, catalog=[], disposition=None)
    assert [row['dataset'] for row in rows] == ['a-benchmark', 'b-research']


def test_missing_private_authority_does_not_stop_public_discovery(tmp_path, monkeypatch):
    monkeypatch.setattr(queue, 'private_history_root', lambda *_a, **_k: (_ for _ in ()).throw(
        SnapshotError('private authority unavailable')))
    monkeypatch.setattr(queue, 'version_policy', lambda *_a: {})
    assert queue.discover_private_sources(tmp_path/'E', tmp_path/'D') == []


def test_uncatalogued_heads_rotate_in_bounded_batches(tmp_path, monkeypatch):
    cold, workspace = tmp_path/'D', tmp_path/'E'
    for name in ('a-one', 'b-two', 'c-three', 'finlab-hidden', 'factorized-work', 'registered'):
        (cold/'heads'/name).mkdir(parents=True)
    monkeypatch.setattr(queue, 'resolve_latest_packed', lambda *_a, **_k: SimpleNamespace(
        manifest_sha256='a'*64, manifest={'source': {'logical_bytes': 10, 'files': 1}, 'archive': {}}))
    catalog = [{'dataset': 'registered'}]
    first, cursor = queue.discover_other_cold_sources(workspace, cold, catalog=catalog, limit=2)
    assert [row['dataset'] for row in first] == ['a-one', 'b-two'] and cursor == 'b-two'
    second, _ = queue.discover_other_cold_sources(workspace, cold, catalog=catalog, cursor=cursor, limit=2)
    assert {row['dataset'] for row in second} == {'c-three', 'a-one'}
    assert all(row['dataset'] not in {'registered', 'finlab-hidden', 'factorized-work'} for row in second)


def test_blocked_batch_rotates_before_later_retries():
    assert queue.next_discovery_scan(1000, [1300, 4600]) == 1030
    assert queue.next_discovery_scan(1000, [1010]) == 1010
    assert queue.next_discovery_scan(1000, []) == 1001


def test_queue_monitor_shows_streaming_cold_source_progress(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    stream = tmp_path/'migration/stream-source-repack-v1'
    root.mkdir(parents=True)
    stream.mkdir(parents=True)
    sha = 'a'*64
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
                     'state': 'running', 'origin': 'cold', 'logical_bytes': 100, 'files': 10}]}))
    (stream/'queue-progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'observed_at_epoch': 1001,
        'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
        'files_total': 10, 'logical_bytes_total': 100,
        'progress': {'files_completed': 3, 'logical_bytes_processed': 40}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['completed_bytes'] == 40 and row['total_bytes'] == 100
    assert row['completed_items'] == 3 and row['total_items'] == 10
    progress_path = stream/'queue-progress.json'
    progress = json.loads(progress_path.read_text())
    progress['progress'] = {'stage': 'installing_D_objects', 'objects_completed': 2, 'objects_total': 5,
                            'encoded_bytes_installed': 60, 'encoded_bytes_total': 150}
    progress_path.write_text(json.dumps(progress))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_D_installing' and row['completed_bytes'] == 60
    assert row['total_bytes'] == 150 and row['completed_items'] == 2 and row['total_items'] == 5
    # A worker already running older code can still report its real install
    # stage from the durable metadata journal, without reading payload bytes.
    progress['progress'] = {'stage': 'streaming_source_into_E_packs', 'files_completed': 9,
                            'logical_bytes_processed': 90}
    progress_path.write_text(json.dumps(progress))
    stage = stream/'openbb-compact'/sha
    stage.mkdir(parents=True)
    prepared = {'contract': 'stockagent.stream_source_local_repack.v1.prepared',
        'dataset': 'openbb-compact', 'old_manifest_sha256': sha, 'identity_sha256': 'b'*64,
        'packages': [{'sha256': 'c'*64, 'bytes': 30}, {'sha256': 'd'*64, 'bytes': 40}],
        'inventory': {'sha256': 'e'*64, 'bytes': 5}}
    (stage/'prepared.json').write_text(json.dumps(prepared))
    (stage/'install.json').write_text(json.dumps({'contract': 'stockagent.stream_source_local_repack.v1.install',
        'dataset': 'openbb-compact', 'old_manifest_sha256': sha, 'prepared_sha256': 'b'*64,
        'state': 'installing', 'installed': [{'sha256': 'c'*64}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_D_installing' and row['completed_bytes'] == 30
    assert row['total_bytes'] == 75 and row['total_items'] == 3


def test_full_D_reports_E_prepared_as_blocked_not_finished(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                     'state': 'retrying', 'origin': 'cold', 'reason': 'repack_D_capacity',
                     'logical_bytes': 100, 'files': 10, 'E_prepared': True,
                     'prepared_logical_bytes': 100, 'prepared_files': 10,
                     'D_required_bytes': 80, 'D_observed_free_bytes': 35,
                     'D_reserve_bytes': 30, 'D_shortfall_bytes': 75,
                     'D_capacity_observed_epoch': 999}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['phase'] == 'source_local_E_prepared'
    assert row['completed_bytes'] == 100 and row['total_bytes'] == 100
    assert row['metrics']['D_shortfall_bytes'] == 75 and row['metrics']['D_capacity_observed_epoch'] == 999
    assert not row['complete_evidence']
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(1001), 'tasks': [row]}, now=1001)['tasks'][0]
    assert public['progress']['percent'] == 100 and public['state'] == 'blocked'
    assert public['complete_evidence'] is False and public['eta']['completion_at_utc'] is None


def test_capacity_observation_is_bound_to_the_fixed_source_generation():
    value = {'contract': 'stockagent.stream_source_local_repack.v1.progress',
        'dataset': 'fixture', 'expected_manifest_sha256': 'a'*64, 'observed_at_epoch': 123,
        'progress': {'stage': 'waiting_D_capacity', 'encoded_bytes_remaining': 80,
                     'D_free_bytes': 35, 'D_reserve_bytes': 30}}
    assert queue.capacity_observation(value, 'fixture', 'a'*64)['D_shortfall_bytes'] == 75
    assert queue.capacity_observation(value, 'fixture', 'b'*64) == {}


def test_E_prepared_capacity_uses_current_D_measurement_and_fixed_metadata(tmp_path):
    sha = 'a'*64
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': 1000,
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
                     'state': 'retrying', 'origin': 'cold', 'reason': 'repack_D_capacity',
                     'logical_bytes': 100, 'files': 10, 'E_prepared': True,
                     'prepared_logical_bytes': 100, 'prepared_files': 10}]}))
    stage = tmp_path/'migration/stream-source-repack-v1/openbb-compact'/sha
    stage.mkdir(parents=True)
    prepared = {'contract': queue.stream_repack.CONTRACT+'.prepared', 'dataset': 'openbb-compact',
                'old_manifest_sha256': sha, 'destination_independent_readback': False,
                'source_member_sha_streamed': True, 'raw_extraction_tree_created': False,
                'packages': [{'bytes': 80}], 'inventory': {'bytes': 5}}
    prepared['identity_sha256'] = queue.stream_repack._identity(prepared)
    (stage/'prepared.json').write_text(json.dumps(prepared))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['_D_encoded_remaining_bytes'] == 85
    from scripts.run_stream_packed_repack import D_INSTALL_RESERVE
    cold = {'state': 'measured', 'observed_at_epoch': 1000.7, 'available_bytes': D_INSTALL_RESERVE+10}
    storage_progress._apply_stream_D_capacity([row], cold, now=1001)
    assert row['metrics']['D_shortfall_bytes'] == 75 and row['metrics']['D_capacity_live'] == 1
    assert row['metrics']['D_capacity_observed_epoch'] == 1000
    row['_D_reserve_bytes'] = 12 * 1024**3
    storage_progress._apply_stream_D_capacity([row], cold, now=1001)
    assert row['metrics']['D_reserve_bytes'] == 12 * 1024**3
    assert row['metrics']['D_shortfall_bytes'] == 0
    stale = {**cold, 'observed_at_epoch': 0}
    another = {**row, 'metrics': {}}
    storage_progress._apply_stream_D_capacity([another], stale, now=100000)
    assert another['metrics'] == {}
    status_path = root/'queue-status.json'
    pending = json.loads(status_path.read_text())
    pending['entries'][0]['reason'] = 'd_e_owner_busy'
    status_path.write_text(json.dumps(pending))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_prepared_ready' and row['reason'] == 'd_e_owner_busy'
    assert row['_D_encoded_remaining_bytes'] == 85
    storage_progress._apply_stream_D_capacity([row], cold, now=1001)
    assert row['metrics']['D_shortfall_bytes'] == 75 and row['reason'] == 'd_e_owner_busy'


def test_foreign_head_E_view_is_visible_without_claiming_D_handoff(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now, sha = queue.time.time(), 'a'*64
    source = {'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
              'state': 'retrying', 'origin': 'cold', 'reason': 'repack_foreign_head_wait',
              'logical_bytes': 100, 'files': 10, 'E_prepared': True,
              'prepared_logical_bytes': 100, 'prepared_files': 10}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    status = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
              'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
                           'state': 'waiting_head_transition', 'E_temporary_authority_ready': True}]}
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_foreign_head_wait'
    assert row['phase'] == 'source_local_E_local_ready' and not row['complete_evidence']
    status['entries'][0]['expected_manifest_sha256'] = 'b'*64
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_prepared_ready'


def test_fresh_ready_installer_replaces_old_foreign_wait_for_same_generation(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now, sha = queue.time.time(), 'a'*64
    source = {'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
              'state': 'retrying', 'origin': 'cold', 'reason': 'repack_foreign_head_wait',
              'logical_bytes': 100, 'files': 10, 'E_prepared': True,
              'prepared_logical_bytes': 100, 'prepared_files': 10, 'retry_after_epoch': now+300}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    status = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
              'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': sha, 'state': 'waiting',
                           'encoded_bytes_remaining': 90, 'D_free_bytes': 200, 'D_reserve_bytes': 30,
                           'D_shortfall_bytes': 0}]}
    path = root/'prepared-install-queue-status.json'
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'source_local_E_prepared_ready'
    assert row['reason'] == 'staging_queued' and row['next_attempt_at_utc'] is None
    assert not row['complete_evidence']
    status['entries'][0]['expected_manifest_sha256'] = 'b'*64
    path.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_foreign_head_wait'


def test_ready_v3_installer_replaces_old_dedup_wait_without_claiming_completion(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now, sha = queue.time.time(), 'a'*64
    source = {'dataset': 'openbb-compact', 'expected_manifest_sha256': sha,
              'state': 'retrying', 'origin': 'cold', 'reason': 'source_duplicate_bloat',
              'logical_bytes': 100, 'files': 10}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    status = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
              'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': sha, 'state': 'waiting',
                           'encoded_bytes_remaining': 90, 'D_free_bytes': 200, 'D_reserve_bytes': 30,
                           'D_shortfall_bytes': 0}]}
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'source_local_E_prepared_ready'
    assert row['reason'] == 'staging_queued' and not row['complete_evidence']
    status['entries'][0].update(state='retrying', error='owner_busy', retry_after_epoch=now+60)
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['reason'] == 'd_e_owner_busy'
    assert row['next_attempt_at_utc'] == storage_progress.iso(now+60)


def test_independent_prepared_install_overrides_stale_waiting_queue_only_for_exact_generation(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    stream = tmp_path/'migration/stream-source-repack-v1'
    root.mkdir(parents=True)
    stream.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
              'state': 'retrying', 'origin': 'cold', 'reason': 'repack_D_capacity',
              'logical_bytes': 100, 'files': 10, 'E_prepared': True,
              'prepared_logical_bytes': 100, 'prepared_files': 10,
              'D_required_bytes': 95, 'D_shortfall_bytes': 20,
              'D_capacity_observed_epoch': now-5, 'retry_after_epoch': now+20}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    status = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
              'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                           'state': 'running', 'encoded_bytes_remaining': 90}]}
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    (stream/'prepared-install-openbb-compact.progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': 'openbb-compact',
        'expected_manifest_sha256': 'a'*64, 'observed_at_epoch': now,
        'progress': {'stage': 'streaming_D_object', 'objects_completed': 1, 'objects_total': 3,
                     'encoded_bytes_installed': 20, 'encoded_bytes_streamed': 25, 'encoded_bytes_total': 90}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_D_installing'
    assert row['completed_bytes'] == 25 and row['total_bytes'] == 90 and row['_priority_install_owner'] is True
    assert not row['metrics'] and row['reason'] is None and row['next_attempt_at_utc'] is None
    validation = {'stage': 'validating_D_inventory', 'inventory_items_completed': 40,
                  'inventory_items_total': 100}
    (stream/'prepared-install-openbb-compact.progress.json').write_text(json.dumps({
        'contract': 'stockagent.stream_source_local_repack.v1.progress', 'dataset': 'openbb-compact',
        'expected_manifest_sha256': 'a'*64, 'observed_at_epoch': now, 'progress': validation}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_D_validating' and row['completed_items'] == 40
    assert row['total_items'] == 100 and row['completed_bytes'] is None
    status['entries'][0] = {'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                            'state': 'waiting_capacity', 'E_temporary_authority_ready': True,
                            'encoded_bytes_remaining': 90, 'D_free_bytes': 20,
                            'D_reserve_bytes': 30, 'D_shortfall_bytes': 100}
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['phase'] == 'source_local_E_local_ready'
    assert row['completed_bytes'] == 100 and row['total_bytes'] == 100
    status['entries'][0]['expected_manifest_sha256'] = 'b'*64
    (root/'prepared-install-queue-status.json').write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['phase'] == 'source_local_E_prepared'


def test_E_first_relief_reports_real_progress_interruption_and_install_wait(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    stage = tmp_path/'migration/stream-source-repack-v1/openbb-compact'/('a'*64)
    root.mkdir(parents=True)
    stage.mkdir(parents=True)
    now = queue.time.time()
    entry = {'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64, 'origin': 'cold',
             'state': 'retrying', 'reason': 'repack_D_capacity', 'E_prepared': True,
             'logical_bytes': 100, 'files': 10, 'prepared_logical_bytes': 100, 'prepared_files': 10}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [entry]}))
    status = {'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
              'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                           'state': 'running_relief'}]}
    priority = root/'prepared-install-queue-status.json'
    priority.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_E_first_preparing'
    journal = {'contract': 'stockagent.staged_current_head_capacity_relief.v1',
               'dataset': 'openbb-compact', 'old_manifest_sha256': 'a'*64,
               'state': 'removing', 'removed_count': 64, 'planned_object_count': 100}
    path = stage/'current-head-capacity-relief.json'
    path.write_text(json.dumps(journal))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_E_first_retiring'
    assert row['completed_items'] == 64 and row['total_items'] == 100
    assert row['metrics']['D_old_head_temporarily_unavailable'] == 1
    status['entries'][0]['state'] = 'retrying'
    priority.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['reason'] == 'repack_E_first_interrupted'
    journal.update(state='completed', removed_count=100)
    path.write_text(json.dumps(journal))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'waiting' and row['phase'] == 'source_local_E_first_install_waiting'
    assert row['reason'] == 'repack_E_first_install_waiting' and not row['complete_evidence']
    status['entries'][0]['error'] = 'owner_busy'
    priority.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['reason'] == 'repack_E_first_owner_busy'
    status['entries'][0]['state'] = 'running'
    priority.write_text(json.dumps(status))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_D_installing'

    # The original source owner may resume this E-only installation before
    # the independent installer. A completed relief is not a waiting state
    # once the matching original generation is actually streaming into D.
    status['entries'][0]['state'] = 'waiting_original_owner'
    priority.write_text(json.dumps(status))
    entry.update(state='running', reason='repack_E_first_install_waiting')
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [entry]}))
    progress_path = tmp_path/'migration/stream-source-repack-v1/queue-progress.json'
    progress_path.write_text(json.dumps({'contract': 'stockagent.stream_source_local_repack.v1.progress',
        'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64, 'observed_at_epoch': now,
        'progress': {'stage': 'streaming_D_object', 'encoded_bytes_installed': 20,
                     'encoded_bytes_streamed': 40, 'encoded_bytes_total': 100,
                     'objects_completed': 1, 'objects_total': 5}}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'running' and row['phase'] == 'source_local_D_installing'
    assert (row['completed_bytes'], row['total_bytes']) == (40, 100)
    assert row['metrics']['D_old_head_temporarily_unavailable'] == 1
    assert not row['complete_evidence']


def test_priority_install_completion_requires_its_matching_publication_receipt(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now = queue.time.time()
    entry = {'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
             'state': 'retrying', 'origin': 'cold', 'reason': 'repack_D_capacity',
             'logical_bytes': 100, 'files': 10, 'E_prepared': True,
             'prepared_logical_bytes': 100, 'prepared_files': 10}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [entry]}))
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64, 'state': 'completed'}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and not row['complete_evidence']
    directory = root/'openbb-compact'
    directory.mkdir()
    (directory/('c'*64+'.json')).write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'openbb-compact',
        'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'c'*64}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['complete_evidence']
    assert row['phase'] == 'source_local_D_published'


def test_priority_capacity_observation_replaces_an_earlier_lock_wait(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    now = queue.time.time()
    source = {'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
              'state': 'retrying', 'origin': 'cold', 'reason': 'd_e_owner_busy',
              'logical_bytes': 100, 'files': 10}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status',
                                                     'observed_at_epoch': now, 'entries': [source]}))
    (root/'prepared-install-queue-status.json').write_text(json.dumps({
        'contract': 'stockagent.prepared_stream_install_queue.v1', 'observed_at_epoch': now,
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                     'state': 'waiting_capacity', 'encoded_bytes_remaining': 80,
                     'D_free_bytes': 40, 'D_reserve_bytes': 30, 'D_shortfall_bytes': 70}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'blocked' and row['phase'] == 'source_local_E_prepared'
    assert row['reason'] == 'repack_D_capacity' and row['metrics']['D_shortfall_bytes'] == 70
    assert row['completed_bytes'] == row['total_bytes'] == 100 and not row['complete_evidence']


def test_published_source_does_not_display_a_waiting_phase(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.status', 'observed_at_epoch': queue.time.time(),
        'entries': [{'dataset': 'openbb-compact', 'expected_manifest_sha256': 'a'*64,
                     'state': 'published', 'origin': 'cold'}]}))
    row, = storage_progress.source_local_repack_queue_tasks(tmp_path, workspace_checked=True)
    assert row['state'] == 'completed' and row['phase'] == 'source_local_D_published'


def test_prepared_history_queue_preserves_head_and_honors_retry_time(tmp_path, monkeypatch):
    sha = 'a'*64
    row = {'dataset': 'fixture', 'expected_manifest_sha256': sha, 'source': str(tmp_path)}
    calls = []
    def run(command, *, on_heartbeat, timeout):
        calls.append(command)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': 'fixture', 'old_manifest_sha256': sha, 'current_D_head_preserved': True,
            'state': 'completed', 'versions_removed': 1}))
    monkeypatch.setattr(queue, 'run_observed', run)
    result = queue.run_prepared_history_retirement(row, on_heartbeat=lambda: None)
    assert result['versions_removed'] == 1 and '--prepared-only' in calls[0]
    (tmp_path/'prepared-history-sweep.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_prepared_history.v1', 'dataset': 'fixture',
        'old_manifest_sha256': sha, 'history_sweep_next_epoch': queue.time.time()+60,
        'history_retirement': {'versions_removed': 1}}))
    assert queue.run_prepared_history_retirement(row, on_heartbeat=lambda: None)['versions_removed'] == 1
    assert len(calls) == 1


def test_private_prepared_history_uses_private_retirement_scope(tmp_path, monkeypatch):
    row = {'dataset': 'local-tw-index-derivatives-ticks', 'origin': 'private_cold',
           'expected_manifest_sha256': 'a'*64, 'source': str(tmp_path)}
    def run(command, *, on_heartbeat, timeout):
        assert '--prepared-only' in command and '--private-source' in command
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'dataset': row['dataset'], 'old_manifest_sha256': row['expected_manifest_sha256'],
            'current_D_head_preserved': True, 'state': 'completed', 'versions_removed': 1}))
    monkeypatch.setattr(queue, 'run_observed', run)
    assert queue.run_prepared_history_retirement(row, on_heartbeat=lambda: None)['versions_removed'] == 1


def test_sealed_source_is_recorded_after_retryable_post_build_failure(tmp_path):
    expected = 'a'*64
    inventory = tmp_path/('inventory-'+'b'*64+'.jsonl.gz')
    inventory.write_bytes(b'x')
    prepared = {'contract': queue.stream_repack.CONTRACT+'.prepared', 'dataset': 'fixture',
        'old_manifest_sha256': expected, 'destination_independent_readback': False,
        'source_member_sha_streamed': True, 'raw_extraction_tree_created': False,
        'packages': [], 'files': 7, 'logical_bytes': 12,
        'inventory': {'name': inventory.name, 'bytes': 1,
                      'E_signature': queue.stream_repack._signature(inventory)}}
    prepared['identity_sha256'] = queue.stream_repack._identity(prepared)
    (tmp_path/'prepared.json').write_text(json.dumps(prepared))
    row = {'origin': 'cold', 'source': str(tmp_path), 'dataset': 'fixture',
           'expected_manifest_sha256': expected}
    assert queue.prepared_source_observation(row) == {
        'E_prepared': True, 'prepared_files': 7, 'prepared_logical_bytes': 12}
    prepared['files'] = 8  # An altered sealed generation cannot be enrolled.
    (tmp_path/'prepared.json').write_text(json.dumps(prepared))
    assert queue.prepared_source_observation(row) == {}


def test_new_queue_owner_does_not_keep_an_abandoned_running_status():
    sha = 'a'*64
    assert queue.reusable_prior({('fixture', sha): {'state': 'running', 'expected_manifest_sha256': sha}},
                                'fixture', sha) == {}
    retry = {'state': 'retrying', 'expected_manifest_sha256': sha, 'retry_after_epoch': 100}
    assert queue.reusable_prior({('fixture', sha): retry}, 'fixture', sha) == retry
    assert queue.reusable_prior({('fixture', sha): retry}, 'fixture', 'b'*64) == {}


def test_retirement_queue_enrolls_only_matching_stream_receipts(tmp_path, monkeypatch):
    root = tmp_path/'repack'
    path = root/'openbb-compact'/('b'*64+'.json')
    path.parent.mkdir(parents=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
               'dataset': 'openbb-compact', 'old_manifest_sha256': 'a'*64,
               'new_manifest_sha256': 'b'*64, 'logical_bytes': 100, 'old_release_removed': False}
    path.write_text(json.dumps(receipt))
    assert queue.discover_pending_retirements(root)[0]['dataset'] == 'openbb-compact'
    def failed(*_args, **_kwargs):
        return SimpleNamespace(returncode=1, stderr='publication owner busy', stdout='')
    monkeypatch.setattr(queue, 'run_observed', failed)
    queue.run_one_retirement(root)
    status = json.loads((root/'retirement-queue-status.json').read_text())
    assert status['entries'][0]['state'] == 'waiting_retry'
    assert status['entries'][0]['reason'] == 'd_e_owner_busy'
    receipt['old_release_removed'] = True
    path.write_text(json.dumps(receipt))
    assert queue.discover_pending_retirements(root)[0]['mode'] == 'history'
    receipt['history_sweep_next_epoch'] = queue.time.time() + 3600
    path.write_text(json.dumps(receipt))
    assert queue.discover_pending_retirements(root) == []


def test_retirement_queue_prioritizes_large_direct_sources_before_history(tmp_path):
    rows = [('small', 10, False), ('large', 50, False), ('middle', 20, False),
            ('history-large', 40, True), ('history-small', 5, True)]
    for index, (name, size, removed) in enumerate(rows):
        digest = f'{index:064x}'
        path = tmp_path/name/(digest+'.json')
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source',
            'source_mode': 'D_streamed_to_E_dataset_pack', 'dataset': name,
            'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': digest,
            'logical_bytes': size, 'old_release_removed': removed}))
    pending = queue.discover_pending_retirements(tmp_path)
    assert [row['dataset'] for row in pending] == [
        'large', 'middle', 'small', 'history-small', 'history-large']


def test_two_retirement_schedulers_share_one_nonblocking_owner(tmp_path, monkeypatch):
    lock_path = tmp_path/'retirement-queue-owner.lock'
    lock_path.touch()
    called = []
    monkeypatch.setattr(queue, '_run_one_retirement_owned', lambda _root, **_kwargs: called.append(True) or True)
    with lock_path.open('r+') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert queue.run_one_retirement(tmp_path) is False and not called
    assert queue.run_one_retirement(tmp_path) is True and called == [True]


def test_retirement_records_the_specific_replication_owner_blocker(tmp_path, monkeypatch):
    path = tmp_path/'fixture'/('b'*64+'.json')
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': 'fixture', 'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64,
        'logical_bytes': 100, 'old_release_removed': False}))
    monkeypatch.setattr(queue, 'run_observed', lambda *_a, **_kw: SimpleNamespace(returncode=1,
        stderr='SnapshotError: retirement owner busy: source-replication-owner.lock', stdout=''))
    queue.run_one_retirement(tmp_path)
    status = json.loads((tmp_path/'retirement-queue-status.json').read_text())
    assert status['entries'][0]['reason'] == 'retirement_source_replication_busy'


def test_retirement_records_shared_publication_contention(tmp_path, monkeypatch):
    path = tmp_path/'fixture'/('b'*64+'.json')
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
        'dataset': 'fixture', 'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64,
        'logical_bytes': 100, 'old_release_removed': False}))
    monkeypatch.setattr(queue, 'run_observed', lambda *_a, **_kw: SimpleNamespace(returncode=1,
        stderr='SnapshotError: retirement owner busy: publication', stdout=''))
    assert queue.run_one_retirement(tmp_path)
    status = json.loads((tmp_path/'retirement-queue-status.json').read_text())
    assert status['last_attempt']['owner_lock'] == 'publish_retention_global'
    assert status['last_attempt']['reason'] == 'd_e_owner_busy'


def test_history_retirement_rotates_past_recently_served_dataset(tmp_path, monkeypatch):
    paths = {}
    for index, name in enumerate(('first', 'second', 'third'), 1):
        path = tmp_path/name/(str(index)*64+'.json')
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
            'dataset': name, 'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': str(index)*64,
            'logical_bytes': index*100, 'old_release_removed': True}))
        paths[name] = path
    selected = []
    def completed(command, **_kw):
        name = command[command.index('--dataset')+1]
        selected.append(name)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps(
            {'state': 'completed', 'dataset': name, 'object_bytes': 0, 'versions_removed': 0}))
    monkeypatch.setattr(queue, 'run_observed', completed)
    queue.run_one_retirement(tmp_path)
    first = json.loads(paths['first'].read_text())
    first['history_sweep_next_epoch'] = queue.time.time()+3600
    paths['first'].write_text(json.dumps(first))
    queue.run_one_retirement(tmp_path)
    first['history_sweep_next_epoch'] = 0
    paths['first'].write_text(json.dumps(first))
    second = json.loads(paths['second'].read_text())
    second['history_sweep_next_epoch'] = queue.time.time()+3600
    paths['second'].write_text(json.dumps(second))
    queue.run_one_retirement(tmp_path)
    assert selected == ['first', 'second', 'third']


def test_newly_retired_head_gets_one_history_pass_before_rotation(tmp_path, monkeypatch):
    for name, size in [('small', 10), ('recent', 1000)]:
        path = tmp_path/name/('b'*64+'.json')
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'source_mode': 'D_streamed_to_E_dataset_pack',
            'dataset': name, 'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64,
            'logical_bytes': size, 'old_release_removed': True}))
    (tmp_path/'retirement-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.retirement-status',
        'entries': [{'dataset': 'recent', 'old_manifest_sha256': 'a'*64,
                     'mode': 'direct', 'state': 'completed'}]}))
    selected = []
    def completed(command, **_kw):
        name = command[command.index('--dataset')+1]
        selected.append(name)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps(
            {'state': 'completed', 'dataset': name, 'object_bytes': 0, 'versions_removed': 0}))
    monkeypatch.setattr(queue, 'run_observed', completed)
    queue.run_one_retirement(tmp_path)
    queue.run_one_retirement(tmp_path)
    assert selected == ['recent', 'small']


def test_direct_retirement_precedes_smaller_historical_sweeps(tmp_path):
    root = tmp_path/'repack'
    for dataset, old, new, size, removed in [
        ('many-small-old-versions', 'a'*64, 'b'*64, 10, True),
        ('large-new-duplicate', 'c'*64, 'd'*64, 1000, False),
    ]:
        directory = root/dataset
        directory.mkdir(parents=True)
        (directory/(new+'.json')).write_text(json.dumps({
            'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source',
            'source_mode': 'D_streamed_to_E_dataset_pack', 'dataset': dataset,
            'old_manifest_sha256': old, 'new_manifest_sha256': new,
            'logical_bytes': size, 'old_release_removed': removed}))
    pending = queue.discover_pending_retirements(root)
    assert [(row['dataset'], row['mode']) for row in pending] == [
        ('large-new-duplicate', 'direct'), ('many-small-old-versions', 'history')]


def test_prepared_history_runs_on_retirement_owner_without_blocking_build(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    sha = 'a'*64
    stage = root.parent/'stream-source-repack-v1'/'fixture'/sha
    stage.mkdir(parents=True)
    source = {'dataset': 'fixture', 'expected_manifest_sha256': sha, 'origin': 'cold',
              'state': 'retrying', 'reason': 'repack_D_capacity', 'E_prepared': True}
    status = {'contract': queue.CONTRACT+'.status', 'entries': [source]}
    (root/'queue-status.json').write_text(json.dumps(status))
    monkeypatch.setattr(queue.stream_repack, '_read_prepared',
                        lambda path, name, expected: {'logical_bytes': 100})
    seen = []
    def completed(command, **_kwargs):
        seen.append(command)
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps({
            'state': 'completed', 'dataset': 'fixture', 'old_manifest_sha256': sha,
            'current_D_head_preserved': True, 'object_bytes': 50, 'versions_removed': 1}))
    monkeypatch.setattr(queue, 'run_observed', completed)
    assert queue.run_one_retirement(root)
    assert '--prepared-only' in seen[0] and 'retire_superseded_repack_history.py' in seen[0][1]
    recorded = json.loads((root/'retirement-queue-status.json').read_text())['entries'][0]
    assert recorded['mode'] == 'prepared_history' and recorded['state'] == 'completed'
    assert recorded['deleted_bytes'] == 50
    # An older in-flight build can still run an inline sweep; never race it.
    source['state'] = 'running'
    (root/'queue-status.json').write_text(json.dumps(status))
    assert queue.discover_prepared_history_retirements(root) == []


def test_prepared_history_respects_exact_generation_and_retry(tmp_path, monkeypatch):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    sha = 'a'*64
    stage = root.parent/'stream-source-repack-v1'/'fixture'/sha
    stage.mkdir(parents=True)
    entry = {'dataset': 'fixture', 'expected_manifest_sha256': sha, 'origin': 'cold',
             'state': 'retrying', 'E_prepared': True}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status', 'entries': [entry]}))
    monkeypatch.setattr(queue.stream_repack, '_read_prepared',
                        lambda path, name, expected: {'logical_bytes': 100})
    (stage/'prepared-history-sweep.json').write_text(json.dumps({
        'contract': 'stockagent.source_local_prepared_history.v1', 'dataset': 'fixture',
        'old_manifest_sha256': sha, 'history_sweep_next_epoch': queue.time.time()+3600}))
    assert queue.discover_prepared_history_retirements(root) == []
    (stage/'prepared-history-sweep.json').unlink()
    monkeypatch.setattr(queue, 'run_observed', lambda *_a, **_kw: SimpleNamespace(
        returncode=0, stderr='', stdout=json.dumps({'state': 'completed', 'dataset': 'fixture',
            'old_manifest_sha256': sha, 'current_D_head_preserved': False, 'object_bytes': 50})))
    assert queue.run_one_retirement(root)
    result = json.loads((root/'retirement-queue-status.json').read_text())['entries'][0]
    assert result['state'] == 'waiting_retry' and result['reason'] == 'retirement_metadata_retry'


def test_prepared_history_skips_a_published_stage_after_its_cleanup(tmp_path, capsys):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    sha, new = 'a'*64, 'b'*64
    entry = {'dataset': 'fixture', 'expected_manifest_sha256': sha, 'origin': 'cold',
             'state': 'retrying', 'E_prepared': True}
    (root/'queue-status.json').write_text(json.dumps({'contract': queue.CONTRACT+'.status', 'entries': [entry]}))
    receipt = root/'fixture'/(new+'.json')
    receipt.parent.mkdir()
    receipt.write_text(json.dumps({'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
        'old_manifest_sha256': sha, 'new_manifest_sha256': new}))
    assert queue.discover_prepared_history_retirements(root) == []
    assert capsys.readouterr().err == ''


def test_prepared_history_monitor_is_not_a_direct_head_retirement(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.retirement-status', 'observed_at_epoch': queue.time.time(),
        'entries': [{'dataset': 'binance', 'old_manifest_sha256': 'a'*64,
                     'mode': 'prepared_history', 'state': 'running', 'logical_bytes': 100}]}))
    row, = storage_progress.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert row['phase'] == 'source_local_prepared_history' and row['state'] == 'running'
    assert not row['complete_evidence']


def test_queue_heartbeat_does_not_replace_child_result(monkeypatch):
    events = []
    class FakeProcess:
        returncode = 0
        calls = 0
        def __enter__(self):
            return self
        def __exit__(self, *_args):
            pass
        def communicate(self, *, timeout):
            self.calls += 1
            if self.calls == 1:
                raise queue.subprocess.TimeoutExpired(['worker'], timeout)
            return 'complete', ''
    monkeypatch.setattr(queue.subprocess, 'Popen', lambda *_a, **_k: FakeProcess())
    result = queue.run_observed(['worker'], on_heartbeat=lambda: events.append('heartbeat'))
    assert events == ['heartbeat'] and result.returncode == 0 and result.stdout == 'complete'


def test_d_pressure_drains_ready_retirements_but_not_cooldowns(tmp_path):
    path = tmp_path/'retirement-queue-status.json'
    path.write_text(json.dumps({'entries': [{'state': 'waiting'}, {'state': 'waiting_retry'}]}))
    assert queue.retirement_has_ready_work(tmp_path, 63*1024**3)
    assert not queue.retirement_has_ready_work(tmp_path, 65*1024**3)
    path.write_text(json.dumps({'entries': [{'state': 'waiting_retry'}]}))
    assert not queue.retirement_has_ready_work(tmp_path, 63*1024**3)
    path.write_text(json.dumps({'entries': [{'state': 'waiting_retry', 'reason': 'd_e_owner_busy',
                                           'retry_after_epoch': queue.time.time()+30}]}))
    assert queue.retirement_has_ready_work(tmp_path, 63*1024**3)


def test_retirement_monitor_keeps_completed_receipt_and_retry_time(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    directory = root/'openbb-compact'
    directory.mkdir(parents=True)
    old, new = 'a'*64, 'b'*64
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'dataset': 'openbb-compact', 'old_manifest_sha256': old,
               'new_manifest_sha256': new, 'old_release_removed': True,
               'retirement': {'state': 'completed', 'apply': True, 'old_manifest_sha256': old,
                              'object_count': 2, 'object_bytes': 100, 'observed_at_epoch': 1000}}
    (directory/(new+'.json')).write_text(json.dumps(receipt))
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.retirement-status', 'observed_at_epoch': 1001,
        'entries': [{'dataset': 'forex-frankfurter', 'old_manifest_sha256': 'c'*64,
                     'state': 'waiting_retry', 'reason': 'd_e_owner_busy', 'retry_after_epoch': 1100}]}))
    waiting, done = storage_progress.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    assert waiting['state'] == 'waiting' and storage_progress.timestamp(waiting['next_attempt_at_utc']) == 1100
    assert done['state'] == 'completed' and done['completed_bytes'] == 100 and done['complete_evidence']


def test_waiting_history_keeps_separate_cumulative_completed_receipt(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack'
    directory = root/'tw-public'
    directory.mkdir(parents=True)
    old, new = 'a'*64, 'b'*64
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': 'tw-public',
        'old_manifest_sha256': old, 'new_manifest_sha256': new, 'old_release_removed': True,
        'retirement': {'state': 'completed', 'apply': True, 'old_manifest_sha256': old,
                       'object_count': 2, 'object_bytes': 100, 'observed_at_epoch': 1000},
        'history_retirement': {'versions_removed': 74, 'object_bytes_removed': 200,
                               'estimated_allocated_bytes_reclaimed': 150, 'observed_at_epoch': 1010}}
    (directory/(new+'.json')).write_text(json.dumps(receipt))
    (root/'retirement-queue-status.json').write_text(json.dumps({
        'contract': queue.CONTRACT+'.retirement-status', 'observed_at_epoch': 1020,
        'entries': [{'dataset': 'tw-public', 'old_manifest_sha256': old, 'mode': 'history', 'state': 'waiting'}]}))
    rows = storage_progress.source_repack_retirement_tasks(tmp_path, workspace_checked=True)
    waiting = next(row for row in rows if row['state'] == 'waiting')
    assert not waiting['complete_evidence']
    assert waiting['metrics']['history_cumulative_versions'] == 74
    assert waiting['metrics']['history_cumulative_reclaimed_bytes'] == 150
    public = storage_progress.public_status({'contract': storage_progress.CONTRACT,
        'observed_at_utc': storage_progress.iso(1020), 'tasks': rows}, now=1020)
    displayed = next(row for row in public['tasks'] if row['state'] == 'waiting')
    assert displayed['metrics']['history_cumulative_reclaimed_bytes'] == 150
    assert displayed['complete_evidence'] is False
def test_E_only_source_gets_D_before_an_unrelated_repack():
    rows = [{'dataset': 'small', 'expected_manifest_sha256': 'a'*64},
            {'dataset': 'urgent', 'expected_manifest_sha256': 'b'*64}]
    states = [{**row, 'state': 'waiting'} for row in rows]
    chosen = queue.prioritize_E_only(rows, states, {('urgent', 'b'*64)},
                                    installer_busy=False, current=rows[0])
    assert chosen == rows[1] and states[1]['reason'] == 'repack_E_first_install_waiting'
    assert queue.prioritize_E_only(rows, states, {('urgent', 'b'*64)},
                                   installer_busy=True, current=rows[0]) is None
    assert queue.prioritize_E_only(rows, states, {('absent', 'c'*64)},
                                   installer_busy=False, current=rows[0]) is None
    newer = {'dataset': 'urgent', 'expected_manifest_sha256': 'c'*64}
    same_dataset_rows = [newer, rows[1]]
    same_dataset_states = [{**row, 'state': 'waiting'} for row in same_dataset_rows]
    chosen = queue.prioritize_E_only(same_dataset_rows, same_dataset_states, {('urgent', 'b'*64)},
                                     installer_busy=False, current=newer)
    assert chosen == rows[1] and same_dataset_states[0]['state'] == 'waiting'
    assert same_dataset_states[1]['reason'] == 'repack_E_first_install_waiting'

