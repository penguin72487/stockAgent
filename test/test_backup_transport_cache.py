"""Real temporary files exercise the exact NAS/source/reader cleanup gates."""
from copy import deepcopy
import json
import os

import pytest

from test_backup_stream import stream, readiness, acknowledge  # noqa: F401
from stockagent.data_sync import backup_transport_cache as module
from stockagent.data_sync.backup_stream import BackupStream, coverage, recovery_index
from stockagent.data_sync.backup_transport_cache import build_plan, process_roots, retire
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import private_json


@pytest.fixture
def accepted(stream):
    queue, cold, _, _, _ = stream
    queue.config['maximum_batch_files'] = 100
    readiness(queue)
    queue.cycle()
    ledger = queue.load_ledger()
    key = next(iter(ledger['deliveries']))
    delivery = ledger['deliveries'][key]
    acknowledge(queue, delivery)
    assert queue.ingest(ledger) == []
    private_json(queue.ledger_path, ledger)
    return queue, cold, ledger, key


def test_only_transport_copy_is_removed_and_NAS_index_coverage_is_retained(accepted):
    queue, cold, ledger, key = accepted
    source_before = {p.relative_to(cold).as_posix(): p.read_bytes() for p in cold.rglob('*') if p.is_file()}
    catalog = json.loads((queue.state/'catalog.json').read_bytes())
    covered = coverage(catalog, ledger)
    plan = build_plan(queue, ledger['deliveries'][key])
    assert plan['source_objects_deleted'] == 0
    result = retire(queue, ledger, key)
    assert result['state'] == 'retired'
    assert not (queue.transport/('batch-'+key)).exists()
    assert not (queue.transport/'.staging'/('retired-cache-'+key)).exists()
    assert {p.relative_to(cold).as_posix(): p.read_bytes() for p in cold.rglob('*') if p.is_file()} == source_before
    assert coverage(catalog, ledger) == covered
    index = recovery_index(catalog, ledger, queue.config['repository_id'])
    assert index['deliveries'][0]['batch_retired_from_transport'] is True
    assert index['deliveries'][0]['snapshot_id'] == ledger['deliveries'][key]['acceptance']['snapshot_id']
    assert retire(queue, ledger, key) == result


@pytest.mark.parametrize('reason', ['no_ack', 'auxiliary', 'source_changed', 'target_changed', 'extra', 'hardlink'])
def test_invalid_or_unique_data_are_never_deleted(accepted, reason):
    queue, cold, ledger, key = accepted
    delivery = ledger['deliveries'][key]
    batch = queue.transport/('batch-'+key)
    if reason == 'no_ack':
        delivery.pop('acceptance')
    elif reason == 'auxiliary':
        delivery['dispatch']['delivery_kind'] = 'code_and_control_backup'
    elif reason == 'source_changed':
        row = next(r for r in delivery['files'] if r['relative'].startswith('objects/'))
        (cold/row['relative']).write_bytes(b'changed original')
    elif reason == 'target_changed':
        row = delivery['files'][0]
        (batch/'cold'/row['relative']).write_bytes(b'unique local changes')
    elif reason == 'extra':
        (batch/'unique.txt').write_text('preserve this new content')
    else:
        os.link(batch/'READY', queue.transport/'external-hardlink')
    with pytest.raises((SnapshotError, ValueError)):
        retire(queue, ledger, key)
    assert batch.is_dir()


def test_active_reader_defers_retirement(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    monkeypatch.setattr(module, 'process_references_many', lambda *args, **kwargs: ['live reader'])
    with pytest.raises(SnapshotError, match='active process'):
        retire(queue, ledger, key)
    assert (queue.transport/('batch-'+key)).is_dir()


def test_process_gates_include_physical_and_bind_aliases_only():
    from pathlib import Path
    mounts = ('1 0 0:1 / /native rw - ext4 native rw\n'
              '2 1 0:2 / /drive rw - 9p D: rw\n'
              '3 1 0:2 /transport /sender rw - 9p D: rw\n'
              '4 1 0:3 / /other rw - 9p E: rw\n'
              '5 1 0:4 /transport /windows\\040alias rw - 9p D: rw\n')
    observed = process_roots(Path('/sender'), Path('/sender/batch-key'), mountinfo=mounts)
    assert set(observed) == {Path('/sender/batch-key'), Path('/drive/transport/batch-key'),
                             Path('/windows alias/batch-key')}
    assert process_roots(Path('/unmounted'), Path('/unmounted/batch-key'), mountinfo=mounts) == (Path('/unmounted/batch-key'),)


def test_alias_reader_blocks_before_any_transport_unlink(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    alias = queue.state / 'physical-batch-alias'
    monkeypatch.setattr(module, 'process_roots', lambda *args: (alias,))
    monkeypatch.setattr(module, 'process_references_many', lambda paths: ['physical reader'] if alias in paths else [])
    with pytest.raises(SnapshotError, match='active process'):
        retire(queue, ledger, key)
    assert (queue.transport/('batch-'+key)).is_dir()


def test_interrupted_rename_resumes_exact_remaining_files(accepted):
    queue, _, ledger, key = accepted
    delivery = ledger['deliveries'][key]
    plan = build_plan(queue, delivery)
    delivery['cache_retirement'] = {**plan, 'state': 'renamed'}
    stage = queue.transport/'.staging'/('retired-cache-'+key)
    os.rename(queue.transport/('batch-'+key), stage)
    # Simulate a crash after some payload and both metadata files were unlinked.
    for name in ['backup-envelope.json', 'READY', next(p for p in plan['files'] if p.startswith('cold/'))]:
        (stage/name).unlink()
    private_json(queue.ledger_path, ledger)
    assert retire(queue, ledger, key)['state'] == 'retired'
    assert not stage.exists()


def test_change_between_preflight_and_rename_preserves_private_stage(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    row = ledger['deliveries'][key]['files'][0]
    original = module.os.rename
    def mutated(source, destination):
        original(source, destination)
        (destination/'cold'/row['relative']).write_bytes(b'new unique bytes')
    monkeypatch.setattr(module.os, 'rename', mutated)
    with pytest.raises(SnapshotError, match='changed since complete'):
        retire(queue, ledger, key)
    assert (queue.transport/'.staging'/('retired-cache-'+key)/'cold'/row['relative']).read_bytes() == b'new unique bytes'


def test_broad_automatic_deletion_requires_exact_bounded_policy(stream):
    queue, *_ = stream
    config = deepcopy(queue.config)
    config['automatic_batch_deletion'] = True
    with pytest.raises(SnapshotError):
        BackupStream(config)
    config['cold_transport_cache'] = {'enabled': True, 'scope': 'machine_acknowledged_reconstructible_cold_transport',
                                    'user_instruction': 'back up all valid data using a bounded relay'}
    BackupStream(config)
    config['automatic_batch_deletion'] = False
    with pytest.raises(SnapshotError):
        BackupStream(config)


def syncthing_api(queue, monkeypatch, **changes):
    from scripts import configure_artifact_ingress_syncthing as api
    queue.config['syncthing_folder_id'] = 'bounded-test'
    folder = {'path': str(queue.transport), 'type': 'sendonly', 'paused': False,
              'devices': [{'deviceID': queue.config[k]} for k in ('producer_device_id', 'receiver_device_id')]}
    status = {'state': 'idle', 'errors': 0, 'needBytes': 0, 'needTotalItems': 0}
    peer = {'completion': 100, 'remoteState': 'valid', 'needBytes': 0, 'needItems': 0, 'needDeletes': 0}
    folder.update(changes.get('folder', {}))
    status.update(changes.get('status', {}))
    peer.update(changes.get('peer', {}))
    scans = []
    def request(base, key, endpoint, parameters=None, **kwargs):
        if endpoint.startswith('/rest/config/folders/'):
            return folder
        if endpoint == '/rest/db/status':
            return status
        if endpoint == '/rest/db/completion':
            return peer
        assert endpoint == '/rest/db/scan' and kwargs['method'] == 'POST'
        scans.append(parameters)
        return {}
    monkeypatch.setattr(api, 'credentials', lambda: ('test-api', 'test-key'))
    monkeypatch.setattr(api, 'request', request)
    return scans


@pytest.mark.parametrize('changes', [{'status': {'state': 'scanning'}},
    {'peer': {'needDeletes': 1}}, {'peer': {'remoteState': 'unknown'}},
    {'status': {'watchError': 'unavailable'}}, {'peer': {'completion': 99}}])
def test_incomplete_paired_transport_defers_cleanup(accepted, monkeypatch, changes):
    queue, _, ledger, key = accepted
    scans = syncthing_api(queue, monkeypatch, **changes)
    assert queue.retire_transport_cache(ledger)['deferred'] is True
    assert (queue.transport/('batch-'+key)).is_dir()
    assert scans == []


def test_redirected_pairing_blocks_cleanup_before_any_unlink(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    syncthing_api(queue, monkeypatch, folder={'type': 'receiveonly'})
    with pytest.raises(SnapshotError, match='paired send-only'):
        queue.retire_transport_cache(ledger)
    assert (queue.transport/('batch-'+key)).is_dir()


def test_deletion_scan_is_retried_after_retirement_before_journal_commit(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    retire(queue, ledger, key)
    scans = syncthing_api(queue, monkeypatch)
    result = queue.retire_transport_cache(ledger)
    assert result['retired_deliveries'] == 0 and not result['errors']
    assert scans == [{'folder': 'bounded-test'}]
    assert queue.load_ledger()['deliveries'][key]['cache_retirement']['deletion_scan_requested'] is True
