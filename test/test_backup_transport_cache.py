"""Real temporary files exercise the exact NAS/source/reader cleanup gates."""
from copy import deepcopy
import fcntl
import json
import os
import threading
import time

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


def test_ctime_only_drift_requires_full_sha_and_preserves_original_receipts(accepted):
    queue, cold, ledger, key = accepted
    delivery = ledger['deliveries'][key]
    row = next(r for r in delivery['files'] if r['relative'].startswith('objects/'))
    original_signature = list(row['signature'])
    original_ack = deepcopy(delivery['acceptance'])
    path = cold/row['relative']; body = path.read_bytes()
    path.chmod(0o600 if path.stat().st_mode & 0o077 else 0o644)
    with pytest.raises(SnapshotError, match='original cold source changed'):
        build_plan(queue, delivery)
    result = retire(queue, ledger, key)
    assert result['state'] == 'retired' and path.read_bytes() == body
    proof = next(p for p in result['source_proofs'] if p['relative'] == row['relative'])
    assert proof['fresh_full_sha256_revalidation'] is True
    assert row['signature'] == original_signature and delivery['acceptance'] == original_ack


def test_ctime_revalidation_rejects_corrupt_bytes_with_restored_size_and_mtime(accepted):
    queue, cold, ledger, key = accepted
    row = next(r for r in ledger['deliveries'][key]['files'] if r['relative'].startswith('objects/'))
    path = cold/row['relative']; before = path.stat(); body = path.read_bytes()
    path.write_bytes(bytes([body[0] ^ 1])+body[1:])
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(SnapshotError, match='full SHA revalidation'):
        retire(queue, ledger, key)
    assert (queue.transport/('batch-'+key)).is_dir()
    assert not (queue.transport/'.staging'/('retired-cache-'+key)).exists()


def test_revalidated_source_generation_cannot_change_before_the_plan(accepted, monkeypatch):
    queue, cold, ledger, key = accepted
    row = next(r for r in ledger['deliveries'][key]['files'] if r['relative'].startswith('objects/'))
    path = cold/row['relative']; path.chmod(0o600)
    original = module.build_plan
    def raced(*args, **kwargs):
        path.chmod(0o644)
        return original(*args, **kwargs)
    monkeypatch.setattr(module, 'build_plan', raced)
    with pytest.raises(SnapshotError, match='original cold source changed'):
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


def test_scanning_retirement_keeps_exact_ACK_and_source_proofs_and_defers_scan(accepted, monkeypatch):
    queue, cold, ledger, key = accepted
    queue.config['cold_transport_cache'] = {'allow_scanning_when_fully_converged': True}
    original = {p.relative_to(cold): p.read_bytes() for p in cold.rglob('*') if p.is_file()}
    scans = syncthing_api(queue, monkeypatch, status={'state': 'scanning'})
    result = queue.retire_transport_cache(ledger)
    assert result['retired_deliveries'] == 1 and result['errors'] == []
    assert result['deletion_scan_deferred'] is True and scans == []
    assert not (queue.transport / ('batch-' + key)).exists()
    assert {p.relative_to(cold): p.read_bytes() for p in cold.rglob('*') if p.is_file()} == original
    progress = json.loads((queue.state / 'transport-cache-progress.json').read_bytes())
    assert progress['state'] == 'completed' and progress['all_candidate_retirements_verified'] is True
    assert progress['completed_items'] == progress['total_items'] == 1
    assert progress['source_objects_deleted'] == progress['NAS_snapshots_deleted'] == 0
    assert (queue.state / 'transport-cache-progress.json').stat().st_size < 2048
    assert not queue.load_ledger()['deliveries'][key]['cache_retirement'].get('deletion_scan_requested')
    scans = syncthing_api(queue, monkeypatch)
    result = queue.retire_transport_cache(queue.load_ledger())
    assert result['retired_deliveries'] == 0
    assert scans == [[('folder', 'bounded-test'), ('sub', 'batch-' + key)]]
    assert queue.load_ledger()['deliveries'][key]['cache_retirement']['deletion_scan_requested'] is True


@pytest.mark.parametrize('changes', [
    {'status': {'needTotalItems': 1}}, {'status': {'pullErrors': 1}},
    {'status': {'watchError': 'watch failed'}}, {'peer': {'needDeletes': 1}},
    {'peer': {'completion': 99}}, {'peer': {'remoteState': 'unknown'}}])
def test_scanning_opt_in_does_not_relax_any_convergence_gate(accepted, monkeypatch, changes):
    queue, _, ledger, key = accepted
    queue.config['cold_transport_cache'] = {'allow_scanning_when_fully_converged': True}
    changes['status'] = {'state': 'scanning', **changes.get('status', {})}
    scans = syncthing_api(queue, monkeypatch, **changes)
    assert queue.retire_transport_cache(ledger)['deferred'] is True
    assert (queue.transport / ('batch-' + key)).is_dir() and scans == []


@pytest.mark.parametrize('fault', ['source_changed', 'reader', 'missing_ack'])
def test_scanning_opt_in_preserves_changed_source_reader_and_ACK_gates(accepted, monkeypatch, fault):
    queue, cold, ledger, key = accepted
    queue.config['cold_transport_cache'] = {'allow_scanning_when_fully_converged': True}
    syncthing_api(queue, monkeypatch, status={'state': 'scanning'})
    if fault == 'source_changed':
        row = next(r for r in ledger['deliveries'][key]['files'] if r['relative'].startswith('objects/'))
        (cold / row['relative']).write_bytes(b'keep this new source')
    elif fault == 'reader':
        monkeypatch.setattr(module, 'process_references_many', lambda *_a, **_k: ['active reader'])
    else:
        ledger['deliveries'][key].pop('acceptance')
    result = queue.retire_transport_cache(ledger)
    assert result['retired_deliveries'] == 0
    assert (queue.transport / ('batch-' + key)).is_dir()


@pytest.mark.parametrize('value', ['true', 1, None, []])
def test_scanning_policy_requires_an_explicit_boolean(stream, value):
    queue, *_ = stream
    config = deepcopy(queue.config)
    config['cold_transport_cache'] = {'allow_scanning_when_fully_converged': value}
    with pytest.raises(SnapshotError, match='explicit boolean'):
        BackupStream(config)


@pytest.mark.parametrize('value', [True, 0, -1, 1025, '4', []])
def test_retirement_turn_requires_a_positive_transaction_count(stream, value):
    queue, *_ = stream
    config = deepcopy(queue.config)
    config['cold_transport_cache'] = {'maximum_retirements_per_cycle': value}
    with pytest.raises(SnapshotError, match='complete transactions'):
        BackupStream(config)


def test_complete_retirement_transactions_yield_to_next_wave_and_resume(stream, monkeypatch):
    queue, *_ = stream
    queue.config['maximum_batch_files'] = 1
    for _ in range(2):
        readiness(queue)
        queue.cycle()
        ledger = queue.load_ledger()
        for delivery in ledger['deliveries'].values():
            if not delivery.get('acceptance'):
                acknowledge(queue, delivery)
        assert queue.ingest(ledger) == []
        private_json(queue.ledger_path, ledger)
    assert len(ledger['deliveries']) == 2
    queue.config['cold_transport_cache'] = {'maximum_retirements_per_cycle': 1}
    syncthing_api(queue, monkeypatch)
    first = queue.retire_transport_cache(ledger)
    assert first['retired_deliveries'] == 1 and first['yielded_to_publication_turn']
    progress = json.loads((queue.state / 'transport-cache-progress.json').read_bytes())
    assert progress['completed_items'] == 1 and progress['total_items'] == 2
    assert not progress['all_candidate_retirements_verified']
    saved = queue.load_ledger()
    assert sum(d.get('cache_retirement', {}).get('state') == 'retired' for d in saved['deliveries'].values()) == 1
    second = queue.retire_transport_cache(saved)
    assert second['retired_deliveries'] == 1 and not second['errors']
    assert all(d['cache_retirement']['state'] == 'retired' for d in queue.load_ledger()['deliveries'].values())
    assert json.loads((queue.state / 'transport-cache-progress.json').read_bytes())['all_candidate_retirements_verified']


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
    assert scans == [[('folder', 'bounded-test'), ('sub', 'batch-' + key)]]
    assert result['deletion_scan_batches_acknowledged'] == 1
    assert queue.load_ledger()['deliveries'][key]['cache_retirement']['deletion_scan_requested'] is True


def test_retired_scan_rejects_invalid_identity_before_notifying(accepted, monkeypatch):
    queue, _, ledger, key = accepted
    retire(queue, ledger, key)
    ledger['deliveries'][key]['dispatch']['batch_relative'] = '../unrelated'
    scans = syncthing_api(queue, monkeypatch)
    with pytest.raises(SnapshotError, match='invalid retired backup batch scan path'):
        queue.retire_transport_cache(ledger)
    assert scans == []
    assert not ledger['deliveries'][key]['cache_retirement'].get('deletion_scan_requested')


def test_retired_scans_journal_each_group_and_retry_only_remaining(accepted, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as api
    queue, _, ledger, key = accepted
    retire(queue, ledger, key)
    template = deepcopy(ledger['deliveries'][key])
    ledger['deliveries'].clear()
    for number in range(65):
        envelope = f'{number:064x}'
        delivery = deepcopy(template)
        delivery['dispatch']['batch_relative'] = 'batch-' + envelope
        ledger['deliveries'][envelope] = delivery
    private_json(queue.ledger_path, ledger)
    scans = syncthing_api(queue, monkeypatch)
    original = api.request
    def fail_second_group(base, token, endpoint, parameters=None, **kwargs):
        if endpoint == '/rest/db/scan' and scans:
            raise TimeoutError('synthetic second group timeout')
        return original(base, token, endpoint, parameters, **kwargs)
    monkeypatch.setattr(api, 'request', fail_second_group)
    result = queue.retire_transport_cache(ledger)
    saved = queue.load_ledger()
    assert result['deferred'] and result['deletion_scan_batches_acknowledged'] == 64
    assert len(scans) == 1 and len(scans[0]) == 65
    assert sum(bool(d['cache_retirement'].get('deletion_scan_requested'))
               for d in saved['deliveries'].values()) == 64
    monkeypatch.setattr(api, 'request', original)
    retried = queue.retire_transport_cache(saved)
    assert not retried['deferred'] and retried['deletion_scan_batches_acknowledged'] == 1
    assert len(scans[-1]) == 2
    assert all(d['cache_retirement'].get('deletion_scan_requested')
               for d in queue.load_ledger()['deliveries'].values())


def test_deletion_notification_timeout_preserves_retry_without_repeating_retirement(accepted, monkeypatch):
    from scripts import configure_artifact_ingress_syncthing as api
    queue, _, ledger, key = accepted
    retire(queue, ledger, key)
    syncthing_api(queue, monkeypatch)
    original = api.request
    def timeout(base, token, endpoint, parameters=None, **kwargs):
        if endpoint == "/rest/db/scan":
            raise TimeoutError("synthetic scan timeout after committed retirement")
        return original(base, token, endpoint, parameters, **kwargs)
    monkeypatch.setattr(api, "request", timeout)
    result = queue.retire_transport_cache(ledger)
    assert result['deferred'] and not result['errors']
    assert result['scan_notification']['error_type'] == 'TimeoutError'
    assert not queue.load_ledger()['deliveries'][key]['cache_retirement'].get('deletion_scan_requested')
    monkeypatch.setattr(api, "request", original)
    retried = queue.retire_transport_cache(queue.load_ledger())
    assert retried['retired_deliveries'] == 0
    assert queue.load_ledger()['deliveries'][key]['cache_retirement']['deletion_scan_requested'] is True


@pytest.mark.parametrize('concurrent_scan', [False, True])
def test_waiting_publisher_runs_between_retirements_and_owner_rebases(accepted, monkeypatch, concurrent_scan):
    from scripts import configure_artifact_ingress_syncthing as api
    from stockagent.data_sync.backup_stream import _owner_has_waiters

    queue, _, ledger, key = accepted
    scans = syncthing_api(queue, monkeypatch)
    original = api.request
    changed = threading.Event()
    attempted = threading.Event()
    writer_errors = []

    def request(base, token, endpoint, parameters=None, **kwargs):
        result = original(base, token, endpoint, parameters, **kwargs)
        if concurrent_scan and changed.is_set() and endpoint == '/rest/db/status':
            return {**result, 'state': 'scanning'}
        return result
    monkeypatch.setattr(api, 'request', request)

    def other_owner():
        try:
            with (queue.state / 'owner.lock').open('a') as other:
                attempted.set()
                fcntl.flock(other, fcntl.LOCK_EX)
                current = queue.load_ledger()
                current['concurrent_publication_evidence'] = 'preserved'
                queue._save_ledger(current)
                changed.set()
        except Exception as exc:
            writer_errors.append(exc)

    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        worker = threading.Thread(target=other_owner)
        worker.start()
        assert attempted.wait(3)
        deadline = time.monotonic() + 3
        while not _owner_has_waiters(owner) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert _owner_has_waiters(owner), 'the second owner must be queued in the kernel'
        result = queue.retire_transport_cache(ledger, owner=owner)
    worker.join(timeout=3)
    assert not worker.is_alive() and not writer_errors and changed.is_set()
    assert result['retired_deliveries'] == 1
    assert queue.load_ledger()['concurrent_publication_evidence'] == 'preserved'
    assert ledger['concurrent_publication_evidence'] == 'preserved'
    assert not (queue.transport / ('batch-' + key)).exists()
    assert result['deferred'] is concurrent_scan
    assert len(scans) == (0 if concurrent_scan else 1)


def test_failed_retirement_keeps_owner_until_error_is_recorded(accepted, monkeypatch):
    from stockagent.data_sync import backup_stream
    queue, _, ledger, key = accepted
    syncthing_api(queue, monkeypatch)
    def failed_retirement(*args):
        raise SnapshotError('simulated interrupted transaction')
    def unexpected_waiter_check(*args):
        pytest.fail('a failed transaction must not enter the post-commit handoff')
    monkeypatch.setattr(module, 'retire', failed_retirement)
    monkeypatch.setattr(backup_stream, '_owner_has_waiters', unexpected_waiter_check)
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        result = queue.retire_transport_cache(ledger, owner=owner)
    assert result['retired_deliveries'] == 0
    assert result['errors'] == [{'envelope': key, 'reason': 'SnapshotError',
                                'reason_code': 'retirement_validation',
                                'detail': 'simulated interrupted transaction'}]
    assert (queue.transport / ('batch-' + key)).is_dir()
