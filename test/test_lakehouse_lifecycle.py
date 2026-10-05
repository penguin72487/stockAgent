"""Durable publication/scan intents and recovery-gated transport cache."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import configure_artifact_ingress_syncthing as transport
from stockagent.control import lakehouse
from stockagent.data_sync import immutable_transport_cache as cache
from stockagent.data_sync.immutable_replication import ACK, digest, seal
from stockagent.runtime_identity import identity_sha256


@pytest.fixture
def source_wave(tmp_path):
    cold, physical, state = [tmp_path / name for name in ('cold', 'transport', 'state')]
    cold.mkdir(); physical.mkdir(); state.mkdir()
    data = b'authoritative original observation' * 8
    sha = hashlib.sha256(data).hexdigest()
    relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
    source = cold / relative
    source.parent.mkdir(parents=True);source.write_bytes(data)
    c = {'cold_root': str(cold), 'state_root': str(state), 'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'}
    return c, [(relative, sha, len(data))], physical, state / 'ledger.json', {'deliveries': {}}, source


def test_interrupted_partial_source_wave_reuses_intent_without_losing_original(source_wave, monkeypatch):
    from stockagent.data_sync import offhost_backup, materialized_cache
    c, chosen, physical, ledger_path, ledger, source = source_wave
    original = source.read_bytes()
    copy = offhost_backup.copy_verified_bytes
    def interrupted(origin, destination, **options):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(original[:11])
        raise KeyboardInterrupt('simulated copy termination')
    monkeypatch.setattr(offhost_backup, 'copy_verified_bytes', interrupted)
    with pytest.raises(KeyboardInterrupt):
        lakehouse._publish_source_wave(c, chosen, physical, ledger_path, ledger)
    intent = json.loads((Path(c['state_root']) / 'source-wave-current.json').read_bytes())['intent_sha256']
    monkeypatch.setattr(offhost_backup, 'copy_verified_bytes', copy)
    monkeypatch.setattr(materialized_cache, 'process_references_many', lambda paths: [])
    identity, root = lakehouse._publish_source_wave(c, [], physical, ledger_path, ledger)
    assert source.read_bytes() == original and root.is_dir()
    assert list(ledger['deliveries']) == [identity]
    assert not list((physical / '.staging').glob('*'))
    resumed = json.loads((Path(c['state_root']) / 'source-wave-current.json').read_bytes())
    assert resumed['intent_sha256'] == intent and resumed['state'] == 'enrolled'


def test_sealed_but_unenrolled_wave_recovers_ready_and_exact_enrollment(source_wave, monkeypatch):
    c, chosen, physical, ledger_path, ledger, source = source_wave
    seal = lakehouse.seal
    def interrupted(root, context):
        result = seal(root, context)
        (root / 'READY').unlink()
        raise KeyboardInterrupt('simulated missing readiness before enrollment')
    monkeypatch.setattr(lakehouse, 'seal', interrupted)
    with pytest.raises(KeyboardInterrupt):
        lakehouse._publish_source_wave(c, chosen, physical, ledger_path, ledger)
    monkeypatch.setattr(lakehouse, 'seal', seal)
    identity, root = lakehouse._publish_source_wave(c, [], physical, ledger_path, ledger)
    assert (root / 'READY').read_text() == identity + '\n'
    assert ledger['deliveries'][identity]['file_keys'] == [chosen[0][0] + '@' + chosen[0][1]]


def test_interrupted_wave_preserves_unknown_members_and_changed_original(source_wave, monkeypatch):
    from stockagent.data_sync import offhost_backup
    c, chosen, physical, ledger_path, ledger, source = source_wave
    def interrupted(origin, destination, **options):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b'partial')
        raise KeyboardInterrupt()
    monkeypatch.setattr(offhost_backup, 'copy_verified_bytes', interrupted)
    with pytest.raises(KeyboardInterrupt):
        lakehouse._publish_source_wave(c, chosen, physical, ledger_path, ledger)
    stage = next((physical / '.staging').iterdir())
    (stage / 'unknown').write_bytes(b'preserve unknown evidence')
    with pytest.raises(ValueError, match='unknown'):
        lakehouse._publish_source_wave(c, [], physical, ledger_path, ledger)
    assert (stage / 'unknown').read_bytes() == b'preserve unknown evidence' and not ledger['deliveries']
    (stage / 'unknown').unlink()
    source.write_bytes(b'changed source')
    with pytest.raises(ValueError, match='recoverable'):
        lakehouse._publish_source_wave(c, [], physical, ledger_path, ledger)
    assert list(stage.rglob('*.partial')) and not ledger['deliveries']


def test_busy_scan_retains_exact_delivery_intent(tmp_path, monkeypatch):
    calls = []
    def api(base, key, route, *args, **kw):
        calls.append(route)
        return {'myID': 'penguin'} if route.endswith('system/status') else {'state': 'scanning'}
    monkeypatch.setattr(transport, 'credentials', lambda: ('local', 'private'))
    monkeypatch.setattr(transport, 'request', api)
    c = {'state_root': str(tmp_path), 'transport_root': str(tmp_path / 'ingress/lakehouse'), 'producer_device_id': 'penguin'}
    target = 'lakehouse/lake-' + 'a' * 64
    lakehouse.notify_transport(c, [target])
    value = json.loads((tmp_path / 'pending-transport-scan.json').read_bytes())
    assert value['paths'] == [target] and value['state'] == 'retry_scan'
    assert '/rest/db/scan' not in calls


def test_scan_timeout_and_new_delivery_preserve_both_intents(tmp_path, monkeypatch):
    failed = True
    scans = []
    def api(base, key, route, query=None, **kw):
        if route.endswith('system/status'):
            return {'myID': 'penguin'}
        if route.endswith('db/status'):
            return {'state': 'idle'}
        scans.append(query['sub'])
        if failed:
            raise TimeoutError('in flight')
    monkeypatch.setattr(transport, 'credentials', lambda: ('local', 'private'))
    monkeypatch.setattr(transport, 'request', api)
    c = {'state_root': str(tmp_path), 'transport_root': str(tmp_path / 'ingress/lakehouse'), 'producer_device_id': 'penguin'}
    a, b = ['lakehouse/lake-' + letter * 64 for letter in ('a', 'b')]
    lakehouse.notify_transport(c, [a])
    failed = False
    lakehouse.notify_transport(c, [b])
    value = json.loads((tmp_path / 'pending-transport-scan.json').read_bytes())
    assert value['state'] == 'scan_requested' and not value['paths']
    assert scans == [a, a, b]
    assert all(p != 'lakehouse' for p in scans)


@pytest.fixture
def delivery(tmp_path, monkeypatch):
    physical = tmp_path / 'ingress'
    physical.mkdir()
    (physical / '.staging').mkdir()
    staging = physical / '.staging/raw'
    staging.mkdir()
    data = b'preserved authoritative bytes'
    sha = hashlib.sha256(data).hexdigest()
    relative = 'objects/blobs/' + sha[:2] + '/' + sha + '.blob'
    file = staging / relative
    file.parent.mkdir(parents=True);file.write_bytes(data)
    manifest = seal(staging, {'kind': 'immutable_source_objects', 'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'})
    identity = manifest['identity_sha256']
    batch = physical / 'lakehouse' / ('lake-' + identity)
    batch.parent.mkdir()
    staging.rename(batch)
    cold = tmp_path / 'cold'
    original = cold / relative
    original.parent.mkdir(parents=True);original.write_bytes(data)
    state, receipts = tmp_path / 'state', tmp_path / 'receipts'
    state.mkdir(); receipts.mkdir()
    c = {'cold_root': str(cold), 'lake_root': str(tmp_path / 'lake'), 'transport_root': str(batch.parent),
         'receipt_root': str(receipts), 'state_root': str(state), 'transport_cache_retirement': True,
         'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'}
    ack = {'contract': ACK, 'delivery_identity_sha256': identity, 'manifest_file_sha256': digest(batch / 'manifest.json'),
           'complete_files': 3, 'complete_bytes': sum(p.stat().st_size for p in batch.rglob('*') if p.is_file()),
           'producer_device_id': 'penguin', 'receiver_device_id': 'lab203', 'command_exit_codes': [0, 0], 'restore_command_exit_codes': [0, 0],
           **{k: True for k in ('all_files_sha256_verified', 'exact_file_set_verified', 'source_unchanged_verified',
               'nas_independent_restore_verified', 'nas_mount_guard_verified', 'single_owner_verified', 'runtime_lock_verified')}}
    atomic_write_json(receipts / (batch.name + '.json'), {**ack, 'identity_sha256': identity_sha256(ack)})
    proof = {'state': 'nas_archive_file_recovery_verified', 'delivery_identity_sha256': identity}
    monkeypatch.setattr(cache, 'PHYSICAL_TRANSPORT_ROOT', physical)
    monkeypatch.setattr(cache, 'process_references_many', lambda roots: [])
    return c, identity, batch, original, proof


def test_cache_plan_preserves_bytes_then_only_exact_transport_is_retired(delivery):
    c, identity, batch, original, proof = delivery
    before = original.read_bytes()
    plan = cache.retire(c, identity, proof)
    assert plan['inventory_and_process_gates_verified'] and batch.exists()
    applied = cache.retire(c, identity, proof, apply=True)
    assert applied['state'] == 'retired' and not batch.exists()
    assert original.read_bytes() == before
    assert applied['primary_source_files_deleted'] == applied['nas_archive_files_deleted'] == 0
    assert lakehouse.acceptance(c, identity)['state'] == 'nas_archive_file_recovery_verified'


def test_source_ack_is_durable_without_waiting_for_transport_gc(delivery, monkeypatch):
    c, identity, batch, original, proof = delivery
    relative = original.relative_to(Path(c['cold_root'])).as_posix()
    key = relative + '@' + hashlib.sha256(original.read_bytes()).hexdigest()
    ledger_path = Path(c['state_root']) / 'source-replication-ledger.json'
    atomic_write_json(ledger_path, {'deliveries': {identity: {'file_keys': [key], 'bytes': original.stat().st_size}}})
    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return [(relative, key.split('@')[1], original.stat().st_size)]
        def close(self): pass
    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())
    monkeypatch.setattr(lakehouse, 'retire_accepted_transport', lambda *args: pytest.fail('source publication waited for HDD GC'))
    result = lakehouse.stage_source_wave(c)
    assert result['nas_archive_covered_objects'] == 1 and not result['pending_delivery_ids']
    persisted = json.loads(ledger_path.read_bytes())['deliveries'][identity]
    assert persisted['nas_acceptance']['state'] == 'nas_archive_file_recovery_verified'
    assert batch.exists() and original.exists()


@pytest.mark.parametrize('change', ['source', 'unknown', 'open', 'ack_pair', 'ack_bytes', 'ack_boolean_exit'])
def test_cache_refuses_changed_source_unknown_member_process_and_invalid_ack(delivery, monkeypatch, change):
    c, identity, batch, original, proof = delivery
    cache.retire(c, identity, proof)
    if change == 'source':
        original.write_bytes(b'changed source')
    elif change == 'unknown':
        (batch / 'unique').write_bytes(b'unregistered bytes')
    elif change == 'open':
        monkeypatch.setattr(cache, 'process_references_many', lambda roots: ['open descriptor'])
    else:
        path = Path(c['receipt_root']) / (batch.name + '.json')
        value = json.loads(path.read_bytes()); value.pop('identity_sha256')
        if change == 'ack_pair': value['receiver_device_id'] = 'other'
        elif change == 'ack_bytes': value['complete_bytes'] += 1
        else: value['command_exit_codes'] = [False, False]
        atomic_write_json(path, {**value, 'identity_sha256': identity_sha256(value)})
    with pytest.raises(ValueError):
        cache.retire(c, identity, proof, apply=True)
    assert batch.exists() and (batch / original.relative_to(Path(c['cold_root']))).exists()


def test_interrupted_exact_unlink_resumes_without_touching_originals(delivery, monkeypatch):
    c, identity, batch, original, proof = delivery
    cache.retire(c, identity, proof)
    real = Path.unlink
    killed = False
    def unlink(path, **kw):
        nonlocal killed
        real(path, **kw)
        if path.name == 'READY' and not killed:
            killed = True
            raise OSError('process interrupted after exact unlink')
    monkeypatch.setattr(Path, 'unlink', unlink)
    with pytest.raises(OSError):
        cache.retire(c, identity, proof, apply=True)
    assert not batch.exists()
    monkeypatch.setattr(Path, 'unlink', real)
    assert cache.retire(c, identity, proof, apply=True)['state'] == 'retired'
    assert original.read_bytes() == b'preserved authoritative bytes'
