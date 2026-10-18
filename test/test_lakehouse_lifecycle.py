"""Durable publication/scan intents and recovery-gated transport cache."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import configure_artifact_ingress_syncthing as transport
from stockagent.control import lakehouse
from stockagent.data_sync import immutable_transport_cache as cache
from stockagent.data_sync.immutable_replication import ACK, digest, seal
from stockagent.runtime_identity import identity_sha256


def test_worker_identity_includes_loaded_packed_reader_and_manifest_contract(monkeypatch):
    from scripts import manage_lakehouse as worker
    seen, changed = set(), {}
    def source_digest(path):
        relative = str(path.relative_to(worker.ROOT))
        seen.add(relative)
        return changed.get(relative, "a" * 64)
    monkeypatch.setattr(worker, "digest", source_digest)
    baseline = worker.worker_code_identity()
    for relative in ("stockagent/data_sync/packed_snapshots.py",
                     "stockagent/data_sync/packed_metadata.py",
                     "stockagent/data_sync/packing_checkpoint.py",
                     "stockagent/data_sync/packed_backup.py",
                     "stockagent/data_sync/desync_snapshots.py",
                     "configs/training_rescue.json"):
        assert relative in seen
        changed[relative] = "b" * 64
        assert worker.worker_code_identity() != baseline
        changed.clear()


@pytest.mark.parametrize("seconds", [0, 86401, 21600, 86400])
def test_safe_handoff_deadline_is_bounded_without_touching_production(tmp_path, monkeypatch, seconds):
    from scripts import rollout_lakehouse_source_policy as rollout
    def no_production_configuration():
        raise RuntimeError("test reached configuration without service changes")
    monkeypatch.setattr(rollout, "configuration", no_production_configuration)
    monkeypatch.setattr(rollout, "worker_code_identity", lambda: "a" * 64)
    args = ["--output", str(tmp_path / "receipt.json"), "--deadline-seconds", str(seconds)]
    if 1 <= seconds <= 86400:
        with pytest.raises(RuntimeError, match="without service changes"):
            rollout.main(args)
    else:
        with pytest.raises(SystemExit) as error:
            rollout.main(args)
        assert error.value.code == 2


def test_active_restart_requires_explicit_code_only_scope_before_any_configuration(tmp_path, monkeypatch):
    from scripts import rollout_lakehouse_source_policy as rollout
    monkeypatch.setattr(rollout, 'configuration', lambda: pytest.fail('unscoped restart reached configuration'))
    with pytest.raises(SystemExit) as error:
        rollout.main(['--output', str(tmp_path / 'receipt.json'), '--restart-active-owner'])
    assert error.value.code == 2


@pytest.mark.parametrize('fault', [None, 'candidate_drift', 'lost_verified_release'])
def test_authorized_restart_preserves_checkpoint_and_releases_old_owner_before_locks(tmp_path, monkeypatch, fault):
    import fcntl
    from scripts import rollout_lakehouse_source_policy as rollout
    state = tmp_path / 'state'
    operation = state / 'dataset-versions/operations/test'
    operation.mkdir(parents=True)
    checkpoint = {'plan_sha256': 'p', 'datasets': {
        'accepted': {'state': 'verified', 'snapshot_id': 'v1', 'manifest_sha256': 'm1'},
        'interrupted': {'state': 'processing', 'attempt_id': 'attempt1'}}}
    atomic_write_json(operation / 'progress.json', checkpoint)
    atomic_write_json(state / 'dataset-versions/current.json', {
        'plan_sha256': 'p', 'operation_directory': str(operation)})
    ready = tmp_path / 'readiness.json'
    atomic_write_json(ready, {'producer_device_id': 'p', 'receiver_device_id': 'r', 'ingress_free_bytes': 1})
    private = tmp_path / 'private.json'
    private.write_bytes(b'{}')
    monkeypatch.setattr(rollout, 'RECEIVER_READINESS', ready)
    monkeypatch.setattr(rollout, 'PRIVATE_CONFIGURATION', private)
    c = {'state_root': str(state), 'producer_device_id': 'p', 'receiver_device_id': 'r', 'source_replication': {}}
    monkeypatch.setattr(rollout, 'configuration', lambda: c)
    monkeypatch.setattr(rollout, 'guard', lambda *_: None)
    monkeypatch.setattr(rollout, 'source_wave_policy', lambda *_: None)
    processes, changed, events = {'worker': '100'}, [], []
    monkeypatch.setattr(rollout, 'worker_code_identity', lambda: ('b' if changed else 'a') * 64)
    monkeypatch.setattr(rollout, 'pid', lambda unit: processes['worker'] if unit == rollout.OWNER else unit)
    async def workflows(_):
        return {name: {'status': 'RUNNING', 'first_run_id': name, 'run_id': name}
                for name in (*rollout.IDS, *rollout.OPTIONAL_IDS)}
    monkeypatch.setattr(rollout, 'workflows', workflows)
    original_run = subprocess.run
    lock = state / 'dataset-versions/owner.lock'
    descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    inode = lock.stat().st_ino
    def run(command, **options):
        if command[0] == 'flock':
            events.append('lock')
            return original_run(command, **options)
        assert command[:2] in (['systemctl', 'stop'], ['systemctl', 'start'])
        events.append(command[1])
        if command[1] == 'stop':
            assert (tmp_path / 'receipt-before.json').is_file(), 'durable intent must precede stopping'
            processes['worker'] = '0'
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            if fault == 'candidate_drift':
                changed.append(True)
            if fault == 'lost_verified_release':
                altered = deepcopy(checkpoint)
                altered['datasets']['accepted']['snapshot_id'] = 'wrong-version'
                atomic_write_json(operation / 'progress.json', altered)
        else:
            processes['worker'] = '200'
            atomic_write_json(state / 'worker-ready.json', {'code_identity_sha256': 'a' * 64})
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(rollout.subprocess, 'run', run)
    try:
        args = ['--output', str(tmp_path / 'receipt.json'), '--deadline-seconds', '2',
                '--reload-code-only', '--restart-active-owner']
        if fault:
            with pytest.raises(ValueError, match='candidate changed|release was lost'):
                rollout.main(args)
            assert not (tmp_path / 'receipt.json').exists()
        else:
            rollout.main(args)
            receipt = json.loads((tmp_path / 'receipt.json').read_bytes())
            assert receipt['authorized_active_owner_restart'] is True
            assert receipt['verified_migration_releases_preserved'] == 1
            assert receipt['workflow_chains_preserved'] is True
            assert receipt['postgres_and_temporal_server_not_restarted'] is True
        assert events[0] == 'stop' and events[-1] == 'start'
        assert lock.stat().st_ino == inode
        assert private.read_bytes() == b'{}' and processes['worker'] == '200'
    finally:
        os.close(descriptor)


@pytest.mark.parametrize('enrollment,existing,expected', [
    ('registered-node', None, 'registered-node'),
    ('registered-node', 'explicit-node', 'explicit-node'),
    ('invalid;command', None, None),
    ('', None, None),
])
def test_unattended_lakehouse_uses_enrolled_wsl_identity_not_a_default(
        tmp_path, enrollment, existing, expected):
    # Execute the exact bootstrap block without launching a worker, mounting D,
    # reading private configuration, or requiring a Windows host in CI.
    root = Path(__file__).resolve().parents[1]
    wrapper = (root / 'scripts/run_lakehouse_control.sh').read_text()
    block = wrapper.split('source scripts/runtime_env.sh\n', 1)[1].split(
        'stockagent_lakehouse_prefix=', 1)[0]
    (tmp_path / 'configs').mkdir()
    atomic_write_json(tmp_path / 'configs/training_rescue.json', {
        'contract': 'stockagent.training_rescue.v1', 'schema_version': 1,
        'windows_distribution': enrollment})
    environment = dict(os.environ)
    environment.pop('WSL_DISTRO_NAME', None)
    if existing is not None:
        environment['WSL_DISTRO_NAME'] = existing
    shell = ('set -euo pipefail\n'
             "uname() { printf 'microsoft\\n'; }\n"
             f'run_fintech_python() {{ "{sys.executable}" "$@"; }}\n')
    shell += block + '\nprintf "%s\\n" "$WSL_DISTRO_NAME"\n'
    result = subprocess.run(['bash', '-c', shell], cwd=tmp_path, env=environment,
                            text=True, capture_output=True)
    if expected is None:
        assert result.returncode != 0
    else:
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == expected


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


def test_control_backup_scan_timeout_is_retryable_without_dumping_or_republishing(tmp_path, monkeypatch):
    fail, calls = True, []
    def api(base, key, route, query=None, **kwargs):
        if route.endswith('system/status'):
            return {'myID': 'penguin'}
        if route.endswith('db/status'):
            return {'state': 'idle'}
        calls.append(query['sub'])
        if fail:
            raise TimeoutError('Syncthing scan is still running')
    monkeypatch.setattr(transport, 'credentials', lambda: ('local', 'private'))
    monkeypatch.setattr(transport, 'request', api)
    c = {'state_root': str(tmp_path), 'transport_root': str(tmp_path / 'ingress/lakehouse'),
         'producer_device_id': 'penguin'}
    identity = 'a' * 64
    first = lakehouse.notify_transport(c, control_backup_identity=identity)
    assert first['state'] == 'retry_scan' and first['error_type'] == 'TimeoutError'
    assert set(first['paths']) == {'tools/dispatch', 'batch-' + identity}
    fail = False
    second = lakehouse.notify_transport(c, control_backup_identity=identity)
    assert second['state'] == 'scan_requested' and not second['paths']
    assert lakehouse.notify_transport(c, control_backup_identity=identity) == second
    assert len(calls) == 3  # one failed request, then only the two retained paths
    assert not (tmp_path / 'pending-transport-scan.json').exists()


@pytest.mark.parametrize('identity,paths', [('not-a-hash', ()), ('a' * 64, ('lakehouse',))])
def test_control_backup_scan_cannot_widen_to_other_deliveries(tmp_path, identity, paths):
    with pytest.raises(ValueError):
        lakehouse.notify_transport({'state_root': str(tmp_path)}, paths, control_backup_identity=identity)


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


def test_nas_archive_receipt_is_distinct_from_corrupted_local_transport_retirement(delivery):
    c, identity, batch, original, proof = delivery
    row = {'file_keys': [original.relative_to(Path(c['cold_root'])).as_posix()+'@'+digest(original)],
           'bytes': original.stat().st_size}
    transport_file = batch/original.relative_to(Path(c['cold_root']))
    transport_file.write_bytes(b'x'*row['bytes'])
    # The receiver independently restored the original fixed archive. Later
    # local transport corruption does not erase that disaster-recovery proof.
    assert lakehouse.acceptance(c, identity, enrolled_source=row)['state'] == 'nas_archive_file_recovery_verified'
    with pytest.raises(ValueError, match='full SHA'):
        lakehouse.acceptance(c, identity)
    with pytest.raises(ValueError, match='full SHA'):
        cache.retire(c, identity, proof, apply=True)
    assert transport_file.exists() and original.read_bytes() == b'preserved authoritative bytes'


def test_nas_receipt_refuses_another_enrolled_source_membership(delivery):
    c, identity, batch, original, proof = delivery
    with pytest.raises(ValueError, match='enrolled source'):
        lakehouse.acceptance(c, identity, enrolled_source={'file_keys': ['different@'+'f'*64],
                                                         'bytes': original.stat().st_size})
    assert batch.exists() and original.exists()


@pytest.fixture
def catalog_delivery(tmp_path, monkeypatch):
    import shutil
    physical, lake, state, receipts = [tmp_path / name for name in ('ingress', 'lake', 'state', 'receipts')]
    for root in (physical, lake, state, receipts):
        root.mkdir()
    (physical / '.staging').mkdir()
    staging = lake / 'prepared'
    staging.mkdir()
    (staging / 'catalog-state.json').write_bytes(b'fixed registry rows and logical identity')
    manifest = seal(staging, {'kind': 'ducklake_catalog_and_data', 'ducklake_snapshot_id': 7,
                             'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'})
    identity = manifest['identity_sha256']
    source = lake / 'releases' / ('lake-' + identity)
    source.parent.mkdir()
    staging.rename(source)
    batch = physical / 'lakehouse' / source.name
    batch.parent.mkdir()
    shutil.copytree(source, batch)
    c = {'lake_root': str(lake), 'transport_root': str(batch.parent), 'receipt_root': str(receipts),
         'state_root': str(state), 'transport_cache_retirement': True,
         'producer_device_id': 'penguin', 'receiver_device_id': 'lab203'}
    copied = {'contract': ACK, 'delivery_identity_sha256': identity, 'manifest_file_sha256': digest(source / 'manifest.json'),
              'complete_files': 3, 'complete_bytes': sum(p.stat().st_size for p in source.iterdir()),
              'all_files_sha256_verified': True, 'exact_file_set_verified': True,
              'source_unchanged_verified': True, 'command_exit_codes': [0, 0]}
    enrolled = {'state': 'published', 'snapshot_id': 7, 'delivery_identity_sha256': identity,
                'delivery_root': str(source), 'transport_root': str(batch), 'replication': copied}
    atomic_write_json(state / 'export-7.json', enrolled)
    ack = {**copied, 'producer_device_id': 'penguin', 'receiver_device_id': 'lab203',
           'restore_command_exit_codes': [0, 0], 'catalog_semantic_restore_verified': True,
           **{key: True for key in ('nas_independent_restore_verified', 'nas_mount_guard_verified',
                                    'single_owner_verified', 'runtime_lock_verified')}}
    atomic_write_json(receipts / (batch.name + '.json'), {**ack, 'identity_sha256': identity_sha256(ack)})
    monkeypatch.setattr(cache, 'PHYSICAL_TRANSPORT_ROOT', physical)
    monkeypatch.setattr(cache, 'process_references_many', lambda roots: [])
    return c, identity, enrolled, source, batch, ack


@pytest.mark.parametrize('damage', ['primary', 'transport'])
def test_enrolled_catalog_nas_proof_survives_local_damage_but_gc_preserves_both(catalog_delivery, damage):
    c, identity, enrolled, source, batch, ack = catalog_delivery
    changed = (source if damage == 'primary' else batch) / 'catalog-state.json'
    changed.write_bytes(b'x' * changed.stat().st_size)
    proof = lakehouse.acceptance(c, identity, enrolled_catalog=enrolled)
    assert proof['catalog_semantic_restore_verified']
    assert proof['admission_basis'] == 'enrolled_closed_catalog_and_paired_nas_restore'
    if damage == 'primary':
        with pytest.raises(ValueError, match='full SHA'):
            lakehouse.acceptance(c, identity)
    with pytest.raises(ValueError):
        cache.retire(c, identity, proof, apply=True)
    assert source.is_dir() and batch.is_dir() and changed.read_bytes().startswith(b'x')
    assert json.loads((Path(c['receipt_root']) / (batch.name + '.json')).read_bytes())['identity_sha256'] == identity_sha256(ack)


@pytest.mark.parametrize('damage', ['snapshot', 'state', 'root', 'manifest_sha', 'bytes', 'boolean_exit_code'])
def test_catalog_admission_requires_exact_completed_producer_enrollment(catalog_delivery, damage):
    c, identity, enrolled, source, batch, _ = catalog_delivery
    wrong = deepcopy(enrolled)
    if damage == 'snapshot':
        wrong['snapshot_id'] += 1
    elif damage == 'state':
        wrong['state'] = 'publication_prepared'
    elif damage == 'root':
        wrong['delivery_root'] = str(source.parent / 'different')
    elif damage == 'manifest_sha':
        wrong['replication']['manifest_file_sha256'] = 'f' * 64
    elif damage == 'bytes':
        wrong['replication']['complete_bytes'] += 1
    else:
        wrong['replication']['command_exit_codes'] = [False, 0]
    with pytest.raises(ValueError, match='enrolled catalog'):
        lakehouse.acceptance(c, identity, enrolled_catalog=wrong)
    assert source.exists() and batch.exists()


@pytest.mark.parametrize('damage', ['semantic_restore', 'pairing'])
def test_catalog_enrollment_never_substitutes_for_paired_nas_native_restore(catalog_delivery, damage):
    c, identity, enrolled, source, batch, ack = catalog_delivery
    wrong = deepcopy(ack)
    wrong['catalog_semantic_restore_verified' if damage == 'semantic_restore' else 'receiver_device_id'] = (
        False if damage == 'semantic_restore' else 'unknown-peer')
    atomic_write_json(Path(c['receipt_root']) / (batch.name + '.json'), {**wrong, 'identity_sha256': identity_sha256(wrong)})
    with pytest.raises(ValueError):
        lakehouse.acceptance(c, identity, enrolled_catalog=enrolled)
    assert source.exists() and batch.exists()


def test_catalog_admission_refuses_conflicting_enrollment_scopes(catalog_delivery):
    c, identity, enrolled, *_ = catalog_delivery
    with pytest.raises(ValueError, match='conflicting enrollment'):
        lakehouse.acceptance(c, identity, enrolled_catalog=enrolled, enrolled_source={})


def test_catalog_enrollment_requires_exact_journal_name_and_ignores_prepared_intents(catalog_delivery):
    c, identity, enrolled, *_ = catalog_delivery
    assert lakehouse.catalog_enrollment(c, identity) == enrolled
    journal = Path(c['state_root']) / 'export-7.json'
    atomic_write_json(journal, {**enrolled, 'state': 'publication_prepared'})
    assert lakehouse.catalog_enrollment(c, identity) is None
    atomic_write_json(journal, enrolled)
    journal.rename(journal.with_name('export-8.json'))
    with pytest.raises(ValueError, match='enrollment identity'):
        lakehouse.catalog_enrollment(c, identity)


@pytest.mark.parametrize('kind', ['symlink', 'fifo'])
def test_catalog_enrollment_refuses_redirected_or_blocking_journals(catalog_delivery, kind):
    import os
    from stockagent.data_sync.desync_snapshots import SnapshotError
    c, identity, enrolled, source, batch, _ = catalog_delivery
    journal = Path(c['state_root']) / 'export-7.json'
    journal.unlink()
    unrelated = Path(c['state_root']) / 'unrelated'
    unrelated.write_bytes(b'preserve unrelated evidence')
    if kind == 'symlink':
        journal.symlink_to(unrelated)
    else:
        os.mkfifo(journal)
    with pytest.raises(SnapshotError):
        lakehouse.catalog_enrollment(c, identity)
    assert unrelated.read_bytes() == b'preserve unrelated evidence'
    assert source.exists() and batch.exists()


def test_catalog_enrollment_refuses_a_non_mapping_journal(catalog_delivery):
    c, identity, *_ = catalog_delivery
    (Path(c['state_root']) / 'export-7.json').write_text('[]')
    with pytest.raises(ValueError, match='not an enrollment'):
        lakehouse.catalog_enrollment(c, identity)


@pytest.mark.parametrize('damage', ['none', 'wrong_sha', 'source_change'])
def test_batched_primary_recovery_verification_preserves_sha_and_stability_gates(tmp_path, monkeypatch, damage):
    from stockagent.data_sync import windows_cold_io as native
    data = b'canonical observation'*(8*1024**2//21+2)
    sha = hashlib.sha256(data).hexdigest();relative=f'objects/blobs/{sha[:2]}/{sha}.blob'
    source=tmp_path/relative;source.parent.mkdir(parents=True);source.write_bytes(data)
    m={'context':{'kind':'immutable_source_objects','source_object_paths':{'payload/a':relative}},
       'files':{'payload/a':{'sha256':sha,'bytes':len(data)}}}
    batches=[]
    monkeypatch.setattr(native,'windows_path',lambda path:'D:\\fixed')
    def hashes(paths):
        batches.append(paths)
        if damage=='source_change':source.write_bytes(b'x'*len(data))
        return {p:('f'*64 if damage=='wrong_sha' else sha) for p in paths}
    monkeypatch.setattr(native,'hash_many',hashes)
    monkeypatch.setattr(cache,'digest',lambda path:pytest.fail('recovery hashes should use the measured batch path'))
    if damage=='none':
        assert cache.validate_sources({'cold_root':str(tmp_path)},'a'*64,m)['payload/a']
    else:
        with pytest.raises(ValueError,match='cannot reconstruct'):
            cache.validate_sources({'cold_root':str(tmp_path)},'a'*64,m)
    assert batches==[[source]] and source.exists()


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


def test_accepted_history_is_not_rewritten_once_per_wave(delivery, monkeypatch):
    c, identity, batch, original, proof = delivery
    sha = hashlib.sha256(original.read_bytes()).hexdigest()
    relative = original.relative_to(Path(c['cold_root'])).as_posix()
    key = relative + '@' + sha
    ledger = Path(c['state_root']) / 'source-replication-ledger.json'
    atomic_write_json(ledger, {'deliveries': {identity: {'file_keys': [key], 'bytes': original.stat().st_size,
        'nas_acceptance': lakehouse.acceptance(c, identity)}}})
    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return [(relative, sha, original.stat().st_size)]
        def close(self): pass
    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())
    write = lakehouse.atomic_write_json
    calls = []
    def observe(path, *args, **kwargs):
        calls.append(Path(path))
        return write(path, *args, **kwargs)
    monkeypatch.setattr(lakehouse, 'atomic_write_json', observe)
    lakehouse.stage_source_wave(c)
    assert ledger not in calls


def test_source_pipeline_prioritizes_unbacked_bytes_and_keeps_disjoint_pending_waves(source_wave, monkeypatch):
    c, chosen, physical, ledger_path, ledger, source = source_wave
    c['transport_root'] = str(physical / 'lakehouse')
    c['receipt_root'] = str(Path(c['state_root']) / 'receipts')
    Path(c['receipt_root']).mkdir()
    legacy = Path(c['state_root']) / 'restic'
    legacy.mkdir()
    entries = list(chosen)
    for index in range(3):
        data = ('unbacked-' + str(index)).encode() * 30
        sha = hashlib.sha256(data).hexdigest()
        relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
        p = Path(c['cold_root']) / relative
        p.parent.mkdir(parents=True, exist_ok=True);p.write_bytes(data)
        entries.append((relative, sha, len(data)))
    backed = chosen[0]
    atomic_write_json(legacy / 'ledger.json', {'deliveries': {'old': {'files': [
        {'relative': backed[0], 'sha256': backed[1], 'bytes': backed[2]}], 'acceptance': True}}})
    c['source_replication'] = {'maximum_pending_deliveries': 2, 'maximum_wave_files': 1,
        'maximum_wave_bytes': 1024**2, 'maximum_pending_bytes': 2 * 1024**2,
        'prioritize_uncovered_nas': True, 'restic_state_root': str(legacy)}
    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return entries
        def close(self): pass
    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())
    monkeypatch.setattr(lakehouse, 'notify_transport', lambda *args: None)
    monkeypatch.setattr(cache, 'PHYSICAL_TRANSPORT_ROOT', physical)
    monkeypatch.setattr(lakehouse.shutil, 'disk_usage', lambda root: SimpleNamespace(free=1024**4))
    first = lakehouse.stage_source_wave(c)
    second = lakehouse.stage_source_wave(c)
    third = lakehouse.stage_source_wave(c)
    assert len(first['pending_delivery_ids']) == 1
    assert len(second['pending_delivery_ids']) == len(third['pending_delivery_ids']) == 2
    persisted = json.loads((Path(c['state_root']) / 'source-replication-ledger.json').read_bytes())
    keys = [row['file_keys'][0] for row in persisted['deliveries'].values()]
    assert len(set(keys)) == 2 and backed[0] + '@' + backed[1] not in keys
    assert second['pending_bytes'] <= c['source_replication']['maximum_pending_bytes']


@pytest.mark.parametrize('has_acceptance', [False, True])
@pytest.mark.parametrize('redelivery_enabled', [False, True])
def test_source_monitoring_skips_restic_priority_without_new_candidates(
        source_wave, monkeypatch, has_acceptance, redelivery_enabled):
    from stockagent.data_sync import nas_coverage
    c, entries, physical, _, _, source = source_wave
    state = Path(c['state_root'])
    c['receipt_root'] = str(state / 'receipts')
    Path(c['receipt_root']).mkdir()
    c['source_replication'] = {
        'decouple_nas_acceptance': True, 'prioritize_uncovered_nas': True,
        'restic_state_root': str(state / 'restic'),
        'receiver_requested_redelivery': redelivery_enabled,
    }
    identity = 'a' * 64
    delivery = {'file_keys': [entries[0][0] + '@' + entries[0][1]], 'bytes': entries[0][2]}
    if has_acceptance:
        delivery['nas_acceptance'] = {'state': 'nas_archive_file_recovery_verified',
                                      'delivery_identity_sha256': identity}
    ledger_path = state / 'source-replication-ledger.json'
    atomic_write_json(ledger_path, {'deliveries': {identity: delivery}})
    original_ledger, original_source = ledger_path.read_bytes(), source.read_bytes()

    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return entries
        def close(self): pass

    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())
    monkeypatch.setattr(nas_coverage, 'read_ledger',
                        lambda *args: pytest.fail('no candidate needs a Restic priority ledger'))
    monkeypatch.setattr(lakehouse, '_publish_source_wave',
                        lambda *args, **kwargs: pytest.fail('published source must not be sent again'))
    result = lakehouse.stage_source_wave(c)
    assert result['unpublished_source_object_count'] == 0
    assert result['minimum_next_source_object_bytes'] is None
    assert result['pending_delivery_ids'] == ([] if has_acceptance else [identity])
    assert result['nas_archive_covered_objects'] == int(has_acceptance)
    assert ledger_path.read_bytes() == original_ledger
    assert source.read_bytes() == original_source
    assert not list(physical.iterdir())


def test_source_priority_still_reads_and_rejects_invalid_ledger_for_new_candidates(
        source_wave, monkeypatch):
    from stockagent.data_sync import nas_coverage
    c, entries, *_ = source_wave
    c['receipt_root'] = str(Path(c['state_root']) / 'receipts')
    c['source_replication'] = {'prioritize_uncovered_nas': True}

    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return entries
        def close(self): pass

    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())

    def invalid_ledger(path):
        raise ValueError('invalid NAS authority ledger')

    monkeypatch.setattr(nas_coverage, 'read_ledger', invalid_ledger)
    with pytest.raises(ValueError, match='invalid NAS authority ledger'):
        lakehouse.stage_source_wave(c)


@pytest.mark.parametrize('policy', [{'maximum_pending_deliveries': True}, {'maximum_wave_bytes': 9 * 1024**3},
    {'maximum_pending_deliveries': 0}, {'prioritize_uncovered_nas': 'true'}, {'unknown_limit': 7}])
def test_source_policy_refuses_unbounded_or_ambiguous_settings(policy):
    with pytest.raises(ValueError):
        lakehouse.source_wave_policy({'source_replication': policy})


@pytest.fixture
def independent_source(source_wave, monkeypatch):
    c, chosen, physical, ledger_path, ledger, source = source_wave
    c['transport_root'] = str(physical / 'lakehouse')
    c['receipt_root'] = str(Path(c['state_root']) / 'receipts')
    c['lake_root'] = str(Path(c['state_root']) / 'lake')
    Path(c['receipt_root']).mkdir()
    entries = list(chosen)
    for index in range(6):
        data = ('independent-' + str(index)).encode() * 30
        sha = hashlib.sha256(data).hexdigest()
        relative = f'objects/blobs/{sha[:2]}/{sha}.blob'
        p = Path(c['cold_root']) / relative
        p.parent.mkdir(parents=True, exist_ok=True);p.write_bytes(data)
        entries.append((relative, sha, len(data)))
    class Catalog:
        def execute(self, sql): return self
        def fetchall(self): return entries
        def close(self): pass
    c['source_replication'] = {'decouple_nas_acceptance': True, 'maximum_wave_files': 1,
        'maximum_retained_transport_bytes': 1024**2, 'receiver_requested_redelivery': True}
    monkeypatch.setattr(lakehouse, 'guard', lambda policy: None)
    monkeypatch.setattr(lakehouse, 'connect', lambda *args, **kwargs: Catalog())
    monkeypatch.setattr(lakehouse, 'notify_transport', lambda *args: None)
    monkeypatch.setattr(cache, 'PHYSICAL_TRANSPORT_ROOT', physical)
    monkeypatch.setattr(lakehouse.shutil, 'disk_usage', lambda root: SimpleNamespace(free=1024**4))
    return c, entries, Path(c['state_root'])/'source-replication-ledger.json'


def failed_receiver(c, identity, **changes):
    value = {'producer_device_id': c['producer_device_id'], 'receiver_device_id': c['receiver_device_id'],
        'nas_mount_guard_verified': True, 'single_owner_verified': True, 'runtime_lock_verified': True,
        'observed_at_utc': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
        'jobs': [{'delivery': 'lake-'+identity, 'state': 'deferred', 'error_type': 'RuntimeError'}], **changes}
    atomic_write_json(Path(c['receipt_root'])/'relay-status.json', value)


def test_source_confirms_exact_nas_receipt_and_sends_next_wave_without_local_batch_reread(independent_source, monkeypatch):
    from stockagent.data_sync import immutable_replication as immutable
    c, entries, ledger_path = independent_source
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    root = Path(c['transport_root'])/('lake-'+original)
    raw = (root/'manifest.json').read_bytes();m=json.loads(raw)
    ack = {'contract': ACK, 'delivery_identity_sha256': original,
           'manifest_file_sha256': hashlib.sha256(raw).hexdigest(),
           'complete_files': len(m['files'])+2,
           'complete_bytes': sum(v['bytes'] for v in m['files'].values())+len(raw)+65,
           'producer_device_id': c['producer_device_id'], 'receiver_device_id': c['receiver_device_id'],
           'command_exit_codes': [0, 0], 'restore_command_exit_codes': [0, 0],
           **{k: True for k in ('all_files_sha256_verified', 'exact_file_set_verified', 'source_unchanged_verified',
               'nas_independent_restore_verified', 'nas_mount_guard_verified', 'single_owner_verified', 'runtime_lock_verified')}}
    atomic_write_json(Path(c['receipt_root'])/('lake-'+original+'.json'),
                      {**ack, 'identity_sha256': identity_sha256(ack)})
    full_inventory = immutable.inventory
    def inventory(path):
        if path == root:raise AssertionError('NAS receipt admission blocks the sender on a local full reread')
        return full_inventory(path)
    monkeypatch.setattr(immutable, 'inventory', inventory)
    result = lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    assert result['nas_archive_covered_objects'] == 1 and rows[original]['nas_acceptance']
    assert len(rows) == 2 and len(result['pending_delivery_ids']) == 1


def test_source_keeps_sending_beyond_nas_window_and_refreshes_catalog(independent_source, monkeypatch):
    c, entries, ledger_path = independent_source
    c['source_replication']['refresh_source_catalog'] = True
    captures = []
    monkeypatch.setattr(lakehouse, 'register', lambda config: captures.append(True))
    results = [lakehouse.stage_source_wave(c) for _ in range(6)]
    assert len(results[-1]['pending_delivery_ids']) == 6 and len(captures) == 1
    assert results[-1]['publication_waits_for_nas_ack'] is False
    assert results[-1]['nas_archive_covered_bytes'] == 0
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    assert len({key for row in rows.values() for key in row['file_keys']}) == 6
    assert all(not row.get('nas_acceptance') for row in rows.values())


def test_failed_new_catalog_refresh_keeps_committed_source_sending_and_backs_off(independent_source, monkeypatch):
    c, entries, ledger_path = independent_source
    c['source_replication']['refresh_source_catalog'] = True
    calls = []
    def reject(config):
        calls.append(True)
        raise ValueError('new metadata is not yet admissible')
    monkeypatch.setattr(lakehouse, 'register', reject)
    first = lakehouse.stage_source_wave(c)
    second = lakehouse.stage_source_wave(c)
    assert len(second['pending_delivery_ids']) == 2 and len(calls) == 1
    assert first['catalog_refresh_error']['error_type'] == 'ValueError'
    assert second['catalog_refresh_error'] == first['catalog_refresh_error']
    assert not any(v.get('nas_acceptance') for v in json.loads(ledger_path.read_bytes())['deliveries'].values())


def test_rejected_catalog_metadata_diagnostics_preserve_previous_registry(independent_source, monkeypatch):
    from stockagent.data_sync import backup_stream
    c, entries, ledger_path = independent_source
    errors = [{'relative': 'heads/example/new.json', 'reason': 'SnapshotError'}]
    monkeypatch.setattr(backup_stream, 'capture_catalog', lambda *args, **kwargs: {'metadata_errors': errors})
    with pytest.raises(ValueError, match='metadata errors'):
        lakehouse.register(c)
    proof = json.loads((Path(c['state_root'])/'catalog-refresh-errors.json').read_bytes())
    assert proof['metadata_errors'] == errors and proof['last_committed_registry_preserved'] is True
    assert not (Path(c['state_root'])/'catalog-status.json').exists()


def test_independent_source_still_honors_retained_capacity(independent_source):
    c, entries, ledger_path = independent_source
    first = lakehouse.stage_source_wave(c)
    c['source_replication']['maximum_retained_transport_bytes'] = 1024**2
    data = json.loads(ledger_path.read_bytes())
    next(iter(data['deliveries'].values()))['bytes'] = 1024**2
    atomic_write_json(ledger_path, data)
    second = lakehouse.stage_source_wave(c)
    assert second['state'] == 'waiting_transport_capacity'
    assert second['pending_delivery_ids'] == first['pending_delivery_ids']


def test_positive_capacity_slack_that_cannot_fit_any_object_reports_backpressure(independent_source):
    c, entries, ledger_path = independent_source
    lakehouse.stage_source_wave(c)
    data = json.loads(ledger_path.read_bytes())
    next(iter(data['deliveries'].values()))['bytes'] = c['source_replication']['maximum_retained_transport_bytes'] - 1
    atomic_write_json(ledger_path, data)
    result = lakehouse.stage_source_wave(c)
    assert result['state'] == 'waiting_transport_capacity'
    assert result['publication_capacity_room_bytes'] == 1
    assert result['unpublished_source_object_count'] == len(entries) - 1
    assert result['minimum_next_source_object_bytes'] > 1
    assert len(json.loads(ledger_path.read_bytes())['deliveries']) == 1


@pytest.mark.parametrize('structured_request', [False, True])
def test_receiver_failure_requeues_exact_bytes_without_overwriting_failed_attempt(independent_source, structured_request):
    c, entries, ledger_path = independent_source
    first = lakehouse.stage_source_wave(c)
    original = first['pending_delivery_ids'][0]
    original_root = Path(c['transport_root'])/('lake-'+original)
    before = (original_root/'manifest.json').read_bytes()
    failed_receiver(c, original, **({'jobs': [{'delivery': 'lake-'+original, 'state': 'rejected',
        'error_type': 'ValueError', 'failure_kind': 'integrity_failure',
        'transport_action': 'request_redelivery'}]} if structured_request else {}))
    second = lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    retry = rows[original]['redeliveries'][0]
    assert retry != original and rows[retry]['retry_of'] == original
    assert rows[retry]['file_keys'] == rows[original]['file_keys']
    assert (original_root/'manifest.json').read_bytes() == before
    assert not rows[retry].get('nas_acceptance')
    # A retry still in progress does not generate another retry or block new data.
    third = lakehouse.stage_source_wave(c)
    assert len(third['pending_delivery_ids']) == 3
    assert json.loads(ledger_path.read_bytes())['deliveries'][original]['redeliveries'] == [retry]


def test_wrong_receiver_retry_and_bad_ack_do_not_block_other_data(independent_source, monkeypatch):
    c, entries, ledger_path = independent_source
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    failed_receiver(c, original, receiver_device_id='unknown-peer')
    (Path(c['receipt_root'])/('lake-'+original+'.json')).write_text('{}')
    monkeypatch.setattr(lakehouse, 'acceptance', lambda *args, **kwargs: (_ for _ in ()).throw(ValueError('bad ACK')))
    result = lakehouse.stage_source_wave(c)
    assert len(result['pending_delivery_ids']) == 2
    assert result['receipt_errors'] and result['redelivery_request_error'] == 'ValueError'
    assert not json.loads(ledger_path.read_bytes())['deliveries'][original].get('redeliveries')


@pytest.mark.parametrize('error_type', ['FileNotFoundError', 'TimeoutError', 'ConnectionError', 'CapacityError'])
def test_incomplete_ingress_or_temporary_nas_wait_keeps_sending_without_duplicate_retry(independent_source, error_type):
    c, entries, ledger_path = independent_source
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    failed_receiver(c, original, jobs=[{'delivery': 'lake-'+original, 'state': 'deferred',
                                      'error_type': error_type}])
    result = lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    assert len(result['pending_delivery_ids']) == 2
    assert not rows[original].get('redeliveries')
    assert len({key for row in rows.values() for key in row['file_keys']}) == 2


@pytest.mark.parametrize('state', ['deferred', 'rejected', 'retry_wait'])
@pytest.mark.parametrize('action', ['wait_for_Syncthing', 'waiting_for_manifest_and_READY'])
def test_receiver_explicit_transport_wait_keeps_sending_new_data(independent_source, state, action):
    c, entries, ledger_path = independent_source
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    root = Path(c['transport_root'])/('lake-'+original)
    original_manifest = (root/'manifest.json').read_bytes()
    original_payloads = {p.relative_to(root): p.read_bytes() for p in root.rglob('*') if p.is_file()}
    failed_receiver(c, original, jobs=[{'delivery': 'lake-'+original, 'state': state,
        'error_type': 'ValueError', 'failure_kind': 'transport_incomplete', 'transport_action': action}])
    result = lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    assert len(result['pending_delivery_ids']) == 2
    assert len({key for row in rows.values() for key in row['file_keys']}) == 2
    assert not rows[original].get('redeliveries') and not rows[original].get('nas_acceptance')
    assert (root/'manifest.json').read_bytes() == original_manifest
    assert all((root/p).read_bytes() == body for p, body in original_payloads.items())


def test_verified_redelivery_covers_original_keys_without_fabricating_original_ack(independent_source, monkeypatch):
    c, entries, ledger_path = independent_source
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    failed_receiver(c, original)
    lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    retry = rows[original]['redeliveries'][0]
    original_bytes = rows[original]['bytes']
    (Path(c['receipt_root'])/('lake-'+retry+'.json')).write_text('{}')
    monkeypatch.setattr(lakehouse,'acceptance',lambda config,key,**kwargs:{'state':'nas_archive_file_recovery_verified',
        'delivery_identity_sha256':key,'receipt_identity_sha256':'f'*64})
    result = lakehouse.stage_source_wave(c)
    persisted = json.loads(ledger_path.read_bytes())['deliveries']
    assert result['nas_archive_covered_bytes'] == original_bytes
    assert result['nas_archive_covered_objects'] == 1
    assert original not in result['pending_delivery_ids']
    assert persisted[retry]['nas_acceptance'] and not persisted[original].get('nas_acceptance')
    assert persisted[original]['redeliveries'] == [retry]
    assert (Path(c['transport_root'])/('lake-'+original)).exists()


def test_rejected_redelivery_retries_only_after_receiver_reports_its_failure(independent_source):
    c, entries, ledger_path = independent_source
    c['source_replication']['redelivery_cooldown_seconds'] = 1
    original = lakehouse.stage_source_wave(c)['pending_delivery_ids'][0]
    failed_receiver(c, original)
    lakehouse.stage_source_wave(c)
    rows = json.loads(ledger_path.read_bytes())['deliveries']
    first_retry = rows[original]['redeliveries'][0]
    rows[original]['last_redelivery_epoch'] = 0
    atomic_write_json(ledger_path,{'deliveries':rows})
    failed_receiver(c, first_retry)
    lakehouse.stage_source_wave(c)
    attempts = json.loads(ledger_path.read_bytes())['deliveries'][original]['redeliveries']
    assert len(attempts) == 2 and attempts[0] == first_retry and attempts[1] != first_retry


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
