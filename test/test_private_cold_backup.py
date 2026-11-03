"""A private backup uses the original owner without weakening source/GC proofs."""
import fcntl
import hashlib
import json
from pathlib import Path
import threading
import time

import pytest

from test_backup_stream import stream, readiness, acknowledge
from stockagent.data_sync.backup_stream import capture_catalog
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import private_json
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
from stockagent.data_sync import private_cold_backup as private
from stockagent.data_sync.backup_transport_cache import retire


def test_stale_readiness_waits_then_resumes_without_repeating_large_catalog_reads(private_stream, monkeypatch):
    queue, *_ = private_stream
    readiness(queue, observed_at_utc='2020-01-01T00:00:00+00:00')
    first = queue.private_history_cycle()
    assert first['state'] == 'private_backup_waiting_receiver_readiness'
    lease = first['receiver_readiness']
    assert lease['state'] == 'expired'
    assert lease['observed_at_utc'] == '2020-01-01T00:00:00+00:00'
    assert lease['valid_until_utc'] == '2020-01-01T00:15:00+00:00'
    assert lease['checked_at_utc'] == first['activity_at_utc']
    assert not first['published_waves'] and first['nas_covered_bytes'] == 0
    coverage = json.loads((queue.state / 'private-coverage.json').read_text())
    assert coverage['contract'] == private.COVERAGE_CONTRACT
    assert coverage['observed_at_utc'] == first['observed_at_utc'] or coverage['observed_at_utc'] < first['observed_at_utc']
    assert coverage['catalog_identity_sha256'] == first['catalog_identity_sha256']
    original_load = queue.load_ledger
    monkeypatch.setattr(queue, 'load_ledger', lambda: pytest.fail('unchanged stale lease reread the ledger'))
    second = queue.private_history_cycle()
    assert second['observed_at_utc'] == first['observed_at_utc']
    assert second['activity_at_utc'] >= first['activity_at_utc']
    assert second['receiver_readiness']['observed_at_utc'] == lease['observed_at_utc']
    assert second['receiver_readiness']['valid_until_utc'] == lease['valid_until_utc']
    assert second['receiver_readiness']['checked_at_utc'] == second['activity_at_utc']
    assert second['source_files_deleted'] == 0
    readiness(queue, nas_mount_guard_verified=False)
    with pytest.raises(ValueError, match='owner, mount and runtime'):
        queue.private_history_cycle()
    monkeypatch.setattr(queue, 'load_ledger', original_load)
    readiness(queue)
    resumed = queue.private_history_cycle()
    assert resumed['published_waves'] and 'receiver_readiness' not in resumed


def test_only_typed_readiness_clock_wait_is_deferred(private_stream, monkeypatch):
    def invalid(*a, **k):
        raise ValueError('relay readiness is stale or in the future')
    monkeypatch.setattr(private, 'validate_readiness', invalid)
    with pytest.raises(ValueError):
        private.receiver_ready(private_stream[0], {})


@pytest.fixture
def private_stream(stream, tmp_path):
    queue, cold, *_ = stream
    root = cold / '.local-state/private-history'
    initialize_packed_layout(root, node_id='penguin')
    root.chmod(0o700)
    private_json(root / 'private-history.json', {
        'contract': 'all_catalog_incremental_migration_v1', 'parent_authority': str(cold),
        'shared_export': False, 'nas_export': False, 'source_retirement': False})
    source = tmp_path / 'private-source'
    source.mkdir()
    (source / 'raw.txt').write_text('raw negative value -2.5 股\n')
    first = publish_packed_snapshot(root, 'restricted', source, loose_file_threshold_bytes=1)
    (source / 'raw.txt').write_text('second raw vintage -3.5 股\n')
    second = publish_packed_snapshot(root, 'restricted', source, loose_file_threshold_bytes=1)
    catalog = Path(queue.config['publication_catalog'])
    body = json.loads(catalog.read_text())
    body['datasets'].append({'dataset': 'restricted', 'publish': False})
    catalog.write_text(json.dumps(body))
    queue.config['private_history_backup'] = {
        'contract': private.CONTRACT, 'enabled': True,
        'source_relative_root': '.local-state/private-history',
        'private_backup_authorized': True, 'public_publish_allowed': False,
        'source_cleanup_authorized': False, 'route': 'existing_paired_encrypted_Restic_relay',
        'scope': 'all_retained_private_immutable_manifests',
        'user_instruction_date': '2026-10-28', 'user_instruction': 'Back up my personal immutable history.'}
    queue.pipeline = {'maximum_waves_per_cycle': 2, 'copy_workers': 2, 'verify_workers': 2,
                      'retry_delays_seconds': [30, 120, 600]}
    queue.config['maximum_batch_files'] = 512
    readiness(queue)
    return queue, root, source, first, second


def test_private_closure_retains_all_vintages_and_deduplicates_objects(private_stream):
    queue, root, _, first, second = private_stream
    original_marker = (root / 'private-history.json').read_bytes()
    # The ordinary public collector still refuses the restricted dataset.
    with pytest.raises(SnapshotError, match='no valid retained'):
        capture_catalog(root, Path(queue.config['publication_catalog']))
    policy = queue.config['private_history_backup']
    catalog = capture_catalog(root, Path(queue.config['publication_catalog']), private_backup_policy=policy)
    assert {r['snapshot_id'] for r in catalog['releases']} == {
        first.manifest['snapshot_id'], second.manifest['snapshot_id']}
    names = [r['relative'] for r in catalog['files']]
    assert len(set(names)) == len(names) and 'private-history.json' in names
    assert (root / 'private-history.json').read_bytes() == original_marker
    assert policy['public_publish_allowed'] is False


@pytest.mark.parametrize('change', ['authorization', 'marker', 'redirect'])
def test_invalid_private_authority_never_stages(private_stream, change):
    queue, root, *_ = private_stream
    if change == 'authorization':
        queue.config['private_history_backup']['private_backup_authorized'] = False
    elif change == 'marker':
        marker = json.loads((root / 'private-history.json').read_bytes())
        marker['nas_export'] = True
        private_json(root / 'private-history.json', marker)
    else:
        root.rename(root.with_name('original'))
        root.symlink_to(root.with_name('original'), target_is_directory=True)
    with pytest.raises(SnapshotError):
        queue.private_history_cycle()
    assert not list(queue.transport.glob('batch-*'))


def test_original_owner_blocks_publication_but_not_metadata_capture(private_stream):
    queue, *_ = private_stream
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            queue.private_history_cycle()
    assert (queue.state / 'private-catalog.json').exists()
    assert not list(queue.transport.glob('batch-*'))
    status = json.loads((queue.state / 'private-status.json').read_bytes())
    catalog = json.loads((queue.state / 'private-catalog.json').read_bytes())
    assert status['state'] == 'private_backup_waiting_publication_owner'
    assert status['available_file_count'] == len(catalog['files'])
    assert status['available_bytes'] == sum(row['bytes'] for row in catalog['files'])
    assert status['nas_covered_file_count'] is None
    assert status['registered_private_closure_NAS_verified'] is False
    assert status['canonical_private_reconstruction_verified'] is False


def test_failed_wave_is_recorded_before_next_wave_and_does_not_poison_its_status(private_stream, monkeypatch):
    queue, *_ = private_stream
    queue.config['maximum_batch_files'] = 1
    events = []
    original_status_write = private.private_json
    original_export = private.export_incremental_delivery
    def record_status(path, value, **kwargs):
        if path == queue.state / 'private-status.json':
            events.append(json.loads(json.dumps(value)))
        return original_status_write(path, value, **kwargs)
    def fail_first_export(root, original, staging, **kwargs):
        if not any(event.get('state') == 'private_export_retry_required' for event in events):
            staging.mkdir(parents=True)
            (staging / 'partial').write_bytes(b'retained')
            raise SnapshotError('original read error')
        return original_export(root, original, staging, **kwargs)
    monkeypatch.setattr(private, 'private_json', record_status)
    monkeypatch.setattr(private, 'export_incremental_delivery', fail_first_export)
    result = queue.private_history_cycle()
    failed = next(event for event in events if event['state'] == 'private_export_retry_required')
    subsequent = next(event for event in events[events.index(failed) + 1:]
                      if event['state'] == 'copying_and_independently_verifying_private_wave')
    assert failed['error_reason'] == 'original read error'
    assert Path(failed['retained_staging'], 'partial').read_bytes() == b'retained'
    assert subsequent['last_failed_wave']['error_reason'] == 'original read error'
    assert 'error_reason' not in subsequent and 'retained_staging' not in subsequent
    assert result['published_waves'] and result['last_failed_wave']['error_reason'] == 'original read error'
    assert queue.load_ledger()['private_export_retries']


def test_supervised_owner_wait_joins_handoff_instead_of_polling_past_it(private_stream):
    queue, *_ = private_stream
    lock = queue.state / 'owner.lock'
    acquired = threading.Event()
    errors = []
    def contender():
        try:
            with private.publication_owner(queue, lock, wait_for_owner=True):
                acquired.set()
        except Exception as error:
            errors.append(error)
    with lock.open('a') as first:
        fcntl.flock(first, fcntl.LOCK_EX | fcntl.LOCK_NB)
        thread = threading.Thread(target=contender)
        thread.start()
        assert not acquired.wait(0.05)
    thread.join(timeout=5)
    assert acquired.is_set() and not thread.is_alive() and not errors


def test_private_delivery_ACK_restart_and_exact_transport_retirement(private_stream):
    queue, root, source, first, second = private_stream
    before = (source / 'raw.txt').read_bytes()
    result = queue.private_history_cycle()
    assert result['published_waves'] and result['pending_private_bytes'] > 0
    assert result['registered_private_closure_NAS_verified'] is False
    ledger = queue.load_ledger()
    for delivery in ledger['deliveries'].values():
        assert delivery['dispatch']['delivery_kind'] == private.KIND
        assert all(r['relative'].startswith(private.NAMESPACE) for r in delivery['files'])
        assert set(delivery['transport_signatures']) == {r['relative'] for r in delivery['files']}
        acknowledge(queue, delivery)
    accepted = queue.private_history_cycle()
    assert accepted['registered_private_closure_NAS_verified'] is True
    assert accepted['all_project_sources_NAS_verified'] is False
    assert accepted['retained_releases_nas_file_covered'] == 2
    assert accepted['pending_private_bytes'] == 0
    ledger = queue.load_ledger()
    for key in ledger['deliveries']:
        proof = retire(queue, ledger, key)
        assert proof['state'] == 'retired'
        assert proof['source_objects_deleted'] == proof['NAS_snapshots_deleted'] == 0
    assert not list(queue.transport.glob('batch-*'))
    assert first.manifest_path.exists() and second.manifest_path.exists()
    assert (source / 'raw.txt').read_bytes() == before
    # A later source version re-enters the same queue after acknowledged closure.
    (source / 'raw.txt').write_text('third actual vintage\n')
    third = publish_packed_snapshot(root, 'restricted', source, loose_file_threshold_bytes=1)
    refreshed = queue.private_history_cycle()
    assert refreshed['retained_release_count'] == 3
    assert refreshed['pending_private_bytes'] > 0 and third.manifest_path.exists()


def test_successive_waves_share_one_conservative_capacity_census_under_same_owner(private_stream, monkeypatch):
    queue, *_ = private_stream
    queue.config['maximum_batch_files'] = 2
    scans = []
    original = queue.transport_usage
    def measure():
        scans.append(True)
        return original()
    monkeypatch.setattr(queue, 'transport_usage', measure)
    result = queue.private_history_cycle()
    assert len(result['published_waves']) == 2
    assert len(scans) == 1
    assert result['registered_private_closure_NAS_verified'] is False


def test_fixed_private_head_bytes_survive_a_new_source_publication(private_stream):
    queue, root, source, *_ = private_stream
    with (queue.state / 'owner.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            queue.private_history_cycle()
    fixed = json.loads((queue.state / 'private-catalog.json').read_bytes())
    head = next(row for row in fixed['files'] if row['relative'].startswith('private-history/heads/'))
    (source / 'raw.txt').write_text('new source value after fixed backup capture')
    publish_packed_snapshot(root, 'restricted', source, loose_file_threshold_bytes=1)
    assert (root / head['relative'][len(private.NAMESPACE):]).read_text() != head['captured_bytes_utf8']
    result = queue.private_history_cycle(wait_for_owner=True)
    ledger = queue.load_ledger()
    key = next(key for key in result['published_waves']
               if any(row['relative'] == head['relative'] for row in ledger['deliveries'][key]['files']))
    copied = queue.transport / ('batch-' + key) / head['relative']
    assert copied.read_text() == head['captured_bytes_utf8']
    assert result['catalog_identity_sha256'] == fixed['identity_sha256']
    assert result['canonical_private_reconstruction_verified'] is False


@pytest.mark.parametrize('damage', [None, 'source_changed', 'target_corrupt', 'partial_failure'])
def test_small_native_object_groups_keep_independent_proofs_and_partial_evidence(private_stream, monkeypatch, damage):
    from stockagent.data_sync import offhost_backup, windows_cold_io as native
    queue, root, source, *_ = private_stream
    for i in range(21):
        (source / f'raw-{i}.txt').write_text(f'unique raw value {i} 股')
    publish_packed_snapshot(root, 'restricted', source, loose_file_threshold_bytes=1)
    catalog = capture_catalog(root, Path(queue.config['publication_catalog']),
                              private_backup_policy=queue.config['private_history_backup'])
    staged = queue.transport / '.staging' / 'native-small-group-proof'
    calls, call_lock = [], threading.Lock()
    def windows_path(path):
        if path.is_relative_to(root):
            return 'D:\\stockagent-cold-primary\\private\\' + path.relative_to(root).as_posix()
        if path.is_relative_to(queue.transport):
            return 'D:\\stockagent-backup-ingress-lab203\\.staging\\' + path.relative_to(queue.transport).as_posix()
        return None
    def batch(members):
        with call_lock:
            first = not calls
            calls.append(members)
        assert 1 <= len(members) <= 16
        for origin, destination, sha, size in members:
            raw = origin.read_bytes()
            assert len(raw) == size and hashlib.sha256(raw).hexdigest() == sha
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(raw)
            if first and damage == 'partial_failure':
                raise SnapshotError('native copy interrupted; partial member retained')
        if first and damage == 'source_changed':
            members[0][0].write_bytes(b'changed source after native copy')
        if first and damage == 'target_corrupt':
            members[0][1].write_bytes(b'corrupted destination')
    monkeypatch.setattr(native, 'windows_path', windows_path)
    monkeypatch.setattr(native, 'copy_many', batch)
    monkeypatch.setattr(native, 'hash_many', lambda paths: {
        path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
    def export():
        return offhost_backup.export_incremental_delivery(root, catalog['files'], staged,
            catalog_identity=catalog['identity_sha256'], namespace='private-history',
            copy_workers=4, verify_workers=4, native_windows_io=True)
    if damage:
        with pytest.raises((SnapshotError, ValueError)):
            export()
        assert staged.exists() and not (staged / 'READY').exists()
        assert any(path.is_file() for path in staged.rglob('*'))
    else:
        exported = export()
        assert exported['copied_files'] == len(catalog['files'])
        assert len(calls) >= 2 and len(calls) < len(catalog['files']) // 4
        assert any(len(group) == 16 for group in calls)
        for row in catalog['files']:
            assert hashlib.sha256((staged / 'private-history' / row['relative']).read_bytes()).hexdigest() == row['sha256']
        assert exported['canonical_reconstruction_verified'] is False


@pytest.mark.parametrize('changed_field', [0, 1, 2, 3, 4])
def test_private_guarded_export_only_accepts_historical_device_renumber(
        private_stream, changed_field):
    from stockagent.data_sync.offhost_backup import export_incremental_delivery

    queue, root, *_ = private_stream
    catalog = capture_catalog(root, Path(queue.config['publication_catalog']),
                              private_backup_policy=queue.config['private_history_backup'])
    original = next(row for row in catalog['files'] if row['relative'].startswith('objects/'))
    row = {**original, 'signature': list(original['signature'])}
    row['signature'][changed_field] += 1
    options = {'catalog_identity': catalog['identity_sha256'], 'namespace': 'private-history'}
    without_guard = queue.transport / '.staging' / 'renumber-no-guard'
    with pytest.raises(SnapshotError, match='source changed'):
        export_incremental_delivery(root, [row], without_guard, **options)
    assert not (without_guard / 'READY').exists()

    guarded = queue.transport / '.staging' / 'renumber-guarded'
    guards = []
    if changed_field:
        with pytest.raises(SnapshotError, match='source changed'):
            export_incremental_delivery(root, [row], guarded,
                                        source_authority_guard=lambda: guards.append(True), **options)
        assert not (guarded / 'READY').exists()
    else:
        result = export_incremental_delivery(root, [row], guarded,
                                             source_authority_guard=lambda: guards.append(True), **options)
        payload = guarded / 'private-history' / row['relative']
        assert hashlib.sha256(payload.read_bytes()).hexdigest() == row['sha256']
        assert len(guards) == 2 and (guarded / 'READY').is_file()
        assert result['durable_off_host_backup_verified'] is False


def test_private_guarded_export_rejects_device_change_during_copy(private_stream, monkeypatch):
    from stockagent.data_sync import offhost_backup

    queue, root, *_ = private_stream
    catalog = capture_catalog(root, Path(queue.config['publication_catalog']),
                              private_backup_policy=queue.config['private_history_backup'])
    row = next(row for row in catalog['files'] if row['relative'].startswith('objects/'))
    original = offhost_backup.PinnedObjectSignatures.signature
    calls = []

    def changed_after_admission(owner, relative):
        observed = original(owner, relative)
        calls.append(relative)
        return observed if len(calls) == 1 else (observed[0] + 1, *observed[1:])

    monkeypatch.setattr(offhost_backup.PinnedObjectSignatures, 'signature', changed_after_admission)
    staged = queue.transport / '.staging' / 'mid-copy-renumber'
    with pytest.raises(SnapshotError, match='source changed'):
        offhost_backup.export_incremental_delivery(root, [row], staged,
            catalog_identity=catalog['identity_sha256'], namespace='private-history',
            source_authority_guard=lambda: None)
    assert not (staged / 'READY').exists() and len(calls) >= 2


@pytest.mark.parametrize('change', ['source', 'target', 'namespace', 'no_ack'])
def test_private_transport_retirement_preserves_changed_or_unacknowledged_files(private_stream, change):
    queue, root, *_ = private_stream
    queue.private_history_cycle()
    ledger = queue.load_ledger()
    for delivery in ledger['deliveries'].values():
        acknowledge(queue, delivery)
    assert queue.ingest(ledger) == []
    key, delivery = next(iter(ledger['deliveries'].items()))
    batch = queue.transport / delivery['dispatch']['batch_relative']
    row = next(r for r in delivery['files'] if r['relative'].startswith(private.NAMESPACE + 'objects/'))
    if change == 'source':
        (root / row['relative'][len(private.NAMESPACE):]).write_bytes(b'changed original')
    elif change == 'target':
        (batch / row['relative']).write_bytes(b'changed transport')
    elif change == 'namespace':
        row['relative'] = 'objects/elsewhere'
    else:
        delivery.pop('acceptance')
    with pytest.raises((SnapshotError, ValueError)):
        retire(queue, ledger, key)
    assert batch.is_dir()


def test_pending_ACK_waits_without_capacity_scans_republishing_or_source_deletion(private_stream, monkeypatch):
    queue, root, source, *_ = private_stream
    first = queue.private_history_cycle()
    ledger = queue.load_ledger()
    queue.config['maximum_pending_bytes'] = 1
    def no_unnecessary_scan():
        raise AssertionError('an ACK wait must not rescan the whole transport')
    monkeypatch.setattr(queue, 'transport_usage', no_unnecessary_scan)
    second = queue.private_history_cycle()
    assert second['state'] == 'private_files_published_waiting_NAS_ACK'
    assert second['source_files_deleted'] == 0
    assert len(queue.load_ledger()['deliveries']) == len(ledger['deliveries'])
    assert first['catalog_identity_sha256'] == second['catalog_identity_sha256']
    assert root.is_dir() and (source / 'raw.txt').exists()


@pytest.mark.parametrize('bound', ['pending', 'source_batch', 'receiver_batch', 'ingress', 'nas', 'source_physical'])
def test_capacity_bound_wait_skips_census_but_capacity_recovery_requires_it(private_stream, monkeypatch, bound):
    queue, root, source, *_ = private_stream
    one_mib = 1024**2
    if bound == 'pending':
        queue.config['maximum_pending_bytes'] = one_mib
    elif bound == 'source_batch':
        queue.config['maximum_batch_bytes'] = one_mib
    elif bound == 'receiver_batch':
        readiness(queue, maximum_batch_bytes=one_mib)
    elif bound == 'source_physical':
        from types import SimpleNamespace
        monkeypatch.setattr(private.shutil, 'disk_usage',
                            lambda path: SimpleNamespace(free=queue.config['reserve_bytes']))
    else:
        readiness(queue, **{bound + '_free_bytes': queue.config['reserve_bytes'] + one_mib})
    def census_forbidden():
        raise AssertionError('a separately proven capacity wait must not scan the transport')
    monkeypatch.setattr(queue, 'transport_usage', census_forbidden)
    blocked = queue.private_history_cycle()
    assert blocked['state'] == ('private_object_exceeds_negotiated_batch_limit'
        if bound in ('source_batch', 'receiver_batch') else 'private_backup_waiting_capacity')
    assert not blocked['published_waves'] and blocked['source_files_deleted'] == 0
    assert root.is_dir() and (source / 'raw.txt').is_file()

    queue.config['maximum_pending_bytes'] = 4 * one_mib
    queue.config['maximum_batch_bytes'] = 2 * one_mib
    readiness(queue)
    if bound == 'source_physical':
        monkeypatch.setattr(private.shutil, 'disk_usage',
                            lambda path: SimpleNamespace(free=queue.config['reserve_bytes'] + 64 * one_mib))
    with pytest.raises(AssertionError, match='must not scan'):
        queue.private_history_cycle()


def test_authorized_backup_pool_resumes_closed_private_waves_below_general_reserve(private_stream, monkeypatch):
    from types import SimpleNamespace
    from stockagent.data_sync.backup_stream import BackupStream
    queue, root, source, *_ = private_stream
    gib = 1024**3
    queue.config['reserve_bytes'] = 64*gib
    readiness(queue, ingress_free_bytes=1024*gib, nas_free_bytes=4096*gib, reserve_bytes=64*gib)
    monkeypatch.setattr(private.shutil, 'disk_usage', lambda path: SimpleNamespace(free=40*gib))
    ordinary = BackupStream(queue.config)
    blocked = ordinary.private_history_cycle()
    assert blocked['state'] == 'private_backup_waiting_capacity'
    assert blocked['wave_budget']['source_reserve_bytes'] == 64*gib
    assert blocked['wave_budget']['uses_backup_reserve'] is False
    assert not blocked['published_waves']

    queue.config['private_backup_capacity'] = {
        'contract': 'private_closed_wave_backup_reserve_v1', 'use_backup_reserve': True,
        'minimum_free_bytes': 32*gib, 'user_instruction': 'Use the existing backup reserve for closed waves.'}
    enrolled = BackupStream(queue.config)
    admitted, exports = [], []
    original_census, original_export = enrolled.transport_usage, private.export_incremental_delivery
    def census():
        admitted.append(True)
        return original_census()
    def export(*args, **kwargs):
        exports.append(kwargs)
        return original_export(*args, **kwargs)
    monkeypatch.setattr(enrolled, 'transport_usage', census)
    monkeypatch.setattr(private, 'export_incremental_delivery', export)
    result = enrolled.private_history_cycle()
    assert result['published_waves'] and admitted and exports
    assert all(call['native_reserve_bytes'] == 32*gib for call in exports)
    assert result['wave_budget']['source_reserve_bytes'] == 32*gib
    assert result['wave_budget']['uses_backup_reserve'] is True
    assert result['source_files_deleted'] == 0 and result['nas_covered_bytes'] == 0
    assert root.is_dir() and (source/'raw.txt').is_file()
    # The ordinary public/export room retains its original reserve.
    usage = {'retained_transport_bytes': 0, 'retained_staging_bytes': 0}
    assert enrolled.staging_bounds(usage)['physical'] == -24*gib
    assert enrolled.staging_bounds(usage, reserve_bytes=32*gib)['physical'] == 8*gib


@pytest.mark.parametrize('change', ['contract', 'authorization', 'public', 'disabled', 'instruction',
                                    'unknown', 'boolean', 'below_native_floor', 'above_general_reserve'])
def test_private_backup_reserve_rejects_unbounded_or_unauthorized_policy(private_stream, change):
    from stockagent.data_sync.backup_stream import BackupStream
    queue, *_ = private_stream
    queue.config['reserve_bytes'] = 64*1024**3
    policy = {'contract': 'private_closed_wave_backup_reserve_v1', 'use_backup_reserve': True,
              'minimum_free_bytes': 32*1024**3, 'user_instruction': 'Use backup reserve.'}
    queue.config['private_backup_capacity'] = policy
    if change == 'authorization':queue.config['private_history_backup']['private_backup_authorized'] = False
    elif change == 'public':queue.config['private_history_backup']['public_publish_allowed'] = True
    elif change == 'disabled':policy['use_backup_reserve'] = False
    elif change == 'contract':policy['contract'] = 'unknown'
    elif change == 'instruction':policy['user_instruction'] = ''
    elif change == 'unknown':policy['bypass'] = True
    elif change == 'boolean':policy['minimum_free_bytes'] = True
    elif change == 'below_native_floor':policy['minimum_free_bytes'] = 32*1024**3-1
    else:policy['minimum_free_bytes'] = 65*1024**3
    with pytest.raises(SnapshotError, match='authorized bounded'):
        BackupStream(queue.config)


def test_backup_reserve_never_hides_retained_failed_staging_or_exhausts_physical_room(private_stream, monkeypatch):
    from types import SimpleNamespace
    from stockagent.data_sync.backup_stream import BackupStream
    queue, *_ = private_stream
    gib = 1024**3
    queue.config.update(reserve_bytes=64*gib, maximum_retained_transport_bytes=512*gib,
        maximum_staging_bytes=64*gib,
        private_backup_capacity={'contract': 'private_closed_wave_backup_reserve_v1',
            'use_backup_reserve': True, 'minimum_free_bytes': 32*gib,
            'user_instruction': 'Use the backup pool for bounded waves.'})
    enrolled = BackupStream(queue.config)
    free = [58*gib]
    monkeypatch.setattr(private.shutil, 'disk_usage', lambda path: SimpleNamespace(free=free[0]))
    usage = {'retained_staging_bytes': 26*gib, 'retained_transport_bytes': 121*gib}
    assert enrolled.staging_bounds(usage, reserve_bytes=32*gib) == {
        'staging': 38*gib, 'retained_transport': 391*gib, 'physical': 26*gib}
    assert enrolled.staging_room(usage, reserve_bytes=32*gib) == 26*gib
    assert enrolled.staging_room({**usage, 'retained_staging_bytes': 65*gib}, reserve_bytes=32*gib) < 0
    free[0] = 31*gib
    assert enrolled.staging_room(usage, reserve_bytes=32*gib) == -gib


@pytest.mark.parametrize('bound', ['pending', 'negotiated', 'physical'])
def test_unfilled_budget_reports_the_actual_bound_and_avoids_impossible_copy(private_stream, monkeypatch, bound):
    from stockagent.data_sync import backup_stream
    queue, *_ = private_stream
    mib = 1024**2
    queue.config.update(maximum_batch_bytes=32*mib, maximum_pending_bytes=64*mib)
    readiness(queue, maximum_batch_bytes=32*mib)
    capture = backup_stream.capture_catalog
    def sized_catalog(*args, **kwargs):
        catalog = capture(*args, **kwargs)
        for row in catalog['files']:
            row['bytes'] = 8*mib
        return catalog
    monkeypatch.setattr(backup_stream, 'capture_catalog', sized_catalog)
    censuses = []
    def census():
        censuses.append(True)
        assert bound == 'physical', 'a proven no-fit wait must not scan the whole transport'
        return {'retained_transport_bytes': 0, 'retained_staging_bytes': 0}
    monkeypatch.setattr(queue, 'transport_usage', census)
    monkeypatch.setattr(private, 'export_incremental_delivery',
                        lambda *a, **kw: pytest.fail('no wave fits; never copy'))
    if bound == 'pending':
        queue.config['maximum_pending_bytes'] = 5*mib
    elif bound == 'negotiated':
        queue.config['maximum_batch_bytes'] = 4*mib
    else:
        monkeypatch.setattr(queue, 'staging_room', lambda _usage: 5*mib)
    result = queue.private_history_cycle()
    assert result['state'] == ('private_object_exceeds_negotiated_batch_limit'
        if bound == 'negotiated' else 'private_backup_waiting_capacity')
    budget = result['wave_budget']
    assert budget['minimum_required_bytes'] == 9*mib
    assert budget['limiting_factors'] == [{'pending': 'pending_window',
        'negotiated': 'source_batch', 'physical': 'transport_and_physical_room'}[bound]]
    assert budget['capacity_census_performed'] is (bound == 'physical')
    assert bool(censuses) is (bound == 'physical')
    assert not result['published_waves'] and result['source_files_deleted'] == 0


def test_capacity_wait_still_refuses_allocation_with_stale_relay_readiness(private_stream, monkeypatch):
    queue, *_ = private_stream
    queue.config['maximum_pending_bytes'] = 1024**2
    readiness(queue, observed_at_utc='2020-01-01T00:00:00+00:00')
    monkeypatch.setattr(queue, 'transport_usage', lambda: pytest.fail('stale readiness needs no census'))
    result = queue.private_history_cycle()
    assert result['state'] == 'private_backup_waiting_receiver_readiness'
    assert not result['published_waves'] and result['source_files_deleted'] == 0


def test_recovered_capacity_cannot_publish_redirected_transport(private_stream):
    queue, _, source, *_ = private_stream
    queue.config['maximum_pending_bytes'] = 1024**2
    staging = queue.transport / '.staging'
    staging.mkdir(exist_ok=True)
    (staging / 'unsafe').symlink_to(source / 'raw.txt')
    blocked = queue.private_history_cycle()
    assert blocked['state'] == 'private_backup_waiting_capacity'
    assert not blocked['published_waves']
    queue.config['maximum_pending_bytes'] = 4 * 1024**2
    with pytest.raises(SnapshotError, match='redirected or nonregular'):
        queue.private_history_cycle()
    assert not list(queue.transport.glob('batch-*'))


def test_corrupt_fixed_plan_is_not_replaced_or_published(private_stream):
    queue, *_ = private_stream
    queue.private_history_cycle(publish=False)
    plan = queue.state / 'private-catalog.json'
    body = json.loads(plan.read_bytes())
    body['files'][0]['bytes'] += 1
    private_json(plan, body)
    corrupt = plan.read_bytes()
    with pytest.raises(SnapshotError, match='identity changed'):
        queue.private_history_cycle()
    assert plan.read_bytes() == corrupt and not list(queue.transport.glob('batch-*'))


def test_private_network_wait_yields_the_original_owner(private_stream, monkeypatch):
    from stockagent.data_sync.backup_stream import TRANSPORT_SCAN
    queue, *_ = private_stream
    queue.config['maximum_batch_files'] = 1
    scanning, second_copy, finish = threading.Event(), threading.Event(), threading.Event()
    outcome = {}
    exported = private.export_incremental_delivery
    copies = []
    def copy(*args, **kwargs):
        copies.append(True)
        if len(copies) == 2:
            assert scanning.wait(5), 'the first notification must overlap the second copy'
            second_copy.set()
        return exported(*args, **kwargs)
    def scan(ledger):
        envelope, delivery = next(iter(ledger['deliveries'].items()))
        assert delivery['state'] == 'published_waiting_nas_receipt'
        scanning.set()
        assert finish.wait(10)
        dispatch = delivery['dispatch']
        return {'contract': TRANSPORT_SCAN, 'state': 'scan_request_acknowledged',
                'NAS_backup_verified': False, 'batch_scan_receipts': [{
                    'state': 'scan_request_acknowledged', 'envelope': envelope,
                    'dispatch_identity_sha256': dispatch['identity_sha256'],
                    'batch_relative': dispatch['batch_relative']}]}
    def cycle():
        try:
            outcome['result'] = queue.private_history_cycle()
        except Exception as error:
            outcome['error'] = error
    monkeypatch.setattr(private, 'export_incremental_delivery', copy)
    monkeypatch.setattr(queue, 'scan_transport', scan)
    worker = threading.Thread(target=cycle)
    worker.start()
    try:
        assert second_copy.wait(5)
        with (queue.state / 'owner.lock').open('a') as owner:
            deadline = time.monotonic() + 5
            while True:
                try:
                    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    assert time.monotonic() < deadline, 'scan wait must release the owner'
                    time.sleep(.01)
            current = queue.load_ledger()
            assert len(current['deliveries']) == 2
            current['concurrent_marker'] = 'preserved'
            queue._save_ledger(current)
    finally:
        finish.set()
        worker.join(10)
    assert not worker.is_alive() and 'error' not in outcome, outcome
    ledger = queue.load_ledger()
    assert ledger['concurrent_marker'] == 'preserved'
    assert all(row['transport_scan_notification']['state'] == 'scan_request_acknowledged'
               and not row.get('acceptance') for row in ledger['deliveries'].values())
    assert len(outcome['result']['transport_scan_notifications']) == 2


def test_private_scan_failure_preserves_closed_waves_for_original_retry(private_stream, monkeypatch):
    queue, *_ = private_stream
    queue.config['maximum_batch_files'] = 1
    def failed(_ledger):
        raise OSError('notification unavailable')
    monkeypatch.setattr(queue, 'scan_transport', failed)
    result = queue.private_history_cycle()
    assert len(result['published_waves']) == 2
    assert result['transport_scan_errors'] == ['OSError', 'OSError']
    assert all(row['state'] == 'published_waiting_nas_receipt' and not row.get('acceptance')
               for row in queue.load_ledger()['deliveries'].values())
    assert all((queue.transport / ('batch-' + key) / 'READY').is_file()
               for key in result['published_waves'])


def test_explicit_private_waiter_queues_before_reading_the_fixed_catalog(private_stream, monkeypatch):
    from stockagent.data_sync import backup_stream
    queue, *_ = private_stream
    entered = threading.Event()
    original_capture = backup_stream.capture_catalog
    def capture(*args, **kwargs):
        entered.set()
        return original_capture(*args, **kwargs)
    monkeypatch.setattr(backup_stream, 'capture_catalog', capture)
    outcome = {}
    def cycle():
        try:
            outcome['result'] = queue.private_history_cycle(publish=False, wait_for_owner=True)
        except Exception as error:
            outcome['error'] = error
    with (queue.state / 'private-capture.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        worker = threading.Thread(target=cycle)
        worker.start()
        try:
            assert not entered.wait(.05), 'a queued waiter must not recapture before the handoff'
            assert worker.is_alive()
        finally:
            fcntl.flock(owner, fcntl.LOCK_UN)
    worker.join(10)
    assert not worker.is_alive() and 'error' not in outcome, outcome
    assert entered.is_set()
    assert outcome['result']['transport_scan_mode'] == 'per_closed_wave_async'


@pytest.mark.parametrize('bound', ['source_batch', 'receiver_batch', 'pending', 'ingress', 'nas', 'staging'])
def test_private_wave_fills_negotiated_capacity_without_the_old_latency_cap(
        private_stream, monkeypatch, bound):
    """Exercise real selection with synthetic sizes; stop before payload I/O."""
    from stockagent.data_sync import backup_stream
    queue, *_ = private_stream
    gib = 1024**3
    queue.config.update(maximum_batch_bytes=8*gib, maximum_pending_bytes=32*gib,
                        maximum_staging_bytes=24*gib, maximum_retained_transport_bytes=64*gib)
    ready = {'maximum_batch_bytes': 8*gib, 'ingress_free_bytes': 64*gib,
             'nas_free_bytes': 64*gib}
    if bound == 'source_batch':
        queue.config['maximum_batch_bytes'] = gib
    elif bound == 'receiver_batch':
        ready['maximum_batch_bytes'] = gib
    elif bound == 'pending':
        queue.config['maximum_pending_bytes'] = gib
    elif bound in ('ingress', 'nas'):
        ready[bound + '_free_bytes'] = gib + queue.config['reserve_bytes']
    else:
        queue.config['maximum_staging_bytes'] = gib
    readiness(queue, **ready)
    original_capture = backup_stream.capture_catalog
    def sized_catalog(*args, **kwargs):
        catalog = original_capture(*args, **kwargs)
        # Four 300-MiB metadata entries model fixed files. No large test files
        # or fake acceptance receipts are created.
        assert len(catalog['files']) >= 4
        for row in catalog['files'][:4]:
            row['bytes'] = 300 * 1024**2
        return catalog
    monkeypatch.setattr(backup_stream, 'capture_catalog', sized_catalog)
    monkeypatch.setattr(queue, 'transport_usage', lambda: {
        'retained_transport_bytes': 0, 'retained_staging_bytes': 0})
    selected = []
    class StopBeforeCopy(Exception):
        pass
    def inspect_wave(_root, rows, _staging, **_kwargs):
        selected.extend(rows)
        raise StopBeforeCopy
    monkeypatch.setattr(private, 'export_incremental_delivery', inspect_wave)
    with pytest.raises(StopBeforeCopy):
        queue.private_history_cycle()
    byte_count = sum(row['bytes'] for row in selected)
    assert 512 * 1024**2 < byte_count <= gib - 1024**2
    assert sum(row['bytes'] == 300 * 1024**2 for row in selected) == 3
    assert not queue.load_ledger()['deliveries']
    assert not list(queue.transport.glob('batch-*'))


def test_scope_audit_resolves_relative_sources_and_exposes_unregistered_roots(private_stream, tmp_path):
    queue, *_ = private_stream
    repository = tmp_path / 'project'
    (repository / 'data_finlab').mkdir(parents=True)
    (repository / 'data_not_enrolled').mkdir()
    catalog = Path(queue.config['publication_catalog'])
    catalog.write_text(json.dumps({'datasets': [
        {'dataset': 'restricted', 'source': 'data_finlab', 'publish': False}]}))
    result = private.source_scope_inventory(repository, queue.config)
    row = result['catalog_sources'][0]
    assert row['source'] == str(repository / 'data_finlab') and row['source_exists']
    assert row['private_cold_head_observed'] and not row['shared_publication_enabled']
    assert result['unregistered_data_roots'] == [str(repository / 'data_not_enrolled')]
    assert result['all_project_sources_NAS_verified'] is False
    assert result['metadata_only'] and result['source_files_deleted'] == 0
