"""A private backup uses the original owner without weakening source/GC proofs."""
import fcntl
import hashlib
import json
from pathlib import Path
import threading

import pytest

from test_backup_stream import stream, readiness, acknowledge
from stockagent.data_sync.backup_stream import capture_catalog
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import private_json
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
from stockagent.data_sync import private_cold_backup as private
from stockagent.data_sync.backup_transport_cache import retire


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
    queue, *_ = private_stream
    calls = []
    def scan(_ledger):
        with (queue.state / 'owner.lock').open('a') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            current = queue.load_ledger()
            current['concurrent_marker'] = 'preserved'
            queue._save_ledger(current)
            calls.append(True)
    monkeypatch.setattr(queue, 'scan_transport', scan)
    queue.private_history_cycle()
    assert calls and queue.load_ledger()['concurrent_marker'] == 'preserved'


def test_small_wave_target_does_not_strand_a_valid_larger_member(private_stream, monkeypatch):
    queue, *_ = private_stream
    monkeypatch.setattr(private, 'WAVE_BYTES', 16)
    result = queue.private_history_cycle()
    assert result['published_waves']
    assert result['state'] != 'private_object_exceeds_available_wave_budget'


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
