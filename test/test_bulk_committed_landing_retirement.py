import fcntl
import os
from pathlib import Path

import pytest

from test_bulk_original_carrier_retirement import case
from stockagent.data_sync import bulk_committed_landing_retirement as landing
from stockagent.data_sync import packed_snapshots as packed, repack_retirement, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def committed(case):
    cold = case['options']['cold']
    receipt = case['new_receipt']
    stage, dataset, old_sha = case['stage'], receipt['dataset'], receipt['old_manifest_sha256']
    prepared = stream_repack._prepared_metadata(stage, dataset, old_sha)
    source = case['batch']/'publish-markets-parts-v2'
    replacement = packed.resolve_packed_snapshot_id(cold, dataset, receipt['new_snapshot_id'])
    items = []
    selected, digests = [], set()
    for row in packed._iter_inventory(cold, replacement.manifest):
        if row['sha256'] in digests:
            continue
        digests.add(row['sha256'])
        path = source/row['path']
        relative = f"objects/blobs/{row['sha256'][:2]}/{row['sha256']}.blob"
        blob = cold/relative
        blob.parent.mkdir(parents=True, exist_ok=True)
        blob.write_bytes(path.read_bytes())
        path.unlink()
        os.link(blob, path)
        assert blob.stat().st_nlink == 2
        selected.append(path)
        items.append({'relative': relative, 'kind': 'object', 'bytes': row['size'],
                      'signature': stream_repack._signature(blob), 'allocated_bytes': 0})
    plan = {'old_snapshot_id': prepared['old_snapshot_id'], 'new_snapshot_id': receipt['new_snapshot_id'],
            'old_refs': [item['relative'] for item in items], 'items': items}
    old = {'contract': repack_retirement.CONTRACT, 'dataset': dataset,
           'old_manifest_sha256': old_sha, 'new_manifest_sha256': receipt['new_manifest_sha256'],
           'prepared_sha256': prepared['identity_sha256'], 'plan': plan,
           'plan_sha256': stream_repack._identity(plan), 'removed': [item['relative'] for item in items],
           'state': 'completed'}
    for item in items:
        (cold/item['relative']).unlink()
    repack_retirement.save_journal(stage/'retirement.json', old)
    case['selected'] = selected
    return case


def perform(case, *, apply=True, **overrides):
    options = {**case['options'], **overrides}
    with options['coordinator_lock'].open('a+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return landing.retire(case['batch'], case['receipt'], coordinator_fd=owner.fileno(), apply=apply, **options)


def test_source_local_commit_and_prior_inode_retirement_release_exact_parts(committed):
    source = committed['batch']/'publish-markets-parts-v2'
    expected = set(committed['selected'])
    planned = perform(committed, apply=False)
    assert planned['state'] == 'planned' and len(planned['plan']['items']) == len(expected)
    assert all(path.exists() for path in expected)
    result = perform(committed)
    assert result['state'] == 'completed' and result['estimated_allocated_bytes_reclaimed'] > 0
    assert result['independent_D_readback'] is False and result['NAS_accepted'] is False
    assert not any(path.exists() for path in expected)
    assert perform(committed)['state'] == 'completed'
    assert landing.completed_reclamation(committed['options']['workspace'], committed['batch'], 'markets') == (
        len(expected), result['estimated_allocated_bytes_reclaimed'])
    assert packed.resolve_packed_snapshot_id(committed['options']['cold'], committed['new_receipt']['dataset'],
        committed['new_receipt']['new_snapshot_id']).manifest_sha256 == committed['new_receipt']['new_manifest_sha256']


def test_changed_file_or_active_reader_is_preserved(committed):
    source = committed['batch']/'publish-markets-parts-v2'
    candidate = committed['selected'][0]
    with pytest.raises(SnapshotError, match='running process'):
        perform(committed, process_check=lambda *_a, **_kw: ['pid=42'])
    assert candidate.exists()
    changed = candidate.read_bytes()+b'changed'
    candidate.unlink()
    candidate.write_bytes(changed)
    with pytest.raises(SnapshotError, match='changed before batch intent'):
        perform(committed)
    assert candidate.read_bytes() == changed


def test_extra_hardlink_and_missing_old_retirement_never_authorize_deletion(committed, tmp_path):
    source = committed['batch']/'publish-markets-parts-v2'
    candidate = committed['selected'][0]
    os.link(candidate, tmp_path/'unexpected-hardlink')
    plan = perform(committed, apply=False)
    assert candidate.name not in {Path(item['relative']).name for item in plan['plan']['items']}
    (committed['stage']/'retirement.json').unlink()
    assert perform(committed)['state'] == 'waiting_D_commit_or_old_retirement'
    assert candidate.exists() and (tmp_path/'unexpected-hardlink').exists()


def test_interrupted_unlink_retries_only_the_durable_pending_batch(committed, monkeypatch):
    original = Path.unlink
    failed = False
    def interrupted(path, *args, **kwargs):
        nonlocal failed
        if path.parent.name == 'publish-markets-parts-v2' and not failed:
            failed = True
            original(path, *args, **kwargs)
            raise OSError('interrupted after unlink')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        perform(committed)
    monkeypatch.setattr(Path, 'unlink', original)
    assert perform(committed)['state'] == 'completed'


def test_original_coordinator_lock_and_payload_readback_boundary(committed, monkeypatch):
    with pytest.raises(SnapshotError, match='owner or scope'):
        landing.retire(committed['batch'], committed['receipt'], coordinator_fd=-1, apply=True, **committed['options'])
    original = Path.open
    def deny(path, *args, **kwargs):
        if path.parent.name == 'publish-markets-parts-v2':
            pytest.fail('old payload must not be read again')
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'open', deny)
    assert perform(committed)['state'] == 'completed'


def test_distinct_unenrolled_mounts_cannot_use_inode_number_as_proof(tmp_path):
    assert not landing._same_volume(tmp_path/'cold', tmp_path/'incoming', 10, 20)
    assert landing._same_volume(tmp_path/'cold', tmp_path/'incoming', 10, 10)
