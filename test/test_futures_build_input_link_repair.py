"""An alias repair preserves bytes/inodes, never fabricates its missing data."""
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path

import pytest

from scripts import repair_futures_build_input_links as repair
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    PackedPublicationBusy, fetch_packed_snapshot, initialize_packed_layout,
    publish_packed_snapshot, verify_packed_snapshot,
)

ORIGINAL_OWNERS = repair.original_owners


@pytest.fixture
def reviewed(tmp_path, monkeypatch):
    repo = tmp_path / 'repo'
    source = repo / 'data_tw_index_futures'
    parent = source / 'build_inputs'
    parent.mkdir(parents=True)
    (source / 'state').mkdir()
    (source / 'raw').mkdir()
    (source / 'raw/original.bin').write_bytes(b'original observation\x00-7')
    for version in repair.VERSIONS:
        (parent / f'universe725_recovery_20261025_v1-v{version}').symlink_to(
            f'/dev/shm/stockagent-universe725-source-build-v{version}')
    native_exists = os.path.lexists
    monkeypatch.setattr(repair.os.path, 'lexists', lambda path:
        False if str(path).startswith('/dev/shm/stockagent-universe725-source-build-v') else native_exists(path))
    monkeypatch.setattr(repair, '_current', lambda: {
        'operation_directory': str(repo / 'original-operation'), 'plan_sha256': 'a' * 64})
    monkeypatch.setattr(repair, '_references', lambda *_: [])
    monkeypatch.setattr(repair, 'original_owners', lambda plan: nullcontext())
    plan = repair.inventory(repo)
    raw = json.dumps(plan, sort_keys=True).encode()
    sha = hashlib.sha256(raw).hexdigest()
    return repo, source, parent, plan, raw, sha, repo / 'artifacts/operations/repair'


def test_preserved_aliases_and_provenance_have_exact_cold_readback(reviewed, tmp_path):
    repo, source, parent, plan, raw, sha, receipt = reviewed
    before = (source / 'raw/original.bin').read_bytes()
    assert all(os.path.lexists(repo / row['path']) for row in plan['aliases'])
    result = repair.apply(repo, raw, sha, receipt)
    assert result['state'] == 'preserved_and_relocated'
    assert result['deleted_payload_files'] == result['reclaimed_bytes'] == 0
    assert result['missing_target_payload_recovered'] is False
    for original, moved in zip(plan['aliases'], result['moved'], strict=True):
        assert not os.path.lexists(repo / original['path'])
        actual = Path(moved['quarantine'])
        assert actual.lstat().st_ino == original['signature']['ino']
        assert os.readlink(actual) == original['target']
    assert (source / 'raw/original.bin').read_bytes() == before
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='test')
    saved = publish_packed_snapshot(cold, 'index-originals', source, pack_buckets=1, node_id='test')
    proof = verify_packed_snapshot(cold, saved, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 2
    restored = fetch_packed_snapshot(cold, tmp_path / 'restore', saved)
    assert (restored / 'raw/original.bin').read_bytes() == before
    relative = Path(result['source_provenance']).relative_to(source)
    document = json.loads((restored / relative).read_bytes())
    assert document['aliases'] == plan['aliases']
    assert document['missing_target_payload'] == 'absent_not_reconstructed'
    assert not list((restored / 'build_inputs').iterdir())


@pytest.mark.parametrize('change', ['source_changed', 'target_present', 'consumer', 'occupied_destination',
                                   'unreviewed', 'bad_inventory_hash', 'incomplete_metadata', 'provenance_changed'])
def test_uncertain_repair_never_overwrites_or_retires_names(reviewed, monkeypatch, change):
    repo, source, parent, plan, raw, sha, receipt = reviewed
    first = repo / plan['aliases'][0]['path']
    if change == 'source_changed':
        first.unlink()
        first.write_bytes(b'new original; preserve')
    elif change == 'target_present':
        native_exists = repair.os.path.lexists
        monkeypatch.setattr(repair.os.path, 'lexists', lambda path:
            str(path) == plan['aliases'][0]['target'] or native_exists(path))
    elif change == 'consumer':
        monkeypatch.setattr(repair, '_references', lambda *_: ['current run still needs this name'])
    elif change == 'occupied_destination':
        target = repo / 'service_data/futures-source-link-repair' / sha / 'quarantine' / first.name
        target.parent.mkdir(parents=True)
        target.write_bytes(b'foreign original; preserve')
    elif change in ('unreviewed', 'incomplete_metadata'):
        if change == 'unreviewed':
            plan['aliases'][0]['path'] = 'data_tw_index_futures/raw/original.bin'
        else:
            del plan['aliases'][0]['signature']['ino']
        raw = json.dumps(plan).encode()
        sha = hashlib.sha256(raw).hexdigest()
    elif change == 'bad_inventory_hash':
        sha = '0' * 64
    elif change == 'provenance_changed':
        path = source / 'state/source_link_repairs' / (sha + '.json')
        path.parent.mkdir()
        path.write_bytes(b'foreign provenance; preserve')
    with pytest.raises(SnapshotError):
        repair.apply(repo, raw, sha, receipt)
    assert os.path.lexists(first)
    assert (source / 'raw/original.bin').read_bytes() == b'original observation\x00-7'
    if change == 'occupied_destination':
        assert target.read_bytes() == b'foreign original; preserve'


def test_interrupted_rename_resumes_exact_original_inodes(reviewed, monkeypatch):
    repo, _, _, plan, raw, sha, receipt = reviewed
    native = repair.rename_without_overwrite
    calls = []
    def fail_after_rename(source, target):
        native(source, target)
        calls.append(str(source))
        raise OSError('simulated interruption after the first durable name move')
    monkeypatch.setattr(repair, 'rename_without_overwrite', fail_after_rename)
    with pytest.raises(OSError):
        repair.apply(repo, raw, sha, receipt)
    assert len(calls) == 1
    monkeypatch.setattr(repair, 'rename_without_overwrite', native)
    result = repair.apply(repo, raw, sha, receipt)
    assert len(result['moved']) == len(plan['aliases'])
    assert repair.apply(repo, raw, sha, receipt)['state'] == 'preserved_and_relocated'


def test_original_busy_owner_prevents_source_mutation(reviewed, monkeypatch):
    repo, _, _, plan, raw, sha, receipt = reviewed
    def busy(_):
        raise PackedPublicationBusy('original publisher remains active')
    monkeypatch.setattr(repair, 'original_owners', busy)
    with pytest.raises(PackedPublicationBusy):
        repair.apply(repo, raw, sha, receipt)
    assert all(os.path.lexists(repo / row['path']) for row in plan['aliases'])


def test_redirected_control_parent_never_creates_foreign_repair_state(reviewed, tmp_path):
    repo, _, _, plan, raw, sha, receipt = reviewed
    foreign = tmp_path / 'foreign'
    foreign.mkdir()
    (repo / 'service_data').symlink_to(foreign, target_is_directory=True)
    with pytest.raises(SnapshotError, match='redirected'):
        repair.apply(repo, raw, sha, receipt)
    assert not list(foreign.iterdir())
    assert all(os.path.lexists(repo / row['path']) for row in plan['aliases'])


def test_original_operation_and_both_publish_owners_are_required(tmp_path, monkeypatch):
    operation = tmp_path / 'operation'
    operation.mkdir()
    cold = tmp_path / 'cold'
    private = cold / '.local-state/private-history'
    private.mkdir(parents=True)
    monkeypatch.setattr(repair, 'COLD', cold)
    monkeypatch.setattr(repair, '_check_d_primary_mount', lambda *_: None)
    current = {'operation_directory': str(operation), 'plan_sha256': 'a' * 64}
    monkeypatch.setattr(repair, '_current', lambda: current)
    plan = {'operation_directory': str(operation), 'migration_plan_sha256': 'a' * 64}
    progress = {'datasets': {name: {'state': 'waiting_retry'} for name in
                             ('tw-index-futures', 'local-tw-index-futures')}}
    path = operation / 'progress.json'
    path.write_text(json.dumps(progress))
    with repair._operation_owner(operation):
        with pytest.raises(PackedPublicationBusy):
            with ORIGINAL_OWNERS(plan):
                pytest.fail('another migration owner is active')
    with ORIGINAL_OWNERS(plan):
        pass
    for root, name in ((cold, 'tw-index-futures'), (private, 'local-tw-index-futures')):
        with repair._try_publication_lock(root / '.local-state/locks' / f'publish-{name}.lock'):
            with pytest.raises(PackedPublicationBusy):
                with ORIGINAL_OWNERS(plan):
                    pytest.fail('another publisher is active')
    progress['datasets']['tw-index-futures']['snapshot_id'] = 'committed-release'
    path.write_text(json.dumps(progress))
    with pytest.raises(PackedPublicationBusy, match='committed'):
        with ORIGINAL_OWNERS(plan):
            pytest.fail('committed original release must finish recovery')


@pytest.fixture
def expired_views(reviewed):
    repo, source, _, _, raw, sha, receipt = reviewed
    repair.apply(repo, raw, sha, receipt)
    for relative, (target, name, contract) in repair.EXPIRED_LEASES.items():
        link = repo / relative
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)
        path = repo / repair.LEASE_ROOT / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'logical': relative, 'physical': target, 'contract': contract,
            'volatile_reconstructible': True, 'volatile_reconstructible_candidate': True,
            'source_originals_untouched': True, 'source_originals_deleted': False,
            'boot_id': '00000000-0000-0000-0000-000000000000'}))
    target = repo / repair.PORTABLE_TARGET
    target.mkdir(parents=True)
    (target / 'manifest.json').write_text('{"original": "fixed checkpoint"}')
    (target / 'minutes.bin').write_bytes(b'original minute observations\x00-7')
    alias = repo / repair.PORTABLE_PATH
    alias.parent.mkdir(parents=True)
    alias.symlink_to(target, target_is_directory=True)
    plan = repair.inventory(repo, 'expired-leases')
    raw = json.dumps(plan, sort_keys=True).encode()
    sha = hashlib.sha256(raw).hexdigest()
    return repo, source, plan, raw, sha, repo / 'artifacts/operations/expired-repair'


def test_expired_views_preserve_original_leases_and_portable_alias_has_full_cold_readback(expired_views, tmp_path):
    repo, source, plan, raw, sha, receipt = expired_views
    result = repair.apply(repo, raw, sha, receipt)
    assert len(result['moved']) == 16 and result['deleted_payload_files'] == 0
    alias = repo / repair.PORTABLE_PATH
    assert alias.is_symlink() and not Path(os.readlink(alias)).is_absolute()
    assert alias.resolve() == repo / repair.PORTABLE_TARGET
    for row in plan['aliases']:
        if row['path'] != repair.PORTABLE_PATH:
            assert not os.path.lexists(repo / row['path'])
    provenance = json.loads(Path(result['source_provenance']).read_bytes())
    assert provenance['expired_view_payload_recovered'] is False
    assert provenance['historical_validation_outputs_verified'] is False
    assert sum('lease_evidence' in row for row in provenance['aliases']) == 15
    cold = tmp_path / 'cold'
    initialize_packed_layout(cold, node_id='test')
    saved = publish_packed_snapshot(cold, 'available-originals', source, pack_buckets=1, node_id='test')
    proof = verify_packed_snapshot(cold, saved, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] > 0
    restored = fetch_packed_snapshot(cold, tmp_path / 'restore', saved)
    portable = restored / Path(repair.PORTABLE_PATH).relative_to('data_tw_index_futures')
    assert portable.resolve().is_relative_to(restored)
    assert (portable / 'minutes.bin').read_bytes() == b'original minute observations\x00-7'
    assert json.loads((restored / Path(result['source_provenance']).relative_to(source)).read_bytes()) == provenance


@pytest.mark.parametrize('fault', ['lease_bytes', 'lease_target', 'lease_boot', 'lease_scope',
                                   'target_manifest', 'target_directory', 'unreviewed_replacement'])
def test_expired_views_never_ignore_changed_lease_or_original_target(expired_views, fault):
    repo, _, plan, raw, sha, receipt = expired_views
    first = plan['aliases'][0]
    path = repo / first['lease_evidence']['path']
    if fault.startswith('lease_'):
        data = json.loads(path.read_bytes())
        if fault == 'lease_bytes':
            data['new_observation'] = True
        elif fault == 'lease_target':
            data['physical'] = '/dev/shm/unreviewed-input'
        elif fault == 'lease_boot':
            data['boot_id'] = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        else:
            data['volatile_reconstructible'] = data['volatile_reconstructible_candidate'] = False
        path.write_text(json.dumps(data))
    elif fault == 'target_manifest':
        (repo / repair.PORTABLE_TARGET / 'manifest.json').write_text('{"changed":true}')
    elif fault == 'target_directory':
        (repo / repair.PORTABLE_TARGET / 'foreign').write_text('new checkpoint child')
    else:
        first['replacement_target'] = '../../raw'
        raw = json.dumps(plan).encode()
        sha = hashlib.sha256(raw).hexdigest()
    with pytest.raises(SnapshotError):
        repair.apply(repo, raw, sha, receipt)
    assert all(os.path.lexists(repo / row['path']) for row in plan['aliases'])


def test_portable_alias_interruption_after_rename_resumes_without_overwriting(expired_views, monkeypatch):
    repo, _, plan, raw, sha, receipt = expired_views
    native = repair.rename_without_overwrite
    def interrupt(source, target):
        native(source, target)
        if source == repo / repair.PORTABLE_PATH:
            raise OSError('interrupted before creating the portable alias')
    monkeypatch.setattr(repair, 'rename_without_overwrite', interrupt)
    with pytest.raises(OSError):
        repair.apply(repo, raw, sha, receipt)
    assert not os.path.lexists(repo / repair.PORTABLE_PATH)
    monkeypatch.setattr(repair, 'rename_without_overwrite', native)
    result = repair.apply(repo, raw, sha, receipt)
    assert len(result['moved']) == len(plan['aliases'])
    assert (repo / repair.PORTABLE_PATH).resolve() == repo / repair.PORTABLE_TARGET
    assert repair.apply(repo, raw, sha, receipt)['state'] == 'preserved_and_relocated'
