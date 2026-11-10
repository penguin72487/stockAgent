from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from scripts import restore_closed_cftc_e_workspace as restore
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    source = tmp_path / 'data_cftc_legacy'
    source.mkdir()
    for name in ('restore', 'migration', 'verify'):
        (tmp_path / name).mkdir()
    snapshot = SimpleNamespace(manifest_sha256='a' * 64, manifest={
        'snapshot_id': 'fixed-v1', 'archive': {'inventory': {'sha256': 'b' * 64}},
        'source': {'files': 10, 'logical_bytes': 1234}})
    status = {'raw_retirement_complete': True, 'snapshot_id': 'fixed-v1'}
    monkeypatch.setattr(restore, 'SOURCE', source)
    monkeypatch.setattr(restore, 'COLD', tmp_path / 'cold')
    monkeypatch.setattr(restore, 'MATERIALIZED', tmp_path / 'cache')
    monkeypatch.setattr(restore.queue, 'catalog_entries', lambda _: [{'dataset': 'test'}])
    monkeypatch.setattr(restore.closed_source, 'validate_catalog', lambda _: None)
    monkeypatch.setattr(restore.publisher, '_source_coordination_lock', lambda *a, **k: nullcontext())
    monkeypatch.setattr(restore.closed_source, 'resolve_closed_source', lambda *a, **k: (snapshot, status))
    monkeypatch.setattr(restore.volume, 'path', lambda name, **k: tmp_path / name)
    monkeypatch.setattr(restore.volume, 'configuration', lambda: {'minimum_free_bytes': 1024})
    monkeypatch.setattr(restore.volume, 'admit', lambda size: {'state': 'mounted', 'admitted_bytes': size})
    monkeypatch.setattr(restore, 'reserve_capacity',
                        lambda *a, **k: nullcontext({'other_reserved_growth_bytes': 0}))
    def fetch(cold, root, resolved, **kwargs):
        assert resolved is snapshot
        assert kwargs == {'d_primary_native_blob_reads': True, 'd_primary_native_pack_reads': True}
        result = root / 'test' / 'fixed-v1'
        result.mkdir(parents=True)
        return result
    monkeypatch.setattr(restore.packed, 'fetch_packed_snapshot', fetch)
    return tmp_path, snapshot, status


def test_restore_retains_source_and_records_only_fixed_E_materialization(workspace):
    root, snapshot, _ = workspace
    result = restore.restore()
    assert result['state'] == 'fixed_release_materialized_and_readback_verified'
    assert result['manifest_sha256'] == snapshot.manifest_sha256 and result['raw_originals_restored'] == 2
    assert result['source_files_deleted'] == 0 and not result['source_alias_changed']
    assert not result['D_snapshot_changed'] and not result['NAS_acceptance_claimed']
    assert (root / 'verify' / (restore.NAME + '.json')).is_file()


def test_restore_rejects_binding_change_and_retains_partial_evidence(workspace, monkeypatch):
    root, snapshot, status = workspace
    observations = iter([(snapshot, status), (snapshot, {**status, 'snapshot_id': 'changed'})])
    monkeypatch.setattr(restore.closed_source, 'resolve_closed_source', lambda *a, **k: next(observations))
    with pytest.raises(SnapshotError, match='binding changed'):
        restore.restore()
    assert not (root / 'verify' / (restore.NAME + '.json')).exists()
    assert (root / 'restore' / restore.NAME / 'test' / 'fixed-v1').is_dir()


def test_restore_rejects_unaccepted_retirement_before_materialization(workspace, monkeypatch):
    _, snapshot, _ = workspace
    monkeypatch.setattr(restore.closed_source, 'resolve_closed_source',
                        lambda *a, **k: (snapshot, {'raw_retirement_complete': False}))
    monkeypatch.setattr(restore.packed, 'fetch_packed_snapshot', lambda *a, **k: pytest.fail('no valid source'))
    with pytest.raises(SnapshotError, match='retirement binding'):
        restore.restore()
