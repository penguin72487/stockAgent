import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import publish_handoff_source_repack as publish
from stockagent.data_sync import handoff_source_repack, packed_snapshots, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_handoff_source_repack import fixture


def setup_publication(tmp_path, monkeypatch):
    source, cold, _stage, index, old, kwargs = fixture(tmp_path, monkeypatch)
    migration = tmp_path/'workspace/migration'
    stage = migration/'handoff-source-repack-v1/fixture'/old.manifest_sha256
    prepared = handoff_source_repack.prepare(cold, source, stage, 'fixture', old.manifest_sha256, index, **kwargs)
    retired = handoff_source_repack.retire(source, stage, 'fixture', old.manifest_sha256, index,
                                            **kwargs, process_check=lambda _paths: [])
    status = stage.parent.parent/'fixture.status.json'
    status.write_text(json.dumps({'contract': publish.handoff_cli.CONTRACT, 'dataset': 'fixture',
        'state': retired['state'], 'C_payload_deleted': True, 'C_retirement': retired,
        'original_manifest_sha256': old.manifest_sha256, 'prepared_sha256': prepared['identity_sha256'],
        'D_new_head_committed': False}))
    config = {'mount_point': str(migration.parent)}
    monkeypatch.setattr(publish.handoff_cli, 'ENROLLED', {'fixture-source': 'fixture'})
    monkeypatch.setattr(publish.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(publish.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(publish.workspace_volume, 'path', lambda *_a, **_kw: migration)
    monkeypatch.setattr(publish, 'COLD', cold)
    monkeypatch.setattr(publish, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(publish, 'private_history_root', lambda *_a, **_kw: cold)
    monkeypatch.setattr(publish, 'version_policy', lambda *_a: {})
    monkeypatch.setattr(publish.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=publish.D_INSTALL_RESERVE+100_000_000))
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(packed_snapshots, '_check_native_pack_read_authority', lambda *_a: None)
    monkeypatch.setattr(packed_snapshots, '_notify_packed_publication', lambda *_a, **_kw: pytest.fail('private export'))
    return cold, stage, old, status


def test_private_D_publish_and_E_cleanup_preserve_original_release(tmp_path, monkeypatch):
    cold, stage, old, status = setup_publication(tmp_path, monkeypatch)
    value = publish.publish('fixture')
    assert value['D_new_head_committed'] and value['D_E_stage_cleanup_complete']
    assert value['D_publication']['private'] and not value['D_publication']['independent_D_readback']
    assert not value['D_publication']['NAS_accepted'] and not value['D_publication']['old_release_removed']
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == value['D_publication']['new_manifest_sha256']
    assert packed_snapshots.resolve_packed_snapshot_id(cold, 'fixture', old.manifest['snapshot_id']).manifest_sha256 == old.manifest_sha256
    assert not list(stage.glob('pack-*.zip'))
    assert json.loads(status.read_text())['D_state'] == 'completed'


def test_private_D_capacity_deferral_preserves_E_and_resumes(tmp_path, monkeypatch):
    cold, stage, old, status = setup_publication(tmp_path, monkeypatch)
    monkeypatch.setattr(publish.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=publish.D_INSTALL_RESERVE))
    with pytest.raises(SnapshotError, match='below the remaining'):
        publish.publish('fixture')
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(publish.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=publish.D_INSTALL_RESERVE+100_000_000))
    assert publish.publish('fixture')['D_E_stage_cleanup_complete']


def test_private_D_head_commit_then_cleanup_crash_resumes(tmp_path, monkeypatch):
    cold, stage, old, status = setup_publication(tmp_path, monkeypatch)
    original = stream_repack.cleanup_stage
    monkeypatch.setattr(stream_repack, 'cleanup_stage', lambda *_a, **_k: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        publish.publish('fixture')
    committed = json.loads(status.read_text())
    assert committed['D_new_head_committed'] and committed['D_state'] == 'head_committed'
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == committed['D_publication']['new_manifest_sha256']
    monkeypatch.setattr(stream_repack, 'cleanup_stage', original)
    assert publish.publish('fixture')['D_E_stage_cleanup_complete']
