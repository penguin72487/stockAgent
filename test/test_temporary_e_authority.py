import json
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, stream_repack, temporary_e_authority as temporary
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import run_stream_packed_repack as stream_cli
from scripts import run_prepared_stream_install_queue as queue


def sealed(tmp_path, monkeypatch):
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'stage'
    (source/'one').mkdir(parents=True)
    (source/'one/a.txt').write_bytes(b'first source bytes')
    (source/'b.parquet').write_bytes(b'raw parquet fixture')
    (source/'one/link').symlink_to('../b.parquet')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=2)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_args, **_kwargs: None)
    def wait_for_capacity(*_args):
        raise SnapshotError('simulated D capacity wait')
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_install=wait_for_capacity)
    return source, cold, stage, old


def test_sealed_stage_is_a_real_local_packed_root_without_payload_copy(tmp_path, monkeypatch):
    source, cold, stage, old = sealed(tmp_path, monkeypatch)
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    before = {item['name']: (stage/item['name']).stat().st_ino
              for item in [*prepared['packages'], prepared['inventory']]}
    marker = temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    assert marker['state'] == 'ready' and marker['local_only'] and not marker['D_retirement_authorized']
    resolved = packed.resolve_latest_packed(stage, 'fixture')
    assert resolved.manifest_sha256 == marker['manifest_sha256']
    assert resolved.manifest['metadata']['physical_repack_original_manifest_sha256'] == old.manifest_sha256
    assert {item['name']: (stage/item['name']).stat().st_ino
            for item in [*prepared['packages'], prepared['inventory']]} == before
    assert all((stage/name).stat().st_nlink == 1 for name in before)
    assert temporary.expose(cold, stage, 'fixture', old.manifest_sha256) == marker
    restored = packed.fetch_packed_snapshot(stage, tmp_path/'restore', resolved)
    assert (restored/'one/a.txt').read_bytes() == (source/'one/a.txt').read_bytes()
    assert (restored/'b.parquet').read_bytes() == (source/'b.parquet').read_bytes()
    assert (restored/'one/link').readlink().as_posix() == '../b.parquet'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_interrupted_metadata_publication_resumes_same_fixed_generation(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    original = packed.write_immutable_json
    def interrupted(path, value):
        original(path, value)
        raise OSError('interrupted after manifest')
    monkeypatch.setattr(packed, 'write_immutable_json', interrupted)
    with pytest.raises(OSError, match='interrupted after manifest'):
        temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    assert json.loads((stage/temporary.MARKER).read_text())['state'] == 'preparing'
    monkeypatch.setattr(packed, 'write_immutable_json', original)
    assert temporary.expose(cold, stage, 'fixture', old.manifest_sha256)['state'] == 'ready'


def test_modified_sealed_member_or_changed_head_is_not_exposed(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    pack = stage/prepared['packages'][0]['name']
    pack.write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    assert not (stage/temporary.MARKER).exists()


def test_stage_cleanup_waits_for_temporary_authority_retirement(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    with pytest.raises(SnapshotError, match='authority must be retired'):
        stream_repack.cleanup_stage(cold, stage, receipt)
    assert packed.resolve_latest_packed(stage, 'fixture').manifest_sha256


def test_active_temporary_reader_blocks_retirement_until_D_commit_and_release(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    with temporary.borrow(stage, 'fixture', old.manifest_sha256) as resolved:
        assert resolved.manifest_sha256
        with pytest.raises(packed.PackedPublicationBusy):
            temporary.retire(cold, stage, receipt)
    assert stream_cli.cleanup_completed_stage(cold, stage, receipt)['state'] == 'completed'
    assert receipt['E_temporary_authority_retirement']['state'] == 'retired'
    assert not (stage/temporary.MARKER).exists()
    with pytest.raises(SnapshotError, match='marker is missing'):
        with temporary.borrow(stage, 'fixture', old.manifest_sha256):
            pass


def test_retirement_resumes_after_head_unlink_without_touching_payload(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    original = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        if path == stage/'heads/fixture' and not failed:
            failed = True
            raise OSError('interrupted after temporary head unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='interrupted after temporary head unlink'):
        temporary.retire(cold, stage, receipt)
    assert json.loads((stage/temporary.MARKER).read_text())['pending'] == 'head'
    monkeypatch.setattr(packed, '_fsync_directory', original)
    assert temporary.retire(cold, stage, receipt)['state'] == 'retired'
    assert stream_repack.cleanup_stage(cold, stage, receipt)['state'] == 'completed'


def test_retirement_does_not_discard_E_when_D_generation_changes(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    resolved = packed.resolve_latest_packed(cold, 'fixture')
    obj = cold/resolved.manifest['archive']['objects'][0]['relpath']
    obj.write_bytes(b'changed')
    with pytest.raises(SnapshotError, match='generation changed'):
        temporary.retire(cold, stage, receipt)
    assert (stage/temporary.MARKER).exists()


def test_incomplete_E_metadata_can_be_retired_after_D_commit(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    original = packed.write_immutable_json
    def interrupted(path, value):
        original(path, value)
        raise OSError('interrupted after manifest')
    monkeypatch.setattr(packed, 'write_immutable_json', interrupted)
    with pytest.raises(OSError, match='interrupted after manifest'):
        temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    monkeypatch.setattr(packed, 'write_immutable_json', original)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    assert stream_cli.cleanup_completed_stage(cold, stage, receipt)['state'] == 'completed'
    assert receipt['E_temporary_authority_retirement']['state'] == 'retired'


def test_cleanup_retry_retains_the_prior_authority_retirement_receipt(tmp_path, monkeypatch):
    _source, cold, stage, old = sealed(tmp_path, monkeypatch)
    temporary.expose(cold, stage, 'fixture', old.manifest_sha256)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
               'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
               'old_manifest_sha256': old.manifest_sha256, 'new_manifest_sha256': result['new_manifest_sha256'],
               'new_snapshot_id': result['snapshot_id']}
    cleanup = stream_repack.cleanup_stage
    monkeypatch.setattr(stream_repack, 'cleanup_stage',
                        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError('cleanup interrupted')))
    with pytest.raises(OSError, match='cleanup interrupted'):
        stream_cli.cleanup_completed_stage(cold, stage, receipt)
    assert not (stage/temporary.MARKER).exists()
    monkeypatch.setattr(stream_repack, 'cleanup_stage', cleanup)
    resumed = {key: value for key, value in receipt.items() if key != 'E_temporary_authority_retirement'}
    assert stream_cli.cleanup_completed_stage(cold, stage, resumed)['state'] == 'completed'
    assert resumed['E_temporary_authority_retirement']['state'] == 'retired'


def test_prepared_scheduler_creates_and_reuses_real_E_view_when_D_is_full(tmp_path, monkeypatch):
    source, cold = tmp_path/'source', tmp_path/'cold'
    workspace = tmp_path/'workspace'
    source.mkdir()
    (source/'sample.txt').write_bytes(b'full-source fixture')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_args, **_kwargs: None)
    stage = workspace/'migration/stream-source-repack-v1/fixture'/old.manifest_sha256
    with pytest.raises(SnapshotError, match='capacity wait'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_args: (_ for _ in ()).throw(SnapshotError('capacity wait')))
    root = workspace/'migration/source-local-exact-repack'
    root.mkdir(parents=True)
    (root/'queue-status.json').write_text(json.dumps({
        'contract': queue.q.CONTRACT+'.status', 'entries': [{'dataset': 'fixture',
        'expected_manifest_sha256': old.manifest_sha256, 'origin': 'cold', 'state': 'retrying',
        'E_prepared': True}]}))
    def no_writer(*_args, **_kwargs):
        pytest.fail('capacity wait must not start a D writer')
    first, = queue.run_once(workspace, cold, free_bytes=0, runner=no_writer, now=lambda: 1000, code_sha='test')
    second, = queue.run_once(workspace, cold, free_bytes=0, runner=no_writer, now=lambda: 1060, code_sha='test')
    assert first['state'] == second['state'] == 'waiting_capacity'
    assert first['E_temporary_authority_ready'] and second['E_temporary_authority_ready']
    with temporary.borrow(stage, 'fixture', old.manifest_sha256) as resolved:
        assert resolved.manifest['source']['files'] == 1
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
