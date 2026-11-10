import json

import pytest

from stockagent.data_sync import handoff_index_archive, index_archive_publish as publish
from stockagent.data_sync import packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from test_handoff_source_repack import fixture


def setup_archive(tmp_path, monkeypatch):
    source, _old_cold, stage, index, _old, kwargs = fixture(tmp_path, monkeypatch)
    prepared = handoff_index_archive.prepare(source, stage, 'fixture', 'a'*64, index,
        **kwargs, preserved_paths=['.download.lock'])
    private = tmp_path/'private-D'
    packed.initialize_packed_layout(private, node_id='test')
    return source, private, stage, index, prepared


def test_source_index_reuses_exact_generation_after_verified_workspace_remount(tmp_path, monkeypatch):
    _source, _private, _stage, index, prepared = setup_archive(tmp_path, monkeypatch)
    signature = publish.relocation.signature
    def renumbered(path):
        value = signature(path)
        return [value[0]+1, *value[1:]] if path == index else value
    monkeypatch.setattr(publish.relocation, 'signature', renumbered)
    with pytest.raises(SnapshotError, match='source index generation'):
        publish.source_summary(index, prepared, 'data_fixture')
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda _path, _dev: True)
    assert publish.source_summary(index, prepared, 'data_fixture')['files'] == prepared['files']
    index.write_bytes(index.read_bytes()+b'changed')
    with pytest.raises(SnapshotError, match='source index generation'):
        publish.source_summary(index, prepared, 'data_fixture')


def test_private_D_source_publication_keeps_public_head_and_can_use_canonical_reader(tmp_path, monkeypatch):
    source, private, stage, index, prepared = setup_archive(tmp_path, monkeypatch)
    _entries, summary = packed._collect_entries(source)
    indexed = publish.source_summary(index, prepared, 'data_fixture')
    assert indexed['portable_fingerprint_sha256'] == summary['portable_fingerprint_sha256']
    events = []
    result = publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture', progress=events.append)
    streamed = [event for event in events if event['stage'] == 'streaming_D_object']
    assert streamed and streamed[0]['encoded_bytes_streamed'] > streamed[0]['encoded_bytes_installed']
    current = packed.resolve_latest_packed(private, 'fixture')
    assert result['state'] == 'published' and not result['public_dataset_merged']
    assert current.manifest_sha256 == result['new_manifest_sha256']
    assert current.manifest['metadata']['source_mode'] == 'retained_handoff_C_index'
    restored = packed.fetch_packed_snapshot(private, tmp_path/'restored', current)
    assert (restored/'nested/a.parquet').read_bytes() == (source/'nested/a.parquet').read_bytes()
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source', 'dataset': 'fixture',
        'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': result['new_manifest_sha256'],
        'new_snapshot_id': result['snapshot_id']}
    cleanup = stream_repack.cleanup_stage(private, stage, receipt)
    assert cleanup['state'] == 'completed' and not list(stage.glob('pack-*.zip'))


def test_private_D_source_install_recovers_after_object_link(tmp_path, monkeypatch):
    _source, private, stage, index, _prepared = setup_archive(tmp_path, monkeypatch)
    original = packed._install_immutable_object
    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise OSError('after object link')
    monkeypatch.setattr(packed, '_install_immutable_object', interrupted)
    with pytest.raises(OSError, match='after object link'):
        publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture')
    monkeypatch.setattr(packed, '_install_immutable_object', original)
    result = publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture')
    assert packed.resolve_latest_packed(private, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_private_D_validates_the_inventory_in_one_stream_without_a_spill_index(tmp_path, monkeypatch):
    _source, private, stage, index, _prepared = setup_archive(tmp_path, monkeypatch)
    monkeypatch.setattr(packed, '_open_inventory', lambda *_a, **_kw: pytest.fail('unneeded metadata index'))
    events = []
    result = publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture', progress=events.append)
    inventory = [event for event in events if event['stage'] == 'validating_D_inventory']
    assert result['state'] == 'published' and inventory
    assert inventory[-1]['inventory_items_completed'] == inventory[-1]['inventory_items_total']


def test_private_D_source_uses_the_same_install_admission_lock(tmp_path, monkeypatch):
    _source, private, stage, index, _prepared = setup_archive(tmp_path, monkeypatch)
    lock = tmp_path/'workspace/cold-install-write-owner.lock'
    admissions = []
    with packed._try_publication_lock(lock):
        with pytest.raises(packed.PackedPublicationBusy, match='owner busy'):
            publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture',
                install_lock=lock, admit_install=lambda *_args: admissions.append(True))
    assert not admissions and (stage/'prepared.json').is_file()
    result = publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture',
        install_lock=lock, admit_install=lambda *_args: admissions.append(True))
    assert result['state'] == 'published' and admissions == [True]


def test_private_D_source_does_not_replace_another_head(tmp_path, monkeypatch):
    source, private, stage, index, _prepared = setup_archive(tmp_path, monkeypatch)
    existing = packed.publish_packed_snapshot(private, 'fixture', source, node_id='test')
    with pytest.raises(SnapshotError, match='different current owner'):
        publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture')
    assert packed.resolve_latest_packed(private, 'fixture').manifest_sha256 == existing.manifest_sha256
    assert (stage/'prepared.json').exists()


def test_private_D_source_resumes_after_head_commit_before_receipt(tmp_path, monkeypatch):
    _source, private, stage, index, _prepared = setup_archive(tmp_path, monkeypatch)
    original = publish.atomic_write_json
    interrupted = False
    def after_write(path, value, **kwargs):
        nonlocal interrupted
        result = original(path, value, **kwargs)
        if path.parent == private/'heads/fixture' and not interrupted:
            interrupted = True
            raise OSError('after head commit')
        return result
    monkeypatch.setattr(publish, 'atomic_write_json', after_write)
    with pytest.raises(OSError, match='after head commit'):
        publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture')
    monkeypatch.setattr(publish, 'atomic_write_json', original)
    result = publish.publish(private, stage, 'fixture', 'a'*64, index, 'data_fixture')
    assert packed.resolve_latest_packed(private, 'fixture').manifest_sha256 == result['new_manifest_sha256']
