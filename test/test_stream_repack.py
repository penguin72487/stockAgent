import json
import os
import zipfile
import io
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_metadata, packed_retention, packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts import run_stream_packed_repack as stream_cli


def _fixture(tmp_path, monkeypatch, *, spill=False):
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'E-stage'
    (source/'alpha').mkdir(parents=True)
    (source/'beta').mkdir()
    (source/'alpha/a.txt').write_bytes(b'first value')
    (source/'alpha/b.bin').write_bytes(b'0123456789'*10)
    (source/'beta/c.parquet').write_bytes(b'precompressed data')
    (source/'beta/link').symlink_to('../alpha/a.txt')
    if spill:
        monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', 2)
        monkeypatch.setattr(packed_metadata, '_metadata_scratch_location', lambda: (tmp_path, 0, False))
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=12, file_chunk_bytes=16, pack_buckets=8)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    return source, cold, stage, old


def test_foreign_head_keeps_d_unchanged_but_seals_the_e_source(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    assert stream_cli.foreign_source_head(cold, 'fixture', old.manifest_sha256, 'penguin')
    assert not stream_cli.foreign_source_head(cold, 'fixture', old.manifest_sha256, 'test')
    def admit(prepared, journal):
        if stream_cli.defer_foreign_head_install(cold, stage, 'fixture', old.manifest_sha256,
                                                'penguin', prepared, journal):
            raise SnapshotError('foreign head waits')
    with pytest.raises(SnapshotError, match='foreign head waits'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                             node_id='penguin', admit_install=admit)
    assert (stage/'prepared.json').is_file()
    assert not (cold/'heads/fixture/penguin.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_foreign_head_gate_never_interrupts_a_started_or_e_only_recovery(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    assert not stream_cli.defer_foreign_head_install(cold, stage, 'fixture', old.manifest_sha256,
                                                     'penguin', {}, {'state': 'installing'})
    monkeypatch.setattr(stream_cli.staged_head_capacity_relief, 'load',
                        lambda *_args: {'state': 'completed', 'removed_count': 1})
    assert not stream_cli.defer_foreign_head_install(cold, stage, 'fixture', old.manifest_sha256,
                                                     'penguin', {}, {})


def test_enrolled_vast_successor_is_published_before_separate_handoff(tmp_path, monkeypatch):
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'stage'
    source.mkdir()
    (source/'a').write_bytes(b'fixed source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    monkeypatch.setattr(stream_cli, '_handoff_timer_active', lambda _bucket: True)
    def admit(prepared, journal):
        assert not stream_cli.defer_foreign_head_install(cold, stage, 'fixture', old.manifest_sha256,
                                                         'penguin', prepared, journal)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='penguin', admit_install=admit)
    assert result['state'] == 'published'
    assert old.head_path.exists() and old.manifest_path.exists()
    latest = packed.resolve_latest_packed(cold, 'fixture')
    assert latest.manifest_sha256 == result['new_manifest_sha256']
    assert latest.manifest['source'] == old.manifest['source']


def test_foreign_successor_requires_enabled_enrolled_peer_and_source_hashes(tmp_path, monkeypatch):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'a').write_bytes(b'data')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='vastai1T')
    ready = {'source_member_sha_streamed': True}
    monkeypatch.setattr(stream_cli, '_handoff_timer_active', lambda _bucket: False)
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    monkeypatch.setattr(stream_cli, '_handoff_timer_active', lambda _bucket: True)
    assert stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', {})
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin',
        {**ready, 'packed_format': 'content-alias-v3'})
    (cold/'heads/fixture/other.json').write_text(json.dumps({'dataset': 'fixture', 'node_id': 'other',
                                                            'manifest_sha256': 'b'*64}))
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)


def test_retired_lab203_successor_requires_fixed_receipt_and_current_topology(tmp_path, monkeypatch):
    from scripts import retire_lab203_syncthing as retired

    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'a').write_bytes(b'fixed source')
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='lab203')
    ready = {'source_member_sha_streamed': True, 'identity_sha256': 'b'*64,
             'old_snapshot_id': old.manifest['snapshot_id']}
    receipt = tmp_path/'receipt.json'
    receipt.write_text(json.dumps({'action': 'retired', 'device_id': retired.LAB203_DEVICE_ID,
        'cold_dataset': 'fixture', 'cold_manifest_sha256': old.manifest_sha256,
        'cold_snapshot_id': old.manifest['snapshot_id']}))
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_DATASET', 'fixture')
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_MANIFEST', old.manifest_sha256)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_PREPARED', 'b'*64)
    monkeypatch.setattr(stream_cli, '_RETIRED_LAB203_RECEIPT', receipt)
    monkeypatch.setattr(retired, 'LAB203_SNAPSHOT_ID', old.manifest['snapshot_id'])
    monkeypatch.setattr(retired, '_api', lambda: ('local', {}))
    monkeypatch.setattr(stream_cli, '_handoff_timer_active', lambda _bucket: False)
    config = {'devices': [{'name': 'penguin', 'deviceID': 'local'},
                          {'name': 'lab203', 'deviceID': retired.LAB203_DEVICE_ID}],
              'folders': [{'id': retired.PACKED_FOLDER_ID, 'paused': False, 'type': 'sendreceive',
                           'devices': [{'deviceID': 'local'}, {'deviceID': 'vast'}]}]}
    monkeypatch.setattr(retired, '_get', lambda *_args, **_kwargs: config)
    assert stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin',
                                                          {**ready, 'identity_sha256': 'c'*64})
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin',
                                                          {**ready, 'source_member_sha_streamed': False})
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin',
                                                          {**ready, 'packed_format': 'content-alias-v3'})
    config['folders'][0]['devices'].append({'deviceID': retired.LAB203_DEVICE_ID})
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    config['folders'][0]['devices'].pop()
    config['folders'][0]['devices'].append({'deviceID': 'new-lab'})
    config['devices'].append({'name': 'lab203', 'deviceID': 'new-lab'})
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    config['folders'][0]['devices'].pop()
    config['folders'][0]['paused'] = True
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    config['folders'][0]['paused'] = False
    monkeypatch.setattr(retired, '_get', lambda *_args, **_kwargs: [])
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    def unavailable(*_args, **_kwargs):
        raise OSError('local Syncthing API unavailable')
    monkeypatch.setattr(retired, '_get', unavailable)
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)
    monkeypatch.setattr(retired, '_get', lambda *_args, **_kwargs: config)
    receipt.write_text('{}')
    assert not stream_cli.foreign_handoff_successor_ready(cold, 'fixture', old.manifest_sha256, 'penguin', ready)


def test_head_commit_guard_rechecks_after_install_and_can_resume(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    calls = []
    def changing_topology(_prepared):
        calls.append(True)
        if len(calls) == 2:
            raise SnapshotError('topology changed')
    with pytest.raises(SnapshotError, match='topology changed'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin',
                             before_head_commit=changing_topology)
    assert len(calls) == 2
    assert old.head_path.exists() and old.manifest_path.exists()
    assert not (cold/'heads/fixture/penguin.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert json.loads((stage/'install.json').read_text())['state'] == 'head_pending'
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='penguin')
    assert result['state'] == 'published'
    assert old.head_path.exists() and old.manifest_path.exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_foreign_head_gate_rejects_malformed_head_instead_of_guessing(tmp_path):
    root = tmp_path/'heads/fixture'; root.mkdir(parents=True)
    (root/'other.json').write_text('{"dataset":"fixture","node_id":"wrong","manifest_sha256":"'+'a'*64+'"}')
    with pytest.raises(SnapshotError, match='filename differs'):
        stream_cli.foreign_source_head(tmp_path, 'fixture', 'a'*64, 'penguin')


def test_recorded_generation_can_survive_only_a_verified_device_renumber(tmp_path, monkeypatch):
    path = tmp_path/'pack'
    path.write_bytes(b'unchanged')
    saved = stream_repack._signature(path)
    saved[0] += 1
    assert not stream_repack._signature_matches(path, saved)
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda _path, _dev: True)
    assert stream_repack._signature_matches(path, saved)
    saved[1] += 1
    assert not stream_repack._signature_matches(path, saved)
    assert not stream_repack._signature_matches(path, ['invalid'])


def test_d_landing_device_renumber_requires_the_enrolled_volume_guard(monkeypatch):
    from types import SimpleNamespace
    root = Path('/srv/stockagent-d-volume/stockagent-cold-primary')
    original_stat = Path.stat
    def stat(path, *args, **kwargs):
        return SimpleNamespace(st_dev=77, st_ino=88) if path == root else original_stat(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'stat', stat)
    seen = []
    monkeypatch.setattr(stream_repack, '_verified_remount',
                        lambda base, device, inode, kind: seen.append((base, device, inode, kind)) or True)
    assert stream_repack._trusted_device_renumbering(root/'remote-artifact-incoming/batch/part', 77)
    assert seen == [(root, 77, 88, 'cold_volume')]
    assert not stream_repack._trusted_device_renumbering(root/'remote-artifact-incoming/batch/part', 78)
    assert not stream_repack._trusted_device_renumbering(Path('/srv/unknown-volume/part'), 77)


def test_sealed_stage_resumes_after_verified_workspace_device_renumber(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='D needs space'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
            admit_install=lambda *_args: (_ for _ in ()).throw(SnapshotError('D needs space')))
    original = stream_repack._signature
    monkeypatch.setattr(stream_repack, '_signature', lambda path: [original(path)[0]+1, *original(path)[1:]])
    monkeypatch.setattr(stream_repack, '_trusted_device_renumbering', lambda _path, _dev: True)
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_kw: pytest.fail('sealed source reread'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published'


@pytest.mark.parametrize('spill', [False, True])
def test_stream_repack_preserves_logical_release_and_old_reference(tmp_path, monkeypatch, spill):
    source, cold, stage, old = _fixture(tmp_path, monkeypatch, spill=spill)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published' and result['files'] == 3 and result['objects'] == 1
    latest = packed.resolve_latest_packed(cold, 'fixture')
    assert latest.manifest_sha256 == result['new_manifest_sha256']
    assert latest.manifest['archive']['new_pack_layout'] == 'source-dataset-v1'
    old_rows = list(packed._iter_inventory(cold, old.manifest))
    new_rows = list(packed._iter_inventory(cold, latest.manifest))
    names = ('path', 'kind', 'mode', 'size', 'sha256', 'target')
    assert [{k: r.get(k) for k in names} for r in old_rows] == [{k: r.get(k) for k in names} for r in new_rows]
    restored = packed.fetch_packed_snapshot(cold, tmp_path/'restore', latest)
    assert (restored/'alpha/a.txt').read_bytes() == (source/'alpha/a.txt').read_bytes()
    assert (restored/'alpha/b.bin').read_bytes() == (source/'alpha/b.bin').read_bytes()
    assert os.readlink(restored/'beta/link') == '../alpha/a.txt'
    package = cold/latest.manifest['archive']['objects'][0]['relpath']
    with zipfile.ZipFile(package) as archive:
        assert set(archive.namelist()) == {'alpha/a.txt', 'alpha/b.bin', 'beta/c.parquet'}
    assert packed.resolve_packed_snapshot_id(cold, 'fixture', old.manifest['snapshot_id']).manifest_sha256 == old.manifest_sha256


def test_stream_repack_detects_corrupt_old_input_without_advancing_head(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    item = old.manifest['archive']['objects'][0]
    path = cold/item['relpath']
    path.write_bytes(b'x'*path.stat().st_size)
    with pytest.raises((SnapshotError, zipfile.BadZipFile)):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert packed.resolve_latest_packed(cold, 'fixture', require_objects=False).manifest_sha256 == old.manifest_sha256


def test_stream_repack_reports_D_install_and_head_commit(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    events = []
    stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test', progress=events.append)
    installs = [event for event in events if event.get('stage') == 'installing_D_objects']
    assert [event['objects_completed'] for event in installs] == [0, 1, 2]
    assert installs[-1]['encoded_bytes_installed'] == installs[-1]['encoded_bytes_total']
    streamed = [event for event in events if event.get('stage') == 'streaming_D_object']
    assert streamed and all(event['encoded_bytes_streamed'] >= event['encoded_bytes_installed'] for event in streamed)
    inventory = [event for event in events if event.get('stage') == 'validating_D_inventory']
    assert inventory and inventory[-1]['inventory_items_completed'] == inventory[-1]['inventory_items_total']
    assert events[-1]['stage'] == 'committing_D_head'


def test_checkpoint_inventory_merge_preserves_source_and_rejects_changed_members():
    source = [{'path': 'a', 'kind': 'directory', 'mode': 0o755},
              {'path': 'a/one', 'kind': 'file', 'mode': 0o644, 'size': 3, 'sha256': 'a'*64},
              {'path': 'b', 'kind': 'symlink', 'mode': 0o777, 'target': 'a/one'},
              {'path': 'z', 'kind': 'file', 'mode': 0o600, 'size': 5, 'sha256': 'b'*64}]
    class Checkpoint:
        _key = staticmethod(stream_repack._Checkpoint._key)
        def __init__(self, rows):
            self.rows = rows
        def ordered_members(self):
            yield from self.rows
    valid = [(b'a/one', 'c'*64, 3), (b'z', 'd'*64, 5)]
    merged = [entry.inventory_row() for entry in stream_repack._checkpoint_mapped_inventory(source, Checkpoint(valid))]
    assert [{k: v for k, v in row.items() if k != 'storage'} for row in merged] == source
    assert merged[1]['storage']['object_sha256'] == 'c'*64
    assert merged[3]['storage']['member'] == 'z'
    for broken in (valid[:1], [(b'a/one', 'c'*64, 4), valid[1]], valid+[(b'zz', 'e'*64, 1)],
                   [(b'a/one', 'not-a-sha', 3), valid[1]]):
        with pytest.raises(SnapshotError):
            list(stream_repack._checkpoint_mapped_inventory(source, Checkpoint(broken)))


def test_pack_builder_requires_a_durable_output_mapping(tmp_path):
    with pytest.raises(ValueError, match='output index or durable checkpoint'):
        stream_repack._PackBuilder(tmp_path, None)


def test_repack_reuses_original_index_without_second_metadata_copy(tmp_path, monkeypatch):
    source, cold, stage, old = _fixture(tmp_path, monkeypatch, spill=True)
    monkeypatch.setattr(stream_repack, 'MetadataRows', lambda *_a, **_k:
                        pytest.fail('the output must not create another metadata index'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    current = packed.resolve_latest_packed(cold, 'fixture')
    assert result['state'] == 'published'
    before = [{k: v for k, v in row.items() if k != 'storage'}
              for row in packed._iter_inventory(cold, old.manifest)]
    after = [{k: v for k, v in row.items() if k != 'storage'}
             for row in packed._iter_inventory(cold, current.manifest)]
    assert after == before
    restored = packed.fetch_packed_snapshot(cold, tmp_path/'restored', current)
    assert (restored/'alpha/a.txt').read_bytes() == (source/'alpha/a.txt').read_bytes()
    assert (restored/'beta/link').is_symlink()


def test_new_source_build_reports_inventory_phases_without_empty_checkpoint_lookups(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    events = []
    monkeypatch.setattr(stream_repack._Checkpoint, 'lookup',
                        lambda *_a: pytest.fail('an empty checkpoint needs no per-member query'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test',
                                  progress=events.append)
    assert result['state'] == 'published'
    stages = {event['stage'] for event in events}
    assert {'loading_source_inventory', 'validating_source_inventory', 'indexing_source_inventory',
            'writing_E_inventory'} <= stages
    assert all(event['inventory_items_completed'] <= event['inventory_items_total'] for event in events
               if event['stage'] in {'loading_source_inventory', 'validating_source_inventory',
                                     'indexing_source_inventory', 'writing_E_inventory'})


def test_repack_streams_new_inventory_without_building_a_second_metadata_index(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    original = packed._open_inventory
    opened = []
    def observed(root, manifest, **kwargs):
        opened.append(manifest['archive']['inventory']['sha256'])
        return original(root, manifest, **kwargs)
    monkeypatch.setattr(packed, '_open_inventory', observed)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published'
    assert opened == [old.manifest['archive']['inventory']['sha256']]


def test_corrupt_new_inventory_never_advances_the_D_head(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    original = packed._install_immutable_object
    def corrupt_inventory(*args, **kwargs):
        result = original(*args, **kwargs)
        target = args[2]
        if target.name.endswith('.jsonl.gz'):
            target.write_bytes(b'x'*target.stat().st_size)
        return result
    monkeypatch.setattr(packed, '_install_immutable_object', corrupt_inventory)
    with pytest.raises(SnapshotError, match='inventory checksum mismatch'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert packed.resolve_latest_packed(cold, 'fixture', require_objects=False).manifest_sha256 == old.manifest_sha256


def test_shared_D_install_lock_allows_E_build_but_defers_D_admission(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    lock = tmp_path/'workspace/cold-install-write-owner.lock'
    admissions = []
    with packed._try_publication_lock(lock):
        with pytest.raises(packed.PackedPublicationBusy, match='owner busy'):
            stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test',
                install_lock=lock, admit_install=lambda *_args: admissions.append(True))
    assert not admissions and (stage/'prepared.json').is_file()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test',
        install_lock=lock, admit_install=lambda *_args: admissions.append(True))
    assert result['state'] == 'published' and admissions == [True]


def test_stream_repack_allows_other_retirement_during_source_stream_with_pin(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    build = stream_repack._build
    observed = []
    def check_boundary(*args, **kwargs):
        with packed._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock'):
            protected, evidence, errors = packed_retention._local_protected_snapshot_ids(
                materialized, {old.manifest['snapshot_id']})
            assert protected == {old.manifest['snapshot_id']} and evidence and not errors
            observed.append(True)
        return build(*args, **kwargs)
    monkeypatch.setattr(stream_repack, '_build', check_boundary)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='test', materialized_pin_root=materialized)
    assert observed == [True] and result['state'] == 'published'
    assert not list(materialized.rglob('*.pin.json'))


def test_stream_repack_interruption_keeps_pin_until_successful_retry(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    build = stream_repack._build
    monkeypatch.setattr(stream_repack, '_build', lambda *_a, **_kw: (_ for _ in ()).throw(OSError('interrupted')))
    with pytest.raises(OSError, match='interrupted'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, materialized_pin_root=materialized)
    assert len(list(materialized.rglob('*.pin.json'))) == 1
    monkeypatch.setattr(stream_repack, '_build', build)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='test', materialized_pin_root=materialized)
    assert result['state'] == 'published' and not list(materialized.rglob('*.pin.json'))


def test_stream_repack_rejects_pin_removed_during_unlocked_build(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    build = stream_repack._build
    def remove_pin(*args, **kwargs):
        result = build(*args, **kwargs)
        next(materialized.rglob('*.pin.json')).unlink()
        return result
    monkeypatch.setattr(stream_repack, '_build', remove_pin)
    with pytest.raises(SnapshotError, match='pin changed'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, materialized_pin_root=materialized)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_stream_repack_pin_survives_notification_failure_and_is_released_on_resume(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    materialized = tmp_path/'materialized'
    materialized.mkdir()
    notify = stream_repack._notify_committed_repack
    monkeypatch.setattr(stream_repack, '_notify_committed_repack',
                        lambda *_a, **_kw: (_ for _ in ()).throw(OSError('notification interrupted')))
    with pytest.raises(OSError, match='notification interrupted'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                             node_id='test', materialized_pin_root=materialized)
    assert len(list(materialized.rglob('*.pin.json'))) == 1
    monkeypatch.setattr(stream_repack, '_notify_committed_repack', notify)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  node_id='test', materialized_pin_root=materialized)
    assert result['recovered_after_head_commit'] is True and not list(materialized.rglob('*.pin.json'))


def test_private_stream_repack_keeps_public_transport_uninvolved(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    seen = []
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda path: seen.append(path))
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: pytest.fail('private export'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='private-repack')
    assert result['state'] == 'published' and seen == [cold]
    assert packed.resolve_packed_snapshot_id(cold, 'fixture', old.manifest['snapshot_id']).manifest_sha256 == old.manifest_sha256


def test_private_repack_preserves_exact_single_publisher(tmp_path, monkeypatch):
    _source, cold, _stage, old = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    assert stream_cli.private_publisher(cold, 'fixture', old) == 'test'
    head = cold/'heads/fixture/test.json'
    extra = head.with_name('other.json')
    extra.write_bytes(head.read_bytes())
    with pytest.raises(SnapshotError, match='one fixed publisher'):
        stream_cli.private_publisher(cold, 'fixture', old)
    extra.unlink()
    value = json.loads(head.read_text())
    value['manifest_sha256'] = 'a'*64
    head.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match='does not match'):
        stream_cli.private_publisher(cold, 'fixture', old)


def test_private_cli_seals_then_resumes_without_public_export(tmp_path, monkeypatch):
    _source, cold, _stage, old = _fixture(tmp_path, monkeypatch)
    workspace = tmp_path/'workspace'
    migration = workspace/'migration'
    migration.mkdir(parents=True)
    config = {'mount_point': str(workspace), 'minimum_free_bytes': 0}
    monkeypatch.setattr(stream_cli, 'COLD', cold)
    monkeypatch.setattr(stream_cli, '_check_d_primary_mount', lambda *_a: None)
    monkeypatch.setattr(stream_cli, 'version_policy', lambda *_a: {})
    monkeypatch.setattr(stream_cli, 'private_history_root', lambda *_a, **_kw: cold)
    monkeypatch.setattr(stream_cli.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(stream_cli.workspace_volume, 'check', lambda **_kw: None)
    monkeypatch.setattr(stream_cli.workspace_volume, 'admit', lambda *_a, **_kw: None)
    monkeypatch.setattr(stream_cli.workspace_volume, 'path', lambda *_a, **_kw: migration)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_ROOT', cold)
    monkeypatch.setattr(stream_repack, 'PRIVATE_REPACK_DATASETS', frozenset({'fixture'}))
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda *_a: None)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: pytest.fail('private export'))
    @contextmanager
    def reserve(*_args, **_kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(stream_cli, 'reserve_capacity', reserve)
    original = stream_repack._stream_inputs
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda c, manifest, entries, consume, *, native, completed:
        original(c, manifest, entries, consume, native=False, completed=completed))
    monkeypatch.setattr(stream_cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=stream_cli.D_INSTALL_RESERVE))
    monkeypatch.setattr(stream_cli.sys, 'argv', ['repack', '--dataset', 'fixture', '--private-source',
        '--expected-manifest-sha256', old.manifest_sha256])
    with pytest.raises(SnapshotError, match='D needs space'):
        stream_cli.main()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_kw: pytest.fail('sealed source reread'))
    monkeypatch.setattr(stream_cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=10**12))
    # A retirement turn releases the publication lock without discarding the
    # sealed E stage; this exact installer resumes without rereading D.
    with stream_cli.retirement_turn.turn(migration/'source-local-exact-repack', retirement=True):
        with pytest.raises(packed.PackedPublicationBusy, match='retirement-turn'):
            stream_cli.main()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    with stream_cli.retirement_turn.reservation(migration/'source-local-exact-repack'):
        with pytest.raises(packed.PackedPublicationBusy, match='retirement-intent'):
            stream_cli.main()
        # A denied new writer must release the original D owner immediately;
        # otherwise the retirement reservation would deadlock against it.
        with packed._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock'):
            pass
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert stream_cli.main() == 0
    latest = packed.resolve_latest_packed(cold, 'fixture')
    receipt = json.loads((migration/'source-local-exact-repack/fixture'/(latest.manifest_sha256+'.json')).read_text())
    assert receipt['private'] is True and latest.manifest['publisher']['node_id'] == 'test'
    assert [path.name for path in (cold/'heads/fixture').glob('*.json')] == ['test.json']


def test_private_native_immutable_copy_writes_once_to_private_D(tmp_path, monkeypatch):
    cold = tmp_path/'private'
    packed.initialize_packed_layout(cold, node_id='test')
    source = tmp_path/'E-stage'
    source.write_bytes(b'one sequential stream')
    destination = cold/'objects/packs/aa/payload.zip'
    seen = []
    monkeypatch.setattr(packed, '_NATIVE_IMMUTABLE_ROOTS', frozenset((cold,)))
    def native(source_path, destination_path):
        seen.append(destination_path)
        return packed._copy_and_hash(source_path, destination_path)
    monkeypatch.setattr(packed, '_native_copy_and_hash', native)
    digest = __import__('hashlib').sha256(source.read_bytes()).hexdigest()
    assert not packed._install_immutable_object(cold, source, destination,
                                                expected_sha256=digest, retain_source=True)
    assert len(seen) == 1 and seen[0].parent == cold/'.local-state/incoming'
    assert source.read_bytes() == destination.read_bytes()


def test_scoped_floor_reaches_the_native_immutable_installer(tmp_path, monkeypatch):
    cold = tmp_path/'private'
    packed.initialize_packed_layout(cold, node_id='test')
    source = tmp_path/'E-stage'
    source.write_bytes(b'one sequential stream')
    destination = cold/'objects/packs/aa/payload.zip'
    seen = []
    monkeypatch.setattr(packed, '_NATIVE_IMMUTABLE_ROOTS', frozenset((cold,)))
    def native(source_path, destination_path, *, progress=None, reserve_bytes=None):
        seen.append(reserve_bytes)
        return packed._copy_and_hash(source_path, destination_path, progress=progress)
    monkeypatch.setattr(packed, '_native_copy_and_hash', native)
    digest = __import__('hashlib').sha256(source.read_bytes()).hexdigest()
    assert not packed._install_immutable_object(cold, source, destination, expected_sha256=digest,
        retain_source=True, native_reserve_bytes=12 * 1024**3)
    assert seen == [12 * 1024**3] and source.read_bytes() == destination.read_bytes()


def test_stream_repack_forwards_its_scoped_floor_to_every_native_object(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    floor, seen = 12 * 1024**3, []
    monkeypatch.setattr(packed, '_NATIVE_IMMUTABLE_ROOTS', frozenset((cold,)))
    def native(source_path, destination_path, *, progress=None, reserve_bytes=None):
        seen.append(reserve_bytes)
        return packed._copy_and_hash(source_path, destination_path, progress=progress)
    monkeypatch.setattr(packed, '_native_copy_and_hash', native)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  native_write_reserve_bytes=floor)
    assert result['state'] == 'published' and seen and all(value == floor for value in seen)


def test_interrupted_native_incoming_is_bound_and_reconciled_before_capacity_admission(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io
    import hashlib
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    lock = tmp_path/'install-owner.lock'
    monkeypatch.setattr(packed, '_NATIVE_IMMUTABLE_ROOTS', frozenset((cold,)))
    class BrokenWriter:
        def __init__(self, path, **_options):
            self.stream = path.open('xb')
        def write(self, block):
            self.stream.write(block[:10])
            raise OSError('simulated writer interruption')
        def close(self):
            self.stream.close()
    monkeypatch.setattr(windows_cold_io, 'BinaryWriter', BrokenWriter)
    reconcile = stream_repack._reconcile_owned_incoming
    # Model a crash before the current process can perform cleanup.
    monkeypatch.setattr(stream_repack, '_reconcile_owned_incoming',
                        lambda *_args: (_ for _ in ()).throw(OSError('process lost')))
    with pytest.raises(OSError, match='simulated writer interruption'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, install_lock=lock)
    journal = json.loads((stage/'install.json').read_text())
    incoming = cold/'.local-state/incoming'/journal['pending']['incoming']['name']
    assert incoming.is_file() and incoming.stat().st_size == 10
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    monkeypatch.setattr(stream_repack, '_reconcile_owned_incoming', reconcile)
    class GoodWriter:
        def __init__(self, path, **_options):
            self.stream = path.open('xb')
            self.digest = hashlib.sha256()
        def write(self, block):
            self.stream.write(block)
            self.digest.update(block)
        def finish(self):
            self.stream.flush()
            return {'sha256': self.digest.hexdigest()}
        def close(self):
            self.stream.close()
    monkeypatch.setattr(windows_cold_io, 'BinaryWriter', GoodWriter)
    def admitted(_prepared, current):
        assert not incoming.exists() and 'incoming' not in current['pending']
        assert current['retired_incoming'][0]['name'] == incoming.name
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256,
                                  install_lock=lock, admit_install=admitted)
    assert result['state'] == 'published'


def test_interrupted_incoming_wrong_inode_is_never_deleted(tmp_path):
    cold = tmp_path/'cold'
    incoming = cold/'.local-state/incoming'
    incoming.mkdir(parents=True)
    path = incoming/('object-'+'a'*32+'.partial')
    path.write_bytes(b'original')
    info = path.stat()
    digest = 'b'*64
    journal = {'pending': {'sha256': digest, 'target_absent_before': True,
        'incoming': {'name': path.name, 'device': info.st_dev, 'inode': info.st_ino+1,
                     'mode': info.st_mode, 'uid': info.st_uid}}}
    prepared = {'packages': [{'sha256': digest, 'bytes': 100}], 'inventory': {}}
    with pytest.raises(SnapshotError, match='inode changed'):
        stream_repack._reconcile_owned_incoming(cold, journal, tmp_path/'journal.json', prepared)
    assert path.read_bytes() == b'original'


def test_interrupted_incoming_hardlink_to_final_object_only_removes_temporary_name(tmp_path):
    cold = tmp_path/'cold'
    incoming = cold/'.local-state/incoming'
    incoming.mkdir(parents=True)
    path = incoming/('object-'+'a'*32+'.partial')
    path.write_bytes(b'complete')
    digest = 'b'*64
    final = cold/packed._object_relpath('packs', digest, '.zip')
    final.parent.mkdir(parents=True)
    os.link(path, final)
    info = path.stat()
    journal = {'pending': {'sha256': digest, 'target_absent_before': True,
        'incoming': {'name': path.name, 'device': info.st_dev, 'inode': info.st_ino,
                     'mode': info.st_mode, 'uid': info.st_uid}}}
    prepared = {'packages': [{'sha256': digest, 'bytes': 8}], 'inventory': {}}
    assert stream_repack._reconcile_owned_incoming(cold, journal, tmp_path/'journal.json', prepared)
    assert not path.exists() and final.read_bytes() == b'complete'
    assert journal['retired_incoming'][0]['reclaimed_allocated_bytes'] == 0


def test_open_interrupted_incoming_is_retained(tmp_path):
    cold = tmp_path/'cold'
    incoming = cold/'.local-state/incoming'
    incoming.mkdir(parents=True)
    path = incoming/('object-'+'a'*32+'.partial')
    path.write_bytes(b'in use')
    info = path.stat()
    digest = 'b'*64
    journal = {'pending': {'sha256': digest, 'target_absent_before': True,
        'incoming': {'name': path.name, 'device': info.st_dev, 'inode': info.st_ino,
                     'mode': info.st_mode, 'uid': info.st_uid}}}
    prepared = {'packages': [{'sha256': digest, 'bytes': 100}], 'inventory': {}}
    with path.open('rb'):
        with pytest.raises(SnapshotError, match='remains in use'):
            stream_repack._reconcile_owned_incoming(cold, journal, tmp_path/'journal.json', prepared)
    assert path.exists()


def test_full_D_keeps_prepared_E_and_original_head_then_resumes_without_source_read(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    def full(_prepared, _journal):
        raise SnapshotError('D needs space')
    with pytest.raises(SnapshotError, match='D needs space'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_install=full)
    assert (stage/'prepared.json').is_file()
    assert not (stage/'install.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_k: pytest.fail('sealed E source was reread'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_production_cli_prepares_on_E_when_D_full_then_publishes_after_capacity_returns(tmp_path, monkeypatch):
    _source, cold, _stage, old = _fixture(tmp_path, monkeypatch)
    # This test isolates the capacity gate; the synthetic fixture publisher
    # is deliberately named "test" rather than the production penguin owner.
    monkeypatch.setattr(stream_cli, 'foreign_source_head', lambda *_args: False)
    workspace = tmp_path/'workspace'
    workspace.mkdir()
    migration = workspace/'migration'
    migration.mkdir()
    config = {'mount_point': str(workspace), 'minimum_free_bytes': 0}
    monkeypatch.setattr(stream_cli, 'COLD', cold)
    monkeypatch.setattr(stream_cli.workspace_volume, 'configuration', lambda: config)
    monkeypatch.setattr(stream_cli.workspace_volume, 'check', lambda **_k: None)
    monkeypatch.setattr(stream_cli.workspace_volume, 'admit', lambda *_a, **_k: None)
    monkeypatch.setattr(stream_cli.workspace_volume, 'path', lambda *_a, **_k: migration)
    monkeypatch.setattr(stream_cli, '_check_d_primary_mount', lambda *_a: None)
    @contextmanager
    def reserve(*_args, **_kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(stream_cli, 'reserve_capacity', reserve)
    monkeypatch.setattr(stream_cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=stream_cli.D_INSTALL_RESERVE))
    original_stream = stream_repack._stream_inputs
    def local_stream(c, manifest, entries, consume, *, native, completed):
        return original_stream(c, manifest, entries, consume, native=False, completed=completed)
    monkeypatch.setattr(stream_repack, '_stream_inputs', local_stream)
    progress = migration/'stream-source-repack-v1/queue-progress.json'
    monkeypatch.setattr(stream_cli.sys, 'argv', ['repack', '--dataset', 'fixture',
        '--expected-manifest-sha256', old.manifest_sha256, '--progress-file', str(progress)])
    with pytest.raises(SnapshotError, match='D needs space'):
        stream_cli.main()
    stage = migration/'stream-source-repack-v1/fixture'/old.manifest_sha256
    assert (stage/'prepared.json').is_file()
    assert json.loads(progress.read_text())['progress']['stage'] == 'waiting_D_capacity'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_k: pytest.fail('E source rebuilt'))
    monkeypatch.setattr(stream_cli.shutil, 'disk_usage', lambda *_a: SimpleNamespace(free=10**12))
    assert stream_cli.main() == 0
    latest = packed.resolve_latest_packed(cold, 'fixture')
    assert latest.manifest_sha256 != old.manifest_sha256
    assert (migration/'source-local-exact-repack/fixture'/(latest.manifest_sha256+'.json')).is_file()


def test_capacity_admission_uses_remaining_exact_packs_and_checks_installed_generation(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    install = packed._install_immutable_object
    calls = 0
    def interrupt(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('interrupted install')
        return install(*args, **kwargs)
    monkeypatch.setattr(packed, '_install_immutable_object', interrupt)
    with pytest.raises(OSError, match='interrupted install'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256)
    prepared = stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    journal = json.loads((stage/'install.json').read_text())
    assert stream_cli.remaining_install_bytes(cold, prepared, journal) == prepared['inventory']['bytes']
    assert stream_cli.stage_growth_budget(stage, 'fixture', old.manifest_sha256,
        prepared['logical_bytes'], prepared['files'], 123, already_committed=False) == 1024**3
    installed = cold/packed._object_relpath('packs', prepared['packages'][0]['sha256'], '.zip')
    installed.write_bytes(b'x' * installed.stat().st_size)
    with pytest.raises(SnapshotError, match='generation changed'):
        stream_cli.remaining_install_bytes(cold, prepared, journal)


def test_D_reserve_is_rechecked_between_packs_and_resume_keeps_original_head(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    calls = []
    def other_writer_filled_D(item):
        calls.append(item['sha256'])
        if len(calls) == 2:
            raise SnapshotError('D reserve reached')
    with pytest.raises(SnapshotError, match='D reserve reached'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, admit_next=other_writer_filled_D)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert len(json.loads((stage/'install.json').read_text())['installed']) == 1
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_k: pytest.fail('sealed E source was reread'))
    assert stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256)['state'] == 'published'


def test_capacity_budget_counts_only_unfinished_checkpoint_packs(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    consume = stream_repack._PackBuilder.consume
    def interrupt(self, stream, row):
        if self.checkpoint.packages:
            raise OSError('interrupted build')
        return consume(self, stream, row)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupt)
    with pytest.raises(OSError, match='interrupted build'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256)
    checkpoint = stream_repack._Checkpoint(stage, 'fixture', old.manifest_sha256)
    completed_bytes = sum(item['logical_bytes'] for item in checkpoint.packages)
    completed_files = sum(item['file_count'] for item in checkpoint.packages)
    checkpoint.close()
    totals = old.manifest['source']
    assert completed_files > 0
    assert stream_cli.stage_growth_budget(stage, 'fixture', old.manifest_sha256,
        totals['logical_bytes'], totals['files'], 123, already_committed=False) == (
            totals['logical_bytes']-completed_bytes + 123 + (totals['files']-completed_files)*1024 + 1024**3)


def test_stream_repack_resumes_install_without_decoding_again(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    install = packed._install_immutable_object
    calls = []
    def interrupted(*args, **kwargs):
        calls.append(args[2])
        if len(calls) == 2:
            raise OSError('simulated install interruption')
        return install(*args, **kwargs)
    monkeypatch.setattr(packed, '_install_immutable_object', interrupted)
    with pytest.raises(OSError, match='simulated install interruption'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert (stage/'prepared.json').is_file()
    assert len(json.loads((stage/'install.json').read_text())['installed']) == 1
    monkeypatch.setattr(packed, '_install_immutable_object', install)
    monkeypatch.setattr(packed, '_materialize_indexed_files', lambda *_a, **_k: pytest.fail('must reuse E packs'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_stream_repack_recovers_after_install_before_journal_without_reread(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    install = packed._install_immutable_object
    def interrupted(*args, **kwargs):
        install(*args, **kwargs)
        raise OSError('interrupted after durable D link')
    monkeypatch.setattr(packed, '_install_immutable_object', interrupted)
    with pytest.raises(OSError, match='interrupted after durable D link'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert json.loads((stage/'install.json').read_text())['pending']['target_absent_before'] is True
    monkeypatch.setattr(packed, '_install_immutable_object', install)
    monkeypatch.setattr(packed, '_installed_object_sha256', lambda *_a, **_k: pytest.fail('no D readback'))
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published'


def test_stream_repack_rejects_changed_head(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='fixed head changed'):
        stream_repack.repack(cold, stage, 'fixture', 'a'*64, node_id='test')
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def _receipt(result):
    return {'contract': 'stockagent.source_local_exact_repack.v1',
            'state': 'new_D_head_committed_from_exact_E_source', 'dataset': result['dataset'],
            'old_manifest_sha256': result['old_manifest_sha256'],
            'new_manifest_sha256': result['new_manifest_sha256'],
            'new_snapshot_id': result['snapshot_id']}


def test_stream_stage_cleanup_resumes_after_exact_unlink(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    receipt = _receipt(result)
    original = packed._fsync_directory
    failed = False
    def interrupted(path):
        nonlocal failed
        journal = stage/'cleanup.json'
        if path == stage and journal.exists() and json.loads(journal.read_text()).get('pending') and not failed:
            failed = True
            raise OSError('interrupted after unlink')
        return original(path)
    monkeypatch.setattr(packed, '_fsync_directory', interrupted)
    with pytest.raises(OSError, match='interrupted after unlink'):
        stream_repack.cleanup_stage(cold, stage, receipt)
    monkeypatch.setattr(packed, '_fsync_directory', original)
    cleaned = stream_repack.cleanup_stage(cold, stage, receipt)
    assert cleaned['state'] == 'completed' and cleaned['files_removed'] == 3
    assert not list(stage.glob('pack-*.zip')) and not list(stage.glob('inventory-*.jsonl.gz'))
    assert stream_repack.cleanup_stage(cold, stage, receipt)['state'] == 'completed'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == result['new_manifest_sha256']


def test_stream_stage_cleanup_rejects_changed_D_generation(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    latest = packed.resolve_latest_packed(cold, 'fixture')
    target = cold/latest.manifest['archive']['objects'][0]['relpath']
    target.write_bytes(b'x' * target.stat().st_size)
    with pytest.raises(SnapshotError, match='installed D generation changed'):
        stream_repack.cleanup_stage(cold, stage, _receipt(result))
    assert list(stage.glob('pack-*.zip'))


def test_stream_repack_resumes_sealed_packs_after_build_interruption(tmp_path, monkeypatch):
    _source, cold, stage, old = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    consume = stream_repack._PackBuilder.consume
    def interrupt_after_first_seal(self, stream, row):
        if self.checkpoint.packages:
            raise OSError('interrupted after a sealed pack')
        return consume(self, stream, row)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupt_after_first_seal)
    with pytest.raises(OSError, match='interrupted after a sealed pack'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    checkpoint = stream_repack._Checkpoint(stage, 'fixture', old.manifest_sha256)
    assert len(checkpoint.packages) == 1
    completed = {row[0].decode('utf-8') for row in checkpoint.db.execute('SELECT path FROM members')}
    checkpoint.close()
    assert completed
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', consume)
    verify = packed._verified_member_blocks
    def do_not_repeat(stream, size, digest, *, label):
        assert label not in completed
        return verify(stream, size, digest, label=label)
    monkeypatch.setattr(packed, '_verified_member_blocks', do_not_repeat)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert result['state'] == 'published' and result['files'] == 3 and result['objects'] == 3


def test_stream_repack_reports_progress_during_one_large_member(tmp_path):
    import hashlib
    data = b'x' * (9 * 1024**2)
    row = {'path': 'large.bin', 'kind': 'file', 'mode': 0o644, 'size': len(data),
           'sha256': hashlib.sha256(data).hexdigest()}
    events = []
    with packed_metadata.MetadataRows(packed._entry_record, packed._entry_from_record) as output:
        output.append(packed._entry_from_record(row))
        builder = stream_repack._PackBuilder(tmp_path, output, progress=events.append)
        builder.last_report = -100
        builder.consume(io.BytesIO(data), row)
        builder.close()
    assert any(event['files_completed'] == 0 and 0 < event['logical_bytes_processed'] <= len(data)
               for event in events)


def test_parallel_large_member_keeps_one_valid_zip_member_and_source_sha(tmp_path, monkeypatch):
    import hashlib
    import random
    monkeypatch.setattr(stream_repack, 'PARALLEL_DEFLATE_MIN_BYTES', 1)
    data = b'original:-7.25\n' * 600000 + random.Random(42).randbytes(8 * 1024**2) + b'last block'
    row = {'path': 'large.jsonl', 'kind': 'file', 'mode': 0o640, 'size': len(data),
           'sha256': hashlib.sha256(data).hexdigest()}
    with packed_metadata.MetadataRows(packed._entry_record, packed._entry_from_record) as output:
        output.append(packed._entry_from_record(row))
        builder = stream_repack._PackBuilder(tmp_path, output, large_member_deflate_workers=4)
        builder.consume(io.BytesIO(data), row)
        packages = builder.close()
    assert len(packages) == 1 and packages[0]['logical_bytes'] == len(data)
    package = tmp_path/packages[0]['name']
    assert hashlib.sha256(package.read_bytes()).hexdigest() == packages[0]['sha256']
    with zipfile.ZipFile(package) as archive:
        assert archive.namelist() == ['large.jsonl']
        assert archive.testzip() is None and archive.read('large.jsonl') == data


def test_parallel_large_member_rejects_wrong_source_sha_without_sealing(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_repack, 'PARALLEL_DEFLATE_MIN_BYTES', 1)
    data = b'raw' * 10000
    row = {'path': 'large.jsonl', 'kind': 'file', 'mode': 0o600, 'size': len(data), 'sha256': 'a'*64}
    with packed_metadata.MetadataRows(packed._entry_record, packed._entry_from_record) as output:
        output.append(packed._entry_from_record(row))
        builder = stream_repack._PackBuilder(tmp_path, output, large_member_deflate_workers=4)
        with pytest.raises(SnapshotError, match='checksum mismatch'):
            builder.consume(io.BytesIO(data), row)
        builder.abort()
    assert not list(tmp_path.glob('pack-*.zip'))


@pytest.mark.parametrize('workers', [0, 5, True, 1.5])
def test_parallel_large_member_requires_bounded_workers(tmp_path, workers):
    with packed_metadata.MetadataRows(packed._entry_record, packed._entry_from_record) as output:
        with pytest.raises(ValueError, match='one and four'):
            stream_repack._PackBuilder(tmp_path, output, large_member_deflate_workers=workers)


def test_native_stream_batches_distinct_blobs_in_one_reader(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'E-stage'
    source.mkdir()
    for name, data in [('a', b'aaa'), ('b', b'bbbb'), ('c', b'ccccc')]:
        (source/name).write_bytes(data)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', loose_file_threshold_bytes=1)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    batches = []
    @contextmanager
    def reader_many(members):
        batches.append(len(members))
        yield io.BytesIO(b''.join(path.read_bytes() for path, _size in members))
    monkeypatch.setattr(windows_cold_io, 'binary_reader_many', reader_many)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test', native=True)
    assert result['state'] == 'published' and batches == [3]


def test_native_stream_uses_shared_bounded_pack_batcher(tmp_path, monkeypatch):
    from stockagent.data_sync import packed_verification_sources
    source, cold, stage = tmp_path/'source', tmp_path/'cold', tmp_path/'E-stage'
    source.mkdir()
    for index in range(16):
        (source/f'{index:02d}.txt').write_bytes(f'data-{index}'.encode())
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=1024, pack_buckets=8)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    observed = []
    class FakeSources:
        def __init__(self, root, items, *, native):
            self.root = root
            observed.append((len(items), native))
        @contextmanager
        def open(self, item):
            yield self.root/item['relpath']
        def close(self):
            pass
    monkeypatch.setattr(packed_verification_sources, 'PackSources', FakeSources)
    result = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test', native=True)
    assert result['files'] == 16 and observed and observed[0][0] > 1 and observed[0][1]


def test_source_local_pack_materializes_in_physical_offset_order(tmp_path):
    import hashlib
    cold = tmp_path/'cold'
    cold.mkdir()
    temporary = tmp_path/'physical.zip'
    with zipfile.ZipFile(temporary, 'w') as archive:
        for name in ('z.txt', 'a.txt', 'm.txt'):
            archive.writestr(name, name.encode())
        archive.filelist.sort(key=lambda item: item.filename)
    digest = hashlib.sha256(temporary.read_bytes()).hexdigest()
    relative = packed._object_relpath('packs', digest, '.zip')
    target = cold/relative
    target.parent.mkdir(parents=True)
    temporary.rename(target)
    item = {'sha256': digest, 'kind': 'pack', 'relpath': relative.as_posix(),
            'bytes': target.stat().st_size, 'file_count': 3}
    manifest = {'source': {'files': 3}, 'archive': {'objects': [item], 'new_pack_layout': 'source-dataset-v1'}}
    rows = [{'path': name, 'kind': 'file', 'size': len(name), 'mode': 0o644,
             'sha256': hashlib.sha256(name.encode()).hexdigest(),
             'storage': {'kind': 'pack', 'object_sha256': digest, 'member': name}}
            for name in ('a.txt', 'm.txt', 'z.txt')]
    observed = []
    packed._materialize_indexed_files(cold, manifest, rows, None,
        d_primary_native_blob_reads=False, d_primary_native_pack_reads=False,
        durable=False, on_copy=None, durability_queue=None,
        consume_file=lambda stream, row: observed.append((row['path'], stream.read())))
    assert observed == [('z.txt', b'z.txt'), ('a.txt', b'a.txt'), ('m.txt', b'm.txt')]
