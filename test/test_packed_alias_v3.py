import copy
import hashlib
import json
import os
import shutil
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_alias_v3 as aliases, packed_snapshots as packed
from stockagent.data_sync import dedup_stream_candidate as candidate, packed_dedup_plan as plan
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def alias_inventory(tmp_path, monkeypatch):
    source, cold, reports, stage = (tmp_path/name for name in ('source', 'cold', 'reports', 'candidate'))
    (source/'one').mkdir(parents=True)
    (source/'two').mkdir()
    (source/'one/a').write_bytes(b'content')
    (source/'two/b').write_bytes(b'content')
    (source/'two/c').write_bytes(b'other')
    (source/'two/link').symlink_to('../one/a')
    reports.mkdir()
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
                                          loose_file_threshold_bytes=8, file_chunk_bytes=4, pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    sealed = candidate.prepare(cold, old, stage, index)
    objects = [{'sha256': row['sha256'], 'kind': 'pack', 'bytes': row['bytes'],
                'relpath': row['name'], 'file_count': row['file_count'], 'logical_bytes': row['logical_bytes'],
                'stored_file_count': row['stored_file_count'], 'stored_logical_bytes': row['stored_logical_bytes']}
               for row in sealed['packages']]
    inv = sealed['inventory']
    manifest = copy.deepcopy(old.manifest)
    manifest['schema_version'] = aliases.SCHEMA_VERSION
    manifest['archive'] = {'format': aliases.ARCHIVE_FORMAT, 'objects': objects,
                           'inventory': {'relpath': inv['name'], 'sha256': inv['sha256'], 'bytes': inv['bytes']}}
    return manifest, list(packed._iter_inventory(stage, manifest)), stage, old


def test_alias_inventory_preserves_logical_counts_and_unique_physical_members(alias_inventory):
    manifest, rows, _, _ = alias_inventory
    checked = aliases.validate_inventory(manifest, rows)
    assert checked['counts'] == {'files': 3, 'directories': 2, 'symlinks': 1, 'logical_bytes': 19}
    assert checked['unique_content_files'] == 2 and checked['unique_content_logical_bytes'] == 12
    assert sum(map(len, checked['object_members'].values())) == 2
    assert sorted(value[2] for value in checked['alias_members'].values()) == [1, 2]
    legacy = {**manifest, 'schema_version': 2}
    with pytest.raises(SnapshotError, match='member/path mismatch'):
        packed._validate_inventory(legacy, copy.deepcopy(rows))
    assert packed._validate_inventory(manifest, copy.deepcopy(rows))['counts'] == checked['counts']


@pytest.mark.parametrize('damage', ['alias_sha', 'representative', 'physical_total', 'mode', 'order', 'schema'])
def test_alias_inventory_rejects_tampering(alias_inventory, damage):
    manifest, rows, _, _ = alias_inventory
    manifest, rows = copy.deepcopy(manifest), copy.deepcopy(rows)
    files = [row for row in rows if row['kind'] == 'file']
    if damage == 'alias_sha':
        files[1]['sha256'] = 'f'*64
    elif damage == 'representative':
        files[0]['storage']['member'] = 'missing/representative'
        files[1]['storage']['member'] = 'missing/representative'
    elif damage == 'physical_total':
        manifest['archive']['objects'][0]['stored_file_count'] += 1
    elif damage == 'mode':
        files[0]['mode'] = 0o10000
    elif damage == 'order':
        rows[0], rows[1] = rows[1], rows[0]
    else:
        manifest['schema_version'] = 2
    with pytest.raises(SnapshotError):
        aliases.validate_inventory(manifest, rows)


def test_alias_reader_decodes_unique_members_and_accounts_for_all_logical_paths(alias_inventory):
    manifest, rows, stage, old = alias_inventory
    resolved = SimpleNamespace(manifest=manifest, manifest_sha256=old.manifest_sha256)
    all_proof = aliases.verify_inventory(stage, resolved, rows, reconstruct_all=True)
    assert all_proof['independently_reconstructed_files'] == 3
    assert all_proof['independently_reconstructed_bytes'] == 19
    assert all_proof['content_alias_reconstruction']['unique_members_decoded'] == 2
    assert not all_proof['content_alias_reconstruction']['old_independent_occurrences_reread']
    selected = aliases.verify_inventory(stage, resolved, rows, reconstruct_paths=['two/b'])
    assert selected['independently_reconstructed_files'] == 1
    assert selected['independently_reconstructed_bytes'] == 7
    with pytest.raises(SnapshotError, match='unknown or non-file'):
        aliases.verify_inventory(stage, resolved, rows, reconstruct_paths=['missing'])
    with pytest.raises(SnapshotError, match='v3-capable'):
        aliases.verify_inventory(stage, resolved, rows, reconstruct_all=True, pack_verification={})


def test_alias_reader_rejects_changed_encoded_object(alias_inventory):
    manifest, rows, stage, old = alias_inventory
    path = stage/manifest['archive']['objects'][0]['relpath']
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    resolved = SimpleNamespace(manifest=manifest, manifest_sha256=old.manifest_sha256)
    with pytest.raises(SnapshotError, match='object SHA differs'):
        aliases.verify_inventory(stage, resolved, rows, reconstruct_all=True)


def test_alias_materialization_keeps_independent_files_and_modes(alias_inventory, tmp_path, monkeypatch):
    manifest, rows, stage, _ = alias_inventory
    from stockagent.data_sync import workspace_volume
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    destination = tmp_path/'restore'
    (destination/'one').mkdir(parents=True)
    (destination/'two').mkdir()
    for row in rows:
        if row['path'] == 'one/a':
            row['mode'] = 0o000
        elif row['path'] == 'two/b':
            row['mode'] = 0o640
    copied_rows = []
    aliases.materialize(stage, manifest, rows, lambda row: destination/row['path'],
                        native_pack_reads=False, durable=False, on_copy=copied_rows.append)
    a, b, c = destination/'one/a', destination/'two/b', destination/'two/c'
    assert a.read_bytes() == b.read_bytes() == b'content' and c.read_bytes() == b'other'
    assert (a.stat().st_dev, a.stat().st_ino) != (b.stat().st_dev, b.stat().st_ino)
    assert a.stat().st_mode & 0o777 == 0 and b.stat().st_mode & 0o777 == 0o640
    assert len(copied_rows) == 3
    os.chmod(a, 0o600)
    a.write_bytes(b'changed')
    assert b.read_bytes() == b'content'


def test_alias_bounded_reader_can_select_two_paths_with_same_member(alias_inventory):
    manifest, rows, stage, _ = alias_inventory
    selected = [row for row in rows if row['path'] in {'one/a', 'two/b'}]
    result = {}
    def consume(stream, row):
        result[row['path']] = b''.join(packed._verified_member_blocks(stream, row['size'], row['sha256']))
    aliases.materialize(stage, manifest, selected, None, native_pack_reads=False, durable=False,
                        consume_file=consume)
    assert result == {'one/a': b'content', 'two/b': b'content'}


def test_incremental_alias_inventory_keeps_removed_representative_and_mixed_extents(alias_inventory, tmp_path, monkeypatch):
    manifest, rows, stage, old = alias_inventory
    manifest, rows = copy.deepcopy(manifest), copy.deepcopy(rows)
    manifest['archive']['alias_policy'] = aliases.INCREMENTAL_POLICY
    removed = next(row for row in rows if row['path'] == 'one/a')
    rows = [row for row in rows if row['path'] != 'one/a']
    pack = next(item for item in manifest['archive']['objects']
                if item['sha256'] == removed['storage']['object_sha256'])
    pack['file_count'] -= 1
    pack['logical_bytes'] -= removed['size']
    manifest['source']['files'] -= 1
    manifest['source']['logical_bytes'] -= removed['size']
    blob = b'blob!'
    blob_sha = hashlib.sha256(blob).hexdigest()
    (stage/'new-blob').write_bytes(blob)
    manifest['archive']['objects'].append({'sha256': blob_sha, 'kind': 'blob', 'bytes': len(blob),
        'relpath': 'new-blob', 'file_count': 1, 'logical_bytes': len(blob)})
    rows.append({'path': 'z-blob', 'kind': 'file', 'mode': 0o600, 'size': len(blob), 'sha256': blob_sha,
                 'storage': {'kind': 'blob', 'object_sha256': blob_sha}})
    chunks = [b'chunk-', b'payload']
    descriptors = []
    for index, chunk in enumerate(chunks):
        sha = hashlib.sha256(chunk).hexdigest()
        name = 'new-chunk-'+str(index)
        (stage/name).write_bytes(chunk)
        manifest['archive']['objects'].append({'sha256': sha, 'kind': 'blob', 'bytes': len(chunk),
            'relpath': name, 'file_count': 1, 'logical_bytes': len(chunk)})
        descriptors.append({'object_sha256': sha, 'bytes': len(chunk)})
    combined = b''.join(chunks)
    rows.append({'path': 'z-chunks', 'kind': 'file', 'mode': 0o600, 'size': len(combined),
                 'sha256': hashlib.sha256(combined).hexdigest(),
                 'storage': {'kind': 'chunks', 'chunks': descriptors}})
    rows.sort(key=lambda row: row['path'])
    manifest['source']['files'] += 2
    manifest['source']['logical_bytes'] += len(blob)+len(combined)
    result = aliases.validate_inventory(manifest, rows)
    assert result['counts']['files'] == 4 and result['direct_blob_stats'][blob_sha] == [1, len(blob)]
    proof = aliases.verify_inventory(stage, SimpleNamespace(manifest=manifest, manifest_sha256=old.manifest_sha256),
                                     rows, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 4
    assert proof['independently_reconstructed_bytes'] == 12+len(blob)+len(combined)
    from stockagent.data_sync import workspace_volume
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    destination = tmp_path/'incremental-restore'
    (destination/'one').mkdir(parents=True)
    (destination/'two').mkdir()
    aliases.materialize(stage, manifest, rows, lambda row: destination/row['path'],
                        native_pack_reads=False, durable=False)
    assert (destination/'two/b').read_bytes() == b'content'
    assert (destination/'two/c').read_bytes() == b'other'
    assert (destination/'z-blob').read_bytes() == blob
    assert (destination/'z-chunks').read_bytes() == combined
    selected = {}
    def consume(stream, row):
        selected[row['path']] = b''.join(packed._verified_member_blocks(stream, row['size'], row['sha256']))
    aliases.materialize(stage, manifest, [row for row in rows if row['path'] in {'two/b', 'z-blob', 'z-chunks'}],
                        None, native_pack_reads=False, durable=False, consume_file=consume)
    assert selected == {'two/b': b'content', 'z-blob': blob, 'z-chunks': combined}


def test_canonical_v3_resolve_verify_read_restore_and_incremental_publish(alias_inventory, tmp_path, monkeypatch):
    manifest, _rows, stage, old = alias_inventory
    cold = old.manifest_path.parents[2]
    from stockagent.data_sync import workspace_volume, cache_capacity
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    monkeypatch.setattr(cache_capacity, 'reserve_cache_write', lambda *_a, **_kw: nullcontext())
    manifest = copy.deepcopy(manifest)
    manifest['snapshot_id'] = 'fixture-alias-v3'
    for item in manifest['archive']['objects']:
        origin = stage/item['relpath']
        item['relpath'] = packed._object_relpath('packs', item['sha256'], '.zip').as_posix()
        target = cold/item['relpath']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(origin, target)
    inventory = manifest['archive']['inventory']
    origin = stage/inventory['relpath']
    inventory['relpath'] = packed._object_relpath('inventories', inventory['sha256'], '.jsonl.gz').as_posix()
    target = cold/inventory['relpath']
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(origin, target)
    manifest_path = cold/'manifests/fixture/fixture-alias-v3.json'
    raw = packed._canonical_json_bytes(manifest)
    manifest_path.write_bytes(raw)
    head = json.loads(old.head_path.read_text())
    head.update(snapshot_id=manifest['snapshot_id'],
                manifest_relpath=manifest_path.relative_to(cold).as_posix(),
                manifest_sha256=hashlib.sha256(raw).hexdigest())
    old.head_path.write_text(json.dumps(head))
    current = packed.resolve_latest_packed(cold, 'fixture')
    assert current.manifest['schema_version'] == 3
    proof = packed.verify_packed_snapshot(cold, current, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 3
    assert proof['content_alias_reconstruction']['unique_members_decoded'] == 2
    found = packed.read_packed_files(cold, current, names=['one/a', 'two/b'],
                                     maximum_bytes=128, maximum_object_bytes=1024**2)
    assert found == {'one/a': b'content', 'two/b': b'content'}
    restored = packed.fetch_packed_snapshot(cold, tmp_path/'restored', current)
    assert (restored/'one/a').read_bytes() == (restored/'two/b').read_bytes() == b'content'
    assert (restored/'two/link').is_symlink()
    assert (restored/'one/a').stat().st_ino != (restored/'two/b').stat().st_ino
    with pytest.raises(SnapshotError, match='explicit v3 reader capability'):
        packed.publish_packed_snapshot(cold, 'fixture', tmp_path/'source', node_id='test')
    unchanged = packed.publish_packed_snapshot(cold, 'fixture', tmp_path/'source', node_id='test',
                                               allow_content_alias_v3=True,
                                               loose_file_threshold_bytes=8, file_chunk_bytes=4,
                                               pack_buckets=4)
    assert unchanged.manifest['schema_version'] == 3
    source = tmp_path/'source'
    (source/'one/a').unlink()  # the representative may disappear while another alias remains
    (source/'one/new').write_bytes(b'new')
    (source/'two/c').write_bytes(b'changed-content')
    updated = packed.publish_packed_snapshot(
        cold, 'fixture', source, node_id='test', loose_file_threshold_bytes=8,
        file_chunk_bytes=4, large_file_layout=packed.CHUNKED_LARGE_FILE_LAYOUT, pack_buckets=4,
        allow_content_alias_v3=True)
    assert updated.manifest['schema_version'] == 3
    assert updated.manifest['archive']['alias_policy'] == aliases.INCREMENTAL_POLICY
    assert updated.manifest_sha256 != current.manifest_sha256
    rows = list(packed._iter_inventory(cold, updated.manifest))
    by_path = {row['path']: row for row in rows}
    assert by_path['two/b']['storage']['member'] == 'one/a'
    assert by_path['two/c']['storage']['kind'] == 'chunks'
    assert by_path['one/new']['storage']['kind'] == 'pack'
    proof = packed.verify_packed_snapshot(cold, updated, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 3
    result = packed.fetch_packed_snapshot(cold, tmp_path/'restored-updated', updated)
    assert (result/'two/b').read_bytes() == b'content'
    assert (result/'two/c').read_bytes() == b'changed-content'
    assert (result/'one/new').read_bytes() == b'new'
    assert (result/'two/link').is_symlink()
    # A later whole-file generation can mix old chunks, shared packs and blobs.
    (source/'whole').write_bytes(b'new-large-whole-file')
    mixed = packed.publish_packed_snapshot(
        cold, 'fixture', source, node_id='test', loose_file_threshold_bytes=8,
        file_chunk_bytes=4, large_file_layout=packed.WHOLE_LARGE_FILE_LAYOUT, pack_buckets=4,
        allow_content_alias_v3=True)
    mixed_rows = {row['path']: row for row in packed._iter_inventory(cold, mixed.manifest)}
    assert mixed_rows['whole']['storage']['kind'] == 'blob'
    assert mixed_rows['two/c']['storage']['kind'] == 'chunks'
    packed.verify_packed_snapshot(cold, mixed, reconstruct_all=True)
    mixed_restore = packed.fetch_packed_snapshot(cold, tmp_path/'restored-mixed', mixed)
    assert (mixed_restore/'whole').read_bytes() == b'new-large-whole-file'
    assert (mixed_restore/'two/c').read_bytes() == b'changed-content'
    # A new path with old bytes is a logical alias, not another physical pack.
    original_objects = {row['sha256'] for row in mixed.manifest['archive']['objects']}
    (source/'copy-one').write_bytes(b'content')
    (source/'copy-two').write_bytes(b'content')
    copied = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        loose_file_threshold_bytes=8, file_chunk_bytes=4, pack_buckets=4, allow_content_alias_v3=True)
    copied_rows = {row['path']: row for row in packed._iter_inventory(cold, copied.manifest)}
    assert copied_rows['copy-one']['storage'] == copied_rows['two/b']['storage']
    assert copied_rows['copy-two']['storage'] == copied_rows['two/b']['storage']
    assert {row['sha256'] for row in copied.manifest['archive']['objects']} == original_objects
    assert copied.manifest['archive']['new_object_count'] == 1  # the new logical inventory only
    copied_restore = packed.fetch_packed_snapshot(cold, tmp_path/'restored-copies', copied)
    assert (copied_restore/'copy-one').read_bytes() == b'content'
    assert (copied_restore/'copy-two').read_bytes() == b'content'


def test_candidate_adoption_uses_existing_E_and_D_install_without_extra_copy(alias_inventory, tmp_path, monkeypatch):
    _manifest, _rows, stage, old = alias_inventory
    cold = old.manifest_path.parents[2]
    from stockagent.data_sync import stream_repack, temporary_e_authority, workspace_volume, cache_capacity
    monkeypatch.setattr(workspace_volume, 'ENROLLMENT', tmp_path/'no-e-enrollment')
    monkeypatch.setattr(cache_capacity, 'reserve_cache_write', lambda *_a, **_kw: nullcontext())
    original_candidate = candidate.read_sealed(stage, 'fixture', old.manifest_sha256)
    original_inodes = {item['name']: (stage/item['name']).stat().st_ino
                       for item in [*original_candidate['packages'], original_candidate['inventory']]}
    prepared = candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    assert prepared['packed_format'] == 'content-alias-v3'
    assert prepared['unique_content_sha_streamed'] is True
    assert prepared['source_member_sha_streamed'] is False
    assert prepared['all_original_occurrences_reread'] is False
    assert prepared['destination_independent_readback'] is False
    assert candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256) == prepared
    assert original_inodes == {name: (stage/name).stat().st_ino for name in original_inodes}
    marker = temporary_e_authority.expose(cold, stage, 'fixture', old.manifest_sha256)
    assert marker['state'] == 'ready'
    with temporary_e_authority.borrow(stage, 'fixture', old.manifest_sha256) as view:
        assert view.manifest['schema_version'] == 3
        assert packed.read_packed_files(stage, view, names=['one/a', 'two/b'],
                                        maximum_bytes=128, maximum_object_bytes=1024**2) == {
                                            'one/a': b'content', 'two/b': b'content'}
    with pytest.raises(SnapshotError, match='not activated'):
        stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    stream_repack.activate_content_alias_install(stage, 'fixture', old.manifest_sha256,
                                                 reader_contract='packed-v3-content-alias')
    monkeypatch.setattr(stream_repack, 'validate_new_inventory',
                        lambda *_a, **_kw: pytest.fail('v3 migration reread its already checked metadata'))
    published = stream_repack.repack(cold, stage, 'fixture', old.manifest_sha256, node_id='test')
    assert published['state'] == 'published'
    current = packed.resolve_latest_packed(cold, 'fixture')
    assert current.manifest['schema_version'] == 3
    assert packed.verify_packed_snapshot(cold, current, reconstruct_all=True)['independently_reconstructed_files'] == 3
    assert (cold/old.manifest['archive']['inventory']['relpath']).exists()


def test_candidate_adoption_rejects_tampered_binding_before_head_change(alias_inventory):
    _manifest, _rows, stage, old = alias_inventory
    cold = old.manifest_path.parents[2]
    candidate.prepare_stream_adoption(cold, stage, 'fixture', old.manifest_sha256)
    candidate_path = stage/'candidate.json'
    value = json.loads(candidate_path.read_text())
    value['logical_bytes'] += 1
    candidate_path.write_text(json.dumps(value))
    from stockagent.data_sync import stream_repack
    with pytest.raises(SnapshotError, match='candidate binding'):
        stream_repack._read_prepared(stage, 'fixture', old.manifest_sha256)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
