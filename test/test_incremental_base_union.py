"""Metadata-only incremental unions must preserve the exact original tree."""
import pytest

from stockagent.data_sync import packed_metadata, packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize('spill', [False, True])
def test_content_alias_union_reuses_old_bytes_and_packs_only_the_new_delta(tmp_path, monkeypatch, spill):
    import copy
    import hashlib
    import json
    from stockagent.data_sync import stream_repack
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'old-a').write_bytes(b'old value')
    (source/'old-b').write_bytes(b'old value')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_k: None)
    original = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    # A canonical repack supplies a v2 source-local base; admit the existing
    # v3 incremental reader using the same independently checked inventory.
    stream_repack.repack(cold, tmp_path/'baseline', 'fixture', original.manifest_sha256)
    base = packed.resolve_latest_packed(cold, 'fixture')
    # A valid incremental v3 inventory may retain legacy one-path ZIP members.
    # Construct this independently of the new writer so the test exercises
    # reader/publisher compatibility rather than mirroring its dedup choices.
    manifest = copy.deepcopy(base.manifest)
    manifest['schema_version'] = 3
    manifest['archive']['format'] = packed.ALIAS_ARCHIVE_FORMAT
    manifest['archive']['alias_policy'] = 'incremental-shared-v1'
    for item in manifest['archive']['objects']:
        item['stored_file_count'] = item['file_count']
        item['stored_logical_bytes'] = item['logical_bytes']
    raw = packed._canonical_json_bytes(manifest)
    base.manifest_path.write_bytes(raw)
    head = json.loads(base.head_path.read_bytes())
    head['manifest_sha256'] = hashlib.sha256(raw).hexdigest()
    base.head_path.write_text(json.dumps(head))
    base = packed.resolve_latest_packed(cold, 'fixture')
    if spill:
        monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', 1)
        monkeypatch.setattr(packed_metadata, '_metadata_scratch_location', lambda: (tmp_path, 0, False))
    (source/'old-a').unlink(); (source/'old-b').unlink()
    (source/'new').write_bytes(b'new value')
    merged = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        require_previous_files_present=True, merge_previous_missing_files=True,
        allow_content_alias_v3=True)
    assert merged.manifest['schema_version'] == 3
    rows = {row['path']:row for row in packed._iter_inventory(cold, merged.manifest)}
    old_rows = {row['path']:row for row in packed._iter_inventory(cold, base.manifest)}
    assert rows['old-a']['storage'] == old_rows['old-a']['storage']
    assert rows['old-b']['storage'] == old_rows['old-b']['storage']
    restored = packed.fetch_packed_snapshot(cold, tmp_path/'restored', merged)
    assert (restored/'old-a').read_bytes() == (restored/'old-b').read_bytes() == b'old value'
    assert (restored/'new').read_bytes() == b'new value'


@pytest.mark.parametrize('spill', [False, True])
def test_partial_workspace_reuses_fixed_base_without_reading_old_payload(tmp_path, monkeypatch, spill):
    source, cold = tmp_path/'source', tmp_path/'cold'
    for name, data in {'a/old.txt': b'old', 'a/nested/x': b'x', 'a-foo': b'first',
                       'z/file': b'z', 'shared': b'before'}.items():
        path = source/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    if spill:
        monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', 2)
        monkeypatch.setattr(packed_metadata, '_metadata_scratch_location', lambda: (tmp_path, 0, False))
    original = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    (source/'a/old.txt').unlink()
    (source/'a/nested/x').unlink()
    (source/'a/nested').rmdir()
    (source/'shared').write_bytes(b'after')
    (source/'new').write_bytes(b'fresh')
    with monkeypatch.context() as scoped:
        # During publish the old content is referenced, not reconstructed.
        scoped.setattr(packed, 'fetch_packed_snapshot', lambda *a, **k: pytest.fail('unexpected restore'))
        scoped.setattr(packed, 'verify_packed_snapshot', lambda *a, **k: pytest.fail('unexpected readback'))
        merged = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            require_previous_files_present=True, merge_previous_missing_files=True)
    binding = merged.manifest['source']['selection']['incremental_base']
    assert binding['manifest_sha256'] == original.manifest_sha256
    assert binding['retained_members'] == 3  # directory plus its two files
    recovered = packed.fetch_packed_snapshot(cold, tmp_path/'recover', merged)
    for name, data in {'a/old.txt': b'old', 'a/nested/x': b'x', 'a-foo': b'first',
                       'z/file': b'z', 'shared': b'after', 'new': b'fresh'}.items():
        assert (recovered/name).read_bytes() == data
    assert packed.verify_packed_snapshot(cold, merged, materialized_path=recovered)['materialized_verified']
    # No content changes means no extra generation, even with a partial E tree.
    assert packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        require_previous_files_present=True, merge_previous_missing_files=True).manifest_sha256 == merged.manifest_sha256


def test_base_path_type_change_does_not_replace_previous_head(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    (source/'sub').mkdir(parents=True)
    (source/'sub/old').write_bytes(b'old')
    original = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    (source/'sub/old').unlink()
    (source/'sub').rmdir()
    (source/'sub').write_bytes(b'new')
    with pytest.raises(SnapshotError, match='changes a previous path type'):
        packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            require_previous_files_present=True, merge_previous_missing_files=True)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == original.manifest_sha256


def test_base_with_symlink_requires_separate_contract(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'old').write_bytes(b'old')
    (source/'link').symlink_to('old')
    original = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    (source/'old').unlink()
    with pytest.raises(SnapshotError, match='symlinks needs its own restore contract'):
        packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            require_previous_files_present=True, merge_previous_missing_files=True)
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == original.manifest_sha256
