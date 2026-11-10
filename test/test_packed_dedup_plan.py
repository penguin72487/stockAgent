import sqlite3

import pytest

from stockagent.data_sync import packed_dedup_plan as plan, packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch):
    source = tmp_path/'source'
    source.mkdir()
    (source/'a').write_bytes(b'same')
    (source/'b').write_bytes(b'same')
    (source/'c').write_bytes(b'other')
    (source/'link').symlink_to('a')
    cold, output = tmp_path/'cold', tmp_path/'E-plan'
    output.mkdir()
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    return cold, output, old


def test_fixed_inventory_plan_counts_duplicates_without_changing_source(tmp_path, monkeypatch):
    cold, output, old = fixture(tmp_path, monkeypatch)
    result = plan.build_plan(cold, 'fixture', old.manifest_sha256, output)
    assert (result['files'], result['logical_bytes'], result['unique_contents']) == (3, 13, 2)
    assert result['unique_content_logical_bytes'] == 9 and result['duplicate_logical_bytes'] == 4
    assert result['inventory_metadata_validated'] and not result['source_payload_reread']
    assert not result['archive_written'] and not result['retirement_authorized']
    with sqlite3.connect(result['index_path']) as db:
        assert sorted((bytes(path).decode(), count) for path, count in
                      db.execute('SELECT representative_path,occurrences FROM contents')) == [('a', 2), ('c', 1)]
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    with pytest.raises(SnapshotError, match='already exists'):
        plan.build_plan(cold, 'fixture', old.manifest_sha256, output)


def test_plan_rejects_wrong_fixed_head_and_bounds(tmp_path, monkeypatch):
    cold, output, old = fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='head changed'):
        plan.build_plan(cold, 'fixture', 'a'*64, output)
    with pytest.raises(SnapshotError, match='memory bound'):
        plan.build_plan(cold, 'fixture', old.manifest_sha256, output, max_unique=1)
    assert not list(output.iterdir())


def test_plan_rejects_inventory_with_same_hash_and_inconsistent_length(tmp_path, monkeypatch):
    cold, output, old = fixture(tmp_path, monkeypatch)
    original = packed._iter_inventory
    def wrong_size(*args, **kwargs):
        for row in original(*args, **kwargs):
            if row['path'] == 'b':
                row['size'] += 1
            yield row
    monkeypatch.setattr(packed, '_iter_inventory', wrong_size)
    with pytest.raises(SnapshotError, match='inconsistent file lengths'):
        plan.build_plan(cold, 'fixture', old.manifest_sha256, output)
    assert not list(output.iterdir())
