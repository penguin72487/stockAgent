import hashlib
import io
import json
import sqlite3

import polars as pl
import pytest

from scripts import audit_binance_archive_generation as audit
from scripts import audit_binance_archive_controls as controls
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.fixed_snapshot_spool import FixedSnapshotSpool, populate_pair


def payload(rows):
    output = io.BytesIO()
    pl.DataFrame(rows).write_parquet(output)
    return output.getvalue()


def binding(data):
    return {'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()}


def make_spool(tmp_path):
    values = {'archive_plan.parquet': {'public': b'old', 'private': b'new'}}
    records = {name: {side: binding(data) for side, data in item.items()} for name, item in values.items()}
    return tmp_path/'scratch.sqlite', records, values


def test_spool_resumes_only_fixed_members_and_removes_its_scratch(tmp_path):
    path, records, values = make_spool(tmp_path)
    args = {'maximum_file_bytes': 1024, 'maximum_side_bytes': 2048}
    spool = FixedSnapshotSpool(path, {'plan': 'fixed'}, records, **args)
    spool.put('private', 'archive_plan.parquet', values['archive_plan.parquet']['private'])
    spool.commit(); spool.close()
    resumed = FixedSnapshotSpool(path, {'plan': 'fixed'}, records, **args)
    assert not resumed.missing('private') and resumed.missing('public') == {'archive_plan.parquet'}
    assert resumed.get('private', 'archive_plan.parquet') == b'new'
    resumed.remove()
    assert not path.exists()


def test_spool_rejects_changed_identity_and_bad_content(tmp_path):
    path, records, values = make_spool(tmp_path)
    args = {'maximum_file_bytes': 1024, 'maximum_side_bytes': 2048}
    spool = FixedSnapshotSpool(path, {'plan': 'fixed'}, records, **args)
    with pytest.raises(SnapshotError, match='worklist'):
        spool.put('private', 'archive_plan.parquet', b'bad')
    spool.put('private', 'archive_plan.parquet', values['archive_plan.parquet']['private'])
    spool.commit(); spool.close()
    with pytest.raises(SnapshotError, match='generation differs'):
        FixedSnapshotSpool(path, {'plan': 'different'}, records, **args)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE members SET payload=? WHERE side='private'", (b'bad',))
    spool = FixedSnapshotSpool(path, {'plan': 'fixed'}, records, **args)
    with pytest.raises(SnapshotError, match='content changed'):
        spool.get('private', 'archive_plan.parquet')
    spool.close()


def test_two_immutable_snapshots_compare_even_when_live_source_has_changed(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    original = payload([{'key': 'a', 'state': 'pending'}])
    newer = payload([{'key': 'a', 'state': 'complete'}, {'key': 'b', 'state': 'complete'}])
    name = 'archive_plan.parquet'
    public_source, private_source = tmp_path/'public-source', tmp_path/'private-source'
    public_source.mkdir(); private_source.mkdir()
    (public_source/name).write_bytes(original)
    (private_source/name).write_bytes(newer)
    public_root, private_root = tmp_path/'public-D', tmp_path/'private-D'
    public = packed.publish_packed_snapshot(public_root, 'fixture', public_source, node_id='test',
                                            pack_layout='source-dataset-v1')
    private = packed.publish_packed_snapshot(private_root, 'fixture', private_source, node_id='test',
                                             pack_layout='source-dataset-v1')
    (private_source/name).write_bytes(b'live source has since changed')
    records = {name: {'public': binding(original), 'private': binding(newer)}}
    spool = FixedSnapshotSpool(tmp_path/'scratch.sqlite', {'plan': 'fixed'}, records,
                               maximum_file_bytes=64*1024**2, maximum_side_bytes=4*1024**3)
    report = {'categories': {}, 'differences': [], 'files_completed': 0, 'logical_bytes_completed': 0}
    proof = audit.compare_fixed_members(spool, records, cold=public_root, current=public,
        private_root=private_root, private=private, report=report, guard=lambda: None,
        save_status=lambda *_args: None)
    assert report['files_completed'] == 1
    assert report['categories']['plan']['new_only_rows'] == 1
    assert proof['public']['original_member_sha256_verified'] and proof['private']['files'] == 1
    assert not proof['private']['independent_recovery_verified']
    spool.remove()


def test_stream_without_source_proof_does_not_become_verified(tmp_path):
    path, records, values = make_spool(tmp_path)
    spool = FixedSnapshotSpool(path, {'plan': 'fixed'}, records,
                               maximum_file_bytes=1024, maximum_side_bytes=2048)
    def no_proof(_root, _snapshot, *, names, consume, **_kwargs):
        for name in names:
            consume(name, values[name]['private'])
        return {'files': len(names), 'logical_bytes': 3}
    with pytest.raises(SnapshotError, match='stream proof'):
        populate_pair(spool, records, sides=(('private', None, None), ('public', None, None)),
                      stream=no_proof, guard=lambda: None, maximum_file_bytes=1024, maximum_side_bytes=2048)
    spool.close()


def test_control_comparison_uses_fixed_generations(tmp_path, monkeypatch):
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    name = 'download_summary.json'
    old, new = json.dumps({'count': 1}).encode(), json.dumps({'count': 2, 'added': True}).encode()
    public_source, private_source = tmp_path/'public-source', tmp_path/'private-source'
    public_source.mkdir(); private_source.mkdir()
    (public_source/name).write_bytes(old)
    (private_source/name).write_bytes(new)
    public_root, private_root = tmp_path/'public-D', tmp_path/'private-D'
    public = packed.publish_packed_snapshot(public_root, 'fixture', public_source, node_id='test',
                                            pack_layout='source-dataset-v1')
    private = packed.publish_packed_snapshot(private_root, 'fixture', private_source, node_id='test',
                                             pack_layout='source-dataset-v1')
    (private_source/name).write_text('{"count": 99}')
    records = {name: {'public': binding(old), 'private': binding(new)}}
    spool = FixedSnapshotSpool(tmp_path/'scratch.sqlite', {'plan': 'fixed'}, records,
                               maximum_file_bytes=256*1024**2, maximum_side_bytes=512*1024**2)
    result = {'files_completed': 0, 'changed_json_fields': {}}
    proof = controls.compare_fixed_controls(spool, records, cold=public_root, current=public,
        private_root=private_root, private=private, result=result, guard=lambda: None,
        save_status=lambda *_args: None)
    assert result['changed_json_fields'][name] == ['added', 'count']
    assert result['files_completed'] == 1 and proof['public']['original_member_sha256_verified']
    assert proof['private']['independent_recovery_verified'] is False
    spool.remove()
