import gzip
import json
import os
import sqlite3
import zipfile
from contextlib import contextmanager
import io

import pytest

from stockagent.data_sync import dedup_stream_candidate as candidate
from stockagent.data_sync import dedup_native_packs, packed_dedup_plan as plan, packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch):
    source, cold, reports, stage = (tmp_path/name for name in ('source', 'cold', 'reports', 'E-candidate'))
    (source/'one').mkdir(parents=True)
    (source/'two').mkdir()
    (source/'one/a').write_bytes(b'same-content')
    (source/'two/b').write_bytes(b'same-content')
    os.chmod(source/'one/a', 0o600)
    os.chmod(source/'two/b', 0o644)
    (source/'two/c').write_bytes(b'different')
    (source/'two/link').symlink_to('../one/a')
    reports.mkdir()
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
                                          loose_file_threshold_bytes=8, file_chunk_bytes=4, pack_buckets=4)
    result = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)
    return source, cold, stage, old, result['index_path']


def test_candidate_stores_one_content_and_preserves_all_logical_paths(tmp_path, monkeypatch):
    source, cold, stage, old, index = fixture(tmp_path, monkeypatch)
    events = []
    result = candidate.prepare(cold, old, stage, index, progress=events.append)
    assert result['files'] == 3 and result['unique_contents'] == 2
    assert result['unique_content_sha_streamed'] and not result['all_original_occurrences_reread']
    assert not result['publishable'] and not result['retirement_authorized']
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    with gzip.open(stage/result['inventory']['name'], 'rt') as stream:
        rows = [json.loads(line) for line in stream]
    files = [row for row in rows if row['kind'] == 'file']
    assert {row['path'] for row in files} == {'one/a', 'two/b', 'two/c'}
    assert {row['path']: row['mode'] for row in files}['one/a'] == 0o600
    assert {row['path']: row['mode'] for row in files}['two/b'] == 0o644
    assert next(row for row in rows if row['path'] == 'two/link')['target'] == '../one/a'
    objects = {item['sha256']: stage/item['name'] for item in result['packages']}
    for row in files:
        with zipfile.ZipFile(objects[row['storage']['object_sha256']]) as archive:
            assert archive.read(row['storage']['member']) == (source/row['path']).read_bytes()
    assert sum(item['stored_file_count'] for item in result['packages']) == 2
    assert sum(item['file_count'] for item in result['packages']) == 3
    assert any(event['stage'] == 'compressing_candidate_inventory'
               and event['items_completed'] == event['items_total'] == 6 for event in events)
    assert candidate.prepare(cold, old, stage, index) == result


def test_candidate_rejects_forged_representative_before_packing(tmp_path, monkeypatch):
    _source, cold, stage, old, index = fixture(tmp_path, monkeypatch)
    with sqlite3.connect(index) as db:
        db.execute("UPDATE contents SET representative_path=? WHERE representative_path=?", (b'two/b', b'one/a'))
    with pytest.raises(SnapshotError, match='representative differs'):
        candidate.prepare(cold, old, stage, index)
    assert not (stage/'candidate.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_candidate_resumes_only_sealed_representative_packs(tmp_path, monkeypatch):
    _source, cold, stage, old, index = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    consume = stream_repack._PackBuilder.consume
    def interrupt(self, source, row):
        result = consume(self, source, row)
        if self.checkpoint.packages:
            raise OSError('interrupted candidate')
        return result
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupt)
    with pytest.raises(OSError, match='interrupted candidate'):
        candidate.prepare(cold, old, stage, index)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', consume)
    result = candidate.prepare(cold, old, stage, index)
    assert result['unique_contents'] == 2 and sum(x['stored_file_count'] for x in result['packages']) == 2


def test_corrupt_source_chunk_never_creates_a_candidate_or_changes_head(tmp_path, monkeypatch):
    _source, cold, stage, old, index = fixture(tmp_path, monkeypatch)
    blob = next(item for item in old.manifest['archive']['objects'] if item['kind'] == 'blob')
    path = cold/blob['relpath']
    path.write_bytes(b'x'*path.stat().st_size)
    with pytest.raises(SnapshotError, match='checksum'):
        candidate.prepare(cold, old, stage, index)
    assert not (stage/'candidate.json').exists()
    assert packed.resolve_latest_packed(cold, 'fixture', require_objects=False).manifest_sha256 == old.manifest_sha256


def test_candidate_refuses_to_reuse_existing_stream_stage(tmp_path, monkeypatch):
    _source, cold, stage, old, index = fixture(tmp_path, monkeypatch)
    stage.mkdir(mode=0o700)
    (stage/'intent.json').write_text(json.dumps({'contract': stream_repack.CONTRACT+'.intent',
                                                'dataset': 'fixture', 'old_manifest_sha256': old.manifest_sha256}))
    with pytest.raises(SnapshotError, match='another transaction'):
        candidate.prepare(cold, old, stage, index)
    assert (stage/'intent.json').is_file()


def test_selective_pack_reads_avoid_a_redundant_full_pack_pass(tmp_path, monkeypatch):
    source, cold, reports, stage = (tmp_path/name for name in ('source', 'cold', 'reports', 'candidate'))
    source.mkdir()
    reports.mkdir()
    (source/'a').write_bytes(b'one')
    (source/'b').write_bytes(b'one')
    (source/'c').write_bytes(b'two')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    selective = dedup_native_packs.stream
    @contextmanager
    def read(groups):
        content = []
        for group in groups:
            with zipfile.ZipFile(group['path']) as archive:
                content.extend(archive.read(item['name']) for item in group['members'])
        yield io.BytesIO(b''.join(content))
    def selected(*args, **kwargs):
        return selective(*args, **kwargs, batch_reader=read)
    monkeypatch.setattr(dedup_native_packs, 'stream', selected)
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_kw: pytest.fail('redundant full-pack pass'))
    result = candidate.prepare(cold, old, stage, index)
    assert result['unique_contents'] == 2 and result['files'] == 3


def test_selective_reader_resumes_a_checkpoint_from_the_previous_full_pack_reader(tmp_path, monkeypatch):
    source, cold, reports, stage = (tmp_path/name for name in ('source', 'cold', 'reports', 'candidate'))
    source.mkdir()
    reports.mkdir()
    for name in ('a', 'b', 'c'):
        (source/name).write_bytes(name.encode())
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    selective, consume = dedup_native_packs.stream, stream_repack._PackBuilder.consume
    monkeypatch.setattr(dedup_native_packs, 'stream', lambda *_a, **_kw: set())
    def interrupt(self, source_stream, row):
        value = consume(self, source_stream, row)
        if self.checkpoint.packages:
            raise OSError('old worker interrupted')
        return value
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', interrupt)
    with pytest.raises(OSError, match='old worker interrupted'):
        candidate.prepare(cold, old, stage, index)
    monkeypatch.setattr(stream_repack._PackBuilder, 'consume', consume)
    @contextmanager
    def read(groups):
        data = []
        for group in groups:
            with zipfile.ZipFile(group['path']) as archive:
                data.extend(archive.read(item['name']) for item in group['members'])
        yield io.BytesIO(b''.join(data))
    monkeypatch.setattr(dedup_native_packs, 'stream',
                        lambda *a, **kw: selective(*a, **kw, batch_reader=read))
    monkeypatch.setattr(stream_repack, '_stream_inputs', lambda *_a, **_kw: pytest.fail('unexpected full-pack pass'))
    result = candidate.prepare(cold, old, stage, index)
    assert result['unique_contents'] == 3 and result['files'] == 3
    assert sum(item['stored_file_count'] for item in result['packages']) == 3
