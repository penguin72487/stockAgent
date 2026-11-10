import json
import os

import pytest

from stockagent.data_sync import dedup_candidate_reader as reader, dedup_stream_candidate as candidate
from stockagent.data_sync import packed_dedup_plan as plan, packed_snapshots as packed, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError


def fixture(tmp_path, monkeypatch):
    source, cold, reports, stage, scratch, dest = (tmp_path/name for name in
        ('source', 'cold', 'reports', 'candidate', 'scratch', 'dest'))
    source.mkdir()
    reports.mkdir()
    scratch.mkdir()
    dest.mkdir()
    (source/'a').write_bytes(b'same')
    (source/'b').write_bytes(b'same')
    os.chmod(source/'a', 0o600)
    os.chmod(source/'b', 0o644)
    (source/'link').symlink_to('a')
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    candidate.prepare(cold, old, stage, index)
    return source, cold, stage, scratch, dest, old


def test_reader_extracts_aliases_as_independent_files_with_original_modes(tmp_path, monkeypatch):
    source, cold, stage, scratch, dest, old = fixture(tmp_path, monkeypatch)
    with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch) as data:
        assert data.copy_file('a', dest/'a')['independent_file']
        assert data.copy_file('b', dest/'b')['independent_file']
        with pytest.raises(SnapshotError, match='not a regular file'):
            data.copy_file('link', dest/'link')
        with pytest.raises(SnapshotError, match='exists'):
            data.copy_file('a', dest/'a')
    assert (dest/'a').read_bytes() == (dest/'b').read_bytes() == b'same'
    assert (dest/'a').stat().st_ino != (dest/'b').stat().st_ino
    assert (dest/'a').stat().st_mode & 0o777 == 0o600
    assert (dest/'b').stat().st_mode & 0o777 == 0o644
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256
    assert not list(scratch.iterdir())


def test_reader_rejects_changed_sealed_pack_without_touching_destination(tmp_path, monkeypatch):
    _source, _cold, stage, scratch, dest, old = fixture(tmp_path, monkeypatch)
    record = json.loads((stage/'candidate.json').read_text())
    package = stage/record['packages'][0]['name']
    package.write_bytes(b'x'*package.stat().st_size)
    with pytest.raises(SnapshotError, match='generation changed'):
        with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch):
            pass
    assert not list(dest.iterdir())


def test_restore_all_preserves_symlinks_modes_and_independent_aliases(tmp_path, monkeypatch):
    source, cold, stage, scratch, dest, old = fixture(tmp_path, monkeypatch)
    with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch) as data:
        result = data.restore_all(dest/'restored')
    restored = dest/'restored'
    assert result['files'] == 2 and result['logical_bytes'] == 8 and result['independent_files']
    assert not result['D_published'] and not result['source_retirement_authorized']
    assert (restored/'a').read_bytes() == (restored/'b').read_bytes() == b'same'
    assert (restored/'a').stat().st_ino != (restored/'b').stat().st_ino
    assert (restored/'a').stat().st_mode & 0o777 == 0o600
    assert (restored/'b').stat().st_mode & 0o777 == 0o644
    assert os.readlink(restored/'link') == 'a'
    (restored/'a').write_bytes(b'changed')
    assert (restored/'b').read_bytes() == b'same' and (source/'a').read_bytes() == b'same'
    assert packed.resolve_latest_packed(cold, 'fixture').manifest_sha256 == old.manifest_sha256


def test_restore_does_not_replace_an_existing_destination(tmp_path, monkeypatch):
    _source, _cold, stage, scratch, dest, old = fixture(tmp_path, monkeypatch)
    existing = dest/'existing'
    existing.mkdir()
    (existing/'keep').write_text('keep')
    with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch) as data:
        with pytest.raises(SnapshotError, match='destination exists'):
            data.restore_all(existing)
    assert (existing/'keep').read_text() == 'keep'


def test_restore_multiple_packs_and_nested_directories(tmp_path, monkeypatch):
    source = tmp_path/'source'
    (source/'one').mkdir(parents=True)
    (source/'two').mkdir()
    os.chmod(source/'one', 0o750)
    (source/'one/a').write_bytes(b'first')
    (source/'two/b').write_bytes(b'first')
    (source/'two/c').write_bytes(b'second')
    (source/'two/link').symlink_to('../one/a')
    cold, reports, stage, scratch, dest = (tmp_path/name for name in
        ('cold', 'reports', 'candidate', 'scratch', 'dest'))
    reports.mkdir()
    scratch.mkdir()
    dest.mkdir()
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    monkeypatch.setattr(stream_repack, 'TARGET_BYTES', 1)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    result = candidate.prepare(cold, old, stage, index)
    assert len(result['packages']) == 2
    with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch) as data:
        restored = data.restore_all(dest/'restored')
    root = dest/'restored'
    assert restored['files'] == 3 and (root/'two/c').read_bytes() == b'second'
    assert (root/'one/a').read_bytes() == (root/'two/b').read_bytes() == b'first'
    assert (root/'one').stat().st_mode & 0o777 == 0o750
    assert os.readlink(root/'two/link') == '../one/a'


def test_restore_keeps_unreadable_representative_mode(tmp_path, monkeypatch):
    source, cold, reports, stage, scratch, dest = (tmp_path/name for name in
        ('source', 'cold', 'reports', 'candidate', 'scratch', 'dest'))
    for path in (source, reports, scratch, dest):
        path.mkdir()
    (source/'a').write_bytes(b'same')
    (source/'b').write_bytes(b'same')
    os.chmod(source/'a', 0)
    monkeypatch.setattr(packed, '_notify_packed_publication', lambda *_a, **_kw: None)
    old = packed.publish_packed_snapshot(cold, 'fixture', source, node_id='test', pack_buckets=4)
    index = plan.build_plan(cold, 'fixture', old.manifest_sha256, reports)['index_path']
    candidate.prepare(cold, old, stage, index)
    with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch) as data:
        data.restore_all(dest/'restored')
    assert (dest/'restored/a').stat().st_mode & 0o777 == 0
    assert (dest/'restored/b').read_bytes() == b'same'


def test_reader_rejects_wrong_identity_and_source_overlap(tmp_path, monkeypatch):
    _source, _cold, stage, scratch, _dest, old = fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match='record differs'):
        with reader.CandidateReader(stage, 'fixture', 'a'*64, scratch=scratch):
            pass
    with pytest.raises(SnapshotError, match='scratch'):
        with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=stage):
            pass


def test_reader_rejects_a_rewritten_descriptor_with_a_valid_self_hash(tmp_path, monkeypatch):
    _source, _cold, stage, scratch, _dest, old = fixture(tmp_path, monkeypatch)
    path = stage/'candidate.json'
    record = json.loads(path.read_text())
    record['packages'][0]['sha256'] = 'a'*64
    record['identity_sha256'] = stream_repack._identity(record)
    path.write_text(json.dumps(record))
    with pytest.raises(SnapshotError, match='package descriptor differs'):
        with reader.CandidateReader(stage, 'fixture', old.manifest_sha256, scratch=scratch):
            pass
