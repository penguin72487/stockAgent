from __future__ import annotations

import hashlib
import io
import json
import os
import random
import sqlite3
import zipfile
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, current_thread
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_metadata import MetadataRows


@pytest.fixture
def failed_metadata_index(tmp_path, monkeypatch):
    from stockagent.data_sync import packed_metadata as metadata
    temporary = tmp_path / 'temporary'
    temporary.mkdir()
    monkeypatch.setattr(metadata, 'METADATA_SCRATCH_ROOT', temporary)
    scratch = temporary / 'stockagent-packed-metadata-abcdefgh'
    scratch.mkdir(mode=0o700)
    index = scratch / 'index.sqlite'
    with sqlite3.connect(index) as db:
        db.execute('CREATE TABLE entries (path BLOB PRIMARY KEY, kind TEXT NOT NULL, '
                   'size INTEGER NOT NULL, object_sha TEXT, bucket INTEGER, data TEXT NOT NULL) WITHOUT ROWID')
        db.execute('INSERT INTO entries VALUES (?,?,?,?,?,?)',
                   (b'raw.bin', 'file', 7, None, None, json.dumps(record('raw.bin'))))
    db.close()
    index.chmod(0o600)
    operation = tmp_path / 'operation'
    attempt = operation / 'attempts/dataset/old-attempt.json'
    attempt.parent.mkdir(parents=True)
    attempt.write_text(json.dumps({'attempt_id': attempt.stem, 'state': 'waiting_retry',
        'error_type': 'OSError', 'error': f"[Errno 24] Too many open files: '{scratch}'",
        'finished_at_utc': '2026-10-18T00:00:00+00:00', 'owner_activity_id': 'old-owner'}))
    (operation / 'plan.json').write_text(json.dumps({'contract':'all_catalog_incremental_migration_v1',
        'plan_sha256':'fixed', 'catalog':[{'dataset':'dataset'}]}))
    (operation / 'progress.json').write_text(json.dumps({'plan_sha256':'fixed',
        'runtime_owner':{'activity_id':'new-owner'}}))
    return metadata, scratch, attempt, tmp_path / 'receipt.json'


def test_explicit_superseded_metadata_index_can_be_planned_and_retired(failed_metadata_index):
    metadata, scratch, attempt, receipt = failed_metadata_index
    plan = metadata.plan_failed_metadata_scratch(scratch, attempt)
    assert scratch.is_dir() and not receipt.exists()
    assert plan['state'] == 'eligible' and plan['index']['sha256']
    result = metadata.retire_failed_metadata_scratch(plan, receipt)
    assert result['state'] == 'retired' and not scratch.exists()
    assert result['allocated_bytes_reclaimed'] == plan['index']['allocated']
    assert result['source_data_files_deleted'] == result['cold_objects_deleted'] == 0
    assert json.loads(receipt.read_text()) == result
    assert attempt.exists()  # All provenance and operation journals survive.


@pytest.mark.parametrize('defect', ['extra_member','shared','symlink','wrong_schema','unknown_origin','same_owner'])
def test_metadata_cleanup_refuses_unknown_shared_redirected_and_live_scope(failed_metadata_index, defect):
    metadata, scratch, attempt, receipt = failed_metadata_index
    index = scratch / 'index.sqlite'
    if defect == 'extra_member':
        (scratch / 'source.parquet').write_bytes(b'must preserve')
    elif defect == 'shared':
        os.link(index, scratch.parent / 'retained.sqlite')
    elif defect == 'symlink':
        index.rename(scratch.parent / 'retained.sqlite')
        index.symlink_to(scratch.parent / 'retained.sqlite')
    elif defect == 'wrong_schema':
        with sqlite3.connect(index) as db:
            db.execute('CREATE TABLE unique_observations (value INTEGER)')
        db.close()
    elif defect == 'unknown_origin':
        value = json.loads(attempt.read_text());value['error'] = 'some unrelated failure'
        attempt.write_text(json.dumps(value))
    else:
        progress = attempt.parent.parent.parent / 'progress.json'
        value = json.loads(progress.read_text());value['runtime_owner']['activity_id'] = 'old-owner'
        progress.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        metadata.plan_failed_metadata_scratch(scratch, attempt)
    assert scratch.exists() and not receipt.exists()


def test_metadata_cleanup_catches_own_open_descriptor_and_changed_plan(failed_metadata_index):
    metadata, scratch, attempt, receipt = failed_metadata_index
    with (scratch / 'index.sqlite').open('rb'):
        with pytest.raises(ValueError, match='process reference'):
            metadata.plan_failed_metadata_scratch(scratch, attempt)
    plan = metadata.plan_failed_metadata_scratch(scratch, attempt)
    (scratch / 'index.sqlite').chmod(0o640)
    with pytest.raises(ValueError):
        metadata.retire_failed_metadata_scratch(plan, receipt)
    assert scratch.is_dir() and not receipt.exists()


def test_metadata_cleanup_retains_quarantine_when_a_reference_appears(failed_metadata_index, monkeypatch):
    from stockagent.data_sync import materialized_cache
    metadata, scratch, attempt, receipt = failed_metadata_index
    plan = metadata.plan_failed_metadata_scratch(scratch, attempt)
    calls = []
    def references(*args, **kwargs):
        calls.append(True)
        return [] if len(calls) == 1 else ['late reference']
    monkeypatch.setattr(materialized_cache, 'process_references_many', references)
    with pytest.raises(ValueError, match='retained'):
        metadata.retire_failed_metadata_scratch(plan, receipt)
    journal = json.loads(receipt.read_text())
    assert journal['state'] == 'quarantine_planned' and journal['allocated_bytes_reclaimed'] == 0
    assert (Path(journal['quarantine']) / 'index.sqlite').exists()


def record(path, **extra):
    return {"path": path, "kind": "file", "size": 7, **extra}


@pytest.mark.parametrize('limit', [1, 100])
def test_census_order_survives_spill_and_releases_early_closed_cursor(limit):
    with MetadataRows(dict, dict, memory_rows=limit) as rows:
        rows.extend([record('z', census_order=0), record('a', census_order=2),
                     record('m', census_order=1)])
        rows.sort(key=lambda row: row['path'])
        assert [row['path'] for row in rows.in_census_order()] == ['z', 'm', 'a']
        assert not rows._cursors
        iterator = rows.in_census_order()
        assert next(iterator)['path'] == 'z'
        assert bool(rows._cursors) is rows.spilled
        iterator.close()
        assert not rows._cursors


@pytest.mark.parametrize('limit', [1, 100])
def test_object_paths_preserve_order_updates_unicode_and_cursor_lifecycle(limit):
    with MetadataRows(dict, dict, memory_rows=limit) as rows:
        paths = ['雪', 'a', 'é', 'surrogate\udcff', 'zero']
        rows.extend(record(path, storage={'kind': 'pack', 'object_sha256': 'one'}) for path in paths)
        rows.sort(key=lambda row: row['path'])
        assert list(rows.object_paths('one')) == sorted(paths)
        assert list(rows.object_paths('missing')) == []
        row = next(iter(rows.object_rows('one')))
        row['storage']['object_sha256'] = 'two'
        rows.update(row)
        assert list(rows.object_paths('two')) == ['a']
        assert list(rows.object_paths('one')) == sorted(set(paths) - {'a'})
        iterator = rows.object_paths('one')
        assert next(iterator) == 'surrogate\udcff'
        assert bool(rows._cursors) is rows.spilled
        iterator.close()
        assert not rows._cursors


def test_spilled_object_paths_use_covering_index_without_decoding_payload(monkeypatch):
    with MetadataRows(dict, dict, memory_rows=1) as rows:
        rows.extend(record(path, payload='large original metadata' * 1024,
                           storage={'kind': 'pack', 'object_sha256': 'one'}) for path in ['b', 'a'])
        def reject_decode(_):
            raise AssertionError('names-only lookup must not decode a data payload')
        monkeypatch.setattr(rows, '_decode', reject_decode)
        assert list(rows.object_paths('one')) == ['a', 'b']
        plan = rows._db.execute('EXPLAIN QUERY PLAN SELECT path FROM entries WHERE object_sha=? ORDER BY path',
                                ('one',)).fetchall()
        assert any('COVERING INDEX object_entries' in row[-1] for row in plan)
        assert not rows._cursors


def test_object_path_cursor_can_be_closed_by_metadata_owner():
    rows = MetadataRows(dict, dict, memory_rows=1)
    rows.extend(record(path, storage={'kind': 'pack', 'object_sha256': 'one'}) for path in ['a', 'b'])
    iterator = rows.object_paths('one')
    assert next(iterator) == 'a'
    rows.close()
    iterator.close()
    assert not rows._cursors and rows._db is None


@pytest.mark.parametrize("limit", [1, 100])
def test_index_preserves_sorted_rows_mutations_groups_and_hints(limit):
    with MetadataRows(dict, dict, memory_rows=limit) as rows:
        rows.extend([record("雪"), record("a"), record("é"), record("zero", size=0)])
        rows.sort(key=lambda row: row["path"])
        assert rows.spilled is (limit == 1)
        assert [row["path"] for row in rows] == ["a", "zero", "é", "雪"]
        for index, row in enumerate(rows):
            row["storage"] = {"kind": "pack", "object_sha256": str(index % 2)}
            rows.update(row, changed_bucket=index % 2)
        assert rows.changed_groups() == [(0, 2, 14), (1, 2, 7)]
        assert [row["path"] for row in rows.changed(1)] == ["zero", "雪"]
        assert [row["path"] for row in rows.object_rows("0")] == ["a", "é"]
        assert rows.take_file_hint("zero")["size"] == 0
        assert rows.take_file_hint("missing") is None
        scratch = Path(rows._scratch.name) if rows.spilled else None
        if scratch:
            assert scratch.parent == Path("/var/tmp") and scratch.stat().st_mode & 0o777 == 0o700
    if scratch:
        assert not scratch.exists()


@pytest.mark.parametrize('limit', [1, 100])
def test_routed_group_bulk_update_preserves_buckets_sizes_objects_and_order(limit):
    with MetadataRows(dict, dict, memory_rows=limit) as rows:
        rows.extend(record(path) for path in ['雪', 'a', 'é', 'surrogate\udcff'])
        rows.sort(key=lambda row: row['path'])
        for index, row in enumerate(rows):
            rows.update(row, changed_bucket=index % 2)
        assert rows.changed_groups() == [(0, 2, 14), (1, 2, 14)]
        # Build both indexes and exercise an open changed-group cursor while
        # the already consumed members receive their new storage mappings.
        assert list(rows.object_paths('new')) == []
        iterator = rows.changed(0)
        first = next(iterator)
        first['size'] = 11  # A captured mutable control can change size.
        first['storage'] = {'kind': 'pack', 'object_sha256': 'new'}
        rows.update_many(iter([first]))
        remaining = list(iterator)
        assert len(remaining) == 1 and remaining[0]['path'] != first['path']
        remaining[0]['storage'] = {'kind': 'pack', 'object_sha256': 'new'}
        rows.update_many(remaining)
        assert rows.changed_groups() == [(0, 2, 18), (1, 2, 14)]
        assert list(rows.object_paths('new')) == sorted([first['path'], remaining[0]['path']])
        assert not rows._cursors
        rows.update_many(iter(()))


def test_spilled_bulk_updates_reject_missing_census_members():
    with MetadataRows(dict, dict, memory_rows=1) as rows:
        rows.extend([record('a'), record('b')])
        with pytest.raises(ValueError, match='every census member'):
            rows.update_many(iter([record('a'), record('unknown')]))
        assert len(rows) == 2  # Never insert a missing source to hide failure.


def test_storage_only_sql_does_not_rewrite_the_routing_index():
    with MetadataRows(dict, dict, memory_rows=1) as rows:
        rows.extend([record('a'), record('b')])
        for row in rows:
            rows.update(row, changed_bucket=0)
        rows.changed_groups()
        trace = []
        rows._db.set_trace_callback(trace.append)
        rows.update_many(rows.changed(0))
        rows.update(next(iter(rows)))
        updates = [sql for sql in trace if sql.startswith('UPDATE')]
        assert len(updates) == 3 and all('bucket=' not in sql for sql in updates)
        candidate = 'UPDATE entries SET kind=?,size=?,object_sha=?,data=? WHERE path=?'
        reference = ('UPDATE entries SET kind=?,size=?,object_sha=?,'
                     'bucket=COALESCE(?,bucket),data=? WHERE path=?')
        parameters = ('file', 7, None, '{}', b'a')
        operations = lambda sql, values: [row[1] for row in rows._db.execute('EXPLAIN ' + sql, values)]
        assert 'IdxDelete' not in operations(candidate, parameters)
        assert 'IdxDelete' in operations(reference, (*parameters[:3], None, *parameters[3:]))


def test_spill_failure_cleans_only_its_disposable_metadata(monkeypatch):
    with pytest.raises(RuntimeError, match="original failure"):
        with MetadataRows(dict, dict, memory_rows=1) as rows:
            rows.extend([record("a"), record("b")])
            scratch = Path(rows._scratch.name)
            raise RuntimeError("original failure")
    assert not scratch.exists()


@pytest.mark.parametrize("limit", [1, 3, 100])
def test_streamed_donor_upsert_is_last_wins_and_keeps_unique_sorted_hints(limit):
    with MetadataRows(dict, dict, memory_rows=limit) as rows:
        for path in ("雪", "a", "é"):
            rows.upsert(record(path, storage={"kind": "pack", "object_sha256": "old"}))
        rows.sort(key=lambda row: row["path"])
        assert len(list(rows.object_rows("old"))) == 3
        rows.upsert(record("a", size=42, storage={"kind": "pack", "object_sha256": "new"}))
        rows.upsert(record("zero", size=0))  # Also exercises spill exactly after a replacement.
        rows.upsert(record("雪", size=99))
        rows.sort(key=lambda row: row["path"])
        assert len(rows) == 4
        assert [row["path"] for row in rows] == ["a", "zero", "é", "雪"]
        assert [row["path"] for row in rows.object_rows("old")] == ["é"]
        assert rows.take_file_hint("a")["size"] == 42
        assert rows.take_file_hint("雪")["size"] == 99


def test_sqlite_index_uses_bounded_page_cache_not_mmap_or_memory_temp():
    with MetadataRows(dict, dict, memory_rows=1) as rows:
        rows.extend([record("a"), record("b")])
        assert rows._db.execute("PRAGMA cache_size").fetchone() == (-8192,)
        assert rows._db.execute("PRAGMA temp_store").fetchone() == (1,)
        with pytest.raises(sqlite3.IntegrityError):
            rows.append(record("a"))


def test_interrupted_iterator_can_close_after_index_without_unraisable_errors():
    with MetadataRows(dict, dict, memory_rows=1) as rows:
        rows.extend([record("a"), record("b")])
        iterator = iter(rows)
        assert next(iterator)["path"] == "a"
    iterator.close()


def test_bootstrap_benchmark_uses_the_real_reader_with_equal_metadata_and_no_public_write():
    from scripts.benchmark_packed_metadata import bootstrap_trial
    retained = bootstrap_trial(20, 'retained', 2)
    bounded = bootstrap_trial(20, 'bounded', 2)
    assert retained['inventory_sha256'] == bounded['inventory_sha256']
    assert retained['metadata_rows'] == bounded['metadata_rows'] == 20
    assert retained['spilled'] is False and bounded['spilled'] is True
    assert retained['public_heads_modified'] == bounded['public_heads_modified'] == 0


def source(root):
    root.mkdir()
    (root / "empty-directory").mkdir()
    for number in range(13):
        (root / f"{number:03}.json").write_text(json.dumps(
            {"code": "0050", "value": -number, "missing": None, "volume_unit": "shares"}))
    (root / "empty.bin").write_bytes(b"")
    (root / "large.bin").write_bytes(b"a" * 48 + b"b" * 48)
    (root / "link").symlink_to("000.json")
    return root


@pytest.mark.parametrize("chunked", [False, True])
def test_spilled_complete_pipeline_matches_ram_and_preserves_all_versions(tmp_path, monkeypatch, chunked):
    original = source(tmp_path / "source")
    opts = {"node_id": "test", "loose_file_threshold_bytes": 80, "pack_buckets": 2}
    if chunked:
        opts["file_chunk_bytes"] = 32
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 1000)
    ram = packed.publish_packed_snapshot(tmp_path / "ram", "test", original, **opts)
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    disk = packed.publish_packed_snapshot(tmp_path / "disk", "test", original, **opts)
    assert ram.manifest["archive"]["inventory"] == disk.manifest["archive"]["inventory"]
    assert ram.manifest["archive"]["objects"] == disk.manifest["archive"]["objects"]

    # The canonical all-file verifier must never request a full Python list.
    def forbid_load(*_):
        pytest.fail("all-file verification must stream its one inventory")
    monkeypatch.setattr(packed, "_load_inventory", forbid_load)
    for cold, release in ((tmp_path / "ram", ram), (tmp_path / "disk", disk)):
        proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True)
        assert proof["independently_reconstructed_files"] == 15
        assert proof["independently_reconstructed_bytes"] == release.manifest["source"]["logical_bytes"]
    monkeypatch.undo()
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    before = disk.manifest_sha256
    noop = packed.publish_packed_snapshot(tmp_path / "disk", "test", original, **opts)
    assert noop.manifest_sha256 == before
    (original / "001.json").write_bytes(b"historical revision")
    (original / "002.json").unlink()
    (original / "new.json").write_bytes(b"new observation")
    (original / "large.bin").write_bytes(b"a" * 48 + b"b" * 48 + b"new")
    update = packed.publish_packed_snapshot(tmp_path / "disk", "test", original, **opts)
    assert update.manifest["archive"]["reused_files"] == 12
    for release in (disk, update):
        proof = packed.verify_packed_snapshot(tmp_path / "disk", release, reconstruct_all=True)
        assert proof["independently_reconstructed_files"] == 15
        restored = packed.fetch_packed_snapshot(tmp_path / "disk", tmp_path / "restored", release)
        if release == disk:
            assert (restored / "002.json").is_file()
            assert json.loads((restored / "001.json").read_text())["value"] == -1
        else:
            assert not (restored / "002.json").exists()
            assert (restored / "001.json").read_bytes() == b"historical revision"


def test_pack_splitting_bounds_central_directory_without_losing_noop_reuse(tmp_path, monkeypatch):
    original = source(tmp_path / "source")
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    monkeypatch.setattr(packed, "MAX_PACK_MEMBERS", 3)
    release = packed.publish_packed_snapshot(tmp_path / "cold", "test", original,
                                           node_id="test", pack_buckets=1)
    objects = release.manifest["archive"]["objects"]
    assert len(objects) == 5 and all(item["file_count"] <= 3 for item in objects)
    proof = packed.verify_packed_snapshot(tmp_path / "cold", release, reconstruct_all=True)
    assert proof["independently_reconstructed_files"] == 15
    noop = packed.publish_packed_snapshot(tmp_path / "cold", "test", original, node_id="test", pack_buckets=1)
    assert noop.manifest_sha256 == release.manifest_sha256


def test_all_file_verifier_fuses_crc_and_sha_but_rejects_wrong_original_hash(tmp_path, monkeypatch):
    original = source(tmp_path / "source")
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    cold = tmp_path / "cold"
    release = packed.publish_packed_snapshot(cold, "test", original, node_id="test", pack_buckets=1)
    monkeypatch.setattr(zipfile.ZipFile, "testzip", lambda *_: pytest.fail("do not decompress every member twice"))
    assert packed.verify_packed_snapshot(cold, release, reconstruct_all=True)["independently_reconstructed_files"] == 15
    iterate = packed._iter_inventory
    def wrong_hash(*args):
        for row in iterate(*args):
            if row["path"] == "001.json":
                row["sha256"] = "0" * 64
            yield row
    monkeypatch.setattr(packed, "_iter_inventory", wrong_hash)
    with pytest.raises(SnapshotError, match="exact cold reconstruction failed"):
        packed.verify_packed_snapshot(cold, release, reconstruct_all=True)


def test_all_file_and_explicit_path_modes_are_not_ambiguous(tmp_path):
    original = source(tmp_path / "source")
    release = packed.publish_packed_snapshot(tmp_path / "cold", "test", original, node_id="test")
    with pytest.raises(SnapshotError, match="choose either"):
        packed.verify_packed_snapshot(tmp_path / "cold", release, reconstruct_all=True, reconstruct_paths=[])


def test_first_small_files_hash_only_while_packing_and_report_real_progress(tmp_path, monkeypatch):
    original = tmp_path / "source"
    original.mkdir()
    for number in range(4):
        (original / f"{number}.json").write_bytes(bytes([number]) * number)
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    def redundant_read(*args, **kwargs):
        pytest.fail("new small files must not be read before their one-pass pack")
    monkeypatch.setattr(packed, "_hash_source_entry", redundant_read)
    progress = []
    cold = tmp_path / "cold"
    release = packed.publish_packed_snapshot(cold, "test", original, node_id="test", progress=progress.append)
    routed = [row for row in progress if row["stage"] == "routing_and_hashing"][-1]
    assert routed["files_routed"] == 4 and routed["files_hashed"] == 0
    assert progress[-1]["files_hashed"] == 4
    assert progress[-1]["logical_bytes_processed"] == 6
    assert packed.verify_packed_snapshot(cold, release, reconstruct_all=True)["independently_reconstructed_files"] == 4


@pytest.mark.parametrize("capacity", [1, 17, 4096])
def test_seekable_pack_buffer_matches_binary_seek_and_write(capacity):
    expected, actual = io.BytesIO(), io.BytesIO()
    buffer = packed._SeekablePackBuffer(actual, capacity=capacity)
    rng = random.Random(1891)
    for _ in range(250):
        payload = rng.randbytes(rng.randrange(0, 100))
        assert buffer.write(payload) == expected.write(payload)
        assert len(buffer._buffer) <= capacity
        target = rng.randrange(0, len(expected.getvalue()) + 20)
        assert buffer.seek(target) == expected.seek(target)
        delta = -rng.randrange(0, target + 1)
        assert buffer.seek(delta, os.SEEK_CUR) == expected.seek(delta, os.SEEK_CUR)
    buffer.flush()
    assert actual.getvalue() == expected.getvalue()
    assert buffer.tell() == expected.tell()
    with pytest.raises(ValueError, match="negative"):
        buffer.seek(-1)
    with pytest.raises(io.UnsupportedOperation):
        buffer.seek(0, os.SEEK_END)


def test_pack_buffer_coalesces_backpatches_and_remains_bounded():
    class Measured(io.BytesIO):
        writes = 0
        def write(self, block):
            self.writes += 1
            return super().write(block)
    output = Measured()
    buffer = packed._SeekablePackBuffer(output, capacity=4096)
    for index in range(2048):
        position = buffer.tell()
        buffer.write(b"x" * 64)
        buffer.seek(position)
        buffer.write(index.to_bytes(8, "big"))
        buffer.seek(position + 64)
        assert len(buffer._buffer) <= 4096
    buffer.flush()
    assert output.writes == 32  # Not two writes/backward seeks per member.
    assert len(output.getvalue()) == 2048 * 64


@pytest.mark.parametrize("capacity", [1, 137, 131072])
def test_pack_buffer_retains_exact_legacy_zip_bytes_and_member_crc(tmp_path, monkeypatch, capacity):
    original = source(tmp_path / "source")
    (original / "大檔.parquet").write_bytes(random.Random(42).randbytes(150000))
    entries, _ = packed._collect_entries(original)
    entries = [entry for entry in entries if entry.kind == "file"]
    legacy, candidate = tmp_path / "legacy.zip", tmp_path / "candidate.zip"
    # The previous implementation used ZipFile's ordinary seekable file.
    with zipfile.ZipFile(legacy, "x", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=packed.DEFAULT_COMPRESSION_LEVEL, allowZip64=True) as archive:
        for entry in entries:
            with archive.open(packed._zip_info(entry), "w", force_zip64=True) as target:
                target.write((original / entry.path).read_bytes())
    native = packed._SeekablePackBuffer
    monkeypatch.setattr(packed, "_SeekablePackBuffer", lambda stream: native(stream, capacity=capacity))
    packed._write_pack(original, entries, candidate, compression_level=packed.DEFAULT_COMPRESSION_LEVEL)
    assert candidate.read_bytes() == legacy.read_bytes()
    with zipfile.ZipFile(candidate) as archive:
        assert archive.testzip() is None
        for entry in entries:
            raw = (original / entry.path).read_bytes()
            assert archive.read(entry.path) == raw
            assert entry.sha256 == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize('operation', ['pack', 'hash'])
def test_descriptor_source_read_uses_one_path_stat_and_preserves_full_hash(tmp_path, monkeypatch, operation):
    root = tmp_path / 'source'
    root.mkdir()
    path = root / '原值.json'
    raw = b'{"value":-7,"missing":null,"unit":"shares"}'
    path.write_bytes(raw)
    entries, _ = packed._collect_entries(root)
    entry = next(row for row in entries if row.kind == 'file')
    observed, original = [], Path.lstat
    def counted(member, *args, **kwargs):
        if member == path:
            observed.append(member)
        return original(member, *args, **kwargs)
    monkeypatch.setattr(Path, 'lstat', counted)
    if operation == 'hash':
        assert packed._hash_source_entry(root, entry) == hashlib.sha256(raw).hexdigest()
    else:
        output = tmp_path / 'packed.zip'
        packed._write_pack(root, [entry], output, compression_level=6)
        with zipfile.ZipFile(output) as archive:
            assert archive.read(entry.path) == raw and archive.testzip() is None
        assert entry.sha256 == hashlib.sha256(raw).hexdigest()
    assert observed == [path]


@pytest.mark.parametrize('fault', ['missing', 'symlink', 'fifo', 'directory', 'changed'])
def test_descriptor_source_read_rejects_post_census_substitution_without_hanging(tmp_path, fault):
    root = tmp_path / 'source'
    root.mkdir()
    path = root / 'original'
    path.write_bytes(b'original')
    entries, _ = packed._collect_entries(root)
    entry = next(row for row in entries if row.kind == 'file')
    path.unlink()
    if fault == 'symlink':
        outside = tmp_path / 'outside'
        outside.write_bytes(b'original')
        path.symlink_to(outside)
    elif fault == 'fifo':
        os.mkfifo(path)
    elif fault == 'directory':
        path.mkdir()
    elif fault == 'changed':
        path.write_bytes(b'changed')
    with pytest.raises(packed.SnapshotError, match='source (entry|file)'):
        with packed._open_source_entry(root, entry):
            pytest.fail('substituted source must not be yielded')


@pytest.mark.parametrize('fault', ['in_place', 'atomic_replace'])
def test_descriptor_source_read_rejects_mutation_after_open(tmp_path, fault):
    root = tmp_path / 'source'
    root.mkdir()
    path = root / 'original'
    path.write_bytes(b'original')
    entries, _ = packed._collect_entries(root)
    entry = next(row for row in entries if row.kind == 'file')
    with pytest.raises(packed.SnapshotError, match='source file changed'):
        with packed._open_source_entry(root, entry) as stream:
            assert stream.read() == b'original'
            if fault == 'in_place':
                path.write_bytes(b'changed!')
            else:
                replacement = root / 'temporary'
                replacement.write_bytes(b'original')
                replacement.replace(path)


@pytest.mark.parametrize("result", [0, None, "error"])
def test_pack_buffer_propagates_failed_output(result):
    class Failed(io.BytesIO):
        def write(self, block):
            if result == "error":
                raise OSError("deliberate storage error")
            return result
    buffer = packed._SeekablePackBuffer(Failed(), capacity=4)
    with pytest.raises(OSError):
        buffer.write(b"too many bytes")


@pytest.mark.parametrize("maximum_write", [1, 7])
def test_pack_buffer_drains_positive_partial_writes_without_losing_backpatches(maximum_write):
    class PartialOutput(io.BytesIO):
        def write(self, block):
            return super().write(block[:maximum_write])
    output, expected = PartialOutput(), io.BytesIO()
    buffer = packed._SeekablePackBuffer(output, capacity=17)
    for stream in (buffer, expected):
        stream.write(b'original stored member' * 4)
        stream.seek(4)
        stream.write(b'patched ZIP header')
        stream.seek(100)
        stream.write(b'final member')
    buffer.flush()
    assert output.getvalue() == expected.getvalue()
    assert buffer.tell() == expected.tell()


def test_pack_buffer_failure_does_not_advance_an_accepted_head(tmp_path, monkeypatch):
    original = source(tmp_path / "source")
    cold = tmp_path / "cold"
    accepted = packed.publish_packed_snapshot(cold, "test", original, node_id="test")
    (original / "new.json").write_bytes(b"new observation")
    def failed_flush(_self):
        raise OSError("deliberate buffered storage failure")
    monkeypatch.setattr(packed._SeekablePackBuffer, "flush", failed_flush)
    with pytest.raises(OSError, match="buffered storage failure"):
        packed.publish_packed_snapshot(cold, "test", original, node_id="test")
    assert packed.resolve_latest_packed(cold, "test").manifest_sha256 == accepted.manifest_sha256
    assert packed.verify_packed_snapshot(cold, accepted, reconstruct_all=True)["independently_reconstructed_files"] == 15


@pytest.mark.parametrize("fail", [False, True])
def test_parallel_file_fsync_retains_every_barrier_and_blocks_ready_until_completion(tmp_path, monkeypatch, fail):
    original = source(tmp_path / "source")
    cold, hot = tmp_path / "cold", tmp_path / "hot"
    release = packed.publish_packed_snapshot(cold, "test", original, node_id="test")
    monkeypatch.setattr(packed, "METADATA_MEMORY_ROWS", 2)
    monkeypatch.setattr(packed, "MIN_PARALLEL_FSYNC_FILES", 1)
    monkeypatch.setattr(packed, "MATERIALIZE_FSYNC_WORKERS", 4)
    entered, proceed = Event(), Event()
    guard = Lock()
    paths = []
    native = os.fsync
    def fsync(descriptor):
        if current_thread().name.startswith("packed-fsync"):
            with guard:
                paths.append(os.readlink(f"/proc/self/fd/{descriptor}"))
            entered.set()
            assert proceed.wait(10)
            if fail:
                raise OSError("deliberate durable-file failure")
        native(descriptor)
    monkeypatch.setattr(packed.os, "fsync", fsync)
    target = hot / "test" / release.manifest["snapshot_id"]
    ready = hot / "test" / ("." + release.manifest["snapshot_id"] + ".READY.json")
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(packed.fetch_packed_snapshot, cold, hot, release)
        try:
            assert entered.wait(10)
            assert not ready.exists() and not target.exists()
        finally:
            proceed.set()
        if fail:
            with pytest.raises(OSError, match="deliberate durable-file failure"):
                future.result(timeout=20)
            assert not ready.exists() and not target.exists()
        else:
            assert future.result(timeout=20) == target and ready.is_file()
            assert len(paths) == len(set(paths)) == 15  # Every original, including empty and chunk files.
