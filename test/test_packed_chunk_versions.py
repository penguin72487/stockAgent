"""Exact incremental versions shared by every file-backed provider, not news-only."""
from __future__ import annotations

import hashlib
import pytest

import stockagent.data_sync.packed_snapshots as packed

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    _load_inventory, _validate_inventory, fetch_packed_snapshot, fetch_packed_subtree,
    publish_packed_snapshot, resolve_packed_snapshot_id, stage_packed_root_files,
    verify_packed_snapshot,
)


def publish(root, source, dataset="provider"):
    return publish_packed_snapshot(root, dataset, source, node_id="test",
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2)


def objects(release):
    return {item["sha256"] for item in release.manifest["archive"]["objects"]}


@pytest.mark.parametrize('pipeline', [False, True])
def test_native_existing_chunk_reuse_batches_only_full_hash_proofs_and_preserves_versions(tmp_path, monkeypatch, pipeline):
    from stockagent.data_sync import windows_cold_io as native
    source, cold = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    blocks = [bytes([index]) * 16 for index in range(18)]
    original = b''.join([blocks[0], blocks[0], *blocks[1:]])
    (source / 'large.bin').write_bytes(original)
    (source / 'receipt.json').write_bytes(b'{}')
    publish(cold, source, dataset='seed')
    calls, authorities = [], []
    def hash_batch(paths):
        calls.append(list(paths))
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(native, 'hash_many', hash_batch)
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_REUSE_BYTES', 0)
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', authorities.append)
    candidate = publish_packed_snapshot(cold, 'candidate', source, node_id='test',
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2,
        d_primary_native_chunk_reuse=True, d_primary_native_chunk_reuse_pipeline=pipeline)
    reference = publish(cold, source, dataset='reference')
    assert authorities == [cold]
    assert [len(batch) for batch in calls] == [16, 2]
    assert len({path for batch in calls for path in batch}) == 18
    assert candidate.manifest['archive']['inventory']['sha256'] == reference.manifest['archive']['inventory']['sha256']
    proof = verify_packed_snapshot(cold, candidate, reconstruct_all=True)
    assert proof['independently_reconstructed_files'] == 2
    assert proof['independently_reconstructed_bytes'] == len(original) + 2
    (source / 'large.bin').write_bytes(original + b'new extent')
    revised = publish_packed_snapshot(cold, 'candidate', source, node_id='test',
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2,
        d_primary_native_chunk_reuse=True, d_primary_native_chunk_reuse_pipeline=pipeline)
    for number, (selected, expected) in enumerate(((candidate, original), (revised, original + b'new extent'))):
        assert verify_packed_snapshot(cold, selected, reconstruct_all=True)['independently_reconstructed_files'] == 2
        restored = fetch_packed_snapshot(cold, tmp_path / f'hot-{number}', selected)
        assert (restored / 'large.bin').read_bytes() == expected
        assert (restored / 'receipt.json').read_bytes() == b'{}'


@pytest.mark.parametrize('fault', ['wrong_sha', 'missing_proof', 'extra_proof', 'changed_chunk', 'changed_source', 'batch_error'])
@pytest.mark.parametrize('pipeline', [False, True])
def test_native_existing_chunk_reuse_rejects_damage_without_committing_head(tmp_path, monkeypatch, fault, pipeline):
    from stockagent.data_sync import windows_cold_io as native
    source, cold = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    path = source / 'large.bin'
    original = b'a' * 16 + b'b' * 16
    path.write_bytes(original)
    seed = publish(cold, source, dataset='seed')
    head = cold / 'heads/seed/test.json'
    before = head.read_bytes()
    def damaged(paths):
        result = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        if fault == 'wrong_sha':result[paths[0]] = '0' * 64
        elif fault == 'missing_proof':result.pop(paths[0])
        elif fault == 'extra_proof':result[tmp_path / 'unrequested'] = '0' * 64
        elif fault == 'changed_chunk':paths[0].write_bytes(b'corrupt'.ljust(16, b'!'))
        elif fault == 'changed_source':path.write_bytes(original + b'changed')
        else:raise SnapshotError('native batch rejected')
        return result
    monkeypatch.setattr(native, 'hash_many', damaged)
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_REUSE_BYTES', 0)
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda root: None)
    with pytest.raises(SnapshotError):
        publish_packed_snapshot(cold, 'candidate', source, node_id='test',
            loose_file_threshold_bytes=16, file_chunk_bytes=16,
            d_primary_native_chunk_reuse=True, d_primary_native_chunk_reuse_pipeline=pipeline)
    assert not list((cold / 'heads/candidate').glob('*.json'))
    assert head.read_bytes() == before
    assert resolve_packed_snapshot_id(cold, 'seed', seed.manifest['snapshot_id']).manifest_sha256 == seed.manifest_sha256


@pytest.mark.parametrize('enabled', [True, 1, 'true'])
def test_native_existing_chunk_reuse_requires_explicit_policy_and_original_authority(tmp_path, enabled):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'large.bin').write_bytes(b'exact source')
    with pytest.raises(SnapshotError, match='canonical D|explicit boolean'):
        publish_packed_snapshot(tmp_path / 'cold', 'candidate', source,
                                d_primary_native_chunk_reuse=enabled)


def test_native_existing_chunk_reuse_keeps_small_tail_in_the_original_reader(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io as native
    source, cold = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    (source / 'large.bin').write_bytes(b'a' * 16 + b'b' * 16)
    publish(cold, source, dataset='seed')
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda root: None)
    monkeypatch.setattr(native, 'hash_many', lambda *_: pytest.fail('native startup for a small tail'))
    candidate = publish_packed_snapshot(cold, 'candidate', source, node_id='test',
        loose_file_threshold_bytes=16, file_chunk_bytes=16, d_primary_native_chunk_reuse=True)
    assert verify_packed_snapshot(cold, candidate, reconstruct_all=True)['independently_reconstructed_bytes'] == 32


def test_native_chunk_pipeline_batches_across_files_and_restores_exact_versions(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io as native
    source, cold = tmp_path / 'source', tmp_path / 'cold'
    source.mkdir()
    for index in range(20):
        (source / f'{index:02}.bin').write_bytes(bytes([index * 2]) * 16 + bytes([index * 2 + 1]) * 16)
    # Repeated extents across different originals need one full cold proof,
    # but remain repeated in their own original SHA/length and inventory.
    (source / 'repeated.bin').write_bytes((source / '00.bin').read_bytes())
    publish(cold, source, dataset='seed')
    calls = []
    def hashing(paths):
        calls.append(list(paths))
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(native, 'hash_many', hashing)
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_REUSE_BYTES', 0)
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda root: None)
    options = dict(node_id='test', loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2,
        d_primary_native_chunk_reuse=True, d_primary_native_chunk_reuse_pipeline=True)
    first = publish_packed_snapshot(cold, 'candidate', source, **options)
    assert [len(batch) for batch in calls] == [16, 16, 8]
    assert len({path for batch in calls for path in batch}) == 40
    reference = publish(cold, source, dataset='reference')
    assert first.manifest['archive']['inventory']['sha256'] == reference.manifest['archive']['inventory']['sha256']
    old_bytes = (source / '00.bin').read_bytes()
    (source / '00.bin').write_bytes(b'x' * 16 + b'y' * 16)
    (source / '01.bin').unlink()
    second = publish_packed_snapshot(cold, 'candidate', source, **options)
    for number, release in enumerate((first, second)):
        proof = verify_packed_snapshot(cold, release, reconstruct_all=True)
        assert proof['independently_reconstructed_files'] == 21 - number
        restored = fetch_packed_snapshot(cold, tmp_path / f'restored-{number}', release)
        assert (restored / '00.bin').read_bytes() == (old_bytes if not number else b'x' * 16 + b'y' * 16)
        assert (restored / '01.bin').exists() is (not number)


def test_native_chunk_pipeline_overlaps_source_work_but_never_admits_early(tmp_path, monkeypatch):
    from threading import Event
    from stockagent.data_sync import windows_cold_io as native
    entered, release = Event(), Event()
    def hashing(paths):
        entered.set()
        assert release.wait(5), 'bounded test reader was not released'
        return {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    monkeypatch.setattr(native, 'hash_many', hashing)
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_REUSE_BYTES', 0)
    created = {}
    with packed._PipelinedChunkReuse(created) as pipeline:
        try:
            for index in range(17):
                path = tmp_path / str(index)
                path.write_bytes(str(index).encode())
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                pipeline.add(digest, path, path.lstat(), {'sha256': digest})
            assert entered.wait(5)
            assert created == {}
            assert len(pipeline.inflight[0]) == 16 and len(pipeline.pending) == 1
        finally:
            release.set()
    assert len(created) == 17 and pipeline.inflight is None and pipeline.pending == {}


def test_native_chunk_pipeline_drains_on_source_failure_without_admitting_pending_proofs(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io as native
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_REUSE_BYTES', 0)
    monkeypatch.setattr(native, 'hash_many', lambda paths: {
        path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
    created = {}
    with pytest.raises(SnapshotError, match='source rejected'):
        with packed._PipelinedChunkReuse(created) as pipeline:
            for index in range(16):
                path = tmp_path / str(index)
                path.write_bytes(str(index).encode())
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                pipeline.add(digest, path, path.lstat(), {'sha256': digest})
            raise SnapshotError('source rejected')
    assert created == {}
    assert all(not thread.is_alive() for thread in pipeline.executor._threads)


@pytest.mark.parametrize('enabled,native_reuse', [(1, True), ('true', True), (True, False)])
def test_native_chunk_pipeline_requires_explicit_reuse_policy(tmp_path, enabled, native_reuse):
    source = tmp_path / 'source'
    source.mkdir()
    with pytest.raises(SnapshotError, match='explicit boolean|requires explicit native chunk reuse'):
        publish_packed_snapshot(tmp_path / 'cold', 'candidate', source,
            d_primary_native_chunk_reuse=native_reuse, d_primary_native_chunk_reuse_pipeline=enabled)


@pytest.mark.parametrize('changed', [False, True])
def test_native_private_chunk_reads_reuse_exact_local_only_authority_guard(monkeypatch, changed):
    from pathlib import Path
    from stockagent.data_sync import dataset_migration as migration
    primary = Path('/srv/stockagent-packed')
    private = primary / '.local-state/private-history'
    seen = []
    def admitted(root, policy, *, initialize):
        seen.append((root, policy, initialize))
        return primary / 'foreign' if changed else private
    monkeypatch.setattr(migration, 'private_history_root', admitted)
    if changed:
        with pytest.raises(SnapshotError, match='private pack read authority'):
            packed._check_native_pack_read_authority(private)
    else:
        packed._check_native_pack_read_authority(private)
    assert seen == [(primary, {'private_history': {'enabled': True,
        'relative_root': '.local-state/private-history', 'shared_export': False,
        'nas_export': False, 'source_retirement': False}}, False)]


def test_changed_large_file_only_installs_new_extents_and_each_version_restores(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "market.parquet"
    initial = b"a" * 16 + b"b" * 16 + b"c" * 16
    path.write_bytes(initial)
    (source / "receipt.json").write_text("{}\n")
    cold = tmp_path / "cold"
    first = publish(cold, source)
    assert first.manifest["schema_version"] == 2
    assert len(objects(first)) == 4
    assert publish(cold, source).manifest_sha256 == first.manifest_sha256
    path.write_bytes(b"a" * 16 + b"x" * 16 + b"c" * 16 + b"d" * 7)
    second = publish(cold, source)
    assert len(objects(second) - objects(first)) == 2
    assert second.manifest["archive"]["new_stored_bytes"] == (
        23 + second.manifest["archive"]["inventory"]["bytes"])
    for release, expected in ((first, initial), (second, path.read_bytes())):
        fixed = resolve_packed_snapshot_id(cold, "provider", release.manifest["snapshot_id"])
        proof = verify_packed_snapshot(cold, fixed, reconstruct_paths=["market.parquet", "receipt.json"])
        assert proof["independently_reconstructed_files"] == 2
        restored = fetch_packed_snapshot(cold, tmp_path / "hot", fixed)
        assert (restored / "market.parquet").read_bytes() == expected


def test_chunked_baseline_and_update_do_not_pre_read_entire_payload(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "large.bin"
    path.write_bytes(b"a" * 32 + b"b" * 32)
    monkeypatch.setattr(packed, "_hash_source_entry", lambda *a, **kw: pytest.fail("redundant full source pass"))
    first = publish(tmp_path / "cold", source)
    assert first.manifest["archive"]["changed_files"] == 1
    path.write_bytes(b"a" * 32 + b"c" * 32)
    second = publish(tmp_path / "cold", source)
    assert second.manifest["archive"]["changed_files"] == 1
    assert publish(tmp_path / "cold", source).manifest_sha256 == second.manifest_sha256
    proof = verify_packed_snapshot(tmp_path / "cold", second, reconstruct_paths=["large.bin"])
    assert proof["independently_reconstructed_files"] == 1


def test_recovery_progress_separates_objects_from_complete_original_files(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'large.bin').write_bytes(b'a' * 16 + b'b' * 16 + b'c' * 16)
    (source / 'small.json').write_bytes(b'{}')
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    seen = []
    result = verify_packed_snapshot(cold, release,
        reconstruct_paths=['large.bin', 'small.json'], progress=seen.append)
    objects = [row for row in seen if row['stage'] == 'verify-cold-objects']
    originals = [row for row in seen if row['stage'] == 'reconstruct-original-files']
    assert objects[-1]['completed_items'] == objects[-1]['total_items']
    assert originals[0]['completed_items'] == 1
    assert originals[0]['completed_bytes'] == 2
    assert len(originals) >= 4
    assert originals[-1]['completed_items'] == originals[-1]['total_items'] == 2
    assert originals[-1]['completed_bytes'] == originals[-1]['total_bytes'] == 50
    assert result['independently_reconstructed_files'] == 2


@pytest.mark.parametrize('memory_rows', [1, 10000])
def test_full_recovery_fuses_chunk_hash_and_original_hash_without_pre_read(
        tmp_path, monkeypatch, memory_rows):
    from collections import Counter
    from pathlib import Path
    monkeypatch.setattr(packed, 'METADATA_MEMORY_ROWS', memory_rows)
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'first.bin').write_bytes(b'a' * 16 + b'b' * 16 + b'c' * 16)
    (source / 'second.bin').write_bytes(b'b' * 32)
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    blobs = {cold / item['relpath'] for item in release.manifest['archive']['objects']}
    native_open, reads = Path.open, Counter()
    def observed_open(path, mode='r', *args, **options):
        if path in blobs and mode == 'rb':
            reads[path] += 1
        return native_open(path, mode, *args, **options)
    monkeypatch.setattr(Path, 'open', observed_open)
    observations = []
    result = verify_packed_snapshot(cold, release, reconstruct_all=True, progress=observations.append)
    assert sum(reads.values()) == 5  # Each original extent; no redundant 3-object hash pass.
    assert result['independently_reconstructed_files'] == 2
    assert result['independently_reconstructed_bytes'] == 80
    assert result['verified_object_bytes'] == 48
    assert result['chunk_object_hash_fusion']['objects'] == 3
    assert result['chunk_object_hash_fusion']['redundant_cold_read_bytes_avoided'] == 48
    original_events = [row for row in observations if row['stage'] == 'reconstruct-original-files']
    assert original_events[0]['completed_items'] == 0
    assert observations[-1]['completed_items'] == observations[-1]['total_items'] == 3
    assert original_events[-1]['completed_items'] == original_events[-1]['total_items'] == 2
    assert original_events[-1]['verified_objects'] == 3


def test_fused_recovery_keeps_direct_blob_proof_when_chunk_and_v1_blob_share_object(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'legacy.bin').write_bytes(b'a' * 16)
    cold = tmp_path / 'cold'
    publish_packed_snapshot(cold, 'provider', source, node_id='test', loose_file_threshold_bytes=16)
    (source / 'chunked.bin').write_bytes(b'a' * 16 + b'b' * 16)
    release = publish(cold, source)
    result = verify_packed_snapshot(cold, release, reconstruct_all=True)
    assert result['independently_reconstructed_files'] == 2
    assert result['independently_reconstructed_bytes'] == 48
    assert result['verified_object_bytes'] == 32
    assert result['chunk_object_hash_fusion']['objects'] == 1
    assert result['chunk_object_hash_fusion']['redundant_cold_read_bytes_avoided'] == 16


@pytest.mark.parametrize('legacy_blob', [False, True])
def test_fused_native_verifier_hashes_only_non_deferred_blobs(tmp_path, monkeypatch, legacy_blob):
    from contextlib import contextmanager
    from stockagent.data_sync import windows_cold_io as native
    from stockagent.data_sync.desync_snapshots import sha256_file

    source = tmp_path / 'source'
    source.mkdir()
    cold = tmp_path / 'cold'
    if legacy_blob:
        (source / 'legacy.bin').write_bytes(b'a' * 16)
        publish_packed_snapshot(cold, 'provider', source, node_id='test', loose_file_threshold_bytes=16)
    (source / 'chunked.bin').write_bytes(b'a' * 16 + b'b' * 16)
    release = publish(cold, source)
    batched, streamed = [], []

    def hash_many(paths):
        batched.extend(paths)
        return {path: sha256_file(path) for path in paths}

    @contextmanager
    def reader(path):
        streamed.append(path)
        with path.open('rb') as stream:
            yield stream

    monkeypatch.setattr(native, 'hash_many', hash_many)
    monkeypatch.setattr(native, 'binary_reader', reader)
    # Exercise the native branch on an isolated fixture. The public entrypoint's
    # canonical-D authority guard has its own tests and is not relaxed here.
    with packed._open_inventory(cold, release.manifest) as entries:
        result = packed._verify_packed_inventory(cold, release, entries,
            materialized_path=None, reconstruct_paths=None, reconstruct_all=True,
            d_primary_native_blob_reads=True, d_primary_native_pack_reads=False, progress=None)
    assert len(batched) == int(legacy_blob)
    assert len(streamed) == 2
    assert result['independently_reconstructed_files'] == 1 + int(legacy_blob)
    assert result['independently_reconstructed_bytes'] == 32 + 16 * int(legacy_blob)
    assert result['verified_object_bytes'] == 32
    assert result['chunk_object_hash_fusion']['objects'] == 2 - int(legacy_blob)


@pytest.mark.parametrize('fault', [None, 'corrupt', 'truncated', 'excess', 'reordered', 'late_guard'])
def test_native_chunk_stream_batches_are_bounded_ordered_and_accepted_only_after_guard(tmp_path, monkeypatch, fault):
    from contextlib import contextmanager
    import hashlib
    import io
    from stockagent.data_sync import windows_cold_io as native

    cold = tmp_path / 'cold'
    cold.mkdir()
    values = [bytes([i]) * 4 for i in range(20)]
    # Deliberately repeat an extent across batches; it remains part of the file.
    values.append(values[0])
    objects, chunks = {}, []
    for value in values:
        digest = hashlib.sha256(value).hexdigest()
        path = cold / digest
        path.write_bytes(value)
        objects[digest] = {'relpath': digest}
        chunks.append({'object_sha256': digest, 'bytes': len(value)})
    row = {'size': sum(map(len, values)), 'storage': {'kind': 'chunks', 'chunks': chunks}}
    batches, accepted = [], []
    @contextmanager
    def reader(members):
        batches.append(members)
        payloads = [path.read_bytes() for path, _ in members]
        if fault == 'reordered':payloads.reverse()
        data = b''.join(payloads)
        if fault == 'corrupt':data = b'!' + data[1:]
        if fault == 'truncated':data = data[:-1]
        if fault == 'excess':data += b'!'
        with io.BytesIO(data) as stream:
            yield stream
            if fault == 'late_guard':raise SnapshotError('authority unavailable after read')
    monkeypatch.setattr(native, 'windows_path', lambda path: 'D:\\stockagent-cold-primary\\fixture')
    monkeypatch.setattr(native, 'binary_reader_many', reader)
    iterator = packed._iter_chunked_file(cold, objects, row, native=True, verified_extent=accepted.append)
    if fault:
        with pytest.raises(SnapshotError):b''.join(iterator)
        assert not accepted
    else:
        assert b''.join(iterator) == b''.join(values)
        assert [len(batch) for batch in batches] == [16, 5]
        assert accepted == [chunk['object_sha256'] for chunk in chunks]


@pytest.mark.parametrize('enabled', [False, True])
def test_explicit_native_chunk_policy_only_selects_large_files_and_keeps_complete_proof(tmp_path, monkeypatch, enabled):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'small.bin').write_bytes(b'a' * 31)
    (source / 'large.bin').write_bytes(b'b' * 32)
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    selected, events = {}, []
    original = packed._iter_chunked_file
    def observed(root, objects, row, **options):
        selected[row['path']] = options['native']
        yield from original(root, objects, row, **options)
    monkeypatch.setattr(packed, 'MIN_NATIVE_CHUNK_STREAM_BYTES', 32)
    monkeypatch.setattr(packed, '_iter_chunked_file', observed)
    with packed._open_inventory(cold, release.manifest) as entries:
        proof = packed._verify_packed_inventory(cold, release, entries,
            materialized_path=None, reconstruct_paths=None, reconstruct_all=True,
            d_primary_native_blob_reads=False, d_primary_native_pack_reads=False,
            d_primary_native_chunk_reads=enabled, progress=events.append)
    assert selected == {'large.bin': enabled, 'small.bin': False}
    assert proof['independently_reconstructed_files'] == 2
    assert proof['independently_reconstructed_bytes'] == 63
    assert any(row.get('native_chunk_batch_reads') for row in events) is enabled


@pytest.mark.parametrize('enabled', [True, 1, 'true'])
def test_new_native_chunk_policy_cannot_bypass_type_or_original_D_authority(tmp_path, enabled):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'bytes').write_bytes(b'exact')
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    with pytest.raises(SnapshotError, match='canonical D|explicit boolean'):
        verify_packed_snapshot(cold, release, reconstruct_all=True,
                               d_primary_native_chunk_reads=enabled)


@pytest.mark.parametrize('fault', ['corrupt', 'truncated', 'grown', 'late_mutation', 'missing_extent_proof'])
def test_fused_reconstruction_rejects_bad_chunks_and_late_changes(tmp_path, monkeypatch, fault):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'file.bin').write_bytes(b'a' * 16 + b'b' * 16)
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    item = release.manifest['archive']['objects'][0]
    path = cold / item['relpath']
    original = packed._iter_chunked_file
    if fault in {'corrupt', 'truncated', 'grown'}:
        raw = path.read_bytes()
        path.write_bytes(b'!' * len(raw) if fault == 'corrupt' else
                         raw[:-1] if fault == 'truncated' else raw + b'!')
    else:
        def changed(root, objects, row, **options):
            if fault == 'missing_extent_proof':
                options.pop('verified_extent')
            yield from original(root, objects, row, **options)
            if fault == 'late_mutation':
                path.write_bytes(path.read_bytes())  # Identical bytes still changed this generation.
        monkeypatch.setattr(packed, '_iter_chunked_file', changed)
    observations = []
    with pytest.raises(SnapshotError):
        verify_packed_snapshot(cold, release, reconstruct_all=True, progress=observations.append)
    assert not any(row['stage'] == 'verify-cold-objects'
                   and row['completed_items'] == row['total_items'] for row in observations)


def test_repeated_extents_multiple_providers_and_deleted_files_share_only_bytes(tmp_path):
    cold = tmp_path / "cold"
    for provider in ("finlab", "finmind", "tej", "weather"):
        source = tmp_path / provider
        source.mkdir()
        (source / "same.bin").write_bytes(b"a" * 64)
        (source / "another.bin").write_bytes(b"a" * 32)
        result = publish(cold, source, provider)
        assert len(result.manifest["archive"]["objects"]) == 1
        item = result.manifest["archive"]["objects"][0]
        assert item["file_count"] == 2 and item["logical_bytes"] == 96
        assert verify_packed_snapshot(cold, result, reconstruct_paths=["same.bin", "another.bin"])[
            "independently_reconstructed_files"] == 2
        restored = fetch_packed_snapshot(cold, tmp_path / "restored", result)
        assert (restored / "same.bin").read_bytes() == b"a" * 64
    assert len(list((cold / "objects" / "blobs").rglob("*.blob"))) == 1
    (source / "same.bin").unlink()
    deleted = publish(cold, source, "weather")
    hot = fetch_packed_snapshot(cold, tmp_path / "restored", deleted)
    assert not (hot / "same.bin").exists() and (hot / "another.bin").is_file()
    assert len(list((cold / "objects" / "blobs").rglob("*.blob"))) == 1


def test_chunk_versions_support_subtree_and_bounded_flat_staging(tmp_path):
    source = tmp_path / "source"
    (source / "partition").mkdir(parents=True)
    payload = b"whole exact file" * 15
    (source / "partition" / "minute.parquet").write_bytes(payload)
    (source / "root.parquet").write_bytes(payload)
    cold = tmp_path / "cold"
    result = publish(cold, source)
    subtree = fetch_packed_subtree(cold, tmp_path / "sub", result, subtree="partition")
    assert (subtree / "minute.parquet").read_bytes() == payload
    destination = tmp_path / "flat"
    destination.mkdir(mode=0o700)
    flat_release = publish_packed_snapshot(cold, "flat-provider", source, node_id="test",
        loose_file_threshold_bytes=16, file_chunk_bytes=16, included_root_files=["root.parquet"])
    stage = stage_packed_root_files(cold, destination, flat_release, expected_names=["root.parquet"],
                                    maximum_bytes=4096)
    assert stage["files"]["root.parquet"].read_bytes() == payload


def test_v1_to_v2_is_lazy_and_legacy_restore_stays_supported(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    original = b"a" * 80
    path = source / "old.bin"
    path.write_bytes(original)
    cold = tmp_path / "cold"
    first = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert first.manifest["schema_version"] == 1
    enrolled = publish(cold, source)
    assert enrolled.manifest["schema_version"] == 2
    assert enrolled.manifest_sha256 != first.manifest_sha256
    assert objects(enrolled) == objects(first)
    assert enrolled.manifest["archive"]["new_stored_bytes"] == 0
    assert enrolled.manifest["archive"]["new_object_count"] == 0
    assert enrolled.manifest["archive"]["inventory"] == first.manifest["archive"]["inventory"]
    assert publish(cold, source).manifest_sha256 == enrolled.manifest_sha256
    (source / "new.bin").write_bytes(b"z" * 80)
    # The native scheduler does not need the operator's one-time CLI flag.
    second = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    rows = {row["path"]: row for row in _load_inventory(cold, second.manifest)}
    assert rows["old.bin"]["storage"]["kind"] == "blob"
    assert rows["new.bin"]["storage"]["kind"] == "chunks"
    assert objects(first).issubset(objects(second))
    assert (fetch_packed_snapshot(cold, tmp_path / "hot", first) / "old.bin").read_bytes() == original


def test_scheduled_publication_inherits_v2_without_requiring_a_new_flag_every_run(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    path.write_bytes(path.read_bytes() + b"c" * 7)
    second = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert second.manifest["schema_version"] == 2
    assert second.manifest["archive"]["file_chunk_bytes"] == 16
    assert len(objects(second) - objects(first)) == 1
    assert verify_packed_snapshot(cold, second)["chunked_files_verified"] == 1


def test_v2_policy_survives_empty_and_small_only_versions(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"small")
    cold = tmp_path / "cold"
    small = publish(cold, source)
    assert small.manifest["schema_version"] == 2
    path.unlink()
    empty = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert empty.manifest["schema_version"] == 2 and empty.manifest["archive"]["objects"] == []
    path.write_bytes(b"a" * 32)
    large = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert _load_inventory(cold, large.manifest)[0]["storage"]["kind"] == "chunks"
    assert large.manifest["archive"]["file_chunk_bytes"] == 16
    verify_packed_snapshot(cold, small)
    verify_packed_snapshot(cold, empty)
    verify_packed_snapshot(cold, large)


@pytest.mark.parametrize("damage", ["unknown", "extent_size", "chunk_hash", "kind", "truncated", "grown"])
def test_chunk_corruption_never_promotes_materialized_tree(tmp_path, damage):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.bin").write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    release = publish(cold, source)
    rows = _load_inventory(cold, release.manifest)
    row = rows[0]
    if damage == "unknown":
        row["storage"]["chunks"][0]["object_sha256"] = "f" * 64
    elif damage == "extent_size":
        row["storage"]["chunks"][0]["bytes"] -= 1
    elif damage == "kind":
        release.manifest["archive"]["objects"][0]["kind"] = "pack"
    else:
        item = release.manifest["archive"]["objects"][0]
        path = cold / item["relpath"]
        payload = path.read_bytes()
        path.write_bytes(payload[:-1] if damage == "truncated" else payload + b"!" if damage == "grown"
                         else b"!" * item["bytes"])
        with pytest.raises(SnapshotError):
            fetch_packed_snapshot(cold, tmp_path / "hot", release)
        assert not list((tmp_path / "hot").rglob("*.READY.json"))
        return
    with pytest.raises(SnapshotError):
        _validate_inventory(release.manifest, rows)


@pytest.mark.parametrize("bad", [True, 0, -1, 1.5, 64 * 1024**2 + 1])
def test_chunk_bounds_fail_before_creating_store(tmp_path, bad):
    source = tmp_path / "source"
    source.mkdir()
    cold = tmp_path / "cold"
    with pytest.raises(SnapshotError, match="file_chunk_bytes"):
        publish_packed_snapshot(cold, "provider", source, file_chunk_bytes=bad)
    assert not cold.exists()


@pytest.mark.parametrize("damage", ["full_hash", "order"])
@pytest.mark.parametrize('reconstruct_all', [False, True])
def test_default_cold_verifier_checks_complete_chunk_order_and_file_hash(
        tmp_path, monkeypatch, damage, reconstruct_all):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    release = publish(cold, source)
    load = packed._iter_inventory

    def damaged(*args):
        for row in load(*args):
            if damage == "order":
                row["storage"]["chunks"].reverse()
            else:
                row["sha256"] = "0" * 64
            yield row

    monkeypatch.setattr(packed, "_iter_inventory", damaged)
    with pytest.raises(SnapshotError, match="exact cold chunk reconstruction"):
        verify_packed_snapshot(cold, release, reconstruct_all=reconstruct_all)


def test_chunk_install_failure_keeps_head_and_retry_reuses_only_complete_objects(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    head_before = first.head_path.read_bytes()
    path.write_bytes(path.read_bytes() + b"x" * 16 + b"y" * 16)
    install = packed._install_immutable_object
    calls = 0

    def failed_install(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected storage interruption")
        return install(*args, **kwargs)

    monkeypatch.setattr(packed, "_install_immutable_object", failed_install)
    with pytest.raises(OSError, match="storage interruption"):
        publish(cold, source)
    assert first.head_path.read_bytes() == head_before
    monkeypatch.setattr(packed, "_install_immutable_object", install)
    second = publish(cold, source)
    assert (fetch_packed_snapshot(cold, tmp_path / "hot", second) / "data.bin").read_bytes() == path.read_bytes()
    assert verify_packed_snapshot(cold, first)["chunked_files_verified"] == 1


def test_source_change_during_chunking_cannot_advance_head(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 32)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    head_before = first.head_path.read_bytes()
    path.write_bytes(b"b" * 32)
    store = packed._store_chunked_source

    def raced(*args, **kwargs):
        store(*args, **kwargs)
        path.write_bytes(b"c" * 32)

    monkeypatch.setattr(packed, "_store_chunked_source", raced)
    with pytest.raises(SnapshotError, match="changed"):
        publish(cold, source)
    assert first.head_path.read_bytes() == head_before
