import hashlib
from pathlib import Path

import pytest

from stockagent.data_sync import cold_primary
from stockagent.data_sync import packed_snapshots as packed
from stockagent.data_sync import windows_chunk_hash as chunks
from stockagent.data_sync import windows_cold_io as native
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def original(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'negative').write_bytes(b'-2,NaN,3.5\n' * 8)
    (source / '重複').write_bytes(b'abcd' * 20)
    root = tmp_path / 'cold'
    selected = packed.publish_packed_snapshot(root, 'example', source,
        loose_file_threshold_bytes=4, file_chunk_bytes=4)
    with packed._open_inventory(root, selected.manifest) as inventory:
        rows = list(inventory.chunked_rows())
    objects = {item['sha256']: item for item in selected.manifest['archive']['objects']}
    monkeypatch.setattr(packed, '_check_native_pack_read_authority', lambda *_: None)
    monkeypatch.setattr(cold_primary, '_check_d_primary_mount', lambda *_: None)
    paths = {}
    def windows_path(path):
        name = 'D:\\stockagent-cold-primary\\' + str(path.relative_to(root)).replace('/', '\\')
        paths[name] = path
        return name
    monkeypatch.setattr(native, 'windows_path', windows_path)
    calls = []
    def batch(configs):
        calls.append(configs)
        result = []
        for config in configs:
            complete = hashlib.sha256()
            total = 0
            for item in config['chunks']:
                data = paths[item['path']].read_bytes()
                if len(data) != item['bytes'] or hashlib.sha256(data).hexdigest() != item['sha256']:
                    raise SnapshotError('full native chunk bytes differ')
                complete.update(data)
                total += len(data)
            result.append({'bytes': total, 'sha256': complete.hexdigest(),
                           'chunks': len(config['chunks']), 'io': 'windows-chunk-original-sha-v1'})
        return result
    monkeypatch.setattr(native, '_batch', batch)
    return root, selected, rows, objects, calls, batch


def test_native_complete_original_equals_portable_with_repeats_and_raw_values(original, monkeypatch):
    root, selected, rows, objects, calls, _ = original
    baseline = packed.verify_packed_snapshot(root, selected, reconstruct_all=True)
    monkeypatch.setattr(packed, '_iter_chunked_file', lambda *_args, **_kwargs: pytest.fail('raw bytes crossed WSL'))
    result = packed.verify_packed_snapshot(root, selected, reconstruct_all=True,
                                          d_primary_native_original_hashes=True)
    assert result['independently_reconstructed_bytes'] == baseline['independently_reconstructed_bytes']
    assert result['independently_reconstructed_files'] == baseline['independently_reconstructed_files'] == 2
    assert result['verified_object_bytes'] == baseline['verified_object_bytes']
    assert result['native_original_reconstruction']['files'] == 2
    assert result['native_original_reconstruction']['payload_bytes_piped_to_wsl'] == 0
    assert sum(len(config['chunks']) for group in calls for config in group) > len(objects)


@pytest.mark.parametrize('damage', ['sha256', 'bytes', 'chunks', 'io', 'generation', 'payload', 'path'])
def test_native_original_never_accepts_wrong_bytes_counts_identity_or_generation(original, monkeypatch, damage):
    root, selected, rows, objects, calls, batch = original
    path = root / next(iter(objects.values()))['relpath']
    if damage == 'payload':
        path.write_bytes(b'X' * path.stat().st_size)
    elif damage == 'path':
        monkeypatch.setattr(native, 'windows_path', lambda *_: 'C:\\unrelated')
    else:
        def corrupt(configs):
            result = batch(configs)
            if damage == 'generation':
                path.write_bytes(path.read_bytes())
            else:
                result[-1][damage] = 'wrong' if damage in {'sha256', 'io'} else -1
            return result
        monkeypatch.setattr(native, '_batch', corrupt)
    with pytest.raises(SnapshotError):
        list(chunks.verified_chunk_rows(root, objects, rows))


def test_chunk_batches_bound_repeated_byte_work_not_only_unique_objects(original, monkeypatch):
    root, selected, rows, objects, calls, _ = original
    monkeypatch.setattr(chunks, 'MAX_BATCH_BYTES', max(row['size'] for row in rows))
    assert len(list(chunks.verified_chunk_rows(root, objects, rows))) == 2
    assert len(calls) == 2
    assert all(sum(config['expected_bytes'] for config in group) <= chunks.MAX_BATCH_BYTES for group in calls)


def test_large_original_keeps_portable_complete_sha_without_native_acceptance(original, monkeypatch):
    root, selected, rows, objects, calls, _ = original
    monkeypatch.setattr(chunks, 'MAX_ORIGINAL_BYTES', 1)
    assert list(chunks.verified_chunk_rows(root, objects, rows)) == [(row, None) for row in rows]
    assert not calls


def test_changed_native_program_is_rejected_before_payload(original, monkeypatch):
    root, selected, rows, objects, calls, _ = original
    monkeypatch.setattr(chunks, 'SCRIPT_SHA256', '0' * 64)
    with pytest.raises(SnapshotError, match='implementation changed'):
        list(chunks.verified_chunk_rows(root, objects, rows))
    assert not calls


@pytest.mark.parametrize('size', [8 * 1024**3 + 1, 14635485808, 16 * 1024**3])
def test_complete_original_can_fill_existing_byte_and_metadata_batch(size):
    # Real 13.63 GiB / 1,745-extent panels used to fall back to the WSL pipe.
    # Exercise their actual manifest shape without allocating giant test data.
    chunk_bytes = 8 * 1024**2
    complete, tail = divmod(size, chunk_bytes)
    extents = [{'object_sha256': 'a' * 64, 'bytes': chunk_bytes}] * complete
    if tail:
        extents.append({'object_sha256': 'b' * 64, 'bytes': tail})
    row = {'size': size, 'storage': {'kind': 'chunks', 'chunks': extents}}
    objects = {'a' * 64: {'kind': 'blob'}, 'b' * 64: {'kind': 'blob'}}
    admitted = chunks._prepare(row, objects)
    assert admitted is not None and sum(count for _, count in admitted) == size
    assert len(admitted) == len(extents)
    assert size <= chunks.MAX_BATCH_BYTES and len(admitted) <= chunks.MAX_BATCH_EXTENTS


def test_oversized_original_still_keeps_portable_path_without_payload_allocation():
    extents = [{'object_sha256': 'a' * 64, 'bytes': 8 * 1024**2}] * 2048
    extents.append({'object_sha256': 'b' * 64, 'bytes': 1})
    row = {'size': 16 * 1024**3 + 1, 'storage': {'kind': 'chunks', 'chunks': extents}}
    assert chunks._prepare(row, {'a' * 64: {'kind': 'blob'}, 'b' * 64: {'kind': 'blob'}}) is None
