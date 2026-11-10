"""Verify complete original/chunk SHAs on D without piping raw bytes to WSL.

The canonical packed verifier owns inventory, counts and final generations.
This read-only adapter hashes every original byte in its exact extent order;
each extent also receives its full SHA. It creates no materialized source.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import stat

from stockagent.data_sync.desync_snapshots import SnapshotError

MAX_BATCH_BYTES = 16 * 1024**3
MAX_BATCH_EXTENTS = 2048
# A complete original can consume one existing batch. Its byte work and
# metadata stay bounded; FileStream retains only the same 1 MiB payload buffer.
# Do not send an otherwise valid 8--16 GiB original over the WSL byte pipe.
MAX_ORIGINAL_BYTES = MAX_BATCH_BYTES
MAX_ORIGINAL_EXTENTS = MAX_BATCH_EXTENTS
SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
from stockagent.data_sync.windows_cold_io import SCRIPT
SCRIPT_SHA256 = hashlib.sha256(SCRIPT.read_bytes()).hexdigest()


def _check_implementation():
    if (hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != SOURCE_SHA256
            or hashlib.sha256(SCRIPT.read_bytes()).hexdigest() != SCRIPT_SHA256):
        raise SnapshotError('native original verification implementation changed')


def _prepare(row, objects):
    from stockagent.data_sync.packed_snapshots import _storage_extents
    extents = list(_storage_extents(row['storage'], int(row['size'])))
    if row['size'] > MAX_ORIGINAL_BYTES or len(extents) > MAX_ORIGINAL_EXTENTS:
        return None
    if not extents or any(objects[digest]['kind'] != 'blob' for digest, _ in extents):
        raise SnapshotError('native original needs exact immutable chunk objects')
    return extents


def _run_batch(root, objects, jobs):
    from stockagent.data_sync import packed_snapshots as packed
    from stockagent.data_sync import windows_cold_io as native
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    _check_implementation()
    packed._check_native_pack_read_authority(root)
    unique = {}
    for _, extents in jobs:
        for digest, size in extents:
            item = objects[digest]
            if item['bytes'] != size:
                raise SnapshotError('native original extent differs from its exact cold object')
            path = packed._path_under(root, item['relpath'], 'native chunk relpath')
            unique[path] = (digest, size)
    before = packed._collect_object_signatures(unique)
    if any(not stat.S_ISREG(info.st_mode) or info.st_size != unique[path][1]
           for path, info in before.items()):
        raise SnapshotError('native original chunk is redirected or its size differs')
    def configure(path):
        value = native.windows_path(path)
        if value is None or not value.lower().startswith('d:\\stockagent-cold-primary\\'):
            raise SnapshotError('native original is outside enrolled immutable D history')
        return path, value
    with ThreadPoolExecutor(max_workers=native.HASH_METADATA_WORKERS,
                            thread_name_prefix='native-chunk-path') as pool:
        paths = dict(pool.map(configure, unique))
    configs = []
    for row, extents in jobs:
        chunks = [{'path': paths[root / objects[digest]['relpath']], 'sha256': digest, 'bytes': size}
                  for digest, size in extents]
        configs.append({'path': chunks[0]['path'], 'mode': 'hash-chunks',
                        'expected_bytes': row['size'], 'chunks': chunks})
    proofs = native._batch(configs)
    _check_d_primary_mount(Path('/srv/stockagent-packed'))
    after = packed._collect_object_signatures(unique)
    if any(not native._stable(before[path], after[path]) for path in before):
        raise SnapshotError('native original chunk generation changed')
    for (row, extents), proof in zip(jobs, proofs, strict=True):
        if (proof.get('bytes') != row['size'] or proof.get('sha256') != row['sha256']
                or proof.get('chunks') != len(extents) or proof.get('io') != 'windows-chunk-original-sha-v1'):
            raise SnapshotError('exact native original SHA/size or ordered chunk count differs')
    _check_implementation()
    return proofs


def verified_chunk_rows(root, objects, rows):
    """Yield accepted per-batch originals; owner still checks final generations.

    Oversized originals retain the canonical streaming implementation. Bounds
    cover actual extents (including repeats) and decoded byte work, not only
    the number of unique content-addressed objects.
    """
    from stockagent.data_sync.windows_cold_io import MAX_HASH_BATCH_MEMBERS
    jobs, count, size = [], 0, 0
    def flush():
        _run_batch(root, objects, jobs)
        return list(jobs)
    for row in rows:
        extents = _prepare(row, objects)
        if extents is not None:
            from stockagent.data_sync import packed_snapshots as packed
            from stockagent.data_sync.packed_object_placement import is_archive_path
            if any(is_archive_path(packed._read_object_path(root, objects[digest]['relpath']))
                   for digest, _size in extents):
                extents = None  # The exact mixed-volume byte reader owns it.
        if extents is None:
            if jobs:
                yield from flush()
                jobs, count, size = [], 0, 0
            yield row, None
            continue
        if jobs and (len(jobs) == MAX_HASH_BATCH_MEMBERS
                     or size + row['size'] > MAX_BATCH_BYTES
                     or count + len(extents) > MAX_BATCH_EXTENTS):
            yield from flush()
            jobs, count, size = [], 0, 0
        jobs.append((row, extents))
        count += len(extents)
        size += row['size']
    if jobs:
        yield from flush()
    _check_implementation()
