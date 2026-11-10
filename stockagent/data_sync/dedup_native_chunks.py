"""Batch verified native chunk reads across dedup representative files.

Each bounded batch is completely read, hashed, and its native source fence is
closed before any byte is exposed to a pack builder. This matters when a ZIP
checkpoint is committed between two files that shared a native read process.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
from pathlib import Path

from stockagent.data_sync import packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError

MAX_BATCH_BYTES = 128 * 1024**2
MAX_BATCH_MEMBERS = 16


def _batches(extents, *, maximum_bytes=MAX_BATCH_BYTES):
    batch, size = [], 0
    for item in extents:
        length = item[2]
        if type(length) is not int or length < 0 or length > maximum_bytes:
            raise SnapshotError('dedup native chunk exceeds its bounded batch')
        if batch and (len(batch) >= MAX_BATCH_MEMBERS or size+length > maximum_bytes):
            yield batch
            batch, size = [], 0
        batch.append(item)
        size += length
    if batch:
        yield batch


class _VerifiedChunkSequence:
    def __init__(self, extents, batch_reader, guard):
        self.batches = iter(_batches(extents))
        self.batch_reader, self.guard = batch_reader, guard
        self.buffers, self.position = iter(()), memoryview(b'')

    def _load(self):
        batch = next(self.batches, None)
        if batch is None:
            return False
        self.guard()
        blocks = []
        with self.batch_reader([(path, size) for path, _digest, size in batch]) as stream:
            for _path, expected, size in batch:
                digest, remaining, parts = hashlib.sha256(), size, []
                while remaining:
                    data = stream.read(min(remaining, 8*1024**2))
                    if not data or len(data) > remaining:
                        raise SnapshotError('dedup native chunk batch ended early')
                    remaining -= len(data)
                    digest.update(data)
                    parts.append(data)
                    self.guard()
                if digest.hexdigest() != expected:
                    raise SnapshotError('dedup native chunk checksum mismatch')
                blocks.append(b''.join(parts))
            if stream.read(1):
                raise SnapshotError('dedup native chunk batch has excess bytes')
        # The context above verified native process completion, volume identity
        # and every source generation before these bytes can be checkpointed.
        self.buffers = iter(blocks)
        return True

    def read(self, size):
        if type(size) is not int or size < 0:
            raise SnapshotError('dedup native chunk read size is invalid')
        if size == 0:
            return b''
        while not self.position:
            block = next(self.buffers, None)
            if block is None:
                if not self._load():
                    return b''
                continue
            self.position = memoryview(block)
        result = bytes(self.position[:size])
        self.position = self.position[len(result):]
        return result


class _BoundedFile:
    def __init__(self, stream, size):
        self.stream, self.remaining = stream, size

    def read(self, size):
        if self.remaining == 0:
            return b''
        data = self.stream.read(min(size, self.remaining))
        if not data:
            raise SnapshotError('dedup native source ended before its file boundary')
        self.remaining -= len(data)
        return data


def stream(cold: Path, manifest: dict, entries, *, selected_paths: set[str], completed_paths: set[str],
           consume, guard=lambda: None, native=False, batch_reader=None) -> set[str]:
    """Stream only unfinished chunked representatives, returning their paths."""
    selected = {row['path'] for row in entries.chunked_rows()
                if row['path'] in selected_paths and row['path'] not in completed_paths}
    if not selected:
        return set()
    if batch_reader is None:
        from stockagent.data_sync.windows_cold_io import binary_reader_many, windows_path
        from stockagent.data_sync.packed_object_placement import has_placed_objects
        if not native or windows_path(Path(cold)/'objects') is None or has_placed_objects(cold, manifest['archive']['objects']):
            # Portable test/other-host path retains the existing exact reader.
            objects = {item['sha256']: item for item in manifest['archive']['objects']}
            for row in entries.chunked_rows():
                if row['path'] in selected:
                    with closing(p._ChunkedFileReader(p._iter_chunked_file(cold, objects, row, native=native))) as stream:
                        source = _BoundedFile(stream, int(row['size']))
                        consume(source, row)
                        if source.remaining:
                            raise SnapshotError('dedup consumer did not finish its source file')
            return selected
        batch_reader = binary_reader_many
    objects = {item['sha256']: item for item in manifest['archive']['objects']}
    def extents():
        for row in entries.chunked_rows():
            if row['path'] not in selected:
                continue
            for digest, size in p._storage_extents(row['storage'], int(row['size'])):
                item = objects.get(digest)
                if item is None or item['kind'] != 'blob' or item['bytes'] != size:
                    raise SnapshotError('dedup native extent differs from its manifest')
                yield p._path_under(cold, item['relpath'], 'chunk object relpath'), digest, size
    sequence = _VerifiedChunkSequence(extents(), batch_reader, guard)
    for row in entries.chunked_rows():
        if row['path'] in selected:
            source = _BoundedFile(sequence, int(row['size']))
            consume(source, row)
            if source.remaining:
                raise SnapshotError('dedup consumer did not finish its source file')
    if sequence.read(1):
        raise SnapshotError('dedup native source contained unexpected extra bytes')
    return selected
