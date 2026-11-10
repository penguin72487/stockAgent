"""Bounded native-D buffers shared by cooperative reconstruction workers.

Only the pool's serial fill lock calls ``open``. Decoders own their returned
buffer; unclaimed prefetched buffers are disposed when the pool joins.
"""
from contextlib import contextmanager
import hashlib
import io
from pathlib import Path
import stat

SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class PackSources:
    def __init__(self, root, packs, *, native):
        self.root, self.native = root, native
        self.pending = {item['sha256']: item for item in packs}
        self.buffers = {}
        self.batches = self.members = self.bytes = 0

    def _signature(self, item):
        from stockagent.data_sync import packed_snapshots as packed
        info = packed._read_object_path(self.root, item['relpath']).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size != item['bytes']:
            raise packed.SnapshotError('batched native pack is redirected or changed')
        return packed._source_stat(info), info.st_nlink

    @staticmethod
    def _candidate(item):
        # Tiny, low-member packs already use a fast direct single read. Batch
        # only packs otherwise needing native-process startup, without grouping
        # huge packs or retaining a whole dataset in RAM.
        count = item['bytes']
        members = int(item.get('stored_file_count', item.get('file_count', 129)))
        return 0 < count <= 64*1024**2 and (count > 1024**2 or members > 128)

    def _fill(self, items):
        from stockagent.data_sync import packed_snapshots as packed
        from stockagent.data_sync.windows_cold_io import binary_reader_many
        signatures = {item['sha256']: self._signature(item) for item in items}
        buffers = {}
        try:
            with binary_reader_many([(packed._read_object_path(self.root, item['relpath']), item['bytes']) for item in items]) as stream:
                for item in items:
                    buffer = buffers[item['sha256']] = io.BytesIO()
                    digest, remaining = hashlib.sha256(), item['bytes']
                    while remaining:
                        block = stream.read(min(remaining, 8*1024**2))
                        if not block:
                            raise packed.SnapshotError('incomplete native pack batch')
                        remaining -= len(block)
                        digest.update(block)
                        buffer.write(block)
                    if digest.hexdigest() != item['sha256']:
                        raise packed.SnapshotError('native batch pack SHA differs')
            # No buffer is exposed until the original native reader has passed
            # child exit, exact EOF, mount identity and all generation fences.
            if any(self._signature(item) != signatures[item['sha256']] for item in items):
                raise packed.SnapshotError('native batch pack generation changed')
        except BaseException:
            for buffer in buffers.values():
                buffer.close()
            raise
        for item in items:
            key = item['sha256']
            self.pending.pop(key, None)
            self.buffers[key] = buffers[key], signatures[key]
        self.batches += 1
        self.members += len(items)
        self.bytes += sum(item['bytes'] for item in items)

    @contextmanager
    def open(self, item):
        from stockagent.data_sync import packed_snapshots as packed
        key = item['sha256']
        self.pending.pop(key, None)
        from stockagent.data_sync.packed_object_placement import is_archive_path
        path = packed._read_object_path(self.root, item['relpath'])
        if key not in self.buffers and self.native and self._candidate(item) and not is_archive_path(path):
            items, size = [item], item['bytes']
            for candidate in self.pending.values():
                if (self._candidate(candidate) and size+candidate['bytes'] <= 128*1024**2
                        and not is_archive_path(packed._read_object_path(self.root, candidate['relpath']))):
                    items.append(candidate)
                    size += candidate['bytes']
                    if len(items) == 8:
                        break
            if len(items) > 1 and packed._native_pack_buffer_allowed(path, size):
                self._fill(items)
        if key not in self.buffers:
            with packed._native_pack_source(path, item, enabled=self.native) as source:
                yield source
            return
        buffer, signature = self.buffers.pop(key)
        try:
            if self._signature(item) != signature:
                raise packed.SnapshotError('prefetched pack generation changed before delegation')
            buffer.seek(0)
            yield buffer
            if self._signature(item) != signature:
                raise packed.SnapshotError('prefetched pack generation changed during delegation')
        finally:
            buffer.close()

    def close(self):
        for buffer, _ in self.buffers.values():
            buffer.close()
        self.buffers.clear()

    def summary(self):
        if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != SOURCE_SHA256:
            raise ValueError('native pack batch adapter changed during reconstruction')
        return {'native_read_batches': self.batches, 'packs_batched': self.members,
                'encoded_bytes_batched': self.bytes, 'maximum_batch_bytes': 128*1024**2,
                'maximum_batch_members': 8, 'adapter_sha256': SOURCE_SHA256}
