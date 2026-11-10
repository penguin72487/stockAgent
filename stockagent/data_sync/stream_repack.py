"""Rewrite one fixed packed release into source-local ZIPs on E.

Source members are decoded directly into their new archive while their original
hash is computed. There is no uncompressed tree, independent readback or
backup. A completed E build and each D install are journalled for retry.
Old releases remain addressable until the separate reference-aware retirement.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
import zipfile
import zlib

from stockagent.data_sync import packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_metadata import MetadataRows

CONTRACT = 'stockagent.stream_source_local_repack.v1'
TARGET_BYTES = 4 * 1024**3
PARALLEL_DEFLATE_MIN_BYTES = 64 * 1024**2


def _identity(value):
    body = {key: item for key, item in value.items() if key != 'identity_sha256'}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=False).encode()).hexdigest()


def _signature(path: Path):
    info = path.lstat()
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_nlink]


@lru_cache(maxsize=8)
def _verified_remount(root: Path, device: int, inode: int, kind: str) -> bool:
    """Bind a renumbered device to its enrolled volume, once per process."""
    if (root.stat().st_dev, root.stat().st_ino) != (device, inode):
        return False
    if kind == 'workspace':
        from stockagent.data_sync import workspace_volume
        config = workspace_volume.configuration()
        if root != Path(config['mount_point']):
            return False
        workspace_volume.check(c=config)  # mount, filesystem UUID and marker
    elif kind == 'cold':
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        if root != Path('/srv/stockagent-packed'):
            return False
        _check_d_primary_mount(root)  # canonical D volume identity and mount
    elif kind == 'cold_volume':
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        if (root != Path('/srv/stockagent-d-volume/stockagent-cold-primary')
                or root.is_symlink() or root.resolve(strict=True) != root):
            return False
        # The D root and canonical bind can have different 9p st_dev values.
        # The existing guard verifies both enrolled sources, markers, NTFS root
        # file ID and 8 KiB request sizes before accepting device renumbering.
        _check_d_primary_mount(Path('/srv/stockagent-packed'))
    else:
        return False
    return True


def _trusted_device_renumbering(path: Path, current_device: int) -> bool:
    """Accept st_dev changes only within an explicitly enrolled E or D root."""
    path = path.absolute()
    for root, kind in ((Path('/srv/stockagent-workspace'), 'workspace'),
                       (Path('/srv/stockagent-packed'), 'cold'),
                       (Path('/srv/stockagent-d-volume/stockagent-cold-primary'), 'cold_volume')):
        if not path.is_relative_to(root) or path == root:
            continue
        try:
            info = root.stat()
            return (info.st_dev == current_device
                    and _verified_remount(root, info.st_dev, info.st_ino, kind))
        except (OSError, ValueError, TypeError, KeyError, SnapshotError, subprocess.SubprocessError):
            return False
    return False


def _generation_matches(path: Path, expected, actual) -> bool:
    """Keep inode, metadata and clocks exact across a verified WSL remount.

    Linux st_dev is assigned at mount time, so it is not a durable volume ID.
    Only that field may differ, and only after the original E/D guard succeeds.
    """
    if actual == expected:
        return True
    if (not isinstance(expected, list) or not isinstance(actual, list) or len(actual) < 2
            or len(expected) != len(actual)
            or any(type(value) is not int or value < 0 for value in expected+actual)
            or actual[1:] != expected[1:]):
        return False
    return _trusted_device_renumbering(path, actual[0])


def _signature_matches(path: Path, expected) -> bool:
    if _generation_matches(path, expected, _signature(path)):
        return True
    from stockagent.data_sync.packed_object_placement import matches_original_generation
    return matches_original_generation(path, expected)


def _safe_stage_file(root: Path, name: str) -> Path:
    if not re.fullmatch(r'(?:pack-[0-9a-f]{64}\.zip|inventory-[0-9a-f]{64}\.jsonl\.gz|checkpoint\.sqlite)', name):
        raise SnapshotError('invalid streaming repack stage member')
    path = root/name
    if (path.is_symlink() or path.resolve(strict=True) != path or not path.is_file()
            or path.stat().st_nlink != 1):
        raise SnapshotError('streaming repack stage member changed or is redirected')
    return path


class _Checkpoint:
    """E-only durable map of complete ZIPs; at most the open ZIP is redone."""
    def __init__(self, stage: Path, dataset: str, expected_sha: str):
        self.path = stage/'checkpoint.sqlite'
        new = not os.path.lexists(self.path)
        if new:
            temporary = stage/('checkpoint-'+uuid.uuid4().hex+'.partial.sqlite')
            fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            db = sqlite3.connect(temporary)
            try:
                db.execute('PRAGMA synchronous=FULL')
                db.execute('PRAGMA journal_mode=DELETE')
                with db:
                    db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID')
                    db.execute('CREATE TABLE packages (number INTEGER PRIMARY KEY, body TEXT NOT NULL)')
                    db.execute('CREATE TABLE members (path BLOB PRIMARY KEY, digest TEXT NOT NULL, '
                               'size INTEGER NOT NULL) WITHOUT ROWID')
                    db.executemany('INSERT INTO meta VALUES (?,?)', [('contract', CONTRACT+'.checkpoint'),
                        ('dataset', dataset), ('old_manifest_sha256', expected_sha)])
            finally:
                db.close()
            os.rename(temporary, self.path)
            p._fsync_directory(stage)
        else:
            _safe_stage_file(stage, self.path.name)
        self.db = sqlite3.connect(self.path)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA journal_mode=DELETE')
        meta = dict(self.db.execute('SELECT key,value FROM meta'))
        if meta != {'contract': CONTRACT+'.checkpoint', 'dataset': dataset, 'old_manifest_sha256': expected_sha}:
            self.db.close()
            raise SnapshotError('streaming checkpoint identity differs')
        self.packages = [json.loads(row[0]) for row in self.db.execute('SELECT body FROM packages ORDER BY number')]
        digests = {row['sha256'] for row in self.packages}
        if len(digests) != len(self.packages):
            raise SnapshotError('streaming checkpoint contains duplicate packs')
        count, total = self.db.execute('SELECT COUNT(*),COALESCE(SUM(size),0) FROM members').fetchone()
        if (count != sum(item['file_count'] for item in self.packages)
                or total != sum(item['logical_bytes'] for item in self.packages)):
            raise SnapshotError('streaming checkpoint counts differ')
        for row in self.packages:
            path = _safe_stage_file(stage, row['name'])
            if not _signature_matches(path, row.get('E_signature')):
                raise SnapshotError('completed checkpoint pack generation changed')
        if any(row[0] not in digests for row in self.db.execute('SELECT DISTINCT digest FROM members')):
            raise SnapshotError('streaming checkpoint member references unknown pack')

    @staticmethod
    def _key(path):
        return path.encode('utf-8', errors='surrogatepass')

    def lookup(self, path):
        result = self.db.execute('SELECT digest FROM members WHERE path=?', (self._key(path),)).fetchone()
        return result[0] if result else None

    def ordered_members(self):
        """Stream the durable member map in the same byte order as inventory paths."""
        cursor = self.db.execute('SELECT path,digest,size FROM members ORDER BY path')
        try:
            yield from cursor
        finally:
            cursor.close()

    def commit(self, package, entries):
        # Source archives are consumed in physical object order, which can
        # scatter paths. Sort this bounded pack's metadata before inserting
        # into the path-keyed B-tree; ZIP payload order is unchanged.
        rows = sorted((self._key(entry.path), package['sha256'], entry.size) for entry in entries)
        with self.db:
            self.db.execute('INSERT INTO packages(body) VALUES (?)',
                            (json.dumps(package, sort_keys=True, separators=(',', ':')),))
            self.db.executemany('INSERT INTO members(path,digest,size) VALUES (?,?,?)', rows)
        self.packages.append(package)

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None


class _SequentialHashWriter:
    """Give zipfile a non-seekable output so hashing needs no second pass."""
    def __init__(self, raw):
        self.raw = raw
        self.digest = hashlib.sha256()
        self.count = 0

    def write(self, data):
        view = memoryview(data)
        length = len(view)
        while view:
            count = self.raw.write(view)
            if type(count) is not int or count <= 0 or count > len(view):
                raise OSError('short streaming pack write')
            self.digest.update(view[:count])
            self.count += count
            view = view[count:]
        return length

    def tell(self):
        return self.count

    def seek(self, *_args):
        raise io.UnsupportedOperation('sequential ZIP output')

    def flush(self):
        self.raw.flush()


def _independent_deflate_block(data: bytes) -> bytes:
    # Every block starts without a preset dictionary, and sync-flush ends on
    # a byte boundary without marking the stream final. The ordered blocks
    # therefore form one valid raw DEFLATE stream when a final empty stream is
    # appended. ZIP CRC and source SHA are still computed by their owners.
    compressor = zlib.compressobj(6, zlib.DEFLATED, -15)
    return compressor.compress(data) + compressor.flush(zlib.Z_SYNC_FLUSH)


class _ParallelDeflate:
    """Bounded ordered compressor implementing ZipFile's compressor interface."""
    def __init__(self, workers: int):
        if type(workers) is not int or not 2 <= workers <= 4:
            raise ValueError('parallel deflate requires two to four workers')
        self.pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='source-pack-deflate')
        self.pending = deque()
        self.maximum_pending = workers * 2

    def compress(self, data) -> bytes:
        if not data:
            return b''
        if len(data) > 8 * 1024**2:
            raise ValueError('parallel deflate source block exceeds its memory bound')
        self.pending.append(self.pool.submit(_independent_deflate_block, bytes(data)))
        return self.pending.popleft().result() if len(self.pending) >= self.maximum_pending else b''

    def flush(self) -> bytes:
        try:
            parts = [future.result() for future in self.pending]
            self.pending.clear()
            final = zlib.compressobj(6, zlib.DEFLATED, -15)
            parts.append(final.flush(zlib.Z_FINISH))
            return b''.join(parts)
        finally:
            self.pool.shutdown(wait=True, cancel_futures=True)


class _PackBuilder:
    def __init__(self, stage: Path, output: MetadataRows | None, *, checkpoint=None, progress=None,
                 large_member_deflate_workers=1):
        if output is None and checkpoint is None:
            raise ValueError('a pack builder requires an output index or durable checkpoint')
        if type(large_member_deflate_workers) is not int or not 1 <= large_member_deflate_workers <= 4:
            raise ValueError('large-member deflate workers must be between one and four')
        self.large_member_deflate_workers = large_member_deflate_workers
        self.stage, self.output, self.progress, self.checkpoint = stage, output, progress, checkpoint
        self.raw = self.writer = self.archive = self.temporary = None
        self.pending = []
        self.pending_bytes = 0
        self.packages = list(checkpoint.packages) if checkpoint is not None else []
        self.files = sum(item['file_count'] for item in self.packages)
        self.bytes = sum(item['logical_bytes'] for item in self.packages)
        self.last_report = 0.0

    def _start(self):
        self.temporary = self.stage/('pack-'+uuid.uuid4().hex+'.partial')
        self.raw = self.temporary.open('xb', buffering=1024*1024)
        self.writer = _SequentialHashWriter(self.raw)
        self.archive = zipfile.ZipFile(self.writer, mode='w', allowZip64=True,
                                       compression=zipfile.ZIP_DEFLATED, compresslevel=6)

    def _finish(self):
        if self.archive is None:
            return
        # Members arrive grouped by old object for sequential HDD reads. The
        # canonical reader expects ZIP central-directory names in path order;
        # sort that metadata only, without rewriting or seeking payload bytes.
        self.archive.filelist.sort(key=lambda info: info.filename)
        self.archive.close()
        self.writer.flush()
        os.fsync(self.raw.fileno())
        digest, count = self.writer.digest.hexdigest(), self.writer.count
        self.raw.close()
        final = self.stage/('pack-'+digest+'.zip')
        if final.exists() or final.is_symlink():
            raise SnapshotError('streaming repack produced a duplicate stage name')
        os.rename(self.temporary, final)
        p._fsync_directory(self.stage)
        for entry in self.pending:
            entry.storage = {'kind': 'pack', 'member': entry.path, 'object_sha256': digest}
        # Input and output inventories are independent; upsert replaces the
        # output row in both its in-memory and SQLite-backed representations.
        if self.output is not None:
            for entry in self.pending:
                self.output.upsert(entry)
        package = {'sha256': digest, 'name': final.name, 'bytes': count,
            'E_signature': _signature(final), 'file_count': len(self.pending),
            'logical_bytes': self.pending_bytes}
        if self.checkpoint is not None:
            self.checkpoint.commit(package, self.pending)
        self.packages.append(package)
        self.raw = self.writer = self.archive = self.temporary = None
        self.pending, self.pending_bytes = [], 0

    def consume(self, stream, row):
        size = int(row['size'])
        if self.pending and (len(self.pending) >= p.MAX_PACK_MEMBERS
                             or self.pending_bytes + size > TARGET_BYTES):
            self._finish()
        if self.archive is None:
            self._start()
        entry = p._entry_from_record(row)
        info = p._zip_info(entry)
        current_file_bytes = 0
        with self.archive.open(info, mode='w', force_zip64=True) as target:
            if (self.large_member_deflate_workers > 1 and info.compress_type == zipfile.ZIP_DEFLATED
                    and size >= PARALLEL_DEFLATE_MIN_BYTES):
                if not hasattr(target, '_compressor'):
                    raise SnapshotError('the runtime cannot attach the bounded ZIP deflate compressor')
                target._compressor = _ParallelDeflate(self.large_member_deflate_workers)
            for block in p._verified_member_blocks(stream, size, str(row['sha256']), label=row['path']):
                target.write(block)
                current_file_bytes += len(block)
                if self.progress is not None and time.monotonic()-self.last_report >= 5:
                    self.last_report = time.monotonic()
                    self.progress({'stage': 'streaming_source_into_E_packs',
                                   'files_completed': self.files,
                                   'logical_bytes_processed': self.bytes+current_file_bytes})
        self.pending.append(entry)
        self.pending_bytes += size
        self.files += 1
        self.bytes += size
        if self.progress is not None and time.monotonic()-self.last_report >= 5:
            self.last_report = time.monotonic()
            self.progress({'stage': 'streaming_source_into_E_packs',
                           'files_completed': self.files, 'logical_bytes_processed': self.bytes})

    def close(self):
        self._finish()
        return self.packages

    def abort(self):
        if self.archive is not None:
            try:
                self.archive.close()
            finally:
                self.raw.close()
            self.archive = self.raw = None


class _BoundedMemberReader:
    """Expose one exact file from a batched native byte stream."""
    def __init__(self, source, size):
        self.source, self.remaining = source, size

    def read(self, size):
        if self.remaining == 0:
            return b''
        block = self.source.read(min(size, self.remaining))
        if not block:
            raise SnapshotError('batched cold object ended before its member boundary')
        self.remaining -= len(block)
        return block


def _stream_inputs(cold: Path, manifest: dict, entries, consume, *, native: bool, completed=lambda _path: False):
    if not native:
        def only_unfinished(stream, row):
            if not completed(row['path']):
                consume(stream, row)
        p._materialize_indexed_files(cold, manifest, entries, None,
            d_primary_native_blob_reads=False, d_primary_native_pack_reads=False,
            durable=False, on_copy=None, durability_queue=None, consume_file=only_unfinished)
        return
    from stockagent.data_sync.windows_cold_io import binary_reader, binary_reader_many
    from stockagent.data_sync.packed_object_placement import is_archive_path
    objects = {str(item['sha256']): item for item in manifest['archive']['objects']}
    for row in entries.chunked_rows():
        if completed(row['path']):
            continue
        with closing(p._ChunkedFileReader(p._iter_chunked_file(cold, objects, row, native=True))) as stream:
            consume(stream, row)

    batch, batch_bytes = [], 0
    def flush():
        nonlocal batch, batch_bytes
        if not batch:
            return
        with binary_reader_many([(path, int(row['size'])) for path, row in batch]) as stream:
            for _path, row in batch:
                consume(_BoundedMemberReader(stream, int(row['size'])), row)
        batch, batch_bytes = [], 0

    for item in sorted((x for x in objects.values() if x['kind'] == 'blob'), key=lambda x: x['relpath']):
        path = p._read_object_path(cold, item['relpath'], 'object relpath')
        rows = (row for row in entries.object_rows(item['sha256']) if not completed(row['path']))
        first = next(rows, None)
        if first is None:
            continue  # This blob is used only as a previously streamed chunk.
        if item.get('file_count') == 1 and int(item['bytes']) <= 8*1024**3:
            if next(rows, None) is not None:
                raise SnapshotError('blob member count differs from its inventory')
            row = first
            if is_archive_path(path):
                flush()
                with binary_reader(path) as stream:
                    consume(stream, row)
                continue
            if batch and (len(batch) == 16 or batch_bytes + int(row['size']) > 16*1024**3):
                flush()
            batch.append((path, row))
            batch_bytes += int(row['size'])
        else:
            flush()
            for row in itertools.chain((first,), rows):
                with binary_reader(path) as stream:
                    consume(stream, row)
    flush()
    # Reuse the existing bounded native pack batcher: it fills up to 128 MiB
    # with one Windows process and validates each original encoded SHA before
    # exposing ZIP bytes. Do not prefetch packs wholly covered by checkpoints.
    from stockagent.data_sync.packed_verification_sources import PackSources
    packs = [item for item in sorted((x for x in objects.values() if x['kind'] == 'pack'),
                                     key=lambda x: x['relpath'])
             if any(not completed(row['path']) for row in entries.object_rows(item['sha256']))]
    sources = PackSources(cold, packs, native=True)
    try:
        for item in packs:
            rows = (row for row in entries.object_rows(item['sha256']) if not completed(row['path']))
            with sources.open(item) as source, zipfile.ZipFile(source, mode='r') as archive:
                for row in rows:
                    with archive.open(str(row['storage']['member']), mode='r') as stream:
                        consume(stream, row)
    finally:
        sources.close()


def _reset_unfinished(stage: Path, retained=()):
    """Delete only unfinished E scratch; keep journalled complete packages."""
    retained = set(retained) | {'intent.json', 'checkpoint.sqlite', 'checkpoint.sqlite-journal'}
    permitted = re.compile(r'(?:intent\.json|checkpoint\.sqlite(?:-journal)?|checkpoint-[0-9a-f]{32}\.partial\.sqlite(?:-journal)?|pack-[0-9a-f]{32}\.partial|pack-[0-9a-f]{64}\.zip|inventory-[0-9a-f]{32}\.partial|inventory-[0-9a-f]{64}\.jsonl\.gz)')
    for path in stage.iterdir():
        if (not permitted.fullmatch(path.name) or path.is_symlink() or not path.is_file()
                or path.stat().st_nlink != 1):
            raise SnapshotError('unrecognised interrupted streaming scratch; retained')
        if path.name not in retained:
            path.unlink()
    p._fsync_directory(stage)


def _checkpoint_mapped_inventory(original, checkpoint):
    """Merge two ordered indexes without copying or randomly probing every row."""
    members = iter(checkpoint.ordered_members())
    current = next(members, None)
    for row in original:
        entry = p._entry_from_record(row)
        if entry.kind == 'file':
            key = checkpoint._key(entry.path)
            if (current is None or current[0] != key or current[2] != entry.size
                    or not isinstance(current[1], str)):
                raise SnapshotError('streaming checkpoint does not exactly match source inventory')
            digest = p._validate_hash(current[1], 'checkpoint package SHA')
            entry.storage = {'kind': 'pack', 'member': entry.path, 'object_sha256': digest}
            current = next(members, None)
        yield entry
    if current is not None:
        raise SnapshotError('streaming checkpoint contains an extra source member')


def _build(cold: Path, resolved, stage: Path, *, native: bool, progress=None) -> dict:
    manifest = resolved.manifest
    def report_inventory(event):
        if progress is not None:
            phase = {'load-packed-inventory': 'loading_source_inventory',
                     'validate-packed-inventory': 'validating_source_inventory'}.get(event.get('stage'))
            if phase:
                progress({'stage': phase, 'inventory_items_completed': event['completed_items'],
                          'inventory_items_total': event['total_items']})
    with ExitStack() as stack:
        checkpoint = _Checkpoint(stage, manifest['dataset'], resolved.manifest_sha256)
        stack.callback(checkpoint.close)
        _reset_unfinished(stage, (item['name'] for item in checkpoint.packages))
        original = stack.enter_context(p._open_inventory(cold, manifest, progress=report_inventory))
        p._validate_inventory(manifest, original, retain_object_members=False, progress=report_inventory)
        has_completed_packs = bool(checkpoint.packages)
        total = len(original)
        if progress is not None:
            # The loaded source index already supplies the required ordering.
            progress({'stage': 'indexing_source_inventory', 'inventory_items_completed': total,
                      'inventory_items_total': total, 'source_index_reused': True})
        builder = _PackBuilder(stage, None, checkpoint=checkpoint, progress=progress)
        try:
            _stream_inputs(cold, manifest, original, builder.consume, native=native,
                           completed=(lambda path: checkpoint.lookup(path) is not None)
                           if has_completed_packs else (lambda _path: False))
            packages = builder.close()
        except BaseException:
            builder.abort()
            raise
        if (builder.files != int(manifest['source']['files'])
                or builder.bytes != int(manifest['source']['logical_bytes'])):
            raise SnapshotError('streaming repack did not consume the complete fixed inventory')
        inventory_tmp = stage/('inventory-'+uuid.uuid4().hex+'.partial')
        def report_output_inventory(completed):
            if progress is not None:
                progress({'stage': 'writing_E_inventory', 'inventory_items_completed': completed,
                          'inventory_items_total': total})
        report_output_inventory(0)
        inventory_sha = p._write_inventory(inventory_tmp, _checkpoint_mapped_inventory(original, checkpoint),
                                           progress=report_output_inventory)
        inventory = stage/('inventory-'+inventory_sha+'.jsonl.gz')
        os.rename(inventory_tmp, inventory)
        p._fsync_directory(stage)
        checkpoint.close()
        checkpoint_path = _safe_stage_file(stage, 'checkpoint.sqlite')
        body = {'contract': CONTRACT+'.prepared', 'dataset': manifest['dataset'],
                'old_manifest_sha256': resolved.manifest_sha256,
                'old_snapshot_id': manifest['snapshot_id'], 'packages': packages,
                'inventory': {'sha256': inventory_sha, 'name': inventory.name,
                              'bytes': inventory.stat().st_size, 'E_signature': _signature(inventory)},
                'resume_index': {'name': checkpoint_path.name, 'bytes': checkpoint_path.stat().st_size,
                                 'E_signature': _signature(checkpoint_path)},
                'files': builder.files, 'logical_bytes': builder.bytes,
                'source_member_sha_streamed': True, 'destination_independent_readback': False,
                'raw_extraction_tree_created': False, 'observed_at_epoch': time.time()}
        body['identity_sha256'] = _identity(body)
        atomic_write_json(stage/'prepared.json', body, durable=True)
        return body


def _prepared_metadata(stage: Path, dataset: str, expected_sha: str) -> dict:
    path = stage/'prepared.json'
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32*1024**2:
        raise SnapshotError('streaming repack prepared record is missing or redirected')
    value = json.loads(path.read_text())
    if (not isinstance(value, dict) or value.get('contract') != CONTRACT+'.prepared'
            or value.get('identity_sha256') != _identity(value)
            or value.get('dataset') != dataset or value.get('old_manifest_sha256') != expected_sha
            or value.get('destination_independent_readback') is not False
            or value.get('raw_extraction_tree_created') is not False
            or not isinstance(value.get('packages'), list)):
        raise SnapshotError('streaming repack prepared identity differs')
    if value.get('packed_format') == 'same-root-subset-v1':
        from stockagent.data_sync import private_subset_reuse as subset
        candidate_path = stage/'subset-prepared.json'
        if candidate_path.is_symlink() or not candidate_path.is_file():
            raise SnapshotError('same-root subset candidate metadata is missing or redirected')
        candidate = subset._stable_document(candidate_path)
        proof = candidate.get('donor_proof')
        if (candidate.get('contract') != subset.CONTRACT+'.prepared'
                or candidate.get('identity_sha256') != _identity(candidate)
                or candidate.get('identity_sha256') != value.get('candidate_identity_sha256')
                or candidate.get('dataset') != dataset or candidate.get('old_manifest_sha256') != expected_sha
                or subset.REVIEWED_PAIRS.get(dataset) != candidate.get('donor_dataset')
                or candidate.get('donor_dataset') != value.get('donor_dataset')
                or candidate.get('donor_manifest_sha256') != value.get('donor_manifest_sha256')
                or candidate.get('old_snapshot_id') != value.get('old_snapshot_id')
                or candidate.get('archive') != value.get('archive')
                or candidate.get('inventory') != value.get('inventory')
                or candidate.get('files') != value.get('files')
                or candidate.get('logical_bytes') != value.get('logical_bytes')
                or candidate.get('source_payload_reread') is not False
                or candidate.get('destination_independent_readback') is not False
                or candidate.get('NAS_accepted') is not False
                or not isinstance(proof, dict) or proof.get('contract') != subset.CONTRACT+'.donor-proof'
                or proof.get('identity_sha256') != _identity(proof)
                or proof.get('dataset') != candidate.get('donor_dataset')
                or proof.get('manifest_sha256') != candidate.get('donor_manifest_sha256')
                or proof.get('source_member_sha_streamed') is not True
                or proof.get('source_layout_confirmed') is not True
                or value.get('source_mode') != subset.SOURCE_MODE
                or value.get('source_inventory_sha_compared') is not True
                or value.get('donor_source_layout_confirmed') is not True
                or value.get('source_member_sha_streamed') is not False
                or value.get('packages') != []):
            raise SnapshotError('same-root subset candidate binding differs')
    elif value.get('packed_format') == 'content-alias-v3':
        candidate_path = stage/'candidate.json'
        if candidate_path.is_symlink() or not candidate_path.is_file() or candidate_path.stat().st_size > 32*1024**2:
            raise SnapshotError('streaming content-alias candidate metadata is missing or redirected')
        candidate = json.loads(candidate_path.read_text())
        if (not isinstance(candidate, dict)
                or candidate.get('contract') != 'stockagent.source_local_content_dedup_candidate.v1.candidate'
                or candidate.get('identity_sha256') != _identity(candidate)
                or candidate.get('identity_sha256') != value.get('candidate_identity_sha256')
                or candidate.get('dataset') != dataset or candidate.get('old_manifest_sha256') != expected_sha
                or candidate.get('old_inventory_sha256') != value.get('old_inventory_sha256')
                or candidate.get('packages') != value['packages']
                or candidate.get('inventory') != value.get('inventory')
                or candidate.get('files') != value.get('files')
                or candidate.get('logical_bytes') != value.get('logical_bytes')
                or candidate.get('unique_content_sha_streamed') is not True
                or candidate.get('all_original_occurrences_reread') is not False
                or value.get('source_member_sha_streamed') is not False
                or value.get('unique_content_sha_streamed') is not True
                or value.get('all_original_occurrences_reread') is not False
                or value.get('candidate_inventory_metadata_validated') is not True):
            raise SnapshotError('streaming content-alias candidate binding differs')
    elif value.get('packed_format') is not None or value.get('source_member_sha_streamed') is not True:
        raise SnapshotError('streaming repack source verification scope differs')
    return value


def _read_prepared(stage: Path, dataset: str, expected_sha: str) -> dict:
    value = _prepared_metadata(stage, dataset, expected_sha)
    for item in [*value['packages'], value['inventory']]:
        source = _safe_stage_file(stage, item['name'])
        if not _signature_matches(source, item.get('E_signature')) or source.stat().st_size != item.get('bytes'):
            raise SnapshotError('completed E package generation changed')
    return value


def activate_content_alias_install(stage: Path, dataset: str, expected_sha: str, *,
                                   reader_contract: str) -> dict:
    """Enroll a fixed prepared v3 release for the existing local installer.

    This metadata-only step does not change a head, remove an object, or
    assert that a remote receiver can read the format.
    """
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'original manifest SHA')
    stage = Path(stage).absolute()
    prepared = _read_prepared(stage, dataset, expected_sha)
    if (reader_contract != 'packed-v3-content-alias'
            or prepared.get('packed_format') != 'content-alias-v3'):
        raise SnapshotError('content-alias install requires explicit v3 local reader enrollment')
    value = {'contract': CONTRACT+'.content-alias-install-v1', 'dataset': dataset,
             'old_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
             'candidate_sha256': prepared['candidate_identity_sha256'], 'reader_contract': reader_contract,
             'source_sha_scope': 'unique-content', 'all_original_occurrences_reread': False,
             'independent_D_readback': False, 'remote_reader_accepted': False}
    path = stage/'alias-install-activation.json'
    if os.path.lexists(path):
        if path.is_symlink() or not path.is_file() or json.loads(path.read_text()) != value:
            raise SnapshotError('content-alias install activation belongs to another generation')
    else:
        atomic_write_json(path, value, durable=True)
    return value


def require_content_alias_install(stage: Path, dataset: str, expected_sha: str, prepared: dict) -> None:
    if prepared.get('packed_format') != 'content-alias-v3':
        return
    path = stage/'alias-install-activation.json'
    expected = {'contract': CONTRACT+'.content-alias-install-v1', 'dataset': dataset,
                'old_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
                'candidate_sha256': prepared['candidate_identity_sha256'],
                'reader_contract': 'packed-v3-content-alias', 'source_sha_scope': 'unique-content',
                'all_original_occurrences_reread': False, 'independent_D_readback': False,
                'remote_reader_accepted': False}
    if (path.is_symlink() or not path.is_file() or path.stat().st_size > 16384
            or json.loads(path.read_text()) != expected):
        raise SnapshotError('content-alias local install is not activated for this fixed generation')


def cleanup_stage(cold: Path, stage: Path, receipt: dict, *, guard=lambda: None) -> dict:
    """Retire only the exact E packages committed to D; keep the small journal.

    This user-requested fast migration checks fixed metadata and generations;
    it does not read back D payloads or assert independent recovery.
    """
    cold, stage = cold.absolute(), stage.absolute()
    dataset = p.validate_slug(receipt.get('dataset'), 'dataset')
    expected = p._validate_hash(receipt.get('old_manifest_sha256'), 'old manifest SHA')
    new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'new manifest SHA')
    snapshot = p.validate_slug(receipt.get('new_snapshot_id'), 'snapshot_id')
    if (receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or p._paths_overlap(cold, stage) or stage.is_symlink()
            or stage.resolve(strict=True) != stage):
        raise SnapshotError('streaming cleanup receipt or stage differs')
    if os.path.lexists(stage/'temporary-authority.json'):
        # Until a separate owner safely retires the temporary authority and
        # its readers, its payloads remain needed even after a D commit.
        raise SnapshotError('temporary E authority must be retired before stage cleanup')
    prepared = _prepared_metadata(stage, dataset, expected)
    journal_path = stage/'install.json'
    if journal_path.is_symlink() or not journal_path.is_file():
        raise SnapshotError('streaming cleanup install journal missing')
    install = json.loads(journal_path.read_text())
    if (install.get('contract') != CONTRACT+'.install' or install.get('state') != 'head_pending'
            or install.get('dataset') != dataset or install.get('old_manifest_sha256') != expected
            or install.get('new_manifest_sha256') != new_sha or install.get('snapshot_id') != snapshot
            or install.get('prepared_sha256') != prepared['identity_sha256']
            or not isinstance(install.get('installed'), list)):
        raise SnapshotError('streaming cleanup install identity differs')
    payloads = [*prepared['packages'], prepared['inventory']]
    members = payloads + ([prepared['resume_index']] if isinstance(prepared.get('resume_index'), dict) else [])
    installed = {row['sha256']: row for row in install['installed']}
    install_refs = ([*prepared['archive']['objects'], prepared['inventory']]
                    if prepared.get('packed_format') == 'same-root-subset-v1' else payloads)
    if len(installed) != len(install_refs) or set(installed) != {row['sha256'] for row in install_refs}:
        raise SnapshotError('streaming cleanup installed object set differs')
    cleanup_path = stage/'cleanup.json'
    with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True):
        guard()
        resolved = p.resolve_packed_snapshot_id(cold, dataset, snapshot, require_objects=False)
        if resolved.manifest_sha256 != new_sha:
            raise SnapshotError('streaming cleanup published manifest differs')
        references = {item['sha256']: item['relpath'] for item in
                      [*resolved.manifest['archive']['objects'], resolved.manifest['archive']['inventory']]}
        if set(references) != set(installed):
            raise SnapshotError('streaming cleanup manifest object set differs')
        for digest, row in installed.items():
            target = p._read_object_path(cold, references[digest], 'installed D object')
            if target.is_symlink() or not target.is_file() or not _signature_matches(target, row.get('D_signature')):
                raise SnapshotError('streaming cleanup installed D generation changed')
        if cleanup_path.exists() or cleanup_path.is_symlink():
            if cleanup_path.is_symlink() or not cleanup_path.is_file():
                raise SnapshotError('streaming cleanup journal redirected')
            cleanup = json.loads(cleanup_path.read_text())
        else:
            cleanup = {'contract': CONTRACT+'.cleanup', 'dataset': dataset, 'old_manifest_sha256': expected,
                       'new_manifest_sha256': new_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'removed': [], 'state': 'removing'}
            atomic_write_json(cleanup_path, cleanup, durable=True)
        names = {row['name'] for row in members}
        if (cleanup.get('contract') != CONTRACT+'.cleanup' or cleanup.get('dataset') != dataset
                or cleanup.get('old_manifest_sha256') != expected or cleanup.get('new_manifest_sha256') != new_sha
                or cleanup.get('prepared_sha256') != prepared['identity_sha256']
                or cleanup.get('state') not in ('removing', 'completed')
                or not isinstance(cleanup.get('removed'), list)
                or len(set(cleanup['removed'])) != len(cleanup['removed'])
                or not set(cleanup['removed']).issubset(names)
                or cleanup.get('pending') not in (None, *names)):
            raise SnapshotError('streaming cleanup journal identity differs')
        for row in members:
            name, path = row['name'], stage/row['name']
            if name in cleanup['removed']:
                if os.path.lexists(path):
                    raise SnapshotError('removed E stage member unexpectedly returned')
                continue
            if not os.path.lexists(path):
                if cleanup.get('pending') != name:
                    raise SnapshotError('E stage member disappeared without cleanup intent')
            else:
                source = _safe_stage_file(stage, name)
                if not _signature_matches(source, row.get('E_signature')):
                    raise SnapshotError('E stage member changed before cleanup')
                cleanup['pending'] = name
                atomic_write_json(cleanup_path, cleanup, durable=True)
                source.unlink()
                p._fsync_directory(stage)
            cleanup['removed'].append(name)
            cleanup.pop('pending', None)
            atomic_write_json(cleanup_path, cleanup, durable=True)
        cleanup.update(state='completed', completed_at_epoch=time.time())
        atomic_write_json(cleanup_path, cleanup, durable=True)
        return {'state': 'completed', 'files_removed': len(members),
                'bytes_removed': sum(item['bytes'] for item in members),
                'independent_D_readback': False}


def _archive(prepared: dict, old_manifest: dict) -> dict:
    if prepared.get('packed_format') == 'same-root-subset-v1':
        archive = prepared.get('archive')
        if (not isinstance(archive, dict) or archive.get('base_snapshot_id') != old_manifest['snapshot_id']
                or archive.get('external_reused_files') != prepared['files']):
            raise SnapshotError('same-root subset archive differs from original source')
        return archive
    content_alias = prepared.get('packed_format') == 'content-alias-v3'
    objects = []
    for item in prepared['packages']:
        digest = p._validate_hash(item['sha256'], 'stream pack SHA')
        objects.append({'bucket': 0, 'bytes': item['bytes'], 'file_count': item['file_count'],
                        'kind': 'pack', 'logical_bytes': item['logical_bytes'],
                        'relpath': p._object_relpath('packs', digest, '.zip').as_posix(), 'sha256': digest,
                        **({'stored_file_count': item['stored_file_count'],
                            'stored_logical_bytes': item['stored_logical_bytes']} if content_alias else {})})
    inv = prepared['inventory']
    inv_sha = p._validate_hash(inv['sha256'], 'stream inventory SHA')
    old = old_manifest['archive']
    archive = {'format': p.ALIAS_ARCHIVE_FORMAT if content_alias else old['format'],
        **({'alias_policy': 'unique-content-v1'} if content_alias else {}),
        'inventory': {'bytes': inv['bytes'], 'format': p.INVENTORY_FORMAT,
        'relpath': p._object_relpath('inventories', inv_sha, '.jsonl.gz').as_posix(), 'sha256': inv_sha},
        'loose_file_threshold_bytes': TARGET_BYTES, 'pack_buckets': 1,
        'new_pack_layout': p.SOURCE_DATASET_PACK_LAYOUT, 'new_pack_target_bytes': TARGET_BYTES,
        'compression': 'deflate-or-store-by-extension', 'compression_level': 6,
        'objects': sorted(objects, key=lambda row: (row['kind'], row['sha256'])),
        'object_count': len(objects), 'stored_bytes': sum(item['bytes'] for item in objects)+inv['bytes'],
        'base_snapshot_id': old_manifest['snapshot_id'], 'reused_files': 0,
        'changed_files': prepared['files'], 'new_object_count': len(objects)+1,
        'new_stored_bytes': sum(item['bytes'] for item in objects)+inv['bytes']}
    if old_manifest['schema_version'] in {p.CHUNKED_SNAPSHOT_SCHEMA_VERSION, p.ALIAS_SNAPSHOT_SCHEMA_VERSION}:
        archive.update(file_chunk_bytes=old['file_chunk_bytes'],
                       file_chunking=old.get('file_chunking', 'fixed-v1'),
                       new_large_file_layout=p.WHOLE_LARGE_FILE_LAYOUT)
    return archive


def validate_new_inventory(cold: Path, manifest: dict, *, progress=None):
    """Validate immutable metadata in one ordered pass without a spill index."""
    def report(event):
        if progress is not None:
            progress({'stage': 'validating_D_inventory',
                      'inventory_items_completed': event['completed_items'],
                      'inventory_items_total': event['total_items']})
    return p._validate_inventory(manifest, p._iter_inventory(cold, manifest),
                                 retain_object_members=False, progress=report if progress is not None else None)


PRIVATE_REPACK_ROOT = Path('/srv/stockagent-packed/.local-state/private-history')
# Existing private heads are automatically discovered without creating a new
# owner or granting replication. These tokens are deliberately excluded while
# their original live-service or concurrent migration owners remain in charge.
PRIVATE_REPACK_EXCLUDED_TOKENS = ('finlab', 'finmind', 'tej', 'factorized', 'tw-daytrade', 'tw-day-trade')
PRIVATE_REPACK_POLICY = 'private-existing-heads-v1'


class _PrivateRepackDatasets:
    """Allow the private owner to resume an exact transaction after a restart.

    Membership is a namespace policy, not proof that a dataset or head exists.
    The publisher and retirement owners still resolve and fence the exact
    private head, receipts, dependencies, pins, processes and volume. Iteration
    discovers only actual, non-redirected private head directories.
    """
    def __contains__(self, name):
        return (isinstance(name, str)
                and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', name) is not None
                and not any(token in name for token in PRIVATE_REPACK_EXCLUDED_TOKENS))

    def __iter__(self):
        root = PRIVATE_REPACK_ROOT/'heads'
        try:
            if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
                raise SnapshotError('private repack head namespace is unavailable or redirected')
            with os.scandir(root) as scan:
                names = sorted(entry.name for entry in scan
                               if entry.is_dir(follow_symlinks=False) and entry.name in self)
            if len(names) > 100000:
                raise SnapshotError('private repack head namespace exceeds the bounded discovery')
        except (OSError, SnapshotError) as error:
            # Losing the private namespace must not block unrelated public work.
            print('private repack discovery deferred: '+str(error), file=sys.stderr)
            names = []
        return iter(names)

    def __len__(self):
        return sum(1 for _ in self)


PRIVATE_REPACK_DATASETS = _PrivateRepackDatasets()


def private_repack_source_allowed(dataset: str, manifest: dict) -> bool:
    """Exclude other owners even when an alias hides their name in the head."""
    if dataset not in PRIVATE_REPACK_DATASETS or not isinstance(manifest, dict):
        return False
    metadata = manifest.get('metadata', {})
    if not isinstance(metadata, dict):
        return False
    for key in ('source_relative', 'source_relative_root', 'artifact_relative_root'):
        value = metadata.get(key, '')
        if (not isinstance(value, str) or
                any(token in value.lower().replace('_', '-') for token in PRIVATE_REPACK_EXCLUDED_TOKENS)):
            return False
    return True


def _repack_pin_name(expected: str, stage: Path, *, scoped: bool = False) -> str:
    if type(scoped) is not bool:
        raise SnapshotError('streaming repack pin scope must be explicit')
    p._validate_hash(expected, 'streaming repack pinned manifest')
    suffix = '.'+hashlib.sha256(os.fsencode(str(stage))).hexdigest() if scoped else ''
    return expected+suffix+'.pin.json'


def _repack_pin(root: Path, dataset: str, expected: str, snapshot_id: str, stage: Path, *, scoped: bool = False):
    """Protect the exact old release while the long source stream runs unlocked."""
    root = Path(root).absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('streaming repack pin root is missing or redirected')
    pin_dir = root/'.stream-repack-pins'/dataset
    pin_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    if pin_dir.resolve(strict=True) != pin_dir or any(path.is_symlink() for path in (pin_dir, pin_dir.parent)):
        raise SnapshotError('streaming repack pin directory redirected')
    path = pin_dir/_repack_pin_name(expected, stage, scoped=scoped)
    value = {'contract': CONTRACT+'.source-pin', 'dataset': dataset,
             'manifest': {'snapshot_id': p.validate_slug(snapshot_id, 'original snapshot')},
             'manifest_sha256': expected, 'stage': str(stage)}
    if os.path.lexists(path):
        if path.is_symlink() or not path.is_file() or json.loads(path.read_text()) != value:
            raise SnapshotError('streaming repack pin belongs to another transaction')
    else:
        atomic_write_json(path, value, durable=True)
    return path, value


def _check_repack_pin(pin):
    if pin is None:
        return
    path, value = pin
    if path.is_symlink() or not path.is_file() or json.loads(path.read_text()) != value:
        raise SnapshotError('streaming repack pin changed before release')


def _clear_repack_pin(pin):
    if pin is None:
        return
    _check_repack_pin(pin)
    path, _ = pin
    path.unlink()
    p._fsync_directory(path.parent)


def _notify_committed_repack(cold, dataset, *, new_object_paths, new_manifest_paths):
    # This authority is explicitly host-private. Its physical repack must not
    # enqueue the public Syncthing path or create an external export intent.
    if cold == PRIVATE_REPACK_ROOT:
        p._check_native_pack_read_authority(cold)
        return
    p._notify_packed_publication(cold, dataset, new_object_paths=new_object_paths,
                                 new_manifest_paths=new_manifest_paths, defer_scan=True)


def _reconcile_owned_incoming(cold: Path, journal: dict, journal_path: Path, prepared: dict) -> bool:
    """Retire only the interrupted incoming inode bound by this install journal.

    The caller holds the shared D install owner and the dataset publisher lock.
    Existing E packs and the D head are untouched. A durable pending-unlink
    marker allows a retry after interruption at either side of unlink.
    """
    pending = journal.get('pending')
    incoming = pending.get('incoming') if isinstance(pending, dict) else None
    if incoming is None:
        return False
    if not isinstance(incoming, dict):
        raise SnapshotError('interrupted incoming identity is malformed')
    name = incoming.get('name')
    if (not isinstance(name, str) or re.fullmatch(r'object-[0-9a-f]{32}\.partial', name) is None
            or any(type(incoming.get(k)) is not int or incoming[k] < 0 for k in ('device', 'inode', 'mode', 'uid'))
            or incoming['inode'] == 0):
        raise SnapshotError('interrupted incoming identity is invalid')
    digest = pending.get('sha256')
    members = [*prepared['packages'], prepared['inventory']]
    matches = [item for item in members if item.get('sha256') == digest]
    if len(matches) != 1 or pending.get('target_absent_before') is not True:
        raise SnapshotError('interrupted incoming is not an exact prepared install')
    directory = cold/'.local-state/incoming'
    if directory.is_symlink() or directory.resolve(strict=True) != directory:
        raise SnapshotError('interrupted incoming directory is redirected')
    retired = journal.get('retired_incoming', [])
    if not isinstance(retired, list) or len(retired) >= 4096:
        raise SnapshotError('interrupted incoming retirement history is invalid')
    path = directory/name
    reclaimed = None
    if os.path.lexists(path):
        info = path.lstat()
        same_device = (info.st_dev == incoming['device'] or _trusted_device_renumbering(path, info.st_dev))
        linked_to_target = False
        if info.st_nlink == 2:
            is_inventory = matches[0] is prepared['inventory']
            rel = p._object_relpath('inventories' if is_inventory else 'packs', digest,
                                    '.jsonl.gz' if is_inventory else '.zip')
            target = cold.joinpath(*rel.parts)
            linked_to_target = (not target.is_symlink() and target.is_file()
                                and target.resolve(strict=True) == target
                                and os.path.samestat(info, target.lstat())
                                and info.st_size == matches[0]['bytes'])
        if (not stat.S_ISREG(info.st_mode) or not same_device or info.st_ino != incoming['inode']
                or info.st_mode != incoming['mode'] or info.st_uid != incoming['uid']
                or (info.st_nlink != 1 and not linked_to_target) or info.st_size > matches[0]['bytes']):
            raise SnapshotError('interrupted incoming inode changed or is shared')
        from stockagent.data_sync.materialized_cache import process_references_many
        if process_references_many([path], inode_keys={(info.st_dev, info.st_ino)}, include_current_process=True):
            raise SnapshotError('interrupted incoming remains in use')
        incoming['retirement_state'] = 'unlink_pending'
        atomic_write_json(journal_path, journal, durable=True)
        current = path.lstat()
        fields = ('st_dev', 'st_ino', 'st_mode', 'st_uid', 'st_nlink', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if any(getattr(current, k) != getattr(info, k) for k in fields):
            raise SnapshotError('interrupted incoming changed before unlink')
        path.unlink()
        p._fsync_directory(directory)
        reclaimed = 0 if linked_to_target else info.st_blocks*512
    journal['retired_incoming'] = retired
    retired.append({'name': name, 'inode': incoming['inode'], 'reclaimed_allocated_bytes': reclaimed})
    pending.pop('incoming')
    atomic_write_json(journal_path, journal, durable=True)
    return True


def repack(cold: Path, stage: Path, dataset: str, expected_sha: str, *,
           node_id='penguin', native=False, guard=lambda: None, progress=None,
           admit_install=lambda _prepared, _journal: None, admit_next=lambda _item: None,
           materialized_pin_root=None, install_lock=None, native_write_reserve_bytes=None,
           before_head_commit=lambda _prepared: None, subset_donor=None,
           subset_layout_check=lambda _cold, _name, _manifest: False):
    """Commit a replacement head without deleting any old release or object."""
    dataset = p.validate_slug(dataset, 'dataset')
    expected_sha = p._validate_hash(expected_sha, 'original manifest SHA')
    cold, stage = cold.absolute(), stage.absolute()
    if subset_donor is not None:
        from stockagent.data_sync import private_subset_reuse as subset
        if (not isinstance(subset_donor, tuple) or len(subset_donor) != 2
                or subset.REVIEWED_PAIRS.get(dataset) != subset_donor[0]
                or cold != PRIVATE_REPACK_ROOT or dataset not in PRIVATE_REPACK_DATASETS):
            raise SnapshotError('same-root subset is not an enrolled private pair')
        donor_name = p.validate_slug(subset_donor[0], 'donor dataset')
        donor_sha = p._validate_hash(subset_donor[1], 'donor manifest SHA')
    if p._paths_overlap(cold, stage) or stage.is_symlink():
        raise SnapshotError('streaming E stage must be outside the cold store')
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('streaming E stage is redirected or not private')
    intent = {'contract': CONTRACT+'.intent', 'dataset': dataset, 'old_manifest_sha256': expected_sha}
    if subset_donor is not None:
        intent.update(subset_donor=donor_name, subset_donor_manifest_sha256=donor_sha)
    intent_path = stage/'intent.json'
    old_subset_intent = None
    if intent_path.exists():
        existing_intent = json.loads(intent_path.read_text()) if not intent_path.is_symlink() else None
        if existing_intent != intent and subset_donor is not None and isinstance(existing_intent, dict):
            old_subset_intent = existing_intent
        elif existing_intent != intent:
            raise SnapshotError('streaming E stage belongs to another transaction')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('streaming E stage is not empty')
        atomic_write_json(intent_path, intent, durable=True)
    guard()
    locks = cold/'.local-state/locks'
    pin = None
    donor_pin = None
    with ExitStack() as stack:
        global_lock = ExitStack()
        stack.callback(global_lock.close)
        global_lock.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
        cross_authority = (subset_donor is not None and donor_name == 'bybit'
            and cold == Path('/srv/stockagent-packed/.local-state/private-history'))
        if cross_authority:
            # A fixed donor can already share its inode with public D. Keep
            # the original cross-authority order used by the link publisher.
            stack.enter_context(p._try_publication_lock(
                Path('/srv/stockagent-packed/.local-state/locks/publish-retention-global.lock'), shared=True))
        for name in sorted({dataset, *([donor_name] if subset_donor is not None else [])}):
            stack.enter_context(p._try_publication_lock(locks/f'publish-{name}.lock'))
        if cross_authority:
            stack.enter_context(p._try_publication_lock(
                Path('/srv/stockagent-packed/.local-state/locks')/f'publish-{donor_name}.lock'))
        latest = p.resolve_latest_packed(cold, dataset, require_objects=False)
        journal_path = stage/'install.json'
        journal = json.loads(journal_path.read_text()) if journal_path.is_file() and not journal_path.is_symlink() else {}
        # Crash after head commit: the immutable new head is already the result.
        if latest.manifest_sha256 != expected_sha:
            if (journal.get('contract') == CONTRACT+'.install' and journal.get('state') == 'head_pending'
                    and journal.get('new_manifest_sha256') == latest.manifest_sha256):
                stack.close()
                archive = latest.manifest['archive']
                paths = tuple(item['relpath'] for item in archive['objects']) + (archive['inventory']['relpath'],)
                _notify_committed_repack(cold, dataset, new_object_paths=paths,
                    new_manifest_paths=(latest.manifest_path.relative_to(cold).as_posix(),))
                if materialized_pin_root is not None:
                    old_id = journal.get('old_snapshot_id')
                    if old_id:
                        pin = _repack_pin(materialized_pin_root, dataset, expected_sha, old_id, stage)
                        _clear_repack_pin(pin)
                    if subset_donor is not None:
                        subset.clear_scoped_donor_pin(materialized_pin_root, donor_name, donor_sha, stage)
                return {'state': 'published', 'dataset': dataset, 'old_manifest_sha256': expected_sha,
                        'new_manifest_sha256': latest.manifest_sha256, 'snapshot_id': latest.manifest['snapshot_id'],
                        'files': latest.manifest['source']['files'], 'logical_bytes': latest.manifest['source']['logical_bytes'],
                        'objects': latest.manifest['archive']['object_count'], 'recovered_after_head_commit': True}
            raise SnapshotError('streaming repack fixed head changed')
        if old_subset_intent is not None:
            subset.supersede_uninstalled_stage(stage, old_subset_intent, intent,
                                               latest.manifest['snapshot_id'], materialized_pin_root)
            journal = {}
        if materialized_pin_root is not None:
            pin = _repack_pin(materialized_pin_root, dataset, expected_sha, latest.manifest['snapshot_id'], stage)
        donor = None
        if subset_donor is not None:
            if (stage/'prepared.json').exists():
                previous = _prepared_metadata(stage, dataset, expected_sha)
                prior_candidate = subset._stable_document(stage/'subset-prepared.json')
                donor_id = p.validate_slug(prior_candidate.get('donor_snapshot_id'), 'fixed donor snapshot')
                if previous.get('donor_manifest_sha256') != donor_sha:
                    raise SnapshotError('same-root subset prepared donor differs')
                donor = p.resolve_packed_snapshot_id(cold, donor_name, donor_id, require_objects=True)
            else:
                donor = p.resolve_latest_packed(cold, donor_name, require_objects=True)
            if (donor.manifest_sha256 != donor_sha
                    or not private_repack_source_allowed(dataset, latest.manifest)
                    or not private_repack_source_allowed(donor_name, donor.manifest)):
                raise SnapshotError('same-root subset donor changed or its source layout is unproven')
            donor_proof = subset_layout_check(cold, donor_name, donor.manifest)
            if (not isinstance(donor_proof, dict) or donor_proof.get('contract') != subset.CONTRACT+'.donor-proof'
                    or donor_proof.get('identity_sha256') != _identity(donor_proof)
                    or donor_proof.get('dataset') != donor_name
                    or donor_proof.get('manifest_sha256') != donor_sha
                    or donor_proof.get('source_member_sha_streamed') is not True
                    or donor_proof.get('source_layout_confirmed') is not True):
                raise SnapshotError('same-root subset donor source proof is unavailable')
            if materialized_pin_root is not None:
                donor_pin = _repack_pin(materialized_pin_root, donor_name, donor_sha,
                                        donor.manifest['snapshot_id'], stage, scoped=True)
        if (stage/'prepared.json').exists():
            prepared = _read_prepared(stage, dataset, expected_sha)
            if subset_donor is not None and (prepared.get('packed_format') != subset.FORMAT
                    or prepared.get('donor_dataset') != donor_name
                    or prepared.get('donor_manifest_sha256') != donor_sha
                    or subset._stable_document(stage/'subset-prepared.json').get('donor_proof') != donor_proof):
                raise SnapshotError('same-root subset prepared donor differs')
        elif subset_donor is not None:
            try:
                candidate = subset.prepare(cold, stage, latest, donor, guard=guard, donor_proof=donor_proof,
                                           progress=progress)
            except SnapshotError as error:
                reason = {'fixed donor does not cover every target path':'missing_path',
                          'fixed donor target metadata or source SHA differs':'different_metadata'}.get(str(error))
                if reason:
                    # Both original publisher locks still fence this attempt.
                    # A fully empty E stage can be closed so the ordinary
                    # repack owner is never trapped by an impossible subset.
                    subset.record_incompatibility(stage, dataset, expected_sha, donor_sha, intent, pin, reason,
                                                  donor_pin=donor_pin)
                raise
            prepared = subset.seal_stream_prepared(stage, candidate)
        else:
            # Dataset owner and a durable retirement-visible pin protect this
            # exact source. Other datasets may retire while the bulk read runs.
            if pin is not None:
                global_lock.close()
            prepared = _build(cold, latest, stage, native=native, progress=progress)
            if pin is not None:
                global_lock.enter_context(p._try_publication_lock(locks/'publish-retention-global.lock', shared=True))
                guard()
                if p.resolve_latest_packed(cold, dataset, require_objects=False).manifest_sha256 != expected_sha:
                    raise SnapshotError('streaming repack fixed head changed during E build')
                _check_repack_pin(pin)
        if (prepared['files'] != latest.manifest['source']['files']
                or prepared['logical_bytes'] != latest.manifest['source']['logical_bytes']):
            raise SnapshotError('streaming prepared counts differ from original manifest')
        require_content_alias_install(stage, dataset, expected_sha, prepared)
        # A crash can interrupt the E-first capacity-relief owner after it
        # removed some old objects. Finish that exact durable transaction
        # before installing or cleaning the only prepared E copy.
        from stockagent.data_sync import staged_head_capacity_relief
        relief = staged_head_capacity_relief.load(stage, dataset, expected_sha, prepared)
        if relief is not None and relief['state'] != 'completed':
            raise SnapshotError('E-first current-head relief is incomplete; resume its owner')
        if install_lock is not None:
            # Different public/private roots share one physical D volume.
            # Serialize the admission and write so independent installers
            # cannot both spend the same observed free capacity. Source
            # preparation remains parallel and never holds this lock.
            stack.enter_context(p._try_publication_lock(Path(install_lock)))
            guard()
        if journal:
            if (journal.get('contract') != CONTRACT+'.install' or journal.get('dataset') != dataset
                    or journal.get('old_manifest_sha256') != expected_sha
                    or journal.get('prepared_sha256') != prepared['identity_sha256']
                    or journal.get('old_snapshot_id', latest.manifest['snapshot_id']) != latest.manifest['snapshot_id']
                    or not isinstance(journal.get('installed'), list)):
                raise SnapshotError('streaming install journal differs from prepared stage')
            if install_lock is not None:
                _reconcile_owned_incoming(cold, journal, journal_path, prepared)
        # The E build is independently useful while D is full. Admission uses
        # actual sealed pack sizes, and failure leaves the original D head and
        # durable E packages available for a later retry.
        admit_install(prepared, journal)
        archive = _archive(prepared, latest.manifest)
        if not journal:
            journal = {'contract': CONTRACT+'.install', 'dataset': dataset,
                       'old_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
                       'old_snapshot_id': latest.manifest['snapshot_id'], 'state': 'installing', 'installed': []}
            atomic_write_json(journal_path, journal, durable=True)
        if (journal.get('contract') != CONTRACT+'.install' or journal.get('dataset') != dataset
                or journal.get('old_manifest_sha256') != expected_sha
                or journal.get('prepared_sha256') != prepared['identity_sha256']
                or journal.get('old_snapshot_id', latest.manifest['snapshot_id']) != latest.manifest['snapshot_id']
                or not isinstance(journal.get('installed'), list)):
            raise SnapshotError('streaming install journal differs from prepared stage')
        if pin is not None and 'old_snapshot_id' not in journal:
            # A retry may inherit an install journal created before source
            # pins were introduced. Bind it before advancing the head.
            journal['old_snapshot_id'] = latest.manifest['snapshot_id']
            atomic_write_json(journal_path, journal, durable=True)
        installed = {item['sha256']: item for item in journal['installed']}
        install_items = [*prepared['packages'], prepared['inventory']]
        install_total = sum(item['bytes'] for item in install_items)
        installed_bytes = installed_count = 0
        def report_install(count):
            if progress is not None:
                progress({'stage': 'installing_D_objects', 'objects_completed': count,
                          'objects_total': len(install_items), 'encoded_bytes_installed': installed_bytes,
                          'encoded_bytes_total': install_total})
        report_install(0)
        for item in install_items:
            digest = item['sha256']
            is_inventory = item is prepared['inventory']
            rel = p._object_relpath('inventories' if is_inventory else 'packs', digest,
                                    '.jsonl.gz' if is_inventory else '.zip')
            target = cold.joinpath(*rel.parts)
            source = _safe_stage_file(stage, item['name'])
            if not _signature_matches(source, item['E_signature']):
                raise SnapshotError('completed E package changed before D install')
            known = installed.get(digest)
            if known is not None:
                if target.is_symlink() or not target.is_file() or not _signature_matches(target, known.get('D_signature')):
                    raise SnapshotError('installed D generation changed during retry')
                installed_bytes += item['bytes']
                installed_count += 1
                report_install(installed_count)
                continue
            guard()
            pending = journal.get('pending')
            if (isinstance(pending, dict) and pending.get('sha256') == digest
                    and pending.get('target_absent_before') is True and os.path.lexists(target)):
                # The content-addressed linker exposes the final name only
                # after hashing and fsync. Recover that precise interrupted
                # intent without rereading or decompressing the D object.
                if (target.is_symlink() or not target.is_file()
                        or target.stat().st_size != item['bytes']):
                    raise SnapshotError('interrupted D install has an invalid final name')
                installed[digest] = {'sha256': digest, 'D_signature': _signature(target)}
                journal['installed'] = list(installed.values())
                journal.pop('pending', None)
                atomic_write_json(journal_path, journal, durable=True)
                installed_bytes += item['bytes']
                installed_count += 1
                report_install(installed_count)
                continue
            # The pending intent is durable before the immutable linker runs.
            admit_next(item)
            journal['pending'] = {'sha256': digest, 'target_absent_before': not os.path.lexists(target)}
            atomic_write_json(journal_path, journal, durable=True)
            def report_copy(sent):
                if progress is not None:
                    progress({'stage': 'streaming_D_object', 'objects_completed': installed_count,
                              'objects_total': len(install_items), 'encoded_bytes_installed': installed_bytes,
                              'encoded_bytes_streamed': installed_bytes+sent, 'encoded_bytes_total': install_total})
            options = ({'native_reserve_bytes': native_write_reserve_bytes}
                       if native_write_reserve_bytes is not None else {})
            if install_lock is not None and cold in p._NATIVE_IMMUTABLE_ROOTS:
                def bind_incoming(path):
                    info = path.lstat()
                    if (path.parent != cold/'.local-state/incoming'
                            or path.parent.is_symlink() or path.parent.resolve(strict=True) != path.parent
                            or re.fullmatch(r'object-[0-9a-f]{32}\.partial', path.name) is None
                            or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != 0):
                        raise SnapshotError('native incoming creation identity differs')
                    journal['pending']['incoming'] = {'name': path.name, 'device': info.st_dev,
                        'inode': info.st_ino, 'mode': info.st_mode, 'uid': info.st_uid}
                    atomic_write_json(journal_path, journal, durable=True)
                options['on_incoming'] = bind_incoming
            try:
                p._install_immutable_object(cold, source, target, expected_sha256=digest,
                                            retain_source=True, on_copy=report_copy, **options)
            except BaseException:
                if install_lock is not None:
                    try:
                        _reconcile_owned_incoming(cold, journal, journal_path, prepared)
                    except (OSError, ValueError, SnapshotError):
                        # Keep the exact durable intent for the next owner.
                        pass
                raise
            installed[digest] = {'sha256': digest, 'D_signature': _signature(target)}
            journal['installed'] = list(installed.values())
            journal.pop('pending', None)
            atomic_write_json(journal_path, journal, durable=True)
            installed_bytes += item['bytes']
            installed_count += 1
            report_install(installed_count)
        if subset_donor is not None:
            # These objects already belong to the fixed donor under the same
            # private D authority. Bind their unchanged inode generations to
            # the original retirement journal; no payload readback is claimed.
            for item in archive['objects']:
                target = p._read_object_path(cold, item['relpath'], 'same-root donor object')
                if target.is_symlink() or not target.is_file() or target.stat().st_size != item['bytes']:
                    raise SnapshotError('same-root donor object changed before commit')
                signature = _signature(target)
                previous = installed.get(item['sha256'])
                if previous is not None and not _signature_matches(target, previous.get('D_signature')):
                    raise SnapshotError('same-root donor object generation changed during retry')
                installed[item['sha256']] = {'sha256': item['sha256'], 'D_signature': signature}
            journal['installed'] = list(installed.values())
            atomic_write_json(journal_path, journal, durable=True)
        guard()
        if progress is not None:
            progress({'stage': 'committing_D_head', 'objects_completed': len(install_items),
                      'objects_total': len(install_items), 'encoded_bytes_installed': install_total,
                      'encoded_bytes_total': install_total})
        now = time.time_ns()
        stamp = p.next_hlc(p._observed_stamps(cold, dataset, now_ns=now,
            max_clock_skew_seconds=p.DEFAULT_MAX_CLOCK_SKEW_SECONDS, require_objects=False), node_id=node_id, now_ns=now)
        seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
        snapshot_id = p.validate_slug(f"{dataset[:40]}-{time.strftime('%Y%m%dT%H%M%S', time.gmtime(seconds))}"
                                      f"{nanos:09d}Z-l{stamp.logical}-{node_id[:40]}-{archive['inventory']['sha256'][:16]}",
                                      'snapshot_id')
        manifest = {'schema_version': (p.ALIAS_SNAPSHOT_SCHEMA_VERSION
                                       if prepared.get('packed_format') == 'content-alias-v3'
                                       else latest.manifest['schema_version']), 'dataset': dataset,
                    'snapshot_id': snapshot_id, 'published_at': p._utc_iso_from_ns(stamp.physical_ns),
                    'hlc': stamp.to_dict(), 'publisher': {'node_id': node_id},
                    'source': latest.manifest['source'], 'archive': archive,
                    'metadata': {**latest.manifest.get('metadata', {}),
                                 'physical_repack_original_manifest_sha256': expected_sha}}
        p._validate_manifest(manifest)
        if prepared.get('packed_format') == 'content-alias-v3':
            # Adoption already validated the complete E inventory against the
            # fixed source. Each E file was generation-fenced and its SHA was
            # checked during the D write above. Do not decompress the same
            # multi-million-row metadata a second time merely to migrate it.
            # This deliberately does not claim an independent D readback.
            if _read_prepared(stage, dataset, expected_sha) != prepared:
                raise SnapshotError('content-alias prepared generation changed before D commit')
        else:
            validate_new_inventory(cold, manifest, progress=progress)
        before_head_commit(prepared)
        if progress is not None:
            progress({'stage': 'committing_D_head', 'objects_completed': len(install_items),
                      'objects_total': len(install_items), 'encoded_bytes_installed': install_total,
                      'encoded_bytes_total': install_total})
        manifest_rel = Path('manifests')/dataset/(snapshot_id+'.json')
        p._ensure_shared_packed_directory(cold, (cold/manifest_rel).parent)
        manifest_sha = p.write_immutable_json(cold/manifest_rel, manifest)
        head = {'schema_version': p.PACKED_HEAD_SCHEMA_VERSION, 'dataset': dataset, 'node_id': node_id,
                'snapshot_id': snapshot_id, 'hlc': stamp.to_dict(), 'updated_at': p._utc_iso_from_ns(stamp.physical_ns),
                'manifest_relpath': manifest_rel.as_posix(), 'manifest_sha256': manifest_sha}
        head_path = cold/'heads'/dataset/(node_id+'.json')
        p._ensure_shared_packed_directory(cold, head_path.parent)
        archived = p._archive_d_primary_head(cold, head_path)
        journal.update(state='head_pending', new_manifest_sha256=manifest_sha, snapshot_id=snapshot_id)
        atomic_write_json(journal_path, journal, durable=True)
        guard()
        before_head_commit(prepared)
        atomic_write_json(head_path, head, durable=True)
    new_paths = tuple(item['relpath'] for item in archive['objects']) + (archive['inventory']['relpath'],)
    if archived:
        new_paths += (archived,)
    _notify_committed_repack(cold, dataset, new_object_paths=new_paths,
                             new_manifest_paths=(manifest_rel.as_posix(),))
    _clear_repack_pin(donor_pin)
    _clear_repack_pin(pin)
    return {'state': 'published', 'dataset': dataset, 'old_manifest_sha256': expected_sha,
            'new_manifest_sha256': manifest_sha, 'snapshot_id': snapshot_id,
            'files': prepared['files'], 'logical_bytes': prepared['logical_bytes'],
            'objects': archive['object_count'], 'recovered_after_head_commit': False}
