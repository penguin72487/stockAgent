"""Read only selected original ZIP members for a deduplicated E candidate.

The original archive's complete encoded SHA is not re-read in this path.
Instead, every selected original's SHA/length is checked while the member is
read, and the original archive generation is fenced before and after each
batch. No selected byte reaches a durable pack checkpoint until the entire
bounded native batch and all of its source fences have succeeded.
"""
from __future__ import annotations

from collections import OrderedDict
import hashlib
import io
from pathlib import Path
import stat
import subprocess
import zipfile

from stockagent.data_sync import packed_snapshots as p, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError

MAX_BATCH_BYTES = 128 * 1024**2
MAX_BATCH_MEMBERS = 2048
MAX_BATCH_PACKS = 16


def _source_members(cold, manifest, entries, selected_paths, completed_paths, guard):
    packs = sorted((item for item in manifest['archive']['objects'] if item['kind'] == 'pack'),
                   key=lambda item: item['relpath'])
    for item in packs:
        if item['bytes'] > 8*1024**3:
            continue  # The existing reader retains oversized source packs.
        rows = [row for row in entries.object_rows(item['sha256'])
                if row['path'] in selected_paths and row['path'] not in completed_paths]
        if not rows:
            continue
        guard()
        path = p._path_under(cold, item['relpath'], 'packed source object')
        before = s._signature(path)
        info = path.lstat()
        if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_size != item['bytes']):
            raise SnapshotError('dedup source ZIP generation or size differs')
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            names = {entry.filename: entry for entry in infos}
            if len(names) != len(infos):
                raise SnapshotError('dedup source ZIP contains duplicate member names')
            wanted = {}
            for row in rows:
                name = row['storage']['member']
                entry = names.get(name)
                if entry is None or name in wanted or entry.file_size != row['size']:
                    raise SnapshotError('dedup source ZIP member metadata differs')
                if entry.flag_bits & 1 or entry.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                    continue  # Leave this member to the existing full-pack reader.
                wanted[name] = row
            ordered = sorted(((names[name], row) for name, row in wanted.items()),
                             key=lambda pair: pair[0].header_offset)
        if not s._signature_matches(path, before):
            raise SnapshotError('dedup source ZIP generation changed during indexing')
        for entry, row in ordered:
            # Oversized individual members retain the original stream path.
            if row['size'] <= MAX_BATCH_BYTES:
                yield path, item['bytes'], before, entry.filename, row


def _batches(members):
    batch, size, paths = [], 0, set()
    for member in members:
        path, _expected, _before, _name, row = member
        count = int(row['size'])
        if count < 0 or count > MAX_BATCH_BYTES:
            raise SnapshotError('dedup ZIP member exceeds its bounded batch')
        if batch and (len(batch) >= MAX_BATCH_MEMBERS or size+count > MAX_BATCH_BYTES
                      or path not in paths and len(paths) >= MAX_BATCH_PACKS):
            yield batch
            batch, size, paths = [], 0, set()
        batch.append(member)
        size += count
        paths.add(path)
    if batch:
        yield batch


def _groups(batch):
    groups = OrderedDict()
    for path, expected, before, name, row in batch:
        key = (path, expected, tuple(before))
        groups.setdefault(key, []).append({'name': name, 'size': int(row['size'])})
    return [{'path': path, 'expected_bytes': expected, 'members': values}
            for (path, expected, _before), values in groups.items()]


def stream(cold: Path, manifest: dict, entries, *, selected_paths: set[str], completed_paths: set[str],
           consume, guard=lambda: None, native=False, batch_reader=None, fallback_on_error=False,
           on_fallback=None) -> set[str]:
    """Return only paths whose exact member bytes reached the pack builder."""
    if batch_reader is None:
        from stockagent.data_sync.windows_cold_io import windows_path
        from stockagent.data_sync.packed_object_placement import has_placed_objects
        if (not native or Path(cold) != Path('/srv/stockagent-packed')
                or windows_path(Path(cold)/'objects') is None or has_placed_objects(cold, manifest['archive']['objects'])):
            return set()  # The existing portable pack reader owns this case.
        from stockagent.data_sync.windows_dedup_zip_io import binary_zip_members
        batch_reader = binary_zip_members
        fallback_on_error = True
    members = _source_members(cold, manifest, entries, selected_paths, completed_paths, guard)
    completed = set()
    for batch in _batches(members):
        guard()
        buffered = []
        # A batch's native process, source signatures, and every member SHA
        # must pass before a later consume() can seal any checkpoint.
        try:
            with batch_reader(_groups(batch)) as stream:
                for _path, _expected, _before, _name, row in batch:
                    digest, remaining, parts = hashlib.sha256(), int(row['size']), []
                    while remaining:
                        block = stream.read(min(remaining, 8*1024**2))
                        if not block or len(block) > remaining:
                            raise SnapshotError('dedup ZIP member stream is truncated or oversized')
                        remaining -= len(block)
                        digest.update(block)
                        parts.append(block)
                        guard()
                    if digest.hexdigest() != row['sha256']:
                        raise SnapshotError('dedup ZIP member checksum mismatch')
                    buffered.append((row, b''.join(parts)))
                if stream.read(1):
                    raise SnapshotError('dedup ZIP member batch has excess bytes')
        except (OSError, SnapshotError, subprocess.SubprocessError) as error:
            if not fallback_on_error or 'day-trade priority window' in str(error).lower():
                raise
            # Nothing from this batch was exposed. Preserve earlier accepted
            # paths and let the original full-pack reader handle the rest.
            if on_fallback is not None:
                on_fallback(type(error).__name__)
            return completed
        for path, _expected, before, _name, _row in batch:
            if not s._signature_matches(path, before):
                raise SnapshotError('dedup source ZIP changed after native member reads')
        for row, payload in buffered:
            consume(io.BytesIO(payload), row)
            completed.add(row['path'])
    return completed
