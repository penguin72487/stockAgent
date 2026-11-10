"""Copy one fixed sealed source package to E without extracting or rereading it.

This is only a resumable source-to-workspace copy. It never changes the
original path, installs a mount, deletes a source or claims D/NAS acceptance.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, rename_without_overwrite
from stockagent.data_sync import packed_snapshots as p

CONTRACT = 'stockagent.sealed_workspace_copy.v1'


def _signature(info):
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_nlink, info.st_uid, info.st_gid]


def _regular(path: Path):
    info = path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
            or info.st_uid != os.geteuid() or path.resolve(strict=True) != path):
        raise SnapshotError('sealed copy member is redirected, shared or not owned')
    return info


def _document(path: Path, limit: int) -> bytes:
    before = _regular(path)
    if before.st_size > limit:
        raise SnapshotError('sealed copy control exceeds its size bound')
    data = path.read_bytes()
    if _signature(before) != _signature(_regular(path)) or len(data) != before.st_size:
        raise SnapshotError('sealed copy control changed while reading')
    return data


def selection(source: Path, expected_manifest_sha: str) -> tuple[list[dict], list[dict]]:
    """Check the fixed manifest/READY and exact closed source namespace."""
    source = Path(source).absolute()
    expected_manifest_sha = p._validate_hash(expected_manifest_sha, 'sealed source manifest SHA')
    if (source.is_symlink() or source.resolve(strict=True) != source or not source.is_dir()
            or source.stat().st_uid != os.geteuid() or source.stat().st_mode & 0o077):
        raise SnapshotError('sealed source root is redirected or not private')
    raw = _document(source/'capture-files.json', 16*1024**2)
    ready = _document(source/'SOURCE-CAPTURE.READY', 1024)
    if hashlib.sha256(raw).hexdigest() != expected_manifest_sha or ready.strip().decode('ascii') != expected_manifest_sha:
        raise SnapshotError('sealed source manifest/READY identity differs')
    doc = json.loads(raw)
    files = doc.get('files') if isinstance(doc, dict) else None
    if not isinstance(files, list) or not 0 < len(files) <= 100_000:
        raise SnapshotError('sealed source member list is invalid')
    names, result = set(), []
    for row in files:
        if not isinstance(row, dict) or not isinstance(row.get('relative'), str):
            raise SnapshotError('sealed source member is invalid')
        name = p._safe_relative_path(row['relative'], 'sealed source member').as_posix()
        if (name != row['relative'] or name in names or name in {'capture-files.json', 'SOURCE-CAPTURE.READY'}
                or type(row.get('bytes')) is not int or row['bytes'] < 0):
            raise SnapshotError('sealed source member name or length differs')
        digest = p._validate_hash(row.get('sha256'), 'sealed source member SHA')
        names.add(name)
        result.append({'relative': name, 'bytes': row['bytes'], 'sha256': digest})
    for name, data in [('capture-files.json', raw), ('SOURCE-CAPTURE.READY', ready)]:
        names.add(name)
        result.append({'relative': name, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    actual, directories_seen = set(), []
    for parent, directories, files in os.walk(source, followlinks=False):
        parent = Path(parent)
        info = parent.lstat()
        directories_seen.append({'relative': parent.relative_to(source).as_posix(),
                                 'mode': stat.S_IMODE(info.st_mode), 'mtime_ns': info.st_mtime_ns})
        for name in directories:
            path = parent/name
            if path.is_symlink() or not path.is_dir() or path.stat().st_dev != source.stat().st_dev:
                raise SnapshotError('sealed source has a redirected or cross-device directory')
        for name in files:
            path = parent/name
            _regular(path)
            actual.add(path.relative_to(source).as_posix())
    if actual != names:
        raise SnapshotError('sealed source namespace differs from its fixed manifest')
    return sorted(result, key=lambda row: row['relative']), sorted(directories_seen, key=lambda row: row['relative'])


def copy(source: Path, stage: Path, expected_manifest_sha: str, *, progress=None, guard=lambda: None) -> dict:
    """Stream/hash once to a private E stage and resume closed file checkpoints."""
    source, stage = Path(source).absolute(), Path(stage).absolute()
    if p._paths_overlap(source, stage):
        raise SnapshotError('sealed workspace stage overlaps its source')
    rows, directories = selection(source, expected_manifest_sha)
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    if stage.is_symlink() or stage.resolve(strict=True) != stage or stat.S_IMODE(stage.stat().st_mode) != 0o700:
        raise SnapshotError('sealed workspace stage is redirected or not private')
    checkpoint_path = stage/'copy-checkpoint.json'
    if checkpoint_path.exists() or checkpoint_path.is_symlink():
        if checkpoint_path.is_symlink():
            raise SnapshotError('sealed copy checkpoint is redirected')
        checkpoint = json.loads(checkpoint_path.read_bytes())
        if (checkpoint.get('contract') != CONTRACT or checkpoint.get('source') != str(source)
                or checkpoint.get('manifest_sha256') != expected_manifest_sha
                or not isinstance(checkpoint.get('files'), dict) or checkpoint.get('directories') != directories):
            raise SnapshotError('sealed copy checkpoint identity differs')
    else:
        if any(stage.iterdir()):
            raise SnapshotError('sealed workspace stage is not empty')
        checkpoint = {'contract': CONTRACT, 'source': str(source), 'manifest_sha256': expected_manifest_sha,
                      'files': {}, 'directories': directories, 'state': 'copying',
                      'source_deleted': False, 'destination_readback': False}
        atomic_write_json(checkpoint_path, checkpoint)
    data = stage/'data'
    data.mkdir(mode=0o700, exist_ok=True)
    if data.is_symlink() or data.resolve(strict=True) != data:
        raise SnapshotError('sealed workspace data directory is redirected')
    for directory in directories:
        target_dir = data if directory['relative'] == '.' else data/directory['relative']
        target_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if target_dir.is_symlink() or target_dir.resolve(strict=True) != target_dir:
            raise SnapshotError('sealed destination directory is redirected')
    total = sum(row['bytes'] for row in rows)
    completed_bytes = completed_files = 0
    for row in rows:
        guard()
        name = row['relative']
        origin, target = source/name, data/name
        before = _signature(_regular(origin))
        if before[3] != row['bytes']:
            raise SnapshotError('sealed source file size changed')
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if target.parent.resolve(strict=True) != target.parent:
            raise SnapshotError('sealed destination parent is redirected')
        partial = stage/('.copy-'+hashlib.sha256(name.encode()).hexdigest()+'.partial')
        saved = checkpoint['files'].get(name)
        if saved is not None:
            if (not isinstance(saved, dict) or saved.get('source_signature') != before
                    or saved.get('sha256') != row['sha256'] or saved.get('bytes') != row['bytes']
                    or saved.get('state') not in {'prepared', 'completed'}):
                raise SnapshotError('sealed workspace checkpoint differs from source')
            current = target if target.exists() and not target.is_symlink() else partial
            actual_signature = _signature(_regular(current))
            recorded = saved.get('destination_signature')
            # An atomic rename may update only ctime before the subsequent
            # completed checkpoint. The private exact inode and all other
            # generation fields must still match for this narrow crash gap.
            renamed_prepared = (saved['state'] == 'prepared' and current == target
                                and isinstance(recorded, list) and len(recorded) == 9
                                and actual_signature[:5] == recorded[:5]
                                and actual_signature[6:] == recorded[6:]
                                and actual_signature[5] >= recorded[5])
            if actual_signature != recorded and not renamed_prepared:
                raise SnapshotError('sealed workspace checkpoint destination changed')
            if current == partial:
                if saved['state'] != 'prepared' or os.path.lexists(target):
                    raise SnapshotError('sealed workspace prepared destination is inconsistent')
                rename_without_overwrite(partial, target)
                p._fsync_directory(target.parent)
            if saved['state'] != 'completed':
                saved['state'] = 'completed'
                saved['destination_signature'] = _signature(_regular(target))
                atomic_write_json(checkpoint_path, checkpoint)
        else:
            if os.path.lexists(target):
                raise SnapshotError('sealed workspace has an unjournaled target')
            if os.path.lexists(partial):
                _regular(partial)
                partial.unlink()  # only this transaction's unsealed temporary
            digest, count, last = hashlib.sha256(), 0, 0.0
            descriptor = os.open(origin, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            with os.fdopen(descriptor, 'rb') as reader, partial.open('xb') as writer:
                if _signature(os.fstat(reader.fileno())) != before:
                    raise SnapshotError('sealed source changed while opening')
                os.fchmod(writer.fileno(), stat.S_IMODE(before[2]))
                while block := reader.read(8*1024**2):
                    guard()
                    digest.update(block)
                    count += len(block)
                    if count > row['bytes']:
                        raise SnapshotError('sealed source member grew while copying')
                    writer.write(block)
                    if progress is not None and time.monotonic()-last >= 3:
                        last = time.monotonic()
                        progress({'stage': 'copying_sealed_source', 'files_completed': completed_files,
                                  'files_total': len(rows), 'bytes_completed': completed_bytes+count,
                                  'bytes_total': total})
                writer.flush()
                os.fsync(writer.fileno())
                if _signature(os.fstat(reader.fileno())) != before:
                    raise SnapshotError('sealed source changed while reading')
            if (count != row['bytes'] or digest.hexdigest() != row['sha256']
                    or _signature(_regular(origin)) != before or _regular(partial).st_size != count):
                raise SnapshotError('sealed source SHA/length/generation changed while copying')
            os.utime(partial, ns=(before[4], before[4]), follow_symlinks=False)
            descriptor = os.open(partial, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)  # include the final copied mtime in the durability barrier
            finally:
                os.close(descriptor)
            saved = {'state': 'prepared', 'source_signature': before, 'sha256': row['sha256'],
                     'bytes': count, 'destination_signature': _signature(_regular(partial))}
            checkpoint['files'][name] = saved
            atomic_write_json(checkpoint_path, checkpoint)
            rename_without_overwrite(partial, target)
            p._fsync_directory(target.parent)
            saved['state'] = 'completed'
            saved['destination_signature'] = _signature(_regular(target))
            atomic_write_json(checkpoint_path, checkpoint)
        completed_bytes += row['bytes']
        completed_files += 1
        if progress is not None:
            progress({'stage': 'copying_sealed_source', 'files_completed': completed_files,
                      'files_total': len(rows), 'bytes_completed': completed_bytes, 'bytes_total': total})
    if selection(source, expected_manifest_sha) != (rows, directories):
        raise SnapshotError('sealed source namespace changed during copying')
    for directory in sorted(directories, key=lambda row: row['relative'].count('/'), reverse=True):
        target_dir = data if directory['relative'] == '.' else data/directory['relative']
        os.chmod(target_dir, directory['mode'], follow_symlinks=False)
        os.utime(target_dir, ns=(directory['mtime_ns'], directory['mtime_ns']), follow_symlinks=False)
    checkpoint.update(state='copied', files_completed=completed_files, bytes_completed=completed_bytes,
                      observed_at_epoch=time.time())
    atomic_write_json(checkpoint_path, checkpoint)
    return checkpoint
