"""Journalled path handoff for one already copied, fixed sealed package.

The caller owns the original cohort lock and supplies the persistent bind
mount adapter. This operation uses source-streamed SHA and fixed generations;
it does not claim an independent destination readback or a new NAS backup.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import time

from stockagent.data_sync import sealed_workspace_copy as copied, packed_snapshots as p
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json, rename_without_overwrite
from stockagent.data_sync.materialized_cache import process_references_many

CONTRACT = 'stockagent.sealed_workspace_handoff.v1'


def _root_identity(path: Path) -> list[int]:
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise SnapshotError('sealed handoff root is redirected or not owned')
    return [info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid]


def _check_destination(stage: Path, checkpoint: dict, expected: str) -> None:
    data = stage/'data'
    if (checkpoint.get('contract') != copied.CONTRACT or checkpoint.get('state') != 'copied'
            or checkpoint.get('manifest_sha256') != expected or checkpoint.get('destination_readback') is not False
            or checkpoint.get('source_deleted') is not False or not isinstance(checkpoint.get('files'), dict)):
        raise SnapshotError('sealed handoff has no completed source-streamed copy')
    if not isinstance(checkpoint.get('directories'), list):
        raise SnapshotError('sealed handoff copied directories are unavailable')
    expected_dirs = {row['relative']: row['mode'] for row in checkpoint['directories']
                     if isinstance(row, dict) and isinstance(row.get('relative'), str) and type(row.get('mode')) is int}
    if len(expected_dirs) != len(checkpoint['directories']) or '.' not in expected_dirs:
        raise SnapshotError('sealed handoff copied directory metadata differs')
    actual_files, actual_dirs = set(), {}
    for parent, directories, files in os.walk(data, followlinks=False):
        parent = Path(parent)
        info = parent.lstat()
        name = parent.relative_to(data).as_posix()
        if (parent.is_symlink() or not stat.S_ISDIR(info.st_mode) or parent.resolve(strict=True) != parent
                or info.st_dev != data.stat().st_dev):
            raise SnapshotError('sealed handoff destination directory is redirected')
        actual_dirs[name] = stat.S_IMODE(info.st_mode)
        for name in directories:
            if (parent/name).is_symlink():
                raise SnapshotError('sealed handoff destination contains a directory symlink')
        actual_files.update((parent/name).relative_to(data).as_posix() for name in files)
    if actual_files != set(checkpoint['files']) or actual_dirs != expected_dirs:
        raise SnapshotError('sealed handoff destination namespace differs')
    for name, row in checkpoint['files'].items():
        if (not isinstance(name, str) or p._safe_relative_path(name, 'sealed handoff member').as_posix() != name
                or not isinstance(row, dict) or row.get('state') != 'completed'
                or copied._signature(copied._regular(data/name)) != row.get('destination_signature')):
            raise SnapshotError('sealed handoff destination generation changed')


def recover_mount(source: Path, stage: Path, expected: str, *, mount_ensure, mount_matches) -> bool:
    """Repair only the path binding after reboot; never delete source names."""
    source, stage = Path(source).absolute(), Path(stage).absolute()
    expected = p._validate_hash(expected, 'sealed handoff manifest SHA')
    journal_path = stage.parent/'handoff.json'
    if not os.path.lexists(journal_path):
        return False
    if journal_path.is_symlink() or not journal_path.is_file():
        raise SnapshotError('sealed handoff journal is redirected')
    journal = json.loads(journal_path.read_bytes())
    old = source.with_name('.'+source.name+'.pre-e-'+expected[:16])
    if (journal.get('contract') != CONTRACT or journal.get('manifest_sha256') != expected
            or journal.get('source') != str(source) or journal.get('old') != str(old)
            or journal.get('stage') != str(stage)):
        raise SnapshotError('sealed boot handoff binding differs')
    checkpoint = json.loads((stage/'copy-checkpoint.json').read_bytes())
    _check_destination(stage, checkpoint, expected)
    data = stage/'data'
    if _root_identity(data) != journal.get('data_identity'):
        raise SnapshotError('sealed boot destination generation differs')
    state = journal.get('state')
    if state == 'prepared' and not os.path.lexists(old):
        if _root_identity(source) != journal.get('source_identity'):
            raise SnapshotError('sealed boot original source differs')
        return False
    if state not in {'prepared', 'source_renamed', 'mounted_retiring', 'completed'}:
        raise SnapshotError('sealed boot handoff state is invalid')
    if os.path.lexists(old) and _root_identity(old) != journal.get('source_identity'):
        raise SnapshotError('sealed boot retained source identity differs')
    if not source.exists():
        source.mkdir(mode=stat.S_IMODE(journal['source_identity'][2]))
        p._fsync_directory(source.parent)
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError('sealed boot mountpoint is redirected')
    if not mount_matches(data, source) and any(source.iterdir()):
        raise SnapshotError('sealed boot mountpoint is unexpectedly nonempty')
    mount_ensure(data, source)
    if not mount_matches(data, source):
        raise SnapshotError('sealed boot mount does not expose the E directory')
    if state in {'prepared', 'source_renamed'}:
        journal['state'] = 'mounted_retiring'
        atomic_write_json(journal_path, journal)
    return True


def handoff(source: Path, stage: Path, expected: str, *, mount_ensure, mount_matches,
            references=process_references_many, progress=None, guard=lambda: None) -> dict:
    """Resume a fixed copy, mount its E path and retire only its old C names."""
    source, stage = Path(source).absolute(), Path(stage).absolute()
    expected = p._validate_hash(expected, 'sealed handoff manifest SHA')
    if p._paths_overlap(source, stage) or source.parent.is_symlink() or stage.is_symlink():
        raise SnapshotError('sealed handoff paths overlap or are redirected')
    journal_path = stage.parent/'handoff.json'
    old = source.with_name('.'+source.name+'.pre-e-'+expected[:16])
    checkpoint_path = stage/'copy-checkpoint.json'
    if checkpoint_path.is_symlink() or not checkpoint_path.is_file():
        raise SnapshotError('sealed handoff copy checkpoint is unavailable')
    checkpoint = json.loads(checkpoint_path.read_bytes())
    _check_destination(stage, checkpoint, expected)
    data = stage/'data'
    data_identity = _root_identity(data)
    if journal_path.exists() or journal_path.is_symlink():
        if journal_path.is_symlink():
            raise SnapshotError('sealed handoff journal is redirected')
        journal = json.loads(journal_path.read_bytes())
        if (journal.get('contract') != CONTRACT or journal.get('manifest_sha256') != expected
                or journal.get('source') != str(source) or journal.get('old') != str(old)
                or journal.get('stage') != str(stage) or journal.get('data_identity') != data_identity
                or not isinstance(journal.get('files'), dict)
                or set(journal['files']) != set(checkpoint['files'])):
            raise SnapshotError('sealed handoff journal binding differs')
        for name, row in journal['files'].items():
            if (not isinstance(row, dict) or row.get('state') not in {'pending', 'removing', 'removed'}
                    or row.get('signature') != checkpoint['files'][name].get('source_signature')
                    or type(row.get('allocated_bytes')) is not int or row['allocated_bytes'] < 0):
                raise SnapshotError('sealed handoff retirement plan differs from the copy')
    else:
        if os.path.lexists(old):
            raise SnapshotError('sealed handoff old path already exists')
        rows, directories = copied.selection(source, expected)
        if checkpoint.get('source') != str(source) or checkpoint.get('directories') != directories:
            raise SnapshotError('sealed source directory generation differs from copied checkpoint')
        if set(checkpoint['files']) != {row['relative'] for row in rows}:
            raise SnapshotError('sealed handoff logical file set differs')
        if references([source], include_current_process=True):
            raise SnapshotError('sealed source is still referenced by a process')
        files = {}
        for row in rows:
            name, info = row['relative'], copied._regular(source/row['relative'])
            original = checkpoint['files'][name]
            if (copied._signature(info) != original['source_signature']
                    or original['sha256'] != row['sha256'] or original['bytes'] != row['bytes']):
                raise SnapshotError('sealed source generation differs from streamed copy')
            files[name] = {'signature': copied._signature(info), 'allocated_bytes': info.st_blocks*512,
                           'state': 'pending'}
        journal = {'contract': CONTRACT, 'state': 'prepared', 'source': str(source), 'old': str(old),
                   'stage': str(stage), 'manifest_sha256': expected, 'data_identity': data_identity,
                   'source_identity': _root_identity(source), 'directories': directories, 'files': files,
                   'destination_readback': False, 'NAS_acceptance_claimed': False,
                   'source_deleted': False, 'reclaimed_allocated_bytes': 0}
        atomic_write_json(journal_path, journal)
    if journal.get('state') == 'completed':
        mount_ensure(data, source)
        if not mount_matches(data, source) or os.path.lexists(old):
            raise SnapshotError('completed sealed handoff mount or old source differs')
        return journal

    guard()
    if journal.get('state') == 'prepared':
        if os.path.lexists(old):
            if _root_identity(old) != journal['source_identity']:
                raise SnapshotError('sealed handoff old root identity differs')
        else:
            if _root_identity(source) != journal['source_identity']:
                raise SnapshotError('sealed handoff source root changed')
            if references([source], include_current_process=True):
                raise SnapshotError('sealed source gained a process reference')
            rename_without_overwrite(source, old)
            p._fsync_directory(source.parent)
        journal['state'] = 'source_renamed'
        atomic_write_json(journal_path, journal)

    if journal.get('state') == 'source_renamed':
        if not source.exists():
            source.mkdir(mode=stat.S_IMODE(journal['source_identity'][2]))
            p._fsync_directory(source.parent)
        if source.is_symlink() or not source.is_dir():
            raise SnapshotError('sealed handoff mountpoint is redirected')
        if not mount_matches(data, source) and any(source.iterdir()):
            raise SnapshotError('sealed handoff mountpoint is unexpectedly nonempty')
        # The adapter installs and enables its exact persistent mount unit.
        # Until this succeeds the complete C directory remains at `old`.
        try:
            mount_ensure(data, source)
            if not mount_matches(data, source):
                raise SnapshotError('sealed handoff mount does not expose the E directory')
        except BaseException:
            # A normal mount failure must not leave readers pointing at an
            # empty directory. A process crash is resumed from the journal.
            if not mount_matches(data, source) and source.is_dir() and not any(source.iterdir()):
                source.rmdir()
                rename_without_overwrite(old, source)
                p._fsync_directory(source.parent)
                journal['state'] = 'prepared'
                atomic_write_json(journal_path, journal)
            raise
        journal['state'] = 'mounted_retiring'
        atomic_write_json(journal_path, journal)

    if journal.get('state') != 'mounted_retiring' or not mount_matches(data, source):
        raise SnapshotError('sealed handoff has no matching E mount')
    if not os.path.lexists(old) and all(row.get('state') == 'removed' for row in journal['files'].values()):
        journal.update(state='completed', source_deleted=True, mount_handoff_completed=True,
                       observed_at_epoch=time.time())
        atomic_write_json(journal_path, journal)
        return journal
    if _root_identity(old) != journal['source_identity']:
        raise SnapshotError('sealed handoff retained C root identity changed')
    for name in sorted(journal['files']):
        guard()
        row = journal['files'][name]
        path = old/name
        if row.get('state') == 'removed':
            if os.path.lexists(path):
                raise SnapshotError('retired sealed source name reappeared')
            continue
        if row.get('state') == 'removing' and not os.path.lexists(path):
            row['state'] = 'removed'
        else:
            if (row.get('state') not in {'pending', 'removing'}
                    or copied._signature(copied._regular(path)) != row.get('signature')):
                raise SnapshotError('sealed source member changed before retirement')
            if references([old], include_current_process=True):
                raise SnapshotError('sealed source retirement is blocked by a process reference')
            row['state'] = 'removing'
            atomic_write_json(journal_path, journal)
            path.unlink()
            p._fsync_directory(path.parent)
            row['state'] = 'removed'
        journal['reclaimed_allocated_bytes'] = sum(r['allocated_bytes'] for r in journal['files'].values()
                                                  if r.get('state') == 'removed')
        atomic_write_json(journal_path, journal)
        if progress is not None:
            progress({'stage': 'retiring_sealed_C_source',
                      'files_completed': sum(r.get('state') == 'removed' for r in journal['files'].values()),
                      'files_total': len(journal['files']),
                      'bytes_completed': journal['reclaimed_allocated_bytes'],
                      'bytes_total': sum(r['allocated_bytes'] for r in journal['files'].values())})
    for directory in sorted(journal['directories'], key=lambda row: len(Path(row['relative']).parts), reverse=True):
        path = old if directory['relative'] == '.' else old/directory['relative']
        if path.exists():
            if path.is_symlink() or not path.is_dir():
                raise SnapshotError('sealed source directory changed before retirement')
            path.rmdir()  # rejects any unexpected retained file
            p._fsync_directory(path.parent)
    journal.update(state='completed', source_deleted=True, mount_handoff_completed=True,
                   observed_at_epoch=time.time())
    atomic_write_json(journal_path, journal)
    return journal
