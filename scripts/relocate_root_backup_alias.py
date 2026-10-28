"""Plan and atomically relocate an allowlisted set of root backups.

The old pathname becomes a compatibility symlink in the same atomic exchange.
No function has production paths as implicit defaults; test callers must pass
their own distinct source and destination parents. No data is deleted.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from stockagent.data_sync.materialized_cache import process_references_many

REPO = Path('/root/stockAgent')
STATE = REPO / 'artifacts/operations/root-backup-consolidation-20261028'
DEST_PARENT = STATE / 'data'
SOURCES = (
    'StockAgent',
    'stockagent-sync-backups',
    'stockAgent-git-recovery-20261015.j0E2PL',
    'stockAgent-github-sync-20261015.LTXWb1',
    'stockAgent-two-node-sync-20261015.Luk1nu',
    'stockAgent.git-recovery-backup-20260620-restore',
    'syncthing-merge-backups',
)


def _checked_parents(source: Path, target: Path, source_parent: Path, target_parent: Path):
    if (not source.is_absolute() or not target.is_absolute() or not source_parent.is_absolute()
            or not target_parent.is_absolute() or source.parent != source_parent
            or target.parent != target_parent or source.name != target.name
            or source_parent == target_parent or source == target):
        raise ValueError('exact, distinct source and destination parents required')
    if source_parent.resolve(strict=True) != source_parent or target_parent.resolve(strict=True) != target_parent:
        raise ValueError('parent path is redirected')
    info = source.lstat()
    if not stat.S_ISDIR(info.st_mode) or source.is_symlink():
        raise ValueError('source must be an actual directory')
    if info.st_dev != target_parent.stat().st_dev:
        raise ValueError('atomic exchange requires a single filesystem')
    try:
        target.lstat()
    except FileNotFoundError:
        pass
    else:
        raise FileExistsError('destination must be absent')
    return info


def _syncdir(parent: Path):
    descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def exchange(source: Path, target: Path, *, source_parent: Path, target_parent: Path,
             expected_directory_id: tuple[int, int]):
    """Swap a directory with a private self-link in one Linux renameat2 call."""
    old = _checked_parents(source, target, source_parent, target_parent)
    if (old.st_dev, old.st_ino) != expected_directory_id:
        raise RuntimeError('source directory identity changed before exchange')
    os.symlink(str(target), target)
    placeholder = target.lstat()
    performed = False
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        fn = libc.renameat2
        fn.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        fn.restype = ctypes.c_int
        if fn(-100, os.fsencode(source), -100, os.fsencode(target), 2) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        performed = True
        _syncdir(source_parent)
        _syncdir(target_parent)
        link, moved = source.lstat(), target.lstat()
        if (not stat.S_ISLNK(link.st_mode) or os.readlink(source) != str(target)
                or (link.st_dev, link.st_ino) != (placeholder.st_dev, placeholder.st_ino)
                or not stat.S_ISDIR(moved.st_mode)
                or (moved.st_dev, moved.st_ino) != (old.st_dev, old.st_ino)):
            raise RuntimeError('atomic exchange identity differs; preserve both paths for inspection')
        return {'source': str(source), 'target': str(target), 'device': old.st_dev,
                'directory_inode': old.st_ino, 'alias_inode': link.st_ino}
    finally:
        if not performed:
            try:
                current = target.lstat()
                if (stat.S_ISLNK(current.st_mode) and (current.st_dev, current.st_ino)
                        == (placeholder.st_dev, placeholder.st_ino)):
                    target.unlink()
                    _syncdir(target_parent)
            except FileNotFoundError:
                pass


def _signature(info):
    return [info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_nlink]


def inventory(root: Path, *, with_sha: bool, workers: int = 4):
    root_info = root.lstat()
    if not stat.S_ISDIR(root_info.st_mode):
        raise ValueError('inventory root must be a directory')
    rows = []
    def fail(error):
        raise error
    for parent, dirs, files in os.walk(root, followlinks=False, onerror=fail):
        for name in dirs + files:
            path = Path(parent) / name
            info = path.lstat()
            if info.st_dev != root_info.st_dev:
                raise ValueError('nested filesystem requires a separate migration')
            if stat.S_ISLNK(info.st_mode):
                raise ValueError('backup contains a symlink; inspect relative semantics first')
            kind = 'dir' if stat.S_ISDIR(info.st_mode) else 'file' if stat.S_ISREG(info.st_mode) else None
            if kind is None:
                raise ValueError('backup contains an unsupported special node')
            rows.append({'path': path.relative_to(root).as_posix(), 'kind': kind,
                         'signature': _signature(info)})
    rows.sort(key=lambda row: row['path'])

    def hash_row(row):
        path = root / row['path']
        fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            if _signature(os.fstat(fd)) != row['signature']:
                raise RuntimeError('file changed before hashing: ' + row['path'])
            digest = hashlib.sha256()
            while block := os.read(fd, 1024 * 1024):
                digest.update(block)
            if _signature(os.fstat(fd)) != row['signature']:
                raise RuntimeError('file changed while hashing: ' + row['path'])
            row['sha256'] = digest.hexdigest()
        finally:
            os.close(fd)

    if with_sha:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(hash_row, (row for row in rows if row['kind'] == 'file')))
    return {'root_signature': _signature(root_info), 'rows': rows}


def _write_new(path: Path, value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode() + b'\n'
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    _syncdir(path.parent)
    return hashlib.sha256(encoded).hexdigest()


def _references(path: Path):
    return process_references_many((path,), limit=100)


def plan(name: str):
    if name not in SOURCES:
        raise ValueError('source is not in the explicit backup list')
    source, target = Path('/root') / name, DEST_PARENT / name
    _checked_parents(source, target, Path('/root'), DEST_PARENT)
    before = inventory(source, with_sha=True)
    refs = _references(source)
    after = inventory(source, with_sha=False)
    without_hash = [{'path': row['path'], 'kind': row['kind'], 'signature': row['signature']}
                    for row in before['rows']]
    if refs or before['root_signature'] != after['root_signature'] or without_hash != after['rows']:
        raise RuntimeError('backup is in use or changed during the full SHA census')
    result = {'contract': 'root-backup-atomic-relocation-v1', 'source': str(source),
              'target': str(target), 'captured_epoch': time.time(), 'inventory': before,
              'process_references': refs, 'payload_deleted': False}
    output = STATE / (name + '.plan.json')
    sha = _write_new(output, result)
    print(json.dumps({'state': 'planned', 'name': name, 'files': sum(r['kind']=='file' for r in before['rows']),
                      'sha256': sha, 'plan': str(output)}, sort_keys=True), flush=True)


def apply(name: str, expected_sha: str):
    if name not in SOURCES or len(expected_sha) != 64:
        raise ValueError('exact allowlisted source and full plan SHA required')
    source, target = Path('/root') / name, DEST_PARENT / name
    plan_path = STATE / (name + '.plan.json')
    raw = plan_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise ValueError('plan identity differs')
    fixed = json.loads(raw)
    if fixed['source'] != str(source) or fixed['target'] != str(target):
        raise ValueError('plan names differ from allowlisted names')
    old = _checked_parents(source, target, Path('/root'), DEST_PARENT)
    current = inventory(source, with_sha=False)
    expected = fixed['inventory']
    without_hash = [{'path': row['path'], 'kind': row['kind'], 'signature': row['signature']}
                    for row in expected['rows']]
    if current['root_signature'] != expected['root_signature'] or current['rows'] != without_hash or _references(source):
        raise RuntimeError('backup changed or is in use; do not relocate')
    intent = STATE / (name + '.intent.json')
    _write_new(intent, {'state': 'prepared', 'plan_sha256': expected_sha, 'source': str(source),
                        'target': str(target), 'directory_device': old.st_dev, 'directory_inode': old.st_ino})
    moved = exchange(source, target, source_parent=Path('/root'), target_parent=DEST_PARENT,
                     expected_directory_id=(old.st_dev, old.st_ino))
    observed = inventory(target, with_sha=True)
    # renameat2 changes the relocated directory's ctime; descendants and all
    # payload bytes must retain the fixed signatures and hashes.
    expected_root, observed_root = list(expected['root_signature']), list(observed['root_signature'])
    expected_root[5] = observed_root[5]
    if expected_root != observed_root or observed['rows'] != expected['rows']:
        _write_new(STATE / (name + '.verification-failed.json'), {'state': 'mismatch', 'move': moved})
        raise RuntimeError('post-exchange identity or SHA differs; preserve both paths')
    receipt = {'contract': 'root-backup-atomic-relocation-v1', 'state': 'verified',
               'plan_sha256': expected_sha, 'move': moved, 'file_count': sum(r['kind']=='file' for r in observed['rows']),
               'payload_deleted': False, 'source_compatibility_alias': True,
               'verified_epoch': time.time()}
    _write_new(STATE / (name + '.receipt.json'), receipt)
    print(json.dumps(receipt, sort_keys=True), flush=True)


def main(argv):
    if len(argv) not in (3, 4) or argv[1] not in ('plan', 'apply'):
        raise ValueError('use plan NAME or apply NAME PLAN_SHA256')
    if STATE.exists() or STATE.is_symlink():
        if STATE.is_symlink() or not STATE.is_dir():
            raise ValueError('state root is redirected')
    else:
        STATE.mkdir(mode=0o700)
    if not DEST_PARENT.exists():
        DEST_PARENT.mkdir(mode=0o700)
    if DEST_PARENT.is_symlink() or not DEST_PARENT.is_dir():
        raise ValueError('destination root is redirected')
    if argv[1] == 'plan' and len(argv) == 3:
        plan(argv[2])
    elif argv[1] == 'apply' and len(argv) == 4:
        apply(argv[2], argv[3])
    else:
        raise ValueError('invalid mode arguments')


if __name__ == '__main__':
    main(sys.argv)
