"""Fixed offline replay pairs: source-index capture and journalled hardlink retirement.

The caller supplies the canonical ownership, consumer and D-publication guards.
These helpers never discover work from directory names or authorize a live ledger.
"""
from __future__ import annotations

from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import stat
import uuid

from stockagent.data_sync import workspace_relocation as relocation, workspace_source_handoff as handoff
from stockagent.data_sync.packed_snapshots import _safe_relative_path
from stockagent.data_sync.desync_snapshots import SnapshotError


def _namespace(root: Path) -> dict:
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError('offline replay root is redirected or missing')
    result = {}
    device = root.stat().st_dev
    for name, kind, sig in relocation.walk(root):
        if sig[0] != device:
            raise SnapshotError('offline replay crosses a filesystem')
        result[name] = (kind, sig)
    return result


def check_pair(source: Path, mirror: Path) -> dict:
    """Bind both complete namespaces to exactly two names per regular inode."""
    source, mirror = Path(source).absolute(), Path(mirror).absolute()
    if source == mirror or source in mirror.parents or mirror in source.parents:
        raise SnapshotError('offline replay pair roots overlap')
    left, right = _namespace(source), _namespace(mirror)
    lock = left.pop('.download.lock', None)
    if lock is None or lock[0] != 'file' or lock[1][3] != 0 or lock[1][8] != 1:
        raise SnapshotError('offline replay lock is unavailable')
    if set(left) != set(right):
        raise SnapshotError('offline replay mirror namespace differs')
    files = logical = allocated = 0
    for name, (kind, sig) in left.items():
        other_kind, other = right[name]
        if kind != other_kind:
            raise SnapshotError('offline replay mirror kind differs')
        if kind == 'file':
            if sig != other or sig[8] != 2:
                raise SnapshotError('offline replay files are not exact paired hardlinks')
            info = (source/name).lstat()
            files += 1
            logical += sig[3]
            allocated += info.st_blocks * 512
    return {'files': files, 'logical_bytes': logical, 'allocated_bytes': allocated}


def hash_index(source: Path, destination: Path, evidence: Path, *, guard=lambda: None,
               progress=lambda _event: None) -> Path:
    """Build/reuse the relocation index, hashing each fixed source generation."""
    source, destination, evidence = map(lambda p: Path(p).absolute(), (source, destination, evidence))
    _reset_interrupted_index(source, destination, evidence)
    rebind = None
    intent = evidence/'intent.json'
    if intent.is_file() and not intent.is_symlink():
        body = json.loads(intent.read_bytes())
        recorded, current = body.get('source_root_signature'), relocation.signature(source)
        rebind = handoff._source_device_rebind(source, recorded, current)
    if rebind is None:
        db = relocation.index(source, destination, evidence)
    else:
        if (body.get('contract') != 'workspace_exact_relocation_v1' or body.get('state') != 'indexed'
                or body.get('source') != str(source) or body.get('destination') != str(destination)
                or (evidence/'files.sqlite').is_symlink()):
            raise SnapshotError('offline replay remounted index identity differs')
        db = sqlite3.connect(evidence/'files.sqlite')
        seen = 0
        for name, kind, actual in relocation.walk(source):
            row = db.execute('SELECT kind,sig FROM files WHERE path=? COLLATE BINARY', (name,)).fetchone()
            if row is None or row[0] != kind or not handoff._same_source_generation(actual, json.loads(row[1]), rebind):
                db.close()
                raise SnapshotError('offline replay remounted source differs from its index')
            seen += 1
        if seen != db.execute('SELECT count(*) FROM files').fetchone()[0]:
            db.close()
            raise SnapshotError('offline replay remounted source namespace differs')
    try:
        total, logical = db.execute("SELECT count(*),coalesce(sum(bytes),0) FROM files WHERE kind='file'").fetchone()
        done = processed = 0
        for name, encoded, digest, size, state in db.execute(
                "SELECT path,sig,sha,bytes,state FROM files WHERE kind='file' ORDER BY path").fetchall():
            guard()
            path = source/name
            before = relocation.signature(path)
            if (not handoff._same_source_generation(before, json.loads(encoded), rebind)
                    or not stat.S_ISREG(before[2]) or before[3] != size):
                raise SnapshotError('offline replay source changed since indexing')
            if state != 'verified':
                def file_progress(file_bytes):
                    guard()
                    progress({'stage': 'hashing_source', 'files_completed': done, 'files_total': total,
                              'logical_bytes_processed': processed+min(file_bytes, size),
                              'logical_bytes_total': logical})
                digest = relocation.digest(path, no_follow=True, progress=file_progress)
                if relocation.signature(path) != before:
                    raise SnapshotError('offline replay source changed while hashing')
                db.execute("UPDATE files SET sha=?,state='verified' WHERE path=?", (digest, name))
                db.commit()
            elif not isinstance(digest, str) or len(digest) != 64:
                raise SnapshotError('offline replay indexed hash is invalid')
            done += 1
            processed += size
            progress({'stage': 'hashing_source', 'files_completed': done, 'files_total': total,
                      'logical_bytes_processed': processed, 'logical_bytes_total': logical})
        db.commit()
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    finally:
        db.close()
    return evidence/'files.sqlite'


def _reset_interrupted_index(source: Path, destination: Path, evidence: Path) -> None:
    """Discard only our unfinished E metadata; no source or payload is removed."""
    if not os.path.lexists(evidence):
        return
    if evidence.is_symlink() or not evidence.is_dir() or evidence.resolve(strict=True) != evidence:
        raise SnapshotError('offline replay index evidence is redirected')
    intent = evidence/'intent.json'
    if intent.is_symlink():
        raise SnapshotError('offline replay index intent is redirected')
    if intent.exists():
        body = json.loads(intent.read_bytes())
        if body.get('state') != 'indexing':
            return
        current = relocation.signature(source)
        rebind = handoff._source_device_rebind(source, body.get('source_root_signature'), current)
        if (body.get('contract') != 'workspace_exact_relocation_v1'
                or body.get('source') != str(source) or body.get('destination') != str(destination)
                or not handoff._same_source_generation(current, body.get('source_root_signature'), rebind)):
            raise SnapshotError('interrupted offline replay index identity changed')
    if os.path.lexists(destination):
        raise SnapshotError('interrupted offline replay index has an unexpected destination')
    allowed = {'files.sqlite', 'files.sqlite-wal', 'files.sqlite-shm', 'files.sqlite-journal',
               'intent.json', 'progress.json'}
    members = list(evidence.iterdir())
    if any(item.name not in allowed or item.is_symlink() or not item.is_file() for item in members):
        raise SnapshotError('interrupted offline replay index contains unknown members')
    for item in members:
        item.unlink()
    evidence.rmdir()


def _same_after_unlink(actual, expected, rebind=None):
    if not isinstance(actual, list) or not isinstance(expected, list) or len(actual) != 9 or len(expected) != 9:
        return False
    device = actual[0] == expected[0] or rebind is not None and (expected[0], actual[0]) == rebind
    return device and all(actual[i] == expected[i] for i in (1, 2, 3, 4, 6, 7)) and actual[8] == 1


def root_rebind(source: Path, mirror: Path, source_root, mirror_root):
    """Recognize only WSL device renumbering of the fixed C inode pair."""
    actual = []
    for path in (source, mirror):
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISDIR(info.st_mode) or path.resolve(strict=True) != path:
            raise SnapshotError('offline replay retirement root identity changed')
        actual.append([info.st_dev, info.st_ino])
    if actual == [source_root, mirror_root]:
        return None
    if (not isinstance(source_root, list) or not isinstance(mirror_root, list)
            or len(source_root) != 2 or len(mirror_root) != 2
            or source_root[1] != actual[0][1] or mirror_root[1] != actual[1][1]
            or source_root[0] != mirror_root[0] or actual[0][0] != actual[1][0]
            or not handoff._canonical_c_source(source, actual[0][0])):
        raise SnapshotError('offline replay retirement root identity changed')
    return source_root[0], actual[0][0]


def _plan(journal: Path, index: Path, source: Path, mirror: Path, binding: dict, rebind=None) -> None:
    if journal.exists() or journal.is_symlink():
        return
    check_pair(source, mirror)
    temporary = journal.with_name(journal.name+'.'+uuid.uuid4().hex+'.partial')
    try:
        with closing(sqlite3.connect(index)) as fixed, closing(sqlite3.connect(temporary)) as db:
            db.execute('PRAGMA journal_mode=DELETE')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
            db.execute('CREATE TABLE files (path TEXT PRIMARY KEY,sig TEXT NOT NULL,sha TEXT NOT NULL, '
                       'bytes INTEGER NOT NULL,allocated INTEGER NOT NULL,state INTEGER NOT NULL,after TEXT)')
            db.execute('INSERT INTO meta VALUES (?,?)', ('binding', json.dumps(binding, sort_keys=True)))
            for name, sig, sha, size, state in fixed.execute(
                    "SELECT path,sig,sha,bytes,state FROM files WHERE kind='file' AND path!='.download.lock' ORDER BY path"):
                if state != 'verified' or not isinstance(sha, str) or len(sha) != 64:
                    raise SnapshotError('offline replay retirement requires a fixed source hash')
                current = relocation.signature(source/name)
                if not handoff._same_source_generation(current, json.loads(sig), rebind):
                    raise SnapshotError('offline replay source changed before retirement planning')
                allocated = (source/name).lstat().st_blocks * 512
                db.execute('INSERT INTO files VALUES (?,?,?,?,?,0,NULL)', (name, sig, sha, size, allocated))
            db.commit()
        os.replace(temporary, journal)
        _sync_directory(journal.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def retire_pair(source: Path, mirror: Path, index: Path, journal: Path, binding: dict, *,
                guard=lambda: None, consumer_guard=lambda: None, progress=lambda _event: None,
                batch_size=64) -> dict:
    """Remove only fixed paired names after the caller validates a committed D head.

    State is persisted before each unlink. An interrupted first unlink re-hashes
    its surviving name before accepting the changed link count and ctime.
    """
    source, mirror, index, journal = map(lambda p: Path(p).absolute(), (source, mirror, index, journal))
    if not 1 <= batch_size <= 512 or journal.is_symlink():
        raise SnapshotError('offline replay retirement parameters differ')
    rebind = root_rebind(source, mirror, binding.get('source_root'), binding.get('mirror_root'))
    guard()
    consumer_guard()
    _plan(journal, index, source, mirror, binding, rebind)
    with closing(sqlite3.connect(journal)) as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        row = db.execute("SELECT value FROM meta WHERE key='binding'").fetchone()
        if row is None or json.loads(row[0]) != binding:
            raise SnapshotError('offline replay retirement binding differs')
        with closing(sqlite3.connect(f'file:{index}?mode=ro', uri=True)) as fixed:
            originals = fixed.execute("SELECT path,sig,sha,bytes FROM files WHERE kind='file' "
                "AND path!='.download.lock' ORDER BY path").fetchall()
        journal_rows = db.execute('SELECT path,sig,sha,bytes,state FROM files ORDER BY path').fetchall()
        if len(originals) != len(journal_rows):
            raise SnapshotError('offline replay retirement inventory count differs')
        for original, current in zip(originals, journal_rows):
            name = original[0]
            if (_safe_relative_path(name, 'offline replay member').as_posix() != name
                    or tuple(current[:4]) != tuple(original) or current[4] not in (0, 1, 2, 3, 4)):
                raise SnapshotError('offline replay retirement inventory differs from the fixed source')
        for (name,) in db.execute('SELECT path FROM files WHERE state=4'):
            if os.path.lexists(source/name) or os.path.lexists(mirror/name):
                raise SnapshotError('retired offline replay name reappeared')
        total, logical, allocated = db.execute('SELECT count(*),coalesce(sum(bytes),0),coalesce(sum(allocated),0) FROM files').fetchone()
        completed = db.execute('SELECT count(*) FROM files WHERE state=4').fetchone()[0]
        for name, encoded, sha, size, blocks, state, after in db.execute(
                'SELECT path,sig,sha,bytes,allocated,state,after FROM files WHERE state!=4 ORDER BY path').fetchall():
            if completed % batch_size == 0:
                guard()
                consumer_guard()
            expected = json.loads(encoded)
            left, right = source/name, mirror/name
            for path in (left, right):
                if path.parent.resolve(strict=True) != path.parent:
                    raise SnapshotError('offline replay retirement parent redirected')
            if state == 0:
                if (not handoff._same_source_generation(relocation.signature(left), expected, rebind)
                        or not handoff._same_source_generation(relocation.signature(right), expected, rebind)
                        or expected[8] != 2):
                    raise SnapshotError('offline replay paired generation changed before unlink')
                db.execute('UPDATE files SET state=1 WHERE path=?', (name,)); db.commit()
                state = 1
            if state == 1:
                if os.path.lexists(left):
                    if (not handoff._same_source_generation(relocation.signature(left), expected, rebind)
                            or not handoff._same_source_generation(relocation.signature(right), expected, rebind)):
                        raise SnapshotError('offline replay pair changed during retirement')
                    left.unlink()
                    _sync_directory(left.parent)
                    actual = relocation.signature(right)
                    if not _same_after_unlink(actual, expected, rebind):
                        raise SnapshotError('offline replay surviving hardlink changed')
                else:
                    actual = relocation.signature(right)
                    if (not _same_after_unlink(actual, expected, rebind)
                            or relocation.digest(right, no_follow=True, progress=lambda _n: guard()) != sha
                            or relocation.signature(right) != actual):
                        raise SnapshotError('interrupted offline replay survivor changed')
                after = json.dumps(actual)
                db.execute('UPDATE files SET state=2,after=? WHERE path=?', (after, name)); db.commit()
                state = 2
            if state == 2:
                if (os.path.lexists(left) or not handoff._same_source_generation(
                        relocation.signature(right), json.loads(after), rebind)):
                    raise SnapshotError('offline replay mirror changed before final unlink')
                db.execute('UPDATE files SET state=3 WHERE path=?', (name,)); db.commit()
                state = 3
            if state == 3:
                if os.path.lexists(left):
                    raise SnapshotError('offline replay original name reappeared')
                if os.path.lexists(right):
                    if not handoff._same_source_generation(relocation.signature(right), json.loads(after), rebind):
                        raise SnapshotError('offline replay mirror changed during final unlink')
                    right.unlink()
                    _sync_directory(right.parent)
                db.execute('UPDATE files SET state=4 WHERE path=?', (name,)); db.commit()
                completed += 1
                progress({'stage': 'retiring_C_pair', 'files_completed': completed, 'files_total': total})
        guard()
        consumer_guard()
        # Empty directories are harmless if another actor created a new file;
        # never recursively delete an unindexed name.
        for root in (source, mirror):
            for directory, _dirs, _files in os.walk(root, topdown=False, followlinks=False):
                if Path(directory) != root:
                    try:
                        Path(directory).rmdir()
                    except OSError:
                        pass
        db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    return {'state': 'paired_payload_retired', 'files_removed': total,
            'logical_bytes': logical, 'estimated_allocated_bytes_reclaimed': allocated}
