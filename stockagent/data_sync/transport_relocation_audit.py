"""Resumable, read-only comparison of a fixed D-to-E transport copy.

This is an observation of mutable E at read time, not a retirement or NAS
receipt. It can run while the original retirement waits for its owners.
"""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.workspace_relocation import digest, signature

CONTRACT = 'stockagent.transport_relocation_comparison.v1'


def _compare(destination, row):
    relative, expected = row
    parts = PurePosixPath(relative)
    if parts.is_absolute() or not parts.parts or any(part in ('..', '.') for part in parts.parts):
        return relative, 'unavailable'
    target = destination.joinpath(*parts.parts)
    try:
        if target.resolve(strict=False) != target:
            return relative, 'unavailable'
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                return relative, 'unavailable'
            # A one-off verification need not make its entire input hot in
            # the host page cache; this is advisory and never a resource cap.
            if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_NOREUSE'):
                try:
                    os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_NOREUSE)
                except OSError:
                    pass
            h = hashlib.sha256()
            for block in iter(lambda: stream.read(8 * 1024**2), b''):
                h.update(block)
            after = os.fstat(stream.fileno())
        current = target.lstat()
        identity = lambda s: (s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns,
                              s.st_ctime_ns, s.st_uid, s.st_gid, s.st_nlink)
        if identity(before) != identity(after) or identity(after) != identity(current):
            return relative, 'unstable'
        return relative, 'same' if h.hexdigest() == expected else 'different'
    except FileNotFoundError:
        return relative, 'missing'
    except OSError:
        return relative, 'unavailable'


def compare_fixed_copy(evidence: Path, destination: Path, *, workers=4):
    if type(workers) is not int or not 1 <= workers <= 16:
        raise ValueError('invalid bounded comparison worker count')
    destination = destination.absolute()
    if destination.is_symlink() or destination.resolve() != destination:
        raise ValueError('redirected E comparison root')
    source_db = evidence / 'files.sqlite'
    intent_raw = (evidence / 'intent.json').read_bytes()
    copy_raw = (evidence / 'copy-result.json').read_bytes()
    intent, copy = json.loads(intent_raw), json.loads(copy_raw)
    if (intent.get('contract') != 'workspace_exact_relocation_v1'
            or copy.get('state') != 'exact_destination_readback_verified'
            or copy.get('all_source_sha256_and_destination_readback_verified') is not True
            or intent.get('files') != copy.get('files')
            or intent.get('logical_bytes') != copy.get('logical_bytes')
            or intent.get('destination') != str(destination)):
        raise ValueError('fixed copy identity is not fully verified')
    wal = evidence / 'files.sqlite-wal'
    if wal.exists() and wal.stat().st_size:
        raise ValueError('fixed source index has an uncheckpointed journal')
    before = signature(source_db)
    source_hash = digest(source_db)
    if signature(source_db) != before:
        raise ValueError('fixed source index changed during fingerprint')
    identity = hashlib.sha256(intent_raw + b'\0' + copy_raw + b'\0' + source_hash.encode()).hexdigest()
    index = sqlite3.connect(f'file:{source_db}?mode=ro', uri=True)
    audit = sqlite3.connect(evidence / 'comparison.sqlite')
    try:
        audit.execute('PRAGMA journal_mode=WAL')
        audit.execute('CREATE TABLE IF NOT EXISTS metadata (identity TEXT NOT NULL)')
        audit.execute('CREATE TABLE IF NOT EXISTS members (path TEXT PRIMARY KEY, sha TEXT NOT NULL, '
                      'bytes INTEGER NOT NULL, outcome TEXT)')
        previous = audit.execute('SELECT identity FROM metadata').fetchone()
        if previous and previous[0] != identity:
            raise ValueError('existing comparison is bound to a different fixed index')
        if not previous:
            audit.execute('INSERT INTO metadata VALUES (?)', (identity,))
            rows = index.execute("SELECT path,sha,bytes FROM files WHERE kind='file' AND state='verified'")
            audit.executemany('INSERT INTO members(path,sha,bytes) VALUES (?,?,?)', rows)
            audit.commit()
        count, logical = audit.execute('SELECT count(*),coalesce(sum(bytes),0) FROM members').fetchone()
        if count != intent['files'] or logical != intent['logical_bytes']:
            raise ValueError('comparison index differs from verified copy')

        # A minute scheduler observing an already completed audit must retain
        # its original measurement time; it is not a new payload read.
        previous_status = evidence / 'comparison-status.json'
        if previous_status.exists():
            saved = json.loads(previous_status.read_bytes())
            grouped = {name: (n, size) for name, n, size in audit.execute(
                'SELECT outcome,count(*),coalesce(sum(bytes),0) FROM members GROUP BY outcome')}
            if (saved.get('contract') == CONTRACT and saved.get('identity_sha256') == identity
                    and saved.get('state') == 'completed' and None not in grouped
                    and saved.get('files_completed') == count
                    and saved.get('logical_bytes_completed') == logical
                    and all(saved.get(name + '_files') == grouped.get(name, (0, 0))[0]
                            for name in ('same', 'different', 'missing', 'unstable', 'unavailable'))):
                return saved

        def publish(state):
            grouped = {name: (n, size) for name, n, size in audit.execute(
                'SELECT outcome,count(*),coalesce(sum(bytes),0) FROM members GROUP BY outcome')}
            pending_count, pending_bytes = grouped.get(None, (0, 0))
            value = {'contract': CONTRACT, 'state': state, 'identity_sha256': identity,
                     'files_total': count, 'logical_bytes_total': logical,
                     'files_completed': count-pending_count, 'logical_bytes_completed': logical-pending_bytes,
                     'same_files': grouped.get('same', (0, 0))[0],
                     'different_files': grouped.get('different', (0, 0))[0],
                     'missing_files': grouped.get('missing', (0, 0))[0],
                     'unstable_files': grouped.get('unstable', (0, 0))[0],
                     'unavailable_files': grouped.get('unavailable', (0, 0))[0],
                     'observed_at_epoch': time.time(), 'source_names_deleted': 0,
                     'retirement_authorized': False, 'NAS_acceptance_claimed': False}
            atomic_write_json(evidence / 'comparison-status.json', value)
            return value

        publish('running')
        cursor = audit.execute('SELECT path,sha FROM members WHERE outcome IS NULL ORDER BY rowid')
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='transport-compare') as pool:
            pending = set()
            exhausted = False
            last_commit = time.monotonic()
            while pending or not exhausted:
                while len(pending) < workers * 2 and not exhausted:
                    row = cursor.fetchone()
                    if row is None:
                        exhausted = True
                    else:
                        pending.add(pool.submit(_compare, destination, row))
                if pending:
                    finished, pending = wait(pending, timeout=2, return_when=FIRST_COMPLETED)
                    for future in finished:
                        path, outcome = future.result()
                        audit.execute('UPDATE members SET outcome=? WHERE path=?', (outcome, path))
                if time.monotonic() - last_commit >= 5:
                    audit.commit()
                    publish('running')
                    last_commit = time.monotonic()
        cursor.close()
        audit.commit()
        return publish('completed')
    finally:
        index.close()
        audit.close()
