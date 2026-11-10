"""Resumable exact copies into E, with a C-side SQLite evidence index.

This is placement proof, not cold/NAS acceptance. It never removes sources.
Bounded streaming avoids a second in-memory million-file inventory. Copy+SHA
shares one source read; a separate destination read verifies persisted bytes.
"""
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from functools import partial
from itertools import islice
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
import time
import uuid

from downloader.artifact_io import atomic_write_json


def signature_from_stat(s):
    """Build the stable signature from an already captured lstat result."""
    return [s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_uid, s.st_gid, s.st_nlink]


def signature(path):
    return signature_from_stat(path.lstat())


def digest(path, *, progress=None, no_follow=False):
    h = hashlib.sha256()
    count = 0
    reader = os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') if no_follow else path.open('rb')
    with reader as f:
        # A tiny receipt must not allocate/release an 8-MiB buffer twice per
        # read. The full file is still read, including growth beyond this hint.
        chunk = min(8 * 1024**2, max(1, os.fstat(f.fileno()).st_size))
        for b in iter(lambda: f.read(chunk), b""):
            h.update(b)
            count += len(b)
            if progress is not None:
                progress(count)
    return h.hexdigest()


def destination_metadata_matches(path, source_signature):
    """Compare portable file metadata, excluding filesystem-specific inode and ctime."""
    try:
        target = signature(path)
    except OSError:
        return False
    return signature_metadata_matches(target, source_signature)


def signature_metadata_matches(target, source_signature):
    return (stat.S_ISREG(target[2])
            and stat.S_IMODE(target[2]) == stat.S_IMODE(source_signature[2])
            and target[3] == source_signature[3]
            and target[4] == source_signature[4]
            and target[6] == source_signature[6] and target[7] == source_signature[7])


def _scope(excluded_subtrees):
    values = tuple(excluded_subtrees)
    if (any(not isinstance(s, str) for s in values)):
        raise ValueError('relocation exclusions require relative directory names')
    values = tuple(sorted(values))
    if (len(values) != len(set(values)) or any(not s
            or Path(s).is_absolute() or '..' in Path(s).parts or Path(s).as_posix() != s for s in values)
            or any(b.startswith(a + '/') for a, b in zip(values, values[1:]))):
        raise ValueError('relocation exclusions require disjoint relative directory names')
    return values


def walk(root, *, excluded_subtrees=(), preserve_symlinks=False):
    excluded = set(_scope(excluded_subtrees))
    pending = [root]
    while pending:
        parent = pending.pop()
        with os.scandir(parent) as entries:
            for item in entries:
                p = Path(item.path)
                s = signature(p)
                relative = p.relative_to(root).as_posix()
                if stat.S_ISDIR(s[2]):
                    if relative not in excluded:
                        pending.append(p)
                    kind = "directory"
                elif stat.S_ISREG(s[2]):
                    kind = "file"
                elif stat.S_ISLNK(s[2]) and preserve_symlinks:
                    kind = "symlink"
                else:
                    raise ValueError("relocation preserves sources and rejects redirected/special transport entries")
                if relative in excluded and kind != 'directory':
                    raise ValueError('relocation exclusion is not an original real directory')
                yield relative, kind, s


def index(source, destination, evidence, *, excluded_subtrees=(), preserve_symlinks=False):
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    db = sqlite3.connect(evidence / "files.sqlite")
    try:
        return _index(db, source, destination, evidence, excluded_subtrees=_scope(excluded_subtrees),
                      preserve_symlinks=preserve_symlinks)
    except BaseException:
        db.close()
        raise


def _index(db, source, destination, evidence, *, excluded_subtrees=(), preserve_symlinks=False):
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)")
    intent = evidence / "intent.json"
    if intent.exists():
        c = json.loads(intent.read_bytes())
        if c["source"] != str(source) or c["destination"] != str(destination) or c["source_root_signature"] != signature(source):
            raise ValueError("relocation source identity changed; preserve previous evidence")
        if (c.get('excluded_subtrees', []) != list(excluded_subtrees)
                or c.get('preserve_symlinks', False) != preserve_symlinks):
            raise ValueError('relocation scope changed; preserve previous evidence')
        if c["state"] != "indexed":
            raise ValueError("interrupted inventory requires explicit replan; no source removed")
        return db
    if destination.exists():
        raise ValueError("relocation requires a fresh E destination")
    c = {"contract": "workspace_exact_relocation_v1", "source": str(source), "destination": str(destination),
         "source_root_signature": signature(source), "state": "indexing", "started_at_epoch": time.time()}
    if excluded_subtrees or preserve_symlinks:
        c.update(contract='workspace_scoped_relocation_v1', excluded_subtrees=list(excluded_subtrees),
                 preserve_symlinks=preserve_symlinks)
    atomic_write_json(intent, c)
    count = 0
    report_at = time.monotonic()
    atomic_write_json(evidence / 'progress.json', {'state':'inventory','entries':0,'observed_at_epoch':time.time()})
    for relative, kind, sig in walk(source, excluded_subtrees=excluded_subtrees,
                                    preserve_symlinks=preserve_symlinks):
        db.execute("INSERT INTO files VALUES(?,?,?,?,?,?)", (relative, kind, json.dumps(sig), None,
                                                            sig[3] if kind == "file" else 0, "pending"))
        count += 1
        if count % 4096 == 0 or time.monotonic()-report_at >= 5:
            db.commit()
            atomic_write_json(evidence / "progress.json", {"state": "inventory", "entries": count,
                                                         "observed_at_epoch": time.time()})
            report_at=time.monotonic()
    db.commit()
    if signature(source) != c["source_root_signature"]:
        raise ValueError("source root changed during relocation inventory")
    if any(db.execute('SELECT kind FROM files WHERE path=?', (name,)).fetchone() != ('directory',)
           for name in excluded_subtrees):
        raise ValueError('relocation exclusion is absent from the original namespace')
    totals = db.execute("SELECT count(*),sum(bytes) FROM files WHERE kind='file'").fetchone()
    c.update(state="indexed", entries=count, files=totals[0], logical_bytes=totals[1] or 0)
    atomic_write_json(intent, c)
    destination.mkdir(mode=0o700, parents=True)
    return db


class _InodeProofCache:
    """Bounded memo of full SHA proofs for unchanged source/destination inodes."""
    def __init__(self, maximum=100000):
        self.maximum = maximum
        self.values = OrderedDict()
        self.lock = threading.Lock()
        self.hits = 0

    def get(self, key):
        with self.lock:
            value = self.values.get(key)
            if value is not None:
                self.values.move_to_end(key)
                self.hits += 1
            return value

    def put(self, key, value):
        with self.lock:
            self.values[key] = value
            self.values.move_to_end(key)
            if len(self.values) > self.maximum:
                self.values.popitem(last=False)


def _copy(row, source, destination, *, native_read=False, progress=None, defer_sync=False, proof_cache=None):
    relative, sig_text, size = row
    expected = json.loads(sig_text)
    origin, target = source / relative, destination / relative
    if signature(origin) != expected:
        raise ValueError("source member changed after inventory")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.path.lexists(target):
        before = signature(target)
        if signature_metadata_matches(before, expected):
            key = ('pair', tuple(expected), tuple(before))
            observed = proof_cache.get(key) if proof_cache is not None else None
            if observed is None:
                original_sha = digest(origin, no_follow=True)
                observed = digest(target, no_follow=True)
                if original_sha != observed:
                    raise ValueError("unproven destination already exists or differs; preserve it")
            if signature(origin) == expected and signature(target) == before:
                if proof_cache is not None:
                    proof_cache.put(key, observed)
                return relative, observed, size
        raise ValueError("unproven destination already exists or differs; preserve it")
    temporary = target.with_name(target.name + ".relocating-" + uuid.uuid4().hex)
    h = hashlib.sha256()
    count = 0
    if native_read and size >= 4 * 1024**2:
        from stockagent.data_sync.windows_cold_io import binary_reader
        reader = binary_reader(origin)
    else:
        descriptor = os.open(origin, os.O_RDONLY | os.O_NOFOLLOW)
        reader = os.fdopen(descriptor, "rb")
    with reader as r, temporary.open("xb") as w:
        chunk = min(8 * 1024**2, max(1, size))
        for block in iter(lambda: r.read(chunk), b""):
            h.update(block)
            count += len(block)
            w.write(block)
            if progress is not None:
                progress(relative, 'copy', count, size)
        w.flush()
        if not defer_sync:
            os.fsync(w.fileno())
    observed = digest(temporary, progress=(lambda count: progress(relative, 'readback', count, size))
                      if progress is not None else None)
    if count != size or signature(origin) != expected or observed != h.hexdigest():
        raise ValueError("full relocation readback or source generation differs; partial retained")
    os.chmod(temporary, stat.S_IMODE(expected[2]))
    os.chown(temporary, expected[6], expected[7])
    os.utime(temporary, ns=(expected[4], expected[4]))
    os.link(temporary, target, follow_symlinks=False)
    temporary.unlink()
    if not defer_sync:
        fd = os.open(target.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return relative, h.hexdigest(), count


def _bounded_as_ready(rows, worker, *, workers):
    """Keep bounded work in flight without a large file blocking the next batch."""
    rows = iter(rows)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="workspace-copy") as pool:
        in_flight = set()
        exhausted = False
        while in_flight or not exhausted:
            while not exhausted and len(in_flight) < workers * 2:
                try:
                    row = next(rows)
                except StopIteration:
                    exhausted = True
                    break
                in_flight.add(pool.submit(worker, row))
            if not in_flight:
                break
            finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in finished:
                yield future.result()


def _copy_as_ready(rows, source, destination, *, workers, native_read, progress=None, defer_sync=False, proof_cache=None):
    worker = partial(_copy, source=source, destination=destination, native_read=native_read,
                     progress=progress, defer_sync=defer_sync, proof_cache=proof_cache)
    yield from _bounded_as_ready(rows, worker, workers=workers)


def _verify_retained(row, destination, proof_cache=None):
    relative, sha, source_sig = row
    target = destination / relative
    try:
        before = signature(target)
        if not signature_metadata_matches(before, json.loads(source_sig)):
            raise ValueError("previously verified E member changed")
        key = ('retained', tuple(before), sha)
        observed = proof_cache.get(key) if proof_cache is not None else None
        if observed is None:
            observed = digest(target, no_follow=True)
        if signature(target) != before or observed != sha:
            raise ValueError("previously verified E member changed")
        if proof_cache is not None:
            proof_cache.put(key, observed)
    except OSError as error:
        raise ValueError("previously verified E member changed") from error
    return relative


def _verification_batches(rows, size=64):
    """Amortize scheduler overhead on millions of small receipt files."""
    while batch := rows.fetchmany(size):
        yield batch


def _verify_batch(rows, destination, proof_cache=None):
    for row in rows:
        _verify_retained(row, destination, proof_cache=proof_cache)
    return len(rows)


def sync_filesystem(path):
    """One Linux filesystem barrier, never a global sync of unrelated disks."""
    import ctypes
    syncfs = ctypes.CDLL(None, use_errno=True).syncfs
    syncfs.argtypes, syncfs.restype = (ctypes.c_int,), ctypes.c_int
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        if syncfs(fd) != 0:
            raise OSError(ctypes.get_errno(), 'relocation filesystem sync failed')
    finally:
        os.close(fd)


def pending_rows(db, *, size=1024):
    """Release each bounded SQLite read before writes/checkpoints, using rowid.

    A full-inventory open cursor prevents WAL reset during a million-file copy.
    Never replace it with OFFSET rescans or an unbounded in-memory inventory.
    """
    after = 0
    while True:
        cursor = db.execute("SELECT rowid,path,sig,bytes FROM files WHERE rowid>? "
                            "AND kind='file' AND state='pending' ORDER BY rowid LIMIT ?", (after, size))
        try:
            batch = cursor.fetchall()
        finally:
            cursor.close()
        if not batch:
            return
        after = batch[-1][0]
        for row in batch:
            yield row[1:]


def _namespace_batches(entries, size):
    """Bounded namespace checks, independent of SQLite's variable limit."""
    entries = iter(entries)
    while batch := list(islice(entries, size)):
        yield batch


def copy(source: Path, destination: Path, evidence: Path, *, workers=4, native_read=False, progress=None,
         excluded_subtrees=(), preserve_symlinks=False, sync_batch_files=0, verify_workers=1,
         deduplicate_hardlink_verification=False):
    source, destination = source.absolute(), destination.absolute()
    if type(workers) is not int or workers < 1:
        raise ValueError("relocation needs at least one worker")
    if type(preserve_symlinks) is not bool:
        raise ValueError('symlink preservation must be explicitly boolean')
    if type(sync_batch_files) is not int or not 0 <= sync_batch_files <= 65536:
        raise ValueError('relocation sync batch must be a bounded nonnegative integer')
    if type(verify_workers) is not int or not 1 <= verify_workers <= 128:
        raise ValueError('relocation verification workers must be 1..128')
    if type(deduplicate_hardlink_verification) is not bool:
        raise ValueError('hardlink verification reuse must be explicitly boolean')
    if (source.is_symlink() or destination.is_symlink() or source.resolve() != source
            or destination.parent.resolve() != destination.parent or destination.is_relative_to(source)):
        raise ValueError("relocation requires unredirected, disjoint roots")
    db = index(source, destination, evidence, excluded_subtrees=excluded_subtrees,
               preserve_symlinks=preserve_symlinks)
    try:
        return _copy_indexed(db, source, destination, evidence, workers=workers, native_read=native_read,
                             progress=progress, sync_batch_files=sync_batch_files, verify_workers=verify_workers,
                             proof_cache=_InodeProofCache() if deduplicate_hardlink_verification else None)
    finally:
        db.close()


def _copy_indexed(db, source, destination, evidence, *, workers, native_read, progress=None, sync_batch_files=0,
                  verify_workers=1, proof_cache=None):
    c = json.loads((evidence / "intent.json").read_bytes())
    walk_options = {'excluded_subtrees': c.get('excluded_subtrees', ()),
                    'preserve_symlinks': c.get('preserve_symlinks', False)}
    # Keep previous committed full-file proofs, but reject corrupt destinations.
    verified = db.execute("SELECT path,sha,sig FROM files WHERE kind='file' AND state='verified'")
    count = 0
    verified_total = db.execute("SELECT count(*) FROM files WHERE kind='file' AND state='verified'").fetchone()[0]
    verify_started, report_at = time.monotonic(), 0.0
    batches = _verification_batches(verified)
    worker = partial(_verify_batch, destination=destination, proof_cache=proof_cache)
    checks = (map(worker, batches) if verify_workers == 1 else
              _bounded_as_ready(batches, worker, workers=verify_workers))
    try:
        for completed in checks:
            count += completed
            if time.monotonic() - report_at >= 5:
                elapsed = time.monotonic() - verify_started
                rate = count / elapsed if elapsed else 0
                atomic_write_json(evidence / 'progress.json', {'state': 'verifying_previously_copied_files',
                    'files_completed': count, 'files_total': verified_total, 'files_per_second': rate,
                    'eta_seconds': (verified_total-count)/rate if rate else None,
                    'observed_at_epoch': time.time(), 'source_files_deleted': 0})
                report_at = time.monotonic()
    finally:
        try:
            if hasattr(checks, 'close'):
                checks.close()
        finally:
            verified.close()
    started = time.monotonic()
    initial = db.execute("SELECT coalesce(sum(bytes),0) FROM files WHERE state='verified'").fetchone()[0]
    completed_bytes = initial
    completed_files = db.execute("SELECT count(*) FROM files WHERE kind='file' AND state='verified'").fetchone()[0]
    initial_files = completed_files
    cursor = pending_rows(db)
    pending_commits, last_commit = 0, time.monotonic()
    sync_batches = 0

    def commit_progress():
        nonlocal sync_batches
        # Completed bytes and namespace must be durable BEFORE verified rows.
        # A failed barrier leaves the transaction uncommitted and preserves all
        # originals. Concurrent unfinished files acquire no completion claim.
        if sync_batch_files and pending_commits:
            sync_filesystem(destination)
            sync_batches += 1
        db.commit()
        elapsed = time.monotonic() - started
        rate = (completed_bytes - initial) / elapsed if elapsed else 0
        file_rate = (completed_files - initial_files) / elapsed if elapsed else 0
        remaining = [value for value in (
            (c['logical_bytes'] - completed_bytes) / rate if rate else None,
            (c['files'] - completed_files) / file_rate if file_rate else None) if value is not None]
        atomic_write_json(evidence / "progress.json", {
            "state": "copying_and_independent_sha_readback", "files_completed": completed_files,
            "files_total": c["files"], "bytes_completed": completed_bytes, "bytes_total": c["logical_bytes"],
            "bytes_per_second": rate, "files_per_second": file_rate,
            "eta_seconds": max(remaining) if remaining else None,
            "sync_batch_files": sync_batch_files, "completed_sync_batches": sync_batches,
            "observed_at_epoch": time.time(), "source_files_deleted": 0})

    commit_progress()
    copies = _copy_as_ready(cursor, source, destination, workers=workers, native_read=native_read,
                            progress=progress, defer_sync=bool(sync_batch_files), proof_cache=proof_cache)
    try:
        for relative, sha, count in copies:
            db.execute("UPDATE files SET sha=?,state='verified' WHERE path=?", (sha, relative))
            completed_bytes += count
            completed_files += 1
            pending_commits += 1
            if pending_commits >= (sync_batch_files or workers * 2) or time.monotonic() - last_commit >= 1:
                commit_progress()
                pending_commits, last_commit = 0, time.monotonic()
    finally:
        try:
            copies.close()  # Join every in-flight copy before closing its evidence database.
        finally:
            cursor.close()  # Release the WAL read snapshot even if a worker raised.
    if pending_commits:
        commit_progress()
    symlinks_changed = False
    for relative, sig_text in db.execute("SELECT path,sig FROM files WHERE kind='symlink'"):
        origin, target = source / relative, destination / relative
        expected = json.loads(sig_text)
        if signature(origin) != expected:
            raise ValueError('source symlink changed after inventory')
        value = os.readlink(origin)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.path.lexists(target):
            if not target.is_symlink() or os.readlink(target) != value:
                raise ValueError('destination symlink differs; preserve it')
        else:
            os.symlink(value, target)
        os.chown(target, expected[6], expected[7], follow_symlinks=False)
        os.utime(target, ns=(expected[4], expected[4]), follow_symlinks=False)
        if signature(origin) != expected or os.readlink(target) != value:
            raise ValueError('source or destination symlink changed during relocation')
        db.execute("UPDATE files SET state='verified',sha=? WHERE path=?",
                   (hashlib.sha256(os.fsencode(value)).hexdigest(), relative))
        symlinks_changed = True
    if symlinks_changed:
        sync_filesystem(destination)
    db.commit()
    # Verify exact namespace and every fixed source generation after all reads.
    seen = 0
    scan_report = 0.0
    scan_size = min(128, db.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
    if scan_size < 1:
        raise ValueError('SQLite cannot bind a namespace path')
    for batch in _namespace_batches(walk(source, **walk_options), scan_size):
        marks = ','.join('?' for _ in batch)
        indexed = {row[0]:row[1:] for row in db.execute(
            'SELECT path,kind,sig,state FROM files WHERE path IN ('+marks+')',
            [relative for relative, _, _ in batch])}
        for relative, kind, sig in batch:
            row = indexed.get(relative)
            if not row or row[:2] != (kind, json.dumps(sig)) or (kind in {"file", "symlink"} and row[2] != "verified"):
                raise ValueError("source namespace changed after relocation")
            seen += 1
        if time.monotonic() - scan_report >= 5:
            atomic_write_json(evidence/'progress.json', {'state':'verifying_source_namespace',
                'entries_completed':seen, 'entries_total':c['entries'],
                'observed_at_epoch':time.time(), 'source_files_deleted':0})
            scan_report = time.monotonic()
    if seen != c["entries"] or signature(source) != c["source_root_signature"]:
        raise ValueError("source exact entry set or root generation differs")
    directory_total = db.execute("SELECT count(*) FROM files WHERE kind='directory'").fetchone()[0]
    directories = db.execute("SELECT path,sig FROM files WHERE kind='directory' ORDER BY length(path) DESC")
    directory_done, directory_report = 0, 0.0
    try:
        for relative, text in directories:
            p = destination / relative
            p.mkdir(mode=0o700, parents=True, exist_ok=True)
            s = json.loads(text)
            os.chmod(p, stat.S_IMODE(s[2]))
            os.chown(p, s[6], s[7])
            os.utime(p, ns=(s[4], s[4]))
            directory_done += 1
            if time.monotonic() - directory_report >= 5 or directory_done == directory_total:
                atomic_write_json(evidence/'progress.json', {'state': 'applying_destination_directory_metadata',
                    'entries_completed': directory_done, 'entries_total': directory_total,
                    'observed_at_epoch': time.time(), 'source_files_deleted': 0})
                directory_report = time.monotonic()
    finally:
        directories.close()
    root_sig = c['source_root_signature']
    os.chmod(destination, stat.S_IMODE(root_sig[2]))
    os.chown(destination, root_sig[6], root_sig[7])
    os.utime(destination, ns=(root_sig[4], root_sig[4]))
    # Every non-directory destination must have a fixed source member. Partial
    # copies are retained as explicit failed work, never admitted as payload.
    target_count = 0
    scan_report = 0.0
    for batch in _namespace_batches(walk(destination, **walk_options), scan_size):
        marks = ','.join('?' for _ in batch)
        indexed = {row[0]:row[1] for row in db.execute(
            'SELECT path,kind FROM files WHERE path IN ('+marks+')',
            [relative for relative, _, _ in batch])}
        for relative, kind, _ in batch:
            if indexed.get(relative) != kind:
                raise ValueError("destination contains unverified names; preserve them")
            target_count += 1
        if time.monotonic() - scan_report >= 5:
            atomic_write_json(evidence/'progress.json', {'state':'verifying_destination_namespace',
                'entries_completed':target_count, 'entries_total':c['entries'],
                'observed_at_epoch':time.time(), 'source_files_deleted':0})
            scan_report = time.monotonic()
    if target_count != c['entries']:
        raise ValueError("destination exact namespace differs")
    if sync_batch_files:
        sync_filesystem(destination)  # Include empty directories and final portable metadata.
    result = {"contract": c["contract"], "state": "exact_destination_readback_verified",
              "files": c["files"], "logical_bytes": c["logical_bytes"], "source_files_deleted": 0,
              "all_source_sha256_and_destination_readback_verified": not bool(c.get('excluded_subtrees')),
              "selected_source_sha256_and_destination_readback_verified": True,
              "excluded_subtrees": c.get('excluded_subtrees', []),
              "source_namespace_unchanged": True,
              "durability": 'filesystem_barrier_before_verified_commit' if sync_batch_files else 'individual_file_and_directory_fsync',
              "sync_batch_files": sync_batch_files,
              "observed_at_epoch": time.time()}
    if proof_cache is not None:
        result['hardlink_generation_proof_cache_hits'] = proof_cache.hits
    atomic_write_json(evidence / "copy-result.json", result)
    atomic_write_json(evidence / "progress.json", result)
    return result
