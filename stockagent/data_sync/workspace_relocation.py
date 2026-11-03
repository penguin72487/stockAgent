"""Resumable exact copies into E, with a C-side SQLite evidence index.

This is placement proof, not cold/NAS acceptance. It never removes sources.
Bounded streaming avoids a second in-memory million-file inventory. Copy+SHA
shares one source read; a separate destination read verifies persisted bytes.
"""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from downloader.artifact_io import atomic_write_json


def signature(path):
    s = path.lstat()
    return [s.st_dev, s.st_ino, s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_uid, s.st_gid, s.st_nlink]


def digest(path, *, progress=None):
    h = hashlib.sha256()
    count = 0
    with path.open("rb") as f:
        for b in iter(lambda: f.read(8 * 1024**2), b""):
            h.update(b)
            count += len(b)
            if progress is not None:
                progress(count)
    return h.hexdigest()


def destination_metadata_matches(path, source_signature):
    """Compare portable file metadata, excluding filesystem-specific inode and ctime."""
    try:
        target = path.lstat()
    except OSError:
        return False
    return (stat.S_ISREG(target.st_mode)
            and stat.S_IMODE(target.st_mode) == stat.S_IMODE(source_signature[2])
            and target.st_size == source_signature[3]
            and target.st_mtime_ns == source_signature[4]
            and target.st_uid == source_signature[6] and target.st_gid == source_signature[7])


def walk(root):
    pending = [root]
    while pending:
        parent = pending.pop()
        with os.scandir(parent) as entries:
            for item in entries:
                p = Path(item.path)
                s = signature(p)
                if stat.S_ISDIR(s[2]):
                    pending.append(p)
                    kind = "directory"
                elif stat.S_ISREG(s[2]):
                    kind = "file"
                else:
                    raise ValueError("relocation preserves sources and rejects redirected/special transport entries")
                yield p.relative_to(root).as_posix(), kind, s


def index(source, destination, evidence):
    evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
    db = sqlite3.connect(evidence / "files.sqlite")
    try:
        return _index(db, source, destination, evidence)
    except BaseException:
        db.close()
        raise


def _index(db, source, destination, evidence):
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY,kind TEXT,sig TEXT,sha TEXT,bytes INTEGER,state TEXT)")
    intent = evidence / "intent.json"
    if intent.exists():
        c = json.loads(intent.read_bytes())
        if c["source"] != str(source) or c["destination"] != str(destination) or c["source_root_signature"] != signature(source):
            raise ValueError("relocation source identity changed; preserve previous evidence")
        if c["state"] != "indexed":
            raise ValueError("interrupted inventory requires explicit replan; no source removed")
        return db
    if destination.exists():
        raise ValueError("relocation requires a fresh E destination")
    c = {"contract": "workspace_exact_relocation_v1", "source": str(source), "destination": str(destination),
         "source_root_signature": signature(source), "state": "indexing", "started_at_epoch": time.time()}
    atomic_write_json(intent, c)
    count = 0
    report_at = time.monotonic()
    atomic_write_json(evidence / 'progress.json', {'state':'inventory','entries':0,'observed_at_epoch':time.time()})
    for relative, kind, sig in walk(source):
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
    totals = db.execute("SELECT count(*),sum(bytes) FROM files WHERE kind='file'").fetchone()
    c.update(state="indexed", entries=count, files=totals[0], logical_bytes=totals[1] or 0)
    atomic_write_json(intent, c)
    destination.mkdir(mode=0o700, parents=True)
    return db


def _copy(row, source, destination, *, native_read=False, progress=None):
    relative, sig_text, size = row
    expected = json.loads(sig_text)
    origin, target = source / relative, destination / relative
    if signature(origin) != expected:
        raise ValueError("source member changed after inventory")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        if (destination_metadata_matches(target, expected)
                and digest(origin) == digest(target) and signature(origin) == expected):
            return relative, digest(target), size
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
        for block in iter(lambda: r.read(8 * 1024**2), b""):
            h.update(block)
            count += len(block)
            w.write(block)
            if progress is not None:
                progress(relative, 'copy', count, size)
        w.flush()
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
    fd = os.open(target.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return relative, h.hexdigest(), count


def _copy_as_ready(rows, source, destination, *, workers, native_read, progress=None):
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
                in_flight.add(pool.submit(_copy, row, source, destination, native_read=native_read,
                                          progress=progress))
            if not in_flight:
                break
            finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in finished:
                yield future.result()


def copy(source: Path, destination: Path, evidence: Path, *, workers=4, native_read=False, progress=None):
    source, destination = source.absolute(), destination.absolute()
    if type(workers) is not int or workers < 1:
        raise ValueError("relocation needs at least one worker")
    if (source.is_symlink() or destination.is_symlink() or source.resolve() != source
            or destination.parent.resolve() != destination.parent or destination.is_relative_to(source)):
        raise ValueError("relocation requires unredirected, disjoint roots")
    db = index(source, destination, evidence)
    try:
        return _copy_indexed(db, source, destination, evidence, workers=workers, native_read=native_read,
                             progress=progress)
    finally:
        db.close()


def _copy_indexed(db, source, destination, evidence, *, workers, native_read, progress=None):
    c = json.loads((evidence / "intent.json").read_bytes())
    # Keep previous committed full-file proofs, but reject corrupt destinations.
    verified = db.execute("SELECT path,sha,sig FROM files WHERE state='verified'")
    try:
        for relative, sha, source_sig in verified:
            target = destination / relative
            if not destination_metadata_matches(target, json.loads(source_sig)) or digest(target) != sha:
                raise ValueError("previously verified E member changed")
    finally:
        verified.close()
    started = time.monotonic()
    initial = db.execute("SELECT coalesce(sum(bytes),0) FROM files WHERE state='verified'").fetchone()[0]
    completed_bytes = initial
    completed_files = db.execute("SELECT count(*) FROM files WHERE state='verified'").fetchone()[0]
    cursor = db.execute("SELECT path,sig,bytes FROM files WHERE kind='file' AND state='pending' ORDER BY rowid")
    pending_commits, last_commit = 0, time.monotonic()

    def commit_progress():
        db.commit()
        elapsed = time.monotonic() - started
        rate = (completed_bytes - initial) / elapsed if elapsed else 0
        atomic_write_json(evidence / "progress.json", {
            "state": "copying_and_independent_sha_readback", "files_completed": completed_files,
            "files_total": c["files"], "bytes_completed": completed_bytes, "bytes_total": c["logical_bytes"],
            "bytes_per_second": rate, "eta_seconds": (c["logical_bytes"] - completed_bytes) / rate if rate else None,
            "observed_at_epoch": time.time(), "source_files_deleted": 0})

    commit_progress()
    copies = _copy_as_ready(cursor, source, destination, workers=workers, native_read=native_read,
                            progress=progress)
    try:
        for relative, sha, count in copies:
            db.execute("UPDATE files SET sha=?,state='verified' WHERE path=?", (sha, relative))
            completed_bytes += count
            completed_files += 1
            pending_commits += 1
            if pending_commits >= workers * 2 or time.monotonic() - last_commit >= 1:
                commit_progress()
                pending_commits, last_commit = 0, time.monotonic()
    finally:
        try:
            copies.close()  # Join every in-flight copy before closing its evidence database.
        finally:
            cursor.close()  # Release the WAL read snapshot even if a worker raised.
    if pending_commits:
        commit_progress()
    # Verify exact namespace and every fixed source generation after all reads.
    seen = 0
    for relative, kind, sig in walk(source):
        row = db.execute("SELECT kind,sig,state FROM files WHERE path=?", (relative,)).fetchone()
        if not row or row[:2] != (kind, json.dumps(sig)) or (kind == "file" and row[2] != "verified"):
            raise ValueError("source namespace changed after relocation")
        seen += 1
    if seen != c["entries"] or signature(source) != c["source_root_signature"]:
        raise ValueError("source exact entry set or root generation differs")
    directories = db.execute("SELECT path,sig FROM files WHERE kind='directory' ORDER BY length(path) DESC")
    try:
        for relative, text in directories:
            p = destination / relative
            p.mkdir(mode=0o700, parents=True, exist_ok=True)
            s = json.loads(text)
            os.chmod(p, stat.S_IMODE(s[2]))
            os.chown(p, s[6], s[7])
            os.utime(p, ns=(s[4], s[4]))
    finally:
        directories.close()
    # Every non-directory destination must have a fixed source member. Partial
    # copies are retained as explicit failed work, never admitted as payload.
    target_count = 0
    for relative, kind, _ in walk(destination):
        expected = db.execute("SELECT kind FROM files WHERE path=?", (relative,)).fetchone()
        if expected != (kind,):
            raise ValueError("destination contains unverified names; preserve them")
        target_count += 1
    if target_count != c['entries']:
        raise ValueError("destination exact namespace differs")
    result = {"contract": c["contract"], "state": "exact_destination_readback_verified",
              "files": c["files"], "logical_bytes": c["logical_bytes"], "source_files_deleted": 0,
              "all_source_sha256_and_destination_readback_verified": True, "source_namespace_unchanged": True,
              "observed_at_epoch": time.time()}
    atomic_write_json(evidence / "copy-result.json", result)
    atomic_write_json(evidence / "progress.json", result)
    return result
