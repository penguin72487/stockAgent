"""Stage one locked source generation on E with a single source-byte read.

This is an explicitly weaker placement contract than workspace_relocation.copy:
it hashes the source while writing, fsyncs the destination and records exact
destination generations, but does not independently read destination payloads.
It never changes source names or authorizes retirement by itself.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import time
import uuid

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import workspace_relocation as relocation

CONTRACT = 'stockagent.source_stream_staging.v1'
_BATCH_FILES = 512
_BATCH_BYTES = 64 * 1024**2


def _batches(rows, *, max_files=_BATCH_FILES, max_bytes=_BATCH_BYTES):
    """Amortize barriers for small files without buffering large payloads.

    Each batch is still published only after its filesystem barrier.  A single
    large file may exceed the byte target and forms a batch on its own.
    """
    batch, total = [], 0
    for row in rows:
        size = row[2]
        if batch and (len(batch) >= max_files or total + size > max_bytes):
            yield batch
            batch, total = [], 0
        batch.append(row)
        total += size
    if batch:
        yield batch


def _safe_file(path: Path, *, inode=None):
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or inode is not None and (info.st_dev, info.st_ino) != tuple(inode)):
        raise ValueError('staging transaction found an unrelated destination inode')
    return info


def _prepare_batch(rows, source: Path, destination: Path, db, transaction: str):
    """Durably bind every private temporary inode before any payload write."""
    planned = []
    for relative, encoded, size in rows:
        expected = json.loads(encoded)
        origin, target = source/relative, destination/relative
        if relocation.signature(origin) != expected:
            raise ValueError('source member changed after inventory')
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if target.parent.resolve(strict=True) != target.parent:
            raise ValueError('staging destination parent was redirected')
        suffix = hashlib.sha256((transaction+'\0'+relative).encode()).hexdigest()[:24]
        temporary = target.with_name('.source-stream-'+suffix+'.partial')
        existing = db.execute('SELECT temp_name,device,inode FROM stream_files WHERE path=?',
                              (relative,)).fetchone()
        if existing is None:
            if os.path.lexists(target) or os.path.lexists(temporary):
                raise ValueError('unowned destination exists before staging')
            db.execute('INSERT INTO stream_files VALUES (?,?,?,?,?,?)',
                       (relative, temporary.name, None, None, 'planned', None))
            existing = (temporary.name, None, None)
        if existing[0] != temporary.name:
            raise ValueError('staging transaction name differs')
        device, inode = existing[1:]
        # Only names reserved by this private transaction may be discarded on
        # resume, and only while the original source still has its fixed inode.
        for path in (target, temporary):
            if not os.path.lexists(path):
                continue
            if device is None:
                if path == target:
                    raise ValueError('unbound destination cannot be discarded')
                info = _safe_file(path)
                if info.st_nlink != 1:
                    raise ValueError('unbound temporary has another name')
            else:
                _safe_file(path, inode=(device, inode))
            path.unlink()
        if relocation.signature(origin) != expected:
            raise ValueError('source member changed before staging retry')
        db.execute('UPDATE stream_files SET device=NULL,inode=NULL,state=?,target_sig=NULL WHERE path=?',
                   ('planned', relative))
        planned.append((relative, encoded, size, temporary))
    db.commit()
    bound = []
    for relative, encoded, size, temporary in planned:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        try:
            opened = os.fstat(descriptor)
            db.execute('UPDATE stream_files SET device=?,inode=?,state=? WHERE path=?',
                       (opened.st_dev, opened.st_ino, 'copying', relative))
        finally:
            os.close(descriptor)
        bound.append((relative, encoded, size, temporary, opened.st_dev, opened.st_ino))
    db.commit()
    return bound


def _copy_one(bound, source: Path, destination: Path, guard):
    relative, encoded, size, temporary, device, inode = bound
    expected = json.loads(encoded)
    origin, target = source/relative, destination/relative
    if relocation.signature(origin) != expected or os.path.lexists(target):
        raise ValueError('source or destination changed before streaming')
    target_fd = os.open(temporary, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        opened = os.fstat(target_fd)
        if (opened.st_dev, opened.st_ino, opened.st_nlink, opened.st_size) != (device, inode, 1, 0):
            raise ValueError('reserved temporary inode changed before streaming')
        source_fd = os.open(origin, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        digest, count = hashlib.sha256(), 0
        try:
            source_stat = os.fstat(source_fd)
            if (source_stat.st_dev, source_stat.st_ino) != tuple(expected[:2]):
                raise ValueError('opened source member is not the fixed inode')
            while block := os.read(source_fd, 8*1024**2):
                guard()
                digest.update(block)
                count += len(block)
                view = memoryview(block)
                while view:
                    written = os.write(target_fd, view)
                    if written <= 0:
                        raise OSError('short write to E source stage')
                    view = view[written:]
            after = os.fstat(source_fd)
            if (count != size or relocation.signature(origin) != expected
                    or (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                    != (source_stat.st_dev, source_stat.st_ino, source_stat.st_size,
                        source_stat.st_mtime_ns, source_stat.st_ctime_ns)):
                raise ValueError('source member changed during its one-pass copy')
        finally:
            os.close(source_fd)
        os.fchown(target_fd, expected[6], expected[7])
        os.fchmod(target_fd, stat.S_IMODE(expected[2]))
        os.utime(target_fd, ns=(expected[4], expected[4]))
    finally:
        os.close(target_fd)
    os.link(temporary, target, follow_symlinks=False)
    temporary.unlink()
    target_signature = relocation.signature(target)
    if not relocation.signature_metadata_matches(target_signature, expected):
        raise ValueError('destination metadata differs after source streaming')
    return relative, digest.hexdigest(), count, target_signature


def copy(source: Path, destination: Path, evidence: Path, *, workers=4, progress=None, budget_bytes=None,
         guard=lambda: None):
    """Copy a locked immutable generation; retain all originals on any error."""
    source, destination, evidence = (Path(p).absolute() for p in (source, destination, evidence))
    if (type(workers) is not int or not 1 <= workers <= 8
            or budget_bytes is not None and (type(budget_bytes) is not int or budget_bytes <= 0)
            or source.is_symlink()
            or source.resolve(strict=True) != source or destination.is_relative_to(source)
            or destination.is_symlink() or destination.parent.resolve(strict=True) != destination.parent
            or evidence.is_symlink() or evidence.is_relative_to(source)):
        raise ValueError('stream staging requires bounded workers and disjoint real roots')
    evidence.mkdir(parents=True, mode=0o700, exist_ok=True)
    if evidence.resolve(strict=True) != evidence or stat.S_IMODE(evidence.stat().st_mode) != 0o700:
        raise ValueError('stream staging evidence is not a private real directory')
    marker = evidence/'source-stream-intent.json'
    if marker.exists():
        value = json.loads(marker.read_bytes())
        if (marker.is_symlink() or value.get('contract') != CONTRACT or value.get('source') != str(source)
                or value.get('destination') != str(destination) or not isinstance(value.get('transaction'), str)):
            raise ValueError('stream staging intent differs')
    else:
        if any(evidence.iterdir()):
            raise ValueError('stream staging cannot adopt another copy transaction')
        value = {'contract': CONTRACT, 'source': str(source), 'destination': str(destination),
                 'transaction': uuid.uuid4().hex}
        atomic_write_json(marker, value, durable=True)
    guard()
    db = relocation.index(source, destination, evidence)
    try:
        db.execute('CREATE TABLE IF NOT EXISTS stream_files (path TEXT PRIMARY KEY, temp_name TEXT NOT NULL, '
                   'device INTEGER, inode INTEGER, state TEXT NOT NULL, target_sig TEXT)')
        db.commit()
        intent = json.loads((evidence/'intent.json').read_bytes())
        if (budget_bytes is not None and intent['logical_bytes'] + intent['entries']*16384 + 1024**3
                > budget_bytes):
            raise ValueError('source stream stage exceeds its reserved E budget')
        for relative, encoded, sha, target_sig in db.execute(
                "SELECT f.path,f.sig,f.sha,s.target_sig FROM files f LEFT JOIN stream_files s ON s.path=f.path "
                "WHERE f.kind='file' AND f.state='verified'"):
            if (not isinstance(sha, str) or len(sha) != 64 or target_sig is None
                    or relocation.signature(source/relative) != json.loads(encoded)
                    or relocation.signature(destination/relative) != json.loads(target_sig)):
                raise ValueError('previously staged source or destination generation changed')
        completed_files, completed_bytes = db.execute("SELECT count(*),coalesce(sum(bytes),0) FROM files "
                                                       "WHERE kind='file' AND state='verified'").fetchone()
        start_files, start_bytes, started = completed_files, completed_bytes, time.monotonic()
        def report():
            elapsed = max(0.001, time.monotonic()-started)
            value = {'contract': CONTRACT, 'state': 'source_stream_copy',
                     'files_completed': completed_files, 'files_total': intent['files'],
                     'bytes_completed': completed_bytes, 'bytes_total': intent['logical_bytes'],
                     'files_per_second': (completed_files-start_files)/elapsed,
                     'bytes_per_second': (completed_bytes-start_bytes)/elapsed,
                     'observed_at_epoch': time.time(), 'source_files_deleted': 0,
                     'destination_payload_readback': False}
            atomic_write_json(evidence/'progress.json', value)
            if progress:
                progress(value)
        report()
        rows = relocation.pending_rows(db)
        last_report = time.monotonic()
        for batch in _batches(rows):
            guard()
            bound = _prepare_batch(batch, source, destination, db, value['transaction'])
            worker = lambda item: _copy_one(item, source, destination, guard)
            completed = list(relocation._bounded_as_ready(bound, worker, workers=workers))
            # One device barrier covers the files, names and metadata in this
            # batch. No durable completion row is written before this returns.
            relocation.sync_filesystem(destination)
            for relative, digest, size, target_signature in completed:
                db.execute('UPDATE stream_files SET state=?,target_sig=? WHERE path=?',
                           ('complete', json.dumps(target_signature), relative))
                db.execute("UPDATE files SET sha=?,state='verified' WHERE path=?", (digest, relative))
                completed_files += 1
                completed_bytes += size
            db.commit()
            if time.monotonic()-last_report >= 1:
                report()
                last_report = time.monotonic()
        report()
        if completed_files != intent['files'] or completed_bytes != intent['logical_bytes']:
            raise ValueError('source stream copy did not account for all files')
        last_phase_report = 0.0
        def phase_report(state, completed, total, *, force=False):
            nonlocal last_phase_report
            now = time.monotonic()
            if not force and now-last_phase_report < 1:
                return
            value = {'contract': CONTRACT, 'state': state,
                     'entries_completed': completed, 'entries_total': total,
                     'observed_at_epoch': time.time(), 'source_files_deleted': 0,
                     'destination_payload_readback': False}
            atomic_write_json(evidence/'progress.json', value)
            if progress:
                progress(value)
            last_phase_report = now
        # File copying creates its parent directories, but a source may also
        # contain empty directories. Materialize those indexed names before the
        # namespace comparison, including after a previously interrupted run.
        # Source signatures and final namespace checks remain authoritative.
        directory_count = db.execute("SELECT count(*) FROM files WHERE kind='directory'").fetchone()[0]
        phase_report('preparing_destination_directories', 0, directory_count, force=True)
        directories = 0
        for relative, encoded in db.execute(
                "SELECT path,sig FROM files WHERE kind='directory' ORDER BY length(path),path"):
            guard()
            origin, target = source/relative, destination/relative
            if relocation.signature(origin) != json.loads(encoded):
                raise ValueError('source directory changed before staging commit')
            target.mkdir(mode=0o700, exist_ok=True)
            if target.is_symlink() or target.resolve(strict=True) != target:
                raise ValueError('staging destination directory was redirected')
            directories += 1
            if directories % 512 == 0:
                phase_report('preparing_destination_directories', directories, directory_count)
        # A path-index lookup proves both namespaces, without reading target
        # payloads or trusting a moving directory's root timestamp alone.
        for root, original in ((source, True), (destination, False)):
            seen = 0
            phase = 'verifying_source_namespace' if original else 'verifying_destination_namespace'
            phase_report(phase, 0, intent['entries'], force=True)
            for name, kind, signature in relocation.walk(root):
                if seen % 512 == 0:
                    guard()
                    phase_report(phase, seen, intent['entries'])
                row = db.execute('SELECT f.kind,f.sig,f.state,s.target_sig FROM files f '
                    'LEFT JOIN stream_files s ON f.path=s.path WHERE f.path=?', (name,)).fetchone()
                if row is None or row[0] != kind:
                    raise ValueError('source or destination namespace changed')
                if original and signature != json.loads(row[1]):
                    raise ValueError('source generation changed before staging commit')
                if not original and kind == 'file' and (row[2] != 'verified' or row[3] is None
                                                        or signature != json.loads(row[3])):
                    raise ValueError('destination generation changed before staging commit')
                seen += 1
            if seen != intent['entries']:
                raise ValueError('source or destination namespace is incomplete')
            phase_report(phase, seen, intent['entries'], force=True)
        phase_report('applying_destination_directory_metadata', 0, directory_count, force=True)
        directories = 0
        for relative, encoded in db.execute("SELECT path,sig FROM files WHERE kind='directory' ORDER BY length(path) DESC"):
            path, signature = destination/relative, json.loads(encoded)
            os.chown(path, signature[6], signature[7])
            os.chmod(path, stat.S_IMODE(signature[2]))
            os.utime(path, ns=(signature[4], signature[4]))
            directories += 1
            if directories % 512 == 0:
                guard()
                phase_report('applying_destination_directory_metadata', directories, directory_count)
        signature = intent['source_root_signature']
        os.chown(destination, signature[6], signature[7])
        os.chmod(destination, stat.S_IMODE(signature[2]))
        os.utime(destination, ns=(signature[4], signature[4]))
        phase_report('flushing_destination', 0, 1, force=True)
        relocation.sync_filesystem(destination)
        result = {'contract': CONTRACT, 'state': 'source_sha256_streamed_no_destination_readback',
                  'files': completed_files, 'logical_bytes': completed_bytes, 'source_files_deleted': 0,
                  'source_sha256_streamed': True, 'destination_independent_readback': False,
                  'all_source_sha256_and_destination_readback_verified': False,
                  'source_namespace_unchanged': True, 'durability': 'batch_syncfs_before_index_commit',
                  'transaction': value['transaction'], 'observed_at_epoch': time.time()}
        atomic_write_json(evidence/'copy-result.json', result, durable=True)
        atomic_write_json(evidence/'progress.json', result, durable=True)
        return result
    finally:
        db.close()
