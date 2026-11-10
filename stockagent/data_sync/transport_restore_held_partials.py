"""Restore one fixed interrupted sender cohort to its original E names.

This is a bounded preparation for the existing D transport-retirement owner.
It never unlinks a source, changes a live control file, publishes a delivery,
or treats a restored file as NAS acceptance. Only the explicit historical
comparison and wave below are enrolled; unknown names and changed data wait.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import tarfile
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import transport_relocation_retirement as retirement
from stockagent.data_sync import transport_relocation_delta as delta
from stockagent.data_sync.workspace_relocation import signature

CONTRACT = 'stockagent.transport_restore_held_partials.v1'
COMPARISON = 'f725ed4e0a288a90c1d68254ae1320488c60f91990178b85a5e9b9dd8058e461'
ARCHIVE_SHA = '0656ff4f74431b27d567c585548f1545593665b04471c783f2a01651cb051b7c'
WAVE = 'c226373aea7fe46958771d0dd6366f639e7bb5ca73b47901e902421b7deb1680'
FILES = 159
BYTES = 1325400064
CHUNK_BYTES = 8 * 1024**2
_NAME = re.compile(r'blobs-([0-9a-f]{64})\.partial\Z')


def enrolled(evidence: Path) -> bool:
    """A fixed operator-owned enrollment is required before timer adoption."""
    path = evidence / 'partial-name-restore-enrollment.json'
    if not path.exists():
        if path.is_symlink():
            raise ValueError('partial restore enrollment is redirected')
        return False
    info = path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
            or info.st_mode & 0o022 or info.st_size > 64 * 1024):
        raise ValueError('partial restore enrollment is not operator-owned')
    value = json.loads(path.read_bytes())
    body = {'contract': CONTRACT, 'comparison_identity_sha256': COMPARISON,
            'archive_sha256': ARCHIVE_SHA, 'wave_intent_sha256': WAVE,
            'files': FILES, 'logical_bytes': BYTES, 'restore_to_sender_staging': True,
            'retirement_authorized_by_restore': False}
    expected = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if value != {**body, 'identity_sha256': expected}:
        raise ValueError('partial restore enrollment differs from the reviewed scope')
    return True


def needs_restore(evidence: Path, destination: Path, comparison: dict) -> bool:
    """Only the reviewed generation may trigger this opt-in preparation."""
    if comparison.get('identity_sha256') != COMPARISON or not enrolled(evidence):
        return False
    path = evidence / 'partial-name-restore-status.json'
    if not path.exists():
        if path.is_symlink():
            raise ValueError('partial restore status is redirected')
        return True
    if path.is_symlink() or path.stat().st_size > 256 * 1024:
        raise ValueError('partial restore status is redirected or too large')
    value = json.loads(path.read_bytes())
    members = value.get('members')
    if (value.get('contract') != CONTRACT or value.get('comparison_identity_sha256') != COMPARISON
            or value.get('state') != 'restored' or value.get('files_completed') != FILES
            or value.get('logical_bytes_completed') != BYTES or not isinstance(members, list)
            or len(members) != FILES):
        return True
    seen = set()
    for member in members:
        if not isinstance(member, dict) or not isinstance(member.get('relative'), str):
            return True
        part = PurePosixPath(member['relative'])
        if (len(part.parts) != 4 or part.parts[:3] != ('.staging', 'lake-raw-' + WAVE, 'payload')
                or _NAME.fullmatch(part.name) is None or member['relative'] in seen):
            return True
        seen.add(member['relative'])
        try:
            target = destination.joinpath(*part.parts)
            if (target.is_symlink() or target.resolve(strict=True) != target
                    or not delta.same_E_generation(target, member.get('signature'))):
                return True
        except OSError:
            return True
    return False


def _bound_locks(config, handles):
    """Require all three original sender boundaries, including inode identity."""
    paths = (Path(config['state_root']) / 'cycle.lock', Path(config['state_root']) / 'owner.lock',
             Path(config['immutable_archive_state_root']) / 'source-replication-owner.lock')
    if not isinstance(handles, (tuple, list)) or len(handles) != len(paths):
        raise ValueError('original sender lock set is required')
    for path, handle in zip(paths, handles):
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError('original sender lock path differs')
        fd = handle.fileno()
        named, held = path.lstat(), os.fstat(fd)
        if (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino):
            raise ValueError('original sender lock inode changed')
        lock_info = Path(f'/proc/self/fdinfo/{fd}').read_text()
        if not any(line.startswith('lock:') and {'FLOCK', 'WRITE'}.issubset(line.split())
                   for line in lock_info.splitlines()):
            raise ValueError('original sender lock is not held')


def _directory(parent: Path, name: str, device: int) -> Path:
    path = parent / name
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or path.is_symlink() or path.resolve() != path
            or info.st_dev != device or info.st_uid != os.geteuid()):
        raise ValueError('E staging directory is redirected, unowned or on another volume')
    return path


def _link_anonymous(fd: int, parent_fd: int, name: str) -> None:
    """Commit a fully fsynced O_TMPFILE without ever overwriting a name."""
    libc = ctypes.CDLL(None, use_errno=True)
    call = libc.linkat
    call.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int)
    call.restype = ctypes.c_int
    if call(fd, b'', parent_fd, os.fsencode(name), 0x1000) != 0:  # AT_EMPTY_PATH
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), name)


def restore(evidence: Path, destination: Path, config: dict, handles, *, admit) -> dict:
    if not enrolled(evidence):
        raise ValueError('partial restore has not been explicitly enrolled')
    _bound_locks(config, handles)
    comparison = json.loads((evidence / 'comparison-status.json').read_bytes())
    if comparison.get('state') != 'completed' or comparison.get('identity_sha256') != COMPARISON:
        raise ValueError('historical comparison is not the explicitly enrolled generation')
    archive, archive_signature, preserved = retirement._delta_manifest(evidence, comparison)
    delta_receipt = json.loads((evidence / 'delta-receipt.json').read_bytes())
    restored = json.loads((evidence / 'delta-restore-receipt.json').read_bytes())
    if (delta_receipt.get('archive_sha256') != ARCHIVE_SHA
            or restored.get('archive_sha256') != ARCHIVE_SHA
            or restored.get('archive_signature') != archive_signature
            or restored.get('comparison_identity_sha256') != COMPARISON
            or restored.get('state') != 'member_restore_verified'
            or restored.get('member_bytes_and_portable_metadata_verified') is not True):
        raise ValueError('fixed E archive and independent restore proof differ')
    journal_path = Path(config['immutable_archive_state_root']) / 'source-wave-current.json'
    if journal_path.is_symlink() or not journal_path.is_file() or journal_path.stat().st_size > 4 * 1024**2:
        raise ValueError('current sender intent is unavailable')
    sender = json.loads(journal_path.read_bytes())
    if (sender.get('state') not in {'copying', 'sealed', 'enrolled'}
            or re.fullmatch(r'[0-9a-f]{64}', str(sender.get('intent_sha256', ''))) is None
            or sender.get('intent_sha256') == WAVE):
        raise BlockingIOError('historical source wave is the current or unknown sender intent')
    intent_identity = sender['intent_sha256']

    with sqlite3.connect(f'file:{evidence / "retirement.sqlite"}?mode=ro', uri=True) as db:
        if db.execute('SELECT identity FROM metadata').fetchall() != [(COMPARISON,)]:
            raise ValueError('retirement journal identity differs')
        states = dict(db.execute('SELECT path,state FROM members'))
    wave_dir = 'lake-raw-' + WAVE
    records = []
    for relative, record in preserved.items():
        part = PurePosixPath(relative)
        if len(part.parts) != 4 or part.parts[:3] != ('.staging', wave_dir, 'payload'):
            continue
        match = _NAME.fullmatch(part.name)
        if (match is None or states.get(relative) not in {'held', 'retired'}
                or not isinstance(record.get('source_signature'), list)
                or len(record['source_signature']) != 9
                or record.get('bytes') not in {0, CHUNK_BYTES}
                or (record['bytes'] and record.get('sha256') != match.group(1))
                or (not record['bytes'] and record.get('sha256') != hashlib.sha256(b'').hexdigest())):
            raise ValueError('historical staged fragment is outside the reviewed scope')
        records.append((relative, record))
    if len(records) != FILES or sum(record['bytes'] for _, record in records) != BYTES:
        raise ValueError('fixed historical partial inventory differs')
    if (destination.is_symlink() or destination.resolve(strict=True) != destination
            or not destination.is_dir()):
        raise ValueError('E sender destination is redirected')
    ignore = destination / '.stignore'
    if (ignore.is_symlink() or not ignore.is_file() or ignore.stat().st_size > 64 * 1024
            or not {'(?d).staging', '(?d).staging/**'} <= set(ignore.read_text().splitlines())):
        raise ValueError('sender staging must remain excluded from synchronization')
    admit(BYTES + 64 * 1024**2)
    device = destination.stat().st_dev
    stage = _directory(destination, '.staging', device)
    wave = _directory(stage, wave_dir, device)
    target_dir = _directory(wave, 'payload', device)
    status_path = evidence / 'partial-name-restore-status.json'
    done = 0
    total_bytes = 0
    members = []
    started = time.monotonic()
    with tarfile.open(archive, 'r:') as tar:
        for relative, record in records:
            target = destination.joinpath(*PurePosixPath(relative).parts)
            proof = retirement._current_e_proof(destination, relative, record['sha256'], record['source_signature'])
            if proof is None:
                if target.exists() or target.is_symlink():
                    raise ValueError('existing E partial differs; never overwrite it')
                member = tar.getmember(record['archive_name'])
                if not member.isfile() or member.size != record['bytes']:
                    raise ValueError('archive partial member differs')
                source = tar.extractfile(member)
                if source is None:
                    raise ValueError('archive partial is unreadable')
                directory_fd = os.open(target_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    def check_directory_binding():
                        named, held = target_dir.lstat(), os.fstat(directory_fd)
                        if (target_dir.resolve(strict=True) != target_dir or not stat.S_ISDIR(named.st_mode)
                                or (named.st_dev, named.st_ino) != (held.st_dev, held.st_ino)):
                            raise ValueError('E staging directory binding changed')
                    check_directory_binding()
                    fd = os.open('.', os.O_TMPFILE | os.O_RDWR | os.O_CLOEXEC, 0o600, dir_fd=directory_fd)
                    with os.fdopen(fd, 'w+b') as output, source:
                        h, count = hashlib.sha256(), 0
                        while block := source.read(4 * 1024**2):
                            output.write(block); h.update(block); count += len(block)
                        output.flush(); os.fsync(output.fileno())
                        expected = record['source_signature']
                        os.fchown(output.fileno(), expected[6], expected[7])
                        os.fchmod(output.fileno(), stat.S_IMODE(expected[2]))
                        os.utime(output.fileno(), ns=(expected[4], expected[4]))
                        os.fsync(output.fileno()); output.seek(0)
                        readback = hashlib.file_digest(output, 'sha256').hexdigest()
                        if (count != record['bytes'] or h.hexdigest() != record['sha256']
                                or readback != record['sha256'] or not delta.same_E_generation(archive, archive_signature)):
                            raise ValueError('restored partial SHA or fixed archive changed')
                        _bound_locks(config, handles)
                        if json.loads(journal_path.read_bytes()).get('intent_sha256') != intent_identity:
                            raise ValueError('sender intent changed before E installation')
                        check_directory_binding()
                        _link_anonymous(output.fileno(), directory_fd, target.name)
                        os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
                proof = retirement._current_e_proof(destination, relative, record['sha256'], record['source_signature'])
                if proof is None:
                    raise ValueError('installed same-name E readback differs')
            done += 1
            total_bytes += record['bytes']
            members.append({'relative': relative, 'signature': proof})
            if done % 16 == 0 or done == FILES:
                atomic_write_json(status_path, {'contract': CONTRACT, 'state': 'restoring' if done < FILES else 'restored',
                    'comparison_identity_sha256': COMPARISON, 'files_completed': done, 'files_total': FILES,
                    'logical_bytes_completed': total_bytes, 'logical_bytes_total': BYTES,
                    'observed_at_epoch': time.time(), 'source_files_deleted': 0,
                    'NAS_acceptance_claimed': False, 'retirement_authorized': False}, durable=True)
    if not delta.same_E_generation(archive, archive_signature) or done != FILES or total_bytes != BYTES:
        raise ValueError('fixed E archive or restored inventory changed')
    _bound_locks(config, handles)
    result = {'contract': CONTRACT, 'state': 'restored', 'comparison_identity_sha256': COMPARISON,
              'files_completed': done, 'files_total': FILES, 'logical_bytes_completed': total_bytes,
              'logical_bytes_total': BYTES, 'observed_at_epoch': time.time(),
              'elapsed_seconds': time.monotonic() - started, 'source_files_deleted': 0,
              'NAS_acceptance_claimed': False, 'retirement_authorized': False, 'members': members}
    atomic_write_json(status_path, result, durable=True)
    return result
