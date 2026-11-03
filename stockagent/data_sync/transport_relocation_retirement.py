"""Retire fixed D transport names after a fresh, same-name E readback.

Uses the existing sender owner locks supplied by the caller. The fixed old
directory remains in place for inherited mount namespaces; no source, cold or
NAS data is removed. Every deletion has a durable prepared journal entry.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import sqlite3
import stat
import tarfile
import tempfile
import time

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import transport_relocation_delta as delta
from stockagent.data_sync.workspace_relocation import destination_metadata_matches, digest, signature, walk

CONTRACT = 'stockagent.transport_relocation_retirement.v1'


def _name_absent(path):
    """One non-following lookup detects regular and dangling recreated names."""
    try:
        path.lstat()
    except FileNotFoundError:
        return True
    return False


def _current_e_proof(root, relative, expected_sha, source_signature):
    path = root.joinpath(*PurePosixPath(relative).parts)
    try:
        if path.resolve() != path or path.is_symlink():
            return None
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, 'rb') as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                return None
            if hasattr(os, 'posix_fadvise') and hasattr(os, 'POSIX_FADV_NOREUSE'):
                try:
                    os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_NOREUSE)
                except OSError:
                    pass
            h = hashlib.sha256()
            for block in iter(lambda: stream.read(8 * 1024**2), b''):
                h.update(block)
            after = os.fstat(stream.fileno())
        observed = signature(path)
        before_fields = [before.st_dev, before.st_ino, before.st_mode, before.st_size,
                         before.st_mtime_ns, before.st_ctime_ns, before.st_uid, before.st_gid, before.st_nlink]
        after_fields = [after.st_dev, after.st_ino, after.st_mode, after.st_size,
                        after.st_mtime_ns, after.st_ctime_ns, after.st_uid, after.st_gid, after.st_nlink]
        if (before_fields != after_fields or observed != after_fields or h.hexdigest() != expected_sha
                or not destination_metadata_matches(path, source_signature)):
            return None
        return observed
    except OSError:
        return None


def _delta_manifest(evidence, comparison):
    receipt = json.loads((evidence/'delta-receipt.json').read_bytes())
    archive = Path(receipt['archive'])
    if (receipt.get('contract') != delta.CONTRACT or receipt.get('state') != 'verified'
            or receipt.get('comparison_identity_sha256') != comparison['identity_sha256']
            or receipt.get('source_names_deleted') != 0 or receipt.get('retirement_authorized') is not False
            or archive.is_symlink() or archive.resolve() != archive
            or signature(archive) != receipt.get('archive_signature')
            or digest(archive) != receipt.get('archive_sha256')
            or signature(archive) != receipt.get('archive_signature')):
        raise ValueError('verified E delta archive or its generation differs')
    with tarfile.open(archive, 'r:') as tar:
        member = tar.getmember('manifest.json')
        if not member.isfile() or member.size > 1024**2:
            raise ValueError('invalid delta manifest')
        stream = tar.extractfile(member)
        raw = stream.read() if stream is not None else b''
    manifest = json.loads(raw)
    if (hashlib.sha256(raw).hexdigest() != receipt.get('manifest_sha256')
            or manifest.get('contract') != delta.CONTRACT
            or manifest.get('comparison_identity_sha256') != comparison['identity_sha256']):
        raise ValueError('delta manifest identity differs')
    records = manifest.get('members')
    if not isinstance(records, list) or len(records) != receipt['files']:
        raise ValueError('delta manifest member count differs')
    by_path = {row['original_path']: row for row in records}
    if len(by_path) != len(records):
        raise ValueError('duplicate delta member')
    return archive, receipt['archive_signature'], by_path


def rehearse_delta_restore(evidence: Path, workspace: Path, *, admit):
    """Rebuild each archived member into an anonymous E file and read it back.

    This is a local member-level restore rehearsal. It does not claim a full
    directory-tree, cold-store, transport-peer or NAS acceptance.
    """
    comparison = json.loads((evidence / 'comparison-status.json').read_bytes())
    delta_receipt = json.loads((evidence / 'delta-receipt.json').read_bytes())
    receipt_path = evidence / 'delta-restore-receipt.json'
    if receipt_path.exists():
        previous = json.loads(receipt_path.read_bytes())
        if (previous.get('contract') == CONTRACT + '.delta_restore'
                and previous.get('comparison_identity_sha256') == comparison.get('identity_sha256')
                and previous.get('archive_sha256') == delta_receipt.get('archive_sha256')
                and previous.get('archive_signature') == delta_receipt.get('archive_signature')
                and signature(Path(delta_receipt['archive'])) == previous.get('archive_signature')
                and previous.get('files') == delta_receipt.get('files')
                and previous.get('logical_bytes') == delta_receipt.get('logical_bytes')
                and previous.get('member_bytes_and_portable_metadata_verified') is True):
            return previous
        raise ValueError('existing delta restore proof or archive changed')
    archive, archive_signature, records = _delta_manifest(evidence, comparison)
    if (workspace.is_symlink() or workspace.parent.resolve() != workspace.parent):
        raise ValueError('redirected E restore workspace')
    admit(max((row['bytes'] for row in records.values()), default=0) + 64 * 1024**2)
    workspace.mkdir(mode=0o700, parents=True, exist_ok=True)
    if workspace.resolve() != workspace or not workspace.is_dir():
        raise ValueError('invalid E restore workspace')
    done, processed = 0, 0
    with tarfile.open(archive, 'r:') as tar:
        for relative, record in records.items():
            parts = PurePosixPath(relative)
            expected = record['source_signature']
            if (parts.is_absolute() or not parts.parts or '..' in parts.parts or len(expected) != 9
                    or not stat.S_ISREG(expected[2]) or expected[3] != record['bytes']):
                raise ValueError('invalid archived source identity')
            member = tar.getmember(record['archive_name'])
            if not member.isfile() or member.size != record['bytes']:
                raise ValueError('archived member differs')
            source = tar.extractfile(member)
            if source is None:
                raise ValueError('archived member cannot be read')
            with source, tempfile.TemporaryFile(dir=workspace) as restored:
                source_hash = hashlib.sha256()
                count = 0
                for block in iter(lambda: source.read(8 * 1024**2), b''):
                    restored.write(block)
                    source_hash.update(block)
                    count += len(block)
                restored.flush()
                os.fsync(restored.fileno())
                os.fchown(restored.fileno(), expected[6], expected[7])
                os.fchmod(restored.fileno(), stat.S_IMODE(expected[2]))
                os.utime(restored.fileno(), ns=(expected[4], expected[4]))
                st = os.fstat(restored.fileno())
                restored.seek(0)
                readback = hashlib.sha256()
                for block in iter(lambda: restored.read(8 * 1024**2), b''):
                    readback.update(block)
                if (count != record['bytes'] or source_hash.hexdigest() != record['sha256']
                        or readback.hexdigest() != record['sha256'] or not stat.S_ISREG(st.st_mode)
                        or stat.S_IMODE(st.st_mode) != stat.S_IMODE(expected[2])
                        or (st.st_size, st.st_mtime_ns, st.st_uid, st.st_gid) !=
                           (expected[3], expected[4], expected[6], expected[7])):
                    raise ValueError('independent E member restore differs')
            done += 1
            processed += count
            if done % 16 == 0:
                atomic_write_json(evidence / 'delta-restore-status.json', {
                    'contract': CONTRACT + '.delta_restore', 'state': 'restoring',
                    'comparison_identity_sha256': comparison['identity_sha256'],
                    'files_completed': done, 'files_total': len(records),
                    'logical_bytes_completed': processed, 'logical_bytes_total': delta_receipt['logical_bytes'],
                    'observed_at_epoch': time.time()})
    if (done != delta_receipt['files'] or processed != delta_receipt['logical_bytes']
            or signature(archive) != archive_signature):
        raise ValueError('delta archive changed during restore rehearsal')
    result = {'contract': CONTRACT + '.delta_restore', 'state': 'member_restore_verified',
              'comparison_identity_sha256': comparison['identity_sha256'],
              'archive_sha256': delta_receipt['archive_sha256'], 'archive_signature': archive_signature,
              'files': done, 'logical_bytes': processed, 'observed_at_epoch': time.time(),
              'member_bytes_and_portable_metadata_verified': True, 'full_tree_restore_claimed': False,
              'NAS_acceptance_claimed': False, 'source_names_deleted': 0}
    atomic_write_json(receipt_path, result)
    atomic_write_json(evidence / 'delta-restore-status.json', {
        'contract': result['contract'], 'state': result['state'],
        'comparison_identity_sha256': result['comparison_identity_sha256'],
        'files_completed': done, 'files_total': done,
        'logical_bytes_completed': processed, 'logical_bytes_total': processed,
        'observed_at_epoch': result['observed_at_epoch']})
    return result


def _check_unreachable_namespaces():
    from stockagent.data_sync.transport_relocation import stale_transport_namespaces, syncthing_alias_matches
    from stockagent.data_sync.transport_placement import backing
    status = stale_transport_namespaces()
    # A temporarily unreachable mount remains a live namespace reference.
    # It must not be equated with a completed handoff.
    if (not status.get('inspection_complete') or status.get('count') != 0
            or status.get('reachable') != 0
            or not syncthing_alias_matches(backing())):
        raise BlockingIOError('old mount namespace is reachable or sender is not on E')
    return status


def _check_references(source, rows):
    from stockagent.data_sync.materialized_cache import process_references_many
    native_device = source.lstat().st_dev
    root_sig = signature(source)
    keys = {(native_device, root_sig[1])}
    for _, raw, _, _ in rows:
        sig = json.loads(raw)
        keys.add((sig[0], sig[1]))
        keys.add((native_device, sig[1]))
    if process_references_many([source], inode_keys=keys):
        raise BlockingIOError('old transport has an active process reference')


def _waiting_held_copy(evidence, destination, comparison, intent, workers):
    """Avoid repeating a D census when no held E target can yet be retired.

    This is a no-action observation, not fresh recovery/absence acceptance.
    Once any target becomes usable the caller repeats every original gate.
    Previously retired counters keep their original timestamp.
    """
    status_path, journal_path = evidence / 'retirement-status.json', evidence / 'retirement.sqlite'
    if not status_path.exists() or not journal_path.exists():
        return None
    status = json.loads(status_path.read_bytes())
    if (status.get('contract') != CONTRACT or status.get('state') != 'waiting_held_members'
            or status.get('comparison_identity_sha256') != comparison['identity_sha256']):
        return None
    with sqlite3.connect(f'file:{journal_path}?mode=ro', uri=True) as journal:
        identity = journal.execute('SELECT identity FROM metadata').fetchall()
        states = dict(journal.execute('SELECT path,state FROM members'))
    if identity != [(comparison['identity_sha256'],)] or set(states.values()) - {'held', 'retired'}:
        return None
    with sqlite3.connect(f'file:{evidence / "files.sqlite"}?mode=ro', uri=True) as inventory:
        rows = inventory.execute("SELECT path,sig,sha,bytes FROM files WHERE kind='file' AND state='verified'").fetchall()
    if (len(rows) != intent['files'] or set(states) != {row[0] for row in rows}
            or sum(row[3] for row in rows) != intent['logical_bytes']
            or status.get('files_completed') != sum(value == 'retired' for value in states.values())
            or status.get('files_held') != sum(value == 'held' for value in states.values())):
        return None
    for relative, raw, _, size in rows:
        path, sig = PurePosixPath(relative), json.loads(raw)
        if (path.is_absolute() or not path.parts or '..' in path.parts or len(sig) != 9
                or not stat.S_ISREG(sig[2]) or sig[8] != 1 or sig[3] != size):
            return None
    held = [row for row in rows if states[row[0]] == 'held']
    if not held:
        return None
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='held-e-check') as pool:
        available = list(pool.map(lambda row: _current_e_proof(destination, row[0], row[2], json.loads(row[1])), held))
    if any(proof is not None for proof in available):
        return None
    atomic_write_json(evidence / 'retirement-wait-status.json', {
        'contract': CONTRACT + '.held_wait', 'comparison_identity_sha256': comparison['identity_sha256'],
        'state': 'waiting_held_E_targets', 'observed_at_epoch': time.time(),
        'progress_observed_at_epoch': status.get('observed_at_epoch'),
        'held_files_checked': len(held), 'D_census_repeated': False,
        'deletion_attempted': False, 'recovery_acceptance_renewed': False})
    return status


def retire_fixed_copy(evidence: Path, source: Path, destination: Path, *, workers=4, batch_size=512):
    if type(workers) is not int or not 1 <= workers <= 16 or not 1 <= batch_size <= 512:
        raise ValueError('invalid bounded retirement parallelism')
    intent = json.loads((evidence/'intent.json').read_bytes())
    comparison = json.loads((evidence/'comparison-status.json').read_bytes())
    if (comparison.get('contract') != 'stockagent.transport_relocation_comparison.v1'
            or comparison.get('state') != 'completed' or comparison.get('files_completed') != intent['files']
            or comparison.get('logical_bytes_completed') != intent['logical_bytes']):
        raise ValueError('original copy comparison is incomplete')
    delta._validate_native_source(source, intent)
    _check_unreachable_namespaces()
    waiting = _waiting_held_copy(evidence, destination, comparison, intent, workers)
    if waiting is not None:
        return waiting
    archive, archive_signature, preserved = _delta_manifest(evidence, comparison)
    restored = json.loads((evidence/'delta-restore-receipt.json').read_bytes())
    delta_receipt = json.loads((evidence/'delta-receipt.json').read_bytes())
    if (restored.get('contract') != CONTRACT + '.delta_restore'
            or restored.get('comparison_identity_sha256') != comparison['identity_sha256']
            or restored.get('archive_signature') != archive_signature
            or restored.get('archive_sha256') != delta_receipt['archive_sha256']
            or restored.get('state') != 'member_restore_verified'
            or restored.get('files') != len(preserved)
            or restored.get('logical_bytes') != delta_receipt['logical_bytes']
            or restored.get('member_bytes_and_portable_metadata_verified') is not True):
        raise ValueError('delta members have not passed local restore rehearsal')
    db = sqlite3.connect(f'file:{evidence / "files.sqlite"}?mode=ro', uri=True)
    try:
        rows = db.execute("SELECT path,sig,sha,bytes FROM files WHERE kind='file' AND state='verified' ORDER BY rowid").fetchall()
        directories = db.execute("SELECT path,sig,sha,bytes FROM files WHERE kind='directory'").fetchall()
    finally:
        db.close()
    if len(rows) != intent['files'] or sum(row[3] for row in rows) != intent['logical_bytes']:
        raise ValueError('original fixed inventory differs')
    for relative, raw, sha, size in rows:
        path = PurePosixPath(relative)
        sig = json.loads(raw)
        if (path.is_absolute() or not path.parts or '..' in path.parts or sig[8] != 1
                or not stat.S_ISREG(sig[2]) or sig[3] != size):
            raise ValueError('unexpected original transport inventory member')
        if relative in preserved and (preserved[relative]['sha256'] != sha or preserved[relative]['bytes'] != size):
            raise ValueError('delta member differs from fixed inventory')
    reference_rows = rows + directories + [('', json.dumps(intent['source_root_signature']), None, 0)]
    _check_references(source, reference_rows)
    journal = sqlite3.connect(evidence/'retirement.sqlite')
    try:
        journal.execute('PRAGMA journal_mode=WAL')
        journal.execute('CREATE TABLE IF NOT EXISTS metadata (identity TEXT NOT NULL)')
        journal.execute('CREATE TABLE IF NOT EXISTS members (path TEXT PRIMARY KEY, state TEXT NOT NULL)')
        existing = journal.execute('SELECT identity FROM metadata').fetchone()
        if existing and existing[0] != comparison['identity_sha256']:
            raise ValueError('retirement journal bound to another inventory')
        if not existing:
            journal.execute('INSERT INTO metadata VALUES (?)', (comparison['identity_sha256'],))
            journal.commit()
        before_free = os.statvfs(source).f_bavail * os.statvfs(source).f_frsize
        states = dict(journal.execute('SELECT path,state FROM members'))
        sizes = {row[0]: row[3] for row in rows}

        def progress(state):
            counts = dict(journal.execute('SELECT state,count(*) FROM members GROUP BY state'))
            retired = counts.get('retired', 0)
            value = {'contract': CONTRACT, 'state': state, 'comparison_identity_sha256': comparison['identity_sha256'],
                     'files_total': len(rows), 'files_completed': retired,
                     'files_held': counts.get('held', 0), 'observed_at_epoch': time.time(),
                     'logical_bytes_total': intent['logical_bytes'],
                     'logical_bytes_completed': sum(sizes[path] for path, state in states.items()
                                                    if state == 'retired'),
                     'logical_bytes_held': sum(sizes[path] for path, state in states.items()
                                               if state == 'held'),
                     'old_root_preserved': True, 'source_observations_deleted': 0,
                     'cold_objects_deleted': 0, 'nas_objects_deleted': 0}
            atomic_write_json(evidence/'retirement-status.json', value)
            return value

        progress('verifying_and_retiring')
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='retire-e-check') as pool:
            for offset in range(0, len(rows), batch_size):
                if signature(archive) != archive_signature:
                    raise ValueError('E delta archive changed during retirement')
                batch = rows[offset:offset+batch_size]
                if all(states.get(row[0]) == 'retired' for row in batch):
                    # Retried waves must still detect recreated names, but do
                    # not rehash, rescan every process or rewrite a journal
                    # for a batch with no remaining work.
                    absent = pool.map(lambda row: _name_absent(source.joinpath(*PurePosixPath(row[0]).parts)), batch)
                    if not all(absent):
                        raise ValueError('retired source name reappeared')
                    continue
                proofs = list(pool.map(lambda row: None if states.get(row[0]) == 'retired' else
                                      _current_e_proof(destination, row[0], row[2], json.loads(row[1])), batch))
                # Hashing may take long enough for a consumer to appear. Check
                # every batch after reading, immediately before preparing names.
                _check_unreachable_namespaces()
                _check_references(source, reference_rows)
                prepared = []
                for (relative, raw, sha, size), proof in zip(batch, proofs):
                    origin = source.joinpath(*PurePosixPath(relative).parts)
                    prior = states.get(relative)
                    if prior == 'retired':
                        if not _name_absent(origin):
                            raise ValueError('retired source name reappeared')
                        continue
                    # The delta archive proves byte recovery, not that an
                    # existing sender or downstream reader can find that
                    # version at its original path. Keep differing/missing E
                    # names until the live path itself is independently proven.
                    safe_copy = proof is not None
                    if not safe_copy or origin.is_symlink() or origin.resolve(strict=False) != origin:
                        journal.execute("INSERT OR REPLACE INTO members VALUES (?, 'held')", (relative,))
                        states[relative] = 'held'
                        continue
                    try:
                        current = signature(origin)
                    except FileNotFoundError:
                        if prior == 'prepared':
                            journal.execute("UPDATE members SET state='retired' WHERE path=?", (relative,))
                            states[relative] = 'retired'
                        else:
                            journal.execute("INSERT OR REPLACE INTO members VALUES (?, 'held')", (relative,))
                            states[relative] = 'held'
                        continue
                    if not delta._same_native_file_generation(current, json.loads(raw)):
                        journal.execute("INSERT OR REPLACE INTO members VALUES (?, 'held')", (relative,))
                        states[relative] = 'held'
                        continue
                    journal.execute("INSERT OR REPLACE INTO members VALUES (?, 'prepared')", (relative,))
                    states[relative] = 'prepared'
                    prepared.append((origin, relative, current, proof))
                journal.commit()  # recovery evidence precedes every unlink

                def unlink_prepared(member):
                    origin, relative, original_signature, proof = member
                    target = destination.joinpath(*PurePosixPath(relative).parts)
                    if (signature(origin) != original_signature or signature(archive) != archive_signature
                            or proof is not None and signature(target) != proof):
                        return relative, 'held'
                    origin.unlink()
                    return relative, 'retired'

                # Reuse the bounded readback pool after every prepared entry is
                # durable. Independent file unlinks may overlap DrvFs latency;
                # SQLite remains on this owner thread. An interrupted result
                # stays prepared and is reconciled by the existing resume gate.
                for relative, state in pool.map(unlink_prepared, prepared):
                    journal.execute("UPDATE members SET state=? WHERE path=?", (state, relative))
                    states[relative] = state
                journal.commit()
                progress('verifying_and_retiring')
        counts = dict(journal.execute('SELECT state,count(*) FROM members GROUP BY state'))
        if counts.get('retired', 0) != len(rows):
            return progress('waiting_held_members')
        if any(kind != 'directory' for _, kind, _ in walk(source)):
            return progress('waiting_unexpected_members')
        receipt = progress('registered_files_retired')
        receipt.update(logical_bytes=intent['logical_bytes'], old_root_and_empty_directories_retained=True,
                       D_available_change_during_retirement_bytes=(
                           os.statvfs(source).f_bavail * os.statvfs(source).f_frsize - before_free))
        atomic_write_json(evidence/'retirement-receipt.json', receipt)
        return receipt
    finally:
        journal.close()
