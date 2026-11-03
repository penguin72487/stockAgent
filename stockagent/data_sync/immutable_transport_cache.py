"""Exact recovery-gated retirement of immutable lake transport copies.

Primary D objects/releases and NAS archives are never removed. The private
plan retains manifest bytes, verified source signatures and the paired ACK.
"""
import json
import hashlib
import fcntl
import os
from pathlib import Path
import stat
import resource
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from itertools import islice
from collections import OrderedDict
import threading

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.backup_transport_cache import process_roots, same_ntfs_signature
from stockagent.data_sync.immutable_replication import (
    ACK, digest, HASH, NATIVE_HASH_MIN_BYTES, safe, _verified_inventory,
)
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.packed_backup import signature
from stockagent.runtime_identity import identity_sha256
from stockagent.data_sync.work_progress import report_work

PHYSICAL_TRANSPORT_ROOT = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')
VALIDATION_CONTRACT = 'immutable_transport_full_sha_read_generation_v2'
_RETIREMENT_SUMMARIES = OrderedDict()
_RETIREMENT_SUMMARIES_LOCK = threading.Lock()
_RETIREMENT_SUMMARIES_LIMIT = 4096


def retirement_summary(path: Path) -> dict | None:
    """Small scheduling projection of an unchanged, completed local audit.

    The canonical replication owner calls this under its existing owner lock.
    Every use checks the receipt's inode/type/size/mtime/ctime/link generation;
    replacements, missing files and in-place changes invalidate reuse. Retain
    only four result fields, never the large plan or a renewed progress clock.
    This is not recovery evidence or permission for a deletion transaction.
    """
    def generation(info):
        return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_nlink,
                info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    path = Path(path)
    key = str(path.absolute())
    try:
        before = path.lstat()
    except FileNotFoundError:
        with _RETIREMENT_SUMMARIES_LOCK:
            _RETIREMENT_SUMMARIES.pop(key, None)
        return None
    if not stat.S_ISREG(before.st_mode):
        raise ValueError('transport retirement audit is not a regular file')
    stamp = generation(before)
    with _RETIREMENT_SUMMARIES_LOCK:
        cached = _RETIREMENT_SUMMARIES.get(key)
        if cached is not None and cached[0] == stamp:
            _RETIREMENT_SUMMARIES.move_to_end(key)
            return dict(cached[1])
        _RETIREMENT_SUMMARIES.pop(key, None)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(descriptor, 'rb') as stream:
        if generation(os.fstat(stream.fileno())) != stamp:
            raise ValueError('transport retirement audit changed before read')
        value = json.loads(stream.read())
        if (generation(os.fstat(stream.fileno())) != stamp
                or generation(path.lstat()) != stamp):
            raise ValueError('transport retirement audit changed during read')
    summary = {k: value[k] for k in (
        'state', 'reclaimed_bytes', 'primary_source_files_deleted', 'nas_archive_files_deleted')}
    if (summary['state'] == 'retired' and type(summary['reclaimed_bytes']) is int
            and summary['reclaimed_bytes'] >= 0
            and summary['primary_source_files_deleted'] == 0
            and summary['nas_archive_files_deleted'] == 0):
        with _RETIREMENT_SUMMARIES_LOCK:
            _RETIREMENT_SUMMARIES[key] = stamp, summary
            _RETIREMENT_SUMMARIES.move_to_end(key)
            while len(_RETIREMENT_SUMMARIES) > _RETIREMENT_SUMMARIES_LIMIT:
                _RETIREMENT_SUMMARIES.popitem(last=False)
    return dict(summary)


def validate_archive_receipt(c, identity, manifest, manifest_bytes):
    path = safe(Path(c['receipt_root']), 'lake-' + identity + '.json')
    if path.stat().st_size > 1024**2:
        raise ValueError('NAS archive ACK is oversized')
    before = signature(path)
    value = json.loads(path.read_bytes())
    if signature(path) != before:
        raise ValueError('NAS archive ACK changed while reading')
    body = {k: v for k, v in value.items() if k != 'identity_sha256'}
    if (value.get('identity_sha256') != identity_sha256(body) or body.get('contract') != ACK
            or body.get('delivery_identity_sha256') != identity):
        raise ValueError('NAS archive ACK identity differs')
    flags = ('all_files_sha256_verified', 'exact_file_set_verified', 'source_unchanged_verified',
             'nas_independent_restore_verified', 'nas_mount_guard_verified', 'single_owner_verified', 'runtime_lock_verified')
    if any(body.get(k) is not True for k in flags) or any(body.get(k) != c[k] for k in ('producer_device_id', 'receiver_device_id')):
        raise ValueError('NAS archive ACK recovery or pairing proof differs')
    for name in ('command_exit_codes', 'restore_command_exit_codes'):
        codes = body.get(name)
        if not isinstance(codes, list) or len(codes) != 2 or any(type(v) is not int or v != 0 for v in codes):
            raise ValueError('NAS archive ACK commands did not succeed')
    expected_bytes = sum(v['bytes'] for v in manifest['files'].values()) + len(manifest_bytes) + 65
    if (body.get('manifest_file_sha256') != hashlib.sha256(manifest_bytes).hexdigest()
            or body.get('complete_files') != len(manifest['files']) + 2 or body.get('complete_bytes') != expected_bytes):
        raise ValueError('NAS archive ACK complete file membership differs')
    if manifest['context'].get('kind') == 'ducklake_catalog_and_data' and body.get('catalog_semantic_restore_verified') is not True:
        raise ValueError('catalog archive lacks independent native DuckLake restore')
    return value


def primary_root(c, identity, manifest):
    kind = manifest['context'].get('kind')
    if kind == 'immutable_source_objects':
        return Path(c['cold_root'])
    if kind == 'ducklake_catalog_and_data':
        return Path(c['lake_root']) / 'releases' / ('lake-' + identity)
    raise ValueError('only reconstructible cold source/catalog transport is a cache')


def _source_read_workers(count):
    """Overlap small-file waits without consuming the owner's FD reserve."""
    if count < 32:
        return 1
    try:
        soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft != resource.RLIM_INFINITY and (soft < 64 or soft - len(os.listdir('/proc/self/fd')) < 16):
            return 1
    except (OSError, ValueError):
        return 1
    return 4


def _bounded_source_map(function, values, executor):
    iterator = iter(values)
    while batch := list(islice(iterator, 32)):
        yield from (map(function, batch) if executor is None else executor.map(function, batch))


def validate_sources(c, identity, manifest, recorded=None, *, file_workers=None):
    """Reconstruct exact source bytes, rehashing metadata-only ctime drift.

    Original receipts stay unchanged. A changed inode, size or mtime still
    rejects the recorded generation. Only ctime/device drift may replace the
    metadata fast path with a fresh complete SHA and before/after generation
    check; subsequent checks use the exact signature returned by this pass.
    """
    from stockagent.data_sync.windows_cold_io import hash_many, windows_path
    root = primary_root(c, identity, manifest)
    proofs = {}
    if file_workers is None:
        file_workers = _source_read_workers(len(manifest['files']))
    if type(file_workers) is not int or not 1 <= file_workers <= 4:
        raise ValueError('source verification workers must be between one and four')
    def observe(item):
        relative, row = item
        original = manifest['context'].get('source_object_paths', {}).get(relative, relative)
        if manifest['context'].get('kind') == 'immutable_source_objects':
            from stockagent.data_sync.offhost_backup import object_descriptor
            if object_descriptor(original) != row['sha256']:
                raise ValueError('flat transport source mapping differs from its canonical object')
        path = safe(root, original)
        before = signature(path)
        return relative, row, path, before
    entries = []
    report_work('observe_primary_sources', total=len(manifest['files']))
    with (ThreadPoolExecutor(max_workers=file_workers) if file_workers > 1 else nullcontext(None)) as executor:
        for entry in _bounded_source_map(observe, manifest['files'].items(), executor):
            entries.append(entry)
            report_work('observe_primary_sources', done=len(entries), total=len(manifest['files']))
    full_reads = set()
    for relative, row, path, before in entries:
        if recorded and relative in recorded:
            old = recorded[relative]
            if same_ntfs_signature(before, old):
                continue
            if len(old) != 5 or tuple(before)[1:4] != tuple(old)[1:4]:
                raise ValueError('primary source changed since the full SHA recovery gate')
        full_reads.add(relative)
    native = list(dict.fromkeys(path for relative, row, path, before in entries
        if relative in full_reads
        and NATIVE_HASH_MIN_BYTES <= row['bytes'] <= 8*1024**3 and windows_path(path) is not None))
    listed = {path: before for relative, row, path, before in entries}
    def before_read(path):
        current = signature(path)
        if tuple(current)[1:4] != tuple(listed[path])[1:4]:
            raise ValueError('primary source changed before its fresh full SHA recovery read')
        return current
    hashes, native_generations = {}, {}
    report_work('hash_primary_sources', total=len(native))
    for offset in range(0, len(native), 16):
        batch = native[offset:offset+16]
        native_generations.update({path: before_read(path) for path in batch})
        hashes.update(hash_many(batch))
        report_work('hash_primary_sources', done=len(hashes), total=len(full_reads))
    def verify_entry(entry):
        relative, row, path, before = entry
        if relative in full_reads:
            # Listing thousands of members can precede this exact read by
            # seconds. Rebase only metadata-only drift *before* the full SHA;
            # any generation change during that read still rejects it.
            before = native_generations[path] if path in hashes else before_read(path)
            if (path.stat().st_size != row['bytes']
                    or (hashes[path] if path in hashes else digest(path)) != row['sha256']
                    or signature(path) != before):
                raise ValueError('primary source cannot reconstruct its immutable transport bytes')
        return relative, list(before)
    report_work('verify_primary_sources', total=len(entries))
    with (ThreadPoolExecutor(max_workers=file_workers) if file_workers > 1 else nullcontext(None)) as executor:
        for relative, proof in _bounded_source_map(verify_entry, entries, executor):
            proofs[relative] = proof
            report_work('verify_primary_sources', done=len(proofs), total=len(entries))
    return proofs


def retire(c: dict, identity: str, proof: dict, *, apply=False) -> dict:
    if not HASH.fullmatch(identity):
        raise ValueError('invalid transport retirement identity')
    with (Path(c['state_root']) / ('cache-retirement-owner-' + identity + '.lock')).open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _retire(c, identity, proof, apply=apply)


def _retire(c: dict, identity: str, proof: dict, *, apply=False) -> dict:
    if not HASH.fullmatch(identity) or proof.get('state') != 'nas_archive_file_recovery_verified' or proof.get('delivery_identity_sha256') != identity:
        raise ValueError('transport retirement requires an exact paired NAS recovery proof')
    if c.get('transport_cache_retirement') is not True:
        return {'state': 'disabled'}
    from stockagent.data_sync.transport_placement import D_BACKING, on_e, backing, check_transport
    physical = PHYSICAL_TRANSPORT_ROOT
    # Only the enrolled canonical transport follows the D-to-E placement.
    # Explicit isolated roots must not inherit this machine's live volume.
    if physical == D_BACKING and on_e():
        check_transport()
        physical = backing()
    if not physical.samefile(Path(c['transport_root']).parent):
        raise ValueError('lake transport physical/bind roots differ')
    key = 'lake-' + identity
    batch = physical / 'lakehouse' / key
    stage = physical / '.staging' / ('retired-lake-' + identity)
    journal = Path(c['state_root']) / ('cache-retirement-' + identity + '.json')
    prior = json.loads(journal.read_bytes()) if journal.exists() else None
    if prior and prior['state'] == 'retired':
        return prior
    if batch.exists() and stage.exists():
        raise ValueError('duplicate retirement roots; preserve both')
    root = stage if stage.exists() else batch
    if prior:
        manifest = json.loads(prior['manifest_bytes_utf8'])
        if manifest['identity_sha256'] != identity or identity_sha256({k: v for k, v in manifest.items() if k != 'identity_sha256'}) != identity:
            raise ValueError('retirement journal manifest differs')
        sources = validate_sources(c, identity, manifest, prior['primary_signatures'])
        ack = validate_archive_receipt(c, identity, manifest, prior['manifest_bytes_utf8'].encode())
    else:
        # Reuse this pass's exact full-SHA observation instead of immediately
        # hashing every member a second time. Source reconstruction, current
        # per-member generation checks and the apply-time gates stay below.
        manifest, expected = _verified_inventory(root)
        if manifest['identity_sha256'] != identity:
            raise ValueError('transport manifest differs from NAS proof')
        ack = validate_archive_receipt(c, identity, manifest, (root / 'manifest.json').read_bytes())
        sources = validate_sources(c, identity, manifest)
        prior = {'contract': 'immutable_lake_transport_retirement_v1', 'state': 'prepared',
                 'delivery_identity_sha256': identity, 'nas_acceptance': proof, 'paired_nas_archive_receipt': ack,
                 'manifest_bytes_utf8': (root / 'manifest.json').read_text(),
                 'primary_signatures': sources, 'files': expected,
                 'transport_signatures': {p: list(signature(safe(root, p))) for p in expected},
                 'reclaimed_bytes': sum(v['bytes'] for v in expected.values()),
                 'primary_source_files_deleted': 0, 'nas_archive_files_deleted': 0,
                 'inventory_and_recovery_verified': True}
        atomic_write_json(journal, prior, durable=True)
    if not root.exists():
        if prior['state'] not in ('prepared', 'renamed'):
            raise ValueError('missing transport without a prepared retirement')
        prior['state'] = 'retired'
        atomic_write_json(journal, prior, durable=True)
        return prior
    # An interrupted unlink may leave a subset. Unknown/shared/redirected files
    # are retained; signatures are checked immediately before each unlink.
    seen = set()
    report_work('inspect_transport_members', total=len(prior['files']))
    for p in root.rglob('*'):
        relative = p.relative_to(root).as_posix()
        safe(root, relative)
        if p.is_dir():
            continue
        info = p.lstat()
        if (relative not in prior['files'] or not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or not same_ntfs_signature(signature(p), prior['transport_signatures'][relative])):
            raise ValueError('transport changed, shared, or has unknown files')
        seen.add(relative)
        report_work('inspect_transport_members', done=len(seen), total=len(prior['files']))
    if not stage.exists() and seen != set(prior['files']):
        raise ValueError('unretired transport is incomplete')
    public = Path(c['transport_root']) / key
    aliases = (*process_roots(Path(c['transport_root']).parent, public), root)
    report_work('check_process_references', total=1)
    if process_references_many(aliases):
        raise ValueError('transport has active process references')
    report_work('check_process_references', done=1, total=1)
    validate_sources(c, identity, manifest, sources)
    if not apply:
        return {**prior, 'apply': False, 'inventory_and_process_gates_verified': True}
    if not stage.exists():
        root.rename(stage)
        root = stage
        prior['state'] = 'renamed'
        atomic_write_json(journal, prior, durable=True)
    total_bytes = sum(prior['files'][relative]['bytes'] for relative in seen)
    done_bytes = 0
    report_work('unlink_verified_transport', total=len(seen), done_bytes=0, total_bytes=total_bytes)
    for index, relative in enumerate(sorted(seen), 1):
        path = safe(root, relative)
        if not same_ntfs_signature(signature(path), prior['transport_signatures'][relative]):
            raise ValueError('transport changed immediately before unlink')
        path.unlink()
        done_bytes += prior['files'][relative]['bytes']
        report_work('unlink_verified_transport', done=index, total=len(seen),
                    done_bytes=done_bytes, total_bytes=total_bytes)
    report_work('remove_transport_directories')
    directories = sorted(os.walk(root), key=lambda row: len(Path(row[0]).parts), reverse=True)
    report_work('remove_transport_directories', total=len(directories))
    for index, (parent, _, _) in enumerate(directories, 1):
        Path(parent).rmdir()
        report_work('remove_transport_directories', done=index, total=len(directories))
    prior['state'] = 'retired'
    atomic_write_json(journal, prior, durable=True)
    return prior
