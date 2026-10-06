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

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync.backup_transport_cache import process_roots, same_ntfs_signature
from stockagent.data_sync.immutable_replication import ACK, digest, HASH, inventory, NATIVE_HASH_MIN_BYTES, safe, verify
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.packed_backup import signature
from stockagent.runtime_identity import identity_sha256

PHYSICAL_TRANSPORT_ROOT = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')


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


def validate_sources(c, identity, manifest, recorded=None):
    from stockagent.data_sync.windows_cold_io import hash_many, windows_path
    root = primary_root(c, identity, manifest)
    proofs = {}
    entries = []
    for relative, row in manifest['files'].items():
        original = manifest['context'].get('source_object_paths', {}).get(relative, relative)
        if manifest['context'].get('kind') == 'immutable_source_objects':
            from stockagent.data_sync.offhost_backup import object_descriptor
            if object_descriptor(original) != row['sha256']:
                raise ValueError('flat transport source mapping differs from its canonical object')
        path = safe(root, original)
        before = signature(path)
        entries.append((relative, row, path, before))
    native = list(dict.fromkeys(path for relative, row, path, before in entries
        if not (recorded and relative in recorded)
        and NATIVE_HASH_MIN_BYTES <= row['bytes'] <= 8*1024**3 and windows_path(path) is not None))
    hashes = {}
    for offset in range(0, len(native), 16):
        hashes.update(hash_many(native[offset:offset+16]))
    for relative, row, path, before in entries:
        if recorded and relative in recorded:
            if not same_ntfs_signature(before, recorded[relative]):
                raise ValueError('primary source changed since the full SHA recovery gate')
        elif path.stat().st_size != row['bytes'] or (hashes[path] if path in hashes else digest(path)) != row['sha256'] or signature(path) != before:
            raise ValueError('primary source cannot reconstruct its immutable transport bytes')
        proofs[relative] = list(before)
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
    physical = PHYSICAL_TRANSPORT_ROOT
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
        manifest = verify(root)
        if manifest['identity_sha256'] != identity:
            raise ValueError('transport manifest differs from NAS proof')
        ack = validate_archive_receipt(c, identity, manifest, (root / 'manifest.json').read_bytes())
        sources = validate_sources(c, identity, manifest)
        expected = inventory(root)
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
    if not stage.exists() and seen != set(prior['files']):
        raise ValueError('unretired transport is incomplete')
    public = Path(c['transport_root']) / key
    aliases = (*process_roots(Path(c['transport_root']).parent, public), root)
    if process_references_many(aliases):
        raise ValueError('transport has active process references')
    validate_sources(c, identity, manifest, sources)
    if not apply:
        return {**prior, 'apply': False, 'inventory_and_process_gates_verified': True}
    if not stage.exists():
        root.rename(stage)
        root = stage
        prior['state'] = 'renamed'
        atomic_write_json(journal, prior, durable=True)
    for relative in sorted(seen):
        path = safe(root, relative)
        if not same_ntfs_signature(signature(path), prior['transport_signatures'][relative]):
            raise ValueError('transport changed immediately before unlink')
        path.unlink()
    for parent, _, _ in sorted(os.walk(root), key=lambda row: len(Path(row[0]).parts), reverse=True):
        Path(parent).rmdir()
    prior['state'] = 'retired'
    atomic_write_json(journal, prior, durable=True)
    return prior
