"""Rebuild missing FRED encoded objects from the already fixed materialization.

The pin embeds the complete original manifest. Recovery is admitted only when
the existing materialization reproduces every original inventory and object
SHA. The current head, source binding and research pins are never changed.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import tempfile
import time
from unittest.mock import patch

from stockagent.data_sync import packed_snapshots as p, retained_source as r
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

DATASET = 'fred-crypto-macro'
CONTRACT = 'stockagent.fred_exact_pin_recovery.v1'


def _unique_json(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise SnapshotError('duplicate fixed recovery field')
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=unique)


def recover(source: Path, cold: Path, pin_root: Path, scratch: Path, *, guard=lambda: None,
            admit=lambda _bytes: None) -> dict:
    """Restore exact old objects and manifest; never alter the current head."""
    source, cold, pin_root, scratch = (Path(value).absolute() for value in (source, cold, pin_root, scratch))
    guard()
    r._root(source)
    if (scratch.is_symlink() or not scratch.is_dir() or scratch.resolve(strict=True) != scratch
            or scratch.stat().st_uid != os.geteuid() or stat.S_IMODE(scratch.stat().st_mode) & 0o077):
        raise SnapshotError('FRED recovery scratch is not a private fixed directory')
    descriptor = os.open(scratch/'owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1
                or not os.path.samestat(info, (scratch/'owner.lock').lstat())):
            raise SnapshotError('FRED recovery owner identity differs')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('FRED exact recovery owner busy') from error
        marker = source/r.BINDING
        raw, binding_signature = r._read_binding(marker)
        binding = _unique_json(raw)
        if (binding.get('contract') != r.CONTRACT or binding.get('dataset') != DATASET
                or binding.get('source') != str(source) or binding.get('cold_root') != str(cold)):
            raise SnapshotError('FRED retained binding does not match its source owner')
        digest = p._validate_hash(binding.get('manifest_sha256'), 'FRED original manifest')
        snapshot = p.validate_slug(binding.get('snapshot_id'), 'FRED original snapshot')
        pin = r._pin_path(pin_root, DATASET, digest)
        pin_raw, pin_signature = r._read_binding(pin)
        pinned = _unique_json(pin_raw)
        manifest = pinned.get('manifest')
        if (set(pinned) != {'schema_version', 'resolved_at', 'manifest_sha256', 'manifest'}
                or pinned.get('schema_version') != 1 or pinned.get('manifest_sha256') != digest
                or not isinstance(manifest, dict) or hashlib.sha256(p._canonical_json_bytes(manifest)).hexdigest() != digest):
            raise SnapshotError('FRED pin does not contain its exact original manifest')
        p._validate_manifest(manifest)
        proof = binding.get('recovery', {})
        files = binding.get('raw_files')
        verification = binding.get('workspace_raw_verification', {})
        materialized = Path(verification.get('fixed_materialization', '')).absolute()
        if (manifest['dataset'] != DATASET or manifest['snapshot_id'] != snapshot
                or manifest['archive']['inventory']['sha256'] != binding.get('inventory_sha256')
                or not isinstance(files, dict) or not files or not materialized.is_dir()
                or materialized.is_symlink() or materialized.resolve(strict=True) != materialized
                or any(p._paths_overlap(scratch, target) for target in (cold, source, materialized, pin_root))
                or verification != r._raw_workspace_proof(source, materialized, files)
                or proof.get('manifest_sha256') != digest or proof.get('materialized_verified') is not True
                or proof.get('independently_reconstructed_files') != manifest['source']['files']
                or proof.get('independently_reconstructed_bytes') != manifest['source']['logical_bytes']):
            raise SnapshotError('FRED original materialization is not bound to the exact pin')
        status = scratch/'status.json'
        if status.is_symlink():
            raise SnapshotError('FRED recovery status is redirected')
        def save(state, **fields):
            result = {'contract': CONTRACT, 'dataset': DATASET, 'state': state,
                      'original_manifest_sha256': digest, 'original_snapshot_id': snapshot,
                      'observed_at_epoch': time.time(), 'current_head_changed': False,
                      'source_deleted': False, 'independent_recovery_verified': False,
                      'NAS_accepted': False, **fields}
            atomic_write_json(status, result, durable=True)
            return result
        try:
            available = p.resolve_packed_pin(cold, pin, expected_dataset=DATASET)
            if available.manifest_sha256 == digest:
                return save('already_available', exact_pin_resolved=True)
        except SnapshotError:
            pass  # Exact object and manifest checks below still fail closed.
        _, census = p._collect_entries(materialized, retain_entries=False)
        fields = ('files', 'directories', 'symlinks', 'logical_bytes', 'portable_fingerprint_sha256')
        if any(census.get(key) != manifest['source'].get(key) for key in fields):
            raise SnapshotError('FRED original materialization namespace changed')
        old_items = [manifest['archive']['inventory'], *manifest['archive']['objects']]
        if len({item['sha256'] for item in old_items}) != len(old_items):
            raise SnapshotError('FRED original object set has duplicate SHA')
        needed = sum(item['bytes'] for item in old_items)
        admit(needed*3 + 32*1024**2)
        save('building_exact_candidate', expected_objects=len(old_items), expected_encoded_bytes=needed)
        with tempfile.TemporaryDirectory(prefix='candidate-', dir=scratch) as temporary:
            candidate_root = Path(temporary)/'packed'
            # This isolated, temporary pack is never a Syncthing publication.
            with patch.object(p, '_notify_packed_publication', lambda *_a, **_kw: None):
                built = p.publish_packed_snapshot(candidate_root, DATASET, materialized,
                    node_id='fred-exact-recovery', pack_buckets=manifest['archive']['pack_buckets'],
                    loose_file_threshold_bytes=manifest['archive']['loose_file_threshold_bytes'],
                    file_chunk_bytes=manifest['archive']['file_chunk_bytes'],
                    compression_level=manifest['archive']['compression_level'], defer_scan=True)
            actual_items = [built.manifest['archive']['inventory'], *built.manifest['archive']['objects']]
            expected = {item['sha256']: (item['bytes'], item['relpath']) for item in old_items}
            actual = {item['sha256']: (item['bytes'], item['relpath']) for item in actual_items}
            if (built.manifest['archive']['inventory'] != manifest['archive']['inventory']
                    or len(actual) != len(actual_items) or actual != expected):
                raise SnapshotError('FRED rebuilt objects do not match every original encoded SHA')
            guard()
            if r._signature(marker) != binding_signature or r._signature(pin) != pin_signature:
                raise SnapshotError('FRED original binding or pin changed during recovery')
            # Only the existing publication owners can expose these exact
            # immutable names; all head and pin metadata remain untouched.
            with p._try_publication_lock(cold/'.local-state/locks/publish-retention-global.lock', shared=True), \
                 p._try_publication_lock(cold/f'.local-state/locks/publish-{DATASET}.lock'):
                guard()
                if r._signature(marker) != binding_signature or r._signature(pin) != pin_signature:
                    raise SnapshotError('FRED original binding or pin changed before installation')
                if shutil.disk_usage(cold).free < needed + 12*1024**3:
                    raise SnapshotError('FRED exact recovery lacks cold-storage reserve')
                save('installing_exact_objects', expected_objects=len(old_items), installed_objects=0)
                for index, item in enumerate(old_items, 1):
                    candidate = p._path_under(candidate_root, item['relpath'], 'FRED candidate object')
                    destination = p._path_under(cold, item['relpath'], 'FRED fixed object')
                    p._install_immutable_object(cold, candidate, destination,
                        expected_sha256=item['sha256'], retain_source=True)
                    if index % 16 == 0 or index == len(old_items):
                        save('installing_exact_objects', expected_objects=len(old_items), installed_objects=index)
                guard()
                if r._signature(marker) != binding_signature or r._signature(pin) != pin_signature:
                    raise SnapshotError('FRED original binding or pin changed before manifest installation')
                target = p._path_under(cold, f'manifests/{DATASET}/{snapshot}.json', 'FRED original manifest')
                p._ensure_shared_packed_directory(cold, target.parent)
                if p.write_immutable_json(target, manifest) != digest:
                    raise SnapshotError('FRED original manifest SHA changed during installation')
                fixed = p.resolve_packed_pin(cold, pin, expected_dataset=DATASET)
                if fixed.manifest_sha256 != digest:
                    raise SnapshotError('FRED restored original pin does not resolve')
                result = save('exact_pin_restored', expected_objects=len(old_items),
                              installed_objects=len(old_items), encoded_bytes=needed, exact_pin_resolved=True)
        return result
    finally:
        os.close(descriptor)
