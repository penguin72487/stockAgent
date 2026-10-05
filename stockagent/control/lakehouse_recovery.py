"""Reconstruct one exact source release from a restored native lake registry."""
import hashlib
import json
from pathlib import Path
import time

from stockagent.data_sync.immutable_replication import safe
from stockagent.data_sync.offhost_backup import copy_verified_bytes
from stockagent.data_sync.packed_backup import object_descriptor
from stockagent.data_sync.packed_snapshots import (
    _validate_manifest, fetch_packed_snapshot, initialize_packed_layout,
    resolve_packed_snapshot_id, validate_slug, verify_packed_snapshot,
)


def reconstruct_registered_release(con, scratch: Path, *, dataset: str, snapshot_id: str, object_source):
    """Read fixed restored metadata; use the canonical original-value verifier.

    object_source is a fixed local resolver, never executable peer metadata.
    Its caller establishes whether the bytes came directly from a NAS archive
    or from independent copies cryptographically bound to prior NAS receipts.
    """
    started = time.perf_counter()
    dataset = validate_slug(dataset, 'recovery dataset')
    snapshot_id = validate_slug(snapshot_id, 'recovery source snapshot')
    rows = con.execute(
        'SELECT r.manifest_relative,r.manifest_sha256,r.source_fingerprint_sha256,m.bytes,m.content_utf8 '
        'FROM lake.dataset_releases r JOIN lake.source_metadata m ON r.manifest_relative=m.relative '
        'AND r.manifest_sha256=m.sha256 WHERE r.dataset=? AND r.source_snapshot_id=? AND r.available',
        [dataset, snapshot_id]).fetchall()
    if len(rows) != 1:
        raise ValueError('fixed restored registry does not identify one available source release')
    relative, manifest_sha, source_sha, count, text = rows[0]
    raw = text.encode('utf-8')
    if len(raw) != count or hashlib.sha256(raw).hexdigest() != manifest_sha:
        raise ValueError('restored original manifest bytes differ from their registered identity')
    manifest = json.loads(raw)
    _validate_manifest(manifest)
    if (relative != f'manifests/{dataset}/{snapshot_id}.json' or manifest['dataset'] != dataset
            or manifest['snapshot_id'] != snapshot_id
            or manifest['source']['portable_fingerprint_sha256'] != source_sha):
        raise ValueError('restored release path/source fingerprint differs')
    refs = [manifest['archive']['inventory'], *manifest['archive']['objects']]
    for item in refs:
        if (object_descriptor(item['relpath']) != item['sha256']
                or type(item['bytes']) is not int or item['bytes'] < 0):
            raise ValueError('restored canonical object descriptor differs')
    if scratch.exists() or any(p.is_symlink() for p in (scratch, *scratch.parents)):
        raise ValueError('packed reconstruction requires fresh independent scratch')
    scratch.mkdir(mode=0o700)
    cold = scratch / 'packed'
    initialize_packed_layout(cold, node_id='independent-recovery')
    destination = safe(cold, relative)
    copy_verified_bytes(None, destination, expected_sha256=manifest_sha, expected_bytes=count, payload=raw)
    for item in refs:
        copy_verified_bytes(Path(object_source(item)), safe(cold, item['relpath']),
                            expected_sha256=item['sha256'], expected_bytes=item['bytes'])
    release = resolve_packed_snapshot_id(cold, dataset, snapshot_id)
    materialized = fetch_packed_snapshot(cold, scratch / 'materialized', release)
    verification = verify_packed_snapshot(cold, release, materialized_path=materialized)
    return {'state': 'independent_packed_canonical_reconstruction_verified', 'dataset': dataset,
            'source_snapshot_id': snapshot_id, 'manifest_sha256': manifest_sha,
            'source_fingerprint_sha256': source_sha, 'canonical_verification': verification,
            'copied_cold_objects': len(refs), 'copied_cold_object_bytes': sum(r['bytes'] for r in refs),
            'complete_workflow_seconds': time.perf_counter() - started,
            'scratch': str(scratch), 'source_files_deleted': 0}
