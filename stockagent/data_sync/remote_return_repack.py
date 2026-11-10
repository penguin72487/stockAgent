"""Follow one proven physical D repack for a pending training-return wave.

Only immutable manifests, original source-stream records, and install/retire
journals are inspected. This is not an independent restore or a remote ACK.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from stockagent.data_sync import packed_snapshots as p, stream_repack as s
from stockagent.data_sync.artifact_maintenance import automatic_dataset_name
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_member_stream import read_stable_bytes


def _json(path: Path, *, maximum=2*1024*1024):
    return json.loads(read_stable_bytes(path, maximum=maximum))


def resolve(cold: Path, workspace: Path, wave: dict):
    """Return the one exact, still-present direct successor of an old wave."""
    cold, workspace = Path(cold).absolute(), Path(workspace).absolute()
    if not isinstance(wave, dict) or set(wave) != {'relative_root','dataset','snapshot_id','manifest_sha256'}:
        raise SnapshotError('pending training wave is malformed')
    dataset = p.validate_slug(wave['dataset'], 'return dataset')
    old_id = p.validate_slug(wave['snapshot_id'], 'return snapshot')
    old_sha = p._validate_hash(wave['manifest_sha256'], 'return old manifest SHA')
    if (not isinstance(wave['relative_root'], str)
            or automatic_dataset_name(wave['relative_root']) != dataset
            or os.path.lexists(cold/'manifests'/dataset/(old_id+'.json'))):
        raise SnapshotError('pending return original identity is not eligible for repack lookup')
    root = workspace/'migration/source-local-exact-repack'/dataset
    stage = workspace/'migration/stream-source-repack-v1'/dataset/old_sha
    if (p._paths_overlap(cold, workspace) or root.is_symlink() or not root.is_dir()
            or root.resolve(strict=True) != root or stage.is_symlink()
            or not stage.is_dir() or stage.resolve(strict=True) != stage):
        raise SnapshotError('pending return repack evidence is missing or redirected')
    candidates = []
    for n, path in enumerate(root.iterdir(), 1):
        if n > 128:
            raise SnapshotError('pending return repack receipt set exceeds its bound')
        if path.is_symlink() or not path.is_file() or path.suffix != '.json':
            raise SnapshotError('pending return repack receipt namespace differs')
        value = _json(path)
        if (isinstance(value, dict) and value.get('contract') == 'stockagent.source_local_exact_repack.v1'
                and value.get('dataset') == dataset and value.get('old_manifest_sha256') == old_sha
                and value.get('state') == 'new_D_head_committed_from_exact_E_source'):
            candidates.append((path, value))
    if len(candidates) != 1:
        raise SnapshotError('pending return does not have one unambiguous repack successor')
    path, receipt = candidates[0]
    new_sha = p._validate_hash(receipt.get('new_manifest_sha256'), 'return replacement SHA')
    new_id = p.validate_slug(receipt.get('new_snapshot_id'), 'return replacement snapshot')
    retirement = receipt.get('retirement')
    if (path.name != new_sha+'.json' or receipt.get('source_mode') != 'D_streamed_to_E_dataset_pack'
            or receipt.get('old_release_removed') is not True or not isinstance(retirement, dict)
            or retirement.get('state') != 'completed'
            or retirement.get('old_source_member_hashes_checked_while_packing') is not True):
        raise SnapshotError('pending return repack receipt is not complete')
    prepared = s._prepared_metadata(stage, dataset, old_sha)
    install, retired = _json(stage/'install.json'), _json(stage/'retirement.json')
    if (prepared.get('old_snapshot_id') != old_id or prepared.get('source_member_sha_streamed') is not True
            or prepared.get('files') != receipt.get('files') or prepared.get('logical_bytes') != receipt.get('logical_bytes')
            or not isinstance(install, dict) or install.get('contract') != s.CONTRACT+'.install'
            or install.get('state') != 'head_pending' or install.get('dataset') != dataset
            or install.get('old_manifest_sha256') != old_sha or install.get('old_snapshot_id') != old_id
            or install.get('new_manifest_sha256') != new_sha or install.get('snapshot_id') != new_id
            or install.get('prepared_sha256') != prepared.get('identity_sha256')
            or not isinstance(retired, dict) or retired.get('contract') != 'stockagent.source_local_repack_retirement.v1'
            or retired.get('state') != 'completed' or retired.get('dataset') != dataset
            or retired.get('old_manifest_sha256') != old_sha or retired.get('new_manifest_sha256') != new_sha
            or retired.get('prepared_sha256') != prepared.get('identity_sha256')):
        raise SnapshotError('pending return repack stage or retirement identity differs')
    resolved = p.resolve_packed_snapshot_id(cold, dataset, new_id, require_objects=True)
    metadata, source = resolved.manifest.get('metadata', {}), resolved.manifest.get('source', {})
    if (resolved.manifest_sha256 != new_sha
            or metadata.get('physical_repack_original_manifest_sha256') != old_sha
            or metadata.get('artifact_relative_root') != wave['relative_root']
            or source.get('files') != prepared['files'] or source.get('logical_bytes') != prepared['logical_bytes']):
        raise SnapshotError('pending return replacement manifest differs from its repack')
    rows = install.get('installed')
    references = [resolved.manifest['archive']['inventory'], *resolved.manifest['archive']['objects']]
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise SnapshotError('pending return replacement install records are malformed')
    installed = {row.get('sha256'):row for row in rows}
    if len(installed) != len(rows) or set(installed) != {item['sha256'] for item in references}:
        raise SnapshotError('pending return replacement objects differ from the install')
    for item in references:
        target = p._path_under(cold, item['relpath'], 'return replacement object')
        if target.is_symlink() or not target.is_file() or not s._signature_matches(target, installed[item['sha256']].get('D_signature')):
            raise SnapshotError('pending return replacement object generation changed')
    return resolved
