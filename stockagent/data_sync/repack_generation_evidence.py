"""Follow source-hashed physical encodings through their original owner journals.

Old manifests may have been retired after a committed migration. The sealed E
preparation, D install journal and matching publication receipt still bind each
exact old/new identity. These proofs never claim historical byte recovery, NAS
acceptance, or permission to delete a source. The caller fences its chosen head
again before publishing or changing a durable reference.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from stockagent.data_sync import packed_snapshots as p, private_subset_reuse as subset, stream_repack as s
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_member_stream import read_stable_bytes


def transition(cold: Path, workspace: Path, dataset: str, *, old_sha: str, old_id: str,
               new_sha: str, new_id: str, private: bool, files: int, logical_bytes: int,
               current_manifest: dict | None = None) -> dict:
    """Verify one exact source-hashed commit, optionally against its live manifest."""
    p.validate_slug(dataset, 'repack evidence dataset')
    p.validate_slug(old_id, 'original snapshot')
    p.validate_slug(new_id, 'new snapshot')
    p._validate_hash(old_sha, 'original manifest SHA')
    p._validate_hash(new_sha, 'new manifest SHA')
    if type(private) is not bool or any(type(x) is not int or x < 0 for x in (files, logical_bytes)):
        raise SnapshotError('repack evidence scope or counts differ')
    stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old_sha
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('refresh repack stage redirected')
    prepared = s._prepared_metadata(stage, dataset, old_sha)
    if (prepared.get('old_snapshot_id') != old_id or prepared.get('files') != files
            or prepared.get('logical_bytes') != logical_bytes):
        raise SnapshotError('refresh repack source identity differs')
    subset_mode = prepared.get('packed_format') == subset.FORMAT
    if current_manifest is not None:
        if (current_manifest.get('dataset') != dataset or current_manifest.get('snapshot_id') != new_id
                or hashlib.sha256(p._canonical_json_bytes(current_manifest)).hexdigest() != new_sha
                or current_manifest.get('metadata', {}).get('physical_repack_original_manifest_sha256') != old_sha
                or current_manifest['source'].get('files') != files
                or current_manifest['source'].get('logical_bytes') != logical_bytes):
            raise SnapshotError('refresh repack current manifest identity differs')
        shape = {**current_manifest, 'snapshot_id': old_id}
        if s._archive(prepared, shape) != current_manifest['archive']:
            raise SnapshotError('refresh repack archive differs from prepared source')
    journal = json.loads(read_stable_bytes(stage/'install.json', maximum=32*1024**2))
    receipt_path = Path(workspace)/'migration/source-local-exact-repack'/dataset/(new_sha+'.json')
    receipt = json.loads(read_stable_bytes(receipt_path, maximum=1024**2))
    if (not isinstance(journal, dict) or journal.get('contract') != s.CONTRACT+'.install'
            or journal.get('state') != 'head_pending' or journal.get('dataset') != dataset
            or journal.get('old_manifest_sha256') != old_sha or journal.get('old_snapshot_id') != old_id
            or journal.get('prepared_sha256') != prepared['identity_sha256']
            or journal.get('new_manifest_sha256') != new_sha or journal.get('snapshot_id') != new_id
            or not isinstance(journal.get('installed'), list)):
        raise SnapshotError('refresh repack install journal differs')
    installed = [item.get('sha256') for item in journal['installed'] if isinstance(item, dict)]
    objects = prepared['archive']['objects'] if subset_mode else prepared['packages']
    expected = [item['sha256'] for item in [*objects, prepared['inventory']]]
    if (len(installed) != len(journal['installed']) or len(installed) != len(expected)
            or len(set(installed)) != len(installed) or set(installed) != set(expected)):
        raise SnapshotError('refresh repack installed object set differs')
    if (not isinstance(receipt, dict) or receipt.get('contract') != 'stockagent.source_local_exact_repack.v1'
            or receipt.get('state') != 'new_D_head_committed_from_exact_E_source'
            or receipt.get('dataset') != dataset or receipt.get('old_manifest_sha256') != old_sha
            or receipt.get('new_snapshot_id') != new_id or receipt.get('new_manifest_sha256') != new_sha
            or receipt.get('source_mode') != (subset.SOURCE_MODE if subset_mode else 'D_streamed_to_E_dataset_pack')
            or (subset_mode and (not private or receipt.get('donor_manifest_sha256')
                                 != prepared.get('donor_manifest_sha256')))
            or type(receipt.get('private')) is not bool or receipt['private'] != private
            or receipt.get('files') != files or receipt.get('logical_bytes') != logical_bytes):
        raise SnapshotError('refresh repack publication receipt differs')
    return {'contract': 'stockagent.refresh_exact_repack_binding.v1',
            'snapshot_id': new_id, 'manifest_sha256': new_sha,
            'previous_snapshot_id': old_id, 'previous_manifest_sha256': old_sha,
            'prepared_sha256': prepared['identity_sha256'],
            'source_member_sha_streamed': prepared.get('source_member_sha_streamed'),
            **({'source_inventory_sha_compared': True, 'donor_manifest_sha256': prepared['donor_manifest_sha256']}
               if subset_mode else {}),
            'unique_content_sha_streamed': prepared.get('unique_content_sha_streamed'),
            'independent_D_readback': False, 'historical_recovery_claimed': False}


def chain(cold: Path, workspace: Path, dataset: str, previous: dict, *, private: bool,
          current=None, limit: int = 32) -> dict | None:
    """Bind a bounded exact encoding chain even after intermediate retirement."""
    if type(limit) is not int or not 1 <= limit <= 32:
        raise SnapshotError('invalid repack evidence chain bound')
    target_sha = p._validate_hash(previous.get('manifest_sha256'), 'original manifest SHA')
    target_id = p.validate_slug(previous.get('snapshot_id'), 'original snapshot')
    current = current or p.resolve_latest_packed(cold, dataset, require_objects=False)
    if current.manifest_sha256 == target_sha:
        if current.manifest['snapshot_id'] != target_id:
            raise SnapshotError('refresh original snapshot identity differs')
        return None
    new_sha, new_id = current.manifest_sha256, current.manifest['snapshot_id']
    counts = current.manifest['source']
    seen, steps = {new_sha}, []
    for index in range(limit):
        receipt_path = Path(workspace)/'migration/source-local-exact-repack'/dataset/(new_sha+'.json')
        receipt = json.loads(read_stable_bytes(receipt_path, maximum=1024**2))
        old_sha = p._validate_hash(receipt.get('old_manifest_sha256'), 'previous repack manifest SHA')
        stage = Path(workspace)/'migration/stream-source-repack-v1'/dataset/old_sha
        prepared = s._prepared_metadata(stage, dataset, old_sha)
        old_id = p.validate_slug(prepared.get('old_snapshot_id'), 'previous repack snapshot')
        if old_sha in seen or (index == 0 and current.manifest.get('metadata', {}).get(
                'physical_repack_original_manifest_sha256') != old_sha):
            raise SnapshotError('repack evidence chain identity differs or cycles')
        steps.append(transition(cold, workspace, dataset, old_sha=old_sha, old_id=old_id,
            new_sha=new_sha, new_id=new_id, private=private, files=counts['files'],
            logical_bytes=counts['logical_bytes'], current_manifest=current.manifest if index == 0 else None))
        if old_sha == target_sha:
            if old_id != target_id:
                raise SnapshotError('repack evidence chain original snapshot differs')
            if len(steps) == 1:
                return steps[0]
            return {'contract': 'stockagent.refresh_exact_repack_chain_binding.v1',
                'snapshot_id': current.manifest['snapshot_id'], 'manifest_sha256': current.manifest_sha256,
                'previous_snapshot_id': target_id, 'previous_manifest_sha256': target_sha,
                'transitions': list(reversed(steps)), 'independent_D_readback': False,
                'historical_recovery_claimed': False}
        seen.add(old_sha)
        new_sha, new_id = old_sha, old_id
    raise SnapshotError('refresh exact repack chain exceeds its bound')
