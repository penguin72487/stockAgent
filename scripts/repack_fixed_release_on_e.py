#!/usr/bin/env python3
"""Repack one fixed catalog release from E into its canonical D publisher.

The old release remains addressable: it may still be pinned or used by a live
reader. This is a physical layout change, not an independent readback or a
claim that old research versions can be deleted.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.publish_data_releases import DEFAULT_CATALOG, _load_catalog, _publication_source_locks
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.packed_snapshots import publish_packed_snapshot, resolve_latest_packed

COLD = Path('/srv/stockagent-packed')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--expected-manifest-sha256', required=True)
    parser.add_argument('--progress-file', type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[0-9a-f]{64}', args.expected_manifest_sha256):
        raise SnapshotError('expected manifest is not a SHA-256')
    config = workspace_volume.configuration()
    workspace_volume.admit(0, c=config)
    workspace = Path(config['mount_point'])
    source = args.source.absolute()
    if (source.is_symlink() or source.resolve(strict=True) != source
            or not source.is_dir() or not source.is_relative_to(workspace)):
        raise SnapshotError('fixed source must be an unredirected directory on E')
    progress_path = args.progress_file
    if progress_path is not None:
        progress_path = progress_path.absolute()
        if (progress_path != workspace/'migration/source-local-exact-repack/queue-progress.json'
                or progress_path.is_symlink()):
            raise SnapshotError('queue progress is outside its fixed E location')
        progress_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    priority_guard = TradingPriorityGuard()
    def progress(value) -> None:
        priority_guard.check()
        if progress_path is not None:
            atomic_write_json(progress_path, {'contract': 'stockagent.source_local_repack_progress.v1',
                'dataset': args.dataset, 'expected_manifest_sha256': args.expected_manifest_sha256,
                'state': 'working', 'observed_at_epoch': time.time(), 'progress': value})
    source_info = source.stat()
    source_identity = (source_info.st_dev, source_info.st_ino)
    matches = [entry for entry in _load_catalog(DEFAULT_CATALOG) if entry['dataset'] == args.dataset]
    if len(matches) != 1:
        raise SnapshotError('dataset does not have one canonical catalog owner')
    entry = matches[0]

    def guard() -> None:
        priority_guard.check()
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        current = source.stat()
        if source.is_symlink() or (current.st_dev, current.st_ino) != source_identity:
            raise SnapshotError('E fixed source changed directory identity')

    guard()
    with _publication_source_locks(entry, wait_seconds=0):
        old = resolve_latest_packed(COLD, args.dataset, require_objects=False)
        if old.manifest_sha256 != args.expected_manifest_sha256:
            raise SnapshotError('fixed release is no longer the current dataset head')
        logical_bytes = old.manifest['source']['logical_bytes']
        workspace_volume.admit(logical_bytes, c=config)
        new = publish_packed_snapshot(
            COLD, args.dataset, source, node_id='penguin',
            metadata=old.manifest.get('metadata', {}),
            pack_layout='source-dataset-v1', pack_target_bytes=4 * 1024**3,
            loose_file_threshold_bytes=4 * 1024**3, large_file_layout='whole-file-v1',
            file_chunk_bytes=old.manifest['archive'].get('file_chunk_bytes'),
            pack_source_workers=2, publication_lock_nonblocking=True,
            source_guard=guard, repack_exact_manifest_sha256=args.expected_manifest_sha256,
            progress=progress if progress_path is not None else None)
    receipt = {
        'contract': 'stockagent.source_local_exact_repack.v1',
        'state': 'new_D_head_committed_from_exact_E_source',
        'dataset': args.dataset, 'old_snapshot_id': old.manifest['snapshot_id'],
        'old_manifest_sha256': old.manifest_sha256, 'new_snapshot_id': new.manifest['snapshot_id'],
        'new_manifest_sha256': new.manifest_sha256,
        'files': new.manifest['source']['files'], 'logical_bytes': new.manifest['source']['logical_bytes'],
        'new_object_count': new.manifest['archive']['object_count'],
        'new_stored_bytes': new.manifest['archive']['stored_bytes'],
        'pack_layout': new.manifest['archive'].get('new_pack_layout'),
        'old_release_removed': False, 'independent_D_readback': False,
        'NAS_accepted': False, 'observed_at_epoch': time.time()}
    receipt_root = workspace / 'migration' / 'source-local-exact-repack' / args.dataset
    receipt_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    atomic_write_json(receipt_root / (new.manifest_sha256 + '.json'), receipt, durable=True)
    import json
    print(json.dumps(receipt, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
