#!/usr/bin/env python3
"""Retire one exact superseded private D encoding after the E-to-D commit."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_source_repack as handoff_cli
from scripts.run_handoff_source_repack_queue import _complete
from stockagent.data_sync import packed_snapshots as packed, private_handoff_retirement as retirement, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')


def retire(dataset: str, *, apply=False):
    if dataset not in handoff_cli.ENROLLED.values():
        raise SnapshotError('private D retirement dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    _check_d_primary_mount(COLD)
    workspace = Path(config['mount_point'])
    root = workspace_volume.path('migration', create=True)/'handoff-source-repack-v1'
    status_path = root/(dataset+'.status.json')
    if (root.is_symlink() or root.resolve(strict=True) != root or status_path.is_symlink()
            or not status_path.is_file()):
        raise SnapshotError('private D retirement status root unavailable')
    status = json.loads(status_path.read_text())
    if (not _complete(status, dataset) or status.get('D_new_head_committed') is not True
            or status.get('D_E_stage_cleanup_complete') is not True):
        raise SnapshotError('private D new head and E stage retirement are not complete')
    receipt = status.get('D_publication')
    if not isinstance(receipt, dict) or receipt.get('dataset') != dataset:
        raise SnapshotError('private D publication receipt unavailable')
    old = packed._validate_hash(status.get('original_manifest_sha256'), 'original private manifest SHA')
    stage = root/dataset/old
    private = private_history_root(COLD, version_policy(ROOT), initialize=False)

    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if private_history_root(COLD, version_policy(ROOT), initialize=False) != private:
            raise SnapshotError('private D authority changed')

    result = retirement.retire(private, stage, receipt,
        public_index=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json',
        materialized=Path('/srv/stockagent-packed-materialized'),
        retention_state=Path('/var/lib/stockagent-packed-retention'),
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),), guard=guard, apply=apply)
    summary = {'dataset': dataset, 'state': result['state'], 'apply': apply,
        'reclaimable_bytes': result.get('reclaimable_bytes'),
        'deleted_object_bytes': result.get('deleted_object_bytes'),
        'estimated_allocated_bytes_reclaimed': result.get('estimated_allocated_bytes_reclaimed'),
        'old_manifest_sha256': old, 'new_manifest_sha256': receipt['new_manifest_sha256'],
        'independent_D_readback': False, 'NAS_accepted': False, 'observed_at_epoch': time.time()}
    if apply and result['state'] == 'completed':
        status.update(D_old_release_removed=True, D_retirement=summary, observed_at_epoch=time.time())
        atomic_write_json(status_path, status, durable=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True, choices=sorted(handoff_cli.ENROLLED.values()))
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(retire(args.dataset, apply=args.apply), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
