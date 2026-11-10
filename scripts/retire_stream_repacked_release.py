#!/usr/bin/env python3
"""Retire one superseded streamed D release under the original storage locks."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import packed_snapshots, private_handoff_retirement, private_repack_history, repack_retirement, stream_repack, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')


def packing_evidence(receipt: dict) -> dict:
    """Keep whole-source and unique-representative hash scopes distinct."""
    format_name = receipt.get('packed_format')
    if format_name == 'same-root-subset-v1':
        if (receipt.get('source_mode') != 'private_same_root_inventory_reuse'
                or receipt.get('private') is not True
                or receipt.get('source_inventory_sha_compared') is not True
                or receipt.get('source_member_sha_streamed') is not False):
            raise SnapshotError('same-root retirement receipt lacks its metadata-only scope')
        return {'old_source_member_hashes_checked_while_packing': False,
                'source_inventory_sha_compared': True, 'source_payload_reread': False}
    if format_name == 'content-alias-v3':
        if (receipt.get('unique_content_sha_streamed') is not True
                or receipt.get('all_original_occurrences_reread') is not False):
            raise SnapshotError('v3 retirement receipt lacks its unique-content hash scope')
        return {'old_source_member_hashes_checked_while_packing': False,
                'unique_content_sha_streamed': True, 'all_original_occurrences_reread': False}
    if format_name is not None:
        raise SnapshotError('retirement receipt has an unknown packed format')
    return {'old_source_member_hashes_checked_while_packing': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--private-source', action='store_true')
    parser.add_argument('--history', action='store_true')
    args = parser.parse_args()
    name = packed_snapshots.validate_slug(args.dataset, 'dataset')
    old = packed_snapshots._validate_hash(args.old_manifest_sha256, 'old manifest SHA')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    _check_d_primary_mount(COLD)
    if args.private_source and name not in stream_repack.PRIVATE_REPACK_DATASETS:
        raise SnapshotError('private retirement dataset is not enrolled')
    if args.history and not args.private_source:
        raise SnapshotError('this history mode is restricted to an enrolled private source')
    cold = private_history_root(COLD, version_policy(ROOT), initialize=False) if args.private_source else COLD
    stage = workspace/'migration/stream-source-repack-v1'/name/old
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('retirement stage is redirected')
    journal_path = stage/'install.json'
    if journal_path.is_symlink() or not journal_path.is_file():
        raise SnapshotError('retirement install record is missing')
    install = json.loads(journal_path.read_text())
    new = packed_snapshots._validate_hash(install.get('new_manifest_sha256'), 'new manifest SHA')
    receipt_path = workspace/'migration/source-local-exact-repack'/name/(new+'.json')
    if receipt_path.is_symlink() or not receipt_path.is_file():
        raise SnapshotError('retirement published receipt is missing')
    receipt = json.loads(receipt_path.read_text())
    if (receipt.get('old_manifest_sha256') != old
            or (receipt.get('private') is True) != args.private_source):
        raise SnapshotError('retirement receipt old manifest differs')
    evidence = packing_evidence(receipt)
    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if args.private_source and private_history_root(COLD, version_policy(ROOT), initialize=False) != cold:
            raise SnapshotError('private retirement D authority changed')
    common = {'materialized': Path('/srv/stockagent-packed-materialized'),
              'retention_state': Path('/var/lib/stockagent-packed-retention'),
              'extra_locks': (Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                              Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
              'guard': guard, 'apply': args.apply}
    if args.private_source and args.history:
        outcome = private_repack_history.retire(cold, stage, receipt,
            public_index=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json', **common)
    elif args.private_source:
        outcome = private_handoff_retirement.retire(cold, stage, receipt,
            public_index=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json', **common)
    else:
        outcome = repack_retirement.retire(cold, stage, receipt,
            catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
            observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
            cache=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json', **common)
    items = outcome.get('plan', {}).get('items', [])
    result = {'dataset': name, 'state': outcome['state'], 'apply': args.apply,
              'old_manifest_sha256': old, 'new_manifest_sha256': new,
              'object_count': sum(x['kind'] == 'object' for x in items),
              'object_bytes': sum(x['bytes'] for x in items if x['kind'] == 'object'),
              'versions_removed': outcome.get('versions_removed', 0),
              'estimated_allocated_bytes_reclaimed': sum(x.get('allocated_bytes', 0)
                  for x in items if x['kind'] == 'object'),
              **evidence,
              'independent_D_readback': False, 'NAS_accepted': False,
              'observed_at_epoch': time.time()}
    if args.apply:
        if args.history:
            receipt['history_retirement'] = result
            receipt['history_sweep_next_epoch'] = time.time()+(3600 if outcome['state'] == 'no_unpinned_history' else 60)
        else:
            receipt['old_release_removed'] = True
            receipt['retirement'] = result
        atomic_write_json(receipt_path, receipt, durable=True)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
