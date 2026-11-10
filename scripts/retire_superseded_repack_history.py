#!/usr/bin/env python3
"""Retire unpinned historical versions after a completed dataset repack."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync import packed_snapshots, private_repack_history, stream_repack, superseded_repack_history, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

COLD = Path('/srv/stockagent-packed')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--prepared-only', action='store_true',
                        help='retire only unpinned older versions after E is fully prepared; keep the current D head')
    parser.add_argument('--private-source', action='store_true')
    args = parser.parse_args()
    name = packed_snapshots.validate_slug(args.dataset, 'dataset')
    old = packed_snapshots._validate_hash(args.old_manifest_sha256, 'old manifest SHA')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    _check_d_primary_mount(COLD)
    if args.private_source and (not args.prepared_only or name not in stream_repack.PRIVATE_REPACK_DATASETS):
        raise SnapshotError('private prepared history source is not enrolled')
    cold = private_history_root(COLD, version_policy(ROOT), initialize=False) if args.private_source else COLD
    stage = workspace/'migration/stream-source-repack-v1'/name/old
    if stage.is_symlink() or stage.resolve(strict=True) != stage:
        raise SnapshotError('history retirement stage redirected')
    if args.prepared_only:
        new = old
        receipt_path = stage/'prepared-history-sweep.json'
        receipt = {'contract': 'stockagent.source_local_prepared_history.v1',
                   'state': 'source_hashed_E_packages_prepared', 'dataset': name,
                   'old_manifest_sha256': old, **({'private': True} if args.private_source else {})}
    else:
        install_path = stage/'install.json'
        if install_path.is_symlink() or not install_path.is_file():
            raise SnapshotError('history retirement install record missing')
        install = json.loads(install_path.read_text())
        new = packed_snapshots._validate_hash(install.get('new_manifest_sha256'), 'new manifest SHA')
        receipt_path = workspace/'migration/source-local-exact-repack'/name/(new+'.json')
        if receipt_path.is_symlink() or not receipt_path.is_file():
            raise SnapshotError('history retirement receipt missing')
        receipt = json.loads(receipt_path.read_text())
        if receipt.get('old_manifest_sha256') != old:
            raise SnapshotError('history retirement old manifest differs')
    def guard():
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if args.private_source and private_history_root(COLD, version_policy(ROOT), initialize=False) != cold:
            raise SnapshotError('private prepared history authority changed')
    common = {'materialized': Path('/srv/stockagent-packed-materialized'),
              'retention_state': Path('/var/lib/stockagent-packed-retention'),
              'extra_locks': (Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                              Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
              'guard': guard, 'apply': args.apply, 'prepared_only': args.prepared_only}
    if args.private_source:
        outcome = private_repack_history.retire(cold, stage, receipt,
            public_index=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json', **common)
    else:
        outcome = superseded_repack_history.retire(cold, stage, receipt,
            catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
            observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
            cache=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json', **common)
    result = {'dataset': name, 'state': outcome['state'], 'apply': args.apply,
              'old_manifest_sha256': old, 'new_manifest_sha256': outcome.get(
                  'effective_new_manifest_sha256', outcome.get('new_manifest_sha256', new)),
              'versions_removed': outcome.get('versions_removed', len(outcome.get('plan', {}).get('versions', []))),
              'object_bytes': outcome.get('object_bytes_removed', outcome.get('object_bytes', 0)),
              'estimated_allocated_bytes_reclaimed': outcome.get('estimated_allocated_bytes_reclaimed',
                  sum(item.get('allocated_bytes', 0) for item in outcome.get('plan', {}).get('items', [])
                      if item.get('kind') == 'object')),
              'protected_versions': outcome.get('protected_versions', 0),
              'historical_versions_independently_recovered': False, 'NAS_accepted': False,
              'current_D_head_preserved': (args.prepared_only
                                           or outcome.get('original_anchor_missing_without_successor') is True),
              'observed_at_epoch': time.time()}
    if outcome.get('semantic_successor') is not None:
        result.update(semantic_successor=outcome['semantic_successor'], original_new_manifest_sha256=new)
    if outcome.get('latest_fixed_successor') is not None:
        result.update(latest_fixed_successor=outcome['latest_fixed_successor'], original_new_manifest_sha256=new,
                      changed_member_values_compared=False, old_historical_value_coverage='not_compared')
    if outcome.get('original_anchor_missing_without_successor') is True:
        result.update(original_anchor_missing_without_successor=True,
                      observed_current_head=outcome['observed_current_head'])
    if args.apply:
        completed = []
        prefix = ('private-prepared-history-retirement-' if args.private_source else
                  'prepared-history-retirement-' if args.prepared_only else 'history-retirement-')
        for path in stage.glob(prefix+'*.json'):
            if path.is_symlink() or not path.is_file():
                raise SnapshotError('history retirement journal redirected')
            value = json.loads(path.read_text())
            if value.get('contract') == (private_repack_history.CONTRACT if args.private_source
                                         else superseded_repack_history.CONTRACT) and value.get('state') == 'completed':
                completed.append(value)
        receipt['history_retirement'] = {'versions_removed': sum(v.get('versions_removed', 0) for v in completed),
            'object_bytes_removed': sum(v.get('object_bytes_removed', 0) for v in completed),
            'estimated_allocated_bytes_reclaimed': sum(v.get('estimated_allocated_bytes_reclaimed', 0)
                                                      for v in completed),
            'protected_versions': result['protected_versions'], 'observed_at_epoch': time.time(),
            'historical_versions_independently_recovered': False}
        if result.get('original_anchor_missing_without_successor') is True:
            receipt['history_retirement'].update(original_anchor_missing_without_successor=True,
                observed_current_head=result['observed_current_head'])
        # A full no-candidate pass is revisited daily; protected versions retry
        # hourly. A bounded version batch continues on the next timer.
        delay = 60 if result['state'] == 'completed' else 3600 if result['protected_versions'] else 86400
        receipt['history_sweep_next_epoch'] = time.time()+delay
        atomic_write_json(receipt_path, receipt, durable=True)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
