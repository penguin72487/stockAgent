#!/usr/bin/env python3
"""Retire the exact old D generation selected by the user's fast migration."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner, promote_latest_fixed_generation as latest  # noqa: E402
from stockagent.data_sync import (packed_member_stream as reader, semantic_link_retirement as retirement,
                                 stream_repack, workspace_volume)  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402


def run(old_sha: str, *, apply: bool, dataset: str = latest.DATASET) -> dict:
    if dataset not in latest.DATASETS:
        raise SnapshotError('latest fixed retirement dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    workspace = workspace_volume.path('migration', create=True)
    root = workspace/'handoff-index-archive-v1'/dataset
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('latest fixed retirement source root is redirected')
    policy = json.loads(reader.read_stable_bytes(latest.POLICY, maximum=65536))
    matches = []
    for path in root.glob('latest-promotion-*.json'):
        value = json.loads(reader.read_stable_bytes(path, maximum=1024**2))
        if value.get('old_manifest_sha256') == old_sha:
            matches.append((path, value))
    if len(matches) != 1:
        raise SnapshotError('latest fixed retirement requires one exact promotion')
    promotion_path, promoted = matches[0]
    identity = promoted.get('comparison_identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        raise SnapshotError('latest fixed retirement comparison identity is invalid')
    plans = list(root.glob('*/integration-diff-v1/diff-plan-'+identity+'.json'))
    if len(plans) != 1:
        raise SnapshotError('latest fixed retirement original comparison is missing')
    plan_path = plans[0]
    plan = json.loads(reader.read_stable_bytes(plan_path, maximum=1024**2))
    if plan.get('identity_sha256') != identity or plan.get('identity_sha256') != stream_repack._identity(plan):
        raise SnapshotError('latest fixed retirement comparison changed')
    name = plan.get('worklist', {}).get('name')
    if not isinstance(name, str) or Path(name).name != name:
        raise SnapshotError('latest fixed retirement worklist is not local')
    latest.validate_worklist(plan, reader.read_stable_bytes(plan_path.parent/name, maximum=latest.MAX_WORKLIST_COMPRESSED_BYTES))
    excludes, catalog_sha = planner.policy(dataset)
    if tuple(plan.get('excluded_subtrees', ())) != excludes:
        raise SnapshotError('latest fixed retirement public policy changed')
    def guard():
        workspace_volume.check(c=config)
        planner._check_d_primary_mount(cold)
        if (private_history_root(cold, version_policy(ROOT), initialize=False) != private
                or planner.policy(dataset) != (excludes, catalog_sha)
                or json.loads(reader.read_stable_bytes(latest.POLICY, maximum=65536)) != policy):
            raise SnapshotError('latest fixed retirement authority or policy changed')
    result = retirement.retire(cold, private, promotion=promoted, plan=plan, rows={}, controls={},
        latest_policy=policy, catalog_sha=catalog_sha, journal_path=root/('latest-retirement-'+identity+'.json'),
        catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
        observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
        cache=workspace/'d-cold-object-staging-v1/catalog-index-cache.json',
        materialized=Path('/srv/stockagent-packed-materialized'),
        retention_state=Path('/var/lib/stockagent-packed-retention'),
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                     Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
        guard=guard, apply=apply)
    items = result['plan']['items']
    summary = {'dataset': dataset, 'state': result['state'], 'apply': apply,
        'old_manifest_sha256': old_sha, 'new_manifest_sha256': promoted['new_manifest_sha256'],
        'object_count': sum(item['kind'] == 'object' for item in items),
        'object_bytes': sum(item['bytes'] for item in items if item['kind'] == 'object'),
        'estimated_allocated_bytes_reclaimed': sum(item.get('allocated_bytes', 0)
            for item in items if item['kind'] == 'object') if apply and result['state'] == 'completed' else 0,
        'changed_member_values_compared': False, 'old_historical_value_coverage': 'not_compared',
        'uncompared_changed_members': plan['counters']['conflict'],
        'independent_D_readback': False, 'NAS_accepted': False, 'observed_at_epoch': time.time()}
    if apply and result['state'] == 'completed':
        if json.loads(reader.read_stable_bytes(promotion_path, maximum=1024**2)) != promoted:
            raise SnapshotError('latest fixed promotion changed before retirement acknowledgment')
        atomic_write_json(promotion_path, {**promoted, 'old_sources_deleted': True, 'retirement': summary}, durable=True)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=latest.DATASETS, required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    try:
        arguments = parser.parse_args()
        print(json.dumps(run(arguments.old_manifest_sha256, apply=arguments.apply, dataset=arguments.dataset), ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
