#!/usr/bin/env python3
"""Retire the exact Binance release superseded by an audited semantic superset."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner  # noqa: E402
from stockagent.data_sync import (packed_member_stream as reader, semantic_link_retirement,
                                  stream_repack, workspace_volume)  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json  # noqa: E402

DATASET = 'binance-public-archive'


def run(old_sha: str, *, apply: bool) -> dict:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    workspace = workspace_volume.path('migration', create=True)
    root = workspace/'handoff-index-archive-v1'/DATASET
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('semantic retirement source root is redirected')
    matches = []
    for path in root.glob('semantic-promotion-*.json'):
        value = json.loads(reader.read_stable_bytes(path, maximum=1024**2))
        if value.get('old_manifest_sha256') == old_sha:
            matches.append((path, value))
    if len(matches) != 1:
        raise SnapshotError('semantic retirement requires one exact promotion')
    promotion_path, promotion = matches[0]
    identity = promotion.get('comparison_identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        raise SnapshotError('semantic promotion comparison identity is invalid')
    rows = json.loads(reader.read_stable_bytes(root/('row-audit-'+identity+'.json'), maximum=4*1024**2))
    controls = json.loads(reader.read_stable_bytes(root/('control-audit-'+identity+'.json'), maximum=1024**2))
    candidates = []
    for path in root.glob('*/integration-diff-v1/diff-plan-*.json'):
        value = json.loads(reader.read_stable_bytes(path, maximum=1024**2))
        if (value.get('comparison_scope') == 'fixed_public_snapshot'
                and value.get('public_manifest_sha256') == old_sha
                and value.get('private_manifest_sha256') == promotion.get('private_manifest_sha256')
                and value.get('identity_sha256') == stream_repack._identity(value)):
            candidates.append((path, value))
    if not candidates:
        raise SnapshotError('semantic retirement fixed-generation comparison is missing')
    # Equivalent regenerated comparisons share a fixed worklist. Select a
    # deterministic identity without treating a newer clock as better proof.
    path, plan = sorted(candidates, key=lambda item: item[1]['identity_sha256'])[0]
    worklist_name = plan['worklist']['name']
    if Path(worklist_name).name != worklist_name:
        raise SnapshotError('semantic retirement worklist is not local')
    compressed = reader.read_stable_bytes(path.parent/worklist_name, maximum=8*1024**2)
    if hashlib.sha256(compressed).hexdigest() != plan['worklist']['sha256']:
        raise SnapshotError('semantic retirement worklist SHA differs')
    excludes, catalog_sha = planner.policy(DATASET)
    if excludes:
        raise SnapshotError('semantic retirement public policy has changed')
    def guard():
        workspace_volume.check(c=config)
        planner._check_d_primary_mount(cold)
        if (private_history_root(cold, version_policy(ROOT), initialize=False) != private
                or planner.policy(DATASET) != (excludes, catalog_sha)):
            raise SnapshotError('semantic retirement authority or policy changed')
    result = semantic_link_retirement.retire(cold, private, promotion=promotion, plan=plan,
        rows=rows, controls=controls, catalog_sha=catalog_sha,
        journal_path=root/('semantic-retirement-'+identity+'.json'),
        catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
        observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
        cache=workspace/'d-cold-object-staging-v1/catalog-index-cache.json',
        materialized=Path('/srv/stockagent-packed-materialized'),
        retention_state=Path('/var/lib/stockagent-packed-retention'),
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                     Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
        guard=guard, apply=apply)
    items = result['plan']['items']
    summary = {'dataset': DATASET, 'state': result['state'], 'apply': apply,
        'old_manifest_sha256': old_sha, 'new_manifest_sha256': promotion['new_manifest_sha256'],
        'object_count': sum(item['kind'] == 'object' for item in items),
        'object_bytes': sum(item['bytes'] for item in items if item['kind'] == 'object'),
        'estimated_allocated_bytes_reclaimed': sum(item.get('allocated_bytes', 0)
            for item in items if item['kind'] == 'object') if apply and result['state'] == 'completed' else 0,
        'independent_D_readback': False, 'NAS_accepted': False, 'observed_at_epoch': time.time()}
    if apply and result['state'] == 'completed':
        if json.loads(reader.read_stable_bytes(promotion_path, maximum=1024**2)) != promotion:
            raise SnapshotError('semantic promotion receipt changed before retirement acknowledgment')
        atomic_write_json(promotion_path, {**promotion, 'old_sources_deleted': True,
                                          'retirement': summary}, durable=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=(DATASET,), required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(run(args.old_manifest_sha256, apply=args.apply), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
