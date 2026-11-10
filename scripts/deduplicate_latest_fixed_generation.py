#!/usr/bin/env python3
"""Share fixed D objects for one already selected, source-hashed generation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import plan_handoff_index_integration as planner, promote_latest_fixed_generation as latest  # noqa: E402
from stockagent.data_sync import packed_member_stream as reader, shared_object_dedup, stream_repack, workspace_volume  # noqa: E402
from stockagent.data_sync.dataset_migration import private_history_root  # noqa: E402
from stockagent.data_sync.dataset_version_layout import version_policy  # noqa: E402
from stockagent.data_sync.desync_snapshots import SnapshotError  # noqa: E402


def run(old_sha: str, *, dataset: str, apply: bool) -> dict:
    if dataset not in latest.DATASETS:
        raise SnapshotError('shared-object dataset is not enrolled')
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    cold = planner.COLD
    planner._check_d_primary_mount(cold)
    private = private_history_root(cold, version_policy(ROOT), initialize=False)
    base = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'/dataset
    if base.is_symlink() or base.resolve(strict=True) != base:
        raise SnapshotError('shared-object source root is redirected')
    policy = json.loads(reader.read_stable_bytes(latest.POLICY, maximum=65536))
    matches = []
    for path in base.glob('latest-promotion-*.json'):
        value = json.loads(reader.read_stable_bytes(path, maximum=1024**2))
        if value.get('old_manifest_sha256') == old_sha:
            matches.append((path, value))
    if len(matches) != 1:
        raise SnapshotError('shared-object transaction needs one exact promotion')
    promotion_path, promoted = matches[0]
    identity = promoted.get('comparison_identity_sha256')
    if not isinstance(identity, str) or len(identity) != 64 or any(c not in '0123456789abcdef' for c in identity):
        raise SnapshotError('shared-object comparison identity is invalid')
    plans = list(base.glob('*/integration-diff-v1/diff-plan-'+identity+'.json'))
    if len(plans) != 1:
        raise SnapshotError('shared-object fixed comparison is missing')
    plan_path = plans[0]
    plan = json.loads(reader.read_stable_bytes(plan_path, maximum=1024**2))
    if plan.get('identity_sha256') != identity or stream_repack._identity(plan) != identity:
        raise SnapshotError('shared-object fixed comparison changed')
    name = plan.get('worklist', {}).get('name')
    if not isinstance(name, str) or Path(name).name != name:
        raise SnapshotError('shared-object comparison worklist is not local')
    latest.validate_worklist(plan, reader.read_stable_bytes(plan_path.parent/name, maximum=latest.MAX_WORKLIST_COMPRESSED_BYTES))
    install = plan_path.parent.parent/'install.json'
    excludes, catalog_sha = planner.policy(dataset)
    if tuple(plan.get('excluded_subtrees', ())) != excludes:
        raise SnapshotError('shared-object public scope changed')

    def guard():
        workspace_volume.check(c=config)
        planner._check_d_primary_mount(cold)
        if (private_history_root(cold, version_policy(ROOT), initialize=False) != private
                or planner.policy(dataset) != (excludes, catalog_sha)
                or json.loads(reader.read_stable_bytes(latest.POLICY, maximum=65536)) != policy):
            raise SnapshotError('shared-object authority or selection policy changed')

    outcome = shared_object_dedup.consolidate(cold, private, promotion_path=promotion_path,
        install_path=install, journal_path=base/('shared-objects-'+identity+'.json'), plan=plan, policy=policy,
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                     Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
        guard=guard, apply=apply)
    return {key: outcome.get(key) for key in ('contract', 'dataset', 'state', 'apply',
        'old_manifest_sha256', 'object_count', 'object_bytes', 'estimated_allocated_bytes_reclaimed',
        'independent_D_readback', 'NAS_accepted')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=latest.DATASETS, required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--apply', action='store_true')
    try:
        args = parser.parse_args()
        print(json.dumps(run(args.old_manifest_sha256, dataset=args.dataset, apply=args.apply), ensure_ascii=False))
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
