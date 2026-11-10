#!/usr/bin/env python3
"""Run the E-first owner for one sealed public or enrolled private D generation."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_stream_packed_repack import COLD, install_reserve_bytes
from stockagent.data_sync import bulk_archive, bulk_landing_alias_retirement as landing
from stockagent.data_sync import bulk_original_carrier_retirement as carrier
from stockagent.data_sync import packed_snapshots as p, staged_head_capacity_relief as relief, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.windows_cold_io import metadata_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', required=True)
    parser.add_argument('--old-manifest-sha256', required=True)
    parser.add_argument('--private-source', action='store_true')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    dataset = p.validate_slug(args.dataset, 'dataset')
    old_sha = p._validate_hash(args.old_manifest_sha256, 'old manifest SHA')
    config = workspace_volume.configuration()
    workspace = Path(config['mount_point'])
    stage = workspace/'migration/stream-source-repack-v1'/dataset/old_sha
    priority = TradingPriorityGuard()
    cold = private_history_root(COLD, version_policy(ROOT), initialize=False) if args.private_source else COLD

    def guard():
        priority.check()
        workspace_volume.check(c=config)
        _check_d_primary_mount(COLD)
        if args.private_source and private_history_root(COLD, version_policy(ROOT), initialize=False) != cold:
            raise SnapshotError('private current-head relief D authority changed')

    guard()
    options = dict(
        catalog=Path('/var/lib/stockagent/backup-stream/catalog.json'),
        observation=Path('/var/lib/stockagent/backup-stream/catalog-observation.json'),
        cache=workspace/'migration/d-cold-object-staging-v1/catalog-index-cache.json',
        materialized=Path('/srv/stockagent-packed-materialized'),
        retention_state=Path('/var/lib/stockagent-packed-retention'),
        install_lock=workspace/'migration/cold-install-write-owner.lock',
        reserve_bytes=install_reserve_bytes(dataset, old_sha, private=args.private_source),
        extra_locks=(Path('/var/lib/stockagent/backup-stream/cycle.lock'),
                     Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')),
        guard=guard, apply=args.apply)
    outcome = (relief.relieve_private(cold, stage, dataset, old_sha, public_cold=COLD, **options)
               if args.private_source else relief.relieve(cold, stage, dataset, old_sha, **options))
    alias = None
    if not args.private_source and outcome['state'] == 'insufficient_exclusive_space':
        # Some retained Vast bulk sources have a second D hardlink in the
        # old landing directory. Release only exact aliases represented by the
        # already-sealed E generation; the ordinary D object remains intact.
        # The existing relief owner then recomputes references and capacity.
        alias = landing.release(COLD, stage, dataset, old_sha,
            incoming=metadata_path(bulk_archive.INCOMING),
            coordinator_lock=Path('/run/lock/stockagent-vast-bulk-organizer.lock'),
            ingress_lock=Path('/run/lock/stockagent-remote-cold-artifact-ingress.lock'),
            install_lock=options['install_lock'], extra_locks=options['extra_locks'],
            guard=guard, apply=args.apply)
        if args.apply and alias['state'] in {'completed', 'no_matching_aliases'}:
            # Capture only the small ordered-parts metadata while the sealed
            # E view still exists. A corrupt/mismatched plan stops before any
            # D blob is retired; the old blobs remain available for retry.
            carrier.capture(stage, dataset, old_sha)
        if args.apply and alias['state'] == 'completed':
            outcome = relief.relieve(COLD, stage, dataset, old_sha, **options)
    if alias is not None:
        outcome['landing_alias_state'] = alias.get('state')
        outcome['landing_alias_names_removed'] = alias.get('cursor', 0)
        outcome['landing_alias_allocated_bytes_reclaimed'] = alias.get('allocated_bytes_reclaimed', 0)
    # The large exact plan stays in E. Return only bounded status to the queue.
    print(json.dumps({key: outcome.get(key) for key in (
        'contract', 'dataset', 'private', 'state', 'planned_object_count', 'planned_allocated_bytes',
        'estimated_allocated_bytes_reclaimed', 'D_shortfall_bytes', 'exclusive_allocated_bytes',
        'landing_alias_state', 'landing_alias_names_removed', 'landing_alias_allocated_bytes_reclaimed',
        'temporary_standard_D_head_unavailable', 'independent_D_readback', 'NAS_accepted')},
        ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
