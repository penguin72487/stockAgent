#!/usr/bin/env python3
"""Materialize the existing pinned CFTC closed release on E, without cutover.

The canonical packed verifier and collector lock remain the owners. The E
copy is a staging materialization; it changes no source, pin, head or NAS.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import time

from downloader.artifact_io import atomic_write_json
from scripts import publish_data_releases as publisher
from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import closed_source, packed_snapshots as packed, workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / 'data_cftc_legacy'
COLD = Path('/srv/stockagent-packed')
MATERIALIZED = Path('/srv/stockagent-packed-materialized')
NAME = 'cftc-closed-release-e-20261104-v1'


def restore() -> dict:
    entry, = queue.catalog_entries({'source': SOURCE.name})
    closed_source.validate_catalog(entry)
    with ExitStack() as stack:
        stack.enter_context(publisher._source_coordination_lock(entry, wait_seconds=0))
        first = closed_source.resolve_closed_source(SOURCE, COLD, MATERIALIZED, hash_files=True)
        if first is None:
            raise SnapshotError('CFTC fixed source binding is unavailable')
        snapshot, original_status = first
        if original_status.get('raw_retirement_complete') is not True:
            raise SnapshotError('CFTC original raw retirement binding differs')
        root = volume.path('restore', create=True) / NAME
        if root.is_symlink():
            raise SnapshotError('E restore root is redirected')
        root.mkdir(mode=0o700, exist_ok=True)
        if root.resolve(strict=True) != root or root.stat().st_dev != volume.path('restore').stat().st_dev:
            raise SnapshotError('E restore root is not on the enrolled workspace')
        budget = int(snapshot.manifest['source']['logical_bytes']) + 128 * 1024**2
        controls = volume.path('migration', create=True) / 'provider-state-temporary-20261103/capacity-leases'
        lease = stack.enter_context(reserve_capacity(controls, key=hashlib.sha256(NAME.encode()).hexdigest(),
            maximum_growth_bytes=budget, capacity_bytes=2**63-1,
            minimum_free_bytes=volume.configuration()['minimum_free_bytes']))
        capacity = volume.admit(budget + lease['other_reserved_growth_bytes'])
        target = packed.fetch_packed_snapshot(COLD, root, snapshot,
            d_primary_native_blob_reads=True, d_primary_native_pack_reads=True)
        last = closed_source.resolve_closed_source(SOURCE, COLD, MATERIALIZED, hash_files=True)
        if last is None or last[0].manifest_sha256 != snapshot.manifest_sha256 or last[1] != original_status:
            raise SnapshotError('CFTC source binding changed during E restore')
        value = {'contract': 'stockagent.cftc_closed_E_restore.v1',
                 'state': 'fixed_release_materialized_and_readback_verified',
                 'dataset': closed_source.DATASET, 'snapshot_id': snapshot.manifest['snapshot_id'],
                 'manifest_sha256': snapshot.manifest_sha256,
                 'inventory_sha256': snapshot.manifest['archive']['inventory']['sha256'],
                 'files': snapshot.manifest['source']['files'],
                 'logical_bytes': snapshot.manifest['source']['logical_bytes'],
                 'E_materialized_path': str(target), 'raw_originals_restored': 2,
                 'source_files_deleted': 0, 'source_alias_changed': False,
                 'D_snapshot_changed': False, 'NAS_acceptance_claimed': False,
                 'capacity_at_admission': capacity, 'observed_at_epoch': time.time()}
        atomic_write_json(volume.path('verify', create=True) / (NAME + '.json'), value, durable=True)
        return value


def main() -> int:
    try:
        result = restore()
    except (publisher.SourceCoordinationLockTimeout, BlockingIOError) as error:
        print(json.dumps({'state': 'waiting_original_owner', 'error_type': type(error).__name__}))
        return 75
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == '__main__':
    os.umask(0o077)
    raise SystemExit(main())
