#!/usr/bin/env python3
"""Seal one explicitly enrolled retained C original into resumable E packs."""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_stream_packed_repack import stage_growth_budget
from stockagent.data_sync import handoff_source_repack, packed_snapshots, provider_workspace, stream_repack
from stockagent.data_sync import workspace_source_handoff as handoff, workspace_volume
from stockagent.data_sync.cold_primary import _check_d_primary_mount
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.dataset_migration import private_history_root
from stockagent.data_sync.dataset_version_layout import version_policy
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_source_repack.v1'
ENROLLED = {
    handoff.COIN_NAME: 'coinmetrics-community',
    'keyed-public-catalogs-e-primary-20261104-v1': 'keyed-public-catalogs',
    'crypto-reference-e-primary-20261104-v1': 'crypto-reference',
    'dune-crypto-e-primary-20261104-v1': 'dune-crypto',
    handoff.NAME: 'crypto-etf-history',
    'crypto-public-context-e-primary-20261104-v1': 'crypto-historical-public',
}
COLD = Path('/srv/stockagent-packed')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=sorted(ENROLLED), required=True)
    parser.add_argument('--retire', action='store_true',
                        help='after E preparation, journal and remove only the fixed C payload; retain root and lock')
    args = parser.parse_args()
    name, dataset = args.source, ENROLLED[args.source]
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    _check_d_primary_mount(COLD)
    workspace = Path(config['mount_point'])
    root = workspace_volume.path('migration', create=True)/'handoff-source-repack-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('handoff preparation root redirected')
    status = root/(dataset+'.status.json')
    if status.is_symlink():
        raise SnapshotError('handoff preparation status redirected')
    previous = {}
    if status.is_file():
        previous = json.loads(status.read_text())
        if not isinstance(previous, dict) or previous.get('contract') != CONTRACT or previous.get('dataset') != dataset:
            raise SnapshotError('handoff preparation status identity differs')
        if (args.retire
                and previous.get('state') == 'C_payload_retired_root_and_lock_preserved'
                and previous.get('C_payload_deleted') is True):
            # Do not treat a later recreation of the same C pathname as part
            # of this completed generation or repeatedly scan all processes.
            print(json.dumps(previous, ensure_ascii=False))
            return 0
    last_report = 0.0
    with handoff.selected_source(name), ExitStack() as stack:
        value = handoff.read_journal()
        if not value or value.get('state') != 'active':
            raise SnapshotError('retained source handoff is not active')
        handoff._assert_retained(value)
        scope = handoff._scope()
        destination = Path(value['destination'])
        if not provider_workspace.exact_mount(scope.source, destination):
            raise SnapshotError('service source does not point at its enrolled E mount')
        # Only the old retained C writer lock is held for the source scan.
        # The current E collector uses its own destination lock.
        stack.enter_context(handoff.lock_file(scope.backup/'.download.lock'))
        private = private_history_root(COLD, version_policy(ROOT), initialize=False)
        # A completed E preparation is tied to a particular source generation.
        # After a partial C retirement, a newer D head must not make us read
        # another generation or strand the journal for the already sealed one.
        previous_sha = previous.get('original_manifest_sha256')
        prepared_stage = None
        if previous_sha is not None:
            previous_sha = packed_snapshots._validate_hash(previous_sha, 'fixed private manifest SHA')
            candidate = root/dataset/previous_sha
            if (candidate/'prepared.json').exists():
                prepared_stage = stream_repack._read_prepared(candidate, dataset, previous_sha)
        if prepared_stage is None:
            latest = packed_snapshots.resolve_latest_packed(private, dataset, require_objects=False)
            expected_sha = latest.manifest_sha256
            if previous_sha is not None and previous_sha != expected_sha:
                raise SnapshotError('private D head changed before E preparation completed')
            source = latest.manifest['source']
        else:
            expected_sha = previous_sha
            source = {'files': prepared_stage['files'], 'logical_bytes': prepared_stage['logical_bytes']}
        if (source.get('files') != value.get('files')
                or source.get('logical_bytes') != value.get('logical_bytes')):
            raise SnapshotError('private fixed release and retained handoff have different totals')
        stage = root/dataset/expected_sha
        budget = stage_growth_budget(stage, dataset, expected_sha,
            source['logical_bytes'], source['files'], 0, already_committed=False)
        controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
        def guard():
            workspace_volume.check(c=config)
            _check_d_primary_mount(COLD)
            handoff._assert_retained(value)
        def report(event):
            nonlocal last_report
            if time.monotonic()-last_report < 3 and event.get('stage') == 'streaming_source_into_E_packs':
                return
            last_report = time.monotonic()
            atomic_write_json(status, {'contract': CONTRACT, 'state': 'running', 'dataset': dataset,
                'files_total': source['files'], 'logical_bytes_total': source['logical_bytes'],
                'original_manifest_sha256': expected_sha, 'observed_at_epoch': time.time(),
                'progress': event})
        with reserve_capacity(controls, key=expected_sha, maximum_growth_bytes=budget,
                capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
            workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
            report({'stage': 'streaming_source_into_E_packs', 'files_completed': 0, 'logical_bytes_processed': 0})
            prepared = handoff_source_repack.prepare(private, scope.backup, stage, dataset,
                expected_sha, Path(value['evidence'])/'files.sqlite',
                original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
                guard=guard, progress=report)
        result = {'contract': CONTRACT, 'state': 'E_source_hashed_prepared', 'dataset': dataset,
            'original_manifest_sha256': expected_sha, 'prepared_sha256': prepared['identity_sha256'],
            'files': prepared['files'], 'logical_bytes': prepared['logical_bytes'],
            'E_packages': len(prepared['packages']), 'C_payload_deleted': False,
            'D_new_head_committed': False, 'NAS_accepted': False,
            'observed_at_epoch': time.time()}
        atomic_write_json(status, result, durable=True)
        if args.retire:
            # Preserve the handoff owner's lock ordering. The long E build
            # held only the old C collector lock; release it before entering
            # the short owner + C/E writer boundary for exact retirement.
            stack.close()
            try:
                with ExitStack() as retirement_locks:
                    retirement_locks.enter_context(handoff.lock_file(handoff.CONTROL/'owner.lock'))
                    handoff._lock_writer_roots(retirement_locks, scope.backup, destination)
                    current = handoff.read_journal()
                    if not isinstance(current, dict):
                        raise SnapshotError('active handoff journal disappeared before C retirement')
                    for key in ('source', 'backup', 'destination', 'evidence', 'original_inode',
                                'original_lock_inode', 'destination_inode', 'copy_result_sha256'):
                        if current.get(key) != value.get(key):
                            raise SnapshotError('active handoff identity changed before C retirement')
                    if current.get('state') != 'active' or not provider_workspace.exact_mount(scope.source, destination):
                        raise SnapshotError('active E service mount changed before C retirement')
                    retired = handoff_source_repack.retire(scope.backup, stage, dataset,
                        expected_sha, Path(value['evidence'])/'files.sqlite',
                        original_inode=value['original_inode'], original_lock_inode=value['original_lock_inode'],
                        guard=guard, progress=report)
                result.update(state=retired['state'], C_payload_deleted=True, C_retirement=retired,
                              observed_at_epoch=time.time())
                atomic_write_json(status, result, durable=True)
            except (OSError, ValueError, SnapshotError) as error:
                atomic_write_json(status, {**result, 'state': 'C_retirement_waiting',
                    'error_type': type(error).__name__, 'observed_at_epoch': time.time()}, durable=True)
                raise
        print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
