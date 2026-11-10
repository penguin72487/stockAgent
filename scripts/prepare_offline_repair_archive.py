#!/usr/bin/env python3
"""Seal explicitly enrolled offline day-trade sources in E source packs.

The existing source-index archive format and its checkpoint owner do the byte
work.  This step neither switches the old path nor deletes or publishes it.
"""
from __future__ import annotations

from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import run_workspace_source_staging as queue
from scripts.run_stream_packed_repack import stage_growth_budget
from stockagent.data_sync import handoff_index_archive, workspace_relocation, workspace_volume
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.materialized_cache import process_references_many

DATASET = 'tw-daytrade-offline-repair-workspace'
ROLLBACK_DATASET = 'tw-daytrade-close-rollback-workspace'
DATASET_SCOPES = {DATASET: queue.OFFLINE_REPAIR_SCOPE, ROLLBACK_DATASET: queue.OFFLINE_ROLLBACK_SCOPE}
CONTRACT = 'stockagent.offline_repair_index_archive.v1'


def fixed_identity(item: dict, report: dict, index: Path, original_inode: int, lock_inode: int) -> str:
    body = {'contract': CONTRACT+'.source', 'source': item['source'],
        'copy_result_sha256': report['relocation_evidence_sha256'],
        'source_receipt_sha256': queue.offline_repair_receipt_sha(item),
        'source_index_signature': workspace_relocation.signature(index),
        'original_inode': original_inode, 'supplemental_lock_inode': lock_inode}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def main(dataset: str = DATASET) -> None:
    if dataset not in DATASET_SCOPES:
        raise SnapshotError('offline repair dataset is not enrolled')
    items = [item for item in queue.load_sources() if queue.stream_scope(item) == DATASET_SCOPES[dataset]]
    if len(items) != 1:
        raise SnapshotError('fixed offline repair staging enrollment differs')
    item = items[0]
    queue.check_offline_repair_provenance(item)
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    source, destination, evidence, _ = queue.paths(item, workspace)
    report = queue.verified_report(item, workspace)
    if report is None:
        raise SnapshotError('fixed offline repair E source copy is not complete')
    status = workspace/'migration/handoff-index-archive-v1'/(dataset+'.status.json')
    if status.exists() and not status.is_symlink():
        previous = queue._read_json(status)
        if (previous.get('contract') == CONTRACT and previous.get('dataset') == dataset
                and previous.get('state') == 'C_payload_retired' and previous.get('C_payload_deleted') is True):
            print(json.dumps(previous, ensure_ascii=False), flush=True)
            return
    if process_references_many([source]):
        raise BlockingIOError('offline repair source has an active process reference')
    intent = queue._read_json(evidence/'intent.json')
    index = evidence/'files.sqlite'
    root_signature = intent.get('source_root_signature')
    if (intent.get('state') != 'indexed' or intent.get('source') != str(source)
            or intent.get('destination') != str(destination)
            or not isinstance(root_signature, list) or len(root_signature) != 9
            or source.is_symlink() or source.resolve(strict=True) != source
            or source.stat().st_ino != root_signature[1]):
        raise SnapshotError('fixed offline repair source index differs')
    original_inode = root_signature[1]
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('offline repair archive root redirected')
    status = root/(dataset+'.status.json')
    if status.is_symlink():
        raise SnapshotError('offline repair archive status redirected')
    last = 0.0
    with ExitStack() as stack:
        # The original candidate had no collector lock. This empty,
        # supplemental file is not part of its indexed source payload.
        lock = stack.enter_context(handoff._lock_supplemental_file(source/'.download.lock'))
        lock_inode = os.fstat(lock.fileno()).st_ino
        identity = fixed_identity(item, report, index, original_inode, lock_inode)
        stage = root/dataset/identity
        guard_at = 0.0

        def guard() -> None:
            nonlocal guard_at
            now = time.monotonic()
            if now < guard_at:
                return
            if not handoff.binance_handoff_window_allowed(for_start=False):
                raise BlockingIOError('tw_day_trade_priority_window')
            workspace_volume.check(c=config)
            if (source.is_symlink() or source.stat().st_ino != original_inode
                    or destination.is_symlink() or not destination.is_dir()
                    or (source/'.download.lock').stat().st_ino != lock_inode):
                raise SnapshotError('fixed offline repair source or destination changed')
            guard_at = now + 5.0

        def progress(event: dict) -> None:
            nonlocal last
            now = time.monotonic()
            if now-last < 2.0 and event.get('stage') == 'streaming_source_into_E_packs':
                return
            last = now
            atomic_write_json(status, {'contract': CONTRACT, 'state': 'running', 'dataset': dataset,
                'source_identity': identity, 'files_total': report['files'],
                'logical_bytes_total': report['logical_bytes'], 'progress': event,
                'C_payload_deleted': False, 'D_archive_committed': False,
                'observed_at_epoch': time.time()})

        guard()
        budget = stage_growth_budget(stage, dataset, identity, report['logical_bytes'], report['files'], 0,
                                     already_committed=False)
        controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
        with reserve_capacity(controls, key=identity, maximum_growth_bytes=budget,
                capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
            workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
            prepared = handoff_index_archive.prepare(source, stage, dataset, identity, index,
                original_inode=original_inode, original_lock_inode=lock_inode,
                preserved_paths=['.download.lock'], supplemental_root_lock=True,
                guard=guard, progress=progress, large_member_deflate_workers=4)
    value = {'contract': CONTRACT, 'state': 'E_index_source_hashed_prepared', 'dataset': dataset,
        'source_identity': identity, 'prepared_sha256': prepared['identity_sha256'],
        'files': prepared['files'], 'logical_bytes': prepared['logical_bytes'],
        'E_packages': len(prepared['packages']), 'C_payload_deleted': False,
        'D_archive_committed': False, 'destination_independent_readback': False,
        'NAS_accepted': False, 'observed_at_epoch': time.time()}
    atomic_write_json(status, value, durable=True)
    print(json.dumps(value, ensure_ascii=False), flush=True)


def run_enrolled() -> None:
    """A missing staging receipt means queued work, not a failed archive."""
    items = queue.load_sources()
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    errors = []
    for dataset, scope in DATASET_SCOPES.items():
        if not any(queue.stream_scope(item) == scope for item in items):
            continue
        try:
            item = next(item for item in items if queue.stream_scope(item) == scope)
            if not queue.paths(item, workspace)[3].exists():
                print(json.dumps({'dataset': dataset, 'state': 'waiting_source_staging'}), flush=True)
                continue
            main(dataset)
        except (OSError, ValueError, SnapshotError) as error:
            errors.append(dataset)
            print(f'{dataset}: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
    if errors:
        raise SnapshotError('offline repair archive needs retry for '+', '.join(errors))


if __name__ == '__main__':
    try:
        run_enrolled()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr, flush=True)
        raise SystemExit(1)
