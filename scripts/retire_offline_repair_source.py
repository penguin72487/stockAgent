#!/usr/bin/env python3
"""Hand the fixed sealed offline repair candidate to E and retire its C payload."""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import prepare_offline_repair_archive as archive, run_workspace_source_staging as queue
from scripts.retire_local_offline_artifacts import COMMON_OWNER
from stockagent.data_sync import offline_repair_handoff, stream_repack, workspace_volume
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync.artifact_consumers import artifact_service_references
from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from stockagent.data_sync.remote_legacy_return import active_configuration_references

CONTRACT = 'stockagent.offline_repair_C_retirement.v1'


def retired_status_matches(value: dict, dataset: str) -> bool:
    result = value.get('C_retirement') if isinstance(value, dict) else None
    return (dataset in archive.DATASET_SCOPES and value.get('contract') == archive.CONTRACT
            and value.get('dataset') == dataset and value.get('state') == 'C_payload_retired'
            and value.get('C_payload_deleted') is True and isinstance(result, dict)
            and result.get('state') == 'C_payload_retired_root_and_lock_preserved'
            and result.get('prepared_sha256') == value.get('prepared_sha256')
            and value.get('source_alias_changed') is True)


def main(dataset: str = archive.DATASET) -> None:
    if dataset not in archive.DATASET_SCOPES:
        raise SnapshotError('offline repair dataset is not enrolled')
    items = [item for item in queue.load_sources() if queue.stream_scope(item) == archive.DATASET_SCOPES[dataset]]
    if len(items) != 1:
        raise SnapshotError('fixed offline repair staging enrollment differs')
    item = items[0]
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    queue.check_offline_repair_provenance(item)
    source, destination, evidence, _ = queue.paths(item, workspace)
    report = queue.verified_report(item, workspace)
    if report is None:
        raise SnapshotError('fixed offline repair E source copy is not complete')
    root = workspace/'migration/handoff-index-archive-v1'
    status_path = root/(dataset+'.status.json')
    status = queue._read_json(status_path) if status_path.exists() else {}
    if status.get('state') == 'C_payload_retired' and status.get('C_payload_deleted') is True:
        print(json.dumps({'state': 'already_retired', 'C_payload_deleted': True}), flush=True)
        return
    if (status.get('contract') != archive.CONTRACT or status.get('dataset') != dataset
            or status.get('state') != 'E_index_source_hashed_prepared'
            or status.get('C_payload_deleted') is not False):
        print(json.dumps({'state': 'waiting_for_E_archive', 'C_payload_deleted': False}), flush=True)
        return
    identity = status.get('source_identity')
    stage = root/dataset/str(identity)
    prepared = stream_repack._read_prepared(stage, dataset, identity)
    if prepared.get('identity_sha256') != status.get('prepared_sha256'):
        raise SnapshotError('offline repair E archive differs from its status')
    index = evidence/'files.sqlite'
    original_inode = prepared.get('source_root_inode')
    preserved = source.parent/('.'+source.name+'.e-handoff-'+identity[:16])
    lock_root = preserved if source.is_symlink() else source
    lock_path = lock_root/'.download.lock'
    lock_inode = lock_path.lstat().st_ino
    if archive.fixed_identity(item, report, index, original_inode, lock_inode) != identity:
        raise SnapshotError('offline repair source, receipt or lock identity differs')
    progress_path = root/(dataset+'.retirement-status.json')
    if progress_path.is_symlink():
        raise SnapshotError('offline repair retirement status redirected')

    def report_progress(event):
        atomic_write_json(progress_path, {'contract': CONTRACT, 'state': 'running', 'dataset': dataset,
            'source_identity': identity, 'files_total': status['files'],
            'logical_bytes_total': status['logical_bytes'], 'progress': event,
            'observed_at_epoch': time.time()})

    check_at = 0.0
    def guard():
        nonlocal check_at
        now = time.monotonic()
        if now < check_at:
            return
        if not handoff.binance_handoff_window_allowed(for_start=False):
            raise BlockingIOError('tw_day_trade_priority_window')
        workspace_volume.check(c=config)
        check_at = now+5.0

    with ExitStack() as stack:
        fd = os.open(COMMON_OWNER, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC)
        owner = stack.enter_context(os.fdopen(fd, 'r+b'))
        owner_info = os.fstat(owner.fileno())
        if (not stat.S_ISREG(owner_info.st_mode) or owner_info.st_uid != os.geteuid()
                or owner_info.st_nlink != 1):
            raise SnapshotError('canonical artifact mutation owner differs')
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock = stack.enter_context(handoff.lock_file(lock_path))
        if os.fstat(lock.fileno()).st_ino != lock_inode:
            raise SnapshotError('offline repair lock changed')
        if not source.is_symlink():
            if (artifact_process_references(source, source.parent)
                    or active_configuration_references(source, ROOT)
                    or artifact_service_references([source], ROOT).get(str(source))):
                raise BlockingIOError('offline repair has an active service or configuration consumer')
        guard()
        result = offline_repair_handoff.handoff(source, destination, index, stage, dataset, identity,
            original_inode=original_inode, lock_inode=lock_inode, guard=guard, progress=report_progress)
    status.update(state='C_payload_retired', C_payload_deleted=True, C_retirement=result['C_retirement'],
                  source_alias_changed=True, destination_independent_readback=False,
                  D_archive_committed=False, NAS_accepted=False, observed_at_epoch=time.time())
    atomic_write_json(status_path, status, durable=True)
    atomic_write_json(progress_path, {'contract': CONTRACT, 'state': 'completed', 'dataset': dataset,
        'source_identity': identity, 'C_payload_deleted': True, 'C_retirement': result['C_retirement'],
        'observed_at_epoch': time.time()}, durable=True)
    print(json.dumps({'state': 'C_payload_retired', 'files': result['C_retirement']['files_removed'],
                      'estimated_allocated_bytes_reclaimed':
                          result['C_retirement']['estimated_allocated_bytes_reclaimed']}), flush=True)


def run_enrolled() -> None:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    root = workspace/'migration/handoff-index-archive-v1'
    items = queue.load_sources()
    errors = []
    for dataset, scope in archive.DATASET_SCOPES.items():
        if not any(queue.stream_scope(item) == scope for item in items):
            continue
        try:
            path = root/(dataset+'.status.json')
            if not path.exists() or queue._read_json(path).get('state') not in {'E_index_source_hashed_prepared', 'C_payload_retired'}:
                print(json.dumps({'dataset': dataset, 'state': 'waiting_E_archive'}), flush=True)
                continue
            main(dataset)
        except (OSError, ValueError, SnapshotError) as error:
            errors.append(dataset)
            print(f'{dataset}: {type(error).__name__}: {error}', file=sys.stderr, flush=True)
    if errors:
        raise SnapshotError('offline repair retirement needs retry for '+', '.join(errors))


if __name__ == '__main__':
    try:
        run_enrolled()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr, flush=True)
        raise SystemExit(1)
