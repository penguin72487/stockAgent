#!/usr/bin/env python3
"""Repair only an idle, enrolled D: root mount that drifted from 8 KiB.

The canonical cold bind stays mounted.  Busy owners and an active trading
window defer the operation; no process is stopped and no forced detach is used.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.check_outside_tw_opening_resource_window import TAIPEI
from scripts.run_bounded_storage_maintenance import allowed_seconds
from stockagent.data_sync import packed_snapshots as packed, repack_retirement, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

VOLUME = Path('/srv/stockagent-d-volume')
CANONICAL = Path('/srv/stockagent-packed')
PRIMARY = VOLUME/'stockagent-cold-primary/packed'
CHECKER = ROOT/'scripts/mount_packed_d_cold.sh'
STATUS = Path('/var/lib/stockagent-d-root-mount-repair/status.json')


def _run(*command, timeout=30):
    return subprocess.run(command, text=True, capture_output=True, timeout=timeout, check=False)


def _field(path: Path, field: str) -> str:
    result = _run('findmnt', '-n', '-o', field, '-T', str(path))
    if result.returncode or '\n' in result.stdout.strip():
        raise SnapshotError('D mount metadata is unavailable or ambiguous')
    return result.stdout.strip()


def _canonical_identity():
    marker = CANONICAL/'.stockagent-d-primary'
    node = CANONICAL/'.local-state/node-id'
    if (CANONICAL.is_symlink() or _field(CANONICAL, 'TARGET') != str(CANONICAL)
            or _field(CANONICAL, 'SOURCE') != 'D:[/stockagent-cold-primary/packed]'
            or _field(CANONICAL, 'FSTYPE') != '9p'
            or 'msize=8192' not in _field(CANONICAL, 'OPTIONS').split(',')
            or marker.is_symlink() or node.is_symlink() or not marker.is_file() or not node.is_file()):
        raise SnapshotError('canonical D bind identity differs')
    expected = {'schema_version': 1, 'volume_id': '9ba6ab87-3889-40e7-90e4-9597dc88abaf',
                'backing': 'drvfs_msize8192', 'authority_node_id': 'penguin', 'resilience': 'single_d_volume'}
    if json.loads(marker.read_text()) != expected or node.read_text().strip() != 'penguin':
        raise SnapshotError('canonical D authority marker differs')


def _volume_identity():
    result = _run('bash', str(CHECKER), '--check-volume-identity')
    if result.returncode:
        raise SnapshotError('D root mount is not the enrolled volume')
    if (PRIMARY.is_symlink() or PRIMARY.resolve(strict=True) != PRIMARY
            or PRIMARY.stat().st_ino != CANONICAL.stat().st_ino
            or (PRIMARY/'.stockagent-d-primary').read_bytes() != (CANONICAL/'.stockagent-d-primary').read_bytes()
            or (PRIMARY/'.local-state/node-id').read_bytes() != (CANONICAL/'.local-state/node-id').read_bytes()):
        raise SnapshotError('D root and canonical bind do not identify the same directory')


def repair(*, apply=False):
    if type(apply) is not bool:
        raise SnapshotError('invalid mount repair mode')
    if _run('bash', str(CHECKER), '--check').returncode == 0:
        return {'state': 'healthy', 'changed': False}
    _canonical_identity()
    mounted = _field(VOLUME, 'TARGET') == str(VOLUME)
    if mounted:
        _volume_identity()
        options = _field(VOLUME, 'OPTIONS').split(',')
        if 'msize=8192' in options:
            raise SnapshotError('D root has 8 KiB requests but another authority check failed')
    elif VOLUME.is_symlink() or not VOLUME.is_dir() or any(VOLUME.iterdir()):
        raise SnapshotError('unmounted D root is not an empty enrolled mountpoint')
    if allowed_seconds(datetime.now(TAIPEI)) < 120:
        return {'state': 'waiting_trading_priority_window', 'changed': False}
    if not apply:
        return {'state': 'repairable_mount_drift', 'changed': False, 'canonical_bind_retained': True}
    config = workspace_volume.configuration()
    workspace = Path(config['mount_point'])
    with ExitStack() as stack:
        stack.enter_context(packed._try_publication_lock(Path('/run/lock/stockagent-vast-bulk-organizer.lock')))
        stack.enter_context(packed._try_publication_lock(Path('/run/lock/stockagent-remote-cold-artifact-ingress.lock')))
        stack.enter_context(repack_retirement._boundary(CANONICAL, (
            Path('/var/lib/stockagent/backup-stream/cycle.lock'),
            Path('/var/lib/stockagent/lakehouse-control/source-replication-owner.lock')), attempts=1))
        stack.enter_context(packed._try_publication_lock(workspace/'migration/cold-install-write-owner.lock'))
        if allowed_seconds(datetime.now(TAIPEI)) < 120:
            return {'state': 'waiting_trading_priority_window', 'changed': False}
        workspace_volume.check(c=config)
        _canonical_identity()
        if mounted:
            _volume_identity()
            busy = _run('fuser', '-m', str(VOLUME))
            if busy.returncode not in (0, 1) or busy.stdout.strip() or busy.stderr.strip():
                return {'state': 'waiting_D_mount_users', 'changed': False}
            result = _run('umount', str(VOLUME))
            if result.returncode:
                return {'state': 'waiting_D_mount_users', 'changed': False}
        # Once unmounted, always attempt a mount even if the first attempt
        # fails.  No --force, --lazy, filesystem trim or process termination.
        command = ('mount', '-t', 'drvfs', 'D:', str(VOLUME), '-o', 'metadata,msize=8192,uid=0,gid=0,noatime')
        try:
            result = _run(*command)
            if result.returncode:
                raise SnapshotError('D root remount failed; retry scheduled')
        finally:
            if not os.path.ismount(VOLUME):
                _run(*command)
        result = _run('bash', str(CHECKER), '--check')
        if result.returncode:
            raise SnapshotError('D root remounted but authority check still fails')
        return {'state': 'repaired', 'changed': True, 'canonical_bind_retained': True,
                'online_trim_performed': False, 'processes_stopped': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    try:
        result = repair(apply=args.apply)
    except (SnapshotError, OSError, ValueError, subprocess.SubprocessError, packed.PackedPublicationBusy) as error:
        result = {'state': 'deferred', 'changed': False, 'reason': str(error)}
    result.update(contract='stockagent.d_root_mount_repair.v1', observed_at_epoch=time.time())
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(STATUS, result, durable=True)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0 if result['state'] in ('healthy', 'repaired', 'repairable_mount_drift') else 75


if __name__ == '__main__':
    raise SystemExit(main())
