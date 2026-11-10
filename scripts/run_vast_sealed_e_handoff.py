#!/usr/bin/env python3
"""Resume the fixed Vast rescue package's C-to-E source-streamed copy."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.return_remote_legacy_archives import cohort_owner
from scripts.run_bounded_storage_maintenance import TradingPriorityGuard
from scripts.run_dedup_candidate_queue import process_identity
from stockagent.data_sync import sealed_workspace_copy as copy, sealed_workspace_handoff as handoff, workspace_volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes, atomic_write_json

SOURCE_ROOT = Path('/var/lib/stockagent/vast-pre-stop-20261010')
SOURCE = SOURCE_ROOT/'recovery-package'
EXPECTED = 'b380dfb674db806d69f5829966954d4a26e3973477f26778c4fac63058bd3e5a'
CONTRACT = 'stockagent.vast_sealed_e_handoff.v1'
UNIT = r'var-lib-stockagent-vast\x2dpre\x2dstop\x2d20261010-recovery\x2dpackage.mount'
UNIT_PATH = Path('/etc/systemd/system')/UNIT


def mount_matches(data: Path, source: Path) -> bool:
    try:
        left, right = data.stat(), source.stat()
        return (not source.is_symlink() and source.is_mount()
                and (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino))
    except OSError:
        return False


def mount_ensure(data: Path, source: Path) -> None:
    if (source != SOURCE or data != Path('/srv/stockagent-workspace/migration/vast-pre-stop-sealed-e-v1/package-copy/data')
            or source.is_symlink() or data.is_symlink() or data.resolve(strict=True) != data):
        raise SnapshotError('Vast sealed E mount is outside its fixed enrolled paths')
    if source.is_mount() and not mount_matches(data, source):
        raise SnapshotError('Vast rescue path already has another mount')
    text = ('[Unit]\nDescription=Fixed Vast rescue package placed on E\n'
            'Requires=stockagent-e-workspace-mount.service\nAfter=stockagent-e-workspace-mount.service\n'
            '[Mount]\nWhat='+str(data)+'\nWhere='+str(source)+'\nType=none\nOptions=bind\n'
            '[Install]\nWantedBy=multi-user.target\n').encode()
    if os.path.lexists(UNIT_PATH):
        if UNIT_PATH.is_symlink() or not UNIT_PATH.is_file() or UNIT_PATH.read_bytes() != text:
            raise SnapshotError('Vast rescue mount unit differs from its enrolled binding')
    else:
        atomic_write_bytes(UNIT_PATH, text, mode=0o644)
        subprocess.run(['systemctl', 'daemon-reload'], check=True, timeout=30)
    wants = Path('/etc/systemd/system/multi-user.target.wants')/UNIT
    if mount_matches(data, source) and wants.is_symlink() and wants.resolve(strict=True) == UNIT_PATH:
        return  # Already mounted and persistently enrolled; no repeated systemctl work.
    try:
        subprocess.run(['systemctl', 'enable', '--now', UNIT], check=True, timeout=45,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if not mount_matches(data, source):
            raise SnapshotError('Vast rescue mount did not expose the exact E inode')
    except BaseException:
        # If startup failed before our mount became visible, do not leave an
        # enabled unit that might hide the restored C path on the next boot.
        if not mount_matches(data, source):
            subprocess.run(['systemctl', 'disable', UNIT], check=False, timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        raise


def main(*, recover_mount_only=False) -> int:
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    workspace = Path(config['mount_point'])
    root = workspace_volume.path('migration', create=True)/'vast-pre-stop-sealed-e-v1'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root or root.stat().st_mode & 0o077:
        raise SnapshotError('Vast sealed workspace root is redirected or not private')
    stage, status = root/'package-copy', root/'status.json'
    owner, guard = process_identity(), TradingPriorityGuard()
    last = 0.0

    def report(event, *, force=False):
        nonlocal last
        guard.check()
        if not force and time.monotonic()-last < 3:
            return
        last = time.monotonic()
        retiring = event.get('stage') == 'retiring_sealed_C_source'
        atomic_write_json(status, {'contract': CONTRACT, 'state': 'retiring' if retiring else 'copying', 'progress': event,
            'observed_at_epoch': time.time(), 'owner_identity': owner,
            'source_deleted': False, 'destination_readback': False, 'mount_handoff_completed': retiring},
            durable=False)

    try:
        cohort = json.loads((SOURCE_ROOT/'private-recovery-cohort.json').read_bytes())
        if (cohort.get('contract') != 'private_vast_pre_stop_recovery_cohort_v1'
                or cohort.get('package_identity_sha256') != EXPECTED):
            raise SnapshotError('fixed Vast rescue package identity differs')
        controls = workspace/'migration/provider-state-temporary-20261103/capacity-leases'
        with cohort_owner(SOURCE_ROOT):
            if recover_mount_only:
                handoff.recover_mount(SOURCE, stage, EXPECTED, mount_ensure=mount_ensure,
                                      mount_matches=mount_matches)
                return 0
            if not (root/'handoff.json').exists():
                rows, _ = copy.selection(SOURCE, EXPECTED)
                checkpoint = stage/'copy-checkpoint.json'
                existing = json.loads(checkpoint.read_bytes()) if checkpoint.is_file() and not checkpoint.is_symlink() else {}
                done = sum(row.get('bytes', 0) for row in existing.get('files', {}).values()
                           if isinstance(row, dict) and row.get('state') == 'completed'
                           and type(row.get('bytes')) is int and row['bytes'] >= 0)
                total = sum(row['bytes'] for row in rows)
                budget = max(0, total-min(done, total))+64*1024**2
                with reserve_capacity(controls, key=EXPECTED, maximum_growth_bytes=budget,
                        capacity_bytes=2**63-1, minimum_free_bytes=config['minimum_free_bytes']) as lease:
                    workspace_volume.admit(budget+lease['other_reserved_growth_bytes'], c=config)
                    report({'stage': 'starting', 'files_completed': 0, 'files_total': len(rows),
                            'bytes_completed': 0, 'bytes_total': total}, force=True)
                    copy.copy(SOURCE, stage, EXPECTED, progress=report, guard=guard.check)
            result = handoff.handoff(SOURCE, stage, EXPECTED, mount_ensure=mount_ensure,
                                     mount_matches=mount_matches, progress=report, guard=guard.check)
        atomic_write_json(status, {'contract': CONTRACT, 'state': 'completed',
            'observed_at_epoch': time.time(), 'files_completed': len(result['files']),
            'bytes_completed': result['reclaimed_allocated_bytes'], 'manifest_sha256': EXPECTED,
            'source_deleted': True, 'destination_readback': False, 'mount_handoff_completed': True})
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        is_mounted = mount_matches(stage/'data', SOURCE)
        atomic_write_json(status, {'contract': CONTRACT, 'state': 'waiting_retry',
            'observed_at_epoch': time.time(), 'error_type': type(error).__name__,
            'source_deleted': False, 'destination_readback': False, 'mount_handoff_completed': is_mounted})
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    if sys.argv[1:] not in ([], ['--recover-mount-only']):
        raise SystemExit('only --recover-mount-only is supported')
    raise SystemExit(main(recover_mount_only=bool(sys.argv[1:])))
