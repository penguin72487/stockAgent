#!/usr/bin/env python3
"""Publish one fixed relay installer with public code and pinned rclone only."""
import fcntl
import json
from pathlib import Path
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.control.lakehouse import configuration, guard
from stockagent.data_sync.immutable_replication import seal, verify, digest
from stockagent.data_sync.packed_backup import mounted_volume
from downloader.artifact_io import atomic_write_json

c = configuration()
guard(c)
transport = Path(c['transport_root']).parent
physical = Path('/srv/stockagent-d-volume/stockagent-backup-ingress-lab203')
if not physical.samefile(transport):
    raise ValueError('control handoff needs the enrolled physical transport alias')
destination = physical / 'tools/immutable-lake-relay-20261005-v1'
if destination.exists():
    raise ValueError('preserve previously sealed installer package')
with Path('/var/lib/stockagent/backup-stream/handoff-publisher.lock').open('a') as owner:
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    staging = physical.parent / 'stockagent-control-handoff-staging' / ('lake-relay-' + uuid.uuid4().hex)
    staging.mkdir(parents=True, mode=0o700)
    # Only the fixed adapter and its canonical dependencies. Unrelated live
    # collectors/model development must not destabilize a relay deployment.
    names = (
        'scripts/__init__.py', 'scripts/runtime_env.sh', 'scripts/manage_runtime_environments.py',
        'scripts/verify_backup_delivery.py', 'scripts/backup_delivery_receipt.py',
        'scripts/install_lab203_lake_relay.py', 'scripts/run_lab203_lake_relay.py', 'scripts/run_lab203_lake_relay.sh',
        'stockagent/__init__.py', 'stockagent/runtime_identity.py', 'stockagent/control/__init__.py',
        'stockagent/control/lakehouse.py', 'stockagent/control/backup.py', 'stockagent/control/recovery.py',
        'stockagent/data_sync/__init__.py', 'stockagent/data_sync/immutable_replication.py',
        'stockagent/data_sync/nas_target.py', 'stockagent/data_sync/offhost_backup.py',
        'stockagent/data_sync/packed_backup.py', 'stockagent/data_sync/packed_snapshots.py',
        'stockagent/data_sync/desync_snapshots.py', 'stockagent/data_sync/backup_auxiliary.py',
        'downloader/__init__.py', 'downloader/artifact_io.py',
        'configs/environments/locks/lakehouse-control-linux-64-20261005.explicit.txt',
        'docs/ducklake_temporal_replication_2026-10-05.md',
    )
    files = {name: digest(ROOT / name) for name in names}
    for name, expected in files.items():
        source = ROOT / name
        if digest(source) != expected:
            raise ValueError('public source changed during fixed handoff capture')
        target = staging / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        if digest(target) != expected:
            raise ValueError('fixed handoff copy differs')
    binary = staging / 'bin/rclone'
    binary.parent.mkdir()
    shutil.copyfile(Path(c['binaries']) / 'bin/rclone', binary)
    if sum(p.stat().st_size for p in staging.rglob('*') if p.is_file()) > 256 * 1024**2:
        raise ValueError('relay handoff exceeds its bounded public-code budget')
    manifest = seal(staging, {'kind': 'fixed_lab203_lake_relay_installation',
        'producer_device_id': c['producer_device_id'], 'receiver_device_id': c['receiver_device_id'],
        'automatic_execution_from_syncthing': False, 'private_credentials_included': False})
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging.rename(destination)
    verify(destination)
    result = {'state': 'published', 'package_relative': 'tools/' + destination.name,
              'package_identity_sha256': manifest['identity_sha256'],
              'installer_sha256': manifest['files']['scripts/install_lab203_lake_relay.py']['sha256'],
              'private_credentials_included': False, 'remote_installed': False}
    atomic_write_json(Path(c['state_root']) / 'lab203-handoff.json', result)
    print(json.dumps(result))
