#!/usr/bin/env python3
"""Keep private logical backups of the dedicated engineering control database."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import urlsplit
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json, durable_replace  # noqa: E402
from stockagent.runtime_identity import runtime_identity  # noqa: E402
from stockagent.control.backup import logical_database_state  # noqa: E402


def private_json(path: Path, value: dict) -> None:
    old = os.umask(0o077)
    try:
        atomic_write_json(path, value)
    finally:
        os.umask(old)


def backup(output: Path) -> dict:
    if os.geteuid() != 0:
        raise ValueError('dedicated database backup uses the local postgres OS account')
    connection = urlsplit(os.environ['CONTROL_PLANE_DSN'])
    if connection.hostname not in {'127.0.0.1', 'localhost'} or connection.path != '/stockagent_control':
        raise ValueError('backup is confined to the dedicated local control database')
    output = output.resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output.stat().st_mode & 0o077:
        raise ValueError('control backups directory must remain private')
    if shutil.disk_usage(output).free < 1024**3:
        raise ValueError('control backup needs 1 GiB free capacity')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '-' + uuid.uuid4().hex[:8]
    archive = output / (stamp + '.backup')
    partial = archive.with_suffix('.partial')
    started = time.perf_counter()
    import psycopg
    with psycopg.connect(os.environ['CONTROL_PLANE_DSN'], autocommit=True) as database:
        with database.transaction():
            database.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            exported = database.execute('SELECT pg_export_snapshot()').fetchone()[0]
            state = logical_database_state(database)
            with partial.open('xb') as handle:
                partial.chmod(0o600)
                result = subprocess.run(['runuser', '-u', 'postgres', '--', 'pg_dump', '-p', str(connection.port or 5432),
                                         '-Fc', '--no-owner', '--no-privileges', '--snapshot', exported,
                                         '-d', 'stockagent_control'],
                                        stdout=handle, stderr=subprocess.PIPE, timeout=180)
                handle.flush(); os.fsync(handle.fileno())
    if result.returncode or partial.stat().st_size == 0:
        raise RuntimeError('control database dump failed; partial diagnostic remains private')
    listing = subprocess.run(['pg_restore', '--list', str(partial)], capture_output=True, timeout=30)
    if listing.returncode or not listing.stdout.strip():
        raise ValueError('logical backup archive is unreadable')
    with partial.open('rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    durable_replace(partial, archive)
    state_file = archive.with_suffix('.state.json')
    private_json(state_file,state)
    proof = {'schema_version': 1, 'state': 'backup_written', 'database': 'stockagent_control',
             'runtime': runtime_identity(), 'conda_managed_prefix': (Path(sys.prefix)/'conda-meta').is_dir(),
             'archive': str(archive), 'bytes': archive.stat().st_size, 'sha256': digest,
             'logical_state_file':str(state_file),
             'logical_state_file_sha256':hashlib.sha256(state_file.read_bytes()).hexdigest(),
             'logical_state_identity_sha256':state['identity_sha256'],
             'same_mvcc_snapshot_as_dump':True,
             'archive_directory_readable': True, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
             'wall_seconds': time.perf_counter()-started,
             'restored_by_this_backup_run': False, 'off_host_recovery_verified': False,
             'scope': 'private complete logical control DB backup; source/model data and DB role credentials excluded'}
    private_json(archive.with_suffix('.receipt.json'), proof)
    private_json(output/'latest.json', proof)
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-root', type=Path, default=Path('/var/lib/stockagent/control-plane/backups'))
    args = parser.parse_args()
    result = backup(args.output_root)
    import json
    print(json.dumps({k: result[k] for k in ('state', 'bytes', 'sha256', 'wall_seconds')}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(type(error).__name__, file=sys.stderr)
        raise SystemExit(1)
