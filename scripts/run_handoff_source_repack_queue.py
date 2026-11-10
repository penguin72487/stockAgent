#!/usr/bin/env python3
"""Resume one reviewed private C-to-E handoff generation per timer run."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_source_repack as handoff_cli
from scripts.run_source_local_repack_queue import run_observed
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_source_repack_queue.v1'
RETRY_SECONDS = 120
CHILD_TIMEOUT_SECONDS = 105 * 60


def implementation_sha():
    paths = (Path(__file__), ROOT/'scripts/prepare_handoff_source_repack.py',
             ROOT/'stockagent/data_sync/handoff_source_repack.py')
    return hashlib.sha256(b''.join(path.read_bytes() for path in paths)).hexdigest()


def _metadata(path):
    if path.is_symlink():
        raise SnapshotError('handoff queue metadata redirected')
    if not path.exists():
        return {}
    if not path.is_file() or path.stat().st_size > 1024*1024:
        raise SnapshotError('handoff queue metadata is not a bounded regular file')
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise SnapshotError('handoff queue metadata is not an object')
    return value


def _complete(value, dataset):
    result = value.get('C_retirement')
    return (value.get('contract') == handoff_cli.CONTRACT and value.get('dataset') == dataset
            and value.get('state') == 'C_payload_retired_root_and_lock_preserved'
            and value.get('C_payload_deleted') is True and isinstance(result, dict)
            and result.get('state') == value['state']
            and result.get('prepared_sha256') == value.get('prepared_sha256')
            and isinstance(result.get('source_root_inode'), int)
            and isinstance(result.get('source_lock_inode'), int))


def run_once(root: Path, *, now=time.time, runner=run_observed, code_sha=None):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('handoff queue root redirected')
    owner = root/'queue-owner.lock'
    descriptor = os.open(owner, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('handoff queue already has an owner') from error
        status_path = root/'queue-status.json'
        previous = _metadata(status_path)
        code_sha = code_sha or implementation_sha()
        prior_rows = (previous.get('entries', []) if previous.get('contract') == CONTRACT
                      and previous.get('implementation_sha256') == code_sha else [])
        prior = {item.get('dataset'): item for item in prior_rows if isinstance(item, dict)}
        entries = []
        for name, dataset in handoff_cli.ENROLLED.items():
            source_status = _metadata(root/(dataset+'.status.json'))
            old = prior.get(dataset, {})
            retry_at = old.get('retry_after_epoch', 0)
            if type(retry_at) not in (int, float) or retry_at < 0 or retry_at > now()+86400:
                retry_at = 0
            entry = {'source': name, 'dataset': dataset,
                'state': 'completed' if _complete(source_status, dataset) else
                         ('retrying' if retry_at > now() else 'waiting'),
                'retry_after_epoch': retry_at if retry_at > now() else None}
            prior_error = old.get('error')
            if entry['state'] == 'retrying' and prior_error in {'worker_failed', 'completion_receipt_missing',
                                                                 'OSError', 'TimeoutExpired'}:
                entry['error'] = prior_error
            entries.append(entry)

        def save():
            atomic_write_json(status_path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
                'observed_at_epoch': now(), 'owner_pid': os.getpid(), 'entries': entries}, durable=True)

        selected = next((row for row in entries if row['state'] == 'waiting'), None)
        if selected is None:
            save()
            return entries
        selected.update(state='running', started_at_epoch=now(), retry_after_epoch=None)
        save()
        command = [sys.executable, str(ROOT/'scripts/prepare_handoff_source_repack.py'),
                   '--source', selected['source'], '--retire']
        try:
            result = runner(command, on_heartbeat=save, timeout=CHILD_TIMEOUT_SECONDS)
            if result.returncode != 0:
                selected.update(state='retrying', retry_after_epoch=now()+RETRY_SECONDS,
                                error='worker_failed', exit_code=result.returncode)
                # Worker output goes to the private system journal, never the public receipt.
                print(result.stderr[-4096:], file=sys.stderr)
            elif _complete(_metadata(root/(selected['dataset']+'.status.json')), selected['dataset']):
                selected.update(state='completed')
            else:
                selected.update(state='retrying', retry_after_epoch=now()+RETRY_SECONDS,
                                error='completion_receipt_missing')
        except (OSError, subprocess.TimeoutExpired) as error:
            selected.update(state='retrying', retry_after_epoch=now()+RETRY_SECONDS,
                            error=type(error).__name__)
        selected['finished_at_epoch'] = now()
        save()
        return entries
    finally:
        os.close(descriptor)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration', create=True)/'handoff-source-repack-v1'
    entries = run_once(root)
    print(json.dumps({'contract': CONTRACT, 'entries': entries}, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
