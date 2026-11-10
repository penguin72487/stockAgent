#!/usr/bin/env python3
"""Resume private E-to-D publication of fully prepared handoff datasets."""
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
from scripts import run_handoff_source_repack_queue as source_queue
from stockagent.data_sync import staged_head_capacity_relief, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_private_D_queue.v1'
# Small releases establish useful progress quickly while the larger E copy is
# still being prepared; this order does not alter any provider writer.
DATASETS = ('keyed-public-catalogs', 'crypto-historical-public', 'crypto-etf-history',
            'dune-crypto', 'crypto-reference', 'coinmetrics-community')


def _done(value, dataset):
    receipt = value.get('D_publication')
    return (source_queue._complete(value, dataset) and value.get('D_new_head_committed') is True
            and value.get('D_E_stage_cleanup_complete') is True and isinstance(receipt, dict)
            and receipt.get('contract') == 'stockagent.source_local_exact_repack.v1'
            and receipt.get('dataset') == dataset
            and receipt.get('old_manifest_sha256') == value.get('original_manifest_sha256')
            and receipt.get('state') == 'new_D_head_committed_from_exact_E_source')


def run_once(root: Path, *, now=time.time, runner=source_queue.run_observed, code_sha=None):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('private D queue root redirected')
    descriptor = os.open(root/'D-queue-owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('private D queue already has an owner') from error
        status_path = root/'D-queue-status.json'
        previous = source_queue._metadata(status_path)
        if code_sha is None:
            paths = (Path(__file__), ROOT/'scripts/publish_handoff_source_repack.py',
                     ROOT/'stockagent/data_sync/stream_repack.py', ROOT/'stockagent/data_sync/packed_snapshots.py')
            code_sha = hashlib.sha256(b''.join(path.read_bytes() for path in paths)).hexdigest()
        old_rows = (previous.get('entries', []) if previous.get('contract') == CONTRACT
                    and previous.get('implementation_sha256') == code_sha else [])
        prior = {item.get('dataset'): item for item in old_rows if isinstance(item, dict)}
        entries = []
        for dataset in DATASETS:
            value = source_queue._metadata(root/(dataset+'.status.json'))
            retry_at = prior.get(dataset, {}).get('retry_after_epoch', 0)
            if type(retry_at) not in (int, float) or retry_at < 0 or retry_at > now()+86400:
                retry_at = 0
            state = ('completed' if _done(value, dataset) else
                     'waiting_input' if not source_queue._complete(value, dataset) else
                     'retrying' if retry_at > now() else 'waiting')
            entry = {'dataset': dataset, 'state': state,
                     'retry_after_epoch': retry_at if state == 'retrying' else None}
            prior_error = prior.get(dataset, {}).get('error')
            if state == 'retrying' and prior_error in {'capacity', 'worker_failed', 'completion_receipt_missing',
                                                       'OSError', 'TimeoutExpired'}:
                entry['error'] = prior_error
            entries.append(entry)

        def save():
            atomic_write_json(status_path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
                'observed_at_epoch': now(), 'owner_pid': os.getpid(), 'entries': entries}, durable=True)

        selected = next((row for row in entries if row['state'] == 'waiting'), None)
        if selected is not None and staged_head_capacity_relief.E_only_pending(root.parent.parent):
            for item in entries:
                if item['state'] == 'waiting':
                    item.update(state='waiting_priority', error='E_only_priority')
            save()
            return entries
        if selected is None:
            save()
            return entries
        selected.update(state='running', started_at_epoch=now(), retry_after_epoch=None)
        save()
        command = [sys.executable, str(ROOT/'scripts/publish_handoff_source_repack.py'),
                   '--dataset', selected['dataset']]
        try:
            result = runner(command, on_heartbeat=save, timeout=source_queue.CHILD_TIMEOUT_SECONDS)
            if result.returncode != 0:
                selected.update(state='retrying', retry_after_epoch=now()+source_queue.RETRY_SECONDS,
                                error='capacity' if 'D free space' in result.stderr or 'D reserve reached' in result.stderr
                                      else 'worker_failed', exit_code=result.returncode)
                print(result.stderr[-4096:], file=sys.stderr)
            elif _done(source_queue._metadata(root/(selected['dataset']+'.status.json')), selected['dataset']):
                selected.update(state='completed')
            else:
                selected.update(state='retrying', retry_after_epoch=now()+source_queue.RETRY_SECONDS,
                                error='completion_receipt_missing')
        except (OSError, subprocess.TimeoutExpired) as error:
            selected.update(state='retrying', retry_after_epoch=now()+source_queue.RETRY_SECONDS,
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
    print(json.dumps({'contract': CONTRACT, 'entries': run_once(root)}, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
