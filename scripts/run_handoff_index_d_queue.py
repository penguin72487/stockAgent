#!/usr/bin/env python3
"""Resume exact source-index E archives into the local private D authority."""
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
from scripts import prepare_handoff_index_archive as source_cli
from scripts import prepare_offline_repair_archive as offline_archive, retire_offline_repair_source as offline_retire
from scripts import run_handoff_index_archive_queue as index_queue, run_handoff_source_repack_queue as source_queue
from stockagent.data_sync import index_archive_existing, staged_head_capacity_relief, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_index_private_D_queue.v1'
# The scheduler already ticks once a minute. A contended owner is a transient
# handoff, not a reason to hold a sealed E batch for the generic 120 seconds.
OWNER_BUSY_RETRY_SECONDS = 15


def _done(value, dataset):
    receipt = value.get('D_publication')
    source_done = (offline_retire.retired_status_matches(value, dataset) if dataset in offline_archive.DATASET_SCOPES
                   else index_queue._done(value, dataset))
    return (source_done and value.get('D_archive_committed') is True
            and value.get('D_E_stage_cleanup_complete') is True and isinstance(receipt, dict)
            and receipt.get('contract') == 'stockagent.source_local_exact_repack.v1'
            and receipt.get('dataset') == dataset and receipt.get('old_manifest_sha256') == value.get('source_identity')
            and (receipt.get('state') == 'new_D_head_committed_from_exact_E_source'
                 or index_archive_existing.receipt_matches(receipt, value.get('prepared_sha256'))))


def run_once(root: Path, *, now=time.time, runner=source_queue.run_observed, code_sha=None):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('source-index private D queue root redirected')
    fd = os.open(root/'D-queue-owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('source-index private D queue already has an owner') from error
        path = root/'D-queue-status.json'
        previous = source_queue._metadata(path)
        if code_sha is None:
            paths = (Path(__file__), ROOT/'scripts/publish_handoff_index_archive.py',
                     ROOT/'stockagent/data_sync/index_archive_publish.py', ROOT/'stockagent/data_sync/index_archive_existing.py',
                     ROOT/'stockagent/data_sync/stream_repack.py')
            code_sha = hashlib.sha256(b''.join(item.read_bytes() for item in paths)).hexdigest()
        old_rows = (previous.get('entries', []) if previous.get('contract') == CONTRACT
                    and previous.get('implementation_sha256') == code_sha else [])
        prior = {row.get('dataset'): row for row in old_rows if isinstance(row, dict)}
        entries = []
        datasets = [dataset for dataset, _locks, _supplemental in source_cli.ENROLLED.values()]
        datasets.extend(dataset for dataset in offline_archive.DATASET_SCOPES
                        if (root/(dataset+'.status.json')).exists())
        for dataset in datasets:
            value = source_queue._metadata(root/(dataset+'.status.json'))
            old = prior.get(dataset, {})
            retry_at = old.get('retry_after_epoch', 0)
            if type(retry_at) not in (int, float) or retry_at < 0 or retry_at > now()+86400:
                retry_at = 0
            source_done = (offline_retire.retired_status_matches(value, dataset) if dataset in offline_archive.DATASET_SCOPES
                           else index_queue._done(value, dataset))
            state = ('completed' if _done(value, dataset) else
                     'waiting_input' if not source_done else
                     'retrying' if retry_at > now() else 'waiting')
            entry = {'dataset': dataset, 'state': state,
                     'retry_after_epoch': retry_at if state == 'retrying' else None}
            if state == 'retrying' and old.get('error') in {'capacity', 'owner_busy', 'worker_failed', 'completion_receipt_missing',
                                                            'OSError', 'TimeoutExpired'}:
                entry['error'] = old['error']
                if old.get('owner_lock') in {'publish_retention_global', 'cold_install'}:
                    entry['owner_lock'] = old['owner_lock']
            entries.append(entry)
        def save():
            atomic_write_json(path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
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
        command = [sys.executable, str(ROOT/'scripts/publish_handoff_index_archive.py'),
                   '--dataset', selected['dataset']]
        try:
            result = runner(command, on_heartbeat=save, timeout=source_queue.CHILD_TIMEOUT_SECONDS)
            if result.returncode != 0:
                error = ('capacity' if 'D free space' in result.stderr or 'D reserve reached' in result.stderr
                         else 'owner_busy' if 'owner busy' in result.stderr.lower() else 'worker_failed')
                delay = OWNER_BUSY_RETRY_SECONDS if error == 'owner_busy' else source_queue.RETRY_SECONDS
                selected.update(state='retrying', retry_after_epoch=now()+delay,
                                error=error, exit_code=result.returncode)
                if error == 'owner_busy' and 'publish-retention-global.lock' in result.stderr:
                    selected['owner_lock'] = 'publish_retention_global'
                elif error == 'owner_busy' and 'cold-install-write-owner.lock' in result.stderr:
                    selected['owner_lock'] = 'cold_install'
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
        os.close(fd)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    print(json.dumps({'contract': CONTRACT, 'entries': run_once(root)}, ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
