#!/usr/bin/env python3
"""Opportunistically seal a second C generation while the archive owner is busy.

This owner only prepares E packages. The original archive queue retains sole
responsibility for C retirement, D publication and subsequent cleanup.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_index_archive as cli
from scripts import run_handoff_index_archive_queue as main_queue
from scripts import run_handoff_source_repack_queue as source_queue
from stockagent.data_sync import stream_repack, workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_index_prepack_queue.v1'


def main_owner(root: Path, *, now: float):
    """Require a fresh running row bound to the actual main queue flock."""
    try:
        value = source_queue._metadata(root/'queue-status.json')
        stamp, pid = value.get('observed_at_epoch'), value.get('owner_pid')
        if (value.get('contract') != main_queue.CONTRACT or type(stamp) not in (int, float)
                or not -60 <= now-stamp <= 180 or type(pid) is not int or pid <= 0
                or main_queue.lock_owner_pid(root/'queue-owner.lock') != pid):
            return None
        entries = value.get('entries')
        active = [row for row in entries if isinstance(row, dict) and row.get('state') == 'running'] \
            if isinstance(entries, list) and len(entries) <= len(cli.ENROLLED) else []
        if len(active) != 1 or cli.ENROLLED.get(active[0].get('source'), (None,))[0] != active[0].get('dataset'):
            return None
        return pid, active[0]['source'], active[0]['dataset']
    except (OSError, ValueError, TypeError, SnapshotError):
        return None


def prepared(root: Path, dataset: str, value: dict) -> bool:
    """Accept only a still-present E generation matching the source receipt."""
    if (value.get('contract') != cli.CONTRACT or value.get('dataset') != dataset
            or value.get('state') not in {'E_index_source_hashed_prepared', 'C_retirement_waiting'}):
        return False
    identity, digest = value.get('source_identity'), value.get('prepared_sha256')
    if not isinstance(identity, str) or re.fullmatch(r'[0-9a-f]{64}', identity) is None:
        return False
    try:
        stage = root/dataset/identity
        fixed = stream_repack._read_prepared(stage, dataset, identity)
        return (fixed.get('identity_sha256') == digest
                and fixed.get('source_mode') == 'retained_handoff_C_index')
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return False


def run_once(root: Path, *, now=time.time, ready=main_queue.handoff_active, runner=None,
             parent=main_owner, is_prepared=prepared):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('index prepack queue root redirected')
    owner = os.open(root/'prepack-owner.lock',
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    reservation = None
    try:
        try:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('index prepack queue already has an owner') from error
        status = root/'prepack-queue-status.json'
        previous = source_queue._metadata(status)
        parent_binding = parent(root, now=now())
        result = {'contract':CONTRACT, 'state':'waiting', 'owner_pid':os.getpid(),
                  'observed_at_epoch':now(), 'reason':'main_queue_not_running'}
        if parent_binding is None:
            atomic_write_json(status, result, durable=True)
            return result
        selected = None
        for source, (dataset, _locks, _supplemental) in cli.ENROLLED.items():
            value = source_queue._metadata(root/(dataset+'.status.json'))
            retry = (previous.get('retry_after_epoch') if previous.get('contract') == CONTRACT
                     and previous.get('dataset') == dataset else None)
            if (source == cli.h.MINUTE_RAW_NAME or dataset == parent_binding[2] or main_queue._done(value, dataset)
                    or is_prepared(root, dataset, value) or not ready(source)
                    or type(retry) in (int, float) and now() < retry <= now()+86400):
                continue
            reservation = main_queue.reserve_dataset(root, dataset)
            if reservation is None:
                continue
            selected = source, dataset
            break
        if selected is None:
            result['reason'] = 'no_other_ready_source'
            atomic_write_json(status, result, durable=True)
            return result
        if parent(root, now=now()) != parent_binding:
            result['reason'] = 'main_queue_changed'
            atomic_write_json(status, result, durable=True)
            return result
        source, dataset = selected
        result.update(state='running', source=source, dataset=dataset,
                      main_dataset=parent_binding[2], started_at_epoch=now(), reason=None)
        def save():
            result['observed_at_epoch'] = now()
            atomic_write_json(status, result, durable=True)
        save()
        command = [sys.executable, str(ROOT/'scripts/prepare_handoff_index_archive.py'), '--source', source]
        try:
            if runner is None:
                child = source_queue.run_observed(command, on_heartbeat=save,
                    timeout=source_queue.CHILD_TIMEOUT_SECONDS,
                    progress_probe=main_queue.progress_tracker(root, dataset))
            else:
                child = runner(command, on_heartbeat=save, timeout=source_queue.CHILD_TIMEOUT_SECONDS)
            if child.returncode == 0 and is_prepared(root, dataset, source_queue._metadata(root/(dataset+'.status.json'))):
                result.update(state='completed', reason=None)
            else:
                result.update(state='retrying', reason='worker_failed',
                              retry_after_epoch=now()+source_queue.RETRY_SECONDS)
                if child.returncode:
                    print(child.stderr[-4096:], file=sys.stderr)
        except (OSError, subprocess.TimeoutExpired) as error:
            result.update(state='retrying', reason=type(error).__name__,
                          retry_after_epoch=now()+source_queue.RETRY_SECONDS)
        result['finished_at_epoch'] = now()
        save()
        return result
    finally:
        if reservation is not None:
            os.close(reservation)
        os.close(owner)


def main():
    config = workspace_volume.configuration()
    workspace_volume.check(c=config)
    root = workspace_volume.path('migration', create=True)/'handoff-index-archive-v1'
    print(json.dumps(run_once(root), ensure_ascii=False))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, SnapshotError) as error:
        print(f'{type(error).__name__}: {error}', file=sys.stderr)
        raise SystemExit(1)
