#!/usr/bin/env python3
"""Resume E packaging of retained C generations that differ from current D."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import prepare_handoff_index_archive as cli, run_handoff_source_repack_queue as source_queue
from stockagent.data_sync import workspace_volume
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json

CONTRACT = 'stockagent.handoff_index_archive_queue.v1'
OWNER_BUSY_RETRY_SECONDS = 5
EXTERNAL_LINK_RETRY_SECONDS = 300
PROGRESS_STAGES = frozenset({'checking_source_namespace', 'indexing_source_inventory',
    'streaming_source_into_E_packs', 'rechecking_source_namespace', 'writing_E_inventory',
    'comparing_existing_D_inventory',
    'retirement_source_namespace', 'retirement_planning', 'retirement_plan_recheck',
    'retiring_C_payload'})


def progress_token(root: Path, dataset: str):
    """Recognize actual work in this dataset's bounded owner status only."""
    try:
        value = source_queue._metadata(root/(dataset+'.status.json'))
        progress = value.get('progress')
        if (value.get('contract') != cli.CONTRACT or value.get('dataset') != dataset
                or value.get('state') != 'running' or not isinstance(progress, dict)
                or re.fullmatch(r'[0-9a-f]{64}', str(value.get('source_identity', ''))) is None
                or progress.get('stage') not in PROGRESS_STAGES):
            return None
        counters = tuple((name, progress[name]) for name in
                         ('files_completed', 'logical_bytes_processed', 'inventory_items_completed')
                         if type(progress.get(name)) is int and progress[name] >= 0)
        if not counters:
            return None
        return value.get('source_identity'), progress['stage'], counters
    except (OSError, ValueError, SnapshotError):
        return None


def progress_tracker(root: Path, dataset: str):
    """Ignore rewritten clocks, regressions and another source identity."""
    previous = None
    sequence = 0

    def observe():
        nonlocal previous, sequence
        current = progress_token(root, dataset)
        if current is None:
            return None
        if previous is None:
            previous = current
            return sequence
        if current[0] != previous[0]:
            return sequence
        if current[1] != previous[1]:
            previous, sequence = current, sequence+1
            return sequence
        before, after = dict(previous[2]), dict(current[2])
        if (before.keys() == after.keys() and all(after[key] >= before[key] for key in before)
                and any(after[key] > before[key] for key in before)):
            previous, sequence = current, sequence+1
        return sequence
    return observe


def _done(value, dataset):
    result = value.get('C_retirement')
    return (value.get('contract') == cli.CONTRACT and value.get('dataset') == dataset
            and value.get('state') == 'C_payload_retired_root_and_locks_preserved'
            and value.get('C_payload_deleted') is True and isinstance(result, dict)
            and result.get('state') == 'C_payload_retired_root_and_lock_preserved'
            and result.get('prepared_sha256') == value.get('prepared_sha256'))


def handoff_active(source: str) -> bool:
    """Only begin source packaging after its original path handoff is active."""
    try:
        with cli.h.selected_source(source):
            journal = cli.h.read_journal()
            return isinstance(journal, dict) and journal.get('state') == 'active'
    except (OSError, ValueError, TypeError, KeyError, SnapshotError):
        return False


def lock_owner_pid(path: Path) -> int | None:
    """Resolve the actual exclusive owner of this private queue lock."""
    from stockagent.data_sync.storage_progress import _staging_owner_pid
    try:
        info = path.lstat()
        if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1):
            return None
        with Path('/proc/locks').open('rb') as stream:
            data = stream.read(4*1024**2 + 1)
        if len(data) > 4*1024**2:
            return None
        return _staging_owner_pid(data.decode('ascii'),
                                  (os.major(info.st_dev), os.minor(info.st_dev)), info.st_ino)
    except (OSError, UnicodeError, ValueError):
        return None


def reserve_dataset(root: Path, dataset: str):
    """Reserve one dataset across normal and opportunistic archive owners."""
    from stockagent.data_sync.packed_snapshots import validate_slug
    dataset = validate_slug(dataset, 'dataset')
    fd = os.open(root/('reservation-'+dataset+'.lock'),
                 os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
            raise SnapshotError('index archive dataset reservation identity differs')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return None
        return fd
    except BaseException:
        os.close(fd)
        raise


def run_once(root: Path, *, now=time.time, runner=None, code_sha=None,
             ready=handoff_active):
    root = Path(root).absolute()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.resolve(strict=True) != root:
        raise SnapshotError('index archive queue root redirected')
    descriptor = os.open(root/'queue-owner.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    reservation = None
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotError('index archive queue already has an owner') from error
        status_path = root/'queue-status.json'
        previous = source_queue._metadata(status_path)
        if code_sha is None:
            paths = (Path(__file__), ROOT/'scripts/prepare_handoff_index_archive.py',
                     ROOT/'stockagent/data_sync/handoff_index_archive.py',
                     ROOT/'stockagent/data_sync/handoff_source_repack.py',
                     ROOT/'stockagent/data_sync/minute_raw_existing_d.py')
            code_sha = hashlib.sha256(b''.join(path.read_bytes() for path in paths)).hexdigest()
        old_rows = (previous.get('entries', []) if previous.get('contract') == CONTRACT
                    and previous.get('implementation_sha256') == code_sha else [])
        prior = {item.get('dataset'): item for item in old_rows if isinstance(item, dict)}
        entries = []
        for source, (dataset, _locks, _supplemental) in cli.ENROLLED.items():
            value = source_queue._metadata(root/(dataset+'.status.json'))
            retry_at = prior.get(dataset, {}).get('retry_after_epoch', 0)
            if type(retry_at) not in (int, float) or retry_at < 0 or retry_at > now()+86400:
                retry_at = 0
            completed = _done(value, dataset)
            waiting_input = not completed and not ready(source)
            state = ('completed' if completed else 'waiting_input' if waiting_input else
                     'retrying' if retry_at > now() else 'waiting')
            entry = {'source': source, 'dataset': dataset, 'state': state,
                'retry_after_epoch': retry_at if state == 'retrying' else None}
            prior_error = prior.get(dataset, {}).get('error')
            if entry['state'] == 'retrying' and prior_error in {'trading_window', 'worker_failed',
                        'completion_receipt_missing', 'OSError', 'TimeoutExpired', 'owner_busy',
                        'external_hardlinks'}:
                entry['error'] = prior_error
            entries.append(entry)

        def save():
            atomic_write_json(status_path, {'contract': CONTRACT, 'implementation_sha256': code_sha,
                'observed_at_epoch': now(), 'owner_pid': os.getpid(), 'entries': entries}, durable=True)

        selected = None
        for candidate in entries:
            if candidate['state'] != 'waiting':
                continue
            reservation = reserve_dataset(root, candidate['dataset'])
            if reservation is None:
                candidate.update(state='waiting_prepack', error='dataset_owner_busy')
                continue
            selected = candidate
            break
        if selected is None:
            save()
            return entries
        selected.update(state='running', started_at_epoch=now(), retry_after_epoch=None)
        save()
        command = [sys.executable, str(ROOT/'scripts/prepare_handoff_index_archive.py'),
                   '--source', selected['source'], '--retire']
        try:
            if runner is None:
                result = source_queue.run_observed(command, on_heartbeat=save,
                    timeout=source_queue.CHILD_TIMEOUT_SECONDS,
                    progress_probe=progress_tracker(root, selected['dataset']))
            else:
                result = runner(command, on_heartbeat=save, timeout=source_queue.CHILD_TIMEOUT_SECONDS)
            if result.returncode != 0 and 'source handoff is not active' in result.stderr:
                selected.update(state='waiting_input', retry_after_epoch=None, error='handoff_not_active')
            elif result.returncode != 0:
                owner_busy = 'BlockingIOError: [Errno 11] Resource temporarily unavailable' in result.stderr
                external_links = 'retained source hardlinks extend outside the retirement scope' in result.stderr
                selected.update(state='retrying',
                    retry_after_epoch=now()+(OWNER_BUSY_RETRY_SECONDS if owner_busy else
                        EXTERNAL_LINK_RETRY_SECONDS if external_links else source_queue.RETRY_SECONDS),
                    error='owner_busy' if owner_busy else 'external_hardlinks' if external_links else
                          'trading_window' if 'protected day-trade window' in result.stderr
                          or 'tw_day_trade_priority_window' in result.stderr else 'worker_failed',
                    exit_code=result.returncode)
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
        if reservation is not None:
            os.close(reservation)
        os.close(descriptor)


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
