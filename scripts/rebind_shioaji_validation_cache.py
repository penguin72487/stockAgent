#!/usr/bin/env python3
"""Reuse the fixed minute validation cache after a WSL device-number change.

Only cache metadata is updated.  Each later cache hit still checks the current
file signature and the original semantic validator contract.  The fixed E
copy's original inventory binds the prior inode, timestamps, length and SHA;
no source data, publication, or migration receipt is modified here.
"""
from __future__ import annotations

import json
import argparse
import hashlib
import fcntl
import os
from pathlib import Path
import sqlite3
import stat
import time

from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_source_handoff as handoff
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.desync_snapshots import SnapshotError
from downloader.artifact_io import atomic_write_json

ROOT = Path(__file__).resolve().parents[1]
NAME = 'tw-minute-source-e-primary-20261104-v1'
SOURCE = ROOT / 'data_tw_minute/shioaji_1m'
CACHE = ROOT / 'artifacts/operations/source_validation/tw-minute-source-cold/files.sqlite3'
STATUS = CACHE.parent / 'device-rebind-status.json'


def _source_mapping(recorded, current):
    mapping = handoff._source_device_rebind(SOURCE, recorded, current)
    if mapping is not None:
        return mapping
    if (isinstance(recorded, list) and isinstance(current, list)
            and len(recorded) == len(current) == 9 and recorded[0] == current[0]
            and all(type(value) is int and value >= 0 for value in recorded+current)
            and all(recorded[index] == current[index] for index in (1, 2, 6, 7, 8))
            and handoff._canonical_c_source(SOURCE, current[0])):
        return recorded[0], current[0]
    return None


def _fixed_binding():
    """Bind the historical full-copy proof to the same canonical C root."""
    volume.check()
    workspace = Path(volume.configuration()['mount_point'])
    item = next((item for item in queue.load_sources() if item['name'] == NAME), None)
    if not item or queue.paths(item, workspace)[0] != SOURCE:
        raise SnapshotError('minute validation cache has no exact staging enrollment')
    report = queue.verified_report(item, workspace)
    if report is None:
        return None
    if report.get('state') != 'verified_temporary_e_source_copy':
        raise SnapshotError('minute validation cache lacks the fixed full-copy proof')
    _, _, evidence, _ = queue.paths(item, workspace)
    intent = queue._read_json(evidence/'intent.json')
    completion = queue._read_json(evidence/'copy-result.json')
    if (intent.get('state') != 'indexed' or intent.get('source') != str(SOURCE)
            or completion.get('state') != 'exact_destination_readback_verified'
            or completion.get('all_source_sha256_and_destination_readback_verified') is not True):
        raise SnapshotError('minute validation cache donor proof differs')
    recorded = intent.get('source_root_signature')
    current = relocation.signature(SOURCE)
    mapping = _source_mapping(recorded, current)
    if mapping is None:
        return None
    index = evidence/'files.sqlite'
    if not index.exists():
        return None
    info = index.lstat()
    completion_at = completion.get('observed_at_epoch')
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1
            or not isinstance(completion_at, (int, float))
            or info.st_mtime > completion_at + 5 or info.st_ctime > completion_at + 5
            or (evidence/'files.sqlite-wal').exists() and (evidence/'files.sqlite-wal').stat().st_size):
        raise SnapshotError('minute validation donor index changed after its fixed copy')
    return index, recorded, mapping, relocation.signature(index)


def _rebind_rows(cache: Path, source: Path, index: Path, contract: str, old_device: int,
                 new_device: int, guard, *, deadline: float, batch_size: int = 2048) -> dict:
    """Use small conditional SQLite transactions, compatible with the live owner."""
    if (type(batch_size) is not int or not 1 <= batch_size <= 4096
            or type(old_device) is not int or type(new_device) is not int):
        raise ValueError('invalid minute cache rebind bounds')
    prefix = str(source) + '/'
    sql = '''SELECT v.rowid,v.path,v.signature,v.sha256
        FROM verified AS v JOIN old.files AS f ON f.path=substr(v.path,?)
        WHERE v.rowid>? AND substr(v.path,1,?)=? AND v.contract=?
          AND json_valid(v.signature) AND json_extract(v.signature,'$[0]')!=?
          AND f.kind='file' AND f.state='verified' AND f.sha=v.sha256
          AND json_valid(f.sig) AND json_extract(f.sig,'$[0]')=?
          AND json_extract(v.signature,'$[1]')=json_extract(f.sig,'$[1]')
          AND json_extract(v.signature,'$[2]')=json_extract(f.sig,'$[3]')
          AND json_extract(v.signature,'$[3]')=json_extract(f.sig,'$[4]')
          AND json_extract(v.signature,'$[4]')=json_extract(f.sig,'$[5]')
        ORDER BY v.rowid LIMIT ?'''
    connection = sqlite3.connect(cache.as_uri() + '?mode=rw', uri=True, timeout=0.05,
                                 isolation_level=None)
    checked = changed = skipped = after = 0
    saw_busy = False
    try:
        connection.execute('ATTACH DATABASE ? AS old', (index.as_uri()+'?mode=ro',))
        while time.monotonic() < deadline:
            guard()
            try:
                rows = connection.execute(sql, (len(prefix)+1, after, len(prefix), prefix, contract,
                                               new_device, old_device, batch_size)).fetchall()
                if not rows:
                    return {'state': 'completed', 'matched': checked, 'rebound': changed, 'raced': skipped}
                updates = []
                for rowid, path, encoded, sha in rows:
                    sig = json.loads(encoded)
                    if (not isinstance(sig, list) or len(sig) != 5
                            or any(type(value) is not int or value < 0 for value in sig)):
                        raise SnapshotError('minute validation cache signature is malformed')
                    updates.append((json.dumps([new_device, *sig[1:]]), rowid, path, encoded, contract, sha))
                connection.execute('BEGIN IMMEDIATE')
                guard()
                before = connection.total_changes
                connection.executemany('UPDATE verified SET signature=? WHERE rowid=? AND path=? '
                    'AND signature=? AND contract=? AND sha256=?', updates)
                guard()
                updated = connection.total_changes - before
                connection.commit()
                after = rows[-1][0]
                checked += len(rows)
                changed += updated
                skipped += len(rows) - updated
            except sqlite3.OperationalError as error:
                if connection.in_transaction:
                    connection.rollback()
                if 'locked' not in str(error).lower() and 'busy' not in str(error).lower():
                    raise
                # A concurrent validator retains SQLite ownership. Retry the
                # same keyset without forcing or resetting its lock.
                saw_busy = True
                time.sleep(min(0.05, max(0, deadline-time.monotonic())))
        return {'state': 'busy' if saw_busy and not checked else 'time_budget',
                'matched': checked, 'rebound': changed, 'raced': skipped}
    finally:
        connection.close()


def rebind(*, max_seconds=30) -> dict:
    """Rebind only the historically verified C source and its existing cache."""
    if type(max_seconds) not in (int, float) or not 0 < max_seconds <= 60:
        raise ValueError('minute cache rebind time budget is invalid')
    if not CACHE.exists():
        return {'state': 'not_needed', 'rebound': 0}
    if CACHE.is_symlink() or CACHE.resolve(strict=True) != CACHE:
        raise SnapshotError('minute validation cache is redirected')
    lock = CACHE.parent / 'device-rebind.lock'
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or info.st_nlink != 1 or info.st_mode & 0o077):
            raise SnapshotError('minute validation cache rebind lock identity differs')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'state': 'busy', 'rebound': 0}
        return _rebind_locked(max_seconds=max_seconds)
    finally:
        os.close(descriptor)


def _rebind_locked(*, max_seconds):
    binding = _fixed_binding()
    if binding is None:
        return {'state': 'not_needed', 'rebound': 0}
    index, recorded, (old_device, new_device), fixed_index = binding
    if CACHE.is_symlink() or CACHE.resolve(strict=True) != CACHE:
        raise SnapshotError('minute validation cache is redirected')
    info = CACHE.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
        raise SnapshotError('minute validation cache identity differs')
    from stockagent.data_sync import source_validation
    contract = source_validation.validator_identity(Path(source_validation.__file__),
        ROOT/'downloader/download_shioaji_tw_minute_kbars.py')
    next_window_check = 0.0

    def identity_guard():
        if (_source_mapping(recorded, relocation.signature(SOURCE))
                != (old_device, new_device) or relocation.signature(index) != fixed_index
                or CACHE.is_symlink() or (CACHE.stat().st_dev, CACHE.stat().st_ino) != (info.st_dev, info.st_ino)):
            raise SnapshotError('minute cache, source, or fixed donor identity changed')

    def guard():
        nonlocal next_window_check
        if time.monotonic() >= next_window_check:
            if not handoff.binance_handoff_window_allowed(for_start=False):
                raise BlockingIOError('tw_day_trade_priority_window')
            next_window_check = time.monotonic() + 30
        identity_guard()

    identity_guard()
    generation = hashlib.sha256(json.dumps([old_device, new_device, info.st_dev, info.st_ino,
        fixed_index, contract, recorded[1]], separators=(',', ':')).encode()).hexdigest()
    previous = queue._read_json(STATUS) if STATUS.exists() else {}
    if (previous.get('contract') == 'stockagent.minute_validation_device_rebind.v1'
            and previous.get('generation') == generation and previous.get('state') == 'completed'):
        return {'state': 'already_completed', 'rebound': 0}
    guard()
    result = _rebind_rows(CACHE, SOURCE, index, contract, old_device, new_device, guard,
                          deadline=time.monotonic()+max_seconds)
    result = {**result, 'source_files_deleted': 0, 'payloads_read': False,
              'semantic_validator_contract_unchanged': True}
    cumulative = (previous.get('cumulative_rebound', 0)
                  if previous.get('generation') == generation else 0)
    if type(cumulative) is not int or cumulative < 0:
        cumulative = 0
    atomic_write_json(STATUS, {'contract': 'stockagent.minute_validation_device_rebind.v1',
        'generation': generation, 'state': result['state'], 'cumulative_rebound': cumulative+result['rebound'],
        'matched': result['matched'], 'raced': result['raced'], 'observed_at_epoch': time.time(),
        'payloads_read': False, 'source_files_deleted': 0}, durable=True)
    return result


def _record_waiting(reason):
    if not STATUS.exists():
        return
    value = queue._read_json(STATUS)
    if (value.get('contract') != 'stockagent.minute_validation_device_rebind.v1'
            or value.get('state') not in {'time_budget', 'busy', 'waiting'}):
        return
    atomic_write_json(STATUS, {**value, 'state': 'waiting', 'reason': reason,
                              'observed_at_epoch': time.time()}, durable=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--drain', action='store_true', help='resume bounded batches until this generation is done')
    args = parser.parse_args()
    while True:
        try:
            output = rebind()
        except BlockingIOError as error:
            if str(error) != 'tw_day_trade_priority_window':
                raise
            output = {'state': 'waiting', 'reason': 'tw_day_trade_priority_window', 'rebound': 0}
            _record_waiting(output['reason'])
        print(json.dumps(output, ensure_ascii=False), flush=True)
        if not args.drain or output['state'] not in {'busy', 'time_budget'}:
            break
        time.sleep(1)
