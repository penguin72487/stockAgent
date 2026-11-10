#!/usr/bin/env python3
"""Safely refresh the fenced TEJ API subtree of the in-progress E copy.

This is a narrow, resumable recovery of the existing provider owner and copy.
It does not make provider calls, publish an alias, or authorize training.
"""
from __future__ import annotations

from contextlib import ExitStack, closing
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json
from downloader.dataset_lock import exclusive_dataset_lock
from scripts import relocate_provider_workspace as owner
from stockagent.data_sync import provider_workspace as placement
from stockagent.data_sync import workspace_relocation as relocation
from stockagent.data_sync import workspace_relocation_reconcile as reconcile
from stockagent.data_sync import workspace_volume as volume
from stockagent.data_sync.completed_forward_capacity import reserve_capacity
from stockagent.data_sync.materialized_cache import process_references_many

REPORT = (placement.ROOT / 'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1'
          / 'provider-e-tej-api-reconciliation-20261103-v1.json')


def queue_stats(path: Path) -> dict:
    wal = path.with_name(path.name + '-wal')
    if wal.exists() and wal.stat().st_size:
        raise RuntimeError('TEJ queue still has an uncheckpointed WAL')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        if db.execute('PRAGMA quick_check').fetchone() != ('ok',):
            raise RuntimeError('TEJ queue integrity check failed')
        tasks = db.execute('SELECT count(*) FROM tasks').fetchone()[0]
        requests, last_request = db.execute('SELECT count(*), max(started) FROM requests').fetchone()
    return {'tasks': tasks, 'requests': requests, 'latest_request_epoch': last_request}


def main():
    if os.geteuid() != 0:
        raise RuntimeError('TEJ native ownership requires root')
    if (not owner.FENCE.is_file() or owner.FENCE.read_text().strip() != placement.GENERATION
            or json.loads((owner.STATE/'pause.json').read_bytes()).get('state') != 'paused'):
        raise RuntimeError('original provider owner is not fenced and paused')
    with ExitStack() as locks:
        own = locks.enter_context((owner.STATE/'tej-api-reconciliation.lock').open('a'))
        fcntl.flock(own, fcntl.LOCK_EX | fcntl.LOCK_NB)
        entry = placement.load()['providers']['tej']
        if entry.get('state') not in ('copying', 'reconciling'):
            raise RuntimeError('TEJ is not in the expected uncommitted state')
        source, target, evidence = (Path(entry[k]) for k in ('alias', 'destination', 'evidence'))
        if (evidence/'copy-result.json').exists():
            raise RuntimeError('full-copy receipt already exists; use normal placement owner')
        paths = owner.lock_paths('tej')
        for path in paths:
            if not path.is_file() or path.is_symlink():
                raise RuntimeError('a native TEJ lock is missing or redirected')
            locks.enter_context(exclusive_dataset_lock(path, provider='tej_e_reconciliation', timeout_seconds=0))
        if process_references_many([source, target, evidence], limit=100):
            raise RuntimeError('the original TEJ copy or another owner still references the dataset')
        journal = evidence.parent/'tej-api-reconciliation-20261103-v1'
        main_index = evidence/'files.sqlite'
        with closing(sqlite3.connect(main_index.as_uri()+'?mode=ro', uri=True)) as db:
            entries, files, scope_bytes = reconcile._subtree_counts(db, 'api_trial_v1')
        budget = 3*main_index.stat().st_size + 2*scope_bytes + 16384*entries + 128*1024**2
        controls = evidence.parent/'capacity-leases'
        lease = locks.enter_context(reserve_capacity(controls, key=hashlib.sha256(b'tej-api-reconcile-v1').hexdigest(),
            maximum_growth_bytes=budget, capacity_bytes=2**63-1,
            minimum_free_bytes=volume.configuration()['minimum_free_bytes']))
        capacity = volume.admit(budget + lease['other_reserved_growth_bytes'])
        before = {'source': queue_stats(source/'api_trial_v1/queue.sqlite3'),
                  'copied': queue_stats(target/'api_trial_v1/queue.sqlite3')}
        entry.update(state='reconciling', reconciliation_journal=str(journal/'reconciliation.json'))
        owner.save_entry('tej', entry)
        print(json.dumps({'phase': 'reconciling', 'before': before, 'scope_files': files}, ensure_ascii=False), flush=True)
        result = reconcile.reconcile_subtree(source, target, evidence, 'api_trial_v1', journal,
                                              workers=12, sync_batch_files=512)
        after = {'source': queue_stats(source/'api_trial_v1/queue.sqlite3'),
                 'copied': queue_stats(target/'api_trial_v1/queue.sqlite3'),
                 'retained_previous_copy': queue_stats(journal/'staged-payload/queue.sqlite3')}
        if after['source'] != after['copied'] or before['source'] != after['source']:
            raise RuntimeError('TEJ queue changed during reconciliation or readback differs')
        current = placement.load()['providers']['tej']
        if current.get('state') != 'reconciling' or current.get('reconciliation_journal') != str(journal/'reconciliation.json'):
            raise RuntimeError('TEJ provider placement identity changed')
        current.update(state='copying', reconciliation_sha256=relocation.digest(journal/'reconciliation.json'))
        owner.save_entry('tej', current)
        report = {'state': 'subtree_reconciled_full_provider_verification_pending',
                  'before': before, 'after': after, 'source_scope_entries': entries, 'source_scope_files': files,
                  'request_count_delta': before['source']['requests'] - before['copied']['requests'],
                  'task_count_delta': before['source']['tasks'] - before['copied']['tasks'],
                  'reconciliation_journal': str(journal/'reconciliation.json'),
                  'full_copy_proof_pending': result['full_relocation_still_requires_verification'],
                  'source_files_deleted': 0, 'provider_calls_by_this_reconciliation': 0,
                  'capacity_admission': capacity, 'observed_at_epoch': time.time()}
        atomic_write_json(REPORT, report)
        print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
