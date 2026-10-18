"""Bounded parallel transport/CPU stages inside the canonical Complement owner.

Threads own independent HTTP sessions. CPU processes only read verified source
receipts and return Arrow tables. The main thread alone owns queue mutations,
durable publication, failures and progress. Every API grant uses shared limits.
"""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, wait
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime
from decimal import getcontext, setcontext
import hashlib
import json
from pathlib import Path
import time

import pyarrow as pa
import requests

DATASET = 'TaiwanStockTradingDailyReport'
CONFIG = Path(__file__).resolve().parents[1] / 'configs/finmind_object_pipeline.json'


def settings(path=CONFIG):
    value = json.loads(path.read_bytes())
    if value.get('schema_version') != 1 or type(value.get('enabled')) is not bool:
        raise ValueError('invalid_object_pipeline_configuration')
    for key, maximum in (('download_workers', 8), ('derivation_workers', 8),
                         ('arrow_threads', 16), ('max_batch_requests', 32)):
        if type(value.get(key)) is not int or not 1 <= value[key] <= maximum:
            raise ValueError('invalid_object_pipeline_configuration')
    return {**{key: value[key] for key in ('enabled', 'download_workers', 'derivation_workers', 'arrow_threads', 'max_batch_requests')},
            'configuration_sha256': hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()}


def initialize_cpu(threads):
    pa.set_cpu_count(threads)
    pa.set_io_thread_count(threads)


def recover_claims(connection):
    """Restore only interrupted claims; never reopen an accepted/terminal key."""
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_prefetch_claims ('
                       'dataset TEXT,data_id TEXT,partition TEXT,prior_state TEXT,'
                       'PRIMARY KEY(dataset,data_id,partition))')
    connection.execute('UPDATE tasks SET state=(SELECT prior_state FROM finmind_object_prefetch_claims c '
                       'WHERE c.dataset=tasks.dataset AND c.data_id=tasks.data_id AND c.partition=tasks.partition) '
                       "WHERE state='inflight' AND (dataset,data_id,partition) IN "
                       '(SELECT dataset,data_id,partition FROM finmind_object_prefetch_claims)')
    connection.execute('DELETE FROM finmind_object_prefetch_claims')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_pipeline_samples ('
                       'id INTEGER PRIMARY KEY,dataset TEXT,requests INTEGER,seconds REAL,rows INTEGER,'
                       'configuration_sha256 TEXT,observed_at_utc TEXT)')


def _claim(connection, task, *, advance_cursor=False):
    key = (task.dataset, task.data_id, task.partition)
    connection.execute('INSERT INTO finmind_object_prefetch_claims VALUES (?,?,?,?)', (*key, task.state))
    changed = connection.execute("UPDATE tasks SET state='inflight' WHERE dataset=? AND data_id=? AND partition=? AND state=?",
                                 (*key, task.state)).rowcount
    if changed != 1:
        connection.rollback()
        raise RuntimeError('object_pipeline_claim_conflict')
    if advance_cursor:
        # The selector's fairness cursor and the admitted key have the same
        # durability boundary. No separate commit before this durable claim.
        connection.execute('CREATE TABLE IF NOT EXISTS dispatch_cursor '
                           '(priority INTEGER PRIMARY KEY,last_dataset TEXT NOT NULL)')
        connection.execute('INSERT OR REPLACE INTO dispatch_cursor VALUES (?,?)',
                           (task.priority, task.dataset))
    connection.commit()  # Crash recovery precedes HTTP/CPU work.


def _release(connection, task):
    key = (task.dataset, task.data_id, task.partition)
    connection.execute('UPDATE tasks SET state=(SELECT prior_state FROM finmind_object_prefetch_claims c '
                       'WHERE c.dataset=tasks.dataset AND c.data_id=tasks.data_id AND c.partition=tasks.partition) '
                       "WHERE dataset=? AND data_id=? AND partition=? AND state='inflight'", key)
    connection.execute('DELETE FROM finmind_object_prefetch_claims WHERE dataset=? AND data_id=? AND partition=?', key)
    connection.commit()


def _fetch(limiter, root, task, token):
    from downloader.finmind_storage_objects import fetch
    with requests.Session() as session:
        return fetch(session, limiter, root, task, token)


def _derive(root, fields, decimal_context):
    from downloader.download_finmind_complement import Task, _derive_broker_aggregate
    # -m launches define Task under __main__; never pickle that class across
    # spawn. Reconstruct its stable fields and preserve the caller's Decimal ABI.
    setcontext(decimal_context.copy())
    return _derive_broker_aggregate(root, Task(*fields))


def run_batch(connection, downloads, processors, root, current, account, budget, reservation, now,
              limiter, token, *, config, max_calls, delegated=frozenset(), datasets=None,
              required_keys=None, heartbeat):
    from downloader import download_finmind_complement as worker
    from downloader.finmind_storage_objects import active_datasets
    from downloader.finmind_history_order import first_unfinished_dataset
    from downloader.finmind_scheduling import incremental_reservation, protected_stock_opening
    from downloader.finmind_corrections import correction_context
    import shutil

    parent = DATASET if current.dataset == DATASET + 'SecIdAgg' else current.dataset
    if (not config['enabled'] or current.data_id or current.priority == 0
            or parent not in active_datasets(connection)
            or account.get('tier') != 'SponsorPro' or not account.get('observed_at_utc')
            or not budget.get('allowed') or budget.get('remaining', 0) < 1
            or reservation.get('ready_requests', 0) or datasets is not None or required_keys is not None
            or first_unfinished_dataset(connection, now, delegated=delegated) != parent):
        return None
    limit = min(config['max_batch_requests'], max_calls, budget['remaining'])
    if limit <= 0:
        return None
    started = time.monotonic()
    source_jobs, cpu_jobs = {}, {}
    attempts = accepted = retained_rows = failures = 0
    last = None
    pause = None
    initial = current
    next_heartbeat = started + 10
    while True:
        # Actual release/opening/disk clocks stop admission; started work
        # drains and publishes under the same owner, with bounded backpressure.
        stamp = worker._now()
        if protected_stock_opening(stamp):
            pause = 'protected_opening'
        if shutil.disk_usage(root).free < worker.MIN_FREE_BYTES:
            pause = 'disk_guard'
        if not pause and incremental_reservation(root.parent, stamp).get('ready_requests', 0):
            pause = 'incremental_reserve'
        while not pause:
            candidate = initial or worker._next_task(connection, stamp, delegated=delegated, advance_cursor=True)
            initial = None
            if (candidate is None or candidate.data_id or candidate.priority == 0
                    or candidate.dataset not in {parent, DATASET + 'SecIdAgg'}):
                break
            derived = candidate.kind == 'derived' and candidate.dataset == DATASET + 'SecIdAgg'
            if derived and (parent != DATASET or len(cpu_jobs) >= config['derivation_workers'] * 2):
                break
            if not derived and (attempts >= limit or len(source_jobs) >= config['download_workers']
                                or len(cpu_jobs) >= config['derivation_workers'] * 2):
                break
            _claim(connection, candidate)
            try:
                if derived:
                    fields = tuple(getattr(candidate, key) for key in ('dataset', 'data_id', 'partition', 'kind', 'priority', 'state'))
                    cpu_jobs[processors.submit(_derive, root, fields, getcontext().copy())] = candidate
                else:
                    source_jobs[downloads.submit(_fetch, limiter, root, candidate, token)] = candidate
                    attempts += 1  # One quota grant, not the signed file GET.
            except BaseException:
                _release(connection, candidate)
                raise
        if not source_jobs and not cpu_jobs:
            break
        done, _ = wait((*cpu_jobs, *source_jobs), timeout=10, return_when=FIRST_COMPLETED)
        for future in done:
            derived = future in cpu_jobs
            task = (cpu_jobs if derived else source_jobs).pop(future)
            try:
                rows, metadata = future.result()
                metadata['pipeline_configuration_sha256'] = config['configuration_sha256']
                context = correction_context(connection, task)
                receipt = worker._store(root, task, rows, worker._now(), request_metadata=metadata,
                                        **({'correction': context} if context else {}))
                worker._save_result(connection, task, receipt, worker._now())
                connection.commit()
                if not derived:
                    accepted += 1
                    retained_rows += receipt['rows']
                last = {'dataset': task.dataset, 'data_id': '', 'partition': task.partition,
                        'status': receipt['status'], 'rows': receipt['rows'],
                        'observed_at_utc': worker._iso(worker._now())}
            except (worker.SourceError, OSError, ValueError, pa.ArrowException, BrokenProcessPool) as exception:
                error = (exception if isinstance(exception, worker.SourceError)
                         else worker.SourceError('local_cpu_worker_interrupted', retry_after=30)
                         if isinstance(exception, BrokenProcessPool)
                         else worker.SourceError('storage_or_schema_error', retry_after=900))
                worker._save_failure(connection, task, error, worker._now())
                failures += 1
                last = {'dataset': task.dataset, 'data_id': '', 'partition': task.partition,
                        'status': 'failed', 'error_code': error.code,
                        'observed_at_utc': worker._iso(worker._now())}
                if error.code in {'rate_limited', 'invalid_token', 'ip_banned', 'object_disk_guard'}:
                    pause = 'disk_guard' if error.code == 'object_disk_guard' else error.code
                if error.code == 'local_cpu_worker_interrupted':
                    pause = error.code
            finally:
                _release(connection, task)
        if time.monotonic() >= next_heartbeat:
            heartbeat(next(iter(cpu_jobs.values()), next(iter(source_jobs.values()), None)), last)
            next_heartbeat = time.monotonic() + 10
    elapsed = time.monotonic() - started
    # Failed/interrupted mandatory CPU tails are not accepted capacity samples.
    tail = connection.execute("SELECT 1 FROM tasks WHERE dataset=? AND state IN ('pending','failed','inflight') LIMIT 1",
                              (DATASET + 'SecIdAgg',)).fetchone() if parent == DATASET else None
    if accepted and accepted == attempts and not failures and not tail:
        connection.execute('INSERT INTO finmind_pipeline_samples '
                           '(dataset,requests,seconds,rows,configuration_sha256,observed_at_utc) VALUES (?,?,?,?,?,?)',
                           (parent, accepted, elapsed, retained_rows, config['configuration_sha256'], datetime.now(UTC).isoformat()))
        connection.commit()
    return {'calls': attempts, 'last': last, 'state': pause, 'seconds': elapsed,
            'accepted_requests': accepted, 'failed_tasks': failures}


def measured_rates(connection, dataset):
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_pipeline_samples'").fetchone():
        return None
    config = settings()
    rows = connection.execute('SELECT requests,seconds FROM finmind_pipeline_samples WHERE dataset=? '
                              'AND configuration_sha256=? AND requests>0 AND seconds>0 ORDER BY id DESC LIMIT 32',
                              (dataset, config['configuration_sha256'])).fetchall()
    if len(rows) < 2:
        return None
    rates = sorted(calls * 3600 / seconds for calls, seconds in rows)
    # One fast small wave must not outweigh a slow large wave. This is actual
    # accepted work / total full-pipeline wall, not the upper median of a tiny
    # sample (which jumped to the fast half after only four observations).
    sampled_requests = sum(calls for calls, _ in rows)
    sampled_seconds = sum(seconds for _, seconds in rows)
    central = sampled_requests * 3600 / sampled_seconds
    return {'samples': len(rows), 'capacity_basis': 'accepted_parallel_pipeline_wall',
            'rates': {'fastest': rates[-1], 'central': central,
                      'slowest': min(central, rates[min(len(rates)-1, int((len(rates)-1)*.1))])},
            'central_rate_basis': 'sum_accepted_requests_over_sum_batch_wall',
            'sampled_requests': sampled_requests, 'sampled_wall_seconds': sampled_seconds,
            'download_workers': config['download_workers'], 'derivation_workers': config['derivation_workers'],
            'arrow_threads': config['arrow_threads']}
