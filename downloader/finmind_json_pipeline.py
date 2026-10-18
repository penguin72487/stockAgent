"""Overlap bounded JSON I/O inside the existing Complement queue owner.

Do not turn concurrency into a larger entitlement. Threads use independent,
reused HTTP sessions and the shared limiter/traffic ledger. Only the owner
selects, claims, validates, publishes and updates SQLite. Complex overlapping
history/bulk/derived queries keep their existing canonical handlers.
"""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, wait
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import threading
import time
import uuid

import pyarrow as pa
import requests
from downloader.common import RateLimitWaitCancelled
from downloader.artifact_io import atomic_write_json

CONFIG = Path(__file__).resolve().parents[1] / 'configs/finmind_json_pipeline.json'
CONTRACT_VERSION = 12  # Stable WAL lifetime and real HTTP counting require fresh capacity.
LOCAL_PREFLIGHT_ERRORS = frozenset({'local_traffic_busy', 'local_traffic_unavailable'})


def settings(path=CONFIG, *, max_requests_per_cycle=None):
    value = json.loads(path.read_bytes())
    if value.get('schema_version') != 1 or type(value.get('enabled')) is not bool:
        raise ValueError('invalid_json_pipeline_configuration')
    for key, minimum, maximum in (('download_workers', 1, 32), ('max_batch_requests', 1, 2400),
                                 ('max_requests_per_cycle', 1, 10000),
                                 ('max_buffered_response_bytes', 64 * 1024**2, 1024**3)):
        if type(value.get(key)) is not int or not minimum <= value[key] <= maximum:
            raise ValueError('invalid_json_pipeline_configuration')
    if max_requests_per_cycle is not None:
        if type(max_requests_per_cycle) is not int or max_requests_per_cycle < 0:
            raise ValueError('invalid_json_pipeline_cycle_limit')
        # Finite/operator runs cannot calibrate the production work quantum.
        value['max_requests_per_cycle'] = max_requests_per_cycle
    fingerprint = hashlib.sha256(json.dumps([CONTRACT_VERSION, value], sort_keys=True).encode()).hexdigest()
    return {**value, 'configuration_sha256': fingerprint}


class Sessions:
    """One keep-alive session per executor thread; close after executor drain."""
    def __init__(self, *, traffic_root=None):
        self.local = threading.local()
        self.sessions = []
        self.lock = threading.Lock()
        self.traffic_root, self.traffic_anchor = traffic_root, None

    def get(self):
        if not hasattr(self.local, 'session'):
            self.local.session = requests.Session()
            with self.lock:
                self.sessions.append(self.local.session)
        return self.local.session

    def __enter__(self):
        if self.traffic_root is not None:
            path = (self.traffic_root / 'request_traffic.sqlite3').resolve()
            if path.is_file():
                try:
                    # Keep this existing WAL open while requests run. Repeated
                    # last-close checkpoints can race new writers/readers even
                    # after the per-process write gate. No read transaction,
                    # new ledger, row mutation or relaxed fsync is introduced.
                    self.traffic_anchor = sqlite3.connect(path.as_uri()+'?mode=rw', uri=True, timeout=5)
                    self.traffic_anchor.execute('PRAGMA query_only=ON')
                    self.traffic_anchor.execute('SELECT 1 FROM requests LIMIT 1').fetchone()
                except sqlite3.Error:
                    if self.traffic_anchor is not None:
                        self.traffic_anchor.close()
                        self.traffic_anchor = None
                    # Optional lifetime optimization, never an accounting grant.
                    # The request's canonical durable preflight remains required.
        return self

    def __exit__(self, *args):
        try:
            for session in self.sessions:
                session.close()
        finally:
            if self.traffic_anchor is not None:
                self.traffic_anchor.close()
                self.traffic_anchor = None


def recover_claims(connection):
    # Reuse the same recoverable claims as object prefetch, not another owner.
    from downloader.finmind_object_pipeline import recover_claims as recover
    recover(connection)
    _schema(connection)


def _schema(connection):
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_json_pipeline_samples ('
                       'id INTEGER PRIMARY KEY,dataset TEXT,requests INTEGER,seconds REAL,rows INTEGER,'
                       'nonempty_requests INTEGER,configuration_sha256 TEXT,observed_at_utc TEXT,'
                       'fetch_validate_publish_seconds REAL,cycle_wall_verified INTEGER NOT NULL DEFAULT 0)')
    columns = {row[1] for row in connection.execute('PRAGMA table_info(finmind_json_pipeline_samples)')}
    for name, kind in (('cycle_id', 'TEXT'), ('cycle_wall_seconds', 'REAL'), ('excluded_work_seconds', 'REAL'),
                       ('cycle_started_at_utc', 'TEXT'), ('cycle_finished_at_utc', 'TEXT')):
        if name not in columns:
            connection.execute(f'ALTER TABLE finmind_json_pipeline_samples ADD COLUMN {name} {kind}')


class CycleCosts:
    """Charge real preparation/status/pool-shutdown time, not just socket time.

    Measured preempting work is stored separately: the ETA calendar already
    models those requests. Common planning, status and pool shutdown stay in
    JSON capacity; complete observed wall time remains available for acceptance.
    Failed/interrupted cycles never calibrate capacity.
    """
    def __init__(self, connection, started):
        self.connection, self.started = connection, started
        self.calibration_invalid = False
        self.excluded_seconds = 0.0
        self.cycle_id = uuid.uuid4().hex

    def exclude_elapsed(self, started):
        self.excluded_seconds += time.monotonic() - started

    def __enter__(self):
        _schema(self.connection)
        self.first = self.connection.execute('SELECT COALESCE(MAX(id),0) FROM finmind_json_pipeline_samples').fetchone()[0]
        return self

    def __exit__(self, kind, error, traceback):
        if kind is not None or self.calibration_invalid:
            return
        rows = self.connection.execute('SELECT id,seconds FROM finmind_json_pipeline_samples WHERE id>?', (self.first,)).fetchall()
        total = sum(seconds for _, seconds in rows)
        elapsed = time.monotonic() - self.started
        attributable = max(total, elapsed - self.excluded_seconds)
        finished = datetime.now(UTC)
        from datetime import timedelta
        started = finished - timedelta(seconds=elapsed)
        if total > 0:
            for key, seconds in rows:
                self.connection.execute('UPDATE finmind_json_pipeline_samples SET '
                                        'fetch_validate_publish_seconds=seconds,seconds=?,cycle_wall_verified=1,'
                                        'cycle_id=?,cycle_wall_seconds=?,excluded_work_seconds=?,'
                                        'cycle_started_at_utc=?,cycle_finished_at_utc=? WHERE id=?',
                                        (attributable * seconds / total, self.cycle_id, elapsed,
                                         self.excluded_seconds, started.isoformat(), finished.isoformat(), key))
            self.connection.commit()


def supported(connection, task):
    from downloader import download_finmind_complement as worker
    if task.kind in {'derived', 'snapshot'}:
        return False
    if task.dataset in worker.DAILY_EQUITY or task.dataset in worker.BULK_GLOBAL_HISTORY:
        return False
    if (task.dataset in worker.storage_objects.active_datasets(connection) and not task.data_id):
        return False
    return (task.dataset in worker.supplemental.SOURCES or task.dataset == worker.news.DATASET
            or task.kind in {'id_history', 'year'})


class _CancellableLimiter:
    def __init__(self, limiter, stop):
        self.limiter, self.stop = limiter, stop

    def wait(self):
        self.limiter.wait(cancel_event=self.stop)

    def defer(self, seconds):
        self.stop.set()
        self.limiter.defer(seconds)


def _fetch(sessions, limiter, root, task, token, today):
    from downloader import download_finmind_complement as worker
    return worker._request_independent(sessions.get(), limiter, root, task, token, today=today)


def run_batch(connection, executor, sessions, root, current, account, budget, reservation, now,
              limiter, token, *, config, max_calls, delegated=frozenset(), datasets=None,
              required_keys=None, heartbeat):
    from downloader import download_finmind_complement as worker
    from downloader.finmind_object_pipeline import _claim, _release
    from downloader.finmind_account import backfill_budget, refresh_dispatch_account, pacing_interval
    from downloader.finmind_scheduling import incremental_reservation, protected_stock_opening
    from downloader.finmind_corrections import correction_context

    due = current.priority == 0
    if (not config['enabled'] or not supported(connection, current)
            or not account.get('observed_at_utc') or (not due and not budget.get('allowed'))
            or (not due and reservation.get('ready_requests', 0)) or required_keys is not None):
        return None
    reserve = 2 if due else budget.get('reserve', 0)
    # This is a continuous work quantum, not a buffer size. Keep refilling the
    # bounded in-flight window instead of draining it every 120 requests. Each
    # admission still checks due work, shared quota, calendars and disk space.
    limit = min(config['max_batch_requests'], max_calls, max(0, budget.get('remaining', 0) - reserve))
    if limit <= 0:
        return None
    # A worst-case response is 64 MiB. Bound admitted responses, not just
    # sockets: completed futures may retain their body until the owner stores it.
    workers = min(config['download_workers'], config['max_buffered_response_bytes'] // worker.BULK_MAX_RESPONSE_BYTES)
    started = time.monotonic()
    jobs = {}
    attempts = accepted = nonempty = rows_total = failures = cancelled = preflight_holds = 0
    failure_codes = {}
    stop = threading.Event()
    cancellable = _CancellableLimiter(limiter, stop)
    last = pause = None
    initial = current
    next_heartbeat = started + worker.STATUS_INTERVAL_SECONDS
    while True:
        stamp = worker._now()
        account = refresh_dispatch_account(account, token, root.parent, stamp)
        limiter.interval_seconds = pacing_interval(account)
        plan = incremental_reservation(root.parent, stamp)
        if protected_stock_opening(stamp):
            pause = 'protected_opening'
        elif shutil.disk_usage(root).free < worker.MIN_FREE_BYTES:
            pause = 'disk_guard'
        elif (not due and plan.get('ready_requests', 0)) or plan.get('queue_errors'):
            pause = 'incremental_reserve'
        if not pause:
            admission = backfill_budget(account, root.parent, fixed_incremental_requests=plan['reserve_requests'],
                                        in_flight=len(jobs), now=stamp, prioritize_due=True, reservation_plan=plan)
            reserve = 2 if due else admission['reserve']
            if (not due and not admission['allowed']) or admission['remaining'] <= reserve + len(jobs):
                pause = 'incremental_reserve'
            available = max(0, admission['remaining'] - reserve - len(jobs))
        else:
            available = 0
        while not pause and available > 0 and attempts < limit and len(jobs) < workers:
            candidate = initial or worker._next_task(connection, stamp, delegated=delegated,
                                                      datasets=datasets, incremental_only=due)
            initial = None
            if not due and candidate is None and worker._refill_current_history(connection, root, stamp,
                                                                    delegated=delegated, datasets=datasets):
                candidate = worker._next_task(connection, stamp, delegated=delegated,
                                               datasets=datasets)
            # The canonical selector is still the authority for preemptions,
            # nine-stage ordering, repair and cooled/terminal states.
            if candidate is None or candidate.dataset != current.dataset or not supported(connection, candidate):
                break
            _claim(connection, candidate, advance_cursor=True)
            try:
                future = executor.submit(_fetch, sessions, cancellable, root, candidate, token,
                                         stamp.astimezone(worker.TAIPEI).date())
                jobs[future] = candidate
                attempts += 1
                available -= 1
            except BaseException:
                _release(connection, candidate)
                raise
        if not jobs:
            break
        done, _ = wait(jobs, timeout=1, return_when=FIRST_COMPLETED)
        for future in done:
            task = jobs.pop(future)
            try:
                rows, metadata = future.result()
                metadata.update(json_pipeline_contract_version=CONTRACT_VERSION,
                                pipeline_configuration_sha256=config['configuration_sha256'])
                context = correction_context(connection, task)
                receipt = worker._store(root, task, rows, worker._now(), request_metadata=metadata,
                                        **({'correction': context} if context else {}))
                # The result and claim release below commit together. Keep
                # durable source/receipt publication and recovery unchanged,
                # without two back-to-back queue fsyncs for one finalization.
                worker._save_result(connection, task, receipt, worker._now(), commit=False)
                accepted += 1
                nonempty += int(receipt['rows'] > 0)
                rows_total += receipt['rows']
                last = {'dataset': task.dataset, 'data_id': task.data_id, 'partition': task.partition,
                        'status': receipt['status'], 'rows': receipt['rows'],
                        'observed_at_utc': worker._iso(worker._now())}
            except RateLimitWaitCancelled:
                # No request/ledger write happened. Restore the prior task;
                # do not manufacture a failure, retry or consumed API call.
                cancelled += 1
            except (worker.SourceError, OSError, ValueError, pa.ArrowException) as exception:
                error = worker._publication_error(exception)
                worker._save_failure(connection, task, error, worker._now(), commit=False)
                failures += 1
                failure_codes[error.code] = failure_codes.get(error.code, 0) + 1
                if error.code in LOCAL_PREFLIGHT_ERRORS:
                    preflight_holds += 1  # The durable ledger failed before HTTP.
                last = {'dataset': task.dataset, 'data_id': task.data_id, 'partition': task.partition,
                        'status': 'failed', 'error_code': error.code,
                        'observed_at_utc': worker._iso(worker._now())}
                if error.code in {'rate_limited', 'invalid_token', 'ip_banned', 'not_entitled'} | LOCAL_PREFLIGHT_ERRORS:
                    pause = error.code
                    stop.set()
            finally:
                _release(connection, task)
            # Refill one vacated slot before publishing another completed
            # response. A completion burst must not drain every socket behind
            # synchronous publication. Unconsumed futures remain in jobs, so
            # the same byte bound includes all retained response bodies.
            break
        if time.monotonic() >= next_heartbeat:
            heartbeat(next(iter(jobs.values()), None), last)
            next_heartbeat = time.monotonic() + worker.STATUS_INTERVAL_SECONDS
    elapsed = time.monotonic() - started
    calibratable = accepted == attempts and accepted >= 2 and not failures and not cancelled
    # A batch without an ETA sample still needs observable real outcomes. Do
    # not turn cancellation, failure or an incomplete cycle into capacity.
    atomic_write_json(root / 'json_pipeline_last.json', {
        'contract_version': CONTRACT_VERSION, 'observed_at_utc': datetime.now(UTC).isoformat(),
        'configuration_sha256': config['configuration_sha256'], 'dataset': current.dataset,
        'download_workers': workers, 'claimed_attempts': attempts, 'cancelled_before_request': cancelled,
        'calls': attempts-cancelled-preflight_holds, 'accepted_requests': accepted, 'nonempty_requests': nonempty,
        'local_preflight_holds': preflight_holds,
        'rows': rows_total, 'failed_tasks': failures, 'state': pause, 'elapsed_seconds': elapsed,
        'failure_codes': failure_codes,
        'batch_capacity_candidate': calibratable, 'whole_cycle_cost_verified': False,
    })
    if calibratable:
        connection.execute('INSERT INTO finmind_json_pipeline_samples '
                           '(dataset,requests,seconds,rows,nonempty_requests,configuration_sha256,observed_at_utc) '
                           'VALUES (?,?,?,?,?,?,?)', (current.dataset, accepted, elapsed, rows_total, nonempty,
                           config['configuration_sha256'], datetime.now(UTC).isoformat()))
        connection.commit()
    return {'calls': attempts - cancelled - preflight_holds, 'last': last, 'state': pause, 'seconds': elapsed,
            'accepted_requests': accepted, 'nonempty_requests': nonempty, 'failed_tasks': failures}


def measured_rates(connection, dataset, now=None):
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_json_pipeline_samples'").fetchone():
        return None
    if 'cycle_id' not in {row[1] for row in connection.execute('PRAGMA table_info(finmind_json_pipeline_samples)')}:
        return None  # Read-only telemetry before the canonical owner's migration.
    config = settings()
    stamp = now or datetime.now(UTC)
    from datetime import timedelta
    rows = connection.execute('SELECT requests,seconds,rows,nonempty_requests,cycle_id,cycle_wall_seconds,excluded_work_seconds '
                              'FROM finmind_json_pipeline_samples '
                              'WHERE dataset=? AND configuration_sha256=? AND requests>=2 AND seconds>0 AND cycle_wall_verified=1 '
                              'AND observed_at_utc>=? ORDER BY id DESC LIMIT 32',
                              (dataset, config['configuration_sha256'], (stamp - timedelta(hours=24)).isoformat())).fetchall()
    if len(rows) < 2:
        return None
    rates = sorted(row[0] * 3600 / row[1] for row in rows)
    calls, seconds = sum(r[0] for r in rows), sum(r[1] for r in rows)
    central = calls * 3600 / seconds
    return {'samples': len(rows), 'capacity_basis': 'accepted_parallel_pipeline_wall',
            'rates': {'fastest': rates[-1], 'central': central,
                      'slowest': min(central, rates[int((len(rates)-1)*.1)])},
            'central_rate_basis': 'sum_accepted_requests_over_sum_attributable_cycle_wall',
            'sampled_requests': calls, 'sampled_wall_seconds': seconds,
            'sampled_rows': sum(r[2] for r in rows), 'nonempty_requests': sum(r[3] for r in rows),
            'download_workers': config['download_workers'], 'derivation_workers': 0,
            'arrow_threads': 1, 'configuration_sha256': config['configuration_sha256'],
            'processing_engine': 'bounded_json_keepalive_single_publication_owner',
            'full_cycle_wall_verified': True,
            'measured_preemption_time_excluded': True,
            'full_owner_cycles': len({row[4] for row in rows if row[4]}),
            'owner_cycle_wall_seconds': sum({row[4]: row[5] for row in rows if row[4]}.values())}
