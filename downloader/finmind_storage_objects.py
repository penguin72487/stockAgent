"""Sponsor Pro transport for the existing Complement owner, not another worker.

One API grant obtains one whole-market/calendar-day object. The signed object
GET is a separate transfer, never an additional FinMind data query. Raw Parquet
bytes and source row order are retained; validation uses bounded Arrow batches.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import shutil
import tempfile
import time
from urllib.parse import urlsplit
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests


CONTRACT_VERSION = 2  # Whole-market files include TAIEX; legacy index history ends before them.
ENDPOINT = 'https://api.finmindtrade.com/api/v4/storage_objects'
MAX_OBJECT_BYTES = 8 * 1024**3
MAX_TRANSFER_SECONDS = 900
OBJECT_FIRST = {
    'TaiwanStockKBar': date(2019, 1, 2),
    'TaiwanFuturesKBar': date(2011, 1, 3),
    'TaiwanStockTradingDailyReport': date(2021, 6, 30),
    'TaiwanStockWarrantTradingDailyReport': date(2023, 6, 21),
    'TaiwanStockPriceTick': date(2018, 12, 7),
    'TaiwanFuturesTick': date(2011, 1, 3),
    'TaiwanOptionTick': date(2011, 1, 3),
}
CASH_OBJECTS = frozenset(name for name in OBJECT_FIRST if name.startswith('TaiwanStock'))
DOCUMENTATION = {
    name: 'https://finmind.github.io/tutor/TaiwanMarket/' + (
        'Chip/' if 'Report' in name else 'Technical/' if name in CASH_OBJECTS else 'Derivative/')
    for name in OBJECT_FIRST
}


def active_datasets(connection: sqlite3.Connection) -> frozenset[str]:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_object_profile'").fetchone():
        return frozenset()
    return frozenset(row[0] for row in connection.execute(
        'SELECT dataset FROM finmind_object_profile WHERE active=1') if row[0] in OBJECT_FIRST)


def effective_sources(connection: sqlite3.Connection, original: dict) -> dict:
    active = active_datasets(connection)
    return {name: replace(spec, universe='market', first=OBJECT_FIRST[name], endpoint='storage_objects')
            if name in active else spec for name, spec in original.items()}


def identity_clause(dataset: str, *, prefix: str = '') -> str:
    """TAIEX has independent pre-2019 history; do not discard its old frontier."""
    field = prefix + 'data_id'
    return f" AND ({field}='' OR {field}='TAIEX')" if dataset == 'TaiwanStockKBar' else f" AND {field}=''"


def forward_identity_clause(dataset: str, *, prefix: str = '') -> str:
    """Only whole-market objects need new dates; TAIEX is old-history only."""
    return f" AND {prefix}data_id=''"


def limit_benchmark_frontier(connection: sqlite3.Connection) -> None:
    if ('TaiwanStockKBar' not in active_datasets(connection) or not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='finmind_source_frontiers'").fetchone()):
        return
    row = connection.execute("SELECT older_than FROM finmind_source_frontiers "
                             "WHERE dataset='TaiwanStockKBar' AND data_id='TAIEX'").fetchone()
    boundary = (OBJECT_FIRST['TaiwanStockKBar'] - timedelta(days=1)).isoformat()
    if row and row[0] and row[0] > boundary:
        from downloader.finmind_supplemental import history_floor
        cursor = boundary if boundary >= history_floor('TaiwanStockKBar', 'TAIEX').isoformat() else None
        connection.execute('INSERT INTO finmind_object_frontier_migrations VALUES (?,?,?,1) '
                           'ON CONFLICT(dataset,data_id) DO UPDATE SET older_than=CASE '
                           'WHEN active=0 THEN excluded.older_than ELSE older_than END,active=1',
                           ('TaiwanStockKBar', 'TAIEX', row[0]))
        connection.execute("UPDATE finmind_source_frontiers SET older_than=? "
                           "WHERE dataset='TaiwanStockKBar' AND data_id='TAIEX'", (cursor,))


def configure(connection: sqlite3.Connection, account: dict, now: datetime) -> None:
    """Idempotent, reversible plan migration inside the canonical writer lock.

    Complete per-ID source artifacts remain on disk and in the migration journal.
    No exhausted source failures are reopened. A verified downgrade restores the
    prior query plan, not a fabricated success or another provider's entitlement.
    """
    from downloader.finmind_supplemental import SOURCES
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_profile ('
                       'dataset TEXT PRIMARY KEY,active INTEGER,contract_version INTEGER)')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_migrations ('
                       'dataset TEXT,data_id TEXT,partition TEXT,prior_task_json TEXT,'
                       'changed_at_utc TEXT,PRIMARY KEY(dataset,data_id,partition))')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_frontier_migrations ('
                       'dataset TEXT,data_id TEXT,older_than TEXT,active INTEGER,PRIMARY KEY(dataset,data_id))')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_transfer_samples ('
                       'id INTEGER PRIMARY KEY,dataset TEXT,partition TEXT,seconds REAL,bytes INTEGER,'
                       'rows INTEGER,observed_at_utc TEXT)')
    connection.execute('CREATE INDEX IF NOT EXISTS idx_finmind_object_samples '
                       'ON finmind_object_transfer_samples(dataset,id DESC)')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_object_derivation_samples ('
                       'id INTEGER PRIMARY KEY,dataset TEXT,partition TEXT,seconds REAL,observed_at_utc TEXT)')
    connection.execute('CREATE INDEX IF NOT EXISTS idx_finmind_object_derivation_samples '
                       'ON finmind_object_derivation_samples(dataset,id DESC)')
    enabled = account.get('tier') == 'SponsorPro'
    for dataset in OBJECT_FIRST.keys() & SOURCES.keys():
        previous = connection.execute('SELECT active,contract_version FROM finmind_object_profile WHERE dataset=?', (dataset,)).fetchone()
        if previous == (int(enabled), CONTRACT_VERSION):
            continue
        if enabled:
            cursor = connection.execute("SELECT * FROM tasks WHERE dataset=? AND data_id!='' "
                                        "AND NOT (dataset='TaiwanStockKBar' AND data_id='TAIEX' AND partition<?) "
                                        "AND state!='deprecated_query_shape'", (dataset, OBJECT_FIRST['TaiwanStockKBar'].isoformat()))
            columns = [item[0] for item in cursor.description]
            # Stream the migration; never load hundreds of thousands of heads.
            for row in cursor:
                prior = dict(zip(columns, row))
                # A renewed upgrade must capture the currently resumed legacy
                # head, rather than restore an obsolete state from the first
                # upgrade on a later downgrade. Source artifacts stay immutable.
                connection.execute('INSERT INTO finmind_object_migrations VALUES (?,?,?,?,?) '
                                   'ON CONFLICT(dataset,data_id,partition) DO UPDATE SET '
                                   'prior_task_json=excluded.prior_task_json,changed_at_utc=excluded.changed_at_utc',
                                   (dataset, prior['data_id'], prior['partition'],
                                    json.dumps(prior, sort_keys=True), now.isoformat()))
            connection.execute("UPDATE tasks SET state='deprecated_query_shape',next_attempt_at_utc=NULL "
                               "WHERE dataset=? AND data_id!='' "
                               "AND NOT (dataset='TaiwanStockKBar' AND data_id='TAIEX' AND partition<?)",
                               (dataset, OBJECT_FIRST['TaiwanStockKBar'].isoformat()))
            connection.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=NULL "
                               "WHERE dataset=? AND data_id='' AND state='object_tier_paused'", (dataset,))
        elif previous:
            for identifier, partition, body in connection.execute(
                    'SELECT data_id,partition,prior_task_json FROM finmind_object_migrations WHERE dataset=?', (dataset,)):
                prior = json.loads(body)
                connection.execute("UPDATE tasks SET state=?,next_attempt_at_utc=? "
                                   "WHERE dataset=? AND data_id=? AND partition=? AND state='deprecated_query_shape'",
                                   (prior['state'], prior['next_attempt_at_utc'], dataset, identifier, partition))
            connection.execute("UPDATE tasks SET state='object_tier_paused',next_attempt_at_utc=NULL "
                               "WHERE dataset=? AND data_id='' AND state IN ('pending','failed','inflight')", (dataset,))
            for identifier, prior in connection.execute('SELECT data_id,older_than FROM finmind_object_frontier_migrations '
                                                        'WHERE dataset=? AND active=1', (dataset,)):
                current = connection.execute('SELECT older_than FROM finmind_source_frontiers WHERE dataset=? AND data_id=?',
                                             (dataset, identifier)).fetchone()
                restored = max(filter(None, (prior, current[0] if current else None)))
                connection.execute('UPDATE finmind_source_frontiers SET older_than=? WHERE dataset=? AND data_id=?',
                                   (restored, dataset, identifier))
            connection.execute('UPDATE finmind_object_frontier_migrations SET active=0 WHERE dataset=?', (dataset,))
        connection.execute('INSERT OR REPLACE INTO finmind_object_profile VALUES (?,?,?)',
                           (dataset, int(enabled), CONTRACT_VERSION))
    limit_benchmark_frontier(connection)


@dataclass
class StorageObject:
    path: Path
    rows: int
    sha256: str
    field_non_null_counts: dict[str, int]
    observation: dict

    def __len__(self):
        return self.rows


def validate_parquet(path: Path, dataset: str, day: str) -> dict:
    """Validate the entire payload without row dictionaries, sorting or dedup."""
    from downloader.finmind_supplemental import SOURCES
    parquet = pq.ParquetFile(path)
    names = set(parquet.schema_arrow.names)
    identity = SOURCES[dataset].identity_field
    required = {'date', identity}
    if dataset.endswith('KBar'):
        required |= {'minute', 'open', 'high', 'low', 'close', 'volume'}
    elif dataset.endswith('Report'):
        required |= {'stock_id', 'securities_trader_id', 'price', 'buy', 'sell'}
    else:
        required |= {'deal_price' if dataset == 'TaiwanStockPriceTick' else 'price', 'volume'}
    if not required <= names or not parquet.metadata.num_rows:
        raise ValueError('object_invalid_schema_or_empty_file')
    counts = dict.fromkeys(parquet.schema_arrow.names, 0)
    for batch in parquet.iter_batches(batch_size=65536, use_threads=False):
        columns = dict(zip(batch.schema.names, batch.columns))
        for name, column in columns.items():
            counts[name] += len(column) - column.null_count
        for field in required:
            if columns[field].null_count:
                raise ValueError('object_null_required_field')
        dates = pc.cast(columns['date'], pa.string())
        if pc.any(pc.not_equal(pc.utf8_slice_codeunits(dates, 0, 10), day)).as_py():
            raise ValueError('object_response_outside_day')
        ids = pc.cast(columns[identity], pa.string())
        if pc.any(pc.equal(pc.utf8_trim_whitespace(ids), '')).as_py():
            raise ValueError('object_empty_identity')
        for field in required - {'date', identity, 'stock_id', 'securities_trader_id', 'minute'}:
            values = pc.cast(columns[field], pa.float64())
            # Futures objects also contain combination/spread rows. A signed
            # derivative price is not a negative quantity or corrupted quote.
            signed_price = dataset in {'TaiwanFuturesTick', 'TaiwanFuturesKBar'} and field != 'volume'
            if (pc.any(pc.invert(pc.is_finite(values))).as_py()
                    or (not signed_price and pc.any(pc.less(values, 0)).as_py())):
                raise ValueError('object_invalid_numeric_value')
        if dataset.endswith('KBar'):
            o, h, l, c = (columns[name] for name in ('open', 'high', 'low', 'close'))
            if any(pc.any(mask).as_py() for mask in (pc.less(h, l), pc.less(h, o), pc.less(h, c),
                                                     pc.greater(l, o), pc.greater(l, c))):
                raise ValueError('object_invalid_ohlc')
    return {'rows': parquet.metadata.num_rows, 'field_non_null_counts': counts}


def _safe_object_url(value: str) -> str:
    parts = urlsplit(value)
    if (parts.scheme != 'https' or not parts.hostname or parts.username or parts.password
            or parts.port not in {None, 443} or not parts.hostname.endswith('.linodeobjects.com')):
        raise ValueError('object_untrusted_redirect')
    return value


def remaining_candidates(connection: sqlite3.Connection, dataset: str, now: datetime) -> int:
    """Unseeded whole-day requests, not per-ID products or missing row counts."""
    from downloader.finmind_history_calendar import load_closures
    from downloader.finmind_supplemental import SOURCES, _eligible_anchor
    sources = effective_sources(connection, SOURCES)
    if dataset not in active_datasets(connection):
        return 0
    frontier = connection.execute('SELECT older_than,newer_than FROM finmind_source_frontiers '
                                  "WHERE dataset=? AND data_id=''", (dataset,)).fetchone()
    if not frontier:
        return 0
    closures = load_closures(connection)
    anchor = _eligible_anchor(sources[dataset], now)
    spans = []
    if frontier[0]:
        spans.append((sources[dataset].first, date.fromisoformat(frontier[0])))
    if frontier[1]:
        from datetime import timedelta
        spans.append((date.fromisoformat(frontier[1]) + timedelta(days=1), anchor))
    count = 0
    for first, last in spans:
        if last < first:
            continue
        count += (last - first).days + 1 - closures.count(dataset, first, last)
        for (stamp,) in connection.execute("SELECT partition FROM tasks WHERE dataset=? AND data_id='' "
                                           'AND partition BETWEEN ? AND ?', (dataset, str(first), str(last))):
            day = date.fromisoformat(stamp)
            if not closures.count(dataset, day, day):
                count -= 1
    if count < 0:
        raise ValueError('inconsistent_object_frontier')
    return count


def transfer_statistics(connection: sqlite3.Connection) -> dict:
    """Bounded measured cost evidence; no historical file scans or API probes."""
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_object_transfer_samples'").fetchone():
        return {}
    result = {}
    for dataset in active_datasets(connection):
        rows = connection.execute('SELECT seconds,bytes FROM finmind_object_transfer_samples '
                                  'WHERE dataset=? AND seconds>0 ORDER BY id DESC LIMIT 32', (dataset,)).fetchall()
        if not rows:
            continue
        seconds = sorted(row[0] for row in rows)
        sizes = sorted(row[1] for row in rows)
        result[dataset] = {'samples': len(rows), 'fastest_seconds': seconds[0],
                           'median_seconds': seconds[len(seconds)//2],
                           'p90_seconds': seconds[min(len(seconds)-1, int(len(seconds)*0.9))],
                           'median_bytes': sizes[len(sizes)//2],
                           'basis': 'latest_32_observed_serial_transfer_validation_store_samples_not_quota_speed'}
        if dataset == 'TaiwanStockTradingDailyReport':
            # Each raw file also schedules a mandatory zero-call local
            # aggregate. Account for its CPU/store time, not just HTTP time.
            exists = connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_object_derivation_samples'").fetchone()
            dependent = (connection.execute(
                'SELECT seconds FROM finmind_object_derivation_samples WHERE dataset=? AND seconds>0 '
                'ORDER BY id DESC LIMIT 32', (dataset,)).fetchall() if exists else [])
            costs = sorted(row[0] for row in dependent)
            result[dataset]['dependent_processing_samples'] = len(costs)
            result[dataset]['dependent_processing_unknown'] = not bool(costs)
            if costs:
                for field, index in (('fastest_seconds', 0), ('median_seconds', len(costs)//2),
                                     ('p90_seconds', min(len(costs)-1, int(len(costs)*0.9)))):
                    result[dataset][field] += costs[index]
            result[dataset]['basis'] = 'serial_transfer_validation_store_plus_mandatory_local_aggregate'
    return result


def fetch(session, limiter, root: Path, task, token: str, *, heartbeat=None):
    from downloader.download_finmind_complement import MIN_FREE_BYTES, SourceError, _response_rows
    from downloader.download_finmind_free import _record_request_start
    if task.dataset not in OBJECT_FIRST or task.data_id:
        raise ValueError('object_requires_registered_whole_market_day')
    limiter.wait()
    from downloader.download_finmind_free import ProviderError
    try:
        _record_request_start(root.parent, task.dataset)
    except ProviderError as error:
        raise SourceError(error.code, retry_after=int(error.retry_after)) from None
    started = datetime.now(UTC)
    metadata = {'object_contract_version': CONTRACT_VERSION, 'endpoint': 'storage_objects',
                'query_shape': 'whole_market_storage_object_day', 'request_start_date': task.partition,
                'request_end_date': task.partition, 'data_id': '', 'request_count': 1,
                'signed_transfer_requests': 0, 'documentation_url': DOCUMENTATION[task.dataset],
                'historical_point_in_time': False, 'universe_verified_complete': False}
    temporary = None
    response = None
    try:
        response = session.get(ENDPOINT, params={'dataset': task.dataset, 'date': task.partition},
                               headers={'Authorization': f'Bearer {token}'}, timeout=(10, 90),
                               stream=True, allow_redirects=False)
        if response.status_code in {301, 302, 303, 307, 308}:
            target = _safe_object_url(response.headers.get('Location', ''))
            response.close()
            # Never forward the credential to signed-object storage. No implicit
            # redirect can send it to an unreviewed host or persist a signed URL.
            response = session.get(target, headers={'Authorization': None}, auth=(),
                                   timeout=(10, 90), stream=True, allow_redirects=False)
            metadata['signed_transfer_requests'] = 1
        if response.status_code == 404:
            metadata['object_availability'] = 'observed_missing_file_not_verified_market_closure'
            return [], metadata
        if response.status_code != 200:
            _response_rows(response, limiter, max_response_bytes=65536)
            raise SourceError('object_invalid_response', retry_after=300)
        folder = root / '.object-downloads'
        folder.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix='finmind-', suffix='.parquet', dir=folder)
        temporary = Path(name)
        total = 0
        clock = beat = time.monotonic()
        digest = hashlib.sha256()
        with os.fdopen(fd, 'wb') as stream:
            for block in response.iter_content(chunk_size=1024**2):
                total += len(block)
                if total > MAX_OBJECT_BYTES or time.monotonic() - clock > MAX_TRANSFER_SECONDS:
                    raise SourceError('object_resource_limit', retry_after=300)
                if shutil.disk_usage(folder).free < MIN_FREE_BYTES + len(block):
                    raise SourceError('object_disk_guard', retry_after=300)
                digest.update(block)
                stream.write(block)
                if heartbeat and time.monotonic() - beat >= 30:
                    heartbeat()
                    beat = time.monotonic()
            stream.flush()
            os.fsync(stream.fileno())
        try:
            checked = validate_parquet(temporary, task.dataset, task.partition)
        except (ValueError, pa.ArrowException):
            from downloader.artifact_io import atomic_write_json, durable_replace
            failed = root / 'object_validation_failures' / task.dataset / task.partition / f'{digest.hexdigest()}.parquet'
            failed.parent.mkdir(parents=True, exist_ok=True)
            if not failed.exists():
                durable_replace(temporary, failed)
            atomic_write_json(failed.with_suffix('.json'), {
                'object_contract_version': CONTRACT_VERSION, 'dataset': task.dataset, 'partition': task.partition,
                'status': 'validation_failed_not_current_data', 'sha256': digest.hexdigest(),
                'bytes': total, 'observed_at_utc': datetime.now(UTC).isoformat()})
            raise
        completed = datetime.now(UTC)
        observation = {'request_id': uuid.uuid4().hex, 'request_started_at_utc': started.isoformat(),
                       'response_received_at_utc': completed.isoformat(), 'response_rows': checked['rows'],
                       'network_seconds': (completed - started).total_seconds()}
        metadata.update(object_bytes=total, response_rows=checked['rows'],
                        validation='full_arrow_batch_schema_day_identity_numeric_ohlc_v1',
                        source_order='preserved_including_duplicate_ticks',
                        transfer_and_validation_seconds=observation['network_seconds'])
        result = StorageObject(temporary, checked['rows'], digest.hexdigest(), checked['field_non_null_counts'], observation)
        temporary = None  # Ownership passes to the canonical store.
        return result, metadata
    except requests.RequestException as error:
        raise SourceError(type(error).__name__) from None
    except (ValueError, pa.ArrowException):
        raise SourceError('object_validation_error', retry_after=900) from None
    finally:
        if response is not None:
            response.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)
