"""Consume reviewed FinMind correction scopes through existing worker tasks.

This module does not parse announcements, infer scope, fetch data, create tasks,
or commit an enclosing worker transaction. Call queue helpers under worker.lock.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from typing import Any
from uuid import uuid4

import pyarrow.parquet as pq

from downloader.finmind_scheduling import PRODUCT_HISTORY_STARTS, TAIPEI


OWNERS = frozenset({'sponsor', 'complement', 'free'})
FREE_DATASETS = frozenset({'TaiwanStockStatisticsOfOrderBookAndTrade', 'TaiwanVariousIndicators5Seconds'})
CALENDAR = 'TaiwanStockTradingDate'
MASTER = 'TaiwanStockInfoWithWarrant'
DERIVED_DATASETS = {'TaiwanStockInstitutionalInvestorsBuySell': 'TaiwanStockInstitutionalInvestorsBuySellWide'}
MAX_PLAN_BYTES = 4 * 1024**2
MAX_RECEIPT_BYTES = 1024**2
MAX_PROOF_BYTES = 512 * 1024**2
PERMANENT_ERRORS = frozenset({'invalid_token', 'not_entitled', 'invalid_request', 'provider_bad_request',
                              'TokenIllegal', 'ip_banned'})
MUTABLE_STATES = frozenset({'complete', 'observed_empty', 'pending', 'failed'})


def _stamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError('correction timestamp must be an aware ISO string')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('correction timestamp must be timezone-aware')
    return stamp.astimezone(UTC)


def _day(value: Any) -> date:
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('correction date must be YYYY-MM-DD')
    return date.fromisoformat(value)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def _local_path(root: Path, relative: Any) -> Path:
    if not isinstance(relative, str):
        raise ValueError('missing local proof path')
    name = Path(relative)
    path = (root / name).resolve()
    if name.is_absolute() or '..' in name.parts or not path.is_relative_to(root.resolve()):
        raise ValueError('unsafe correction proof path')
    return path


def _read_receipt(root: Path, task: dict[str, Any]) -> dict[str, Any]:
    try:
        path = _local_path(root, task.get('receipt_path'))
        if path.stat().st_size > MAX_RECEIPT_BYTES:
            return {}
        value = json.loads(path.read_bytes())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _load_plan(root: Path, owner: str) -> tuple[dict[str, Any], str]:
    if owner not in OWNERS:
        raise ValueError('unsupported correction worker owner')
    base = root if owner == 'free' else root.parent
    path = base / 'announcements' / 'repair_plan.json'
    if not path.exists():
        return {'schema_version': 1, 'plan_id': None, 'requests': []}, ''
    if not path.resolve().is_relative_to(base.resolve()) or path.stat().st_size > MAX_PLAN_BYTES:
        raise ValueError('unsafe or oversized correction plan')
    stat = path.stat()
    signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
    return _parse_plan(str(path.resolve()), signature)


@lru_cache(maxsize=16)
def _parse_plan(path_text: str, signature: tuple[int, ...]) -> tuple[dict[str, Any], str]:
    path = Path(path_text)
    raw = path.read_bytes()
    stat = path.stat()
    if signature != (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns):
        raise ValueError('correction plan changed during verification')
    plan = json.loads(raw)
    if (not isinstance(plan, dict) or plan.get('schema_version') != 1
            or not isinstance(plan.get('plan_id'), str) or not plan['plan_id']
            or not isinstance(plan.get('requests'), list) or len(plan['requests']) > 4096):
        raise ValueError('invalid correction plan envelope')
    seen = set()
    for request in plan['requests']:
        if not isinstance(request, dict):
            raise ValueError('invalid correction request')
        for name in ('correction_id', 'dataset', 'reason', 'source_url'):
            if not isinstance(request.get(name), str) or not request[name] or len(request[name]) > 4096:
                raise ValueError('invalid correction request identity')
        if request.get('owner') not in OWNERS or type(request.get('allow_empty')) is not bool:
            raise ValueError('correction requires explicit owner and empty policy')
        if request.get('scope_kind') != 'snapshot':
            start, end = _day(request.get('start_date')), _day(request.get('end_date'))
            if start > end:
                raise ValueError('reversed correction dates')
        elif request.get('allow_empty'):
            raise ValueError('snapshot cannot authorize empty deletion')
        _stamp(request.get('required_after_utc'))
        ids = request.get('data_ids')
        if ids is not None and (not isinstance(ids, list) or len(ids) > 50000
                                or any(not isinstance(item, str) or not item or len(item) > 128 for item in ids)):
            raise ValueError('invalid correction identifier scope')
        identity = (request['owner'], request['correction_id'], request['dataset'])
        if identity in seen:
            raise ValueError('duplicate correction request identity')
        seen.add(identity)
        derived = request.get('derived_datasets', [])
        if (not isinstance(derived, list) or any(item != DERIVED_DATASETS.get(request['dataset']) for item in derived)):
            raise ValueError('unproven derived correction relationship')
    return plan, hashlib.sha256(raw).hexdigest()


@contextmanager
def queue_transaction(connection: sqlite3.Connection):
    """Atomically apply a notice without committing the caller's transaction."""
    name = 'finmind_correction_' + uuid4().hex
    connection.execute(f'SAVEPOINT {name}')
    try:
        yield
    except BaseException:
        connection.execute(f'ROLLBACK TO SAVEPOINT {name}')
        connection.execute(f'RELEASE SAVEPOINT {name}')
        raise
    else:
        connection.execute(f'RELEASE SAVEPOINT {name}')


def _schema(connection: sqlite3.Connection) -> None:
    connection.execute('''CREATE TABLE IF NOT EXISTS finmind_correction_tasks (
        owner TEXT NOT NULL,correction_id TEXT NOT NULL,dataset TEXT NOT NULL,
        data_id TEXT NOT NULL,partition TEXT NOT NULL,kind TEXT NOT NULL,
        plan_id TEXT NOT NULL,plan_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,
        state TEXT NOT NULL,first_seen_at_utc TEXT NOT NULL,applied_at_utc TEXT,
        completed_at_utc TEXT,prior_task_json TEXT NOT NULL,receipt_evidence_json TEXT,last_checked_at_utc TEXT,
        PRIMARY KEY(owner,correction_id,dataset,data_id,partition))''')
    connection.execute('''CREATE TABLE IF NOT EXISTS finmind_correction_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,owner TEXT NOT NULL,correction_id TEXT NOT NULL,
        dataset TEXT NOT NULL,data_id TEXT NOT NULL,partition TEXT NOT NULL,
        observed_at_utc TEXT NOT NULL,action TEXT NOT NULL,prior_task_json TEXT NOT NULL)''')
    connection.execute('CREATE INDEX IF NOT EXISTS idx_finmind_correction_task_context '
                       'ON finmind_correction_tasks(dataset,data_id,partition,state)')
    connection.execute('CREATE INDEX IF NOT EXISTS idx_finmind_correction_reconcile '
                       'ON finmind_correction_tasks(owner,state,last_checked_at_utc)')


def _has_schema(connection: sqlite3.Connection) -> bool:
    return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='finmind_correction_tasks'").fetchone() is not None


def _task_dict(task: Any) -> dict[str, Any]:
    return task if isinstance(task, dict) else {key: getattr(task, key) for key in
                                               ('dataset', 'data_id', 'partition', 'kind')}


def _bounds(task: dict[str, Any], now: datetime) -> tuple[date, date] | None:
    partition, kind = task['partition'], task['kind']
    try:
        if kind in {'id_history', 'derived'} and partition == 'history':
            first = PRODUCT_HISTORY_STARTS.get(task['dataset'], date(1900, 1, 1))
            return first, now.astimezone(TAIPEI).date()
        if kind == 'year':
            start = date(int(partition), 1, 1) if re.fullmatch(r'\d{4}', partition) else _day(partition)
            return start, date(start.year, 12, 31)
        start = _day(partition)
        if kind in {'day', 'id_day', 'derived'}:
            return start, start
        if kind == 'two_day':
            return start, start + timedelta(days=1)
        if kind in {'month', 'id_month'}:
            return start, date(start.year + (start.month == 12), start.month % 12 + 1, 1) - timedelta(days=1)
    except (TypeError, ValueError):
        return None
    return None


def _matches(task: dict[str, Any], request: dict[str, Any], now: datetime) -> bool:
    if task['dataset'] != request['dataset']:
        return False
    if request.get('scope_kind') == 'snapshot':
        return task['kind'] == 'snapshot'
    ids = request.get('data_ids')
    # A canonical whole-market partition can repair named IDs without creating
    # a new per-ID downloader. Empty authorization below remains stricter.
    if ids and task['data_id'] and task['data_id'] not in ids:
        return False
    bounds = _bounds(task, now)
    return bool(bounds and bounds[0] <= _day(request['end_date']) and bounds[1] >= _day(request['start_date']))


def _empty_token(owner: str, task: dict[str, Any], request: dict[str, Any]) -> str | None:
    ids = request.get('data_ids')
    if (not request['allow_empty'] or request.get('scope_kind') == 'snapshot' or task['kind'] not in {'day', 'derived'}
            or request['start_date'] != request['end_date'] or task['partition'] != request['start_date']
            or (not task['data_id'] and ids) or (task['data_id'] and ids and set(ids) != {task['data_id']})):
        return None
    return _day_token(owner, task['dataset'], task['data_id'], task['partition'], request['correction_id'])


def _day_token(owner: str, dataset: str, data_id: str, partition: str, correction_id: str) -> str:
    identity = {'owner': owner, 'dataset': dataset, 'data_id': data_id, 'partition': partition,
                'day': partition, 'correction_id': correction_id}
    return hashlib.sha256(_json(identity).encode()).hexdigest()


def verified_authoritative_empty(receipt: dict[str, Any], *, owner: str, dataset: str,
                                 data_id: str, partition: str) -> bool:
    """Verify a preserved exact-day marker without needing the current plan.

    The digest binds the audit identity; it is not a cryptographic provider
    signature. Only reviewed local plans authorize creation of this marker.
    """
    try:
        _day(partition)
        if (owner not in OWNERS or receipt.get('status') != 'complete'
                or type(receipt.get('rows')) is not int or receipt['rows'] != 0 or receipt.get('parquet_path')
                or receipt.get('dataset') != dataset or receipt.get('data_id', '') != data_id
                or receipt.get('partition', receipt.get('date')) != partition
                or receipt.get('kind', 'day' if owner == 'free' else None) not in {'day', 'derived'}):
            return False
        marker = receipt.get('finmind_corrections', {})
        if (not isinstance(marker, dict) or marker.get('schema_version') != 1
                or marker.get('authoritative_empty') is not True or marker.get('allow_empty') is not True
                or any(marker.get(key) != value for key, value in
                       {'owner': owner, 'dataset': dataset, 'data_id': data_id, 'partition': partition}.items())):
            return False
        ids, tokens = marker.get('correction_ids'), marker.get('empty_authorization_tokens')
        return (isinstance(ids, list) and all(isinstance(item, str) and item for item in ids)
                and isinstance(tokens, dict) and bool(tokens)
                and all(correction_id in ids and token == _day_token(owner, dataset, data_id, partition, correction_id)
                        for correction_id, token in tokens.items()))
    except (ValueError, TypeError):
        return False


def _context(owner: str, task: dict[str, Any], requests: list[dict[str, Any]]) -> dict[str, Any]:
    tokens = {request['correction_id']: token for request in requests
              if (token := _empty_token(owner, task, request)) is not None}
    return {'schema_version': 1, 'owner': owner, **{key: task[key] for key in ('dataset', 'data_id', 'partition')},
            'correction_ids': [request['correction_id'] for request in requests],
            'required_after_utc': max((_stamp(request['required_after_utc']).isoformat() for request in requests), default=None),
            'allow_empty': bool(tokens), 'empty_authorization_tokens': tokens}


def correction_context(connection: sqlite3.Connection, task: Any) -> dict[str, Any]:
    """Return task-bound metadata; never a dataset-wide empty-guard override."""
    task = _task_dict(task)
    if not _has_schema(connection):
        return {}
    records = connection.execute(
        "SELECT owner,request_json FROM finmind_correction_tasks WHERE dataset=? AND data_id=? AND partition=? "
        "AND state IN ('queued','observed_empty_unverified','calendar_excluded_unverified') ORDER BY correction_id",
        (task['dataset'], task['data_id'], task['partition']),
    ).fetchall()
    if not records:
        return {}
    owners = {row[0] for row in records}
    if len(owners) != 1:
        raise ValueError('ambiguous correction task owner')
    return _context(records[0][0], task, [json.loads(row[1]) for row in records])


def correction_receipt_metadata(context: dict[str, Any], *, authoritative_empty: bool = False) -> dict[str, Any]:
    if not context:
        return {}
    if authoritative_empty and not context.get('allow_empty'):
        raise ValueError('empty correction lacks an exact day authorization')
    return {'finmind_corrections': {**context, 'authoritative_empty': authoritative_empty}}


def _post_notice_proven(receipt: dict[str, Any], request: dict[str, Any], owner: str,
                        task: dict[str, Any], fetched: datetime) -> bool:
    if request.get('watermark_basis') not in {
        'first_detection_of_new_or_edited_notice', 'first_detection_same_day_baseline',
        'baseline_same_day_detection_not_pit',
    }:
        return True
    marker = receipt.get('finmind_corrections', {})
    identity = {'owner': owner, 'dataset': task['dataset'], 'data_id': task['data_id']}
    if task['kind'] != 'snapshot':
        identity['partition'] = task['partition']
    if (isinstance(marker, dict) and request['correction_id'] in marker.get('correction_ids', [])
            and all(marker.get(key) == value for key, value in identity.items())):
        return True
    metadata = receipt.get('request', {})
    if not isinstance(metadata, dict):
        metadata = {}
    try:
        started = _stamp(receipt.get('request_started_at_utc', metadata.get('request_started_at_utc')))
        return _stamp(request['required_after_utc']) <= started <= fetched
    except ValueError:
        return False


@lru_cache(maxsize=32768)
def _parquet_verified(path_text: str, signature: tuple[int, ...], digest: str, rows: int) -> bool:
    """An unchanged inode/content signature may reuse its exact-byte proof."""
    path = Path(path_text)
    if _sha(path) != digest or pq.ParquetFile(path).metadata.num_rows != rows:
        return False
    stat = path.stat()
    return signature == (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def _verified_receipt(root: Path, task: dict[str, Any], request: dict[str, Any],
                      owner: str, receipt: dict[str, Any]) -> dict[str, Any] | None:
    """Hash and Parquet-footer check only an affected receipt; never scan rows."""
    try:
        fetched = _stamp(receipt.get('fetched_at_utc'))
        if (receipt.get('status') != 'complete' or receipt.get('dataset') != task['dataset']
                or fetched < _stamp(request['required_after_utc'])
                or not _post_notice_proven(receipt, request, owner, task, fetched)):
            return None
        if owner == 'free':
            if task['dataset'] == MASTER:
                if (receipt.get('query_scope') != 'full_table_snapshot'
                        or receipt.get('snapshot_date_taipei') != task['partition']
                        or receipt.get('parquet_path') != f'snapshots/{MASTER}/snapshot={task["partition"]}-full.parquet'):
                    return None
            elif receipt.get('date') != task['partition']:
                return None
        elif (receipt.get('partition') != task['partition'] or receipt.get('data_id') != task['data_id']
              or receipt.get('kind') != task['kind']):
            return None
        rows = receipt.get('rows')
        if type(rows) is not int or rows < 0 or ('rows' in task and rows != task['rows']):
            return None
        evidence = {'fetched_at_utc': receipt['fetched_at_utc'], 'rows': rows,
                    'status': 'complete', 'checked_dataset': task['dataset'], 'checked_partition': task['partition']}
        if rows == 0:
            token = _empty_token(owner, task, request)
            marker = receipt.get('finmind_corrections', {})
            if (not token or not verified_authoritative_empty(receipt, owner=owner, dataset=task['dataset'],
                                                              data_id=task['data_id'], partition=task['partition'])
                    or marker.get('empty_authorization_tokens', {}).get(request['correction_id']) != token
                    or receipt.get('parquet_path')):
                return None
            evidence['authoritative_empty_token'] = token
            return evidence
        path = _local_path(root, receipt.get('parquet_path'))
        before = path.stat()
        signature = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        if (type(receipt.get('parquet_size_bytes')) is not int or not 0 < before.st_size <= MAX_PROOF_BYTES
                or path.stat().st_size != receipt['parquet_size_bytes']
                or not re.fullmatch(r'[a-f0-9]{64}', str(receipt.get('sha256', '')))
                or not _parquet_verified(str(path), signature, receipt['sha256'], rows)):
            return None
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            return None
        metadata = receipt.get('request', {})
        if metadata:
            start = metadata.get('covered_start_date', metadata.get('request_start_date'))
            end = metadata.get('covered_end_date', metadata.get('request_end_date'))
            bounds = _bounds(task, _stamp(receipt['fetched_at_utc']))
            if start is not None and end is not None and bounds is not None:
                needed_start = max(bounds[0], _day(request['start_date']))
                needed_end = min(bounds[1], _day(request['end_date']))
                if _day(start) > needed_start or _day(end) < needed_end:
                    return None
        evidence.update(parquet_path=receipt['parquet_path'], parquet_sha256=receipt['sha256'],
                        parquet_size_bytes=receipt['parquet_size_bytes'])
        return evidence
    except (OSError, ValueError, TypeError, AttributeError):
        return None


def _deferred(task: dict[str, Any], now: datetime, *, empty_authorized: bool = False) -> bool:
    mutable = task['state'] in MUTABLE_STATES or (empty_authorized and task['state'] in {'non_session', 'not_observation_date'})
    if not mutable or task.get('error_code') in PERMANENT_ERRORS:
        return True
    if task['state'] in {'failed', 'pending'} and task.get('next_attempt_at_utc'):
        try:
            return _stamp(task['next_attempt_at_utc']) > now
        except ValueError:
            return True
    return False


def _expanded_requests(plan: dict[str, Any]):
    for request in plan['requests']:
        yield request
        for dataset in request.get('derived_datasets', []):
            yield {**request, 'dataset': dataset, 'derived_datasets': [],
                   'dependency_dataset': request['dataset']}


def apply_worker_corrections(connection: sqlite3.Connection, root: Path, owner: str,
                             now: datetime, *, max_receipts: int = 128) -> dict[str, Any]:
    """Mark existing overlapping tasks due; repeated calls discover late seeds."""
    plan, digest = _load_plan(root, owner)
    summary = {'plan_id': plan['plan_id'], 'applied': 0, 'already_refreshed': 0, 'deferred': 0,
               'unchanged': 0, 'unmatched_requests': 0, 'owner_skipped': 0}
    checked = 0
    with queue_transaction(connection):
        _schema(connection)
        for request in _expanded_requests(plan):
            if request['owner'] != owner:
                summary['owner_skipped'] += 1
                continue
            cursor = connection.execute('SELECT * FROM tasks WHERE dataset=?', (request['dataset'],))
            columns = [column[0] for column in cursor.description]
            tasks = [dict(zip(columns, row)) for row in cursor.fetchall()]
            matched = [task for task in tasks if _matches(task, request, now)]
            summary['unmatched_requests'] += int(not matched)
            for task in matched:
                key = (owner, request['correction_id'], task['dataset'], task['data_id'], task['partition'])
                old = connection.execute('SELECT request_json,state FROM finmind_correction_tasks WHERE '
                                         'owner=? AND correction_id=? AND dataset=? AND data_id=? AND partition=?', key).fetchone()
                if old and old[0] != _json(request):
                    raise ValueError('correction_id reused for changed scope; require a new content identity')
                empty_authorized = _empty_token(owner, task, request) is not None
                # A normal seed may classify a holiday again after a crash.
                # Only the exact day authorization can recover its queued job.
                retry_excluded = (old and old[1] == 'queued' and empty_authorized
                                  and task['state'] in {'non_session', 'not_observation_date'})
                if old and old[1] in {'queued', 'repaired', 'already_refreshed', 'observed_empty_unverified', 'calendar_excluded_unverified'} and not retry_excluded:
                    summary['unchanged'] += 1
                    continue
                state, evidence = 'deferred', None
                if _stamp(request['required_after_utc']) <= now and not _deferred(task, now, empty_authorized=empty_authorized):
                    receipt = _read_receipt(root, task)
                    fresh = False
                    try:
                        fresh = _stamp(receipt.get('fetched_at_utc')) >= _stamp(request['required_after_utc'])
                    except ValueError:
                        pass
                    if request.get('dependency_dataset'):
                        # The canonical derived task waits for its dirty parent;
                        # never reuse a timestamp-only proof of stale wide rows.
                        fresh = False
                    if fresh and checked >= max_receipts:
                        state = 'deferred'
                    else:
                        if fresh:
                            checked += 1
                            evidence = _verified_receipt(root, task, request, owner, receipt)
                        state = 'already_refreshed' if evidence else 'queued'
                if old:
                    connection.execute('UPDATE finmind_correction_tasks SET state=?,applied_at_utc=?,completed_at_utc=?,receipt_evidence_json=? '
                                       'WHERE owner=? AND correction_id=? AND dataset=? AND data_id=? AND partition=?',
                                       (state, now.isoformat() if state == 'queued' else None,
                                        now.isoformat() if evidence else None, _json(evidence) if evidence else None, *key))
                else:
                    connection.execute('INSERT INTO finmind_correction_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                                       (*key, task['kind'], plan['plan_id'], digest, _json(request), state, now.isoformat(),
                                        now.isoformat() if state == 'queued' else None, now.isoformat() if evidence else None,
                                        _json(task), _json(evidence) if evidence else None, None))
                if state == 'queued':
                    connection.execute('INSERT INTO finmind_correction_events '
                                       '(owner,correction_id,dataset,data_id,partition,observed_at_utc,action,prior_task_json) '
                                       'VALUES (?,?,?,?,?,?,?,?)', (*key, now.isoformat(), 'queued', _json(task)))
                    connection.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=?,priority=min(priority,1),"
                                       "error_code='provider_correction_due' WHERE dataset=? AND data_id=? AND partition=?",
                                       (now.isoformat(), task['dataset'], task['data_id'], task['partition']))
                summary[{'queued': 'applied', 'already_refreshed': 'already_refreshed', 'deferred': 'deferred'}[state]] += 1
    return summary


def reconcile_worker_corrections(connection: sqlite3.Connection, root: Path, owner: str,
                                 now: datetime, *, max_receipts: int = 128) -> dict[str, int]:
    """Persist verified completion evidence; status-only successes do not pass."""
    summary = {'checked': 0, 'repaired': 0, 'unverified': 0}
    if owner not in OWNERS:
        raise ValueError('unsupported correction worker owner')
    if not _has_schema(connection):
        return summary
    with queue_transaction(connection):
        records = connection.execute(
            "SELECT c.correction_id,c.dataset,c.data_id,c.partition,c.request_json FROM finmind_correction_tasks c "
            "JOIN tasks t ON t.dataset=c.dataset AND t.data_id=c.data_id AND t.partition=c.partition "
            "WHERE c.owner=? AND ((c.state='queued' AND t.state IN ('complete','observed_empty','non_session','not_observation_date')) "
            "OR (c.state IN ('observed_empty_unverified','calendar_excluded_unverified') AND t.state='complete')) "
            "ORDER BY COALESCE(c.last_checked_at_utc,''),c.first_seen_at_utc,c.correction_id,c.dataset,c.data_id,c.partition LIMIT ?",
            (owner, max_receipts),
        ).fetchall()
        for correction_id, dataset, data_id, partition, request_json in records:
            cursor = connection.execute('SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                                        (dataset, data_id, partition))
            row = cursor.fetchone()
            if row is None:
                continue
            task = dict(zip((column[0] for column in cursor.description), row))
            summary['checked'] += 1
            connection.execute('UPDATE finmind_correction_tasks SET last_checked_at_utc=? '
                               'WHERE owner=? AND correction_id=? AND dataset=? AND data_id=? AND partition=?',
                               (now.isoformat(), owner, correction_id, dataset, data_id, partition))
            request = json.loads(request_json)
            authorized_excluded = (task['state'] in {'non_session', 'not_observation_date'}
                                   and _empty_token(owner, task, request) is not None)
            evidence = (_verified_receipt(root, task, request, owner, _read_receipt(root, task))
                        if task['state'] == 'complete' or authorized_excluded else None)
            if evidence is None:
                summary['unverified'] += 1
                state = None
                if task['state'] == 'observed_empty':
                    receipt = _read_receipt(root, task)
                    try:
                        stamp = _stamp(receipt.get('fetched_at_utc'))
                        if (receipt.get('status') == 'observed_empty' and receipt.get('rows') == 0
                                and all(receipt.get(key) == task[key] for key in ('dataset','data_id','partition','kind'))
                                and stamp >= _stamp(request['required_after_utc'])
                                and _post_notice_proven(receipt, request, owner, task, stamp)):
                            state = 'observed_empty_unverified'
                    except (ValueError, TypeError):
                        pass
                elif task['state'] in {'non_session', 'not_observation_date'} and not authorized_excluded:
                    state = 'calendar_excluded_unverified'
                if state:
                    connection.execute("UPDATE finmind_correction_tasks SET state=?,completed_at_utc=NULL "
                                       "WHERE owner=? AND correction_id=? AND dataset=? AND data_id=? AND partition=?",
                                       (state, owner, correction_id, dataset, data_id, partition))
                continue
            connection.execute("UPDATE finmind_correction_tasks SET state='repaired',completed_at_utc=?,receipt_evidence_json=? "
                               "WHERE owner=? AND correction_id=? AND dataset=? AND data_id=? AND partition=?",
                               (now.isoformat(), _json(evidence), owner, correction_id, dataset, data_id, partition))
            summary['repaired'] += 1
    return summary


def free_correction_due(root: Path, dataset: str, day: date, receipt: dict[str, Any],
                        now: datetime) -> dict[str, Any]:
    """Read-only day decision; the Free worker retains its normal writer/archive."""
    if dataset not in FREE_DATASETS | {CALENDAR, MASTER}:
        return {'due': False, 'context': {}, 'reason': 'unsupported_free_dataset'}
    plan, _ = _load_plan(root, 'free')
    task = {'dataset': dataset, 'data_id': '', 'partition': day.isoformat(),
            'kind': 'snapshot' if dataset in {CALENDAR, MASTER} else 'day'}
    requests = [request for request in plan['requests'] if request['owner'] == 'free'
                and _stamp(request['required_after_utc']) <= now and _matches(task, request, now)]
    if not requests:
        return {'due': False, 'context': {}, 'reason': 'no_matching_correction'}
    if dataset == CALENDAR:
        pending = [request for request in requests if not _verified_calendar(root, receipt, request)]
        return {'due': bool(pending), 'context': _context('free', task, pending) if pending else {},
                'reason': 'correction_due' if pending else 'verified_refreshed', 'plan_id': plan['plan_id']}
    task_state = {'state': 'failed' if receipt.get('status') in {'partial', 'provider_empty'} else receipt.get('status', 'pending'), 'error_code': receipt.get('error_code'),
                  'next_attempt_at_utc': receipt.get('retry_at_utc')}
    if _deferred(task_state, now):
        return {'due': False, 'context': {}, 'reason': 'provider_or_task_cooldown'}
    pending = [request for request in requests if _verified_receipt(root, task, request, 'free', receipt) is None]
    return {'due': bool(pending), 'context': _context('free', task, pending) if pending else {},
            'reason': 'correction_due' if pending else 'verified_refreshed', 'plan_id': plan['plan_id']}


def _verified_calendar(root: Path, receipt: dict[str, Any], request: dict[str, Any]) -> bool:
    try:
        path = root / 'calendar.json'
        observed = _stamp(receipt.get('observed_at_utc'))
        task = {'dataset': CALENDAR, 'data_id': '', 'partition': observed.astimezone(TAIPEI).date().isoformat(), 'kind': 'snapshot'}
        if (not path.resolve().is_relative_to(root.resolve()) or path.stat().st_size > MAX_RECEIPT_BYTES
                or json.loads(path.read_bytes()) != receipt or receipt.get('schema_version') != 1
                or receipt.get('source_dataset') != CALENDAR
                or observed < _stamp(request['required_after_utc'])
                or not _post_notice_proven(receipt, request, 'free', task, observed)):
            return False
        days = receipt.get('dates')
        return (isinstance(days, list) and 0 < len(days) <= 50000
                and all(isinstance(item, str) and _day(item) >= date(2005, 1, 1) for item in days)
                and days == sorted(set(days)))
    except (OSError, ValueError, TypeError):
        return False
