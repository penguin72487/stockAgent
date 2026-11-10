"""Finite per-product source checks distinct from whole-market acquisition.

Only an explicit, content-pinned plan admits these existing keys. The normal
Complement owner still controls locks, credentials, quota, cooldown and storage;
this registration never reopens a completed, empty or in-flight task.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import sqlite3


CONTRACT = 'finmind-finite-product-query-verification-v1'
TABLE = 'finmind_product_query_verifications'
DATASETS = frozenset({'TaiwanFuturesDaily', 'TaiwanOptionDaily'})
FOREIGN_HISTORY_BACKFILLS = ('EuropeStockPrice', 'JapanStockPrice', 'UKStockPrice', 'USStockPrice')


def _read(path: Path) -> tuple[dict, str]:
    if path.is_symlink() or any(p.is_symlink() for p in path.parents):
        raise ValueError('redirected_product_query_plan')
    with path.open('rb') as stream:
        raw = stream.read()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def _keys(plan: dict) -> tuple[tuple[str, str, str], ...]:
    if (not isinstance(plan, dict) or plan.get('contract') != CONTRACT
            or plan.get('query_shape') != 'per_id_full_history_verification'
            or not isinstance(plan.get('authorization'), str) or not plan['authorization'].strip()
            or not isinstance(plan.get('reason'), str) or not plan['reason'].strip()
            or plan.get('source_only_no_training_injection') is not True):
        raise ValueError('explicit_product_query_verification_plan_required')
    rows = plan.get('keys')
    if not isinstance(rows, list) or not rows:
        raise ValueError('product_query_requires_existing_keys')
    keys = []
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {'dataset', 'data_id', 'partition'}
                or row['dataset'] not in DATASETS or row['partition'] != 'history'
                or not isinstance(row['data_id'], str) or not re.fullmatch(r'[A-Z0-9]{2,12}', row['data_id'])):
            raise ValueError('unsupported_product_query_verification_key')
        keys.append((row['dataset'], row['data_id'], row['partition']))
    return tuple(dict.fromkeys(keys))


def register(connection: sqlite3.Connection, plan_path: Path, now: datetime) -> dict:
    """Caller owns the canonical queue transaction; task rows stay untouched."""
    plan_path = plan_path.absolute()
    plan, digest = _read(plan_path)
    keys = _keys(plan)
    evidence = plan.get('source_evidence')
    if not isinstance(evidence, list) or not evidence:
        raise ValueError('source_evidence_required_for_product_query')
    for row in evidence:
        if not isinstance(row, dict) or set(row) != {'path', 'sha256'}:
            raise ValueError('invalid_product_query_source_pin')
        path = Path(row['path'])
        if not path.is_absolute() or hashlib.sha256(path.read_bytes()).hexdigest() != row['sha256']:
            raise ValueError('product_query_source_evidence_changed')
    for key in keys:
        row = connection.execute('SELECT kind FROM tasks WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
        if row is None or row[0] != 'id_history':
            raise ValueError('product_query_verification_requires_existing_history_task')
    connection.execute(f'CREATE TABLE IF NOT EXISTS {TABLE} ('
        'dataset TEXT,data_id TEXT,partition TEXT,plan_path TEXT,plan_sha256 TEXT,registered_at_utc TEXT,'
        'PRIMARY KEY(dataset,data_id,partition))')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_priority_tasks ('
        'dataset TEXT,data_id TEXT,partition TEXT,PRIMARY KEY(dataset,data_id,partition))')
    for key in keys:
        prior = connection.execute(f'SELECT plan_path,plan_sha256 FROM {TABLE} '
            'WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
        if prior is not None and tuple(prior) != (str(plan_path), digest):
            raise ValueError('preserve_previous_product_query_registration')
    for key in keys:
        connection.execute(f'INSERT OR IGNORE INTO {TABLE} VALUES (?,?,?,?,?,?)',
            (*key, str(plan_path), digest, now.isoformat()))
        connection.execute('INSERT OR IGNORE INTO finmind_priority_tasks(dataset,data_id,partition) VALUES (?,?,?)', key)
    return dict(contract=CONTRACT, plan_path=str(plan_path), plan_sha256=digest, registered_keys=len(keys),
                task_states_changed=False, provider_calls=0, source_only_no_training_injection=True)


def permits(connection: sqlite3.Connection, key: tuple[str, str, str]) -> bool:
    if key[0] not in DATASETS or not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone():
        return False
    row = connection.execute(f'SELECT plan_path,plan_sha256 FROM {TABLE} '
        'WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
    if row is None:
        return False
    try:
        plan, digest = _read(Path(row[0]))
        return digest == row[1] and key in _keys(plan)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def next_task(connection: sqlite3.Connection, now: datetime, retry_filter: str, retry_args: tuple):
    """Called only after the existing incremental/quota admission has passed."""
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (TABLE,)).fetchone():
        return None
    rows = connection.execute(f'SELECT t.dataset,t.data_id,t.partition,t.kind,t.priority,t.state '
        f'FROM {TABLE} v JOIN tasks t USING(dataset,data_id,partition) '
        'JOIN finmind_priority_tasks p USING(dataset,data_id,partition) '
        "WHERE t.kind='id_history' AND t.state IN ('pending','failed') "
        'AND (t.next_attempt_at_utc IS NULL OR t.next_attempt_at_utc<=?) '
        + retry_filter.replace('tasks.', 't.') + ' ORDER BY t.priority,t.dataset,t.data_id',
        (now.isoformat(), *retry_args))
    for row in rows:
        if permits(connection, tuple(row[:3])):
            return tuple(row)
    return None


def prioritize_source_checks(connection: sqlite3.Connection, plan: dict, now: datetime,
                             delegated: frozenset[str], *, finite_query_ready: bool = False) -> dict:
    """Let a finite requested check precede foreign ID-history backfills.

    Dated releases, future reservations and all provider limits remain
    protected. Pending history and due retries in these four foreign price
    datasets may yield, as requested, without changing their state or retry
    clocks. This is a dispatch view only: original
    task priorities, due clocks and source-plan registrations stay unchanged.
    The caller must still obtain a positive ordinary backfill budget.
    """
    from downloader import finmind_retry_cohorts

    retry_filter, retry_args = finmind_retry_cohorts.admission_clause(connection, now)
    if (plan.get('observed_at_utc') != now.isoformat()
            or not isinstance(plan.get('events'), list) or plan.get('queue_errors')
            or (not finite_query_ready and next_task(connection, now, retry_filter, retry_args) is None)):
        return plan
    datasets = tuple(name for name in FOREIGN_HISTORY_BACKFILLS if name not in delegated)
    counts = {}
    if datasets:
        rows = connection.execute(
            'SELECT dataset,next_attempt_at_utc,count(*) FROM tasks '
            "WHERE priority=0 AND kind='id_history' AND state IN ('pending','failed','complete','observed_empty') "
            'AND next_attempt_at_utc<=? AND dataset IN (' + ','.join('?' for _ in datasets) + ')'
            + retry_filter + ' GROUP BY dataset,next_attempt_at_utc',
            (now.isoformat(), *datasets, *retry_args))
        counts = {(dataset, due): count for dataset, due, count in rows}
    removed, events = 0, []
    for event in plan['events']:
        event = dict(event)
        key = (event.get('dataset'), event.get('due_at_utc'))
        if event.get('owner') == 'complement' and key in counts:
            count = min(event['requests'], counts.pop(key))
            event['requests'] -= count
            removed += count
        events.append(event)
    # Fail closed if the caller supplied a different reservation aggregate.
    if removed > plan.get('ready_requests', -1) or removed > plan.get('reserve_requests', -1):
        return plan
    return dict(plan, events=events,
                ready_requests=plan['ready_requests'] - removed,
                reserve_requests=plan['reserve_requests'] - removed,
                preempted_foreign_history_requests=removed,
                finite_query_priority=finite_query_ready,
                source_verification_priority=True)


def yield_explicitly_paused_owners(plan: dict, now: datetime, paused: frozenset[str]) -> dict:
    """Let a user-requested finite query proceed without restarting paused lanes.

    The complete original quota reservation remains unchanged. Only the
    dispatch precedence of *currently disabled and inactive* owners yields;
    future releases, queue state, retry clocks and provider limits are kept.
    Callers must independently re-observe service and process state.
    """
    if (not paused or plan.get('observed_at_utc') != now.isoformat()
            or not isinstance(plan.get('events'), list) or plan.get('queue_errors')
            or type(plan.get('ready_requests')) is not int
            or type(plan.get('reserve_requests')) is not int
            or not 0 <= plan['ready_requests'] <= plan['reserve_requests']):
        return plan
    total = removed = 0
    try:
        for event in plan['events']:
            count = event['requests']
            due = datetime.fromisoformat(event['due_at_utc'].replace('Z', '+00:00'))
            if type(count) is not int or count < 0 or due.tzinfo is None:
                return plan
            if due <= now:
                total += count
                if event.get('owner') in paused:
                    removed += count
    except (KeyError, TypeError, ValueError):
        return plan
    if total != plan['ready_requests'] or not removed:
        return plan
    return dict(plan, ready_requests=plan['ready_requests'] - removed,
                paused_owner_ready_yielded=removed, paused_owners=sorted(paused),
                quota_reservation_for_paused_owners_preserved=True)
