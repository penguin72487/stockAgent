"""Bounded owner-queue retries, separate from data completeness and API quota.

Only task failures count. Quota/IP cooldowns, local traffic-accounting holds
and derived dependency waits do not. Exhaustion preserves the task's data and last actual attempt;
normal seeding, process restarts and another ticker's recovery cannot reopen it.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime
from functools import lru_cache
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any

CONTRACT_VERSION = 1
EXHAUSTED = 'retry_exhausted'
COOLDOWNS = frozenset({'rate_limited', 'ip_banned', 'invalid_token', 'not_entitled',
                       'local_traffic_busy', 'local_traffic_unavailable'})
CONFIG = Path(__file__).resolve().parents[1] / 'configs/finmind_retry_policy.json'


@lru_cache(maxsize=1)
def failure_limit() -> int:
    """Process-level policy; a configuration change takes effect on restart."""
    value = json.loads(CONFIG.read_bytes())
    limit = value.get('max_consecutive_failures')
    if value.get('schema_version') != 1 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError('invalid_finmind_retry_policy')
    return limit


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_task_retries ('
                 'dataset TEXT,data_id TEXT,partition TEXT,cycle INTEGER NOT NULL DEFAULT 1,'
                 'failures INTEGER NOT NULL DEFAULT 0,first_failure_at_utc TEXT,last_failure_at_utc TEXT,'
                 'error_code TEXT,exhausted_at_utc TEXT,PRIMARY KEY(dataset,data_id,partition))')
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_retry_events ('
                 'dataset TEXT,data_id TEXT,partition TEXT,cycle INTEGER,event TEXT,at_utc TEXT,'
                 'metadata_json TEXT NOT NULL,PRIMARY KEY(dataset,data_id,partition,cycle,event,at_utc))')
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_retry_policy_migrations ('
                 'version INTEGER PRIMARY KEY,at_utc TEXT NOT NULL)')


def _eligible(kind: str, code: str | None) -> bool:
    return kind != 'derived' and bool(code) and code not in COOLDOWNS


def _exhaust(conn: sqlite3.Connection, key: tuple[str, str, str], now: datetime) -> None:
    stamp = now.astimezone(UTC).isoformat()
    cursor = conn.execute('SELECT * FROM tasks WHERE dataset=? AND data_id=? AND partition=?', key)
    previous = dict(zip((col[0] for col in cursor.description), cursor.fetchone()))
    cycle, failures = conn.execute('SELECT cycle,failures FROM finmind_task_retries '
                                  'WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
    conn.execute('INSERT OR IGNORE INTO finmind_retry_events VALUES (?,?,?,?,?,?,?)',
                 (*key, cycle, EXHAUSTED, stamp, json.dumps({
                     'contract_version': CONTRACT_VERSION, 'max_consecutive_failures': failure_limit(),
                     'failures': failures, 'prior_task': previous,
                     'source_unavailable_proven': False}, sort_keys=True)))
    conn.execute('UPDATE finmind_task_retries SET exhausted_at_utc=? '
                 'WHERE dataset=? AND data_id=? AND partition=?', (stamp, *key))
    # Never alter rows, dates, paths, source error or last_attempt_at_utc.
    conn.execute('UPDATE tasks SET state=?,next_attempt_at_utc=NULL '
                 'WHERE dataset=? AND data_id=? AND partition=?', (EXHAUSTED, *key))


def migrate(conn: sqlite3.Connection, root: Path, now: datetime) -> None:
    """One owner-locked migration from actual per-key checks, not cohort totals.

    One indexed chronological stream per affected dataset, O(checks + keys),
    avoiding a correlated all-history scan for every failed row. Unmonitored
    keys have only one proven failure in their queue; do not invent earlier ones.
    """
    ensure_schema(conn)
    if conn.execute('SELECT 1 FROM finmind_retry_policy_migrations WHERE version=?',
                    (CONTRACT_VERSION,)).fetchone():
        return
    tasks = {tuple(row[:3]): row for row in conn.execute(
        "SELECT dataset,data_id,partition,kind,error_code,last_attempt_at_utc FROM tasks WHERE state='failed'")
        if _eligible(row[3], row[4]) and row[5]}
    counts: dict[tuple, tuple[int, str | None, str | None]] = {}
    observation = root / 'update_observations.sqlite3'
    if tasks and observation.is_file():
        try:
            with closing(sqlite3.connect(observation.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as source:
                for dataset in sorted({key[0] for key in tasks}):
                    for identifier, partition, stamp, kind, body in source.execute(
                        'SELECT data_id,partition,checked_at_utc,kind,metadata_json FROM checks '
                        'WHERE dataset=? ORDER BY checked_at_utc,id', (dataset,)):
                        key = (dataset, identifier, partition)
                        if key not in tasks or stamp > tasks[key][5]:
                            continue
                        if kind != 'failed':
                            counts[key] = (0, None, stamp)
                        elif _eligible(tasks[key][3], json.loads(body).get('error_code')):
                            n, first, _ = counts.get(key, (0, None, None))
                            counts[key] = (n + 1, first or stamp, stamp)
        except (sqlite3.Error, OSError, ValueError, TypeError, AttributeError) as error:
            # Incomplete chronological evidence could miss a later success.
            # Discard partial counts instead of incorrectly retiring histories.
            # A bad sidecar must not prevent the healthy owner queue progressing.
            counts.clear()
            logging.getLogger(__name__).warning('finmind_retry_legacy_observation_unavailable error_type=%s',
                                               type(error).__name__)
    for key, row in tasks.items():
        n, first, last = counts.get(key, (1, row[5], row[5]))
        # A queue failure committed after/missing from the observations is still
        # evidence of one attempt. A successful observation cannot erase it.
        if last != row[5]:
            n, first, last = n + 1, first or row[5], row[5]
        conn.execute('INSERT OR IGNORE INTO finmind_task_retries '
                     '(dataset,data_id,partition,failures,first_failure_at_utc,last_failure_at_utc,error_code) '
                     'VALUES (?,?,?,?,?,?,?)', (*key, n, first, last, row[4]))
        conn.execute('INSERT OR IGNORE INTO finmind_retry_events VALUES (?,?,?,1,?,?,?)',
                     (*key, 'legacy_import', now.astimezone(UTC).isoformat(), json.dumps({
                         'failures': n, 'basis': 'per_key_observations' if key in counts else 'queue_proven_last_failure_only'})))
        if n >= failure_limit():
            _exhaust(conn, key, now)
    conn.execute('INSERT INTO finmind_retry_policy_migrations VALUES (?,?)',
                 (CONTRACT_VERSION, now.astimezone(UTC).isoformat()))


def record_failure(conn: sqlite3.Connection, task: Any, code: str, now: datetime) -> bool:
    """Same transaction as the saved queue failure; idempotent per attempt clock."""
    if not _eligible(task.kind, code):
        return False
    key = (task.dataset, task.data_id, task.partition)
    state = conn.execute('SELECT state FROM tasks WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
    if not state or state[0] != 'failed':
        return False
    stamp = now.astimezone(UTC).isoformat()
    conn.execute('INSERT INTO finmind_task_retries '
                 '(dataset,data_id,partition,failures,first_failure_at_utc,last_failure_at_utc,error_code) '
                 'VALUES (?,?,?,1,?,?,?) ON CONFLICT(dataset,data_id,partition) DO UPDATE SET '
                 'failures=failures+1,first_failure_at_utc=coalesce(first_failure_at_utc,excluded.first_failure_at_utc),'
                 'last_failure_at_utc=excluded.last_failure_at_utc,error_code=excluded.error_code '
                 'WHERE exhausted_at_utc IS NULL AND '
                 '(last_failure_at_utc IS NULL OR last_failure_at_utc<excluded.last_failure_at_utc)',
                 (*key, stamp, stamp, code))
    failures, exhausted = conn.execute('SELECT failures,exhausted_at_utc FROM finmind_task_retries '
                                      'WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
    if failures >= failure_limit() or exhausted:
        _exhaust(conn, key, now)
        return True
    return False


def record_success(conn: sqlite3.Connection, task: Any, now: datetime) -> None:
    key = (task.dataset, task.data_id, task.partition)
    row = conn.execute('SELECT cycle,failures,exhausted_at_utc FROM finmind_task_retries '
                       'WHERE dataset=? AND data_id=? AND partition=?', key).fetchone()
    if not row or not row[1]:
        return
    if row[2]:
        raise ValueError('exhausted_task_requires_explicit_reopen')
    conn.execute('INSERT OR IGNORE INTO finmind_retry_events VALUES (?,?,?,?,?,?,?)',
                 (*key, row[0], 'recovered', now.astimezone(UTC).isoformat(), json.dumps({'failures': row[1]})))
    conn.execute('UPDATE finmind_task_retries SET cycle=cycle+1,failures=0,first_failure_at_utc=NULL,'
                 'last_failure_at_utc=NULL,error_code=NULL WHERE dataset=? AND data_id=? AND partition=?', key)


def reopen(conn: sqlite3.Connection, datasets: tuple[str, ...], now: datetime) -> int:
    """Explicit operator action only; retained evidence survives the new cycle."""
    if not datasets:
        raise ValueError('retry_reopen_requires_dataset_scope')
    stamp, changed = now.astimezone(UTC).isoformat(), 0
    for dataset in sorted(set(datasets)):
        for identifier, partition, cycle, failures in conn.execute(
            'SELECT t.data_id,t.partition,r.cycle,r.failures FROM tasks t JOIN finmind_task_retries r '
            'USING(dataset,data_id,partition) WHERE t.dataset=? AND t.state=?', (dataset, EXHAUSTED)).fetchall():
            key = (dataset, identifier, partition)
            conn.execute('INSERT INTO finmind_retry_events VALUES (?,?,?,?,?,?,?)',
                         (*key, cycle, 'operator_reopened', stamp, json.dumps({'prior_failures': failures})))
            conn.execute('UPDATE finmind_task_retries SET cycle=cycle+1,failures=0,first_failure_at_utc=NULL,'
                         'last_failure_at_utc=NULL,exhausted_at_utc=NULL WHERE dataset=? AND data_id=? AND partition=?', key)
            # Retain the source error so the canonical worker forces a full
            # refetch, never an empty tail falsely repairing old nonempty data.
            conn.execute("UPDATE tasks SET state='failed',next_attempt_at_utc=? "
                         'WHERE dataset=? AND data_id=? AND partition=?', (stamp, *key))
            changed += 1
    return changed


def summary(conn: sqlite3.Connection) -> dict:
    return {'contract_version': CONTRACT_VERSION, 'max_consecutive_failures': failure_limit(),
            'reset_requires_explicit_action': True, 'source_unavailable_proven': False,
            'datasets': [{'dataset': ds, 'exhausted_tasks': count, 'min_failures': low, 'max_failures': high,
                          'first_exhausted_at_utc': first, 'last_exhausted_at_utc': last}
                         for ds, count, low, high, first, last in conn.execute(
                             'SELECT dataset,count(*),min(failures),max(failures),min(exhausted_at_utc),'
                             'max(exhausted_at_utc) FROM finmind_task_retries '
                             'WHERE exhausted_at_utc IS NOT NULL GROUP BY dataset ORDER BY dataset')]}
