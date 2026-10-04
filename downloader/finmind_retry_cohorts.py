"""Coalesce retryable empty-history failures within the bounded retry policy.

Request admission is not a source-unavailability verdict. Only validated
overseas full histories unexpectedly losing all known rows qualify. Rotating
minute probes reopen the ordinary repair lane; state survives owner restarts.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any

from downloader.finmind_history_refresh import DAILY_EQUITY

CONTRACT_VERSION = 2
ERROR = 'unexpected_empty_after_nonempty'
DISTINCT_FAILURES = 3
EVIDENCE_SECONDS = 900
PROBE_SECONDS = 60


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_empty_history_failures ('
                 'dataset TEXT,data_id TEXT,failed_at_utc TEXT NOT NULL,'
                 'PRIMARY KEY(dataset,data_id))')
    conn.execute('CREATE TABLE IF NOT EXISTS finmind_retry_cohorts ('
                 'dataset TEXT PRIMARY KEY,active INTEGER NOT NULL,canary_id TEXT,'
                 'next_probe_at_utc TEXT,opened_at_utc TEXT,last_probe_at_utc TEXT,'
                 'recovered_at_utc TEXT,probe_failures INTEGER NOT NULL DEFAULT 0)')


def _exists(conn: sqlite3.Connection) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_retry_cohorts'").fetchone())


def admission_clause(conn: sqlite3.Connection, cutoff: datetime, *, alias: str = 'tasks') -> tuple[str, tuple]:
    """Parameterized filter shared by dispatch and quota reservations.

    No schema writes when reading an old queue. Unrelated failures, new tasks
    and accepted history remain admissible. Future windows count only a canary,
    not every deferred copy of its error.
    """
    if alias not in {'tasks', 't'}:
        raise ValueError('unsupported retry-cohort SQL alias')
    if not _exists(conn):
        return '', ()
    rows = conn.execute('SELECT dataset,canary_id,next_probe_at_utc FROM finmind_retry_cohorts '
                        'WHERE active=1 ORDER BY dataset').fetchall()
    clauses, args = [], []
    for dataset, identifier, due in rows:
        if dataset not in DAILY_EQUITY:
            continue
        if not identifier or not due:
            raise ValueError('invalid_retry_cohort')
        clauses.append(f" AND NOT ({alias}.dataset=? AND {alias}.kind='id_history' "
                       f"AND {alias}.state='failed' AND COALESCE({alias}.error_code,'')=? AND {alias}.rows>0 "
                       f"AND ({alias}.data_id!=? OR ?>?))")
        args.extend((dataset, ERROR, identifier, due, cutoff.astimezone(UTC).isoformat()))
    return ''.join(clauses), tuple(args)


def record_failure(conn: sqlite3.Connection, task: Any, code: str, now: datetime) -> bool:
    """After a real failure is saved, in the same owner queue transaction."""
    if task.dataset not in DAILY_EQUITY or task.kind != 'id_history':
        return False
    if code != ERROR:
        _retain_probe_after_other_error(conn, task, now)
        return False
    row = conn.execute('SELECT rows,state FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                       (task.dataset, task.data_id, task.partition)).fetchone()
    if not row or row[0] <= 0:
        return False
    if row[1] != 'failed':
        _retain_probe_after_other_error(conn, task, now)
        return False
    ensure_schema(conn)
    stamp = now.astimezone(UTC).isoformat()
    conn.execute('INSERT INTO finmind_empty_history_failures VALUES (?,?,?) '
                 'ON CONFLICT(dataset,data_id) DO UPDATE SET failed_at_utc=excluded.failed_at_utc',
                 (task.dataset, task.data_id, stamp))
    active = conn.execute('SELECT active FROM finmind_retry_cohorts WHERE dataset=?',
                          (task.dataset,)).fetchone()
    recent = conn.execute('SELECT count(*) FROM finmind_empty_history_failures '
                          'WHERE dataset=? AND failed_at_utc BETWEEN ? AND ?',
                          (task.dataset, (now - timedelta(seconds=EVIDENCE_SECONDS)).isoformat(), stamp)).fetchone()[0]
    if not (active and active[0]) and recent < DISTINCT_FAILURES:
        return False
    # Probe retained failed keys, not a healthy ticker: partial source recovery
    # stays discoverable even when an individual symbol remains broken.
    identifiers = [row[0] for row in conn.execute(
        "SELECT data_id FROM tasks WHERE dataset=? AND kind='id_history' "
        "AND state='failed' AND error_code=? AND rows>0 ORDER BY data_id", (task.dataset, ERROR))]
    if not identifiers:
        return False
    canary = next((value for value in identifiers if value > task.data_id), identifiers[0])
    due = (now + timedelta(seconds=PROBE_SECONDS)).astimezone(UTC).isoformat()
    conn.execute('INSERT INTO finmind_retry_cohorts '
                 '(dataset,active,canary_id,next_probe_at_utc,opened_at_utc,last_probe_at_utc,probe_failures) '
                 'VALUES (?,1,?,?,?,?,1) ON CONFLICT(dataset) DO UPDATE SET active=1,'
                 'canary_id=excluded.canary_id,next_probe_at_utc=excluded.next_probe_at_utc,'
                 'opened_at_utc=CASE WHEN active=1 THEN opened_at_utc ELSE excluded.opened_at_utc END,'
                 'last_probe_at_utc=excluded.last_probe_at_utc,recovered_at_utc=NULL,'
                 'probe_failures=CASE WHEN active=1 THEN probe_failures+1 ELSE 1 END',
                 (task.dataset, canary, due, stamp, stamp))
    # Only deadlines change. Never manufacture attempts, rows or receipts.
    # ETA retains every failed key as repair work, not deleted work.
    conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND kind='id_history' "
                 "AND state='failed' AND error_code=? AND rows>0", (due, task.dataset, ERROR))
    return True


def reconcile(conn: sqlite3.Connection, now: datetime) -> None:
    """Initialization may exhaust old canaries; no stale gate may strand peers."""
    for dataset, identifier in conn.execute(
            'SELECT dataset,canary_id FROM finmind_retry_cohorts WHERE active=1').fetchall():
        row = conn.execute("SELECT state,next_attempt_at_utc FROM tasks WHERE dataset=? AND data_id=? "
                           "AND kind='id_history'", (dataset, identifier)).fetchone()
        if row and row[0] == 'failed' and row[1]:
            continue
        from types import SimpleNamespace
        _retain_probe_after_other_error(conn, SimpleNamespace(
            dataset=dataset, data_id=identifier, partition='history'), now)


def _retain_probe_after_other_error(conn: sqlite3.Connection, task: Any, now: datetime) -> None:
    """A timeout preserves its cooldown; a terminal canary cannot strand peers."""
    if not _exists(conn):
        return
    gate = conn.execute('SELECT canary_id FROM finmind_retry_cohorts WHERE dataset=? AND active=1',
                        (task.dataset,)).fetchone()
    if not gate or gate[0] != task.data_id:
        return
    row = conn.execute('SELECT state,next_attempt_at_utc FROM tasks '
                              'WHERE dataset=? AND data_id=? AND partition=?',
                              (task.dataset, task.data_id, task.partition)).fetchone()
    state, due = row if row else (None, None)
    if state == 'failed' and due:
        canary = task.data_id
    else:
        ids = [row[0] for row in conn.execute(
            "SELECT data_id FROM tasks WHERE dataset=? AND state='failed' AND kind='id_history' "
            "AND error_code=? AND rows>0 ORDER BY data_id", (task.dataset, ERROR))]
        if not ids:
            conn.execute('UPDATE finmind_retry_cohorts SET active=0,canary_id=NULL,next_probe_at_utc=NULL WHERE dataset=?',
                         (task.dataset,))
            return
        canary = next((value for value in ids if value > task.data_id), ids[0])
        due = (now + timedelta(seconds=PROBE_SECONDS)).astimezone(UTC).isoformat()
    conn.execute('UPDATE finmind_retry_cohorts SET canary_id=?,next_probe_at_utc=? WHERE dataset=?',
                 (canary, due, task.dataset))
    conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND state='failed' "
                 "AND kind='id_history' AND error_code=? AND rows>0", (due, task.dataset, ERROR))


def full_probe_required(conn: sqlite3.Connection, task: Any) -> bool:
    """A failed full history cannot recover by merely retaining an empty tail."""
    if task.dataset not in DAILY_EQUITY or task.kind != 'id_history':
        return False
    row = conn.execute('SELECT error_code FROM tasks WHERE dataset=? AND data_id=? AND partition=?',
                       (task.dataset, task.data_id, task.partition)).fetchone()
    if task.state == 'failed' and row and row[0] == ERROR:
        return True
    if not _exists(conn):
        return False
    return bool(conn.execute('SELECT 1 FROM finmind_retry_cohorts WHERE dataset=? AND active=1 AND canary_id=?',
                             (task.dataset, task.data_id)).fetchone())


def record_recovery(conn: sqlite3.Connection, task: Any, rows: int, now: datetime) -> bool:
    if rows <= 0 or task.dataset not in DAILY_EQUITY or task.kind != 'id_history' or not _exists(conn):
        return False
    gate = conn.execute('SELECT canary_id FROM finmind_retry_cohorts WHERE dataset=? AND active=1',
                        (task.dataset,)).fetchone()
    if not gate or gate[0] != task.data_id:
        return False
    stamp = now.astimezone(UTC).isoformat()
    conn.execute('UPDATE finmind_retry_cohorts SET active=0,recovered_at_utc=? WHERE dataset=?',
                 (stamp, task.dataset))
    conn.execute('DELETE FROM finmind_empty_history_failures WHERE dataset=?', (task.dataset,))
    conn.execute("UPDATE tasks SET next_attempt_at_utc=? WHERE dataset=? AND kind='id_history' "
                 "AND state='failed' AND error_code=? AND rows>0", (stamp, task.dataset, ERROR))
    return True


def summary(conn: sqlite3.Connection) -> list[dict]:
    if not _exists(conn):
        return []
    return [{'dataset': dataset, 'active': bool(active), 'probe_interval_seconds': PROBE_SECONDS,
             'next_probe_at_utc': due, 'opened_at_utc': opened, 'last_probe_at_utc': attempted,
             'recovered_at_utc': recovered, 'failed_probes': failures, 'contract_version': CONTRACT_VERSION,
             'source_unavailable_proven': False}
            for dataset, active, due, opened, attempted, recovered, failures in conn.execute(
                'SELECT dataset,active,next_probe_at_utc,opened_at_utc,last_probe_at_utc,'
                'recovered_at_utc,probe_failures FROM finmind_retry_cohorts ORDER BY dataset')]
