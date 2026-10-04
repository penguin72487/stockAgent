"""Compact, reversible session exclusions for Complement's lazy history.

The existing official-session loader owns byte/receipt verification. Only cash
stocks/brokers use that proof: a futures tick timestamp is not a cash trading
date. One small closure set replaces millions of ID x closed-day queue rows.
Loss/revision of proof rewinds affected cursors, while materialized tasks and
raw receipts remain intact. Frontier counting subtracts those retained tasks.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import hashlib
import json
import sqlite3
from typing import Any, Mapping


CONTRACT_VERSION = 2  # Verified scheduled tail, without guessing beyond actual sessions.


@dataclass(frozen=True)
class HistoryClosures:
    days: tuple[str, ...] = ()
    datasets: frozenset[str] = frozenset()
    receipt_sha256: str | None = None
    first: str | None = None
    last: str | None = None
    observed_at_utc: str | None = None

    def count(self, dataset: str, first: date, last: date) -> int:
        if dataset not in self.datasets or last < first:
            return 0
        return bisect_right(self.days, last.isoformat()) - bisect_left(self.days, first.isoformat())

    def advance(self, dataset: str, day: date, *, direction: int) -> date:
        if direction not in {-1, 1}:
            raise ValueError('invalid_history_direction')
        if dataset in self.datasets:
            while self.count(dataset, day, day):
                day += timedelta(days=direction)
        return day


def load_closures(connection: sqlite3.Connection) -> HistoryClosures:
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE name='finmind_history_calendar'").fetchone():
        return HistoryClosures()
    row = connection.execute('SELECT metadata_json,sha256 FROM finmind_history_calendar WHERE singleton=1').fetchone()
    if row is None:
        return HistoryClosures()
    body, digest = row
    if hashlib.sha256(body.encode()).hexdigest() != digest:
        raise ValueError('history_calendar_metadata_integrity')
    value = json.loads(body)
    return HistoryClosures(tuple(value['closed_days']), frozenset(value['datasets']),
                           value['receipt_sha256'], value['first'], value['last'], value['observed_at_utc'])


def arrival_density(closures: HistoryClosures, dataset: str, now: datetime) -> dict[str, Any]:
    """A labeled future-load model from a full verified trailing cash year.

    Never a future calendar/exclusion rule. Missing, conflicted or stale proof
    retains the conservative calendar-day model; overseas/night dates do not
    inherit this density. Compact counts cost O(log closed days), no file scan.
    """
    from downloader.finmind_scheduling import TAIPEI
    fallback = {'state': 'calendar_day_upper_model', 'factor': 1.0,
                'basis': 'unverified_or_non_cash_calendar_not_assumed_closed'}
    if dataset not in closures.datasets or not closures.first or not closures.last:
        return fallback
    first, last = date.fromisoformat(closures.first), date.fromisoformat(closures.last)
    today = now.astimezone(TAIPEI).date()
    end = min(last, today - timedelta(days=1))
    start = end - timedelta(days=364)
    if start < first or not timedelta(0) <= today - end <= timedelta(days=31):
        return fallback
    sessions = 365 - closures.count(dataset, start, end)
    if not 0 < sessions <= 365:
        return fallback
    return {'state': 'modeled_from_verified_cash_year', 'factor': sessions / 365,
            'basis': 'trailing_actual_sessions_not_exact_future_holidays',
            'first_date': start.isoformat(), 'last_date': end.isoformat(),
            'observed_sessions': sessions, 'calendar_days': 365,
            'receipt_sha256': closures.receipt_sha256}


def reconcile_closures(connection: sqlite3.Connection, sources: Mapping[str, Any],
                       official_sessions: Any, now: datetime, *, day_decision=None) -> HistoryClosures:
    """Called only by the canonical worker under its existing writer lock.

    The proof applies inside its verified bounds only. Existing nonempty
    conflicts disable pruning for the entire dataset, just as Sponsor does.
    Unknown proof never becomes an empty observation or a completed task.
    """
    previous = load_closures(connection)
    datasets = frozenset(name for name, source in sources.items()
                         if source.grain == 'day' and source.universe in {'stocks', 'brokers'})
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_history_calendar ('
                       'singleton INTEGER PRIMARY KEY CHECK(singleton=1),metadata_json TEXT NOT NULL,sha256 TEXT NOT NULL)')
    connection.execute('CREATE TABLE IF NOT EXISTS finmind_history_calendar_versions ('
                       'sha256 TEXT PRIMARY KEY,metadata_json TEXT NOT NULL)')
    closed: tuple[str, ...] = ()
    first = last = receipt = None
    if official_sessions is not None:
        first, last = official_sessions.first, official_sessions.last
        sessions = official_sessions.days
        if first > last or not sessions or min(sessions) < first or max(sessions) > last:
            raise ValueError('invalid_official_history_session_bounds')
        closed = tuple((first + timedelta(days=offset)).isoformat()
                       for offset in range((last - first).days + 1)
                       if first + timedelta(days=offset) not in sessions)
        receipt = official_sessions.receipt_sha256
        if day_decision is not None:
            from downloader.finmind_scheduling import TAIPEI
            today = now.astimezone(TAIPEI).date()
            # A stale/missing historical archive is not a license to scan or
            # guess years of sessions. The tail is bounded and independently
            # proven by the current official schedule, including Saturday opens.
            tail = []
            for offset in range(1, min(366, max(0, (today - last).days)) + 1):
                day = last + timedelta(days=offset)
                decision = day_decision(day)
                if decision.status == 'closed' and 'official TWSE' in decision.reason:
                    tail.append(day.isoformat())
            closed += tuple(tail)
    connection.execute('CREATE TEMP TABLE IF NOT EXISTS finmind_history_closed_days (day TEXT PRIMARY KEY)')
    connection.execute('DELETE FROM finmind_history_closed_days')
    connection.executemany('INSERT INTO finmind_history_closed_days VALUES (?)', ((day,) for day in closed))
    conflicts: set[str] = set()
    if datasets and closed:
        placeholders = ','.join('?' for _ in datasets)
        conflicts = {row[0] for row in connection.execute(
            f'SELECT DISTINCT dataset FROM tasks WHERE dataset IN ({placeholders}) AND rows>0 '
            'AND partition IN (SELECT day FROM finmind_history_closed_days)', tuple(sorted(datasets)))}
    active = datasets - conflicts if closed else frozenset()
    new_closed = frozenset(closed)
    for dataset in sorted(datasets | previous.datasets):
        removed = (set(previous.days) - new_closed if dataset in active else set(previous.days))
        if dataset in previous.datasets and removed:
            # Rewalk only when a formerly excluded date lost its proof. INSERT
            # OR IGNORE preserves all completed tasks; counting removes overlap.
            replay_from = max(removed)
            source = sources.get(dataset)
            if source is not None:
                # Never rewind beyond the forward cursor: later lost closures
                # already belong to forward work, not a second history range.
                connection.execute(
                    'UPDATE finmind_source_frontiers SET older_than=MIN(?,newer_than) WHERE dataset=? '
                    'AND newer_than>=? AND (older_than IS NULL OR older_than<MIN(?,newer_than)) '
                    "AND (MIN(?,newer_than)>=? OR (dataset='TaiwanStockKBar' AND data_id='TAIEX' "
                    "AND MIN(?,newer_than)>='2005-01-03'))",
                    (replay_from, dataset, min(removed), replay_from, replay_from,
                     source.first.isoformat(), replay_from))
        if dataset in active:
            connection.execute(
                "UPDATE tasks SET state='non_session',next_attempt_at_utc=NULL WHERE dataset=? AND rows=0 "
                "AND state IN ('pending','failed') AND partition IN (SELECT day FROM finmind_history_closed_days)",
                (dataset,))
            connection.execute(
                "UPDATE tasks SET state='pending',next_attempt_at_utc=NULL WHERE dataset=? AND state='non_session' "
                'AND partition NOT IN (SELECT day FROM finmind_history_closed_days)', (dataset,))
        else:
            connection.execute("UPDATE tasks SET state='pending',next_attempt_at_utc=NULL WHERE dataset=? AND state='non_session'",
                               (dataset,))
    metadata = {'contract_version': CONTRACT_VERSION, 'datasets': sorted(active), 'closed_days': closed,
                'first': first.isoformat() if first else None, 'last': last.isoformat() if last else None,
                'receipt_sha256': receipt, 'observed_at_utc': now.isoformat(),
                'conflicting_datasets': sorted(conflicts),
                'basis': 'receipt_verified_cash_sessions_and_official_scheduled_tail_not_derivative_dates'}
    body = json.dumps(metadata, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(body.encode()).hexdigest()
    # Archive a proof only when its semantic scope changes, not every heartbeat.
    same_proof = (previous.days == closed and previous.datasets == active and previous.receipt_sha256 == receipt
                  and previous.first == metadata['first'] and previous.last == metadata['last'])
    if not same_proof:
        connection.execute('INSERT OR IGNORE INTO finmind_history_calendar_versions VALUES (?,?)', (digest, body))
    connection.execute('INSERT OR REPLACE INTO finmind_history_calendar VALUES (1,?,?)', (body, digest))
    return load_closures(connection)
