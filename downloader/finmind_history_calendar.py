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


CONTRACT_VERSION = 3  # Independently scoped calendars; never apply TW holidays overseas.


@dataclass(frozen=True)
class HistoryClosures:
    days: tuple[str, ...] = ()
    datasets: frozenset[str] = frozenset()
    receipt_sha256: str | None = None
    first: str | None = None
    last: str | None = None
    observed_at_utc: str | None = None
    basis: str = 'receipt_verified_cash_sessions'
    dataset_scopes: tuple[tuple[str, HistoryClosures], ...] = ()

    def scope(self, dataset: str) -> HistoryClosures:
        return next((value for name, value in self.dataset_scopes if name == dataset), self)

    def count(self, dataset: str, first: date, last: date) -> int:
        if dataset not in self.datasets or last < first:
            return 0
        days = self.scope(dataset).days
        return bisect_right(days, last.isoformat()) - bisect_left(days, first.isoformat())

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
    scopes = tuple((dataset, HistoryClosures(tuple(scope['closed_days']), frozenset({dataset}),
                     scope['receipt_sha256'], scope['first'], scope['last'], value['observed_at_utc'], scope['basis']))
                   for dataset, scope in sorted(value.get('dataset_scopes', {}).items()))
    return HistoryClosures(tuple(value['closed_days']), frozenset(value['datasets']),
                           value['receipt_sha256'], value['first'], value['last'], value['observed_at_utc'],
                           value.get('basis', 'receipt_verified_cash_sessions'), scopes)


def arrival_density(closures: HistoryClosures, dataset: str, now: datetime) -> dict[str, Any]:
    """A labeled future-load model from a full verified trailing cash year.

    Never a future calendar/exclusion rule. Missing, conflicted or stale proof
    retains the conservative calendar-day model; overseas/night dates do not
    inherit this density. Compact counts cost O(log closed days), no file scan.
    """
    from downloader.finmind_scheduling import TAIPEI
    fallback = {'state': 'calendar_day_upper_model', 'factor': 1.0,
                'basis': 'unverified_or_non_cash_calendar_not_assumed_closed'}
    closures = closures.scope(dataset)
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
    published = closures.basis == 'official_published_us_cash_calendar'
    return {'state': ('modeled_from_published_cash_calendar_year' if published else 'modeled_from_verified_cash_year'),
            'factor': sessions / 365,
            'basis': ('published_schedule_not_observed_provider_sessions' if published else
                      'trailing_actual_sessions_not_exact_future_holidays'),
            'first_date': start.isoformat(), 'last_date': end.isoformat(),
            'observed_sessions': sessions, 'calendar_days': 365,
            'receipt_sha256': closures.receipt_sha256}


def reconcile_closures(connection: sqlite3.Connection, sources: Mapping[str, Any],
                       official_sessions: Any, now: datetime, *, day_decision=None,
                       additional_calendars: Mapping[str, HistoryClosures] | None = None) -> HistoryClosures:
    """Called only by the canonical worker under its existing writer lock.

    The proof applies inside its verified bounds only. Existing nonempty
    conflicts disable pruning for the entire dataset, just as Sponsor does.
    Unknown proof never becomes an empty observation or a completed task.
    """
    previous = load_closures(connection)
    from downloader.finmind_storage_objects import CASH_OBJECTS
    datasets = frozenset(name for name, source in sources.items()
                         if source.grain == 'day' and (source.universe in {'stocks', 'brokers'}
                         or (source.endpoint == 'storage_objects' and name in CASH_OBJECTS)))
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
    additional = {name: scope for name, scope in (additional_calendars or {}).items() if name in sources}
    for name, scope in additional.items():
        # Explicit market/date-grain boundary, not a generic weekend heuristic.
        if (sources[name].universe != 'us' or sources[name].grain != 'day'
                or scope.datasets != frozenset({name}) or scope.dataset_scopes
                or tuple(sorted(set(scope.days))) != scope.days or not scope.receipt_sha256
                or not scope.first or not scope.last or scope.first > scope.last
                or any(not scope.first <= day <= scope.last for day in scope.days)):
            raise ValueError('invalid_additional_cash_calendar_scope')
    connection.execute('CREATE TEMP TABLE IF NOT EXISTS finmind_history_closed_days (day TEXT PRIMARY KEY)')
    conflicts: set[str] = set()
    active = set()
    scoped_metadata = {}
    loaded_days = None
    for dataset in sorted(datasets | previous.datasets | additional.keys()):
        scope = additional.get(dataset)
        current_days = scope.days if scope else closed if dataset in datasets else ()
        if current_days != loaded_days:
            connection.execute('DELETE FROM finmind_history_closed_days')
            connection.executemany('INSERT INTO finmind_history_closed_days VALUES (?)',
                                   ((day,) for day in current_days))
            loaded_days = current_days
        conflict = connection.execute(
            'SELECT 1 FROM tasks WHERE dataset=? AND rows>0 '
            'AND partition IN (SELECT day FROM finmind_history_closed_days) LIMIT 1', (dataset,)).fetchone()
        if conflict:
            conflicts.add(dataset)
        elif current_days:
            active.add(dataset)
            if scope:
                scoped_metadata[dataset] = {'closed_days': scope.days, 'first': scope.first, 'last': scope.last,
                                           'receipt_sha256': scope.receipt_sha256, 'basis': scope.basis}
        previous_days = previous.scope(dataset).days if dataset in previous.datasets else ()
        removed = (set(previous_days) - set(current_days) if dataset in active else set(previous_days))
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
                "AND state IN ('pending','failed','observed_empty') AND partition IN (SELECT day FROM finmind_history_closed_days)",
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
    metadata['dataset_scopes'] = scoped_metadata
    # Observed-at changes are heartbeats, not another semantic calendar version.
    semantic = {key: value for key, value in metadata.items() if key != 'observed_at_utc'}
    prior_semantic = {}
    if connection.execute('SELECT 1 FROM finmind_history_calendar WHERE singleton=1').fetchone():
        prior_value = json.loads(connection.execute(
            'SELECT metadata_json FROM finmind_history_calendar WHERE singleton=1').fetchone()[0])
        prior_semantic = {key: value for key, value in prior_value.items() if key != 'observed_at_utc'}
    body = json.dumps(metadata, sort_keys=True, separators=(',', ':'))
    digest = hashlib.sha256(body.encode()).hexdigest()
    same_proof = json.dumps(prior_semantic, sort_keys=True) == json.dumps(semantic, sort_keys=True)
    if not same_proof:
        connection.execute('INSERT OR IGNORE INTO finmind_history_calendar_versions VALUES (?,?)', (digest, body))
    connection.execute('INSERT OR REPLACE INTO finmind_history_calendar VALUES (1,?,?)', (body, digest))
    return load_closures(connection)
