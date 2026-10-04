"""Cheap idle liveness and deadline-aware waits for the existing FinMind loops.

Worker heartbeats are not acquisition receipts. Waiting never changes status.json,
counts, provider usage or data timestamps, and never makes a provider request.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, time as wall_time, timedelta
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Iterable

from downloader.artifact_io import atomic_write_json
from downloader.finmind_scheduling import TAIPEI, Source, calendar_next_check


def optimize_queue(connection: sqlite3.Connection) -> None:
    """Maintain bounded planner statistics, without touching acquisition rows.

    A freshly opened owner connection has no query history. The 0x10000 bit
    considers existing indexes too; a warm call is normally a no-op. SQLite
    3.46+ bounds analysis internally. Preserve older runtimes' caller settings
    while explicitly bounding their analysis. This is NOT a full ANALYZE,
    VACUUM, durability change, or maintenance performed by read-only readers.
    """
    if sqlite3.sqlite_version_info >= (3, 46, 0):
        connection.execute('PRAGMA optimize=0x10002').fetchall()
        return
    previous = connection.execute('PRAGMA analysis_limit').fetchone()[0]
    limit = min(previous, 1000) if previous > 0 else 1000
    try:
        connection.execute(f'PRAGMA analysis_limit={limit}')
        connection.execute('PRAGMA optimize=0x10002').fetchall()
    finally:
        connection.execute(f'PRAGMA analysis_limit={previous}')


def retryable_queue_error(error: sqlite3.OperationalError) -> bool:
    """Retry contention, not corrupt databases, invalid SQL or arbitrary I/O.

    SQLite extended codes have the primary error in their low byte. Do not
    persist exception strings: they may include local paths or request text.
    """
    code = getattr(error, 'sqlite_errorcode', None)
    return isinstance(code, int) and (code & 0xff) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


def _stamp(value: Any) -> datetime | None:
    try:
        stamp = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return stamp.astimezone(UTC) if stamp.tzinfo else None
    except (ValueError, TypeError):
        return None


def idle_heartbeat(root: Path, status: dict[str, Any], now: datetime) -> dict[str, Any]:
    """Allowlisted liveness, valid only for the exact completed acquisition cycle."""
    result: dict[str, Any] = {'state': 'unavailable', 'alive': False,
                              'observed_at_utc': None, 'next_check_at_utc': None}
    try:
        with (root / 'worker_status.json').open('rb') as stream:
            body = stream.read(8193)
        if len(body) > 8192:
            return result
        value = json.loads(body)
        stamp = _stamp(value.get('observed_at_utc'))
        deadline = _stamp(value.get('next_check_at_utc'))
        cycle = _stamp(status.get('observed_at_utc'))
        valid = bool(value.get('schema_version') == 1 and value.get('phase') == 'waiting'
                     and value.get('cycle_observed_at_utc') == status.get('observed_at_utc')
                     and value.get('reason') == status.get('state') and cycle and stamp and deadline
                     and cycle <= stamp <= deadline
                     and -5 <= (now - stamp).total_seconds() <= 90
                     and -5 <= (deadline - now).total_seconds() <= 3600
                     and 0 <= (deadline - cycle).total_seconds() <= 3660)
        if valid:
            result.update(state='waiting', alive=True, observed_at_utc=stamp.isoformat(),
                          next_check_at_utc=deadline.isoformat())
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return result


def next_cycle_delay(root: Path, now: datetime, maximum: float, *,
                     sources: Iterable[Source] = (), snapshot_hours: tuple[int, ...] = ()) -> float:
    """Wake at an existing future retry/release boundary, not one poll late."""
    deadline = now + timedelta(seconds=maximum)
    if (root / 'calendar.json').is_file():
        # Free's calendar has an independent expiry/publication clock; waking
        # solely at midnight/14:00 left a 20-hour-old calendar in use.
        deadline = min(deadline, max(now + timedelta(seconds=1), calendar_next_check(root, now)))
    path = root / 'queue.sqlite3'
    if path.is_file():
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0.2)) as conn:
                row = conn.execute(
                    "SELECT MIN(next_attempt_at_utc) FROM tasks WHERE "
                    "state IN ('pending','failed','complete','observed_empty') AND next_attempt_at_utc>?",
                    (now.isoformat(),),
                ).fetchone()
            retry = _stamp(row[0]) if row else None
            if retry:
                deadline = min(deadline, retry)
        except sqlite3.Error:
            pass  # Retry the ordinary cycle; never infer an empty queue.
    local = now.astimezone(TAIPEI)
    clocks = {(source.release_hour, source.release_minute) for source in sources}
    clocks.update((hour, 0) for hour in snapshot_hours)
    clocks.add((9, 10))  # End of the canonical opening-protection window.
    for hour, minute in clocks:
        release = datetime.combine(local.date(), wall_time(hour, minute), TAIPEI)
        if release <= local:
            release += timedelta(days=1)
        deadline = min(deadline, release.astimezone(UTC))
    return max(0.0, (deadline - now).total_seconds())


def _backfill_ready(root: Path, now: datetime) -> bool:
    """Release a short priority/quota wait using local evidence only."""
    from downloader.finmind_account import backfill_budget
    from downloader.finmind_scheduling import _read_metadata, fixed_incremental_demand
    shared = root.parent if root.name in {'sponsor', 'complement'} else root
    account = _read_metadata(shared / 'account_status.json')
    try:
        return backfill_budget(account, shared, now=now, prioritize_due=True,
                               fixed_incremental_requests=fixed_incremental_demand(shared, now))['allowed']
    except (ValueError, OSError):
        return False


def wait_for_next_cycle(root: Path, result: dict[str, Any], seconds: float, *,
                        sources: Iterable[Source] = (), snapshot_hours: tuple[int, ...] = (),
                        wake_if: Callable[[], bool] | None = None,
                        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
                        sleep: Callable[[float], None] = time.sleep,
                        monotonic: Callable[[], float] = time.monotonic) -> None:
    now = clock()
    cycle_observed = result.get('observed_at_utc')
    # The Free worker returns a compact outcome, while its complete status was
    # atomically published separately. Bind to that exact cycle, never to now.
    if cycle_observed is None:
        try:
            with (root / 'status.json').open('rb') as stream:
                body = stream.read(4 * 1024 * 1024 + 1)
            status = json.loads(body) if len(body) <= 4 * 1024 * 1024 else {}
            if status.get('state') == result.get('state'):
                cycle_observed = status.get('observed_at_utc')
        except (OSError, ValueError, AttributeError):
            pass
    # Backoff deadlines for bans/rate errors must not be shortened by ordinary
    # publication clocks. The shared account limiter remains authoritative too.
    delay = (seconds if seconds == 0 or result.get('state') in {'rate_limited', 'ip_banned', 'queue_busy'} else
             next_cycle_delay(root, now, seconds, sources=sources, snapshot_hours=snapshot_hours))
    until = now + timedelta(seconds=delay)
    stop = monotonic() + delay
    while (remaining := stop - monotonic()) > 0:
        atomic_write_json(root / 'worker_status.json', {
            'schema_version': 1, 'phase': 'waiting', 'reason': result.get('state'),
            'observed_at_utc': clock().isoformat(), 'next_check_at_utc': until.isoformat(),
            'cycle_observed_at_utc': cycle_observed,
        })
        if wake_if is not None and wake_if():
            break
        priority_wait = result.get('state') == 'incremental_reserve'
        if priority_wait and _backfill_ready(root, clock()):
            break
        sleep(min(5.0 if priority_wait else 30.0, remaining))
    # Invalidate the old idle lease before executing a new cycle. Existing
    # running-task status heartbeats then own acquisition liveness as before.
    atomic_write_json(root / 'worker_status.json', {
        'schema_version': 1, 'phase': 'running', 'observed_at_utc': clock().isoformat(),
    })
