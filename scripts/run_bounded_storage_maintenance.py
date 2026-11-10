#!/usr/bin/env python3
"""Run the existing storage owner until its next trading-priority boundary."""
from __future__ import annotations

from datetime import datetime, time as wall_time, timedelta
import math
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.check_outside_tw_opening_resource_window import TAIPEI
from stockagent.data_sync.desync_snapshots import SnapshotError

MAX_RUNTIME_SECONDS = 8 * 60 * 60
PRIORITY_START = wall_time(8, 0)
PRIORITY_END = wall_time(13, 30)
ALLOWED = frozenset({'scripts/run_source_local_repack_queue.py',
                     'scripts/run_offline_replay_pair_migration.py',
                     'scripts/run_private_subset_reuse_queue.py',
                     'scripts/run_prepared_stream_install_queue.py',
                     'scripts/run_dedup_candidate_queue.py',
                     'scripts/run_packed_object_placement.py',
                     'scripts/run_vast_sealed_e_handoff.py'})
GUARD_ENV = 'STOCKAGENT_TRADING_PRIORITY_GUARD'


def session_requires_protection(day) -> bool:
    """Use the official schedule, including exceptional weekend sessions."""
    from stockagent.live.market_status import tw_stock_day_decision
    try:
        decision = tw_stock_day_decision(day, parquet_root=ROOT/'data_tw_public')
    except (OSError, ValueError, RuntimeError, KeyError, TypeError):
        return True
    return decision.status != 'closed'


class TradingPriorityGuard:
    """Yield after a clock change or host resume as well as at normal cutoff."""
    def __init__(self):
        self.day, self.checked_at, self.session = None, 0.0, True

    def check(self, observed=None):
        if os.environ.get(GUARD_ENV) != '1':
            return
        local = (observed or datetime.now(TAIPEI)).astimezone(TAIPEI)
        wall = local.timetz().replace(tzinfo=None)
        if not PRIORITY_START <= wall < PRIORITY_END:
            return
        now = time.monotonic()
        if self.day != local.date() or now-self.checked_at >= 60:
            self.session = session_requires_protection(local.date())
            self.day, self.checked_at = local.date(), now
        if self.session:
            raise SnapshotError('Taiwan day-trade priority window; maintenance deferred')


def allowed_seconds(observed: datetime, *, session_on=None, maximum=MAX_RUNTIME_SECONDS) -> int:
    """Bound even a stalled child before the next protected stock session.

    Unknown calendar evidence is treated as a possible session.
    An 08:00 deadline leaves an hour before the ordinary 09:00 market open.
    """
    if observed.tzinfo is None or type(maximum) is not int or maximum <= 0:
        raise ValueError('maintenance deadline requires an aware clock and positive bound')
    now = observed.astimezone(TAIPEI)
    if session_on is None:
        session_on = session_requires_protection
    deadline = now + timedelta(seconds=maximum)
    for offset in range(9):
        day = now.date() + timedelta(days=offset)
        start = datetime.combine(day, PRIORITY_START, tzinfo=TAIPEI)
        end = datetime.combine(day, PRIORITY_END, tzinfo=TAIPEI)
        if end <= now or start >= deadline:
            continue
        if not session_on(day):
            continue
        if start <= now < end:
            return 0
        deadline = min(deadline, start)
        break
    return max(0, math.floor((deadline-now).total_seconds()))


def main() -> int:
    placement_apply = (len(sys.argv) == 3 and sys.argv[1:] ==
                       ['scripts/run_packed_object_placement.py', '--apply'])
    if (len(sys.argv) < 2 or sys.argv[1] not in ALLOWED
            or (len(sys.argv) != 2 and not placement_apply)):
        raise ValueError('maintenance owner is not enrolled')
    duration = allowed_seconds(datetime.now(TAIPEI))
    if duration <= 0:
        print('Taiwan day-trade priority window; maintenance deferred', file=sys.stderr)
        return 75
    print(f'storage maintenance time budget: {duration} seconds', flush=True)
    # GNU timeout signals the child process group, with a bounded final stop.
    # The original systemd owner, dataset locks, journals and timers stay in use.
    os.environ[GUARD_ENV] = '1'
    os.execv('/usr/bin/timeout', ['/usr/bin/timeout', '--signal=TERM', '--kill-after=30s',
        f'{duration}s', '/bin/bash', str(ROOT/'scripts/run_authority_storage_operation.sh'), *sys.argv[1:]])
    return 127


if __name__ == '__main__':
    raise SystemExit(main())
