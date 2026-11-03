#!/usr/bin/env python3
"""Bounded handoff of explicit futures requests to the existing FinLab owner.

The predecessor is allowed to finish first.  This coordinator never creates
an SDK session or changes requests: it calls the canonical finite drain, which
retains the shared account lock, source quota, opening window and capacity gate.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from downloader.provider_priority import inspect  # noqa: E402
from scripts.run_finlab_finite_priority_drain import drain, wait_for_headroom  # noqa: E402

FINLAB = ROOT / 'data_finlab'
OWNER_FILES = tuple(ROOT / path for path in (
    'scripts/runtime_env.sh', 'scripts/run_finlab_finite_priority_handoff.py',
    'scripts/run_finlab_finite_priority_drain.py', 'scripts/download_finlab_market_intraday.py',
    'scripts/download_finlab_intraday.py', 'scripts/check_acquisition_capacity.py',
    'scripts/finlab_futures_sdk_bridge.py', 'downloader/provider_priority.py',
    'stockagent/data/finlab_futures_sdk.py',
    'stockagent/data_sync/cache_capacity.py',
))
WORKSPACE_FILES = tuple(ROOT / path for path in (
    'stockagent/data_sync/workspace_volume.py', 'configs/data_sync/workspace_volume.json',
))
WAITING = frozenset({'pending_existing_owner', 'waiting_retry_existing_owner', 'waiting_local_scope'})
TERMINAL = frozenset({'downloaded_unverified_for_pit', 'verified_closed_date', 'verified_no_trade', 'expired'})


def process_start_ticks(pid: int, *, proc_root: Path = Path('/proc')) -> int | None:
    """A PID plus kernel start time distinguishes the original process from reuse."""
    try:
        raw = (proc_root / str(pid) / 'stat').read_text()
    except FileNotFoundError:
        return None
    fields = raw[raw.rindex(')') + 2:].split()
    if len(fields) < 20:
        raise ValueError('invalid predecessor process stat')
    if fields[0] == 'Z':
        return None
    return int(fields[19])


def request_summary(request_ids: list[str], now: datetime, *, reader=inspect) -> dict:
    counts: Counter[str] = Counter()
    expiries = []
    active_expiries = []
    active_waiting = 0
    for identifier in request_ids:
        status = reader(FINLAB, identifier, now=now)
        if (status.get('contract') != 'provider_priority_query_v1'
                or status.get('provider') != 'finlab' or status.get('request_id') != identifier
                or status.get('provider_calls') != 0 or type(status.get('priority_active')) is not bool
                or not isinstance(status.get('items'), list)
                or not status['items']):
            raise ValueError('invalid priority receipt')
        expiry = datetime.fromisoformat(status['expires_at_utc'])
        if expiry.tzinfo is None:
            raise ValueError('priority expiry must have a timezone')
        expiries.append(expiry)
        for row in status['items']:
            key = json.loads(row['key'])
            if (not isinstance(key, list) or len(key) != 2
                    or not isinstance(key[0], str) or not key[0].startswith('tw_futures_minute:')
                    or not isinstance(key[1], str)):
                raise ValueError('handoff is limited to explicit futures minute partitions')
            state = row.get('state')
            if state not in WAITING | TERMINAL:
                raise ValueError(f'priority receipt needs review: {state}')
            counts[state] += 1
            if status['priority_active'] and state in WAITING:
                active_waiting += 1
                active_expiries.append(expiry)
    return {'counts': dict(counts), 'active_waiting': active_waiting, 'latest_expiry': max(expiries),
            'latest_active_expiry': max(active_expiries) if active_expiries else None}


def retry_delay(code: int, result: dict, now: datetime) -> int | None:
    """Only retry known bounded deferrals; unknown failures stay visible."""
    state = result.get('state')
    if code == 0 and state == 'pass_limit_reached':
        return 0
    if code == 0 and state == 'no_more_actionable_in_pass':
        return 3600
    if code == 75 and state == 'capacity_wait_timeout':
        return 0
    if code == 75 and state == 'account_busy':
        return 300
    if code == 75 and state == 'child_deferred' and result.get('child_state') == 'priority_handoff':
        # The native owner yielded to a due day-trade request. It has already
        # released its account lock; wait and let that same owner re-evaluate
        # all admission gates. A normal priority handoff is not an unknown
        # source result, and must not permanently abandon the finite request.
        return 300
    if code == 75 and state == 'child_deferred' and result.get('child_state') == 'quota_margin_reached':
        tomorrow = (now + timedelta(days=1)).date()
        return max(1, int((datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 1, tzinfo=UTC) - now).total_seconds()))
    return None


def run(request_ids: list[str], predecessor_pid: int, predecessor_ticks: int, progress: Path,
        *, now=lambda: datetime.now(UTC), monotonic=time.monotonic, sleep=time.sleep,
        reader=inspect, process=process_start_ticks, digest=sha256_file, drain_owner=drain,
        headroom=wait_for_headroom, futures_workspace_only: bool = False) -> int:
    if not request_ids or len(request_ids) != len(set(request_ids)) or predecessor_pid <= 0 or predecessor_ticks <= 0:
        raise ValueError('unique requests and a specific predecessor are required')
    start = now()
    initial = request_summary(request_ids, start, reader=reader)
    deadline = min(initial['latest_expiry'], start + timedelta(days=7))
    if type(futures_workspace_only) is not bool:
        raise ValueError('invalid futures workspace mode')
    pins = {str(path): digest(path) for path in
            OWNER_FILES + (WORKSPACE_FILES if futures_workspace_only else ())}

    def pins_match() -> bool:
        return all(digest(Path(path)) == value for path, value in pins.items())

    def report(stage: str, summary: dict, **extra) -> None:
        atomic_write_json(progress, {'contract': 'finlab_finite_priority_handoff_v1', 'stage': stage,
            'updated_at_utc': now().isoformat(), 'deadline_utc': deadline.isoformat(),
            'request_ids': request_ids, 'states': summary['counts'],
            'active_waiting': summary['active_waiting'], 'predecessor_pid': predecessor_pid,
            'predecessor_start_ticks': predecessor_ticks, 'source_sha256': pins,
            'futures_workspace_only': futures_workspace_only,
            'data_completeness_claim': False, **extra})

    while True:
        if not pins_match():
            report('owner_changed_review_required', initial)
            return 75
        summary = request_summary(request_ids, now(), reader=reader)
        if now() >= deadline or not summary['active_waiting']:
            report('requests_expired_or_no_longer_waiting', summary)
            return 0
        if process(predecessor_pid) == predecessor_ticks:
            report('waiting_for_original_predecessor', summary)
            sleep(min(60, max(0, (deadline - now()).total_seconds())))
            continue
        report('waiting_for_canonical_owner_admission', summary)

        def capacity_wait() -> bool:
            if not pins_match():
                return False
            current = request_summary(request_ids, now(), reader=reader)
            active_expiry = current['latest_active_expiry']
            if active_expiry is None:
                return False
            remaining = max(0, (min(deadline, active_expiry) - now()).total_seconds())
            if not remaining:
                return False
            # The wait can last hours. Recheck the same owner pins each minute
            # rather than leaving an already superseded coordinator asleep
            # until capacity recovers; no account lock is held here.
            kwargs = {}
            if futures_workspace_only:
                from scripts.check_acquisition_capacity import futures_workspace_dispatch_status
                kwargs['sample'] = lambda: futures_workspace_dispatch_status(FINLAB)
            admitted = headroom(monotonic() + min(12 * 3600, remaining),
                                still_valid=pins_match, **kwargs)
            if not admitted or not pins_match() or now() >= deadline:
                return False
            current = request_summary(request_ids, now(), reader=reader)
            return bool(current['active_waiting'] and current['latest_active_expiry']
                        and now() < min(deadline, current['latest_active_expiry']))

        kwargs = {'futures_workspace_only': True} if futures_workspace_only else {}
        code, result = drain_owner(32, kind='tw_minute', capacity_wait=capacity_wait, **kwargs)
        after = request_summary(request_ids, now(), reader=reader)
        report('canonical_owner_returned', after, owner_exit_code=code, owner_result=result)
        if now() >= deadline or not after['active_waiting']:
            continue
        delay = retry_delay(code, result, now())
        if delay is None:
            report('owner_result_review_required', after, owner_exit_code=code, owner_result=result)
            return code or 1
        if delay:
            sleep(min(delay, max(0, (deadline - now()).total_seconds())))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--request-id', action='append', required=True)
    parser.add_argument('--predecessor-pid', type=int, required=True)
    parser.add_argument('--predecessor-start-ticks', type=int, required=True)
    parser.add_argument('--progress', type=Path, required=True)
    parser.add_argument('--futures-workspace-only', action='store_true')
    args = parser.parse_args()
    progress = args.progress.resolve()
    if not progress.is_relative_to((ROOT / 'artifacts/operations').resolve()):
        parser.error('progress must be within artifacts/operations')
    return run(args.request_id, args.predecessor_pid, args.predecessor_start_ticks, progress,
               futures_workspace_only=args.futures_workspace_only)


if __name__ == '__main__':
    raise SystemExit(main())
