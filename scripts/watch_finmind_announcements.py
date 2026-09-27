"""Daily, source-backed FinMind correction planning; never downloads market data.

Existing workers consume the atomic plan under their own locks and quota gates.
An unreadable announcement page must not erase a previously accepted repair plan.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime, timedelta
import fcntl
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_json
from downloader.finmind_announcements import fetch_announcements
from downloader.finmind_correction_plans import build_repair_plan


def _read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if path.stat().st_size > 4 * 1024**2:
        raise ValueError('announcement plan exceeds size bound')
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError('announcement plan is not an object')
    return value


def next_check(now: datetime) -> str:
    local = now.astimezone(ZoneInfo('Asia/Taipei'))
    scheduled = local.replace(hour=6, minute=10, second=0, microsecond=0)
    if scheduled <= local:
        scheduled += timedelta(days=1)
    return scheduled.isoformat()


def run_once(root: Path, *, now: datetime | None = None, dry_run: bool = False) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise ValueError('now must include a timezone')
    root.mkdir(parents=True, exist_ok=True)
    with (root / 'watcher.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'state': 'already_running'}
        try:
            previous = _read(root / 'repair_plan.json')
        except (OSError, ValueError, TypeError):
            failure = {'state': 'degraded', 'checked_at_utc': now.astimezone(UTC).isoformat(),
                       'error_code': 'previous_plan_unreadable', 'previous_plan_preserved': True,
                       'data_api_requests': 0}
            if not dry_run:
                atomic_write_json(root / 'monitor_status.json', failure)
            return failure
        snapshot = fetch_announcements(root, now)
        status = {
            'schema_version': 1, 'checked_at_utc': now.astimezone(UTC).isoformat(),
            'next_check_taipei': next_check(now), 'schedule': 'daily_06:10_Asia/Taipei_including_holidays',
            'source_url': 'https://finmind.github.io/WhatIsNew/',
            'snapshot_sha256': snapshot.get('snapshot_sha256'),
            'fetch_status': snapshot.get('fetch_status'),
            'latest_notice_date': snapshot.get('latest_notice_date'),
            'data_api_requests': 0, 'dry_run': dry_run,
            'completion_claim': 'plan_only_workers_must_verify_new_source_receipts',
        }
        if snapshot.get('state') != 'ok':
            status.update(state='degraded', error_code=snapshot.get('error_code', 'announcement_unavailable'),
                          plan_id=previous.get('plan_id'), previous_plan_preserved=bool(previous))
        else:
            try:
                plan = build_repair_plan(snapshot['entries'], previous.get('state'), now=now)
            except (OSError, ValueError, TypeError, KeyError):
                status.update(state='degraded', error_code='correction_scope_planning_failed',
                              plan_id=previous.get('plan_id'), previous_plan_preserved=bool(previous))
                if not dry_run:
                    atomic_write_json(root / 'monitor_status.json', status)
                return status
            plan['source_snapshot_sha256'] = snapshot['snapshot_sha256']
            notices = plan.get('notices', [])
            status.update(state='planned', plan_id=plan['plan_id'],
                          plan_changed=plan.get('plan_id') != previous.get('plan_id'),
                          notice_count=len(notices), repair_scope_count=len(plan['requests']),
                          owner_scope_counts=dict(Counter(r['owner'] for r in plan['requests'])),
                          notice_states=dict(Counter(n.get('state', n.get('status', 'unknown')) for n in notices)))
            if not dry_run:
                # Watermarks and queue scopes advance together, so a crash cannot
                # mark a notice seen without publishing its repair intent.
                atomic_write_json(root / 'repair_plan.json', plan)
        if not dry_run:
            stamp = now.astimezone(UTC).strftime('%Y%m%dT%H%M%S%fZ')
            atomic_write_json(root / 'checks' / f'{stamp}.json', status)
            atomic_write_json(root / 'monitor_status.json', status)
        return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('data_finmind/announcements'))
    parser.add_argument('--dry-run', action='store_true', help='cache official page but do not publish a repair plan')
    args = parser.parse_args()
    result = run_once(args.root, dry_run=args.dry_run)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 1 if result['state'] == 'degraded' else 0


if __name__ == '__main__':
    raise SystemExit(main())
