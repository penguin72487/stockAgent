"""Cheap, allowlisted public projection of the minute-sampled local ETA."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any


def _number(value: Any) -> int | float | None:
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return stamp.astimezone(UTC) if stamp.tzinfo else None
    except ValueError:
        return None


def public_completion_estimate(root: Path, now: datetime) -> dict[str, Any]:
    """No queue scan or provider call on the browser request path."""
    payload: dict[str, Any] = {}
    try:
        with (root / 'eta_status.json').open('rb') as stream:
            body = stream.read(1024 * 1024 + 1)
        if len(body) <= 1024 * 1024:
            value = json.loads(body)
            if isinstance(value, dict) and value.get('schema_version') == 1 and isinstance(value.get('estimate'), dict):
                payload = value['estimate']
    except (OSError, ValueError):
        pass
    observed, expiry = _stamp(payload.get('observed_at_utc')), _stamp(payload.get('valid_until_utc'))
    fresh = bool(observed and expiry and -60 <= (now - observed).total_seconds() <= 300
                 and observed <= expiry <= observed + timedelta(minutes=5) and now <= expiry)
    state = payload.get('state')
    if state not in {'estimated', 'conditional', 'current', 'warming_up', 'unavailable', 'stale'}:
        state = 'unavailable'
    if payload and not fresh:
        state = 'stale'
    usable = fresh and state not in {'unavailable', 'stale'}
    result = {
        'state': state, 'observed_at_utc': observed.isoformat() if observed else None,
        'valid_until_utc': expiry.isoformat() if expiry else None,
        'scope_label': str(payload.get('scope_label', 'FinMind 已排程工作'))[:160],
        'is_guaranteed': False, 'absolute_slowest_seconds': None,
        'scenarios': {}, 'workload': {}, 'assumptions': [], 'blockers': [],
        'basis': str(payload.get('basis', '等待每分鐘本機估算快照；不因瀏覽網頁呼叫資料 API。'))[:400],
    }
    work = payload.get('workload')
    if isinstance(work, dict):
        result['workload'] = {key: _number(work.get(key)) for key in (
            'required_requests', 'validation_requests', 'planned_requests', 'unbatched_requests',
            'blocked_tasks', 'unscheduled_datasets', 'unknown_datasets', 'inflight_tasks', 'local_derived_tasks', 'batch_savings')}
    scenarios = payload.get('scenarios')
    for key in ('fastest', 'central', 'slowest'):
        raw = scenarios.get(key) if isinstance(scenarios, dict) else None
        raw = raw if isinstance(raw, dict) else {}
        finish, seconds = _stamp(raw.get('estimated_complete_at_utc')), _number(raw.get('remaining_seconds'))
        valid = bool(usable and raw.get('state') == 'estimated' and seconds is not None
                     and finish and observed and finish >= observed
                     and abs((finish-observed).total_seconds() - seconds) <= 2)
        result['scenarios'][key] = {
            'state': 'estimated' if valid else 'unknown',
            'remaining_seconds': seconds if valid else None,
            'estimated_complete_at_utc': finish.isoformat() if valid else None,
            'request_count': _number(raw.get('request_count')),
            'effective_requests_per_hour': _number(raw.get('effective_requests_per_hour')),
            'basis': str(raw.get('basis', '尚缺估算證據'))[:300],
        }
    assumptions = payload.get('assumptions')
    if isinstance(assumptions, list):
        result['assumptions'] = [item[:400] for item in assumptions[:10] if isinstance(item, str)]
    blockers = payload.get('blockers')
    if isinstance(blockers, list):
        result['blockers'] = [
            {'code': item['code'], 'count': _number(item.get('count')), 'reason': item['reason'][:400]}
            for item in blockers[:10] if isinstance(item, dict) and isinstance(item.get('reason'), str)
            and item.get('code') in {'blocked_tasks', 'unscheduled_datasets', 'unknown_datasets',
                                     'secondary_admission', 'unmeasured_tail', 'quota_pause'}]
    return result
