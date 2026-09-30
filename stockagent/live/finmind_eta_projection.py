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
    result = _public_estimate(payload, now)
    children = payload.get('milestones')
    if isinstance(children, dict):
        result['milestones'] = {key: _public_estimate(children[key], now) for key in ('core', 'non_tick', 'all')
                                if isinstance(children.get(key), dict)}
    stages = payload.get('stages')
    if isinstance(stages, list):
        result['stages'] = []
        for key in ('priority', 'core', 'detail', 'tick', 'validation'):
            raw = next((item for item in stages if isinstance(item, dict) and item.get('key') == key), None)
            if raw is not None:
                # A child cannot outlive the evidence envelope that owns it.
                item = _public_estimate({**raw, 'observed_at_utc': payload.get('observed_at_utc'),
                                         'valid_until_utc': payload.get('valid_until_utc')}, now)
                item.update(key=key, cumulative_planned_requests=_number(raw.get('cumulative_planned_requests')))
                result['stages'].append(item)
    return result


def _public_estimate(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    observed, expiry = _stamp(payload.get('observed_at_utc')), _stamp(payload.get('valid_until_utc'))
    fresh = bool(observed and expiry and -60 <= (now - observed).total_seconds() <= 300
                 and observed <= expiry <= observed + timedelta(minutes=5) and now <= expiry)
    state = payload.get('state')
    if state not in {'estimated', 'conditional', 'current', 'warming_up', 'unavailable', 'stale',
                     'waiting_admission', 'waiting_quota'}:
        state = 'unavailable'
    if payload and not fresh:
        state = 'stale'
    usable = fresh and state not in {'unavailable', 'stale'}
    result = {
        'schema_version': payload['schema_version'] if payload.get('schema_version') in {3, 4} else 2,
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
            'blocked_tasks', 'unscheduled_datasets', 'unknown_datasets', 'inflight_tasks', 'local_derived_tasks',
            'batch_savings', 'calendar_wait_tasks')}
    scenarios = payload.get('scenarios')
    for key in ('fastest', 'central', 'slowest'):
        raw = scenarios.get(key) if isinstance(scenarios, dict) else None
        raw = raw if isinstance(raw, dict) else {}
        finish, seconds = _stamp(raw.get('estimated_complete_at_utc')), _number(raw.get('remaining_seconds'))
        waiting = usable and state in {'waiting_admission', 'waiting_quota'}
        valid = bool(usable and not waiting and raw.get('state') == 'estimated' and seconds is not None
                     and finish and observed and finish >= observed
                     and abs((finish-observed).total_seconds() - seconds) <= 2)
        result['scenarios'][key] = {
            'state': 'estimated' if valid else state if waiting else 'unknown',
            'remaining_seconds': seconds if valid else None,
            'estimated_complete_at_utc': finish.isoformat() if valid else None,
            'active_work_seconds': _number(raw.get('active_work_seconds')) if usable else None,
            'request_count': _number(raw.get('request_count')),
            'effective_requests_per_hour': _number(raw.get('effective_requests_per_hour')),
            'basis': str(raw.get('basis', '尚缺估算證據'))[:300],
        }
        start, duration = _stamp(raw.get('stage_start_at_utc')), _number(raw.get('stage_duration_seconds'))
        valid_stage = bool(valid and start and observed and observed <= start <= finish and duration is not None
                           and abs((finish - start).total_seconds() - duration) <= 2)
        result['scenarios'][key].update(
            stage_start_at_utc=start.isoformat() if valid_stage else None,
            stage_duration_seconds=duration if valid_stage else None,
            base_request_count=_number(raw.get('base_request_count')),
            forecast_arrival_requests=_number(raw.get('forecast_arrival_requests')),
            forecast_refresh_requests=_number(raw.get('forecast_refresh_requests')),
            cumulative_request_count=_number(raw.get('cumulative_request_count')),
            cumulative_active_work_seconds=_number(raw.get('cumulative_active_work_seconds')) if usable else None)
    rates = payload.get('rate_evidence')
    if isinstance(rates, dict):
        result['rate_evidence'] = {key: _number(rates.get(key)) for key in (
            'official_requests_per_hour', 'paced_requests_per_hour', 'rolling_requests_60m',
            'gross_requests_per_hour', 'future_recurring_requests_per_hour',
            'effective_requests_per_hour', 'current_reserved_requests')}
        result['rate_evidence'].update(
            scheduling_basis='release_clock_events' if rates.get('scheduling_basis') == 'release_clock_events' else 'average_load',
            rolling_complete_window=rates.get('rolling_complete_window') is True,
            stage_label=str(rates.get('stage_label', '目前階段'))[:160],
            overall_rate_basis='stage_weighted' if rates.get('overall_rate_basis') == 'stage_weighted' else 'single_stage')
        for key in ('rolling_window_start_at_utc', 'rolling_window_end_at_utc'):
            stamp = _stamp(rates.get(key))
            result['rate_evidence'][key] = stamp.isoformat() if stamp else None
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
