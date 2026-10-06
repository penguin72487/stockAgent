"""Cheap, allowlisted public projection of the minute-sampled local ETA."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION
from downloader.finmind_history_order import HISTORY_STAGES, STAGES, metadata as history_order_metadata


ORDERED_SNAPSHOT_VERSIONS = frozenset(range(6, SNAPSHOT_CONTRACT_VERSION + 1))
RETRY_SNAPSHOT_VERSIONS = frozenset(range(7, SNAPSHOT_CONTRACT_VERSION + 1))
PUBLIC_SNAPSHOT_VERSIONS = frozenset({3, 4, 5}) | ORDERED_SNAPSHOT_VERSIONS


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
    if payload.get('schema_version') in ORDERED_SNAPSHOT_VERSIONS:
        children = payload.get('stages')
        keys = [item.get('key') for item in children if isinstance(item, dict)] if isinstance(children, list) else []
        if (payload.get('history_order') != history_order_metadata()
                or keys != [key for key, _ in STAGES]):
            # Reordering an old estimate's rows would retain the OLD predecessor
            # times. The minute sampler must recompute the new actual sequence.
            return _public_estimate({**payload, 'state': 'unavailable',
                                     'basis': '排程順序已變更，等待符合目前九類順序的每分鐘估算。'}, now)
    result = _public_estimate(payload, now)
    children = payload.get('milestones')
    if isinstance(children, dict):
        result['milestones'] = {key: _public_estimate(children[key], now) for key in ('core', 'non_tick', 'all')
                                if isinstance(children.get(key), dict)}
    stages = payload.get('stages')
    if isinstance(stages, list):
        result['stages'] = []
        stage_keys = tuple(key for key, _ in STAGES) if payload.get('schema_version') in ORDERED_SNAPSHOT_VERSIONS else (
            'priority', 'core', 'detail', 'tick', 'validation')
        for key in stage_keys:
            raw = next((item for item in stages if isinstance(item, dict) and item.get('key') == key), None)
            if raw is not None:
                # A child cannot outlive the evidence envelope that owns it.
                item = _public_estimate({**raw, 'observed_at_utc': payload.get('observed_at_utc'),
                                         'valid_until_utc': payload.get('valid_until_utc')}, now)
                history = next((stage for stage in HISTORY_STAGES if stage.key == key), None)
                item.update(key=key, dataset=history.dataset if history else None,
                            history_rank=HISTORY_STAGES.index(history) + 1 if history else None,
                            cumulative_planned_requests=_number(raw.get('cumulative_planned_requests')))
                result['stages'].append(item)
    return result


def _public_estimate(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    observed, expiry = _stamp(payload.get('observed_at_utc')), _stamp(payload.get('valid_until_utc'))
    fresh = bool(observed and expiry and -60 <= (now - observed).total_seconds() <= 300
                 and observed <= expiry <= observed + timedelta(minutes=5) and now <= expiry)
    state = payload.get('state')
    if state not in {'estimated', 'conditional', 'current', 'warming_up', 'unavailable', 'stale',
                     'waiting_admission', 'waiting_quota', 'waiting_retry'}:
        state = 'unavailable'
    if payload and not fresh:
        state = 'stale'
    usable = fresh and state not in {'unavailable', 'stale'}
    result = {
        'schema_version': payload['schema_version'] if payload.get('schema_version') in PUBLIC_SNAPSHOT_VERSIONS else 2,
        'scenario_projection_contract': 1 if payload.get('schema_version') in ORDERED_SNAPSHOT_VERSIONS else None,
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
            'batch_savings', 'calendar_wait_tasks', 'retry_tasks', 'retry_exhausted_tasks', 'candidate_requests',
            'independent_retry_tasks', 'independent_retry_requests', 'independent_inflight_tasks')}
    result['retry_wait_seconds'] = _number(payload.get('retry_wait_seconds')) if usable else None
    scenarios = payload.get('scenarios')
    for key in ('fastest', 'central', 'slowest'):
        raw = scenarios.get(key) if isinstance(scenarios, dict) else None
        raw = raw if isinstance(raw, dict) else {}
        finish, seconds = _stamp(raw.get('estimated_complete_at_utc')), _number(raw.get('remaining_seconds'))
        waiting = usable and state in {'waiting_admission', 'waiting_quota', 'waiting_retry'}
        valid = bool(usable and not waiting and raw.get('state') == 'estimated' and seconds is not None
                     and finish and observed and finish >= observed
                     and abs((finish-observed).total_seconds() - seconds) <= 2)
        # A deadline passing is not an accepted receipt. In particular a fresh
        # one-minute forecast must not stay "one minute" until its 5m expiry.
        overdue = bool(valid and finish < now and seconds > 0)
        if overdue:
            valid = False
        result['scenarios'][key] = {
            'state': 'estimated' if valid else 'overdue' if overdue else state if waiting else 'unknown',
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
            cumulative_active_work_seconds=_number(raw.get('cumulative_active_work_seconds')) if usable else None,
            standalone_active_work_seconds=_number(raw.get('standalone_active_work_seconds')) if usable else None)
    projection = payload.get('scheduling_projection')
    if isinstance(projection, dict):
        result['scheduling_projection'] = {
            'independent_retry_requests': _number(projection.get('independent_retry_requests')),
            'independent_inflight_tasks': _number(projection.get('independent_inflight_tasks')),
            'basis': 'independent_worker_retry_does_not_block_history_lane',
        }
    rates = payload.get('rate_evidence')
    if isinstance(rates, dict):
        result['rate_evidence'] = {key: _number(rates.get(key)) for key in (
            'official_requests_per_hour', 'paced_requests_per_hour', 'rolling_requests_60m',
            'gross_requests_per_hour', 'future_recurring_requests_per_hour',
            'effective_requests_per_hour', 'current_reserved_requests', 'priority_requests_per_hour')}
        result['rate_evidence'].update(
            scheduling_basis='release_clock_events' if rates.get('scheduling_basis') == 'release_clock_events' else 'average_load',
            rolling_complete_window=rates.get('rolling_complete_window') is True,
            stage_label=str(rates.get('stage_label', '目前階段'))[:160],
            overall_rate_basis='stage_weighted' if rates.get('overall_rate_basis') == 'stage_weighted' else 'single_stage')
        for key in ('rolling_window_start_at_utc', 'rolling_window_end_at_utc'):
            stamp = _stamp(rates.get(key))
            result['rate_evidence'][key] = stamp.isoformat() if stamp else None
        processing = rates.get('request_processing')
        if isinstance(processing, dict):
            result['rate_evidence']['request_processing'] = {
                'object_requests': _number(processing.get('object_requests')),
                'samples': _number(processing.get('samples')),
                'dependent_processing_samples': _number(processing.get('dependent_processing_samples')),
                'dependent_processing_unknown': processing.get('dependent_processing_unknown') is True,
                'basis': 'observed_object_and_mandatory_local_cost_not_legacy_request_rate',
                'rates': {key: _number(processing.get('rates', {}).get(key)) for key in ('fastest', 'central', 'slowest')},
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
                                     'secondary_admission', 'unmeasured_tail', 'quota_pause', 'retry_tasks', 'retry_exhausted_tasks'}]
    condition = payload.get('retry_condition')
    if (usable and payload.get('schema_version') in RETRY_SNAPSHOT_VERSIONS and isinstance(condition, dict)
            and condition.get('schema_version') == 1
            and condition.get('basis') == 'next_retry_succeeds_no_additional_failures'
            and (_number(condition.get('retry_tasks')) or 0) > 0):
        # Reuse the exact freshness/deadline allowlist. Never release arbitrary
        # nested provider text, source paths, or stale modeled deadlines.
        projected = _public_estimate({**payload, 'state': 'conditional',
                                      'scenarios': condition.get('scenarios'), 'retry_condition': None}, now)
        result['retry_condition'] = {
            'schema_version': 1, 'basis': condition['basis'], 'is_guaranteed': False,
            'retry_tasks': _number(condition.get('retry_tasks')),
            'known_retry_wait_seconds': _number(condition.get('known_retry_wait_seconds')),
            'scenarios': projected['scenarios'],
        }
    return result
