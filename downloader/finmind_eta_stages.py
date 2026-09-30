"""Ordered, conditional projections using one shared FinMind request budget.

This observes the existing scheduler; it neither reprioritizes nor claims work.
The finite priority override is a subset of ordinary history, never extra work.
Future low-priority daily arrivals wait for their stage and are labeled modeled
requests, separately from the measured backlog at the observation cutoff.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta
import math
from typing import Any, Callable

from downloader.finmind_eta import estimate_completion, scheduled_finish
from downloader.finmind_eta_schedule import release_aware_finish
from downloader.finmind_eta_work import COUNT_FIELDS
from downloader.finmind_supplemental import SOURCES


STAGES = (
    ('priority', '到期追新／指定優先回補（選定期貨分鐘）'),
    ('core', '主要歷史／日資料／財報／總經／新聞'),
    ('detail', '其餘分鐘與分點明細'),
    ('tick', '最低優先 tick 歷史'),
    ('validation', '剩餘跨來源校驗'),
)
SCENARIOS = ('fastest', 'central', 'slowest')


def _summary() -> dict[str, Any]:
    return dict.fromkeys((*COUNT_FIELDS, 'unknown_datasets', 'unscheduled_datasets', 'max_retry_wait_seconds'), 0)


def stage_workloads(workload: dict[str, Any]) -> list[dict[str, Any]]:
    """Disjoint request sets in scheduler order; no full-capacity parallel ETAs."""
    buckets = {key: _summary() for key, _ in STAGES}
    for row in workload['datasets']:
        source = SOURCES.get(row['dataset'])
        key = 'tick' if source and source.priority >= 10 else 'detail' if source and source.priority >= 8 else 'core'
        values = {field: row.get(field) or 0 for field in COUNT_FIELDS}
        values.update(unknown_datasets=int(row['state'] == 'unknown'),
                      unscheduled_datasets=int(row['state'] == 'unscheduled'),
                      max_retry_wait_seconds=row.get('max_retry_wait_seconds', 0))
        validation = values['validation_requests']
        # Secondary tasks are never eligible for range batching (priority >=8).
        for field in ('validation_requests', 'current_plan_requests', 'unbatched_requests', 'fastest_requests'):
            values[field] -= validation
            buckets['validation'][field] += validation
        if validation:
            buckets['validation']['max_retry_wait_seconds'] = max(
                buckets['validation']['max_retry_wait_seconds'], values['max_retry_wait_seconds'])
        incremental = values['incremental_requests']
        unbatched_incremental = row.get('unbatched_incremental_requests', incremental)
        fastest_incremental = row.get('fastest_incremental_requests', incremental)
        moved_fields = dict.fromkeys(('required_requests', 'incremental_requests', 'current_plan_requests'), incremental)
        moved_fields.update(unbatched_requests=unbatched_incremental, fastest_requests=fastest_incremental,
                            batch_savings=unbatched_incremental-incremental,
                            uncertain_requests=unbatched_incremental-fastest_incremental)
        for field, moved in moved_fields.items():
            values[field] -= moved
            buckets['priority'][field] += moved
        if incremental:
            buckets['priority']['max_retry_wait_seconds'] = max(
                buckets['priority']['max_retry_wait_seconds'], values['max_retry_wait_seconds'])
        override = row.get('priority_override', {})
        requests = override.get('requests', 0)
        if override.get('unsupported_tasks', 0):
            raise ValueError('priority_override_query_grain_unverified')
        if requests:
            if requests > values['backfill_requests']:
                raise ValueError('priority_override_must_be_subset_of_required_history')
            for field in ('required_requests', 'backfill_requests', 'current_plan_requests',
                          'unbatched_requests', 'fastest_requests'):
                values[field] -= requests
                buckets['priority'][field] += requests
            buckets['priority']['max_retry_wait_seconds'] = max(
                buckets['priority']['max_retry_wait_seconds'], override.get('max_retry_wait_seconds', 0))
        for field in ('inflight_tasks', 'blocked_tasks'):
            moved = override.get(field, 0)
            values[field] -= moved
            buckets['priority'][field] += moved
        for field, value in values.items():
            if field == 'max_retry_wait_seconds':
                buckets[key][field] = max(buckets[key][field], value)
            else:
                buckets[key][field] += value
    if sum(x['current_plan_requests'] for x in buckets.values()) != workload['summary']['current_plan_requests']:
        raise ValueError('stage_request_counts_do_not_reconcile')
    cumulative = 0
    result = []
    for key, label in STAGES:
        summary = buckets[key]
        cumulative += summary['current_plan_requests']
        result.append({'key': key, 'label': label, 'summary': summary,
                       'cumulative_planned_requests': cumulative,
                       'state': 'partial' if summary['unknown_datasets'] else 'observed'})
    return result


def ordered_estimate(workload: dict[str, Any], telemetry: dict[str, Any], now: datetime, *,
                     day_is_protected: Callable, secondary_admission: dict[str, Any]) -> dict[str, Any]:
    overall = estimate_completion(workload, telemetry, now, day_is_protected=day_is_protected,
                                  secondary_admission=secondary_admission)
    forecast = telemetry.get('quota', {}).get('recurring_forecast', {})
    components = forecast.get('requests_per_hour_by_phase', {})
    arrivals = forecast.get('new_partition_requests_per_hour_by_phase', {})
    timed = forecast.get('timed_incremental', {})
    timed_events = timed.get('events') if timed.get('state') == 'modeled' else None
    cursors: dict[str, datetime | None] = dict.fromkeys(SCENARIOS, now)
    cumulative_work: dict[str, int | None] = dict.fromkeys(SCENARIOS, 0)
    cumulative_requests: dict[str, int | None] = dict.fromkeys(SCENARIOS, 0)
    stages = []
    for scope in stage_workloads(workload):
        key = scope['key']
        active_phases = (('incremental',) if key == 'priority' and 'incremental' in components else
                         ('incremental', 'core') if key in {'priority', 'core'} else
                         ('incremental', 'core', 'detail') if key == 'detail' else
                         ('incremental', 'core', 'detail', 'tick'))
        stage_telemetry = {**telemetry, 'quota': dict(telemetry['quota'])}
        if components:
            stage_telemetry['quota']['forecast_recurring_requests_per_hour'] = math.ceil(
                sum(components.get(phase, 0) for phase in active_phases
                    if timed_events is None or phase != 'incremental'))
        budget = stage_telemetry['quota'].get('current_budget', {})
        if (timed_events is not None and budget.get('priority_wait') and budget.get('schedule_verified', True)
                and budget.get('remaining', 0) > 0):
            # This known predecessor is simulated below, not an unknown quota
            # outage. This changes only the conditional forecast, not admission.
            stage_telemetry['quota']['current_budget'] = {**budget, 'allowed': True}
        admission = secondary_admission
        # The current denial is the modeled predecessor itself, not an unknown
        # external approval. This changes no runtime admission or quota policy.
        if (key == 'validation' and workload.get('state') == 'observed'
                and not workload['summary'].get('unknown_datasets')
                and admission.get('scope') == 'finmind_shared_account'
                and admission.get('mode') == 'provider_spare_quota'
                and admission.get('reason') == 'finmind_required_acquisition_pending'):
            admission = {'allowed': True, 'reason': 'conditional_after_preceding_required_stages'}
        item = estimate_completion(scope, stage_telemetry, now, day_is_protected=day_is_protected,
                                   secondary_admission=admission)
        item.update(key=key, scope_label=scope['label'],
                    cumulative_planned_requests=scope['cumulative_planned_requests'])
        for name, scenario in item['scenarios'].items():
            cursor = cursors[name]
            base_requests = scenario['request_count']
            rate = scenario['effective_requests_per_hour']
            duration = scenario['active_work_seconds']
            # Low-priority new daily partitions accumulated while earlier stages
            # ran. Do not pretend they used early-stage capacity OR disappear.
            new_rate = arrivals.get(key, 0) if key in {'detail', 'tick'} else 0
            accrued = (math.ceil(new_rate * max(0, (cursor - now).total_seconds()) / 3600)
                       if cursor is not None else None if new_rate else 0)
            if accrued and base_requests is not None:
                scenario['request_count'] = base_requests + accrued
                duration = math.ceil((base_requests + accrued) * 3600 / rate) if rate and duration is not None else None
            elif accrued is None:
                duration = None
            scenario.update(base_request_count=base_requests, forecast_arrival_requests=accrued,
                            active_work_seconds=duration, stage_start_at_utc=None, stage_duration_seconds=None)
            cumulative_work[name] = (cumulative_work[name] + duration
                                     if cumulative_work[name] is not None and duration is not None else None)
            cumulative_requests[name] = (cumulative_requests[name] + scenario['request_count']
                                         if cumulative_requests[name] is not None and accrued is not None
                                         and scenario['request_count'] is not None else None)
            scenario['cumulative_active_work_seconds'] = cumulative_work[name]
            scenario['cumulative_request_count'] = cumulative_requests[name]
            waiting = scenario['state'] in {'waiting_admission', 'waiting_quota'}
            scenario.update(state=scenario['state'] if waiting else 'unknown',
                            remaining_seconds=None, estimated_complete_at_utc=None)
            if cursor is not None and duration is not None and not waiting:
                cooldown = scope['summary']['max_retry_wait_seconds'] if scenario['request_count'] else 0
                earliest_retry = now + timedelta(seconds=cooldown)
                start = max(cursor, earliest_retry) if name == 'slowest' else cursor
                try:
                    if timed_events is not None and rate and scenario['request_count']:
                        finish, pause, refresh = release_aware_finish(
                            start, scenario['request_count'], rate, timed_events,
                            day_is_protected=day_is_protected)
                        extra = math.ceil(refresh * 3600 / rate)
                        duration += extra
                        cumulative_work[name] += extra
                        scenario.update(active_work_seconds=duration, forecast_refresh_requests=refresh,
                                        cumulative_active_work_seconds=cumulative_work[name],
                                        effective_requests_per_hour=scenario['request_count'] * 3600 / duration,
                                        basis='依發布／到期時點插入追新，未到期不扣容量；後續重試與新代號未計入')
                    else:
                        finish, pause = scheduled_finish(start, duration, day_is_protected=day_is_protected)
                    if cooldown and rate:
                        retry_finish, _ = scheduled_finish(earliest_retry, 3600 / rate, day_is_protected=day_is_protected)
                        finish = max(finish, retry_finish)
                    if (finish - now).total_seconds() > 10 * 366 * 86400:
                        raise ValueError('cumulative_projection_horizon')
                except ValueError:
                    scenario['basis'] += '；累計超過十年投影範圍，不提供遠期日曆完成日'
                    cursors[name] = None
                else:
                    scenario.update(state='estimated', stage_start_at_utc=start.isoformat(),
                                    stage_duration_seconds=math.ceil((finish - start).total_seconds()),
                                    remaining_seconds=math.ceil((finish - now).total_seconds()),
                                    estimated_complete_at_utc=finish.isoformat(), opening_pause_seconds=pause)
                    cursors[name] = finish
            else:
                cursors[name] = None
        if timed_events is not None:
            item['rate_evidence'].update(
                scheduling_basis='release_clock_events',
                effective_requests_per_hour=item['scenarios']['central']['effective_requests_per_hour'])
        stages.append(item)
    overall['stages'] = stages
    overall['schema_version'] = 4 if timed_events is not None else 3
    if workload.get('state') == 'observed':
        overall['scope_label'] = 'FinMind 已知待抓＋分階段追新模型（非永久完工）'
    overall['basis'] = '依現行優先順序分階段，共用一個帳號；逐階段計入追新負載、等待與前階段耗時。'
    overall['assumptions'].append('階段為共用容量的條件式排程投影；worker 可交錯執行，不保證每類串行獨占。未到階段的新日分區另列模型量。')
    if timed_events is not None:
        overall['basis'] = '共用吞吐按階段推進；最高優先追新只在發布／到期時插入，不以日均需求全天扣額度。'
        overall['assumptions'].append('追新按現有佇列首次到期與成功續期規律，向上取整至分鐘；低優先歷史重驗與新分區仍為階段平均模型，不是實際保留額度。')
    overall['scenarios'] = deepcopy(stages[-1]['scenarios'])
    runtime_budget = telemetry['quota'].get('current_budget', {})
    if (timed_events is not None and runtime_budget.get('priority_wait')
            and runtime_budget.get('schedule_verified', True) and runtime_budget.get('remaining', 0) > 0):
        overall['blockers'] = [row for row in overall['blockers'] if row['code'] != 'quota_pause']
    for name, scenario in overall['scenarios'].items():
        scenario['request_count'] = scenario['cumulative_request_count']
        scenario['active_work_seconds'] = scenario['cumulative_active_work_seconds']
        base = workload['summary']['unbatched_requests' if name == 'slowest' else 'current_plan_requests']
        scenario.update(base_request_count=base, stage_start_at_utc=None, stage_duration_seconds=None,
                        forecast_arrival_requests=scenario['request_count'] - base if scenario['request_count'] is not None else None)
        duration, requests = scenario['active_work_seconds'], scenario['request_count']
        scenario['effective_requests_per_hour'] = requests * 3600 / duration if duration and requests is not None else None
        scenario['basis'] = '各階段順序累加（含等待期間新增日分區模型）；有效速度是全程加權，不是單一滾動小時速度。'
    current = next((item for item in stages if item['workload']['planned_requests']), stages[0])
    overall['rate_evidence'] = {**current['rate_evidence'], 'stage_key': current['key'],
                                'stage_label': current['scope_label'], 'overall_rate_basis': 'stage_weighted'}
    # Preserve the old API keys, now with actual predecessor time included.
    overall['milestones'] = {key: deepcopy(stages[index]) for key, index in (('core', 1), ('non_tick', 2), ('all', 4))}
    overall['milestones']['core']['scope_label'] = '累計：指定優先回補＋主要歷史／新聞'
    overall['milestones']['non_tick']['scope_label'] = '累計：全部非 tick 資料'
    overall['milestones']['all']['scope_label'] = '累計：全部已知工作（含校驗）'
    if overall['state'] != 'unavailable':
        if any(item['state'] == 'waiting_quota' for item in stages):
            overall['state'] = 'waiting_quota'
        elif any(item['state'] == 'waiting_admission' for item in stages):
            overall['state'] = 'waiting_admission'
        elif any(value['active_work_seconds'] is None for value in overall['scenarios'].values()):
            overall['state'] = 'warming_up'
        else:
            overall['state'] = 'conditional' if workload['summary']['current_plan_requests'] or overall['blockers'] else 'current'
            overall['blockers'] = [row for row in overall['blockers'] if row['code'] != 'secondary_admission']
    return overall
