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

from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION, estimate_completion, scheduled_finish
from downloader.finmind_eta_schedule import release_aware_finish
from downloader.finmind_eta_work import COUNT_FIELDS
from downloader.finmind_history_order import BY_DATASET, HISTORY_STAGES, STAGES, metadata as history_order_metadata


SCENARIOS = ('fastest', 'central', 'slowest')


def _cumulative_milestone(stages: list[dict[str, Any]], index: int, label: str) -> dict[str, Any]:
    """A milestone owns every predecessor, not just its last (possibly empty) stage."""
    prefix = stages[:index + 1]
    item = deepcopy(prefix[-1])
    item['scope_label'] = label
    item['workload'] = {field: sum(stage['workload'][field] or 0 for stage in prefix)
                        for field in item['workload']}
    blockers = {}
    for stage in prefix:
        for blocker in stage['blockers']:
            entry = blockers.setdefault(blocker['code'], {**blocker, 'count': 0})
            entry['count'] += blocker['count'] or 0
    item['blockers'] = list(blockers.values())
    for name, scenario in item['scenarios'].items():
        base = sum(stage['scenarios'][name]['base_request_count'] or 0 for stage in prefix)
        requests = scenario['cumulative_request_count']
        duration = scenario['cumulative_active_work_seconds']
        scenario.update(request_count=requests, active_work_seconds=duration, base_request_count=base,
                        stage_start_at_utc=None, stage_duration_seconds=None,
                        forecast_arrival_requests=requests - base if requests is not None else None,
                        effective_requests_per_hour=requests * 3600 / duration if duration and requests is not None else None,
                        opening_pause_seconds=(sum(stage['scenarios'][name]['opening_pause_seconds'] for stage in prefix)
                                               if all(stage['scenarios'][name].get('opening_pause_seconds') is not None
                                                      for stage in prefix) else None))
        if any('forecast_refresh_requests' in stage['scenarios'][name] for stage in prefix):
            scenario['forecast_refresh_requests'] = sum(stage['scenarios'][name].get('forecast_refresh_requests', 0)
                                                      for stage in prefix)
    states = {stage['state'] for stage in prefix}
    item['state'] = next((state for state in ('unavailable', 'waiting_quota', 'waiting_admission', 'waiting_retry', 'warming_up')
                          if state in states),
                         'conditional' if item['workload']['planned_requests'] or item['blockers'] else 'current')
    item['rate_evidence'].update(overall_rate_basis='stage_weighted', stage_label=label,
                                effective_requests_per_hour=item['scenarios']['central']['effective_requests_per_hour'])
    return item


def _summary() -> dict[str, Any]:
    return dict.fromkeys((*COUNT_FIELDS, 'unknown_datasets', 'unscheduled_datasets', 'max_retry_wait_seconds'), 0)


def stage_workloads(workload: dict[str, Any]) -> list[dict[str, Any]]:
    """Disjoint request sets in scheduler order; no full-capacity parallel ETAs."""
    buckets = {key: _summary() for key, _ in STAGES}
    for row in workload['datasets']:
        source = BY_DATASET.get(row['dataset'])
        key = source.key if source else 'core'
        values = {field: row.get(field) or 0 for field in COUNT_FIELDS}
        values.update(unknown_datasets=int(row['state'] == 'unknown'),
                      unscheduled_datasets=int(row['state'] == 'unscheduled'),
                      max_retry_wait_seconds=row.get('max_retry_wait_seconds', 0))
        retry_classes = row.get('retry_tasks_by_class', {})
        retry_waits = row.get('retry_wait_seconds_by_class', {})
        values['max_retry_wait_seconds'] = retry_waits.get('backfill', values['max_retry_wait_seconds'])
        validation = values['validation_requests']
        # Secondary tasks are never eligible for range batching (priority >=8).
        for field in ('validation_requests', 'current_plan_requests', 'unbatched_requests', 'fastest_requests'):
            values[field] -= validation
            buckets['validation'][field] += validation
        if validation:
            retry_count = retry_classes.get('validation', 0)
            values['retry_tasks'] -= retry_count
            buckets['validation']['retry_tasks'] += retry_count
            buckets['validation']['max_retry_wait_seconds'] = max(
                buckets['validation']['max_retry_wait_seconds'],
                retry_waits.get('validation', values['max_retry_wait_seconds']))
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
            retry_count = retry_classes.get('incremental', 0)
            values['retry_tasks'] -= retry_count
            buckets['priority']['retry_tasks'] += retry_count
            buckets['priority']['max_retry_wait_seconds'] = max(
                buckets['priority']['max_retry_wait_seconds'],
                retry_waits.get('incremental', values['max_retry_wait_seconds']))
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
        for field in ('inflight_tasks', 'blocked_tasks', 'retry_tasks'):
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
        if key == 'priority' and not any(
            any(row.get('priority_override', {}).get(field, 0) for field in ('requests', 'inflight_tasks', 'blocked_tasks'))
            for row in workload['datasets']
        ):
            label = '到期追新（指定優先回補目前無待發）'
        history = next((stage for stage in HISTORY_STAGES if stage.key == key), None)
        result.append({'key': key, 'label': label, 'summary': summary,
                       'dataset': history.dataset if history else None,
                       'history_rank': HISTORY_STAGES.index(history) + 1 if history else None,
                       'cumulative_planned_requests': cumulative,
                       'state': 'partial' if summary['unknown_datasets'] else 'observed'})
    return result


def ordered_estimate(workload: dict[str, Any], telemetry: dict[str, Any], now: datetime, *,
                     day_is_protected: Callable, secondary_admission: dict[str, Any],
                     _retry_projection: bool = False) -> dict[str, Any]:
    overall = estimate_completion(workload, telemetry, now, day_is_protected=day_is_protected,
                                  secondary_admission=secondary_admission, _project_calendar=False)
    forecast = telemetry.get('quota', {}).get('recurring_forecast', {})
    components = forecast.get('requests_per_hour_by_stage', {})
    arrivals = forecast.get('new_partition_requests_per_hour_by_stage', {})
    # Old cached telemetry has only coarse families and cannot identify which
    # of the nine stages owns load. Do not split it into fabricated equal shares.
    fine_forecast = bool(components)
    if not fine_forecast:
        components = forecast.get('requests_per_hour_by_phase', {})
        arrivals = forecast.get('new_partition_requests_per_hour_by_phase', {})
    timed = forecast.get('timed_incremental', {})
    timed_events = timed.get('events') if timed.get('state') == 'modeled' else None
    cursors: dict[str, datetime | None] = dict.fromkeys(SCENARIOS, now)
    cumulative_work: dict[str, int | None] = dict.fromkeys(SCENARIOS, 0)
    cumulative_requests: dict[str, int | None] = dict.fromkeys(SCENARIOS, 0)
    stages = []
    active_phases = ['incremental'] if 'incremental' in components else []
    legacy_family_entered = set()
    for scope in stage_workloads(workload):
        key = scope['key']
        history = BY_DATASET.get(scope['dataset'])
        if fine_forecast:
            if key == 'core' or history:
                active_phases.append(key)
            new_rate = arrivals.get(key, 0) if history else 0
        else:
            family = history.family if history else 'core' if key == 'core' else None
            if family and family not in legacy_family_entered:
                active_phases.append(family)
                legacy_family_entered.add(family)
            new_rate = arrivals.get(family, 0) if history else 0
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
                                   secondary_admission=admission, _project_calendar=False)
        item.update(schema_version=SNAPSHOT_CONTRACT_VERSION, key=key, scope_label=scope['label'],
                    dataset=scope['dataset'], history_rank=scope['history_rank'],
                    cumulative_planned_requests=scope['cumulative_planned_requests'])
        for name, scenario in item['scenarios'].items():
            cursor = cursors[name]
            base_requests = scenario['request_count']
            rate = scenario['effective_requests_per_hour']
            duration = scenario['active_work_seconds']
            # Low-priority new daily partitions accumulated while earlier stages
            # ran. Do not pretend they used early-stage capacity OR disappear.
            if history and not fine_forecast and arrivals:
                # A coarse arrival count cannot be charged to every child or
                # assigned to one arbitrary child. Keep absolute dates unknown
                # until the minute sampler produces dataset-grained telemetry.
                cursor = None
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
            waiting = scenario['state'] in {'waiting_admission', 'waiting_quota', 'waiting_retry'}
            scenario.update(state=scenario['state'] if waiting else 'unknown',
                            remaining_seconds=None, estimated_complete_at_utc=None,
                            opening_pause_seconds=None)
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
    overall['schema_version'] = SNAPSHOT_CONTRACT_VERSION
    overall['history_order'] = history_order_metadata()
    if workload.get('state') == 'observed':
        overall['scope_label'] = 'FinMind 已知待抓＋分階段追新模型（非永久完工）'
    overall['basis'] = '依現行優先順序分階段，共用一個帳號；逐階段計入追新負載、等待與前階段耗時。'
    overall['assumptions'].append('九類歷史依指定順序逐階段，美股分鐘最後；到期追新及明確有限優先回補可插入，前階段未建候選／重試不算完成。等待期間新增日分區另列模型量。')
    if not fine_forecast and arrivals:
        overall['assumptions'].append('舊追新快照只有大類負載，尚不能分配至九個階段；等待下次每分鐘細分類取樣，不捏造逐類完成日期。')
    if timed_events is not None:
        overall['basis'] = '共用吞吐按階段推進；最高優先追新只在發布／到期時插入，不以日均需求全天扣額度。'
        overall['assumptions'].append('追新按現有佇列首次到期與成功續期規律，向上取整至分鐘；低優先歷史重驗與新分區仍為階段平均模型，不是實際保留額度。')
    cash_calendars = forecast.get('new_partition_calendars', {})
    if any(value.get('state') == 'modeled_from_verified_cash_year' for value in cash_calendars.values()):
        overall['assumptions'].insert(0, '現貨新分區依最近一年已驗證實際場次密度推算，非未來精確開休市表；期貨夜盤／海外未知日曆仍用每日上側模型。')
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
                        forecast_arrival_requests=scenario['request_count'] - base if scenario['request_count'] is not None else None,
                        opening_pause_seconds=(sum(stage['scenarios'][name]['opening_pause_seconds'] for stage in stages)
                                               if all(stage['scenarios'][name].get('opening_pause_seconds') is not None
                                                      for stage in stages) else None))
        duration, requests = scenario['active_work_seconds'], scenario['request_count']
        scenario['effective_requests_per_hour'] = requests * 3600 / duration if duration and requests is not None else None
        scenario['basis'] = '各階段順序累加（含等待期間新增日分區模型）；有效速度是全程加權，不是單一滾動小時速度。'
    current = next((item for item in stages if item['workload']['planned_requests']), stages[0])
    overall['rate_evidence'] = {**current['rate_evidence'], 'stage_key': current['key'],
                                'stage_label': current['scope_label'], 'overall_rate_basis': 'stage_weighted'}
    # Preserve the old API keys, now with actual predecessor time included.
    overall['milestones'] = {key: _cumulative_milestone(stages, index, label) for key, index, label in (
        ('core', 1, '累計：指定優先回補＋主要歷史／新聞'),
        ('non_tick', max(index + 2 for index, stage in enumerate(HISTORY_STAGES) if stage.family == 'detail'),
         '累計：至全部非 tick 完成（美股分鐘最後，含先行 tick）'),
        ('all', len(stages) - 1, '累計：全部已知工作（含校驗）'))}
    if overall['state'] != 'unavailable':
        if any(item['state'] == 'waiting_quota' for item in stages):
            overall['state'] = 'waiting_quota'
        elif any(item['state'] == 'waiting_admission' for item in stages):
            overall['state'] = 'waiting_admission'
        elif any(item['state'] == 'waiting_retry' for item in stages):
            overall['state'] = 'waiting_retry'
        elif any(value['active_work_seconds'] is None for value in overall['scenarios'].values()):
            overall['state'] = 'warming_up'
        else:
            overall['state'] = 'conditional' if workload['summary']['current_plan_requests'] or overall['blockers'] else 'current'
            overall['blockers'] = [row for row in overall['blockers'] if row['code'] != 'secondary_admission']
    if not _retry_projection and workload['summary'].get('retry_tasks', 0):
        # The actual forecast remains blocked; this is explicitly a separate
        # counterfactual, NOT a successful receipt or a retry completion rate.
        # The real debt/counts/cooldowns are unchanged. Unknown quota/admission
        # and unmeasured in-flight tails still cannot gain finite deadlines.
        modeled = deepcopy(workload)
        modeled['summary']['retry_tasks'] = 0
        for row in modeled['datasets']:
            row['retry_tasks'] = 0
            row['retry_tasks_by_class'] = {}
            if 'priority_override' in row:
                row['priority_override']['retry_tasks'] = 0
        projection = ordered_estimate(modeled, telemetry, now, day_is_protected=day_is_protected,
                                      secondary_admission=secondary_admission, _retry_projection=True)
        condition = {'schema_version': 1, 'basis': 'next_retry_succeeds_no_additional_failures',
                     'retry_tasks': workload['summary']['retry_tasks'],
                     'known_retry_wait_seconds': workload['summary'].get('max_retry_wait_seconds', 0),
                     'is_guaranteed': False}
        overall['retry_condition'] = {**condition, 'scenarios': projection['scenarios']}
        for actual, projected in zip(overall['stages'], projection['stages'], strict=True):
            actual['retry_condition'] = {**condition, 'scenarios': projected['scenarios']}
        for key, actual in overall['milestones'].items():
            actual['retry_condition'] = {**condition, 'scenarios': projection['milestones'][key]['scenarios']}
    return overall
