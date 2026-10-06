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
from downloader.finmind_history_order import BY_DATASET, HISTORY_STAGES, LOCAL_DERIVED_STAGES, STAGES, metadata as history_order_metadata


SCENARIOS = ('fastest', 'central', 'slowest')


def _min_rate(*values: Any) -> float | None:
    return min(values) if all(type(value) in (int, float) and math.isfinite(value) and value >= 0
                              for value in values) else None


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
                        standalone_active_work_seconds=None,
                        stage_start_at_utc=None, stage_duration_seconds=None,
                        forecast_arrival_requests=requests - base if requests is not None else None,
                        effective_requests_per_hour=requests * 3600 / duration if duration and requests is not None else None,
                        opening_pause_seconds=(sum(stage['scenarios'][name]['opening_pause_seconds'] for stage in prefix)
                                               if all(stage['scenarios'][name].get('opening_pause_seconds') is not None
                                                      for stage in prefix) else None))
        if any('forecast_refresh_requests' in stage['scenarios'][name] for stage in prefix):
            scenario['forecast_refresh_requests'] = sum(stage['scenarios'][name].get('forecast_refresh_requests', 0)
                                                      for stage in prefix)
        if any(stage['workload'].get('independent_retry_requests') for stage in prefix):
            finite = [stage['scenarios'][name] for stage in prefix]
            if all(row.get('estimated_complete_at_utc') for row in finite):
                finish = max(datetime.fromisoformat(row['estimated_complete_at_utc']) for row in finite)
                scenario.update(estimated_complete_at_utc=finish.isoformat(),
                                remaining_seconds=max(row['remaining_seconds'] for row in finite))
            else:
                scenario.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None)
    states = {stage['state'] for stage in prefix}
    item['state'] = next((state for state in ('unavailable', 'waiting_quota', 'waiting_admission', 'waiting_retry', 'warming_up')
                          if state in states),
                         'conditional' if item['workload']['planned_requests'] or item['blockers'] else 'current')
    if item['state'] in {'waiting_quota', 'waiting_admission', 'waiting_retry', 'unavailable'}:
        for scenario in item['scenarios'].values():
            scenario.update(state=item['state'], remaining_seconds=None, estimated_complete_at_utc=None)
    item['rate_evidence'].update(overall_rate_basis='stage_weighted', stage_label=label,
                                effective_requests_per_hour=item['scenarios']['central']['effective_requests_per_hour'])
    return item


def _summary() -> dict[str, Any]:
    return dict.fromkeys((*COUNT_FIELDS, 'unknown_datasets', 'unscheduled_datasets',
                          'max_retry_wait_seconds', 'serial_max_retry_wait_seconds'), 0)


def _independent_retry_events(workload: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    """Free owns an independent worker, not a predecessor of Complement.

    These are existing debts, not additional requests or assumed successes.
    Missing clocks remain unknown; never turn a missing timestamp into now.
    """
    return [{
        'first_at_utc': (now + timedelta(seconds=row.get('max_retry_wait_seconds', 0))).isoformat(),
        'interval_seconds': 0, 'requests': row['independent_retry_requests'],
        'session_only': False, 'existing_retry_debt': True,
    } for row in workload['datasets'] if row.get('independent_retry_requests', 0)
        and row.get('independent_retry_clock_known') is True]


def _serial_scope(scope: dict[str, Any]) -> dict[str, Any]:
    """Remove only proven independent retry debt from the predecessor lane."""
    result = deepcopy(scope)
    summary = result['summary']
    requests = summary['independent_retry_requests']
    for field in ('required_requests', 'backfill_requests', 'current_plan_requests',
                  'unbatched_requests', 'fastest_requests'):
        summary[field] -= requests
        if summary[field] < 0:
            raise ValueError('independent_retry_debt_must_be_subset_of_core_work')
    summary['retry_tasks'] = max(0, summary['retry_tasks'] - summary['independent_retry_tasks'])
    for field in ('inflight_tasks', 'inflight_requests'):
        summary[field] -= summary['independent_inflight_tasks']
        if summary[field] < 0:
            raise ValueError('independent_inflight_must_be_subset_of_core_work')
    summary['max_retry_wait_seconds'] = summary['serial_max_retry_wait_seconds']
    return result


def stage_workloads(workload: dict[str, Any]) -> list[dict[str, Any]]:
    """Disjoint request sets in scheduler order; no full-capacity parallel ETAs."""
    buckets = {key: _summary() for key, _ in STAGES}
    for row in workload['datasets']:
        source = BY_DATASET.get(row['dataset']) or LOCAL_DERIVED_STAGES.get(row['dataset'])
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
        values['serial_max_retry_wait_seconds'] = (0 if values['independent_retry_requests']
                                                   else values['max_retry_wait_seconds'])
        for field, value in values.items():
            if field in {'max_retry_wait_seconds', 'serial_max_retry_wait_seconds'}:
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
    independent_events = _independent_retry_events(workload, now)
    if independent_events:
        timed_events = [*(timed_events or []), *independent_events]
    independent_finishes: dict[str, datetime | None] = dict.fromkeys(SCENARIOS)
    independent_conditionals: dict[str, dict[str, Any]] = {}
    independent_total = sum(row.get('independent_retry_requests', 0) or 0 for row in workload['datasets'])
    independent_core_cost = dict.fromkeys(SCENARIOS, 0)
    independent_serviced = dict.fromkeys(SCENARIOS, 0)
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
        row = next((row for row in workload['datasets'] if row['dataset'] == scope['dataset']), {})
        object_calls = min(row.get('object_requests', 0), scope['summary']['current_plan_requests'])
        if object_calls:
            measured = telemetry.get('object_transfer_statistics', {}).get(scope['dataset'], {})
            total = scope['summary']['current_plan_requests']
            paced = stage_telemetry['quota'].get('paced_requests_per_hour')
            rates = {}
            if paced and measured:
                for scenario, field in (('fastest', 'fastest_seconds'), ('central', 'median_seconds'), ('slowest', 'p90_seconds')):
                    if scenario != 'fastest' and measured.get('dependent_processing_unknown'):
                        continue
                    seconds = object_calls * measured[field] + (total-object_calls) * 3600 / paced
                    rates[scenario] = total * 3600 / seconds if seconds > 0 else None
            stage_telemetry['request_processing_rates'] = {
                'rates': rates, 'object_requests': object_calls, 'samples': measured.get('samples', 0),
                'dependent_processing_samples': measured.get('dependent_processing_samples', 0),
                'dependent_processing_unknown': measured.get('dependent_processing_unknown', False),
                'basis': 'serial_object_and_mandatory_local_processing_plus_separate_legacy_calls',
                'without_samples': 'quota_only_fastest_lower_bound_central_and_slowest_unknown',
            }
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
        rates = item['rate_evidence']
        priority_cap = _min_rate(rates['official_requests_per_hour'], rates['paced_requests_per_hour'])
        priority_middle = _min_rate(priority_cap, rates['gross_requests_per_hour'])
        priority_slow = _min_rate(priority_middle, rates['dispatch_rate_p10'])
        priority_rates = dict(zip(SCENARIOS, (priority_cap, priority_middle, priority_slow), strict=True))
        independent_requests = scope['summary']['independent_retry_requests']
        independent_inflight = scope['summary']['independent_inflight_tasks']
        independent_work = bool(independent_requests or independent_inflight)
        serial_scope = _serial_scope(scope) if independent_work else scope
        serial = (estimate_completion(serial_scope, stage_telemetry, now,
                  day_is_protected=day_is_protected, secondary_admission=admission, _project_calendar=False)
                  if independent_work else item)
        for name, scenario in item['scenarios'].items():
            cursor = cursors[name]
            base_requests = scenario['request_count']
            rate = scenario['effective_requests_per_hour']
            standalone = scenario['active_work_seconds']
            duration = serial['scenarios'][name]['active_work_seconds']
            independent_cost = math.ceil(independent_requests * 3600 / rate) if independent_requests and rate else 0
            if independent_requests:
                independent_core_cost[name] = independent_cost
            # Preserve measured own-stage work even if a predecessor lacks cost
            # evidence. It is not an absolute deadline or a global duration.
            scenario['standalone_active_work_seconds'] = standalone
            if independent_requests and rate and len(independent_events) == sum(
                    bool(row.get('independent_retry_requests')) for row in workload['datasets']):
                wait = max((datetime.fromisoformat(event['first_at_utc']) for event in independent_events), default=now)
                try:
                    independent_finishes[name], _ = scheduled_finish(
                        wait, independent_cost, day_is_protected=day_is_protected)
                except ValueError:
                    independent_finishes[name] = None
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
            unserved_cost = (independent_cost * max(0, 1 - independent_serviced[name] / independent_total)
                             if independent_total else 0)
            cumulative_work[name] = (cumulative_work[name] + duration + unserved_cost
                                     if cumulative_work[name] is not None and duration is not None else None)
            cumulative_requests[name] = (cumulative_requests[name] + scenario['request_count']
                                         if cumulative_requests[name] is not None and accrued is not None
                                         and scenario['request_count'] is not None else None)
            scenario['cumulative_active_work_seconds'] = cumulative_work[name]
            scenario['cumulative_request_count'] = cumulative_requests[name]
            waiting = serial['scenarios'][name]['state'] in {'waiting_admission', 'waiting_quota', 'waiting_retry'}
            scenario.update(state=scenario['state'] if waiting else 'unknown',
                            remaining_seconds=None, estimated_complete_at_utc=None,
                            opening_pause_seconds=None)
            if cursor is not None and duration is not None and not waiting:
                cooldown = serial_scope['summary']['max_retry_wait_seconds']
                cooldown = cooldown if scenario['request_count'] else 0
                earliest_retry = now + timedelta(seconds=cooldown)
                start = max(cursor, earliest_retry) if name == 'slowest' else cursor
                try:
                    serial_requests = max(0, scenario['request_count'] - independent_requests)
                    if timed_events is not None and rate and serial_requests:
                        event_counts = {}
                        finish, pause, refresh = release_aware_finish(
                            start, serial_requests, rate, timed_events,
                            day_is_protected=day_is_protected, event_counts=event_counts,
                            priority_rate=priority_rates[name])
                        extra = math.ceil(refresh * 3600 / priority_rates[name])
                        duration += extra
                        existing_debt = sum(count for index, count in event_counts.items()
                                            if timed_events[index].get('existing_retry_debt'))
                        # Existing Free debt was included once in core. Charging
                        # capacity when it releases must not count it a second time.
                        already_accounted = (independent_core_cost[name] * existing_debt / independent_total
                                             if independent_total else 0)
                        cumulative_work[name] += extra - already_accounted
                        independent_serviced[name] += existing_debt
                        new_refresh = refresh - existing_debt
                        cumulative_requests[name] += new_refresh
                        scenario['request_count'] += new_refresh
                        scenario.update(active_work_seconds=duration, forecast_refresh_requests=refresh,
                                        cumulative_request_count=cumulative_requests[name],
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
            if independent_work:
                item['scheduling_projection'] = {
                    'independent_retry_requests': independent_requests,
                    'independent_inflight_tasks': independent_inflight,
                    'basis': 'independent_worker_retry_does_not_block_history_lane'}
                scenario['active_work_seconds'] = standalone
                # Actual debt is unresolved. Only the explicit retry-success
                # projection can have a date; the serial cursor remains separate.
                tail = independent_finishes[name]
                candidate = dict(scenario)
                if tail is not None and cursors[name] is not None:
                    finish = max(tail, cursors[name])
                    candidate.update(state='estimated', remaining_seconds=math.ceil((finish-now).total_seconds()),
                                    estimated_complete_at_utc=finish.isoformat(), stage_start_at_utc=now.isoformat(),
                                    stage_duration_seconds=math.ceil((finish-now).total_seconds()))
                else:
                    candidate.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None,
                                     stage_start_at_utc=None, stage_duration_seconds=None)
                independent_conditionals.setdefault(key, {})[name] = candidate
                if _retry_projection:
                    scenario.update(candidate)
                else:
                    scenario.update(state=item['state'], remaining_seconds=None, estimated_complete_at_utc=None,
                                    stage_start_at_utc=None, stage_duration_seconds=None)
        if timed_events is not None:
            item['rate_evidence'].update(
                scheduling_basis='release_clock_events',
                priority_requests_per_hour=priority_rates['central'],
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
        overall['assumptions'].append('追新按現有佇列首次到期與成功續期規律，向上取整至分鐘；小型追新採共用帳號實發速度代理，大型來源採下載＋本機處理成本，不把每次追新當大型檔。低優先歷史重驗與新分區仍為階段平均模型。')
    cash_calendars = forecast.get('new_partition_calendars', {})
    if any(value.get('state') == 'modeled_from_verified_cash_year' for value in cash_calendars.values()):
        overall['assumptions'].insert(0, '現貨新分區依最近一年已驗證實際場次密度推算，非未來精確開休市表；期貨夜盤／海外未知日曆仍用每日上側模型。')
    overall['scenarios'] = deepcopy(stages[-1]['scenarios'])
    runtime_budget = telemetry['quota'].get('current_budget', {})
    if (timed_events is not None and runtime_budget.get('priority_wait')
            and runtime_budget.get('schedule_verified', True) and runtime_budget.get('remaining', 0) > 0):
        overall['blockers'] = [row for row in overall['blockers'] if row['code'] != 'quota_pause']
    for name, scenario in overall['scenarios'].items():
        tail = independent_finishes[name]
        if _retry_projection and workload['summary'].get('independent_retry_requests'):
            finish = datetime.fromisoformat(scenario['estimated_complete_at_utc']) if scenario.get('estimated_complete_at_utc') else None
            if finish and tail:
                finish = max(finish, tail)
                scenario.update(remaining_seconds=math.ceil((finish-now).total_seconds()),
                                estimated_complete_at_utc=finish.isoformat())
            else:
                scenario.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None)
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
        scenario['standalone_active_work_seconds'] = None  # Never expose last-stage work as global work.
    if workload['summary'].get('independent_retry_requests') or workload['summary'].get('independent_inflight_tasks'):
        overall['scheduling_projection'] = {
            'independent_retry_requests': workload['summary'].get('independent_retry_requests', 0),
            'independent_inflight_tasks': workload['summary'].get('independent_inflight_tasks', 0),
            'basis': 'independent_worker_retry_does_not_block_history_lane'}
        overall['assumptions'].insert(0, 'Free 的來源重試與 Complement 歷史佇列獨立；不作為九類歷史的前置閘門，到期仍計入同一帳號容量。未知後續錯誤不提供保證完工日期。')
    current = next((item for item in stages if (item['workload']['planned_requests'] or 0)
                    > (item['workload'].get('independent_retry_requests') or 0)), stages[0])
    overall['rate_evidence'] = {**current['rate_evidence'], 'stage_key': current['key'],
                                'stage_label': current['scope_label'], 'overall_rate_basis': 'stage_weighted'}
    # Preserve the old API keys, now with actual predecessor time included.
    milestone_indices = (
        ('core', 1, '累計：指定優先回補＋主要歷史／新聞'),
        ('non_tick', max(index + 2 for index, stage in enumerate(HISTORY_STAGES) if stage.family == 'detail'),
         '累計：至全部非 tick 完成（美股分鐘最後，含先行 tick）'),
        ('all', len(stages) - 1, '累計：全部已知工作（含校驗）'))
    overall['milestones'] = {key: _cumulative_milestone(stages, index, label)
                             for key, index, label in milestone_indices}
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
    if workload['summary'].get('independent_inflight_tasks') and not workload['summary']['current_plan_requests']:
        overall['state'] = 'warming_up'
        for item in (overall, *overall['milestones'].values()):
            if item['workload']['inflight_tasks']:
                item['state'] = 'warming_up'
                for scenario in item['scenarios'].values():
                    scenario.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None,
                                    active_work_seconds=None)
    if not _retry_projection and workload['summary'].get('retry_tasks', 0):
        independent_only = (workload['summary']['retry_tasks']
                            == workload['summary'].get('independent_retry_tasks', 0))
        if independent_only:
            # Only the independent tail's success assumption differs. The same
            # serial history schedule already charged its release/quota event;
            # do not walk months of release events a second time each minute.
            projection = deepcopy(overall)
            for item in projection['stages']:
                if item['key'] in independent_conditionals:
                    item['scenarios'] = independent_conditionals[item['key']]
                    item['workload']['retry_tasks'] = 0
                    item['blockers'] = [row for row in item['blockers'] if row['code'] != 'retry_tasks']
                    item['state'] = 'conditional'
            for name, scenario in projection['scenarios'].items():
                finish = datetime.fromisoformat(scenario['estimated_complete_at_utc']) if scenario.get('estimated_complete_at_utc') else None
                tail = independent_finishes[name]
                if finish and tail:
                    finish = max(finish, tail)
                    scenario.update(remaining_seconds=math.ceil((finish-now).total_seconds()),
                                    estimated_complete_at_utc=finish.isoformat())
                else:
                    scenario.update(state='unknown', remaining_seconds=None, estimated_complete_at_utc=None)
            projection['milestones'] = {key: _cumulative_milestone(projection['stages'], index, label)
                                       for key, index, label in milestone_indices}
        if overall['state'] == 'waiting_retry':
            for scenario in overall['scenarios'].values():
                scenario.update(state='waiting_retry', remaining_seconds=None, estimated_complete_at_utc=None)
        # The actual forecast remains blocked; this is explicitly a separate
        # counterfactual, NOT a successful receipt or a retry completion rate.
        # The real debt/counts/cooldowns are unchanged. Unknown quota/admission
        # and unmeasured in-flight tails still cannot gain finite deadlines.
        if not independent_only:
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
