from datetime import timedelta
import json

import pytest

from downloader.finmind_eta_stages import ordered_estimate, stage_workloads
from downloader.finmind_eta import SNAPSHOT_CONTRACT_VERSION
from downloader.finmind_history_order import STAGES
from stockagent.live.finmind_eta_projection import public_completion_estimate
from test_finmind_eta import NOW, evidence


def inputs():
    _, telemetry = evidence()
    telemetry['quota']['recurring_forecast'] = {
        'requests_per_hour_by_phase': {'core': 100, 'detail': 900, 'tick': 500},
        'new_partition_requests_per_hour_by_phase': {'core': 0, 'detail': 100, 'tick': 200},
        'requests_per_hour_by_stage': {'incremental': 100, 'core': 0, 'tw_futures_minute': 900,
                                       'tw_stock_tick': 500},
        'new_partition_requests_per_hour_by_stage': {'tw_futures_minute': 100, 'tw_stock_tick': 200},
    }
    rows = []
    for dataset, count in [('TaiwanStockNews', 5500), ('TaiwanFuturesKBar', 16500),
                           ('TaiwanStockPriceTick', 4100), ('TaiwanStockPrice', 100)]:
        validation = 100 if dataset == 'TaiwanStockPrice' else 0
        rows.append(dict(dataset=dataset, state='observed', current_plan_requests=count,
                         required_requests=count-validation, backfill_requests=count-validation,
                         validation_requests=validation, unbatched_requests=count, fastest_requests=count))
    rows[1]['priority_override'] = {'requests': 5500}
    work = {'state': 'observed', 'datasets': rows, 'summary': {
        'current_plan_requests': 26200, 'unbatched_requests': 26200, 'required_requests': 26100,
        'validation_requests': 100, 'unknown_datasets': 0}}
    return work, telemetry


def run(work=None, telemetry=None, admission=None):
    a, b = inputs()
    return ordered_estimate(work or a, telemetry or b, NOW, day_is_protected=lambda _: False,
                            secondary_admission=admission or {'allowed': True})


def test_stages_are_disjoint_and_override_precedes_core():
    work, _ = inputs()
    stages = stage_workloads(work)
    assert [x['key'] for x in stages] == [key for key, _ in STAGES]
    assert [x['summary']['current_plan_requests'] for x in stages] == [5500, 5500, 0, 11000, 0, 0, 4100, 0, 0, 0, 0, 100]
    assert stages[-1]['cumulative_planned_requests'] == 26200
    assert sum(x['summary']['unbatched_requests'] for x in stages) == 26200


def test_broker_local_child_stays_in_broker_phase_without_extra_calls_or_core_gate():
    work, telemetry = inputs()
    work['datasets'].append(dict(dataset='TaiwanStockTradingDailyReportSecIdAgg', state='observed',
                                local_derived_tasks=1, current_plan_requests=0, unbatched_requests=0))
    stages = stage_workloads(work)
    assert stages[1]['summary']['local_derived_tasks'] == 0
    broker = next(stage for stage in stages if stage['dataset'] == 'TaiwanStockTradingDailyReport')
    assert broker['summary']['local_derived_tasks'] == 1
    assert sum(stage['summary']['current_plan_requests'] for stage in stages) == 26200
    result = run(work, telemetry)
    assert result['stages'][1]['scenarios']['central']['estimated_complete_at_utc'] is not None
    # A zero-request local tail is still not completed, and gates later phases.
    projected = next(stage for stage in result['stages'] if stage['dataset'] == broker['dataset'])
    assert projected['scenarios']['central']['estimated_complete_at_utc'] is None


def test_retry_work_and_cooldowns_follow_their_actual_stage_not_whole_dataset():
    work, telemetry = inputs()
    work['datasets'][0].update(incremental_requests=20, backfill_requests=5480,
        retry_tasks=30, retry_tasks_by_class={'incremental': 20, 'backfill': 10},
        retry_wait_seconds_by_class={'incremental': 60, 'backfill': 900}, max_retry_wait_seconds=900)
    work['datasets'][1].update(retry_tasks=2, retry_tasks_by_class={'backfill': 2})
    work['datasets'][1]['priority_override'].update(retry_tasks=1)
    work['summary']['retry_tasks'] = 32
    stages = stage_workloads(work)
    assert [s['summary']['retry_tasks'] for s in stages] == [21, 10, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]
    assert stages[0]['summary']['max_retry_wait_seconds'] == 60
    assert stages[1]['summary']['max_retry_wait_seconds'] == 900
    result = run(work, telemetry)
    assert result['state'] == 'waiting_retry'
    assert result['milestones']['core']['state'] == 'waiting_retry'
    assert all(x['estimated_complete_at_utc'] is None for x in result['scenarios'].values())


def test_timed_release_never_subtracts_tomorrows_burst_from_early_stage(tmp_path):
    work, telemetry = inputs()
    forecast = telemetry['quota']['recurring_forecast']
    forecast['requests_per_hour_by_phase']['incremental'] = 1000
    forecast['requests_per_hour_by_stage']['incremental'] = 1000
    forecast['timed_incremental'] = {'state': 'modeled', 'events': [{
        'first_at_utc': (NOW + timedelta(hours=2)).isoformat(), 'interval_seconds': 86400,
        'requests': 24000, 'session_only': False}]}
    result = run(work, telemetry)
    assert result['schema_version'] == SNAPSHOT_CONTRACT_VERSION
    first = result['stages'][0]['scenarios']['central']
    assert first['forecast_refresh_requests'] == 0
    assert first['remaining_seconds'] < 3600
    assert result['rate_evidence']['scheduling_basis'] == 'release_clock_events'
    # The later burst is real work, and delays whichever later stage spans it.
    assert sum(s['scenarios']['central'].get('forecast_refresh_requests', 0) for s in result['stages']) >= 24000
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['schema_version'] == SNAPSHOT_CONTRACT_VERSION


def test_phase_capacity_reconciliation_and_lower_priority_arrivals():
    result = run()
    stages = [item for item in result['stages'] if item['key'] in
              {'priority', 'core', 'tw_futures_minute', 'tw_stock_tick', 'validation'}]
    assert [x['rate_evidence']['future_recurring_requests_per_hour'] for x in stages] == [100, 100, 1000, 1500, 1500]
    assert [x['scenarios']['central']['effective_requests_per_hour'] for x in stages] == [5500, 5500, 4600, 4100, 4100]
    central = [x['scenarios']['central'] for x in stages]
    assert central[0]['remaining_seconds'] == 3600
    assert central[1]['remaining_seconds'] == 7200  # Includes finite priority predecessor.
    assert central[2]['forecast_arrival_requests'] == 200  # Two hours at 100/hour, separately modeled.
    assert central[2]['base_request_count'] == 11000
    assert central[2]['request_count'] == 11200
    assert result['rate_evidence']['rolling_requests_60m'] == 5600
    assert result['rate_evidence']['effective_requests_per_hour'] == 5500
    assert result['rate_evidence']['overall_rate_basis'] == 'stage_weighted'
    for name in ('fastest', 'central', 'slowest'):
        previous = NOW.isoformat()
        for item in stages:
            row = item['scenarios'][name]
            assert row['stage_start_at_utc'] >= previous
            assert row['estimated_complete_at_utc'] >= row['stage_start_at_utc']
            previous = row['estimated_complete_at_utc']
        assert result['scenarios'][name]['estimated_complete_at_utc'] == previous
        assert result['scenarios'][name]['request_count'] == sum(x['scenarios'][name]['request_count'] for x in stages)


def test_conditional_own_required_admission_is_satisfied_by_predecessors_only():
    admission = {'allowed': False, 'scope': 'finmind_shared_account', 'mode': 'provider_spare_quota',
                 'reason': 'finmind_required_acquisition_pending'}
    result = run(admission=admission)
    assert result['state'] == 'conditional'
    assert result['stages'][-1]['scenarios']['central']['estimated_complete_at_utc'] is not None
    admission['reason'] = 'finmind_policy_or_queue_unverified'
    result = run(admission=admission)
    assert result['state'] == 'waiting_admission'
    assert result['scenarios']['central']['estimated_complete_at_utc'] is None
    assert result['stages'][1]['scenarios']['central']['estimated_complete_at_utc'] is not None


def test_finite_override_only_competes_with_priority_zero_future_refreshes():
    work, telemetry = inputs()
    telemetry['quota']['recurring_forecast']['requests_per_hour_by_phase'].update(incremental=50)
    telemetry['quota']['recurring_forecast']['requests_per_hour_by_stage'].update(incremental=50, core=100)
    work['datasets'][0].update(incremental_requests=20, backfill_requests=5480)
    result = run(work, telemetry)
    assert result['stages'][0]['workload']['planned_requests'] == 5520
    assert result['stages'][1]['workload']['planned_requests'] == 5480
    assert result['stages'][0]['rate_evidence']['future_recurring_requests_per_hour'] == 50
    assert result['stages'][1]['rate_evidence']['future_recurring_requests_per_hour'] == 150


def test_batched_incremental_moves_its_actual_unbatched_cost_once():
    work, _ = inputs()
    work['datasets'] = [dict(dataset='TaiwanBusinessIndicator', state='observed',
                             current_plan_requests=3, required_requests=3, incremental_requests=3,
                             unbatched_requests=9, fastest_requests=1, unbatched_incremental_requests=9,
                             fastest_incremental_requests=1, batch_savings=6, uncertain_requests=8)]
    work['summary'].update(current_plan_requests=3, required_requests=3, unbatched_requests=9,
                           validation_requests=0)
    stages = stage_workloads(work)
    assert stages[0]['summary']['unbatched_requests'] == 9
    assert stages[0]['summary']['fastest_requests'] == 1
    assert stages[1]['summary']['unbatched_requests'] == 0
    assert stages[1]['summary']['fastest_requests'] == 0


def test_cooldown_overlaps_predecessor_work_instead_of_being_added_twice():
    work, telemetry = inputs()
    work['datasets'][0]['max_retry_wait_seconds'] = 1800
    result = run(work, telemetry)
    assert result['stages'][1]['scenarios']['central']['remaining_seconds'] == 7200


def test_unknown_predecessor_cannot_give_later_absolute_dates():
    work, telemetry = inputs()
    telemetry['quota']['current_budget'] = {'allowed': False}
    result = run(work, telemetry)
    assert result['state'] == 'waiting_quota'
    for item in result['stages']:
        assert all(x['estimated_complete_at_utc'] is None for x in item['scenarios'].values())


@pytest.mark.parametrize('measured', [True, False])
def test_object_stage_eta_never_uses_legacy_api_rate_without_transfer_evidence(measured):
    work, telemetry = inputs()
    row = next(row for row in work['datasets'] if row['dataset'] == 'TaiwanFuturesKBar')
    row['object_requests'] = row['current_plan_requests']
    row.pop('priority_override')
    if measured:
        telemetry['object_transfer_statistics'] = {'TaiwanFuturesKBar': {
            'samples': 3, 'fastest_seconds': 5, 'median_seconds': 10, 'p90_seconds': 20}}
    out = ordered_estimate(work, telemetry, NOW, day_is_protected=lambda _: False,
                           secondary_admission={'allowed': True})
    stage = next(stage for stage in out['stages'] if stage['dataset'] == 'TaiwanFuturesKBar')
    if measured:
        assert stage['scenarios']['central']['effective_requests_per_hour'] <= 360
        assert stage['scenarios']['fastest']['effective_requests_per_hour'] <= 720
    else:
        assert stage['scenarios']['central']['estimated_complete_at_utc'] is None
        assert stage['scenarios']['slowest']['estimated_complete_at_utc'] is None
    assert stage['rate_evidence']['request_processing']['samples'] == (3 if measured else 0)


@pytest.mark.parametrize('pending_cost', [True, False])
def test_broker_stage_waits_for_mandatory_local_processing_cost(pending_cost):
    work, telemetry = inputs()
    row = work['datasets'][1]
    row.update(dataset='TaiwanStockTradingDailyReport', object_requests=row['current_plan_requests'])
    row.pop('priority_override')
    telemetry['object_transfer_statistics'] = {'TaiwanStockTradingDailyReport': {
        'samples': 1, 'fastest_seconds': 2, 'median_seconds': 22, 'p90_seconds': 30,
        'dependent_processing_unknown': pending_cost, 'dependent_processing_samples': 0 if pending_cost else 1}}
    out = ordered_estimate(work, telemetry, NOW, day_is_protected=lambda _: False,
                           secondary_admission={'allowed': True})
    stage = next(stage for stage in out['stages'] if stage['dataset'] == 'TaiwanStockTradingDailyReport')
    if pending_cost:
        assert stage['scenarios']['central']['estimated_complete_at_utc'] is None
        assert stage['scenarios']['slowest']['estimated_complete_at_utc'] is None
    else:
        assert stage['scenarios']['central']['effective_requests_per_hour'] <= 3600 / 22


def test_object_eta_uses_json_priority_cost_not_object_cost_for_daily_bursts():
    work, telemetry = independent_inputs(history_calls=100)
    row = work['datasets'][1]
    row.update(dataset='TaiwanStockTradingDailyReport', object_requests=100)
    telemetry['object_transfer_statistics'] = {'TaiwanStockTradingDailyReport': {
        'samples': 3, 'fastest_seconds': 36, 'median_seconds': 36, 'p90_seconds': 36}}
    telemetry['quota']['recurring_forecast']['timed_incremental']['events'] = [{
        'first_at_utc': (NOW + timedelta(minutes=30)).isoformat(), 'interval_seconds': 3600,
        'requests': 200, 'session_only': False}]
    result = run(work, telemetry)
    broker = result['stages'][4]['scenarios']['central']
    assert broker['state'] == 'estimated'
    assert 3600 < broker['remaining_seconds'] < 3800
    assert broker['request_count'] == 300
    assert result['stages'][4]['rate_evidence']['priority_requests_per_hour'] == 5600


def test_empty_stages_take_no_extra_capacity_or_service_time():
    work, telemetry = inputs()
    work['datasets'] = work['datasets'][:1]
    work['summary'].update(current_plan_requests=5500, unbatched_requests=5500,
                           required_requests=5500, validation_requests=0)
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_phase'] = {}
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_stage'] = {}
    result = run(work, telemetry)
    assert result['scenarios']['central']['remaining_seconds'] == 3600
    assert result['stages'][0]['scenarios']['central']['remaining_seconds'] == 0


def test_empty_final_stage_is_not_a_completed_cumulative_milestone(tmp_path):
    work, telemetry = inputs()
    work['datasets'] = work['datasets'][:1]
    work['summary'].update(current_plan_requests=5500, unbatched_requests=5500,
                           required_requests=5500, validation_requests=0)
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_phase'] = {}
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_stage'] = {}
    result = run(work, telemetry)
    assert result['stages'][-1]['state'] == 'current'
    for key in ('core', 'non_tick', 'all'):
        milestone = result['milestones'][key]
        assert milestone['state'] == 'conditional'
        assert milestone['workload']['planned_requests'] == 5500
        assert milestone['scenarios']['central']['request_count'] == 5500
        assert milestone['scenarios']['central']['active_work_seconds'] == 3600
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['milestones']['all']['state'] == 'conditional'
    assert public['milestones']['all']['workload']['planned_requests'] == 5500


def test_milestone_counts_and_work_are_cumulative_not_last_stage_only():
    result = run()
    for key, index in (('core', 1), ('non_tick', 10), ('all', 11)):
        milestone = result['milestones'][key]
        prefix = result['stages'][:index + 1]
        assert milestone['workload']['planned_requests'] == sum(s['workload']['planned_requests'] for s in prefix)
        for name, scenario in milestone['scenarios'].items():
            assert scenario['request_count'] == sum(s['scenarios'][name]['request_count'] for s in prefix)
            assert scenario['active_work_seconds'] == sum(s['scenarios'][name]['active_work_seconds'] for s in prefix)


def test_empty_milestone_remains_current_and_unknown_predecessor_remains_waiting():
    work, telemetry = inputs()
    work['datasets'] = []
    work['summary'].update(current_plan_requests=0, unbatched_requests=0,
                           required_requests=0, validation_requests=0)
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_phase'] = {}
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_stage'] = {}
    assert all(item['state'] == 'current' for item in run(work, telemetry)['milestones'].values())
    work, telemetry = inputs()
    telemetry['quota']['current_budget'] = {'allowed': False}
    assert all(item['state'] == 'waiting_quota' for item in run(work, telemetry)['milestones'].values())


def test_invalid_priority_grain_fails_without_fabricating_request_counts():
    work, telemetry = inputs()
    work['datasets'][1]['priority_override']['unsupported_tasks'] = 1
    with pytest.raises(ValueError, match='grain'):
        run(work, telemetry)


def test_public_projection_reconciles_stages_rates_and_redacts_private_fields(tmp_path):
    result = run()
    result['rate_evidence']['token'] = 'SECRET'
    result['stages'][0]['credential'] = 'SECRET'
    result['stages'][0]['scenarios']['central']['token'] = 'SECRET'
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert 'SECRET' not in json.dumps(public)
    assert public['rate_evidence']['gross_requests_per_hour'] == 5600
    assert public['stages'][1]['scenarios']['central']['stage_start_at_utc'] == (NOW + timedelta(hours=1)).isoformat()
    assert public['milestones']['core']['scenarios']['central']['remaining_seconds'] == 7200
    stale = public_completion_estimate(tmp_path, NOW + timedelta(minutes=6))
    assert stale['state'] == 'stale'
    for item in stale['stages']:
        assert item['state'] == 'stale'
        assert all(x['stage_start_at_utc'] is None for x in item['scenarios'].values())


def independent_inputs(*, history_calls=10, clock_known=True):
    work, telemetry = inputs()
    work['datasets'] = [
        dict(dataset='TaiwanVariousIndicators5Seconds', owner='free', state='observed',
             current_plan_requests=2, required_requests=2, backfill_requests=2,
             unbatched_requests=2, fastest_requests=2, retry_tasks=2,
             independent_retry_tasks=2, independent_retry_requests=2,
             independent_retry_clock_known=clock_known, max_retry_wait_seconds=900),
        dict(dataset='TaiwanFuturesKBar', state='observed', current_plan_requests=history_calls,
             required_requests=history_calls, backfill_requests=history_calls,
             unbatched_requests=history_calls, fastest_requests=history_calls),
    ]
    work['summary'].update(current_plan_requests=2+history_calls, unbatched_requests=2+history_calls,
                           required_requests=2+history_calls, validation_requests=0, retry_tasks=2,
                           independent_retry_tasks=2, independent_retry_requests=2, max_retry_wait_seconds=900)
    telemetry['quota']['recurring_forecast'] = {
        'requests_per_hour_by_stage': {'incremental': 0, 'core': 0},
        'new_partition_requests_per_hour_by_stage': {},
        'timed_incremental': {'state': 'modeled', 'events': []},
    }
    return work, telemetry


def test_free_retry_does_not_gate_independent_history_but_global_stays_unfinished(tmp_path):
    work, telemetry = independent_inputs()
    result = run(work, telemetry)
    assert result['state'] == 'waiting_retry'
    assert all(row['estimated_complete_at_utc'] is None for row in result['scenarios'].values())
    core, futures = result['stages'][1], result['stages'][3]
    assert core['workload']['retry_tasks'] == 2
    assert core['state'] == 'waiting_retry'
    assert futures['scenarios']['central']['stage_start_at_utc'] == NOW.isoformat()
    assert futures['scenarios']['central']['remaining_seconds'] < 900
    model = result['retry_condition']['scenarios']['central']
    assert model['remaining_seconds'] >= 901
    assert model['request_count'] == 12
    assert result['milestones']['all']['retry_condition']['scenarios']['central']['remaining_seconds'] >= 901
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['scenario_projection_contract'] == 1
    assert public['scenarios']['central']['state'] == 'waiting_retry'
    assert public['stages'][3]['scenarios']['central']['state'] == 'estimated'


def test_existing_independent_retry_uses_shared_capacity_once_not_extra_backlog():
    work, telemetry = independent_inputs(history_calls=6000)
    result = run(work, telemetry)
    futures = result['stages'][3]['scenarios']['central']
    assert futures['forecast_refresh_requests'] == 2
    assert sum(item['scenarios']['central'].get('forecast_refresh_requests', 0)
               for item in result['stages']) == 2
    model = result['retry_condition']['scenarios']['central']
    assert model['request_count'] == 6002
    assert abs(model['active_work_seconds'] - 6002 * 3600 / 5600) < 3


def test_independent_retry_with_unknown_clock_has_no_made_up_full_completion():
    work, telemetry = independent_inputs(clock_known=False)
    result = run(work, telemetry)
    assert all(row['estimated_complete_at_utc'] is None for row in result['retry_condition']['scenarios'].values())
    assert all(row['estimated_complete_at_utc'] is None for row in result['milestones']['all']['retry_condition']['scenarios'].values())
    assert result['stages'][3]['scenarios']['central']['state'] == 'estimated'


def test_independent_retry_condition_reuses_the_same_capacity_projection(monkeypatch):
    from copy import deepcopy
    from downloader import finmind_eta_stages as module
    work, telemetry = independent_inputs(history_calls=6000)
    calls = []
    original = module.estimate_completion
    def observed(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(module, 'estimate_completion', observed)
    result = run(work, telemetry)
    assert len(calls) == 14  # Overall + 12 stages + one independent serial scope, not twice.
    modeled = deepcopy(work)
    modeled['summary']['retry_tasks'] = 0
    for row in modeled['datasets']:
        row['retry_tasks'] = 0
        row['retry_tasks_by_class'] = {}
    reference = module.ordered_estimate(modeled, telemetry, NOW, day_is_protected=lambda _: False,
        secondary_admission={'allowed': True}, _retry_projection=True)
    for name in ('fastest', 'central', 'slowest'):
        for field in ('remaining_seconds', 'estimated_complete_at_utc', 'active_work_seconds', 'request_count'):
            assert result['retry_condition']['scenarios'][name][field] == reference['scenarios'][name][field]


@pytest.mark.parametrize('history_calls', [0, 10])
def test_independent_inflight_is_not_a_history_gate_or_proof_of_global_completion(history_calls):
    work, telemetry = independent_inputs(history_calls=history_calls)
    work['datasets'][0].update(current_plan_requests=0, required_requests=0, backfill_requests=0,
        unbatched_requests=0, fastest_requests=0, retry_tasks=0, independent_retry_tasks=0,
        independent_retry_requests=0, independent_inflight_tasks=1, inflight_tasks=1, inflight_requests=1)
    work['summary'].update(current_plan_requests=history_calls, required_requests=history_calls,
        unbatched_requests=history_calls, retry_tasks=0, independent_retry_tasks=0,
        independent_retry_requests=0, independent_inflight_tasks=1, inflight_tasks=1, inflight_requests=1)
    result = run(work, telemetry)
    if history_calls:
        assert result['stages'][3]['scenarios']['central']['stage_start_at_utc'] == NOW.isoformat()
    else:
        assert result['state'] == 'warming_up'
        assert all(row['estimated_complete_at_utc'] is None for row in result['scenarios'].values())
        assert all(row['estimated_complete_at_utc'] is None for row in result['milestones']['all']['scenarios'].values())


def test_unknown_predecessor_preserves_measured_standalone_cost_not_deadline():
    work, telemetry = inputs()
    work['datasets'][1]['object_requests'] = 16500  # No cost sample for preceding futures objects.
    work['datasets'][1].pop('priority_override')
    result = run(work, telemetry)
    stage = result['stages'][6]['scenarios']['central']
    assert stage['estimated_complete_at_utc'] is None
    assert stage['active_work_seconds'] is None
    assert stage['standalone_active_work_seconds'] > 0
    assert all(row['standalone_active_work_seconds'] is None for row in result['scenarios'].values())
    assert all(row['standalone_active_work_seconds'] is None for row in result['milestones']['all']['scenarios'].values())
