from datetime import timedelta
import json

import pytest

from downloader.finmind_eta_stages import ordered_estimate, stage_workloads
from stockagent.live.finmind_eta_projection import public_completion_estimate
from test_finmind_eta import NOW, evidence


def inputs():
    _, telemetry = evidence()
    telemetry['quota']['recurring_forecast'] = {
        'requests_per_hour_by_phase': {'core': 100, 'detail': 900, 'tick': 500},
        'new_partition_requests_per_hour_by_phase': {'core': 0, 'detail': 100, 'tick': 200},
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
    assert [x['key'] for x in stages] == ['priority', 'core', 'detail', 'tick', 'validation']
    assert [x['summary']['current_plan_requests'] for x in stages] == [5500, 5500, 11000, 4100, 100]
    assert stages[-1]['cumulative_planned_requests'] == 26200
    assert sum(x['summary']['unbatched_requests'] for x in stages) == 26200


def test_timed_release_never_subtracts_tomorrows_burst_from_early_stage(tmp_path):
    work, telemetry = inputs()
    forecast = telemetry['quota']['recurring_forecast']
    forecast['requests_per_hour_by_phase']['incremental'] = 1000
    forecast['timed_incremental'] = {'state': 'modeled', 'events': [{
        'first_at_utc': (NOW + timedelta(hours=2)).isoformat(), 'interval_seconds': 86400,
        'requests': 24000, 'session_only': False}]}
    result = run(work, telemetry)
    assert result['schema_version'] == 4
    first = result['stages'][0]['scenarios']['central']
    assert first['forecast_refresh_requests'] == 0
    assert first['remaining_seconds'] < 3600
    assert result['rate_evidence']['scheduling_basis'] == 'release_clock_events'
    # The later burst is real work, and delays whichever later stage spans it.
    assert sum(s['scenarios']['central'].get('forecast_refresh_requests', 0) for s in result['stages']) >= 24000
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': result}))
    public = public_completion_estimate(tmp_path, NOW)
    assert public['schema_version'] == 4


def test_phase_capacity_reconciliation_and_lower_priority_arrivals():
    result = run()
    stages = result['stages']
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


def test_empty_stages_take_no_extra_capacity_or_service_time():
    work, telemetry = inputs()
    work['datasets'] = work['datasets'][:1]
    work['summary'].update(current_plan_requests=5500, unbatched_requests=5500,
                           required_requests=5500, validation_requests=0)
    telemetry['quota']['recurring_forecast']['new_partition_requests_per_hour_by_phase'] = {}
    result = run(work, telemetry)
    assert result['scenarios']['central']['remaining_seconds'] == 3600
    assert result['stages'][0]['scenarios']['central']['remaining_seconds'] == 0


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
