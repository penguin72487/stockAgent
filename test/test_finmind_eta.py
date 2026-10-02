from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.finmind_eta import estimate_completion, scheduled_finish
from stockagent.live.finmind_eta_projection import public_completion_estimate


NOW = datetime(2026, 9, 27, 4, tzinfo=UTC)


def evidence():
    work = {'state': 'observed', 'summary': {
        'current_plan_requests': 6000, 'unbatched_requests': 12000,
        'required_requests': 5900, 'validation_requests': 100,
        'blocked_tasks': 13, 'unknown_datasets': 0, 'unscheduled_datasets': 17,
        'inflight_tasks': 4, 'local_derived_tasks': 10,
    }}
    telemetry = {'quota': {'official_requests_per_hour': 6000, 'paced_requests_per_hour': 5900,
                          'reserved_requests_per_hour': 100, 'account_fresh': True},
                 'traffic': {'windows': {'1h': {'attempts': 5600, 'wall_requests_per_hour': 5600,
                                               'complete_window': True}},
                             'rate_distributions': {'24h': {'p50': 5600, 'p10': 5100, 'wall_p10': 5100,
                                                          'active_bin_count': 96, 'idle_bin_count': 0}}}}
    return work, telemetry


def estimate(work=None, telemetry=None):
    a, b = evidence()
    return estimate_completion(work or a, telemetry or b, NOW, day_is_protected=lambda _: False,
                               secondary_admission={'allowed': True})


@pytest.mark.parametrize('admission', [None, {'allowed': False}])
def test_blocked_or_unknown_admission_has_work_duration_but_no_completion_clock(admission):
    work, telemetry = evidence()
    result = estimate_completion(work, telemetry, NOW, secondary_admission=admission)
    assert result['state'] == 'waiting_admission'
    for row in result['scenarios'].values():
        assert row['active_work_seconds'] > 0
        assert row['remaining_seconds'] is None
        assert row['estimated_complete_at_utc'] is None


def test_quota_pause_does_not_start_eta_from_now():
    work, telemetry = evidence()
    telemetry['quota']['current_budget'] = {'allowed': False}
    result = estimate(work, telemetry)
    assert result['state'] == 'waiting_quota'
    assert all(row['estimated_complete_at_utc'] is None for row in result['scenarios'].values())


def test_public_waiting_projection_drops_old_finish_but_keeps_active_work(tmp_path):
    payload = estimate()
    payload['state'] = 'waiting_admission'
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': payload}))
    result = public_completion_estimate(tmp_path, NOW)
    assert result['state'] == 'waiting_admission'
    assert result['scenarios']['fastest']['active_work_seconds'] > 0
    assert all(row['estimated_complete_at_utc'] is None for row in result['scenarios'].values())


def test_scenarios_share_one_quota_and_preserve_uncertain_tail():
    result = estimate()
    a, b, c = result['scenarios'].values()
    assert [r['effective_requests_per_hour'] for r in (a, b, c)] == [5800, 5500, 5000]
    assert a['remaining_seconds'] <= b['remaining_seconds'] <= c['remaining_seconds']
    assert c['request_count'] == 12000
    assert result['absolute_slowest_seconds'] is None and not result['is_guaranteed']
    assert {row['code'] for row in result['blockers']} >= {'blocked_tasks', 'unscheduled_datasets', 'unmeasured_tail'}


@pytest.mark.parametrize('rate', [None, 0, float('nan'), float('inf'), -1])
def test_invalid_observed_rate_is_not_zero_time(rate):
    work, telemetry = evidence()
    telemetry['traffic']['rate_distributions']['24h'].update(p50=rate, p10=rate)
    telemetry['traffic']['windows']['1h']['wall_requests_per_hour'] = rate
    result = estimate(work, telemetry)
    assert result['state'] == 'warming_up'
    assert result['scenarios']['central']['remaining_seconds'] is None


def test_partial_scope_is_labeled_and_never_claims_full_completion():
    work, telemetry = evidence()
    work['state'] = 'partial'
    work['summary']['unknown_datasets'] = 2
    result = estimate(work, telemetry)
    assert result['state'] == 'conditional'
    assert '小計' in result['scope_label']
    assert any(item['code'] == 'unknown_datasets' for item in result['blockers'])


def test_stale_account_invalidates_estimates():
    work, telemetry = evidence()
    telemetry['quota']['account_fresh'] = False
    result = estimate(work, telemetry)
    assert result['state'] == 'unavailable'
    assert all(row['remaining_seconds'] is None for row in result['scenarios'].values())


def test_zero_network_with_inflight_is_not_current():
    work, telemetry = evidence()
    work['summary'].update(current_plan_requests=0, unbatched_requests=0)
    result = estimate(work, telemetry)
    assert result['state'] != 'current'
    assert all(row['remaining_seconds'] is None for row in result['scenarios'].values())


def test_known_cooldown_is_not_ignored():
    work, telemetry = evidence()
    work['summary']['max_retry_wait_seconds'] = 10000
    result = estimate(work, telemetry)
    assert all(row['remaining_seconds'] > 10000 for row in result['scenarios'].values())


def test_failed_debt_never_gets_a_finish_from_other_datasets_dispatch_rate(tmp_path):
    work, telemetry = evidence()
    work['summary'].update(current_plan_requests=100, unbatched_requests=100, retry_tasks=100,
                           max_retry_wait_seconds=900)
    result = estimate(work, telemetry)
    assert result['state'] == 'waiting_retry'
    assert result['retry_wait_seconds'] == 900
    assert all(row['active_work_seconds'] > 0 for row in result['scenarios'].values())
    assert all(row['estimated_complete_at_utc'] is None for row in result['scenarios'].values())
    assert result['workload']['retry_tasks'] == 100
    write_snapshot(tmp_path, result)
    public = public_completion_estimate(tmp_path, NOW)
    assert public['state'] == 'waiting_retry'
    assert public['workload']['retry_tasks'] == 100
    assert public['blockers'][-1]['code'] == 'retry_tasks'


def test_fresh_snapshot_past_deadline_is_overdue_not_one_minute_or_complete(tmp_path):
    payload = estimate()
    payload['scenarios']['central'].update(remaining_seconds=60,
        estimated_complete_at_utc=(NOW + timedelta(seconds=60)).isoformat())
    write_snapshot(tmp_path, payload)
    result = public_completion_estimate(tmp_path, NOW + timedelta(seconds=120))
    assert result['state'] != 'stale'
    assert result['scenarios']['central']['state'] == 'overdue'
    assert result['scenarios']['central']['remaining_seconds'] is None
    assert result['scenarios']['central']['estimated_complete_at_utc'] is None


def test_opening_break_and_official_holiday():
    start = datetime(2026, 9, 28, 0, tzinfo=UTC)  # Taipei 08:00
    finish, pause = scheduled_finish(start, 3600, day_is_protected=lambda _: True)
    assert finish == start + timedelta(seconds=6600) and pause == 3000
    finish, pause = scheduled_finish(start, 3600, day_is_protected=lambda _: False)
    assert finish == start + timedelta(hours=1) and pause == 0


def test_long_horizon_converges_and_naive_datetime_rejected():
    finish, pause = scheduled_finish(NOW, 300 * 86400, day_is_protected=lambda _: True)
    assert finish > NOW + timedelta(days=300) and pause > 0
    with pytest.raises(ValueError):
        scheduled_finish(NOW.replace(tzinfo=None), 1, day_is_protected=lambda _: False)
    finish, _ = scheduled_finish(NOW, 800 * 86400, day_is_protected=lambda _: False)
    assert finish == NOW + timedelta(days=800)


def test_recent_full_hour_and_future_load_replace_old_idle_rates_and_backlog_reserve():
    work, telemetry = evidence()
    telemetry['traffic']['rate_distributions']['1h'] = {'complete_bin_count':4, 'active_bin_count':4,
                                                       'p50':5200, 'p10':4000, 'wall_p10':4000}
    telemetry['traffic']['windows']['1h'].update(attempts=5000, wall_requests_per_hour=5000)
    telemetry['quota'].update(reserved_requests_per_hour=6000, forecast_recurring_requests_per_hour=1000)
    result = estimate(work, telemetry)
    assert result['rate_evidence']['observation_window'] == 'rolling_60m'
    assert result['scenarios']['central']['effective_requests_per_hour'] == 4000
    assert result['scenarios']['central']['remaining_seconds'] == 5400


def test_incomplete_hour_never_silently_falls_back_to_active_bin_median():
    work, telemetry = evidence()
    telemetry['traffic']['windows']['1h']['complete_window'] = False
    result = estimate(work, telemetry)
    assert result['scenarios']['central']['remaining_seconds'] is None
    assert result['scenarios']['fastest']['remaining_seconds'] > 0


def test_conservative_idle_bins_are_not_removed():
    work, telemetry = evidence()
    telemetry['traffic']['rate_distributions']['24h'].update(wall_p10=0, idle_bin_count=15)
    result = estimate(work, telemetry)
    assert result['scenarios']['central']['remaining_seconds'] > 0
    assert result['scenarios']['slowest']['remaining_seconds'] is None


def test_reserved_quota_is_not_total_worker_stop_when_incremental_work_continues():
    work, telemetry = evidence()
    work['summary']['incremental_requests'] = 100
    telemetry['quota']['current_budget'] = {'allowed':False, 'remaining':100}
    assert estimate(work, telemetry)['state'] == 'conditional'


def write_snapshot(tmp_path, payload):
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': payload,
                                                       'private_evidence': {'token': 'secret-marker'}}))


def test_projection_is_allowlisted_and_contains_all_three_scenarios(tmp_path):
    payload = estimate()
    payload['token'] = 'secret-marker'
    write_snapshot(tmp_path, payload)
    result = public_completion_estimate(tmp_path, NOW)
    assert result['state'] == 'conditional'
    assert len(result['scenarios']) == 3
    assert result['scenarios']['central']['remaining_seconds'] == payload['scenarios']['central']['remaining_seconds']
    assert 'secret-marker' not in json.dumps(result)


@pytest.mark.parametrize('offset', [301, -61])
def test_stale_or_future_projection_cannot_present_completion_time(tmp_path, offset):
    write_snapshot(tmp_path, estimate())
    result = public_completion_estimate(tmp_path, NOW + timedelta(seconds=offset))
    assert result['state'] == 'stale'
    assert all(row['remaining_seconds'] is None for row in result['scenarios'].values())


@pytest.mark.parametrize('body', ['[]', '{bad', '{"schema_version":1,"estimate":[]}'])
def test_invalid_snapshot_is_unavailable(tmp_path, body):
    (tmp_path / 'eta_status.json').write_text(body)
    result = public_completion_estimate(tmp_path, NOW)
    assert result['state'] == 'unavailable'
    assert all(row['remaining_seconds'] is None for row in result['scenarios'].values())


def test_inconsistent_finish_is_rejected(tmp_path):
    payload = estimate()
    payload['scenarios']['central']['estimated_complete_at_utc'] = NOW.isoformat()
    write_snapshot(tmp_path, payload)
    assert public_completion_estimate(tmp_path, NOW)['scenarios']['central']['remaining_seconds'] is None
