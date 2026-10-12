"""ETA uses all consecutive measured time, not fastest/startup/old-owner rates."""
from copy import deepcopy
from datetime import timedelta

import pytest

from stockagent.live.news_dashboard import _completion_forecast, build_news_public_status
from test_news_dashboard import NOW, _save, _seed


def samples(counts=(20, 50, 80), seconds=50, offset=0):
    end = NOW.timestamp() - offset
    return [{'ended_at_epoch': end - (len(counts) - index - 1) * seconds,
             'owner_id': 'a' * 32,
             'wall_seconds': seconds, 'new_stored_url_heads': count,
             'new_heads_per_second': 1e15,  # Never trust redundant rate fields.
             'healthy': index == 0, 'private_reason': 'PRIVATE_REASON'}
            for index, count in enumerate(counts)]


def project(history=None, *, elapsed=1200, heads=150, live=True, clock=NOW, observed=NOW, **changes):
    history = samples() if history is None else history
    calibration = {'measurement_contract_version': 10, 'history': history,
                   'completion_window_owner_contract_version': 1,
                   'last_window': history[-1] if history else None, **changes}
    metrics = {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': clock.isoformat(),
               'owner_elapsed_seconds': elapsed, 'owner_id': 'a' * 32}
    return _completion_forecast(metrics, calibration, heads, observed, NOW, live, current_owner='a' * 32)


def test_recent_rate_includes_unhealthy_windows_and_does_not_trust_fastest_rate():
    result = project()
    assert result['new_url_heads_per_second'] == 1
    assert result['sample_seconds'] == 150 and result['sample_count'] == 3
    assert result['state'] == 'recent_measured' and result['new_url_heads'] == 150
    assert result['includes_failures_waits_and_gaps'] and result['cached_heads_excluded']
    assert result['current_owner_clock_bound'] and not result['all_history_complete']
    assert 'PRIVATE' not in str(result)


def test_recent_eta_and_display_rate_are_separate_from_owner_startup_average(tmp_path):
    status = _seed(tmp_path)
    status['providers']['cnyes']['states'] = {'complete': 200, 'pending': 1200}
    status['owner_id'] = 'a' * 32
    status.update(unique_articles=1404, articles_with_stored_body=200,
                  states={'complete': 200, 'pending': 1200, 'awaiting_authorization': 4})
    status['performance'].update(owner_elapsed_seconds=1200, snapshot_clock_contract_version=1, owner_id='a' * 32,
                                 snapshot_at_utc=NOW.isoformat())
    status['performance']['providers']['cnyes']['counts']['durably_accepted_new_url_heads'] = 150
    history = samples()
    status['http_capacity'] = {'calibration': {'measurement_contract_version': 10, 'completion_window_owner_contract_version': 1,
        'providers': {'cnyes': {'history': history, 'last_window': history[-1]}}}}
    _save(tmp_path, status)
    result = build_news_public_status(tmp_path, now=NOW)
    assert result['health'] == 'running'
    source = next(row for row in result['providers'] if row['id'] == 'cnyes')
    assert source['owner_articles_per_second'] == 0.125
    assert source['completion_forecast']['new_url_heads_per_second'] == 1
    assert source['conditional_eta_seconds'] == 1200
    assert source['conditional_finish_at_utc'] == (NOW + timedelta(seconds=1200)).isoformat()
    assert source['completion_forecast_rate_basis'] == 'recent_independent_new_url_heads'
    assert all(row['completion_forecast'] is None for row in result['providers'] if not row['enabled'])
    assert result['acquisition']['all_sources_eta_seconds'] is None
    assert 'PRIVATE_REASON' not in str(result)
    status['provider_next_attempt_utc']['cnyes'] = (NOW + timedelta(seconds=600)).isoformat()
    _save(tmp_path, status)
    source = next(row for row in build_news_public_status(tmp_path, now=NOW)['providers'] if row['id'] == 'cnyes')
    assert source['conditional_eta_seconds'] is None and source['state'] == 'cooldown'


def test_zero_progress_is_measured_zero_but_not_infinite_or_completed_eta(tmp_path):
    result = project(samples(counts=(0, 0, 0)))
    assert result['state'] == 'no_recent_new_heads'
    assert result['new_url_heads_per_second'] == 0 and result['new_url_heads'] == 0


@pytest.mark.parametrize('counts', [(5,), (1, 2, 3), (0,)])
def test_small_samples_remain_explicitly_unestimated(counts):
    result = project(samples(counts=counts))
    assert result['state'] == 'collecting_recent_samples'
    assert result['sample_count'] == len(counts)
    assert result['new_url_heads_per_second'] is None


@pytest.mark.parametrize('elapsed,expected_count', [(20, 0), (65, 1), (105, 2)])
def test_saved_windows_wholly_before_current_owner_are_not_reused(elapsed, expected_count):
    result = project(elapsed=elapsed)
    assert result['sample_count'] == expected_count
    assert (result['new_url_heads_per_second'] is not None) == (expected_count == 2)


@pytest.mark.parametrize('owner', [None, 'b' * 32, '', True, 'not-an-owner'])
def test_other_owner_or_legacy_windows_cannot_forecast_even_with_plausible_clocks(owner):
    history = samples()
    for sample in history:
        sample['owner_id'] = owner
    result = project(history)
    assert result['state'] == 'collecting_recent_samples'
    assert result['sample_count'] == 0 and result['new_url_heads_per_second'] is None


def test_current_windows_stop_at_owner_boundary_without_discarding_observed_time():
    history = samples()
    history[0]['owner_id'] = 'b' * 32
    result = project(history)
    assert result['sample_count'] == 2 and result['sample_seconds'] == 100
    assert result['new_url_heads_per_second'] == 1.3


def test_metric_owner_must_equal_the_status_owner():
    history = samples()
    calibration = {'measurement_contract_version': 10, 'completion_window_owner_contract_version': 1,
                   'history': history, 'last_window': history[-1]}
    metrics = {'snapshot_clock_contract_version': 1, 'snapshot_at_utc': NOW.isoformat(),
               'owner_elapsed_seconds': 1200, 'owner_id': 'a' * 32}
    result = _completion_forecast(metrics, calibration, 150, NOW, NOW, True, current_owner='b' * 32)
    assert result['state'] == 'unavailable' and result['new_url_heads_per_second'] is None


@pytest.mark.parametrize('version', [None, True, 0, 2, '1'])
def test_unverified_window_owner_contract_is_an_explicit_legacy_fallback(version):
    assert project(completion_window_owner_contract_version=version) is None


def test_small_window_gap_is_counted_in_denominator_not_silently_skipped():
    history = samples(counts=(60, 90))
    history[0]['ended_at_epoch'] -= 4
    result = project(history)
    assert result['sample_seconds'] == 104
    assert result['new_url_heads_per_second'] == 150 / 104


def test_large_gap_cannot_bridge_an_unobserved_reset():
    history = samples(counts=(60, 90))
    history[0]['ended_at_epoch'] -= 8
    result = project(history)
    assert result['sample_count'] == 1 and result['new_url_heads_per_second'] is None


def test_overlapping_windows_are_not_added_twice():
    history = samples()
    history[0]['ended_at_epoch'] += 20
    assert project(history)['state'] == 'unavailable'


def test_horizon_uses_whole_windows_not_scaled_counts():
    history = samples(counts=tuple(range(1, 17)))
    result = project(history, heads=1000)
    assert result['sample_count'] == 6 and result['sample_seconds'] == 300
    assert result['new_url_heads'] == sum(range(11, 17))


@pytest.mark.parametrize('value', [True, -1, 1.2, float('nan'), float('inf'), '80', 2**53])
def test_invalid_completed_head_count_never_becomes_progress(value):
    history = samples()
    history[-1]['new_stored_url_heads'] = value
    result = project(history)
    assert result['state'] == 'unavailable' and result['new_url_heads_per_second'] is None


@pytest.mark.parametrize('value', [True, 0, -1, 601, float('nan'), float('inf'), '50'])
def test_invalid_window_duration_never_estimates_completion(value):
    history = samples()
    history[-1]['wall_seconds'] = value
    assert project(history)['state'] == 'unavailable'


@pytest.mark.parametrize('change', ['future', 'stale', 'mismatch', 'too_many', 'overshoot', 'bad_owner_age'])
def test_bad_stale_mismatched_or_unreconciled_evidence_is_unknown(change):
    history = samples()
    options = {}
    if change == 'future':
        history[-1]['ended_at_epoch'] += 1
    elif change == 'stale':
        history = samples(offset=121)
    elif change == 'mismatch':
        options['last_window'] = deepcopy(history[-1])
        options['last_window']['new_stored_url_heads'] += 1
    elif change == 'too_many':
        history = samples(counts=(1,) * 129)
    elif change == 'overshoot':
        options['heads'] = 149
    else:
        options['elapsed'] = 1e18
    result = project(history, **options)
    assert result['new_url_heads_per_second'] is None
    assert result['state'] in {'unavailable', 'stale_recent_measurement'}


@pytest.mark.parametrize('change', ['not_live', 'future_clock', 'stale_clock', 'missing_head_count'])
def test_snapshot_clock_and_liveness_are_required(change):
    options = {}
    if change == 'not_live':
        options['live'] = False
    elif change == 'future_clock':
        options['clock'] = NOW + timedelta(seconds=1)
    elif change == 'stale_clock':
        options['clock'] = NOW - timedelta(seconds=181)
    else:
        options['heads'] = None
    result = project(**options)
    assert result['state'] == 'unavailable' and result['new_url_heads_per_second'] is None


@pytest.mark.parametrize('version', [None, True, 9, 12, '10'])
def test_unsupported_window_contract_is_only_an_explicit_legacy_fallback(version):
    assert project(measurement_contract_version=version) is None
