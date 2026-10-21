"""Progress sampling is independent of capacity trials, errors and resets."""
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from downloader.news_fulltext_capacity import COMPLETION_OBSERVATION_SECONDS, PublisherCapacity
from stockagent.live.news_dashboard import _completion_forecast
from test_news_capacity import config, controller_fixture
from test_news_completion_forecast import NOW, project, samples


def owned(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    http.metrics.owner_id = 'a' * 32
    controller.tick(force=True)
    return http, controller, clock


def advance(http, controller, clock, *, heads=30, cached=0, seconds=45):
    http.metrics.count('publisher', 'durably_accepted_new_url_heads', heads)
    http.metrics.count('publisher', 'durably_accepted_cached_new_url_heads', cached)
    http.metrics.count('publisher', 'durably_accepted_versions', heads + 500)
    clock.value += seconds
    controller.tick()
    return controller.snapshot()['providers']['publisher']['completion_observation']


def test_progress_windows_continue_during_a_ten_minute_capacity_trial(config, tmp_path):
    http, controller, clock = owned(config, tmp_path)
    try:
        controller.policy.update(window_seconds=45, minimum_requests=20, maximum_window_seconds=600)
        # No adequate calibration denominator: no pressure trial can finish.
        for _ in range(8):
            observation = advance(http, controller, clock)
        assert not controller.states['publisher']['history']
        assert len(observation['history']) == 8
        assert sum(row['new_stored_url_heads'] for row in observation['history']) == 240
        now = datetime.fromtimestamp(clock.value, timezone.utc)
        result = _completion_forecast({'snapshot_clock_contract_version': 1, 'snapshot_at_utc': now.isoformat(),
            'owner_id': 'a' * 32, 'owner_elapsed_seconds': 360},
            {**controller.snapshot(), **controller.snapshot()['providers']['publisher']},
            240, now, now, True, current_owner='a' * 32)
        assert result['state'] == 'recent_measured'
        assert result['new_url_heads_per_second'] == pytest.approx(30 / 45)
        assert result['sample_seconds'] == 315
        assert result['includes_failures_waits_and_gaps']
    finally:
        http.close()


def test_zero_cached_revisions_and_cooldown_time_are_not_fresh_heads(config, tmp_path):
    http, controller, clock = owned(config, tmp_path)
    try:
        advance(http, controller, clock, heads=100, cached=100)
        http.provider_not_before['publisher'] = clock.value + 1000
        observation = advance(http, controller, clock, heads=15, cached=5)
        assert [row['new_stored_url_heads'] for row in observation['history']] == [0, 10]
        assert [row['wall_seconds'] for row in observation['history']] == [45, 45]
        # Calibration resets/circuits do not discard already measured waits.
        assert controller.states['publisher']['state'] == 'waiting_source'
        controller._reset('publisher', {}, clock.value)
        assert advance(http, controller, clock, heads=0)['last_window']['new_stored_url_heads'] == 0
    finally:
        http.close()


def test_owner_change_and_restart_never_reuse_previous_completion_counters(config, tmp_path):
    http, controller, clock = owned(config, tmp_path)
    try:
        prior = deepcopy(advance(http, controller, clock))
        http.metrics.owner_id = 'b' * 32
        assert advance(http, controller, clock, heads=50)['history'] == []
        observation = advance(http, controller, clock, heads=7)
        assert observation['last_window']['owner_id'] == 'b' * 32
        assert observation['last_window']['new_stored_url_heads'] == 7
        restarted = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert restarted.snapshot()['providers']['publisher']['completion_observation']['history'] == []
        assert prior['last_window']['owner_id'] == 'a' * 32
    finally:
        http.close()


def test_fixed_sample_clock_and_bounded_status_do_not_change_capacity_fingerprint(config, tmp_path):
    http, controller, clock = owned(config, tmp_path)
    try:
        fingerprint = controller.fingerprint
        assert advance(http, controller, clock, seconds=COMPLETION_OBSERVATION_SECONDS - 1)['history'] == []
        assert advance(http, controller, clock, seconds=1)['last_window']['new_stored_url_heads'] == 60
        for _ in range(130):
            advance(http, controller, clock, heads=0)
        full = controller.snapshot()['providers']['publisher']['completion_observation']
        heartbeat = controller.snapshot(for_status=True)['providers']['publisher']['completion_observation']
        assert len(full['history']) == 128 and len(heartbeat['history']) == 64
        assert heartbeat['history'] == full['history'][-64:]
        heartbeat['history'][0]['new_stored_url_heads'] = 999
        assert controller.completion_observations['publisher']['history'][-64]['new_stored_url_heads'] == 0
        assert controller.fingerprint == fingerprint
    finally:
        http.close()


def test_new_progress_contract_uses_independent_windows_not_stale_capacity_trials():
    progress = samples()
    result = project(samples(offset=121), completion_observation_contract_version=1,
        completion_observation={'history': progress, 'last_window': progress[-1]})
    assert result['state'] == 'recent_measured' and result['new_url_heads_per_second'] == 1


def test_progress_only_tick_does_not_rewrite_or_fsync_whole_calibration_file(config, tmp_path, monkeypatch):
    http, controller, clock = owned(config, tmp_path)
    writes = []
    try:
        monkeypatch.setattr('downloader.news_fulltext_capacity.atomic_write_json', lambda *a, **k: writes.append(a))
        observation = advance(http, controller, clock)
        assert observation['last_window']['new_stored_url_heads'] == 30
        assert not controller.states['publisher']['history']
        assert writes == []  # The existing status heartbeat owns report persistence.
    finally:
        http.close()


@pytest.mark.parametrize('bad', [None, {}, {'history': [], 'last_window': samples()[-1]},
    {'history': samples(), 'last_window': {}}, {'history': 'not a history', 'last_window': None}])
def test_broken_progress_contract_never_silently_falls_back_to_fast_capacity_samples(bad):
    result = project(completion_observation_contract_version=1, completion_observation=bad)
    assert result['new_url_heads_per_second'] is None


@pytest.mark.parametrize('version', [True, 0, 2, '1'])
def test_unknown_progress_contract_does_not_fall_back(version):
    assert project(completion_observation_contract_version=version)['state'] == 'unavailable'
