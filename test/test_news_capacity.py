from copy import deepcopy
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from downloader.news_fulltext_capacity import PublisherCapacity, capacity_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture


class Clock:
    def __init__(self):
        self.value = 10000.0

    def __call__(self):
        return self.value


def controller_fixture(config, tmp_path, *, slots=1, maximum=3):
    config["workers"] = maximum + sum(p.get("capacity_max_parallelism", p["parallelism"]) for p in config["providers"][1:])
    config["providers"][0].update(interval_seconds=1.0, parallelism=slots, capacity_max_parallelism=maximum)
    config["adaptive_rate"] = {"enabled": True, "window_seconds": 10, "minimum_requests": 2,
                               "stable_windows": 2, "probe_through_rps": 1,
                               "ladder_rps": [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30]}
    http, _ = http_fixture(config, tmp_path, {})
    clock = Clock()
    controller = PublisherCapacity(http, config, clock=clock, epoch=clock)
    http.capacity_controller = controller
    return http, controller, clock


def observe(http, controller, clock, *, requests=10, heads=10, versions=None, errors=0):
    for key, count in {"http_requests_started": requests, "http_responses_received": requests,
                       "http_status_200": requests, "durably_accepted_new_url_heads": heads,
                       "durably_accepted_versions": heads if versions is None else versions,
                       "transport_errors": errors}.items():
        http.metrics.count("publisher", key, count)
    clock.value += 10
    controller.tick()
    return controller.states["publisher"]


def test_healthy_windows_progress_across_requested_ladder(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, maximum=1)
    try:
        targets = []
        for requests, heads in ((10, 10), (20, 20), (50, 50), (100, 100), (200, 200)):
            item = observe(http, controller, clock, requests=requests, heads=heads)
            targets.append(1 / item["interval_seconds"])
        assert targets == [2, 5, 10, 20, 30]
        assert controller.snapshot()["official_maximum_claimed"] is False
    finally:
        http.close()


def test_underfilled_cadence_measures_slots_then_rolls_back_plateau(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        assert observe(http, controller, clock, requests=3, heads=10)["parallelism"] == 2
        assert observe(http, controller, clock, requests=3, heads=10)["parallelism"] == 3
        state = observe(http, controller, clock, requests=3, heads=10)
        assert state["state"] == "verifying" and state["parallelism"] == 1
        assert observe(http, controller, clock, requests=3, heads=10)["stable_samples"] == 1
        assert observe(http, controller, clock, requests=3, heads=10)["state"] == "stable_pipeline_capacity"
        assert state["last_window"]["new_stored_url_heads"] == 10
    finally:
        http.close()


def test_robots_floor_is_not_undone_by_capacity_or_refresh(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.robot_delays["publisher"] = 10
        observe(http, controller, clock, requests=3, heads=20)
        assert controller.states["publisher"]["interval_seconds"] == 10
        assert observe(http, controller, clock, requests=3, heads=20)["state"] == "official_or_learned_floor"
        http.set_client_capacity("publisher", 0.01, 3)
        assert http.effective_interval("publisher") == 10
    finally:
        http.close()


def test_429_backoff_uses_refused_runtime_cadence_and_survives_restart(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.set_client_capacity("publisher", 0.1, 2)
        with pytest.raises(NewsError, match="http_429"):
            http._response_status("publisher", 429, {"retry-after": "120"})
        assert http.rate_refusal_intervals["publisher"] == 0.2
        # Calibration is not permission to ignore a persisted source refusal.
        http.set_client_capacity("publisher", 0.01, 3)
        assert http.effective_interval("publisher") == 0.2
        path = tmp_path / "cooldowns/publisher.json"
        persisted = json.loads(path.read_text())
        persisted["not_before_epoch"] = 0
        atomic_write_json(path, persisted)
    finally:
        http.close()
    resumed, _ = http_fixture(config, tmp_path, {})
    try:
        assert resumed.hard_interval_floor("publisher") == 0.2
        resumed.set_client_capacity("publisher", 0.01, 1)
        assert resumed.effective_interval("publisher") == 0.2
    finally:
        resumed.close()


def test_zero_requests_or_only_metadata_do_not_escalate(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        clock.value += 601
        controller.tick()
        assert controller.states["publisher"]["state"] == "insufficient_samples"
        assert observe(http, controller, clock, requests=10, heads=0)["state"] == "insufficient_useful_data"
        assert http.client_intervals["publisher"] == 1
    finally:
        http.close()


def test_unhealthy_window_lowers_pressure_without_claiming_quota(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=2)
    try:
        state = observe(http, controller, clock, errors=1)
        assert state["state"] == "recovering_transport" and state["parallelism"] == 1
        assert state["interval_seconds"] == 2
        assert "not proof of rate limit" in state["reason"]
        assert observe(http, controller, clock)["state"] == "recovering_transport"
        assert observe(http, controller, clock)["state"] == "warming"
        assert http.client_intervals["publisher"] == 1
    finally:
        http.close()


def test_isolated_error_extends_sample_without_ramping_or_automatic_slowdown(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=2)
    try:
        item = observe(http, controller, clock, requests=30, heads=25, errors=1)
        assert item['state'] == 'verifying_transient_error'
        assert item['parallelism'] == 2 and item['interval_seconds'] == 1
        assert item['pending_observation']['required_completed_results'] == 100
        assert not item['history'] and item['best'] is None
        assert observe(http, controller, clock, requests=30, heads=25)['state'] == 'verifying_transient_error'
        item = observe(http, controller, clock, requests=40, heads=35)
        assert item['last_window']['requests'] == 100 and item['last_window']['wall_seconds'] == 30
        assert item['last_window']['healthy'] and 'pending_observation' not in item
    finally:
        http.close()


def test_recovering_transport_single_failure_waits_for_denominator_without_resetting_capacity(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path, slots=2)
    try:
        state = observe(http, controller, clock, requests=20, errors=2)
        assert state['state'] == 'recovering_transport'
        pressure = (state['interval_seconds'], state['parallelism'])
        state = observe(http, controller, clock, requests=30, heads=25, errors=1)
        assert state['state'] == 'recovering_transport' and state['stable_samples'] == 0
        assert state['pending_observation']['required_completed_results'] == 100
        assert (state['interval_seconds'], state['parallelism']) == pressure
        state = observe(http, controller, clock, requests=30, heads=25)
        assert state['state'] == 'recovering_transport' and state['stable_samples'] == 0
        state = observe(http, controller, clock, requests=40, heads=35)
        assert state['last_window']['requests'] == 100 and state['last_window']['healthy']
        assert state['state'] == 'recovering_transport' and state['stable_samples'] == 1
        assert 'pending_observation' not in state
        assert (state['interval_seconds'], state['parallelism']) == pressure
    finally:
        http.close()


def _stable_prior_body_capacity(http, controller, clock, *, interval=.1, slots=2, family='article_html'):
    candidate = {'interval_seconds': interval, 'parallelism': slots, 'new_heads_per_second': 10,
                 'comparable_new_heads_per_second': 10, 'healthy': True, 'workload_family': family,
                 'requests': 100, 'responses': 100, 'new_stored_url_heads': 100, 'durable_body_versions': 100,
                 'transport_or_server_errors': 0, 'unusable_http_responses': 0, 'wall_seconds': 10,
                 'http_status_counts': {'200': 100}}
    history = [{**candidate, 'ended_at_epoch': clock.value - 10 * (i + 1)} for i in range(2)]
    state = controller.states['publisher']
    state.update(best={**history[0]}, best_by_workload={family: {**history[0]}}, history=history,
                 workload_family=family)
    controller._set('publisher', 2, 1, 'recovering_transport', 'bounded transient recovery; not proof of rate limit')
    return state


def _observe_html(http, controller, clock, *, heads=100, errors=0):
    http.metrics.count('publisher', 'endpoint_article_html_requests', 100)
    http.metrics.count('publisher', 'durable_workload_article_html_new_heads', heads)
    return observe(http, controller, clock, requests=100, heads=heads, errors=errors)


def test_transport_recovery_reuses_recent_stable_pressure_and_survives_restart(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        _stable_prior_body_capacity(http, controller, clock)
        assert _observe_html(http, controller, clock)['state'] == 'recovering_transport'
        state = _observe_html(http, controller, clock)
        assert state['state'] == 'verifying' and state['interval_seconds'] == .1 and state['parallelism'] == 2
        proof = state['transport_resume_evidence']
        assert proof['recent_healthy_windows_at_exact_capacity'] == 2
        assert proof['throughput_estimate_reused'] is False and proof['provider_permission_changed'] is False
        assert http.metrics.snapshot()['providers']['publisher']['counts']['transient_recovery_prior_capacity_restored'] == 1
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states['publisher']['interval_seconds'] == .1
        assert resumed.states['publisher']['transport_resume_evidence'] == proof
        assert resumed.states['publisher']['capacity_fingerprint'] == controller.provider_fingerprints['publisher']
    finally:
        http.close()


@pytest.mark.parametrize('reason', ['expired', 'future', 'wrong_workload', 'one_window', 'metadata_only',
                                   'corrupt_counter', 'refused_window', 'response_error', 'robots', 'learned_429'])
def test_unproven_or_restricted_prior_capacity_is_not_restored(config, tmp_path, reason):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        state = _stable_prior_body_capacity(http, controller, clock)
        if reason == 'expired':
            for sample in state['history']:
                sample['ended_at_epoch'] = clock.value - controller.policy['recheck_seconds'] - 1
        elif reason == 'future':
            for sample in state['history']:
                sample['ended_at_epoch'] = clock.value + 600
        elif reason == 'wrong_workload':
            state['best_by_workload'] = {'cnyes_bulk': state['best']}
            for sample in state['history']:
                sample['workload_family'] = 'cnyes_bulk'
        elif reason == 'one_window':
            state['history'] = state['history'][:1]
        elif reason == 'metadata_only':
            for sample in state['history']:
                sample['new_stored_url_heads'] = 0
        elif reason == 'corrupt_counter':
            state['history'][0]['requests'] = True
        elif reason == 'refused_window':
            state['history'][0]['http_status_counts']['429'] = 1
        elif reason == 'response_error':
            state['history'][0]['unusable_http_responses'] = 1
        elif reason == 'robots':
            http.robot_delays['publisher'] = 5
        elif reason == 'learned_429':
            http.rate_refusal_intervals['publisher'] = .5
        assert controller._transport_resume_candidate('publisher', 'article_html') is None
        _observe_html(http, controller, clock)
        state = _observe_html(http, controller, clock)
        assert 'transport_resume_evidence' not in state
        assert state['interval_seconds'] >= http.hard_interval_floor('publisher')
        if reason not in {'robots'}:
            assert state['interval_seconds'] >= 1  # Reviewed baseline, not the unproven .1 candidate.
    finally:
        http.close()


def test_persisted_or_new_refusal_interrupts_recovery_instead_of_resuming(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        _stable_prior_body_capacity(http, controller, clock)
        _observe_html(http, controller, clock)
        deadline = clock.value + 900
        http.provider_not_before['publisher'] = deadline
        assert controller._transport_resume_candidate('publisher', 'article_html') is None
        state = _observe_html(http, controller, clock)
        assert state['state'] == 'waiting_source' and 'transport_resume_evidence' not in state
        assert http.provider_not_before['publisher'] == deadline
    finally:
        http.close()


@pytest.mark.parametrize('peak_failure', ['one_window', 'expired', 'wrong_workload', 'bad_counter', 'refused'])
def test_unqualified_peak_does_not_hide_repeated_recent_capacity(config, tmp_path, peak_failure):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        state = _stable_prior_body_capacity(http, controller, clock, interval=.5, slots=2)
        peak = {**deepcopy(state['history'][0]), 'interval_seconds': .1, 'parallelism': 3,
                'new_heads_per_second': 100, 'comparable_new_heads_per_second': 100}
        if peak_failure == 'expired':
            peak['ended_at_epoch'] = clock.value - controller.policy['recheck_seconds'] - 1
        elif peak_failure == 'wrong_workload':
            peak['workload_family'] = 'cnyes_bulk'
        elif peak_failure == 'bad_counter':
            peak['requests'] = True
        elif peak_failure == 'refused':
            peak['http_status_counts']['429'] = 1
        state['best_by_workload']['article_html'] = peak
        state['history'].append(peak)
        _observe_html(http, controller, clock)
        state = _observe_html(http, controller, clock)
        assert (state['state'], state['interval_seconds'], state['parallelism']) == ('verifying', .5, 2)
        proof = state['transport_resume_evidence']
        assert proof['contract_version'] == 3 and proof['independent_nonoverlapping_windows']
        assert proof['eligible_repeated_capacity_groups'] == 1
        assert proof['recent_healthy_windows_at_exact_capacity'] == 2
        assert not proof['throughput_estimate_reused'] and not proof['provider_permission_changed']
    finally:
        http.close()


@pytest.mark.parametrize('mutation', ['duplicate', 'overlap', 'different_slots', 'different_cadence'])
def test_resume_cannot_combine_duplicate_overlapping_or_different_pressure_windows(config, tmp_path, mutation):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        state = _stable_prior_body_capacity(http, controller, clock)
        if mutation == 'duplicate':
            state['history'][1] = deepcopy(state['history'][0])
        elif mutation == 'overlap':
            state['history'][1]['ended_at_epoch'] = state['history'][0]['ended_at_epoch'] - 1
        elif mutation == 'different_slots':
            state['history'][1]['parallelism'] = 3
        else:
            state['history'][1]['interval_seconds'] = .5
        assert controller._transport_resume_candidate('publisher', 'article_html') is None
    finally:
        http.close()


def test_resume_ranks_repeated_candidates_not_a_single_outlier(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        state = _stable_prior_body_capacity(http, controller, clock, interval=.5, slots=2)
        # Faster nominal cadence has one spectacular result but worse repeat
        # median. Actual durable throughput, not cadence or the peak, wins.
        fast = [{**deepcopy(state['history'][0]), 'interval_seconds': .1, 'parallelism': 3,
                 'ended_at_epoch': clock.value - 30 - 10 * i,
                 'new_heads_per_second': rate, 'comparable_new_heads_per_second': rate}
                for i, rate in enumerate((100, 1, 1))]
        state['history'].extend(fast)
        state['best_by_workload']['article_html'] = fast[0]
        candidate, proof = controller._transport_resume_candidate('publisher', 'article_html')
        assert (candidate['interval_seconds'], candidate['parallelism']) == (.5, 2)
        assert proof['eligible_repeated_capacity_groups'] == 2
        assert proof['ranking_median_new_heads_per_second'] == 10
    finally:
        http.close()


@pytest.mark.parametrize('stop_reason', ['second_error', 'deadline', 'refusal'])
def test_extended_window_still_recovers_or_honors_refusals(config, tmp_path, stop_reason):
    http, controller, clock = controller_fixture(config, tmp_path, slots=2)
    try:
        observe(http, controller, clock, requests=30, heads=20, errors=1)
        if stop_reason == 'refusal':
            http.metrics.count('publisher', 'http_status_403', 1)
        elif stop_reason == 'deadline':
            clock.value += 601
        item = observe(http, controller, clock, requests=20, heads=10,
                       errors=int(stop_reason == 'second_error'))
        if stop_reason == 'refusal':
            assert item['state'] == 'waiting_source'
        else:
            assert item['state'] == 'recovering_transport' and item['parallelism'] == 1
    finally:
        http.close()


def test_offline_replay_heads_cannot_establish_online_capacity(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.metrics.count('publisher', 'durably_accepted_cached_versions', 100)
        http.metrics.count('publisher', 'durably_accepted_cached_new_url_heads', 100)
        item = observe(http, controller, clock, requests=20, heads=100)
        assert item['last_window']['new_stored_url_heads'] == 0
        assert item['state'] == 'insufficient_useful_data' and item['interval_seconds'] == 1
    finally:
        http.close()


def test_checkpoint_restores_candidate_but_changed_fingerprint_restarts_search(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        observe(http, controller, clock)
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states["publisher"]["interval_seconds"] == 0.5
        changed = deepcopy(config)
        changed["adaptive_rate"]["minimum_requests"] = 3
        fresh = PublisherCapacity(http, changed, clock=clock, epoch=clock)
        assert fresh.states["publisher"]["interval_seconds"] == 1.0
        assert fresh.fingerprint != resumed.fingerprint
    finally:
        http.close()


@pytest.mark.parametrize("corrupt", [None, [], {"publisher": []}, {"publisher": {"interval_seconds": float("nan"), "parallelism": 1}}])
def test_corrupted_calibration_preserves_source_floors_and_recovers(config, tmp_path, corrupt):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        atomic_write_json(tmp_path / "rate_calibration.json", {"version": 1, "fingerprint": controller.fingerprint, "providers": corrupt})
        restored = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert restored.states["publisher"]["state"] == "warming"
    finally:
        http.close()


def test_checkpoint_cannot_restore_an_unregistered_provider(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        saved = controller.snapshot()
        saved["providers"]["unregistered"] = deepcopy(saved["providers"]["publisher"])
        atomic_write_json(tmp_path / "rate_calibration.json", saved)
        restored = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert set(restored.states) == {"publisher"}
        observe(http, restored, clock)
    finally:
        http.close()


@pytest.mark.parametrize("patch", [{"ladder_rps": [1, 1]}, {"ladder_rps": [1, 101]}, {"minimum_requests": 0},
                                  {"window_seconds": float("inf")}, {"enabled": 1}, {"unknown": 10}])
def test_invalid_capacity_policy_is_rejected(config, tmp_path, patch):
    registry = PolicyRegistry(config, base=tmp_path)
    config["adaptive_rate"] = {**patch}
    with pytest.raises(ValueError):
        capacity_config(config, registry)


def test_worker_maximum_reservations_are_checked(config, tmp_path):
    config["adaptive_rate"] = {"enabled": True}
    config["require_provider_worker_reservations"] = True
    config["workers"] = 1
    config["providers"][0]["capacity_max_parallelism"] = 3
    with pytest.raises(ValueError, match="maximum adaptive"):
        capacity_config(config, PolicyRegistry(config, base=tmp_path))


def test_official_floor_requires_source_evidence(config, tmp_path):
    config["providers"][0]["official_min_interval_seconds"] = 10
    with pytest.raises(ValueError, match="primary evidence"):
        PolicyRegistry(config, base=tmp_path)
    config["providers"][0]["official_rate_limit_evidence_url"] = "https://publisher.test/api-rules"
    registry = PolicyRegistry(config, base=tmp_path)
    assert registry.sources["publisher"].official_min_interval_seconds == 10


def test_new_heads_are_not_revisions(config, tmp_path):
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_queue import NewsQueue
    http, _ = http_fixture(config, tmp_path, {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
                                            "https://publisher.test/item": FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    http.metrics = queue.metrics
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/item"}])
        row = queue.claim_url("https://publisher.test/item", 600)
        record = fetch_article(http, row)
        queue.accept([record])
        queue.accept([record])
        counts = queue.metrics.snapshot()["providers"]["publisher"]["counts"]
        assert counts["durably_accepted_new_url_heads"] == 1
        assert counts["durably_accepted_versions"] == 2
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize("bad_state", [[], {"version": 1, "providers": []}])
def test_top_level_malformed_checkpoint_is_recoverable(config, tmp_path, bad_state):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        atomic_write_json(tmp_path / "rate_calibration.json", bad_state)
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states["publisher"]["state"] == "warming"
    finally:
        http.close()


def test_406_response_does_not_establish_healthy_throughput(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.metrics.count("publisher", "http_status_406", 10)
        state = observe(http, controller, clock, heads=1)
        assert state["state"] == "endpoint_validation_failed"
        assert state["last_window"]["healthy"] is False
        assert state["interval_seconds"] == 1
        assert state["best"] is None
    finally:
        http.close()


def test_different_workloads_do_not_share_best_body_rate(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.metrics.count("publisher", "endpoint_cnyes_headline_requests", 10)
        http.metrics.count("publisher", "durable_workload_cnyes_bulk_new_heads", 100)
        state = observe(http, controller, clock, heads=100)
        assert state["best"]["workload_family"] == "cnyes_bulk"
        http.metrics.count("publisher", "endpoint_article_html_requests", 10)
        http.metrics.count("publisher", "durable_workload_article_html_new_heads", 1)
        state = observe(http, controller, clock, heads=1)
        assert state["best"]["workload_family"] == "article_html"
        assert state["best_by_workload"]["cnyes_bulk"]["comparable_new_heads_per_second"] == 10
        assert state["best_by_workload"]["article_html"]["comparable_new_heads_per_second"] == 0.1
    finally:
        http.close()


def test_mixed_requests_and_revisions_only_do_not_escalate(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        http.metrics.count("publisher", "endpoint_article_html_requests", 5)
        http.metrics.count("publisher", "endpoint_article_pdf_requests", 5)
        http.metrics.count("publisher", "durable_workload_article_html_new_heads", 10)
        assert observe(http, controller, clock)["state"] == "insufficient_useful_data"
        assert http.client_intervals["publisher"] == 1
        assert observe(http, controller, clock, heads=0, versions=10)["state"] == "insufficient_useful_data"
        assert http.client_intervals["publisher"] == 1
    finally:
        http.close()


def test_one_provider_contract_change_preserves_independent_peer_capacity(config, tmp_path):
    from dataclasses import replace
    peer = deepcopy(config["providers"][0])
    peer.update(id="peer", domains=["peer.test"], interval_seconds=1, capacity_max_parallelism=1)
    config["providers"].append(peer)
    http, controller, clock = controller_fixture(config, tmp_path, maximum=2)
    try:
        observe(http, controller, clock)
        http.registry.sources["peer"] = replace(http.registry.sources["peer"], data_fingerprint="changed")
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states["publisher"]["interval_seconds"] == 0.5
        assert resumed.states["peer"]["interval_seconds"] == 1
        assert resumed.states["peer"]["state"] == "warming"
    finally:
        http.close()


def test_corrupt_workload_candidate_is_not_restored(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        payload = controller.snapshot()
        payload["providers"]["publisher"]["best_by_workload"] = {"article_html": {"interval_seconds": -1}}
        atomic_write_json(tmp_path / "rate_calibration.json", payload)
        resumed = PublisherCapacity(http, config, clock=clock, epoch=clock)
        assert resumed.states["publisher"]["best_by_workload"] == {}
    finally:
        http.close()


def test_repeated_transient_errors_never_create_512_second_death_spiral(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        for _ in range(40):
            state = observe(http, controller, clock, errors=1, heads=0)
            assert state["interval_seconds"] <= 2
            assert http.hard_interval_floor("publisher") == 0
        assert state["state"] == "recovering_transport"
        assert observe(http, controller, clock, heads=0)["state"] == "recovering_transport"
        assert observe(http, controller, clock, heads=0)["state"] == "warming"
        assert http.client_intervals["publisher"] == 1
    finally:
        http.close()


def test_provider_minimum_client_rate_never_overrides_real_source_floor(config, tmp_path):
    http, controller, clock = controller_fixture(config, tmp_path)
    try:
        controller.transient_backoff_limits["publisher"] = 0.5
        assert observe(http, controller, clock, errors=1)["interval_seconds"] == 0.5
        http.rate_refusal_intervals["publisher"] = 10
        assert observe(http, controller, clock, errors=1)["interval_seconds"] == 10
        assert http.hard_interval_floor("publisher") == 10
    finally:
        http.close()


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), 3601, "0.5"])
def test_transient_backoff_policy_must_be_finite_and_bounded(config, tmp_path, value):
    config["providers"][0]["capacity_transient_backoff_max_seconds"] = value
    with pytest.raises(ValueError, match="transient capacity"):
        capacity_config(config, PolicyRegistry(config, base=tmp_path))
