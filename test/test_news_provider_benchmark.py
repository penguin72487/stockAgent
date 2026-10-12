from __future__ import annotations

from copy import deepcopy
import json
import time

import pytest

from scripts.benchmark_news_providers import (
    accept_retested_candidate, cnyes_retest_candidates, phase_provider_metrics,
    repeated_candidate_is_healthy, set_intervals,
)
from test_news_capacity import controller_fixture
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture


def test_fixed_rate_benchmark_disables_adaptation_without_changing_source_contract(config, tmp_path):
    http, _, _ = controller_fixture(config, tmp_path)
    try:
        original = http.registry.sources["publisher"]
        effective = set_intervals(config, http.registry, http, {"publisher": 0.05}, {"publisher": 3})
        assert effective["adaptive_rate"] == {"enabled": False}
        assert config["adaptive_rate"]["enabled"] is True
        assert http.client_intervals["publisher"] == 0.05 and http.parallelism["publisher"] == 3
        assert http.registry.sources["publisher"] is original
        http.rate_refusal_intervals["publisher"] = 0.2
        set_intervals(config, http.registry, http, {"publisher": 0.01}, {"publisher": 3})
        assert http.effective_interval("publisher") == 0.2
    finally:
        http.close()


def test_explicit_cnyes_ladder_keeps_each_requested_stage():
    assert [x[0] for x in cnyes_retest_candidates(100, 8)] == [2, 5, 10, 20, 30, 50, 75, 100]
    assert [x[0] for x in cnyes_retest_candidates(30, 8)] == [2, 5, 10, 20, 30]


def test_phase_reports_effective_runtime_not_old_registry_rate(config, tmp_path):
    http, _, _ = controller_fixture(config, tmp_path)
    try:
        http.set_client_capacity("publisher", 0.05, 3)
        performance = {"counts": {"http_requests_started": 100, "http_responses_received": 100, "http_status_200": 100}}
        row = phase_provider_metrics(http.registry.sources["publisher"], performance, 10, 1010, 50, False, None,
                                     http.capacity_snapshot()["providers"]["publisher"])
        assert row["client_interval_seconds"] == row["effective_interval_seconds"] == 0.05
        assert row["parallelism"] == 3
        assert row["requests_per_full_workflow_second"] == 2
        assert row["new_heads_per_full_workflow_second"] == 20
        assert repeated_candidate_is_healthy(row)
        row["transport_or_transient_errors"] = 2
        assert not repeated_candidate_is_healthy(row)
    finally:
        http.close()


@pytest.mark.parametrize("counts", [{"http_status_429": 1}, {"http_status_451": 1}, {"content_access_refusals": 1}])
def test_any_source_refusal_rejects_benchmark_candidate(config, tmp_path, counts):
    http, _, _ = controller_fixture(config, tmp_path)
    try:
        row = phase_provider_metrics(http.registry.sources["publisher"], {"counts": {
            "http_requests_started": 100, "http_responses_received": 100, **counts}}, 0, 100, 45, False, None)
        assert not repeated_candidate_is_healthy(row)
    finally:
        http.close()


@pytest.mark.parametrize("code", [400, 406, 415, 422])
def test_endpoint_errors_cannot_be_ignored_in_stable_candidate_proof(config, tmp_path, code):
    http, _, _ = controller_fixture(config, tmp_path)
    try:
        row = phase_provider_metrics(http.registry.sources["publisher"], {"counts": {
            "http_requests_started": 100, "http_responses_received": 100,
            "http_status_200": 98, "http_status_" + str(code): 2,
        }}, 0, 100, 45, False, None)
        assert row["unusable_http_responses"] == 2
        assert not repeated_candidate_is_healthy(row)
    finally:
        http.close()


def test_promotion_requires_multiple_matching_useful_windows(config, tmp_path):
    http, _, _ = controller_fixture(config, tmp_path)
    try:
        row = {"healthy_for_next_bounded_phase": True, "http_requests": 100, "http_responses": 100,
               "transport_or_transient_errors": 0, "new_stored_url_heads": 200,
               "client_interval_seconds": 0.05, "parallelism": 3}
        assert not accept_retested_candidate(http, config, row, [row])["accepted"]
        other = deepcopy(row)
        other["parallelism"] = 2
        assert not accept_retested_candidate(http, config, row, [row, other])["accepted"]
    finally:
        http.close()


@pytest.mark.parametrize("failure", [None, "source_429", "repeat_transport_error"])
def test_whole_retest_waits_runs_fixed_ladder_verifies_and_promotes(config, tmp_path, monkeypatch, capsys, failure):
    from scripts import benchmark_news_providers as module
    from downloader.news_fulltext import fetch_article
    from downloader.news_fulltext_policy import NewsError
    config["root"] = "lake"
    config["providers"][0].update(id="cnyes", interval_seconds=0.5, capacity_max_parallelism=8)
    config["adaptive_rate"] = {"enabled": True, "stable_windows": 3}
    config["workers"] = 16
    http, session = http_fixture(config, tmp_path / "lake", {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
    })
    http.provider_not_before["cnyes"] = time.time() + 0.01
    monkeypatch.setattr(module, "load_config", lambda _: (config, http.registry, tmp_path))
    monkeypatch.setattr(module, "NewsHTTP", lambda *_args, **_kwargs: http)
    if failure == "source_429":
        # Exercise the real refusal handling, with a short fixture-only wait.
        # The live controller still honors the actual shared source deadline.
        monkeypatch.setattr(http, "defer", lambda provider, seconds: http.provider_not_before.update(
            {provider: time.time() + 0.01}))
    intervals = []

    def fixed_cycle(queue, transport, effective, **kwargs):
        assert effective["adaptive_rate"] == {"enabled": False}
        assert transport.capacity_controller is None
        assert kwargs["provider_ids"] == {"cnyes"} and kwargs["max_articles"] == 0
        intervals.append(transport.client_intervals["cnyes"])
        if failure == "source_429" and len(intervals) == 2:
            for key in ("http_requests_started", "http_responses_received"):
                transport.metrics.count("cnyes", key, 20)
            transport.metrics.count("cnyes", "http_status_200", 19)
            transport.metrics.count("cnyes", "http_status_429")
            with pytest.raises(NewsError, match="http_429"):
                transport._response_status("cnyes", 429, {"retry-after": "30"})
            return {"cycle": {"fixture": True, "source_refused": True}}
        url = "https://publisher.test/article" + str(len(intervals))
        session.routes[url] = FakeResponse(article_html())
        with queue.db:
            queue.add_articles([{"url": url}])
        queue.accept([fetch_article(transport, queue.claim_url(url, 300))])
        for key in ("http_requests_started", "http_responses_received", "http_status_200"):
            transport.metrics.count("cnyes", key, 20)
        if failure == "repeat_transport_error" and len(intervals) == 6:
            transport.metrics.count("cnyes", "transport_errors")
        return {"cycle": {"fixture": True}}

    monkeypatch.setattr(module, "run_cycle", fixed_cycle)
    output = tmp_path / "retest.json"
    assert module.main(["--cnyes-retest", "--maximum-cnyes-rps", "30", "--phase-seconds", "10", "--output", str(output)]) == 0
    result = json.loads(output.read_text())
    assert result["accepted"]
    if failure == "source_429":
        assert intervals == [0.5, 0.2, 0.5, 0.5, 0.5]
        assert len(result["phases"]) == 2
        assert result["stopped_escalation_by_provider"]["cnyes"]
        assert http.rate_refusal_intervals["cnyes"] == 0.4
    else:
        assert intervals[:5] == [0.5, 0.2, 0.1, 0.05, 1 / 30]
        assert len(intervals) == (6 if failure else 8)
    assert result["candidate_promotion"]["accepted"] is (failure != "repeat_transport_error")
    assert result["final_acceptance"]["verification_scope"].startswith("each fixed-rate phase")
    assert all(row["acceptance"]["body_versions"] == 1 for row in result["winner_repeats"])
    status = json.loads((tmp_path / "lake/status.json").read_text())
    assert status["activity"] == "service_duration_complete"
    if failure != "repeat_transport_error":
        assert result["candidate_promotion"]["source_maximum_verified"] is False
        calibration = status["http_capacity"]["calibration"]["providers"]["cnyes"]
        assert calibration["stable_samples"] == 3
        assert calibration["last_window"]["ended_at_epoch"] == result["winner_repeats"][-1]["metrics"]["ended_at_epoch"]
        assert calibration["best"]["ended_at_epoch"] <= calibration["last_window"]["ended_at_epoch"]
