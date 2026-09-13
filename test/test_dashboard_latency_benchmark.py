import pytest

from scripts.benchmark_dashboard_latency import capture_measurement, http_sample, measure_http, summarize, timing_summary, warm_samples


def test_percentiles_are_nearest_rank_and_empty_is_explicit():
    assert summarize([])["median_ms"] is None
    assert summarize(range(1, 21))["p95_ms"] == 19
    assert summarize(range(1, 101))["p99_ms"] == 99


def test_failed_phase_is_not_reported_as_success_or_zero_latency():
    def fail():
        raise TimeoutError("private exception details")
    assert capture_measurement(fail) == {"error": "TimeoutError"}


@pytest.mark.parametrize("content_type,body,expected", [
    ("text/html", b"<html>error</html>", False),
    ("application/json", b"[]", False),
    ("application/json", b"malformed", False),
    ("application/json", b'{"health":"degraded"}', True),
])
def test_api_probe_requires_json_object_without_hiding_source_health(content_type, body, expected):
    class Response:
        status_code = 200
        headers = {"Content-Type": content_type}
        content = body
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    class Session:
        def get(self, *args, **kwargs):
            return Response()
    assert http_sample(Session(), "http://localhost/api/status", 1)["ok"] is expected


def test_warm_measurement_excludes_each_worker_connection_not_failures():
    samples = [{"ok": True, "total_ms": n} for n in (3000, 10, 20, 4000, 30, 40)]
    samples[2]["ok"] = False
    warm = warm_samples({"samples": samples}, 3)
    assert len(warm) == 4
    assert timing_summary(warm)["total_ms"]["median_ms"] == 30
    assert timing_summary(warm_samples({"samples": samples}, 1))["total_ms"]["n"] == 0


def test_http_benchmark_preserves_errors_and_exact_sample_counts(monkeypatch):
    from scripts import benchmark_dashboard_latency as benchmark
    import requests
    calls = []
    class Session:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    def sample(session, url, timeout):
        calls.append(url)
        return {"ok": "bad" not in url, "total_ms": 2, "headers_ms": 1}
    monkeypatch.setattr(requests, "Session", Session)
    monkeypatch.setattr(benchmark, "http_sample", sample)
    good, bad = measure_http("http://localhost", ["/good", "/bad"], repeats=3, concurrency=2, timeout=1)
    assert len(calls) == 14
    assert len(good["samples"]) == 6
    assert good["warm_reuse"]["total_ms"]["n"] == 4
    assert good["connection_first"]["total_ms"]["n"] == 2
    assert good["errors"] == 0
    assert bad["errors"] == 7
    assert bad["steady"]["total_ms"]["median_ms"] is None
    assert bad["requests_per_second"] == 0


def test_local_profile_reports_points_and_avoids_large_signature_scans(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_history
    from stockagent.live import dashboard_updates, tw_day_trade_dashboard
    path = tmp_path / "marks.jsonl"
    with path.open("wb") as stream:
        stream.truncate(17 * 1024 * 1024)
    monkeypatch.setattr(tw_day_trade_dashboard, "build_dashboard_history_snapshot", lambda **kw: {
        "returned_points": 2, "minute_series": [{"points": [[1], [2]]}],
    })
    def unexpected_read(*args):
        raise AssertionError("unbounded microbenchmark source scan")
    monkeypatch.setattr(dashboard_updates, "file_signature", unexpected_read)
    report = profile_history(tmp_path, 2)
    assert report["source_stable"]
    assert report["outputs_identical"]
    assert report["samples"][0]["point_count"] == 2
    assert report["samples"][0]["series_count"] == 1
    assert report["signature"][0]["skipped"]


def test_local_profile_can_bypass_final_projection_caches(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_history
    from stockagent.live import tw_day_trade_dashboard

    observed = []

    def build(**kwargs):
        observed.append(kwargs)
        return {"returned_points": 1, "minute_series": [{"points": [[1]]}]}

    monkeypatch.setattr(tw_day_trade_dashboard, "build_dashboard_history_snapshot", build)

    report = profile_history(tmp_path, 1, source_rebuild=True)

    assert report["source_rebuild"] is True
    assert "projection bypassed" in report["boundary"]
    assert observed == [
        {
            "state_dir": tmp_path.resolve(),
            "range_key": "all",
            "resolution": "1m",
            "history_encoding": "minute_columns_v2",
            "use_memory_cache": False,
            "use_persistent_cache": False,
        }
    ]


def test_local_profile_retains_separate_full_source_baseline(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_history
    from stockagent.live import tw_day_trade_dashboard

    observed = []

    def build(**kwargs):
        observed.append(kwargs)
        return {"returned_points": 1, "minute_series": [{"minute_indexes": [0]}]}

    monkeypatch.setattr(tw_day_trade_dashboard, "build_dashboard_history_snapshot", build)

    report = profile_history(
        tmp_path,
        1,
        source_rebuild=True,
        full_source_rebuild=True,
    )

    assert report["full_source_rebuild"] is True
    assert report["session_projection_enabled"] is False
    assert "session projection bypassed" in report["boundary"]
    assert observed[0]["use_session_projection"] is False
