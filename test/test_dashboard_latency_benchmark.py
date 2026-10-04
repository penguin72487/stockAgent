import pytest

from scripts.benchmark_dashboard_latency import capture_measurement, http_sample, measure_http, summarize, timing_summary, warm_samples


def test_service_counters_are_allowlisted_and_unavailable_values_are_not_sizes(monkeypatch):
    import subprocess
    from scripts import benchmark_dashboard_latency as benchmark
    output = ("Id=stockagent-test.service\nMainPID=23\nInvocationID=abc\n"
              "Transient=yes\nUnitFileState=transient\n"
              "CPUUsageNSec=5000000\nMemoryCurrent=18446744073709551615\n"
              "ExecMainStartTimestampMonotonic=1000000\nExecMainExitTimestampMonotonic=3500000\n"
              "ExecMainStatus=1\nResult=exit-code\nEnvironment=secret\n\n"
              "Id=other.service\nCPUUsageNSec=123\n")
    def run(args, **kwargs):
        assert "--all" in args  # Failed/inactive oneshots must not disappear.
        return subprocess.CompletedProcess([], 0, stdout=output)
    monkeypatch.setattr(benchmark.subprocess, "run", run)
    snapshot = benchmark.service_snapshot()
    assert set(snapshot["units"]) == {"stockagent-test.service"}
    row = snapshot["units"]["stockagent-test.service"]
    assert row["CPUUsageNSec"] == 5000000
    assert row["MemoryCurrent"] is None
    assert row["last_attempt_seconds"] == 2.5
    assert row["ExecMainStatus"] == 1
    assert row["Result"] == "exit-code"
    assert row["Transient"] == "yes"
    assert row["UnitFileState"] == "transient"
    assert row["last_attempt_outcome"] == "failed"
    assert "Environment" not in row


def test_cgroup_memory_breakdown_separates_file_cache_from_anon(tmp_path):
    from scripts.benchmark_dashboard_latency import _cgroup_memory_breakdown

    cgroup = tmp_path / "system.slice" / "stockagent-openbb-archive.service"
    cgroup.mkdir(parents=True)
    (cgroup / "memory.stat").write_text(
        "anon 196161536\nfile 2403622912\nslab 38166088\n"
    )
    assert _cgroup_memory_breakdown(
        "/system.slice/stockagent-openbb-archive.service",
        "stockagent-openbb-archive.service", root=tmp_path,
    ) == {"MemoryAnon": 196161536, "MemoryFile": 2403622912}
    assert _cgroup_memory_breakdown(
        "/system.slice/../stockagent-openbb-archive.service",
        "stockagent-openbb-archive.service", root=tmp_path,
    ) == {"MemoryAnon": None, "MemoryFile": None}


def test_cgroup_memory_events_are_cumulative_and_missing_is_unknown(tmp_path):
    from scripts.benchmark_dashboard_latency import _cgroup_memory_events

    unit = "stockagent-source-events.service"
    cgroup = tmp_path / "system.slice" / unit
    cgroup.mkdir(parents=True)
    (cgroup / "memory.events").write_text(
        "low 0\nhigh 106709\nmax 0\noom 0\noom_kill 0\n"
    )
    assert _cgroup_memory_events(
        f"/system.slice/{unit}", unit, root=tmp_path,
    ) == {
        "MemoryHighEvents": 106709,
        "MemoryMaxEvents": 0,
        "MemoryOomEvents": 0,
        "MemoryOomKillEvents": 0,
    }
    assert all(
        value is None for value in _cgroup_memory_events(
            f"/system.slice/../{unit}", unit, root=tmp_path,
        ).values()
    )


def test_cgroup_io_sums_devices_and_rejects_untrusted_paths(tmp_path):
    from scripts.benchmark_dashboard_latency import _cgroup_io_bytes

    unit = "stockagent-writer.service"
    cgroup = tmp_path / "system.slice" / unit
    cgroup.mkdir(parents=True)
    (cgroup / "io.stat").write_text(
        "8:32 rbytes=100 wbytes=300 rios=1 wios=2\n"
        "8:48 rbytes=20 wbytes=50 rios=1 wios=1\n"
    )
    assert _cgroup_io_bytes(f"/system.slice/{unit}", unit, root=tmp_path) == {
        "CgroupIOReadBytes": 120,
        "CgroupIOWriteBytes": 350,
    }
    assert all(
        value is None
        for value in _cgroup_io_bytes(
            f"/system.slice/../{unit}", unit, root=tmp_path
        ).values()
    )


def test_service_attempt_nonzero_exit_is_not_hidden_by_success_result():
    from scripts.benchmark_dashboard_latency import classify_service_attempt

    assert classify_service_attempt({
        "ActiveState": "inactive", "last_attempt_seconds": 3340.7,
        "Result": "success", "ExecMainStatus": 15,
    }) == "nonzero_exit"
    assert classify_service_attempt({
        "ActiveState": "inactive", "last_attempt_seconds": 1.0,
        "Result": "success", "ExecMainStatus": 0,
    }) == "process_exited_zero"
    assert classify_service_attempt({
        "ActiveState": "active", "last_attempt_seconds": None,
        "Result": "success", "ExecMainStatus": 15,
    }) == "running"
    assert classify_service_attempt({
        "ActiveState": "failed", "last_attempt_seconds": None,
        "Result": "start-limit-hit", "ExecMainStatus": None,
    }) == "failed"


def test_service_counter_reset_and_restart_do_not_fake_low_cpu():
    from scripts.benchmark_dashboard_latency import service_deltas
    unit = {"MainPID": "23", "NRestarts": "0", "InvocationID": "abc", "CPUUsageNSec": 1000000000, "MemoryHighEvents": 10, "CgroupIOWriteBytes": 100, "ActiveState": "active"}
    before = {"monotonic": 1, "units": {"stockagent-test.service": unit}}
    after = {"monotonic": 3, "units": {"stockagent-test.service": {**unit, "CPUUsageNSec": 2000000000, "MemoryHighEvents": 12, "CgroupIOWriteBytes": 200}}}
    row = service_deltas(before, after)["rows"][0]
    assert row["cpu_cores_average"] == .5
    assert row["MemoryHighEventsDelta"] == 2
    assert row["CgroupIOWriteBytesDelta"] == 100
    after["units"]["stockagent-test.service"]["ActiveState"] = "inactive"
    assert service_deltas(before, after)["rows"][0]["cpu_cores_average"] is None
    after["units"]["stockagent-test.service"]["ActiveState"] = "active"
    after["units"]["stockagent-test.service"]["InvocationID"] = "new"
    assert service_deltas(before, after)["rows"][0]["cpu_used_ms"] is None
    assert service_deltas(before, after)["rows"][0]["MemoryHighEventsDelta"] is None
    assert service_deltas(before, after)["rows"][0]["CgroupIOWriteBytesDelta"] is None
    assert "error" in service_deltas({"error": "unavailable"}, after)


def test_inventory_replay_fails_explicitly_without_current_inputs(tmp_path):
    from scripts.benchmark_dashboard_latency import profile_inventory
    with pytest.raises(ValueError, match="current footer inventory required"):
        profile_inventory(tmp_path, 1)


def test_shioaji_profile_preserves_one_observation_clock_and_compares_real_outputs(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_shioaji_monitor
    from stockagent.live import shioaji_api_dashboard as monitor
    clocks = []
    def build(root, *, now):
        assert root == tmp_path
        clocks.append(now)
        return {"observed_at": now.isoformat(), "health": "degraded"}
    monkeypatch.setattr(monitor, "build_shioaji_public_status", build)
    result = profile_shioaji_monitor(tmp_path, 2)
    assert len(clocks) == 4 and len(set(clocks)) == 1
    assert result["outputs_stable"] is True
    assert result["implementations"]["parallel"]["timing"]["n"] == 2


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
    monkeypatch.delenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", raising=False)

    report = profile_history(tmp_path, 1, source_rebuild=True)

    assert report["source_rebuild"] is True
    assert report["session_projection_enabled"] is False
    assert "unavailable" in report["boundary"]
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


def test_local_profile_reports_actual_session_cache_configuration(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_history
    from stockagent.live import tw_day_trade_dashboard

    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", str(cache))
    monkeypatch.setattr(
        tw_day_trade_dashboard,
        "build_dashboard_history_snapshot",
        lambda **kwargs: {"returned_points": 1, "minute_series": []},
    )
    report = profile_history(tmp_path, 1, source_rebuild=True)
    assert report["session_projection_enabled"] is True
    assert report["index_cache_dir"] == str(cache)


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
