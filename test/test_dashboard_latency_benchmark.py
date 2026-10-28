import pytest

from scripts.benchmark_dashboard_latency import capture_measurement, http_sample, measure_http, summarize, timing_summary, warm_samples


@pytest.mark.parametrize("name", ["profile_opening_attempt_tail", "profile_opening_attempt_append"])
def test_opening_profiles_preserve_mode_denominator_in_immutable_checkpoint(tmp_path, monkeypatch, name):
    import json
    from scripts import benchmark_dashboard_latency as benchmark
    from stockagent.live import tw_day_trade_dashboard as dashboard
    from stockagent.live.paper_mode_snapshot import ModeSnapshotWriter

    value = {"modes": {f"mode_{i}": {} for i in range(6)}}
    manifest = ModeSnapshotWriter(tmp_path).prepare(value)
    (tmp_path / "state.json").write_text(json.dumps(manifest))
    (tmp_path / "opening_signal_latency.jsonl").write_text(json.dumps({
        "source": "discord_scheduled_signal", "status": "failed", "market": "mode_0",
        "session_date": "2026-10-22", "error_type": "TimeoutError",
    }) + "\n")
    original, observed = dashboard._opening_signal_latency_summary, []

    def summary(rows, **kwargs):
        observed.append(kwargs["expected_markets"])
        return original(rows, **kwargs)

    monkeypatch.setattr(dashboard, "_opening_signal_latency_summary", summary)
    report = getattr(benchmark, name)(tmp_path, 1)
    assert report["complete_summary_parity"] and report["source_unchanged"]
    assert observed and all(markets == sorted(value["modes"]) for markets in observed)


@pytest.mark.parametrize("trailing_newline", (False, True))
def test_verified_append_profile_preserves_complete_summary_and_original_source(tmp_path, trailing_newline):
    import json
    from scripts.benchmark_dashboard_latency import profile_opening_attempt_append
    source = tmp_path / "opening_signal_latency.jsonl"
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode": {}}}))
    line = json.dumps({"source": "discord_scheduled_signal", "status": "failed", "market": "mode",
        "session_date": "2026-10-22", "error_type": "TimeoutError", "quote_transport": {"unused": "x" * 1200}})
    original = ("\n".join([line] * 12) + ("\n" if trailing_newline else "")).encode()
    source.write_bytes(original)
    report = profile_opening_attempt_append(tmp_path, 2)
    assert report["source_unchanged"] and report["complete_summary_parity"]
    assert not report["broker_orders"] and not report["ledger_writes"]
    assert source.read_bytes() == original
    assert [row["variant"] for row in report["samples"]] == ["full_native", "verified_append", "verified_append", "full_native"] * 2
    assert {row["row_count"] for row in report["samples"]} == {13}
    assert len({row["complete_summary_sha256"] for row in report["samples"]}) == 1
    assert all(row["initial_cache_seed_ms_outside_refresh"] >= 0 for row in report["samples"])


def test_verified_append_profile_detects_incremental_summary_loss(tmp_path, monkeypatch):
    import json
    from scripts.benchmark_dashboard_latency import profile_opening_attempt_append
    from stockagent.live import tw_day_trade_dashboard as dashboard
    source = tmp_path / "opening_signal_latency.jsonl"
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode": {}}}))
    source.write_text(json.dumps({"source": "discord_scheduled_signal", "status": "failed",
        "market": "mode", "session_date": "2026-10-22", "error_type": "TimeoutError"}) + "\n")
    original = dashboard._read_verified_jsonl_append
    def damaged(*args, **kwargs):
        result = original(*args, **kwargs)
        return [] if result is not None else None
    monkeypatch.setattr(dashboard, "_read_verified_jsonl_append", damaged)
    with pytest.raises(ValueError, match="changed the complete opening summary"):
        profile_opening_attempt_append(tmp_path, 1)


def test_opening_tail_profile_is_full_summary_abba_and_does_not_modify_source(tmp_path):
    import json
    from scripts.benchmark_dashboard_latency import profile_opening_attempt_tail

    source = tmp_path / "opening_signal_latency.jsonl"
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode": {}}}))
    rows = [{"source": "discord_scheduled_signal", "status": "failed", "market": "mode",
             "session_date": "2026-10-22", "error_type": "TimeoutError",
             "quote_transport": {"unused": "x" * 10000}} for _ in range(12)]
    original = "".join(json.dumps(row) + "\n" for row in rows).encode()
    source.write_bytes(original)
    result = profile_opening_attempt_tail(tmp_path, 2)
    assert result["source_unchanged"] and result["complete_summary_parity"]
    assert result["tail_cache_cleared_each_sample"] and not result["os_cache_flushed"]
    assert not result["broker_orders"] and not result["ledger_writes"]
    assert [row["variant"] for row in result["samples"]] == ["stdlib", "native", "native", "stdlib"] * 2
    assert all(row["row_count"] == 12 for row in result["samples"])
    assert len({row["complete_summary_sha256"] for row in result["samples"]}) == 1
    assert source.read_bytes() == original


def test_opening_tail_profile_rejects_a_dropped_failure(tmp_path, monkeypatch):
    import json
    from scripts.benchmark_dashboard_latency import profile_opening_attempt_tail
    from stockagent.live import tw_day_trade_dashboard as dashboard

    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode": {}}}))
    (tmp_path / "opening_signal_latency.jsonl").write_text(json.dumps({
        "source": "discord_scheduled_signal", "status": "failed", "market": "mode",
        "session_date": "2026-10-22", "error_type": "TimeoutError",
    }) + "\n")
    monkeypatch.setattr(dashboard, "_decode_opening_latency_row", lambda _line: {})
    with pytest.raises(ValueError, match="changed the complete summary"):
        profile_opening_attempt_tail(tmp_path, 1)


def test_opening_tail_profile_rejects_source_generation_change(tmp_path, monkeypatch):
    import json
    from scripts.benchmark_dashboard_latency import profile_opening_attempt_tail
    from stockagent.live import tw_day_trade_dashboard as dashboard

    (tmp_path / "state.json").write_text(json.dumps({"modes": {"mode": {}}}))
    source = tmp_path / "opening_signal_latency.jsonl"
    source.write_text('{"market":"mode"}\n')
    original = dashboard._tail

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        with source.open("ab") as handle:
            handle.write(b'{"market":"mode"}\n')
        return result

    monkeypatch.setattr(dashboard, "_tail", mutate)
    with pytest.raises(ValueError, match="source changed"):
        profile_opening_attempt_tail(tmp_path, 1)


def test_status_projection_profiles_abba_full_wire_and_unchanged_visible_values(tmp_path):
    from copy import deepcopy
    from scripts.benchmark_dashboard_latency import profile_tw_status_projection
    from stockagent.live.public_dashboards import sanitize_tw_status
    source = {"simulation_only": True, "production_order_possible": False,
        "opening_signal_latency": {"failure_count": 100,
            "latest_failures": [{"market": "mode", "reason_code": "source_pending"}],
            "failures": [{"market": "mode", "reason_code": f"old_attempt_{i}"} for i in range(100)]},
        "modes": [{"market": "mode", "realized_pnl": -15.25}]}
    before = deepcopy(source)
    reference_value = sanitize_tw_status(source)
    reference_value["opening_signal_latency"]["failures"] = deepcopy(source["opening_signal_latency"]["failures"])
    reference = tmp_path / "reference.py"
    reference.write_text("def sanitize_tw_status(payload):\n    return " + repr(reference_value) + "\n")
    result = profile_tw_status_projection(source, 4, reference)
    assert not (reference.parent / "__pycache__").exists()
    assert source == before
    assert result["input_failure_count"] == 100
    assert result["retained_display_values_equal"] and result["source_receipt_unchanged"]
    samples = result["samples"]
    assert [row["variant"] for row in samples] == ["reference", "current", "current", "reference"] * 2
    assert len({row["visible_contract_sha256"] for row in samples}) == 1
    assert {name: value["n"] for name, value in result["variants"].items()} == {"reference": 4, "current": 4}
    assert samples[1]["decoded_bytes"] < samples[0]["decoded_bytes"]
    assert samples[1]["wire_bytes"] < samples[0]["wire_bytes"]
    assert all(row["complete_ms"] >= row["projection_ms"] >= 0 for row in samples)


@pytest.mark.parametrize("damage", ["visible_value", "input_mutation"])
def test_status_projection_rejects_visible_changes_or_source_mutation(tmp_path, monkeypatch, damage):
    from scripts.benchmark_dashboard_latency import profile_tw_status_projection
    from stockagent.live import public_dashboards
    source = {"simulation_only": True, "production_order_possible": False, "value": 1}
    original = public_dashboards.sanitize_tw_status
    control = original(source)
    reference = tmp_path / "reference.py"
    reference.write_text("def sanitize_tw_status(payload):\n    return " + repr(control) + "\n")
    def damaged(payload):
        result = original(payload)
        (result if damage == "visible_value" else payload)["value"] = 2
        return result
    monkeypatch.setattr(public_dashboards, "sanitize_tw_status", damaged)
    with pytest.raises(ValueError, match="retained display values|mutated source receipt"):
        profile_tw_status_projection(source, 1, reference)


@pytest.mark.parametrize("clock,state,day", [
    ("2026-10-27T00:58:30+00:00", "armed", "2026-10-27"),
    ("2026-10-27T08:59:59+08:00", "armed", "2026-10-27"),
    ("2026-10-27T09:00:00+08:00", "missed_opening_observation_window", "2026-10-27"),
    ("2026-10-27T14:00:00+08:00", "missed_opening_observation_window", "2026-10-27"),
    ("2026-10-24T08:58:30+08:00", "missed_opening_observation_window", "2026-10-24"),
])
def test_scheduled_browser_never_turns_a_late_boot_into_opening_success(tmp_path, clock, state, day):
    from datetime import datetime
    from scripts.benchmark_dashboard_browser import scheduled_probe_plan
    result = scheduled_probe_plan(datetime.fromisoformat(clock), tmp_path)
    assert result["state"] == state and result["session_date"] == day
    assert result["native_opening_success_claim"] is False
    assert str(tmp_path / day) in result["output"]
    assert not tmp_path.joinpath(day).exists()


def test_scheduled_browser_rejects_naive_time_and_keeps_same_day_attempts_separate(tmp_path):
    from datetime import datetime
    from scripts.benchmark_dashboard_browser import scheduled_probe_plan
    with pytest.raises(ValueError, match="aware clock"):
        scheduled_probe_plan(datetime(2026, 10, 27, 8, 59), tmp_path)
    first = scheduled_probe_plan(datetime.fromisoformat("2026-10-27T08:58:30+08:00"), tmp_path)
    retry = scheduled_probe_plan(datetime.fromisoformat("2026-10-27T08:58:31+08:00"), tmp_path)
    assert first["output"] != retry["output"]


def test_scheduled_browser_is_persistent_and_isolated_from_trading_owners():
    from pathlib import Path
    service = Path("deploy/systemd/stockagent-tw-opening-browser.service.in").read_text()
    timer = Path("deploy/systemd/stockagent-tw-opening-browser.timer.in").read_text()
    installer = Path("scripts/install_discord_bot_service.sh").read_text()
    assert "--scheduled --cdp-port 9241" in service
    assert "ProtectSystem=strict" in service and "KillMode=control-group" in service
    assert "PrivateTmp=true" in service
    assert "RuntimeDirectory=stockagent-tw-opening-browser" in service
    assert 'Environment="TMPDIR=/run/stockagent-tw-opening-browser"' in service
    assert "ReadWritePaths=@REPO_ROOT@/artifacts/operations" in service
    assert "Requires=" not in service and "PartOf=" not in service
    assert "08:58:30 Asia/Taipei" in timer and "Persistent=true" in timer
    scoped = installer.split('if [[ "$OPENING_AUDIT_ONLY" == true ]]; then')[-1].split("fi", 1)[0]
    assert '"$OPENING_BROWSER_TIMER_NAME"' in scoped
    assert 'systemctl restart "$SERVICE_NAME"' not in scoped


def test_scheduled_late_start_persists_missed_evidence_without_launching_browser(tmp_path, monkeypatch, capsys):
    from datetime import datetime
    import json
    import sys
    from scripts import benchmark_dashboard_browser as probe

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.fromisoformat("2026-10-27T14:00:00+08:00").astimezone(tz)

    monkeypatch.setattr(probe, "datetime", Clock)
    monkeypatch.setattr(sys, "argv", ["probe", "--scheduled", "--scheduled-root", str(tmp_path)])
    monkeypatch.setitem(sys.modules, "playwright.sync_api", None)
    assert probe.main() == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["status"] == "missed_opening_observation_window"
    from pathlib import Path
    sidecar = json.loads(Path(summary["launcher_receipt"]).read_bytes())
    assert sidecar["url"] == "https://penguin72487.dpdns.org/tw-day-trade/"
    browser_source = Path("scripts/benchmark_dashboard_browser.mjs").read_text()
    assert '|| "https://penguin72487.dpdns.org/tw-day-trade/"' in browser_source
    assert sidecar["status"] == "missed_opening_observation_window"
    assert sidecar["scheduled_observation"]["native_opening_success_claim"] is False
    assert sidecar["broker_orders"] is False and sidecar["ledger_writes"] is False
    assert not Path(summary["receipt"]).exists()


@pytest.mark.parametrize("suffix", ["", ".updates.jsonl", ".launch.json"])
def test_owned_browser_probe_never_overwrites_existing_evidence(tmp_path, suffix):
    from scripts.benchmark_dashboard_browser import validate_probe
    output = tmp_path / "browser.json"
    existing = tmp_path / ("browser.json" + suffix)
    existing.write_bytes(b"retained")
    with pytest.raises(FileExistsError):
        validate_probe("https://dashboard.example/tw-day-trade/", output, watch_ms=1000, port=9238)
    assert existing.read_bytes() == b"retained"


@pytest.mark.parametrize("change", [
    {"url": "https://token:password@dashboard.example/"}, {"url": "file:///private"},
    {"url": "https://dashboard.example/?token=private"}, {"url": "https://dashboard.example/#private"},
    {"watch_ms": -1}, {"watch_ms": 900001}, {"port": 0}, {"port": 65536},
])
def test_owned_browser_probe_rejects_credentials_and_unbounded_runs(tmp_path, change):
    from scripts.benchmark_dashboard_browser import validate_probe
    args = {"url": "http://127.0.0.1:8770/tw-day-trade/", "output": tmp_path / "new.json",
            "watch_ms": 0, "port": 9238, **change}
    with pytest.raises(ValueError):
        validate_probe(**args)


def test_owned_browser_watch_can_arm_without_creating_a_fake_receipt(tmp_path):
    from scripts.benchmark_dashboard_browser import validate_probe
    output = tmp_path / "new.json"
    assert validate_probe("http://127.0.0.1:8770/tw-day-trade/", output,
                          watch_ms=600000, port=9238) == output.resolve()
    assert not output.exists()


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


def test_optional_host_service_sample_stays_allowlisted(monkeypatch):
    import subprocess
    from scripts import benchmark_dashboard_latency as benchmark
    def run(args, **kwargs):
        assert args[2] == '*.service'
        return subprocess.CompletedProcess([], 0, stdout=(
            'Id=postgresql.service\nActiveState=active\nCPUUsageNSec=10\nEnvironment=secret\n\n'
            'Id=stockagent-test.service\nActiveState=active\nCPUUsageNSec=20\n'))
    monkeypatch.setattr(benchmark.subprocess, 'run', run)
    units = benchmark.service_snapshot(include_host_services=True)['units']
    assert set(units) == {'postgresql.service', 'stockagent-test.service'}
    assert all('Environment' not in row for row in units.values())


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


def test_frozen_inventory_paths_support_source_identity_encoding(tmp_path, monkeypatch):
    import os
    from scripts.benchmark_dashboard_latency import profile_inventory
    from stockagent.live import data_monitor_inventory as inventory
    source = tmp_path / "source.parquet"
    source.write_bytes(b"source")
    monkeypatch.setattr(inventory, "_read_json", lambda path: {"version": inventory.INVENTORY_VERSION})
    monkeypatch.setattr(inventory, "_selected_files", lambda root: {"dataset": [source]})

    def build(root):
        observed = inventory._selected_files(root)["dataset"][0]
        assert os.fsencode(observed) == os.fsencode(source)
        assert observed.name == source.name
        assert observed.stat().st_size == len(b"source")
        assert len(inventory._source_file_identity(observed, observed.stat())) == 5
        return {"rows": [{"dataset_id": "dataset", "field": "price"}]}

    monkeypatch.setattr(inventory, "build_feature_inventory", build)
    receipt = profile_inventory(tmp_path, 2)
    assert receipt["outputs_identical"]
    assert receipt["implementations"]["current"]["fields"] == 1


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


def test_local_history_profile_detects_changed_enabled_market_inputs(tmp_path, monkeypatch):
    from scripts.benchmark_dashboard_latency import profile_history
    from stockagent.live import tw_day_trade_dashboard
    state = tmp_path / "state.json"
    state.write_text('{"product":"tw_day_trade","enabled_markets":["a"]}')

    def build(**kwargs):
        state.write_text('{"product":"tw_day_trade","enabled_markets":["b"]}')
        return {"returned_points": 0, "minute_series": []}

    monkeypatch.setattr(tw_day_trade_dashboard, "build_dashboard_history_snapshot", build)
    assert profile_history(tmp_path, 1)["source_stable"] is False
