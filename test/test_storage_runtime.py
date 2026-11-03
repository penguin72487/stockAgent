"""Fleet observations remain small, read-only and distinct from acceptance."""
import io
import json
from types import SimpleNamespace

import pytest

from stockagent.data_sync import storage_runtime as fleet
from stockagent.data_sync import storage_monitor as storage

NOW = 1793463600.0


def view(nodes, now=NOW):
    return fleet.public_status({"contract": fleet.CONTRACT, "nodes": nodes}, now=now)


def test_unknown_receiver_and_first_sample_are_not_zero_usage_or_completed():
    data = view({})
    assert len(data["nodes"]) == 3 and data["interval_seconds"] == 60
    for row in data["nodes"]:
        assert row["state"] == "unknown"
        assert row["metrics"]["cpu_percent"] is None
        assert "complete" not in row


@pytest.mark.parametrize("age,state", [(1, "measured"), (181, "stale"), (-121, "unknown")])
def test_each_node_retains_its_own_freshness_clock(age, state):
    row = {"state": "measured", "observed_at_utc": fleet.iso(NOW-age), "metrics": {"cpu_percent": 0}}
    value = view({"vastai1T": row})
    assert value["nodes"][2]["state"] == state
    assert value["nodes"][2]["metrics"]["cpu_percent"] == 0
    assert value["nodes"][1]["state"] == "unknown"


def test_public_projection_drops_connection_details_invalid_metrics_and_private_sampler():
    row = {"state": "measured", "observed_at_utc": fleet.iso(NOW), "address": "secret",
           "cpu_sample": {"boot": "secret"}, "metrics": {"cpu_percent": float("nan"), "gpu_count": True,
           "memory_total_bytes": 8, "memory_used_bytes": 9, "gpu_percent": 101, "password": "secret"},
           "services": [{"id": "day_trade", "state": "active", "command": "secret"},
                        {"id": "secret", "state": "active"}]}
    data = view({"penguin": row, "lab203": "secret"})
    assert "secret" not in json.dumps(data)
    local = data["nodes"][0]
    assert local["metrics"]["cpu_percent"] is None
    assert local["metrics"]["memory_used_bytes"] is None
    assert local["metrics"]["gpu_count"] is None
    assert local["services"][0]["state"] == "active"
    assert local["services"][1]["state"] == "unknown"


def test_trading_deferred_exit_is_waiting_but_real_failure_is_not_hidden(monkeypatch):
    service_data = (
        "Id=stockagent-tw-day-trade-priority-source.service\nLoadState=loaded\n"
        "ActiveState=failed\nExecMainStatus=75\n\n"
        "Id=stockagent-finlab-daytrade-projection.service\nLoadState=loaded\n"
        "ActiveState=failed\nExecMainStatus=1\n"
    )
    monkeypatch.setattr(fleet.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=service_data))
    sampled = fleet.local_observation({}, now=NOW)
    services = {s["id"]: s["state"] for s in sampled["services"]}
    assert services["day_trade_source"] == "waiting"
    assert services["day_trade_projection"] == "failed"
    projected = view({"penguin": sampled})["nodes"][0]
    assert {s["id"]: s["state"] for s in projected["services"]}["day_trade_source"] == "waiting"


def test_failed_probe_keeps_last_observation_without_refreshing_its_clock(monkeypatch):
    monkeypatch.setattr(fleet, "local_observation", lambda *a, **k: {"state": "measured"})
    old = {"contract": fleet.CONTRACT, "nodes": {"vastai1T": {
        "state": "measured", "observed_at_utc": fleet.iso(NOW-30), "metrics": {"gpu_percent": 0}}}}
    raw = fleet.collect(old, {"state": "unavailable"}, now=NOW)
    remote = fleet.public_status(raw, now=NOW)["nodes"][2]
    assert remote["state"] == "unavailable" and remote["age_seconds"] == 30
    assert remote["metrics"]["gpu_percent"] == 0
    assert raw["nodes"]["vastai1T"]["observed_at_utc"] == fleet.iso(NOW-30)


def test_cpu_uses_deltas_and_reboot_resets_sample_without_sleep(monkeypatch):
    from pathlib import Path
    content = {"/proc/stat": "cpu 20 0 10 60 10 0 0 0 0 0\n", "/proc/meminfo": "MemTotal: 100 kB\nMemAvailable: 80 kB\n",
               "/proc/sys/kernel/random/boot_id": "boot-a"}
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **k: content[str(path)])
    monkeypatch.setattr(fleet.os, "sched_getaffinity", lambda _: {0, 1})
    monkeypatch.setattr(fleet.time, "monotonic", lambda: 100)
    monkeypatch.setattr(fleet.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout=""))
    first = fleet.local_observation({}, now=NOW)
    assert "cpu_percent" not in first["metrics"]
    assert first["metrics"]["memory_used_bytes"] == 20 * 1024
    monkeypatch.setattr(fleet.time, "monotonic", lambda: 160)
    content["/proc/stat"] = "cpu 40 0 20 110 30 0 0 0 0 0\n"
    assert fleet.local_observation(first, now=NOW+60)["metrics"]["cpu_percent"] == 30
    content["/proc/sys/kernel/random/boot_id"] = "boot-b"
    assert "cpu_percent" not in fleet.local_observation(first, now=NOW+60)["metrics"]


def test_receiver_sampler_uses_cpu_deltas_without_service_probe_or_private_state(monkeypatch):
    from pathlib import Path
    content = {"/proc/stat": "cpu 20 0 10 60 10 0 0 0\n",
               "/proc/meminfo": "MemTotal: 100 kB\nMemAvailable: 80 kB\n",
               "/proc/sys/kernel/random/boot_id": "private-boot-id"}
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **k: content[str(path)])
    monkeypatch.setattr(fleet.os, "sched_getaffinity", lambda _: {0, 1})
    monkeypatch.setattr(fleet.time, "monotonic", lambda: 100)
    monkeypatch.setattr(fleet.subprocess, "run", lambda *a, **k: pytest.fail("receiver started a service probe"))
    previous, initial = fleet.receiver_host_observation({}, now=NOW)
    assert initial["metrics"]["memory_used_bytes"] == 20 * 1024
    assert "cpu_percent" not in initial["metrics"]
    assert "private-boot-id" not in json.dumps(initial)
    monkeypatch.setattr(fleet.time, "monotonic", lambda: 160)
    content["/proc/stat"] = "cpu 40 0 20 110 30 0 0 0\n"
    _, current = fleet.receiver_host_observation(previous, now=NOW+60)
    assert current["metrics"]["cpu_percent"] == 30
    assert "cpu_sample" not in current and "services" not in current


def test_receiver_runtime_uses_its_sample_clock_not_the_new_receipt_clock(monkeypatch):
    monkeypatch.setattr(fleet, "local_observation", lambda *a, **k: {"state": "measured"})
    receiver = {"generated_at_utc": fleet.iso(NOW), "host_runtime": {
        "contract": fleet.HOST_RUNTIME_CONTRACT, "observed_at_utc": fleet.iso(NOW-200),
        "metrics": {"cpu_percent": 23, "memory_total_bytes": 100, "memory_used_bytes": 80,
                    "password": "private"}}}
    raw = fleet.collect({}, {}, receiver=receiver, now=NOW)
    row = fleet.public_status(raw, now=NOW)["nodes"][1]
    assert row["state"] == "stale" and row["age_seconds"] == 200
    assert row["metrics"]["cpu_percent"] == 23 and row["metrics"]["memory_used_bytes"] == 80
    assert "private" not in json.dumps(raw)


@pytest.mark.parametrize("contract,offset", [("wrong", 0), (fleet.HOST_RUNTIME_CONTRACT, 121)])
def test_receiver_rejects_foreign_or_future_runtime_but_keeps_receipt(monkeypatch, contract, offset):
    monkeypatch.setattr(fleet, "local_observation", lambda *a, **k: {"state": "measured"})
    receiver = {"generated_at_utc": fleet.iso(NOW), "host_runtime": {"contract": contract,
        "observed_at_utc": fleet.iso(NOW+offset), "metrics": {"cpu_percent": 42}}}
    row = fleet.public_status(fleet.collect({}, {}, receiver=receiver, now=NOW), now=NOW)["nodes"][1]
    assert row["state"] == "measured" and row["metrics"]["cpu_percent"] is None


def test_remote_metrics_share_one_capacity_request_and_preserve_source_units(monkeypatch):
    import urllib.request
    payload = {"cpu": {"percent": 0, "count": 61}, "ram": {"total": 100, "used": 20},
               "gpu": {"count": 2, "avg_load_percent": 0, "memory_used": 4.0, "memory_total": 65214.0, "memory_unit": "MB"}}
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: io.BytesIO(json.dumps(payload).encode()))
    monkeypatch.setattr(fleet.subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="stockagent-a RUNNING pid 1\nstockagent-b EXITED done\nstockagent-c BACKOFF failure\nportal RUNNING pid 2\n"))
    namespace = {"datetime": __import__("datetime"), "json": json}
    exec(fleet.REMOTE_RUNTIME, namespace)
    raw = namespace["runtime"]
    assert raw["metrics"]["supervised_running"] == 1 and raw["metrics"]["supervised_failed"] == 1
    assert raw["metrics"]["gpu_memory_used_mb"] == 4.0
    assert "programs" not in raw
    assert fleet.REMOTE_RUNTIME in storage.REMOTE_CAPACITY
    assert storage.REMOTE_CAPACITY.count("print(") == 1


def test_bad_runtime_does_not_hide_valid_capacity_or_trigger_http_probes(monkeypatch):
    monkeypatch.setattr(fleet.subprocess, "run", lambda *a, **k: pytest.fail("public read launched a probe"))
    data = storage.public_status({"contract": storage.CONTRACT, "runtime": {"contract": "bad"}})
    assert len(data["nodes"]) == 4 and len(data["runtime"]["nodes"]) == 3


def test_invalid_service_state_shape_does_not_abort_capacity_projection():
    row = {"state": [], "observed_at_utc": fleet.iso(NOW), "services": [{"id": "day_trade", "state": []}]}
    assert view({"penguin": row})["nodes"][0]["services"][0]["state"] == "unknown"


def test_receiver_observer_bundle_imports_without_the_penguin_checkout(tmp_path):
    """A new monitor helper must also be present in the reviewed tool closure."""
    from pathlib import Path
    import shutil
    import subprocess
    import sys
    from scripts.publish_backup_handoff import STORAGE_OBSERVER_FILES

    repo = Path(__file__).resolve().parents[1]
    for relative in STORAGE_OBSERVER_FILES:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repo / relative, target)
    result = subprocess.run([sys.executable, "-I", str(tmp_path / "scripts/lab203_storage_monitor.py"), "--help"],
                            cwd=tmp_path, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
