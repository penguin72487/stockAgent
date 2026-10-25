import json
import os
import socket
import subprocess
from types import SimpleNamespace

import pytest

from scripts import audit_windows_gpu_ownership as audit


def fixture():
    return {
        "observed_at_utc": "2026-10-13T23:05:00Z",
        "engines": [
            {"instance": "pid_123_luid_0xAB_0xCD_phys_0_eng_2_engtype_3D", "percent": 3},
            {"instance": "pid_123_luid_0xab_0xcd_phys_0_eng_3_engtype_Compute_0", "percent": 1},
            {"instance": "pid_456_luid_0xab_0xcd_phys_0_eng_1_engtype_Copy", "percent": 2},
        ],
        "memory": [{"instance": "pid_123_luid_0xAB_0xCD_phys_0", "dedicated_bytes": 100,
                    "shared_bytes": 20, "committed_bytes": 140}],
        "processes": [{"pid": 123, "name": "桌面.exe", "created_at_utc": "2026-10-13T22:00:00Z",
                       "stockagent_reference": False, "CommandLine": "must never be exported"}],
    }


def test_windows_pid_engines_and_memory_are_not_confused_with_linux_owners():
    result = audit.parse_snapshot(fixture())
    rows = result["processes"]
    assert len(rows) == 2 and rows[0]["windows_pid"] == 123
    assert len(rows[0]["engines"]) == 2
    assert rows[0]["memory_counters"] == [{"dedicated_bytes": 100, "shared_bytes": 20, "committed_bytes": 140}]
    assert rows[1]["process_observation"] is None  # Exited or inaccessible is not PID zero.
    assert rows[1]["memory_counters"] == []  # Missing memory counters are not zero bytes.
    assert not any(row["linux_service_owner_verified"] for row in rows)
    assert not result["all_gpu_memory_attributed"] and not result["memory_counters_additive"]
    assert not result["memory_counter_accuracy_verified"]
    assert "CommandLine" not in json.dumps(result)


def test_unknown_counter_formats_and_missing_engine_are_retained_as_unknown():
    value = fixture()
    value["engines"].extend([
        {"instance": "future_counter_schema", "percent": 10},
        {"instance": "pid_789_luid_0xab_0xcd_phys_0", "percent": 20},
    ])
    result = audit.parse_snapshot(value)
    assert result["unrecognized_count"] == 2
    assert result["unrecognized_instances"][1]["instance"] == "pid_789_luid_0xab_0xcd_phys_0"


def test_no_windows_runtime_is_explicitly_unavailable(monkeypatch):
    monkeypatch.setattr(audit.shutil, "which", lambda *_args: None)
    monkeypatch.setattr(audit.subprocess, "run", lambda *_args, **_kwargs: pytest.fail("must not start a runtime"))
    assert audit.snapshot() == {"state": "unavailable", "reason": "windows_powershell_unavailable"}


def test_query_is_readonly_and_does_not_export_raw_arguments(monkeypatch):
    monkeypatch.setattr(audit.shutil, "which", lambda *_args: "/test/powershell.exe")

    def run(argv, **options):
        assert argv[:4] == ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command"]
        assert options["timeout"] == 30 and options["check"]
        assert "Stop-Process" not in argv[-1] and "Restart-Service" not in argv[-1]
        return SimpleNamespace(stdout=json.dumps(fixture(), ensure_ascii=False).encode("utf-8-sig"))

    monkeypatch.setattr(audit.subprocess, "run", run)
    result = audit.snapshot()
    assert result["state"] == "observed"
    assert result["processes"][0]["process_observation"]["name"] == "桌面.exe"
    assert "must never be exported" not in json.dumps(result)


@pytest.mark.parametrize("failure", [
    subprocess.TimeoutExpired("powershell.exe", 30),
    subprocess.CalledProcessError(1, "powershell.exe", stderr="private diagnostic"),
    OSError("Windows endpoint unavailable"),
])
def test_query_failure_does_not_claim_zero_gpu_or_export_private_stderr(monkeypatch, failure):
    monkeypatch.setattr(audit.shutil, "which", lambda *_args: "/test/powershell.exe")

    def fail(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(audit.subprocess, "run", fail)
    result = audit.snapshot()
    assert result["state"] == "unavailable" and "processes" not in result
    assert "private diagnostic" not in json.dumps(result)


def test_explicit_socket_reaches_only_the_child_environment(monkeypatch, tmp_path):
    path = tmp_path / "caller_interop"
    monkeypatch.setenv("WSL_INTEROP", "/original/unavailable_interop")
    monkeypatch.setattr(audit.shutil, "which", lambda *_args: "/test/powershell.exe")

    def run(argv, **options):
        assert options["env"]["WSL_INTEROP"] == str(path)
        assert options["timeout"] == 30 and options["check"]
        assert os.environ["WSL_INTEROP"] == "/original/unavailable_interop"
        return SimpleNamespace(stdout=json.dumps(fixture()).encode())

    monkeypatch.setattr(audit.subprocess, "run", run)
    monkeypatch.chdir(tmp_path)
    with socket.socket(socket.AF_UNIX) as endpoint:
        endpoint.bind(path.name)
        assert audit.snapshot(interop_socket=path)["state"] == "observed"
    assert os.environ["WSL_INTEROP"] == "/original/unavailable_interop"


@pytest.mark.parametrize("kind", ["missing", "regular", "relative", "symlink"])
def test_invalid_explicit_socket_does_not_fall_back_or_start_windows(kind, monkeypatch, tmp_path):
    path = tmp_path / "endpoint"
    if kind == "regular":
        path.write_text("not a socket")
    elif kind == "relative":
        path = type(path)("relative_interop")
    elif kind == "symlink":
        target = tmp_path / "regular"
        target.write_text("not a socket")
        path.symlink_to(target)
    monkeypatch.setattr(audit.shutil, "which", lambda *_args: "/test/powershell.exe")
    monkeypatch.setattr(audit.subprocess, "run", lambda *_a, **_k: pytest.fail("no fallback query"))
    result = audit.snapshot(interop_socket=path)
    assert result["state"] == "unavailable" and "processes" not in result


def test_cli_records_explicit_transport_without_starting_gpu_or_windows(monkeypatch, tmp_path):
    path = tmp_path / "interop"
    output = tmp_path / "artifacts/operations/observation.json"
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    monkeypatch.setattr(audit, "gpu_snapshot", lambda: {"state": "fixture_board"})
    received = []

    def observe(*, interop_socket):
        received.append(interop_socket)
        return {"state": "observed", "processes": []}

    monkeypatch.setattr(audit, "snapshot", observe)
    monkeypatch.setattr(audit.sys, "argv", ["audit", "--output", str(output), "--samples", "2",
                                         "--wsl-interop-socket", str(path)])
    monkeypatch.chdir(tmp_path)
    with socket.socket(socket.AF_UNIX) as endpoint:
        endpoint.bind(path.name)
        audit.main()
    result = json.loads(output.read_bytes())
    assert received == [path, path]
    assert result["contract"] == "windows_wddm_gpu_owner_observation_v2"
    assert result["windows_transport"] == {
        "interop_socket_selection": "explicit", "explicit_socket_path": str(path),
        "parent_environment_modified": False}
    assert result["cuda_initialized_by_probe"] is False
    assert result["provider_requests"] == 0 and result["services_or_applications_stopped"] is False
