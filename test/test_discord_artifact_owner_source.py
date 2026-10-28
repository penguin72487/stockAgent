from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import run_discord_artifact_maintenance as maintenance
from scripts import run_discord_artifact_with_owner as wrapper
from stockagent.live import service_code_release


def test_worker_owner_command_pins_live_gateway_without_joining_its_cgroup(tmp_path, monkeypatch):
    entry = tmp_path / "worker.py"
    entry.write_text("print('worker')\n")
    receipt = tmp_path / "release.json"
    monkeypatch.setattr(wrapper, "ROOT", tmp_path)
    monkeypatch.setattr(wrapper, "ENTRY", entry)
    monkeypatch.setattr(wrapper, "configured_release", lambda _: receipt)
    monkeypatch.setattr(wrapper, "native_owner_pid", lambda *a, **k: 37)
    verified = []
    monkeypatch.setattr(wrapper, "verify_owner_namespace", lambda *a, **k: verified.append((a, k)))
    command, env = wrapper.owner_command(["--signal-cache-only"])
    assert verified
    assert command[:4] == ["nsenter", "--target", "37", "--mount"]
    assert "--join-cgroup" not in command and "--all" not in command
    assert command[-1] == "--signal-cache-only"
    assert env["STOCKAGENT_MAINTENANCE_RELEASE"] == str(receipt)
    assert env["STOCKAGENT_MAINTENANCE_OWNER_PID"] == "37"
    assert env["STOCKAGENT_MAINTENANCE_ENTRY_SHA256"] == wrapper._stable_entry_digest(entry)


def test_worker_owner_command_rejects_missing_or_changed_gateway(tmp_path, monkeypatch):
    monkeypatch.setattr(wrapper, "ROOT", tmp_path)
    monkeypatch.setattr(wrapper, "configured_release", lambda _: None)
    with pytest.raises(RuntimeError, match="no verified pinned"):
        wrapper.owner_command([])
    receipt = tmp_path / "release.json"
    monkeypatch.setattr(wrapper, "configured_release", lambda _: receipt)
    pids = iter([37, 38])
    monkeypatch.setattr(wrapper, "native_owner_pid", lambda *a, **k: next(pids))
    monkeypatch.setattr(wrapper, "verify_owner_namespace", lambda *a, **k: None)
    with pytest.raises(RuntimeError, match="changed during pinning"):
        wrapper.owner_command([])


def test_worker_source_guard_fails_closed_on_gateway_restart(monkeypatch, tmp_path):
    receipt = tmp_path / "release.json"
    monkeypatch.setenv("STOCKAGENT_MAINTENANCE_OWNER_PID", "37")
    monkeypatch.setenv("STOCKAGENT_MAINTENANCE_RELEASE", str(receipt))
    monkeypatch.setattr(service_code_release, "configured_release", lambda _: receipt)
    monkeypatch.setattr(service_code_release, "native_owner_pid", lambda *a, **k: 37)
    maintenance._verify_maintenance_gateway_generation()
    monkeypatch.setattr(service_code_release, "native_owner_pid", lambda *a, **k: 38)
    with pytest.raises(RuntimeError, match="changed during maintenance"):
        maintenance._verify_maintenance_gateway_generation()
    monkeypatch.delenv("STOCKAGENT_MAINTENANCE_RELEASE")
    with pytest.raises(RuntimeError, match="incomplete"):
        maintenance._verify_maintenance_gateway_generation()


def test_entry_pin_rejects_symlink(tmp_path):
    target = tmp_path / "worker.py"
    target.write_text("pass\n")
    alias = tmp_path / "alias.py"
    alias.symlink_to(target)
    with pytest.raises(OSError):
        wrapper._stable_entry_digest(alias)
