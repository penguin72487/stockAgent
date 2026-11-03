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
    monkeypatch.setattr(wrapper, "_verified_interop_for_maintenance", lambda: "/run/WSL/verified_interop")
    command, env = wrapper.owner_command(["--signal-cache-only"])
    assert verified
    assert command[:4] == ["nsenter", "--target", "37", "--mount"]
    assert "--join-cgroup" not in command and "--all" not in command
    assert command[-1] == "--signal-cache-only"
    assert env["STOCKAGENT_MAINTENANCE_RELEASE"] == str(receipt)
    assert env["STOCKAGENT_MAINTENANCE_OWNER_PID"] == "37"
    assert env["STOCKAGENT_MAINTENANCE_ENTRY_SHA256"] == wrapper._stable_entry_digest(entry)
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert env["WSL_INTEROP"] == "/run/WSL/verified_interop"


def test_worker_verify_only_does_not_probe_windows_interop(tmp_path, monkeypatch):
    entry = tmp_path / "worker.py"
    entry.write_text("pass\n")
    monkeypatch.setattr(wrapper, "ROOT", tmp_path)
    monkeypatch.setattr(wrapper, "ENTRY", entry)
    monkeypatch.setattr(wrapper, "configured_release", lambda _: tmp_path / "release.json")
    monkeypatch.setattr(wrapper, "native_owner_pid", lambda *a, **k: 37)
    monkeypatch.setattr(wrapper, "verify_owner_namespace", lambda *a, **k: None)
    monkeypatch.setattr(wrapper, "_verified_interop_for_maintenance",
                        lambda: pytest.fail("code-only probe must not depend on Windows"))
    _, env = wrapper.owner_command([], verify_only=True)
    assert env["STOCKAGENT_MAINTENANCE_VERIFY_ONLY"] == "1"
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"


def test_worker_missing_interop_never_bypasses_existing_checks(tmp_path, monkeypatch):
    entry = tmp_path / "worker.py"
    entry.write_text("pass\n")
    monkeypatch.setattr(wrapper, "ROOT", tmp_path)
    monkeypatch.setattr(wrapper, "ENTRY", entry)
    monkeypatch.setattr(wrapper, "configured_release", lambda _: tmp_path / "release.json")
    monkeypatch.setattr(wrapper, "native_owner_pid", lambda *a, **k: 37)
    monkeypatch.setattr(wrapper, "verify_owner_namespace", lambda *a, **k: None)
    monkeypatch.setenv("WSL_INTEROP", "/run/WSL/unverified_interop")
    monkeypatch.setattr(wrapper, "_verified_interop_for_maintenance", lambda: None)
    _, env = wrapper.owner_command([])
    assert "WSL_INTEROP" not in env
    assert env["STOCKAGENT_MAINTENANCE_ENTRY_SHA256"] == wrapper._stable_entry_digest(entry)


def test_interop_assist_only_returns_verified_socket_and_handles_failures(monkeypatch):
    from stockagent.data_sync import windows_cold_io
    from stockagent.data_sync.desync_snapshots import SnapshotError

    monkeypatch.setattr(windows_cold_io, "_windows_process_environment",
                        lambda: {"WSL_INTEROP": "/run/WSL/123_interop", "SECRET": "ignored"})
    assert wrapper._verified_interop_for_maintenance() == "/run/WSL/123_interop"
    monkeypatch.setattr(windows_cold_io, "_windows_process_environment", lambda: None)
    assert wrapper._verified_interop_for_maintenance() is None
    def unavailable():
        raise SnapshotError("unavailable")
    monkeypatch.setattr(windows_cold_io, "_windows_process_environment", unavailable)
    assert wrapper._verified_interop_for_maintenance() is None


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


@pytest.mark.parametrize("arguments", sorted(wrapper.RECEIPT_CONTROL_ARGS))
def test_receipt_owner_command_retains_source_checks_without_windows_probe(tmp_path, monkeypatch, arguments):
    entry = tmp_path / "worker.py"
    entry.write_text("pass\n")
    monkeypatch.setattr(wrapper, "ROOT", tmp_path)
    monkeypatch.setattr(wrapper, "ENTRY", entry)
    monkeypatch.setattr(wrapper, "configured_release", lambda _: tmp_path / "release.json")
    monkeypatch.setattr(wrapper, "native_owner_pid", lambda *a, **k: 37)
    verified = []
    monkeypatch.setattr(wrapper, "verify_owner_namespace", lambda *a, **k: verified.append((a, k)))
    monkeypatch.setattr(wrapper, "_verified_interop_for_maintenance",
                        lambda: pytest.fail("receipt-only probe must not initialize Windows I/O"))
    command, env = wrapper.owner_command(list(arguments))
    assert verified and command[-1:] == list(arguments)
    assert env["PYTHONDONTWRITEBYTECODE"] == "1"
    assert "STOCKAGENT_MAINTENANCE_VERIFY_ONLY" not in env


class _ProcessReplaced(BaseException):
    pass


@pytest.mark.parametrize("arguments", sorted(wrapper.RECEIPT_CONTROL_ARGS))
def test_receipt_controller_prefers_verified_worker_namespace(monkeypatch, arguments):
    monkeypatch.setattr(wrapper.sys, "argv", [str(wrapper.__file__), *arguments])
    requested = []
    def owner_command(args, *, verify_only):
        requested.append((args, verify_only))
        return ["nsenter", "verified-owner", *args], {"verified": "yes"}
    def replace(file, argv, env):
        assert file == "nsenter" and argv[-1:] == list(arguments)
        assert env == {"verified": "yes"}
        raise _ProcessReplaced
    monkeypatch.setattr(wrapper, "owner_command", owner_command)
    monkeypatch.setattr(wrapper.os, "execvpe", replace)
    monkeypatch.setattr(wrapper.os, "execv", lambda *a: pytest.fail("healthy probe must use the worker view"))
    with pytest.raises(_ProcessReplaced):
        wrapper.main()
    assert requested == [(list(arguments), False)]


@pytest.mark.parametrize("arguments", sorted(wrapper.RECEIPT_CONTROL_ARGS))
def test_receipt_controller_recovers_independently_of_gateway(monkeypatch, capsys, arguments):
    monkeypatch.setattr(wrapper.sys, "argv", [str(wrapper.__file__), *arguments])
    def unavailable(*a, **k):
        raise RuntimeError("Gateway owner unavailable or source changed")
    def replace(file, argv):
        assert file == wrapper.sys.executable
        assert argv == [file, str(wrapper.ENTRY), *arguments]
        raise _ProcessReplaced
    monkeypatch.setattr(wrapper, "owner_command", unavailable)
    monkeypatch.setattr(wrapper.os, "execv", replace)
    monkeypatch.setattr(wrapper.os, "execvpe", lambda *a: pytest.fail("unavailable owner must not be entered"))
    with pytest.raises(_ProcessReplaced):
        wrapper.main()
    assert '"status": "receipt_controller_workspace_fallback"' in capsys.readouterr().out


@pytest.mark.parametrize("arguments", [[], ["--signal-cache-only"], ["--verify-code-only"]])
def test_inference_and_code_admission_never_use_workspace_fallback(monkeypatch, capsys, arguments):
    monkeypatch.setattr(wrapper.sys, "argv", [str(wrapper.__file__), *arguments])
    def unavailable(*a, **k):
        raise RuntimeError("Gateway source changed")
    monkeypatch.setattr(wrapper, "owner_command", unavailable)
    monkeypatch.setattr(wrapper.os, "execv", lambda *a: pytest.fail("inference must remain fail-closed"))
    with pytest.raises(SystemExit) as error:
        wrapper.main()
    assert error.value.code == 1
    assert '"status": "gateway_source_unavailable"' in capsys.readouterr().out


@pytest.mark.parametrize("arguments", [["--preopen-source-only"], ["--preopen-source-child"]])
def test_source_recovery_stays_independent_of_gateway(monkeypatch, arguments):
    monkeypatch.setattr(wrapper.sys, "argv", [str(wrapper.__file__), *arguments])
    monkeypatch.setattr(wrapper, "owner_command", lambda *a, **k: pytest.fail("source recovery is independent"))
    def replace(file, argv):
        assert argv == [file, str(wrapper.ENTRY), *arguments]
        raise _ProcessReplaced
    monkeypatch.setattr(wrapper.os, "execv", replace)
    with pytest.raises(_ProcessReplaced):
        wrapper.main()
