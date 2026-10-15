from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import zipfile

import pytest

from scripts import maintain_shioaji_sdk as sdk


def release(version="1.7.8", **changes):
    row = {"filename": f"shioaji-{version}-cp37-abi3-manylinux_2_17_x86_64.whl",
           "packagetype": "bdist_wheel", "yanked": False, "requires_python": ">=3.7",
           "url": "https://files.pythonhosted.org/packages/sdk.whl",
           "digests": {"sha256": "0" * 64}, "size": 5} | changes
    return {"info": {"version": version, "requires_python": ">=3.7"}, "urls": [row]}


@pytest.mark.parametrize("hour,minute,allowed", [
    (5, 0, False), (5, 14, False), (5, 15, True), (5, 30, True),
    (6, 29, True), (6, 30, False), (8, 20, False), (9, 0, False),
    (14, 30, False), (15, 0, False), (23, 0, False), (0, 1, False),
])
def test_closed_session_window_does_not_depend_on_guessed_holiday(hour, minute, allowed):
    local = datetime(2026, 10, 14, hour, minute, tzinfo=sdk.TAIPEI)
    assert sdk.maintenance_window(local) is allowed
    assert sdk.maintenance_window(local.astimezone(timezone.utc)) is allowed


def test_naive_maintenance_clock_is_rejected():
    with pytest.raises(ValueError, match="timezone"):
        sdk.maintenance_window(datetime(2026, 10, 14, 5, 30))


def test_latest_stable_wheel_and_rollback_have_distinct_yank_policy():
    assert sdk.select_wheel(release())["filename"].startswith("shioaji-1.7.8-")
    with pytest.raises(ValueError, match="non-yanked"):
        sdk.select_wheel(release(yanked=True))
    assert sdk.select_wheel(release(yanked=True), rollback=True)["yanked"]


@pytest.mark.parametrize("version", ["1.8.0rc1", "1.8.0.dev1"])
def test_preview_is_not_automatically_promoted(version):
    with pytest.raises(ValueError, match="stable"):
        sdk.select_wheel(release(version))


@pytest.mark.parametrize("changes", [
    {"url": "http://files.pythonhosted.org/packages/sdk.whl"},
    {"url": "https://other.example/sdk.whl"},
    {"filename": "../shioaji-1.7.8-cp37-abi3-manylinux_2_17_x86_64.whl"},
    {"filename": "shioaji-1.7.7-cp37-abi3-manylinux_2_17_x86_64.whl"},
    {"filename": "other-1.7.8-cp37-abi3-manylinux_2_17_x86_64.whl"},
    {"requires_python": ">=9"},
    {"filename": "shioaji-1.7.8-cp37-abi3-win32.whl"},
])
def test_wrong_source_version_platform_or_python_cannot_be_installed(changes):
    with pytest.raises(ValueError):
        sdk.select_wheel(release(**changes))


def test_wheel_metadata_preserves_other_dependencies_and_extras(tmp_path):
    wheel = tmp_path / "fixture.whl"
    body = "Name: shioaji\nVersion: 1.7.8\nRequires-Dist: definitely_missing_sdk_dependency; extra == 'test'\n"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("shioaji-1.7.8.dist-info/METADATA", body)
    sdk.check_dependencies(wheel, "1.7.8")
    with pytest.raises(ValueError, match="identity"):
        sdk.check_dependencies(wheel, "1.7.7")


@pytest.fixture
def transaction(tmp_path, monkeypatch):
    installed = {"version": "1.7.0"}
    enabled = {sdk.OWNERS[0], sdk.OWNERS[2], sdk.TIMERS[0]}
    running = set(enabled)
    commands = []
    failing = {"install": False, "start": False}
    wheel = tmp_path / "candidate.whl"
    wheel.write_bytes(b"new-sdk")
    rollback = tmp_path / "rollback.whl"
    rollback.write_bytes(b"old-sdk")
    candidate = {"version": "1.7.8", "fingerprint": "accepted-source",
                 "wheel": str(wheel), "wheel_sha256": sdk.sha(wheel)}
    monkeypatch.setattr(sdk, "fingerprint", lambda: "accepted-source")
    monkeypatch.setattr(sdk, "now", lambda: datetime(2026, 10, 14, 5, 30, tzinfo=sdk.TAIPEI))
    monkeypatch.setattr(sdk.metadata, "version", lambda name: installed["version"])
    monkeypatch.setattr(sdk, "native_users", lambda: [])
    monkeypatch.setattr(sdk, "active", lambda unit: unit in running)

    def systemctl(action, units):
        commands.append((action, list(units)))
        if action == "stop":
            running.difference_update(units)
        elif action == "start":
            if failing["start"]:
                failing["start"] = False
                raise RuntimeError("owner failed startup")
            running.update(units)

    def execute(argv, log, **kwargs):
        commands.append(("execute", argv))
        if "install" in argv:
            if argv[-1] == str(wheel) and failing["install"]:
                raise RuntimeError("pip failed mid-replacement")
            installed["version"] = "1.7.8" if argv[-1] == str(wheel) else "1.7.0"

    monkeypatch.setattr(sdk, "systemctl", systemctl)
    monkeypatch.setattr(sdk, "execute", execute)
    return tmp_path, candidate, rollback, installed, enabled, running, commands, failing


def test_upgrade_stops_native_owners_first_and_restores_only_original_units(transaction):
    state, candidate, rollback, installed, original, running, commands, _ = transaction
    result = sdk.promote(candidate, rollback, state)
    assert result["state"] == "updated"
    assert installed["version"] == "1.7.8"
    assert running == original
    assert commands[0][0] == commands[1][0] == "stop"
    assert commands[2][0] == "execute"
    assert "--no-deps" in commands[2][1] and "--no-index" in commands[2][1]
    assert json.loads((state / "transaction.json").read_text())["phase"] == "complete"


@pytest.mark.parametrize("failure", ["install", "start"])
def test_replacement_or_owner_failure_restores_the_exact_previous_wheel(transaction, failure):
    state, candidate, rollback, installed, original, running, commands, failing = transaction
    failing[failure] = True
    with pytest.raises(RuntimeError):
        sdk.promote(candidate, rollback, state)
    assert installed["version"] == "1.7.0"
    assert running == original
    assert any(cmd[0] == "execute" and cmd[1][-1] == str(rollback) for cmd in commands)
    assert json.loads((state / "transaction.json").read_text())["phase"] == "recovered"


def test_crash_journal_recovers_before_next_market_window(transaction, monkeypatch):
    state, candidate, rollback, installed, original, running, commands, _ = transaction
    journal = {"phase": "installing", "python": sys.executable, "old_version": "1.7.0",
               "rollback_wheel": str(rollback), "rollback_sha256": sdk.sha(rollback),
               "active_owners": [sdk.OWNERS[0], sdk.OWNERS[2]], "active_timers": [sdk.TIMERS[0]]}
    sdk.evidence(state, "transaction.json", journal)
    running.clear()
    installed["version"] = "partially-written-sdk"
    monkeypatch.setattr(sdk, "now", lambda: datetime(2026, 10, 14, 8, 30, tzinfo=sdk.TAIPEI))
    assert sdk.recover(state)["state"] == "recovered_previous_sdk"
    assert installed["version"] == "1.7.0" and running == original


def test_corrupt_rollback_or_unreviewed_owner_is_not_executed(transaction):
    state, _, rollback, _, _, _, commands, _ = transaction
    sdk.evidence(state, "transaction.json", {"phase": "installing", "python": sys.executable,
        "active_owners": ["unrelated.service"], "active_timers": [],
        "rollback_wheel": str(rollback), "rollback_sha256": sdk.sha(rollback)})
    with pytest.raises(ValueError, match="unreviewed"):
        sdk.recover(state)
    assert commands == []


@pytest.mark.parametrize("bad", ["clock", "source", "wheel"])
def test_invalid_promotion_has_no_service_side_effects(transaction, monkeypatch, bad):
    state, candidate, rollback, _, _, _, commands, _ = transaction
    if bad == "clock":
        monkeypatch.setattr(sdk, "now", lambda: datetime(2026, 10, 14, 9, 0, tzinfo=sdk.TAIPEI))
    elif bad == "source":
        candidate["fingerprint"] = "changed-source"
    else:
        Path(candidate["wheel"]).write_bytes(b"tampered")
    with pytest.raises(ValueError):
        sdk.promote(candidate, rollback, state)
    assert commands == []


def test_unmanaged_native_owner_is_not_killed(transaction, monkeypatch):
    state, candidate, rollback, _, _, _, commands, _ = transaction
    monkeypatch.setattr(sdk, "native_users", lambda: [{"pid": 123, "unit": None}])
    assert sdk.promote(candidate, rollback, state)["state"] == "waiting_unmanaged_sdk_owner"
    assert commands == []


def test_late_persistent_timer_is_version_check_only(transaction, monkeypatch):
    state, _, _, _, original, running, commands, _ = transaction
    monkeypatch.setattr(sdk, "now", lambda: datetime(2026, 10, 14, 9, 0, tzinfo=sdk.TAIPEI))
    monkeypatch.setattr(sdk, "official_release", lambda *args: release())
    def forbidden_stage(*args):
        raise AssertionError("a late timer must not stage/install/restart during trading")
    monkeypatch.setattr(sdk, "stage", forbidden_stage)
    result = sdk.maintain(state, apply=True)
    assert result["state"] == "update_available"
    assert commands == [] and running == original


def test_latest_disk_version_does_not_hide_stale_native_process(transaction, monkeypatch):
    state, _, _, installed, _, _, commands, _ = transaction
    installed["version"] = "1.7.8"
    monkeypatch.setattr(sdk, "official_release", lambda *args: release())
    monkeypatch.setattr(sdk, "native_users", lambda: [{"pid": 123, "matches_installed_native": False}])
    assert sdk.maintain(state)["state"] == "loaded_version_mismatch"
    assert commands == []


def test_unexpected_native_process_during_stop_restores_original_units(transaction, monkeypatch):
    state, candidate, rollback, installed, original, running, commands, _ = transaction
    calls = iter([[], [{"pid": 123, "unit": None}], [{"pid": 123, "unit": None}]])
    monkeypatch.setattr(sdk, "native_users", lambda: next(calls))
    with pytest.raises(RuntimeError, match="remains"):
        sdk.promote(candidate, rollback, state)
    assert installed["version"] == "1.7.0" and running == original
    assert not any(action == "execute" for action, _ in commands)


def test_native_map_inode_detects_old_loaded_binary(tmp_path):
    prefix = tmp_path / "runtime"
    native = prefix / "lib/shioaji/_core.abi3.so"
    native.parent.mkdir(parents=True)
    native.write_bytes(b"current-sdk")
    proc = tmp_path / "proc"
    process = proc / "123"
    process.mkdir(parents=True)
    (process / "cgroup").write_text("0::/system.slice/stockagent-tw-day-trade-simulation.service\n")
    (process / "maps").write_text(f"0000-1000 r-xp 0000 00:00 {native.stat().st_ino} {native}\n")
    assert sdk.native_users(proc, prefix)[0]["matches_installed_native"]
    (process / "maps").write_text(f"0000-1000 r-xp 0000 00:00 999 {native} (deleted)\n")
    assert not sdk.native_users(proc, prefix)[0]["matches_installed_native"]


def test_download_hash_mismatch_is_never_published(tmp_path, monkeypatch):
    from io import BytesIO
    class Response(BytesIO):
        def geturl(self):
            return "https://files.pythonhosted.org/packages/sdk.whl"
    monkeypatch.setattr(sdk, "urlopen", lambda *args, **kwargs: Response(b"wrong"))
    with pytest.raises(ValueError, match="SHA256"):
        sdk.download_wheel(release()["urls"][0], tmp_path)
    assert not list(tmp_path.glob("*.whl"))


def test_update_wrapper_and_timer_keep_canonical_runtime_and_skip_opening():
    root = Path(__file__).resolve().parents[1]
    wrapper = (root / "scripts/run_shioaji_sdk_maintenance.sh").read_text()
    timer = (root / "deploy/systemd/stockagent-shioaji-sdk-maintenance.timer.in").read_text()
    service = (root / "deploy/systemd/stockagent-shioaji-sdk-maintenance.service.in").read_text()
    assert "source scripts/runtime_env.sh" in wrapper and "exec_fintech_python" in wrapper
    assert "05:30:00 Asia/Taipei" in timer and "*:40:00 Asia/Taipei" in timer
    assert "Persistent=true" in timer
    assert "ExecStopPost=" in service and " recover" in service
