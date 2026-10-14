"""Native event bridge gates without any systemd/GPU/source operation."""
from copy import deepcopy
import json
from types import SimpleNamespace

import pytest

from scripts import verify_postclose_source_event_runtime as event


@pytest.fixture
def portable(tmp_path, monkeypatch):
    monkeypatch.setattr(event, "ROOT", tmp_path)
    fragment = tmp_path / "native.path"
    template = tmp_path / "deploy/systemd/stockagent-discord-artifact-maintenance.path.in"
    template.parent.mkdir(parents=True)
    template.write_text("[Path]\nPathChanged=@REPO_ROOT@/close.json\n")
    fragment.write_text(template.read_text().replace("@REPO_ROOT@", str(tmp_path)))
    watches = {str(tmp_path / "close.json"), str(tmp_path / "twse.parquet"), str(tmp_path / "tpex.parquet")}
    monkeypatch.setattr(event, "WATCHES", watches)
    state = {"ActiveState": "active", "SubState": "waiting", "Triggers": event.CACHE_UNIT,
             "FragmentPath": str(fragment),
             "Paths": " ".join(f"{path} (PathChanged)" for path in watches)}
    monkeypatch.setattr(event, "show", lambda *_: state)
    return state, fragment


def test_requires_exact_loaded_watch_set_and_canonical_fragment(portable):
    state, _ = portable
    assert event.check_wire() == state


def test_real_systemctl_repeated_list_properties_are_not_truncated(monkeypatch):
    completed = SimpleNamespace(stdout="ActiveState=active\nPaths=/first (PathChanged)\n"
                                      "Paths=/second (PathChanged)\nPaths=/third (PathChanged)\n")
    monkeypatch.setattr(event.subprocess, "run", lambda *_, **__: completed)
    state = event.show(event.PATH_UNIT, ("ActiveState", "Paths"))
    assert state["ActiveState"] == "active"
    assert state["Paths"] == "/first (PathChanged) /second (PathChanged) /third (PathChanged)"


@pytest.mark.parametrize("failure", ["heartbeat", "formal_history", "path_modified", "inactive", "fragment"])
def test_rejects_wrong_owner_polling_or_config_drift(portable, failure):
    state, fragment = portable
    if failure == "heartbeat":
        state["Paths"] += " /tmp/eligibility/latest.json (PathChanged)"
    elif failure == "formal_history":
        state["Triggers"] = "stockagent-discord-artifact-maintenance.service"
    elif failure == "path_modified":
        state["Paths"] = state["Paths"].replace("PathChanged", "PathModified")
    elif failure == "inactive":
        state["ActiveState"] = "inactive"
    else:
        fragment.write_text("changed")
    with pytest.raises(ValueError):
        event.check_wire()


def test_observe_same_live_handle_until_actual_terminal_not_timeout(monkeypatch):
    observations = iter([
        {"ActiveState": "inactive", "ExecMainStartTimestampMonotonic": "10"},
        {"ActiveState": "activating", "ExecMainPID": "123", "ExecMainStartTimestampMonotonic": "20"},
        {"ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0",
         "ExecMainPID": "123", "ExecMainStartTimestampMonotonic": "20"},
    ])
    monkeypatch.setattr(event, "worker", lambda: next(observations))
    monkeypatch.setattr(event.time, "sleep", lambda _: None)
    terminal, rows = event.await_terminal({"ExecMainStartTimestampMonotonic": "10"})
    assert terminal["ExecMainPID"] == "123"
    assert len(rows) == 3


def test_observation_timeout_is_not_terminal_or_restart_permission(monkeypatch):
    monkeypatch.setattr(event, "worker", lambda: {"ActiveState": "activating", "ExecMainPID": "123",
                                                 "ExecMainStartTimestampMonotonic": "20"})
    with pytest.raises(TimeoutError, match="not_terminal"):
        event.await_terminal({"ExecMainStartTimestampMonotonic": "10"}, deadline_seconds=0)


@pytest.mark.parametrize("mutation", [None, "source", "protected", "worker", "code"])
def test_complete_readback_rejects_drift_and_retains_failure(tmp_path, monkeypatch, mutation):
    owners = {unit: {"ActiveState": "active", "InvocationID": unit} for unit in (*event.PROTECTED, event.UNIT)}
    snapshot = {"complete_outputs": ["unchanged"]}
    worker = {"ActiveState": "inactive", "ExecMainStartTimestampMonotonic": "20"}
    proofs = {"current": "code"}
    monkeypatch.setattr(event, "identity", lambda unit: owners[unit])
    monkeypatch.setattr(event, "fingerprints", lambda: snapshot)
    monkeypatch.setattr(event, "worker", lambda: worker)
    monkeypatch.setattr(event, "code_proofs", lambda: proofs)
    monkeypatch.setattr(event, "check_wire", lambda **_: {"native": "canonical"})
    monkeypatch.setattr(event, "check_cache_only_owner", lambda: {"Type": "oneshot"})
    control = {"protected": deepcopy(owners), "cache": deepcopy(snapshot),
               "worker": deepcopy(worker), "code_sha256": deepcopy(proofs)}
    if mutation == "source":
        control["cache"] = {"complete_outputs": ["changed"]}
    elif mutation == "protected":
        control["protected"][event.UNIT] = {"ActiveState": "active", "InvocationID": "old"}
    elif mutation == "worker":
        control["worker"]["ExecMainStartTimestampMonotonic"] = "10"
    elif mutation == "code":
        control["code_sha256"] = {"current": "different"}
    previous = tmp_path / "control.json"
    previous.write_text(json.dumps(control))
    args = SimpleNamespace(output=tmp_path / "receipt.json", baseline=False, notify=False, control=previous)
    if mutation:
        with pytest.raises(ValueError):
            event.run(args)
        assert json.loads(args.output.read_bytes())["state"] == "failed_verification"
    else:
        assert event.run(args)["state"] == "normal_watch_set_and_stable_terminal_worker_verified"


def test_committed_rule_files_not_mutable_publisher_heartbeat_wake_only_cache():
    from pathlib import Path
    text = Path("deploy/systemd/stockagent-discord-artifact-maintenance.path.in").read_text()
    watched = [line.removeprefix("PathChanged=") for line in text.splitlines() if line.startswith("PathChanged=")]
    assert len(watched) == 3
    assert any(path.endswith("completed_session/latest.json") for path in watched)
    for venue in ("twse", "tpex"):
        assert f"/srv/stockagent-live/data_tw_public/{venue}_day_trade_eligibility.parquet" in watched
    assert "tw_day_trade_eligibility/latest.json" not in text
    assert "Unit=stockagent-discord-postclose-cache.service" in text
    assert "PathModified=" not in text and "PathExists=" not in text


@pytest.mark.parametrize("failure", [None, "persistent", "formal_history", "extra_args", "override"])
def test_native_dispatch_cannot_wake_full_history_or_an_unreviewed_owner(monkeypatch, failure):
    argv = f"/usr/bin/bash {event.ROOT}/scripts/run_discord_artifact_maintenance.sh --signal-cache-only"
    state = {"Type": "oneshot", "ExecStart": f"{{ argv[]={argv} ; ignore_errors=no }}", "DropInPaths": ""}
    if failure == "persistent":
        state["Type"] = "simple"
    elif failure == "formal_history":
        state["ExecStart"] = state["ExecStart"].replace(" --signal-cache-only", "")
    elif failure == "extra_args":
        state["ExecStart"] = state["ExecStart"].replace(" ; ignore_errors", " --other-mode ; ignore_errors")
    elif failure == "override":
        state["DropInPaths"] = "unreviewed.conf"
    monkeypatch.setattr(event, "show", lambda *_: state)
    if failure:
        with pytest.raises(ValueError):
            event.check_cache_only_owner()
    else:
        assert event.check_cache_only_owner() == state
