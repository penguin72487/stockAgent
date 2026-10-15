from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import verify_discord_artifact_wakeup_runtime as verifier


@pytest.fixture
def scope(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    templates = root / "deploy/systemd"
    templates.mkdir(parents=True)
    states = {}
    for unit in verifier.UNITS:
        content = "[Unit]\nDescription=@REPO_ROOT@\n"
        (templates / f"{unit}.in").write_text(content)
        installed = root / unit
        installed.write_text(content.replace("@REPO_ROOT@", str(root)))
        states[unit] = {"DropInPaths": "", "FragmentPath": str(installed),
                        "ActiveState": "active" if unit.endswith(".timer") else "inactive",
                        "Result": "success", "ExecMainStatus": "0"}
    states["stockagent-discord-artifact-maintenance.path"] = {"ActiveState": "failed", "Result": "resources"}
    protected = {"gateway": {"ActiveState": "active", "MainPID": "123", "InvocationID": "unchanged"}}
    monkeypatch.setattr(verifier, "ROOT", root)
    monkeypatch.setattr(verifier, "show", lambda unit: dict(states[unit]))
    monkeypatch.setattr(verifier, "protected", lambda: protected)
    monkeypatch.setattr(verifier, "code", lambda: {"code": "unchanged"})
    monkeypatch.setattr(verifier, "sys", SimpleNamespace(modules={}))
    monkeypatch.delenv("STOCKAGENT_POSTCLOSE_WAKEUP_STATUS_PATH", raising=False)
    snapshot = {"contract": verifier.wakeup.CONTRACT, "fingerprint": "current"}
    monkeypatch.setattr(verifier.wakeup, "observe", lambda *a, **kw: snapshot)
    artifact = root / "weights.parquet"
    artifact.write_bytes(b"weights")
    accepted = {**snapshot, "status": "canonical_artifacts_reusable",
                "artifact_generations": verifier.wakeup._generations([str(artifact)])}
    path = verifier.wakeup.accepted_path(root)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(accepted))
    control = root / "control.json"
    control.write_text(json.dumps({"protected": protected, "code_sha256": {"code": "unchanged"}}))
    args = SimpleNamespace(baseline=False, control=control, output=root / "readback.json")
    return root, states, protected, accepted, args


def test_native_readback_keeps_failed_event_path_visible_and_does_not_claim_consumer_acceptance(scope):
    _root, _states, _protected, _accepted, args = scope
    result = verifier.run(args)
    assert result["state"] == "native_wakeup_and_worker_readback_verified"
    assert result["path"]["Result"] == "resources"
    assert result["observer_service_controls"] == 0
    assert not result["canonical_consumer_reuse_verified"]


@pytest.mark.parametrize("failure", ["protected_restart", "changed_code", "unit_dropin", "wrong_template",
                                     "timer_stopped", "worker_running", "worker_failed", "old_source", "artifact_damage"])
def test_native_readback_never_accepts_an_unverified_or_incomplete_owner(scope, failure):
    root, states, _protected, accepted, args = scope
    if failure == "protected_restart":
        before = json.loads(args.control.read_bytes())
        before["protected"]["gateway"]["InvocationID"] = "old"
        args.control.write_text(json.dumps(before))
    elif failure == "changed_code":
        before = json.loads(args.control.read_bytes())
        before["code_sha256"] = {"code": "old"}
        args.control.write_text(json.dumps(before))
    elif failure == "unit_dropin":
        states[verifier.UNITS[0]]["DropInPaths"] = "unreviewed.conf"
    elif failure == "wrong_template":
        Path(states[verifier.UNITS[0]]["FragmentPath"]).write_text("wrong")
    elif failure == "timer_stopped":
        states[verifier.UNITS[1]]["ActiveState"] = "inactive"
    elif failure == "worker_running":
        states[verifier.wakeup.OWNER_UNIT]["ActiveState"] = "activating"
    elif failure == "worker_failed":
        states[verifier.wakeup.OWNER_UNIT]["Result"] = "exit-code"
    elif failure == "old_source":
        accepted["fingerprint"] = "old"
        verifier.wakeup.accepted_path(root).write_text(json.dumps(accepted))
    else:
        (root / "weights.parquet").write_bytes(b"damaged")
    with pytest.raises(ValueError):
        verifier.run(args)
    assert not args.output.exists()
