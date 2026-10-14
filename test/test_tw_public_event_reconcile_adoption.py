from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pytest
import torch

from scripts import verify_tw_public_event_reconcile as verify


@pytest.fixture
def native_fixture(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()
    proc_root = tmp_path / "proc"
    proc = proc_root / "123"
    proc.mkdir(parents=True)
    (proc / "exe").symlink_to(Path(sys.executable).resolve())
    (proc / "cwd").symlink_to(root)
    (proc / "cmdline").write_bytes(b"python\0scripts/watch_tw_public_source_events.py\0")
    cgroup = tmp_path / "cgroups" / verify.UNIT
    cgroup.mkdir(parents=True)
    (cgroup / "cgroup.procs").write_text("123\n")
    code = root / "events.py"
    code.write_text("reviewed code")
    state_path = root / "state.json"
    state = {
        "updated_at_taipei": datetime.now(timezone.utc).isoformat(),
        "refresh_in_progress": False,
        "runtime_contract": "completed_dependency_reconcile_v2",
        "runtime_source_sha256": verify.sha(code),
        "completed_session_finalize": {"status": "ok", "expected_date": "2026-10-14"},
    }
    state_path.write_text(json.dumps(state))
    owner = {"MainPID": "123", "InvocationID": "original", "ActiveState": "active"}
    monkeypatch.setattr(verify, "ROOT", root)
    monkeypatch.setattr(verify, "PROC_ROOT", proc_root)
    monkeypatch.setattr(verify, "CGROUP_ROOT", cgroup.parent)
    monkeypatch.setattr(verify, "CODE", (code,))
    monkeypatch.setattr(verify, "STATE", state_path)
    monkeypatch.setattr(verify, "LIVE", root / "live")
    monkeypatch.setattr(verify, "identity", lambda unit: dict(owner) if unit == verify.UNIT else {"InvocationID": unit})
    monkeypatch.setattr(verify.completed, "_latest_completed_taiex_session", lambda *_a, **_kw: "2026-10-14")
    monkeypatch.setattr(verify.completed, "_derived_state", lambda *_a, **_kw: {"current": True})
    monkeypatch.setattr(torch.cuda, "is_initialized", lambda: False)
    return owner, state_path, state, proc, cgroup, code


def test_current_owner_admission_and_independent_adoption(native_fixture):
    owner, _path, _state, _proc, _cg, _code = native_fixture
    before = verify.observe()
    assert before["state"] == "admitted_idle_event_owner"
    assert before["owner_runtime"]["matches_current_inode"]
    owner["InvocationID"] = "new"
    after = verify.observe(control=before)
    assert after["state"] == "accepted_native_event_adoption"
    assert after["provider_calls"] == 0 and not after["cuda_initialized"]


@pytest.mark.parametrize("damage,error", [
    ("busy", "active children"), ("pending", "work still pending"),
    ("old_owner", "has not loaded"), ("old_contract", "runtime identity"),
    ("old_code", "runtime identity"), ("failed", "not accepted"),
    ("stale_heartbeat", "not current"),
])
def test_live_adoption_refuses_unverified_or_busy_owners(native_fixture, damage, error):
    owner, path, state, _proc, cgroup, _code = native_fixture
    before = verify.observe()
    if damage != "old_owner":
        owner["InvocationID"] = "new"
    if damage == "busy":
        (cgroup / "cgroup.procs").write_text("123\n456\n")
    elif damage == "pending":
        state["completed_session_pending"] = {"reason": "waiting"}
    elif damage == "old_contract":
        state["runtime_contract"] = "old"
    elif damage == "old_code":
        state["runtime_source_sha256"] = "old"
    elif damage == "failed":
        state["completed_session_finalize"]["status"] = "failed"
    elif damage == "stale_heartbeat":
        state["updated_at_taipei"] = "2000-01-01T00:00:00+00:00"
    path.write_text(json.dumps(state))
    with pytest.raises(RuntimeError, match=error):
        verify.observe(control=before)


def test_current_receipt_flag_does_not_replace_real_validation(native_fixture, monkeypatch):
    owner, *_ = native_fixture
    before = verify.observe()
    owner["InvocationID"] = "new"
    monkeypatch.setattr(verify.completed, "_derived_state", lambda *_a, **_kw: {"current": False})
    with pytest.raises(RuntimeError, match="not accepted"):
        verify.observe(control=before)


def test_unknown_interpreter_is_not_admitted(native_fixture):
    _owner, _path, _state, proc, _cg, code = native_fixture
    (proc / "exe").unlink()
    (proc / "exe").symlink_to(code)
    with pytest.raises(RuntimeError, match="unexpected event owner/runtime"):
        verify.observe()


def test_exact_retired_runtime_is_recorded_before_but_rejected_after_reload(native_fixture, tmp_path, monkeypatch):
    owner, _path, _state, proc, _cg, _code = native_fixture
    selected = tmp_path / "python"
    selected.write_bytes(b"new selected interpreter")
    retired = tmp_path / "python (deleted)"
    retired.write_bytes(b"retired mapped interpreter")
    (proc / "exe").unlink()
    (proc / "exe").symlink_to(retired)
    monkeypatch.setattr(sys, "executable", str(selected))
    before = verify.observe()
    assert before["owner_runtime"]["matches_current_inode"] is False
    assert before["owner_runtime"]["loaded_sha256"] != before["owner_runtime"]["selected_sha256"]
    owner["InvocationID"] = "new"
    with pytest.raises(RuntimeError, match="unexpected event owner/runtime"):
        verify.observe(control=before)
