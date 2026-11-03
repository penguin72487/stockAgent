import json

import pytest

from scripts import run_finlab_finite_due_bridge as bridge_module
from scripts.run_finlab_finite_due_bridge import classify_pass


def summary(state):
    return "finlab_sync=" + json.dumps({"state": state, "required_pending": 3})


def test_pass_uses_child_summary_and_preserves_priority_handoff():
    assert classify_pass(0, [summary("partial")])[0] == "partial"
    assert classify_pass(0, [summary("partial"), summary("priority_handoff")])[0] == "priority_handoff"
    assert classify_pass(0, [summary("complete")])[0] == "owner_stopped"
    assert classify_pass(0, ["[finlab] another local sync is active; skipping this timer run"])[0] == "owner_busy"
    assert classify_pass(75, [])[0] == "capacity_deferred"
    assert classify_pass(1, [])[0] == "owner_failed"


def test_pass_fails_closed_without_owned_summary():
    with pytest.raises(ValueError, match="attributable"):
        classify_pass(0, [])
    with pytest.raises(ValueError, match="invalid owner summary"):
        classify_pass(0, ["finlab_sync={bad"])


def test_bridge_yields_busy_owner_and_stops_at_owner_boundary(tmp_path, monkeypatch):
    run = tmp_path / "artifacts/operations/agent-workflow/runs/prerequisite/run.json"
    run.parent.mkdir(parents=True)
    run.write_text(json.dumps({"state": "succeeded", "exit_code": 0}))
    calls = iter([(0, ["another local sync is active"]),
                  (0, [summary("partial")]), (0, [summary("complete")])])
    sleeps = []
    monkeypatch.setattr(bridge_module, "ROOT", tmp_path)
    monkeypatch.setattr(bridge_module, "OWNER_FILES", ())
    monkeypatch.setattr(bridge_module, "_run_owner", lambda: next(calls))
    monkeypatch.setattr(bridge_module.time, "sleep", sleeps.append)
    code, result = bridge_module.bridge(run, max_passes=4, wait_seconds=300)
    assert code == 0 and result["state"] == "owner_stopped_review_receipts"
    assert result["passes"] == 2 and result["owner_busy_waits"] == 1
    assert result["source_history_or_training_complete"] is False
    assert len(sleeps) == 1
