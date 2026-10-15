from datetime import datetime, timedelta, timezone
import json

import pytest

from scripts import verify_shioaji_history_audit_runtime as runtime


def test_log_readback_requires_both_real_canonical_markers(tmp_path, monkeypatch):
    path = tmp_path / "run.log"
    path.write_text("previous logs\n")
    monkeypatch.setattr(runtime, "LOG", path)
    control = {"log_position": runtime.log_position()}
    with path.open("a") as handle:
        handle.write("[shioaji-history] provider=x pending=0 contracts=10 summary_strategy=single_pass_receipt_v1\n")
        handle.write("[shioaji-history] summary_audit query=0 kind=final coverage_passes=1 elapsed_seconds=12.300\n")
    audit = runtime.recent_audit(control)
    assert audit["startup_markers"] == 1
    assert audit["final_audits"] == [{"queries": 0, "seconds": 12.3}]


def test_log_rotation_is_not_silent_adoption(tmp_path, monkeypatch):
    path = tmp_path / "run.log"
    path.write_text("previous logs\n")
    monkeypatch.setattr(runtime, "LOG", path)
    control = {"log_position": runtime.log_position()}
    path.unlink()
    path.write_text("new")
    with pytest.raises(ValueError, match="rotated_or_truncated"):
        runtime.recent_audit(control)


@pytest.mark.parametrize("changed", ["owner", "protected", "code_sha256"])
def test_native_owner_and_code_fences(tmp_path, monkeypatch, changed):
    control = {"state": "ready_for_natural_batch", "owner": {"id": "old"},
               "protected": {}, "code_sha256": {"x": "old"}}
    result = {**control, changed: "changed"}
    with pytest.raises(ValueError, match="native_adoption_control_changed"):
        runtime.readback(control, result)


def test_running_batch_is_waiting_not_success_and_gaps_stay_visible(tmp_path, monkeypatch):
    now = datetime(2026, 10, 14, tzinfo=timezone.utc)
    summary = tmp_path / "summary.json"
    scheduler = tmp_path / "scheduler.json"
    monkeypatch.setattr(runtime, "SUMMARY", summary)
    monkeypatch.setattr(runtime, "RUN_ROOT", tmp_path)
    monkeypatch.setattr(runtime, "recent_audit", lambda _: {
        "startup_markers": 1, "final_audits": [{"queries": 0, "seconds": 1}],
    })
    control = {"state": "ready_for_natural_batch", "owner": {}, "protected": {},
               "code_sha256": {}, "observed_at_utc": now.isoformat()}
    written = now + timedelta(seconds=30)
    summary.write_text(json.dumps({
        "source": "shioaji_historical_market_data_v2", "schema_version": 2,
        "written_at_utc": written.isoformat(), "state": "waiting_source",
        "methods": ["kbars", "ticks"], "contracts": 5, "coverage_state": "source_gaps",
    }))
    scheduler.write_text(json.dumps({"state": "waiting", "reason": "await_catalog_or_retry_deadline",
                                     "observed_at_utc": (now + timedelta(seconds=10)).isoformat()}))
    result = {**control}
    runtime.readback(control, result)
    assert result["state"] == "waiting_next_natural_batch"
    scheduler.write_text(json.dumps({"state": "waiting", "reason": "await_catalog_or_retry_deadline",
                                     "observed_at_utc": (written + timedelta(seconds=10)).isoformat()}))
    result = {**control}
    runtime.readback(control, result)
    assert result["state"] == "accepted_native_audit_adoption"
    assert result["all_source_data_complete"] is False


def test_no_new_audit_does_not_read_source_or_claim_success(monkeypatch):
    monkeypatch.setattr(runtime, "recent_audit", lambda _: {"startup_markers": 0, "final_audits": []})
    control = {"state": "ready_for_natural_batch", "owner": {}, "protected": {}, "code_sha256": {}}
    result = {**control}
    runtime.readback(control, result)
    assert result["state"] == "waiting_next_natural_batch"
