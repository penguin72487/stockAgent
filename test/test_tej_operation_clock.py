"""Different Windows/WSL wall clocks must not strand exact source evidence."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import connect
from downloader.tej_metadata_recovery import (
    CONTRACT, RETRY_SCHEDULED, failed_plan_proof, metadata_recovery_policy,
    recover_failed_metadata,
)
from test_tej_history import registry
from test_tej_metadata_recovery import CONFIG, FreshBridge, failed_metadata
from test_tej_metadata_contention import uncommitted_failure
from test_tej_discovery_response_recovery import discovery


def set_source_failure_clock(root, outcome, offset):
    """Keep host filesystem/commit order intact; skew only the remote clock."""
    boundary = datetime.now(UTC)
    value = json.loads(outcome.read_text())
    value["observed_at_utc"] = (boundary + timedelta(seconds=offset)).isoformat()
    atomic_write_json(outcome, value)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE traffic SET completed_at_utc=?", (datetime.now(UTC).isoformat(),))


def test_proven_metadata_failure_survives_positive_source_clock_skew(failed_metadata):
    _, root, task, prepared, outcome, *_ = failed_metadata
    set_source_failure_clock(root, outcome, 1.057264)
    before = prepared.read_bytes(), outcome.read_bytes()
    assert failed_plan_proof(root, task)["prepared"] == prepared
    bridge = FreshBridge()
    result = recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG))
    assert result["state"] == RETRY_SCHEDULED
    assert bridge.calls == ["inspect_notices", "confirm_metadata_error_cleared"]
    assert (prepared.read_bytes(), outcome.read_bytes()) == before


def test_changed_clock_validator_rechecks_only_local_evidence_cooldown(failed_metadata):
    _, root, task, _, outcome, *_ = failed_metadata
    set_source_failure_clock(root, outcome, 1.057264)
    old = {"contract": CONTRACT, "state": "metadata_evidence_requires_review",
           "attempted_at_utc": task["attempted_at_utc"], "failures": 63,
           "next_check_at_utc": (datetime.now(UTC) + timedelta(seconds=900)).isoformat()}
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO meta VALUES (?,?)", ("metadata_recovery." + task["task_id"], json.dumps(old)))
    result = recover_failed_metadata(root, FreshBridge(), policy=metadata_recovery_policy(CONFIG))
    assert result["state"] == RETRY_SCHEDULED
    assert result["backoff_seconds"] == 60  # Local validator failures are not source failures.


class SkewedReadback(FreshBridge):
    def __init__(self, skew):
        super().__init__()
        self.skew = skew

    def execute(self, root, task):
        payload, output, seconds = super().execute(root, task)
        stamp = datetime.fromisoformat(payload["observed_at_utc"])
        payload["observed_at_utc"] = (stamp + timedelta(seconds=self.skew)).isoformat()
        atomic_write_json(output, payload)
        return payload, output, seconds


@pytest.mark.parametrize("skew", [-1.1, 1.1])
def test_fresh_owned_interface_accepts_bounded_bidirectional_clock_skew(failed_metadata, skew):
    _, root, _, *_ = failed_metadata
    bridge = SkewedReadback(skew)
    result = recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG))
    assert result["state"] == RETRY_SCHEDULED
    assert "plan" not in bridge.calls and "download" not in bridge.calls


@pytest.mark.parametrize("offset", [-20, 5.1, 60])
def test_excessive_source_clock_skew_never_licenses_recovery(failed_metadata, offset):
    _, root, task, _, outcome, *_ = failed_metadata
    set_source_failure_clock(root, outcome, offset)
    bridge = FreshBridge()
    assert recover_failed_metadata(root, bridge, policy=metadata_recovery_policy(CONFIG)) is None
    assert bridge.calls == []
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "blocked"


def test_rolled_back_negative_classification_uses_same_clock_contract(uncommitted_failure):
    from downloader.tej_scheduler import recover_complete_local_response
    root, task, prepared, outcome, diagnostic = uncommitted_failure
    boundary = datetime.fromisoformat(json.loads(diagnostic.read_text())["observed_at_utc"])
    value = json.loads(outcome.read_text())
    value["observed_at_utc"] = (boundary + timedelta(seconds=1.1)).isoformat()
    atomic_write_json(outcome, value)
    # Local file writes must precede the local diagnostic/worker boundaries.
    doc = json.loads(diagnostic.read_text()); doc["observed_at_utc"] = datetime.now(UTC).isoformat()
    atomic_write_json(diagnostic, doc)
    atomic_write_json(root / "worker_status.json", {"state": "waiting_metadata", "observed_at_utc": datetime.now(UTC).isoformat()})
    original = [p.read_bytes() for p in (prepared, outcome, diagnostic)]
    assert recover_complete_local_response(root)
    assert [p.read_bytes() for p in (prepared, outcome, diagnostic)] == original
    with closing(connect(root)) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task["task_id"],)).fetchone()[0] == "blocked"


def test_complete_discovery_readback_accepts_remote_clock_not_future_local_action(discovery):
    from downloader.tej_scheduler import finished_discovery_response
    root, task, _, output, _ = discovery
    payload = json.loads(output.read_text())
    payload["observed_at_utc"] = (datetime.now(UTC) + timedelta(seconds=1.1)).isoformat()
    atomic_write_json(output, payload)
    atomic_write_json(root / "worker_status.json", {"state": "waiting_metadata", "observed_at_utc": datetime.now(UTC).isoformat()})
    assert finished_discovery_response(root, task["task_id"]) == output


