"""Previously missing history stays visible after its exact manifest is retired."""
import json
from copy import deepcopy

import pytest

from stockagent.data_sync.backup_history import apply_disposition, build_disposition
from stockagent.data_sync.backup_stream import capture_catalog, coverage, empty_ledger, recovery_index
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
from stockagent.runtime_identity import identity_sha256


@pytest.fixture
def retired_history(tmp_path):
    cold, source = tmp_path / "cold", tmp_path / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir()
    (source / "prices.txt").write_text("old price\n")
    old = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024)
    (source / "prices.txt").write_text("new price\n")
    current = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024)
    missing = old.manifest["archive"]["objects"][0]["relpath"]
    (cold / missing).unlink()
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"datasets": [{"dataset": "prices", "publish": True}]}))
    before = capture_catalog(cold, policy)
    decision = build_disposition(before, authorization="retain an explicit historical gap",
                                 evidence=[{"sha256": "a" * 64}])
    disposition = tmp_path / "disposition.json"
    disposition.write_text(json.dumps(decision))
    waiver = decision["releases"][0]
    refs = [{"relative": r["relpath"], "sha256": r["sha256"], "bytes": r["bytes"]}
            for r in [old.manifest["archive"]["inventory"], *old.manifest["archive"]["objects"]]]
    plan = {"versions": [{"relative": waiver["manifest_relative"], "snapshot_id": waiver["snapshot_id"],
                           "sha256": waiver["manifest_sha256"], "refs": refs}],
            "items": [{"kind": "manifest", "relative": waiver["manifest_relative"]}]}
    root = tmp_path / "retirements"
    stage = root / "prices" / ("b" * 64)
    stage.mkdir(parents=True)

    def write_plan(value, *, count=1):
        digest = identity_sha256(value)
        (stage / ("retirement-plan-" + digest + ".json")).write_text(json.dumps(value))
        journal = {"contract": "stockagent.superseded_repack_history.v1", "state": "completed",
                   "dataset": "prices", "plan_sha256": digest, "versions_removed": 1,
                   "plan_ref": "retirement-plan-" + digest + ".json", "removed_count": count}
        path = stage / ("history-retirement-" + digest + ".json")
        path.write_text(json.dumps(journal))
        return path

    journal = write_plan(plan)
    old.manifest_path.unlink()
    return locals()


def test_retired_missing_history_does_not_block_other_catalogs(retired_history):
    f = retired_history
    result = capture_catalog(f["cold"], f["policy"], f["disposition"],
                             history_retirement_root=f["root"])
    assert result["releases"][0]["snapshot_id"] == f["current"].manifest["snapshot_id"]
    record = result["abandoned_releases"][0]
    assert record["snapshot_id"] == f["old"].manifest["snapshot_id"]
    assert record["manifest_observed_in_current_catalog"] is False
    assert record["historical_recovery_claimed"] is False
    assert record["retirement_proof"]["plan_sha256"] == identity_sha256(f["plan"])
    assert result["retired_historical_missing_objects"][0]["relative"] == f["missing"]
    assert result["retired_historical_missing_objects"][0]["current_presence"] == (
        "not_rechecked_after_manifest_retirement")
    state = coverage(result, empty_ledger())
    assert state["observed_release_count"] == 1
    assert state["abandoned_history_release_count"] == 1
    assert state["retired_history_disposition_count"] == 1
    assert state["retired_historical_missing_object_count"] == 1
    assert state["all_history_backup_verified"] is False
    index = recovery_index(result, empty_ledger(), "a" * 64)
    assert index["retired_historical_missing_objects"] == result["retired_historical_missing_objects"]
    assert f["current"].manifest_path.exists()


def test_absent_history_without_exact_retirement_proof_still_rejects(retired_history):
    f = retired_history
    with pytest.raises(SnapshotError, match="abandoned history changed"):
        capture_catalog(f["cold"], f["policy"], f["disposition"])


@pytest.mark.parametrize("damage", ["incomplete", "plan_hash", "wrong_manifest", "wrong_object", "symlink"])
def test_retirement_proof_must_be_complete_and_exact(retired_history, damage):
    f = retired_history
    plan = deepcopy(f["plan"])
    if damage == "incomplete":
        journal = json.loads(f["journal"].read_text())
        journal["removed_count"] = 0
        f["journal"].write_text(json.dumps(journal))
    elif damage == "plan_hash":
        path = f["stage"] / ("retirement-plan-" + identity_sha256(plan) + ".json")
        path.write_text('{}')
    elif damage in {"wrong_manifest", "wrong_object"}:
        f["journal"].unlink()
        if damage == "wrong_manifest":
            plan["versions"][0]["sha256"] = "c" * 64
        else:
            next(ref for ref in plan["versions"][0]["refs"]
                 if ref["relative"] == f["missing"])["sha256"] = "c" * 64
        f["write_plan"](plan)
    else:
        path = f["journal"]
        raw = path.read_bytes()
        path.unlink()
        replacement = f["stage"] / "other.json"
        replacement.write_bytes(raw)
        path.symlink_to(replacement)
    with pytest.raises(SnapshotError):
        capture_catalog(f["cold"], f["policy"], f["disposition"],
                        history_retirement_root=f["root"])


def test_retirement_proof_never_waives_a_current_head(retired_history):
    f = retired_history
    catalog = deepcopy(f["before"])
    next(row for row in catalog["releases"]
         if row["snapshot_id"] == f["old"].manifest["snapshot_id"])["current_head"] = True
    with pytest.raises(SnapshotError, match="current head"):
        apply_disposition(catalog, f["disposition"], retirement_journal_root=f["root"])


def test_lifecycle_identity_changes_with_history_disposition_code(monkeypatch):
    from scripts import manage_lakehouse as worker
    source = "stockagent/data_sync/backup_history.py"
    changed = {}
    monkeypatch.setattr(worker, "digest", lambda path: changed.get(str(path.relative_to(worker.ROOT)), "a" * 64))
    first = worker.worker_code_identity()
    changed[source] = "b" * 64
    assert worker.worker_code_identity() != first
