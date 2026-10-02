from __future__ import annotations

import os
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import stockagent.data_sync.artifact_retirement as retirement
import stockagent.data_sync.cold_primary as cold_primary
from stockagent.data_sync.cold_artifacts import ColdArtifactSpec
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.live_artifacts import reconcile_artifacts
from stockagent.data_sync.materialized_cache import use_materialized_snapshot
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
)


def _fixture(tmp_path: Path, monkeypatch):
    spec = ColdArtifactSpec(
        dataset="artifact-complete-run",
        relative_root="markets/complete-run",
        maximum_file_bytes=None,
        loose_file_threshold_bytes=64,
        pack_buckets=2,
        min_stable_hours=0,
        completion_contract="training-lifecycle-v1",
    )
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / spec.relative_root
    source.mkdir(parents=True)
    (source / "checkpoint.pt").write_bytes(b"verified checkpoint payload")
    sync_root = tmp_path / "packed"
    initialize_packed_layout(sync_root, node_id="penguin")
    resolved = publish_packed_snapshot(
        sync_root,
        spec.dataset,
        source,
        loose_file_threshold_bytes=spec.loose_file_threshold_bytes,
        pack_buckets=spec.pack_buckets,
        metadata={
            "artifact_relative_root": spec.relative_root,
            "transport_role": "cold-full-run",
            "completion_contract": spec.completion_contract,
            "source_lifecycle_validated": "true",
        },
    )
    hot_root = tmp_path / "hot"
    hot_tree = hot_root / spec.relative_root
    hot_tree.mkdir(parents=True)
    os.link(source / "checkpoint.pt", hot_tree / "checkpoint.pt")
    backup = tmp_path / "backup"
    shutil.copytree(sync_root, backup)
    monkeypatch.setattr(
        cold_primary.BackupConfig,
        "load",
        lambda path: SimpleNamespace(source=sync_root, destination=backup),
    )
    monkeypatch.setattr(cold_primary.VolumeGuard, "check", lambda self: None)
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: [])
    monkeypatch.setattr(retirement, "process_references", lambda *args: [])
    options = {
        "artifact_root": artifact_root,
        "hot_root": hot_root,
        "sync_root": sync_root,
        "materialized_root": tmp_path / "materialized",
        "state_root": tmp_path / "state",
        "backup_config": tmp_path / "backup-config.json",
        "peer_proof": {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "peers": [{"name": "lab203", "ok": True}],
        },
        "required_peer_names": ("lab203",),
        "bridge_inactive": True,
    }
    return spec, resolved, source, hot_tree, options


def test_retirement_enrolls_before_seven_day_deletion(tmp_path: Path, monkeypatch) -> None:
    spec, _resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = 1_800_000_000_000_000_000
    options["now_ns"] = start
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert "seven-day-use-lease-not-enrolled" in plan["blockers"]

    result = retirement.apply_artifact_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )

    assert result["action"] == "enrolled"
    assert source.exists() and hot_tree.exists()
    later = retirement.plan_artifact_retirement(
        spec, **{**options, "now_ns": start + 6 * 86_400 * 1_000_000_000}
    )
    assert "seven-day-use-lease-active" in later["blockers"]


def test_full_run_retirement_protects_enabled_service(tmp_path, monkeypatch):
    spec, _resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    service = options["artifact_root"].parent / "services/discord_bot/markets/paper.yaml"
    service.parent.mkdir(parents=True)
    service.write_text(f"enabled: true\noutput_dir: artifacts/{spec.relative_root}\n")
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert "enabled-service-references-artifact" in plan["blockers"]
    assert plan["service_references"]
    assert not plan["apply_ready"]
    assert source.is_dir() and hot_tree.is_dir()


def test_retirement_accepts_verified_single_d_primary_without_backup_claim(
    tmp_path: Path, monkeypatch
) -> None:
    spec, _resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    cold_primary_marker = options["sync_root"] / cold_primary.D_PRIMARY_MARKER
    cold_primary_marker.write_text(json.dumps({
        "schema_version": 1,
        "volume_id": cold_primary.D_PRIMARY_VOLUME_ID,
        "backing": cold_primary.D_PRIMARY_BACKING,
        "authority_node_id": "penguin",
        "resilience": "single_d_volume",
    }))
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda root: None)
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert plan["cold_primary_verified"] is True
    assert plan["backup_verified"] is False
    assert plan["resilience"] == "single_d_volume"
    assert source.exists() and hot_tree.exists()


def test_retirement_removes_both_hot_names_and_rehydrates_on_use(
    tmp_path: Path, monkeypatch
) -> None:
    spec, resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = 1_800_000_000_000_000_000
    options["now_ns"] = start
    enrollment = retirement.plan_artifact_retirement(spec, **options)
    retirement.apply_artifact_retirement(
        spec, expected_fingerprint=enrollment["plan_fingerprint"], **options
    )
    options["now_ns"] = start + 8 * 86_400 * 1_000_000_000
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert plan["apply_ready"]

    result = retirement.apply_artifact_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )

    assert result["action"] == "retired"
    assert not source.exists() and not hot_tree.exists()
    assert (options["sync_root"] / "heads" / spec.dataset / "penguin.json").exists()
    assert (options["hot_root"] / ".stignore-cold-local").read_text().find(
        "(?d)/markets/complete-run"
    ) >= 0
    assert reconcile_artifacts(options["artifact_root"], options["hot_root"]).incoming_added == 0

    lease = use_materialized_snapshot(
        options["sync_root"],
        options["materialized_root"],
        spec.dataset,
        snapshot_id=resolved.manifest["snapshot_id"],
        links=[source],
    )
    assert source.is_symlink()
    assert (source / "checkpoint.pt").read_bytes() == b"verified checkpoint payload"
    assert lease["ttl_days"] == 7.0


def test_retirement_rejects_unverified_hot_or_missing_peer(
    tmp_path: Path, monkeypatch
) -> None:
    spec, _resolved, _source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    (hot_tree / "checkpoint.pt").unlink()
    (hot_tree / "checkpoint.pt").write_bytes(b"different payload")
    with pytest.raises(SnapshotError, match="hot mirror"):
        retirement.plan_artifact_retirement(spec, **options)

    (hot_tree / "checkpoint.pt").unlink()
    options["peer_proof"] = {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat(), "peers": []}
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert "intended-cold-peers-not-converged" in plan["blockers"]


def test_retirement_rejects_changed_plan_and_active_bridge(
    tmp_path: Path, monkeypatch
) -> None:
    spec, _resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retirement, "_running_hot_bridge", lambda root: True)
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert "hot-bridge-must-be-stopped-for-retirement" in plan["blockers"]
    with pytest.raises(SnapshotError, match="plan changed"):
        retirement.apply_artifact_retirement(
            spec, expected_fingerprint="0" * 64, **options
        )
    assert source.exists() and hot_tree.exists()


def test_interrupted_retirement_keeps_quarantine_and_cold_release(
    tmp_path: Path, monkeypatch
) -> None:
    spec, _resolved, source, _hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = 1_800_000_000_000_000_000
    options["now_ns"] = start
    enrollment = retirement.plan_artifact_retirement(spec, **options)
    retirement.apply_artifact_retirement(
        spec, expected_fingerprint=enrollment["plan_fingerprint"], **options
    )
    options["now_ns"] = start + 8 * 86_400 * 1_000_000_000
    plan = retirement.plan_artifact_retirement(spec, **options)
    original_verify = retirement.verify_packed_snapshot

    def reject_after_rename(*args, **kwargs):
        materialized = kwargs.get("materialized_path")
        if materialized is not None and "quarantine" in Path(materialized).parts:
            raise SnapshotError("post-rename verification failed")
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(retirement, "verify_packed_snapshot", reject_after_rename)
    with pytest.raises(SnapshotError, match="post-rename verification failed"):
        retirement.apply_artifact_retirement(
            spec, expected_fingerprint=plan["plan_fingerprint"], **options
        )

    assert not source.exists()
    quarantine = options["state_root"] / "retirements" / "quarantine" / spec.dataset
    assert any((item / "source" / "checkpoint.pt").exists() for item in quarantine.iterdir())
    assert (options["sync_root"] / "heads" / spec.dataset / "penguin.json").exists()
    assert reconcile_artifacts(options["artifact_root"], options["hot_root"]).incoming_added == 0


def test_partial_cold_release_cannot_retire_whole_run(tmp_path: Path) -> None:
    partial = ColdArtifactSpec(
        dataset="artifact-small-only",
        relative_root="markets/partial",
        maximum_file_bytes=63,
        loose_file_threshold_bytes=64,
        pack_buckets=2,
        min_stable_hours=0,
        completion_contract="training-lifecycle-v1",
    )
    with pytest.raises(SnapshotError, match="partial cold artifact releases"):
        retirement.plan_artifact_retirement(
            partial,
            artifact_root=tmp_path / "artifacts",
            hot_root=tmp_path / "hot",
            sync_root=tmp_path / "packed",
            materialized_root=tmp_path / "materialized",
            state_root=tmp_path / "state",
            backup_config=tmp_path / "backup.json",
            peer_proof={},
            required_peer_names=("lab203",),
            bridge_inactive=True,
        )


def test_retirement_peer_policy_is_separate_from_packed_retention(tmp_path: Path) -> None:
    policy = tmp_path / "retirement.json"
    policy.write_text(
        json.dumps(
            {"schema_version": 1, "authority_node_id": "penguin", "required_peer_names": []}
        ),
        encoding="utf-8",
    )
    assert retirement.load_retirement_peer_names(
        policy, authority_node_id="penguin"
    ) == ()
    with pytest.raises(SnapshotError, match="authority/schema"):
        retirement.load_retirement_peer_names(policy, authority_node_id="vastai1T")


def test_penguin_hot_retirement_does_not_require_lab203(tmp_path: Path, monkeypatch) -> None:
    spec, _resolved, _source, _hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["required_peer_names"] = ()
    options["peer_proof"] = {
        "ok": True,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "peers": [],
    }
    plan = retirement.plan_artifact_retirement(spec, **options)

    assert plan["required_peer_names"] == []
    assert "intended-cold-peers-not-converged" not in plan["blockers"]
    assert plan["blockers"] == ["seven-day-use-lease-not-enrolled"]


@pytest.mark.parametrize("enrolled", [False, True])
def test_manual_immediate_only_bypasses_age_and_records_real_time(tmp_path, monkeypatch, enrolled):
    spec, resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = time.time_ns()
    options["now_ns"] = start
    if enrolled:
        initial = retirement.plan_artifact_retirement(spec, **options)
        retirement.apply_artifact_retirement(spec, expected_fingerprint=initial["plan_fingerprint"], **options)
    ordinary = retirement.plan_artifact_retirement(spec, **options)
    assert not ordinary["apply_ready"]
    options["manual_immediate"] = True
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert plan["apply_ready"]
    assert plan["plan_fingerprint"] != ordinary["plan_fingerprint"]
    with pytest.raises(SnapshotError, match="plan changed"):
        retirement.apply_artifact_retirement(spec, expected_fingerprint=ordinary["plan_fingerprint"], **options)
    allocated = (source / "checkpoint.pt").stat().st_blocks * 512
    result = retirement.apply_artifact_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert not source.exists() and not hot_tree.exists()
    assert result["reclaimed_allocated_file_bytes"] == allocated
    state = json.loads(retirement._state_path(options["state_root"], spec.dataset).read_text())
    assert state["last_used_ns"] == start and state["manual_immediate"] is True
    lease = use_materialized_snapshot(options["sync_root"], options["materialized_root"],
                                      spec.dataset, snapshot_id=resolved.manifest["snapshot_id"], links=[source])
    assert (source / "checkpoint.pt").read_bytes() == b"verified checkpoint payload"
    assert lease["ttl_days"] == 7.0


@pytest.mark.parametrize("gate", ["process", "service", "pin", "bridge", "state", "quarantine", "peer"])
def test_manual_immediate_never_bypasses_other_gates(tmp_path, monkeypatch, gate):
    spec, resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    if gate == "process":
        monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: ["pid=123:fd"])
    elif gate == "service":
        monkeypatch.setattr(retirement, "artifact_service_references", lambda *args: {str(source): ["service"]})
    elif gate == "pin":
        monkeypatch.setattr(retirement, "_pinned_snapshot_ids", lambda *args: {resolved.manifest["snapshot_id"]})
    elif gate == "bridge":
        options["bridge_inactive"] = False
    elif gate == "state":
        path = retirement._state_path(options["state_root"], spec.dataset)
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"schema_version": 1, "dataset": spec.dataset,
                                   "artifact_relative_root": spec.relative_root,
                                   "state": "cold-only", "last_used_ns": time.time_ns()}))
    elif gate == "quarantine":
        path = options["state_root"] / "retirements/quarantine" / spec.dataset
        path.mkdir(parents=True)
        (path / "unknown-evidence").write_bytes(b"keep")
    else:
        options["peer_proof"]["ok"] = False
    plan = retirement.plan_artifact_retirement(spec, **options)
    assert not plan["apply_ready"]
    with pytest.raises(SnapshotError, match="blocked"):
        retirement.apply_artifact_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_full_retirement_refreshes_peer_after_expensive_checks(tmp_path, monkeypatch):
    spec, _, _, _, options = _fixture(tmp_path, monkeypatch)
    options["peer_proof"] = {"ok": False, "checked_at": "2020-01-01T00:00:00+00:00"}
    options["peer_probe"] = lambda: {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat(),
                                     "peers": [{"name": "lab203", "ok": True}]}
    assert retirement.plan_artifact_retirement(spec, **options)["blockers"] == ["seven-day-use-lease-not-enrolled"]


def test_reclaimed_bytes_exclude_external_hardlinks(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    payload = first / "payload"
    payload.write_bytes(b"allocated payload")
    os.link(payload, second / "payload")
    os.link(payload, tmp_path / "external")
    assert retirement._reclaimable_file_bytes(first, second) == 0
    (tmp_path / "external").unlink()
    assert retirement._reclaimable_file_bytes(first, second) == payload.stat().st_blocks * 512


def test_manual_immediate_missing_cold_payload_never_changes_hot_paths(tmp_path, monkeypatch):
    spec, resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    payload = options["sync_root"] / resolved.manifest["archive"]["objects"][0]["relpath"]
    payload.unlink()
    with pytest.raises(SnapshotError):
        retirement.plan_artifact_retirement(spec, **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_manual_new_pin_after_rename_keeps_exact_bytes_in_quarantine(tmp_path, monkeypatch):
    spec, resolved, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    plan = retirement.plan_artifact_retirement(spec, **options)
    monkeypatch.setattr(retirement, "_pinned_snapshot_ids", lambda *args:
                        {resolved.manifest["snapshot_id"]} if not source.exists() else set())
    with pytest.raises(SnapshotError, match="post-rename release pinned"):
        retirement.apply_artifact_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    state = json.loads(retirement._state_path(options["state_root"], spec.dataset).read_text())
    assert state["state"] == "retiring"
    assert (Path(state["quarantine"]) / "source/checkpoint.pt").read_bytes() == b"verified checkpoint payload"
    assert (Path(state["quarantine"]) / "hot/checkpoint.pt").exists()
