from __future__ import annotations

import json
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import stockagent.data_sync.legacy_artifact_retirement as retirement
import stockagent.data_sync.cold_primary as cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, publish_archive, restore_archive
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, resolve_latest_packed


def _fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    repo_root = tmp_path / "repo"
    artifact_root = repo_root / "artifacts"
    source = artifact_root / "markets/crypto"
    source.mkdir(parents=True)
    original = source / "checkpoint.pt"
    original.write_bytes(b"old unfinished run; preserve bytes, not deployment permission")
    old = time.time() - 10 * 86_400
    os.utime(original, (old, old))
    hot_root = tmp_path / "hot"
    hot_tree = hot_root / "markets/crypto"
    hot_tree.mkdir(parents=True)
    os.link(original, hot_tree / original.name)
    spec = LegacyArchiveSpec("legacy-artifact-markets-crypto", "markets/crypto", 7, tmp_path / "stage")
    sync_root = tmp_path / "packed"
    initialize_packed_layout(sync_root, node_id="penguin")
    publish_archive(spec, artifact_root, sync_root, repo_root=repo_root)
    backup = tmp_path / "backup"
    shutil.copytree(sync_root, backup)
    monkeypatch.setattr(
        cold_primary.BackupConfig,
        "load",
        lambda _path: SimpleNamespace(source=sync_root, destination=backup),
    )
    monkeypatch.setattr(cold_primary.VolumeGuard, "check", lambda self: None)
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: [])
    monkeypatch.setattr(retirement, "process_references", lambda *args: [])
    options = {
        "repo_root": repo_root,
        "artifact_root": artifact_root,
        "hot_root": hot_root,
        "sync_root": sync_root,
        "materialized_root": tmp_path / "materialized",
        "state_root": tmp_path / "state",
        "activation_root": tmp_path / "activations",
        "backup_config": tmp_path / "backup-config.json",
        "peer_proof": {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "peers": [{"name": "vastai1T", "ok": True}],
        },
        "bridge_inactive": True,
    }
    return spec, source, hot_tree, options


def test_observation_enrollment_without_cold_release_deletes_nothing(tmp_path: Path, monkeypatch):
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "markets/us/checkpoint.pt"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"unique legacy bytes")
    old = time.time() - 10 * 86_400
    os.utime(source, (old, old))
    spec = LegacyArchiveSpec("legacy-artifact-markets-us", "markets/us", 7, tmp_path / "stage")
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: [])
    before = time.time_ns()
    result = retirement.enroll_legacy_lease(spec, artifact_root=artifact_root,
                                          state_root=tmp_path / "state")
    assert result["action"] == "enrolled"
    assert result["deleted"] is False and result["cold_verified"] is False
    assert before <= result["last_used_ns"] <= time.time_ns()
    assert source.read_bytes() == b"unique legacy bytes"
    repeated = retirement.enroll_legacy_lease(spec, artifact_root=artifact_root,
                                            state_root=tmp_path / "state")
    assert repeated["action"] == "already-enrolled"
    assert repeated["last_used_ns"] == result["last_used_ns"]


def test_observation_enrollment_refuses_in_use_source(tmp_path: Path, monkeypatch):
    spec, source, _, options = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: ["pid=123:fd"])
    with pytest.raises(SnapshotError, match="in use"):
        retirement.enroll_legacy_lease(spec, artifact_root=options["artifact_root"],
                                      state_root=options["state_root"])
    assert not (options["state_root"] / "retirements" / f"{spec.dataset}.json").exists()


def test_legacy_retirement_enrolls_then_removes_both_names_after_seven_days(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = time.time_ns()
    options["now_ns"] = start
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["blockers"] == ["seven-day-use-lease-not-enrolled"]
    enrolled = retirement.apply_legacy_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )
    assert enrolled["action"] == "enrolled"
    assert source.is_dir() and hot_tree.is_dir()
    renewed = retirement.renew_legacy_lease(
        spec,
        artifact_root=options["artifact_root"],
        state_root=options["state_root"],
        now_ns=start + 2 * 86_400_000_000_000,
    )
    assert renewed["lease_expires_at"]
    options["now_ns"] = start + 8 * 86_400_000_000_000
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "seven-day-use-lease-active" in plan["blockers"]
    options["now_ns"] = start + 10 * 86_400_000_000_000
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["apply_ready"]
    retired = retirement.apply_legacy_retirement(
        spec, expected_fingerprint=plan["plan_fingerprint"], **options
    )
    assert retired["action"] == "retired"
    assert not source.exists() and not hot_tree.exists()
    assert (options["sync_root"] / "heads" / spec.dataset / "penguin.json").is_file()
    assert (options["hot_root"] / ".stignore-cold-local").is_file()
    state = json.loads((options["state_root"] / "retirements" / f"{spec.dataset}.json").read_text())
    assert state["state"] == "cold-only"


def test_legacy_retirement_accepts_verified_single_d_primary_without_backup_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    (options["sync_root"] / cold_primary.D_PRIMARY_MARKER).write_text(json.dumps({
        "schema_version": 1,
        "volume_id": cold_primary.D_PRIMARY_VOLUME_ID,
        "backing": cold_primary.D_PRIMARY_BACKING,
        "authority_node_id": "penguin",
        "resilience": "single_d_volume",
    }))
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda root: None)
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["cold_primary_verified"] is True
    assert plan["backup_verified"] is False
    assert plan["resilience"] == "single_d_volume"
    assert source.exists() and hot_tree.exists()


def test_legacy_retirement_blocks_enabled_service_and_changed_mirror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    service = options["repo_root"] / "services/discord_bot/markets/crypto.yaml"
    service.parent.mkdir(parents=True)
    service.write_text("enabled: true\noutput_dir: artifacts/markets/crypto\n")
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "enabled-service-references-artifact" in plan["blockers"]
    service.write_text("enabled: false\noutput_dir: artifacts/markets/crypto\n")
    runtime_state = options["repo_root"] / "artifacts/discord_bot/state.json"
    runtime_state.parent.mkdir(parents=True)
    runtime_state.write_text(json.dumps({"markets": {"crypto": {"enabled": True}}}))
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "enabled-service-references-artifact" in plan["blockers"]
    runtime_state.write_text(json.dumps({"markets": {"crypto": {"enabled": False}}}))
    (hot_tree / "checkpoint.pt").unlink()
    (hot_tree / "checkpoint.pt").write_bytes(b"unverified copy")
    with pytest.raises(SnapshotError, match="legacy source differs|hot mirror"):
        retirement.plan_legacy_retirement(spec, **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_retirement_refreshes_peer_proof_after_full_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec, _source, _hot_tree, options = _fixture(tmp_path, monkeypatch)
    stale = {"ok": True, "checked_at": "2020-01-01T00:00:00+00:00"}
    options["peer_proof"] = stale
    refreshed = {"ok": True, "checked_at": datetime.now(timezone.utc).isoformat()}
    options["peer_probe"] = lambda: refreshed
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["blockers"] == ["seven-day-use-lease-not-enrolled"]


def test_legacy_retirement_keeps_pinned_release(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    plan = retirement.plan_legacy_retirement(spec, **options)
    root = options["materialized_root"]
    root.mkdir()
    (root / "legacy.pin.json").write_text(json.dumps({
        "manifest": {"snapshot_id": plan["snapshot_id"]}
    }))
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "artifact-release-is-pinned" in plan["blockers"]
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_retirement_rejects_naive_peer_timestamp(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["peer_proof"]["checked_at"] = datetime.now().isoformat()
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert "peer-proof-missing-timestamp" in plan["blockers"]
    assert not plan["apply_ready"]
    assert source.is_dir() and hot_tree.is_dir()


@pytest.mark.parametrize("enrolled", [False, True])
def test_legacy_manual_immediate_preserves_restore_and_automatic_policy(tmp_path, monkeypatch, enrolled):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    start = time.time_ns()
    options["now_ns"] = start
    if enrolled:
        retirement.enroll_legacy_lease(spec, artifact_root=options["artifact_root"], state_root=options["state_root"])
    ordinary = retirement.plan_legacy_retirement(spec, **options)
    assert not ordinary["apply_ready"]
    options["manual_immediate"] = True
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert plan["apply_ready"]
    with pytest.raises(SnapshotError, match="plan changed"):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=ordinary["plan_fingerprint"], **options)
    allocated = (source / "checkpoint.pt").stat().st_blocks * 512
    result = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert result["deleted"] and result["reclaimed_allocated_file_bytes"] == allocated
    assert not source.exists() and not hot_tree.exists()
    state = json.loads(retirement._state_path(options["state_root"], spec.dataset).read_text())
    assert state["manual_immediate"] is True
    if not enrolled:
        assert state["last_used_ns"] == start
    restore = tmp_path / "restore"
    restore_archive(spec, options["sync_root"], restore, materialized_root=tmp_path / "restore-cache")
    assert (restore / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"


@pytest.mark.parametrize("gate", ["process", "service", "pin", "bridge", "state", "quarantine", "peer"])
def test_legacy_manual_immediate_never_bypasses_other_gates(tmp_path, monkeypatch, gate):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    if gate == "process":
        monkeypatch.setattr(retirement, "artifact_process_references", lambda *args: ["pid=123:fd"])
    elif gate == "service":
        monkeypatch.setattr(retirement, "_active_service_references", lambda *args: ["service"])
    elif gate == "pin":
        first = retirement.plan_legacy_retirement(spec, **options)
        monkeypatch.setattr(retirement, "_pinned_snapshot_ids", lambda *args: {first["snapshot_id"]})
    elif gate == "bridge":
        options["bridge_inactive"] = False
    elif gate == "state":
        path = retirement._state_path(options["state_root"], spec.dataset)
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"schema_version": 1, "dataset": spec.dataset,
                                   "relative_root": spec.relative_root,
                                   "state": "cold-only", "last_used_ns": time.time_ns()}))
    elif gate == "quarantine":
        path = options["state_root"] / "retirements/quarantine" / spec.dataset
        path.mkdir(parents=True)
        (path / "unknown-evidence").write_bytes(b"keep")
    else:
        options["peer_proof"]["ok"] = False
    plan = retirement.plan_legacy_retirement(spec, **options)
    assert not plan["apply_ready"]
    with pytest.raises(SnapshotError, match="blocked"):
        retirement.apply_legacy_retirement(spec, expected_fingerprint=plan["plan_fingerprint"], **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_legacy_manual_immediate_missing_cold_keeps_hot_sources(tmp_path, monkeypatch):
    spec, source, hot_tree, options = _fixture(tmp_path, monkeypatch)
    options["manual_immediate"] = True
    resolved = resolve_latest_packed(options["sync_root"], spec.dataset)
    (options["sync_root"] / resolved.manifest["archive"]["objects"][0]["relpath"]).unlink()
    with pytest.raises(SnapshotError):
        retirement.plan_legacy_retirement(spec, **options)
    assert source.is_dir() and hot_tree.is_dir()


def test_sequential_legacy_retirement_handles_shared_inodes_without_fake_reclaimed_bytes(tmp_path, monkeypatch):
    first, source, hot, options = _fixture(tmp_path, monkeypatch)
    second = LegacyArchiveSpec("legacy-artifact-markets-us", "markets/us", 7, tmp_path / "stage")
    second_source = options["artifact_root"] / second.relative_root
    second_hot = options["hot_root"] / second.relative_root
    second_source.mkdir(parents=True)
    second_hot.mkdir(parents=True)
    os.link(source / "checkpoint.pt", second_source / "checkpoint.pt")
    os.link(source / "checkpoint.pt", second_hot / "checkpoint.pt")
    publish_archive(second, options["artifact_root"], options["sync_root"], repo_root=options["repo_root"])
    shutil.copytree(options["sync_root"], tmp_path / "backup", dirs_exist_ok=True)
    allocated = (source / "checkpoint.pt").stat().st_blocks * 512
    options["manual_immediate"] = True
    first_plan = retirement.plan_legacy_retirement(first, **options)
    first_result = retirement.apply_legacy_retirement(first, expected_fingerprint=first_plan["plan_fingerprint"], **options)
    assert first_result["deleted"] and first_result["reclaimed_allocated_file_bytes"] == 0
    assert second_source.is_dir() and second_hot.is_dir()
    second_plan = retirement.plan_legacy_retirement(second, **options)
    assert second_plan["source_metadata_drift"]
    second_result = retirement.apply_legacy_retirement(second, expected_fingerprint=second_plan["plan_fingerprint"], **options)
    assert second_result["deleted"] and second_result["reclaimed_allocated_file_bytes"] == allocated
    for spec in (first, second):
        restored = tmp_path / "restore" / spec.dataset
        restore_archive(spec, options["sync_root"], restored, materialized_root=tmp_path / "restore-cache")
        assert (restored / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"
