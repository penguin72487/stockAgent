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
from stockagent.data_sync.legacy_artifact_archive import LegacyArchiveSpec, publish_archive
from stockagent.data_sync.packed_snapshots import initialize_packed_layout


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
