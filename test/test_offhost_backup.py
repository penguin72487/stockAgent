"""Exercise encryption, pinned-version recovery and fail-closed backup acceptance."""
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from stockagent.data_sync import offhost_backup as module
from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
from stockagent.data_sync.offhost_backup import (
    ResticBackup, capture_plan, export_delivery, private_json, unchanged_inputs, validate_plan, verify_restore,
)
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot, initialize_packed_layout, publish_packed_snapshot,
    resolve_packed_snapshot_id,
)
from stockagent.runtime_identity import identity_sha256
from scripts.verify_backup_delivery import verify as verify_delivery


@pytest.fixture
def release(tmp_path):
    cold, work = tmp_path / "cold", tmp_path / "work"
    initialize_packed_layout(cold, node_id="penguin")
    work.mkdir()
    (work / "來源.txt").write_text("audited source version one\n", encoding="utf-8")
    (work / "large.bin").write_bytes(os.urandom(64 * 1024))
    published = publish_packed_snapshot(cold, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    return cold, work, published


@pytest.fixture
def restic(tmp_path):
    binary = os.environ.get("STOCKAGENT_TEST_RESTIC_BINARY") or shutil.which("restic")
    if not binary:
        pytest.skip("real Restic integration needs the isolated backup role")
    key = tmp_path / "password"
    key.write_bytes(os.urandom(32).hex().encode())
    key.chmod(0o600)
    client = ResticBackup(Path(binary), str(tmp_path / "repository"), key, cache=tmp_path / "cache")
    client.initialize()
    return client


def resign(plan):
    body = {k: v for k, v in plan.items() if k not in {"identity_sha256", "observed_at_utc"}}
    plan["identity_sha256"] = identity_sha256(body)
    return plan


def test_real_encrypted_backup_restores_selected_version_after_head_advances(release, restic, tmp_path):
    cold, work, published = release
    before = scan_tree(work)["portable_fingerprint_sha256"]
    plan = capture_plan(cold)
    recorded = tmp_path / "plan.json"
    private_json(recorded, plan)
    result = restic.backup(plan, recorded)
    assert result["durable_off_host_backup_verified"] is False
    assert len(result["snapshot_id"]) == 64
    # The backup captures release metadata; a new head must not change recovery.
    (work / "來源.txt").write_text("version two\n", encoding="utf-8")
    newer = publish_packed_snapshot(cold, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    old_id, new_id = published.manifest["snapshot_id"], newer.manifest["snapshot_id"]
    assert new_id != old_id
    restored = tmp_path / "restored"
    restic.restore(result["snapshot_id"], restored)
    verified = verify_restore(plan, restored, record_plan_path=recorded)
    assert verified["state"] == "restore_verified"
    assert verified["durable_off_host_backup_verified"] is False
    assert verified["control_database_restore_verified"] is False
    cold_copy = restored / cold.as_posix().lstrip("/")
    selected = resolve_packed_snapshot_id(cold_copy, "prices", old_id)
    recovered = fetch_packed_snapshot(cold_copy, tmp_path / "materialized", selected)
    assert scan_tree(recovered)["portable_fingerprint_sha256"] == before
    assert json.loads((cold / "heads/prices/penguin.json").read_bytes())["snapshot_id"] == new_id
    restic.run(["check", "--read-data"])
    # A different password cannot read even the repository's snapshot index.
    wrong_key = tmp_path / "wrong-password"
    wrong_key.write_text("wrong password")
    wrong_key.chmod(0o600)
    wrong = ResticBackup(restic.executable, restic.repository, wrong_key, cache=tmp_path / "wrong-cache")
    with pytest.raises(SnapshotError, match="acceptance withheld"):
        wrong.run(["snapshots", "--json"])


def test_real_restore_rejects_changed_bytes_and_existing_destination(release, restic, tmp_path):
    cold, _, _ = release
    plan = capture_plan(cold)
    recorded = tmp_path / "plan.json"
    private_json(recorded, plan)
    backed_up = restic.backup(plan, recorded)
    restored = tmp_path / "restored"
    restic.restore(backed_up["snapshot_id"], restored)
    target = restored / plan["files"][0]["path"].lstrip("/")
    target.write_bytes(b"x" * target.stat().st_size)
    with pytest.raises(SnapshotError, match="content differs"):
        verify_restore(plan, restored, record_plan_path=recorded)
    with pytest.raises(SnapshotError, match="fresh independent"):
        restic.restore(backed_up["snapshot_id"], restored)
    with pytest.raises(SnapshotError, match="exact full"):
        restic.restore("latest", tmp_path / "latest")


def test_changed_source_cannot_receive_success(release):
    cold, _, _ = release
    plan = capture_plan(cold)
    target = Path(plan["files"][0]["path"])
    target.write_bytes(b"x" * target.stat().st_size)
    with pytest.raises(SnapshotError, match="changed after capture"):
        unchanged_inputs(plan)


@pytest.mark.parametrize("damage", ["missing_object", "outside_source", "changed_manifest", "changed_head", "unexpected_role"])
def test_even_resigned_plan_must_match_canonical_release(release, damage):
    cold, _, _ = release
    plan = deepcopy(capture_plan(cold))
    if damage == "missing_object":
        plan["files"].pop()
    elif damage == "outside_source":
        plan["files"][0]["path"] = "/outside/" + plan["files"][0]["relative"]
    elif damage == "changed_manifest":
        plan["releases"][0]["manifest_json"] += " "
    elif damage == "changed_head":
        key = next(iter(plan["captured_heads"]))
        head = json.loads(plan["captured_heads"][key])
        head["manifest_sha256"] = "0" * 64
        plan["captured_heads"][key] = json.dumps(head)
    else:
        plan["files"][0]["role"] = "password"
    with pytest.raises(SnapshotError):
        validate_plan(resign(plan))


def test_plan_fingerprint_tampering_is_rejected(release):
    plan = capture_plan(release[0])
    plan["files"][0]["sha256"] = "0" * 64
    with pytest.raises(SnapshotError, match="fingerprint"):
        validate_plan(plan)


def test_partial_restic_snapshot_never_becomes_accepted(monkeypatch, tmp_path):
    key = tmp_path / "key"
    key.write_text("private key")
    key.chmod(0o600)
    client = ResticBackup(Path(shutil.which("true")), str(tmp_path / "repo"), key, cache=tmp_path / "cache")
    monkeypatch.setattr(module.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 3, "snapshot exists", ""))
    with pytest.raises(SnapshotError, match="exit 3"):
        client.run(["backup"])


def test_secret_permissions_and_ambient_repository_are_not_trusted(monkeypatch, tmp_path):
    key = tmp_path / "key"
    key.write_text("private key")
    key.chmod(0o644)
    with pytest.raises(SnapshotError, match="owner-only"):
        ResticBackup(Path(shutil.which("true")), str(tmp_path / "repo"), key, cache=tmp_path / "cache")
    key.chmod(0o600)
    monkeypatch.setenv("RESTIC_PASSWORD", "must not be inherited")
    monkeypatch.setenv("RESTIC_REPOSITORY", "wrong target")
    observed = {}
    def execute(*args, **kwargs):
        observed.update(kwargs["env"])
        return subprocess.CompletedProcess(args, 0, "", "")
    monkeypatch.setattr(module.subprocess, "run", execute)
    client = ResticBackup(Path(shutil.which("true")), str(tmp_path / "repo"), key, cache=tmp_path / "cache")
    client.run(["snapshots"])
    assert "RESTIC_PASSWORD" not in observed
    assert observed["RESTIC_REPOSITORY"] == str(tmp_path / "repo")


def test_restore_must_be_independent_and_symlink_free(release, tmp_path):
    plan = capture_plan(release[0])
    recorded = tmp_path / "plan.json"
    private_json(recorded, plan)
    with pytest.raises(SnapshotError, match="independent"):
        verify_restore(plan, release[0], record_plan_path=recorded)
    alias = tmp_path / "alias"
    alias.symlink_to(recorded)
    with pytest.raises(SnapshotError, match="symlink"):
        module.private_file(alias)


def test_closed_delivery_preserves_source_and_rejects_corruption(release, tmp_path):
    plan = capture_plan(release[0])
    destination = tmp_path / "delivery"
    result = export_delivery(plan, destination)
    assert result["state"] == "closed_backup_delivery_verified"
    assert result["canonical_releases_verified"] == 1
    assert result["durable_off_host_backup_verified"] is False
    unchanged_inputs(plan)
    row = plan["files"][0]
    copied = destination / "cold" / row["relative"]
    assert copied.stat().st_ino != Path(row["path"]).stat().st_ino
    copied.write_bytes(b"x" * copied.stat().st_size)
    with pytest.raises(ValueError, match="content differs"):
        verify_delivery(destination, result["envelope_identity_sha256"])
    unchanged_inputs(plan)


def test_delivery_rejects_unknown_files_and_source_overlap(release, tmp_path):
    plan = capture_plan(release[0])
    with pytest.raises(SnapshotError, match="independent"):
        export_delivery(plan, release[0] / "nested")
    destination = tmp_path / "delivery"
    result = export_delivery(plan, destination)
    (destination / "extra-password.txt").write_text("unexpected")
    with pytest.raises(ValueError, match="file set differs"):
        verify_delivery(destination, result["envelope_identity_sha256"])


def test_failed_canonical_check_never_publishes_ready(release, tmp_path, monkeypatch):
    plan = capture_plan(release[0])
    destination = tmp_path / "failed-delivery"
    def reject(*args, **kwargs):
        assert not (destination / "READY").exists()
        raise SnapshotError("canonical validation rejected")
    monkeypatch.setattr(module, "verify_packed_snapshot", reject)
    with pytest.raises(SnapshotError, match="canonical validation rejected"):
        export_delivery(plan, destination)
    assert not (destination / "READY").exists()
    unchanged_inputs(plan)


def test_all_history_retains_old_releases_and_refuses_missing_object(release):
    cold, work, old = release
    (work / "來源.txt").write_text("new source version")
    new = publish_packed_snapshot(cold, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    current = capture_plan(cold)
    history = capture_plan(cold, all_history=True)
    assert len(current["releases"]) == 1
    assert len(history["releases"]) == 2
    assert history["selection"] == "all_observed_retained_manifests"
    assert history["all_history_verified"] is False
    old_only = next(v for v in old.manifest["archive"]["objects"] if v not in new.manifest["archive"]["objects"])
    (cold / old_only["relpath"]).unlink()
    capture_plan(cold)
    with pytest.raises((OSError, SnapshotError)):
        capture_plan(cold, all_history=True)
