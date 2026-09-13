import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_backup as module
from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
from stockagent.data_sync.packed_backup import BackupConfig, PackedBackup, VolumeGuard, inventory, safe_path
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout, publish_packed_snapshot, resolve_latest_packed,
    fetch_packed_snapshot, verify_packed_snapshot,
)


class TestGuard:
    __test__ = False

    def check(self, **kwargs):
        pass


@pytest.fixture
def setup(tmp_path):
    source = tmp_path / "cold"
    initialize_packed_layout(source, node_id="penguin")
    work = tmp_path / "work"
    work.mkdir()
    (work / "small.txt").write_text("audited immutable data\n")
    (work / "large.bin").write_bytes(b"1234567890" * 1024)
    (work / "alias").symlink_to("small.txt")
    release = publish_packed_snapshot(source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    cfg = BackupConfig(source=source, destination=tmp_path / "d/backup/packed", mount_point=tmp_path / "d",
                       mount_source="D:\\", volume_id="test-volume", state_dir=tmp_path / "state", reserve_bytes=0)
    cfg.mount_point.mkdir()
    backup = PackedBackup(cfg, guard=TestGuard())
    yield cfg, work, release, backup
    backup.close()


def test_incremental_backup_and_existing_reader_restore(setup, tmp_path):
    cfg, work, release, backup = setup
    result = backup.run_once()
    assert result["state"] == "up_to_date"
    assert result["verified_releases"] == 1
    assert result["remaining_bytes"] == 0
    selected = resolve_latest_packed(cfg.destination, "prices")
    assert selected.manifest_sha256 == release.manifest_sha256
    verify_packed_snapshot(cfg.destination, selected)
    restored = fetch_packed_snapshot(cfg.destination, tmp_path / "restore", selected)
    assert scan_tree(restored)["portable_fingerprint_sha256"] == scan_tree(work)["portable_fingerprint_sha256"]
    assert backup.run_once()["copied_objects"] == 0
    assert not (cfg.destination / ".local-state/node-id").exists()
    assert not (cfg.destination / "current").exists()
    assert (cfg.destination.parent / "last-complete.json").exists()


def test_deletion_does_not_propagate_and_missing_current_is_degraded(setup):
    cfg, work, release, backup = setup
    backup.run_once()
    ref = release.manifest["archive"]["objects"][0]
    prior = (cfg.destination / ref["relpath"]).read_bytes()
    (cfg.source / ref["relpath"]).unlink()
    result = backup.run_once()
    assert result["state"] == "degraded"
    assert (cfg.destination / ref["relpath"]).read_bytes() == prior
    assert resolve_latest_packed(cfg.destination, "prices").manifest_sha256 == release.manifest_sha256


def test_new_head_waits_for_objects_and_keeps_old_head_history(setup):
    cfg, work, old, backup = setup
    backup.run_once()
    old_head = (cfg.destination / "heads/prices/penguin.json").read_bytes()
    (work / "small.txt").write_text("new release\n")
    new = publish_packed_snapshot(cfg.source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    pending = backup.run_once(max_objects=0)
    assert pending["pending_heads"] == 1
    assert (cfg.destination / "heads/prices/penguin.json").read_bytes() == old_head
    assert backup.run_once()["state"] == "up_to_date"
    assert resolve_latest_packed(cfg.destination, "prices").manifest_sha256 == new.manifest_sha256
    assert [p.read_bytes() for p in (cfg.destination / "head-history").rglob("*.json")] == [old_head]
    assert old.manifest_path.name in [p.name for p in (cfg.destination / "manifests/prices").iterdir()]


def test_resumes_verified_partial_and_retains_bad_partial(setup):
    cfg, work, release, backup = setup
    item = next(item for item in inventory(cfg)[0] if "/blobs/" in item["relative"])
    partial = cfg.destination / (item["relative"] + ".partial")
    partial.parent.mkdir(parents=True)
    source = (cfg.source / item["relative"]).read_bytes()
    partial.write_bytes(source[:31])
    assert backup.copy_object(item)
    assert not partial.exists()
    assert (cfg.destination / item["relative"]).read_bytes() == source
    other = next(item for item in inventory(cfg)[0] if "/packs/" in item["relative"])
    bad = cfg.destination / (other["relative"] + ".partial")
    bad.parent.mkdir(parents=True)
    bad.write_bytes(b"BAD")
    with pytest.raises(SnapshotError, match="partial prefix mismatch"):
        backup.copy_object(other)
    assert bad.read_bytes() == b"BAD"


def test_corrupt_backup_is_never_overwritten_or_promoted(setup):
    cfg, work, release, backup = setup
    item = inventory(cfg)[0][0]
    target = cfg.destination / item["relative"]
    target.parent.mkdir(parents=True)
    target.write_bytes(b"corrupted")
    result = backup.run_once()
    assert result["state"] == "degraded"
    assert target.read_bytes() == b"corrupted"
    assert not (cfg.destination / "heads/prices/penguin.json").exists()


def test_corrupt_source_is_not_committed(setup):
    cfg, work, release, backup = setup
    item = inventory(cfg)[0][0]
    source = cfg.source / item["relative"]
    source.write_bytes(b"x" * item["bytes"])
    result = backup.run_once()
    assert result["state"] == "degraded"
    assert not (cfg.destination / item["relative"]).exists()
    assert (cfg.destination / (item["relative"] + ".partial")).exists()


def test_space_guard(setup, monkeypatch):
    cfg, work, release, backup = setup
    monkeypatch.setattr(module.shutil, "disk_usage", lambda _path: SimpleNamespace(free=0))
    with pytest.raises(SnapshotError, match="insufficient backup free space"):
        backup.copy_object(inventory(cfg)[0][0])
    assert not list(cfg.destination.rglob("*.partial"))


def test_readback_failure_cannot_promote_object(setup, monkeypatch):
    cfg, work, release, backup = setup
    monkeypatch.setattr(backup, "hash_file", lambda *args: "0" * 64)
    item = inventory(cfg)[0][0]
    with pytest.raises(SnapshotError, match="readback checksum failed"):
        backup.copy_object(item)
    assert not (cfg.destination / item["relative"]).exists()


def test_source_mutation_during_copy_detected(setup, monkeypatch):
    cfg, work, release, backup = setup
    item = inventory(cfg)[0][0]
    mutated = False

    def mutate(*args):
        nonlocal mutated
        if not mutated:
            path = cfg.source / item["relative"]
            path.write_bytes(b"x" * item["bytes"])
            mutated = True

    monkeypatch.setattr(backup, "checkpoint", mutate)
    with pytest.raises(SnapshotError, match="source changed or failed checksum"):
        backup.copy_object(item)


def test_metadata_path_injection_is_rejected(setup):
    cfg, work, release, backup = setup
    head = cfg.source / "heads/prices/penguin.json"
    raw = json.loads(head.read_text())
    raw["manifest_relpath"] = "../../outside.json"
    head.write_text(json.dumps(raw))
    assert backup.run_once()["state"] == "degraded"
    assert not (cfg.destination / "heads/prices/penguin.json").exists()


def test_historical_release_missing_objects_blocks_full_completion(setup):
    cfg, work, old, backup = setup
    backup.run_once()
    ref = next(ref for ref in old.manifest["archive"]["objects"] if ref["kind"] == "blob")
    (work / "large.bin").write_bytes(b"newblob" * 2048)
    publish_packed_snapshot(cfg.source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    (cfg.source / ref["relpath"]).unlink()
    result = backup.run_once()
    assert result["state"] == "degraded"
    assert result["pending_releases"] == 1


def test_safe_paths_reject_escape_and_symlinks(tmp_path):
    (tmp_path / "redirect").symlink_to(tmp_path / "outside")
    for relative in ("../escape", "/escape", "redirect/file", ""):
        with pytest.raises(SnapshotError):
            safe_path(tmp_path, relative)


def test_volume_guard_missing_wrong_or_source_disk(setup, monkeypatch):
    cfg, *_ = setup
    guard = VolumeGuard(cfg)
    with pytest.raises(SnapshotError, match="not mounted"):
        guard.check(require_marker=False)
    monkeypatch.setattr(module, "mounted_volume", lambda _: ("1", "9p", "E:\\"))
    with pytest.raises(SnapshotError, match="unexpected backup mount"):
        guard.check(require_marker=False)
    monkeypatch.setattr(module, "mounted_volume", lambda _: ("1", "9p", "D:\\"))
    with pytest.raises(SnapshotError, match="source filesystem"):
        guard.check(require_marker=False)


def test_readonly_plan_inventory_skips_only_incomplete_transfer_files(setup):
    cfg, *_ = setup
    objects, errors = inventory(cfg)
    assert not errors
    parent = cfg.source / "objects/packs/aa"
    parent.mkdir(exist_ok=True)
    (parent / ".syncthing.transfer.tmp").write_bytes(b"partial")
    assert len(inventory(cfg)[0]) == len(objects)
    (parent / "bad.sync-conflict-test.zip").write_bytes(b"conflict")
    assert inventory(cfg)[1]


def test_prior_receipt_never_hides_missing_or_changed_backup(setup):
    cfg, work, release, backup = setup
    backup.run_once()
    item = inventory(cfg)[0][0]
    target = cfg.destination / item["relative"]
    target.unlink()
    assert backup.run_once()["copied_objects"] == 1
    target.write_bytes(b"x" * item["bytes"])
    assert backup.run_once()["state"] == "degraded"


def test_explicit_scrub_verifies_existing_without_duplicate_objects(setup):
    cfg, work, release, backup = setup
    backup.run_once()
    result = backup.run_once(force_verify=True)
    assert result["state"] == "up_to_date"
    assert result["copied_objects"] == 0
    assert result["verified_bytes"] == result["total_bytes"]


def test_volume_guard_mount_change_fails_closed(setup, monkeypatch):
    cfg, *_ = setup
    guard = VolumeGuard(cfg)
    guard.mount_identity = ("original", "9p", "D:\\")
    monkeypatch.setattr(module, "mounted_volume", lambda _: ("replacement", "9p", "D:\\"))
    with pytest.raises(SnapshotError, match="remounted"):
        guard.check(require_marker=False)


def test_watcher_observes_atomic_cold_receive(tmp_path):
    from scripts.run_live_artifact_sync import RecursiveInotify
    root = tmp_path / "objects"
    root.mkdir()
    watcher = RecursiveInotify({"objects": root})
    try:
        stage = root / ".syncthing.example.tmp"
        stage.write_bytes(b"received")
        stage.rename(root / "completed.blob")
        changed, overflow = watcher.wait(1)
        assert not overflow
        assert Path("completed.blob") in changed["objects"]
        assert not any(str(path).startswith(".syncthing.") for path in changed["objects"])
    finally:
        watcher.close()
