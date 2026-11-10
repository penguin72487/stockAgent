import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from stockagent.data_sync import packed_backup as module
from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
from stockagent.data_sync.packed_backup import (
    BackupConfig, PackedBackup, PinnedObjectSignatures, VolumeGuard, inventory, safe_path,
)
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout, publish_packed_snapshot, resolve_latest_packed,
    fetch_packed_snapshot, verify_packed_snapshot,
)
from scripts.packed_backup import watch_timeout


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
    timings = result["stage_timings_ms"]
    assert set(timings) == {
        "volume_guard", "source_inventory", "destination_trust", "initial_status",
        "object_processing", "metadata", "metadata_manifests", "metadata_heads",
        "head_recheck", "pre_final_status_total",
    }
    assert all(value >= 0 for value in timings.values())
    assert timings["metadata_manifests"] + timings["metadata_heads"] <= timings["metadata"] + 0.02
    assert timings["pre_final_status_total"] >= max(
        timings["source_inventory"], timings["destination_trust"], timings["metadata"]
    )
    selected = resolve_latest_packed(cfg.destination, "prices")
    assert selected.manifest_sha256 == release.manifest_sha256
    verify_packed_snapshot(cfg.destination, selected)
    restored = fetch_packed_snapshot(cfg.destination, tmp_path / "restore", selected)
    assert scan_tree(restored)["portable_fingerprint_sha256"] == scan_tree(work)["portable_fingerprint_sha256"]
    assert backup.run_once()["copied_objects"] == 0
    assert not (cfg.destination / ".local-state/node-id").exists()
    assert not (cfg.destination / "current").exists()
    assert (cfg.destination.parent / "last-complete.json").exists()


def test_watch_reconciles_on_events_and_bounds_quiet_full_scans(setup):
    cfg, _work, _release, _backup = setup
    assert watch_timeout(cfg, {"pending_objects": 0, "completed_objects": 0}, has_watcher=True) == 300
    assert watch_timeout(cfg, {"pending_objects": 0, "completed_objects": 0}, has_watcher=False) == 30
    assert watch_timeout(cfg, {"pending_objects": 2, "completed_objects": 1}, has_watcher=True) == 0
    assert watch_timeout(cfg, {"pending_objects": 2, "completed_objects": 0}, has_watcher=True) == 300


def test_backup_config_rejects_idle_reconcile_shorter_than_fallback_poll(tmp_path):
    raw = json.loads((Path(__file__).resolve().parents[1] / "configs/data_sync/packed_backup.json").read_text())
    raw["idle_reconcile_seconds"] = raw["poll_seconds"] - 1
    path = tmp_path / "backup.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(SnapshotError, match="invalid backup limits"):
        BackupConfig.load(path)


def test_backup_config_rejects_unknown_scope(tmp_path):
    raw = json.loads((Path(__file__).resolve().parents[1] / "configs/data_sync/packed_backup.json").read_text())
    raw["backup_scope"] = "latest_guess"
    path = tmp_path / "backup.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(SnapshotError, match="invalid backup limits"):
        BackupConfig.load(path)


def test_backup_guard_refuses_d_primary_source(setup):
    cfg, _work, _release, _backup = setup
    (cfg.source / ".stockagent-d-primary").write_text("D is primary\n")
    with pytest.raises(SnapshotError, match="same D volume"):
        VolumeGuard(cfg).check()


def test_unchanged_backup_checks_each_target_once_per_pass(setup, monkeypatch):
    cfg, _work, _release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    original = backup.trusted
    calls = []

    def record(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(backup, "trusted", record)
    assert backup.run_once()["state"] == "up_to_date"
    assert len(calls) == len(inventory(cfg)[0])


def test_backup_marks_old_success_as_checking_before_destination_scan(setup, monkeypatch):
    cfg, _work, _release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    original = backup.trusted
    seen = []

    def inspect_status(*args, **kwargs):
        if not seen:
            seen.append(json.loads((cfg.state_dir / "status.json").read_text()))
        return original(*args, **kwargs)

    monkeypatch.setattr(backup, "trusted", inspect_status)
    assert backup.run_once()["state"] == "up_to_date"
    assert seen[0]["state"] == "checking"
    assert seen[0]["phase"] == "destination_trust"
    assert seen[0]["current_heads_complete"] is False
    assert seen[0]["destination_trust_scanned"] == 0


def test_unchanged_metadata_skips_repeated_guard_but_write_still_requires_it(setup):
    cfg, _work, release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    relative = release.manifest_path.relative_to(cfg.source).as_posix()
    target = cfg.destination / relative
    original = target.read_bytes()

    class FailingGuard:
        def check(self, **kwargs):
            raise SnapshotError("volume unavailable")

    backup.guard = FailingGuard()
    backup.copy_metadata(relative, original)
    with pytest.raises(SnapshotError, match="volume unavailable"):
        backup.copy_metadata(relative, b"changed", mutable=True)
    assert target.read_bytes() == original


def test_unchanged_metadata_fastpath_does_not_resolve_destination_paths(
    setup, monkeypatch,
):
    cfg, _work, release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    relative = release.manifest_path.relative_to(cfg.source).as_posix()
    original = (cfg.destination / relative).read_bytes()
    safe_path_original = module.safe_path

    def source_only_safe_path(root, selected):
        if root == cfg.destination:
            raise AssertionError("unchanged D: metadata should use no-follow read")
        return safe_path_original(root, selected)

    monkeypatch.setattr(module, "safe_path", source_only_safe_path)
    backup.copy_metadata(relative, original)


def test_existing_metadata_fastpath_rejects_symlink_leaf(setup):
    cfg, _work, release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    relative = release.manifest_path.relative_to(cfg.source).as_posix()
    source = cfg.destination / relative
    alias = source.with_name("alias.json")
    alias.symlink_to(source.name)
    with pytest.raises((OSError, SnapshotError)):
        backup.copy_metadata(alias.relative_to(cfg.destination).as_posix(), source.read_bytes())


def test_unchanged_head_probe_does_not_use_following_path_read(setup, monkeypatch):
    cfg, _work, _release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    target_head = cfg.destination / "heads/prices/penguin.json"
    original = Path.read_bytes

    def forbid_following_head_read(path):
        if path == target_head:
            raise AssertionError("backup head probe must not follow a path")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", forbid_following_head_read)
    assert backup.run_once()["state"] == "up_to_date"


def test_replaced_backup_head_symlink_fails_closed(setup, tmp_path):
    cfg, _work, _release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    target_head = cfg.destination / "heads/prices/penguin.json"
    outside = tmp_path / "outside-head.json"
    original = target_head.read_bytes()
    outside.write_bytes(original)
    target_head.unlink()
    target_head.symlink_to(outside)

    result = backup.run_once()
    assert result["state"] == "degraded"
    assert result["current_heads_complete"] is False
    assert result["pending_heads"] == 1
    assert outside.read_bytes() == original


def test_new_head_rechecks_target_after_pass_cache(setup, monkeypatch):
    cfg, work, old, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    old_head = (cfg.destination / "heads/prices/penguin.json").read_bytes()
    old_refs = {ref["relpath"] for ref in old.manifest["archive"]["objects"]}
    (work / "large.bin").write_bytes(b"newblob" * 2048)
    new = publish_packed_snapshot(cfg.source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2)
    new_ref = next(ref for ref in new.manifest["archive"]["objects"] if ref["relpath"] not in old_refs)
    new_manifest = new.manifest_path.relative_to(cfg.source).as_posix()
    original = backup.copy_metadata
    changed = False

    def corrupt_before_promotion(relative, expected, **kwargs):
        nonlocal changed
        if relative == new_manifest and not changed:
            target = cfg.destination / new_ref["relpath"]
            target.write_bytes(b"x" * target.stat().st_size)
            changed = True
        return original(relative, expected, **kwargs)

    monkeypatch.setattr(backup, "copy_metadata", corrupt_before_promotion)
    result = backup.run_once()
    assert changed
    assert result["pending_heads"] == 1
    assert result["state"] == "copying"
    assert (cfg.destination / "heads/prices/penguin.json").read_bytes() == old_head


def test_pass_cache_rejects_changed_source_signature(setup, monkeypatch):
    cfg, _work, release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    ref = release.manifest["archive"]["objects"][0]
    original = backup.copy_metadata
    changed = False

    def change_source_during_metadata(relative, expected, **kwargs):
        nonlocal changed
        if relative == release.manifest_path.relative_to(cfg.source).as_posix() and not changed:
            source = cfg.source / ref["relpath"]
            source.write_bytes(b"x" * source.stat().st_size)
            changed = True
        return original(relative, expected, **kwargs)

    monkeypatch.setattr(backup, "copy_metadata", change_source_during_metadata)
    result = backup.run_once()
    assert changed
    assert result["pending_releases"] == 1
    assert result["pending_heads"] == 1


def test_backup_receipt_survives_remount_only_when_file_identity_is_stable(setup):
    cfg, _work, _release, backup = setup
    assert backup.run_once()["state"] == "up_to_date"
    item = inventory(cfg)[0][0]
    relative = item["relative"]
    with sqlite3.connect(cfg.state_dir / "verified.sqlite3") as connection:
        source_json, target_json = connection.execute(
            "SELECT source_sig,target_sig FROM verified WHERE path=?", (relative,)
        ).fetchone()
        source_sig = json.loads(source_json)
        target_sig = json.loads(target_json)
        source_sig[0] += 1
        target_sig[0] += 1
        connection.execute(
            "UPDATE verified SET source_sig=?,target_sig=? WHERE path=?",
            (json.dumps(source_sig), json.dumps(target_sig), relative),
        )
    assert backup.trusted(relative, item["sha256"], item["signature"])
    assert backup.run_once()["copied_objects"] == 0

    target_sig[2] += 1
    with sqlite3.connect(cfg.state_dir / "verified.sqlite3") as connection:
        connection.execute(
            "UPDATE verified SET target_sig=? WHERE path=?",
            (json.dumps(target_sig), relative),
        )
    assert not backup.trusted(relative, item["sha256"], item["signature"])


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


def test_current_scope_ignores_missing_old_release_without_deleting_history(setup, tmp_path):
    cfg, work, old, _backup = setup
    assert _backup.run_once()["state"] == "up_to_date"
    old_ref = next(ref for ref in old.manifest["archive"]["objects"] if ref["kind"] == "blob")
    old_backup_bytes = (cfg.destination / old_ref["relpath"]).read_bytes()
    (work / "large.bin").write_bytes(b"newblob" * 2048)
    current_release = publish_packed_snapshot(
        cfg.source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2,
    )
    (cfg.source / old_ref["relpath"]).unlink()
    current_cfg = replace(cfg, backup_scope="current_heads", state_dir=tmp_path / "current-state")
    current = PackedBackup(current_cfg, guard=TestGuard())
    try:
        result = current.run_once()
        assert result["state"] == "up_to_date"
        assert result["current_heads_complete"] is True
        assert result["backup_scope"] == "current_heads"
        assert result["historical_completeness"] == "not_checked"
        assert result["verified_releases"] == 1
        assert result["pending_releases"] == 0
        assert (cfg.destination / old_ref["relpath"]).read_bytes() == old_backup_bytes
        assert (cfg.destination.parent / "last-current-complete.json").exists()
        selected = resolve_latest_packed(cfg.destination, "prices")
        assert selected.manifest_sha256 == current_release.manifest_sha256
        verify_packed_snapshot(cfg.destination, selected)
    finally:
        current.close()


def test_current_scope_fails_closed_when_latest_object_is_missing(setup, tmp_path):
    cfg, work, _old, _backup = setup
    (work / "large.bin").write_bytes(b"newblob" * 2048)
    current_release = publish_packed_snapshot(
        cfg.source, "prices", work, loose_file_threshold_bytes=1024, pack_buckets=2,
    )
    current_ref = next(ref for ref in current_release.manifest["archive"]["objects"] if ref["kind"] == "blob")
    (cfg.source / current_ref["relpath"]).unlink()
    current_cfg = replace(cfg, backup_scope="current_heads", state_dir=tmp_path / "current-state")
    current = PackedBackup(current_cfg, guard=TestGuard())
    try:
        result = current.run_once()
        assert result["state"] == "degraded"
        assert result["current_heads_complete"] is False
        assert result["pending_heads"] == 1
        assert not (cfg.destination / "heads/prices/penguin.json").exists()
    finally:
        current.close()


def test_current_scope_does_not_complete_if_head_changes_during_pass(setup, tmp_path, monkeypatch):
    cfg, _work, _old, _backup = setup
    current_cfg = replace(cfg, backup_scope="current_heads", state_dir=tmp_path / "current-state")
    current = PackedBackup(current_cfg, guard=TestGuard())
    original = current.metadata

    def remove_head_after_metadata(*args, **kwargs):
        result = original(*args, **kwargs)
        (cfg.source / "heads/prices/penguin.json").unlink()
        return result

    monkeypatch.setattr(current, "metadata", remove_head_after_metadata)
    try:
        result = current.run_once()
        assert result["state"] == "copying"
        assert result["pending_heads"] == 1
        assert result["current_heads_complete"] is False
        assert not (cfg.destination.parent / "last-current-complete.json").exists()
    finally:
        current.close()


def test_safe_paths_reject_escape_and_symlinks(tmp_path):
    (tmp_path / "redirect").symlink_to(tmp_path / "outside")
    for relative in ("../escape", "/escape", "redirect/file", ""):
        with pytest.raises(SnapshotError):
            safe_path(tmp_path, relative)


@pytest.mark.parametrize('parallel_signatures', [False, True])
def test_pinned_object_signatures_reject_changed_directory(tmp_path, parallel_signatures):
    root = tmp_path / "packed"
    shard = root / "objects/blobs/aa"
    shard.mkdir(parents=True)
    relative = "objects/blobs/aa/" + "a" * 64 + ".blob"
    target = root / relative
    target.write_bytes(b"object")
    with PinnedObjectSignatures(root, parallel_signatures=parallel_signatures) as reader:
        assert reader.signature(relative) == module.signature(safe_path(root, relative))
        moved = tmp_path / "moved"
        shard.rename(moved)
        shard.symlink_to(moved, target_is_directory=True)
        with pytest.raises(SnapshotError, match="directory changed"):
            reader.recheck()


def test_regular_member_signature_keeps_cas_entrypoint_strict(tmp_path):
    member = tmp_path / 'encodings/deep/original.raw'
    member.parent.mkdir(parents=True)
    member.write_bytes(b'original archive bytes')
    with PinnedObjectSignatures(tmp_path) as reader:
        assert reader.regular_file_signature('encodings/deep/original.raw') == module.signature(member)
        with pytest.raises(SnapshotError):
            reader.signature('encodings/deep/original.raw')
        reader.recheck()


def test_directory_name_enumeration_uses_pins_without_leaf_type_probes(tmp_path, monkeypatch):
    shard = tmp_path / 'objects/blobs/aa'
    shard.mkdir(parents=True)
    (shard / ('a' * 64 + '.blob')).write_bytes(b'exact original')
    with PinnedObjectSignatures(tmp_path) as reader:
        # Opening/fstatting directories remains required; enumeration never
        # asks a leaf's type. The catalog later observes every selected leaf.
        monkeypatch.setattr(os, 'scandir', lambda *_: pytest.fail('no per-leaf type walk'))
        assert reader.directory_names('objects/blobs/aa') == ['a' * 64 + '.blob']
        moved = tmp_path / 'old-shard'
        shard.rename(moved)
        shard.mkdir()
        with pytest.raises(SnapshotError, match='directory changed'):
            reader.recheck()


def test_directory_name_enumeration_does_not_follow_an_unopened_alias(tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    (tmp_path / 'redirect').symlink_to(outside, target_is_directory=True)
    with PinnedObjectSignatures(tmp_path) as reader:
        with pytest.raises((OSError, SnapshotError)):
            reader.directory_names('redirect')


def test_parallel_member_signatures_overlap_and_keep_recheck_fences(tmp_path, monkeypatch):
    for name in ('first', 'second'):
        (tmp_path / name).write_bytes(name.encode())
    expected = [module.signature(tmp_path / name) for name in ('first', 'second')]
    original_stat = os.stat
    overlap = threading.Barrier(2, timeout=5)

    def observed_stat(path, *args, **kwargs):
        if path in ('first', 'second') and kwargs.get('dir_fd') is not None:
            overlap.wait()
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, 'stat', observed_stat)
    with PinnedObjectSignatures(tmp_path, parallel_signatures=True) as reader:
        with ThreadPoolExecutor(max_workers=2) as workers:
            assert list(workers.map(reader.regular_file_signature, ('first', 'second'))) == expected
        reader.recheck()
        assert reader._inflight_signatures == 0


@pytest.mark.parametrize('finish', ['close', 'recheck'])
def test_parallel_signatures_finish_before_close_or_recheck(tmp_path, monkeypatch, finish):
    (tmp_path / 'member').write_bytes(b'original')
    started, release = threading.Event(), threading.Event()
    original_stat = os.stat

    def slow_stat(path, *args, **kwargs):
        if path == 'member' and kwargs.get('dir_fd') is not None:
            started.set()
            assert release.wait(5)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, 'stat', slow_stat)
    reader = PinnedObjectSignatures(tmp_path, parallel_signatures=True)
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            signature = workers.submit(reader.regular_file_signature, 'member')
            assert started.wait(5)
            finished = workers.submit(getattr(reader, finish))
            assert not finished.done()
            release.set()
            assert signature.result(timeout=5)[2] == len(b'original')
            finished.result(timeout=5)
        assert reader._inflight_signatures == 0
    finally:
        release.set()
        reader.close()


def test_parallel_signature_error_releases_its_pin(tmp_path):
    (tmp_path / 'not-a-file').mkdir()
    with PinnedObjectSignatures(tmp_path, parallel_signatures=True) as reader:
        with pytest.raises(SnapshotError, match='not a regular'):
            reader.regular_file_signature('not-a-file')
        assert reader._inflight_signatures == 0
        reader.recheck()


def test_parallel_signature_survives_cache_eviction_without_losing_fence(tmp_path, monkeypatch):
    for directory in ('first', 'second'):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / 'member').write_bytes(directory.encode())
    started, release = threading.Event(), threading.Event()
    original_stat = os.stat
    calls = 0
    calls_lock = threading.Lock()

    def slow_first_stat(path, *args, **kwargs):
        nonlocal calls
        if path == 'member' and kwargs.get('dir_fd') is not None:
            with calls_lock:
                calls += 1
                first = calls == 1
            if first:
                started.set()
                assert release.wait(5)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, 'stat', slow_first_stat)
    with PinnedObjectSignatures(tmp_path, max_open_directories=2,
                                parallel_signatures=True) as reader:
        try:
            with ThreadPoolExecutor(max_workers=2) as workers:
                first = workers.submit(reader.regular_file_signature, 'first/member')
                assert started.wait(5)
                second = workers.submit(reader.regular_file_signature, 'second/member')
                assert second.result(timeout=5)[2] == len(b'second')
                assert ('first',) not in reader._directories
                release.set()
                assert first.result(timeout=5)[2] == len(b'first')
            reader.recheck()
        finally:
            release.set()


def test_parallel_metadata_read_survives_eviction_and_recheck_waits(tmp_path, monkeypatch):
    for directory in ('first', 'second'):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / 'member.json').write_bytes(directory.encode())
    started, release = threading.Event(), threading.Event()
    original_open = os.open
    calls = 0
    lock = threading.Lock()

    def slow_first_open(path, flags, *args, **kwargs):
        nonlocal calls
        if path == 'member.json' and kwargs.get('dir_fd') is not None:
            with lock:
                calls += 1
                first = calls == 1
            if first:
                started.set()
                assert release.wait(5)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', slow_first_open)
    with PinnedObjectSignatures(tmp_path, max_open_directories=2,
                                parallel_signatures=True) as reader:
        try:
            with ThreadPoolExecutor(max_workers=3) as workers:
                first = workers.submit(reader.read_metadata, 'first/member.json')
                assert started.wait(5)
                second = workers.submit(reader.read_metadata, 'second/member.json')
                assert second.result(timeout=5)[0] == b'second'
                assert ('first',) not in reader._directories
                fence = workers.submit(reader.recheck)
                assert not fence.done()
                release.set()
                assert first.result(timeout=5)[0] == b'first'
                fence.result(timeout=5)
            assert reader._inflight_signatures == 0
        finally:
            release.set()


def test_parallel_metadata_read_rejects_symlink_and_releases_pin(tmp_path):
    (tmp_path / 'source').write_bytes(b'original')
    (tmp_path / 'link.json').symlink_to(tmp_path / 'source')
    with PinnedObjectSignatures(tmp_path, parallel_signatures=True) as reader:
        with pytest.raises(OSError):
            reader.read_metadata('link.json')
        assert reader._inflight_signatures == 0
        reader.recheck()


@pytest.mark.parametrize('parallel_signatures', [False, True])
@pytest.mark.parametrize('kind', ['symlink', 'directory', 'fifo'])
def test_regular_member_signature_rejects_nonregular_leaf_without_opening(tmp_path, kind, parallel_signatures):
    member = tmp_path / 'member'
    if kind == 'symlink':
        other = tmp_path / 'other'
        other.write_bytes(b'preserve')
        member.symlink_to(other)
    elif kind == 'directory':
        member.mkdir()
    else:
        os.mkfifo(member)
    with PinnedObjectSignatures(tmp_path, parallel_signatures=parallel_signatures) as reader:
        with pytest.raises(SnapshotError, match='not a regular'):
            reader.regular_file_signature('member')


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
