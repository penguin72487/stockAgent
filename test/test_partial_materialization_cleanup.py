import fcntl
import json
import os
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import materialized_cache as cache
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
)


def staging_fixture(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "good.txt").write_text("recoverable")
    (source / "large.txt").write_text("big data" * 1000)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    release = publish_packed_snapshot(
        cold, "example", source, pack_buckets=2, loose_file_threshold_bytes=1000
    )
    sid = release.manifest["snapshot_id"]
    hot = tmp_path / "hot"
    name = f".{sid}.partial." + "a" * 32
    target = hot / "example" / name
    target.mkdir(parents=True)
    old = time.time() - 10 * 86400
    for name in ("good.txt", "large.txt"):
        (target / name).write_bytes((source / name).read_bytes())
        os.utime(target / name, (old, old))
    return cold, hot, sid, target, release


def prune(cold, hot, sid, target, *, apply=False):
    return cache.prune_partial_materialization(
        cold,
        hot,
        "example",
        sid,
        target.name,
        receipt_dir=hot / "receipts",
        apply=apply,
    )


def test_partial_dry_run_decodes_both_pack_and_blob_but_deletes_nothing(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    result = prune(cold, hot, sid, target)
    assert result["deleted_files"] == 0
    assert result["independently_decoded_files"] == 2
    assert len(result["selected"]) == 2
    assert (target / "good.txt").exists()


def test_partial_apply_unlinks_only_cold_verified_copy(tmp_path):
    cold, hot, sid, target, release = staging_fixture(tmp_path)
    result = prune(cold, hot, sid, target, apply=True)
    assert result["deleted_files"] == 2
    assert result["deleted_allocated_bytes"] > 0
    assert not target.exists()
    assert all(
        (cold / obj["relpath"]).exists()
        for obj in release.manifest["archive"]["objects"]
    )
    assert (tmp_path / "source/good.txt").read_text() == "recoverable"


def test_unknown_changed_and_young_files_remain(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    (target / "unknown.txt").write_text("unique evidence")
    (target / "good.txt").write_text("different!!")
    old = time.time() - 10 * 86400
    os.utime(target / "good.txt", (old, old))
    os.utime(target / "large.txt", None)
    result = prune(cold, hot, sid, target, apply=True)
    assert result["deleted_files"] == 0
    assert {row["path"] for row in result["kept"]} == {
        "good.txt",
        "unknown.txt",
        "large.txt",
    }
    assert (target / "unknown.txt").read_text() == "unique evidence"


def test_partial_missing_cold_object_fails_closed(tmp_path):
    cold, hot, sid, target, release = staging_fixture(tmp_path)
    (cold / release.manifest["archive"]["objects"][0]["relpath"]).unlink()
    with pytest.raises(SnapshotError, match="not locally complete"):
        prune(cold, hot, sid, target, apply=True)
    assert (target / "good.txt").exists()


def test_partial_corrupt_cold_object_fails_closed(tmp_path):
    cold, hot, sid, target, release = staging_fixture(tmp_path)
    obj = cold / release.manifest["archive"]["objects"][0]["relpath"]
    value = bytearray(obj.read_bytes())
    value[0] ^= 1
    obj.write_bytes(value)
    with pytest.raises(SnapshotError):
        prune(cold, hot, sid, target, apply=True)
    assert (target / "good.txt").exists()


def test_complete_materialization_name_cannot_be_targeted(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    with pytest.raises(SnapshotError, match="staging name"):
        cache.prune_partial_materialization(
            cold, hot, "example", sid, sid, receipt_dir=hot / "receipts", apply=True
        )
    assert target.exists()


def test_partial_active_process_or_pin_blocks(tmp_path, monkeypatch):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    monkeypatch.setattr(cache, "process_references", lambda target: ["pid=1:fd"])
    with pytest.raises(SnapshotError, match="in use"):
        prune(cold, hot, sid, target, apply=True)
    monkeypatch.setattr(cache, "process_references", lambda target: [])
    (hot / "example.pin.json").write_text(
        json.dumps({"manifest": {"snapshot_id": sid}})
    )
    with pytest.raises(SnapshotError, match="pinned"):
        prune(cold, hot, sid, target, apply=True)


def test_partial_fetch_lock_is_nonblocking(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    lock = hot / ".locks" / f"fetch-example-{sid}.lock"
    lock.parent.mkdir()
    with lock.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            prune(cold, hot, sid, target, apply=True)
    assert target.exists()


def test_shared_payload_does_not_claim_freed_space(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    os.link(target / "good.txt", tmp_path / "another-alias.txt")
    result = prune(cold, hot, sid, target, apply=True)
    assert result["deleted_files"] == 1
    assert (target / "good.txt").exists()
    assert any(row["reason"] == "shared-inode-preserve" for row in result["kept"])


def test_unknown_empty_directory_is_preserved(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    (target / "unknown-empty-evidence").mkdir()
    result = prune(cold, hot, sid, target, apply=True)
    assert result["deleted_files"] == 2
    assert (target / "unknown-empty-evidence").is_dir()


def test_redirected_materialized_root_cannot_be_targeted(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    redirected = tmp_path / "hot-alias"
    redirected.symlink_to(hot, target_is_directory=True)
    with pytest.raises(SnapshotError, match="redirected"):
        prune(cold, redirected, sid, target, apply=True)
    assert (target / "good.txt").exists()


def test_file_changed_after_cold_verification_is_preserved(tmp_path, monkeypatch):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    verify = cache.verify_packed_snapshot

    def change_after_proof(*args, **kwargs):
        result = verify(*args, **kwargs)
        (target / "good.txt").write_text("unique changed data")
        return result

    monkeypatch.setattr(cache, "verify_packed_snapshot", change_after_proof)
    result = prune(cold, hot, sid, target, apply=True)
    assert result["deleted_files"] == 1
    assert (target / "good.txt").read_text() == "unique changed data"
    assert {row["reason"] for row in result["kept"]} == {"changed-before-unlink"}


@pytest.mark.parametrize("immediate", [False, True])
def test_young_partial_requires_explicit_manual_age_bypass_and_retains_unknowns(tmp_path, immediate):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    for file in target.iterdir():
        os.utime(file, None)
    (target / "unknown-proof").write_bytes(b"unique evidence")
    result = cache.prune_partial_materialization(cold, hot, "example", sid, target.name,
        receipt_dir=hot / "receipts", apply=True, manual_immediate=immediate)
    assert result["deleted_files"] == (2 if immediate else 0)
    assert result["manual_immediate"] is immediate
    assert result["configured_min_age_days"] == 7
    assert result["effective_min_age_days"] == (0 if immediate else 7)
    assert (target / "unknown-proof").read_bytes() == b"unique evidence"
    assert (tmp_path / "source/good.txt").read_text() == "recoverable"


@pytest.mark.parametrize("flag", [1, "true", None])
def test_partial_manual_mode_requires_explicit_boolean(tmp_path, flag):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    with pytest.raises(SnapshotError, match="explicit booleans"):
        cache.prune_partial_materialization(cold, hot, "example", sid, target.name,
            receipt_dir=hot / "receipts", apply=True, manual_immediate=flag)
    assert (target / "good.txt").exists()


def test_native_partial_reads_cannot_target_unenrolled_cold_namespace(tmp_path):
    cold, hot, sid, target, _ = staging_fixture(tmp_path)
    with pytest.raises(SnapshotError, match="canonical D authority"):
        cache.prune_partial_materialization(cold, hot, "example", sid, target.name,
            receipt_dir=hot / "receipts", apply=True, manual_immediate=True, d_primary_native_reads=True)
    assert (target / "good.txt").exists()
