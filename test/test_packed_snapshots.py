from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
import stockagent.data_sync.packed_snapshots as packed_snapshots
from scripts.packed_snapshot import main as packed_snapshot_main
from stockagent.data_sync.packed_snapshots import (
    audit_packed_store,
    fetch_packed_snapshot,
    fetch_packed_subtree,
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)


def _source_tree(root: Path) -> Path:
    source = root / "source"
    (source / "empty").mkdir(parents=True)
    (source / "text").mkdir()
    (source / "text" / "first.json").write_text('{"first": 1}\n', encoding="utf-8")
    (source / "text" / "second.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    large = b"large-content\n" * 512
    (source / "large-a.bin").write_bytes(large)
    (source / "large-b.bin").write_bytes(large)
    (source / "current").symlink_to("text/first.json")
    return source


def test_unmounted_d_cold_fallback_refuses_layout_and_publication(tmp_path: Path) -> None:
    sync_root = tmp_path / "cold"
    sync_root.mkdir()
    (sync_root / ".stockagent-d-mount-required").write_text("mount required\n")
    source = _source_tree(tmp_path)

    with pytest.raises(SnapshotError, match="not mounted"):
        initialize_packed_layout(sync_root, node_id="node-a")
    with pytest.raises(SnapshotError, match="not mounted"):
        publish_packed_snapshot(sync_root, "prices", source, node_id="node-a")
    assert not (sync_root / "objects").exists()


def test_packed_snapshot_round_trip_and_content_dedup(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )
    archive = resolved.manifest["archive"]
    blobs = [item for item in archive["objects"] if item["kind"] == "blob"]
    packs = [item for item in archive["objects"] if item["kind"] == "pack"]

    assert len(blobs) == 1
    assert blobs[0]["file_count"] == 2
    assert 1 <= len(packs) <= 4
    assert archive["object_count"] == len(blobs) + len(packs)
    verification = verify_packed_snapshot(sync_root, resolved)
    assert verification["objects"] == archive["object_count"]

    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)
    assert (
        scan_tree(target)["portable_fingerprint_sha256"]
        == scan_tree(source)["portable_fingerprint_sha256"]
    )
    assert (target / "large-a.bin").read_bytes() == (
        source / "large-a.bin"
    ).read_bytes()
    assert (target / "text" / "first.json").read_text(encoding="utf-8") == (
        source / "text" / "first.json"
    ).read_text(encoding="utf-8")
    assert (target / "current").is_symlink()
    assert os.readlink(target / "current") == "text/first.json"
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_metadata_only_resolver_allows_edge_to_hydrate_missing_objects(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)
    for item in published.manifest["archive"]["objects"]:
        (sync_root / item["relpath"]).unlink()

    with pytest.raises(SnapshotError, match="not locally complete"):
        resolve_latest_packed(sync_root, "prices")

    resolved = resolve_latest_packed(sync_root, "prices", require_objects=False)
    by_id = resolve_packed_snapshot_id(
        sync_root,
        "prices",
        str(published.manifest["snapshot_id"]),
        require_objects=False,
    )
    assert resolved.manifest["snapshot_id"] == published.manifest["snapshot_id"]
    assert by_id.manifest["snapshot_id"] == published.manifest["snapshot_id"]


def test_shared_blob_reuse_does_not_write_duplicate_scratch(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    copy_and_hash = packed_snapshots._copy_and_hash
    copied = []

    def record_copy(path, temporary):
        copied.append(path)
        return copy_and_hash(path, temporary)

    monkeypatch.setattr(packed_snapshots, "_copy_and_hash", record_copy)
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    assert len(copied) == 1  # two equal source files, one physical blob write
    copied.clear()
    second = publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert copied == []  # cross-dataset reuse still hashes the existing cold blob
    assert first.manifest["archive"]["stored_bytes"] == second.manifest["archive"]["stored_bytes"]
    assert second.manifest["archive"]["new_object_count"] == 0
    result = verify_packed_snapshot(cold, second, reconstruct_paths=["large-a.bin", "large-b.bin"])
    assert result["independently_reconstructed_files"] == 2


def test_shared_corrupt_blob_is_not_overwritten_or_published(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    corrupt = b"X" * path.stat().st_size
    path.write_bytes(corrupt)
    monkeypatch.setattr(packed_snapshots, "_copy_and_hash", lambda *_: pytest.fail("duplicate scratch write"))
    with pytest.raises(SnapshotError, match="object is corrupt"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert path.read_bytes() == corrupt
    assert not (cold / "heads/second/test-node.json").exists()


def test_shared_blob_reuse_rechecks_source_signature(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    hash_file = packed_snapshots.sha256_file

    def mutate_source_during_cold_hash(target):
        digest = hash_file(target)
        if target == path:
            original = source / "large-a.bin"
            original.write_bytes(b"Y" * original.stat().st_size)
        return digest

    monkeypatch.setattr(packed_snapshots, "sha256_file", mutate_source_during_cold_hash)
    with pytest.raises(SnapshotError, match="source file changed"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert not (cold / "heads/second/test-node.json").exists()


def test_shared_blob_reuse_rejects_cold_path_redirection(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    first = publish_packed_snapshot(cold, "first", source, loose_file_threshold_bytes=1024)
    blob = next(item for item in first.manifest["archive"]["objects"] if item["kind"] == "blob")
    path = cold / blob["relpath"]
    path.unlink()
    path.symlink_to(source / "large-a.bin")
    with pytest.raises(SnapshotError, match="object is redirected"):
        publish_packed_snapshot(cold, "second", source, loose_file_threshold_bytes=1024)
    assert not (cold / "heads/second/test-node.json").exists()


def test_selected_reconstruction_checks_pack_and_shared_blob_paths(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(
        cold, "prices", source, loose_file_threshold_bytes=1024
    )
    result = verify_packed_snapshot(
        cold,
        resolved,
        reconstruct_paths=["text/first.json", "large-a.bin", "large-b.bin", "large-a.bin"],
    )
    assert result["independently_reconstructed_files"] == 3
    assert result["objects"] == resolved.manifest["archive"]["object_count"]
    assert not result["materialized_verified"]


@pytest.mark.parametrize("requested", ["current", "empty", "missing", "../escape"])
def test_selected_reconstruction_rejects_non_file_inventory_paths(tmp_path, requested):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    with pytest.raises(SnapshotError, match="non-file or unknown"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=[requested])


def test_selected_reconstruction_checks_file_hash_independent_of_zip_crc(tmp_path, monkeypatch):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    load_inventory = packed_snapshots._load_inventory

    def wrong_file_hash(*args):
        rows = load_inventory(*args)
        next(row for row in rows if row["path"] == "text/first.json")["sha256"] = "0" * 64
        return rows

    monkeypatch.setattr(packed_snapshots, "_load_inventory", wrong_file_hash)
    with pytest.raises(SnapshotError, match="exact cold reconstruction failed"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=["text/first.json"])


def test_empty_reconstruction_request_still_hashes_all_cold_objects(tmp_path):
    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    resolved = publish_packed_snapshot(cold, "prices", source)
    obj = cold / resolved.manifest["archive"]["objects"][0]["relpath"]
    payload = bytearray(obj.read_bytes())
    payload[0] ^= 1
    obj.write_bytes(payload)
    with pytest.raises(SnapshotError, match="checksum mismatch"):
        verify_packed_snapshot(cold, resolved, reconstruct_paths=[])


def test_explicit_missing_object_recovery_publishes_current_bytes_under_new_identity(tmp_path):
    source = _source_tree(tmp_path)
    root = tmp_path / "cold"
    initialize_packed_layout(root, node_id="node-a")
    original = publish_packed_snapshot(root, "prices", source, pack_buckets=1)
    original_manifest = original.manifest_path.read_bytes()
    old = original.manifest["archive"]["objects"][0]
    (root / old["relpath"]).unlink()
    (source / "text/first.json").write_text('{"first": 2}\n')
    with pytest.raises(SnapshotError):
        publish_packed_snapshot(root, "prices", source)
    repaired = publish_packed_snapshot(root, "prices", source, pack_buckets=1,
                                      recover_missing_base_objects=True)
    assert repaired.manifest["snapshot_id"] != original.manifest["snapshot_id"]
    assert original.manifest_path.read_bytes() == original_manifest
    assert not (root / old["relpath"]).exists()  # newer bytes never pretend to be the lost old version
    assert repaired.manifest["archive"]["base_missing_objects_repacked_from_current_source"][0]["sha256"] == old["sha256"]
    verify_packed_snapshot(root, repaired)
    assert resolve_latest_packed(root, "prices").manifest["snapshot_id"] == repaired.manifest["snapshot_id"]


def test_recovery_checks_corrupt_reused_objects_before_head_update(tmp_path):
    source = _source_tree(tmp_path)
    root = tmp_path / "cold"
    initialize_packed_layout(root, node_id="node-a")
    original = publish_packed_snapshot(root, "prices", source, loose_file_threshold_bytes=1024)
    head = original.head_path.read_bytes()
    blob = next(o for o in original.manifest["archive"]["objects"] if o["kind"] == "blob")
    original_size = (root / blob["relpath"]).stat().st_size
    (root / blob["relpath"]).write_bytes(b"X" * original_size)
    (source / "text/first.json").write_text('{"first": 2}\n')
    with pytest.raises(SnapshotError, match="checksum mismatch"):
        publish_packed_snapshot(root, "prices", source, loose_file_threshold_bytes=1024,
                                recover_missing_base_objects=True)
    assert original.head_path.read_bytes() == head


def test_full_store_audit_hashes_all_objects_without_deleting(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)

    result = audit_packed_store(sync_root)

    assert result["valid_manifests"] == 1
    assert result["resolved_datasets"] == 1
    assert result["stored_objects"] == published.manifest["archive"]["object_count"] + 1
    assert result["all_stored_object_sha256_valid"] is True
    assert result["deleted"] == 0


def test_group_writable_packed_root_repairs_public_directory_modes(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    sync_root.mkdir()
    sync_root.chmod(0o2770)
    stale_bucket = sync_root / "objects" / "inventories" / "ff"
    stale_bucket.mkdir(parents=True)
    stale_bucket.chmod(0o2755)

    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    public_directories = [
        path
        for top in ("heads", "manifests", "objects")
        for path in (sync_root / top).rglob("*")
        if path.is_dir()
    ]
    public_directories.extend(
        sync_root / top for top in ("heads", "manifests", "objects")
    )
    assert stale_bucket in public_directories
    assert all(path.stat().st_mode & stat.S_IWGRP for path in public_directories)
    assert all(path.stat().st_mode & stat.S_ISGID for path in public_directories)
    assert resolved.manifest_path.parent.stat().st_mode & stat.S_IWGRP


def test_packed_snapshot_fetch_subtree_is_atomic_and_verified(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    target = fetch_packed_subtree(
        sync_root, tmp_path / "materialized-subtree", resolved, "text"
    )

    assert target.name == "text"
    assert (target / "first.json").read_bytes() == (
        source / "text" / "first.json"
    ).read_bytes()
    assert (target / "second.csv").read_bytes() == (
        source / "text" / "second.csv"
    ).read_bytes()
    assert not (target / "large-a.bin").exists()
    assert (
        fetch_packed_subtree(
            sync_root, tmp_path / "materialized-subtree", resolved, "text"
        )
        == target
    )


def test_packed_snapshot_fetch_subtree_rejects_missing_directory(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(sync_root, "prices", source)

    with pytest.raises(SnapshotError, match="subtree is missing"):
        fetch_packed_subtree(
            sync_root, tmp_path / "materialized-subtree", resolved, "missing"
        )


def test_packed_snapshot_excludes_reproducible_subtree(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    cache = source / "text" / "cache"
    cache.mkdir()
    (cache / "derived.npy").write_bytes(b"reproducible-cache")
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        excluded_subtrees=["text/cache"],
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)

    assert resolved.manifest["source"]["excluded_subtrees"] == ["text/cache"]
    assert not (target / "text" / "cache").exists()
    assert (target / "text" / "first.json").is_file()
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_packed_snapshot_can_select_only_small_files(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    resolved = publish_packed_snapshot(
        sync_root,
        "small-prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
        maximum_file_bytes=1023,
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", resolved)

    assert resolved.manifest["source"]["selection"] == {
        "maximum_file_bytes": 1023,
        "symlinks": "included",
    }
    assert resolved.manifest["source"]["files"] == 2
    assert resolved.manifest["source"]["omitted_files_above_maximum"] == 2
    assert all(
        item["kind"] == "pack" for item in resolved.manifest["archive"]["objects"]
    )
    assert (target / "text" / "first.json").is_file()
    assert not (target / "large-a.bin").exists()
    assert not (target / "large-b.bin").exists()
    assert verify_packed_snapshot(sync_root, resolved, materialized_path=target)[
        "materialized_verified"
    ]


def test_unchanged_publish_is_a_semantic_noop(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )
    second = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=4,
    )

    assert second.manifest["snapshot_id"] == first.manifest["snapshot_id"]
    assert second.manifest_sha256 == first.manifest_sha256
    assert second.head_path == first.head_path
    assert len(list((sync_root / "manifests" / "prices").glob("*.json"))) == 1


def test_source_guard_vetoes_semantic_noop_without_changing_existing_head(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)
    head_before = first.head_path.read_bytes()
    calls = 0

    def guard() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SnapshotError("producer proof changed")

    with pytest.raises(SnapshotError, match="producer proof changed"):
        publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2, source_guard=guard)
    assert calls == 2
    assert first.head_path.read_bytes() == head_before
    assert len(list((sync_root / "manifests/prices").glob("*.json"))) == 1


@pytest.mark.parametrize("defer", [False, True])
def test_transport_notification_releases_global_publish_locks_even_for_noop(tmp_path, monkeypatch, defer):
    import fcntl
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    observed = []

    def notify(sync_root, dataset, **kwargs):
        assert (sync_root / "heads" / dataset / "node-a.json").is_file()
        for name in ("publish-retention-global.lock", "publish-prices.lock"):
            with (sync_root / ".local-state/locks" / name).open("a+b") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        observed.append(kwargs)
        return True

    monkeypatch.setattr(syncthing_scan, "queue_after_publish" if defer else "scan_after_publish", notify)
    if defer:
        monkeypatch.setattr(syncthing_scan, "scan_after_publish", lambda *_a, **_k: pytest.fail("deferred commit cannot do network scan"))
    first = publish_packed_snapshot(cold, "prices", source, defer_scan=defer)
    second = publish_packed_snapshot(cold, "prices", source, defer_scan=defer)
    assert first.manifest["snapshot_id"] == second.manifest["snapshot_id"]
    assert len(observed) == 2
    assert observed[0]["new_object_paths"]


def test_queue_failure_after_head_commit_is_not_an_unpublished_release(tmp_path, monkeypatch):
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    monkeypatch.setattr(syncthing_scan, "queue_after_publish", lambda *_a, **_k: (_ for _ in ()).throw(SnapshotError("queue fsync failed")))
    with pytest.raises(SnapshotError, match="queue fsync failed"):
        publish_packed_snapshot(cold, "prices", source, defer_scan=True)
    assert (cold / "heads/prices/node-a.json").is_file()


def test_deferred_scan_cannot_claim_queue_when_d_primary_disappears(tmp_path, monkeypatch):
    from stockagent.data_sync import syncthing_scan

    source = _source_tree(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="node-a")
    monkeypatch.setattr(syncthing_scan, "queue_after_publish", lambda *_a, **_k: False)
    with pytest.raises(SnapshotError, match="durable scan intent was not queued"):
        publish_packed_snapshot(cold, "prices", source, defer_scan=True)
    assert (cold / "heads/prices/node-a.json").is_file()


def test_changed_small_file_uses_delta_pack_and_reuses_old_members(
    tmp_path: Path,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    first = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=1,
    )
    (source / "text" / "first.json").write_text(
        '{"first": 2}\n', encoding="utf-8"
    )

    second = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=1,
    )
    archive = second.manifest["archive"]

    assert archive["base_snapshot_id"] == first.manifest["snapshot_id"]
    assert archive["reused_files"] == 3
    assert archive["changed_files"] == 1
    assert archive["new_stored_bytes"] < archive["stored_bytes"]
    assert any(
        item.get("member_selection") == "subset"
        for item in archive["objects"]
    )
    target = fetch_packed_snapshot(sync_root, tmp_path / "materialized", second)
    assert (target / "text" / "first.json").read_text(encoding="utf-8") == (
        '{"first": 2}\n'
    )
    assert (target / "text" / "second.csv").is_file()
    assert verify_packed_snapshot(sync_root, second, materialized_path=target)[
        "materialized_verified"
    ]


def test_latest_packed_snapshot_uses_per_node_heads(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)
    initialize_packed_layout(
        sync_root,
        node_id="node-b",
        replace_node_id=True,
    )
    (source / "text" / "first.json").write_text(
        '{"publisher": "node-b"}\n', encoding="utf-8"
    )
    expected = publish_packed_snapshot(sync_root, "prices", source, pack_buckets=2)

    actual = resolve_latest_packed(sync_root, "prices")

    assert actual.manifest["snapshot_id"] == expected.manifest["snapshot_id"]
    assert actual.manifest["publisher"]["node_id"] == "node-b"


def test_corrupt_object_is_rejected(tmp_path: Path) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root,
        "prices",
        source,
        loose_file_threshold_bytes=1024,
        pack_buckets=2,
    )
    pack = next(
        item
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "pack"
    )
    pack_path = sync_root.joinpath(*Path(pack["relpath"]).parts)
    with pack_path.open("r+b") as stream:
        stream.seek(max(0, pack_path.stat().st_size // 2))
        original = stream.read(1)
        stream.seek(-1, os.SEEK_CUR)
        stream.write(bytes([original[0] ^ 0xFF]))

    with pytest.raises(SnapshotError, match="checksum mismatch"):
        verify_packed_snapshot(sync_root, resolved)


def test_small_pack_verification_reuses_one_bounded_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    resolved = publish_packed_snapshot(
        sync_root, "prices", source, loose_file_threshold_bytes=1024,
        pack_buckets=2,
    )
    packs = {
        sync_root / item["relpath"]
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "pack"
    }
    blobs = {
        sync_root / item["relpath"]
        for item in resolved.manifest["archive"]["objects"]
        if item["kind"] == "blob"
    }
    original_sha256_file = packed_snapshots.sha256_file
    hashed_paths: list[Path] = []

    def traced_sha256_file(path: Path) -> str:
        hashed_paths.append(path)
        return original_sha256_file(path)

    monkeypatch.setattr(packed_snapshots, "sha256_file", traced_sha256_file)
    bounded = verify_packed_snapshot(sync_root, resolved)
    assert not packs.intersection(hashed_paths)
    assert blobs.issubset(hashed_paths)

    hashed_paths.clear()
    monkeypatch.setattr(packed_snapshots, "MAX_IN_MEMORY_PACK_VERIFY_BYTES", 0)
    streamed = verify_packed_snapshot(sync_root, resolved)
    assert packs.issubset(hashed_paths)
    assert streamed == bounded


def test_verify_cli_profile_keeps_complete_verification_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    source = _source_tree(tmp_path)
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")
    published = publish_packed_snapshot(sync_root, "prices", source)

    assert packed_snapshot_main([
        "verify", "prices", "--snapshot-id", published.manifest["snapshot_id"],
        "--sync-root", str(sync_root), "--profile",
    ]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["snapshot_id"] == published.manifest["snapshot_id"]
    assert payload["objects"] == published.manifest["archive"]["object_count"]
    assert payload["profile"]["scope"] == (
        "local_process_resolution_and_full_verify_only"
    )
    assert payload["profile"]["elapsed_seconds"] >= 0
    assert payload["profile"]["rchar_bytes"] is None or (
        payload["profile"]["rchar_bytes"] > 0
    )


def test_symlink_that_escapes_source_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "unsafe").symlink_to("../outside")
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    with pytest.raises(SnapshotError, match="escapes the snapshot root"):
        publish_packed_snapshot(sync_root, "prices", source)
