from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError, scan_tree
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot,
    fetch_packed_subtree,
    initialize_packed_layout,
    publish_packed_snapshot,
    resolve_latest_packed,
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


def test_symlink_that_escapes_source_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "unsafe").symlink_to("../outside")
    sync_root = tmp_path / "sync"
    initialize_packed_layout(sync_root, node_id="node-a")

    with pytest.raises(SnapshotError, match="escapes the snapshot root"):
        publish_packed_snapshot(sync_root, "prices", source)
