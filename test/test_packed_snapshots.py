from __future__ import annotations

import os
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


def test_unchanged_publish_reuses_identical_content_objects(tmp_path: Path) -> None:
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

    assert (
        first.manifest["archive"]["inventory"]["sha256"]
        == second.manifest["archive"]["inventory"]["sha256"]
    )
    assert first.manifest["archive"]["objects"] == second.manifest["archive"]["objects"]
    assert first.manifest["snapshot_id"] != second.manifest["snapshot_id"]


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
