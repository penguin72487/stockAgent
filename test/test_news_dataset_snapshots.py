"""Byte-exact incremental news input projections; no collectors or live data."""
from __future__ import annotations

import hashlib

import pytest

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, sha256_file
from stockagent.data import news_dataset_snapshots as snapshots


def projection(root, name, data):
    generation = root / "versions" / name
    generation.mkdir(parents=True)
    atomic_write_json(generation / "generation.json", {"generation": f"versions/{name}"})
    atomic_write_bytes(generation / "snapshot.sqlite3", data)
    return generation, {"snapshot_path": "snapshot.sqlite3", "snapshot_sha256": hashlib.sha256(data).hexdigest()}


def test_changed_chunk_is_the_only_new_payload_and_restore_is_byte_exact(tmp_path):
    block = snapshots.CHUNK_BYTES
    original = b"a" * block + b"b" * block + b"tail"
    first, old = projection(tmp_path, "one", original)
    old = snapshots.seal_snapshot(tmp_path, first, old, None)
    second, new = projection(tmp_path, "two", b"a" * block + b"c" * block + b"tail")
    parent = {"generation": "versions/one", "source_snapshot": old}
    new = snapshots.seal_snapshot(tmp_path, second, new, parent)
    storage = new["snapshot_storage"]
    assert storage["written_chunk_bytes"] == block
    assert storage["reused_chunk_bytes"] == block + 4
    assert len(list((second / "snapshot_chunks").iterdir())) == 1
    destination = tmp_path / "restored.sqlite3"
    snapshots.copy_snapshot(tmp_path, second, new, destination)
    assert destination.read_bytes() == b"a" * block + b"c" * block + b"tail"
    assert sha256_file(first / "snapshot.sqlite3") == old["snapshot_sha256"]
    assert snapshots.seal_snapshot(tmp_path, second, new, parent) == new


def test_legacy_ranges_share_full_snapshot_without_copying_it(tmp_path):
    data = b"x" * (snapshots.CHUNK_BYTES + 37)
    first, old = projection(tmp_path, "one", data)
    second, new = projection(tmp_path, "two", data)
    new = snapshots.seal_snapshot(tmp_path, second, new, {"generation": "versions/one", "source_snapshot": old})
    chunks = new["snapshot_storage"]["chunks"]
    assert new["snapshot_storage"]["written_chunk_bytes"] == 0
    assert {c["path"] for c in chunks} == {"versions/one/snapshot.sqlite3"}
    assert [c["offset"] for c in chunks] == [0, snapshots.CHUNK_BYTES]
    assert not (second / "snapshot_chunks").exists()
    assert b"".join(snapshots.snapshot_bytes(tmp_path, new)) == data


@pytest.mark.parametrize("mutation", ["sha", "truncated", "extent", "logical_size", "escape"])
def test_chunk_corruption_and_unsafe_references_are_rejected(tmp_path, mutation):
    first, plan = projection(tmp_path, "one", b"raw input")
    plan = snapshots.seal_snapshot(tmp_path, first, plan, None)
    chunk = plan["snapshot_storage"]["chunks"][0]
    if mutation == "sha":
        chunk["sha256"] = "0" * 64
    elif mutation == "truncated":
        atomic_write_bytes(tmp_path / chunk["path"], b"short")
    elif mutation == "extent":
        chunk["offset"] = -1
    elif mutation == "logical_size":
        plan["snapshot_storage"]["logical_bytes"] += 1
    else:
        chunk["path"] = "../outside.sqlite3"
    with pytest.raises(ValueError):
        snapshots.verify_snapshot(tmp_path, first, plan)


def test_legacy_projection_cannot_escape_generation(tmp_path):
    generation, plan = projection(tmp_path, "one", b"raw input")
    projection(tmp_path, "two", b"raw input")
    plan["snapshot_path"] = "../two/snapshot.sqlite3"
    with pytest.raises(ValueError):
        snapshots.verify_snapshot(tmp_path, generation, plan)


def test_scratch_cleanup_requires_own_active_committed_transaction(tmp_path):
    generation, plan = projection(tmp_path, "one", b"raw input")
    plan = snapshots.seal_snapshot(tmp_path, generation, plan, None)
    manifest = {"contract_version": 2, "generation": "versions/one", "source_snapshot": plan}
    atomic_write_json(tmp_path / "manifest.json", manifest)
    atomic_write_json(tmp_path / ".active-build.json", {"generation": "versions/another"})
    with pytest.raises(ValueError, match="own committed active transaction"):
        snapshots.discard_new_build_work(tmp_path, generation, manifest)
    assert (generation / "snapshot.sqlite3").exists()
