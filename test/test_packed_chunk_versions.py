"""Exact incremental versions shared by every file-backed provider, not news-only."""
from __future__ import annotations

import pytest

import stockagent.data_sync.packed_snapshots as packed

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import (
    _load_inventory, _validate_inventory, fetch_packed_snapshot, fetch_packed_subtree,
    publish_packed_snapshot, resolve_packed_snapshot_id, stage_packed_root_files,
    verify_packed_snapshot,
)


def publish(root, source, dataset="provider"):
    return publish_packed_snapshot(root, dataset, source, node_id="test",
        loose_file_threshold_bytes=16, file_chunk_bytes=16, pack_buckets=2)


def objects(release):
    return {item["sha256"] for item in release.manifest["archive"]["objects"]}


def test_changed_large_file_only_installs_new_extents_and_each_version_restores(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "market.parquet"
    initial = b"a" * 16 + b"b" * 16 + b"c" * 16
    path.write_bytes(initial)
    (source / "receipt.json").write_text("{}\n")
    cold = tmp_path / "cold"
    first = publish(cold, source)
    assert first.manifest["schema_version"] == 2
    assert len(objects(first)) == 4
    assert publish(cold, source).manifest_sha256 == first.manifest_sha256
    path.write_bytes(b"a" * 16 + b"x" * 16 + b"c" * 16 + b"d" * 7)
    second = publish(cold, source)
    assert len(objects(second) - objects(first)) == 2
    assert second.manifest["archive"]["new_stored_bytes"] == (
        23 + second.manifest["archive"]["inventory"]["bytes"])
    for release, expected in ((first, initial), (second, path.read_bytes())):
        fixed = resolve_packed_snapshot_id(cold, "provider", release.manifest["snapshot_id"])
        proof = verify_packed_snapshot(cold, fixed, reconstruct_paths=["market.parquet", "receipt.json"])
        assert proof["independently_reconstructed_files"] == 2
        restored = fetch_packed_snapshot(cold, tmp_path / "hot", fixed)
        assert (restored / "market.parquet").read_bytes() == expected


def test_chunked_baseline_and_update_do_not_pre_read_entire_payload(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "large.bin"
    path.write_bytes(b"a" * 32 + b"b" * 32)
    monkeypatch.setattr(packed, "_hash_source_entry", lambda *a, **kw: pytest.fail("redundant full source pass"))
    first = publish(tmp_path / "cold", source)
    assert first.manifest["archive"]["changed_files"] == 1
    path.write_bytes(b"a" * 32 + b"c" * 32)
    second = publish(tmp_path / "cold", source)
    assert second.manifest["archive"]["changed_files"] == 1
    assert publish(tmp_path / "cold", source).manifest_sha256 == second.manifest_sha256
    proof = verify_packed_snapshot(tmp_path / "cold", second, reconstruct_paths=["large.bin"])
    assert proof["independently_reconstructed_files"] == 1


def test_recovery_progress_separates_objects_from_complete_original_files(tmp_path):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'large.bin').write_bytes(b'a' * 16 + b'b' * 16 + b'c' * 16)
    (source / 'small.json').write_bytes(b'{}')
    cold = tmp_path / 'cold'
    release = publish(cold, source)
    seen = []
    result = verify_packed_snapshot(cold, release,
        reconstruct_paths=['large.bin', 'small.json'], progress=seen.append)
    objects = [row for row in seen if row['stage'] == 'verify-cold-objects']
    originals = [row for row in seen if row['stage'] == 'reconstruct-original-files']
    assert objects[-1]['completed_items'] == objects[-1]['total_items']
    assert originals[0]['completed_items'] == 1
    assert originals[0]['completed_bytes'] == 2
    assert len(originals) >= 4
    assert originals[-1]['completed_items'] == originals[-1]['total_items'] == 2
    assert originals[-1]['completed_bytes'] == originals[-1]['total_bytes'] == 50
    assert result['independently_reconstructed_files'] == 2


def test_repeated_extents_multiple_providers_and_deleted_files_share_only_bytes(tmp_path):
    cold = tmp_path / "cold"
    for provider in ("finlab", "finmind", "tej", "weather"):
        source = tmp_path / provider
        source.mkdir()
        (source / "same.bin").write_bytes(b"a" * 64)
        (source / "another.bin").write_bytes(b"a" * 32)
        result = publish(cold, source, provider)
        assert len(result.manifest["archive"]["objects"]) == 1
        item = result.manifest["archive"]["objects"][0]
        assert item["file_count"] == 2 and item["logical_bytes"] == 96
        assert verify_packed_snapshot(cold, result, reconstruct_paths=["same.bin", "another.bin"])[
            "independently_reconstructed_files"] == 2
        restored = fetch_packed_snapshot(cold, tmp_path / "restored", result)
        assert (restored / "same.bin").read_bytes() == b"a" * 64
    assert len(list((cold / "objects" / "blobs").rglob("*.blob"))) == 1
    (source / "same.bin").unlink()
    deleted = publish(cold, source, "weather")
    hot = fetch_packed_snapshot(cold, tmp_path / "restored", deleted)
    assert not (hot / "same.bin").exists() and (hot / "another.bin").is_file()
    assert len(list((cold / "objects" / "blobs").rglob("*.blob"))) == 1


def test_chunk_versions_support_subtree_and_bounded_flat_staging(tmp_path):
    source = tmp_path / "source"
    (source / "partition").mkdir(parents=True)
    payload = b"whole exact file" * 15
    (source / "partition" / "minute.parquet").write_bytes(payload)
    (source / "root.parquet").write_bytes(payload)
    cold = tmp_path / "cold"
    result = publish(cold, source)
    subtree = fetch_packed_subtree(cold, tmp_path / "sub", result, subtree="partition")
    assert (subtree / "minute.parquet").read_bytes() == payload
    destination = tmp_path / "flat"
    destination.mkdir(mode=0o700)
    flat_release = publish_packed_snapshot(cold, "flat-provider", source, node_id="test",
        loose_file_threshold_bytes=16, file_chunk_bytes=16, included_root_files=["root.parquet"])
    stage = stage_packed_root_files(cold, destination, flat_release, expected_names=["root.parquet"],
                                    maximum_bytes=4096)
    assert stage["files"]["root.parquet"].read_bytes() == payload


def test_v1_to_v2_is_lazy_and_legacy_restore_stays_supported(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    original = b"a" * 80
    path = source / "old.bin"
    path.write_bytes(original)
    cold = tmp_path / "cold"
    first = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert first.manifest["schema_version"] == 1
    enrolled = publish(cold, source)
    assert enrolled.manifest["schema_version"] == 2
    assert enrolled.manifest_sha256 != first.manifest_sha256
    assert objects(enrolled) == objects(first)
    assert enrolled.manifest["archive"]["new_stored_bytes"] == 0
    assert enrolled.manifest["archive"]["new_object_count"] == 0
    assert enrolled.manifest["archive"]["inventory"] == first.manifest["archive"]["inventory"]
    assert publish(cold, source).manifest_sha256 == enrolled.manifest_sha256
    (source / "new.bin").write_bytes(b"z" * 80)
    # The native scheduler does not need the operator's one-time CLI flag.
    second = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    rows = {row["path"]: row for row in _load_inventory(cold, second.manifest)}
    assert rows["old.bin"]["storage"]["kind"] == "blob"
    assert rows["new.bin"]["storage"]["kind"] == "chunks"
    assert objects(first).issubset(objects(second))
    assert (fetch_packed_snapshot(cold, tmp_path / "hot", first) / "old.bin").read_bytes() == original


def test_scheduled_publication_inherits_v2_without_requiring_a_new_flag_every_run(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    path.write_bytes(path.read_bytes() + b"c" * 7)
    second = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert second.manifest["schema_version"] == 2
    assert second.manifest["archive"]["file_chunk_bytes"] == 16
    assert len(objects(second) - objects(first)) == 1
    assert verify_packed_snapshot(cold, second)["chunked_files_verified"] == 1


def test_v2_policy_survives_empty_and_small_only_versions(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"small")
    cold = tmp_path / "cold"
    small = publish(cold, source)
    assert small.manifest["schema_version"] == 2
    path.unlink()
    empty = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert empty.manifest["schema_version"] == 2 and empty.manifest["archive"]["objects"] == []
    path.write_bytes(b"a" * 32)
    large = publish_packed_snapshot(cold, "provider", source, node_id="test", loose_file_threshold_bytes=16)
    assert _load_inventory(cold, large.manifest)[0]["storage"]["kind"] == "chunks"
    assert large.manifest["archive"]["file_chunk_bytes"] == 16
    verify_packed_snapshot(cold, small)
    verify_packed_snapshot(cold, empty)
    verify_packed_snapshot(cold, large)


@pytest.mark.parametrize("damage", ["unknown", "extent_size", "chunk_hash", "kind", "truncated", "grown"])
def test_chunk_corruption_never_promotes_materialized_tree(tmp_path, damage):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.bin").write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    release = publish(cold, source)
    rows = _load_inventory(cold, release.manifest)
    row = rows[0]
    if damage == "unknown":
        row["storage"]["chunks"][0]["object_sha256"] = "f" * 64
    elif damage == "extent_size":
        row["storage"]["chunks"][0]["bytes"] -= 1
    elif damage == "kind":
        release.manifest["archive"]["objects"][0]["kind"] = "pack"
    else:
        item = release.manifest["archive"]["objects"][0]
        path = cold / item["relpath"]
        payload = path.read_bytes()
        path.write_bytes(payload[:-1] if damage == "truncated" else payload + b"!" if damage == "grown"
                         else b"!" * item["bytes"])
        with pytest.raises(SnapshotError):
            fetch_packed_snapshot(cold, tmp_path / "hot", release)
        assert not list((tmp_path / "hot").rglob("*.READY.json"))
        return
    with pytest.raises(SnapshotError):
        _validate_inventory(release.manifest, rows)


@pytest.mark.parametrize("bad", [True, 0, -1, 1.5, 64 * 1024**2 + 1])
def test_chunk_bounds_fail_before_creating_store(tmp_path, bad):
    source = tmp_path / "source"
    source.mkdir()
    cold = tmp_path / "cold"
    with pytest.raises(SnapshotError, match="file_chunk_bytes"):
        publish_packed_snapshot(cold, "provider", source, file_chunk_bytes=bad)
    assert not cold.exists()


@pytest.mark.parametrize("damage", ["full_hash", "order"])
def test_default_cold_verifier_checks_complete_chunk_order_and_file_hash(tmp_path, monkeypatch, damage):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.bin").write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    release = publish(cold, source)
    load = packed._iter_inventory

    def damaged(*args):
        for row in load(*args):
            if damage == "order":
                row["storage"]["chunks"].reverse()
            else:
                row["sha256"] = "0" * 64
            yield row

    monkeypatch.setattr(packed, "_iter_inventory", damaged)
    with pytest.raises(SnapshotError, match="exact cold chunk reconstruction"):
        verify_packed_snapshot(cold, release)


def test_chunk_install_failure_keeps_head_and_retry_reuses_only_complete_objects(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 16 + b"b" * 16)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    head_before = first.head_path.read_bytes()
    path.write_bytes(path.read_bytes() + b"x" * 16 + b"y" * 16)
    install = packed._install_immutable_object
    calls = 0

    def failed_install(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected storage interruption")
        return install(*args, **kwargs)

    monkeypatch.setattr(packed, "_install_immutable_object", failed_install)
    with pytest.raises(OSError, match="storage interruption"):
        publish(cold, source)
    assert first.head_path.read_bytes() == head_before
    monkeypatch.setattr(packed, "_install_immutable_object", install)
    second = publish(cold, source)
    assert (fetch_packed_snapshot(cold, tmp_path / "hot", second) / "data.bin").read_bytes() == path.read_bytes()
    assert verify_packed_snapshot(cold, first)["chunked_files_verified"] == 1


def test_source_change_during_chunking_cannot_advance_head(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    path = source / "data.bin"
    path.write_bytes(b"a" * 32)
    cold = tmp_path / "cold"
    first = publish(cold, source)
    head_before = first.head_path.read_bytes()
    path.write_bytes(b"b" * 32)
    store = packed._store_chunked_source

    def raced(*args, **kwargs):
        store(*args, **kwargs)
        path.write_bytes(b"c" * 32)

    monkeypatch.setattr(packed, "_store_chunked_source", raced)
    with pytest.raises(SnapshotError, match="changed"):
        publish(cold, source)
    assert first.head_path.read_bytes() == head_before
