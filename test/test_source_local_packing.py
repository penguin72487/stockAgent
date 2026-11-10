from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from scripts import publish_data_releases
from stockagent.data_sync import packed_metadata, packed_snapshots
from stockagent.data_sync.desync_snapshots import SnapshotError


def _members(root: Path, snapshot) -> dict[str, list[str]]:
    result = {}
    for obj in snapshot.manifest["archive"]["objects"]:
        if obj["kind"] == "pack":
            with zipfile.ZipFile(root / obj["relpath"]) as archive:
                result[obj["sha256"]] = archive.namelist()
    return result


@pytest.mark.parametrize("spill", [False, True])
def test_source_local_packs_restore_exactly_and_incrementally(tmp_path, monkeypatch, spill):
    source, cold = tmp_path / "source", tmp_path / "cold"
    for name, content in {
        "alpha/one.txt": b"alpha-one",
        "alpha/nested/two.txt": b"alpha-two",
        "beta/one.txt": b"beta-one!",
        "beta/nested/two.txt": b"beta-two!",
        "root.txt": b"root-data",
    }.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    if spill:
        monkeypatch.setattr(packed_snapshots, "METADATA_MEMORY_ROWS", 2)
        monkeypatch.setattr(packed_metadata, "_metadata_scratch_location", lambda: (tmp_path, 0, False))

    first = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", pack_buckets=1,
        loose_file_threshold_bytes=1024, pack_layout="source-directory-v1", pack_target_bytes=15,
    )
    assert first.manifest["archive"]["new_pack_layout"] == "source-directory-v1"
    packs = _members(cold, first)
    assert len(packs) == 5  # the logical-size limit separates 9-byte members
    for members in packs.values():
        assert len({packed_snapshots._source_pack_directory(name) for name in members}) == 1
        assert sum((source / name).stat().st_size for name in members) <= 15
    restored = packed_snapshots.fetch_packed_snapshot(cold, tmp_path / "restore-first", first)
    assert all((restored / name).read_bytes() == (source / name).read_bytes()
               for members in packs.values() for name in members)
    assert packed_snapshots.verify_packed_snapshot(cold, first, materialized_path=restored)["materialized_verified"]

    (source / "alpha/one.txt").write_bytes(b"changed-a")
    (source / "beta/one.txt").write_bytes(b"changed-b")
    second = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", pack_buckets=1, loose_file_threshold_bytes=1024,
    )
    assert second.manifest["archive"]["new_pack_layout"] == "source-directory-v1"
    assert second.manifest["archive"]["new_pack_target_bytes"] == 15
    assert second.manifest["archive"]["reused_files"] == 3
    new_packs = _members(cold, second).keys() - packs.keys()
    assert len(new_packs) == 2
    assert all(len({packed_snapshots._source_pack_directory(name) for name in _members(cold, second)[key]}) == 1
               for key in new_packs)
    restored_second = packed_snapshots.fetch_packed_snapshot(cold, tmp_path / "restore-second", second)
    assert (restored_second / "alpha/one.txt").read_bytes() == b"changed-a"
    assert (restored / "alpha/one.txt").read_bytes() == b"alpha-one"
    assert packed_snapshots.verify_packed_snapshot(cold, second, materialized_path=restored_second)["materialized_verified"]
    assert packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", pack_buckets=1, loose_file_threshold_bytes=1024,
    ).manifest["snapshot_id"] == second.manifest["snapshot_id"]


def test_layout_transition_reuses_unchanged_legacy_objects(tmp_path):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "one.txt").write_bytes(b"original")
    legacy = packed_snapshots.publish_packed_snapshot(cold, "fixture", source, node_id="test")
    adopted = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", pack_layout="source-directory-v1", pack_target_bytes=20,
    )
    assert adopted.manifest["snapshot_id"] != legacy.manifest["snapshot_id"]
    assert adopted.manifest["archive"]["reused_files"] == 1
    assert adopted.manifest["archive"]["new_object_count"] == 0
    assert set(_members(cold, legacy)) == set(_members(cold, adopted))
    assert packed_snapshots.verify_packed_snapshot(cold, legacy, reconstruct_all=True)["independently_reconstructed_files"] == 1
    assert packed_snapshots.verify_packed_snapshot(cold, adopted, reconstruct_all=True)["independently_reconstructed_files"] == 1


@pytest.mark.parametrize('spill', [False, True])
def test_exact_repack_changes_physical_layout_but_preserves_both_releases(tmp_path, monkeypatch, spill):
    source, cold = tmp_path/'source', tmp_path/'cold'
    for name, data in {'one/a': b'one', 'two/b': b'two'}.items():
        path = source/name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    if spill:
        monkeypatch.setattr(packed_snapshots, 'METADATA_MEMORY_ROWS', 2)
        monkeypatch.setattr(packed_metadata, '_metadata_scratch_location', lambda: (tmp_path, 0, False))
    legacy = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        pack_buckets=8, loose_file_threshold_bytes=2)
    reports = []
    repacked = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        pack_layout='source-dataset-v1', pack_target_bytes=1024, loose_file_threshold_bytes=1024,
        repack_exact_manifest_sha256=legacy.manifest_sha256, progress=reports.append)
    assert repacked.manifest_sha256 != legacy.manifest_sha256
    assert repacked.manifest['source']['snapshot_root_name'] == legacy.manifest['source']['snapshot_root_name']
    assert len(_members(cold, repacked)) == 1 and repacked.manifest['archive']['reused_files'] == 0
    assert any(r.get('files_completed') == 2 and r.get('logical_bytes_processed') == 6 for r in reports)
    assert packed_snapshots.verify_packed_snapshot(cold, legacy, reconstruct_all=True)['independently_reconstructed_files'] == 2
    assert packed_snapshots.verify_packed_snapshot(cold, repacked, reconstruct_all=True)['independently_reconstructed_files'] == 2


def test_exact_repack_rejects_changed_source_or_head_without_advancing(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'one').write_bytes(b'original')
    legacy = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    (source/'one').write_bytes(b'different')
    with pytest.raises(SnapshotError, match='source file changed while packing|source content or metadata differs'):
        packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            pack_layout='source-dataset-v1', repack_exact_manifest_sha256=legacy.manifest_sha256)
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == legacy.manifest_sha256


def test_exact_repack_rejects_same_length_drift_before_installing_pack(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'one').write_bytes(b'original')
    legacy = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    objects = {path.relative_to(cold) for path in (cold/'objects').rglob('*') if path.is_file()}
    (source/'one').write_bytes(b'changed!')
    with pytest.raises(SnapshotError, match='source file changed while packing'):
        packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            pack_layout='source-dataset-v1', repack_exact_manifest_sha256=legacy.manifest_sha256)
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == legacy.manifest_sha256
    assert {path.relative_to(cold) for path in (cold/'objects').rglob('*') if path.is_file()} == objects
    with pytest.raises(SnapshotError, match='original head changed'):
        packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
            repack_exact_manifest_sha256='a'*64)
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == legacy.manifest_sha256


def test_exact_repack_can_recreate_missing_old_payload_from_exact_source(tmp_path):
    source, cold = tmp_path/'source', tmp_path/'cold'
    source.mkdir()
    (source/'one').write_bytes(b'original')
    legacy = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test')
    for item in legacy.manifest['archive']['objects']:
        (cold/item['relpath']).unlink()
    repacked = packed_snapshots.publish_packed_snapshot(cold, 'fixture', source, node_id='test',
        pack_layout='source-dataset-v1', pack_target_bytes=1024,
        repack_exact_manifest_sha256=legacy.manifest_sha256)
    assert repacked.manifest_sha256 != legacy.manifest_sha256
    assert packed_snapshots.resolve_latest_packed(cold, 'fixture').manifest_sha256 == repacked.manifest_sha256


@pytest.mark.parametrize("spill", [False, True])
def test_dataset_layout_keeps_source_tree_together_and_daily_delta_bounded(tmp_path, monkeypatch, spill):
    source, cold = tmp_path / "source", tmp_path / "cold"
    for name, data in {"alpha/one": b"alpha", "beta/two": b"beta", "root.txt": b"root"}.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    if spill:
        monkeypatch.setattr(packed_snapshots, "METADATA_MEMORY_ROWS", 1)
        monkeypatch.setattr(packed_metadata, "_metadata_scratch_location", lambda: (tmp_path, 0, False))
    first = packed_snapshots.publish_packed_snapshot(cold, "source", source, node_id="test",
        pack_buckets=64, loose_file_threshold_bytes=1024, pack_layout="source-dataset-v1", pack_target_bytes=100)
    old_packs = _members(cold, first)
    assert len(old_packs) == 1
    assert set(next(iter(old_packs.values()))) == {"alpha/one", "beta/two", "root.txt"}
    (source / "alpha/one").write_bytes(b"new-alpha")
    (source / "beta/two").write_bytes(b"new-beta")
    second = packed_snapshots.publish_packed_snapshot(cold, "source", source, node_id="test",
        pack_buckets=64, loose_file_threshold_bytes=1024)
    new_packs = _members(cold, second)
    delta, = new_packs.keys() - old_packs.keys()
    assert set(new_packs[delta]) == {"alpha/one", "beta/two"}
    assert second.manifest["archive"]["new_pack_layout"] == "source-dataset-v1"
    assert second.manifest["archive"]["reused_files"] == 1
    restored = packed_snapshots.fetch_packed_snapshot(cold, tmp_path / "restore-dataset", second)
    assert (restored / "alpha/one").read_bytes() == b"new-alpha"
    assert (restored / "beta/two").read_bytes() == b"new-beta"
    assert (restored / "root.txt").read_bytes() == b"root"
    assert packed_snapshots.verify_packed_snapshot(cold, first, reconstruct_all=True)["independently_reconstructed_files"] == 3
    assert packed_snapshots.verify_packed_snapshot(cold, second, reconstruct_all=True)["independently_reconstructed_files"] == 3


def test_dataset_pack_target_splits_between_files_never_inside_a_file(tmp_path):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    for name in ("a", "b", "c"):
        (source / name).write_bytes(name.encode() * 8)
    release = packed_snapshots.publish_packed_snapshot(cold, "source", source, node_id="test",
        pack_layout="source-dataset-v1", pack_target_bytes=10, loose_file_threshold_bytes=100)
    packs = _members(cold, release)
    assert len(packs) == 3 and all(len(members) == 1 for members in packs.values())
    assert packed_snapshots.verify_packed_snapshot(cold, release, reconstruct_all=True)["independently_reconstructed_files"] == 3


def test_catalog_publishes_and_reads_the_selected_new_layout(tmp_path, monkeypatch, capsys):
    source, cold = tmp_path / "source", tmp_path / "cold"
    for name, payload in {"alpha/a": b"alpha", "beta/b": b"beta", "big.bin": b"x" * 48}.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    monkeypatch.setattr(publish_data_releases, "REPO_ROOT", tmp_path)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{
        "dataset": "fixture", "source": str(source), "role": "test", "publish": True,
        "note": "bounded layout fixture", "pack_buckets": 1, "loose_threshold_bytes": 16,
        "reader_contract": "packed-v2-chunks", "file_chunk_bytes": 16,
        "pack_layout": "source-directory-v1", "pack_target_bytes": 15,
        "large_file_layout": "whole-file-v1", "source_coordination_lock": "source/.owner.lock",
    }]}))
    assert publish_data_releases.main(["publish", "fixture", "--catalog", str(catalog),
        "--sync-root", str(cold), "--node-id", "test"]) == 0
    published = json.loads(capsys.readouterr().out)["published"][0]
    resolved = packed_snapshots.resolve_packed_snapshot_id(cold, "fixture", published["snapshot_id"])
    rows = {row["path"]: row for row in packed_snapshots._iter_inventory(cold, resolved.manifest)}
    assert resolved.manifest["archive"]["new_pack_layout"] == "source-directory-v1"
    assert rows["big.bin"]["storage"]["kind"] == "blob"
    assert all(len({packed_snapshots._source_pack_directory(name) for name in members}) == 1
               for members in _members(cold, resolved).values())
    assert packed_snapshots.verify_packed_snapshot(cold, resolved, reconstruct_all=True)["independently_reconstructed_files"] >= 3


def test_v2_whole_file_policy_keeps_old_chunks_and_new_files_intact(tmp_path):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "old.bin").write_bytes(b"a" * 16 + b"b" * 16 + b"c" * 16)
    first = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", loose_file_threshold_bytes=16, file_chunk_bytes=16,
    )
    first_rows = {row["path"]: row for row in packed_snapshots._iter_inventory(cold, first.manifest)}
    assert first_rows["old.bin"]["storage"]["kind"] == "chunks"
    adopted = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", loose_file_threshold_bytes=16,
        large_file_layout="whole-file-v1",
    )
    assert adopted.manifest["archive"]["new_large_file_layout"] == "whole-file-v1"
    adopted_rows = {row["path"]: row for row in packed_snapshots._iter_inventory(cold, adopted.manifest)}
    assert adopted_rows["old.bin"]["storage"] == first_rows["old.bin"]["storage"]
    (source / "new.bin").write_bytes(b"x" * 16 + b"y" * 16 + b"z" * 16)
    second = packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", loose_file_threshold_bytes=16,
    )
    second_rows = {row["path"]: row for row in packed_snapshots._iter_inventory(cold, second.manifest)}
    assert second_rows["old.bin"]["storage"]["kind"] == "chunks"
    assert second_rows["new.bin"]["storage"]["kind"] == "blob"
    assert second.manifest["archive"]["new_large_file_layout"] == "whole-file-v1"
    restored = packed_snapshots.fetch_packed_snapshot(cold, tmp_path / "restore-whole", second)
    assert (restored / "old.bin").read_bytes() == (source / "old.bin").read_bytes()
    assert (restored / "new.bin").read_bytes() == (source / "new.bin").read_bytes()
    assert packed_snapshots.verify_packed_snapshot(cold, second, reconstruct_all=True)["independently_reconstructed_files"] == 2
    assert packed_snapshots.publish_packed_snapshot(
        cold, "fixture", source, node_id="test", loose_file_threshold_bytes=16,
    ).manifest["snapshot_id"] == second.manifest["snapshot_id"]


@pytest.mark.parametrize("options", [
    {"pack_layout": "unknown"},
    {"pack_layout": []},
    {"pack_target_bytes": 1},
    {"pack_layout": "path-hash-v1", "pack_target_bytes": 1},
    {"pack_layout": "source-directory-v1", "pack_target_bytes": True},
    {"pack_layout": "source-directory-v1", "pack_target_bytes": 0},
    {"pack_layout": "source-directory-v1", "pack_target_bytes": 8 * 1024**3 + 1},
    {"large_file_layout": []},
    {"large_file_layout": "unknown"},
])
def test_bad_source_layout_is_rejected_before_publication(tmp_path, options):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    with pytest.raises(SnapshotError):
        packed_snapshots.publish_packed_snapshot(cold, "fixture", source, **options)
    assert not cold.exists()


@pytest.mark.parametrize("options", [
    {"pack_layout": "unknown"},
    {"pack_layout": []},
    {"pack_target_bytes": 10},
    {"pack_layout": "path-hash-v1", "pack_target_bytes": 10},
    {"pack_layout": "source-directory-v1", "pack_target_bytes": True},
    {"pack_layout": "source-directory-v1", "pack_target_bytes": None},
    {"large_file_layout": []},
    {"large_file_layout": "unknown"},
])
def test_catalog_rejects_ambiguous_source_layout(tmp_path, options):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{"dataset": "fixture", **options}]}))
    with pytest.raises(SnapshotError):
        publish_data_releases._load_catalog(catalog)
