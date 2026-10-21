"""Fixed-release readback and monitoring preserve the logical Parquet ABI."""
from datetime import date
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data import partitioned_parquet as parts
from stockagent.data.tw_public_incremental import table_content_sha256
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.parquet_publication import ParquetPublicationSnapshot
from stockagent.live.data_monitor_inventory import (
    build_feature_inventory, build_record_inventory, parquet_footer_stats,
)


@pytest.fixture(autouse=True)
def capacity_controls(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})


def table():
    return pa.table({"date": pa.array([date(2024, 1, 2), date(2025, 1, 2)], type=pa.date32()),
                     "symbol": ["2330", "2330"], "close": [-0.0, None]})


def source(tmp_path):
    root = tmp_path / "data_tw_public"
    path = root / "stocks/2330_features.parquet"
    parts.write_partitioned_table(table(), path, source_identity={"original": "test"})
    return root, path


def test_monitor_reads_one_logical_file_and_all_fields_not_archived_generations(tmp_path):
    root, path = source(tmp_path)
    pointer = parts.manifest_path(path)
    stats = parquet_footer_stats(pointer)
    assert stats["count"] == 2 and stats["non_null"] == [2, 2, 1]
    assert (stats["first"], stats["last"]) == ("2024-01-02", "2025-01-02")
    first = build_record_inventory(tmp_path, refresh=True)
    dataset = first["datasets"]["physical:tw-public:stock-features"]
    assert dataset["count"] == 2 and dataset["files_total"] == 1
    second = build_record_inventory(tmp_path, refresh=True)
    assert second["fast_index_hit"]
    assert second["refreshed_files"] == 0
    features = build_feature_inventory(tmp_path)
    assert any(row["field"] == "close" for row in features["rows"])


def test_legacy_flat_inventory_does_not_probe_nonexistent_manifest_per_file(tmp_path, monkeypatch):
    from pathlib import Path
    from stockagent.live.data_monitor_inventory import _feature_paths
    directory = tmp_path / "stocks"
    directory.mkdir()
    for index in range(30):
        (directory / f"{index}_features.parquet").write_bytes(b"discovery only")
    original = Path.stat
    def no_extra_stat(path, *args, **kwargs):
        assert not str(path).endswith(".manifest.json"), "no missing-manifest stat per legacy file"
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "stat", no_extra_stat)
    assert len(_feature_paths(directory, files_only=True)) == 30


def test_catalog_identity_keeps_partition_guards_after_source_relocation(tmp_path):
    root, path = source(tmp_path)
    relocated = tmp_path / "restored-source-generation"
    root.rename(relocated)
    assert ParquetPublicationSnapshot(relocated, dataset="unrelated-private").metadata == {}
    proof = ParquetPublicationSnapshot(relocated, dataset="tw-public")
    assert proof.metadata["partitioned_logical_sources"] == "1"
    with parts.ParquetSource(relocated / path.relative_to(root)) as snapshot:
        member = snapshot.members[0]
    member.rename(member.with_suffix(".unavailable"))
    with pytest.raises(SnapshotError, match="partitioned source changed"):
        proof.guard()


@pytest.mark.parametrize("change", ["corrupt", "missing", "ambiguous", "replace_same_size"])
def test_manifest_cache_hit_cannot_hide_an_old_member_change(tmp_path, change):
    root, path = source(tmp_path)
    build_record_inventory(tmp_path, refresh=True)
    with parts.ParquetSource(path) as snapshot:
        member = snapshot.members[0]
    before = member.stat()
    if change == "missing":
        member.rename(member.with_suffix(".unavailable"))
    elif change == "ambiguous":
        pq.write_table(table(), path)
    else:
        raw = bytearray(member.read_bytes())
        raw[len(raw) // 2] ^= 1
        member.chmod(0o600)
        member.write_bytes(raw)
        member.chmod(0o444)
        os.utime(member, ns=(before.st_atime_ns, before.st_mtime_ns))
    result = build_record_inventory(tmp_path, refresh=True)
    dataset = result["datasets"]["physical:tw-public:stock-features"]
    assert not result["fast_index_hit"]
    assert dataset["count"] is None and dataset["state"] == "invalid"
    assert not result["source_observation_matches_cache"]


def test_whole_tree_publisher_roundtrips_exact_manifest_and_every_member(tmp_path, monkeypatch, capsys):
    from scripts import publish_data_releases as publisher
    from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, resolve_packed_snapshot_id
    root, path = source(tmp_path)
    monkeypatch.setattr(publisher, "REPO_ROOT", tmp_path)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "datasets": [{
        "dataset": "tw-fixture", "source": str(root), "role": "test public source",
        "publish": True, "note": "offline fixed release fixture", "pack_buckets": 1,
        "loose_threshold_mib": 8, "source_coordination_lock": "source/.lock"}]}))
    cold = tmp_path / "cold"
    assert publisher.main(["publish", "tw-fixture", "--catalog", str(catalog),
                           "--sync-root", str(cold), "--node-id", "test-node"]) == 0
    published = json.loads(capsys.readouterr().out)["published"][0]
    fixed = resolve_packed_snapshot_id(cold, published["dataset"], published["snapshot_id"])
    restored = fetch_packed_snapshot(cold, tmp_path / "materialized", fixed)
    assert table_content_sha256(parts.read_table(restored / path.relative_to(root))) == table_content_sha256(table())
    assert (restored / parts.manifest_path(path).relative_to(root)).read_bytes() == parts.manifest_path(path).read_bytes()
    assert fixed.manifest["metadata"]["partitioned_logical_sources"] == "1"


@pytest.mark.parametrize("failure", ["omitted", "override", "corrupt", "ambiguous", "membership"])
def test_publisher_refuses_broken_or_mixed_logical_relationship(tmp_path, failure):
    root, path = source(tmp_path)
    options = {}
    if failure == "omitted":
        options["excluded"] = ["stocks/2330_features.parquet.manifest.json.parts"]
    elif failure == "override":
        options["file_overrides"] = {"stocks/2330_features.parquet": {"path": "retained-old-file"}}
    elif failure == "corrupt":
        parts.manifest_path(path).chmod(0o600)
        parts.manifest_path(path).write_bytes(b"{}")
    elif failure == "ambiguous":
        pq.write_table(table(), path)
    if failure == "membership":
        snapshot = ParquetPublicationSnapshot(root)
        second = root / "stocks/1101_features.parquet"
        parts.write_partitioned_table(table(), second, source_identity={"original": "second"})
        with pytest.raises(SnapshotError, match="membership"):
            snapshot.guard()
    else:
        with pytest.raises(SnapshotError):
            ParquetPublicationSnapshot(root, **options)


def test_private_preparation_ignores_new_head_not_immutable_members(tmp_path):
    from stockagent.data_sync.dataset_migration import _freeze_controls
    root, path = source(tmp_path)
    frozen = _freeze_controls(root, tmp_path / "private-capture", [])
    assert str(parts.manifest_path(path).relative_to(root)) not in frozen
    proof = ParquetPublicationSnapshot(root, file_overrides=frozen)
    assert len(proof.sources) == 1


@pytest.mark.parametrize("change", ["member", "membership", "format"])
def test_late_logical_change_cannot_replace_last_good_cold_head(tmp_path, change):
    from stockagent.data_sync.packed_snapshots import (
        fetch_packed_snapshot, initialize_packed_layout, publish_packed_snapshot,
        resolve_packed_snapshot_id,
    )
    root, path = source(tmp_path)
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="test-node")
    proof = ParquetPublicationSnapshot(root)
    first = publish_packed_snapshot(cold, "tw-fixture", root, pack_buckets=1,
                                    source_guard=proof.guard)
    head_before = first.head_path.read_bytes()
    calls = 0

    def changed_during_publication():
        nonlocal calls
        calls += 1
        if calls == 2:
            if change == "member":
                member = proof.sources[0].members[0]
                member.chmod(0o600)
                member.write_bytes(b"broken exact source member")
                member.chmod(0o444)
            elif change == "membership":
                parts.write_partitioned_table(table(), root / "stocks/1101_features.parquet",
                                              source_identity={"late": "membership"})
            else:
                pq.write_table(table(), path)
        proof.guard()

    with pytest.raises(SnapshotError):
        publish_packed_snapshot(cold, "tw-fixture", root, pack_buckets=1,
                                source_guard=changed_during_publication)
    assert calls == 2 and first.head_path.read_bytes() == head_before
    # Restore from the fixed accepted release, not the now-invalid live tree.
    fixed = resolve_packed_snapshot_id(cold, "tw-fixture", first.manifest["snapshot_id"])
    restored = fetch_packed_snapshot(cold, tmp_path / "after-failure", fixed)
    assert table_content_sha256(parts.read_table(restored / path.relative_to(root))) == table_content_sha256(table())
