"""A user's missing-history waiver cannot hide current damage or change data."""
from copy import deepcopy
import hashlib
import json

import pytest

from stockagent.data_sync.backup_history import apply_disposition, build_disposition
from stockagent.data_sync.backup_stream import capture_catalog, coverage, empty_ledger
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.offhost_backup import export_incremental_delivery
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
from stockagent.runtime_identity import identity_sha256


@pytest.fixture
def history(tmp_path):
    cold, source = tmp_path / "cold", tmp_path / "source"
    initialize_packed_layout(cold, node_id="penguin")
    source.mkdir()
    (source / "prices.txt").write_text("first\n")
    old = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024)
    (source / "prices.txt").write_text("second\n")
    current = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024)
    missing = old.manifest["archive"]["objects"][0]["relpath"]
    (cold / missing).unlink()
    catalog_config = tmp_path / "catalog.json"
    catalog_config.write_text(json.dumps({"datasets": [{"dataset": "prices", "publish": True}]}))
    catalog = capture_catalog(cold, catalog_config)
    decision = build_disposition(catalog, authorization="user explicitly abandons unavailable history",
                                 evidence=[{"name": "exact-recovery.json", "sha256": "a" * 64}])
    path = tmp_path / "disposition.json"
    path.write_text(json.dumps(decision))
    return cold, catalog_config, catalog, decision, path, old, current


def resign(value):
    return {**value, "identity_sha256": identity_sha256({k: v for k, v in value.items() if k != "identity_sha256"})}


def test_missing_history_is_terminal_without_deleting_existing_metadata_or_bytes(history):
    cold, config, before, _, path, old, current = history
    raw = old.manifest_path.read_bytes()
    after = capture_catalog(cold, config, path)
    assert [r["snapshot_id"] for r in after["releases"]] == [current.manifest["snapshot_id"]]
    assert after["abandoned_releases"][0]["snapshot_id"] == old.manifest["snapshot_id"]
    assert after["missing_objects"] == []
    assert len(after["abandoned_missing_objects"]) == 1
    assert after["files"] == before["files"]
    assert old.manifest_path.read_bytes() == raw
    state = coverage(after, empty_ledger())
    assert state["observed_release_count"] == 2
    assert state["retained_release_count"] == 1
    assert state["abandoned_history_release_count"] == 1
    assert state["all_history_backup_verified"] is False


def test_no_disposition_keeps_historical_failure_visible(history):
    cold, config, _, _, _, old, _ = history
    result = capture_catalog(cold, config)
    assert len(result["missing_objects"]) == 1
    assert old.manifest["snapshot_id"] in {r["snapshot_id"] for r in result["releases"]}


def test_a_waiver_never_waives_current_missing_bytes(history):
    cold, config, _, _, path, _, current = history
    ref = current.manifest["archive"]["objects"][0]
    (cold / ref["relpath"]).unlink()
    result = capture_catalog(cold, config, path)
    assert result["missing_objects"][0]["relative"] == ref["relpath"]
    assert result["releases"][0]["current_head"] is True
    with pytest.raises(SnapshotError, match="non-current"):
        build_disposition(result, authorization="discard missing history", evidence=[{"sha256": "a" * 64}])


@pytest.mark.parametrize("mutation", ["head", "manifest", "object", "duplicate", "extra_object"])
def test_exact_waiver_identity_and_current_head_are_rechecked(history, mutation):
    _, _, catalog, decision, path, _, _ = history
    body = deepcopy(catalog)
    if mutation == "head":
        body["releases"][0]["current_head"] = True
    elif mutation == "manifest":
        body["releases"][0]["manifest_sha256"] = "b" * 64
    elif mutation == "object":
        decision["unavailable_objects"][0]["sha256"] = "b" * 64
    elif mutation == "duplicate":
        decision["releases"].append(deepcopy(decision["releases"][0]))
    else:
        decision["unavailable_objects"].append({"relative": "objects/blobs/ab/" + "ab" * 32 + ".blob", "sha256": "ab" * 32})
    path.write_text(json.dumps(resign(decision)))
    with pytest.raises(SnapshotError):
        apply_disposition(body, path)


def test_corrupt_disposition_hash_is_rejected(history):
    _, _, catalog, decision, path, _, _ = history
    decision["authorization"] = "changed"
    path.write_text(json.dumps(decision))
    with pytest.raises(SnapshotError, match="invalid explicit"):
        apply_disposition(catalog, path)


def test_alternate_reader_cannot_substitute_different_bytes(history, tmp_path):
    cold, _, catalog, _, _, _, current = history
    row = next(r for r in catalog["files"] if r["relative"] == current.manifest["archive"]["objects"][0]["relpath"])
    alias = tmp_path / "alias"
    file = alias / row["relative"]
    file.parent.mkdir(parents=True)
    file.write_bytes(b"different bytes")
    with pytest.raises(SnapshotError, match="bytes differ"):
        export_incremental_delivery(cold, [row], tmp_path / "delivery", catalog_identity=catalog["identity_sha256"], read_root=alias)
    assert not (tmp_path / "delivery" / "READY").exists()


def test_exact_alternate_reader_retains_canonical_signature_checks(history, tmp_path):
    cold, _, catalog, _, _, _, current = history
    row = next(r for r in catalog["files"] if r["relative"] == current.manifest["archive"]["objects"][0]["relpath"])
    alias = tmp_path / "alias"
    file = alias / row["relative"]
    file.parent.mkdir(parents=True)
    file.write_bytes((cold / row["relative"]).read_bytes())
    assert hashlib.sha256(file.read_bytes()).hexdigest() == row["sha256"]
    result = export_incremental_delivery(cold, [row], tmp_path / "delivery", catalog_identity=catalog["identity_sha256"], read_root=alias)
    assert result["alternate_read_root_used"] is True
    assert result["transport"]["files_verified"] == 1
    assert (tmp_path / "delivery" / "READY").exists()


def test_historical_head_bytes_are_catalogued_and_delivered_as_provenance(history, tmp_path):
    cold, config, _, _, _, old, _ = history
    body = {"schema_version": 2, "dataset": old.manifest["dataset"],
            "node_id": old.manifest["publisher"]["node_id"], "snapshot_id": old.manifest["snapshot_id"],
            "hlc": old.manifest["hlc"], "manifest_relpath": old.manifest_path.relative_to(cold).as_posix(),
            "manifest_sha256": old.manifest_sha256}
    from stockagent.data_sync.packed_snapshots import PACKED_HEAD_SCHEMA_VERSION
    body["schema_version"] = PACKED_HEAD_SCHEMA_VERSION
    raw = json.dumps(body).encode()
    path = cold / "head-history/heads/prices/penguin" / (hashlib.sha256(raw).hexdigest() + ".json")
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    catalog = capture_catalog(cold, config)
    row = next(r for r in catalog["files"] if r["relative"].startswith("head-history/"))
    result = export_incremental_delivery(cold, [row], tmp_path / "head-history-delivery", catalog_identity=catalog["identity_sha256"])
    assert result["transport"]["files_verified"] == 1
    assert len(catalog["releases"]) == 2
    assert sum(r["current_head"] for r in catalog["releases"]) == 1


def test_corrupt_historical_head_is_not_accepted_as_provenance(history):
    cold, config, _, _, _, _, _ = history
    path = cold / "head-history/heads/prices/penguin" / ("a" * 64 + ".json")
    path.parent.mkdir(parents=True)
    path.write_text('{}')
    catalog = capture_catalog(cold, config)
    assert any(r["relative"].startswith("head-history/") for r in catalog["metadata_errors"])


def test_captured_heads_remain_original_observations_after_publication_advances(history, tmp_path):
    cold, config, _, _, _, _, second = history
    catalog = capture_catalog(cold, config)
    row = next(r for r in catalog["files"] if r["relative"].startswith("heads/"))
    source = tmp_path / "third-source"
    source.mkdir()
    (source / "prices.txt").write_text("third vintage\n")
    third = publish_packed_snapshot(cold, "prices", source, loose_file_threshold_bytes=1024)
    result = export_incremental_delivery(cold, [row], tmp_path / "captured-head-delivery", catalog_identity=catalog["identity_sha256"])
    copied = json.loads((tmp_path / "captured-head-delivery/cold" / row["relative"]).read_bytes())
    assert copied["snapshot_id"] == second.manifest["snapshot_id"]
    assert third.manifest["snapshot_id"] != copied["snapshot_id"]
    assert result["transport"]["files_verified"] == 1
