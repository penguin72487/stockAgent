import json
import hashlib
import shutil
from pathlib import Path

import pytest

from scripts import repair_packed_source_objects as repair
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot, verify_packed_snapshot


@pytest.fixture
def transport_recovery(tmp_path, monkeypatch):
    from stockagent.control import lakehouse
    from stockagent.data_sync import immutable_transport_cache as transport
    from stockagent.data_sync.immutable_replication import ACK, seal
    from stockagent.runtime_identity import identity_sha256

    source, cold, physical = tmp_path / "source", tmp_path / "cold", tmp_path / "transport"
    source.mkdir()
    (source / "raw.bin").write_bytes(b"exact original revision\x00\xff")
    initialize_packed_layout(cold, node_id="penguin")
    release = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=1)
    delivery = physical / "lakehouse" / "initial"
    delivery.mkdir(parents=True)
    state, receipts = tmp_path / "state", tmp_path / "receipts"
    state.mkdir()
    receipts.mkdir()
    config = {"cold_root": str(cold), "state_root": str(state), "receipt_root": str(receipts),
              "transport_root": str(physical / "lakehouse"),
              "producer_device_id": "penguin", "receiver_device_id": "lab203"}
    objects = [*release.manifest["archive"]["objects"], release.manifest["archive"]["inventory"]]
    mapping = {}
    for number, obj in enumerate(objects):
        member = f"objects/{number:03d}.payload"
        (delivery / "objects").mkdir(exist_ok=True)
        shutil.copyfile(cold / obj["relpath"], delivery / member)
        mapping[member] = obj["relpath"]
    context = {"kind": "immutable_source_objects", "source_object_paths": mapping,
               "producer_device_id": "penguin", "receiver_device_id": "lab203"}
    manifest = seal(delivery, context)
    identity = manifest["identity_sha256"]
    delivery = delivery.rename(delivery.with_name("lake-" + identity))
    raw = (delivery / "manifest.json").read_bytes()
    body = {"contract": ACK, "delivery_identity_sha256": identity,
            "producer_device_id": "penguin", "receiver_device_id": "lab203",
            "command_exit_codes": [0, 0], "restore_command_exit_codes": [0, 0],
            "manifest_file_sha256": hashlib.sha256(raw).hexdigest(),
            "complete_files": len(manifest["files"]) + 2,
            "complete_bytes": sum(v["bytes"] for v in manifest["files"].values()) + len(raw) + 65,
            **{key: True for key in ("all_files_sha256_verified", "exact_file_set_verified",
                "source_unchanged_verified", "nas_independent_restore_verified", "nas_mount_guard_verified",
                "single_owner_verified", "runtime_lock_verified")}}
    ack = {**body, "identity_sha256": identity_sha256(body)}
    (receipts / ("lake-" + identity + ".json")).write_text(json.dumps(ack))
    keys = [obj["relpath"] + "@" + obj["sha256"] for obj in objects]
    (state / "source-replication-ledger.json").write_text(json.dumps({"deliveries": {identity: {
        "file_keys": keys, "nas_acceptance": {"state": "nas_archive_file_recovery_verified"}}}}))
    monkeypatch.setattr(lakehouse, "guard", lambda c: None)
    monkeypatch.setattr(transport, "PHYSICAL_TRANSPORT_ROOT", physical)
    for obj in objects:
        (cold / obj["relpath"]).unlink()  # isolated deliberate damage only
    return config, identity, cold, delivery, release, objects, ack


def test_transport_recovery_preserves_original_heads_and_exact_versions(transport_recovery, tmp_path):
    c, identity, cold, delivery, release, objects, _ = transport_recovery
    head = cold / "heads/fixture/penguin.json"
    original = head.read_bytes()
    dry = repair.repair_transport(c, identity, apply=False, receipt=tmp_path / "dry.json")
    assert len(dry["recoverable"]) == 2 and dry["restored"] == []
    result = repair.repair_transport(c, identity, apply=True, receipt=tmp_path / "apply.json")
    assert len(result["restored"]) == 2 and result["restored_bytes"] == sum(o["bytes"] for o in objects)
    assert head.read_bytes() == original and delivery.is_dir()
    assert result["nas_archive_files_deleted"] == result["cold_objects_deleted"] == 0
    verify_packed_snapshot(cold, release)


@pytest.mark.parametrize("failure", ["checksum", "unenrolled", "nas_proof"])
def test_transport_recovery_rejects_unproven_donors(transport_recovery, tmp_path, failure):
    from stockagent.runtime_identity import identity_sha256
    c, identity, cold, delivery, _, objects, ack = transport_recovery
    if failure == "checksum":
        (delivery / "objects/000.payload").write_bytes(b"wrong revision")
    elif failure == "unenrolled":
        state = Path(c["state_root"]) / "source-replication-ledger.json"
        value = json.loads(state.read_text())
        value["deliveries"][identity]["file_keys"] = []
        state.write_text(json.dumps(value))
    else:
        ack["nas_independent_restore_verified"] = False
        body = {k: v for k, v in ack.items() if k != "identity_sha256"}
        ack["identity_sha256"] = identity_sha256(body)
        (Path(c["receipt_root"]) / ("lake-" + identity + ".json")).write_text(json.dumps(ack))
    with pytest.raises(ValueError):
        repair.repair_transport(c, identity, apply=True, receipt=tmp_path / "rejected.json")
    assert all(not (cold / o["relpath"]).exists() for o in objects)
    assert not (tmp_path / "rejected.json").exists()


def test_transport_recovery_never_overwrites_an_existing_bad_object(transport_recovery, tmp_path):
    c, identity, cold, _, _, objects, _ = transport_recovery
    target = cold / objects[0]["relpath"]
    target.write_bytes(b"existing corrupt evidence")
    result = repair.repair_transport(c, identity, apply=True, receipt=tmp_path / "partial.json")
    assert len(result["restored"]) == 1
    assert target.read_bytes() == b"existing corrupt evidence"
    assert result["existing_objects_verified"] is False


def test_transport_recovery_retains_changed_copy_and_does_not_install(transport_recovery, tmp_path, monkeypatch):
    c, identity, cold, delivery, _, objects, _ = transport_recovery
    original_copy = repair._copy_and_hash
    def changed(source, destination):
        digest = original_copy(source, destination)
        source.write_bytes(b"changed after copy")
        return digest
    monkeypatch.setattr(repair, "_copy_and_hash", changed)
    with pytest.raises(ValueError, match="reconstruction changed"):
        repair.repair_transport(c, identity, apply=True, receipt=tmp_path / "failed.json")
    assert list((cold / ".local-state/staging").glob("transport-repair-*/object.partial"))
    assert all(not (cold / o["relpath"]).exists() for o in objects)


def test_restore_missing_objects_only_if_exact_and_preserve_head(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "x.txt").write_text("original")
    (source / "y.bin").write_bytes(b"unique payload" * 20)
    initialize_packed_layout(cold, node_id="test")
    release = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=100)
    head = cold / "heads/fixture/test.json"
    original_head = head.read_bytes()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True, "source": str(source), "active_process_substrings": []}])
    for obj in release.manifest["archive"]["objects"]:
        (cold / obj["relpath"]).unlink()  # test-only intentional damage
    dry = repair.repair("fixture", cold, apply=False, receipt=tmp_path / "dry.json")
    assert len(dry["recoverable"]) == 2 and not dry["restored"]
    fixed = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "fixed.json")
    assert len(fixed["restored"]) == 2 and not fixed["unresolved"]
    assert head.read_bytes() == original_head
    verify_packed_snapshot(cold, release)
    for obj in release.manifest["archive"]["objects"]:
        (cold / obj["relpath"]).unlink()
    (source / "x.txt").write_text("changed")
    (source / "y.bin").write_bytes(b"changed")
    rejected = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "reject.json")
    assert not rejected["restored"] and len(rejected["unresolved"]) == 2
    assert json.loads(head.read_text())["snapshot_id"] == release.manifest["snapshot_id"]


def test_subset_pack_restored_with_original_inventory(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "a.txt").write_text("unchanged A")
    (source / "b.txt").write_text("original B")
    initialize_packed_layout(cold, node_id="test")
    original = publish_packed_snapshot(cold, "fixture", source, pack_buckets=1)
    # Keep both original files locally, but exclude B from the newer inventory.
    latest = publish_packed_snapshot(cold, "fixture", source, pack_buckets=1,
                                     excluded_subtrees=("b.txt",))
    old_obj = original.manifest["archive"]["objects"][0]
    assert any(o["sha256"] == old_obj["sha256"] and o.get("member_selection") == "subset"
               for o in latest.manifest["archive"]["objects"])
    head = cold / "heads/fixture/test.json"
    before = head.read_bytes()
    (cold / old_obj["relpath"]).unlink()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": []}])
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "receipt.json")
    assert len(result["restored"]) == 1 and not result["unresolved"]
    assert result["restored"][0]["original_member_count"] == 2
    assert head.read_bytes() == before
    verify_packed_snapshot(cold, latest)


def test_fixed_historical_release_is_repaired_without_republishing_latest(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "old.bin").write_bytes(b"retained authentic vintage" * 20)
    (source / "shared.txt").write_text("unchanged source\n")
    initialize_packed_layout(cold, node_id="test")
    old = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=100)
    latest = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=100, excluded_subtrees=("old.bin",))
    old_object = next(o for o in old.manifest["archive"]["objects"] if o["kind"] == "blob")
    target = cold / old_object["relpath"]
    target.unlink()
    head = cold / "heads/fixture/test.json"
    before = head.read_bytes()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": []}])
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "old-recovery.json",
                           snapshot_id=old.manifest["snapshot_id"])
    assert result["snapshot_id"] == old.manifest["snapshot_id"]
    assert len(result["restored"]) == 1 and not result["unresolved"]
    assert head.read_bytes() == before
    verify_packed_snapshot(cold, old)
    verify_packed_snapshot(cold, latest)


def test_source_writer_start_during_reconstruction_prevents_install(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "x.bin").write_bytes(b"exact original" * 30)
    initialize_packed_layout(cold, node_id="test")
    release = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=100)
    target = cold / release.manifest["archive"]["objects"][0]["relpath"]
    target.unlink()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": ["synthetic-source-writer"]}])
    observations = iter(([], [(999999, "synthetic-source-writer")]))
    monkeypatch.setattr(repair, "_running_commands", lambda: next(observations))
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "blocked.json")
    assert not result["restored"]
    assert result["unresolved"][0]["reason"] == "source_writer_started_before_install"
    assert not target.exists()


def test_chunked_source_recovery_reconstructs_exact_extent_not_full_file(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "x.bin").write_bytes(b"a" * 64 + b"b" * 64 + b"a" * 64 + b"end")
    (source / "small.txt").write_text("small source")
    initialize_packed_layout(cold, node_id="test")
    release = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=16,
                                      file_chunk_bytes=64)
    objects = release.manifest["archive"]["objects"]
    assert len([o for o in objects if o["kind"] == "blob"]) == 3
    head = cold / "heads/fixture/test.json"
    before = head.read_bytes()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": []}])
    for obj in objects:
        (cold / obj["relpath"]).unlink()
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "fixed.json")
    assert len(result["restored"]) == len(objects) and not result["unresolved"]
    assert head.read_bytes() == before
    verify_packed_snapshot(cold, release)


def test_chunked_recovery_does_not_substitute_changed_source(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    path = source / "x.bin"
    path.write_bytes(b"a" * 64 + b"b" * 64)
    initialize_packed_layout(cold, node_id="test")
    release = publish_packed_snapshot(cold, "fixture", source, loose_file_threshold_bytes=16,
                                      file_chunk_bytes=64)
    for obj in release.manifest["archive"]["objects"]:
        (cold / obj["relpath"]).unlink()
    path.write_bytes(b"a" * 64 + b"c" * 64)
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": []}])
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "refused.json")
    assert not result["restored"] and len(result["unresolved"]) == 2
    assert all(row["reason"] == "source_bytes_differ_from_retained_release" for row in result["unresolved"])


def test_subset_pack_recovery_from_inventory_also_containing_chunks(tmp_path, monkeypatch):
    source, cold = tmp_path / "source", tmp_path / "cold"
    source.mkdir()
    (source / "large.bin").write_bytes(b"a" * 64 + b"b" * 64)
    (source / "a.txt").write_text("old A")
    (source / "b.txt").write_text("old B")
    initialize_packed_layout(cold, node_id="test")
    original = publish_packed_snapshot(cold, "fixture", source, pack_buckets=1,
        loose_file_threshold_bytes=16, file_chunk_bytes=64)
    latest = publish_packed_snapshot(cold, "fixture", source, pack_buckets=1,
        loose_file_threshold_bytes=16, file_chunk_bytes=64, excluded_subtrees=("b.txt",))
    pack = next(o for o in original.manifest["archive"]["objects"] if o["kind"] == "pack")
    assert any(o.get("member_selection") == "subset" for o in latest.manifest["archive"]["objects"])
    (cold / pack["relpath"]).unlink()
    monkeypatch.setattr(repair, "_load_catalog", lambda _: [{"dataset": "fixture", "publish": True,
        "source": str(source), "active_process_substrings": []}])
    result = repair.repair("fixture", cold, apply=True, receipt=tmp_path / "fixed-pack.json")
    assert len(result["restored"]) == 1 and not result["unresolved"]
    verify_packed_snapshot(cold, original)
    verify_packed_snapshot(cold, latest)
