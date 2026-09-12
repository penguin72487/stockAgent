import json

from scripts import repair_packed_source_objects as repair
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot, verify_packed_snapshot


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
