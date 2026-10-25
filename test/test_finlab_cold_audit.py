"""FinLab release audit keeps full verification with scoped native D reads."""
import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import audit_finlab_research_cold as cold


@pytest.fixture
def release(tmp_path, monkeypatch):
    sync, staged = tmp_path / "cold", tmp_path / "staged"
    sync.mkdir()
    staged.mkdir()
    receipt = {"status": "complete", "private_personal_research": True,
               "staged_files_sha256": {"features.parquet": "a" * 64},
               "formal_action_members_sha256": {"actions.parquet": "b" * 64},
               "feature_rows": 123, "model_channels": 5, "base_feature_sha256": "c" * 64}
    path = staged / "release_receipt.json"
    path.write_text(json.dumps(receipt))
    inventory = {**receipt["staged_files_sha256"], **receipt["formal_action_members_sha256"],
                 "release_receipt.json": cold.sha256_file(path)}
    with gzip.open(sync / "inventory.jsonl.gz", "wt") as stream:
        for name, digest in inventory.items():
            stream.write(json.dumps({"kind": "file", "path": name, "sha256": digest}) + "\n")
    resolved = SimpleNamespace(manifest={"archive": {"inventory": {"relpath": "inventory.jsonl.gz"}},
                                         "snapshot_id": "base-version"})
    monkeypatch.setattr(cold, "resolve_latest_packed", lambda *_a, **_k: resolved)
    monkeypatch.setattr(cold, "_formal_inventory_hashes",
                        lambda *_a, **_k: {"features/tw_public_stock_daily.parquet": "c" * 64})
    return sync, staged, resolved


@pytest.mark.parametrize("canonical,marker,expected", [
    (True, True, True), (True, False, False), (False, True, False), (False, False, False),
])
def test_native_reader_requires_canonical_root_and_enrollment(release, monkeypatch, canonical, marker, expected):
    sync, staged, resolved = release
    if marker:
        (sync / ".stockagent-d-primary").write_text("enrolled")
    original_resolve = Path.resolve
    monkeypatch.setattr(Path, "resolve", lambda path, *a, **k:
                        Path("/srv/stockagent-packed") if path == sync and canonical
                        else original_resolve(path, *a, **k))
    calls = []
    def verify(root, version, **options):
        assert root == sync and version is resolved
        calls.append(options)
        return {"snapshot_id": "private-version", "manifest_sha256": "d" * 64,
                "inventory_sha256": "e" * 64, "verified_object_bytes": 456}
    monkeypatch.setattr(cold, "verify_packed_snapshot", verify)
    proof = cold.audit(sync, staged)
    assert calls == [{"d_primary_native_blob_reads": expected, "d_primary_native_pack_reads": expected,
                      "d_primary_native_chunk_reads": expected}]
    assert proof["cold_objects_verified"] and proof["base_cold_feature_match"]
    assert proof["verified_object_bytes"] == 456 and proof["feature_rows"] == 123
    assert proof["remote_materialization"] == "not_verified" and proof["historical_point_in_time"] is False


def test_corrupt_cold_objects_never_fall_back_to_a_green_audit(release, monkeypatch):
    sync, staged, _ = release
    calls = []
    def broken(*_a, **_k):
        calls.append(True)
        raise cold.SnapshotError("object checksum mismatch")
    monkeypatch.setattr(cold, "verify_packed_snapshot", broken)
    with pytest.raises(cold.SnapshotError, match="checksum mismatch"):
        cold.audit(sync, staged)
    assert calls == [True]


@pytest.mark.parametrize("member", ["features.parquet", "release_receipt.json"])
def test_staged_hash_mismatch_remains_fatal(release, monkeypatch, member):
    sync, staged, _ = release
    with gzip.open(sync / "inventory.jsonl.gz", "rt") as stream:
        entries = [json.loads(line) for line in stream]
    for row in entries:
        if row["path"] == member:
            row["sha256"] = "f" * 64
    with gzip.open(sync / "inventory.jsonl.gz", "wt") as stream:
        for row in entries:
            stream.write(json.dumps(row) + "\n")
    monkeypatch.setattr(cold, "verify_packed_snapshot", lambda *_a, **_k: {})
    with pytest.raises(RuntimeError, match="differs|missing"):
        cold.audit(sync, staged)
