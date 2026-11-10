from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path

import pytest

from scripts import stage_source_local_packed_candidate as stage
from stockagent.data_sync import cold_primary, packed_snapshots as packed
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.fixture
def fixed_release(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    for name in ("build", "verify", "restore", "migration"):
        (workspace / name).mkdir(parents=True)
    source, cold = tmp_path / "source", tmp_path / "cold"
    for name, contents in {"raw/first.zip": b"raw-source", "normalized/values.parquet": b"actual-values",
                           "README.md": b"fixed-release"}.items():
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    original = packed.publish_packed_snapshot(cold, "cftc-legacy-pre2000", source, node_id="test")
    materialized = packed.fetch_packed_snapshot(cold, workspace / "restore", original)
    fixed = {"contract": "stockagent.cftc_closed_E_restore.v1",
        "state": "fixed_release_materialized_and_readback_verified", "dataset": "cftc-legacy-pre2000",
        "snapshot_id": original.manifest["snapshot_id"], "manifest_sha256": original.manifest_sha256,
        "inventory_sha256": original.manifest["archive"]["inventory"]["sha256"],
        "E_materialized_path": str(materialized)}
    receipt = workspace / "verify/fixed.json"
    receipt.write_text(json.dumps(fixed))
    monkeypatch.setattr(stage, "COLD", cold)
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda _: None)
    monkeypatch.setattr(stage.volume, "configuration", lambda: {"minimum_free_bytes": 1})
    monkeypatch.setattr(stage.volume, "path", lambda name, **_: workspace / name)
    monkeypatch.setattr(stage.volume, "admit", lambda *_, **__: {"state": "fixture"})
    @contextmanager
    def capacity(*_, **__):
        yield {"other_reserved_growth_bytes": 0}
    monkeypatch.setattr(stage, "reserve_capacity", capacity)
    verify = packed.verify_packed_snapshot
    def portable_verify(root, snapshot, **kwargs):
        kwargs.pop("d_primary_native_blob_reads", None)
        kwargs.pop("d_primary_native_pack_reads", None)
        return verify(root, snapshot, **kwargs)
    monkeypatch.setattr(stage.packed, "verify_packed_snapshot", portable_verify)
    return workspace, cold, original, materialized, receipt


def test_fixed_candidate_is_independently_restored_without_changing_original(fixed_release):
    workspace, cold, original, _, receipt = fixed_release
    original_head = original.head_path.read_bytes()
    result = stage.stage(receipt, workspace / "build/candidate")
    assert result["state"] == "E_candidate_independently_restored"
    assert result["files"] == result["original_reconstructed_files"] == result["candidate_reconstructed_files"] == 3
    assert result["new_pack_layout"] == "source-dataset-v1" and result["candidate_object_count"] == 1
    assert result["portable_inventories_equal"]
    assert result["D_authority_changed"] is result["NAS_accepted"] is result["source_retirement_authorized"] is False
    assert original.head_path.read_bytes() == original_head
    ready = workspace / "build/candidate/candidate-ready.json"
    prior = ready.read_bytes()
    assert json.loads(prior)["candidate_manifest_sha256"]
    assert stage.stage(receipt, workspace / "build/candidate")["candidate_manifest_sha256"] == result["candidate_manifest_sha256"]
    assert ready.read_bytes() == prior
    assert original.head_path.read_bytes() == original_head


def test_changed_source_cannot_be_accepted_as_fixed_release(fixed_release):
    workspace, cold, original, materialized, receipt = fixed_release
    original_head = original.head_path.read_bytes()
    (materialized / "raw/first.zip").write_bytes(b"not-original")
    with pytest.raises(SnapshotError):
        stage.stage(receipt, workspace / "build/bad-candidate")
    candidate = workspace / "build/bad-candidate"
    assert json.loads((candidate / "status.json").read_text())["state"] == "failed_retained_for_inspection"
    assert not (candidate / "candidate-ready.json").exists()
    assert original.head_path.read_bytes() == original_head


def test_wrong_receipt_hash_cannot_create_candidate(fixed_release):
    workspace, cold, original, _, receipt = fixed_release
    value = json.loads(receipt.read_text())
    value["manifest_sha256"] = "0" * 64
    receipt.write_text(json.dumps(value))
    with pytest.raises(SnapshotError, match="fixed D release"):
        stage.stage(receipt, workspace / "build/rejected")
    assert not (workspace / "build/rejected").exists()


def test_interrupted_candidate_resumes_with_its_original_intent(fixed_release, monkeypatch):
    workspace, cold, original, _, receipt = fixed_release
    candidate = workspace / "build/resume"
    with monkeypatch.context() as patch:
        def interrupted(*_, **__):
            raise OSError("simulated interrupted write")
        patch.setattr(stage.packed, "publish_packed_snapshot", interrupted)
        with pytest.raises(OSError, match="simulated"):
            stage.stage(receipt, candidate)
    assert (candidate / "intent.json").is_file()
    assert not (candidate / "candidate-ready.json").exists()
    assert stage.stage(receipt, candidate)["state"] == "E_candidate_independently_restored"
    assert packed.resolve_packed_snapshot_id(cold, "cftc-legacy-pre2000", original.manifest["snapshot_id"]).manifest_sha256 == original.manifest_sha256


def test_interrupted_candidate_rejects_redirected_cold_root(fixed_release, monkeypatch):
    workspace, cold, original, _, receipt = fixed_release
    candidate = workspace / "build/redirect"
    with monkeypatch.context() as patch:
        def interrupted(*_, **__):
            raise OSError("simulated interrupted write")
        patch.setattr(stage.packed, "publish_packed_snapshot", interrupted)
        with pytest.raises(OSError):
            stage.stage(receipt, candidate)
    (candidate / "cold").symlink_to(cold)
    original_head = original.head_path.read_bytes()
    with pytest.raises(SnapshotError, match="redirected"):
        stage.stage(receipt, candidate)
    assert original.head_path.read_bytes() == original_head
