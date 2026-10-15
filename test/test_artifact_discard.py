import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from stockagent.data_sync import artifact_discard as d
from stockagent.data_sync.desync_snapshots import SnapshotError


def policy(tmp_path, targets=None):
    body = {"contract": d.CONTRACT, "authority_node_id": "penguin", "origin_node_id": "vastai1T",
            "authorization": "user explicitly discards testing outputs and cold copies",
            "decision": "discard_without_preservation", "targets": targets or [
                {"base": "artifact", "relative": "smoke/trial", "kind": "directory", "reason": "smoke-test"}]}
    value = {**body, "identity_sha256": d.identity(body)}
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(value))
    return path, d.load_policy(path)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    root = repo / "artifacts/smoke/trial"
    root.mkdir(parents=True)
    (root / "report.txt").write_text("unused engineering result")
    (root / "empty").mkdir()
    _, p = policy(tmp_path)
    monkeypatch.setattr(d, "artifact_process_references", lambda *a: [])
    monkeypatch.setattr(d, "active_configuration_references", lambda *a: [])
    monkeypatch.setattr(d, "artifact_service_references", lambda sources, *a: {str(s): [] for s in sources})
    monkeypatch.setattr(d, "shared_inode_references", lambda *a: [])
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    args = dict(repo_root=repo, engineering_root=tmp_path / "engineering", state_root=tmp_path / "state",
                materialized_root=tmp_path / "materialized", cold_root=tmp_path / "cold")
    return p, root, args


def test_exact_unused_test_discard_needs_no_cold_recovery(setup):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    assert root.exists() and not plan["blockers"]
    result = d.discard_target(p, p["targets"][0], **args, apply=True,
                              expected_plan_identity=plan["identity_sha256"])
    assert result["deleted"] and not result["recovery_claimed"] and not root.exists()
    assert result["files"] == 1 and result["reclaimed_allocated_bytes"] > 0
    again = d.discard_target(p, p["targets"][0], **args, apply=True)
    assert again["state"] == "already-absent" and again["reclaimed_allocated_bytes"] == 0


def test_dry_run_generation_change_is_rejected(setup):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    (root / "new.txt").write_text("new result")
    with pytest.raises(SnapshotError, match="dry run changed"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert (root / "new.txt").exists()


@pytest.mark.parametrize("gate", ["artifact_process_references", "active_configuration_references", "shared_inode_references"])
def test_active_training_and_config_or_inode_use_overrides_discard(setup, monkeypatch, gate):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    monkeypatch.setattr(d, gate, lambda *a: ["live training"])
    with pytest.raises(SnapshotError, match="protection applies"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert root.exists()


def test_shared_external_names_survive_and_are_not_counted_as_reclaimed(setup, tmp_path):
    p, root, args = setup
    external = tmp_path / "formal-result.txt"
    os.link(root / "report.txt", external)
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not root.exists() and external.read_text() == "unused engineering result"
    assert result["reclaimed_allocated_bytes"] == 0


def test_symlink_escape_is_rejected(setup, tmp_path):
    p, root, args = setup
    (root / "escape").symlink_to(tmp_path)
    plan = d.discard_target(p, p["targets"][0], **args)
    assert "unsupported-or-cross-filesystem-entry" in plan["blockers"]
    with pytest.raises(SnapshotError):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])


@pytest.mark.parametrize("relative", ["smoke", "../source", "/source", "data_tw_futures/original"])
def test_namespace_and_source_roots_cannot_be_discarded(tmp_path, relative):
    with pytest.raises(SnapshotError):
        policy(tmp_path, [{"base": "artifact", "relative": relative, "kind": "directory", "reason": "smoke-test"}])


def test_overlapping_and_duplicate_targets_rejected(tmp_path):
    target = {"base": "artifact", "relative": "smoke/trial", "kind": "directory", "reason": "smoke-test"}
    for other in [target, {**target, "relative": "smoke/trial/output"}]:
        with pytest.raises(SnapshotError, match="overlapping"):
            policy(tmp_path, [target, other])


def test_only_matching_test_cold_manifest_is_disposed(tmp_path):
    _, p = policy(tmp_path)
    assert d.manifest_discarded({"metadata": {"source_relative_root": "smoke/trial"}}, p)
    assert d.manifest_discarded({"metadata": {"source_relative_root": "smoke/trial/cache"}}, p)
    assert not d.manifest_discarded({"metadata": {"source_relative_root": "smoke/other"}}, p)
    assert not d.manifest_discarded({"metadata": {"source_relative_root": "markets/formal"}}, p)


def test_individual_engineering_file_with_other_links(tmp_path, monkeypatch):
    name = "stockagent-remote-build-measurement-fixture/measurements/trial/first.parquet"
    _, p = policy(tmp_path, [{"base": "engineering", "relative": name, "kind": "file", "reason": "benchmark"}])
    source = tmp_path / "engineering" / name
    source.parent.mkdir(parents=True)
    source.write_bytes(b"test parquet fixture")
    peer = source.with_name("keep.parquet")
    os.link(source, peer)
    monkeypatch.setattr(d, "_gates", lambda *a: [])
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    args = dict(repo_root=tmp_path / "repo", engineering_root=tmp_path / "engineering", state_root=tmp_path / "state",
                materialized_root=tmp_path / "materialized", cold_root=tmp_path / "cold")
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not source.exists() and peer.exists() and result["reclaimed_allocated_bytes"] == 0


def test_directory_rename_size_drift_is_not_content_change():
    before = [64, 123, 24, 99, 98, 16893, 3]
    after = [64, 123, 32, 99, 101, 16893, 3]
    assert d._same_root_before_after_rename(before, after, "directory")
    assert not d._same_root_before_after_rename(before, after, "file")
    assert not d._same_root_before_after_rename(before, [64, 124, *after[2:]], "directory")


def test_only_unchanged_owned_quarantine_can_resume(setup, monkeypatch):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    monkeypatch.setattr(d, "process_references", lambda *a: ["temporary reader"])
    with pytest.raises(SnapshotError, match="quarantined"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not root.exists()
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    assert d.restore_unchanged_discard_quarantine(p, p["targets"][0], **args)
    assert (root / "report.txt").exists()
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert result["deleted"] and not root.exists()


def cold_fixture(tmp_path):
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="penguin")
    source = tmp_path / "source"
    source.mkdir()
    (source / "shared.txt").write_text("shared original observation")
    (source / "formal.txt").write_text("retained formal result")
    formal = publish_packed_snapshot(cold, "formal", source, loose_file_threshold_bytes=1)
    (source / "formal.txt").unlink()
    (source / "test.txt").write_text("discard this test result")
    test = publish_packed_snapshot(cold, "testing", source, loose_file_threshold_bytes=1,
                                  metadata={"source_relative_root": "smoke/trial"})
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"datasets": []}))
    path, p = policy(tmp_path)
    return cold, formal, test, catalog, path, p


def test_cold_discard_keeps_shared_formal_objects_and_manifest_receipts(tmp_path):
    from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot
    from stockagent.data_sync.backup_stream import capture_catalog
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    before = capture_catalog(cold, catalog, testing_disposition=path, include_unreferenced_objects=True)
    assert len(before["releases"]) == 1 and len(before["discarded_testing_releases"]) == 1
    assert not before["missing_objects"] and not before["metadata_errors"]
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    assert plan["objects"] and plan["retained_shared_objects"] and not plan["blockers"]
    result = d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")
    assert result["reclaimed_allocated_bytes"] > 0 and not result["nas_deletion_claimed"]
    assert (cold / plan["releases"][0]["manifest_relative"]).exists()
    recovered = fetch_packed_snapshot(cold, tmp_path / "restore", formal)
    assert (recovered / "formal.txt").read_text() == "retained formal result"
    after = capture_catalog(cold, catalog, testing_disposition=path, include_unreferenced_objects=True)
    assert not after["missing_objects"] and not after["metadata_errors"]
    assert len(after["releases"]) == 1
    assert not any(r["relative"] in after["discarded_testing_objects"] for r in after["files"])


def test_new_formal_reference_prevents_stale_cold_discard(tmp_path):
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    publish_packed_snapshot(cold, "another-formal", tmp_path / "source", loose_file_threshold_bytes=1)
    with pytest.raises(SnapshotError, match="graph changed"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")
    assert all((cold / r["relative"]).exists() for r in plan["objects"])


def test_pinned_cold_testing_version_is_protected(tmp_path, monkeypatch):
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    monkeypatch.setattr(d, "_pinned_snapshot_ids", lambda *a: {test.manifest["snapshot_id"]})
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    assert any(b.startswith("pinned-testing-release:") for b in plan["blockers"])
    with pytest.raises(SnapshotError, match="protection applies"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")


def test_disposition_runs_without_training_or_site_packages():
    root = Path(__file__).resolve().parents[1]
    code = "from stockagent.data_sync.testing_disposition import DEFAULT_POLICY,load_policy; import sys; load_policy(DEFAULT_POLICY); assert 'numpy' not in sys.modules and 'torch' not in sys.modules"
    result = subprocess.run([sys.executable, "-S", "-c", code], cwd=root, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_same_process_cold_audit_rechecks_new_references_and_age(tmp_path):
    import time
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    publish_packed_snapshot(cold, "formal-new", tmp_path / "source", loose_file_threshold_bytes=1)
    with pytest.raises(SnapshotError, match="generation changed"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized",
                            receipt=tmp_path / "receipt.json", _audit_started_monotonic=time.monotonic())
    with pytest.raises(SnapshotError, match="expired"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized",
                            receipt=tmp_path / "receipt.json", _audit_started_monotonic=time.monotonic()-1501)
    assert all((cold/r["relative"]).exists() for r in plan["objects"])


def test_shared_blob_descriptor_statistics_do_not_change_cas_identity(tmp_path):
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
    cold=tmp_path/'cold';initialize_packed_layout(cold,node_id='penguin')
    source=tmp_path/'source';source.mkdir();(source/'one.bin').write_bytes(b'unused same testing blob')
    publish_packed_snapshot(cold,'testing-one',source,loose_file_threshold_bytes=1,
                           metadata={'source_relative_root':'smoke/trial'})
    (source/'two.bin').write_bytes((source/'one.bin').read_bytes())
    publish_packed_snapshot(cold,'testing-two',source,loose_file_threshold_bytes=1,
                           metadata={'source_relative_root':'smoke/trial'})
    _,p=policy(tmp_path)
    plan=d.cold_discard_plan(cold,p,materialized_root=tmp_path/'materialized')
    assert len(plan['releases'])==2 and len({r['relative'] for r in plan['objects']})==len(plan['objects'])


def test_corrupt_discarded_testing_head_is_still_a_catalog_error(tmp_path):
    from stockagent.data_sync.backup_stream import capture_catalog
    cold,formal,test,catalog,path,p=cold_fixture(tmp_path)
    head=cold/'heads/testing/penguin.json';value=json.loads(head.read_bytes());value['snapshot_id']='invalid-snapshot'
    head.write_text(json.dumps(value))
    captured=capture_catalog(cold,catalog,testing_disposition=path)
    assert any(r['relative']=='heads/testing/penguin.json' for r in captured['metadata_errors'])
