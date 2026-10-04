import hashlib
import json
import os
import time
from pathlib import Path

import pytest

from scripts import resume_preparation_evidence_dedup as module


def _fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(module, "artifact_process_references_many", lambda *args: [])
    root = tmp_path / "artifacts/markets/tw_futures_v8_margin_preparation"
    sources = root / "all_products_rule_facts_native_v1/sources"
    sources.mkdir(parents=True)
    for name in ("a", "b"):
        (sources / name).write_bytes(b"identical evidence")
    report = tmp_path / "report"
    (report / "dedup_batches").mkdir(parents=True)
    group = {"canonical": "all_products_rule_facts_native_v1/sources/a",
             "duplicates": ["all_products_rule_facts_native_v1/sources/b"],
             "sha256": hashlib.sha256(b"identical evidence").hexdigest(), "size": 18}
    plan = {"root": str(root), "selected_roots": [sources.parent.name], "groups": [group]}
    # Test the age gate without sleeping: this audited wave began 48h later.
    status = {"completed_groups": 0, "groups": 1, "started_ns": time.time_ns() + 48 * 3600 * 10**9,
              "reclaimed_allocated_bytes": 0, "reclaimed_logical_bytes": 0,
              "replaced_files": 0, "skipped_files": 0}
    (report / "preparation_dedup_dry_run.json").write_text(json.dumps(plan))
    (report / "preparation_dedup_apply_status.json").write_text(json.dumps(status))
    return root, sources, report, plan


def test_dry_run_is_read_only_and_apply_is_resumable(tmp_path, monkeypatch):
    root, sources, report, _ = _fixture(tmp_path, monkeypatch)
    before = (sources / "b").stat().st_ino
    assert module.resume(report)["remaining_groups"] == 1
    assert (sources / "b").stat().st_ino == before
    result = module.resume(report, apply=True)
    assert result["phase"] == "complete" and result["replaced_files"] == 1
    assert os.path.samestat((sources / "a").stat(), (sources / "b").stat())
    assert (sources / "a").read_bytes() == b"identical evidence"
    assert module.resume(report, apply=True)["replaced_files"] == 1


def test_partial_batch_alias_is_not_counted_as_new_recovery(tmp_path, monkeypatch):
    _, sources, report, _ = _fixture(tmp_path, monkeypatch)
    (sources / "b").unlink()
    os.link(sources / "a", sources / "b")
    result = module.resume(report, apply=True)
    assert result["reclaimed_allocated_bytes"] == 0
    assert result["uncounted_existing_or_external_links"] == 1


def test_active_process_blocks_before_mutation(tmp_path, monkeypatch):
    _, sources, report, _ = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "artifact_process_references_many", lambda *args: ["pid=123"])
    with pytest.raises(RuntimeError, match="in use"):
        module.resume(report, apply=True)
    assert not os.path.samestat((sources / "a").stat(), (sources / "b").stat())


def test_symlink_scope_and_pending_receipt_fail_closed(tmp_path, monkeypatch):
    root, sources, report, _ = _fixture(tmp_path, monkeypatch)
    pending = report / "dedup_batches/0000.json"
    pending.write_text(json.dumps({"same_inode_verified": True}))
    with pytest.raises(ValueError, match="reconciliation"):
        module.resume(report, apply=True)
    pending.unlink()
    moved = tmp_path / "moved"
    sources.parent.rename(moved)
    (root / moved.name).symlink_to(moved, target_is_directory=True)
    with pytest.raises(ValueError, match="redirected"):
        module.resume(report, apply=True)


def test_changed_payload_is_not_replaced(tmp_path, monkeypatch):
    _, sources, report, _ = _fixture(tmp_path, monkeypatch)
    (sources / "b").write_bytes(b"different evidence")
    result = module.resume(report, apply=True)
    assert result["replaced_files"] == 0 and result["skipped_files"] == 1
    assert (sources / "b").read_bytes() == b"different evidence"
