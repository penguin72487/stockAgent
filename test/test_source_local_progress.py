from __future__ import annotations

import json
import time

from stockagent.data_sync import storage_progress as progress


def _fixture(tmp_path):
    workspace = tmp_path / "workspace"
    candidate = workspace / "build/source-local-test"
    candidate.mkdir(parents=True)
    original = "a" * 64
    config = [{"name": "source-local-test", "subject": "cftc-legacy-pre2000",
               "original_manifest_sha256": original}]
    ready = {"contract": "stockagent.source_local_E_candidate.v1", "state": "E_candidate_independently_restored",
        "dataset": "cftc-legacy-pre2000", "candidate_path": str(candidate),
        "original_manifest_sha256": original, "candidate_manifest_sha256": "b" * 64,
        "files": 3, "logical_bytes": 42, "original_reconstructed_files": 3,
        "candidate_reconstructed_files": 3, "portable_inventories_equal": True,
        "D_authority_changed": False, "NAS_accepted": False, "source_retirement_authorized": False,
        "observed_at_epoch": time.time()}
    (candidate / "candidate-ready.json").write_text(json.dumps(ready))
    (candidate / "status.json").write_text(json.dumps({"state": "E_candidate_independently_restored"}))
    return workspace, candidate, config, ready


def test_fixed_candidate_is_visible_without_claiming_D_or_NAS(tmp_path):
    workspace, _, config, _ = _fixture(tmp_path)
    rows = progress.source_local_candidate_tasks(workspace, config, workspace_checked=True)
    assert len(rows) == 1 and rows[0]["complete_evidence"]
    public = progress.public_status({"contract": progress.CONTRACT,
        "observed_at_utc": progress.iso(time.time()), "tasks": rows})
    row = public["tasks"][0]
    assert row["state"] == "completed" and row["progress"]["completed_items"] == 3
    assert "不是 D 新版本" in row["scope"] and "NAS" in row["scope"]
    assert str(workspace) not in json.dumps(public)


def test_candidate_cannot_claim_ready_without_matching_independent_proof(tmp_path):
    workspace, candidate, config, ready = _fixture(tmp_path)
    ready["candidate_reconstructed_files"] = 2
    (candidate / "candidate-ready.json").write_text(json.dumps(ready))
    row, = progress.source_local_candidate_tasks(workspace, config, workspace_checked=True)
    assert row["state"] == "blocked" and not row["complete_evidence"]
    (candidate / "candidate-ready.json").unlink()
    row, = progress.source_local_candidate_tasks(workspace, config, workspace_checked=True)
    assert row["state"] == "blocked" and not row["complete_evidence"]


def test_candidate_path_and_subject_are_strictly_enrolled(tmp_path):
    workspace, _, config, _ = _fixture(tmp_path)
    assert not progress.source_local_candidate_tasks(workspace, [{**config[0], "name": "../bad"}])
    assert not progress.source_local_candidate_tasks(workspace, [{**config[0], "subject": []}])
    row, = progress.source_local_candidate_tasks(workspace, config, workspace_checked=False)
    assert row["state"] == "blocked" and not row["complete_evidence"]


def test_candidate_layout_must_match_enrolled_policy(tmp_path):
    workspace, candidate, config, ready = _fixture(tmp_path)
    config[0]["pack_layout"] = "source-dataset-v1"
    ready["new_pack_layout"] = "source-directory-v1"
    (candidate / "candidate-ready.json").write_text(json.dumps(ready))
    row, = progress.source_local_candidate_tasks(workspace, config, workspace_checked=True)
    assert row["state"] == "blocked" and not row["complete_evidence"]
    ready["new_pack_layout"] = "source-dataset-v1"
    (candidate / "candidate-ready.json").write_text(json.dumps(ready))
    row, = progress.source_local_candidate_tasks(workspace, config, workspace_checked=True)
    assert row["state"] == "completed" and row["complete_evidence"]
