from __future__ import annotations

import json
import os
import time
from pathlib import Path

import stockagent.data_sync.artifact_maintenance as maintenance
from stockagent.data_sync.artifact_maintenance import (
    automatic_dataset_name,
    discover_completed_runs,
    maintain_completed_artifacts,
)


def test_automatic_dataset_name_is_stable_and_path_specific() -> None:
    first = automatic_dataset_name("ablations/suite/baseline")
    assert first == automatic_dataset_name("ablations/suite/baseline")
    assert first != automatic_dataset_name("ablations/other/baseline")
    assert first.startswith("artifact-auto-baseline-")


def test_discover_completed_runs_excludes_running_and_symlink(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    complete = artifact_root / "ablations" / "suite" / "complete"
    running = artifact_root / "ablations" / "suite" / "running"
    complete.mkdir(parents=True)
    running.mkdir(parents=True)
    (complete / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    (running / "progress.json").write_text(
        json.dumps({"state": "running", "phase": "train"}), encoding="utf-8"
    )
    link = artifact_root / "ablations" / "linked"
    link.symlink_to(complete, target_is_directory=True)

    assert discover_completed_runs(artifact_root, "ablations") == [complete]


class _Resolved:
    manifest = {"snapshot_id": "snapshot-one"}
    manifest_sha256 = "a" * 64


def test_expired_source_is_kept_until_peer_converges(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "ablations" / "suite" / "complete"
    source.mkdir(parents=True)
    (source / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    monkeypatch.setattr(maintenance, "_release_matches_source", lambda *args: _Resolved())
    monkeypatch.setattr(maintenance, "artifact_process_references", lambda *args: [])
    old_ns = time.time_ns() - 8 * 86_400 * 1_000_000_000
    monkeypatch.setattr(maintenance, "newest_activity_ns", lambda path: old_ns)

    result = maintain_completed_artifacts(
        artifact_root,
        tmp_path / "packed",
        tmp_path / "state",
        apply=True,
        peer_converged=lambda: {"ok": False},
    )

    assert source.is_dir()
    assert result["evicted"] == 0
    assert result["rows"][0]["reason"] == "peer-not-converged"


def test_expired_exact_source_is_evicted_after_peer_converges(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "ablations" / "suite" / "complete"
    source.mkdir(parents=True)
    (source / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    calls = []
    monkeypatch.setattr(
        maintenance,
        "_release_matches_source",
        lambda *args: calls.append(args) or _Resolved(),
    )
    monkeypatch.setattr(maintenance, "artifact_process_references", lambda *args: [])
    old_ns = time.time_ns() - 8 * 86_400 * 1_000_000_000
    monkeypatch.setattr(maintenance, "newest_activity_ns", lambda path: old_ns)

    result = maintain_completed_artifacts(
        artifact_root,
        tmp_path / "packed",
        tmp_path / "state",
        apply=True,
        peer_converged=lambda: {"ok": True, "completion": 100},
    )

    assert not source.exists()
    assert len(calls) == 2
    assert result["evicted"] == 1
    assert result["rows"][0]["reason"] == "verified-cold-and-peer-converged"


def test_source_changed_during_final_verification_is_kept(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "ablations" / "suite" / "complete"
    source.mkdir(parents=True)
    progress = source / "progress.json"
    progress.write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    old_ns = time.time_ns() - 8 * 86_400 * 1_000_000_000
    progress.touch()
    progress_mtime = old_ns
    progress.chmod(0o644)
    os.utime(progress, ns=(progress_mtime, progress_mtime))
    calls = 0

    def release_matches(*_args):
        nonlocal calls
        calls += 1
        if calls == 2:
            changed = source / "late-change.txt"
            changed.write_text("changed", encoding="utf-8")
        return _Resolved()

    monkeypatch.setattr(maintenance, "_release_matches_source", release_matches)
    monkeypatch.setattr(maintenance, "artifact_process_references", lambda *args: [])

    result = maintain_completed_artifacts(
        artifact_root,
        tmp_path / "packed",
        tmp_path / "state",
        retention_days=0,
        apply=True,
        peer_converged=lambda: {"ok": True, "completion": 100},
        now_ns=time.time_ns(),
    )

    assert source.is_dir()
    assert result["evicted"] == 0
    assert result["rows"][0]["reason"] == "source-changed-during-verification"
