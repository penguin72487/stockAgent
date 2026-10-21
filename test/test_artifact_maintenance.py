from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

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


def test_discovery_stops_at_run_boundary_and_never_walks_running_caches(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    run = root / "markets" / "running"
    cache = run / "cache" / "nested"
    cache.mkdir(parents=True)
    (run / "run_manifest.json").write_text("{}")
    (run / "progress.json").write_text(json.dumps({"state": "running", "phase": "training"}))
    (cache / "progress.json").write_text(json.dumps({"state": "complete", "phase": "complete"}))
    assert discover_completed_runs(root, "markets") == []


def test_malformed_progress_does_not_abort_other_complete_runs(tmp_path):
    root = tmp_path / "artifacts"
    for name, content in (("broken", "[]"), ("complete", '{"state":"complete","phase":"complete"}')):
        run = root / "markets" / name
        run.mkdir(parents=True)
        (run / "progress.json").write_text(content)
    assert discover_completed_runs(root, "markets") == [root / "markets/complete"]


@pytest.mark.parametrize("argument", ["artifacts/markets/suite", "--output=artifacts/markets/suite", "artifacts/markets"])
def test_relative_or_option_ancestor_keeps_a_live_process_reference(tmp_path, argument):
    scope = tmp_path / "artifacts/markets"
    source = scope / "suite/run"
    source.mkdir(parents=True)
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", argument], cwd=tmp_path)
    try:
        references = maintenance.artifact_process_references(source, scope)
        assert any(f"pid={process.pid}:cmdline:" in row for row in references)
    finally:
        process.terminate()
        process.wait(timeout=5)


@pytest.mark.parametrize("argument", ["artifacts/markets/other", "--root=/"])
def test_sibling_or_generic_server_root_does_not_block_the_selected_run(tmp_path, argument):
    scope = tmp_path / "artifacts/markets"
    source = scope / "selected"
    source.mkdir(parents=True)
    sibling = scope / "other"
    sibling.mkdir()
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", argument], cwd=tmp_path)
    try:
        assert not any(f"pid={process.pid}:" in row for row in maintenance.artifact_process_references(source, scope))
    finally:
        process.terminate()
        process.wait(timeout=5)


def _fake_argument_process(tmp_path, monkeypatch, arguments):
    process = tmp_path / 'proc/999999999'
    process.mkdir(parents=True)
    (process / 'cmdline').write_bytes(b'\0'.join(os.fsencode(value) for value in arguments) + b'\0')
    (process / 'cwd').symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(maintenance, '_process_directories', lambda: [process])
    return process


def test_ancestor_resolution_shares_repeated_paths_but_rechecks_before_acceptance(tmp_path, monkeypatch):
    scope = tmp_path / 'artifacts/markets'
    source = scope / 'suite/run'
    source.mkdir(parents=True)
    process = _fake_argument_process(tmp_path, monkeypatch, ['unrelated'] * 100)
    calls = {}
    original = Path.resolve
    def tracked(path, **options):
        calls[path] = calls.get(path, 0) + 1
        return original(path, **options)
    monkeypatch.setattr(Path, 'resolve', tracked)
    assert maintenance._ancestor_process_references((source,), scope, []) == []
    assert calls[process / 'cwd'] == 2
    assert calls[tmp_path / 'unrelated'] == 2
    # A second guard is always fresh, never a cached no-use lease.
    assert maintenance._ancestor_process_references((source,), scope, []) == []
    assert calls[process / 'cwd'] == calls[tmp_path / 'unrelated'] == 4


def test_ancestor_resolution_still_protects_external_alias_and_option_paths(tmp_path, monkeypatch):
    scope = tmp_path / 'artifacts/markets'
    source = scope / 'suite/run'
    source.mkdir(parents=True)
    alias = tmp_path / 'external-alias'
    alias.symlink_to(scope / 'suite', target_is_directory=True)
    process = _fake_argument_process(tmp_path, monkeypatch, ['--root=external-alias'] * 4)
    references = maintenance._ancestor_process_references((source,), scope, [])
    assert references == [f'pid={process.name}:cmdline:{scope / "suite"}']


@pytest.mark.parametrize('problem', ['redirected_alias', 'changed_cwd', 'permission_lost', 'symlink_loop'])
def test_reused_argument_changes_deny_retirement_instead_of_using_negative_cache(tmp_path, monkeypatch, problem):
    from stockagent.data_sync.desync_snapshots import SnapshotError
    scope = tmp_path / 'artifacts/markets'
    source = scope / 'suite/run'
    source.mkdir(parents=True)
    process = _fake_argument_process(tmp_path, monkeypatch, ['unrelated'] * 4)
    original = Path.resolve
    candidate = process / 'cwd' if problem == 'changed_cwd' else tmp_path / 'unrelated'
    calls = 0
    def changed(path, **options):
        nonlocal calls
        if path == candidate:
            calls += 1
            if calls > 1:
                if problem == 'permission_lost':
                    raise PermissionError(13, 'lost visibility')
                if problem == 'symlink_loop':
                    raise OSError(40, 'new loop')
                return scope / 'suite'
        return original(path, **options)
    monkeypatch.setattr(Path, 'resolve', changed)
    with pytest.raises(SnapshotError, match='process argument'):
        maintenance._ancestor_process_references((source,), scope, [])


def test_exited_process_during_cwd_recheck_keeps_original_disappearance_rule(tmp_path, monkeypatch):
    scope = tmp_path / 'artifacts/markets'
    source = scope / 'suite/run'
    source.mkdir(parents=True)
    process = _fake_argument_process(tmp_path, monkeypatch, ['unrelated'] * 4)
    original = Path.resolve
    calls = 0
    def exited(path, **options):
        nonlocal calls
        if path == process / 'cwd':
            calls += 1
            if calls > 1:
                raise FileNotFoundError(2, 'exited')
        return original(path, **options)
    monkeypatch.setattr(Path, 'resolve', exited)
    assert maintenance._ancestor_process_references((source,), scope, []) == []


def test_unrelated_alias_rotation_is_not_misclassified_as_a_cleanup_consumer(tmp_path, monkeypatch):
    scope = tmp_path / 'artifacts/markets'
    source = scope / 'suite/run'
    source.mkdir(parents=True)
    _fake_argument_process(tmp_path, monkeypatch, ['unrelated'] * 4)
    original = Path.resolve
    calls = 0
    def rotate(path, **options):
        nonlocal calls
        if path == tmp_path / 'unrelated':
            calls += 1
            if calls > 1:
                return tmp_path / 'other-job/new-version'
        return original(path, **options)
    monkeypatch.setattr(Path, 'resolve', rotate)
    assert maintenance._ancestor_process_references((source,), scope, []) == []


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


def test_hot_transport_blocks_source_only_eviction(
    tmp_path: Path, monkeypatch
) -> None:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "ablations" / "suite" / "complete"
    source.mkdir(parents=True)
    (source / "progress.json").write_text(
        json.dumps({"state": "complete", "phase": "complete"}), encoding="utf-8"
    )
    hot_root = tmp_path / "hot"
    hot_root.mkdir()
    monkeypatch.setattr(maintenance, "_release_matches_source", lambda *args: _Resolved())
    monkeypatch.setattr(maintenance, "artifact_process_references", lambda *args: [])
    old_ns = time.time_ns() - 8 * 86_400 * 1_000_000_000
    monkeypatch.setattr(maintenance, "newest_activity_ns", lambda path: old_ns)

    result = maintain_completed_artifacts(
        artifact_root,
        tmp_path / "packed",
        tmp_path / "state",
        live_sync_root=hot_root,
        apply=True,
        peer_converged=lambda: {"ok": True},
    )

    assert source.is_dir()
    assert result["evicted"] == 0
    assert result["rows"][0]["reason"] == "hot-transport-retirement-required"
