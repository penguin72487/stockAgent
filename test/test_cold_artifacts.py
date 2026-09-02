from __future__ import annotations

import json
from pathlib import Path

from stockagent.data_sync.cold_artifacts import (
    COLD_IGNORE_DIRECTIVE,
    COLD_IGNORE_INCLUDE,
    ColdArtifactSpec,
    activate_cold_artifact,
    load_cold_artifact_registry,
)
from stockagent.data_sync.packed_snapshots import (
    initialize_packed_layout,
    publish_packed_snapshot,
)


def _spec() -> ColdArtifactSpec:
    return ColdArtifactSpec(
        dataset="artifact-run-small",
        relative_root="markets/completed-run",
        maximum_file_bytes=15,
        loose_file_threshold_bytes=16,
        pack_buckets=2,
        min_stable_hours=24,
        completion_contract="training-lifecycle-v1",
    )


def _publish(tmp_path: Path) -> tuple[Path, ColdArtifactSpec]:
    spec = _spec()
    source = tmp_path / "publisher" / spec.relative_root
    (source / "reports").mkdir(parents=True)
    (source / "small.json").write_text('{"ok": true}\n', encoding="utf-8")
    (source / "reports" / "tiny.txt").write_text("tiny\n", encoding="utf-8")
    (source / "large.bin").write_bytes(b"x" * 32)
    sync_root = tmp_path / "packed"
    initialize_packed_layout(sync_root, node_id="penguin")
    publish_packed_snapshot(
        sync_root,
        spec.dataset,
        source,
        loose_file_threshold_bytes=spec.loose_file_threshold_bytes,
        maximum_file_bytes=spec.maximum_file_bytes,
        pack_buckets=spec.pack_buckets,
        metadata={
            "artifact_relative_root": spec.relative_root,
            "completion_contract": spec.completion_contract,
            "transport_role": "cold-small-files",
        },
    )
    return sync_root, spec


def test_activate_materializes_directly_and_writes_local_ignore(tmp_path: Path) -> None:
    sync_root, spec = _publish(tmp_path)
    artifact_root = tmp_path / "receiver" / "artifacts"
    live_root = tmp_path / "receiver" / "live-sync"
    live_root.mkdir(parents=True)
    (live_root / ".stignore").write_text("(?d)data_locks\n", encoding="utf-8")

    receipt = activate_cold_artifact(
        sync_root,
        artifact_root,
        live_root,
        tmp_path / "state",
        spec,
    )

    destination = artifact_root / spec.relative_root
    assert (destination / "small.json").read_text(encoding="utf-8") == '{"ok": true}\n'
    assert (destination / "reports" / "tiny.txt").read_text(encoding="utf-8") == "tiny\n"
    assert not (destination / "large.bin").exists()
    assert receipt["added"] == 2
    assert receipt["local_conflicts"] == 0
    ignore = (live_root / COLD_IGNORE_INCLUDE).read_text(encoding="utf-8")
    assert "(?d)/markets/completed-run/small.json" in ignore
    assert "(?d)/markets/completed-run/reports/tiny.txt" in ignore
    assert "large.bin" not in ignore
    assert COLD_IGNORE_DIRECTIVE in (live_root / ".stignore").read_text(
        encoding="utf-8"
    )


def test_local_wins_conflict_is_preserved_and_not_ignored(tmp_path: Path) -> None:
    sync_root, spec = _publish(tmp_path)
    artifact_root = tmp_path / "receiver" / "artifacts"
    destination = artifact_root / spec.relative_root
    destination.mkdir(parents=True)
    (destination / "small.json").write_text("receiver wins\n", encoding="utf-8")
    live_root = tmp_path / "receiver" / "live-sync"
    live_root.mkdir(parents=True)

    receipt = activate_cold_artifact(
        sync_root,
        artifact_root,
        live_root,
        tmp_path / "state",
        spec,
        conflict_policy="local-wins",
    )

    assert (destination / "small.json").read_text(encoding="utf-8") == "receiver wins\n"
    assert receipt["local_conflicts"] == 1
    assert receipt["conflicts_detected"] == 1
    ignore = (live_root / COLD_IGNORE_INCLUDE).read_text(encoding="utf-8")
    assert "/markets/completed-run/small.json" not in ignore
    assert "/markets/completed-run/reports/tiny.txt" in ignore


def test_packed_wins_conflict_is_replaced_and_audited(tmp_path: Path) -> None:
    sync_root, spec = _publish(tmp_path)
    artifact_root = tmp_path / "receiver" / "artifacts"
    destination = artifact_root / spec.relative_root
    destination.mkdir(parents=True)
    (destination / "small.json").write_text("receiver loses\n", encoding="utf-8")
    live_root = tmp_path / "receiver" / "live-sync"
    live_root.mkdir(parents=True)

    receipt = activate_cold_artifact(
        sync_root,
        artifact_root,
        live_root,
        tmp_path / "state",
        spec,
        conflict_policy="packed-wins",
    )

    assert (destination / "small.json").read_text(encoding="utf-8") == '{"ok": true}\n'
    assert receipt["conflicts_detected"] == 1
    assert receipt["replaced"] == 1
    ignore = (live_root / COLD_IGNORE_INCLUDE).read_text(encoding="utf-8")
    assert "/markets/completed-run/small.json" in ignore


def test_registry_rejects_duplicate_roots(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    row = {
        "relative_root": "markets/run",
        "maximum_file_bytes": 10,
        "loose_file_threshold_bytes": 11,
        "pack_buckets": 2,
        "min_stable_hours": 24,
        "completion_contract": "training-lifecycle-v1",
    }
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifacts": [
                    {"dataset": "first", **row},
                    {"dataset": "second", **row},
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        load_cold_artifact_registry(path)
    except Exception as exc:
        assert "duplicate cold artifact root" in str(exc)
    else:
        raise AssertionError("duplicate cold root should fail closed")


def test_registry_allows_full_run_without_file_size_limit(tmp_path: Path) -> None:
    path = tmp_path / "registry.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "artifacts": [
                    {
                        "dataset": "full-run",
                        "relative_root": "markets/full-run",
                        "maximum_file_bytes": None,
                        "loose_file_threshold_bytes": 8 * 1024 * 1024,
                        "pack_buckets": 32,
                        "min_stable_hours": 24,
                        "completion_contract": "training-lifecycle-v1",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    registry = load_cold_artifact_registry(path)

    assert registry["full-run"].maximum_file_bytes is None
