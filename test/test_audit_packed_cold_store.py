from __future__ import annotations

import json
from pathlib import Path

from scripts.audit_packed_cold_store import audit


def _json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def _manifest(dataset: str, snapshot: str, paths: tuple[str, ...]) -> dict:
    return {
        "dataset": dataset,
        "snapshot_id": snapshot,
        "archive": {
            "inventory": {"relpath": paths[0], "bytes": 1},
            "objects": [{"relpath": path, "bytes": 1} for path in paths[1:]],
        },
    }


def test_reachability_distinguishes_current_history_and_orphans(tmp_path: Path) -> None:
    root = tmp_path / "cold"
    compare = tmp_path / "other"
    current = ("objects/blobs/aa/a", "objects/blobs/bb/b")
    history = ("objects/blobs/bb/b", "objects/blobs/cc/c")
    _json(root / "heads/prices/node.json", {
        "dataset": "prices", "node_id": "node", "snapshot_id": "new",
    })
    _json(root / "manifests/prices/new.json", _manifest("prices", "new", current))
    _json(root / "manifests/prices/old.json", _manifest("prices", "old", history))
    for relative in (*current, "objects/blobs/dd/d"):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    missing_elsewhere = compare / "objects/blobs/cc/c"
    missing_elsewhere.parent.mkdir(parents=True)
    missing_elsewhere.write_bytes(b"x")

    result = audit(root, compare)

    assert result["deletion_authorized"] is False
    assert result["current_unique_object_count"] == 2
    assert result["current_unique_object_bytes"] == 2
    assert result["historical_only_object_count"] == 0
    assert result["unreferenced_object_count"] == 1
    assert result["missing_referenced_object_count"] == 1
    assert result["missing_current_object_count"] == 0
    assert result["missing_objects_present_in_compare_root"] == 1
    assert result["missing_by_dataset"] == {"prices": 1}
    assert result["errors"] == []

    fast_result = audit(root, compare, fast=True)
    assert fast_result["current_unique_object_count"] == 2
    assert fast_result["unreferenced_object_bytes"] == 1
    assert fast_result["missing_current_object_count"] == 0
    assert "not checked" in fast_result["proof_level"]


def test_missing_current_object_is_separate_from_historical_gap(tmp_path: Path) -> None:
    root = tmp_path / "cold"
    _json(root / "heads/prices/node.json", {
        "dataset": "prices", "node_id": "node", "snapshot_id": "new",
    })
    _json(root / "manifests/prices/new.json", _manifest(
        "prices", "new", ("objects/blobs/aa/a",),
    ))

    result = audit(root)

    assert result["missing_referenced_object_count"] == 1
    assert result["missing_current_object_count"] == 1


def test_rejects_unsafe_manifest_reference(tmp_path: Path) -> None:
    root = tmp_path / "cold"
    _json(root / "manifests/prices/old.json", _manifest(
        "prices", "old", ("objects/../escape",),
    ))

    result = audit(root)

    assert result["errors"]
    assert result["deletion_authorized"] is False
