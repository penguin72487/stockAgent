from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import stockagent.data_sync.cold_primary as cold_primary
from stockagent.data_sync.desync_snapshots import SnapshotError


def _marker(root: Path) -> Path:
    marker = root / cold_primary.D_PRIMARY_MARKER
    marker.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "volume_id": cold_primary.D_PRIMARY_VOLUME_ID,
                "backing": cold_primary.D_PRIMARY_BACKING,
                "authority_node_id": "penguin",
                "resilience": "single_d_volume",
            }
        )
    )
    return marker


def test_d_primary_reports_single_volume_not_independent_backup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "packed"
    root.mkdir()
    _marker(root)
    checked = []
    monkeypatch.setattr(
        cold_primary, "_check_d_primary_mount", lambda path: checked.append(path)
    )
    resolved = SimpleNamespace(manifest={"dataset": "example", "snapshot_id": "one"})
    proof = cold_primary.verify_cold_resilience(
        root, resolved, tmp_path / "unused.json"
    )
    assert checked == [root]
    assert proof == {
        "backup_verified": False,
        "cold_primary_verified": True,
        "resilience": "single_d_volume",
    }


def test_d_primary_fails_closed_for_bad_identity_or_mount(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "packed"
    root.mkdir()
    marker = _marker(root)
    resolved = SimpleNamespace(manifest={"dataset": "example", "snapshot_id": "one"})
    marker.write_text('{"schema_version":1}')
    with pytest.raises(SnapshotError, match="identity mismatch"):
        cold_primary.verify_cold_resilience(root, resolved, tmp_path / "unused.json")
    _marker(root)
    monkeypatch.setattr(
        cold_primary,
        "_check_d_primary_mount",
        lambda path: (_ for _ in ()).throw(SnapshotError("mount unavailable")),
    )
    with pytest.raises(SnapshotError, match="mount unavailable"):
        cold_primary.verify_cold_resilience(root, resolved, tmp_path / "unused.json")
