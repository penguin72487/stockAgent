from __future__ import annotations

import json
import os
import stat
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


@pytest.mark.parametrize("difference", [None, "permissions", "st_ino", "st_size",
                                       "st_mtime_ns", "st_uid", "st_gid", "st_nlink", "file-type"])
def test_native_alias_requires_physical_identity_but_not_mount_permission_projection(
    tmp_path, monkeypatch, difference
):
    canonical, native = tmp_path / "canonical", tmp_path / "native"
    for root in (canonical, native):
        (root / "objects").mkdir(parents=True)
        _marker(root)
    source = canonical / "objects/exact.blob"
    source.write_bytes(b"unchanged immutable bytes")
    alias = native / "objects/exact.blob"
    os.link(source, alias)
    original_mode = source.stat().st_mode
    actual_lstat = Path.lstat

    def projected_lstat(path):
        observed = actual_lstat(path)
        if path != alias or difference is None:
            return observed
        values = {key: getattr(observed, key) for key in (
            "st_ino", "st_size", "st_mtime_ns", "st_mode", "st_uid", "st_gid", "st_nlink")}
        if difference == "permissions":
            values["st_mode"] = stat.S_IFREG | 0o744
        elif difference == "file-type":
            values["st_mode"] = stat.S_IFDIR | 0o744
        else:
            values[difference] += 1
        return SimpleNamespace(**values)

    monkeypatch.setattr(cold_primary, "Path", lambda value: canonical if value == "/srv/stockagent-packed" else Path(value))
    monkeypatch.setattr(cold_primary, "NATIVE_D_PRIMARY", native)
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", lambda path: None)
    monkeypatch.setattr(cold_primary.subprocess, "check_output", lambda *a, **k: json.dumps(
        {"filesystems": [{"source": "D:\\", "fstype": "9p"}]}))
    monkeypatch.setattr(Path, "lstat", projected_lstat)
    if difference in (None, "permissions"):
        assert cold_primary.d_primary_read_alias(source) == alias
        assert source.read_bytes() == alias.read_bytes() == b"unchanged immutable bytes"
    else:
        with pytest.raises(SnapshotError, match="identity differs|regular immutable"):
            cold_primary.d_primary_read_alias(source)
    assert source.stat().st_mode == original_mode
