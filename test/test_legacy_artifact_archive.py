from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec,
    prepare_archive,
    publish_archive,
    restore_archive,
    verify_archive_directory,
)
from stockagent.data_sync.packed_snapshots import resolve_latest_packed


def _fixture(tmp_path: Path) -> tuple[LegacyArchiveSpec, Path, Path]:
    artifact_root = tmp_path / "artifacts"
    source = artifact_root / "markets" / "crypto"
    (source / "fold_01").mkdir(parents=True)
    (source / "fold_01" / "holdings.csv").write_bytes(b"date,symbol,weight\n" * 500_000)
    (source / "fold_01" / "checkpoint_best.pt").write_bytes(b"legacy incomplete checkpoint")
    old = time.time() - 10 * 86_400
    for path in source.rglob("*"):
        if path.is_file():
            os.utime(path, (old, old))
    spec = LegacyArchiveSpec("legacy-artifact-markets-crypto", "markets/crypto", 7, tmp_path / "stage")
    return spec, artifact_root, source


def test_legacy_archive_roundtrip_is_non_deployable_and_exact(tmp_path: Path) -> None:
    spec, artifact_root, source = _fixture(tmp_path)
    archive = spec.stage_root / spec.dataset / "archive"
    prepared = prepare_archive(spec, artifact_root)
    assert prepared["deployable"] is False
    assert sorted(row["codec"] for row in prepared["files"]) == ["gzip", "raw"]
    assert prepare_archive(spec, artifact_root) == prepared
    assert verify_archive_directory(archive, source)["files"] == 2
    sync_root = tmp_path / "packed"
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    repeated = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    assert repeated["snapshot_id"] == release["snapshot_id"]
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    assert resolved.manifest["metadata"]["transport_role"] == "legacy-quarantine-archive"
    assert resolved.manifest["metadata"]["deployable"] == "false"
    assert release["source_files"] == 2
    destination = tmp_path / "restored"
    restore_archive(spec, sync_root, destination, materialized_root=tmp_path / "restore-cache")
    for original in source.rglob("*"):
        if original.is_file():
            assert (destination / original.relative_to(source)).read_bytes() == original.read_bytes()
    assert json.loads((destination / ".LEGACY_RESTORED.json").read_text())["deployable"] is False
    with pytest.raises(SnapshotError, match="overwrite"):
        restore_archive(spec, sync_root, destination, materialized_root=tmp_path / "restore-cache")


def test_legacy_archive_rejects_source_change_after_staging(tmp_path: Path) -> None:
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    (source / "fold_01" / "checkpoint_best.pt").write_bytes(b"changed")
    with pytest.raises(SnapshotError, match="stable|changed"):
        prepare_archive(spec, artifact_root)
