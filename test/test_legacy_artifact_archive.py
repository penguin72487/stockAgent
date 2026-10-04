from __future__ import annotations

from dataclasses import replace
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
import stockagent.data_sync.legacy_artifact_archive as archive_module


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


def test_guarded_stage_refuses_missing_d_mount_before_writes(tmp_path: Path, monkeypatch):
    import stockagent.data_sync.cold_primary as cold_primary

    spec, artifact_root, source = _fixture(tmp_path)
    spec = replace(spec, stage_root=Path("/srv/stockagent-d-volume/test-archive-stage"))
    def missing_mount(root):
        assert root == Path("/srv/stockagent-packed")
        raise SnapshotError("D cold primary mount check failed")
    monkeypatch.setattr(cold_primary, "_check_d_primary_mount", missing_mount)
    with pytest.raises(SnapshotError, match="mount check failed"):
        prepare_archive(spec, artifact_root)
    assert (source / "fold_01/checkpoint_best.pt").read_bytes() == b"legacy incomplete checkpoint"


def test_legacy_exact_recovery_accepts_hardlink_ctime_drift(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    payload = source / "fold_01/checkpoint_best.pt"
    alias = tmp_path / "verified-alias"
    os.link(payload, alias)
    prepared = prepare_archive(spec, artifact_root)
    before = payload.stat().st_ctime_ns
    alias.unlink()
    assert payload.stat().st_ctime_ns != before
    proof = verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)
    drift = next(row for row in proof["source_metadata_drift"] if row["path"] == "fold_01/checkpoint_best.pt")
    assert drift["changed_observations"] == ["ctime_ns"]
    assert proof["manifest"] == prepared


def test_legacy_source_mutating_while_hashing_is_still_rejected(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    payload = source / "fold_01/checkpoint_best.pt"
    sha = archive_module.sha256_file

    def mutate(path):
        digest = sha(path)
        if path == payload:
            info = path.stat()
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
        return digest

    monkeypatch.setattr(archive_module, "sha256_file", mutate)
    with pytest.raises(SnapshotError, match="source differs"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)


@pytest.mark.parametrize("change", ["bytes", "mtime", "mode"])
def test_legacy_ctime_fix_never_ignores_recoverable_metadata_or_bytes(tmp_path, change):
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    payload = source / "fold_01/checkpoint_best.pt"
    info = payload.stat()
    if change == "bytes":
        payload.write_bytes(b"x" * info.st_size)
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns))
    elif change == "mtime":
        os.utime(payload, ns=(info.st_atime_ns, info.st_mtime_ns - 1))
    else:
        payload.chmod(0o600)
    with pytest.raises(SnapshotError, match="source differs"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)
