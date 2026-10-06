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


def test_adaptive_large_binary_compression_keeps_exact_recovery(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    data = b"uncompressed training tensor\0" * 400_000
    (source / "backtest.npz").write_bytes(data)
    os.utime(source / "backtest.npz", (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    result = publish_archive(spec, artifact_root, tmp_path / "cold", repo_root=tmp_path,
                             batch_directory_fsync=True)
    proof = archive_module.verify_cold_archive(spec, tmp_path / "cold")
    row = next(r for r in proof["manifest"]["files"] if r["path"] == "backtest.npz")
    assert row["codec"] == "gzip" and row["encoded_size"] < len(data) // 10
    restored = tmp_path / "restored"
    restore_archive(spec, tmp_path / "cold", restored, materialized_root=tmp_path / "restore-cache")
    assert (restored / "backtest.npz").read_bytes() == data
    assert result["cold_bytes"] < result["source_bytes"] // 10


def test_adaptive_incompressible_binary_falls_back_without_extra_trial(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    data = os.urandom(archive_module.COMPRESS_MIN_BYTES)
    (source / "already-compressed.parquet").write_bytes(data)
    os.utime(source / "already-compressed.parquet", (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    manifest = prepare_archive(spec, artifact_root)
    row = next(r for r in manifest["files"] if r["path"] == "already-compressed.parquet")
    assert row["codec"] == "raw" and row["encoded_size"] == len(data)
    assert not list((spec.stage_root / spec.dataset / "codec-trials").iterdir())
    assert verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)["files"] == 3


def test_new_compression_profile_preserves_verified_previous_stage(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    (source / "old.npz").write_bytes(b"0" * archive_module.COMPRESS_MIN_BYTES)
    os.utime(source / "old.npz", (1_000_000_000, 1_000_000_000))
    old = prepare_archive(spec, artifact_root)
    prior = next(r for r in old["files"] if r["path"] == "old.npz")
    assert prior["codec"] == "raw"
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    new = prepare_archive(spec, artifact_root)
    assert new["files"] == old["files"]
    assert verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)["files"] == 3


def test_adaptive_trial_source_mutation_keeps_source_and_trial(tmp_path, monkeypatch):
    spec, artifact_root, source = _fixture(tmp_path)
    path = source / "data.npz"
    path.write_bytes(b"0" * archive_module.COMPRESS_MIN_BYTES)
    os.utime(path, (1_000_000_000, 1_000_000_000))
    spec = replace(spec, compression_profile=archive_module.ADAPTIVE_COMPRESSION_PROFILE)
    encode = archive_module._encode_one

    def mutate(original, target, codec, **kwargs):
        digest = encode(original, target, codec, **kwargs)
        if original == path:
            path.write_bytes(b"changed during trial")
        return digest

    monkeypatch.setattr(archive_module, "_encode_one", mutate)
    with pytest.raises(SnapshotError, match="trial retained"):
        prepare_archive(spec, artifact_root)
    assert path.exists() and list((spec.stage_root / spec.dataset / "codec-trials").glob("*.gz"))
    assert not (spec.stage_root / spec.dataset / "archive/legacy_archive_manifest.json").exists()


def test_catalog_compression_profile_is_explicit_and_fail_closed(tmp_path):
    row = {"dataset": "legacy-test", "relative_root": "markets/example", "minimum_stable_days": 7,
           "archive_only": True, "compression": archive_module.ADAPTIVE_COMPRESSION_PROFILE,
           "stage_root": str(tmp_path / "stage")}
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}))
    assert archive_module.load_legacy_specs(catalog)["legacy-test"].compression_profile == row["compression"]
    row["compression"] = "unknown-fast-no-verification"
    catalog.write_text(json.dumps({"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}))
    with pytest.raises(SnapshotError, match="safety contract"):
        archive_module.load_legacy_specs(catalog)


def test_deferred_legacy_notification_keeps_durable_intent_and_exact_cold_recovery(tmp_path, monkeypatch):
    import stockagent.data_sync.syncthing_scan as scan
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    packed.mkdir()
    (packed / scan.D_PRIMARY_MARKER).write_text("{}")
    monkeypatch.setattr(scan, "CANONICAL_ROOT", packed)
    monkeypatch.setattr(scan, "_scan_pending", lambda *a, **kw: pytest.fail("publication did a network scan"))
    result = publish_archive(spec, artifact_root, packed, repo_root=tmp_path, defer_scan=True)
    pending = packed / ".local-state/scan-pending" / (spec.dataset + ".json")
    intent = json.loads(pending.read_text())
    assert intent["dataset"] == spec.dataset and intent["new_object_paths"]
    proof = archive_module.verify_cold_archive(spec, packed)
    assert proof["snapshot_id"] == result["snapshot_id"]
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert pending.exists() and (source / "fold_01/checkpoint_best.pt").exists()


def test_explicit_verification_workspace_recovers_when_default_tmp_is_too_small(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import stockagent.data_sync.training_return as training_return
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    scratch = tmp_path / "native-verification"
    observed = []
    monkeypatch.setattr(archive_module.shutil, "disk_usage", lambda path: SimpleNamespace(free=0))
    with pytest.raises(SnapshotError, match="verification scratch lacks"):
        archive_module.verify_cold_archive(spec, packed)

    def admit(path, required):
        observed.append((path, required))
        return {"filesystem": "ext4", "physical_free_bytes": 280 * 1024**3,
                "required_bytes": required}

    monkeypatch.setattr(training_return, "admit_workspace", admit)
    proof = archive_module.verify_cold_archive(spec, packed, verification_root=scratch)
    assert proof["cold_verified"] and proof["decoded_originals_verified"]
    assert proof["files"] == 2 and proof["verification_scratch_removed"]
    assert observed[0][0] == scratch and observed[0][1] > 32 * 1024**3
    assert proof["verification_workspace"]["parent"] == str(scratch)
    assert list(scratch.iterdir()) == []
    assert (source / "fold_01/checkpoint_best.pt").exists()


def test_explicit_verification_workspace_preserves_failed_reconstruction(tmp_path, monkeypatch):
    import stockagent.data_sync.training_return as training_return
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    monkeypatch.setattr(training_return, "admit_workspace", lambda *a: {})
    original_fetch = archive_module.fetch_packed_snapshot

    def corrupt_reconstructed_manifest(cold, destination, resolved):
        archive = original_fetch(cold, destination, resolved)
        (archive / "legacy_archive_manifest.json").write_bytes(b"corrupted reconstruction")
        return archive

    monkeypatch.setattr(archive_module, "fetch_packed_snapshot", corrupt_reconstructed_manifest)
    scratch = tmp_path / "native-verification"
    with pytest.raises(SnapshotError, match="scratch retained"):
        archive_module.verify_cold_archive(spec, packed, verification_root=scratch)
    assert list(scratch.glob("stockagent-legacy-verify-*"))
    assert (source / "fold_01/checkpoint_best.pt").exists()


@pytest.mark.parametrize("redirect", [False, True])
def test_verification_workspace_rejects_cold_overlap_or_symlinks(tmp_path, redirect):
    spec, artifact_root, source = _fixture(tmp_path)
    packed = tmp_path / "packed"
    publish_archive(spec, artifact_root, packed, repo_root=tmp_path)
    scratch = packed / "verification"
    if redirect:
        scratch = tmp_path / "redirect"
        scratch.symlink_to(tmp_path / "unrelated")
    with pytest.raises(SnapshotError, match="redirected or overlaps"):
        archive_module.verify_cold_archive(spec, packed, verification_root=scratch)


def test_archive_restores_empty_directories_and_portable_metadata(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    empty = source / "unused" / "nested"
    empty.mkdir(parents=True)
    empty.chmod(0o750)
    os.utime(empty, ns=(1_000_000_000, 2_000_000_000))
    publish_archive(spec, artifact_root, tmp_path / "packed", repo_root=tmp_path)
    restored = tmp_path / "restored"
    restore_archive(spec, tmp_path / "packed", restored, materialized_root=tmp_path / "restore-cache")
    actual = restored / "unused/nested"
    assert actual.is_dir() and list(actual.iterdir()) == []
    assert actual.stat().st_mode & 0o7777 == 0o750
    assert actual.stat().st_mtime_ns == 2_000_000_000


def test_archive_accepts_allowlisted_ablation_scope_and_rejects_cache_scope(tmp_path):
    path = tmp_path / "catalog.json"
    row = {"dataset": "legacy-ablation", "relative_root": "ablations/example",
           "minimum_stable_days": 7, "archive_only": True, "compression": "gzip-1-csv-over-8m",
           "stage_root": str(tmp_path / "stage")}
    catalog = {"schema_version": 1, "authority_node_id": "penguin", "archives": [row]}
    path.write_text(json.dumps(catalog))
    assert archive_module.load_legacy_specs(path)["legacy-ablation"].relative_root == "ablations/example"
    row["relative_root"] = "cache/example"
    path.write_text(json.dumps(catalog))
    with pytest.raises(SnapshotError, match="scoped"):
        archive_module.load_legacy_specs(path)


def test_empty_directory_change_after_encoding_is_rejected(tmp_path):
    spec, artifact_root, source = _fixture(tmp_path)
    (source / "empty").mkdir()
    prepare_archive(spec, artifact_root)
    (source / "empty").rmdir()
    with pytest.raises(SnapshotError, match="directory inventory"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)


def test_legacy_archive_rejects_source_change_after_staging(tmp_path: Path) -> None:
    spec, artifact_root, source = _fixture(tmp_path)
    prepare_archive(spec, artifact_root)
    (source / "fold_01" / "checkpoint_best.pt").write_bytes(b"changed")
    with pytest.raises(SnapshotError, match="stable|changed"):
        prepare_archive(spec, artifact_root)


def test_cli_cold_only_verifies_retired_archive_and_still_rejects_corrupt_objects(tmp_path, monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli

    spec, artifact_root, source = _fixture(tmp_path)
    sync_root = tmp_path / "packed"
    release = publish_archive(spec, artifact_root, sync_root, repo_root=tmp_path)
    source.rename(source.with_name("retired-test-source"))
    monkeypatch.setattr(cli, "load_legacy_specs", lambda _path: {spec.dataset: spec})
    args = ["manage_legacy_artifact_archives.py", "verify", spec.dataset,
            "--artifact-root", str(artifact_root), "--sync-root", str(sync_root)]
    monkeypatch.setattr(sys, "argv", args)
    assert cli.main() == 2
    assert "source file missing" in capsys.readouterr().err
    monkeypatch.setattr(sys, "argv", args + ["--cold-only"])
    assert cli.main() == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["snapshot_id"] == release["snapshot_id"]
    assert proof["cold_verified"] is proof["decoded_originals_verified"] is True
    assert proof["source_comparison"] == "not_requested_cold_only"
    assert proof["deployable"] is False
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    object_path = sync_root / resolved.manifest["archive"]["objects"][0]["relpath"]
    object_path.write_bytes(b"corrupt cold object")
    assert cli.main() == 2
    assert "mismatch" in capsys.readouterr().err


def test_cli_refuses_cold_only_option_for_publication(monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli

    monkeypatch.setattr(sys, "argv", ["manage_legacy_artifact_archives.py", "publish", "example", "--cold-only"])
    assert cli.main() == 2
    assert "requires verify" in capsys.readouterr().err


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
        payload.chmod(0o400 if info.st_mode & 0o7777 == 0o600 else 0o600)
    with pytest.raises(SnapshotError, match="source differs"):
        verify_archive_directory(spec.stage_root / spec.dataset / "archive", source)
