from dataclasses import replace
import json
import os
from pathlib import Path
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.legacy_artifact_archive import (
    LegacyArchiveSpec, load_legacy_specs, prepare_archive, publish_archive,
    restore_archive, source_plan, verify_archive_directory,
)
import stockagent.data_sync.legacy_artifact_archive as archive_module
import stockagent.data_sync.legacy_artifact_retirement as retirement


def fixture(tmp_path):
    artifact_root = tmp_path / "repo/artifacts"
    source = artifact_root / "markets/old-research"
    source.mkdir(parents=True)
    for name in ("a.pt", "b.csv"):
        path = source / name
        path.write_bytes(b"unique old result " + name.encode())
        old = time.time_ns() - 86_400_000_000_000
        os.utime(path, ns=(old, old))
    spec = LegacyArchiveSpec("legacy-manual-test", "markets/old-research", 7,
                             tmp_path / "stage", 12)
    return spec, artifact_root, source


def test_manual_capture_is_explicit_allowlisted_and_automatic_age_is_unchanged(tmp_path):
    spec, _, source = fixture(tmp_path)
    with pytest.raises(SnapshotError, match="stable"):
        source_plan(source, spec)
    assert len(source_plan(source, spec, manual_capture=True)) == 2
    assert spec.minimum_stable_days == 7
    with pytest.raises(SnapshotError, match="not allowlisted"):
        source_plan(source, replace(spec, manual_capture_min_stable_hours=None), manual_capture=True)
    with pytest.raises(SnapshotError, match="explicit boolean"):
        source_plan(source, spec, manual_capture="true")


def test_manual_capture_keeps_a_real_stability_floor(tmp_path):
    spec, _, source = fixture(tmp_path)
    (source / "a.pt").touch()
    with pytest.raises(SnapshotError, match="stable"):
        source_plan(source, spec, manual_capture=True)
    with pytest.raises(SnapshotError, match="not allowlisted"):
        source_plan(source, replace(spec, manual_capture_min_stable_hours=0), manual_capture=True)


def test_rebuildable_staging_recovers_corrupt_receipts_and_payloads(tmp_path):
    spec, artifact_root, source = fixture(tmp_path)
    spec = replace(spec, durable_staging=False)
    with pytest.raises(SnapshotError, match="explicit manual capture"):
        prepare_archive(spec, artifact_root)
    manifest = prepare_archive(spec, artifact_root, manual_capture=True)
    receipts = spec.stage_root / spec.dataset / 'receipts'
    next(receipts.iterdir()).write_text('{interrupted write')
    archive = spec.stage_root / spec.dataset / 'archive'
    (archive / manifest['files'][0]['encoded_path']).write_bytes(b'torn encoded payload')
    prepare_archive(spec, artifact_root, manual_capture=True)
    proof = verify_archive_directory(archive, source, spec=spec, manual_capture=True,
                                     artifact_root=artifact_root)
    assert proof['files'] == 2
    result = publish_archive(spec, artifact_root, tmp_path / 'packed',
                             repo_root=artifact_root.parent, manual_capture=True)
    assert result['source_files'] == 2
    restored = tmp_path / 'restored'
    restore_archive(spec, tmp_path / 'packed', restored, materialized_root=tmp_path / 'restore-cache')
    assert all((restored/p.name).read_bytes() == p.read_bytes() for p in source.iterdir())


@pytest.mark.parametrize("hours", [0, 11, True, "12", float("nan")])
def test_catalog_refuses_unsafe_manual_capture_floor(tmp_path, hours):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps({"schema_version": 1, "authority_node_id": "penguin", "archives": [{
        "dataset": "legacy-example", "relative_root": "markets/example", "minimum_stable_days": 7,
        "manual_capture_min_stable_hours": hours, "archive_only": True,
        "compression": "gzip-1-csv-over-8m", "stage_root": str(tmp_path / "stage"),
    }]}))
    with pytest.raises(SnapshotError, match="stability floor"):
        load_legacy_specs(path)


def test_capture_roundtrip_checks_metadata_and_never_claims_deployability(tmp_path):
    spec, artifact_root, source = fixture(tmp_path)
    result = publish_archive(spec, artifact_root, tmp_path / "packed",
                             repo_root=artifact_root.parent, manual_capture=True)
    archive = spec.stage_root / spec.dataset / "archive"
    proof = verify_archive_directory(archive, source, spec=spec, manual_capture=True,
                                     artifact_root=artifact_root)
    assert proof["manifest"]["deployable"] is False
    assert proof["manifest"]["source_stability"]["mode"] == "manual-exact-capture-v1"
    with pytest.raises(SnapshotError, match="stable"):
        verify_archive_directory(archive, source, spec=spec)
    restored = tmp_path / "restored"
    restored_result = restore_archive(spec, tmp_path / "packed", restored,
                                      materialized_root=tmp_path / "restore-cache")
    assert restored_result["snapshot_id"] == result["snapshot_id"]
    for path in source.iterdir():
        target = restored / path.name
        assert target.read_bytes() == path.read_bytes()
        assert target.stat().st_mtime_ns == path.stat().st_mtime_ns


@pytest.mark.parametrize("use", ["process", "service"])
def test_capture_blocks_live_consumers_before_staging(tmp_path, monkeypatch, use):
    spec, artifact_root, source = fixture(tmp_path)
    if use == "process":
        monkeypatch.setattr(archive_module, "artifact_process_references", lambda *a: ["active-process"])
    else:
        config = artifact_root.parent / "services/discord_bot/markets/crypto.yaml"
        config.parent.mkdir(parents=True)
        config.write_text("enabled: true\noutput_dir: artifacts/markets/old-research\n")
    with pytest.raises(SnapshotError, match="process references|in use"):
        prepare_archive(spec, artifact_root, manual_capture=True)
    assert not spec.stage_root.exists()
    assert source.is_dir()


def test_capture_rejects_path_added_during_encoding(tmp_path, monkeypatch):
    spec, artifact_root, source = fixture(tmp_path)
    encode = archive_module._encode_one
    def add_path(*args):
        result = encode(*args)
        added = source / "unplanned.pt"
        added.write_bytes(b"do not lose a concurrent result")
        old = time.time_ns() - 86_400_000_000_000
        os.utime(added, ns=(old, old))
        return result
    monkeypatch.setattr(archive_module, "_encode_one", add_path)
    with pytest.raises(SnapshotError, match="changed during staging"):
        prepare_archive(spec, artifact_root, manual_capture=True)
    assert not (spec.stage_root / spec.dataset / "archive/legacy_archive_manifest.json").exists()
    assert (source / "unplanned.pt").is_file()


def test_capture_rejects_already_verified_file_changing_later(tmp_path, monkeypatch):
    spec, artifact_root, source = fixture(tmp_path)
    manifest = prepare_archive(spec, artifact_root, manual_capture=True)
    archive = spec.stage_root / spec.dataset / "archive"
    second_encoded = archive / manifest["files"][1]["encoded_path"]
    sha = archive_module.sha256_file
    def mutate_later(path):
        result = sha(path)
        if path == second_encoded:
            first = source / manifest["files"][0]["path"]
            info = first.stat()
            first.write_bytes(b"x" * info.st_size)
            os.utime(first, ns=(info.st_atime_ns, info.st_mtime_ns))
        return result
    monkeypatch.setattr(archive_module, "sha256_file", mutate_later)
    with pytest.raises(SnapshotError, match="changed during verification"):
        verify_archive_directory(archive, source, spec=spec, manual_capture=True,
                                 artifact_root=artifact_root)


def test_capture_digest_is_bound_to_original_inventory(tmp_path):
    spec, artifact_root, _ = fixture(tmp_path)
    prepare_archive(spec, artifact_root, manual_capture=True)
    archive = spec.stage_root / spec.dataset / "archive"
    path = archive / "legacy_archive_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["source_stability"]["source_plan_sha256"] = "0" * 64
    path.write_text(json.dumps(manifest))
    with pytest.raises(SnapshotError, match="source-plan digest"):
        verify_archive_directory(archive, spec=spec)


def test_manual_capture_retirement_requires_both_explicit_flags_and_restores(tmp_path, monkeypatch):
    from test_legacy_artifact_retirement import _fixture
    old_spec, source, hot, options = _fixture(tmp_path, monkeypatch)
    spec = replace(old_spec, dataset="legacy-manual-retirement", manual_capture_min_stable_hours=12)
    young = time.time_ns() - 86_400_000_000_000
    os.utime(source / "checkpoint.pt", ns=(young, young))
    publish_archive(spec, options["artifact_root"], options["sync_root"],
                    repo_root=options["repo_root"], manual_capture=True)
    import shutil
    backup = tmp_path / "backup"
    shutil.copytree(options["sync_root"], backup, dirs_exist_ok=True)
    with pytest.raises(SnapshotError, match="manual immediate"):
        retirement.plan_legacy_retirement(spec, **options, manual_capture=True)
    plan = retirement.plan_legacy_retirement(spec, **options, manual_immediate=True, manual_capture=True)
    assert plan["apply_ready"]
    result = retirement.apply_legacy_retirement(spec, expected_fingerprint=plan["plan_fingerprint"],
                                               **options, manual_immediate=True, manual_capture=True)
    assert result["deleted"] is True
    assert result["manual_capture"] is True
    assert not source.exists() and not hot.exists()
    restored = tmp_path / "restored"
    restore_archive(spec, options["sync_root"], restored, materialized_root=tmp_path / "restore-cache")
    assert (restored / "checkpoint.pt").read_bytes() == b"old unfinished run; preserve bytes, not deployment permission"
