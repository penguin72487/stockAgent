from dataclasses import replace
import json
import os
from pathlib import Path
import time

import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError
import stockagent.data_sync.legacy_artifact_archive as module
from stockagent.data_sync.legacy_artifact_archive import (
    apply_archive_stage_prune, plan_archive_stage_prune, verify_cold_archive,
)
from stockagent.data_sync.packed_snapshots import resolve_latest_packed


def fixture(tmp_path, monkeypatch):
    from test_legacy_manual_capture import fixture as source_fixture
    spec, artifacts, source = source_fixture(tmp_path)
    packed = tmp_path / "packed"
    release = module.publish_archive(spec, artifacts, packed, repo_root=artifacts.parent,
                                     manual_capture=True)
    source.rename(source.with_name("original-byte-proof-kept-by-test"))
    states = tmp_path / "states"
    state = states / "retirements" / f"{spec.dataset}.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"state": "cold-only", "dataset": spec.dataset,
                                "relative_root": spec.relative_root, "snapshot_id": release["snapshot_id"]}))
    monkeypatch.setattr(module, "C_TEMP_STAGE_ROOT", spec.stage_root)
    import stockagent.data_sync.cold_primary as primary
    monkeypatch.setattr(primary, "_check_d_primary_mount", lambda root: None)
    options = {"artifact_root": artifacts, "state_root": states}
    return spec, packed, options


def test_c_encoding_scratch_is_pruned_only_after_cold_recovery_and_hot_retirement(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    assert result["pruned"] is True and result["cold_objects_deleted"] == 0
    assert not (spec.stage_root / spec.dataset).exists()
    verified = verify_cold_archive(spec, packed)
    assert verified["cold_verified"] and verified["decoded_originals_verified"]
    assert verified["verification_scratch_removed"]
    assert verified["files"] == 2


@pytest.mark.parametrize('failure', [None, 'different-bytes', 'unknown-receipt', 'payload-temp', 'changed-after-audit'])
def test_syncthing_control_temp_requires_exact_fixed_receipt_duplicate(tmp_path, monkeypatch, failure):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    stage = spec.stage_root/spec.dataset
    canonical = next((stage/'receipts').iterdir())
    name = '.syncthing.'+canonical.stem+'.json.123.'+'a'*32+'.tmp'
    extra = stage/'receipts'/name
    extra.write_bytes(canonical.read_bytes())
    if failure=='different-bytes':
        extra.write_bytes(b'X'*extra.stat().st_size)
    elif failure=='unknown-receipt':
        extra.rename(extra.with_name('.syncthing.'+'f'*64+'.json.123.'+'a'*32+'.tmp'))
    elif failure=='payload-temp':
        extra.rename(stage/'archive'/'payload'/name)
    if failure in {'different-bytes','unknown-receipt','payload-temp'}:
        with pytest.raises(SnapshotError):
            plan_archive_stage_prune(spec,packed,**options)
        assert stage.exists()
        return
    plan=plan_archive_stage_prune(spec,packed,**options)
    assert plan['verified_duplicate_control_files']==[{
        'path':extra.relative_to(stage).as_posix(),
        'canonical_receipt':canonical.relative_to(stage).as_posix(),
        'sha256':module.sha256_file(canonical)}]
    if failure=='changed-after-audit':
        info=extra.stat();extra.write_bytes(b'X'*info.st_size)
        os.utime(extra,ns=(info.st_atime_ns,info.st_mtime_ns))
        with pytest.raises(SnapshotError,match='changed after dry run'):
            apply_archive_stage_prune(spec,packed,expected_fingerprint=plan['stage_fingerprint'],
                                      owned_verified_plan=plan,**options)
        assert stage.is_dir()
    else:
        result=apply_archive_stage_prune(spec,packed,expected_fingerprint=plan['stage_fingerprint'],
                                         owned_verified_plan=plan,**options)
        assert result['pruned'] and not stage.exists()
        proof=verify_cold_archive(spec,packed)
        assert proof['decoded_originals_verified'] and proof['files']==2


@pytest.mark.parametrize("gate", ["source", "state", "object", "unknown", "receipt", "pin", "process", "d-mount"])
def test_stage_prune_preserves_bytes_when_any_gate_fails(tmp_path, monkeypatch, gate):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    stage = spec.stage_root / spec.dataset
    if gate == "source":
        (options["artifact_root"] / spec.relative_root).mkdir()
    elif gate == "state":
        path = options["state_root"] / "retirements" / f"{spec.dataset}.json"
        state = json.loads(path.read_text())
        state["state"] = "hot-enrolled"
        path.write_text(json.dumps(state))
    elif gate == "object":
        release = resolve_latest_packed(packed, spec.dataset)
        (packed / release.manifest["archive"]["objects"][0]["relpath"]).write_bytes(b"corruption")
    elif gate == "unknown":
        (stage / "unique-unclassified-data").write_bytes(b"do not drop")
    elif gate == "receipt":
        next((stage / "receipts").iterdir()).write_text("{}")
    elif gate in {"pin", "process"}:
        import stockagent.data_sync.materialized_cache as cache
        if gate == "pin":
            release = resolve_latest_packed(packed, spec.dataset)
            monkeypatch.setattr(cache, "_pinned_snapshot_ids", lambda root: {release.manifest["snapshot_id"]})
        else:
            monkeypatch.setattr(cache, "process_references", lambda root: ["pid=123"])
    else:
        import stockagent.data_sync.cold_primary as primary
        def missing(root):
            raise SnapshotError("missing D")
        monkeypatch.setattr(primary, "_check_d_primary_mount", missing)
    with pytest.raises((SnapshotError, ValueError)):
        plan_archive_stage_prune(spec, packed, **options)
    assert stage.is_dir() and (stage / "archive/legacy_archive_manifest.json").is_file()


def test_stage_prune_rejects_a_changed_dry_run(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    receipt = next((spec.stage_root / spec.dataset / "receipts").iterdir())
    receipt.touch()
    with pytest.raises(SnapshotError, match="changed after dry run"):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    assert (spec.stage_root / spec.dataset).is_dir()


def test_stage_prune_rejects_d_staging_and_non_catalog_roots(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    with pytest.raises(SnapshotError, match="restricted"):
        plan_archive_stage_prune(replace(spec, stage_root=tmp_path / "not-approved"), packed, **options)


def test_cold_only_cli_recovers_directly_when_stage_has_been_pruned(tmp_path, monkeypatch, capsys):
    import sys
    from scripts import manage_legacy_artifact_archives as cli
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    monkeypatch.setattr(cli, "load_legacy_specs", lambda path: {spec.dataset: spec})
    monkeypatch.setattr(sys, "argv", ["manage_legacy_artifact_archives.py", "verify", spec.dataset,
                                     "--cold-only", "--sync-root", str(packed)])
    assert cli.main() == 0
    proof = json.loads(capsys.readouterr().out)
    assert proof["verification_scratch_removed"] and proof["cold_verified"]
    assert proof["source_comparison"] == "not_requested_cold_only"


@pytest.mark.parametrize("mutation", ["unknown-file", "cold-object"])
def test_post_rename_change_keeps_stage_quarantine(tmp_path, monkeypatch, mutation):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    import stockagent.data_sync.cold_primary as primary
    checks = []
    def mutate_after_rename(root):
        checks.append(root)
        if len(checks) == 2:
            quarantine = next(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))
            if mutation == "unknown-file":
                (quarantine / "new-unique-data").write_bytes(b"keep")
            else:
                release = resolve_latest_packed(packed, spec.dataset)
                item = packed / release.manifest["archive"]["objects"][0]["relpath"]
                info = item.stat()
                item.write_bytes(b"x" * info.st_size)
    monkeypatch.setattr(primary, "_check_d_primary_mount", mutate_after_rename)
    with pytest.raises(SnapshotError):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"], **options)
    quarantine = next(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))
    assert (quarantine / "archive/legacy_archive_manifest.json").is_file()


def test_owned_stage_plan_reuses_full_d_proof_and_still_decodes_the_quarantine(tmp_path, monkeypatch):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    calls = []
    verify = module.verify_packed_snapshot
    def counted(*a, **k):
        calls.append(k)
        return verify(*a, **k)
    monkeypatch.setattr(module, "verify_packed_snapshot", counted)
    plan = plan_archive_stage_prune(spec, packed, **options)
    assert len(calls) == 1
    result = apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"],
                                     owned_verified_plan=plan, **options)
    assert result["pruned"] and len(calls) == 1
    assert verify_cold_archive(spec, packed)["decoded_originals_verified"]


@pytest.mark.parametrize("gate", ["expired", "future", "proof-contract", "cold-object", "source",
                                   "state", "pin", "process", "redirected", "unknown-file", "local-bytes"])
def test_owned_stage_proof_never_bypasses_fresh_retirement_or_content_gates(tmp_path, monkeypatch, gate):
    spec, packed, options = fixture(tmp_path, monkeypatch)
    plan = plan_archive_stage_prune(spec, packed, **options)
    stage = spec.stage_root / spec.dataset
    if gate in {"expired", "future"}:
        plan["full_verification"]["verified_at_epoch"] = time.time() + (10 if gate == "future" else -1501)
    elif gate == "proof-contract":
        plan["full_verification"]["contract"] = "stat-only"
    elif gate == "cold-object":
        release = resolve_latest_packed(packed, spec.dataset)
        item = packed / release.manifest["archive"]["objects"][0]["relpath"]
        observed = item.stat()
        item.write_bytes(b"X" * observed.st_size)
        os.utime(item, ns=(observed.st_atime_ns, observed.st_mtime_ns))
    elif gate == "source":
        (options["artifact_root"] / spec.relative_root).mkdir()
    elif gate == "state":
        path = options["state_root"] / "retirements" / f"{spec.dataset}.json"
        value = json.loads(path.read_text()); value["state"] = "retiring"
        path.write_text(json.dumps(value))
    elif gate in {"pin", "process"}:
        import stockagent.data_sync.materialized_cache as cache
        if gate == "pin":
            monkeypatch.setattr(cache, "_pinned_snapshot_ids", lambda root: {plan["snapshot_id"]})
        else:
            monkeypatch.setattr(cache, "process_references", lambda root: ["pid=123"])
    elif gate == "redirected":
        saved = stage.with_name("redirected-stage-kept")
        stage.rename(saved); stage.symlink_to(saved, target_is_directory=True)
    elif gate == "unknown-file":
        (stage / "unknown-source").write_bytes(b"preserve")
    else:
        # Change bytes only after the before-rename signature check. A fresh
        # full decode must still catch them, even with the original mtime.
        rename = module.os.replace
        def mutate_after_rename(source, target):
            rename(source, target)
            if source == stage:
                file = next((Path(target) / "archive/payload").rglob("*.raw"))
                observed = file.stat(); file.write_bytes(b"X" * observed.st_size)
                os.utime(file, ns=(observed.st_atime_ns, observed.st_mtime_ns))
        monkeypatch.setattr(module.os, "replace", mutate_after_rename)
    with pytest.raises((SnapshotError, ValueError)):
        apply_archive_stage_prune(spec, packed, expected_fingerprint=plan["stage_fingerprint"],
                                 owned_verified_plan=plan, **options)
    assert stage.exists() or list(spec.stage_root.glob(f".{spec.dataset}.pruning-*"))
