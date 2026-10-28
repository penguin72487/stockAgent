"""Explicit original-backup recovery retains the exact model/output ABI."""
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import zipfile

import pytest

from stockagent import training_rescue_worker as worker
from stockagent.training_rescue import input_source_readback_ready

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def preserved(tmp_path):
    root = tmp_path / "artifacts/data_preparation/recovery/features"
    root.mkdir(parents=True)
    originals = {"common.npy.zst": b"exact model -0 NaN and null masks",
                 "individual_0.npy.zst": b"exact individual Float32 bits",
                 "feature_dictionary.json": b'{"features":["raw"],"excluded":[1,2]}',
                 "observations/000.parquet": b"original nullable observations with duplicates",
                 "annual_events/2025.parquet": b"original annual order and exact raw values"}
    auxiliary = {name: raw for name, raw in originals.items() if not name.endswith(".npy.zst")}
    for name, raw in originals.items():
        p = root / name
        p.parent.mkdir(exist_ok=True)
        p.write_bytes(raw)
    manifest = {"contract": "tw_factorized_nullable_panel_values_lag1_v2", "status": "complete",
                "stock_context_preparation": {"futures_context_only": True},
                "historical_point_in_time": False, "live_eligible": False,
                "common": {"path": "common.npy.zst", "sha256": worker.file_hash(root / "common.npy.zst")},
                "blocks": [{"path": "individual_0.npy.zst", "sha256": worker.file_hash(root / "individual_0.npy.zst")}],
                "feature_dictionary_sha256": worker.file_hash(root / "feature_dictionary.json"),
                "observations": [{"path": "observations/000.parquet", "rows": 3,
                                  "sha256": worker.file_hash(root / "observations/000.parquet")}],
                "annual_events": [{"path": "annual_events/2025.parquet",
                                   "sha256": worker.file_hash(root / "annual_events/2025.parquet")}],
                "build_wall_seconds": 3600.0}
    reference = tmp_path / "references/original.json"
    worker.atomic_json(reference, manifest)
    rebuilt = root / "factorized_manifest.json"
    current = copy.deepcopy(manifest)
    for name, raw in auxiliary.items():
        (root / name).write_bytes(raw + b"new build ordering")
    current["feature_dictionary_sha256"] = worker.file_hash(root / "feature_dictionary.json")
    for key in ("observations", "annual_events"):
        for row in current[key]:
            row["sha256"] = worker.file_hash(root / row["path"])
    current["build_wall_seconds"] = 3.0
    worker.atomic_json(rebuilt, current)
    index = {"contract": worker.PRESERVED_AUXILIARY_CONTRACT,
             "original_manifest_sha256": worker.file_hash(reference),
             "files": [{"relative": name, "sha256": worker.hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
                       for name, raw in auxiliary.items()]}
    bundle = tmp_path / "references/auxiliary.zip"
    row = {"step_index": 0, "kind": worker.PRESERVED_AUXILIARY_KIND, "reference_path": str(reference),
           "reference_sha256": worker.file_hash(reference), "rebuilt_manifest": str(rebuilt)}
    def repack(*, damaged=None, extra=False, duplicate=False, override_index=None):
        with zipfile.ZipFile(bundle, "w") as archive:
            for name, raw in auxiliary.items():
                archive.writestr(name, raw + b"corruption" if damaged == name else raw)
            archive.writestr("ORIGINAL_AUXILIARY.json", json.dumps(override_index or index))
            if extra:
                archive.writestr("../escape", b"forbidden")
            if duplicate:
                with pytest.warns(UserWarning):
                    archive.writestr(next(iter(auxiliary)), b"duplicate")
        row["preserved_auxiliary_bundle"] = {"path": str(bundle), "sha256": worker.file_hash(bundle),
                                              "bytes": bundle.stat().st_size}
    repack()
    spec = {"schema_version": 1, "contract": worker.CONTRACT, "run_id": "preserved-test",
            "source_root": str(ROOT), "runtime_prefix": sys.prefix, "config_file": str(ROOT / "README.md"),
            "output_root": str(tmp_path / "artifacts/markets/result"), "world_size": 1, "global_batch": 1,
            "environment": {}, "data_sources": [{"dataset": "stock", "snapshot_id": "stock-fixed",
                "manifest_sha256": "1" * 64, "materialized_root": str(tmp_path / "inputs")}],
            "rebuild_steps": [["scripts/prepare_tw_day_trade_factorized_panel.py", "--output-root", str(root)]],
            "prepared_reconciliation": [row]}
    spec["train_argv"] = ["train.py", "--config", spec["config_file"], "--output-dir", spec["output_root"], "--resume"]
    return root, originals, auxiliary, manifest, current, index, row, spec, repack


def test_exact_original_auxiliary_recovery_then_unchanged_second_readback(preserved):
    root, originals, _, _, _, _, ref, _, _ = preserved
    model_before = {p: worker.signature(root / p) for p in originals if p.endswith(".npy.zst")}
    result = worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    assert result["restored_auxiliary_files"] == 3
    assert result["recomputed_model_files"] == 2
    assert not result["auxiliary_values_claimed_recomputed"]
    assert not result["fresh_gpu_acceptance_claim"]
    assert {name: (root / name).read_bytes() for name in originals} == originals
    assert (root / "factorized_manifest.json").read_bytes() == Path(ref["reference_path"]).read_bytes()
    before = {p: worker.signature(root / p) for p in [*originals, "factorized_manifest.json"]}
    again = worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    assert again["restored_auxiliary_files"] == 0
    assert {p: worker.signature(root / p) for p in before} == before
    assert all(worker.signature(root / p) == s for p, s in model_before.items())


@pytest.mark.parametrize("damage", ["model", "source_abi", "point_in_time", "raw_count", "undeclared_auxiliary",
                                    "bundle_sha", "missing", "extra", "duplicate", "index_sha", "bool_bytes",
                                    "member_sha", "escape", "wrong_scope", "file_symlink"])
def test_nothing_is_replaced_before_every_original_and_model_is_admitted(preserved, damage):
    root, originals, _, _, current, index, ref, _, repack = preserved
    rebuilt = root / "factorized_manifest.json"
    if damage == "model":
        (root / "common.npy.zst").write_bytes(b"model values changed")
    elif damage in {"source_abi", "point_in_time", "raw_count"}:
        if damage == "source_abi":current["new_source"] = "changed"
        elif damage == "point_in_time":current["historical_point_in_time"] = True
        else:current["observations"][0]["rows"] += 1
        worker.atomic_json(rebuilt, current)
    elif damage == "undeclared_auxiliary":
        (root / "feature_dictionary.json").write_bytes(b"not the admitted builder result")
    elif damage == "bundle_sha":ref["preserved_auxiliary_bundle"]["sha256"] = "f" * 64
    elif damage == "missing":(root / "observations/000.parquet").unlink()
    elif damage == "extra":repack(extra=True)
    elif damage == "duplicate":repack(duplicate=True)
    elif damage in {"index_sha", "bool_bytes", "escape", "wrong_scope"}:
        invalid = copy.deepcopy(index)
        if damage == "index_sha":invalid["original_manifest_sha256"] = "f" * 64
        elif damage == "bool_bytes":invalid["files"][0]["bytes"] = True
        elif damage == "escape":invalid["files"][0]["relative"] = "../escape"
        else:invalid["contract"] = "another-contract"
        repack(override_index=invalid)
    elif damage == "member_sha":repack(damaged="annual_events/2025.parquet")
    else:
        path = root / "common.npy.zst"
        path.rename(root / "outside")
        path.symlink_to(root / "outside")
    before = {name: (root / name).read_bytes() for name in originals if (root / name).is_file()}
    manifest_before = rebuilt.read_bytes()
    with pytest.raises((ValueError, FileNotFoundError)):
        worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    assert {name: (root / name).read_bytes() for name in before} == before
    assert rebuilt.read_bytes() == manifest_before


def test_same_original_artifact_different_immutable_recovery_recipe(preserved):
    _, _, _, _, _, _, ref, spec, _ = preserved
    assert worker.validate_spec(spec) == spec
    old = copy.deepcopy(spec)
    old_ref = old["prepared_reconciliation"][0]
    old_ref.pop("preserved_auxiliary_bundle")
    old_ref["kind"] = "factorized_manifest_v1"
    assert worker.prepared_build_identity(spec, 0) == worker.prepared_build_identity(old, 0)
    assert worker.digest(spec) != worker.digest(old)
    before = Path(ref["rebuilt_manifest"]).read_bytes()
    with pytest.raises(ValueError, match="ABI/member inventory"):
        worker.reconcile_prepared(Path(ref["reference_path"]), Path(ref["rebuilt_manifest"]), source_root=ROOT)
    assert Path(ref["rebuilt_manifest"]).read_bytes() == before


def test_preserved_bundle_is_a_required_source_readiness_resource(preserved):
    _, _, _, _, _, _, ref, spec, _ = preserved
    bindings = worker.preparation_resource_bindings(spec)
    expected = {row["path"]: row["sha256"] for row in bindings}
    assert expected[ref["preserved_auxiliary_bundle"]["path"]] == ref["preserved_auxiliary_bundle"]["sha256"]
    proof = {"spec_sha256": worker.digest(spec), "level": "all_original_file_SHA_decoded",
             "prepared_reference_sha256": expected}
    assert input_source_readback_ready(spec, {"input_verification": proof})
    del expected[ref["preserved_auxiliary_bundle"]["path"]]
    assert not input_source_readback_ready(spec, {"input_verification": proof})


def test_existing_financial_representation_bundle_is_also_required_for_readiness(preserved):
    root, _, _, _, _, _, ref, spec, _ = preserved
    bundle = ref.pop("preserved_auxiliary_bundle")
    ref.pop("rebuilt_manifest")
    ref.update(kind="file_set_v2", rebuilt_root=str(root), representation_bundle=bundle)
    expected = {row["path"]: row["sha256"] for row in worker.preparation_resource_bindings(spec)}
    proof = {"spec_sha256": worker.digest(spec), "level": "all_original_file_SHA_decoded",
             "prepared_reference_sha256": expected}
    assert input_source_readback_ready(spec, {"input_verification": proof})
    expected.pop(bundle["path"])
    assert not input_source_readback_ready(spec, {"input_verification": proof})


def test_preserved_payload_uses_its_own_bound_not_the_small_json_bound(preserved, tmp_path, monkeypatch):
    from stockagent import training_rescue as controller
    import stockagent.data_sync.windows_cold_io as native
    _, _, _, _, _, _, ref, spec, _ = preserved
    path = Path(ref["preserved_auxiliary_bundle"]["path"])
    with zipfile.ZipFile(path, "a") as archive:
        archive.comment = b"original archive encoding" * 400
    ref["preserved_auxiliary_bundle"].update(sha256=worker.file_hash(path), bytes=path.stat().st_size)
    resources = worker.preparation_resource_bindings(spec)
    for row in resources:
        (tmp_path / row["sha256"]).write_bytes(Path(row["path"]).read_bytes())
    owner = SimpleNamespace(guard=lambda: None, object_path=lambda sha: tmp_path / sha, object_hash=worker.file_hash)
    monkeypatch.setattr(controller, "MAX_PREPARED_REFERENCE_BYTES", 4096)
    monkeypatch.setattr(native, "windows_path", lambda path: None)
    proof = controller.Rescue.prepared_reference_objects(owner, spec)
    assert {r["reference_path"]: r["sha256"] for r in proof} == {r["path"]: r["sha256"] for r in resources}


def test_interrupted_original_install_reuses_the_exact_staged_payload(preserved, monkeypatch):
    root, originals, auxiliary, _, _, _, ref, _, _ = preserved
    replace = worker.os.replace
    installed = []
    def interrupt(source, target):
        if str(target) in {str(root / name) for name in auxiliary}:
            if installed:
                raise OSError("injected interruption after first exact original")
            installed.append(str(target))
        return replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(worker.os, "replace", interrupt)
        with pytest.raises(OSError, match="injected interruption"):
            worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    journals = list(root.parent.glob(".rescue-preserved-auxiliary-*"))
    assert len(journals) == 1
    staged = {p: worker.signature(p) for p in journals[0].iterdir() if p.name != "RESTORATION.private.json"}
    assert len(staged) == 2
    result = worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    assert result["restored_auxiliary_files"] == 2
    assert {name: (root / name).read_bytes() for name in originals} == originals
    assert list(root.parent.glob(".rescue-preserved-auxiliary-*")) == journals
    assert {p.name for p in journals[0].iterdir()} == {"RESTORATION.private.json"}


@pytest.mark.parametrize("damage", ["empty_prefix", "valid_prefix", "corrupt_prefix", "oversized", "hardlink", "no_growth"])
def test_partial_auxiliary_stage_resumes_verified_bytes_without_a_second_copy(preserved, monkeypatch, damage):
    root, originals, auxiliary, _, _, _, ref, _, _ = preserved
    replace = worker.os.replace
    def stop_before_install(source, target):
        if str(target) in {str(root / name) for name in auxiliary}:
            raise OSError("injected stop before first install")
        return replace(source, target)
    with monkeypatch.context() as patch:
        patch.setattr(worker.os, "replace", stop_before_install)
        with pytest.raises(OSError):
            worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
    journal, = root.parent.glob(".rescue-preserved-auxiliary-*")
    stage = next(p for p in journal.iterdir() if p.name != "RESTORATION.private.json")
    full = stage.read_bytes()
    if damage == "empty_prefix":stage.write_bytes(b"")
    elif damage == "valid_prefix":stage.write_bytes(full[:len(full) // 2])
    elif damage == "corrupt_prefix":stage.write_bytes(b"wrong prefix")
    elif damage == "oversized":stage.write_bytes(full + b"oversized")
    elif damage == "hardlink":worker.os.link(stage, stage.with_name("unowned-alias"))
    else:
        monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=25 * 1024**3 + 1))
    before = {name: (root / name).read_bytes() for name in originals}
    if damage in {"corrupt_prefix", "oversized", "hardlink"}:
        with pytest.raises(ValueError):
            worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
        assert {name: (root / name).read_bytes() for name in originals} == before
    else:
        result = worker.reconcile_preserved_factorized_auxiliary(ref, source_root=ROOT)
        assert result["restored_auxiliary_files"] == 3
        assert {name: (root / name).read_bytes() for name in originals} == originals
        assert {p.name for p in journal.iterdir()} == {"RESTORATION.private.json"}
    assert list(root.parent.glob(".rescue-preserved-auxiliary-*")) == [journal]


@pytest.mark.parametrize("damage", ["missing", "bool", "oversized", "inside_output", "legacy", "mixed", "conflict"])
def test_explicit_resource_enrollment_is_bounded_and_unambiguous(preserved, damage):
    root, _, _, _, _, _, ref, spec, _ = preserved
    bundle = ref["preserved_auxiliary_bundle"]
    if damage == "missing":del ref["preserved_auxiliary_bundle"]
    elif damage == "bool":bundle["bytes"] = True
    elif damage == "oversized":bundle["bytes"] = worker.MAX_PRESERVED_AUXILIARY_BYTES + 1
    elif damage == "inside_output":bundle["path"] = str(root / "private.zip")
    elif damage == "legacy":ref["kind"] = "factorized_manifest_v1"
    elif damage == "mixed":spec["prepared_reconciliation"].append(copy.deepcopy(ref))
    else:
        spec["input_controls"] = [{**bundle, "sha256": "e" * 64}]
    with pytest.raises(ValueError):
        worker.preparation_resource_bindings(spec)
