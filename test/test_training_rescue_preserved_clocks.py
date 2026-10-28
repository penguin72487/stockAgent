"""Original float bits survive cold recovery; failed verification is terminal.

Tiny real ZIPs and CPU child processes exercise the canonical prepared owner.
No market coverage or GPU continuation is inferred from these protocol checks.
"""
import copy
import hashlib
import json
from pathlib import Path
import stat
from types import SimpleNamespace
import zipfile

import pytest

import stockagent.training_rescue_worker as worker
from test_training_rescue_preparation import oracle_fixture
from test_training_rescue_shared_preparation import shared


def pack(spec, originals, *, mutate=None):
    ref = spec["prepared_reconciliation"][0]
    rows = [{"relative": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
            for name, raw in originals.items()]
    control = {"contract": worker.PRESERVED_CLOCK_FILE_SET,
               "original_file_set_oracle_sha256": ref["reference_sha256"],
               "artifact_kind": "tw_futures_decision_clocks_v1", "files": rows}
    members = {"objects/" + row["sha256"]: originals[row["relative"]] for row in rows}
    if mutate:
        mutate(control, members)
    members["ORIGINAL_CLOCK_FILES.json"] = json.dumps(control).encode()
    path = Path(ref["reference_path"]).with_name("clock-originals.zip")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, raw in members.items():
            archive.writestr(name, raw)
    ref["original_file_set_bundle"] = {"path": str(path), "sha256": worker.file_hash(path),
                                       "bytes": path.stat().st_size}
    return path


@pytest.fixture
def clocks(tmp_path, monkeypatch):
    spec, path, root, oracle, originals = oracle_fixture(tmp_path, "tw_futures_decision_clocks_v1")
    # One content object serves two distinct original logical names.
    raw = originals["release/rules/rules.parquet"]
    originals["0900/identical.parquet"] = raw
    oracle["files"].append({"relative": "0900/identical.parquet", "bytes": len(raw),
                            "sha256": hashlib.sha256(raw).hexdigest()})
    ref = spec["prepared_reconciliation"][0]
    worker.atomic_json(Path(ref["reference_path"]), oracle)
    ref["reference_sha256"] = worker.file_hash(Path(ref["reference_path"]))
    ref["kind"] = worker.PRESERVED_FILE_SET_KIND
    spec["prepared_build_policy"] = worker.PREPARED_BUILD_POLICY
    pack(spec, originals)
    worker.atomic_json(path, spec)
    controls = tmp_path / "shared-clock-control"
    monkeypatch.setattr(worker, "PREPARED_BUILD_ROOT", controls)
    monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * 1024**3))
    def no_builder(*args, **kwargs):
        raise AssertionError("clock recovery must never re-run the raw float reduction")
    monkeypatch.setattr(worker.subprocess, "Popen", no_builder)
    return spec, path, root, oracle, originals, controls


def state(clocks):
    _, path, root, _, _, controls = clocks
    return (json.loads((controls / (worker.digest(str(root)) + ".json")).read_bytes()),
            json.loads((path.parent / "rebuild/0.json").read_bytes()))


def test_exact_original_restore_and_cross_run_reuse_with_full_readback(clocks, tmp_path, monkeypatch):
    spec, path, root, _, originals, _ = clocks
    first = worker.prepare_run(path, 0)
    assert not first["builder_executed"] and first["skip_builder"]
    assert first["original_files_restored"] == len(originals)
    assert first["prepared_recovery_method"] == "preserved_original_file_set"
    assert {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()} == originals
    claim, marker = state(clocks)
    assert claim["state"] == marker["state"] == "complete"
    assert not marker["proofs"][0]["restored_json_receipts"]
    def no_decode(*args):
        raise AssertionError("complete original files must be reused")
    monkeypatch.setattr(worker, "_restore_preserved_clock_files", no_decode)
    other = copy.deepcopy(spec); other["run_id"] = "another-original-clock"
    second_path = tmp_path / "another-controller/spec.json"; worker.atomic_json(second_path, other)
    reused = worker.prepare_run(second_path, 0)
    assert reused["fresh_full_original_readback"] and reused["original_files_restored"] == 0
    assert reused["artifact_sha256"] == first["artifact_sha256"]
    (root / "0900/manifest.json").write_bytes(b'{ "rows":3,"clock":"09:00","source_coverage_complete":false }')
    with pytest.raises(ValueError, match="SHA/size"):
        worker.prepare_run(path, 0)
    failed, _ = state(clocks)
    evidence = json.loads(Path(failed["failure_path"]).read_bytes())
    assert evidence["previous_claim"] == claim and evidence["contract"] == "stockagent.prepared_build_failure.v1"
    assert evidence["previous_run_marker"] == marker


@pytest.mark.parametrize("damage", ["one_float_bit", "missing", "extra", "index_sha", "index_bytes",
                                     "index_duplicate", "index_relative", "index_extra", "object_size"])
def test_wrong_content_and_inventories_fail_without_completion(clocks, damage):
    spec, path, root, _, originals, _ = clocks
    def change(control, members):
        row = control["files"][0]; key = "objects/" + row["sha256"]
        if damage == "one_float_bit":
            raw = bytearray(members[key]); raw[0] ^= 1; members[key] = bytes(raw)
        elif damage == "missing": members.pop(key)
        elif damage == "extra": members["../escape"] = b"reject"
        elif damage == "index_sha": control["original_file_set_oracle_sha256"] = "0" * 64
        elif damage == "index_bytes": row["bytes"] = True
        elif damage == "index_duplicate": control["files"][1] = dict(row)
        elif damage == "index_relative": row["relative"] = "../escape"
        elif damage == "index_extra": row["unknown"] = "reject"
        else: members[key] += b"wrong size"
    pack(spec, originals, mutate=change); worker.atomic_json(path, spec)
    with pytest.raises(ValueError): worker.prepare_run(path, 0)
    claim, marker = state(clocks)
    assert claim["state"] == marker["state"] == "failed"
    assert worker.file_hash(Path(claim["failure_path"])) == claim["failure_sha256"]
    if damage != "one_float_bit": assert not root.exists()


@pytest.mark.parametrize("damage", ["duplicate", "symlink", "crc", "bundle_sha", "capacity"])
def test_archive_safety_and_capacity_keep_owners_failed(clocks, monkeypatch, damage):
    spec, path, root, _, _, _ = clocks
    ref = spec["prepared_reconciliation"][0]; bundle = ref["original_file_set_bundle"]
    archive_path = Path(bundle["path"])
    if damage in {"duplicate", "symlink"}:
        with zipfile.ZipFile(archive_path, "a") as archive:
            name = next(n for n in archive.namelist() if n.startswith("objects/"))
            if damage == "duplicate":
                with pytest.warns(UserWarning, match="Duplicate"):
                    archive.writestr(name, b"same name forbidden")
            else:
                # Replace an object's external type without changing its bytes.
                archive.getinfo(name).external_attr = (stat.S_IFLNK | 0o777) << 16
                archive.comment = b"write changed central directory"
    elif damage == "crc":
        with zipfile.ZipFile(archive_path, "a") as archive:
            info = next(i for i in archive.infolist() if i.filename.startswith("objects/"))
            info.CRC ^= 1
            archive.comment = b"write changed CRC"
    elif damage == "capacity":
        monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=25 * 1024**3))
    bundle.update(sha256=worker.file_hash(archive_path), bytes=archive_path.stat().st_size)
    if damage == "bundle_sha": bundle["sha256"] = "0" * 64
    worker.atomic_json(path, spec)
    with pytest.raises((ValueError, zipfile.BadZipFile, RuntimeError)):
        worker.prepare_run(path, 0)
    claim, marker = state(clocks)
    assert claim["state"] == marker["state"] == "failed"
    if damage != "crc": assert not root.exists()


@pytest.mark.parametrize("damage", ["policy", "legacy", "mixed", "inside_output", "bad_size", "financial"])
def test_method_is_explicit_and_clock_only_before_any_output(clocks, damage):
    spec, path, root, _, _, _ = clocks
    ref = spec["prepared_reconciliation"][0]
    if damage == "policy": spec.pop("prepared_build_policy")
    elif damage == "legacy": ref["kind"] = "file_set_v1"
    elif damage == "mixed": ref["representation_bundle"] = dict(ref["original_file_set_bundle"])
    elif damage == "inside_output": ref["original_file_set_bundle"]["path"] = str(root / "originals.zip")
    elif damage == "bad_size": ref["original_file_set_bundle"]["bytes"] = True
    else: spec["rebuild_steps"][0][0] = worker.PREPARED_BUILDERS["tw_futures_financial_v1"]
    worker.atomic_json(path, spec)
    with pytest.raises(ValueError): worker.prepare_run(path, 0)
    assert not root.exists()


def test_restore_cannot_adopt_an_existing_foreign_root(clocks):
    _, path, root, _, _, _ = clocks
    root.mkdir(parents=True); (root / "private.bin").write_bytes(b"foreign")
    with pytest.raises(ValueError, match="unowned"): worker.prepare_run(path, 0)
    assert (root / "private.bin").read_bytes() == b"foreign"


@pytest.mark.parametrize("damage", ["prefix", "too_many", "large_member", "large_total"])
def test_decoded_file_scope_is_bounded_before_writes(clocks, damage):
    spec, path, root, oracle, _, _ = clocks
    rows = oracle["files"]
    if damage == "prefix": rows.append({"relative": "0900", "bytes": 0, "sha256": "a" * 64})
    elif damage == "large_member": rows[0]["bytes"] = 64 * 1024**2 + 1
    else:
        count = 1025 if damage == "too_many" else 10
        oracle["files"] = [{"relative": f"{i}.parquet", "bytes": 64 * 1024**2, "sha256": "a" * 64}
                           for i in range(count)]
    ref = spec["prepared_reconciliation"][0]
    worker.atomic_json(Path(ref["reference_path"]), oracle)
    ref["reference_sha256"] = worker.file_hash(Path(ref["reference_path"]))
    worker.atomic_json(path, spec)
    with pytest.raises(ValueError): worker.prepare_run(path, 0)
    assert not root.exists()


def test_changing_archive_generation_during_restore_preserves_failed_owned_tree(clocks, monkeypatch):
    spec, path, root, _, _, _ = clocks
    archive = Path(spec["prepared_reconciliation"][0]["original_file_set_bundle"]["path"])
    original = worker._sync_receive_parents
    changed = []
    def touch(chain):
        original(chain)
        if root.exists() and not changed:
            archive.touch(); changed.append(True)
    monkeypatch.setattr(worker, "_sync_receive_parents", touch)
    with pytest.raises(ValueError, match="resources or output root changed"):
        worker.prepare_run(path, 0)
    claim, marker = state(clocks)
    assert changed and claim["state"] == marker["state"] == "failed" and root.is_dir()


def test_new_bundle_is_in_common_D_resource_and_readiness_census(clocks, tmp_path, monkeypatch):
    from stockagent.training_rescue import Rescue, RescueError, input_source_readback_ready
    spec, _, _, _, _, _ = clocks
    cas = tmp_path / "cold-objects"; cas.mkdir()
    owner = object.__new__(Rescue)
    monkeypatch.setattr(owner, "guard", lambda: None)
    monkeypatch.setattr(owner, "object_path", lambda sha: cas / sha)
    monkeypatch.setattr(owner, "object_hash", worker.file_hash)
    bindings = worker.preparation_resource_bindings(spec)
    for row in bindings:
        (cas / row["sha256"]).write_bytes(Path(row["path"]).read_bytes())
    verified = owner.prepared_reference_objects(spec)
    assert {x["reference_path"] for x in verified} == {x["path"] for x in bindings}
    spec["data_sources"] = [{"dataset": "originals", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                              "materialized_root": str(tmp_path / "original-source")}]
    source = {"spec_sha256": worker.digest(spec), "level": "all_original_file_SHA_decoded",
              "prepared_reference_sha256": {x["path"]: x["sha256"] for x in bindings}}
    assert input_source_readback_ready(spec, {"input_verification": source})
    bundle = spec["prepared_reconciliation"][0]["original_file_set_bundle"]
    source["prepared_reference_sha256"].pop(bundle["path"])
    assert not input_source_readback_ready(spec, {"input_verification": source})
    (cas / bundle["sha256"]).write_bytes(b"x" * bundle["bytes"])
    with pytest.raises(RescueError, match="SHA readback"): owner.prepared_reference_objects(spec)


def test_real_builder_zero_exit_bad_bytes_are_failed_then_retry_keeps_evidence(shared):
    spec, path, root, source, controls, originals = shared
    builder = source / spec["rebuild_steps"][0][0]
    builder.write_text(builder.read_text() +
        "if (source/'corrupt-output').exists():(root/'lifetime_scope.parquet').write_bytes(b'wrong')\n")
    ref = spec["prepared_reconciliation"][0]
    oracle = json.loads(Path(ref["reference_path"]).read_bytes()); oracle["builder_sha256"] = worker.file_hash(builder)
    worker.atomic_json(Path(ref["reference_path"]), oracle)
    ref["reference_sha256"] = worker.file_hash(Path(ref["reference_path"])); worker.atomic_json(path, spec)
    (source / "corrupt-output").touch()
    with pytest.raises(ValueError, match="SHA/size"): worker.prepare_run(path, 0)
    claim_path = controls / (worker.digest(str(root)) + ".json")
    failed = json.loads(claim_path.read_bytes())
    marker = json.loads((path.parent / "rebuild/0.json").read_bytes())
    assert failed["state"] == marker["state"] == "failed"
    assert failed["phase"] == "fresh_original_readback"
    failure_path = Path(failed["failure_path"]); failure_sha = worker.file_hash(failure_path)
    log_sha = worker.file_hash(Path(failed["log_path"]))
    (source / "corrupt-output").unlink()
    assert worker.prepare_run(path, 0)["builder_executed"]
    quarantines = list(root.parent.glob(root.name + ".rescue-incomplete-*"))
    assert len(quarantines) == 1 and (quarantines[0] / "lifetime_scope.parquet").read_bytes() == b"wrong"
    assert worker.file_hash(failure_path) == failure_sha
    assert worker.file_hash(Path(failed["log_path"])) == log_sha
    assert {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()} == originals


def test_replaced_owner_cannot_write_failed_state(clocks, monkeypatch):
    _, path, root, _, _, controls = clocks
    claim_path = controls / (worker.digest(str(root)) + ".json")
    def changed(*args):
        lock = controls / (worker.digest(str(root)) + ".lock")
        lock.rename(lock.with_suffix(".old-owner")); lock.touch()
        worker.atomic_json(claim_path, {"state": "foreign"})
        raise ValueError("deliberate restore failure")
    monkeypatch.setattr(worker, "_restore_preserved_clock_files", changed)
    with pytest.raises(ValueError, match="lock inode changed|received file changed"): worker.prepare_run(path, 0)
    assert json.loads(claim_path.read_bytes()) == {"state": "foreign"}
    assert not list(controls.glob("*.failure-*.private.json"))
