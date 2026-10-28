"""Bind preserved runtime/code to the original optimizer enrollment, without GPU IO."""
import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import zipfile

import pytest

from scripts.inventory_training_rescue import freeze_inventory
from scripts.verify_training_rescue import verify_restored_plan
from stockagent.runtime_identity import verify_source_release
from stockagent.training_rescue import RescueError, SourceIdentityChanged
from stockagent.training_rescue_bootstrap import (
    BOOTSTRAP_BINDING_V2, BOOTSTRAP_REQUEST_V1, bootstrap_runtime_lock,
    decode_runtime_capsule, validate_bootstrap_binding,
)
from stockagent.training_rescue_worker import (
    atomic_json, digest, file_hash, freeze_runtime, signature, verify_runtime_capsule,
)
from test_training_rescue import formal, rescue, enroll_fixture, LocalSource


def object_row(path, role=None):
    row = {"source_path": str(path), "sha256": file_hash(path), "bytes": path.stat().st_size}
    if role:
        row.update(target_path=str(path), role=role)
    return row


def runtime_fixture(tmp_path):
    prefix = tmp_path / "fixed-runtime"
    (prefix / "lib").mkdir(parents=True)
    (prefix / "bin").mkdir()
    value = prefix / "lib/payload.bin"
    value.write_bytes(b"preserved runtime payload\x00" * 4)
    os.link(value, prefix / "lib/payload-hardlink.bin")
    (prefix / "bin/link").symlink_to("../lib//payload.bin")
    archive = tmp_path / "runtime.tar.gz"
    freeze_runtime(prefix, archive)
    proof_path = Path(str(archive) + ".json")
    return prefix, archive, proof_path


@pytest.fixture
def binding_case(rescue, formal, tmp_path, monkeypatch):
    spec = formal[0]
    # Use the canonical already-imported checkpoint loader. No repository file
    # is written; this tiny release binds one existing source member only.
    source_root = Path(__file__).resolve().parents[1]
    spec["source_root"] = str(source_root)
    bundle, wheel, receipt = (tmp_path / n for n in ("source.zip", "source.whl", "release.json"))
    with zipfile.ZipFile(bundle, "w") as z:
        z.write(source_root / "train.py", "train.py")
    wheel.write_bytes(b"closed source wheel")
    members = {"train.py": file_hash(source_root / "train.py")}
    atomic_json(receipt, {"schema_version": 1, "state": "built",
        "code": {"source_sha256": digest(members), "files": members, "git_head": "fixture"},
        "source_bundle": {"file": bundle.name, "files": members, "sha256": file_hash(bundle), "bytes": bundle.stat().st_size},
        "wheel": {"file": wheel.name, "sha256": file_hash(wheel), "bytes": wheel.stat().st_size}})
    release = verify_source_release(receipt, source_root)
    runtime = {"schema_version": 1, "python": "3.11.fixture", "implementation": "CPython",
        "system": "Linux", "machine": "x86_64", "packages": {"fixture": "1"}, "duplicate_distribution_versions": {}}
    runtime["sha256"] = digest(runtime)
    prefix, archive, proof_path = runtime_fixture(tmp_path)
    config = Path(spec["config_file"])
    original_config = object_row(config, "training_config")
    original_config.pop("bytes")  # Original checkpoint-only enrollment lacked sizes.
    spec.update(source_receipt=str(receipt), source_release=release,
                runtime_prefix=str(prefix), bootstrap_files=[original_config])
    manifest = Path(spec["output_root"]) / "run_manifest.json"
    value = json.loads(manifest.read_bytes())
    value["runtime_provenance"] = {"runtime": runtime, "source_release": release}
    atomic_json(manifest, value)
    enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture(spec["run_id"])
    state = rescue.journal(spec["run_id"])
    state["source_identity"] = {"boot_id": "fixture", "instance_id": "123"}
    rescue.record(spec["run_id"], state)
    files = [object_row(p, role) for p, role in ((config, "training_config"), (receipt, "source_receipt"),
        (bundle, "source_zip"), (wheel, "source_wheel"))]
    request = {"contract": BOOTSTRAP_REQUEST_V1, "run_id": spec["run_id"], "spec_sha256": digest(spec),
        "source_identity": {"boot_id": "fixture", "instance_id": "123"},
        "checkpoint_commit_id": rescue.journal(spec["run_id"])["latest"]["commit_id"],
        "files": files, "runtime_archive": object_row(archive), "runtime_archive_proof": object_row(proof_path)}
    for row in [*files, request["runtime_archive"], request["runtime_archive_proof"]]:
        rescue.put_stream(io.BytesIO(Path(row["source_path"]).read_bytes()), row)

    class BoundSource(LocalSource):
        def __init__(self):
            super().__init__(spec, rescue.state / "remote-fixture")
            self.actions = []

        def adapter(self, action, *args, **kwargs):
            self.actions.append(action)
            if action == "metadata":
                return object_row(Path(args[args.index("--path") + 1]))
            if action == "verify-runtime":
                return verify_runtime_capsule(Path(spec["runtime_prefix"]), proof_path)
            return super().adapter(action, *args, **kwargs)

        def json(self, argv, **kwargs):
            self.actions.append("runtime-probe")
            assert "PYTHONDONTWRITEBYTECODE=1" in argv and spec["source_root"] in argv
            return {"runtime": runtime, "source_release": release}

    source = BoundSource()
    monkeypatch.setattr(rescue, "source", lambda _: source)
    return rescue, spec, request, source, json.loads(proof_path.read_bytes())


def bound(case):
    owner, spec, request, _, _ = case
    result = owner.bind_bootstrap(request)
    return result, owner.bootstrap(spec["run_id"], result["bootstrap_id"])


def test_existing_objects_and_original_enrollment_are_reused_without_GPU_or_source_writes(binding_case, monkeypatch):
    owner, spec, _, source, proof = binding_case
    original = owner.path(spec["run_id"]) / "spec.json"
    before = signature(original), original.read_bytes()
    objects = {p: (signature(p), file_hash(p)) for p in (owner.cold / "objects").glob("*/*")}
    latest = copy.deepcopy(owner.journal(spec["run_id"])["latest"])
    monkeypatch.setattr(owner, "pull", lambda *_a, **_kw: pytest.fail("existing D payload must not be downloaded again"))
    report, bootstrap = bound(binding_case)
    assert report["original_enrollment_unchanged"] and not report["reused"]
    assert report["bootstrap_binding_contract"] == BOOTSTRAP_BINDING_V2
    assert report["GPU_jobs_started"] == 0 and not report["source_inputs_rebuilt_on_remote"]
    assert report["runtime_full_decode"]["files"] == len(proof["files"])
    assert report["runtime_full_decode"]["C_payload_bytes_written"] == 0
    assert before == (signature(original), original.read_bytes())
    assert objects == {p: (signature(p), file_hash(p)) for p in (owner.cold / "objects").glob("*/*")}
    assert owner.journal(spec["run_id"])["latest"] == latest
    assert bootstrap_runtime_lock(spec, bootstrap) == bootstrap["runtime_lock"]
    assert source.actions == ["metadata"] * 5 + ["runtime-probe", "verify-runtime"]
    status = owner.status()["runs"][0]
    assert status["bootstrap_binding_contract"] == BOOTSTRAP_BINDING_V2 and status["bootstrap_preserved"]
    assert not status["recovery_prepared"] and not status["resume_progress_verified"]  # No input proof/GPU.


def test_repeated_binding_preserves_original_journal_and_bootstrap_inode(binding_case):
    owner, spec, request, _, _ = binding_case
    first, _ = bound(binding_case)
    journal = owner.path(spec["run_id"]) / "state.json"
    bootstrap = owner.cold / "bootstrap" / spec["run_id"] / (first["bootstrap_id"] + ".json")
    before = [(signature(p), p.read_bytes()) for p in (journal, bootstrap)]
    second = owner.bind_bootstrap(request)
    assert second["reused"] and second["bootstrap_id"] == first["bootstrap_id"]
    assert before == [(signature(p), p.read_bytes()) for p in (journal, bootstrap)]


def test_slow_readback_releases_capture_owner_and_merges_newest_checkpoint(binding_case, monkeypatch):
    owner, spec, _, source, _ = binding_case
    real = source.json
    observed = []

    def during_io(*args, **kwargs):
        with owner.lock():
            state = owner.journal(spec["run_id"])
            state["latest"]["concurrent_capture_observation"] = "retained"
            state["concurrent_control_observation"] = True
            owner.record(spec["run_id"], state)
        observed.append(True)
        return real(*args, **kwargs)

    monkeypatch.setattr(source, "json", during_io)
    bound(binding_case)
    state = owner.journal(spec["run_id"])
    assert observed and state["concurrent_control_observation"]
    assert state["latest"]["concurrent_capture_observation"] == "retained"


@pytest.mark.parametrize("damage", ["spec", "config", "extra", "source_zip", "runtime_prefix", "capsule_path"])
def test_unbound_attachment_rejects_before_source_contact_and_preserves_checkpoint(binding_case, monkeypatch, damage):
    owner, spec, request, _, _ = binding_case
    value = copy.deepcopy(request)
    if damage == "spec": value["spec_sha256"] = "f" * 64
    elif damage == "config": value["files"][0]["target_path"] += "-different"
    elif damage == "extra": value["files"].append({**value["files"][0], "role": "unbound", "target_path": "/unbound/config"})
    elif damage == "source_zip": value["files"][2]["role"] = "unbound"
    elif damage == "runtime_prefix":
        proof = json.loads(owner._bootstrap_object(value["runtime_archive_proof"]))
        proof["prefix"] += "-different"
        raw = json.dumps(proof).encode()
        row = {**value["runtime_archive_proof"], "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        owner.put_stream(io.BytesIO(raw), row)
        value["runtime_archive_proof"] = row
    else:
        value["runtime_archive_proof"]["source_path"] += "-different"
    before = copy.deepcopy(owner.journal(spec["run_id"]))
    monkeypatch.setattr(owner, "source", lambda *_: pytest.fail("unbound attachment must not contact source"))
    with pytest.raises((ValueError, RescueError)):
        owner.bind_bootstrap(value)
    assert owner.journal(spec["run_id"]) == before
    assert not (owner.cold / "bootstrap").exists()


@pytest.mark.parametrize("damage", ["before_identity", "after_identity", "runtime_lock", "source_file", "active_target", "object_bytes"])
def test_drift_or_running_target_never_admits_bootstrap(binding_case, monkeypatch, damage):
    owner, spec, request, source, _ = binding_case
    state = owner.journal(spec["run_id"])
    if damage == "active_target":
        state["active_target"] = {"plan_id": "b" * 64}
        owner.record(spec["run_id"], state)
    elif damage in {"before_identity", "after_identity"}:
        calls = []
        def identity():
            calls.append(True)
            if damage == "before_identity" or len(calls) > 1:
                return {"boot_id": "replaced", "instance_id": "999"}
            return request["source_identity"]
        monkeypatch.setattr(source, "node_identity", identity)
    elif damage == "runtime_lock":
        real = source.json
        def probe(*a, **kw):
            observed = copy.deepcopy(real(*a, **kw)); observed["runtime"]["packages"]["fixture"] = "2"
            return observed
        monkeypatch.setattr(source, "json", probe)
    elif damage == "source_file":
        Path(request["files"][0]["source_path"]).write_bytes(b"changed original config")
    else:
        path = owner.object_path(request["runtime_archive"]["sha256"])
        raw = bytearray(path.read_bytes()); raw[-8] ^= 1; path.write_bytes(raw)
    before = copy.deepcopy(owner.journal(spec["run_id"]))
    with pytest.raises((ValueError, RescueError, gzip.BadGzipFile)):
        owner.bind_bootstrap(request)
    assert owner.journal(spec["run_id"]) == before and not (owner.cold / "bootstrap").exists()


def test_new_bootstrap_is_validated_before_plan_or_target_contact(binding_case, monkeypatch):
    owner, spec, _, _, _ = binding_case
    report, bootstrap = bound(binding_case)
    state = owner.journal(spec["run_id"])
    spec["data_sources"] = [{"dataset": "fixture", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                             "materialized_root": str(Path(spec["source_root"]) / "input")}]
    # A different spec cannot inherit the preserved bootstrap, even if a forged
    # source-proof journal claims the new digest was decoded.
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    state["input_verification"] = {"spec_sha256": digest(spec), "level": "all_original_file_SHA_decoded"}
    owner.record(spec["run_id"], state)
    key = owner.issue_key(hours=1)
    with pytest.raises(ValueError, match="original enrollment"):
        owner.plan(spec["run_id"], host="192.0.2.1", port=40032, user="root", key_id=key["key_id"],
            host_key_line="[192.0.2.1]:40032 " + " ".join(key["public_key"].split()[:2]))
    assert not (owner.state / "plans").exists()


def test_versioned_bootstrap_survives_closed_backup_and_isolated_multi_root_restore(binding_case, tmp_path, monkeypatch):
    owner, spec, _, _, _ = binding_case
    _, bootstrap = bound(binding_case)
    monkeypatch.setattr(owner, "source", lambda *_: pytest.fail("backup must not contact trainer"))
    plan = freeze_inventory(owner)
    roots = [tmp_path / "restore-a", tmp_path / "restore-b"]
    proof_name = f"objects/{bootstrap['runtime_archive_proof']['sha256'][:2]}/{bootstrap['runtime_archive_proof']['sha256']}"
    for row in plan["members"]:
        target = roots[row["relative"] == proof_name] / row["relative"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(owner.cold / row["relative"], target)
    plan_path = roots[0] / "recovery-plan.private.json"
    for root in roots:
        atomic_json(root / "recovery-plan.private.json", plan)
    proof = verify_restored_plan(roots[0], plan_path, plan["identity_sha256"], additional_roots=[roots[1]])
    assert proof["exact_restored_set_and_full_SHA_verified"]
    assert proof["runs"][0]["bootstrap_id"] == owner.journal(spec["run_id"])["bootstrap"]["id"]
    assert "new_GPU_resume_not_performed" in proof["runs"][0]["pending"]


@pytest.mark.parametrize("damage", ["runtime_lock", "provenance", "source_receipt", "version"])
def test_rehashed_bootstrap_control_cannot_replace_committed_provenance(binding_case, damage):
    owner, spec, _, _, _ = binding_case
    _, bootstrap = bound(binding_case)
    if damage == "runtime_lock": bootstrap["runtime_lock"]["packages"]["fixture"] = "different"
    elif damage == "provenance": bootstrap["provenance_commit"]["inventory"]["context_sha256"] = "a" * 64
    elif damage == "source_receipt": bootstrap["files"][1]["sha256"] = "b" * 64
    else: bootstrap["bootstrap_contract"] = "unknown"
    with pytest.raises((ValueError, RescueError)):
        validate_bootstrap_binding(spec, bootstrap, read_object=owner._bootstrap_object,
            source_identity=owner.journal(spec["run_id"])["source_identity"])


@pytest.mark.parametrize("drift", [False, True])
def test_explicit_batch_reuses_one_decode_but_never_reuses_a_changed_object(binding_case, tmp_path, monkeypatch, drift):
    owner, spec, request, source, _ = binding_case
    second = copy.deepcopy(spec)
    output = tmp_path / "artifacts/markets/second"
    shutil.copytree(Path(spec["output_root"]), output)
    config = str(output / Path(spec["config_file"]).name)
    second.update(run_id="formal-second", output_root=str(output), config_file=config,
                  bootstrap_files=[{**spec["bootstrap_files"][0], "source_path": config, "target_path": config}])
    second["train_argv"] = [config if arg == spec["config_file"] else str(output) if arg == spec["output_root"] else arg
                            for arg in spec["train_argv"]]
    enroll_fixture(owner, second, monkeypatch)
    owner.capture(second["run_id"])
    state = owner.journal(second["run_id"])
    state["source_identity"] = request["source_identity"]
    owner.record(second["run_id"], state)
    second_request = copy.deepcopy(request)
    second_request.update(run_id=second["run_id"], spec_sha256=digest(second), checkpoint_commit_id=state["latest"]["commit_id"])
    second_request["files"][0].update(source_path=config, target_path=config)
    monkeypatch.setattr(owner, "source", lambda _: source)
    calls = []
    real = decode_runtime_capsule

    def decode(*args):
        calls.append(True)
        return real(*args)

    monkeypatch.setattr("stockagent.training_rescue.decode_runtime_capsule", decode)
    if drift:
        actual = owner._bind_bootstrap
        def run(value, **kwargs):
            result = actual(value, **kwargs)
            if value["run_id"] == spec["run_id"]:
                path = owner.object_path(request["runtime_archive"]["sha256"])
                path.write_bytes(path.read_bytes())  # Same SHA, different ctime: fresh read is required.
            return result
        monkeypatch.setattr(owner, "_bind_bootstrap", run)
        with pytest.raises(RescueError, match="generation changed within"):
            owner.bind_bootstrap_batch([request, second_request])
        assert owner.journal(spec["run_id"])["bootstrap"]
        assert not owner.journal(second["run_id"]).get("bootstrap")
    else:
        result = owner.bind_bootstrap_batch([request, second_request])
        assert result["unique_D_runtime_decodes"] == 1
        assert [r["runtime_decode_reused_within_this_cohort"] for r in result["runs"]] == [False, True]
    assert len(calls) == 1


def encoded_runtime(tmp_path, damage=None):
    _, archive, proof_path = runtime_fixture(tmp_path)
    proof = json.loads(proof_path.read_bytes())
    raw = archive.read_bytes()
    if damage in {None, "legacy_links"}:
        if damage == "legacy_links":
            proof.pop("link_target_encoding")
            for row in proof["files"]:
                if row["kind"] == "symlink": row["target"] = str(PurePosixPath(row["target"]))
        return raw, proof
    out = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as original, tarfile.open(fileobj=out, mode="w") as modified:
        changed = False
        for member in original:
            content = original.extractfile(member).read() if member.isfile() else None
            member = copy.deepcopy(member)
            if not changed:
                if damage == "missing": changed = True; continue
                if damage == "unknown": member.name = "unbound-original"; changed = True
                if damage == "symlink" and member.issym(): member.linkname = "/outside"; changed = True
                if damage == "hardlink" and member.islnk(): member.linkname = "unknown-original"; changed = True
                if damage == "bytes" and member.isfile(): content = b"x" * len(content); changed = True
            modified.addfile(member, io.BytesIO(content) if member.isfile() else None)
            if damage == "duplicate" and not changed:
                modified.addfile(member, io.BytesIO(content) if member.isfile() else None); changed = True
    decoded = out.getvalue() + (b"hidden-tail" if damage == "tail" else b"")
    raw = gzip.compress(decoded, mtime=0)
    if damage == "CRC": raw = raw[:-8] + bytes([raw[-8] ^ 1]) + raw[-7:]
    if damage == "truncated": raw = raw[:-4]
    if damage == "second_tar": raw += gzip.compress(decoded)
    proof.update(archive_sha256=hashlib.sha256(raw).hexdigest(), archive_bytes=len(raw))
    return raw, proof


@pytest.mark.parametrize("variant", [None, "legacy_links"])
def test_runtime_stream_keeps_exact_link_semantics_and_reads_complete_archive(tmp_path, variant):
    raw, proof = encoded_runtime(tmp_path, variant)
    result = decode_runtime_capsule(io.BytesIO(raw), proof)
    assert result["files"] == 3 and result["links"] == 2
    assert result["encoded_bytes"] == len(raw) and result["C_payload_bytes_written"] == 0


@pytest.mark.parametrize("damage", ["missing", "unknown", "symlink", "hardlink", "bytes", "duplicate", "tail", "CRC", "truncated", "second_tar"])
def test_runtime_stream_rejects_bad_members_even_when_outer_archive_hash_matches(tmp_path, damage):
    raw, proof = encoded_runtime(tmp_path, damage)
    with pytest.raises((ValueError, EOFError, gzip.BadGzipFile, tarfile.ReadError)):
        decode_runtime_capsule(io.BytesIO(raw), proof)
