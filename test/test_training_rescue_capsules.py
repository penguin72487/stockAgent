"""Exact raw-capsule recovery, separate from training/CUDA acceptance."""
import copy
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from types import SimpleNamespace

import pytest

from stockagent.training_rescue import CACHE_CONTROLLER, Rescue, RescueError, input_source_readback_ready, load_policy
from stockagent.training_rescue_worker import (
    CONTRACT, atomic_json, decode_input_capsule, digest, file_hash,
    input_capsule_proof, materialize_capsule, validate_spec,
)

ROOT = Path(__file__).resolve().parents[1]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def capsule(tmp_path, damage=None):
    values = {"data/ticks/A.parquet": b"raw negative values: -1,0,3\n",
              "data/ticks/A.receipt.json": b'{"source":"fixture","empty":false}'}
    files = [{"relative": name, "bytes": len(raw), "sha256": sha(raw), "mode": 0o640,
              "mtime_ns": 1700000000123456789} for name, raw in values.items()]
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for index, row in enumerate(files):
            if damage == "missing" and index == 1:
                continue
            member = tarfile.TarInfo(row["relative"])
            member.size, member.mode = row["bytes"], row["mode"]
            member.mtime = row["mtime_ns"] // 10**9
            member.pax_headers["mtime"] = "1700000000.123456789"
            raw = values[row["relative"]]
            if damage == "values" and index == 0:
                raw = b"x" * len(raw)
            if damage == "traversal" and index == 0:
                member.name = "../escape"
            if damage == "mode" and index == 0:
                member.mode = 0o600
            if damage == "mtime" and index == 0:
                member.pax_headers["mtime"] = "1700000000.123456788"
            if damage == "symlink" and index == 0:
                member.type, member.linkname, member.size = tarfile.SYMTYPE, "/etc/passwd", 0
            archive.addfile(member, io.BytesIO(raw) if member.isfile() else None)
            if damage == "duplicate" and index == 0:
                archive.addfile(member, io.BytesIO(raw))
    decoded = stream.getvalue() + (b"unregistered tail" if damage == "tail" else b"")
    encoded = gzip.compress(decoded, mtime=0)
    if damage == "gzip_crc":
        encoded = encoded[:-8] + bytes([encoded[-8] ^ 1]) + encoded[-7:]
    if damage == "truncated":
        encoded = encoded[:-4]
    proof = {"contract": "declared_tick_input_capsule_v1", "source_root": "/original",
        "source_plan_sha256": "a" * 64, "files": files,
        "original_bytes": sum(row["bytes"] for row in files), "archive_sha256": sha(encoded),
        "archive_bytes": len(encoded)}
    raw_proof = json.dumps(proof).encode()
    binding = {"capsule_id": "tick-originals", "source_plan_sha256": proof["source_plan_sha256"],
        "archive_sha256": sha(encoded), "archive_bytes": len(encoded),
        "proof_sha256": sha(raw_proof), "proof_bytes": len(raw_proof),
        "materialized_root": str(tmp_path / "shared-inputs" / sha(encoded)),
        "subtree_links": {str(tmp_path / "original/ticks"): "data/ticks"}}
    spec = {"contract": CONTRACT, "schema_version": 1, "run_id": "capsule-fixture",
        "source_root": str(ROOT), "runtime_prefix": sys.prefix,
        "output_root": str(tmp_path / "artifacts/markets/formal"),
        "config_file": str(tmp_path / "config.yaml"), "world_size": 1, "global_batch": 4,
        "environment": {}, "data_sources": [], "data_capsules": [binding], "rebuild_steps": []}
    spec["train_argv"] = ["train.py", "--config", spec["config_file"],
                          "--output-dir", spec["output_root"], "--resume"]
    path = tmp_path / "controller/spec.json"
    atomic_json(path, spec)
    proof_path, archive_path = tmp_path / "proof.json", tmp_path / "originals.tar.gz"
    proof_path.write_bytes(raw_proof)
    archive_path.write_bytes(encoded)
    return spec, path, binding, proof, proof_path, archive_path, values


def controller(tmp_path, monkeypatch):
    policy = load_policy(ROOT / "configs/training_rescue.json")
    policy.update(state_root=str(tmp_path / "state"), cold_root=str(tmp_path / "cold"))
    owner = Rescue(policy)
    monkeypatch.setattr(owner, "guard", lambda: owner.cold.mkdir(parents=True, exist_ok=True))
    return owner


def store_objects(owner, binding, proof_path, archive):
    for name, path in (("proof", proof_path), ("archive", archive)):
        target = owner.object_path(binding[name + "_sha256"])
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())


@pytest.mark.parametrize("damage", [None, "missing", "values", "traversal", "mode", "mtime", "symlink",
                                  "duplicate", "tail", "gzip_crc", "truncated"])
def test_full_decode_checks_encoded_and_original_bytes_metadata_and_gzip(tmp_path, damage):
    _, _, binding, _, proof_path, archive, _ = capsule(tmp_path, damage)
    proof = input_capsule_proof(binding, proof_path.read_bytes())
    with archive.open("rb") as stream:
        if damage:
            with pytest.raises((ValueError, OSError, EOFError, tarfile.TarError)):
                decode_input_capsule(stream, binding, proof)
        else:
            result = decode_input_capsule(stream, binding, proof)
            assert result["files"] == 2 and result["original_mode_mtime_SHA_verified"]
            assert result["bytes"] == proof["original_bytes"]
    assert not Path(binding["materialized_root"]).exists()


@pytest.mark.parametrize("damage", ["duplicate", "bool_size", "unsafe", "mode", "mtime", "wrong_plan",
                                  "wrong_total", "empty", "outside_subtree", "wrong_SHA"])
def test_invalid_proof_rejects_without_materialization(tmp_path, damage):
    _, _, binding, proof, _, _, _ = capsule(tmp_path)
    row = proof["files"][0]
    if damage == "duplicate":
        proof["files"].append(row)
    elif damage == "bool_size":
        row["bytes"] = True
    elif damage == "unsafe":
        row["relative"] = "data/../escape"
    elif damage == "mode":
        row["mode"] = 0o4755
    elif damage == "mtime":
        row["mtime_ns"] = True
    elif damage == "wrong_plan":
        proof["source_plan_sha256"] = "f" * 64
    elif damage == "wrong_total":
        proof["original_bytes"] += 1
    elif damage == "empty":
        proof["files"] = []
    elif damage == "outside_subtree":
        row["relative"] = "unknown/A"
    raw = json.dumps(proof).encode()
    binding.update(proof_bytes=len(raw), proof_sha256=sha(raw) if damage != "wrong_SHA" else "0" * 64)
    with pytest.raises(ValueError):
        input_capsule_proof(binding, raw)
    assert not Path(binding["materialized_root"]).exists()


def test_shared_capsule_reconstruction_preserves_mtime_mode_and_exact_inode(tmp_path, monkeypatch):
    spec, path, binding, proof, proof_path, archive, values = capsule(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    import stockagent.training_rescue_worker as worker
    monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    args = path, 0, CACHE_CONTROLLER, file_hash(CACHE_CONTROLLER), proof_path, archive
    assert materialize_capsule(*args, preflight=True)["preflight_only"]
    root, alias = Path(binding["materialized_root"]), Path(next(iter(binding["subtree_links"])))
    assert not root.parent.exists() and not alias.parent.exists()
    first = materialize_capsule(*args)
    assert first["original_source_paths_ready"] and not first["automatic_eviction_authorized"]
    inodes = {}
    for row in proof["files"]:
        member = root / row["relative"]
        assert member.read_bytes() == values[row["relative"]]
        assert member.stat().st_mtime_ns == row["mtime_ns"]
        assert member.stat().st_mode & 0o777 == row["mode"]
        inodes[row["relative"]] = member.stat().st_ino
    # A sibling run shares the same original tree, while keeping its own proof.
    spec["run_id"] = "second-clock-fixture"
    atomic_json(path, spec)
    second = materialize_capsule(*args)
    assert second["reused"]
    assert {name: (root / name).stat().st_ino for name in values} == inodes
    assert (alias / "A.parquet").stat().st_ino == inodes["data/ticks/A.parquet"]
    (root / "unknown").mkdir()
    with pytest.raises(ValueError, match="unknown directory"):
        materialize_capsule(*args)
    assert (root / "unknown").exists()


@pytest.mark.parametrize("damage", ["regular_alias", "foreign_alias", "unowned_tree", "redirected_parent", "capacity"])
def test_conflicts_and_capacity_preserve_unknown_data(tmp_path, monkeypatch, damage):
    _, path, binding, _, proof_path, archive, _ = capsule(tmp_path)
    monkeypatch.setattr(sys, "path", list(sys.path))
    alias, target = Path(next(iter(binding["subtree_links"]))), Path(binding["materialized_root"])
    if damage == "unowned_tree":
        target.mkdir(parents=True)
    elif damage == "redirected_parent":
        other = tmp_path / "other"
        other.mkdir()
        alias.parent.symlink_to(other)
    elif damage != "capacity":
        alias.parent.mkdir()
        if damage == "regular_alias":
            alias.write_bytes(b"protect")
        else:
            alias.symlink_to(tmp_path / "foreign")
    import stockagent.training_rescue_worker as worker
    monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=1))
    with pytest.raises((ValueError, RuntimeError)):
        materialize_capsule(path, 0, CACHE_CONTROLLER, file_hash(CACHE_CONTROLLER), proof_path, archive)
    assert not (path.parent / "source-capsules").exists()
    if damage == "regular_alias":
        assert alias.read_bytes() == b"protect"
    elif damage == "foreign_alias":
        assert alias.readlink() == tmp_path / "foreign"


def test_capsule_only_source_verification_requires_its_exact_proof(tmp_path, monkeypatch):
    spec, _, binding, proof, proof_path, archive, _ = capsule(tmp_path)
    owner = controller(tmp_path, monkeypatch)
    store_objects(owner, binding, proof_path, archive)
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    result = owner.verify_sources(spec["run_id"])
    assert result["capsule_count"] == 1 and result["source_count"] == 0
    state = owner.journal(spec["run_id"])
    assert input_source_readback_ready(spec, state)
    state["input_verification"].pop("capsule_proof_sha256")
    assert not input_source_readback_ready(spec, state)
    assert not Path(binding["materialized_root"]).exists()
    body = json.loads(Path(result["receipt"]).read_bytes())
    assert body["capsules"][0]["verification"]["bytes"] == proof["original_bytes"]


def test_corrupt_capsule_proof_preflights_before_target_contact(tmp_path, monkeypatch):
    spec, _, binding, _, proof_path, archive, _ = capsule(tmp_path)
    owner = controller(tmp_path, monkeypatch)
    store_objects(owner, binding, proof_path, archive)
    owner.object_path(binding["proof_sha256"]).write_bytes(b"wrong")
    monkeypatch.setattr(owner, "push_object", lambda *_: pytest.fail("no partial target contact"))
    with pytest.raises(RescueError, match="proof"):
        owner.restore_inputs(object(), spec, tmp_path / "incoming")


@pytest.mark.parametrize("damage", ["overlapping_raw_root", "overlapping_source_code", "duplicate",
                                  "unsafe_subtree", "bool_count", "alias_overlap"])
def test_capsule_spec_cannot_overlap_other_input_or_output_owners(tmp_path, damage):
    spec, _, binding, _, _, _, _ = capsule(tmp_path)
    if damage == "duplicate":
        spec["data_capsules"].append(copy.deepcopy(binding))
    elif damage == "overlapping_source_code":
        binding["materialized_root"] = str(ROOT / "raw")
    elif damage == "overlapping_raw_root":
        spec["data_sources"] = [{"dataset": "other", "snapshot_id": "fixed", "manifest_sha256": "a" * 64,
                                 "materialized_root": str(Path(binding["materialized_root"]).parent)}]
    elif damage == "unsafe_subtree":
        binding["subtree_links"] = {str(tmp_path / "original/ticks"): "../escape"}
    elif damage == "alias_overlap":
        binding["subtree_links"] = {spec["output_root"]: "data/ticks"}
    else:
        binding["archive_bytes"] = True
    with pytest.raises(ValueError):
        validate_spec(spec)


def test_capsule_preflight_cli_is_read_only(tmp_path):
    _, path, binding, _, proof, _, _ = capsule(tmp_path)
    result = subprocess.run([sys.executable, str(ROOT / "stockagent/training_rescue_worker.py"),
        "capsule-preflight", "--spec", str(path), "--index", "0", "--path", str(CACHE_CONTROLLER),
        "--sha256", file_hash(CACHE_CONTROLLER), "--proof", str(proof)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["preflight_only"]
    assert not Path(binding["materialized_root"]).parent.exists()


@pytest.mark.parametrize("collision", [False, True])
def test_restore_delivers_proof_then_preflights_before_archive_and_reuses_originals(tmp_path, monkeypatch, collision):
    spec, path, binding, _, proof_path, archive, values = capsule(tmp_path)
    owner = controller(tmp_path, monkeypatch)
    store_objects(owner, binding, proof_path, archive)
    monkeypatch.setattr(sys, "path", list(sys.path))
    import stockagent.training_rescue_worker as worker
    monkeypatch.setattr(worker.shutil, "disk_usage", lambda _: SimpleNamespace(free=64 * 1024**3))
    events = []
    class Target:
        def upload(self, target, raw):
            assert raw == CACHE_CONTROLLER.read_bytes()
            events.append("controller")
            return {"sha256": sha(raw), "bytes": len(raw)}

        def command(self, argv):
            assert argv[:3] == ["python3", "/opt/stockagent-training-rescue/worker.py", "receive"]
            expected = argv[argv.index("--sha256") + 1]
            events.append("proof" if expected == binding["proof_sha256"] else "archive")
            return [sys.executable, str(ROOT / "stockagent/training_rescue_worker.py"), *argv[2:]]

        def adapter(self, action, *arguments, **kwargs):
            assert action in {"capsule-preflight", "materialize-capsule"}
            events.append(action)
            proof = Path(arguments[arguments.index("--proof") + 1])
            archive = (Path(arguments[arguments.index("--archive") + 1])
                       if "--archive" in arguments else None)
            return materialize_capsule(path, 0, CACHE_CONTROLLER, file_hash(CACHE_CONTROLLER),
                                       proof, archive, preflight=action == "capsule-preflight")
    alias = Path(next(iter(binding["subtree_links"])))
    if collision:
        alias.parent.mkdir()
        alias.write_bytes(b"preserve")
        with pytest.raises(RuntimeError, match="preserv|replace|exist"):
            owner.restore_inputs(Target(), spec, tmp_path / "incoming")
        assert events == ["proof", "controller", "capsule-preflight"]
        assert alias.read_bytes() == b"preserve"
    else:
        owner.restore_inputs(Target(), spec, tmp_path / "incoming")
        assert events == ["proof", "controller", "capsule-preflight", "archive", "materialize-capsule"]
        assert (alias / "A.parquet").read_bytes() == values["data/ticks/A.parquet"]
        assert owner.object_path(binding["archive_sha256"]).read_bytes() == archive.read_bytes()


@pytest.mark.parametrize("missing_control", [False, True])
def test_quarantine_and_config_operands_are_full_D_preflighted_and_bind_readiness(tmp_path, monkeypatch, missing_control):
    spec, _, binding, _, proof_path, archive, _ = capsule(tmp_path)
    control = b'{"policy":"original","quarantine":["fixture-contract-day"]}'
    destination = tmp_path / "original/source_policy/execution_quarantine.json"
    spec["input_controls"] = [{"path": str(destination), "sha256": sha(control), "bytes": len(control)}]
    owner = controller(tmp_path, monkeypatch)
    store_objects(owner, binding, proof_path, archive)
    if not missing_control:
        obj = owner.object_path(sha(control))
        obj.parent.mkdir(parents=True, exist_ok=True)
        obj.write_bytes(control)
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    if missing_control:
        monkeypatch.setattr(owner, "push_object", lambda *_: pytest.fail("no incomplete control delivery"))
        with pytest.raises(FileNotFoundError):
            owner.restore_inputs(object(), spec, tmp_path / "incoming")
        assert not owner.journal(spec["run_id"]).get("input_verification")
    else:
        result = owner.verify_sources(spec["run_id"])
        state = owner.journal(spec["run_id"])
        assert input_source_readback_ready(spec, state)
        assert state["input_verification"]["prepared_reference_sha256"] == {str(destination): sha(control)}
        body = json.loads(Path(result["receipt"]).read_bytes())
        assert body["prepared_references"] == [{"reference_path": str(destination), "sha256": sha(control), "bytes": len(control)}]
        state["input_verification"]["prepared_reference_sha256"] = {}
        assert not input_source_readback_ready(spec, state)
    assert not destination.exists()


@pytest.mark.parametrize("damage", ["source", "derived", "alias", "duplicate", "unsafe", "bool_size"])
def test_input_controls_cannot_write_into_another_owner(tmp_path, damage):
    spec, _, binding, _, _, _, _ = capsule(tmp_path)
    row = {"path": str(tmp_path / "original/policy.json"), "sha256": "f" * 64, "bytes": 12}
    spec["input_controls"] = [row]
    if damage == "source":
        row["path"] = str(ROOT / "train.py")
    elif damage == "derived":
        row["path"] = spec["output_root"] + "/policy.json"
    elif damage == "alias":
        row["path"] = next(iter(binding["subtree_links"])) + "/policy.json"
    elif damage == "duplicate":
        spec["input_controls"].append(dict(row))
    elif damage == "unsafe":
        row["path"] = "../policy.json"
    else:
        row["bytes"] = True
    with pytest.raises(ValueError):
        validate_spec(spec)
