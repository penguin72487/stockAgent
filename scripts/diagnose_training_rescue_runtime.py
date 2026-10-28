#!/usr/bin/env python3
"""Read a pinned original runtime without changing its lock or distributions.

The original wheel is a real, hash-verified metadata search location. Duplicate
distributions remain visible; their observed selection is never forced to the
historical lock. A metadata mismatch produces evidence and exit 75, not a pass.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.verify_training_rescue import _closed_bytes, _legacy_bootstrap_reference  # noqa: E402
from stockagent.runtime_identity import validate_runtime_lock, verify_source_release  # noqa: E402
from stockagent.training_rescue_bootstrap import validate_bootstrap_binding  # noqa: E402
from stockagent.training_rescue_worker import (  # noqa: E402
    HASH, absolute, digest, file_hash, preparation_references, relative,
    runtime_link_target, signature, validate_spec,
)

CONTRACT = "stockagent.original_runtime_restore_qualification.v1"
MEMBERS = {"spec", "bootstrap", "runtime_proof", "source_receipt", "source_wheel"}


def read_bound(roots: list[Path], row: dict) -> tuple[Path, bytes]:
    name = relative(row["relative"])
    if (not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])
            or type(row.get("bytes")) is not int or not 0 < row["bytes"] <= 32 * 1024**2):
        raise ValueError("invalid bounded original runtime binding")
    paths = [absolute(str(root / name)) for root in roots]
    found = [path for path in paths if path.exists()]
    if len(found) != 1:
        raise ValueError("original member must occur in exactly one restored part")
    raw = _closed_bytes(found[0])
    if len(raw) != row["bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
        raise ValueError("original runtime member SHA/size differs")
    return found[0], raw


def runtime_files(prefix: Path, proof: dict) -> dict[Path, tuple]:
    """Verify the restored prefix, retaining every original distribution."""
    rows = proof.get("files")
    if (proof.get("kind") != "fixed_prefix_runtime" or proof.get("prefix") != str(prefix)
            or not isinstance(rows, list) or not 0 < len(rows) <= 250000):
        raise ValueError("invalid original runtime proof")
    expected, observations = set(), {}
    for row in rows:
        name = relative(row["relative"])
        if name in expected:
            raise ValueError("duplicate original runtime member")
        expected.add(name)
        path = prefix / name
        absolute(str(path.parent))
        before = signature(path)
        if row["kind"] == "file":
            if (not stat.S_ISREG(before[-1]) or before[2] != row["bytes"]
                    or file_hash(path) != row["sha256"]):
                raise ValueError("restored runtime original bytes differ")
        elif row["kind"] == "symlink":
            if not stat.S_ISLNK(before[-1]) or runtime_link_target(path, proof) != row["target"]:
                raise ValueError("restored runtime original link differs")
        else:
            raise ValueError("unknown original runtime member kind")
        if signature(path) != before:
            raise ValueError("runtime member changed during original readback")
        observations[path] = before
    actual = set()
    for directory, dirs, files in os.walk(prefix, followlinks=False):
        parent = Path(directory)
        if "__pycache__" not in parent.parts:
            observations[parent] = signature(parent)
        for name in [*dirs, *files]:
            path = parent / name
            if "__pycache__" in path.parts or path.suffix == ".pyc":
                continue  # The original capsule explicitly excludes compiler caches.
            if path.is_symlink() or path.is_file():
                actual.add(path.relative_to(prefix).as_posix())
    marker = ".stockagent-rescue-runtime.json"
    if marker in actual:
        raw = _closed_bytes(prefix / marker)
        if json.loads(raw).get("archive_sha256") != proof["archive_sha256"]:
            raise ValueError("restored runtime marker belongs to another archive")
        actual.remove(marker)
        observations[prefix / marker] = signature(prefix / marker)
    if actual != expected:
        raise ValueError("restored runtime has missing or unregistered original files")
    return observations


PROBE = r'''
import hashlib,json,sys
from importlib import metadata
sys.path.insert(0,sys.argv[1])
sys.path.insert(1,sys.argv[2])
from stockagent.runtime_identity import runtime_identity
observed=runtime_identity()
distributions=[]
for dist in metadata.distributions():
    value=dist.metadata
    distributions.append({"name":value.get("Name"),"version":value.get("Version"),
        "metadata_location":str(getattr(dist,"_path","unknown"))})
print(json.dumps({"runtime":observed,"distributions_in_observed_order":distributions,
    "sys_path":sys.path,"runtime_observer_file":__import__("stockagent.runtime_identity",fromlist=[""]).__file__,
    "torch_imported":"torch" in sys.modules}))
'''


def diagnose(contract: dict, contract_id: str, roots: list[Path]) -> dict:
    if (contract.get("contract") != CONTRACT or contract.get("identity_sha256") != contract_id
            or digest({k: v for k, v in contract.items() if k != "identity_sha256"}) != contract_id
            or set(contract.get("bindings", {})) != MEMBERS):
        raise ValueError("original runtime qualification contract identity differs")
    if not 1 <= len(roots) <= 64 or len(set(roots)) != len(roots):
        raise ValueError("qualification needs explicit distinct restored roots")
    roots = [absolute(str(path.absolute())) for path in roots]
    if (len({(path.stat().st_dev, path.stat().st_ino) for path in roots}) != len(roots)
            or any(not path.is_dir() or any(path in old.parents or old in path.parents for old in roots[:index])
                   for index, path in enumerate(roots))):
        raise ValueError("restored runtime parts must be distinct nonoverlapping directories")
    resolved = {name: read_bound(roots, row) for name, row in contract["bindings"].items()}
    observed_controls = {path: signature(path) for path, _ in resolved.values()}
    spec, bootstrap, proof, receipt = [json.loads(resolved[key][1])
        for key in ("spec", "bootstrap", "runtime_proof", "source_receipt")]
    validate_spec(spec)
    if (spec["run_id"] != contract["run_id"] or digest(spec) != contract["original_spec_sha256"]
            or digest(bootstrap) != contract["original_bootstrap_id"]
            or spec["runtime_lock"]["sha256"] != contract["original_runtime_lock_sha256"]
            or validate_runtime_lock(spec["runtime_lock"], spec["runtime_lock"])):
        raise ValueError("qualification differs from the unchanged original enrollment")
    def original(row):
        path, raw = read_bound(roots, {"relative": f"objects/{row['sha256'][:2]}/{row['sha256']}", **row})
        observed_controls[path] = signature(path)
        return raw
    validate_bootstrap_binding(spec, bootstrap, read_object=original)
    if (hashlib.sha256(resolved["runtime_proof"][1]).hexdigest() != bootstrap["runtime_archive_proof"]["sha256"]
            or proof["archive_sha256"] != bootstrap["runtime_archive"]["sha256"]
            or proof["archive_bytes"] != bootstrap["runtime_archive"]["bytes"]
            or receipt["wheel"]["sha256"] != contract["bindings"]["source_wheel"]["sha256"]):
        raise ValueError("original runtime/source material provenance differs")
    references = []
    for index, rows in preparation_references(spec).items():
        for row in rows:
            bound = _legacy_bootstrap_reference(spec, spec, bootstrap, row)
            if bound is None:
                raise ValueError("qualification needs the exact original legacy reference binding")
            original(bound)
            references.append({"step_index": index, **bound})
    source = absolute(spec["source_root"])
    source_readback = verify_source_release(resolved["source_receipt"][0], source)
    prefix = absolute(spec["runtime_prefix"])
    observations = runtime_files(prefix, proof)
    environment = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME"}}
    environment.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    command = [str(prefix / "bin/python"), "-s", "-B", "-c", PROBE,
               str(source), str(resolved["source_wheel"][0])]
    result = subprocess.run(command, cwd=source, env=environment, capture_output=True,
                            text=True, check=True, timeout=180)
    actual = json.loads(result.stdout)
    if actual.get("torch_imported") is not False:
        raise ValueError("runtime diagnostic unexpectedly imported torch")
    differences = validate_runtime_lock(spec["runtime_lock"], actual["runtime"])
    if any(signature(path) != value for path, value in {**observations, **observed_controls}.items()):
        raise ValueError("original runtime or controls changed during metadata observation")
    verify_source_release(resolved["source_receipt"][0], source)
    return {"contract": "stockagent.original_runtime_qualification_readback.v1",
        "restore_contract_identity_sha256": contract_id, "run_id": spec["run_id"],
        "state": "original_runtime_strict_lock_verified" if not differences else "original_runtime_strict_lock_pending",
        "original_spec_sha256": digest(spec), "original_bootstrap_id": digest(bootstrap),
        "original_runtime_lock_sha256": spec["runtime_lock"]["sha256"],
        "original_runtime_files_verified": len(proof["files"]),
        "original_source_readback": source_readback, "legacy_reference_bindings": references,
        "original_wheel_metadata_search_location": str(resolved["source_wheel"][0]),
        "metadata_observation": actual, "runtime_lock_differences": differences,
        "strict_original_runtime_lock_verified": not differences,
        "distribution_filtering_performed": False, "forced_distribution_version_selection": False,
        "packages_installed_or_removed": 0, "checkpoint_loaded": False,
        "GPU_started": False, "training_started": False, "resume_qualified": False,
        "observed_at_epoch": time.time()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--restored-part", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = absolute(str(args.output.absolute()))
    roots = [path.absolute() for path in args.restored_part]
    if output.exists() or any(output.is_relative_to(path) for path in roots):
        raise ValueError("keep a new diagnostic output outside the immutable restore")
    raw = _closed_bytes(args.contract.absolute())
    result = diagnose(json.loads(raw), args.contract_id, roots)
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    with output.open("xb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write((json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode())
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({key: result[key] for key in ("state", "run_id", "runtime_lock_differences")}))
    return 0 if result["strict_original_runtime_lock_verified"] else 75


if __name__ == "__main__":
    raise SystemExit(main())
