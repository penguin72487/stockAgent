"""Small Linux rescue adapter. No CUDA import in discovery/lease supervision.

This file is also deployed as a fixed, standalone adapter outside frozen source
releases. Training itself always executes the recorded canonical entrypoint.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from decimal import Decimal
import fcntl
import gzip
import hashlib
import io
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

CONTRACT = "stockagent.training_rescue.v1"
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}\Z")
HASH = re.compile(r"[a-f0-9]{64}\Z")
RESUME_FIELDS = {
    "epoch", "model_state_dict", "optimizer_state_dict", "scaler_state_dict",
    "scheduler_state_dict", "rng_state", "experiment_manifest",
    "effective_train_batch_size", "no_improve_epochs", "early_stop_patience",
    "early_stopping_no_improve_ratio", "early_stop_val_interval_epochs",
}
CONTEXT_SUFFIXES = {".json", ".jsonl", ".yaml", ".yml", ".pt", ".png", ".svg", ".parquet", ".zip", ".csv"}
ENV_NAMES = {
    "CUDA_VISIBLE_DEVICES", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS", "POLARS_MAX_THREADS", "RAYON_NUM_THREADS",
    "TORCHINDUCTOR_COMPILE_THREADS", "STOCKAGENT_SOURCE_RELEASE_RECEIPT",
    "STOCKAGENT_CODE_RELEASE_RECEIPT", "STOCKAGENT_CUDA_ROOT",
    "FINTECH_ENV_PATH", "PYTHON_BIN", "PYTHONUNBUFFERED",
    "STOCKAGENT_GPU_LEASE_ROOT",
}
PREPARED_FILE_SET = "stockagent.prepared_file_set.v1"
PREPARED_REPRESENTATION_FILE_SET = "stockagent.prepared_file_set.v2"
PRESERVED_FILE_SET_KIND = "preserved_file_set_v1"
PRESERVED_CLOCK_FILE_SET = "stockagent.preserved_original_clock_files.v1"
MAX_PRESERVED_FILE_SET_BYTES = 512 * 1024**2
FILE_SET_KINDS = {"file_set_v1", "file_set_v2", PRESERVED_FILE_SET_KIND}
PRESERVED_AUXILIARY_KIND = "factorized_preserved_auxiliary_v1"
PRESERVED_AUXILIARY_CONTRACT = "stockagent.preserved_factorized_auxiliary.v1"
MAX_PRESERVED_AUXILIARY_BYTES = 8 * 1024**3
MAX_PREPARED_REFERENCE_BYTES = 32 * 1024**2
FINANCIAL_REPRESENTATIONS = {
    "lifetime_scope.parquet", "release/rules/sources/context_only_suppressed_carries.parquet",
    "release/rules/sources/admission_review.json", "release/rules/manifest.json",
    "materialization/manifest.json",
}
INPUT_CAPSULE = "declared_tick_input_capsule_v1"
PREPARED_BUILD_POLICY = "shared_atomic_v1"
PREPARED_BUILD_ROOT = Path("/var/lib/stockagent-training-rescue/prepared-builds")
PREPARED_BUILDERS = {
    "tw_futures_financial_v1": "scripts/prepare_tw_futures_margin_training.py",
    "tw_futures_decision_clocks_v1": "scripts/prepare_tw_futures_decision_clocks.py",
}
FINANCIAL_CONTROLS = {"build_acceptance.json", "accounting_checkpoint.json", "product_scope.csv", "lifetime_scope.parquet"}


def _prepared_scope(kind: str) -> str:
    return "published_financial_release_and_controls_v1" if kind == "tw_futures_financial_v1" else "all_clock_outputs_v1"


def _prepared_json_receipt(kind: str, name: str) -> bool:
    if kind == "tw_futures_financial_v1":
        return name in {"build_acceptance.json", "accounting_checkpoint.json", "release/build_summary.json",
                        "release/daily/manifest.json", "release/rules/manifest.json"}
    path = PurePosixPath(name)
    return path.parent.name in {"0845", "0900"} and path.name in {"manifest.json", "opening_reference_manifest.json"}


def identifier(value: str) -> str:
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise ValueError("unsafe rescue identifier")
    return value


def absolute(value: str) -> Path:
    p = Path(value)
    if not p.is_absolute() or ".." in p.parts or p == Path("/"):
        raise ValueError("rescue paths must be explicit non-root absolute paths")
    if any(x.is_symlink() for x in (p, *p.parents)):
        raise ValueError("rescue path traverses a symlink")
    return p


def relative(value: str) -> str:
    p = PurePosixPath(value)
    if (not value or p.is_absolute() or ".." in p.parts or "\\" in value
            or p.as_posix() != value or value == "."):
        raise ValueError("unsafe rescue member path")
    return value


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def signature(path: Path):
    s = path.lstat()
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode


def file_hash(path: Path) -> str:
    before = signature(path)
    if not stat.S_ISREG(before[-1]):
        raise ValueError("only closed regular rescue files are accepted")
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            h.update(block)
    if signature(path) != before:
        raise ValueError("rescue file changed during hashing")
    return h.hexdigest()


def atomic_json(path: Path, body: dict) -> None:
    absolute(str(path))
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".rescue-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(json.dumps(body, sort_keys=True, ensure_ascii=False,
                                    allow_nan=False, indent=2).encode() + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        d = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(d)
        finally:
            os.close(d)
    finally:
        if os.path.exists(name):
            os.unlink(name)  # Exactly this newly generated temporary file.


def validate_spec(spec: dict) -> dict:
    if spec.get("contract") != CONTRACT or spec.get("schema_version") != 1:
        raise ValueError("unsupported training rescue contract")
    if "prepared_build_policy" in spec and spec["prepared_build_policy"] != PREPARED_BUILD_POLICY:
        raise ValueError("unsupported prepared build ownership policy")
    if "input_recipe_revision_sha256" in spec and (
            not isinstance(spec["input_recipe_revision_sha256"], str)
            or not HASH.fullmatch(spec["input_recipe_revision_sha256"])):
        raise ValueError("input recipe revision must pin its canonical SHA")
    identifier(spec["run_id"])
    for key in ("source_root", "output_root", "config_file", "runtime_prefix"):
        absolute(spec[key])
    output = PurePosixPath(spec["output_root"])
    if "artifacts" not in output.parts or not {"markets", "ablations"}.intersection(output.parts):
        raise ValueError("only explicitly enrolled formal result roots can be rescued")
    if (type(spec.get("world_size")) is not int or not 1 <= spec["world_size"] <= 16
            or type(spec.get("global_batch")) is not int or spec["global_batch"] <= 0):
        raise ValueError("rescue needs fixed world size and effective global batch")
    argv = spec["train_argv"]
    if (not isinstance(argv, list) or not argv or argv[0] != "train.py"
            or not all(isinstance(a, str) and "\x00" not in a for a in argv)
            or "--no-resume" in argv or "--resume" not in argv):
        raise ValueError("rescue must invoke canonical train.py with explicit resume")
    for option, expected in (("--config", spec["config_file"]), ("--output-dir", spec["output_root"])):
        if argv.count(option) != 1 or argv[argv.index(option) + 1] != expected:
            raise ValueError("rescue argv does not match its exact config/output")
    env = spec.get("environment", {})
    if (not isinstance(env, dict) or not set(env) <= ENV_NAMES
            or not all(isinstance(v, str) and "\x00" not in v for v in env.values())):
        raise ValueError("only public training runtime controls may be captured")
    for row in spec.get("data_sources", []):
        for key in ("dataset", "snapshot_id"):
            identifier(row[key])
        if not HASH.fullmatch(row["manifest_sha256"]):
            raise ValueError("source release must be pinned by its original manifest SHA")
        absolute(row["materialized_root"])
    capsules = spec.get("data_capsules", [])
    if not isinstance(capsules, list) or len(capsules) > 16:
        raise ValueError("invalid bounded input capsule collection")
    capsule_ids = set()
    for row in capsules:
        identifier(row["capsule_id"])
        if row["capsule_id"] in capsule_ids:
            raise ValueError("duplicate input capsule identity")
        capsule_ids.add(row["capsule_id"])
        for key in ("archive_sha256", "proof_sha256", "source_plan_sha256"):
            if not isinstance(row.get(key), str) or not HASH.fullmatch(row[key]):
                raise ValueError("input capsule needs pinned archive/proof/plan SHAs")
        for key, bound in (("archive_bytes", 128 * 1024**3), ("proof_bytes", MAX_PREPARED_REFERENCE_BYTES)):
            if type(row.get(key)) is not int or not 0 < row[key] <= bound:
                raise ValueError("input capsule exceeds its exact resource bound")
        absolute(row["materialized_root"])
        if not row.get("subtree_links") or row.get("links"):
            raise ValueError("input capsule needs explicit original subtree aliases")
    steps = spec.get("rebuild_steps", [])
    if not isinstance(steps, list):
        raise ValueError("invalid canonical preparation steps")
    roots = []
    for step in steps:
        if not isinstance(step, list) or not step or not all(isinstance(s, str) and "\x00" not in s for s in step):
            raise ValueError("invalid canonical preparation argv")
        path = PurePosixPath(relative(step[0]))
        if path.parts[0] != "scripts" or path.suffix != ".py":
            raise ValueError("rebuild steps must be repository-owned Python builders")
        _, out = prepared_output(step)
        if not any(p.name == "data_preparation" and p.parent.name == "artifacts" for p in out.parents):
            raise ValueError("rebuild may only write derived artifacts/data_preparation roots")
        if any(out == old or out in old.parents or old in out.parents for old in roots):
            raise ValueError("prepared step output roots must be disjoint")
        for binding in [*spec.get("data_sources", []), *capsules]:
            source = Path(binding["materialized_root"])
            if out == source or out in source.parents or source in out.parents:
                raise ValueError("prepared output may not overlap source originals")
        roots.append(out)
    if "rebuild_source_roots" in spec:
        selected = spec["rebuild_source_roots"]
        if not isinstance(selected, list) or len(selected) != len(steps):
            raise ValueError("each preparation must bind its exact builder source root")
        for source in selected:
            absolute(source)
    if "rebuild_working_directories" in spec:
        selected = spec["rebuild_working_directories"]
        if (spec.get("prepared_build_policy") != PREPARED_BUILD_POLICY
                or not isinstance(selected, list) or len(selected) != len(steps)):
            raise ValueError("shared preparation must bind one working directory per step")
        for directory in selected:
            if not isinstance(directory, str) or not directory or "\x00" in directory:
                raise ValueError("invalid prepared working directory")
            path = absolute(directory)
            if any(path == out or out in path.parents for out in roots):
                raise ValueError("prepared working directory may not be inside generated outputs")
    references = preparation_references(spec, require_complete=spec.get("prepared_build_policy") == PREPARED_BUILD_POLICY)
    if spec.get("prepared_build_policy") == PREPARED_BUILD_POLICY and any(
            not isinstance(row.get("reference_sha256"), str) or not HASH.fullmatch(row["reference_sha256"])
            for rows in references.values() for row in rows):
        raise ValueError("shared prepared builds require every pinned original oracle SHA")
    source_aliases(spec)
    input_control_bindings(spec)
    return spec


def source_aliases(spec: dict) -> list[dict]:
    """Validate optional original paths without following the final alias.

    These paths belong to a destination; an existing source directory in the
    controller checkout is not a destination collision or permission to move it.
    """
    protected = [Path(spec[key]) for key in ("source_root", "output_root", "config_file", "runtime_prefix")]
    bindings = [*spec.get("data_sources", []), *spec.get("data_capsules", [])]
    protected += [Path(row["materialized_root"]) for row in bindings]
    protected += [prepared_output(step)[1] for step in spec.get("rebuild_steps", [])]
    protected += [Path(path) for path in spec.get("rebuild_source_roots", [])]
    protected += [Path("/srv/stockagent-packed"), Path("/var/lib/stockagent-training-rescue")]
    for row in spec.get("data_capsules", []):
        root = Path(row["materialized_root"])
        other_roots = [Path(item["materialized_root"]) for item in bindings if item is not row]
        if any(root == old or root in old.parents or old in root.parents for old in [
                *other_roots, *(Path(spec[key]) for key in ("source_root", "output_root", "config_file", "runtime_prefix")),
                *(prepared_output(step)[1] for step in spec.get("rebuild_steps", [])),
                Path("/srv/stockagent-packed"), Path("/var/lib/stockagent-training-rescue")]):
            raise ValueError("input capsule materialization overlaps another recovery root")
    result = []
    current_links = set()
    capsule_start = len(spec.get("data_sources", []))
    for index, row in enumerate(bindings):
        is_capsule = index >= capsule_start
        current = (Path(row["materialized_root"]) if is_capsule else
                   Path(row["materialized_root"]) / "current" / row["dataset"])
        if current in current_links:
            raise ValueError("duplicate/conflicting pinned source cache binding")
        current_links.add(current)
        links, subtrees = row.get("links", []), row.get("subtree_links", {})
        if (not isinstance(links, list) or not isinstance(subtrees, dict)
                or len(links) + len(subtrees) > 64):
            raise ValueError("invalid original source alias collection")
        for name, subtree in [*((name, "") for name in links), *subtrees.items()]:
            if not isinstance(name, str) or "\x00" in name:
                raise ValueError("invalid original source alias")
            path = Path(name)
            if not path.is_absolute() or path == Path("/") or ".." in path.parts:
                raise ValueError("source aliases must be non-root absolute paths")
            if path.parent != Path("/"):
                absolute(str(path.parent))
            if not isinstance(subtree, str) or "\x00" in subtree:
                raise ValueError("invalid original source subtree")
            if subtree:
                relative(subtree)
            elif name in subtrees:
                raise ValueError("subtree source alias must name a non-root directory")
            if any(path == old or path in old.parents or old in path.parents for old in protected):
                raise ValueError("original source alias overlaps a protected recovery root")
            if any(path == Path(old["path"]) or path in Path(old["path"]).parents
                   or Path(old["path"]) in path.parents for old in result):
                raise ValueError("original source aliases must be disjoint")
            target = (Path(row["materialized_root"]) if is_capsule else
                      Path(row["materialized_root"]) / row["dataset"] / row["snapshot_id"])
            result.append({("capsule_index" if is_capsule else "source_index"):
                index - capsule_start if is_capsule else index,
                "path": str(path), "subtree": subtree, "target": str(target / subtree)})
    prepared_links = spec.get("prepared_links", [])
    if not isinstance(prepared_links, list) or len(prepared_links) > 64:
        raise ValueError("invalid bounded prepared alias collection")
    for row in prepared_links:
        if (not isinstance(row, dict) or set(row) != {"step_index", "path"}
                or type(row.get("step_index")) is not int
                or not 0 <= row["step_index"] < len(spec.get("rebuild_steps", []))):
            raise ValueError("prepared alias needs one existing original preparation step")
        name = row["path"]
        if not isinstance(name, str) or "\x00" in name:
            raise ValueError("invalid prepared alias path")
        path = Path(name)
        if not path.is_absolute() or path == Path("/") or ".." in path.parts:
            raise ValueError("prepared alias must be a non-root absolute path")
        if path.parent != Path("/"):
            absolute(str(path.parent))
        if any(path == old or path in old.parents or old in path.parents for old in [
                *protected, *(Path(item["path"]) for item in result)]):
            raise ValueError("prepared alias overlaps a protected recovery/source root")
        if not preparation_references(spec).get(row["step_index"]):
            raise ValueError("prepared alias requires original-byte reconciliation")
        result.append({**row, "target": str(prepared_output(spec["rebuild_steps"][row["step_index"]])[1])})
    return result


def _input_cache_controller(spec: dict, controller: Path, controller_sha: str):
    """Load a control resource in this process, outside original trainer code."""
    controller = absolute(str(controller))
    if (not isinstance(controller_sha, str) or not HASH.fullmatch(controller_sha)
            or not 0 < controller.stat().st_size <= 512 * 1024 or file_hash(controller) != controller_sha):
        raise ValueError("cache controller resource failed its pinned SHA/bound")
    source_root = absolute(spec["source_root"])
    sys.path.insert(0, str(source_root))
    loader = importlib.util.spec_from_file_location("stockagent_rescue_materialized_cache", controller)
    module = importlib.util.module_from_spec(loader)
    sys.modules[loader.name] = module
    loader.loader.exec_module(module)
    return module


def input_control_bindings(spec: dict) -> list[dict]:
    """Exact small config/quarantine operands that a fresh host cannot infer."""
    rows = spec.get("input_controls", [])
    if not isinstance(rows, list) or len(rows) > 512:
        raise ValueError("invalid bounded input control collection")
    protected = [Path(spec[key]) for key in ("source_root", "runtime_prefix", "output_root")]
    protected += [Path(row["materialized_root"]) for row in [
        *spec.get("data_sources", []), *spec.get("data_capsules", [])]]
    protected += [prepared_output(step)[1] for step in spec.get("rebuild_steps", [])]
    protected += [Path(row["path"]) for row in source_aliases(spec)]
    protected += [Path("/srv/stockagent-packed"), Path("/var/lib/stockagent-training-rescue")]
    selected, total = [], 0
    for row in rows:
        if (not isinstance(row, dict) or set(row) != {"path", "sha256", "bytes"}
                or type(row.get("bytes")) is not int or not 0 < row["bytes"] <= MAX_PREPARED_REFERENCE_BYTES
                or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])):
            raise ValueError("input control needs exact bounded SHA/size")
        path = absolute(row["path"])
        if any(path == old or path in old.parents or old in path.parents for old in [*protected, *selected]):
            raise ValueError("input control overlaps another recovery owner or control")
        selected.append(path)
        total += row["bytes"]
    if total > 128 * 1024**2:
        raise ValueError("input controls exceed their total metadata budget")
    return rows


def materialize_input(spec_path: Path, index: int, controller: Path, controller_sha: str, *,
                      preflight: bool = False) -> dict:
    """Use the canonical cache owner while retaining the original trainer code."""
    spec = validate_spec(json.loads(absolute(str(spec_path)).read_bytes()))
    sources = spec.get("data_sources", [])
    if type(index) is not int or not 0 <= index < len(sources):
        raise ValueError("unknown pinned input source index")
    module = _input_cache_controller(spec, controller, controller_sha)
    row = sources[index]
    materialized = absolute(row["materialized_root"])
    options = {"links": [Path(p) for p in row.get("links", [])],
               "subtree_links": {Path(p): subtree for p, subtree in row.get("subtree_links", {}).items()},
               "preserve_existing_links": True}
    targets = module.materialized_link_targets(materialized, row["dataset"], row["snapshot_id"], **options)
    for link, target in targets.items():
        module._check_preserved_link(link, target)
    result = {"contract": "stockagent.rescued_input_materialization.v1", "spec_sha256": digest(spec),
        "source_index": index, "dataset": row["dataset"], "snapshot_id": row["snapshot_id"],
        "manifest_sha256": row["manifest_sha256"], "cache_controller_sha256": controller_sha,
        "link_targets": {str(p): str(t) for p, t in targets.items()}, "preflight_only": preflight}
    if preflight:
        return result
    cold = Path("/srv/stockagent-packed")
    resolved = module.resolve_packed_snapshot_id(cold, row["dataset"], row["snapshot_id"])
    if resolved.manifest_sha256 != row["manifest_sha256"]:
        raise ValueError("pinned input source manifest changed before materialization")
    lease = module.use_materialized_snapshot(cold, materialized, row["dataset"],
        snapshot_id=row["snapshot_id"], verify_existing=True, **options)
    if lease["manifest_sha256"] != row["manifest_sha256"] or lease["verification"] != "full":
        raise ValueError("canonical input materialization lacks full fixed release verification")
    result.update(lease=lease, original_source_paths_ready=True, fresh_GPU_acceptance_claim=False)
    atomic_json(spec_path.parent / "source-inputs" / f"{index}.json", result)
    return result


def prepared_output(step: list[str]) -> tuple[str, Path]:
    options = [option for option in ("--output-root", "--output") for _ in range(step.count(option))]
    if len(options) != 1:
        raise ValueError("each rebuild needs one explicit new prepared output root")
    option = options[0]
    if option == "--output" and step[0] not in PREPARED_BUILDERS.values():
        raise ValueError("--output is supported only for the named canonical futures builders")
    position = step.index(option) + 1
    if position >= len(step):
        raise ValueError("prepared output option has no path")
    return option, absolute(step[position])


def preparation_references(spec: dict, *, require_complete: bool = False) -> dict[int, list[dict]]:
    """Validate references without reading target files during enrollment."""
    steps = spec.get("rebuild_steps", [])
    rows = spec.get("prepared_reconciliation", [])
    if not isinstance(rows, list):
        raise ValueError("invalid prepared reconciliation references")
    selected = {index: [] for index in range(len(steps))}
    outputs = [prepared_output(step)[1] for step in steps]
    reference_digests = {}
    for row in rows:
        if not isinstance(row, dict) or type(row.get("step_index")) is not int or row["step_index"] not in selected:
            raise ValueError("prepared reference has an unknown step")
        index = row["step_index"]
        _, root = prepared_output(steps[index])
        kind = row.get("kind", "factorized_manifest_v1")
        if kind not in {"factorized_manifest_v1", PRESERVED_AUXILIARY_KIND, *FILE_SET_KINDS}:
            raise ValueError("unknown prepared reconciliation kind")
        reference = absolute(row["reference_path"])
        if any(reference == output or output in reference.parents for output in outputs):
            raise ValueError("original prepared reference must be outside rebuilt output")
        sha = row.get("reference_sha256")
        if (kind in FILE_SET_KINDS or "kind" in row or sha is not None) and (
                not isinstance(sha, str) or not HASH.fullmatch(sha)):
            raise ValueError("typed prepared reference needs a pinned original SHA")
        if reference in reference_digests and reference_digests[reference] != sha:
            raise ValueError("one original prepared reference path has conflicting SHA bindings")
        reference_digests[reference] = sha
        if kind in FILE_SET_KINDS:
            if absolute(row["rebuilt_root"]) != root or selected[index]:
                raise ValueError("file-set reconciliation must own the entire step output")
            if kind == PRESERVED_FILE_SET_KIND:
                if spec.get("prepared_build_policy") != PREPARED_BUILD_POLICY:
                    raise ValueError("original file-set restoration requires atomic shared preparation")
                if steps[index][0] != PREPARED_BUILDERS["tw_futures_decision_clocks_v1"]:
                    raise ValueError("original file-set restoration is confined to the named clock builder")
                bundle = row.get("original_file_set_bundle")
                if (not isinstance(bundle, dict) or set(bundle) != {"path", "sha256", "bytes"}
                        or not isinstance(bundle.get("sha256"), str) or not HASH.fullmatch(bundle["sha256"])
                        or type(bundle.get("bytes")) is not int
                        or not 0 < bundle["bytes"] <= MAX_PRESERVED_FILE_SET_BYTES):
                    raise ValueError("original file-set restoration needs its pinned bounded bundle")
                resource = absolute(bundle["path"])
                if (resource == reference or any(resource == output or output in resource.parents for output in outputs)
                        or "representation_bundle" in row or "preserved_auxiliary_bundle" in row):
                    raise ValueError("original file-set bundle must be external and cannot mix recovery methods")
                if resource in reference_digests and reference_digests[resource] != bundle["sha256"]:
                    raise ValueError("original file-set resource has conflicting SHA bindings")
                reference_digests[resource] = bundle["sha256"]
            elif "original_file_set_bundle" in row:
                raise ValueError("recomputed file sets cannot silently use preserved original files")
            if kind == "file_set_v2":
                bundle = row.get("representation_bundle")
                if (not isinstance(bundle, dict) or set(bundle) != {"path", "sha256", "bytes"}
                        or not isinstance(bundle.get("sha256"), str) or not HASH.fullmatch(bundle["sha256"])
                        or type(bundle.get("bytes")) is not int
                        or not 0 < bundle["bytes"] <= MAX_PREPARED_REFERENCE_BYTES):
                    raise ValueError("representation reconciliation requires its pinned bounded original bundle")
                resource = absolute(bundle["path"])
                if (any(resource == output or output in resource.parents for output in outputs)
                        or resource == reference):
                    raise ValueError("original representation bundle must be outside rebuilt outputs and oracle")
                if resource in reference_digests and reference_digests[resource] != bundle["sha256"]:
                    raise ValueError("original representation resource has conflicting SHA bindings")
                reference_digests[resource] = bundle["sha256"]
            elif "representation_bundle" in row:
                raise ValueError("v1 original-file reconciliation remains byte-exact")
        else:
            if "original_file_set_bundle" in row:
                raise ValueError("original file-set bundle needs its explicit restoration kind")
            rebuilt = absolute(row["rebuilt_manifest"])
            if root not in rebuilt.parents or any(r.get("kind") in FILE_SET_KINDS for r in selected[index]):
                raise ValueError("prepared manifest is outside its step output or has mixed oracles")
            if kind == PRESERVED_AUXILIARY_KIND:
                bundle = row.get("preserved_auxiliary_bundle")
                if (not isinstance(bundle, dict) or set(bundle) != {"path", "sha256", "bytes"}
                        or not isinstance(bundle.get("sha256"), str) or not HASH.fullmatch(bundle["sha256"])
                        or type(bundle.get("bytes")) is not int
                        or not 0 < bundle["bytes"] <= MAX_PRESERVED_AUXILIARY_BYTES):
                    raise ValueError("preserved auxiliary recovery needs its pinned bounded original bundle")
                resource = absolute(bundle["path"])
                if (resource == reference or any(resource == output or output in resource.parents for output in outputs)
                        or selected[index]):
                    raise ValueError("preserved auxiliary recovery must own its step and use an external bundle")
                if resource in reference_digests and reference_digests[resource] != bundle["sha256"]:
                    raise ValueError("preserved auxiliary bundle has conflicting SHA bindings")
                reference_digests[resource] = bundle["sha256"]
            elif "preserved_auxiliary_bundle" in row:
                raise ValueError("legacy factorized recovery remains exact recomputation")
            if any(r.get("kind") == PRESERVED_AUXILIARY_KIND for r in selected[index]):
                raise ValueError("preserved auxiliary recovery cannot share a step oracle")
        selected[index].append(row)
    if require_complete and any(not refs for refs in selected.values()):
        raise ValueError("every rebuild step needs original-byte reconciliation before source transfer")
    return selected


def preparation_resource_bindings(spec: dict) -> list[dict]:
    """One census for D admission, target transport and input-readiness proofs."""
    bindings = {}
    def add(resource):
        prior = bindings.get(resource["path"], {})
        if (prior.get("sha256", resource["sha256"]) != resource["sha256"]
                or ("bytes" in prior and "bytes" in resource and prior["bytes"] != resource["bytes"])):
            raise ValueError("prepared resource/control has conflicting exact bindings")
        bindings[resource["path"]] = {**prior, **resource}
    for rows in preparation_references(spec, require_complete=True).values():
        for row in rows:
            if row.get("reference_sha256"):
                add({"path": row["reference_path"], "sha256": row["reference_sha256"]})
            for key in ("representation_bundle", "preserved_auxiliary_bundle", "original_file_set_bundle"):
                if key in row:
                    add(row[key])
    for resource in input_control_bindings(spec):
        add(resource)
    return list(bindings.values())


def checkpoint_summary(path: Path, spec: dict) -> dict:
    # This executes in the SELECTED training runtime, always on CPU. A newer
    # control checkout never rewrites a frozen trainer's compatibility schema.
    before = signature(path)
    source = absolute(spec["source_root"])
    loader = source / "stockagent/training/runtime.py"
    previous_path = list(sys.path)
    try:
        if loader.is_file():
            # The standalone adapter is outside the frozen training checkout.
            # Login cwd and an installed wheel are not its selected source.
            sys.path.insert(0, str(source))
        elif spec.get("source_release") or spec.get("source_receipt"):
            raise ValueError("enrolled checkpoint loader source is unavailable")
        from stockagent.training.runtime import load_checkpoint
        if loader.is_file() and Path(load_checkpoint.__code__.co_filename).resolve() != loader.resolve():
            raise ValueError("checkpoint loader was imported from a different source")
        saved = load_checkpoint(path)
    finally:
        sys.path[:] = previous_path
    if not isinstance(saved, dict) or not RESUME_FIELDS <= set(saved):
        raise ValueError("weights-only/best checkpoint is not an optimizer recovery point")
    if (not saved["model_state_dict"] or not saved["optimizer_state_dict"].get("param_groups")
            or type(saved["epoch"]) is not int or saved["epoch"] < 0
            or not isinstance(saved["experiment_manifest"], dict)
            or not saved["experiment_manifest"].get("fingerprints")):
        raise ValueError("incomplete canonical optimizer checkpoint")
    rng = saved["rng_state"]
    world = int(rng.get("world_size", 1)) if isinstance(rng, dict) else 0
    if world != spec["world_size"] or saved["effective_train_batch_size"] != spec["global_batch"]:
        raise ValueError("checkpoint world size or effective global batch differs")
    if world > 1 and (len(rng.get("by_rank", [])) != world):
        raise ValueError("distributed checkpoint has incomplete per-rank RNG state")
    expected = spec.get("checkpoint_manifests", {}).get(path.relative_to(Path(spec["output_root"])).as_posix())
    if expected is not None and digest(saved["experiment_manifest"]) != expected:
        raise ValueError("checkpoint semantic manifest differs from enrolled optimizer trajectory")
    if signature(path) != before:
        raise ValueError("checkpoint replaced during validation; retry, do not acknowledge")
    return {"epoch": saved["epoch"], "world_size": world,
            "global_batch": saved["effective_train_batch_size"],
            "semantic_manifest_sha256": digest(saved["experiment_manifest"]),
            "full_optimizer_state": True, "signature": before}


def inspect_run(spec: dict) -> dict:
    validate_spec(spec)
    for row in spec.get("bootstrap_files", []):
        if row.get("role") == "training_config" and file_hash(Path(spec["config_file"])) != row["sha256"]:
            raise ValueError("enrolled training configuration changed; use a new compatible run contract")
    root = absolute(spec["output_root"])
    checkpoints = sorted(root.glob("train_*/checkpoint_last.pt"))
    if not checkpoints:
        raise ValueError("no committed group-last checkpoint; recovery cannot claim saved progress")
    names = set()
    proofs = {}
    for p in checkpoints:
        absolute(str(p))
        proofs[p.relative_to(root).as_posix()] = checkpoint_summary(p, spec)
        names.add(p.relative_to(root).as_posix())
    # Curves/normalizers/best weights are resume context, not optimizer proofs.
    # Require a stable cross-file epoch boundary. A partial append simply retries.
    for name, proof in proofs.items():
        curve = root / Path(name).parent / "epoch_curve.jsonl"
        if not curve.is_file():
            raise ValueError("checkpoint curve has not committed yet")
        lines = [json.loads(v) for v in curve.read_bytes().splitlines() if v.strip()]
        epochs = [int(v["epoch"]) for v in lines]
        if (not epochs or len(epochs) != len(set(epochs)) or epochs != sorted(epochs)
                or epochs[-1] != proof["epoch"]):
            raise ValueError("checkpoint and curve are not at the same committed epoch")
    for p in root.rglob("*"):
        if p.is_symlink():
            raise ValueError("formal recovery context contains a symlink")
        if p.is_file() and p.suffix in CONTEXT_SUFFIXES:
            if p.name in {"progress.json", "startup_timing.jsonl", "trainer_startup_timing.jsonl", "pre_epoch_timing.jsonl"}:
                continue  # Diagnostics do not govern resume and can be appended.
            names.add(p.relative_to(root).as_posix())
    rows = []
    for name in sorted(names):
        p = root / name
        if p.stat().st_size > 2 * 1024**3:
            raise ValueError("recovery context member exceeds its bounded size")
        rows.append({"relative": name, "bytes": p.stat().st_size, "sha256": file_hash(p),
                     "mode": stat.S_IMODE(p.stat().st_mode)})
    for name, proof in proofs.items():
        if signature(root / name) != tuple(proof["signature"]):
            raise ValueError("checkpoint advanced during context inventory; retry")
    try:
        lifecycle_state = json.loads((root / "progress.json").read_bytes()).get("state", "unknown")
    except (OSError, ValueError):
        lifecycle_state = "unknown"
    return {"contract": CONTRACT, "spec_sha256": digest(spec), "files": rows,
            "checkpoints": proofs, "context_sha256": digest(rows),
            "source_lifecycle_state": lifecycle_state,
            "observed_at": time.time(), "source_boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def seal_run(spec: dict, state_root: Path) -> dict:
    """Short SSD transaction, independent of WAN speed or mutable progress.json.

    No datasets/caches are copied. Only the bounded optimizer recovery context
    is sealed; it can be discarded after verified D ACK. This prevents frequent
    live progress updates from starving checkpoint preservation.
    """
    inventory = inspect_run(spec)
    sha = inventory["context_sha256"]
    root = absolute(str(state_root / "sealed" / sha))
    receipt = root / ".sealed.json"
    if receipt.exists():
        saved = json.loads(receipt.read_bytes())
        if saved["context_sha256"] != sha or saved["spec_sha256"] != digest(spec):
            raise ValueError("existing rescue seal differs")
        for row in saved["files"]:
            if file_hash(root / relative(row["relative"])) != row["sha256"]:
                raise ValueError("existing rescue seal corrupted")
        return {**saved, "sealed_root": str(root)}
    if root.exists():
        raise ValueError("unowned rescue seal retained; inspect it before retrying")
    # Interrupted attempts stay quarantined under their own UUID. A later
    # attempt can still preserve the same epoch, without deleting partial data
    # or mistaking an incomplete directory for an immutable committed seal.
    stage = root.with_name(".partial-" + sha + "-" + uuid.uuid4().hex)
    stage.mkdir(parents=True, mode=0o700)
    for row in inventory["files"]:
        source = absolute(str(Path(spec["output_root"]) / relative(row["relative"])))
        destination = stage / row["relative"]
        destination.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        before = signature(source)
        with source.open("rb") as src, destination.open("xb") as dst:
            shutil.copyfileobj(src, dst, 4 * 1024**2); dst.flush(); os.fsync(dst.fileno())
        if signature(source) != before or file_hash(destination) != row["sha256"]:
            raise ValueError("source advanced while sealing; retain incomplete seal, do not acknowledge")
        destination.chmod(row["mode"] & 0o777)
    after = inspect_run(spec)
    if after["context_sha256"] != sha:
        raise ValueError("source crossed epoch boundary while sealing; do not acknowledge")
    atomic_json(stage / ".sealed.json", inventory)
    os.rename(stage, root)
    return {**inventory, "sealed_root": str(root)}


def ack_seal(spec: dict, state_root: Path, sha: str, commit_id: str) -> dict:
    if not HASH.fullmatch(sha) or not HASH.fullmatch(commit_id):
        raise ValueError("invalid D commit ACK")
    root = absolute(str(state_root / "sealed" / sha))
    if not root.exists():
        return {"state": "already_acknowledged", "source_files_deleted": False}
    saved = json.loads((root / ".sealed.json").read_bytes())
    if (saved["context_sha256"] != sha or digest(saved["files"]) != sha
            or saved["spec_sha256"] != digest(spec)):
        raise ValueError("ACK does not own this private seal")
    expected = {r["relative"] for r in saved["files"]} | {".sealed.json"}
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if not p.is_dir()}
    if actual != expected or any(p.is_symlink() for p in root.rglob("*")):
        raise ValueError("private seal contains unexpected data; never recursively delete it")
    for row in saved["files"]:
        if file_hash(root / row["relative"]) != row["sha256"]:
            raise ValueError("private seal changed before acknowledged staging retirement")
    atomic_json(state_root / "acks" / (sha + ".json"), {"D_commit_id": commit_id, "acknowledged_at": time.time()})
    shutil.rmtree(root)  # Exact validated newly-created transport copy, NEVER source/cold data.
    return {"state": "verified_transport_copy_retired", "source_files_deleted": False}


def read_member(root: Path, row: dict) -> None:
    count, sha = row["bytes"], row["sha256"]
    offset, prefix_sha = row.get("resume_offset", 0), row.get("prefix_sha256")
    if (type(count) is not int or count < 0 or not isinstance(sha, str) or not HASH.fullmatch(sha)
            or type(offset) is not int or not 0 <= offset <= count
            or (prefix_sha is not None and (not isinstance(prefix_sha, str) or not HASH.fullmatch(prefix_sha)))
            or (offset and prefix_sha is None)):
        raise ValueError("invalid immutable transfer offset/prefix identity")
    p = absolute(str(root / relative(row["relative"])))
    before = signature(p)
    if before[2] != count or file_hash(p) != sha:
        raise ValueError("source member no longer matches the sealed inventory")
    h = hashlib.sha256()
    with p.open("rb") as stream:
        remaining = offset
        while remaining:
            block = stream.read(min(4 * 1024**2, remaining))
            if not block:
                raise ValueError("source prefix truncated before transfer")
            h.update(block)
            remaining -= len(block)
        if prefix_sha is not None and h.hexdigest() != prefix_sha:
            raise ValueError("retained D prefix differs from the sealed source; no bytes sent")
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            h.update(block)
            sys.stdout.buffer.write(block)
    sys.stdout.buffer.flush()
    if signature(p) != before or h.hexdigest() != sha:
        raise ValueError("source changed during transfer; receiver must reject")


@contextmanager
def _receive_parents(path: Path):
    """Hold the exact directory chain through no-overwrite publication."""
    absolute(str(path))
    chain = []
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        directory = Path("/")
        descriptor = os.open(directory, flags)
        chain.append((directory, descriptor))
        for part in path.parent.parts[1:]:
            directory = directory / part
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass  # The following no-follow directory open decides.
                child = os.open(part, flags, dir_fd=descriptor)
            descriptor = child
            chain.append((directory, descriptor))
        yield descriptor, chain
    finally:
        for _, descriptor in reversed(chain):
            os.close(descriptor)


def _receive_signature(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns,
            value.st_ctime_ns, value.st_mode, value.st_nlink)


def _receive_stream(descriptor: int, mode: str):
    try:
        return os.fdopen(descriptor, mode)
    except BaseException:
        os.close(descriptor)
        raise


def _received_file_identity(descriptor: int, parent: int, name: str):
    value = os.fstat(descriptor)
    if (not stat.S_ISREG(value.st_mode)
            or _receive_signature(value) != _receive_signature(os.stat(name, dir_fd=parent, follow_symlinks=False))):
        raise ValueError("received file changed or is not a closed regular file")
    return _receive_signature(value)


def _sync_receive_parents(chain) -> None:
    # Fsync newly created ancestors too. A durable leaf directory alone does
    # not preserve its own new name after a crash. Retry repeats every barrier.
    def check(path, descriptor):
        held, named = os.fstat(descriptor), path.lstat()
        if not stat.S_ISDIR(named.st_mode) or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino):
            raise ValueError("received directory moved or was redirected")
    device = os.fstat(chain[-1][1]).st_dev
    for path, descriptor in reversed(chain):
        if os.fstat(descriptor).st_dev != device:
            break  # The existing mount's parent belongs to another filesystem.
        check(path, descriptor)
        os.fsync(descriptor)
    for path, descriptor in chain:
        check(path, descriptor)


def receive_file(path: Path, sha: str, count: int, mode: int | None = None) -> dict:
    absolute(str(path))
    if not HASH.fullmatch(sha) or type(count) is not int or count < 0:
        raise ValueError("invalid transfer identity")
    if mode is not None and (type(mode) is not int or not 0 <= mode <= 0o777):
        raise ValueError("invalid target member permissions")
    with _receive_parents(path) as (parent, chain):
        try:
            descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                                 dir_fd=parent)
        except FileNotFoundError:
            descriptor = None
        if descriptor is not None:
            with _receive_stream(descriptor, "rb") as stream:
                before = _received_file_identity(stream.fileno(), parent, path.name)
                if before[2] != count:
                    raise ValueError("never overwrite an existing different target file")
                h = hashlib.sha256()
                for block in iter(lambda: stream.read(4 * 1024**2), b""):
                    h.update(block)
                if h.hexdigest() != sha or _received_file_identity(stream.fileno(), parent, path.name) != before:
                    raise ValueError("never overwrite an existing different target file")
                if mode is not None and stat.S_IMODE(before[-2]) != mode:
                    if before[-1] != 1:
                        raise ValueError("never change permissions of a shared target inode")
                    os.fchmod(stream.fileno(), mode)
                accepted = _received_file_identity(stream.fileno(), parent, path.name)
                os.fsync(stream.fileno())
                _sync_receive_parents(chain)
                if _received_file_identity(stream.fileno(), parent, path.name) != accepted:
                    raise ValueError("received file changed before durable acknowledgement")
            return {"sha256": sha, "bytes": count, "reused": True, "durable": True}
        partial = path.name + ".partial." + uuid.uuid4().hex
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                             0o600, dir_fd=parent)
        h, received = hashlib.sha256(), 0
        with _receive_stream(descriptor, "wb") as stream:
            for block in iter(lambda: sys.stdin.buffer.read(4 * 1024**2), b""):
                received += len(block)
                if received > count:
                    raise ValueError("transfer exceeded exact byte count")
                stream.write(block)
                h.update(block)
            valid = received == count and h.hexdigest() == sha
            if valid and mode is not None:
                os.fchmod(stream.fileno(), mode)
            stream.flush()
            os.fsync(stream.fileno())  # Includes original permissions, before linking.
            if not valid:
                raise ValueError("partial/corrupt transfer; preserve partial and do not activate")
            os.link(partial, path.name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
            os.unlink(partial, dir_fd=parent)  # Only this successfully verified incoming name.
            accepted = _received_file_identity(stream.fileno(), parent, path.name)
            _sync_receive_parents(chain)
            if _received_file_identity(stream.fileno(), parent, path.name) != accepted:
                raise ValueError("received file changed before durable acknowledgement")
        return {"sha256": sha, "bytes": received, "durable": True}


def extract_tar(archive, destination: Path | None, *, maximum_bytes: int,
                absolute_link_prefix: Path | None = None,
                allow_internal_hardlinks: bool = False,
                expected_files: dict | None = None) -> dict:
    # A declared raw capsule can use the same decoder without creating a hot
    # tree. Runtime callers retain their original path/link extraction contract.
    if destination is None and not expected_files:
        raise ValueError("read-only tar verification needs a complete file inventory")
    if destination is not None:
        absolute(str(destination))
        if destination.exists():
            raise ValueError("archive extraction requires a NEW target directory")
        destination.mkdir(parents=True, mode=0o700)
    total, count, seen = 0, 0, set()
    options = ({"fileobj": archive, "mode": "r|*"} if hasattr(archive, "read") else
               {"name": archive, "mode": "r:*"})
    with tarfile.open(**options) as source:
        for m in source:
            name = relative(m.name.rstrip("/"))
            if name in seen or m.isdev() or m.isfifo() or (m.islnk() and not allow_internal_hardlinks):
                raise ValueError("unsafe or repeated archive member")
            seen.add(name)
            expected = None
            if expected_files is not None:
                expected = expected_files.get(name)
                try:
                    matches_time = (expected is not None and
                        Decimal(str(m.pax_headers.get("mtime", m.mtime))) * 10**9 == expected["mtime_ns"])
                except ArithmeticError as error:
                    raise ValueError("invalid original capsule timestamp") from error
                if (expected is None or not m.isfile() or m.size != expected["bytes"]
                        or m.mode != expected["mode"] or not matches_time):
                    raise ValueError("raw capsule member differs from its pinned original inventory")
            path = destination / name if destination is not None else None
            if path is not None:
                absolute(str(path.parent))
            if m.isdir():
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
            elif m.issym():
                # Runtime capsules may have internal relative Conda symlinks.
                target = PurePosixPath(m.linkname)
                internal_absolute = (target.is_absolute() and absolute_link_prefix is not None
                    and Path(m.linkname).is_relative_to(absolute_link_prefix) and ".." not in target.parts)
                if (not internal_absolute and (target.is_absolute()
                        or not (path.parent / m.linkname).resolve().is_relative_to(destination))):
                    raise ValueError("runtime archive symlink escapes its fixed prefix")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.symlink_to(m.linkname)
            elif m.islnk() and allow_internal_hardlinks:
                # Conda can contain regular files sharing an inode. Admit ONLY
                # a previously extracted regular member inside this new stage;
                # never an absolute/external/forward/symlink hardlink target.
                target = absolute(str(destination / relative(m.linkname)))
                if not target.is_relative_to(destination) or not target.is_file() or target.is_symlink():
                    raise ValueError("runtime hardlink does not name an earlier internal regular member")
                os.link(target, path)
            elif m.isfile():
                total += m.size
                if m.size < 0 or total > maximum_bytes or count >= 250000:
                    raise ValueError("archive exceeds enrolled resource bounds")
                if path is not None:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    if path.exists() or path.is_symlink():
                        raise ValueError("archive member would overwrite existing data")
                h, copied = hashlib.sha256(), 0
                with source.extractfile(m) as src:
                    dst = path.open("xb") if path is not None else None
                    try:
                        for block in iter(lambda: src.read(4 * 1024**2), b""):
                            if dst is not None:
                                dst.write(block)
                            if expected is not None:
                                h.update(block)
                            copied += len(block)
                        if dst is not None:
                            dst.flush()
                            os.fsync(dst.fileno())
                    finally:
                        if dst is not None:
                            dst.close()
                if copied != m.size or (expected is not None and h.hexdigest() != expected["sha256"]):
                    raise ValueError("raw capsule original SHA/size differs")
                if path is not None:
                    path.chmod(m.mode & 0o777)
                    if expected is not None:
                        os.utime(path, ns=(expected["mtime_ns"], expected["mtime_ns"]))
                count += 1
            else:
                raise ValueError("unsupported archive entry")
        if expected_files is not None:
            if seen != set(expected_files):
                raise ValueError("raw capsule is missing original members")
            # Force gzip CRC/end-of-stream validation and reject a valid tar
            # prefix followed by hidden data or a second concatenated tar.
            for block in iter(lambda: source.fileobj.read(4 * 1024**2), b""):
                if any(block):
                    raise ValueError("non-padding bytes after raw capsule tar end")
    return {"bytes": total, "files": count}


def input_capsule_proof(binding: dict, raw: bytes) -> dict:
    if (len(raw) != binding["proof_bytes"] or len(raw) > MAX_PREPARED_REFERENCE_BYTES
            or hashlib.sha256(raw).hexdigest() != binding["proof_sha256"]):
        raise ValueError("input capsule proof failed its pinned SHA/size")
    proof = _strict_json(raw)
    if (proof.get("contract") != INPUT_CAPSULE
            or proof.get("source_plan_sha256") != binding["source_plan_sha256"]
            or proof.get("archive_sha256") != binding["archive_sha256"]
            or type(proof.get("archive_bytes")) is not int or proof["archive_bytes"] != binding["archive_bytes"]
            or type(proof.get("original_bytes")) is not int or not 0 < proof["original_bytes"] <= 512 * 1024**3):
        raise ValueError("input capsule proof does not bind the original archive/plan")
    rows = proof.get("files")
    if not isinstance(rows, list) or not 0 < len(rows) <= 100000:
        raise ValueError("input capsule needs a bounded complete original inventory")
    expected = {}
    for row in rows:
        name = relative(row["relative"])
        if (name in expected or type(row.get("bytes")) is not int or row["bytes"] < 0
                or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])
                or type(row.get("mode")) is not int or not 0 <= row["mode"] <= 0o777
                or type(row.get("mtime_ns")) is not int or not 0 <= row["mtime_ns"] < 2**63):
            raise ValueError("invalid original capsule member identity/metadata")
        if not any(name.startswith(prefix + "/") for prefix in binding["subtree_links"].values()):
            raise ValueError("capsule contains an original outside its declared source subtrees")
        expected[name] = row
    if sum(row["bytes"] for row in rows) != proof["original_bytes"]:
        raise ValueError("input capsule original byte count differs")
    for name in expected:
        if any(parent.as_posix() in expected for parent in PurePosixPath(name).parents):
            raise ValueError("input capsule file/directory paths overlap")
    return proof


def decode_input_capsule(stream, binding: dict, proof: dict, destination: Path | None = None) -> dict:
    class HashedReader:
        def __init__(self):
            self.sha, self.count = hashlib.sha256(), 0

        def read(self, size=-1):
            data = stream.read(size)
            self.count += len(data)
            if self.count > binding["archive_bytes"]:
                raise ValueError("input capsule exceeded its enrolled encoded size")
            self.sha.update(data)
            return data

    encoded = HashedReader()
    # GzipFile validates the CRC/ISIZE trailer. tarfile's streaming gzip
    # adapter alone does not provide this independent compressed-frame gate.
    with gzip.GzipFile(fileobj=encoded, mode="rb") as decoded:
        result = extract_tar(decoded, destination, maximum_bytes=proof["original_bytes"],
                             expected_files={row["relative"]: row for row in proof["files"]})
    for _ in iter(lambda: encoded.read(4 * 1024**2), b""):
        pass
    if (encoded.count != binding["archive_bytes"] or encoded.sha.hexdigest() != binding["archive_sha256"]
            or result["bytes"] != proof["original_bytes"]):
        raise ValueError("input capsule encoded archive SHA/size differs")
    return {**result, "archive_sha256": encoded.sha.hexdigest(), "encoded_bytes": encoded.count,
            "original_mode_mtime_SHA_verified": True}


def _verify_capsule_tree(root: Path, proof: dict) -> None:
    expected = {row["relative"]: row for row in proof["files"]}
    expected_dirs = {p.as_posix() for name in expected for p in PurePosixPath(name).parents}
    observed, directories = {}, {}
    def fail(error):
        raise error
    for current, dirs, files in os.walk(root, followlinks=False, onerror=fail):
        parent = absolute(current)
        directories[parent] = signature(parent)
        for name in dirs:
            directory = absolute(str(parent / name))
            if directory.relative_to(root).as_posix() not in expected_dirs:
                raise ValueError("unknown directory in existing input capsule tree")
        for name in files:
            path = absolute(str(parent / name))
            key = path.relative_to(root).as_posix()
            row = expected.get(key)
            before = signature(path)
            if (row is None or not stat.S_ISREG(before[-1]) or before[2:4] != (row["bytes"], row["mtime_ns"])
                    or stat.S_IMODE(before[-1]) != row["mode"] or file_hash(path) != row["sha256"]):
                raise ValueError("existing capsule tree differs from pinned original bytes/metadata")
            observed[path] = before
    if len(observed) != len(expected) or any(signature(path) != before for path, before in {**directories, **observed}.items()):
        raise ValueError("capsule tree is incomplete or changed during verification")


def materialize_capsule(spec_path: Path, index: int, controller: Path, controller_sha: str,
                        proof_path: Path, archive_path: Path | None = None, *, preflight=False) -> dict:
    """Restore an already preserved capsule under the existing rescue owner.

    This is an immutable, shared remote working view, not a second cold store
    or automatic GC owner. Unknown targets/partials are always retained.
    """
    spec = validate_spec(_strict_json(absolute(str(spec_path)).read_bytes()))
    rows = spec.get("data_capsules", [])
    if type(index) is not int or not 0 <= index < len(rows):
        raise ValueError("unknown input capsule index")
    row = rows[index]
    module = _input_cache_controller(spec, controller, controller_sha)
    proof_path = absolute(str(proof_path))
    before = signature(proof_path)
    if not stat.S_ISREG(before[-1]) or before[2] != row["proof_bytes"]:
        raise ValueError("input capsule proof is not its bounded regular object")
    proof = input_capsule_proof(row, proof_path.read_bytes())
    if signature(proof_path) != before:
        raise ValueError("input capsule proof changed")
    target = absolute(row["materialized_root"])
    links = {Path(name): target / subtree for name, subtree in row["subtree_links"].items()}
    marker = target.with_name("." + target.name + ".SOURCE-READY.json")
    identity = {key: row[key] for key in ("archive_sha256", "proof_sha256", "source_plan_sha256")}
    def check():
        for link, destination in links.items():
            module._check_preserved_link(link, destination)
        if target.exists():
            absolute(str(marker))
            if not marker.is_file() or _strict_json(marker.read_bytes()).get("identity") != identity:
                raise ValueError("do not overwrite or adopt an unowned input capsule tree")
        elif marker.exists() or marker.is_symlink():
            raise ValueError("preserve orphaned capsule readiness metadata")
    check()
    result = {"contract": "stockagent.rescued_input_capsule.v1", "spec_sha256": digest(spec),
        "capsule_index": index, **identity, "cache_controller_sha256": controller_sha,
        "preflight_only": preflight, "link_targets": {str(p): str(t) for p, t in links.items()}}
    if preflight:
        return result
    target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    lock = target.with_name("." + target.name + ".source.lock")
    absolute(str(lock))
    with lock.open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        check()
        reused = target.exists()
        if not reused:
            archive_path = absolute(str(archive_path))
            archive_before = signature(archive_path)
            if not stat.S_ISREG(archive_before[-1]) or archive_before[2] != row["archive_bytes"]:
                raise ValueError("input capsule archive is not its pinned regular object")
            if shutil.disk_usage(target.parent).free < proof["original_bytes"] + 32 * 1024**3:
                raise ValueError("input capsule lacks original byte budget plus 32 GiB reserve")
            stage = target.with_name("." + target.name + ".source-partial-" + uuid.uuid4().hex)
            with archive_path.open("rb") as stream:
                decode_input_capsule(stream, row, proof, stage)
            if signature(archive_path) != archive_before:
                raise ValueError("input capsule archive changed during reconstruction")
            _verify_capsule_tree(stage, proof)
            from stockagent.data_sync.desync_snapshots import rename_without_overwrite, _fsync_directory
            rename_without_overwrite(stage, target)
            _fsync_directory(target.parent)
        else:
            _verify_capsule_tree(target, proof)
        for link, destination in links.items():
            if not destination.is_dir():
                raise ValueError("input capsule alias does not name a verified source directory")
            module._atomic_symlink(link, destination, replace_existing=False)
        result.update(original_source_paths_ready=True, reused=reused, files=len(proof["files"]),
            original_bytes=proof["original_bytes"], fresh_GPU_acceptance_claim=False,
            automatic_eviction_authorized=False, retain_until=time.time() + 7 * 86400)
        atomic_json(marker, {"identity": identity, **result})
        atomic_json(spec_path.parent / "source-capsules" / f"{index}.json", result)
    return result


def freeze_runtime(prefix: Path, output: Path) -> dict:
    """Preserve the exact prefix, including pip overlays, not a moving solver.

    Existing prefixes are never modified. Relocation to a different prefix is
    deliberately unsupported; the target must match Linux/arch/glibc and pass
    the original runtime lock plus strict CUDA checks after extraction.
    """
    absolute(str(prefix)); absolute(str(output))
    if output.exists():
        raise ValueError("runtime capsule destination must be fresh")
    rows = []
    output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    with tarfile.open(output, "w:gz", compresslevel=1, dereference=False) as archive:
        for p in sorted(prefix.rglob("*")):
            rel = p.relative_to(prefix).as_posix()
            if "__pycache__" in p.parts or p.suffix == ".pyc":
                continue
            before = signature(p)
            if p.is_symlink():
                target = os.readlink(p)
                if not p.resolve().is_relative_to(prefix):
                    raise ValueError("runtime has an external symlink; enroll its dependency explicitly")
                row = {"relative": rel, "kind": "symlink", "target": target}
            elif p.is_dir():
                continue
            elif p.is_file():
                row = {"relative": rel, "kind": "file", "bytes": p.stat().st_size, "sha256": file_hash(p)}
            else:
                raise ValueError("unsupported runtime special file")
            archive.add(p, arcname=rel, recursive=False)
            if signature(p) != before:
                raise ValueError("runtime changed while packaging; do not publish")
            rows.append(row)
    # Full before/after package stability, not just inventory or package versions.
    for row in rows:
        p = prefix / row["relative"]
        if row["kind"] == "file" and file_hash(p) != row["sha256"]:
            raise ValueError("runtime changed during archive publication")
        if row["kind"] == "symlink" and os.readlink(p) != row["target"]:
            raise ValueError("runtime symlink changed during publication")
    with output.open("rb") as stream:
        os.fsync(stream.fileno())
    proof = {"contract": CONTRACT, "kind": "fixed_prefix_runtime", "prefix": str(prefix),
             "link_target_encoding": "raw_readlink_v2",
             "system": platform.system(), "machine": platform.machine(), "libc": platform.libc_ver(),
             "files": rows, "uncompressed_bytes": sum(r.get("bytes", 0) for r in rows),
             "archive_sha256": file_hash(output), "archive_bytes": output.stat().st_size}
    atomic_json(output.with_suffix(output.suffix + ".json"), proof)
    return {k: v for k, v in proof.items() if k != "files"}


def node_identity() -> dict:
    instance = os.environ.get("CONTAINER_ID", "")
    return {"boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "instance_id": instance if re.fullmatch(r"[0-9]+", instance) else None}


def verify_runtime_capsule(prefix: Path, proof_path: Path) -> dict:
    """Reject drift between packaging and enrollment, not just version drift."""
    proof = json.loads(proof_path.read_bytes())
    if proof.get("kind") != "fixed_prefix_runtime" or proof["prefix"] != str(prefix):
        raise ValueError("runtime capsule does not own this prefix")
    expected = {r["relative"] for r in proof["files"]}
    actual = {p.relative_to(prefix).as_posix() for p in prefix.rglob("*")
              if (p.is_file() or p.is_symlink()) and "__pycache__" not in p.parts and p.suffix != ".pyc"}
    if actual != expected:
        raise ValueError("runtime inventory changed after packaging")
    for row in proof["files"]:
        path = prefix / relative(row["relative"])
        if row["kind"] == "file" and file_hash(path) != row["sha256"]:
            raise ValueError("runtime bytes changed after packaging")
        if row["kind"] == "symlink" and runtime_link_target(path, proof) != row["target"]:
            raise ValueError("runtime link changed after packaging")
    return {"state": "source_runtime_still_matches_closed_capsule", "files": len(expected)}


def runtime_link_target(path: Path, proof: dict) -> str:
    encoding = proof.get("link_target_encoding", "path_normalized_v1")
    if encoding == "raw_readlink_v2":
        return os.readlink(path)
    if encoding == "path_normalized_v1":
        return str(path.readlink())
    raise ValueError("unknown runtime symlink target encoding")


def health() -> dict:
    command = ["nvidia-smi", "--query-gpu=index,uuid,name,gpu_recovery_action", "--format=csv,noheader"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=8)
        if result.returncode:
            return {"state": "gpu_fault", "reason": "nvml_query_failed", "exit_code": result.returncode,
                    "node_identity": node_identity()}
        rows = [line.split(",") for line in result.stdout.splitlines() if line.strip()]
        if not rows or any(len(r) != 4 for r in rows):
            return {"state": "gpu_fault", "reason": "gpu_inventory_invalid", "node_identity": node_identity()}
        devices = [{"index": int(r[0]), "uuid": r[1].strip(), "name": r[2].strip(),
                    "recovery_action": r[3].strip()} for r in rows]
        bad = any(d["recovery_action"].lower() not in {"none", "n/a", "not supported"} for d in devices)
        return {"state": "gpu_fault" if bad else "nvml_visible_not_compute_acceptance", "devices": devices,
                "node_identity": node_identity()}
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return {"state": "gpu_fault", "reason": "nvml_unavailable_or_timeout", "node_identity": node_identity()}


def reconcile_prepared(original: Path, rebuilt: Path, *, source_root: Path,
                       expected_original_sha: str | None = None) -> dict:
    """Restore ORIGINAL manifest bytes only after exact artifact equivalence.

    Elapsed build time is not an observation. Keeping original receipt bytes
    preserves the canonical manifest-based feature fingerprint. The separate
    rebuild receipt records when reconstruction actually happened; this never
    backdates source availability, changes builder identity, or promotes CUDA.
    """
    original_signature = signature(original)
    rebuilt_signature = signature(rebuilt)
    if original_signature[2] > MAX_PREPARED_REFERENCE_BYTES or rebuilt_signature[2] > MAX_PREPARED_REFERENCE_BYTES:
        raise ValueError("prepared manifest exceeds its bounded control resource")
    original_bytes = original.read_bytes()
    original_sha = hashlib.sha256(original_bytes).hexdigest()
    if expected_original_sha is not None and original_sha != expected_original_sha:
        raise ValueError("original prepared manifest SHA changed before reconciliation")
    left, right = _strict_json(original_bytes), _strict_json(rebuilt.read_bytes())
    volatile = {"build_wall_seconds", "projection_wall_seconds"}
    if digest({k: v for k, v in left.items() if k not in volatile}) != digest(
            {k: v for k, v in right.items() if k not in volatile}):
        raise ValueError("rebuilt prepared ABI/member inventory differs from original; do not resume")
    sys.path.insert(0, str(source_root))
    from stockagent.data.factorized_panel import verify_factorized_members
    verify_factorized_members(rebuilt.parent, left)
    rebuilt_sha = file_hash(rebuilt)
    if signature(original) != original_signature or signature(rebuilt) != rebuilt_signature:
        raise ValueError("original prepared manifest changed during reconciliation")
    if rebuilt_sha == original_sha:
        return {"state": "prepared_original_bytes_reconstructed", "sha256": original_sha}
    atomic_json(rebuilt.with_name("training-rescue-rebuild-receipt.json"), {
        "contract": CONTRACT, "rebuilt_at": time.time(), "original_manifest_sha256": original_sha,
        "rebuilt_manifest_sha256": rebuilt_sha, "exact_members_verified": True,
        "changed_semantics": False, "fresh_gpu_acceptance_claim": False})
    # Atomic publication of the already-preserved original reference receipt.
    _restore_prepared_json(rebuilt, original_bytes)
    if signature(original) != original_signature or file_hash(rebuilt) != original_sha:
        raise ValueError("original prepared manifest changed before complete publication")
    return {"state": "prepared_original_bytes_reconstructed", "sha256": file_hash(rebuilt)}


def reconcile_preserved_factorized_auxiliary(reference: dict, *, source_root: Path) -> dict:
    """Restore explicitly preserved side tables; never call them recomputed.

    Original manifest bytes pin all model and auxiliary members. Every model
    member must already match; only the dictionary and the two named nullable
    provenance tables may come from this separately pinned original backup.
    Legacy exact-recomputation reconciliation is unchanged.
    """
    import zipfile
    original = absolute(reference["reference_path"])
    rebuilt = absolute(reference["rebuilt_manifest"])
    bundle = reference["preserved_auxiliary_bundle"]
    if (reference.get("kind") != PRESERVED_AUXILIARY_KIND or type(bundle.get("bytes")) is not int
            or not 0 < bundle["bytes"] <= MAX_PRESERVED_AUXILIARY_BYTES):
        raise ValueError("preserved auxiliary recovery requires its explicit bounded contract")
    archive_path = absolute(bundle["path"])
    before = {path: signature(path) for path in (original, rebuilt, archive_path)}
    if (before[original][2] > MAX_PREPARED_REFERENCE_BYTES or before[rebuilt][2] > MAX_PREPARED_REFERENCE_BYTES
            or before[archive_path][2] != bundle["bytes"]
            or file_hash(original) != reference["reference_sha256"]
            or file_hash(archive_path) != bundle["sha256"]):
        raise ValueError("preserved auxiliary reference or original bundle SHA/size differs")
    left, right = _strict_json(original.read_bytes()), _strict_json(rebuilt.read_bytes())
    if (left.get("contract") != "tw_factorized_nullable_panel_values_lag1_v2"
            or left.get("stock_context_preparation", {}).get("futures_context_only") is not True
            or rebuilt.name != "factorized_manifest.json"):
        raise ValueError("preserved auxiliary recovery is confined to the named stock-context ABI")
    auxiliary = {"feature_dictionary.json": left["feature_dictionary_sha256"]}
    for key in ("observations", "annual_events"):
        for row in left[key]:
            name = relative(row["path"])
            if name in auxiliary or PurePosixPath(name).parent.as_posix() != key or not name.endswith(".parquet"):
                raise ValueError("preserved auxiliary original paths are repeated or outside their named tables")
            auxiliary[name] = row["sha256"]
    if not 1 <= len(auxiliary) <= 1024 or any(not isinstance(sha, str) or not HASH.fullmatch(sha) for sha in auxiliary.values()):
        raise ValueError("preserved auxiliary inventory exceeds its pinned scope")
    def comparable(value):
        body = json.loads(json.dumps(value))
        body.pop("build_wall_seconds", None)
        body.pop("projection_wall_seconds", None)
        body["feature_dictionary_sha256"] = "preserved-original-sha"
        for key in ("observations", "annual_events"):
            for row in body.get(key, []):
                row["sha256"] = "preserved-original-sha"
        return body
    if digest(comparable(left)) != digest(comparable(right)):
        raise ValueError("rebuilt stock model/source ABI differs from the original; do not restore auxiliaries")
    sys.path.insert(0, str(source_root))
    from stockagent.data.factorized_panel import factorized_member_proofs
    expected = {row["path"]: row for row in factorized_member_proofs(left)}
    current = {row["path"]: row for row in factorized_member_proofs(right)}
    if (len(expected) != len(factorized_member_proofs(left)) or set(expected) != set(current)
            or not set(auxiliary) <= expected.keys()):
        raise ValueError("stock original member inventory is repeated or differs")
    root = rebuilt.parent
    generations, replacements, model_files = {}, {}, 0
    for name, row in expected.items():
        path = absolute(str(root / relative(name)))
        generations[path] = signature(path)
        actual = file_hash(path)
        if name not in auxiliary:
            if actual != row["sha256"]:
                raise ValueError("rebuilt stock model member differs; no auxiliary original can be installed")
            model_files += 1
        elif actual not in {row["sha256"], current[name]["sha256"]}:
            raise ValueError("rebuilt auxiliary is neither its declared builder output nor its exact original")
        elif actual != row["sha256"]:
            replacements[name] = actual
    staged, staged_signatures, journal = {}, {}, None
    with zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or set(names) != set(auxiliary) | {"ORIGINAL_AUXILIARY.json"}:
            raise ValueError("preserved auxiliary archive contains missing, repeated or extra members")
        control = archive.getinfo("ORIGINAL_AUXILIARY.json")
        if control.file_size > MAX_PREPARED_REFERENCE_BYTES:
            raise ValueError("preserved auxiliary archive index exceeds its control bound")
        index = _strict_json(archive.read(control))
        rows = index.get("files", [])
        if (index.get("contract") != PRESERVED_AUXILIARY_CONTRACT
                or index.get("original_manifest_sha256") != reference["reference_sha256"]
                or not isinstance(rows, list) or len(rows) != len(auxiliary)):
            raise ValueError("preserved auxiliary index differs from the pinned original manifest")
        indexed = {}
        for row in rows:
            name = relative(row["relative"])
            if (name not in auxiliary or name in indexed or row.get("sha256") != auxiliary[name]
                    or type(row.get("bytes")) is not int or not 0 < row["bytes"] <= 1024**3
                    or archive.getinfo(name).file_size != row["bytes"]):
                raise ValueError("preserved auxiliary member has an invalid original SHA/size binding")
            info = archive.getinfo(name)
            mode = (info.external_attr >> 16) & 0o170000
            if mode not in {0, stat.S_IFREG} or info.flag_bits & 1:
                raise ValueError("preserved auxiliary archive requires regular unencrypted original members")
            indexed[name] = row
        if sum(row["bytes"] for row in indexed.values()) > MAX_PRESERVED_AUXILIARY_BYTES:
            raise ValueError("preserved auxiliary decoded bytes exceed their recovery bound")
        needed = sum(indexed[name]["bytes"] for name in replacements)
        if needed and shutil.disk_usage(root.parent).free < 25 * 1024**3:
            raise ValueError("preserved auxiliary staging would violate the existing 25 GiB reserve")
        if replacements:
            plan = {"contract": PRESERVED_AUXILIARY_CONTRACT, "output_root": str(root),
                    "original_manifest_sha256": reference["reference_sha256"], "bundle_sha256": bundle["sha256"],
                    "original_inventory_sha256": digest(indexed)}
            journal = absolute(str(root.parent / (".rescue-preserved-auxiliary-" + digest(plan))))
            if journal.exists():
                control_path = journal / "RESTORATION.private.json"
                if (not journal.is_dir() or journal.stat().st_mode & 0o077
                        or not control_path.is_file() or control_path.is_symlink()
                        or control_path.stat().st_size > MAX_PREPARED_REFERENCE_BYTES
                        or _strict_json(control_path.read_bytes()).get("plan") != plan):
                    raise ValueError("preserved auxiliary staging lacks its exact original owner plan")
                allowed = {hashlib.sha256(name.encode()).hexdigest() for name in indexed} | {control_path.name}
                if any(path.name not in allowed for path in journal.iterdir()):
                    raise ValueError("preserved auxiliary staging contains an unowned member")
            else:
                journal.mkdir(mode=0o700)
                atomic_json(journal / "RESTORATION.private.json", {"plan": plan, "state": "planned_before_staging"})
            growth = 0
            for name in replacements:
                path = absolute(str(journal / hashlib.sha256(name.encode()).hexdigest()))
                allocated = 0
                if path.exists():
                    state = path.lstat()
                    if not stat.S_ISREG(state.st_mode) or state.st_nlink != 1 or state.st_size > indexed[name]["bytes"]:
                        raise ValueError("retained auxiliary stage has changed its original file identity or size")
                    allocated = state.st_size
                growth += indexed[name]["bytes"] - allocated
            if shutil.disk_usage(root.parent).free < 25 * 1024**3 + growth:
                raise ValueError("preserved auxiliary stage growth would violate the existing 25 GiB reserve")
        for name, row in indexed.items():
            digest_state, count = hashlib.sha256(), 0
            with ExitStack() as opened:
                source = opened.enter_context(archive.open(name))
                staged_path = (absolute(str(journal / hashlib.sha256(name.encode()).hexdigest()))
                               if name in replacements else None)
                exists = staged_path is not None and staged_path.exists()
                offset = staged_path.stat().st_size if exists else 0
                reuse = exists and offset == row["bytes"]
                if reuse and file_hash(staged_path) != row["sha256"]:
                    raise ValueError("retained auxiliary stage is corrupt; preserve it for explicit recovery")
                target = (opened.enter_context(staged_path.open("r+b" if exists else "xb"))
                          if staged_path is not None and not reuse else None)
                while block := source.read(8 * 1024**2):
                    count += len(block)
                    if count > row["bytes"]:
                        raise ValueError("preserved auxiliary original exceeds its pinned decoded size")
                    digest_state.update(block)
                    if target is not None:
                        prefix = min(len(block), max(0, offset - (count - len(block))))
                        if prefix and target.read(prefix) != block[:prefix]:
                            raise ValueError("retained auxiliary stage prefix differs from its exact original; no append admitted")
                        target.write(block[prefix:])
                if target is not None:
                    target.flush(); os.fsync(target.fileno())
                if staged_path is not None:
                    staged[name] = staged_path
            if count != row["bytes"] or digest_state.hexdigest() != row["sha256"]:
                raise ValueError("preserved auxiliary original failed its full decoded SHA")
            if staged_path is not None:
                staged_signatures[name] = signature(staged_path)
                if file_hash(staged_path) != row["sha256"] or signature(staged_path) != staged_signatures[name]:
                    raise ValueError("staged auxiliary original changed before restoration")
    if (any(signature(path) != saved for path, saved in before.items())
            or any(signature(path) != saved for path, saved in generations.items())):
        raise ValueError("stock recovery changed while originals were staged; no replacement admitted")
    if journal is not None:
        atomic_json(journal / "RESTORATION.private.json", {
            "plan": plan, "replacements": replacements, "state": "staged_originals_verified"})
    for name, staged_path in staged.items():
        path = root / name
        if signature(path) != generations[path] or signature(staged_path) != staged_signatures[name]:
            raise ValueError("stock auxiliary changed before installing its exact original")
        os.chmod(staged_path, generations[path][-1] & 0o777)
        os.replace(staged_path, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    # Only the pinned originals' SHA fields and volatile build durations may
    # differ. Ordinary reconciliation again verifies EVERY original member.
    if file_hash(rebuilt) != reference["reference_sha256"]:
        _restore_prepared_json(rebuilt, original.read_bytes())
    proof = reconcile_prepared(original, rebuilt, source_root=source_root,
                               expected_original_sha=reference["reference_sha256"])
    if any(signature(path) != saved for path, saved in before.items() if path != rebuilt):
        raise ValueError("preserved auxiliary originals changed during final recovery")
    result = {**proof, "state": "prepared_exact_originals_recomputed_and_preserved_auxiliaries_restored",
              "preserved_auxiliary_bundle_sha256": bundle["sha256"], "recomputed_model_files": model_files,
              "preserved_auxiliary_files": len(auxiliary), "restored_auxiliary_files": len(staged),
              "auxiliary_values_claimed_recomputed": False, "fresh_gpu_acceptance_claim": False}
    if journal is not None:
        atomic_json(journal / "RESTORATION.private.json", {**result, "plan": plan,
                    "state": "complete", "replacements": replacements})
    return result


def _strict_json(raw: bytes) -> dict:
    def pairs(items):
        body = {}
        for key, value in items:
            if key in body:
                raise ValueError("duplicate key in prepared reference JSON")
            body[key] = value
        return body

    def constant(value):
        raise ValueError("nonfinite number in prepared reference JSON")

    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    if not isinstance(value, dict):
        raise ValueError("prepared receipt must be a JSON object")
    json.dumps(value, allow_nan=False)  # Also reject exponent overflow such as 1e999.
    return value


def _prepared_oracle(reference: dict, spec: dict, index: int) -> dict:
    path = absolute(reference["reference_path"])
    before = signature(path)
    if not stat.S_ISREG(before[-1]) or not 0 < before[2] <= MAX_PREPARED_REFERENCE_BYTES:
        raise ValueError("prepared original-file oracle exceeds its metadata bound")
    raw = path.read_bytes()
    if signature(path) != before or hashlib.sha256(raw).hexdigest() != reference["reference_sha256"]:
        raise ValueError("prepared original-file oracle SHA changed")
    oracle = _strict_json(raw)
    step = spec["rebuild_steps"][index]
    builder = PREPARED_BUILDERS.get(oracle.get("artifact_kind"))
    version2 = reference.get("kind") == "file_set_v2"
    contract = PREPARED_REPRESENTATION_FILE_SET if version2 else PREPARED_FILE_SET
    if (oracle.get("contract") != contract or builder != step[0]
            or oracle.get("scope") != _prepared_scope(oracle.get("artifact_kind"))
            or not isinstance(oracle.get("builder_sha256"), str)
            or not HASH.fullmatch(oracle["builder_sha256"])):
        raise ValueError("prepared file-set contract/builder is not the selected canonical builder")
    source = spec.get("rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"]))[index]
    if file_hash(absolute(str(Path(source) / builder))) != oracle["builder_sha256"]:
        raise ValueError("prepared builder differs from original file-set oracle")
    original_root = Path(oracle["original_root"])
    original_link = next((row for row in spec.get("prepared_links", [])
                          if row["step_index"] == index and Path(row["path"]) == original_root), None)
    if original_link:
        # This is historical provenance, now an explicitly declared compatible
        # alias. Validate its parent and literal target without resolving it.
        absolute(str(original_root.parent))
        expected_target = prepared_output(spec["rebuild_steps"][index])[1]
        if original_root.is_symlink() and os.readlink(original_root) != str(expected_target):
            raise ValueError("original prepared alias no longer names its verified rebuild")
    else:
        absolute(oracle["original_root"])
    rows = oracle.get("files")
    if not isinstance(rows, list) or not 0 < len(rows) <= 100000:
        raise ValueError("prepared file-set oracle needs a bounded nonempty complete inventory")
    names, metadata_bytes = set(), 0
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid prepared original file")
        name = relative(row["relative"])
        if (name in names or "\x00" in name or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])
                or type(row.get("bytes")) is not int or row["bytes"] < 0):
            raise ValueError("invalid or duplicate prepared original file")
        names.add(name)
        if "original_json" in row:
            text = row["original_json"]
            if not isinstance(text, str) or not _prepared_json_receipt(oracle["artifact_kind"], name):
                raise ValueError("only original JSON receipts support semantic reconciliation")
            original = text.encode("utf-8")
            metadata_bytes += len(original)
            if (metadata_bytes > 8 * 1024**2 or len(original) != row["bytes"]
                    or hashlib.sha256(original).hexdigest() != row["sha256"]):
                raise ValueError("original prepared JSON bytes differ from their pinned identity")
            _strict_json(original)
    if version2:
        if oracle["artifact_kind"] != "tw_futures_financial_v1":
            raise ValueError("representation reconciliation only supports the named financial controls")
        extra = oracle.get("representation_auxiliary")
        if (not isinstance(extra, dict) or set(extra) != {"relative", "sha256", "bytes"}
                or extra.get("relative") != "materialization/manifest.json"
                or not isinstance(extra.get("sha256"), str) or not HASH.fullmatch(extra["sha256"])
                or type(extra.get("bytes")) is not int or not 0 < extra["bytes"] <= 1024**2):
            raise ValueError("representation oracle needs its exact materialization dependency")
        _prepared_representation_originals(reference, oracle)
    if reference.get("kind") == PRESERVED_FILE_SET_KIND:
        if (oracle["artifact_kind"] != "tw_futures_decision_clocks_v1" or len(rows) > 1024
                or sum(row["bytes"] for row in rows) > MAX_PRESERVED_FILE_SET_BYTES
                or any(row["bytes"] > 64 * 1024**2 for row in rows)
                or any(parent.as_posix() in names for name in names
                       for parent in PurePosixPath(name).parents if parent != PurePosixPath("."))):
            raise ValueError("preserved clock originals exceed their bounded nonoverlapping file scope")
    return oracle


def _restore_preserved_clock_files(reference: dict, spec: dict, index: int) -> dict:
    """Restore exact enrolled clock bytes, never re-evaluate float reductions.

    The atomic prepared owner supplies a previously absent output root. Any
    partial remains owned failure evidence; retries quarantine it. The ZIP is
    content-deduplicated, while original logical names retain their own files.
    """
    import zipfile
    if reference.get("kind") != PRESERVED_FILE_SET_KIND:
        raise ValueError("original clock restoration requires its explicit recovery kind")
    oracle = _prepared_oracle(reference, spec, index)
    bundle = reference["original_file_set_bundle"]
    archive_path = absolute(bundle["path"])
    control_path = absolute(reference["reference_path"])
    before = {p: signature(p) for p in (control_path, archive_path)}
    if (before[archive_path][2] != bundle["bytes"]
            or file_hash(archive_path) != bundle["sha256"]):
        raise ValueError("original clock bundle SHA/size differs")
    rows = [{k: row[k] for k in ("relative", "bytes", "sha256")} for row in oracle["files"]]
    expected = {row["relative"]: row for row in rows}
    objects = {}
    for row in rows:
        sha, size = row["sha256"], row["bytes"]
        if sha in objects and objects[sha] != size:
            raise ValueError("one original content SHA has conflicting sizes")
        objects[sha] = size
    root = absolute(reference["rebuilt_root"])
    with zipfile.ZipFile(archive_path) as archive:
        names = archive.namelist()
        if (len(names) != len(set(names))
                or set(names) != {"ORIGINAL_CLOCK_FILES.json", *("objects/" + sha for sha in objects)}):
            raise ValueError("original clock bundle has missing, repeated or extra members")
        for info in archive.infolist():
            mode = stat.S_IFMT(info.external_attr >> 16)
            if (mode not in {0, stat.S_IFREG} or info.flag_bits & 1
                    or info.compress_type not in {zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED}):
                raise ValueError("original clock bundle needs regular unencrypted members")
        control = archive.getinfo("ORIGINAL_CLOCK_FILES.json")
        if not 0 < control.file_size <= MAX_PREPARED_REFERENCE_BYTES:
            raise ValueError("original clock bundle index exceeds its metadata bound")
        manifest = _strict_json(archive.read(control))
        if (not isinstance(manifest, dict)
                or set(manifest) != {"contract", "original_file_set_oracle_sha256", "artifact_kind", "files"}
                or manifest["contract"] != PRESERVED_CLOCK_FILE_SET
                or manifest["original_file_set_oracle_sha256"] != reference["reference_sha256"]
                or manifest["artifact_kind"] != oracle["artifact_kind"]
                or not isinstance(manifest["files"], list)
                or len(manifest["files"]) != len(rows)):
            raise ValueError("original clock bundle index differs from the pinned oracle")
        indexed = {}
        for row in manifest["files"]:
            if (not isinstance(row, dict) or set(row) != {"relative", "bytes", "sha256"}
                    or type(row["bytes"]) is not int or not isinstance(row["relative"], str)
                    or row["relative"] in indexed
                    or row != expected.get(row["relative"])):
                raise ValueError("original clock index member differs from its exact original")
            indexed[row["relative"]] = row
        if set(indexed) != set(expected) or any(
                archive.getinfo("objects/" + sha).file_size != size for sha, size in objects.items()):
            raise ValueError("original clock object sizes or complete inventory differ")
        if any(signature(p) != value for p, value in before.items()):
            raise ValueError("original clock resources changed before restoration")
        with _receive_parents(root) as (parent, parent_chain):
            needed = sum(row["bytes"] for row in rows)
            if shutil.disk_usage(root.parent).free < 25 * 1024**3 + needed:
                raise ValueError("original clock restoration would violate the 25 GiB reserve")
            os.mkdir(root.name, mode=0o700, dir_fd=parent)  # Never adopt/replace a foreign root.
            original_root = signature(root)[:2]
            for row in rows:
                if signature(root)[:2] != original_root:
                    raise ValueError("original clock output root changed during restoration")
                if shutil.disk_usage(root).free < 25 * 1024**3 + needed:
                    raise ValueError("original clock restoration lost its remaining capacity admission")
                target = root / row["relative"]
                h, count = hashlib.sha256(), 0
                with _receive_parents(target) as (directory, chain):
                    _sync_receive_parents(chain)
                    fd = os.open(target.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=directory)
                    with _receive_stream(fd, "wb") as out, archive.open("objects/" + row["sha256"]) as source:
                        for block in iter(lambda: source.read(4 * 1024**2), b""):
                            count += len(block)
                            if count > row["bytes"]:
                                raise ValueError("original clock decoded member exceeds its pinned size")
                            h.update(block); out.write(block)
                        out.flush(); os.fsync(out.fileno())
                    if count != row["bytes"] or h.hexdigest() != row["sha256"]:
                        raise ValueError("original clock decoded member SHA/size differs")
                    _sync_receive_parents(chain)
                needed -= row["bytes"]
            _sync_receive_parents(parent_chain)
            if signature(root)[:2] != original_root or any(signature(p) != v for p, v in before.items()):
                raise ValueError("original clock resources or output root changed during restoration")
    return {"method": "preserved_original_file_set", "original_files_restored": len(rows),
            "original_bytes_restored": sum(row["bytes"] for row in rows),
            "bundle_sha256": bundle["sha256"], "raw_builder_executed": False}


def _prepared_receipt_identity(body: dict, *, kind: str, name: str, root: Path) -> dict:
    body = dict(body)
    if kind == "tw_futures_financial_v1" and name in {"build_acceptance.json", "release/build_summary.json"}:
        if name == "build_acceptance.json":
            created, elapsed = body.pop("created_at_utc", None), body.pop("elapsed_s", None)
            if (not isinstance(created, str) or not created
                    or type(elapsed) not in {int, float} or not math.isfinite(elapsed) or elapsed < 0):
                raise ValueError("financial build timing receipt is malformed")
        for key in ("daily", "rules"):
            value = body.get(key)
            if not isinstance(value, str) or not Path(value).is_absolute() or ".." in Path(value).parts:
                raise ValueError("financial summary needs exact absolute output paths")
            try:
                body[key] = relative(Path(value).relative_to(root).as_posix())
            except ValueError as error:
                raise ValueError("financial summary path escaped its enrolled output root") from error
    return body


def _prepared_tree(root: Path, *, artifact_kind: str) -> tuple[dict[str, tuple], dict[Path, tuple]]:
    files, directories = {}, {}
    def failed(error):
        raise error
    for directory, folders, names in os.walk(root, followlinks=False, onerror=failed):
        parent = absolute(directory)
        if parent == root and artifact_kind == "tw_futures_financial_v1":
            # Compilation caches and integrated-input copies are remote scratch.
            # This named scope verifies the ENTIRE published release plus its
            # accounting/build evidence, without hashing raw duplicates again.
            folders[:] = [name for name in folders if name == "release"]
            names = [name for name in names if name in FINANCIAL_CONTROLS]
        directories[parent] = signature(parent)
        for name in folders:
            if not stat.S_ISDIR((parent / name).lstat().st_mode):
                raise ValueError("prepared output contains a redirected directory")
        for name in names:
            path = parent / name
            value = signature(path)
            if not stat.S_ISREG(value[-1]):
                raise ValueError("prepared output contains a nonregular member")
            files[path.relative_to(root).as_posix()] = value
        if len(files) > 100000 or len(directories) > 100000:
            raise ValueError("prepared output exceeds the bounded complete inventory")
    return files, directories


def prepared_file_set(root: Path, *, artifact_kind: str, source_root: Path,
                      representation_bundle: Path | None = None) -> dict:
    """Read a closed original tree once; emit bounded metadata, never payloads."""
    root, source_root = absolute(str(root)), absolute(str(source_root))
    builder = PREPARED_BUILDERS.get(artifact_kind)
    if builder is None:
        raise ValueError("unknown canonical prepared artifact kind")
    builder_path = source_root / builder
    builder_sha = file_hash(builder_path)
    before, directories = _prepared_tree(root, artifact_kind=artifact_kind)
    if not before:
        raise ValueError("original prepared tree is empty")
    rows, metadata_bytes = [], 0
    for name, value in sorted(before.items()):
        path = root / name
        row = {"relative": name, "bytes": value[2], "sha256": file_hash(path)}
        # Large source manifests remain exact-SHA files. Bounded original JSON
        # allows receipt formatting/order changes without duplicating raw data.
        if (_prepared_json_receipt(artifact_kind, name) and value[2] <= 1024**2
                and metadata_bytes + value[2] <= 8 * 1024**2):
            raw = path.read_bytes()
            _strict_json(raw)
            if len(raw) != row["bytes"] or hashlib.sha256(raw).hexdigest() != row["sha256"]:
                raise ValueError("original prepared JSON changed during inventory")
            row["original_json"] = raw.decode("utf-8")
            metadata_bytes += len(raw)
        rows.append(row)
    after, after_directories = _prepared_tree(root, artifact_kind=artifact_kind)
    if before != after or directories != after_directories or file_hash(builder_path) != builder_sha:
        raise ValueError("original prepared tree/builder changed during inventory")
    oracle = {"contract": PREPARED_FILE_SET, "artifact_kind": artifact_kind,
        "scope": _prepared_scope(artifact_kind),
        "original_root": str(root), "builder_sha256": builder_sha, "files": rows}
    if representation_bundle is not None:
        if artifact_kind != "tw_futures_financial_v1":
            raise ValueError("only the named financial controls have representation oracles")
        bundle = absolute(str(representation_bundle))
        if bundle == root or root in bundle.parents:
            raise ValueError("original representation bundle must be outside its original tree")
        import zipfile
        originals = {}
        extra = None
        for name in sorted(FINANCIAL_REPRESENTATIONS):
            path = absolute(str(root / name))
            state = signature(path)
            if not stat.S_ISREG(state[-1]) or not 0 < state[2] <= MAX_PREPARED_REFERENCE_BYTES:
                raise ValueError("original representation exceeds its resource bound")
            raw = path.read_bytes()
            if signature(path) != state or hashlib.sha256(raw).hexdigest() != file_hash(path):
                raise ValueError("original representation changed during capture")
            if name in before and signature(path) != before[name]:
                raise ValueError("original representation changed after census")
            originals[name] = raw
            if name == "materialization/manifest.json":
                extra = {"relative": name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        if sum(map(len, originals.values())) > 64 * 1024**2:
            raise ValueError("original representation bundle exceeds its decoded bound")
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for name, raw in originals.items():
                archive.writestr(name, raw)
        raw = payload.getvalue()
        if len(raw) > MAX_PREPARED_REFERENCE_BYTES:
            raise ValueError("original representation bundle exceeds its encoded bound")
        bundle.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        with bundle.open("xb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        oracle.update(contract=PREPARED_REPRESENTATION_FILE_SET, representation_auxiliary=extra)
        after, after_directories = _prepared_tree(root, artifact_kind=artifact_kind)
        if (before != after or directories != after_directories
                or file_hash(root / extra["relative"]) != extra["sha256"]):
            raise ValueError("original financial tree changed during representation capture")
    if len(json.dumps(oracle, ensure_ascii=False).encode()) > MAX_PREPARED_REFERENCE_BYTES:
        raise ValueError("prepared original-file oracle exceeds its metadata bound")
    return oracle


def _prepared_representation_originals(reference: dict, oracle: dict) -> dict[str, bytes]:
    """Read five exact control originals; never unpack into a prepared tree."""
    import zipfile
    bundle = reference["representation_bundle"]
    path = absolute(bundle["path"])
    before = signature(path)
    if (not stat.S_ISREG(before[-1]) or before[2] != bundle["bytes"]
            or not 0 < before[2] <= MAX_PREPARED_REFERENCE_BYTES):
        raise ValueError("original representation bundle changed or exceeds its bound")
    raw = path.read_bytes()
    if signature(path) != before or hashlib.sha256(raw).hexdigest() != bundle["sha256"]:
        raise ValueError("original representation bundle SHA changed")
    expected = {row["relative"]: row for row in oracle["files"]}
    extra = oracle["representation_auxiliary"]
    expected[extra["relative"]] = extra
    if not FINANCIAL_REPRESENTATIONS <= expected.keys():
        raise ValueError("financial representation oracle is incomplete")
    result = {}
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if (len(members) != len(FINANCIAL_REPRESENTATIONS)
                or {row.filename for row in members} != FINANCIAL_REPRESENTATIONS
                or sum(row.file_size for row in members) > 64 * 1024**2):
            raise ValueError("original representation bundle has an unexpected member scope")
        for info in members:
            row = expected[info.filename]
            if (info.is_dir() or stat.S_ISLNK(info.external_attr >> 16)
                    or info.file_size != row["bytes"] or info.file_size > MAX_PREPARED_REFERENCE_BYTES):
                raise ValueError("original representation member identity/bound differs")
            payload = archive.read(info)
            if hashlib.sha256(payload).hexdigest() != row["sha256"]:
                raise ValueError("original representation member SHA differs")
            result[info.filename] = payload
    return result


def _ordered_prepared_parquet_identity(raw: bytes) -> str:
    """Exact ordered Arrow values/types/bits, independent of Parquet encoding.

    Identity take canonicalizes offsets, chunks and NULL backing buffers. It
    does not sort, cast, round, fill, or normalize observed floating-point bits.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    source = pa.BufferReader(raw)
    metadata = pq.read_metadata(source)
    if metadata.num_rows > 1000000 or metadata.num_columns > 128:
        raise ValueError("financial control Parquet exceeds its decoded shape bound")
    table = pq.read_table(pa.BufferReader(raw))
    if table.nbytes > 256 * 1024**2:
        raise ValueError("financial control Parquet exceeds its decoded byte bound")
    table = table.take(pa.array(range(table.num_rows), type=pa.int64())).combine_chunks()
    target = pa.BufferOutputStream()
    with pa.ipc.new_stream(target, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(target.getvalue()).hexdigest()


def _financial_representation_json(body: dict, name: str, root: Path, identities: dict) -> dict:
    """Normalize only witnessed representation edges in the financial DAG."""
    body = _prepared_receipt_identity(body, kind="tw_futures_financial_v1", name=name, root=root)

    def link(container, key, target):
        current, original = identities[target]
        if container.get(key) != current:
            raise ValueError("financial representation SHA link does not name its actual dependency")
        container[key] = original

    if name in {"build_acceptance.json", "release/rules/sources/admission_review.json"}:
        link(body, "lifetime_scope_sha256", "lifetime_scope.parquet")
    if name in {"release/daily/manifest.json", "release/rules/manifest.json"}:
        link(body, "source_materialization_sha256", "materialization/manifest.json")
    if name in {"materialization/manifest.json", "release/daily/manifest.json"}:
        values = body.get("zero_print_products")
        if (not isinstance(values, list) or not all(isinstance(v, str) and v for v in values)
                or len(values) != len(set(values))):
            raise ValueError("zero-print products must be an exact unique product set")
        body["zero_print_products"] = sorted(values)
    if name == "release/rules/manifest.json":
        target = "release/rules/sources/context_only_suppressed_carries.parquet"
        item = body.get("context_only_prefix", {}).get("suppressed_carries", {})
        if item.get("path") != "sources/context_only_suppressed_carries.parquet":
            raise ValueError("suppressed-carry representation has an unexpected source path")
        link(item, "sha256", target)
        selected = {"sources/context_only_suppressed_carries.parquet": target,
                    "sources/admission_review.json": "release/rules/sources/admission_review.json"}
        counts = dict.fromkeys(selected, 0)
        for row in body.get("sources", []):
            if row.get("path") in selected:
                counts[row["path"]] += 1
                link(row, "sha256", selected[row["path"]])
        if any(value != 1 for value in counts.values()):
            raise ValueError("financial representation source links are missing or ambiguous")
    return body


def _reconcile_financial_representations(reference: dict, oracle: dict) -> dict:
    """Prove the complete output before restoring finite encoding controls.

    The ordinary full SHA reconciler runs again afterwards. This never accepts
    alternative financial tensors, different row order, or changed rule values.
    """
    originals = _prepared_representation_originals(reference, oracle)
    root = absolute(reference["rebuilt_root"])
    before, directories = _prepared_tree(root, artifact_kind=oracle["artifact_kind"])
    expected = {row["relative"]: row for row in oracle["files"]}
    if before.keys() != expected.keys():
        raise ValueError("rebuilt prepared file set differs from complete original inventory")
    extra = oracle["representation_auxiliary"]
    auxiliary = absolute(str(root / extra["relative"]))
    auxiliary_state, auxiliary_parent = signature(auxiliary), signature(auxiliary.parent)
    if not stat.S_ISREG(auxiliary_state[-1]):
        raise ValueError("materialization dependency is not a closed regular control")
    expected[extra["relative"]] = extra
    actual = {name: file_hash(root / name) for name in expected}
    graph = {name: (actual[name], row["sha256"]) for name, row in expected.items()}
    original_graph = {name: (row["sha256"], row["sha256"]) for name, row in expected.items()}
    json_controls = {name for name in FINANCIAL_REPRESENTATIONS if name.endswith(".json")}
    json_controls |= {"build_acceptance.json", "release/daily/manifest.json"}
    replacements, parquet_proofs = [], {}
    for name, row in expected.items():
        path = root / name
        if actual[name] == row["sha256"] and path.stat().st_size == row["bytes"]:
            # Representation edges must still agree with their live children.
            if name not in json_controls:
                continue
        original = originals.get(name)
        if original is None and "original_json" in row:
            original = row["original_json"].encode("utf-8")
        if original is None:
            raise ValueError("rebuilt prepared original-file SHA/size differs; do not resume")
        if path.stat().st_size > MAX_PREPARED_REFERENCE_BYTES:
            raise ValueError("rebuilt financial representation exceeds its resource bound")
        if name.endswith(".parquet"):
            if name not in FINANCIAL_REPRESENTATIONS or path.stat().st_size > MAX_PREPARED_REFERENCE_BYTES:
                raise ValueError("only the two bounded financial controls support encoding reconciliation")
            canonical = _ordered_prepared_parquet_identity(original)
            if _ordered_prepared_parquet_identity(path.read_bytes()) != canonical:
                raise ValueError("ordered financial control values/types/bits differ; do not resume")
            parquet_proofs[name] = canonical
        elif name in json_controls:
            left = _financial_representation_json(_strict_json(original), name, Path(oracle["original_root"]), original_graph)
            # A previous successful restoration deliberately preserved original
            # absolute receipt paths. Accept those paths only with exact bytes.
            receipt_root = Path(oracle["original_root"]) if actual[name] == row["sha256"] else root
            right = _financial_representation_json(_strict_json(path.read_bytes()), name, receipt_root, graph)
            if digest(left) != digest(right):
                raise ValueError("financial representation JSON semantics differ; do not resume")
        else:
            left = _prepared_receipt_identity(_strict_json(original), kind=oracle["artifact_kind"],
                                              name=name, root=Path(oracle["original_root"]))
            right = _prepared_receipt_identity(_strict_json(path.read_bytes()), kind=oracle["artifact_kind"], name=name, root=root)
            if digest(left) != digest(right):
                raise ValueError("rebuilt prepared JSON semantics differ; do not resume")
        if actual[name] != row["sha256"]:
            replacements.append((path, original, actual[name]))
    if (any(signature(root / name) != value for name, value in before.items())
            or any(signature(path) != value for path, value in directories.items())
            or signature(auxiliary) != auxiliary_state or signature(auxiliary.parent) != auxiliary_parent):
        raise ValueError("prepared output changed during representation verification")
    bundle = reference["representation_bundle"]
    if file_hash(absolute(bundle["path"])) != bundle["sha256"]:
        raise ValueError("original representation bundle changed before restoration")
    if file_hash(absolute(reference["reference_path"])) != reference["reference_sha256"]:
        raise ValueError("original representation oracle changed before restoration")
    restored = []
    for path, raw, previous in replacements:
        _restore_prepared_json(path, raw)
        name = path.relative_to(root).as_posix()
        if file_hash(path) != expected[name]["sha256"]:
            raise ValueError("restored financial representation failed full original SHA")
        restored.append({"relative": name, "rebuilt_sha256": previous, "original_sha256": expected[name]["sha256"]})
    if file_hash(auxiliary) != extra["sha256"]:
        raise ValueError("restored materialization dependency differs from its original identity")
    if file_hash(absolute(bundle["path"])) != bundle["sha256"]:
        raise ValueError("original representation bundle changed during restoration")
    return {"contract": "finite_financial_representation_reconciliation_v1",
            "ordered_arrow_bit_identities": parquet_proofs, "restored_originals": restored,
            "materialization_dependency_sha256": extra["sha256"], "changed_financial_values": False}


def _restore_prepared_json(path: Path, raw: bytes) -> None:
    descriptor, name = tempfile.mkstemp(prefix=".rescue-receipt-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), path.stat().st_mode & 0o777)
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(name, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if os.path.exists(name):
            os.unlink(name)  # Only this newly created receipt temporary.


def reconcile_prepared_files(reference: dict, spec: dict, index: int) -> dict:
    """Verify every output before restoring equivalent original JSON receipts.

    Binary data remain byte-exact. JSON values/types stay exact; only financial
    build timing and output-root relocation have named, narrow exceptions.
    Actual reconstruction time and replacement hashes live in the controller
    marker, outside the original prepared file set.
    """
    oracle = _prepared_oracle(reference, spec, index)
    representations = None
    if reference.get("kind") == "file_set_v2":
        representations = _reconcile_financial_representations(reference, oracle)
    root = absolute(reference["rebuilt_root"])
    before, directories = _prepared_tree(root, artifact_kind=oracle["artifact_kind"])
    expected = {row["relative"]: row for row in oracle["files"]}
    if set(before) != set(expected):
        raise ValueError("rebuilt prepared file set differs from complete original inventory")
    replacements = []
    for name, row in expected.items():
        path = root / name
        actual = file_hash(path)
        if actual == row["sha256"] and before[name][2] == row["bytes"]:
            continue
        if (reference.get("kind") == PRESERVED_FILE_SET_KIND
                or "original_json" not in row or before[name][2] > 8 * 1024**2):
            raise ValueError("rebuilt prepared original-file SHA/size differs; do not resume")
        left = _prepared_receipt_identity(_strict_json(row["original_json"].encode()),
            kind=oracle["artifact_kind"], name=name, root=Path(oracle["original_root"]))
        right = _prepared_receipt_identity(_strict_json(path.read_bytes()),
            kind=oracle["artifact_kind"], name=name, root=root)
        if digest(left) != digest(right):
            raise ValueError("rebuilt prepared JSON semantics differ; do not resume")
        replacements.append((path, row["original_json"].encode("utf-8"), actual))
    if (any(signature(root / name) != value for name, value in before.items())
            or any(signature(path) != value for path, value in directories.items())):
        raise ValueError("prepared output changed during original-file verification")
    # No original receipt is written until ALL data and JSON semantics pass.
    restored, replacement_signatures = [], {}
    for path, raw, actual in replacements:
        _restore_prepared_json(path, raw)
        sha = hashlib.sha256(raw).hexdigest()
        written = signature(path)
        if file_hash(path) != sha or signature(path) != written:
            raise ValueError("restored original JSON receipt failed SHA readback")
        replacement_signatures[path.relative_to(root).as_posix()] = written
        restored.append({"relative": path.relative_to(root).as_posix(),
                         "rebuilt_sha256": actual, "original_sha256": sha})
    after, _ = _prepared_tree(root, artifact_kind=oracle["artifact_kind"])
    if set(after) != set(expected) or any(
            signature(root / name) != value for name, value in after.items()):
        raise ValueError("prepared output changed before completion")
    replaced = {row["relative"] for row in restored}
    if any(after[name] != before[name] for name in expected if name not in replaced):
        raise ValueError("prepared data changed during receipt restoration")
    if any(after[name] != value for name, value in replacement_signatures.items()):
        raise ValueError("restored prepared JSON changed before completion")
    if file_hash(Path(reference["reference_path"])) != reference["reference_sha256"]:
        raise ValueError("prepared original-file oracle changed before completion")
    return {"state": "prepared_original_file_set_reconstructed", "artifact_kind": oracle["artifact_kind"],
        "reference_sha256": reference["reference_sha256"], "files": len(expected),
        "original_bytes": sum(row["bytes"] for row in expected.values()),
        "restored_json_receipts": restored, "verified_at": time.time(),
        "changed_semantics": False, "fresh_gpu_acceptance_claim": False,
        **({"representation_reconciliation": representations} if representations is not None else {})}


def prepared_link_preflight(spec_path: Path, controller: Path, controller_sha: str) -> dict:
    spec = validate_spec(_strict_json(absolute(str(spec_path)).read_bytes()))
    module = _input_cache_controller(spec, controller, controller_sha)
    links = [row for row in source_aliases(spec) if "step_index" in row]
    for row in links:
        module._check_preserved_link(Path(row["path"]), Path(row["target"]))
    return {"spec_sha256": digest(spec), "preflight_only": True, "cache_controller_sha256": controller_sha,
            "prepared_link_targets": {row["path"]: row["target"] for row in links}}


def prepare_step(spec_path: Path, index: int, *, finish: bool = False,
                 controller: Path | None = None, controller_sha: str | None = None) -> dict:
    """Recover interrupted CPU builds without overwriting an unowned view."""
    spec = validate_spec(json.loads(spec_path.read_bytes()))
    if spec.get("prepared_build_policy") == PREPARED_BUILD_POLICY:
        raise ValueError("shared prepared builds require atomic prepare-run")
    steps = spec.get("rebuild_steps", [])
    if type(index) is not int or not 0 <= index < len(steps):
        raise ValueError("unknown preparation step")
    links = [row for row in source_aliases(spec) if row.get("step_index") == index]
    module = _input_cache_controller(spec, controller, controller_sha) if links else None
    for row in links:
        module._check_preserved_link(Path(row["path"]), Path(row["target"]))
    marker = absolute(str(spec_path.parent / "rebuild" / (str(index) + ".json")))
    marker.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    descriptor = os.open(marker.with_suffix(".lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a+b") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise ValueError("prepared step lock is not a regular file")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _prepare_step(spec, marker, index, finish=finish)
        if links and result.get("skip_builder"):
            # The same step lock covers full original-byte reconciliation and
            # alias installation. Never publish a path from a complete marker
            # alone, or rewrite the frozen training config to find a new view.
            for row in links:
                module._atomic_symlink(Path(row["path"]), Path(row["target"]), replace_existing=False)
            result.update(prepared_link_targets={row["path"]: row["target"] for row in links},
                          cache_controller_sha256=controller_sha)
        return result


def _assert_no_prepared_writer(root: Path) -> None:
    for cmdline in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            args = cmdline.read_bytes().decode(errors="replace").split("\0")
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Other read failures are unknown ownership, not proof of no writer.
        for option in ("--output-root", "--output"):
            values = [args[i + 1] for i, value in enumerate(args[:-1]) if value == option]
            values += [value[len(option) + 1:] for value in args if value.startswith(option + "=")]
            for value in values:
                candidate = Path(value)
                if not candidate.is_absolute():
                    try:
                        candidate = (cmdline.parent / "cwd").readlink() / candidate
                    except (FileNotFoundError, ProcessLookupError):
                        continue
                if candidate.resolve() == root:
                    raise ValueError("previous prepared builder still running; do not race or restart it")


def _prepare_step(spec: dict, marker: Path, index: int, *, finish: bool) -> dict:
    steps = spec["rebuild_steps"]
    step = steps[index]
    _, root = prepared_output(step)
    owned = {"spec_sha256": digest(spec), "output_root": str(root), "step_sha256": digest(step)}
    saved = json.loads(marker.read_bytes()) if marker.exists() else None
    if saved is not None and any(saved.get(k) != v for k, v in owned.items()):
        raise ValueError("another preparation owns this output")
    references = preparation_references(spec)[index]
    for reference in references:
        if reference.get("kind") in FILE_SET_KINDS:
            _prepared_oracle(reference, spec, index)
    # The builder can be alive before its first mkdir. An absent directory is
    # never evidence that an earlier admitted CPU writer has exited.
    _assert_no_prepared_writer(root)
    if finish or (saved and saved.get("state") == "complete"):
        if not saved or not references:
            raise ValueError("prepared completion needs owned original-byte reconciliation")
        proofs = _reconcile_prepared_step(spec, index)
        atomic_json(marker, {**owned, "state": "complete", "verified_at": time.time(), "proofs": proofs})
        return {"state": "prepared_exact_members_already_verified", "skip_builder": True}
    if root.exists():
        if saved is None:
            raise ValueError("do not overwrite or quarantine an unowned prepared directory")
        quarantine = root.with_name(root.name + ".rescue-incomplete-" + uuid.uuid4().hex)
        os.rename(root, quarantine)  # Keep this exact owned partial, never delete inputs.
    atomic_json(marker, {**owned, "state": "building", "started_at": time.time()})
    return {"state": "owned_fresh_preparation_admitted", "skip_builder": False}


def _reconcile_prepared_step(spec: dict, index: int) -> list[dict]:
    proofs = []
    for ref in preparation_references(spec)[index]:
        if ref.get("kind") in FILE_SET_KINDS:
            proofs.append(reconcile_prepared_files(ref, spec, index))
        else:
            if ref.get("reference_sha256") and file_hash(Path(ref["reference_path"])) != ref["reference_sha256"]:
                raise ValueError("original factorized reference SHA changed")
            source = spec.get("rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"]))[index]
            if ref.get("kind") == PRESERVED_AUXILIARY_KIND:
                proofs.append(reconcile_preserved_factorized_auxiliary(ref, source_root=Path(source)))
            else:
                proofs.append(reconcile_prepared(Path(ref["reference_path"]), Path(ref["rebuilt_manifest"]),
                                                source_root=Path(source), expected_original_sha=ref.get("reference_sha256")))
    return proofs


def prepared_build_identity(spec: dict, index: int) -> dict:
    """Bind shared CPU work to its inputs, not a training run's output/clock."""
    steps = spec.get("rebuild_steps", [])
    if type(index) is not int or not 0 <= index < len(steps):
        raise ValueError("unknown preparation step")
    refs = preparation_references(spec, require_complete=True)
    def original_artifact_reference(row):
        result = {k: v for k, v in row.items() if k != "step_index"}
        if result.get("kind") == PRESERVED_AUXILIARY_KIND:
            # The original manifest already pins every preserved member.
            # Archive transport/encoding changes the immutable recipe SHA,
            # never the identity of these exact same original output bytes.
            result["kind"] = "factorized_manifest_v1"
            result.pop("preserved_auxiliary_bundle")
        return result
    sources = spec.get("rebuild_source_roots", [spec["source_root"]] * len(steps))[:index + 1]
    working = spec.get("rebuild_working_directories", spec.get(
        "rebuild_source_roots", [spec["source_root"]] * len(steps)))[:index + 1]
    result = {"contract": "stockagent.prepared_build_inputs.v1",
        "steps": steps[:index + 1],
        "source_roots": sources,
        "runtime_prefix": spec["runtime_prefix"], "environment": spec.get("environment", {}),
        "sources": spec.get("data_sources", []), "capsules": spec.get("data_capsules", []),
        "controls": spec.get("input_controls", []),
        "references": [original_artifact_reference(row)
                       for i in range(index + 1) for row in refs[i]]}
    # Explicitly spelling the legacy default keeps existing complete artifacts
    # reusable. A different cwd changes relative input resolution and must own
    # a different build identity, including all its downstream steps.
    if working != sources:
        result["working_directories"] = working
    return result


@contextmanager
def _prepared_lock(path: Path, *, shared: bool = False, create: bool = True):
    with _receive_parents(path) as (parent, chain):
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_CREAT if create else 0)
        fd = os.open(path.name, flags, 0o600, dir_fd=parent)
        with os.fdopen(fd, "a+b") as lock:
            before = _received_file_identity(fd, parent, path.name)
            fcntl.flock(fd, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
            def check():
                if _received_file_identity(fd, parent, path.name) != before:
                    raise ValueError("prepared build lock inode changed")
                _sync_receive_parents(chain)
            check()
            yield fd, check
            check()


def _prepared_control(path: Path) -> dict:
    path = absolute(str(path))
    before = signature(path)
    if not stat.S_ISREG(before[-1]) or before[2] > MAX_PREPARED_REFERENCE_BYTES:
        raise ValueError("prepared ownership control is not a bounded regular file")
    body = _strict_json(path.read_bytes())
    if signature(path) != before:
        raise ValueError("prepared ownership control changed while reading")
    return body


def _prepared_hierarchy(root: Path) -> list[Path]:
    namespace = next(p for p in root.parents if p.name == "data_preparation" and p.parent.name == "artifacts")
    hierarchy = [namespace]
    for part in root.relative_to(namespace).parts:
        hierarchy.append(hierarchy[-1] / part)
    if len(hierarchy) > 64:
        raise ValueError("prepared output hierarchy exceeds its ownership bound")
    return hierarchy


def _exact_prepared_originals(spec: dict, index: int) -> dict:
    """Require original encoding before an explicit, read-only owner transfer."""
    result = {}
    for ref in preparation_references(spec, require_complete=True)[index]:
        if file_hash(Path(ref["reference_path"])) != ref["reference_sha256"]:
            raise ValueError("original prepared reference changed before owner transfer")
        if ref.get("kind") not in FILE_SET_KINDS:
            path = absolute(ref["rebuilt_manifest"])
            if file_hash(path) != ref["reference_sha256"]:
                raise ValueError("prepared owner transfer requires exact original manifest bytes")
            result[str(path)] = ref["reference_sha256"]
            if ref.get("kind") == PRESERVED_AUXILIARY_KIND:
                source = spec.get("rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"]))[index]
                sys.path.insert(0, source)
                from stockagent.data.factorized_panel import factorized_member_proofs
                for row in factorized_member_proofs(_strict_json(path.read_bytes())):
                    member = absolute(str(path.parent / relative(row["path"])))
                    if file_hash(member) != row["sha256"]:
                        raise ValueError("prepared owner transfer requires exact original auxiliary/model bytes")
                    result[str(member)] = row["sha256"]
            continue
        oracle = _prepared_oracle(ref, spec, index)
        root = absolute(ref["rebuilt_root"])
        original_rows = list(oracle["files"])
        if ref.get("kind") == "file_set_v2":
            original_rows.append(oracle["representation_auxiliary"])
        for row in original_rows:
            path = absolute(str(root / relative(row["relative"])))
            if path.stat().st_size != row["bytes"] or file_hash(path) != row["sha256"]:
                raise ValueError("prepared owner transfer requires exact original member bytes")
            result[str(path)] = row["sha256"]
    return result


def adopt_prepared_run(spec_path: Path, index: int, *, prior_spec: Path,
                       prior_spec_sha256: str, prior_marker_sha256: str) -> dict:
    """Explicitly transfer a finished legacy CPU job to shared ownership.

    Matching files alone never authorize adoption. Both original owner lock
    inodes and the exact completed legacy marker must exist, be pinned and be
    held throughout independent full readback and durable claim publication.
    Original prepared files, aliases and legacy controls are never rewritten.
    """
    spec_path, prior_spec = absolute(str(spec_path)), absolute(str(prior_spec))
    if spec_path == prior_spec or any(not isinstance(sha, str) or not HASH.fullmatch(sha)
                                     for sha in (prior_spec_sha256, prior_marker_sha256)):
        raise ValueError("pin the distinct original preparation owner and marker")
    original_signatures = {p: signature(p) for p in (spec_path, prior_spec)}
    spec, previous = validate_spec(_prepared_control(spec_path)), validate_spec(_prepared_control(prior_spec))
    if spec.get("prepared_build_policy") != PREPARED_BUILD_POLICY or "prepared_build_policy" in previous:
        raise ValueError("explicit transfer requires the legacy and shared owner contracts")
    if digest(previous) != prior_spec_sha256:
        raise ValueError("prior preparation spec no longer matches the pinned owner")
    identity, old_identity = prepared_build_identity(spec, index), prepared_build_identity(previous, index)
    if digest({k: v for k, v in identity.items() if k != "references"}) != digest(
            {k: v for k, v in old_identity.items() if k != "references"}):
        raise ValueError("prior CPU builder, fixed sources or runtime differ from the shared inputs")
    step = spec["rebuild_steps"][index]
    _, root = prepared_output(step)
    controls = absolute(str(PREPARED_BUILD_ROOT))
    key, artifact_sha = digest(str(root)), digest(identity)
    claim = absolute(str(controls / (key + ".json")))
    marker = absolute(str(spec_path.parent / "rebuild" / (str(index) + ".json")))
    old_marker = absolute(str(prior_spec.parent / "rebuild" / (str(index) + ".json")))
    old_owner = {"spec_sha256": prior_spec_sha256, "output_root": str(root), "step_sha256": digest(step)}
    owned = {"spec_sha256": digest(spec), "output_root": str(root), "step_sha256": digest(step)}
    shared_owned = {"contract": "stockagent.prepared_build_claim.v1", "output_root": str(root),
                    "artifact_sha256": artifact_sha, "inputs": identity}
    with ExitStack() as stack:
        locks = [stack.enter_context(_prepared_lock(prior_spec.parent / "cpu-rebuild.lock", create=False)),
                 stack.enter_context(_prepared_lock(old_marker.with_suffix(".lock"), create=False)),
                 stack.enter_context(_prepared_lock(marker.with_suffix(".lock")))]
        for path in _prepared_hierarchy(root):
            locks.append(stack.enter_context(_prepared_lock(controls / (digest(str(path)) + ".lock"), shared=path != root)))
        def check():
            for _, fence in locks:
                fence()
            if any(signature(p) != sig for p, sig in original_signatures.items()):
                raise ValueError("original preparation owner spec changed during transfer")
            if file_hash(old_marker) != prior_marker_sha256:
                raise ValueError("original preparation marker changed during transfer")
        check()
        original = _prepared_control(old_marker)
        if original.get("state") != "complete" or any(original.get(k) != v for k, v in old_owner.items()):
            raise ValueError("prior preparation lacks its exact completed ownership marker")
        saved = _prepared_control(claim) if claim.exists() else None
        if saved is not None and (saved.get("state") != "complete"
                                  or any(saved.get(k) != v for k, v in shared_owned.items())):
            raise ValueError("another prepared recipe owns this shared output")
        if marker.exists() and any(_prepared_control(marker).get(k) != v for k, v in owned.items()):
            raise ValueError("another preparation owns the receiving run marker")
        _assert_no_prepared_writer(root)
        # Exact prechecks prevent the ordinary reconciler from restoring even
        # permitted encoding/timing differences while ownership is transferred.
        old_members = _exact_prepared_originals(previous, index)
        members = _exact_prepared_originals(spec, index)
        if any(members.get(name) != sha for name, sha in old_members.items()):
            raise ValueError("shared recipe changes an original prepared member")
        generations = {Path(name): signature(Path(name)) for name in members}
        proofs = _reconcile_prepared_step(spec, index)
        if any(signature(path) != sig for path, sig in generations.items()):
            raise ValueError("prepared files changed during read-only owner transfer")
        check()
        origin = {"prior_spec_path": str(prior_spec), "prior_spec_sha256": prior_spec_sha256,
                  "prior_marker_sha256": prior_marker_sha256}
        if saved is None:
            atomic_json(claim, {**shared_owned, "state": "complete", "verified_at": time.time(),
                               "proofs": proofs, "explicit_legacy_owner_transfer": origin})
        atomic_json(marker, {**owned, "state": "complete", "artifact_sha256": artifact_sha,
                             "verified_at": time.time(), "proofs": proofs, "explicit_legacy_owner_transfer": origin})
        check()
        return {"state": "explicit_original_prepared_ownership_transferred", "spec_sha256": digest(spec),
                "step_index": index, "artifact_sha256": artifact_sha, "prior_owner": origin,
                "fresh_full_original_readback": True, "builder_executed": False,
                "prepared_files_rewritten": 0, "source_files_deleted": 0, "reused_shared_claim": saved is not None}


def _record_prepared_failure(claim: Path, marker: Path, *, shared_owned: dict,
                             owned: dict, run_id: str, phase: str,
                             error: BaseException, check_locks) -> None:
    """Append the old claim/marker before changing their current failure state."""
    check_locks()  # A replaced owner inode must never publish over its successor.
    previous = _prepared_control(claim) if claim.exists() else None
    prior_marker = _prepared_control(marker) if marker.exists() else None
    failure = claim.with_name(claim.stem + ".failure-" + uuid.uuid4().hex + ".private.json")
    body = {**shared_owned, "contract": "stockagent.prepared_build_failure.v1",
            "state": "failed", "run_id": run_id, "phase": phase, "failed_at": time.time(),
            "error_type": type(error).__name__, "error": str(error)[:4096],
            "previous_claim": previous, "previous_run_marker": prior_marker}
    log_path = previous.get("log_path") if previous else None
    if log_path:
        body["log_path"] = log_path
        try:
            body["log_sha256"] = file_hash(absolute(log_path))
        except (OSError, ValueError) as log_error:
            body["log_readback_error_type"] = type(log_error).__name__
    # Immutable UUID record: do not replace a previous failure, log or output.
    with failure.open("xb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(json.dumps(body, sort_keys=True, ensure_ascii=False, allow_nan=False).encode() + b"\n")
        stream.flush(); os.fsync(stream.fileno())
    with _receive_parents(failure) as (_, chain):
        _sync_receive_parents(chain)
    check_locks()
    failed = {"state": "failed", "failed_at": body["failed_at"], "failure_path": str(failure),
              "failure_sha256": file_hash(failure), "phase": phase}
    atomic_json(claim, {**(previous or {}), **shared_owned, **failed})
    check_locks()
    atomic_json(marker, {**owned, "artifact_sha256": shared_owned["artifact_sha256"], **failed})


def prepare_run(spec_path: Path, index: int, *, controller: Path | None = None,
                controller_sha: str | None = None, build_timeout: float = 7200) -> dict:
    """Hold kernel ownership through original CPU build and fresh reconciliation.

    This opt-in cannot adopt an unowned existing tree. Interrupted owned trees
    remain in quarantine; every cross-run reuse verifies all original members.
    The builder inherits the lock descriptors, so a dead SSH controller cannot
    release ownership while its original child is still writing.
    """
    spec_path = absolute(str(spec_path))
    spec_signature = signature(spec_path)
    spec = validate_spec(_prepared_control(spec_path))
    if spec.get("prepared_build_policy") != PREPARED_BUILD_POLICY:
        raise ValueError("atomic shared preparation must be explicitly enrolled")
    identity = prepared_build_identity(spec, index)
    artifact_sha = digest(identity)
    step = spec["rebuild_steps"][index]
    _, root = prepared_output(step)
    working = absolute(spec.get("rebuild_working_directories", spec.get(
        "rebuild_source_roots", [spec["source_root"]] * len(spec["rebuild_steps"])))[index])
    if not working.is_dir():
        raise ValueError("prepared working directory is not an existing directory")
    hierarchy = _prepared_hierarchy(root)
    controls = absolute(str(PREPARED_BUILD_ROOT))
    key = digest(str(root))
    claim = absolute(str(controls / (key + ".json")))
    marker = absolute(str(spec_path.parent / "rebuild" / (str(index) + ".json")))
    links = [row for row in source_aliases(spec) if row.get("step_index") == index]
    module = _input_cache_controller(spec, controller, controller_sha) if links else None
    for row in links:
        module._check_preserved_link(Path(row["path"]), Path(row["target"]))
    owned = {"spec_sha256": digest(spec), "output_root": str(root), "step_sha256": digest(step)}
    shared_owned = {"contract": "stockagent.prepared_build_claim.v1", "output_root": str(root),
                    "artifact_sha256": artifact_sha, "inputs": identity}
    with ExitStack() as stack:
        locks = [stack.enter_context(_prepared_lock(marker.with_suffix(".lock")))]
        for path in hierarchy:
            locks.append(stack.enter_context(_prepared_lock(controls / (digest(str(path)) + ".lock"),
                                                             shared=path != root)))
        def check_locks():
            for _, check in locks:
                check()
        saved = _prepared_control(claim) if claim.exists() else None
        run_saved = _prepared_control(marker) if marker.exists() else None
        if run_saved is not None and any(run_saved.get(k) != v for k, v in owned.items()):
            raise ValueError("another preparation owns this run marker")
        if saved is not None and (any(saved.get(k) != v for k, v in shared_owned.items())
                                  or saved.get("state") not in {"building", "complete", "failed"}):
            raise ValueError("another input recipe owns this shared prepared output")
        references = preparation_references(spec, require_complete=True)[index]
        for ref in references:
            if ref.get("kind") in FILE_SET_KINDS:
                _prepared_oracle(ref, spec, index)
            elif file_hash(Path(ref["reference_path"])) != ref["reference_sha256"]:
                raise ValueError("original factorized reference SHA changed")
        _assert_no_prepared_writer(root)
        if root.exists() and saved is None:
            raise ValueError("do not adopt or overwrite an unowned prepared directory")
        check_locks()
        built = not (saved and saved["state"] == "complete" and root.exists())
        log_path = None
        restoring = references[0].get("kind") == PRESERVED_FILE_SET_KIND
        recovery = {"method": "preserved_original_file_set" if restoring else "original_builder",
                    "original_files_restored": 0}
        process, phase = None, "fresh_original_readback"
        try:
            if built:
                if root.exists():
                    os.rename(root, root.with_name(root.name + ".rescue-incomplete-" + uuid.uuid4().hex))
                log_path = controls / (key + ".build-" + uuid.uuid4().hex + ".private.log")
                begun = {**shared_owned, "state": "building", "owner_spec_sha256": owned["spec_sha256"],
                         "run_id": spec["run_id"], "started_at": time.time(), "log_path": str(log_path)}
                atomic_json(claim, begun)
                atomic_json(marker, {**owned, "state": "building", "artifact_sha256": artifact_sha})
                source = identity["source_roots"][-1]
                # Both code and interpreter remain the enrolled frozen release.
                command = ["bash", "-c", 'cd "$1" && source scripts/runtime_env.sh && cd "$2" && shift 2 && exec_fintech_python "$@"',
                           "stockagent-rescue-prepare", source, str(working), str(Path(source) / step[0]), *step[1:]]
                env = dict(os.environ, **spec.get("environment", {}))
                env.update(PYTHON_BIN=spec["runtime_prefix"] + "/bin/python", FINTECH_ENV_PATH=spec["runtime_prefix"])
                phase = "restore_original_files" if restoring else "original_builder"
                with log_path.open("xb") as log:
                    os.chmod(log_path, 0o600)
                    try:
                        if restoring:
                            recovery = _restore_preserved_clock_files(references[0], spec, index)
                            log.write(json.dumps(recovery, sort_keys=True).encode() + b"\n")
                        else:
                            process = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT,
                                                       start_new_session=True, pass_fds=tuple(fd for fd, _ in locks))
                            if process.wait(timeout=build_timeout):
                                raise ValueError("original prepared builder failed; preserve owned output and log")
                    finally:
                        log.flush(); os.fsync(log.fileno())
                if process is not None and _group_members(process.pid):
                    raise ValueError("original prepared builder left live owned children")
            phase = "fresh_original_readback"
            _assert_no_prepared_writer(root)
            check_locks()
            proofs = _reconcile_prepared_step(spec, index)
            check_locks()
            if signature(spec_path) != spec_signature or digest(_prepared_control(spec_path)) != owned["spec_sha256"]:
                raise ValueError("prepared spec changed before complete publication")
        except BaseException as error:
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=20)
            _record_prepared_failure(claim, marker, shared_owned=shared_owned, owned=owned,
                                     run_id=spec["run_id"], phase=phase, error=error, check_locks=check_locks)
            raise
        if log_path is not None:
            shared_complete = {**shared_owned, "state": "complete", "verified_at": time.time(), "proofs": proofs,
                               "log_path": str(log_path), "log_sha256": file_hash(log_path), "recovery": recovery}
            atomic_json(claim, shared_complete)
        atomic_json(marker, {**owned, "state": "complete", "artifact_sha256": artifact_sha,
                             "verified_at": time.time(), "proofs": proofs})
        result = {"state": "shared_prepared_original_members_verified", "spec_sha256": owned["spec_sha256"],
                  "step_index": index, "artifact_sha256": artifact_sha, "skip_builder": True,
                  "builder_executed": built and not restoring, "fresh_full_original_readback": True,
                  "prepared_recovery_method": recovery["method"],
                  "original_files_restored": recovery["original_files_restored"]}
        for row in links:
            module._atomic_symlink(Path(row["path"]), Path(row["target"]), replace_existing=False)
        if links:
            result.update(prepared_link_targets={row["path"]: row["target"] for row in links},
                          cache_controller_sha256=controller_sha)
        return result


def target_preflight(request: dict) -> dict:
    """Read-only cheap admission before multi-GB bootstrap, never GPU proof."""
    required = request["runtime_platform"]
    if (platform.system() != required["system"] or platform.machine() != required["machine"]
            or platform.libc_ver()[0] != required["libc"][0]
            or tuple(map(int, platform.libc_ver()[1].split("."))) < tuple(map(int, required["libc"][1].split(".")))):
        raise ValueError("new image is incompatible with the captured runtime platform")
    if not shutil.which("supervisorctl") or not Path("/etc/supervisor/conf.d").is_dir():
        raise ValueError("use a standard Vast Supervisor Linux image; never detach an unowned trainer")
    prefix = absolute(request["runtime_prefix"])
    if prefix.exists():
        marker = prefix / ".stockagent-rescue-runtime.json"
        if not marker.is_file() or json.loads(marker.read_bytes()).get("archive_sha256") != request["archive_sha256"]:
            raise ValueError("new image has another environment at the required prefix; never overwrite it")
    free = shutil.disk_usage(Path("/")).free
    if free < request["minimum_free_bytes"]:
        raise ValueError("new target has insufficient disk for this run's restored inputs/views and reserve")
    return {"state": "target_platform_owner_capacity_admitted", "free_bytes": free,
            "node_identity": node_identity(), "CUDA_compute_verified": False}


def restore_runtime(archive: Path, proof_path: Path, destination: Path) -> dict:
    proof = json.loads(proof_path.read_bytes())
    if (proof.get("kind") != "fixed_prefix_runtime" or proof["prefix"] != str(destination)
            or platform.system() != proof["system"] or platform.machine() != proof["machine"]
            or platform.libc_ver()[0] != proof["libc"][0]
            or tuple(map(int, platform.libc_ver()[1].split("."))) < tuple(map(int, proof["libc"][1].split(".")))
            or file_hash(archive) != proof["archive_sha256"]):
        raise ValueError("runtime platform/prefix/archive identity is incompatible")
    destination = absolute(str(destination))
    marker_name = ".stockagent-rescue-runtime.json"
    if destination.exists():
        marker = destination / marker_name
        if not marker.is_file() or json.loads(marker.read_bytes()).get("archive_sha256") != proof["archive_sha256"]:
            raise ValueError("do not overwrite a target's existing runtime")
        stage = destination
    else:
        stage = destination.with_name(destination.name + ".rescue-stage-" + uuid.uuid4().hex)
        extract_tar(archive, stage, maximum_bytes=proof["uncompressed_bytes"], absolute_link_prefix=destination,
                    allow_internal_hardlinks=True)
    for row in proof["files"]:
        path = stage / relative(row["relative"])
        if row["kind"] == "file":
            if file_hash(path) != row["sha256"] or path.stat().st_size != row["bytes"]:
                raise ValueError("restored runtime member differs")
        elif row["kind"] == "symlink":
            if not path.is_symlink() or runtime_link_target(path, proof) != row["target"]:
                raise ValueError("restored runtime link differs")
        else:
            raise ValueError("unsupported runtime proof member")
    atomic_json(stage / marker_name, {"archive_sha256": proof["archive_sha256"], "restored_at": time.time()})
    if stage != destination:
        os.rename(stage, destination)
    return {"state": "runtime_exact_bytes_restored", "files": len(proof["files"])}


def restore_source(archive: Path, receipt_path: Path, destination: Path) -> dict:
    import zipfile
    receipt = json.loads(receipt_path.read_bytes())
    files = receipt["source_bundle"]["files"]
    if (file_hash(archive) != receipt["source_bundle"]["sha256"]
            or digest(receipt["code"]["files"]) != receipt["code"]["source_sha256"]):
        raise ValueError("frozen source receipt/archive differs")
    destination = absolute(str(destination))
    if destination.exists():
        if all(file_hash(destination / relative(n)) == sha for n, sha in files.items()):
            return {"state": "frozen_source_verified", "reused": True}
        raise ValueError("do not overwrite a different existing source tree")
    stage = destination.with_name(destination.name + ".rescue-stage-" + uuid.uuid4().hex)
    stage.mkdir(parents=True, mode=0o700)
    with zipfile.ZipFile(archive) as z:
        if len(z.namelist()) != len(files) or set(z.namelist()) != set(files):
            raise ValueError("source ZIP inventory differs from frozen receipt")
        for name, expected in files.items():
            path = stage / relative(name)
            entry = z.getinfo(name)
            if stat.S_ISLNK(entry.external_attr >> 16):
                raise ValueError("source ZIP symlinks are not admitted")
            path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            with z.open(name) as src, path.open("xb") as dst:
                shutil.copyfileobj(src, dst, 4 * 1024**2); dst.flush(); os.fsync(dst.fileno())
            path.chmod((entry.external_attr >> 16) & 0o777 or 0o600)
            if file_hash(path) != expected:
                raise ValueError("source member does not reconstruct original bytes")
    os.rename(stage, destination)
    return {"state": "frozen_source_verified", "files": len(files)}


def install_keys(request: dict) -> dict:
    """Harden ONLY the incident key and add a forced per-run control key."""
    public = request["rescue_public_key"].split()
    control_public = request["control_public_key"].split()
    if (len(public) != 3 or len(control_public) != 3 or public[0] != "ssh-ed25519"
            or control_public[0] != "ssh-ed25519" or not re.fullmatch(r"\d{14}Z", request["expiry"])):
        raise ValueError("invalid scoped SSH key enrollment")
    forced = request["forced_command"]
    if "\n" in forced or "\r" in forced or '"' in forced or "\\" in forced:
        raise ValueError("unsafe forced-command encoding")
    path = Path.home() / ".ssh/authorized_keys"
    absolute(str(path))
    original = path.read_text() if path.exists() else ""
    lines, matched = [], False
    for line in original.splitlines():
        fields = line.split()
        if public[1] in fields:
            if "ssh-ed25519" not in fields or fields[fields.index("ssh-ed25519") + 1] != public[1]:
                raise ValueError("ambiguous rescue authorization")
            lines.append(f'restrict,expiry-time="{request["expiry"]}" ' + " ".join(public)); matched = True
        elif control_public[1] not in fields:
            lines.append(line)
    if not matched:
        raise ValueError("Vast has not authorized this incident public key")
    lines.append(f'restrict,command="{forced}" ' + " ".join(control_public))
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    temporary = path.with_name(".authorized-rescue-" + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        os.fchmod(stream.fileno(), 0o600); stream.write("\n".join(lines) + "\n")
        stream.flush(); os.fsync(stream.fileno())
    if (path.read_text() if path.exists() else "") != original:
        raise ValueError("authorized_keys changed concurrently; retain prior authorization")
    os.replace(temporary, path)
    return {"state": "incident_key_expiry_enforced", "control_key_forced_command": True,
            "other_authorizations_preserved": True}


def managed_launch(spec_path: Path, state_root: Path, generation: int, adapter: Path) -> dict:
    """Use the host Supervisor owner, not a detached unowned Python trainer."""
    spec = validate_spec(json.loads(spec_path.read_bytes()))
    state_root = absolute(str(state_root)); state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    name = "stockagent-rescue-" + spec["run_id"] + "-g" + str(generation)
    identifier(name)
    supervisor = shutil.which("supervisorctl")
    if not supervisor:
        raise ValueError("target needs Supervisor; choose the Vast base image or install its userspace owner")
    status = state_root / "supervisor.json"
    if status.exists():
        saved = json.loads(status.read_bytes())
        identity = saved.get("supervisor_identity")
        if (identity and saved.get("generation") == generation
                and _process_identity(identity["pid"]) == identity):
            return {"state": "existing_owned_guardian", "training_progress_verified": False}
        raise ValueError("previous target guardian exited; preserve receipt and explicitly plan continuation")
    command = [spec["runtime_prefix"] + "/bin/python", str(adapter), "supervise", "--spec", str(spec_path),
               "--lease", str(spec_path.parent / "lease.json"), "--output", str(status), "--generation", str(generation)]
    # Supervisor command strings use shlex, with no shell expansion. Percent is
    # config interpolation, so reject it in all generated command paths.
    import shlex
    rendered = shlex.join(command)
    if "%" in rendered or "\n" in rendered:
        raise ValueError("unsafe Supervisor interpolation")
    config = Path("/etc/supervisor/conf.d") / (name + ".conf")
    text = (f"[program:{name}]\ncommand={rendered}\nautostart=false\nautorestart=false\n"
            "startsecs=0\nstartretries=0\nstopasgroup=true\nkillasgroup=true\nstopwaitsecs=35\n"
            f"stdout_logfile={state_root}/training.log\nstdout_logfile_maxbytes=50MB\nstdout_logfile_backups=2\n"
            "redirect_stderr=true\n")
    if config.exists() and config.read_text() != text:
        raise ValueError("preserve a different existing Supervisor registration")
    if not config.exists():
        with config.open("x") as stream:
            stream.write(text); stream.flush(); os.fsync(stream.fileno())
    subprocess.run([supervisor, "reread"], check=True, capture_output=True, timeout=30)
    subprocess.run([supervisor, "update", name], check=True, capture_output=True, timeout=30)
    subprocess.run([supervisor, "start", name], check=True, capture_output=True, timeout=30)
    return {"state": "owned_guardian_started", "supervisor_program": name, "training_progress_verified": False}


def _process_identity(pid: int) -> dict | None:
    try:
        body = Path(f"/proc/{pid}/stat").read_text()
        fields = body[body.rindex(")") + 2:].split()
        return {"pid": pid, "start_ticks": int(fields[19]),
                "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}
    except (OSError, ValueError, IndexError):
        return None


def _group_members(pgid: int) -> list[int]:
    """Non-zombie descendants still own resources after their leader exits."""
    members = []
    for path in Path("/proc").glob("[0-9]*/stat"):
        try:
            body = path.read_text(); fields = body[body.rindex(")") + 2:].split()
            if int(fields[2]) == pgid and fields[0] != "Z":
                members.append(int(path.parent.name))
        except (OSError, ValueError, IndexError):
            continue
    return members


def _tagged_members(tag: str) -> list[dict]:
    """Elastic DDP/compile workers can create NEW sessions and outlive leaders.

    Match only this guardian's injected ownership tag. Never log/persist other
    environment values. PID + start ticks + boot ID are rechecked before signals.
    """
    if not HASH.fullmatch(tag):
        raise ValueError("invalid owned-process tag")
    token = ("STOCKAGENT_TRAINING_RESCUE_OWNER=" + tag).encode()
    result = []
    for path in Path("/proc").glob("[0-9]*/environ"):
        try:
            if path.parent.stat().st_uid != os.getuid():
                continue
            pid = int(path.parent.name)
            before = _process_identity(pid)
            if not before:
                continue
            with path.open("rb") as stream:
                body = stream.read(1024 * 1024)
            if token in body.split(b"\0") and _process_identity(pid) == before:
                result.append(before)
        except (OSError, ValueError):
            continue
    return result


def supervise(spec_path: Path, lease_path: Path, status_path: Path, generation: int) -> int:
    spec = validate_spec(json.loads(spec_path.read_bytes()))
    root, prefix = absolute(spec["source_root"]), absolute(spec["runtime_prefix"])
    env = os.environ.copy()
    env.update(spec.get("environment", {}))
    env.update(PYTHON_BIN=str(prefix / "bin/python"), FINTECH_ENV_PATH=str(prefix), PYTHONUNBUFFERED="1")
    env["CUDA_VISIBLE_DEVICES"] = ",".join(str(v) for v in range(spec["world_size"]))
    record = {"contract": CONTRACT, "run_id": spec["run_id"], "generation": generation,
              "supervisor_identity": _process_identity(os.getpid()), "state": "starting"}
    tag = digest([spec["run_id"], generation, record["supervisor_identity"], uuid.uuid4().hex])
    env["STOCKAGENT_TRAINING_RESCUE_OWNER"] = tag
    record["owned_process_tag"] = tag
    child = None
    last_sequence, deadline = -1, 0.0
    spec_sha = digest(spec)

    def renew():
        nonlocal last_sequence, deadline
        lease = json.loads(lease_path.read_bytes())
        if (lease.get("spec_sha256") != spec_sha or lease.get("generation") != generation
                or lease.get("run_id") != spec["run_id"] or lease.get("boot_id") != record["supervisor_identity"]["boot_id"]
                or lease.get("expires_at", 0) <= time.time() or lease.get("issued_at", 0) > time.time() + 5
                or not 10 <= lease.get("ttl_seconds", 0) <= 180):
            raise ValueError("rescue owner lease is absent, expired or fenced")
        if lease["sequence"] > last_sequence:
            last_sequence = lease["sequence"]
            deadline = time.monotonic() + min(lease["ttl_seconds"], lease["expires_at"] - time.time())
        if time.monotonic() >= deadline:
            raise ValueError("rescue owner lease expired on monotonic clock")

    def terminate(_signum=None, _frame=None):
        nonlocal child
        if child is None:
            return
        for sig, seconds in ((signal.SIGTERM, 20), (signal.SIGKILL, 10)):
            if _group_members(child.pid):
                try:
                    os.killpg(child.pid, sig)
                except ProcessLookupError:
                    pass
            for identity in _tagged_members(tag):
                if _process_identity(identity["pid"]) == identity:
                    try:
                        os.kill(identity["pid"], sig)
                    except ProcessLookupError:
                        pass
            until = time.monotonic() + seconds
            while (_group_members(child.pid) or _tagged_members(tag)) and time.monotonic() < until:
                child.poll(); time.sleep(0.1)
        child.poll()
        if _group_members(child.pid) or _tagged_members(tag):
            raise RuntimeError("owned training descendants remain; fencing is NOT verified")

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda s, f: (_ for _ in ()).throw(InterruptedError("supervisor interrupted")))
    try:
        renew()
        lock_root = (absolute(env["STOCKAGENT_GPU_LEASE_ROOT"]) if env.get("STOCKAGENT_GPU_LEASE_ROOT")
                     else Path.home() / ".local/state/stockagent/gpu-leases")
        lock_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        # Same node-level admission as manage_gpu_jobs, not an alternate GPU owner.
        with contextmanager(_gpu_locks)(lock_root, spec["world_size"], cleanup=terminate):
            check = [str(prefix / "bin/python"), "scripts/check_environment.py", "--require-cuda",
                     "--minimum-cuda-devices", str(spec["world_size"]), "--strict"]
            child = subprocess.Popen(check, cwd=root, env=env, stdin=subprocess.DEVNULL, start_new_session=True)
            preflight_deadline = time.monotonic() + 180
            while child.poll() is None:
                renew()
                if time.monotonic() >= preflight_deadline:
                    raise TimeoutError("strict CUDA preflight timed out")
                time.sleep(1)
            if child.returncode:
                raise ValueError("strict CUDA allocation/kernel/synchronize preflight failed")
            renew()
            command = ["bash", "-c", 'source scripts/runtime_env.sh; exec_fintech_python "$@"',
                       "stockagent-rescue-train", *spec["train_argv"]]
            child = subprocess.Popen(command, cwd=root, env=env, stdin=subprocess.DEVNULL, start_new_session=True)
            record.update(state="training_launched_not_resume_acceptance", child_identity=_process_identity(child.pid))
            atomic_json(status_path, record)
            while child.poll() is None:
                renew()
                time.sleep(2)
            record.update(state="child_exited", exit_code=child.returncode)
            return child.returncode
    except BaseException as exc:
        terminate()
        record.update(state="fenced_or_failed", failure_type=type(exc).__name__)
        return 75
    finally:
        record["observed_at"] = time.time()
        record["node_identity"] = node_identity()
        record["owned_child_gone"] = (child is None or not _group_members(child.pid)) and not _tagged_members(tag)
        record["owned_processes_gone_verified"] = record["owned_child_gone"]
        atomic_json(status_path, record)


def _gpu_locks(root: Path, count: int, *, cleanup=None):
    files = []
    try:
        for gpu in range(count):
            f = (root / f"gpu-{gpu}.lock").open("a")
            files.append(f)
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        try:
            if cleanup is not None:
                cleanup()  # Terminate DDP children BEFORE releasing node admission.
        finally:
            for f in reversed(files):
                f.close()


def control(spec_path: Path, status_path: Path, generation: int) -> int:
    """Forced-command SSH RPC: one run, no shell/forwarding/arbitrary paths.

    The bootstrap key expires. This separate run-scoped key can only renew its
    lease, read recovery context/status/health or stop its own guardian.
    """
    spec = validate_spec(json.loads(spec_path.read_bytes()))
    body = sys.stdin.buffer.readline(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise ValueError("control RPC exceeds bound")
    request = json.loads(body)
    action = request.get("action")
    if action == "lease":
        lease = request["lease"]
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        if (lease.get("contract") != CONTRACT or lease.get("run_id") != spec["run_id"]
                or lease.get("spec_sha256") != digest(spec) or lease.get("generation") != generation
                or lease.get("boot_id") != boot or type(lease.get("sequence")) is not int
                or not 10 <= lease.get("ttl_seconds", 0) <= 180
                or not 0 < lease.get("expires_at", 0) - time.time() <= 185
                or abs(lease.get("issued_at", 0) - time.time()) > 5):
            raise ValueError("invalid/fenced owner lease")
        path = spec_path.parent / "lease.json"
        if path.exists():
            old = json.loads(path.read_bytes())
            if old.get("generation", 0) > generation or (old.get("generation") == generation
                    and old.get("sequence", -1) >= lease["sequence"]):
                raise ValueError("lease replay or previous generation refused")
        atomic_json(path, lease)
        result = {"state": "lease_renewed", "generation": generation}
    elif action == "inspect":
        result = inspect_run(spec)
    elif action == "seal":
        result = seal_run(spec, spec_path.parent)
    elif action == "ack-seal":
        result = ack_seal(spec, spec_path.parent, request["context_sha256"], request["commit_id"])
    elif action == "health":
        result = health()
    elif action == "read":
        row = request["row"]
        member = Path(relative(row["relative"]))
        if member.suffix not in CONTEXT_SUFFIXES:
            raise ValueError("only enrolled public training recovery context may be read")
        if request.get("seal"):
            if not HASH.fullmatch(request["seal"]):
                raise ValueError("invalid private seal identifier")
            root = spec_path.parent / "sealed" / request["seal"]
        else:
            root = Path(spec["output_root"])
        read_member(absolute(str(root)), row)
        return 0
    elif action == "status":
        result = json.loads(status_path.read_bytes()) if status_path.exists() else {"state": "not_started"}
        identity = result.get("supervisor_identity")
        result["owned_guardian_gone_verified"] = bool(identity and _process_identity(identity["pid"]) != identity)
        if result.get("owned_process_tag"):
            result["owned_processes_gone_verified"] = bool(result.get("owned_processes_gone_verified")
                                                           and not _tagged_members(result["owned_process_tag"]))
    elif action == "stop":
        current = json.loads(status_path.read_bytes())
        identity = current.get("supervisor_identity")
        if current.get("generation") != generation or not identity:
            raise ValueError("stop does not own this guardian generation")
        if _process_identity(identity["pid"]) == identity:
            os.kill(identity["pid"], signal.SIGTERM)
        result = {"state": "owned_stop_requested", "owned_processes_gone_verified": False}
    elif action == "revoke-bootstrap":
        enrollment = json.loads((status_path.parent / "key-enrollment.json").read_bytes())
        if enrollment.get("generation") != generation:
            raise ValueError("key revocation does not own this generation")
        public = enrollment["rescue_public_key"].split()[1]
        path = absolute(str(Path.home() / ".ssh/authorized_keys"))
        original = path.read_text()
        lines = [line for line in original.splitlines() if public not in line.split()]
        temporary = path.with_name(".rescue-key-revoked-" + uuid.uuid4().hex)
        with temporary.open("x") as stream:
            os.fchmod(stream.fileno(), 0o600); stream.write("\n".join(lines) + "\n")
            stream.flush(); os.fsync(stream.fileno())
        if path.read_text() != original:
            raise ValueError("authorized_keys changed concurrently")
        os.replace(temporary, path)
        result = {"state": "incident_public_key_revoked", "ongoing_control_key_unchanged": True}
    else:
        raise ValueError("forced control key cannot execute arbitrary commands")
    print(json.dumps(result))
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=("inspect", "health", "read", "receive", "freeze-runtime", "supervise", "metadata", "control",
                                      "restore-runtime", "restore-source", "install-keys", "launch", "reconcile", "seal", "ack-seal",
                                      "prepare-begin", "prepare-finish", "verify-runtime", "target-preflight",
                                      "prepared-reference", "input-preflight", "materialize-input",
                                      "capsule-preflight", "materialize-capsule", "prepared-preflight", "prepare-run", "adopt-prepared"))
    p.add_argument("--spec", type=Path)
    p.add_argument("--path", type=Path)
    p.add_argument("--output", type=Path)
    p.add_argument("--row")
    p.add_argument("--sha256")
    p.add_argument("--bytes", type=int)
    p.add_argument("--lease", type=Path)
    p.add_argument("--generation", type=int)
    p.add_argument("--proof", type=Path)
    p.add_argument("--archive", type=Path)
    p.add_argument("--source-root", type=Path)
    p.add_argument("--commit-id")
    p.add_argument("--mode", type=int)
    p.add_argument("--index", type=int)
    p.add_argument("--artifact-kind", choices=tuple(PREPARED_BUILDERS))
    p.add_argument("--representation-bundle", type=Path)
    p.add_argument("--prior-spec", type=Path)
    p.add_argument("--prior-spec-sha256")
    p.add_argument("--prior-marker-sha256")
    args = p.parse_args()
    os.umask(0o077)
    if args.action == "inspect":
        result = inspect_run(json.loads(args.spec.read_bytes()))
    elif args.action == "health":
        result = health()
    elif args.action == "read":
        read_member(absolute(str(args.path)), json.loads(args.row))
        return 0
    elif args.action == "receive":
        result = receive_file(absolute(str(args.path)), args.sha256, args.bytes, args.mode)
    elif args.action == "freeze-runtime":
        result = freeze_runtime(absolute(str(args.path)), absolute(str(args.output)))
    elif args.action == "verify-runtime":
        result = verify_runtime_capsule(absolute(str(args.path)), absolute(str(args.proof)))
    elif args.action == "metadata":
        path = absolute(str(args.path))
        result = {"source_path": str(path), "sha256": file_hash(path), "bytes": path.stat().st_size}
    elif args.action == "control":
        return control(args.spec, args.output, args.generation)
    elif args.action == "restore-runtime":
        result = restore_runtime(args.path, args.proof, args.output)
    elif args.action == "restore-source":
        result = restore_source(args.path, args.proof, args.output)
    elif args.action == "install-keys":
        result = install_keys(json.loads(sys.stdin.buffer.read(1024 * 1024)))
    elif args.action == "target-preflight":
        result = target_preflight(json.loads(sys.stdin.buffer.read(1024 * 1024)))
    elif args.action == "launch":
        result = managed_launch(args.spec, args.output, args.generation, Path(__file__).resolve())
    elif args.action == "reconcile":
        result = reconcile_prepared(args.proof, args.path, source_root=args.source_root)
    elif args.action == "prepared-reference":
        result = prepared_file_set(args.path, artifact_kind=args.artifact_kind, source_root=args.source_root,
                                   representation_bundle=args.representation_bundle)
    elif args.action in {"input-preflight", "materialize-input"}:
        result = materialize_input(args.spec, args.index, args.path, args.sha256,
                                   preflight=args.action == "input-preflight")
    elif args.action in {"capsule-preflight", "materialize-capsule"}:
        result = materialize_capsule(args.spec, args.index, args.path, args.sha256, args.proof, args.archive,
                                    preflight=args.action == "capsule-preflight")
    elif args.action == "prepared-preflight":
        result = prepared_link_preflight(args.spec, args.path, args.sha256)
    elif args.action in {"prepare-begin", "prepare-finish"}:
        result = prepare_step(args.spec, args.index, finish=args.action == "prepare-finish",
                              controller=args.path, controller_sha=args.sha256)
    elif args.action == "prepare-run":
        result = prepare_run(args.spec, args.index, controller=args.path, controller_sha=args.sha256)
    elif args.action == "adopt-prepared":
        result = adopt_prepared_run(args.spec, args.index, prior_spec=args.prior_spec,
                                    prior_spec_sha256=args.prior_spec_sha256,
                                    prior_marker_sha256=args.prior_marker_sha256)
    elif args.action == "seal":
        result = seal_run(json.loads(args.spec.read_bytes()), args.spec.parent)
    elif args.action == "ack-seal":
        result = ack_seal(json.loads(args.spec.read_bytes()), args.spec.parent, args.sha256, args.commit_id)
    else:
        return supervise(args.spec, args.lease, args.output, args.generation)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, EOFError, tarfile.TarError) as error:
        print(json.dumps({"state": "rescue_rejected", "error_type": type(error).__name__,
                          "reason": str(error)[:400]}), file=sys.stderr)
        raise SystemExit(65)
