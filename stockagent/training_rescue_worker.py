"""Small Linux rescue adapter. No CUDA import in discovery/lease supervision.

This file is also deployed as a fixed, standalone adapter outside frozen source
releases. Training itself always executes the recorded canonical entrypoint.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
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
    for step in spec.get("rebuild_steps", []):
        if not step or not all(isinstance(s, str) and "\x00" not in s for s in step):
            raise ValueError("invalid canonical preparation argv")
        path = PurePosixPath(relative(step[0]))
        if path.parts[0] != "scripts" or path.suffix != ".py":
            raise ValueError("rebuild steps must be repository-owned Python builders")
        if step.count("--output-root") != 1:
            raise ValueError("each rebuild needs one explicit new prepared output root")
        out = absolute(step[step.index("--output-root") + 1])
        if "data_preparation" not in out.parts or "artifacts" not in out.parts:
            raise ValueError("rebuild may only write derived artifacts/data_preparation roots")
    return spec


def checkpoint_summary(path: Path, spec: dict) -> dict:
    # This executes in the SELECTED training runtime, always on CPU. A newer
    # control checkout never rewrites a frozen trainer's compatibility schema.
    from stockagent.training.runtime import load_checkpoint
    before = signature(path)
    saved = load_checkpoint(path)
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
    p = absolute(str(root / relative(row["relative"])))
    before = signature(p)
    if before[2] != row["bytes"] or file_hash(p) != row["sha256"]:
        raise ValueError("source member no longer matches the sealed inventory")
    h = hashlib.sha256()
    with p.open("rb") as stream:
        for block in iter(lambda: stream.read(4 * 1024**2), b""):
            h.update(block)
            sys.stdout.buffer.write(block)
    sys.stdout.buffer.flush()
    if signature(p) != before or h.hexdigest() != row["sha256"]:
        raise ValueError("source changed during transfer; receiver must reject")


def receive_file(path: Path, sha: str, count: int, mode: int | None = None) -> dict:
    absolute(str(path))
    if not HASH.fullmatch(sha) or type(count) is not int or count < 0:
        raise ValueError("invalid transfer identity")
    if mode is not None and (type(mode) is not int or not 0 <= mode <= 0o777):
        raise ValueError("invalid target member permissions")
    if path.exists():
        if path.stat().st_size == count and file_hash(path) == sha:
            if mode is not None:
                path.chmod(mode)
            return {"sha256": sha, "bytes": count, "reused": True}
        raise ValueError("never overwrite an existing different target file")
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    partial = path.with_name(path.name + ".partial." + uuid.uuid4().hex)
    h, received = hashlib.sha256(), 0
    with partial.open("xb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        for block in iter(lambda: sys.stdin.buffer.read(4 * 1024**2), b""):
            received += len(block)
            if received > count:
                raise ValueError("transfer exceeded exact byte count")
            stream.write(block)
            h.update(block)
        stream.flush()
        os.fsync(stream.fileno())
    if received != count or h.hexdigest() != sha:
        raise ValueError("partial/corrupt transfer; preserve partial and do not activate")
    if mode is not None:
        partial.chmod(mode)
    os.link(partial, path)  # Atomic no-overwrite, unlike replace.
    partial.unlink()  # Exactly this successfully verified incoming member.
    return {"sha256": sha, "bytes": received, "durable": True}


def extract_tar(archive: Path, destination: Path, *, maximum_bytes: int,
                absolute_link_prefix: Path | None = None,
                allow_internal_hardlinks: bool = False) -> dict:
    absolute(str(destination))
    if destination.exists():
        raise ValueError("archive extraction requires a NEW target directory")
    destination.mkdir(parents=True, mode=0o700)
    total, count, seen = 0, 0, set()
    with tarfile.open(archive, "r:*") as source:
        for m in source:
            name = relative(m.name.rstrip("/"))
            if name in seen or m.isdev() or m.isfifo() or (m.islnk() and not allow_internal_hardlinks):
                raise ValueError("unsafe or repeated archive member")
            seen.add(name)
            path = destination / name
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
                if total > maximum_bytes or count > 250000:
                    raise ValueError("archive exceeds enrolled resource bounds")
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() or path.is_symlink():
                    raise ValueError("archive member would overwrite existing data")
                with source.extractfile(m) as src, path.open("xb") as dst:
                    shutil.copyfileobj(src, dst, 4 * 1024**2)
                    dst.flush()
                    os.fsync(dst.fileno())
                path.chmod(m.mode & 0o777)
                count += 1
            else:
                raise ValueError("unsupported archive entry")
    return {"bytes": total, "files": count}


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
                target = p.readlink()
                if not p.resolve().is_relative_to(prefix):
                    raise ValueError("runtime has an external symlink; enroll its dependency explicitly")
                row = {"relative": rel, "kind": "symlink", "target": str(target)}
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
        if row["kind"] == "symlink" and str(p.readlink()) != row["target"]:
            raise ValueError("runtime symlink changed during publication")
    with output.open("rb") as stream:
        os.fsync(stream.fileno())
    proof = {"contract": CONTRACT, "kind": "fixed_prefix_runtime", "prefix": str(prefix),
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
        if row["kind"] == "symlink" and str(path.readlink()) != row["target"]:
            raise ValueError("runtime link changed after packaging")
    return {"state": "source_runtime_still_matches_closed_capsule", "files": len(expected)}


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


def reconcile_prepared(original: Path, rebuilt: Path, *, source_root: Path) -> dict:
    """Restore ORIGINAL manifest bytes only after exact artifact equivalence.

    Elapsed build time is not an observation. Keeping original receipt bytes
    preserves the canonical manifest-based feature fingerprint. The separate
    rebuild receipt records when reconstruction actually happened; this never
    backdates source availability, changes builder identity, or promotes CUDA.
    """
    left, right = json.loads(original.read_bytes()), json.loads(rebuilt.read_bytes())
    volatile = {"build_wall_seconds", "projection_wall_seconds"}
    if ({k: v for k, v in left.items() if k not in volatile}
            != {k: v for k, v in right.items() if k not in volatile}):
        raise ValueError("rebuilt prepared ABI/member inventory differs from original; do not resume")
    sys.path.insert(0, str(source_root))
    from stockagent.data.factorized_panel import verify_factorized_members
    verify_factorized_members(rebuilt.parent, left)
    rebuilt_sha = file_hash(rebuilt)
    atomic_json(rebuilt.with_name("training-rescue-rebuild-receipt.json"), {
        "contract": CONTRACT, "rebuilt_at": time.time(), "original_manifest_sha256": file_hash(original),
        "rebuilt_manifest_sha256": rebuilt_sha, "exact_members_verified": True,
        "changed_semantics": False, "fresh_gpu_acceptance_claim": False})
    # Atomic publication of the already-preserved original reference receipt.
    temporary = rebuilt.with_name(rebuilt.name + ".rescue-" + uuid.uuid4().hex)
    with temporary.open("xb") as stream:
        stream.write(original.read_bytes()); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, rebuilt)
    return {"state": "prepared_original_bytes_reconstructed", "sha256": file_hash(rebuilt)}


def prepare_step(spec_path: Path, index: int, *, finish: bool = False) -> dict:
    """Recover interrupted CPU builds without overwriting an unowned view."""
    spec = validate_spec(json.loads(spec_path.read_bytes()))
    steps = spec.get("rebuild_steps", [])
    if type(index) is not int or not 0 <= index < len(steps):
        raise ValueError("unknown preparation step")
    step = steps[index]
    root = absolute(step[step.index("--output-root") + 1])
    marker = spec_path.parent / "rebuild" / (str(index) + ".json")
    owned = {"spec_sha256": digest(spec), "output_root": str(root), "step_sha256": digest(step)}
    saved = json.loads(marker.read_bytes()) if marker.exists() else None
    if saved is not None and any(saved.get(k) != v for k, v in owned.items()):
        raise ValueError("another preparation owns this output")
    references = [r for r in spec.get("prepared_reconciliation", []) if r["step_index"] == index]
    if finish or (saved and saved.get("state") == "complete"):
        if not saved or not references:
            raise ValueError("prepared completion needs owned original-byte reconciliation")
        for ref in references:
            reconcile_prepared(Path(ref["reference_path"]), Path(ref["rebuilt_manifest"]),
                               source_root=Path(spec["source_root"]))
        atomic_json(marker, {**owned, "state": "complete", "verified_at": time.time()})
        return {"state": "prepared_exact_members_already_verified", "skip_builder": True}
    if root.exists():
        if saved is None:
            raise ValueError("do not overwrite or quarantine an unowned prepared directory")
        # A previous SSH timeout is not proof its CPU writer exited. Read-only
        # process admission before moving this controller-owned incomplete view.
        for cmdline in Path("/proc").glob("[0-9]*/cmdline"):
            try:
                args = cmdline.read_bytes().decode(errors="replace").split("\0")
                if "--output-root" in args and args[args.index("--output-root") + 1] == str(root):
                    raise ValueError("previous prepared builder still running; do not race or restart it")
            except (OSError, IndexError):
                continue
        quarantine = root.with_name(root.name + ".rescue-incomplete-" + uuid.uuid4().hex)
        os.rename(root, quarantine)  # Keep this exact owned partial, never delete inputs.
    atomic_json(marker, {**owned, "state": "building", "started_at": time.time()})
    return {"state": "owned_fresh_preparation_admitted", "skip_builder": False}


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
            if not path.is_symlink() or str(path.readlink()) != row["target"]:
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
                                      "prepare-begin", "prepare-finish", "verify-runtime", "target-preflight"))
    p.add_argument("--spec", type=Path)
    p.add_argument("--path", type=Path)
    p.add_argument("--output", type=Path)
    p.add_argument("--row")
    p.add_argument("--sha256")
    p.add_argument("--bytes", type=int)
    p.add_argument("--lease", type=Path)
    p.add_argument("--generation", type=int)
    p.add_argument("--proof", type=Path)
    p.add_argument("--source-root", type=Path)
    p.add_argument("--commit-id")
    p.add_argument("--mode", type=int)
    p.add_argument("--index", type=int)
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
    elif args.action in {"prepare-begin", "prepare-finish"}:
        result = prepare_step(args.spec, args.index, finish=args.action == "prepare-finish")
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
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(json.dumps({"state": "rescue_rejected", "error_type": type(error).__name__,
                          "reason": str(error)[:400]}), file=sys.stderr)
        raise SystemExit(65)
