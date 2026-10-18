"""Lossless CPU forward results for completed, receipt-accepted TW sessions.

This cache owns neither session acceptance nor orders. Its caller must exclude
live quotes. A complete input identity, not a date or display TTL, owns reuse.
Only tensors and safe primitive containers are serialized; a cache hit never
constructs a model, transfers to CUDA, or requires a working GPU.
"""

from __future__ import annotations

import ast
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from functools import lru_cache
import hashlib
import io
import json
import mmap
import os
from pathlib import Path
import pickle
import stat
import threading
import time
from typing import Any, Callable, Iterator

import numpy as np
import torch

from downloader.artifact_io import atomic_write_bytes
from stockagent.data_sync.completed_forward_usage import note_successful_use
from stockagent.data_sync.immutable_cache_files import link_immutable_payload, payload_info

CONTRACT = "completed_tw_panel_forward_cpu_v3"
# The five deployed models have 13--842 MiB of complete outputs. A 64 MiB
# guard rejected three valid models and made them recompute on every attempt.
# Retain a finite 1 GiB bound, never discard fields or quantize to fit it.
MAX_PAYLOAD_BYTES = 1024**3
_CACHE_MISS = object()


def identity_digest(identity: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()


def configuration_identity(config: Any) -> str:
    """Include resolved inherited settings, not only the deployment YAML."""
    value = json.dumps(asdict(config), sort_keys=True, separators=(",", ":"),
                       default=str, allow_nan=False)
    return hashlib.sha256(value.encode()).hexdigest()


@lru_cache(maxsize=1024)
def _repository_imports(path: Path, sha256: str) -> tuple[tuple[int, str], ...]:
    """Parse lazy imports too, without importing code or initializing CUDA.

    Cache syntax, not dependency membership: adding/deleting a module named by
    an existing import must still change the next identity. Never infer this
    closure from sys.modules, whose contents depend on previous requests.
    """
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise LoadedImplementationChanged()
    tree = ast.parse(raw, filename=str(path))
    requests: set[tuple[int, str]] = set()
    dynamic_loaders = {"__import__"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in {"importlib", "importlib.util"}:
            dynamic_loaders.update(name.asname or name.name for name in node.names
                                   if name.name in {"import_module", "spec_from_file_location"})
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            requests.update((0, name.name) for name in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            requests.add((node.level, module))
            requests.update((node.level, f"{module}.{name.name}".lstrip("."))
                            for name in node.names if name.name != "*")
        elif isinstance(node, ast.Call) and (
            isinstance(node.func, ast.Name) and node.func.id in dynamic_loaders
            or isinstance(node.func, ast.Attribute)
            and node.func.attr == "import_module"
        ):
            if (node.args and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)
                    and not node.args[0].value.startswith(".")):
                requests.add((0, node.args[0].value))
            # The inference factory explicitly imports its registered models.
            # Do not execute variable training-only dispatchers just because
            # inference imports their shared contract/registry module.
    return tuple(sorted(requests))


def _collect_implementation_identity(
    repo: Path, roots: tuple[Path, ...],
) -> tuple[list[dict[str, Any]], dict[Path, tuple[int, ...] | None]]:
    """Own only the transitive local Python imports of the inference role."""
    pending = list(roots)
    identities: dict[Path, dict[str, Any]] = {}
    top_level = {entry.name for entry in repo.iterdir()}
    import_requests: set[tuple[str, ...]] = set()
    candidates: dict[Path, bool] = {}
    import_roots: set[str] = set()

    def enqueue(source: Path) -> None:
        if source not in candidates:
            candidates[source] = source.is_file()
        if candidates[source]:
            pending.append(source)

    while pending:
        path = pending.pop()
        if path in identities:
            continue
        identity = file_identity(path)
        identities[path] = identity
        package = path.relative_to(repo).parts[:-1]
        for level, module in _repository_imports(path, identity["sha256"]):
            if level > len(package):
                raise ValueError(f"relative inference import escapes repository: {path}")
            prefix = package[:len(package) - level + 1] if level else ()
            parts = (*prefix, *module.split(".")) if module else prefix
            if parts:
                import_roots.add(parts[0])
            if (not parts or parts in import_requests
                    or parts[0] not in top_level and f"{parts[0]}.py" not in top_level):
                continue
            import_requests.add(parts)
            # Python executes package initializers even when importing only a
            # submodule. Include their dependencies and preserve import cycles.
            for end in range(1, len(parts) + 1):
                enqueue(repo.joinpath(*parts[:end], "__init__.py"))
            enqueue(repo.joinpath(*parts).with_suffix(".py"))
    # Watch directory membership as well as consumed file generations. Missing
    # parent directories are included: creating a namespace/package later must
    # not evade the closure simply because it was absent at first admission.
    watch_paths = set(identities)
    for candidate in candidates:
        watch_paths.update(parent for parent in candidate.parents
                           if parent == repo or repo in parent.parents)
    for name in import_roots:
        watch_paths.update((repo / name, repo / f"{name}.py"))
    watch = {path: _dependency_generation(path) for path in watch_paths}
    if (any(list(file_generation(Path(identity["path"]))) != identity["generation"]
           or (str(path) != identity["path"] and file_identity(path) != identity)
            for path, identity in identities.items())
            or any(path.is_file() != exists for path, exists in candidates.items())
            or {name for name in import_roots if name in top_level or f"{name}.py" in top_level}
            != {name for name in import_roots
                if (repo / name).exists() or (repo / f"{name}.py").exists()}):
        raise LoadedImplementationChanged()
    return [identities[path] for path in sorted(identities)], watch


def _dependency_generation(path: Path) -> tuple[int, ...] | None:
    try:
        return file_generation(path)
    except (FileNotFoundError, NotADirectoryError):
        return None


_DEPENDENCY_GRAPH_LOCK = threading.RLock()
_DEPENDENCY_GRAPHS: dict[tuple[Path, tuple[Path, ...]], tuple[list, dict]] = {}


def _implementation_identity_from_roots(
    repo: Path, roots: tuple[Path, ...],
) -> list[dict[str, Any]]:
    with _DEPENDENCY_GRAPH_LOCK:
        key = (repo, roots)
        cached = _DEPENDENCY_GRAPHS.get(key)
        if cached is None or any(_dependency_generation(path) != generation
                                 for path, generation in cached[1].items()):
            cached = _collect_implementation_identity(repo, roots)
            _DEPENDENCY_GRAPHS[key] = cached
            if len(_DEPENDENCY_GRAPHS) > 4:
                _DEPENDENCY_GRAPHS.pop(next(iter(_DEPENDENCY_GRAPHS)))
        # Callers cannot mutate the admitted graph through returned rows.
        return [{**row, "generation": list(row["generation"])} for row in cached[0]]


def implementation_identity() -> list[dict[str, Any]]:
    repo = Path(__file__).resolve().parents[2]
    return _implementation_identity_from_roots(
        repo, (Path(__file__), repo / "stockagent/live/signal_engine.py"),
    )


class LoadedImplementationChanged(RuntimeError):
    """Disk code no longer describes this process's admitted live implementation."""

    def __init__(self) -> None:
        self.details = {"reason": "live_implementation_changed", "retryable": False,
                        "requires_fresh_process": True, "model_not_admitted": True}
        super().__init__("Live inference code changed after process admission; "
                         "use the canonical owner reload, not a relabelled cache.")


@lru_cache(maxsize=1)
def _admitted_implementation_json() -> str:
    # A string cannot be mutated by an identity consumer. The role entrypoint
    # binds this BEFORE importing its model/config/data dependencies.
    return json.dumps(implementation_identity(), sort_keys=True, allow_nan=False)


def loaded_implementation_identity() -> list[dict[str, Any]]:
    """Require a stable admitted code generation, including added/deleted files.

    Reading disk on each request is not proof of a resident Python module's
    version. Never bless a changed tree as the code behind an old model/config.
    This performs CPU file identities only; it neither reloads modules nor
    initializes CUDA. A fresh role is required after a code change.
    """
    admitted = json.loads(_admitted_implementation_json())
    if implementation_identity() != admitted:
        raise LoadedImplementationChanged()
    return admitted


def array_identity(value: np.ndarray) -> dict[str, Any]:
    array = np.ascontiguousarray(value)
    if array.dtype.hasobject:
        raise ValueError("inference inputs cannot contain Python objects")
    return {"shape": list(array.shape), "dtype": array.dtype.str,
            "sha256": hashlib.sha256(memoryview(array.reshape(-1)).cast("B")).hexdigest()}


def window_identity(window: Any) -> dict[str, Any]:
    if not getattr(window, "_stockagent_factorized_live_window", False):
        return array_identity(window)
    # These fingerprints are produced only after the canonical factorized
    # member/hash and base-array checks. Never materialize the 5 GiB dense view.
    proof = window.features.content_fingerprint
    if not isinstance(proof, dict) or not proof.get("sha256"):
        raise ValueError("factorized window has no verified content identity")
    overlay = window.overlay
    return {"features": proof, "start": window.start, "stop": window.stop,
            "shape": list(window.shape), "sentinel": "finite_fp32_nan_inf_to_positive_zero",
            "overlay": None if overlay is None else
            {"column": int(overlay[0]), "values": array_identity(overlay[1])}}


@lru_cache(maxsize=256)
def _file_digest_at_generation(path: str, generation: tuple[int, ...]) -> str:
    target = Path(path)
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        for block in iter(lambda: handle.read(1024**2), b""):
            digest.update(block)
    if file_generation(target) != generation:
        raise RuntimeError("inference identity changed during hashing")
    return digest.hexdigest()


def file_generation(path: Path) -> tuple[int, ...]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def file_identity(path: str | Path) -> dict[str, Any]:
    target = Path(path).resolve(strict=True)
    generation = file_generation(target)
    return {"path": str(target), "generation": list(generation),
            "sha256": _file_digest_at_generation(str(target), generation)}


def source_fingerprint(config: Any, *, rule_data_dir: str | Path | None = None) -> str:
    """Cheap complete file-generation proof, including same-day replacements.

    This is not source acceptance or a substitute for the panel's hash checks.
    Stat every feature member: directory mtime alone misses in-place repairs.
    """
    from stockagent.config import external_panel_data_kwargs
    from stockagent.data.factorized_panel import factorized_member_proofs

    root = Path(config.data.parquet_root).resolve(strict=True)
    # Enumerate consumed members, not directory mtimes or every raw table in
    # the provider workspace. Adding an unrelated MOPS snapshot cannot change
    # an already published feature panel or its completed-session prices.
    paths = {root / "symbols.csv"}
    paths.update(root.glob("*_features.parquet"))
    if (root / "_hot_tail").is_dir():
        paths.update((root / "_hot_tail").glob("*_features.parquet"))
    directories = set()
    external = external_panel_data_kwargs(config.data).get("external_feature_path")
    if external:
        target = Path(str(external)).resolve()
        paths.update((target, target.with_suffix(".summary.json")))
        directories.update((target.parent, target.parent.parent))
    from stockagent.live.tw_day_trade_simulation import resolve_day_trade_rule_data_dir

    rule_root = resolve_day_trade_rule_data_dir(
        rule_data_dir, parquet_root=root, repo_root=Path(__file__).resolve().parents[2],
    ).resolve()
    paths.update(rule_root / f"{venue}_day_trade_eligibility.parquet"
                 for venue in ("twse", "tpex"))
    for directory in directories:
        # Same action-reference sentinels as the canonical panel loader/cache.
        for name in ("tw_corporate_action_reference", "tw_corporate_action_entitlements"):
            paths.update((directory / f"{name}.parquet", directory / f"{name}.summary.json"))
    manifest_text = config.data.factorized_feature_manifest
    if manifest_text:
        manifest_path = Path(manifest_text).resolve(strict=True)
        manifest = json.loads(manifest_path.read_text())
        paths.add(manifest_path)
        paths.update(manifest_path.parent / proof["path"]
                     for proof in factorized_member_proofs(manifest))
    members = []
    for path in sorted(paths):
        members.append([str(path), list(file_generation(path)) if path.exists() else None])
    return identity_digest({"contract": CONTRACT, "members": members})


def _cpu_copy(value: Any, *, clone: bool = True) -> Any:
    if isinstance(value, torch.Tensor):
        if value.layout != torch.strided:
            raise ValueError("completed inference cache requires dense output tensors")
        # Clone small outputs: storing a view could otherwise serialize an
        # entire backing allocation or share mutable memory with another caller.
        value = value.detach().to(device="cpu")
        return value.clone() if clone else value
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {key: _cpu_copy(item, clone=clone) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_cpu_copy(item, clone=clone) for item in value)
    if value is None or type(value) in (str, int, float, bool):
        return value
    raise ValueError(f"unsupported completed inference output: {type(value).__name__}")


@contextmanager
def _singleflight(path: Path, *, timeout_seconds: float = 60.0) -> Iterator[int]:
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.is_symlink():
        raise ValueError("completed inference cache root must not be a symlink")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("completed inference cache owner is still computing")
                time.sleep(0.05)
        yield descriptor
    finally:
        os.close(descriptor)  # also releases the lease after crash/exception


def _read(path: Path, key: str, identity: dict[str, Any], *, commit: dict | None = None) -> Any:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return _CACHE_MISS
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_PAYLOAD_BYTES:
            raise ValueError("invalid completed inference cache file")
        receipt_path = path.with_suffix(".receipt.json")
        try:
            receipt_descriptor = os.open(receipt_path, os.O_RDONLY | os.O_NONBLOCK |
                                         getattr(os, "O_NOFOLLOW", 0))
        except FileNotFoundError as exc:
            raise ValueError("completed inference cache has no committed receipt") from exc
        with os.fdopen(receipt_descriptor, "rb") as receipt_handle:
            receipt_info = os.fstat(receipt_handle.fileno())
            if not stat.S_ISREG(receipt_info.st_mode) or receipt_info.st_size > 1024**2:
                raise ValueError("invalid completed inference cache receipt")
            receipt_data = receipt_handle.read(1024**2 + 1)
            receipt = json.loads(receipt_data)
        if (not isinstance(receipt, dict)
                or receipt.get("contract") != CONTRACT or receipt.get("key") != key
                or receipt.get("identity") != identity
                or receipt.get("payload_bytes") != info.st_size):
            raise ValueError("completed inference cache identity or size mismatch")
        digest = hashlib.sha256()
        remaining = info.st_size
        while remaining:
            block = handle.read(min(1024**2, remaining))
            if not block:
                raise ValueError("completed inference cache was truncated")
            digest.update(block)
            remaining -= len(block)
        if handle.read(1):
            raise ValueError("completed inference cache grew beyond its receipt")
        if receipt.get("payload_sha256") != digest.hexdigest():
            raise ValueError("completed inference cache checksum mismatch")
        # Stream the complete checksum, then map the SAME no-follow regular
        # inode. Avoid three full-size bytes/tensor copies for the 842 MiB aux.
        # MAP_PRIVATE means caller mutation cannot overwrite the durable file.
        if torch.serialization.get_default_mmap_options() != mmap.MAP_PRIVATE:
            raise ValueError("completed inference cache requires private memory mapping")
        result = torch.load(f"/proc/self/fd/{handle.fileno()}", map_location="cpu",
                            weights_only=True, mmap=True)
        after = os.fstat(handle.fileno())
        if (info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (
                after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise ValueError("completed inference cache changed during read")
    # Each deserialization owns fresh CPU storage. Cloning this complete tree
    # again wastes bandwidth and allocations; it cannot alias a prior caller.
    value = _cpu_copy(result, clone=False)
    if commit is not None:
        commit.update(receipt_sha256=hashlib.sha256(receipt_data).hexdigest(),
                      payload_sha256=receipt["payload_sha256"], payload_bytes=info.st_size)
    return value


def _reuse_payload(root: Path, destination: Path, *, sha256: str, size: int) -> bool:
    """Do not write another complete forward when these exact bytes exist.

    The new input identity is still computed/validated by the caller. Output
    equality only shares storage; it never blesses another key's inputs, avoids
    a required model call or migrates a legacy writable payload's permissions.
    Each key owns a link, with no permanent CAS or additional garbage collector.
    """
    for receipt_path in sorted(root.glob("*.receipt.json")):
        donor_key = receipt_path.name.removesuffix(".receipt.json")
        if (len(donor_key) != 64 or any(c not in "0123456789abcdef" for c in donor_key)
                or donor_key == destination.stem):
            continue
        donor = root / f"{donor_key}.forward"
        try:
            info = payload_info(donor)
            if info["size"] != size or info["mode"] & 0o222 or info["xattrs"]:
                continue
            # The caller holds its own key. A busy donor is a miss, never a
            # blocking lock inversion between simultaneous computations.
            with _singleflight(root / f"{donor_key}.lock", timeout_seconds=0):
                before = payload_info(receipt_path)
                if before["size"] > 1024**2:
                    continue
                record = json.loads(receipt_path.read_bytes())
                if payload_info(receipt_path) != before:
                    continue
                if (not isinstance(record, dict) or record.get("contract") != CONTRACT
                        or record.get("key") != donor_key or not isinstance(record.get("identity"), dict)
                        or identity_digest(record["identity"]) != donor_key
                        or record.get("payload_sha256") != sha256 or record.get("payload_bytes") != size):
                    continue
                if link_immutable_payload(donor, destination, sha256=sha256, size=size):
                    return True
        except (FileNotFoundError, ValueError, TimeoutError):
            continue
    return False


def cached_forward(
    root: Path,
    identity: dict[str, Any],
    compute: Callable[[], Any],
    *,
    validate_before_commit: Callable[[], None] | None = None,
) -> tuple[Any, dict[str, Any]]:
    # Own a canonical, immutable-by-caller snapshot. JSON tuples become lists
    # consistently on both write and read; a caller cannot relabel the result
    # by mutating its identity while a compute callback is running.
    identity = json.loads(json.dumps(identity, sort_keys=True, allow_nan=False))
    key = identity_digest(identity)
    path = root / f"{key}.forward"
    started = time.perf_counter()
    with _singleflight(root / f"{key}.lock") as key_descriptor:
        invalid = False
        commit: dict[str, Any] = {}
        try:
            value = _read(path, key, identity, commit=commit)
        except (OSError, ValueError, RuntimeError, EOFError, pickle.UnpicklingError):
            value = _CACHE_MISS
            invalid = True
        if value is not _CACHE_MISS:
            if validate_before_commit is not None:
                validate_before_commit()
            usage = note_successful_use(root, key=key, **commit)
            return value, {"contract": CONTRACT, "key": key, "state": "disk_hit",
                           "gpu_forward_calls": 0, "usage": usage,
                           "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
        # A validated hot hit does not import storage/config or depend on D.
        # Only the canonical producer root enrolls the cold-first miss policy;
        # offline private caches retain their original standalone behavior.
        from stockagent.data_sync.completed_forward_restore import (
            forward_miss_admission, production_binding, restore_completed_forward,
        )

        binding = production_binding(root)
        with ExitStack() as transaction:
            admission = None
            if binding is not None:
                admission = transaction.enter_context(forward_miss_admission(root, key, MAX_PAYLOAD_BYTES))
                restored = restore_completed_forward(
                    root, key, identity, binding=binding, key_descriptor=key_descriptor,
                    read=_read, validate_before_commit=validate_before_commit,
                    maximum_payload_bytes=MAX_PAYLOAD_BYTES)
                if restored is not None:
                    value, cold = restored
                    usage = note_successful_use(root, key=key, **cold["commit"])
                    return value, {"contract": CONTRACT, "key": key, "state": "cold_restored",
                                   "gpu_forward_calls": 0, "repaired_invalid_cache": invalid,
                                   "usage": usage, "cold_restore": cold, "capacity": admission,
                                   "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
            value = _cpu_copy(compute())
            if validate_before_commit is not None:
                validate_before_commit()
            if binding is not None:
                binding.guard()
            buffer = io.BytesIO()
            torch.save(value, buffer)
            payload = buffer.getbuffer()
            if len(payload) > MAX_PAYLOAD_BYTES:
                raise ValueError(
                    f"completed inference output exceeds the bounded CPU cache: "
                    f"{len(payload)} bytes > {MAX_PAYLOAD_BYTES} bytes")
            payload_sha256 = hashlib.sha256(payload).hexdigest()
            receipt_record = {"contract": CONTRACT, "key": key, "identity": identity,
                              "payload_bytes": len(payload), "payload_sha256": payload_sha256}
            receipt_data = json.dumps(receipt_record, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()
            if len(receipt_data) > 1024**2:
                raise ValueError("completed inference identity exceeds its bounded receipt")
            # Commit payload before receipt, under both key and growth leases.
            shared = _reuse_payload(root, path, sha256=payload_sha256, size=len(payload))
            if validate_before_commit is not None:
                validate_before_commit()
            if binding is not None:
                binding.guard()
            if not shared:
                atomic_write_bytes(path, payload, durable=True, mode=0o400)
            atomic_write_bytes(path.with_suffix(".receipt.json"), receipt_data,
                               durable=True, mode=0o600)
            usage = note_successful_use(root, key=key, receipt_sha256=hashlib.sha256(receipt_data).hexdigest(),
                                        payload_sha256=payload_sha256, payload_bytes=len(payload))
            return value, {"contract": CONTRACT, "key": key, "state": "computed",
                           "repaired_invalid_cache": invalid, "payload_bytes": len(payload), "usage": usage,
                           "storage_build": {"contract": "completed_forward_payload_reuse_v1",
                                             "reused_payload": shared,
                                             "avoided_payload_bytes": len(payload) if shared else 0},
                           **({"capacity": admission} if admission is not None else {}),
                           "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
