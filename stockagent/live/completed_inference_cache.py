"""Lossless CPU forward results for completed, receipt-accepted TW sessions.

This cache owns neither session acceptance nor orders. Its caller must exclude
live quotes. A complete input identity, not a date or display TTL, owns reuse.
Only tensors and safe primitive containers are serialized; a cache hit never
constructs a model, transfers to CUDA, or requires a working GPU.
"""

from __future__ import annotations

from contextlib import contextmanager
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
import time
from typing import Any, Callable, Iterator

import numpy as np
import torch

from downloader.artifact_io import atomic_write_bytes

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


def implementation_identity() -> list[dict[str, Any]]:
    repo = Path(__file__).resolve().parents[2]
    paths = {Path(__file__), repo / "stockagent/live/signal_engine.py",
             repo / "stockagent/config.py", repo / "stockagent/training/runtime.py",
             repo / "stockagent/training/runtime_configuration.py",
             repo / "stockagent/training/inference_contract.py"}
    for directory in ("stockagent/models", "stockagent/data"):
        paths.update((repo / directory).rglob("*.py"))
    return [file_identity(path) for path in sorted(paths)]


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
def _singleflight(path: Path, *, timeout_seconds: float = 60.0) -> Iterator[None]:
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
        yield
    finally:
        os.close(descriptor)  # also releases the lease after crash/exception


def _read(path: Path, key: str, identity: dict[str, Any]) -> Any:
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
            receipt = json.loads(receipt_handle.read(1024**2 + 1))
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
    return _cpu_copy(result, clone=False)


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
    with _singleflight(root / f"{key}.lock"):
        invalid = False
        try:
            value = _read(path, key, identity)
        except (OSError, ValueError, RuntimeError, EOFError, pickle.UnpicklingError):
            value = _CACHE_MISS
            invalid = True
        if value is not _CACHE_MISS:
            if validate_before_commit is not None:
                validate_before_commit()
            return value, {"contract": CONTRACT, "key": key, "state": "disk_hit",
                           "gpu_forward_calls": 0,
                           "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
        value = _cpu_copy(compute())
        if validate_before_commit is not None:
            validate_before_commit()
        buffer = io.BytesIO()
        torch.save(value, buffer)
        payload = buffer.getbuffer()
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError(
                f"completed inference output exceeds the bounded CPU cache: "
                f"{len(payload)} bytes > {MAX_PAYLOAD_BYTES} bytes")
        receipt_data = json.dumps({"contract": CONTRACT, "key": key, "identity": identity,
                                   "payload_bytes": len(payload),
                                   "payload_sha256": hashlib.sha256(payload).hexdigest()},
                                  sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if len(receipt_data) > 1024**2:
            raise ValueError("completed inference identity exceeds its bounded receipt")
        # Commit payload before receipt, under the kernel lease. A crash between
        # replaces is a detectable incomplete cache, never a ready observation.
        atomic_write_bytes(path, payload, durable=True, mode=0o600)
        atomic_write_bytes(path.with_suffix(".receipt.json"), receipt_data,
                           durable=True, mode=0o600)
        return value, {"contract": CONTRACT, "key": key, "state": "computed",
                       "repaired_invalid_cache": invalid, "payload_bytes": len(payload),
                       "elapsed_ms": round((time.perf_counter() - started) * 1000, 3)}
