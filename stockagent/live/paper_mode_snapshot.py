"""Versioned, immutable mode objects for the canonical paper state checkpoint.

This is a storage representation, not another ledger or an execution policy.
The engine still publishes state -> projections -> durable commit/ACK. Readers
accept the old inline representation and this opt-in manifest representation.
Objects precede the manifest's namespace barrier; unreferenced crash leftovers
are not commits and are never collected here.
"""

from __future__ import annotations

from collections import OrderedDict
import errno
import hashlib
import os
from pathlib import Path
import re
import stat
from threading import RLock
from time import perf_counter
from typing import Any, Mapping
import uuid

from stockagent.private_json import ledger_bytes, loads


CONTRACT = "paper_immutable_mode_objects_v1"
MANIFEST_KEY = "paper_mode_snapshot"
OBJECT_DIRECTORY = "mode_snapshots"
MAX_OBJECT_BYTES = 64 * 1024**2
MAX_SNAPSHOT_BYTES = 256 * 1024**2
_CACHE_BUDGET = 64 * 1024**2
_CACHE: OrderedDict[tuple, bytes] = OrderedDict()
_CACHE_BYTES = 0
_CACHE_LOCK = RLock()
_HASH = re.compile(r"[0-9a-f]{64}\Z")
# Large, usually static execution maps must not ride along with a changing
# mode header. This is representation partitioning, not dropping any field.
SECTION_BYTES = 32 * 1024


def _valid_reference(reference: Any) -> bool:
    return (
        isinstance(reference, dict)
        and set(reference) == {"sha256", "size_bytes"}
        and isinstance(reference["sha256"], str)
        and _HASH.fullmatch(reference["sha256"]) is not None
        and type(reference["size_bytes"]) is int
        and 0 < reference["size_bytes"] <= MAX_OBJECT_BYTES
    )


def _all_references(objects):
    for reference in objects.values():
        yield {key: reference[key] for key in ("sha256", "size_bytes")}
        yield from reference.get("sections", {}).values()


def _generation(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _directory(root: Path) -> int:
    return os.open(
        Path(root) / OBJECT_DIRECTORY, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    )


def _objects(payload: Mapping[str, Any]) -> dict[str, dict] | None:
    if MANIFEST_KEY not in payload:
        return None
    manifest = payload[MANIFEST_KEY]
    if (
        not isinstance(manifest, dict)
        or manifest.get("contract") != CONTRACT
        or set(manifest) != {"contract", "objects"}
        or "modes" in payload
        or not isinstance(manifest.get("objects"), dict)
        or len(manifest["objects"]) > 4096
    ):
        raise ValueError("invalid paper mode snapshot manifest")
    size = 0
    for market, reference in manifest["objects"].items():
        if (
            not isinstance(market, str)
            or not market
            or not isinstance(reference, dict)
            or set(reference) != {"sha256", "size_bytes", "sections"}
            or not _valid_reference(
                {key: reference.get(key) for key in ("sha256", "size_bytes")}
            )
            or not isinstance(reference["sections"], dict)
            or len(reference["sections"]) > 4096
            or any(
                not isinstance(key, str) or not key or not _valid_reference(value)
                for key, value in reference["sections"].items()
            )
        ):
            raise ValueError("invalid paper mode object reference")
    for reference in _all_references(manifest["objects"]):
        size += reference["size_bytes"]
    if size > MAX_SNAPSHOT_BYTES:
        raise ValueError("paper mode snapshot exceeds the bounded read budget")
    return manifest["objects"]


def _read_object(directory: int, reference: dict, *, cache: bool, suffix: str = ".json") -> bytes:
    global _CACHE_BYTES
    if suffix not in {".json", ".zst"} or not _valid_reference(reference):
        raise ValueError("invalid immutable paper object reference")
    name = reference["sha256"] + suffix
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_size != reference["size_bytes"]
        ):
            raise ValueError(
                "paper mode object is not a regular file of its declared size"
            )
        key = (reference["sha256"], *_generation(opened))
        with _CACHE_LOCK:
            body = _CACHE.get(key) if cache else None
            if body is not None:
                _CACHE.move_to_end(key)
        if body is None:
            with os.fdopen(fd, "rb", closefd=False) as handle:
                body = handle.read(reference["size_bytes"] + 1)
            if (
                len(body) != reference["size_bytes"]
                or hashlib.sha256(body).hexdigest() != reference["sha256"]
            ):
                raise ValueError("paper mode object content hash mismatch")
        current = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if _generation(opened) != _generation(os.fstat(fd)) or _generation(
            opened
        ) != _generation(current):
            raise OSError("paper mode object changed while being read")
        if cache and len(body) <= _CACHE_BUDGET:
            with _CACHE_LOCK:
                if key not in _CACHE:
                    _CACHE[key] = body
                    _CACHE_BYTES += len(body)
                while _CACHE_BYTES > _CACHE_BUDGET:
                    _, retired = _CACHE.popitem(last=False)
                    _CACHE_BYTES -= len(retired)
        return body
    finally:
        os.close(fd)


def _put_object(directory: int, body: bytes, *, suffix: str = ".json") -> tuple[dict, bool]:
    """Shared no-replace CAS for private paper representation owners.

    The caller owns the directory barrier and the eventual commit/ACK. Neither
    a newly installed object nor an orphan is a financial commit on its own.
    """
    reference = {"sha256": hashlib.sha256(body).hexdigest(), "size_bytes": len(body)}
    if suffix not in {".json", ".zst"} or not _valid_reference(reference):
        raise ValueError("immutable paper object exceeds bounded write contract")
    try:
        existing = _read_object(directory, reference, cache=True, suffix=suffix)
    except FileNotFoundError:
        capacity = os.fstatvfs(directory)
        if capacity.f_bavail * capacity.f_frsize < len(body) + 16 * 1024**2:
            raise OSError(errno.ENOSPC, "paper object capacity admission failed")
        temporary = ".pending-" + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb", closefd=False) as handle:
                handle.write(body)
                handle.flush()
                os.fchmod(fd, 0o400)
                os.fsync(fd)
            try:
                os.link(temporary, reference["sha256"] + suffix, src_dir_fd=directory,
                        dst_dir_fd=directory, follow_symlinks=False)
            except FileExistsError:
                if _read_object(directory, reference, cache=False, suffix=suffix) != body:
                    raise ValueError("paper object address collision")
                return reference, False
            return reference, True
        finally:
            os.close(fd)
            os.unlink(temporary, dir_fd=directory)
    else:
        if existing != body:
            raise ValueError("paper object address collision")
        return reference, False


def expand_paper_state(payload: dict, root: Path, *, use_cache: bool = True) -> dict:
    """Decode complete modes; unknown/corrupt manifests are NEVER empty state.

    Cached objects are immutable bytes, not shared mutable decoded dictionaries.
    Metadata is a cache invalidation fence; SHA is verified on first use and on
    every changed generation. Callers still own the committed-view guard.
    """
    references = _objects(payload)
    if references is None:
        return payload
    modes = {}
    if references:
        directory = _directory(root)
        try:
            for market, reference in references.items():
                mode = loads(_read_object(directory, {key: reference[key] for key in
                                                      ("sha256", "size_bytes")}, cache=use_cache))
                if not isinstance(mode, dict):
                    raise ValueError("paper mode object root is not an object")
                for key, section in reference["sections"].items():
                    if key in mode:
                        raise ValueError("paper mode section conflicts with its header")
                    mode[key] = loads(_read_object(directory, section, cache=use_cache))
                modes[market] = mode
        finally:
            os.close(directory)
    result = {key: value for key, value in payload.items() if key != MANIFEST_KEY}
    result["modes"] = modes
    return result


def read_paper_payload(path: Path) -> dict:
    """One bounded, stable regular-file read, never a blocking FIFO or reset."""
    path = Path(path)
    descriptor = os.open(
        path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    )
    with os.fdopen(descriptor, "rb") as handle:
        opened = os.fstat(handle.fileno())
        if not stat.S_ISREG(opened.st_mode) or opened.st_size > MAX_SNAPSHOT_BYTES:
            raise ValueError("paper JSON requires a bounded regular file")
        before = _generation(opened)
        payload = loads(handle.read(MAX_SNAPSHOT_BYTES + 1))
        if before != _generation(os.fstat(handle.fileno())) or before != _generation(
            path.stat()
        ):
            raise OSError("paper state changed while being read")
    if not isinstance(payload, dict):
        raise ValueError(f"paper JSON root is not an object: {path.name}")
    return payload


def read_paper_state(path: Path, *, use_cache: bool = True) -> dict:
    """Reader-first compatibility for state and ordinary inline JSON objects."""
    path = Path(path)
    payload = read_paper_payload(path)
    return expand_paper_state(payload, path.parent, use_cache=use_cache)


def mode_object_generations(root: Path) -> dict[str, list[int]]:
    """Bind immutable children to the same commit as the small state manifest."""
    path = Path(root) / "state.json"
    try:
        payload = read_paper_payload(path)
    except FileNotFoundError:
        return {}
    references = _objects(payload)
    if not references:
        return {}
    directory = _directory(root)
    try:
        result = {}
        for reference in _all_references(references):
            name = reference["sha256"] + ".json"
            value = os.stat(name, dir_fd=directory, follow_symlinks=False)
            if (
                not stat.S_ISREG(value.st_mode)
                or value.st_size != reference["size_bytes"]
            ):
                raise ValueError("invalid paper mode object generation")
            result[name] = list(_generation(value))
        return result
    finally:
        os.close(directory)


class ModeSnapshotWriter:
    """The engine's bounded representation writer; no state/ledger ownership."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.last_metrics: dict[str, Any] = {}
        self.object_generations: dict[str, list[int]] = {}

    def prepare(self, state: Mapping[str, Any]) -> dict:
        """Make changed objects durable BEFORE the caller publishes state.json."""
        started = perf_counter()
        if MANIFEST_KEY in state or not isinstance(state.get("modes"), dict):
            raise ValueError("complete inline engine state is required")
        directory_path = self.root / OBJECT_DIRECTORY
        directory_path.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory = _directory(self.root)
        references, created, reused, new_bytes, logical_bytes = {}, 0, 0, 0, 0
        generations = {}

        def publish_body(body):
            nonlocal created, reused, new_bytes, logical_bytes
            # Validate the complete manifest BEFORE any marker publication.
            if len(body) > MAX_OBJECT_BYTES:
                raise ValueError("paper mode object exceeds the bounded write budget")
            logical_bytes += len(body)
            if logical_bytes > MAX_SNAPSHOT_BYTES:
                raise ValueError("paper mode snapshot exceeds the bounded write budget")
            reference, installed = _put_object(directory, body)
            name = reference["sha256"] + ".json"
            created += int(installed)
            reused += int(not installed)
            new_bytes += len(body) if installed else 0
            generations[name] = list(
                _generation(os.stat(name, dir_fd=directory, follow_symlinks=False))
            )
            return reference

        try:
            for market, mode in state["modes"].items():
                if (
                    not isinstance(market, str)
                    or not market
                    or not isinstance(mode, dict)
                    or any(not isinstance(key, str) or not key for key in mode)
                ):
                    raise ValueError("complete named mode objects are required")
                header, sections = [], {}
                for key, value in mode.items():
                    body = ledger_bytes(value) + b"\n"
                    if len(body) >= SECTION_BYTES:
                        sections[key] = publish_body(body)
                    else:
                        # The value is already encoded under the canonical
                        # private codec. Do not traverse/encode it again just
                        # to construct its header; retain exact JSON scalars,
                        # arbitrary integers and legacy default=str behavior.
                        header.append((key, ledger_bytes(key) + b":" + body[:-1]))
                references[market] = {
                    **publish_body(
                        b"{" + b",".join(value for _, value in sorted(header)) + b"}\n"
                    ),
                    "sections": sections,
                }
            if len(references) > 4096:
                raise ValueError("too many paper mode objects")
            # Also sync on an all-reuse recovery: a previous process may have
            # crashed after the link but before this directory barrier.
            os.fsync(directory)
        finally:
            os.close(directory)
        self.last_metrics = {
            "contract": CONTRACT,
            "mode_count": len(references),
            "created_objects": created,
            "reused_objects": reused,
            "new_object_bytes": new_bytes,
            "logical_mode_bytes": logical_bytes,
            "prepare_ms": round((perf_counter() - started) * 1000, 3),
        }
        self.object_generations = generations
        return {
            **{key: value for key, value in state.items() if key != "modes"},
            MANIFEST_KEY: {"contract": CONTRACT, "objects": references},
        }
