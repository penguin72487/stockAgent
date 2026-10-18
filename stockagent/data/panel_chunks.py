"""Immutable Panel byte chunks with a contiguous, copy-on-write NumPy view.

The Panel writer/manifest transaction still owns publication and retirement.
Each generation owns its chunk links; there is no second object store or GC.
On 64-bit Linux, reserve the complete virtual range *before* mapping any file
inside it (mmap(2)'s safe MAP_FIXED case). This preserves the existing ndarray
ABI without concatenating the historical payload into RAM or another file.
"""
from __future__ import annotations

import ctypes
import hashlib
import json
import mmap
import os
from pathlib import Path
import stat
import sys
import uuid

import numpy as np

from stockagent.data_sync.desync_snapshots import _fsync_directory
from stockagent.data_sync.immutable_cache_files import link_immutable_payload


CHUNK_STORAGE = "panel_time_chunks_v1"
CHUNK_BYTES = 8 * 1024**2
MIN_CHUNKED_BYTES = 8 * 1024**2
PAGE_BYTES = 4096  # Persisted format alignment, not the reader's page size.


def can_chunk(array: np.ndarray) -> bool:
    return (
        os.environ.get("STOCKAGENT_PANEL_CHUNK_STORAGE", "0") == "1"
        and sys.platform == "linux" and ctypes.sizeof(ctypes.c_void_p) == 8
        and array.nbytes >= MIN_CHUNKED_BYTES and array.flags.c_contiguous
        and not array.dtype.hasobject and array.dtype.fields is None
        and array.dtype.itemsize > 0
    )


def _safe_path(root: Path, relative: str) -> Path:
    path = root / relative
    if not relative or Path(relative).is_absolute() or path.resolve() != path:
        raise ValueError(f"unsafe Panel chunk reference: {relative!r}")
    if not path.is_relative_to(root):
        raise ValueError(f"Panel chunk reference escapes its generation: {relative!r}")
    return path


def read_descriptor(root: Path, item: dict) -> tuple[Path, dict]:
    """Validate all ranges and paths before opening/mapping the first chunk."""
    descriptor_path = _safe_path(root.resolve(), str(item["file"]))
    with descriptor_path.open(encoding="utf-8") as stream:
        descriptor = json.load(stream)
    if not isinstance(descriptor, dict) or descriptor.get("storage") != CHUNK_STORAGE:
        raise ValueError("invalid Panel chunk storage contract")
    if any(descriptor.get(key) != item.get(key) for key in (
        "shape", "dtype", "content_fingerprint",
    )):
        raise ValueError("Panel chunk descriptor disagrees with its committed manifest")
    shape = descriptor["shape"]
    if not isinstance(shape, list) or not shape or any(
        type(size) is not int or size <= 0 for size in shape
    ):
        raise ValueError("invalid Panel chunk shape")
    dtype = np.dtype(descriptor["dtype"])
    if dtype.hasobject or dtype.fields is not None or dtype.itemsize <= 0:
        raise ValueError("Panel chunks require an unstructured non-object dtype")
    logical_bytes = int(np.prod(shape, dtype=object)) * dtype.itemsize
    if logical_bytes <= 0 or logical_bytes > sys.maxsize:
        raise ValueError("invalid Panel chunk byte extent")
    chunks = descriptor.get("chunks")
    if not isinstance(chunks, list) or not 0 < len(chunks) <= 1_000_000:
        raise ValueError("invalid Panel chunk inventory")
    offset = 0
    for index, chunk in enumerate(chunks):
        if not isinstance(chunk, dict) or chunk.get("offset") != offset:
            raise ValueError("Panel chunks must be an ordered complete byte partition")
        length, stored = chunk.get("logical_bytes"), chunk.get("stored_bytes")
        if (type(length) is not int or type(stored) is not int or length <= 0
                or stored != (length + PAGE_BYTES - 1) // PAGE_BYTES * PAGE_BYTES
                or (index < len(chunks) - 1 and length % PAGE_BYTES)):
            raise ValueError("invalid Panel chunk size/alignment")
        relative = chunk.get("file")
        if not isinstance(relative, str):
            raise ValueError("invalid Panel chunk file")
        path = _safe_path(root.resolve(), relative)
        # Links belong to this generation, never another generation or CAS.
        if path.parent != descriptor_path.parent / descriptor_path.stem.removesuffix(".chunks"):
            raise ValueError("Panel chunk is outside its owning array directory")
        digest = chunk.get("sha256")
        if not isinstance(digest, str) or len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise ValueError("invalid Panel chunk SHA-256")
        offset += length
    if offset != logical_bytes:
        raise ValueError("Panel chunk byte coverage does not match array shape")
    return descriptor_path, descriptor


def array_paths(root: Path, item: dict) -> list[Path]:
    if item.get("storage") != CHUNK_STORAGE:
        return [_safe_path(root.resolve(), str(item["file"]))]
    descriptor_path, descriptor = read_descriptor(root, item)
    return [descriptor_path, *[
        _safe_path(root.resolve(), chunk["file"]) for chunk in descriptor["chunks"]
    ]]


def add_donors(root: Path, item: dict, donors: dict) -> None:
    if item.get("storage") != CHUNK_STORAGE:
        return
    _, descriptor = read_descriptor(root, item)
    for chunk in descriptor["chunks"]:
        key = (chunk["sha256"], chunk["stored_bytes"])
        path = _safe_path(root.resolve(), chunk["file"])
        if path not in donors.setdefault(key, []):
            donors[key].append(path)


def write_chunks(generation_dir: Path, name: str, array: np.ndarray,
                 fingerprint: dict, donors: dict, counters: dict) -> dict:
    """Write only changed complete byte chunks, including a bounded tail."""
    folder = generation_dir / name
    folder.mkdir()
    raw = memoryview(array.view(np.uint8).reshape(-1))
    chunks = []
    for offset in range(0, len(raw), CHUNK_BYTES):
        block = raw[offset:offset + CHUNK_BYTES]
        padding = bytes((-len(block)) % PAGE_BYTES)
        digest = hashlib.sha256(block)
        digest.update(padding)
        sha = digest.hexdigest()
        size = len(block) + len(padding)
        path = folder / f"{offset:016x}-{sha}.bin"
        key = (sha, size)
        linked = False
        for donor in donors.get(key, ()):
            if link_immutable_payload(donor, path, sha256=sha, size=size):
                linked = True
                break
        if linked:
            counters["reused_chunks"] += 1
            counters["avoided_chunk_bytes"] += size
        else:
            temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
            try:
                with temporary.open("xb") as stream:
                    stream.write(block)
                    stream.write(padding)
                    stream.flush()
                    os.fchmod(stream.fileno(), 0o400)
                    os.fsync(stream.fileno())
                temporary.replace(path)
                _fsync_directory(folder)
            finally:
                temporary.unlink(missing_ok=True)
            counters["written_chunks"] += 1
            counters["written_chunk_bytes"] += size
        if path not in donors.setdefault(key, []):
            donors[key].append(path)
        chunks.append({
            "file": str(path.relative_to(generation_dir.parent.parent)),
            "offset": offset, "logical_bytes": len(block), "stored_bytes": size,
            "sha256": sha,
        })
    return {"storage": CHUNK_STORAGE, "shape": list(array.shape),
            "dtype": str(array.dtype), "content_fingerprint": fingerprint,
            "chunks": chunks}


def _open_chunk(root: Path, chunk: dict) -> int:
    path = _safe_path(root.resolve(), chunk["file"])
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_size != chunk["stored_bytes"]
                or info.st_mode & 0o222):
            raise ValueError(f"invalid immutable Panel chunk: {path}")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def load_chunks(root: Path, item: dict, mmap_mode: str | None = "r") -> np.ndarray:
    if mmap_mode not in ("r", "c", None):
        raise ValueError("immutable Panel chunks support only mmap_mode='r', 'c', or None")
    descriptor_path, descriptor = read_descriptor(root, item)
    shape, dtype = tuple(descriptor["shape"]), np.dtype(descriptor["dtype"])
    chunks = descriptor["chunks"]
    extent = chunks[-1]["offset"] + chunks[-1]["stored_bytes"]
    native = sys.platform == "linux" and ctypes.sizeof(ctypes.c_void_p) == 8
    if not native or any(
        chunk["offset"] % mmap.PAGESIZE or chunk["stored_bytes"] % mmap.PAGESIZE
        for chunk in chunks
    ):
        # Portable recovery path, explicit allocation rather than a fake mmap.
        result = np.empty(shape, dtype=dtype)
        raw = memoryview(result.view(np.uint8).reshape(-1))
        for chunk in chunks:
            descriptor_fd = _open_chunk(root, chunk)
            with os.fdopen(descriptor_fd, "rb") as stream:
                start, length = chunk["offset"], chunk["logical_bytes"]
                if stream.readinto(raw[start:start + length]) != length:
                    raise ValueError("short Panel chunk read")
        if mmap_mode == "r":
            result.flags.writeable = False
        return result

    protection = mmap.PROT_READ | (mmap.PROT_WRITE if mmap_mode == "c" else 0)
    reservation = mmap.mmap(-1, extent, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS,
                            prot=protection)
    try:
        address = np.frombuffer(reservation, dtype=np.uint8, count=1).ctypes.data
        libc = ctypes.CDLL(None, use_errno=True)
        native_mmap = libc.mmap
        native_mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                               ctypes.c_int, ctypes.c_int, ctypes.c_long]
        native_mmap.restype = ctypes.c_void_p
        # MAP_FIXED is Linux ABI 0x10. Every target is contained in the private
        # reservation above; no /proc-map gap guessing or external address use.
        for chunk in chunks:
            descriptor_fd = _open_chunk(root, chunk)
            try:
                target = address + chunk["offset"]
                mapped = native_mmap(target, chunk["stored_bytes"], protection,
                                     mmap.MAP_PRIVATE | 0x10, descriptor_fd, 0)
                if mapped != target:
                    number = ctypes.get_errno()
                    raise OSError(number, os.strerror(number))
            finally:
                os.close(descriptor_fd)
        result = np.ndarray.__new__(np.memmap, shape, dtype=dtype, buffer=reservation)
        result._mmap, result.mode = reservation, mmap_mode or "r"
        result.offset, result.filename = 0, descriptor_path
        if mmap_mode is None:
            copied = np.array(result, copy=True, subok=False)
            del result
            reservation.close()
            return copied
        return result
    except BaseException:
        reservation.close()
        raise
