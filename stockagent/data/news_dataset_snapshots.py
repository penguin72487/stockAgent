"""Exact byte-chunk snapshots of the news builder's SQLite input projection.

Only new/changed chunks are retained. Legacy frozen SQLite files may donate
bounded byte ranges without copying them. This is owned by the existing corpus
transaction; it creates no global object pool, publisher, GC or source writer.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile

from downloader.artifact_io import atomic_write_bytes, durable_replace, sha256_file
from stockagent.data.news_dataset_sources import checked_path


FORMAT = "news_sqlite_chunks_v1"
CHUNK_BYTES = 1024**2


def _path(root: Path, relative: str) -> Path:
    candidate = root / relative
    if (not relative or Path(relative).is_absolute() or ".." in Path(relative).parts
            or not candidate.resolve().is_relative_to(root.resolve()) or not candidate.is_file()):
        raise ValueError("invalid news snapshot chunk path")
    return candidate


def _legacy_chunks(root: Path, generation: Path, plan: dict) -> list[dict]:
    path = checked_path(generation, plan["snapshot_path"])
    if sha256_file(path) != plan["snapshot_sha256"]:
        raise ValueError("news baseline snapshot checksum mismatch")
    chunks, offset = [], 0
    with path.open("rb") as stream:
        while data := stream.read(CHUNK_BYTES):
            chunks.append({"path": str(path.relative_to(root)), "offset": offset,
                           "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            offset += len(data)
    return chunks


def snapshot_bytes(root: Path, plan: dict):
    storage = plan["snapshot_storage"]
    if storage.get("format") != FORMAT or storage.get("chunk_bytes") != CHUNK_BYTES:
        raise ValueError("invalid news snapshot storage contract")
    chunks = storage.get("chunks")
    if not isinstance(chunks, list) or not 0 < len(chunks) <= 1_000_000:
        raise ValueError("invalid news snapshot chunk inventory")
    digest, length = hashlib.sha256(), 0
    for number, chunk in enumerate(chunks):
        offset, size = chunk["offset"], chunk["bytes"]
        if (type(offset) is not int or offset < 0 or type(size) is not int or not 0 < size <= CHUNK_BYTES
                or (number < len(chunks) - 1 and size != CHUNK_BYTES)):
            raise ValueError("invalid news snapshot chunk extent")
        path = _path(root, chunk["path"])
        with path.open("rb") as stream:
            stream.seek(offset)
            data = stream.read(size)
        if len(data) != size or hashlib.sha256(data).hexdigest() != chunk["sha256"]:
            raise ValueError("news snapshot chunk checksum mismatch")
        digest.update(data)
        length += size
        yield data
    if length != storage["logical_bytes"] or digest.hexdigest() != plan["snapshot_sha256"]:
        raise ValueError("news reconstructed snapshot checksum/size mismatch")


def verify_snapshot(root: Path, generation: Path, plan: dict) -> None:
    if "snapshot_storage" in plan:
        for _ in snapshot_bytes(root, plan):
            pass
    elif sha256_file(checked_path(generation, plan["snapshot_path"])) != plan["snapshot_sha256"]:
        raise ValueError("frozen news input snapshot changed")


def seal_snapshot(root: Path, generation: Path, plan: dict, parent: dict | None) -> dict:
    if "snapshot_storage" in plan:
        verify_snapshot(root, generation, plan)
        return plan
    donors = []
    if parent:
        old = parent["source_snapshot"]
        old_generation = checked_path(root, parent["generation"] + "/generation.json").parent
        if "snapshot_storage" in old:
            verify_snapshot(root, old_generation, old)
            donors = old["snapshot_storage"]["chunks"]
        else:
            donors = _legacy_chunks(root, old_generation, old)
    path = checked_path(generation, plan["snapshot_path"])
    digest, chunks, written, reused = hashlib.sha256(), [], 0, 0
    with path.open("rb") as stream:
        while data := stream.read(CHUNK_BYTES):
            sha = hashlib.sha256(data).hexdigest()
            number = len(chunks)
            if number < len(donors) and donors[number]["sha256"] == sha and donors[number]["bytes"] == len(data):
                chunk = dict(donors[number])
                reused += len(data)
            else:
                destination = generation / "snapshot_chunks" / f"{number:08d}-{sha}.bin"
                atomic_write_bytes(destination, data, durable=True, mode=0o444)
                chunk = {"path": str(destination.relative_to(root)), "offset": 0, "bytes": len(data), "sha256": sha}
                written += len(data)
            digest.update(data)
            chunks.append(chunk)
    if digest.hexdigest() != plan["snapshot_sha256"]:
        raise ValueError("frozen news input snapshot changed before sealing")
    return {**plan, "snapshot_storage": {"format": FORMAT, "chunk_bytes": CHUNK_BYTES, "chunks": chunks,
            "logical_bytes": written + reused, "written_chunk_bytes": written, "reused_chunk_bytes": reused}}


def copy_snapshot(root: Path, generation: Path, plan: dict, destination: Path) -> None:
    """Reconstruct into a caller-owned work file, never into a prior release."""
    descriptor, temporary = tempfile.mkstemp(prefix=".snapshot-", suffix=".tmp", dir=destination.parent)
    temporary = Path(temporary)
    try:
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "wb") as stream:
            if "snapshot_storage" in plan:
                for block in snapshot_bytes(root, plan):
                    stream.write(block)
                    digest.update(block)
            else:
                with checked_path(generation, plan["snapshot_path"]).open("rb") as original:
                    while block := original.read(CHUNK_BYTES):
                        stream.write(block)
                        digest.update(block)
        if digest.hexdigest() != plan["snapshot_sha256"]:
            raise ValueError("reused news snapshot source/checksum mismatch")
        durable_replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def discard_new_build_work(root: Path, generation: Path, manifest: dict) -> None:
    """Remove only this new transaction's reconstructible scratch after commit.

    Existing legacy generations, sources and retained parts are never targets.
    Exact source-projection bytes remain in the verified chunk inventory.
    """
    plan = manifest["source_snapshot"]
    if manifest.get("contract_version") != 2 or "snapshot_storage" not in plan:
        return
    if generation.resolve() != (root / manifest["generation"]).resolve():
        raise ValueError("news build-work generation mismatch")
    active = json.loads((root / ".active-build.json").read_text())
    published = json.loads((root / "manifest.json").read_text())
    if published != manifest or any(manifest.get(key) != value for key, value in active.items()):
        raise ValueError("news scratch cleanup requires its own committed active transaction")
    own_snapshot = str((generation / "snapshot.sqlite3").relative_to(root))
    if any(chunk["path"] == own_snapshot for chunk in plan["snapshot_storage"]["chunks"]):
        raise ValueError("cannot retire a referenced news input snapshot")
    for name in ("snapshot.sqlite3", "build.sqlite3", "build.sqlite3-wal", "build.sqlite3-shm"):
        path = generation / name
        if path.is_symlink():
            raise ValueError("news transient work is redirected")
        path.unlink(missing_ok=True)
