"""Lossless sharing primitives for explicitly owned immutable-payload contracts.

Owners must hold every affected canonical writer lock, validate their own
committed manifests/receipts and exclude process references before sharing.
This module discovers no roots, retires no logical names and creates no pool.
Each surviving logical name owns its link; mutable databases are not eligible.
Source owners additionally validate original provenance receipts, as FinLab does.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import uuid

from stockagent.data_sync.desync_snapshots import _fsync_directory
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash


def inode_key(item: dict) -> tuple[int, int]:
    return item["device"], item["inode"]


def allocated_bytes(files: list[dict]) -> int:
    return sum({inode_key(item): item["allocated"] for item in files}.values())


def _equivalence(item: dict) -> tuple:
    attrs = hashlib.sha256(json.dumps(item["xattrs"], sort_keys=True).encode()).hexdigest()
    return (item["device"], item["size"], item["sha256"], item["uid"],
            item["gid"], item["mode"] & ~0o222, attrs)


def duplicate_payload_groups(files: list[dict]) -> tuple[list[list[list[dict]]], int]:
    """Group complete inode aliases, retaining unknown external hardlinks."""
    aliases = defaultdict(list)
    for item in files:
        aliases[inode_key(item)].append(item)
    grouped = defaultdict(list)
    external = 0
    for names in aliases.values():
        first = names[0]
        if any(item["sha256"] != first["sha256"] for item in names):
            raise ValueError("one cache inode has contradictory content hashes")
        if any(item["nlink"] != len(names) for item in names):
            external += 1
            continue
        grouped[_equivalence(first)].append(sorted(names, key=lambda row: row["path"]))
    groups = [sorted(value, key=lambda names: (-len(names), names[0]["path"]))
              for value in grouped.values() if len(value) > 1]
    return sorted(groups, key=lambda group: group[0][0]["path"]), external


def check_payload_identity(path: Path, original: dict, *, nlink: int,
                           readonly: bool = False) -> dict:
    current = payload_info(path)
    for field in ("device", "inode", "size", "mtime_ns", "uid", "gid", "xattrs"):
        if current[field] != original[field]:
            raise ValueError(f"immutable sharing identity changed: {path}")
    mode = original["mode"] & ~0o222 if readonly else original["mode"]
    if current["mode"] != mode or current["nlink"] != nlink:
        raise ValueError(f"immutable sharing permissions/links changed: {path}")
    return current


def share_locked_payload_group(
    root: Path, group: list[list[dict]], *, touched: set[Path], report: dict,
    retired_handles: ExitStack, temporary_tag: str,
) -> set[tuple[int, int]]:
    """Share an owner-verified class while its native writer locks are held.

    Full bytes are checked again inside the lease. Old inodes stay pinned until
    the caller probes retired mmap/FD references; recycled inode IDs cannot
    masquerade as readers. Interruption leaves all logical names with identical
    bytes and the caller's receipt owner records the partial attempt.
    """
    if temporary_tag not in {"session-share", "forward-share", "source-share"} or len(group) < 2:
        raise ValueError("explicit supported sharing operation and duplicate group required")
    if any(not names for names in group):
        raise ValueError("empty cache inode alias set")
    equivalence, named_paths, inodes = _equivalence(group[0][0]), set(), set()
    for names in group:
        key = inode_key(names[0])
        if key in inodes:
            raise ValueError("duplicate cache inode alias set")
        inodes.add(key)
        for item in names:
            if (_equivalence(item) != equivalence or inode_key(item) != key
                    or item["path"] in named_paths):
                raise ValueError("cache group is not one exact payload equivalence class")
            named_paths.add(item["path"])
            relative = Path(item["path"])
            path = root / relative
            if (relative.is_absolute() or ".." in relative.parts
                    or path.resolve(strict=True) != path):
                raise ValueError("cache sharing path is redirected/outside its root")
            check_payload_identity(path, item, nlink=len(names))
        first = names[0]
        path = root / first["path"]
        if stable_payload_hash(path, payload_info(path)) != first["sha256"]:
            raise ValueError("cache payload SHA changed before sharing")
    canonical = group[0][0]
    canonical_path = root / canonical["path"]
    os.chmod(canonical_path, canonical["mode"] & ~0o222, follow_symlinks=False)
    with canonical_path.open("rb") as stream:
        os.fsync(stream.fileno())
    canonical_links, retired, changed_directories = len(group[0]), set(), set()
    try:
        for names in group[1:]:
            descriptor = os.open(root / names[0]["path"], os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            retired_handles.callback(os.close, descriptor)
            pinned = os.fstat(descriptor)
            if (pinned.st_dev, pinned.st_ino) != inode_key(names[0]):
                raise ValueError("cache retired inode changed before pin")
            remaining = len(names)
            for item in names:
                path = root / item["path"]
                check_payload_identity(canonical_path, canonical, nlink=canonical_links, readonly=True)
                check_payload_identity(path, item, nlink=remaining)
                temporary = path.with_name(f".{path.name}.{temporary_tag}-{uuid.uuid4().hex}.tmp")
                try:
                    os.link(canonical_path, temporary, follow_symlinks=False)
                    check_payload_identity(path, item, nlink=remaining)
                    os.replace(temporary, path)
                finally:
                    temporary.unlink(missing_ok=True)
                touched.add(path.parent)
                changed_directories.add(path.parent)
                canonical_links += 1
                remaining -= 1
                report["replaced_paths"] += 1
            if os.fstat(descriptor).st_nlink != 0:
                raise ValueError("retired cache inode gained an external alias")
            retired.add(inode_key(names[0]))
        if stable_payload_hash(canonical_path, payload_info(canonical_path)) != canonical["sha256"]:
            raise ValueError("shared cache payload SHA differs")
        for names in group:
            for item in names:
                if inode_key(payload_info(root / item["path"])) != inode_key(canonical):
                    raise ValueError("shared cache alias readback differs")
    finally:
        for directory in sorted(changed_directories):
            _fsync_directory(directory)
    return retired
