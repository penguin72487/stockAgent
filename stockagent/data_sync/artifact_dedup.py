"""Conservative content deduplication for stable artifact files.

The deduplicator keeps every logical path and replaces byte-identical regular
files with hard links.  Mutable runtime trees and recently modified files are
excluded so independent writers cannot accidentally share an inode.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import stat
import time
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_EXCLUDED_TOP = frozenset(
    {
        "daily_downloader",
        "data_capture",
        "data_locks",
        "data_refresh",
        "data_repair",
        "discord_bot",
        "live",
        "live_signals",
        "logs",
        "orders",
        "run_logs",
    }
)
DEFAULT_EXCLUDED_SUFFIXES = frozenset({".jsonl", ".lock", ".log", ".pid"})


@dataclasses.dataclass(frozen=True)
class ArtifactFile:
    relative: str
    device: int
    inode: int
    size: int
    blocks: int
    mode: int
    uid: int
    gid: int
    mtime_ns: int
    xattrs: tuple[tuple[str, bytes], ...]

    @classmethod
    def from_path(cls, root: Path, path: Path) -> "ArtifactFile":
        metadata = path.stat(follow_symlinks=False)
        xattrs = tuple(
            (name, os.getxattr(path, name, follow_symlinks=False))
            for name in sorted(os.listxattr(path, follow_symlinks=False))
        )
        return cls(
            relative=path.relative_to(root).as_posix(),
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size=metadata.st_size,
            blocks=metadata.st_blocks,
            mode=stat.S_IMODE(metadata.st_mode),
            uid=metadata.st_uid,
            gid=metadata.st_gid,
            mtime_ns=metadata.st_mtime_ns,
            xattrs=xattrs,
        )

    def signature(self) -> tuple[int, int, int, int]:
        return self.device, self.inode, self.size, self.mtime_ns


@dataclasses.dataclass(frozen=True)
class DuplicateGroup:
    sha256: str
    size: int
    canonical: ArtifactFile
    duplicates: tuple[ArtifactFile, ...]

    @property
    def reclaimable_bytes(self) -> int:
        return self.size * len(self.duplicates)

    @property
    def reclaimable_allocated_bytes(self) -> int:
        return sum(item.blocks * 512 for item in self.duplicates)


def _iter_files(
    root: Path,
    *,
    cutoff_ns: int,
    excluded_top: frozenset[str],
    excluded_suffixes: frozenset[str],
    completed_roots: tuple[tuple[str, ...], ...] | None,
    blocked_roots: tuple[tuple[str, ...], ...],
) -> Iterable[ArtifactFile]:
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        if directory_path == root:
            dirnames[:] = sorted(name for name in dirnames if name not in excluded_top)
        else:
            dirnames.sort()
        for name in sorted(filenames):
            path = directory_path / name
            relative_parts = path.relative_to(root).parts
            if completed_roots is not None:
                completed_depth = max(
                    (
                        len(candidate)
                        for candidate in completed_roots
                        if relative_parts[: len(candidate)] == candidate
                    ),
                    default=-1,
                )
                blocked_depth = max(
                    (
                        len(candidate)
                        for candidate in blocked_roots
                        if relative_parts[: len(candidate)] == candidate
                    ),
                    default=-1,
                )
                if completed_depth < 0 or blocked_depth >= completed_depth:
                    continue
            if path.suffix.lower() in excluded_suffixes:
                continue
            try:
                metadata = path.stat(follow_symlinks=False)
            except FileNotFoundError:
                continue
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_size <= 0
                or metadata.st_mtime_ns > cutoff_ns
            ):
                continue
            yield ArtifactFile.from_path(root, path)


def _artifact_completion_roots(
    root: Path,
) -> tuple[tuple[tuple[str, ...], ...], tuple[tuple[str, ...], ...]]:
    """Return complete and mutable run roots from lifecycle progress envelopes."""

    completed: list[tuple[str, ...]] = []
    blocked: list[tuple[str, ...]] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames.sort()
        if "progress.json" not in filenames:
            continue
        path = Path(directory) / "progress.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        relative = Path(directory).relative_to(root).parts
        if payload.get("state") == "complete" and payload.get("phase") == "complete":
            completed.append(relative)
        else:
            blocked.append(relative)
    return tuple(sorted(completed)), tuple(sorted(blocked))


def _hash_file(root: Path, item: ArtifactFile) -> str | None:
    path = root / item.relative
    try:
        before = path.stat(follow_symlinks=False)
    except (FileNotFoundError, OSError):
        return None
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != item.signature():
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb", buffering=0) as stream:
            while chunk := stream.read(8 * 1024 * 1024):
                digest.update(chunk)
        after = path.stat(follow_symlinks=False)
    except (FileNotFoundError, OSError):
        return None
    if (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ) != item.signature():
        return None
    return digest.hexdigest()


def find_duplicate_groups(
    root: Path,
    *,
    min_age_hours: float = 24.0,
    excluded_top: frozenset[str] = DEFAULT_EXCLUDED_TOP,
    excluded_suffixes: frozenset[str] = DEFAULT_EXCLUDED_SUFFIXES,
    require_complete_marker: bool = False,
) -> tuple[list[DuplicateGroup], dict[str, int]]:
    """Return exact duplicate groups and audit counters without changing files."""

    root = root.resolve()
    cutoff_ns = time.time_ns() - int(min_age_hours * 3600 * 1_000_000_000)
    completed_roots: tuple[tuple[str, ...], ...] | None = None
    blocked_roots: tuple[tuple[str, ...], ...] = ()
    if require_complete_marker:
        completed_roots, blocked_roots = _artifact_completion_roots(root)
    files = list(
        _iter_files(
            root,
            cutoff_ns=cutoff_ns,
            excluded_top=excluded_top,
            excluded_suffixes=excluded_suffixes,
            completed_roots=completed_roots,
            blocked_roots=blocked_roots,
        )
    )
    by_size: dict[int, list[ArtifactFile]] = defaultdict(list)
    for item in files:
        by_size[item.size].append(item)

    hashed_inodes = 0
    changed_while_hashing = 0
    exact: dict[
        tuple[int, int, str, int, int, int, tuple[tuple[str, bytes], ...]],
        list[ArtifactFile],
    ] = defaultdict(list)
    for size, candidates in sorted(by_size.items()):
        distinct: dict[tuple[int, int], ArtifactFile] = {}
        for item in candidates:
            distinct.setdefault((item.device, item.inode), item)
        if len(distinct) < 2:
            continue
        for item in distinct.values():
            digest = _hash_file(root, item)
            if digest is None:
                changed_while_hashing += 1
                continue
            hashed_inodes += 1
            exact[
                (
                    item.device,
                    size,
                    digest,
                    item.mode,
                    item.uid,
                    item.gid,
                    item.xattrs,
                )
            ].append(item)

    groups: list[DuplicateGroup] = []
    for (
        _device,
        size,
        digest,
        _mode,
        _uid,
        _gid,
        _xattrs,
    ), matches in exact.items():
        if len(matches) < 2:
            continue
        ordered = sorted(
            matches,
            key=lambda item: (-item.mtime_ns, len(Path(item.relative).parts), item.relative),
        )
        groups.append(
            DuplicateGroup(
                sha256=digest,
                size=size,
                canonical=ordered[0],
                duplicates=tuple(ordered[1:]),
            )
        )
    groups.sort(key=lambda item: (-item.reclaimable_bytes, item.canonical.relative))
    counters = {
        "eligible_files": len(files),
        "eligible_bytes": sum(item.size for item in files),
        "same_size_groups": sum(1 for value in by_size.values() if len(value) > 1),
        "hashed_distinct_inodes": hashed_inodes,
        "changed_while_hashing": changed_while_hashing,
        "exact_duplicate_groups": len(groups),
        "duplicate_inodes": sum(len(group.duplicates) for group in groups),
        "reclaimable_logical_bytes": sum(group.reclaimable_bytes for group in groups),
        "reclaimable_allocated_bytes": sum(
            group.reclaimable_allocated_bytes for group in groups
        ),
        "completed_marker_roots": len(completed_roots or ()),
        "blocked_marker_roots": len(blocked_roots),
    }
    return groups, counters


def apply_duplicate_groups(
    root: Path, groups: Iterable[DuplicateGroup]
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Atomically hard-link audited duplicates, revalidating every byte first."""

    root = root.resolve()
    replaced: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    for group in groups:
        canonical_path = root / group.canonical.relative
        canonical_digest = _hash_file(root, group.canonical)
        if canonical_digest != group.sha256:
            skipped.append(
                {"path": group.canonical.relative, "reason": "canonical_changed"}
            )
            continue
        for duplicate in group.duplicates:
            duplicate_path = root / duplicate.relative
            current_digest = _hash_file(root, duplicate)
            if current_digest != group.sha256:
                skipped.append({"path": duplicate.relative, "reason": "file_changed"})
                continue
            try:
                canonical_current = ArtifactFile.from_path(root, canonical_path)
                if canonical_current.signature() != group.canonical.signature():
                    skipped.append(
                        {"path": duplicate.relative, "reason": "canonical_changed"}
                    )
                    continue
                if os.path.samestat(canonical_path.stat(), duplicate_path.stat()):
                    continue
                temporary = duplicate_path.parent / (
                    f".stockagent-dedup.{duplicate_path.name}.{os.getpid()}."
                    f"{uuid.uuid4().hex}.tmp"
                )
                os.link(canonical_path, temporary, follow_symlinks=False)
                current = ArtifactFile.from_path(root, duplicate_path)
                if current.signature() != duplicate.signature():
                    temporary.unlink(missing_ok=True)
                    skipped.append(
                        {"path": duplicate.relative, "reason": "file_changed_before_replace"}
                    )
                    continue
                os.replace(temporary, duplicate_path)
                replaced.append(
                    {
                        "path": duplicate.relative,
                        "canonical": group.canonical.relative,
                        "sha256": group.sha256,
                        "size": group.size,
                        "old_inode": duplicate.inode,
                        "new_inode": group.canonical.inode,
                        "old_mtime_ns": duplicate.mtime_ns,
                        "new_mtime_ns": group.canonical.mtime_ns,
                    }
                )
            except OSError as exc:
                try:
                    temporary.unlink(missing_ok=True)
                except UnboundLocalError:
                    pass
                skipped.append(
                    {
                        "path": duplicate.relative,
                        "reason": f"{type(exc).__name__}:{exc.errno}",
                    }
                )
    return replaced, skipped


def groups_as_json(groups: Iterable[DuplicateGroup]) -> list[dict[str, object]]:
    return [
        {
            "sha256": group.sha256,
            "size": group.size,
            "canonical": group.canonical.relative,
            "duplicates": [item.relative for item in group.duplicates],
            "reclaimable_logical_bytes": group.reclaimable_bytes,
            "reclaimable_allocated_bytes": group.reclaimable_allocated_bytes,
        }
        for group in groups
    ]


__all__ = [
    "DEFAULT_EXCLUDED_SUFFIXES",
    "DEFAULT_EXCLUDED_TOP",
    "DuplicateGroup",
    "apply_duplicate_groups",
    "find_duplicate_groups",
    "groups_as_json",
]
