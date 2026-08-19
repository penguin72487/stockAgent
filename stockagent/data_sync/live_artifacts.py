"""Direct, local-authoritative artifact replication helpers.

Syncthing writes into a transport directory.  This module immediately links
new files into the working artifact tree and republishes the local file for
every path that already exists locally.  There are deliberately no deletions
or snapshot directories in this layer.
"""

from __future__ import annotations

import dataclasses
import errno
import os
import shutil
import time
import uuid
from pathlib import Path, PurePosixPath


CONTROL_NAMES = {
    ".stfolder",
    ".stignore",
    ".stignore-cold-local",
    ".stversions",
}
NODE_LOCAL_SUFFIXES = {".lock", ".pid"}
COLD_IGNORE_INCLUDE = ".stignore-cold-local"


@dataclasses.dataclass
class ReconcileResult:
    incoming_added: int = 0
    local_published: int = 0
    local_conflicts_won: int = 0
    already_linked: int = 0
    ignored: int = 0
    unsupported: int = 0

    def as_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


def _validate_roots(local_root: Path, sync_root: Path) -> tuple[Path, Path]:
    local = local_root.resolve()
    sync = sync_root.resolve()
    if local == sync or local in sync.parents or sync in local.parents:
        raise ValueError("local and Syncthing roots must not overlap")
    return local, sync


def _relative_path(value: str | Path) -> PurePosixPath:
    raw = PurePosixPath(Path(value).as_posix())
    if raw.is_absolute() or not raw.parts or any(part in {"", ".", ".."} for part in raw.parts):
        raise ValueError(f"unsafe relative artifact path: {value!s}")
    return raw


def is_ignored_artifact(relative: str | Path) -> bool:
    """Return whether a path is transport control or node-local runtime state."""

    relative_path = _relative_path(relative)
    parts = relative_path.parts
    name = parts[-1]
    if any(part in CONTROL_NAMES for part in parts):
        return True
    if parts[0] == "data_locks":
        return True
    if name.startswith(".syncthing.") or ".sync-conflict-" in name:
        return True
    return any(name.endswith(suffix) for suffix in NODE_LOCAL_SUFFIXES)


def cold_ignored_artifacts(sync_root: Path) -> frozenset[PurePosixPath]:
    """Load exact paths generated only after local cold materialization."""

    path = sync_root / COLD_IGNORE_INCLUDE
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return frozenset()
    ignored: set[PurePosixPath] = set()
    for line in lines:
        value = line.strip()
        if not value or value.startswith("//"):
            continue
        if not value.startswith("(?d)/"):
            raise ValueError(f"unsupported generated cold ignore pattern: {value}")
        ignored.add(_relative_path(value[len("(?d)/") :]))
    return frozenset(ignored)


def _same_inode(first: Path, second: Path) -> bool:
    try:
        return os.path.samestat(first.stat(), second.stat())
    except (FileNotFoundError, OSError):
        return False


def _atomic_link_or_copy(source: Path, destination: Path, *, replace: bool) -> bool:
    """Install one regular file atomically, preferring a zero-copy hard link."""

    try:
        source_stat = source.stat()
    except FileNotFoundError:
        return False
    if not source.is_file() or source.is_symlink():
        return False

    destination.parent.mkdir(parents=True, exist_ok=True)
    if not replace and os.path.lexists(destination):
        return False
    if destination.exists() and not destination.is_file():
        return False
    if destination.is_symlink():
        return False

    temporary = destination.parent / (
        f".stockagent-live-sync.{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        try:
            os.link(source, temporary, follow_symlinks=False)
        except OSError as exc:
            if exc.errno not in {
                errno.EXDEV,
                errno.EPERM,
                errno.EACCES,
                errno.EOPNOTSUPP,
            }:
                raise
            shutil.copy2(source, temporary, follow_symlinks=False)
        if not replace and os.path.lexists(destination):
            temporary.unlink(missing_ok=True)
            return False
        current_source_stat = source.stat()
        if (
            current_source_stat.st_dev,
            current_source_stat.st_ino,
            current_source_stat.st_size,
            current_source_stat.st_mtime_ns,
        ) != (
            source_stat.st_dev,
            source_stat.st_ino,
            source_stat.st_size,
            source_stat.st_mtime_ns,
        ):
            temporary.unlink(missing_ok=True)
            return False
        os.replace(temporary, destination)
        return True
    except FileNotFoundError:
        temporary.unlink(missing_ok=True)
        return False
    finally:
        temporary.unlink(missing_ok=True)


def _iter_regular_files(
    root: Path,
    prefix: PurePosixPath | None = None,
    *,
    ignored_paths: frozenset[PurePosixPath] = frozenset(),
):
    start = root if prefix is None else root.joinpath(*prefix.parts)
    if not start.exists():
        return
    if start.is_file() and not start.is_symlink():
        relative = PurePosixPath(start.relative_to(root).as_posix())
        if not is_ignored_artifact(relative) and relative not in ignored_paths:
            yield relative, start
        return
    if not start.is_dir() or start.is_symlink():
        return
    for directory, dirnames, filenames in os.walk(start, followlinks=False):
        directory_path = Path(directory)
        kept_directories: list[str] = []
        for name in sorted(dirnames):
            child = directory_path / name
            relative = PurePosixPath(child.relative_to(root).as_posix())
            if child.is_symlink() or is_ignored_artifact(relative):
                continue
            kept_directories.append(name)
        dirnames[:] = kept_directories
        for name in sorted(filenames):
            path = directory_path / name
            relative = PurePosixPath(path.relative_to(root).as_posix())
            if is_ignored_artifact(relative) or relative in ignored_paths:
                continue
            try:
                if path.is_file() and not path.is_symlink():
                    yield relative, path
            except OSError:
                continue


def reconcile_artifacts(
    local_root: Path,
    sync_root: Path,
    *,
    relative: str | Path | None = None,
    force_local_publish: bool = False,
) -> ReconcileResult:
    """Reconcile one tree or path with deterministic penguin-local precedence.

    The receive side is processed first only for locally missing paths.  The
    local tree is then authoritative for every existing path.  Deletions are
    intentionally not propagated in either direction.
    """

    local, sync = _validate_roots(local_root, sync_root)
    local.mkdir(parents=True, exist_ok=True)
    sync.mkdir(parents=True, exist_ok=True)
    prefix = _relative_path(relative) if relative is not None else None
    result = ReconcileResult()
    ignored_paths = cold_ignored_artifacts(sync)
    if prefix is not None and prefix in ignored_paths:
        result.ignored += 1
        return result

    for relpath, incoming in _iter_regular_files(
        sync, prefix, ignored_paths=ignored_paths
    ):
        destination = local.joinpath(*relpath.parts)
        if os.path.lexists(destination):
            continue
        if _atomic_link_or_copy(incoming, destination, replace=False):
            result.incoming_added += 1

    for relpath, authoritative in _iter_regular_files(
        local, prefix, ignored_paths=ignored_paths
    ):
        replica = sync.joinpath(*relpath.parts)
        if _same_inode(authoritative, replica):
            if not force_local_publish:
                result.already_linked += 1
                continue
        existed = os.path.lexists(replica)
        if existed and (replica.is_symlink() or not replica.is_file()):
            result.unsupported += 1
            continue
        if _atomic_link_or_copy(authoritative, replica, replace=True):
            result.local_published += 1
            if existed:
                result.local_conflicts_won += 1

    return result


def atomic_write_status(path: Path, payload: dict[str, object]) -> None:
    """Write daemon status without exposing a partial JSON document."""

    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
