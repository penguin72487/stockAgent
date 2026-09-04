"""Fail-closed cleanup for rebuildable compiler caches under disk pressure."""

from __future__ import annotations

import dataclasses
import hashlib
import os
import shutil
import stat
import time
from pathlib import Path
from typing import Any, Iterable


DEFAULT_PROTECTED_PROCESS_SUBSTRINGS = (
    "train.py",
    "torch.distributed.run",
    "torchrun",
    "torch/_inductor/compile_worker",
)


@dataclasses.dataclass(frozen=True)
class CacheFile:
    root: Path
    path: Path
    device: int
    inode: int
    size: int
    allocated_bytes: int
    atime_ns: int
    mtime_ns: int

    @property
    def last_used_ns(self) -> int:
        return max(self.atime_ns, self.mtime_ns)

    @property
    def signature(self) -> tuple[int, int, int, int, int]:
        return self.device, self.inode, self.size, self.atime_ns, self.mtime_ns


def _is_below(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def validate_cache_roots(
    roots: Iterable[Path], *, allowed_root: Path
) -> tuple[Path, ...]:
    """Resolve an explicit allowlist and reject broad, symlinked, or overlapping roots."""

    allowed = allowed_root.expanduser().resolve()
    resolved: list[Path] = []
    for raw in roots:
        path = raw.expanduser()
        if path.is_symlink():
            raise ValueError(f"cache root must not be a symlink: {path}")
        path = path.resolve(strict=False)
        if path == allowed or not _is_below(path, allowed):
            raise ValueError(f"cache root must be a strict child of {allowed}: {path}")
        if any(_is_below(path, prior) or _is_below(prior, path) for prior in resolved):
            raise ValueError(f"cache roots overlap: {path}")
        resolved.append(path)
    if not resolved:
        raise ValueError("at least one cache root is required")
    return tuple(resolved)


def _open_cache_files() -> set[Path]:
    """Return currently open or memory-mapped absolute paths visible in procfs."""

    result: set[Path] = set()
    own_pid = os.getpid()
    for process in Path("/proc").glob("[0-9]*"):
        try:
            if int(process.name) == own_pid:
                continue
        except ValueError:
            continue
        for descriptor in (process / "fd").glob("*"):
            try:
                target = Path(os.readlink(descriptor))
            except OSError:
                continue
            if target.is_absolute():
                result.add(Path(str(target).removesuffix(" (deleted)")))
        try:
            lines = (process / "maps").read_text(
                encoding="utf-8", errors="replace"
            ).splitlines()
        except OSError:
            continue
        for line in lines:
            fields = line.split(maxsplit=5)
            if len(fields) == 6 and fields[5].startswith("/"):
                result.add(Path(fields[5].removesuffix(" (deleted)")))
    return result


def protected_processes(
    patterns: Iterable[str] = DEFAULT_PROTECTED_PROCESS_SUBSTRINGS,
    *,
    proc_root: Path = Path("/proc"),
) -> list[dict[str, str | int]]:
    """Return bounded evidence for workloads whose future cache use must be preserved."""

    selected_patterns = tuple(pattern for pattern in patterns if pattern)
    if not selected_patterns:
        return []
    result: list[dict[str, str | int]] = []
    own_pid = os.getpid()
    for process in sorted(proc_root.glob("[0-9]*"), key=lambda path: int(path.name)):
        try:
            pid = int(process.name)
        except ValueError:
            continue
        if pid == own_pid:
            continue
        try:
            raw = (process / "cmdline").read_bytes()
        except OSError:
            continue
        command = raw.replace(b"\0", b" ").decode("utf-8", errors="replace").strip()
        if not command:
            continue
        matched = next((pattern for pattern in selected_patterns if pattern in command), None)
        if matched is None:
            continue
        result.append({"pid": pid, "matched": matched, "command": command[:500]})
        if len(result) >= 100:
            break
    return result


def _scan_cache_files(
    roots: tuple[Path, ...], *, cutoff_ns: int, open_paths: set[Path]
) -> tuple[list[CacheFile], dict[str, dict[str, int]]]:
    candidates: list[CacheFile] = []
    counters: dict[str, dict[str, int]] = {}
    excluded_names = {"LOCK", ".lock"}
    excluded_suffixes = {".lock", ".partial", ".tmp"}
    for root in roots:
        row = {
            "files": 0,
            "allocated_bytes": 0,
            "eligible_files": 0,
            "eligible_allocated_bytes": 0,
            "open_files": 0,
        }
        counters[str(root)] = row
        if not root.exists():
            continue
        root_device = root.stat().st_dev
        for directory, dirnames, filenames in os.walk(root, followlinks=False):
            directory_path = Path(directory)
            kept_dirs: list[str] = []
            for name in sorted(dirnames):
                path = directory_path / name
                try:
                    info = path.stat(follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISDIR(info.st_mode) and info.st_dev == root_device:
                    kept_dirs.append(name)
            dirnames[:] = kept_dirs
            for name in sorted(filenames):
                path = directory_path / name
                try:
                    info = path.stat(follow_symlinks=False)
                except OSError:
                    continue
                if not stat.S_ISREG(info.st_mode) or info.st_dev != root_device:
                    continue
                row["files"] += 1
                row["allocated_bytes"] += info.st_blocks * 512
                if path in open_paths:
                    row["open_files"] += 1
                    continue
                if (
                    name in excluded_names
                    or path.suffix.lower() in excluded_suffixes
                    or info.st_nlink != 1
                    or max(info.st_atime_ns, info.st_mtime_ns) > cutoff_ns
                ):
                    continue
                item = CacheFile(
                    root=root,
                    path=path,
                    device=info.st_dev,
                    inode=info.st_ino,
                    size=info.st_size,
                    allocated_bytes=info.st_blocks * 512,
                    atime_ns=info.st_atime_ns,
                    mtime_ns=info.st_mtime_ns,
                )
                candidates.append(item)
                row["eligible_files"] += 1
                row["eligible_allocated_bytes"] += item.allocated_bytes
    candidates.sort(key=lambda item: (item.last_used_ns, str(item.path)))
    return candidates, counters


def _filesystem_usage(path: Path) -> dict[str, float | int]:
    usage = shutil.disk_usage(path)
    return {
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "used_percent": usage.used * 100.0 / usage.total,
    }


def _selection_fingerprint(files: Iterable[CacheFile]) -> str:
    digest = hashlib.sha256()
    for item in files:
        relative = item.path.relative_to(item.root).as_posix()
        digest.update(
            (
                f"{item.root}\0{relative}\0{item.device}\0{item.inode}\0"
                f"{item.size}\0{item.allocated_bytes}\0{item.atime_ns}\0"
                f"{item.mtime_ns}\n"
            ).encode("utf-8", errors="surrogateescape")
        )
    return digest.hexdigest()


def maintain_rebuildable_caches(
    roots: Iterable[Path],
    *,
    allowed_root: Path,
    min_age_days: float = 14.0,
    high_watermark_percent: float = 95.0,
    target_percent: float = 92.0,
    apply: bool = False,
    force: bool = False,
    protected_process_substrings: Iterable[
        str
    ] = DEFAULT_PROTECTED_PROCESS_SUBSTRINGS,
    now_ns: int | None = None,
) -> dict[str, Any]:
    """Audit or prune old allowlisted cache files until the target watermark."""

    if min_age_days < 0:
        raise ValueError("min_age_days must be non-negative")
    if not 0 < target_percent < high_watermark_percent < 100:
        raise ValueError("require 0 < target < high watermark < 100")
    protected_patterns = tuple(
        pattern for pattern in protected_process_substrings if pattern
    )
    selected_roots = validate_cache_roots(roots, allowed_root=allowed_root)
    existing = next((root for root in selected_roots if root.exists()), allowed_root)
    before = _filesystem_usage(existing)
    under_pressure = float(before["used_percent"]) >= high_watermark_percent
    protected = protected_processes(protected_patterns)
    deferred_reason = (
        "protected-process-active" if protected and not force else None
    )
    scan_skipped_reason: str | None = None
    if apply and not force:
        if deferred_reason is not None:
            scan_skipped_reason = deferred_reason
        elif not under_pressure:
            scan_skipped_reason = "below-high-watermark"
    cutoff_ns = (time.time_ns() if now_ns is None else int(now_ns)) - int(
        min_age_days * 86_400 * 1_000_000_000
    )
    if scan_skipped_reason is None:
        candidates, per_root = _scan_cache_files(
            selected_roots, cutoff_ns=cutoff_ns, open_paths=_open_cache_files()
        )
    else:
        candidates, per_root = [], {}
    required_bytes = max(
        0,
        int(before["used_bytes"])
        - int(int(before["total_bytes"]) * target_percent / 100.0),
    )
    if force:
        required_bytes = sum(item.allocated_bytes for item in candidates)
    elif not under_pressure or deferred_reason is not None:
        required_bytes = 0
    selected: list[CacheFile] = []
    selected_bytes = 0
    for item in candidates:
        if selected_bytes >= required_bytes:
            break
        selected.append(item)
        selected_bytes += item.allocated_bytes

    deleted_files = 0
    deleted_allocated_bytes = 0
    skipped_changed = 0
    skipped_open = 0
    skipped_protected_start = 0
    errors: list[dict[str, str]] = []
    if apply and selected:
        open_paths = _open_cache_files()
        for index, item in enumerate(selected):
            if not force and index % 128 == 0:
                newly_protected = protected_processes(protected_patterns)
                if newly_protected:
                    protected = newly_protected
                    deferred_reason = "protected-process-started"
                    skipped_protected_start = len(selected) - index
                    break
            if item.path in open_paths:
                skipped_open += 1
                continue
            try:
                current = item.path.stat(follow_symlinks=False)
                signature = (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_atime_ns,
                    current.st_mtime_ns,
                )
                if not stat.S_ISREG(current.st_mode) or signature != item.signature:
                    skipped_changed += 1
                    continue
                item.path.unlink()
                deleted_files += 1
                deleted_allocated_bytes += item.allocated_bytes
            except FileNotFoundError:
                skipped_changed += 1
            except OSError as exc:
                errors.append({"path": str(item.path), "error": str(exc)})
                if len(errors) >= 50:
                    break
        for root in selected_roots:
            if not root.exists():
                continue
            directories = [Path(path) for path, _dirs, _files in os.walk(root)]
            for directory in sorted(directories, key=lambda path: len(path.parts), reverse=True):
                if directory == root:
                    continue
                try:
                    directory.rmdir()
                except OSError:
                    pass
    after = _filesystem_usage(existing)
    return {
        "schema_version": 1,
        "policy": {
            "allowed_root": str(allowed_root.expanduser().resolve()),
            "cache_roots": [str(root) for root in selected_roots],
            "min_age_days": min_age_days,
            "high_watermark_percent": high_watermark_percent,
            "target_percent": target_percent,
            "force": force,
            "protected_process_substrings": list(protected_patterns),
        },
        "apply": apply,
        "under_pressure": under_pressure,
        "inventory_complete": scan_skipped_reason is None,
        "scan_skipped_reason": scan_skipped_reason,
        "deferred_reason": deferred_reason,
        "protected_processes": protected,
        "filesystem_before": before,
        "filesystem_after": after,
        "per_root": per_root,
        "eligible_files": len(candidates),
        "eligible_allocated_bytes": sum(item.allocated_bytes for item in candidates),
        "selected_files": len(selected),
        "selected_allocated_bytes": selected_bytes,
        "selected_fingerprint_sha256": _selection_fingerprint(selected),
        "deleted_files": deleted_files,
        "deleted_allocated_bytes": deleted_allocated_bytes,
        "skipped_changed": skipped_changed,
        "skipped_open": skipped_open,
        "skipped_protected_start": skipped_protected_start,
        "errors": errors,
    }
