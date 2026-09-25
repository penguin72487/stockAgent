#!/usr/bin/env python3
"""Read-only, current-head D: metadata path-check A/B benchmark.

This does not copy, publish, prune, or update backup status. Every selected
destination file is read in full and the variants must agree byte-for-byte.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import time

from stockagent.data_sync.packed_backup import (
    BackupConfig,
    VolumeGuard,
    read_existing_metadata_nofollow,
    safe_path,
)
from stockagent.data_sync.desync_snapshots import SnapshotError


REPO_ROOT = Path(__file__).resolve().parents[1]


class _PinnedMetadataReader:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        root_fd = os.open(root, self.flags)
        self.directories: dict[tuple[str, ...], tuple[int, tuple[int, int]]] = {
            (): (root_fd, self._identity(os.fstat(root_fd)))
        }

    @staticmethod
    def _identity(info: os.stat_result) -> tuple[int, int]:
        return (info.st_dev, info.st_ino)

    def read(self, relative: str) -> bytes:
        path = Path(relative)
        if path.is_absolute() or not path.parts or any(
            part in {".", ".."} for part in path.parts
        ):
            raise SnapshotError(f"unsafe metadata relative path: {relative}")
        parts = tuple(path.parts)
        for length in range(1, len(parts)):
            prefix = parts[:length]
            if prefix in self.directories:
                continue
            parent_fd = self.directories[parts[:length - 1]][0]
            fd = os.open(parts[length - 1], self.flags, dir_fd=parent_fd)
            self.directories[prefix] = (fd, self._identity(os.fstat(fd)))
        parent_fd = self.directories[parts[:-1]][0]
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
        with os.fdopen(fd, "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise SnapshotError(f"backup metadata is not a regular file: {relative}")
            return handle.read()

    def recheck(self) -> None:
        for parts, (_fd, identity) in self.directories.items():
            path = self.root.joinpath(*parts)
            info = os.stat(path, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or self._identity(info) != identity:
                raise SnapshotError(f"backup metadata directory changed: {path}")

    def close(self) -> None:
        for fd, _identity in self.directories.values():
            os.close(fd)
        self.directories.clear()


def _current_metadata_paths(config: BackupConfig) -> list[str]:
    relatives: set[str] = set()
    for head_path in sorted((config.source / "heads").glob("*/*.json")):
        head_relative = head_path.relative_to(config.source).as_posix()
        raw = json.loads(safe_path(config.source, head_relative).read_bytes())
        manifest_relative = str(raw["manifest_relpath"])
        safe_path(config.source, manifest_relative)
        relatives.add(head_relative)
        relatives.add(manifest_relative)
    return sorted(relatives)


def _run_variant(root: Path, paths: list[str], *, variant: str) -> dict[str, object]:
    digest = hashlib.sha256()
    byte_count = 0
    started = time.perf_counter()
    reader = _PinnedMetadataReader(root) if variant == "pinned_reused" else None
    try:
        for relative in paths:
            if variant == "pinned_per_file":
                raw = read_existing_metadata_nofollow(root, relative)
            elif reader is not None:
                raw = reader.read(relative)
            else:
                raw = safe_path(root, relative).read_bytes()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            digest.update(hashlib.sha256(raw).digest())
            byte_count += len(raw)
        if reader is not None:
            reader.recheck()
    finally:
        if reader is not None:
            reader.close()
    return {
        "variant": variant,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "files": len(paths),
        "bytes_read": byte_count,
        "digest_sha256": digest.hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path,
        default=REPO_ROOT / "configs/data_sync/packed_backup.json",
    )
    args = parser.parse_args()
    config = BackupConfig.load(args.config)
    if config.backup_scope != "current_heads":
        parser.error("this benchmark requires current_heads scope")
    VolumeGuard(config).check()
    paths = _current_metadata_paths(config)
    if not paths:
        raise SnapshotError("no current metadata paths")
    results = [
        _run_variant(config.destination, paths, variant=variant)
        for variant in (
            "existing_safe_path", "pinned_per_file", "pinned_reused",
            "pinned_reused", "pinned_per_file", "existing_safe_path",
        )
    ]
    if len({str(result["digest_sha256"]) for result in results}) != 1:
        raise SnapshotError("metadata changed or benchmark variants disagree")
    print(json.dumps({
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": config.backup_scope,
        "claim_boundary": (
            "Current-head metadata path validation and read only. Symmetric "
            "A-B-C-C-B-A order "
            "reduces but does not remove DrvFs cache and concurrent I/O bias; "
            "not a backup service wall-time or restoration proof."
        ),
        "results": results,
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
