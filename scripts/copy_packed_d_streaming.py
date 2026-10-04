#!/usr/bin/env python3
"""Safely stream the D raw archive into its D-backed ext4 staging store.

Unlike rsync's source mapping on DrvFs, this uses bounded read/write buffers.
Existing stage objects are verified in place; no cold source is modified.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import uuid

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.verify_packed_d_stage import (  # noqa: E402
    STAGE_ROOT, STATE_DIR, guard_stage, verify_one,
)
from stockagent.data_sync.desync_snapshots import (  # noqa: E402
    SnapshotError, atomic_write_json, sha256_file,
)
from stockagent.data_sync.packed_backup import (  # noqa: E402
    BackupConfig, EXPECTED_SUFFIX, object_descriptor, safe_path, signature,
)

SOURCE_ROOT = Path("/mnt/d/stockagent-backup/packed")
BUFFER_BYTES = 4 * 1024 * 1024
QUARANTINE_NAME = ".migration-quarantine"


def quarantine_bad_stage(
    connection: sqlite3.Connection, source: Path, target: Path,
    stage_root: Path, relative: str, expected_digest: str,
    source_before: tuple[int, ...],
) -> None:
    """Preserve a corrupt staging inode, only after the raw D source verifies."""
    target_before = signature(target)
    observed_digest = sha256_file(target)
    if signature(target) != target_before or observed_digest == expected_digest:
        raise SnapshotError(f"staged object changed during verification: {relative}")
    if sha256_file(source) != expected_digest or signature(source) != source_before:
        raise SnapshotError(f"cannot repair stage; D raw source is invalid: {relative}")
    if target.lstat().st_nlink != 1:
        raise SnapshotError(f"staged object has unexpected hard links: {relative}")
    quarantine_dir = stage_root / QUARANTINE_NAME / Path(relative).parent
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine = quarantine_dir / f"{target.name}.{uuid.uuid4().hex}.corrupt"
    os.link(target, quarantine, follow_symlinks=False)
    receipt = {
        "relative": relative,
        "expected_sha256": expected_digest,
        "observed_sha256": observed_digest,
        "bytes": target_before[2],
        "stage_signature": list(target_before),
        "quarantine": str(quarantine),
        "quarantined_at": time.time(),
    }
    atomic_write_json(quarantine.with_suffix(quarantine.suffix + ".json"), receipt)
    after_link = signature(target)
    # Creating the quarantine hard link necessarily updates ctime and nlink.
    if after_link[:4] != target_before[:4] or target.lstat().st_nlink != 2:
        raise SnapshotError(f"staged object changed before quarantine: {relative}")
    target.unlink()
    connection.execute("DELETE FROM verified WHERE path=?", (relative,))
    print(json.dumps({"quarantined_corrupt_stage": receipt}, ensure_ascii=False), flush=True)


def copy_one(
    connection: sqlite3.Connection, source_root: Path, stage_root: Path,
    item: dict[str, object],
) -> tuple[bool, int]:
    relative = str(item["relative"])
    digest = str(item["sha256"])
    source = safe_path(source_root, relative)
    target = safe_path(stage_root, relative)
    before = signature(source)
    expected_size = int(item.get("bytes", before[2]))
    if before[2] != expected_size:
        raise SnapshotError(f"source object changed size: {relative}")
    if target.exists() or target.is_symlink():
        try:
            verify_one(connection, stage_root, {**item, "bytes": expected_size})
            return False, expected_size
        except SnapshotError:
            quarantine_bad_stage(
                connection, source, target, stage_root, relative, digest, before,
            )

    target.parent.mkdir(parents=True, exist_ok=True)
    source_info = source.lstat()
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=target.parent, prefix=".stockagent-copy-",
            suffix=".tmp", delete=False,
        ) as output:
            temporary = Path(output.name)
            running = hashlib.sha256()
            copied = 0
            descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino, opened.st_size,
                    opened.st_mtime_ns, opened.st_ctime_ns) != before:
                    raise SnapshotError(f"source changed before reading: {relative}")
                while block := os.read(descriptor, BUFFER_BYTES):
                    output.write(block)
                    running.update(block)
                    copied += len(block)
            finally:
                os.close(descriptor)
            if copied != expected_size or running.hexdigest() != digest:
                raise SnapshotError(f"source SHA-256 or size mismatch: {relative}")
            if signature(source) != before:
                raise SnapshotError(f"source changed while copying: {relative}")
            os.fchmod(output.fileno(), stat.S_IMODE(source_info.st_mode))
            os.utime(output.fileno(), ns=(source_info.st_atime_ns, source_info.st_mtime_ns))
            output.flush()
            os.fsync(output.fileno())
        if sha256_file(temporary) != digest:
            raise SnapshotError(f"D stage readback SHA-256 mismatch: {relative}")
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            verify_one(connection, stage_root, {**item, "bytes": expected_size})
            return False, expected_size
        directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        observed = signature(target)
        connection.execute(
            "INSERT INTO verified(path,digest,signature,checked_at) VALUES(?,?,?,?) "
            "ON CONFLICT(path) DO UPDATE SET digest=excluded.digest, "
            "signature=excluded.signature, checked_at=excluded.checked_at",
            (relative, digest, json.dumps(observed), time.time()),
        )
        return True, expected_size
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def refresh_metadata() -> None:
    for name in ("manifests", "heads", "head-history"):
        source = SOURCE_ROOT / name
        if not source.is_dir():
            continue
        target = STAGE_ROOT / name
        target.mkdir(parents=True, exist_ok=True)
        command = ["rsync", "-a", "--numeric-ids"]
        if name != "heads":
            command.append("--ignore-existing")
        command.extend((f"{source}/", f"{target}/"))
        subprocess.run(command, check=True)


def list_object_names(root: Path) -> list[dict[str, str]]:
    """List names without 9p stat storms; validate each path when it is used."""
    objects: list[dict[str, str]] = []
    for kind in EXPECTED_SUFFIX:
        parent = root / "objects" / kind
        if parent.is_symlink() or not parent.is_dir():
            raise SnapshotError(f"missing or unsafe D object directory: {parent}")
        for directory, subdirs, names in os.walk(parent, followlinks=False):
            for name in subdirs:
                if (Path(directory) / name).is_symlink():
                    raise SnapshotError(f"symlink in D object namespace: {directory}/{name}")
            for name in names:
                if name.startswith(".syncthing.") or name.endswith(".tmp"):
                    continue
                relative = (Path(directory) / name).relative_to(root).as_posix()
                objects.append({"relative": relative, "sha256": object_descriptor(relative)})
    return sorted(objects, key=lambda item: item["relative"])


def run() -> dict[str, object]:
    guard_stage()
    if SOURCE_ROOT.is_symlink() or (SOURCE_ROOT / "objects").is_symlink():
        raise SnapshotError("D raw archive root is a symlink")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with (STATE_DIR / "stage.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        backup = BackupConfig.load(REPO_ROOT / "configs/data_sync/packed_backup.json")
        if backup.destination != SOURCE_ROOT:
            raise SnapshotError("configured D archive destination changed")
        objects = list_object_names(SOURCE_ROOT)
        status_path = STATE_DIR / "copy-status.json"
        status: dict[str, object] = {
            "state": "copying", "objects_total": len(objects),
            "bytes_total": None,
            "objects_checked": 0, "bytes_checked": 0,
            "objects_copied": 0, "bytes_copied": 0,
            "source": str(SOURCE_ROOT), "stage": str(STAGE_ROOT),
            "started_at": time.time(),
        }
        atomic_write_json(status_path, status)
        connection = sqlite3.connect(STATE_DIR / "verified.sqlite3")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS verified (path TEXT PRIMARY KEY, "
            "digest TEXT NOT NULL, signature TEXT NOT NULL, checked_at REAL NOT NULL)"
        )
        try:
            for index, item in enumerate(objects, 1):
                copied, size = copy_one(connection, SOURCE_ROOT, STAGE_ROOT, item)
                status["objects_checked"] = index
                status["bytes_checked"] = int(status["bytes_checked"]) + size
                if copied:
                    status["objects_copied"] = int(status["objects_copied"]) + 1
                    status["bytes_copied"] = int(status["bytes_copied"]) + size
                if index % 16 == 0 or index == len(objects):
                    connection.commit()
                    atomic_write_json(status_path, status)
            refresh_metadata()
            status["state"] = "copied_and_hashed_present_objects"
            status["finished_at"] = time.time()
            atomic_write_json(status_path, status)
            return status
        except Exception as exc:
            connection.commit()
            status["state"] = "failed"
            status["error"] = str(exc)
            status["finished_at"] = time.time()
            atomic_write_json(status_path, status)
            raise
        finally:
            connection.close()


def main() -> int:
    try:
        result = run()
    except (OSError, ValueError, SnapshotError, subprocess.SubprocessError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
