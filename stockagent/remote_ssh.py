"""Lightweight shared SSH transport validation; no model/training imports."""
from pathlib import Path, PurePosixPath
import re
from stockagent.data_sync.desync_snapshots import SnapshotError

SSH_TARGET = re.compile(r"[A-Za-z0-9_.@:-]+")
REMOTE_ABSOLUTE = re.compile(r"/[A-Za-z0-9_./-]+")


def validate_ssh_target(value: str) -> str:
    target = str(value).strip()
    if not target or not SSH_TARGET.fullmatch(target) or target.startswith("-"):
        raise SnapshotError(f"unsafe SSH target: {value!r}")
    return target


def validate_remote_absolute(value: str, label: str) -> str:
    path = str(value).rstrip("/")
    pure = PurePosixPath(path)
    if not pure.is_absolute() or ".." in pure.parts or not REMOTE_ABSOLUTE.fullmatch(path):
        raise SnapshotError(f"unsafe {label}: {value!r}")
    return path


def ssh_base(identity_file: Path, port: int) -> list[str]:
    identity = identity_file.resolve()
    if not identity.is_file() or identity.is_symlink():
        raise SnapshotError(f"SSH identity is not a regular file: {identity}")
    if identity.stat().st_mode & 0o077:
        raise SnapshotError(f"SSH identity permissions must be 0600 or stricter: {identity}")
    if not 1 <= int(port) <= 65535:
        raise SnapshotError(f"invalid SSH port: {port}")
    return ["ssh", "-i", str(identity), "-p", str(int(port)), "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "ConnectTimeout=15"]
