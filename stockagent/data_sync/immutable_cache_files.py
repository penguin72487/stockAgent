"""Publish derived cache names without copying an existing immutable payload.

The canonical producer owns its destination lock and the donor's writer lock.
This helper creates no object pool and never changes donor bytes/permissions.
Every generation owns its own hardlink, so normal generation retirement still
works without a second garbage collector.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
import uuid

from stockagent.data_sync.desync_snapshots import _fsync_directory


def payload_info(path: Path) -> dict:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"not a regular cache file: {path}")
    attrs = {name: os.getxattr(path, name, follow_symlinks=False).hex()
             for name in sorted(os.listxattr(path, follow_symlinks=False))}
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "allocated": info.st_blocks * 512, "mtime_ns": info.st_mtime_ns,
            "ctime_ns": info.st_ctime_ns, "mode": stat.S_IMODE(info.st_mode),
            "uid": info.st_uid, "gid": info.st_gid, "nlink": info.st_nlink,
            "xattrs": attrs}


def stable_payload_hash(path: Path, expected: dict) -> str:
    if payload_info(path) != expected:
        raise ValueError(f"cache changed before hashing: {path}")
    digest = hashlib.sha256()
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as stream:
        try:
            # A streaming integrity pass should not promote tens of GiB above
            # the service working set. The advisory hint is not a safety gate.
            os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_NOREUSE)
        except (AttributeError, OSError):
            pass
        opened = os.fstat(stream.fileno())
        if (opened.st_dev, opened.st_ino) != (expected["device"], expected["inode"]):
            raise ValueError(f"cache changed while opening: {path}")
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            digest.update(block)
    if payload_info(path) != expected:
        raise ValueError(f"cache changed during hashing: {path}")
    return digest.hexdigest()


def link_immutable_payload(source: Path, destination: Path, *, sha256: str, size: int) -> bool:
    """Link only a same-device, owned, readonly regular payload with exact SHA.

    Busy/changed/malformed donors are cache misses. I/O failures during the
    actual publication propagate; the producer must not report a ready cache.
    Caller serialization, rather than a new global pool lock, owns coherence.
    """
    source, destination = source.absolute(), destination.absolute()
    try:
        if source.resolve(strict=True) != source or source == destination:
            return False
        before = payload_info(source)
        if (before["device"] != destination.parent.stat().st_dev
                or before["uid"] != os.geteuid() or before["gid"] != os.getegid()
                or before["mode"] & ~0o444 or not before["mode"] & 0o400
                or before["size"] != size or before["xattrs"]
                or stable_payload_hash(source, before) != sha256):
            return False
    except (FileNotFoundError, ValueError):
        return False
    temporary = destination.with_name(f".{destination.name}.reuse-{uuid.uuid4().hex}.tmp")
    try:
        os.link(source, temporary, follow_symlinks=False)
        linked = payload_info(temporary)
        if any(linked[field] != before[field] for field in (
                "device", "inode", "size", "mtime_ns", "mode", "uid", "gid", "xattrs")):
            return False
        if stable_payload_hash(temporary, linked) != sha256:
            return False
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return True
    finally:
        temporary.unlink(missing_ok=True)

