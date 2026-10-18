"""Byte admission for the flat completed-forward namespace, not cache eviction.

The canonical producer must hold its key lock and keep this context around the
ENTIRE miss/restore transaction, including serialization and durable commit.
The namespace lock only serializes admission/reaping, never model or cold I/O.
Reservations use kernel leases: a crash cannot leave a live reservation behind.
Unknown data is counted and retained; no payload, receipt or pin is removed.

Control records need atomic visibility, NOT fsync: a same-boot process crash
leaves a reapable record, while a power loss releases every kernel lease. A
missing record after reboot grants no old work new credit; admission counts
actual files again. Payload/receipt durability remains the producer's duty.
"""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Iterator
import uuid

from downloader.artifact_io import atomic_write_bytes
from stockagent.data_sync.desync_snapshots import SnapshotError

CONTRACT = "completed_forward_capacity_reservation_v1"
MAX_NAMES = 4 * 4096 + 32
MAX_RECORD_BYTES = 4096
CONTROL_HEADROOM_BYTES = 128 * 1024
_KEY = re.compile(r"[0-9a-f]{64}\Z")
_LEASE = re.compile(r"\.([0-9a-f]{64})\.capacity-([0-9a-f]{32})\.json\Z")
_LOCK = ".namespace-capacity.lock"


class CapacityDeferred(SnapshotError):
    """No allocation was admitted; retain existing cache and retry its owner."""

    def __init__(self, reason: str, **fields) -> None:
        self.details = {"contract": CONTRACT, "reason": reason,
                        "retryable": True, "allocation_admitted": False, **fields}
        super().__init__(f"completed-forward capacity deferred: {reason}")


def _positive(value: int, name: str) -> None:
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError(f"{name} must be a positive integer byte count")


def _timeout(value: float) -> None:
    if type(value) not in (float, int) or not 0 <= value <= 60:
        raise ValueError("lock timeout must be finite and between zero and 60 seconds")


def _generation(info) -> tuple:
    return (info.st_dev, info.st_ino, info.st_size, info.st_blocks,
            info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_nlink,
            info.st_uid, info.st_gid)


def _root_info(root: Path):
    info = root.lstat()
    if (root.resolve(strict=True) != root or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.geteuid() or info.st_mode & 0o022):
        raise CapacityDeferred("untrusted_namespace")
    return info


def _same_root(root: Path, before) -> None:
    after = _root_info(root)
    if (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino):
        raise CapacityDeferred("namespace_replaced")


@contextmanager
def _namespace_lock(root: Path, *, timeout_seconds: float) -> Iterator[None]:
    _timeout(timeout_seconds)
    descriptor = os.open(root / _LOCK, os.O_CREAT | os.O_RDWR | os.O_CLOEXEC |
                         os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or info.st_size != 0):
            raise CapacityDeferred("untrusted_namespace_lock")
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise CapacityDeferred("namespace_lock_busy") from None
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        if _generation((root / _LOCK).lstat()) != _generation(info):
            raise CapacityDeferred("namespace_lock_replaced")
        yield
    finally:
        os.close(descriptor)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate reservation field")
        result[key] = value
    return result


def _read_reservation(descriptor: int, path: Path, root_info) -> dict:
    before = os.fstat(descriptor)
    if (not stat.S_ISREG(before.st_mode) or before.st_size > MAX_RECORD_BYTES
            or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
            or before.st_uid != os.geteuid() or before.st_gid != os.getegid()):
        raise CapacityDeferred("untrusted_reservation", name=path.name)
    os.lseek(descriptor, 0, os.SEEK_SET)
    raw = os.read(descriptor, MAX_RECORD_BYTES + 1)
    if (len(raw) != before.st_size or _generation(os.fstat(descriptor)) != _generation(before)
            or _generation(path.lstat()) != _generation(before)):
        raise CapacityDeferred("reservation_changed", name=path.name)
    try:
        record = json.loads(raw, object_pairs_hook=_unique)
        match = _LEASE.fullmatch(path.name)
        fields = {"contract", "key", "token", "capacity_bytes", "maximum_growth_bytes", "control_bytes",
                  "root_device", "root_inode"}
        if (not isinstance(record, dict) or set(record) != fields or match is None
                or record["contract"] != CONTRACT or record["key"] != match[1]
                or record["token"] != match[2]
                or type(record["root_device"]) is not int or type(record["root_inode"]) is not int
                or record["root_device"] != root_info.st_dev or record["root_inode"] != root_info.st_ino
                or type(record["control_bytes"]) is not int
                or record["control_bytes"] != CONTROL_HEADROOM_BYTES):
            raise ValueError("reservation identity mismatch")
        _positive(record["maximum_growth_bytes"], "maximum_growth_bytes")
        _positive(record["capacity_bytes"], "capacity_bytes")
        if record["maximum_growth_bytes"] + record["control_bytes"] > record["capacity_bytes"]:
            raise ValueError("reservation exceeds its admitted policy")
    except (ValueError, TypeError, RecursionError) as error:
        raise CapacityDeferred("invalid_reservation", name=path.name) from error
    return record


def _unlink_own_record(root: Path, root_info, path: Path, descriptor: int) -> None:
    _same_root(root, root_info)
    _read_reservation(descriptor, path, root_info)
    path.unlink()


def _observe_locked(root: Path, root_info, *, reap: bool) -> dict:
    """Count physical allocations once. Only unlocked VALID control records reap."""
    _same_root(root, root_info)
    names = []
    with os.scandir(root) as entries:
        for entry in entries:
            names.append(entry.name)
            if len(names) > MAX_NAMES:
                raise CapacityDeferred("namespace_inventory_bound")
    names.sort()
    allocations, active, stale = {}, [], []
    for name in names:
        path = root / name
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_dev != root_info.st_dev):
            raise CapacityDeferred("unmeasured_namespace_entry", name=name)
        allocations[(info.st_dev, info.st_ino)] = info.st_blocks * 512
        if _LEASE.fullmatch(name) is None:
            continue
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            record = _read_reservation(descriptor, path, root_info)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                active.append(record)
                continue
            stale.append(record)
            if reap:
                _unlink_own_record(root, root_info, path, descriptor)
                allocations.pop((info.st_dev, info.st_ino))
        finally:
            os.close(descriptor)
    _same_root(root, root_info)
    # Directory blocks belong to this namespace too. st_blocks, not logical
    # size/number of names, owns accounting; outside hardlinks earn no credit.
    allocations[(root_info.st_dev, root_info.st_ino)] = root.stat().st_blocks * 512
    return {"contract": CONTRACT, "namespace_allocated_bytes": sum(allocations.values()),
            "physical_inodes": len(allocations), "active_reservations": active,
            "reserved_growth_bytes": sum(r["maximum_growth_bytes"] + r["control_bytes"] for r in active),
            "stale_reservations": stale, "stale_reaped": len(stale) if reap else 0,
            "payload_bytes_read": 0, "data_paths_removed": 0,
            "scope": "flat namespace allocations and cooperating writer leases; not cold or consumer proof"}


def capacity_status(root: Path, *, timeout_seconds: float = 2.0) -> dict:
    """Observe without reaping. This is not permission to retire any data."""
    root = root.absolute()
    info = _root_info(root)
    with _namespace_lock(root, timeout_seconds=timeout_seconds):
        return _observe_locked(root, info, reap=False)


@contextmanager
def reserve_capacity(root: Path, *, key: str, maximum_growth_bytes: int,
                     capacity_bytes: int, timeout_seconds: float = 2.0,
                     minimum_free_bytes: int = 0) -> Iterator[dict]:
    """Reserve an upper bound BEFORE compute; release after durable publication.

    Existing hits do not need admission. Callers supply a proven upper bound
    for all their transaction's payload/temp/receipt/usage growth. We reserve
    extra control headroom. No directory recursion, payload hashing or eviction
    occurs. All namespace writers must adopt this before enrolling a hard cap.
    """
    if not isinstance(key, str) or _KEY.fullmatch(key) is None:
        raise ValueError("invalid canonical forward key")
    _positive(maximum_growth_bytes, "maximum_growth_bytes")
    _positive(capacity_bytes, "capacity_bytes")
    if type(minimum_free_bytes) is not int or not 0 <= minimum_free_bytes < 2**63:
        raise ValueError("minimum_free_bytes must be a nonnegative integer byte count")
    _timeout(timeout_seconds)
    root = root.absolute()
    root_info = _root_info(root)
    token = uuid.uuid4().hex
    path = root / f".{key}.capacity-{token}.json"
    descriptor = None
    result = {}
    try:
        with _namespace_lock(root, timeout_seconds=timeout_seconds):
            observed = _observe_locked(root, root_info, reap=True)
            if any(row["capacity_bytes"] != capacity_bytes for row in observed["active_reservations"]):
                raise CapacityDeferred("capacity_policy_changed_while_busy",
                                       capacity_bytes=capacity_bytes)
            required = (observed["namespace_allocated_bytes"] + observed["reserved_growth_bytes"]
                        + maximum_growth_bytes + CONTROL_HEADROOM_BYTES)
            if minimum_free_bytes:
                filesystem = os.statvfs(root)
                available = filesystem.f_bavail * filesystem.f_frsize
                growth = observed["reserved_growth_bytes"] + maximum_growth_bytes + CONTROL_HEADROOM_BYTES
                if available < growth + minimum_free_bytes:
                    raise CapacityDeferred("filesystem_headroom_exhausted", available_bytes=available,
                                           reserved_and_requested_growth_bytes=growth,
                                           minimum_free_bytes=minimum_free_bytes)
            if required > capacity_bytes:
                raise CapacityDeferred("capacity_exhausted", capacity_bytes=capacity_bytes,
                                       maximum_growth_bytes=maximum_growth_bytes,
                                       namespace_allocated_bytes=observed["namespace_allocated_bytes"],
                                       reserved_growth_bytes=observed["reserved_growth_bytes"],
                                       required_bytes=required)
            record = {"contract": CONTRACT, "key": key, "token": token, "capacity_bytes": capacity_bytes,
                      "maximum_growth_bytes": maximum_growth_bytes,
                      "control_bytes": CONTROL_HEADROOM_BYTES,
                      "root_device": root_info.st_dev, "root_inode": root_info.st_ino}
            raw = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            if len(raw) > MAX_RECORD_BYTES:
                raise ValueError("reservation record exceeds its control bound")
            atomic_write_bytes(path, raw, durable=False, mode=0o600)
            descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _same_root(root, root_info)
            _read_reservation(descriptor, path, root_info)
            result = {"contract": CONTRACT, "key": key, "state": "reserved",
                      "capacity_bytes": capacity_bytes, "maximum_growth_bytes": maximum_growth_bytes,
                      "minimum_free_bytes": minimum_free_bytes,
                      "control_bytes": CONTROL_HEADROOM_BYTES, "required_bytes_at_admission": required,
                      "namespace_allocated_bytes_at_admission": observed["namespace_allocated_bytes"],
                      "other_reserved_growth_bytes": observed["reserved_growth_bytes"],
                      "stale_reaped": observed["stale_reaped"], "data_paths_removed": 0}
        yield result
    finally:
        if descriptor is not None:
            # A cleanup failure must not discard a valid committed result.
            # Closing the FD makes only this record safely reapable next time.
            try:
                with _namespace_lock(root, timeout_seconds=timeout_seconds):
                    _unlink_own_record(root, root_info, path, descriptor)
                result["state"] = "released"
            except (OSError, SnapshotError, ValueError) as error:
                result["state"] = "released_lease_cleanup_unconfirmed"
                result["release_error_type"] = type(error).__name__
            finally:
                os.close(descriptor)
