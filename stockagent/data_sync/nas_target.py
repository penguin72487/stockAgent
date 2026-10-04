"""Bound NAS writes to the authorized SMB mount and exact user directory."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import time
import uuid

from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_backup import mounted_volume, safe_path


class NasTarget:
    def __init__(self, configuration: Path):
        raw = json.loads(configuration.read_bytes())
        if raw.get("schema_version") != 1 or raw.get("automatic_pruning") is not False:
            raise SnapshotError("unsupported NAS configuration or automatic pruning enabled")
        self.nas = raw["nas"]
        self.mount_point = Path(self.nas["mount_point"])
        self.reserve = raw["reserve_bytes"]
        if (not self.mount_point.is_absolute() or self.mount_point == Path("/")
                or ".." in self.mount_point.parts or type(self.reserve) is not int or self.reserve < 0):
            raise SnapshotError("invalid NAS mount or free-space reserve")
        for value in (self.nas["host"], self.nas["share"], self.nas["repository_directory"]):
            if not isinstance(value, str) or not value or any(c in value for c in "/\\\r\n\0") or value in {".", ".."}:
                raise SnapshotError("invalid NAS host/share/repository component")
        relative = PurePosixPath(self.nas["relative_directory"])
        if (relative.is_absolute() or not relative.parts or ".." in relative.parts
                or any(c in str(relative) for c in "\\\r\n\0")):
            raise SnapshotError("invalid NAS user directory")
        self.relative = relative.as_posix()
        self.source = f"//{self.nas['host']}/{self.nas['share']}"

    @property
    def user_directory(self) -> Path:
        return safe_path(self.mount_point, self.relative)

    @property
    def repository(self) -> Path:
        return safe_path(self.mount_point, self.relative + "/" + self.nas["repository_directory"])

    def check(self, *, additional_bytes: int = 0) -> dict:
        if os.name != "posix":
            raise SnapshotError("Linux NAS target guard requires a verified CIFS mount")
        if any(p.is_symlink() for p in (self.mount_point, *self.mount_point.parents)):
            raise SnapshotError("NAS mount path is redirected")
        mount_id, fs_type, source = mounted_volume(self.mount_point)
        if fs_type != "cifs" or source.casefold() != self.source.casefold():
            raise SnapshotError("NAS must be the exact configured SMB server/share, never a local fallback")
        directory = self.user_directory
        if not directory.is_dir():
            raise SnapshotError("the authorized NAS user directory is unavailable")
        free = shutil.disk_usage(directory).free
        if free < self.reserve + additional_bytes:
            raise SnapshotError("NAS capacity is below the required reserve")
        return {"mount_id": mount_id, "filesystem": fs_type, "source": source,
                "user_directory": str(directory), "repository": str(self.repository),
                "free_bytes": free, "reserve_bytes": self.reserve,
                "authenticated_mount_observed": True, "durable_off_host_backup_verified": False}

    def unchanged(self, observed: dict) -> dict:
        current = self.check()
        if any(current[k] != observed[k] for k in ("mount_id", "filesystem", "source", "user_directory", "repository")):
            raise SnapshotError("NAS mount identity changed during the operation")
        return current

    def probe(self, *, size_mib: int = 32, repeats: int = 3) -> dict:
        if type(size_mib) is not int or not 1 <= size_mib <= 256 or not 1 <= repeats <= 5:
            raise SnapshotError("NAS probe limits are out of bounds")
        before = self.check(additional_bytes=size_mib * 1024**2)
        directory = self.user_directory / ("stockagent-probe-" + uuid.uuid4().hex)
        directory.mkdir(mode=0o700)
        files = []
        samples = []
        try:
            for number in range(repeats):
                self.unchanged(before)
                path = directory / f"probe-{number}.bin"
                expected = hashlib.sha256()
                started = time.perf_counter()
                with path.open("xb") as stream:
                    files.append(path)
                    for _ in range(size_mib):
                        chunk = os.urandom(1024**2)
                        expected.update(chunk)
                        stream.write(chunk)
                    stream.flush()
                    os.fsync(stream.fileno())
                written = time.perf_counter() - started
                actual = hashlib.sha256()
                started = time.perf_counter()
                with path.open("rb") as stream:
                    for chunk in iter(lambda: stream.read(1024**2), b""):
                        actual.update(chunk)
                read = time.perf_counter() - started
                if path.stat().st_size != size_mib * 1024**2 or expected.digest() != actual.digest():
                    raise SnapshotError("NAS probe content differs")
                samples.append({"round": number, "bytes": size_mib * 1024**2,
                                "write_seconds": written, "read_seconds": read,
                                "sha256": actual.hexdigest(), "verified": True})
            after = self.unchanged(before)
        finally:
            # Remove only this invocation's disposable probes. Refuse unknown
            # children and a lost/remounted NAS; never recurse through a tree.
            self.unchanged(before)
            for path in files:
                path.unlink()
            directory.rmdir()
        return {"state": "nas_write_read_probe_verified", "before": before, "after": after,
                "samples": samples, "durable_off_host_backup_verified": False,
                "scope": "fresh random files, flushed writes and same-client readback; client cache possible, no cold restore proof"}
