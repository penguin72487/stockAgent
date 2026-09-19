"""One-way, additive, checksum-verified backup of an existing packed store.

No materialization, deletion propagation, node identity copying, publication,
or peer access. D: keeps the canonical object format so the existing verifier
and fetch commands can restore it without a second archive implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import time
from typing import Any

from stockagent.data_sync.desync_snapshots import (
    SnapshotError, _fsync_directory, atomic_write_bytes, atomic_write_json,
)
from stockagent.data_sync.packed_snapshots import (
    PACKED_HEAD_SCHEMA_VERSION, _validate_manifest, resolve_packed_snapshot_id,
)

CHUNK_BYTES = 8 * 1024 * 1024
OBJECT_PATH = re.compile(r"objects/(blobs|packs|inventories)/([0-9a-f]{2})/([0-9a-f]{64})\.(blob|zip|jsonl\.gz)$")
EXPECTED_SUFFIX = {"blobs": "blob", "packs": "zip", "inventories": "jsonl.gz"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def signature(path: Path) -> tuple[int, ...]:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise SnapshotError(f"not a regular file: {path}")
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def same_file_signature(recorded: object, observed: tuple[int, ...]) -> bool:
    """Compare a receipt across a WSL remount without trusting ``st_dev``.

    DrvFs may assign the same enrolled D: volume a new Linux device number at
    boot.  The volume guard checks the mount and enrollment separately; inode,
    size, mtime and ctime must still match the checksum-readback receipt.
    """
    return (
        isinstance(recorded, list)
        and len(recorded) == 5
        and all(type(value) is int for value in recorded)
        and len(observed) == 5
        and recorded[1:] == list(observed)[1:]
    )


def safe_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or any(part in {".", ".."} for part in path.parts):
        raise SnapshotError(f"unsafe backup relative path: {relative}")
    target = root / path
    current = root
    for part in path.parts:
        current /= part
        if current.is_symlink():
            raise SnapshotError(f"symlink is forbidden in backup paths: {current}")
    if target.resolve(strict=False) != target.absolute():
        raise SnapshotError(f"backup path is redirected: {target}")
    return target


def mounted_volume(mount: Path) -> tuple[str, str, str]:
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        fields = line.split()
        if fields[4] == str(mount):
            separator = fields.index("-")
            source = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[separator + 2])
            return fields[0], fields[separator + 1], source
    raise SnapshotError(f"backup drive is not mounted: {mount}")


@dataclass(frozen=True)
class BackupConfig:
    source: Path
    destination: Path
    mount_point: Path
    mount_source: str
    volume_id: str
    state_dir: Path
    authority_node_id: str = "penguin"
    reserve_bytes: int = 20 * 1024**3
    batch_objects: int = 32
    poll_seconds: float = 30
    checksum_recheck_days: float = 30

    @classmethod
    def load(cls, path: Path) -> "BackupConfig":
        raw = json.loads(path.read_text())
        if raw.pop("schema_version", None) != 1:
            raise SnapshotError("unsupported packed backup configuration")
        for key in ("source", "destination", "mount_point", "state_dir"):
            raw[key] = Path(raw[key])
        result = cls(**raw)
        if result.batch_objects < 1 or result.poll_seconds <= 0 or result.reserve_bytes < 0 or result.checksum_recheck_days <= 0:
            raise SnapshotError("invalid backup limits")
        return result


class VolumeGuard:
    """Fail closed if D: disappeared, changed mount, or points back to C:."""

    def __init__(self, config: BackupConfig):
        self.config = config
        self.mount_identity: tuple[str, str, str] | None = None
        self.marker_path = config.destination.parent / "backup-volume.json"

    def check(self, *, require_marker: bool = True) -> None:
        cfg = self.config
        for path in (cfg.source, cfg.destination, cfg.mount_point, cfg.state_dir):
            if not path.is_absolute() or path.resolve(strict=False) != path:
                raise SnapshotError(f"backup requires explicit non-aliased paths: {path}")
        if cfg.source == cfg.destination or cfg.source in cfg.destination.parents or cfg.destination in cfg.source.parents:
            raise SnapshotError("source and backup must not overlap")
        if cfg.mount_point not in cfg.destination.parents or len(cfg.destination.relative_to(cfg.mount_point).parts) < 2:
            raise SnapshotError("backup must use a dedicated directory below its volume")
        if any(root == cfg.state_dir or root in cfg.state_dir.parents for root in (cfg.source, cfg.destination)):
            raise SnapshotError("backup state must not be stored inside source or backup")
        identity = mounted_volume(cfg.mount_point)
        if identity[2] != cfg.mount_source or identity[1] not in {"9p", "drvfs"}:
            raise SnapshotError(f"unexpected backup mount: {identity}")
        if self.mount_identity is not None and identity != self.mount_identity:
            raise SnapshotError("backup volume remounted during operation; retry in a new pass")
        if cfg.source.stat().st_dev == cfg.mount_point.stat().st_dev:
            raise SnapshotError("backup is on the source filesystem")
        if (cfg.source / ".local-state/node-id").read_text().strip() != cfg.authority_node_id:
            raise SnapshotError("backup source is not the configured authority node")
        for relative in ("objects/blobs", "objects/packs", "objects/inventories", "heads", "manifests"):
            if not safe_path(cfg.source, relative).is_dir():
                raise SnapshotError(f"missing source namespace: {relative}")
        if require_marker:
            if self.marker_path.is_symlink():
                raise SnapshotError("backup volume marker may not be a symlink")
            marker = json.loads(self.marker_path.read_text())
            if marker != self.marker():
                raise SnapshotError("backup volume marker does not match configuration")
        self.mount_identity = identity

    def marker(self) -> dict[str, Any]:
        cfg = self.config
        return {"schema_version": 1, "purpose": "stockagent-additive-cold-backup",
                "authority_node_id": cfg.authority_node_id, "source": str(cfg.source),
                "destination": str(cfg.destination), "volume_id": cfg.volume_id}

    def initialize(self) -> None:
        self.check(require_marker=False)
        cfg = self.config
        safe_path(cfg.mount_point, cfg.destination.relative_to(cfg.mount_point).as_posix())
        if self.marker_path.exists():
            self.check()
            return
        if cfg.destination.parent.exists() and any(cfg.destination.parent.iterdir()):
            raise SnapshotError("refusing to enroll a non-empty unmarked backup directory")
        cfg.destination.mkdir(parents=True, exist_ok=True)
        atomic_write_json(self.marker_path, self.marker())
        self.check()


def object_descriptor(relative: str) -> str:
    match = OBJECT_PATH.fullmatch(relative)
    if not match or match[2] != match[3][:2] or EXPECTED_SUFFIX[match[1]] != match[4]:
        raise SnapshotError(f"non-canonical packed object: {relative}")
    return match[3]


def inventory(config: BackupConfig) -> tuple[list[dict[str, Any]], list[str]]:
    objects = []
    errors = []
    for kind in EXPECTED_SUFFIX:
        parent = config.source / "objects" / kind
        if parent.is_symlink():
            raise SnapshotError(f"source object directory is a symlink: {parent}")
        for directory, subdirs, names in os.walk(parent, followlinks=False):
            for name in list(subdirs):
                child = Path(directory) / name
                if child.is_symlink():
                    errors.append(f"source symlink: {child}")
                    subdirs.remove(name)
            for name in names:
                path = Path(directory) / name
                relative = path.relative_to(config.source).as_posix()
                # A partial transfer has not entered the immutable namespace.
                if name.startswith(".syncthing.") or name.endswith(".tmp"):
                    continue
                try:
                    digest = object_descriptor(relative)
                    info = signature(safe_path(config.source, relative))
                    objects.append({"relative": relative, "sha256": digest, "signature": info, "bytes": info[2]})
                except (OSError, SnapshotError) as exc:
                    errors.append(str(exc))
    return objects, errors


class PackedBackup:
    def __init__(self, config: BackupConfig, *, guard: VolumeGuard | None = None):
        self.config = config
        self.guard = guard or VolumeGuard(config)
        self.guard.check()
        config.state_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(config.state_dir / "verified.sqlite3")
        self.db.execute("CREATE TABLE IF NOT EXISTS verified (path TEXT PRIMARY KEY, digest TEXT NOT NULL, source_sig TEXT NOT NULL, target_sig TEXT NOT NULL, checked REAL NOT NULL)")
        self.current: dict[str, Any] = {}
        previous = config.state_dir / "status.json"
        if previous.exists():
            try:
                self.current["last_complete_at"] = json.loads(previous.read_text()).get("last_complete_at")
            except (OSError, ValueError):
                pass
        self.last_status_write = 0.0

    def close(self) -> None:
        self.db.close()

    def write_status(self, **changes: Any) -> dict[str, Any]:
        self.current.update(changes, updated_at=now_iso())
        atomic_write_json(self.config.state_dir / "status.json", self.current)
        self.last_status_write = time.monotonic()
        return dict(self.current)

    def trusted(self, relative: str, digest: str, source_sig: tuple[int, ...] | None = None, *, force: bool = False) -> bool:
        if force:
            return False
        row = self.db.execute("SELECT digest,source_sig,target_sig,checked FROM verified WHERE path=?", (relative,)).fetchone()
        if row is None or row[0] != digest or time.time() - row[3] > self.config.checksum_recheck_days * 86400:
            return False
        try:
            if source_sig is not None and not same_file_signature(json.loads(row[1]), source_sig):
                return False
            return same_file_signature(
                json.loads(row[2]), signature(safe_path(self.config.destination, relative))
            )
        except (OSError, ValueError, TypeError, SnapshotError):
            return False

    def remember(self, relative: str, digest: str, source_sig: tuple[int, ...]) -> None:
        target_sig = signature(safe_path(self.config.destination, relative))
        self.db.execute("INSERT OR REPLACE INTO verified VALUES (?,?,?,?,?)", (relative, digest, json.dumps(source_sig), json.dumps(target_sig), time.time()))
        self.db.commit()

    def checkpoint(self, path: str, phase: str, processed: int, total: int) -> None:
        if time.monotonic() - self.last_status_write >= 2:
            self.guard.check()
            self.write_status(current_object=path, phase=phase,
                              current_object_bytes=processed, current_object_total_bytes=total)

    def hash_file(self, path: Path, relative: str, phase: str) -> str:
        before = signature(path)
        digest = hashlib.sha256()
        count = 0
        with path.open("rb") as stream:
            while block := stream.read(CHUNK_BYTES):
                digest.update(block)
                count += len(block)
                self.checkpoint(relative, phase, count, before[2])
        if signature(path) != before:
            raise SnapshotError(f"file changed during verification: {path}")
        return digest.hexdigest()

    def copy_object(self, item: dict[str, Any], *, force: bool = False) -> bool:
        relative, expected = item["relative"], item["sha256"]
        self.guard.check()
        source = safe_path(self.config.source, relative)
        target = safe_path(self.config.destination, relative)
        before = signature(source)
        if before != item["signature"]:
            raise SnapshotError(f"source changed since inventory: {relative}")
        if self.trusted(relative, expected, before, force=force):
            return False
        if target.exists():
            if signature(target)[2] == before[2] and self.hash_file(target, relative, "verify_existing") == expected:
                if self.hash_file(source, relative, "verify_source") != expected:
                    raise SnapshotError(f"source digest mismatch; valid backup retained: {relative}")
                self.remember(relative, expected, before)
                return False
            # Never silently overwrite a corrupt/different existing backup.
            raise SnapshotError(f"backup checksum mismatch; retained for inspection: {relative}")
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = safe_path(self.config.destination, relative + ".partial")
        if partial.exists() and signature(partial)[2] > before[2]:
            raise SnapshotError(f"oversize partial backup retained: {relative}")
        offset = signature(partial)[2] if partial.exists() else 0
        if shutil.disk_usage(self.config.mount_point).free < before[2] - offset + self.config.reserve_bytes:
            raise SnapshotError("insufficient backup free space; previous backup retained")
        digest = hashlib.sha256()
        with source.open("rb") as incoming, partial.open("r+b" if partial.exists() else "x+b") as outgoing:
            # A restart can resume a partially copied object only after checking
            # every retained prefix byte against the still-immutable source.
            processed = 0
            while processed < offset:
                size = min(CHUNK_BYTES, offset - processed)
                block = incoming.read(size)
                if len(block) != size or outgoing.read(size) != block:
                    raise SnapshotError(f"partial prefix mismatch; retained for inspection: {relative}")
                digest.update(block)
                processed += size
                self.checkpoint(relative, "resume_prefix", processed, before[2])
            while block := incoming.read(CHUNK_BYTES):
                outgoing.write(block)
                digest.update(block)
                processed += len(block)
                self.checkpoint(relative, "copy", processed, before[2])
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if signature(source) != before or processed != before[2] or digest.hexdigest() != expected:
            raise SnapshotError(f"source changed or failed checksum; partial retained: {relative}")
        if self.hash_file(partial, relative, "readback") != expected:
            raise SnapshotError(f"backup readback checksum failed; partial retained: {relative}")
        self.guard.check()
        if target.exists():
            raise SnapshotError(f"backup target appeared during copy: {relative}")
        os.replace(partial, target)
        _fsync_directory(target.parent)
        self.remember(relative, expected, before)
        return True

    def copy_metadata(self, relative: str, expected: bytes, *, mutable: bool = False) -> None:
        self.guard.check()
        target = safe_path(self.config.destination, relative)
        if target.exists():
            prior = target.read_bytes()
            if prior == expected:
                return
            if not mutable:
                raise SnapshotError(f"immutable backup metadata differs: {relative}")
            digest = hashlib.sha256(prior).hexdigest()
            history_key = Path(relative).with_suffix("").as_posix()
            archive = safe_path(self.config.destination, f"head-history/{history_key}/{digest}.json")
            if archive.exists() and archive.read_bytes() != prior:
                raise SnapshotError("head history digest collision")
            if not archive.exists():
                atomic_write_bytes(archive, prior)
        atomic_write_bytes(target, expected)
        if target.read_bytes() != expected:
            raise SnapshotError(f"backup metadata readback failed: {relative}")

    def references_complete(self, manifest: dict[str, Any], checked: dict[tuple, bool]) -> bool:
        complete = True
        for ref in [manifest["archive"]["inventory"], *manifest["archive"]["objects"]]:
            key = (ref["relpath"], ref["sha256"], int(ref["bytes"]))
            if key in checked:
                complete = complete and checked[key]
                continue
            if object_descriptor(ref["relpath"]) != ref["sha256"]:
                raise SnapshotError("object descriptor/path mismatch")
            source_sig = signature(safe_path(self.config.source, ref["relpath"]))
            if source_sig[2] != int(ref["bytes"]):
                raise SnapshotError(f"object size mismatch: {ref['relpath']}")
            checked[key] = self.trusted(ref["relpath"], ref["sha256"], source_sig)
            if not checked[key]:
                complete = False
        return complete

    def metadata(self, errors: list[str]) -> tuple[int, int, int, int]:
        manifests = 0
        verified_releases = set()
        checked: dict[tuple, bool] = {}
        promoted = 0
        pending = 0
        # Preserve every immutable manifest, including historical releases. A
        # current head is published separately, only when every object is proven.
        for path in sorted((self.config.source / "manifests").glob("*/*.json")):
            try:
                relative = path.relative_to(self.config.source).as_posix()
                safe_path(self.config.source, relative)
                signature(path)
                raw = path.read_bytes()
                manifest = json.loads(raw)
                _validate_manifest(manifest)
                if manifest["dataset"] != path.parent.name or manifest["snapshot_id"] != path.stem:
                    raise SnapshotError(f"manifest identity mismatch: {relative}")
                self.copy_metadata(relative, raw)
                manifests += 1
                if self.references_complete(manifest, checked):
                    verified_releases.add(relative)
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
                errors.append(str(exc))
        for path in sorted((self.config.source / "heads").glob("*/*.json")):
            try:
                relative = path.relative_to(self.config.source).as_posix()
                safe_path(self.config.source, relative)
                signature(path)
                raw = path.read_bytes()
                head = json.loads(raw)
                if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION or head["dataset"] != path.parent.name
                        or head["node_id"] != path.stem or ".sync-conflict-" in path.name):
                    raise SnapshotError(f"invalid source head: {relative}")
                resolved = resolve_packed_snapshot_id(self.config.source, head["dataset"], head["snapshot_id"], require_objects=False)
                manifest = resolved.manifest
                if (resolved.manifest_sha256 != head["manifest_sha256"] or manifest["hlc"] != head["hlc"]
                        or manifest["publisher"]["node_id"] != head["node_id"]
                        or head["manifest_relpath"] != resolved.manifest_path.relative_to(self.config.source).as_posix()):
                    raise SnapshotError(f"head/manifest proof mismatch: {relative}")
                if head["manifest_relpath"] not in verified_releases:
                    pending += 1
                    continue
                # Source publication may advance during a long copy. Never
                # commit a stale read under the current head's name.
                if path.read_bytes() != raw:
                    pending += 1
                    continue
                manifest_raw = resolved.manifest_path.read_bytes()
                if hashlib.sha256(manifest_raw).hexdigest() != head["manifest_sha256"]:
                    raise SnapshotError("manifest changed while preparing backup head")
                self.copy_metadata(head["manifest_relpath"], manifest_raw)
                self.copy_metadata(relative, raw, mutable=True)
                promoted += 1
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
                pending += 1
                errors.append(str(exc))
        return manifests, promoted, pending, len(verified_releases)

    def run_once(self, *, max_objects: int | None = None, priority: set[str] | None = None,
                 force_verify: bool = False, time_budget_seconds: float | None = None) -> dict[str, Any]:
        self.guard.check()
        objects, errors = inventory(self.config)
        total_bytes = sum(item["bytes"] for item in objects)
        pending = []
        verified_bytes = 0
        for item in objects:
            if self.trusted(item["relative"], item["sha256"], item["signature"], force=force_verify):
                verified_bytes += item["bytes"]
            else:
                pending.append(item)
        pending.sort(key=lambda item: (item["relative"] not in (priority or set()), -item["signature"][3], item["bytes"]))
        prior_errors = self.current.get("errors", [])
        self.write_status(state="copying", started_at=now_iso(), source=str(self.config.source),
                          destination=str(self.config.destination), authority_node_id=self.config.authority_node_id,
                          total_objects=len(objects), total_bytes=total_bytes, verified_bytes=verified_bytes,
                          remaining_bytes=total_bytes - verified_bytes, pending_objects=len(pending),
                          errors=errors[:50], error_count=len(errors), previous_pass_errors=prior_errors,
                          integrity_state="checking", deletion_propagation=False, materialization=False)
        copied_objects = 0
        completed = 0
        batch_start = time.monotonic()
        for item in pending[:max_objects]:
            try:
                copied_objects += int(self.copy_object(item, force=force_verify))
                verified_bytes += item["bytes"]
                completed += 1
                self.write_status(verified_bytes=verified_bytes, remaining_bytes=total_bytes - verified_bytes,
                                  pending_objects=len(pending) - completed, current_object=item["relative"], phase="verified")
            except (OSError, SnapshotError) as exc:
                errors.append(str(exc))
                self.write_status(errors=errors[:50], error_count=len(errors), integrity_state="degraded")
                # A missing volume or low disk must stop the whole pass.
                self.guard.check()
                if shutil.disk_usage(self.config.mount_point).free < self.config.reserve_bytes:
                    break
            if time_budget_seconds and time.monotonic() - batch_start >= time_budget_seconds:
                break
        manifests, heads, pending_heads, verified_releases = self.metadata(errors)
        state = "degraded" if errors else "copying" if len(pending) > completed or pending_heads or verified_releases < manifests else "up_to_date"
        result = self.write_status(state=state, verified_bytes=verified_bytes, remaining_bytes=total_bytes - verified_bytes,
                                  copied_objects=copied_objects, completed_objects=completed, pending_objects=len(pending) - completed,
                                  manifests=manifests, verified_heads=heads, pending_heads=pending_heads,
                                  verified_releases=verified_releases, pending_releases=manifests - verified_releases,
                                  current_heads_complete=bool(heads) and pending_heads == 0,
                                  present_objects_complete=len(pending) == completed,
                                  integrity_state="degraded" if errors else "verified" if state == "up_to_date" else "pending",
                                  errors=errors[:50], error_count=len(errors), current_object=None, phase=None)
        self.guard.check()
        # An independent receipt is useful even if the WSL/C: disk is lost.
        atomic_write_json(self.config.destination.parent / "status.json", result)
        if state == "up_to_date":
            result["last_complete_at"] = now_iso()
            atomic_write_json(self.config.destination.parent / "last-complete.json", result)
            self.write_status(last_complete_at=result["last_complete_at"])
        return result
