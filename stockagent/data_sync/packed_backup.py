"""One-way, additive, checksum-verified backup of an existing packed store.

No materialization, deletion propagation, node identity copying, publication,
or peer access. D: keeps the canonical object format so the existing verifier
and fetch commands can restore it without a second archive implementation.
"""

from __future__ import annotations

from contextlib import ExitStack, nullcontext
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


def read_existing_metadata_nofollow(root: Path, relative: str) -> bytes:
    """Read an existing D: metadata file without repeated DrvFs path resolve.

    This is only an unchanged-file fast path. Missing targets use the existing
    safe_path/write path; changed targets are re-read there before promotion.
    All ancestors and the leaf are opened without following symlinks and are
    rechecked against their names before the bytes are trusted.
    """

    path = Path(relative)
    if path.is_absolute() or not path.parts or any(
        part in {".", ".."} for part in path.parts
    ):
        raise SnapshotError(f"unsafe backup relative path: {relative}")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    parts = path.parts
    pinned: list[tuple[Path, tuple[int, int]]] = []
    with ExitStack() as stack:
        parent_fd = os.open(root, flags)
        stack.callback(os.close, parent_fd)
        root_info = os.fstat(parent_fd)
        pinned.append((root, (root_info.st_dev, root_info.st_ino)))
        current_path = root
        for part in parts[:-1]:
            child_fd = os.open(part, flags, dir_fd=parent_fd)
            stack.callback(os.close, child_fd)
            current_path = current_path / part
            child_info = os.fstat(child_fd)
            pinned.append((current_path, (child_info.st_dev, child_info.st_ino)))
            parent_fd = child_fd
        leaf_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW, dir_fd=parent_fd)
        with os.fdopen(leaf_fd, "rb") as handle:
            before = os.fstat(handle.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise SnapshotError(f"not a regular backup metadata file: {relative}")
            raw = handle.read()
            after = os.fstat(handle.fileno())
        leaf_at_name = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        if signature_from_stat(before) != signature_from_stat(after) or (
            signature_from_stat(after) != signature_from_stat(leaf_at_name)
        ):
            raise SnapshotError(f"backup metadata changed during read: {relative}")
        for directory, identity in pinned:
            current = os.stat(directory, follow_symlinks=False)
            if not stat.S_ISDIR(current.st_mode) or (
                current.st_dev, current.st_ino
            ) != identity:
                raise SnapshotError(f"backup metadata directory changed: {directory}")
        return raw


def signature_from_stat(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class PinnedObjectSignatures:
    """Read target object metadata through no-follow, pass-local directory FDs.

    DrvFs makes repeated ``resolve``/``is_symlink`` calls expensive. Pin each
    object shard once, then recheck that every pinned directory still occupies
    its original path before trusting the pass. New/copy paths retain the
    existing ``safe_path`` and checksum-readback rules.
    """

    def __init__(self, root: Path):
        self.root = root
        self._flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        root_fd = os.open(root, self._flags)
        try:
            self._directories: dict[tuple[str, ...], tuple[int, tuple[int, int]]] = {
                (): (root_fd, self._identity(os.fstat(root_fd))),
            }
        except BaseException:
            os.close(root_fd)
            raise

    @staticmethod
    def _identity(info: os.stat_result) -> tuple[int, int]:
        return (info.st_dev, info.st_ino)

    def signature(self, relative: str) -> tuple[int, ...]:
        object_descriptor(relative)
        parts = tuple(Path(relative).parts)
        for length in range(1, len(parts)):
            prefix = parts[:length]
            if prefix not in self._directories:
                parent_fd = self._directories[parts[:length - 1]][0]
                fd = os.open(parts[length - 1], self._flags, dir_fd=parent_fd)
                try:
                    self._directories[prefix] = (fd, self._identity(os.fstat(fd)))
                except BaseException:
                    os.close(fd)
                    raise
        info = os.stat(parts[-1], dir_fd=self._directories[parts[:-1]][0], follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            raise SnapshotError(f"not a regular backup object: {relative}")
        return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)

    def recheck(self) -> None:
        for parts, (_fd, identity) in self._directories.items():
            path = self.root.joinpath(*parts)
            info = os.stat(path, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or self._identity(info) != identity:
                raise SnapshotError(f"backup object directory changed during scan: {path}")

    def close(self) -> None:
        for fd, _identity in self._directories.values():
            os.close(fd)
        self._directories.clear()

    def __enter__(self) -> "PinnedObjectSignatures":
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        self.close()


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
    idle_reconcile_seconds: float = 300
    checksum_recheck_days: float = 30
    backup_scope: str = "all_history"

    @classmethod
    def load(cls, path: Path) -> "BackupConfig":
        raw = json.loads(path.read_text())
        if raw.pop("schema_version", None) != 1:
            raise SnapshotError("unsupported packed backup configuration")
        for key in ("source", "destination", "mount_point", "state_dir"):
            raw[key] = Path(raw[key])
        result = cls(**raw)
        if (result.batch_objects < 1 or result.poll_seconds <= 0
                or result.idle_reconcile_seconds < result.poll_seconds
                or result.reserve_bytes < 0 or result.checksum_recheck_days <= 0
                or result.backup_scope not in {"all_history", "current_heads"}):
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
        if (cfg.source / ".stockagent-d-primary").exists():
            raise SnapshotError("D cold primary cannot back up to the same D volume")
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


def current_inventory(config: BackupConfig) -> tuple[
    list[dict[str, Any]], list[str], set[str], dict[str, bytes],
]:
    """Inventory only immutable objects reachable from the current source heads.

    Historical D: objects and manifests are left untouched. An invalid or
    incomplete current head remains an error, never a reason to select an older
    snapshot silently.
    """
    objects: dict[str, dict[str, Any]] = {}
    manifests: set[str] = set()
    errors: list[str] = []
    head_bytes: dict[str, bytes] = {}
    heads = sorted((config.source / "heads").glob("*/*.json"))
    if not heads:
        errors.append("no current packed heads found")
    for path in heads:
        try:
            relative = path.relative_to(config.source).as_posix()
            safe_path(config.source, relative)
            signature(path)
            raw = path.read_bytes()
            head_bytes[relative] = raw
            head = json.loads(raw)
            if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION
                    or head["dataset"] != path.parent.name or head["node_id"] != path.stem
                    or ".sync-conflict-" in path.name):
                raise SnapshotError(f"invalid source head: {relative}")
            resolved = resolve_packed_snapshot_id(
                config.source, head["dataset"], head["snapshot_id"], require_objects=False,
            )
            manifest = resolved.manifest
            manifest_relative = resolved.manifest_path.relative_to(config.source).as_posix()
            if (resolved.manifest_sha256 != head["manifest_sha256"]
                    or manifest["hlc"] != head["hlc"]
                    or manifest["publisher"]["node_id"] != head["node_id"]
                    or head["manifest_relpath"] != manifest_relative):
                raise SnapshotError(f"head/manifest proof mismatch: {relative}")
            manifests.add(manifest_relative)
            for ref in [manifest["archive"]["inventory"], *manifest["archive"]["objects"]]:
                object_relative = ref["relpath"]
                digest = object_descriptor(object_relative)
                if digest != ref["sha256"]:
                    raise SnapshotError(f"object descriptor/path mismatch: {object_relative}")
                source_sig = signature(safe_path(config.source, object_relative))
                size = int(ref["bytes"])
                if source_sig[2] != size:
                    raise SnapshotError(f"object size mismatch: {object_relative}")
                existing = objects.get(object_relative)
                if existing is not None and (existing["sha256"] != digest or existing["bytes"] != size):
                    raise SnapshotError(f"conflicting current object reference: {object_relative}")
                objects[object_relative] = {
                    "relative": object_relative, "sha256": digest,
                    "signature": source_sig, "bytes": size,
                }
        except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
            errors.append(f"{path}: {exc}")
    return list(objects.values()), errors, manifests, head_bytes


def current_heads_unchanged(config: BackupConfig, before: dict[str, bytes]) -> bool:
    """Fail the pass if a source publication advanced during its object scan."""
    paths = sorted((config.source / "heads").glob("*/*.json"))
    if {path.relative_to(config.source).as_posix() for path in paths} != set(before):
        return False
    return all(path.read_bytes() == before[path.relative_to(config.source).as_posix()] for path in paths)


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
                prior = json.loads(previous.read_text())
                field = "last_current_complete_at" if config.backup_scope == "current_heads" else "last_complete_at"
                self.current[field] = prior.get(field)
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

    def trusted(self, relative: str, digest: str, source_sig: tuple[int, ...] | None = None, *,
                force: bool = False, target_signatures: PinnedObjectSignatures | None = None) -> bool:
        if force:
            return False
        row = self.db.execute("SELECT digest,source_sig,target_sig,checked FROM verified WHERE path=?", (relative,)).fetchone()
        if row is None or row[0] != digest or time.time() - row[3] > self.config.checksum_recheck_days * 86400:
            return False
        try:
            if source_sig is not None and not same_file_signature(json.loads(row[1]), source_sig):
                return False
            return same_file_signature(
                json.loads(row[2]),
                target_signatures.signature(relative) if target_signatures is not None
                else signature(safe_path(self.config.destination, relative)),
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
        try:
            if read_existing_metadata_nofollow(self.config.destination, relative) == expected:
                return
        except FileNotFoundError:
            # A new file or directory still follows the audited safe_path,
            # volume guard and atomic write/readback path below.
            pass
        target = safe_path(self.config.destination, relative)
        if target.exists():
            prior = target.read_bytes()
            if prior == expected:
                # run_once checks the volume before the pass and again before
                # publishing success. An unchanged read needs no per-file
                # volume check; every metadata write still does.
                return
            self.guard.check()
            if not mutable:
                raise SnapshotError(f"immutable backup metadata differs: {relative}")
            digest = hashlib.sha256(prior).hexdigest()
            history_key = Path(relative).with_suffix("").as_posix()
            archive = safe_path(self.config.destination, f"head-history/{history_key}/{digest}.json")
            if archive.exists() and archive.read_bytes() != prior:
                raise SnapshotError("head history digest collision")
            if not archive.exists():
                atomic_write_bytes(archive, prior)
        else:
            self.guard.check()
        atomic_write_bytes(target, expected)
        if target.read_bytes() != expected:
            raise SnapshotError(f"backup metadata readback failed: {relative}")

    def references_complete(
        self,
        manifest: dict[str, Any],
        checked: dict[tuple, bool],
        known_objects: dict[tuple[str, str, int], tuple[tuple[int, ...], bool]] | None = None,
    ) -> bool:
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
            known = (known_objects or {}).get(key)
            checked[key] = (
                known[1] if known is not None and known[0] == source_sig
                else self.trusted(ref["relpath"], ref["sha256"], source_sig)
            )
            if not checked[key]:
                complete = False
        return complete

    def metadata(
        self,
        errors: list[str],
        known_objects: dict[tuple[str, str, int], tuple[tuple[int, ...], bool]] | None = None,
        manifest_paths: set[str] | None = None,
    ) -> tuple[int, int, int, int, dict[str, float]]:
        manifests = 0
        verified_releases = set()
        checked: dict[tuple, bool] = {}
        promoted = 0
        pending = 0
        # Full-history mode copies every manifest; current-head mode copies
        # only selected manifests. In either mode a head is promoted only
        # after its complete object graph is proven.
        paths = (
            [safe_path(self.config.source, relative) for relative in sorted(manifest_paths)]
            if manifest_paths is not None
            else sorted((self.config.source / "manifests").glob("*/*.json"))
        )
        manifests_started = time.monotonic()
        for path in paths:
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
                if self.references_complete(manifest, checked, known_objects):
                    verified_releases.add(relative)
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
                errors.append(str(exc))
        manifests_done = time.monotonic()
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
                # The unchanged-head probe is read-only, but it must obey the
                # same no-follow boundary as copy_metadata. A safe_path()
                # check followed by Path.read_bytes() leaves a replacement
                # window and repeats expensive DrvFs path resolution.
                try:
                    head_unchanged = (
                        read_existing_metadata_nofollow(self.config.destination, relative)
                        == raw
                    )
                except FileNotFoundError:
                    head_unchanged = False
                if not head_unchanged:
                    # A new/changed head needs a fresh target proof immediately
                    # before promotion; the pass-local cache only avoids a
                    # second scan for heads that were already committed.
                    if not self.references_complete(manifest, {}):
                        pending += 1
                        continue
                self.copy_metadata(head["manifest_relpath"], manifest_raw)
                self.copy_metadata(relative, raw, mutable=True)
                promoted += 1
            except (OSError, ValueError, TypeError, KeyError, SnapshotError) as exc:
                pending += 1
                errors.append(str(exc))
        heads_done = time.monotonic()
        return manifests, promoted, pending, len(verified_releases), {
            "metadata_manifests": round((manifests_done - manifests_started) * 1000, 2),
            "metadata_heads": round((heads_done - manifests_done) * 1000, 2),
        }

    def run_once(self, *, max_objects: int | None = None, priority: set[str] | None = None,
                 force_verify: bool = False, time_budget_seconds: float | None = None) -> dict[str, Any]:
        pass_started = time.monotonic()
        self.guard.check()
        guard_done = time.monotonic()
        prior_errors = self.current.get("errors", [])
        # A previous successful receipt must not remain the apparent current
        # state while a slow DrvFs scan is still in progress.
        self.write_status(state="checking", started_at=now_iso(), phase="source_inventory",
                          backup_scope=self.config.backup_scope, integrity_state="checking",
                          current_heads_complete=False, present_objects_complete=False,
                          current_object=None, destination_trust_scanned=0,
                          pending_objects=None, remaining_bytes=None, verified_bytes=None,
                          total_bytes=None, total_objects=None, error_count=0, errors=[],
                          stage_timings_ms={})
        manifest_paths = None
        source_heads = None
        if self.config.backup_scope == "current_heads":
            objects, errors, manifest_paths, source_heads = current_inventory(self.config)
        else:
            objects, errors = inventory(self.config)
        inventory_done = time.monotonic()
        self.write_status(phase="destination_trust", total_objects=len(objects),
                          destination_trust_scanned=0, error_count=len(errors), errors=errors[:50])
        last_scan_status = time.monotonic()
        total_bytes = sum(item["bytes"] for item in objects)
        pending = []
        known_objects: dict[tuple[str, str, int], tuple[tuple[int, ...], bool]] = {}
        verified_bytes = 0
        target_context = (
            PinnedObjectSignatures(self.config.destination)
            if self.config.destination.is_dir() else nullcontext(None)
        )
        with target_context as target_signatures:
            for scanned, item in enumerate(objects, 1):
                key = (item["relative"], item["sha256"], item["bytes"])
                trusted = self.trusted(item["relative"], item["sha256"], item["signature"],
                                       force=force_verify, target_signatures=target_signatures)
                known_objects[key] = (item["signature"], trusted)
                if trusted:
                    verified_bytes += item["bytes"]
                else:
                    pending.append(item)
                if time.monotonic() - last_scan_status >= 5:
                    self.write_status(destination_trust_scanned=scanned)
                    last_scan_status = time.monotonic()
            if target_signatures is not None:
                target_signatures.recheck()
        pending.sort(key=lambda item: (item["relative"] not in (priority or set()), -item["signature"][3], item["bytes"]))
        trust_done = time.monotonic()
        self.write_status(state="copying", started_at=now_iso(), source=str(self.config.source),
                          destination=str(self.config.destination), authority_node_id=self.config.authority_node_id,
                          backup_scope=self.config.backup_scope,
                          historical_completeness="not_checked" if manifest_paths is not None else "checking",
                          total_objects=len(objects), total_bytes=total_bytes, verified_bytes=verified_bytes,
                          remaining_bytes=total_bytes - verified_bytes, pending_objects=len(pending),
                          destination_trust_scanned=len(objects),
                          errors=errors[:50], error_count=len(errors), previous_pass_errors=prior_errors,
                          stage_timings_ms={
                              "volume_guard": round((guard_done - pass_started) * 1000, 2),
                              "source_inventory": round((inventory_done - guard_done) * 1000, 2),
                              "destination_trust": round((trust_done - inventory_done) * 1000, 2),
                          },
                          integrity_state="checking", deletion_propagation=False, materialization=False)
        initial_status_done = time.monotonic()
        copied_objects = 0
        completed = 0
        batch_start = time.monotonic()
        for item in pending[:max_objects]:
            try:
                copied_objects += int(self.copy_object(item, force=force_verify))
                known_objects[(item["relative"], item["sha256"], item["bytes"])] = (item["signature"], True)
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
        object_processing_done = time.monotonic()
        manifests, heads, pending_heads, verified_releases, metadata_timings_ms = self.metadata(
            errors, known_objects, manifest_paths,
        )
        metadata_done = time.monotonic()
        if source_heads is not None:
            try:
                if not current_heads_unchanged(self.config, source_heads):
                    pending_heads += 1
            except OSError as exc:
                errors.append(f"current head recheck failed: {exc}")
        head_recheck_done = time.monotonic()
        stage_timings_ms = {
            "volume_guard": round((guard_done - pass_started) * 1000, 2),
            "source_inventory": round((inventory_done - guard_done) * 1000, 2),
            "destination_trust": round((trust_done - inventory_done) * 1000, 2),
            "initial_status": round((initial_status_done - trust_done) * 1000, 2),
            "object_processing": round((object_processing_done - initial_status_done) * 1000, 2),
            "metadata": round((metadata_done - object_processing_done) * 1000, 2),
            **metadata_timings_ms,
            "head_recheck": round((head_recheck_done - metadata_done) * 1000, 2),
            "pre_final_status_total": round((head_recheck_done - pass_started) * 1000, 2),
        }
        state = "degraded" if errors else "copying" if len(pending) > completed or pending_heads or verified_releases < manifests else "up_to_date"
        result = self.write_status(state=state, verified_bytes=verified_bytes, remaining_bytes=total_bytes - verified_bytes,
                                  copied_objects=copied_objects, completed_objects=completed, pending_objects=len(pending) - completed,
                                  manifests=manifests, verified_heads=heads, pending_heads=pending_heads,
                                  verified_releases=verified_releases, pending_releases=manifests - verified_releases,
                                  current_heads_complete=bool(heads) and pending_heads == 0 and not errors,
                                  present_objects_complete=len(pending) == completed,
                                  historical_completeness="not_checked" if manifest_paths is not None else (
                                      "degraded" if errors or verified_releases < manifests else "verified"
                                  ),
                                  integrity_state="degraded" if errors else "verified" if state == "up_to_date" else "pending",
                                  stage_timings_ms=stage_timings_ms,
                                  errors=errors[:50], error_count=len(errors), current_object=None, phase=None)
        self.guard.check()
        # An independent receipt is useful even if the WSL/C: disk is lost.
        atomic_write_json(self.config.destination.parent / "status.json", result)
        if state == "up_to_date":
            if manifest_paths is not None:
                result["last_current_complete_at"] = now_iso()
                atomic_write_json(self.config.destination.parent / "last-current-complete.json", result)
                self.write_status(last_current_complete_at=result["last_current_complete_at"])
            else:
                result["last_complete_at"] = now_iso()
                atomic_write_json(self.config.destination.parent / "last-complete.json", result)
                self.write_status(last_complete_at=result["last_complete_at"])
        return result
