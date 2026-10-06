"""Sealed append-only lake replication; full SHA proof supplements rclone.

No destination deletion, catalogue mutation, executable dispatch or credential
transport. Shared with the fixed locally installed lab203 archive adapter.
"""
from __future__ import annotations

from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import time

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.runtime_identity import identity_sha256

CONTRACT = "immutable_lake_delivery_v1"
ACK = "immutable_lake_archive_acceptance_v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")
NATIVE_HASH_MIN_BYTES = 8 * 1024**2  # Penguin NTFS full-hash comparison, 2026-10-06.


def safe(root: Path, relative: str) -> Path:
    name = PurePosixPath(relative)
    if (not name.parts or name.is_absolute() or ".." in name.parts
            or any(c in relative for c in "\\\r\n\0") or name.as_posix() != relative):
        raise ValueError("unsafe immutable member path")
    path = root / relative
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("immutable member path is redirected")
    return path


def digest(path: Path) -> str:
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError("immutable member is not a regular file")
    if NATIVE_HASH_MIN_BYTES <= before.st_size <= 8 * 1024**3:
        from stockagent.data_sync.windows_cold_io import hash_file, windows_path
        if windows_path(path) is not None:
            return hash_file(path)
    with path.open("rb") as stream:
        value = hashlib.file_digest(stream, "sha256").hexdigest()
    after = path.lstat()
    keys = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, k) != getattr(after, k) for k in keys):
        raise ValueError("immutable member changed during full verification")
    return value


def inventory(root: Path) -> dict:
    safe(root, "manifest.json")
    files = {}
    pending = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        safe(root, relative)
        if path.is_dir():
            continue
        pending.append((path, relative))
    from stockagent.data_sync.windows_cold_io import hash_many, windows_path
    native = [p for p, _ in pending if NATIVE_HASH_MIN_BYTES <= p.lstat().st_size <= 8 * 1024**3
              and windows_path(p) is not None]
    hashes = {}
    if len(native) > 1:
        for offset in range(0, len(native), 16):
            hashes.update(hash_many(native[offset:offset + 16]))
    for path, relative in pending:
        files[relative] = {"sha256": hashes[path] if path in hashes else digest(path), "bytes": path.stat().st_size}
    return files


def seal(root: Path, context: dict) -> dict:
    if (root / "manifest.json").exists() or (root / "READY").exists():
        raise ValueError("preserve the existing sealed delivery")
    body = {"contract": CONTRACT, "context": context, "files": inventory(root)}
    if not body["files"]:
        raise ValueError("empty immutable delivery")
    value = {**body, "identity_sha256": identity_sha256(body)}
    atomic_write_json(root / "manifest.json", value, durable=True)
    atomic_write_text(root / "READY", value["identity_sha256"] + "\n", durable=True)
    verify(root)
    return value


def _read_manifest(root: Path) -> dict:
    safe(root, "manifest.json")
    if (root / "manifest.json").stat().st_size > 16 * 1024**2:
        raise ValueError("lake manifest exceeds the bounded metadata size")
    value = json.loads((root / "manifest.json").read_bytes())
    if (set(value) != {"contract", "context", "files", "identity_sha256"}
            or value["contract"] != CONTRACT or not HASH.fullmatch(value["identity_sha256"])
            or identity_sha256({k: v for k, v in value.items() if k != "identity_sha256"}) != value["identity_sha256"]):
        raise ValueError("lake manifest identity differs")
    for relative, row in value["files"].items():
        if relative in {"READY", "manifest.json"} or set(row) != {"sha256", "bytes"}:
            raise ValueError("invalid immutable member descriptor")
        safe(root, relative)
        if not HASH.fullmatch(row["sha256"]) or type(row["bytes"]) is not int or row["bytes"] < 0:
            raise ValueError("invalid immutable member hash/size")
    return value


def read_manifest(root: Path, *, require_ready: bool = True) -> dict:
    """Validate bounded metadata; payload SHA/set verification is in verify."""
    value = _read_manifest(root)
    if require_ready or (root / 'READY').exists():
        if safe(root, "READY").read_text() != value["identity_sha256"] + "\n":
            raise ValueError("READY differs from its exact manifest")
    return value


def verify(root: Path, *, require_ready: bool = True) -> dict:
    value = _read_manifest(root)
    actual = inventory(root)
    expected = {**value["files"], "manifest.json": actual.get("manifest.json")}
    if require_ready or "READY" in actual:
        if safe(root, "READY").read_text() != value["identity_sha256"] + "\n":
            raise ValueError("READY differs from its exact manifest")
        expected["READY"] = actual.get("READY")
    if actual != expected:
        raise ValueError("immutable delivery exact set or full SHA-256 differs")
    return value


def replicate(source: Path, destination: Path, rclone: Path, *, workers: int = 2) -> dict:
    started = time.perf_counter()
    if type(workers) is not int or not 1 <= workers <= 8:
        raise ValueError("replication worker count is outside its resource budget")
    manifest = verify(source)
    safe(destination, "manifest.json")
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("replication roots overlap")
    # --checksum can be size-only for a backend with no shared hash. Refuse all
    # conflicting existing members before asking rclone to perform any write.
    expected = inventory(source)
    if destination.exists():
        actual = inventory(destination)
        if any(expected.get(k) != row for k, row in actual.items()):
            raise ValueError("destination has conflicting or unexpected immutable bytes")
    destination.mkdir(parents=True, exist_ok=True)
    flags = ["--immutable", "--checksum", "--transfers", str(workers), "--checkers", str(workers),
             "--buffer-size", "8Mi", "--retries", "1", "--low-level-retries", "2", "--stats", "0"]
    commands = [
        [str(rclone), "copy", str(source), str(destination), "--exclude", "READY", *flags],
        [str(rclone), "copyto", str(source / "READY"), str(destination / "READY"), *flags],
    ]
    for i, argv in enumerate(commands):
        if i == 1:
            value = verify(destination, require_ready=False)
            if value != manifest:
                raise ValueError("copied lake manifest differs from its source")
        result = subprocess.run(argv, capture_output=True, timeout=3600)
        if result.returncode:
            raise RuntimeError("immutable rclone replication failed with exit " + str(result.returncode))
    if verify(source) != manifest or verify(destination) != manifest:
        raise ValueError("replication changed its source or target identity")
    return {"contract": ACK, "delivery_identity_sha256": manifest["identity_sha256"],
            "manifest_file_sha256": digest(source / "manifest.json"), "all_files_sha256_verified": True,
            "exact_file_set_verified": True, "source_unchanged_verified": True,
            "complete_files": len(expected), "complete_bytes": sum(r["bytes"] for r in expected.values()),
            "command_exit_codes": [0, 0], "complete_workflow_seconds": time.perf_counter() - started,
            "accepted_at_utc": datetime.now(timezone.utc).isoformat()}


def relay_cycle(config: dict) -> dict:
    from stockagent.data_sync.nas_target import NasTarget
    from stockagent.runtime_identity import runtime_identity, validate_runtime_lock
    nas = NasTarget(Path(config["nas_configuration"]))
    state = Path(config["state_root"])
    state.mkdir(parents=True, mode=0o700, exist_ok=True)
    rows = []
    with Path(config["owner_lock"]).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock = json.loads(Path(config["runtime_lock"]).read_bytes())
        if validate_runtime_lock(lock, runtime_identity()):
            raise ValueError("relay Mamba runtime changed")
        before = nas.check()
        archive = nas.user_directory / "stockagent-immutable-lake-v1"
        safe(archive, "archive-volume.json")
        archive.mkdir(exist_ok=True)
        marker = {"contract": "immutable_lake_archive_volume_v1", "producer_device_id": config["producer_device_id"],
                  "receiver_device_id": config["receiver_device_id"]}
        mark = archive / "archive-volume.json"
        if mark.exists():
            if json.loads(mark.read_bytes()) != marker:
                raise ValueError("NAS archive belongs to another authority")
        else:
            atomic_write_json(mark, marker, durable=True)
        candidates = sorted(Path(config["ingress_root"]).glob("lake-*"))
        candidates.sort(key=lambda p: ((state / (p.name + ".json")).exists(), p.name))
        for source in candidates[:config.get("maximum_jobs", 4)]:
            try:
                m = verify(source)
                if source.name != "lake-" + m["identity_sha256"]:
                    raise ValueError("delivery directory identity differs")
                if any(m["context"].get(k) != config[k] for k in ("producer_device_id", "receiver_device_id")):
                    raise ValueError("lake delivery is from another paired producer")
                nas.check(additional_bytes=sum(r["bytes"] for r in m["files"].values()))
                proof = replicate(source, archive / source.name, Path(config["rclone"]), workers=config.get("workers", 2))
                nas.unchanged(before)
                # Copy from NAS to a separate local scratch: this reads the
                # actual archive, not the still-present Syncthing ingress.
                scratch = state / ("restore-" + m["identity_sha256"])
                restored = replicate(archive / source.name, scratch, Path(config["rclone"]), workers=config.get("workers", 2))
                nas.unchanged(before)
                semantic = {}
                if m["context"].get("kind") == "ducklake_catalog_and_data":
                    from stockagent.control.lakehouse import restore_lake_delivery
                    import tempfile
                    cluster = Path(tempfile.mkdtemp(prefix="sa-lake-restore-"))
                    cluster.rmdir()  # canonical restore admits a fresh absent root
                    semantic = restore_lake_delivery(scratch, cluster, extensions=Path(config["extensions"]), pg_bin=Path(config["pg_bin"]))
                    atomic_write_json(state / (source.name + "-semantic.json"), semantic)
                proof.update({"nas_independent_restore_verified": True, "nas_mount_guard_verified": True,
                              "single_owner_verified": True, "runtime_lock_verified": True,
                              "restore_command_exit_codes": restored["command_exit_codes"],
                              "producer_device_id": config["producer_device_id"], "receiver_device_id": config["receiver_device_id"],
                              "catalog_semantic_restore_verified": semantic.get("catalog_semantic_restore_verified", False),
                              "scope": "fixed immutable lake delivery file recovery; catalog semantic restore separate"})
                proof["identity_sha256"] = identity_sha256(proof)
                receipts = Path(config["receipt_root"])
                receipts.mkdir(parents=True, exist_ok=True)
                atomic_write_json(receipts / (source.name + ".json"), proof, durable=True)
                atomic_write_json(state / (source.name + ".json"), proof, durable=True)
                # Only this exact verified scratch; never ingress/archive.
                if verify(scratch) != m:
                    raise ValueError("scratch changed before cleanup")
                shutil.rmtree(scratch)
                rows.append({"delivery": source.name, "state": "accepted"})
            except Exception as error:
                rows.append({"delivery": source.name, "state": "deferred", "error_type": type(error).__name__})
        result = {"state": "ready" if all(r["state"] == "accepted" for r in rows) else "degraded",
                  "observed_at_utc": datetime.now(timezone.utc).isoformat(), "jobs": rows,
                  "nas_mount_guard_verified": True, "single_owner_verified": True, "runtime_lock_verified": True,
                  "producer_device_id": config["producer_device_id"], "receiver_device_id": config["receiver_device_id"],
                  "automatic_archive_deletion": False}
        atomic_write_json(Path(config["receipt_root"]) / "relay-status.json", result, durable=True)
        return result
