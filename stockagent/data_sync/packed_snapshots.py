from __future__ import annotations

import contextlib
import dataclasses
import gzip
import hashlib
import json
import os
import re
import shutil
import stat
import time
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping

from stockagent.data_sync.desync_snapshots import (
    DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    HLC,
    ResolvedSnapshot,
    SnapshotError,
    _contains_git_metadata,
    _exclusive_lock,
    _fsync_directory,
    _git_state,
    _load_json,
    _path_under,
    _paths_overlap,
    _safe_relative_path,
    _utc_iso_from_ns,
    atomic_write_bytes,
    atomic_write_json,
    default_node_id,
    next_hlc,
    scan_tree,
    sha256_file,
    validate_slug,
    write_immutable_json,
)


PACKED_SNAPSHOT_SCHEMA_VERSION = 1
PACKED_HEAD_SCHEMA_VERSION = 1
DEFAULT_LOOSE_FILE_THRESHOLD_BYTES = 8 * 1024 * 1024
DEFAULT_PACK_BUCKETS = 64
DEFAULT_COMPRESSION_LEVEL = 6
PACKED_ARCHIVE_FORMAT = "stockagent-path-bucket-zip-v1"
INVENTORY_FORMAT = "jsonl-gzip-v1"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_PACK_STORED_SUFFIXES = {
    ".7z",
    ".bz2",
    ".feather",
    ".gif",
    ".gz",
    ".jpeg",
    ".jpg",
    ".lz4",
    ".npy",
    ".npz",
    ".orc",
    ".parquet",
    ".png",
    ".tar",
    ".webp",
    ".xz",
    ".zip",
    ".zst",
}
_STIGNORE = """// Node-local transactional state must never be replicated.
(?d).local-state
(?d).local-state/**
"""


@dataclasses.dataclass
class _SourceEntry:
    path: str
    kind: str
    mode: int
    size: int = 0
    target: str | None = None
    source_stat: tuple[int, int, int, int, int] | None = None
    sha256: str | None = None
    storage: dict[str, Any] | None = None

    def inventory_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "kind": self.kind,
            "mode": self.mode,
            "path": self.path,
        }
        if self.kind == "file":
            row.update(
                {
                    "sha256": self.sha256,
                    "size": self.size,
                    "storage": self.storage,
                }
            )
        elif self.kind == "symlink":
            row["target"] = self.target
        return row


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _validate_hash(value: Any, label: str) -> str:
    text = str(value)
    if not _HASH_RE.fullmatch(text):
        raise SnapshotError(f"{label} must be a lowercase SHA-256; got {value!r}")
    return text


def _source_stat(info: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        int(info.st_dev),
        int(info.st_ino),
        int(info.st_size),
        int(info.st_mtime_ns),
        int(info.st_ctime_ns),
    )


def _ensure_source_stat(path: Path, expected: _SourceEntry) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise SnapshotError(f"source entry disappeared while packing: {path}") from exc
    if not stat.S_ISREG(info.st_mode) or _source_stat(info) != expected.source_stat:
        raise SnapshotError(f"source file changed while packing: {path}")


def _validate_symlink_target(path: str, target: str) -> None:
    target_path = PurePosixPath(target)
    if target_path.is_absolute():
        raise SnapshotError(f"absolute symlink is not snapshot-safe: {path} -> {target}")
    depth = len(PurePosixPath(path).parent.parts)
    for part in target_path.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            depth -= 1
            if depth < 0:
                raise SnapshotError(
                    f"symlink escapes the snapshot root: {path} -> {target}"
                )
        else:
            depth += 1


def _collect_entries(source: Path) -> list[_SourceEntry]:
    entries: list[_SourceEntry] = []
    for current, dir_names, file_names in os.walk(
        source, topdown=True, followlinks=False
    ):
        dir_names.sort()
        file_names.sort()
        current_path = Path(current)
        for name in list(dir_names):
            path = current_path / name
            info = path.lstat()
            relative = path.relative_to(source).as_posix()
            if stat.S_ISLNK(info.st_mode):
                dir_names.remove(name)
                target = os.readlink(path)
                _validate_symlink_target(relative, target)
                entries.append(
                    _SourceEntry(
                        path=relative,
                        kind="symlink",
                        mode=stat.S_IMODE(info.st_mode),
                        target=target,
                    )
                )
            elif stat.S_ISDIR(info.st_mode):
                entries.append(
                    _SourceEntry(
                        path=relative,
                        kind="directory",
                        mode=stat.S_IMODE(info.st_mode),
                    )
                )
            else:
                raise SnapshotError(f"unsupported directory entry type: {path}")
        for name in file_names:
            path = current_path / name
            info = path.lstat()
            relative = path.relative_to(source).as_posix()
            if stat.S_ISREG(info.st_mode):
                entries.append(
                    _SourceEntry(
                        path=relative,
                        kind="file",
                        mode=stat.S_IMODE(info.st_mode),
                        size=int(info.st_size),
                        source_stat=_source_stat(info),
                    )
                )
            elif stat.S_ISLNK(info.st_mode):
                target = os.readlink(path)
                _validate_symlink_target(relative, target)
                entries.append(
                    _SourceEntry(
                        path=relative,
                        kind="symlink",
                        mode=stat.S_IMODE(info.st_mode),
                        target=target,
                    )
                )
            else:
                raise SnapshotError(f"special files are not snapshot-safe: {path}")
    entries.sort(key=lambda entry: entry.path)
    return entries


def _copy_and_hash(source: Path, destination: Path) -> str:
    digest = hashlib.sha256()
    with source.open("rb") as input_stream, destination.open("xb") as output_stream:
        while block := input_stream.read(8 * 1024 * 1024):
            digest.update(block)
            output_stream.write(block)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    return digest.hexdigest()


def _install_immutable_object(
    temporary: Path,
    destination: Path,
    *,
    expected_sha256: str,
) -> bool:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256_file(destination) != expected_sha256:
            raise SnapshotError(f"content-addressed object is corrupt: {destination}")
        temporary.unlink()
        return True
    os.replace(temporary, destination)
    _fsync_directory(destination.parent)
    return False


def _bucket_for_path(path: str, bucket_count: int) -> int:
    digest = hashlib.sha256(path.encode("utf-8", errors="surrogateescape")).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def _zip_info(entry: _SourceEntry) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(entry.path, date_time=(1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = (stat.S_IFREG | entry.mode) << 16
    info.compress_type = (
        zipfile.ZIP_STORED
        if Path(entry.path).suffix.lower() in _PACK_STORED_SUFFIXES
        else zipfile.ZIP_DEFLATED
    )
    return info


def _write_pack(
    source: Path,
    entries: list[_SourceEntry],
    temporary: Path,
    *,
    compression_level: int,
) -> None:
    with zipfile.ZipFile(
        temporary,
        mode="x",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=compression_level,
        allowZip64=True,
    ) as archive:
        for entry in entries:
            source_path = source.joinpath(*PurePosixPath(entry.path).parts)
            _ensure_source_stat(source_path, entry)
            digest = hashlib.sha256()
            with source_path.open("rb") as input_stream, archive.open(
                _zip_info(entry), mode="w", force_zip64=True
            ) as output_stream:
                while block := input_stream.read(8 * 1024 * 1024):
                    digest.update(block)
                    output_stream.write(block)
            _ensure_source_stat(source_path, entry)
            entry.sha256 = digest.hexdigest()


def _write_inventory(temporary: Path, entries: Iterable[_SourceEntry]) -> str:
    digest = hashlib.sha256()
    with temporary.open("xb") as raw_stream:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=6,
            fileobj=raw_stream,
            mtime=0,
        ) as stream:
            for entry in entries:
                stream.write(_canonical_json_bytes(entry.inventory_row()))
        raw_stream.flush()
        os.fsync(raw_stream.fileno())
    with temporary.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _object_relpath(kind: str, digest: str, suffix: str) -> PurePosixPath:
    return PurePosixPath("objects") / kind / digest[:2] / f"{digest}{suffix}"


def initialize_packed_layout(
    sync_root: Path,
    *,
    node_id: str | None = None,
    replace_ignore: bool = False,
    replace_node_id: bool = False,
) -> str:
    sync_root = sync_root.resolve()
    if git_root := _contains_git_metadata(sync_root):
        raise SnapshotError(
            f"sync root {sync_root} is inside Git worktree {git_root}; "
            "use a separate data-only directory"
        )
    resolved_node_id = validate_slug(node_id or default_node_id(), "node_id")
    for relative in (
        "heads",
        "manifests",
        "objects/blobs",
        "objects/packs",
        "objects/inventories",
        ".local-state/locks",
        ".local-state/staging",
    ):
        (sync_root / relative).mkdir(parents=True, exist_ok=True)
    ignore_path = sync_root / ".stignore"
    if replace_ignore or not ignore_path.exists():
        atomic_write_bytes(ignore_path, _STIGNORE.encode("utf-8"))
    node_path = sync_root / ".local-state" / "node-id"
    if node_path.exists():
        existing = validate_slug(
            node_path.read_text(encoding="utf-8").strip(), "stored node_id"
        )
        if node_id is None:
            return existing
        if existing != resolved_node_id and not replace_node_id:
            raise SnapshotError(
                f"node_id is already {existing}; refusing to change it to "
                f"{resolved_node_id} without --replace-node-id"
            )
    atomic_write_bytes(node_path, f"{resolved_node_id}\n".encode(), mode=0o600)
    return resolved_node_id


def _resolve_node_id(sync_root: Path, explicit: str | None = None) -> str:
    requested = validate_slug(explicit, "node_id") if explicit else None
    if from_env := os.environ.get("STOCKAGENT_SYNC_NODE_ID"):
        env_node = validate_slug(from_env, "STOCKAGENT_SYNC_NODE_ID")
        if requested is not None and requested != env_node:
            raise SnapshotError(
                f"--node-id {requested} disagrees with STOCKAGENT_SYNC_NODE_ID "
                f"{env_node}"
            )
        requested = env_node
    node_path = sync_root / ".local-state" / "node-id"
    try:
        stored = validate_slug(
            node_path.read_text(encoding="utf-8").strip(), "stored node_id"
        )
    except OSError as exc:
        raise SnapshotError(f"packed sync root is not initialized: {sync_root}") from exc
    if requested is not None and requested != stored:
        raise SnapshotError(
            f"requested node_id {requested} disagrees with initialized node_id {stored}"
        )
    return stored


def _validate_manifest(manifest: Mapping[str, Any]) -> HLC:
    if int(manifest.get("schema_version", -1)) != PACKED_SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotError("unsupported packed snapshot manifest schema")
    dataset = validate_slug(str(manifest.get("dataset", "")), "manifest dataset")
    snapshot_id = validate_slug(
        str(manifest.get("snapshot_id", "")), "manifest snapshot_id"
    )
    publisher = manifest.get("publisher")
    archive = manifest.get("archive")
    source = manifest.get("source")
    if not all(isinstance(value, Mapping) for value in (publisher, archive, source)):
        raise SnapshotError(f"manifest {snapshot_id} is incomplete")
    if archive.get("format") != PACKED_ARCHIVE_FORMAT:
        raise SnapshotError(f"manifest {snapshot_id} has unsupported archive format")
    publisher_node = validate_slug(
        str(publisher.get("node_id", "")), "publisher node_id"
    )
    stamp = HLC.from_mapping(manifest.get("hlc", {}))
    if stamp.node_id != publisher_node:
        raise SnapshotError(f"manifest {snapshot_id} has inconsistent publisher HLC")
    inventory = archive.get("inventory")
    objects = archive.get("objects")
    if not isinstance(inventory, Mapping) or not isinstance(objects, list):
        raise SnapshotError(f"manifest {snapshot_id} has invalid inventory/objects")
    _safe_relative_path(str(inventory.get("relpath", "")), "inventory relpath")
    _validate_hash(inventory.get("sha256"), "inventory SHA-256")
    seen_hashes: set[str] = set()
    for item in objects:
        if not isinstance(item, Mapping):
            raise SnapshotError(f"manifest {snapshot_id} contains an invalid object")
        digest = _validate_hash(item.get("sha256"), "object SHA-256")
        _safe_relative_path(str(item.get("relpath", "")), "object relpath")
        if item.get("kind") not in {"blob", "pack"}:
            raise SnapshotError(f"manifest {snapshot_id} has invalid object kind")
        if digest in seen_hashes:
            raise SnapshotError(f"manifest {snapshot_id} repeats object {digest}")
        seen_hashes.add(digest)
    fingerprint = str(source.get("portable_fingerprint_sha256", ""))
    _validate_hash(fingerprint, "source fingerprint")
    if not dataset:
        raise SnapshotError("manifest dataset is empty")
    return stamp


def _validate_object_presence(sync_root: Path, manifest: Mapping[str, Any]) -> None:
    archive = manifest["archive"]
    references = [archive["inventory"], *archive["objects"]]
    for item in references:
        path = _path_under(sync_root, str(item["relpath"]), "object relpath")
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise SnapshotError(f"snapshot object is not locally complete: {path}") from exc
        if size != int(item["bytes"]):
            raise SnapshotError(
                f"snapshot object size mismatch for {path}: "
                f"expected {item['bytes']}, got {size}"
            )


def _head_candidates(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
) -> tuple[list[ResolvedSnapshot], list[str], list[tuple[HLC | None, str]]]:
    candidates: list[ResolvedSnapshot] = []
    diagnostics: list[str] = []
    incomplete: list[tuple[HLC | None, str]] = []
    future_limit_ns = int(max_clock_skew_seconds) * 1_000_000_000
    for head_path in sorted((sync_root / "heads" / dataset).glob("*.json")):
        head_stamp: HLC | None = None
        canonical = False
        try:
            head = _load_json(head_path)
            if int(head.get("schema_version", -1)) != PACKED_HEAD_SCHEMA_VERSION:
                raise SnapshotError("unsupported packed head schema")
            if validate_slug(str(head.get("dataset", "")), "head dataset") != dataset:
                raise SnapshotError("head dataset differs from requested dataset")
            node_id = validate_slug(str(head.get("node_id", "")), "head node_id")
            canonical = head_path.name == f"{node_id}.json"
            if not canonical:
                raise SnapshotError("head filename does not match its node_id")
            head_stamp = HLC.from_mapping(head.get("hlc", {}))
            if head_stamp.physical_ns - now_ns > future_limit_ns:
                raise SnapshotError(
                    f"head from {node_id} is in the future; repair clock synchronization"
                )
            manifest_path = _path_under(
                sync_root, str(head.get("manifest_relpath", "")), "manifest relpath"
            )
            manifest_sha = _validate_hash(
                head.get("manifest_sha256"), "manifest SHA-256"
            )
            if sha256_file(manifest_path) != manifest_sha:
                raise SnapshotError("manifest checksum mismatch")
            manifest = _load_json(manifest_path)
            manifest_stamp = _validate_manifest(manifest)
            if manifest_stamp != head_stamp:
                raise SnapshotError("head and manifest HLC differ")
            if manifest.get("snapshot_id") != head.get("snapshot_id"):
                raise SnapshotError("head and manifest snapshot IDs differ")
            _validate_object_presence(sync_root, manifest)
            candidates.append(
                ResolvedSnapshot(
                    manifest=manifest,
                    manifest_path=manifest_path,
                    manifest_sha256=manifest_sha,
                    head_path=head_path,
                )
            )
        except (OSError, SnapshotError, TypeError, ValueError) as exc:
            message = f"{head_path.name}: {exc}"
            diagnostics.append(message)
            if canonical or (head_stamp is None and ".sync-conflict-" not in head_path.name):
                incomplete.append((head_stamp, message))
    return candidates, diagnostics, incomplete


def resolve_latest_packed(
    sync_root: Path,
    dataset: str,
    *,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    now_ns: int | None = None,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    current_ns = time.time_ns() if now_ns is None else int(now_ns)
    candidates, diagnostics, incomplete = _head_candidates(
        sync_root,
        dataset,
        now_ns=current_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
    )
    if not candidates:
        detail = "; ".join(diagnostics) if diagnostics else "no per-node heads found"
        raise SnapshotError(f"no valid packed snapshot for {dataset}: {detail}")
    winner = max(
        candidates,
        key=lambda item: (
            HLC.from_mapping(item.manifest["hlc"]),
            str(item.manifest["snapshot_id"]),
        ),
    )
    winner_stamp = HLC.from_mapping(winner.manifest["hlc"])
    blocking = [message for stamp, message in incomplete if stamp is None or stamp >= winner_stamp]
    if blocking:
        raise SnapshotError(
            "newest canonical packed head is not locally complete; refusing to fall "
            f"back to {winner.manifest['snapshot_id']}: {'; '.join(blocking)}"
        )
    return dataclasses.replace(winner, diagnostics=tuple(diagnostics))


def resolve_packed_snapshot_id(
    sync_root: Path, dataset: str, snapshot_id: str
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    dataset = validate_slug(dataset, "dataset")
    snapshot_id = validate_slug(snapshot_id, "snapshot_id")
    manifest_path = sync_root / "manifests" / dataset / f"{snapshot_id}.json"
    manifest_sha = sha256_file(manifest_path)
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    if manifest.get("dataset") != dataset or manifest.get("snapshot_id") != snapshot_id:
        raise SnapshotError(f"manifest identity mismatch: {manifest_path}")
    _validate_object_presence(sync_root, manifest)
    return ResolvedSnapshot(
        manifest=manifest,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        head_path=None,
    )


def _observed_stamps(
    sync_root: Path,
    dataset: str,
    *,
    now_ns: int,
    max_clock_skew_seconds: int,
) -> list[HLC]:
    candidates, diagnostics, incomplete = _head_candidates(
        sync_root,
        dataset,
        now_ns=now_ns,
        max_clock_skew_seconds=max_clock_skew_seconds,
    )
    blocking = [message for _, message in incomplete]
    blocking.extend(item for item in diagnostics if "in the future" in item)
    if blocking:
        raise SnapshotError("; ".join(dict.fromkeys(blocking)))
    return [HLC.from_mapping(item.manifest["hlc"]) for item in candidates]


def publish_packed_snapshot(
    sync_root: Path,
    dataset: str,
    source: Path,
    *,
    node_id: str | None = None,
    loose_file_threshold_bytes: int = DEFAULT_LOOSE_FILE_THRESHOLD_BYTES,
    pack_buckets: int = DEFAULT_PACK_BUCKETS,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
    max_clock_skew_seconds: int = DEFAULT_MAX_CLOCK_SKEW_SECONDS,
    metadata: Mapping[str, str] | None = None,
    repo_root: Path | None = None,
) -> ResolvedSnapshot:
    sync_root = sync_root.resolve()
    source = source.resolve()
    dataset = validate_slug(dataset, "dataset")
    if _paths_overlap(sync_root, source):
        raise SnapshotError("snapshot source and packed sync root must not overlap")
    if loose_file_threshold_bytes < 1:
        raise SnapshotError("loose_file_threshold_bytes must be positive")
    if not 1 <= pack_buckets <= 4096:
        raise SnapshotError("pack_buckets must be between 1 and 4096")
    if not 0 <= compression_level <= 9:
        raise SnapshotError("compression_level must be between 0 and 9")
    node_identity = sync_root / ".local-state" / "node-id"
    initialize_packed_layout(
        sync_root,
        node_id=node_id if not node_identity.exists() else None,
    )
    publisher_node = _resolve_node_id(sync_root, node_id)
    lock_path = sync_root / ".local-state" / "locks" / f"publish-{dataset}.lock"

    with _exclusive_lock(lock_path):
        before = scan_tree(source)
        entries = _collect_entries(source)
        staging_root = sync_root / ".local-state" / "staging"
        staging_root.mkdir(parents=True, exist_ok=True)
        objects: list[dict[str, Any]] = []

        large_files = [
            entry
            for entry in entries
            if entry.kind == "file" and entry.size >= loose_file_threshold_bytes
        ]
        for entry in large_files:
            source_path = source.joinpath(*PurePosixPath(entry.path).parts)
            _ensure_source_stat(source_path, entry)
            temporary = staging_root / f"blob-{uuid.uuid4().hex}.partial"
            digest = _copy_and_hash(source_path, temporary)
            _ensure_source_stat(source_path, entry)
            relpath = _object_relpath("blobs", digest, ".blob")
            destination = sync_root.joinpath(*relpath.parts)
            _install_immutable_object(temporary, destination, expected_sha256=digest)
            entry.sha256 = digest
            entry.storage = {
                "kind": "blob",
                "object_sha256": digest,
            }
            objects.append(
                {
                    "bytes": entry.size,
                    "file_count": 1,
                    "kind": "blob",
                    "logical_bytes": entry.size,
                    "relpath": relpath.as_posix(),
                    "sha256": digest,
                }
            )

        buckets: dict[int, list[_SourceEntry]] = {}
        for entry in entries:
            if entry.kind == "file" and entry.size < loose_file_threshold_bytes:
                bucket = _bucket_for_path(entry.path, pack_buckets)
                buckets.setdefault(bucket, []).append(entry)
        for bucket, bucket_entries in sorted(buckets.items()):
            temporary = staging_root / f"pack-{bucket:04d}-{uuid.uuid4().hex}.partial"
            _write_pack(
                source,
                bucket_entries,
                temporary,
                compression_level=compression_level,
            )
            digest = sha256_file(temporary)
            relpath = _object_relpath("packs", digest, ".zip")
            destination = sync_root.joinpath(*relpath.parts)
            size = temporary.stat().st_size
            _install_immutable_object(temporary, destination, expected_sha256=digest)
            logical_bytes = sum(entry.size for entry in bucket_entries)
            for entry in bucket_entries:
                entry.storage = {
                    "kind": "pack",
                    "member": entry.path,
                    "object_sha256": digest,
                }
            objects.append(
                {
                    "bucket": bucket,
                    "bytes": size,
                    "file_count": len(bucket_entries),
                    "kind": "pack",
                    "logical_bytes": logical_bytes,
                    "relpath": relpath.as_posix(),
                    "sha256": digest,
                }
            )

        for entry in entries:
            if entry.kind == "file" and (not entry.sha256 or not entry.storage):
                raise SnapshotError(f"file was not assigned to a packed object: {entry.path}")

        inventory_temp = staging_root / f"inventory-{uuid.uuid4().hex}.partial"
        inventory_sha = _write_inventory(inventory_temp, entries)
        inventory_relpath = _object_relpath(
            "inventories", inventory_sha, ".jsonl.gz"
        )
        inventory_path = sync_root.joinpath(*inventory_relpath.parts)
        inventory_bytes = inventory_temp.stat().st_size
        _install_immutable_object(
            inventory_temp, inventory_path, expected_sha256=inventory_sha
        )

        after = scan_tree(source)
        if before["stability_fingerprint_sha256"] != after["stability_fingerprint_sha256"]:
            raise SnapshotError(
                "source tree changed while it was being packed; publish from a frozen "
                "snapshot or under the downloader's dataset lock"
            )

        wall_time_ns = time.time_ns()
        stamp = next_hlc(
            _observed_stamps(
                sync_root,
                dataset,
                now_ns=wall_time_ns,
                max_clock_skew_seconds=max_clock_skew_seconds,
            ),
            node_id=publisher_node,
            now_ns=wall_time_ns,
        )
        seconds, nanos = divmod(stamp.physical_ns, 1_000_000_000)
        timestamp_slug = time.strftime("%Y%m%dT%H%M%S", time.gmtime(seconds)) + f"{nanos:09d}Z"
        snapshot_id = validate_slug(
            f"{dataset[:40]}-{timestamp_slug}-l{stamp.logical}-"
            f"{publisher_node[:40]}-{inventory_sha[:16]}",
            "snapshot_id",
        )
        unique_objects: dict[str, dict[str, Any]] = {}
        for item in objects:
            digest = str(item["sha256"])
            existing = unique_objects.get(digest)
            if existing is None:
                unique_objects[digest] = item
                continue
            if any(
                existing[key] != item[key] for key in ("bytes", "kind", "relpath")
            ):
                raise SnapshotError(f"inconsistent duplicate object descriptor: {digest}")
            existing["file_count"] += int(item["file_count"])
            existing["logical_bytes"] += int(item["logical_bytes"])
        objects = sorted(
            unique_objects.values(),
            key=lambda item: (str(item["kind"]), str(item["sha256"])),
        )
        manifest: dict[str, Any] = {
            "schema_version": PACKED_SNAPSHOT_SCHEMA_VERSION,
            "dataset": dataset,
            "snapshot_id": snapshot_id,
            "published_at": _utc_iso_from_ns(wall_time_ns),
            "hlc": stamp.to_dict(),
            "publisher": {"node_id": publisher_node},
            "source": {
                "snapshot_root_name": source.name,
                **{
                    key: value
                    for key, value in before.items()
                    if key != "stability_fingerprint_sha256"
                },
            },
            "archive": {
                "format": PACKED_ARCHIVE_FORMAT,
                "inventory": {
                    "bytes": inventory_bytes,
                    "format": INVENTORY_FORMAT,
                    "relpath": inventory_relpath.as_posix(),
                    "sha256": inventory_sha,
                },
                "loose_file_threshold_bytes": loose_file_threshold_bytes,
                "pack_buckets": pack_buckets,
                "compression": "deflate-or-store-by-extension",
                "compression_level": compression_level,
                "objects": objects,
                "object_count": len(objects),
                "stored_bytes": sum(int(item["bytes"]) for item in objects)
                + inventory_bytes,
            },
            "metadata": dict(sorted((metadata or {}).items())),
        }
        if git_state := _git_state(repo_root):
            manifest["git"] = git_state
        manifest_relpath = PurePosixPath("manifests") / dataset / f"{snapshot_id}.json"
        manifest_path = sync_root.joinpath(*manifest_relpath.parts)
        manifest_sha = write_immutable_json(manifest_path, manifest)
        head = {
            "schema_version": PACKED_HEAD_SCHEMA_VERSION,
            "dataset": dataset,
            "node_id": publisher_node,
            "snapshot_id": snapshot_id,
            "hlc": stamp.to_dict(),
            "updated_at": _utc_iso_from_ns(wall_time_ns),
            "manifest_relpath": manifest_relpath.as_posix(),
            "manifest_sha256": manifest_sha,
        }
        head_path = sync_root / "heads" / dataset / f"{publisher_node}.json"
        atomic_write_json(head_path, head)
        return ResolvedSnapshot(
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_sha256=manifest_sha,
            head_path=head_path,
        )


def _load_inventory(sync_root: Path, manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    descriptor = manifest["archive"]["inventory"]
    path = _path_under(sync_root, str(descriptor["relpath"]), "inventory relpath")
    expected_sha = _validate_hash(descriptor["sha256"], "inventory SHA-256")
    if sha256_file(path) != expected_sha:
        raise SnapshotError(f"inventory checksum mismatch: {path}")
    entries: list[dict[str, Any]] = []
    previous_path = ""
    try:
        with gzip.open(path, mode="rt", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise SnapshotError(f"inventory row {line_number} is not an object")
                relative = _safe_relative_path(
                    str(row.get("path", "")), f"inventory row {line_number} path"
                ).as_posix()
                if relative <= previous_path:
                    raise SnapshotError("inventory paths are duplicated or not sorted")
                previous_path = relative
                row["path"] = relative
                entries.append(row)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotError(f"cannot read packed inventory {path}: {exc}") from exc
    return entries


def _validate_inventory(
    manifest: Mapping[str, Any], entries: list[dict[str, Any]]
) -> dict[str, Any]:
    object_by_hash = {
        str(item["sha256"]): item for item in manifest["archive"]["objects"]
    }
    counts = {"files": 0, "directories": 0, "symlinks": 0, "logical_bytes": 0}
    object_members: dict[str, list[str]] = {digest: [] for digest in object_by_hash}
    for row in entries:
        kind = row.get("kind")
        mode = int(row.get("mode", -1))
        if not 0 <= mode <= 0o7777:
            raise SnapshotError(f"invalid mode for inventory path {row['path']}")
        if kind == "directory":
            counts["directories"] += 1
            continue
        if kind == "symlink":
            target = str(row.get("target", ""))
            _validate_symlink_target(str(row["path"]), target)
            counts["symlinks"] += 1
            continue
        if kind != "file":
            raise SnapshotError(f"invalid inventory kind for {row['path']}: {kind!r}")
        size = int(row.get("size", -1))
        digest = _validate_hash(row.get("sha256"), "file SHA-256")
        storage = row.get("storage")
        if size < 0 or not isinstance(storage, Mapping):
            raise SnapshotError(f"invalid file inventory row for {row['path']}")
        object_sha = _validate_hash(storage.get("object_sha256"), "storage SHA-256")
        if object_sha not in object_by_hash:
            raise SnapshotError(f"inventory references an unknown object: {object_sha}")
        storage_kind = storage.get("kind")
        if storage_kind != object_by_hash[object_sha]["kind"]:
            raise SnapshotError(f"inventory/object kind mismatch for {row['path']}")
        if storage_kind == "pack" and storage.get("member") != row["path"]:
            raise SnapshotError(f"pack member/path mismatch for {row['path']}")
        if storage_kind == "blob" and digest != object_sha:
            raise SnapshotError(f"blob/file checksum mismatch for {row['path']}")
        if storage_kind not in {"pack", "blob"}:
            raise SnapshotError(f"invalid storage kind for {row['path']}")
        object_members[object_sha].append(str(row["path"]))
        counts["files"] += 1
        counts["logical_bytes"] += size
        row["sha256"] = digest
    expected = manifest["source"]
    for key, value in counts.items():
        if int(expected.get(key, -1)) != value:
            raise SnapshotError(
                f"inventory {key} mismatch: expected {expected.get(key)}, got {value}"
            )
    for digest, item in object_by_hash.items():
        members = object_members[digest]
        if len(members) != int(item["file_count"]):
            raise SnapshotError(f"object file count mismatch: {digest}")
        if item["kind"] == "blob" and not members:
            raise SnapshotError(f"blob object has no inventory references: {digest}")
    return {"counts": counts, "object_members": object_members}


def verify_packed_snapshot(
    sync_root: Path,
    resolved: ResolvedSnapshot,
    *,
    materialized_path: Path | None = None,
) -> dict[str, Any]:
    sync_root = sync_root.resolve()
    manifest = resolved.manifest
    _validate_manifest(manifest)
    if sha256_file(resolved.manifest_path) != resolved.manifest_sha256:
        raise SnapshotError(f"manifest changed after resolution: {resolved.manifest_path}")
    _validate_object_presence(sync_root, manifest)
    entries = _load_inventory(sync_root, manifest)
    inventory_result = _validate_inventory(manifest, entries)
    verified_bytes = 0
    for item in manifest["archive"]["objects"]:
        path = _path_under(sync_root, str(item["relpath"]), "object relpath")
        actual = sha256_file(path)
        if actual != item["sha256"]:
            raise SnapshotError(
                f"packed object checksum mismatch: expected {item['sha256']}, got {actual}"
            )
        verified_bytes += int(item["bytes"])
        if item["kind"] == "pack":
            expected_names = inventory_result["object_members"][str(item["sha256"])]
            try:
                with zipfile.ZipFile(path, mode="r") as archive:
                    names = archive.namelist()
                    bad_member = archive.testzip()
            except (OSError, zipfile.BadZipFile) as exc:
                raise SnapshotError(f"invalid ZIP pack {path}: {exc}") from exc
            if names != expected_names:
                raise SnapshotError(f"ZIP member list differs from inventory: {path}")
            if bad_member is not None:
                raise SnapshotError(f"ZIP CRC check failed for {bad_member} in {path}")
    result: dict[str, Any] = {
        "snapshot_id": manifest["snapshot_id"],
        "manifest_sha256": resolved.manifest_sha256,
        "inventory_sha256": manifest["archive"]["inventory"]["sha256"],
        "objects": len(manifest["archive"]["objects"]),
        "verified_object_bytes": verified_bytes,
        "materialized_verified": False,
    }
    if materialized_path is not None:
        _verify_materialized(materialized_path, manifest, entries)
        result["materialized_verified"] = True
    return result


def _copy_member_and_verify(
    input_stream: Any,
    destination: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    mode: int,
) -> None:
    digest = hashlib.sha256()
    copied = 0
    with destination.open("xb") as output_stream:
        while block := input_stream.read(8 * 1024 * 1024):
            copied += len(block)
            digest.update(block)
            output_stream.write(block)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    if copied != expected_size or digest.hexdigest() != expected_sha256:
        raise SnapshotError(f"extracted file checksum mismatch: {destination}")
    destination.chmod(mode)


def _verify_materialized(
    materialized_path: Path,
    manifest: Mapping[str, Any],
    entries: list[dict[str, Any]],
) -> None:
    for row in entries:
        path = materialized_path.joinpath(*PurePosixPath(row["path"]).parts)
        kind = row["kind"]
        if kind == "directory" and not path.is_dir():
            raise SnapshotError(f"materialized directory is missing: {path}")
        if kind == "symlink":
            if not path.is_symlink() or os.readlink(path) != row["target"]:
                raise SnapshotError(f"materialized symlink differs: {path}")
        if kind == "file":
            if not path.is_file() or path.stat().st_size != int(row["size"]):
                raise SnapshotError(f"materialized file differs: {path}")
            if sha256_file(path) != row["sha256"]:
                raise SnapshotError(f"materialized file checksum mismatch: {path}")
    inventory = scan_tree(materialized_path)
    expected = manifest["source"]["portable_fingerprint_sha256"]
    if inventory["portable_fingerprint_sha256"] != expected:
        raise SnapshotError(
            f"materialized fingerprint mismatch: expected {expected}, "
            f"got {inventory['portable_fingerprint_sha256']}"
        )


def fetch_packed_snapshot(
    sync_root: Path,
    materialized_root: Path,
    resolved: ResolvedSnapshot,
) -> Path:
    sync_root = sync_root.resolve()
    materialized_root = materialized_root.resolve()
    if _paths_overlap(sync_root, materialized_root):
        raise SnapshotError("materialized root must be outside the packed sync root")
    manifest = resolved.manifest
    dataset = validate_slug(str(manifest["dataset"]), "dataset")
    snapshot_id = validate_slug(str(manifest["snapshot_id"]), "snapshot_id")
    dataset_root = materialized_root / dataset
    target = dataset_root / snapshot_id
    ready_path = dataset_root / f".{snapshot_id}.READY.json"
    lock_path = materialized_root / ".locks" / f"fetch-{dataset}-{snapshot_id}.lock"
    with _exclusive_lock(lock_path):
        verification = verify_packed_snapshot(sync_root, resolved)
        entries = _load_inventory(sync_root, manifest)
        _validate_inventory(manifest, entries)
        if target.exists():
            _verify_materialized(target, manifest, entries)
            if not ready_path.exists():
                atomic_write_json(
                    ready_path,
                    {
                        "snapshot_id": snapshot_id,
                        "manifest_sha256": resolved.manifest_sha256,
                        "verified_at": _utc_iso_from_ns(time.time_ns()),
                    },
                )
            return target
        dataset_root.mkdir(parents=True, exist_ok=True)
        staging = dataset_root / f".{snapshot_id}.partial.{uuid.uuid4().hex}"
        staging.mkdir()
        try:
            object_by_hash = {
                str(item["sha256"]): item for item in manifest["archive"]["objects"]
            }
            for row in entries:
                if row["kind"] == "directory":
                    path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                    path.mkdir(parents=True, exist_ok=True)
                    path.chmod(int(row["mode"]))
            for row in entries:
                path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                if row["kind"] == "symlink":
                    path.symlink_to(str(row["target"]))
            blob_rows = [
                row
                for row in entries
                if row["kind"] == "file" and row["storage"]["kind"] == "blob"
            ]
            for row in blob_rows:
                storage = row["storage"]
                object_item = object_by_hash[str(storage["object_sha256"])]
                object_path = _path_under(
                    sync_root, str(object_item["relpath"]), "object relpath"
                )
                path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                with object_path.open("rb") as input_stream:
                    _copy_member_and_verify(
                        input_stream,
                        path,
                        expected_size=int(row["size"]),
                        expected_sha256=str(row["sha256"]),
                        mode=int(row["mode"]),
                    )
            pack_rows: dict[str, list[dict[str, Any]]] = {}
            for row in entries:
                if row["kind"] == "file" and row["storage"]["kind"] == "pack":
                    object_sha = str(row["storage"]["object_sha256"])
                    pack_rows.setdefault(object_sha, []).append(row)
            for object_sha, rows in pack_rows.items():
                object_item = object_by_hash[object_sha]
                object_path = _path_under(
                    sync_root, str(object_item["relpath"]), "object relpath"
                )
                with zipfile.ZipFile(object_path, mode="r") as archive:
                    for row in rows:
                        path = staging.joinpath(*PurePosixPath(row["path"]).parts)
                        with archive.open(
                            str(row["storage"]["member"]), mode="r"
                        ) as input_stream:
                            _copy_member_and_verify(
                                input_stream,
                                path,
                                expected_size=int(row["size"]),
                                expected_sha256=str(row["sha256"]),
                                mode=int(row["mode"]),
                            )
            _verify_materialized(staging, manifest, entries)
            os.replace(staging, target)
            _fsync_directory(dataset_root)
        except Exception:
            # Keep the partial tree for diagnosis; it is never treated as ready.
            raise
        atomic_write_json(
            ready_path,
            {
                "snapshot_id": snapshot_id,
                "manifest_sha256": resolved.manifest_sha256,
                "inventory_sha256": verification["inventory_sha256"],
                "verified_at": _utc_iso_from_ns(time.time_ns()),
            },
        )
        return target


def write_packed_pin(path: Path, resolved: ResolvedSnapshot) -> None:
    atomic_write_json(
        path,
        {
            "schema_version": 1,
            "resolved_at": _utc_iso_from_ns(time.time_ns()),
            "manifest_sha256": resolved.manifest_sha256,
            "manifest": resolved.manifest,
        },
    )


def referenced_packed_objects(sync_root: Path) -> set[Path]:
    """Return objects referenced by valid immutable manifests without deleting any."""

    sync_root = sync_root.resolve()
    referenced: set[Path] = set()
    for manifest_path in sorted((sync_root / "manifests").glob("*/*.json")):
        try:
            manifest = _load_json(manifest_path)
            _validate_manifest(manifest)
        except (OSError, SnapshotError):
            continue
        inventory = manifest["archive"]["inventory"]
        referenced.add(
            _path_under(sync_root, str(inventory["relpath"]), "inventory relpath")
        )
        for item in manifest["archive"]["objects"]:
            referenced.add(_path_under(sync_root, str(item["relpath"]), "object relpath"))
    return referenced
