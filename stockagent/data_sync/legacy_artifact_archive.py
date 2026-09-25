"""Byte-preserving, non-deployable archive for allowlisted legacy artifacts.

This does not assert training completeness. The packed store holds deterministic
per-file encoded payloads; a second manifest retains every original path and
SHA-256 so recovery can be independently checked without a giant tarball.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import stat
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _safe_relative_path,
    atomic_write_json,
    sha256_file,
    validate_slug,
)
from stockagent.data_sync.packed_snapshots import (
    fetch_packed_snapshot,
    publish_packed_snapshot,
    resolve_latest_packed,
    verify_packed_snapshot,
)


ARCHIVE_SCHEMA = 1
COPY_CHUNK = 8 * 1024 * 1024
COMPRESS_MIN_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class LegacyArchiveSpec:
    dataset: str
    relative_root: str
    minimum_stable_days: int
    stage_root: Path


def load_legacy_specs(path: Path) -> dict[str, LegacyArchiveSpec]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1 or raw.get("authority_node_id") != "penguin":
        raise SnapshotError("legacy archive catalog authority/schema mismatch")
    rows = raw.get("archives")
    if not isinstance(rows, list):
        raise SnapshotError("legacy archive catalog is not a list")
    result: dict[str, LegacyArchiveSpec] = {}
    for item in rows:
        dataset = validate_slug(item.get("dataset", ""), "legacy dataset")
        relative = _safe_relative_path(item.get("relative_root", ""), "legacy root")
        if relative.parts[0] != "markets" or len(relative.parts) < 2:
            raise SnapshotError("legacy archives must be scoped under markets")
        days = item.get("minimum_stable_days")
        stage_root = Path(item.get("stage_root", ""))
        if (
            item.get("archive_only") is not True
            or item.get("compression") != "gzip-1-csv-over-8m"
            or not isinstance(days, int)
            or days < 7
            or not stage_root.is_absolute()
        ):
            raise SnapshotError("invalid legacy archive safety contract")
        if dataset in result or any(x.relative_root == relative.as_posix() for x in result.values()):
            raise SnapshotError("duplicate legacy archive dataset/root")
        result[dataset] = LegacyArchiveSpec(dataset, relative.as_posix(), days, stage_root)
    return result


def _source_signature(info: os.stat_result) -> dict[str, int]:
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "size": info.st_size,
        "mtime_ns": info.st_mtime_ns,
        "ctime_ns": info.st_ctime_ns,
        "mode": stat.S_IMODE(info.st_mode),
    }


def source_plan(source: Path, spec: LegacyArchiveSpec) -> list[dict[str, Any]]:
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError(f"legacy source is not a real directory: {source}")
    age_limit = time.time_ns() - spec.minimum_stable_days * 86_400_000_000_000
    rows: list[dict[str, Any]] = []
    for parent, dirnames, filenames in os.walk(source, followlinks=False):
        dirnames.sort()
        filenames.sort()
        for name in dirnames:
            if (Path(parent) / name).is_symlink():
                raise SnapshotError("legacy source contains a symlink directory")
        for name in filenames:
            path = Path(parent) / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                raise SnapshotError(f"legacy source has non-regular file: {path}")
            if info.st_mtime_ns > age_limit:
                raise SnapshotError(f"legacy source is not stable for {spec.minimum_stable_days} days: {path}")
            rows.append({"path": path.relative_to(source).as_posix(), "source": _source_signature(info)})
    if not rows:
        raise SnapshotError("legacy source has no files")
    return rows


def _paths(spec: LegacyArchiveSpec, artifact_root: Path) -> tuple[Path, Path, Path]:
    source = artifact_root / spec.relative_root
    stage = spec.stage_root / spec.dataset
    archive = stage / "archive"
    if source.resolve() != source or stage.resolve(strict=False) != stage:
        raise SnapshotError("legacy archive root is redirected by a symlink")
    if source == archive or source in archive.parents or archive in source.parents:
        raise SnapshotError("legacy source and stage overlap")
    return source, stage, archive


def _decode_hash(path: Path, codec: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    if codec == "gzip":
        input_file = gzip.open(path, "rb")
    elif codec == "raw":
        input_file = path.open("rb")
    else:
        raise SnapshotError(f"unsupported legacy codec: {codec}")
    with input_file as stream:
        while chunk := stream.read(COPY_CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _encode_one(source_file: Path, target: Path, codec: str) -> str:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".partial")
    if temporary.exists():
        temporary.unlink()
    digest = hashlib.sha256()
    try:
        with source_file.open("rb") as raw, temporary.open("wb") as output:
            if codec == "gzip":
                encoded = gzip.GzipFile(filename="", mode="wb", fileobj=output, compresslevel=1, mtime=0)
            else:
                encoded = output
            try:
                while chunk := raw.read(COPY_CHUNK):
                    digest.update(chunk)
                    encoded.write(chunk)
            finally:
                if encoded is not output:
                    encoded.close()
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return digest.hexdigest()


def prepare_archive(spec: LegacyArchiveSpec, artifact_root: Path) -> dict[str, Any]:
    artifact_root = artifact_root.resolve()
    source, stage, archive = _paths(spec, artifact_root)
    references = artifact_process_references(source, artifact_root / "markets")
    if references:
        raise SnapshotError(f"legacy source has process references: {references[:3]}")
    planned = source_plan(source, spec)
    stage.mkdir(parents=True, exist_ok=True)
    plan_path = stage / "source_plan.json"
    if plan_path.exists():
        prior = json.loads(plan_path.read_text(encoding="utf-8"))
        if prior != planned:
            raise SnapshotError("legacy source changed since staging began; quarantine stage for audit")
    else:
        atomic_write_json(plan_path, planned)
    receipts = stage / "receipts"
    receipts.mkdir(exist_ok=True)
    result_rows: list[dict[str, Any]] = []
    for item in planned:
        relative = item["path"]
        source_file = source.joinpath(*PurePosixPath(relative).parts)
        signature = item["source"]
        if _source_signature(source_file.lstat()) != signature:
            raise SnapshotError(f"legacy source changed during staging: {source_file}")
        codec = "gzip" if relative.lower().endswith(".csv") and signature["size"] >= COMPRESS_MIN_BYTES else "raw"
        key = hashlib.sha256(relative.encode("utf-8")).hexdigest()
        encoded_path = f"payload/{key[:2]}/{key}.{'gz' if codec == 'gzip' else 'raw'}"
        encoded_file = archive / encoded_path
        receipt_path = receipts / f"{key}.json"
        if receipt_path.is_file():
            row = json.loads(receipt_path.read_text(encoding="utf-8"))
            if (
                row.get("path") == relative
                and row.get("source") == signature
                and row.get("encoded_path") == encoded_path
                and encoded_file.is_file()
                and sha256_file(encoded_file) == row.get("encoded_sha256")
            ):
                result_rows.append(row)
                continue
        original_hash = _encode_one(source_file, encoded_file, codec)
        if _source_signature(source_file.lstat()) != signature:
            raise SnapshotError(f"legacy source changed during compression: {source_file}")
        row = {
            "path": relative,
            "source": signature,
            "original_sha256": original_hash,
            "codec": codec,
            "encoded_path": encoded_path,
            "encoded_sha256": sha256_file(encoded_file),
            "encoded_size": encoded_file.stat().st_size,
        }
        atomic_write_json(receipt_path, row)
        result_rows.append(row)
    manifest = {
        "schema_version": ARCHIVE_SCHEMA,
        "dataset": spec.dataset,
        "relative_root": spec.relative_root,
        "deployable": False,
        "completion_claim": "not_checked",
        "files": result_rows,
    }
    atomic_write_json(archive / "legacy_archive_manifest.json", manifest)
    return manifest


def verify_archive_directory(
    archive: Path, source: Path | None = None, *, spec: LegacyArchiveSpec | None = None
) -> dict[str, Any]:
    manifest = json.loads((archive / "legacy_archive_manifest.json").read_text(encoding="utf-8"))
    if (
        manifest.get("schema_version") != ARCHIVE_SCHEMA
        or manifest.get("deployable") is not False
        or manifest.get("completion_claim") != "not_checked"
    ):
        raise SnapshotError("invalid legacy archive manifest")
    if spec is not None and (
        manifest.get("dataset") != spec.dataset
        or manifest.get("relative_root") != spec.relative_root
    ):
        raise SnapshotError("legacy archive identity differs from allowlisted source")
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise SnapshotError("legacy archive has no file inventory")
    expected_encoded = {"legacy_archive_manifest.json"}
    expected_original: set[str] = set()
    total = 0
    for row in rows:
        relative = _safe_relative_path(row["path"], "legacy archived path").as_posix()
        encoded = _safe_relative_path(row["encoded_path"], "legacy payload path").as_posix()
        if relative in expected_original or encoded in expected_encoded:
            raise SnapshotError("duplicate legacy archive path")
        expected_original.add(relative)
        expected_encoded.add(encoded)
        encoded_file = archive.joinpath(*PurePosixPath(encoded).parts)
        if (
            not encoded_file.is_file()
            or encoded_file.is_symlink()
            or archive.resolve() not in encoded_file.resolve().parents
        ):
            raise SnapshotError(f"legacy encoded file missing: {encoded}")
        if encoded_file.stat().st_size != row["encoded_size"] or sha256_file(encoded_file) != row["encoded_sha256"]:
            raise SnapshotError(f"legacy encoded file differs: {encoded}")
        digest, size = _decode_hash(encoded_file, row["codec"])
        if digest != row["original_sha256"] or size != row["source"]["size"]:
            raise SnapshotError(f"legacy decode proof differs: {relative}")
        if source is not None:
            original = source.joinpath(*PurePosixPath(relative).parts)
            if original.is_symlink() or not original.is_file():
                raise SnapshotError(f"legacy source file missing: {relative}")
            if _source_signature(original.lstat()) != row["source"] or sha256_file(original) != digest:
                raise SnapshotError(f"legacy source differs: {relative}")
        total += size
    observed = {p.relative_to(archive).as_posix() for p in archive.rglob("*") if p.is_file()}
    if observed != expected_encoded:
        raise SnapshotError("legacy encoded tree has extra or missing files")
    if source is not None:
        source_spec = spec or LegacyArchiveSpec(
            manifest["dataset"], manifest["relative_root"], 7, archive.parent
        )
        observed_source = {row["path"] for row in source_plan(source, source_spec)}
        if observed_source != expected_original:
            raise SnapshotError("legacy source has extra or missing files")
    return {"dataset": manifest["dataset"], "files": len(rows), "original_bytes": total, "manifest": manifest}


def publish_archive(spec: LegacyArchiveSpec, artifact_root: Path, sync_root: Path, *, repo_root: Path):
    manifest = prepare_archive(spec, artifact_root)
    source, _stage, archive = _paths(spec, artifact_root.resolve())
    proof = verify_archive_directory(archive, source, spec=spec)
    resolved = publish_packed_snapshot(
        sync_root,
        spec.dataset,
        archive,
        loose_file_threshold_bytes=8 * 1024 * 1024,
        pack_buckets=32,
        metadata={
            "transport_role": "legacy-quarantine-archive",
            "deployable": "false",
            "completion_claim": "not_checked",
            "source_relative_root": spec.relative_root,
            "legacy_manifest_sha256": sha256_file(archive / "legacy_archive_manifest.json"),
        },
        repo_root=repo_root,
    )
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive)
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "source_files": proof["files"], "source_bytes": proof["original_bytes"], "cold_bytes": resolved.manifest["archive"]["stored_bytes"]}


def restore_archive(
    spec: LegacyArchiveSpec,
    sync_root: Path,
    destination: Path,
    *,
    materialized_root: Path,
) -> dict[str, Any]:
    if destination.exists() or destination.is_symlink():
        raise SnapshotError(f"refusing to overwrite existing restore path: {destination}")
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if (
        metadata.get("transport_role") != "legacy-quarantine-archive"
        or metadata.get("deployable") != "false"
        or metadata.get("source_relative_root") != spec.relative_root
    ):
        raise SnapshotError("release is not the selected legacy quarantine archive")
    archive = fetch_packed_snapshot(sync_root, materialized_root, resolved)
    if metadata.get("legacy_manifest_sha256") != sha256_file(
        archive / "legacy_archive_manifest.json"
    ):
        raise SnapshotError("restored legacy manifest differs from cold release metadata")
    proof = verify_archive_directory(archive, spec=spec)
    manifest = proof["manifest"]
    temporary = destination.with_name(destination.name + f".partial-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise SnapshotError(f"stale restore staging path needs audit: {temporary}")
    temporary.mkdir(parents=True, exist_ok=False)
    for row in manifest["files"]:
        relative = PurePosixPath(row["path"])
        target = temporary.joinpath(*relative.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = archive.joinpath(*PurePosixPath(row["encoded_path"]).parts)
        if row["codec"] == "gzip":
            reader = gzip.open(encoded, "rb")
        else:
            reader = encoded.open("rb")
        digest = hashlib.sha256()
        with reader as stream, target.open("xb") as output:
            while chunk := stream.read(COPY_CHUNK):
                output.write(chunk)
                digest.update(chunk)
        if digest.hexdigest() != row["original_sha256"] or target.stat().st_size != row["source"]["size"]:
            raise SnapshotError(f"restored legacy file differs: {relative}")
        target.chmod(row["source"]["mode"])
    atomic_write_json(
        temporary / ".LEGACY_RESTORED.json",
        {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "deployable": False},
    )
    if destination.exists() or destination.is_symlink():
        raise SnapshotError(f"restore destination appeared during verification: {destination}")
    os.replace(temporary, destination)
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "destination": str(destination), "files": proof["files"], "original_bytes": proof["original_bytes"]}
