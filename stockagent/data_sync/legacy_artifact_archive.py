"""Byte-preserving, non-deployable archive for allowlisted legacy artifacts.

This does not assert training completeness. The packed store holds deterministic
per-file encoded payloads; a second manifest retains every original path and
SHA-256 so recovery can be independently checked without a giant tarball.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import shutil
import stat
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    _safe_relative_path,
    atomic_write_json,
    sha256_file,
    validate_slug,
)
from stockagent.data_sync.packed_snapshots import (
    _load_inventory,
    fetch_packed_snapshot,
    publish_packed_snapshot,
    resolve_latest_packed,
    verify_packed_snapshot,
)


ARCHIVE_SCHEMA = 1
COPY_CHUNK = 8 * 1024 * 1024
COMPRESS_MIN_BYTES = 8 * 1024 * 1024
C_TEMP_STAGE_ROOT = Path("/var/lib/stockagent-legacy-archive-stage")


@dataclass(frozen=True)
class LegacyArchiveSpec:
    dataset: str
    relative_root: str
    minimum_stable_days: int
    stage_root: Path
    manual_capture_min_stable_hours: float | None = None
    durable_staging: bool = True


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
        manual_hours = item.get("manual_capture_min_stable_hours")
        durable_staging = item.get("durable_staging", True)
        if type(durable_staging) is not bool:
            raise SnapshotError('invalid staging durability option')
        stage_root = Path(item.get("stage_root", ""))
        if (
            item.get("archive_only") is not True
            or item.get("compression") != "gzip-1-csv-over-8m"
            or not isinstance(days, int)
            or days < 7
            or not stage_root.is_absolute()
        ):
            raise SnapshotError("invalid legacy archive safety contract")
        if manual_hours is not None and (
            type(manual_hours) not in (int, float)
            or not math.isfinite(manual_hours)
            or manual_hours < 12
        ):
            raise SnapshotError("manual capture requires an allowlisted stability floor of at least 12 hours")
        if dataset in result or any(x.relative_root == relative.as_posix() for x in result.values()):
            raise SnapshotError("duplicate legacy archive dataset/root")
        result[dataset] = LegacyArchiveSpec(dataset, relative.as_posix(), days, stage_root, manual_hours, durable_staging)
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


def source_plan(
    source: Path, spec: LegacyArchiveSpec, *, manual_capture: bool = False
) -> list[dict[str, Any]]:
    if type(manual_capture) is not bool:
        raise SnapshotError("manual capture must be an explicit boolean")
    if manual_capture and (
        type(spec.manual_capture_min_stable_hours) not in (int, float)
        or not math.isfinite(spec.manual_capture_min_stable_hours)
        or spec.manual_capture_min_stable_hours < 12
    ):
        raise SnapshotError("manual capture is not allowlisted for this source")
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError(f"legacy source file missing or root is not a real directory: {source}")
    stability_hours = (
        spec.manual_capture_min_stable_hours if manual_capture
        else spec.minimum_stable_days * 24
    )
    age_limit = time.time_ns() - int(stability_hours * 3_600_000_000_000)
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
                raise SnapshotError(f"legacy source is not stable for {stability_hours} hours: {path}")
            rows.append({"path": path.relative_to(source).as_posix(), "source": _source_signature(info)})
    if not rows:
        raise SnapshotError("legacy source has no files")
    return rows


def _paths(spec: LegacyArchiveSpec, artifact_root: Path) -> tuple[Path, Path, Path]:
    source = artifact_root / spec.relative_root
    stage = spec.stage_root / spec.dataset
    archive = stage / "archive"
    if (spec.stage_root.is_relative_to(Path("/srv/stockagent-d-volume"))
        or spec.stage_root == C_TEMP_STAGE_ROOT):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount

        _check_d_primary_mount(Path("/srv/stockagent-packed"))
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


def _encode_one(source_file: Path, target: Path, codec: str, *, durable: bool = True) -> str:
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
            if durable:
                os.fsync(output.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return digest.hexdigest()


def _manual_capture_usage_gate(source: Path, artifact_root: Path) -> None:
    from stockagent.data_sync.artifact_consumers import artifact_service_references

    references = artifact_process_references(source, artifact_root / "markets")
    services = artifact_service_references((source,), artifact_root.parent)[str(source)]
    if references or services:
        raise SnapshotError(f"manual capture source is in use: {(references + services)[:3]}")


def prepare_archive(
    spec: LegacyArchiveSpec, artifact_root: Path, *, manual_capture: bool = False
) -> dict[str, Any]:
    artifact_root = artifact_root.resolve()
    if not spec.durable_staging and not manual_capture:
        raise SnapshotError('rebuildable staging requires explicit manual capture; cold publication remains durable')
    source, stage, archive = _paths(spec, artifact_root)
    if spec.stage_root == C_TEMP_STAGE_ROOT:
        available = shutil.disk_usage(spec.stage_root.parent).free
        planned_bytes = sum(row["source"]["size"] for row in source_plan(source, spec, manual_capture=manual_capture))
        if available < planned_bytes + 32 * 1024**3:
            raise SnapshotError("C archive scratch lacks source-size budget plus 32 GiB reserve")
        # WSL's virtual ext4 free space is not the Windows backing volume's
        # free space. Budget both before a new scratch wave can grow its VHDX.
        windows_c = Path("/mnt/c")
        if os.path.ismount(windows_c) and shutil.disk_usage(windows_c).free < planned_bytes + 32 * 1024**3:
            raise SnapshotError("Windows C backing volume lacks scratch budget plus 32 GiB reserve")
    references = artifact_process_references(source, artifact_root / "markets")
    if references:
        raise SnapshotError(f"legacy source has process references: {references[:3]}")
    planned = source_plan(source, spec, manual_capture=manual_capture)
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root)
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
            try:
                row = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                row = {}
            if not isinstance(row, dict):
                row = {}
            if (
                row.get("path") == relative
                and row.get("source") == signature
                and row.get("encoded_path") == encoded_path
                and encoded_file.is_file()
                and sha256_file(encoded_file) == row.get("encoded_sha256")
            ):
                result_rows.append(row)
                continue
        original_hash = (_encode_one(source_file, encoded_file, codec) if spec.durable_staging else
                         _encode_one(source_file, encoded_file, codec, durable=False))
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
        if spec.durable_staging:
            atomic_write_json(receipt_path, row)
        else:
            atomic_write_json(receipt_path, row, durable=False)
        result_rows.append(row)
    # Per-file checks alone miss an earlier file changing, or a new path
    # appearing, while a later file is being encoded. Require the whole tree
    # to remain identical before exposing the preservation manifest.
    if source_plan(source, spec, manual_capture=manual_capture) != planned:
        raise SnapshotError("legacy source changed during staging; stage retained for audit")
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root)
    manifest = {
        "schema_version": ARCHIVE_SCHEMA,
        "dataset": spec.dataset,
        "relative_root": spec.relative_root,
        "deployable": False,
        "completion_claim": "not_checked",
        "files": result_rows,
    }
    if manual_capture:
        manifest["source_stability"] = {
            "mode": "manual-exact-capture-v1",
            "minimum_stable_hours": spec.manual_capture_min_stable_hours,
            "source_plan_sha256": hashlib.sha256(json.dumps(planned, sort_keys=True).encode()).hexdigest(),
        }
    atomic_write_json(archive / "legacy_archive_manifest.json", manifest)
    return manifest


def verify_archive_directory(
    archive: Path, source: Path | None = None, *, spec: LegacyArchiveSpec | None = None,
    manual_capture: bool = False, artifact_root: Path | None = None,
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
    capture = manifest.get("source_stability")
    if capture is not None and (
        not isinstance(capture, dict)
        or capture.get("mode") != "manual-exact-capture-v1"
        or spec is None
        or spec.manual_capture_min_stable_hours is None
        or capture.get("minimum_stable_hours") != spec.manual_capture_min_stable_hours
    ):
        raise SnapshotError("manual capture archive differs from allowlisted stability contract")
    if manual_capture and capture is None:
        raise SnapshotError("manual capture requires a matching captured archive")
    source_spec = spec or LegacyArchiveSpec(
        manifest["dataset"], manifest["relative_root"], 7, archive.parent
    )
    before_source = source_plan(source, source_spec, manual_capture=manual_capture) if source is not None else None
    if manual_capture and source is not None:
        if artifact_root is None:
            raise SnapshotError("manual capture verification requires the original artifact root")
        _manual_capture_usage_gate(source, artifact_root.resolve())
    rows = manifest.get("files")
    if not isinstance(rows, list) or not rows:
        raise SnapshotError("legacy archive has no file inventory")
    if capture is not None and capture.get("source_plan_sha256") != hashlib.sha256(
        json.dumps([{"path": row["path"], "source": row["source"]} for row in rows], sort_keys=True).encode()
    ).hexdigest():
        raise SnapshotError("manual capture source-plan digest differs from archive inventory")
    expected_encoded = {"legacy_archive_manifest.json"}
    expected_original: set[str] = set()
    source_metadata_drift: list[dict[str, Any]] = []
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
            # An inode's ctime also changes when another verified hard-link
            # name is removed. Historical inode/ctime are observations, not
            # recoverable content. Require exact portable metadata + bytes,
            # and compare the *current* complete signature before/after hashing
            # to retain the mutation/race gate without rejecting alias cleanup.
            before = _source_signature(original.lstat())
            original_digest = sha256_file(original)
            after = _source_signature(original.lstat())
            if (before != after or original_digest != digest
                or any(before[key] != row["source"][key] for key in ("size", "mtime_ns", "mode"))):
                raise SnapshotError(f"legacy source differs: {relative}")
            changed = [key for key in ("device", "inode", "ctime_ns") if before[key] != row["source"][key]]
            if changed:
                source_metadata_drift.append({"path": relative, "changed_observations": changed,
                                              "current_signature": before, "exact_sha256": original_digest})
        total += size
    observed = {p.relative_to(archive).as_posix() for p in archive.rglob("*") if p.is_file()}
    if observed != expected_encoded:
        raise SnapshotError("legacy encoded tree has extra or missing files")
    if source is not None:
        after_source = source_plan(source, source_spec, manual_capture=manual_capture)
        observed_source = {row["path"] for row in after_source}
        if observed_source != expected_original:
            raise SnapshotError("legacy source has extra or missing files")
        if before_source != after_source:
            raise SnapshotError("legacy source changed during verification")
        if manual_capture:
            _manual_capture_usage_gate(source, artifact_root.resolve())
    return {"dataset": manifest["dataset"], "files": len(rows), "original_bytes": total, "manifest": manifest,
            "source_metadata_drift": source_metadata_drift}


def publish_archive(
    spec: LegacyArchiveSpec, artifact_root: Path, sync_root: Path, *, repo_root: Path,
    manual_capture: bool = False,
):
    if spec.stage_root == C_TEMP_STAGE_ROOT and sync_root.resolve() != Path("/srv/stockagent-packed"):
        raise SnapshotError("C scratch may publish only into the guarded D canonical cold store")
    manifest = prepare_archive(spec, artifact_root, manual_capture=manual_capture)
    source, _stage, archive = _paths(spec, artifact_root.resolve())
    proof = verify_archive_directory(archive, source, spec=spec, manual_capture=manual_capture,
                                     artifact_root=artifact_root)
    baseline = source_plan(source, spec, manual_capture=manual_capture)
    def source_guard():
        if source_plan(source, spec, manual_capture=manual_capture) != baseline:
            raise SnapshotError("legacy source changed before cold commit")
        if manual_capture:
            _manual_capture_usage_gate(source, artifact_root.resolve())
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
        source_guard=source_guard,
    )
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive)
    # Publication may have been slow. Never declare the hot source preserved
    # if it changed after the archive verification but before the cold commit.
    verify_archive_directory(archive, source, spec=spec, manual_capture=manual_capture,
                             artifact_root=artifact_root)
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "source_files": proof["files"], "source_bytes": proof["original_bytes"], "cold_bytes": resolved.manifest["archive"]["stored_bytes"]}


def verify_cold_archive(spec: LegacyArchiveSpec, sync_root: Path) -> dict[str, Any]:
    """Explicit full recovery check without retaining a second cold copy on C.

    Uses the canonical fetch into a private, bounded verification scratch tree;
    never restores an artifact path or changes a managed current link/lease.
    A failed recovery keeps the scratch for diagnosis; only a fully verified,
    exclusively created temporary tree is removed.
    """
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    metadata = resolved.manifest.get("metadata", {})
    if (metadata.get("transport_role") != "legacy-quarantine-archive"
        or metadata.get("deployable") != "false"
        or metadata.get("source_relative_root") != spec.relative_root):
        raise SnapshotError("cold release is not the allowlisted legacy archive")
    needed = int(resolved.manifest["source"]["logical_bytes"])
    if shutil.disk_usage(Path(tempfile.gettempdir())).free < needed + 32 * 1024**3:
        raise SnapshotError("verification scratch lacks bounded space plus 32 GiB reserve")
    temporary = Path(tempfile.mkdtemp(prefix="stockagent-legacy-verify-"))
    try:
        archive = fetch_packed_snapshot(sync_root, temporary, resolved)
        if sha256_file(archive / "legacy_archive_manifest.json") != metadata.get("legacy_manifest_sha256"):
            raise SnapshotError("reconstructed legacy manifest differs from cold metadata")
        proof = verify_archive_directory(archive, spec=spec)
        inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
        verify_packed_snapshot(sync_root, resolved,
                               reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"])
    except Exception as exc:
        raise SnapshotError(f"cold verification failed; scratch retained at {temporary}: {exc}") from exc
    shutil.rmtree(temporary)
    proof.update(snapshot_id=resolved.manifest["snapshot_id"],
                 manifest_sha256=resolved.manifest_sha256, cold_verified=True,
                 decoded_originals_verified=True, verification_scratch_removed=True)
    return proof


def plan_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path
) -> dict[str, Any]:
    """Audit only the bounded C encoding scratch after exact hot retirement."""
    if spec.stage_root != C_TEMP_STAGE_ROOT:
        raise SnapshotError("stage pruning is restricted to the catalog's bounded C temporary root")
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.materialized_cache import process_references, _pinned_snapshot_ids
    from stockagent.data_sync.artifact_retirement import _reclaimable_file_bytes

    _check_d_primary_mount(sync_root)
    source = artifact_root.resolve() / spec.relative_root
    if source.exists() or source.is_symlink():
        raise SnapshotError("C stage pruning requires completed hot-source retirement")
    stage = spec.stage_root / spec.dataset
    if stage.is_symlink() or stage.resolve() != stage or not stage.is_dir():
        raise SnapshotError("C stage is missing or redirected")
    state = json.loads((state_root / "retirements" / f"{spec.dataset}.json").read_text())
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    if (state.get("state") != "cold-only" or state.get("dataset") != spec.dataset
        or state.get("relative_root") != spec.relative_root
        or state.get("snapshot_id") != resolved.manifest["snapshot_id"]):
        raise SnapshotError("C stage retirement state differs from the selected cold release")
    if str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized")):
        raise SnapshotError("C stage release is pinned")
    archive = stage / "archive"
    proof = verify_archive_directory(archive, spec=spec)
    if sha256_file(archive / "legacy_archive_manifest.json") != resolved.manifest.get("metadata", {}).get("legacy_manifest_sha256"):
        raise SnapshotError("C stage manifest differs from the cold release")
    inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive,
                           reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"])
    rows = proof["manifest"]["files"]
    if json.loads((stage / "source_plan.json").read_text()) != [
        {"path": row["path"], "source": row["source"]} for row in rows
    ]:
        raise SnapshotError("C stage source-plan control file differs")
    expected = {"source_plan.json", "archive/legacy_archive_manifest.json"}
    for row in rows:
        expected.add("archive/" + row["encoded_path"])
        key = hashlib.sha256(row["path"].encode()).hexdigest()
        name = "receipts/" + key + ".json"
        if json.loads((stage / name).read_text()) != row:
            raise SnapshotError("C stage per-file receipt differs")
        expected.add(name)
    signatures = []
    for parent, dirs, files in os.walk(stage, followlinks=False):
        if any((Path(parent) / name).is_symlink() for name in dirs):
            raise SnapshotError("C stage contains a symlink directory")
        for name in files:
            path = Path(parent) / name
            if not stat.S_ISREG(path.lstat().st_mode):
                raise SnapshotError("C stage contains a non-regular file")
            signatures.append({"path": path.relative_to(stage).as_posix(), "source": _source_signature(path.lstat())})
    signatures.sort(key=lambda row: row["path"])
    if {row["path"] for row in signatures} != expected:
        raise SnapshotError("C stage has unknown or missing files; preserve for audit")
    if process_references(stage):
        raise SnapshotError("C stage has live process references")
    identity = {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"],
                "manifest_sha256": resolved.manifest_sha256, "signatures": signatures}
    return {"dataset": spec.dataset, "stage": str(stage), "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256, "apply_ready": True,
            "stage_fingerprint": hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
            "reclaimable_allocated_file_bytes": _reclaimable_file_bytes(stage), "cold_objects_deleted": 0}


def _stage_prune_fingerprint(stage: Path, plan: dict[str, Any]) -> str:
    signatures = []
    for parent, dirs, files in os.walk(stage, followlinks=False):
        if any((Path(parent) / name).is_symlink() for name in dirs):
            raise SnapshotError("C stage contains a symlink directory")
        for name in files:
            path = Path(parent) / name
            if not stat.S_ISREG(path.lstat().st_mode):
                raise SnapshotError("C stage contains a non-regular file")
            signatures.append({"path": path.relative_to(stage).as_posix(), "source": _source_signature(path.lstat())})
    signatures.sort(key=lambda row: row["path"])
    identity = {"dataset": plan["dataset"], "snapshot_id": plan["snapshot_id"],
                "manifest_sha256": plan["manifest_sha256"], "signatures": signatures}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def apply_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path,
    expected_fingerprint: str,
) -> dict[str, Any]:
    with _exclusive_lock(state_root / "retirements" / f"{spec.dataset}.lock"):
        return _apply_archive_stage_prune(spec, sync_root, artifact_root=artifact_root,
                                          state_root=state_root, expected_fingerprint=expected_fingerprint)


def _apply_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path,
    expected_fingerprint: str,
) -> dict[str, Any]:
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.materialized_cache import process_references, _pinned_snapshot_ids

    plan = plan_archive_stage_prune(spec, sync_root, artifact_root=artifact_root, state_root=state_root)
    if plan["stage_fingerprint"] != expected_fingerprint:
        raise SnapshotError("C stage changed after dry run")
    stage = Path(plan["stage"])
    quarantine = stage.with_name(f".{spec.dataset}.pruning-{os.getpid()}")
    if quarantine.exists() or quarantine.is_symlink():
        raise SnapshotError("unfinished C stage quarantine requires audit")
    os.replace(stage, quarantine)
    if _stage_prune_fingerprint(quarantine, plan) != expected_fingerprint:
        raise SnapshotError("C stage changed during rename; quarantine retained")
    verify_archive_directory(quarantine / "archive", spec=spec)
    _check_d_primary_mount(sync_root)
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
    verify_packed_snapshot(sync_root, resolved,
                           reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"])
    source = artifact_root.resolve() / spec.relative_root
    if (resolved.manifest_sha256 != plan["manifest_sha256"] or process_references(quarantine)
        or process_references(stage) or source.exists() or source.is_symlink()
        or str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized"))
        or _stage_prune_fingerprint(quarantine, plan) != expected_fingerprint):
        raise SnapshotError("C stage final gates changed; quarantine retained")
    shutil.rmtree(quarantine)
    return {**plan, "pruned": True, "reclaimed_allocated_file_bytes": plan["reclaimable_allocated_file_bytes"]}


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
        os.utime(target, ns=(target.stat().st_atime_ns, row["source"]["mtime_ns"]))
    atomic_write_json(
        temporary / ".LEGACY_RESTORED.json",
        {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "deployable": False},
    )
    if destination.exists() or destination.is_symlink():
        raise SnapshotError(f"restore destination appeared during verification: {destination}")
    os.replace(temporary, destination)
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "destination": str(destination), "files": proof["files"], "original_bytes": proof["original_bytes"]}
