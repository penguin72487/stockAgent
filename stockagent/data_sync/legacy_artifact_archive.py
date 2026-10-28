"""Byte-preserving, non-deployable archive for allowlisted legacy artifacts.

This does not assert training completeness. The packed store holds deterministic
per-file encoded payloads; a second manifest retains every original path and
SHA-256 so recovery can be independently checked without a giant tarball.
"""

from __future__ import annotations

import base64
import binascii
import gzip
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
import hashlib
import itertools
import json
import math
import os
import re
import resource
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from stockagent.data_sync.artifact_maintenance import artifact_process_references
from stockagent.data_sync.desync_snapshots import (
    SnapshotError,
    _exclusive_lock,
    _iter_lstat_bounded,
    _raise_walk_error,
    _safe_relative_path,
    atomic_write_json,
    sha256_file,
    validate_slug,
)
from stockagent.data_sync.packed_snapshots import (
    _load_inventory,
    _verified_packed_inventory,
    _verify_materialized,
    fetch_packed_snapshot,
    publish_packed_snapshot,
    resolve_latest_packed,
    resolve_packed_snapshot_id,
    verify_packed_snapshot,
)
from stockagent.data_sync.packed_backup import PinnedObjectSignatures


ARCHIVE_SCHEMA = 1
LINK_ARCHIVE_SCHEMA = 2
OPAQUE_SYMLINK_POLICY = "completed-benchmark-opaque-symlinks-v1"
COPY_CHUNK = 8 * 1024 * 1024
COMPRESS_MIN_BYTES = 8 * 1024 * 1024
DEFAULT_COMPRESSION_PROFILE = "gzip-1-csv-over-8m"
ADAPTIVE_COMPRESSION_PROFILE = "gzip-1-adaptive-over-8m"
ZSTD_COMPRESSION_PROFILE = "zstd-1-adaptive-over-8m"
ZSTD3_COMPRESSION_PROFILE = "zstd-3-adaptive-over-8m"
COMPRESSION_PROFILES = {DEFAULT_COMPRESSION_PROFILE, ADAPTIVE_COMPRESSION_PROFILE,
                        ZSTD_COMPRESSION_PROFILE, ZSTD3_COMPRESSION_PROFILE}
CODEC_SUFFIXES = {"raw": "raw", "gzip": "gz", "zstd": "zst"}
C_TEMP_STAGE_ROOT = Path("/var/lib/stockagent-legacy-archive-stage")
D_BENCHMARK_STAGE_ROOT = Path("/srv/stockagent-d-volume/stockagent-completed-benchmark-scratch")
D_OFFLINE_STAGE_ROOT = Path("/srv/stockagent-d-volume/stockagent-completed-offline-scratch")
MANUAL_WSL_CAPTURE_CONTRACT = "manual-wsl-artifact-preservation-v1"
MANUAL_WSL_SCOPES = {"cache", "replays", "audits", "datasets", "data_repair", "maintenance", "operations", "benchmarks"}
MANUAL_OFFLINE_SIMULATION_CONTRACT = "manual-wsl-offline-simulation-preservation-v1"
MANUAL_TRANSFER_QUARANTINE_CONTRACT = "manual-wsl-retired-transfer-preservation-v1"
MANUAL_MINUTE_DERIVED_VIEW_CONTRACT = "manual-wsl-retired-minute-view-preservation-v1"
MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT = "manual-vast-offline-artifact-preservation-v1"
VAST_OFFLINE_SCOPES = frozenset({"smoke", "operations"})
RETIRED_MINUTE_DERIVED_VIEWS = frozenset({
    "data_tw_minute/research_dataset_developing_v5",
    "data_tw_minute/research_dataset_schema2_volume_bug_20260807",
})
OFFLINE_SIMULATION_PREFIXES = ("official-close-settlement-", "account-addition-",
                               "recompute-all-", "tw-day-trade-independent-")


def reviewed_wsl_artifact_root(relative: str) -> bool:
    """An explicit case, never an entire benchmark suite or namespace."""
    from stockagent.data_sync.benchmark_fixtures import standalone_benchmark_contract
    path = _safe_relative_path(relative, "reviewed WSL artifact root")
    return (len(path.parts) >= 2 and path.parts[0] in MANUAL_WSL_SCOPES
            and (path.parts[0] != "benchmarks" or len(path.parts) == 3
                 or (len(path.parts) == 2
                     and standalone_benchmark_contract(path.parts[1]) is not None)))


def reviewed_offline_scratch_root(relative: str) -> bool:
    """One explicitly selected operations case, never the namespace or raw data."""
    return relative.startswith("operations/") and reviewed_wsl_artifact_root(relative)


def reviewed_offline_simulation_root(relative: str) -> bool:
    path = _safe_relative_path(relative, "offline simulation root")
    return (len(path.parts) == 2 and path.parts[0] == "live"
            and any(path.parts[1].startswith(prefix) and len(path.parts[1]) > len(prefix)
                    for prefix in OFFLINE_SIMULATION_PREFIXES))


def reviewed_transfer_quarantine_root(relative: str) -> bool:
    path = _safe_relative_path(relative, "retired transport root")
    return (len(path.parts) == 2 and path.parts[0] == "stockagent-transfer-quarantine"
            and re.fullmatch(r"\d{4}-\d{2}-\d{2}_ssh_transport", path.parts[1]) is not None)


def reviewed_minute_derived_view_root(relative: str) -> bool:
    return _safe_relative_path(relative, "retired minute derived view").as_posix() in RETIRED_MINUTE_DERIVED_VIEWS


def reviewed_vast_offline_root(relative: str) -> bool:
    path = _safe_relative_path(relative, "reviewed Vast offline artifact root")
    return len(path.parts) == 2 and path.parts[0] in VAST_OFFLINE_SCOPES


@dataclass(frozen=True)
class LegacyArchiveSpec:
    dataset: str
    relative_root: str
    minimum_stable_days: int
    stage_root: Path
    manual_capture_min_stable_hours: float | None = None
    durable_staging: bool = True
    compression_profile: str = DEFAULT_COMPRESSION_PROFILE
    capture_contract: str | None = None
    symlink_policy: str | None = None


def _opaque_symlinks_enabled(spec: LegacyArchiveSpec | None) -> bool:
    """An explicit format opt-in, only for manually captured benchmark cases."""
    if spec is None or spec.symlink_policy is None:
        return False
    if (spec.symlink_policy != OPAQUE_SYMLINK_POLICY
            or spec.capture_contract != MANUAL_WSL_CAPTURE_CONTRACT
            or not spec.relative_root.startswith("benchmarks/")
            or not reviewed_wsl_artifact_root(spec.relative_root)
            or type(spec.manual_capture_min_stable_hours) not in (int, float)
            or not math.isfinite(spec.manual_capture_min_stable_hours)
            or spec.manual_capture_min_stable_hours < 12):
        raise SnapshotError("opaque symlinks require an explicitly reviewed manual benchmark capture")
    return True


def legacy_process_scope(spec: LegacyArchiveSpec | None, artifact_root: Path) -> Path:
    if spec is not None and spec.capture_contract == MANUAL_MINUTE_DERIVED_VIEW_CONTRACT:
        if not reviewed_minute_derived_view_root(spec.relative_root):
            raise SnapshotError("minute view process scope requires its reviewed root")
        return artifact_root / "data_tw_minute"
    return artifact_root


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
        capture_contract = item.get("capture_contract")
        if capture_contract not in {None, MANUAL_WSL_CAPTURE_CONTRACT, MANUAL_OFFLINE_SIMULATION_CONTRACT,
                                    MANUAL_TRANSFER_QUARANTINE_CONTRACT, MANUAL_MINUTE_DERIVED_VIEW_CONTRACT,
                                    MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT}:
            raise SnapshotError("unknown manual artifact preservation contract")
        allowed = {"markets", "ablations"}
        if capture_contract == MANUAL_WSL_CAPTURE_CONTRACT:
            if relative.parts[0] in MANUAL_WSL_SCOPES and not reviewed_wsl_artifact_root(relative.as_posix()):
                raise SnapshotError("manual WSL capture requires one reviewed artifact case")
            allowed |= MANUAL_WSL_SCOPES
        if capture_contract == MANUAL_OFFLINE_SIMULATION_CONTRACT:
            if not reviewed_offline_simulation_root(relative.as_posix()):
                raise SnapshotError("offline simulation capture excludes canonical live ledgers")
            allowed = {"live"}
        if capture_contract == MANUAL_TRANSFER_QUARANTINE_CONTRACT:
            if not reviewed_transfer_quarantine_root(relative.as_posix()):
                raise SnapshotError("retired transport capture requires one explicitly dated quarantine root")
            allowed = {"stockagent-transfer-quarantine"}
        if capture_contract == MANUAL_MINUTE_DERIVED_VIEW_CONTRACT:
            if not reviewed_minute_derived_view_root(relative.as_posix()):
                raise SnapshotError("retired minute view capture excludes current originals and other data roots")
            allowed = {"data_tw_minute"}
        if capture_contract == MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT:
            if not reviewed_vast_offline_root(relative.as_posix()):
                raise SnapshotError("Vast offline capture requires one explicitly reviewed smoke/operations root")
            allowed = VAST_OFFLINE_SCOPES
        if relative.parts[0] not in allowed or len(relative.parts) < 2:
            raise SnapshotError("legacy archives must be scoped under markets or ablations")
        days = item.get("minimum_stable_days")
        manual_hours = item.get("manual_capture_min_stable_hours")
        durable_staging = item.get("durable_staging", True)
        if type(durable_staging) is not bool:
            raise SnapshotError('invalid staging durability option')
        stage_root = Path(item.get("stage_root", ""))
        if (
            item.get("archive_only") is not True
            or not isinstance(item.get("compression"), str)
            or item.get("compression") not in COMPRESSION_PROFILES
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
        if capture_contract is not None and manual_hours is None:
            raise SnapshotError("manual artifact preservation requires an explicit stability floor")
        if dataset in result or any(x.relative_root == relative.as_posix() for x in result.values()):
            raise SnapshotError("duplicate legacy archive dataset/root")
        spec = LegacyArchiveSpec(dataset, relative.as_posix(), days, stage_root, manual_hours, durable_staging,
                                 item["compression"], capture_contract, item.get("symlink_policy"))
        _opaque_symlinks_enabled(spec)
        result[dataset] = spec
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


def _validated_symlinks(rows: Any) -> list[dict[str, Any]]:
    """Validate opaque link bytes without ever resolving their targets."""
    if not isinstance(rows, list):
        raise SnapshotError("legacy symlink inventory is invalid")
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or set(row) != {"path", "target_b64", "source"}:
            raise SnapshotError("legacy symlink descriptor is invalid")
        relative = _safe_relative_path(row["path"], "legacy symlink").as_posix()
        signature = row["source"]
        if (row["path"] != relative or relative in seen or not isinstance(signature, dict)
                or set(signature) != {"device", "inode", "size", "mtime_ns", "ctime_ns", "mode"}
                or any(type(value) is not int or value < 0 for value in signature.values())
                or signature["mode"] != 0o777 or signature["size"] > 1024 * 1024
                or not isinstance(row["target_b64"], str)
                or len(row["target_b64"]) != 4 * ((signature["size"] + 2) // 3)):
            raise SnapshotError("legacy symlink descriptor is invalid")
        try:
            target = base64.b64decode(row["target_b64"], validate=True)
        except (ValueError, binascii.Error) as error:
            raise SnapshotError("legacy symlink target encoding is invalid") from error
        if (not target or b"\0" in target or len(target) != signature["size"]
                or base64.b64encode(target).decode("ascii") != row["target_b64"]):
            raise SnapshotError("legacy symlink target differs from its descriptor")
        seen.add(relative)
    if [row["path"] for row in rows] != sorted(seen):
        raise SnapshotError("legacy symlink inventory is not uniquely sorted")
    return rows


def _assert_internal_symlink_targets(rows: list[dict[str, Any]], original_source: Path) -> None:
    """Retire only links whose textual targets stay in the captured root.

    Existing targets and dangling names are treated identically. This is a
    lexical check, not permission to open or follow a target.
    """
    root = os.fsencode(str(original_source.absolute()))
    for row in _validated_symlinks(rows):
        target = base64.b64decode(row["target_b64"], validate=True)
        # '..' is interpreted after resolving a preceding symlink component.
        # A lexical commonpath check alone would therefore miss an escape.
        if b".." in target.split(b"/"):
            raise SnapshotError("legacy symlink target contains a parent traversal")
        parent = os.fsencode(str(PurePosixPath(row["path"]).parent))
        endpoint = os.path.normpath(target if target.startswith(b"/") else os.path.join(root, parent, target))
        if os.path.commonpath((root, endpoint)) != root:
            raise SnapshotError("legacy symlink target is outside the captured benchmark root")


def _archive_symlinks(source: Path, spec: LegacyArchiveSpec, *,
                      original_source: Path | None = None) -> list[dict[str, Any]]:
    """Freeze link names, target bytes and lstat identities, without following."""
    if not _opaque_symlinks_enabled(spec):
        return []
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError("legacy symlink inventory source is not a real directory")
    device = source.stat().st_dev
    age_limit = time.time_ns() - int(spec.manual_capture_min_stable_hours * 3_600_000_000_000)
    rows = []
    for parent, dirs, files in os.walk(source, followlinks=False, onerror=_raise_walk_error):
        for name in sorted(dirs + files):
            path = Path(parent) / name
            before = path.lstat()
            if not stat.S_ISLNK(before.st_mode):
                continue
            if before.st_dev != device or before.st_nlink != 1 or before.st_mtime_ns > age_limit:
                raise SnapshotError("legacy symlink is unstable or crosses a filesystem")
            target = os.readlink(os.fsencode(path))
            after = path.lstat()
            if not stat.S_ISLNK(after.st_mode) or _source_signature(before) != _source_signature(after):
                raise SnapshotError("legacy symlink changed during inventory")
            rows.append({"path": path.relative_to(source).as_posix(),
                         "target_b64": base64.b64encode(target).decode("ascii"),
                         "source": _source_signature(after)})
    rows.sort(key=lambda row: row["path"])
    _validated_symlinks(rows)
    _assert_internal_symlink_targets(rows, original_source or source)
    return rows


def source_plan(
    source: Path, spec: LegacyArchiveSpec, *, manual_capture: bool = False,
    original_source: Path | None = None,
) -> list[dict[str, Any]]:
    if type(manual_capture) is not bool:
        raise SnapshotError("manual capture must be an explicit boolean")
    if spec.capture_contract is not None and not manual_capture:
        raise SnapshotError("reviewed WSL artifacts require explicit manual capture; no automatic enrollment")
    if (spec.capture_contract == MANUAL_WSL_CAPTURE_CONTRACT
            and spec.relative_root.split("/", 1)[0] in MANUAL_WSL_SCOPES
            and not reviewed_wsl_artifact_root(spec.relative_root)):
        raise SnapshotError("manual WSL capture requires one reviewed artifact case")
    if (spec.capture_contract == MANUAL_VAST_OFFLINE_CAPTURE_CONTRACT
            and not reviewed_vast_offline_root(spec.relative_root)):
        raise SnapshotError("Vast offline capture is outside its reviewed namespaces")
    if (spec.capture_contract == MANUAL_OFFLINE_SIMULATION_CONTRACT
            and not reviewed_offline_simulation_root(spec.relative_root)):
        raise SnapshotError("offline simulation capture excludes canonical live ledgers")
    if (spec.capture_contract == MANUAL_TRANSFER_QUARANTINE_CONTRACT
            and not reviewed_transfer_quarantine_root(spec.relative_root)):
        raise SnapshotError("retired transport capture excludes live producer and other /srv roots")
    if (spec.capture_contract == MANUAL_MINUTE_DERIVED_VIEW_CONTRACT
            and not reviewed_minute_derived_view_root(spec.relative_root)):
        raise SnapshotError("retired minute view capture excludes current originals and other data roots")
    if manual_capture and (
        type(spec.manual_capture_min_stable_hours) not in (int, float)
        or not math.isfinite(spec.manual_capture_min_stable_hours)
        or spec.manual_capture_min_stable_hours < 12
    ):
        raise SnapshotError("manual capture is not allowlisted for this source")
    if source.is_symlink() or not source.is_dir():
        raise SnapshotError(f"legacy source file missing or root is not a real directory: {source}")
    if (spec.capture_contract == MANUAL_WSL_CAPTURE_CONTRACT
            and spec.relative_root.split("/", 1)[0] == "benchmarks"):
        from stockagent.data_sync.benchmark_fixtures import (
            external_benchmark_contract, completed_benchmark_evidence, standalone_benchmark_contract,
        )
        parts = PurePosixPath(spec.relative_root).parts
        required = standalone_benchmark_contract(parts[1]) if len(parts) == 2 else None
        if original_source is not None:
            required = external_benchmark_contract(original_source)
            if required is None:
                raise SnapshotError('renamed benchmark has no exact original producer binding')
        completed_benchmark_evidence(source, required_contract=required, original_source=original_source)
    stability_hours = (
        spec.manual_capture_min_stable_hours if manual_capture
        else spec.minimum_stable_days * 24
    )
    age_limit = time.time_ns() - int(stability_hours * 3_600_000_000_000)
    rows: list[dict[str, Any]] = []
    allow_symlinks = _opaque_symlinks_enabled(spec)
    for parent, dirnames, filenames in os.walk(source, followlinks=False, onerror=_raise_walk_error):
        dirnames.sort()
        filenames.sort()
        parent_path = Path(parent)
        relative_parent = parent_path.relative_to(source).as_posix()
        relative_prefix = "" if relative_parent == "." else relative_parent + "/"
        for name in dirnames:
            if (parent_path / name).is_symlink() and not allow_symlinks:
                raise SnapshotError("legacy source contains a symlink directory")
        for name in filenames:
            path = parent_path / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) and allow_symlinks:
                continue
            if not stat.S_ISREG(info.st_mode):
                raise SnapshotError(f"legacy source has non-regular file: {path}")
            if info.st_mtime_ns > age_limit:
                raise SnapshotError(f"legacy source is not stable for {stability_hours} hours: {path}")
            rows.append({"path": relative_prefix + name, "source": _source_signature(info)})
    if not rows:
        raise SnapshotError("legacy source has no files")
    return rows


def _paths(spec: LegacyArchiveSpec, artifact_root: Path) -> tuple[Path, Path, Path]:
    if spec.stage_root in (D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT) and not _prunable_stage(spec):
        raise SnapshotError('guarded D benchmark scratch requires its explicit completed-case scope')
    source = artifact_root / spec.relative_root
    stage = spec.stage_root / spec.dataset
    archive = stage / "archive"
    if (spec.stage_root.is_relative_to(Path("/srv/stockagent-d-volume"))
        or spec.stage_root in (C_TEMP_STAGE_ROOT, D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT)):
        from stockagent.data_sync.cold_primary import _check_d_primary_mount

        _check_d_primary_mount(Path("/srv/stockagent-packed"))
    if source.resolve() != source or stage.resolve(strict=False) != stage:
        raise SnapshotError("legacy archive root is redirected by a symlink")
    if source == archive or source in archive.parents or archive in source.parents:
        raise SnapshotError("legacy source and stage overlap")
    return source, stage, archive


def _native_blob_options(sync_root: Path) -> dict[str, bool]:
    # This adapter changes only byte I/O, never archive identities or authority.
    # An edge consumer and test scratch retain normal native Linux I/O.
    from stockagent.data_sync.cold_primary import D_PRIMARY_MARKER, _check_d_primary_mount
    if sync_root.resolve() == Path("/srv/stockagent-packed") and (sync_root / D_PRIMARY_MARKER).exists():
        _check_d_primary_mount(sync_root)
        return {"d_primary_native_blob_reads": True}
    return {}


def _native_read_options(sync_root: Path) -> dict[str, bool]:
    native = _native_blob_options(sync_root)
    if native:
        # Measured complete real-pack recovery: native sequential D read, one
        # anonymous SSD pack, CRC/member SHA and original decode remain intact.
        native["d_primary_native_pack_reads"] = True
    return native


def _native_publish_options(sync_root: Path) -> dict[str, bool]:
    native = _native_blob_options(sync_root)
    if native:
        native["d_primary_native_blob_writes"] = True
    return native


@contextmanager
def _decoded_reader(path: Path, codec: str):
    """Read the entire encoding, including the compressor's exit status."""
    if codec == "gzip":
        with gzip.open(path, "rb") as stream:
            yield stream
    elif codec == "raw":
        with path.open("rb") as stream:
            yield stream
    elif codec == "zstd":
        with path.open("rb") as encoded, tempfile.TemporaryFile() as errors:
            try:
                process = subprocess.Popen(["zstd", "-d", "-q", "-c"], stdin=encoded,
                                           stdout=subprocess.PIPE, stderr=errors)
            except OSError as error:
                raise SnapshotError("zstd runtime is unavailable") from error
            try:
                yield process.stdout
                if process.wait() != 0:
                    raise SnapshotError("legacy zstd decode failed")
            finally:
                process.stdout.close()
                if process.poll() is None:
                    process.terminate()
                    process.wait()
    else:
        raise SnapshotError(f"unsupported legacy codec: {codec}")


def _decode_hash(path: Path, codec: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with _decoded_reader(path, codec) as stream:
        while chunk := stream.read(COPY_CHUNK):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


class _HashingReader:
    """Observe every encoded byte consumed by the decoder, in bounded blocks."""

    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()
        self.size = 0

    def read(self, size=COPY_CHUNK):
        # gzip uses bounded reads, but do not allow a caller to allocate an
        # entire multi-GB encoding through this verification adapter.
        block = self.source.read(COPY_CHUNK if size < 0 else min(size, COPY_CHUNK))
        self.digest.update(block)
        self.size += len(block)
        return block


NATIVE_ENCODED_READ_MIN_BYTES = 64 * 1024**2
NATIVE_ENCODED_READ_MAX_BYTES = 8 * 1024**3


@contextmanager
def _encoded_input(path: Path, fd: int, opened):
    """Read large reviewed D encodings natively, retaining the Linux FD fence.

    Only byte transport changes. The caller hashes every encoded/decoded byte
    and checks the still-open Linux descriptor and current path afterwards.
    Small members and all other paths retain the original local reader.
    """
    if (NATIVE_ENCODED_READ_MIN_BYTES <= opened.st_size <= NATIVE_ENCODED_READ_MAX_BYTES
            and any(path.is_relative_to(root) for root in (D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT))):
        from stockagent.data_sync.windows_cold_io import binary_reader
        with binary_reader(path, expected_bytes=opened.st_size) as source:
            yield source
    else:
        with os.fdopen(fd, "rb", closefd=False) as source:
            yield source


def _encoded_original_hashes(path: Path, codec: str) -> tuple[str, int, str, int]:
    """Hash the complete encoding and decoded original in one input pass.

    This does not share a proof across publication/recovery transactions. Each
    invocation rereads the exact file and retains both hashes and size checks.
    The zstd CLI still validates the complete stream and its exit status; one
    bounded feeder overlaps input hashing with decoding without buffering the
    encoding in memory or permitting a pipe-fill deadlock.
    """
    if codec not in CODEC_SUFFIXES:
        raise SnapshotError(f"unsupported legacy codec: {codec}")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode):
            raise SnapshotError("legacy encoded payload is not a regular file")
        signature = _source_signature(opened)
        with _encoded_input(path, fd, opened) as source:
            encoded = _HashingReader(source)
            original = hashlib.sha256()
            size = 0

            def consume(stream):
                nonlocal size
                while block := stream.read(COPY_CHUNK):
                    original.update(block)
                    size += len(block)

            if codec == "raw":
                # Encoding and original are exactly the same byte sequence.
                while encoded.read():
                    pass
                decoded_digest, size = encoded.digest.hexdigest(), encoded.size
            elif codec == "gzip":
                with gzip.GzipFile(fileobj=encoded, mode="rb") as stream:
                    consume(stream)
                decoded_digest = original.hexdigest()
            else:
                with tempfile.TemporaryFile() as errors:
                    try:
                        process = subprocess.Popen(["zstd", "-d", "-q", "-c"],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
                    except OSError as error:
                        raise SnapshotError("zstd runtime is unavailable") from error

                    def feed():
                        with process.stdin:
                            while block := encoded.read():
                                process.stdin.write(block)

                    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="legacy-decode-input") as pool:
                        feeding = pool.submit(feed)
                        try:
                            consume(process.stdout)
                            if process.wait() != 0:
                                raise SnapshotError("legacy zstd decode failed")
                            # A feeder read/write failure can otherwise look
                            # like a successfully decoded, shorter stream.
                            feeding.result()
                        finally:
                            process.stdout.close()
                            if process.poll() is None:
                                process.terminate()
                                process.wait()
                            # Wait for the one writer before closing its input
                            # descriptor, including decoder/consumer failures.
                            # A terminated child unblocks a pending pipe write.
                    decoded_digest = original.hexdigest()
            # Include any remaining encoded bytes. gzip validates concatenated
            # members/trailers, zstd consumes stdin to EOF; this explicit drain
            # also prevents an early-returning adapter from omitting the tail.
            while encoded.read():
                pass
            result = encoded.digest.hexdigest(), encoded.size, decoded_digest, size
        # Native exit/EOF validation is part of the input context. Check both
        # identities after it too, including failures or changes at pipe close.
        after = path.lstat()
        if (_source_signature(os.fstat(fd)) != signature
                or not stat.S_ISREG(after.st_mode)
                or _source_signature(after) != signature):
            raise SnapshotError("legacy encoded payload changed during verification")
        return result
    except (EOFError, gzip.BadGzipFile, zlib.error) as error:
        raise SnapshotError("legacy encoded payload decode failed") from error
    finally:
        os.close(fd)


@contextmanager
def _encoded_writer(output, codec: str, level: int):
    if codec == "raw":
        yield output
    elif codec == "gzip":
        with gzip.GzipFile(filename="", mode="wb", fileobj=output, compresslevel=1, mtime=0) as stream:
            yield stream
    elif codec == "zstd":
        with tempfile.TemporaryFile() as errors:
            try:
                # A fixed worker count makes the representation repeatable and
                # bounds CPU use alongside acquisition and execution services.
                process = subprocess.Popen(["zstd", "-q", f"-{level}", "-T1", "-c"],
                                           stdin=subprocess.PIPE, stdout=output, stderr=errors)
            except OSError as error:
                raise SnapshotError("zstd runtime is unavailable") from error
            try:
                yield process.stdin
                process.stdin.close()
                if process.wait() != 0:
                    raise SnapshotError("legacy zstd encode failed")
            finally:
                process.stdin.close()
                if process.poll() is None:
                    process.terminate()
                    process.wait()
    else:
        raise SnapshotError(f"unsupported legacy codec: {codec}")


def _encode_one(source_file: Path, target: Path, codec: str, *, durable: bool = True,
                compression_level: int = 1) -> str:
    if codec not in CODEC_SUFFIXES or compression_level not in {1, 3}:
        raise SnapshotError("unsupported legacy encoding parameters")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".partial")
    if temporary.exists():
        temporary.unlink()
    digest = hashlib.sha256()
    try:
        with source_file.open("rb") as raw, temporary.open("wb") as output:
            with _encoded_writer(output, codec, compression_level) as encoded:
                while chunk := raw.read(COPY_CHUNK):
                    digest.update(chunk)
                    encoded.write(chunk)
            output.flush()
            if durable:
                os.fsync(output.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return digest.hexdigest()


def _manual_capture_usage_gate(source: Path, artifact_root: Path, *, repo_root: Path | None = None,
                               spec: LegacyArchiveSpec | None = None) -> None:
    from stockagent.data_sync.artifact_consumers import artifact_service_references
    from stockagent.data_sync.remote_legacy_return import active_configuration_references

    references = artifact_process_references(source, legacy_process_scope(spec, artifact_root))
    repository = repo_root.resolve() if repo_root is not None else artifact_root.parent
    services = artifact_service_references((source,), repository)[str(source)]
    references.extend(active_configuration_references(source, repository))
    if references or services:
        raise SnapshotError(f"manual capture source is in use: {(references + services)[:3]}")


def _archive_directories(source: Path, *, allow_symlinks: bool = False) -> list[dict[str, Any]]:
    """Preserve empty directories and portable metadata, not inode identity."""
    result = []
    for parent, dirs, _files in os.walk(source, followlinks=False, onerror=_raise_walk_error):
        for name in sorted(dirs):
            path = Path(parent) / name
            info = path.lstat()
            if stat.S_ISLNK(info.st_mode) and allow_symlinks:
                continue
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != source.stat().st_dev:
                raise SnapshotError("legacy directory is linked or crosses a filesystem")
            result.append({"path": path.relative_to(source).as_posix(),
                           "mode": stat.S_IMODE(info.st_mode), "mtime_ns": info.st_mtime_ns})
    return sorted(result, key=lambda row: row["path"])


def prepare_archive(
    spec: LegacyArchiveSpec, artifact_root: Path, *, manual_capture: bool = False,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    artifact_root = artifact_root.resolve()
    if spec.compression_profile not in COMPRESSION_PROFILES:
        raise SnapshotError("unknown legacy compression profile")
    if not spec.durable_staging and not manual_capture:
        raise SnapshotError('rebuildable staging requires explicit manual capture; cold publication remains durable')
    source, stage, archive = _paths(spec, artifact_root)
    if spec.stage_root in (C_TEMP_STAGE_ROOT, D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT):
        available = shutil.disk_usage(spec.stage_root.parent).free
        planned_bytes = sum(row["source"]["size"] for row in source_plan(source, spec, manual_capture=manual_capture))
        if available < planned_bytes + 32 * 1024**3:
            volume = "C" if spec.stage_root == C_TEMP_STAGE_ROOT else "guarded D temporary"
            raise SnapshotError(f"{volume} archive scratch lacks source-size budget plus 32 GiB reserve")
        # WSL's virtual ext4 free space is not the Windows backing volume's
        # free space. Budget both before a new scratch wave can grow its VHDX.
        windows_c = Path("/mnt/c")
        if (spec.stage_root == C_TEMP_STAGE_ROOT and os.path.ismount(windows_c)
                and shutil.disk_usage(windows_c).free < planned_bytes + 32 * 1024**3):
            raise SnapshotError("Windows C backing volume lacks scratch budget plus 32 GiB reserve")
    references = artifact_process_references(source, legacy_process_scope(spec, artifact_root))
    if references:
        raise SnapshotError(f"legacy source has process references: {references[:3]}")
    planned = source_plan(source, spec, manual_capture=manual_capture)
    allow_symlinks = _opaque_symlinks_enabled(spec)
    directories = _archive_directories(source, allow_symlinks=allow_symlinks)
    symlinks = _archive_symlinks(source, spec) if allow_symlinks else None
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root, repo_root=repo_root, spec=spec)
    stage.mkdir(parents=True, exist_ok=True)
    plan_path = stage / "source_plan.json"
    if allow_symlinks:
        link_plan_path = stage / "symlink_plan.json"
        if link_plan_path.exists() or link_plan_path.is_symlink():
            if link_plan_path.is_symlink() or not link_plan_path.is_file():
                raise SnapshotError("legacy symlink plan is redirected")
            if json.loads(link_plan_path.read_text()) != symlinks:
                raise SnapshotError("legacy symlinks changed since staging began")
        else:
            if (stage / "receipts").exists() or archive.exists():
                raise SnapshotError("interrupted stage has no frozen symlink plan")
            atomic_write_json(link_plan_path, symlinks)
    if plan_path.exists():
        prior = json.loads(plan_path.read_text(encoding="utf-8"))
        if prior != planned:
            raise SnapshotError("legacy source changed since staging began; quarantine stage for audit")
    else:
        atomic_write_json(plan_path, planned)
    receipts = stage / "receipts"
    receipts.mkdir(exist_ok=True)
    result_rows: list[dict[str, Any]] = []
    def stage_item(item):
        relative = item["path"]
        source_file = source.joinpath(*PurePosixPath(relative).parts)
        signature = item["source"]
        if _source_signature(source_file.lstat()) != signature:
            raise SnapshotError(f"legacy source changed during staging: {source_file}")
        key = hashlib.sha256(relative.encode("utf-8")).hexdigest()
        receipt_path = receipts / f"{key}.json"
        if receipt_path.is_file():
            try:
                row = json.loads(receipt_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                row = {}
            if not isinstance(row, dict):
                row = {}
            prior_codec = row.get("codec")
            prior_path = f"payload/{key[:2]}/{key}.{CODEC_SUFFIXES.get(prior_codec, 'invalid')}"
            prior_file = archive / prior_path
            if (
                row.get("path") == relative
                and row.get("source") == signature
                and prior_codec in CODEC_SUFFIXES
                and row.get("encoded_path") == prior_path
                and prior_file.is_file() and not prior_file.is_symlink()
                and sha256_file(prior_file) == row.get("encoded_sha256")
            ):
                # A verified interrupted stage keeps its exact representation;
                # a new profile must not rewrite history or leave extra payloads.
                return row
        codec = "gzip" if relative.lower().endswith(".csv") and signature["size"] >= COMPRESS_MIN_BYTES else "raw"
        trial = None
        adaptive = spec.compression_profile != DEFAULT_COMPRESSION_PROFILE
        trial_codec = "zstd" if spec.compression_profile in {ZSTD_COMPRESSION_PROFILE, ZSTD3_COMPRESSION_PROFILE} else "gzip"
        level = 3 if spec.compression_profile == ZSTD3_COMPRESSION_PROFILE else 1
        if adaptive and signature["size"] >= COMPRESS_MIN_BYTES:
            trial_root = stage / "codec-trials"
            trial_root.mkdir(exist_ok=True)
            trial = trial_root / f"{key}-{uuid.uuid4().hex}.{CODEC_SUFFIXES[trial_codec]}"
            trial_hash = _encode_one(source_file, trial, trial_codec, durable=spec.durable_staging,
                                     compression_level=level)
            if _source_signature(source_file.lstat()) != signature:
                raise SnapshotError("legacy source changed during compression trial; trial retained")
            # Actual complete-file gain, not extension or a nonrepresentative
            # sample. Do not spend D writes/reads on incompressible encodings.
            codec = trial_codec if trial.stat().st_size * 10 < signature["size"] * 9 else "raw"
        encoded_path = f"payload/{key[:2]}/{key}.{CODEC_SUFFIXES[codec]}"
        encoded_file = archive / encoded_path
        if trial is not None and codec == trial_codec:
            encoded_file.parent.mkdir(parents=True, exist_ok=True)
            os.replace(trial, encoded_file)
            original_hash = trial_hash
        else:
            original_hash = (_encode_one(source_file, encoded_file, codec) if spec.durable_staging else
                             _encode_one(source_file, encoded_file, codec, durable=False))
            if trial is not None:
                if original_hash != trial_hash or _source_signature(source_file.lstat()) != signature:
                    raise SnapshotError("legacy source changed between codec trials; trial retained")
                trial.unlink()  # exact, privately created rejected encoding only
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
        return row

    # Only reviewed, bounded D scratch uses concurrent encoding. Each job
    # owns one unique source/receipt/payload name; final manifest order and
    # all whole-tree source/consumer fences are unchanged. Limit submission
    # and serialize large files so many slow small D writes can overlap
    # without multiplying compression buffers or active large-file I/O.
    workers = _d_stage_file_workers(spec, archive) if manual_capture and _prunable_stage(spec) else 1
    try:
        soft_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft_limit != resource.RLIM_INFINITY and soft_limit - len(os.listdir('/proc/self/fd')) < 32:
            workers = 1
    except (OSError, ValueError, OverflowError):
        workers = 1
    large_file_lock = threading.Lock()
    def guarded_stage_item(item):
        with large_file_lock if item['source']['size'] >= 64 * 1024**2 else nullcontext():
            return stage_item(item)
    if workers == 1:
        result_rows = list(map(guarded_stage_item, planned))
    else:
        keys = [hashlib.sha256(item['path'].encode('utf-8')).digest() for item in planned]
        if len(set(keys)) != len(keys):
            raise SnapshotError('legacy source paths have duplicate staging keys')
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='legacy-stage') as executor:
            iterator = iter(planned)
            while batch := list(itertools.islice(iterator, 32)):
                result_rows.extend(executor.map(guarded_stage_item, batch))
    # Per-file checks alone miss an earlier file changing, or a new path
    # appearing, while a later file is being encoded. Require the whole tree
    # to remain identical before exposing the preservation manifest.
    if source_plan(source, spec, manual_capture=manual_capture) != planned:
        raise SnapshotError("legacy source changed during staging; stage retained for audit")
    if _archive_directories(source, allow_symlinks=allow_symlinks) != directories:
        raise SnapshotError("legacy directories changed during staging; stage retained for audit")
    if allow_symlinks and _archive_symlinks(source, spec) != symlinks:
        raise SnapshotError("legacy symlinks changed during staging; stage retained for audit")
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root, repo_root=repo_root, spec=spec)
    manifest = _capture_manifest(spec, planned, directories, result_rows, manual_capture=manual_capture,
                                 symlinks=symlinks)
    atomic_write_json(archive / "legacy_archive_manifest.json", manifest)
    return manifest


def _capture_manifest(spec, planned, directories, result_rows, *, manual_capture, symlinks=None):
    """The same preservation descriptor for new and resumed complete captures."""
    manifest = {
        "schema_version": LINK_ARCHIVE_SCHEMA if symlinks is not None else ARCHIVE_SCHEMA,
        "dataset": spec.dataset,
        "relative_root": spec.relative_root,
        "deployable": False,
        "completion_claim": "not_checked",
        "files": result_rows,
        "directories": directories,
    }
    if spec.compression_profile != DEFAULT_COMPRESSION_PROFILE:
        manifest["encoding_profile"] = spec.compression_profile
    if spec.capture_contract is not None:
        manifest["capture_contract"] = spec.capture_contract
    if symlinks is not None:
        if not _opaque_symlinks_enabled(spec):
            raise SnapshotError("symlink manifest lacks its explicit capture policy")
        manifest["symlink_policy"] = spec.symlink_policy
        manifest["symlinks"] = _validated_symlinks(symlinks)
    if manual_capture:
        manifest["source_stability"] = {
            "mode": "manual-exact-capture-v1",
            "minimum_stable_hours": spec.manual_capture_min_stable_hours,
            "source_plan_sha256": hashlib.sha256(json.dumps(planned, sort_keys=True).encode()).hexdigest(),
        }
        if symlinks is not None:
            manifest["source_stability"]["symlink_plan_sha256"] = hashlib.sha256(
                json.dumps(symlinks, sort_keys=True).encode()).hexdigest()
    return manifest


def recover_complete_archive_manifest(
    spec: LegacyArchiveSpec, artifact_root: Path, *, manual_capture: bool = False,
    repo_root: Path | None = None,
) -> dict[str, Any] | None:
    """Finish only a complete interrupted encoding, with no payload writes.

    The existing capture owner must serialize this call. Missing receipts or
    payloads return None for ordinary preparation; corrupt/unknown members and
    source drift fail closed. Old real observations remain unchanged, including
    a uniform device renumber after reboot. This creates no cold/READY proof.
    """
    from stockagent.data_sync.remote_legacy_return import metadata_tree

    artifact_root = artifact_root.resolve()
    if spec.compression_profile not in COMPRESSION_PROFILES:
        raise SnapshotError('unknown legacy compression profile')
    if not spec.durable_staging and not manual_capture:
        raise SnapshotError('rebuildable staging requires explicit manual capture')
    source, stage, archive = _paths(spec, artifact_root)
    destination = archive / 'legacy_archive_manifest.json'
    if destination.exists() or destination.is_symlink():
        raise SnapshotError('complete capture manifest already exists; use ordinary verification')
    plan_path = stage / 'source_plan.json'
    if not plan_path.exists():
        return None
    if (plan_path.is_symlink() or not plan_path.is_file()
            or plan_path.stat().st_size > 64 * 1024**2):
        raise SnapshotError('interrupted source-plan control is invalid')
    planned = json.loads(plan_path.read_text())
    current = source_plan(source, spec, manual_capture=manual_capture)
    if not isinstance(planned, list) or not planned or len(planned) != len(current):
        raise SnapshotError('interrupted source plan differs from current source')
    mappings, changed = set(), 0
    for old, now in zip(planned, current, strict=True):
        if (not isinstance(old, dict) or set(old) != {'path', 'source'}
                or old['path'] != now['path'] or not isinstance(old['source'], dict)
                or set(old['source']) != set(now['source'])):
            raise SnapshotError('interrupted source observation schema differs')
        before, after = old['source']['device'], now['source']['device']
        if type(before) is not int or type(after) is not int or min(before, after) < 0:
            raise SnapshotError('interrupted device observation is invalid')
        normalized = {**old['source'], 'device': after}
        if json.dumps(normalized, sort_keys=True) != json.dumps(now['source'], sort_keys=True):
            raise SnapshotError('source changed since the interrupted capture')
        if before != after:
            mappings.add((before, after))
            changed += 1
    if mappings and (len(mappings) != 1 or changed != len(planned)):
        raise SnapshotError('interrupted capture has a partial or mixed device renumber')
    allow_symlinks = _opaque_symlinks_enabled(spec)
    directories = _archive_directories(source, allow_symlinks=allow_symlinks)
    symlinks = None
    rows, expected = [], {'source_plan.json'}
    if allow_symlinks:
        link_plan = stage / 'symlink_plan.json'
        if link_plan.is_symlink() or not link_plan.is_file() or link_plan.stat().st_size > 64 * 1024**2:
            raise SnapshotError('interrupted capture lacks its frozen symlink plan')
        symlinks = _validated_symlinks(json.loads(link_plan.read_text()))
        if _archive_symlinks(source, spec) != symlinks:
            raise SnapshotError('source symlinks changed since interrupted capture')
        expected.add('symlink_plan.json')
    for item in planned:
        key = hashlib.sha256(item['path'].encode('utf-8')).hexdigest()
        receipt_name = 'receipts/' + key + '.json'
        receipt = stage / receipt_name
        if not receipt.exists():
            return None
        if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 64 * 1024:
            raise SnapshotError('interrupted per-file encoding control is invalid')
        row = json.loads(receipt.read_text())
        if (not isinstance(row, dict) or set(row) != {'path', 'source', 'codec', 'encoded_path',
                'encoded_sha256', 'encoded_size', 'original_sha256'}
                or row['path'] != item['path']
                or json.dumps(row['source'], sort_keys=True) != json.dumps(item['source'], sort_keys=True)
                or not isinstance(row['codec'], str) or row['codec'] not in CODEC_SUFFIXES
                or row['encoded_path'] != f"payload/{key[:2]}/{key}.{CODEC_SUFFIXES[row['codec']]}"
                or type(row['encoded_size']) is not int or row['encoded_size'] < 0
                or any(not isinstance(row[k], str) or re.fullmatch('[0-9a-f]{64}', row[k]) is None
                       for k in ('original_sha256', 'encoded_sha256'))):
            raise SnapshotError('interrupted encoding receipt differs from its original plan')
        encoded = archive / row['encoded_path']
        if not encoded.exists():
            return None
        expected.update((receipt_name, 'archive/' + row['encoded_path']))
        rows.append(row)
    # A complete stage is metadata-only recovery, not another source-sized wave.
    before_stage = metadata_tree(stage)
    if ({r['path'] for r in before_stage['rows'] if r['kind'] == 'file'} != expected
            or any(r['kind'] not in {'file', 'directory'} or r['cross_filesystem']
                   or (r['kind'] == 'file' and r['signature'][6] != 1)
                   for r in before_stage['rows'])):
        raise SnapshotError('interrupted encoding contains unknown, linked or nonregular members')
    manifest = _capture_manifest(spec, planned, directories, rows, manual_capture=manual_capture,
                                 symlinks=symlinks)
    metadata_bytes = len(json.dumps(manifest, ensure_ascii=True).encode())
    if shutil.disk_usage(stage).free < 256 * 1024**2 + 2 * metadata_bytes:
        raise SnapshotError('complete capture metadata recovery lacks its bounded write budget')
    if artifact_process_references(stage, legacy_process_scope(spec, artifact_root)):
        raise SnapshotError('interrupted encoding has current process references')
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root, repo_root=repo_root, spec=spec)
    for row in rows:
        encoded = archive / row['encoded_path']
        proof = _encoded_original_hashes(encoded, row['codec'])
        if proof != (row['encoded_sha256'], row['encoded_size'], row['original_sha256'], row['source']['size']):
            raise SnapshotError('interrupted encoding or decoded original bytes differ')
        if sha256_file(source / row['path']) != row['original_sha256']:
            raise SnapshotError('current source bytes differ from the interrupted original')
    if (source_plan(source, spec, manual_capture=manual_capture) != current
            or _archive_directories(source, allow_symlinks=allow_symlinks) != directories
            or (allow_symlinks and _archive_symlinks(source, spec) != symlinks)
            or metadata_tree(stage)['fingerprint'] != before_stage['fingerprint']
            or artifact_process_references(stage, legacy_process_scope(spec, artifact_root))):
        raise SnapshotError('source or encoding changed during complete manifest recovery')
    if manual_capture:
        _manual_capture_usage_gate(source, artifact_root, repo_root=repo_root, spec=spec)
    if destination.exists() or destination.is_symlink():
        raise SnapshotError('capture manifest appeared during recovery; preserve the original')
    atomic_write_json(destination, manifest)
    return manifest


def _d_stage_file_workers(spec: LegacyArchiveSpec | None, archive: Path) -> int:
    """Bound reads to an enrolled D stage or this process's prune quarantine."""
    if spec is not None and spec.stage_root in (D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT):
        allowed = {
            (spec.stage_root / spec.dataset / 'archive').absolute(),
            (spec.stage_root / f'.{spec.dataset}.pruning-{os.getpid()}' / 'archive').absolute(),
        }
        if archive.absolute() in allowed:
            return 4
    return 1


def _archive_directory_budget(file_workers: int) -> int:
    """Retain common D shards without spending the process's FD reserve."""
    if file_workers == 1:
        return 64
    try:
        soft_limit, _hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft_limit == resource.RLIM_INFINITY:
            return 256
        # The cache limit includes its root, and parallel lookups hold at most
        # one extra duplicate per worker. On normal limits leave three quarters
        # of the process budget for the owner and its other descriptors.
        return max(2, min(256, int(soft_limit) // 4 - file_workers))
    except (OSError, ValueError, OverflowError):
        return 64


def verify_archive_directory(
    archive: Path, source: Path | None = None, *, spec: LegacyArchiveSpec | None = None,
    manual_capture: bool = False, artifact_root: Path | None = None,
    repo_root: Path | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
    file_workers: int = 1,
) -> dict[str, Any]:
    # Reuse the bounded no-follow directory reader instead of resolving every
    # ancestor for every encoding. Full source/encoded/decoded SHA checks stay
    # in the original verifier; all observed directory names are fenced before
    # its result can be returned. The manifest is fenced as a regular leaf too.
    if type(file_workers) is not int or not 1 <= file_workers <= 4:
        raise ValueError('legacy verification workers must be between 1 and 4')
    if file_workers != 1 and _d_stage_file_workers(spec, archive) == 1:
        raise SnapshotError('parallel decoding is restricted to the exact enrolled D recovery stage')
    with PinnedObjectSignatures(archive, max_open_directories=_archive_directory_budget(file_workers),
                                parallel_signatures=file_workers > 1) as encoded_paths:
        raw, manifest_signature = encoded_paths.read_metadata('legacy_archive_manifest.json')
        manifest = json.loads(raw.decode('utf-8'))
        del raw
        proof = _verify_archive_directory(
            archive, source, spec=spec, manual_capture=manual_capture,
            artifact_root=artifact_root, repo_root=repo_root, progress=progress,
            manifest=manifest, encoded_paths=encoded_paths, file_workers=file_workers,
        )
        if encoded_paths.regular_file_signature('legacy_archive_manifest.json') != manifest_signature:
            raise SnapshotError('legacy archive manifest changed during verification')
        encoded_paths.recheck()
        return proof


def _archive_encoded_census(archive: Path, *, file_workers: int,
                            progress: Callable[[dict[str, Any]], None] | None = None,
                            expected_items: int | None = None) -> set[str]:
    """Independently observe the whole encoded tree, never following links.

    Census counts are metadata observations, not a successful byte audit. If
    an extra file is seen, drop the denominator instead of displaying 100%.
    """
    if expected_items is not None and (type(expected_items) is not int or expected_items < 0):
        raise ValueError('expected encoded census items must be a nonnegative integer')
    observed: set[str] = set()
    last_report = time.monotonic()
    def report():
        total = expected_items if expected_items is None or len(observed) <= expected_items else None
        progress({'stage': 'scan-encoded-tree', 'completed_items': len(observed),
                  'total_items': total, 'item_scope': 'encoded_regular_files_observed',
                  'metadata_only': True, 'acceptance_proof': False})

    if progress is not None:
        report()
    pool = ThreadPoolExecutor(max_workers=file_workers) if file_workers > 1 else nullcontext(None)
    with pool as executor:
        for parent, dirs, files in os.walk(archive, followlinks=False, onerror=_raise_walk_error):
            paths = (Path(parent) / name for name in (*dirs, *files))
            for path, info in _iter_lstat_bounded(paths, executor):
                if stat.S_ISREG(info.st_mode):
                    observed.add(path.relative_to(archive).as_posix())
                elif not stat.S_ISDIR(info.st_mode):
                    raise SnapshotError(f"legacy encoded tree has an unsupported entry: {path}")
            if progress is not None and time.monotonic() - last_report >= 5:
                report()
                last_report = time.monotonic()
    if progress is not None:
        report()
    return observed


def _verify_archive_directory(
    archive: Path, source: Path | None, *, spec: LegacyArchiveSpec | None,
    manual_capture: bool, artifact_root: Path | None, repo_root: Path | None,
    progress: Callable[[dict[str, Any]], None] | None,
    manifest: dict[str, Any], encoded_paths: PinnedObjectSignatures, file_workers: int,
) -> dict[str, Any]:
    symlink_capture = _opaque_symlinks_enabled(spec)
    expected_schema = LINK_ARCHIVE_SCHEMA if symlink_capture else ARCHIVE_SCHEMA
    if (
        manifest.get("schema_version") != expected_schema
        or manifest.get("deployable") is not False
        or manifest.get("completion_claim") != "not_checked"
    ):
        raise SnapshotError("invalid legacy archive manifest")
    if symlink_capture:
        if manifest.get("symlink_policy") != spec.symlink_policy:
            raise SnapshotError("legacy symlink policy differs from its catalog")
        symlinks = _validated_symlinks(manifest.get("symlinks"))
    else:
        if "symlinks" in manifest or "symlink_policy" in manifest:
            raise SnapshotError("unexpected legacy symlink inventory")
        symlinks = []
    if manifest.get("encoding_profile", DEFAULT_COMPRESSION_PROFILE) not in COMPRESSION_PROFILES:
        raise SnapshotError("unknown legacy archive encoding profile")
    if spec is not None and (
        manifest.get("dataset") != spec.dataset
        or manifest.get("relative_root") != spec.relative_root
        or manifest.get("capture_contract") != spec.capture_contract
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
    if symlink_capture and (capture is None or capture.get("symlink_plan_sha256") != hashlib.sha256(
            json.dumps(symlinks, sort_keys=True).encode()).hexdigest()):
        raise SnapshotError("manual symlink plan digest differs from archive inventory")
    source_spec = spec or LegacyArchiveSpec(
        manifest["dataset"], manifest["relative_root"], 7, archive.parent
    )
    original_source = None
    if source is not None and artifact_root is not None and manual_capture:
        from stockagent.data_sync.benchmark_fixtures import external_benchmark_contract
        expected_source = artifact_root.resolve() / source_spec.relative_root
        if source != expected_source and external_benchmark_contract(expected_source) is not None:
            original_source = expected_source
    before_source = source_plan(source, source_spec, manual_capture=manual_capture,
        original_source=original_source) if source is not None else None
    before_symlinks = None
    if source is not None and symlink_capture:
        logical_source = (artifact_root.resolve() / source_spec.relative_root
                          if artifact_root is not None else source)
        before_symlinks = _archive_symlinks(source, source_spec, original_source=logical_source)
        def portable_link(row):
            return {"path": row["path"], "target_b64": row["target_b64"],
                    "source": {key: row["source"][key] for key in ("size", "mtime_ns", "mode")}}
        if list(map(portable_link, before_symlinks)) != list(map(portable_link, symlinks)):
            raise SnapshotError("legacy source symlinks differ from the archive")
    directories = manifest.get("directories")
    if symlink_capture and not isinstance(directories, list):
        raise SnapshotError("symlink archive requires its complete directory inventory")
    if directories is not None:
        if not isinstance(directories, list):
            raise SnapshotError("legacy directory inventory is invalid")
        seen_directories = set()
        for row in directories:
            relative = _safe_relative_path(row.get("path", ""), "legacy directory").as_posix()
            if (set(row) != {"path", "mode", "mtime_ns"} or relative in seen_directories
                or type(row["mode"]) is not int or not 0 <= row["mode"] <= 0o7777
                or type(row["mtime_ns"]) is not int or row["mtime_ns"] < 0):
                raise SnapshotError("legacy directory descriptor is invalid")
            seen_directories.add(relative)
        if source is not None and _archive_directories(source, allow_symlinks=symlink_capture) != directories:
            raise SnapshotError("legacy source directory inventory differs")
    if manual_capture and source is not None:
        if artifact_root is None:
            raise SnapshotError("manual capture verification requires the original artifact root")
        _manual_capture_usage_gate(source, artifact_root.resolve(), repo_root=repo_root, spec=spec)
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
    original_bytes = sum(int(row['source']['size']) for row in rows)
    if progress is not None:
        progress({'stage': 'decode-originals', 'completed_items': 0, 'total_items': len(rows),
                  'completed_bytes': 0, 'total_bytes': original_bytes})

    def checked_rows():
        for row in rows:
            relative = _safe_relative_path(row["path"], "legacy archived path").as_posix()
            encoded = _safe_relative_path(row["encoded_path"], "legacy payload path").as_posix()
            if relative in expected_original or encoded in expected_encoded:
                raise SnapshotError("duplicate legacy archive path")
            expected_original.add(relative)
            expected_encoded.add(encoded)
            yield row, relative, encoded

    large_file_lock = threading.Lock()

    def verify_row(checked):
        row, relative, encoded = checked
        encoded_file = archive.joinpath(*PurePosixPath(encoded).parts)
        encoded_signature = encoded_paths.regular_file_signature(encoded)
        if encoded_signature[2] != row["encoded_size"]:
            raise SnapshotError(f"legacy encoded file differs: {encoded}")
        with large_file_lock if row['encoded_size'] >= NATIVE_ENCODED_READ_MIN_BYTES else nullcontext():
            encoded_digest, encoded_size, digest, size = _encoded_original_hashes(encoded_file, row["codec"])
        if encoded_paths.regular_file_signature(encoded) != encoded_signature:
            raise SnapshotError(f"legacy encoded file changed during verification: {encoded}")
        if encoded_size != row["encoded_size"] or encoded_digest != row["encoded_sha256"]:
            raise SnapshotError(f"legacy encoded file differs: {encoded}")
        if digest != row["original_sha256"] or size != row["source"]["size"]:
            raise SnapshotError(f"legacy decode proof differs: {relative}")
        drift = None
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
                drift = {"path": relative, "changed_observations": changed,
                         "current_signature": before, "exact_sha256": original_digest}
        return size, drift

    def verified_rows():
        checked = checked_rows()
        if file_workers == 1:
            yield from map(verify_row, checked)
            return
        # The archive manifest is untrusted input. Keep outstanding checks
        # bounded, validate unique safe paths in the owner thread, and retain
        # the same pass-pinned directory and per-file generation checks.
        with ThreadPoolExecutor(max_workers=file_workers) as executor:
            while batch := list(itertools.islice(checked, 64)):
                yield from executor.map(verify_row, batch)

    for index, (size, drift) in enumerate(verified_rows(), start=1):
        if drift is not None:
            source_metadata_drift.append(drift)
        total += size
        if progress is not None:
            progress({'stage': 'decode-originals', 'completed_items': index, 'total_items': len(rows),
                      'completed_bytes': total, 'total_bytes': original_bytes})
    # Census does bounded metadata reads only, distinct from content decoding.
    # On the exact enrolled D archive, a small-shard measurement supported
    # eight metadata workers; preserve the four-worker content limit.
    census_workers = 8 if file_workers == 4 and _d_stage_file_workers(spec, archive) == 4 else file_workers
    observed = _archive_encoded_census(archive, file_workers=census_workers,
                                       progress=progress, expected_items=len(expected_encoded))
    if observed != expected_encoded:
        raise SnapshotError("legacy encoded tree has extra or missing files")
    if symlink_capture:
        link_paths = {row["path"] for row in symlinks}
        directory_paths = {row["path"] for row in directories or []}
        occupied = expected_original | directory_paths | link_paths
        if (link_paths & (expected_original | directory_paths)
                or directory_paths & expected_original
                or ".LEGACY_RESTORED.json" in occupied
                or any(other.startswith(link + "/") for link in link_paths for other in occupied)
                or any(str(parent) != "." and parent.as_posix() not in directory_paths
                       for name in occupied for parent in PurePosixPath(name).parents)):
            raise SnapshotError("legacy symlink paths overlap other restored members")
    if source is not None:
        after_source = source_plan(source, source_spec, manual_capture=manual_capture,
                                  original_source=original_source)
        observed_source = {row["path"] for row in after_source}
        if observed_source != expected_original:
            raise SnapshotError("legacy source has extra or missing files")
        if before_source != after_source:
            raise SnapshotError("legacy source changed during verification")
        if directories is not None and _archive_directories(source, allow_symlinks=symlink_capture) != directories:
            raise SnapshotError("legacy directories changed during verification")
        if symlink_capture and _archive_symlinks(source, source_spec, original_source=logical_source) != before_symlinks:
            raise SnapshotError("legacy symlinks changed during verification")
        if manual_capture:
            _manual_capture_usage_gate(source, artifact_root.resolve(), repo_root=repo_root, spec=spec)
    proof = {"dataset": manifest["dataset"], "files": len(rows), "original_bytes": total, "manifest": manifest,
             "source_metadata_drift": source_metadata_drift}
    if symlink_capture:
        proof["symlinks"] = len(symlinks)
    return proof


def publish_archive(
    spec: LegacyArchiveSpec, artifact_root: Path, sync_root: Path, *, repo_root: Path,
    manual_capture: bool = False,
    defer_scan: bool = False,
    batch_directory_fsync: bool = False,
    pack_buckets: int = 32,
    publication_owner=None,
    publication_progress=None,
):
    if type(defer_scan) is not bool or type(batch_directory_fsync) is not bool:
        raise SnapshotError("defer_scan and batch_directory_fsync must be booleans")
    if type(pack_buckets) is not int or not 1 <= pack_buckets <= 4096:
        raise SnapshotError("legacy publication pack_buckets must be an integer between 1 and 4096")
    if publication_owner is not None and not callable(publication_owner):
        raise SnapshotError('legacy publication owner must be a context factory')
    if publication_progress is not None and not callable(publication_progress):
        raise SnapshotError('legacy publication progress must be a callback')
    if spec.compression_profile not in COMPRESSION_PROFILES:
        raise SnapshotError('unknown legacy compression profile')
    if not spec.durable_staging and not manual_capture:
        raise SnapshotError('rebuildable staging requires explicit manual capture; cold publication remains durable')
    if (spec.stage_root in (C_TEMP_STAGE_ROOT, D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT)
            and sync_root.resolve() != Path("/srv/stockagent-packed")):
        raise SnapshotError("temporary scratch may publish only into the guarded D canonical cold store")
    source, _stage, archive = _paths(spec, artifact_root.resolve())
    if not (archive / 'legacy_archive_manifest.json').exists():
        prepare_archive(spec, artifact_root, manual_capture=manual_capture, repo_root=repo_root)
    # The enrolled D scratch already supports bounded parallel decoding in
    # recovery/pruning. Use that same verifier for BOTH publication fences;
    # the default single-worker argument previously serialized these passes.
    # Other archives retain their original sequential admission.
    file_workers = _d_stage_file_workers(spec, archive) if manual_capture else 1
    # A complete stage keeps its original observation provenance. Re-verify
    # every encoded/original byte and current source signature, rather than
    # recreating source_plan after another verified hard link was removed.
    proof = verify_archive_directory(archive, source, spec=spec, manual_capture=manual_capture,
                                     artifact_root=artifact_root, repo_root=repo_root,
                                     file_workers=file_workers)
    baseline = source_plan(source, spec, manual_capture=manual_capture)
    baseline_symlinks = _archive_symlinks(source, spec) if _opaque_symlinks_enabled(spec) else None
    def source_guard():
        if source_plan(source, spec, manual_capture=manual_capture) != baseline:
            raise SnapshotError("legacy source changed before cold commit")
        if baseline_symlinks is not None and _archive_symlinks(source, spec) != baseline_symlinks:
            raise SnapshotError("legacy symlinks changed before cold commit")
        if manual_capture:
            _manual_capture_usage_gate(source, artifact_root.resolve(), repo_root=repo_root, spec=spec)
    # Full C/original validation and subsequent D recovery are independent
    # reads. Acquire the caller's shared owner only for canonical publication;
    # its source_guard still rejects changes made while waiting for the owner.
    with (publication_owner() if publication_owner is not None else nullcontext()):
        resolved = publish_packed_snapshot(
            sync_root,
            spec.dataset,
            archive,
            loose_file_threshold_bytes=8 * 1024 * 1024,
            pack_buckets=pack_buckets,
            metadata={
                "transport_role": "legacy-quarantine-archive",
                "deployable": "false",
                "completion_claim": "not_checked",
                "source_relative_root": spec.relative_root,
                "legacy_manifest_sha256": sha256_file(archive / "legacy_archive_manifest.json"),
            },
            repo_root=repo_root,
            source_guard=source_guard,
            **({"defer_scan": True} if defer_scan else {}),
            **({"batch_directory_fsync": True} if batch_directory_fsync else {}),
            **({"progress": publication_progress} if publication_progress is not None else {}),
            # These guarded D stages are closed legacy archives, and measured
            # small-file 9p reads benefit from bounded concurrent prefetch.
            # The canonical pack bytes and source/recovery fences stay intact.
            **({"pack_source_workers": 8}
               if manual_capture and spec.stage_root in (D_BENCHMARK_STAGE_ROOT, D_OFFLINE_STAGE_ROOT)
               else {}),
            **_native_publish_options(sync_root),
        )
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive, **_native_read_options(sync_root))
    # Publication may have been slow. Never declare the hot source preserved
    # if it changed after the archive verification but before the cold commit.
    verify_archive_directory(archive, source, spec=spec, manual_capture=manual_capture,
                             artifact_root=artifact_root, repo_root=repo_root,
                             file_workers=file_workers)
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "source_files": proof["files"], "source_bytes": proof["original_bytes"], "cold_bytes": resolved.manifest["archive"]["stored_bytes"]}


def _verification_filesystem(path: Path) -> str:
    return subprocess.run(['findmnt', '-n', '-o', 'FSTYPE', '-T', str(path)],
                          capture_output=True, text=True, check=True, timeout=10).stdout.strip()


def verify_cold_archive(
    spec: LegacyArchiveSpec, sync_root: Path, *, verification_root: Path | None = None,
    reuse_encoded_archive: Path | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
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
    if reuse_encoded_archive is not None:
        if verification_root is not None:
            raise SnapshotError("choose existing encoded bytes or private recovery scratch, not both")
        return _verify_cold_using_existing_encoding(
            spec, sync_root, resolved, reuse_encoded_archive, progress=progress)
    needed = int(resolved.manifest["source"]["logical_bytes"])
    admission = None
    if verification_root is None:
        scratch_parent = Path(tempfile.gettempdir())
        required = needed + 32 * 1024**3
        free = shutil.disk_usage(scratch_parent).free
        if free < required:
            raise SnapshotError("verification scratch lacks bounded space plus 32 GiB reserve")
        mount = _verification_filesystem(scratch_parent)
        if mount == 'tmpfs':
            # tmpfs is a memory/swap workspace, not a WSL ext4 allocation.
            # Its free-space admission is not proof of unswapped RAM or of C
            # backing capacity; preserve that distinction in the receipt.
            admission = {'filesystem': mount, 'available_bytes': free, 'required_bytes': required,
                         'physical_backing_verified': False}
        else:
            # WSL's virtual ext4 free space can exceed the Windows volume
            # backing its VHDX. Check it before allocating the default tree.
            from stockagent.data_sync.training_return import admit_workspace
            admission = admit_workspace(scratch_parent, required)
    else:
        scratch_parent = verification_root.absolute()
        cold = sync_root.resolve()
        if (scratch_parent.resolve() != scratch_parent or scratch_parent == cold
            or cold in scratch_parent.parents or scratch_parent in cold.parents):
            raise SnapshotError("verification workspace is redirected or overlaps the cold authority")
        scratch_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        from stockagent.data_sync.training_return import admit_workspace
        admission = admit_workspace(scratch_parent, needed + 32 * 1024**3)
    temporary = Path(tempfile.mkdtemp(prefix="stockagent-legacy-verify-", dir=scratch_parent))
    def progress_options(step):
        if progress is None:
            return {}
        return {'progress': lambda value: progress({'recovery_step': step, **value})}
    try:
        native = _native_read_options(sync_root)
        archive = fetch_packed_snapshot(sync_root, temporary, resolved, verification_only=True,
                                       **native, **progress_options('fetch-encoded-archive'))
        if sha256_file(archive / "legacy_archive_manifest.json") != metadata.get("legacy_manifest_sha256"):
            raise SnapshotError("reconstructed legacy manifest differs from cold metadata")
        proof = verify_archive_directory(archive, spec=spec, **progress_options('decode-originals'))
        inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
        verify_packed_snapshot(sync_root, resolved,
                               reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"],
                               **native, **progress_options('independent-cold-reconstruction'))
    except Exception as exc:
        raise SnapshotError(f"cold verification failed; scratch retained at {temporary}: {exc}") from exc
    shutil.rmtree(temporary)
    proof.update(snapshot_id=resolved.manifest["snapshot_id"],
                 manifest_sha256=resolved.manifest_sha256, cold_verified=True,
                 decoded_originals_verified=True, verification_scratch_removed=True,
                 verification_only_reconstruction=True,
                 verified_at_epoch=time.time())
    if admission is not None:
        proof["verification_workspace"] = {"parent": str(scratch_parent), **admission}
    return proof


def _verify_cold_using_existing_encoding(spec, sync_root, resolved, archive, *, progress):
    """Recover/hash every cold file, then decode the EXACT same encoded bytes.

    The publisher's already retained encoding is not a recovery proof. It is
    merely a byte-for-byte checked decode workspace, avoiding another full
    encoded-tree allocation. Cold object SHA, all ZIP CRC/member SHA, every
    independently reconstructed encoded file, original decoded SHA/size/count
    and fresh local full-tree verification remain mandatory.
    """
    archive = Path(archive).absolute()
    expected = (spec.stage_root / spec.dataset / "archive").absolute()
    cold = sync_root.resolve()
    if (archive != expected or archive.resolve() != archive or not archive.is_dir()
            or archive == cold or cold in archive.parents or archive in cold.parents):
        raise SnapshotError("existing recovery encoding must be the exact real publisher stage outside cold")
    metadata = resolved.manifest["metadata"]
    manifest_path = archive / "legacy_archive_manifest.json"
    expected_manifest_sha = metadata.get("legacy_manifest_sha256")
    if sha256_file(manifest_path) != expected_manifest_sha:
        raise SnapshotError("existing recovery manifest differs from fixed cold metadata")
    # Measured DrvFs per-file latency dominates these two enrolled D stages.
    # Parallelism is bounded to four reads; every hash, tree scan and the
    # independent cold reconstruction remain in this same transaction.
    file_workers = _d_stage_file_workers(spec, archive)
    def report(step):
        return None if progress is None else lambda value: progress({"recovery_step": step, **value})
    with _verified_packed_inventory(
            sync_root, resolved, materialized_path=archive, reconstruct_all=True,
            **_native_read_options(sync_root),
            progress=report("independent-cold-reconstruction"),
            materialized_file_workers=file_workers) as (entries, cold_proof):
        proof = verify_archive_directory(archive, spec=spec, progress=report("decode-originals"),
                                         file_workers=file_workers)
        # A producer may have changed an earlier file while a later original
        # was decoding. Recheck the complete local tree, not only its manifest.
        if progress is None:
            _verify_materialized(archive, resolved.manifest, entries, file_workers=file_workers)
        else:
            _verify_materialized(archive, resolved.manifest, entries,
                                 progress=report("decode-originals"),
                                 stage="verify-materialized-post-decode", file_workers=file_workers)
        if sha256_file(manifest_path) != expected_manifest_sha:
            raise SnapshotError("existing recovery manifest changed during decode")
    proof.update(snapshot_id=resolved.manifest["snapshot_id"],
                 manifest_sha256=resolved.manifest_sha256, cold_verified=True,
                 decoded_originals_verified=True, verification_scratch_removed=True,
                 verification_only_reconstruction=True, verified_at_epoch=time.time(),
                 verification_mode="independent_cold_reconstruction_and_exact_encoding_decode_v1",
                 verification_scratch_created=False, reused_encoded_archive=str(archive),
                 cold_reconstruction=cold_proof)
    return proof


def _stage_cold_generation(sync_root: Path, spec: LegacyArchiveSpec) -> dict[str, Any]:
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    objects = []
    for item in [resolved.manifest["archive"]["inventory"], *resolved.manifest["archive"]["objects"]]:
        path = sync_root / item["relpath"]
        observed = path.lstat()
        if (path.resolve() != path or not stat.S_ISREG(observed.st_mode)
                or observed.st_size != item["bytes"]):
            raise SnapshotError("encoding stage cold object generation is unavailable")
        objects.append({"relative": item["relpath"], "signature": _source_signature(observed)})
    return {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256, "objects": objects}


def _prunable_stage(spec: LegacyArchiveSpec) -> bool:
    if spec.stage_root == C_TEMP_STAGE_ROOT:
        return True
    reviewed = ((spec.stage_root == D_BENCHMARK_STAGE_ROOT
                 and spec.relative_root.startswith("benchmarks/")
                 and reviewed_wsl_artifact_root(spec.relative_root))
                or (spec.stage_root == D_OFFLINE_STAGE_ROOT
                    and reviewed_offline_scratch_root(spec.relative_root)))
    return (reviewed
            and spec.capture_contract == MANUAL_WSL_CAPTURE_CONTRACT
            and spec.manual_capture_min_stable_hours is not None
            and spec.manual_capture_min_stable_hours >= 12)


def _stage_prune_metadata_workers(spec: LegacyArchiveSpec, stage: Path) -> int:
    """Bound D scratch metadata/receipts separately from content decoding.

    Only the exact enrolled D stage (or this owner's named prune quarantine)
    can use the measured eight-worker path. Do not spend descriptor headroom
    on a process with a small or unobservable limit. C scratch stays serial.
    """
    content_workers = _d_stage_file_workers(spec, stage / 'archive')
    if content_workers != 4 or not _prunable_stage(spec):
        return content_workers
    try:
        soft_limit, _hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft_limit != resource.RLIM_INFINITY:
            # Eight receipt readers need descriptors concurrently; the limit
            # alone does not show whether another part of this owner has
            # already spent the reserve. An unobservable count fails closed.
            if soft_limit < 64 or soft_limit - len(os.listdir('/proc/self/fd')) < 16:
                return content_workers
    except (OSError, ValueError, OverflowError):
        return content_workers
    return 8


def plan_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path
) -> dict[str, Any]:
    """Audit bounded temporary encoding only after exact hot retirement.

    Existing D legacy stages/objects remain excluded. Only the separate explicit
    completed-benchmark scratch may share these full recovery/quarantine gates.
    """
    if not _prunable_stage(spec):
        raise SnapshotError("stage pruning is restricted to the reviewed temporary roots")
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.materialized_cache import process_references, _pinned_snapshot_ids

    _check_d_primary_mount(sync_root)
    source = artifact_root.resolve() / spec.relative_root
    if source.exists() or source.is_symlink():
        raise SnapshotError("encoding stage pruning requires completed hot-source retirement")
    stage = spec.stage_root / spec.dataset
    if stage.is_symlink() or stage.resolve() != stage or not stage.is_dir():
        raise SnapshotError("encoding stage is missing or redirected")
    state = json.loads((state_root / "retirements" / f"{spec.dataset}.json").read_text())
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    if (state.get("state") != "cold-only" or state.get("dataset") != spec.dataset
        or state.get("relative_root") != spec.relative_root
        or state.get("snapshot_id") != resolved.manifest["snapshot_id"]):
        raise SnapshotError("encoding stage retirement state differs from the selected cold release")
    if str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized")):
        raise SnapshotError("encoding stage release is pinned")
    generation = _stage_cold_generation(sync_root, spec)
    archive = stage / "archive"
    workers = _d_stage_file_workers(spec, archive)
    metadata_workers = _stage_prune_metadata_workers(spec, stage)
    identity_before = {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"],
                       "manifest_sha256": resolved.manifest_sha256}
    stage_before = _stage_prune_fingerprint(stage, identity_before, file_workers=metadata_workers)
    proof = verify_archive_directory(archive, spec=spec, file_workers=workers)
    if sha256_file(archive / "legacy_archive_manifest.json") != resolved.manifest.get("metadata", {}).get("legacy_manifest_sha256"):
        raise SnapshotError("encoding stage manifest differs from the cold release")
    inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
    verify_packed_snapshot(sync_root, resolved, materialized_path=archive,
                           reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"],
                           materialized_file_workers=workers, **_native_read_options(sync_root))
    rows = proof["manifest"]["files"]
    if json.loads((stage / "source_plan.json").read_text()) != [
        {"path": row["path"], "source": row["source"]} for row in rows
    ]:
        raise SnapshotError("encoding stage source-plan control file differs")
    expected = {"source_plan.json", "archive/legacy_archive_manifest.json"}
    if _opaque_symlinks_enabled(spec):
        link_plan = stage / "symlink_plan.json"
        if (link_plan.is_symlink() or not link_plan.is_file()
                or link_plan.stat().st_size > 64 * 1024 * 1024
                or json.loads(link_plan.read_text()) != proof["manifest"]["symlinks"]):
            raise SnapshotError("encoding stage symlink-plan control file differs")
        expected.add("symlink_plan.json")
    def check_receipt(row):
        key = hashlib.sha256(row["path"].encode()).hexdigest()
        name = "receipts/" + key + ".json"
        if json.loads((stage / name).read_text()) != row:
            raise SnapshotError("encoding stage per-file receipt differs")
        return name

    def checked_receipts():
        if metadata_workers == 1:
            yield from map(check_receipt, rows)
            return
        with ThreadPoolExecutor(max_workers=metadata_workers) as executor:
            iterator = iter(rows)
            while batch := list(itertools.islice(iterator, 64)):
                yield from executor.map(check_receipt, batch)

    for row, name in zip(rows, checked_receipts()):
        expected.add("archive/" + row["encoded_path"])
        expected.add(name)
    signatures, reclaimable = _stage_prune_signatures(stage, file_workers=metadata_workers, collect_allocation=True)
    observed_paths = {row["path"] for row in signatures}
    duplicates = []
    for extra in sorted(observed_paths-expected):
        match = re.fullmatch(r"receipts/\.syncthing\.([0-9a-f]{64})\.json\.[0-9]+\.[0-9a-f]{32}\.tmp", extra)
        canonical = "receipts/"+match[1]+".json" if match else None
        path = stage/extra
        if (canonical not in expected or path.stat().st_size > 65536
                or path.stat().st_size != (stage/canonical).stat().st_size
                or sha256_file(path) != sha256_file(stage/canonical)):
            raise SnapshotError("encoding stage has unknown or missing files; preserve for audit")
        # This is an exact duplicate of a control receipt already checked
        # against the fixed original manifest, never an unclassified payload.
        duplicates.append({"path":extra,"canonical_receipt":canonical,"sha256":sha256_file(path)})
    if observed_paths != expected | {r["path"] for r in duplicates}:
        raise SnapshotError("encoding stage has unknown or missing files; preserve for audit")
    if process_references(stage):
        raise SnapshotError("encoding stage has live process references")
    identity = {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"],
                "manifest_sha256": resolved.manifest_sha256, "signatures": signatures}
    fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    if stage_before != fingerprint or _stage_cold_generation(sync_root, spec) != generation:
        raise SnapshotError("encoding stage or cold generation changed during full verification")
    return {"dataset": spec.dataset, "stage": str(stage), "snapshot_id": resolved.manifest["snapshot_id"],
            "manifest_sha256": resolved.manifest_sha256, "apply_ready": True,
            "stage_fingerprint": fingerprint,
            "verified_duplicate_control_files": duplicates,
            "full_verification": {"contract": "owned-archive-stage-verification-v1",
                "verified_at_epoch": time.time(), "cold_generation": generation,
                "artifact_root": str(artifact_root.resolve()), "state_root": str(state_root.resolve())},
            "reclaimable_allocated_file_bytes": reclaimable, "cold_objects_deleted": 0}


def _stage_prune_signatures(stage: Path, *, file_workers: int = 1, collect_allocation: bool = False):
    """One fresh observation per file for both its type guard and identity.

    Every caller still walks the complete tree. Nothing is cached between the
    independent pre-verification, post-verification and quarantine fences.
    """
    if type(file_workers) is not int or not 1 <= file_workers <= 8:
        raise ValueError('stage signature workers must be between 1 and 8')
    signatures = []
    inodes: dict[tuple[int, int], list[int]] = {}
    pool = ThreadPoolExecutor(max_workers=file_workers) if file_workers > 1 else nullcontext(None)
    with pool as executor:
        for parent, dirs, files in os.walk(stage, followlinks=False, onerror=_raise_walk_error):
            directory = Path(parent)
            for path, info in _iter_lstat_bounded((directory / name for name in dirs), executor):
                if not stat.S_ISDIR(info.st_mode):
                    raise SnapshotError("encoding stage contains a symlink directory or non-directory entry")
            for path, info in _iter_lstat_bounded((directory / name for name in files), executor):
                if not stat.S_ISREG(info.st_mode):
                    raise SnapshotError("encoding stage contains a non-regular file")
                signatures.append({"path": path.relative_to(stage).as_posix(), "source": _source_signature(info)})
                if collect_allocation:
                    # Same fresh lstat as the stage identity; do not count an
                    # inode with names outside this exact deletion scope.
                    key = (info.st_dev, info.st_ino)
                    record = inodes.setdefault(key, [info.st_nlink, info.st_blocks * 512, 0])
                    if record[:2] != [info.st_nlink, info.st_blocks * 512]:
                        raise SnapshotError("retirement inode changed during space audit")
                    record[2] += 1
    signatures.sort(key=lambda row: row["path"])
    if collect_allocation:
        reclaimable = sum(allocated for links, allocated, names in inodes.values() if links == names)
        return signatures, reclaimable
    return signatures


def _stage_prune_fingerprint(stage: Path, plan: dict[str, Any], *, file_workers: int = 1) -> str:
    identity = {"dataset": plan["dataset"], "snapshot_id": plan["snapshot_id"],
                "manifest_sha256": plan["manifest_sha256"],
                "signatures": _stage_prune_signatures(stage, file_workers=file_workers)}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def apply_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path,
    expected_fingerprint: str, owned_verified_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with _exclusive_lock(state_root / "retirements" / f"{spec.dataset}.lock"):
        return _apply_archive_stage_prune(spec, sync_root, artifact_root=artifact_root,
            state_root=state_root, expected_fingerprint=expected_fingerprint,
            owned_verified_plan=owned_verified_plan)


def _check_owned_stage_plan(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint):
    proof = plan.get("full_verification", {})
    if not 0 <= time.time() - proof.get("verified_at_epoch", 0) <= 1500:
        raise SnapshotError("owned encoding stage full verification expired or its generation changed")
    _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint)


def _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint):
    """Identity gates only; this grants neither fresh proof nor deletion rights."""
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.materialized_cache import _pinned_snapshot_ids
    _check_d_primary_mount(sync_root)
    proof = plan.get("full_verification", {})
    if (not _prunable_stage(spec) or plan.get("dataset") != spec.dataset
            or plan.get("stage") != str(spec.stage_root / spec.dataset)
            or plan.get("stage_fingerprint") != expected_fingerprint or plan.get("apply_ready") is not True
            or proof.get("contract") != "owned-archive-stage-verification-v1"
            or proof.get("artifact_root") != str(artifact_root.resolve())
            or proof.get("state_root") != str(state_root.resolve())
            or proof.get("cold_generation") != _stage_cold_generation(sync_root, spec)):
        raise SnapshotError("owned encoding stage full verification expired or its generation changed")
    state = json.loads((state_root / "retirements" / f"{spec.dataset}.json").read_text())
    source = artifact_root.resolve() / spec.relative_root
    if (state.get("state") != "cold-only" or state.get("dataset") != spec.dataset
            or state.get("relative_root") != spec.relative_root
            or state.get("snapshot_id") != plan["snapshot_id"]
            or proof["cold_generation"]["manifest_sha256"] != plan["manifest_sha256"]
            or proof["cold_generation"]["snapshot_id"] != plan["snapshot_id"]
            or source.exists() or source.is_symlink()
            or plan["snapshot_id"] in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized"))):
        raise SnapshotError("owned encoding stage retirement, source or pin gate changed")


def recover_archive_stage_quarantine(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path,
    owned_failed_plan: dict[str, Any], journal: Path,
) -> dict[str, Any]:
    """Return one exact interrupted quarantine for a new full audit, never unlink.

    An expired plan binds only names/generations here. It is not renewed and
    cannot authorize stage pruning. The normal full audit must run afterwards.
    """
    from stockagent.data_sync.materialized_cache import process_references
    with _exclusive_lock(state_root / 'retirements' / f'{spec.dataset}.lock'):
        plan = owned_failed_plan
        fingerprint = plan['stage_fingerprint']
        _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, fingerprint)
        stage = spec.stage_root / spec.dataset
        if stage.exists() or stage.is_symlink():
            raise SnapshotError('stage quarantine recovery refuses to replace an existing stage')
        candidates = [p for p in spec.stage_root.iterdir()
                      if re.fullmatch(re.escape(f'.{spec.dataset}.pruning-') + r'[1-9][0-9]*', p.name)]
        if len(candidates) != 1:
            raise SnapshotError('stage quarantine recovery requires one exact owned quarantine')
        quarantine = candidates[0]
        owner_pid = int(quarantine.name.rsplit('-', 1)[1])
        if (quarantine.is_symlink() or quarantine.resolve() != quarantine or not quarantine.is_dir()
                or Path(f'/proc/{owner_pid}').exists()):
            raise SnapshotError('stage quarantine recovery owner is active or path is redirected')
        workers = _stage_prune_metadata_workers(spec, stage)
        if (_stage_prune_fingerprint(quarantine, plan, file_workers=workers) != fingerprint
                or process_references(quarantine) or process_references(stage)):
            raise SnapshotError('stage quarantine changed or is in use; retained')
        _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, fingerprint)
        result = {'contract': 'owned-archive-stage-quarantine-recovery-v1',
                  'dataset': spec.dataset, 'snapshot_id': plan['snapshot_id'],
                  'manifest_sha256': plan['manifest_sha256'], 'stage_fingerprint': fingerprint,
                  'failed_plan_sha256': hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest(),
                  'quarantine': str(quarantine), 'stage': str(stage), 'state': 'restore_stage_intent',
                  'proof_time_extended': False, 'deletion_authorized': False,
                  'checked_at_epoch': time.time()}
        atomic_write_json(journal, result)
        os.rename(quarantine, stage)
        fd = os.open(stage.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        result = {**result, 'state': 'stage_restored_requires_full_audit', 'checked_at_epoch': time.time()}
        atomic_write_json(journal, result)
        return result


def _apply_archive_stage_prune(
    spec: LegacyArchiveSpec, sync_root: Path, *, artifact_root: Path, state_root: Path,
    expected_fingerprint: str, owned_verified_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from stockagent.data_sync.cold_primary import _check_d_primary_mount
    from stockagent.data_sync.materialized_cache import process_references, _pinned_snapshot_ids

    if owned_verified_plan is None:
        plan = plan_archive_stage_prune(spec, sync_root, artifact_root=artifact_root, state_root=state_root)
    else:
        plan = owned_verified_plan
        _check_owned_stage_plan(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint)
        stage = Path(plan["stage"])
        if stage.is_symlink() or stage.resolve() != stage or not stage.is_dir():
            raise SnapshotError("encoding stage is missing or redirected")
        metadata_workers = _stage_prune_metadata_workers(spec, stage)
        if _stage_prune_fingerprint(Path(plan["stage"]), plan, file_workers=metadata_workers) != expected_fingerprint:
            raise SnapshotError("encoding stage changed after dry run")
    if plan["stage_fingerprint"] != expected_fingerprint:
        raise SnapshotError("encoding stage changed after dry run")
    stage = Path(plan["stage"])
    workers = _d_stage_file_workers(spec, stage / 'archive')
    metadata_workers = _stage_prune_metadata_workers(spec, stage)
    quarantine = stage.with_name(f".{spec.dataset}.pruning-{os.getpid()}")
    if quarantine.exists() or quarantine.is_symlink():
        raise SnapshotError("unfinished encoding stage quarantine requires audit")
    os.replace(stage, quarantine)
    if _stage_prune_fingerprint(quarantine, plan, file_workers=metadata_workers) != expected_fingerprint:
        raise SnapshotError("encoding stage changed during rename; quarantine retained")
    verify_archive_directory(quarantine / "archive", spec=spec, file_workers=workers)
    _check_d_primary_mount(sync_root)
    resolved = resolve_latest_packed(sync_root, spec.dataset)
    if owned_verified_plan is None:
        inventory = _load_inventory(sync_root.resolve(), resolved.manifest)
        verify_packed_snapshot(sync_root, resolved,
                               reconstruct_paths=[row["path"] for row in inventory if row["kind"] == "file"],
                               **_native_read_options(sync_root))
    else:
        # Decode can take longer than the old proof lifetime on a small-file
        # HDD stage. Admit with a fresh proof before mutation; if it expires
        # during decoding, independently reconstruct the complete D release
        # again. This is new full verification, never a timestamp extension.
        _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint)
        age = time.time() - plan['full_verification']['verified_at_epoch']
        if age > 1500:
            cold_proof = verify_packed_snapshot(sync_root, resolved, reconstruct_all=True,
                                               **_native_read_options(sync_root))
            _check_stage_plan_binding(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint)
            old_proof = plan['full_verification']
            plan = {**plan, 'prior_full_verification': old_proof,
                    'quarantine_cold_reconstruction': cold_proof,
                    'full_verification': {**old_proof, 'verified_at_epoch': time.time(),
                        'verification_reason': 'complete_D_reconstruction_after_quarantine_decode'}}
        _check_owned_stage_plan(spec, sync_root, artifact_root, state_root, plan, expected_fingerprint)
    source = artifact_root.resolve() / spec.relative_root
    if (resolved.manifest_sha256 != plan["manifest_sha256"] or process_references(quarantine)
        or process_references(stage) or source.exists() or source.is_symlink()
        or str(resolved.manifest["snapshot_id"]) in _pinned_snapshot_ids(Path("/srv/stockagent-packed-materialized"))
        or _stage_prune_fingerprint(quarantine, plan, file_workers=metadata_workers) != expected_fingerprint):
        raise SnapshotError("encoding stage final gates changed; quarantine retained")
    shutil.rmtree(quarantine)
    return {**plan, "pruned": True, "reclaimed_allocated_file_bytes": plan["reclaimable_allocated_file_bytes"]}


def restore_archive(
    spec: LegacyArchiveSpec,
    sync_root: Path,
    destination: Path,
    *,
    materialized_root: Path,
    include_paths: list[str] | None = None,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    if destination.exists() or destination.is_symlink():
        raise SnapshotError(f"refusing to overwrite existing restore path: {destination}")
    resolved = (resolve_packed_snapshot_id(sync_root, spec.dataset, snapshot_id)
                if snapshot_id is not None else resolve_latest_packed(sync_root, spec.dataset))
    metadata = resolved.manifest.get("metadata", {})
    if (
        metadata.get("transport_role") != "legacy-quarantine-archive"
        or metadata.get("deployable") != "false"
        or metadata.get("source_relative_root") != spec.relative_root
    ):
        raise SnapshotError("release is not the selected legacy quarantine archive")
    archive = fetch_packed_snapshot(sync_root, materialized_root, resolved, **_native_read_options(sync_root))
    if metadata.get("legacy_manifest_sha256") != sha256_file(
        archive / "legacy_archive_manifest.json"
    ):
        raise SnapshotError("restored legacy manifest differs from cold release metadata")
    proof = verify_archive_directory(archive, spec=spec)
    manifest = proof["manifest"]
    selected = manifest["files"]
    selected_symlinks = list(manifest.get("symlinks", []))
    if include_paths is not None:
        requested = {_safe_relative_path(p, "requested restore member").as_posix()
                     for p in include_paths}
        if not requested:
            raise SnapshotError("selective restore requires at least one exact member")
        selected = [row for row in selected if row["path"] in requested]
        selected_symlinks = [row for row in selected_symlinks if row["path"] in requested]
        if {row["path"] for row in (*selected, *selected_symlinks)} != requested:
            raise SnapshotError("selected restore members are absent from the fixed archive")
    temporary = destination.with_name(destination.name + f".partial-{os.getpid()}")
    if temporary.exists() or temporary.is_symlink():
        raise SnapshotError(f"stale restore staging path needs audit: {temporary}")
    temporary.mkdir(parents=True, exist_ok=False, mode=0o700 if _opaque_symlinks_enabled(spec) else 0o777)
    directories = [row for row in manifest.get("directories", [])
                   if include_paths is None or any(
                       PurePosixPath(row["path"]) in PurePosixPath(item["path"]).parents
                       for item in (*selected, *selected_symlinks))]
    for row in directories:
        temporary.joinpath(*PurePosixPath(row["path"]).parts).mkdir(parents=True, exist_ok=True)
    for row in selected:
        relative = PurePosixPath(row["path"])
        target = temporary.joinpath(*relative.parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = archive.joinpath(*PurePosixPath(row["encoded_path"]).parts)
        digest = hashlib.sha256()
        with _decoded_reader(encoded, row["codec"]) as stream, target.open("xb") as output:
            while chunk := stream.read(COPY_CHUNK):
                output.write(chunk)
                digest.update(chunk)
        if digest.hexdigest() != row["original_sha256"] or target.stat().st_size != row["source"]["size"]:
            raise SnapshotError(f"restored legacy file differs: {relative}")
        target.chmod(row["source"]["mode"])
        os.utime(target, ns=(target.stat().st_atime_ns, row["source"]["mtime_ns"]))
    # All real directories and files are already populated. Create links last,
    # and never open their targets, even for an absolute or dangling link.
    for row in selected_symlinks:
        target = temporary.joinpath(*PurePosixPath(row["path"]).parts)
        link_bytes = base64.b64decode(row["target_b64"], validate=True)
        os.symlink(link_bytes, os.fsencode(target))
        os.utime(target, ns=(row["source"]["mtime_ns"], row["source"]["mtime_ns"]), follow_symlinks=False)
        info = target.lstat()
        if (not stat.S_ISLNK(info.st_mode) or info.st_size != row["source"]["size"]
                or stat.S_IMODE(info.st_mode) != row["source"]["mode"]
                or info.st_mtime_ns != row["source"]["mtime_ns"]
                or os.readlink(os.fsencode(target)) != link_bytes):
            raise SnapshotError(f"restored legacy symlink differs: {row['path']}")
    for row in sorted(directories, key=lambda r: len(PurePosixPath(r["path"]).parts), reverse=True):
        target = temporary.joinpath(*PurePosixPath(row["path"]).parts)
        target.chmod(row["mode"])
        os.utime(target, ns=(target.stat().st_atime_ns, row["mtime_ns"]))
    atomic_write_json(
        temporary / ".LEGACY_RESTORED.json",
        {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "manifest_sha256": resolved.manifest_sha256, "deployable": False,
         "restored_members": [row["path"] for row in (*selected, *selected_symlinks)],
         "restore_scope": "selected_exact_members" if include_paths is not None else "complete_archive"},
    )
    if destination.exists() or destination.is_symlink():
        raise SnapshotError(f"restore destination appeared during verification: {destination}")
    os.replace(temporary, destination)
    result = {"dataset": spec.dataset, "snapshot_id": resolved.manifest["snapshot_id"], "destination": str(destination),
              "files": len(selected), "original_bytes": sum(row["source"]["size"] for row in selected),
              "archive_files_verified": proof["files"], "restore_scope": "selected_exact_members" if include_paths is not None else "complete_archive"}
    if _opaque_symlinks_enabled(spec):
        result["symlinks"] = len(selected_symlinks)
    return result
