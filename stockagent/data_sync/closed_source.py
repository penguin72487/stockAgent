"""Fixed cold ownership for the closed CFTC pre-2000 archive.

The three local Parquet views remain the website's bounded working set.  A
binding never grants permission to unlink originals: retirement has its own
recovery, consumer, transport and quarantine gates.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from stockagent.data_sync.desync_snapshots import (
    SnapshotError, write_immutable_json,
)
from stockagent.data_sync import packed_snapshots as packed

CONTRACT = "cftc_pre2000_closed_raw_v1"
DATASET = "cftc-legacy-pre2000"
BINDING = "state/closed-source.json"
RETIREMENT_RECEIPT = "state/raw-retirement.json"
LOCK = "state/collector.lock"
SERVICE_VIEWS = frozenset({
    "normalized/legacy_pre2000.parquet",
    "normalized/legacy_futures_only_pre2000.parquet",
    "normalized/legacy_futures_and_options_combined_pre2000.parquet",
})
_CONTROL = frozenset({"README.md", "manifest.json", LOCK,
    "receipts/legacy_futures_only.json",
    "receipts/legacy_futures_and_options_combined.json"})
_RAW = re.compile(r"raw/(legacy_futures_only|legacy_futures_and_options_combined)_([0-9a-f]{64})\.zip\Z")


def validate_catalog(entry: dict) -> None:
    if (entry.get("closed_source_contract") != CONTRACT
            or entry.get("dataset") != DATASET
            or not entry.get("source_coordination_lock")
            or entry.get("source_coordination_lock_mode", "exclusive") != "exclusive"
            or entry.get("included_root_files") or entry.get("excluded_subtrees")):
        raise SnapshotError("closed CFTC source requires its complete catalog and exclusive collector owner")


def _root(source: Path) -> Path:
    source = source.absolute()
    if source.resolve(strict=True) != source or not source.is_dir():
        raise SnapshotError("closed source root is redirected or absent")
    return source


def _signature(info: os.stat_result) -> tuple:
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _read(path: Path, *, limit: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            raise SnapshotError(f"closed source member is not bounded regular data: {path.name}")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            value = handle.read(limit + 1)
        if (len(value) != before.st_size
                or _signature(before) != _signature(os.fstat(descriptor))
                or _signature(before) != _signature(path.lstat())):
            raise SnapshotError("closed source member changed during read")
        return value
    finally:
        os.close(descriptor)


def _json(path: Path) -> dict:
    try:
        value = json.loads(_read(path, limit=64 * 1024))
    except (OSError, ValueError) as error:
        raise SnapshotError(f"cannot read closed source control: {path.name}: {error}") from error
    if not isinstance(value, dict):
        raise SnapshotError("closed source control must be an object")
    return value


def _hash(path: Path, size: int) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size != size:
            raise SnapshotError("closed source hash input differs")
        digest = hashlib.sha256()
        count = 0
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            while block := handle.read(1024 * 1024):
                count += len(block)
                if count > size:
                    raise SnapshotError("closed source grew while hashing")
                digest.update(block)
        if (count != size or _signature(before) != _signature(os.fstat(descriptor))
                or _signature(before) != _signature(path.lstat())):
            raise SnapshotError("closed source changed while hashing")
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def configured_cache_root(cold: Path, retention_config: Path, *, repo_root: Path) -> Path:
    from stockagent.data_sync.packed_retention import RetentionConfig
    config = RetentionConfig.load(retention_config, repo_root=repo_root)
    if config.sync_root != cold.absolute():
        raise SnapshotError("closed source pin owner does not cover this cold authority")
    return config.materialized_root


def _files(cold: Path, resolved) -> dict:
    with packed._open_inventory(cold, resolved.manifest) as inventory:
        packed._validate_inventory(resolved.manifest, inventory)
        rows = {}
        directories = set()
        for row in inventory:
            if len(rows) + len(directories) >= 32:
                raise SnapshotError("closed CFTC inventory exceeds its explicit archive scope")
            if row["kind"] == "directory":
                directories.add(row["path"])
            elif row["kind"] == "file":
                rows[row["path"]] = {"bytes": row["size"], "sha256": row["sha256"]}
            else:
                raise SnapshotError("closed CFTC inventory contains a non-file member")
    _file_scope(rows)
    if directories != {"raw", "normalized", "receipts", "state"}:
        raise SnapshotError("fixed release differs from the closed CFTC directory scope")
    return rows


def _file_scope(rows: dict) -> None:
    if not all(isinstance(name, str) and isinstance(row, dict) for name, row in rows.items()):
        raise SnapshotError("closed CFTC file inventory must contain named file records")
    raws = {name for name in rows if _RAW.fullmatch(name)}
    if (set(rows) != _CONTROL | SERVICE_VIEWS | raws or len(raws) != 2
            or {_RAW.fullmatch(name).group(1) for name in raws} != {
                "legacy_futures_only", "legacy_futures_and_options_combined"}
            or any(type(row.get("bytes")) is not int or row["bytes"] < 0
                   or re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256"))) is None
                   for row in rows.values())
            or any(_RAW.fullmatch(name).group(2) != rows[name]["sha256"] for name in raws)
            or rows[LOCK]["bytes"] != 0):
        raise SnapshotError("fixed release differs from the closed CFTC archive scope")


def require_unbound_originals(source: Path) -> None:
    """A not-yet-adopted workspace may publish only both original ZIPs."""
    source = _root(source)
    for mode in ("legacy_futures_only", "legacy_futures_and_options_combined"):
        receipt = _json(source / "receipts" / f"{mode}.json")
        name = receipt.get("raw_path")
        match = _RAW.fullmatch(name) if isinstance(name, str) else None
        if (match is None or match.group(1) != mode
                or match.group(2) != receipt.get("raw_sha256")
                or type(receipt.get("raw_bytes")) is not int or receipt["raw_bytes"] < 1):
            raise SnapshotError("unbound closed CFTC source lacks its complete raw receipts")
        path = source / name
        if path.resolve() != path or _hash(path, receipt["raw_bytes"]) != receipt["raw_sha256"]:
            raise SnapshotError("unbound closed CFTC source lacks an original ZIP")


def _census(source: Path, files: dict) -> dict:
    allowed = set(files) | {BINDING, RETIREMENT_RECEIPT}
    result = {".": _signature(source.lstat())}
    for directory, subdirs, names in os.walk(source, followlinks=False):
        for name in subdirs + names:
            path = Path(directory) / name
            relative = path.relative_to(source).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                if relative not in {"raw", "normalized", "receipts", "state"}:
                    raise SnapshotError(f"unknown closed source directory: {relative}")
            elif not stat.S_ISREG(info.st_mode) or relative not in allowed:
                raise SnapshotError(f"unknown or redirected closed source member: {relative}")
            result[relative] = _signature(info)
            if len(result) > 17:
                raise SnapshotError("closed source exceeds its bounded workspace")
    missing = set(files) - set(result)
    if any(not _RAW.fullmatch(name) for name in missing):
        raise SnapshotError("closed source service/control member is missing")
    return result


def _workspace(source: Path, files: dict, *, hash_files: bool) -> dict:
    before = _census(source, files)
    for name, row in files.items():
        if name not in before:
            continue
        if before[name][4] != row["bytes"]:
            raise SnapshotError(f"closed source size differs: {name}")
        if hash_files and _hash(source / name, row["bytes"]) != row["sha256"]:
            raise SnapshotError(f"closed source SHA differs: {name}")
    if before != _census(source, files):
        raise SnapshotError("closed source workspace changed during verification")
    raw = {name for name in files if _RAW.fullmatch(name)}
    return {"raw_files_local": len(raw & before.keys()),
            "raw_bytes_local": sum(files[name]["bytes"] for name in raw & before.keys()),
            "raw_bytes_in_fixed_D": sum(files[name]["bytes"] for name in raw),
            "service_files": len(SERVICE_VIEWS),
            "service_bytes": sum(files[name]["bytes"] for name in SERVICE_VIEWS),
            "workspace_hashes_verified": hash_files,
            "raw_workspace_empty": not bool(raw & before.keys())}


def _retirement_status(source: Path, files: dict, binding: dict, status: dict) -> dict:
    receipt_path = source / RETIREMENT_RECEIPT
    accepted = False
    retired_at = None
    if receipt_path.exists() or receipt_path.is_symlink():
        receipt = _json(receipt_path)
        raw = {name: row for name, row in files.items() if _RAW.fullmatch(name)}
        if (receipt.get("contract") != "closed_source_raw_retirement_v1"
                or receipt.get("state") != "cold-raw-retired"
                or receipt.get("source") != str(source)
                or receipt.get("snapshot_id") != binding.get("snapshot_id")
                or receipt.get("manifest_sha256") != binding.get("manifest_sha256")
                or receipt.get("retired_members") != raw
                or receipt.get("deleted_names") != len(raw)):
            raise SnapshotError("closed source raw retirement receipt differs from its fixed source")
        recovery = receipt.get("post_unlink_original_recovery")
        evidence = receipt.get("unlinked_member_evidence")
        if (not isinstance(recovery, dict)
                or recovery.get("snapshot_id") != binding.get("snapshot_id")
                or recovery.get("manifest_sha256") != binding.get("manifest_sha256")
                or recovery.get("inventory_sha256") != binding.get("inventory_sha256")
                or recovery.get("independently_reconstructed_files") != len(files)
                or recovery.get("independently_reconstructed_bytes") != sum(row["bytes"] for row in files.values())
                or not isinstance(evidence, dict) or set(evidence) != set(raw)
                or any(not isinstance(row, dict)
                       or type(row.get("device")) is not int or row["device"] < 1
                       or type(row.get("inode")) is not int or row["inode"] < 1
                       or type(row.get("allocated_bytes")) is not int or row["allocated_bytes"] < 0
                       or row.get("links_before") != 1 or row.get("links_after") != 0
                       or row.get("descriptor_closed") is not True for row in evidence.values())
                or receipt.get("reclaimed_allocated_file_bytes") != sum(row["allocated_bytes"] for row in evidence.values())
                or not isinstance(receipt.get("retired_at_utc"), str)):
            raise SnapshotError("closed source raw retirement lacks complete unlink and recovery evidence")
        try:
            if datetime.fromisoformat(receipt["retired_at_utc"]).utcoffset() is None:
                raise ValueError("retirement timestamp has no timezone")
        except ValueError as error:
            raise SnapshotError("closed source raw retirement timestamp is invalid") from error
        accepted = status["raw_workspace_empty"]
        retired_at = receipt["retired_at_utc"] if accepted else None
    return {**status, "raw_retirement_complete": accepted,
            "raw_retired_at_utc": retired_at,
            "raw_absence_without_retirement_receipt": status["raw_workspace_empty"] and not accepted}


def _source_owner(source: Path, descriptor: int) -> None:
    if type(descriptor) is not int or descriptor < 3:
        raise SnapshotError("closed source adoption requires its held original collector descriptor")
    expected, opened = (source / LOCK).lstat(), os.fstat(descriptor)
    info = Path(f"/proc/self/fdinfo/{descriptor}").read_text()
    if (not stat.S_ISREG(expected.st_mode) or expected.st_nlink != 1
            or not os.path.samestat(expected, opened)
            or not any(line.startswith("lock:") and {"FLOCK", "WRITE"}.issubset(line.split())
                       for line in info.splitlines())):
        raise SnapshotError("closed source adoption has no exclusive original collector owner")


def _pin_path(materialized_root: Path) -> Path:
    root = materialized_root.absolute()
    if root.resolve() != root:
        raise SnapshotError("closed source retention root is redirected")
    path = root / "pins" / "closed-sources" / f"{DATASET}.pin.json"
    for parent in (path.parent, path.parent.parent):
        if parent.exists() and (parent.resolve() != parent or not parent.is_dir()):
            raise SnapshotError("closed source pin parent is redirected")
    return path


def resolve_closed_source(source: Path, cold: Path, materialized_root: Path,
                          *, hash_files: bool = True):
    """Read the exact binding; absent binding is the ordinary collector path.

    Normal use verifies the small local working set and fixed cold metadata,
    without cold payload materialization, API calls or rewritten Parquet.
    """
    source = _root(source)
    marker = source / BINDING
    if not marker.exists() and not marker.is_symlink():
        return None
    packed._check_packed_read_authority(cold)
    with packed._try_publication_lock(cold / ".local-state/locks/publish-retention-global.lock",
                                      shared=True):
        return _resolve_closed_source(source, cold, materialized_root, hash_files=hash_files)


def _resolve_closed_source(source: Path, cold: Path, materialized_root: Path, *, hash_files: bool):
    marker = source / BINDING
    binding_bytes = _read(marker, limit=64 * 1024)
    try:
        binding = json.loads(binding_bytes)
    except ValueError as error:
        raise SnapshotError("invalid closed source binding JSON") from error
    if (not isinstance(binding, dict) or binding.get("contract") != CONTRACT or binding.get("source") != str(source)
            or binding.get("dataset") != DATASET):
        raise SnapshotError("closed source binding has an incompatible identity")
    pin = _pin_path(materialized_root)
    resolved = packed.resolve_packed_pin(cold, pin, expected_dataset=DATASET)
    if (resolved.manifest["snapshot_id"] != binding.get("snapshot_id")
            or resolved.manifest_sha256 != binding.get("manifest_sha256")
            or resolved.manifest["archive"]["inventory"]["sha256"] != binding.get("inventory_sha256")):
        raise SnapshotError("closed source fixed pin differs from its binding")
    files = _files(cold, resolved)
    if binding.get("workspace_files") != {name: {"bytes": row["bytes"], "sha256": row["sha256"]}
                                         for name, row in files.items()}:
        raise SnapshotError("closed source working set differs from its cold inventory")
    recovery = binding.get("recovery", {})
    if (not isinstance(recovery, dict)
            or recovery.get("manifest_sha256") != resolved.manifest_sha256
            or recovery.get("materialized_verified") is not True
            or recovery.get("independently_reconstructed_files") != len(files)
            or recovery.get("independently_reconstructed_bytes") != sum(row["bytes"] for row in files.values())):
        raise SnapshotError("closed source binding lacks complete original recovery")
    status = _retirement_status(source, files, binding, _workspace(source, files, hash_files=hash_files))
    if _read(marker, limit=64 * 1024) != binding_bytes:
        raise SnapshotError("closed source binding changed during verification")
    return resolved, {"contract": CONTRACT, "snapshot_id": resolved.manifest["snapshot_id"],
                      "manifest_sha256": resolved.manifest_sha256,
                      "raw_authority": "D_fixed_release", "local_role": "service_views_and_control",
                      "adopted_at_utc": binding.get("adopted_at_utc"),
                      "cold_payload_reconstructed_this_check": False, **status}


def local_closed_source_status(source: Path) -> dict | None:
    """Public-safe metadata projection; never opens cold objects or raw data."""
    source = _root(source)
    marker = source / BINDING
    if not marker.exists() and not marker.is_symlink():
        return None
    binding = _json(marker)
    if (binding.get("contract") != CONTRACT or binding.get("source") != str(source)
            or binding.get("dataset") != DATASET):
        raise SnapshotError("closed source local receipt identity differs")
    files = binding.get("workspace_files")
    if not isinstance(files, dict):
        raise SnapshotError("closed source local receipt omits its working set")
    _file_scope(files)
    return {"contract": CONTRACT, "snapshot_id": binding.get("snapshot_id"),
            "manifest_sha256": binding.get("manifest_sha256"),
            "raw_authority": "D_fixed_release", "local_role": "service_views_and_control",
            "verification_scope": "adoption_receipt_and_current_local_sizes",
            "cold_payload_reverified_this_check": False,
            **_retirement_status(source, files, binding, _workspace(source, files, hash_files=False))}


def adopt_closed_source(source: Path, cold: Path, materialized_root: Path, *,
                        snapshot_id: str, source_lock_fd: int, progress=None):
    """Adopt a fully reconstructed fixed release, preserving every source name."""
    source = _root(source)
    _source_owner(source, source_lock_fd)
    packed._check_packed_read_authority(cold)
    owner = cold / ".local-state/locks/publish-retention-global.lock"
    with packed._try_publication_lock(owner, shared=True):
        existing = resolve_closed_source(source, cold, materialized_root)
        if existing is not None:
            if existing[0].manifest["snapshot_id"] != snapshot_id:
                raise SnapshotError("closed source is already bound to a different release")
            return existing
        resolved = packed.resolve_packed_snapshot_id(cold, DATASET, snapshot_id)
        files = _files(cold, resolved)
        if _workspace(source, files, hash_files=True)["raw_files_local"] != 2:
            raise SnapshotError("initial closed source adoption requires all original raw files")
        native = cold == Path("/srv/stockagent-packed")
        proof = packed.verify_packed_snapshot(cold, resolved, materialized_path=source,
            reconstruct_all=True, d_primary_native_blob_reads=native,
            d_primary_native_pack_reads=native, progress=progress)
        if (not proof["materialized_verified"]
                or proof["independently_reconstructed_files"] != len(files)
                or proof["independently_reconstructed_bytes"] != sum(row["bytes"] for row in files.values())):
            raise SnapshotError("closed source lacks complete original cold recovery")
        _source_owner(source, source_lock_fd)
        _workspace(source, files, hash_files=True)
        pin = _pin_path(materialized_root)
        if pin.is_relative_to(source) or pin.is_relative_to(cold):
            raise SnapshotError("closed source retention pin must use the separate managed cache root")
        pin.parent.mkdir(parents=True, exist_ok=True)
        if pin.exists() or pin.is_symlink():
            current = packed.resolve_packed_pin(cold, pin, expected_dataset=DATASET)
            if current.manifest_sha256 != resolved.manifest_sha256:
                raise SnapshotError("existing closed source pin names a different release")
        else:
            packed.write_packed_pin(pin, resolved, exclusive=True)
        write_immutable_json(source / BINDING, {
            "contract": CONTRACT, "dataset": DATASET, "source": str(source),
            "snapshot_id": snapshot_id, "manifest_sha256": resolved.manifest_sha256,
            "inventory_sha256": resolved.manifest["archive"]["inventory"]["sha256"],
            "workspace_files": {name: {"bytes": row["bytes"], "sha256": row["sha256"]}
                                for name, row in files.items()},
            "adopted_at_utc": datetime.now(timezone.utc).isoformat(),
            "recovery": proof, "source_names_deleted": 0,
            "retirement_authorized_by_binding": False,
        })
        return resolve_closed_source(source, cold, materialized_root)
