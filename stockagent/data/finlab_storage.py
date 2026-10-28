"""Lossless native FinLab history sharing under its original sync owner.

Every receipt and logical Parquet name remains. Only byte-identical, verified
source payloads share readonly inodes. This neither exports licensed data nor
claims that distinct historical revisions have been partition-migrated.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time
import uuid

from downloader.artifact_io import atomic_write_json, durable_replace
from stockagent.data_sync.immutable_cache_compaction import (
    allocated_bytes, duplicate_payload_groups, inode_key, share_locked_payload_group,
)
from stockagent.data_sync.immutable_cache_files import link_immutable_payload, payload_info, stable_payload_hash
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.physical_cache_reuse import _metadata, _real_directory, _writer_lock


CONTRACT = "finlab_exact_native_history_sharing_v1"
WORKING_SET_CONTRACT = "finlab_native_working_set_inventory_v1"
HASH = re.compile(r"[0-9a-f]{64}\Z")


@contextmanager
def _sync_owner(root: Path, wait_seconds: float):
    if not math.isfinite(wait_seconds) or not 0 <= wait_seconds <= 3600:
        raise ValueError("FinLab owner wait must be finite and between 0 and 3600 seconds")
    deadline = time.monotonic() + wait_seconds
    with ExitStack() as lease:
        while True:
            try:
                lease.enter_context(_writer_lock(root / ".sync.lock"))
                break
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(0.5, remaining))
        yield


def install_history_payload(temporary: Path, destination: Path, digest: str) -> str:
    """Original writer: share an accepted readonly donor, otherwise install once."""
    if not HASH.fullmatch(digest) or not destination.name.endswith(f"-{digest[:24]}.parquet"):
        raise ValueError("FinLab immutable payload name/hash differs")
    size = temporary.stat().st_size
    for donor in sorted(destination.parent.glob(f"*-{digest[:24]}.parquet")):
        if donor != destination and link_immutable_payload(donor, destination, sha256=digest, size=size):
            return "shared_exact_native_payload"
    os.chmod(temporary, 0o444, follow_symlinks=False)
    durable_replace(temporary, destination)
    return "new_immutable_native_payload"


def _generation(path: Path) -> tuple:
    info = path.lstat()
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def inventory_working_set(root: Path, receipt_dir: Path, *, wait_owner_seconds: float = 0) -> dict:
    """Classify flat general-table payloads by exact receipt references.

    Borrow the existing FinLab writer boundary. Read only receipt bytes and
    filesystem metadata, with no source API, payload hashing or deletion.
    Historical and unreferenced names remain protected: neither is a recovery
    proof. Intraday partitions, SDK caches and derived overlays are outside this
    explicitly bounded inventory.
    """
    root = _real_directory(root)
    receipt_dir = receipt_dir.absolute()
    if (receipt_dir.resolve() != receipt_dir or receipt_dir == root
            or root in receipt_dir.parents or receipt_dir in root.parents):
        raise ValueError("FinLab inventory reports must be outside the source")
    receipt_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    report = {"contract": WORKING_SET_CONTRACT, "root": str(root), "state": "running",
        "scope": "general_tables_raw_datasets_and_receipts; intraday_sdk_and_overlays_excluded",
        "source_payload_reads": 0, "source_payload_writes": 0, "provider_API_calls": 0,
        "deletion_authorized": False, "source_files_deleted": 0,
        "payload_SHA_reverified": False, "cold_recovery_verified": False,
        "issues": [], "files": [], "controls": {}, "metadata_bytes_read": 0}
    directories, generations, references = {}, {}, {}
    issues = report["issues"]

    def entries(path):
        _real_directory(path)
        before = _generation(path)
        values = sorted(path.iterdir())
        if _generation(path) != before:
            raise ValueError("FinLab directory changed during inventory")
        directories[path] = before
        return values

    def read_receipt(path, category, *, raw_only=False):
        before = _generation(path)
        if not stat.S_ISREG(before[2]) or path.resolve(strict=True) != path:
            raise ValueError("FinLab receipt is redirected or not regular")
        report["metadata_bytes_read"] += before[5]
        if report["metadata_bytes_read"] > 512 * 1024**2 or len(generations) >= 100000:
            raise ValueError("FinLab receipt inventory exceeds its metadata bound")
        value, digest = _metadata(path)
        if _generation(path) != before:
            raise ValueError("FinLab receipt changed during inventory")
        generations[path] = before
        receipt_name = path.relative_to(root).as_posix()
        report["controls"][receipt_name] = {"sha256": digest, "generation": list(before)}
        if not isinstance(value.get("dataset"), str) or not value["dataset"]:
            issues.append({"receipt": receipt_name, "reason": "missing_dataset_identity"})
        kinds = [("raw_path", "raw_sha256", "raw_bytes", "raw")]
        if not raw_only:
            kinds.insert(0, ("parquet_path", "sha256", "parquet_size_bytes", "datasets"))
        for field, sha_field, size_field, parent in kinds:
            name = value.get(field)
            if name is None and field == "raw_path" and not raw_only:
                continue
            relative = Path(name) if isinstance(name, str) else None
            if (relative is None or relative.parent != Path(parent)
                    or relative.as_posix() != name or ".." in relative.parts):
                issues.append({"receipt": receipt_name, "field": field, "reason": "invalid_payload_path"})
                continue
            reference = references.setdefault(name, {"current_receipts": [], "historical_receipts": [],
                "expected_sha256": set(), "expected_bytes": set()})
            reference[category].append(receipt_name)
            digest = value.get(sha_field)
            if not isinstance(digest, str) or not HASH.fullmatch(digest):
                issues.append({"receipt": receipt_name, "field": sha_field, "reason": "invalid_payload_SHA"})
            else:
                reference["expected_sha256"].add(digest)
            size = value.get(size_field)
            if size is not None:
                if type(size) is not int or size < 0:
                    issues.append({"receipt": receipt_name, "field": size_field, "reason": "invalid_payload_size"})
                else:
                    reference["expected_bytes"].add(size)

    try:
        with _sync_owner(root, wait_owner_seconds):
            controls = {name: entries(root / name) for name in ("receipts", "raw_receipts", "versions")}
            for parent in ("receipts", "raw_receipts"):
                for path in controls[parent]:
                    if path.suffix == ".json":
                        read_receipt(path, "current_receipts", raw_only=parent == "raw_receipts")
            for directory in controls["versions"]:
                for path in entries(directory):
                    if path.suffix == ".json":
                        read_receipt(path, "historical_receipts")
            payloads = {}
            for parent in ("raw", "datasets"):
                for path in entries(root / parent):
                    before = _generation(path)
                    name = path.relative_to(root).as_posix()
                    if not stat.S_ISREG(before[2]) or path.resolve(strict=True) != path:
                        issues.append({"path": name, "reason": "payload_not_unredirected_regular_file"})
                        continue
                    info = payload_info(path)
                    if _generation(path) != before:
                        raise ValueError("FinLab payload metadata changed during inventory")
                    generations[path] = before
                    payloads[name] = info
            if len(payloads) > 100000:
                raise ValueError("FinLab general-table inventory exceeds its file bound")
            for name, reference in references.items():
                if name not in payloads:
                    issues.append({"path": name, "reason": "referenced_payload_absent_or_unsafe"})
                if len(reference["expected_sha256"]) > 1 or len(reference["expected_bytes"]) > 1:
                    issues.append({"path": name, "reason": "contradictory_receipt_identity"})
                if name in payloads and reference["expected_bytes"] - {payloads[name]["size"]}:
                    issues.append({"path": name, "reason": "receipt_size_differs_from_current_file"})
            for name, info in sorted(payloads.items()):
                reference = references.get(name, {"current_receipts": [], "historical_receipts": [],
                    "expected_sha256": set(), "expected_bytes": set()})
                category = ("current_referenced" if reference["current_receipts"] else
                            "historical_only" if reference["historical_receipts"] else "unreferenced_preserve")
                report["files"].append({"path": name, "category": category, **info,
                    **{key: sorted(value) for key, value in reference.items()}})
            # Assign a shared inode's allocated bytes to its strongest current
            # use. A historical alias of an active payload cannot free that inode.
            categories = ("current_referenced", "historical_only", "unreferenced_preserve")
            summaries, seen = {}, set()
            for category in categories:
                selected = [row for row in report["files"] if row["category"] == category]
                allocated = 0
                for row in selected:
                    key = inode_key(row)
                    if key not in seen:
                        seen.add(key)
                        allocated += row["allocated"]
                summaries[category] = {"files": len(selected),
                    "logical_bytes": sum(row["size"] for row in selected),
                    "unique_allocated_bytes_assigned": allocated}
            if any(_generation(path) != before for path, before in {**directories, **generations}.items()):
                raise ValueError("FinLab source generation changed before inventory acceptance")
            report.update(state="incomplete" if issues else "inventoried", categories=summaries,
                unique_payload_inodes=len(seen), receipt_files=len(report["controls"]),
                source_owner_boundary_verified=True, metadata_complete=not issues,
                total_logical_bytes=sum(row["size"] for row in report["files"]),
                total_unique_allocated_bytes=sum(row["unique_allocated_bytes_assigned"] for row in summaries.values()))
    except BlockingIOError:
        report.update(state="deferred", reason="native_sync_owner_busy", metadata_complete=False)
    report["observed_at_utc"] = datetime.now(UTC).isoformat()
    name = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    path = receipt_dir / (name + ".working-set.json")
    report["result_path"] = str(path)
    atomic_write_json(path, report, durable=True)
    return report


def _inventory(root: Path):
    files, controls, skipped = {}, {}, []
    paths = sorted((root / "receipts").glob("*.json"))
    paths += sorted((root / "versions").glob("*/*.json"))
    if not paths:
        raise ValueError("FinLab native history has no source receipts")
    for path in paths:
        if path.resolve(strict=True) != path:
            raise ValueError("FinLab source receipt is redirected")
        receipt, sha = _metadata(path)
        controls[str(path.relative_to(root))] = sha
        name, digest = receipt.get("parquet_path"), receipt.get("sha256")
        if not isinstance(name, str) or not isinstance(digest, str) or not HASH.fullmatch(digest):
            skipped.append({"receipt": str(path.relative_to(root)), "reason": "unsupported_receipt"})
            continue
        relative = Path(name)
        if (relative.parent != Path("datasets") or relative.as_posix() != name
                or not relative.name.endswith(f"-{digest[:24]}.parquet")):
            skipped.append({"receipt": str(path.relative_to(root)), "reason": "legacy_name_retained"})
            continue
        payload = root / relative
        if payload.resolve(strict=True) != payload:
            raise ValueError("FinLab native payload is redirected")
        info = payload_info(payload)
        size = receipt.get("parquet_size_bytes")
        # Old receipts omitted size but did retain the full original SHA. Those
        # payloads are admitted only by exact SHA before any sharing, not guessed
        # or backfilled metadata. A malformed/present size remains a hard error.
        if ((size is not None and (type(size) is not int or info["size"] != size))
                or info["mode"] & ~0o666 or not info["mode"] & 0o400
                or info["uid"] != os.geteuid() or info["xattrs"].get("system.posix_acl_access")):
            raise ValueError("FinLab native payload identity/size/permission differs")
        row = {"path": name, "sha256": digest, **info}
        if name in files and files[name] != row:
            raise ValueError("contradictory FinLab history receipts")
        files[name] = row
    return sorted(files.values(), key=lambda row: row["path"]), controls, skipped


def compact_history(root: Path, receipt_dir: Path, *, apply: bool = False,
                    expected_fingerprint: str | None = None,
                    wait_owner_seconds: float = 0,
                    reference_probe=process_references_many) -> dict:
    root = _real_directory(root)
    _real_directory(root / "datasets")
    receipt_dir = receipt_dir.absolute()
    if (receipt_dir.resolve() != receipt_dir or receipt_dir == root
            or root in receipt_dir.parents or receipt_dir in root.parents):
        raise ValueError("FinLab migration receipts must be outside the source")
    receipt_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    report = {"contract": CONTRACT, "state": "running", "root": str(root), "apply": apply,
              "replaced_paths": 0, "logical_paths_removed": 0, "historical_receipts_modified": 0,
              "cold_objects_removed": 0, "nas_archive_files_removed": 0, "retired_allocated_bytes": 0,
              "complete_native_partition_migration": False}
    try:
        with _sync_owner(root, wait_owner_seconds):
            files, controls, skipped = _inventory(root)
            groups, external = duplicate_payload_groups(files)
            candidates = [item for group in groups for names in group for item in names]
            keys = {inode_key(item) for item in candidates}
            references = reference_probe([root / "datasets"], inode_keys=keys)
            if references:
                report.update(state="deferred", reason="process_references", references=references)
            else:
                for item in candidates:
                    if stable_payload_hash(root / item["path"], payload_info(root / item["path"])) != item["sha256"]:
                        raise ValueError("FinLab duplicate payload differs from its original receipt SHA")
                if _inventory(root) != (files, controls, skipped):
                    raise ValueError("FinLab history changed during exact audit")
                fingerprint = hashlib.sha256(json.dumps({"files": files, "controls": controls},
                                                          sort_keys=True).encode()).hexdigest()
                if expected_fingerprint and expected_fingerprint != fingerprint:
                    raise ValueError("FinLab migration plan changed")
                plan_path = receipt_dir / (stamp + ".plan.json")
                atomic_write_json(plan_path, {"contract": CONTRACT, "root": str(root),
                    "fingerprint": fingerprint, "controls": controls,
                    "groups": groups, "scope": "exact duplicate payloads only"}, durable=True)
                report.update(state="audited", plan_fingerprint=fingerprint, plan_path=str(plan_path),
                              receipt_count=len(controls), retained_payload_paths=len(files),
                              skipped_receipts=skipped, external_link_inodes_retained=external,
                              duplicate_groups=len(groups), allocated_before=allocated_bytes(files),
                              candidate_allocated_bytes=sum(names[0]["allocated"] for g in groups for names in g[1:]))
                if apply:
                    references = reference_probe([root / "datasets"], inode_keys=keys)
                    if references:
                        report.update(state="deferred", reason="reader_started_before_apply", references=references)
                    else:
                        retired, touched = set(), set()
                        with ExitStack() as handles:
                            for group in groups:
                                retired |= share_locked_payload_group(root, group, touched=touched, report=report,
                                    retired_handles=handles, temporary_tag="source-share")
                            references = reference_probe([], inode_keys=retired)
                            if not references:
                                report["retired_allocated_bytes"] = sum(
                                    {inode_key(row): row["allocated"] for row in files if inode_key(row) in retired}.values())
                        after, after_controls, after_skipped = _inventory(root)
                        if (after_controls != controls or after_skipped != skipped
                                or [(r["path"], r["sha256"]) for r in after]
                                != [(r["path"], r["sha256"]) for r in files]):
                            raise ValueError("FinLab receipt identity changed after immutable sharing")
                        report.update(state="shared", retired_inode_references=references,
                                      allocated_after=allocated_bytes(after), receipts_readback="unchanged")
    except BlockingIOError:
        report.update(state="deferred", reason="native_sync_owner_busy")
    except BaseException as error:
        report.update(state="failed", error_type=type(error).__name__)
        raise
    finally:
        report["result_path"] = str(receipt_dir / (stamp + ".result.json"))
        atomic_write_json(Path(report["result_path"]), report, durable=True)
    return report
