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
