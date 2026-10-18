"""Compact exact completed-forward replicas without evicting any cache key.

The v3 producer owns input identity, full payload SHA, per-key singleflight and
atomic replacement. This maintenance decoder intentionally admits only that
known ABI, including legacy writable replicas; incomplete/unknown keys remain.
It preserves every receipt/name and never imports Torch, calls a model or
claims a cold recovery, LRU eviction or total cache budget.
"""
from __future__ import annotations

from contextlib import ExitStack
from collections import defaultdict
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid
from typing import Callable

from stockagent.data_sync.desync_snapshots import atomic_write_json, _fsync_directory
from stockagent.data_sync.completed_forward_usage import (
    SAMPLE_INTERVAL_NS, commit_binding, read_usage,
)
from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash
from stockagent.data_sync.immutable_cache_compaction import (
    allocated_bytes, duplicate_payload_groups, inode_key, share_locked_payload_group,
)
from stockagent.data_sync.materialized_cache import process_references_many
from stockagent.data_sync.physical_cache_reuse import _metadata, _real_directory, _writer_lock

CONTRACT = "completed_forward_payload_sharing_v1"
SUPPORTED_FORWARD_CONTRACT = "completed_tw_panel_forward_cpu_v3"
MAX_KEYS = 4096
MAX_RECEIPT_BYTES = 1024**2
MAX_PAYLOAD_BYTES = 1024**3
_KEY = re.compile(r"[0-9a-f]{64}\Z")


class ForwardCommitChanged(RuntimeError):
    """A legitimate per-key writer arrived; other independent keys may proceed."""


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _inventory(root: Path) -> tuple[list[dict], list[dict], list[dict], list[str]]:
    paths = sorted(root.glob("*.forward"))
    if len(paths) > MAX_KEYS:
        raise ValueError("forward inventory exceeds the explicit bounded key count")
    files, proofs, retained = [], [], []
    for path in paths:
        key = path.stem
        if _KEY.fullmatch(key) is None:
            retained.append({"path": path.name, "reason": "unknown_name"})
            continue
        receipt_path = path.with_suffix(".receipt.json")
        try:
            with _writer_lock(root / f"{key}.lock"):
                receipt_info = payload_info(receipt_path)
                if receipt_info["size"] > MAX_RECEIPT_BYTES:
                    raise ValueError("oversized forward receipt")
                record, receipt_sha = _metadata(receipt_path)
                info = payload_info(path)
                if (receipt_path.resolve(strict=True) != receipt_path or path.resolve(strict=True) != path
                        or payload_info(receipt_path) != receipt_info
                        or record.get("contract") != SUPPORTED_FORWARD_CONTRACT
                        or record.get("key") != key or not isinstance(record.get("identity"), dict)
                        or _digest(record["identity"]) != key
                        or type(record.get("payload_bytes")) is not int
                        or not 0 < record["payload_bytes"] <= MAX_PAYLOAD_BYTES
                        or record["payload_bytes"] != info["size"]
                        or not isinstance(record.get("payload_sha256"), str)
                        or _KEY.fullmatch(record["payload_sha256"]) is None
                        or info["uid"] != os.geteuid() or info["gid"] != os.getegid()
                        or info["mode"] not in {0o400, 0o600}):
                    raise ValueError("unsupported or incoherent forward receipt/payload")
                files.append({"path": path.name, "sha256": record["payload_sha256"], **info})
                proofs.append({"key": key, "receipt_sha256": receipt_sha, "receipt_identity": receipt_info})
        except BlockingIOError:
            retained.append({"path": path.name, "reason": "writer_busy"})
        except (FileNotFoundError, ValueError, TypeError, KeyError) as error:
            retained.append({"path": path.name, "reason": "unverified_commit", "error": str(error)})
    return files, proofs, retained, [path.name for path in paths]


def _share_group(root, group, proofs, *, reference_probe, touched, report, retired_handles):
    keys = sorted(Path(item["path"]).stem for names in group for item in names)
    with ExitStack() as locks:
        for key in keys:
            locks.enter_context(_writer_lock(root / f"{key}.lock"))
        for key in keys:
            proof = proofs[key]
            path = root / f"{key}.receipt.json"
            if (payload_info(path) != proof["receipt_identity"]
                    or _metadata(path)[1] != proof["receipt_sha256"]):
                raise ForwardCommitChanged("forward commit changed before sharing")
        # Detect even a same-size, restored-mtime repair before mutating names.
        for names in group:
            for item in names:
                if payload_info(root / item["path"]) != {k: v for k, v in item.items()
                                                        if k not in {"path", "sha256"}}:
                    raise ForwardCommitChanged("forward payload generation changed before sharing")
        references = reference_probe([root / item["path"] for names in group for item in names],
                                     inode_keys={inode_key(names[0]) for names in group})
        if references:
            report["referenced_groups_retained"] += 1
            report["process_references"].extend(references)
            return set()
        return share_locked_payload_group(
            root, group, touched=touched, report=report,
            retired_handles=retired_handles, temporary_tag="forward-share")


def plan_completed_forward_working_set(
    root: Path, *, target_bytes: int | None = None, protected_keys=(),
    now_ns: int | None = None, recent_seconds: int = 3600,
    reference_probe: Callable = process_references_many,
    consumer_pin_probe: Callable[[], dict] | None = None,
) -> dict:
    """Metadata-only capacity/oldest-use plan, NEVER a retirement permission.

    Count physical allocations once across all immediate regular names, not
    logical keys. Unknown nested content makes the scope incomplete. Current
    consumer pins may be supplied by their authority; sampled use is separate
    evidence and is deliberately not presented as a complete consumer census.
    No payload hashing, Torch import, recursive source walk or deletion occurs.
    """
    if target_bytes is not None and (type(target_bytes) is not int or target_bytes <= 0):
        raise ValueError("capacity target must be a positive byte count")
    if type(recent_seconds) is not int or recent_seconds < 0:
        raise ValueError("recent-use window must be nonnegative seconds")
    now = time.time_ns() if now_ns is None else now_ns
    if type(now) is not int or now <= 0:
        raise ValueError("invalid capacity observation clock")
    protected = set(protected_keys)
    if any(not isinstance(key, str) or _KEY.fullmatch(key) is None for key in protected):
        raise ValueError("invalid protected forward key")
    root = _real_directory(root)

    def consumer_snapshot():
        if consumer_pin_probe is None:
            return None
        try:
            snapshot = consumer_pin_probe()
            if (not isinstance(snapshot, dict)
                    or snapshot.get("contract") != "completed_forward_current_consumers_v1"
                    or type(snapshot.get("complete_for_scope")) is not bool
                    or not isinstance(snapshot.get("keys"), list)
                    or any(not isinstance(key, str) or _KEY.fullmatch(key) is None
                           for key in snapshot["keys"])):
                raise ValueError("invalid current-consumer pin census")
            return snapshot
        except Exception as error:
            # A control-plane failure is retention, never permission to evict
            # or a reason to stop independent exact duplicate sharing.
            return {"complete_for_scope": False, "keys": [], "error_type": type(error).__name__}

    consumers_before = consumer_snapshot()
    with os.scandir(root) as entries:
        names = []
        for entry in entries:
            names.append(entry.name)
            if len(names) > MAX_KEYS * 4 + 32:
                raise ValueError("forward namespace exceeds the bounded inventory")
    physical, unmeasured, generations = {}, [], {}
    def generation(info):
        return (info.st_dev, info.st_ino, info.st_size, info.st_blocks,
                info.st_mtime_ns, info.st_ctime_ns, info.st_nlink, info.st_mode)
    for name in sorted(names):
        try:
            info = (root / name).lstat()
            generations[name] = generation(info)
            if stat.S_ISREG(info.st_mode):
                physical[(info.st_dev, info.st_ino)] = info.st_blocks * 512
            else:
                unmeasured.append({"path": name, "reason": "non_regular_namespace_entry"})
        except FileNotFoundError:
            unmeasured.append({"path": name, "reason": "changed_during_inventory"})
    files, proofs, retained, _ = _inventory(root)
    proof_map = {proof["key"]: proof for proof in proofs}
    groups, rows = defaultdict(list), []
    for item in files:
        key = Path(item["path"]).stem
        row = {"key": key, "path": item["path"], "allocated_bytes": item["allocated"],
               "payload_bytes": item["size"], "retention_reasons": []}
        if key in protected:
            row["retention_reasons"].append("explicit_consumer_pin")
        try:
            with _writer_lock(root / f"{key}.lock"):
                proof = proof_map[key]
                receipt_path = root / f"{key}.receipt.json"
                if (payload_info(receipt_path) != proof["receipt_identity"]
                        or _metadata(receipt_path)[1] != proof["receipt_sha256"]
                        or payload_info(root / item["path"]) != {
                            name: value for name, value in item.items() if name not in {"path", "sha256"}}):
                    raise ValueError("forward commit changed during usage planning")
                usage, usage_sha = read_usage(root / f"{key}.usage.json")
                binding = commit_binding(key=key, receipt_sha256=proof["receipt_sha256"],
                                         payload_sha256=item["sha256"], payload_bytes=item["size"])
                if any(usage[name] != value for name, value in binding.items()):
                    raise ValueError("usage belongs to a different forward commit")
                row.update(last_success_at_ns=usage["last_success_at_ns"], usage_sha256=usage_sha,
                           recorded_samples=usage["recorded_samples"])
                if usage["last_success_at_ns"] > now:
                    row["retention_reasons"].append("future_usage_clock")
                elif now - usage["last_success_at_ns"] <= recent_seconds * 10**9 + SAMPLE_INTERVAL_NS:
                    row["retention_reasons"].append("recent_successful_use")
        except (OSError, ValueError, TypeError) as error:
            row["retention_reasons"].append("unknown_or_changed_usage")
            row["usage_error_type"] = type(error).__name__
        groups[inode_key(item)].append((item, row))
        rows.append(row)
    # Retention is already whole-inventory if any reference exists. One root
    # boundary plus an inode set has the same conservative safety consequence,
    # without testing every /proc path against hundreds of individual keys.
    references = reference_probe([root], inode_keys=set(groups)) if files else []
    consumers_after = consumer_snapshot()
    pins_stable = consumers_before == consumers_after if consumer_pin_probe is not None else None
    pins_complete = bool(consumer_pin_probe is not None and pins_stable
                         and consumers_before.get("complete_for_scope"))
    service_pins = set()
    for snapshot in (consumers_before, consumers_after):
        if snapshot is not None:
            service_pins.update(snapshot["keys"])
    for row in rows:
        if row["key"] in service_pins:
            row["retention_reasons"].append("current_service_consumer_pin")
        if consumer_pin_probe is not None and not pins_complete:
            row["retention_reasons"].append("consumer_pin_inventory_incomplete_or_changed")
    oldest = []
    for entries in groups.values():
        first = entries[0][0]
        reasons = {reason for _, row in entries for reason in row["retention_reasons"]}
        if any(item["nlink"] != len(entries) for item, _ in entries):
            reasons.add("unaccounted_hardlink_alias")
        if references:
            # The shared process probe intentionally returns bounded references,
            # not an exhaustive per-key map. Retain all, do not infer absent IDs.
            reasons.add("active_process_reference_in_inventory")
        if reasons:
            for _, row in entries:
                row["retention_reasons"] = sorted(set(row["retention_reasons"]) | reasons)
            continue
        oldest.append({"keys": sorted(row["key"] for _, row in entries),
                       "allocated_bytes": first["allocated"],
                       "last_success_at_ns": max(row["last_success_at_ns"] for _, row in entries),
                       "blocked_by": ["exact_cold_recovery_not_proven", "fresh_consumer_pin_audit_required"]})
    oldest.sort(key=lambda row: (row["last_success_at_ns"], row["keys"]))
    # A live owner may publish between independent per-key observations. This
    # is a bounded advisory snapshot, never an atomic all-cache transaction.
    with os.scandir(root) as entries:
        after_names = []
        for entry in entries:
            after_names.append(entry.name)
            if len(after_names) > MAX_KEYS * 4 + 32:
                raise ValueError("forward namespace grew beyond the bounded inventory")
    stable = set(after_names) == set(names)
    for name, before in generations.items():
        try:
            stable = generation((root / name).lstat()) == before and stable
        except FileNotFoundError:
            stable = False
    if not stable:
        unmeasured.append({"path": ".", "reason": "namespace_changed_during_inventory"})
    allocated = sum(physical.values())
    return {"contract": "completed_forward_working_set_plan_v1", "root": str(root),
            "state": "metadata_plan_only", "observed_at_ns": now,
            "scope": "immediate_regular_namespace_allocations_not_integrity_or_cold_acceptance",
            "inventory_complete": not unmeasured, "unmeasured_entries": unmeasured,
            "metadata_snapshot_stable": stable,
            "namespace_allocated_bytes": allocated, "physical_regular_inodes": len(physical),
            "committed_logical_payload_bytes": sum(item["size"] for item in files),
            "committed_payload_allocated_bytes": allocated_bytes(files),
            "target_bytes": target_bytes,
            "pressure_bytes": None if target_bytes is None else max(0, allocated - target_bytes),
            "consumer_pins_exhaustive": False, "explicit_protected_keys": sorted(protected),
            "consumer_pins_complete_for_scope": pins_complete,
            "consumer_pin_snapshot_stable": pins_stable,
            "current_service_protected_keys": sorted(service_pins),
            "consumer_pin_census": consumers_after,
            "consumer_keys_missing_verified_commit": sorted(service_pins - {row["key"] for row in rows}),
            "recent_use_seconds": recent_seconds, "sample_uncertainty_seconds": SAMPLE_INTERVAL_NS // 10**9,
            "keys": rows, "unverified_keys_retained": retained,
            "process_references": references, "oldest_groups_requiring_recovery": oldest,
            "potential_payload_bytes_after_recovery": sum(row["allocated_bytes"] for row in oldest),
            "eviction_allowed": False, "logical_paths_removed": 0, "payload_bytes_hashed": 0}


def compact_completed_forward_cache(
    root: Path, receipt_dir: Path, *, apply: bool = False,
    expected_fingerprint: str | None = None,
    full_audit: bool = True,
    reference_probe: Callable = process_references_many,
    progress: Callable[[dict], None] | None = None,
    capacity_target_bytes: int | None = None,
    consumer_pin_probe: Callable[[], dict] | None = None,
) -> dict:
    """Hash complete eligible payloads; preserve unique, busy and unknown keys."""
    if type(full_audit) is not bool:
        raise ValueError("full_audit must be an explicit boolean")
    if capacity_target_bytes is not None and (type(capacity_target_bytes) is not int or capacity_target_bytes <= 0):
        raise ValueError("capacity target must be a positive byte count")
    root = _real_directory(root)
    receipt_dir = receipt_dir.absolute()
    if receipt_dir.resolve(strict=False) != receipt_dir or (
            root == receipt_dir or root in receipt_dir.parents or receipt_dir in root.parents):
        raise ValueError("compaction receipts must be outside the forward cache")
    receipt_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
    result_path = receipt_dir / f"{stamp}.result.json"
    started, last_progress = time.monotonic(), [0.0]
    report = {"contract": CONTRACT, "root": str(root), "apply": apply, "state": "running",
              "attempt_id": stamp, "started_at": datetime.now(UTC).isoformat(),
              "result_path": str(result_path), "replaced_paths": 0,
              "logical_paths_removed": 0, "receipt_bytes_changed": False,
              "source_or_ledger_changed": False, "cold_objects_removed": 0,
              "retired_allocated_bytes": 0, "referenced_groups_retained": 0,
              "changed_groups_retained": 0, "process_references": []}
    touched = set()
    def emit(stage, **fields):
        value = {"contract": CONTRACT, "attempt_id": stamp, "stage": stage,
                 "elapsed_seconds": time.monotonic() - started, **fields}
        atomic_write_json(receipt_dir / "progress.json", value)
        if progress:
            progress(value)
    try:
        with _writer_lock(root / ".forward-compaction.lock"):
            files, proofs, retained, names = _inventory(root)
            report.update(committed_keys=len(files), retained_keys=retained,
                          logical_payload_bytes=sum(item["size"] for item in files))
            groups, external = duplicate_payload_groups(files)
            selected_files = files if full_audit else [
                item for group in groups for aliases in group for item in aliases]
            verified, verified_bytes = set(), 0
            total_bytes = sum({inode_key(item): item["size"] for item in selected_files}.values())
            emit("hash-forward-payloads", completed_bytes=0, total_bytes=total_bytes)
            raced = set()
            for item in selected_files:
                key = inode_key(item)
                if key not in verified:
                    info = {k: v for k, v in item.items() if k not in {"path", "sha256"}}
                    try:
                        digest = stable_payload_hash(root / item["path"], info)
                    except (ValueError, FileNotFoundError):
                        try:
                            changed = payload_info(root / item["path"]) != info
                        except FileNotFoundError:
                            changed = True
                        if not changed:
                            raise
                        raced.add(item["path"])
                        continue  # Never trust the changed key; do not block unrelated commits.
                    if digest != item["sha256"]:
                        raise ValueError(f"forward payload SHA mismatch: {item['path']}")
                    verified.add(key)
                    verified_bytes += item["size"]
                if time.monotonic() - last_progress[0] >= 10:
                    emit("hash-forward-payloads", completed_bytes=verified_bytes, total_bytes=total_bytes)
                    last_progress[0] = time.monotonic()
            current_files, current_proofs, _, current_names = _inventory(root)
            by_path = {item["path"]: item for item in current_files}
            proof_map = {proof["key"]: proof for proof in current_proofs}
            old_proofs = {proof["key"]: proof for proof in proofs}
            raced.update(item["path"] for item in files
                         if by_path.get(item["path"]) != item
                         or proof_map.get(Path(item["path"]).stem) != old_proofs[Path(item["path"]).stem])
            files = [item for item in files if item["path"] not in raced]
            supported_keys = {Path(item["path"]).stem for item in files}
            proofs = [proof for proof in proofs if proof["key"] in supported_keys]
            retained.extend({"path": path, "reason": "changed_during_audit"} for path in sorted(raced))
            new_keys = sorted(set(current_names) - set(names))
            verified.intersection_update(inode_key(item) for item in files)
            verified_bytes = sum({inode_key(item): item["size"] for item in files
                                  if inode_key(item) in verified}.values())
            groups, external = duplicate_payload_groups(files)
            fingerprint = _digest({"contract": CONTRACT, "root": str(root),
                                   "files": files, "proofs": proofs, "retained": retained, "names": names})
            if expected_fingerprint is not None and expected_fingerprint != fingerprint:
                raise ValueError("forward cache dry-run fingerprint changed")
            plan_path = receipt_dir / f"{stamp}.plan.json"
            atomic_write_json(plan_path, {"contract": CONTRACT, "root": str(root), "fingerprint": fingerprint,
                                         "files": files, "proofs": proofs, "retained_keys": retained,
                                         "groups": [[[item["path"] for item in aliases] for aliases in group]
                                                    for group in groups]})
            report.update(plan_path=str(plan_path), plan_fingerprint=fingerprint,
                          stable_committed_keys=len(files), new_keys_retained=new_keys,
                          changed_keys_retained=sorted(raced), retained_keys=retained,
                          verified_payload_inodes=len(verified), verified_payload_bytes=verified_bytes,
                          hash_scope="all_supported_payloads" if full_audit else "sharing_candidates_only",
                          metadata_only_payload_inodes=len({inode_key(item) for item in files} - verified),
                          duplicate_groups=len(groups), external_link_inodes_retained=external,
                          candidate_allocated_bytes=sum(aliases[0]["allocated"] for group in groups
                                                        for aliases in group[1:]),
                          allocated_before=allocated_bytes(files))
            if not apply:
                report["state"] = "audited"
            else:
                proof_map = {proof["key"]: proof for proof in proofs}
                allocations = {inode_key(item): item["allocated"] for item in files}
                retired_keys, retired_proven_bytes, open_retired = set(), 0, []
                busy_groups = 0
                with ExitStack() as retired_handles:
                    def flush_retired():
                        nonlocal retired_proven_bytes
                        if retired_keys:
                            references = reference_probe([], inode_keys=set(retired_keys))
                            open_retired.extend(references)
                            if not references:
                                retired_proven_bytes += sum(allocations[key] for key in retired_keys)
                        retired_handles.close()
                        retired_keys.clear()
                    for index, group in enumerate(groups):
                        try:
                            retired_keys.update(_share_group(
                                root, group, proof_map, reference_probe=reference_probe,
                                touched=touched, report=report, retired_handles=retired_handles))
                        except BlockingIOError:
                            busy_groups += 1
                        except ForwardCommitChanged:
                            report["changed_groups_retained"] += 1
                        if len(retired_keys) >= 64:
                            flush_retired()
                        if time.monotonic() - last_progress[0] >= 10:
                            emit("share-forward-payloads", completed_groups=index + 1,
                                 total_groups=len(groups), replaced_paths=report["replaced_paths"])
                            last_progress[0] = time.monotonic()
                    flush_retired()
                after_all, after_proofs, _, after_names = _inventory(root)
                after = [item for item in after_all if Path(item["path"]).stem in supported_keys]
                after_proofs = [proof for proof in after_proofs if proof["key"] in supported_keys]
                if (after_proofs != proofs
                        or [(item["path"], item["size"], item["sha256"]) for item in after]
                        != [(item["path"], item["size"], item["sha256"]) for item in files]):
                    raise ValueError("forward receipts or logical payload set changed")
                reduction = allocated_bytes(files) - allocated_bytes(after)
                report.update(state="partially_compacted" if busy_groups or raced
                              or report["changed_groups_retained"] or report["referenced_groups_retained"]
                              else "compacted", busy_groups_retained=busy_groups,
                              new_keys_retained=sorted(set(after_names) - set(names)),
                              allocated_after=allocated_bytes(after),
                              allocated_namespace_reduction_bytes=reduction,
                              retired_allocated_bytes=min(reduction, retired_proven_bytes),
                              retired_inode_checks="descriptor_pinned_bounded_batches_v1",
                              retained_open_inode_references=open_retired)
            report["working_set"] = plan_completed_forward_working_set(
                root, target_bytes=capacity_target_bytes, reference_probe=reference_probe,
                consumer_pin_probe=consumer_pin_probe)
    except BlockingIOError:
        report.update(state="deferred", reason="compaction_or_writer_lock_busy")
    except (KeyboardInterrupt, SystemExit) as error:
        report.update(state="interrupted", error_type=type(error).__name__)
        raise
    except Exception as error:
        report.update(state="failed", error_type=type(error).__name__, error=str(error))
    finally:
        for directory in sorted(touched):
            _fsync_directory(directory)
        report.update(completed_at=datetime.now(UTC).isoformat(), elapsed_seconds=time.monotonic() - started)
        atomic_write_json(result_path, report)
        atomic_write_json(receipt_dir / "latest.json", report)
    return report
