"""Bounded NAS delivery queue over the existing immutable packed namespace.

Syncthing completion is transport evidence. Only a source-pinned acknowledgement
from the paired relay establishes NAS file recovery. Only explicitly enabled,
reconstructible cold transport copies may be retired after that proof. Canonical
source objects, code/SQL batches and NAS snapshots are preserved.
"""
from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import time
import uuid

from scripts.backup_delivery_receipt import (
    DISPATCH, HASH, atomic_public, read_json, signed, validate_ack, validate_readiness,
)
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_bytes
from stockagent.data_sync.offhost_backup import (
    _regular, export_incremental_delivery, private_json,
)
from stockagent.data_sync.packed_backup import (
    PinnedObjectSignatures, mounted_volume, object_descriptor, safe_path, signature,
)
from stockagent.data_sync.packed_snapshots import PACKED_HEAD_SCHEMA_VERSION, _validate_manifest
from stockagent.runtime_identity import identity_sha256

CATALOG = "retained_packed_backup_catalog_v1"
LEDGER = "bounded_nas_backup_stream_ledger_v1"


def file_key(row: dict) -> str:
    return row["relative"] + "@" + row["sha256"]


def capture_catalog(cold: Path, publication_catalog: Path, history_disposition: Path | None = None,
                    *, include_unreferenced_objects: bool = False) -> dict:
    """Fixed metadata inventory, with per-release missing-object diagnostics.

    Missing historical objects do not erase a historical manifest or obstruct
    unrelated valid deliveries. Catalog-restricted workspaces are not scanned.
    """
    cold = cold.absolute()
    entries = json.loads(publication_catalog.read_bytes())["datasets"]
    excluded = {r["dataset"] for r in entries if r.get("publish") is not True}
    rows: dict[str, dict] = {}
    releases = []
    head_snapshots = set()
    errors = []
    excluded_manifests = []
    metadata = {}
    all_referenced_objects = set()
    orphan_scan_blocked = False

    def add_metadata(path: Path) -> tuple[bytes, dict]:
        relative = path.relative_to(cold).as_posix()
        path = _regular(safe_path(cold, relative))
        before = signature(path)
        raw = path.read_bytes()
        if before != signature(path):
            raise SnapshotError("cold metadata changed during catalog capture")
        row = {"relative": relative, "sha256": hashlib.sha256(raw).hexdigest(),
               "bytes": len(raw), "signature": list(before), "role": "cold_metadata"}
        rows[relative] = row
        return raw, row

    for path in sorted((cold / "manifests").glob("*/*.json")):
        relative = path.relative_to(cold).as_posix()
        if path.parent.name in excluded:
            excluded_manifests.append(relative)
            if include_unreferenced_objects:
                try:
                    manifest = json.loads(_regular(path).read_bytes())
                    _validate_manifest(manifest)
                    all_referenced_objects.update(r["relpath"] for r in [
                        manifest["archive"]["inventory"], *manifest["archive"]["objects"]])
                except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
                    orphan_scan_blocked = True
                    errors.append({"relative": relative, "reason": type(error).__name__})
            continue
        try:
            raw, row = add_metadata(path)
            manifest = json.loads(raw)
            _validate_manifest(manifest)
            if manifest["dataset"] != path.parent.name or manifest["snapshot_id"] != path.stem:
                raise SnapshotError("retained manifest path identity differs")
            all_referenced_objects.update(r["relpath"] for r in [
                manifest["archive"]["inventory"], *manifest["archive"]["objects"]])
            metadata[relative] = (manifest, row)
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            rows.pop(relative, None)
            errors.append({"relative": relative, "reason": type(error).__name__})
    for path in sorted((cold / "heads").glob("*/*.json")):
        if path.parent.name in excluded:
            continue
        relative = path.relative_to(cold).as_posix()
        try:
            raw, row = add_metadata(path)
            head = json.loads(raw)
            manifest, manifest_row = metadata[head["manifest_relpath"]]
            if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION
                    or head["dataset"] != path.parent.name or head["node_id"] != path.stem
                    or ".sync-conflict-" in path.name or head["manifest_sha256"] != manifest_row["sha256"]
                    or head["snapshot_id"] != manifest["snapshot_id"] or head["hlc"] != manifest["hlc"]
                    or head["node_id"] != manifest["publisher"]["node_id"]):
                raise SnapshotError("current head does not match its fixed retained manifest")
            head_snapshots.add((manifest["dataset"], manifest["snapshot_id"]))
            row["captured_bytes_utf8"] = raw.decode("utf-8")
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            rows.pop(relative, None)
            errors.append({"relative": relative, "reason": type(error).__name__})

    # Previous head bytes are durable provenance, distinct from mutable heads.
    for path in sorted((cold / "head-history/heads").glob("*/*/*.json")):
        dataset, node = path.parent.parent.name, path.parent.name
        if dataset in excluded:
            continue
        relative = path.relative_to(cold).as_posix()
        try:
            raw, row = add_metadata(path)
            head = json.loads(raw)
            manifest, manifest_row = metadata[head["manifest_relpath"]]
            if (head["schema_version"] != PACKED_HEAD_SCHEMA_VERSION or head["dataset"] != dataset
                    or head["node_id"] != node or path.stem != row["sha256"]
                    or head["manifest_sha256"] != manifest_row["sha256"]
                    or head["snapshot_id"] != manifest["snapshot_id"] or head["hlc"] != manifest["hlc"]):
                raise SnapshotError("historical head does not match its fixed manifest")
        except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
            rows.pop(relative, None)
            errors.append({"relative": relative, "reason": type(error).__name__})

    missing = {}
    with PinnedObjectSignatures(cold) as pinned:
        for relative, (manifest, manifest_row) in sorted(metadata.items()):
            refs = [manifest["archive"]["inventory"], *manifest["archive"]["objects"]]
            required = [file_key(manifest_row)]
            absent = []
            for ref in refs:
                object_relative = ref["relpath"]
                digest = object_descriptor(object_relative)
                if digest != ref["sha256"] or type(ref["bytes"]) is not int or ref["bytes"] < 0:
                    raise SnapshotError("retained object descriptor differs from its manifest")
                required.append(object_relative + "@" + digest)
                if object_relative in missing:
                    absent.append(object_relative)
                    continue
                if object_relative in rows:
                    if rows[object_relative]["sha256"] != digest or rows[object_relative]["bytes"] != ref["bytes"]:
                        raise SnapshotError("retained manifests disagree on an immutable object")
                    continue
                try:
                    observed = pinned.signature(object_relative)
                    if observed[2] != ref["bytes"]:
                        raise SnapshotError("retained object size differs")
                    rows[object_relative] = {"relative": object_relative, "sha256": digest,
                        "bytes": ref["bytes"], "signature": list(observed), "role": "packed_object"}
                except (OSError, SnapshotError) as error:
                    missing[object_relative] = {"relative": object_relative, "sha256": digest,
                        "bytes": ref["bytes"], "reason": type(error).__name__}
                    absent.append(object_relative)
            releases.append({"dataset": manifest["dataset"], "snapshot_id": manifest["snapshot_id"],
                "manifest_sha256": manifest_row["sha256"], "manifest_relative": relative,
                "source_fingerprint_sha256": manifest["source"]["portable_fingerprint_sha256"],
                "current_head": (manifest["dataset"], manifest["snapshot_id"]) in head_snapshots,
                "required_file_keys": sorted(required), "missing_objects": sorted(absent)})
        if include_unreferenced_objects and not orphan_scan_blocked:
            # Back up unique evidence in the enrolled cold object namespace;
            # lack of a manifest grants neither deletion nor release identity.
            # Objects belonging to restricted manifests remain excluded.
            for parent, dirs, names in os.walk(cold / "objects", followlinks=False):
                for name in dirs:
                    if (Path(parent) / name).is_symlink():
                        raise SnapshotError("cold object directory is redirected")
                for name in names:
                    relative = (Path(parent) / name).relative_to(cold).as_posix()
                    if relative in all_referenced_objects:
                        continue
                    try:
                        digest = object_descriptor(relative)
                        observed = pinned.signature(relative)
                        rows[relative] = {"relative": relative, "sha256": digest,
                            "bytes": observed[2], "signature": list(observed),
                            "role": "unreferenced_cold_object", "release_provenance_verified": False}
                    except (OSError, SnapshotError) as error:
                        errors.append({"relative": relative, "reason": type(error).__name__})
        pinned.recheck()
    if not releases:
        raise SnapshotError("no valid retained packed releases found")
    body = {"contract": CATALOG, "releases": releases,
            "files": sorted(rows.values(), key=lambda r: r["relative"]),
            "missing_objects": sorted(missing.values(), key=lambda r: r["relative"]),
            "metadata_errors": errors, "excluded_catalog_datasets": sorted(excluded),
            "excluded_manifests": excluded_manifests, "unpublished_sources_included": False,
            "unreferenced_cold_objects_included": include_unreferenced_objects and not orphan_scan_blocked,
            "scope": "all observed retained canonical manifests; current heads prioritized"}
    from stockagent.data_sync.backup_history import apply_disposition
    body = apply_disposition(body, history_disposition)
    # Filesystem signatures pin this execution, but are not content identity.
    content = {**body, "files": [{k: v for k, v in r.items() if k != "signature"} for r in body["files"]]}
    return {**body, "identity_sha256": identity_sha256(content),
            "observed_at_utc": datetime.now(timezone.utc).isoformat()}


def empty_ledger() -> dict:
    return {"contract": LEDGER, "deliveries": {}, "reported_baseline": []}


def reported_baseline(delivery: Path, report_path: Path, repository_id: str) -> dict:
    """Bind a relayed pilot report to current exact source bytes, not an ACK.

    The original NAS receipts remain on lab203. Keep this provenance explicit
    so user-relayed acceptance cannot silently become machine verification.
    """
    from scripts.verify_backup_delivery import verify
    report_path = _regular(report_path)
    raw_report = report_path.read_bytes()
    report = json.loads(raw_report)
    if report.get("original_remote_receipts_received") is not False:
        raise SnapshotError("pilot baseline needs its explicit relayed provenance")
    if report.get("evidence_origin") == "user_relay_from_lab203_local_codex":
        acceptance = report["delivery"]
        key = acceptance["envelope_identity_sha256"]
        snapshot = acceptance["snapshot_id"]
        reported_repo = acceptance["repository_id"]
        required = ("all_command_exit_codes_zero", "handoff_ready_both_envelope_hashes_exact_set_verified",
                    "encrypted_backup_verified", "check_read_data_verified",
                    "fixed_snapshot_independent_restore_verified", "source_verifier_verified",
                    "all_401_file_sha256_verified")
        if any(acceptance.get(k) is not True for k in required):
            raise SnapshotError("relayed pilot acceptance is incomplete")
    elif report.get("evidence_kind") == "user_relayed_lab203_acceptance_summary":
        key = report["envelope_identity_sha256"]
        snapshot = report["nas_restic_snapshot_id"]
        reported_repo = report["nas_report"]["repository_id"]
        required = ("ready_and_pinned_envelope_identity", "complete_file_set", "nas_encrypted_backup",
                    "nas_check_read_all_data", "exact_full_snapshot_independent_restore",
                    "restored_source_verifier", "all_13_file_sha256_match")
        if any(report["reported_checks"].get(k) is not True for k in required):
            raise SnapshotError("relayed pilot acceptance is incomplete")
        rows = report["source_local_independent_checks"]["batch_files"]
        acceptance = {"envelope_file_sha256": next(r["sha256"] for r in rows if r["relative"] == "backup-envelope.json"),
                      "files": len(rows), "bytes": sum(r["bytes"] for r in rows)}
    else:
        raise SnapshotError("unknown pilot acceptance report")
    if (reported_repo != repository_id or not HASH.fullmatch(snapshot)
            or delivery.name != "batch-" + key):
        raise SnapshotError("pilot baseline repository, snapshot or batch identity differs")
    verified = verify(delivery, key)
    raw_envelope = (delivery / "backup-envelope.json").read_bytes()
    envelope = json.loads(raw_envelope)
    if (hashlib.sha256(raw_envelope).hexdigest() != acceptance["envelope_file_sha256"]
            or verified["files_verified"] + 2 != acceptance["files"]
            or verified["bytes_verified"] + len(raw_envelope) + (delivery / "READY").stat().st_size != acceptance["bytes"]):
        raise SnapshotError("pilot baseline source byte proof differs from its report")
    files = []
    for row in envelope["files"]:
        if row["relative"] == "source-plan.json":
            # Original full-release pilots include their fixed source plan;
            # it is byte-verified above but is not a canonical cold object.
            continue
        if not row["relative"].startswith("cold/"):
            raise SnapshotError("pilot baseline must contain only canonical cold inputs")
        relative = row["relative"][5:]
        safe_path(delivery / "cold", relative)
        if relative.startswith("objects/"):
            if object_descriptor(relative) != row["sha256"]:
                raise SnapshotError("pilot baseline object descriptor differs")
        elif not (relative.startswith(("manifests/", "heads/")) and relative.endswith(".json")):
            raise SnapshotError("pilot baseline has noncanonical cold metadata")
        files.append({**row, "relative": relative})
    return {"envelope_identity_sha256": key, "envelope_file_sha256": acceptance["envelope_file_sha256"],
        "batch_relative": delivery.name, "repository_id": repository_id, "snapshot_id": snapshot,
        "report_sha256": hashlib.sha256(raw_report).hexdigest(), "files": files,
        "evidence_origin": "user_relay_from_lab203_local_codex",
        "original_remote_receipts_received": False, "machine_acknowledged": False}


def coverage(catalog: dict, ledger: dict) -> dict:
    confirmed = set()
    reported = set()
    published = set()
    pending_bytes = 0
    for delivery in ledger["deliveries"].values():
        keys = {file_key(row) for row in delivery["files"]}
        published.update(keys)
        if delivery.get("acceptance"):
            confirmed.update(keys)
        else:
            pending_bytes += delivery["dispatch"]["complete_bytes"]
    for baseline in ledger.get("reported_baseline", []):
        reported.update(file_key(r) for r in baseline["files"])
    accepted = confirmed | reported
    available = {file_key(row) for row in catalog["files"]}
    current = [r for r in catalog["releases"] if r["current_head"]]
    recovered = lambda r: not r["missing_objects"] and set(r["required_file_keys"]) <= accepted
    return {"available_file_count": len(available), "available_bytes": sum(r["bytes"] for r in catalog["files"]),
        "machine_acknowledged_files": len(available & confirmed),
        "user_reported_baseline_files": len(available & reported),
        "nas_covered_file_count": len(available & accepted),
        "nas_covered_bytes": sum(r["bytes"] for r in catalog["files"] if file_key(r) in accepted),
        "pending_delivery_count": sum(not d.get("acceptance") for d in ledger["deliveries"].values()),
        "pending_bytes": pending_bytes, "published_file_keys": published,
        "accepted_file_keys": accepted, "current_release_count": len(current),
        "current_releases_nas_file_covered": sum(recovered(r) for r in current),
        "retained_release_count": len(catalog["releases"]),
        "observed_release_count": len(catalog["releases"]) + len(catalog.get("abandoned_releases", [])),
        "abandoned_history_release_count": len(catalog.get("abandoned_releases", [])),
        "abandoned_unavailable_object_count": len(catalog.get("abandoned_missing_objects", [])),
        "retained_releases_nas_file_covered": sum(recovered(r) for r in catalog["releases"]),
        "missing_object_count": len(catalog["missing_objects"]),
        "blocked_retained_releases": sum(bool(r["missing_objects"]) for r in catalog["releases"]),
        "metadata_error_count": len(catalog["metadata_errors"]),
        "unreferenced_cold_object_count": sum(r["role"] == "unreferenced_cold_object" for r in catalog["files"]),
        "unreferenced_cold_object_bytes": sum(r["bytes"] for r in catalog["files"] if r["role"] == "unreferenced_cold_object"),
        "all_history_backup_verified": False, "canonical_reconstruction_verified": False,
        "unpublished_sources_included": False}


def select_wave(catalog: dict, covered: set[str], *, maximum_bytes: int,
                maximum_files: int = 2048) -> list[dict]:
    current_keys = {key for r in catalog["releases"] if r["current_head"] for key in r["required_file_keys"]}
    eligible = [r for r in catalog["files"] if file_key(r) not in covered]
    eligible.sort(key=lambda r: (r["role"] != "cold_metadata", file_key(r) not in current_keys,
                                 r["bytes"], r["relative"]))
    selected = []
    total = 0
    # Allow room for the JSON envelope, paths and dispatch bookkeeping.
    overhead = 1024 * 1024
    for row in eligible:
        if len(selected) >= maximum_files:
            break
        if total + row["bytes"] + overhead > maximum_bytes:
            continue
        selected.append(row)
        total += row["bytes"]
    return selected


def recovery_index(catalog: dict, ledger: dict, repository_id: str) -> dict:
    deliveries = []
    for item in ledger.get("reported_baseline", []):
        deliveries.append({k: item[k] for k in ("envelope_identity_sha256", "envelope_file_sha256",
            "batch_relative", "repository_id", "snapshot_id", "files", "evidence_origin", "report_sha256")})
    for delivery in ledger["deliveries"].values():
        ack = delivery.get("acceptance")
        if not ack:
            continue
        dispatch = delivery["dispatch"]
        deliveries.append({**{k: dispatch[k] for k in ("envelope_identity_sha256", "envelope_file_sha256",
            "batch_relative", "repository_id", "delivery_kind")}, "snapshot_id": ack["snapshot_id"],
            "acceptance_identity_sha256": ack["identity_sha256"], "evidence_origin": "paired_relay_machine_acknowledgement",
            "batch_retired_from_transport": delivery.get("cache_retirement", {}).get("state") == "retired",
            "files": [{k: row[k] for k in ("relative", "sha256", "bytes")} for row in delivery["files"]]})
    body = {"contract": "fixed_nas_snapshot_recovery_index_v1", "repository_id": repository_id,
        "catalog_identity_sha256": catalog["identity_sha256"], "deliveries": deliveries,
        "releases": [{k: row[k] for k in ("dataset", "snapshot_id", "manifest_sha256", "manifest_relative",
            "source_fingerprint_sha256", "current_head", "missing_objects")} for row in catalog["releases"]],
        "abandoned_releases": catalog.get("abandoned_releases", []),
        "nas_provenance_verified_by_index": False, "canonical_reconstruction_verified": False,
        "full_history_backup_verified": False}
    return {**body, "identity_sha256": identity_sha256(body)}


class BackupStream:
    def __init__(self, configuration: dict):
        self.config = configuration
        if type(configuration.get("include_unreferenced_cold_objects", False)) is not bool:
            raise SnapshotError("unreferenced cold inclusion must be an explicit boolean")
        if type(configuration.get("cold_object_replication_enabled", True)) is not bool:
            raise SnapshotError("cold object replication must be an explicit boolean")
        if type(configuration.get('cold_metadata_replication_enabled', False)) is not bool:
            raise SnapshotError('cold metadata replication must be an explicit boolean')
        cache = configuration.get("cold_transport_cache", {})
        if cache.get("enabled") is True and configuration.get("automatic_batch_deletion") is not True:
            raise SnapshotError("cold cache retirement must be reported as automatic batch deletion")
        if (configuration.get("schema_version") != 1 or configuration.get("automatic_pruning") is not False
                or (configuration.get("automatic_batch_deletion") is not False and not (
                    configuration.get("automatic_batch_deletion") is True and cache.get("enabled") is True
                    and cache.get("scope") == "machine_acknowledged_reconstructible_cold_transport"
                    and isinstance(cache.get("user_instruction"), str) and bool(cache["user_instruction"].strip())))
                or configuration.get("source_cleanup_authorized") is not False):
            raise SnapshotError("stream preserves source, batches and NAS snapshots")
        self.cold = Path(configuration["cold_root"]).absolute()
        self.transport = Path(configuration["transport_root"]).absolute()
        self.receipts = Path(configuration["receipt_root"]).absolute()
        self.state = Path(configuration["state_root"]).absolute()
        for root in (self.cold, self.transport, self.receipts, self.state):
            if any(p.is_symlink() for p in (root, *root.parents)):
                raise SnapshotError("stream roots must not traverse symlinks")
        if (self.transport == self.cold or self.transport.is_relative_to(self.cold)
                or self.state.is_relative_to(self.transport) or self.receipts.is_relative_to(self.transport)):
            raise SnapshotError("stream roots have conflicting authorities")
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ledger_path = self.state / "ledger.json"
        for key in ("maximum_batch_bytes", "maximum_batch_files", "maximum_pending_bytes",
                    "maximum_pending_deliveries", "maximum_retained_transport_bytes", "reserve_bytes",
                    "readiness_max_age_seconds"):
            if type(configuration.get(key)) is not int or configuration[key] <= 0:
                raise SnapshotError("stream capacity and age limits must be positive integers")
        if "maximum_staging_bytes" in configuration and (type(configuration["maximum_staging_bytes"]) is not int
                                                       or configuration["maximum_staging_bytes"] <= 0):
            raise SnapshotError("staging capacity must be a positive integer")
        self.pipeline = configuration.get("pipeline", {})
        if self.pipeline:
            if set(self.pipeline) != {"maximum_waves_per_cycle", "copy_workers", "verify_workers", "retry_delays_seconds"}:
                raise SnapshotError("source pipeline needs an exact bounded policy")
            for key in ("maximum_waves_per_cycle", "copy_workers", "verify_workers"):
                if type(self.pipeline[key]) is not int or not 1 <= self.pipeline[key] <= 16:
                    raise SnapshotError("source pipeline workers and waves must be between 1 and 16")
            if not isinstance(self.pipeline["retry_delays_seconds"], list) or not self.pipeline["retry_delays_seconds"] or any(
                    type(n) is not int or not 1 <= n <= 86400 for n in self.pipeline["retry_delays_seconds"]):
                raise SnapshotError("source pipeline retry delays must be bounded")

    def storage_guard(self) -> None:
        for mount_name, expected_source in self.config.get("required_mounts", {}).items():
            if mounted_volume(Path(mount_name))[2] != expected_source:
                raise SnapshotError("backup transport has the wrong physical storage volume")
        if self.config.get("require_transport_mount", True):
            mounted_volume(self.transport)
        if self.config.get("read_root"):
            raise SnapshotError("larger-request D aliases failed full-size validation; use the canonical 8 KiB mount")
        if not self.transport.is_dir() or not (self.transport / ".stfolder").is_dir():
            raise SnapshotError("backup transport marker is missing")
        ignore = _regular(self.transport / ".stignore").read_text().splitlines()
        if not {"(?d).staging", "(?d).staging/**"} <= set(ignore):
            raise SnapshotError("unready staging must be excluded from Syncthing")

    def backup_read_root(self) -> Path:
        """The authoritative, measured-stable D reader for formal backups."""
        return self.cold

    def load_ledger(self) -> dict:
        value = json.loads(self.ledger_path.read_bytes()) if self.ledger_path.exists() else empty_ledger()
        if value.get("contract") != LEDGER or not isinstance(value.get("deliveries"), dict):
            raise SnapshotError("unknown stream ledger")
        return value

    def register_baseline(self, report_path: Path, delivery: Path) -> dict:
        self.storage_guard()
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            baseline = reported_baseline(delivery, report_path, self.config["repository_id"])
            ledger = self.load_ledger()
            existing = [r for r in ledger["reported_baseline"]
                        if r["envelope_identity_sha256"] == baseline["envelope_identity_sha256"]]
            if existing and existing != [baseline]:
                raise SnapshotError("preserve the previous relayed pilot baseline")
            if not existing:
                ledger["reported_baseline"].append(baseline)
                private_json(self.ledger_path, ledger)
            return {"state": "user_reported_pilot_baseline_registered", "envelope_identity_sha256": baseline["envelope_identity_sha256"],
                    "files": len(baseline["files"]), "machine_acknowledged": False}

    def refresh_auxiliary(self, *, wait_for_owner: bool = False) -> dict:
        """Retain a prior unsubmitted snapshot and freeze the current work."""
        self.storage_guard()
        if not self.config.get("auxiliary"):
            raise SnapshotError("no auxiliary snapshot scope is configured")
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            self.storage_guard()
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            pending = ledger.get("auxiliary_pending")
            if pending:
                from stockagent.data_sync.backup_auxiliary import working_tree_inventory, control_input
                config = self.config["auxiliary"]
                tree = working_tree_inventory(Path(config["repository_root"]), documentation=config["include_documentation"],
                                              configs=config["include_configs"])
                control, _, _ = control_input(Path(config["control_receipt"]))
                if identity_sha256({"code": tree["identity_sha256"], "control": control}) == pending["export"]["content_identity_sha256"]:
                    return {"state": "current_auxiliary_snapshot_staged", "published_to_relay": False,
                            "pending": True, "source_or_staging_deleted": False}
                del ledger["auxiliary_pending"]
            if pending:
                ledger.setdefault("quarantined_auxiliary_snapshots", []).append({
                    "staging_relative": pending["staging_relative"], "reason": "explicit_current_working_tree_refresh"})
            ledger["auxiliary_last_capture_epoch"] = 0
            private_json(self.ledger_path, ledger)
            self.stage_auxiliary(ledger)
            private_json(self.ledger_path, ledger)
            return {"state": "current_auxiliary_snapshot_staged", "published_to_relay": False,
                    "pending": bool(ledger.get("auxiliary_pending")), "source_or_staging_deleted": False}

    def transport_usage(self) -> dict:
        # Include old pilots, orphaned failed copies and quarantined staging.
        # Journal counters alone would miss those retained physical bytes.
        total = staging = 0
        if any(p.is_symlink() for p in (self.transport,*self.transport.parents)):
            raise SnapshotError("transport capacity root is redirected")
        before = self.transport.stat()
        def walk_error(error):
            raise error
        # A directory FD pins each traversed parent. One no-follow stat per
        # entry replaces repeatedly walking every ancestor on slow DrvFs/9p.
        for parent, directories, names, directory_fd in os.fwalk(
                self.transport, follow_symlinks=False, onerror=walk_error):
            root = Path(parent)
            opened = os.fstat(directory_fd)
            if opened.st_dev != before.st_dev or (root == self.transport and opened.st_ino != before.st_ino):
                raise SnapshotError("transport contains another mounted volume")
            for name in directories:
                observed = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if not stat.S_ISDIR(observed.st_mode) or observed.st_dev != before.st_dev:
                    raise SnapshotError("transport contains a redirected directory")
            for name in names:
                path = root / name
                observed = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                if not stat.S_ISREG(observed.st_mode) or observed.st_dev != before.st_dev:
                    raise SnapshotError("transport contains a redirected or nonregular file")
                size = observed.st_size
                total += size
                if path.is_relative_to(self.transport / ".staging"):
                    staging += size
        after = self.transport.stat()
        if (before.st_dev,before.st_ino) != (after.st_dev,after.st_ino):
            raise SnapshotError("transport authority changed during capacity inventory")
        return {"retained_transport_bytes": total, "retained_staging_bytes": staging}

    def staging_room(self, usage: dict) -> int:
        return min(self.config.get("maximum_staging_bytes", self.config["maximum_pending_bytes"]) - usage["retained_staging_bytes"],
                   self.config["maximum_retained_transport_bytes"] - usage["retained_transport_bytes"],
                   shutil.disk_usage(self.transport).free - self.config["reserve_bytes"])

    def ingest(self, ledger: dict) -> list[dict]:
        errors = []
        if not self.receipts.exists():
            return errors
        for path in sorted(self.receipts.glob("acceptance-*.json")):
            try:
                ack = read_json(path)
                key = ack["envelope_identity_sha256"]
                delivery = ledger["deliveries"].get(key)
                if not delivery:
                    # Pilot acknowledgements can coexist with the new stream.
                    continue
                if path.name != "acceptance-" + key + ".json":
                    raise ValueError("NAS receipt filename differs")
                validate_ack(ack, delivery["dispatch"])
                if delivery.get("acceptance") and delivery["acceptance"] != ack:
                    raise ValueError("immutable NAS acknowledgement changed")
                delivery["acceptance"] = ack
                delivery["state"] = "nas_file_restore_acknowledged"
            except (OSError, ValueError, KeyError, TypeError) as error:
                errors.append({"receipt": path.name, "reason": type(error).__name__})
        return errors

    def cycle(self, *, publish: bool = True) -> dict:
        self.storage_guard()
        started = time.perf_counter()
        lock = self.state / "owner.lock"
        if lock.is_symlink():
            raise SnapshotError("stream owner lock is redirected")
        with lock.open("a") as owner:
            try:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise SnapshotError("another source stream owner is active") from error
            ledger = self.load_ledger()
            self.recover_publications(ledger)
            receipt_errors = self.ingest(ledger)
            automatic_recovery = {"state": "disabled"}
            if publish and self.config.get("automatic_recovery", {}).get("enabled") is True:
                from stockagent.data_sync.recovery_queue import publish_requests
                try:
                    automatic_recovery = publish_requests(self.config, self.transport, ledger)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    automatic_recovery = {"state": "request_publication_failed", "error_type": type(error).__name__}
            cache_result = self.retire_transport_cache(ledger) if (
                publish and not receipt_errors and self.config.get("cold_transport_cache", {}).get("enabled")) else {
                "retired_deliveries": 0, "retired_logical_bytes": 0, "errors": [],
                "deferred": bool(receipt_errors) or not publish}
            auxiliary_error = None
            if self.config.get("auxiliary"):
                try:
                    self.stage_auxiliary(ledger)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    auxiliary_error = type(error).__name__
            catalog = capture_catalog(self.backup_read_root(), Path(self.config["publication_catalog"]),
                                      Path(self.config["history_disposition"]) if self.config.get("history_disposition") else None,
                                      include_unreferenced_objects=self.config.get("include_unreferenced_cold_objects", False))
            private_json(self.state / "catalog.json", catalog)
            covered = coverage(catalog, ledger)
            state = "inspected"
            readiness_error = None
            published_waves, export_errors = [], []
            scan_errors = []
            retries = ledger.setdefault("export_retries", {})
            # Hashing/indexing batch N runs while the source exports batch N+1.
            # The single controller still owns every ledger mutation.
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="backup-scan") as scanner:
                scans = []
                for _ in range(self.pipeline.get("maximum_waves_per_cycle", 1) if publish else 0):
                    # NAS ACKs can arrive while the preceding wave is copied.
                    # Reconcile on this controller before applying admission.
                    if published_waves:
                        receipt_errors.extend(self.ingest(ledger))
                    covered = coverage(catalog, ledger)
                    try:
                        ready = read_json(self.receipts / "readiness.json")
                        validate_readiness(ready, self.config)
                        reserve = max(self.config["reserve_bytes"], ready["reserve_bytes"])
                        limit = min(self.config["maximum_batch_bytes"], ready["maximum_batch_bytes"],
                            ready["ingress_free_bytes"] - reserve - covered["pending_bytes"],
                            ready["nas_free_bytes"] - reserve - covered["pending_bytes"],
                            self.config["maximum_pending_bytes"] - covered["pending_bytes"],
                            self.staging_room(self.transport_usage()))
                    except (OSError, ValueError, KeyError, TypeError) as error:
                        readiness_error = type(error).__name__
                        state = "waiting_receiver_readiness"
                        break
                    if covered["pending_delivery_count"] >= self.config["maximum_pending_deliveries"] or limit <= 1024**2:
                        state = "backpressure"
                        break
                    deferred = {key for key, item in retries.items() if item["next_attempt_epoch"] > time.time()}
                    auxiliary = ledger.get("auxiliary_pending")
                    metadata_only = (not self.config.get("cold_object_replication_enabled", True)
                                     and self.config.get("cold_metadata_replication_enabled", False))
                    selection_covered = covered['published_file_keys'] | covered['accepted_file_keys'] | deferred
                    if metadata_only:
                        from stockagent.data_sync.nas_coverage import restic_keys
                        # Pilot reports remain provenance, not machine restore
                        # receipts. Re-deliver their small metadata for a proof.
                        selection_covered = covered['published_file_keys'] | restic_keys(ledger) | deferred
                    selection_catalog = {**catalog, "files": [r for r in catalog["files"] if r["role"] == "cold_metadata"]} if metadata_only else catalog
                    selection = [] if ((not self.config.get("cold_object_replication_enabled", True) and not metadata_only)
                        or auxiliary and auxiliary["complete_bytes"] <= limit) else select_wave(
                        selection_catalog, selection_covered,
                        maximum_bytes=limit, maximum_files=self.config["maximum_batch_files"])
                    selection_identity = catalog['identity_sha256']
                    if (not selection and metadata_only and self.config.get('metadata_backfill_catalog')
                            and not (auxiliary and auxiliary['complete_bytes'] <= limit)):
                        fixed_path = _regular(Path(self.config['metadata_backfill_catalog']))
                        if fixed_path.stat().st_size > 128 * 1024**2:
                            raise SnapshotError('fixed metadata backfill inventory is oversized')
                        raw = fixed_path.read_bytes()
                        if hashlib.sha256(raw).hexdigest() != self.config.get('metadata_backfill_sha256'):
                            raise SnapshotError('fixed metadata backfill inventory changed')
                        fixed = json.loads(raw)
                        if fixed.get('contract') != CATALOG:
                            raise SnapshotError('fixed metadata backfill contract differs')
                        selection = select_wave({**fixed, 'files': [r for r in fixed['files'] if r['role'] == 'cold_metadata']},
                            selection_covered,
                            maximum_bytes=limit, maximum_files=self.config['maximum_batch_files'])
                        selection_identity = fixed['identity_sha256']
                    if auxiliary and auxiliary["complete_bytes"] <= limit:
                        staging = safe_path(self.transport, auxiliary["staging_relative"])
                        key = auxiliary["export"]["envelope_identity_sha256"]
                        self.publish_closed(ledger, auxiliary["files"], staging, key,
                                            catalog["identity_sha256"], "code_and_control_backup")
                        ledger["last_auxiliary_delivery"] = key
                        ledger.pop("auxiliary_pending", None)
                    elif selection:
                        staging = self.transport / ".staging" / uuid.uuid4().hex
                        try:
                            result = export_incremental_delivery(self.backup_read_root(), selection, staging,
                                catalog_identity=selection_identity,
                                copy_workers=self.pipeline.get("copy_workers", 1),
                                verify_workers=self.pipeline.get("verify_workers", 1))
                        except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                            if not self.pipeline:
                                raise  # Preserve the existing diagnostic interface.
                            delays = self.pipeline["retry_delays_seconds"]
                            for row in selection:
                                key = file_key(row)
                                attempt = retries.get(key, {}).get("attempts", 0) + 1
                                retries[key] = {"attempts": attempt,
                                    "next_attempt_epoch": time.time() + delays[min(attempt - 1, len(delays) - 1)],
                                    "error_type": type(error).__name__,
                                    "staging_relative": staging.relative_to(self.transport).as_posix()}
                            export_errors.append({"error_type": type(error).__name__, "files": len(selection),
                                "staging_relative": staging.relative_to(self.transport).as_posix()})
                            private_json(self.ledger_path, ledger)
                            state = "export_requeued"
                            continue
                        key = result["envelope_identity_sha256"]
                        self.publish_closed(ledger, selection, staging, key,
                                            selection_identity, "incremental_cold_objects")
                        for row in selection:
                            retries.pop(file_key(row), None)
                    else:
                        if not self.config.get("cold_object_replication_enabled", True):
                            state = "cold_replication_delegated_to_immutable_lake"
                            break
                        unsent = [r for r in catalog["files"] if file_key(r) not in (
                            covered["published_file_keys"] | covered["accepted_file_keys"])]
                        state = ("waiting_export_retry" if any(file_key(r) in deferred for r in unsent)
                            else "waiting_object_exceeds_batch_budget" if unsent
                            else "available_cold_files_complete_with_history_gaps" if catalog["missing_objects"]
                            else "caught_up_with_available_cold_files")
                        break
                    self.recover_publications(ledger)
                    private_json(self.ledger_path, ledger)
                    # Snapshot the controller-owned ledger before handing it to
                    # the scan thread; worker threads never touch that journal.
                    one = {"deliveries": {key: json.loads(json.dumps(ledger["deliveries"][key]))}}
                    scans.append(scanner.submit(self.scan_transport, one))
                    published_waves.append(key)
                    state = "published_waiting_nas_receipt"
                for future in scans:
                    try:
                        future.result()
                    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                        scan_errors.append(type(error).__name__)
            if published_waves:
                state = "published_waiting_nas_receipt"
            self.recover_publications(ledger)
            private_json(self.ledger_path, ledger)
            covered = coverage(catalog, ledger)
            result = {"state": state, "catalog_identity_sha256": catalog["identity_sha256"],
                "cold_object_replication_enabled": self.config.get("cold_object_replication_enabled", True),
                **self.transport_usage(),
                **{k: v for k, v in covered.items() if not k.endswith("file_keys")},
                "receipt_errors": receipt_errors, "readiness_error": readiness_error,
                "auxiliary_error": auxiliary_error,
                "pipeline": {"published_waves": published_waves, "export_errors": export_errors,
                    "scan_errors": scan_errors, "deferred_files": sum(
                        item["next_attempt_epoch"] > time.time() for item in retries.values()),
                    "maximum_in_flight": self.config["maximum_pending_deliveries"],
                    "copy_workers": self.pipeline.get("copy_workers", 1),
                    "verify_workers": self.pipeline.get("verify_workers", 1)},
                "automatic_recovery_requests": automatic_recovery,
                "last_auxiliary_delivery": ledger.get("last_auxiliary_delivery"),
                "auxiliary_pending": bool(ledger.get("auxiliary_pending")),
                "auxiliary_nas_acknowledged": bool(ledger.get("last_auxiliary_delivery") and
                    ledger["deliveries"][ledger["last_auxiliary_delivery"]].get("acceptance")),
                "cold_transport_cache": cache_result,
                "automatic_pruning": False, "automatic_batch_deletion": self.config["automatic_batch_deletion"],
                "automatic_batch_deletion_scope": self.config.get("cold_transport_cache", {}).get("scope"),
                "source_cleanup_authorized": False, "complete_cycle_seconds": time.perf_counter() - started,
                "catalog_observed_at_utc": catalog["observed_at_utc"],
                "observed_at_utc": datetime.now(timezone.utc).isoformat()}
            from stockagent.data_sync.nas_recovery_acceptance import recovery_status
            if self.config.get('immutable_archive_state_root'):
                from stockagent.data_sync.nas_coverage import combined_coverage, read_ledger
                try:
                    result['combined_nas_coverage'] = combined_coverage(catalog, ledger,
                        read_ledger(Path(self.config['immutable_archive_state_root']) / 'source-replication-ledger.json'))
                except (OSError, ValueError, KeyError, TypeError) as error:
                    result['combined_nas_coverage'] = {'state': 'unavailable', 'error_type': type(error).__name__}
            try:
                result["independent_recovery"] = recovery_status(self.config, self.receipts)
            except (OSError, ValueError, KeyError, TypeError, SnapshotError) as error:
                # Semantic recovery failure is visible separately; it cannot
                # erase successful file ACKs or stop ordinary backup transport.
                result["independent_recovery"] = {"state": "invalid_recovery_evidence", "error": type(error).__name__}
            if self.config.get("automatic_recovery", {}).get("enabled") is True:
                from stockagent.data_sync.recovery_queue import queue_status
                try:
                    result["automatic_recovery"] = queue_status(self.config, self.transport, self.receipts)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    result["automatic_recovery"] = {"state": "invalid_queue_evidence", "error_type": type(error).__name__}
            if self.pipeline:
                from stockagent.data_sync.backup_relay_pipeline import receiver_status
                try:
                    result["parallel_receiver"] = receiver_status(self.config, self.transport, self.receipts)
                except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                    result["parallel_receiver"] = {"state": "invalid_pipeline_evidence", "error_type": type(error).__name__}
            private_json(self.state / "status.json", result)
            atomic_write_bytes(self.transport / "tools/source-status.json",
                               json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2).encode(), mode=0o600)
            index_path = self.transport / "tools/recovery-index.json"
            raw_index = json.dumps(recovery_index(catalog, ledger, self.config["repository_id"]),
                                   ensure_ascii=False, sort_keys=True, indent=2).encode()
            if not index_path.exists() or index_path.read_bytes() != raw_index:
                atomic_write_bytes(index_path, raw_index, mode=0o600)
            if not published_waves or scan_errors:
                self.scan_transport(ledger)
            return result

    def publish_recovery_requests(self, *, wait_for_owner: bool = False) -> dict:
        """Publish data-only tasks without interrupting or duplicating a cycle."""
        from stockagent.data_sync.recovery_queue import publish_requests, queue_status
        self.storage_guard()
        with (self.state / "owner.lock").open("a") as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | (0 if wait_for_owner else fcntl.LOCK_NB))
            self.storage_guard()
            ledger = self.load_ledger()
            errors = self.ingest(ledger)
            private_json(self.ledger_path, ledger)
            if errors:
                return {"state": "waiting_valid_file_receipts", "receipt_errors": errors}
            result = publish_requests(self.config, self.transport, ledger)
            self.scan_transport(ledger)
            return {**result, "automatic_recovery": queue_status(self.config, self.transport, self.receipts)}


    def retire_transport_cache(self, ledger: dict) -> dict:
        from stockagent.data_sync.backup_transport_cache import retire
        candidates = [(key, d) for key, d in ledger["deliveries"].items()
                      if d.get("acceptance") and d["dispatch"]["delivery_kind"] == "incremental_cold_objects"
                      and d.get("cache_retirement", {}).get("state") != "retired"]
        result = {"retired_deliveries": 0, "retired_logical_bytes": 0, "errors": [], "deferred": False}
        unscanned = [d for d in ledger["deliveries"].values()
                     if d.get("cache_retirement", {}).get("state") == "retired"
                     and not d["cache_retirement"].get("deletion_scan_requested")]
        if not candidates and not unscanned:
            return result
        from scripts.configure_artifact_ingress_syncthing import request
        base, key, folder = self.paired_transport()
        status = request(base, key, "/rest/db/status", {"folder": folder})
        peer = request(base, key, "/rest/db/completion", {"folder": folder, "device": self.config["receiver_device_id"]})
        if (status["state"] != "idle" or any(status.get(k, 0) for k in (
                "errors", "pullErrors", "needBytes", "needTotalItems", "needDeletes"))
                or status.get("watchError") or status.get("error")
                or peer["completion"] != 100 or peer["remoteState"] != "valid"
                or any(peer.get(k, 0) for k in ("needBytes", "needItems", "needDeletes"))):
            result["deferred"] = True
            return result
        for envelope, delivery in candidates:
            try:
                plan = retire(self, ledger, envelope)
                result["retired_deliveries"] += 1
                result["retired_logical_bytes"] += plan["complete_bytes"]
            except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
                result["errors"].append({"envelope": envelope, "reason": type(error).__name__})
        unscanned = [d for d in ledger["deliveries"].values()
                     if d.get("cache_retirement", {}).get("state") == "retired"
                     and not d["cache_retirement"].get("deletion_scan_requested")]
        if unscanned:
            # DrvFs does not supply reliable watcher events. Explicitly index
            # source-owned deletions before creating another large batch.
            request(base, key, "/rest/db/scan", {"folder": folder}, method="POST",
                    timeout=self.config.get("scan_timeout_seconds", 30))
            for delivery in unscanned:
                delivery["cache_retirement"]["deletion_scan_requested"] = True
            private_json(self.ledger_path, ledger)
        return result

    def paired_transport(self) -> tuple[str, str, str]:
        from scripts.configure_artifact_ingress_syncthing import credentials, request
        base, key = credentials()
        folder_id = self.config["syncthing_folder_id"]
        folder = request(base, key, "/rest/config/folders/" + folder_id)
        if (folder["path"] != str(self.transport) or folder["type"] != "sendonly" or folder["paused"]
                or {d["deviceID"] for d in folder["devices"]} != {
                    self.config["producer_device_id"], self.config["receiver_device_id"]}):
            raise SnapshotError("backup transport no longer has the paired send-only scope")
        return base, key, folder_id

    def scan_transport(self, ledger: dict) -> None:
        if not self.config.get("syncthing_folder_id"):
            return
        from scripts.configure_artifact_ingress_syncthing import request
        base, key, folder_id = self.paired_transport()
        timeout = self.config.get("scan_timeout_seconds", 30)
        request(base, key, "/rest/db/scan", {"folder": folder_id, "sub": "tools"}, method="POST", timeout=timeout)
        for delivery in ledger["deliveries"].values():
            if not delivery.get("acceptance"):
                request(base, key, "/rest/db/scan", {"folder": folder_id,
                    "sub": delivery["dispatch"]["batch_relative"]}, method="POST", timeout=timeout)

    def publish_closed(self, ledger: dict, rows: list[dict], staging: Path, envelope_id: str,
                       catalog_identity: str, kind: str) -> None:
        from scripts.verify_backup_delivery import verify
        verified = verify(staging, envelope_id, include_file_signatures=kind == "incremental_cold_objects",
                          workers=self.pipeline.get("verify_workers", 1))
        batch = self.transport / ("batch-" + envelope_id)
        if batch.exists():
            raise SnapshotError("unrecorded existing batch needs explicit recovery")
        raw = (staging / "backup-envelope.json").read_bytes()
        size = verified["bytes_verified"] + len(raw) + (staging / "READY").stat().st_size
        dispatch = signed({"contract": DISPATCH, "batch_relative": batch.name,
            "producer_device_id": self.config["producer_device_id"],
            "receiver_device_id": self.config["receiver_device_id"], "repository_id": self.config["repository_id"],
            "envelope_identity_sha256": envelope_id, "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
            "complete_files": verified["files_verified"] + 2, "complete_bytes": size,
            "catalog_identity_sha256": catalog_identity, "delivery_kind": kind,
            "complete_release_in_this_delivery": False})
        ledger["deliveries"][envelope_id] = {"state": "publication_prepared", "dispatch": dispatch,
            "files": rows, "staging_relative": staging.relative_to(self.transport).as_posix(),
            "transport_signatures": verified.get("file_signatures", {})}
        private_json(self.ledger_path, ledger)
        self.storage_guard()
        os.rename(staging, batch)
        directory = os.open(self.transport, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        atomic_public(self.transport / "tools/dispatch" / (envelope_id + ".json"), dispatch)
        ledger["deliveries"][envelope_id]["state"] = "published_waiting_nas_receipt"

    def stage_auxiliary(self, ledger: dict) -> None:
        from stockagent.data_sync.backup_auxiliary import (
            export_auxiliary, working_tree_inventory, control_input, PRIVACY_FILTER_VERSION,
        )
        if ledger.get("auxiliary_pending"):
            pending = ledger["auxiliary_pending"]
            if pending["export"].get("privacy_filter_version") == PRIVACY_FILTER_VERSION:
                return
            ledger.setdefault("quarantined_auxiliary_snapshots", []).append({
                "staging_relative": pending["staging_relative"], "reason": "superseded_privacy_filter_version"})
            del ledger["auxiliary_pending"]
            ledger["auxiliary_last_capture_epoch"] = 0
        config = self.config["auxiliary"]
        latest = Path(config["control_receipt"])
        receipt_sha = hashlib.sha256(_regular(latest).read_bytes()).hexdigest()
        if (time.time() - ledger.get("auxiliary_last_capture_epoch", 0) < config["interval_seconds"]
                and receipt_sha == ledger.get("auxiliary_source_control_receipt_sha256")):
            return
        if self.staging_room(self.transport_usage()) < config["maximum_bytes"] + 1024**2:
            raise SnapshotError("retain failed staging; auxiliary capacity budget reached")
        tree = working_tree_inventory(Path(config["repository_root"]), documentation=config["include_documentation"],
                                      configs=config["include_configs"])
        control, _, _ = control_input(latest)
        content_identity = identity_sha256({"code": tree["identity_sha256"], "control": control})
        if content_identity == ledger.get("last_auxiliary_content_identity"):
            ledger["auxiliary_last_capture_epoch"] = time.time()
            ledger["auxiliary_source_control_receipt_sha256"] = receipt_sha
            return
        staging = self.transport / ".staging" / ("auxiliary-" + uuid.uuid4().hex)
        result = export_auxiliary(Path(config["repository_root"]), staging, control_receipt=latest,
            documentation=config["include_documentation"], configs=config["include_configs"],
            maximum_bytes=config["maximum_bytes"])
        envelope = json.loads((staging / "backup-envelope.json").read_bytes())
        ledger["auxiliary_pending"] = {"staging_relative": staging.relative_to(self.transport).as_posix(),
            "export": result, "complete_bytes": sum(r["bytes"] for r in envelope["files"]) + sum(
                (staging / n).stat().st_size for n in ("READY", "backup-envelope.json")),
            "files": [{**r, "relative": "auxiliary/" + result["content_identity_sha256"] + "/" + r["relative"],
                       "role": "auxiliary"} for r in envelope["files"]]}
        ledger["auxiliary_last_capture_epoch"] = time.time()
        ledger["auxiliary_source_control_receipt_sha256"] = receipt_sha
        ledger["last_auxiliary_content_identity"] = result["content_identity_sha256"]
        private_json(self.ledger_path, ledger)

    def recover_publications(self, ledger: dict) -> None:
        from scripts.verify_backup_delivery import verify
        for key, delivery in ledger["deliveries"].items():
            if delivery["state"] != "publication_prepared":
                self.finish_auxiliary_handoff(ledger, key, delivery)
                continue
            target = self.transport / delivery["dispatch"]["batch_relative"]
            staging = safe_path(self.transport, delivery["staging_relative"])
            selected = target if target.exists() else staging
            verify(selected, key)
            if hashlib.sha256((selected / "backup-envelope.json").read_bytes()).hexdigest() != delivery["dispatch"]["envelope_file_sha256"]:
                raise SnapshotError("interrupted publication envelope differs")
            if not target.exists():
                os.rename(staging, target)
            atomic_public(self.transport / "tools/dispatch" / (key + ".json"), delivery["dispatch"])
            delivery["state"] = "published_waiting_nas_receipt"
            self.finish_auxiliary_handoff(ledger, key, delivery)

    @staticmethod
    def finish_auxiliary_handoff(ledger: dict, key: str, delivery: dict) -> None:
        pending = ledger.get("auxiliary_pending")
        if (delivery["state"] in {"published_waiting_nas_receipt", "nas_file_restore_acknowledged"}
                and delivery["dispatch"].get("delivery_kind") == "code_and_control_backup" and pending
                and pending["export"]["envelope_identity_sha256"] == key):
            ledger["last_auxiliary_delivery"] = key
            del ledger["auxiliary_pending"]
