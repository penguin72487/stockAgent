"""Exact union of authority-validated NAS ledgers; never add byte counters.

This reports the retained canonical files in a supplied inventory. It does not
claim that unpublished workspaces or discarded historical observations exist.
User-relayed pilot evidence remains separate from machine restore evidence.
"""
from __future__ import annotations

import json
from pathlib import Path
import stat


def read_ledger(path: Path) -> dict:
    if not path.exists():
        return {"deliveries": {}}
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > 128 * 1024**2:
        raise ValueError("NAS authority ledger is redirected or oversized")
    with path.open("rb") as stream:
        value = json.load(stream)
    if not isinstance(value, dict) or not isinstance(value.get("deliveries"), dict):
        raise ValueError("invalid NAS authority ledger")
    return value


def restic_keys(ledger: dict, *, include_reported: bool = False) -> set[str]:
    keys = {row["relative"] + "@" + row["sha256"]
            for delivery in ledger["deliveries"].values() if delivery.get("acceptance")
            for row in delivery["files"]}
    if include_reported:
        keys.update(row["relative"] + "@" + row["sha256"]
                    for delivery in ledger.get("reported_baseline", ()) for row in delivery["files"])
    return keys


def archive_keys(ledger: dict) -> set[str]:
    keys = set()
    for identity, row in ledger["deliveries"].items():
        proof = row.get("nas_acceptance", {})
        if proof.get("state") != "nas_archive_file_recovery_verified":
            continue
        if proof.get("delivery_identity_sha256") != identity:
            raise ValueError("NAS archive ledger acceptance belongs to another delivery")
        keys.update(row["file_keys"])
    return keys


def transport_converged(local, peer, *, connected, folder_errors, system_errors):
    return (connected is True and local.get('state') == 'idle'
            and local.get('needTotalItems') == 0 and local.get('needBytes') == 0
            and local.get('errors') == 0 and not folder_errors and not system_errors
            and peer.get('needBytes') == 0 and peer.get('needItems') == 0
            and peer.get('needDeletes') == 0 and peer.get('completion') == 100)


def combined_coverage(catalog: dict, restic: dict, archive: dict) -> dict:
    confirmed = restic_keys(restic)
    reported = restic_keys(restic, include_reported=True) - confirmed
    archived = archive_keys(archive)
    machine = confirmed | archived
    accepted = machine | reported
    files = catalog["files"]
    available = {row["relative"] + "@" + row["sha256"]: row for row in files}
    covered = set(available) & accepted
    pending = set(available) - accepted
    measure = lambda keys: sum(available[k]["bytes"] for k in set(available) & keys)
    return {
        "contract": "deduplicated_nas_file_coverage_v1",
        "scope": "exact files in the supplied retained canonical inventory",
        "catalog_identity_sha256": catalog.get("identity_sha256"),
        "catalog_observed_at_utc": catalog.get("observed_at_utc"),
        "available_file_count": len(available),
        "available_bytes": measure(set(available)),
        "nas_covered_file_count": len(covered),
        "nas_covered_bytes": measure(covered),
        "machine_verified_file_count": len(set(available) & machine),
        "machine_verified_bytes": measure(machine),
        "machine_pending_file_count": len(set(available) - machine),
        "machine_pending_bytes": measure(set(available) - machine),
        "reported_only_file_count": len(set(available) & (reported - machine)),
        "restic_covered_bytes": measure(confirmed | reported),
        "immutable_archive_covered_bytes": measure(archived),
        "overlap_bytes": measure((confirmed | reported) & archived),
        "pending_file_count": len(pending),
        "pending_bytes": measure(pending),
        "all_available_cold_files_verified": bool(available) and not pending
            and not catalog.get("missing_objects") and not catalog.get("metadata_errors")
            and not (set(available) & (reported - machine)),
        "all_history_backup_verified": False,
        "unpublished_sources_included": False,
    }
