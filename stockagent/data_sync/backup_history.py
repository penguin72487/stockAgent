"""Explicit terminal dispositions for unrecoverable, non-current history.

The decision drops recovery requirements for already unavailable bytes. It does
not delete a manifest, a pin, a shared object, or a materialized quarantine.
Existing bytes and metadata still travel in the backup catalog for audit.
"""
from __future__ import annotations

from pathlib import Path

from scripts.backup_delivery_receipt import HASH, read_json
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.runtime_identity import identity_sha256

CONTRACT = "explicit_unrecoverable_backup_history_disposition_v1"


def build_disposition(catalog: dict, *, authorization: str, evidence: list[dict]) -> dict:
    if not authorization.strip() or not evidence:
        raise SnapshotError("history disposition needs explicit authorization and recovery evidence")
    releases = [r for r in catalog["releases"] if r["missing_objects"]]
    if not releases or any(r["current_head"] for r in releases) or catalog["metadata_errors"]:
        raise SnapshotError("only valid, non-current, unrecoverable history may be abandoned")
    body = {
        "contract": CONTRACT,
        "authorization": authorization,
        "source_catalog_identity_sha256": catalog["identity_sha256"],
        "recovery_evidence": evidence,
        "decision": "abandon_missing_history_recovery",
        "existing_source_or_metadata_deletion_authorized": False,
        "releases": [{k: r[k] for k in (
            "dataset", "snapshot_id", "manifest_relative", "manifest_sha256",
            "source_fingerprint_sha256", "missing_objects",
        )} for r in releases],
        "unavailable_objects": catalog["missing_objects"],
    }
    return {**body, "identity_sha256": identity_sha256(body)}


def apply_disposition(catalog_body: dict, path: Path | None) -> dict:
    """Bind every waiver to original bytes; never waive a new current head."""
    body = {**catalog_body, "abandoned_releases": [], "abandoned_missing_objects": []}
    if path is None:
        return body
    value = read_json(path)
    claimed = value.get("identity_sha256")
    content = {k: v for k, v in value.items() if k != "identity_sha256"}
    if (value.get("contract") != CONTRACT or not isinstance(claimed, str)
            or not HASH.fullmatch(claimed) or claimed != identity_sha256(content)
            or value.get("decision") != "abandon_missing_history_recovery"
            or value.get("existing_source_or_metadata_deletion_authorized") is not False
            or not isinstance(value.get("authorization"), str) or not value["authorization"].strip()
            or not value.get("recovery_evidence")):
        raise SnapshotError("invalid explicit history disposition")
    for proof in value["recovery_evidence"]:
        if not isinstance(proof.get("sha256"), str) or not HASH.fullmatch(proof["sha256"]):
            raise SnapshotError("recovery evidence must be content pinned")
    known = {(r["dataset"], r["snapshot_id"]): r for r in body["releases"]}
    unavailable = {r["relative"]: r for r in value["unavailable_objects"]}
    if len(unavailable) != len(value["unavailable_objects"]) or not unavailable:
        raise SnapshotError("history disposition needs unique unavailable objects")
    abandoned = set()
    for waiver in value["releases"]:
        key = (waiver["dataset"], waiver["snapshot_id"])
        release = known.get(key)
        if key in abandoned or release is None or release["current_head"]:
            raise SnapshotError("abandoned history changed or is now a current head")
        if any(waiver[k] != release[k] for k in (
            "manifest_relative", "manifest_sha256", "source_fingerprint_sha256",
        )):
            raise SnapshotError("abandoned history manifest identity changed")
        missing = waiver["missing_objects"]
        required = set(release["required_file_keys"])
        if (not missing or len(set(missing)) != len(missing)
                or not set(release["missing_objects"]) <= set(missing)
                or any(name not in unavailable
                       or name + "@" + unavailable[name]["sha256"] not in required for name in missing)):
            raise SnapshotError("abandoned history unavailable-object identity changed")
        abandoned.add(key)
        body["abandoned_releases"].append({
            **waiver, "disposition_identity_sha256": claimed,
            "decision": value["decision"], "historical_recovery_claimed": False,
        })
    if not abandoned or set(unavailable) != {
        name for r in value["releases"] for name in r["missing_objects"]
    }:
        raise SnapshotError("history disposition object/release set differs")
    body["releases"] = [r for r in body["releases"] if (r["dataset"], r["snapshot_id"]) not in abandoned]
    active_missing = {name for r in body["releases"] for name in r["missing_objects"]}
    body["abandoned_missing_objects"] = [r for r in body["missing_objects"] if r["relative"] not in active_missing]
    body["missing_objects"] = [r for r in body["missing_objects"] if r["relative"] in active_missing]
    body["history_disposition_identity_sha256"] = claimed
    body["scope"] += "; explicitly abandoned missing history excluded from recovery requirements; existing bytes retained"
    return body
