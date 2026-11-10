"""Explicit terminal dispositions for unrecoverable, non-current history.

The decision drops recovery requirements for already unavailable bytes. It does
not delete a manifest, a pin, a shared object, or a materialized quarantine.
Existing bytes and metadata still travel in the backup catalog for audit.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from scripts.backup_delivery_receipt import HASH, read_json
from stockagent.data_sync.desync_snapshots import SnapshotError, validate_slug
from stockagent.runtime_identity import identity_sha256

CONTRACT = "explicit_unrecoverable_backup_history_disposition_v1"
RETIREMENT_CONTRACT = "stockagent.superseded_repack_history.v1"


def _stable_regular_bytes(path: Path, *, maximum: int) -> bytes:
    """Read bounded local owner evidence without following redirected paths."""
    if path.is_symlink() or not path.is_file() or path.resolve(strict=True) != path:
        raise SnapshotError("history retirement evidence is missing or redirected")
    before = path.stat()
    if before.st_size > maximum:
        raise SnapshotError("history retirement evidence exceeds its bound")
    payload = path.read_bytes()
    after = path.stat()
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if signature(before) != signature(after) or len(payload) != before.st_size:
        raise SnapshotError("history retirement evidence changed during capture")
    return payload


def _retirement_proofs(root: Path, waivers: list[dict], unavailable: dict) -> dict:
    """Bind absent waivers to completed exact-version retirement journals.

    A completed retirement is evidence that an observed manifest was removed,
    not that its missing source objects were recovered or backed up. Only the
    relevant dataset directories are inspected; unrelated journals cannot
    authorize a waiver. The original immutable plan remains the evidence.
    """
    root = root.absolute()
    if root.is_symlink() or not root.is_dir() or root.resolve(strict=True) != root:
        raise SnapshotError("history retirement evidence root is unavailable or redirected")
    wanted = {}
    for waiver in waivers:
        dataset = validate_slug(waiver["dataset"], "history dataset")
        snapshot = validate_slug(waiver["snapshot_id"], "history snapshot")
        key = (dataset, snapshot, waiver["manifest_sha256"])
        if key in wanted:
            raise SnapshotError("duplicate historical release disposition")
        wanted[key] = waiver
    proofs = {}
    for dataset in sorted({key[0] for key in wanted}):
        dataset_root = root / dataset
        if not dataset_root.exists():
            continue
        if dataset_root.is_symlink() or not dataset_root.is_dir() or dataset_root.resolve(strict=True) != dataset_root:
            raise SnapshotError("history retirement dataset root is redirected")
        stages = sorted(dataset_root.iterdir())
        if len(stages) > 4096:
            raise SnapshotError("history retirement dataset exceeds stage bound")
        for stage in stages:
            if not HASH.fullmatch(stage.name):
                continue
            if stage.is_symlink() or not stage.is_dir() or stage.resolve(strict=True) != stage:
                raise SnapshotError("history retirement stage is redirected")
            journals = sorted(stage.glob("*history-retirement-*.json"))
            if len(journals) > 4096:
                raise SnapshotError("history retirement stage exceeds journal bound")
            for journal_path in journals:
                raw = _stable_regular_bytes(journal_path, maximum=1024 * 1024)
                journal = json.loads(raw)
                if (not isinstance(journal, dict) or journal.get("contract") != RETIREMENT_CONTRACT
                        or journal.get("state") != "completed" or journal.get("dataset") != dataset):
                    continue
                digest = journal.get("plan_sha256")
                if not isinstance(digest, str) or not HASH.fullmatch(digest):
                    raise SnapshotError("history retirement plan identity is invalid")
                if journal_path.name not in {"history-retirement-" + digest + ".json",
                                             "prepared-history-retirement-" + digest + ".json"}:
                    raise SnapshotError("history retirement journal name differs")
                if "plan" in journal:
                    plan = journal["plan"]
                    removed = journal.get("removed")
                else:
                    name = journal.get("plan_ref")
                    if name != "retirement-plan-" + digest + ".json":
                        raise SnapshotError("history retirement plan reference differs")
                    plan = json.loads(_stable_regular_bytes(stage / name, maximum=64 * 1024**2))
                    removed = None
                if (not isinstance(plan, dict)
                        or identity_sha256({k: v for k, v in plan.items() if k != "identity_sha256"}) != digest
                        or not isinstance(plan.get("versions"), list)
                        or not isinstance(plan.get("items"), list)):
                    raise SnapshotError("history retirement plan identity differs")
                items = plan["items"]
                count = journal.get("removed_count") if removed is None else len(removed)
                if (type(count) is not int or count != len(items)
                        or journal.get("versions_removed") != len(plan["versions"])
                        or removed is not None and removed != [item.get("relative") for item in items]):
                    raise SnapshotError("history retirement was not completely journalled")
                removed_manifests = {item.get("relative") for item in items if item.get("kind") == "manifest"}
                for version in plan["versions"]:
                    key = (dataset, version.get("snapshot_id"), version.get("sha256"))
                    waiver = wanted.get(key)
                    if waiver is None:
                        continue
                    refs = {(ref.get("relative"), ref.get("sha256")) for ref in version.get("refs", [])}
                    if (version.get("relative") != waiver["manifest_relative"]
                            or version.get("relative") not in removed_manifests
                            or any((name, unavailable[name]["sha256"]) not in refs
                                   for name in waiver["missing_objects"])):
                        raise SnapshotError("retired historical release identity differs")
                    proof = {"contract": RETIREMENT_CONTRACT, "plan_sha256": digest,
                             "journal_sha256": hashlib.sha256(raw).hexdigest(),
                             "historical_recovery_claimed": False}
                    if key in proofs and proofs[key] != proof:
                        raise SnapshotError("multiple retirement proofs name the same history")
                    proofs[key] = proof
    return proofs


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


def apply_disposition(catalog_body: dict, path: Path | None, *, retirement_journal_root=None,
                      retirement_root_factory=None) -> dict:
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
    if (not isinstance(value.get("releases"), list)
            or not isinstance(value.get("unavailable_objects"), list)
            or any(not isinstance(r, dict) or not isinstance(r.get("relative"), str)
                   or not isinstance(r.get("sha256"), str) or not HASH.fullmatch(r["sha256"])
                   or type(r.get("bytes")) is not int or r["bytes"] < 0
                   for r in value["unavailable_objects"])):
        raise SnapshotError("history disposition object descriptors are invalid")
    unavailable = {r["relative"]: r for r in value["unavailable_objects"]}
    if len(unavailable) != len(value["unavailable_objects"]) or not unavailable:
        raise SnapshotError("history disposition needs unique unavailable objects")
    abandoned = set()
    absent = []
    for waiver in value["releases"]:
        key = (waiver["dataset"], waiver["snapshot_id"])
        if key in abandoned:
            raise SnapshotError("duplicate historical release disposition")
        abandoned.add(key)
        if key not in known:
            absent.append(waiver)
    abandoned.clear()
    retired_proofs = {}
    if absent:
        guard = None
        if retirement_journal_root is None and retirement_root_factory is not None:
            retirement_journal_root, guard = retirement_root_factory()
        if retirement_journal_root is not None:
            if guard is not None:
                guard()
            retired_proofs = _retirement_proofs(Path(retirement_journal_root), absent, unavailable)
            if guard is not None:
                guard()
    retired_objects = set()
    for waiver in value["releases"]:
        key = (waiver["dataset"], waiver["snapshot_id"])
        release = known.get(key)
        missing = waiver["missing_objects"]
        if (not missing or len(set(missing)) != len(missing)
                or any(name not in unavailable for name in missing)):
            raise SnapshotError("abandoned history unavailable-object identity changed")
        if release is None:
            proof = retired_proofs.get((*key, waiver["manifest_sha256"]))
            if proof is None:
                raise SnapshotError("abandoned history changed or is now a current head")
            abandoned.add(key)
            retired_objects.update(missing)
            body["abandoned_releases"].append({
                **waiver, "disposition_identity_sha256": claimed,
                "decision": value["decision"], "historical_recovery_claimed": False,
                "manifest_observed_in_current_catalog": False, "retirement_proof": proof,
            })
            continue
        if key in abandoned or release["current_head"]:
            raise SnapshotError("abandoned history changed or is now a current head")
        if any(waiver[k] != release[k] for k in (
            "manifest_relative", "manifest_sha256", "source_fingerprint_sha256",
        )):
            raise SnapshotError("abandoned history manifest identity changed")
        required = set(release["required_file_keys"])
        if (not set(release["missing_objects"]) <= set(missing)
                or any(name + "@" + unavailable[name]["sha256"] not in required for name in missing)):
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
    body["retired_historical_missing_objects"] = [
        {**unavailable[name], "current_presence": "not_rechecked_after_manifest_retirement"}
        for name in sorted(retired_objects)
    ]
    body["history_disposition_identity_sha256"] = claimed
    body["scope"] += "; explicitly abandoned missing history excluded from recovery requirements; existing bytes retained"
    return body
