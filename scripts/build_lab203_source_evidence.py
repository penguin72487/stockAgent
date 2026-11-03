#!/usr/bin/env python3
"""Build bounded source-side evidence for the paired lab203 storage owner.

This only reads metadata and fixed controls. A hash match is a recovery
candidate, never an acknowledgement for another batch or permission to prune.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.backup_delivery_receipt import validate_ack, validate_dispatch
from scripts.verify_backup_delivery import CONTRACT, identity, regular, verify
from stockagent.data_sync.offhost_backup import private_json
from stockagent.training_rescue_backup import json_bytes, plan_members

OPERATIONS = ROOT / "artifacts/operations/storage-single-authority-20261025"
RECEIPTS = Path("/srv/stockagent-backup-receipts-lab203")
TRANSPORT = Path("/srv/stockagent-backup-ingress-lab203")
RESCUE = Path("/srv/stockagent-packed/.local-state/training-rescue")
LEDGER = Path("/var/lib/stockagent/backup-stream/ledger.json")
DIAGNOSTIC = RECEIPTS / "lab203-backpressure-diagnostic-v1-20261029T040046Z-e9dc6d3f.json"
NEWS = RECEIPTS / "lab203-news-backpressure-evidence-v1-20261029T043101Z-c060788f.json"
REQUEST = RECEIPTS / ("private-rescue-verifier-source-pin-request-"
                      "965a8cd00fb77f3bc7ad4f0a28a89afd06bb2540a737a4643cbd3745facff2f0-20261028.json")
SUPPLEMENT = OPERATIONS / "tw-original-evidence-supplement-plan-20261028.private.json"
PLAN = OPERATIONS / "private-rescue-bootstrap-v7-nas-plan-20261027.private.json"
TOOL_RECEIPT = OPERATIONS / "tw-original-runtime-tool-publication-20261028.private.json"
OUTPUT = OPERATIONS / "lab203-source-evidence-20261029-v1.json"
NEW_WAVES = {
    "1ad520951a257c62ecc54af98c5bf3440383b980605edb514e89ce0dba4afca7",
    "74bfaba1bea820cd786a7b2a7f8bb97936cf14a1b0314be44b8ac0dfd915a020",
}
PAIRING = ("producer_device_id", "receiver_device_id", "repository_id")


def load_fixed(path: Path) -> tuple[dict, bytes]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"fixed control unavailable: {path.name}")
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        raw = stream.read()
        after = os.fstat(stream.fileno())
    stamp = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if stamp(before) != stamp(after) or len(raw) != before.st_size:
        raise ValueError(f"fixed control changed: {path.name}")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("control is not an object")
    return value, raw


def fixed_identity(value: dict) -> None:
    if value.get("identity_sha256") != identity({k: v for k, v in value.items() if k != "identity_sha256"}):
        raise ValueError("control identity differs")


def paired(value: dict, expected: dict, prefix: str = "") -> None:
    for field in PAIRING:
        actual = value.get(prefix + field, value.get(field))
        if actual != expected[field]:
            raise ValueError("control belongs to another producer, receiver or repository")


def descriptor(raw: bytes) -> dict:
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def pending_observation(ledger: dict, diagnostic: dict, pairing: dict) -> dict:
    pending = {}
    for key, delivery in ledger["deliveries"].items():
        dispatch = delivery["dispatch"]
        validate_dispatch(dispatch)
        paired(dispatch, pairing)
        if dispatch["envelope_identity_sha256"] != key:
            raise ValueError("ledger key differs from dispatch")
        if delivery.get("acceptance"):
            validate_ack(delivery["acceptance"], dispatch)
        else:
            pending[key] = {k: dispatch[k] for k in (
                "envelope_identity_sha256", "identity_sha256", "delivery_kind", "complete_files", "complete_bytes")}
    receiver = {r["envelope_identity_sha256"] for r in diagnostic["dispatches"]["records"]}
    # Counts, totals and the subsequent two publications constrain, but do not
    # prove, the old identity set: no immutable 11:20 ledger was captured.
    old = set(pending) - NEW_WAVES
    historical_possible = (NEW_WAVES <= set(pending) and len(old) == 83
                           and sum(pending[k]["complete_bytes"] for k in old) == 34657806071)
    old_rows = [pending[k] for k in sorted(old)] if historical_possible else []
    return {"current_pending": [pending[k] for k in sorted(pending)],
            "current_pending_bytes": sum(r["complete_bytes"] for r in pending.values()),
            "receiver_diagnostic_identity_sha256": diagnostic["identity_sha256"],
            "receiver_observation_utc": diagnostic["observed_at_utc"],
            "receiver_indexed_count": len(receiver),
            "historical_1120": {"status": "candidate_not_original_snapshot" if historical_possible else "not_reconstructable",
                "original_count": 83, "original_bytes": 34657806071,
                "basis": "current pending less two journal-observed later waves; counts and bytes match, identity swaps remain possible",
                "rows": old_rows, "receiver_intersection_count": len(old & receiver) if historical_possible else None,
                "candidate_only": sorted(old - receiver) if historical_possible else [],
                "receiver_only": sorted(receiver - old) if historical_possible else []}}


def checked_header(dispatch: dict, *, raw: bytes, ready: bytes | None) -> dict:
    env = json.loads(raw)
    fixed_identity(env)
    if (env.get("contract") != CONTRACT
            or env["identity_sha256"] != dispatch["envelope_identity_sha256"]
            or hashlib.sha256(raw).hexdigest() != dispatch["envelope_file_sha256"]
            or len(env["files"]) + 2 != dispatch["complete_files"]
            or sum(r["bytes"] for r in env["files"]) + len(raw) + 65 != dispatch["complete_bytes"]
            or (ready is not None and ready != (env["identity_sha256"] + "\n").encode())):
        raise ValueError("closed envelope differs from the paired dispatch")
    indexed = {r["relative"]: (r["sha256"], r["bytes"]) for r in env["files"]}
    if len(indexed) != len(env["files"]):
        raise ValueError("duplicate envelope member")
    return indexed


def candidate_donors(ledger: dict, pairing: dict, transport: Path) -> dict:
    donors = {}
    target_deliveries = []
    acked_count = 0
    for key, delivery in ledger["deliveries"].items():
        dispatch = delivery["dispatch"]
        validate_dispatch(dispatch)
        paired(dispatch, pairing)
        if dispatch["envelope_identity_sha256"] != key:
            raise ValueError("ledger key differs from dispatch")
        if delivery.get("acceptance"):
            validate_ack(delivery["acceptance"], dispatch)
            acked_count += 1
            if dispatch["delivery_kind"] == "incremental_cold_objects":
                for row in delivery["files"]:
                    donors.setdefault((row["sha256"], row["bytes"]), []).append((key, row["relative"]))
        elif dispatch["delivery_kind"] in {"private_immutable_cold_backup", "private_personal_news_backup"}:
            target_deliveries.append((key, delivery))
    matches, donor_proofs, targets, errors = [], {}, [], []
    for key, delivery in target_deliveries:
        dispatch = delivery["dispatch"]
        batch = transport / dispatch["batch_relative"]
        try:
            env_raw = regular(batch, "backup-envelope.json").read_bytes()
            ready = regular(batch, "READY").read_bytes()
            header = checked_header(dispatch, raw=env_raw, ready=ready)
            if len(header) != len(delivery["files"]):
                raise ValueError("ledger and target envelope have different member counts")
            def target_name(row):
                name = row["relative"]
                if dispatch["delivery_kind"] == "private_personal_news_backup":
                    if not name.startswith("private/"):
                        raise ValueError("news ledger member has an unexpected namespace")
                    name = name.removeprefix("private/")
                return name
            if len({target_name(row) for row in delivery["files"]}) != len(header):
                raise ValueError("duplicate ledger member")
            for row in delivery["files"]:
                if header.get(target_name(row)) != (row["sha256"], row["bytes"]):
                    raise ValueError("ledger member differs from target envelope")
            rows = []
            for row in delivery["files"]:
                for donor_key, donor_relative in donors.get((row["sha256"], row["bytes"]), []):
                    donor = ledger["deliveries"][donor_key]
                    if donor_key not in donor_proofs:
                        frozen = donor.get("cache_retirement", {}).get("envelope")
                        if frozen:
                            raw = json_bytes(frozen)
                            donor_ready = None
                        else:
                            donor_dir = transport / donor["dispatch"]["batch_relative"]
                            raw = regular(donor_dir, "backup-envelope.json").read_bytes()
                            donor_ready = regular(donor_dir, "READY").read_bytes()
                        donor_header = checked_header(donor["dispatch"], raw=raw, ready=donor_ready)
                        donor_proofs[donor_key] = {"header": donor_header,
                            "envelope_identity_sha256": donor_key,
                            "dispatch_identity_sha256": donor["dispatch"]["identity_sha256"],
                            "NAS_ack_identity_sha256": donor["acceptance"]["identity_sha256"],
                            "NAS_snapshot_id": donor["acceptance"]["snapshot_id"]}
                    if donor_proofs[donor_key]["header"].get("cold/" + donor_relative) != (row["sha256"], row["bytes"]):
                        raise ValueError("donor ledger member differs from retained envelope")
                    rows.append({"target_relative": target_name(row), "sha256": row["sha256"],
                        "bytes": row["bytes"], "donor_envelope_identity_sha256": donor_key,
                        "donor_relative": "cold/" + donor_relative})
                    break
            matches.extend({"target_envelope_identity_sha256": key, **r} for r in rows)
            targets.append({"envelope_identity_sha256": key, "delivery_kind": dispatch["delivery_kind"],
                            "target_member_count": len(delivery["files"]), "candidate_member_count": len(rows),
                            "candidate_member_bytes": sum(r["bytes"] for r in rows)})
        except (OSError, ValueError, KeyError, TypeError) as error:
            errors.append({"envelope_identity_sha256": key, "error_type": type(error).__name__})
    return {"scope": "metadata_candidates_only_revalidate_missing_target_and_independent_restore",
            "accepted_ledger_deliveries_checked": acked_count, "targets": targets, "errors": errors,
            "donors": [{k: v for k, v in p.items() if k != "header"} for _, p in sorted(donor_proofs.items())],
            "candidates": matches, "target_NAS_acceptance_or_payload_verified": False}


def rescue_binding(pairing: dict, ledger: dict) -> dict:
    request, _ = load_fixed(REQUEST)
    fixed_identity(request)
    paired(request, pairing)
    supplement, supplement_raw = load_fixed(SUPPLEMENT)
    members = plan_members(supplement, supplement_raw)
    paired(supplement["pairing"], pairing)
    plan, plan_raw = load_fixed(PLAN)
    plan_members(plan, plan_raw)
    if (supplement["identity_sha256"] != request["supplement_catalog_identity_sha256"]
            or supplement["original_plan_identity_sha256"] != plan["identity_sha256"]
            or supplement["original_plan_file"] != descriptor(plan_raw)):
        raise ValueError("supplement does not bind its fixed original plan")
    for row in members.values():
        source = regular(RESCUE, row["relative"])
        raw = source.read_bytes()
        if descriptor(raw) != {k: row[k] for k in ("sha256", "bytes")}:
            raise ValueError("supplement control differs from source plan")
        fixed_identity(json.loads(raw))
    index = supplement["response_index"]
    index_member = members.get(index["relative"])
    if not index_member or any(index_member.get(k) != index[k] for k in ("relative", "identity_sha256", "sha256", "bytes")):
        raise ValueError("response index is not in the fixed supplement")
    index_raw = regular(RESCUE, index["relative"]).read_bytes()
    response = json.loads(index_raw)
    fixed_identity(response)
    if (descriptor(index_raw) != {k: index[k] for k in ("sha256", "bytes")}
            or response["identity_sha256"] != index["identity_sha256"]):
        raise ValueError("response index differs")
    contract = response["qualification_contract"]
    for key in ("qualification_contract", "runtime_source_provenance", "prepared_reference_bindings"):
        item = response[key]
        if members.get(item["relative"]) != item:
            raise ValueError("response control is not in the fixed supplement")
    acked = ledger["deliveries"][request["supplement_envelope_identity_sha256"]]
    validate_ack(acked["acceptance"], acked["dispatch"])
    paired(acked["dispatch"], pairing)
    if (acked["acceptance"]["identity_sha256"] != request["paired_NAS_file_restore_ACK_identity_sha256"]
            or acked["dispatch"].get("catalog_identity_sha256") != supplement["identity_sha256"]):
        raise ValueError("supplement is not paired with the stated file restore acknowledgement")
    tool, _ = load_fixed(TOOL_RECEIPT)
    tool_dir = TRANSPORT / tool["receiver_relative"]
    if tool["envelope_identity_sha256"] != request["observed_tool_envelope_identity_sha256"]:
        raise ValueError("original source tool envelope differs")
    verify(tool_dir, tool["envelope_identity_sha256"])
    manifest, manifest_raw = load_fixed(tool_dir / "handoff-manifest.json")
    fixed_identity(manifest)
    if (manifest["identity_sha256"] != request["observed_handoff_manifest_identity_sha256"]
            or hashlib.sha256(manifest_raw).hexdigest() != tool["handoff_manifest_sha256"]):
        raise ValueError("original handoff manifest differs")
    scripts = {}
    for name, observed in request["observed_fixed_code_sha256"].items():
        source_raw = regular(ROOT, name).read_bytes()
        package_raw = regular(tool_dir, name).read_bytes()
        sha = hashlib.sha256(source_raw).hexdigest()
        if sha != observed or hashlib.sha256(package_raw).hexdigest() != sha:
            raise ValueError("source and fixed package verifier code differ")
        scripts[name] = sha
    return {"request_identity_sha256": request["identity_sha256"],
            "original_base_plan_identity_sha256": request["original_plan_identity_sha256"],
            "v7_plan_identity_sha256": plan["identity_sha256"], "v7_plan_file": descriptor(plan_raw),
            "v7_plan_member_count": plan["files"], "v7_plan_member_bytes": plan["source_bytes"],
            "v7_runs": [{"run_id": r["run_id"], "spec_sha256": r["spec_sha256"], "latest": r["latest"]} for r in plan["runs"]],
            "supplement_identity_sha256": supplement["identity_sha256"], "supplement_file": descriptor(supplement_raw),
            "supplement_envelope_identity_sha256": request["supplement_envelope_identity_sha256"],
            "supplement_NAS_file_ack_identity_sha256": request["paired_NAS_file_restore_ACK_identity_sha256"],
            "response_index": {k: index[k] for k in ("relative", "identity_sha256", "sha256", "bytes")},
            "runtime_contract": {k: contract[k] for k in ("relative", "identity_sha256", "sha256", "bytes")},
            "tool_envelope_identity_sha256": tool["envelope_identity_sha256"],
            "handoff_manifest_identity_sha256": manifest["identity_sha256"],
            "handoff_manifest_sha256": tool["handoff_manifest_sha256"], "source_and_tool_code_sha256": scripts,
            "runtime_source_provenance": response["runtime_source_provenance"],
            "prepared_reference_bindings": response["prepared_reference_bindings"],
            "scope": "fixed_v7_inventory_and_existing_supplement_not_a_new_capsule_inventory",
            "rng_resume_semantics_independently_verified": False,
            "strict_runtime_or_semantic_resume_verified": False}


def build() -> dict:
    config, _ = load_fixed(ROOT / "configs/data_sync/backup_stream.json")
    pairing = {k: config[k] for k in PAIRING}
    ledger, ledger_raw = load_fixed(LEDGER)
    diagnostic, _ = load_fixed(DIAGNOSTIC)
    fixed_identity(diagnostic)
    paired(diagnostic, pairing, prefix="paired_")
    news, _ = load_fixed(NEWS)
    fixed_identity(news)
    paired(news, pairing)
    observed = datetime.now(timezone.utc).isoformat()
    body = {"contract": "stockagent.lab203.source_storage_evidence.v1", "observed_at_utc": observed,
            "pairing": pairing, "ledger_file": descriptor(ledger_raw),
            "pending": pending_observation(ledger, diagnostic, pairing),
            "rescue": rescue_binding(pairing, ledger),
            "candidate_donors": candidate_donors(ledger, pairing, TRANSPORT),
            "historical_missing": {"diagnostic_identity_sha256": diagnostic["identity_sha256"],
                "releases": diagnostic["historical_tw_public"]["releases"],
                "objects": diagnostic["historical_tw_public"]["objects"],
                "full_live_Restic_search_completed": False, "global_irrecoverability_claimed": False},
            "news_evidence_identity_sha256": news["identity_sha256"],
            "is_batch_or_recovery_ACK": False, "source_or_NAS_data_deleted": False,
            "limits_or_reserves_changed": False, "private_payload_or_credentials_included": False}
    return {**body, "identity_sha256": identity(body)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve the existing frozen evidence")
    value = build()
    private_json(args.output, value)
    print(json.dumps({"path": str(args.output), "identity_sha256": value["identity_sha256"],
        "bytes": args.output.stat().st_size, "current_pending": len(value["pending"]["current_pending"]),
        "candidate_count": len(value["candidate_donors"]["candidates"]),
        "candidate_errors": value["candidate_donors"]["errors"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
