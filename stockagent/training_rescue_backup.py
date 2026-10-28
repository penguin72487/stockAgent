"""Append-only private rescue backups over fixed, paired NAS delivery proofs.

This module plans bytes only. It neither contacts NAS nor turns historical file
ACKs into a fresh restore, source retirement, or optimizer-resume acceptance.
"""
from __future__ import annotations

import hashlib
import json

from scripts.backup_delivery_receipt import validate_ack
from scripts.verify_backup_delivery import CONTRACT, identity
from stockagent.training_rescue_worker import HASH, digest, relative

DELTA_CONTRACT = "stockagent.private_rescue_incremental_backup.v1"
EVIDENCE_PLAN = "stockagent_private_rescue_evidence_supplement_plan_v1"
PLAN_CONTRACTS = {"stockagent_private_rescue_NAS_closed_plan_v1",
                  "stockagent_private_rescue_NAS_closed_plan_v2", EVIDENCE_PLAN}
CONTROL_NAMES = ("recovery-plan.private.json", "base-recovery-plan.private.json",
                 "incremental-backup.private.json")
PAIRED_KEYS = ("producer_device_id", "receiver_device_id", "repository_id")


def json_bytes(value):
    """The original private_json producer's exact on-disk JSON encoding."""
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()


def incremental_bytes(value):
    """New controls stay byte-identical across original-ledger JSON roundtrips."""
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def descriptor(raw):
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def plan_members(plan, raw):
    if (not isinstance(plan, dict) or plan.get("contract") not in PLAN_CONTRACTS
            or json.loads(raw) != plan or len(raw) > 1024**2
            or plan.get("identity_sha256") != digest({k: v for k, v in plan.items()
                                                       if k != "identity_sha256"})
            or plan.get("route") != "existing_encrypted_Restic_relay"
            or plan.get("delivery_kind") != "private_vast_recovery_backup"
            or plan.get("encryption_required") is not True
            or plan.get("plaintext_lake_export_allowed") is not False):
        raise ValueError("private plan identity or encrypted route differs")
    members = plan.get("members")
    if not isinstance(members, list) or not 0 < len(members) <= 10000:
        raise ValueError("private plan needs a bounded exact member census")
    evidence_only = plan["contract"] == EVIDENCE_PLAN
    if evidence_only:
        parent = plan.get("original_plan_file", {})
        if (plan.get("scope") != "original_runtime_evidence_only" or plan.get("runs") != []
                or not isinstance(plan.get("original_plan_identity_sha256"), str)
                or not HASH.fullmatch(plan["original_plan_identity_sha256"])
                or not isinstance(parent.get("sha256"), str) or not HASH.fullmatch(parent["sha256"])
                or type(parent.get("bytes")) is not int or not 0 < parent["bytes"] <= 1024**2
                or plan.get("semantic_resume_verified") is not False
                or plan.get("original_payload_retransmitted") is not False
                or plan.get("NAS_backup_verified") is not False):
            raise ValueError("evidence supplement needs its exact original plan and pending qualification")
    result = {}
    for row in members:
        name = relative(row["relative"])
        if (any(c in name for c in "\r\n\0:") or name in result
                or type(row.get("bytes")) is not int or not 0 <= row["bytes"] < 8 * 1024**3
                or not isinstance(row.get("sha256"), str) or not HASH.fullmatch(row["sha256"])):
            raise ValueError("invalid private member identity or size")
        parts = name.split('/')
        if evidence_only and (row.get('role') != 'private_rescue_control' or parts[0] != 'evidence'):
            raise ValueError("evidence supplement cannot republish runtime/checkpoint payloads")
        if row.get('role') == 'private_rescue_object':
            if parts != ['objects', row['sha256'][:2], row['sha256']]:
                raise ValueError("private CAS namespace differs from its immutable identity")
        elif (row.get('role') != 'private_rescue_control' or len(parts) < 3
              or parts[0] not in {'commits', 'bootstrap', 'evidence', 'source-verification', 'input-recipes'}
              or not name.endswith('.json')):
            raise ValueError("unrecognized private backup namespace")
        result[name] = row
    if (type(plan.get("files")) is not int or plan["files"] != len(result)
            or type(plan.get("source_bytes")) is not int
            or plan["source_bytes"] != sum(row["bytes"] for row in members)
            or plan["source_bytes"] > 32 * 1024**3):
        raise ValueError("private member census totals differ")
    return result


def deliveries(plan, raw, *, maximum_batch_bytes, maximum_batch_files,
               incremental=None, base_raw=None):
    """Compile the existing publisher's deterministic full or append-only waves."""
    members = plan_members(plan, raw)
    if (type(maximum_batch_bytes) is not int or not 2 * 1024**2 < maximum_batch_bytes <= 8 * 1024**3
            or type(maximum_batch_files) is not int or not 5 <= maximum_batch_files <= 100000):
        raise ValueError("private wave needs bounded byte and file admission")
    prefix = "private-rescue/" + plan["identity_sha256"] + "/"
    controls = {CONTROL_NAMES[0]: raw}
    selected = list(members.values())
    if incremental is not None:
        if base_raw is None:
            raise ValueError("incremental publication needs its exact original base plan")
        base = json.loads(base_raw)
        validate_incremental(plan, raw, base, base_raw, incremental)
        selected = [members[row["relative"]] for row in incremental["new_members"]]
        controls.update({CONTROL_NAMES[1]: base_raw, CONTROL_NAMES[2]: incremental_bytes(incremental)})
    if sum(map(len, controls.values())) > 2 * 1024**2:
        raise ValueError("private wave controls exceed the original bounded metadata reserve")
    payload_files = maximum_batch_files - 2 - len(controls)
    if selected and payload_files < 1:
        raise ValueError("private controls consume the single-wave file admission")
    groups = []
    budget = maximum_batch_bytes - 2 * 1024**2
    for row in sorted(selected, key=lambda value: (-value["bytes"], value["relative"])):
        if row["bytes"] > budget:
            raise ValueError("private member exceeds single-wave admission")
        for group in groups:
            if (sum(value["bytes"] for value in group) + row["bytes"] <= budget
                    and len(group) < payload_files):
                group.append(row)
                break
        else:
            groups.append([row])
            if len(groups) > 64:
                raise ValueError("private backup exceeds its bounded restored-part collection")
    # A control-only increment still preserves its complete current bindings.
    if not groups:
        groups.append([])
    result = []
    for ordinal, group in enumerate(groups):
        files = [{"relative": prefix + row["relative"], "bytes": row["bytes"], "sha256": row["sha256"]}
                 for row in sorted(group, key=lambda value: value["relative"])]
        files.extend({"relative": prefix + name, "bytes": len(value),
                      "sha256": hashlib.sha256(value).hexdigest()} for name, value in controls.items())
        body = {"contract": CONTRACT, "source_plan_identity_sha256": plan["identity_sha256"],
            "files": files, "producer_node_id": "penguin", "selection": "private_vast_recovery_backup",
            "encryption_required": True, "part_ordinal": ordinal, "part_count": len(groups),
            "all_history_verified": False, "unpublished_sources_included": True,
            "complete_release_in_this_delivery": False}
        key = identity(body)
        result.append({"ordinal": ordinal, "envelope_identity_sha256": key,
            "batch_relative": "batch-" + key, "files": files,
            "envelope": {**body, "identity_sha256": key}, "members": group,
            "control_bytes": {prefix + name: value for name, value in controls.items()}})
    return result


def incremental_plan(plan, raw, base, base_raw, base_deliveries, *, paired_identity,
                     maximum_batch_bytes, maximum_batch_files):
    """Bind every reused member to all of its original paired file-restore ACKs."""
    current = plan_members(plan, raw)
    previous = plan_members(base, base_raw)
    if (plan["contract"] != "stockagent_private_rescue_NAS_closed_plan_v2"
            or base["contract"] != plan["contract"]
            or plan.get("source_root") != base.get("source_root")
            or plan["identity_sha256"] == base["identity_sha256"]
            or set(paired_identity) != set(PAIRED_KEYS)):
        raise ValueError("incremental backup needs distinct append-only plans of the same private authority")
    for name, old in previous.items():
        if name not in current or any(current[name][key] != old[key] for key in ("sha256", "bytes")):
            raise ValueError("incremental private history must retain every original base member unchanged")
    expected = deliveries(base, base_raw, maximum_batch_bytes=maximum_batch_bytes,
                          maximum_batch_files=maximum_batch_files)
    if set(base_deliveries) != {row["envelope_identity_sha256"] for row in expected}:
        raise ValueError("incremental backup needs the exact complete base delivery collection")
    proofs = []
    for row in expected:
        original = base_deliveries[row["envelope_identity_sha256"]]
        dispatch, ack = original["dispatch"], original["acceptance"]
        if ack is None:
            raise ValueError("base private delivery has no verified NAS file-restore ACK")
        validate_ack(ack, dispatch)
        envelope = json_bytes(row["envelope"])
        if (any(dispatch.get(key) != paired_identity[key] for key in PAIRED_KEYS)
                or dispatch.get("catalog_identity_sha256") != base["identity_sha256"]
                or dispatch.get("delivery_kind") != "private_vast_recovery_backup"
                or dispatch.get("envelope_identity_sha256") != row["envelope_identity_sha256"]
                or dispatch.get("envelope_file_sha256") != hashlib.sha256(envelope).hexdigest()
                or dispatch.get("complete_files") != len(row["files"]) + 2
                or dispatch.get("complete_bytes") != sum(item["bytes"] for item in row["files"]) + len(envelope) + 65):
            raise ValueError("base paired ACK differs from the exact compiled original envelope")
        proofs.append({"ordinal": row["ordinal"], "dispatch": dispatch, "acceptance": ack})
    projection = lambda row: {key: row[key] for key in ("relative", "bytes", "sha256")}
    new = [projection(current[name]) for name in sorted(set(current) - set(previous))]
    result = {"contract": DELTA_CONTRACT,
        "current_plan_identity_sha256": plan["identity_sha256"], "current_plan_file": descriptor(raw),
        "base_plan_identity_sha256": base["identity_sha256"], "base_plan_file": descriptor(base_raw),
        "paired_identity": dict(paired_identity), "base_deliveries": proofs,
        "maximum_batch_bytes": maximum_batch_bytes, "maximum_batch_files": maximum_batch_files,
        "reused_files": len(previous), "reused_bytes": base["source_bytes"],
        "reused_members_identity_sha256": digest([projection(previous[name]) for name in sorted(previous)]),
        "new_members": new, "new_files": len(new), "new_bytes": sum(row["bytes"] for row in new),
        "fresh_complete_restore_verified": False, "new_GPU_resume_verified": False,
        "source_files_deleted": 0}
    value = {**result, "identity_sha256": digest(result)}
    if len(incremental_bytes(value)) > 1024**2:
        raise ValueError("incremental private control exceeds its bounded restore admission")
    return value


def validate_incremental(plan, raw, base, base_raw, value):
    if not isinstance(value, dict) or value.get("contract") != DELTA_CONTRACT:
        raise ValueError("unknown incremental private backup contract")
    proofs = value.get("base_deliveries")
    if not isinstance(proofs, list) or not 1 <= len(proofs) <= 64:
        raise ValueError("incremental private base proof collection is unbounded")
    collection = {}
    for row in proofs:
        key = row["dispatch"]["envelope_identity_sha256"]
        if key in collection:
            raise ValueError("duplicate incremental private base proof")
        collection[key] = row
    expected = incremental_plan(plan, raw, base, base_raw, collection,
        paired_identity=value["paired_identity"], maximum_batch_bytes=value["maximum_batch_bytes"],
        maximum_batch_files=value["maximum_batch_files"])
    if expected != value:
        raise ValueError("incremental private manifest differs from its exact plans and paired proofs")
    return expected
