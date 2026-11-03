#!/usr/bin/env python3
"""Bind the original TW checkpoint, config, source, references and runtime.

Only fixed metadata is exported. Inventory membership is not a new payload
readback or proof of RNG, runtime qualification, or optimizer resume.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.build_lab203_source_evidence import (OPERATIONS, PLAN, RESCUE, descriptor,
                                                 fixed_identity, load_fixed)
from scripts.verify_backup_delivery import identity, regular
from stockagent.data_sync.offhost_backup import private_json
from stockagent.training_rescue_backup import plan_members

RUN = "tw-v6-embedding-d128"
BOOTSTRAP = "6d79ebe7ee8124e8f1beec599ea3b15749f75e98ac5cfd392b978c7c6bbbce5a"
SOURCE_EVIDENCE = OPERATIONS / "lab203-source-evidence-20261029-v1.json"
OUTPUT = OPERATIONS / "lab203-training-capsule-binding-20261029-v1.json"


def member_descriptor(members: dict, relative: str, raw: bytes) -> dict:
    row = members.get(relative)
    if not row or descriptor(raw) != {k: row[k] for k in ("sha256", "bytes")}:
        raise ValueError("fixed control differs from the v7 inventory")
    return {"relative": relative, "sha256": row["sha256"], "bytes": row["bytes"]}


def object_descriptor(members: dict, row: dict) -> dict:
    sha = row["sha256"]
    name = "objects/" + sha[:2] + "/" + sha
    stored = members.get(name)
    if (not stored or stored.get("role") != "private_rescue_object"
            or stored.get("sha256") != sha or stored.get("bytes") != row["bytes"]):
        raise ValueError("fixed payload descriptor is absent from the v7 inventory")
    return {"relative": name, "sha256": sha, "bytes": row["bytes"]}


def build() -> dict:
    evidence, _ = load_fixed(SOURCE_EVIDENCE)
    fixed_identity(evidence)
    plan, raw = load_fixed(PLAN)
    members = plan_members(plan, raw)
    if (plan["identity_sha256"] != evidence["rescue"]["v7_plan_identity_sha256"]
            or descriptor(raw) != evidence["rescue"]["v7_plan_file"]):
        raise ValueError("v7 plan differs from the source-pinned response")
    run = next(r for r in plan["runs"] if r["run_id"] == RUN)
    commit_id = run["latest"]["commit_id"]
    commit_path = f"commits/{RUN}/{commit_id}.json"
    bootstrap_path = f"bootstrap/{RUN}/{BOOTSTRAP}.json"
    commit_raw = regular(RESCUE, commit_path).read_bytes()
    bootstrap_raw = regular(RESCUE, bootstrap_path).read_bytes()
    commit, bootstrap = json.loads(commit_raw), json.loads(bootstrap_raw)
    commit_desc = member_descriptor(members, commit_path, commit_raw)
    bootstrap_desc = member_descriptor(members, bootstrap_path, bootstrap_raw)
    if (identity(commit) != commit_id or identity(bootstrap) != BOOTSTRAP
            or commit["run_id"] != RUN or bootstrap["run_id"] != RUN
            or commit["spec_sha256"] != run["spec_sha256"]
            or bootstrap["spec_sha256"] != run["spec_sha256"]
            or commit["inventory"]["context_sha256"] != run["latest"]["context_sha256"]
            or commit["inventory"]["checkpoints"] != run["latest"]["checkpoints"]):
        raise ValueError("original run, checkpoint context or bootstrap binding differs")
    committed = [{"run_relative": row["relative"], **object_descriptor(members, row)}
                 for row in commit["inventory"]["files"]]
    checkpoint_name, checkpoint_semantics = next(iter(commit["inventory"]["checkpoints"].items()))
    checkpoint = next(row for row in committed if row["run_relative"] == checkpoint_name)
    inputs = [{"role": row["role"], **object_descriptor(members, row)} for row in bootstrap["files"]]
    runtime = object_descriptor(members, bootstrap["runtime_archive"])
    config = next(row for row in inputs if row["role"] == "training_config")
    if not any(row["sha256"] == config["sha256"] and row["bytes"] == config["bytes"] for row in committed):
        raise ValueError("bootstrap config differs from the original committed config")
    body = {"contract": "stockagent.lab203.original_tw_capsule_binding.v1",
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "pairing": evidence["pairing"], "source_evidence_identity_sha256": evidence["identity_sha256"],
            "v7_plan_identity_sha256": plan["identity_sha256"], "v7_plan_file": descriptor(raw),
            "v7_inventory_members": plan["files"], "v7_inventory_bytes": plan["source_bytes"],
            "run_id": RUN, "spec_sha256": run["spec_sha256"], "commit_id": commit_id,
            "commit_control": commit_desc, "bootstrap_identity_sha256": BOOTSTRAP,
            "bootstrap_control": bootstrap_desc, "checkpoint": checkpoint,
            "checkpoint_semantics": {k: checkpoint_semantics[k] for k in
                ("epoch", "global_batch", "world_size", "full_optimizer_state", "semantic_manifest_sha256")},
            "committed_file_objects": committed, "bootstrap_file_objects": inputs, "runtime_archive": runtime,
            "source_verifier_binding": evidence["rescue"]["source_and_tool_code_sha256"],
            "original_runtime_contract": evidence["rescue"]["runtime_contract"],
            "original_response_index": evidence["rescue"]["response_index"],
            "scope": "source_metadata_binding_for_existing_fixed_v7_plan_only",
            "object_payload_rehashed_now": False, "new_NAS_or_recovery_ACK": False,
            "rng_state_independently_verified": False, "strict_runtime_or_semantic_resume_verified": False,
            "original_runtime_metadata_mismatches_preserved": True,
            "source_or_NAS_data_deleted": False, "private_payload_or_credentials_included": False}
    return {**body, "identity_sha256": identity(body)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve the existing frozen binding")
    value = build()
    private_json(args.output, value)
    print(json.dumps({"path": str(args.output), "identity_sha256": value["identity_sha256"],
                      "bytes": args.output.stat().st_size}, ensure_ascii=False))


if __name__ == "__main__":
    main()
