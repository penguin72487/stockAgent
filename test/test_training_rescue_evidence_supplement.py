"""Evidence-only encrypted waves cannot masquerade as a full rescue restore."""
import copy
import hashlib

import pytest

from stockagent.training_rescue_backup import EVIDENCE_PLAN, deliveries, json_bytes, plan_members
from stockagent.training_rescue_worker import digest


def evidence_plan():
    row = {"relative": "evidence/original-runtime/qualification.json", "bytes": 17,
           "sha256": "b" * 64, "role": "private_rescue_control"}
    return {"contract": EVIDENCE_PLAN, "route": "existing_encrypted_Restic_relay",
        "delivery_kind": "private_vast_recovery_backup", "encryption_required": True,
        "plaintext_lake_export_allowed": False, "scope": "original_runtime_evidence_only",
        "runs": [], "original_plan_identity_sha256": "a" * 64,
        "original_plan_file": {"sha256": "c" * 64, "bytes": 12345},
        "semantic_resume_verified": False, "original_payload_retransmitted": False,
        "NAS_backup_verified": False, "members": [row], "files": 1, "source_bytes": 17}


def closed(value):
    value = copy.deepcopy(value)
    value.pop("identity_sha256", None)
    value["identity_sha256"] = digest(value)
    return value, json_bytes(value)


def test_evidence_wave_preserves_original_plan_binding_without_retransmitting_original_payload():
    plan, raw = closed(evidence_plan())
    assert len(plan_members(plan, raw)) == 1
    waves = deliveries(plan, raw, maximum_batch_bytes=8 * 1024**3, maximum_batch_files=10000)
    assert len(waves) == 1 and len(waves[0]["members"]) == 1
    assert waves[0]["envelope"]["all_history_verified"] is False
    assert waves[0]["envelope"]["complete_release_in_this_delivery"] is False
    control = waves[0]["control_bytes"][f"private-rescue/{plan['identity_sha256']}/recovery-plan.private.json"]
    assert hashlib.sha256(control).digest() == hashlib.sha256(raw).digest()
    assert not any(row["relative"].startswith("objects/") for row in waves[0]["members"])


@pytest.mark.parametrize("damage", ["checkpoint", "bootstrap", "runs", "resume", "nas", "payload", "parent", "scope"])
def test_evidence_only_scope_rejects_payload_or_completion_claims(damage):
    plan = evidence_plan()
    if damage == "checkpoint":
        plan["members"][0].update(relative="objects/bb/" + "b" * 64, role="private_rescue_object")
    elif damage == "bootstrap":
        plan["members"][0]["relative"] = "bootstrap/original/run.json"
    elif damage == "runs":
        plan["runs"] = [{"run_id": "original"}]
    elif damage == "resume":
        plan["semantic_resume_verified"] = True
    elif damage == "nas":
        plan["NAS_backup_verified"] = True
    elif damage == "payload":
        plan["original_payload_retransmitted"] = True
    elif damage == "parent":
        plan.pop("original_plan_file")
    else:
        plan["scope"] = "full_restore"
    plan, raw = closed(plan)
    with pytest.raises(ValueError, match="evidence supplement"):
        plan_members(plan, raw)
