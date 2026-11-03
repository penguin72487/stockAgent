import hashlib
import json
from datetime import datetime, timezone

import pytest

from scripts.backup_delivery_receipt import ACK, DISPATCH, signed
from scripts.build_lab203_capsule_binding import member_descriptor, object_descriptor
from scripts.build_lab203_source_evidence import NEW_WAVES, candidate_donors, checked_header, pending_observation
from scripts.verify_backup_delivery import CONTRACT
from stockagent.training_rescue_backup import json_bytes


PAIRING = {"producer_device_id": "producer", "receiver_device_id": "receiver", "repository_id": "f" * 64}


def dispatch(envelope, size):
    return signed({"contract": DISPATCH, "batch_relative": "batch-" + envelope,
                   "envelope_identity_sha256": envelope, "envelope_file_sha256": "a" * 64,
                   "complete_files": 3, "complete_bytes": size,
                   "delivery_kind": "incremental_cold_objects", **PAIRING})


def test_old_pending_reconstruction_is_never_claimed_to_be_the_snapshot():
    old = [hashlib.sha256(str(i).encode()).hexdigest() for i in range(83)]
    deliveries = {key: {"dispatch": dispatch(key, 100), "files": []} for key in old}
    deliveries[old[-1]]["dispatch"] = dispatch(old[-1], 34657806071 - 82 * 100)
    for key in NEW_WAVES:
        deliveries[key] = {"dispatch": dispatch(key, 100), "files": []}
    diagnostic = {"identity_sha256": "d" * 64, "observed_at_utc": "2026-10-29T03:20:00Z",
                  "dispatches": {"records": [{"envelope_identity_sha256": k} for k in old[:80]]}}
    value = pending_observation({"deliveries": deliveries}, diagnostic, PAIRING)
    history = value["historical_1120"]
    assert history["status"] == "candidate_not_original_snapshot"
    assert len(history["rows"]) == 83 and history["receiver_intersection_count"] == 80
    deliveries[old[-1]]["dispatch"] = dispatch(old[-1], 100)
    history = pending_observation({"deliveries": deliveries}, diagnostic, PAIRING)["historical_1120"]
    assert history["status"] == "not_reconstructable" and history["rows"] == []


def test_bad_dispatch_is_not_a_candidate():
    key = "b" * 64
    item = {"dispatch": dispatch(key, 100), "files": []}
    item["dispatch"]["complete_bytes"] = 101
    diagnostic = {"identity_sha256": "d" * 64, "observed_at_utc": "2026-10-29T03:20:00Z",
                  "dispatches": {"records": []}}
    with pytest.raises(ValueError, match="identity"):
        pending_observation({"deliveries": {key: item}}, diagnostic, PAIRING)


def test_header_requires_exact_raw_envelope_and_ready():
    env = signed({"contract": CONTRACT, "files": [{"relative": "cold/x", "sha256": "1" * 64, "bytes": 12}]})
    raw = json_bytes(env)
    d = dispatch(env["identity_sha256"], len(raw) + 65 + 12)
    d["envelope_file_sha256"] = hashlib.sha256(raw).hexdigest()
    ready = (env["identity_sha256"] + "\n").encode()
    assert checked_header(d, raw=raw, ready=ready) == {"cold/x": ("1" * 64, 12)}
    with pytest.raises(ValueError, match="dispatch"):
        checked_header(d, raw=raw, ready=b"0" * 64 + b"\n")
    changed = json.loads(raw)
    changed["files"][0]["bytes"] = 13
    with pytest.raises(ValueError, match="identity"):
        checked_header(d, raw=json_bytes(changed), ready=ready)


def test_donor_candidates_require_paired_ack_and_match_original_envelopes(tmp_path):
    digest = "c" * 64

    def delivery(kind, envelope_path, ledger_path, *, accepted=False):
        env = signed({"contract": CONTRACT,
                      "files": [{"relative": envelope_path, "sha256": digest, "bytes": 5}]})
        raw = json_bytes(env)
        envelope = env["identity_sha256"]
        d = signed({"contract": DISPATCH, "batch_relative": "batch-" + envelope, **PAIRING,
            "envelope_identity_sha256": envelope, "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
            "complete_files": 3, "complete_bytes": len(raw) + 65 + 5, "delivery_kind": kind})
        item = {"dispatch": d, "files": [{"relative": ledger_path, "sha256": digest, "bytes": 5}]}
        if accepted:
            item["cache_retirement"] = {"envelope": env}
            item["acceptance"] = signed({"contract": ACK, **{k: d[k] for k in (
                "producer_device_id", "receiver_device_id", "repository_id", "envelope_identity_sha256",
                "envelope_file_sha256", "complete_files", "complete_bytes")},
                "dispatch_identity_sha256": d["identity_sha256"], "snapshot_id": "9" * 64,
                "private_evidence_sha256": "8" * 64, "repository_check_mode": "full_read_data",
                "repository_check_verified": True, "independent_restore_verified": True,
                "source_verifier_verified": True, "all_files_sha256_verified": True,
                "command_exit_codes": [0], "complete_workflow_seconds": 1.0,
                "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
                "scope": "this_closed_delivery_off_host_file_backup_only",
                "canonical_reconstruction_verified": False, "control_database_restore_verified": False,
                "independent_key_custody_verified": False})
        else:
            batch = tmp_path / d["batch_relative"]
            batch.mkdir()
            (batch / "backup-envelope.json").write_bytes(raw)
            (batch / "READY").write_text(envelope + "\n")
        return envelope, item

    donor, donor_item = delivery("incremental_cold_objects", "cold/objects/x", "objects/x", accepted=True)
    target, target_item = delivery("private_immutable_cold_backup", "private-history/x", "private-history/x")
    news, news_item = delivery("private_personal_news_backup", "private-news/x", "private/private-news/x")
    ledger = {"deliveries": {donor: donor_item, target: target_item, news: news_item}}
    result = candidate_donors(ledger, PAIRING, tmp_path)
    assert not result["errors"] and len(result["candidates"]) == 2
    assert {r["target_relative"] for r in result["candidates"]} == {"private-history/x", "private-news/x"}
    assert not result["target_NAS_acceptance_or_payload_verified"]
    donor_item["acceptance"]["snapshot_id"] = "0" * 64
    with pytest.raises(ValueError, match="identity"):
        candidate_donors(ledger, PAIRING, tmp_path)


def test_capsule_binding_requires_exact_inventory_membership():
    raw = b"fixed control"
    sha = hashlib.sha256(raw).hexdigest()
    object_name = "objects/" + sha[:2] + "/" + sha
    members = {"commits/run/control.json": {"sha256": sha, "bytes": len(raw)},
               object_name: {"sha256": sha, "bytes": len(raw), "role": "private_rescue_object"}}
    assert member_descriptor(members, "commits/run/control.json", raw)["sha256"] == sha
    assert object_descriptor(members, {"sha256": sha, "bytes": len(raw)})["relative"] == object_name
    with pytest.raises(ValueError, match="fixed control"):
        member_descriptor(members, "commits/run/control.json", raw + b"x")
    with pytest.raises(ValueError, match="absent"):
        object_descriptor(members, {"sha256": sha, "bytes": len(raw) + 1})
