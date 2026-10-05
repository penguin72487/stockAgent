"""Unattended dispatch, shared ownership, retries and proof-bounded scratch."""
from copy import deepcopy
from datetime import datetime, timezone
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from scripts.backup_delivery_receipt import DISPATCH, atomic_public, signed
from scripts.install_lab203_recovery_queue import dropin
from scripts.verify_backup_delivery import verify
from stockagent.data_sync import recovery_queue as module
from stockagent.data_sync.backup_auxiliary import export_auxiliary
from stockagent.data_sync.nas_recovery_acceptance import PLAN, validate_acceptance
from stockagent.data_sync.offhost_backup import ResticBackup, capture_plan, export_delivery
from stockagent.runtime_identity import runtime_identity
from test_backup_auxiliary import auxiliary  # noqa: F401
from test_backup_stream import acknowledge, readiness, stream  # noqa: F401
from test_nas_recovery_acceptance import acceptance, local_admission, recovery_plan  # noqa: F401

ACTUAL_PHYSICAL_FREE_BYTES = module.physical_free_bytes


@pytest.fixture
def queue_configuration(tmp_path, recovery_plan, local_admission, monkeypatch):
    requests, scratch, state = (tmp_path / name for name in ("requests", "scratch", "queue-state"))
    for root in (requests, scratch, state): root.mkdir()
    c = {"schema_version": 1, **{k: recovery_plan[k] for k in module.IDENTITIES},
        "request_root": str(requests), "receipt_root": str(local_admission["receipt_root"]),
        "state_root": str(state), "scratch_root": str(scratch),
        **{k: str(local_admission[k]) for k in ("owner_lock", "nas_configuration", "password_file", "restic", "runtime_lock")},
        "pg_bin": str(tmp_path / "pg"), "minimum_free_bytes": 1024, "maximum_restore_bytes": 1024**2,
        "maximum_plans_per_cycle": 1, "retry_delays_seconds": [5, 15, 60],
        "installed_handoff_identity_sha256": "f" * 64, "windows_distribution": "", "powershell": "/unused/powershell"}
    module.validate_configuration(c)
    monkeypatch.setattr(module, "physical_free_bytes", lambda _: 1024**3)
    atomic_public(requests / ("plan-" + recovery_plan["identity_sha256"] + ".json"), recovery_plan)
    return c


def simulated_acceptance(plan, **kwargs):
    # Queue state tests only: actual semantic/Restic integration is below.
    output = kwargs["output"]
    output.mkdir()
    private = output / "acceptance.private.json"
    private.write_text('{"synthetic_unit_proof":true}')
    result = acceptance(plan)
    result["private_evidence_sha256"] = hashlib.sha256(private.read_bytes()).hexdigest()
    result = signed({k: v for k, v in result.items() if k != "identity_sha256"})
    atomic_public(kwargs["receipt_root"] / ("recovery-acceptance-" + plan["identity_sha256"] + ".json"), result)
    return result


def test_existing_backup_owner_blocks_queue_without_state_changes(queue_configuration):
    c = queue_configuration
    with Path(c["owner_lock"]).open("a") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError): module.run_cycle(c)
    assert not (Path(c["state_root"]) / "queue-state.json").exists()
    assert not Path(c["receipt_root"]).exists()


def test_completed_request_is_not_reexecuted_and_only_own_scratch_is_removed(queue_configuration, monkeypatch):
    calls = []
    def execute(plan, **kwargs):
        calls.append(kwargs)
        # The original owner remains held through the complete semantic run.
        with kwargs["owner_lock"].open("a") as other:
            with pytest.raises(BlockingIOError): fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return simulated_acceptance(plan, **kwargs)
    monkeypatch.setattr(module, "run_acceptance", execute)
    c = queue_configuration
    first = module.run_cycle(c)
    again = module.run_cycle(c)
    assert len(calls) == 1 and first["plans"][0]["state"] == again["plans"][0]["state"] == "accepted"
    assert not list(Path(c["scratch_root"]).iterdir())
    assert list(Path(c["request_root"]).iterdir())
    proof = next(Path(c["state_root"]).glob("*-cleanup.json"))
    assert json.loads(proof.read_bytes())["state"] == "removed_completed_private_scratch"
    assert list(Path(c["state_root"]).glob("*-evidence/acceptance.private.json"))


def test_failure_retries_later_without_false_ack_or_loss_of_failed_scratch(queue_configuration, monkeypatch):
    c = queue_configuration
    def fail(plan, **kwargs):
        kwargs["output"].mkdir()
        (kwargs["output"] / "failure.private.json").write_text("{}")
        raise RuntimeError("synthetic credential-like text must remain private")
    monkeypatch.setattr(module, "run_acceptance", fail)
    result = module.run_cycle(c)
    assert result["plans"][0]["state"] == "retry_wait"
    assert "credential-like" not in json.dumps(result)
    assert not list(Path(c["receipt_root"]).glob("recovery-acceptance-*.json"))
    assert len(list(Path(c["scratch_root"]).iterdir())) == 1
    monkeypatch.setattr(module, "run_acceptance", lambda *_a, **_k: pytest.fail("retry fired before its delay"))
    assert module.run_cycle(c)["plans"][0]["state"] == "retry_wait"
    state_file = Path(c["state_root"]) / "queue-state.json"
    state = json.loads(state_file.read_bytes())
    for item in state["plans"].values(): item["next_attempt_epoch"] = 0
    state_file.write_text(json.dumps(state))
    monkeypatch.setattr(module, "run_acceptance", simulated_acceptance)
    assert module.run_cycle(c)["plans"][0]["state"] == "accepted"
    assert len(list(Path(c["scratch_root"]).iterdir())) == 1  # Original failure stays inspectable.


@pytest.mark.parametrize("damage", ["paired", "command", "budget"])
def test_unsafe_request_cannot_invoke_a_command(queue_configuration, recovery_plan, monkeypatch, damage):
    plan = deepcopy(recovery_plan)
    if damage == "paired": plan["receiver_device_id"] = "another-machine"
    elif damage == "command": plan["command"] = ["arbitrary", "not permitted"]
    else: plan["maximum_restore_bytes"] *= 2
    plan = signed({k: v for k, v in plan.items() if k != "identity_sha256"})
    root = Path(queue_configuration["request_root"])
    for path in root.iterdir(): path.unlink()
    atomic_public(root / ("plan-" + plan["identity_sha256"] + ".json"), plan)
    monkeypatch.setattr(module, "run_acceptance", lambda *_a, **_k: pytest.fail("unsafe request executed"))
    result = module.run_cycle(queue_configuration)
    assert result["plans"][0]["state"] == "rejected"


def test_physical_capacity_gate_never_creates_restore_or_ack(queue_configuration, monkeypatch):
    monkeypatch.setattr(module, "physical_free_bytes", lambda _: 0)
    monkeypatch.setattr(module, "run_acceptance", lambda *_a, **_k: pytest.fail("capacity check bypassed"))
    result = module.run_cycle(queue_configuration)
    assert result["plans"][0]["state"] == "retry_wait"
    assert not list(Path(queue_configuration["scratch_root"]).iterdir())


def test_receipt_survives_restart_between_acceptance_and_cleanup(queue_configuration, monkeypatch):
    cleanup = module.cleanup_success
    monkeypatch.setattr(module, "run_acceptance", simulated_acceptance)
    monkeypatch.setattr(module, "cleanup_success", lambda *_: (_ for _ in ()).throw(OSError("synthetic cleanup interruption")))
    assert module.run_cycle(queue_configuration)["plans"][0]["state"] == "retry_wait"
    monkeypatch.setattr(module, "run_acceptance", lambda *_a, **_k: pytest.fail("completed recovery reexecuted"))
    monkeypatch.setattr(module, "cleanup_success", cleanup)
    assert module.run_cycle(queue_configuration)["plans"][0]["state"] == "accepted"
    assert not list(Path(queue_configuration["scratch_root"]).iterdir())


def test_failed_plan_does_not_starve_later_valid_plans(queue_configuration, recovery_plan, monkeypatch):
    c = queue_configuration
    bad = deepcopy(recovery_plan)
    bad["jobs"][0]["job_id"] = "earlier-failure"
    bad = signed({k: v for k, v in bad.items() if k != "identity_sha256"})
    # Make the first deterministic ordering select the failing plan.
    root = Path(c["request_root"])
    for path in root.iterdir(): path.unlink()
    plans = sorted([bad, recovery_plan], key=lambda p: p["identity_sha256"])
    for plan in plans: atomic_public(root / ("plan-" + plan["identity_sha256"] + ".json"), plan)
    failed = plans[0]["identity_sha256"]
    def execute(plan, **kwargs):
        if plan["identity_sha256"] == failed: raise RuntimeError("synthetic failed snapshot")
        return simulated_acceptance(plan, **kwargs)
    monkeypatch.setattr(module, "run_acceptance", execute)
    first = module.run_cycle(c)
    assert {p["state"] for p in first["plans"]} == {"retry_wait", "queued"}
    # Even if the failure is already due again, the unattempted plan goes first.
    state_file = Path(c["state_root"]) / "queue-state.json"
    state = json.loads(state_file.read_bytes())
    state["plans"][failed]["next_attempt_epoch"] = 0
    state_file.write_text(json.dumps(state))
    second = module.run_cycle(c)
    assert any(p["state"] == "accepted" and p["plan_identity_sha256"] != failed for p in second["plans"])


@pytest.mark.parametrize("block", ["unknown", "link", "process", "private_proof"])
def test_scratch_cleanup_preserves_unknown_or_referenced_data(queue_configuration, recovery_plan, monkeypatch, block):
    c = queue_configuration
    root = Path(c["scratch_root"]) / (recovery_plan["identity_sha256"] + "-" + "a" * 32)
    journal = Path(c["state_root"]) / (root.name + ".json")
    journal.write_text(json.dumps({"output": str(root), "output_name": root.name,
                                  "plan_identity_sha256": recovery_plan["identity_sha256"]}))
    proof = simulated_acceptance(recovery_plan, output=root, receipt_root=Path(c["receipt_root"]))
    if block == "unknown": (root / "do-not-delete.txt").write_text("unknown data")
    elif block == "link":
        (root / "cache").mkdir()
        original = root / "cache/a"; original.write_text("shared bytes")
        os.link(original, root / "cache/b")
    elif block == "process": monkeypatch.setattr(module, "process_references_many", lambda _: ["unit-test-active-process"])
    else: (root / "acceptance.private.json").write_text("changed proof")
    with pytest.raises(ValueError): module.cleanup_success(root, recovery_plan, proof, journal)
    assert root.exists() and (root / "acceptance.private.json").exists()


def test_bad_automatic_request_does_not_block_normal_backup(stream, monkeypatch):
    queue, *_ = stream
    queue.config["automatic_recovery"] = {"enabled": True}
    readiness(queue)
    monkeypatch.setattr(module, "publish_requests", lambda *_: (_ for _ in ()).throw(ValueError("synthetic invalid request")))
    result = queue.cycle()
    assert result["state"] == "published_waiting_nas_receipt"
    assert result["automatic_recovery_requests"]["state"] == "request_publication_failed"
    assert result["receipt_errors"] == []


def test_source_publishes_initial_plan_once_without_promoting_delivery_to_execution(stream, recovery_plan, tmp_path):
    queue, *_ = stream
    plan = signed({**{k: v for k, v in recovery_plan.items() if k != "identity_sha256"},
                   **{k: queue.config[k] for k in module.IDENTITIES}})
    pin = tmp_path / "initial.json"; pin.write_text(json.dumps(plan))
    queue.config.update(nas_recovery_plan=str(pin), automatic_recovery={"enabled": True,
        "minimum_free_bytes": 1024, "maximum_restore_bytes": 1024**2})
    ledger = queue.load_ledger()
    assert module.publish_requests(queue.config, queue.transport, ledger)["published_requests"] == 1
    assert module.publish_requests(queue.config, queue.transport, ledger)["published_requests"] == 0
    status = module.queue_status(queue.config, queue.transport, queue.receipts)
    assert status["state"] == "awaiting_receiver_hook" and status["accepted_count"] == 0


def test_source_queues_only_actual_acked_control_state_then_deduplicates_code_changes(stream, auxiliary, tmp_path):
    queue, *_ = stream
    repo, receipt, _ = auxiliary
    delivery = tmp_path / "closed-control"
    exported = export_auxiliary(repo, delivery, control_receipt=receipt)
    proof = verify(delivery, exported["envelope_identity_sha256"])
    raw = (delivery / "backup-envelope.json").read_bytes()
    key = exported["envelope_identity_sha256"]
    batch = "batch-" + key
    delivery.rename(queue.transport / batch)
    dispatch = signed({"contract": DISPATCH, **{k: queue.config[k] for k in module.IDENTITIES},
        "envelope_identity_sha256": key, "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
        "batch_relative": batch, "complete_files": proof["files_verified"] + 2,
        "complete_bytes": proof["bytes_verified"] + len(raw) + (queue.transport / batch / "READY").stat().st_size,
        "delivery_kind": "code_and_control_backup"})
    record = {"dispatch": dispatch}
    ledger = {"deliveries": {key: record}}
    queue.config["automatic_recovery"] = {"enabled": True, "minimum_free_bytes": 1024, "maximum_restore_bytes": 1024**2}
    assert module.publish_requests(queue.config, queue.transport, ledger)["published_requests"] == 0
    record["acceptance"] = acknowledge(queue, record)
    assert module.publish_requests(queue.config, queue.transport, ledger)["published_requests"] == 1
    assert module.publish_requests(queue.config, queue.transport, ledger)["published_requests"] == 0
    plan = json.loads(next((queue.transport / "tools/recovery-requests").glob("plan-*.json")).read_bytes())
    assert plan["jobs"][0]["nas_snapshot_id"] == record["acceptance"]["snapshot_id"]
    assert plan["jobs"][0]["table_count"] == plan["jobs"][0]["row_count"] == 0


def test_windows_capacity_uses_encoded_fixed_registry_query(queue_configuration, monkeypatch):
    c = {**queue_configuration, "windows_distribution": "Ubuntu"}
    monkeypatch.setattr(module, "physical_free_bytes", ACTUAL_PHYSICAL_FREE_BYTES)
    original_read = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda self, *a, **k: "microsoft-standard-WSL2" if str(self) == "/proc/sys/kernel/osrelease" else original_read(self, *a, **k))
    monkeypatch.setenv("WSL_DISTRO_NAME", "Ubuntu")
    def execute(argv, **kwargs):
        assert "-EncodedCommand" in argv and kwargs["timeout"] == 30
        program = base64.b64decode(argv[-1]).decode("utf-16-le")
        assert "ext4.vhdx" in program and "AvailableFreeSpace" in program and "HKCU:" in program
        assert program.endswith(" 'Ubuntu'")
        return subprocess.CompletedProcess(argv, 0, stdout=b'{"free_bytes":99999999,"distribution":"Ubuntu"}', stderr="無關的中文診斷".encode("cp950"))
    monkeypatch.setattr(module.subprocess, "run", execute)
    assert module.physical_free_bytes(c) == 99999999
    monkeypatch.setenv("WSL_DISTRO_NAME", "DifferentDistro")
    with pytest.raises(ValueError): module.physical_free_bytes(c)


def test_dropin_uses_existing_service_fixed_installed_script_only():
    body = dropin(Path("/opt/lab203-backup/recovery-code-v6"))
    assert "ExecStartPost=-/bin/bash" in body and "ExecStart=" not in body and "ingress" not in body
    with pytest.raises(ValueError): dropin(Path("/opt/unsafe%systemd"))


def test_public_queue_status_rejects_secret_fields(queue_configuration, monkeypatch):
    c = queue_configuration
    monkeypatch.setattr(module, "run_acceptance", simulated_acceptance)
    status = module.run_cycle(c)
    root = Path(c["request_root"])
    # Source layout with the same plans, independent private roots.
    transport = root.parent / "transport"
    (transport / "tools/recovery-requests").mkdir(parents=True)
    for path in root.iterdir(): (transport / "tools/recovery-requests" / path.name).write_bytes(path.read_bytes())
    config = {**c, "readiness_max_age_seconds": 900}
    observed = module.queue_status(config, transport, Path(c["receipt_root"]))
    assert observed["accepted_count"] == 1 and observed["state"] == "receiver_polling"
    status["plans"][0]["private_path"] = "/must/not/be/returned"
    changed = signed({k: v for k, v in status.items() if k != "identity_sha256"})
    atomic_public(Path(c["receipt_root"]) / "recovery-queue-status.json", changed, replace=True)
    with pytest.raises(ValueError): module.queue_status(config, transport, Path(c["receipt_root"]))


def test_real_restic_queue_restores_reconstructs_and_retires_its_successful_scratch(
        stream, queue_configuration, local_admission, tmp_path, monkeypatch):
    # Real encrypted I/O and canonical verifier; CIFS admission is simulated.
    # This engineering test does not claim an actual lab203 NAS acceptance.
    binary = os.environ.get("STOCKAGENT_TEST_RESTIC_BINARY")
    if not binary: pytest.skip("real Restic binary not supplied")
    from stockagent.data_sync import nas_target
    monkeypatch.setattr(nas_target, "mounted_volume", lambda _: ("unit-test", "cifs", "//140.127.208.143/Lab203"))
    _, cold, _, _, selected = stream
    delivery = tmp_path / "encrypted-delivery"
    exported = export_delivery(capture_plan(cold, selectors=[("prices", selected.manifest["snapshot_id"])]), delivery)
    client = ResticBackup(Path(binary), str(tmp_path / "nas/user/repository"), local_admission["password_file"], cache=tmp_path / "test-cache")
    repository = client.initialize()
    backup = client.run(["backup", "--json", str(delivery)])
    snap = next(json.loads(line)["snapshot_id"] for line in backup.stdout.splitlines() if json.loads(line).get("message_type") == "summary")
    transport = verify(delivery, exported["envelope_identity_sha256"])
    raw = (delivery / "backup-envelope.json").read_bytes()
    plan = signed({"contract": PLAN, "producer_device_id": "source-test", "receiver_device_id": "receiver-test",
        "repository_id": repository["id"], "minimum_free_bytes": 1024, "maximum_restore_bytes": 1024**2,
        "full_history_backup_verified": False, "jobs": [{"job_id": "prices", "kind": "packed",
            "nas_snapshot_id": snap, "envelope_identity_sha256": exported["envelope_identity_sha256"],
            "envelope_file_sha256": hashlib.sha256(raw).hexdigest(), "complete_files": transport["files_verified"] + 2,
            "complete_bytes": transport["bytes_verified"] + len(raw) + (delivery / "READY").stat().st_size,
            "dataset": "prices", "source_snapshot_id": selected.manifest["snapshot_id"],
            "manifest_sha256": selected.manifest_sha256,
            "source_fingerprint_sha256": selected.manifest["source"]["portable_fingerprint_sha256"]}]})
    c = {**queue_configuration, "repository_id": repository["id"], "restic": binary}
    root = Path(c["request_root"])
    for path in root.iterdir(): path.unlink()
    atomic_public(root / ("plan-" + plan["identity_sha256"] + ".json"), plan)
    status = module.run_cycle(c)
    assert status["plans"][0]["state"] == "accepted", status
    receipt = json.loads((Path(c["receipt_root"]) / ("recovery-acceptance-" + plan["identity_sha256"] + ".json")).read_bytes())
    validate_acceptance(receipt, plan)
    assert receipt["jobs"][0]["canonical_reconstruction_verified"]
    assert not list(Path(c["scratch_root"]).iterdir()) and delivery.exists()
    assert module.run_cycle(c)["plans"][0]["state"] == "accepted"
