"""Real CPU checkpoint reconstruction and fail-closed rescue orchestration.

These tests do not claim a new Vast GPU, WAN throughput or cross-GPU bit parity.
"""
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tarfile
import time
import zipfile

import numpy as np
import pytest
import torch

from stockagent.training_rescue import Rescue, RescueError, SSH, ControlSSH, load_policy
from stockagent.training_rescue_worker import (
    CONTRACT, absolute, atomic_json, checkpoint_summary, control, digest,
    extract_tar, file_hash, freeze_runtime, inspect_run, relative,
    restore_runtime, restore_source, validate_spec, seal_run, ack_seal,
    prepare_step, _group_members, _tagged_members, target_preflight, verify_runtime_capsule,
)
from stockagent.training.trainer import (
    _save_group_checkpoint, _load_checkpoint, _restore_rng_state,
)

REPO = Path(__file__).resolve().parents[1]
WORKER = REPO / "stockagent/training_rescue_worker.py"


@pytest.fixture
def formal(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    output = tmp_path / "artifacts/markets/formal"
    group = output / "train_2024"
    group.mkdir(parents=True)
    config = output / "generated.yaml"
    config.write_text("runner: {resume: true}\n")
    spec = {"contract": CONTRACT, "schema_version": 1, "run_id": "formal",
        "source_root": str(root), "output_root": str(output), "config_file": str(config),
        "runtime_prefix": sys.prefix, "world_size": 1, "global_batch": 4,
        "train_argv": ["train.py", "--config", str(config), "--output-dir", str(output), "--resume"],
        "environment": {}, "data_sources": [], "rebuild_steps": []}
    model = torch.nn.Linear(3, 2)
    optimizer = torch.optim.AdamW(model.parameters(), lr=0.003)
    scaler = torch.amp.GradScaler("cpu", enabled=False)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=2, gamma=0.7)
    semantic = {"schema_version": 4, "fingerprints": {"data": "test-data", "model": "linear", "training": "adam"}}

    def step(m=model, o=optimizer, s=scheduler):
        x = torch.randn(4, 3)
        y = torch.randn(4, 2) + float(np.random.randn()) + random.random()
        o.zero_grad(set_to_none=True)
        loss = (m(x) - y).square().mean()
        loss.backward(); o.step(); s.step()
        return loss.detach().clone()

    random.seed(91); np.random.seed(92); torch.manual_seed(93)
    for _ in range(3):
        step()
    checkpoint = group / "checkpoint_last.pt"
    _save_group_checkpoint(checkpoint, train_years=[2024], epoch=3, model=model,
        optimizer=optimizer, scaler=scaler, experiment_manifest=semantic,
        effective_train_batch_size=4, scheduler=scheduler, no_improve_epochs=2,
        early_stop_patience=10, early_stopping_no_improve_ratio=0.1,
        early_stop_val_interval_epochs=1)
    (group / "epoch_curve.jsonl").write_text("".join(json.dumps({"epoch": n}) + "\n" for n in (1, 2, 3)))
    atomic_json(output / "run_manifest.json", {"configuration": {"training": {"epochs": 10}}, "schema_version": 1})
    atomic_json(output / "progress.json", {"state": "training", "epoch": {"current": 3}})
    return spec, model, optimizer, scheduler, checkpoint, step


@pytest.fixture
def rescue(tmp_path, monkeypatch):
    policy = load_policy(REPO / "configs/training_rescue.json")
    policy.update(state_root=str(tmp_path / "state"), cold_root=str(tmp_path / "cold"))
    r = Rescue(policy)
    monkeypatch.setattr(r, "guard", lambda: r.cold.mkdir(parents=True, exist_ok=True))
    return r


class LocalSource:
    """Actual adapter/byte processes; only the SSH network boundary is replaced."""
    def __init__(self, spec, remote_state):
        self.spec = spec
        self.remote_state = remote_state / spec["run_id"]
    def adapter(self, action, *args, **kwargs):
        if action == "health":
            return {"state": "nvml_visible_not_compute_acceptance", "devices": [{"index": 0}],
                    "node_identity": {"boot_id": "fixture", "instance_id": "123"}}
        if action == "inspect":
            return inspect_run(self.spec)
        if action == "seal":
            return seal_run(self.spec, self.remote_state)
        if action == "ack-seal":
            return ack_seal(self.spec, self.remote_state,
                            args[args.index("--sha256") + 1], args[args.index("--commit-id") + 1])
        raise AssertionError(action)
    def open_read(self, root, row, *, python, errors):
        return subprocess.Popen([sys.executable, str(WORKER), "read", "--path", str(root),
            "--row", json.dumps(row)], stdout=subprocess.PIPE, stderr=errors)


def enroll_fixture(rescue, spec, monkeypatch):
    atomic_json(rescue.path(spec["run_id"]) / "spec.json", spec)
    remote_state = rescue.state / "remote-fixture"
    monkeypatch.setattr("stockagent.training_rescue.REMOTE_STATE", str(remote_state))
    monkeypatch.setattr(rescue, "source", lambda _: LocalSource(spec, remote_state))
    return LocalSource(spec, remote_state)


def test_full_canonical_optimizer_rng_resume_after_cold_reconstruction(rescue, formal, monkeypatch, tmp_path):
    spec, model, optimizer, scheduler, checkpoint, step = formal
    enroll_fixture(rescue, spec, monkeypatch)
    accepted = rescue.capture("formal")
    assert accepted["D_original_readback_verified"] is True
    commit = json.loads((rescue.cold / "commits/formal" / (accepted["commit_id"] + ".json")).read_bytes())
    restored = tmp_path / "empty-worker/artifacts/markets/formal"
    for row in commit["inventory"]["files"]:
        p = restored / row["relative"]
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(rescue.object_path(row["sha256"]).read_bytes())
        assert file_hash(p) == row["sha256"]
    saved = _load_checkpoint(restored / "train_2024/checkpoint_last.pt")
    expected_losses = [step() for _ in range(3)]
    expected_weights = {k: v.clone() for k, v in model.state_dict().items()}
    # A new process/model starts with unrelated values, then restores ALL state.
    other = torch.nn.Linear(3, 2)
    opt = torch.optim.AdamW(other.parameters(), lr=99)
    sched = torch.optim.lr_scheduler.StepLR(opt, step_size=2, gamma=0.7)
    other.load_state_dict(saved["model_state_dict"])
    opt.load_state_dict(saved["optimizer_state_dict"])
    sched.load_state_dict(saved["scheduler_state_dict"])
    _restore_rng_state(saved["rng_state"])
    actual_losses = [step(other, opt, sched) for _ in range(3)]
    assert all(torch.equal(a, b) for a, b in zip(actual_losses, expected_losses))
    assert all(torch.equal(v, expected_weights[k]) for k, v in other.state_dict().items())
    assert saved["epoch"] == 3 and saved["no_improve_epochs"] == 2
    assert saved["early_stop_patience"] == 10
    assert saved["effective_train_batch_size"] == 4
    assert saved["scaler_state_dict"] == {}
    assert sched.state_dict() == scheduler.state_dict()
    assert checkpoint.exists()  # Source was NEVER deleted.


def test_unchanged_capture_is_incremental_no_new_objects(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    first = rescue.capture("formal")
    objects = set(rescue.cold.rglob("*"))
    second = rescue.capture("formal")
    assert second["state"] == "unchanged_committed_recovery_point"
    assert set(rescue.cold.rglob("*")) == objects
    assert rescue.journal("formal")["latest"]["commit_id"] == first["commit_id"]


def test_corrupt_incoming_cannot_publish_head(rescue):
    row = {"sha256": "a" * 64, "bytes": 3}
    with pytest.raises(RescueError, match="partial/corrupt"):
        rescue.put_stream(io.BytesIO(b"bad"), row)
    assert not rescue.object_path(row["sha256"]).exists()
    assert list(rescue.cold.rglob("*.partial.*"))
    assert not list(rescue.cold.glob("commits/*/*"))


def test_corrupt_existing_object_blocks_recovery(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture("formal")
    p = next(p for p in (rescue.cold / "objects").rglob("*") if p.is_file())
    p.write_bytes(b"bad")
    row = {"sha256": p.name, "bytes": 3}
    with pytest.raises(RescueError, match="independently recoverable"):
        rescue.push_object(None, row, Path("/tmp/unused-destination"))


def test_context_advance_cannot_mix_epoch(rescue, formal, monkeypatch):
    spec = formal[0]; source = enroll_fixture(rescue, spec, monkeypatch)
    calls = [0]
    def inspect_changed(value_spec):
        value = inspect_run(spec); calls[0] += 1
        if calls[0] == 2:
            value["context_sha256"] = "b" * 64
        return value
    monkeypatch.setattr("stockagent.training_rescue_worker.inspect_run", inspect_changed)
    with pytest.raises(ValueError, match="epoch boundary"):
        rescue.capture("formal", transport=source)
    assert not rescue.journal("formal").get("latest")
    assert list(source.remote_state.glob("sealed/.partial-*"))
    monkeypatch.setattr("stockagent.training_rescue_worker.inspect_run", inspect_run)
    assert rescue.capture("formal", transport=source)["D_original_readback_verified"]


def test_live_progress_writes_do_not_starve_recovery(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture("formal")
    atomic_json(Path(spec["output_root"]) / "progress.json", {"state": "training", "epoch": 999})
    assert rescue.capture("formal")["state"] == "unchanged_committed_recovery_point"


def test_existing_ack_does_not_hide_cold_corruption(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture("formal")
    obj = next(p for p in (rescue.cold / "objects").rglob("*") if p.is_file())
    obj.write_bytes(b"corrupt")
    with pytest.raises(RescueError, match="corrupt"):
        rescue.capture("formal")


def test_checkpoint_curve_lag_fails_closed(formal):
    spec, *_ = formal
    (Path(spec["output_root"]) / "train_2024/epoch_curve.jsonl").write_text('{"epoch": 2}\n')
    with pytest.raises(ValueError, match="same committed epoch"):
        inspect_run(spec)


@pytest.mark.parametrize("field", ["optimizer_state_dict", "rng_state", "scheduler_state_dict", "scaler_state_dict", "effective_train_batch_size"])
def test_weights_only_is_not_full_resume_point(formal, field):
    spec, _, _, _, p, _ = formal
    payload = torch.load(p, weights_only=True)
    del payload[field]
    torch.save(payload, p)
    with pytest.raises(ValueError, match="optimizer recovery"):
        checkpoint_summary(p, spec)


@pytest.mark.parametrize("changed", [{"world_size": 2}, {"global_batch": 8}])
def test_no_silent_world_size_or_batch_change(formal, changed):
    spec, _, _, _, p, _ = formal
    with pytest.raises(ValueError, match="world size or effective global batch"):
        checkpoint_summary(p, {**spec, **changed})


def test_semantic_checkpoint_mismatch_rejected(formal):
    spec, _, _, _, p, _ = formal
    spec["checkpoint_manifests"] = {p.relative_to(Path(spec["output_root"])).as_posix(): "0" * 64}
    with pytest.raises(ValueError, match="semantic manifest"):
        checkpoint_summary(p, spec)


def test_compact_checkpoint_preserves_rescue_identity_and_rejects_corruption(formal):
    from stockagent.training.checkpoint_transport import encode_checkpoint_manifest
    spec, _, _, _, path, _ = formal
    payload = torch.load(path, map_location="cpu", weights_only=True)
    original = checkpoint_summary(path, spec)
    spec["checkpoint_manifests"] = {
        path.relative_to(Path(spec["output_root"])).as_posix():
            digest(payload["experiment_manifest"]),
    }
    encoded = encode_checkpoint_manifest(payload)
    torch.save(encoded, path)
    restored = checkpoint_summary(path, spec)
    assert restored["semantic_manifest_sha256"] == original["semantic_manifest_sha256"]
    assert restored["epoch"] == 3 and restored["full_optimizer_state"]
    encoded["experiment_manifest"]["payload"] += b" "
    torch.save(encoded, path)
    with pytest.raises(ValueError, match="SHA256"):
        checkpoint_summary(path, spec)


def test_config_drift_is_not_an_acknowledged_recovery_point(formal):
    spec = formal[0]
    config = Path(spec["config_file"])
    spec["bootstrap_files"] = [{"role": "training_config", "sha256": file_hash(config)}]
    config.write_text("runner: {resume: false}\n")
    with pytest.raises(ValueError, match="configuration changed"):
        inspect_run(spec)


@pytest.mark.parametrize("bad", ["../secret", "/etc/passwd", "a/../../secret", "a\\b", "a//b", "."])
def test_member_path_traversal_rejected(bad):
    with pytest.raises(ValueError):
        relative(bad)


def test_d_unavailable_never_falls_back_to_c(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    def missing():
        raise RescueError("D missing")
    monkeypatch.setattr(rescue, "guard", missing)
    with pytest.raises(RescueError, match="D missing"):
        rescue.capture("formal")
    assert not rescue.journal("formal").get("latest")
    assert not rescue.cold.exists()


def test_gpu_fault_preserves_progress_but_never_migrates(rescue, formal, monkeypatch):
    spec = formal[0]; source = enroll_fixture(rescue, spec, monkeypatch)
    original = source.adapter
    source.adapter = lambda action, *args, **kwargs: {"state": "gpu_fault", "reason": "reboot_required"} if action == "health" else original(action, *args, **kwargs)
    monkeypatch.setattr(rescue, "source", lambda _: source)
    value = rescue.cycle()
    state = rescue.journal("formal")
    assert value["automatic_migration"] is False
    assert state["state"] == "awaiting_user_confirmation"
    assert state["latest"]["checkpoints"]["train_2024/checkpoint_last.pt"]["epoch"] == 3
    assert not (rescue.state / "plans").exists()


def test_ssh_loss_retains_previous_verified_checkpoint(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture("formal")
    previous = rescue.journal("formal")["latest"]
    def lost(_):
        raise RescueError("SSH lost")
    monkeypatch.setattr(rescue, "source", lost)
    rescue.cycle()
    assert rescue.journal("formal")["latest"] == previous
    assert rescue.journal("formal")["state"] == "awaiting_user_confirmation"


def test_temporary_key_only_exports_public_and_expires(rescue, monkeypatch):
    record = rescue.issue_key(hours=1)
    assert record["public_key"].startswith("ssh-ed25519 ")
    assert "restrict,expiry-time=" in record["authorized_keys_line"]
    assert "PRIVATE KEY" not in json.dumps(record)
    value, key = rescue.key(record["key_id"])
    assert key.stat().st_mode & 0o077 == 0
    monkeypatch.setattr(time, "time", lambda: value["expires_at"] + 1)
    with pytest.raises(RescueError, match="expired"):
        rescue.key(record["key_id"])


def make_plan(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture("formal")
    state = rescue.journal("formal"); state["bootstrap"] = {"id": "c" * 64}
    state["source_identity"] = {"boot_id": "fixture", "instance_id": "123"}
    rescue.record("formal", state)
    key = rescue.issue_key(hours=1)
    public = " ".join(key["public_key"].split()[:2])
    planned = rescue.plan("formal", host="192.0.2.1", port=40032, user="root", key_id=key["key_id"],
                         host_key_line="[192.0.2.1]:40032 " + public)
    return planned, key


def test_user_confirmation_required_before_contact(rescue, formal, monkeypatch):
    planned, _ = make_plan(rescue, formal, monkeypatch)
    assert planned["target_contacted"] is False
    with pytest.raises(RescueError, match="explicit confirmation"):
        rescue.recover(planned["plan_id"], confirm="yes", old_node_stopped_receipt=Path("/not-used"))


def test_host_key_bound_to_exact_endpoint(rescue, formal, monkeypatch):
    make_plan(rescue, formal, monkeypatch)
    k = rescue.issue_key()
    with pytest.raises(RescueError, match="different destination"):
        rescue.plan("formal", host="192.0.2.2", port=40032, user="root", key_id=k["key_id"],
                    host_key_line="[192.0.2.1]:40032 " + " ".join(k["public_key"].split()[:2]))


def test_tampered_approval_refused(rescue, formal, monkeypatch):
    p, _ = make_plan(rescue, formal, monkeypatch)
    path = rescue.state / "plans" / (p["plan_id"] + ".json")
    value = json.loads(path.read_bytes()); value["target"]["host"] = "192.0.2.88"
    atomic_json(path, value)
    with pytest.raises(RescueError, match="corrupted"):
        rescue.confirmed_plan(p["plan_id"], p["plan_id"])


def test_gpu_failure_is_not_old_instance_stop_proof(rescue, formal, monkeypatch, tmp_path):
    p, _ = make_plan(rescue, formal, monkeypatch)
    receipt = tmp_path / "unproved.json"
    atomic_json(receipt, {"run_id": "formal", "source_generation": 1, "gpu_down": True})
    with pytest.raises(RescueError, match="fencing proof"):
        rescue.recover(p["plan_id"], confirm=p["plan_id"], old_node_stopped_receipt=receipt)
    assert rescue.journal("formal")["generation"] == 1


def test_runtime_capsule_restore_is_exact_and_no_overwrite(tmp_path):
    prefix = tmp_path / "runtime"
    (prefix / "bin").mkdir(parents=True)
    (prefix / "bin/fixture").write_bytes(b"unchanged binary\x00")
    (prefix / "bin/fixture").chmod(0o755)
    (prefix / "bin/alias").symlink_to("fixture")
    capsule = tmp_path / "capsule.tar.gz"
    summary = freeze_runtime(prefix, capsule)
    # Simulate disappearance of the source node in this isolated fixture.
    prefix.rename(tmp_path / "lost-worker-runtime")
    result = restore_runtime(capsule, capsule.with_suffix(".gz.json"), prefix)
    assert result["state"] == "runtime_exact_bytes_restored"
    assert (prefix / "bin/fixture").read_bytes() == b"unchanged binary\x00"
    assert (prefix / "bin/alias").readlink() == Path("fixture")
    assert summary["uncompressed_bytes"] == len(b"unchanged binary\x00")
    assert restore_runtime(capsule, capsule.with_suffix(".gz.json"), prefix)
    (prefix / "bin/fixture").write_bytes(b"changed")
    with pytest.raises(ValueError, match="restored runtime member"):
        restore_runtime(capsule, capsule.with_suffix(".gz.json"), prefix)


def test_runtime_external_link_not_archived(tmp_path):
    p = tmp_path / "runtime"; p.mkdir()
    (p / "secret").symlink_to("/etc/passwd")
    with pytest.raises(ValueError, match="external symlink"):
        freeze_runtime(p, tmp_path / "bad.tar.gz")
    assert not (tmp_path / "bad.tar.gz.json").exists()


def test_fixed_runtime_internal_hardlinks_reconstruct_without_external_links(tmp_path):
    prefix = tmp_path / "runtime"; prefix.mkdir()
    (prefix / "first").write_bytes(b"closed runtime binary")
    os.link(prefix / "first", prefix / "second")
    capsule = tmp_path / "capsule.tar.gz"
    freeze_runtime(prefix, capsule)
    prefix.rename(tmp_path / "old-runtime")
    restore_runtime(capsule, capsule.with_suffix(".gz.json"), prefix)
    assert (prefix / "first").read_bytes() == (prefix / "second").read_bytes() == b"closed runtime binary"
    assert (prefix / "first").stat().st_ino == (prefix / "second").stat().st_ino


@pytest.mark.parametrize("member", ["../escape", "/etc/overwrite"])
def test_archive_traversal_cannot_activate(tmp_path, member):
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as f:
        info = tarfile.TarInfo(member); info.size = 1; f.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="unsafe rescue member"):
        extract_tar(archive, tmp_path / "restored", maximum_bytes=100)


def test_source_bundle_restores_without_git_or_package_solver(tmp_path):
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("train.py", b"canonical entrypoint")
    files = {"train.py": __import__("hashlib").sha256(b"canonical entrypoint").hexdigest()}
    receipt = tmp_path / "release.json"
    atomic_json(receipt, {"code": {"files": files, "source_sha256": digest(files)},
        "source_bundle": {"files": files, "sha256": file_hash(archive)}})
    target = tmp_path / "empty-node/code/build-source"
    assert restore_source(archive, receipt, target)["state"] == "frozen_source_verified"
    assert (target / "train.py").read_bytes() == b"canonical entrypoint"
    assert restore_source(archive, receipt, target)["reused"]


def test_forced_control_key_cannot_run_shell(formal, tmp_path, monkeypatch):
    spec = formal[0]; file = tmp_path / "spec.json"; atomic_json(file, spec)
    fake = type("Input", (), {"buffer": io.BytesIO(b'{"action":"shell","argv":["rm","-rf","/"]}\n')})()
    monkeypatch.setattr(sys, "stdin", fake)
    with pytest.raises(ValueError, match="arbitrary commands"):
        control(file, tmp_path / "status.json", 2)


def test_lease_replay_and_old_generation_rejected(formal, tmp_path, monkeypatch):
    spec = formal[0]; file = tmp_path / "run/spec.json"; atomic_json(file, spec)
    boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    lease = {"contract": CONTRACT, "run_id": "formal", "spec_sha256": digest(spec), "generation": 2,
        "boot_id": boot, "sequence": 10, "issued_at": time.time(), "expires_at": time.time() + 30, "ttl_seconds": 30}
    def call(value):
        monkeypatch.setattr(sys, "stdin", type("Input", (), {"buffer": io.BytesIO(json.dumps({"action": "lease", "lease": value}).encode() + b"\n")})())
        return control(file, tmp_path / "status.json", 2)
    assert call(lease) == 0
    with pytest.raises(ValueError, match="replay"):
        call(lease)
    with pytest.raises(ValueError, match="fenced"):
        call({**lease, "generation": 1, "sequence": 11})


def test_ssh_uses_strict_known_hosts_no_agent_or_controlmaster(tmp_path):
    key = tmp_path / "key"; key.write_bytes(b"fixture-not-an-actual-key"); key.chmod(0o600)
    known = tmp_path / "known"; known.write_bytes(b"fixture"); known.chmod(0o600)
    ssh = SSH.target({"user": "root", "host": "192.0.2.1", "port": 40032}, key, known)
    joined = " ".join(ssh.base)
    assert "StrictHostKeyChecking=yes" in joined and "ForwardAgent=no" in joined
    assert "ControlMaster=no" in joined and "-F /dev/null" in joined
    assert "StrictHostKeyChecking=no" not in joined


def test_cli_help_never_looks_for_a_new_host():
    result = subprocess.run([sys.executable, str(REPO / "scripts/training_rescue.py"), "--help"], capture_output=True, check=True)
    assert b"recover" in result.stdout and b"revoke-key" in result.stdout


def test_policy_forbids_provider_mutations_and_c_fallback(tmp_path):
    p = json.loads((REPO / "configs/training_rescue.json").read_bytes())
    p["automatic_provider_mutations"] = True
    f = tmp_path / "policy.json"; atomic_json(f, p)
    with pytest.raises(RescueError, match="destroys"):
        load_policy(f)


def test_receive_preserves_original_permissions(tmp_path):
    path = tmp_path / "checkpoint.pt"
    payload = b"closed optimizer fixture"
    sha = __import__("hashlib").sha256(payload).hexdigest()
    command = [sys.executable, str(WORKER), "receive", "--path", str(path),
               "--sha256", sha, "--bytes", str(len(payload)), "--mode", str(0o644)]
    result = subprocess.run(command, input=payload, capture_output=True, check=True)
    assert json.loads(result.stdout)["durable"]
    assert path.stat().st_mode & 0o777 == 0o644 and path.read_bytes() == payload


def test_ack_retirement_never_removes_source_or_unexpected_data(formal, tmp_path):
    spec = formal[0]
    inventory = seal_run(spec, tmp_path / "transport")
    root = Path(inventory["sealed_root"])
    extra = root / "not-owned.txt"; extra.write_bytes(b"protect")
    with pytest.raises(ValueError, match="unexpected data"):
        ack_seal(spec, tmp_path / "transport", inventory["context_sha256"], "a" * 64)
    assert extra.exists() and formal[4].exists()


@pytest.mark.parametrize("status", ["running", "offline", "unknown", "loading"])
def test_provider_stop_intent_or_offline_is_not_stop_readback(rescue, formal, monkeypatch, status):
    make_plan(rescue, formal, monkeypatch)
    monkeypatch.setenv("VAST_API_KEY", "fixture-token-not-a-real-secret")
    def urlopen(request, timeout):
        assert request.get_method() == "GET"
        assert request.full_url.endswith("/123/?owner=me")
        return io.BytesIO(json.dumps({"instances": {"id": 123, "actual_status": status,
                                                   "intended_status": "stopped"}}).encode())
    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    with pytest.raises(RescueError, match="NOT been verified stopped"):
        rescue.fence_provider("formal")
    assert not list((rescue.state / "fences").glob("*.json"))


def test_readonly_provider_fence_pins_instance_without_leaking_env(rescue, formal, monkeypatch):
    make_plan(rescue, formal, monkeypatch)
    monkeypatch.setenv("VAST_API_KEY", "fixture-token-not-a-real-secret")
    def urlopen(request, timeout):
        assert request.get_method() == "GET"
        return io.BytesIO(b'{"instances":{"id":123,"actual_status":"stopped","extra_env":{"private":"not-for-receipt"}}}')
    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    proof = rescue.fence_provider("formal")
    body = Path(proof["receipt"]).read_text()
    assert "not-for-receipt" not in body and "fixture-token" not in body
    assert json.loads(body)["source_identity"] == {"boot_id": "fixture", "instance_id": "123"}
    assert proof["provider_write_called"] is False


def test_another_instance_stop_receipt_cannot_fence_this_run(rescue, formal, monkeypatch, tmp_path):
    p, _ = make_plan(rescue, formal, monkeypatch)
    file = tmp_path / "wrong-instance.json"
    atomic_json(file, {"run_id": "formal", "source_generation": 1,
        "old_node_stopped_verified": True, "evidence_kind": "provider_stopped_readback",
        "source_identity": {"boot_id": "other", "instance_id": "456"}, "instance_id": "456",
        "actual_status": "stopped", "observed_at": time.time()})
    with pytest.raises(RescueError, match="fencing proof"):
        rescue.recover(p["plan_id"], confirm=p["plan_id"], old_node_stopped_receipt=file)


def test_prepare_retry_preserves_partial_and_refuses_unowned(formal, tmp_path):
    spec = formal[0]
    root = tmp_path / "artifacts/data_preparation/formal/prepared"
    spec["rebuild_steps"] = [["scripts/prepare_fixture.py", "--output-root", str(root)]]
    path = tmp_path / "controller/spec.json"; atomic_json(path, spec)
    root.mkdir(parents=True); (root / "unique.bin").write_bytes(b"keep")
    with pytest.raises(ValueError, match="unowned"):
        prepare_step(path, 0)
    root.rename(tmp_path / "unowned-preserved")
    assert not prepare_step(path, 0)["skip_builder"]
    root.mkdir(); (root / "partial.bin").write_bytes(b"incomplete")
    assert not prepare_step(path, 0)["skip_builder"]
    assert not root.exists()
    quarantined = list(root.parent.glob("prepared.rescue-incomplete-*"))
    assert len(quarantined) == 1 and (quarantined[0] / "partial.bin").read_bytes() == b"incomplete"


def test_child_exit_does_not_prove_descendants_gone():
    # Isolated test process group; no service or user job is targeted.
    leader = subprocess.Popen([sys.executable, "-c",
        "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print(p.pid,flush=True)"],
        stdout=subprocess.PIPE, start_new_session=True, text=True)
    child_pid = int(leader.stdout.readline())
    try:
        leader.wait(timeout=10)
        assert leader.returncode == 0 and child_pid in _group_members(leader.pid)
    finally:
        import signal
        try:
            os.killpg(leader.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        leader.stdout.close()


def test_real_guardian_expires_without_owner_and_stops_owned_process_group(formal, tmp_path):
    # CUDA acceptance is an explicit stub here. This is a real Linux process,
    # lease-clock and descendant-retirement test, not GPU readiness evidence.
    spec = formal[0]
    prefix = tmp_path / "isolated-runtime"; (prefix / "bin").mkdir(parents=True)
    import shlex
    python = prefix / "bin/python"
    python.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
    python.chmod(0o755)
    spec["runtime_prefix"] = str(prefix)
    spec["environment"] = {"STOCKAGENT_GPU_LEASE_ROOT": str(tmp_path / "isolated-gpu-locks")}
    source = Path(spec["source_root"]); (source / "scripts").mkdir()
    (source / "scripts/runtime_env.sh").write_text('exec_fintech_python() { exec "$PYTHON_BIN" "$@"; }\n')
    (source / "scripts/check_environment.py").write_text("# fixture preflight, no CUDA claim\n")
    (source / "train.py").write_text("import subprocess,sys,time,pathlib\np=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session=True)\npathlib.Path('isolated-child.pid').write_text(str(p.pid))\ntime.sleep(60)\n")
    path = tmp_path / "guardian/spec.json"; atomic_json(path, spec)
    lease = path.parent / "lease.json"; status = path.parent / "status.json"
    atomic_json(lease, {"contract": CONTRACT, "run_id": "formal", "spec_sha256": digest(spec),
        "generation": 2, "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "sequence": 1, "issued_at": time.time(), "expires_at": time.time() + 10, "ttl_seconds": 10})
    worker = subprocess.Popen([sys.executable, str(WORKER), "supervise", "--spec", str(path),
        "--lease", str(lease), "--output", str(status), "--generation", "2"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        assert worker.wait(timeout=25) == 75
        saved = json.loads(status.read_bytes())
        assert saved["state"] == "fenced_or_failed" and saved["child_identity"]
        assert saved["owned_processes_gone_verified"] is True
        assert not _group_members(saved["child_identity"]["pid"])
        assert (source / "isolated-child.pid").is_file()
        assert not _tagged_members(saved["owned_process_tag"])
    finally:
        if worker.poll() is None:
            worker.terminate(); worker.wait(timeout=35)
        worker.stderr.close()


def test_cancel_approved_but_unbootstrapped_key_revokes_local_use(rescue, formal, monkeypatch):
    p, key = make_plan(rescue, formal, monkeypatch)
    result = rescue.revoke_key(key["key_id"])
    assert result["remote_key_revoked"] is False
    assert result["remove_unbootstrapped_bare_key_in_Vast_UI"] is True
    with pytest.raises(RescueError, match="revoked"):
        rescue.key(key["key_id"])


def test_manual_capture_uses_current_target_not_retired_source(rescue, formal, monkeypatch):
    p, _ = make_plan(rescue, formal, monkeypatch)
    state = rescue.journal("formal")
    state["active_target"] = {"plan_id": p["plan_id"]}
    state["generation"] = 2
    rescue.record("formal", state)
    source = LocalSource(formal[0], rescue.state / "remote-fixture")
    monkeypatch.setattr(rescue, "control_ssh", lambda _: source)
    monkeypatch.setattr(rescue, "source", lambda _: (_ for _ in ()).throw(AssertionError("must not contact old source")))
    assert rescue.capture("formal")["state"] == "unchanged_committed_recovery_point"


def test_checkpoint_epoch_regression_never_replaces_cold_head(rescue, formal, monkeypatch):
    spec = formal[0]; enroll_fixture(rescue, spec, monkeypatch)
    original = rescue.capture("formal")["commit_id"]
    cp = torch.load(formal[4], weights_only=True); cp["epoch"] = 2; torch.save(cp, formal[4])
    (formal[4].parent / "epoch_curve.jsonl").write_text('{"epoch":1}\n{"epoch":2}\n')
    with pytest.raises(RescueError, match="regressed"):
        rescue.capture("formal")
    assert rescue.journal("formal")["latest"]["commit_id"] == original


def test_runtime_changed_between_capture_and_enrollment_rejected(tmp_path):
    prefix = tmp_path / "runtime"; prefix.mkdir(); (prefix / "payload").write_bytes(b"original")
    archive = tmp_path / "closed.tar.gz"; freeze_runtime(prefix, archive)
    proof = archive.with_suffix(".gz.json")
    assert verify_runtime_capsule(prefix, proof)["state"] == "source_runtime_still_matches_closed_capsule"
    (prefix / "payload").write_bytes(b"changed")
    with pytest.raises(ValueError, match="bytes changed"):
        verify_runtime_capsule(prefix, proof)


def test_target_preflight_catches_bad_image_before_transfer(monkeypatch, tmp_path):
    import platform
    request = {"runtime_platform": {"system": "Linux", "machine": "NOT_THIS_ARCH", "libc": list(platform.libc_ver())},
        "runtime_prefix": str(tmp_path / "new-env"), "archive_sha256": "a" * 64, "minimum_free_bytes": 1}
    with pytest.raises(ValueError, match="incompatible"):
        target_preflight(request)


def test_new_target_gpu_fault_stops_renewal_without_automatic_second_migration(rescue, formal, monkeypatch):
    p, _ = make_plan(rescue, formal, monkeypatch)
    state = rescue.journal("formal")
    state.update(generation=2, state="recovering", target_identity={"boot_id": "fixture", "instance_id": "123"},
                 active_target={"plan_id": p["plan_id"], "baseline_epochs": {"train_2024/checkpoint_last.pt": 3}})
    rescue.record("formal", state)
    source = LocalSource(formal[0], rescue.state / "remote-fixture")
    original = source.adapter
    source.adapter = lambda action, *a, **kw: {"state": "gpu_fault", "devices": [],
        "node_identity": state["target_identity"]} if action == "health" else original(action, *a, **kw)
    monkeypatch.setattr(rescue, "control_ssh", lambda _: source)
    result = rescue.watch_target("formal")
    assert result["state"] == "awaiting_user_confirmation" and result["automatic_migration"] is False
    assert rescue.journal("formal")["state"] == "awaiting_user_confirmation"
    with pytest.raises(RescueError, match="stopped/fenced"):
        rescue.renew_target("formal", state)
