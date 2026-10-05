from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

import stockagent.data_sync.training_return as returns
from stockagent.data_sync.artifact_maintenance import automatic_dataset_name
from stockagent.data_sync.cold_artifacts import ColdArtifactSpec, publish_cold_artifact
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot, initialize_packed_layout
from stockagent.training.lifecycle import TrainingRunLifecycle
from test_training_lifecycle import _write_minimal_completed_artifacts


@pytest.fixture
def example(tmp_path, monkeypatch):
    artifact = tmp_path / "artifacts"
    relative = "markets/complete-run"
    source = artifact / relative
    lifecycle = TrainingRunLifecycle(source, execution_mode="naive", run_mode="train",
                                     strategy="none", model_name="transformer_base_portfolio")
    lifecycle.start(fold_ids=[1], dataset_fingerprint="exact-source", configuration_fingerprint="exact-config")
    lifecycle.start_group(group_name="train_2020-2021", group_index=1, group_total=1, fold_ids=[1], epoch_total=1)
    lifecycle.finish_fold(1)
    _write_minimal_completed_artifacts(lifecycle.layout, group_name="train_2020-2021", fold_id=1, execution_mode="naive")
    lifecycle.complete(fold_ids=[1])
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="penguin")
    spec = ColdArtifactSpec(automatic_dataset_name(relative), relative, None, 64, 4, 0, "training-lifecycle-v1")
    release = publish_cold_artifact(cold, artifact, spec)
    ack = returns.make_ack(cold, release, relative_root=relative, origin="vastai1T")
    policy = returns.load_policy(Path("configs/data_sync/training_return.json"))
    policy["stable_hours"] = 0
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setattr(returns, "artifact_process_references", lambda *args: [])
    monkeypatch.setattr(returns, "artifact_service_references", lambda *args: {})
    return artifact, source, state, cold, release, ack, policy


def test_complete_return_reconstructs_after_exact_source_cleanup(example, tmp_path):
    artifact, source, state, cold, release, ack, policy = example
    input_source = tmp_path / "unique-source.parquet"
    input_source.write_bytes(b"untouched original")
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert not plan["blockers"] and source.exists()
    result = returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert result["deleted"] and not source.exists()
    assert input_source.read_bytes() == b"untouched original"
    restored = fetch_packed_snapshot(cold, tmp_path / "restored", release)
    assert (restored / "fold_01/checkpoint_best.pt").is_file()
    assert returns.inventory(restored)["rows"]


def test_same_size_changed_source_fails_content_ack(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    path = source / "fold_01/checkpoint_best.pt"
    path.write_bytes(b"mutation")  # Same size as the fixture payload.
    with pytest.raises(SnapshotError, match="differs from"):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists()


@pytest.mark.parametrize("stamp", ["expired", "future", True, float("nan"), float("inf")])
def test_stale_or_invalid_ack_never_retire_source(example, tmp_path, stamp):
    artifact, source, state, _, _, ack, policy = example
    if stamp == "expired":
        stamp = ack["recorded_at_epoch"] - returns.ACK_TTL_SECONDS - 10
    elif stamp == "future":
        stamp = ack["recorded_at_epoch"] + returns.ACK_FUTURE_SKEW_SECONDS + 120
    ack = {**ack, "recorded_at_epoch": stamp}
    ack["identity_sha256"] = returns.identity({k: v for k, v in ack.items() if k != "identity_sha256"})
    with pytest.raises(SnapshotError, match="not fresh"):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists() and not list(state.glob("retirement-*.json"))


def test_missing_plot_rejects_complete_label(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    (source / "fold_01/equity_curve.png").unlink()
    with pytest.raises(SnapshotError):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)


@pytest.mark.parametrize("kind", ["unknown", "symlink"])
def test_unknown_or_linked_source_is_preserved(example, tmp_path, kind):
    artifact, source, state, _, _, ack, policy = example
    path = source / "new-file"
    if kind == "unknown":
        path.write_bytes(b"unique new result")
    elif kind == "symlink":
        path.symlink_to(source / "fold_01/checkpoint_best.pt")
    with pytest.raises(SnapshotError):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists()


def test_exact_return_unlinks_only_its_names_and_preserves_external_hardlinks(example, tmp_path):
    artifact, source, state, cold, release, ack, policy = example
    alias = tmp_path / "external-checkpoint"
    original = source / "fold_01/checkpoint_best.pt"
    contents = original.read_bytes()
    os.link(original, alias)
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert plan["shared_file_names"] == 1
    all_allocated = sum(p.stat().st_blocks*512 for p in source.rglob("*") if p.is_file())
    assert plan["allocated_bytes"] == all_allocated - original.stat().st_blocks*512
    result = returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert result["deleted"] and result["external_shared_names_deleted"] is False
    assert alias.read_bytes() == contents and alias.stat().st_nlink == 1
    restored = fetch_packed_snapshot(cold, tmp_path / "recovered-shared", release)
    assert (restored / "fold_01/checkpoint_best.pt").read_bytes() == contents


def test_shared_names_inside_one_exact_return_are_unlinked_once_per_inode(example, tmp_path):
    artifact, source, state, _, _, _, policy = example
    os.link(source / "fold_01/checkpoint_best.pt", source / "extra-checkpoint.pt")
    cold = tmp_path / "with-internal-links"
    initialize_packed_layout(cold, node_id="penguin")
    spec = ColdArtifactSpec(automatic_dataset_name("markets/complete-run"), "markets/complete-run", None, 64, 4, 0,
                            "training-lifecycle-v1")
    release = publish_cold_artifact(cold, artifact, spec)
    ack = returns.make_ack(cold, release, relative_root="markets/complete-run", origin="vastai1T")
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert plan["shared_file_names"] == 2
    result = returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert result["deleted"] and not source.exists()
    restored = fetch_packed_snapshot(cold, tmp_path / "recovered-internal-links", release)
    assert (restored / "extra-checkpoint.pt").read_bytes() == (restored / "fold_01/checkpoint_best.pt").read_bytes()


def test_an_outside_process_holding_a_shared_inode_blocks_unlink(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    alias = tmp_path / "outside-open-checkpoint"
    os.link(source / "fold_01/checkpoint_best.pt", alias)
    process = subprocess.Popen([sys.executable, "-c",
        "import sys,time; f=open(sys.argv[1],'rb'); print('ready',flush=True); time.sleep(60)", str(alias)],
        stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "ready"
        plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
        assert "source-shared-inode-in-use" in plan["blockers"]
        with pytest.raises(SnapshotError, match="blocked"):
            returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
        assert source.exists() and alias.exists()
    finally:
        process.terminate()
        process.wait(timeout=5)
        process.stdout.close()


def test_a_shared_mmap_without_an_open_descriptor_blocks_unlink(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    alias = tmp_path / "outside-mapped-checkpoint"
    os.link(source / "fold_01/checkpoint_best.pt", alias)
    program = """import mmap,os,sys,time
f=open(sys.argv[1],'rb')
key=(os.fstat(f.fileno()).st_dev,os.fstat(f.fileno()).st_ino)
mapping=mmap.mmap(f.fileno(),0,access=mmap.ACCESS_READ)
for name in os.listdir('/proc/self/fd'):
    try:
        fd=int(name); info=os.fstat(fd)
        if (info.st_dev,info.st_ino)==key: os.close(fd)
    except OSError: pass
print('ready',flush=True)
time.sleep(60)
"""
    process = subprocess.Popen([sys.executable, "-c", program, str(alias)], stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == "ready"
        plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
        assert any(f"pid={process.pid}:maps-inode:" in r for r in plan["shared_inode_references"])
        with pytest.raises(SnapshotError, match="blocked"):
            returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
        assert source.exists()
    finally:
        process.terminate()
        process.wait(timeout=5)
        process.stdout.close()


def test_v1_control_acknowledgement_cannot_apply_the_v2_shared_name_policy(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    ack = {**ack, "contract": "durable_completed_training_return_v1"}
    ack["identity_sha256"] = returns.identity({k:v for k,v in ack.items() if k != "identity_sha256"})
    with pytest.raises(SnapshotError, match="acknowledgement"):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists()


def test_a_new_external_inode_name_invalidates_the_retirement_plan(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    os.link(source / "fold_01/checkpoint_best.pt", tmp_path / "unexpected-alias")
    with pytest.raises(SnapshotError, match="changed"):
        returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert source.exists()


@pytest.mark.parametrize("dependency", ["process", "service"])
def test_live_or_service_consumers_block_cleanup(example, tmp_path, monkeypatch, dependency):
    artifact, source, state, _, _, ack, policy = example
    if dependency == "process":
        monkeypatch.setattr(returns, "artifact_process_references", lambda *args: ["pid=123:fd"])
    else:
        monkeypatch.setattr(returns, "artifact_service_references", lambda *args: {str(source): ["deployed-model"]})
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    with pytest.raises(SnapshotError, match="blocked"):
        returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert source.exists()


def test_conflicted_consumer_parser_protects_the_verified_return(example, tmp_path, monkeypatch):
    artifact, source, state, _, _, ack, policy = example
    def invalid(*args):
        raise SyntaxError("unresolved merge", ("stockagent/config.py", 12, 1, "<<<<<<<"))
    monkeypatch.setattr(returns, "artifact_service_references", invalid)
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert plan["blockers"] == ["source-service-dependencies-unverifiable"]
    assert plan["service_dependency_error"] == {"error_type": "SyntaxError", "file_name": "config.py", "line": 12}
    with pytest.raises(SnapshotError, match="blocked"):
        returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert source.exists()


def test_changed_dry_run_is_preserved(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    path = source / "fold_01/checkpoint_best.pt"
    os.utime(path, ns=(path.stat().st_atime_ns, path.stat().st_mtime_ns+1))
    with pytest.raises(SnapshotError, match="changed"):
        returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert source.exists()


@pytest.mark.parametrize("field,value", [("authority_node_id", "lab203"), ("relative_root", "../sources"),
                                        ("logical_bytes", 0), ("cold_reconstruction_verified", False)])
def test_forged_or_different_ack_cannot_delete(example, tmp_path, field, value):
    artifact, source, state, _, _, ack, policy = example
    ack = {**ack, field: value}
    ack["identity_sha256"] = returns.identity({k:v for k,v in ack.items() if k != "identity_sha256"})
    with pytest.raises(SnapshotError):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists()


def test_unenrolled_policy_scope_is_rejected():
    policy = json.loads(Path("configs/data_sync/training_return.json").read_text())
    policy["scopes"] = ["sources"]
    with pytest.raises(SnapshotError):
        returns.load_policy(policy)


def test_redirected_quarantine_is_rejected_before_move(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    (state / "quarantine").symlink_to(tmp_path)
    plan = returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    with pytest.raises(SnapshotError, match="redirected"):
        returns.apply_retirement(artifact, state, tmp_path, ack, policy, plan["plan_fingerprint"])
    assert source.exists()


def test_permission_changes_do_not_match_the_durable_return(example, tmp_path):
    artifact, source, state, _, _, ack, policy = example
    (source / "fold_01/checkpoint_best.pt").chmod(0o400)
    with pytest.raises(SnapshotError, match="differs"):
        returns.plan_retirement(artifact, state, tmp_path, ack, policy)
    assert source.exists()


def test_network_workspace_is_rejected_before_transfer(tmp_path, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(returns.subprocess, "run", lambda *args, **kw: SimpleNamespace(stdout="cifs\n"))
    with pytest.raises(SnapshotError, match="native local filesystem"):
        returns.admit_workspace(tmp_path, 1)


def test_unattended_wsl_capacity_never_guesses_a_distribution(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from stockagent.data_sync import recovery_queue
    read_text = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda p, *a, **kw:
                        "microsoft" if str(p) == "/proc/sys/kernel/osrelease" else read_text(p, *a, **kw))
    monkeypatch.setattr(returns.subprocess, "run", lambda *args, **kw: SimpleNamespace(stdout="ext4\n"))
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
    seen = []
    def probe(config):
        seen.append(config["windows_distribution"])
        raise ValueError("actual WSL identity required")
    monkeypatch.setattr(recovery_queue, "physical_free_bytes", probe)
    with pytest.raises(ValueError, match="identity required"):
        returns.admit_workspace(tmp_path, 1)
    assert seen == [""]
