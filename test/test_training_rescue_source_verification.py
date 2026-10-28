"""Long cold readback must leave checkpoint preservation available.

The packed network/storage boundary is a fixture. Checkpoint sealing, D CAS,
owner locks and concurrent journal commits use the actual CPU implementations.
"""
import copy
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import torch
import threading

from stockagent.training_rescue import RescueError
from stockagent.training_rescue_worker import atomic_json
from test_training_rescue import formal, rescue, enroll_fixture


@pytest.fixture
def readback_case(rescue, formal, monkeypatch, tmp_path):
    spec = formal[0]
    spec["data_sources"] = [{"dataset": "fixture-originals", "snapshot_id": "fixed-originals",
                             "manifest_sha256": "a" * 64,
                             "materialized_root": str(tmp_path / "raw-originals")}]
    enroll_fixture(rescue, spec, monkeypatch)
    rescue.capture(spec["run_id"])
    import stockagent.data_sync.packed_snapshots as packed
    monkeypatch.setattr(packed, "resolve_packed_snapshot_id", lambda *_: SimpleNamespace(
        manifest_sha256="a" * 64, manifest={"source": {"files": 1}}))
    return rescue, spec, formal, packed


def test_full_decode_allows_a_real_new_checkpoint_commit(readback_case, monkeypatch):
    owner, spec, formal, packed = readback_case
    run = spec["run_id"]
    before = owner.journal(run)["latest"]["commit_id"]
    committed = {}

    def verify(*args, **kwargs):
        assert kwargs["reconstruct_all"]
        with owner.lock():
            payload = torch.load(formal[4], weights_only=True)
            payload["epoch"] = 4
            torch.save(payload, formal[4])
            curve = formal[4].with_name("epoch_curve.jsonl")
            with curve.open("a") as stream:
                stream.write(json.dumps({"epoch": 4}) + "\n")
            atomic_json(Path(spec["output_root"]) / "progress.json", {
                "state": "training", "epoch": {"current": 4}})
            committed.update(owner.capture(run))
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    result = owner.verify_sources(run)
    latest = owner.journal(run)
    assert committed["commit_id"] != before
    assert latest["latest"]["commit_id"] == committed["commit_id"]
    assert latest["latest"]["checkpoints"]["train_2024/checkpoint_last.pt"]["epoch"] == 4
    assert latest["input_verification"]["id"] == Path(result["receipt"]).stem
    preserved = json.loads(Path(result["receipt"]).read_bytes())
    assert preserved["hot_materialization_created"] is False


def test_reference_hashing_also_releases_the_global_owner(readback_case, monkeypatch):
    owner, spec, _, packed = readback_case
    actual = owner.prepared_reference_objects
    called = []

    def references(current):
        with owner.lock():
            called.append(True)
        return actual(current)

    monkeypatch.setattr(owner, "prepared_reference_objects", references)
    monkeypatch.setattr(packed, "verify_packed_snapshot", lambda *a, **k: {
        "independently_reconstructed_files": 1})
    owner.verify_sources(spec["run_id"])
    assert called


@pytest.mark.parametrize("change", ["enrollment", "recipe"])
def test_changed_input_during_decode_cannot_publish_readiness(readback_case, monkeypatch, change):
    owner, spec, _, packed = readback_case
    run = spec["run_id"]

    def verify(*args, **kwargs):
        with owner.lock():
            if change == "enrollment":
                other = copy.deepcopy(spec)
                other["global_batch"] *= 2
                atomic_json(owner.path(run) / "spec.json", other)
            else:
                from stockagent.training_rescue_worker import PREPARED_BUILD_POLICY
                other = copy.deepcopy(spec)
                other["prepared_build_policy"] = PREPARED_BUILD_POLICY
                other["data_sources"][0]["manifest_sha256"] = "b" * 64
                owner.register_input_recipe(other)
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    with pytest.raises(RescueError, match="changed.*source verification"):
        owner.verify_sources(run)
    assert not owner.journal(run).get("input_verification")
    assert not list((owner.cold / "source-verification").glob("**/*.json"))


def test_duplicate_run_readback_is_rejected_without_blocking_checkpoint_owner(readback_case, monkeypatch):
    owner, spec, _, packed = readback_case
    run = spec["run_id"]
    observed = []

    def verify(*args, **kwargs):
        with pytest.raises(BlockingIOError):
            owner.verify_sources(run)
        with owner.lock():
            observed.append(owner.journal(run)["latest"]["commit_id"])
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    owner.verify_sources(run)
    assert len(observed) == 1


def test_commit_waits_for_a_short_capture_without_repeating_decode(readback_case, monkeypatch):
    owner, spec, _, packed = readback_case
    busy, release = threading.Event(), threading.Event()
    calls = []
    owners = []

    def capture_owner():
        with owner.lock():
            busy.set()
            assert release.wait(5)

    def verify(*args, **kwargs):
        calls.append(True)
        thread = threading.Thread(target=capture_owner)
        owners.append(thread)
        thread.start()
        assert busy.wait(5)
        timer = threading.Timer(0.1, release.set)
        owners.append(timer)
        timer.start()
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    try:
        owner.verify_sources(spec["run_id"])
    finally:
        release.set()
        for thread in owners:
            thread.join(timeout=5)
            assert not thread.is_alive()
    assert len(calls) == 1
    assert owner.journal(spec["run_id"])["input_verification"]["id"]


def test_decode_failure_releases_the_run_claim_and_preserves_checkpoint(readback_case, monkeypatch):
    owner, spec, _, packed = readback_case
    run = spec["run_id"]
    before = owner.journal(run)

    def corrupt(*args, **kwargs):
        raise ValueError("original SHA differs")

    monkeypatch.setattr(packed, "verify_packed_snapshot", corrupt)
    with pytest.raises(ValueError, match="SHA differs"):
        owner.verify_sources(run)
    assert owner.journal(run) == before
    monkeypatch.setattr(packed, "verify_packed_snapshot", lambda *a, **k: {
        "independently_reconstructed_files": 1})
    owner.verify_sources(run)
    assert owner.journal(run)["latest"] == before["latest"]


def test_cold_control_changed_after_hash_cannot_publish_readiness(readback_case, monkeypatch, tmp_path):
    owner, spec, _, packed = readback_case
    raw = b'{"quarantine":["original-contract-day"]}'
    sha = hashlib.sha256(raw).hexdigest()
    spec["input_controls"] = [{"path": str(tmp_path / "historical-policy.json"),
                               "sha256": sha, "bytes": len(raw)}]
    atomic_json(owner.path(spec["run_id"]) / "spec.json", spec)
    obj = owner.object_path(sha)
    obj.parent.mkdir(parents=True, exist_ok=True)
    obj.write_bytes(raw)

    def verify(*args, **kwargs):
        obj.write_bytes(b"x" * len(raw))
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    with pytest.raises(RescueError, match="changed.*source verification"):
        owner.verify_sources(spec["run_id"])
    assert not owner.journal(spec["run_id"]).get("input_verification")


def test_replaced_readback_claim_inode_cannot_publish_readiness(readback_case, monkeypatch):
    owner, spec, _, packed = readback_case

    def verify(*args, **kwargs):
        claim = owner.state / "source-verification-locks" / (spec["run_id"] + ".lock")
        claim.unlink()
        claim.touch(mode=0o600)
        return {"independently_reconstructed_files": 1}

    monkeypatch.setattr(packed, "verify_packed_snapshot", verify)
    with pytest.raises(ValueError, match="changed|identity|links"):
        owner.verify_sources(spec["run_id"])
    assert not owner.journal(spec["run_id"]).get("input_verification")


def test_cli_source_readback_uses_its_own_short_owner_transactions(monkeypatch):
    from scripts import training_rescue as cli
    held = []

    class Owner:
        def __init__(self, policy):
            pass

        @contextmanager
        def lock(self, **kwargs):
            held.append(True)
            try:
                yield
            finally:
                held.pop()

        def verify_sources(self, run, *, owner_wait_seconds):
            assert not held
            assert run == "fixture" and owner_wait_seconds == 7
            return {"state": "fixture_full_decode"}

    monkeypatch.setattr(cli, "Rescue", Owner)
    monkeypatch.setattr(cli, "load_policy", lambda _: {})
    monkeypatch.setattr(cli.sys, "argv", ["training_rescue.py", "--owner-wait-seconds", "7",
                                         "verify-sources", "--run", "fixture"])
    assert cli.main() == 0
