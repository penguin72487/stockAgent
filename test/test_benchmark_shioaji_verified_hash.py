import json
import os
import stat

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import benchmark_shioaji_verified_hash as benchmark


def captured(tmp_path, monkeypatch):
    original = tmp_path / "repair.py"
    original.write_text(
        'from functools import lru_cache\n'
        '@lru_cache(maxsize=16384)\n'
        'def _signature_sha(path, signature):\n    return "independently saved original"\n'
        'def verified_sha(path):\n    return _signature_sha(str(path), ())\n'
    )
    monkeypatch.setattr(benchmark, "REPAIR", original)
    monkeypatch.setattr(benchmark.audit, "bounded_new_path", lambda path: path)
    fixture, bundle = tmp_path / "fixture", tmp_path / "reference"
    manifest = {"fixture": "portable frozen full workload"}
    expected = {"task_sha256": "full tasks", "summary_sha256": ["full 1", "full 250", "full 500"],
                "pending_queries": 7}
    accepted = tmp_path / "accepted.json"
    atomic_write_json(fixture / "manifest.json", manifest)
    atomic_write_json(accepted, {"state": "accepted_isolated_cache_policy_only", "fixture": manifest,
                                "code_sha256": {str(original): sha256_file(original)}, "trials": [expected]})
    proof = benchmark.prepare_reference(fixture, accepted, bundle)
    return fixture, bundle, accepted, original, proof


def test_original_reference_is_bound_copied_and_private(tmp_path, monkeypatch):
    fixture, bundle, _, original, proof = captured(tmp_path, monkeypatch)
    assert proof == benchmark.reference_proof(fixture, bundle)
    assert (bundle / "repair_reference.py").read_bytes() == original.read_bytes()
    assert (bundle / "repair_reference.py").stat().st_ino != original.stat().st_ino
    assert stat.S_IMODE((bundle / "repair_reference.py").stat().st_mode) == 0o600
    assert stat.S_IMODE((bundle / "reference.json").stat().st_mode) == 0o600
    # Candidate changes must not silently rebuild the independently saved old
    # implementation or its accepted expected outputs.
    original.write_text("candidate changed after capture")
    assert benchmark.reference_proof(fixture, bundle) == proof
    assert benchmark.load_original(bundle).verified_sha(None) == "independently saved original"


@pytest.mark.parametrize("boundary", ["source", "accepted", "fixture", "expected", "declared_hash"])
def test_reference_identity_drift_is_rejected(tmp_path, monkeypatch, boundary):
    fixture, bundle, accepted, _, _ = captured(tmp_path, monkeypatch)
    if boundary in {"expected", "declared_hash"}:
        proof = json.loads((bundle / "reference.json").read_bytes())
        if boundary == "expected":
            proof["expected"]["pending_queries"] = 999
        else:
            (bundle / "repair_reference.py").write_bytes(b"fake independently saved implementation")
            proof["repair_sha256"] = sha256_file(bundle / "repair_reference.py")
        atomic_write_json(bundle / "reference.json", proof)
    else:
        target = {"source": bundle / "repair_reference.py", "accepted": accepted,
                  "fixture": fixture / "manifest.json"}[boundary]
        target.write_text('{}')
    with pytest.raises(ValueError, match="identity changed"):
        benchmark.reference_proof(fixture, bundle)


@pytest.mark.parametrize("policy", tuple(benchmark.POLICIES))
def test_worker_uses_independent_original_or_candidate_then_restores(tmp_path, monkeypatch, policy):
    fixture, bundle, _, _, proof = captured(tmp_path, monkeypatch)
    collector, repair = benchmark.audit.current, benchmark.audit.repair
    original_receipt, original_sha, original_verified = (
        collector._valid_receipt_at_signature, repair._signature_sha, collector.verified_sha
    )
    source = tmp_path / "bytes"
    source.write_bytes(b"actual source")

    def worker(selected, variant):
        assert selected == fixture and variant == "candidate"
        assert collector._valid_receipt_at_signature.cache_info().maxsize == benchmark.POLICIES[policy][0]
        expected = "independently saved original" if policy == "original" else sha256_file(source)
        assert collector.verified_sha(source) == expected
        return {**proof["expected"]}

    monkeypatch.setattr(benchmark.audit, "worker", worker)
    result = benchmark.worker(fixture, bundle, policy)
    assert result["policy"] == policy and result["hash_cache"]["maxsize"] == benchmark.POLICIES[policy][1]
    assert collector._valid_receipt_at_signature is original_receipt
    assert repair._signature_sha is original_sha and collector.verified_sha is original_verified


def test_worker_failure_restores_validators_and_preserves_oracle(tmp_path, monkeypatch):
    fixture, bundle, _, _, _ = captured(tmp_path, monkeypatch)
    collector, repair = benchmark.audit.current, benchmark.audit.repair
    before = collector._valid_receipt_at_signature, repair._signature_sha, collector.verified_sha
    monkeypatch.setattr(benchmark.audit, "worker", lambda *_args: {"task_sha256": "wrong"})
    with pytest.raises(AssertionError, match="oracle changed"):
        benchmark.worker(fixture, bundle, "original")
    assert before == (collector._valid_receipt_at_signature, repair._signature_sha, collector.verified_sha)


def test_failed_compare_persists_failure_without_accepting_partial_work(tmp_path, monkeypatch):
    fixture, bundle, _, original, _ = captured(tmp_path, monkeypatch)
    monkeypatch.setattr(benchmark, "MUTABLE", {str(original)})
    monkeypatch.setattr(benchmark.audit, "fixture_fence", lambda *_args: "fixture fence")
    monkeypatch.setattr(benchmark, "observe_node", lambda *_args: {"host": "portable test"})
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError()))
    with pytest.raises(TimeoutError):
        benchmark.compare(fixture, bundle, tmp_path / "failure.json")
    result = json.loads((tmp_path / "failure.json").read_bytes())
    assert result["state"] == "failed" and result["failure_type"] == "TimeoutError"
    assert result["all_tasks_and_summaries_equal"] is False
    assert not result["trials"] and result["services_restarted"] is False
    assert os.path.exists(bundle / "repair_reference.py")
