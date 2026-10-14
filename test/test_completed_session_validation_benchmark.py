"""A changed source or partial ABBA cannot be promoted to a speedup claim."""
import json

import pytest

from scripts import benchmark_completed_session_validation as benchmark
from scripts import finalize_tw_public_completed_session as finalizer


def test_actual_source_generation_change_is_denied_before_validation_or_build(tmp_path, monkeypatch):
    source = benchmark.SOURCE.read_bytes()
    (tmp_path / "original.py").write_bytes(source)
    fixture = {
        "contract": benchmark.CONTRACT,
        "original_code_sha256": benchmark.sha(source),
        "candidate_code_sha256": benchmark.sha(source),
        "live_root": str(tmp_path / "live"), "publications": str(tmp_path / "publications"),
        "expected_date": "2026-10-14", "source_inputs": [{"path": "source", "bytes": 10}],
        "implementation": {"generation": "fixed"},
        "implementation_sha256": benchmark.sha(benchmark.json_bytes({"generation": "fixed"})),
    }
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    monkeypatch.setattr(benchmark, "inputs", lambda *a: [{"path": "source", "bytes": 11}])
    monkeypatch.setattr(benchmark, "implementation_identity", lambda: fixture["implementation"])

    def forbidden(*a, **k):
        raise AssertionError("changed-source trial reached validation or main")

    monkeypatch.setattr(finalizer, "main", forbidden)
    monkeypatch.setattr(finalizer, "_derived_state", forbidden)
    output = tmp_path / "result.json"
    with pytest.raises(RuntimeError, match="source inputs changed before"):
        benchmark.worker(tmp_path, "candidate", output)
    assert not output.exists()


@pytest.mark.parametrize("changed", ["dependency", "runtime"])
def test_full_dependency_or_runtime_change_cannot_reuse_captured_fixture(tmp_path, monkeypatch, changed):
    implementation = {"code_sha256": "before", "runtime": "before"}
    fixture = {
        "contract": benchmark.CONTRACT, "implementation": implementation,
        "implementation_sha256": benchmark.sha(benchmark.json_bytes(implementation)),
    }
    (tmp_path / "fixture.json").write_text(json.dumps(fixture))
    current = {**implementation, "code_sha256" if changed == "dependency" else "runtime": "after"}
    monkeypatch.setattr(benchmark, "implementation_identity", lambda: current)

    def forbidden(*args, **kwargs):
        raise AssertionError("changed implementation reached main or source validation")

    monkeypatch.setattr(finalizer, "main", forbidden)
    monkeypatch.setattr(finalizer, "_derived_state", forbidden)
    output = tmp_path / "result.json"
    with pytest.raises(RuntimeError, match="dependency or runtime generation changed"):
        benchmark.worker(tmp_path, "candidate", output)
    assert not output.exists()


def test_old_incomplete_fixture_is_preserved_not_relabelled(tmp_path):
    original = json.dumps({"contract": "completed_session_validation_abba_v1"})
    fixture = tmp_path / "fixture.json"
    fixture.write_text(original)
    output = tmp_path / "new-result.json"
    with pytest.raises(RuntimeError, match="fresh v2 fixture"):
        benchmark.worker(tmp_path, "candidate", output)
    assert fixture.read_text() == original
    assert not output.exists()


@pytest.mark.parametrize("changed", ["contract", "implementation_sha256"])
def test_abba_different_dependency_or_contract_generations_cannot_merge(tmp_path, changed):
    paths = []
    for number, variant in enumerate(("original", "candidate", "candidate", "original")):
        row = {
            "state": "completed_trial", "variant": variant, "provider_calls": 0,
            "canonical_writes": False, "cuda_initialized": False, "contract": benchmark.CONTRACT,
            "implementation_sha256": "fixed", "semantic_receipt_sha256": "fixed",
            "source_inputs_sha256": "fixed", "original_code_sha256": "fixed",
            "candidate_code_sha256": "fixed",
        }
        if number == 3:
            row[changed] = "different"
        path = tmp_path / f"trial-{number}.json"
        path.write_text(json.dumps(row))
        paths.append(path)
    output = tmp_path / "result.json"
    with pytest.raises(RuntimeError, match="contract|generation mismatch"):
        benchmark.aggregate(paths, output)
    assert not output.exists()


def test_missing_fourth_trial_does_not_publish_partial_abba_acceptance(tmp_path):
    paths = [tmp_path / f"trial-{number}.json" for number in range(4)]
    for path in paths[:3]:
        path.write_text("{}")
    output = tmp_path / "result.json"
    with pytest.raises(FileNotFoundError):
        benchmark.aggregate(paths, output)
    assert not output.exists()


@pytest.mark.parametrize("field,value", [
    ("state", "failed"), ("provider_calls", 1),
    ("canonical_writes", True), ("cuda_initialized", True),
])
def test_incomplete_or_stateful_trials_cannot_claim_full_validation_acceptance(tmp_path, field, value):
    paths = []
    for number, variant in enumerate(("original", "candidate", "candidate", "original")):
        row = {"state": "completed_trial", "variant": variant,
               "provider_calls": 0, "canonical_writes": False, "cuda_initialized": False}
        if number == 3:
            row[field] = value
        path = tmp_path / f"trial-{number}.json"
        path.write_text(json.dumps(row))
        paths.append(path)
    output = tmp_path / "result.json"
    with pytest.raises(RuntimeError, match="complete, read-only and CUDA-cold"):
        benchmark.aggregate(paths, output)
    assert not output.exists()
