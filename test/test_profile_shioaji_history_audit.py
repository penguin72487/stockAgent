import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import profile_shioaji_history_audit as profile


def controls(tmp_path, monkeypatch):
    manifest = {"fixture": "portable profile oracle"}
    expected = {"task_sha256": "all canonical tasks", "summary_sha256": ["full 1", "full 250", "full 500"]}
    source = tmp_path / "source.py"
    source.write_bytes(b"private original implementation")
    reference = tmp_path / "reference.json"
    atomic_write_json(tmp_path / "manifest.json", manifest)
    atomic_write_json(reference, {"state": "accepted_isolated_cache_policy_only", "fixture": manifest,
                                  "code_sha256": {str(source): sha256_file(source)}, "trials": [expected]})
    monkeypatch.setattr(profile.probe.audit, "bounded_new_path", lambda path: path)
    monkeypatch.setattr(profile.probe.audit, "fixture_fence", lambda *_args: "full fixture unchanged")
    monkeypatch.setattr(profile, "observe_node", lambda *_args: {"host": "portable test"})
    return reference, expected, source


@pytest.mark.parametrize("policy", ["original", "compact"])
def test_profile_preserves_full_oracle_and_labels_instrumentation(tmp_path, monkeypatch, policy):
    reference, expected, _ = controls(tmp_path, monkeypatch)

    def worker(fixture, selected):
        assert fixture == tmp_path and selected == policy
        return {**expected, "elapsed_seconds": 123, "provider_requests": 0}

    monkeypatch.setattr(profile.probe, "worker", worker)
    result = profile.profile(tmp_path, reference, tmp_path / "result.json", policy)
    assert result == json.loads((tmp_path / "result.json").read_bytes())
    assert result["state"] == "profile_diagnostic_only"
    assert result["instrumentation_overhead_included"] is True
    assert result["all_tasks_and_summaries_equal"] is True
    assert result["provider_requests"] == 0 and result["services_restarted"] is False
    assert result["total_calls"] > 0 and result["top_cumulative"]
    assert result["worker_result"]["task_sha256"] == expected["task_sha256"]


@pytest.mark.parametrize("state", ["failed", "running", "accepted_local_audit_only"])
def test_profile_rejects_unaccepted_reference_before_worker(tmp_path, monkeypatch, state):
    reference, _, _ = controls(tmp_path, monkeypatch)
    value = json.loads(reference.read_bytes())
    value["state"] = state
    atomic_write_json(reference, value)
    monkeypatch.setattr(profile.probe, "worker", lambda *_args: pytest.fail("unaccepted reference must not run"))
    with pytest.raises(ValueError, match="accepted exact memo workload"):
        profile.profile(tmp_path, reference, tmp_path / "result.json", "compact")


@pytest.mark.parametrize("boundary", ["tasks", "summaries", "source", "code"])
def test_profile_rejects_output_or_source_drift(tmp_path, monkeypatch, boundary):
    reference, expected, source = controls(tmp_path, monkeypatch)
    if boundary == "source":
        fences = iter(("before", "after"))
        monkeypatch.setattr(profile.probe.audit, "fixture_fence", lambda *_args: next(fences))

    def worker(*_args):
        if boundary == "code":
            source.write_bytes(b"changed code during full audit")
        return {**expected,
                "task_sha256": "wrong task" if boundary == "tasks" else expected["task_sha256"],
                "summary_sha256": ["wrong summary"] if boundary == "summaries" else expected["summary_sha256"]}

    monkeypatch.setattr(profile.probe, "worker", worker)
    with pytest.raises(AssertionError):
        profile.profile(tmp_path, reference, tmp_path / "result.json", "compact")
    assert not (tmp_path / "result.json").exists()
