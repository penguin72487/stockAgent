import json

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import verify_shioaji_hash_default_policy as verify


SOURCE = b'from functools import lru_cache\nVALUE = "preserve"\n@lru_cache(maxsize=16384)\ndef cache(path, signature):\n    return path, signature, VALUE\n'


def test_normalized_module_ignores_only_measured_capacity():
    expected = verify.normalized_module(SOURCE, "cache")
    assert verify.normalized_module(SOURCE.replace(b"16384", b"131_072"), "cache") == expected
    assert verify.normalized_module(SOURCE.replace(b"preserve", b"changed unrelated global"), "cache") != expected
    assert verify.normalized_module(SOURCE.replace(b"return path, signature, VALUE", b"return path, signature"), "cache") != expected


@pytest.mark.parametrize("payload", [
    SOURCE.replace(b"maxsize=16384", b"maxsize=True"),
    SOURCE.replace(b"maxsize=16384", b"maxsize=float('inf')"),
    SOURCE.replace(b"maxsize=16384", b"maxsize=16384, typed=True"),
    SOURCE.replace(b"maxsize=16384", b"16384"),
    SOURCE.replace(b"@lru_cache(maxsize=16384)\n", b""),
    SOURCE + SOURCE,
])
def test_normalization_rejects_ambiguous_or_unmeasured_decorators(payload):
    with pytest.raises(ValueError, match="exactly one literal LRU capacity"):
        verify.normalized_module(payload, "cache")


def control(tmp_path):
    path = tmp_path / "control.json"
    value = {"state": "accepted_local_verified_hash_only", "all_tasks_and_summaries_equal": True,
             "medians": {"compact_fd": {"elapsed_seconds": 10, "cpu_seconds": 10, "peak_rss_bytes": 10},
                         "original": {"elapsed_seconds": 20, "cpu_seconds": 20, "peak_rss_bytes": 20},
                         "default_fd": {"elapsed_seconds": 15, "cpu_seconds": 15, "peak_rss_bytes": 15}}}
    atomic_write_json(path, value)
    return path, value


def test_selected_policy_needs_both_exact_output_and_measured_pareto_improvement(tmp_path):
    path, value = control(tmp_path)
    assert verify.control_value(path) == value
    value["medians"]["compact_fd"]["cpu_seconds"] = 21
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="improve all measured resource"):
        verify.control_value(path)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -1, 0, "fake timing"])
def test_invalid_measurement_is_never_policy_acceptance(tmp_path, number):
    path, value = control(tmp_path)
    value["medians"]["original"]["cpu_seconds"] = number
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="invalid complete-workflow"):
        verify.control_value(path)


@pytest.mark.parametrize("state,equal", [("failed", True), ("accepted_local_verified_hash_only", False)])
def test_unaccepted_or_partial_workload_cannot_select_production_policy(tmp_path, state, equal):
    path, value = control(tmp_path)
    value.update(state=state, all_tasks_and_summaries_equal=equal)
    atomic_write_json(path, value)
    with pytest.raises(ValueError, match="complete independent-reference"):
        verify.control_value(path)
    assert json.loads(path.read_bytes())["state"] == state
