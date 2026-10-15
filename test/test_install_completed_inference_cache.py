from copy import deepcopy

import pytest

from scripts import install_completed_inference_cache as installation


def qualified_record():
    controls = [{"model_calls": calls, "whole_output_bitwise_equal": True,
                 "candidate_key": "complete-new-key"} for calls in (1, 0)]
    samples = [{"stages": {"completed_forward_cache": {
        "key": "complete-new-key", "contract": installation.cache.CONTRACT}}} for _ in controls]
    samples[0].update(gpu_allocated_after_release_bytes=0, gpu_reserved_after_release_bytes=0)
    return {"contract": "canonical_live_inference_benchmark_v1", "market": "market",
            "panel_date": "2026-10-14", "exact_output_parity": True,
            "production_writes": False, "network_requests": 0, "completed_session_cache": True,
            "full_forward_controls": controls, "samples": samples}


def test_qualification_requires_real_new_forward_then_complete_warm_hit():
    assert installation.qualification(qualified_record(), market="market",
                                      panel_date="2026-10-14") == "complete-new-key"


@pytest.mark.parametrize("field,value", [
    ("contract", "completed_forward_identity_observer_v1"), ("market", "other"),
    ("panel_date", "2026-10-13"), ("exact_output_parity", False),
    ("production_writes", True), ("network_requests", 1), ("completed_session_cache", False),
])
def test_installation_refuses_non_independent_or_wrong_scope_receipt(field, value):
    record = {**qualified_record(), field: value}
    with pytest.raises(ValueError):
        installation.qualification(record, market="market", panel_date="2026-10-14")


@pytest.mark.parametrize("change", ["no-model", "repeated-model", "partial-aux", "wrong-key", "gpu-retained"])
def test_installation_rejects_invalid_compute_reuse_or_release_proof(change):
    record = deepcopy(qualified_record())
    if change == "no-model":
        record["full_forward_controls"][0]["model_calls"] = 0
    elif change == "repeated-model":
        record["full_forward_controls"][1]["model_calls"] = 1
    elif change == "partial-aux":
        record["full_forward_controls"][0]["whole_output_bitwise_equal"] = False
    elif change == "wrong-key":
        record["samples"][1]["stages"]["completed_forward_cache"]["key"] = "old"
    else:
        record["samples"][0]["gpu_allocated_after_release_bytes"] = 4
    with pytest.raises(ValueError):
        installation.qualification(record, market="market", panel_date="2026-10-14")
