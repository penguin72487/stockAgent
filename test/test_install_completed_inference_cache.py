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


@pytest.mark.parametrize("damage", [None, "no-full-model", "no-projection", "wrong-profile"])
def test_projection_installation_requires_full_model_and_complete_signal_oracles(damage):
    record = qualified_record()
    for row in record["full_forward_controls"]:
        row.update(whole_output_bitwise_equal=False,
                   projection_contract=installation.signal_engine._COMPLETED_SIGNAL_PROJECTION,
                   complete_signal_projection_bitwise_equal=True,
                   full_model_output_bitwise_equal=True if row["model_calls"] else None)
    if damage == "no-full-model":
        record["full_forward_controls"][0]["full_model_output_bitwise_equal"] = None
    elif damage == "no-projection":
        record["full_forward_controls"][1]["complete_signal_projection_bitwise_equal"] = False
    elif damage == "wrong-profile":
        record["full_forward_controls"][1]["projection_contract"] = "unknown_profile"
    if damage:
        with pytest.raises(ValueError):
            installation.qualification(record, market="market", panel_date="2026-10-14")
    else:
        assert installation.qualification(record, market="market", panel_date="2026-10-14")


@pytest.mark.parametrize("damage", [None, "default", "retained", "inventory", "extra-oracle", "policy"])
def test_metadata_only_installation_requires_separate_full_and_lean_model_oracles(damage):
    record = qualified_record()
    for row in record["full_forward_controls"]:
        first = row["model_calls"] == 1
        row.update(whole_output_bitwise_equal=False,
                   projection_contract=installation.signal_engine._COMPLETED_SIGNAL_PROJECTION,
                   complete_signal_projection_bitwise_equal=True,
                   full_model_output_bitwise_equal=None,
                   candle_aux_policy=installation.signal_engine._COMPLETED_CANDLE_AUX_POLICY,
                   oracle_model_calls=int(first),
                   full_default_model_output_bitwise_equal=True if first else None,
                   non_candle_output_bitwise_equal=True if first else None,
                   omitted_candle_fields=["candle_embedding"] if first else [])
    first, second = record["full_forward_controls"]
    if damage == "default":
        first["full_default_model_output_bitwise_equal"] = None
    elif damage == "retained":
        first["non_candle_output_bitwise_equal"] = None
    elif damage == "inventory":
        first["omitted_candle_fields"] = []
    elif damage == "extra-oracle":
        second["oracle_model_calls"] = 1
    elif damage == "policy":
        second["candle_aux_policy"] = "unknown"
    if damage:
        with pytest.raises(ValueError):
            installation.qualification(record, market="market", panel_date="2026-10-14")
    else:
        assert installation.qualification(record, market="market", panel_date="2026-10-14")


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
