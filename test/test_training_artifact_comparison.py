"""Performance receipts must detect changed state, prices, dtypes and bits."""
import numpy as np
import pytest
import torch

from scripts.compare_training_artifacts import checkpoint_differences, exact_differences
from stockagent.training.checkpoint_contract import _stable_fingerprint


def test_comparison_preserves_bfloat16_and_optimizer_rng_bits():
    value = {"optimizer": {1: {"exp_avg": torch.tensor([1, 2], dtype=torch.bfloat16)}},
             "rng": torch.tensor([0, 255], dtype=torch.uint8)}
    assert exact_differences(value, value) == []
    other = {"optimizer": {1: {"exp_avg": torch.tensor([1, 3], dtype=torch.bfloat16)}},
             "rng": value["rng"]}
    assert exact_differences(value, other) == ["value.optimizer.1.exp_avg: tensor bits differ"]


def test_comparison_rejects_price_change_and_float_dtype_change():
    prices = np.array([100.0, 100.5], dtype=np.float64)
    assert exact_differences(prices, prices.copy()) == []
    assert "bits differ" in exact_differences(prices, np.array([100, 101], dtype=np.float64))[0]
    assert "dtype differs" in exact_differences(prices, prices.astype(np.float32))[0]


def test_comparison_rejects_signed_zero_tensor_and_missing_state():
    assert exact_differences(torch.tensor([0.0]), torch.tensor([-0.0]))
    assert exact_differences({"fee": 0.001}, {})
    assert exact_differences([1], [True])


def test_comparison_refuses_unsupported_pickle_values():
    with pytest.raises(TypeError, match="unsupported"):
        exact_differences(object(), object())


def _checkpoint(*, threads=112, output="control", fee=0.001, weight=1.0):
    configuration = {"environment": {"cpu_threads": threads, "seed": 7},
                     "runner": {"output_dir": output}, "trading": {"fee": fee}}
    return {"model": {"weight": torch.tensor([weight])},
            "experiment_manifest": {"configuration": configuration,
                                    "configuration_fingerprint": _stable_fingerprint(configuration),
                                    "fingerprints": {"execution": "same-contract"}}}


def test_runtime_profile_admission_is_explicit_and_records_verified_changes():
    left, right = _checkpoint(), _checkpoint(threads=16, output="candidate")
    assert checkpoint_differences(left, right, path="checkpoint")[0]
    differences, admitted = checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True,
    )
    assert differences == []
    assert {row["field"] for row in admitted} == {
        "configuration.environment.cpu_threads", "configuration.runner.output_dir",
        "configuration_fingerprint",
    }
    assert admitted[-1]["verified_from_configuration"] is True


@pytest.mark.parametrize("changed", [
    {"threads": 16, "fee": 0.002}, {"threads": 16, "weight": 2.0},
])
def test_runtime_profile_admission_rejects_financial_or_model_changes(changed):
    differences, _ = checkpoint_differences(
        _checkpoint(), _checkpoint(**changed), path="checkpoint",
        allow_runtime_profile_differences=True,
    )
    assert differences


def test_runtime_profile_admission_rejects_tampered_fingerprint_and_boolean_budget():
    right = _checkpoint(threads=16)
    right["experiment_manifest"]["configuration_fingerprint"] = "tampered"
    differences, admitted = checkpoint_differences(
        _checkpoint(), right, path="checkpoint", allow_runtime_profile_differences=True,
    )
    assert "fingerprint invalid" in differences[0] and admitted == []
    assert checkpoint_differences(
        _checkpoint(), _checkpoint(threads=True), path="checkpoint",
        allow_runtime_profile_differences=True,
    )[0]


def test_runtime_replay_schedule_admission_still_requires_exact_optimizer_and_finance():
    left, right = _checkpoint(), _checkpoint(output="candidate")
    for checkpoint, chunk in ((left, 32), (right, 8)):
        manifest = checkpoint["experiment_manifest"]
        manifest["configuration"]["training"] = {"eval_backtest_chunk_rows": chunk, "batch_size_train": 32}
        manifest["configuration_fingerprint"] = _stable_fingerprint(manifest["configuration"])
    differences, admitted = checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True)
    assert differences == []
    assert any(row["field"] == "configuration.training.eval_backtest_chunk_rows" for row in admitted)
    manifest = right["experiment_manifest"]
    manifest["configuration"]["training"]["batch_size_train"] = 16
    manifest["configuration_fingerprint"] = _stable_fingerprint(manifest["configuration"])
    assert checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True)[0]


def test_runtime_replay_schedule_rejects_boolean_and_one_sided_knob():
    left, right = _checkpoint(), _checkpoint()
    manifest = right["experiment_manifest"]
    manifest["configuration"]["training"] = {"eval_backtest_chunk_rows": True}
    manifest["configuration_fingerprint"] = _stable_fingerprint(manifest["configuration"])
    assert checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True)[0]


def test_single_fold_lifecycle_defaults_do_not_hide_checkpoint_state_changes():
    left, right = _checkpoint(), _checkpoint()
    for checkpoint, resume, isolate in ((left, False, True), (right, True, False)):
        manifest = checkpoint["experiment_manifest"]
        manifest["configuration"]["runner"].update(resume=resume, isolate_train_folds=isolate)
        manifest["configuration_fingerprint"] = _stable_fingerprint(manifest["configuration"])
        checkpoint["optimizer"] = {"step": torch.tensor(3)}
    assert checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True)[0] == []
    right["optimizer"]["step"] = torch.tensor(4)
    assert checkpoint_differences(
        left, right, path="checkpoint", allow_runtime_profile_differences=True)[0]
