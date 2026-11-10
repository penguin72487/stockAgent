"""Priority changes must preserve fold ownership, initialization and coverage."""
from types import SimpleNamespace

import pytest
import yaml
import numpy as np
import random
import torch

import train
from scripts.run_ablation_experiments import _experiment_rows


def test_reverse_groups_exclude_later_training_rng_but_preserve_existing_schedules():
    from stockagent.training import trainer
    folds = [SimpleNamespace(fold_id=2), SimpleNamespace(fold_id=1)]
    before = trainer._capture_rng_state()
    try:
        state = trainer._reverse_fold_initialization_rng(folds)
        expected = (random.random(), np.random.rand(), torch.rand(4))
        for count in (7, 31):
            random.random()
            np.random.rand(count)
            torch.rand(count)
            assert trainer._restore_rng_state(state, rank_local=True)
            actual = (random.random(), np.random.rand(), torch.rand(4))
            assert actual[:2] == expected[:2]
            torch.testing.assert_close(actual[2], expected[2], rtol=0, atol=0)
        assert trainer._reverse_fold_initialization_rng(list(reversed(folds))) is None
        assert trainer._reverse_fold_initialization_rng(folds[:1]) is None
        with pytest.raises(ValueError, match="later-group"):
            trainer._reverse_fold_initialization_rng(folds, warm_start_from_previous_fold=True)
    finally:
        trainer._restore_rng_state(before, rank_local=True)


def test_only_source_bound_physical_execution_can_elide_the_discarded_tape(monkeypatch):
    from stockagent.training.trainer import _move_windowed_batch_to_device
    class MustNotTransfer:
        def to(self, **kwargs):
            raise AssertionError("the discarded tape was transferred")
    batch = {"future_log_returns": torch.tensor([[.01]]),
             "overnight_log_returns": MustNotTransfer()}
    runtime = SimpleNamespace(mode="tw_day_trade", day_trade_carry_source=object())
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_OMIT_LEGACY_TAPE", "1")
    result = _move_windowed_batch_to_device(batch, torch.device("cpu"), False,
                                           execution_runtime=runtime)
    assert "overnight_log_returns" not in result
    assert result["future_log_returns"] is batch["future_log_returns"]
    assert "overnight_log_returns" in batch
    for owner in (None, SimpleNamespace(mode="tw_day_trade", day_trade_carry_source=None)):
        with pytest.raises(AssertionError, match="discarded tape"):
            _move_windowed_batch_to_device(batch, torch.device("cpu"), False, execution_runtime=owner)
    monkeypatch.setenv("STOCKAGENT_DAY_TRADE_OMIT_LEGACY_TAPE", "0")
    with pytest.raises(AssertionError, match="discarded tape"):
        _move_windowed_batch_to_device(batch, torch.device("cpu"), False, execution_runtime=runtime)


def test_descending_keeps_original_fold_objects_and_rejects_weight_transfer():
    folds = [SimpleNamespace(fold_id=i, train_years=tuple(range(2015, 2015+i)))
             for i in range(1, 11)]
    ordered = train._order_training_folds(folds, order="descending")
    assert [fold.fold_id for fold in ordered] == list(range(10, 0, -1))
    assert all(a is b for a, b in zip(ordered, reversed(folds), strict=True))
    assert [fold.fold_id for fold in folds] == list(range(1, 11))
    assert train._order_training_folds(ordered, order="ascending") == folds
    with pytest.raises(ValueError, match="later fold"):
        train._order_training_folds(folds, order="descending", warm_start_from_previous_fold=True)
    with pytest.raises(ValueError, match="fold order"):
        train._order_training_folds(folds, order="random")


@pytest.fixture
def spec(tmp_path):
    path = tmp_path / "study.yaml"
    payload = {"base_config": "unused.yaml", "matrix": {
        "include_baseline": True, "dimensions": [{"name": "layers", "enabled": True, "path": "training.layers",
            "values": [{"name": "one", "value": 1}, {"name": "two", "value": 2}]}]}}
    path.write_text(yaml.safe_dump(payload))
    return path, payload


def test_priority_is_applied_before_only_filter_without_changing_overrides(spec):
    path, payload = spec
    original = {row["name"]: row for row in _experiment_rows(path)[1]}
    payload["experiment_order"] = ["layers__two", "baseline", "layers__one"]
    path.write_text(yaml.safe_dump(payload))
    rows = _experiment_rows(path)[1]
    assert [row["name"] for row in rows] == payload["experiment_order"]
    assert {row["name"]: row for row in rows} == original
    assert [row["name"] for row in _experiment_rows(path, {"baseline", "layers__two"})[1]] == [
        "layers__two", "baseline"]


@pytest.mark.parametrize("order", [["baseline"], ["baseline", "layers__one", "layers__one"],
    ["baseline", "layers__one", "unknown"], "baseline", ["baseline", "layers__one", None]])
def test_incomplete_duplicate_or_unknown_priority_fails_closed(spec, order):
    path, payload = spec
    payload["experiment_order"] = order
    path.write_text(yaml.safe_dump(payload))
    with pytest.raises(ValueError, match="exactly once"):
        _experiment_rows(path)
