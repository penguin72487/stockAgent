import pytest

from scripts.audit_factorized_training_capacity import checkpoint_budget


def test_checkpoint_capacity_preserves_atomic_group_and_best():
    budget=checkpoint_budget(100,report_reserve_bytes=40)
    assert budget["adam_group_bytes"]==300
    assert budget["group_atomic_peak_plus_fold_best_bytes"]==700
    assert budget["required_free_bytes"]==740


@pytest.mark.parametrize("parameters,reserve", [(-1,0),(0,-1)])
def test_invalid_capacity_is_not_available_space(parameters,reserve):
    with pytest.raises(ValueError):checkpoint_budget(parameters,report_reserve_bytes=reserve)
