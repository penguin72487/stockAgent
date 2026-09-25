"""Keep derivative cost validation and old serialized class paths stable."""

from __future__ import annotations

from dataclasses import asdict
import pickle
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

from stockagent.backtest.tw_derivatives_cost_policy import (
    FuturesCostSchedule,
    OptionDayCostSchedule,
)


def test_cost_policy_reexports_keep_checkpoint_class_identity() -> None:
    from stockagent.backtest.tw_index_derivatives_day import (
        OptionDayCostSchedule as legacy_option,
    )
    from stockagent.backtest.tw_index_futures import (
        FuturesCostSchedule as legacy_futures,
    )

    assert legacy_futures is FuturesCostSchedule
    assert legacy_option is OptionDayCostSchedule
    for schedule in (FuturesCostSchedule(), OptionDayCostSchedule()):
        assert pickle.loads(pickle.dumps(schedule)) == schedule
    assert asdict(FuturesCostSchedule()) == {
        "tax_rate": 0.00002,
        "exchange_and_clearing_fee_per_side_twd": (20.0, 12.5, 8.0),
        "broker_fee_per_side_twd": (0.0, 0.0, 0.0),
        "slippage_points_per_side": (0.0, 0.0, 0.0),
        "basket_fee_penalty": 1.0,
    }
    np.testing.assert_array_equal(
        FuturesCostSchedule().fixed_fee_per_side_twd, [20.0, 12.5, 8.0]
    )
    assert asdict(OptionDayCostSchedule()) == {
        "fixed_fee_per_contract_per_side_twd": 22.0,
        "transaction_tax_rate": 0.001,
        "slippage_points_per_side": 0.5,
    }


@pytest.mark.parametrize(
    "constructor,overrides",
    [
        (FuturesCostSchedule, {"tax_rate": -0.1}),
        (FuturesCostSchedule, {"broker_fee_per_side_twd": (1.0,)}),
        (FuturesCostSchedule, {"basket_fee_penalty": float("nan")}),
        (OptionDayCostSchedule, {"transaction_tax_rate": -0.1}),
        (OptionDayCostSchedule, {"slippage_points_per_side": float("inf")}),
    ],
)
def test_cost_policy_still_rejects_invalid_values(constructor, overrides) -> None:
    with pytest.raises(ValueError):
        constructor(**overrides)


def test_config_import_does_not_eagerly_load_tensor_backtests() -> None:
    command = "import sys; import stockagent.config; assert 'torch' not in sys.modules"
    result = subprocess.run(
        [sys.executable, "-c", command],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
