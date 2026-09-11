from types import SimpleNamespace

import numpy as np
import pytest

from stockagent.evaluation.futures_execution_status import futures_minute_execution_status


@pytest.mark.parametrize("case", ["cash", "traded", "failed"])
def test_inactive_failed_tail_is_not_misreported_as_a_cash_policy(case):
    q = np.zeros((5, 2, 2), dtype=np.int64)
    residual = np.zeros_like(q)
    failed = np.zeros(5, dtype=bool)
    if case != "cash":
        q[1, 0, 0] = -1
    if case == "failed":
        residual[1, 0, 0] = -1
        failed[1] = True
    result = SimpleNamespace(execution_mode="tw_stock_futures_day_trade_0845_minute",
        futures_contract_quantities_history=q,
        futures_residual_contract_quantities_history=residual,
        settlement_default=failed, final_alive=case != "failed")
    dates = np.arange(np.datetime64("2022-03-01"), np.datetime64("2022-03-06"))
    out = futures_minute_execution_status(result, dates, ["2888", "2330"])
    assert out["status"] == {"cash": "valid_cash", "traded": "valid_traded", "failed": "execution_contract_failed"}[case]
    assert out["inactive_days_after_failure"] == (3 if case == "failed" else 0)
    if case == "failed":
        assert out["first_failure_date"] == "2022-03-02"
        assert out["first_failure_residuals"] == [dict(symbol="2888", candidate_slot=0, signed_contracts=-1)]
        assert out["cash_days_before_failure"] == 1
        assert "not_realized_account_loss" in out["failure_return_interpretation"]


def test_residual_without_failure_is_not_accepted_as_valid_trading():
    result = SimpleNamespace(execution_mode="tw_stock_futures_day_trade_0845_minute",
        futures_contract_quantities_history=np.ones((1, 1, 2), dtype=np.int64),
        futures_residual_contract_quantities_history=np.ones((1, 1, 2), dtype=np.int64),
        settlement_default=np.zeros(1, dtype=bool), final_alive=True)
    with pytest.raises(ValueError, match="residual contracts"):
        futures_minute_execution_status(result, np.array(["2022-03-02"]), ["2888"])
