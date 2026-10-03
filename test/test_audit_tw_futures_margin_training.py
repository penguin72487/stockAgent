import numpy as np
import pytest

from scripts.audit_tw_futures_margin_training import summarize_path
from stockagent.data.tw_futures_margin import MARGIN_AUDIT_COLUMNS


def arrays():
    audit = np.zeros((3, len(MARGIN_AUDIT_COLUMNS)))
    audit[:, :3] = [[1000, 1000, 800], [800, 800, 800], [800, 800, 800]]
    audit[0, MARGIN_AUDIT_COLUMNS.index('gross_notional_to_equity')] = 2
    return dict(
        dates=np.array(['2020-01-02', '2020-01-03', '2020-01-06'], dtype='datetime64[D]'),
        strategy_returns=np.log([.8, 1., 1.]), benchmark_returns=np.zeros(3),
        turnovers=np.array([.2, 0., 0.]), requested_weights_history=np.full((3, 1), .5),
        futures_contract_quantities_history=np.array([[1], [0], [0]]),
        futures_margin_audit=audit, futures_margin_audit_columns=np.array(MARGIN_AUDIT_COLUMNS),
        settlement_default=np.zeros(3, dtype=bool),
    )


def test_audit_does_not_call_unfilled_requests_learned_cash_or_default():
    result = summarize_path(arrays())
    assert result['active_days'] == 1
    assert result['requested_but_no_position_days'] == 2
    assert result['default_events'] == 0
    assert result['trailing_flat_from'] == '2020-01-03'
    assert result['end_equity_twd'] == 800
    assert result['canonical_metrics_log_return_ratios']['max_drawdown'] == pytest.approx(-.2)


@pytest.mark.parametrize('fault', ['return', 'reset', 'fractional', 'columns'])
def test_audit_rejects_inconsistent_saved_account(fault):
    data = arrays()
    if fault == 'return':
        data['strategy_returns'][0] = .1
    elif fault == 'reset':
        data['futures_margin_audit'][1, :3] = 1000
    elif fault == 'fractional':
        data['futures_contract_quantities_history'] = np.full((3, 1), .5)
    else:
        data['futures_margin_audit_columns'] = np.array(['bad'])
    with pytest.raises((AssertionError, ValueError)):
        summarize_path(data)
