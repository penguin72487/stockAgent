from datetime import date

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.backtest.futures_data_validity import FuturesCarryDataError
from stockagent.backtest.tw_stock_futures_carry import run_tw_stock_futures_carry_torch as run
from stockagent.data.tw_stock_futures_carry import (
    CARRY_QUARANTINE_OFFSET, CARRY_QUARANTINE_CONTRACT_VERSION,
    CARRY_TAPE_FIELDS_BY_VERSION, QUARANTINED_CARRY_POLICY, build_carry_tape,
)
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS
from test_tw_stock_futures_carry import tape, bar


def quarantined_tape(symbols=1):
    x = torch.nn.functional.pad(tape(3, symbols=symbols), (0, 3))
    x[1, 0, 0, TAPE_FIELDS + 6] = 0
    x[1, 0, 0, CARRY_QUARANTINE_OFFSET:] = torch.tensor([1., 11000.])
    # Deliberately keep spurious entry/exit bars and a new target: the executor
    # must enforce quarantine itself, even if the builder normally erases them.
    bar(x, 1, 400., 1000, event=6)
    x[1, 0, 0, TAPE_FIELDS + 3] = 99999.
    bar(x, 2, 115., 10)
    x[2, :, 0, TAPE_FIELDS + 3] = 11500.
    return x


@pytest.mark.parametrize('sign', [-1., 1.])
@pytest.mark.parametrize('source_verified', [0., 1.])
def test_quarantine_keeps_inventory_and_official_mtm_then_resumes_orders(sign, source_verified):
    x = quarantined_tape()
    x[1, 0, 0, TAPE_FIELDS + 6] = source_verified
    w = torch.tensor([[sign * .11], [-sign * .9], [0.]])
    result = run(w, x, initial_capital=100000., use_compile=False)
    assert result.final_alive
    assert result.residual_contract_quantities_history[:, 0, 0].tolist() == [sign, sign, 0]
    assert result.turnovers[1] == 0
    assert result.carry_state_history[1, 0, 0, :3].tolist() == [sign, 11000., 1.]
    expected = [99960., 99960. + sign * 1000., 99920. + sign * 1500.]
    np.testing.assert_allclose(result.equity_scale_history.numpy() * 100000., expected, atol=.02, rtol=0)


@pytest.mark.parametrize('damage', ['mark_zero', 'mark_nan', 'identity', 'corporate', 'expiry', 'unlisted_gap'])
def test_quarantine_never_suppresses_other_unknown_evidence(damage):
    x = quarantined_tape()
    if damage == 'mark_zero':
        x[1, 0, 0, CARRY_QUARANTINE_OFFSET + 1] = 0
    elif damage == 'mark_nan':
        x[1, 0, 0, CARRY_QUARANTINE_OFFSET + 1] = float('nan')
    elif damage == 'identity':
        x[1, 0, 0, TAPE_FIELDS] = 2
    elif damage == 'corporate':
        x[1, 0, 0, TAPE_FIELDS + 8] = 1
    elif damage == 'expiry':
        x[1, 0, 0, TAPE_FIELDS + 5] = 1
    else:
        x[1, 0, 0, CARRY_QUARANTINE_OFFSET] = 0
    with pytest.raises(FuturesCarryDataError):
        run(torch.tensor([[.11], [0.], [0.]]), x, initial_capital=100000., use_compile=False)


def test_flat_quarantined_contract_cannot_open_even_with_verified_bars():
    x = quarantined_tape()
    x[1, 0, 0, TAPE_FIELDS + 6] = 1
    result = run(torch.tensor([[0.], [.9], [0.]]), x, initial_capital=100000., use_compile=False)
    assert result.final_equity_scale == 1
    assert not result.contract_quantities_history.any()
    assert not result.turnovers.any()


def test_official_end_mark_cannot_fund_peer_opening_orders_or_change_prior_rows():
    x = quarantined_tape(symbols=2)
    x[1, 1, 0, 3:TAPE_FIELDS] = 0
    x[1, 1, 0, 3:8] = torch.tensor([100., 100., 100., 100., 100.])
    w = torch.tensor([[.51, 0.], [0., .99], [0., 0.]])
    a = run(w, x, initial_capital=100000., use_compile=False)
    changed = x.clone()
    changed[1, 0, 0, CARRY_QUARANTINE_OFFSET + 1] = 100000.
    b = run(w, changed, initial_capital=100000., use_compile=False)
    torch.testing.assert_close(a.contract_quantities_history[:2], b.contract_quantities_history[:2], rtol=0, atol=0)
    torch.testing.assert_close(a.strategy_returns[:1], b.strategy_returns[:1], rtol=0, atol=0)
    assert a.contract_quantities_history[1, 1, 0] > 0  # Healthy peer still trades.


def test_chunk_boundary_and_common_loss_preserve_quarantined_signed_state():
    from stockagent.training.loss import risk_aware_loss
    x = quarantined_tape()
    w = torch.tensor([[.11], [-.9], [0.]], requires_grad=True)
    full = run(w, x, initial_capital=100000., use_compile=False)
    first = run(w[:1], x[:1], initial_capital=100000., use_compile=False)
    rest = run(w[1:], x[1:], initial_capital=100000., use_compile=False,
               initial_carry_state=first.final_carry_state, initial_equity_scale=first.final_equity_scale)
    torch.testing.assert_close(full.strategy_returns, torch.cat((first.strategy_returns, rest.strategy_returns)), rtol=0, atol=0)
    aux = {}
    loss = risk_aware_loss(w, torch.zeros_like(w), torch.ones_like(w, dtype=torch.bool),
        overnight_log_returns=x, aux_outputs=aux, objective='log_utility',
        execution_mode='tw_stock_futures_day_trade_0845_minute',
        portfolio_activation='pre_normalized', day_trade_execution_initial_capital=100000.)
    loss.backward()
    assert torch.isfinite(w.grad).all()
    assert w.grad[1, 0] == 0
    assert w.grad[0, 0] != 0
    torch.testing.assert_close(aux['_final_equity_scale'], full.final_equity_scale)


def builder_inputs():
    dates = np.array(['2026-01-19', '2026-01-20', '2026-01-21'], dtype='datetime64[D]')
    source = pl.DataFrame({'date': [date(2026, 1, 19), date(2026, 1, 20), date(2026, 1, 21)],
        'physical_contract': ['ABC:202602'] * 3, 'underlying_symbol': ['1234'] * 3,
        'contract_multiplier': [100.] * 3, 'valuation_settlement': [100., 999., 115.],
        'source_row_observed': [True] * 3})
    selected = source.select('date', 'physical_contract', 'underlying_symbol')
    coverage = source.select('date', 'physical_contract').with_columns(
        pl.when(pl.col('date') == date(2026, 1, 20)).then(pl.lit('source_empty_unresolved'))
        .otherwise(pl.lit('minute_verified')).alias('status'))
    minutes = source.select('date', 'physical_contract').with_columns(pl.lit(526).alias('minute'),
        *[pl.lit(100.).alias(name) for name in ['vwap', 'high', 'low', 'close', 'volume']])
    final = pl.DataFrame(schema={'date': pl.Date, 'physical_contract': pl.String,
                                'final_settlement_price': pl.Float64})
    marks = pl.DataFrame({'date': [date(2026, 1, 20)], 'physical_contract': ['ABC:202602'],
                          'official_daily_settlement': [110.]})
    kwargs = dict(fee=40., participation=.5, capacity_rounding='ceil',
        quarantine_contract_days=[{'date': '2026-01-20', 'physical_contract': 'ABC:202602'}],
        daily_settlements=marks, quarantined_carry_policy=QUARANTINED_CARRY_POLICY)
    return (source, selected, minutes, coverage, dates, ('1234',), final), kwargs


def test_builder_marks_exact_scope_preserves_unknown_evidence_and_calendar():
    args, kwargs = builder_inputs()
    x, meta = build_carry_tape(*args, **kwargs)
    assert x.shape == (3, 1, 2, CARRY_TAPE_FIELDS_BY_VERSION[CARRY_QUARANTINE_CONTRACT_VERSION])
    assert x[1, 0, 0, CARRY_QUARANTINE_OFFSET:].tolist() == [1., 11000.]
    assert x[1, 0, 0, TAPE_FIELDS + 6] == 0  # Unknown source remains unknown.
    assert not x[1, 0, 0, 3:TAPE_FIELDS].any()
    assert x[:, 0, 0, TAPE_FIELDS + 2].tolist() == [1., 0., 1.]
    assert x[0, 0, 0, 7] == x[2, 0, 0, 7] == 50.
    assert meta['quarantined_carry_contract_days'][0]['official_daily_settlement'] == 110.
    assert args[2].height == 3  # Original observations are preserved.


@pytest.mark.parametrize('damage', ['missing', 'wrong_date', 'wrong_contract', 'nan', 'zero'])
def test_builder_rejects_unproven_quarantine_mark_before_training(damage):
    args, kwargs = builder_inputs()
    if damage == 'missing':
        kwargs['daily_settlements'] = None
    else:
        field, value = {'wrong_date': ('date', date(2026, 1, 19)),
                        'wrong_contract': ('physical_contract', 'ABC:202603'),
                        'nan': ('official_daily_settlement', float('nan')),
                        'zero': ('official_daily_settlement', 0.)}[damage]
        kwargs['daily_settlements'] = kwargs['daily_settlements'].with_columns(pl.lit(value).alias(field))
    with pytest.raises(ValueError, match='verified same-day official settlement'):
        build_carry_tape(*args, **kwargs)


def test_default_keeps_old_checkpoint_contract_and_new_policy_rejects_optimizer_resume():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import (
        _configuration_fingerprint_snapshot, _checkpoint_manifest, _validate_checkpoint_manifest,
    )
    from stockagent.training.trainer import _mode_artifact_contract_for_config
    from test_checkpoint_manifest import _panel
    old = load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v10.yaml')
    new = load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v11.yaml')
    assert 'tw_stock_futures_day_trade_quarantined_carry_policy' not in _configuration_fingerprint_snapshot(old)['trading']
    a, b = [_checkpoint_manifest(_panel(), c) for c in (old, new)]
    assert a['fingerprints']['model'] == b['fingerprints']['model']
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': a}, b, checkpoint_path=__file__, scope='resume')
    details = _mode_artifact_contract_for_config(new)['mode_details']
    assert details['execution_contract_version'] == CARRY_QUARANTINE_CONTRACT_VERSION
    assert details['quarantined_carry_policy'] == QUARANTINED_CARRY_POLICY


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA required')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_compiled_quarantine_matches_eager_values_and_gradients():
    x = quarantined_tape().cuda()
    w = torch.tensor([[.11], [-.9], [0.]], device='cuda', requires_grad=True)
    a = run(w, x, initial_capital=100000., use_compile=False)
    gradient = torch.autograd.grad(a.strategy_returns.sum(), w)[0]
    b = run(w, x, initial_capital=100000., use_compile=True)
    torch.testing.assert_close(a.strategy_returns, b.strategy_returns)
    torch.testing.assert_close(a.final_carry_state, b.final_carry_state)
    torch.testing.assert_close(gradient, torch.autograd.grad(b.strategy_returns.sum(), w)[0])
