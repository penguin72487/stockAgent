"""Mixed cash dividend/subscription: old holders, exact sources, and causality."""
import json
from pathlib import Path

import polars as pl
import pytest
import torch

from stockagent.backtest.tw_stock_futures_carry import run_tw_stock_futures_carry_torch as run
from stockagent.data.tw_stock_futures_minute import TAPE_FIELDS
from stockagent.data.tw_stock_futures_transition import _validate_transition_cash, load_transition_bundle
from test_futures_corporate_transition import transition_tape


def mixed_tape():
    x = transition_tape()
    x[1, 0, 2, TAPE_FIELDS + 7] = 8297
    x[1:, 0, 2, TAPE_FIELDS + 3] = 10000 - 8297 + 500
    return x


@pytest.mark.parametrize('sign', [-1., 1.])
def test_cash_adjustment_is_signed_once_and_no_transfer_fill(sign):
    x = mixed_tape()
    w = torch.tensor([[sign * .11], [0.], [0.]])
    out = run(w, x, initial_capital=100000., use_compile=False)
    expected = torch.tensor([99960., 99960. + sign * 500., 99960. + sign * 500.]) / 100000.
    torch.testing.assert_close(out.equity_scale_history, expected, rtol=0, atol=2e-7)
    assert out.turnovers[1:].tolist() == [0., 0.]
    assert out.residual_contract_quantities_history[:, 0].tolist() == [[sign,0,0],[0,0,sign],[0,0,sign]]
    before = run(w[:1], x[:1], initial_capital=100000., use_compile=False)
    after = run(w[1:], x[1:], initial_capital=100000., initial_carry_state=before.final_carry_state,
                initial_equity_scale=before.final_equity_scale, use_compile=False)
    torch.testing.assert_close(out.strategy_returns, torch.cat([before.strategy_returns, after.strategy_returns]), rtol=0, atol=0)


def test_flat_account_receives_no_old_holder_credit():
    out = run(torch.zeros(3, 1), mixed_tape(), initial_capital=100000., use_compile=False)
    assert out.equity_scale_history.tolist() == [1., 1., 1.]
    assert not out.turnovers.any()


@pytest.mark.parametrize('amount', [True, '8297', -1, float('nan'), float('inf'), 8297.1, 2**24])
def test_unrepresentable_or_noninteger_notice_cash_rejected(amount):
    with pytest.raises(ValueError):
        _validate_transition_cash({'kind': 'cash_dividend_and_subscription_unchanged_multiplier',
                                   'cash_adjustment_per_contract': amount}, 2)


def test_mixed_cash_requires_new_schema_and_preserves_legacy_zero_contract():
    record = {'kind': 'cash_dividend_and_subscription_unchanged_multiplier', 'cash_adjustment_per_contract': 8297}
    _validate_transition_cash(record, 2)
    with pytest.raises(ValueError):
        _validate_transition_cash(record, 1)
    for version in [1, 2]:
        _validate_transition_cash({'kind': 'cash_subscription_unchanged_multiplier', 'cash_adjustment_per_contract': 0}, version)


@pytest.mark.skipif(not torch.cuda.is_available(), reason='CUDA required')
@pytest.mark.filterwarnings('ignore:The .grad attribute of a Tensor that is not a leaf Tensor is being accessed:UserWarning')
def test_compiled_mixed_cash_matches_eager_and_chunked_gradients():
    x = mixed_tape().cuda()
    w = torch.tensor([[.11], [0.], [0.]], device='cuda', requires_grad=True)
    eager = run(w, x, initial_capital=100000., use_compile=False)
    grad = torch.autograd.grad(eager.strategy_returns.sum(), w)[0]
    compiled = run(w, x, initial_capital=100000., use_compile=True)
    torch.testing.assert_close(compiled.strategy_returns, eager.strategy_returns)
    torch.testing.assert_close(compiled.final_carry_state, eager.final_carry_state)
    torch.testing.assert_close(torch.autograd.grad(compiled.strategy_returns.sum(), w)[0], grad)


def test_real_mkf_source_is_complete_and_preserves_existing_sources():
    root = Path('artifacts/data_preparation/futures_corporate_transitions_v6_20260910')
    if not root.exists():
        pytest.skip('remote receipt-backed source unavailable')
    bundle = load_transition_bundle(root / 'manifest.json')
    old = load_transition_bundle('artifacts/data_preparation/futures_corporate_transitions_v5_20260910/manifest.json')
    coverage = bundle['coverage'].filter(pl.col('physical_contract') == 'MK1:202409')
    assert coverage.height == 13 and set(coverage['status']) == {'minute_verified'}
    assert coverage['tick_rows'].sum() == 478  # 405 ticks + 73 KBars.
    quality = json.loads((root / 'mkf_observed_volume_audit.json').read_text())
    assert quality['observed_volume'] == 314 and quality['unexplained_volume_gap'] == 1
    assert quality['trade_completeness'] == 'not_proven'
    assert bundle['finals'].filter(pl.col('physical_contract') == 'MK1:202409')['final_settlement_value'].item() == 254130
    for name in ['coverage', 'minutes', 'daily', 'finals']:
        assert bundle[name].filter(pl.col('physical_contract') != 'MK1:202409').equals(old[name])
    records = [r for r in bundle['records'] if r['underlying_symbol'] == '2439']
    assert len(records) == 5 and all(r['cash_adjustment_per_contract'] == 8297 for r in records)
    assert json.loads((root / 'manifest.json').read_text())['schema_version'] == 2


def test_new_source_rejects_old_optimizer_without_changing_model_or_clocks():
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import _checkpoint_manifest, _validate_checkpoint_manifest
    from test_checkpoint_manifest import _panel
    old = load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v14.yaml')
    new = load_config('configs/markets/tw_stock_futures_day_trade_0845_carry_v15.yaml')
    a, b = [_checkpoint_manifest(_panel(), c) for c in [old, new]]
    assert a['fingerprints']['model'] == b['fingerprints']['model']
    assert old.training == new.training and old.data == new.data and old.walk_forward == new.walk_forward
    assert old.trading.tw_stock_futures_day_trade_quarantine_contract_days == new.trading.tw_stock_futures_day_trade_quarantine_contract_days
    with pytest.raises(RuntimeError, match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest': a}, b, checkpoint_path=__file__, scope='resume')
