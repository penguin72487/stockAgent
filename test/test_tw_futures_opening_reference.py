"""An input reference can carry a real print; it cannot manufacture a fill."""
from dataclasses import replace
from datetime import date, datetime
import json

import numpy as np
import polars as pl
import pytest
import torch

from downloader.artifact_io import sha256_file
from scripts.prepare_tw_futures_decision_clocks import paired_opening_references
from stockagent.config import load_config
from stockagent.data.tw_futures_decision_clock import (
    OPENING_REFERENCE_DATASET, OPENING_REFERENCE_FEATURES, opening_reference_contract,
    resolve_opening_references, validate_opening_reference_source,
)
from stockagent.data.tw_stock_context_futures_portfolio import TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.models.financial_transformer_futures import FinancialTransformerFuturesModel
from stockagent.models.futures_account_policy import FUTURES_ACCOUNT_PACKET_WIDTH
from test_futures_causal_account_policy import account
from test_tw_futures_margin import tape


def decisions():
    return pl.DataFrame({'date': [date(2026, 1, 5)], 'physical_contract': ['A'], 'symbol': ['s1']})


def events():
    return pl.DataFrame({
        'physical_contract': ['A']*4,
        'price': [100., 103., 105., 106.],
        'trade_date': [date(2026, 1, 2)] + [date(2026, 1, 5)]*3,
        'trade_ts': [None, datetime(2026,1,5,8,30), datetime(2026,1,5,8,45,0,1), datetime(2026,1,5,8,59,59)],
        'available_by': [datetime(2026,1,2,23,59,59), datetime(2026,1,5,8,30,1),
                         datetime(2026,1,5,8,46), datetime(2026,1,5,9,0)],
        'source_sha256': ['a'*64]*4, 'is_session_open': [False, False, True, False],
    })


def test_preopen_real_trade_and_observed_first_print_have_distinct_clocks():
    a = resolve_opening_references(decisions(), events(), decision_time='08:45').row(0, named=True)
    b = resolve_opening_references(decisions(), events(), decision_time='09:00').row(0, named=True)
    assert a['reference_quote'] == 103. and a['reference_kind'] == 'last_observed_trade'
    assert b['reference_quote'] == 105. and b['reference_kind'] == 'observed_session_open'
    assert b['reference_trade_ts'] == datetime(2026,1,5,8,45,0,1)
    assert a[OPENING_REFERENCE_FEATURES[0]] == pytest.approx(np.log(1.03))
    assert b[OPENING_REFERENCE_FEATURES[0]] == pytest.approx(np.log(1.05))


def test_previous_actual_close_keeps_its_date_and_not_a_fictional_trade_timestamp():
    row = resolve_opening_references(decisions(), events().head(1), decision_time='08:45').row(0, named=True)
    assert row['reference_quote'] == 100.
    assert row['reference_trade_date'] == date(2026,1,2) and row['reference_trade_ts'] is None
    assert row['reference_age_days'] == 3
    assert row[OPENING_REFERENCE_FEATURES[0]] == 0
    assert row[OPENING_REFERENCE_FEATURES[3]] == pytest.approx(np.log(4))


def test_delayed_receipt_and_future_prices_do_not_leak_into_earlier_decision():
    before = resolve_opening_references(decisions(), events(), decision_time='08:45')
    changed = events().with_columns(pl.when(pl.col('available_by') > datetime(2026,1,5,8,45))
        .then(999.).otherwise(pl.col('price')).alias('price'))
    assert resolve_opening_references(decisions(), changed, decision_time='08:45').equals(before)
    delayed = events().with_columns(pl.when(pl.col('is_session_open'))
        .then(datetime(2026,1,5,9,0,1)).otherwise(pl.col('available_by')).alias('available_by'))
    row = resolve_opening_references(decisions(), delayed, decision_time='09:00').row(0, named=True)
    assert row['reference_quote'] == 106. and row['reference_kind'] == 'last_observed_trade'


def test_unknown_new_generation_has_missing_reference_without_cross_contract_fill():
    changed = decisions().with_columns(pl.lit('A#generation=1').alias('physical_contract'))
    row = resolve_opening_references(changed, events(), decision_time='09:00').row(0, named=True)
    assert row['reference_quote'] is None and row['reference_kind'] == 'unavailable'
    assert row[OPENING_REFERENCE_FEATURES[1]] == 0.
    assert all(np.isfinite(row[name]) for name in OPENING_REFERENCE_FEATURES)


def test_original_opening_trade_can_itself_be_the_last_trade_of_a_prior_session():
    only = events().head(1).with_columns(pl.lit(True).alias('is_session_open'))
    row = resolve_opening_references(decisions(), only, decision_time='08:45').row(0, named=True)
    assert row['reference_quote'] == 100. and row['reference_kind'] == 'last_observed_trade'


def test_repeated_opening_flags_cannot_silently_turn_first_open_into_latest_trade():
    invalid = events().with_columns((pl.col('is_session_open') | (pl.col('price') == 106.)).alias('is_session_open'))
    with pytest.raises(ValueError, match='multiple session-opening'):
        resolve_opening_references(decisions(), invalid, decision_time='09:00')


@pytest.mark.parametrize('field,value', [('price', float('nan')), ('source_sha256', 'not-a-receipt'),
    ('available_by', datetime(2026,1,1))])
def test_invalid_trade_or_reversed_source_clock_is_rejected(field, value):
    with pytest.raises(ValueError, match='invalid real-trade'):
        resolve_opening_references(decisions(), events().with_columns(pl.lit(value).alias(field)), decision_time='08:45')


def test_daily_settlement_and_late_first_minute_never_become_known_open():
    source = pl.DataFrame({'date': [date(2026,1,2),date(2026,1,5)], 'physical_contract': ['A']*2,
        'symbol': ['s1']*2, 'observed_open': [99.,105.], 'observed_close': [100.,110.],
        'close': [999.,999.], 'source_row_observed': [True]*2, 'volume': [20.,30.],
        'official_source_sha256': ['b'*64]*2})
    minutes = pl.DataFrame({'date': [date(2026,1,5)]*2, 'physical_contract': ['A']*2,
        'minute': [539,540], 'vwap': [106.,107.], 'high': [106.,108.], 'low': [105.,106.],
        'close': [106.,107.], 'volume': [2.,4.], 'source_file_sha256': ['a'*64]*2})
    result = paired_opening_references(source, minutes, source.tail(1).select('date','physical_contract'))
    assert result['08:45']['reference_quote'].to_list() == [None,100.]
    assert result['09:00']['reference_quote'].to_list() == [None,105.]
    assert result['09:00']['prior_trade_quote'].to_list() == [None,100.]


def test_reference_source_rejects_future_clock_or_wrong_pair_even_with_valid_file_sha(tmp_path):
    path, daily = tmp_path/'opening_reference.parquet', tmp_path/'daily'
    daily.write_bytes(b'original daily')
    refs = resolve_opening_references(decisions(), events(), decision_time='09:00')
    refs.write_parquet(path)
    meta = dict(dataset=OPENING_REFERENCE_DATASET, status='complete', contract=opening_reference_contract('09:00'),
        data_sha256=sha256_file(path), source_daily_sha256=sha256_file(daily), pair_identity='paired', rows=1)
    m = tmp_path/'opening_reference_manifest.json'; m.write_text(json.dumps(meta))
    validate_opening_reference_source(path,daily_path=daily,decision_time='09:00',entry_manifest={'pair_identity':'paired'})
    with pytest.raises(ValueError, match='incompatible'):
        validate_opening_reference_source(path,daily_path=daily,decision_time='08:45',entry_manifest={'pair_identity':'paired'})
    refs.with_columns(pl.lit(datetime(2026,1,5,9,0,1)).alias('reference_available_by')).write_parquet(path)
    m.write_text(json.dumps({**meta,'data_sha256':sha256_file(path)}))
    with pytest.raises(ValueError, match='future'):
        validate_opening_reference_source(path,daily_path=daily,decision_time='09:00',entry_manifest={'pair_identity':'paired'})


def test_new_clock_configs_keep_same_training_and_account_controls():
    a = load_config('configs/markets/tw_stock_futures_carry_0845_openref_20261021_v2.yaml')
    b = load_config('configs/markets/tw_stock_futures_carry_0900_openref_20261021_v2.yaml')
    assert a.data == b.data and a.training == b.training and a.walk_forward == b.walk_forward
    assert replace(a.trading, tw_futures_portfolio_decision_time='09:00',
        tw_futures_portfolio_entry_clock_path=b.trading.tw_futures_portfolio_entry_clock_path,
        tw_futures_portfolio_opening_reference_path=b.trading.tw_futures_portfolio_opening_reference_path) == b.trading
    assert a.training.pretrained_initialization_root is None
    assert a.training.epochs*a.training.early_stopping_no_improve_ratio == 100


def test_opening_reference_feature_and_source_change_reject_incompatible_resume(tmp_path):
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest, _validate_checkpoint_manifest
    from test_tw_stock_context_futures_portfolio import _stock_panel
    c = load_config('configs/markets/tw_futures_v8_margin_components_causal_account_capital100m_v5.yaml')
    entry = tmp_path/'entries.parquet'; entry.write_bytes(b'entry fixture')
    entry.with_name('manifest.json').write_text('{}')
    c.trading.tw_futures_portfolio_entry_clock_path = str(entry)
    c.trading.tw_futures_portfolio_decision_time = '09:00'
    old = build_checkpoint_manifest(_stock_panel(),c,include_data_content=False)
    ref = tmp_path/'opening_reference.parquet'; ref.write_bytes(b'reference fixture')
    ref.with_name('opening_reference_manifest.json').write_text('{}')
    c.trading.tw_futures_portfolio_opening_reference_path = str(ref)
    new = build_checkpoint_manifest(_stock_panel(),c,include_data_content=False)
    assert new['fingerprints']['trading'] != old['fingerprints']['trading']
    with pytest.raises(RuntimeError,match='semantic fingerprint mismatch'):
        _validate_checkpoint_manifest({'experiment_manifest':old},new,checkpoint_path=tmp_path/'old.pt',scope='resume')
    ref.write_bytes(b'changed reference fixture')
    changed = build_checkpoint_manifest(_stock_panel(),c,include_data_content=False)
    assert new['fingerprints']['trading'] != changed['fingerprints']['trading']


def test_opening_reference_encoder_receives_gradients_through_the_actual_integer_account():
    torch.set_num_threads(2); torch.manual_seed(51)
    model = FinancialTransformerFuturesModel(
        lookback=4,num_features=3,num_symbols=2,d_model=8,attention_mode='market_token',
        num_market_tokens=2,temporal_heads=2,temporal_layers=1,temporal_pooling='last',
        temporal_query_mode='last_only',use_symbol_pos=False,portfolio_mode='long_short',
        portfolio_output_mode='score_entmax_log_cash',center_long_short_logits=False,
        futures_denomination_hard_projection=False,futures_margin_budget_output=True,
        futures_margin_amount_context=True,futures_causal_account_policy=True,
        futures_opening_reference_feature=True,feature_bottleneck_dim=2,dropout=0.,return_aux=False).eval()
    base = len(TW_STOCK_CONTEXT_FUTURES_MODEL_FEATURE_COLUMNS)
    f = torch.zeros(2,1936,base+3+2+len(OPENING_REFERENCE_FEATURES))
    f[:,:2,1:18] = torch.randn(2,2,17)
    f[...,base-1] = 1004
    f[...,base:base+3] = torch.tensor([100.,.1,.75])
    f[...,base+3:base+5] = torch.tensor([.02,1.])
    f[...,-4:] = torch.tensor([.03,1.,1.,0.])
    mask = torch.zeros(2,1936,dtype=torch.bool);mask[:,:2] = True
    x, sm = torch.randn(2,4,2,3),torch.ones(2,2,dtype=torch.bool)
    out = model(x,sm,portfolio_context=dict(candidate_features=f,candidate_mask=mask))
    assert out.shape == (2,1936,FUTURES_ACCOUNT_PACKET_WIDTH)
    execution = tape(2,1936);execution[:,2:,1] = 0;execution[0,0,4] += 40
    (-account(out,execution,recoverable_backward=True).strategy_returns.mean()).backward()
    grad = model.futures_opening_reference_encoder.weight.grad
    assert torch.isfinite(grad).all() and grad.abs().sum() > 0
    with pytest.raises(ValueError,match='shape'):
        model(x,sm,portfolio_context=dict(candidate_features=f[...,:-4],candidate_mask=mask))
