from datetime import date, timedelta
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_release import (
    materialize_margin_market_rows, margin_execution_dependency_rows,
    publish_all_twd_margin_release,
    validate_accounting_continuation,
)
from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS
from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily
from test_tw_stock_context_futures_portfolio import _stock_panel


def source_rows():
    rows, raw = [], []
    for i in range(4):
        day = date(2026, 1, 2) + timedelta(days=i)
        for product in ('TX', 'ZZ1'):
            volume = 10 if product == 'TX' and i != 1 else 0
            rows.append(dict(date=day, product=product, contract='202601',
                physical_contract=product+':202601', physical_instance=product+':202601#g0',
                asset_class='index_future' if product == 'TX' else 'stock_future',
                official_open=str(100+i) if volume else None, official_close=str(101+i) if volume else None,
                official_high=str(102+i) if volume else None, official_low='99' if volume else None,
                outright_volume=volume, spread_leg_volume=0, valuation_price=float(101+i),
                cash_settlement=False, lifetime_status='observed_at_dataset_boundary',
                final_settlement_value=None))
            raw.append(dict(date=day, product=product, contract='202601', session='一般', open_interest=3))
    universe = pl.DataFrame(dict(product=['TX', 'ZZ1'], settlement_currency=['TWD']*2,
                                underlying_symbol=[None, '0123'], region=['domestic']*2))
    return pl.DataFrame(rows), pl.DataFrame(raw), universe


def test_full_market_rows_keep_zero_print_product_and_no_synthetic_fills():
    source, raw, universe = source_rows()
    daily, slots = materialize_margin_market_rows(source, raw, universe)
    assert daily.height == source.height
    assert set(daily['product']) == {'TX', 'ZZ1'}
    assert slots.height == 2
    assert not daily.filter(pl.col('product') == 'ZZ1')['executable'].any()
    zero = daily.filter((pl.col('product') == 'TX') & (pl.col('volume') == 0)).row(0, named=True)
    assert zero['open'] == 101  # preceding official mark, not next/current close
    assert not zero['executable']
    assert zero['official_row_observed']
    assert not zero['source_row_observed']  # A settlement mark is no benchmark close fill.
    assert not daily['must_liquidate'].any()  # EOF is not a liquidation
    assert daily['contract_multiplier'].null_count() == daily.height


def test_future_price_perturbation_cannot_change_earlier_features():
    source, raw, universe = source_rows()
    first, _ = materialize_margin_market_rows(source, raw, universe)
    last = source['date'].max()
    mutated = source.with_columns(pl.when(pl.col('date') == last).then(9000.)
                                 .otherwise(pl.col('valuation_price')).alias('valuation_price'))
    second, _ = materialize_margin_market_rows(mutated, raw, universe)
    np.testing.assert_equal(first.filter(pl.col('date') < last).select(FUTURES_MODEL_FEATURE_COLUMNS).to_numpy(),
                            second.filter(pl.col('date') < last).select(FUTURES_MODEL_FEATURE_COLUMNS).to_numpy())


def test_reused_code_cannot_inherit_previous_generation_mark():
    source, raw, universe = source_rows()
    source = source.with_columns(pl.when(pl.col('date') >= date(2026, 1, 4))
        .then(pl.col('physical_instance') + 'new').otherwise(pl.col('physical_instance')).alias('physical_instance'))
    daily, slots = materialize_margin_market_rows(source, raw, universe)
    second_generation = daily.filter(pl.col('date') == date(2026, 1, 4))
    assert second_generation['previous_settlement'].null_count() == 2
    assert not second_generation['same_contract_as_previous_session'].any()
    assert slots['portfolio_slot'].n_unique() == 4


def test_missing_mark_is_not_forward_filled_or_hidden():
    source, raw, universe = source_rows()
    source = source.with_columns(pl.when(pl.col('date') == date(2026, 1, 3))
        .then(None).otherwise(pl.col('valuation_price')).alias('valuation_price'))
    daily, _ = materialize_margin_market_rows(source, raw, universe)
    assert daily.filter(pl.col('date') == date(2026, 1, 3))['settlement'].null_count() == 2
    assert daily.filter(pl.col('date') == date(2026, 1, 4))['previous_settlement'].null_count() == 2


def test_unknown_volume_is_preserved_but_marks_can_value_old_inventory():
    source, raw, universe = source_rows()
    source = source.with_columns(pl.when(pl.col('date') == date(2026, 1, 3)).then(None)
                                 .otherwise(pl.col('outright_volume')).alias('outright_volume'))
    daily, _ = materialize_margin_market_rows(source, raw, universe)
    unreported = daily.filter(pl.col('date') == date(2026, 1, 3))
    assert unreported['volume'].null_count() == 2
    assert unreported['open'].to_list() == [101., 101.]
    assert not unreported['executable'].any()


def test_weekly_contract_tenor_is_ordered_by_calendar_not_month_string():
    source, raw, universe = source_rows()
    source = source.filter(pl.col('product') == 'TX')
    raw = raw.filter(pl.col('product') == 'TX')
    week = source.with_columns(pl.lit('202601W1').alias('contract'),
        pl.lit('TX:202601W1').alias('physical_contract'), pl.lit('TX:202601W1#g0').alias('physical_instance'))
    raw_week = raw.with_columns(pl.lit('202601W1').alias('contract'))
    daily, _ = materialize_margin_market_rows(pl.concat([source, week]), pl.concat([raw, raw_week]),
                                              universe.filter(pl.col('product') == 'TX'))
    assert set(daily.filter(pl.col('contract') == '202601W1')['tenor_rank']) == {1}


@pytest.mark.parametrize('problem', ['duplicate_raw', 'missing_physical_day', 'drop_product', 'foreign_currency'])
def test_market_rows_reject_incomplete_or_ambiguous_scope(problem):
    source, raw, universe = source_rows()
    if problem == 'duplicate_raw': raw = pl.concat([raw, raw.head(1)])
    if problem == 'missing_physical_day': source = source.slice(1)
    if problem == 'drop_product': source = source.filter(pl.col('product') == 'TX')
    if problem == 'foreign_currency': universe = universe.with_columns(pl.lit('USD').alias('settlement_currency'))
    with pytest.raises(ValueError): materialize_margin_market_rows(source, raw, universe)


def test_numerically_bound_inputs_are_not_execution_admission():
    source, raw, universe = source_rows()
    daily, _ = materialize_margin_market_rows(source, raw, universe)
    keys = ['date', 'product', 'contract']
    margins = daily.select('date', 'product').unique().with_columns(
        pl.lit('bound_prior_publication').alias('opening_binding_status'),
        pl.lit('bound_prior_publication').alias('settlement_binding_status'))
    positions = daily.select(keys).with_columns(pl.lit(True).alias('position_numeric_inputs_resolved'))
    terms = daily.select(keys).with_columns(pl.lit('bound_prior_publication').alias('terms_binding_status'),
                                           pl.lit(False).alias('subscription_rights_at_final_settlement'))
    terminals = daily.select(keys).with_columns(pl.lit(0.).alias('terminal_value_input_twd'))
    dependencies = margin_execution_dependency_rows(daily, margins, positions, terms, terminals)
    assert not dependencies['missing_margin'].any()
    assert not dependencies['missing_position'].any()
    assert not dependencies['training_admitted'].any()


@pytest.mark.parametrize('broken', [False, True])
def test_corporate_first_day_must_inherit_old_inventory(broken):
    d1, d2 = date(2026, 1, 2), date(2026, 1, 5)
    frame = pl.DataFrame(dict(date=[d1, d2], physical_contract=['OLD', 'NEW'],
        next_market_date=[d2, None], cash_settlement=[False, False]))
    rules = pl.DataFrame(dict(date=[d1, d2], physical_contract=['OLD', 'NEW'],
        terminal_event=['mark_only', 'mark_only'], carry_from_date=[None, d1],
        carry_from_physical_contract=['', '' if broken else 'OLD']))
    if broken:
        with pytest.raises(ValueError, match='loses its next-session owner'):
            validate_accounting_continuation(frame, rules)
    else:
        validate_accounting_continuation(frame, rules)


def test_training_rejects_market_only_materialization_before_loading_prices(tmp_path):
    data = tmp_path/'continuous_daily.parquet'
    data.write_bytes(b'not even a price file')
    (tmp_path/'manifest.json').write_text(json.dumps(dict(materialization_version=1,
        status='accounting_admission_required', all_products_training_ready=False)))
    with pytest.raises(ValueError, match='accounting admission'):
        attach_stock_context_futures_portfolio_daily(_stock_panel(), data,
            fee_per_side_twd_by_group={'standard': 40.}, integer_contracts=True)


def test_publisher_rejects_candidate_terms_and_does_not_publish(tmp_path):
    material = tmp_path/'material'; material.mkdir()
    source, raw, universe = source_rows()
    daily, _ = materialize_margin_market_rows(source, raw, universe)
    daily.write_parquet(material/'continuous_daily.parquet')
    (material/'manifest.json').write_text(json.dumps(dict(outputs={'continuous_daily.parquet': {
        'sha256': sha256_file(material/'continuous_daily.parquet')}})))
    terms = tmp_path/'terms'; terms.mkdir()
    (terms/'manifest.json').write_text(json.dumps(dict(dataset='taifex_all_twd_margin_inputs',
        status='margin_inputs_bound_requires_product_accounting_admission', point_in_time_verified=False)))
    output = tmp_path/'release'
    with pytest.raises(ValueError, match='candidate inputs cannot be admitted'):
        publish_all_twd_margin_release(materialization=material, execution_terms=terms/'rules.parquet',
                                      final_settlement=tmp_path/'final.parquet', output=output)
    assert not output.exists()


@pytest.mark.parametrize('product', ['TX', 'MTX'])
def test_admitted_compiler_output_runs_the_shared_margin_account(tmp_path, product):
    """Synthetic integration proof, deliberately not historical admission."""
    from stockagent.data.tw_futures_margin_preparation import index_margin_corporate_execution_rules
    from stockagent.data.tw_futures_margin import attach_futures_margin_rules, TERMINAL_CAPACITY
    from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
    import torch
    source, raw, universe = source_rows()
    source = source.filter(pl.col('product') == 'TX')
    raw = raw.filter(pl.col('product') == 'TX')
    universe = universe.filter(pl.col('product') == 'TX')
    if product != 'TX':
        source = source.with_columns(pl.lit(product).alias('product'),
            pl.col('physical_contract').str.replace('TX',product),
            pl.col('physical_instance').str.replace('TX',product))
        raw = raw.with_columns(pl.lit(product).alias('product'))
        universe = universe.with_columns(pl.lit(product).alias('product'))
    daily, _ = materialize_margin_market_rows(source, raw, universe)
    material = tmp_path/'material'; material.mkdir()
    daily.write_parquet(material/'continuous_daily.parquet')
    dependencies = daily.select('date', 'physical_contract', 'previous_symbol_date').with_columns(
        *[pl.lit(False).alias(c) for c in ('missing_valuation', 'intermediate_calendar_gap', 'unresolved_lifetime')])
    dependencies.write_parquet(material/'execution_dependencies.parquet')
    (material/'manifest.json').write_text(json.dumps(dict(materialization_version=1,
        requested_products=[product], zero_print_products=[], products=1,
        contract_version=6, feature_contract_version=3, fixed_model_output_slots=2816,
        outputs={p.name: {'sha256': sha256_file(p)} for p in material.glob('*.parquet')})))
    account = daily.filter(pl.col('previous_symbol_date').is_not_null()).with_columns(
        pl.lit(200. if product == 'TX' else 50.).alias('contract_multiplier'))
    rules = account.select('date', 'physical_contract').with_columns(
        pl.lit('fixed_twd').alias('margin_kind'), pl.lit(2000.).alias('initial'), pl.lit(1500.).alias('maintenance'),
        pl.lit(2000.).alias('settlement_initial'), pl.lit(1500.).alias('settlement_maintenance'),
        *[pl.lit('2026-01-01T00:00:00+08:00').alias(c) for c in ('known_at', 'effective_at', 'settlement_known_at', 'settlement_effective_at')],
        pl.lit('13:45:00').alias('settlement_time'), pl.lit('TX').alias('position_group'),
        pl.lit(1.).alias('position_unit'), pl.lit(12000.).alias('position_limit'),
        pl.lit(120.).alias('upper_limit'), pl.lit(80.).alias('lower_limit'))
    rules = index_margin_corporate_execution_rules(account, rules).with_columns(
        pl.col('position_group').alias('second_position_group'), pl.col('position_unit').alias('second_position_unit'),
        pl.col('position_limit').alias('second_position_limit'),
        pl.lit(False).alias('position_grandfather_existing'), pl.lit(False).alias('second_position_grandfather_existing'),
        pl.col('opening_contract_value_twd').alias('opening_margin_value_twd'),
        pl.col('settlement_contract_value_twd').alias('settlement_margin_value_twd'),
        pl.col('known_margin_contract_value_twd').alias('known_margin_value_twd'))
    terms_dir = tmp_path/'terms'; terms_dir.mkdir()
    rules.write_parquet(terms_dir/'rules.parquet')
    pl.DataFrame(schema={'point_in_time_verified': pl.Boolean}).write_parquet(terms_dir/'corporate.parquet')
    corporate_proof = dict(path='corporate.parquet', sha256=sha256_file(terms_dir/'corporate.parquet'))
    (terms_dir/'manifest.json').write_text(json.dumps(dict(dataset='taifex_futures_margin_execution_terms',
        schema_version=6, status='complete', point_in_time_verified=True,
        source_materialization_sha256=sha256_file(material/'manifest.json'),
        outputs={'rules': {'sha256': sha256_file(terms_dir/'rules.parquet')}}, sources=[corporate_proof],
        adjusted_terminal_components=dict(status='admitted', corporate_terms=corporate_proof))))
    final_dir=tmp_path/'final'; final_dir.mkdir()
    receipt=final_dir/'source.json'; receipt.write_text('{"synthetic_test_only":true}')
    final=final_dir/'final.parquet'
    pl.DataFrame([dict(product=product, contract='202601', settlement_date=date(2026, 1, 21),
        final_settlement_price=110., final_settlement_value=None, reported_date_role='final_settlement_day',
        settlement_method='cash_settlement', source_sha256=sha256_file(receipt))]).write_parquet(final)
    (final_dir/'manifest.json').write_text(json.dumps(dict(schema_version=2,status='complete',
        requires_product_specific_settlement_clock=True, receipts=[dict(path='source.json',sha256=sha256_file(receipt))],
        outputs={'futures_final_settlement_history':{'sha256':sha256_file(final)}})))
    output=tmp_path/'release'
    result=publish_all_twd_margin_release(materialization=material,execution_terms=terms_dir/'rules.parquet',
                                         final_settlement=final,output=output)
    assert result['runtime_training_verified'] is False
    panel=_stock_panel(rows=4)
    attached=attach_stock_context_futures_portfolio_daily(panel, result['daily'],
        fee_per_side_twd_by_group={'standard':40.},integer_contracts=True,
        denomination_context_basis='prior_settlement',max_volume_participation=.5,
        futures_slot_count=2816,margin_rules_path=result['rules'],final_settlement_path=final)
    attached=attach_futures_margin_rules(attached,result['rules'])
    assert not attached.stock_context_futures_portfolio_daily.benchmark_log_returns.any()
    execution=torch.from_numpy(attached.stock_context_futures_portfolio_daily.integer_execution[1:, :1])
    assert execution[0,0,TERMINAL_CAPACITY]==0
    weights=torch.tensor([[.1],[.1],[.1]],requires_grad=True)
    backtest=run_tw_futures_portfolio_integer_torch(weights,execution,initial_capital=100_000_000.)
    assert backtest.final_alive
    assert torch.isfinite(backtest.strategy_returns).all()
    backtest.strategy_returns.sum().backward()
    assert torch.isfinite(weights.grad).all()
