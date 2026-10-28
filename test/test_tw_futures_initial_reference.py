"""Native opening references value new inventory without creating prior holdings."""
from pathlib import Path
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_execution_terms import compile_execution_terms
from stockagent.data.tw_futures_initial_reference import (
    CONTRACT, COLLECTION_CONTRACT, LISTING_CONTRACT, LISTING_REASON,
    EVENT_BIRTH_CONTRACT, EVENT_COLLECTION_CONTRACT, EVENT_BIRTH_REASONS,
    DISCLOSURE_COLUMNS, REISSUE_REASON, SCHEMA,
    native_reissue_reference, native_listing_reference, native_event_birth_reference,
    read_initial_reference_source,
)
from test_tw_futures_execution_terms import DAYS, inputs


def case():
    x = inputs()
    x['specifications'] = x['specifications'].with_columns(
        pl.lit(2000.).alias('contract_multiplier'))
    return x


def references(**updates):
    row = dict(date=DAYS[0], physical_contract='ABC:202603#g0', product='ABC',
        contract='202603', opening_reference_quote=99.5, reference_contract_multiplier=2000.,
        market_reference_asof=f'{DAYS[0]}T08:45:00+08:00', reference_contract=CONTRACT,
        native_adjustment_reason=REISSUE_REASON, native_contract_issued='Y',
        source_task_id='native-query', source_raw_sha256='a'*64,
        source_parquet_sha256='b'*64, source_receipt_sha256='c'*64,
        definition_content_sha256='d'*64)
    return pl.DataFrame([dict(row, **updates)], schema=SCHEMA)


def test_native_birth_is_valued_but_has_no_prior_inventory_or_synthetic_trade():
    x = case()
    original = x['frame'].clone()
    rules, flags = compile_execution_terms(**x, initial_references=references())
    first = rules.filter(pl.col('date') == DAYS[0]).row(0, named=True)
    assert rules.height == 4 and not flags['has_blocker'].any()
    assert not flags['is_warmup'].any()
    assert first['initial_reference_applied']
    assert first['known_margin_contract_value_twd'] == 99.5*2000.
    assert first['opening_contract_value_twd'] == 100.*2000.
    assert first['upper_limit'] == 109. and first['lower_limit'] == 90.
    assert first['carry_from_physical_contract'] == ''
    assert first['carry_from_date'] is None and first['carry_previous_value_twd'] is None
    assert first['carry_cash_twd'] == 0. and first['carry_quantity_numerator'] == 1
    assert not first['inventory_origin_unresolved']
    assert first['known_at'] == f'{DAYS[0]}T00:45:00+00:00'
    second = rules.filter(pl.col('date') == DAYS[1]).row(0, named=True)
    assert second['carry_from_physical_contract'] == first['physical_contract']
    assert second['carry_from_date'] == DAYS[0]
    assert second['known_margin_contract_value_twd'] == 102.*2000.
    assert second['carry_previous_value_twd'] == 102.*2000.
    assert x['frame'].equals(original)


@pytest.mark.parametrize('units', [100., 1000., 2000., 10000.])
def test_standard_share_units_use_exact_dated_specification_without_rescaling_quotes(units):
    x = case()
    x['specifications'] = x['specifications'].with_columns(
        pl.lit(units).alias('contract_multiplier'))
    rules, flags = compile_execution_terms(**x, initial_references=references(
        reference_contract_multiplier=units))
    first = rules.filter(pl.col('date') == DAYS[0]).row(0,named=True)
    assert first['initial_reference_quote'] == 99.5
    assert first['known_margin_contract_value_twd'] == 99.5*units
    assert first['opening_contract_value_twd'] == 100.*units
    assert first['carry_previous_value_twd'] is None
    assert first['carry_from_physical_contract'] == ''
    assert not flags['has_blocker'].any()


@pytest.mark.parametrize('units', [100., 1000., 10000.])
def test_permitted_standard_unit_cannot_override_another_dated_specification(units):
    with pytest.raises(ValueError,match='exact new generation'):
        compile_execution_terms(**case(),initial_references=references(
            reference_contract_multiplier=units))


def test_reference_is_not_a_fill_when_no_trade_exists():
    x = case()
    x['frame'] = x['frame'].with_columns(
        (pl.col('date') != DAYS[0]).alias('executable'),
        pl.when(pl.col('date') == DAYS[0]).then(None).otherwise(pl.col('open')).alias('open'),
        pl.when(pl.col('date') == DAYS[0]).then(0.).otherwise(1.).alias('volume'))
    rules, flags = compile_execution_terms(**x, initial_references=references())
    first = rules.filter(pl.col('date') == DAYS[0]).row(0, named=True)
    assert first['known_margin_contract_value_twd'] == 199000.
    assert first['opening_contract_value_twd'] == 199000.
    assert not first['inventory_entry_reachable']
    assert x['frame'].filter(pl.col('date') == DAYS[0])['open'].item() is None
    assert not flags['has_blocker'].any()


def test_reissued_generation_never_inherits_same_code_old_generation_value_or_cash():
    from datetime import timedelta
    x = case()
    before = DAYS[0] - timedelta(days=3)
    old = x['frame'].head(1).with_columns(pl.lit(before).alias('date'),
        pl.lit(DAYS[0]).alias('next_market_date'),
        pl.lit('ABC:202603#old').alias('physical_contract'),
        pl.lit(99999.).alias('settlement'))
    x['frame'] = pl.concat([old, x['frame'].with_columns(
        pl.when(pl.col('date') == DAYS[0]).then(pl.lit(before))
          .otherwise(pl.col('previous_market_date')).alias('previous_market_date'))])
    for name in ('positions', 'margins'):
        x[name] = pl.concat([x[name].head(1).with_columns(pl.lit(before).alias('date')), x[name]])
    rules, _ = compile_execution_terms(**x, initial_references=references())
    first = rules.filter(pl.col('date') == DAYS[0]).row(0, named=True)
    assert first['known_margin_contract_value_twd'] == 199000.
    assert first['carry_previous_value_twd'] is None
    assert first['carry_from_physical_contract'] == '' and first['carry_cash_twd'] == 0.


@pytest.mark.parametrize('update', [
    {'opening_reference_quote': 0.}, {'opening_reference_quote': float('nan')},
    {'opening_reference_quote': float('inf')}, {'reference_contract_multiplier': 1000.},
    {'native_adjustment_reason': '除息'}, {'native_contract_issued': 'N'},
    {'source_raw_sha256': 'unbound'}, {'source_receipt_sha256': ''},
    {'reference_contract': 'unversioned'}, {'product': 'WRONG'}, {'contract': '202604'},
    {'market_reference_asof': f'{DAYS[0]}T09:00:00+08:00'},
    {'market_reference_asof': '2025-12-01T08:45:00+08:00'},
    {'market_reference_asof': 'not a clock'},
])
def test_bad_native_evidence_is_rejected(update):
    with pytest.raises(ValueError):
        compile_execution_terms(**case(), initial_references=references(**update))


def test_reference_cannot_overwrite_an_existing_generation_prior_mark():
    with pytest.raises(ValueError, match='exact new generation'):
        compile_execution_terms(**case(), initial_references=references(
            date=DAYS[1], market_reference_asof=f'{DAYS[1]}T08:45:00+08:00'))


def test_duplicate_keys_and_incomplete_schema_are_rejected():
    for bad in (pl.concat([references(), references()]), references().drop('source_receipt_sha256')):
        with pytest.raises(ValueError, match='Unique typed'):
            compile_execution_terms(**case(), initial_references=bad)


def test_no_reference_and_empty_reference_preserve_all_old_financial_values():
    x = case()
    old, old_flags = compile_execution_terms(**x)
    empty, flags = compile_execution_terms(**x, initial_references=pl.DataFrame(schema=SCHEMA))
    assert empty.drop(DISCLOSURE_COLUMNS).equals(old)
    assert flags.equals(old_flags)
    assert not empty['initial_reference_applied'].any()


def test_future_market_values_do_not_change_earlier_rules():
    x = case()
    old, flags = compile_execution_terms(**x, initial_references=references())
    x['frame'] = x['frame'].with_columns(
        pl.when(pl.col('date') == DAYS[-1]).then(9999.).otherwise(pl.col('settlement')).alias('settlement'))
    new, new_flags = compile_execution_terms(**x, initial_references=references())
    assert old.filter(pl.col('date') < DAYS[-1]).equals(new.filter(pl.col('date') < DAYS[-1]))
    assert flags.filter(pl.col('date') < DAYS[-1]).equals(new_flags.filter(pl.col('date') < DAYS[-1]))


def test_source_loader_requires_explicit_contract_and_exact_parquet_sha(tmp_path: Path):
    assert read_initial_reference_source(tmp_path) is None
    folder = tmp_path/'initial_reference'
    folder.mkdir()
    path = folder/'initial_references.parquet'
    references().write_parquet(path)
    manifest = dict(contract=CONTRACT, reference_value_definition_verified=True,
        reference_units_bound_to_dated_specifications=True,
        snapshot_transport_clock_backdated=False, reference_is_execution_price=False,
        outputs={path.name: {'sha256': sha256_file(path)}})
    mp = folder/'manifest.json'
    mp.write_text(json.dumps(manifest))
    assert read_initial_reference_source(tmp_path).equals(references())
    manifest['snapshot_transport_clock_backdated'] = True
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='source-defined'):
        read_initial_reference_source(tmp_path)
    manifest['snapshot_transport_clock_backdated'] = False
    mp.write_text(json.dumps(manifest))
    references(opening_reference_quote=100.).write_parquet(path)
    with pytest.raises(ValueError, match='SHA mismatch'):
        read_initial_reference_source(tmp_path)


def native_row(**updates):
    return dict({'Future Code': 'ABF202603 原生契約名稱', 'Adjusted Date': '2026/01/05',
        '_query_symbol': 'ABF202603', '_query_period': '2026-01-05',
        'Reference Price': '99.50', 'Shares per Contract': '2000',
        'Reasons for Contract Adjustment': REISSUE_REASON,
        'Failure to Contract Adjustment': 'Y'}, **updates)


def test_native_reference_uses_native_identity_date_units_and_literal_value():
    row = native_row()
    parsed = native_reissue_reference(row)
    assert parsed['date'] == DAYS[0] and parsed['product'] == 'ABF' and parsed['contract'] == '202603'
    assert parsed['opening_reference_quote'] == 99.5
    assert parsed['reference_contract_multiplier'] == 2000.
    assert row['Reference Price'] == '99.50' and row['Adjusted Date'] == '2026/01/05'


@pytest.mark.parametrize('changes', [
    {'_query_symbol': 'ABF202604'}, {'_query_period': '2026-01-06'},
    {'Future Code': 'ABF202613 非有效月份'},
    {'Adjusted Date': '2026-01'}, {'Reference Price': '99,50'},
])
def test_native_reference_rejects_wrong_owner_clock_locale_or_nonstandard_contract(changes):
    with pytest.raises(ValueError):
        native_reissue_reference(native_row(**changes))


@pytest.mark.parametrize('changes', [
    {'Reasons for Contract Adjustment': '除息'}, {'Failure to Contract Adjustment': 'N'},
    {'Reference Price': '(null)'}, {'Shares per Contract': None},
    {'Reference Price': '0'}, {'Shares per Contract': '3000'},
    {'Future Code': 'AB1202603 調整契約', '_query_symbol': 'AB1202603'},
])
def test_other_events_and_absent_values_cannot_create_a_native_reference(changes):
    assert native_reissue_reference(native_row(**changes)) is None


@pytest.mark.parametrize('units', ['100','1000','2000','10000'])
def test_native_small_and_etf_standard_units_are_retained_without_a_conversion(units):
    parsed = native_reissue_reference(native_row(**{'Shares per Contract':units}))
    assert parsed['reference_contract_multiplier'] == float(units)
    assert parsed['opening_reference_quote'] == 99.5


def test_populated_listing_reference_preserves_native_role_and_literal_value():
    row = native_row(**{'Reasons for Contract Adjustment': LISTING_REASON})
    parsed = native_listing_reference(row)
    assert parsed['opening_reference_quote'] == 99.5
    assert parsed['native_adjustment_reason'] == LISTING_REASON
    assert parsed['product'] == 'ABF' and parsed['contract'] == '202603'
    assert native_reissue_reference(row) is None
    assert row['Reference Price'] == '99.50'


@pytest.mark.parametrize('changes', [
    {'Reference Price': None}, {'Reference Price': '(null)'}, {'Reference Price': '0'},
    {'Failure to Contract Adjustment': 'N'}, {'Reasons for Contract Adjustment': '除息上市'},
    {'Reasons for Contract Adjustment': REISSUE_REASON}, {'Shares per Contract': '3000'},
    {'Future Code': 'AB1202603 調整契約', '_query_symbol': 'AB1202603'},
])
def test_listing_status_or_other_event_never_invents_a_reference(changes):
    assert native_listing_reference(native_row(**{
        'Reasons for Contract Adjustment': LISTING_REASON, **changes})) is None


def listing_case():
    x = case()
    for name, frame in x.items():
        if not isinstance(frame, pl.DataFrame):
            continue
        expressions = []
        if 'product' in frame.columns:
            expressions.append(pl.lit('ABF').alias('product'))
        if 'physical_contract' in frame.columns:
            expressions.append(pl.col('physical_contract').str.replace('ABC:', 'ABF:'))
        if expressions:
            x[name] = frame.with_columns(expressions)
    return x


def listing_references(**updates):
    return references(**dict(product='ABF', physical_contract='ABF:202603#g0',
        reference_contract=LISTING_CONTRACT, native_adjustment_reason=LISTING_REASON, **updates))


def test_new_listing_reference_has_empty_origin_and_exact_same_financial_mechanism():
    x = listing_case()
    old, old_flags = compile_execution_terms(**x, initial_references=references(
        product='ABF', physical_contract='ABF:202603#g0'))
    new, new_flags = compile_execution_terms(**x, initial_references=listing_references())
    assert new.equals(old) and new_flags.equals(old_flags)
    first = new.head(1).row(0, named=True)
    assert first['carry_from_physical_contract'] == ''
    assert first['carry_previous_value_twd'] is None
    assert first['carry_cash_twd'] == 0.
    assert first['known_margin_contract_value_twd'] == 99.5*2000.


@pytest.mark.parametrize('updates', [
    {'reference_contract': CONTRACT}, {'native_adjustment_reason': REISSUE_REASON},
    {'native_adjustment_reason': '除息'}, {'product': 'AB1'},
    {'date': DAYS[1], 'market_reference_asof': f'{DAYS[1]}T08:45:00+08:00'},
])
def test_listing_reference_rejects_wrong_role_or_existing_inventory(updates):
    row = listing_references().row(0, named=True)
    with pytest.raises(ValueError):
        compile_execution_terms(**listing_case(), initial_references=pl.DataFrame(
            [dict(row, **updates)], schema=SCHEMA))


def test_collection_manifest_requires_event_contracts_and_legacy_cannot_admit_listing(tmp_path):
    folder = tmp_path/'initial_reference'
    folder.mkdir()
    path = folder/'initial_references.parquet'
    listing_references().write_parquet(path)
    manifest = dict(contract=CONTRACT, reference_value_definition_verified=True,
        reference_units_bound_to_dated_specifications=True,
        snapshot_transport_clock_backdated=False, reference_is_execution_price=False,
        outputs={path.name: {'sha256': sha256_file(path)}})
    mp = folder/'manifest.json'
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='Legacy'):
        read_initial_reference_source(tmp_path)
    manifest['contract'] = COLLECTION_CONTRACT
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='event contracts'):
        read_initial_reference_source(tmp_path)
    manifest['reference_event_contracts'] = {CONTRACT: REISSUE_REASON, LISTING_CONTRACT: LISTING_REASON}
    mp.write_text(json.dumps(manifest))
    assert read_initial_reference_source(tmp_path).equals(listing_references())


@pytest.mark.parametrize('reason', EVENT_BIRTH_REASONS)
def test_populated_native_event_birth_keeps_literal_value_but_no_old_holder_entitlement(reason):
    row = native_row(**{'Reasons for Contract Adjustment': reason,
        'Cash Dividends per Unit': '7250', 'Stock Dividend per Share': '0.5'})
    parsed = native_event_birth_reference(row)
    assert parsed['opening_reference_quote'] == 99.5
    assert parsed['reference_contract_multiplier'] == 2000.
    assert parsed['native_adjustment_reason'] == reason
    assert native_reissue_reference(row) is None
    assert native_listing_reference(row) is None
    assert 'carry_cash_twd' not in parsed
    assert row['Cash Dividends per Unit'] == '7250'
    old, old_flags = compile_execution_terms(**listing_case(), initial_references=listing_references())
    event_ref = listing_references().with_columns(
        pl.lit(EVENT_BIRTH_CONTRACT).alias('reference_contract'),
        pl.lit(reason).alias('native_adjustment_reason'))
    new, new_flags = compile_execution_terms(**listing_case(), initial_references=event_ref)
    assert new.equals(old) and new_flags.equals(old_flags)
    assert new.head(1)['carry_cash_twd'].item() == 0.
    assert new.head(1)['carry_previous_value_twd'].item() is None


@pytest.mark.parametrize('changes', [
    {'Reference Price': None}, {'Reference Price': '(null)'}, {'Reference Price': '0'},
    {'Reference Price': '-1'}, {'Failure to Contract Adjustment': 'N'},
    {'Reasons for Contract Adjustment': '未知事件'}, {'Reasons for Contract Adjustment': '上市'},
    {'Reasons for Contract Adjustment': REISSUE_REASON}, {'Shares per Contract': '3000'},
    {'Future Code': 'AB1202603 調整契約', '_query_symbol': 'AB1202603'},
])
def test_event_birth_parser_does_not_invent_missing_values_or_broaden_event_or_generation(changes):
    assert native_event_birth_reference(native_row(**{
        'Reasons for Contract Adjustment': '除息上市', **changes})) is None


@pytest.mark.parametrize('changes', [
    {'_query_symbol': 'ABF202604'}, {'_query_period': '2026-01-06'},
    {'Adjusted Date': '2026-01'}, {'Reference Price': '99,50'},
])
def test_event_birth_parser_rejects_conflicting_native_identity(changes):
    with pytest.raises(ValueError):
        native_event_birth_reference(native_row(**{
            'Reasons for Contract Adjustment': '除息上市', **changes}))


@pytest.mark.parametrize('updates', [
    {'reference_contract': LISTING_CONTRACT}, {'native_adjustment_reason': LISTING_REASON},
    {'native_adjustment_reason': '未知事件'}, {'product': 'AB1'},
    {'date': DAYS[1], 'market_reference_asof': f'{DAYS[1]}T08:45:00+08:00'},
])
def test_event_birth_reference_rejects_wrong_role_or_existing_inventory(updates):
    row = listing_references().row(0, named=True)
    row.update(reference_contract=EVENT_BIRTH_CONTRACT, native_adjustment_reason='除息上市')
    with pytest.raises(ValueError):
        compile_execution_terms(**listing_case(), initial_references=pl.DataFrame(
            [dict(row, **updates)], schema=SCHEMA))


def test_event_birth_reference_cannot_replace_a_real_corporate_carry_boundary():
    from datetime import timedelta
    from test_tw_futures_execution_terms import CORPORATE_SCHEMA, STAMP
    x = listing_case()
    x['frame'] = x['frame'].with_columns(
        pl.when(pl.col('date') == DAYS[0]).then(pl.lit(DAYS[0]-timedelta(days=3)))
          .otherwise(pl.col('previous_market_date')).alias('previous_market_date'))
    x['corporate_terms'] = pl.DataFrame([dict(product='ABF', contract='202603',
        effective_date=str(DAYS[0]), valid_until_exclusive=None, from_product='ABF',
        contract_multiplier=2000., deliverable_cash_twd=0., fixed_subscription_rights_twd=0.,
        subscription_rights_at_final_settlement=False, known_at=STAMP,
        source_content_sha256s=['d'*64], effective_at=None, equity_cash_credit_twd=7250.,
        has_equity_credit_fields=True, carry_quantity_numerator=1, carry_quantity_denominator=1)],
        schema=CORPORATE_SCHEMA)
    rows = listing_references().with_columns(
        pl.lit(EVENT_BIRTH_CONTRACT).alias('reference_contract'),
        pl.lit('除息上市').alias('native_adjustment_reason'))
    with pytest.raises(ValueError, match='exact new generation'):
        compile_execution_terms(**x, initial_references=rows)


def test_event_birth_collection_requires_versioned_exact_roles_and_cannot_enter_v1(tmp_path):
    folder = tmp_path/'initial_reference'
    folder.mkdir()
    path = folder/'initial_references.parquet'
    rows = listing_references().with_columns(
        pl.lit(EVENT_BIRTH_CONTRACT).alias('reference_contract'),
        pl.lit('除息上市').alias('native_adjustment_reason'))
    rows.write_parquet(path)
    roles = {CONTRACT: REISSUE_REASON, LISTING_CONTRACT: LISTING_REASON}
    manifest = dict(contract=COLLECTION_CONTRACT, reference_value_definition_verified=True,
        reference_units_bound_to_dated_specifications=True, reference_event_contracts=roles,
        snapshot_transport_clock_backdated=False, reference_is_execution_price=False,
        outputs={path.name: {'sha256': sha256_file(path)}})
    mp = folder/'manifest.json'
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='event contracts'):
        read_initial_reference_source(tmp_path)
    manifest['contract'] = EVENT_COLLECTION_CONTRACT
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='event contracts'):
        read_initial_reference_source(tmp_path)
    roles[EVENT_BIRTH_CONTRACT] = list(EVENT_BIRTH_REASONS)
    mp.write_text(json.dumps(manifest))
    assert read_initial_reference_source(tmp_path).equals(rows)
    roles[EVENT_BIRTH_CONTRACT].append('未知事件')
    mp.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='event contracts'):
        read_initial_reference_source(tmp_path)
