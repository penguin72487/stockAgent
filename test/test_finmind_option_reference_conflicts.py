from datetime import date, timedelta

import polars as pl
import pytest

from scripts.audit_taifex_finmind_evening_option_contracts import missing_for_day
from scripts.request_finmind_evening_option_contract_checks import covered_by_previous
from scripts.request_finmind_option_reference_conflicts import missing_contracts
from stockagent.data.tw_option_finmind_reference import candidate_rows
from stockagent.data.tw_option_reference_candidates import reference_candidates


SOURCE_DAY = date(2026, 10, 21)
DECISION_DAY = SOURCE_DAY + timedelta(days=1)


def vendor(rows):
    return pl.DataFrame(rows, schema_overrides={
        'date': pl.String, 'option_id': pl.String, 'contract_date': pl.String,
        'ExercisePrice': pl.Float32, 'PutCall': pl.String, 'price': pl.Float32,
        'volume': pl.Int32,
    })


def row(strike, hour):
    return {'date': f'2026-10-21 {hour}:00:00', 'option_id': 'TXO',
            'contract_date': '202610.0', 'ExercisePrice': strike, 'PutCall': 'C',
            'price': 6.0, 'volume': 1}


def references(source):
    rows, _ = candidate_rows(source, day=SOURCE_DAY, source_sha256='a' * 64,
                             candidate_products={'TXO'})
    windows = {'TXO': (525, 825)}
    return pl.concat([
        reference_candidates(rows, trading_date=DECISION_DAY, decision_time=clock,
                             session_windows=windows)
        .with_columns(pl.lit(DECISION_DAY).cast(pl.Date).alias('decision_date'),
                      pl.lit(clock).alias('decision_time'))
        for clock in ('08:45', '09:00')
    ])


def test_only_prior_exchange_contracts_absent_from_whole_day_are_requested():
    official = references(vendor([row(100, '20'), row(200, '13'), row(300, '20')]))
    selected_vendor = references(vendor([row(200, '13')]))
    # A contract absent from the reference view but present in the full vendor
    # object is not a source-day missing key and must not be re-requested.
    complete_vendor = vendor([row(200, '13'), row(300, '13')])
    missing, counts = missing_contracts(official, selected_vendor, complete_vendor,
                                        SOURCE_DAY, DECISION_DAY)
    assert counts['official_only_decision_keys_from_source_day'] == 4
    assert counts['distinct_candidate_contracts'] == 2
    assert counts['absent_from_vendor_natural_day_contracts'] == 1
    assert missing.to_dicts() == [{'product': 'TXO', 'series': '202610',
                                   'strike': 100.0, 'right': 'C'}]


def test_reference_source_rejects_unproven_date_gaps():
    frame = references(vendor([row(100, '20')]))
    with pytest.raises(ValueError, match='consecutive'):
        missing_contracts(frame, frame, vendor([row(100, '20')]),
                          SOURCE_DAY, SOURCE_DAY + timedelta(days=3))


def test_evening_audit_checks_whole_day_and_rejects_float32_collisions():
    official = references(vendor([row(100, '20'), row(200, '13'), row(300, '20')]))
    raw = vendor([row(200, '13'), row(300, '13')])
    missing, reference_rows = missing_for_day(official, raw, SOURCE_DAY)
    assert reference_rows == 4
    assert missing.to_dicts() == [{'product': 'TXO', 'series': '202610',
                                   'strike': 100.0, 'right': 'C'}]
    duplicate = official.filter(pl.col('strike_decimal') == '100').with_columns(
        pl.lit('100.0000001').alias('strike_decimal'))
    with pytest.raises(ValueError, match='Float32'):
        missing_for_day(pl.concat([official, duplicate]), raw, SOURCE_DAY)


def test_only_same_source_and_complete_key_set_reuses_prior_product_query():
    original = {'receipt_sha256': 'a' * 64, 'sha256': 'b' * 64}
    row = {'source_day': '2026-10-21', 'product': 'TXO', 'original_source': original,
           'missing_contracts': [['TXO', '202610', 100.0, 'C'], ['TXO', '202610', 200.0, 'P']]}
    previous = {'contract': 'finmind_option_tick_product_source_check_v1',
                'scope': 'per_product_physical_day_source_comparison_only_no_training_injection',
                'checks': [{'dataset': 'TaiwanOptionTick', 'partition': '2026-10-21', 'data_id': 'TXO',
                            'original_receipt_sha256': 'a' * 64, 'original_parquet_sha256': 'b' * 64,
                            'audited_missing_contracts': row['missing_contracts']}]}
    assert covered_by_previous(row, previous)
    previous['checks'][0]['audited_missing_contracts'] = row['missing_contracts'][:1]
    assert not covered_by_previous(row, previous)
    previous['checks'][0]['audited_missing_contracts'] = row['missing_contracts']
    previous['checks'][0]['original_parquet_sha256'] = 'c' * 64
    assert not covered_by_previous(row, previous)
