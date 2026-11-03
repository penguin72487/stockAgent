from datetime import date
import json

import polars as pl
import pytest

from scripts.audit_tx_reopen_finmind_ticks import audit_day
from scripts.audit_tx_reopen_independent_readback import readback
from scripts.build_tx_contract_day_audit_projection import project
from downloader.artifact_io import sha256_file
from stockagent.data.tw_stock_futures_repair import apply_block_evidence


def _official(**changes):
    value = {'date': date(2011, 3, 16), 'product': 'TX', 'contract': '201104',
             'official_expiry': date(2011, 4, 20), 'official_source_sha256': 'book-sha',
             'outright_volume': 4, 'official_open': '100', 'official_high': '102',
             'official_low': '100', 'official_close': '100', 'official_row_observed': True,
             'source_row_observed': True, 'accounting_only_date': False, 'official_volume': 4,
             'block_volume': 0}
    return {**value, **changes}


def _ticks():
    return pl.DataFrame({'date': ['2011-03-16 08:45:00', '2011-03-16 13:31:01',
                                 '2011-03-16 13:31:01', '2011-03-16 13:45:00'],
                         'futures_id': ['TX'] * 4, 'contract_date': ['201104'] * 4,
                         'price': [100., 101., 102., 100.], 'volume': [2, 2, 2, 2]})


def test_reopen_probe_retains_multiple_same_second_prices():
    result = audit_day(_ticks(), day=date(2011, 3, 16), contract='201104',
                       official=_official(), role='expiry_reopen')
    assert result['full_book_matches']
    assert result['observed_matched_contracts'] == 4
    probe = result['entry_probes'][0]
    assert probe['timestamp'] == '2011-03-16 13:31:01'
    assert probe['distinct_prices'] == [101., 102.]
    assert not probe['same_second_price_unambiguous']
    assert 'price' not in probe


def test_volume_mismatch_and_odd_native_volume_are_not_admitted():
    mismatched = audit_day(_ticks(), day=date(2011, 3, 16), contract='201104',
                           official=_official(outright_volume=5), role='expiry_reopen')
    assert mismatched['valid_tick_values'] and not mismatched['full_book_matches']
    odd = _ticks().with_columns(pl.when(pl.col('price') == 102.).then(3)
                                .otherwise(pl.col('volume')).alias('volume'))
    invalid = audit_day(odd, day=date(2011, 3, 16), contract='201104',
                        official=_official(), role='expiry_reopen')
    assert not invalid['valid_tick_values'] and not invalid['full_book_matches']


def test_official_block_volume_is_not_counted_as_regular_book(tmp_path):
    raw = tmp_path / 'block.csv'
    raw.write_bytes('交易日期,契約,到期月份(週別),成交數量,交易時段\n2011/03/16,TX,201104,2,一般\n'.encode('cp950'))
    manifest = tmp_path / 'manifest.json'
    manifest.write_text(json.dumps({'source': 'taifex_official_block_volume_inventory_v1',
        'sources': [{'path': str(raw), 'url': 'https://www.taifex.com.tw/cht/3/dlProductOrderDown',
                     'sha256': sha256_file(raw), 'source_first_date': '2011-01-01',
                     'source_last_date': '2011-12-31', 'method': 'POST', 'http_status': 200,
                     'form': {'queryStartDate': '2011/01/01', 'queryEndDate': '2011/12/31'}}]}))
    gross = pl.DataFrame([_official(outright_volume=6)]).drop('block_volume')
    regular, _ = apply_block_evidence(gross, manifest)
    result = audit_day(_ticks(), day=date(2011, 3, 16), contract='201104',
                       official=regular.to_dicts()[0], role='expiry_reopen')
    assert result['full_book_matches']
    assert result['official_block_volume'] == 2
    assert result['official_regular_outright_volume'] == 4


def test_known_final_day_does_not_claim_post_expiry_prints():
    result = audit_day(_ticks(), day=date(2011, 3, 16), contract='201104',
                       official=_official(official_expiry=date(2011, 3, 16)),
                       role='expiry_reopen')
    assert result['session_envelope'][1] == '13:30:00'
    assert not result['entry_probes'][0]['observed']
    assert not result['full_book_matches']


def test_projection_pins_input_and_rejects_ambiguous_contract_days(tmp_path):
    source = tmp_path / 'daily.parquet'
    pl.DataFrame([_official()]).write_parquet(source)
    digest = sha256_file(source)
    assert project(source, digest, '2011-01-01', '2011-12-31').height == 1
    with pytest.raises(ValueError, match='digest'):
        project(source, '0' * 64, '2011-01-01', '2011-12-31')
    pl.DataFrame([_official(), _official()]).write_parquet(source)
    with pytest.raises(ValueError, match='ambiguous'):
        project(source, sha256_file(source), '2011-01-01', '2011-12-31')


def test_independent_reader_rechecks_raw_ticks_and_rejects_changes(tmp_path):
    payload = tmp_path / 'ticks.parquet'
    _ticks().write_parquet(payload)
    receipt = tmp_path / 'receipt.json'
    receipt.write_text('{}')
    block = tmp_path / 'block.csv'
    block.write_bytes('交易日期,契約,到期月份(週別),成交數量,交易時段\n2011/03/16,TX,201104,2,一般\n'.encode('cp950'))
    block_manifest = tmp_path / 'block.json'
    block_manifest.write_text(json.dumps({'source': 'taifex_official_block_volume_inventory_v1',
        'sources': [{'path': str(block), 'url': 'https://www.taifex.com.tw/cht/3/dlProductOrderDown',
                     'sha256': sha256_file(block), 'source_first_date': '2011-01-01',
                     'source_last_date': '2011-12-31'}]}))
    projection = tmp_path / 'projection.parquet'
    pl.DataFrame([_official(outright_volume=6)]).write_parquet(projection)
    result = audit_day(_ticks(), day=date(2011, 3, 16), contract='201104',
                       official=_official(block_volume=2), role='expiry_reopen')
    audit = tmp_path / 'audit.json'
    audit.write_text(json.dumps({'contract': 'tx_retained_finmind_contract_day_clock_audit_v2',
        'inputs_sha256': {str(projection): sha256_file(projection),
                          str(block_manifest): sha256_file(block_manifest)},
        'counts': {'target_contract_days': 1}, 'rows': [result],
        'source_objects': [{'date': '2011-03-16', 'path': str(payload),
                            'sha256': sha256_file(payload), 'receipt_path': str(receipt),
                            'receipt_sha256': sha256_file(receipt)}]}))
    verified = readback(audit, projection, block_manifest, ['2011-03-16'])
    assert verified['all_contract_days_block_and_quantity_arithmetic_checked'] == 1
    assert verified['sample_native_pyarrow_readback'][0]['source_rows'] == 4
    receipt.write_text('{"changed":true}')
    with pytest.raises(ValueError, match='receipt changed'):
        readback(audit, projection, block_manifest, ['2011-03-16'])
