from datetime import date
import hashlib
import json
from pathlib import Path
import zipfile

import polars as pl
import pytest

from stockagent.data.tw_futures_official_ticks import pin_official_option_tick_inventory
from stockagent.data.tw_option_product_ticks import iter_official_option_trade_batches
from stockagent.data.tw_option_source_scope import annotate_option_tick_source_scope, load_option_source_scope
from scripts.audit_taifex_option_original_daily_presence import audit


def _source(tmp_path: Path, *, extra: str = '') -> tuple[dict, Path]:
    archive = tmp_path / 'OptionsDaily_2026_10_06.zip'
    header = ('成交日期,商品代號,履約價格,到期月份(週別),買賣權別,成交時間,成交價格,'
              '成交數量(B or S),開盤集合競價\n----------\n')
    rows = ('20261006,CAO,23.50,202610,P,090001,0.1500,1,*\n'
            '20261006,CAO,23.50,202610,P,090001,0.1500,1,*\n'
            '20261005,TXO,23000,202610W1,C,150000,50,2,\n' + extra)
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('OptionsDaily_2026_10_06.csv', (header + rows).encode('cp950'))
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    receipt = tmp_path / 'manifest.json'
    receipt.write_text(json.dumps({'dataset': 'taifex_recent_tx_txo_trade_by_trade', 'status': 'complete',
        'raw_downloads': [{'kind': 'options', 'trading_date': '2026-10-06', 'path': str(archive),
                           'sha256': sha, 'bytes': archive.stat().st_size,
                           'url': 'https://www.taifex.com.tw/file/taifex/Dailydownload/'
                                  'OptionsDailydownloadCSV/OptionsDaily_2026_10_06.zip'}]}))
    return pin_official_option_tick_inventory(receipt, [date(2026, 10, 6)])[0], receipt


def test_source_preserves_side_rows_decimals_and_both_dates(tmp_path):
    item, _ = _source(tmp_path)
    frame = pl.concat(list(iter_official_option_trade_batches(item)))
    assert frame['product_raw'].to_list() == ['CAO', 'CAO', 'TXO']
    assert frame['strike_raw'].to_list() == ['23.50', '23.50', '23000']
    assert frame['price_raw'].to_list() == ['0.1500', '0.1500', '50']
    assert frame['source_row_number'].to_list() == [3, 4, 5]
    assert frame['event_date'].to_list() == [date(2026, 10, 6), date(2026, 10, 6), date(2026, 10, 5)]
    assert set(frame['trading_date']) == {date(2026, 10, 6)}
    assert frame['event_ts'].dtype == pl.Datetime('us', 'Asia/Taipei')
    assert 'session' not in frame.columns and 'executable' not in frame.columns


def test_product_filter_keeps_source_identity_and_duplicates(tmp_path):
    item, _ = _source(tmp_path)
    frame = pl.concat(list(iter_official_option_trade_batches(item, products={'CAO'})))
    assert frame['source_row_number'].to_list() == [3, 4]
    assert set(frame['source_sha256']) == {item['sha256']}
    with pytest.raises(ValueError, match='explicit option codes'):
        list(iter_official_option_trade_batches(item, products={'cao'}))


@pytest.mark.parametrize('extra', [
    '20261007,CAO,23.5,202610,P,090001,1,1,\n',
    '20261006,CAO,23.5,202610,P,246001,1,1,\n',
    '20261006,CAO,23.5,202610,P,090001,1,0,\n',
])
def test_invalid_events_fail_closed_even_outside_selected_products(tmp_path, extra):
    item, _ = _source(tmp_path, extra=extra)
    with pytest.raises(ValueError, match='invalid TAIFEX option source row'):
        list(iter_official_option_trade_batches(item, products={'TXO'}))


def test_source_mutation_during_iteration_is_detected(tmp_path):
    item, _ = _source(tmp_path)
    batches = iter_official_option_trade_batches(item)
    assert next(batches).height == 3
    with Path(item['path']).open('ab') as stream:
        stream.write(b'changed')
    with pytest.raises(ValueError, match='changed'):
        list(batches)


def test_receipt_mutation_during_iteration_is_detected(tmp_path):
    item, receipt = _source(tmp_path)
    batches = iter_official_option_trade_batches(item)
    assert next(batches).height == 3
    receipt.write_text(receipt.read_text() + ' ')
    with pytest.raises(ValueError, match='changed'):
        list(batches)


def test_pinned_receipt_rejects_other_vendor_url(tmp_path):
    _, receipt = _source(tmp_path)
    content = json.loads(receipt.read_text())
    content['raw_downloads'][0]['url'] = 'https://example.invalid/OptionsDaily_2026_10_06.zip'
    receipt.write_text(json.dumps(content))
    with pytest.raises(ValueError, match='identity, URL'):
        pin_official_option_tick_inventory(receipt, [date(2026, 10, 6)])


def test_daily_audit_compares_decimal_identity_and_physical_day(tmp_path):
    _, receipt = _source(tmp_path)
    daily = tmp_path / 'daily.csv'
    daily.write_bytes(('交易日期,契約,到期月份(週別),履約價,買賣權,開盤價,收盤價,結算價,成交量,交易時段\n'
        '2026/10/06,CAO,202610,23.5,賣權,0.15,0.15,0.15,1,一般\n'
        '2026/10/06,TXO,202610W1,23000,買權,50,50,50,1,一般\n'
        '2026/10/06,CAO,unknown,23.5,賣權,0.15,0.15,0.15,1,一般\n').encode('cp950'))
    result = audit(date(2026, 10, 6), daily, hashlib.sha256(daily.read_bytes()).hexdigest(), receipt)
    assert result['official_positive_supported_contracts'] == 2
    assert result['expected_contracts_with_tick'] == 1
    assert result['expected_contracts_without_tick'] == 1
    assert result['missing_contracts'][0]['product'] == 'TXO'
    assert result['unsupported_daily_key_rows'] == 1


def test_tick_scope_uses_shared_catalog_without_dropping_unknown_products(tmp_path):
    item, _ = _source(tmp_path, extra='20261006,XAO,10,202610,C,091000,1,2,\n'
                                     '20261006,ZZO,10,202610,P,091100,1,2,\n')
    frame = pl.concat(list(iter_official_option_trade_batches(item)))
    inventory = tmp_path / 'inventory.csv'
    inventory.write_text('product,provisional_class\nCAO,security_option_candidate\n'
                         'TXO,index_option\nXAO,etf_option_candidate\n')
    scope = load_option_source_scope(inventory, expected_sha256=hashlib.sha256(inventory.read_bytes()).hexdigest(),
                                     scopes=['etf_options', 'index_options'])
    result = annotate_option_tick_source_scope(frame, scope)
    assert result['product'].to_list() == ['CAO', 'CAO', 'TXO', 'XAO', 'ZZO']
    assert result['source_scope_match'].to_list() == [False, False, True, True, False]
    assert result['source_scope_class'].to_list()[-1] == 'unclassified'
    assert len(set(result['tick_scope_fingerprint'])) == 1
    with pytest.raises(ValueError, match='source contract differs'):
        annotate_option_tick_source_scope(frame.with_columns(pl.lit('unknown').alias('source_contract')), scope)
