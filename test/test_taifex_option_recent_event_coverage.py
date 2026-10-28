import hashlib
import json
import gzip
import zipfile

import pytest

from scripts.audit_taifex_option_recent_event_coverage import (
    apply_current_stock_comparison_windows, expand_stock_option_windows, observed_products,
)


def test_official_archive_inventory_retains_unknown_products(tmp_path):
    source = tmp_path / 'options.zip'
    with zipfile.ZipFile(source, 'w') as archive:
        archive.writestr('trades.csv', 'date,product,strike,series,right,time,price,qty,auction\n'
                         '20261027,TXO,1,202610,C,084500,1,1,\n'
                         '20261027,NEW,1,202610,P,084501,1,1,\n')
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    assert observed_products(source, digest) == ['NEW', 'TXO']
    with pytest.raises(ValueError, match='digest mismatch'):
        observed_products(source, '0' * 64)


def test_comparison_windows_require_pinned_provisional_inventory(tmp_path):
    inventory = tmp_path / 'inventory.csv'
    inventory.write_text('product,provisional_class\nCFO,security_option_candidate\n'
                         'TGO,commodity_option\n', encoding='utf-8')
    receipt = tmp_path / 'receipt.json'
    receipt.write_text(json.dumps({
        'contract': 'taifex_option_archive_product_day_inventory_v1',
        'product_inventory': str(inventory),
        'product_inventory_sha256': hashlib.sha256(inventory.read_bytes()).hexdigest(),
    }))
    template = {'products': {'TXO': {'day_minutes': [525, 825], 'night_minutes': [900, 300]}}}
    result, pins = expand_stock_option_windows(template, inventory, receipt)
    assert set(result['products']) == {'TXO', 'CFO'}
    assert result['products']['TXO'] == template['products']['TXO']
    assert pins['added_provisional_products'] == ['CFO']
    inventory.write_text(inventory.read_text() + 'NEW,security_option_candidate\n')
    with pytest.raises(ValueError, match='evidence mismatch'):
        expand_stock_option_windows(template, inventory, receipt)


def test_current_exchange_comparison_windows_keep_extended_etf_day(tmp_path):
    body = '''<table><tr><th>股票期貨、選擇權商品代碼</th><th>證券代號</th>
    <th>是否為股票選擇權標的</th><th>一般交易時段交易時間(期貨、選擇權契約)</th></tr>
    <tr><td>OB</td><td>006206</td><td>● 是股票選擇權標的</td><td>8:45~16:15</td></tr>
    </table>'''.encode()
    raw = tmp_path / 'catalog.gz'
    raw.write_bytes(gzip.compress(body))
    receipt = tmp_path / 'current.json'
    receipt.write_text(json.dumps({'contract': 'current_official_family_catalog_membership_readback_v1',
        'source_evidence': [{'url': 'https://www.taifex.com.tw/cht/2/stockLists', 'raw_path': 'catalog.gz',
          'raw_sha256': hashlib.sha256(raw.read_bytes()).hexdigest(), 'body_sha256': hashlib.sha256(body).hexdigest(),
          'checked_at_utc': '2026-10-27T00:00:00+00:00'}]}))
    template = {'products': {'OBO': {'day_minutes': [525, 825], 'night_minutes': None}}}
    result, pins = apply_current_stock_comparison_windows(template, receipt, tmp_path)
    assert result['products']['OBO']['day_minutes'] == [525, 975]
    assert pins['historical_eligibility_verified'] is False
    raw.write_bytes(b'bad')
    with pytest.raises(ValueError, match='raw digest mismatch'):
        apply_current_stock_comparison_windows(template, receipt, tmp_path)
