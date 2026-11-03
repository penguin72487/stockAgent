from datetime import date

import polars as pl
import pytest

from scripts.audit_stock_option_settlement_collection import compare_rows, native_rows


def page(month='02', price='12.30', underlying='0050'):
    return f'''<html><select name="queryYear"><option value="2011" selected>2011</option></select>
    <select name="queryMonth"><option value="{month}" selected>{month}</option></select>
    <table><thead><tr><th>商品名稱</th><th>商品代號</th><th>標的證券代號</th>
    <th>到期日</th><th>契約月份</th><th>最後結算價</th><th>約定標的物價值</th></tr></thead>
    <tbody><tr><td><span>測試</span> 選擇權</td><td>NYO</td><td>{underlying}</td>
    <td>2011/01/31</td><td>201102</td><td>{price}</td><td>123,000</td></tr></tbody></table></html>'''.encode()


def frame():
    return pl.DataFrame({'source_row': [1], 'product_name_raw': ['測試 選擇權'], 'product_raw': ['NYO'],
                         'underlying_raw': ['0050'], 'settlement_date': [date(2011, 1, 31)],
                         'series_raw': ['201102'], 'settlement_price_raw': ['12.30'],
                         'contract_value_raw': ['123,000'], 'supported_key': [True],
                         'positive_numeric_price': [True]})


def test_native_html_readback_retains_values_and_early_settlement():
    rows = native_rows(page(), '2011-02')
    result = compare_rows(rows, frame())
    assert result['positive_price_rows'] == 1
    assert result['underlying_leading_zero_rows'] == 1
    assert result['settled_before_contract_month_rows'] == 1


def test_month_and_source_value_mismatches_are_rejected():
    with pytest.raises(ValueError, match='selection'):
        native_rows(page(month='03'), '2011-02')
    with pytest.raises(ValueError, match='source value mismatch'):
        compare_rows(native_rows(page(price='12.3'), '2011-02'), frame())


def test_native_html_preserves_an_empty_official_page():
    empty = page().decode().replace(
        '<tr><td><span>測試</span> 選擇權</td><td>NYO</td><td>0050</td>\n    '
        '<td>2011/01/31</td><td>201102</td><td>12.30</td><td>123,000</td></tr>',
        '<tr><td>查無資料</td></tr>')
    assert native_rows(empty.encode(), '2011-02') == []
