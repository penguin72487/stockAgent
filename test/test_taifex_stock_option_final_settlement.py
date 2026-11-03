from datetime import date
from pathlib import Path

import pytest

from downloader.artifact_io import sha256_file
from scripts.download_taifex_stock_option_final_settlement import implementation_compatible, parse
import scripts.download_taifex_stock_option_final_settlement as collector


def page(rows, *, year='2011', month='02', headers=None):
    headers = headers or ['商品名稱', '商品代號', '標的證券代號', '到期日', '契約月份', '最後結算價']
    return (f'<select name="queryYear"><option selected value="{year}">{year}</option></select>'
            f'<select name="queryMonth"><option selected value="{month}">{month}</option></select>'
            '<table><thead><tr>' + ''.join(f'<th>{x}</th>' for x in headers) +
            '</tr></thead><tbody>' + rows + '</tbody></table>').encode()


def test_monthly_source_preserves_underlying_zeroes_and_early_settlement():
    frame = parse(page('<tr><td>ETF</td><td>OBO</td><td>006206</td><td>2011/01/19</td>'
                       '<td>201102</td><td>1,012.30</td></tr>'), 2011, 2)
    row = frame.row(0, named=True)
    assert row['underlying_raw'] == '006206'
    assert row['settlement_date'] == date(2011, 1, 19)
    assert row['settlement_price_raw'] == '1,012.30'
    assert row['supported_key'] and row['positive_numeric_price']


def test_weekly_and_unsupported_values_are_retained():
    frame = parse(page('<tr><td>ETF</td><td>OBO</td><td>006206</td><td>2011/02/16</td>'
                       '<td>201102W2</td><td>--</td></tr>'), 2011, 2)
    assert frame.item(0, 'series_raw') == '201102W2'
    assert frame.item(0, 'supported_key')
    assert not frame.item(0, 'positive_numeric_price')


def test_response_must_echo_requested_month_and_valid_dates():
    with pytest.raises(ValueError, match='echo'):
        parse(page('', month='03'), 2011, 2)
    with pytest.raises(ValueError, match='malformed'):
        parse(page('<tr><td>ETF</td><td>OBO</td><td>006206</td><td>2011/02/xx</td>'
                   '<td>201102</td><td>20</td></tr>'), 2011, 2)
    with pytest.raises(ValueError, match='expected exactly one'):
        parse(page('', headers=['wrong']), 2011, 2)


def test_explicit_empty_is_not_a_fabricated_zero_settlement():
    assert parse(page('<tr><td>查無資料</td></tr>'), 2011, 2).is_empty()


def test_legacy_smoke_requires_exact_snapshot_and_other_dependencies(tmp_path):
    snapshot = tmp_path / 'old.py'
    snapshot.write_text('old producer')
    script = str(Path(collector.__file__).resolve())
    previous = {script: sha256_file(snapshot), '/dependency': 'same'}
    current = {script: 'new', '/dependency': 'same'}
    assert implementation_compatible(previous, current, snapshot)
    assert not implementation_compatible(previous, {script: 'new', '/dependency': 'changed'}, snapshot)
    snapshot.write_text('changed')
    assert not implementation_compatible(previous, current, snapshot)
