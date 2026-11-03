from datetime import date

import pytest

from scripts.publish_taifex_index_option_settlements import parse


def page(*, checked='checked', expiry='202610F4'):
    fields = ''.join(f'<select name="{name}"><option selected value="{value}">{value}</option></select>'
                     for name, value in [('start_year', '2011'), ('start_month', '01'),
                                         ('end_year', '2026'), ('end_month', '10')])
    fields += ''.join(f'<input name="commodityIds" value="{value}" {checked}>'
                      for value in ['2', '8', '9', '11', '14', '16'])
    headers = ['最後結算日', '契約月份'] + [f'商品（{x}）' for x in ['TXO', 'TEO', 'TFO', 'MSO', 'XIO', 'GTO']]
    return (fields + '<table><thead><tr>' + ''.join(f'<th>{x}</th>' for x in headers) +
            '</tr></thead><tbody><tr><td>2026/10/23</td><td>' + expiry +
            '</td><td>46,123.45</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>'
            '</tr></tbody></table>').encode()


def test_friday_series_and_blank_cells_are_preserved():
    frame = parse(page(), date(2011, 1, 1), date(2026, 10, 31))
    assert frame.height == 6
    assert frame.item(0, 'series_raw') == '202610F4'
    assert frame.item(0, 'settlement_price_raw') == '46,123.45'
    assert frame.item(0, 'positive_numeric_price')
    assert frame.item(1, 'settlement_price_raw') == '-'
    assert not frame.item(1, 'positive_numeric_price')


def test_wrong_product_selection_or_unrecognized_series_fails_closed():
    with pytest.raises(ValueError, match='six requested'):
        parse(page(checked=''), date(2011, 1, 1), date(2026, 10, 31))
    with pytest.raises(ValueError, match='unsupported date or series'):
        parse(page(expiry='unknown'), date(2011, 1, 1), date(2026, 10, 31))
