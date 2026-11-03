from datetime import date

import pytest

from scripts.publish_taifex_fx_option_settlements import parse_raw_cells


def page(*, selection=('22', '23'), second='RTO'):
    fields = ''.join(f'<select name="{name}"><option selected value="{value}">{value}</option></select>'
                     for name, value in [('start_year', '2016'), ('start_month', '07'),
                                         ('end_year', '2022'), ('end_month', '09')])
    fields += ''.join(f'<input name="commodityIds" value="{value}" checked>' for value in selection)
    return (fields + '<table><thead><tr><th>最後結算日</th><th>契約月份</th>'
            f'<th>商品（RHO）</th><th>商品（{second}）</th></tr></thead><tbody>'
            '<tr><td>2021/05/20</td><td>202105</td><td>6.4396</td><td>-</td></tr>'
            '<tr><td>2021/05/19</td><td>202105</td><td>-</td><td>6.4295</td></tr>'
            '</tbody></table>').encode()


def test_separate_product_dates_and_blank_cells_are_preserved():
    frame = parse_raw_cells(page(), date(2016, 7, 1), date(2022, 9, 30))
    assert frame.height == 4
    assert frame.item(0, 'settlement_date') == date(2021, 5, 20)
    assert frame.item(0, 'settlement_price_raw') == '6.4396'
    assert frame.item(1, 'settlement_price_raw') == '-'
    assert not frame.item(1, 'positive_numeric_price')
    assert frame.item(3, 'settlement_date') == date(2021, 5, 19)
    assert frame.item(3, 'settlement_price_raw') == '6.4295'


def test_wrong_requested_selection_or_columns_fail_closed():
    with pytest.raises(ValueError, match='selection'):
        parse_raw_cells(page(selection=('22',)), date(2016, 7, 1), date(2022, 9, 30))
    with pytest.raises(ValueError, match='columns differ'):
        parse_raw_cells(page(second='RHF'), date(2016, 7, 1), date(2022, 9, 30))
