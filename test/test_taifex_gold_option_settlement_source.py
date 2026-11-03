from datetime import date

import pytest

from scripts.publish_taifex_gold_option_settlements import parse


def page(product='TGO', checked='18'):
    fields = ''.join(f'<select name="{name}"><option selected value="{value}">{value}</option></select>'
                     for name, value in [('start_year', '2011'), ('start_month', '01'),
                                         ('end_year', '2026'), ('end_month', '10')])
    return (fields + f'<input name="commodityIds" value="{checked}" checked>'
            f'<table><thead><tr><th>最後結算日</th><th>契約月份</th><th>黃金（{product}）</th></tr></thead>'
            '<tbody><tr><td>2011/02/24</td><td>201102</td><td>5,020.5</td></tr></tbody></table>').encode()


def test_gold_option_preserves_actual_date_and_source_precision():
    frame = parse(page(), date(2011, 1, 1), date(2026, 10, 31))
    assert frame.item(0, 'settlement_date') == date(2011, 2, 24)
    assert frame.item(0, 'product_raw') == 'TGO'
    assert frame.item(0, 'settlement_price_raw') == '5,020.5'


def test_wrong_commodity_selection_or_column_is_rejected():
    with pytest.raises(ValueError, match='selection'):
        parse(page(checked='10'), date(2011, 1, 1), date(2026, 10, 31))
    with pytest.raises(ValueError, match='product column'):
        parse(page(product='GDF'), date(2011, 1, 1), date(2026, 10, 31))


def test_exchange_unclosed_first_row_does_not_merge_later_cells():
    malformed = page().decode().replace('</tr></tbody>',
        '<tr><td>2011/04/28</td><td>201104</td><td>5280</td></tr></tbody>')
    frame = parse(malformed.encode(), date(2011, 1, 1), date(2026, 10, 31))
    assert frame.height == 2
    assert frame['series_raw'].to_list() == ['201102', '201104']
