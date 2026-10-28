from datetime import date
import pytest

from stockagent.data.tw_futures_official_block_details import parse_block_detail, response_status


def test_negotiated_combination_preserves_contract_quantity_and_skips_night():
    body = ('組合式序號,日期,契約數,交易時段\n'
            '組合式序號,子序號,契約,到期月份(週別),履約價格,買/賣權,成交價格,成交數量\n'
            'C1,2017/05/16,2,一般,\nC1,1,OAF,201705,-,-,26.22,600,\n'
            'C1,2,OAF,201706,-,-,26.3,600,\n'
            'C2,2017/05/16,1,盤後,\nC2,1,TX,201706,-,-,10000,200,\n').encode('cp950')
    rows = parse_block_detail(body, 'bargain', date(2017,5,16))
    assert rows == [dict(product='OAF',contract='201705',price='26.22',quantity=600),
                    dict(product='OAF',contract='201706',price='26.3',quantity=600)]
    with pytest.raises(ValueError, match='leg count'):
        parse_block_detail(body.replace(b'C1,2017/05/16,2',b'C1,2017/05/16,3'),
                           'bargain', date(2017,5,16))


def test_explicit_empty_is_not_generic_html_or_http_success():
    exact = '<html><script>alert("查無資料");window.location.replace("dlSingleOrderView")</script></html>'.encode()
    assert response_status(exact, 'single') == 'official_explicit_empty'
    assert response_status(b'<html>timeout</html>', 'single') == 'rejected_html'
