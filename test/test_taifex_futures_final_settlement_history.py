from __future__ import annotations

from datetime import date
import pytest

from scripts.download_taifex_futures_final_settlement_history import (
    parse_index_futures_final_settlement_html,
    parse_stock_futures_final_settlement_html,
)


def test_parse_index_futures_final_settlement_expands_shared_products() -> None:
    body = """
    <html><body><table>
      <thead><tr>
        <th>最後結算日</th><th>契約月份</th>
        <th>臺股期貨/小型臺指期貨/微型臺指期貨（TX/MTX/TMF）</th>
        <th>電子期貨/小型電子期貨（TE/ZEF）</th>
      </tr></thead>
      <tbody>
        <tr><td>2026/06/17</td><td>202606</td><td>45,670</td><td>2940.25</td></tr>
        <tr><td>2026/06/10</td><td>202606W2</td><td>43389</td><td>-</td></tr>
      </tbody>
    </table></body></html>
    """.encode()
    result = parse_index_futures_final_settlement_html(
        body,
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
        source_file="receipt.html",
        source_sha256="abc",
        source_url="https://www.taifex.com.tw/cht/5/futIndxFSP",
    )
    monthly = result.filter(result["contract"] == "202606")
    assert set(monthly["product"].to_list()) == {"TX", "MTX", "TMF", "TE", "ZEF"}
    assert monthly.filter(monthly["product"] == "TX")[
        "final_settlement_price"
    ].item() == 45_670.0
    weekly = result.filter(result["contract"] == "202606W2")
    assert set(weekly["product"].to_list()) == {"TX", "MTX", "TMF"}


def test_parse_stock_futures_final_settlement_uses_product_contract_date_key() -> None:
    body = """
    <html><body><table>
      <thead><tr>
        <th>商品名稱</th><th>商品代號</th><th>標的證券代號</th>
        <th>到期日</th><th>契約月份</th><th>最後結算價</th><th>約定標的物價值</th>
      </tr></thead>
      <tbody>
        <tr><td>台泥期貨</td><td>DFF</td><td>1101</td><td>2014/01/15</td><td>201401</td><td>44.81</td><td>89620</td></tr>
        <tr><td>小型0050期貨</td><td>SRF</td><td>0050</td><td>2014/01/15</td><td>201401</td><td>61.23</td><td>61230</td></tr>
      </tbody>
    </table></body></html>
    """.encode()
    result = parse_stock_futures_final_settlement_html(
        body,
        start_date=date(2014, 1, 1),
        end_date=date(2014, 1, 31),
        source_file="receipt.html",
        source_sha256="def",
        source_url="https://www.taifex.com.tw/cht/5/sSFFSP",
    )
    assert result.select("product", "contract").rows() == [
        ("DFF", "201401"),
        ("SRF", "201401"),
    ]
    assert result["settlement_date"].to_list() == [
        date(2014, 1, 15),
        date(2014, 1, 15),
    ]
    assert result['final_settlement_value'].to_list() == [89620., 61230.]


def test_adjusted_contract_final_value_includes_rights_beyond_price_times_multiplier():
    body = '''<table><tr><th>商品名稱</th><th>商品代號</th><th>標的證券代號</th>
    <th>到期日</th><th>契約月份</th><th>最後結算價</th><th>約定標的物價值</th></tr>
    <tr><td>合晶期貨</td><td>PLF</td><td>6182</td><td>2024/10/16</td><td>202410</td><td>29.82</td><td>59,640</td></tr>
    <tr><td>合晶期貨</td><td>PL1</td><td>6182</td><td>2024/10/16</td><td>202410</td><td>29.82</td><td>59,923</td></tr></table>'''.encode()
    frame = parse_stock_futures_final_settlement_html(body, start_date=date(2024,10,1),
        end_date=date(2024,10,31), source_file='official.html', source_sha256='abc',
        source_url='https://www.taifex.com.tw/cht/5/sSFFSP')
    adjusted = frame.filter(frame['product']=='PL1').row(0, named=True)
    assert adjusted['final_settlement_value'] == 59923
    assert adjusted['final_settlement_value'] - adjusted['final_settlement_price'] * 2000 == 283
    with pytest.raises(ValueError, match='invalid official final contract value'):
        parse_stock_futures_final_settlement_html(body.replace(b'59,923', b'-'),
            start_date=date(2024,10,1), end_date=date(2024,10,31), source_file='bad.html',
            source_sha256='bad', source_url='https://www.taifex.com.tw/cht/5/sSFFSP')


def test_commodity_futures_do_not_import_gold_options_or_nonfinite_prices():
    body = '''<table><tr><th>最後結算日</th><th>契約月份</th>
    <th>台幣黃金期貨(TGF)</th><th>黃金選擇權(TGO)</th><th>布蘭特原油期貨(BRF)</th></tr>
    <tr><td>2026/08/28</td><td>202608</td><td>17,659.5</td><td>17659.5</td><td>-</td></tr>
    <tr><td>2026/08/04</td><td>202609</td><td>inf</td><td>-</td><td>2926.02</td></tr></table>'''.encode()
    result = parse_index_futures_final_settlement_html(body,
        start_date=date(2026,1,1), end_date=date(2026,12,31), source_file='commodity.html',
        source_sha256='abc', source_url='https://www.taifex.com.tw/cht/5/goldFSP',
        source_kind='official_commodity_futures_html', allowed_products=frozenset({'TGF','BRF'}))
    assert result.select('product','final_settlement_price').rows() == [('BRF',2926.02),('TGF',17659.5)]
    assert set(result['source_kind']) == {'official_commodity_futures_html'}


def test_official_empty_interest_period_is_not_a_fake_settlement():
    body = '<table><thead><tr><th>最後交易日</th><th>契約月份</th></tr></thead></table>'.encode()
    result = parse_index_futures_final_settlement_html(body,
        start_date=date(2026,1,1), end_date=date(2026,12,31), source_file='interest.html',
        source_sha256='abc', source_url='https://www.taifex.com.tw/cht/5/interestRateFSP',
        source_kind='official_interest_rate_futures_html', allowed_products=frozenset({'GBF','CPF'}))
    assert result.is_empty()
