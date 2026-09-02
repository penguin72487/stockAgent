from __future__ import annotations

from datetime import date

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
