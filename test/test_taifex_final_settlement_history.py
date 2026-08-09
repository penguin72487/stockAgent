from __future__ import annotations

from datetime import date

from scripts.download_taifex_final_settlement_history import (
    parse_taifex_txo_final_settlement_html,
)


def test_parse_official_txo_final_settlement_table() -> None:
    body = """
    <html><body><table>
      <thead><tr><th>最後結算日</th><th>契約月份</th><th>最後結算價</th></tr></thead>
      <tbody>
        <tr><td>2013/05/02</td><td>201305W1</td><td>8,230</td></tr>
        <tr><td>2013/05/08</td><td>201305W2</td><td>8,350</td></tr>
      </tbody>
    </table></body></html>
    """.encode()

    result = parse_taifex_txo_final_settlement_html(
        body,
        start_date=date(2013, 5, 1),
        end_date=date(2013, 5, 31),
        source_file="receipt.html",
        source_sha256="a" * 64,
        source_url="https://www.taifex.com.tw/cht/5/optIndxFSP",
    )

    assert result.height == 2
    assert result["settlement_date"].to_list() == [
        date(2013, 5, 2),
        date(2013, 5, 8),
    ]
    assert result["option_series"].to_list() == ["201305W1", "201305W2"]
    assert result["final_settlement_price"].to_list() == [8_230, 8_350]
