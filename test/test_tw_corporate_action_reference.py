from __future__ import annotations

from downloader.download_tw_corporate_action_reference import _payload_rows


def test_parse_twse_corporate_action_reference_payload() -> None:
    payload = {
        "stat": "OK",
        "fields": ["資料日期", "股票代號", "股票名稱", "除權息前收盤價", "除權息參考價", "權/息", "開盤競價基準"],
        "data": [["113年07月16日", "0050", "元大台灣50", "196.70", "195.70", "息", "195.70"]],
    }

    rows = _payload_rows(payload, market="twse", url="https://official.test")

    assert rows[0]["date"].isoformat() == "2024-07-16"
    assert rows[0]["symbol"] == "0050"
    assert rows[0]["reference_price"] == 195.7


def test_parse_tpex_historical_reference_payload() -> None:
    payload = {
        "tables": [
            {
                "fields": ["除權除息交易日", "股票代號", "股票名稱", "漲停價格", "除權參考價", "跌停價格", "開市交易基準價"],
                "data": [["95/01/10", "1333", "恩得利", "54.80", "49.99", "46.50", "51.30"]],
            }
        ]
    }

    rows = _payload_rows(payload, market="tpex", url="https://official.test")

    assert rows[0]["date"].isoformat() == "2006-01-10"
    assert rows[0]["reference_price"] == 49.99
    assert rows[0]["opening_reference_price"] == 51.3
