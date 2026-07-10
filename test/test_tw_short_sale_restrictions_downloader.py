from downloader.download_tw_short_sale_restrictions import _announcement_record, _roc_date


def test_roc_date_parser() -> None:
    assert _roc_date("公告日期 111年9月29日") == "2022-09-29"
    assert _roc_date("1150709") == "2026-07-09"


def test_announcement_parser_extracts_short_ban_and_delisting_dates() -> None:
    record = _announcement_record(
        market="tpex",
        issued_date="2022-09-29",
        number="test",
        subject="公告股票代號：5820終止上櫃",
        body=(
            "股票代號：5820，自111年9月30日起暫停融資融券交易，惟了結交易不在此限；"
            "自111年11月1日起停止櫃檯買賣，並訂於111年11月11日起終止櫃檯買賣。"
        ),
        url="https://example.test",
    )
    assert record["symbols"] == "5820"
    assert record["short_open_ban_date"] == "2022-09-30"
    assert record["trading_suspension_date"] == "2022-11-01"
    assert record["delisting_date"] == "2022-11-11"
    assert record["closing_only"] is True
