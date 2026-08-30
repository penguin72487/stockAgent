from __future__ import annotations

from datetime import date
import importlib.util
import io
from pathlib import Path

import pytest


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "download_taifex_recent_index_derivatives_ticks.py"
)
SPEC = importlib.util.spec_from_file_location(
    "download_taifex_recent_index_derivatives_ticks", SCRIPT
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_extracts_latest_common_taifex_dates() -> None:
    futures_page = b"""
    https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_2026_08_05.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_2026_08_06.zip
    """
    options_page = b"""
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_04.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_05.zip
    https://www.taifex.com.tw/file/taifex/Dailydownload/OptionsDailydownloadCSV/OptionsDaily_2026_08_06.zip
    """

    futures = MODULE._extract_downloads(futures_page, pattern=MODULE.FUTURES_URL_RE)
    options = MODULE._extract_downloads(options_page, pattern=MODULE.OPTIONS_URL_RE)

    assert MODULE._selected_common_dates(futures, options, count=2) == [
        date(2026, 8, 5),
        date(2026, 8, 6),
    ]


def test_options_preserve_side_rows_and_pair_matched_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,履約價格,到期月份(週別),買賣權別,成交時間,成交價格,成交數量(B or S),開盤集合競價\n"
        "20260805,TXO,44000,202608W1,C,150000,123,3,*\n"
        "20260805,TXO,44000,202608W1,C,150000,123,3,*\n"
        "20260806,CBO,21,202609,P,090000,1.99,2,\n"
    )

    frame = MODULE._parse_options_rows(
        source,
        trading_date=date(2026, 8, 6),
        source_file="OptionsDaily_2026_08_06.zip",
        source_sha256="a" * 64,
    )

    assert frame.height == 2
    assert frame["product"].unique().to_list() == ["TXO"]
    assert frame["session"].unique().to_list() == ["night"]
    assert frame["reported_side_quantity"].sum() == 6
    assert frame["matched_quantity_equivalent"].sum() == 3.0
    assert frame["event_ts"].dtype.time_zone == "Asia/Taipei"


def test_futures_convert_b_plus_s_quantity_to_matched_contracts() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260806,TX,202608,084500,44580,8,-,-,*\n"
        "20260806,MTX,202608,084500,44580,8,-,-,*\n"
    )

    frame = MODULE._parse_futures_rows(
        source,
        trading_date=date(2026, 8, 6),
        source_file="Daily_2026_08_06.zip",
        source_sha256="b" * 64,
    )

    assert frame.height == 1
    assert frame["product"].to_list() == ["TX"]
    assert frame["reported_b_plus_s_quantity"].to_list() == [8]
    assert frame["matched_quantity"].to_list() == [4]
    assert frame["session"].to_list() == ["day"]


def test_futures_preserve_zero_and_negative_calendar_spread_prices() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260819,TX,202608/202609,102057,0,20,44577,44577,\n"
        "20260819,TX,202608/202609,102058,-1,8,44580,44579,\n"
    )

    frame = MODULE._parse_futures_rows(
        source,
        trading_date=date(2026, 8, 19),
        source_file="Daily_2026_08_19.zip",
        source_sha256="b" * 64,
    )

    assert frame["price"].to_list() == [0.0, -1.0]
    assert frame["matched_quantity"].to_list() == [10, 4]


def test_futures_still_reject_non_positive_outright_price() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260819,TX,202608,102057,0,20,-,-,\n"
    )

    with pytest.raises(ValueError, match="non-positive price"):
        MODULE._parse_futures_rows(
            source,
            trading_date=date(2026, 8, 19),
            source_file="Daily_2026_08_19.zip",
            source_sha256="b" * 64,
        )


def test_options_fail_closed_on_unpaired_side_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,履約價格,到期月份(週別),買賣權別,成交時間,成交價格,成交數量(B or S),開盤集合競價\n"
        "20260806,TXO,44000,202608W1,C,090000,123,3,\n"
    )

    with pytest.raises(ValueError, match="unpaired B-or-S quantities"):
        MODULE._parse_options_rows(
            source,
            trading_date=date(2026, 8, 6),
            source_file="OptionsDaily_2026_08_06.zip",
            source_sha256="a" * 64,
        )


def test_futures_fail_closed_on_odd_b_plus_s_quantity() -> None:
    source = io.StringIO(
        "成交日期,商品代號,到期月份(週別),成交時間,成交價格,成交數量(B+S),近月價格,遠月價格,開盤集合競價\n"
        "20260806,TX,202608,084500,44580,7,-,-,*\n"
    )

    with pytest.raises(ValueError, match="positive and even"):
        MODULE._parse_futures_rows(
            source,
            trading_date=date(2026, 8, 6),
            source_file="Daily_2026_08_06.zip",
            source_sha256="b" * 64,
        )
