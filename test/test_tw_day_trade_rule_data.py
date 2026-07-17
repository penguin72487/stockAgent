from __future__ import annotations

from datetime import date
import json

import polars as pl
import pytest

from downloader import download_tw_public_data as downloader
from stockagent.data.tw_public_features import _build_day_trade_rule_features


def _payload_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def test_day_trade_history_specs_use_official_daily_endpoints() -> None:
    twse = downloader.DEFAULT_DATASETS["twse_day_trade_eligibility"]
    tpex = downloader.DEFAULT_DATASETS["tpex_day_trade_eligibility"]

    assert twse.start_date == "2014-01-06"
    assert tpex.start_date == "2014-01-06"
    assert "tpex_day_trade_eligibility" in downloader.TPEX_SESSION_DEPENDENT_DATASETS
    assert downloader._historical_request_info(twse, date(2024, 7, 15))[0] == (
        "https://www.twse.com.tw/rwd/zh/dayTrading/TWTB4U"
        "?date=20240715&selectType=All&response=json"
    )
    assert downloader._historical_request_info(tpex, date(2024, 7, 15))[0] == (
        "https://www.tpex.org.tw/www/zh-tw/intraday/list"
        "?date=2024/07/15&code="
    )


def test_twse_day_trade_history_selects_rule_table_and_binds_both_dates() -> None:
    spec = downloader.DEFAULT_DATASETS["twse_day_trade_eligibility"]
    payload = {
        "stat": "OK",
        "date": "20240715",
        "tables": [
            {
                "title": "113年07月15日 當日沖銷交易統計資訊",
                "fields": ["成交股數"],
                "data": [["123"]],
            },
            {
                "title": "113年07月15日 當日沖銷交易標的及成交量值",
                "fields": [
                    "證券代號",
                    "證券名稱",
                    "暫停現股賣出後現款買進當沖註記",
                ],
                "data": [["2330", "台積電", "Y"], ["2317", "鴻海", ""]],
            },
        ],
    }

    frame, suffix = downloader._parse_historical_response_content(
        spec,
        date(2024, 7, 15),
        _payload_bytes(payload),
        "json",
    )

    assert suffix == ".json"
    assert frame["證券代號"].to_list() == ["2330", "2317"]
    assert "成交股數" not in frame.columns

    stale = dict(payload)
    stale["date"] = "20240712"
    with pytest.raises(downloader.HistoricalResponseError, match="date mismatch"):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(stale),
            "json",
        )


def test_tpex_day_trade_history_requires_post_launch_direction_marker() -> None:
    spec = downloader.DEFAULT_DATASETS["tpex_day_trade_eligibility"]
    payload = {
        "date": "20140630",
        "tables": [
            {
                "title": "現股當沖交易標的",
                "date": "103/06/30",
                "totalCount": 1,
                "fields": ["證券代號", "證券名稱"],
                "data": [["6488", "環球晶"]],
            }
        ],
    }
    with pytest.raises(
        downloader.HistoricalResponseError,
        match="missing .*暫停現股賣出後現款買進當沖註記",
    ):
        downloader._parse_historical_response_content(
            spec,
            date(2014, 6, 30),
            _payload_bytes(payload),
            "json",
        )

    payload["tables"][0]["fields"].append("暫停現股賣出後現款買進當沖註記")
    payload["tables"][0]["data"] = [["6488", "環球晶", "unknown"]]
    with pytest.raises(
        downloader.HistoricalResponseError,
        match="unknown day-trade sell-first suspension markers",
    ):
        downloader._parse_historical_response_content(
            spec,
            date(2014, 6, 30),
            _payload_bytes(payload),
            "json",
        )


def test_tpex_day_trade_history_binds_table_date_independently() -> None:
    spec = downloader.DEFAULT_DATASETS["tpex_day_trade_eligibility"]
    payload = {
        "date": "20240715",
        "tables": [
            {
                "title": "現股當沖交易標的",
                "date": "113/07/12",
                "fields": [
                    "證券代號",
                    "證券名稱",
                    "暫停現股賣出後現款買進當沖註記",
                ],
                "data": [["6488", "環球晶", ""]],
            }
        ],
    }
    with pytest.raises(downloader.HistoricalResponseError, match="date mismatch"):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(payload),
            "json",
        )


@pytest.mark.parametrize(
    ("dataset", "title", "table_date", "total_key"),
    [
        (
            "twse_day_trade_eligibility",
            "113年07月15日 當日沖銷交易標的及成交量值",
            None,
            "total",
        ),
        (
            "tpex_day_trade_eligibility",
            "現股當沖交易標的",
            "113/07/15",
            "totalCount",
        ),
    ],
)
def test_day_trade_history_rejects_truncated_marker_rows(
    dataset: str,
    title: str,
    table_date: str | None,
    total_key: str,
) -> None:
    spec = downloader.DEFAULT_DATASETS[dataset]
    table = {
        "title": title,
        total_key: 1,
        "fields": [
            "證券代號",
            "證券名稱",
            "暫停現股賣出後現款買進當沖註記",
        ],
        # A missing third cell is not equivalent to an official blank marker.
        "data": [["2330", "台積電"]],
    }
    if table_date is not None:
        table["date"] = table_date
    payload = {"stat": "OK", "date": "20240715", "tables": [table]}

    with pytest.raises(downloader.HistoricalResponseError, match="row width mismatch"):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(payload),
            "json",
        )


@pytest.mark.parametrize(
    ("dataset", "title", "table_date", "total_key"),
    [
        (
            "twse_day_trade_eligibility",
            "113年07月15日 當日沖銷交易標的及成交量值",
            None,
            "total",
        ),
        (
            "tpex_day_trade_eligibility",
            "現股當沖交易標的",
            "113/07/15",
            "totalCount",
        ),
    ],
)
def test_day_trade_history_rejects_partial_declared_table_total(
    dataset: str,
    title: str,
    table_date: str | None,
    total_key: str,
) -> None:
    spec = downloader.DEFAULT_DATASETS[dataset]
    table = {
        "title": title,
        total_key: "2",
        "fields": [
            "證券代號",
            "證券名稱",
            "暫停現股賣出後現款買進當沖註記",
        ],
        "data": [["2330", "台積電", ""]],
    }
    if table_date is not None:
        table["date"] = table_date
    payload = {"stat": "OK", "date": "20240715", "tables": [table]}

    with pytest.raises(
        downloader.HistoricalResponseError,
        match=f"{total_key} does not match data rows",
    ):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(payload),
            "json",
        )


def test_day_trade_history_rejects_dict_row_missing_direction_marker() -> None:
    spec = downloader.DEFAULT_DATASETS["twse_day_trade_eligibility"]
    payload = {
        "stat": "OK",
        "date": "20240715",
        "tables": [
            {
                "title": "113年07月15日 當日沖銷交易標的及成交量值",
                "total": 1,
                "fields": [
                    "證券代號",
                    "證券名稱",
                    "暫停現股賣出後現款買進當沖註記",
                ],
                "data": [{"證券代號": "2330", "證券名稱": "台積電"}],
            }
        ],
    }

    with pytest.raises(
        downloader.HistoricalResponseError,
        match="dict row missing required fields",
    ):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(payload),
            "json",
        )


@pytest.mark.parametrize(
    ("dataset", "title", "table_date", "total_key"),
    [
        (
            "twse_day_trade_eligibility",
            "113年07月15日 當日沖銷交易標的及成交量值",
            None,
            "total",
        ),
        (
            "tpex_day_trade_eligibility",
            "現股當沖交易標的",
            "113/07/15",
            "totalCount",
        ),
    ],
)
def test_day_trade_history_rejects_null_direction_marker(
    dataset: str,
    title: str,
    table_date: str | None,
    total_key: str,
) -> None:
    spec = downloader.DEFAULT_DATASETS[dataset]
    table = {
        "title": title,
        total_key: 1,
        "fields": [
            "證券代號",
            "證券名稱",
            "暫停現股賣出後現款買進當沖註記",
        ],
        "data": [["2330", "台積電", None]],
    }
    if table_date is not None:
        table["date"] = table_date
    payload = {"stat": "OK", "date": "20240715", "tables": [table]}

    with pytest.raises(
        downloader.HistoricalResponseError,
        match="null day-trade sell-first suspension marker",
    ):
        downloader._parse_historical_response_content(
            spec,
            date(2024, 7, 15),
            _payload_bytes(payload),
            "json",
        )


def _write_day_trade_inputs(input_dir) -> None:
    dates = [date(2014, 1, 6), date(2014, 6, 30)]
    pl.DataFrame(
        {
            "date": [dates[0], dates[0], dates[1], dates[1]],
            "證券代號": ["2330", "2317", "2330", "2317"],
        }
    ).write_parquet(input_dir / "twse_daily_ohlcv.parquet")
    pl.DataFrame(
        {
            "date": [dates[0], dates[0], dates[1], dates[1]],
            "代號": ["6488", "8069", "6488", "8069"],
        }
    ).write_parquet(input_dir / "tpex_daily_ohlcv.parquet")
    pl.DataFrame(
        {
            "date": dates,
            "證券代號": ["2330", "2330"],
            "證券名稱": ["台積電", "台積電"],
            "暫停現股賣出後現款買進當沖註記": [None, "Y"],
        }
    ).write_parquet(input_dir / "twse_day_trade_eligibility.parquet")
    pl.DataFrame(
        {
            "date": dates,
            "證券代號": ["6488", "6488"],
            "證券名稱": ["環球晶", "環球晶"],
            "暫停現股賣出後現款買進當沖註記": [None, ""],
        }
    ).write_parquet(input_dir / "tpex_day_trade_eligibility.parquet")


def test_day_trade_feature_builder_emits_explicit_point_in_time_negatives(tmp_path) -> None:
    _write_day_trade_inputs(tmp_path)

    rules = _build_day_trade_rule_features(tmp_path)

    assert rules.height == 8
    by_key = {
        (row["date"], row["symbol"]): (
            row["_twpub_day_trade_eligible"],
            row["_twpub_day_trade_short_open"],
        )
        for row in rules.to_dicts()
    }
    assert by_key[(date(2014, 1, 6), "2330")] == (1.0, 0.0)
    assert by_key[(date(2014, 1, 6), "2317")] == (0.0, 0.0)
    assert by_key[(date(2014, 6, 30), "2330")] == (1.0, 0.0)
    assert by_key[(date(2014, 6, 30), "6488")] == (1.0, 1.0)
    assert by_key[(date(2014, 6, 30), "8069")] == (0.0, 0.0)

    filtered = _build_day_trade_rule_features(
        tmp_path,
        symbols={"2330", "6488"},
    )
    assert filtered.height == 4
    assert set(filtered["symbol"].to_list()) == {"2330", "6488"}


def test_day_trade_feature_builder_rejects_incomplete_session_coverage(tmp_path) -> None:
    _write_day_trade_inputs(tmp_path)
    tpex = pl.read_parquet(tmp_path / "tpex_day_trade_eligibility.parquet")
    tpex.filter(pl.col("date") == date(2014, 1, 6)).write_parquet(
        tmp_path / "tpex_day_trade_eligibility.parquet"
    )

    with pytest.raises(ValueError, match="coverage does not match"):
        _build_day_trade_rule_features(tmp_path)


def test_day_trade_feature_builder_requires_both_exchange_histories(tmp_path) -> None:
    _write_day_trade_inputs(tmp_path)
    (tmp_path / "tpex_day_trade_eligibility.parquet").unlink()

    with pytest.raises(ValueError, match="requires both TWSE and TPEx"):
        _build_day_trade_rule_features(tmp_path)
