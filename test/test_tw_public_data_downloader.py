from __future__ import annotations

from datetime import date

import polars as pl

from downloader import download_tw_public_data as twpub


def test_parse_json_table_payload_keeps_stock_codes_as_strings():
    spec = twpub.DatasetSpec(
        name="sample",
        kind="historical_json_table",
        source="TWSE",
        description="sample",
        tags=("test",),
    )
    payload = {
        "stat": "OK",
        "title": "sample",
        "fields": ["證券代號", "買進", "買進"],
        "data": [
            ["0050", "1,000", "2,000"],
            ["1101", "3", "4"],
        ],
    }

    frame = twpub._parse_json_table_payload(payload, spec, date(2024, 6, 3))

    assert frame["證券代號"].to_list() == ["0050", "1101"]
    assert "買進_2" in frame.columns
    assert frame.schema["證券代號"] == pl.String
    assert frame["date"].to_list() == ["2024-06-03", "2024-06-03"]


def test_table_mode_filters_json_tables_by_title():
    spec = twpub.DatasetSpec(
        name="sample",
        kind="historical_json_table",
        source="TWSE",
        description="sample",
        tags=("test",),
        table_mode="title_contains:每日收盤行情",
    )
    payload = {
        "stat": "OK",
        "tables": [
            {"title": "價格指數", "fields": ["指數"], "data": [["發行量加權股價指數"]]},
            {"title": "每日收盤行情", "fields": ["證券代號"], "data": [["2330"]]},
        ],
    }

    frame = twpub._parse_json_table_payload(payload, spec, date(2024, 6, 3))

    assert frame.height == 1
    assert frame["證券代號"].to_list() == ["2330"]
    assert frame["_table_title"].to_list() == ["每日收盤行情"]


def test_parse_csv_bytes_accepts_big5_and_dedupes_columns():
    raw = "日期,利率[%],利率[%]\n2002/5/2,2.269,2.270\n".encode("cp950")

    frame = twpub._parse_csv_bytes(raw)

    assert frame.columns == ["日期", "利率[%]", "利率[%]_2"]
    assert frame["利率[%]"].to_list() == ["2.269"]


def test_parse_xml_bytes_flattens_repeated_records():
    raw = b"""
    <Root>
      <Row><TIME_PERIOD>2024M01</TIME_PERIOD><VALUE>1.2</VALUE></Row>
      <Row><TIME_PERIOD>2024M02</TIME_PERIOD><VALUE>1.3</VALUE></Row>
    </Root>
    """

    frame = twpub._parse_xml_bytes(raw)

    assert frame.shape == (2, 2)
    assert frame["TIME_PERIOD"].to_list() == ["2024M01", "2024M02"]


def test_select_specs_accepts_tags_and_names():
    names = {spec.name for spec in twpub._select_specs(["macro", "twse_daily_ohlcv"])}

    assert "twse_daily_ohlcv" in names
    assert "cbc_overnight_rate" in names
    assert "dgbas_unemployment_rate" in names


def test_merge_frames_replaces_existing_dates():
    existing = pl.DataFrame(
        {
            "date": ["2024-06-03", "2024-06-04"],
            "value": ["old", "keep"],
        }
    )
    incoming = pl.DataFrame(
        {
            "date": ["2024-06-03"],
            "value": ["new"],
        }
    )

    merged = twpub._merge_frames(existing, incoming, refresh=False)

    assert merged.sort("date")["value"].to_list() == ["new", "keep"]
