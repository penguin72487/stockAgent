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


def test_model_useful_group_is_curated_and_has_unique_dataset_names():
    specs = twpub._select_specs(["model_useful"])
    names = [spec.name for spec in specs]

    assert len(specs) == 117
    assert len(names) == len(set(names))
    assert all("model_useful" in spec.tags for spec in specs)
    assert not any("warrant" in spec.name or "bond" in spec.name or "gold" in spec.name for spec in specs)


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


def test_parse_twse_delisted_company_payload():
    payload = [{"DelistingDate": "114/07/24", "Company": "新光金", "Code": "2888"}]

    frame = twpub._twse_delisted_frame(payload)

    assert frame.to_dicts() == [{
        "date": "2025-07-24",
        "market": "twse",
        "symbol": "2888",
        "company_name": "新光金",
        "delisting_reason": "",
    }]


def test_parse_tpex_delisted_company_payload():
    payload = {
        "tables": [{
            "fields": ["股票代號", "公司名稱", "終止上櫃日期", "終止上櫃原因", "公司資料網址"],
            "data": [["6747", "亨泰光學股份有限公司", "114-12-04", "業務規則第15條之18", "https://example.test"]],
        }],
        "stat": "ok",
    }

    frame = twpub._tpex_delisted_frame(payload)

    assert frame["symbol"].to_list() == ["6747"]
    assert frame["date"].to_list() == ["2025-12-04"]
    assert frame["delisting_reason"].to_list() == ["業務規則第15條之18"]


def test_parse_tpex_legacy_daily_quotes_with_split_close_cells():
    raw_html = """
    <table>
      <tr><td>股票代號</td><td>證券名稱</td><td colspan=2>收盤價</td></tr>
      <tr><td>4205</td><td>恆義食品</td><td></td><td>18.1</td><td>△</td><td>0.1</td>
          <td>18</td><td>18.5</td><td>17.95</td><td>18.12</td><td>97,000</td>
          <td>1,757,700</td><td>24</td><td>18.1</td><td>18.2</td><td>60,086,250</td>
          <td>18.1</td><td>19.35</td><td>16.85</td></tr>
    </table>
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2006, 12, 29))

    assert frame.select(["date", "代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]).to_dicts() == [
        {
            "date": "2006-12-29",
            "代號": "4205",
            "收盤": "18.1",
            "漲跌": "+0.1",
            "開盤": "18",
            "最高": "18.5",
            "最低": "17.95",
            "成交股數": "97,000",
        }
    ]


def test_parse_tpex_2003_oracle_report_with_unclosed_spacer_cells():
    raw_html = """
    <table><tr valign=top>
      <td colspan=2><td>3087<td><td>翔準<td><td>13.85<td><td>+0.00<td>
      <td>14.00<td><td>14.30<td><td>13.85<td><td>14.03<td><td>1,178,000<td>
      <td>16,521,950<td><td>472<td><td>13.85<td><td>13.90<td>
    <tr><td>next row
    """

    frame = twpub._parse_tpex_daily_quotes_html(raw_html, date(2003, 8, 1))

    assert frame.select(["代號", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"]).to_dicts() == [
        {
            "代號": "3087",
            "收盤": "13.85",
            "漲跌": "+0.00",
            "開盤": "14.00",
            "最高": "14.30",
            "最低": "13.85",
            "成交股數": "1,178,000",
        }
    ]


def test_tpex_historical_request_routes_each_official_archive_generation():
    spec = next(item for item in twpub.HISTORICAL_DAILY_DATASETS if item.name == "tpex_daily_ohlcv")

    archive_url, archive_kind = twpub._tpex_historical_request(date(2006, 12, 29), spec)
    legacy_url, legacy_kind = twpub._tpex_historical_request(date(2007, 5, 15), spec)
    current_url, current_kind = twpub._tpex_historical_request(date(2007, 7, 2), spec)

    assert archive_kind == "archive_html"
    assert archive_url.endswith("RSTA3104_951229.HTML")
    assert legacy_kind == "legacy_json_html"
    assert "dailyQuotesHis" in legacy_url
    assert current_kind == "json"
    assert "afterTrading/otc" in current_url
