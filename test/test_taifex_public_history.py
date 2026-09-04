from __future__ import annotations

from datetime import date

import pandas as pd

from scripts.download_taifex_public_history import (
    POSITIONING_SPECS,
    _next_session_map,
    _parse_positioning,
    _parse_put_call,
    _receipt_covers_request,
)


def _html_with_date(frame: pd.DataFrame) -> bytes:
    return (
        "<html><body><p>2026/09/01</p>"
        + frame.to_html(index=False)
        + "</body></html>"
    ).encode()


def test_put_call_ratio_keeps_trailing_comma_and_next_session_causality() -> None:
    content = (
        "日期,賣權成交量,買權成交量,買賣權成交量比率%,賣權未平倉量,"
        "買權未平倉量,買賣權未平倉量比率%\r\n"
        "2001/12/24,275,1145,24.02,90,683,13.18,\r\n"
    ).encode("cp950")
    frame = _parse_put_call(
        content,
        {date(2001, 12, 24): date(2001, 12, 25)},
    )

    assert len(frame) == 1
    assert frame.loc[0, "put_volume"] == 275
    assert frame.loc[0, "call_volume"] == 1145
    assert frame.loc[0, "available_date"].date() == date(2001, 12, 25)
    assert bool(frame.loc[0, "published_after_close"]) is True


def test_put_call_current_month_receipt_must_cover_latest_requested_date() -> None:
    receipt = {
        "request_payload": {
            "queryStartDate": "2026/09/01",
            "queryEndDate": "2026/09/01",
            "down_type": "1",
        }
    }

    assert _receipt_covers_request(receipt, dict(receipt["request_payload"])) is True
    assert _receipt_covers_request(
        receipt,
        {
            "queryStartDate": "2026/09/01",
            "queryEndDate": "2026/09/02",
            "down_type": "1",
        },
    ) is False


def test_institutional_parser_preserves_total_rows() -> None:
    rows = [
        ["1", "臺股期貨", "自營商", *range(1, 13)],
        ["期貨合計", "期貨合計", "期貨合計", *range(11, 23)],
    ]
    source = pd.DataFrame(rows)
    spec = next(item for item in POSITIONING_SPECS if item.name == "institutional_futures")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    assert frame["sequence"].tolist() == ["1", "期貨合計"]
    assert frame["participant_type"].tolist() == ["自營商", "期貨合計"]
    assert str(frame["trade_long_lots"].dtype) == "Int64"
    assert frame["available_date"].dt.date.tolist() == [
        date(2026, 9, 2),
        date(2026, 9, 2),
    ]


def test_large_trader_parser_splits_specific_corporate_values() -> None:
    source = pd.DataFrame(
        [[
            "臺股期貨(TX+MTX/4+TMF/20)",
            "所有 契約",
            "75,116  (75,116)",
            "64.3%  (64.3%)",
            "82,175  (81,277)",
            "70.4%  (69.6%)",
            "50,737  (50,737)",
            "43.5%  (43.5%)",
            "71,020  (71,020)",
            "60.8%  (60.8%)",
            "116742",
        ]]
    )
    spec = next(item for item in POSITIONING_SPECS if item.name == "large_trader_futures_tx")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    row = frame.iloc[0]
    assert row["expiry_bucket"] == "所有契約"
    assert row["buy_top10_positions"] == 82175
    assert row["buy_top10_specific_positions"] == 81277
    assert row["sell_top10_share_pct"] == 60.8
    assert row["market_open_interest"] == 116742


def test_large_trader_parser_accepts_spaces_inside_parentheses() -> None:
    source = pd.DataFrame(
        [["臺股期貨", "週契約", *(["0  (  0  )", "0%  (  0%  )"] * 4), "0"]]
    )
    spec = next(item for item in POSITIONING_SPECS if item.name == "large_trader_futures_tx")
    frame = _parse_positioning(
        spec,
        _html_with_date(source),
        date(2026, 9, 1),
        _next_session_map([date(2026, 9, 1), date(2026, 9, 2)]),
    )

    assert frame.loc[0, "buy_top5_positions"] == 0
    assert frame.loc[0, "buy_top5_specific_positions"] == 0
