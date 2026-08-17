from __future__ import annotations

from datetime import date
import json

from scripts import rebuild_tw_day_trade_open_price_replay as replay


def test_retained_twse_openapi_rule_receipt_uses_exact_parser() -> None:
    raw = json.dumps(
        [
            {
                "Date": "1150817",
                "Code": "2330",
                "Name": "台積電",
                "Suspension": "",
            }
        ],
        ensure_ascii=False,
    ).encode("utf-8")

    assert replay._retained_rule_response_kind(
        "twse_day_trade_eligibility", raw
    ) == "twse_day_trade_openapi_json"
    assert replay._retained_rule_response_kind(
        "tpex_day_trade_eligibility", raw
    ) == "json"


def test_replay_range_can_skip_weekend_non_sessions() -> None:
    assert replay._is_weekend(date(2026, 8, 15))
    assert replay._is_weekend(date(2026, 8, 16))
    assert not replay._is_weekend(date(2026, 8, 17))
