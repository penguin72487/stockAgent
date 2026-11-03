from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_current_weekly_scope import compare_weekly


def _row(product, delivery, volume, *, day="20261029", session="一般",
         captured="2026-10-30T00:00:00+00:00", payload="a" * 64):
    return {"Date": day, "Contract": product, "ContractMonth(Week)": delivery,
            "TradingSession": session, "Volume": volume,
            "_downloaded_at_utc": captured, "_payload_sha256": payload}


def _inventory():
    return pl.DataFrame({"product": ["MTX"], "product_name": ["小型臺指期貨"],
                         "asset_class": ["index_future"], "settlement_currency": ["TWD"]})


def test_weekly_and_monthly_use_same_snapshot_and_exclude_spread_and_night():
    frame = pl.DataFrame([
        _row("MTX", "202611", "100"), _row("MTX", "202611W1", "2"),
        _row("MTX", "202611W2", None), _row("MTX", "202611W1/202611", "99"),
        _row("MTX", "202611W1", "99", session="盤後"),
        _row("MTX", "202610W4", "99", captured="2026-10-29T00:00:00+00:00", payload="b" * 64),
    ])
    result = compare_weekly(frame, _inventory(), date(2026, 10, 29))
    assert (result["reported_monthly_contract_rows"], result["reported_weekly_contract_rows"],
            result["reported_outright_contract_rows"], result["combined_products"]) == (1, 2, 3, 1)
    assert [row["delivery"] for row in result["weekly_contracts"]] == ["202611W1", "202611W2"]
    assert [row["volume"] for row in result["weekly_contracts"]] == [2, None]
    assert result["weekly_contracts"][0]["settlement_currency"] == "TWD"


@pytest.mark.parametrize("other", [
    _row("MTX", "202611W1", "3"),
    _row("MTX", "202611W2", "3", payload="b" * 64),
    _row("MTX", "202611W2", "-1"),
])
def test_weekly_source_rejects_duplicates_mixed_payload_or_negative_volume(other):
    rows = [_row("MTX", "202611", "10"), _row("MTX", "202611W1", "2"), other]
    with pytest.raises(ValueError):
        compare_weekly(pl.DataFrame(rows), _inventory(), date(2026, 10, 29))
