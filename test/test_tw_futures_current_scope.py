from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_current_scope import compare_snapshot


def _inventory():
    return pl.DataFrame({
        "product": ["TX"], "product_name": ["台股期貨"],
        "asset_class": ["index_future"], "settlement_currency": ["TWD"],
    })


def _row(product, month, volume, *, market="20261029", capture="2026-10-30T00:00:00+00:00",
         payload="a" * 64, session="一般"):
    return {"Date": market, "date": "2026-10-30", "Contract": product,
            "ContractMonth(Week)": month, "TradingSession": session, "Volume": volume,
            "_downloaded_at_utc": capture, "_payload_sha256": payload}


def test_market_day_and_contract_scope_keep_unknown_volume_and_new_codes_distinct():
    frame = pl.DataFrame([
        _row("TX", "202611", "10"),
        _row("TX", "202612", "0"),
        _row("TX", "202703", None),
        _row("PK1", "202611", "45"),
        _row("NEW", "202611", "5"),
        _row("TX", "202611/202612", "99"),  # spread is not an outright month
        _row("TX", "202611", "99", session="盤後"),
        _row("TX", "202611", "99", market="20261030"),
    ])
    result = compare_snapshot(frame, _inventory(), date(2026, 10, 29))
    assert result["reported_monthly_contract_rows"] == 5
    assert result["reported_twd_settlement_products"] == 2
    assert result["twd_settlement_products_with_positive_daily_volume"] == 2
    assert result["unreviewed_codes"] == ["NEW"]
    tx = next(p for p in result["products"] if p["product"] == "TX")
    assert (tx["positive_volume_contracts"], tx["explicit_zero_volume_contracts"],
            tx["missing_or_invalid_volume_contracts"]) == (1, 1, 1)
    assert result["market_day"] == "2026-10-29"  # not the capture date


def test_latest_capture_is_selected_without_combining_snapshot_generations():
    old = _row("TX", "202611", "10", capture="2026-10-29T23:00:00+00:00")
    new = _row("TX", "202611", "0", capture="2026-10-30T00:00:00+00:00", payload="b" * 64)
    result = compare_snapshot(pl.DataFrame([old, new]), _inventory(), date(2026, 10, 29))
    assert result["snapshot_raw_payload_sha256"] == "b" * 64
    assert result["twd_settlement_products_with_positive_daily_volume"] == 0


@pytest.mark.parametrize("rows", [
    [_row("TX", "202611", "1"), _row("TX", "202611", "2")],
    [_row("TX", "202611", "1"), _row("PK1", "202611", "2", payload="b" * 64)],
    [_row("TX", "202611", "-1")],
])
def test_ambiguous_or_invalid_source_is_rejected(rows):
    with pytest.raises(ValueError):
        compare_snapshot(pl.DataFrame(rows), _inventory(), date(2026, 10, 29))
