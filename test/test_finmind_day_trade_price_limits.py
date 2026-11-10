import math
import json

import pytest

from scripts.prepare_finmind_tw_day_trade_price_limits import (
    source_price_bounds, _projected_source_row, _selected_priority_items,
)


def _priority_item(symbol, state="complete"):
    return {"key": json.dumps(["TaiwanStockPriceLimit", symbol, "history"]), "state": state}


def test_reuses_shared_request_for_exact_daily_scope():
    status = {"items": [_priority_item("0050"), _priority_item("2330", "pending")]}
    assert _selected_priority_items(status, {"0050"}) == [("0050", status["items"][0])]


@pytest.mark.parametrize("items", [
    [_priority_item("0050", "pending")],
    [_priority_item("0050"), _priority_item("0050")],
    [_priority_item("2330")],
    [{"key": json.dumps(["TaiwanStockPrice", "0050", "history"]), "state": "complete"}],
])
def test_shared_request_still_rejects_incomplete_duplicate_or_wrong_source(items):
    with pytest.raises(ValueError):
        _selected_priority_items({"items": items}, {"0050"})


def test_projection_supports_both_dated_official_metadata_and_legacy_schema():
    columns = ["trading_date", "symbol", "reference_price", "upper_limit_price", "lower_limit_price",
               "prepared_at", "source", "source_session_date", "source_url", "historical_reconstruction"]
    kwargs = {"day": "2026-07-08", "symbol": "6542", "ref": 45.55, "up": 50.1, "down": 41.,
              "state": "provider_historical_price_limits", "fetched_at": "2026-11-04T00:00:00Z"}
    row = _projected_source_row(**kwargs, columns=columns)
    assert row["source_session_date"] == "2026-07-08"
    assert row["historical_reconstruction"] is True
    assert row["source"].startswith("finmind_historical_replay:")
    assert list(_projected_source_row(**kwargs, columns=columns[:7])) == columns[:7]
    with pytest.raises(ValueError, match="unknown original"):
        _projected_source_row(**kwargs, columns=columns + ["unexpected"])


def test_historical_finite_limits_preserve_vendor_values():
    row = {"reference_price": 34.3, "limit_up": 37.7, "limit_down": 30.9}
    assert source_price_bounds(row) == (34.3, 37.7, 30.9, "provider_historical_price_limits")


@pytest.mark.parametrize("up,down", [(0, 0), (9999.95, 0.01)])
def test_no_limit_and_extreme_sentinels_are_not_fabricated_tp_sl(up, down):
    row = {"reference_price": 9.6, "limit_up": up, "limit_down": down}
    assert source_price_bounds(row) == (
        9.6, None, None, "provider_no_limit_or_extreme_sentinel_not_executable")


@pytest.mark.parametrize("ref,up,down", [
    (0, 10, 5), (math.nan, 10, 5), (8, 7, 5), (8, 10, 9), (8, math.inf, 5),
    (8, 0, 5), (8, 10, -1),
])
def test_invalid_price_limit_observations_fail_closed(ref, up, down):
    with pytest.raises(ValueError):
        source_price_bounds({"reference_price": ref, "limit_up": up, "limit_down": down})
