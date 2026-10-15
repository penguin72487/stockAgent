from copy import deepcopy

import numpy as np
import pytest

from stockagent.data.tw_price_rules import move_price_ticks_numpy
from stockagent.live.public_price_metrics import public_entry_distances
from stockagent.live import public_dashboards as public
from stockagent.live.public_quote_display import PublicQuoteDisplayPolicy


def row(entry=49.95, current=50.1, **extra):
    return {"symbol": "2330", "session_date": "2026-10-14", "security_type": "stock",
            "entry_price": entry, "last_mark_price": current,
            "public_price_display": {"visible_fields": {"entry_price": {}, "last_mark_price": {}}},
            **extra}


@pytest.mark.parametrize("entry,current,expected", [
    (9.99, 10.05, 2), (49.95, 50.1, 2), (99.9, 100.5, 2),
    (499.5, 501, 2), (999, 1005, 2), (48.95, 50.2, 23),
    (990, 1005, 11), (1005, 990, -11), (42.65, 42.65, 0),
])
def test_piecewise_distance_counts_each_legal_tick_without_changing_prices(entry, current, expected):
    source = row(entry, current)
    before = deepcopy(source)
    result = public_entry_distances(source)["last_mark_price"]
    assert result["ticks"] == expected
    assert result["change_pct"] == pytest.approx((current - entry) / entry * 100)
    assert result["approximate_ticks"] is False
    assert source == before


@pytest.mark.parametrize("entry", [5, 10, 15, 49.95, 50, 99.9, 100, 150, 499.5, 500, 999, 1000])
@pytest.mark.parametrize("steps", [-3, -1, 0, 1, 3])
@pytest.mark.parametrize("session", ["2004-02-25", "2026-10-14"])
def test_tick_distance_matches_independent_canonical_grid_walker(entry, steps, session):
    current = float(move_price_ticks_numpy(np.array([entry]), steps, [session])[0])
    # Some modern prices are not legal on the historical grid. Keep the
    # independent oracle limited to legal endpoints instead of rounding them.
    from stockagent.data.tw_price_rules import price_on_tick_grid_numpy
    if not price_on_tick_grid_numpy(np.array([entry]), [session])[0]:
        return
    result = public_entry_distances(row(entry, current, session_date=session))["last_mark_price"]
    assert result["ticks"] == steps
    assert result["approximate_ticks"] is False


def test_etf_dated_grid_is_not_the_stock_grid():
    result = public_entry_distances(row(49.99, 50.05, symbol="0050", security_type="etf"))["last_mark_price"]
    assert result["ticks"] == 2
    assert public_entry_distances(row(49.99, 50.05, symbol="0050", security_type=None))["last_mark_price"]["ticks"] == 2


def test_unrounded_average_price_yields_explicit_approximate_ticks():
    source = row(42.554581872809216, 42.65)
    result = public_entry_distances(source)["last_mark_price"]
    assert result["approximate_ticks"] is True
    assert result["ticks"] == pytest.approx((42.65 - 42.554581872809216) / 0.05)
    assert source["entry_price"] == 42.554581872809216


def test_price_axis_change_does_not_invert_short_side_or_claim_net_pnl():
    for side in ("long", "short"):
        result = public_entry_distances(row(100, 101, side=side, net_pnl_twd=-50))["last_mark_price"]
        assert result["change_pct"] == 1
        assert result["ticks"] == 2
        assert result["semantics"] == "price_change_not_net_pnl"


@pytest.mark.parametrize("field", ["entry_price", "last_mark_price"])
@pytest.mark.parametrize("bad", [None, 0, -1, float("inf"), float("nan"), True, "100"])
def test_invalid_prices_never_produce_zero_or_invented_moves(field, bad):
    assert not public_entry_distances(row(**{field: bad}))


@pytest.mark.parametrize("field", ["entry_price", "last_mark_price"])
def test_withheld_or_unproven_values_cannot_leak_through_distance(field):
    source = row()
    source["public_price_display"]["withheld_fields"] = [field]
    assert not public_entry_distances(source)
    source["public_price_display"].pop("withheld_fields")
    del source["public_price_display"]["visible_fields"][field]
    assert not public_entry_distances(source)


@pytest.mark.parametrize("extra", [
    {"session_date": "not-a-date"}, {"session_date": "1999-01-01"},
    {"symbol": "TX", "security_type": "stock_future"},
    {"symbol": "00900K", "security_type": "etf"},
    {"symbol": "unknown", "security_type": None},
])
def test_unsupported_rules_keep_percent_but_do_not_guess_tick(extra):
    result = public_entry_distances(row(**extra))["last_mark_price"]
    assert result["change_pct"] > 0
    assert result["ticks"] is None


def test_overnight_exit_and_partial_exit_use_their_own_dated_clocks():
    source = row(exit_price=50.1, last_exit_price=50.2, exit_at="2026-10-15T09:00:01+08:00",
                 last_exit_at="2026-10-14T13:24:00+08:00")
    source["public_price_display"]["visible_fields"].update(exit_price={}, last_exit_price={})
    metrics = public_entry_distances(source)
    assert metrics["exit_price"]["tick_session_date"] == "2026-10-15"
    assert metrics["last_exit_price"]["tick_session_date"] == "2026-10-14"


@pytest.mark.parametrize("sanitize", [public.sanitize_tw_positions, public.sanitize_tw_signals, public.sanitize_tw_status])
def test_public_projection_calculates_only_published_values_and_scrubs_fake_metrics(monkeypatch, sanitize):
    monkeypatch.setattr(public, "public_quote_display_policy", lambda: PublicQuoteDisplayPolicy(paper_transaction_reports=True))
    position = {"symbol": "2330", "session_date": "2020-10-14", "filled_shares": 1000,
                "signed_shares": 0, "status": "closed", "entry_price": 999, "exit_price": 1005,
                "entry_at": "2020-10-14T09:01:00+08:00", "exit_at": "2020-10-14T13:24:00+08:00",
                "last_mark_price": 1090, "last_quote_at": "2020-10-14T13:22:00+08:00",
                "entry_price_distances": {"last_mark_price": {"ticks": 12345}}}
    before = deepcopy(position)
    signal = {"symbol": "2330", "session_date": "2020-10-14", "position": position}
    result = sanitize({"simulation_only": True, "production_order_possible": False,
                       "positions": [position], "rows": [signal if sanitize is public.sanitize_tw_signals else position]})
    output = result["positions"][0] if sanitize is public.sanitize_tw_status else result["rows"][0]
    if sanitize is public.sanitize_tw_signals:
        output = output["position"]
    assert output["entry_price_distances"]["exit_price"]["ticks"] == 2
    assert "last_mark_price" not in output["entry_price_distances"]
    assert output["last_mark_price"] is None
    assert position == before
