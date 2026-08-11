from __future__ import annotations

from datetime import date

import pytest

from scripts.scan_taifex_bidask_arbitrage import (
    ContractMeta,
    Quote,
    _leg,
    _scan_chain,
)


EXPIRY = date(2026, 8, 19)


def _future(*, expiry: date = EXPIRY) -> ContractMeta:
    return ContractMeta(
        code="TXFH6",
        security_type="FUT",
        expiry=expiry,
        delivery_month="202608",
        multiplier=200.0,
    )


def _option(code: str, strike: float, right: str) -> ContractMeta:
    return ContractMeta(
        code=code,
        security_type="OPT",
        expiry=EXPIRY,
        delivery_month="202608",
        multiplier=50.0,
        strike=strike,
        right=right,
    )


def _quote(
    code: str,
    bid: float,
    ask: float,
    *,
    bid_qty: int = 10,
    ask_qty: int = 10,
) -> Quote:
    return Quote(
        code=code,
        bid=bid,
        ask=ask,
        bid_qty=bid_qty,
        ask_qty=ask_qty,
        age_ms=1.0,
        receive_ns=1,
    )


def test_put_call_parity_uses_active_bid_ask_and_matching_expiry() -> None:
    future = _future()
    call = _option("C100", 100.0, "C")
    put = _option("P100", 100.0, "P")
    future_quote = _quote(future.code, 100.0, 101.0)
    quotes = {
        future.code: future_quote,
        call.code: _quote(call.code, 10.0, 11.0),
        put.code: _quote(put.code, 1.0, 2.0),
    }

    result = _scan_chain(
        option_metas=[call, put],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
    )["put_call_parity_tx"]

    assert result is not None
    assert result.direction == "sell_rich_synthetic_buy_tx"
    assert result.gross_locked_edge_twd == pytest.approx(1_400.0)
    assert result.net_after_estimated_settlement_tax_twd > 0.0

    mismatched_future = _future(expiry=date(2026, 9, 16))
    mismatch = _scan_chain(
        option_metas=[call, put],
        quotes=quotes,
        future_meta=mismatched_future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
    )["put_call_parity_tx"]
    assert mismatch is None


def test_call_vertical_locked_edge_uses_long_ask_and_short_bid() -> None:
    future = _future()
    low = _option("C90", 90.0, "C")
    high = _option("C100", 100.0, "C")
    future_quote = _quote(future.code, 100.0, 101.0)
    quotes = {
        future.code: future_quote,
        low.code: _quote(low.code, 0.5, 1.0),
        high.code: _quote(high.code, 5.0, 6.0),
    }

    result = _scan_chain(
        option_metas=[low, high],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
    )["call_vertical_bounds"]

    assert result is not None
    assert result.direction == "buy_negative_cost_vertical"
    assert result.gross_locked_edge_twd == pytest.approx(200.0)
    assert result.net_after_estimated_settlement_tax_twd > 0.0


def test_level_one_quantity_must_cover_full_leg() -> None:
    middle = _option("C100", 100.0, "C")
    quote = _quote(middle.code, 5.0, 6.0, bid_qty=1, ask_qty=1)

    assert _leg(middle, quote, side="sell", quantity=2) is None
    assert _leg(middle, quote, side="buy", quantity=2) is None
    assert _leg(middle, quote, side="buy", quantity=1) is not None


def test_midpoint_ceiling_is_more_favorable_than_active_bidask() -> None:
    future = _future()
    low = _option("C90", 90.0, "C")
    high = _option("C100", 100.0, "C")
    future_quote = _quote(future.code, 100.0, 101.0)
    quotes = {
        future.code: future_quote,
        low.code: _quote(low.code, 10.0, 12.0),
        high.code: _quote(high.code, 4.0, 6.0),
    }

    midpoint = _scan_chain(
        option_metas=[low, high],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
        price_mode="midpoint",
        enforce_depth=False,
        objective="gross",
    )["call_vertical_bounds"]
    active = _scan_chain(
        option_metas=[low, high],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
        price_mode="active",
        enforce_depth=False,
        objective="gross",
    )["call_vertical_bounds"]

    assert midpoint is not None
    assert active is not None
    assert midpoint.gross_locked_edge_twd == pytest.approx(-200.0)
    assert active.gross_locked_edge_twd == pytest.approx(-300.0)


def test_no_depth_layer_keeps_package_rejected_by_level_one_gate() -> None:
    future = _future()
    call = _option("C100", 100.0, "C")
    put = _option("P100", 100.0, "P")
    future_quote = _quote(future.code, 100.0, 101.0)
    quotes = {
        future.code: future_quote,
        call.code: _quote(call.code, 10.0, 11.0, bid_qty=1, ask_qty=1),
        put.code: _quote(put.code, 1.0, 2.0, bid_qty=1, ask_qty=1),
    }

    without_depth = _scan_chain(
        option_metas=[call, put],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
        enforce_depth=False,
        objective="gross",
    )["put_call_parity_tx"]
    with_depth = _scan_chain(
        option_metas=[call, put],
        quotes=quotes,
        future_meta=future,
        future_quote=future_quote,
        trading_date=date(2026, 8, 10),
        enforce_depth=True,
        objective="gross",
    )["put_call_parity_tx"]

    assert without_depth is not None
    assert with_depth is None
