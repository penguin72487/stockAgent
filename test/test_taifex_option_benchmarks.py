from __future__ import annotations

from datetime import date, datetime
import io

import numpy as np
import pytest

from scripts.backtest_taifex_atm_straddle_rolling import (
    DayMarket,
    Fill,
    OptionContract,
    _ns,
)
from scripts.backtest_taifex_option_benchmarks_expiry_carry import (
    CarryState,
    _settle_expiry,
)
from scripts.backtest_taifex_option_benchmarks import (
    BENCHMARK_CATALOG,
    FAMILY_CANDIDATE,
    FAMILY_RANDOM,
    FIXED_FEES_PER_CONTRACT_SIDE,
    _build_variants,
    _causal_straddle_delta,
    _future_trade,
    _simulate_underlying,
    _straddle_price_and_delta,
    _variant,
)
from scripts.download_taifex_recent_index_derivatives_ticks import (
    _parse_futures_rows,
)
from stockagent.data.tw_index_derivatives_tick import TAIPEI


def _at(hour: int, minute: int, second: int) -> int:
    return _ns(datetime(2026, 8, 6, hour, minute, second, tzinfo=TAIPEI))


def test_shared_futures_parser_selects_tx_mtx_tmf_without_changing_default() -> None:
    payload = io.StringIO(
        "date,product,month,time,price,quantity,near,far,auction\n"
        "20260806,TX,202608,085000,20000,2,-,-,\n"
        "20260806,MTX,202608,085001,20001,4,-,-,\n"
        "20260806,TMF,202608,085002,20002,6,-,-,\n"
        "20260806,MTX,202608W1/202608,085003,-5,2,20000,19995,\n"
    )
    default = _parse_futures_rows(
        payload,
        trading_date=date(2026, 8, 6),
        source_file="synthetic.csv",
        source_sha256="0" * 64,
    )
    assert default["product"].to_list() == ["TX"]

    payload.seek(0)
    all_products = _parse_futures_rows(
        payload,
        trading_date=date(2026, 8, 6),
        source_file="synthetic.csv",
        source_sha256="0" * 64,
        products=("TX", "MTX", "TMF"),
        outright_contracts_only=True,
    )
    assert set(all_products["product"].to_list()) == {"TX", "MTX", "TMF"}
    assert all_products["matched_quantity"].sum() == 6


@pytest.mark.parametrize(
    ("product", "fee", "tax_per_contract"),
    [("TX", 60.0, 80.0), ("MTX", 24.0, 20.0), ("TMF", 16.0, 4.0)],
)
def test_future_trade_uses_product_specific_contract_side_fee(
    product: str, fee: float, tax_per_contract: float
) -> None:
    event_ns = _at(8, 50, 1)
    market = DayMarket(
        trading_date=date(2026, 8, 6),
        tx_times_ns=np.asarray([event_ns], dtype=np.int64),
        tx_prices=np.asarray([20_000.0]),
        option_events={},
        pair_availability=(),
    )
    variant = _variant("test", f"test_{product}")
    row = _future_trade(
        market=market,
        variant=variant,
        product=product,
        fill=Fill(event_ns=event_ns, price=20_000.0),
        delta_contracts=2,
        reason="test",
        decision_ns=_at(8, 50, 0),
    )
    assert row["fixed_fee_twd"] == pytest.approx(2 * fee)
    assert row["transaction_tax_twd"] == pytest.approx(2 * tax_per_contract)
    assert row["slippage_cost_twd"] == 0.0
    assert row["fill_delay_seconds"] == pytest.approx(1.0)


def test_causal_common_iv_delta_inverts_black_scholes_straddle() -> None:
    price, expected_delta = _straddle_price_and_delta(
        spot=20_250.0,
        strike=20_000.0,
        years_to_expiry=5.0 / 365.0,
        volatility=0.30,
    )
    estimate = _causal_straddle_delta(
        spot=20_250.0,
        strike=20_000.0,
        years_to_expiry=5.0 / 365.0,
        observed_straddle_price=price,
    )
    assert estimate is not None
    delta, implied_vol = estimate
    assert delta == pytest.approx(expected_delta, abs=1e-10)
    assert implied_vol == pytest.approx(0.30, abs=1e-10)


def test_catalog_and_variant_matrix_cover_all_requested_controls() -> None:
    thresholds = tuple(range(50, 1001, 50))
    variants = _build_variants(
        rolling_points=thresholds,
        tp_percent=(25.0, 50.0, 100.0),
        sl_percent=(25.0, 50.0),
        strangle_distances=thresholds,
        time_recenter_minutes=(15, 30, 60),
        delta_bands=(0.2, 0.3),
        random_seed=20260807,
    )
    catalog_families = {row[0] for row in BENCHMARK_CATALOG}
    variant_families = {variant.family for variant in variants}
    assert catalog_families <= variant_families
    assert len(BENCHMARK_CATALOG) == 10
    assert sum(variant.family == FAMILY_CANDIDATE for variant in variants) == 40
    assert sum(variant.family == FAMILY_RANDOM for variant in variants) == 40
    assert FIXED_FEES_PER_CONTRACT_SIDE == {
        "TX": 60.0,
        "MTX": 24.0,
        "TXO": 22.0,
        "TMF": 16.0,
    }


def test_expiry_cash_settlement_taxes_each_in_the_money_rolled_leg() -> None:
    """Crossed rolled strikes can leave both the call and put in the money."""
    trading_date = date(2026, 7, 1)
    market = DayMarket(
        trading_date=trading_date,
        tx_times_ns=np.asarray([], dtype=np.int64),
        tx_prices=np.asarray([], dtype=np.float64),
        option_events={},
        pair_availability=(),
    )
    variant = _variant("test", "crossed_strikes")
    state = CarryState(
        variant=variant,
        cycle_id="202607W1",
        series="202607W1",
        expiry=trading_date,
        opening_date=date(2026, 6, 25),
        opening_underlying=45_000.0,
        opening_premium_twd=10_000.0,
        opening_strikes={"C": 45_000.0, "P": 45_000.0},
        positions={
            "C": OptionContract("202607W1", 44_550.0, "C"),
            "P": OptionContract("202607W1", 47_500.0, "P"),
        },
    )
    rows = _settle_expiry(
        market,
        state=state,
        settlement_payload={"settlement_price": 46_959.0},
        session_end_ns=_at(13, 45, 0),
    )
    assert [row["price_points"] for row in rows] == [2_409.0, 541.0]
    assert [row["transaction_tax_twd"] for row in rows] == [47.0, 47.0]
    assert all(row["fixed_fee_twd"] == 0.0 for row in rows)
    assert state.positions == {}


def test_underlying_control_uses_next_actual_mtx_trade_and_flattens() -> None:
    selection_ns = _at(8, 50, 0)
    entry_ns = _at(8, 50, 1)
    terminal_ns = _at(13, 44, 59)
    session_end_ns = _at(13, 45, 0)
    market = DayMarket(
        trading_date=date(2026, 8, 6),
        tx_times_ns=np.asarray([selection_ns], dtype=np.int64),
        tx_prices=np.asarray([20_000.0]),
        option_events={},
        pair_availability=(),
        futures_events={
            "MTX": (
                np.asarray([entry_ns, terminal_ns], dtype=np.int64),
                np.asarray([20_010.0, 20_020.0]),
            )
        },
    )
    variant = _variant(
        "no_option_underlying",
        "underlying__mtx__long",
        futures_product="MTX",
        direction="long",
    )
    outcome, trades = _simulate_underlying(
        market,
        variant=variant,
        selection_ns=selection_ns,
        close_decision_ns=_at(13, 40, 0),
        session_end_ns=session_end_ns,
    )

    assert [row["fill_ts"] for row in trades] == [
        datetime(2026, 8, 6, 8, 50, 1, tzinfo=TAIPEI),
        datetime(2026, 8, 6, 13, 44, 59, tzinfo=TAIPEI),
    ]
    assert sum(row["delta_contracts"] for row in trades) == 0
    assert outcome["gross_pnl_twd"] == pytest.approx(10.0 * 50.0)
    assert outcome["fixed_fees_twd"] == pytest.approx(2.0 * 24.0)
    assert outcome["net_after_fee_twd"] == pytest.approx(452.0)
