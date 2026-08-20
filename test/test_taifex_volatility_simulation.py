from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.live.taifex_volatility_simulation import (
    EXECUTION_CONTRACT_VERSION,
    FuturesInstrument,
    STRATEGY_IDS,
    TaifexVolatilitySimulation,
)
from stockagent.research.taifex_volatility_metadata import (
    CLASSIC_VARIANT_ID,
    DYNAMIC_HEDGE_STRATEGY_IDS,
    MODEL_VARIANT_PREFIX,
    PUT_CALL_PARITY_TX_STRATEGY_ID,
    ROLLING_ITM_LONG_STRADDLE_ID,
    ROLLING_ITM_SHORT_STRADDLE_ID,
    ROLLING_OTM_LONG_STRADDLE_ID,
    ROLLING_OTM_SHORT_STRADDLE_ID,
    ROLLING_STRADDLE_IDS,
    STRATEGY_MODE_DAILY,
    STRATEGY_MODE_INTRADAY_FUTURES,
    VOLATILITY_MODEL_IDS,
    STRATEGY_SPEC_BY_ID,
)
from stockagent.research.taifex_volatility_models import black76_price


class FakeBase:
    def __init__(self, code: str) -> None:
        self.code = code


class FakeOptionInfo:
    def __init__(
        self,
        *,
        root: str,
        expiry: date,
        strike: float,
        right: str,
    ) -> None:
        self.root = root
        self.delivery_month = expiry.strftime("%Y%m")
        self.delivery_date = expiry
        self.strike_price = strike
        self.option_right = right
        self.base = FakeBase(f"{root}-{expiry:%Y%m%d}-{strike:g}-{right}")


class FakeApi:
    def __init__(self) -> None:
        self.futopt_account = object()

    def update_status(self, *_args, **_kwargs) -> None:
        return None

    def list_trades(self) -> list[object]:
        return []


def _engine(
    tmp_path: Path,
    *,
    bootstrap_after: date,
    strategy_mode: str = STRATEGY_MODE_DAILY,
    hedge_logical_code: str = "MXFR1",
    broker_orders_enabled: bool = False,
    strategy_capital_buffer_multiple: float = 2.0,
    catalog_expansion_entry_policy: str = "next_cycle",
    option_infos_override: list[FakeOptionInfo] | None = None,
    settlement_bootstrap_only: bool = False,
    startup_now: datetime | None = None,
    active_strategy_ids: tuple[str, ...] | None = None,
) -> TaifexVolatilitySimulation:
    expiry = date(2026, 8, 14)
    if option_infos_override is None:
        option_infos = [
            FakeOptionInfo(root="TXV", expiry=expiry, strike=strike, right=right)
            for strike in range(44_000, 46_001, 100)
            for right in ("C", "P")
        ]
        # Cross-expiry points are available to Local/SLV/Rough fits.
        option_infos.extend(
            FakeOptionInfo(
                root="TXO",
                expiry=date(2026, 8, 19),
                strike=strike,
                right=right,
            )
            for strike in range(44_000, 46_001, 100)
            for right in ("C", "P")
        )
    else:
        option_infos = option_infos_override
    return TaifexVolatilitySimulation(
        api=FakeApi(),
        shioaji_module=SimpleNamespace(),
        state_dir=tmp_path,
        option_infos=option_infos,
        underlying=FuturesInstrument(
            logical_code="TXFR1",
            code="TXFH6",
            contract=FakeBase("TXFH6"),
            last_trading_date=date(2026, 8, 19),
        ),
        hedge=FuturesInstrument(
            logical_code=hedge_logical_code,
            code="TMFH6" if hedge_logical_code.startswith("TMF") else "MXFH6",
            contract=FakeBase(
                "TMFH6" if hedge_logical_code.startswith("TMF") else "MXFH6"
            ),
        ),
        final_settlement_path=tmp_path / "settlements.parquet",
        calibration_time=time(13, 29),
        bootstrap_after=bootstrap_after,
        broker_orders_enabled=broker_orders_enabled,
        strategy_mode=strategy_mode,
        strategy_capital_buffer_multiple=strategy_capital_buffer_multiple,
        catalog_expansion_entry_policy=catalog_expansion_entry_policy,
        settlement_bootstrap_only=settlement_bootstrap_only,
        startup_now=startup_now,
        active_strategy_ids=active_strategy_ids,
    )


def _book(
    code: str,
    *,
    bid: float,
    ask: float,
    receive_ns: int,
    simtrade: bool = False,
) -> dict[str, object]:
    return {
        "code": code,
        "receive_ts_ns": receive_ns,
        "simtrade": simtrade,
        "bid_price_1": bid,
        "ask_price_1": ask,
        "bid_volume_1": 10,
        "ask_volume_1": 10,
    }


def _seed_surface_books(
    engine: TaifexVolatilitySimulation,
    *,
    observed_at: datetime,
    receive_ns: int,
    forward: float = 45_000.0,
) -> None:
    engine.on_book(
        _book(
            "TXFH6",
            bid=forward - 5,
            ask=forward + 5,
            receive_ns=receive_ns,
        )
    )
    engine.on_book(
        _book(
            engine.hedge.code,
            bid=forward - 6,
            ask=forward + 6,
            receive_ns=receive_ns,
        )
    )
    for instrument in engine.options:
        expiry_at = datetime.combine(
            instrument.expiry,
            time(13, 30),
            tzinfo=TAIPEI,
        )
        years = (expiry_at - observed_at).total_seconds() / (365.0 * 86400.0)
        volatility = 0.2 + abs(instrument.strike / forward - 1.0) * 0.5
        midpoint = black76_price(
            forward,
            instrument.strike,
            years,
            volatility,
            instrument.right,
        )
        engine.on_book(
            _book(
                instrument.code,
                bid=max(0.1, midpoint - 0.5),
                ask=midpoint + 0.5,
                receive_ns=receive_ns,
            )
        )


def _seed_profitable_put_call_parity_books(
    engine: TaifexVolatilitySimulation,
    *,
    receive_ns: int,
) -> None:
    engine.on_book(_book("TXFH6", bid=44_999.0, ask=45_000.0, receive_ns=receive_ns))
    engine.on_book(
        _book(
            "TXO-20260819-45000-C",
            bid=220.0,
            ask=221.0,
            receive_ns=receive_ns,
        )
    )
    engine.on_book(
        _book(
            "TXO-20260819-45000-P",
            bid=99.0,
            ask=100.0,
            receive_ns=receive_ns,
        )
    )


def test_engine_opens_each_catalog_strategy_with_its_own_option_recipe(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    engine.on_book(_book("TXFH6", bid=44_995, ask=45_005, receive_ns=receive_ns))
    engine.on_book(
        _book("TXV-20260814-45000-C", bid=149, ask=151, receive_ns=receive_ns)
    )
    engine.on_book(
        _book("TXV-20260814-45000-P", bid=139, ask=141, receive_ns=receive_ns)
    )
    engine.step(now=datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI))
    assert engine.state["active_cycle"]["strike"] == 45_000.0
    assert engine.state["engine_status"] == "cycle_open"
    assert tuple(engine.state["strategy_ids"]) == STRATEGY_IDS
    for strategy_id in STRATEGY_IDS:
        strategy = engine.state["strategies"][strategy_id]
        assert strategy["entry_state"] in {
            "entered",
            "waiting_for_fresh_entry_depth",
            "waiting_for_contract_ladder",
            "waiting_for_same_expiry_monthly_books",
            "waiting_for_profitable_parity",
            "signal_pending_next_books",
        }
        spec = STRATEGY_SPEC_BY_ID[strategy_id]
        if strategy["entry_state"] == "entered":
            assert strategy["trade_sides"] == sum(
                abs(quantity) for _right, _offset, quantity in spec.option_legs
            )
            assert strategy["initial_capital_twd"] > 0.0
    rows = [line for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()]
    assert len(rows) >= sum(
        1
        for strategy_id in STRATEGY_IDS
        if engine.state["strategies"][strategy_id]["entry_state"] == "entered"
        and STRATEGY_SPEC_BY_ID[strategy_id].option_legs
    )
    engine.close()


def test_engine_can_scope_a_receipt_replay_to_rolling_straddles(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 10),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        active_strategy_ids=ROLLING_STRADDLE_IDS,
    )
    observed_at = datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI)
    decision_ns = int(observed_at.timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=decision_ns)
    engine._maybe_open_cycle(observed_at, decision_ns + 100_000_000)

    assert tuple(engine.state["strategy_ids"]) == ROLLING_STRADDLE_IDS
    assert set(engine.state["strategies"]) == set(ROLLING_STRADDLE_IDS)
    assert all(
        engine.state["strategies"][strategy_id]["entry_state"] == "entered"
        for strategy_id in ROLLING_STRADDLE_IDS
    )
    engine._maybe_enforce_strategy_margin(decision_ns + 200_000_000)
    engine.close()


@pytest.mark.parametrize(
    ("strategy_id", "rolled_right", "quantity"),
    (
        (ROLLING_ITM_LONG_STRADDLE_ID, "C", 1),
        (ROLLING_ITM_SHORT_STRADDLE_ID, "C", -1),
        (ROLLING_OTM_LONG_STRADDLE_ID, "P", 1),
        (ROLLING_OTM_SHORT_STRADDLE_ID, "P", -1),
    ),
)
def test_rolling_straddles_atomically_replace_selected_leg_on_later_books(
    tmp_path: Path,
    strategy_id: str,
    rolled_right: str,
    quantity: int,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    observed_at = datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI)
    base_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=base_ns,
    )
    engine._maybe_open_cycle(observed_at, base_ns + 100_000_000)

    old_code = f"TXV-20260814-45000-{rolled_right}"
    new_code = f"TXV-20260814-45100-{rolled_right}"
    untouched_right = "P" if rolled_right == "C" else "C"
    untouched_code = f"TXV-20260814-45000-{untouched_right}"
    signal_ns = base_ns + 300_000_000
    engine.on_book(
        _book(
            "TXFH6",
            bid=45_095.0,
            ask=45_105.0,
            receive_ns=base_ns + 200_000_000,
        )
    )
    engine._maybe_roll_straddles(signal_ns)

    ledger = engine.state["strategies"][strategy_id]
    assert ledger["pending_option_roll"]["signal_decision_ts_ns"] == signal_ns
    assert ledger["trade_sides"] == 2
    assert ledger["option_positions"] == {
        "TXV-20260814-45000-C": quantity,
        "TXV-20260814-45000-P": quantity,
    }

    later_receive_ns = signal_ns + 100_000_000
    engine.on_book(
        _book(
            old_code,
            bid=111.0,
            ask=113.0,
            receive_ns=later_receive_ns,
        )
    )
    engine.on_book(
        _book(
            new_code,
            bid=201.0,
            ask=207.0,
            receive_ns=later_receive_ns + 1,
        )
    )
    execution_ns = later_receive_ns + 100_000_000
    engine._maybe_roll_straddles(execution_ns)

    assert ledger["pending_option_roll"] is None
    assert ledger["option_roll_count"] == 1
    assert ledger["trade_sides"] == 4
    assert ledger["option_positions"] == {
        new_code: quantity,
        untouched_code: quantity,
    }
    rolling_rows = []
    for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines():
        row = json.loads(line)
        if row["strategy_id"] == strategy_id and str(row["reason"]).startswith(
            "rolling_straddle_"
        ):
            rolling_rows.append(row)
    assert len(rolling_rows) == 2
    close_row, open_row = rolling_rows
    assert close_row["code"] == old_code
    assert close_row["delta_contracts"] == -quantity
    assert close_row["price_points"] == (113.0 if quantity < 0 else 111.0)
    assert open_row["code"] == new_code
    assert open_row["delta_contracts"] == quantity
    assert open_row["price_points"] == (207.0 if quantity > 0 else 201.0)
    assert all(
        row["signal_decision_ts_ns"] == signal_ns
        and signal_ns < row["book_receive_ts_ns"] <= execution_ns
        for row in rolling_rows
    )
    engine.close()


def test_rolling_straddle_waits_without_mutating_when_one_later_book_is_missing(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    observed_at = datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI)
    base_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=base_ns,
    )
    engine._maybe_open_cycle(observed_at, base_ns + 100_000_000)
    engine.on_book(
        _book(
            "TXFH6",
            bid=45_095.0,
            ask=45_105.0,
            receive_ns=base_ns + 200_000_000,
        )
    )
    signal_ns = base_ns + 300_000_000
    engine._maybe_roll_straddles(signal_ns)

    ledger = engine.state["strategies"][ROLLING_ITM_LONG_STRADDLE_ID]
    before_positions = dict(ledger["option_positions"])
    before_trade_sides = ledger["trade_sides"]
    engine.on_book(
        _book(
            "TXV-20260814-45100-C",
            bid=201.0,
            ask=207.0,
            receive_ns=signal_ns + 100_000_000,
        )
    )
    engine._maybe_roll_straddles(signal_ns + 200_000_000)

    assert ledger["pending_option_roll"] is not None
    assert ledger["option_positions"] == before_positions
    assert ledger["trade_sides"] == before_trade_sides
    engine.close()


def test_put_call_parity_tx_waits_for_later_books_then_locks_positive_net_edge(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 14, 10, 0, tzinfo=TAIPEI)
    base_ns = int(observed_at.timestamp() * 1e9)
    _seed_profitable_put_call_parity_books(engine, receive_ns=base_ns)

    signal_ns = base_ns + 100_000_000
    engine._maybe_run_put_call_parity(observed_at, signal_ns)
    parity_state = engine.state["put_call_parity_tx"]
    assert parity_state["pending_signal"]["signal_decision_ts_ns"] == signal_ns
    assert parity_state["monitor"]["state"] == "signal_pending_next_books"
    assert not (tmp_path / "ideal_ledger.jsonl").exists()

    next_receive_ns = base_ns + 200_000_000
    execution_ns = base_ns + 300_000_000
    _seed_profitable_put_call_parity_books(
        engine,
        receive_ns=next_receive_ns,
    )
    engine._maybe_run_put_call_parity(observed_at, execution_ns)

    ledger = engine.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
    assert ledger["option_positions"] == {
        "TXO-20260819-45000-C": -4,
        "TXO-20260819-45000-P": 4,
    }
    assert ledger["underlying_futures_position"] == 1
    assert ledger["futures_position"] == 0
    assert ledger["initial_capital_twd"] > ledger["entry_capital_requirement_twd"]
    monitor = engine.state["put_call_parity_tx"]["monitor"]
    assert monitor["state"] == "locked_until_official_settlement"
    assert monitor["net_after_estimated_cost_twd"] > 0.0
    assert monitor["broker_submission"] is False
    rows = [
        json.loads(line)
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
    ]
    assert len(rows) == 3
    assert {row["instrument_type"] for row in rows} == {"option", "future"}
    assert all(
        signal_ns < row["book_receive_ts_ns"] <= row["decision_ts_ns"] for row in rows
    )
    assert {row["signal_decision_ts_ns"] for row in rows} == {signal_ns}
    engine.close()


def test_put_call_parity_tx_rejects_nonpositive_cost_adjusted_package(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 14, 10, 0, tzinfo=TAIPEI)
    decision_ns = int(observed_at.timestamp() * 1e9)
    engine.on_book(_book("TXFH6", bid=44_999.0, ask=45_001.0, receive_ns=decision_ns))
    for right in ("C", "P"):
        engine.on_book(
            _book(
                f"TXO-20260819-45000-{right}",
                bid=149.0,
                ask=151.0,
                receive_ns=decision_ns,
            )
        )

    engine._maybe_run_put_call_parity(observed_at, decision_ns + 100_000_000)

    ledger = engine.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
    assert ledger["option_positions"] == {}
    assert ledger["underlying_futures_position"] == 0
    assert ledger["entry_state"] == "waiting_for_profitable_parity"
    assert engine.state["put_call_parity_tx"]["monitor"]["state"] == (
        "no_positive_edge_after_cost"
    )
    assert engine.state["put_call_parity_tx"]["pending_signal"] is None
    assert not (tmp_path / "ideal_ledger.jsonl").exists()
    engine.close()


def test_put_call_parity_tx_cash_settles_all_three_legs_at_official_price(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 14, 10, 0, tzinfo=TAIPEI)
    base_ns = int(observed_at.timestamp() * 1e9)
    _seed_profitable_put_call_parity_books(engine, receive_ns=base_ns)
    signal_ns = base_ns + 100_000_000
    engine._maybe_run_put_call_parity(observed_at, signal_ns)
    _seed_profitable_put_call_parity_books(
        engine,
        receive_ns=base_ns + 200_000_000,
    )
    engine._maybe_run_put_call_parity(observed_at, base_ns + 300_000_000)
    locked_edge = engine.state["put_call_parity_tx"]["monitor"][
        "locked_net_edge_after_estimated_cost_twd"
    ]
    pl.DataFrame(
        {
            "settlement_date": [date(2026, 8, 19)],
            "option_series": ["202608"],
            "final_settlement_price": [45_100.0],
            "source_file": ["official.html"],
            "source_sha256": ["b" * 64],
        }
    ).write_parquet(tmp_path / "settlements.parquet")

    settlement_at = datetime(2026, 8, 20, 8, 46, tzinfo=TAIPEI)
    settlement_ns = int(settlement_at.timestamp() * 1e9)
    engine._maybe_settle_expired_put_call_parity(
        settlement_at,
        settlement_ns,
    )

    ledger = engine.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]
    assert ledger["option_positions"] == {}
    assert ledger["underlying_futures_position"] == 0
    assert ledger["entry_state"] == "settled_waiting_next_monthly_contract"
    parity_state = engine.state["put_call_parity_tx"]
    assert parity_state["open_position"] is None
    assert parity_state["last_settled_expiry"] == "2026-08-19"
    assert parity_state["monitor"]["state"] == ("settled_waiting_next_monthly_contract")
    assert parity_state["monitor"]["realized_cumulative_pnl_twd"] == pytest.approx(
        locked_edge
    )
    settlement_rows = [
        json.loads(line)
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
        if json.loads(line)["reason"] == "put_call_parity_tx_official_cash_settlement"
    ]
    assert len(settlement_rows) == 3
    assert {row["price_source"] for row in settlement_rows} == {
        "official_taifex_final_settlement",
        "official_taifex_final_settlement_intrinsic_value",
    }
    engine.close()


def test_missing_book_carries_last_complete_equity_for_same_position_only(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        hedge_logical_code="TMFR1",
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    engine._maybe_open_cycle(observed_at, receive_ns)

    fresh = engine._strategy_mark(STRATEGY_IDS[0], receive_ns + 1_000_000_000)
    assert fresh["valuation_source"] == "fresh_executable_bidask"
    assert fresh["valuation_stale"] is False
    assert fresh["total_equity_twd"] == (
        fresh["initial_capital_twd"] + fresh["cumulative_pnl_twd"]
    )

    stale = engine._strategy_mark(STRATEGY_IDS[0], receive_ns + 121_000_000_000)
    assert stale["valuation_source"] == "carried_forward_last_complete_mark"
    assert stale["valuation_stale"] is True
    assert stale["valuation_available"] is True
    assert stale["open_liquidation_value_twd"] == fresh["open_liquidation_value_twd"]
    assert stale["total_equity_twd"] == fresh["total_equity_twd"]

    engine.state["strategies"][STRATEGY_IDS[0]]["futures_position"] = 1
    changed_position = engine._strategy_mark(
        STRATEGY_IDS[0], receive_ns + 122_000_000_000
    )
    assert changed_position["valuation_source"] == "unavailable"
    assert changed_position["valuation_available"] is False
    assert changed_position["total_equity_twd"] is None
    engine.close()


def test_flat_strategy_waiting_for_roll_entry_has_exact_zero_live_value(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    engine.state["active_cycle"] = {"cycle_id": "next_nearest_weekly"}
    ledger = engine.state["strategies"]["long_strap_2c1p"]
    ledger["entry_state"] = "waiting_for_fresh_entry_depth"
    ledger["option_positions"] = {}
    ledger["futures_position"] = 0

    decision_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    mark = engine._strategy_mark("long_strap_2c1p", decision_ns)

    assert mark["valuation_available"] is True
    assert mark["valuation_source"] == "fresh_executable_bidask"
    assert mark["open_liquidation_value_twd"] == 0.0
    assert mark["cumulative_pnl_twd"] == pytest.approx(
        float(ledger["gross_cash_twd"])
        - float(ledger["fees_twd"])
        - float(ledger["tax_twd"])
    )
    engine.close()


def test_engine_does_not_open_from_stale_books(tmp_path: Path) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    stale_ns = int(datetime.now(timezone.utc).timestamp() * 1e9) - 10_000_000_000
    engine.on_book(_book("TXFH6", bid=44_995, ask=45_005, receive_ns=stale_ns))
    engine.step(now=datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI))
    assert engine.state["active_cycle"] is None
    assert engine.state["engine_status"] == "waiting_for_fresh_open_books"
    engine.close()


def test_short_straddle_recapitalizes_next_trading_date_and_accumulates_capital(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    engine.step(now=observed_at)

    strategy_id = "short_atm_straddle"
    ledger = engine.state["strategies"][strategy_id]
    assert sorted(ledger["option_positions"].values()) == [-1, -1]
    assert ledger["gross_cash_twd"] > 0.0
    assert ledger["initial_capital_twd"] > 2 * 94_000.0
    entry_rows = [
        json.loads(line)
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
        if json.loads(line)["strategy_id"] == strategy_id
    ]
    assert {row["price_source"] for row in entry_rows} == {
        "causally_received_five_level_bid_vwap"
    }

    cycle = engine.state["active_cycle"]
    loss_receive_ns = receive_ns + 1_000_000_000
    for code in (cycle["call_code"], cycle["put_code"]):
        engine.on_book(
            _book(
                code,
                bid=9_999.0,
                ask=10_000.0,
                receive_ns=loss_receive_ns,
            )
        )
    engine._maybe_enforce_strategy_margin(loss_receive_ns + 1_000_000_000)

    ledger = engine.state["strategies"][strategy_id]
    assert ledger["option_positions"] == {}
    assert ledger["alive"] is False
    assert ledger["entry_state"] == "awaiting_next_trading_date_recapitalization"
    assert ledger["margin_call_count"] == 1
    assert ledger["bankruptcy_count"] == 1
    contributed_before_recap = float(
        ledger["cumulative_contributed_capital_twd"]
    )
    forced_rows = [
        json.loads(line)
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
        if json.loads(line)["strategy_id"] == strategy_id
        and "forced_flatten" in json.loads(line)["reason"]
    ]
    assert len(forced_rows) == 2
    assert {row["price_source"] for row in forced_rows} == {
        "forced_liquidation_five_level_depth_vwap"
    }

    same_day_receive_ns = loss_receive_ns + 2_000_000_000
    _seed_surface_books(
        engine,
        observed_at=datetime.fromtimestamp(same_day_receive_ns / 1e9, tz=TAIPEI),
        receive_ns=same_day_receive_ns,
    )
    assert not engine._enter_strategy_for_cycle(
        strategy_id,
        decision_ns=same_day_receive_ns + 1_000_000_000,
    )
    assert not ledger["alive"]
    assert (
        float(ledger["cumulative_contributed_capital_twd"])
        == contributed_before_recap
    )

    next_day_receive_ns = same_day_receive_ns + int(timedelta(days=1).total_seconds() * 1e9)
    _seed_surface_books(
        engine,
        observed_at=datetime.fromtimestamp(next_day_receive_ns / 1e9, tz=TAIPEI),
        receive_ns=next_day_receive_ns,
    )
    assert engine._enter_strategy_for_cycle(
        strategy_id,
        decision_ns=next_day_receive_ns + 1_000_000_000,
    )
    assert ledger["alive"]
    assert ledger["entry_state"] == "entered"
    assert ledger["recapitalization_count"] == 1
    first_recapitalized_total = float(
        ledger["cumulative_contributed_capital_twd"]
    )
    assert first_recapitalized_total > contributed_before_recap

    second_loss_receive_ns = next_day_receive_ns + 2_000_000_000
    for code in list(ledger["option_positions"]):
        engine.on_book(
            _book(code, bid=9_999.0, ask=10_000.0, receive_ns=second_loss_receive_ns)
        )
    engine._maybe_enforce_strategy_margin(second_loss_receive_ns + 1_000_000_000)
    assert not ledger["alive"]
    assert ledger["bankruptcy_count"] == 2

    second_recap_receive_ns = second_loss_receive_ns + int(
        timedelta(days=1).total_seconds() * 1e9
    )
    _seed_surface_books(
        engine,
        observed_at=datetime.fromtimestamp(second_recap_receive_ns / 1e9, tz=TAIPEI),
        receive_ns=second_recap_receive_ns,
    )
    ledger["entry_state"] = "waiting_for_contract_ladder"
    assert engine._strategy_recapitalization_is_due(
        strategy_id,
        second_recap_receive_ns + 1_000_000_000,
    )
    assert engine._enter_strategy_for_cycle(
        strategy_id,
        decision_ns=second_recap_receive_ns + 1_000_000_000,
    )
    assert ledger["recapitalization_count"] == 2
    assert (
        float(ledger["cumulative_contributed_capital_twd"])
        > first_recapitalized_total
    )
    contribution_rows = [
        json.loads(line)
        for line in (tmp_path / "capital_contributions.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if json.loads(line)["strategy_id"] == strategy_id
    ]
    assert [row["reason"] for row in contribution_rows].count(
        "post_bankruptcy_one_package_recapitalization"
    ) == 2
    recapitalizations = [
        row
        for row in contribution_rows
        if row["reason"] == "post_bankruptcy_one_package_recapitalization"
    ]
    assert all(
        str(row["last_bankruptcy_trading_date"]) < str(row["trading_date"])
        for row in recapitalizations
    )
    assert recapitalizations[0]["last_bankruptcy_trading_date"] < (
        recapitalizations[1]["last_bankruptcy_trading_date"]
    )
    engine.close()


def test_complete_books_start_all_live_catalog_curves_without_margin_failure(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    engine.step(now=observed_at)

    assert len(engine.state["strategies"]) == len(STRATEGY_IDS) == 58
    for strategy_id in ("naked_short_call", "naked_short_put"):
        ledger = engine.state["strategies"][strategy_id]
        assert len(ledger["option_positions"]) == 1
        assert next(iter(ledger["option_positions"].values())) == -1
    assert {
        ledger["entry_state"]
        for strategy_id, ledger in engine.state["strategies"].items()
        if strategy_id != PUT_CALL_PARITY_TX_STRATEGY_ID
    } == {"entered"}
    assert engine.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID][
        "entry_state"
    ] in {"waiting_for_profitable_parity", "signal_pending_next_books"}
    assert all(ledger["alive"] for ledger in engine.state["strategies"].values())
    assert (
        sum(
            int(ledger["margin_call_count"])
            for ledger in engine.state["strategies"].values()
        )
        == 0
    )
    engine._write_marks(receive_ns + 1_000_000_000)
    engine._write_status(force=True, decision_ns=receive_ns + 1_000_000_000)
    mark_rows = (tmp_path / "marks.jsonl").read_text().splitlines()
    assert len(mark_rows) >= len(STRATEGY_IDS)
    status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert status["strategy_count"] == len(STRATEGY_IDS)
    assert status["strategy_fresh_valuation_count"] == len(STRATEGY_IDS)
    assert status["strategy_valuation_available_count"] == len(STRATEGY_IDS)
    assert status["held_option_contract_count"] > 0
    assert (
        status["held_option_subscribed_count"] == status["held_option_contract_count"]
    )
    assert status["missing_held_option_subscription_codes"] == []
    engine.close()


def test_positive_equity_future_margin_deficit_does_not_churn_or_reenter(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    engine.step(now=observed_at)

    strategy_id = "underlying_hedge_future_long"
    ledger = engine.state["strategies"][strategy_id]
    ledger["initial_capital_twd"] = 10_000.0
    ledger["cumulative_contributed_capital_twd"] = 10_000.0
    mark = engine._strategy_mark(strategy_id, receive_ns + 1_000_000_000)
    assert 0.0 < mark["total_equity_twd"] < mark["margin_required_twd"]
    engine._maybe_enforce_strategy_margin(receive_ns + 1_000_000_000)
    assert ledger["margin_call_count"] == 0
    assert ledger["futures_position"] == 1

    assert engine._force_liquidate_strategy(
        strategy_id,
        decision_ns=receive_ns + 1_000_000_000,
        reason="test_margin_terminal_flatten",
    )
    sides_after_flatten = int(ledger["trade_sides"])
    assert ledger["entry_state"] == "forced_flat"
    assert ledger["futures_position"] == 0
    engine._maybe_enter_cycle_strategies(receive_ns + 1_000_000_000)
    engine._maybe_apply_fixed_future_targets(
        observed_at,
        receive_ns + 1_000_000_000,
    )
    assert ledger["entry_state"] == "forced_flat"
    assert ledger["futures_position"] == 0
    assert ledger["trade_sides"] == sides_after_flatten
    engine.close()


def test_restart_rejects_option_risk_margin_contract_change(tmp_path: Path) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.close()
    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["option_risk_margin_a_twd"] = 170_000.0
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(
        RuntimeError,
        match="strategy option risk-margin/capital contract mismatch",
    ):
        _engine(tmp_path, bootstrap_after=date(2026, 8, 12))


def test_restart_rejects_persisted_held_option_without_worker_subscription(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.state["strategies"][CLASSIC_VARIANT_ID]["option_positions"] = {
        "TXV-NOT-SUBSCRIBED": 1
    }
    engine.close()

    with pytest.raises(
        RuntimeError,
        match=(
            "persisted held or pending-roll option contracts are not subscribed "
            "on strategy worker 0"
        ),
    ):
        _engine(tmp_path, bootstrap_after=date(2026, 8, 12))


def test_v8_restart_pair_repair_preserves_only_ideal_ledger_backed_positions(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    call_code = "TXV-20260814-45000-C"
    put_code = "TXV-20260814-45000-P"
    engine.state["active_cycle"] = {
        "cycle_id": "current_weekly_cycle",
        "expiry_date": "2026-08-14",
        "series": "TXV:202608:2026-08-14",
        "strike": 45_000.0,
        "call_code": call_code,
        "put_code": put_code,
        "strategy_entries": {
            CLASSIC_VARIANT_ID: "entered",
            "long_strap_2c1p": "waiting_for_fresh_entry_depth",
            "underlying_hedge_future_long": "entered",
            PUT_CALL_PARITY_TX_STRATEGY_ID: "independent_monthly_lifecycle",
        },
    }
    decision_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    for code, right in ((call_code, "C"), (put_code, "P")):
        engine._record_ideal_trade(
            strategy_id=CLASSIC_VARIANT_ID,
            instrument_type="option",
            product="TXO",
            code=code,
            delta_contracts=1,
            price_points=100.0,
            decision_ns=decision_ns,
            receive_ns=decision_ns,
            reason="strategy_catalog_cycle_entry",
            series="TXV:202608:2026-08-14",
            strike=45_000.0,
            option_right=right,
            fee_twd=0.0,
            tax_twd=0.0,
        )
    engine.state["strategies"][CLASSIC_VARIANT_ID]["entry_state"] = "entered"
    fabricated_ids = (
        "long_strap_2c1p",
        "underlying_hedge_future_long",
        PUT_CALL_PARITY_TX_STRATEGY_ID,
    )
    for strategy_id in fabricated_ids:
        ledger = engine.state["strategies"][strategy_id]
        ledger["option_positions"] = {call_code: 1, put_code: 1}
        ledger["option_position_metadata"] = {
            call_code: {
                "series": "TXV:202608:2026-08-14",
                "strike": 45_000.0,
                "option_right": "C",
            },
            put_code: {
                "series": "TXV:202608:2026-08-14",
                "strike": 45_000.0,
                "option_right": "P",
            },
        }
    engine.state["strategies"]["long_strap_2c1p"]["entry_state"] = "entered"
    engine.state["strategies"]["underlying_hedge_future_long"]["entry_state"] = (
        "entered"
    )
    engine.state["strategies"]["underlying_hedge_future_long"]["futures_position"] = 1
    engine.close()

    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["execution_contract_version"] = 8
    state_path.write_text(json.dumps(state), encoding="utf-8")

    migrated = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    assert migrated.state["execution_contract_version"] == EXECUTION_CONTRACT_VERSION
    assert migrated.state["strategies"][CLASSIC_VARIANT_ID]["option_positions"] == {
        call_code: 1,
        put_code: 1,
    }
    assert migrated.state["strategies"]["long_strap_2c1p"]["option_positions"] == {}
    assert (
        migrated.state["strategies"]["long_strap_2c1p"]["entry_state"]
        == "waiting_for_fresh_entry_depth"
    )
    assert (
        migrated.state["strategies"]["underlying_hedge_future_long"]["option_positions"]
        == {}
    )
    assert (
        migrated.state["strategies"]["underlying_hedge_future_long"]["entry_state"]
        == "entered"
    )
    assert (
        migrated.state["strategies"]["underlying_hedge_future_long"]["futures_position"]
        == 1
    )
    assert (
        migrated.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]["option_positions"]
        == {}
    )
    assert (
        migrated.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID]["entry_state"]
        == "waiting_for_same_expiry_monthly_books"
    )
    migration_events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
        if json.loads(line).get("event") == "execution_contract_migrated"
    ]
    assert migration_events[-1]["repaired_v8_restart_strategy_ids"] == sorted(
        fabricated_ids
    )
    migrated.close()

    restarted = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    assert restarted.state["strategies"]["long_strap_2c1p"]["option_positions"] == {}
    assert (
        restarted.state["strategies"][PUT_CALL_PARITY_TX_STRATEGY_ID][
            "option_positions"
        ]
        == {}
    )
    restarted.close()


def test_restart_migrates_exact_official_2026_08_13_margin_step(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.state["strategies"][CLASSIC_VARIANT_ID]["gross_cash_twd"] = 12_345.0
    engine.close()
    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["execution_contract_version"] = 6
    state["option_risk_margin_a_twd"] = 169_000.0
    state["option_risk_margin_b_twd"] = 85_000.0
    state.pop("option_risk_margin_c_twd", None)
    state_path.write_text(json.dumps(state), encoding="utf-8")

    migrated = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))

    assert migrated.state["execution_contract_version"] == EXECUTION_CONTRACT_VERSION
    assert migrated.state["option_risk_margin_a_twd"] == 187_000.0
    assert migrated.state["option_risk_margin_b_twd"] == 94_000.0
    assert migrated.state["option_risk_margin_c_twd"] == 18_800.0
    assert (
        migrated.state["strategies"][CLASSIC_VARIANT_ID]["gross_cash_twd"] == 12_345.0
    )
    migration = json.loads(
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    assert migration["event"] == "execution_contract_migrated"
    assert migration["margin_schedule"]["positions_and_pnl_preserved"] is True
    migrated.close()


def test_restart_migrates_v9_ledgers_to_causal_rolling_state(tmp_path: Path) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.state["strategies"][CLASSIC_VARIANT_ID]["gross_cash_twd"] = 12_345.0
    engine.close()
    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["execution_contract_version"] = 9
    for ledger in state["strategies"].values():
        for key in (
            "pending_option_roll",
            "option_roll_count",
            "last_option_roll_decision_ts_ns",
            "last_option_roll_signal_ts_ns",
            "last_option_roll_forward_mid",
            "last_option_roll_atm_strike",
        ):
            ledger.pop(key, None)
    state_path.write_text(json.dumps(state), encoding="utf-8")

    migrated = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))

    assert migrated.state["execution_contract_version"] == EXECUTION_CONTRACT_VERSION
    classic = migrated.state["strategies"][CLASSIC_VARIANT_ID]
    assert classic["gross_cash_twd"] == 12_345.0
    assert classic["pending_option_roll"] is None
    assert classic["option_roll_count"] == 0
    migration = json.loads(
        (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()[-1]
    )
    assert migration["event"] == "execution_contract_migrated"
    assert migration["from_version"] == 9
    assert migration["reason"] == "causal_rolling_straddle_migration"
    migrated.close()


def test_restart_recovers_partial_hot_reload_margin_step(tmp_path: Path) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.close()
    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["execution_contract_version"] = EXECUTION_CONTRACT_VERSION
    state["option_risk_margin_a_twd"] = 169_000.0
    state["option_risk_margin_b_twd"] = 85_000.0
    state["option_risk_margin_c_twd"] = 18_800.0
    state_path.write_text(json.dumps(state), encoding="utf-8")

    migrated = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))

    assert migrated.state["option_risk_margin_a_twd"] == 187_000.0
    assert migrated.state["option_risk_margin_b_twd"] == 94_000.0
    assert migrated.state["option_risk_margin_c_twd"] == 18_800.0
    migrated.close()


@pytest.mark.parametrize(
    ("catalog_expansion_entry_policy", "expected_entry_state"),
    (("next_cycle", "waiting_next_cycle"), ("immediate_live", "pending")),
)
def test_active_seven_strategy_migration_capitalizes_new_curve_on_first_fill(
    tmp_path: Path,
    catalog_expansion_entry_policy: str,
    expected_entry_state: str,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    observed_at = datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    engine.step(now=observed_at)
    legacy_ids = (
        CLASSIC_VARIANT_ID,
        *(f"{MODEL_VARIANT_PREFIX}{model_id}" for model_id in VOLATILITY_MODEL_IDS),
    )
    engine.close()
    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["strategy_ids"] = list(legacy_ids)
    state["strategies"] = {
        strategy_id: state["strategies"][strategy_id] for strategy_id in legacy_ids
    }
    state["active_cycle"]["strategy_entries"] = {
        strategy_id: "entered" for strategy_id in legacy_ids
    }
    state["execution_contract_version"] = 5
    state_path.write_text(json.dumps(state), encoding="utf-8")

    migrated = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 10),
        catalog_expansion_entry_policy=catalog_expansion_entry_policy,
    )
    strategy_id = "covered_call"
    ledger = migrated.state["strategies"][strategy_id]
    assert ledger["entry_state"] == expected_entry_state
    prefill_capital = float(ledger["initial_capital_twd"])
    if expected_entry_state == "waiting_next_cycle":
        ledger["entry_state"] = "pending"
        migrated.state["active_cycle"]["strategy_entries"][strategy_id] = "pending"
    _seed_surface_books(migrated, observed_at=observed_at, receive_ns=receive_ns)

    assert migrated._enter_strategy_for_cycle(
        strategy_id,
        decision_ns=receive_ns,
    )
    ledger = migrated.state["strategies"][strategy_id]
    assert ledger["initial_capital_twd"] > prefill_capital
    assert ledger["initial_capital_twd"] > ledger["entry_capital_requirement_twd"]
    migrated.close()


def test_successful_step_clears_only_the_previous_transient_step_error(
    tmp_path: Path,
    monkeypatch,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    original = engine._maybe_open_cycle

    def fail_once(*_args, **_kwargs) -> None:
        raise ValueError("surface is still warming")

    monkeypatch.setattr(engine, "_maybe_open_cycle", fail_once)
    engine.step(now=datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI))
    assert engine.state["engine_status"] == "blocked"
    assert engine.state["last_engine_step_error"] == (
        "ValueError: surface is still warming"
    )

    monkeypatch.setattr(engine, "_maybe_open_cycle", original)
    engine.step(now=datetime(2026, 8, 12, 10, 0, 1, tzinfo=TAIPEI))
    assert engine.state["blocked_reason"] is None
    assert engine.state["last_engine_step_error"] is None

    engine.state["engine_status"] = "intraday_active"
    engine.state["blocked_reason"] = (
        "ValueError: held-series IV surface is too sparse: series=test points=7"
    )
    engine.step(now=datetime(2026, 8, 12, 10, 0, 2, tzinfo=TAIPEI))
    assert engine.state["blocked_reason"] is None
    engine.close()


def test_intraday_mode_late_starts_and_calibrates_once_per_minute(
    tmp_path: Path,
    monkeypatch,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(observed_at.timestamp() * 1e9)
    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.time.time_ns",
        lambda: receive_ns + 1_000_000,
    )
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=receive_ns,
    )

    engine.step(now=observed_at)
    cycle = engine.state["active_cycle"]
    assert cycle["entry_timing"] == "intraday_late_start"
    assert cycle["strategy_mode"] == STRATEGY_MODE_INTRADAY_FUTURES
    assert engine.state["engine_status"] == "intraday_active"
    assert engine.state["last_intraday_decision_bucket"] is not None
    rows = [
        json.loads(line)
        for line in (tmp_path / "calibrations.jsonl").read_text().splitlines()
    ]
    expected_dynamic = {
        strategy_id
        for strategy_id in DYNAMIC_HEDGE_STRATEGY_IDS
        if STRATEGY_SPEC_BY_ID[strategy_id].hedge_policy
        not in {"fixed_future", "fixed_index_equivalent"}
    }
    assert len(rows) == len(expected_dynamic)
    assert {row["strategy_id"] for row in rows} == expected_dynamic
    assert {row["execution_timing"] for row in rows} == {
        "same_decision_executable_bidask"
    }
    assert all(row["decision_ts_ns"] >= receive_ns for row in rows)

    engine.step(now=observed_at.replace(second=30))
    duplicate_rows = (tmp_path / "calibrations.jsonl").read_text().splitlines()
    assert len(duplicate_rows) == len(expected_dynamic)
    engine.close()


def test_intraday_sparse_iv_surface_waits_without_blocking_engine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(observed_at.timestamp() * 1e9)
    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.time.time_ns",
        lambda: receive_ns + 1_000_000,
    )
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=receive_ns,
    )

    def sparse_surface(*_args, **_kwargs):
        raise ValueError("live Bid/Ask IV surface is too sparse: 0 points")

    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.build_bidask_iv_surface",
        sparse_surface,
    )
    engine.step(now=observed_at)

    assert engine.state["engine_status"] == "waiting_for_sufficient_iv_surface"
    assert engine.state["last_iv_surface_error"] == (
        "live Bid/Ask IV surface is too sparse: 0 points"
    )
    assert engine.state.get("last_engine_step_error") is None
    engine.close()


def test_intraday_sparse_held_series_waits_without_blocking_engine(
    tmp_path: Path,
    monkeypatch,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(observed_at.timestamp() * 1e9)
    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.time.time_ns",
        lambda: receive_ns + 1_000_000,
    )
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=receive_ns,
    )

    def sparse_held_series(*_args, **_kwargs):
        raise ValueError(
            "held-series IV surface is too sparse: series=test points=7"
        )

    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.fit_volatility_model",
        sparse_held_series,
    )
    engine.step(now=observed_at)

    assert engine.state["engine_status"] == (
        "waiting_for_sufficient_held_series_iv_surface"
    )
    assert engine.state["last_iv_surface_error"] == (
        "held-series IV surface is too sparse: series=test points=7"
    )
    assert engine.state.get("last_engine_step_error") is None
    engine.close()


def test_intraday_mode_flattens_all_model_futures_before_close(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    for model_id in VOLATILITY_MODEL_IDS:
        strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
        engine.state["strategies"][strategy_id]["futures_position"] = 1
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    engine.on_book(_book("MXFH6", bid=44_994, ask=45_006, receive_ns=receive_ns))

    engine.step(now=datetime(2026, 8, 12, 13, 36, tzinfo=TAIPEI))
    for model_id in VOLATILITY_MODEL_IDS:
        strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
        assert engine.state["strategies"][strategy_id]["futures_position"] == 0
    assert engine.state["last_intraday_flatten_date"] == "2026-08-12"
    assert engine.state["engine_status"] == "intraday_flat_for_day_close"
    rows = [
        json.loads(line)
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
    ]
    flattened = [
        row
        for row in rows
        if row["reason"] == "intraday_futures_flatten_before_day_session_close"
    ]
    assert len(flattened) == len(VOLATILITY_MODEL_IDS)
    assert all(row["delta_contracts"] == -1 for row in flattened)
    engine.close()


def test_intraday_mode_trades_night_session_on_following_trading_date(
    tmp_path: Path,
    monkeypatch,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 12, 15, 1, tzinfo=TAIPEI)
    receive_ns = int(observed_at.timestamp() * 1e9)
    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.time.time_ns",
        lambda: receive_ns + 1_000_000,
    )
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)

    engine.step(now=observed_at)

    assert engine.state["active_cycle"]["entry_date"] == "2026-08-13"
    assert engine.state["active_cycle"]["entry_session"] == "night"
    assert engine.state["active_cycle"]["entry_timing"] == "night_session_open"
    assert engine.state["last_intraday_decision_session"] == "2026-08-13:night"
    calibrations = [
        json.loads(line)
        for line in (tmp_path / "calibrations.jsonl").read_text().splitlines()
    ]
    assert {row["trading_date"] for row in calibrations} == {"2026-08-13"}
    assert {row["session"] for row in calibrations} == {"night"}
    engine.close()


def test_preopen_simtrade_books_are_recordable_but_never_executable(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    observed_at = datetime(2026, 8, 13, 8, 35, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(engine, observed_at=observed_at, receive_ns=receive_ns)
    for code, row in tuple(engine.latest_books.items()):
        engine.latest_books[code] = {**row, "simtrade": True}

    engine.step(now=observed_at)

    assert engine.state["active_cycle"] is None
    assert engine.state["engine_status"] == "waiting_for_intraday_entry_window"
    assert not (tmp_path / "ideal_ledger.jsonl").exists()
    engine.close()


def test_broker_order_fail_closed_state_survives_restart(tmp_path: Path) -> None:
    disabled = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        broker_orders_enabled=False,
    )
    assert disabled.state["broker_orders_enabled"] is False
    disabled.close()

    restarted = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        broker_orders_enabled=True,
    )
    assert restarted.broker_orders_enabled is False
    assert restarted.state["broker_orders_enabled"] is False
    status = json.loads((tmp_path / "status.json").read_text())
    assert status["broker_orders_enabled"] is False
    restarted.close()


def test_night_and_day_flatten_are_independent_for_same_trading_date(
    tmp_path: Path,
) -> None:
    engine = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    engine.on_book(_book("MXFH6", bid=44_994, ask=45_006, receive_ns=receive_ns))
    for model_id in VOLATILITY_MODEL_IDS:
        strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
        engine.state["strategies"][strategy_id]["futures_position"] = 1

    engine.step(now=datetime(2026, 8, 12, 4, 56, tzinfo=TAIPEI))
    assert engine.state["last_intraday_flatten_date"] == "2026-08-12"
    assert engine.state["last_intraday_flatten_session"] == "2026-08-12:night"
    assert engine.state["engine_status"] == "intraday_flat_for_night_close"

    for model_id in VOLATILITY_MODEL_IDS:
        strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
        engine.state["strategies"][strategy_id]["futures_position"] = 1
    engine.step(now=datetime(2026, 8, 12, 13, 36, tzinfo=TAIPEI))
    assert engine.state["last_intraday_flatten_session"] == "2026-08-12:day"
    assert engine.state["engine_status"] == "intraday_flat_for_day_close"

    reasons = {
        json.loads(line)["reason"]
        for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()
    }
    assert "intraday_futures_flatten_before_night_session_close" in reasons
    assert "intraday_futures_flatten_before_day_session_close" in reasons
    engine.close()


def test_flat_futures_deal_callback_clears_matching_inflight_order(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    engine.state["inflight_orders"]["V00001"] = {
        "trade_id": "broker-trade-1",
        "code": engine.hedge.code,
        "strategy_id": f"{MODEL_VARIANT_PREFIX}{VOLATILITY_MODEL_IDS[0]}",
    }

    engine.on_order_event(
        "OrderState.FuturesDeal",
        {
            "trade_id": "broker-trade-1",
            "code": engine.hedge.code,
            "action": "Sell",
            "price": 45_000.0,
            "quantity": 1,
        },
    )
    engine._drain_callbacks()

    assert engine.state["inflight_orders"] == {}
    engine.close()


def test_intraday_one_lot_uses_tmf_granularity_and_user_fee(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class FixedDeltaFit:
        def straddle_delta(self, **_kwargs) -> float:
            return 0.2

        def diagnostics(self) -> dict[str, object]:
            return {"test_fixed_delta": 0.2}

    fit_calls = 0

    def fixed_delta_fit(*_args, **_kwargs) -> FixedDeltaFit:
        nonlocal fit_calls
        fit_calls += 1
        if fit_calls == 1:
            # A callback arriving during model fitting must not replace the
            # causally frozen execution quote for this decision bucket.
            engine.on_book(
                _book(
                    engine.hedge.code,
                    bid=44_000,
                    ask=44_001,
                    receive_ns=receive_ns + 60_000_000_000,
                )
            )
        return FixedDeltaFit()

    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.fit_volatility_model",
        fixed_delta_fit,
    )
    engine = _engine(
        tmp_path / "tmf",
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        hedge_logical_code="TMFR1",
    )
    observed_at = datetime(2026, 8, 12, 10, 0, tzinfo=TAIPEI)
    receive_ns = int(observed_at.timestamp() * 1e9)
    monkeypatch.setattr(
        "stockagent.live.taifex_volatility_simulation.time.time_ns",
        lambda: receive_ns + 1_000_000,
    )
    _seed_surface_books(
        engine,
        observed_at=observed_at,
        receive_ns=receive_ns,
    )

    engine.step(now=observed_at)
    rows = [
        json.loads(line)
        for line in (tmp_path / "tmf" / "ideal_ledger.jsonl").read_text().splitlines()
    ]
    futures = [row for row in rows if row["instrument_type"] == "future"]
    expected_dynamic = {
        strategy_id
        for strategy_id in DYNAMIC_HEDGE_STRATEGY_IDS
        if STRATEGY_SPEC_BY_ID[strategy_id].hedge_policy
        not in {"fixed_future", "fixed_index_equivalent"}
    }
    dynamic_futures = [row for row in futures if row["strategy_id"] in expected_dynamic]
    assert {row["strategy_id"] for row in dynamic_futures} <= expected_dynamic
    assert {
        f"{MODEL_VARIANT_PREFIX}{model_id}" for model_id in VOLATILITY_MODEL_IDS
    } <= {row["strategy_id"] for row in dynamic_futures}
    assert len(dynamic_futures) == len({row["strategy_id"] for row in dynamic_futures})
    assert {row["book_receive_ts_ns"] for row in dynamic_futures} == {receive_ns}
    assert {row["product"] for row in dynamic_futures} == {"TMF"}
    assert {row["multiplier_twd_per_point"] for row in dynamic_futures} == {10.0}
    assert {row["fixed_fee_twd"] for row in dynamic_futures} == {16.0}
    assert any(
        engine.state["strategies"][f"{MODEL_VARIANT_PREFIX}{model_id}"][
            "futures_position"
        ]
        != 0
        for model_id in VOLATILITY_MODEL_IDS
    )
    engine.close()

    mtx_engine = _engine(
        tmp_path / "mtx",
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        hedge_logical_code="MXFR1",
    )
    _seed_surface_books(
        mtx_engine,
        observed_at=observed_at,
        receive_ns=receive_ns,
    )
    mtx_engine.step(now=observed_at)
    mtx_rows = [
        json.loads(line)
        for line in (tmp_path / "mtx" / "ideal_ledger.jsonl").read_text().splitlines()
    ]
    assert not [
        row
        for row in mtx_rows
        if row["instrument_type"] == "future" and row["strategy_id"] in expected_dynamic
    ]
    mtx_engine.close()


def test_flat_legacy_state_migrates_to_intraday_execution_contract(
    tmp_path: Path,
) -> None:
    legacy = _engine(tmp_path, bootstrap_after=date(2026, 8, 12))
    legacy.close()
    state_path = tmp_path / "state.json"
    payload = json.loads(state_path.read_text())
    for key in (
        "execution_contract_version",
        "strategy_mode",
        "last_intraday_decision_bucket",
        "last_intraday_flatten_date",
        "last_intraday_decision_session",
        "last_intraday_flatten_session",
    ):
        payload.pop(key, None)
    state_path.write_text(json.dumps(payload) + "\n")

    migrated = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    assert migrated.state["execution_contract_version"] == EXECUTION_CONTRACT_VERSION
    assert migrated.state["strategy_mode"] == STRATEGY_MODE_INTRADAY_FUTURES
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text().splitlines()
    ]
    assert events[-1]["event"] == "execution_contract_migrated"
    assert events[-1]["reason"] == "flat_state_safe_migration"
    migrated.close()


def test_expired_cycle_uses_official_settlement_and_finishes_flat(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    engine.on_book(_book("TXFH6", bid=44_995, ask=45_005, receive_ns=receive_ns))
    engine.on_book(
        _book("TXV-20260814-45000-C", bid=149, ask=151, receive_ns=receive_ns)
    )
    engine.on_book(
        _book("TXV-20260814-45000-P", bid=139, ask=141, receive_ns=receive_ns)
    )
    engine.step(now=datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI))
    engine.state["active_cycle"]["expiry_date"] = "2026-08-12"
    engine.state["active_cycle"]["status"] = "waiting_official_final_settlement"
    engine._persist_state()
    pl.DataFrame(
        {
            "settlement_date": [date(2026, 8, 12)],
            "option_series": ["202608W2"],
            "final_settlement_price": [45_100.0],
            "source_file": ["official.html"],
            "source_sha256": ["a" * 64],
        }
    ).write_parquet(tmp_path / "settlements.parquet")
    engine.latest_books.clear()
    engine.step(now=datetime(2026, 8, 13, 8, 46, tzinfo=TAIPEI))
    assert engine.state["active_cycle"] is None
    assert engine.state["last_settled_expiry"] == "2026-08-12"
    assert engine.state["engine_status"] in {
        "flat_ready_for_next_cycle",
        "waiting_for_fresh_open_books",
    }
    for strategy_id in STRATEGY_IDS:
        assert engine.state["strategies"][strategy_id]["futures_position"] == 0
    engine.close()


def test_startup_settlement_bootstrap_migrates_v7_then_rolls_to_nearest_weekly(
    tmp_path: Path,
) -> None:
    initial = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    initial.state["active_cycle"] = {
        "cycle_id": "expired_weekly_cycle",
        "expiry_date": "2026-08-14",
        "series": "TXV:202608:2026-08-14",
    }
    ledger = initial.state["strategies"][CLASSIC_VARIANT_ID]
    ledger["option_positions"] = {"TXV-20260814-45000-C": 1}
    ledger["option_position_metadata"] = {
        "TXV-20260814-45000-C": {
            "series": "TXV:202608:2026-08-14",
            "strike": 45_000.0,
            "option_right": "C",
        }
    }
    initial.close()

    state_path = tmp_path / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["execution_contract_version"] = 7
    state["strategy_ids"] = [
        strategy_id
        for strategy_id in state["strategy_ids"]
        if strategy_id != PUT_CALL_PARITY_TX_STRATEGY_ID
    ]
    state["strategies"].pop(PUT_CALL_PARITY_TX_STRATEGY_ID)
    state.pop("put_call_parity_tx", None)
    state_path.write_text(json.dumps(state), encoding="utf-8")
    pl.DataFrame(
        {
            "settlement_date": [date(2026, 8, 14)],
            "option_series": ["202608F2"],
            "final_settlement_price": [45_100.0],
            "source_file": ["official.html"],
            "source_sha256": ["c" * 64],
        }
    ).write_parquet(tmp_path / "settlements.parquet")

    bootstrap = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        option_infos_override=[],
        settlement_bootstrap_only=True,
        startup_now=datetime(2026, 8, 14, 15, 1, tzinfo=TAIPEI),
    )
    assert bootstrap.state["execution_contract_version"] == EXECUTION_CONTRACT_VERSION
    assert bootstrap.state["active_cycle"] is None
    assert bootstrap.state["last_settled_expiry"] == "2026-08-14"
    assert all(
        not ledger["option_positions"]
        for ledger in bootstrap.state["strategies"].values()
    )
    bootstrap.close()

    next_options = [
        FakeOptionInfo(
            root=root,
            expiry=expiry,
            strike=strike,
            right=right,
        )
        for root, expiry in (
            ("TXO", date(2026, 8, 19)),
            ("TXV", date(2026, 8, 21)),
        )
        for strike in range(44_000, 46_001, 100)
        for right in ("C", "P")
    ]
    live = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 12),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
        option_infos_override=next_options,
    )
    observed_at = datetime(2026, 8, 14, 15, 1, tzinfo=TAIPEI)
    receive_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)
    _seed_surface_books(live, observed_at=observed_at, receive_ns=receive_ns)
    live.step(now=observed_at)

    assert live.state["active_cycle"]["expiry_date"] == "2026-08-21"
    assert live.state["active_cycle"]["entry_session"] == "night"
    assert live.state["active_cycle"]["call_code"].startswith("TXV-20260821-")
    events = [
        json.loads(line)
        for line in (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert any(
        row["event"] == "subscription_bootstrap_expired_positions_settled"
        for row in events
    )
    assert events[-1]["event"] == "cycle_opened"
    live.close()


def test_startup_settlement_bootstrap_fails_closed_without_official_price(
    tmp_path: Path,
) -> None:
    initial = _engine(
        tmp_path,
        bootstrap_after=date(2026, 8, 10),
        strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
    )
    initial.state["active_cycle"] = {
        "cycle_id": "expired_without_settlement",
        "expiry_date": "2026-08-12",
        "series": "TXV:202608:2026-08-12",
    }
    ledger = initial.state["strategies"][CLASSIC_VARIANT_ID]
    ledger["option_positions"] = {"TXV-EXPIRED-C": 1}
    ledger["option_position_metadata"] = {
        "TXV-EXPIRED-C": {
            "series": "TXV:202608:2026-08-12",
            "strike": 45_000.0,
            "option_right": "C",
        }
    }
    initial.close()

    with pytest.raises(RuntimeError, match="missing_official_final_settlement"):
        _engine(
            tmp_path,
            bootstrap_after=date(2026, 8, 10),
            strategy_mode=STRATEGY_MODE_INTRADAY_FUTURES,
            option_infos_override=[],
            settlement_bootstrap_only=True,
            startup_now=datetime(2026, 8, 13, 8, 46, tzinfo=TAIPEI),
        )
    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["active_cycle"] is not None
    assert state["engine_status"] == "blocked_subscription_bootstrap_settlement"
    assert state["blocked_reason"] == "missing_official_final_settlement:2026-08-12"
