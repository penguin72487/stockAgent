from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.live.taifex_volatility_simulation import (
    FuturesInstrument,
    STRATEGY_IDS,
    TaifexVolatilitySimulation,
)


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


def _engine(tmp_path: Path, *, bootstrap_after: date) -> TaifexVolatilitySimulation:
    expiry = date(2026, 8, 14)
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
    return TaifexVolatilitySimulation(
        api=FakeApi(),
        shioaji_module=SimpleNamespace(),
        state_dir=tmp_path,
        option_infos=option_infos,
        underlying=FuturesInstrument(
            logical_code="TXFR1", code="TXFH6", contract=FakeBase("TXFH6")
        ),
        hedge=FuturesInstrument(
            logical_code="MXFR1", code="MXFH6", contract=FakeBase("MXFH6")
        ),
        final_settlement_path=tmp_path / "settlements.parquet",
        calibration_time=time(13, 29),
        bootstrap_after=bootstrap_after,
        broker_orders_enabled=False,
    )


def _book(code: str, *, bid: float, ask: float, receive_ns: int) -> dict[str, object]:
    return {
        "code": code,
        "receive_ts_ns": receive_ns,
        "simtrade": False,
        "bid_price_1": bid,
        "ask_price_1": ask,
        "bid_volume_1": 1,
        "ask_volume_1": 1,
    }


def test_engine_opens_one_quote_executable_straddle_for_all_seven_ledgers(
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
        assert strategy["gross_cash_twd"] == -(151 + 141) * 50
        assert strategy["fees_twd"] == 44
        assert strategy["trade_sides"] == 2
    rows = [line for line in (tmp_path / "ideal_ledger.jsonl").read_text().splitlines()]
    assert len(rows) == len(STRATEGY_IDS) * 2
    engine.close()


def test_engine_does_not_open_from_stale_books(tmp_path: Path) -> None:
    engine = _engine(tmp_path, bootstrap_after=date(2026, 8, 10))
    stale_ns = int(datetime.now(timezone.utc).timestamp() * 1e9) - 10_000_000_000
    engine.on_book(_book("TXFH6", bid=44_995, ask=45_005, receive_ns=stale_ns))
    engine.step(now=datetime(2026, 8, 12, 8, 46, tzinfo=TAIPEI))
    assert engine.state["active_cycle"] is None
    assert engine.state["engine_status"] == "waiting_for_fresh_open_books"
    engine.close()


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
