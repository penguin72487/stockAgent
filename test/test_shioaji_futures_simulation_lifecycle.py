from __future__ import annotations

import argparse
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from scripts.test_shioaji_futures_simulation_lifecycle import (
    _session_open,
    run_futures_simulation_lifecycle,
)
from stockagent.data.taifex_sessions import (
    next_taifex_capture_window,
    taifex_market_phase,
    taifex_session_kind,
    taifex_trading_date,
)


TAIPEI = ZoneInfo("Asia/Taipei")


def test_day_and_night_session_boundaries_and_trading_dates() -> None:
    assert _session_open(datetime(2026, 8, 11, 8, 45, tzinfo=TAIPEI))
    assert _session_open(datetime(2026, 8, 11, 15, 0, tzinfo=TAIPEI))
    assert _session_open(datetime(2026, 8, 12, 4, 59, tzinfo=TAIPEI))
    assert not _session_open(datetime(2026, 8, 12, 5, 0, tzinfo=TAIPEI))
    assert not _session_open(datetime(2026, 8, 17, 2, 0, tzinfo=TAIPEI))
    assert (
        taifex_trading_date(datetime(2026, 8, 14, 15, 0, tzinfo=TAIPEI)).isoformat()
        == "2026-08-17"
    )
    assert (
        taifex_session_kind(
            datetime(2026, 8, 11, 8, 30, tzinfo=TAIPEI),
            include_preopen=True,
        )
        == "day"
    )
    assert (
        taifex_market_phase(datetime(2026, 8, 11, 8, 30, tzinfo=TAIPEI))
        == "day_preopen"
    )
    assert (
        taifex_market_phase(datetime(2026, 8, 11, 13, 45, tzinfo=TAIPEI))
        == "day_close_to_night_preopen"
    )
    assert (
        taifex_market_phase(datetime(2026, 8, 11, 14, 50, tzinfo=TAIPEI))
        == "night_preopen"
    )


def test_capture_windows_cover_day_night_and_cross_midnight() -> None:
    before_day = datetime(2026, 8, 11, 8, 0, tzinfo=TAIPEI)
    day = next_taifex_capture_window(before_day)
    assert day.session == "day"
    assert day.trading_date.isoformat() == "2026-08-11"
    assert day.starts_at.isoformat() == "2026-08-11T08:30:00+08:00"
    assert day.stops_at.isoformat() == "2026-08-11T13:45:05+08:00"

    evening = next_taifex_capture_window(datetime(2026, 8, 11, 16, 0, tzinfo=TAIPEI))
    assert evening.session == "night"
    assert evening.trading_date.isoformat() == "2026-08-12"
    assert evening.starts_at.isoformat() == "2026-08-11T14:50:00+08:00"
    assert evening.stops_at.isoformat() == "2026-08-12T05:00:05+08:00"

    after_midnight = next_taifex_capture_window(
        datetime(2026, 8, 12, 2, 0, tzinfo=TAIPEI)
    )
    assert after_midnight == evening

    friday_night = next_taifex_capture_window(
        datetime(2026, 8, 14, 16, 0, tzinfo=TAIPEI)
    )
    assert friday_night.session == "night"
    assert friday_night.trading_date.isoformat() == "2026-08-17"


def test_futures_round_trip_is_simulation_market_ioc_and_returns_to_baseline(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("SHIOAJI_API_KEY", "api")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "secret")
    account = SimpleNamespace(
        account_type=SimpleNamespace(value="F"),
        broker_id="F002000",
        account_id="1234567",
    )
    logical = SimpleNamespace(code="TXFR1", target_code="TXFH6")
    concrete = SimpleNamespace(code="TXFH6", target_code=None)

    class Contracts:
        @staticmethod
        def get(code):
            return {"TXFR1": logical, "TXFH6": concrete}.get(code)

    class FakeApi:
        def __init__(self) -> None:
            self.contracts = Contracts()
            self.orders = []

        def set_event_callback(self, callback):
            self.event_callback = callback

        def set_order_callback(self, callback):
            self.order_callback = callback

        def login(self, **_kwargs):
            return [account]

        def set_default_account(self, selected):
            assert selected is account

        def list_positions(self, **_kwargs):
            return []

        def snapshots(self, contracts):
            assert contracts == [concrete]
            return [
                SimpleNamespace(
                    buy_price=20_000,
                    sell_price=20_001,
                    buy_volume=1,
                    sell_volume=1,
                    ts=1,
                )
            ]

        def place_order(self, selected, order):
            assert selected is concrete
            self.orders.append(order)
            sequence = len(self.orders)
            trade = SimpleNamespace(
                order=SimpleNamespace(id=f"SIM{sequence}", ordno=f"N{sequence}"),
                status=SimpleNamespace(
                    id=f"SIM{sequence}",
                    status=SimpleNamespace(value="Filled"),
                    status_code="00",
                    msg="",
                    order_quantity=1,
                    deal_quantity=1,
                    cancel_quantity=0,
                    deals=[SimpleNamespace(price=20_000.5, quantity=1, seq=sequence)],
                ),
            )
            self.order_callback("FuturesDeal", {"trade_id": f"SIM{sequence}"})
            return trade

        def update_status(self, **_kwargs):
            return None

        def logout(self):
            return None

    api = FakeApi()
    constructor_modes = []

    def constructor(*, simulation):
        constructor_modes.append(simulation)
        return api

    fake_sj = SimpleNamespace(
        __version__="test",
        Shioaji=constructor,
        Action=SimpleNamespace(Buy="Buy", Sell="Sell"),
        FuturesPriceType=SimpleNamespace(MKT="MKT", LMT="LMT"),
        OrderType=SimpleNamespace(IOC="IOC"),
        FuturesOCType=SimpleNamespace(Auto="Auto"),
        FuturesOrder=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    args = argparse.Namespace(
        execute_simulation=True,
        contract="TXFR1",
        quantity=1,
        receipt_dir=tmp_path,
    )
    summary, receipt = run_futures_simulation_lifecycle(
        args,
        shioaji_module=fake_sj,
        sleeper=lambda _seconds: None,
        enforce_session=False,
    )
    assert constructor_modes == [True]
    assert summary["result"] == "ok"
    assert summary["baseline_position"] == 0
    assert summary["final_position"] == 0
    assert receipt.exists()
    assert [order.action for order in api.orders] == ["Buy", "Sell"]
    assert all(order.price == 0 for order in api.orders)
    assert all(order.price_type == "MKT" for order in api.orders)
    assert all(order.order_type == "IOC" for order in api.orders)


def test_futures_simulation_acknowledgement_is_required(tmp_path) -> None:
    args = argparse.Namespace(
        execute_simulation=False,
        contract="TXFR1",
        quantity=1,
        receipt_dir=tmp_path,
    )
    with pytest.raises(RuntimeError, match="execute-simulation"):
        run_futures_simulation_lifecycle(
            args,
            shioaji_module=SimpleNamespace(),
            enforce_session=False,
        )


def test_restore_mode_uses_broker_position_truth_once(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SHIOAJI_API_KEY", "api")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "secret")
    account = SimpleNamespace(
        account_type=SimpleNamespace(value="F"),
        broker_id="F002000",
        account_id="1234567",
    )
    logical = SimpleNamespace(code="TXFR1", target_code="TXFH6")
    concrete = SimpleNamespace(code="TXFH6", target_code=None)

    class Contracts:
        @staticmethod
        def get(code):
            return {"TXFR1": logical, "TXFH6": concrete}.get(code)

    class FakeApi:
        def __init__(self) -> None:
            self.contracts = Contracts()
            self.position = -1
            self.orders = []

        def set_event_callback(self, callback):
            self.event_callback = callback

        def set_order_callback(self, callback):
            self.order_callback = callback

        def login(self, **_kwargs):
            return [account]

        def set_default_account(self, selected):
            assert selected is account

        def update_status(self, *_args, **_kwargs):
            return None

        def list_positions(self, **_kwargs):
            if self.position == 0:
                return []
            return [
                SimpleNamespace(
                    code="TXFH6",
                    quantity=abs(self.position),
                    direction=SimpleNamespace(
                        value="Buy" if self.position > 0 else "Sell"
                    ),
                )
            ]

        def snapshots(self, contracts):
            assert contracts == [concrete]
            return [
                SimpleNamespace(
                    buy_price=20_000,
                    sell_price=20_001,
                    buy_volume=1,
                    sell_volume=1,
                    ts=1,
                )
            ]

        def place_order(self, selected, order):
            assert selected is concrete
            self.orders.append(order)
            self.position += 1 if order.action == "Buy" else -1
            return SimpleNamespace(
                order=SimpleNamespace(id="SIM-R", ordno="NR"),
                status=SimpleNamespace(
                    id="SIM-R",
                    status=SimpleNamespace(value="Filled"),
                    status_code="00",
                    msg="",
                    order_quantity=1,
                    deal_quantity=1,
                    cancel_quantity=0,
                    deals=[],
                ),
            )

        def logout(self):
            return None

    api = FakeApi()
    fake_sj = SimpleNamespace(
        Shioaji=lambda *, simulation: api,
        Action=SimpleNamespace(Buy="Buy", Sell="Sell"),
        FuturesPriceType=SimpleNamespace(MKT="MKT"),
        OrderType=SimpleNamespace(IOC="IOC"),
        FuturesOCType=SimpleNamespace(Auto="Auto"),
        FuturesOrder=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    args = argparse.Namespace(
        execute_simulation=True,
        contract="TXFR1",
        quantity=1,
        restore_position=0,
        receipt_dir=tmp_path,
    )
    summary, receipt = run_futures_simulation_lifecycle(
        args,
        shioaji_module=fake_sj,
        sleeper=lambda _seconds: None,
        enforce_session=False,
    )
    assert summary["result"] == "ok"
    assert summary["baseline_position"] == -1
    assert summary["final_position"] == 0
    assert len(api.orders) == 1
    assert api.orders[0].action == "Buy"
    assert receipt.exists()
