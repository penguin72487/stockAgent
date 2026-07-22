from __future__ import annotations

import argparse
from types import SimpleNamespace

import pytest

from scripts.test_shioaji_simulation_lifecycle import run_simulation_lifecycle


def _trade(contract, account):
    return SimpleNamespace(
        contract=contract,
        order=SimpleNamespace(ordno="SIM001", account=account),
        status=SimpleNamespace(
            id="SIM-TRADE-1",
            status=SimpleNamespace(value="Submitted"),
            status_code="00",
            msg="",
            order_quantity=2,
            deal_quantity=0,
            cancel_quantity=0,
        ),
    )


def test_complete_simulation_lifecycle_never_constructs_production_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("SHIOAJI_TRADE_API_KEY", "api")
    monkeypatch.setenv("SHIOAJI_TRADE_SECRET_KEY", "secret")
    monkeypatch.setenv("SHIOAJI_TRADE_BROKER_ID", "9A00")
    monkeypatch.setenv("SHIOAJI_TRADE_ACCOUNT_ID", "1234567")
    account = SimpleNamespace(
        account_type=SimpleNamespace(value="S"),
        broker_id="9A00",
        account_id="1234567",
    )
    contract = SimpleNamespace(
        code="2002",
        name="中鋼",
        exchange=SimpleNamespace(value="TSE"),
        reference=20.0,
        limit_down=18.0,
    )

    class FakeApi:
        def __init__(self) -> None:
            self.Contracts = SimpleNamespace(Stocks={"2002": contract})
            self.calls = []
            self.trade = _trade(contract, account)

        def set_event_callback(self, callback):
            self.event_callback = callback

        def set_order_callback(self, callback):
            self.callback = callback

        def login(self, **kwargs):
            self.calls.append("login")
            return [account]

        def set_default_account(self, selected):
            assert selected is account

        def place_order(self, selected_contract, order):
            assert selected_contract is contract
            self.calls.append("place")
            self.callback("StockOrder", {"sensitive": "not persisted"})
            return self.trade

        def update_status(self, account_arg=None, *, trade=None):
            if trade is not None:
                assert trade is self.trade
            else:
                assert account_arg is account
            self.calls.append("status")

        def update_order(self, *, trade, qty):
            assert qty == 1
            self.calls.append("reduce")
            trade.status.order_quantity = 1
            trade.status.cancel_quantity = 1
            return trade

        def cancel_order(self, trade):
            self.calls.append("cancel")
            trade.status.status = SimpleNamespace(value="Cancelled")
            trade.status.cancel_quantity = 2
            return trade

        def list_trades(self):
            self.calls.append("list")
            return [self.trade]

        def logout(self):
            self.calls.append("logout")

    api = FakeApi()
    constructor_modes = []

    def constructor(*, simulation):
        constructor_modes.append(simulation)
        return api

    fake_sj = SimpleNamespace(
        __version__="test",
        Shioaji=constructor,
        Action=SimpleNamespace(Buy="Buy"),
        StockPriceType=SimpleNamespace(LMT="LMT"),
        OrderType=SimpleNamespace(ROD="ROD"),
        StockOrderLot=SimpleNamespace(Common="Common"),
        StockOrderCond=SimpleNamespace(Cash="Cash"),
        StockOrder=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    args = argparse.Namespace(
        execute_simulation=True,
        symbol="2002",
        quantity=2,
        receipt_dir=tmp_path,
    )
    summary, receipt = run_simulation_lifecycle(
        args,
        shioaji_module=fake_sj,
        sleeper=lambda _: None,
        enforce_test_window=False,
    )

    assert constructor_modes == [True]
    assert summary["simulation"] is True
    assert summary["production_order_possible"] is False
    assert summary["result"] == "ok"
    contract_step = next(step for step in summary["steps"] if step["step"] == "contract")
    assert contract_step["test_price"] == 20.0
    assert contract_step["test_price_source"] == "reference"
    assert receipt.exists()
    assert api.calls == [
        "login",
        "place",
        "status",
        "reduce",
        "status",
        "cancel",
        "status",
        "list",
        "list",
        "logout",
    ]


def test_simulation_acknowledgement_is_required(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    args = argparse.Namespace(
        execute_simulation=False,
        symbol="2002",
        quantity=2,
        receipt_dir=tmp_path,
    )
    with pytest.raises(RuntimeError, match="execute-simulation"):
        run_simulation_lifecycle(
            args,
            shioaji_module=SimpleNamespace(),
            enforce_test_window=False,
        )
