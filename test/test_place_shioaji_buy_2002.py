from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Event
from types import SimpleNamespace

import pytest

from scripts.place_shioaji_buy_2002 import (
    BuyRequest,
    LIVE_CONFIRMATION,
    LIVE_ENV_ACK,
    _public_request,
    _find_stock_account,
    _validate_live_guards,
    _wait_for_acknowledgement,
    submit_live,
)


def test_common_lot_preview_expands_to_shares_and_notional() -> None:
    request = BuyRequest(
        symbol="2002", price=Decimal("20.50"), quantity=2, lot="common"
    )
    output = _public_request(request)
    assert output["estimated_shares"] == 2_000
    assert output["estimated_notional_ntd_excluding_fees"] == "41000.00"
    assert output["action"] == "Buy"
    assert output["price_type"] == "LMT"
    assert output["order_cond"] == "Cash"


def test_intraday_odd_quantity_is_shares() -> None:
    request = BuyRequest(
        symbol="2002", price=Decimal("20.5"), quantity=35, lot="intraday-odd"
    )
    assert request.estimated_shares == 35
    assert request.estimated_notional_ntd == Decimal("717.5")


def test_live_guard_rejects_notional_above_configured_limit(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    certificate = tmp_path / "Sinopac.pfx"
    certificate.write_bytes(b"test certificate placeholder")
    values = {
        "SHIOAJI_TRADE_LIVE_ACK": LIVE_ENV_ACK,
        "SHIOAJI_TRADE_MAX_NOTIONAL_NTD": "20000",
        "SHIOAJI_TRADE_CA_PATH": str(certificate),
        "SHIOAJI_TRADE_API_KEY": "api",
        "SHIOAJI_TRADE_SECRET_KEY": "secret",
        "SHIOAJI_TRADE_CA_PASSWORD": "password",
        "SHIOAJI_TRADE_PERSON_ID": "person",
        "SHIOAJI_TRADE_BROKER_ID": "broker",
        "SHIOAJI_TRADE_ACCOUNT_ID": "account",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    args = argparse.Namespace(confirm_live_order=LIVE_CONFIRMATION)
    request = BuyRequest(
        symbol="2002", price=Decimal("20.5"), quantity=1, lot="common"
    )
    with pytest.raises(RuntimeError, match="exceeds configured maximum"):
        _validate_live_guards(args, request)


def test_live_guard_requires_exact_command_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SHIOAJI_TRADE_LIVE_ACK", LIVE_ENV_ACK)
    args = argparse.Namespace(confirm_live_order="wrong")
    request = BuyRequest(
        symbol="2002", price=Decimal("20.5"), quantity=1, lot="common"
    )
    with pytest.raises(RuntimeError, match="confirm-live-order"):
        _validate_live_guards(args, request)


def test_live_path_places_exactly_one_guarded_cash_limit_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    certificate = tmp_path / "Sinopac.pfx"
    certificate.write_bytes(b"test certificate placeholder")
    values = {
        "SHIOAJI_TRADE_LIVE_ACK": LIVE_ENV_ACK,
        "SHIOAJI_TRADE_MAX_NOTIONAL_NTD": "50000",
        "SHIOAJI_TRADE_CA_PATH": str(certificate),
        "SHIOAJI_TRADE_API_KEY": "api",
        "SHIOAJI_TRADE_SECRET_KEY": "secret",
        "SHIOAJI_TRADE_CA_PASSWORD": "password",
        "SHIOAJI_TRADE_PERSON_ID": "person",
        "SHIOAJI_TRADE_BROKER_ID": "9A00",
        "SHIOAJI_TRADE_ACCOUNT_ID": "1234567",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    account = SimpleNamespace(
        broker_id="9A00",
        account_id="1234567",
        account_type=SimpleNamespace(value="S"),
        signed=True,
    )
    contract = SimpleNamespace(
        code="2002",
        exchange=SimpleNamespace(value="TSE"),
    )
    contract_info = SimpleNamespace(
        name="中鋼",
        reference=20.0,
        limit_up=22.0,
        limit_down=18.0,
    )
    trade = SimpleNamespace(
        status=SimpleNamespace(
            status=SimpleNamespace(value="Submitted"),
            status_code="00",
            msg="委託成功",
        )
    )

    class FakeApi:
        def __init__(self) -> None:
            self.contracts = SimpleNamespace(
                get=lambda code: contract if code == "2002" else None,
                info=lambda selected: contract_info if selected is contract else None,
            )
            self.orders = []
            self.default_account = None
            self.logged_out = False

        def set_event_callback(self, callback) -> None:
            self.event_callback = callback

        def set_order_callback(self, callback) -> None:
            self.order_callback = callback

        def login(self, **kwargs):
            assert kwargs["subscribe_trade"] is True
            return [account]

        def set_default_account(self, selected) -> None:
            self.default_account = selected

        def activate_ca(self, **kwargs) -> bool:
            return True

        def get_ca_expiretime(self, **kwargs):
            return datetime.now() + timedelta(days=30)

        def place_order(self, selected_contract, order):
            self.orders.append((selected_contract, order))
            return trade

        def logout(self) -> None:
            self.logged_out = True

    api = FakeApi()
    fake_sj = SimpleNamespace(
        Shioaji=lambda simulation: api,
        Action=SimpleNamespace(Buy="Buy"),
        StockPriceType=SimpleNamespace(LMT="LMT"),
        OrderType=SimpleNamespace(ROD="ROD"),
        StockOrderLot=SimpleNamespace(Common="Common", IntradayOdd="IntradayOdd"),
        StockOrderCond=SimpleNamespace(Cash="Cash"),
        StockOrder=lambda **kwargs: SimpleNamespace(**kwargs),
    )
    args = argparse.Namespace(
        confirm_live_order=LIVE_CONFIRMATION,
        receipt_dir=tmp_path / "receipts",
    )
    request = BuyRequest(
        symbol="2002", price=Decimal("20.5"), quantity=1, lot="common"
    )
    status, receipt = submit_live(args, request, shioaji_module=fake_sj)

    assert status == "Submitted"
    assert receipt.exists()
    assert api.default_account is account
    assert api.logged_out is True
    assert len(api.orders) == 1
    selected_contract, order = api.orders[0]
    assert selected_contract is contract
    assert order.action == "Buy"
    assert order.price == 20.5
    assert order.quantity == 1
    assert order.price_type == "LMT"
    assert order.order_type == "ROD"
    assert order.order_lot == "Common"
    assert order.order_cond == "Cash"


def test_pending_submit_is_reconciled_before_success() -> None:
    trade = SimpleNamespace(
        order=SimpleNamespace(id="trade-1"),
        status=SimpleNamespace(
            id="trade-1",
            status=SimpleNamespace(value="PendingSubmit"),
        ),
    )

    class FakeApi:
        def update_status(self, *, trade) -> None:
            trade.status.status.value = "Submitted"

        def list_trades(self):
            return [trade]

    reconciled, status = _wait_for_acknowledgement(
        FakeApi(), trade, Event(), attempts=1
    )
    assert reconciled is trade
    assert status == "Submitted"


def test_pending_submit_is_not_reported_as_success_when_unresolved() -> None:
    trade = SimpleNamespace(
        order=SimpleNamespace(id="trade-1"),
        status=SimpleNamespace(
            id="trade-1",
            status=SimpleNamespace(value="PendingSubmit"),
        ),
    )

    class FakeApi:
        def update_status(self, *, trade) -> None:
            pass

        def list_trades(self):
            return [trade]

    _, status = _wait_for_acknowledgement(FakeApi(), trade, Event(), attempts=1)
    assert status == "PendingSubmit"


def test_unsigned_stock_account_is_rejected_before_order_submission() -> None:
    account = SimpleNamespace(
        broker_id="9A00",
        account_id="1234567",
        account_type=SimpleNamespace(value="S"),
        signed=False,
    )
    with pytest.raises(RuntimeError, match="signed=False"):
        _find_stock_account([account], "9A00", "1234567")
