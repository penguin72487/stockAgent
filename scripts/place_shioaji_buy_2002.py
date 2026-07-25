from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
from threading import Event
from typing import Any


SYMBOL = "2002"
LIVE_CONFIRMATION = "BUY-2002"
LIVE_ENV_ACK = "I_UNDERSTAND_THIS_SENDS_REAL_ORDERS"
ACKNOWLEDGED_STATUSES = {"PreSubmitted", "Submitted", "PartFilled", "Filled"}
REJECTED_STATUSES = {"Failed", "Cancelled"}


@dataclass(frozen=True)
class BuyRequest:
    symbol: str
    price: Decimal
    quantity: int
    lot: str

    @property
    def estimated_shares(self) -> int:
        return self.quantity * 1_000 if self.lot == "common" else self.quantity

    @property
    def estimated_notional_ntd(self) -> Decimal:
        return self.price * self.estimated_shares


def _positive_decimal(raw: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"invalid decimal: {raw}") from exc
    if not value.is_finite() or value <= 0:
        raise argparse.ArgumentTypeError("price must be a positive finite number")
    return value


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {raw}") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("quantity must be positive")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or explicitly submit one production Shioaji cash limit buy "
            "order for 2002 China Steel. Preview is the default."
        )
    )
    parser.add_argument("--price", required=True, type=_positive_decimal)
    parser.add_argument("--quantity", required=True, type=_positive_int)
    parser.add_argument(
        "--lot",
        required=True,
        choices=("common", "intraday-odd"),
        help="common quantity is lots (1 lot=1000 shares); intraday-odd is shares",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="enable the production login and order path",
    )
    parser.add_argument(
        "--confirm-live-order",
        default="",
        help=f"live mode requires the exact value {LIVE_CONFIRMATION}",
    )
    parser.add_argument(
        "--receipt-dir", type=Path, default=Path("artifacts/orders/shioaji")
    )
    return parser.parse_args()


def _request(args: argparse.Namespace) -> BuyRequest:
    return BuyRequest(
        symbol=SYMBOL,
        price=args.price,
        quantity=args.quantity,
        lot=args.lot,
    )


def _public_request(request: BuyRequest) -> dict[str, Any]:
    payload = asdict(request)
    payload["price"] = str(request.price)
    payload.update(
        {
            "action": "Buy",
            "price_type": "LMT",
            "order_type": "ROD",
            "order_cond": "Cash",
            "estimated_shares": request.estimated_shares,
            "estimated_notional_ntd_excluding_fees": str(
                request.estimated_notional_ntd
            ),
        }
    )
    return payload


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _redact(value: str) -> str:
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value or ""))


def _find_stock_account(accounts: list[Any], broker_id: str, account_id: str) -> Any:
    matches = [
        account
        for account in accounts
        if str(getattr(account, "broker_id", "")) == broker_id
        and str(getattr(account, "account_id", "")) == account_id
        and str(getattr(getattr(account, "account_type", None), "value", "")) == "S"
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "configured stock account did not match exactly one logged-in account"
        )
    account = matches[0]
    if getattr(account, "signed", False) is not True:
        raise RuntimeError(
            "configured stock account has signed=False; complete the SinoPac API "
            "agreement and simulation test before production ordering"
        )
    return account


def _validate_live_guards(args: argparse.Namespace, request: BuyRequest) -> dict[str, str]:
    if args.confirm_live_order != LIVE_CONFIRMATION:
        raise RuntimeError(
            f"live order refused: pass --confirm-live-order {LIVE_CONFIRMATION}"
        )
    if _required_env("SHIOAJI_TRADE_LIVE_ACK") != LIVE_ENV_ACK:
        raise RuntimeError(
            "live order refused: SHIOAJI_TRADE_LIVE_ACK does not contain the "
            "required acknowledgement"
        )
    maximum = _positive_decimal(_required_env("SHIOAJI_TRADE_MAX_NOTIONAL_NTD"))
    if request.estimated_notional_ntd > maximum:
        raise RuntimeError(
            "live order refused: estimated notional "
            f"{request.estimated_notional_ntd} exceeds configured maximum {maximum}"
        )
    ca_path = Path(_required_env("SHIOAJI_TRADE_CA_PATH")).expanduser().resolve()
    if not ca_path.is_file():
        raise RuntimeError(f"CA certificate does not exist: {ca_path}")
    return {
        "api_key": _required_env("SHIOAJI_TRADE_API_KEY"),
        "secret_key": _required_env("SHIOAJI_TRADE_SECRET_KEY"),
        "ca_path": str(ca_path),
        "ca_password": _required_env("SHIOAJI_TRADE_CA_PASSWORD"),
        "person_id": _required_env("SHIOAJI_TRADE_PERSON_ID"),
        "broker_id": _required_env("SHIOAJI_TRADE_BROKER_ID"),
        "account_id": _required_env("SHIOAJI_TRADE_ACCOUNT_ID"),
    }


def _status_value(trade: Any) -> str:
    status = getattr(getattr(trade, "status", None), "status", None)
    return str(getattr(status, "value", status or "Unknown"))


def _trade_id(trade: Any) -> str:
    status_id = getattr(getattr(trade, "status", None), "id", "")
    order_id = getattr(getattr(trade, "order", None), "id", "")
    return str(status_id or order_id or "")


def _order_event_summary(state: Any, message: Any) -> dict[str, str]:
    operation = message.get("operation", {}) if isinstance(message, dict) else {}
    return {
        "state": str(state),
        "op_code": str(operation.get("op_code", "")),
        "op_msg": str(operation.get("op_msg", "")),
    }


def _wait_for_acknowledgement(
    api: Any,
    trade: Any,
    order_event: Event,
    *,
    attempts: int = 10,
) -> tuple[Any, str]:
    latest_trade = trade
    status = _status_value(latest_trade)
    for _ in range(attempts):
        if status in ACKNOWLEDGED_STATUSES | REJECTED_STATUSES:
            return latest_trade, status
        order_event.wait(timeout=1.0)
        order_event.clear()
        api.update_status(trade=latest_trade)
        target_id = _trade_id(latest_trade)
        if target_id:
            latest_trade = next(
                (
                    candidate
                    for candidate in api.list_trades()
                    if _trade_id(candidate) == target_id
                ),
                latest_trade,
            )
        status = _status_value(latest_trade)
    return latest_trade, status


def _write_receipt(
    receipt_dir: Path,
    *,
    request: BuyRequest,
    account: Any,
    contract: Any,
    contract_info: Any,
    trade: Any,
    ca_expire_time: Any,
    order_events: list[dict[str, str]],
) -> Path:
    now = datetime.now(timezone.utc)
    payload = {
        "schema_version": 1,
        "submitted_at_utc": now.isoformat(),
        "request": _public_request(request),
        "contract": {
            "code": str(getattr(contract, "code", "")),
            "name": str(getattr(contract_info, "name", "")),
            "exchange": _enum_value(getattr(contract, "exchange", None)),
            "reference": getattr(contract_info, "reference", None),
            "limit_up": getattr(contract_info, "limit_up", None),
            "limit_down": getattr(contract_info, "limit_down", None),
        },
        "account": {
            "broker_id": _redact(str(account.broker_id)),
            "account_id": _redact(str(account.account_id)),
            "signed": getattr(account, "signed", False) is True,
        },
        "ca_expire_time": ca_expire_time,
        "result": {
            "status": _status_value(trade),
            "status_code": str(getattr(getattr(trade, "status", None), "status_code", "")),
            "message": str(getattr(getattr(trade, "status", None), "msg", "")),
            "order_id": _trade_id(trade),
            "deal_quantity": int(
                getattr(getattr(trade, "status", None), "deal_quantity", 0) or 0
            ),
            "cancel_quantity": int(
                getattr(getattr(trade, "status", None), "cancel_quantity", 0) or 0
            ),
            "order_events": order_events,
        },
    }
    receipt_dir.mkdir(parents=True, exist_ok=True)
    path = receipt_dir / f"{now.strftime('%Y%m%dT%H%M%S.%fZ')}-buy-2002.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


def submit_live(
    args: argparse.Namespace,
    request: BuyRequest,
    *,
    shioaji_module: Any | None = None,
) -> tuple[str, Path]:
    credentials = _validate_live_guards(args, request)
    if shioaji_module is None:
        import shioaji as shioaji_module

    sj = shioaji_module
    api = sj.Shioaji(simulation=False)
    api.set_event_callback(lambda *_args: None)
    order_event = Event()
    order_events: list[dict[str, str]] = []

    def order_callback(state: Any, message: Any) -> None:
        order_events.append(_order_event_summary(state, message))
        order_event.set()

    try:
        accounts = api.login(
            api_key=credentials["api_key"],
            secret_key=credentials["secret_key"],
            subscribe_trade=True,
        )
        account = _find_stock_account(
            list(accounts), credentials["broker_id"], credentials["account_id"]
        )
        api.set_default_account(account)
        api.set_order_callback(order_callback)
        activated = api.activate_ca(
            ca_path=credentials["ca_path"],
            ca_passwd=credentials["ca_password"],
            person_id=credentials["person_id"],
        )
        if activated is not True:
            raise RuntimeError("CA activation failed; order was not submitted")
        ca_expire_time = api.get_ca_expiretime(person_id=credentials["person_id"])
        if ca_expire_time is None:
            raise RuntimeError("CA expiry lookup failed; order was not submitted")
        if hasattr(ca_expire_time, "tzinfo"):
            now = (
                datetime.now(ca_expire_time.tzinfo)
                if ca_expire_time.tzinfo is not None
                else datetime.now()
            )
            if ca_expire_time <= now:
                raise RuntimeError("CA certificate is expired; order was not submitted")

        contract = api.contracts.get(request.symbol)
        if contract is None or str(getattr(contract, "code", "")) != SYMBOL:
            raise RuntimeError("contract lookup did not return stock 2002")
        contract_info = api.contracts.info(contract)
        if contract_info is None:
            raise RuntimeError("contract info lookup did not return stock 2002")
        exchange = _enum_value(getattr(contract, "exchange", None))
        if exchange != "TSE":
            raise RuntimeError(f"unexpected exchange for 2002: {exchange}")
        lower = Decimal(str(getattr(contract_info, "limit_down", "0") or "0"))
        upper = Decimal(str(getattr(contract_info, "limit_up", "0") or "0"))
        if lower > 0 and upper > 0 and not lower <= request.price <= upper:
            raise RuntimeError(
                f"limit price {request.price} is outside contract limits {lower}..{upper}"
            )
        lot = (
            sj.StockOrderLot.Common
            if request.lot == "common"
            else sj.StockOrderLot.IntradayOdd
        )
        order = sj.StockOrder(
            action=sj.Action.Buy,
            price=float(request.price),
            quantity=request.quantity,
            price_type=sj.StockPriceType.LMT,
            order_type=sj.OrderType.ROD,
            order_lot=lot,
            order_cond=sj.StockOrderCond.Cash,
            custom_field="C2002",
            account=account,
        )
        trade = api.place_order(contract, order)
        trade, status = _wait_for_acknowledgement(api, trade, order_event)
        receipt = _write_receipt(
            args.receipt_dir,
            request=request,
            account=account,
            contract=contract,
            contract_info=contract_info,
            trade=trade,
            ca_expire_time=ca_expire_time,
            order_events=order_events,
        )
        if status in REJECTED_STATUSES:
            raise RuntimeError(f"order was not accepted; status={status}; receipt={receipt}")
        if status not in ACKNOWLEDGED_STATUSES:
            raise RuntimeError(
                "order submission remains unconfirmed; do not retry automatically; "
                f"status={status}; receipt={receipt}"
            )
        return status, receipt
    finally:
        try:
            api.logout()
        except Exception:
            pass


def main() -> None:
    args = parse_args()
    request = _request(args)
    print(
        json.dumps(
            {"mode": "LIVE" if args.live else "PREVIEW", **_public_request(request)},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )
    if not args.live:
        print("[shioaji-order] preview only; no login and no order submission", flush=True)
        return
    status, receipt = submit_live(args, request)
    print(
        f"[shioaji-order] submitted symbol={SYMBOL} status={status} receipt={receipt}",
        flush=True,
    )


if __name__ == "__main__":
    main()
