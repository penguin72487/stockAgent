from __future__ import annotations

import argparse
from datetime import datetime, time as datetime_time, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
TERMINAL_STATUSES = {"Cancelled", "Filled", "Failed"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute a Shioaji simulation-only stock order lifecycle: login, "
            "place, status refresh, reduce quantity, cancel, and reconcile."
        )
    )
    parser.add_argument(
        "--symbol",
        default="2890",
        help="stock code; 2890 is SinoPac's documented stock simulation-test contract",
    )
    parser.add_argument("--quantity", type=int, default=2)
    parser.add_argument(
        "--execute-simulation",
        action="store_true",
        help="required acknowledgement; this still uses simulation=True only",
    )
    parser.add_argument(
        "--receipt-dir",
        type=Path,
        default=Path("artifacts/orders/shioaji_simulation"),
    )
    return parser.parse_args()


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"missing required environment variable: {name}")
    return value


def _account_type(account: Any) -> str:
    value = getattr(account, "account_type", "")
    return str(getattr(value, "value", value))


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value or ""))


def _select_stock_account(accounts: list[Any]) -> Any:
    broker_id = os.environ.get("SHIOAJI_TRADE_BROKER_ID", "").strip()
    account_id = os.environ.get("SHIOAJI_TRADE_ACCOUNT_ID", "").strip()
    stocks = [account for account in accounts if _account_type(account) == "S"]
    if broker_id and account_id:
        stocks = [
            account
            for account in stocks
            if str(getattr(account, "broker_id", "")) == broker_id
            and str(getattr(account, "account_id", "")) == account_id
        ]
    if len(stocks) != 1:
        raise RuntimeError(
            f"expected exactly one matching simulation stock account, found {len(stocks)}"
        )
    return stocks[0]


def _mask(value: Any) -> str:
    text = str(value)
    if len(text) <= 4:
        return "*" * len(text)
    return f"{text[:2]}{'*' * (len(text) - 4)}{text[-2:]}"


def _status_value(trade: Any) -> str:
    raw = getattr(getattr(trade, "status", None), "status", None)
    return str(getattr(raw, "value", raw or "Unknown"))


def _status_summary(trade: Any) -> dict[str, Any]:
    status = getattr(trade, "status", None)
    order = getattr(trade, "order", None)
    return {
        "status": _status_value(trade),
        "status_code": str(getattr(status, "status_code", "")),
        "message": str(getattr(status, "msg", "")),
        "ordno_present": bool(getattr(order, "ordno", "")),
        "order_quantity": int(getattr(status, "order_quantity", 0) or 0),
        "deal_quantity": int(getattr(status, "deal_quantity", 0) or 0),
        "cancel_quantity": int(getattr(status, "cancel_quantity", 0) or 0),
    }


def _poll_status(
    api: Any,
    account: Any,
    trade: Any,
    *,
    predicate: Callable[[dict[str, Any]], bool],
    sleeper: Callable[[float], None],
    attempts: int = 10,
) -> dict[str, Any]:
    latest = _status_summary(trade)
    for _ in range(attempts):
        sleeper(1.0)
        api.update_status(trade=trade)
        latest = _status_summary(trade)
        if predicate(latest):
            return latest
    return latest


def _trade_id(trade: Any) -> str:
    status_id = getattr(getattr(trade, "status", None), "id", "")
    order_id = getattr(getattr(trade, "order", None), "id", "")
    return str(status_id or order_id or "")


def _poll_reconciled_status(
    api: Any,
    account: Any,
    trade: Any,
    *,
    predicate: Callable[[dict[str, Any]], bool],
    sleeper: Callable[[float], None],
    attempts: int = 10,
) -> tuple[dict[str, Any], Any]:
    target_id = _trade_id(trade)
    latest_trade = trade
    latest = _status_summary(trade)
    for _ in range(attempts):
        sleeper(1.0)
        api.update_status(account)
        candidates = list(api.list_trades())
        if target_id:
            latest_trade = next(
                (candidate for candidate in candidates if _trade_id(candidate) == target_id),
                latest_trade,
            )
        latest = _status_summary(latest_trade)
        if predicate(latest):
            return latest, latest_trade
    return latest, latest_trade


def _test_window_open(now: datetime | None = None) -> bool:
    current = now or datetime.now(TAIPEI)
    return current.weekday() < 5 and datetime_time(8, 0) <= current.time() < datetime_time(20, 0)


def _write_receipt(receipt_dir: Path, summary: dict[str, Any]) -> Path:
    receipt_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    path = receipt_dir / f"{stamp}-simulation-lifecycle.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


def run_simulation_lifecycle(
    args: argparse.Namespace,
    *,
    shioaji_module: Any | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    enforce_test_window: bool = True,
) -> tuple[dict[str, Any], Path]:
    if not args.execute_simulation:
        raise RuntimeError("refusing to run without --execute-simulation")
    if args.quantity < 2:
        raise RuntimeError("quantity must be at least 2 so the reduce-quantity step is testable")
    if not args.symbol.isdigit():
        raise RuntimeError("symbol must contain digits only")
    if enforce_test_window and not _test_window_open():
        raise RuntimeError(
            "SinoPac simulation test records are accepted only on business days 08:00-20:00"
        )
    api_key = _required_env("SHIOAJI_TRADE_API_KEY")
    secret_key = _required_env("SHIOAJI_TRADE_SECRET_KEY")
    if shioaji_module is None:
        import shioaji as shioaji_module
    sj = shioaji_module

    # This literal is intentionally not configurable. This module cannot create
    # a production Shioaji client.
    api = sj.Shioaji(simulation=True)
    if hasattr(api, "set_event_callback"):
        api.set_event_callback(lambda *_args: None)
    callback_events: list[dict[str, str]] = []
    steps: list[dict[str, Any]] = []
    logged_in = False
    try:
        def order_callback(state: Any, message: Any) -> None:
            callback_events.append(
                {"state": str(state), "message_type": type(message).__name__}
            )

        accounts = api.login(
            api_key=api_key,
            secret_key=secret_key,
            subscribe_trade=True,
        )
        logged_in = True
        api.set_order_callback(order_callback)
        account = _select_stock_account(list(accounts))
        api.set_default_account(account)
        steps.append({"step": "login", "status": "ok"})

        contracts = getattr(api, "contracts", None)
        if contracts is not None and hasattr(contracts, "get"):
            contract = contracts.get(args.symbol)
            contract_info = contracts.info(contract) if contract is not None else None
        else:
            contracts = api.Contracts
            contract = contracts.Stocks[args.symbol]
            contract_info = contract
        if contract is None or str(getattr(contract, "code", "")) != args.symbol:
            raise RuntimeError(f"simulation contract lookup failed for {args.symbol}")
        exchange = _enum_value(getattr(contract, "exchange", None))
        if exchange not in {"TSE", "OTC"}:
            raise RuntimeError(f"unexpected stock exchange: {exchange}")
        reference = float(getattr(contract_info, "reference", 0.0) or 0.0)
        limit_down = float(getattr(contract_info, "limit_down", 0.0) or 0.0)
        # SinoPac's official Python simulation-test example prices the stock
        # order at the Contract V2 reference price.  Keep limit_down only as a
        # diagnostic field; do not silently substitute a different test price.
        price = reference
        if price <= 0:
            raise RuntimeError("simulation contract has no usable reference price")
        steps.append(
            {
                "step": "contract",
                "status": "ok",
                "code": args.symbol,
                "exchange": exchange,
                "name": str(getattr(contract_info, "name", "")),
                "test_price": price,
                "test_price_source": "reference",
                "limit_down": limit_down,
            }
        )

        order = sj.StockOrder(
            action=sj.Action.Buy,
            price=price,
            quantity=args.quantity,
            price_type=sj.StockPriceType.LMT,
            order_type=sj.OrderType.ROD,
            order_lot=sj.StockOrderLot.Common,
            order_cond=sj.StockOrderCond.Cash,
            custom_field="SIMTST",
            account=account,
        )
        trade = api.place_order(contract, order)
        steps.append({"step": "place", **_status_summary(trade)})
        if _status_value(trade) == "Failed":
            raise RuntimeError(f"simulation place_order failed: {_status_summary(trade)}")

        place_status = _poll_status(
            api,
            account,
            trade,
            predicate=lambda status: status["ordno_present"]
            and status["status"] != "PendingSubmit",
            sleeper=sleeper,
        )
        steps.append({"step": "status_after_place", **place_status})
        if _status_value(trade) == "Failed":
            raise RuntimeError(f"simulation order failed: {_status_summary(trade)}")

        lifecycle_skipped = None
        if _status_value(trade) in TERMINAL_STATUSES:
            lifecycle_skipped = f"order already terminal: {_status_value(trade)}"
        else:
            updated = api.update_order(trade=trade, qty=args.quantity - 1)
            if updated is not None:
                trade = updated
            reduce_status = _poll_status(
                api,
                account,
                trade,
                predicate=lambda status: status["cancel_quantity"] >= 1
                or status["status"] in TERMINAL_STATUSES,
                sleeper=sleeper,
            )
            steps.append({"step": "reduce_quantity", **reduce_status})
            if _status_value(trade) == "Failed":
                raise RuntimeError(
                    f"simulation reduce quantity failed: {_status_summary(trade)}"
                )

            if _status_value(trade) in TERMINAL_STATUSES:
                lifecycle_skipped = (
                    f"cancel skipped because order became {_status_value(trade)}"
                )
            else:
                cancelled = api.cancel_order(trade)
                if cancelled is not None:
                    trade = cancelled
                cancel_status, trade = _poll_reconciled_status(
                    api,
                    account,
                    trade,
                    predicate=lambda status: (
                        status["cancel_quantity"] + status["deal_quantity"]
                        >= args.quantity
                    )
                    or status["status"] in TERMINAL_STATUSES,
                    sleeper=sleeper,
                )
                steps.append({"step": "cancel", **cancel_status})
                if _status_value(trade) == "Failed":
                    raise RuntimeError(
                        f"simulation cancel failed: {_status_summary(trade)}"
                    )
                if (
                    cancel_status["cancel_quantity"]
                    + cancel_status["deal_quantity"]
                    < args.quantity
                ):
                    raise RuntimeError(
                        "simulation cancel did not reduce remaining quantity to zero: "
                        f"{cancel_status}"
                    )

        trades = list(api.list_trades())
        target_id = _trade_id(trade)
        reconciled = any(
            (
                bool(target_id)
                and _trade_id(item) == target_id
                and str(getattr(getattr(item, "contract", None), "code", ""))
                == args.symbol
            )
            for item in trades
        )
        steps.append(
            {
                "step": "reconcile",
                "status": "ok" if reconciled else "missing",
                "trade_count": len(trades),
            }
        )
        if not reconciled:
            raise RuntimeError("simulation order was not found in list_trades")

        summary = {
            "schema_version": 1,
            "simulation": True,
            "production_order_possible": False,
            "sdk_version": str(getattr(sj, "__version__", "unknown")),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": args.symbol,
            "quantity": args.quantity,
            "account": {
                "broker_id": _mask(getattr(account, "broker_id", "")),
                "account_id": _mask(getattr(account, "account_id", "")),
            },
            "steps": steps,
            "callback_event_count": len(callback_events),
            "callback_events": callback_events,
            "lifecycle_skipped": lifecycle_skipped,
            "result": "ok",
        }
        receipt = _write_receipt(args.receipt_dir, summary)
        return summary, receipt
    finally:
        if logged_in:
            try:
                api.logout()
            except Exception:
                pass


def main() -> None:
    args = parse_args()
    summary, receipt = run_simulation_lifecycle(args)
    print(
        json.dumps(
            {
                "simulation": summary["simulation"],
                "result": summary["result"],
                "symbol": summary["symbol"],
                "steps": summary["steps"],
                "callback_event_count": summary["callback_event_count"],
                "receipt": str(receipt),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
