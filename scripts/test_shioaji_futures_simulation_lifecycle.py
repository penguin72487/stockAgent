#!/usr/bin/env python3
"""Exercise one executable futures round trip in Shioaji simulation mode.

The client construction is intentionally hard-coded to ``simulation=True``.
The test records the observable Bid/Ask, sends the TAIFEX-legal ``MKT+IOC``
order requested by the user, closes the filled quantity the same way, and
verifies that the paper position returns to its pre-test value.  Every attempt
writes a receipt.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable
from zoneinfo import ZoneInfo

from stockagent.data.taifex_sessions import (
    taifex_continuous_session_open,
    taifex_session_kind,
    taifex_trading_date,
)


TAIPEI = ZoneInfo("Asia/Taipei")
TERMINAL_STATUSES = {"Cancelled", "Filled", "Failed"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", default="TXFR1")
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument(
        "--execute-simulation",
        action="store_true",
        help="required acknowledgement; production mode is not implemented",
    )
    parser.add_argument(
        "--receipt-dir",
        type=Path,
        default=Path("artifacts/orders/shioaji_futures_simulation"),
    )
    return parser.parse_args()


def _required_credential(primary: str, fallback: str) -> str:
    value = os.environ.get(primary, "").strip() or os.environ.get(fallback, "").strip()
    if not value:
        raise RuntimeError(
            f"missing required environment variable: {primary} (or {fallback})"
        )
    return value


def _enum(value: Any) -> str:
    return str(getattr(value, "value", value or ""))


def _mask(value: Any) -> str:
    text = str(value)
    if len(text) <= 4:
        return "*" * len(text)
    return f"{text[:2]}{'*' * (len(text) - 4)}{text[-2:]}"


def _account_type(account: Any) -> str:
    return _enum(getattr(account, "account_type", ""))


def _select_futures_account(accounts: list[Any]) -> Any:
    requested_broker = os.environ.get("SHIOAJI_FUTURES_BROKER_ID", "").strip()
    requested_account = os.environ.get("SHIOAJI_FUTURES_ACCOUNT_ID", "").strip()
    futures = [account for account in accounts if _account_type(account) == "F"]
    if requested_broker and requested_account:
        futures = [
            account
            for account in futures
            if str(getattr(account, "broker_id", "")) == requested_broker
            and str(getattr(account, "account_id", "")) == requested_account
        ]
    if len(futures) != 1:
        raise RuntimeError(
            f"expected exactly one matching simulation futures account, found {len(futures)}"
        )
    return futures[0]


def _status_value(trade: Any) -> str:
    return _enum(getattr(getattr(trade, "status", None), "status", "Unknown"))


def _status_summary(trade: Any) -> dict[str, Any]:
    order = getattr(trade, "order", None)
    status = getattr(trade, "status", None)
    raw_deals = list(getattr(status, "deals", None) or [])
    deals: list[dict[str, Any]] = []
    for raw in raw_deals:
        if isinstance(raw, dict):
            price = raw.get("price")
            quantity = raw.get("quantity")
            sequence = raw.get("seq") or raw.get("seqno")
        else:
            price = getattr(raw, "price", None)
            quantity = getattr(raw, "quantity", None)
            sequence = getattr(raw, "seq", getattr(raw, "seqno", None))
        deals.append(
            {
                "price": float(price or 0.0),
                "quantity": int(quantity or 0),
                "sequence": str(sequence or ""),
            }
        )
    return {
        "status": _status_value(trade),
        "status_code": str(getattr(status, "status_code", "")),
        "message": str(getattr(status, "msg", "")),
        "order_id": str(getattr(order, "id", "")),
        "ordno_present": bool(getattr(order, "ordno", "")),
        "order_quantity": int(getattr(status, "order_quantity", 0) or 0),
        "deal_quantity": int(getattr(status, "deal_quantity", 0) or 0),
        "cancel_quantity": int(getattr(status, "cancel_quantity", 0) or 0),
        "deals": deals,
    }


def _poll_terminal(
    api: Any,
    trade: Any,
    *,
    sleeper: Callable[[float], None] = time.sleep,
    attempts: int = 20,
) -> dict[str, Any]:
    latest = _status_summary(trade)
    for _ in range(attempts):
        if latest["status"] in TERMINAL_STATUSES:
            return latest
        sleeper(0.5)
        api.update_status(trade=trade)
        latest = _status_summary(trade)
    return latest


def _snapshot_book(api: Any, contract: Any) -> dict[str, float]:
    rows = list(api.snapshots([contract]))
    if len(rows) != 1:
        raise RuntimeError("Shioaji did not return exactly one futures snapshot")
    row = rows[0]
    bid = float(getattr(row, "buy_price", 0.0) or 0.0)
    ask = float(getattr(row, "sell_price", 0.0) or 0.0)
    bid_volume = float(getattr(row, "buy_volume", 0.0) or 0.0)
    ask_volume = float(getattr(row, "sell_volume", 0.0) or 0.0)
    if not (0.0 < bid <= ask and bid_volume >= 1.0 and ask_volume >= 1.0):
        raise RuntimeError(
            "no executable one-lot futures Bid/Ask snapshot: "
            f"bid={bid} ask={ask} bid_volume={bid_volume} ask_volume={ask_volume}"
        )
    return {
        "bid": bid,
        "ask": ask,
        "bid_volume": bid_volume,
        "ask_volume": ask_volume,
        "snapshot_ts_ns": int(getattr(row, "ts", 0) or 0),
    }


def _net_position(positions: list[Any], code: str) -> int:
    net = 0
    for position in positions:
        if str(getattr(position, "code", "")) != code:
            continue
        quantity = int(getattr(position, "quantity", 0) or 0)
        direction = _enum(
            getattr(position, "direction", getattr(position, "action", ""))
        ).lower()
        net += -quantity if direction in {"sell", "short", "s"} else quantity
    return net


def _write_receipt(receipt_dir: Path, payload: dict[str, Any]) -> Path:
    receipt_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    path = receipt_dir / f"{stamp}-futures-simulation-lifecycle.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    return path


def _session_open(now: datetime | None = None) -> bool:
    current = now or datetime.now(TAIPEI)
    return taifex_continuous_session_open(current)


def run_futures_simulation_lifecycle(
    args: argparse.Namespace,
    *,
    shioaji_module: Any | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    enforce_session: bool = True,
) -> tuple[dict[str, Any], Path]:
    if not bool(args.execute_simulation):
        raise RuntimeError("refusing to run without --execute-simulation")
    if int(args.quantity) != 1:
        raise RuntimeError("this one-lot safety test requires --quantity 1")
    if enforce_session and not _session_open():
        raise RuntimeError("TAIFEX day/night continuous session is not open")
    api_key = _required_credential("SHIOAJI_API_KEY", "SHIOAJI_TRADE_API_KEY")
    secret_key = _required_credential("SHIOAJI_SECRET_KEY", "SHIOAJI_TRADE_SECRET_KEY")
    if shioaji_module is None:
        import shioaji as shioaji_module
    sj = shioaji_module

    observed_at_taipei = datetime.now(TAIPEI)
    summary: dict[str, Any] = {
        "schema_version": 1,
        "simulation": True,
        "production_order_possible": False,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "observed_at_taipei": observed_at_taipei.isoformat(),
        "session": taifex_session_kind(observed_at_taipei),
        "trading_date": taifex_trading_date(observed_at_taipei).isoformat(),
        "logical_contract": str(args.contract),
        "quantity": 1,
        "steps": [],
        "callback_events": [],
        "result": "failed",
    }
    # This literal must remain non-configurable.  There is no production branch.
    api = sj.Shioaji(simulation=True)
    logged_in = False
    opened_quantity = 0
    concrete_contract: Any | None = None
    account: Any | None = None
    baseline_position = 0
    try:
        if hasattr(api, "set_event_callback"):
            api.set_event_callback(lambda *_args: None)

        def order_callback(state: Any, message: Any) -> None:
            summary["callback_events"].append(
                {
                    "received_at_utc": datetime.now(timezone.utc).isoformat(),
                    "state": str(state),
                    "message_type": type(message).__name__,
                }
            )

        accounts = list(
            api.login(
                api_key=api_key,
                secret_key=secret_key,
                subscribe_trade=True,
            )
        )
        logged_in = True
        api.set_order_callback(order_callback)
        account = _select_futures_account(accounts)
        api.set_default_account(account)
        summary["account"] = {
            "broker_id": _mask(getattr(account, "broker_id", "")),
            "account_id": _mask(getattr(account, "account_id", "")),
        }
        summary["steps"].append({"step": "login_futures_account", "status": "ok"})

        logical = api.contracts.get(str(args.contract))
        if logical is None:
            raise LookupError(f"simulation contract not found: {args.contract}")
        target_code = str(getattr(logical, "target_code", "") or "")
        concrete_contract = api.contracts.get(target_code) if target_code else logical
        if concrete_contract is None:
            raise LookupError(f"resolved futures contract not found: {target_code}")
        concrete_code = str(getattr(concrete_contract, "code", ""))
        if not concrete_code:
            raise RuntimeError("resolved futures contract has no code")
        summary["resolved_contract"] = concrete_code
        summary["steps"].append(
            {
                "step": "resolve_contract_v2",
                "status": "ok",
                "logical": str(args.contract),
                "resolved": concrete_code,
            }
        )

        before_positions = list(api.list_positions(account=account))
        baseline_position = _net_position(before_positions, concrete_code)
        summary["baseline_position"] = baseline_position
        opening_book = _snapshot_book(api, concrete_contract)
        summary["opening_book"] = opening_book
        buy_order = sj.FuturesOrder(
            action=sj.Action.Buy,
            price=0,
            quantity=1,
            price_type=sj.FuturesPriceType.MKT,
            order_type=sj.OrderType.IOC,
            octype=sj.FuturesOCType.Auto,
            custom_field="FAPI01",
            account=account,
        )
        buy_trade = api.place_order(concrete_contract, buy_order)
        buy_status = _poll_terminal(api, buy_trade, sleeper=sleeper)
        summary["steps"].append({"step": "market_buy_ioc", **buy_status})
        opened_quantity = int(buy_status["deal_quantity"])
        if opened_quantity != 1 or buy_status["status"] == "Failed":
            raise RuntimeError(
                f"simulation buy did not fill one contract: {buy_status}"
            )

        closing_book = _snapshot_book(api, concrete_contract)
        summary["closing_book"] = closing_book
        sell_order = sj.FuturesOrder(
            action=sj.Action.Sell,
            price=0,
            quantity=opened_quantity,
            price_type=sj.FuturesPriceType.MKT,
            order_type=sj.OrderType.IOC,
            octype=sj.FuturesOCType.Auto,
            custom_field="FAPI02",
            account=account,
        )
        sell_trade = api.place_order(concrete_contract, sell_order)
        sell_status = _poll_terminal(api, sell_trade, sleeper=sleeper)
        summary["steps"].append({"step": "market_sell_ioc", **sell_status})
        if int(sell_status["deal_quantity"]) != opened_quantity:
            raise RuntimeError(f"simulation close did not fill: {sell_status}")
        opened_quantity = 0

        observed_position = baseline_position
        for _ in range(20):
            positions = list(api.list_positions(account=account))
            observed_position = _net_position(positions, concrete_code)
            if observed_position == baseline_position:
                break
            sleeper(0.5)
        summary["final_position"] = observed_position
        summary["steps"].append(
            {
                "step": "reconcile_position",
                "status": "ok"
                if observed_position == baseline_position
                else "mismatch",
                "baseline_position": baseline_position,
                "final_position": observed_position,
            }
        )
        if observed_position != baseline_position:
            raise RuntimeError(
                "simulation position did not return to its baseline: "
                f"{baseline_position} -> {observed_position}"
            )
        summary["result"] = "ok"
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
        # If the opening leg filled but the normal close path failed, make one
        # best-effort simulation-only IOC close before returning a failed test.
        if opened_quantity and concrete_contract is not None and account is not None:
            try:
                book = _snapshot_book(api, concrete_contract)
                emergency = sj.FuturesOrder(
                    action=sj.Action.Sell,
                    price=0,
                    quantity=opened_quantity,
                    price_type=sj.FuturesPriceType.MKT,
                    order_type=sj.OrderType.IOC,
                    octype=sj.FuturesOCType.Auto,
                    custom_field="FAPIFL",
                    account=account,
                )
                emergency_trade = api.place_order(concrete_contract, emergency)
                emergency_status = _poll_terminal(api, emergency_trade, sleeper=sleeper)
                summary["steps"].append(
                    {"step": "emergency_simulation_close", **emergency_status}
                )
                if int(emergency_status["deal_quantity"]) == opened_quantity:
                    opened_quantity = 0
            except Exception as close_exc:
                summary["emergency_close_error"] = (
                    f"{type(close_exc).__name__}: {close_exc}"
                )
    finally:
        summary["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        summary["callback_event_count"] = len(summary["callback_events"])
        if logged_in:
            try:
                api.logout()
            except Exception as exc:
                summary["logout_error"] = f"{type(exc).__name__}: {exc}"
        receipt = _write_receipt(Path(args.receipt_dir), summary)
    return summary, receipt


def main() -> int:
    args = parse_args()
    summary, receipt = run_futures_simulation_lifecycle(args)
    print(
        json.dumps(
            {
                "simulation": True,
                "result": summary["result"],
                "logical_contract": summary["logical_contract"],
                "resolved_contract": summary.get("resolved_contract"),
                "steps": summary["steps"],
                "callback_event_count": summary["callback_event_count"],
                "receipt": str(receipt),
                "error": summary.get("error"),
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )
    return 0 if summary["result"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
