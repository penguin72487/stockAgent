"""Forward Shioaji simulation for the seven TXO volatility-model strategies.

This module is deliberately attached to the existing TAIFEX market-data
connection.  It maintains two separate truths:

* an attributable one-lot ideal ledger filled at the causally received best
  opposing price, including the user's fee and statutory tax schedule; and
* Shioaji ``simulation=True`` MKT+IOC orders and their broker callbacks.

The broker account nets all strategy orders, so broker positions are an
execution/reconciliation channel.  Per-strategy P&L always comes from the
independent ideal ledgers, never from a fabricated split of a net broker
position.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import queue
import threading
import time
from typing import Any, Final, Iterable, Mapping

import polars as pl

from stockagent.data.tw_index_derivatives_tick import TAIPEI
from stockagent.data.tw_index_futures import TAIFEX_INDEX_FUTURES_MULTIPLIERS
from stockagent.research.taifex_transaction_tax import (
    option_cash_settlement_transaction_tax_twd,
    option_premium_transaction_tax_twd,
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)
from stockagent.research.taifex_volatility_models import (
    BidAskSurfaceQuote,
    SECONDS_PER_YEAR,
    VOLATILITY_MODEL_IDS,
    VOLATILITY_MODEL_IMPLEMENTATION,
    VOLATILITY_MODEL_LABELS,
    build_bidask_iv_surface,
    fit_volatility_model,
)


SCHEMA_VERSION: Final[int] = 1
CLASSIC_VARIANT_ID: Final[str] = "classic_opening_straddle"
MODEL_VARIANT_PREFIX: Final[str] = "daily_vol_model_gamma__"
STRATEGY_IDS: Final[tuple[str, ...]] = (
    CLASSIC_VARIANT_ID,
    *(f"{MODEL_VARIANT_PREFIX}{model_id}" for model_id in VOLATILITY_MODEL_IDS),
)
OPTION_MULTIPLIER: Final[float] = 50.0
HEDGE_MULTIPLIER: Final[float] = float(TAIFEX_INDEX_FUTURES_MULTIPLIERS["MTX"])
OPTION_FEE_PER_SIDE_TWD: Final[float] = 22.0
HEDGE_FEE_PER_SIDE_TWD: Final[float] = 24.0
ENTRY_BOOK_MAX_AGE_SECONDS: Final[float] = 2.0
SURFACE_BOOK_MAX_AGE_SECONDS: Final[float] = 120.0
MARK_INTERVAL_SECONDS: Final[float] = 60.0
STATUS_INTERVAL_SECONDS: Final[float] = 5.0
TERMINAL_ORDER_STATUSES: Final[frozenset[str]] = frozenset(
    {"Cancelled", "Filled", "Failed"}
)


def _enum(value: Any) -> str:
    return str(getattr(value, "value", value or ""))


def _right(value: Any) -> str:
    normalized = _enum(value).strip().upper()
    if normalized in {"C", "CALL"}:
        return "C"
    if normalized in {"P", "PUT"}:
        return "P"
    raise ValueError(f"unsupported option right: {value!r}")


def _base(info: Any) -> Any:
    base = getattr(info, "base", None)
    if base is None:
        raise ValueError("Contract V2 info has no Base contract")
    return base


def _series_id(info: Any) -> str:
    root = str(getattr(info, "root", "")).strip().upper()
    month = str(getattr(info, "delivery_month", "")).strip()
    expiry = getattr(info, "delivery_date", None)
    if not root or not month or not isinstance(expiry, date):
        raise ValueError("option info lacks root/delivery_month/delivery_date")
    return f"{root}:{month}:{expiry.isoformat()}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _round_nearest_contract(value: float) -> int:
    if value >= 0.0:
        return int(math.floor(value + 0.5))
    return int(math.ceil(value - 0.5))


def _book_price(row: Mapping[str, Any], side: str) -> float | None:
    value = row.get(f"{side}_price_1")
    try:
        price = float(value)
    except (TypeError, ValueError):
        return None
    return price if math.isfinite(price) and price > 0.0 else None


def _book_volume(row: Mapping[str, Any], side: str) -> int:
    try:
        return int(row.get(f"{side}_volume_1") or 0)
    except (TypeError, ValueError):
        return 0


def _book_age_seconds(row: Mapping[str, Any], decision_ns: int) -> float:
    return max(0.0, (int(decision_ns) - int(row["receive_ts_ns"])) / 1e9)


def _executable_book(
    row: Mapping[str, Any] | None,
    *,
    decision_ns: int,
    maximum_age_seconds: float,
    require_one_lot: bool,
) -> tuple[float, float] | None:
    if row is None or bool(row.get("simtrade", False)):
        return None
    if int(row.get("receive_ts_ns") or 0) > int(decision_ns):
        return None
    if _book_age_seconds(row, decision_ns) > maximum_age_seconds:
        return None
    bid = _book_price(row, "bid")
    ask = _book_price(row, "ask")
    if bid is None or ask is None or bid > ask:
        return None
    if require_one_lot and (
        _book_volume(row, "bid") < 1 or _book_volume(row, "ask") < 1
    ):
        return None
    return bid, ask


def _sanitize_event(value: Any, *, key: str = "") -> Any:
    lowered = key.casefold()
    if lowered in {"person_id", "account_id", "broker_id", "username"}:
        text = str(value or "")
        return "***" if text else ""
    if isinstance(value, Mapping):
        return {str(k): _sanitize_event(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_event(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "to_dict"):
        try:
            return _sanitize_event(value.to_dict())
        except Exception:
            pass
    try:
        return _sanitize_event(dict(value))
    except Exception:
        return str(value)


@dataclass(frozen=True, slots=True)
class OptionInstrument:
    code: str
    root: str
    series: str
    expiry: date
    strike: float
    right: str
    contract: Any


@dataclass(frozen=True, slots=True)
class FuturesInstrument:
    logical_code: str
    code: str
    contract: Any
    last_trading_date: date | None = None


def option_instruments(infos: Iterable[Any]) -> tuple[OptionInstrument, ...]:
    output: list[OptionInstrument] = []
    for info in infos:
        if not hasattr(info, "option_right"):
            continue
        expiry = getattr(info, "delivery_date", None)
        if not isinstance(expiry, date):
            continue
        base = _base(info)
        output.append(
            OptionInstrument(
                code=str(getattr(base, "code")),
                root=str(getattr(info, "root", "")).strip().upper(),
                series=_series_id(info),
                expiry=expiry,
                strike=float(getattr(info, "strike_price")),
                right=_right(getattr(info, "option_right")),
                contract=base,
            )
        )
    output.sort(
        key=lambda item: (item.expiry, item.root, item.strike, item.right, item.code)
    )
    return tuple(output)


class TaifexVolatilitySimulation:
    """Stateful coordinator called from the existing Shioaji FOP worker."""

    def __init__(
        self,
        *,
        api: Any,
        shioaji_module: Any,
        state_dir: Path,
        option_infos: Iterable[Any],
        underlying: FuturesInstrument,
        hedge: FuturesInstrument,
        final_settlement_path: Path,
        calibration_time: datetime_time = datetime_time(13, 29),
        bootstrap_after: date | None = None,
        broker_orders_enabled: bool = True,
    ) -> None:
        self.api = api
        self.sj = shioaji_module
        self.state_dir = Path(state_dir)
        self.state_path = self.state_dir / "state.json"
        self.status_path = self.state_dir / "status.json"
        self.events_path = self.state_dir / "events.jsonl"
        self.ledger_path = self.state_dir / "ideal_ledger.jsonl"
        self.marks_path = self.state_dir / "marks.jsonl"
        self.calibrations_path = self.state_dir / "calibrations.jsonl"
        self.final_settlement_path = Path(final_settlement_path)
        self.calibration_time = calibration_time
        self.underlying = underlying
        self.hedge = hedge
        self.options = option_instruments(option_infos)
        self.options_by_code = {item.code: item for item in self.options}
        self.contracts_by_code = {
            underlying.code: underlying.contract,
            hedge.code: hedge.contract,
            **{item.code: item.contract for item in self.options},
        }
        self.latest_books: dict[str, dict[str, Any]] = {}
        self.callback_queue: queue.Queue[tuple[str, Any]] = queue.Queue(10_000)
        self.callback_overflow = threading.Event()
        self.last_mark_monotonic = 0.0
        self.last_status_monotonic = 0.0
        self.broker_orders_enabled = bool(broker_orders_enabled)
        self.account = getattr(api, "futopt_account", None)
        if self.account is None:
            raise RuntimeError("simulation login has no futures/options account")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.lock_handle = (self.state_dir / "engine.lock").open("a+")
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                "another TAIFEX strategy engine holds engine.lock"
            ) from exc
        self.state = self._load_or_initialize_state(bootstrap_after)
        self._reconcile_inflight_orders()
        self._write_status(force=True)

    def _load_or_initialize_state(self, bootstrap_after: date | None) -> dict[str, Any]:
        if self.state_path.is_file():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if int(payload.get("schema_version", -1)) != SCHEMA_VERSION:
                raise RuntimeError("strategy state schema mismatch")
            if payload.get("simulation_only") is not True:
                raise RuntimeError("strategy state is not marked simulation_only")
            if tuple(payload.get("strategy_ids", ())) != STRATEGY_IDS:
                raise RuntimeError("strategy state variant set mismatch")
            return payload
        today = datetime.now(TAIPEI).date()
        weekly_expiries = sorted(
            {
                item.expiry
                for item in self.options
                if item.root != "TXO" and item.expiry >= today
            }
        )
        inferred_bootstrap = bootstrap_after or (
            weekly_expiries[0] if weekly_expiries else today
        )
        payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "simulation_only": True,
            "production_order_possible": False,
            "strategy_ids": list(STRATEGY_IDS),
            "created_at_utc": _now_iso(),
            "updated_at_utc": _now_iso(),
            "bootstrap_after_date": inferred_bootstrap.isoformat(),
            "active_cycle": None,
            "pending_targets": {},
            "last_calibration_date": None,
            "last_settled_expiry": None,
            "broker_sequence": 0,
            "inflight_orders": {},
            "broker_order_failures": 0,
            "broker_orders_enabled": self.broker_orders_enabled,
            "engine_status": "waiting_for_bootstrap",
            "blocked_reason": None,
            "strategies": {
                strategy_id: {
                    "gross_cash_twd": 0.0,
                    "fees_twd": 0.0,
                    "tax_twd": 0.0,
                    "futures_position": 0,
                    "trade_sides": 0,
                }
                for strategy_id in STRATEGY_IDS
            },
        }
        self.state = payload
        self._persist_state()
        _append_jsonl(
            self.events_path,
            {
                "event": "state_initialized",
                "at_utc": _now_iso(),
                "bootstrap_after_date": inferred_bootstrap.isoformat(),
                "reason": "do_not_reconstruct_an_already_open_weekly_cycle",
            },
        )
        return payload

    def _persist_state(self) -> None:
        self.state["updated_at_utc"] = _now_iso()
        _atomic_json(self.state_path, self.state)

    def on_book(self, row: Mapping[str, Any]) -> None:
        """Non-blocking market-data callback handoff."""

        self.latest_books[str(row["code"])] = dict(row)

    def on_tick(self, _row: Mapping[str, Any]) -> None:
        return

    def on_order_event(self, state: Any, message: Any) -> None:
        """Non-blocking order callback handoff."""

        try:
            self.callback_queue.put_nowait((str(state), message))
        except queue.Full:
            self.callback_overflow.set()

    def _drain_callbacks(self) -> None:
        if self.callback_overflow.is_set():
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = "order_callback_queue_overflow"
        changed = False
        while True:
            try:
                callback_state, raw_payload = self.callback_queue.get_nowait()
            except queue.Empty:
                break
            payload = _sanitize_event(raw_payload)
            _append_jsonl(
                self.events_path,
                {
                    "event": "broker_order_callback",
                    "at_utc": _now_iso(),
                    "state": callback_state,
                    "payload": payload,
                },
            )
            operation = (
                payload.get("operation", {}) if isinstance(payload, Mapping) else {}
            )
            if operation and str(operation.get("op_code", "")) != "00":
                self.state["broker_order_failures"] = (
                    int(self.state.get("broker_order_failures", 0)) + 1
                )
            order = payload.get("order", {}) if isinstance(payload, Mapping) else {}
            status = payload.get("status", {}) if isinstance(payload, Mapping) else {}
            trade_id = str(order.get("id") or status.get("id") or "")
            if trade_id:
                for custom, inflight in list(self.state["inflight_orders"].items()):
                    if str(inflight.get("trade_id", "")) == trade_id:
                        event_name = callback_state.casefold()
                        if (
                            "deal" in event_name
                            or int(status.get("cancel_quantity", 0) or 0) > 0
                        ):
                            self.state["inflight_orders"].pop(custom, None)
                            changed = True
        if changed:
            self._persist_state()

    def _trade_status(self, trade: Any) -> str:
        return _enum(getattr(getattr(trade, "status", None), "status", "Unknown"))

    def _reconcile_inflight_orders(self) -> None:
        inflight = dict(self.state.get("inflight_orders") or {})
        if not inflight:
            return
        try:
            self.api.update_status(self.account)
            trades = list(self.api.list_trades())
        except Exception as exc:
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = (
                f"inflight_reconciliation_failed:{type(exc).__name__}:{exc}"
            )
            self._persist_state()
            return
        by_custom = {
            str(getattr(getattr(trade, "order", None), "custom_field", "")): trade
            for trade in trades
        }
        unresolved: list[str] = []
        for custom, intent in inflight.items():
            trade = by_custom.get(custom)
            if trade is None:
                unresolved.append(custom)
                continue
            status = self._trade_status(trade)
            if status not in TERMINAL_ORDER_STATUSES:
                try:
                    self.api.cancel_order(trade)
                    time.sleep(0.25)
                    self.api.update_status(trade=trade)
                    status = self._trade_status(trade)
                except Exception:
                    unresolved.append(custom)
                    continue
            if status in TERMINAL_ORDER_STATUSES:
                self.state["inflight_orders"].pop(custom, None)
            else:
                unresolved.append(custom)
        if unresolved:
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = (
                f"unresolved_broker_orders:{','.join(unresolved)}"
            )
        self._persist_state()

    def _submit_broker_order(
        self,
        *,
        code: str,
        delta_contracts: int,
        strategy_id: str,
        reason: str,
    ) -> None:
        if delta_contracts == 0 or not self.broker_orders_enabled:
            return
        contract = self.contracts_by_code.get(code)
        if contract is None:
            raise LookupError(f"broker contract unavailable: {code}")
        self.state["broker_sequence"] = int(self.state.get("broker_sequence", 0)) + 1
        sequence = int(self.state["broker_sequence"])
        custom = f"V{sequence % 100000:05d}"
        action = self.sj.Action.Buy if delta_contracts > 0 else self.sj.Action.Sell
        intent = {
            "custom_field": custom,
            "created_at_utc": _now_iso(),
            "code": code,
            "delta_contracts": int(delta_contracts),
            "strategy_id": strategy_id,
            "reason": reason,
            "status": "intent_persisted_before_place_order",
        }
        self.state["inflight_orders"][custom] = intent
        self._persist_state()
        _append_jsonl(self.events_path, {"event": "broker_order_intent", **intent})
        try:
            order = self.sj.FuturesOrder(
                action=action,
                price=0,
                quantity=abs(int(delta_contracts)),
                price_type=self.sj.FuturesPriceType.MKT,
                order_type=self.sj.OrderType.IOC,
                octype=self.sj.FuturesOCType.Auto,
                custom_field=custom,
                account=self.account,
            )
            trade = self.api.place_order(contract, order)
            trade_id = str(
                getattr(getattr(trade, "order", None), "id", "")
                or getattr(getattr(trade, "status", None), "id", "")
            )
            status = self._trade_status(trade)
            self.state["inflight_orders"][custom].update(
                {"trade_id": trade_id, "initial_status": status}
            )
            _append_jsonl(
                self.events_path,
                {
                    "event": "broker_order_response",
                    "at_utc": _now_iso(),
                    "custom_field": custom,
                    "trade_id": trade_id,
                    "status": status,
                },
            )
            if status in TERMINAL_ORDER_STATUSES:
                self.state["inflight_orders"].pop(custom, None)
            self._persist_state()
        except Exception as exc:
            self.state["inflight_orders"][custom]["place_error"] = (
                f"{type(exc).__name__}: {exc}"
            )
            self.state["broker_order_failures"] = (
                int(self.state.get("broker_order_failures", 0)) + 1
            )
            self.broker_orders_enabled = False
            self.state["broker_orders_enabled"] = False
            self.state["blocked_reason"] = f"broker_place_failed:{custom}"
            self._persist_state()
            raise

    def _record_ideal_trade(
        self,
        *,
        strategy_id: str,
        instrument_type: str,
        product: str,
        code: str,
        delta_contracts: int,
        price_points: float,
        decision_ns: int,
        receive_ns: int,
        reason: str,
        series: str | None = None,
        strike: float | None = None,
        option_right: str | None = None,
        fee_twd: float | None = None,
        tax_twd: float | None = None,
        price_source: str | None = None,
    ) -> None:
        if strategy_id not in STRATEGY_IDS:
            raise ValueError(f"unknown strategy id: {strategy_id}")
        quantity = abs(int(delta_contracts))
        multiplier = (
            OPTION_MULTIPLIER if instrument_type == "option" else HEDGE_MULTIPLIER
        )
        if fee_twd is None:
            rate = (
                OPTION_FEE_PER_SIDE_TWD
                if instrument_type == "option"
                else HEDGE_FEE_PER_SIDE_TWD
            )
            fee_twd = quantity * rate
        if tax_twd is None:
            if instrument_type == "option":
                tax_twd = quantity * option_premium_transaction_tax_twd(
                    price_points,
                    multiplier_twd_per_point=OPTION_MULTIPLIER,
                )
            else:
                tax_twd = quantity * taifex_tax_per_contract_twd(
                    price_points,
                    multiplier_twd_per_point=HEDGE_MULTIPLIER,
                    tax_rate=stock_index_futures_tax_rate(
                        datetime.fromtimestamp(decision_ns / 1e9, tz=TAIPEI).date()
                    ),
                )
        gross_cash = -int(delta_contracts) * float(price_points) * multiplier
        ledger = self.state["strategies"][strategy_id]
        ledger["gross_cash_twd"] = float(ledger["gross_cash_twd"]) + gross_cash
        ledger["fees_twd"] = float(ledger["fees_twd"]) + float(fee_twd)
        ledger["tax_twd"] = float(ledger["tax_twd"]) + float(tax_twd)
        ledger["trade_sides"] = int(ledger["trade_sides"]) + quantity
        if instrument_type == "future":
            ledger["futures_position"] = int(ledger["futures_position"]) + int(
                delta_contracts
            )
        row = {
            "schema_version": SCHEMA_VERSION,
            "event": "ideal_executable_trade",
            "recorded_at_utc": _now_iso(),
            "strategy_id": strategy_id,
            "instrument_type": instrument_type,
            "product": product,
            "code": code,
            "series": series,
            "strike": strike,
            "option_right": option_right,
            "decision_ts_ns": int(decision_ns),
            "book_receive_ts_ns": int(receive_ns),
            "book_age_ms": max(0.0, (decision_ns - receive_ns) / 1e6),
            "price_source": price_source
            or (
                "causally_received_best_ask"
                if delta_contracts > 0
                else "causally_received_best_bid"
            ),
            "price_points": float(price_points),
            "delta_contracts": int(delta_contracts),
            "multiplier_twd_per_point": multiplier,
            "gross_cash_flow_twd": gross_cash,
            "fixed_fee_twd": float(fee_twd),
            "transaction_tax_twd": float(tax_twd),
            "net_cash_flow_twd": gross_cash - float(fee_twd) - float(tax_twd),
            "reason": reason,
        }
        _append_jsonl(self.ledger_path, row)

    def _weekly_pairs_after(
        self, trading_date: date
    ) -> list[tuple[date, str, float, OptionInstrument, OptionInstrument]]:
        grouped: dict[tuple[date, str, float], dict[str, OptionInstrument]] = {}
        for item in self.options:
            if item.root == "TXO" or item.expiry <= trading_date:
                continue
            grouped.setdefault((item.expiry, item.series, item.strike), {})[
                item.right
            ] = item
        output = []
        for (expiry, series, strike), rights in grouped.items():
            if {"C", "P"} <= set(rights):
                output.append((expiry, series, strike, rights["C"], rights["P"]))
        output.sort(key=lambda item: (item[0], item[2], item[1]))
        return output

    def _underlying_book(
        self, decision_ns: int, *, max_age: float = ENTRY_BOOK_MAX_AGE_SECONDS
    ) -> tuple[float, float] | None:
        return _executable_book(
            self.latest_books.get(self.underlying.code),
            decision_ns=decision_ns,
            maximum_age_seconds=max_age,
            require_one_lot=True,
        )

    def _hedge_book(self, decision_ns: int) -> tuple[float, float] | None:
        return _executable_book(
            self.latest_books.get(self.hedge.code),
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )

    def _maybe_open_cycle(self, now: datetime, decision_ns: int) -> None:
        if self.state.get("active_cycle") is not None:
            return
        bootstrap_after = date.fromisoformat(str(self.state["bootstrap_after_date"]))
        if now.date() <= bootstrap_after:
            self.state["engine_status"] = "waiting_for_bootstrap"
            return
        if not (datetime_time(8, 45) <= now.time() < datetime_time(9, 5)):
            return
        underlying_book = self._underlying_book(decision_ns)
        if underlying_book is None:
            self.state["engine_status"] = "waiting_for_fresh_open_books"
            return
        forward = sum(underlying_book) / 2.0
        pairs = self._weekly_pairs_after(now.date())
        if not pairs:
            self.state["engine_status"] = "waiting_for_listed_weekly_pair"
            return
        nearest_expiry = pairs[0][0]
        expiry_pairs = [item for item in pairs if item[0] == nearest_expiry]
        expiry, series, strike, call, put = min(
            expiry_pairs, key=lambda item: (abs(item[2] - forward), item[2])
        )
        call_row = self.latest_books.get(call.code)
        put_row = self.latest_books.get(put.code)
        call_book = _executable_book(
            call_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        put_book = _executable_book(
            put_row,
            decision_ns=decision_ns,
            maximum_age_seconds=ENTRY_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if call_book is None or put_book is None or call_row is None or put_row is None:
            self.state["engine_status"] = "waiting_for_fresh_atm_call_put_books"
            return
        cycle_id = f"{now.date().isoformat()}_{series}_{strike:g}"
        for strategy_id in STRATEGY_IDS:
            for instrument, book, row in (
                (call, call_book, call_row),
                (put, put_book, put_row),
            ):
                self._record_ideal_trade(
                    strategy_id=strategy_id,
                    instrument_type="option",
                    product="TXO",
                    code=instrument.code,
                    delta_contracts=1,
                    price_points=book[1],
                    decision_ns=decision_ns,
                    receive_ns=int(row["receive_ts_ns"]),
                    reason="open_atm_straddle_hold_to_weekly_expiry",
                    series=series,
                    strike=strike,
                    option_right=instrument.right,
                )
        self.state["active_cycle"] = {
            "cycle_id": cycle_id,
            "entry_date": now.date().isoformat(),
            "expiry_date": expiry.isoformat(),
            "series": series,
            "strike": strike,
            "call_code": call.code,
            "put_code": put.code,
            "call_entry_ask": call_book[1],
            "put_entry_ask": put_book[1],
            "status": "open",
        }
        self.state["pending_targets"] = {}
        self.state["engine_status"] = "cycle_open"
        self._persist_state()
        # The one physical simulation straddle is shared by all seven shadow
        # ledgers.  Repeating it seven times would only inflate the net account.
        self._submit_broker_order(
            code=call.code,
            delta_contracts=1,
            strategy_id="shared_option_position",
            reason="shared_seven_strategy_atm_call_entry",
        )
        self._submit_broker_order(
            code=put.code,
            delta_contracts=1,
            strategy_id="shared_option_position",
            reason="shared_seven_strategy_atm_put_entry",
        )
        _append_jsonl(
            self.events_path,
            {
                "event": "cycle_opened",
                "at_utc": _now_iso(),
                **self.state["active_cycle"],
            },
        )

    def _execute_future_target(
        self,
        *,
        strategy_id: str,
        target: int,
        decision_ns: int,
        reason: str,
    ) -> bool:
        ledger = self.state["strategies"][strategy_id]
        current = int(ledger["futures_position"])
        change = int(target) - current
        if change == 0:
            return True
        book = self._hedge_book(decision_ns)
        row = self.latest_books.get(self.hedge.code)
        if book is None or row is None:
            return False
        price = book[1] if change > 0 else book[0]
        self._record_ideal_trade(
            strategy_id=strategy_id,
            instrument_type="future",
            product="MTX",
            code=self.hedge.code,
            delta_contracts=change,
            price_points=price,
            decision_ns=decision_ns,
            receive_ns=int(row["receive_ts_ns"]),
            reason=reason,
        )
        self._persist_state()
        self._submit_broker_order(
            code=self.hedge.code,
            delta_contracts=change,
            strategy_id=strategy_id,
            reason=reason,
        )
        return True

    def _maybe_execute_pending_targets(self, now: datetime, decision_ns: int) -> None:
        if not (datetime_time(8, 45) <= now.time() < datetime_time(9, 5)):
            return
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        pending = dict(self.state.get("pending_targets") or {})
        changed = False
        for strategy_id, signal in pending.items():
            if str(signal.get("cycle_id")) != str(cycle["cycle_id"]):
                raise RuntimeError("pending target cycle identity mismatch")
            if date.fromisoformat(str(signal["decision_date"])) >= now.date():
                continue
            if self._execute_future_target(
                strategy_id=strategy_id,
                target=int(signal["target_contracts"]),
                decision_ns=decision_ns,
                reason="prior_close_model_delta_target_at_next_open",
            ):
                self.state["pending_targets"].pop(strategy_id, None)
                changed = True
        if changed:
            self._persist_state()

    def _surface_quotes(self) -> list[BidAskSurfaceQuote]:
        output: list[BidAskSurfaceQuote] = []
        for item in self.options:
            row = self.latest_books.get(item.code)
            if row is None:
                continue
            bid = _book_price(row, "bid")
            ask = _book_price(row, "ask")
            if bid is None or ask is None:
                continue
            output.append(
                BidAskSurfaceQuote(
                    series=item.series,
                    expiry=item.expiry,
                    strike=item.strike,
                    option_right=item.right,
                    bid_price=bid,
                    ask_price=ask,
                    receive_ts_ns=int(row["receive_ts_ns"]),
                )
            )
        return output

    def _maybe_calibrate(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if (
            not cycle
            or self.state.get("last_calibration_date") == now.date().isoformat()
        ):
            return
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        if expiry <= now.date() or not (
            self.calibration_time <= now.time() < datetime_time(13, 30)
        ):
            return
        forward_row = self.latest_books.get(self.underlying.code)
        forward_book = _executable_book(
            forward_row,
            decision_ns=decision_ns,
            maximum_age_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
            require_one_lot=True,
        )
        if forward_book is None or forward_row is None:
            self.state["engine_status"] = "waiting_for_calibration_forward_book"
            return
        surface = build_bidask_iv_surface(
            self._surface_quotes(),
            calibration_decision_ns=decision_ns,
            forward_bid=forward_book[0],
            forward_ask=forward_book[1],
            forward_receive_ts_ns=int(forward_row["receive_ts_ns"]),
            maximum_staleness_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
        )
        expiry_ns = int(
            datetime.combine(expiry, datetime_time(13, 30), tzinfo=TAIPEI).timestamp()
            * 1e9
        )
        years = (expiry_ns - decision_ns) / 1e9 / SECONDS_PER_YEAR
        if years <= 0.0:
            return
        for model_id in VOLATILITY_MODEL_IDS:
            fitted = fit_volatility_model(
                surface,
                model_id=model_id,
                held_series=str(cycle["series"]),
            )
            delta = fitted.straddle_delta(
                forward=surface.forward,
                strike=float(cycle["strike"]),
                years_to_expiry=years,
            )
            target = _round_nearest_contract(
                -delta * OPTION_MULTIPLIER / HEDGE_MULTIPLIER
            )
            strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
            self.state["pending_targets"][strategy_id] = {
                "cycle_id": cycle["cycle_id"],
                "decision_date": now.date().isoformat(),
                "decision_ts_ns": decision_ns,
                "target_contracts": target,
                "straddle_delta": delta,
            }
            _append_jsonl(
                self.calibrations_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "recorded_at_utc": _now_iso(),
                    "trading_date": now.date().isoformat(),
                    "cycle_id": cycle["cycle_id"],
                    "series": cycle["series"],
                    "strike": cycle["strike"],
                    "model_id": model_id,
                    "model_label": VOLATILITY_MODEL_LABELS[model_id],
                    "implementation_level": VOLATILITY_MODEL_IMPLEMENTATION[model_id],
                    "surface_forward_mid": surface.forward,
                    "surface_points": len(surface.points),
                    "surface_maturities": surface.maturity_count,
                    "target_contracts": target,
                    "straddle_delta": delta,
                    **fitted.diagnostics(),
                },
            )
        self.state["last_calibration_date"] = now.date().isoformat()
        self.state["engine_status"] = "calibrated_waiting_next_open"
        self._persist_state()

    def _maybe_flatten_expiry_hedges(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        option_expiry_today = (
            date.fromisoformat(str(cycle["expiry_date"])) == now.date()
        )
        hedge_expiry_today = self.hedge.last_trading_date == now.date()
        if not option_expiry_today and not hedge_expiry_today:
            return
        if not (datetime_time(13, 24) <= now.time() < datetime_time(13, 30)):
            return
        all_flat = True
        for model_id in VOLATILITY_MODEL_IDS:
            strategy_id = f"{MODEL_VARIANT_PREFIX}{model_id}"
            if not self._execute_future_target(
                strategy_id=strategy_id,
                target=0,
                decision_ns=decision_ns,
                reason="expiry_day_mtx_flatten_before_option_settlement",
            ):
                all_flat = False
        if all_flat and option_expiry_today:
            cycle["status"] = "waiting_official_final_settlement"
            self.state["pending_targets"] = {}
            self.state["engine_status"] = "waiting_official_final_settlement"
            self._persist_state()
        elif all_flat and hedge_expiry_today:
            self.state["engine_status"] = "hedge_contract_flat_for_monthly_roll"
            self._persist_state()

    def _official_settlement(self, expiry: date) -> tuple[float, str, str] | None:
        if not self.final_settlement_path.is_file():
            return None
        frame = pl.read_parquet(self.final_settlement_path).filter(
            pl.col("settlement_date") == expiry
        )
        if frame.height != 1:
            return None
        row = frame.row(0, named=True)
        return (
            float(row["final_settlement_price"]),
            str(row.get("source_file", "")),
            str(row.get("source_sha256", "")),
        )

    def _maybe_settle_expired_cycle(self, now: datetime, decision_ns: int) -> None:
        cycle = self.state.get("active_cycle")
        if not cycle:
            return
        expiry = date.fromisoformat(str(cycle["expiry_date"]))
        if expiry >= now.date():
            return
        if any(
            int(
                self.state["strategies"][f"{MODEL_VARIANT_PREFIX}{model_id}"][
                    "futures_position"
                ]
            )
            != 0
            for model_id in VOLATILITY_MODEL_IDS
        ):
            raise RuntimeError(
                "cannot cash-settle cycle while a shadow MTX hedge remains open"
            )
        settlement = self._official_settlement(expiry)
        if settlement is None:
            self.state["engine_status"] = "blocked_missing_official_final_settlement"
            self.state["blocked_reason"] = f"missing_official_final_settlement:{expiry}"
            return
        settlement_price, source_file, source_sha = settlement
        strike = float(cycle["strike"])
        terminal = {
            "C": max(settlement_price - strike, 0.0),
            "P": max(strike - settlement_price, 0.0),
        }
        for strategy_id in STRATEGY_IDS:
            for right, code in (
                ("C", str(cycle["call_code"])),
                ("P", str(cycle["put_code"])),
            ):
                price = terminal[right]
                tax = (
                    option_cash_settlement_transaction_tax_twd(
                        settlement_price,
                        settlement_date=expiry,
                        multiplier_twd_per_point=OPTION_MULTIPLIER,
                    )
                    if price > 0.0
                    else 0.0
                )
                self._record_ideal_trade(
                    strategy_id=strategy_id,
                    instrument_type="option",
                    product="TXO",
                    code=code,
                    delta_contracts=-1,
                    price_points=price,
                    decision_ns=decision_ns,
                    receive_ns=decision_ns,
                    reason="official_taifex_final_cash_settlement",
                    series=str(cycle["series"]),
                    strike=strike,
                    option_right=right,
                    fee_twd=0.0,
                    tax_twd=tax,
                    price_source="official_taifex_final_settlement_intrinsic_value",
                )
        _append_jsonl(
            self.events_path,
            {
                "event": "cycle_cash_settled",
                "at_utc": _now_iso(),
                "cycle_id": cycle["cycle_id"],
                "expiry_date": expiry.isoformat(),
                "official_final_settlement": settlement_price,
                "source_file": source_file,
                "source_sha256": source_sha,
            },
        )
        self.state["active_cycle"] = None
        self.state["pending_targets"] = {}
        self.state["last_settled_expiry"] = expiry.isoformat()
        self.state["blocked_reason"] = None
        self.state["engine_status"] = "flat_ready_for_next_cycle"
        self._persist_state()

    def _strategy_mark(self, strategy_id: str, decision_ns: int) -> dict[str, Any]:
        ledger = self.state["strategies"][strategy_id]
        open_value = 0.0
        cycle = self.state.get("active_cycle")
        option_books_valid = False
        if cycle:
            call_row = self.latest_books.get(str(cycle["call_code"]))
            put_row = self.latest_books.get(str(cycle["put_code"]))
            call_book = _executable_book(
                call_row,
                decision_ns=decision_ns,
                maximum_age_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
                require_one_lot=False,
            )
            put_book = _executable_book(
                put_row,
                decision_ns=decision_ns,
                maximum_age_seconds=SURFACE_BOOK_MAX_AGE_SECONDS,
                require_one_lot=False,
            )
            if call_book and put_book:
                open_value += (call_book[0] + put_book[0]) * OPTION_MULTIPLIER
                option_books_valid = True
        future_position = int(ledger["futures_position"])
        future_book = self._hedge_book(decision_ns)
        future_mark_valid = future_position == 0 or future_book is not None
        if future_position and future_book:
            liquidation = future_book[0] if future_position > 0 else future_book[1]
            open_value += future_position * liquidation * HEDGE_MULTIPLIER
        net_equity = (
            float(ledger["gross_cash_twd"])
            + open_value
            - float(ledger["fees_twd"])
            - float(ledger["tax_twd"])
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "recorded_at_utc": _now_iso(),
            "decision_ts_ns": decision_ns,
            "strategy_id": strategy_id,
            "gross_cash_twd": float(ledger["gross_cash_twd"]),
            "open_liquidation_value_twd": open_value,
            "fixed_fees_twd": float(ledger["fees_twd"]),
            "transaction_tax_twd": float(ledger["tax_twd"]),
            "net_equity_twd": net_equity,
            "futures_position": future_position,
            "active_cycle_id": cycle.get("cycle_id") if cycle else None,
            "option_books_valid": option_books_valid if cycle else True,
            "future_book_valid": future_mark_valid,
            "mark_price_policy": "long_options_at_best_bid_and_signed_mtx_at_liquidation_side",
        }

    def _write_marks(self, decision_ns: int) -> None:
        for strategy_id in STRATEGY_IDS:
            _append_jsonl(
                self.marks_path, self._strategy_mark(strategy_id, decision_ns)
            )

    def _write_status(
        self, *, force: bool = False, decision_ns: int | None = None
    ) -> None:
        now_monotonic = time.monotonic()
        if (
            not force
            and now_monotonic - self.last_status_monotonic < STATUS_INTERVAL_SECONDS
        ):
            return
        timestamp_ns = int(decision_ns or time.time_ns())
        payload = {
            "schema_version": SCHEMA_VERSION,
            "updated_at_utc": _now_iso(),
            "simulation_only": True,
            "production_order_possible": False,
            "engine_status": self.state.get("engine_status"),
            "blocked_reason": self.state.get("blocked_reason"),
            "bootstrap_after_date": self.state.get("bootstrap_after_date"),
            "active_cycle": self.state.get("active_cycle"),
            "pending_targets": self.state.get("pending_targets"),
            "broker_orders_enabled": self.broker_orders_enabled,
            "broker_order_failures": int(self.state.get("broker_order_failures", 0)),
            "inflight_order_count": len(self.state.get("inflight_orders") or {}),
            "underlying_contract": self.underlying.code,
            "hedge_contract": self.hedge.code,
            "option_contract_count": len(self.options),
            "latest_book_count": len(self.latest_books),
            "strategies": {
                strategy_id: self._strategy_mark(strategy_id, timestamp_ns)
                for strategy_id in STRATEGY_IDS
            },
        }
        _atomic_json(self.status_path, payload)
        self.last_status_monotonic = now_monotonic

    def step(self, *, now: datetime | None = None) -> None:
        observed_now = now or datetime.now(TAIPEI)
        decision_ns = time.time_ns()
        self._drain_callbacks()
        try:
            self._maybe_settle_expired_cycle(observed_now, decision_ns)
            self._maybe_open_cycle(observed_now, decision_ns)
            self._maybe_execute_pending_targets(observed_now, decision_ns)
            self._maybe_flatten_expiry_hedges(observed_now, decision_ns)
            self._maybe_calibrate(observed_now, decision_ns)
        except Exception as exc:
            self.state["engine_status"] = "blocked"
            self.state["blocked_reason"] = f"{type(exc).__name__}: {exc}"
            self._persist_state()
            _append_jsonl(
                self.events_path,
                {
                    "event": "engine_step_blocked",
                    "at_utc": _now_iso(),
                    "error": self.state["blocked_reason"],
                },
            )
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_mark_monotonic >= MARK_INTERVAL_SECONDS:
            self._write_marks(decision_ns)
            self.last_mark_monotonic = now_monotonic
        self._write_status(decision_ns=decision_ns)

    def close(self) -> None:
        self._drain_callbacks()
        self._write_status(force=True)
        self._persist_state()
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.lock_handle.close()


__all__ = [
    "CLASSIC_VARIANT_ID",
    "MODEL_VARIANT_PREFIX",
    "OptionInstrument",
    "STRATEGY_IDS",
    "TaifexVolatilitySimulation",
    "FuturesInstrument",
    "option_instruments",
]
