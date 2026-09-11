"""Durable Taiwan close-to-next-open paper execution.

The model and execution clocks are intentionally separate.  A temporary
``tw_day_trade`` checkpoint may produce the 13:25 target weights, but this
engine only submits legal-limit closing-auction orders, recognizes a fill from
an actual (non-simulated) closing print, carries that cohort overnight, and
closes it only after the next valid session's actual opening print.

No broker order API is called.  Full auction quantity is an explicit paper
assumption because a level-one snapshot cannot prove queue allocation.
"""

from __future__ import annotations

from datetime import datetime, time
import json
import math
import os
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

import numpy as np

from stockagent.backtest.tw_execution import (
    commission_rebate_rate_vector,
    gross_fee_rate_vectors,
)
from stockagent.live.tw_day_trade_simulation import (
    ModeSpec,
    TAIPEI,
    TwDayTradeSimulationEngine,
    _atomic_json,
    _finite,
    _now_taipei,
    _parse_timestamp,
    position_net_liquidation_pnl,
)


CLOSE_ORDER_GATE: Final[time] = time(13, 25)
REGULAR_CLOSE: Final[time] = time(13, 30)
DELAYED_CLOSE_DEADLINE: Final[time] = time(13, 33, 59, 999_999)
OPEN_ORDER_GATE: Final[time] = time(8, 30)
OPEN_MATCH_GATE: Final[time] = time(9, 0)
OPEN_OBSERVATION_DEADLINE: Final[time] = time(9, 10)
OVERNIGHT_CONTRACT_VERSION: Final[int] = 1


def _clock(value: datetime) -> time:
    return value.timetz().replace(tzinfo=None)


def _auction_print(
    quote: Mapping[str, Any],
    *,
    session_date: str,
    not_before: time,
    not_after: time | None = None,
    price_field: str,
) -> tuple[float, datetime] | None:
    """Return a venue-timestamped non-simulated auction print or fail closed."""

    # Unknown is not equivalent to a real trade.  Shioaji exposes this flag on
    # stock snapshots; a provider or schema that omits it cannot prove an
    # auction fill and must wait/fail closed.
    if quote.get("simtrade") is not False:
        return None
    price = _finite(quote.get(price_field))
    exchange_at = _parse_timestamp(quote.get("exchange_quote_at"))
    if price is None or exchange_at is None:
        return None
    if exchange_at.date().isoformat() != session_date:
        return None
    if _clock(exchange_at) < not_before:
        return None
    if not_after is not None and _clock(exchange_at) > not_after:
        return None
    return price, exchange_at


def _inside_limits(price: float, lower: float | None, upper: float | None) -> bool:
    return bool(
        lower is not None
        and upper is not None
        and lower <= float(price) <= upper
    )


class TwOvernightSimulationEngine(TwDayTradeSimulationEngine):
    """One close-entry cohort per mode, mandatorily due at the next open."""

    def __init__(self, state_dir: Path) -> None:
        super().__init__(state_dir)
        self.state["product"] = "tw_overnight"
        self.state["overnight_contract_version"] = OVERNIGHT_CONTRACT_VERSION
        # A brand-new product has a valid zero-row ledger before its first
        # 13:25 decision.  Materialize those append-only files immediately so
        # bounded public readers return an empty page instead of treating
        # normal first-day state as a missing-source failure.
        for path in (
            self.signals_path,
            self.orders_path,
            self.fills_path,
            self.marks_path,
            self.events_path,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
            os.close(descriptor)

    @staticmethod
    def _ordinary_fee_rates(
        spec: ModeSpec,
        *,
        symbol: str,
        security_type: str,
    ) -> tuple[float, float, float]:
        buy, sell = gross_fee_rate_vectors(
            [symbol],
            "tw_overnight",
            fee_schedule=spec.fee_schedule,
            security_types=[security_type],
        )
        rebate = commission_rebate_rate_vector(
            [symbol],
            "tw_overnight",
            fee_schedule=spec.fee_schedule,
            security_types=[security_type],
        )
        return float(buy[0]), float(sell[0]), float(rebate[0])

    def _mode(self, spec: ModeSpec) -> dict[str, Any]:
        mode = super()._mode(spec)
        mode["product"] = "tw_overnight"
        mode["model_adapter"] = "tw_day_trade_checkpoint_at_13_25"
        mode["model_trained_for_overnight"] = False
        mode.setdefault("pending_entry_orders", {})
        return mode

    def record_close_signal_latency_sample(
        self,
        *,
        market: str,
        signal_id: str,
        result: str,
        summary: Mapping[str, Any],
        consumer_detected_at: datetime,
        ledger_persisted_at: datetime,
        executor_quote_fetch_ms: float,
        security_metadata_load_ms: float,
        ledger_compute_persist_ms: float,
        shared_quote_batch_mode_count: int,
    ) -> None:
        """Persist the 13:25 input-to-paper-ledger critical path."""

        started_at = _parse_timestamp(
            summary.get("signal_started_at") or summary.get("generated_at")
        )
        ready_at = _parse_timestamp(
            summary.get("signal_ready_at")
            or summary.get("artifact_published_at")
            or summary.get("generated_at")
        )
        published_at = _parse_timestamp(
            summary.get("artifact_published_at") or summary.get("signal_ready_at")
        )
        live_latency = dict(summary.get("live_latency") or {})

        def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
            if start is None or end is None:
                return None
            return round(max(0.0, (end - start).total_seconds() * 1_000.0), 3)

        stages = {
            "signal_pre_quote_prepare_ms": _finite(
                live_latency.get("pre_quote_prepare_ms")
            ),
            "signal_quote_fetch_ms": _finite(live_latency.get("quote_fetch_ms")),
            "signal_pre_inference_prepare_ms": _finite(
                live_latency.get("pre_inference_prepare_ms")
            ),
            "model_inference_ms": _finite(live_latency.get("model_inference_ms")),
            "signal_post_inference_format_ms": _finite(
                live_latency.get("post_inference_format_ms")
            ),
            "artifact_publish_ms": _finite(live_latency.get("artifact_publish_ms")),
            "artifact_discovery_ms": elapsed_ms(published_at, consumer_detected_at),
            "executor_shared_quote_fetch_ms": round(
                max(0.0, float(executor_quote_fetch_ms)), 3
            ),
            "security_metadata_load_ms": round(
                max(0.0, float(security_metadata_load_ms)), 3
            ),
            "ledger_compute_persist_ms": round(
                max(0.0, float(ledger_compute_persist_ms)), 3
            ),
        }
        finite_stages = {
            name: float(value)
            for name, value in stages.items()
            if value is not None and math.isfinite(float(value))
        }
        bottleneck = (
            max(finite_stages, key=finite_stages.get) if finite_stages else None
        )
        local_persisted_at = ledger_persisted_at.astimezone(TAIPEI)
        decision_gate = local_persisted_at.replace(
            hour=13,
            minute=25,
            second=0,
            microsecond=0,
        )
        decision_delay_ms = max(
            0.0, (local_persisted_at - decision_gate).total_seconds() * 1_000.0
        )
        self._append_ledger(
            self.latency_path,
            {
                "schema_version": 1,
                "recorded_at": ledger_persisted_at.isoformat(timespec="microseconds"),
                "session_date": local_persisted_at.date().isoformat(),
                "market": market,
                "signal_id": signal_id,
                "result": result,
                "simulation_only": True,
                "measurement_boundary": "signal_input_to_simulation_ledger_persisted",
                "decision_clock": "13:25 close-auction target",
                "signal_started_at": (
                    started_at.isoformat(timespec="microseconds")
                    if started_at
                    else None
                ),
                "signal_ready_at": (
                    ready_at.isoformat(timespec="microseconds") if ready_at else None
                ),
                "artifact_published_at": (
                    published_at.isoformat(timespec="microseconds")
                    if published_at
                    else None
                ),
                "consumer_detected_at": consumer_detected_at.isoformat(
                    timespec="microseconds"
                ),
                "ledger_persisted_at": ledger_persisted_at.isoformat(
                    timespec="microseconds"
                ),
                "input_to_ledger_ms": elapsed_ms(started_at, ledger_persisted_at),
                "ready_to_ledger_ms": elapsed_ms(ready_at, ledger_persisted_at),
                "decision_gate_to_ledger_ms": round(decision_delay_ms, 3),
                "shared_quote_batch_mode_count": max(
                    0, int(shared_quote_batch_mode_count)
                ),
                "stages": stages,
                "bottleneck_stage": bottleneck,
                "bottleneck_ms": finite_stages.get(bottleneck) if bottleneck else None,
            },
        )

    def update_readiness(
        self,
        specs: Sequence[ModeSpec],
        *,
        now: datetime | None = None,
        errors: Mapping[str, str] | None = None,
    ) -> None:
        observed = _now_taipei(now)
        self.state["enabled_markets"] = [str(spec.market) for spec in specs]
        enabled = set(self.state["enabled_markets"])
        for market, existing in (self.state.get("modes") or {}).items():
            if isinstance(existing, dict):
                existing["configured_enabled"] = str(market) in enabled
        for spec in specs:
            mode = self._mode(spec)
            mode["configured_enabled"] = True
            checkpoint = Path(spec.checkpoint_path) if spec.checkpoint_path else None
            mode["checkpoint_ready"] = bool(checkpoint and checkpoint.is_file())
            mode["readiness_error"] = (errors or {}).get(spec.market)
            open_count = sum(
                int(position.get("signed_shares") or 0) != 0
                for position in (mode.get("positions") or {}).values()
            )
            pending = sum(
                row.get("status") == "working"
                for row in (mode.get("pending_entry_orders") or {}).values()
            )
            if mode.get("readiness_error"):
                mode["engine_status"] = "blocked_readiness"
            elif not mode["checkpoint_ready"]:
                mode["engine_status"] = "blocked_missing_checkpoint"
            elif open_count:
                missed_open = any(
                    str(position.get("opening_exit_order_status") or "")
                    == "missed_actual_opening_print"
                    and str(position.get("opening_exit_order_session_date") or "")
                    == observed.date().isoformat()
                    for position in (mode.get("positions") or {}).values()
                )
                mode["engine_status"] = (
                    "critical_actual_opening_print_missing"
                    if missed_open
                    else "carrying_to_next_open"
                )
            elif pending:
                mode["engine_status"] = "waiting_close_auction_match"
            elif observed.weekday() >= 5:
                mode["engine_status"] = "waiting_trading_day"
            elif _clock(observed) < CLOSE_ORDER_GATE:
                mode["engine_status"] = "waiting_13_25_signal"
            elif _clock(observed) < REGULAR_CLOSE:
                mode["engine_status"] = "waiting_signal"
            else:
                mode["engine_status"] = "session_complete"
        self._persist(observed)

    def register_close_signal(
        self,
        *,
        spec: ModeSpec,
        summary: Mapping[str, Any],
        signal_rows: Sequence[Mapping[str, Any]],
        quotes: Mapping[str, Mapping[str, Any]],
        security_types: Mapping[str, str] | None = None,
        now: datetime | None = None,
    ) -> str:
        observed = _now_taipei(now)
        mode = self._mode(spec)
        signal_id = str(summary.get("signal_id") or "").strip()
        if not signal_id:
            raise ValueError("signal summary has no signal_id")
        if signal_id in set(mode.get("processed_signal_ids") or ()):
            return "already_processed"
        if str(summary.get("execution_mode") or "") != "tw_day_trade":
            return self._block_signal(mode, signal_id, "not_tw_day_trade_adapter", observed)
        if not bool(summary.get("live_session_latest_quote_feature_applied")):
            return self._block_signal(mode, signal_id, "latest_quote_feature_missing", observed)
        signal_at = _parse_timestamp(
            summary.get("signal_ready_at")
            or summary.get("artifact_published_at")
            or summary.get("generated_at")
        )
        if signal_at is None or signal_at.date() != observed.date():
            return self._block_signal(mode, signal_id, "signal_not_current_session", observed)
        if not CLOSE_ORDER_GATE <= _clock(observed) < REGULAR_CLOSE:
            return self._block_signal(mode, signal_id, "outside_13_25_close_order_window", observed)
        if any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            return self._block_signal(
                mode,
                signal_id,
                "prior_overnight_cohort_still_open",
                observed,
                engine_status="critical_prior_overnight_cohort_open",
            )
        if any(
            row.get("status") == "working"
            for row in (mode.get("pending_entry_orders") or {}).values()
        ):
            return self._block_signal(mode, signal_id, "close_orders_already_working", observed)

        prior_session = str(mode.get("session_date") or "")
        if prior_session and prior_session != observed.date().isoformat():
            self._archive_mode_positions(mode, archived_at=observed)
        session_date = observed.date().isoformat()
        mode.update(
            {
                "session_date": session_date,
                "session_valid": True,
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": str(summary.get("signal_started_at") or "") or None,
                "signal_registered_at": observed.isoformat(timespec="seconds"),
                "signal_source_path": summary.get("summary_path"),
                "target_weights_path": summary.get("weights_path"),
                "target_positions_path": summary.get("weights_path"),
                "target_symbol_count": summary.get("symbol_count") or len(signal_rows),
                "target_risk": dict(summary.get("target_risk") or {}),
                "feature_cutoff_date": summary.get("feature_cutoff_date"),
                "checkpoint_fingerprint": summary.get("checkpoint_fingerprint"),
                "config_fingerprint": summary.get("config_fingerprint"),
                "entry_fill_contract": "actual_close_auction_price_after_13_30",
                "entry_liquidity_assumption": "full_paper_quantity_no_queue_allocation_claim",
                "entry_completed_at": None,
                "pending_entry_orders": {},
                "positions": {},
                "blocked_reason": None,
            }
        )
        counts: dict[str, int] = {}
        signal_records: list[dict[str, Any]] = []
        order_records: list[dict[str, Any]] = []
        security_types = security_types or {}
        for raw_row in signal_rows:
            row = dict(raw_row)
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                continue
            target_weight = float(row.get("target_weight") or 0.0)
            side = "long" if target_weight > 0 else "short" if target_weight < 0 else "flat"
            quote = dict(quotes.get(symbol) or {})
            sizing_price = _finite(row.get("current_price")) or _finite(quote.get("last"))
            upper = _finite(quote.get("upper_limit"))
            lower = _finite(quote.get("lower_limit"))
            status = "working"
            reason: str | None = None
            requested_shares = 0
            if side == "flat":
                status, reason = "hold", "zero_target_weight"
            elif not bool(row.get("tradable")):
                status, reason = "blocked", "model_tradable_mask_false"
            elif side == "long" and not bool(row.get("can_buy")):
                status, reason = "blocked", "buy_not_allowed"
            elif side == "short" and not bool(row.get("overnight_can_short_open")):
                status, reason = "blocked", "ordinary_short_open_not_allowed"
            elif sizing_price is None:
                status, reason = "blocked", "13_25_sizing_price_missing"
            elif lower is None or upper is None:
                status, reason = "blocked", "same_session_price_limits_missing"
            else:
                requested_shares = int(
                    math.floor(
                        abs(target_weight)
                        * float(spec.initial_capital_twd)
                        / sizing_price
                        / int(spec.lot_size)
                    )
                ) * int(spec.lot_size)
                if requested_shares <= 0:
                    status, reason = "blocked", "target_below_one_board_lot"
                elif side == "short":
                    capacity = row.get("overnight_short_capacity_shares")
                    try:
                        short_capacity = int(float(capacity))
                    except (TypeError, ValueError, OverflowError):
                        short_capacity = -1
                    if short_capacity < requested_shares:
                        status, reason = (
                            "blocked",
                            "short_inventory_missing_or_below_full_model_target",
                        )

            security_type = str(security_types.get(symbol) or "stock")
            if security_type not in {"stock", "etf"}:
                security_type = "stock"
            limit_price = upper if side == "long" else lower if side == "short" else None
            record = {
                "recorded_at": observed.isoformat(timespec="seconds"),
                "session_date": session_date,
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": summary.get("signal_started_at"),
                "symbol": symbol,
                "name": row.get("name"),
                "side": side,
                "action": row.get("action"),
                "score": row.get("score"),
                "raw_score": row.get("raw_score"),
                "target_weight": target_weight,
                "requested_shares": requested_shares,
                "filled_shares": 0,
                "execution_price": None,
                "sizing_open_price": sizing_price,
                "sizing_price_at_13_25": sizing_price,
                "order_limit_price": limit_price,
                "upper_limit": upper,
                "lower_limit": lower,
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "quote_at": quote.get("quote_at"),
                "exchange_quote_at": quote.get("exchange_quote_at"),
                "simtrade": quote.get("simtrade"),
                "status": status,
                "reason": reason,
                "security_type": security_type,
                "temporary_day_trade_model_adapter": True,
                "model_trained_for_overnight": False,
            }
            signal_records.append(record)
            counts[status] = counts.get(status, 0) + 1
            if status != "working" or limit_price is None:
                continue
            buy_rate, sell_rate, rebate_rate = self._ordinary_fee_rates(
                spec,
                symbol=symbol,
                security_type=security_type,
            )
            order_id = f"{spec.market}:{session_date}:{symbol}:close-entry"
            order = {
                **record,
                "order_id": order_id,
                "purpose": "close_auction_entry",
                "side": "buy" if side == "long" else "sell_short",
                "order_type": "LMT_ROD",
                "price": limit_price,
                "quantity": requested_shares,
                "lot_size": int(spec.lot_size),
                "status": "working",
                "simulation_only": True,
                "production_order_possible": False,
                "buy_fee_rate": buy_rate,
                "sell_fee_rate": sell_rate,
                "commission_rebate_rate": rebate_rate,
            }
            mode["pending_entry_orders"][order_id] = order
            order_records.append(order)

        self._event(
            "signal_commit_started",
            recorded_at=observed,
            market=spec.market,
            signal_id=signal_id,
            session_date=session_date,
        )
        self.begin_deferred_ledger_writes()
        try:
            for row in signal_records:
                self._append_ledger(self.signals_path, row)
            for row in order_records:
                self._order(row)
            self._event(
                "signal_registered",
                recorded_at=observed,
                market=spec.market,
                signal_id=signal_id,
                session_date=session_date,
                working_orders=len(order_records),
                product="tw_overnight",
            )
            self.flush_deferred_ledger_writes()
        except Exception:
            self._deferred_ledger_rows = None
            raise
        processed = list(mode.get("processed_signal_ids") or ())
        processed.append(signal_id)
        mode["processed_signal_ids"] = processed[-512:]
        mode["signal_counts"] = counts
        mode["entry_requested_shares"] = sum(
            int(row.get("quantity") or 0) for row in order_records
        )
        mode["entry_filled_shares"] = 0
        mode["entry_unfilled_shares"] = mode["entry_requested_shares"]
        mode["entry_fill_count"] = 0
        mode["entry_fill_outcome"] = (
            "waiting_close_auction" if order_records else "no_executable_signal"
        )
        mode["engine_status"] = (
            "waiting_close_auction_match" if order_records else "flat_no_executable_signal"
        )
        self._persist(observed)
        return "registered"

    def _settle_close_entries(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> bool:
        session_date = str(mode.get("session_date") or "")
        if not session_date or now.date().isoformat() != session_date:
            return False
        changed = False
        pending = mode.get("pending_entry_orders") or {}
        for order_id, order in pending.items():
            if order.get("status") != "working":
                continue
            symbol = str(order.get("symbol") or "")
            quote = dict(quotes.get(symbol) or {})
            matched = _auction_print(
                quote,
                session_date=session_date,
                not_before=REGULAR_CLOSE,
                not_after=DELAYED_CLOSE_DEADLINE,
                price_field="last",
            )
            if matched is None:
                if _clock(now) > DELAYED_CLOSE_DEADLINE:
                    order.update(
                        {
                            "status": "expired_without_actual_close_print",
                            "expired_at": now.isoformat(timespec="seconds"),
                            "match_wait_reason": "no_proven_closing_auction_fill",
                        }
                    )
                    self._order(
                        {**order, "recorded_at": now.isoformat(timespec="seconds")}
                    )
                    changed = True
                    continue
                wait_reason = (
                    "simulated_indicative_match_not_a_fill"
                    if quote.get("simtrade") is True
                    else "actual_close_auction_print_unproven"
                )
                if order.get("match_wait_reason") != wait_reason:
                    order["last_match_check_at"] = now.isoformat(timespec="seconds")
                    order["match_wait_reason"] = wait_reason
                    changed = True
                continue
            price, exchange_at = matched
            lower = _finite(order.get("lower_limit"))
            upper = _finite(order.get("upper_limit"))
            if not _inside_limits(price, lower, upper):
                order["status"] = "blocked_close_outside_legal_limits"
                self._order(
                    {**order, "recorded_at": now.isoformat(timespec="seconds")}
                )
                changed = True
                continue
            quantity = int(order.get("quantity") or 0)
            side = str(order.get("side") or "")
            position_side = "long" if side == "buy" else "short"
            signed = quantity if position_side == "long" else -quantity
            buy_rate = float(order.get("buy_fee_rate") or 0.0)
            sell_rate = float(order.get("sell_fee_rate") or 0.0)
            rebate_rate = float(order.get("commission_rebate_rate") or 0.0)
            entry_rate = buy_rate if position_side == "long" else sell_rate
            gross_fee = quantity * price * entry_rate
            rebate = quantity * price * rebate_rate
            entry_fee = gross_fee - rebate
            position_id = f"{market}:{session_date}:{symbol}"
            position = {
                "position_id": position_id,
                "market": market,
                "session_date": session_date,
                "signal_id": mode.get("signal_id"),
                "signal_at": mode.get("signal_at"),
                "source_signal_at": mode.get("source_signal_at"),
                "symbol": symbol,
                "name": order.get("name"),
                "side": position_side,
                "target_weight": order.get("target_weight"),
                "requested_shares": quantity,
                "filled_shares": quantity,
                "signed_shares": signed,
                "lot_size": int(order.get("lot_size") or 1_000),
                "entry_order_id": order_id,
                "entry_at": now.isoformat(timespec="seconds"),
                "entry_quote_at": quote.get("quote_at"),
                "entry_exchange_at": exchange_at.isoformat(timespec="milliseconds"),
                "entry_price": price,
                "entry_price_source": "actual_close_auction_print",
                "entry_fee_twd": entry_fee,
                "remaining_entry_fee_twd": entry_fee,
                "entry_gross_fee_and_tax_twd": gross_fee,
                "entry_commission_rebate_accrued_twd": rebate,
                "buy_fee_rate": buy_rate,
                "sell_fee_rate": sell_rate,
                "cash_buy_fee_rate": buy_rate,
                "cash_sell_fee_rate": sell_rate,
                "commission_rebate_rate": rebate_rate,
                "security_type": order.get("security_type"),
                "upper_limit": upper,
                "lower_limit": lower,
                "status": "open",
                "last_mark_price": price,
                "last_complete_net_pnl_twd": -entry_fee,
                "realized_gross_pnl_twd": 0.0,
                "realized_exit_fee_twd": 0.0,
                "realized_net_pnl_twd": 0.0,
                "valuation_stale": False,
                "opening_exit_order_status": "not_submitted",
                "mandatory_exit_phase": "next_valid_session_open",
                "temporary_day_trade_model_adapter": True,
            }
            mode.setdefault("positions", {})[position_id] = position
            order.update(
                {
                    "status": "filled",
                    "filled_at": now.isoformat(timespec="seconds"),
                    "exchange_match_at": exchange_at.isoformat(timespec="milliseconds"),
                    "fill_price": price,
                }
            )
            self._order({**order, "recorded_at": now.isoformat(timespec="seconds")})
            self._fill(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": session_date,
                    "market": market,
                    "position_id": position_id,
                    "order_id": order_id,
                    "symbol": symbol,
                    "purpose": "close_auction_entry",
                    "fill_at": now.isoformat(timespec="seconds"),
                    "quote_at": quote.get("quote_at"),
                    "exchange_match_at": exchange_at.isoformat(timespec="milliseconds"),
                    "quantity": quantity,
                    "price": price,
                    "fee_and_tax_twd": entry_fee,
                    "gross_fee_and_tax_twd": gross_fee,
                    "commission_rebate_accrued_twd": rebate,
                    "fill_contract": "actual_close_auction_price",
                    "depth_assumption": "full_paper_quantity_no_queue_allocation_claim",
                    "simulation_only": True,
                }
            )
            mode["cumulative_commission_rebate_accrued_twd"] = (
                float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
                + rebate
            )
            changed = True
        filled = sum(row.get("status") == "filled" for row in pending.values())
        working = sum(row.get("status") == "working" for row in pending.values())
        filled_shares = sum(
            int(row.get("quantity") or 0)
            for row in pending.values()
            if row.get("status") == "filled"
        )
        next_values = {
            "entry_fill_count": filled,
            "entry_filled_shares": filled_shares,
            "entry_unfilled_shares": max(
                0, int(mode.get("entry_requested_shares") or 0) - filled_shares
            ),
        }
        if filled:
            next_values["entry_completed_at"] = (
                mode.get("entry_completed_at") or now.isoformat(timespec="seconds")
            )
        requested_shares = int(mode.get("entry_requested_shares") or 0)
        next_values["entry_fill_outcome"] = (
            "filled_at_actual_close"
            if requested_shares > 0 and filled_shares == requested_shares
            else "partial_waiting_actual_close"
            if filled and working
            else "partial_close_auction_fill"
            if filled
            else "waiting_actual_close_print"
            if working
            else "close_auction_fill_unproven"
            if requested_shares > 0
            else "no_executable_signal"
        )
        next_values["entry_full_fill"] = bool(
            requested_shares > 0 and filled_shares == requested_shares
        )
        next_values["engine_status"] = (
            "carrying_to_next_open" if filled
            else "waiting_close_auction_match" if working
            else "critical_actual_close_print_missing" if requested_shares > 0
            else "flat_no_executable_signal"
        )
        if any(mode.get(key) != value for key, value in next_values.items()):
            mode.update(next_values)
            changed = True
        return changed

    @staticmethod
    def _expire_stale_close_orders(mode: dict[str, Any], now: datetime) -> bool:
        entry_session = str(mode.get("session_date") or "")
        if not entry_session or entry_session >= now.date().isoformat():
            return False
        changed = False
        for order in (mode.get("pending_entry_orders") or {}).values():
            if order.get("status") != "working":
                continue
            order["status"] = "expired_without_actual_close_print"
            order["expired_at"] = now.isoformat(timespec="seconds")
            order["match_wait_reason"] = "no_proven_closing_auction_fill"
            changed = True
        if not any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            mode["engine_status"] = "flat_close_orders_expired"
        return changed

    def _submit_opening_exits(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> bool:
        if not OPEN_ORDER_GATE <= _clock(now) <= OPEN_OBSERVATION_DEADLINE:
            return False
        changed = False
        session_date = now.date().isoformat()
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            if str(position.get("session_date") or "") >= session_date:
                continue
            order_status = str(position.get("opening_exit_order_status") or "")
            order_session = str(position.get("opening_exit_order_session_date") or "")
            if order_status == "filled":
                continue
            if order_status == "working" and order_session == session_date:
                continue
            if order_status == "working" and order_session != session_date:
                self._order(
                    {
                        "recorded_at": now.isoformat(timespec="seconds"),
                        "session_date": order_session or None,
                        "entry_session_date": position.get("session_date"),
                        "market": market,
                        "position_id": position.get("position_id"),
                        "order_id": position.get("opening_exit_order_id"),
                        "symbol": position.get("symbol"),
                        "purpose": "next_session_open_auction_exit",
                        "status": "expired_end_of_session_without_open_fill",
                        "simulation_only": True,
                    }
                )
                position["opening_exit_order_status"] = (
                    "expired_end_of_session_without_open_fill"
                )
                changed = True
            quote = dict(quotes.get(str(position.get("symbol") or "")) or {})
            exchange_at = _parse_timestamp(quote.get("exchange_quote_at"))
            current_session_quote = bool(
                exchange_at is not None
                and exchange_at.date().isoformat() == session_date
                and OPEN_ORDER_GATE <= _clock(exchange_at) <= OPEN_OBSERVATION_DEADLINE
            )
            lower = _finite(quote.get("lower_limit"))
            upper = _finite(quote.get("upper_limit"))
            if not current_session_quote or lower is None or upper is None:
                if position.get("opening_exit_order_status") != "waiting_current_session_limits":
                    position["opening_exit_order_status"] = "waiting_current_session_limits"
                    changed = True
                continue
            side = str(position.get("side") or "")
            price = lower if side == "long" else upper
            order_id = f"{position.get('position_id')}:{session_date}:open-exit"
            position.update(
                {
                    "opening_exit_order_id": order_id,
                    "opening_exit_order_status": "working",
                    "opening_exit_limit_price": price,
                    "opening_exit_submitted_at": now.isoformat(timespec="seconds"),
                    "opening_exit_order_session_date": session_date,
                }
            )
            self._order(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": session_date,
                    "entry_session_date": position.get("session_date"),
                    "market": market,
                    "position_id": position.get("position_id"),
                    "order_id": order_id,
                    "symbol": position.get("symbol"),
                    "purpose": "next_session_open_auction_exit",
                    "side": "sell" if side == "long" else "buy_to_cover",
                    "order_type": "LMT_ROD",
                    "price": price,
                    "quantity": abs(int(position.get("signed_shares") or 0)),
                    "status": "working",
                    "simulation_only": True,
                }
            )
            changed = True
        return changed

    def _settle_opening_exits(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> bool:
        changed = False
        closed_any = False
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0 or position.get("opening_exit_order_status") != "working":
                continue
            if str(position.get("opening_exit_order_session_date") or "") != now.date().isoformat():
                continue
            if _clock(now) > OPEN_OBSERVATION_DEADLINE:
                position.update(
                    {
                        "opening_exit_order_status": "missed_actual_opening_print",
                        "opening_match_wait_reason": "no_proven_opening_auction_fill",
                    }
                )
                self._order(
                    {
                        "recorded_at": now.isoformat(timespec="seconds"),
                        "session_date": now.date().isoformat(),
                        "entry_session_date": position.get("session_date"),
                        "market": market,
                        "position_id": position.get("position_id"),
                        "order_id": position.get("opening_exit_order_id"),
                        "symbol": position.get("symbol"),
                        "purpose": "next_session_open_auction_exit",
                        "status": "missed_actual_opening_print",
                        "simulation_only": True,
                    }
                )
                mode["engine_status"] = "critical_actual_opening_print_missing"
                changed = True
                continue
            quote = dict(quotes.get(str(position.get("symbol") or "")) or {})
            matched = _auction_print(
                quote,
                session_date=now.date().isoformat(),
                not_before=OPEN_MATCH_GATE,
                not_after=OPEN_OBSERVATION_DEADLINE,
                price_field="open",
            )
            if matched is None:
                wait_reason = (
                    "simulated_indicative_match_not_a_fill"
                    if quote.get("simtrade") is True
                    else "actual_opening_print_unproven"
                )
                if position.get("opening_match_wait_reason") != wait_reason:
                    position["opening_match_wait_reason"] = wait_reason
                    changed = True
                continue
            price, exchange_at = matched
            lower = _finite(quote.get("lower_limit"))
            upper = _finite(quote.get("upper_limit"))
            if not _inside_limits(price, lower, upper):
                position["opening_exit_order_status"] = "blocked_open_outside_legal_limits"
                self._order(
                    {
                        "recorded_at": now.isoformat(timespec="seconds"),
                        "session_date": now.date().isoformat(),
                        "entry_session_date": position.get("session_date"),
                        "market": market,
                        "position_id": position.get("position_id"),
                        "order_id": position.get("opening_exit_order_id"),
                        "symbol": position.get("symbol"),
                        "purpose": "next_session_open_auction_exit",
                        "status": "blocked_open_outside_legal_limits",
                        "simulation_only": True,
                    }
                )
                changed = True
                continue
            quote["fill_contract"] = "actual_next_session_opening_auction_price"
            quote["depth_assumption"] = "full_paper_quantity_no_queue_allocation_claim"
            quote["exchange_match_at"] = exchange_at.isoformat(timespec="milliseconds")
            self._close_position(
                position,
                mode,
                price=price,
                quote=quote,
                now=now,
                reason="next_session_open_auction_fill",
                order_type="LMT_ROD",
                quantity=abs(signed),
                ledger_order_id=str(position.get("opening_exit_order_id") or "") or None,
                ledger_session_date=now.date().isoformat(),
            )
            position["opening_exit_order_status"] = "filled"
            position["exit_session_date"] = now.date().isoformat()
            position["exit_exchange_at"] = exchange_at.isoformat(timespec="milliseconds")
            changed = True
            closed_any = True
        if closed_any and not any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            mode["engine_status"] = "flat_after_next_open"
            mode["mandatory_open_exit_completed_at"] = now.isoformat(timespec="seconds")
        return changed

    def _mark_mode(
        self,
        market: str,
        now: datetime,
        quotes: Mapping[str, Mapping[str, Any]],
        *,
        append_history: bool = True,
    ) -> None:
        mode = (self.state.get("modes") or {}).get(market)
        if not isinstance(mode, dict):
            return
        open_net = 0.0
        stale_count = 0
        open_count = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            open_count += 1
            quote = quotes.get(str(position.get("symbol") or "")) or {}
            price = _finite(
                quote.get("bid" if signed > 0 else "ask")
            ) or _finite(quote.get("last"))
            if price is None:
                stale_count += 1
                position["valuation_stale"] = True
                open_net += float(position.get("last_complete_net_pnl_twd") or 0.0)
                continue
            pnl = position_net_liquidation_pnl(position, price)
            position.update(
                {
                    "last_mark_at": now.isoformat(timespec="seconds"),
                    "last_quote_at": quote.get("quote_at"),
                    "last_mark_price": price,
                    "last_complete_net_pnl_twd": pnl,
                    "total_net_pnl_twd": float(
                        position.get("realized_net_pnl_twd") or 0.0
                    ) + pnl,
                    "valuation_stale": False,
                }
            )
            open_net += pnl
        cumulative = float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
        total_equity = float(mode.get("initial_capital_twd") or 0.0) + cumulative + open_net
        mode.update(
            {
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "last_mark_at": now.isoformat(timespec="seconds"),
                "valuation_stale": stale_count > 0,
            }
        )
        if not append_history or open_count == 0:
            return
        self._append_ledger(
            self.marks_path,
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "minute": now.replace(second=0, microsecond=0).isoformat(timespec="minutes"),
                "session_date": now.date().isoformat(),
                "market": market,
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "cumulative_realized_net_pnl_twd": cumulative,
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "valuation_stale": stale_count > 0,
                "holding_phase": (
                    "next_session_open_auction"
                    if now.date().isoformat() != str(mode.get("session_date") or "")
                    else "post_close_overnight"
                ),
            },
        )

    def process_quotes(
        self,
        *,
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime | None = None,
        append_mark_history: bool = True,
    ) -> None:
        observed = _now_taipei(now)
        changed_markets: set[str] = set()
        self.begin_deferred_ledger_writes()
        try:
            for market, mode in (self.state.get("modes") or {}).items():
                if not isinstance(mode, dict):
                    continue
                market_text = str(market)
                clock = _clock(observed)
                changed = self._expire_stale_close_orders(mode, observed)
                if clock >= REGULAR_CLOSE:
                    changed = self._settle_close_entries(
                        market_text, mode, quotes, observed
                    ) or changed
                if OPEN_ORDER_GATE <= clock < REGULAR_CLOSE:
                    changed = self._submit_opening_exits(
                        market_text, mode, quotes, observed
                    ) or changed
                if clock >= OPEN_MATCH_GATE:
                    changed = self._settle_opening_exits(
                        market_text, mode, quotes, observed
                    ) or changed
                if changed or append_mark_history:
                    self._mark_mode(
                        market_text,
                        observed,
                        quotes,
                        append_history=append_mark_history,
                    )
                    changed_markets.add(market_text)
            self.flush_deferred_ledger_writes()
        except Exception:
            self._deferred_ledger_rows = None
            raise
        if changed_markets:
            self._persist(observed)

    def _persist(self, now: datetime | None = None) -> None:
        observed = _now_taipei(now)
        super()._persist(observed)
        # Reuse the battle-tested atomic state/position publication, then
        # replace its day-trade schedule description with this product's actual
        # contract.  This status file is display metadata; the ledgers remain
        # authoritative and were fsynced before the state revision advanced.
        status = json.loads(self.status_path.read_text(encoding="utf-8"))
        status["product"] = "tw_overnight"
        status["overnight_contract_version"] = OVERNIGHT_CONTRACT_VERSION
        public_modes = status.get("modes") or {}
        statuses = [
            str(row.get("engine_status") or "")
            for row in public_modes.values()
            if isinstance(row, Mapping)
        ]
        status["health"] = (
            "critical"
            if any(value.startswith("critical") for value in statuses)
            else "blocked"
            if any(value.startswith("blocked") for value in statuses)
            else "active"
            if any(
                value in {
                    "waiting_close_auction_match",
                    "carrying_to_next_open",
                    "flat_after_next_open",
                }
                for value in statuses
            )
            else "waiting"
        )
        status["schedule"] = {
            "model_decision": "13:25 current quote using temporary day-trade checkpoint adapter",
            "close_entry_order": "13:25 LMT_ROD; long buy at upper limit, short sell at lower limit",
            "close_fill": "actual 13:30 closing-auction print; delayed symbols may settle at 13:33",
            "overnight": "one strict cohort; no same-session exit and no overlapping cohort",
            "opening_exit_order": "08:30 LMT_ROD; long sell at lower limit, short cover at upper limit",
            "opening_fill": "actual next-session opening-auction print at or after 09:00",
            "simtrade": "indicative simulated matching is observable but never recorded as a fill",
            "liquidity": "full requested paper quantity; queue allocation is not claimed",
            "fees": "ordinary cash buy/sell commission and ordinary stock/ETF transaction tax",
            "simulation_only": True,
        }
        for market, public_mode in (status.get("modes") or {}).items():
            state_mode = (self.state.get("modes") or {}).get(market) or {}
            for key in (
                "product",
                "model_adapter",
                "model_trained_for_overnight",
                "signal_registered_at",
                "entry_fill_outcome",
                "mandatory_open_exit_completed_at",
            ):
                public_mode[key] = state_mode.get(key)
        _atomic_json(self.status_path, status)


__all__ = [
    "CLOSE_ORDER_GATE",
    "DELAYED_CLOSE_DEADLINE",
    "OPEN_MATCH_GATE",
    "OPEN_ORDER_GATE",
    "REGULAR_CLOSE",
    "TwOvernightSimulationEngine",
]
