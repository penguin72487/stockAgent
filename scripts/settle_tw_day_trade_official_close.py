#!/usr/bin/env python3
"""Explicit, offline, user-authorized paper close reconciliation. Never broker orders.

Plan from retained official raw reports; apply only with both ledger writers
locked. Preserve the complete old ledger by atomic directory exchange. This is
not strategy promotion and does not rebuild or relabel historical signals.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import date, datetime, time, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.promote_tw_day_trade_replay import _acquire_engine_lock, _atomic_json, _exchange_directories, _sha256
from scripts.add_tw_day_trade_paper_account import _prefix_hash, _sync_tree
from stockagent.data.tw_price_rules import price_on_tick_grid_numpy
from stockagent.live.tw_day_trade_simulation import TAIPEI, TwDayTradeSimulationEngine, minute_curve_write_lock

CONTRACT = "user_authorized_official_close_paper_settlement_v1"
LAST_PRICE_CONTRACT = "user_authorized_last_traded_price_paper_settlement_v1"
LEDGERS = ("signals.jsonl", "orders.jsonl", "fills.jsonl", "events.jsonl", "latency.jsonl")


def book_fingerprint(state):
    keys = ("signal_id", "session_date", "checkpoint_fingerprint", "config_fingerprint",
            "initial_capital_twd", "cumulative_realized_net_pnl_twd", "cumulative_carry_cost_twd",
            "cumulative_corporate_action_net_twd", "corporate_action_receivable_twd", "corporate_action_payable_twd",
            "manual_close_settlement")
    position_keys = ("symbol", "session_date", "signed_shares", "entry_price", "inventory_basis_price",
                     "remaining_entry_fee_twd", "realized_net_pnl_twd", "margin_cost_twd", "margin_carry_contract")
    book = {k: {**{field: m.get(field) for field in keys},
                "positions": {pid: {field: p.get(field) for field in position_keys}
                              for pid, p in m.get("positions", {}).items()}}
            for k, m in state["modes"].items()}
    return hashlib.sha256(json.dumps(book, sort_keys=True, allow_nan=False).encode()).hexdigest()


def official_closes(raw_root: Path, day: date, *, exchanges=("twse", "tpex")):
    prices, sources = {}, []
    for exchange in exchanges:
        path = raw_root / f"{exchange}_daily_ohlcv" / f"{day}.json"
        raw = path.read_bytes()
        payload = json.loads(raw)
        if str(payload.get("date")) != day.strftime("%Y%m%d") or str(payload.get("stat")).lower() != "ok":
            raise ValueError(f"invalid official report date/status: {path}")
        source = {"path": str(path.resolve()), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        sources.append(source)
        for table in payload.get("tables", []):
            fields = [f.strip() for f in table.get("fields", [])]
            code_key = next((k for k in ("證券代號", "代號") if k in fields), None)
            close_key = next((k for k in ("收盤價", "收盤") if k in fields), None)
            if code_key is None or close_key is None:
                continue
            for values in table.get("data", []):
                row = dict(zip(fields, values))
                symbol = str(row[code_key]).strip()
                if symbol in prices:
                    raise ValueError(f"duplicate official symbol: {symbol}")
                try:
                    close = float(str(row[close_key]).replace(",", ""))
                    if not math.isfinite(close) or close <= 0:
                        close = None
                except (ValueError, TypeError):
                    close = None
                prices[symbol] = {"symbol": symbol, "price": close, "official_date": str(day),
                                  "price_date": str(day), "price_basis": "official_session_close",
                                  "source": f"{exchange}_official_daily_close", "source_sha256": source["sha256"],
                                  "raw_close": row[close_key], "official_row": row}
    return prices, sources


def last_traded_price(raw_root: Path, day: date, symbol: str, current):
    """Use a traded daily close, never a bid/ask or a carried zero-volume price.

    Every intervening weekday requires an official report. Missing reports
    (including holidays without a report) fail closed, not skip to an old price.
    This conservative offline fallback is only for explicitly selected symbols.
    """
    exchange = str(current.get("source", "")).split("_", 1)[0]
    if exchange not in ("twse", "tpex"):
        raise ValueError(f"missing official symbol/exchange: {symbol}")
    sources = []
    for age in range(366):
        observed_day = day - timedelta(days=age)
        path = raw_root / f"{exchange}_daily_ohlcv" / f"{observed_day}.json"
        if not path.exists() and observed_day.weekday() >= 5:
            continue
        if not path.exists():
            raise ValueError(f"cannot prove last trade across missing report: {path}")
        prices, proof = official_closes(raw_root, observed_day, exchanges=(exchange,))
        sources.extend(proof)
        row = prices.get(symbol)
        if row is None:
            raise ValueError(f"cannot prove last trade across missing symbol: {symbol} {observed_day}")
        try:
            volume = float(str(row["official_row"]["成交股數"]).replace(",", ""))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid official volume: {symbol} {observed_day}") from exc
        if not math.isfinite(volume) or volume < 0:
            raise ValueError(f"invalid official volume: {symbol} {observed_day}")
        if volume > 0:
            if row["price"] is None:
                raise ValueError(f"positive volume without close: {symbol} {observed_day}")
            return {**row, "price_basis": "last_available_trade", "price_age_calendar_days": age}, sources
    raise ValueError(f"no proven last trade within 366 days: {symbol}")


def make_plan(root: Path, raw_root: Path, day: date, *, last_traded_price_for=()):
    state = json.loads((root / "state.json").read_text())
    if state.get("simulation_only") is not True or state.get("production_order_possible") is not False:
        raise ValueError("not an exclusively paper ledger")
    prices, sources = official_closes(raw_root, day)
    allowed = set(last_traded_price_for)
    open_symbols = {p["symbol"] for m in state["modes"].values()
                    for p in m.get("positions", {}).values() if p.get("signed_shares")}
    if allowed - open_symbols:
        raise ValueError("last-price scope includes symbols without residual positions")
    for symbol in sorted(allowed):
        if prices.get(symbol, {}).get("price") is not None:
            raise ValueError(f"last-price override unnecessary: today's close exists for {symbol}")
        prices[symbol], proofs = last_traded_price(raw_root, day, symbol, prices.get(symbol, {}))
        sources.extend(proofs)
    sources = list({s["path"]: s for s in sources}.values())
    entries, blocked = [], []
    for market, mode in state["modes"].items():
        if mode.get("session_date") != str(day):
            raise ValueError(f"scope must match current ledger session: {market}")
        if mode.get("ledger_state_divergence"):
            raise ValueError(f"ledger divergence: {market}")
        for pid, p in mode.get("positions", {}).items():
            if not int(p.get("signed_shares") or 0):
                continue
            source = prices.get(p["symbol"], {})
            price = source.get("price")
            row = {"market": market, "position_id": pid, "symbol": p["symbol"],
                   "signed_shares": p["signed_shares"], "entry_session_date": p["session_date"], **source}
            if price is None:
                blocked.append(row | {"reason": "official_session_has_no_close"})
                continue
            if not bool(price_on_tick_grid_numpy(np.array([price]), np.array([date.fromisoformat(source['price_date'])]),
                        security_types=p.get("security_type") or "stock")[0]):
                raise ValueError(f"off-grid official close: {p['symbol']} {price}")
            entries.append(row)
    key = hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()[:16]
    return {"contract": LAST_PRICE_CONTRACT if allowed else CONTRACT,
            "last_traded_price_for": sorted(allowed), "raw_root": str(raw_root.resolve()),
            "operation_id": f"{day}:{key}", "session_date": str(day),
            "simulation_only": True, "production_order_possible": False,
            "book_fingerprint": book_fingerprint(state), "sources": sources,
            "entries": entries, "blocked": blocked, "settle_count": len(entries),
            "blocked_count": len(blocked), "liquidity_assumption": "user_requested_full_residual_not_exchange_fill"}


def reconcile_candidate(root: Path, plan, *, recorded_at: datetime):
    """Mutate only an isolated candidate. All original ledger prefixes survive."""
    day = date.fromisoformat(plan["session_date"])
    if recorded_at.astimezone(TAIPEI).date() != day or recorded_at.astimezone(TAIPEI).time() < time(13, 35):
        raise ValueError("offline reconciliation must run after today's closing auction")
    engine = TwDayTradeSimulationEngine(root)
    if book_fingerprint(engine.state) != plan["book_fingerprint"]:
        raise ValueError("ledger changed after plan; replan required")
    effective_at = datetime.combine(day, time(13, 30), TAIPEI)
    rows, touched, reversal_total = [], set(), 0.
    for entry in plan["entries"]:
        mode = engine.state["modes"][entry["market"]]
        p = mode["positions"][entry["position_id"]]
        if p["signed_shares"] != entry["signed_shares"]:
            raise ValueError("quantity changed")
        # Same-session conversions cannot coexist with a counterfactual
        # same-session roundtrip. Retain the original debit and append reversal.
        reversal = 0.
        if p.get("session_date") == str(day) and str(p.get("margin_converted_at") or "").startswith(str(day)):
            for cost in list(mode.get("carry_cost_ledger", [])):
                if cost.get("position_id") == p["position_id"] and cost.get("date") == str(day):
                    amount = float(cost["amount_twd"])
                    if amount < 0:
                        raise ValueError("unexpected prior conversion reversal")
                    mode["carry_cost_ledger"].append({**cost, "cost_id": cost["cost_id"] + ":official_close_reversal",
                        "kind": "manual_same_day_close_conversion_reversal", "amount_twd": -amount,
                        "reverses_cost_id": cost["cost_id"], "recorded_at": recorded_at.isoformat()})
                    reversal += amount
            p["reversed_margin_conversion"] = {"contract": p.pop("margin_carry_contract", None),
                "original_converted_at": p.get("margin_converted_at"), "amount_twd": reversal}
            p["margin_cost_twd"] = float(p.get("margin_cost_twd") or 0) - reversal
            mode["cumulative_carry_cost_twd"] = float(mode.get("cumulative_carry_cost_twd") or 0) - reversal
        contract = LAST_PRICE_CONTRACT if entry["price_basis"] == "last_available_trade" else CONTRACT
        reason = "manual_last_traded_price_settlement" if contract == LAST_PRICE_CONTRACT else "manual_official_close_settlement"
        receipt = {"contract": contract, "session_date": str(day), "recorded_at": recorded_at.isoformat(),
                   "effective_at": effective_at.isoformat(), "price": entry["price"], "source": entry["source"],
                   "price_date": entry["price_date"], "price_basis": entry["price_basis"],
                   "source_sha256": entry["source_sha256"], "broker_fill": False,
                   "full_quantity_is_user_assumption": True}
        orders, fills = [], []
        before_realized = float(mode.get("cumulative_realized_net_pnl_twd") or 0)
        q = {"fill_contract": contract, "depth_assumption": plan["liquidity_assumption"]}
        engine._book_position_close(p, mode, price=entry["price"], quote=q, now=effective_at,
            reason=reason, order_type="PAPER_CLOSE_SETTLEMENT",
            quantity=abs(int(entry["signed_shares"])), ledger_session_date=str(day),
            ledger_order_id=f"{plan['operation_id']}:{p['position_id']}", ledger_rows=(orders, fills))
        for order in orders:
            engine._order(order | {"recorded_at": recorded_at.isoformat(), "counterfactual_settlement": receipt})
        for fill in fills:
            engine._fill(fill | {"recorded_at": recorded_at.isoformat(), "counterfactual_settlement": receipt,
                                 "quote_at": None, "exchange_match_at": None})
        p.update(manual_close_settlement=receipt, closing_auction_order_status="manual_close_settled_not_exchange_fill",
                 mandatory_exit_pending=False, carry_type=f"closed_by_{reason}")
        row = {**entry, "net_pnl_twd": float(mode["cumulative_realized_net_pnl_twd"]) - before_realized,
               "conversion_cost_reversed_twd": reversal, "exit_fee_and_tax_twd": fills[0]["fee_and_tax_twd"]}
        rows.append(row)
        reversal_total += reversal
        touched.add(entry["market"])
    for market in touched:
        mode = engine.state["modes"][market]
        for order in (mode.get("pending_entry_orders") or {}).values():
            if order.get("status") == "working":
                order["status"] = "cancelled_manual_close_settlement"
        remaining = [p for p in mode["positions"].values() if p.get("signed_shares")]
        previous = mode.get("manual_close_settlement") or {}
        operations = list(previous.get("operations") or []) if previous.get("session_date") == str(day) else []
        if previous.get("session_date") == str(day) and not operations:
            operations.append(dict(previous))
        market_rows = [r for r in rows if r["market"] == market]
        operations.append({"operation_id": plan["operation_id"], "contract": plan["contract"],
            "recorded_at": recorded_at.isoformat(), "session_date": str(day), "settled_count": len(market_rows),
            "last_traded_prices": [{k: r[k] for k in ("symbol", "price", "price_date", "signed_shares")}
                                   for r in market_rows if r["price_basis"] == "last_available_trade"],
            "broker_fill": False})
        mode["manual_close_settlement"] = {"contract": plan["contract"], "recorded_at": recorded_at.isoformat(),
            "session_date": str(day), "settled_count": sum(op["settled_count"] for op in operations),
            "remaining_count": len(remaining), "broker_fill": False, "operations": operations,
            "last_traded_prices": [p for op in operations for p in op.get("last_traded_prices", [])]}
        mode["pending_entry_shares"] = 0
        mode["margin_carry_position_count"] = sum(bool(p.get("margin_carry_contract")) for p in remaining)
        # Keep the original force-exit failures and original closing receipt:
        # this adjustment does not retroactively make the original engine pass.
        engine._mark_mode(market, effective_at, {}, append_history=False)
        engine._archive_mode_positions(mode, archived_at=recorded_at)
    engine._event("manual_paper_close_settlement_completed", recorded_at=recorded_at, contract=plan["contract"],
                  operation_id=plan["operation_id"], session_date=str(day), settled_count=len(rows),
                  remaining_count=len(plan["blocked"]), simulation_only=True, production_order_possible=False)
    engine._persist(recorded_at)
    return {"rows": rows, "conversion_cost_reversed_twd": reversal_total,
            "remaining_count": sum(bool(p.get("signed_shares")) for m in engine.state["modes"].values() for p in m["positions"].values()),
            "ending_equity_twd": {k: m["total_equity_twd"] for k,m in engine.state["modes"].items()}}


def rewrite_endpoints(root: Path, day: str, markets: set[str]):
    """Change only the authorized close endpoints; preserve all interior bytes."""
    state = json.loads((root / "state.json").read_text())
    path = root / "marks.jsonl"
    if not path.exists():
        return {"updated": 0, "missing": sorted(markets)}
    last, count = {}, {}
    with path.open() as stream:
        for line in stream:
            if day not in line:
                continue
            row = json.loads(line)
            if row.get("session_date") == day and row.get("market") in markets:
                count.setdefault(row["market"], set()).add(row.get("minute"))
                if str(row.get("minute", ""))[11:16] == "13:30":
                    last[row["market"]] = row
    temporary = path.with_suffix(".close-settlement.tmp")
    with path.open() as source, temporary.open("w") as dest:
        for line in source:
            if day in line:
                row = json.loads(line)
                if row.get("session_date") == day and row.get("market") in last and str(row.get("minute", ""))[11:16] == "13:30":
                    continue
            dest.write(line)
        for market,row in sorted(last.items()):
            mode = state["modes"][market]
            for key in ("cumulative_realized_net_pnl_twd", "open_net_liquidation_pnl_twd", "total_equity_twd",
                        "cumulative_carry_cost_twd", "open_position_count", "stale_position_count", "valuation_stale"):
                row[key] = mode.get(key)
            row["manual_close_settlement"] = mode["manual_close_settlement"]
            row["recorded_at"] = mode["manual_close_settlement"]["recorded_at"]
            row["valuation_source"] = mode["manual_close_settlement"]["contract"]
            row["accepted_endpoint_accounting_verified"] = True
            dest.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
        dest.flush()
        os.fsync(dest.fileno())
    os.replace(temporary, path)
    return {"updated": len(last), "missing": sorted(markets - last.keys()),
            "unique_minute_points": {market: len(minutes) for market,minutes in count.items()},
            "interior_minutes_unchanged": True, "prior_dates_unchanged": True}


def apply_plan(live: Path, plan, *, recorded_at: datetime):
    if not plan["entries"]:
        return {"status": "no_priced_residuals", "remaining_count": len(plan["blocked"])}
    with ExitStack() as stack:
        stack.enter_context(_acquire_engine_lock(live))
        stack.enter_context(minute_curve_write_lock(live))
        if book_fingerprint(json.loads((live / "state.json").read_text())) != plan["book_fingerprint"]:
            raise ValueError("live book changed; replan required")
        for source in plan["sources"]:
            if _sha256(Path(source["path"])) != source["sha256"]:
                raise ValueError("official source changed")
        verified = make_plan(live, Path(plan["raw_root"]), date.fromisoformat(plan["session_date"]),
                             last_traded_price_for=plan["last_traded_price_for"])
        if verified != plan:
            raise ValueError("plan price or scope differs from the official source and current book")
        staging = Path(tempfile.mkdtemp(prefix="official-close-settlement-", dir=live.parent))
        candidate = staging / "ledger"
        before = {name: {"size": (live/name).stat().st_size, "sha256": _sha256(live/name)}
                  for name in LEDGERS if (live/name).exists()}
        shutil.copytree(live, candidate, ignore=lambda directory,names: {
            name for name in names if Path(directory) == live and (name.startswith(".") or name.startswith("state.json.tmp.") or name == "quote_broker")})
        stack.enter_context(_acquire_engine_lock(candidate))
        result = reconcile_candidate(candidate, plan, recorded_at=recorded_at)
        result["minute_endpoints"] = rewrite_endpoints(candidate, plan["session_date"], {e["market"] for e in plan["entries"]})
        for name, proof in before.items():
            if _prefix_hash(candidate/name, proof["size"]) != proof["sha256"]:
                raise ValueError(f"original ledger prefix changed: {name}")
        after = json.loads((candidate / "state.json").read_text())
        if result["remaining_count"] != len(plan["blocked"]):
            raise ValueError("unexpected residual count")
        for entry in plan["entries"]:
            p = after["modes"][entry["market"]]["positions"][entry["position_id"]]
            if p["signed_shares"] != 0 or p["exit_price"] != entry["price"]:
                raise ValueError("settlement quantity/price mismatch")
        audit = {**plan, **result, "status": "applied_with_missing_closes" if plan["blocked"] else "applied",
                 "recorded_at": recorded_at.isoformat(), "rollback_directory": str(candidate),
                 "original_ledger_prefixes": before, "strategy_changed": False}
        receipts = candidate / "settlement_receipts"
        previous = candidate / "official_close_settlement_receipt.json"
        if previous.exists():
            prior = json.loads(previous.read_text())
            _atomic_json(receipts / f"{prior['operation_id'].replace(':', '-')}.json", prior)
        _atomic_json(receipts / f"{plan['operation_id'].replace(':', '-')}.json", audit)
        _atomic_json(candidate / "paper_close_settlement_receipt.json", audit)
        if plan["contract"] == CONTRACT:
            _atomic_json(candidate / "official_close_settlement_receipt.json", audit)
        _atomic_json(staging / "receipt.json", audit)
        # Both paths stay available after exchange; no original file is deleted.
        _sync_tree(candidate)
        _exchange_directories(live, candidate)
        for parent in (live.parent, candidate.parent):
            descriptor = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        return audit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("artifacts/live/tw_day_trade_simulation"))
    parser.add_argument("--raw-root", type=Path, default=Path("data_tw_public/raw"))
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--confirm-counterfactual-settlement", action="store_true")
    parser.add_argument("--last-traded-price-for", action="append", default=[], metavar="SYMBOL",
                        help="Explicit fallback only for this residual symbol when today's official close is absent")
    args = parser.parse_args()
    if args.apply:
        if not args.confirm_counterfactual_settlement:
            parser.error("explicit counterfactual settlement confirmation required")
        plan = json.loads(args.plan.read_text())
        if plan["contract"] not in (CONTRACT, LAST_PRICE_CONTRACT) or plan["session_date"] != str(args.date):
            raise ValueError("plan contract/date mismatch")
        result = apply_plan(args.state_dir.resolve(), plan, recorded_at=datetime.now(TAIPEI))
        print(json.dumps({k: result.get(k) for k in ("status", "settle_count", "remaining_count", "rollback_directory", "ending_equity_twd")}, ensure_ascii=False))
    else:
        plan = make_plan(args.state_dir.resolve(), args.raw_root, args.date,
                         last_traded_price_for=args.last_traded_price_for)
        _atomic_json(args.plan, plan)
        print(json.dumps({"settle_count": plan["settle_count"], "blocked_count": plan["blocked_count"],
                          "blocked_symbols": sorted({e["symbol"] for e in plan["blocked"]}), "plan": str(args.plan)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
