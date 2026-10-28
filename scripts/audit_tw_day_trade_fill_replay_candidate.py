#!/usr/bin/env python3
"""Reconstruct a paper inventory from a pinned journal prefix, without trading.

This is an independently written *inventory* candidate, not a re-inference,
minute-price execution replay, PnL verification, or a promotable ledger.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_tw_day_trade_restart import verify_prefix
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.signal_artifacts import read_execution_weight_rows

ENTRY = frozenset({"entry", "entry_completion"})
EXIT = frozenset({
    "next_signal_inventory_delta", "stop_loss_inside_daily_limit_1_tick_trigger",
    "take_profit_inside_daily_limit_1_tick", "13_20_limit_filled",
    "13_24_market_force_exit", "13_30_unlimited_close_paper_settlement",
})


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def audit_pnl_arithmetic(*, modes: dict, fills: list[dict]) -> dict:
    """Reconcile recorded arithmetic only, not execution prices or fee policy."""
    errors, sums, count = [], defaultdict(float), 0
    positions = {(market, str(position.get("position_id") or "")): position
                 for market, mode in modes.items() for position in (mode.get("positions") or {}).values()}
    for index, fill in enumerate(fills):
        if fill.get("purpose") not in EXIT:
            continue
        key = (str(fill.get("market") or ""), str(fill.get("position_id") or ""))
        try:
            gross, fee, allocated, net = (float(fill[name]) for name in
                                          ("gross_pnl_twd", "fee_and_tax_twd",
                                           "entry_fee_allocated_twd", "net_pnl_twd"))
            if (key not in positions or not all(map(math.isfinite, (gross, fee, allocated, net)))
                    or fee < 0 or allocated < 0
                    or not math.isclose(gross - fee - allocated, net, rel_tol=1e-10, abs_tol=1e-6)):
                raise ValueError("unknown position or inconsistent recorded arithmetic")
        except (KeyError, TypeError, ValueError, OverflowError):
            errors.append(f"fill {index}: invalid or inconsistent recorded PnL fields")
            continue
        count += 1
        sums[key] += net
    for key, position in positions.items():
        try:
            recorded = float(position.get("realized_net_pnl_twd") or 0)
            if not math.isfinite(recorded) or not math.isclose(recorded, sums[key], rel_tol=1e-10, abs_tol=1e-6):
                raise ValueError("snapshot differs")
        except (TypeError, ValueError, OverflowError):
            errors.append(f"{key[0]}/{key[1]}: realized PnL does not match recorded exits")
    return {"exit_rows_reconciled": count, "errors": errors,
            "arithmetic_consistency_passed": count > 0 and not errors,
            "fee_schedule_execution_prices_and_economic_correctness_verified": False}


def audit_recorded_fee_rates(*, modes: dict, fills: list[dict]) -> dict:
    """Independently recalculate fees from retained per-position rates.

    A final snapshot cannot establish when a position converted to cash, so
    carry positions are explicitly unresolved. The snapshot rates and rebate
    are not proof of the broker's historic fee contract, executable prices, or
    rebate payment; this checks only the retained paper-accounting arithmetic.
    """
    positions = {(market, str(position.get("position_id") or "")): position
                 for market, mode in modes.items() for position in (mode.get("positions") or {}).values()}
    errors: list[str] = []
    unresolved: list[str] = []
    checked = 0
    for index, fill in enumerate(fills):
        key = (str(fill.get("market") or ""), str(fill.get("position_id") or ""))
        position = positions.get(key)
        purpose = fill.get("purpose")
        if position is None or purpose not in ENTRY | EXIT:
            unresolved.append(f"fill {index}: unknown position or purpose")
            continue
        if position.get("margin_carry_contract"):
            unresolved.append(f"fill {index}: per-fill carry conversion clock is not established")
            continue
        try:
            side = position["side"]
            if side not in {"long", "short"}:
                raise ValueError("invalid position side")
            buy = (side == "long") == (purpose in ENTRY)
            gross_rate = float(position["buy_fee_rate" if buy else "sell_fee_rate"])
            rebate_rate = float(position["commission_rebate_rate"])
            quantity, price = int(fill["quantity"]), float(fill["price"])
            gross, rebate, net = (float(fill[name]) for name in
                                  ("gross_fee_and_tax_twd", "commission_rebate_accrued_twd", "fee_and_tax_twd"))
            if (quantity <= 0 or price <= 0 or not all(map(math.isfinite,
                    (price, gross_rate, rebate_rate, gross, rebate, net)))
                    or min(gross_rate, rebate_rate, gross, rebate, net) < 0
                    or rebate_rate > gross_rate):
                raise ValueError("invalid fee input")
            expected_gross = quantity * price * gross_rate
            expected_rebate = quantity * price * rebate_rate
            if not all(math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-6)
                       for actual, expected in ((gross, expected_gross), (rebate, expected_rebate),
                                                (net, expected_gross - expected_rebate))):
                raise ValueError("recorded fee differs from quantity, price and retained rates")
        except (KeyError, TypeError, ValueError, OverflowError):
            errors.append(f"fill {index}: invalid or inconsistent recorded fee")
            continue
        checked += 1
    return {"fill_rows_reconciled": checked, "errors": errors, "unresolved": unresolved,
            "recorded_fee_arithmetic_passed": checked > 0 and not errors and not unresolved,
            "historical_broker_fee_terms_and_rebate_payment_verified": False,
            "independent_execution_price_or_quantity_verified": False}


def reconstruct(*, day: date, modes: dict, fills: list[dict], signal_inventory: dict) -> dict:
    """Replay explicit entry and reduction quantities, failing closed on ambiguity."""
    errors: list[str] = []
    by_position: dict[tuple[str, str], dict] = {}
    for market, mode in modes.items():
        if mode.get("session_date") != str(day):
            errors.append(f"{market}: snapshot is not the requested session")
            continue
        for position in (mode.get("positions") or {}).values():
            pid = str(position.get("position_id") or "")
            if not pid or position.get("side") not in {"long", "short"}:
                errors.append(f"{market}: incomplete position identity")
                continue
            key = (market, pid)
            if key in by_position:
                errors.append(f"{market}/{pid}: duplicate position identity")
                continue
            by_position[key] = {"position": position, "entry": 0, "exit": 0,
                                "remaining": 0, "at_0901": 0, "rows": []}
    for index, fill in enumerate(fills):
        market, pid = str(fill.get("market") or ""), str(fill.get("position_id") or "")
        item = by_position.get((market, pid))
        purpose = str(fill.get("purpose") or "")
        quantity = fill.get("quantity")
        try:
            when = datetime.fromisoformat(str(fill.get("fill_at") or ""))
            price = float(fill.get("price"))
            valid = (when.utcoffset() is not None and when.astimezone(ZoneInfo("Asia/Taipei")).date() == day
                     and type(quantity) is int and quantity > 0
                     and math.isfinite(price) and price > 0
                     and fill.get("simulation_only") is True)
        except (TypeError, ValueError, OverflowError):
            valid = False
        if (item is None or not valid or purpose not in ENTRY | EXIT
                or fill.get("symbol") != item["position"].get("symbol")):
            errors.append(f"fill {index}: unknown identity, purpose, clock, quantity, or price")
            continue
        item["rows"].append((when, index, purpose, quantity))
    per_mode: dict[str, dict[str, Any]] = {}
    for market in modes:
        market_rows = {pid: row for (name, pid), row in by_position.items() if name == market}
        opening = datetime.fromisoformat(f"{day}T09:01:00+08:00")
        current: dict[str, int] = defaultdict(int)
        opening_symbols: set[str] = set()
        for pid, item in market_rows.items():
            position = item["position"]
            balance = 0
            for when, index, purpose, quantity in sorted(item["rows"]):
                if purpose in ENTRY:
                    item["entry"] += quantity
                    balance += quantity
                else:
                    item["exit"] += quantity
                    balance -= quantity
                    if balance < 0:
                        errors.append(f"{market}/{pid}: reduction exceeds prior inventory at fill {index}")
                if when <= opening:
                    item["at_0901"] = balance
            item["remaining"] = balance
            expected = abs(int(position.get("signed_shares") or 0))
            if balance != expected or item["entry"] != int(position.get("filled_shares") or 0):
                errors.append(f"{market}/{pid}: fill quantities disagree with snapshot")
            if item["at_0901"] > 0:
                opening_symbols.add(str(position["symbol"]))
            current["entry_shares"] += item["entry"]
            current["exit_shares"] += item["exit"]
            current["remaining_shares"] += balance
        inventory = (signal_inventory.get("modes") or {}).get(market) or {}
        registrations = inventory.get("registrations") or []
        weight_path = Path(str(inventory.get("weights_path") or ""))
        if not weight_path.is_file() or _hash(weight_path) != inventory.get("weights_sha256"):
            errors.append(f"{market}: first signal weights changed or missing")
            signal_symbols: set[str] = set()
        else:
            if weight_path.suffix == ".parquet":
                rows = pl.read_parquet(weight_path).to_dicts()
            else:
                rows = read_execution_weight_rows(weight_path, market=str(modes[market].get("signal_market") or market),
                                                  signal_id=str(inventory.get("first_published_signal_id") or ""))
            signal_symbols = {str(row["symbol"]) for row in rows
                              if math.isfinite(float(row.get("target_weight") or 0))
                              and float(row.get("target_weight") or 0) != 0}
        current.update({
            "positions": len(market_rows), "held_symbols_at_0901": len(opening_symbols),
            "first_signal_nonzero_symbols": len(signal_symbols),
            "first_signal_only_symbols": sorted(signal_symbols - opening_symbols),
            "held_at_0901_not_in_first_signal": sorted(opening_symbols - signal_symbols),
            "first_registered_matches_first_publication": inventory.get("first_registered_matches_first_publication"),
            "registration_count": len(registrations),
            "comparison_is_diagnostic_not_expected_position_equality": True,
        })
        per_mode[market] = dict(current)
    return {"errors": errors, "modes": per_mode,
            "quantity_reconciliation_passed": not errors,
            "pnl_fee_source_price_and_minute_coverage_verified": False}


def audit(*, day: date, state_dir: Path, restart_path: Path, inventory_path: Path) -> dict:
    restart = json.loads(restart_path.read_bytes())
    inventory = json.loads(inventory_path.read_bytes())
    export = restart_path.with_suffix(".state.json")
    if (restart.get("status") != "accepted" or restart.get("simulation_only") is not True
            or restart.get("production_order_possible") is not False
            or Path(restart.get("state_dir") or "").resolve() != state_dir.resolve()
            or _hash(export) != (restart.get("state_export") or {}).get("sha256")
            or inventory.get("session_date") != str(day)
            or inventory.get("logical_event_journal_sha256") != restart["inputs"]["events.jsonl"]["sha256"]):
        raise ValueError("restart snapshot or signal inventory is not one coherent pinned prefix")
    for row in (inventory.get("modes") or {}).values():
        path = Path(str(row.get("summary_path") or ""))
        if not path.is_file() or _hash(path) != row.get("summary_sha256"):
            raise ValueError("first signal summary source changed")
    prefixes = verify_prefix(state_dir, restart)
    snapshot = json.loads(export.read_bytes())
    modes = snapshot.get("modes") or {}
    if (snapshot.get("simulation_only") is not True or snapshot.get("production_order_possible") is not False
            or set(modes) != set(inventory.get("modes") or {})):
        raise ValueError("snapshot paper contract or enabled mode set differs")
    size = restart["inputs"]["fills.jsonl"]["size_bytes"]
    rows: list[dict] = []
    with open_paper_journal(state_dir / "fills.jsonl", "rb") as stream:
        while stream.tell() < size:
            line = stream.readline()
            if not line or stream.tell() > size or not line.endswith(b"\n"):
                raise ValueError("pinned fill prefix is not a complete line boundary")
            if str(day).encode() in line:
                row = json.loads(line)
                if row.get("session_date") == str(day):
                    rows.append(row)
    result = reconstruct(day=day, modes=modes, fills=rows, signal_inventory=inventory)
    pnl_arithmetic = audit_pnl_arithmetic(modes=modes, fills=rows)
    fee_arithmetic = audit_recorded_fee_rates(modes=modes, fills=rows)
    if verify_prefix(state_dir, restart) != prefixes:
        raise RuntimeError("source prefixes changed during replay")
    return {
        "contract": "independent_pinned_paper_fill_inventory_replay_v1", "session_date": str(day),
        "status": "inventory_reconciled_only" if result["quantity_reconciliation_passed"] else "inventory_reconciliation_failed",
        "simulation_only": True, "production_order_possible": False, "production_state_modified": False,
        "promotable_ledger": False, "source_journal_prefixes": prefixes,
        "restart_receipt_sha256": _hash(restart_path), "first_signal_inventory_sha256": _hash(inventory_path),
        "fill_rows": len(rows), "fill_purpose_counts": dict(Counter(str(row.get("purpose")) for row in rows)),
        "caveat": "This replays retained paper fill quantities, including already-labelled counterfactuals, and checks recorded PnL and retained-rate arithmetic only; it does not rerun progressive revisions or validate broker fee terms, fill prices, or economic PnL.",
        "pnl_arithmetic": pnl_arithmetic,
        "recorded_fee_arithmetic": fee_arithmetic,
        **result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--restart-audit", type=Path, required=True)
    parser.add_argument("--signal-inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output, state_dir = args.output.resolve(), args.state_dir.resolve()
    if output.is_relative_to(state_dir) or output.exists():
        raise ValueError("candidate output must be new and outside the production ledger")
    result = audit(day=args.date, state_dir=state_dir, restart_path=args.restart_audit.resolve(),
                   inventory_path=args.signal_inventory.resolve())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"status": result["status"], "fill_rows": result["fill_rows"],
                      "errors": len(result["errors"]),
                      "pnl_arithmetic_errors": len(result["pnl_arithmetic"]["errors"]),
                      "output": str(output)}, ensure_ascii=False))
    if result["errors"] or result["pnl_arithmetic"]["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
