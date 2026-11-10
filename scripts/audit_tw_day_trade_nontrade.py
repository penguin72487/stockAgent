#!/usr/bin/env python3
"""Independently reconcile a candidate's non-execution paper settlements."""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import repair_tw_day_trade_flat_opening as repair
from stockagent.live.paper_journal import open_paper_journal
from stockagent.live.tw_day_trade_nontrade import CONTRACT


def _rows(path: Path) -> list[dict]:
    with open_paper_journal(path) as source:
        rows = []
        for line in source:
            if not line.endswith(b"\n"):
                raise RuntimeError(f"torn candidate journal: {path.name}")
            value = json.loads(line)
            if not isinstance(value, dict):
                raise RuntimeError(f"non-object candidate row: {path.name}")
            rows.append(value)
        return rows


def _same(actual: object, expected: float) -> bool:
    try:
        return math.isfinite(float(actual)) and math.isclose(float(actual), expected, rel_tol=1e-9, abs_tol=1e-5)
    except (TypeError, ValueError, OverflowError):
        return False


def audit(operation_root: Path) -> dict:
    root = operation_root.resolve(strict=True)
    manifest_path = root / "ledger_repair_manifest.json"
    manifest = repair._object(manifest_path)
    if (manifest.get("status") != "candidate_validated_not_applied"
            or not manifest.get("nontrade_tick_receipt")
            or manifest.get("provisional_minute_source_coverage") is not True):
        raise RuntimeError("audit requires an explicitly authorized provisional nontrade candidate")
    repair._assert_source_pins(manifest["source_pins"])
    candidate = Path(manifest["candidate_dir"]).resolve(strict=True)
    mode = repair._object(candidate / "state.json")["modes"][manifest["market"]]
    day = str(manifest["sessions"][0]["session_date"])
    positions = {key: p for key, p in mode["positions"].items()
                 if p.get("session_date") == day and isinstance(p.get("nontrade_settlement"), dict)}
    fills, orders, events = (_rows(candidate / name) for name in ("fills.jsonl", "orders.jsonl", "events.jsonl"))
    if any(str(row.get("purpose") or "").startswith("nontrade_") for row in (*fills, *orders)):
        raise RuntimeError("nontrade bookkeeping must not create order/fill journal rows")
    exits, entries = defaultdict(list), defaultdict(list)
    for row in fills:
        if row.get("position_id") in positions:
            (entries if row.get("purpose") in {"entry", "entry_completion"} else exits)[row["position_id"]].append(row)
    selected_events = [row for row in events if row.get("event") == "nontrade_last_observed_accounting_settlement"]
    event_by_position = {row.get("position_id"): row for row in selected_events}
    if len(event_by_position) != len(selected_events) or set(event_by_position) != set(positions):
        raise RuntimeError("nontrade event/position ownership mismatch")
    shares = 0
    total_nontrade_net = 0.
    before_entry, official_ties = [], []
    for key, position in positions.items():
        d = position["nontrade_settlement"]
        ref = d["reference"]
        event = event_by_position[key]
        qty = int(d["quantity"])
        if (d.get("contract") != CONTRACT or d.get("simulated_fill") is not False
                or d.get("simulated_order") is not False or d.get("broker_fill") is not False
                or d.get("exchange_fill") is not False or d.get("source_completeness_verified") is not False
                or qty <= 0 or qty % int(position["lot_size"])
                or int(position.get("signed_shares") or 0) != 0
                or any(event.get(name) != value for name, value in d.items())):
            raise RuntimeError(f"invalid nontrade settlement disclosure: {key}")
        if sum(int(row["quantity"]) for row in entries[key]) != qty + sum(int(row["quantity"]) for row in exits[key]):
            raise RuntimeError(f"nontrade inventory does not reconcile: {key}")
        if any(row.get("entry_fee_allocated_twd") is None for row in exits[key]):
            raise RuntimeError(f"actual partial-exit fee allocation missing: {key}")
        entry_fee = sum(float(row["fee_and_tax_twd"]) for row in entries[key])
        allocated = entry_fee - sum(float(row["entry_fee_allocated_twd"]) for row in exits[key])
        price = float(ref["reference_price"])
        direction = 1 if position["side"] == "long" else -1
        gross = direction * qty * (price - float(position.get("inventory_basis_price", position["entry_price"])))
        rate = float(position["sell_fee_rate" if direction > 0 else "buy_fee_rate"])
        rebate = qty * price * float(position.get("commission_rebate_rate") or 0)
        fee = qty * price * rate - rebate
        net = gross - allocated - fee
        prior_simulated_net = sum(float(row["net_pnl_twd"]) for row in exits[key])
        if (not _same(d.get("entry_fee_allocated_twd"), allocated)
                or not _same(d.get("gross_pnl_twd"), gross)
                or not _same(d.get("assumed_exit_fee_and_tax_twd"), fee)
                or not _same(d.get("assumed_commission_rebate_twd"), rebate)
                or not _same(d.get("net_pnl_twd"), net)
                or not _same(position.get("realized_net_pnl_twd"), prior_simulated_net + net)):
            raise RuntimeError(f"nontrade gross/fee/rebate/net accounting mismatch: {key}")
        observation = datetime.fromisoformat(ref["reference_observed_at"])
        entry = datetime.fromisoformat(position["entry_at"])
        book_at = datetime.fromisoformat(d["accounting_at"])
        recorded = datetime.fromisoformat(event["recorded_at"])
        if (observation.date().isoformat() != day or observation > book_at or book_at > recorded
                or d.get("reference_before_simulated_entry") != (observation < entry)):
            raise RuntimeError(f"nontrade observation/accounting/recording clock mismatch: {key}")
        if observation < entry:
            before_entry.append(position["symbol"])
        if ref.get("same_clock_price_tie_resolved_by_official_close"):
            official_ties.append(position["symbol"])
        shares += qty
        total_nontrade_net += net
    prior = float(manifest["anchor"].get("cumulative_nontrade_settlement_net_pnl_twd") or 0.)
    if (not _same(mode.get("cumulative_nontrade_settlement_net_pnl_twd"), prior + total_nontrade_net)
            or mode.get("execution_evidence_complete") is not False
            or mode.get("nontrade_settlement_count") != len(positions)
            or mode.get("nontrade_settlement_shares") != shares):
        raise RuntimeError("nontrade account-level totals or execution disclosure do not reconcile")
    evidence = {"status": "verified_with_data_gaps", "market": manifest["market"], "session_date": day,
                "manifest_sha256": repair._sha256(manifest_path), "candidate_state_sha256": repair._sha256(candidate / "state.json"),
                "candidate_fills_sha256": repair._sha256(candidate / "fills.jsonl"),
                "candidate_events_sha256": repair._sha256(candidate / "events.jsonl"),
                "auditor_sha256": repair._sha256(Path(__file__)), "settlement_positions": len(positions),
                "settlement_shares": shares, "settlement_net_pnl_twd": total_nontrade_net,
                "reference_before_entry_symbols": sorted(before_entry),
                "same_clock_ties_resolved_by_official_close": sorted(official_ties),
                "simulation_only": True, "simulated_order": False, "simulated_fill": False,
                "exchange_fill": False, "broker_fill": False,
                "source_completeness_verified": False}
    repair._assert_source_pins(manifest["source_pins"])
    repair._atomic_json(root / "nontrade_accounting_audit.json", evidence)
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation-root", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.operation_root), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
