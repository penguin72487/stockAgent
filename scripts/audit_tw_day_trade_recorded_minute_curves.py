#!/usr/bin/env python3
"""Audit the existing paper marks against the retained fills and state.

This verifies the 270-minute clock, recorded equity arithmetic, endpoint, and
receipt hashes. It cannot certify prices or repair a different signal replay.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_tw_day_trade_restart import verify_prefix
from stockagent.live.paper_journal import open_paper_journal

TAIPEI = ZoneInfo("Asia/Taipei")


def _equity_valid(row: dict[str, Any]) -> bool:
    try:
        if any(row.get(name) is None for name in ("initial_capital_twd", "cumulative_realized_net_pnl_twd",
                                                   "open_net_liquidation_pnl_twd", "total_equity_twd")):
            return False
        initial, realized, unrealized, actions, carry, equity = (
            float(row.get(name) or 0) for name in (
                "initial_capital_twd", "cumulative_realized_net_pnl_twd", "open_net_liquidation_pnl_twd",
                "cumulative_corporate_action_net_twd", "cumulative_carry_cost_twd", "total_equity_twd"))
        return all(map(math.isfinite, (initial, realized, unrealized, actions, carry, equity))) and math.isclose(
            initial + realized + unrealized + actions - carry, equity, rel_tol=1e-10, abs_tol=1e-6)
    except (TypeError, ValueError, OverflowError):
        return False


def inspect_marks(*, day: date, modes: dict[str, Any], path: Path) -> dict[str, Any]:
    start = datetime.combine(day, datetime.min.time(), tzinfo=TAIPEI).replace(hour=9, minute=1)
    expected = {(start + timedelta(minutes=i)).isoformat(timespec="minutes") for i in range(270)}
    by_mode: dict[str, dict[str, dict[str, Any]]] = {market: {} for market in modes}
    duplicates: Counter[str] = Counter()
    malformed: Counter[str] = Counter()
    digest = hashlib.sha256()
    before = path.stat()
    with open_paper_journal(path, "rb") as stream:
        for line in stream:
            digest.update(line)
            if str(day).encode() not in line:
                continue
            row = None
            try:
                row = json.loads(line)
                market = row.get("market")
                if row.get("session_date") != str(day) or market not in modes:
                    continue
                moment = datetime.fromisoformat(str(row.get("minute") or ""))
                if moment.utcoffset() is None:
                    raise ValueError("naive minute")
                minute = moment.astimezone(TAIPEI).isoformat(timespec="minutes")
                if minute not in expected or moment.second or moment.microsecond:
                    raise ValueError("minute outside completed session")
                if minute in by_mode[market]:
                    duplicates[market] += 1
                by_mode[market][minute] = row  # The canonical last append remains visible.
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                malformed[str(row.get("market") if isinstance(row, dict) else "unknown")] += 1
    after = path.stat()
    generation = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if generation(before) != generation(after):
        raise RuntimeError("minute journal changed during audit")
    results = {}
    for market, mode in modes.items():
        marks = by_mode[market]
        invalid_arithmetic = sorted(key for key, row in marks.items() if not _equity_valid(row))
        missing_valuation = sorted(key for key, row in marks.items()
                                   if int(row.get("missing_price_position_count") or 0) > 0)
        stale_valuation = sorted(key for key, row in marks.items() if row.get("valuation_stale") is True)
        end = marks.get((start + timedelta(minutes=269)).isoformat(timespec="minutes"))
        try:
            endpoint_matches = end is not None and math.isclose(float(end["total_equity_twd"]),
                    float(mode["total_equity_twd"]), rel_tol=1e-10, abs_tol=1e-6)
        except (TypeError, ValueError, KeyError):
            endpoint_matches = False
        results[market] = {"minute_count": len(marks), "missing_minutes": sorted(expected - set(marks)),
                           "duplicate_minute_appends": duplicates[market], "malformed_rows": malformed[market],
                           "invalid_equity_arithmetic_minutes": invalid_arithmetic,
                           "missing_valuation_minutes": missing_valuation,
                           "stale_valuation_minutes": stale_valuation,
                           "terminal_equity_matches_snapshot": endpoint_matches}
    return {"logical_marks_sha256": digest.hexdigest(), "modes": results,
            "unclassified_malformed_rows": malformed["unknown"],
            "source_market_price_correctness_verified": False,
            "signal_replay_or_broker_fills_verified": False}


def audit(*, day: date, state_dir: Path, restart_path: Path, curve_receipt_path: Path) -> dict[str, Any]:
    restart = json.loads(restart_path.read_bytes())
    snapshot_path = restart_path.with_suffix(".state.json")
    snapshot_bytes = snapshot_path.read_bytes()
    if (restart.get("status") != "accepted" or restart.get("simulation_only") is not True
            or restart.get("production_order_possible") is not False
            or Path(restart.get("state_dir") or "").resolve() != state_dir.resolve()
            or hashlib.sha256(snapshot_bytes).hexdigest() != (restart.get("state_export") or {}).get("sha256")):
        raise ValueError("expected an accepted independent paper restart snapshot")
    prefixes = verify_prefix(state_dir, restart)
    snapshot = json.loads(snapshot_bytes)
    modes = snapshot.get("modes") or {}
    if (snapshot.get("simulation_only") is not True or snapshot.get("production_order_possible") is not False
            or not modes or any(mode.get("session_date") != str(day) for mode in modes.values())):
        raise ValueError("snapshot is not the requested simulation session")
    receipt_bytes = curve_receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    result = inspect_marks(day=day, modes=modes, path=state_dir / "marks.jsonl")
    if verify_prefix(state_dir, restart) != prefixes or curve_receipt_path.read_bytes() != receipt_bytes:
        raise RuntimeError("source ledger prefix or minute receipt changed during audit")
    result.update({"contract": "recorded_paper_minute_curve_integrity_v1", "session_date": str(day),
                   "status": "recorded_curve_integrity_only_not_corrected_replay",
                   "simulation_only": True, "production_order_possible": False, "ledger_modified": False,
                   "curve_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
                   "receipt_pins_restart_fills": receipt.get("unchanged_fills_sha256") == restart["inputs"]["fills.jsonl"]["sha256"],
                   "receipt_pins_current_marks": (receipt.get("outputs") or {}).get("marks", {}).get("sha256") == result["logical_marks_sha256"]})
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--restart-audit", type=Path, required=True)
    parser.add_argument("--curve-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    state_dir, output = args.state_dir.resolve(), args.output.resolve()
    if output.is_relative_to(state_dir):
        raise ValueError("audit output cannot be in the live ledger")
    result = audit(day=args.date, state_dir=state_dir, restart_path=args.restart_audit.resolve(),
                   curve_receipt_path=args.curve_receipt.resolve())
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"status": result["status"], "receipt_pins_restart_fills": result["receipt_pins_restart_fills"],
                      "receipt_pins_current_marks": result["receipt_pins_current_marks"],
                      "modes": {market: {"minutes": row["minute_count"], "missing": len(row["missing_minutes"]),
                                        "bad_arithmetic": len(row["invalid_equity_arithmetic_minutes"]),
                                        "endpoint_matches": row["terminal_equity_matches_snapshot"]}
                                for market, row in result["modes"].items()}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
