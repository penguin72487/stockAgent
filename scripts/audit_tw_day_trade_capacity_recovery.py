#!/usr/bin/env python3
"""Read-only inventory of historical paper entry gaps; never edits the live ledger."""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


TAIPEI = ZoneInfo("Asia/Taipei")
CAPACITY_REASONS = {
    "observed_09_01_minute_liquidity_unavailable",
    "observed_09_01_minute_capacity_exhausted",
    "marketable_depth_unavailable",
    "marketable_depth_exhausted",
}


def _rows(path: Path):
    with path.open("rb") as handle:
        size = path.stat().st_size
        while handle.tell() < size:
            line = handle.readline()
            if not line.endswith(b"\n"):
                break
            yield json.loads(line)


def audit(ledger_dir: Path, through: date, *, collect_missing_pairs: bool = False) -> dict:
    fills_path = ledger_dir / "fills.jsonl"
    signals_path = ledger_dir / "signals.jsonl"
    if not fills_path.is_file() or not signals_path.is_file():
        raise FileNotFoundError("both fills.jsonl and signals.jsonl are required")
    prior: dict[tuple[str, str, str], int] = Counter()
    for row in _rows(fills_path):
        session = str(row.get("session_date") or "")
        if (session <= through.isoformat() and row.get("purpose") == "entry"
                and row.get("simulation_only") is True):
            prior[(session, str(row.get("market") or ""),
                   str(row.get("symbol") or ""))] += int(row.get("quantity") or 0)
    by_market: dict[str, Counter] = {}
    by_day: dict[str, Counter] = {}
    reasons: Counter = Counter()
    missing_pairs: set[tuple[str, str]] = set()
    for row in _rows(signals_path):
        session = str(row.get("session_date") or "")
        if not session or session > through.isoformat():
            continue
        market = str(row.get("market") or "")
        symbol = str(row.get("symbol") or "")
        requested = int(row.get("requested_shares") or 0)
        filled = int(row.get("filled_shares") or 0)
        gap = max(0, requested - filled)
        reduction_gap = max(0, int(row.get("reduction_requested_shares") or 0)
                            - int(row.get("reduction_filled_shares") or 0))
        for bucket in (by_market.setdefault(market, Counter()),
                       by_day.setdefault(session, Counter())):
            bucket["signal_rows"] += 1
            bucket["requested_shares"] += requested
            bucket["paper_filled_shares"] += filled
            bucket["unfilled_target_shares"] += gap
            bucket["unfilled_reduction_shares"] += reduction_gap
        reason = str(row.get("reason") or "")
        if gap:
            reasons[reason] += 1
        if gap and reason in CAPACITY_REASONS:
            for bucket in (by_market[market], by_day[session]):
                bucket["capacity_limited_rows"] += 1
                bucket["capacity_limited_gap_shares"] += gap
                if prior.get((session, market, symbol), 0) > 0:
                    bucket["capacity_gap_with_prior_paper_fill_shares"] += gap
                else:
                    bucket["capacity_gap_without_prior_paper_fill_shares"] += gap
        if gap and reason == "observed_09_01_minute_price_unavailable":
            has_prior = prior.get((session, market, symbol), 0) > 0
            for bucket in (by_market[market], by_day[session]):
                bucket["missing_0901_price_rows"] += 1
                bucket["missing_0901_price_gap_shares"] += gap
                if has_prior:
                    bucket["missing_0901_price_with_prior_paper_fill_rows"] += 1
                    bucket["missing_0901_price_with_prior_paper_fill_shares"] += gap
            if collect_missing_pairs and not has_prior:
                missing_pairs.add((session, symbol))
        if reduction_gap:
            for bucket in (by_market[market], by_day[session]):
                bucket["reduction_gap_rows"] += 1
    return {
        "schema_version": 1,
        "created_at": datetime.now(TAIPEI).isoformat(timespec="seconds"),
        "through": through.isoformat(),
        "source": {"signals": str(signals_path), "fills": str(fills_path),
                   "signal_size_bytes_at_end": signals_path.stat().st_size,
                   "fill_size_bytes_at_end": fills_path.stat().st_size},
        "claim": "read-only local-paper inventory, not broker fills or a stateful replay result",
        "capacity_reasons": sorted(CAPACITY_REASONS),
        "by_market": {k: dict(v) for k, v in sorted(by_market.items())},
        "by_day": {k: dict(v) for k, v in sorted(by_day.items())},
        "unfilled_reason_rows": dict(reasons.most_common()),
        "prior_paper_symbol_sessions": len(prior),
        "missing_0901_price_without_prior_unique_symbol_days": len(missing_pairs),
        "missing_0901_price_without_prior_pairs": (
            [{"session_date": day, "symbol": symbol}
             for day, symbol in sorted(missing_pairs)]
            if collect_missing_pairs else None
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger-dir", type=Path, required=True)
    parser.add_argument("--through", type=date.fromisoformat,
                        default=datetime.now(TAIPEI).date() - timedelta(days=1))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--missing-pairs-output", type=Path,
                        help="Optional CSV of unique symbol-days needing a sourced 09:01 price.")
    args = parser.parse_args()
    result = audit(args.ledger_dir, args.through,
                   collect_missing_pairs=args.missing_pairs_output is not None)
    if args.missing_pairs_output is not None:
        args.missing_pairs_output.parent.mkdir(parents=True, exist_ok=True)
        pairs_tmp = args.missing_pairs_output.with_suffix(
            args.missing_pairs_output.suffix + f".tmp.{os.getpid()}")
        with pairs_tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=("session_date", "symbol"))
            writer.writeheader()
            writer.writerows(result["missing_0901_price_without_prior_pairs"] or [])
        pairs_tmp.replace(args.missing_pairs_output)
        result["missing_0901_price_without_prior_pairs"] = None
        result["missing_pairs_output"] = str(args.missing_pairs_output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + f".tmp.{os.getpid()}")
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2,
                                    sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(args.output)
    print(json.dumps({"output": str(args.output), "through": result["through"],
                      "markets": len(result["by_market"]), "days": len(result["by_day"])},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
