#!/usr/bin/env python3
"""Audit legacy 09:01 stock-Tick volume units without modifying a live ledger.

The report measures *capacity evidence*, not hypothetical broker fills.  It
checks each matching signal against the immutable missed-opening price receipt
and never infers a price or volume for a missing source row.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


TICK_SOURCE = "shioaji:historical_ticks_0900_090059_vwap_right_label_0901"
DEFAULT_LEDGER = Path("artifacts/live/tw_day_trade_simulation/signals.jsonl")
DEFAULT_RECEIPTS = Path("artifacts/live/tw_day_trade_simulation/missed_opening_0901_prices")


def _source_family(value: Any) -> str:
    source = str(value or "unknown")
    # Local minute provenance embeds a per-symbol filesystem path.  The
    # physical file is not a different price/volume ABI, and including it in
    # the grouping obscures date-level coverage with thousands of keys.
    if source.startswith("local_minute_parquet_0901_"):
        return source.partition(":")[0]
    return source


def _receipt_rows(directory: Path) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    rows: dict[str, dict[str, Any]] = {}
    digests: dict[str, str] = {}
    for path in sorted(directory.glob("????-??-??.json")):
        raw = path.read_bytes()
        payload = json.loads(raw)
        day = path.stem
        accepted_contracts = {
            1: "right_labelled_09_01_minute_vwap_from_09_00_00_to_09_00_59_ticks",
            2: "source_backed_right_labelled_09_01_minute_price_vwap_else_kbar_close",
        }
        if (
            not isinstance(payload, dict)
            or payload.get("execution_price_contract")
            != accepted_contracts.get(payload.get("schema_version"))
            or payload.get("session_date") != day
            or payload.get("simulation_only") is not True
            or payload.get("production_order_possible") is not False
            or not isinstance(payload.get("prices"), dict)
        ):
            raise ValueError(f"invalid missed-opening receipt: {path}")
        rows[day] = payload["prices"]
        digests[day] = hashlib.sha256(raw).hexdigest()
    return rows, digests


def audit(
    ledger_path: Path,
    receipt_dir: Path,
    *,
    lot_size: int = 1_000,
) -> dict[str, Any]:
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    receipts, digests = _receipt_rows(receipt_dir)
    counters: dict[str, Counter[str]] = defaultdict(Counter)
    source_coverage: dict[str, Counter[str]] = defaultdict(Counter)
    policy_coverage: dict[str, Counter[str]] = defaultdict(Counter)
    missing_price_symbols: dict[str, set[str]] = defaultdict(set)
    first_session: str | None = None
    last_session: str | None = None
    source_errors: Counter[str] = Counter()
    source_error_examples: list[dict[str, str]] = []
    legacy_capacity_corrections: list[dict[str, Any]] = []
    seen_signal_keys: set[tuple[str, str, str]] = set()
    ledger_size = ledger_path.stat().st_size
    with ledger_path.open("rb") as handle:
        while handle.tell() < ledger_size:
            raw = handle.readline()
            if not raw or not raw.endswith(b"\n"):
                break
            row = json.loads(raw)
            day = str(row.get("session_date") or "")
            if day:
                first_session = day if first_session is None else min(first_session, day)
                last_session = day if last_session is None else max(last_session, day)
            source = _source_family(row.get("quote_source"))
            coverage = source_coverage[f"{day}|{source}"]
            coverage["signal_rows"] += 1
            if int(row.get("requested_shares") or 0) > 0:
                coverage["requested_rows"] += 1
                coverage["requested_shares"] += int(row.get("requested_shares") or 0)
                coverage["recorded_filled_shares"] += int(row.get("filled_shares") or 0)
                if source == "missing_observed_09_01_minute_price":
                    missing_price_symbols[day].add(str(row.get("symbol") or ""))
            policy = str(row.get("entry_fill_policy") or "unknown")
            policy_counts = policy_coverage[f"{day}|{policy}"]
            policy_counts["signal_rows"] += 1
            if int(row.get("requested_shares") or 0) > 0:
                policy_counts["requested_rows"] += 1
            if TICK_SOURCE.encode() not in raw:
                continue
            if row.get("quote_source") != TICK_SOURCE:
                continue
            mode = str(row.get("market") or "")
            symbol = str(row.get("symbol") or "")
            signal_key = (day, mode, symbol)
            if signal_key in seen_signal_keys:
                source_errors["duplicate_session_mode_symbol"] += 1
                continue
            seen_signal_keys.add(signal_key)
            key = f"{day}|{mode}"
            count = counters[key]
            count["tick_signal_rows"] += 1
            source = receipts.get(day, {}).get(symbol)
            if (
                not isinstance(source, dict)
                or source.get("source") != TICK_SOURCE
                or source.get("symbol") != symbol
            ):
                source_errors["receipt_row_missing_or_wrong_source"] += 1
                if len(source_error_examples) < 10:
                    source_error_examples.append({"day": day, "mode": mode, "symbol": symbol})
                continue
            raw_lots = source.get("tick_volume_units_0901")
            stored_lots = row.get("minute_kbar_volume_lots")
            try:
                raw_lots = float(raw_lots)
                stored_lots = float(stored_lots)
            except (TypeError, ValueError):
                source_errors["invalid_volume"] += 1
                continue
            if not math.isfinite(raw_lots) or raw_lots < 0 or not math.isfinite(stored_lots):
                source_errors["invalid_volume"] += 1
                continue
            corrected_unit = math.isclose(
                stored_lots, raw_lots, rel_tol=1e-8, abs_tol=1e-8
            )
            legacy_unit = math.isclose(
                stored_lots, raw_lots / lot_size, rel_tol=1e-8, abs_tol=1e-8
            )
            if not corrected_unit and not legacy_unit:
                source_errors["ledger_receipt_volume_mismatch"] += 1
                if len(source_error_examples) < 10:
                    source_error_examples.append({"day": day, "mode": mode, "symbol": symbol})
                continue
            count["receipt_verified_rows"] += 1
            count["corrected_unit_rows" if corrected_unit else "legacy_underdivided_rows"] += 1
            requested = int(row.get("requested_shares") or 0)
            if requested <= 0:
                continue
            count["requested_rows"] += 1
            count["requested_shares"] += requested
            count["recorded_filled_shares"] += int(row.get("filled_shares") or 0)
            participation = float(row.get("minute_volume_participation") or 0)
            if not math.isfinite(participation) or not 0 < participation <= 1:
                source_errors["invalid_participation"] += 1
                continue
            old_capacity = int(row.get("minute_kbar_capacity_shares") or 0)
            corrected_capacity = math.floor(raw_lots * participation) * lot_size
            if corrected_unit and old_capacity != corrected_capacity:
                source_errors["corrected_capacity_mismatch"] += 1
                continue
            if old_capacity < corrected_capacity:
                count["understated_capacity_rows"] += 1
                legacy_capacity_corrections.append({
                    "session_date": day,
                    "market": mode,
                    "symbol": symbol,
                    "requested_shares": requested,
                    "recorded_filled_shares": int(row.get("filled_shares") or 0),
                    "recorded_capacity_shares": old_capacity,
                    "source_backed_capacity_shares": corrected_capacity,
                    "receipt_sha256": digests[day],
                })
            if old_capacity == 0 and corrected_capacity > 0:
                count["false_zero_capacity_rows"] += 1
            if corrected_capacity == 0:
                count["still_zero_capacity_rows"] += 1
            elif corrected_capacity >= requested:
                count["corrected_capacity_full_rows"] += 1
            else:
                count["corrected_capacity_partial_rows"] += 1
    return {
        "schema_version": 1,
        "created_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds"),
        "claim": "receipt_verified_minute_capacity_only_not_fills_or_replay_promotion",
        "unresolved_0901_price_requested_rows": sum(
            counts.get("requested_rows", 0)
            for key, counts in source_coverage.items()
            if key.endswith("|missing_observed_09_01_minute_price")
        ),
        "lot_size_shares": lot_size,
        "ledger_path": str(ledger_path.resolve()),
        "ledger_size_bytes_at_start": ledger_size,
        "first_session": first_session,
        "last_session": last_session,
        "source_coverage_by_date": {
            key: dict(value) for key, value in sorted(source_coverage.items())
        },
        "execution_policy_by_date": {
            key: dict(value) for key, value in sorted(policy_coverage.items())
        },
        "missing_0901_price_symbols_by_date": {
            day: sorted(symbols - {""})
            for day, symbols in sorted(missing_price_symbols.items())
        },
        "receipt_dir": str(receipt_dir.resolve()),
        "receipt_sha256_by_date": digests,
        "by_date_mode": {key: dict(value) for key, value in sorted(counters.items())},
        "source_errors": dict(source_errors),
        "source_error_examples": source_error_examples,
        "legacy_capacity_corrections": sorted(
            legacy_capacity_corrections,
            key=lambda value: (
                value["session_date"], value["market"], value["symbol"]
            ),
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--receipt-dir", type=Path, default=DEFAULT_RECEIPTS)
    parser.add_argument("--lot-size", type=int, default=1_000)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = audit(args.ledger, args.receipt_dir, lot_size=args.lot_size)
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.exists():
            raise FileExistsError(args.output)
        args.output.write_text(encoded, encoding="utf-8")
        print(args.output)


if __name__ == "__main__":
    main()
