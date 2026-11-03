#!/usr/bin/env python3
"""Compare quarantined one-minute futures windows with retained native K-bars.

Both datasets may share an upstream source. A match is a consistency check,
not independent market verification, a completeness proof, or a fill. Missing
rows are unknown; this script never changes execution or training admission.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts.audit_tw_futures_session_envelope import read_market_object


CLOCK_MINUTES = {"08:45": "08:46:00", "09:00": "09:01:00"}


def classify_window(tick: dict | None, bars: list[dict] | None) -> str:
    """Classify one window without making missing-source/zero-volume claims."""
    if bars is None:
        return "bar_source_unavailable"
    if len(bars) > 1:
        return "bar_identity_ambiguous"
    if not bars:
        return "tick_only" if tick is not None else "neither_observed"
    bar = bars[0]
    try:
        o, h, l, c, volume = (float(bar[k]) for k in ("open", "high", "low", "close", "volume"))
    except (KeyError, TypeError, ValueError):
        return "bar_invalid"
    if (not all(math.isfinite(v) for v in (o, h, l, c, volume))
            or min(o, h, l, c, volume) <= 0 or volume != math.floor(volume)
            or int(volume) % 2 or not (l <= min(o, c) <= max(o, c) <= h)):
        return "bar_invalid"
    if tick is None:
        return "bar_only"
    try:
        quote, quantity = float(tick["entry_quote"]), float(tick["entry_volume"])
    except (KeyError, TypeError, ValueError):
        return "tick_invalid"
    if not math.isfinite(quote) or not math.isfinite(quantity) or quote <= 0 or quantity <= 0:
        return "tick_invalid"
    # K-bar native volume uses FinMind's two-sided convention. The candidate
    # is an outright-contract volume and its quote is VWAP, not bar open/close.
    tolerance = max(1e-6, max(abs(l), abs(h)) * 1e-6)
    if volume == 2.0 * quantity and l - tolerance <= quote <= h + tolerance:
        return "volume_and_vwap_range_match"
    return "tick_bar_disagree"


def audit(quarantine: Path, candidates: Path, provider_root: Path, *,
          quarantine_sha: str, candidates_sha: str, selected_days: set[str] | None = None) -> dict:
    if sha256_file(quarantine) != quarantine_sha or sha256_file(candidates) != candidates_sha:
        raise ValueError("input source differs from pinned quarantine or candidates")
    doc = json.loads(quarantine.read_text())
    if doc.get("contract") != "tw_futures_unknown_entry_quarantine_v1":
        raise ValueError("unsupported quarantine source")
    gaps = doc["contract_days"]
    keys: set[tuple[str, str]] = set()
    product_keys: set[tuple[str, str, str]] = set()
    product_for_physical: dict[tuple[str, str], tuple[str, str]] = {}
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in gaps:
        day, physical, product, delivery = (row[k] for k in ("date", "physical_contract", "product", "delivery_month"))
        key, product_key = (day, physical), (day, product, delivery)
        if key in keys or product_key in product_keys:
            raise ValueError("quarantine has ambiguous physical or product/month identities")
        keys.add(key)
        product_keys.add(product_key)
        product_for_physical[key] = (product, delivery)
        by_day[day].append(row)
    if selected_days is not None:
        if not selected_days or not selected_days <= set(by_day):
            raise ValueError("selected days must exist in quarantine")
        by_day = {day: rows for day, rows in by_day.items() if day in selected_days}

    frame = pl.read_parquet(candidates).filter(pl.col("window_minutes") == 1)
    candidates_by_key = {}
    for row in frame.iter_rows(named=True):
        day = str(row["date"])
        key = (day, row["physical_contract"], row["decision_time"])
        if key in candidates_by_key or (day, row["physical_contract"]) not in keys or row["decision_time"] not in CLOCK_MINUTES:
            raise ValueError("candidate is duplicated or outside the quarantine")
        if product_for_physical[(day, row["physical_contract"])] != (row["product"], row["contract"]):
            raise ValueError("candidate product/month differs from quarantine")
        candidates_by_key[key] = row

    counts: Counter[tuple[str, str]] = Counter()
    provider_states: Counter[str] = Counter()
    objects = []
    examples = []
    ordered = sorted(by_day)
    for position, day in enumerate(ordered, 1):
        receipt = provider_root / "receipts" / "TaiwanFuturesKBar" / "all" / f"{day}.json"
        bars_by_key: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
        available = False
        if receipt.is_file():
            info = json.loads(receipt.read_text())
            if info.get("status") == "complete":
                bars, proof = read_market_object(provider_root, "TaiwanFuturesKBar", day)
                available = True
                objects.append(proof)
                provider_states["receipt_verified"] += 1
                relevant = bars.filter(pl.col("minute").is_in(list(CLOCK_MINUTES.values())))
                for bar in relevant.iter_rows(named=True):
                    if bar["date"] != day:
                        raise ValueError("retained K-bar source contains a different market date")
                    bars_by_key[(bar["futures_id"], bar["contract_date"], bar["minute"])].append(bar)
            else:
                provider_states["receipt_not_complete"] += 1
        else:
            provider_states["receipt_missing"] += 1
        for row in by_day[day]:
            for clock, minute in CLOCK_MINUTES.items():
                tick = candidates_by_key.get((day, row["physical_contract"], clock))
                bars = bars_by_key.get((row["product"], row["delivery_month"], minute), []) if available else None
                state = classify_window(tick, bars)
                counts[(clock, state)] += 1
                if state in {"bar_only", "tick_only", "tick_bar_disagree", "bar_invalid", "bar_identity_ambiguous", "tick_invalid"} and len(examples) < 30:
                    examples.append({"date": day, "product": row["product"], "contract": row["delivery_month"],
                                     "physical_contract": row["physical_contract"], "decision_time": clock,
                                     "state": state, "tick_quote": None if tick is None else tick["entry_quote"],
                                     "tick_volume": None if tick is None else tick["entry_volume"],
                                     "bar_rows": bars})
        if position % 100 == 0:
            print(f"[K-bar audit] {position}/{len(ordered)} source dates", file=sys.stderr, flush=True)
    if sha256_file(quarantine) != quarantine_sha or sha256_file(candidates) != candidates_sha:
        raise ValueError("a quarantine or candidate input changed during audit")
    return {"contract": "tw_futures_gap_retained_kbar_window_audit_v1",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "selected_contract_days": sum(len(rows) for rows in by_day.values()),
            "selected_dates": len(ordered), "provider_date_states": dict(provider_states),
            "counts_by_clock_and_state": [{"clock": clock, "state": state, "contract_days": count}
                for (clock, state), count in sorted(counts.items())],
            "examples": examples, "verified_kbar_source_objects": objects,
            "input_sha256": {"quarantine": quarantine_sha, "partial_candidates": candidates_sha,
                             "implementation": sha256_file(Path(__file__))},
            "training_ready": False, "independent_provider": False,
            "limitations": ["No matching or missing K-bar proves complete ticks, a fill, or the historical receive time.",
                "K-bar and ticks may have the same upstream source; VWAP is checked only against bar range.",
                "No source is promoted, no quarantined window is released, and unknown bars are not zero trades."]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("quarantine", "candidates", "provider-root", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--quarantine-sha", required=True)
    parser.add_argument("--candidates-sha", required=True)
    parser.add_argument("--day", action="append", default=[])
    args = parser.parse_args()
    output = args.output.resolve()
    if not output.is_relative_to(ROOT / "artifacts/markets") or output.exists() or output.is_symlink():
        raise ValueError("choose a new report under artifacts/markets")
    result = audit(args.quarantine, args.candidates, args.provider_root,
                   quarantine_sha=args.quarantine_sha, candidates_sha=args.candidates_sha,
                   selected_days=set(args.day) if args.day else None)
    atomic_write_json(output, result)
    print(json.dumps({key: result[key] for key in ("selected_contract_days", "selected_dates",
        "provider_date_states", "counts_by_clock_and_state")}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
