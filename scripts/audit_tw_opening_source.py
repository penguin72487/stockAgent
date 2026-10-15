#!/usr/bin/env python3
"""Measure current-session opening acquisition without inference or orders.

The full frozen signal universe is retained. Default receipts are isolated;
--apply-current-session explicitly repairs the canonical opening source cache.
This is a source-availability audit, NOT a historical opening/fill latency test.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl

from stockagent.live.quote_provider import tw_session_open_observed_mask
from stockagent.live.signal_engine import _price_snapshot
from stockagent.live.tw_day_trade_simulation import _atomic_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--weights", type=Path, required=True)
    parser.add_argument("--parquet-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--apply-current-session", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.rounds <= 10:
        parser.error("rounds must be between 1 and 10")
    frame = pl.read_parquet(args.weights)
    session = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    if set(frame["date"].cast(pl.String).str.slice(0, 10).to_list()) != {session}:
        parser.error("weights must belong to the current session; never backdate current quotes")
    frame = frame.filter(pl.col("alive"))
    symbols = frame["symbol"].to_list()
    if len(set(symbols)) != len(symbols) or not symbols:
        parser.error("active universe must be nonempty with unique symbols")
    fallback = frame["base_price"].to_numpy().astype(np.float64)
    args.output_root.mkdir(parents=True, exist_ok=True)
    if not args.apply_current_session:
        os.environ["STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT"] = str(args.output_root / "isolated_source_receipts")
    else:
        canonical_root = Path(os.getenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT") or "artifacts/live/tw_opening_snapshots")
        receipt = canonical_root / f"{session}.json"
        if receipt.exists():
            original = json.loads(receipt.read_text(encoding="utf-8"))
            _atomic_json(args.output_root / "source_receipt_before.json", original)
    input_sha = hashlib.sha256(args.weights.read_bytes()).hexdigest()
    report = {
        "schema_version": 1, "session_date": session,
        "audit_started_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="milliseconds"),
        "basis": "current_session_source_availability_not_real_0900_latency",
        "weights_path": str(args.weights.resolve()), "weights_sha256": input_sha,
        "parquet_root": str(args.parquet_root.resolve()), "requested_count": len(symbols),
        "canonical_source_cache_updated": args.apply_current_session,
        "inference_performed": False, "ledger_modified": False, "orders_submitted": False,
        "rounds": [],
    }
    selected = np.abs(frame["model_weight"].to_numpy()) > 0
    overall_started = time.perf_counter()
    for attempt in range(1, args.rounds + 1):
        started = time.perf_counter()
        result = _price_snapshot(
            symbols=symbols, fallback_prices=fallback, source="tw",
            parquet_root=args.parquet_root, prices_csv=None, yahoo_chunk_size=80,
            require_official_tw_session_open=True,
        )
        valid = tw_session_open_observed_mask(result, session_date=session)
        item = {
            "attempt": attempt, "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "source": result.source, "response_count": result.available_count,
            "valid_open_count": int(np.count_nonzero(valid)),
            "missing_count": int(np.count_nonzero(~valid)),
            "missing_symbols": [symbols[int(idx)] for idx in np.flatnonzero(~valid)],
            "selected_model_count": int(np.count_nonzero(selected)),
            "selected_model_missing_count": int(np.count_nonzero(selected & ~valid)),
            "timing": result.transport_timing,
        }
        report["rounds"].append(item)
        report["overall_wall_ms"] = round((time.perf_counter() - overall_started) * 1000.0, 3)
        report["all_symbols_complete"] = bool(np.all(valid))
        _atomic_json(args.output_root / "report.json", report)
        _atomic_json(args.output_root / f"observed_opens_{attempt}.json", {
            "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="milliseconds"),
            "session_date": session, "source": result.source,
            "opens": {symbols[int(idx)]: float(result.open_prices[idx]) for idx in np.flatnonzero(valid)},
        })
        print(json.dumps({k: v for k, v in item.items() if k not in {"missing_symbols", "timing"}}, ensure_ascii=False), flush=True)
        if np.all(valid):
            break
    if hashlib.sha256(args.weights.read_bytes()).hexdigest() != input_sha:
        raise RuntimeError("original weights changed during the audit")
    return 0  # Partial coverage is recorded explicitly, not a claimed acceptance.


if __name__ == "__main__":
    raise SystemExit(main())
