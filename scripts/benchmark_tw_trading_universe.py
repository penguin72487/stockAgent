#!/usr/bin/env python3
"""Read-only official-universe validation/mask benchmark; no quotes or orders."""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json
from stockagent.data.tw_current_trading_universe import (
    load_current_universe, opening_scope, _read_receipt, public_current_universe,
)
from stockagent.live.signal_engine import _live_quote_request_key


def measured(samples):
    return {"samples": len(samples), "p50_ms": float(np.percentile(samples, 50)),
            "p95_ms": float(np.percentile(samples, 95)), "max_ms": float(max(samples))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--signal-pointer", action="append", default=[])
    parser.add_argument("--rounds", type=int, default=500)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output is None:
        args.output = Path("artifacts/operations/tw_trading_universe_benchmarks") / args.session_date / (datetime.now(ZoneInfo("Asia/Taipei")).strftime("%H%M%S%f") + ".json")
    if args.rounds < 1:
        parser.error("rounds must be positive")
    _read_receipt.cache_clear()
    started = time.perf_counter()
    receipt = load_current_universe(args.session_date)
    cold_ms = (time.perf_counter() - started) * 1000
    official = {r["symbol"] for r in receipt["records"] if r["trading_status"] == "listed_for_regular_trading"}
    legacy_path = Path("artifacts/live/tw_opening_snapshots") / (args.session_date + ".json")
    legacy = json.loads(legacy_path.read_text()) if legacy_path.exists() else {}
    old = set(legacy.get("requested_symbols") or ())
    modes = []
    for pointer_path in args.signal_pointer:
        path = Path(pointer_path)
        before = path.read_bytes()
        pointer = json.loads(before)
        weights_path = Path(pointer["weights_path"])
        weights_sha = hashlib.sha256(weights_path.read_bytes()).hexdigest()
        columns = list(pl.read_parquet_schema(weights_path))
        symbols = (pl.read_parquet(weights_path, columns=["symbol"])["symbol"].to_list()
                   if "symbol" in columns else [s for s in columns if s not in {"date", "datetime", "timestamp"}])
        mask_samples, key_samples = [], []
        keys = set()
        for _ in range(args.rounds):
            start = time.perf_counter()
            mask, scope = opening_scope(symbols, args.session_date)
            mask_samples.append((time.perf_counter() - start) * 1000)
            start = time.perf_counter()
            keys.add(_live_quote_request_key({"symbols": symbols, "request_mask": mask,
                                             "tw_trading_universe": scope,
                                             "fallback_prices": np.full(len(symbols), np.nan)}))
            key_samples.append((time.perf_counter() - start) * 1000)
        unchanged = (before == path.read_bytes()
                     and weights_sha == hashlib.sha256(weights_path.read_bytes()).hexdigest())
        if not unchanged or len(keys) != 1:
            raise RuntimeError("immutable signal or official request identity changed during benchmark")
        modes.append({"market": pointer["market"], "signal_id": pointer["signal_id"],
                      "pointer_path": str(path), "pointer_sha256": hashlib.sha256(before).hexdigest(),
                      "weights_path": str(weights_path), "weights_sha256": weights_sha,
                      "model_symbol_count": len(symbols), "model_official_trading_count": int(mask.sum()),
                      "official_not_in_model_symbols": scope["official_not_in_model_symbols"],
                      "membership_mask": measured(mask_samples), "request_identity": measured(key_samples),
                      "immutable_signal_unchanged": unchanged})
    result = {"schema_version": 1, "status": "accepted_bounded_universe_validation",
              "observed_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
              "current_universe": public_current_universe(args.session_date),
              "cold_receipt_validation_ms": cold_ms,
              "source_acquisition_or_projection_ms": receipt["elapsed_ms"],
              "source_clocks": {k: v.get("observed_at_taipei", v.get("upstream_observed_at_utc")) for k, v in receipt["sources"].items()},
              "legacy_requested_count": len(old), "legacy_removed_symbols": sorted(old - official),
              "legacy_added_symbols": sorted(official - old), "modes": modes,
              "boundary": "Local receipt SHA validation, full-model membership masks and request identity only; not live quote/inference/broker latency",
              "quotes_requested": False, "inference_executed": False,
              "ledger_modified": False, "orders_submitted": False}
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "listed": receipt["listed_count"],
                      "halted": receipt["halted_count"], "trading": len(official),
                      "removed": len(old - official), "added": len(official - old),
                      "cold_validation_ms": cold_ms,
                      "mode_hot_mask_p50_ms": {r["market"]: r["membership_mask"]["p50_ms"] for r in modes}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
