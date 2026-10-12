#!/usr/bin/env python3
"""Real current-day full-market acquisition in isolation; never publishes trades.

Each round starts with an EMPTY opening receipt, not a warmed receipt. Calls
the existing simulation engine's broker, never opens an extra Shioaji login.
Midday source timing is explicitly NOT evidence for a true 09:00 SLO.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import opening_price_acquisition as acquisition, quote_provider as quotes
from stockagent.live.tw_day_trade_simulation import _atomic_json


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True, help="Read universe/root only; never reused as the timed opening cache")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--no-connection-prewarm", action="store_true", help="Also retain cold HTTP connection costs; a prewarm failure never cancels other sources")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env", override=False)
    source_bytes = args.receipt.read_bytes()
    source = json.loads(source_bytes)
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    if source["session_date"] != session_date or not 1 <= args.rounds <= 5:
        parser.error("probe requires today's universe receipt and 1..5 rounds")
    symbols = list(source.get("requested_symbols") or source["rows"])
    fallback = np.asarray([source["rows"].get(symbol, {}).get("price") or np.nan for symbol in symbols], dtype=np.float64)
    root = source["parquet_root"]
    providers = acquisition.configured_opening_providers(root)
    report = {"schema_version": 1, "scope": "actual_network_empty_receipt_current_time_not_0900_slo", "observed_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(), "universe_receipt_sha256": hashlib.sha256(source_bytes).hexdigest(), "requested_count": len(symbols), "providers": list(providers), "production_ledger_modified": False, "signal_published": False, "broker_order_submitted": False, "extra_shioaji_login": False, "opening_slo_claim": False, "rounds": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="opening-source-probe-") as temporary, patch.dict(quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}):
        report["mis_connection_prewarm"] = {"performed": False}
        if not args.no_connection_prewarm:
            try:
                report["mis_connection_prewarm"] = quotes.warm_tw_mis_quote_client()
            except Exception as exc:
                # A failed source prewarm must not cancel the independent race.
                report["mis_connection_prewarm"] = {"ready": False, "error_type": type(exc).__name__}
        for round_id in range(1, args.rounds + 1):
            quotes._TW_MIS_OPENING_CACHE_KEY = None
            quotes._TW_MIS_OPENING_CACHE.clear()
            quotes._TW_MIS_OPENING_REQUESTED.clear()
            quotes._TW_MIS_OPENING_DIRTY = False
            receipt = quotes._tw_mis_opening_receipt_path(session_date)
            if receipt.exists():
                receipt.unlink()  # validated, exact temporary probe file only
            owner = acquisition.OpeningPriceFanIn()
            started = time.perf_counter()
            try:
                snapshot = owner.fetch(symbols, fallback, parquet_root=root, providers=providers, minimum_coverage=0.90, cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False))
                quorum_ms = (time.perf_counter() - started) * 1000
                drained = owner.wait_idle(timeout_seconds=8)
            finally:
                owner.close()
            final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
            provider_states = {name: dict(metric) for state in owner._sessions.values() for name, metric in state.metrics.items()}
            record = {"round": round_id, "initial_cached_open_count": 0, "quorum_return_ms": round(quorum_ms, 3), "all_sources_complete_ms": round((time.perf_counter() - started) * 1000, 3), "background_drained": drained, "positive_opens_at_quorum": int(quotes.tw_session_open_observed_mask(snapshot).sum()), "resolved_at_quorum": int((quotes.tw_session_open_observed_mask(snapshot) | quotes.tw_session_no_open_print_mask(snapshot)).sum()), "final_positive_opens": int(quotes.tw_session_open_observed_mask(final).sum()), "final_observed_no_prints": int(quotes.tw_session_no_open_print_mask(final).sum()), "quorum_timing": snapshot.transport_timing, "providers": provider_states, "final_conflicts": final.transport_timing.get("opening_conflict_count", 0)}
            report["rounds"].append(record)
            shutil.copyfile(receipt, args.output.with_name(f"{args.output.stem}.round{round_id}.receipt.json"))
            _atomic_json(args.output, report)
            print(json.dumps({k: v for k, v in record.items() if k not in {"quorum_timing", "providers"}}, ensure_ascii=False), flush=True)
        telemetry = quotes._tw_mis_opening_receipt_path(session_date).with_suffix(".acquisition.jsonl")
        if telemetry.exists():
            shutil.copyfile(telemetry, args.output.with_suffix(".acquisition.jsonl"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
