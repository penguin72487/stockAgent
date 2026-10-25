#!/usr/bin/env python3
"""Replay real preopen previews through the canonical quote-ordering owner.

Pairs original/ranked request prefixes and measures local ranking overhead.
No HTTP, model inference, paper orders or native opening/fill SLO is claimed.
Generate --pipeline with benchmark_opening_signal_pipeline.py's
--preopen-panel-warmup --preopen-quote-priority first.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.live import opening_price_priority as priority


def coverage_report(original, ordered, previews):
    """Counts are request positions, not invented network elapsed times."""
    if len(set(original)) != len(original) or sorted(original) != sorted(ordered):
        raise ValueError("priority must retain exactly the full unique requested universe")
    scope = set(original)
    groups = {item["market"]: {row["symbol"] for row in item["rows"]
              if row["abs_model_weight"] > 0 and row["symbol"] in scope} for item in previews}
    groups["all_modes_union"] = set().union(*groups.values())
    result = {}
    for market, candidates in groups.items():
        def arm(symbols):
            positions = [i + 1 for i, symbol in enumerate(symbols) if symbol in candidates]
            return {"first_80": len(candidates.intersection(symbols[:80])),
                    "first_320": len(candidates.intersection(symbols[:320])),
                    "first_500": len(candidates.intersection(symbols[:500])),
                    "median_request_position": statistics.median(positions) if positions else None,
                    "last_request_position": max(positions) if positions else None}
        result[market] = {"candidate_count": len(candidates), "original": arm(original), "priority": arm(ordered)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True, help="Official full requested universe, not a model subset")
    parser.add_argument("--repeats", type=int, default=50)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error("at least two ABBA pairs required")
    pipeline_bytes, receipt_bytes = args.pipeline.read_bytes(), args.receipt.read_bytes()
    pipeline, receipt = json.loads(pipeline_bytes), json.loads(receipt_bytes)
    previews = pipeline.get("preopen_priority_previews")
    session = pipeline["session"]
    if not previews or session != receipt["session_date"]:
        raise ValueError("same-session, genuine preopen priority previews required")
    original = list(receipt.get("requested_symbols") or receipt["rows"])
    records, publications = [], []
    with tempfile.TemporaryDirectory(prefix="opening-priority-benchmark-") as directory:
        root = Path(directory)
        with patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": str(root / "receipts")}):
            for item in previews:
                publications.append(priority.publish_preopen_priority(parquet_root=root, session_date=session,
                    market=item["market"], feature_date=item["feature_date"], rows=item["rows"],
                    model_identity=item["model_identity"],
                    prepared_at=datetime.fromisoformat(session + "T08:50:00+08:00")))
            priority._ranked_hint.cache_clear()
            priority._loaded_hint.cache_clear()
            priority._permutation.cache_clear()
            started = time.perf_counter()
            indices, evidence = priority.priority_indices(original, parquet_root=root, session_date=session)
            cold_ms = (time.perf_counter() - started) * 1000
            if evidence["status"] != "prepared" or evidence["prepared_market_count"] != len(previews):
                raise ValueError("prepared model dependencies changed; benchmark rejected")
            ordered = [original[i] for i in indices]
            coverage = coverage_report(original, ordered, previews)
            for pair in range(args.repeats):
                for candidate in ((False, True) if pair % 2 == 0 else (True, False)):
                    started = time.perf_counter()
                    if candidate:
                        current, _ = priority.priority_indices(original, parquet_root=root, session_date=session)
                        if current != indices:
                            raise ValueError("priority changed within fixed-input measurement")
                    else:
                        list(range(len(original)))
                    records.append({"pair": pair, "priority": candidate,
                                    "elapsed_ms": (time.perf_counter() - started) * 1000})
    output = {"scope": "offline_full_scope_priority_not_network_or_opening_fill_slo",
        "contract": priority.CONTRACT, "session_date": session,
        "pipeline_sha256": hashlib.sha256(pipeline_bytes).hexdigest(),
        "universe_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "requested_count": len(original), "scope_unchanged": True,
        "network_requests": 0, "production_writes": False, "ledger_changed": False,
        "preopen_publication_ms": [row["prepare_publish_ms"] for row in publications],
        "first_read_rank_ms": cold_ms, "records": records,
        "median_original_order_ms": statistics.median(r["elapsed_ms"] for r in records if not r["priority"]),
        "median_priority_order_ms": statistics.median(r["elapsed_ms"] for r in records if r["priority"]),
        "priority_evidence": evidence, "candidate_coverage_by_request_position": coverage,
        "original_order": original, "priority_order": ordered,
        "remaining_boundary": "Provider response/completion times and native 09:00 require separate real-session evidence"}
    atomic_write_json(args.output, output)
    print(json.dumps({key: output[key] for key in ("scope", "requested_count", "scope_unchanged",
        "first_read_rank_ms", "median_priority_order_ms", "candidate_coverage_by_request_position")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
