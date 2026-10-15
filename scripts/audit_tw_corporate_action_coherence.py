#!/usr/bin/env python3
"""Read-only acceptance of the exact TW reference/entitlement generation.

Use the same fail-closed panel loader as live inference. This proves source
coherence, not complete issuer terms, broker fills or opening latency.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import statistics
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl

from downloader.artifact_io import atomic_write_json
from downloader.download_tw_corporate_action_entitlements import _entitlement_input_change_errors
from stockagent.data.panel import (
    _load_corporate_action_reference,
    _resolve_corporate_action_reference_paths,
)
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard


def audit(root: Path, expected_date: str, *, guard_samples: int = 1000) -> dict:
    root = root.resolve()
    started = time.perf_counter()
    with live_tw_public_source_read_guard(root / "stocks"):
        paths = _resolve_corporate_action_reference_paths(
            root / "features" / "tw_public_features.parquet", include_rules=True,
        )
        if paths is None or paths.entitlements_parquet is None or paths.entitlements_summary is None:
            raise ValueError("reference and exact entitlement receipts must both exist")
        reference_summary = json.loads(paths.summary.read_text())
        entitlement_summary = json.loads(paths.entitlements_summary.read_text())
        if str(reference_summary.get("end_date") or "")[:10] < expected_date:
            raise ValueError("reference coverage is behind the requested date")
        if str(entitlement_summary.get("coverage_end") or "")[:10] < expected_date:
            raise ValueError("entitlement coverage is behind the requested date")
        input_errors = _entitlement_input_change_errors(
            entitlement_summary["reference_receipt"],
            entitlement_summary["universe_receipt"],
        )
        if input_errors:
            raise ValueError(f"entitlement dependency receipt differs: {input_errors}")
        loaded = _load_corporate_action_reference(paths)
        if loaded is None:
            raise ValueError("canonical panel loader did not accept the reference")
        ledger = pl.read_parquet(paths.entitlements_parquet)
        reference_keys = (
            pl.read_parquet(paths.parquet, columns=["date", "symbol"])
            .filter(pl.col("date").is_between(
                pl.lit(entitlement_summary["coverage_start"]).str.to_date(),
                pl.lit(entitlement_summary["coverage_end"]).str.to_date(),
            ))
        )
        if reference_keys.height != ledger.height or (
            reference_keys.join(ledger.select("date", "symbol"), on=["date", "symbol"], how="anti").height
            or ledger.select("date", "symbol").join(reference_keys, on=["date", "symbol"], how="anti").height
        ):
            raise ValueError("entitlement coverage keys differ from the reference window")
        handling_counts = dict(ledger.group_by("handling").len().iter_rows())
        source = {
            "reference_receipt": entitlement_summary["reference_receipt"],
            "entitlement_output_receipt": entitlement_summary["output_receipt"],
            "entitlement_coverage_start": entitlement_summary["coverage_start"],
            "entitlement_coverage_end": entitlement_summary["coverage_end"],
            "entitlement_rows": ledger.height,
            "raw_manifest_receipt": entitlement_summary["raw_receipt_manifest"],
            "handling_counts": handling_counts,
            "unresolved_stock_delivery_events": entitlement_summary.get("unresolved_stock_delivery_events"),
            "failure_count": entitlement_summary["failure_count"],
        }
    source_validation_ms = (time.perf_counter() - started) * 1000.0
    timings = []
    for _ in range(max(1, guard_samples)):
        begin = time.perf_counter()
        with live_tw_public_source_read_guard(root / "stocks"):
            pass
        timings.append((time.perf_counter() - begin) * 1000.0)
    timings.sort()
    return {
        "schema_version": 1, "status": "accepted_source_coherence",
        "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "source_root": str(root), "expected_date": expected_date, **source,
        "canonical_panel_loader_passed": True, "coverage_key_identity_passed": True,
        "source_validation_ms": round(source_validation_ms, 3),
        "idle_shared_guard_ms": {
            "samples": len(timings), "median": round(statistics.median(timings), 6),
            "p95": round(timings[min(len(timings) - 1, int(len(timings) * .95))], 6),
            "max": round(max(timings), 6),
            "scope": "idle canonical local source guard only, not model or opening latency",
        },
        "all_issuer_terms_resolved": source["handling_counts"].get("avoid", 0) == 0,
        "source_data_modified": False, "ledger_modified": False,
        "broker_order_sent": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT / "data_tw_public")
    parser.add_argument("--expected-date", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Validate the requested clock before comparing ISO coverage boundaries.
    datetime.strptime(args.expected_date, "%Y-%m-%d")
    try:
        receipt = audit(args.source_root, args.expected_date)
    except Exception as exc:
        receipt = {"schema_version": 1, "status": "failed", "error_type": type(exc).__name__,
                   "error": str(exc), "source_data_modified": False, "ledger_modified": False}
        atomic_write_json(args.output, receipt)
        print(json.dumps(receipt, ensure_ascii=False), flush=True)
        return 1
    atomic_write_json(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
