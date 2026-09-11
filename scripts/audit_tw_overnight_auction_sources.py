#!/usr/bin/env python3
"""Read-only evidence audit for a 13:25 decision and two auction executions.

This does not certify KBar OPEN/CLOSE as an exchange auction fill. The output
keeps available price observations, receipt integrity, and auction proof separate.
Run through scripts/runtime_env.sh; no broker client is imported.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def audit_partition(path: Path, day: str, expected_sha256: str) -> dict:
    """Verify one immutable source and count independently observable facts."""
    if not path.is_file():
        return {"date": day, "receipt_valid": False, "error": "missing_partition"}
    actual_sha = file_sha256(path)
    if len(expected_sha256) != 64 or actual_sha != expected_sha256:
        return {
            "date": day, "receipt_valid": False, "error": "sha256_mismatch",
            "expected_sha256": expected_sha256, "actual_sha256": actual_sha,
        }
    names = set(pq.ParquetFile(path).schema_arrow.names)
    required = {"symbol", "ts", "minutes_from_open", "Open", "Close", "volume_shares"}
    if not required <= names:
        return {"date": day, "receipt_valid": True, "error": "missing_columns",
                "missing_columns": sorted(required - names)}
    table = pq.ParquetFile(path).read(columns=sorted(required))
    table = table.filter(pc.is_in(table["minutes_from_open"], value_set=pc.cast(
        pa.array([0, 1, 265, 270, 273]), table["minutes_from_open"].type
    )))
    counts = Counter()
    seen = set()
    decisions, closes, opens = set(), set(), set()
    date_errors, duplicate_keys = [], []
    for row in table.to_pylist():
        symbol, minute = str(row["symbol"]), int(row["minutes_from_open"])
        key = (symbol, minute)
        if key in seen:
            duplicate_keys.append([symbol, minute])
        seen.add(key)
        ts = row["ts"]
        # Canonical minute timestamps are exchange-local and the source
        # manifest must explicitly bind Asia/Taipei before this call.
        if ts is None or ts.date().isoformat() != day or (
            ts.hour * 60 + ts.minute - 540 != minute
        ) or ts.second != 0 or ts.microsecond != 0 or (
            ts.tzinfo is not None and ts.utcoffset().total_seconds() != 8 * 3600
        ):
            date_errors.append([symbol, minute, str(ts)])
            continue
        price = row["Close"] if minute >= 265 else row["Open"]
        valid_price = price is not None and np.isfinite(price) and price > 0
        volume = row["volume_shares"]
        positive_volume = volume is not None and np.isfinite(volume) and volume > 0
        counts[f"minute_{minute}_rows"] += 1
        if valid_price:
            counts[f"minute_{minute}_positive_price"] += 1
        if valid_price and positive_volume:
            counts[f"minute_{minute}_positive_price_and_volume"] += 1
        if minute == 265 and valid_price:
            decisions.add(symbol)
        if minute in (270, 273) and valid_price and positive_volume:
            closes.add(symbol)
        if minute in (0, 1) and valid_price and positive_volume:
            opens.add(symbol)
    if file_sha256(path) != actual_sha:
        return {"date": day, "receipt_valid": False, "error": "source_changed_during_read"}
    return {
        "date": day, "receipt_valid": True, "sha256": actual_sha,
        "bytes": path.stat().st_size, "counts": dict(counts),
        "decision_symbols": sorted(decisions),
        "close_price_volume_symbols": sorted(closes),
        "opening_bar_price_volume_symbols": sorted(opens),
        "duplicate_keys": duplicate_keys, "timestamp_errors": date_errors,
        "error": "invalid_row_identity" if duplicate_keys or date_errors else None,
        "auction_identity_proven": False,
        "auction_identity_reason": (
            "KBar interval timestamps and aggregate volume do not identify an "
            "individual 09:00 or 13:30 auction trade, order acknowledgement, "
            "queue priority, or this account's filled quantity."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minute-root", type=Path, required=True)
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    args = parser.parse_args()
    start = time.monotonic()
    root = args.minute_root.resolve()
    manifest_path = root / "manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    source = json.loads(manifest_bytes)
    if (source.get("source") != "shioaji_kbars_1m"
            or source.get("timezone") != "Asia/Taipei"
            or source.get("decision_clock") != "completed_right_labelled_1m_bar"):
        raise ValueError("source identity, timezone, or right-labelled clock is invalid")
    baseline_path = args.baseline_root / "run_manifest.json"
    baseline_bytes = baseline_path.read_bytes()
    baseline = json.loads(baseline_bytes)
    config = baseline["configuration"]
    summaries = source["partitions"]
    dates = [str(s["trade_date"]) for s in summaries]
    if len(set(dates)) != len(dates) or set(dates) != set(source["dates"]):
        raise ValueError("source partition dates are duplicated or differ from the calendar")
    selected = sorted(
        (s for s in summaries if (not args.start_date or s["trade_date"] >= args.start_date)
         and (not args.end_date or s["trade_date"] <= args.end_date)),
        key=lambda s: s["trade_date"],
    )
    results = []
    for i, summary in enumerate(selected, 1):
        day = str(summary["trade_date"])
        path = root / f"trade_date={day}" / "data.parquet"
        if not path.resolve().is_relative_to(root):
            raise ValueError(f"partition escapes the declared source root: {path}")
        results.append(audit_partition(path, day, str(summary.get("output_sha256", ""))))
        if i == 1 or i % 100 == 0 or i == len(selected):
            print(f"[overnight source audit] {i}/{len(selected)} date={day} "
                  f"elapsed={time.monotonic() - start:.1f}s", flush=True)
    source_unchanged = manifest_path.read_bytes() == manifest_bytes
    counts = Counter()
    for result in results:
        counts.update(result.get("counts", {}))
    failures = [r for r in results if not r["receipt_valid"] or r.get("error")]
    financial = config["training"]["financial_transformer"]
    report = {
        "schema_version": 1,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_root": str(args.baseline_root.resolve()),
        "baseline_manifest_sha256": hashlib.sha256(baseline_bytes).hexdigest(),
        "baseline": {
            "execution_mode": config["trading"]["execution_mode"],
            "model_name": config["training"]["model_name"],
            "lookback": config["training"]["lookback"],
            "epochs": config["training"]["epochs"],
            "portfolio_output_mode": financial["portfolio_output_mode"],
            "temporal_basis_families": financial["temporal_basis_families"],
            "temporal_basis_components_by_family": financial["temporal_basis_components_by_family"],
            "capital": config["trading"]["volume_participation_equity"],
            "commission_discount": config["trading"]["tw_commission_discount"],
            "panel_start_date": config["data"]["panel_start_date"],
        },
        "source_root": str(root),
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "source_manifest_unchanged": source_unchanged,
        "source_date_start": min(dates), "source_date_end": max(dates),
        "source_partition_count": len(dates), "audited_partition_count": len(results),
        "audited_date_start": selected[0]["trade_date"] if selected else None,
        "audited_date_end": selected[-1]["trade_date"] if selected else None,
        "receipt_failure_count": len(failures),
        "source_receipts_valid": bool(results) and not failures and source_unchanged,
        "counts": dict(counts), "partitions": results,
        "dates_without_1325_prices": [r["date"] for r in results
                                     if not r.get("decision_symbols")],
        "dates_without_closing_bar_price_and_volume": [r["date"] for r in results
                                                       if not r.get("close_price_volume_symbols")],
        "strict_auction_training_ready": False,
        "limitations": [
            "Availability counts are not whole-universe completeness or point-in-time eligibility.",
            "No decision quote is interpolated or filled from the same-day final close.",
            "Minute OPEN is an interval's first trade, not a proven 09:00 auction timestamp.",
            "Positive closing-bar volume is price evidence, not guaranteed whole-order liquidity.",
            f"13:25 history before {min(dates)} is absent from this source; daily OHLC cannot reconstruct it.",
        ],
        "elapsed_seconds": time.monotonic() - start,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in (
        "audited_partition_count", "receipt_failure_count", "source_receipts_valid",
        "source_date_start", "source_date_end", "strict_auction_training_ready", "elapsed_seconds"
    )}, ensure_ascii=False))
    return 0 if report["source_receipts_valid"] else 2


if __name__ == "__main__":
    sys.exit(main())
