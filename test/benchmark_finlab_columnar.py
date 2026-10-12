"""Offline, paired aggregation benchmark against a tests-only legacy oracle.

No API calls or source writes. Measures one bounded whole tick partition, not
download throughput. Both paths include semantic validation before aggregation
and before returning the final minute bars.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "test"))

import pandas as pd
import polars as pl

from scripts.download_finlab_history import _atomic_json
from scripts.download_finlab_intraday import _partition_paths, _stored_receipt
from scripts.derive_finlab_minute import ticks_to_minutes
from test_finlab_columnar import legacy_tick_workflow


def benchmark(root: Path, *, repeat: int) -> dict:
    index = root / "intraday/market_index.sqlite3"
    with sqlite3.connect(index.resolve().as_uri()+"?mode=ro", uri=True) as db:
        symbol, day_text, rows = db.execute(
            "SELECT symbol,trade_date,rows FROM partitions WHERE kind='tw_tick' "
            "AND status='downloaded_unverified_for_pit' AND rows BETWEEN 10000 AND 1000000 "
            "ORDER BY rows DESC LIMIT 1").fetchone()
    day = date.fromisoformat(day_text)
    key = f"tw_tick:{symbol}"
    receipt = _stored_receipt(_partition_paths(root, key, day)[0], root, key, day)
    if not receipt:
        raise ValueError("verified source receipt required")
    path = root / receipt["parquet_path"]
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest != receipt["sha256"]:
        raise ValueError("source SHA mismatch")
    source = pl.read_parquet(path)
    if len(source) != rows:
        raise ValueError("source row count mismatch")
    if "volume_shares" not in source.columns:
        source = source.with_columns(pl.lit(None, dtype=pl.Int64).alias("volume_shares"))
    reference_input = source.to_pandas()
    reference_input["volume_shares"] = reference_input["volume_shares"].astype("Int64")
    expected = legacy_tick_workflow(reference_input, symbol, day).rename(columns={"timestamp": "minute"})
    candidate = ticks_to_minutes(source, symbol, day)
    observed = candidate.to_pandas().rename(columns={"timestamp": "minute"})
    for field in expected.columns:
        if field != "vwap":
            pd.testing.assert_series_equal(observed[field], expected[field], check_dtype=False)
    max_vwap_error = float((observed.vwap-expected.vwap).abs().max())
    if not (observed.vwap-expected.vwap).abs().le(1e-10 + expected.vwap.abs()*1e-12).all():
        raise ValueError("VWAP reduction exceeds float64 tolerance")
    cases = []
    for _ in range(repeat):
        for mode in ("legacy_reference", "polars_arrow", "polars_arrow", "legacy_reference"):
            started = time.perf_counter()
            result = (legacy_tick_workflow(reference_input, symbol, day) if mode == "legacy_reference"
                      else ticks_to_minutes(source, symbol, day))
            cases.append({"mode": mode, "seconds": time.perf_counter()-started, "output_rows": len(result)})
    totals = {mode: sum(c["seconds"] for c in cases if c["mode"] == mode)
              for mode in ("legacy_reference", "polars_arrow")}
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ValueError("source changed during benchmark")
    return {"contract_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
        "accepted": True, "scope": "whole_verified_tick_partition_validation_and_aggregation_not_download_throughput",
        "dataset": key, "trade_date": day_text, "source_rows": rows, "source_sha256": digest,
        "source_bytes": path.stat().st_size, "source_unchanged": True, "provider_calls": 0,
        "max_vwap_absolute_error": max_vwap_error, "candidate_includes_two_semantic_validations": True,
        "reference_validation_excluded": False, "cases": cases, "total_seconds": totals,
        "speed_ratio": totals["legacy_reference"]/totals["polars_arrow"],
        "polars_threads": pl.thread_pool_size(), "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT / "data_finlab")
    parser.add_argument("--repeat", type=int, default=3, choices=range(1, 11))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = benchmark(args.root, repeat=args.repeat)
    _atomic_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k != "cases"}, ensure_ascii=False))
