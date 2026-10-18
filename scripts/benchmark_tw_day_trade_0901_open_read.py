#!/usr/bin/env python3
"""Compare local 09:01 open extraction; not an opening/model/fill SLO test."""
from __future__ import annotations

import argparse
from datetime import date, datetime, time
import json
from pathlib import Path
import statistics
import sys
from time import perf_counter, process_time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trading-date", type=date.fromisoformat, required=True)
    parser.add_argument("--symbols-from", type=Path, required=True,
                        help="Retained Parquet whose symbol column supplies the measured scope, not official membership.")
    parser.add_argument("--minute-data-root", type=Path, action="append")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)

    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    pools = configure_cpu_source_runtime()
    import polars as pl
    import pyarrow as pa
    from downloader.artifact_io import atomic_write_json
    from scripts import rebuild_tw_day_trade_open_price_replay as replay
    from stockagent.data.factorized_panel import file_sha256

    pools.update(polars_observed_threads=pl.thread_pool_size(),
                 arrow_observed_threads=pa.cpu_count())

    roots = tuple(path.resolve() for path in (args.minute_data_root or replay.DEFAULT_MINUTE_DATA_ROOTS))
    symbols = set(pl.scan_parquet(args.symbols_from).select(pl.col("symbol").unique()).collect()["symbol"])
    files: dict[str, str] = {str(args.symbols_from.resolve()): file_sha256(args.symbols_from)}
    minute = datetime.combine(args.trading_date, time(9, 1), tzinfo=replay.TAIPEI).isoformat(timespec="minutes")
    oracle: dict[str, float] | None = None
    samples = []
    # Balanced order, unchanged page cache, no provider or model call.
    for variant in ("full_day", "first_minute", "first_minute", "full_day"):
        started, cpu = perf_counter(), process_time()
        if variant == "full_day":
            bars, receipt = replay._minute_bar_rows(roots, trading_date=args.trading_date, symbols=symbols)
            opens = {symbol: float(row["open"]) for symbol, by_minute in bars.items()
                     if (row := by_minute.get(minute)) is not None}
        else:
            opens, receipt = replay._local_first_minute_session_opens(
                roots, trading_date=args.trading_date, symbols=symbols)
        sample = {"variant": variant, "wall_seconds": perf_counter()-started,
                  "cpu_seconds": process_time()-cpu, "resolved_symbols": len(opens)}
        samples.append(sample)
        if oracle is None:
            oracle = opens
        if opens != oracle:
            raise ValueError("source/semantics differ; refusing a speed claim without exact open parity")
        for proof in receipt.get("source_files", ()):
            name, digest = proof["path"], proof["sha256"]
            if name in files and files[name] != digest:
                raise ValueError("source changed between measured reads")
            files[name] = digest
    if any(file_sha256(Path(path)) != digest for path, digest in files.items()):
        raise ValueError("source changed during measurement")
    medians = {variant: statistics.median(s["wall_seconds"] for s in samples if s["variant"] == variant)
               for variant in ("full_day", "first_minute")}
    result = {"contract": "local_0901_open_read_parity_abba_v1", "status": "verified",
              "scope": "local first-minute open extraction only; not model/ledger/publication or external opening latency",
              "trading_date": str(args.trading_date), "requested_symbols": len(symbols),
              "resolved_symbols": len(oracle or {}), "exact_price_and_symbol_parity": True,
              "additional_provider_requests": 0, "ledger_modified": False,
              "pools": pools, "samples": samples, "median_seconds": medians,
              "speedup": medians["full_day"] / medians["first_minute"],
              "source_files_sha256": files,
              "implementation_sha256": {name: file_sha256(ROOT/name) for name in (
                  "scripts/benchmark_tw_day_trade_0901_open_read.py",
                  "stockagent/data/source_processing_runtime.py",
                  "stockagent/live/quote_provider.py", "scripts/rebuild_tw_day_trade_open_price_replay.py")}}
    atomic_write_json(args.output, result)
    print(json.dumps({k: v for k, v in result.items() if k not in {"source_files_sha256", "implementation_sha256"}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
