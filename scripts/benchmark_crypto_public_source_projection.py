#!/usr/bin/env python3
"""Read-only A/B parity and timing for crypto public daily source projection."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.ohlcv_hot_tail import logical_parts
from scripts import build_bybit_crypto_public_daily_features as builder


def benchmark(source: str, path: Path, symbol: str) -> dict[str, object]:
    functions = {"binance": builder._binance_daily, "okx": builder._okx_daily}
    projections = {
        "binance": builder.BINANCE_SOURCE_COLUMNS,
        "okx": builder.OKX_SOURCE_COLUMNS,
    }
    parts = logical_parts(path)
    if not parts:
        raise FileNotFoundError(path)
    before = [(str(part), part.stat().st_size, part.stat().st_mtime_ns) for part in parts]
    physical_columns = {
        name for part in parts for name in pq.read_schema(part).names
    }
    original_reader = builder.read_logical_parquet
    baseline = None
    measurements: list[dict[str, object]] = []
    try:
        for mode in ("full", "projected", "projected", "full"):
            if mode == "full":
                builder.read_logical_parquet = (
                    lambda source_path, **_: original_reader(source_path)
                )
            else:
                builder.read_logical_parquet = original_reader
            started = time.perf_counter()
            output = functions[source](path, symbol)
            elapsed = time.perf_counter() - started
            if baseline is None:
                baseline = output
            elif not output.equals(baseline, null_equal=True):
                raise RuntimeError(f"{source} {mode} output differs from full read")
            measurements.append({
                "mode": mode,
                "elapsed_seconds": round(elapsed, 3),
                "output_rows": output.height,
            })
    finally:
        builder.read_logical_parquet = original_reader
    after = [
        (str(part), part.stat().st_size, part.stat().st_mtime_ns)
        for part in logical_parts(path)
    ]
    if before != after:
        raise RuntimeError("source file identity changed during A/B benchmark")
    assert baseline is not None
    return {
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "path": str(path.resolve()),
        "symbol": symbol,
        "source_parts": before,
        "physical_column_count": len(physical_columns),
        "projected_column_count": len(physical_columns.intersection(projections[source])),
        "output_sha256": hashlib.sha256(baseline.write_json().encode()).hexdigest(),
        "parity": "equal",
        "measurements": measurements,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("binance", "okx"), required=True)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--symbol", default="BENCHMARK")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = benchmark(args.source, args.path, args.symbol)
    payload = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_name(f".{args.output.name}.tmp")
        try:
            temporary.write_text(payload, encoding="utf-8")
            temporary.replace(args.output)
        finally:
            temporary.unlink(missing_ok=True)
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
