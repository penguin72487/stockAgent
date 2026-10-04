#!/usr/bin/env python3
"""Read-only A/B timing of quote-grid security classification on real rows."""

from __future__ import annotations

import argparse
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sys
import time

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_exchange_price_classification import (  # noqa: E402
    classify_tw_exchange_security,
)


def _rows(path: Path, *, symbol_column: str, name_column: str, limit: int):
    parquet = pq.ParquetFile(path)
    rows: list[tuple[object, object]] = []
    for batch in parquet.iter_batches(
        columns=[symbol_column, name_column], batch_size=100_000
    ):
        symbols, names = batch.column(0).to_pylist(), batch.column(1).to_pylist()
        rows.extend(zip(symbols, names, strict=True))
        if len(rows) >= limit:
            return rows[:limit]
    return rows


def _measure(
    rows: list[tuple[object, object]], *, venue: str, cached: bool
) -> dict[str, object]:
    @lru_cache(maxsize=16_384)
    def cached_classify(symbol: object, name: object) -> str | None:
        return classify_tw_exchange_security(venue, symbol, name)

    digest = hashlib.sha256()
    started = time.perf_counter()
    for symbol, name in rows:
        kind = (
            cached_classify(symbol, name)
            if cached
            else classify_tw_exchange_security(venue, symbol, name)
        )
        digest.update((kind or "unknown").encode("ascii"))
        digest.update(b"\n")
    return {
        "variant": "cached" if cached else "direct",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "rows": len(rows),
        "result_sha256": digest.hexdigest(),
        "cache_misses": cached_classify.cache_info().misses if cached else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--venue", choices=("twse", "tpex"), required=True)
    parser.add_argument("--symbol-column", required=True)
    parser.add_argument("--name-column", required=True)
    parser.add_argument("--limit", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit must be positive")
    rows = _rows(
        args.path,
        symbol_column=args.symbol_column,
        name_column=args.name_column,
        limit=args.limit,
    )
    if not rows:
        raise RuntimeError("no source rows for quote-grid classification benchmark")
    results = [
        _measure(rows, venue=args.venue, cached=cached)
        for cached in (False, True, True, False)
    ]
    if len({row["result_sha256"] for row in results}) != 1:
        raise RuntimeError("cached and direct classifications disagree")
    print(
        json.dumps(
            {
                "source": str(args.path),
                "venue": args.venue,
                "claim_boundary": (
                    "Classification CPU on an in-memory prefix of real source rows; "
                    "not full quote-grid audit wall time, I/O, or p95."
                ),
                "results": results,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
