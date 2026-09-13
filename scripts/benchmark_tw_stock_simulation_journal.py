#!/usr/bin/env python3
"""Measure local durable intent/fill cost; NEVER login or call a broker API."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import json
from pathlib import Path
from statistics import median
import sys
from tempfile import TemporaryDirectory
from time import perf_counter_ns

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.tw_stock_simulation_execution import (  # noqa: E402
    TAIPEI, StockSimulationIntent, StockSimulationJournal, account_fingerprint,
)


def benchmark(*, samples: int, directory: Path) -> dict:
    if not 1 <= samples <= 10000:
        raise ValueError("samples must be in [1, 10000]")
    if not directory.is_dir():
        raise ValueError("benchmark parent must already exist on the measured filesystem")
    at = datetime(2026, 9, 10, 9, 1, tzinfo=TAIPEI)
    names = ("separate_prepare_claim", "atomic_prepare_claim")
    timings: dict[str, list[float]] = {key: [] for key in (*names, "deal_commit", "duplicate")}
    with TemporaryDirectory(prefix="simulation-journal-benchmark-", dir=directory) as temporary:
        journals = {name: StockSimulationJournal(Path(temporary) / f"{name}.sqlite",
                     account_key=account_fingerprint("fixture", "fixture")) for name in names}
        try:
            for i in range(samples):
                # Interleave variants to reduce cache/load-order bias.
                for name in names[::1 if i % 2 == 0 else -1]:
                    journal = journals[name]
                    order = StockSimulationIntent(f"test-{i}", "bench", "2330",
                              at - timedelta(seconds=1), 1000, "Buy")
                    start = perf_counter_ns()
                    if name == "atomic_prepare_claim":
                        row = journal.prepare_and_claim(order)
                    else:
                        row = journal.prepare(order)
                        journal.claim(order.key)
                    timings[name].append((perf_counter_ns() - start) / 1e6)
                    deal = dict(custom_field=row["tag"], trade_id=f"fixture-{i}", exchange_seq="1",
                        broker_id="fixture", account_id="fixture", code="2330", action="Buy",
                        order_cond="Cash", order_lot="Common", price=1000, quantity=1, ts=at.timestamp())
                    start = perf_counter_ns()
                    assert journal.stock_deal(deal, received_at=at)
                    timings["deal_commit"].append((perf_counter_ns() - start) / 1e6)
                    start = perf_counter_ns()
                    assert not journal.stock_deal(deal, received_at=at)
                    timings["duplicate"].append((perf_counter_ns() - start) / 1e6)
            for journal in journals.values():
                receipts = journal.receipts()
                assert len(receipts) == samples and sum(r["shares"] for r in receipts) == samples * 1000
        finally:
            for journal in journals.values():
                journal.close()
    return {"simulation_only": True, "network_calls": 0,
            "boundary": "local_SQLite_WAL_FULL_sync_only_not_broker_or_dashboard_latency",
            "filesystem_parent": str(directory.resolve()),
            "metrics_ms": {name: {"samples": len(values), "p50": round(median(values), 4),
                "p95": round(sorted(values)[max(0, (95 * len(values) + 99) // 100 - 1)], 4),
                "max": round(max(values), 4)} for name, values in timings.items()}}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--directory", type=Path, default=REPO_ROOT / "artifacts/live")
    args = parser.parse_args()
    print(json.dumps(benchmark(samples=args.samples, directory=args.directory), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
