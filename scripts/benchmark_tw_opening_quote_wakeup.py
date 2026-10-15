"""Measure local native-quote callback to executor wakeup, never market fills.

Uses explicit synthetic book events in this standalone process. No login,
network, trading, production quote receipt or ledger mutation is performed.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import queue
import select
import statistics
import sys
import threading
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import quote_provider as quotes


def measure(repeats: int) -> dict:
    if not 1 <= repeats <= 10000:
        raise ValueError("repeats must be in [1, 10000]")
    if quotes._SHIOAJI_STOCK_API is not None:
        raise RuntimeError("benchmark must run in an isolated, logged-out process")
    wakeup = quotes.StockQuoteWakeup()
    if wakeup.fd is None:
        raise RuntimeError("nonblocking quote notifications unavailable on this host")
    ready, completions = threading.Event(), queue.Queue()
    previous = quotes._SHIOAJI_STOCK_WAKEUP
    quotes._SHIOAJI_STOCK_WAKEUP = wakeup
    wakeup.arm({"2330"})
    samples = []

    def receive():
        try:
            for _ in range(repeats):
                ready.set()
                if not select.select([wakeup.fd], [], [], 2)[0]:
                    raise TimeoutError("quote notification missing")
                observed = time.perf_counter_ns()
                if not wakeup.consume():
                    raise RuntimeError("notification lost before consumption")
                completions.put(observed)
        except Exception as exc:
            completions.put(exc)

    consumer = threading.Thread(target=receive, name="isolated-quote-wakeup")
    consumer.start()
    try:
        for _ in range(repeats):
            if not ready.wait(timeout=2):
                raise TimeoutError("quote consumer not ready")
            ready.clear()
            at = datetime.now(ZoneInfo("Asia/Taipei"))
            event = SimpleNamespace(code="2330", datetime=at, simtrade=False,
                                    bid_price=[100.0], ask_price=[101.0],
                                    bid_volume=[10], ask_volume=[10])
            started = time.perf_counter_ns()
            quotes._record_stock_stream_event("book", event, received=at)
            observed = completions.get(timeout=2)
            if isinstance(observed, Exception):
                raise observed
            samples.append((observed - started) / 1_000_000)
        consumer.join(timeout=2)
        if consumer.is_alive():
            raise RuntimeError("quote consumer failed to terminate")
    finally:
        quotes._SHIOAJI_STOCK_WAKEUP = previous
        wakeup.close()
    ordered = sorted(samples)
    return {
        "schema_version": 1,
        "scope": "isolated_synthetic_native_callback_to_executor_wakeup_not_signal_or_fill",
        "network_requests": 0, "production_writes": False,
        "broker_fill_claim": False, "sample_count": len(samples),
        "measurement_clock": "perf_counter_ns_monotonic",
        "median_ms": statistics.median(samples),
        "p95_ms": ordered[max(0, (95 * len(ordered) + 99) // 100 - 1)],
        "p99_ms": ordered[max(0, (99 * len(ordered) + 99) // 100 - 1)],
        "max_ms": max(samples), "samples_ms": samples,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=500)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = measure(args.repeats)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "samples_ms"}))


if __name__ == "__main__":
    main()
