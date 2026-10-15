#!/usr/bin/env python3
"""Paired opening ownership/IPC benchmark, not a live quote or fill SLO.

Uses the complete recorded official symbol axis, a frozen real raw-tail source,
and an explicitly simulated provider/ledger stall. No API login, signal, model,
order or paper-ledger write. Results must not be relabelled opening acceptance.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import threading
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pyarrow as pa
from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.live_tail_source_cache import LiveTailSourceCache, _identity, _load
from downloader.ohlcv_hot_tail import logical_parts
from stockagent.live import quote_provider as quotes
from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard


def arrow_digest(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table.combine_chunks())
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def raw_trial(path, *, enabled, readers=3):
    calls, lock, barrier = [], threading.Lock(), threading.Barrier(readers)

    def load(source, rows):
        with lock:
            calls.append(1)
        return _load(source, rows)

    cache = LiveTailSourceCache(max_bytes=64 * 1024**2 if enabled else 0, loader=load)

    def read():
        barrier.wait(timeout=10)
        return cache.read(path, tail_rows=48)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=readers) as pool:
        tables = list(pool.map(lambda _: read(), range(readers)))
    elapsed = (time.perf_counter() - started) * 1000
    digests = {arrow_digest(table) for table in tables}
    if len(digests) != 1:
        raise AssertionError("raw reader output mismatch")
    return {"elapsed_ms": elapsed, "physical_decodes": len(calls),
            "output_sha256": digests.pop(), "rows": tables[0].num_rows,
            "columns": tables[0].num_columns, "stats": cache.stats()}


def ipc_trial(state_dir, symbols, *, autonomous, stall_ms):
    start = time.perf_counter()
    base = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)

    class OpeningClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (base + timedelta(seconds=time.perf_counter() - start)).astimezone(tz)

    # Values are transport fixtures, never provider observations or executions.
    prices = np.arange(len(symbols), dtype=np.float64) + 100.0
    fixture = quotes.PriceSnapshot(prices=prices, open_prices=prices.copy(),
        source="offline_transport_fixture_not_market_data", requested_count=len(symbols),
        available_count=len(symbols), available_mask=np.ones(len(symbols), dtype=bool),
        timestamps_ms=np.full(len(symbols), int(base.timestamp() * 1000), dtype=np.int64))
    published, write_json = threading.Event(), quotes._atomic_write_json

    def publish(path, payload):
        write_json(path, payload)
        if path.parent.name == "requests":
            published.set()

    def provider(requested, fallback, **kwargs):
        if requested != symbols or not np.isnan(fallback).all():
            raise AssertionError("IPC fixture changed the requested axis")
        return fixture

    with patch.object(quotes, "datetime", OpeningClock), \
            patch.object(quotes, "_atomic_write_json", publish), \
            patch.object(quotes, "fetch_shioaji_stock_snapshots", provider):
        broker = quotes.SharedDayTradeQuoteBroker(state_dir, autonomous=autonomous)
        broker.authorize(base.date().isoformat())
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                client = pool.submit(quotes.fetch_shared_day_trade_stock_snapshots,
                    symbols, np.full(len(symbols), np.nan), state_dir=state_dir,
                    purpose="opening_signal", timeout_seconds=5, on_progress=lambda *_: None)
                if not published.wait(5):
                    raise TimeoutError("fixture request publication failed")
                time.sleep(stall_ms / 1000)  # Explicit injected ledger-loop stall.
                deadline = time.monotonic() + 5
                while not client.done() and time.monotonic() < deadline:
                    broker.poll(allowed_purposes={"opening_signal"})
                    time.sleep(0.001)
                result = client.result(timeout=1)
        finally:
            broker.close()
    if not (np.array_equal(result.prices, fixture.prices)
            and np.array_equal(result.open_prices, fixture.open_prices)
            and np.array_equal(result.available_mask, fixture.available_mask)
            and np.array_equal(result.timestamps_ms, fixture.timestamps_ms)
            and result.available_count == len(symbols)):
        raise AssertionError("full IPC snapshot/clock/coverage parity failed")
    return {"client_total_ms": result.transport_timing["client_total_ms"],
            "server_request_queue_ms": result.transport_timing["server_request_queue_ms"],
            "fixture_output_sha256": hashlib.sha256(prices.tobytes()).hexdigest()}


def run(receipt_path, output, *, repeats=7, stall_ms=200):
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    symbols = receipt["requested_symbols"]
    if not symbols or len(symbols) != len(set(symbols)) or len(symbols) != receipt["requested_count"]:
        raise ValueError("complete unique official axis required")
    source = Path(receipt["parquet_root"]) / "2330_features.parquet"
    result = {"contract": "opening_ownership_paired_offline_v1",
        "scope": "local_ipc_and_single_source_decode_not_model_or_live_opening_slo",
        "observed_at": datetime.now().astimezone().isoformat(),
        "official_receipt": str(receipt_path), "official_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "symbol_count": len(symbols), "session_date": receipt["session_date"],
        "injected_ledger_stall_ms": stall_ms, "api_requests": 0, "broker_orders": 0,
        "ledger_writes": 0, "all_outputs_equal": True, "pairs": []}
    with tempfile.TemporaryDirectory(prefix="stockagent-opening-ownership-") as temp:
        frozen = Path(temp) / "source" / source.name
        with live_tw_public_source_read_guard(source.parent):
            identity = _identity(source)
            proofs = []
            for part in logical_parts(source):
                target = frozen.parent / part.relative_to(source.parent)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(part, target)
                proofs.append({"source": str(part), "sha256": sha256_file(target)})
            if _identity(source) != identity:
                raise RuntimeError("source changed during fixture freeze")
        result["frozen_raw_source"] = proofs
        for index in range(repeats):
            pair = {}
            # Alternate trial order; neither raw read promises an OS-cold page cache.
            for enabled in ([False, True] if index % 2 == 0 else [True, False]):
                label = "autonomous_shared" if enabled else "manual_unshared"
                pair[label] = {"raw": raw_trial(frozen, enabled=enabled),
                    "ipc": ipc_trial(Path(temp) / f"ipc-{index}-{label}", symbols,
                                     autonomous=enabled, stall_ms=stall_ms)}
            for boundary, field in (("raw", "output_sha256"), ("ipc", "fixture_output_sha256")):
                if pair["autonomous_shared"][boundary][field] != pair["manual_unshared"][boundary][field]:
                    raise AssertionError("paired full-output mismatch")
            result["pairs"].append(pair)
    result["median"] = {label: {
        "ipc_total_ms": statistics.median(pair[label]["ipc"]["client_total_ms"] for pair in result["pairs"]),
        "ipc_queue_ms": statistics.median(pair[label]["ipc"]["server_request_queue_ms"] for pair in result["pairs"]),
        "raw_elapsed_ms": statistics.median(pair[label]["raw"]["elapsed_ms"] for pair in result["pairs"]),
        "raw_physical_decodes": statistics.median(pair[label]["raw"]["physical_decodes"] for pair in result["pairs"]),
    } for label in ("manual_unshared", "autonomous_shared")}
    atomic_write_json(output, result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--stall-ms", type=float, default=200)
    args = parser.parse_args()
    if args.repeats < 1 or not 0 <= args.stall_ms <= 1000:
        parser.error("require repeats >= 1 and 0 <= stall-ms <= 1000")
    measured = run(args.receipt, args.output, repeats=args.repeats, stall_ms=args.stall_ms)
    print(json.dumps({"symbol_count": measured["symbol_count"], "scope": measured["scope"],
                      "median": measured["median"]}, indent=2))
