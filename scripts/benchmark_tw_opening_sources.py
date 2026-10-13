#!/usr/bin/env python3
"""Real current-day full-market acquisition in isolation; never publishes trades.

Each round starts with an EMPTY opening receipt, not a warmed receipt. Calls
the existing simulation engine's broker, never opens an extra Shioaji login.
Midday source timing is explicitly NOT evidence for a true 09:00 SLO.
"""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import statistics
import stat
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.live import opening_price_acquisition as acquisition, quote_provider as quotes
from stockagent.live.tw_day_trade_simulation import _atomic_json


def compare_callback_pipeline(symbols, baseline_file: Path, *, rounds: int) -> dict:
    """Lossless full-axis local pipeline comparison; no live-feed/SLO claim.

    Load the explicitly captured pre-change quote module, not a pretend slow
    implementation. Each variant receives the same ordered native batches,
    drains every callback and compares all source arrays, flags and clocks.
    Durable writes for genuinely new input remain inside the measured scope.
    """
    if quotes._SHIOAJI_STOCK_API is not None or not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("comparison requires an isolated logged-out process and unique symbols")
    spec = importlib.util.spec_from_file_location("_opening_quote_baseline", baseline_file)
    if spec is None or spec.loader is None:
        raise ValueError("invalid captured baseline module")
    before = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = before
    spec.loader.exec_module(before)
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    fixed = now.replace(hour=9, minute=0, second=0, microsecond=200000)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz or ZoneInfo("Asia/Taipei"))

    local_time = SimpleNamespace(time=lambda: fixed.timestamp(), monotonic=time.monotonic,
                                perf_counter=time.perf_counter, sleep=time.sleep)
    fields = ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
              "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms")
    fallback = np.full(len(symbols), 99.)
    market_ns = int(np.datetime64(fixed.replace(tzinfo=None), "ns").astype(np.int64))
    contracts = {s: SimpleNamespace(code=s, reference=100., limit_up=110., limit_down=90.) for s in symbols}
    samples = []

    def digest(snapshot):
        result = hashlib.sha256()
        result.update(snapshot.source.encode())
        for field in fields:
            value = getattr(snapshot, field, None)
            result.update(field.encode())
            result.update(b"none" if value is None else np.asarray(value).tobytes())
        return result.hexdigest()

    def forbid_network(*args, **kwargs):
        raise AssertionError("offline pipeline comparison forbids network requests")

    try:
        for round_id in range(rounds):
            variants = [("before", before), ("after", quotes)]
            for label, module in (variants if round_id % 2 == 0 else reversed(variants)):
                with tempfile.TemporaryDirectory(prefix="opening-pipeline-benchmark-") as temporary, patch.dict(
                    quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary,
                        "STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT": str(Path(temporary) / "traffic")}
                ), patch("requests.sessions.Session.request", forbid_network), patch.object(module, "datetime", Clock), patch.object(
                    module, "time", local_time
                ), patch.object(module, "_SHIOAJI_STOCK_CACHE", {}), patch.object(module, "_SHIOAJI_STOCK_CONTRACTS", {}), patch.object(
                    module, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque()
                ), patch.object(module, "_TW_MIS_OPENING_CACHE_KEY", None), patch.object(module, "_TW_MIS_OPENING_CACHE", {}), patch.object(
                    module, "_TW_MIS_OPENING_REQUESTED", set()
                ), patch.object(module, "_TW_MIS_OPENING_DIRTY", False), patch.object(module, "_load_prepared_tw_price_limits", lambda *_: ({}, None)):
                    callbacks, progress_digests, decoded, writes, syncs = [], [], [], [], []
                    submitted, progressed = threading.Event(), threading.Event()
                    original_decode, original_write = module._shioaji_snapshot_values, module._persist_tw_mis_opening_receipt
                    original_sync, observing_storage = os.fsync, False

                    def decode(row, contract, *, received_ms):
                        decoded.append(row.code)
                        return original_decode(row, contract, received_ms=received_ms)

                    def durable(**kwargs):
                        writes.append(1)
                        return original_write(**kwargs)

                    def synced(fd):
                        if observing_storage:
                            syncs.append("directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file")
                        original_sync(fd)

                    class API:
                        def __init__(self):
                            self.contracts = contracts

                        def snapshots(self, requested, *, timeout, cb):
                            assert timeout == 0
                            rows = [SimpleNamespace(code=c.code, ts=market_ns, open=100., close=101.,
                                high=102., low=99., total_volume=2., buy_price=100., sell_price=101.,
                                buy_volume=2., sell_volume=3., simtrade=False) for c in requested]
                            callbacks.append((cb, rows))
                            if len(callbacks) == int(np.ceil(len(symbols) / 500)):
                                submitted.set()
                            return []

                    def progress(names, snapshot):
                        assert names == symbols
                        progress_digests.append(digest(snapshot))
                        progressed.set()

                    started = time.perf_counter()
                    with patch.object(module, "_shioaji_snapshot_values", decode), patch.object(module, "_persist_tw_mis_opening_receipt", durable), patch.object(os, "fsync", synced), ThreadPoolExecutor(max_workers=1) as pool:
                        future = pool.submit(module._fetch_shioaji_stock_snapshots_once,
                            symbols, fallback, api=API(), cache_ttl_seconds=0., on_progress=progress)
                        if not submitted.wait(timeout=5):
                            raise AssertionError("nonblocking native batches were not all submitted")
                        for cb, rows in callbacks:
                            progressed.clear()
                            cb(rows)
                            if not progressed.wait(timeout=5):
                                raise AssertionError("cumulative callback progress was lost")
                        snapshot = future.result(timeout=5)
                        pipeline_ms = (time.perf_counter() - started) * 1000
                        root = Path(temporary) / "nonproduction-data"
                        observing_storage = True
                        publish_started = time.perf_counter()
                        module.seed_tw_opening_snapshot_cache(symbols, snapshot, parquet_root=root)
                        initial_publish_ms = (time.perf_counter() - publish_started) * 1000
                        pipeline_to_source_receipt_ms = (time.perf_counter() - started) * 1000
                        initial_writes = len(writes)
                        initial_barriers = {kind: syncs.count(kind) for kind in ("file", "directory")}
                        syncs.clear()
                        noop_started = time.perf_counter()
                        for _ in range(5):
                            module.seed_tw_opening_snapshot_cache(symbols, snapshot, parquet_root=root)
                            module.seed_tw_opening_snapshot_cache(symbols, module.PriceSnapshot(
                                prices=fallback, source="opening_request_scope", available_mask=np.zeros(len(symbols), dtype=bool)), parquet_root=root)
                        noop_ms = (time.perf_counter() - noop_started) * 1000
                        receipt = json.loads(module._tw_mis_opening_receipt_path(fixed.date().isoformat()).read_text())
                    samples.append({"round": round_id + 1, "variant": label,
                        "pipeline_ms": round(pipeline_ms, 3), "snapshot_decode_count": len(decoded),
                        "initial_receipt_publish_ms": round(initial_publish_ms, 3),
                        "pipeline_to_source_receipt_ms": round(pipeline_to_source_receipt_ms, 3),
                        "native_batch_count": len(callbacks), "snapshot_sha256": digest(snapshot),
                        "progress_sha256": progress_digests, "initial_receipt_writes": initial_writes,
                        "initial_storage_barriers": initial_barriers,
                        "unchanged_progress_ms": round(noop_ms, 3), "unchanged_receipt_writes": len(writes) - initial_writes,
                        "unchanged_storage_barriers": {kind: syncs.count(kind) for kind in ("file", "directory")},
                        "receipt_rows_sha256": hashlib.sha256(json.dumps(receipt["rows"], sort_keys=True, allow_nan=False).encode()).hexdigest()})
    finally:
        before._TW_MIS_HTTP_POOL.shutdown(wait=True, cancel_futures=True)
        sys.modules.pop(spec.name, None)
    for round_id in range(1, rounds + 1):
        pair = [row for row in samples if row["round"] == round_id]
        for field in ("snapshot_sha256", "progress_sha256", "receipt_rows_sha256"):
            if pair[0][field] != pair[1][field]:
                raise AssertionError(f"pipeline comparison changed {field}")
    return {"schema_version": 1, "scope": "offline_full_axis_callback_pipeline_not_live_opening_slo",
        "observed_at_taipei": now.isoformat(), "requested_count": len(symbols), "rounds": rounds,
        "baseline_code_sha256": hashlib.sha256(baseline_file.read_bytes()).hexdigest(),
        "candidate_code_sha256": hashlib.sha256(Path(quotes.__file__).read_bytes()).hexdigest(),
        "network_requests": 0, "extra_shioaji_login": False, "production_ledger_modified": False,
        "signal_published": False, "broker_order_submitted": False, "opening_slo_claim": False,
        "source_receipt_storage_barriers_measured": True,
        "snapshot_parity": True, "progress_parity": True, "durable_rows_parity": True,
        "samples": samples,
        "median_ms": {label: {metric: statistics.median(row[metric] for row in samples if row["variant"] == label)
            for metric in ("pipeline_ms", "initial_receipt_publish_ms", "pipeline_to_source_receipt_ms", "unchanged_progress_ms")} for label in ("before", "after")}}


def compare_stream_priority(symbols, root, *, rounds: int, snapshot_delay_seconds: float = 0.1,
                            evidence_dir: Path | None = None) -> dict:
    """Complete real symbol axis with synthetic sources, no login/network/order.

    Native capacity remains 200. MIS supplies a disjoint 82% of the axis while
    Snapshot callbacks are delayed; a failing third source remains isolated.
    Every variant drains all sources and validates all eventual opening prices.
    This measures engineering scheduling, never external feed delivery or fills.
    """
    if quotes._SHIOAJI_STOCK_API is not None:
        raise RuntimeError("stream benchmark requires an isolated logged-out process")
    if len(symbols) < 20 or len(set(symbols)) != len(symbols):
        raise ValueError("stream comparison requires at least 20 unique symbols")
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    fixed = now.replace(hour=9, minute=0, second=0, microsecond=200000)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz or ZoneInfo("Asia/Taipei"))

    stream_count = min(200, max(1, int(np.ceil(len(symbols) * .08))))
    mis_count = int(len(symbols) * .82)
    stream_names, mis_names = symbols[:stream_count], set(symbols[stream_count:stream_count + mis_count])
    stamp, fallback = int(fixed.timestamp() * 1000), np.full(len(symbols), 99.)
    market_ns = int(np.datetime64(fixed.replace(tzinfo=None), "ns").astype(np.int64))
    samples = []

    def forbid_network(*args, **kwargs):
        raise AssertionError("offline stream comparison forbids external requests")

    for round_id in range(rounds):
        for enabled in ([False, True] if round_id % 2 == 0 else [True, False]):
            with tempfile.TemporaryDirectory(prefix="opening-stream-benchmark-") as temporary, patch.dict(
                quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary,
                                   "STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT": str(Path(temporary) / "traffic")}
            ), patch("requests.sessions.Session.request", forbid_network), patch.object(
                acquisition, "datetime", Clock
            ), patch.object(quotes, "datetime", Clock), patch.object(
                quotes, "_SHIOAJI_STOCK_CACHE", {}
            ), patch.object(quotes, "_SHIOAJI_STOCK_CONTRACTS", {}), patch.object(
                quotes, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque()
            ), patch.object(
                quotes, "_SHIOAJI_STREAM_ROWS", {s: {"quote": {
                    "open": 100., "close": 100., "high": 100., "low": 100., "volume": 1.,
                    "total_volume": 1., "bid": 99.9, "ask": 100.1, "bid_volume": 1., "ask_volume": 1.,
                    "simtrade": False, "suspend": False,
                    "exchange_at": fixed.isoformat(), "received_at": fixed.isoformat(),
                }} for s in stream_names}
            ), patch.object(quotes, "_load_prepared_tw_price_limits", lambda *_: ({}, None)):
                quotes._TW_MIS_OPENING_CACHE_KEY = None
                quotes._TW_MIS_OPENING_CACHE.clear()
                quotes._TW_MIS_OPENING_REQUESTED.clear()
                quotes._TW_MIS_OPENING_DIRTY = False
                contracts = {s: SimpleNamespace(code=s, reference=100., limit_up=110., limit_down=90.) for s in symbols}
                query_rows, callbacks, first_shioaji_valid = [], [], []

                class API:
                    def __init__(self):
                        self.contracts = contracts

                    def snapshots(self, requested, *, timeout, cb):
                        assert timeout == 0
                        query_rows.append(len(requested))
                        rows = [SimpleNamespace(code=c.code, ts=market_ns, open=100., close=100.,
                            high=100., low=100., total_volume=1., buy_price=99.9, sell_price=100.1,
                            buy_volume=1., sell_volume=1., simtrade=False) for c in requested]
                        callback = threading.Timer(snapshot_delay_seconds, cb, args=(rows,))
                        callbacks.append(callback)
                        callback.start()
                        return []

                def broker(names, values, progress):
                    def observed(names, snapshot):
                        if not first_shioaji_valid and quotes.tw_session_open_observed_mask(snapshot).any():
                            first_shioaji_valid.append((time.perf_counter() - started) * 1000)
                        progress(names, snapshot)
                    return quotes._fetch_shioaji_stock_snapshots_once(names, values, api=API(),
                        cache_ttl_seconds=0., prefer_opening_stream=enabled, on_progress=observed)

                def mis(names, values, progress):
                    time.sleep(.005)
                    mask = np.asarray([s in mis_names for s in names], dtype=bool)
                    return quotes.PriceSnapshot(prices=np.where(mask, 100., values),
                        open_prices=np.where(mask, 100., np.nan), source="twse_tpex:mis",
                        available_mask=mask, available_count=int(mask.sum()), requested_count=len(names),
                        volumes=np.where(mask, 1., np.nan), timestamps_ms=np.where(mask, stamp, 0),
                        exchange_timestamps_ms=np.where(mask, stamp, 0), simtrade_flags=np.zeros(len(names), dtype=np.int8))

                def failed(*args):
                    raise ConnectionError("synthetic source unavailable")

                owner = acquisition.OpeningPriceFanIn()
                started = time.perf_counter()
                try:
                    result = owner.fetch(symbols, fallback, parquet_root=root,
                        providers={"shioaji_fixture": broker, "mis_fixture": mis, "failed_fixture": failed},
                        minimum_coverage=.90,
                        cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False))
                    quorum_ms = (time.perf_counter() - started) * 1000
                    drained = owner.wait_idle(timeout_seconds=5)
                finally:
                    owner.close()
                    for callback in callbacks:
                        callback.join(timeout=2)
                final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
                valid = quotes.tw_session_open_observed_mask(final)
                if not drained or not valid.all() or not np.array_equal(final.open_prices, np.full(len(symbols), 100.)):
                    raise AssertionError("stream variant lost complete opening price coverage")
                sample = {"round": round_id + 1, "native_stream_priority": enabled,
                    "initial_canonical_open_count": 0, "native_fixture_open_count": stream_count,
                    "mis_fixture_open_count": mis_count, "snapshot_requested_rows": sum(query_rows),
                    "first_shioaji_valid_ms": round(first_shioaji_valid[0], 3),
                    "quorum_ms": round(quorum_ms, 3), "full_source_drain_ms": round((time.perf_counter() - started) * 1000, 3),
                    "positive_opens_at_quorum": int(quotes.tw_session_open_observed_mask(result).sum()),
                    "final_positive_opens": int(valid.sum()),
                    "opening_price_sha256": hashlib.sha256(final.open_prices.tobytes()).hexdigest(),
                    "source_clock_parity_claim": False}
                samples.append(sample)
                evidence = Path(temporary) / (fixed.date().isoformat() + ".acquisition.jsonl")
                if evidence.exists():
                    sample["diagnostic_events"] = len(evidence.read_text().splitlines())
                    if evidence_dir is not None:
                        evidence_dir.mkdir(parents=True, exist_ok=True)
                        prefix = f"round{round_id + 1}.priority-{enabled}"
                        target = evidence_dir / (prefix + ".acquisition.jsonl")
                        shutil.copyfile(evidence, target)
                        shutil.copyfile(quotes._tw_mis_opening_receipt_path(fixed.date().isoformat()),
                                        evidence_dir / (prefix + ".receipt.json"))
                        sample["source_evidence_path"] = str(target)
    if len({s["opening_price_sha256"] for s in samples}) != 1:
        raise AssertionError("stream preference changed opening price values")
    return {"schema_version": 1, "scope": "offline_full_universe_synthetic_native_vs_snapshot_not_opening_slo",
        "requested_count": len(symbols), "source_prices": "synthetic_fixture_not_live",
        "native_subscription_capacity": 200, "snapshot_callback_delay_seconds": snapshot_delay_seconds,
        "observed_at_taipei": now.isoformat(), "network_requests": 0, "extra_shioaji_login": False,
        "production_ledger_modified": False, "signal_published": False, "broker_order_submitted": False,
        "opening_slo_claim": False, "opening_price_parity": True, "samples": samples,
        "median_quorum_ms": {str(flag): statistics.median(s["quorum_ms"] for s in samples if s["native_stream_priority"] == flag) for flag in [False, True]},
        "median_full_source_drain_ms": {str(flag): statistics.median(s["full_source_drain_ms"] for s in samples if s["native_stream_priority"] == flag) for flag in [False, True]}}


def compare_no_progress(symbols, root, *, rounds: int, attempts: int = 12) -> dict:
    """Full requested universe, stale synthetic replies, no external I/O.

    Compare the old no-backoff behavior with provider-local no-progress retry.
    Retain every observer duration and compare all financial/source arrays;
    neither variant is a real opening-price acquisition or an opening SLO.
    """
    if quotes._SHIOAJI_STOCK_API is not None:
        raise RuntimeError("retry benchmark requires an isolated logged-out process")
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    fixture_clock = now.replace(hour=9, minute=0, second=2, microsecond=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixture_clock.astimezone(tz or ZoneInfo("Asia/Taipei"))

    session_date = now.date().isoformat()
    received_ms = int(fixture_clock.timestamp() * 1000)
    stale_ms = received_ms - 86_400_000
    fallback = np.full(len(symbols), 99., dtype=np.float64)
    samples = []

    def forbid_network(*args, **kwargs):
        raise AssertionError("offline retry comparison forbids network access")

    for round_id in range(rounds):
        for enabled in ([False, True] if round_id % 2 == 0 else [True, False]):
            with tempfile.TemporaryDirectory(prefix="opening-retry-benchmark-") as temporary, patch.dict(
                quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}
            ), patch("requests.sessions.Session.request", forbid_network), patch.object(
                acquisition, "datetime", Clock
            ), patch.object(quotes, "datetime", Clock):
                quotes._TW_MIS_OPENING_CACHE_KEY = None
                quotes._TW_MIS_OPENING_CACHE.clear()
                quotes._TW_MIS_OPENING_REQUESTED.clear()
                quotes._TW_MIS_OPENING_DIRTY = False
                calls = []

                def unchanged(names, values, progress):
                    calls.append(len(names))
                    return quotes.PriceSnapshot(
                        prices=np.full(len(names), 100.), open_prices=np.full(len(names), 100.),
                        source="engineering:unchanged_stale_inputs", available_mask=np.ones(len(names), dtype=bool),
                        available_count=len(names), requested_count=len(names),
                        timestamps_ms=np.full(len(names), received_ms, dtype=np.int64),
                        exchange_timestamps_ms=np.full(len(names), stale_ms, dtype=np.int64),
                    )

                policy = acquisition._source_no_progress_retry if enabled else lambda _metric: None
                owner = acquisition.OpeningPriceFanIn()
                durations = []
                started = time.perf_counter()
                with patch.object(acquisition, "_source_no_progress_retry", policy):
                    try:
                        for _ in range(attempts):
                            attempt_started = time.perf_counter()
                            observed = owner.fetch(
                                symbols, fallback, parquet_root=root, providers={"synthetic": unchanged},
                                minimum_coverage=0.90,
                                cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False),
                            )
                            if quotes.tw_session_open_observed_mask(observed).any():
                                raise AssertionError("stale input must never reach the opening quorum")
                            durations.append(round((time.perf_counter() - attempt_started) * 1000, 3))
                    finally:
                        owner.close()
                elapsed_ms = (time.perf_counter() - started) * 1000
                final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
                digest = hashlib.sha256()
                for name in ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
                             "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms"):
                    value = getattr(final, name, None)
                    digest.update(name.encode())
                    digest.update(b"none" if value is None else np.asarray(value).tobytes())
                samples.append({
                    "round": round_id + 1, "provider_local_backoff": enabled,
                    "observer_attempts": attempts, "provider_calls": len(calls),
                    "provider_requested_rows": sum(calls), "total_ms": round(elapsed_ms, 3),
                    "observer_ms": durations, "snapshot_sha256": digest.hexdigest(),
                    "valid_open_count": int(quotes.tw_session_open_observed_mask(final).sum()),
                    "observed_no_print_count": int(quotes.tw_session_no_open_print_mask(final).sum()),
                })
    if len({row["snapshot_sha256"] for row in samples}) != 1:
        raise AssertionError("retry variants changed financial/source arrays")
    return {
        "schema_version": 1, "scope": "offline_synthetic_stale_source_retry_not_opening_slo",
        "observed_at_taipei": now.isoformat(), "requested_count": len(symbols),
        "synthetic_market_clock": fixture_clock.isoformat(), "duration_clock": "real_monotonic_perf_counter",
        "network_requests": 0, "production_ledger_modified": False,
        "signal_published": False, "broker_order_submitted": False,
        "opening_slo_claim": False, "snapshot_parity": True, "samples": samples,
        "median_total_ms": {str(enabled): statistics.median(row["total_ms"] for row in samples if row["provider_local_backoff"] == enabled) for enabled in [False, True]},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True, help="Read universe/root only; never reused as the timed opening cache")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--no-connection-prewarm", action="store_true", help="Also retain cold HTTP connection costs; a prewarm failure never cancels other sources")
    parser.add_argument("--compare-no-progress", action="store_true", help="OFFLINE: compare provider-local retry using synthetic stale replies over the receipt's complete universe")
    parser.add_argument("--compare-stream-priority", action="store_true", help="OFFLINE: full-universe native-priority scheduling and opening price parity; not a live feed SLA")
    parser.add_argument("--compare-callback-pipeline", type=Path, help="OFFLINE: compare against the explicit pre-change quote_provider.py; preserve all financial/source arrays and durable writes")
    parser.add_argument("--mis-only", action="store_true", help="Actual-network MIS transport/coverage probe through the existing source owner; no broker requests")
    parser.add_argument("--mis-chunk-size", type=int, default=80)
    parser.add_argument("--mis-request-timeout", type=float, default=4.)
    parser.add_argument("--mis-parallel-requests", type=int, default=16)
    args = parser.parse_args()
    source_bytes = args.receipt.read_bytes()
    source = json.loads(source_bytes)
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    if not 1 <= args.rounds <= 5:
        parser.error("probe requires 1..5 rounds")
    symbols = list(source.get("requested_symbols") or source["rows"])
    if not symbols or len(set(symbols)) != len(symbols):
        parser.error("universe must be nonempty and unique")
    comparisons = sum(bool(value) for value in (args.compare_no_progress, args.compare_stream_priority, args.compare_callback_pipeline))
    if comparisons > 1:
        parser.error("select one isolated comparison")
    if args.mis_only and comparisons:
        parser.error("MIS network probe cannot be combined with an offline comparison")
    if not (1 <= args.mis_chunk_size <= 80 and .25 <= args.mis_request_timeout <= 8
            and 1 <= args.mis_parallel_requests <= quotes._TW_MIS_HTTP_WORKERS):
        parser.error("MIS probe requires 1..80 channels, .25..8s timeout and 1..16 workers")
    if comparisons:
        result = (compare_callback_pipeline(symbols, args.compare_callback_pipeline, rounds=args.rounds)
                  if args.compare_callback_pipeline else compare_stream_priority(symbols, source["parquet_root"], rounds=args.rounds,
                    evidence_dir=args.output.parent / (args.output.stem + ".sources"))
                  if args.compare_stream_priority else compare_no_progress(symbols, source["parquet_root"], rounds=args.rounds))
        result["universe_receipt_sha256"] = hashlib.sha256(source_bytes).hexdigest()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        _atomic_json(args.output, result)
        print(json.dumps({key: value for key, value in result.items() if key != "samples"}, ensure_ascii=False))
        return 0
    if source["session_date"] != session_date:
        parser.error("network probe requires today's universe receipt")
    load_dotenv(ROOT / ".env", override=False)
    fallback = np.asarray([source["rows"].get(symbol, {}).get("price") or np.nan for symbol in symbols], dtype=np.float64)
    root = source["parquet_root"]
    if args.mis_only:
        def mis_probe(names, values, progress):
            return quotes.fetch_tw_mis_last_prices(names, values, parquet_root=root,
                chunk_size=args.mis_chunk_size, request_timeout_seconds=args.mis_request_timeout,
                max_parallel_requests=args.mis_parallel_requests,
                empty_chunk_retry_attempts=1, empty_chunk_retry_delay_seconds=.35,
                on_progress=progress)
        providers = {"official_mis": mis_probe}
    else:
        providers = acquisition.configured_opening_providers(root)
    report = {"schema_version": 1, "scope": "actual_network_empty_receipt_current_time_not_0900_slo", "observed_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(), "universe_receipt_sha256": hashlib.sha256(source_bytes).hexdigest(), "requested_count": len(symbols), "providers": list(providers), "production_ledger_modified": False, "signal_published": False, "broker_order_submitted": False, "extra_shioaji_login": False, "opening_slo_claim": False, "rounds": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="opening-source-probe-") as temporary, patch.dict(quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}):
        report["mis_connection_prewarm"] = {"performed": False}
        if not args.no_connection_prewarm:
            try:
                report["mis_connection_prewarm"] = quotes.warm_tw_mis_quote_client()
            except Exception as exc:
                # A failed source prewarm must not cancel the independent race.
                report["mis_connection_prewarm"] = {
                    "ready": False, "error_type": type(exc).__name__,
                    "error_chain_types": quotes._tw_mis_transport_error_types(exc),
                }
        if args.mis_only and report["mis_connection_prewarm"].get("ready") is False:
            # This is a diagnostic of a refused source, not an accepted full
            # market acquisition. Do not issue a burst after the single-symbol
            # transport probe already proved the endpoint unavailable.
            report.update(status="waiting_source", data_complete=False,
                          network_sweep_skipped="failed_single_symbol_transport_probe")
            _atomic_json(args.output, report)
            print(json.dumps(report, ensure_ascii=False), flush=True)
            return 0
        if report["mis_connection_prewarm"].get("ready") is False:
            # Bounded read-only diagnosis must not burst a refused endpoint.
            # This does not change the production 09:00 all-source policy.
            report["providers_dispatch_skipped"] = {"official_mis": "failed_single_symbol_transport_probe"}
            providers = {name: provider for name, provider in providers.items() if name != "official_mis"}
        for round_id in range(1, args.rounds + 1):
            quotes._TW_MIS_OPENING_CACHE_KEY = None
            quotes._TW_MIS_OPENING_CACHE.clear()
            quotes._TW_MIS_OPENING_REQUESTED.clear()
            quotes._TW_MIS_OPENING_DIRTY = False
            receipt = quotes._tw_mis_opening_receipt_path(session_date)
            if receipt.exists():
                receipt.unlink()  # validated, exact temporary probe file only
            owner = acquisition.OpeningPriceFanIn()
            started = time.perf_counter()
            try:
                snapshot = owner.fetch(symbols, fallback, parquet_root=root, providers=providers, minimum_coverage=0.90, cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False))
                quorum_ms = (time.perf_counter() - started) * 1000
                drained = owner.wait_idle(timeout_seconds=30 if args.mis_only else 8)
            finally:
                owner.close()
            final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
            provider_states = {name: dict(metric) for state in owner._sessions.values() for name, metric in state.metrics.items()}
            record = {"round": round_id, "initial_cached_open_count": 0, "quorum_return_ms": round(quorum_ms, 3), "all_sources_complete_ms": round((time.perf_counter() - started) * 1000, 3), "background_drained": drained, "positive_opens_at_quorum": int(quotes.tw_session_open_observed_mask(snapshot).sum()), "resolved_at_quorum": int((quotes.tw_session_open_observed_mask(snapshot) | quotes.tw_session_no_open_print_mask(snapshot)).sum()), "final_positive_opens": int(quotes.tw_session_open_observed_mask(final).sum()), "final_observed_no_prints": int(quotes.tw_session_no_open_print_mask(final).sum()), "quorum_timing": snapshot.transport_timing, "providers": provider_states, "final_conflicts": final.transport_timing.get("opening_conflict_count", 0)}
            report["rounds"].append(record)
            shutil.copyfile(receipt, args.output.with_name(f"{args.output.stem}.round{round_id}.receipt.json"))
            _atomic_json(args.output, report)
            print(json.dumps({k: v for k, v in record.items() if k not in {"quorum_timing", "providers"}}, ensure_ascii=False), flush=True)
        telemetry = quotes._tw_mis_opening_receipt_path(session_date).with_suffix(".acquisition.jsonl")
        if telemetry.exists():
            shutil.copyfile(telemetry, args.output.with_suffix(".acquisition.jsonl"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
