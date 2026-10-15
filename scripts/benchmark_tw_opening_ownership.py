#!/usr/bin/env python3
"""Paired opening ownership/IPC benchmark, not a live quote or fill SLO.

Uses the complete recorded official symbol axis, a frozen real raw-tail source,
and an explicitly simulated provider/ledger stall. No API login, signal, model,
order or paper-ledger write. Results must not be relabelled opening acceptance.
"""
from __future__ import annotations

import argparse
import ast
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import subprocess
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

# History-cleaned identities; the pinned baseline source blobs are unchanged.
BASELINE_COMMIT = "8b094a64b720eeac7cd84b6515bb37566bc129d7"
STAGE_BASELINE_COMMIT = "c6f4c70465c43efd4e9ed8d6314c92a4dfa977a9"


def factorized_stage_trial(panel, config, *, checkpoint_payload, checkpoint_key,
                           fold_dir, enabled, readers=2):
    """Real verified attachment + alignment; exclude the already-built base.

    The baseline executes the pinned original functions, not a cache-disabled
    surrogate. Both variants retain their original capacities and validation.
    Every request uses the complete feature/symbol/date axis. No model or CUDA.
    """
    from stockagent.live import signal_engine as engine
    from stockagent.data import factorized_panel as source
    engine.clear_live_panel_memory_cache()
    build = engine._build_panel
    align = engine._cached_aligned_panel
    baseline_sha = None
    if not enabled:
        code = subprocess.run(
            ["git", "show", f"{STAGE_BASELINE_COMMIT}:stockagent/live/signal_engine.py"],
            cwd=ROOT, check=True, capture_output=True,
        ).stdout
        baseline_sha = hashlib.sha256(code).hexdigest()
        names = {"_build_panel", "_cached_aligned_panel"}
        nodes = [node for node in ast.parse(code).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
        if {node.name for node in nodes} != names:
            raise RuntimeError("pinned stage baseline functions not found")
        namespace = dict(vars(engine))
        namespace.update(_LIVE_FACTORIZED_PANEL_CACHE=OrderedDict(),
                         _LIVE_ALIGNED_PANEL_CACHE=OrderedDict(),
                         _LIVE_ALIGNED_PANEL_CACHE_LOCK=threading.Lock())
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "<pinned-panel-stages>", "exec"), namespace)
        build, align = namespace["_build_panel"], namespace["_cached_aligned_panel"]
    attachments, alignments, lock = [], [], threading.Lock()
    original_attach = source.attach_factorized_live_features
    original_align = engine.align_panel_to_checkpoint_universe

    def attach(*a, **kw):
        with lock:
            attachments.append(1)
        return original_attach(*a, **kw)

    def aligned(*a, **kw):
        with lock:
            alignments.append(1)
        return original_align(*a, **kw)

    base = lambda *a, **kw: (panel, True, "frozen-base")
    if not enabled:
        namespace.update(_build_panel_from_stable_source=base,
                         align_panel_to_checkpoint_universe=aligned)
    barrier = threading.Barrier(readers)

    def request(_):
        barrier.wait(10)
        value = build(config, live_tail=True)[0]
        return align(value, checkpoint_key=checkpoint_key,
                     checkpoint_payload=checkpoint_payload,
                     state_dict=checkpoint_payload["model_state_dict"],
                     fold_dir=fold_dir, context="offline opening ownership")[0]

    with patch.object(engine, "_build_panel_from_stable_source", base), \
            patch.object(source, "attach_factorized_live_features", attach), \
            patch.object(engine, "align_panel_to_checkpoint_universe", aligned):
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=readers) as pool:
            results = list(pool.map(request, range(readers)))
        elapsed = (time.perf_counter() - started) * 1000
    identities = {hashlib.sha256(json.dumps({
        "symbols": value.symbols, "features": value.feature_names,
        "dates": [str(day) for day in value.dates],
        "full_feature_source_identity": value.features.content_fingerprint,
    }, sort_keys=True).encode()).hexdigest() for value in results}
    if len(identities) != 1:
        raise AssertionError("concurrent full stage identities differ")
    result = {"elapsed_ms": elapsed, "attachment_calls": len(attachments),
              "alignment_calls": len(alignments), "readers": readers,
              "unique_decoded_source_caches": len({id(value.features.source._cache) for value in results}),
              "shape": list(results[0].features.shape), "output_identity_sha256": identities.pop(),
              "oracle": "all_source_member_sha_and_complete_base_calendar_schema_axis_not_dense_output_bytes",
              "baseline_code_sha256": baseline_sha}
    engine.clear_live_panel_memory_cache()
    return result


def run_factorized_stages(market_config_path, output, *, repeats=3, readers=2):
    import torch
    import requests
    from stockagent.live import signal_engine as engine
    from stockagent.live.market_config import load_market_config
    from stockagent.config import load_config
    if torch.cuda.is_initialized():
        raise RuntimeError("stage benchmark requires a CUDA-cold process")
    market = load_market_config(market_config_path)
    config = load_config(market.config_path)
    if not config.data.factorized_feature_manifest:
        raise ValueError("factorized market required")
    _, checkpoint = engine._resolve_checkpoint(market.output_dir, market.fold_id, market.checkpoint_path)
    payload, key, _ = engine._cached_checkpoint(checkpoint)
    def no_network(*_a, **_kw):
        raise AssertionError("offline stage benchmark forbids network")
    with patch.object(requests.sessions.Session, "request", no_network), \
            live_tw_public_source_read_guard(config.data.parquet_root):
        manifest = Path(config.data.factorized_feature_manifest).resolve(strict=True)
        signatures = engine._factorized_member_signatures(manifest)
        began = time.perf_counter()
        base, _hit, tier = engine._build_panel_from_stable_source(config, live_tail=True)
        base_ms = (time.perf_counter() - began) * 1000
        pairs = []
        for index in range(repeats):
            pair = {}
            for enabled in ([False, True] if index % 2 == 0 else [True, False]):
                pair["shared" if enabled else "original"] = factorized_stage_trial(
                    base, config, checkpoint_payload=payload, checkpoint_key=key,
                    fold_dir=Path(market.output_dir) / f"fold_{market.fold_id:02d}",
                    enabled=enabled, readers=readers)
            if pair["shared"]["output_identity_sha256"] != pair["original"]["output_identity_sha256"]:
                raise AssertionError("paired full stage source identities differ")
            pairs.append(pair)
        if signatures != engine._factorized_member_signatures(manifest):
            raise RuntimeError("factorized source generation changed during stage benchmark")
    if torch.cuda.is_initialized():
        raise AssertionError("stage benchmark initialized CUDA")
    result = {"contract": "opening_factorized_stage_ownership_paired_offline_v1",
              "scope": "complete_attachment_and_alignment_excludes_base_build_model_and_live_quotes",
              "observed_at": datetime.now().astimezone().isoformat(),
              "market_config": str(market_config_path), "config_path": str(market.config_path),
              "checkpoint_sha256": sha256_file(checkpoint), "manifest": str(manifest),
              "manifest_sha256": sha256_file(manifest), "baseline_commit": STAGE_BASELINE_COMMIT,
              "base_preparation_ms_excluded": base_ms, "base_preparation_tier": tier,
              "api_requests": 0, "broker_orders": 0, "ledger_writes": 0,
              "cuda_initialized": False, "pairs": pairs,
              "median": {name: {field: statistics.median(pair[name][field] for pair in pairs)
                         for field in ("elapsed_ms", "attachment_calls", "alignment_calls", "unique_decoded_source_caches")}
                         for name in ("original", "shared")}}
    atomic_write_json(output, result)
    return result


@lru_cache(maxsize=1)
def baseline_cache():
    """Execute the pinned pre-change reader, with its original byte-bound LRU.

    A disabled cache is not a fair substitute for the real concurrent-miss
    baseline: it would also remove the original Arrow buffer compaction cost.
    """
    code = subprocess.run(["git", "show", f"{BASELINE_COMMIT}:downloader/live_tail_source_cache.py"],
        cwd=ROOT, check=True, capture_output=True).stdout
    namespace = {"__name__": "_opening_raw_baseline", "__file__": "<pinned-opening-baseline>"}
    exec(compile(code, namespace["__file__"], "exec"), namespace)
    return namespace["LiveTailSourceCache"], hashlib.sha256(code).hexdigest()


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

    cache_class = LiveTailSourceCache if enabled else baseline_cache()[0]
    cache = cache_class(max_bytes=64 * 1024**2, loader=load)

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
    result["raw_baseline"] = {"commit": BASELINE_COMMIT, "code_sha256": baseline_cache()[1],
                              "same_cache_budget_bytes": 64 * 1024**2}
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
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--factorized-market-config", type=Path,
                        help="Instead measure complete verified attachment/alignment, no model or quotes")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--stall-ms", type=float, default=200)
    args = parser.parse_args()
    if args.repeats < 1 or not 0 <= args.stall_ms <= 1000:
        parser.error("require repeats >= 1 and 0 <= stall-ms <= 1000")
    if bool(args.receipt) == bool(args.factorized_market_config):
        parser.error("select exactly one of --receipt or --factorized-market-config")
    measured = (run_factorized_stages(args.factorized_market_config, args.output, repeats=args.repeats)
                if args.factorized_market_config else
                run(args.receipt, args.output, repeats=args.repeats, stall_ms=args.stall_ms))
    print(json.dumps({"symbol_count": measured.get("symbol_count"), "scope": measured["scope"],
                      "median": measured["median"]}, indent=2))
