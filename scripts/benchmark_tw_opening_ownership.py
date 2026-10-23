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
from contextlib import ExitStack
import copy
from datetime import datetime, timedelta
from functools import lru_cache
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import statistics
import stat
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
CACHE_FLUSH_BASELINE_COMMIT = "5a9383607ddb46fa4951d8f42c0de64afba8d8d0"


def cache_flush_trial(receipt, directory, *, enabled, stall_ms):
    """Complete recorded axis, real durable writes, explicit storage delay.

    The old functions execute verbatim from the pinned source, in an isolated
    namespace. Both variants read the same retained facts; they do not acquire
    new market observations or write the original receipt. The optional stall
    models storage tail latency, not a measured provider/network delay.
    """
    # Function-only rebinding leaves column-view class methods pointing at the
    # importing process's cache. Load the complete canonical implementation in
    # isolation instead; neither its methods nor its helpers may read live/test
    # globals belonging to a different trial.
    name = f"_opening_cache_trial_{time.monotonic_ns()}"
    spec = importlib.util.spec_from_file_location(name, quotes.__file__)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return _cache_flush_trial_namespace(receipt, directory, enabled=enabled,
                                             stall_ms=stall_ms, namespace=vars(module))
    finally:
        sys.modules.pop(name, None)


def _cache_flush_trial_namespace(receipt, directory, *, enabled, stall_ms, namespace):
    directory.mkdir(parents=True, exist_ok=True)
    baseline_sha = None
    if not enabled:
        code = subprocess.run(
            ["git", "show", f"{CACHE_FLUSH_BASELINE_COMMIT}:stockagent/live/quote_provider.py"],
            cwd=ROOT, check=True, capture_output=True).stdout
        baseline_sha = hashlib.sha256(code).hexdigest()
        names = {"seed_tw_opening_snapshot_cache", "persist_tw_opening_snapshot_cache",
                 "fetch_tw_mis_opening_snapshot"}
        nodes = [node for node in ast.parse(code).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
        if {node.name for node in nodes} != names:
            raise RuntimeError("pinned opening cache functions not found")
        exec(compile(ast.Module(body=nodes, type_ignores=[]), "<pinned-opening-cache>", "exec"), namespace)
    day, symbols = receipt["session_date"], receipt["requested_symbols"]
    root = str((directory.parent / "offline-stocks").resolve())
    fixed = datetime.fromisoformat(day + "T12:00:00+08:00")

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz)

    source = copy.deepcopy(receipt["rows"])
    namespace.update(datetime=Clock, _TW_MIS_OPENING_LOCK=threading.RLock(),
                     _TW_MIS_OPENING_PERSIST_LOCK=threading.Lock(),
                     _TW_MIS_OPENING_CACHE_KEY=(root, day), _TW_MIS_OPENING_DIRTY=True,
                     _TW_MIS_OPENING_REVISION=1,
                     _TW_MIS_OPENING_CACHE={s: (time.monotonic(), row) for s, row in source.items()},
                     _TW_MIS_OPENING_REQUESTED=set(symbols))
    snapshot_read = namespace["fetch_tw_mis_opening_snapshot"]
    merge, flush = namespace["seed_tw_opening_snapshot_cache"], namespace["persist_tw_opening_snapshot_cache"]
    fallback = np.full(len(symbols), np.nan)
    entered, release, writes, syncs = threading.Event(), threading.Event(), [], []
    original = namespace["_persist_tw_mis_opening_receipt"]
    original_fsync = quotes.os.fsync

    def writer(**kwargs):
        writes.append(len(kwargs["rows"]))
        if len(writes) == 1:
            entered.set()
            if not release.wait(5):
                raise TimeoutError("offline storage delay was not released")
        return original(**kwargs)

    def synced(fd):
        syncs.append("directory" if stat.S_ISDIR(quotes.os.fstat(fd).st_mode) else "file")
        original_fsync(fd)

    def forbid_network(*_a, **_kw):
        raise AssertionError("opening cache benchmark forbids network")

    namespace["_persist_tw_mis_opening_receipt"] = writer
    with patch.dict(quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": str(directory)}), \
            patch.object(quotes, "datetime", Clock), \
            patch.object(quotes.os, "fsync", synced), \
            patch("requests.sessions.Session.request", forbid_network):
        frozen = snapshot_read(symbols, fallback, parquet_root=root, allow_network=False)

        def merge_provider():
            started = time.perf_counter()
            merge(symbols, frozen, parquet_root=root, session_date=day, persist=False)
            return (time.perf_counter() - started) * 1000

        def read_provider():
            started = time.perf_counter()
            value = snapshot_read(symbols, fallback, parquet_root=root, allow_network=False)
            return value, (time.perf_counter() - started) * 1000

        with ThreadPoolExecutor(max_workers=3) as pool:
            started = time.perf_counter()
            writing = pool.submit(flush, parquet_root=root, session_date=day)
            if not entered.wait(2):
                raise RuntimeError("offline durable writer did not start")
            merging = pool.submit(merge_provider)
            reading = pool.submit(read_provider)
            try:
                time.sleep(stall_ms / 1000)
            finally:
                release.set()
            path = writing.result(timeout=5)
            flush_ms = (time.perf_counter() - started) * 1000
            merge_ms = merging.result(timeout=5)
            _concurrent_snapshot, read_ms = reading.result(timeout=5)
        # A newer accepted generation requires its own durable barrier. Do not
        # declare a late merge durable just because the first writer returned.
        flush(parquet_root=root, session_date=day)
        committed = json.loads(path.read_bytes())
        if syncs != [kind for _ in writes for kind in ("file", "directory")]:
            raise AssertionError("source durability barriers changed")
        final = snapshot_read(symbols, fallback, parquet_root=root, allow_network=False)
        namespace.update(_TW_MIS_OPENING_CACHE_KEY=None, _TW_MIS_OPENING_CACHE={},
                         _TW_MIS_OPENING_REQUESTED=set(), _TW_MIS_OPENING_DIRTY=False)
        restored = snapshot_read(symbols, fallback, parquet_root=root, allow_network=False)
        fields = ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
                  "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms")
        result_hash = hashlib.sha256()
        array_hashes = {}
        for name in fields:
            left, right = getattr(final, name), getattr(restored, name)
            if left.dtype != right.dtype or left.shape != right.shape or left.tobytes() != right.tobytes():
                raise AssertionError(f"complete cold-restore mismatch: {name}")
            result_hash.update(name.encode())
            result_hash.update(left.tobytes())
            array_hashes[name] = hashlib.sha256(left.tobytes()).hexdigest()
        committed["parquet_root"] = "<isolated-scope>"
        if set(committed["requested_symbols"]) != set(symbols):
            raise AssertionError("source receipt lost requested universe")
        result_hash.update(json.dumps(committed, sort_keys=True, allow_nan=False).encode())
    return {"merge_during_flush_ms": merge_ms, "read_during_flush_ms": read_ms,
            "first_durable_flush_ms": flush_ms, "durable_write_count": len(writes),
            "file_and_directory_fsync_count": len(syncs), "restored_full_output_sha256": result_hash.hexdigest(),
            "restored_row_count": len(committed["rows"]), "baseline_code_sha256": baseline_sha,
            "restored_array_sha256": array_hashes,
            "receipt_field_sha256": {name: hashlib.sha256(json.dumps(value, sort_keys=True,
                allow_nan=False).encode()).hexdigest() for name, value in committed.items()}}


def run_cache_flush(receipt_path, output, *, repeats=3, stall_ms=100):
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    symbols = receipt["requested_symbols"]
    if not symbols or len(symbols) != len(set(symbols)) or len(symbols) != receipt["requested_count"]:
        raise ValueError("complete unique recorded universe required")
    output.parent.mkdir(parents=True, exist_ok=True)
    pairs = []
    with tempfile.TemporaryDirectory(prefix="opening-cache-flush-", dir=output.parent) as temp:
        for index in range(repeats):
            pair = {}
            for enabled in ([False, True] if index % 2 == 0 else [True, False]):
                name = "off_lock" if enabled else "original"
                pair[name] = cache_flush_trial(receipt, Path(temp) / f"{index}-{name}",
                                              enabled=enabled, stall_ms=stall_ms)
            if pair["original"]["restored_full_output_sha256"] != pair["off_lock"]["restored_full_output_sha256"]:
                differences = {key: [name for name in set(pair["original"][key]) | set(pair["off_lock"][key])
                    if pair["original"][key].get(name) != pair["off_lock"][key].get(name)]
                    for key in ("restored_array_sha256", "receipt_field_sha256")}
                raise AssertionError(f"full opening source/clock/coverage restore parity failed: {differences}")
            pairs.append(pair)
    result = {"contract": "opening_cache_flush_paired_offline_v1",
              "scope": "full_recorded_axis_merge_read_and_durable_restore_not_live_opening_slo",
              "observed_at": datetime.now().astimezone().isoformat(), "symbol_count": len(symbols),
              "input_receipt_sha256": hashlib.sha256(raw).hexdigest(), "injected_storage_stall_ms": stall_ms,
              "baseline_commit": CACHE_FLUSH_BASELINE_COMMIT,
              "api_requests": 0, "broker_orders": 0, "production_receipt_writes": 0, "ledger_writes": 0,
              "all_outputs_equal": True, "pairs": pairs,
              "median": {name: {field: statistics.median(pair[name][field] for pair in pairs)
                                    for field in ("merge_during_flush_ms", "read_during_flush_ms", "first_durable_flush_ms")}
                         for name in ("original", "off_lock")}}
    atomic_write_json(output, result)
    return result


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


def ipc_trial(state_dir, symbols, *, autonomous, stall_ms, response_format=None,
              mixed_missing_values=False):
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
        timestamps_ms=np.full(len(symbols), int(base.timestamp() * 1000), dtype=np.int64),
        exchange_timestamps_ms=np.full(len(symbols), int(base.timestamp() * 1000), dtype=np.int64),
        no_open_print_timestamps_ms=np.zeros(len(symbols), dtype=np.int64),
        simtrade_flags=np.zeros(len(symbols), dtype=np.int8))
    for index, field in enumerate(quotes._PRICE_SNAPSHOT_ARRAY_FIELDS):
        if getattr(fixture, field) is None:
            setattr(fixture, field, prices + index + .125)
    if mixed_missing_values:
        # Non-finite placeholders must survive transport, not become JSON null
        # or zero-volume observations. These remain synthetic transport values.
        for field in ("prices", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS):
            values = getattr(fixture, field)
            values[::17] = np.nan
            if len(values) > 3:
                values[1:4] = [-0.0, np.nextafter(100., 101.), np.inf]
        fixture.available_mask = np.isfinite(fixture.prices) & (fixture.prices > 0)
        fixture.available_count = int(fixture.available_mask.sum())
        fixture.timestamps_ms[-1] = 2 ** 53 + 1
        fixture.simtrade_flags[::17] = -1
    published, write_json = threading.Event(), quotes._atomic_write_json
    write_bytes, decode = quotes._atomic_write_quote_bytes, quotes._decode_quote_ipc
    response_sizes, response_formats, decode_spans, publish_spans = [], [], [], []
    write_response = quotes._atomic_write_quote_response

    def publish(path, payload):
        write_json(path, payload)
        if path.parent.name == "requests":
            published.set()

    def provider(requested, fallback, **kwargs):
        if requested != symbols or not np.isnan(fallback).all():
            raise AssertionError("IPC fixture changed the requested axis")
        return fixture

    def observe_write(path, body):
        if path.parent.name == "responses":
            response_sizes.append(len(body))
            response_formats.append("msgpack_v1" if body.startswith(quotes._QUOTE_IPC_MSGPACK_MAGIC) else "json_v1")
        return write_bytes(path, body)

    def observe_response(*args):
        started = time.perf_counter()
        try:
            return write_response(*args)
        finally:
            publish_spans.append((time.perf_counter() - started) * 1000)

    def observe_decode(body):
        started = time.perf_counter()
        try:
            return decode(body)
        finally:
            decode_spans.append((time.perf_counter() - started) * 1000)

    def verify(result):
        digest = hashlib.sha256()
        for field in ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
                      "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms", "simtrade_flags"):
            left, right = getattr(result, field), getattr(fixture, field)
            if left.dtype != right.dtype or left.shape != right.shape or left.tobytes() != right.tobytes():
                raise AssertionError(f"full IPC snapshot/clock/coverage parity failed: {field}")
            digest.update(field.encode())
            digest.update(right.dtype.str.encode())
            digest.update(right.tobytes())
        if result.available_count != fixture.available_count or result.requested_count != len(symbols):
            raise AssertionError("full IPC requested/available count changed")
        return digest.hexdigest()

    with ExitStack() as patches:
        for name, replacement in (("datetime", OpeningClock), ("_atomic_write_json", publish),
                ("_atomic_write_quote_bytes", observe_write), ("_decode_quote_ipc", observe_decode),
                ("_atomic_write_quote_response", observe_response), ("fetch_shioaji_stock_snapshots", provider)):
            patches.enter_context(patch.object(quotes, name, replacement))
        if response_format is not None:
            patches.enter_context(patch.object(quotes, "_quote_ipc_response_format", lambda: response_format))
        broker = quotes.SharedDayTradeQuoteBroker(state_dir, autonomous=autonomous)
        broker.authorize(base.date().isoformat())
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                workflow_started = time.perf_counter()
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
                output_sha256 = verify(result)
                verified_ms = (time.perf_counter() - workflow_started) * 1000
        finally:
            broker.close()
    return {"client_total_ms": result.transport_timing["client_total_ms"],
            "request_to_verified_readback_ms": verified_ms,
            "server_request_queue_ms": result.transport_timing["server_request_queue_ms"],
            "server_receiver_thread": result.transport_timing["server_receiver_thread"],
            "response_write_bytes": sum(response_sizes), "response_write_count": len(response_sizes),
            "response_formats": response_formats, "response_encode_publish_ms": sum(publish_spans),
            "client_response_decode_ms": sum(decode_spans), "fixture_output_sha256": output_sha256}


def run_ipc_serialization(receipt_path, output, *, repeats=7):
    """Balanced full-axis codec comparison with the identical autonomous owner."""
    if not quotes.private_json.native_available():
        raise RuntimeError("MessagePack comparison requires the optional installed msgspec decoder")
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    symbols = receipt["requested_symbols"]
    if not symbols or len(symbols) != len(set(symbols)) or len(symbols) != receipt["requested_count"]:
        raise ValueError("complete unique recorded universe required")
    samples = []
    with tempfile.TemporaryDirectory(prefix="opening-ipc-codec-") as temp:
        for index in range(repeats):
            pair = {}
            for codec in (["json_v1", "msgpack_v1"] if index % 2 == 0 else ["msgpack_v1", "json_v1"]):
                pair[codec] = ipc_trial(Path(temp) / f"{index}-{codec}", symbols,
                    autonomous=True, stall_ms=0, response_format=codec, mixed_missing_values=True)
                if pair[codec]["response_formats"] != [codec]:
                    raise AssertionError("codec comparison silently used a fallback")
                if pair[codec]["server_receiver_thread"] != "tw-shared-quote-owner":
                    raise AssertionError("codec comparison changed quote ownership")
            if len({row["fixture_output_sha256"] for row in pair.values()}) != 1:
                raise AssertionError("full IPC numeric/NaN/signed-zero/int64/flag parity failed")
            samples.append(pair)
    result = {"contract": "opening_ipc_serialization_paired_offline_v1",
        "scope": "full_axis_request_to_verified_readback_not_provider_model_or_live_opening_slo",
        "observed_at": datetime.now().astimezone().isoformat(), "symbol_count": len(symbols),
        "session_date": receipt["session_date"], "official_receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "candidate_quote_owner_sha256": sha256_file(Path(quotes.__file__)),
        "injected_stall_ms": 0, "fixture_has_non_finite_signed_zero_and_int64_over_2pow53": True,
        "api_requests": 0, "broker_orders": 0, "ledger_writes": 0, "all_outputs_equal": True,
        "pairs": samples, "median": {codec: {field: statistics.median(row[codec][field] for row in samples)
            for field in ("request_to_verified_readback_ms", "client_total_ms", "server_request_queue_ms",
                          "response_write_bytes", "response_encode_publish_ms", "client_response_decode_ms")}
            for codec in ("json_v1", "msgpack_v1")}}
    atomic_write_json(output, result)
    return result


def run_ipc(receipt_path, output, *, repeats=7, stall_ms=200):
    """Full-axis transport A/B without decoding or locking unrelated features."""
    raw = receipt_path.read_bytes()
    receipt = json.loads(raw)
    symbols = receipt["requested_symbols"]
    if not symbols or len(symbols) != len(set(symbols)) or len(symbols) != receipt["requested_count"]:
        raise ValueError("complete unique recorded universe required")
    pairs = []
    with tempfile.TemporaryDirectory(prefix="opening-ipc-benchmark-") as temp:
        for index in range(repeats):
            pair = {}
            for enabled in ([False, True] if index % 2 == 0 else [True, False]):
                label = "autonomous_shared" if enabled else "manual_unshared"
                pair[label] = ipc_trial(Path(temp) / f"{index}-{label}", symbols,
                                       autonomous=enabled, stall_ms=stall_ms)
            if len({row["fixture_output_sha256"] for row in pair.values()}) != 1:
                raise AssertionError("paired full IPC output mismatch")
            if pair["autonomous_shared"]["server_receiver_thread"] != "tw-shared-quote-owner":
                raise AssertionError("autonomous receiver did not serve the IPC request")
            pairs.append(pair)
    result = {"contract": "opening_ipc_ownership_paired_offline_v2",
        "scope": "full_axis_local_ipc_fixture_not_provider_model_or_live_opening_slo",
        "observed_at": datetime.now().astimezone().isoformat(),
        "official_receipt_sha256": hashlib.sha256(raw).hexdigest(),
        "symbol_count": len(symbols), "session_date": receipt["session_date"],
        "injected_ledger_stall_ms": stall_ms, "api_requests": 0, "broker_orders": 0,
        "ledger_writes": 0, "all_outputs_equal": True, "pairs": pairs,
        "median": {label: {field: statistics.median(pair[label][field] for pair in pairs)
                          for field in ("client_total_ms", "server_request_queue_ms")}
                   for label in ("manual_unshared", "autonomous_shared")}}
    atomic_write_json(output, result)
    return result


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
    parser.add_argument("--cache-flush", action="store_true",
                        help="With --receipt, compare full-axis cache read/merge during durable flush")
    parser.add_argument("--ipc-only", action="store_true",
                        help="With --receipt, isolate full-axis IPC from feature-source rebuilding")
    parser.add_argument("--ipc-serialization", action="store_true",
                        help="With --receipt, compare negotiated codecs with the same owner and no injected stall")
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
    if args.cache_flush and not args.receipt:
        parser.error("--cache-flush requires --receipt")
    if args.ipc_only and (not args.receipt or args.cache_flush):
        parser.error("--ipc-only requires --receipt and cannot combine with --cache-flush")
    if args.ipc_serialization and (not args.receipt or args.ipc_only or args.cache_flush):
        parser.error("--ipc-serialization requires --receipt and cannot combine with other modes")
    measured = (run_ipc_serialization(args.receipt, args.output, repeats=args.repeats)
                if args.ipc_serialization else run_ipc(args.receipt, args.output, repeats=args.repeats, stall_ms=args.stall_ms)
                if args.ipc_only else run_cache_flush(args.receipt, args.output, repeats=args.repeats, stall_ms=args.stall_ms)
                if args.cache_flush else run_factorized_stages(args.factorized_market_config, args.output, repeats=args.repeats)
                if args.factorized_market_config else
                run(args.receipt, args.output, repeats=args.repeats, stall_ms=args.stall_ms))
    print(json.dumps({"symbol_count": measured.get("symbol_count"), "scope": measured["scope"],
                      "median": measured["median"]}, indent=2))
