#!/usr/bin/env python3
"""Real current-day full-market acquisition in isolation; never publishes trades.

Each round starts with an EMPTY opening receipt, not a warmed receipt. Calls
the existing simulation engine's broker, never opens an extra Shioaji login.
Midday source timing is explicitly NOT evidence for a true 09:00 SLO.
"""
from __future__ import annotations

import argparse
import base64
import cProfile
import fcntl
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import stat
import subprocess
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
from stockagent.data import tw_current_trading_universe as universe
from stockagent.live.tw_day_trade_simulation import _atomic_json


def compare_mis_pipeline(symbols, root, baseline_file, responses_file, *, rounds, profile_local=False,
                         baseline_acquisition_file=None, retained_universe_file=None,
                         simulated_http_delay_ms=0., simulated_progress_delay_ms=0.):
    """Replay retained public response bytes through the complete local owner.

    Original exchange clocks are untouched; the explicitly frozen local clock
    makes financial/receipt parity comparable. Includes parsing, fan-in, journal
    and fsync, but NOT network latency or a claim about the opening/fill SLO.
    """
    if quotes._SHIOAJI_STOCK_API is not None or not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("MIS comparison requires logged-out isolation and unique symbols")
    if any(not math.isfinite(value) or not 0 <= value <= 100
           for value in (simulated_http_delay_ms, simulated_progress_delay_ms)):
        raise ValueError("offline stage delays must be finite and within 0..100ms")
    retained = json.loads(responses_file.read_bytes())
    fixed = datetime.fromisoformat(retained["observed_at_taipei"])
    if fixed.utcoffset() is None or not 1 <= rounds <= 5:
        raise ValueError("aware retained response clock and 1..5 rounds required")
    bodies = {}
    for row in retained["responses"]:
        body = base64.b64decode(row["body_base64"], validate=True)
        if row["http_status"] != 200 or hashlib.sha256(body).hexdigest() != row["body_sha256"]:
            raise ValueError("retained MIS response integrity/status mismatch")
        if row["channels"] in bodies:
            raise ValueError("duplicate retained MIS request")
        bodies[row["channels"]] = body
    if not bodies:
        raise ValueError("retained MIS responses are empty")
    spec = importlib.util.spec_from_file_location("_mis_quote_baseline", baseline_file)
    before = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = before
    samples = []
    captured_modules = []

    def captured_owner(path, name):
        captured_spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(captured_spec)
        sys.modules[name] = module
        captured_modules.append((name, module))
        captured_spec.loader.exec_module(module)
        return module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)

    timer = SimpleNamespace(time=lambda: fixed.timestamp(), monotonic=time.monotonic,
                            perf_counter=time.perf_counter, sleep=time.sleep)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("retained MIS comparison forbids real network requests")

    try:
        spec.loader.exec_module(before)
        baseline_acquisition = (captured_owner(baseline_acquisition_file, "_mis_acquisition_baseline")
                                if baseline_acquisition_file else acquisition)
        # A retained response belongs to its original registry contract. Run
        # that explicitly captured verifier, including its raw SHA checks,
        # rather than weakening today's membership admission or rewriting an
        # old receipt. This owner is scoped to the offline comparison only.
        authority = (captured_owner(retained_universe_file, "_mis_retained_universe")
                     if retained_universe_file else universe)
        for round_id in range(rounds):
            variants = [("before", before), ("after", quotes)]
            for label, module in (variants if round_id % 2 == 0 else reversed(variants)):
                requests_seen = []

                class Session:
                    def __init__(self):
                        self.headers = {}
                        self.cookies = quotes.requests.cookies.RequestsCookieJar()

                    def get(self, _url, *, params, **_kwargs):
                        key = params["ex_ch"]
                        if key not in bodies:
                            raise AssertionError("request scope differs from retained responses")
                        requests_seen.append(key)
                        if simulated_http_delay_ms:
                            time.sleep(simulated_http_delay_ms / 1000.)
                        body = bodies[key]
                        return SimpleNamespace(content=body, json=lambda: json.loads(body), status_code=200,
                            raise_for_status=lambda: None, close=lambda: None)

                    def close(self):
                        pass

                with ExitStack() as stack:
                    temporary = stack.enter_context(tempfile.TemporaryDirectory(prefix="mis-retained-pipeline-"))
                    stack.enter_context(patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}))
                    stack.enter_context(patch("requests.sessions.Session.request", forbidden))
                    stack.enter_context(patch.object(module.requests, "Session", Session))
                    owner_module = baseline_acquisition if label == "before" else acquisition
                    stack.enter_context(patch.object(owner_module, "quotes", module))
                    stack.enter_context(patch.object(owner_module, "datetime", Clock))
                    # Replaying Friday's retained bytes on Sunday must use the
                    # same explicit capture clock at the authority boundary.
                    # Its original receipt/hash/date checks remain enabled;
                    # never weaken the production current-session reader.
                    stack.enter_context(patch.object(authority, "datetime", Clock))
                    if retained_universe_file:
                        stack.enter_context(patch.dict(sys.modules,
                            {"stockagent.data.tw_current_trading_universe": authority}))
                        stack.enter_context(patch("stockagent.data.tw_current_trading_universe", authority))
                    for name, value in {"datetime": Clock, "time": timer, "_TW_MIS_OPENING_CACHE_KEY": None,
                            "_TW_MIS_OPENING_CACHE": {}, "_TW_MIS_OPENING_REQUESTED": set(),
                            "_TW_MIS_OPENING_DIRTY": False, "_TW_MIS_OPENING_REVISION": 0,
                            "_TW_MIS_BOOTSTRAP_AT": 0., "_TW_MIS_BOOTSTRAP_COOKIES": {}}.items():
                        stack.enter_context(patch.object(module, name, value))
                    profile = cProfile.Profile()
                    storage = {"fsync_calls": 0, "fsync_sum_ms": 0.0, "progress_batches": 0}
                    storage_lock, original_fsync = threading.Lock(), os.fsync
                    stage_spans = {}
                    input_flushes = []

                    def measure_stage(target, attribute, label):
                        original = getattr(target, attribute)

                        def measured(*args, **kwargs):
                            stage_started = time.perf_counter()
                            try:
                                return original(*args, **kwargs)
                            finally:
                                duration = (time.perf_counter() - stage_started) * 1000
                                with storage_lock:
                                    spans = stage_spans.setdefault(label, {"calls": 0, "sum_ms": 0., "max_ms": 0.})
                                    spans["calls"] += 1
                                    spans["sum_ms"] += duration
                                    spans["max_ms"] = max(spans["max_ms"], duration)
                                    if attribute == "persist_tw_opening_snapshot_cache" and kwargs.get("timings"):
                                        input_flushes.append(dict(kwargs["timings"]))

                        stack.enter_context(patch.object(target, attribute, measured))

                    # Lightweight wall spans cover EVERY calling thread, unlike
                    # the optional provider-thread-only cProfile. Nested and
                    # concurrent spans are diagnostic, never additive costs.
                    for attribute in ("seed_tw_opening_snapshot_cache", "fetch_tw_mis_opening_snapshot",
                                      "tw_opening_observation_diagnostics", "persist_tw_opening_snapshot_cache",
                                      "_persist_tw_mis_opening_receipt"):
                        measure_stage(module, attribute, attribute)
                    measure_stage(owner_module, "_record_source_events", "source_journal")

                    def measured_fsync(fd):
                        sync_started = time.perf_counter()
                        try:
                            return original_fsync(fd)
                        finally:
                            with storage_lock:
                                storage["fsync_calls"] += 1
                                storage["fsync_sum_ms"] += (time.perf_counter() - sync_started) * 1000

                    stack.enter_context(patch.object(os, "fsync", measured_fsync))

                    def provider(names, values, progress):
                        def measured_progress(scope, snapshot):
                            storage["progress_batches"] += 1
                            if simulated_progress_delay_ms:
                                time.sleep(simulated_progress_delay_ms / 1000.)
                            progress(scope, snapshot)
                        if profile_local:
                            profile.enable()
                        try:
                            return module.fetch_tw_mis_last_prices(names, values, parquet_root=root,
                                max_parallel_requests=4, chunk_size=80, request_timeout_seconds=4.,
                                empty_chunk_retry_attempts=0, on_progress=measured_progress)
                        finally:
                            if profile_local:
                                profile.disable()

                    fallback = np.full(len(symbols), np.nan)
                    owner = owner_module.OpeningPriceFanIn()
                    started, cpu_started = time.perf_counter(), time.process_time()
                    try:
                        owner.fetch(symbols, fallback, parquet_root=root, providers={"official_mis": provider},
                            minimum_coverage=1., timeout_seconds=5.,
                            cache_reader=lambda: module.fetch_tw_mis_opening_snapshot(
                                symbols, fallback, parquet_root=root, allow_network=False))
                        if not owner.wait_idle(timeout_seconds=5.):
                            raise AssertionError("complete retained MIS work did not drain")
                    finally:
                        owner.close()
                    elapsed_ms, cpu_ms = (time.perf_counter() - started) * 1000, (time.process_time() - cpu_started) * 1000
                    final = module.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
                    receipt_path = module._tw_mis_opening_receipt_path(fixed.date().isoformat())
                    receipt = json.loads(receipt_path.read_bytes())
                    events = [json.loads(line) for line in receipt_path.with_suffix(".acquisition.jsonl").read_text().splitlines()]
                    completed_source = next(row for row in reversed(events)
                        if row["provider"] == "official_mis" and row["event"] == "finished")
                    digest = hashlib.sha256()
                    for name in ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
                                 "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms"):
                        value = getattr(final, name, None)
                        digest.update(name.encode())
                        digest.update(b"None" if value is None else np.asarray(value).tobytes())
                    if set(requests_seen) != set(bodies) or len(requests_seen) != len(bodies):
                        raise AssertionError("full retained MIS request scope was not completed exactly once: "
                            f"variant={label}, observed={len(requests_seen)}, expected={len(bodies)}, "
                            f"missing={len(set(bodies) - set(requests_seen))}, "
                            f"unexpected={len(set(requests_seen) - set(bodies))}")
                    top = sorted(profile.getstats(), key=lambda row: row.totaltime, reverse=True)[:20]
                    samples.append({"round": round_id + 1, "variant": label,
                        "total_local_workflow_ms": round(elapsed_ms, 3), "process_cpu_ms": round(cpu_ms, 3),
                        "storage_barriers": {**storage, "fsync_sum_ms": round(storage["fsync_sum_ms"], 3),
                            "span_policy": "overlapping_barrier_sum_not_additive_to_total"},
                        "local_stage_spans": {name: {key: round(value, 3) if key.endswith("_ms") else value
                            for key, value in span.items()} for name, span in stage_spans.items()},
                        "local_stage_policy": "all_threads_nested_wall_spans_not_additive_v1",
                        "input_durable_flushes": input_flushes,
                        "mis_local_pipeline": {key: completed_source.get("transport_timing", {}).get(key)
                            for key in ("mis_first_pass_ms", "mis_total_ms", "mis_progress_spans", "mis_column_assembly_policy")},
                        "source_dispatch": {key: completed_source[key] for key in (
                            "worker_queue_ms", "provider_call_offset_ms", "dispatch_telemetry_wait_ms",
                            "dispatch_telemetry_recorded", "dispatch_journal_policy") if key in completed_source},
                        "request_count": len(requests_seen), "durable_requested_count": receipt["requested_count"],
                        "valid_open_count": int(module.tw_session_open_observed_mask(final).sum()),
                        "financial_and_clock_sha256": digest.hexdigest(),
                        "durable_rows_sha256": hashlib.sha256(json.dumps(receipt["rows"], sort_keys=True, allow_nan=False).encode()).hexdigest(),
                        "provider_thread_profile": [{"function": str(row.code) if isinstance(row.code, str) else
                            f"{Path(row.code.co_filename).name}:{row.code.co_firstlineno}:{row.code.co_name}",
                            "calls": row.callcount, "cumulative_ms": round(row.totaltime * 1000, 3),
                            "own_ms": round(row.inlinetime * 1000, 3)} for row in top]})
        for name in ("financial_and_clock_sha256", "durable_rows_sha256"):
            if len({row[name] for row in samples}) != 1:
                raise AssertionError(f"retained MIS comparison changed {name}")
        return {"schema_version": 1, "scope": "retained_complete_mis_local_pipeline_not_network_or_opening_slo",
            "requested_count": len(symbols), "response_count": len(bodies), "rounds": rounds,
            "local_clock_policy": "fixed_to_capture_clock_for_exact_receipt_comparison",
            "provider_thread_profiling_enabled": profile_local,
            "controlled_stage_delays": {"http_delay_ms": simulated_http_delay_ms,
                "progress_delay_ms": simulated_progress_delay_ms,
                "boundary": "explicit_synthetic_delays_not_observed_provider_or_storage_latency"},
            "baseline_sha256": hashlib.sha256(baseline_file.read_bytes()).hexdigest(),
            "candidate_sha256": hashlib.sha256(Path(quotes.__file__).read_bytes()).hexdigest(),
            "baseline_acquisition_sha256": hashlib.sha256(Path(baseline_acquisition.__file__).read_bytes()).hexdigest(),
            "candidate_acquisition_sha256": hashlib.sha256(Path(acquisition.__file__).read_bytes()).hexdigest(),
            "retained_universe_verifier_sha256": hashlib.sha256(Path(authority.__file__).read_bytes()).hexdigest(),
            "retained_universe_verifier_explicit": retained_universe_file is not None,
            "retained_manifest_sha256": hashlib.sha256(responses_file.read_bytes()).hexdigest(),
            "financial_and_clock_parity": True, "durable_rows_parity": True, "network_requests": 0,
            "signal_published": False, "broker_order_submitted": False, "production_ledger_modified": False,
            "opening_slo_claim": False, "samples": samples,
            "median_total_local_workflow_ms": {name: statistics.median(row["total_local_workflow_ms"]
                for row in samples if row["variant"] == name) for name in ("before", "after")}}
    finally:
        before._TW_MIS_HTTP_POOL.shutdown(wait=True, cancel_futures=True)
        sys.modules.pop(spec.name, None)
        for name, module in reversed(captured_modules):
            if hasattr(module, "OPENING_FAN_IN"):
                module.OPENING_FAN_IN.close()
            sys.modules.pop(name, None)


def compare_finmind_decoder(symbols, root, baseline_path, payload_path, *, rounds, baseline_quotes_path=None):
    """Same retained full response, original prices/clocks, real fsyncs.

    Zero network latency is explicit. This measures the complete fan-in local
    ingestion, validation, telemetry and durable input workflow, not a live
    opening/fill SLO. Both variants retain the entire requested axis.
    """
    import types
    from datetime import timedelta

    code, body = baseline_path.read_bytes(), payload_path.read_bytes()
    baseline = types.ModuleType("_finmind_decode_baseline")
    baseline_quotes = types.ModuleType("_finmind_quote_baseline") if baseline_quotes_path else quotes
    sys.modules[baseline.__name__] = baseline
    if baseline_quotes_path:
        sys.modules[baseline_quotes.__name__] = baseline_quotes
    samples = []
    now = datetime.now(ZoneInfo("Asia/Taipei"))
    gate = now.replace(hour=9, minute=0, second=0, microsecond=0)
    fixture_at = max(now, gate + timedelta(seconds=12))

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixture_at.astimezone(tz) if tz else fixture_at.replace(tzinfo=None)

    def forbidden(*args, **kwargs):
        raise AssertionError("retained-response comparison forbids network")

    def response(*args, **kwargs):
        return SimpleNamespace(content=body, json=lambda: json.loads(body),
                               raise_for_status=lambda: None, close=lambda: None)

    try:
        if baseline_quotes_path:
            exec(compile(baseline_quotes_path.read_bytes(), str(baseline_quotes_path), "exec"), baseline_quotes.__dict__)
        exec(compile(code, str(baseline_path), "exec"), baseline.__dict__)
        baseline.quotes = baseline_quotes
        for round_id in range(rounds):
            variants = [("before", baseline), ("after", acquisition)]
            for label, module in variants if round_id % 2 == 0 else reversed(variants):
                variant_quotes = baseline_quotes if label == "before" else quotes
                with tempfile.TemporaryDirectory(prefix="finmind-retained-response-") as temporary, \
                        patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary,
                                               "FINMIND_TOKEN": "offline-fixture-not-a-credential"}), \
                        patch("requests.sessions.Session.request", forbidden), \
                        patch.object(module, "datetime", Clock), patch.object(variant_quotes, "datetime", Clock), \
                        patch.object(module, "_http_session", lambda: SimpleNamespace(get=response)), \
                        patch.object(variant_quotes, "_TW_MIS_OPENING_CACHE_KEY", None), \
                        patch.object(variant_quotes, "_TW_MIS_OPENING_CACHE", {}), \
                        patch.object(variant_quotes, "_TW_MIS_OPENING_REQUESTED", set()), \
                        patch.object(variant_quotes, "_TW_MIS_OPENING_DIRTY", False), \
                        patch.object(variant_quotes, "_TW_MIS_OPENING_REVISION", 0):
                    if hasattr(module, "_FINMIND_SNAPSHOT_OWNER"):
                        owner_patch = patch.object(module, "_FINMIND_SNAPSHOT_OWNER", module._FinMindSnapshotOwner())
                    else:
                        from contextlib import nullcontext
                        owner_patch = nullcontext()
                    with owner_patch:
                        fallback = np.full(len(symbols), np.nan)
                        owner = module.OpeningPriceFanIn()
                        started = time.perf_counter()
                        try:
                            result = owner.fetch(symbols, fallback, parquet_root=root,
                                providers={"finmind_snapshot": module.fetch_finmind_opening_prices},
                                minimum_coverage=.9, timeout_seconds=4.5,
                                cache_reader=lambda: variant_quotes.fetch_tw_mis_opening_snapshot(
                                    symbols, fallback, parquet_root=root, allow_network=False))
                            quorum_ms = (time.perf_counter() - started) * 1000
                            drained = owner.wait_idle(timeout_seconds=5)
                        finally:
                            owner.close()
                        total_ms = (time.perf_counter() - started) * 1000
                        final = variant_quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
                        digest = hashlib.sha256()
                        for field in ("prices", "open_prices", "available_mask", "timestamps_ms",
                                      "exchange_timestamps_ms", "simtrade_flags", "no_open_print_timestamps_ms"):
                            value = getattr(final, field)
                            digest.update(field.encode())
                            digest.update(b"None" if value is None else np.asarray(value).tobytes())
                        receipt = json.loads(variant_quotes._tw_mis_opening_receipt_path(fixture_at.date().isoformat()).read_bytes())
                        samples.append({"round": round_id + 1, "variant": label,
                            "quorum_return_ms": round(quorum_ms, 3), "total_local_workflow_ms": round(total_ms, 3),
                            "background_drained": drained, "requested_count": len(symbols),
                            "durable_requested_count": receipt["requested_count"],
                            "valid_open_count": int(quotes.tw_session_open_observed_mask(final).sum()),
                            "financial_and_clock_sha256": digest.hexdigest(),
                            "quorum_reached": result.transport_timing["opening_quorum_ready"]})
        if len({row["financial_and_clock_sha256"] for row in samples}) != 1:
            raise AssertionError("FinMind decoding changed original financial fields or receipt clocks")
        return {"schema_version": 1, "scope": "retained_full_response_complete_local_ingestion_not_network_or_opening_slo",
            "baseline_sha256": hashlib.sha256(code).hexdigest(),
            "candidate_sha256": hashlib.sha256(Path(acquisition.__file__).read_bytes()).hexdigest(),
            "baseline_quote_owner_sha256": hashlib.sha256(baseline_quotes_path.read_bytes()).hexdigest() if baseline_quotes_path else None,
            "candidate_quote_owner_sha256": hashlib.sha256(Path(quotes.__file__).read_bytes()).hexdigest(),
            "payload_sha256": hashlib.sha256(body).hexdigest(), "payload_bytes": len(body),
            "requested_count": len(symbols), "financial_and_clock_parity": True,
            "network_requests": 0, "signal_published": False, "broker_order_submitted": False,
            "production_ledger_modified": False, "opening_slo_claim": False,
            "median_total_local_workflow_ms": {name: statistics.median(row["total_local_workflow_ms"]
                for row in samples if row["variant"] == name) for name in ("before", "after")},
            "samples": samples}
    finally:
        if hasattr(baseline, "OPENING_FAN_IN"):
            baseline.OPENING_FAN_IN.close()
        sys.modules.pop(baseline.__name__, None)
        if baseline_quotes_path:
            sys.modules.pop(baseline_quotes.__name__, None)


def compare_finmind_cadence(symbols, root, *, rounds: int) -> dict:
    """Same full axis/validation/fsync, controlled provider availability clock.

    Execute the pinned original owner rather than inventing a slow surrogate.
    The nine-second upstream update and 1.2-second transfer are explicit
    synthetic inputs. Virtual elapsed time isolates scheduling from CPU noise;
    real wall time is retained separately. No real price or fill SLA is proved.
    """
    import types

    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("complete unique symbol axis required")
    baseline_commit = "5a9383607ddb46fa4951d8f42c0de64afba8d8d0"
    code = subprocess.run(["git", "show", f"{baseline_commit}:stockagent/live/opening_price_acquisition.py"],
                          cwd=ROOT, check=True, capture_output=True).stdout
    before = types.ModuleType("_opening_cadence_baseline")
    sys.modules[before.__name__] = before
    samples = []
    try:
        exec(compile(code, "<pinned-opening-cadence>", "exec"), before.__dict__)
        gate = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)
        fallback = np.full(len(symbols), np.nan)

        def forbidden(*a, **k):
            raise AssertionError("offline cadence comparison forbids network")

        for round_id in range(rounds):
            variants = [("before", before), ("after", acquisition)]
            for label, module in (variants if round_id % 2 == 0 else reversed(variants)):
                clock, starts = [0.], []

                class Clock(datetime):
                    @classmethod
                    def now(cls, tz=None):
                        at = datetime.fromtimestamp(gate.timestamp() + clock[0], gate.tzinfo)
                        return at.astimezone(tz) if tz else at.replace(tzinfo=None)

                timer = SimpleNamespace(perf_counter=time.perf_counter, monotonic=lambda: 1000. + clock[0])
                with tempfile.TemporaryDirectory(prefix="opening-finmind-cadence-") as temporary, \
                        patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}), \
                        patch("requests.sessions.Session.request", forbidden), \
                        patch.object(module, "datetime", Clock), patch.object(module, "time", timer), \
                        patch.object(quotes, "datetime", Clock), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE_KEY", None), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE", {}), \
                        patch.object(quotes, "_TW_MIS_OPENING_REQUESTED", set()), \
                        patch.object(quotes, "_TW_MIS_OPENING_DIRTY", False), \
                        patch.object(quotes, "_TW_MIS_OPENING_REVISION", 0):
                    def provider(names, values, progress):
                        starts.append(round(clock[0], 6))
                        available = clock[0] + 1.e-9 >= 9.
                        clock[0] += 1.2
                        size = len(names)
                        return quotes.PriceSnapshot(prices=np.full(size, 100.) if available else values,
                            open_prices=np.full(size, 100. if available else np.nan),
                            available_mask=np.full(size, available), available_count=size if available else 0,
                            requested_count=size, source="finmind:synthetic_cadence_fixture",
                            timestamps_ms=np.full(size, int(Clock.now(gate.tzinfo).timestamp() * 1000)) if available else np.zeros(size, dtype=np.int64),
                            exchange_timestamps_ms=np.full(size, int((gate.timestamp() + 9.) * 1000)) if available else np.zeros(size, dtype=np.int64),
                            transport_timing={"response_schema_valid": True})

                    owner = module.OpeningPriceFanIn()
                    wall_started = time.perf_counter()
                    try:
                        for _ in range(301):
                            result = owner.fetch(symbols, fallback, parquet_root=root,
                                providers={"finmind_snapshot": provider}, minimum_coverage=1., resolve_no_prints=False,
                                cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False))
                            if not owner.wait_idle(timeout_seconds=1):
                                raise RuntimeError("offline provider failed to drain")
                            if quotes.tw_session_open_observed_mask(result).all():
                                break
                            clock[0] = round(clock[0] + .1, 6)
                        else:
                            raise RuntimeError("offline source never reached complete coverage")
                        digest = hashlib.sha256()
                        for field in ("prices", "open_prices", "available_mask", "exchange_timestamps_ms"):
                            digest.update(np.asarray(getattr(result, field)).tobytes())
                        receipt = json.loads(quotes._tw_mis_opening_receipt_path(gate.date().isoformat()).read_bytes())
                        if len(receipt["rows"]) != len(symbols) or set(receipt["requested_symbols"]) != set(symbols):
                            raise AssertionError("cadence comparison lost durable source scope")
                        samples.append({"round": round_id + 1, "variant": label,
                            "virtual_full_source_completion_ms": round(clock[0] * 1000, 3),
                            "real_comparison_wall_ms": round((time.perf_counter() - wall_started) * 1000, 3),
                            "simulated_request_start_seconds": starts, "simulated_provider_calls": len(starts),
                            "financial_and_exchange_clock_sha256": digest.hexdigest(),
                            "durable_requested_count": len(receipt["requested_symbols"]),
                            "durable_row_count": len(receipt["rows"])})
                    finally:
                        owner.close()
    finally:
        if hasattr(before, "OPENING_FAN_IN"):
            before.OPENING_FAN_IN.close()
        sys.modules.pop(before.__name__, None)
    if len({row["financial_and_exchange_clock_sha256"] for row in samples}) != 1:
        raise AssertionError("full source values or original exchange clock changed")
    return {"schema_version": 1, "scope": "offline_full_axis_controlled_provider_clock_not_live_opening_slo",
        "baseline_commit": baseline_commit, "baseline_code_sha256": hashlib.sha256(code).hexdigest(),
        "candidate_code_sha256": hashlib.sha256(Path(acquisition.__file__).read_bytes()).hexdigest(),
        "requested_count": len(symbols), "synthetic_upstream_first_update_seconds": 9.,
        "synthetic_http_duration_seconds": 1.2, "scheduler_poll_seconds": .1,
        "network_requests": 0, "broker_orders": 0, "production_ledger_modified": False,
        "opening_slo_claim": False, "financial_and_exchange_clock_parity": True,
        "samples": samples, "median_virtual_completion_ms": {label: statistics.median(
            row["virtual_full_source_completion_ms"] for row in samples if row["variant"] == label)
            for label in ("before", "after")}}


def compare_independent_poll(symbols, root, baseline_file: Path, *, rounds: int) -> dict:
    """Real local wall time, identical complete source fixture; NOT a fill SLA.

    The captured previous owner already has one-second start cadence. A slow
    competitor exposes the remaining caller-driven delay. The scaled fixture
    retains every row, validation, canonical merge and durable barrier.
    """
    import types
    if not symbols or len(set(symbols)) != len(symbols) or quotes._SHIOAJI_STOCK_API is not None:
        raise ValueError("isolated logged-out process and complete unique scope required")
    code = baseline_file.read_bytes()
    before = types.ModuleType("_independent_poll_baseline")
    sys.modules[before.__name__] = before
    samples = []
    fixed = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz or fixed.tzinfo)

    def forbidden(*a, **k):
        raise AssertionError("offline comparison forbids network")

    try:
        exec(compile(code, str(baseline_file), "exec"), before.__dict__)
        for round_id in range(rounds):
            variants = [("before", before), ("after", acquisition)]
            for label, module in (variants if round_id % 2 == 0 else reversed(variants)):
                with tempfile.TemporaryDirectory(prefix="opening-independent-poll-") as temporary, \
                        patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}), \
                        patch("requests.sessions.Session.request", forbidden), \
                        patch.object(module, "datetime", Clock), patch.object(quotes, "datetime", Clock), \
                        patch.object(module, "_snapshot_poll_interval", lambda name, at: .1 if name.startswith("finmind") else None), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE_KEY", None), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE", {}), \
                        patch.object(quotes, "_TW_MIS_OPENING_REQUESTED", set()), \
                        patch.object(quotes, "_TW_MIS_OPENING_DIRTY", False), \
                        patch.object(quotes, "_TW_MIS_OPENING_REVISION", 0):
                    starts, calls = [], []
                    fallback = np.full(len(symbols), 99.)
                    started = time.perf_counter()

                    def finmind(names, values, progress):
                        elapsed = time.perf_counter() - started
                        starts.append(elapsed)
                        ready = elapsed >= .2
                        size = len(names)
                        stamp = int(fixed.timestamp() * 1000)
                        return quotes.PriceSnapshot(prices=np.full(size, 100.) if ready else values,
                            open_prices=np.full(size, 100. if ready else np.nan), source="finmind:fixture",
                            available_mask=np.full(size, ready), available_count=size if ready else 0, requested_count=size,
                            timestamps_ms=np.full(size, stamp if ready else 0, dtype=np.int64),
                            exchange_timestamps_ms=np.full(size, stamp if ready else 0, dtype=np.int64),
                            transport_timing={"response_schema_valid": True})

                    def slow(*args):
                        calls.append(1)
                        time.sleep(.65)
                        raise TimeoutError("controlled slow source; same before and after")

                    owner = module.OpeningPriceFanIn()
                    try:
                        for _ in range(3):
                            extra = {"poll_opening_snapshots": True} if label == "after" else {}
                            result = owner.fetch(symbols, fallback, parquet_root=root,
                                providers={"finmind_snapshot": finmind, "slow_fixture": slow}, minimum_coverage=1.,
                                cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False),
                                **extra)
                            if quotes.tw_session_open_observed_mask(result).all():
                                break
                        else:
                            raise AssertionError("fixture never reached complete scope")
                        completed_ms = (time.perf_counter() - started) * 1000
                        receipt = json.loads(quotes._tw_mis_opening_receipt_path(fixed.date().isoformat()).read_bytes())
                        if set(receipt["requested_symbols"]) != set(symbols) or len(receipt["rows"]) != len(symbols):
                            raise AssertionError("lost canonical source scope")
                        digest = hashlib.sha256()
                        for field in ("prices", "open_prices", "available_mask", "exchange_timestamps_ms"):
                            digest.update(np.asarray(getattr(result, field)).tobytes())
                        samples.append({"round": round_id + 1, "variant": label, "full_source_quorum_ms": round(completed_ms, 3),
                            "request_starts_ms": [round(x * 1000, 3) for x in starts],
                            "financial_and_exchange_clock_sha256": digest.hexdigest(),
                            "durable_requested_count": len(receipt["requested_symbols"]), "slow_provider_calls": len(calls)})
                    finally:
                        owner.close()
    finally:
        if hasattr(before, "OPENING_FAN_IN"):
            before.OPENING_FAN_IN.close()
        sys.modules.pop(before.__name__, None)
    if len({row["financial_and_exchange_clock_sha256"] for row in samples}) != 1:
        raise AssertionError("changed source values or original exchange clocks")
    return {"contract": "full_axis_independent_snapshot_poll_comparison_v1",
        "scope": "offline_real_local_wall_controlled_sources_not_live_opening_or_fill_slo", "requested_count": len(symbols),
        "baseline_code_sha256": hashlib.sha256(code).hexdigest(),
        "candidate_code_sha256": hashlib.sha256(Path(acquisition.__file__).read_bytes()).hexdigest(),
        "fixture_update_after_seconds": .2, "fixture_poll_seconds": .1, "fixture_slow_provider_seconds": .65,
        "network_requests": 0, "production_ledger_modified": False, "opening_slo_claim": False,
        "financial_and_exchange_clock_parity": True, "samples": samples,
        "median_full_source_quorum_ms": {label: statistics.median(row["full_source_quorum_ms"]
            for row in samples if row["variant"] == label) for label in ("before", "after")}}


def compare_mis_recovery(symbols, root, baseline_file: Path, *, rounds: int) -> dict:
    """Full-axis fault injection with real retry clocks; NOT live API timing.

    The first sweep reports a transport outage, then the fixture is healthy.
    Both owners retain their own production retry policy, scope, validation,
    journal and durable writer. No scaled backoff or deleted input is timed.
    """
    import types
    if (not symbols or len(set(symbols)) != len(symbols) or not 1 <= rounds <= 5
            or quotes._SHIOAJI_STOCK_API is not None):
        raise ValueError("isolated logged-out process, unique full scope and 1..5 rounds required")
    code = baseline_file.read_bytes()
    before = types.ModuleType("_mis_recovery_baseline")
    sys.modules[before.__name__] = before
    samples = []
    fixed = datetime.now(ZoneInfo("Asia/Taipei")).replace(hour=9, minute=0, second=0, microsecond=0)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz or fixed.tzinfo)

    def forbidden(*a, **k):
        raise AssertionError("offline recovery comparison forbids network")

    try:
        exec(compile(code, str(baseline_file), "exec"), before.__dict__)
        for round_id in range(rounds):
            variants = [("before", before), ("after", acquisition)]
            for label, module in (variants if round_id % 2 == 0 else reversed(variants)):
                with tempfile.TemporaryDirectory(prefix="opening-mis-recovery-") as temporary, \
                        patch.dict(os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}), \
                        patch("requests.sessions.Session.request", forbidden), \
                        patch.object(module, "datetime", Clock), patch.object(quotes, "datetime", Clock), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE_KEY", None), \
                        patch.object(quotes, "_TW_MIS_OPENING_CACHE", {}), \
                        patch.object(quotes, "_TW_MIS_OPENING_REQUESTED", set()), \
                        patch.object(quotes, "_TW_MIS_OPENING_DIRTY", False), \
                        patch.object(quotes, "_TW_MIS_OPENING_REVISION", 0):
                    calls = []
                    stamp = int(fixed.timestamp() * 1000)
                    fallback = np.full(len(symbols), 99.)
                    started = time.perf_counter()

                    def provider(names, values, progress):
                        calls.append({"count": len(names), "started_ms": round((time.perf_counter() - started) * 1000, 3)})
                        healthy = len(calls) > 1
                        size = len(names)
                        return quotes.PriceSnapshot(prices=np.full(size, 100.) if healthy else values,
                            open_prices=np.full(size, 100. if healthy else np.nan), source="twse_tpex:mis",
                            available_mask=np.full(size, healthy), available_count=size if healthy else 0,
                            requested_count=size, volumes=np.full(size, 1. if healthy else np.nan),
                            timestamps_ms=np.full(size, stamp, dtype=np.int64),
                            exchange_timestamps_ms=np.full(size, stamp, dtype=np.int64),
                            transport_timing={} if healthy else {"mis_dispatch_stopped": True,
                                "mis_failed_request_count": 4, "mis_error_chain_types": ["ReadTimeout"]})

                    owner = module.OpeningPriceFanIn()
                    try:
                        for _ in range(4):
                            result = owner.fetch(symbols, fallback, parquet_root=root,
                                providers={"official_mis": provider}, minimum_coverage=1., timeout_seconds=.1,
                                cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(
                                    symbols, fallback, parquet_root=root, allow_network=False))
                            if not owner.wait_idle(timeout_seconds=5):
                                raise AssertionError("fixture source leaked work")
                            if quotes.tw_session_open_observed_mask(result).all():
                                break
                            metric = next(iter(owner._sessions.values())).metrics["official_mis"]
                            deadline = min(metric.get("retry_after_monotonic", 0.),
                                metric.get("transport_probe_after_monotonic", float("inf")))
                            time.sleep(max(0., deadline - time.monotonic()))
                        else:
                            raise AssertionError("recovery failed to acquire the complete fixture")
                    finally:
                        owner.close()
                    # Independent readback after every admitted worker/barrier
                    # has drained. A return from the in-memory quorum is not
                    # the end of this complete local workflow measurement.
                    receipt = json.loads(quotes._tw_mis_opening_receipt_path(fixed.date().isoformat()).read_bytes())
                    if set(receipt["requested_symbols"]) != set(symbols) or set(receipt["rows"]) != set(symbols):
                        raise AssertionError("recovery lost canonical requested/durable scope")
                    digest = hashlib.sha256()
                    for field in ("prices", "available_mask", *quotes._PRICE_SNAPSHOT_ARRAY_FIELDS,
                                  "timestamps_ms", "exchange_timestamps_ms", "no_open_print_timestamps_ms"):
                        value = getattr(result, field, None)
                        digest.update(field.encode())
                        digest.update(b"none" if value is None else np.asarray(value).tobytes())
                    samples.append({"round": round_id + 1, "variant": label,
                        "complete_workflow_ms": round((time.perf_counter() - started) * 1000, 3),
                        "source_calls": calls, "valid_open_count": int(quotes.tw_session_open_observed_mask(result).sum()),
                        "durable_requested_count": len(receipt["requested_symbols"]),
                        "financial_and_clock_sha256": digest.hexdigest(),
                        "durable_rows_sha256": hashlib.sha256(json.dumps(receipt["rows"],
                            sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()})
    finally:
        if hasattr(before, "OPENING_FAN_IN"):
            before.OPENING_FAN_IN.close()
        if hasattr(before, "_TELEMETRY_START_POOL"):
            before._TELEMETRY_START_POOL.shutdown(wait=True)
        sys.modules.pop(before.__name__, None)
    if any(len({row[key] for row in samples}) != 1
           for key in ("financial_and_clock_sha256", "durable_rows_sha256")):
        raise AssertionError("recovery changed source/financial clocks or durable rows")
    return {"contract": "full_axis_mis_transport_half_open_recovery_comparison_v1",
        "scope": "offline_injected_outage_real_retry_clocks_not_live_opening_or_fill_slo",
        "requested_count": len(symbols), "synthetic_market_clock": fixed.isoformat(),
        "fixture": "first_full_sweep_transport_timeout_then_healthy_source",
        "baseline_code_sha256": hashlib.sha256(code).hexdigest(),
        "candidate_code_sha256": hashlib.sha256(Path(acquisition.__file__).read_bytes()).hexdigest(),
        "network_requests": 0, "production_ledger_modified": False, "signal_published": False,
        "broker_order_submitted": False, "opening_slo_claim": False,
        "financial_and_clock_parity": True, "durable_row_parity": True, "samples": samples,
        "median_complete_workflow_ms": {label: statistics.median(row["complete_workflow_ms"]
            for row in samples if row["variant"] == label) for label in ("before", "after")}}


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


def scheduled_probe_context(*, observed_at=None, receipt_path=None, output_root=None):
    """Schedule only bounded non-broker sweeps outside the protected open.

    Official scope/calendar receipts are required. A late boot may not turn a
    missed 09:00 measurement into a current-time opening success.
    """
    from scripts.audit_tw_opening_latency import quote_session_phase
    from stockagent.live.market_status import verified_tw_stock_session_day

    now = observed_at or datetime.now(ZoneInfo("Asia/Taipei"))
    phase = quote_session_phase(now)
    now = now.astimezone(ZoneInfo("Asia/Taipei"))
    date_text = now.date().isoformat()
    path = receipt_path or quotes._tw_mis_opening_receipt_path(date_text)
    root = output_root or ROOT / "artifacts/operations/tw_quote_phase_benchmarks"
    output = root / date_text / (phase + "_" + now.strftime("%H%M%S%f") + ".json")
    context = {"phase": phase, "session_date": date_text, "observed_at_taipei": now.isoformat(),
        "scope": "scheduled_current_time_full_market_probe_not_0900_slo",
        "broker_orders": 0, "extra_shioaji_login": False, "production_ledger_modified": False,
        "shioaji_policy": "native_owner_passive_audit_no_snapshot_polling",
        "opening_slo_claim": False}
    if phase in {"opening", "preopen"}:
        context["state"] = "skipped_protected_opening_use_native_passive_audit"
        return path, output, context
    try:
        receipt = json.loads(path.read_bytes())
        official = receipt["official_trading_universe"]
        symbols = receipt["requested_symbols"]
        if (receipt["session_date"] != date_text or official["session_date"] != date_text
                or not str(official["contract"]).startswith("tw_official_current_regular_stock_etf_universe_v")
                or not symbols or len(set(symbols)) != len(symbols)
                or set(symbols) != set(official["symbols"])
                or len(symbols) != official["official_trading_count"]):
            raise ValueError("invalid official full-market scope")
        day_ok, reason = verified_tw_stock_session_day(now.date(), parquet_root=Path(receipt["parquet_root"]).parent)
        context.update(calendar_reason=reason, requested_count=len(symbols))
        context["state"] = "probe" if day_ok else "skipped_unverified_or_closed_session"
    except (OSError, KeyError, TypeError, ValueError) as exc:
        context.update(state="waiting_official_scope", error_type=type(exc).__name__)
    return path, output, context


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, help="Read universe/root only; never reused as the timed opening cache")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scheduled", action="store_true", help="Native intraday/postclose non-broker probe; opening uses the existing passive audit")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--no-connection-prewarm", action="store_true", help="Also retain cold HTTP connection costs; a prewarm failure never cancels other sources")
    parser.add_argument("--compare-no-progress", action="store_true", help="OFFLINE: compare provider-local retry using synthetic stale replies over the receipt's complete universe")
    parser.add_argument("--compare-finmind-cadence", action="store_true", help="OFFLINE: pinned old owner versus bounded start-clock polling over the full recorded universe")
    parser.add_argument("--compare-independent-poll", type=Path, help="OFFLINE: captured previous acquisition owner versus source-owned retry while a competing source stalls")
    parser.add_argument("--compare-mis-recovery", type=Path, help="OFFLINE: full-axis transport outage recovery against a captured acquisition owner; real retry clocks, no live SLO claim")
    parser.add_argument("--compare-finmind-decoder", type=Path, help="OFFLINE: captured prior owner versus complete retained FinMind response ingestion, validation and persistence")
    parser.add_argument("--finmind-payload", type=Path, help="Private retained original response bytes, required only for --compare-finmind-decoder")
    parser.add_argument("--baseline-quote-owner", type=Path, help="Captured pre-change quote owner for the complete local FinMind ingestion comparison")
    parser.add_argument("--compare-mis-pipeline", type=Path, help="OFFLINE: explicit prior quote owner versus retained complete MIS response parsing, fan-in and durable receipts")
    parser.add_argument("--mis-responses", type=Path, help="Integrity-checked retained original MIS response manifest; required with --compare-mis-pipeline")
    parser.add_argument("--profile-mis-local", action="store_true", help="Add provider-thread cProfile; profiling overhead is included, so keep off for ordinary timings")
    parser.add_argument("--mis-simulated-http-delay-ms", type=float, default=0., help="OFFLINE retained MIS comparison only: explicit synthetic per-request wait, 0..100ms")
    parser.add_argument("--mis-simulated-progress-delay-ms", type=float, default=0., help="OFFLINE retained MIS comparison only: explicit synthetic per-callback wait, 0..100ms")
    parser.add_argument("--baseline-acquisition-owner", type=Path, help="OFFLINE MIS comparison: also compare the captured pre-change fan-in owner")
    parser.add_argument("--retained-universe-owner", type=Path, help="OFFLINE MIS comparison: captured registry verifier matching the retained historical response contract; never bypasses its source hashes")
    parser.add_argument("--compare-stream-priority", action="store_true", help="OFFLINE: full-universe native-priority scheduling and opening price parity; not a live feed SLA")
    parser.add_argument("--compare-callback-pipeline", type=Path, help="OFFLINE: compare against the explicit pre-change quote_provider.py; preserve all financial/source arrays and durable writes")
    parser.add_argument("--mis-only", action="store_true", help="Actual-network MIS transport/coverage probe through the existing source owner; no broker requests")
    parser.add_argument("--finmind-only", action="store_true", help="Actual-network full FinMind responses with original HTTP/body/parse clocks; no MIS, broker, publication or orders")
    parser.add_argument("--mis-chunk-size", type=int, default=80)
    parser.add_argument("--mis-request-timeout", type=float, default=4.)
    parser.add_argument("--mis-parallel-requests", type=int, default=4,
                        help="Provider-local workers; defaults to the measured canonical opening setting")
    parser.add_argument("--mis-retry-attempts", type=int, default=1, choices=(0, 1, 2))
    args = parser.parse_args()
    schedule_context = None
    if args.scheduled:
        if args.output or args.rounds != 2 or any(value for name, value in vars(args).items()
                if name.startswith("compare_") or name in {"mis_only", "finmind_only", "profile_mis_local",
                    "baseline_acquisition_owner", "retained_universe_owner", "baseline_quote_owner",
                    "mis_responses", "finmind_payload", "mis_simulated_http_delay_ms", "mis_simulated_progress_delay_ms"}):
            parser.error("scheduled probe owns its providers, one round and append-only output paths")
        args.receipt, args.output, schedule_context = scheduled_probe_context(receipt_path=args.receipt)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if schedule_context["state"] != "probe":
            _atomic_json(args.output, schedule_context)
            print(json.dumps(schedule_context, ensure_ascii=False))
            return 0
        # The systemd owner already serializes invocations; also fence manual
        # scheduled calls so they cannot accidentally multiply source traffic.
        with (args.output.parents[1] / ".probe.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                schedule_context["state"] = "skipped_existing_probe_owner"
                _atomic_json(args.output, schedule_context)
                return 0
            return _network_or_comparison(args, parser, schedule_context)
    if args.receipt is None or args.output is None:
        parser.error("--receipt and --output are required outside --scheduled")
    return _network_or_comparison(args, parser, schedule_context)


def _network_or_comparison(args, parser, schedule_context=None) -> int:
    probe_started = time.perf_counter()
    probe_entered_at = datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="microseconds")
    source_bytes = args.receipt.read_bytes()
    source = json.loads(source_bytes)
    session_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    if not 1 <= args.rounds <= 5:
        parser.error("probe requires 1..5 rounds")
    symbols = list(source.get("requested_symbols") or source["rows"])
    if not symbols or len(set(symbols)) != len(symbols):
        parser.error("universe must be nonempty and unique")
    comparisons = sum(bool(value) for value in (args.compare_no_progress, args.compare_finmind_cadence, args.compare_stream_priority, args.compare_callback_pipeline, args.compare_independent_poll, args.compare_mis_recovery, args.compare_finmind_decoder, args.compare_mis_pipeline))
    if bool(args.compare_mis_pipeline) != bool(args.mis_responses):
        parser.error("retained MIS comparison requires both baseline quote owner and response manifest")
    if args.profile_mis_local and not args.compare_mis_pipeline:
        parser.error("MIS local profiling requires the retained-response comparison")
    if (args.mis_simulated_http_delay_ms or args.mis_simulated_progress_delay_ms) and not args.compare_mis_pipeline:
        parser.error("simulated stage delays require the offline retained MIS comparison")
    if (args.baseline_acquisition_owner or args.retained_universe_owner) and not args.compare_mis_pipeline:
        parser.error("captured fan-in/registry owners require the offline retained MIS comparison")
    if bool(args.compare_finmind_decoder) != bool(args.finmind_payload):
        parser.error("retained FinMind comparison requires both baseline owner and original payload")
    if args.baseline_quote_owner and not args.compare_finmind_decoder:
        parser.error("baseline quote owner is only valid for the retained FinMind comparison")
    if comparisons > 1:
        parser.error("select one isolated comparison")
    if args.mis_only and args.finmind_only:
        parser.error("select at most one isolated network provider")
    if (args.mis_only or args.finmind_only) and comparisons:
        parser.error("network probe cannot be combined with an offline comparison")
    if not (1 <= args.mis_chunk_size <= 80 and .25 <= args.mis_request_timeout <= 8
            and 1 <= args.mis_parallel_requests <= quotes._TW_MIS_HTTP_WORKERS):
        parser.error("MIS probe requires 1..80 channels, .25..8s timeout and 1..16 workers")
    if comparisons:
        result = (compare_mis_pipeline(symbols, source["parquet_root"], args.compare_mis_pipeline, args.mis_responses,
                    rounds=args.rounds, profile_local=args.profile_mis_local,
                    baseline_acquisition_file=args.baseline_acquisition_owner,
                    retained_universe_file=args.retained_universe_owner,
                    simulated_http_delay_ms=args.mis_simulated_http_delay_ms,
                    simulated_progress_delay_ms=args.mis_simulated_progress_delay_ms)
                  if args.compare_mis_pipeline else compare_finmind_decoder(symbols, source["parquet_root"], args.compare_finmind_decoder, args.finmind_payload,
                    rounds=args.rounds, baseline_quotes_path=args.baseline_quote_owner)
                  if args.compare_finmind_decoder else compare_independent_poll(symbols, source["parquet_root"], args.compare_independent_poll, rounds=args.rounds)
                  if args.compare_independent_poll else compare_mis_recovery(symbols, source["parquet_root"], args.compare_mis_recovery, rounds=args.rounds)
                  if args.compare_mis_recovery else compare_finmind_cadence(symbols, source["parquet_root"], rounds=args.rounds)
                  if args.compare_finmind_cadence else compare_callback_pipeline(symbols, args.compare_callback_pipeline, rounds=args.rounds)
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
    if args.finmind_only:
        def finmind_probe(names, values, progress):
            # A real network sample must not claim a decoded-view cache hit as
            # API speed. Keep the owner connection, but require a fresh response.
            return acquisition.fetch_finmind_opening_prices(names, values, progress, force_network=True)
        providers = {"finmind_snapshot": finmind_probe}
    elif args.mis_only:
        def mis_probe(names, values, progress):
            return quotes.fetch_tw_mis_last_prices(names, values, parquet_root=root,
                chunk_size=args.mis_chunk_size, request_timeout_seconds=args.mis_request_timeout,
                max_parallel_requests=args.mis_parallel_requests,
                empty_chunk_retry_attempts=args.mis_retry_attempts, empty_chunk_retry_delay_seconds=.35,
                on_progress=progress)
        providers = {"official_mis": mis_probe}
    else:
        providers = acquisition.configured_opening_providers(root)
        if providers.get("finmind_snapshot") is acquisition.fetch_finmind_opening_prices:
            def fresh_finmind_probe(names, values, progress):
                # Empty receipt does not imply an empty source response cache.
                # A requested network round must retain a new HTTP/body clock,
                # while source-error backoff still cannot be forced through.
                return acquisition.fetch_finmind_opening_prices(names, values, progress, force_network=True)
            providers = {**providers, "finmind_snapshot": fresh_finmind_probe}
    if schedule_context is not None:
        providers = {name: provider for name, provider in providers.items() if name != "shioaji_shared"}
        args.rounds = 1
    report = {"schema_version": 1, "scope": "actual_network_empty_receipt_current_time_not_0900_slo", "observed_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(), "universe_receipt_sha256": hashlib.sha256(source_bytes).hexdigest(), "requested_count": len(symbols), "providers": list(providers), "production_ledger_modified": False, "signal_published": False, "broker_order_submitted": False, "extra_shioaji_login": False, "opening_slo_claim": False, "rounds": []}
    from scripts.audit_tw_opening_latency import quote_session_phase
    report["phase"] = quote_session_phase(datetime.now(ZoneInfo("Asia/Taipei")))
    if schedule_context:
        report["schedule"] = schedule_context
    report["workflow_timing"] = {
        "probe_entered_at_taipei": probe_entered_at,
        "boundary": "network_probe_entry_includes_scope_env_prewarm_and_readback_before_final_report_write",
    }
    report["finmind_probe_policy"] = "fresh_http_per_round_preserve_provider_error_backoff"

    def commit_report():
        # A report cannot contain the duration of its own final durable write.
        # Keep that last span in the workflow's captured stdout instead of
        # silently dropping it or inventing a self-referential zero duration.
        report["workflow_timing"]["total_before_final_report_write_ms"] = round(
            (time.perf_counter() - probe_started) * 1000, 3)
        writing = time.perf_counter()
        _atomic_json(args.output, report)
        committed = time.perf_counter()
        print(json.dumps({"event": "probe_receipt_committed", "report": str(args.output),
            "total_entry_to_report_committed_ms": round((committed - probe_started) * 1000, 3),
            "final_report_write_ms": round((committed - writing) * 1000, 3),
            "status": report["status"], "data_complete": report["data_complete"]},
            ensure_ascii=False), flush=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="opening-source-probe-") as temporary, patch.dict(quotes.os.environ, {"STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT": temporary}):
        report["mis_connection_prewarm"] = {"performed": False}
        prewarm_error = []

        def prewarm_mis():
            if report["mis_connection_prewarm"]["performed"]:
                return report["mis_connection_prewarm"].get("ready") is True
            prewarm_started = time.perf_counter()
            observation = {"performed": True,
                "started_at_taipei": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="microseconds")}
            try:
                observation.update(quotes.warm_tw_mis_quote_client())
            except Exception as exc:
                prewarm_error.append(exc)
                observation.update({
                    "ready": False, "error_type": type(exc).__name__,
                    "error_chain_types": quotes._tw_mis_transport_error_types(exc),
                })
            observation["elapsed_ms"] = round((time.perf_counter() - prewarm_started) * 1000, 3)
            report["mis_connection_prewarm"] = observation
            return observation.get("ready") is True

        prewarm_enabled = not args.no_connection_prewarm and "official_mis" in providers
        if args.mis_only and prewarm_enabled and not prewarm_mis():
            # This is a diagnostic of a refused source, not an accepted full
            # market acquisition. Do not issue a burst after the single-symbol
            # transport probe already proved the endpoint unavailable.
            report.update(status="waiting_source", data_complete=False,
                          network_sweep_skipped="failed_single_symbol_transport_probe")
            commit_report()
            return 0
        if prewarm_enabled and not args.mis_only:
            mis_source = providers["official_mis"]

            def independent_mis(names, values, progress):
                # The optional diagnostic guard belongs to MIS's own worker.
                # Its slow/failed proof must not hold up independent sources.
                # Reuse this one proof across rounds; never burst a source
                # after its transport probe already failed.
                if not prewarm_mis():
                    report["providers_dispatch_skipped"] = {"official_mis": "failed_single_symbol_transport_probe"}
                    error = RuntimeError("MIS diagnostic sweep skipped after failed transport proof")
                    error.opening_transport_timing = {"mis_connection_prewarm": report["mis_connection_prewarm"],
                        "network_sweep_skipped": "failed_single_symbol_transport_probe"}
                    raise error from (prewarm_error[0] if prewarm_error else None)
                return mis_source(names, values, progress)

            providers = {**providers, "official_mis": independent_mis}
            report["mis_prewarm_dispatch"] = "independent_provider_worker_not_global_barrier"
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
            if round_id == 1:
                report["workflow_timing"]["preparation_before_first_fan_in_ms"] = round(
                    (started - probe_started) * 1000, 3)
            try:
                snapshot = owner.fetch(symbols, fallback, parquet_root=root, providers=providers, minimum_coverage=0.90, cache_reader=lambda: quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False))
                quorum_ms = (time.perf_counter() - started) * 1000
                drained = owner.wait_idle(timeout_seconds=40 if schedule_context else 30 if args.mis_only else 8)
            finally:
                owner.close()
            volatile_final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
            volatile_positive = int(quotes.tw_session_open_observed_mask(volatile_final).sum())
            persistence_pending = quotes._TW_MIS_OPENING_DIRTY
            # The source workers have stopped. Reconstruct via the canonical
            # receipt reader with an empty process cache: a file's existence
            # alone does not prove every in-memory source fact was committed.
            # Do not flush here; that would hide a failed producer's barrier.
            quotes._TW_MIS_OPENING_CACHE_KEY = None
            quotes._TW_MIS_OPENING_CACHE = {}
            quotes._TW_MIS_OPENING_REQUESTED = set()
            quotes._TW_MIS_OPENING_DIRTY = False
            final = quotes.fetch_tw_mis_opening_snapshot(symbols, fallback, parquet_root=root, allow_network=False)
            provider_states = {name: dict(metric) for state in owner._sessions.values() for name, metric in state.metrics.items()}
            record = {"round": round_id, "initial_cached_open_count": 0, "quorum_return_ms": round(quorum_ms, 3), "all_sources_complete_ms": round((time.perf_counter() - started) * 1000, 3), "background_drained": drained, "positive_opens_at_quorum": int(quotes.tw_session_open_observed_mask(snapshot).sum()), "resolved_at_quorum": int((quotes.tw_session_open_observed_mask(snapshot) | quotes.tw_session_no_open_print_mask(snapshot)).sum()), "final_positive_opens": int(quotes.tw_session_open_observed_mask(final).sum()), "final_observed_no_prints": int(quotes.tw_session_no_open_print_mask(final).sum()), "quorum_timing": snapshot.transport_timing, "providers": provider_states, "final_conflicts": final.transport_timing.get("opening_conflict_count", 0)}
            report["rounds"].append(record)
            if receipt.exists():
                shutil.copyfile(receipt, args.output.with_name(f"{args.output.stem}.round{round_id}.receipt.json"))
                record["durable_receipt_retained"] = True
            else:
                record["durable_receipt_retained"] = False
                record["data_state"] = "missing_durable_receipt"
            record.update(
                volatile_final_positive_opens=volatile_positive,
                input_persistence_pending_before_readback=persistence_pending,
                readback_boundary="canonical_receipt_empty_process_cache_not_forced_os_disk_cache_miss",
                unknown_open_count=len(symbols) - record["final_positive_opens"] - record["final_observed_no_prints"],
                quorum_reached=record["resolved_at_quorum"] >= int(np.ceil(len(symbols) * .90)),
                data_complete=record["durable_receipt_retained"] and record["final_positive_opens"] == len(symbols),
                resolved_complete=record["durable_receipt_retained"] and record["final_positive_opens"] + record["final_observed_no_prints"] == len(symbols),
                all_sources_complete_boundary="source_jobs_stopped_not_full_quote_coverage",
                entry_to_round_readback_ms=round((time.perf_counter() - probe_started) * 1000, 3))
            if record["durable_receipt_retained"]:
                record["data_state"] = ("complete" if record["data_complete"] else
                    "awaiting_first_print" if record["resolved_complete"] else
                    "partial" if record["final_positive_opens"] + record["final_observed_no_prints"] else "waiting_source")
            _atomic_json(args.output, report)
            print(json.dumps({k: v for k, v in record.items() if k not in {"quorum_timing", "providers"}}, ensure_ascii=False), flush=True)
        telemetry = quotes._tw_mis_opening_receipt_path(session_date).with_suffix(".acquisition.jsonl")
        if telemetry.exists():
            shutil.copyfile(telemetry, args.output.with_suffix(".acquisition.jsonl"))
            from scripts.audit_tw_opening_latency import opening_coverage_milestones
            audit = opening_coverage_milestones(telemetry, session_date, symbols)
            report["provider_latency"] = audit["provider_latency"]
            report["provider_phase_latency"] = audit["provider_phase_latency"]
    report["data_complete"] = bool(report["rounds"]) and all(row["data_complete"] for row in report["rounds"])
    report["resolved_complete"] = bool(report["rounds"]) and all(row["resolved_complete"] for row in report["rounds"])
    report["status"] = ("complete" if report["data_complete"] else
        "awaiting_first_print" if report["resolved_complete"] else
        "partial" if any(row["final_positive_opens"] + row["final_observed_no_prints"] for row in report["rounds"]) else "waiting_source")
    commit_report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
