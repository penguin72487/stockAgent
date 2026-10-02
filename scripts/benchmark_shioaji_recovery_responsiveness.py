"""Bounded fixture-only ABBA of canonical synchronous/background price recovery.

Broker replies and usage latency are fixtures; canonical accounting, atomic
receipts and thread dispatch are real. This measures event-loop responsiveness,
not live opening performance. No provider login, orders or source promotion.
"""

from __future__ import annotations

import argparse
from collections import deque
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import requests  # noqa: E402

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from downloader.common import provider_rate_limit  # noqa: E402
from scripts import run_tw_day_trade_simulation as runner  # noqa: E402
from stockagent.live import quote_provider as provider  # noqa: E402


class _FixtureAPI:
    def __init__(self, native_ms: float, usage_ms: float):
        self.native_ms, self.usage_ms = native_ms, usage_ms
        self.used = 100
        self.native_calls = self.usage_calls = 0
        self.contracts = SimpleNamespace(get=lambda code: SimpleNamespace(code=code))

    def usage(self):
        self.usage_calls += 1
        time.sleep(self.usage_ms / 1_000)
        return SimpleNamespace(bytes=self.used, limit_bytes=1_000_000)

    def ticks(self, **kwargs):
        if (kwargs["date"] != "2026-10-01" or kwargs["time_start"] != "09:00:00"
                or kwargs["time_end"] != "09:00:59"):
            raise AssertionError("canonical source window changed")
        self.native_calls += 1
        time.sleep(self.native_ms / 1_000)
        self.used += 2
        result = SimpleNamespace(
            ts=[int(np.datetime64(t, "ns").astype(np.int64)) for t in (
                "2026-10-01T09:00:10", "2026-10-01T09:00:50",
            )], close=[100.0, 110.0], volume=[1.0, 3.0],
        )
        if callback := kwargs.get("cb"):
            callback(result)
            return SimpleNamespace(ts=[], close=[], volume=[])
        return result


def run_probe(*, symbols: int = 8, batch_size: int = 8,
              native_delay_ms: float = 10.0, usage_delay_ms: float = 2.0) -> dict:
    if not isinstance(symbols, int) or not 1 <= symbols <= 8:
        raise ValueError("symbols must be in 1..8")
    if not isinstance(batch_size, int) or not 1 <= batch_size <= 8:
        raise ValueError("batch_size must be in 1..8")
    if any(not math.isfinite(v) or not 0 <= v <= 20 for v in (native_delay_ms, usage_delay_ms)):
        raise ValueError("fixture delays must be finite and in 0..20 ms")
    started = time.perf_counter()
    observed = datetime(2026, 10, 1, 9, 5, tzinfo=ZoneInfo("Asia/Taipei"))
    codes = {str(1000 + index) for index in range(symbols)}
    api = _FixtureAPI(native_delay_ms, usage_delay_ms)
    trials = []

    def forbidden(*_args, **_kwargs):
        raise AssertionError("fixture benchmark attempted real provider access")

    with tempfile.TemporaryDirectory(prefix="stockagent-recovery-probe-") as workspace, ExitStack() as guards:
        root = Path(workspace)
        journal = root / "journal"
        guards.enter_context(patch.dict("os.environ", {"STOCKAGENT_SHIOAJI_TRAFFIC_LEDGER_ROOT": str(journal)}))
        guards.enter_context(patch.object(provider, "_shioaji_stock_api", lambda: api))
        guards.enter_context(patch.object(provider, "_SHIOAJI_STOCK_CONTRACTS", {}))
        guards.enter_context(patch.object(provider, "_SHIOAJI_HISTORY_REQUEST_TIMES", deque()))
        guards.enter_context(patch.dict(sys.modules, {"shioaji": SimpleNamespace(
            TicksQueryType=SimpleNamespace(RangeTime="RangeTime"),
        )}))
        guards.enter_context(patch.object(runner, "_missed_opening_minute_roots", lambda: []))
        guards.enter_context(patch.object(runner, "notify_systemd", lambda *_: None))
        guards.enter_context(patch.object(requests.Session, "request", forbidden))
        for index, variant in enumerate(("synchronous", "background", "background", "synchronous")):
            state_dir = root / f"trial-{index}"
            native_before, usage_before = api.native_calls, api.usage_calls
            calls_ms = []
            begin = time.perf_counter()
            recovery = None
            try:
                if variant == "synchronous":
                    prices, receipt = runner._resolve_missed_opening_prices(state_dir, observed, codes)
                    calls_ms.append((time.perf_counter() - begin) * 1_000)
                else:
                    recovery = runner._MissedOpeningPriceRecovery(state_dir, batch_size=batch_size)
                    deadline = begin + 10
                    while True:
                        call_started = time.perf_counter()
                        prices, receipt = recovery.poll(observed, codes)
                        calls_ms.append((time.perf_counter() - call_started) * 1_000)
                        if set(prices) == codes and not recovery._thread.is_alive():
                            break
                        if time.perf_counter() >= deadline:
                            raise TimeoutError("bounded fixture recovery did not finish")
                        time.sleep(0.001)
            finally:
                if recovery is not None and recovery._thread is not None:
                    recovery._thread.join(timeout=5)
                    if recovery._thread.is_alive():
                        raise TimeoutError("fixture worker did not stop")
            total_ms = (time.perf_counter() - begin) * 1_000
            retained_prices, retained_receipt = runner._load_missed_opening_prices(state_dir, observed)
            if (set(prices) != codes or retained_prices != prices or retained_receipt != receipt
                    or receipt.get("error_counts") or receipt.get("unqueried_symbols")
                    or api.native_calls - native_before != symbols):
                raise AssertionError("price recovery/receipt equivalence failed")
            trials.append({
                "variant": variant, "total_recovery_ms": round(total_ms, 3),
                "main_call_max_ms": round(max(calls_ms), 3),
                "main_call_median_ms": round(statistics.median(calls_ms), 3),
                "main_calls": len(calls_ms), "fixture_native_calls": api.native_calls - native_before,
                "fixture_usage_calls": api.usage_calls - usage_before,
                "prices_sha256": hashlib.sha256(json.dumps(prices, sort_keys=True).encode()).hexdigest(),
                "durable_receipt_matches": True,
                "remote_timing_scope": "last batch only; not whole recovery" if variant == "background" else "whole cohort",
                "remote_timing": receipt["remote"]["timing"],
            })
        raw = [json.loads(line) for path in (journal / "daily").glob("*.jsonl")
               for line in path.read_text().splitlines()]
        summary = json.loads((journal / "summary.json").read_text())
        if (len(raw) != 4 * symbols or summary["totals"]["queries"] != 4 * symbols
                or summary["totals"]["rows"] != 8 * symbols
                or len({t["prices_sha256"] for t in trials}) != 1):
            raise AssertionError("accounting/price parity failed")
        journal_bytes = sum(p.stat().st_size for p in journal.rglob("*") if p.is_file())
    return {
        "schema_version": 1, "contract": "shioaji-recovery-fixture-abba-v1",
        "state": "accepted_fixture_only", "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "fixture_only": True, "symbols": symbols, "batch_size": batch_size,
        "fixture_native_delay_ms": native_delay_ms, "fixture_usage_delay_ms": usage_delay_ms,
        "native_profile": {
            "requests": provider_rate_limit("shioaji_quote_query").requests,
            "seconds": provider_rate_limit("shioaji_quote_query").seconds,
        },
        "live_broker_requests": 0, "source_files_written": 0, "promoted": False,
        "running_processes_reloaded": False, "full_live_job_measured": False,
        "os_cache_controlled": False, "imports_and_discovery_measured": False,
        "scope": "fixed reply/usage fixtures; real canonical ledger fsync, recovery receipts and dispatch",
        "trials": trials, "journal_events": len(raw), "journal_bytes": journal_bytes,
        "all_price_hashes_equal": True,
        "main_call_max_medians_ms": {
            v: statistics.median(t["main_call_max_ms"] for t in trials if t["variant"] == v)
            for v in ("synchronous", "background")
        },
        "full_recovery_medians_ms": {
            v: statistics.median(t["total_recovery_ms"] for t in trials if t["variant"] == v)
            for v in ("synchronous", "background")
        },
        "elapsed_seconds": time.perf_counter() - started,
        "code_sha256": {
            "provider": sha256_file(Path(provider.__file__)),
            "runner": sha256_file(Path(runner.__file__)),
            "ledger": sha256_file(ROOT / "stockagent/live/shioaji_traffic_ledger.py"),
            "benchmark": sha256_file(Path(__file__)),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--native-delay-ms", type=float, default=10.0)
    parser.add_argument("--usage-delay-ms", type=float, default=2.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.output.is_symlink():
        raise FileExistsError("benchmark output already exists")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        raise ValueError("output must be under artifacts/benchmarks")
    result = run_probe(symbols=args.symbols, batch_size=args.batch_size,
                       native_delay_ms=args.native_delay_ms, usage_delay_ms=args.usage_delay_ms)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in (
        "state", "main_call_max_medians_ms", "full_recovery_medians_ms", "elapsed_seconds",
    )}))


if __name__ == "__main__":
    main()
