#!/usr/bin/env python3
"""Offline scheduler retry/lifetime comparison, NOT live opening/fill latency."""
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path
import resource
import sys
import time
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.discord_bot import bot
from stockagent.live.tw_day_trade_simulation import _atomic_json, TwDayTradeSimulationEngine
from stockagent.live.opening_execution_latency import OpeningExecutionObserver


async def trial(policy, *, slow_seconds, retry_seconds=.25):
    started, cpu_started = time.perf_counter(), time.process_time()
    now = datetime.fromisoformat("2026-10-14T09:00:00.004+08:00")
    configs = [SimpleNamespace(market=name, day_trade_simulation_enabled=True,
                               overnight_simulation_enabled=False) for name in ("slow", "fast")]
    calls, success, retry_at = {"slow": 0, "fast": 0}, {}, {}
    original = {cfg.market: (cfg, now, cfg.market + ":fixture", False) for cfg in configs}
    output = {}

    async def prepare(row, stage=None):
        name = row[0].market
        calls[name] += 1
        await asyncio.sleep(slow_seconds if name == "slow" else .003)
        error = RuntimeError("fixture_source_pending") if name == "fast" and calls[name] == 1 else None
        prices = None if error else (100., 101., 102.)
        return prices, error, 0., None, {}

    with patch.object(bot.bot, "_scheduled_input_jobs", {}), \
         patch.object(bot.bot, "_opening_input_stages", {}), \
         patch.object(bot.bot, "_opening_first_observed_at", {}), \
         patch.object(bot, "_preopen_market_final_armed_for_session", lambda *_: False), \
         patch.object(bot, "_prepare_scheduled_attempt_inputs", prepare):
        while len(success) < len(configs):
            due = [row for name, row in original.items()
                   if name not in success and time.monotonic() >= retry_at.get(name, 0)]
            if policy == "bounded_single_flight":
                rows, jobs = bot._scheduled_input_poll_jobs(due)
                iterator = bot._scheduled_inputs_ready_order(rows, jobs, poll_timeout=.05, cancel_pending=False)
            else:
                rows = due
                jobs = {row[0].market: asyncio.create_task(prepare(row)) for row in rows}
                iterator = bot._scheduled_inputs_ready_order(rows, jobs)
            async for row, inputs in iterator:
                name = row[0].market
                bot.bot._scheduled_input_jobs.pop(name, None)
                if inputs[1] is not None:
                    retry_at[name] = time.monotonic() + retry_seconds
                else:
                    success[name] = round((time.perf_counter() - started) * 1000, 3)
                    output[name] = inputs[0]
            if len(success) < len(configs):
                await asyncio.sleep(.1)  # Canonical scheduler cadence, not a tight benchmark spin.
        bot._scheduled_input_poll_jobs([])
    return {"policy": policy, "first_fast_input_success_ms": success["fast"],
            "all_mode_input_success_ms": max(success.values()), "source_calls": calls,
            "fixture_input_sha256": hashlib.sha256(json.dumps(output, sort_keys=True).encode()).hexdigest(),
            "cpu_ms": round((time.process_time() - cpu_started) * 1000, 3)}


def state_publication_trial(state_file, directory):
    """Full private projection of one captured state, no price/model/ledger work."""
    raw = state_file.read_bytes()
    captured = json.loads(raw)
    clock = datetime.fromisoformat(captured["updated_at"])
    metadata = {"state_revision", "dashboard_content_revision", "dashboard_content_fingerprint", "engine_run_id", "updated_at"}

    def economic_sha(state):
        return hashlib.sha256(json.dumps({key: value for key, value in state.items() if key not in metadata},
                                         sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    expected = economic_sha(captured)
    rows = []
    for round_id in range(3):
        policies = ("without_progress_observer", "with_progress_observer")
        for policy in policies[::1 if round_id % 2 == 0 else -1]:
            with TemporaryDirectory(prefix="opening-private-projection-", dir=directory) as private:
                engine = TwDayTradeSimulationEngine(Path(private), publication_clock=lambda: clock)
                engine.state = deepcopy(captured)
                observer = OpeningExecutionObserver({})
                if policy == "without_progress_observer":
                    observer.samples = lambda *_a, **_kw: []
                engine._opening_execution_observer = observer
                started = time.perf_counter()
                engine._persist(clock)
                elapsed = (time.perf_counter() - started) * 1000
                restored = json.loads(engine.state_path.read_text())
                if economic_sha(restored) != expected:
                    raise AssertionError("financial/quantity/price/source state changed")
                if engine.last_persist_metrics.get("execution_observer_error"):
                    raise AssertionError("progress observer failed")
                rows.append({"round": round_id + 1, "policy": policy,
                             "full_projection_persist_ms": round(elapsed, 3),
                             "execution_observer_ms": engine.last_persist_metrics["execution_observer_ms"],
                             "execution_observer_rows": engine.last_persist_metrics["execution_observer_rows"],
                             "economic_state_sha256": expected})
    return {"measurement_boundary": "captured_full_state_private_fsync_projection_not_signal_or_fill",
            "source_state_sha256": hashlib.sha256(raw).hexdigest(), "source_state_bytes": len(raw),
            "source_state_observed_at": captured["updated_at"], "financial_state_exact_parity": True,
            "source_clock_is_frozen_benchmark_fixture": True, "trials": rows}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--slow-seconds", type=float, default=.6)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--state-file", type=Path, help="Optional read-only captured full state; benchmark writes only private temporary projections")
    args = parser.parse_args()
    if not .4 <= args.slow_seconds <= 2 or not 1 <= args.rounds <= 5:
        parser.error("slow-seconds must be .4..2 and rounds 1..5")
    results = []
    for index in range(args.rounds):
        policies = ("blocking_round", "bounded_single_flight") if index % 2 == 0 else (
            "bounded_single_flight", "blocking_round")
        pair = [asyncio.run(trial(policy, slow_seconds=args.slow_seconds)) for policy in policies]
        if len({row["fixture_input_sha256"] for row in pair}) != 1:
            raise AssertionError("fixture inputs differ")
        if any(row["source_calls"] != {"slow": 1, "fast": 2} for row in pair):
            raise AssertionError("duplicate input owners")
        results.append(pair)
    report = {"schema_version": 1, "measurement_boundary": "offline_two_mode_input_dispatch_not_inference_or_fill",
              "slow_source_fixture_seconds": args.slow_seconds,
              "live_opening_slo_claim": False, "broker_orders_submitted": False,
              "live_ledger_modified": False, "network_calls": 0, "rounds": results,
              "exact_fixture_inputs_and_call_count_parity": True,
              "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "code_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                              for path in (Path(__file__), ROOT / "services/discord_bot/bot.py",
                                           ROOT / "stockagent/live/tw_day_trade_simulation.py",
                                           ROOT / "stockagent/live/opening_execution_latency.py")}}
    if args.state_file:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        report["private_state_publication"] = state_publication_trial(args.state_file, args.output.parent)
    _atomic_json(args.output, report)
    print(json.dumps({key: value for key, value in report.items() if key != "code_sha256"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
