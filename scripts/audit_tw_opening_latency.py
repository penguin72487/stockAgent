#!/usr/bin/env python3
"""Retain real opening attempts and benchmark current models without publishing.

The optional inference probe is receipt-only and never writes a signal pointer,
ledger, or broker order. Its midday/hot-cache timing is NOT a 09:00 latency.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import time
import subprocess
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.discord_bot import bot
from stockagent.live import signal_engine
from stockagent.live.tw_day_trade_simulation import _atomic_json


def opening_coverage_milestones(path: Path, session_date: str, symbols: list[str]) -> dict:
    """Audit retained validation events, not invented exchange/feed clocks.

    A latest receipt's axis is a labelled posthoc scope, not evidence that the
    same roster existed at 09:00. Retain unknowns and source failures. Native
    receipt clocks, durable quorum, model publication and fills are separate.
    """
    start = datetime.fromisoformat(session_date + "T09:00:00+08:00")
    requested = set(symbols)
    if not requested or len(requested) != len(symbols):
        raise ValueError("opening coverage audit requires a nonempty unique scope")
    cutoffs = (100, 250, 500, 1000, 2000, 5000)
    states = {cutoff: {} for cutoff in cutoffs}
    failures, malformed, known_providers, legacy_events = {}, 0, set(), []
    if not path.exists():
        return {"state": "unavailable", "path": str(path), "requested_count": len(symbols)}
    with path.open() as handle:
        for line in handle:
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("invalid source event")
                if not isinstance(row.get("session_date"), str) or not row["session_date"]:
                    raise ValueError("invalid session date")
                if row.get("session_date") != session_date:
                    continue
                at = datetime.fromisoformat(row["observed_at_taipei"])
                if at.tzinfo is None:
                    raise ValueError("missing observation timezone")
                elapsed = (at - start).total_seconds() * 1000
                provider = row["provider"]
                if not isinstance(provider, str) or not provider:
                    raise ValueError("invalid provider")
                if provider != "merged" and row.get("event") in {"started", "finished", "observations_delta"}:
                    known_providers.add(provider)
                if (row.get("event") == "first_batch" and 0 <= elapsed <= max(cutoffs)
                        and not isinstance(row.get("observation_quality"), dict)):
                    # Older owners logged counts, not IDs. Lack of the new
                    # delta schema is missing evidence, NOT zero quotes.
                    legacy_events.append({key: row.get(key) for key in (
                        "provider", "observed_at_taipei", "requested_count",
                        "response_count", "valid_open_count")})
                if row.get("event") == "finished" and row.get("state") in {"failed", "degraded"}:
                    failures[provider] = failures.get(provider, 0) + 1
                if row.get("event") != "observations_delta" or elapsed < 0:
                    continue
                for cutoff in cutoffs:
                    if elapsed > cutoff:
                        continue
                    source_states = states[cutoff].setdefault(provider, {"valid": set(), "no_print": set()})
                    for observation in row.get("observations", []):
                        symbol, state = observation["symbol"], observation["state"]
                        if symbol not in requested:
                            continue
                        if state == "valid_open":
                            source_states["valid"].add(symbol)
                            source_states["no_print"].discard(symbol)
                        elif state == "observed_no_first_print" and symbol not in source_states["valid"]:
                            source_states["no_print"].add(symbol)
            except (KeyError, TypeError, ValueError):
                malformed += 1
    milestones = []
    for cutoff, providers in states.items():
        for provider in sorted(known_providers):
            providers.setdefault(provider, {"valid": set(), "no_print": set()})
        valid = set().union(*(source["valid"] for source in providers.values()))
        no_print = set().union(*(source["no_print"] for source in providers.values())) - valid
        milestones.append({"after_open_ms": cutoff, "valid_open_count": len(valid),
            "observed_no_print_count": len(no_print), "unknown_count": len(requested - valid - no_print),
            "providers": {name: {"valid_open_count": len(source["valid"]),
                "observed_no_print_count": len(source["no_print"])} for name, source in providers.items()}})
    if legacy_events:
        for row in milestones:
            row["retained_symbol_event_lower_bounds"] = {key: row[key] for key in ("valid_open_count", "observed_no_print_count")}
            row.update(valid_open_count=None, observed_no_print_count=None, unknown_count=None,
                       provider_counts_are_lower_bounds=True)
    return {"state": "degraded" if malformed else "insufficient_evidence" if legacy_events else "observed", "path": str(path), "session_date": session_date,
        "requested_count": len(symbols), "scope_basis": "canonical_receipt_posthoc_axis_not_backdated_registry",
        "measurement_boundary": "source_validation_event_clock_not_durable_quorum_or_broker_fill",
        "malformed_lines": malformed, "legacy_aggregate_events": legacy_events,
        "per_symbol_schema_usable": not legacy_events and not malformed,
        "failed_or_degraded_jobs": failures, "milestones": milestones}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--infer", action="store_true")
    parser.add_argument("--runtime", action="store_true", help="Attach current owners, Gateway, execution and source-repair receipts; no mutations")
    parser.add_argument("--ledger-fingerprints", action="store_true", help="Explicit deep reload audit with --runtime: stream-hash large immutable ledgers; normal health reads do not scan them")
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()
    if args.ledger_fingerprints and not args.runtime:
        parser.error("--ledger-fingerprints requires --runtime")
    if not 1 <= args.rounds <= 10:
        parser.error("rounds must be 1..10")
    if args.infer and args.session_date != datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat():
        parser.error("receipt-only inference cannot backdate current observations")
    if args.infer and not (datetime.now(ZoneInfo("Asia/Taipei")).hour >= 9):
        parser.error("receipt-only current-open inference cannot run before 09:00")
    report = {"schema_version": 1, "session_date": args.session_date,
              "observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
              "simulation_only": True, "ledger_modified": False, "signal_published": False,
              "orders_submitted": False, "real_opening_attempts": {}, "receipt_only_model_probes": []}
    path = bot._opening_signal_latency_path()
    with path.open() as handle:
        for line in handle:
            row = json.loads(line)
            if row.get("session_date") != args.session_date:
                continue
            stats = report["real_opening_attempts"].setdefault(row["market"], {
                "attempt_count": 0, "failed_count": 0, "first": row,
                "last": row, "ready": []})
            stats["attempt_count"] += 1
            stats["failed_count"] += int(row["status"] == "failed")
            stats["last"] = row
            if row["status"] == "ready":
                stats["ready"].append(row)
    _atomic_json(args.output, report)
    from stockagent.live import quote_provider as quotes
    receipt_path = quotes._tw_mis_opening_receipt_path(args.session_date)
    try:
        opening_receipt = json.loads(receipt_path.read_text())
        report["source_coverage_milestones"] = opening_coverage_milestones(
            receipt_path.with_suffix(".acquisition.jsonl"), args.session_date,
            opening_receipt["requested_symbols"])
        report["source_coverage_milestones"]["universe_receipt_sha256"] = hashlib.sha256(receipt_path.read_bytes()).hexdigest()
        report["source_coverage_milestones"]["official_scope_observed_at"] = opening_receipt.get("official_trading_universe", {}).get("observed_at_taipei")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        report["source_coverage_milestones"] = {"state": "unavailable", "error_type": type(exc).__name__}
    _atomic_json(args.output, report)
    if args.runtime:
        runtime = {"observed_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
                   "service_restart_performed": False, "broker_orders_submitted": False}
        clock_started = time.perf_counter()
        recovered = bot._recover_opening_first_observations(args.session_date)
        runtime["first_scheduler_observation_recovery_ms"] = round((time.perf_counter() - clock_started) * 1000, 3)
        runtime["first_scheduler_observations"] = {key: value.isoformat(timespec="microseconds") for key, value in recovered.items()}
        runtime["scheduler_clock_contract"] = "session_first_observation_across_owner_restarts_v1"
        runtime["ledger_fingerprints"] = {}
        runtime["ledger_fingerprints_performed"] = args.ledger_fingerprints
        for name in (("fills.jsonl", "orders.jsonl", "signals.jsonl") if args.ledger_fingerprints else ()):
            path = bot._day_trade_state_dir() / name
            try:
                before = path.stat()
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                after = path.stat()
                unchanged = (before.st_ino, before.st_size, before.st_mtime_ns) == (after.st_ino, after.st_size, after.st_mtime_ns)
                runtime["ledger_fingerprints"][name] = {"state": "observed" if unchanged else "concurrent_change",
                    "bytes": after.st_size, "sha256": digest.hexdigest() if unchanged else None}
            except OSError as exc:
                runtime["ledger_fingerprints"][name] = {"state": "unavailable", "error_type": type(exc).__name__}
        for name, path in {
            "engine": bot._day_trade_state_dir() / "status.json",
            "discord": bot._discord_service_status_path(),
            "source_repair": bot._day_trade_state_dir() / "opening_source_repair.json",
        }.items():
            try:
                runtime[name] = json.loads(path.read_text())
            except (OSError, ValueError) as exc:
                runtime[name] = {"status": "unavailable", "error_type": type(exc).__name__}
        owner = subprocess.run(["systemctl", "show",
            "stockagent-discord-bot.service", "stockagent-tw-day-trade-simulation.service",
            "-p", "Id", "-p", "ActiveState", "-p", "SubState", "-p", "InvocationID",
            "-p", "MainPID", "-p", "ActiveEnterTimestamp", "-p", "NRestarts"],
            capture_output=True, text=True, timeout=10, check=True)
        runtime["service_owners"] = [dict(line.split("=", 1) for line in block.splitlines()
             if "=" in line) for block in owner.stdout.strip().split("\n\n")]
        runtime["current_session_signal_ready"] = {
            market: bool(stats["ready"]) for market, stats in report["real_opening_attempts"].items()}
        runtime["current_session_engine_registered"] = {
            market: mode.get("session_date") == args.session_date
            for market, mode in (runtime.get("engine", {}).get("modes") or {}).items()}
        runtime["opening_slo_success_claim"] = False
        report["runtime_verification"] = runtime
        _atomic_json(args.output, report)
    if not args.infer:
        return 0

    def receipt_only(symbols, fallback, *, parquet_root, fetch_mis):
        return fetch_mis(allow_network=False), True

    with patch.object(signal_engine, "_fetch_tw_opening_source", receipt_only):
        for round_id in range(1, args.rounds + 1):
            for original in bot._market_configs().values():
                if not original.day_trade_simulation_enabled or not bot._market_enabled(original):
                    continue
                cfg = bot._effective_market_config(original)
                checkpoint_sha = hashlib.sha256(Path(cfg.checkpoint_path).read_bytes()).hexdigest()
                started = time.perf_counter()
                timing = {}
                item = {"round": round_id, "market": cfg.market,
                        "measurement_boundary": "current_session_receipt_only_no_network_no_publication",
                        "checkpoint_sha256": checkpoint_sha,
                        "live_opening_slo_claim": False}
                try:
                    result = signal_engine.generate_live_signal(**cfg.signal_kwargs(
                        price_source="tw", write=False, publish_latest=False,
                        ensure_previous_signal=False, previous_signal_backfill_limit=0,
                        defer_rich_artifacts=True, _timing_sink=timing))
                    item.update(status="ready", summary=result.summary,
                                missing_model_targets=[
                                    {k: r.get(k) for k in ("symbol", "model_weight", "target_weight", "open_price", "opening_price_wait_reason")}
                                    for r in result.weights_rows if r.get("opening_price_wait_reason")])
                except Exception as exc:
                    item.update(status="failed", error_type=type(exc).__name__, error=str(exc), timing=timing)
                item["elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
                report["receipt_only_model_probes"].append(item)
                _atomic_json(args.output, report)
                print(json.dumps({k: v for k, v in item.items() if k not in {"summary", "missing_model_targets", "timing"}}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
