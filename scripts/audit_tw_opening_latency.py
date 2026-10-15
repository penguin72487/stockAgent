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
    cutoffs = (100, 250, 500, 1000, 2000, 5000, 10000, 20000, 30000)
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


def opening_publication_slo(session_date: str, attempts: dict, expected_markets: list[str]) -> dict:
    """Opening gate to ALL immutable execution pointers, not hot inference.

    Recompute from original timezone-aware clocks. A missing mode or an invalid
    clock cannot earn a stage, and repeat publication cannot move the first
    accepted pointer. The caller supplies a labelled mode roster, not a claim
    that today's config proves a historical deployment.
    """
    if not expected_markets or len(set(expected_markets)) != len(expected_markets):
        raise ValueError("opening publication SLO requires a nonempty unique mode roster")
    gate = datetime.fromisoformat(session_date + "T09:00:00+08:00")
    modes = {}
    for market in expected_markets:
        stats = attempts.get(market, {})
        accepted, invalid = [], 0
        for row in stats.get("ready", []):
            try:
                if (row.get("status") != "ready" or row.get("session_date") != session_date
                        or row.get("market") != market or not row.get("signal_id")):
                    raise ValueError("wrong publication identity")
                original_gate = datetime.fromisoformat(row["gate_at"])
                ready = datetime.fromisoformat(row["signal_ready_at"])
                published = datetime.fromisoformat(row["artifact_published_at"])
                if any(at.tzinfo is None for at in (original_gate, ready, published)):
                    raise ValueError("missing clock timezone")
                if original_gate != gate or not gate <= ready <= published:
                    raise ValueError("invalid publication clock order")
                accepted.append((published, row))
            except (KeyError, TypeError, ValueError):
                invalid += 1
        accepted.sort(key=lambda item: item[0])
        published, row = accepted[0] if accepted else (None, {})
        modes[market] = {
            "state": "published" if published else "unavailable",
            "attempt_count": stats.get("attempt_count", 0),
            "failed_count": stats.get("failed_count", 0),
            "invalid_ready_records": invalid,
            "signal_id": row.get("signal_id"),
            "artifact_published_at": row.get("artifact_published_at"),
            "published_from_open_ms": round((published - gate).total_seconds() * 1000, 3) if published else None,
        }
    latencies = [row["published_from_open_ms"] for row in modes.values() if row["state"] == "published"]
    complete = len(latencies) == len(expected_markets)
    all_published_ms = max(latencies) if complete else None
    stages = [{"target_ms": target, "passed": complete and all_published_ms <= target,
               "state": "incomplete" if not complete else "passed" if all_published_ms <= target else "missed"}
              for target in (30000, 20000, 10000, 5000, 1000)]
    return {
        "contract": "all_enabled_mode_immutable_publication_stages_v1",
        "measurement_boundary": "09:00_gate_to_all_mode_durable_execution_pointers_not_broker_fills",
        "scope_basis": "caller_supplied_mode_roster_not_historical_deployment_proof",
        "state": "observed" if complete else "incomplete",
        "expected_mode_count": len(expected_markets), "published_mode_count": len(latencies),
        "first_mode_published_from_open_ms": min(latencies) if latencies else None,
        "all_modes_published_from_open_ms": all_published_ms,
        "stages": stages, "modes": modes,
    }


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
    report["code_sha256"] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (Path(__file__), ROOT / "services/discord_bot/bot.py", ROOT / "stockagent/live/quote_provider.py",
                     ROOT / "stockagent/live/signal_engine.py", ROOT / "stockagent/live/opening_price_acquisition.py",
                     ROOT / "stockagent/live/tw_day_trade_simulation.py", ROOT / "stockagent/live/opening_execution_latency.py",
                     ROOT / "scripts/run_tw_day_trade_simulation.py")}
    report["code_identity_scope"] = "audit_current_disk_not_loaded_owner_attestation"
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
    expected_markets = sorted(cfg.market for cfg in bot._market_configs().values()
        if cfg.day_trade_simulation_enabled and bot._market_enabled(cfg))
    report["opening_publication_slo"] = opening_publication_slo(
        args.session_date, report["real_opening_attempts"], expected_markets,
    )
    report["opening_publication_slo"]["scope_basis"] = "current_enabled_mode_roster_not_historical_deployment_proof"
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
        # Reuse the dashboard's canonical executor-clock projection. A replay
        # fill's 09:01 economic timestamp is never a real registration clock.
        from stockagent.live.tw_day_trade_dashboard import _tail, _opening_signal_latency_summary
        execution_summary = _opening_signal_latency_summary(
            [*(row for stats in report["real_opening_attempts"].values() for row in stats["ready"]),
             *_tail(bot._day_trade_state_dir() / "latency.jsonl", 2000)],
            expected_markets=expected_markets, session_date=args.session_date,
        )
        runtime["paper_ledger_registration"] = {
            "measurement_boundary": "09:00_gate_to_real_paper_ledger_registration_not_full_target_fill_or_broker_fill",
            "read_scope": "canonical_executor_projection_last_2000_retained_rows",
            **{key: execution_summary.get(key) for key in (
                "session_date", "ledger_complete", "ledger_observed_mode_count",
                "ledger_missing_markets", "first_ledger_ms", "final_ledger_ms",
            )},
            "modes": [{key: row.get(key) for key in (
                "market", "signal_id", "consumer_detected_at", "ledger_persisted_at",
                "ledger_from_0900_ms", "ready_to_ledger_ms", "stages", "bottleneck_stage",
            )} for row in execution_summary.get("modes", ())],
        }
        from stockagent.live.opening_execution_latency import opening_execution_slo
        execution_path = bot._day_trade_state_dir() / "execution_latency.jsonl"
        report["opening_execution_slo"] = opening_execution_slo(
            args.session_date, report["opening_publication_slo"],
            runtime.get("engine", {}).get("modes") or {}, _tail(execution_path, 5000),
            observed_at=datetime.now(ZoneInfo("Asia/Taipei")),
        )
        report["opening_execution_slo"].update(
            source_path=str(execution_path), read_scope="last_5000_progress_rows_missing_clocks_never_assumed_complete",
            scope_basis="current_enabled_mode_roster_not_historical_deployment_proof",
        )
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
