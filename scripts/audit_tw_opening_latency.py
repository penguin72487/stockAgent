#!/usr/bin/env python3
"""Retain real opening attempts and benchmark current models without publishing.

The optional inference probe is receipt-only and never writes a signal pointer,
ledger, or broker order. Its midday/hot-cache timing is NOT a 09:00 latency.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
import subprocess
from unittest.mock import patch
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json


def _atomic_json(path, payload):
    atomic_write_json(path, payload, durable=True)


def quote_session_phase(observed_at: datetime) -> str:
    """Taipei regular session; this is a clock label, not a trading-day proof."""
    if observed_at.tzinfo is None:
        raise ValueError("quote phase requires an aware clock")
    local = observed_at.astimezone(ZoneInfo("Asia/Taipei"))
    seconds = local.hour * 3600 + local.minute * 60 + local.second
    return ("preopen" if seconds < 9 * 3600 else "opening" if seconds < 9 * 3600 + 60
            else "intraday" if seconds < 13 * 3600 + 30 * 60 else "postclose")


def provider_phase_latency(events, symbols, session_start):
    """Per-request full-axis proof; never time a shrinking delta as a full sweep.

    Source monotonic spans and journal wall clocks are kept separate. Repeated
    prices need per-job coverage events, not deduplicated session observations.
    Old owners are explicitly unmeasured, not zero-latency successes.
    """
    requested_count = len(symbols)
    scope_hash = hashlib.sha256("\n".join(sorted(symbols)).encode()).hexdigest()
    ordered_hash = hashlib.sha256("\n".join(symbols).encode()).hexdigest()
    jobs = {}
    for elapsed, row in sorted(events, key=lambda item: item[0]):
        if not row.get("job_id"):
            continue
        key = (row["provider"], row.get("owner_pid"), row["job_id"])
        job = jobs.setdefault(key, {"provider": key[0], "owner_pid": key[1], "job_id": key[2],
            "started_after_open_ms": None, "phase": "unverified", "state": "running",
            "full_scope_verified": False, "requested_count": None,
            "first_valid_open_ms": None, "positive_coverage_ms": {}, "resolved_coverage_ms": {},
            "source_total_ms": None, "source_and_persistence_ms": None,
            "coverage_evidence": "missing_per_job_coverage_not_zero"})
        if row["event"] == "started":
            job.update(started_after_open_ms=round(elapsed, 3),
                phase=quote_session_phase(session_start + timedelta(milliseconds=elapsed)),
                requested_count=row.get("requested_count"),
                worker_queue_ms=row.get("worker_queue_ms"),
                full_scope_verified=(row.get("requested_count") == requested_count and (
                    row.get("requested_scope_sha256") == scope_hash or
                    row.get("requested_symbols_sha256") == ordered_hash)))
        elif row["event"] == "coverage_progress":
            count = row.get("requested_count")
            positive, no_print, responses = (row.get(field) for field in (
                "valid_open_count", "observed_no_print_count", "response_count"))
            duration = row.get("elapsed_ms")
            if (not isinstance(count, int) or count != job["requested_count"]
                    or any(not isinstance(value, int) or not 0 <= value <= count
                           for value in (positive, no_print, responses))
                    or positive + no_print > responses
                    or not isinstance(duration, (int, float)) or not math.isfinite(duration) or duration < 0
                    or row.get("measurement_clock") != "provider_job_perf_counter_v1"):
                job["coverage_evidence"] = "invalid_per_job_coverage"
                continue
            job.update(coverage_evidence="per_job_validated_counts_monotonic_v1",
                valid_open_count=positive, observed_no_print_count=no_print,
                response_count=responses, unknown_count=count - positive - no_print)
            if positive and job["first_valid_open_ms"] is None:
                job["first_valid_open_ms"] = duration
            for fraction in (.5, .9, .95, .99, 1.):
                if positive >= math.ceil(count * fraction):
                    job["positive_coverage_ms"].setdefault(str(fraction), duration)
                if positive + no_print >= math.ceil(count * fraction):
                    job["resolved_coverage_ms"].setdefault(str(fraction), duration)
        elif row["event"] == "finished":
            job.update(state=row.get("state", "unverified"), source_total_ms=row.get("last_total_ms"),
                       source_and_persistence_ms=row.get("source_and_persistence_ms"),
                       error_type=row.get("error_type"), http_status=row.get("http_status"))
            # Legacy counts are useful final observations, not first/full clocks.
            job.setdefault("valid_open_count", row.get("valid_open_count"))
    groups = {}
    for job in jobs.values():
        group = groups.setdefault(job["phase"], {}).setdefault(job["provider"], {
            "job_count": 0, "failed_or_degraded_jobs": 0, "full_scope_jobs": 0,
            "full_positive_observed_jobs": 0, "full_resolved_observed_jobs": 0,
            "source_duration_samples_ms": [], "jobs": []})
        group["job_count"] += 1
        group["failed_or_degraded_jobs"] += job["state"] in {"failed", "degraded"}
        group["full_scope_jobs"] += job["full_scope_verified"]
        group["full_positive_observed_jobs"] += job["full_scope_verified"] and "1.0" in job["positive_coverage_ms"]
        group["full_resolved_observed_jobs"] += job["full_scope_verified"] and "1.0" in job["resolved_coverage_ms"]
        duration = job["source_total_ms"]
        if isinstance(duration, (int, float)) and math.isfinite(duration) and duration >= 0:
            group["source_duration_samples_ms"].append(duration)  # include failures/timeouts
        group["jobs"].append(job)
    for providers in groups.values():
        for group in providers.values():
            durations = sorted(group.pop("source_duration_samples_ms"))
            group["source_duration_percentiles_ms"] = {str(p): durations[max(0, math.ceil(len(durations) * p) - 1)]
                if durations else None for p in (.5, .95, .99)}
            group["duration_sample_count_including_failures"] = len(durations)
    return {"contract": "quote_phase_full_scope_per_job_latency_v1", "requested_count": requested_count,
        "requested_scope_sha256": scope_hash, "phases": groups,
        "boundary": "validated_opening_fields_not_fresh_executable_book_or_broker_fill",
        "full_positive_requires_every_symbol_to_have_a_genuine_open": True}


def _provider_latency_summary(events, requested, start):
    """First valid and full requested scope are different, source-local clocks.

    Old model-gap events are only lower bounds on a vendor's full response.
    Never credit another vendor's prices to FinMind or label missing rows as
    untraded. Retain failures and absent/old/trial/invalid-price reasons.
    """
    providers = {}
    for elapsed, row in sorted(events, key=lambda item: item[0]):
        name = row["provider"]
        p = providers.setdefault(name, {"valid": set(), "no_print": set(), "first_request_ms": None,
            "first_valid_open_ms": None, "first_full_validated_payload_ms": None,
            "first_retained_payload_received_ms": None, "first_retained_validated_open_ms": None,
            "response_job_count": 0, "request_job_count": 0, "failed_jobs": 0,
            "full_scope_snapshot_count": 0, "retained_http_request_count": 0,
            "snapshot_ids": set(),
            "coverage_milestones": [], "jobs_first_30s": [],
            "thresholds": {fraction: None for fraction in (.5, .9, .95, .99, 1.)}})
        event = row["event"]
        if event == "started":
            p["request_job_count"] += 1
            if p["first_request_ms"] is None:
                p["first_request_ms"] = round(elapsed, 3)
        elif event == "finished":
            p["response_job_count"] += 1
            p["failed_jobs"] += row.get("state") in {"failed", "degraded"}
            if (row.get("transport_timing", {}).get("http_request_to_failure_ms") is not None
                    and not row["transport_timing"].get("provider_failure_reused")):
                p["retained_http_request_count"] += 1
            if elapsed <= 30000:
                p["jobs_first_30s"].append({"finished_after_open_ms": round(elapsed, 3),
                    **{key: row.get(key) for key in ("job_id", "owner_pid", "requested_count",
                        "state", "valid_open_count", "worker_queue_ms", "last_total_ms")},
                    "transport_timing": row.get("transport_timing", {})})
        before = len(p["valid"])
        if event == "observations_delta":
            for obs in row.get("observations", []):
                symbol, state = obs["symbol"], obs["state"]
                if symbol not in requested:
                    continue
                if state == "valid_open":
                    p["valid"].add(symbol)
                    p["no_print"].discard(symbol)
                elif state == "observed_no_first_print" and symbol not in p["valid"]:
                    p["no_print"].add(symbol)
        elif event == "provider_snapshot":
            coverage = row["coverage"]
            if coverage.get("contract") != "finmind_unfiltered_market_snapshot_coverage_v1":
                continue
            if coverage["snapshot_id"] in p["snapshot_ids"]:
                continue
            p["snapshot_ids"].add(coverage["snapshot_id"])
            valid = requested.intersection(coverage["valid_open_symbols"])
            p["full_scope_snapshot_count"] += 1
            p["retained_http_request_count"] += 1
            received_ms = round((datetime.fromisoformat(coverage["received_at_taipei"]) - start).total_seconds() * 1000, 3)
            if p["first_retained_payload_received_ms"] is None:
                p["first_retained_payload_received_ms"] = received_ms
            p["valid"].update(valid)
            p["no_print"].difference_update(valid)
            rejected = {reason: sorted(requested.intersection(symbols) - valid)
                        for reason, symbols in coverage["rejected_symbols"].items()}
            returned = valid.union(*(set(symbols) for symbols in rejected.values()))
            rejected["absent_from_payload"] = sorted(requested - returned)
            p["latest_full_scope_snapshot"] = {
                "snapshot_id": coverage["snapshot_id"], "observed_after_open_ms": round(elapsed, 3),
                "received_at_taipei": coverage["received_at_taipei"],
                "validated_at_taipei": coverage["validated_at_taipei"],
                "valid_open_count": len(valid), "missing_count": len(requested - valid),
                "rejected_counts": {reason: len(symbols) for reason, symbols in rejected.items()},
                "missing_symbols_by_reason": rejected}
            # Retain the original provider parse clock separately from journal
            # observation/durability. A source clock is NOT a fill clock.
            validated = datetime.fromisoformat(coverage["validated_at_taipei"])
            if validated.tzinfo is not None and validated >= start:
                validated_ms = round((validated - start).total_seconds() * 1000, 3)
                if valid and p["first_retained_validated_open_ms"] is None:
                    p["first_retained_validated_open_ms"] = validated_ms
                if len(valid) == len(requested) and p["first_full_validated_payload_ms"] is None:
                    p["first_full_validated_payload_ms"] = validated_ms
        count = len(p["valid"])
        if count > before:
            p["coverage_milestones"].append({"observed_after_open_ms": round(elapsed, 3), "valid_open_count": count})
        if count and p["first_valid_open_ms"] is None:
            p["first_valid_open_ms"] = round(elapsed, 3)
        for fraction, at in p["thresholds"].items():
            if at is None and count >= math.ceil(len(requested) * fraction):
                p["thresholds"][fraction] = round(elapsed, 3)
    for p in providers.values():
        del p["snapshot_ids"]
        p["valid_open_count"] = len(p.pop("valid"))
        p["observed_no_print_count"] = len(p.pop("no_print"))
        p["unknown_count"] = len(requested) - p["valid_open_count"] - p["observed_no_print_count"]
        p["scope_evidence"] = "full_market_payload" if p["full_scope_snapshot_count"] else "consumer_deltas_only_lower_bound"
        p["first_full_positive_scope_observed_ms"] = p["thresholds"][1.]
        p["thresholds"] = [{"coverage_fraction": fraction, "required_count": math.ceil(len(requested) * fraction),
            "first_observed_ms": at} for fraction, at in p["thresholds"].items()]
    return {"contract": "source_local_opening_first_valid_and_full_scope_latency_v1",
        "requested_count": len(requested), "full_scope_requires_positive_same_session_opens": True,
        "boundary": "validated_source_observations_not_model_quorum_durable_publication_or_fills",
        "providers": providers}


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
    cutoffs = (100, 250, 500, 1000, 2000, 5000, 10000, 11000, 20000, 30000)
    states = {cutoff: {} for cutoff in cutoffs}
    failures, malformed, known_providers, legacy_events, provider_events = {}, 0, set(), [], []
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
                if provider != "merged" and elapsed >= 0 and row.get("event") in {
                        "started", "finished", "observations_delta", "provider_snapshot", "coverage_progress"}:
                    compact = {key: row.get(key) for key in ("provider", "event", "job_id", "owner_pid",
                        "requested_count", "state", "valid_open_count", "worker_queue_ms", "last_total_ms",
                        "requested_symbols_sha256", "requested_scope_sha256", "elapsed_ms", "measurement_clock",
                        "observed_no_print_count", "response_count", "source_and_persistence_ms", "error_type", "http_status")}
                    if row["event"] == "observations_delta":
                        compact["observations"] = [{"symbol": obs["symbol"], "state": obs["state"]}
                            for obs in row.get("observations", [])]
                    elif row["event"] == "provider_snapshot":
                        coverage = row["coverage"]
                        if (not isinstance(coverage, dict)
                                or coverage.get("contract") != "finmind_unfiltered_market_snapshot_coverage_v1"
                                or not isinstance(coverage.get("valid_open_symbols"), list)
                                or not isinstance(coverage.get("rejected_symbols"), dict)):
                            raise ValueError("invalid full-market source coverage")
                        lists = [coverage["valid_open_symbols"], *coverage["rejected_symbols"].values()]
                        if (not isinstance(coverage.get("snapshot_id"), str) or not coverage["snapshot_id"]
                                or any(not isinstance(items, list) or any(not isinstance(symbol, str) for symbol in items)
                                       for items in lists)):
                            raise ValueError("invalid full-market source symbols/identity")
                        receipt_clocks = []
                        for field in ("received_at_taipei", "validated_at_taipei"):
                            stamp = datetime.fromisoformat(coverage[field])
                            if stamp.tzinfo is None or not start <= stamp <= at:
                                raise ValueError("invalid full-market source receipt clock")
                            receipt_clocks.append(stamp)
                        if receipt_clocks[0] > receipt_clocks[1]:
                            raise ValueError("full-market validation predates receipt")
                        compact["coverage"] = coverage
                    transport = row.get("transport_timing") or {}
                    if not isinstance(transport, dict):
                        raise ValueError("invalid provider transport timings")
                    compact["transport_timing"] = {key: value for key, value in transport.items()
                        if key.startswith(("http_", "json_", "full_market_", "provider_response_", "provider_owner_"))
                        or key in {"body_bytes", "provider_payload_rows", "mis_failed_request_count", "provider_failure_reused",
                            "mis_failure_phase_counts", "mis_dispatch_stopped", "mis_dispatch_stop_reason", "mis_dispatch_policy",
                            "mis_deferred_channel_count", "mis_request_count", "mis_http_workers",
                            "mis_http_error_statuses", "mis_error_chain_types"}}
                    provider_events.append((elapsed, compact))
                if provider != "merged" and row.get("event") in {"started", "finished", "observations_delta", "provider_snapshot"}:
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
                if row.get("event") not in {"observations_delta", "provider_snapshot"} or elapsed < 0:
                    continue
                observations = (row.get("observations", []) if row["event"] == "observations_delta" else
                    [{"symbol": symbol, "state": "valid_open"} for symbol in row["coverage"]["valid_open_symbols"]])
                for cutoff in cutoffs:
                    if elapsed > cutoff:
                        continue
                    source_states = states[cutoff].setdefault(provider, {"valid": set(), "no_print": set()})
                    for observation in observations:
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
        "failed_or_degraded_jobs": failures, "milestones": milestones,
        "provider_latency": _provider_latency_summary(provider_events, requested, start),
        "provider_phase_latency": provider_phase_latency(provider_events, symbols, start)}


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
              for target in (30000, 20000, 11000, 10000, 5000, 1000)]
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


def read_attempts(path: Path, session_date: str) -> tuple[dict, int]:
    """Stream retained native facts; missing/corrupt evidence never means fast."""
    attempts, malformed = {}, 0
    try:
        with path.open() as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError("not an attempt")
                    if row.get("session_date") != session_date:
                        continue
                    market = row["market"]
                    stats = attempts.setdefault(market, {"attempt_count": 0, "failed_count": 0,
                        "first": row, "last": row, "ready": []})
                    stats["attempt_count"] += 1
                    stats["failed_count"] += int(row["status"] == "failed")
                    stats["last"] = row
                    if row["status"] == "ready":
                        stats["ready"].append(row)
                except (KeyError, TypeError, ValueError):
                    malformed += 1
    except FileNotFoundError:
        return {}, 0
    return attempts, malformed


def deadline_assessment(report: dict, target_ms: int) -> dict:
    """Evaluate real event clocks AS OF the deadline, never replay event time."""
    from stockagent.live.opening_execution_latency import opening_execution_slo

    gate = datetime.fromisoformat(report["session_date"] + "T09:00:00+08:00")
    execution = opening_execution_slo(report["session_date"], report["opening_publication_slo"],
        report["engine_modes"], report["execution_samples"],
        observed_at=gate + timedelta(milliseconds=target_ms))
    publication = report["opening_publication_slo"]
    pub_pass = next(row["passed"] for row in publication["stages"] if row["target_ms"] == target_ms)
    fill_pass = next(row["passed"] for row in execution["stages"] if row["target_ms"] == target_ms)
    trustworthy = not report["evidence_errors"] and report["scope_matches_preopen"]
    source = next((row for row in report.get("source_coverage_milestones", {}).get("milestones", [])
                   if row["after_open_ms"] == target_ms), None)
    return {"contract": "daily_opening_original_clock_deadline_v1",
        "session_date": report["session_date"], "target_ms": target_ms,
        "observed_at": report["observed_at"], "expected_markets": report["expected_markets"],
        "scope_matches_preopen": report["scope_matches_preopen"],
        "passed": bool(trustworthy and pub_pass and fill_pass),
        "state": "passed" if trustworthy and pub_pass and fill_pass else "missed_or_unverified",
        "evidence_errors": report["evidence_errors"],
        "publication": publication, "execution": execution, "source_at_deadline": source,
        "simulation_only": True, "broker_fill_claim": False,
        "measurement_boundary": "native_durable_publication_and_full_paper_targets_not_replay_or_hot_probe"}


def daily_audit(*, session_date: str, state_dir: Path, markets_dir: Path, output_root: Path,
                observed_at: datetime | None = None) -> dict:
    """Independent read-only observer; no Discord/Torch/provider initialization.

    Preopen freezes the current configured mode roster. Later observations
    cannot silently reduce it, erase a missed deadline, or turn late repair
    into live success. The timer also runs after close and after a late boot.
    """
    from stockagent.live.market_config import load_market_configs
    from stockagent.live.market_status import verified_tw_stock_session_day

    now = observed_at or datetime.now(ZoneInfo("Asia/Taipei"))
    if now.tzinfo is None or now.astimezone(ZoneInfo("Asia/Taipei")).date().isoformat() != session_date:
        raise ValueError("daily observer requires the actual current Taipei date/clock")
    root = output_root / session_date
    root.mkdir(parents=True, exist_ok=True)
    configs = load_market_configs(markets_dir)
    overrides_path = ROOT / "artifacts/discord_bot/state.json"
    errors = []
    try:
        overrides = json.loads(overrides_path.read_text())["markets"] if overrides_path.exists() else {}
        if not isinstance(overrides, dict):
            raise ValueError("invalid market overrides")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        overrides = {}
        errors.append("market_overrides_" + type(exc).__name__)
    for name in configs:
        if name in overrides and not isinstance(overrides[name], dict):
            errors.append("invalid_market_override:" + name)
            overrides[name] = {}
    enabled = sorted(name for name, cfg in configs.items() if cfg.day_trade_simulation_enabled
        and overrides.get(name, {}).get("enabled", cfg.enabled))
    if not enabled:
        raise ValueError("daily observer requires a nonempty configured mode roster")
    calendars = {name: verified_tw_stock_session_day(now.date(), configs[name].holidays,
        parquet_root=Path(configs[name].day_trade_rule_data_dir or "data_tw_public")) for name in enabled}
    gate = datetime.fromisoformat(session_date + "T09:00:00+08:00")
    scope = {"contract": "daily_opening_configured_mode_scope_v1", "session_date": session_date,
        "observed_at": now.isoformat(), "markets": enabled, "captured_before_open": now < gate}
    scope_path = root / "scope.json"
    if not scope_path.exists():
        _atomic_json(scope_path, scope)
    try:
        frozen_scope = json.loads(scope_path.read_text())
        markets = frozen_scope["markets"]
        if (not isinstance(markets, list) or not markets
                or any(not isinstance(name, str) or not name for name in markets)
                or len(set(markets)) != len(markets)):
            raise ValueError("invalid frozen mode roster")
    except (OSError, KeyError, TypeError, ValueError) as exc:
        # Preserve the corrupt original for diagnosis. This observation uses
        # the full CURRENT scope but cannot acquire a preopen attestation.
        errors.append("preopen_scope_" + type(exc).__name__)
        frozen_scope = {"markets": enabled, "captured_before_open": False}
    expected = frozen_scope["markets"]
    try:
        captured = datetime.fromisoformat(frozen_scope["observed_at"])
        preopen_clock_valid = (captured.tzinfo is not None and captured < gate
            and captured.astimezone(ZoneInfo("Asia/Taipei")).date().isoformat() == session_date)
    except (KeyError, TypeError, ValueError):
        preopen_clock_valid = False
    report = {"schema_version": 1, "session_date": session_date, "observed_at": now.isoformat(),
        "contract": "daily_native_opening_latency_audit_v1", "expected_markets": expected,
        "scope_matches_preopen": bool(frozen_scope.get("captured_before_open") is True and preopen_clock_valid
            and frozen_scope.get("contract") == scope["contract"]
            and frozen_scope.get("session_date") == session_date and expected == enabled),
        "scope_receipt": str(scope_path), "calendars": calendars, "evidence_errors": errors,
        "simulation_only": True, "provider_requests": 0, "signal_published": False,
        "ledger_modified": False, "service_restarts": False, "broker_fill_claim": False}
    # Keep the real owner's preparation evidence before the next day/restart
    # overwrites its latest status. This is context, not a substitute for the
    # original publication/fill clocks or an additional historical SLO gate.
    try:
        owner = json.loads((ROOT / "artifacts/discord_bot/service_status.json").read_text())
        updated = datetime.fromisoformat(owner["updated_at"])
        if updated.tzinfo is None:
            raise ValueError("owner clock has no timezone")
        warm = owner.get("startup_inference_warmup") or {}
        owner_read_at = datetime.now(ZoneInfo("Asia/Taipei")) if observed_at is None else now
        report["owner_context"] = {"state": "observed", "updated_at": owner["updated_at"],
            "observed_at": owner_read_at.isoformat(),
            "age_seconds": round((owner_read_at - updated).total_seconds(), 3), "run_id": owner["run_id"],
            "discord_connected": owner.get("discord_connected"), "core_health": owner.get("core_health"),
            "live_implementation_admission": owner.get("live_implementation_admission"),
            "startup_inference_warmup": warm,
            "same_owner_resident_warmup_declared": bool(warm.get("run_id") == owner["run_id"]
                and warm.get("status") == "ready" and warm.get("model_cache_resident_ready") is True
                and warm.get("ready_count") == warm.get("market_count") == len(expected)
                and {row["market"] for row in warm.get("markets", []) if row.get("status") == "ready"} == set(expected))}
    except (OSError, KeyError, TypeError, ValueError, AttributeError) as exc:
        report["owner_context"] = {"state": "unavailable", "error_type": type(exc).__name__}
    # A closed calendar is not a failed trading-day opening. Unknown evidence
    # is still visible and may not suppress a future missed deadline.
    if not all(row[0] for row in calendars.values()):
        report["state"] = "calendar_closed_or_unverified"
    elif now < gate:
        report["state"] = "armed" if report["scope_matches_preopen"] and not errors else "scope_unverified"
    else:
        attempts, malformed = read_attempts(state_dir / "opening_signal_latency.jsonl", session_date)
        if malformed:
            errors.append(f"malformed_attempt_rows:{malformed}")
        report["opening_publication_slo"] = opening_publication_slo(session_date, attempts, expected)
        try:
            native = json.loads((state_dir / "status.json").read_text())
            report["engine_modes"] = native["modes"]
            if not isinstance(report["engine_modes"], dict):
                raise ValueError("invalid engine modes")
            report["execution_samples"] = []
            with (state_dir / "execution_latency.jsonl").open() as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                        if isinstance(row, dict) and row.get("session_date") == session_date:
                            report["execution_samples"].append(row)
                    except ValueError:
                        errors.append("malformed_execution_row")
        except (OSError, KeyError, TypeError, ValueError) as exc:
            errors.append("engine_evidence_" + type(exc).__name__)
            report.setdefault("engine_modes", {})
            report.setdefault("execution_samples", [])
        snapshot_root = Path(os.getenv("STOCKAGENT_TW_OPENING_SNAPSHOT_ROOT") or "artifacts/live/tw_opening_snapshots")
        receipt_path = snapshot_root / (session_date + ".json")
        try:
            receipt_bytes = receipt_path.read_bytes()
            receipt = json.loads(receipt_bytes)
            report["source_coverage_milestones"] = opening_coverage_milestones(
                receipt_path.with_suffix(".acquisition.jsonl"), session_date, receipt["requested_symbols"])
            report["source_coverage_milestones"]["universe_receipt_sha256"] = hashlib.sha256(receipt_bytes).hexdigest()
        except (OSError, KeyError, TypeError, ValueError) as exc:
            report["source_coverage_milestones"] = {"state": "unavailable", "error_type": type(exc).__name__}
        report["mode_timings"] = {market: {"attempt_count": stats["attempt_count"],
            "failed_count": stats["failed_count"],
            "first_failure": {key: stats["first"].get(key) for key in ("error_type", "failure_stage", "recorded_at")},
            "ready": [{key: row.get(key) for key in ("run_id", "signal_id", "signal_ready_at", "artifact_published_at",
                "stages", "input_preparation_stages_ms", "price_receipt_timing")} for row in stats["ready"]]}
            for market, stats in attempts.items()}
        report["deadlines"] = [deadline_assessment(report, target)
            for target in (1000, 5000, 10000, 11000, 20000, 30000)
            if now >= gate + timedelta(milliseconds=target)]
        for deadline in report["deadlines"]:
            path = root / f"first_assessment_{deadline['target_ms']}ms.json"
            if not path.exists():
                _atomic_json(path, deadline)
        report["state"] = next((row["state"] for row in report["deadlines"] if row["target_ms"] == 11000), "observing")
        # Progress rows stay in the existing executor journal, not duplicated
        # into every durable observer snapshot.
        del report["execution_samples"], report["engine_modes"]
    _atomic_json(root / "latest.json", report)
    with (output_root / "daily.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({key: report[key] for key in ("session_date", "observed_at", "state", "expected_markets")}) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", default=datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat())
    parser.add_argument("--output", type=Path)
    parser.add_argument("--daily", action="store_true", help="Independent native journal observer: no Discord/Torch, inference, providers, orders or restarts")
    parser.add_argument("--infer", action="store_true")
    parser.add_argument("--runtime", action="store_true", help="Attach current owners, Gateway, execution and source-repair receipts; no mutations")
    parser.add_argument("--ledger-fingerprints", action="store_true", help="Explicit deep reload audit with --runtime: stream-hash large immutable ledgers; normal health reads do not scan them")
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()
    if args.daily:
        if args.infer or args.runtime or args.ledger_fingerprints:
            parser.error("daily observer is strictly journal-only")
        from dotenv import load_dotenv
        load_dotenv(ROOT / "services/discord_bot/.env", override=False)
        state_dir = Path(os.getenv("TW_DAY_TRADE_STATE_DIR") or "artifacts/live/tw_day_trade_simulation")
        output_root = args.output or state_dir / "opening_audits"
        output_root.mkdir(parents=True, exist_ok=True)
        with (output_root / ".observer.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            report = daily_audit(session_date=args.session_date, state_dir=state_dir,
                markets_dir=Path(os.getenv("STOCKAGENT_MARKETS_DIR") or "services/discord_bot/markets"), output_root=output_root)
        print(json.dumps({key: report[key] for key in ("session_date", "state", "expected_markets", "provider_requests", "ledger_modified")}))
        return 0
    if args.output is None:
        parser.error("--output is required outside --daily")
    from services.discord_bot import bot
    from stockagent.live import signal_engine
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
