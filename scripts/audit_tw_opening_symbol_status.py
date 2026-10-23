#!/usr/bin/env python3
"""Explain missing opens against verified official membership, without orders.

Optional MIS refresh uses the canonical quote adapter on unresolved symbols
only. Results stay in this private diagnostic receipt, never the live ledger.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
from zoneinfo import ZoneInfo

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.data.tw_current_trading_universe import load_current_universe
from stockagent.live import quote_provider as quotes
from stockagent.live.opening_price_journal import journal_absence_proof_ms, observation_values
from stockagent.private_json import projected_object_values

TAIPEI = ZoneInfo("Asia/Taipei")


def journal_symbol_timeline(path: Path, payload: dict, universe: dict, *,
                            cutoffs=(1000, 5000, 10000, 11000, 20000, 30000),
                            project_headers=True, include_reference_rows=False) -> dict:
    """One read-only pass: every official symbol, including later recoveries.

    Full-market source rejection lists have exact attribution. A failed job
    with only a scope digest is attributed to individual symbols ONLY if its
    digest matches this entire scope. Unknown partial scopes remain job-level.
    Old delta rows without the original absence-proof clock cannot establish
    a fresh no-first-print reference, even if their delivery was recent.
    """
    if (payload["session_date"] != universe["session_date"]
            or payload.get("official_trading_universe", {}).get("fingerprint") != universe["fingerprint"]):
        raise ValueError("journal requires the receipt's verified official generation")
    symbols = payload["requested_symbols"]
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("journal requires a nonempty unique scope")
    cutoffs = tuple(sorted(set(cutoffs)))
    if not cutoffs or any(type(value) is not int or value < 0 for value in cutoffs):
        raise ValueError("invalid journal deadlines")
    scope = set(symbols)
    gate = datetime.fromisoformat(payload["session_date"] + "T09:00:00+08:00")
    gate_ms = int(gate.timestamp() * 1000)
    scope_hash = hashlib.sha256("\n".join(sorted(symbols)).encode()).hexdigest()
    ordered_hash = hashlib.sha256("\n".join(symbols).encode()).hexdigest()
    registry = {row["symbol"]: row for row in universe["records"]}
    first = {symbol: {} for symbol in symbols}
    first_rows = {}
    latest = {cutoff: {} for cutoff in cutoffs}
    starts, failures, malformed, line_count, legacy_absence = {}, [], [], 0, 0
    started = time.perf_counter()

    def observe(provider, job, elapsed, symbol, state, proof=None, details=None):
        nonlocal legacy_absence
        if symbol not in scope:
            return
        if not isinstance(state, str) or not state:
            raise ValueError("invalid observation state")
        if state == "valid_open":
            previous = first[symbol].get(provider)
            if previous is None or elapsed < previous:
                first[symbol][provider] = elapsed
            if (include_reference_rows and details is not None
                    and isinstance(details.get("open_price"), (int, float))
                    and np.isfinite(details["open_price"]) and details["open_price"] > 0
                    and (symbol not in first_rows or elapsed < first_rows[symbol][0])):
                first_rows[symbol] = (elapsed, details)
        if state == "observed_no_first_print" and proof is None:
            legacy_absence += 1
        for cutoff in cutoffs:
            if elapsed > cutoff:
                continue
            by_provider = latest[cutoff].setdefault(symbol, {})
            previous = by_provider.get(provider)
            if previous is None or elapsed >= previous["observed_after_open_ms"]:
                by_provider[provider] = {"state": state, "observed_after_open_ms": elapsed,
                    "proof_timestamp_ms": proof, "job_id": job,
                    **({"reference_row": details} if include_reference_rows else {})}

    before = path.stat()
    # A native writer may append during an audit. Pin this byte prefix and
    # never treat a truncated/uncommitted final line as a complete observation.
    digest, read_bytes = hashlib.sha256(), 0
    with path.open("rb") as handle:
        while read_bytes < before.st_size:
            raw = handle.readline(before.st_size - read_bytes)
            if not raw:
                break
            read_bytes += len(raw)
            digest.update(raw)
            line_count += 1
            try:
                if not raw.endswith(b"\n"):
                    raise ValueError("partial journal line")
                if project_headers:
                    header = projected_object_values(raw,
                        ("session_date", "provider", "observed_at_taipei", "event"))
                    if header is None:
                        raise ValueError("invalid event")
                    if header[0] != payload["session_date"]:
                        continue
                    _, provider, observed_at, event = header
                    row = None
                else:
                    row = json.loads(raw)
                    if not isinstance(row, dict):
                        raise ValueError("invalid event")
                    if row.get("session_date") != payload["session_date"]:
                        continue
                    provider, observed_at, event = (row.get(field) for field in
                        ("provider", "observed_at_taipei", "event"))
                at = datetime.fromisoformat(observed_at)
                if at.tzinfo is None:
                    raise ValueError("naive event clock")
                elapsed = round((at - gate).total_seconds() * 1000, 3)
                if not isinstance(provider, str) or not provider:
                    raise ValueError("invalid provider")
                if provider == "merged" or elapsed < 0:
                    continue
                if event not in {"started", "observations_delta", "provider_snapshot", "finished"}:
                    continue
                if row is None:
                    row = json.loads(raw)
                job = row.get("job_id")
                key = (provider, row.get("owner_pid"), job)
                if row.get("event") == "started":
                    starts[key] = {"after_open_ms": elapsed,
                        "full_scope": row.get("requested_count") == len(symbols) and (
                            row.get("requested_scope_sha256") == scope_hash
                            or row.get("requested_symbols_sha256") == ordered_hash)}
                elif row.get("event") == "observations_delta":
                    optional = ("no_first_print_observed_timestamp_ms", "source", "available",
                        "market_timestamp_ms", "received_timestamp_ms", "open_price", "simtrade")
                    observations = list(observation_values(row, ("symbol", "state"), optional_fields=optional))
                    # Commit a validated whole event, not a partially parsed
                    # prefix of a corrupt packed layout.
                    if any(not isinstance(symbol, str) or not symbol
                            or not isinstance(state, str) or not state for symbol, state, *_ in observations):
                        raise ValueError("invalid observation identity/state")
                    for symbol, state, proof, source, available, market, received, price, simtrade in observations:
                        # A legacy DIRECT MIS response's original receive clock
                        # is the canonical absence clock. Shared/merged rows can
                        # have newer delivery than proof and must not infer it.
                        proof = journal_absence_proof_ms(state=state, proof=proof, source=source,
                            available=available, market=market, received=received, price=price,
                            simtrade=simtrade, gate_ms=gate_ms, elapsed=elapsed)
                        details = {"source": source, "available": available, "market_timestamp_ms": market,
                            "received_timestamp_ms": received, "open_price": price, "simtrade": simtrade}
                        observe(provider, job, elapsed, symbol, state, proof, details)
                elif row.get("event") == "provider_snapshot":
                    coverage = row["coverage"]
                    if coverage.get("contract") != "finmind_unfiltered_market_snapshot_coverage_v1":
                        raise ValueError("unknown full-market coverage")
                    received = datetime.fromisoformat(coverage["received_at_taipei"])
                    validated = datetime.fromisoformat(coverage["validated_at_taipei"])
                    if (received.tzinfo is None or validated.tzinfo is None
                            or not gate <= received <= validated <= at):
                        raise ValueError("invalid full-market coverage clocks")
                    positive, rejected = coverage["valid_open_symbols"], coverage["rejected_symbols"]
                    if not isinstance(rejected, dict) or any(not isinstance(reason, str) or not reason for reason in rejected):
                        raise ValueError("invalid full-market rejection reasons")
                    lists = [positive, *rejected.values()]
                    if any(not isinstance(items, list) or any(not isinstance(s, str) for s in items) for items in lists):
                        raise ValueError("invalid full-market symbols")
                    if (sum(map(len, lists)) != len(set(s for items in lists for s in items))):
                        raise ValueError("conflicting full-market classifications")
                    for symbol in positive:
                        observe(provider, job, elapsed, symbol, "valid_open")
                    for reason, rejected_symbols in rejected.items():
                        for symbol in rejected_symbols:
                            observe(provider, job, elapsed, symbol, "rejected:" + reason)
                elif row.get("event") == "finished" and (
                        row.get("state") in {"failed", "degraded"}
                        or (row.get("transport_timing") or {}).get("mis_failed_request_count", 0)):
                    from scripts.audit_tw_opening_latency import _source_failure_reasons
                    failure = {"provider": provider, "job_id": job, "after_open_ms": elapsed,
                        "_job_identity": key,
                        "state": row.get("state"), "error_type": row.get("error_type"),
                        "http_status": row.get("http_status"),
                        "full_scope_verified": starts.get(key, {}).get("full_scope", False),
                        "reasons": _source_failure_reasons({**row,
                            "stage_timing": {"transport_timing": row.get("transport_timing") or {}}})}
                    failures.append(failure)
            except (KeyError, TypeError, ValueError, AttributeError):
                malformed.append(line_count)
    after = path.stat()
    stable_prefix = ((before.st_dev, before.st_ino) == (after.st_dev, after.st_ino)
        and read_bytes == before.st_size and after.st_size >= before.st_size)
    for failure in failures:
        failure["full_scope_verified"] = starts.get(failure.pop("_job_identity"), {}).get("full_scope", False)
    rows, counts = [], {cutoff: Counter() for cutoff in cutoffs}
    reference_rows = {str(cutoff): {} for cutoff in cutoffs} if include_reference_rows else None
    failure_counts = {cutoff: dict(Counter(reason for failure in failures
        if failure["after_open_ms"] <= cutoff for reason in failure["reasons"])) for cutoff in cutoffs}
    failed_providers = {cutoff: sorted({failure["provider"] for failure in failures
        if failure["full_scope_verified"] and failure["after_open_ms"] <= cutoff}) for cutoff in cutoffs}
    # A valid first print is an immutable session fact; later stale snapshots
    # cannot erase it. Failure details still remain visible beside that fact.
    for symbol in symbols:
        provider_first = first[symbol]
        first_ms = min(provider_first.values()) if provider_first else None
        deadlines = {}
        for cutoff in cutoffs:
            observed = latest[cutoff].get(symbol, {})
            states = {provider: value["state"] for provider, value in observed.items()}
            good = first_ms is not None and first_ms <= cutoff
            absence = [value for value in observed.values() if value["state"] == "observed_no_first_print"
                and type(value["proof_timestamp_ms"]) is int
                and gate_ms <= value["proof_timestamp_ms"] <= gate_ms + value["observed_after_open_ms"]
                and 0 <= gate_ms + cutoff - value["proof_timestamp_ms"] <= quotes._TW_OPENING_NO_PRINT_PROOF_TTL_SECONDS * 1000]
            no_print = bool(absence)
            state = "valid_open" if good else "observed_no_first_print" if no_print else "unknown"
            counts[cutoff][state] += 1
            deadlines[str(cutoff)] = {"state": state, "source_states": states,
                "full_scope_failed_providers": failed_providers[cutoff],
                "absence_clock_unverified": any(value == "observed_no_first_print" for value in states.values()) and not no_print}
            price_row = first_rows.get(symbol)
            if include_reference_rows and ((good and price_row is not None and price_row[0] <= cutoff) or (not good and absence)):
                chosen = None if good else max(absence, key=lambda value: value["proof_timestamp_ms"])
                reference_rows[str(cutoff)][symbol] = {"state": state,
                    **(price_row[1] if good else chosen["reference_row"] or {}),
                    "no_first_print_observed_timestamp_ms": None if good else chosen["proof_timestamp_ms"]}
        record = registry.get(symbol, {})
        rows.append({"symbol": symbol, "name": record.get("name"), "market": record.get("market"),
            "official_status": record.get("trading_status", "not_in_verified_current_registry"),
            "first_valid_open_after_open_ms": first_ms, "first_valid_by_provider_ms": provider_first,
            "deadlines": deadlines})
    return {"contract": "official_axis_opening_symbol_deadlines_v2", "session_date": payload["session_date"],
        "requested_count": len(symbols), "official_fingerprint": universe["fingerprint"],
        "official_observed_at": universe["observed_at_taipei"],
        "scope_basis": "receipt_official_axis_not_backdated_membership",
        "state": "observed" if stable_prefix and not malformed else "degraded",
        "journal_prefix": {"path": str(path), "bytes": read_bytes, "sha256": digest.hexdigest(),
            "same_inode_after_read": stable_prefix, "line_count": line_count, "malformed_lines": malformed},
        "milestones": [{"after_open_ms": cutoff, "counts": dict(counts[cutoff]),
            "input_reference_candidate_count": counts[cutoff]["valid_open"] + counts[cutoff]["observed_no_first_print"],
            "job_failure_reason_counts": failure_counts[cutoff]} for cutoff in cutoffs],
        **({"deadline_reference_rows": reference_rows} if include_reference_rows else {}),
        "legacy_absence_observations_without_proof_clock": legacy_absence,
        "failure_jobs": failures, "symbols": rows,
        "read_and_analysis_ms": round((time.perf_counter() - started) * 1000, 3),
        "boundary": "source_event_availability_not_input_fsync_or_execution_or_browser_paint",
        "failed_scope_boundary": "whole-scope job failure is not proof that each member timed out individually"}


def journal_reference_receipt(payload: dict, timeline: dict, after_open_ms: int) -> dict:
    """Private clock-pinned source projection, never a promoted opening receipt.

    Retained limits are engineering fixture metadata, not newly proved L1 depth.
    Price/absence values come ONLY from original events available by this clock.
    """
    if (timeline["state"] != "observed" or timeline["session_date"] != payload["session_date"]
            or timeline["official_fingerprint"] != payload["official_trading_universe"]["fingerprint"]):
        raise ValueError("reference projection requires a verified journal generation")
    from datetime import timedelta
    captured = datetime.fromisoformat(payload["session_date"] + "T09:00:00+08:00") + timedelta(milliseconds=after_open_ms)
    if datetime.fromisoformat(timeline["official_observed_at"]) > captured:
        raise ValueError("official universe was not yet observed at this deadline")
    rows = {}
    for symbol in payload["requested_symbols"]:
        source = timeline["deadline_reference_rows"][str(after_open_ms)].get(symbol, {})
        opening = source.get("open_price") if source.get("state") == "valid_open" else None
        valid = isinstance(opening, (int, float)) and np.isfinite(opening) and opening > 0
        absence = source.get("state") == "observed_no_first_print"
        prior = payload["rows"].get(symbol, {})
        rows[symbol] = {"source": source.get("source"), "price": opening if valid else None,
            "open_prices": opening if valid else None, "available": valid or absence,
            "timestamp_ms": source.get("received_timestamp_ms") or 0,
            "exchange_timestamp_ms": source.get("market_timestamp_ms") or 0,
            "open_timestamp_ms": source.get("market_timestamp_ms") or 0 if valid else 0,
            "no_open_print_observed_ms": source.get("no_first_print_observed_timestamp_ms") or 0 if absence else 0,
            "volumes": 0. if absence else None,
            **{key: prior.get(key) for key in ("upper_limit_prices", "lower_limit_prices")}}
    return {**payload, "captured_at_taipei": captured.isoformat(timespec="milliseconds"), "rows": rows,
        "diagnostic_projection": {"contract": "private_native_journal_reference_projection_v1",
            "journal_prefix": timeline["journal_prefix"], "after_open_ms": after_open_ms,
            "production_promotion_allowed": False, "limits_basis": "retained_engineering_metadata_not_L1_proof"}}


def receipt_snapshot(payload: dict) -> tuple[list[str], quotes.PriceSnapshot]:
    """Reuse canonical masks; do not trust a saved missing-symbol count."""
    symbols = payload["requested_symbols"]
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError("opening audit requires a nonempty unique symbol scope")
    captured = datetime.fromisoformat(payload["captured_at_taipei"])
    if captured.tzinfo is None or captured.astimezone(TAIPEI).date().isoformat() != payload["session_date"]:
        raise ValueError("opening audit receipt clock/session mismatch")
    rows = [payload["rows"].get(symbol, {}) for symbol in symbols]
    def values(field, *, integer=False):
        return np.asarray([row.get(field) or 0 if integer else
            np.nan if row.get(field) is None else row[field] for row in rows],
            dtype=np.int64 if integer else np.float64)
    received = values("timestamp_ms", integer=True)
    market = values("exchange_timestamp_ms", integer=True)
    opened = values("open_timestamp_ms", integer=True)
    absence = values("no_open_print_observed_ms", integer=True)
    captured_ms = int(captured.timestamp() * 1000)
    # Canonical v2 capture clocks are truncated to seconds. Respect that
    # declared precision interval, never relabel its fractional rows as future.
    capture_precision_ms = 1000 if "." not in payload["captured_at_taipei"] else 1
    if any(np.any(stamps >= captured_ms + capture_precision_ms) for stamps in (received, market, opened, absence)):
        raise ValueError("opening receipt contains observations after its capture clock")
    # An observation inside a declared whole-second precision interval supplies
    # the more precise lower bound. Do not mark a valid .240 row as future.
    snapshot_clock_ms = max(captured_ms, *(int(stamps.max(initial=0)) for stamps in (received, market, opened, absence)))
    snapshot = quotes.PriceSnapshot(
        prices=values("price"), source="opening:audited_receipt",
        timestamp=datetime.fromtimestamp(snapshot_clock_ms / 1000., TAIPEI).isoformat(), requested_count=len(symbols),
        available_mask=np.asarray([bool(row.get("available")) for row in rows]),
        open_prices=values("open_prices"), volumes=values("volumes"),
        timestamps_ms=received, exchange_timestamps_ms=opened,
        no_open_print_timestamps_ms=absence)
    return symbols, snapshot


def benchmark_journal_decoders(path: Path, payload: dict, universe: dict, *, blocks: int) -> dict:
    """Fixed-prefix full-output ABBA; does not assert an opening-path speedup."""
    if type(blocks) is not int or not 1 <= blocks <= 5:
        raise ValueError("decoder ABBA blocks must be between 1 and 5")
    samples, oracle = [], None
    for block in range(blocks):
        for decoder in ("full", "projected", "projected", "full"):
            result = journal_symbol_timeline(path, payload, universe, project_headers=decoder == "projected")
            duration = result.pop("read_and_analysis_ms")
            content_hash = hashlib.sha256(json.dumps(result, sort_keys=True, ensure_ascii=False,
                separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            if oracle is None:
                oracle = content_hash
            samples.append({"block": block, "decoder": decoder, "read_and_analysis_ms": duration,
                "output_sha256": content_hash, "journal_prefix": result["journal_prefix"],
                "state": result["state"], "parity": content_hash == oracle})
    return {"contract": "read_only_journal_full_output_abba_v1", "blocks": blocks, "samples": samples,
        "parity": all(row["parity"] for row in samples),
        "medians_ms": {decoder: round(statistics.median(row["read_and_analysis_ms"]
            for row in samples if row["decoder"] == decoder), 3) for decoder in ("full", "projected")},
        "boundary": "entire_offline_symbol_analysis_not_live_opening_latency_OS_cache_uncontrolled"}


def classify_symbols(payload: dict, universe: dict, *, comparison: dict | None = None) -> dict:
    if payload["session_date"] != universe["session_date"]:
        raise ValueError("official membership and opening receipt sessions differ")
    scope = payload.get("official_trading_universe", {})
    if scope.get("fingerprint") != universe["fingerprint"]:
        raise ValueError("opening/official scope generation differs; reconcile explicitly")
    symbols, snapshot = receipt_snapshot(payload)
    captured_ms = int(datetime.fromisoformat(snapshot.timestamp).timestamp() * 1000)
    quality, observations = quotes.tw_opening_observation_diagnostics(
        symbols, snapshot, session_date=payload["session_date"], asof_timestamp_ms=captured_ms)
    registry = {row["symbol"]: row for row in universe["records"]}
    compared = {}
    if comparison is not None:
        if comparison.get("official_trading_universe", {}).get("fingerprint") != universe["fingerprint"]:
            raise ValueError("comparison/official scope generation differs; reconcile explicitly")
        if (comparison["session_date"] != payload["session_date"] or
                datetime.fromisoformat(comparison["captured_at_taipei"]) <
                datetime.fromisoformat(payload["captured_at_taipei"])):
            raise ValueError("comparison must be a later observation of the same session")
        other_symbols, other_snapshot = receipt_snapshot(comparison)
        _, later_rows = quotes.tw_opening_observation_diagnostics(
            other_symbols, other_snapshot, session_date=payload["session_date"],
            asof_timestamp_ms=int(datetime.fromisoformat(other_snapshot.timestamp).timestamp() * 1000))
        compared = {row["symbol"]: row for row in later_rows}
    unknown, no_print = [], []
    for observation in observations:
        if observation["state"] == "valid_open":
            continue
        record = registry.get(observation["symbol"], {})
        row = {**observation, "name": record.get("name"), "market": record.get("market"),
            "official_status": record.get("trading_status", "not_in_verified_current_registry"),
            "listing_date": record.get("listing_date"),
            "later_observation": compared.get(observation["symbol"]),
            "exclusion_allowed": record.get("trading_status") == "halted"}
        (no_print if observation["state"] == "observed_no_first_print" else unknown).append(row)
    return {"contract": "opening_symbol_membership_and_source_evidence_v1",
        "session_date": payload["session_date"], "source_captured_at": payload["captured_at_taipei"],
        "official_observed_at": universe["observed_at_taipei"],
        "official_fingerprint": universe["fingerprint"], "source_quality": quality,
        "unknown_count": len(unknown), "unknown_official_status_counts": dict(Counter(row["official_status"] for row in unknown)),
        "unknown_symbols": unknown, "observed_no_first_print_symbols": no_print,
        "boundary": "reference_and_source_diagnostics_not_execution_or_delisting_inference",
        "no_first_print_max_age_seconds": quotes._TW_OPENING_NO_PRINT_PROOF_TTL_SECONDS}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--compare-receipt", type=Path)
    parser.add_argument("--refresh-unknown-mis", action="store_true")
    parser.add_argument("--journal", type=Path, help="Read-only complete official-axis deadline and recovery analysis")
    parser.add_argument("--deadlines-ms", default="1000,5000,10000,11000,20000,30000,32000,59000,60000,120000,300000")
    parser.add_argument("--project-at-ms", type=int, help="Also save a private diagnostic source projection at this original clock")
    parser.add_argument("--projection-output", type=Path)
    parser.add_argument("--journal-decoder-abba-blocks", type=int, default=0,
                        help="Optional fixed-prefix full-output decoder comparison, 1-5 ABBA blocks")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        cutoffs = tuple(int(value) for value in args.deadlines_ms.split(","))
        if not cutoffs or any(value < 0 for value in cutoffs):
            raise ValueError("negative/empty deadline")
    except ValueError:
        parser.error("deadlines-ms must contain nonnegative integer milliseconds")
    if (args.project_at_ms is None) != (args.projection_output is None) or (args.project_at_ms is not None and not args.journal):
        parser.error("projection requires --journal, --project-at-ms and --projection-output")
    if args.projection_output is not None and (args.projection_output.exists()
            or args.projection_output.resolve() in {args.receipt.resolve(), args.output.resolve(), args.journal.resolve()}):
        parser.error("projection output must be new and distinct from sources/report")
    if not 0 <= args.journal_decoder_abba_blocks <= 5 or (args.journal_decoder_abba_blocks and not args.journal):
        parser.error("journal decoder comparison requires --journal and 1-5 blocks")
    if args.output.resolve() in {path.resolve() for path in (args.receipt, args.compare_receipt, args.journal) if path is not None}:
        parser.error("output must not overwrite a source receipt")
    if args.output.exists():
        parser.error("output receipt must be new")
    raw_source = args.receipt.read_bytes()
    payload = json.loads(raw_source)
    universe = load_current_universe(payload["session_date"])
    raw_comparison = args.compare_receipt.read_bytes() if args.compare_receipt else None
    comparison = json.loads(raw_comparison) if raw_comparison is not None else None
    report = classify_symbols(payload, universe, comparison=comparison)
    if args.journal:
        if args.project_at_ms is not None:
            cutoffs = (*cutoffs, args.project_at_ms)
        report["journal_timeline"] = journal_symbol_timeline(args.journal, payload, universe,
            cutoffs=cutoffs, include_reference_rows=args.projection_output is not None)
        if args.projection_output is not None:
            projection = journal_reference_receipt(payload, report["journal_timeline"], args.project_at_ms)
            atomic_write_json(args.projection_output, projection)
            report["journal_timeline"].pop("deadline_reference_rows")
            report["private_projection"] = {"path": str(args.projection_output), "after_open_ms": args.project_at_ms,
                "production_promotion_allowed": False}
        if args.journal_decoder_abba_blocks:
            report["journal_decoder_benchmark"] = benchmark_journal_decoders(args.journal, payload, universe,
                blocks=args.journal_decoder_abba_blocks)
    report.update(observed_at_taipei=datetime.now(TAIPEI).isoformat(),
        source_sha256=hashlib.sha256(raw_source).hexdigest(),
        provider_requests_enabled=args.refresh_unknown_mis, broker_orders=0,
        production_ledger_modified=False, extra_shioaji_login=False)
    if raw_comparison is not None:
        report["comparison_sha256"] = hashlib.sha256(raw_comparison).hexdigest()
        report["comparison_captured_at"] = comparison["captured_at_taipei"]
    # Preserve the classification even if every new request fails.
    atomic_write_json(args.output, report)
    if args.refresh_unknown_mis:
        symbols = [row["symbol"] for row in report["unknown_symbols"] if not row["exclusion_allowed"]]
        if symbols:
            started = time.perf_counter()
            try:
                snapshot = quotes.fetch_tw_mis_last_prices(symbols, np.full(len(symbols), np.nan),
                    parquet_root=payload["parquet_root"], chunk_size=80, max_parallel_requests=4,
                    empty_chunk_retry_attempts=1, request_timeout_seconds=4.)
                quality, observations = quotes.tw_opening_observation_diagnostics(
                    symbols, snapshot, session_date=payload["session_date"])
                report["fresh_mis"] = {"quality": quality, "observations": observations,
                    "transport_timing": snapshot.transport_timing}
            except Exception as exc:
                report["fresh_mis"] = {"state": "failed", "error_type": type(exc).__name__}
            report["fresh_mis"]["total_ms"] = round((time.perf_counter() - started) * 1000, 3)
            report["fresh_mis"]["observed_at_taipei"] = datetime.now(TAIPEI).isoformat()
            atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("session_date", "unknown_count", "unknown_official_status_counts")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
