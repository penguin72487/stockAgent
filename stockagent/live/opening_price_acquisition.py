"""Session-fenced, single-flight fan-in for model opening inputs (not fills)."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, time as daytime
import hashlib
import json
import math
import os
from pathlib import Path
import threading
import time
import uuid
from typing import Callable
from zoneinfo import ZoneInfo

import numpy as np
import requests

from stockagent import private_json
from stockagent.live import quote_provider as quotes
from stockagent.live.opening_price_journal import pack_observation_details

Progress = Callable[[list[str], quotes.PriceSnapshot], None]
Provider = Callable[[list[str], np.ndarray, Progress], quotes.PriceSnapshot]
TAIPEI = ZoneInfo("Asia/Taipei")
_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY_START_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tw-opening-start-log")


def _snapshot_poll_interval(name: str, observed: datetime) -> float | None:
    """Provider refresh cadence is not a mandatory client polling interval.

    FinMind's full-market response is refreshed approximately every ten seconds,
    with no promised phase relative to 09:00. Brief, serial opening polls avoid
    sleeping through that first update. This is at most eleven starts in the
    protected eleven-second window, not a permanent high-rate poller. Quota and
    transport failures retain their independent, longer backoff.
    """
    if name.startswith("finmind"):
        local = observed.astimezone(TAIPEI)
        opening = local.replace(hour=9, minute=0, second=0, microsecond=0)
        return 1.0 if 0 <= (local - opening).total_seconds() < 11 else 10.0
    if name.startswith("fugle"):
        return 5.0
    return None


def _source_failure_retry(metric: dict, *, status: int | None = None,
                          endpoint_unavailable: bool = False,
                          allow_transport_probe: bool = False) -> None:
    """Provider-local failure isolation; accepted rows and other sources continue."""
    failures = int(metric.get("consecutive_source_failures", 0)) + 1
    metric["consecutive_source_failures"] = failures
    delay = 60.0 if status in {401, 402, 403, 429} else min(
        15.0, 0.5 * 2 ** min(failures - 1, 5)
    )
    if endpoint_unavailable:
        delay = max(delay, 15.0)
    metric["retry_delay_seconds"] = delay
    metric["retry_after_monotonic"] = time.monotonic() + delay
    metric["retry_reason"] = "source_endpoint_unavailable" if endpoint_unavailable else "source_failure"
    metric.pop("transport_probe_after_monotonic", None)
    metric.pop("transport_probe_delay_seconds", None)
    metric.pop("transport_recovery_resume", None)
    if endpoint_unavailable and allow_transport_probe and status not in {401, 402, 403, 429}:
        # A transport timeout proves neither a permission refusal nor a fixed
        # 15-second outage. Keep the full-sweep circuit open, but admit ONE
        # real requested symbol at an exponentially backed-off half-open clock.
        # This is not another quote owner/session or a full-market retry burst.
        probe_delay = min(15.0, 0.5 * 2 ** min(failures - 1, 5))
        metric["transport_probe_after_monotonic"] = time.monotonic() + probe_delay
        metric["transport_probe_delay_seconds"] = probe_delay


def _source_no_progress_retry(metric: dict) -> None:
    """A responding source is not a fresh input; do not hammer unchanged gaps.

    Keep this distinct from transport failure and from a proven first-print
    absence. Other providers, accepted facts and the consumer retry continue.
    """
    attempts = int(metric.get("consecutive_source_no_progress", 0)) + 1
    metric["consecutive_source_failures"] = 0
    metric.pop("transport_probe_after_monotonic", None)
    metric.pop("transport_probe_delay_seconds", None)
    metric.pop("transport_recovery_resume", None)
    metric["consecutive_source_no_progress"] = attempts
    # A responding source is a short-lived data wait, not an outage. Keep its
    # next opportunity within one second; transport failures use the separate
    # longer backoff. Native push observations remain independent of this wait.
    delay = min(1.0, 0.5 * 2 ** min(attempts - 1, 1))
    metric["retry_delay_seconds"] = delay
    metric["retry_after_monotonic"] = time.monotonic() + delay
    metric["retry_reason"] = "no_same_session_opening_inputs"


def _record_source_event(key, provider, event, details) -> bool:
    return _record_source_events(key, provider, [(event, details)])


def _record_source_events(key, provider, events) -> bool:
    """Durable, safe metadata only; slow/failed cases remain in the daily log."""
    path = quotes._tw_mis_opening_receipt_path(key[1]).with_suffix(".acquisition.jsonl")
    try:
        # Large per-symbol evidence must not serialize while holding the shared
        # append lock: a different provider's tiny quorum receipt can be ready.
        payloads = []
        for event, details in events:
            packed = pack_observation_details(details) if event == "observations_delta" else details
            payloads.append({"schema_version": 2 if packed is not details else 1,
                "session_date": key[1], "parquet_root": key[0],
                "provider": provider, "event": event, "owner_pid": os.getpid(),
                "observed_at_taipei": datetime.now(TAIPEI).isoformat(timespec="microseconds"),
                "measurement_boundary": "opening_source_acquisition_not_broker_fill", **packed})
        encoded = "".join(json.dumps(payload, ensure_ascii=False, allow_nan=False,
            separators=(",", ":")) + "\n" for payload in payloads)
        with _TELEMETRY_LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
        return True
    except (OSError, TypeError, ValueError):
        return False


def _journal_provider_metrics(metrics, durable_jobs):
    """Reference only source details already fsynced in this owner's journal.

    Consumer DTOs retain the full metrics. A pending/failed journal append must
    keep its details inline; a reference never substitutes for missing evidence.
    """
    result = {}
    for name, metric in metrics.items():
        summary = dict(metric)
        job = summary.get("last_completed_job")
        job_id = job.get("job_id") if isinstance(job, dict) else None
        if job_id is not None and durable_jobs.get(name) == job_id:
            reference = {"event": "finished", "job_id": job_id, "owner_pid": os.getpid()}
            summary.pop("last_completed_job")
            summary["completed_job_ref"] = reference
            # A newer result may already have updated aggregate transport while
            # the previous job's durable completion is still the latest one.
            if summary.get("last_result_job_id") == job_id:
                summary.pop("transport_timing", None)
                summary["transport_timing_ref"] = reference
        result[name] = summary
    return result


@dataclass
class _Session:
    condition: threading.Condition = field(default_factory=threading.Condition)
    inflight: dict[str, set[str]] = field(default_factory=dict)
    metrics: dict[str, dict] = field(default_factory=dict)
    generation: int = 0
    provider_symbols: dict[str, dict[str, set[str]]] = field(default_factory=dict)
    observations: dict[str, dict[str, dict]] = field(default_factory=dict)
    provider_snapshot_ids: set[str] = field(default_factory=set)
    durable_completed_jobs: dict[str, str] = field(default_factory=dict)
    jobs: set[Future] = field(default_factory=set)


class OpeningPriceFanIn:
    """Provider workers continue independently after a caller reaches quorum.

    No network I/O while holding the session/cache locks. Overlapping model
    universes borrow in-flight work; only universe deltas submit extra work.
    Returned arrays are detached from the mutable acquisition state.
    """

    def __init__(self, *, workers: int = 8):
        self._owner_id = uuid.uuid4().hex
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tw-opening-source")
        self._lock = threading.Lock()
        self._sessions: dict[tuple[str, str], _Session] = {}
        self._transport_sessions: dict[tuple[str, str], _Session] = {}
        self._closed = False

    def close(self) -> None:
        # Tests/normal shutdown only; a quorum return never shuts down the pool.
        with self._lock:
            self._closed = True
            for session in [*self._sessions.values(), *self._transport_sessions.values()]:
                with session.condition:
                    session.generation += 1
                    session.condition.notify_all()
        self._pool.shutdown(wait=True, cancel_futures=True)

    def wait_idle(self, *, timeout_seconds: float = 8) -> bool:
        """Drain complete jobs, including input fsync and final journal writes.

        Network inflight is cleared before persistence so model retries need
        not wait on telemetry. It cannot prove the worker's durable completion.
        This diagnostic boundary is never part of the model quorum wait.
        """
        deadline = time.monotonic() + timeout_seconds
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            with session.condition:
                while session.jobs or any(session.inflight.values()):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    session.condition.wait(timeout=remaining)
        return True

    @staticmethod
    def _job_finished(session: _Session, job: Future) -> None:
        with session.condition:
            session.jobs.discard(job)
            session.condition.notify_all()

    def warm_transports(self, *, parquet_root, session_date, probes, force=False, timeout_seconds=2):
        """First proven transport wins; this cannot supply model prices.

        Shared across the four cache-warm/final-arm passes. Probe observations
        never seed the opening cache, especially before the actual open gate.
        """
        key = (str(Path(parquet_root).resolve()), session_date)
        started = time.perf_counter()
        with self._lock:
            if self._closed:
                raise RuntimeError("opening acquisition owner is closed")
            self._transport_sessions = {k: v for k, v in self._transport_sessions.items() if k[1] == session_date}
            session = self._transport_sessions.setdefault(key, _Session())
        # Parse the local venue map before the opening gate, alongside existing
        # preparation, not on the first full sweep. The quote owner rechecks its
        # file generation on every use; this is NOT official scope or a price.
        local_started = time.perf_counter()
        venue_mapping_count = len(quotes.load_symbol_yahoo_map(key[0]))
        local_preparation = {"venue_mapping_count": venue_mapping_count,
            "venue_mapping_ms": round((time.perf_counter() - local_started) * 1000, 3),
            "boundary": "venue_mapping_only_not_official_membership_or_quote"}
        with session.condition:
            ttl = 60 if force else 7200
            if not any(row.get("ready") and time.monotonic() - row.get("completed_monotonic", 0) < ttl for row in session.metrics.values()):
                for name, probe in probes.items():
                    if session.inflight.get(name):
                        continue
                    session.inflight[name] = {"transport_probe"}
                    session.metrics[name] = {"state": "running", "ready": False}
                    self._pool.submit(self._probe_transport, session, name, probe, key)
            deadline = time.monotonic() + timeout_seconds
            while not any(row.get("ready") and time.monotonic() - row.get("completed_monotonic", 0) < ttl for row in session.metrics.values()):
                if self._closed:
                    raise RuntimeError("opening acquisition owner is closed")
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not any(session.inflight.values()):
                    raise RuntimeError("no configured opening source transport is currently proven")
                session.condition.wait(timeout=remaining)
            if self._closed:
                raise RuntimeError("opening acquisition owner is closed")
            metrics = {name: dict(row) for name, row in session.metrics.items()}
        return {"ready": True, "source": "parallel_opening_sources", "proof": "responding_transport_not_opening_price_readiness", "session_date": session_date, "elapsed_ms": round((time.perf_counter() - started) * 1000, 3), "local_preparation": local_preparation, "providers": metrics}

    def _probe_transport(self, session, name, probe, key):
        started = time.perf_counter()
        try:
            result = probe()
            if result.get("ready") is not True:
                raise RuntimeError("source transport probe did not establish readiness")
            metric = {"state": "complete", "ready": True, "proof": result.get("proof", "validated_provider_response"), "completed_monotonic": time.monotonic()}
        except Exception as exc:
            metric = {"state": "failed", "ready": False, "error_type": type(exc).__name__}
        metric["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        with session.condition:
            session.metrics[name] = metric
            session.inflight[name].clear()
            session.condition.notify_all()
        _record_source_event(key, name, "transport_probe", {**metric, "measurement_boundary": "transport_readiness_not_opening_price_or_fill"})

    def fetch(
        self, symbols: list[str], fallback: np.ndarray, *, parquet_root: str | Path,
        providers: dict[str, Provider], minimum_coverage: float,
        cache_reader: Callable[[], quotes.PriceSnapshot], timeout_seconds: float = 4.5,
        resolve_no_prints: bool = True, force_refresh: bool = False,
        poll_opening_snapshots: bool = False,
    ) -> quotes.PriceSnapshot:
        requested_set = frozenset(symbols)
        if not symbols or len(requested_set) != len(symbols):
            raise ValueError("opening universe must be nonempty and unique")
        fallback = np.asarray(fallback, dtype=np.float64)
        if fallback.shape != (len(symbols),) or not 0 <= minimum_coverage <= 1:
            raise ValueError("invalid opening fallback/coverage")
        now = datetime.now(TAIPEI)
        if now.time() < daytime(9):
            raise RuntimeError("opening acquisition cannot start before 09:00 Taipei")
        date_text = now.date().isoformat()
        # The caller owns a bounded wait, not the provider's next opportunity.
        # Without this wake clock a fast old-date FinMind response waited for
        # slow MIS/broker work (up to 4.5s) before another client could poll it.
        opening = now.replace(hour=9, minute=0, second=0, microsecond=0)
        polling_remaining = max(0.0, 11.0 - (now - opening).total_seconds())
        polling_until = time.monotonic() + (polling_remaining if poll_opening_snapshots else 0.0)
        key = (str(Path(parquet_root).resolve()), date_text)
        started = time.perf_counter()
        required = max(1, int(np.ceil(len(symbols) * minimum_coverage)))
        with self._lock:
            if self._closed:
                raise RuntimeError("opening acquisition owner is closed")
            # Retire yesterday's generation before accepting any new callbacks.
            for old_key, old in list(self._sessions.items()):
                if old_key[0] == key[0] and old_key[1] != date_text:
                    with old.condition:
                        old.generation += 1
                        old.condition.notify_all()
                    del self._sessions[old_key]
            session = self._sessions.setdefault(key, _Session())
        observed = cache_reader()
        flush_timing = {}
        before = quotes.tw_session_open_observed_mask(observed, session_date=date_text)
        resolved = before | (quotes.tw_session_no_open_print_mask(observed, session_date=date_text,
            asof_timestamp_ms=int(datetime.now(TAIPEI).timestamp() * 1000)) if resolve_no_prints else False)
        if not force_refresh and int(resolved.sum()) >= required:
            quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=date_text, timings=flush_timing)
            observed.transport_timing = {**(observed.transport_timing or {}), "opening_acquisition_policy": "parallel_incremental_fan_in_v1", "opening_cache_ready": True, "opening_durable_flush": flush_timing, "opening_quorum_wait_ms": round((time.perf_counter() - started) * 1000, 3)}
            return observed
        # Record the entire requested universe even if only an early partial
        # batch reaches quorum and a later provider disconnects completely.
        quotes.seed_tw_opening_snapshot_cache(symbols, quotes.PriceSnapshot(prices=fallback, source="opening_request_scope", available_mask=np.zeros(len(symbols), dtype=bool)), parquet_root=key[0], session_date=date_text, persist=False)
        def dispatch(observed, observed_at, *, names=None):
            # Called under the short session lock; provider I/O stays in its
            # existing worker. Shared inflight/cooldown covers ALL consumers.
            # A callback can finish after the optimistic cache read but before
            # this dispatch lock is acquired. Re-read under the same lock used
            # by callback merges/removal from inflight: otherwise a concurrent
            # model can resubmit an already-resolved delta (and spend API quota).
            opens = quotes.tw_session_open_observed_mask(observed, session_date=date_text)
            resolved = opens | (quotes.tw_session_no_open_print_mask(observed, session_date=date_text,
                asof_timestamp_ms=int(observed_at.timestamp() * 1000)) if resolve_no_prints else False)
            # Initial pass requests the complete universe from EVERY provider,
            # even if another provider wins immediately. Later passes retry gaps.
            jobs = []
            for name, provider in providers.items():
                if names is not None and name not in names:
                    continue
                previous = session.metrics.get(name, {})
                clock = time.monotonic()
                cooling_down = clock < previous.get("retry_after_monotonic", 0)
                recovery_probe = bool(name == "official_mis" and cooling_down
                    and previous.get("retry_reason") == "source_endpoint_unavailable"
                    and "transport_probe_after_monotonic" in previous
                    and clock >= previous["transport_probe_after_monotonic"])
                if cooling_down and not recovery_probe:
                    continue
                active = session.inflight.setdefault(name, set())
                if recovery_probe and active:
                    continue
                # These endpoints fetch a whole market even for a one-symbol
                # consumer delta. Per-symbol single-flight alone allowed five
                # modes to send overlapping full-market HTTP requests.
                if active and _snapshot_poll_interval(name, observed_at) is not None:
                    continue
                indices = [i for i, symbol in enumerate(symbols) if symbol not in active and (name not in session.metrics or not resolved[i])]
                if not indices:
                    continue
                if recovery_probe:
                    indices = indices[:1]
                requested = [symbols[i] for i in indices]
                active.update(requested)
                metric = session.metrics.setdefault(name, {"attempts": 0, "requested_count": 0, "state": "pending"})
                metric["attempts"] += 1
                metric["requested_count"] += len(requested)
                metric["state"] = "running"
                if not recovery_probe:
                    metric.pop("transport_recovery_resume", None)
                scope = session.provider_symbols.setdefault(name, {key: set() for key in ("requested", "responded", "valid", "no_print")})
                scope["requested"].update(requested)
                metric["unique_requested_count"] = len(scope["requested"])
                job_id = f"{name}:{metric['attempts']}:{self._owner_id}"
                jobs.append((name, provider, requested, fallback[indices].copy(), job_id, recovery_probe))
            # Enqueue all sources under a short lock. Workers perform no I/O
            # until the lock is released; a fast winner cannot cancel a loser.
            for name, provider, requested, values, job_id, recovery_probe in jobs:
                job = self._pool.submit(self._run, session, generation, key, name, provider,
                    requested, values, started, job_id, time.perf_counter(), recovery_probe)
                session.jobs.add(job)
                job.add_done_callback(lambda completed, state=session: self._job_finished(state, completed))
        with session.condition:
            generation = session.generation
            dispatch(cache_reader(), now)
        deadline = time.monotonic() + max(0.01, timeout_seconds)
        snapshot_poll_names = {name for name in providers if name.startswith("finmind")}
        while True:
            with session.condition:
                if session.generation != generation or self._closed:
                    raise RuntimeError("opening acquisition session was superseded")
                observed = cache_reader()
                opens = quotes.tw_session_open_observed_mask(observed, session_date=date_text)
                resolved = opens | (quotes.tw_session_no_open_print_mask(observed, session_date=date_text,
                    asof_timestamp_ms=int(datetime.now(TAIPEI).timestamp() * 1000)) if resolve_no_prints else False)
                ready = int(resolved.sum()) >= required
                clock = time.monotonic()
                mis_metric = session.metrics.get("official_mis", {})
                poll_names = snapshot_poll_names | ({"official_mis"} if "official_mis" in providers
                    and ("transport_probe_after_monotonic" in mis_metric
                         or mis_metric.get("transport_recovery_resume")) else set())
                can_poll = not ready and clock < min(deadline, polling_until) and bool(poll_names)
                if can_poll:
                    dispatch(observed, datetime.now(TAIPEI), names=poll_names)
                pending = any(not requested_set.isdisjoint(values) for values in session.inflight.values())
                remaining = deadline - clock
                next_poll = min((session.metrics[name].get("transport_probe_after_monotonic",
                                    session.metrics[name].get("retry_after_monotonic", clock))
                                 for name in poll_names if not session.inflight.get(name)), default=None) if can_poll else None
                scheduled_poll = next_poll is not None and next_poll < min(deadline, polling_until)
                if ready or (not pending and not scheduled_poll) or remaining <= 0:
                    metrics = {name: dict(metric) for name, metric in session.metrics.items()}
                    journal_metrics = _journal_provider_metrics(metrics, session.durable_completed_jobs)
                    break
                # Provider completion notifies immediately; an idle healthy
                # snapshot also wakes at its OWN request-start retry clock.
                wake = min(deadline, polling_until) - clock if can_poll else remaining
                if scheduled_poll:
                    wake = min(wake, max(0.001, next_poll - clock))
                session.condition.wait(timeout=max(0.001, min(remaining, wake)))
        # Persist the exact accepted input set before it can drive publication.
        quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=date_text, timings=flush_timing)
        observed.transport_timing = {
            **(observed.transport_timing or {}),
            "opening_acquisition_policy": "parallel_incremental_fan_in_v1",
            "opening_cache_ready": False,
            "opening_source_pass_ms": round((time.perf_counter() - started) * 1000, 3),
            "opening_quorum_wait_ms": round((time.perf_counter() - started) * 1000, 3),
            "opening_valid_before": int(before.sum()), "opening_valid_after": int(opens.sum()),
            "opening_resolved_count": int(resolved.sum()), "opening_required_count": required,
            "opening_quorum_ready": ready, "opening_background_pending": pending,
            "opening_snapshot_poll_owner": "bounded_shared_fan_in" if poll_opening_snapshots else "caller",
            "opening_durable_flush": flush_timing,
            "opening_providers": metrics,
            "opening_source_errors": [f"{name}_{row['error_type']}" for name, row in metrics.items() if row.get("error_type")],
        }
        observed.transport_timing["opening_telemetry_recorded"] = _record_source_event(key, "merged", "quorum" if ready else "waiting_source", {"requested_count": len(symbols), "valid_open_count": int(opens.sum()), "resolved_count": int(resolved.sum()), "required_count": required, "elapsed_ms": observed.transport_timing["opening_quorum_wait_ms"], "background_pending": pending, "input_flush": flush_timing, "providers": journal_metrics, "provider_metric_layout": "inline_until_durable_finished_reference_v1"})
        return observed

    def _run(self, session, generation, key, name, provider, symbols, fallback, caller_started, job_id, enqueued_at=None, recovery_probe=False):
        started = time.perf_counter()
        source_call_started = time.monotonic()
        poll_interval = _snapshot_poll_interval(name, datetime.now(TAIPEI))
        # Per-request facts must not alias the provider aggregate: a one-symbol
        # delta used to overwrite the simultaneous 2,293-symbol batch receipt.
        metric = {"job_id": job_id, "requested_count": len(symbols), "state": "running",
                  "transport_recovery_probe": recovery_probe,
                  "requested_symbols_sha256": hashlib.sha256("\n".join(symbols).encode()).hexdigest(),
                  "requested_scope_sha256": hashlib.sha256("\n".join(sorted(symbols)).encode()).hexdigest(),
                  "measurement_clock": "provider_job_perf_counter_v1"}
        with session.condition:
            metric["last_started_at_taipei"] = datetime.now(TAIPEI).isoformat(timespec="microseconds")
            metric["last_dispatch_ms"] = round((started - caller_started) * 1000, 3)
            metric["worker_queue_ms"] = round((started - enqueued_at) * 1000, 3) if enqueued_at is not None else None
            aggregate = session.metrics[name]
            aggregate["last_started_at_taipei"] = metric["last_started_at_taipei"]
            aggregate["last_dispatch_ms"] = metric["last_dispatch_ms"]
            if poll_interval is not None:
                metric["minimum_poll_interval_seconds"] = poll_interval
                metric["poll_clock_basis"] = "request_start_not_completion"
            aggregate.pop("error_type", None)
            aggregate.pop("http_status", None)
        # Dispatch metadata is observation telemetry, not an input durability
        # gate. Journal it alongside the external request instead of making
        # every provider wait behind all earlier providers' fsyncs. Its actual
        # original clock is frozen here, not when the writer gets CPU time.
        # Each job joins this write BEFORE any of its progress/finished events;
        # the source and model-input barriers below remain synchronous.
        dispatch_receipt = _TELEMETRY_START_POOL.submit(_record_source_event, key, name, "started", {
            "job_id": job_id, "requested_count": len(symbols),
            "requested_symbols_sha256": metric["requested_symbols_sha256"],
            "requested_scope_sha256": metric["requested_scope_sha256"],
            "dispatch_ms": metric["last_dispatch_ms"], "worker_queue_ms": metric["worker_queue_ms"],
            "observed_at_taipei": metric["last_started_at_taipei"]})
        metric["dispatch_journal_policy"] = "request_overlaps_start_receipt_join_before_progress_v1"
        last_progress = [None]
        requested_set = set(symbols)
        job_valid, job_no_print, job_responded = set(), set(), set()

        def join_dispatch_receipt():
            if "dispatch_telemetry_recorded" not in metric:
                waiting = time.perf_counter()
                metric["dispatch_telemetry_recorded"] = dispatch_receipt.result()
                metric["dispatch_telemetry_wait_ms"] = round((time.perf_counter() - waiting) * 1000, 3)

        def progress(requested, snapshot):
            validation_started = time.perf_counter()
            if not set(requested).issubset(requested_set) or len(requested) != len(snapshot.prices):
                raise ValueError("provider progress does not match requested universe")
            quality, observations = quotes.tw_opening_observation_diagnostics(requested, snapshot, session_date=key[1],
                include_no_first_print_clock=True)
            diagnostics_ms = (time.perf_counter() - validation_started) * 1000
            changed = []
            with session.condition:
                if session.generation != generation or datetime.now(TAIPEI).date().isoformat() != key[1] or self._closed:
                    return
                merge_started = time.perf_counter()
                merged = quotes.seed_tw_opening_snapshot_cache(requested, snapshot, parquet_root=key[0], session_date=key[1], persist=False)
                merge_ms = (time.perf_counter() - merge_started) * 1000
                # A failed/absent source owns no input durability barrier. The
                # consumer always persists the full requested scope, and a
                # source that observed ANY rows still flushes even if it later
                # fails. Do not let a 403/timeout force an empty receipt write
                # ahead of the independently arriving useful observations.
                if merged["accepted_count"]:
                    metric["input_observation_received"] = True
                scope = session.provider_symbols[name]
                observed = session.observations.setdefault(name, {})
                previous_counts = (len(job_valid), len(job_no_print), len(job_responded))
                for row in observations:
                    symbol, state = row["symbol"], row["state"]
                    if row["available"]:
                        scope["responded"].add(symbol)
                        job_responded.add(symbol)
                    if state == "valid_open":
                        scope["valid"].add(symbol)
                        scope["no_print"].discard(symbol)
                        job_valid.add(symbol)
                        job_no_print.discard(symbol)
                    elif state == "observed_no_first_print" and symbol not in scope["valid"]:
                        scope["no_print"].add(symbol)
                    if state == "observed_no_first_print" and symbol not in job_valid:
                        job_no_print.add(symbol)
                    previous = observed.get(symbol)
                    # A partial cumulative batch cannot turn an earlier source
                    # observation into absence. Persist the first observation
                    # and every reason/open/flag transition, not every tick's
                    # wall-clock update; successful observations stay intact.
                    if state == "no_source_observation" and previous is not None:
                        continue
                    # Absence is a time-bounded fact. Refresh its OWN proof,
                    # never the latest delivery time of an unrelated row.
                    identity = (state, row["open_price"], row["simtrade"],
                        row.get("no_first_print_observed_timestamp_ms") if state == "observed_no_first_print" else None)
                    before = (previous["state"], previous["open_price"], previous["simtrade"],
                        previous.get("no_first_print_observed_timestamp_ms") if previous["state"] == "observed_no_first_print" else None) if previous is not None else None
                    observed[symbol] = row
                    if identity != before:
                        changed.append(row)
                aggregate.update(response_count=len(scope["responded"]),
                                 valid_open_count=len(scope["valid"]), no_print_count=len(scope["no_print"]))
                metric["observation_quality"] = quality
                # Nested spans, not extra sequential costs to add to source
                # total. Preserve slow/failed samples instead of hot-only SLOs.
                metric["last_diagnostics_ms"] = round(diagnostics_ms, 3)
                metric["last_cache_merge_ms"] = round(merge_ms, 3)
                metric["last_validation_ms"] = round((time.perf_counter() - validation_started) * 1000, 3)
                counts = (len(job_valid), len(job_no_print), len(job_responded))
                coverage_event = {"job_id": job_id, "requested_count": len(symbols),
                    "valid_open_count": counts[0], "observed_no_print_count": counts[1],
                    "response_count": counts[2], "resolved_count": counts[0] + counts[1],
                    "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                    "measurement_clock": "provider_job_perf_counter_v1"}
                metric["job_coverage"] = coverage_event
                last_progress[0] = snapshot
                is_first = "first_batch_ms" not in metric
                metric.setdefault("first_batch_ms", round((time.perf_counter() - started) * 1000, 3))
                aggregate.setdefault("first_batch_ms", metric["first_batch_ms"])
                if quality["valid_open_count"]:
                    metric.setdefault("first_valid_open_ms", round((time.perf_counter() - started) * 1000, 3))
                    metric.setdefault("first_valid_open_at_taipei", datetime.now(TAIPEI).isoformat(timespec="microseconds"))
                    aggregate.setdefault("first_valid_open_ms", metric["first_valid_open_ms"])
                    aggregate.setdefault("first_valid_open_at_taipei", metric["first_valid_open_at_taipei"])
                validated_at = datetime.now(TAIPEI).isoformat(timespec="microseconds")
                session.condition.notify_all()
            events = []
            if is_first:
                events.append(("first_batch", {"job_id": job_id, "requested_count": len(symbols), "response_count": int(snapshot.available_count), "valid_open_count": quality["valid_open_count"], "observation_quality": quality, "elapsed_ms": metric["first_batch_ms"], "validation_ms": metric["last_validation_ms"], "diagnostics_ms": metric["last_diagnostics_ms"], "cache_merge_ms": metric["last_cache_merge_ms"]}))
            if changed:
                events.append(("observations_delta", {"job_id": job_id, "observation_quality": quality, "observations": changed}))
            if is_first or counts != previous_counts:
                # Per-job counts remain measurable when a later full-market
                # sweep repeats facts already in the session. Session-level
                # deduplication must not erase intraday/postclose latency proof.
                # Piggyback the existing durable event bundle: no extra fsync.
                events.append(("coverage_progress", coverage_event))
            if events:
                join_dispatch_receipt()
                metric["observation_telemetry_recorded"] = _record_source_events(key, name, [
                    (event, {"observed_at_taipei": validated_at, **details}) for event, details in events])
        try:
            # Transport cadence starts at the actual provider call, not before
            # durable dispatch telemetry. Slow journal I/O must not silently
            # shorten the interval between two real external requests.
            source_call_started = time.monotonic()
            metric["provider_call_offset_ms"] = round((time.perf_counter() - started) * 1000, 3)
            metric["provider_invocation_at_taipei"] = datetime.now(TAIPEI).isoformat(timespec="microseconds")
            snapshot = provider(symbols, fallback, progress)
            if last_progress[0] is not snapshot:
                progress(symbols, snapshot)
            transport = dict(snapshot.transport_timing or {})
            # Full-market source evidence must not be confused with the model's
            # shrinking unresolved delta. Keep it once per actual HTTP response,
            # outside locks and out of every consumer's copied timing metrics.
            source_snapshot = transport.pop("provider_snapshot_coverage", None)
            if source_snapshot is not None:
                with session.condition:
                    snapshot_id = source_snapshot["snapshot_id"]
                    record_snapshot = snapshot_id not in session.provider_snapshot_ids
                    session.provider_snapshot_ids.add(snapshot_id)
                if record_snapshot:
                    metric["provider_snapshot_recorded"] = _record_source_event(
                        key, name, "provider_snapshot", {"job_id": job_id,
                        "coverage": source_snapshot, "transport_timing": transport})
            with session.condition:
                responses = int(snapshot.available_count)
                valid_opens = int(quotes.tw_session_open_observed_mask(snapshot, session_date=key[1]).sum())
                no_prints = int(quotes.tw_session_no_open_print_mask(snapshot, session_date=key[1]).sum())
                state = "complete" if valid_opens or no_prints else "waiting_source"
                if responses == 0:
                    state = "failed" if int(transport.get("mis_failed_request_count", 0)) else "waiting_source"
                endpoint_unavailable = bool(transport.get("mis_dispatch_stopped")
                    or (recovery_probe and responses == 0))
                if endpoint_unavailable and responses:
                    state = "degraded"
                metric.update(state=state, valid_open_count=valid_opens,
                              response_count=responses, no_print_count=no_prints,
                              transport_timing=transport)
                aggregate.update(transport_timing=transport, last_result_job_id=job_id, last_response_count=responses,
                                 last_valid_open_count=valid_opens,
                                 last_observation_quality=metric.get("observation_quality"))
                if endpoint_unavailable or (responses == 0 and not transport.get("response_schema_valid")):
                    statuses = transport.get("mis_http_error_statuses", [])
                    limited = next((code for code in statuses if code in {401, 402, 403, 429}), None)
                    _source_failure_retry(aggregate, status=limited,
                                          endpoint_unavailable=endpoint_unavailable,
                                          allow_transport_probe=name == "official_mis" and endpoint_unavailable)
                elif valid_opens or no_prints or (recovery_probe and responses):
                    aggregate["consecutive_source_failures"] = 0
                    aggregate["consecutive_source_no_progress"] = 0
                    aggregate.pop("retry_after_monotonic", None)
                    aggregate.pop("retry_delay_seconds", None)
                    aggregate.pop("retry_reason", None)
                    aggregate.pop("transport_probe_after_monotonic", None)
                    aggregate.pop("transport_probe_delay_seconds", None)
                    if recovery_probe:
                        # A parsed requested row restores transport admission,
                        # not opening-price validity. Original source/date/volume
                        # validation still decides whether any input is ready.
                        metric["transport_health_recovered"] = True
                        aggregate["transport_recovery_resume"] = True
                elif poll_interval is not None:
                    # A valid HTTP/schema response with only yesterday's rows
                    # is a data wait, not an accumulating transport outage.
                    aggregate["consecutive_source_failures"] = 0
                    aggregate["consecutive_source_no_progress"] = int(aggregate.get("consecutive_source_no_progress", 0)) + 1
                    aggregate["retry_reason"] = "no_same_session_opening_inputs"
                    aggregate["retry_after_monotonic"] = source_call_started + poll_interval
                    aggregate["retry_delay_seconds"] = poll_interval
                else:
                    _source_no_progress_retry(aggregate)
        except Exception as exc:
            # Never write provider exception text/HTTP headers/tokens to receipts.
            with session.condition:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                metric.update(state="failed", error_type=type(exc).__name__)
                failure_timing = getattr(exc, "opening_transport_timing", None)
                if isinstance(failure_timing, dict):
                    metric["transport_timing"] = dict(failure_timing)
                if status is not None:
                    metric["http_status"] = int(status)
                aggregate.update(state="failed", error_type=type(exc).__name__)
                if status is not None:
                    aggregate["http_status"] = int(status)
                _source_failure_retry(aggregate, status=403 if isinstance(exc, PermissionError) else status)
        finally:
            # An exception or empty response still retains the started event
            # and its failure result. Do not leave writes running after the
            # provider job completes or turn an I/O failure into a good receipt.
            join_dispatch_receipt()
            with session.condition:
                metric["last_total_ms"] = round((time.perf_counter() - started) * 1000, 3)
                if poll_interval is not None:
                    aggregate["retry_after_monotonic"] = max(aggregate.get("retry_after_monotonic", 0), source_call_started + poll_interval)
                session.inflight[name].difference_update(symbols)
                aggregate["last_total_ms"] = metric["last_total_ms"]
                aggregate["last_completed_job"] = dict(metric)
                aggregate["state"] = "running" if session.inflight[name] else metric["state"]
                should_persist = session.generation == generation and datetime.now(TAIPEI).date().isoformat() == key[1] and not self._closed
                session.condition.notify_all()
            # Fsync/JSON serialization is not a condition-lock dependency of
            # another provider or model. The canonical cache writer still owns
            # generation checking, serialization and durable publication.
            try:
                if should_persist and metric.get("input_observation_received"):
                    flush_timing = {}
                    quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=key[1], timings=flush_timing)
                    with session.condition:
                        metric["input_flush"] = flush_timing
                elif should_persist:
                    metric["input_flush_policy"] = "no_source_observations_consumer_retains_scope_barrier_v1"
            except Exception as exc:
                with session.condition:
                    metric.update(state="failed", error_type=type(exc).__name__)
                    aggregate.update(state="failed", error_type=type(exc).__name__)
                    _source_failure_retry(aggregate)
            finally:
                with session.condition:
                    # last_total_ms retains its source/validation boundary;
                    # persistence overlaps other sources and is not additive.
                    metric["source_and_persistence_ms"] = round((time.perf_counter() - started) * 1000, 3)
                    aggregate["last_completed_job"] = dict(metric)
                    session.condition.notify_all()
            if _record_source_event(key, name, "finished", dict(metric)):
                with session.condition:
                    session.durable_completed_jobs[name] = job_id


_HTTP_LOCAL = threading.local()


def _http_session():
    session = getattr(_HTTP_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        _HTTP_LOCAL.session = session
    return session


def _opening_only_snapshot(symbols, fallback, rows, *, source, session_date, received_ms):
    """Only positive dated open fields; no vendor books become broker fills."""
    size = len(symbols)
    opens, prices = np.full(size, np.nan), np.asarray(fallback, dtype=np.float64).copy()
    available = np.zeros(size, dtype=bool)
    stamps, received = np.zeros(size, dtype=np.int64), np.zeros(size, dtype=np.int64)
    for i, symbol in enumerate(symbols):
        value = rows.get(symbol)
        if value is None:
            continue
        opening, market_ms = value
        if quotes._float_or_none(opening) is None or not quotes._same_taipei_open_timestamp(market_ms, session_date) or market_ms > received_ms + 1000:
            continue
        opens[i], prices[i], stamps[i], received[i], available[i] = opening, opening, market_ms, received_ms, True
    return quotes.PriceSnapshot(prices=prices, open_prices=opens, available_mask=available, available_count=int(available.sum()), requested_count=size, source=source, timestamps_ms=received, exchange_timestamps_ms=stamps)


def _decode_finmind_opening_payload(payload, *, received_ms, session_date):
    """Decode one whole response once, independently of a model's gap scope.

    Old-dated/zero-volume rows remain rejected, not fictional no-first-print
    observations. Full-market diagnostics retain their reasons even when the
    consumer has already resolved that symbol through another provider.
    """
    if not isinstance(payload, dict):
        raise ValueError("FinMind snapshot schema is not an object")
    if payload.get("status") != 200 or not isinstance(payload.get("data"), list):
        if payload.get("status") in {400, 401, 403, 402, 429}:
            raise PermissionError("FinMind snapshot permission/quota unavailable")
        raise RuntimeError("FinMind snapshot unavailable or not entitled")
    rows, states, quality_records = {}, {}, []
    malformed_without_symbol = 0
    midnight, end = quotes._taipei_session_bounds(session_date)
    opening_gate = midnight + 9 * 60 * 60 * 1000
    for row in payload["data"]:
        symbol = None
        state = "malformed_rows"
        try:
            symbol = str(row["stock_id"])
            at = datetime.fromisoformat(str(row["date"]))
            at = at.replace(tzinfo=TAIPEI) if at.tzinfo is None else at.astimezone(TAIPEI)
            if row.get("isTrial") is True:
                state = "trial_rows"
            else:
                volume = float(row.get("total_volume", 0))
                if not math.isfinite(volume) or volume <= 0:
                    state = "no_positive_volume_rows"
                else:
                    opening = float(row["open"])
                    market_ms = int(at.timestamp() * 1000)
                    if not math.isfinite(opening) or opening <= 0:
                        state = "invalid_open_rows"
                    elif not opening_gate <= market_ms < end:
                        state = "other_session_rows"
                    elif market_ms > received_ms + 1000:
                        state = "future_timestamp_rows"
                    else:
                        rows[symbol] = (opening, market_ms)
                        state = "accepted_rows"
        except (KeyError, TypeError, ValueError, OverflowError):
            state = "malformed_rows"
        if symbol is None:
            malformed_without_symbol += 1
        else:
            quality_records.append((symbol, state))
            # A later malformed duplicate cannot erase an accepted source fact.
            states[symbol] = "accepted_rows" if symbol in rows else state
    return {"rows": rows, "states": states, "quality_records": quality_records,
            "malformed_without_symbol": malformed_without_symbol,
            "provider_payload_rows": len(payload["data"])}


class _FinMindSnapshotOwner:
    """One reusable connection and decoded full-market view per process.

    Thread-pool migration must not discard a warmed TLS connection. Concurrent
    model deltas borrow the same timed response rather than issuing duplicate
    full-market HTTP requests. This is not a new price authority: every consumer
    still projects and validates its own requested axis, using ORIGINAL receipt
    clocks. The open gate, trading date, PID and token rotation fence reuse.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.session = None
        self.identity = None
        self.value = None
        self.retry_at = 0.
        self.error = None
        self.failures = 0

    def read(self, *, force_network=False):
        queued = time.perf_counter()
        with self.lock:
            acquired = time.perf_counter()
            now = datetime.now(TAIPEI)
            token = os.environ["FINMIND_TOKEN"]
            identity = (os.getpid(), now.date().isoformat(), now.time() >= daytime(9),
                        hashlib.sha256(token.encode()).digest())
            if identity != self.identity:
                self.value, self.error, self.retry_at, self.failures = None, None, 0., 0
                if self.identity is not None and self.identity[0] != identity[0]:
                    self.session = None  # Never reuse a forked socket.
                self.identity = identity
            clock = time.monotonic()
            # Errors are never bypassed by a diagnostic force-network request.
            if self.error is not None and clock < self.retry_at:
                self.error.opening_transport_timing = {
                    **getattr(self.error, "opening_transport_timing", {}), "provider_failure_reused": True}
                raise self.error
            if not force_network and self.value is not None and clock < self.retry_at:
                return self.value, True, (acquired - queued) * 1000
            if self.session is None:
                self.session = _http_session()
            request_started = time.perf_counter()
            requested_at = datetime.now(TAIPEI)
            response = None
            try:
                response = self.session.get(
                    "https://api.finmindtrade.com/api/v4/taiwan_stock_tick_snapshot",
                    params={"data_id": ""}, headers={"Authorization": "Bearer " + token},
                    timeout=(1.5, 2.0), stream=True)
                headers_received = time.perf_counter()
                headers_at = datetime.now(TAIPEI)
                response.raise_for_status()
                body = getattr(response, "content", None)
                body_received = time.perf_counter()
                received = datetime.now(TAIPEI)
                payload = private_json.loads(body) if isinstance(body, bytes) else response.json()
                json_decoded = time.perf_counter()
                received_ms = int(received.timestamp() * 1000)
                value = _decode_finmind_opening_payload(payload, received_ms=received_ms,
                                                       session_date=now.date().isoformat())
                validated = time.perf_counter()
                coverage = {"contract": "finmind_unfiltered_market_snapshot_coverage_v1",
                    "snapshot_id": f"{os.getpid()}:{time.time_ns()}",
                    "scope_basis": "unfiltered_full_market_payload_not_model_gaps",
                    "requested_at_taipei": requested_at.isoformat(timespec="microseconds"),
                    "headers_received_at_taipei": headers_at.isoformat(timespec="microseconds"),
                    "received_at_taipei": received.isoformat(timespec="microseconds"),
                    "validated_at_taipei": datetime.now(TAIPEI).isoformat(timespec="microseconds"),
                    "valid_open_symbols": list(value["rows"]),
                    "rejected_symbols": {state: [symbol for symbol, reason in value["states"].items()
                        if reason == state] for state in sorted(set(value["states"].values()) - {"accepted_rows"})}}
                timing = {"response_schema_valid": True,
                    "provider_payload_rows": value["provider_payload_rows"],
                    "provider_update_interval_seconds": 10,
                    "http_request_to_headers_ms": round((headers_received - request_started) * 1000, 3),
                    "http_body_read_ms": round((body_received - headers_received) * 1000, 3),
                    "json_decode_ms": round((json_decoded - body_received) * 1000, 3),
                    "json_decoder": "private_json" if isinstance(body, bytes) else "response_json",
                    "full_market_validation_ms": round((validated - json_decoded) * 1000, 3),
                    "http_request_to_validated_ms": round((validated - request_started) * 1000, 3),
                    "body_bytes": len(body) if isinstance(body, bytes) else None,
                    "body_sha256": hashlib.sha256(body).hexdigest() if isinstance(body, bytes) else None,
                    "provider_snapshot_coverage": coverage}
                self.value = {**value, "timing": timing, "received_ms": received_ms,
                              "session_date": now.date().isoformat()}
                self.error, self.failures = None, 0
                self.retry_at = clock + (_snapshot_poll_interval("finmind_snapshot", now) or 10.)
            except Exception as exc:
                exc.opening_transport_timing = {
                    "http_request_to_failure_ms": round((time.perf_counter() - request_started) * 1000, 3),
                    "http_failure_observed_at_taipei": datetime.now(TAIPEI).isoformat(timespec="microseconds"),
                    "http_failure_phase": "connection_or_headers" if response is None else "status_body_or_validation",
                    "provider_response_reused": False,
                }
                self.value, self.error = None, exc
                self.failures += 1
                status = getattr(getattr(exc, "response", None), "status_code", None)
                self.retry_at = time.monotonic() + (60. if isinstance(exc, PermissionError)
                    or status in {401, 402, 403, 429} else min(15., .5 * 2 ** min(self.failures - 1, 5)))
                raise
            finally:
                if response is not None:
                    getattr(response, "close", lambda: None)()
            return self.value, False, (acquired - queued) * 1000


_FINMIND_SNAPSHOT_OWNER = _FinMindSnapshotOwner()


def fetch_finmind_opening_prices(symbols, fallback, progress, *, force_network=False):
    projection_started = time.perf_counter()
    value, reused, queue_ms = _FINMIND_SNAPSHOT_OWNER.read(force_network=force_network)
    requested = set(symbols)
    counts = Counter(reason for symbol, reason in value["quality_records"] if symbol in requested)
    quality = {key: counts[key] for key in ("malformed_rows", "trial_rows", "no_positive_volume_rows",
        "invalid_open_rows", "other_session_rows", "future_timestamp_rows", "accepted_rows")}
    quality["requested_rows"] = sum(counts.values())
    quality["malformed_rows"] += value["malformed_without_symbol"]
    result = _opening_only_snapshot(symbols, fallback, value["rows"], source="finmind:stock_tick_snapshot",
        session_date=value["session_date"], received_ms=value["received_ms"])
    result.transport_timing = {**value["timing"], "provider_row_quality": quality,
        "provider_response_reused": reused, "provider_connection_scope": "source_owner_not_pool_thread",
        "provider_owner_pid": os.getpid(), "provider_owner_queue_ms": round(queue_ms, 3),
        "consumer_total_ms": round((time.perf_counter() - projection_started) * 1000, 3)}
    progress(symbols, result)
    return result


def fugle_market_provider(market: str) -> Provider:
    if market not in {"TSE", "OTC", "TIB"}:
        raise ValueError("unsupported official opening market")

    def fetch(symbols, fallback, progress):
        started, response, phase = time.perf_counter(), None, "connection_or_headers"
        timing = {"provider_update_interval_seconds": 5, "provider_response_reused": False,
                  "provider_market": market, "provider_scope_type": "ALLBUT0999"}
        try:
            response = _http_session().get(
                "https://api.fugle.tw/marketdata/v1.0/stock/snapshot/quotes/" + market,
                params={"type": "ALLBUT0999"}, headers={"X-API-KEY": os.environ["FUGLE_API_KEY"]},
                timeout=(1.5, 2.0), stream=True)
            headers_at = time.perf_counter()
            timing["http_headers_ms"] = round((headers_at - started) * 1000, 3)
            phase = "http_status"
            response.raise_for_status()
            phase = "body_read"
            body = getattr(response, "content", None)
            body_at = time.perf_counter()
            received_ms = int(datetime.now(TAIPEI).timestamp() * 1000)
            timing.update(body_read_ms=round((body_at - headers_at) * 1000, 3),
                          body_bytes=len(body) if isinstance(body, bytes) else None)
            phase = "json_decode"
            payload = quotes._decode_quote_response(response, body)
            decoded_at = time.perf_counter()
            timing["json_decode_ms"] = round((decoded_at - body_at) * 1000, 3)
            date_text = datetime.now(TAIPEI).date().isoformat()
            phase = "schema_validation"
            if (not isinstance(payload, dict) or payload.get("date") != date_text
                    or payload.get("market") != market or not isinstance(payload.get("data"), list)):
                raise ValueError("Fugle snapshot date/market/schema mismatch")
            rows = {}
            for row in payload["data"]:
                try:
                    if row.get("type") != "EQUITY" or float(row.get("tradeVolume", 0)) <= 0:
                        continue
                    rows[str(row["symbol"])] = (float(row["openPrice"]), int(row["lastUpdated"]) // 1000)
                except (AttributeError, KeyError, TypeError, ValueError, OverflowError):
                    continue
            phase = "row_validation_projection"
            result = _opening_only_snapshot(symbols, fallback, rows, source="fugle:market_snapshot:" + market,
                session_date=date_text, received_ms=received_ms)
            timing.update(response_schema_valid=True, provider_payload_rows=len(payload["data"]),
                row_validation_projection_ms=round((time.perf_counter() - decoded_at) * 1000, 3),
                http_request_to_validated_ms=round((time.perf_counter() - started) * 1000, 3))
            result.transport_timing = timing
            progress(symbols, result)
            return result
        except Exception as exc:
            # Only safe stage metadata reaches the shared journal. A valid
            # single-symbol key is NOT proof of a snapshot entitlement.
            exc.opening_transport_timing = {**timing, "http_failure_phase": phase,
                "http_request_to_failure_ms": round((time.perf_counter() - started) * 1000, 3)}
            raise
        finally:
            if response is not None:
                getattr(response, "close", lambda: None)()
    return fetch


def optional_opening_providers() -> dict[str, Provider]:
    providers = {}
    if os.getenv("FINMIND_TOKEN", "").strip():
        providers["finmind_snapshot"] = fetch_finmind_opening_prices
    if os.getenv("FUGLE_API_KEY", "").strip():
        # The canonical regular-stock/ETF universe contains TSE and OTC, not
        # the separate innovation board. Do not spend a full-market request /
        # prewarm on a route that cannot contribute to any requested symbol.
        # The explicit TIB adapter remains available for a future scoped model.
        for market in ("TSE", "OTC"):
            providers["fugle_" + market.lower()] = fugle_market_provider(market)
    return providers


def configured_opening_providers(parquet_root: str | Path, *, broker_fetch=None) -> dict[str, Provider]:
    broker_fetch = broker_fetch or quotes.fetch_shared_day_trade_stock_snapshots

    def broker(symbols, fallback, progress):
        return broker_fetch(symbols, fallback, timeout_seconds=4, purpose="opening_signal", on_progress=progress)

    def mis(symbols, fallback, progress):
        # Return the actual MIS response, not a cache assembled from OTHER
        # providers. This keeps per-provider coverage and provenance truthful.
        # Measured 16-worker sweeps suffered timeouts/invalid responses; the
        # full 2,301-symbol 4-worker/80-channel sweep completed all 29 requests
        # without a transport failure. The smaller-batch trial multiplied
        # requests and encountered source refusal (causality is unproven).
        # This is a provider-local measured default,
        # not a global request/throughput cap; explicit source tuning is kept.
        # Its read budget remains independent of the model quorum/other feeds.
        return quotes.fetch_tw_mis_last_prices(symbols, fallback, parquet_root=parquet_root, empty_chunk_retry_attempts=0, max_parallel_requests=int(os.getenv("STOCKAGENT_TW_OPENING_PARALLEL_REQUESTS", "4") or "4"), request_timeout_seconds=float(os.getenv("STOCKAGENT_TW_OPENING_REQUEST_TIMEOUT_SECONDS", "4") or "4"), on_progress=progress)

    return {"shioaji_shared": broker, "official_mis": mis, **optional_opening_providers()}


def configured_transport_probes(*, mis_probe=None, force=False):
    def broker():
        result = quotes.fetch_shared_day_trade_stock_snapshots(["2330"], np.array([np.nan]), timeout_seconds=1.5, purpose="opening_signal")
        if result.available_count <= 0:
            raise RuntimeError("shared broker transport returned no usable response")
        return {"ready": True, "proof": "shared_engine_response_not_opening_input"}

    def optional(provider):
        def probe():
            result = provider(["2330"], np.array([np.nan]), lambda *args: None)
            return {"ready": (result.transport_timing or {}).get("response_schema_valid") is True, "proof": "validated_provider_schema_not_opening_input"}
        return probe

    return {"shioaji_shared": broker, "official_mis": mis_probe or (lambda: quotes.warm_tw_mis_quote_client(force=force)), **{name: optional(provider) for name, provider in optional_opening_providers().items()}}


OPENING_FAN_IN = OpeningPriceFanIn()
