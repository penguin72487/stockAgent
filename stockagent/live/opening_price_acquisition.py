"""Session-fenced, single-flight fan-in for model opening inputs (not fills)."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, time as daytime
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from typing import Callable
from zoneinfo import ZoneInfo

import numpy as np
import requests

from stockagent.live import quote_provider as quotes

Progress = Callable[[list[str], quotes.PriceSnapshot], None]
Provider = Callable[[list[str], np.ndarray, Progress], quotes.PriceSnapshot]
TAIPEI = ZoneInfo("Asia/Taipei")
_TELEMETRY_LOCK = threading.Lock()


def _source_failure_retry(metric: dict, *, status: int | None = None) -> None:
    """Provider-local failure isolation; accepted rows and other sources continue."""
    failures = int(metric.get("consecutive_source_failures", 0)) + 1
    metric["consecutive_source_failures"] = failures
    delay = 60.0 if status in {401, 402, 403, 429} else min(
        15.0, 0.5 * 2 ** min(failures - 1, 5)
    )
    metric["retry_delay_seconds"] = delay
    metric["retry_after_monotonic"] = time.monotonic() + delay


def _record_source_event(key, provider, event, details) -> bool:
    """Durable, safe metadata only; slow/failed cases remain in the daily log."""
    path = quotes._tw_mis_opening_receipt_path(key[1]).with_suffix(".acquisition.jsonl")
    payload = {"schema_version": 1, "session_date": key[1], "parquet_root": key[0], "provider": provider, "event": event, "observed_at_taipei": datetime.now(TAIPEI).isoformat(timespec="microseconds"), "measurement_boundary": "opening_source_acquisition_not_broker_fill", **details}
    try:
        with _TELEMETRY_LOCK:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, allow_nan=False) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
        return True
    except (OSError, TypeError, ValueError):
        return False


@dataclass
class _Session:
    condition: threading.Condition = field(default_factory=threading.Condition)
    inflight: dict[str, set[str]] = field(default_factory=dict)
    metrics: dict[str, dict] = field(default_factory=dict)
    generation: int = 0


class OpeningPriceFanIn:
    """Provider workers continue independently after a caller reaches quorum.

    No network I/O while holding the session/cache locks. Overlapping model
    universes borrow in-flight work; only universe deltas submit extra work.
    Returned arrays are detached from the mutable acquisition state.
    """

    def __init__(self, *, workers: int = 8):
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
        """Diagnostic/shutdown boundary only, never part of model quorum wait."""
        deadline = time.monotonic() + timeout_seconds
        with self._lock:
            sessions = list(self._sessions.values())
        for session in sessions:
            with session.condition:
                while any(session.inflight.values()):
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    session.condition.wait(timeout=remaining)
        return True

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
        return {"ready": True, "source": "parallel_opening_sources", "proof": "responding_transport_not_opening_price_readiness", "session_date": session_date, "elapsed_ms": round((time.perf_counter() - started) * 1000, 3), "providers": metrics}

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
    ) -> quotes.PriceSnapshot:
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("opening universe must be nonempty and unique")
        fallback = np.asarray(fallback, dtype=np.float64)
        if fallback.shape != (len(symbols),) or not 0 <= minimum_coverage <= 1:
            raise ValueError("invalid opening fallback/coverage")
        now = datetime.now(TAIPEI)
        if now.time() < daytime(9):
            raise RuntimeError("opening acquisition cannot start before 09:00 Taipei")
        date_text = now.date().isoformat()
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
        before = quotes.tw_session_open_observed_mask(observed, session_date=date_text)
        resolved = before | (quotes.tw_session_no_open_print_mask(observed, session_date=date_text) if resolve_no_prints else False)
        if not force_refresh and int(resolved.sum()) >= required:
            quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=date_text)
            observed.transport_timing = {**(observed.transport_timing or {}), "opening_acquisition_policy": "parallel_incremental_fan_in_v1", "opening_cache_ready": True, "opening_quorum_wait_ms": round((time.perf_counter() - started) * 1000, 3)}
            return observed
        # Record the entire requested universe even if only an early partial
        # batch reaches quorum and a later provider disconnects completely.
        quotes.seed_tw_opening_snapshot_cache(symbols, quotes.PriceSnapshot(prices=fallback, source="opening_request_scope", available_mask=np.zeros(len(symbols), dtype=bool)), parquet_root=key[0], session_date=date_text, persist=False)
        with session.condition:
            generation = session.generation
            # Initial pass requests the complete universe from EVERY provider,
            # even if another provider wins immediately. Later passes retry gaps.
            jobs = []
            for name, provider in providers.items():
                previous = session.metrics.get(name, {})
                if time.monotonic() < previous.get("retry_after_monotonic", 0):
                    continue
                active = session.inflight.setdefault(name, set())
                indices = [i for i, symbol in enumerate(symbols) if symbol not in active and (name not in session.metrics or not resolved[i])]
                if not indices:
                    continue
                requested = [symbols[i] for i in indices]
                active.update(requested)
                metric = session.metrics.setdefault(name, {"attempts": 0, "requested_count": 0, "state": "pending"})
                metric["attempts"] += 1
                metric["requested_count"] += len(requested)
                metric["state"] = "running"
                jobs.append((name, provider, requested, fallback[indices].copy()))
            # Enqueue all sources under a short lock. Workers perform no I/O
            # until the lock is released; a fast winner cannot cancel a loser.
            for name, provider, requested, values in jobs:
                self._pool.submit(self._run, session, generation, key, name, provider, requested, values, started)
        deadline = time.monotonic() + max(0.01, timeout_seconds)
        while True:
            with session.condition:
                if session.generation != generation or self._closed:
                    raise RuntimeError("opening acquisition session was superseded")
                observed = cache_reader()
                opens = quotes.tw_session_open_observed_mask(observed, session_date=date_text)
                resolved = opens | (quotes.tw_session_no_open_print_mask(observed, session_date=date_text) if resolve_no_prints else False)
                ready = int(resolved.sum()) >= required
                pending = any(set(symbols).intersection(values) for values in session.inflight.values())
                remaining = deadline - time.monotonic()
                if ready or not pending or remaining <= 0:
                    metrics = {name: dict(metric) for name, metric in session.metrics.items()}
                    break
                session.condition.wait(timeout=remaining)
        # Persist the exact accepted input set before it can drive publication.
        quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=date_text)
        observed.transport_timing = {
            **(observed.transport_timing or {}),
            "opening_acquisition_policy": "parallel_incremental_fan_in_v1",
            "opening_cache_ready": False,
            "opening_source_pass_ms": round((time.perf_counter() - started) * 1000, 3),
            "opening_quorum_wait_ms": round((time.perf_counter() - started) * 1000, 3),
            "opening_valid_before": int(before.sum()), "opening_valid_after": int(opens.sum()),
            "opening_resolved_count": int(resolved.sum()), "opening_required_count": required,
            "opening_quorum_ready": ready, "opening_background_pending": pending,
            "opening_providers": metrics,
            "opening_source_errors": [f"{name}_{row['error_type']}" for name, row in metrics.items() if row.get("error_type")],
        }
        observed.transport_timing["opening_telemetry_recorded"] = _record_source_event(key, "merged", "quorum" if ready else "waiting_source", {"requested_count": len(symbols), "valid_open_count": int(opens.sum()), "resolved_count": int(resolved.sum()), "required_count": required, "elapsed_ms": observed.transport_timing["opening_quorum_wait_ms"], "background_pending": pending, "providers": metrics})
        return observed

    def _run(self, session, generation, key, name, provider, symbols, fallback, caller_started):
        started = time.perf_counter()
        with session.condition:
            metric = session.metrics[name]
            metric["last_started_at_taipei"] = datetime.now(TAIPEI).isoformat(timespec="microseconds")
            metric["last_dispatch_ms"] = round((started - caller_started) * 1000, 3)
            metric.pop("error_type", None)
        _record_source_event(key, name, "started", {"requested_count": len(symbols), "requested_symbols_sha256": hashlib.sha256("\n".join(symbols).encode()).hexdigest(), "dispatch_ms": metric["last_dispatch_ms"]})
        last_progress = [None]

        def progress(requested, snapshot):
            with session.condition:
                if session.generation != generation or datetime.now(TAIPEI).date().isoformat() != key[1] or self._closed:
                    return
                if not set(requested).issubset(symbols) or len(requested) != len(snapshot.prices):
                    raise ValueError("provider progress does not match requested universe")
                quotes.seed_tw_opening_snapshot_cache(requested, snapshot, parquet_root=key[0], session_date=key[1], persist=False)
                last_progress[0] = snapshot
                is_first = "first_batch_ms" not in metric
                metric.setdefault("first_batch_ms", round((time.perf_counter() - started) * 1000, 3))
                if quotes.tw_session_open_observed_mask(snapshot, session_date=key[1]).any():
                    metric.setdefault("first_valid_open_ms", round((time.perf_counter() - started) * 1000, 3))
                    metric.setdefault("first_valid_open_at_taipei", datetime.now(TAIPEI).isoformat(timespec="microseconds"))
                session.condition.notify_all()
            if is_first:
                _record_source_event(key, name, "first_batch", {"requested_count": len(symbols), "response_count": int(snapshot.available_count), "valid_open_count": int(quotes.tw_session_open_observed_mask(snapshot, session_date=key[1]).sum()), "elapsed_ms": metric["first_batch_ms"]})
        try:
            snapshot = provider(symbols, fallback, progress)
            if last_progress[0] is not snapshot:
                progress(symbols, snapshot)
            with session.condition:
                transport = dict(snapshot.transport_timing or {})
                responses = int(snapshot.available_count)
                valid_opens = int(quotes.tw_session_open_observed_mask(snapshot, session_date=key[1]).sum())
                no_prints = int(quotes.tw_session_no_open_print_mask(snapshot, session_date=key[1]).sum())
                state = "complete" if valid_opens or no_prints else "waiting_source"
                if responses == 0:
                    state = "failed" if int(transport.get("mis_failed_request_count", 0)) else "waiting_source"
                metric.update(state=state, valid_open_count=valid_opens,
                              response_count=responses, no_print_count=no_prints,
                              transport_timing=transport)
                if responses == 0:
                    _source_failure_retry(metric)
                elif valid_opens or no_prints:
                    metric["consecutive_source_failures"] = 0
                    metric.pop("retry_delay_seconds", None)
        except Exception as exc:
            # Never write provider exception text/HTTP headers/tokens to receipts.
            with session.condition:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                metric.update(state="failed", error_type=type(exc).__name__)
                if status is not None:
                    metric["http_status"] = int(status)
                _source_failure_retry(metric, status=403 if isinstance(exc, PermissionError) else status)
        finally:
            with session.condition:
                metric["last_total_ms"] = round((time.perf_counter() - started) * 1000, 3)
                if name.startswith("finmind"):
                    metric["retry_after_monotonic"] = max(metric.get("retry_after_monotonic", 0), time.monotonic() + 10)
                elif name.startswith("fugle"):
                    metric["retry_after_monotonic"] = max(metric.get("retry_after_monotonic", 0), time.monotonic() + 5)
                session.inflight[name].difference_update(symbols)
                should_persist = session.generation == generation and datetime.now(TAIPEI).date().isoformat() == key[1] and not self._closed
                session.condition.notify_all()
            # Fsync/JSON serialization is not a condition-lock dependency of
            # another provider or model. The canonical cache writer still owns
            # generation checking, serialization and durable publication.
            try:
                if should_persist:
                    quotes.persist_tw_opening_snapshot_cache(parquet_root=key[0], session_date=key[1])
            except Exception as exc:
                with session.condition:
                    metric.update(state="failed", error_type=type(exc).__name__)
                    _source_failure_retry(metric)
            finally:
                with session.condition:
                    session.condition.notify_all()
            _record_source_event(key, name, "finished", dict(metric))


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


def fetch_finmind_opening_prices(symbols, fallback, progress):
    response = _http_session().get("https://api.finmindtrade.com/api/v4/taiwan_stock_tick_snapshot", params={"data_id": ""}, headers={"Authorization": "Bearer " + os.environ["FINMIND_TOKEN"]}, timeout=(1.5, 2.0))
    received_ms = int(datetime.now(TAIPEI).timestamp() * 1000)
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != 200 or not isinstance(payload.get("data"), list):
        if payload.get("status") in {401, 403, 402}:
            raise PermissionError("FinMind snapshot permission/quota unavailable")
        raise RuntimeError("FinMind snapshot unavailable or not entitled")
    rows = {}
    for row in payload["data"]:
        try:
            at = datetime.fromisoformat(str(row["date"]))
            at = at.replace(tzinfo=TAIPEI) if at.tzinfo is None else at.astimezone(TAIPEI)
            # Opening price is published separately from last/previous close.
            if row.get("isTrial") is True or float(row.get("total_volume", 0)) <= 0:
                continue
            rows[str(row["stock_id"])] = (float(row["open"]), int(at.timestamp() * 1000))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
    result = _opening_only_snapshot(symbols, fallback, rows, source="finmind:stock_tick_snapshot", session_date=datetime.now(TAIPEI).date().isoformat(), received_ms=received_ms)
    result.transport_timing = {"response_schema_valid": True, "provider_payload_rows": len(payload["data"]), "provider_update_interval_seconds": 10}
    progress(symbols, result)
    return result


def fugle_market_provider(market: str) -> Provider:
    def fetch(symbols, fallback, progress):
        response = _http_session().get("https://api.fugle.tw/marketdata/v1.0/stock/snapshot/quotes/" + market, headers={"X-API-KEY": os.environ["FUGLE_API_KEY"]}, timeout=(1.5, 2.0))
        received_ms = int(datetime.now(TAIPEI).timestamp() * 1000)
        response.raise_for_status()
        payload = response.json()
        date_text = datetime.now(TAIPEI).date().isoformat()
        if payload.get("date") != date_text or payload.get("market") != market or not isinstance(payload.get("data"), list):
            raise ValueError("Fugle snapshot date/market/schema mismatch")
        rows = {}
        for row in payload["data"]:
            try:
                if row.get("type") != "EQUITY" or float(row.get("tradeVolume", 0)) <= 0:
                    continue
                rows[str(row["symbol"])] = (float(row["openPrice"]), int(row["lastUpdated"]) // 1000)
            except (KeyError, TypeError, ValueError, OverflowError):
                continue
        result = _opening_only_snapshot(symbols, fallback, rows, source="fugle:market_snapshot:" + market, session_date=date_text, received_ms=received_ms)
        result.transport_timing = {"response_schema_valid": True, "provider_payload_rows": len(payload["data"]), "provider_update_interval_seconds": 5}
        progress(symbols, result)
        return result
    return fetch


def optional_opening_providers() -> dict[str, Provider]:
    providers = {}
    if os.getenv("FINMIND_TOKEN", "").strip():
        providers["finmind_snapshot"] = fetch_finmind_opening_prices
    if os.getenv("FUGLE_API_KEY", "").strip():
        for market in ("TSE", "OTC", "TIB"):
            providers["fugle_" + market.lower()] = fugle_market_provider(market)
    return providers


def configured_opening_providers(parquet_root: str | Path, *, broker_fetch=None) -> dict[str, Provider]:
    broker_fetch = broker_fetch or quotes.fetch_shared_day_trade_stock_snapshots

    def broker(symbols, fallback, progress):
        return broker_fetch(symbols, fallback, timeout_seconds=4, purpose="opening_signal", on_progress=progress)

    def mis(symbols, fallback, progress):
        # Return the actual MIS response, not a cache assembled from OTHER
        # providers. This keeps per-provider coverage and provenance truthful.
        return quotes.fetch_tw_mis_last_prices(symbols, fallback, parquet_root=parquet_root, empty_chunk_retry_attempts=0, max_parallel_requests=int(os.getenv("STOCKAGENT_TW_OPENING_PARALLEL_REQUESTS", "16") or "16"), request_timeout_seconds=float(os.getenv("STOCKAGENT_TW_OPENING_REQUEST_TIMEOUT_SECONDS", "1.5") or "1.5"), on_progress=progress)

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
