from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
from queue import Queue
import socket
import threading
import tempfile
import time
from dataclasses import dataclass
from typing import TypeVar

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

from tqdm import tqdm

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


_SYSTEM_GETADDRINFO = socket.getaddrinfo
_DNS_FALLBACK_LOCK = threading.Lock()
_DNS_FALLBACK_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}
_DNS_FALLBACK_INSTALLED = False


def _dns_over_https_addresses(host: str) -> tuple[str, ...]:
    now = time.monotonic()
    with _DNS_FALLBACK_LOCK:
        cached = _DNS_FALLBACK_CACHE.get(host)
        if cached is not None and cached[0] > now:
            return cached[1]

    addresses: list[str] = []
    ttl = 60
    for record_type, answer_type in (("A", 1), ("AAAA", 28)):
        connection = http.client.HTTPSConnection("1.1.1.1", timeout=5)
        try:
            connection.request(
                "GET",
                f"/dns-query?name={host}&type={record_type}",
                headers={"Accept": "application/dns-json"},
            )
            response = connection.getresponse()
            payload = json.loads(response.read().decode("utf-8"))
            if response.status != 200 or int(payload.get("Status", -1)) != 0:
                continue
            for answer in payload.get("Answer") or []:
                if int(answer.get("type", -1)) != answer_type:
                    continue
                value = str(answer.get("data") or "").strip()
                try:
                    ipaddress.ip_address(value)
                except ValueError:
                    continue
                addresses.append(value)
                ttl = min(ttl, max(10, int(answer.get("TTL") or 60)))
        finally:
            connection.close()
    unique = tuple(dict.fromkeys(addresses))
    if not unique:
        raise socket.gaierror(f"DNS-over-HTTPS returned no address for {host}")
    with _DNS_FALLBACK_LOCK:
        _DNS_FALLBACK_CACHE[host] = (now + min(ttl, 300), unique)
    return unique


def install_dns_over_https_fallback() -> None:
    """Use DoH only after the host resolver fails; TLS still verifies the original host."""

    global _DNS_FALLBACK_INSTALLED
    if _DNS_FALLBACK_INSTALLED:
        return
    if os.getenv("STOCKAGENT_DNS_OVER_HTTPS_FALLBACK", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    def resilient_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        try:
            return _SYSTEM_GETADDRINFO(host, port, family, type, proto, flags)
        except socket.gaierror as original_error:
            text = str(host or "").strip()
            try:
                ipaddress.ip_address(text)
            except ValueError:
                pass
            else:
                raise original_error
            try:
                addresses = _dns_over_https_addresses(text)
            except Exception:
                raise original_error
            resolved: list[tuple] = []
            for address in addresses:
                address_family = socket.AF_INET6 if ":" in address else socket.AF_INET
                if family not in {0, socket.AF_UNSPEC, address_family}:
                    continue
                resolved.extend(
                    _SYSTEM_GETADDRINFO(
                        address,
                        port,
                        address_family,
                        type,
                        proto,
                        flags | socket.AI_NUMERICHOST,
                    )
                )
            if not resolved:
                raise original_error
            return resolved

    socket.getaddrinfo = resilient_getaddrinfo
    _DNS_FALLBACK_INSTALLED = True


install_dns_over_https_fallback()


@dataclass(frozen=True, slots=True)
class ProviderRateLimit:
    provider: str
    requests: float
    seconds: float
    basis: str
    source_url: str
    note: str = ""

    @property
    def requests_per_second(self) -> float:
        return max(0.001, float(self.requests) / float(self.seconds))

    @property
    def interval_seconds(self) -> float:
        return 1.0 / self.requests_per_second


DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND = 10.0


PROVIDER_RATE_LIMITS: dict[str, ProviderRateLimit] = {
    "okx_history_candles": ProviderRateLimit(
        provider="okx_history_candles",
        requests=20,
        seconds=2,
        basis="official endpoint limit; IP",
        source_url="https://www.okx.com/docs-v5/en/",
        note="GET /api/v5/market/history-candles",
    ),
    "bybit_public_rest": ProviderRateLimit(
        provider="bybit_public_rest",
        requests=600,
        seconds=5,
        basis="official HTTP IP limit",
        source_url="https://bybit-exchange.github.io/docs/v5/rate-limit",
        note="Public market data shares the api.bybit.com IP bucket.",
    ),
    "frankfurter_public": ProviderRateLimit(
        provider="frankfurter_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://frankfurter.dev/",
        note="No daily/monthly caps are published; keep a configurable client-side cap.",
    ),
    "tw_public": ProviderRateLimit(
        provider="tw_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://openapi.twse.com.tw/",
        note=(
            "Default 10 req/s is a stockAgent policy, not an official "
            "TWSE/TPEx/data.gov.tw limit."
        ),
    ),
    "yahoo_finance": ProviderRateLimit(
        provider="yahoo_finance",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://finance.yahoo.com/",
        note=(
            "CLI default is 10 req/s when no interval is supplied. Large TW "
            "fallback bootstraps explicitly use a slower 1.5-second interval."
        ),
    ),
    "alpaca_market_data_basic": ProviderRateLimit(
        provider="alpaca_market_data_basic",
        requests=200,
        seconds=60,
        basis="official Basic historical market-data limit; account",
        source_url="https://docs.alpaca.markets/docs/about-market-data-api",
        note="Algo Trader Plus supports 10,000 requests per minute via downloader configuration.",
    ),
}


class SharedRateLimiter:
    def __init__(
        self,
        interval_seconds: float,
        *,
        name: str = "rate-limit",
        state_dir: str | Path | None = None,
        on_claim: Callable[[], None] | None = None,
        on_caller_claim: Callable[[], None] | None = None,
    ) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.name = str(name)
        self._on_claim = on_claim
        # The stable dispatcher owns slot timing, while some observers need
        # the identity/context of the data worker that received that slot.
        # Keep these callbacks separate so pacing remains single-leader and
        # request attribution can use worker-local context safely.
        self._on_caller_claim = on_caller_claim
        self._lock = threading.Lock()
        self._next_time = 0.0
        # A stable FIFO dispatcher owns the local schedule.  Handing leadership
        # from one data worker to another after every ticket lets the newly
        # released worker enter provider parsing/network setup before the next
        # waiter gets CPU.  Under OpenBB's many blocking portals that GIL/thread
        # handoff cut independent providers to roughly half their configured
        # request-start rate.  The dispatcher never performs provider work: it
        # only claims the host-global slot, records it, and releases one waiter.
        self._dispatch_condition = threading.Condition()
        self._dispatch_queue: deque[threading.Event] = deque()
        self._dispatcher_thread: threading.Thread | None = None
        # Granted-slot telemetry can persist JSON and contend with hundreds of
        # provider workers. It must never run in the cadence-owning dispatcher
        # thread, otherwise an observer lock turns an 8-10 req/s limiter into
        # an accidental ~4 req/s limiter. A single ordered observer preserves
        # every claim without delaying tickets or creating one thread per slot.
        self._claim_observer_queue: Queue[None] = Queue()
        self._claim_observer_thread: threading.Thread | None = None
        # Request cadence telemetry must be recorded at the grant boundary,
        # not when the asynchronous diagnostic observer eventually persists
        # it.  Under many provider workers that observer may lag while the
        # dispatcher continues issuing perfectly paced slots; timestamping the
        # delayed callbacks would falsely report low API utilization.
        self._grant_session_started_at = time.time()
        self._grant_times: deque[float] = deque()
        self._grant_total = 0

        root_value = state_dir or os.environ.get("STOCKAGENT_RATE_LIMIT_DIR")
        if root_value is None:
            uid = getattr(os, "getuid", lambda: "user")()
            root_value = Path(tempfile.gettempdir()) / f"stockagent-rate-limits-{uid}"
        root = Path(root_value)
        digest = hashlib.sha256(self.name.encode("utf-8")).hexdigest()[:20]
        self._state_path = root / f"{digest}.state"

    def _claim_process_shared(self) -> tuple[bool, float]:
        """Claim the current slot only when it is ready.

        A caller that is too early gets a delay but does not reserve a future
        slot.  It must sleep and retry, which lets a concurrent ``defer()``
        extend the provider-wide cooldown before the request is sent.
        """
        if fcntl is None:
            with self._lock:
                now = time.monotonic()
                wait_s = max(0.0, self._next_time - now)
                if wait_s > 0.0:
                    return False, wait_s
                self._next_time = self._next_deadline(self._next_time, now)
                return True, 0.0

        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._state_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read().strip()
                try:
                    next_time = float(raw) if raw else 0.0
                except ValueError:
                    next_time = 0.0
                now = time.monotonic()
                # /tmp may survive a container or host restart while CLOCK_MONOTONIC
                # restarts from zero. Ignore an implausibly distant stale reservation.
                if next_time - now > max(300.0, self.interval_seconds * 100_000.0):
                    next_time = 0.0
                wait_s = max(0.0, next_time - now)
                claimed = wait_s <= 0.0
                if claimed:
                    next_time = self._next_deadline(next_time, now)
                    handle.seek(0)
                    handle.truncate()
                    handle.write(f"{next_time:.9f}\n")
                    handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return claimed, wait_s

    def _next_deadline(self, previous_deadline: float, now: float) -> float:
        """Advance an absolute schedule without accumulating wake-up jitter.

        Reset after a full missed interval so a suspended process never emits
        a catch-up burst.  For normal sub-interval scheduler/GIL latency, keep
        the original cadence: otherwise adding the latency to every ticket
        permanently lowers a 10 req/s policy to roughly 8 req/s under load.
        """
        interval = self.interval_seconds
        if interval <= 0.0:
            return now
        if previous_deadline <= 0.0 or now - previous_deadline >= interval:
            return now + interval
        return previous_deadline + interval

    def _defer_process_shared(self, seconds: float) -> None:
        if fcntl is None:
            with self._lock:
                now = time.monotonic()
                self._next_time = max(self._next_time, now + seconds)
            return

        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._state_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read().strip()
                try:
                    next_time = float(raw) if raw else 0.0
                except ValueError:
                    next_time = 0.0
                now = time.monotonic()
                if next_time - now > max(300.0, self.interval_seconds * 100_000.0):
                    next_time = 0.0
                next_time = max(next_time, now + seconds)
                handle.seek(0)
                handle.truncate()
                handle.write(f"{next_time:.9f}\n")
                handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _ensure_dispatcher_locked(self) -> None:
        if self._dispatcher_thread is not None and self._dispatcher_thread.is_alive():
            return
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_waiters,
            name=f"{self.name}-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()

    def _ensure_claim_observer_locked(self) -> None:
        if self._on_claim is None:
            return
        if (
            self._claim_observer_thread is not None
            and self._claim_observer_thread.is_alive()
        ):
            return
        self._claim_observer_thread = threading.Thread(
            target=self._observe_claims,
            name=f"{self.name}-claim-observer",
            daemon=True,
        )
        self._claim_observer_thread.start()

    def _observe_claims(self) -> None:
        """Record granted slots independently from request-slot cadence."""
        while True:
            self._claim_observer_queue.get()
            try:
                self._notify_claim()
            finally:
                self._claim_observer_queue.task_done()

    def _dispatch_waiters(self) -> None:
        """Grant local waiters in FIFO order from one scheduling-only thread."""
        while True:
            with self._dispatch_condition:
                while not self._dispatch_queue:
                    self._dispatch_condition.wait()
                ticket = self._dispatch_queue[0]

            claimed, wait_s = self._claim_process_shared()
            if not claimed:
                if wait_s > 0.0:
                    time.sleep(wait_s)
                continue

            with self._dispatch_condition:
                # Only this dispatcher removes local tickets.  Keeping the
                # identity check makes a future cancellation extension safe.
                if not self._dispatch_queue or self._dispatch_queue[0] is not ticket:
                    continue
                self._dispatch_queue.popleft()
                self._record_grant_locked()
            if self._on_claim is not None:
                with self._dispatch_condition:
                    self._ensure_claim_observer_locked()
                self._claim_observer_queue.put(None)
            ticket.set()

    def wait(self) -> None:
        if self.interval_seconds <= 0.0:
            with self._dispatch_condition:
                self._record_grant_locked()
            self._notify_claim()
            self._notify_caller_claim()
            return
        ticket = threading.Event()
        with self._dispatch_condition:
            self._dispatch_queue.append(ticket)
            self._ensure_dispatcher_locked()
            self._dispatch_condition.notify()
        ticket.wait()
        self._notify_caller_claim()

    def _notify_claim(self) -> None:
        """Publish one granted request slot without risking the data request."""
        if self._on_claim is None:
            return
        try:
            self._on_claim()
        except Exception:
            # Rate telemetry is diagnostic. A broken observer must never turn
            # a successfully paced provider call into a failed data request.
            return

    def _notify_caller_claim(self) -> None:
        """Publish a granted slot from the waiting data-worker context."""
        if self._on_caller_claim is None:
            return
        try:
            self._on_caller_claim()
        except Exception:
            # Like dispatcher telemetry, attribution must never fail a data
            # request that already received its process-shared slot.
            return

    def pending_waiters(self) -> int:
        """Return local callers queued for a request-start ticket."""
        with self._dispatch_condition:
            return len(self._dispatch_queue)

    def _record_grant_locked(self) -> None:
        now = time.time()
        self._grant_times.append(now)
        self._grant_total += 1
        cutoff = now - 60.0
        while self._grant_times and self._grant_times[0] < cutoff:
            self._grant_times.popleft()

    def grant_activity(self, now: float | None = None) -> dict[str, float | int]:
        """Return dispatcher-boundary activity without observer timestamp lag."""
        current = time.time() if now is None else float(now)
        with self._dispatch_condition:
            cutoff = current - 60.0
            while self._grant_times and self._grant_times[0] < cutoff:
                self._grant_times.popleft()
            window_seconds = min(
                60.0,
                max(0.001, current - self._grant_session_started_at),
            )
            return {
                "grants_total": int(self._grant_total),
                "grants_last_60s": len(self._grant_times),
                "window_seconds": window_seconds,
                "pending_claim_observations": self._claim_observer_queue.qsize(),
            }

    def flush_claim_observations(self) -> None:
        """Wait until all already-granted slots have reached telemetry.

        Normal requests never call this method. A provider quota response uses
        it once so the durable claims-at-limit evidence includes the request
        that produced that response, while ordinary slot cadence remains fully
        decoupled from diagnostic persistence.
        """
        if threading.current_thread() is self._claim_observer_thread:
            return
        self._claim_observer_queue.join()

    def defer(self, seconds: float) -> None:
        delay = max(0.0, float(seconds))
        if delay > 0.0:
            self._defer_process_shared(delay)


def provider_rate_limit(provider: str) -> ProviderRateLimit:
    key = str(provider).strip().lower()
    if key in PROVIDER_RATE_LIMITS:
        return PROVIDER_RATE_LIMITS[key]
    return ProviderRateLimit(
        provider=key or "unspecified",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="n/a",
        note="Unregistered providers default to 10 requests per second.",
    )


def resolve_request_interval(
    provider: str,
    requested_interval: float | None = None,
    *,
    env_var: str | None = None,
    allow_zero: bool = False,
) -> float:
    profile = provider_rate_limit(provider)
    raw: float | None = requested_interval
    if env_var:
        text = os.environ.get(env_var)
        if text not in {None, ""}:
            raw = float(text)
    floor = profile.interval_seconds
    if raw is None:
        return floor
    value = max(0.0, float(raw))
    if allow_zero and value == 0.0:
        return 0.0
    if value < floor:
        print(
            f"[rate-limit] provider={profile.provider} requested_interval={value:.6f}s "
            f"below_policy_interval={floor:.6f}s; clamped basis={profile.basis}",
            flush=True,
        )
        return floor
    return value


def describe_rate_limit(provider: str, interval_seconds: float) -> str:
    profile = provider_rate_limit(provider)
    rps = float("inf") if interval_seconds <= 0.0 else 1.0 / float(interval_seconds)
    return (
        f"provider={profile.provider} interval={interval_seconds:.6f}s "
        f"rps={rps:.2f} source={profile.source_url} basis={profile.basis}"
    )


def resolve_end_date(value: str) -> str:
    text = value.strip().lower()
    if text in {"today", "now"}:
        return date.today().isoformat()
    return value.strip()


def run_parallel_tasks(
    items: Iterable[TItem],
    worker: Callable[[TItem], TResult],
    *,
    max_workers: int,
    desc: str,
    unit: str = "item",
    on_error: Callable[[TItem, Exception], TResult] | None = None,
) -> list[TResult]:
    item_list = list(items)
    if not item_list:
        return []

    results: list[TResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        futures = {executor.submit(worker, item): item for item in item_list}
        progress = tqdm(total=len(futures), desc=desc, unit=unit)
        try:
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    if on_error is None:
                        raise
                    result = on_error(item, exc)
                results.append(result)
                progress.update(1)
        finally:
            progress.close()

    return results
