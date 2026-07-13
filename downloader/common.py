from __future__ import annotations

from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import hashlib
import os
from pathlib import Path
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
    ) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.name = str(name)
        self._lock = threading.Lock()
        self._next_time = 0.0

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
                self._next_time = now + self.interval_seconds
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
                    next_time = now + self.interval_seconds
                    handle.seek(0)
                    handle.truncate()
                    handle.write(f"{next_time:.9f}\n")
                    handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return claimed, wait_s

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

    def wait(self) -> None:
        if self.interval_seconds <= 0.0:
            return
        while True:
            claimed, wait_s = self._claim_process_shared()
            if claimed:
                return
            if wait_s > 0.0:
                time.sleep(wait_s)

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
