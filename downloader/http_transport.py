"""Shared HTTP reliability policy for provider-specific download adapters.

This module owns transport concerns only: request-start pacing, bounded retry,
``Retry-After`` handling, timeouts, error redaction, and telemetry.  It does not
interpret provider payloads or decide whether a provider-specific response is
complete; those semantics stay in each downloader adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import time
from typing import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlsplit, urlunsplit
from urllib.request import Request, urlopen

try:
    from .common import SharedRateLimiter, provider_rate_limit, retry_delay_seconds
except ImportError:  # direct ``python downloader/<script>.py`` execution
    from common import SharedRateLimiter, provider_rate_limit, retry_delay_seconds


DEFAULT_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


def sanitized_url(url: str) -> str:
    """Remove query/fragment data so API keys never reach errors or receipts."""

    parts = urlsplit(str(url))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _sanitized_error_body(body: bytes, url: str) -> bytes:
    """Redact query values if an upstream error echoes the request URL."""

    preview = body[:4096].decode("utf-8", errors="replace")
    parts = urlsplit(str(url))
    preview = preview.replace(str(url), sanitized_url(url))
    for _name, value in parse_qsl(parts.query, keep_blank_values=False):
        if value:
            preview = preview.replace(value, "[REDACTED]")
    return preview.encode("utf-8", errors="replace")


@dataclass(frozen=True, slots=True)
class HttpRequestPolicy:
    provider: str
    timeout_seconds: float = 60.0
    max_retries: int = 5
    retry_base_seconds: float = 1.0
    retry_cap_seconds: float = 30.0
    retryable_statuses: frozenset[int] = field(
        default_factory=lambda: DEFAULT_RETRYABLE_STATUSES
    )


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]
    attempts: int


class HttpStatusError(RuntimeError):
    def __init__(self, status: int, url: str, body: bytes = b"") -> None:
        sanitized_body = _sanitized_error_body(body, url)
        preview = sanitized_body.decode("utf-8", errors="replace").strip()
        message = f"HTTP {int(status)} for {sanitized_url(url)}"
        if preview:
            message += f": {preview}"
        super().__init__(message)
        self.status = int(status)
        self.url = sanitized_url(url)
        self.body = sanitized_body


class ResilientHttpTransport:
    """One reusable transport per independent provider-limit bucket."""

    def __init__(
        self,
        policy: HttpRequestPolicy,
        *,
        limiter: SharedRateLimiter | None = None,
        opener: Callable[..., object] = urlopen,
        sleeper: Callable[[float], None] = time.sleep,
        retry_delay: Callable[..., float] = retry_delay_seconds,
        on_attempt: Callable[[str], None] | None = None,
    ) -> None:
        self.policy = policy
        profile = provider_rate_limit(policy.provider)
        self.limiter = limiter or SharedRateLimiter(
            profile.interval_seconds,
            name=profile.provider,
        )
        self._opener = opener
        self._sleep = sleeper
        self._retry_delay = retry_delay
        self._on_attempt = on_attempt

    def request_bytes(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        method: str = "GET",
        data: bytes | None = None,
        cost: float = 1.0,
        accepted_statuses: frozenset[int] = frozenset(),
    ) -> HttpResponse:
        retries = max(0, int(self.policy.max_retries))
        request = Request(
            url,
            data=data,
            method=str(method),
            headers=dict(headers or {}),
        )
        for attempt in range(retries + 1):
            self.limiter.wait(cost=cost)
            if self._on_attempt is not None:
                self._on_attempt(self.policy.provider)
            try:
                response = self._opener(
                    request,
                    timeout=self.policy.timeout_seconds,
                )
                with response:
                    body = response.read()
                    status = int(getattr(response, "status", 200))
                    response_headers = {
                        str(key): str(value)
                        for key, value in getattr(response, "headers", {}).items()
                    }
                return HttpResponse(status, body, response_headers, attempt + 1)
            except HTTPError as exc:
                body = exc.read()
                status = int(exc.code)
                headers_map = {
                    str(key): str(value)
                    for key, value in (exc.headers.items() if exc.headers else [])
                }
                if status in accepted_statuses:
                    return HttpResponse(status, body, headers_map, attempt + 1)
                if status not in self.policy.retryable_statuses or attempt >= retries:
                    raise HttpStatusError(status, url, body) from exc
                retry_after = next(
                    (
                        value
                        for key, value in headers_map.items()
                        if key.casefold() == "retry-after"
                    ),
                    None,
                )
                cooldown = self._retry_delay(
                    attempt + 1,
                    base=self.policy.retry_base_seconds,
                    cap=self.policy.retry_cap_seconds,
                    retry_after=retry_after,
                )
                # The limiter owns provider-wide cooldown.  Sleeping this
                # worker as well would apply the same backoff twice and leave
                # the official bucket unnecessarily idle.
                self.limiter.defer(cooldown)
            except (URLError, TimeoutError, OSError):
                if attempt >= retries:
                    raise
                # Transport failures do not imply that every worker sharing
                # the provider bucket must stop, so back off only this caller.
                self._sleep(
                    self._retry_delay(
                        attempt + 1,
                        base=self.policy.retry_base_seconds,
                        cap=self.policy.retry_cap_seconds,
                    )
                )
        raise RuntimeError(f"HTTP retry loop exhausted for {sanitized_url(url)}")
