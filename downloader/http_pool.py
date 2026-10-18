"""Bounded keep-alive transport for public, non-redirecting HTTP GET APIs.

The provider adapter still owns *every* retry and rate-limit ticket. urllib3
must never replay a failed request internally. Bodies are fully read before
returning the connection, preserving the existing ``urlopen``/``json.load``
call sites without leaving sockets pinned during parsing or backoff.

This is deliberately opt-in, not a process-global urllib patch. Signed writes,
redirecting archives and custom/proxy handlers keep their existing transport.
"""

from __future__ import annotations

import atexit
import io
import os
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, getproxies, proxy_bypass, urlopen

import urllib3


class _Response(io.BytesIO):
    def __init__(self, body: bytes, *, status: int, headers: object, url: str) -> None:
        super().__init__(body)
        self.status = self.code = status
        self.headers = headers
        self.url = url

    def getcode(self) -> int:
        return self.status

    def geturl(self) -> str:
        return self.url

    def info(self) -> object:
        return self.headers


class PooledUrlOpener:
    """One thread-safe, bounded pool per process; no hidden retries/redirects."""

    def __init__(self, *, origins: int = 16, connections_per_origin: int = 32) -> None:
        if origins < 1 or connections_per_origin < 1:
            raise ValueError("HTTP pool bounds must be positive")
        self._origins = int(origins)
        self._connections = int(connections_per_origin)
        self._lock = threading.Lock()
        self._pid = os.getpid()
        self._pool: urllib3.PoolManager | None = None
        # Match urllib's environment/OS proxy selection. Custom proxy handling
        # is not reimplemented; proxy requests retain the stdlib path.
        self._proxies = getproxies()

    def _after_fork(self) -> None:
        # Never inherit an acquired lock or use a parent's live TCP connection.
        self._lock = threading.Lock()
        self._pid = os.getpid()
        # Do not acquire/clear inherited pool locks; another parent thread may
        # have owned one at fork. Drop the inherited manager instead.
        self._pool = None

    def _manager(self) -> urllib3.PoolManager:
        with self._lock:
            if self._pool is None:
                self._pool = urllib3.PoolManager(
                    num_pools=self._origins, maxsize=self._connections, block=True,
                )
            return self._pool

    def close(self) -> None:
        if os.getpid() != self._pid:
            self._after_fork()
        with self._lock:
            pool, self._pool = self._pool, None
        if pool is not None:
            pool.clear()

    def __call__(self, request: Request | str, *, timeout: float = 60.0) -> _Response:
        req = Request(request) if isinstance(request, str) else request
        parts = urlsplit(req.full_url)
        if (
            req.get_method() not in {"GET", "HEAD"} or req.data is not None
            or parts.scheme not in {"https", "http"}
            or parts.username is not None or parts.password is not None
            or (self._proxies.get(parts.scheme) and not proxy_bypass(parts.netloc))
        ):
            return urlopen(req, timeout=timeout)
        if os.getpid() != self._pid:  # Also safe on runtimes without at-fork hooks.
            self._after_fork()
        response = None
        try:
            response = self._manager().request(
                req.get_method(), req.full_url,
                headers=dict(req.header_items()),
                timeout=urllib3.Timeout(connect=timeout, read=timeout),
                pool_timeout=timeout,
                retries=False, redirect=False, preload_content=False,
                decode_content=False,
            )
            body = response.read(decode_content=False)
            result = _Response(
                body, status=int(response.status), headers=response.headers,
                url=req.full_url,
            )
            if not 200 <= response.status < 300:
                # Includes redirects: an API must not silently change origin or
                # spend unaccounted calls following an upstream error page.
                raise HTTPError(
                    req.full_url, response.status, response.reason,
                    response.headers, result,
                )
            return result
        except urllib3.exceptions.HTTPError as exc:
            # Keep adapter retry classification and avoid credential-bearing
            # urllib3 URLs in exception text. TLS validation remains enabled.
            raise URLError(type(exc).__name__) from None
        finally:
            if response is not None:
                # A partially read connection must be discarded, not reused.
                response.close()
                response.release_conn()


pooled_urlopen = PooledUrlOpener()
atexit.register(pooled_urlopen.close)


def close_pooled_connections() -> None:
    """Drop idle sessions after an upstream ban; in-flight responses can finish."""
    pooled_urlopen.close()
