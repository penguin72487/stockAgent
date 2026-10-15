"""Bounded transport adapters around the native FinLab 2.1.1 intraday SDK.

The SDK still owns manifests, signing (five objects), entitlement, billing,
checksum/schema checks and its generation-bound cache. Only the already signed
minute objects are prefetched. Never retain URLs, auth bodies or response text
in telemetry. This adapter is installed only in isolated acquisition workers.
"""
from __future__ import annotations

import atexit
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib.metadata import version
import math
import random
import threading
import time


HTTP_COUNTERS = ("http_gets", "http_posts", "http_errors", "http_429s", "http_503s",
                 "http_download_bytes", "http_seconds", "http_get_seconds", "http_post_seconds",
                 "http_conflicts", "http_conflict_retries", "http_post_queue_seconds",
                 "http_manifests", "http_manifest_seconds", "http_signs", "http_sign_seconds")
SUPPORTED_SDK = "2.1.1"
SIGNED_BATCH_LIMIT = 5  # Native SDK signing batch, not a requests/second limit.
PREFETCH_MAX_BYTES = 64 * 1024**2  # Above this bound retain native serial reads.


def retry_after_seconds(value, *, now: float) -> float:
    """Respect both HTTP Retry-After forms; malformed values use 60 seconds."""
    if not isinstance(value, str) or len(value) > 128:
        return 60.0
    try:
        seconds = float(value)
    except ValueError:
        try:
            at = parsedate_to_datetime(value)
            seconds = at.timestamp() - now if at.tzinfo is not None else math.nan
        except (ValueError, TypeError, OverflowError):
            seconds = math.nan
    return max(1.0, seconds) if math.isfinite(seconds) and seconds >= 0 else 60.0


class RateGate:
    """One shared cooldown across every process/thread, without busy waits.

    ``shared`` is a multiprocessing synchronized array [deadline, event count].
    An active cooldown fails before making another HTTP call: native SDK retries
    cannot bypass it, and the single parent persists it for the next owner.
    """
    def __init__(self, shared):
        self.shared = shared

    def deadline(self) -> float:
        if self.shared is None:
            return 0.0
        with self.shared.get_lock():
            return self.shared[0]

    def check(self) -> None:
        if self.deadline() > time.time():
            from finlab.exceptions import DataError
            raise DataError("rate_limited: shared source cooldown")

    def defer(self, seconds: float) -> None:
        if self.shared is not None:
            with self.shared.get_lock():
                self.shared[0] = max(self.shared[0], time.time() + seconds)
                self.shared[1] += 1


class WorkerHTTPTransport:
    """Requests facade with thread-local connections and scalar-only metrics.

    Each thread owns its Session/cookie jar; sessions must never be shared by
    concurrent native downloads. Timeout/TLS/proxy/auth parameters pass through
    unchanged. Only explicit rejected native control transactions receive a
    bounded retry; transport retries and object validation remain native.
    """
    def __init__(self, requests_module, *, keepalive=True, rate_gate=None, authorization_gate=None):
        self._module = requests_module
        self._keepalive = keepalive
        self._local = threading.local()
        self._lock = threading.Lock()
        self._sessions = []
        self._metrics = {key: 0.0 if key.endswith("seconds") else 0 for key in HTTP_COUNTERS}
        self._active = self._peak = 0
        self._gate = RateGate(rate_gate)
        self._authorization_gate = authorization_gate
        if keepalive:
            self._session()  # Preflight Session construction before jobs start.
        atexit.register(self.close)

    def _session(self):
        if not hasattr(self._local, "session"):
            session = self._module.Session()
            adapter = self._module.adapters.HTTPAdapter(
                pool_connections=4, pool_maxsize=1, pool_block=True, max_retries=0)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self._local.session = session
            with self._lock:
                self._sessions.append(session)
        return self._local.session

    def _request(self, method, args, kwargs):
        self._gate.check()
        client = self._session() if self._keepalive else self._module
        if self._keepalive:
            client.cookies.clear()  # Preserve native stateless request semantics.
        started = time.monotonic()
        with self._lock:
            self._metrics["http_gets" if method == "get" else "http_posts"] += 1
            self._active += 1
            self._peak = max(self._peak, self._active)
        response = None
        phase = None
        try:
            response = getattr(client, method)(*args, **kwargs)
            code = response.status_code
            query = kwargs.get("json", {}).get("intraday", {}) if isinstance(kwargs.get("json"), dict) else {}
            phase = query.get("mode") if isinstance(query, dict) else None
            if code == 429 or code == 503 and response.headers.get("Retry-After"):
                self._gate.defer(retry_after_seconds(response.headers.get("Retry-After"), now=time.time()))
            with self._lock:
                if code == 429:
                    self._metrics["http_429s"] += 1
                if code == 503:
                    self._metrics["http_503s"] += 1
                if isinstance(code, int) and code >= 400:
                    self._metrics["http_errors"] += 1
                if method == "get" and code == 200:
                    self._metrics["http_download_bytes"] += len(response.content)
                if method == "post" and phase in {"manifest", "download"}:
                    self._metrics["http_manifests" if phase == "manifest" else "http_signs"] += 1
            return response
        except Exception:
            if response is None:
                with self._lock:
                    self._metrics["http_errors"] += 1
            raise
        finally:
            with self._lock:
                self._active -= 1
                elapsed = time.monotonic() - started
                self._metrics["http_seconds"] += elapsed
                self._metrics["http_get_seconds" if method == "get" else "http_post_seconds"] += elapsed
                if response is not None and method == "post" and phase in {"manifest", "download"}:
                    self._metrics["http_manifest_seconds" if phase == "manifest" else "http_sign_seconds"] += elapsed

    def get(self, *args, **kwargs):
        return self._request("get", args, kwargs)

    def post(self, *args, **kwargs):
        body = kwargs.get("json")
        query = body.get("intraday") if isinstance(body, dict) else None
        control = isinstance(query, dict) and query.get("mode") in {"manifest", "download"}
        for attempt in range(3):
            if control and self._authorization_gate is not None:
                before = time.monotonic()
                self._authorization_gate.acquire()
                with self._lock:
                    self._metrics["http_post_queue_seconds"] += time.monotonic() - before
                try:
                    response = self._request("post", args, kwargs)
                finally:
                    self._authorization_gate.release()
            else:
                response = self._request("post", args, kwargs)
            # Only a definite rejected manifest/signing transaction is retried
            # with the exact original body/request_id for idempotency. Do not
            # retry arbitrary 4xx, quota/auth failures or generate a new ID.
            conflict = False
            if control and response.status_code in {200, 409}:
                try:
                    payload = response.json()
                    conflict = isinstance(payload, dict) and payload.get("code") == "request_conflict" and "error" in payload
                    if (isinstance(payload, dict) and "error" in payload and payload.get("code")
                            in {"rate_limited", "rate_limit", "too_many_requests"}):
                        self._gate.defer(retry_after_seconds(response.headers.get("Retry-After"), now=time.time()))
                except (ValueError, TypeError):
                    pass
            if not conflict:
                return response
            with self._lock:
                self._metrics["http_conflicts"] += 1
            if attempt == 2:
                return response  # Native SDK retains the final bounded error.
            with self._lock:
                self._metrics["http_conflict_retries"] += 1
            time.sleep(0.5 * 2**attempt + random.uniform(0, 0.5))
        return response

    def metrics(self) -> dict:
        with self._lock:
            result = {**self._metrics, "http_peak_inflight": self._peak}
        result["source_backoff_until_utc"] = (datetime.fromtimestamp(self._gate.deadline(), UTC).isoformat()
                                             if self._gate.deadline() else None)
        return result

    def close(self):
        with self._lock:
            sessions, self._sessions = self._sessions, []
        for session in sessions:
            session.close()

    def __getattr__(self, name):
        return getattr(self._module, name)


def validate_prefetch_sdk(module=None) -> None:
    """Fail in the parent, not in a repeatedly respawned Pool initializer."""
    if module is None:
        from finlab.data import intraday as module
    if version("finlab") != SUPPORTED_SDK or not all(callable(getattr(module, key, None)) for key in (
            "_post", "_download", "_verify_signed_partition", "get_intraday")):
        raise RuntimeError("FinLab prefetch adapter requires verified SDK 2.1.1; set minute_download_threads=1")


class SignedMinutePrefetch:
    """Pipeline the native five-file signing group, not arbitrary API requests.

    Validate *all* requested identities against the current manifest before any
    parallel read. Native _download retains size/SHA verification; native reader
    retains schema/identity and generation-bound cache writes. At most one signed
    group (5 objects / 64 MiB) is retained. Futures are consumed exactly once.
    """
    def __init__(self, module, threads: int):
        if type(threads) is not int or not 1 <= threads <= SIGNED_BATCH_LIMIT:
            raise ValueError("invalid signed minute download parallelism")
        self._module = module
        self._threads = threads
        self._post = module._post
        self._download = module._download
        self._get = module.get_intraday
        self._verify = module._verify_signed_partition
        self._pool = None
        self._futures = {}
        self._manifest = {}
        self._query = None
        atexit.register(self.close)

    @staticmethod
    def _identity(part):
        return tuple(part[key] for key in ("key", "generation", "sha256", "size", "rows", "date"))

    def post(self, query):
        result = self._post(query)  # Signing, quota, auth refresh remain native.
        if not query.get("dataset", "").startswith("tw_minute:") or self._threads == 1:
            return result
        mode = query.get("mode")
        if mode == "manifest":
            self.release()
            self._query = tuple(query[key] for key in ("dataset", "start", "end"))
            self._manifest = {part["date"]: part for part in result["partitions"]}
        elif mode == "download":
            from finlab.exceptions import DataError
            self.release()
            requested = query.get("partitions", [])
            if (self._query != tuple(query[key] for key in ("dataset", "start", "end"))
                    or not 1 <= len(requested) <= SIGNED_BATCH_LIMIT
                    or len({part["date"] for part in requested}) != len(requested)):
                raise DataError("Invalid bounded intraday signing group")
            actual = {part["date"]: part for part in result["partitions"]}
            if len(actual) != len(result["partitions"]):
                raise DataError("Duplicate signed intraday partition")
            parts = []
            for request in requested:
                expected = self._manifest.get(request["date"])
                if not expected or expected.get("generation") != request["generation"]:
                    raise DataError("Publication changed during download; retry data.get()")
                parts.append(self._verify(actual.get(request["date"]), expected))
            if (any(type(part.get("size")) is not int or part["size"] < 0 for part in parts)
                    or sum(part["size"] for part in parts) > PREFETCH_MAX_BYTES):
                return result  # Large groups keep the exact native serial path.
            if self._pool is None:
                self._pool = ThreadPoolExecutor(max_workers=self._threads, thread_name_prefix="finlab-object")
            self._futures = {self._identity(part): self._pool.submit(self._download, part) for part in parts}
        return result

    def download(self, part):
        future = self._futures.pop(self._identity(part), None)
        return future.result() if future is not None else self._download(part)

    def get(self, *args, **kwargs):
        try:
            return self._get(*args, **kwargs)
        finally:
            self.release()  # Failed parse/validation cannot leak stale futures.
            self._manifest, self._query = {}, None

    def release(self):
        pending, self._futures = self._futures, {}
        for future in pending.values():
            future.cancel()
        for future in pending.values():
            if not future.cancelled():
                # Finish already admitted reads; native checksum exceptions are
                # not published as success and are consumed by their SDK caller.
                try:
                    future.result()
                except Exception:
                    pass

    def install(self):
        self._module._post, self._module._download = self.post, self.download
        self._module.get_intraday = self.get

    def close(self):
        self.release()
        if self._pool is not None:
            self._pool.shutdown(wait=True, cancel_futures=True)
            self._pool = None
