"""Bounded public catalogs and snapshots; no global-history download by default.

The default CLI is a zero-request plan. Explicit --execute captures one bounded
request per supported adapter. A catalog is a capacity-planning input, never a
claim that the catalog's historical observations have been acquired.
"""
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import shutil
import sys
from urllib.error import HTTPError
from urllib.parse import parse_qsl, quote, quote_plus, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, build_opener

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from downloader.common import SharedRateLimiter, load_env_file, parse_retry_after_seconds
from downloader.http_transport import HttpRequestPolicy, HttpStatusError, ResilientHttpTransport


@dataclass(frozen=True)
class CatalogSpec:
    provider: str
    dataset: str
    credential_name: str | None
    endpoint: str | None
    documentation: str
    kind: str = "metadata_catalog"
    ttl_seconds: int = 86400
    interval_seconds: float = 1.0
    rate_basis: str = "local bounded discovery policy; no official maximum asserted"
    scope: str = "bounded catalog, historical observations require a separate capacity plan"
    history_start_documented: str | None = None
    upstream_cadence: str | None = None
    implementation: str = "implemented"


SPECS = (
    CatalogSpec("noaa_cdo", "datasets", "NOAA_CDO_TOKEN",
                "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets",
                "https://www.ncei.noaa.gov/cdo-web/webservices/v2",
                interval_seconds=0.2, rate_basis="official 5 requests/s and 10000 requests/day; catalog uses one request",
                scope="dataset catalog, advertised mindate/maxdate/datacoverage; at most 1000 datasets"),
    CatalogSpec("bea", "datasets", "BEA_API_KEY", "https://apps.bea.gov/api/data",
                "https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf",
                interval_seconds=0.6, rate_basis="official 100 requests/min, 100 MB/min and 30 errors/min; bounded serial discovery",
                scope="GetDatasetList metadata only; no ALL-year/table data query"),
    CatalogSpec("census", "datasets", None, "https://api.census.gov/data.json",
                "https://www.census.gov/data/developers/guidance/api-user-guide/discovery.html",
                scope="public API dataset metadata discovery; data queries require a key under the June 2026 policy; no observations requested"),
    CatalogSpec("finnhub", "us_stock_symbols", "FINNHUB_API_KEY", "https://api.finnhub.io/api/v1/stock/symbol",
                "https://finnhub.io/docs/api/stock-symbols",
                scope="current exchange=US security identity catalog; not historic membership, news or duplicate OHLC"),
    CatalogSpec("nasa_firms", "data_availability", "NASA_FIRMS_MAP_KEY",
                "https://firms.modaps.eosdis.nasa.gov/api/data_availability/csv/{credential}/ALL",
                "https://firms.modaps.eosdis.nasa.gov/api/data_availability/",
                rate_basis="official 5000 transactions/10 minutes; availability tutorial example costs 5 transactions for one HTTP request; bounded catalog only",
                scope="available satellite products and dates only; no global fire-pixel history request"),
    CatalogSpec("cwa", "O-A0001-001", "CWA_API_KEY",
                "https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001",
                "https://opendata.cwa.gov.tw/opendatadoc/Observation/O-A0002-001.pdf",
                kind="prospective_snapshot", ttl_seconds=3600, upstream_cadence="hourly; source_event is observation time, not publication time",
                scope="one current automatic weather-station snapshot, at most 1000 stations; no historical completeness claim"),
    CatalogSpec("moenv", "aqx_p_432", "MOENV_API_KEY",
                "https://data.moenv.gov.tw/api/v2/aqx_p_432",
                "https://data.moenv.gov.tw/dataset/detail/AQX_P_432",
                kind="prospective_snapshot", ttl_seconds=3600, upstream_cadence="hourly; source_event is provider publishtime",
                scope="one current Taiwan AQI station snapshot, at most 1000 records; no historical completeness claim"),
    CatalogSpec("airnow", "reporting_area_history", "AIRNOW_API_KEY", None,
                "https://docs.airnowapi.org/webservices",
                scope="requires an explicit reporting-area/location and date scope before observation requests",
                implementation="needs_region_date"),
    CatalogSpec("api_data_gov", "credential_hub", "API_DATA_GOV_KEY", None,
                "https://api.data.gov/docs/",
                scope="shared credential hub, not an independent dataset; consumers require a documented API and scope",
                implementation="credential_hub"),
)
SPEC_BY_PROVIDER = {spec.provider: spec for spec in SPECS}
FINNHUB_CDN_CATALOG = "https://static2.finnhub.io/file/privatedatany2/exchange/USf.json"


class CatalogError(RuntimeError):
    """Only fixed, credential-free reason codes may leave a request boundary."""

    REASONS = frozenset({
        "request_budget_exhausted", "redirect_refused", "request_origin_refused",
        "response_byte_limit", "invalid_content_length", "transport_failed",
        "invalid_availability_schema", "provider_application_error", "provider_throttled",
        "credential_activation_required", "credential_rejected",
        "provider_cooldown",
        "approved_cdn_redirect",
        "unsupported_adapter", "empty_or_invalid_records", "invalid_payload_schema",
        "credential_echo_rejected", "normalized_byte_limit", "existing_object_hash_mismatch",
        "disk_reserve_reached", "local_processing_failed",
    })

    def __init__(self, reason: str):
        if reason not in self.REASONS and reason not in {f"http_{code}" for code in range(100, 600)}:
            reason = "local_processing_failed"
        super().__init__(reason)


class RequestBudget:
    def __init__(self, maximum: int):
        self.maximum = maximum
        self.used = 0

    def claim(self, _provider: str) -> None:
        if self.used >= self.maximum:
            raise CatalogError("request_budget_exhausted")
        self.used += 1


class _NoRedirect(HTTPRedirectHandler):
    def __init__(self, redirect_callback=None):
        super().__init__()
        self.redirect_callback = redirect_callback

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # urllib must never automatically copy credential headers to a redirect.
        # The one reviewed Finnhub delivery is a separately budgeted GET below.
        try:
            if self.redirect_callback is not None:
                self.redirect_callback(req.full_url, newurl)
            raise CatalogError("redirect_refused")
        finally:
            if fp is not None:
                fp.close()


class _BoundedResponse:
    def __init__(self, response, maximum: int):
        self.response = response
        self.maximum = maximum
        self.status = getattr(response, "status", 200)
        self.headers = getattr(response, "headers", {})

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.response.close()

    def read(self):
        length = self.headers.get("Content-Length")
        if length is not None:
            try:
                if int(length) > self.maximum:
                    raise CatalogError("response_byte_limit")
            except ValueError:
                raise CatalogError("invalid_content_length") from None
        body = self.response.read(self.maximum + 1)
        if len(body) > self.maximum:
            raise CatalogError("response_byte_limit")
        return body


def _secret_variants(secrets: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted({variant for secret in secrets if secret for variant in (
        secret, quote(secret, safe=""), quote_plus(secret, safe=""),
        json.dumps(secret, ensure_ascii=True)[1:-1],
    )}, key=len, reverse=True))


def _redact_text(text: str, secrets: tuple[str, ...]) -> str:
    for secret in _secret_variants(secrets):
        text = text.replace(secret, "[REDACTED]")
    return text


def _redact_value(value, secrets: tuple[str, ...]):
    if isinstance(value, str):
        return _redact_text(value, secrets)
    if isinstance(value, list):
        return [_redact_value(item, secrets) for item in value]
    if isinstance(value, dict):
        return {_redact_text(str(key), secrets): _redact_value(item, secrets)
                for key, item in value.items()}
    return value


class SafeCatalogTransport:
    """Reuse canonical retry/pacing while enclosing key-in-path transport.

    Errors, response headers, request URLs and opaque upstream messages never
    leave this object. Body reads (including HTTP failures) are byte-bounded.
    """
    def __init__(self, spec: CatalogSpec, budget: RequestBudget, *, maximum_bytes: int,
                 secrets: tuple[str, ...], opener=None, limiter=None):
        self.spec = spec
        self.budget = budget
        self.maximum_bytes = maximum_bytes
        self.secrets = secrets
        self.retry_after_seconds = 0.0
        self._redirect_taken = False
        self._approved_location: str | None = None
        self._open = opener or build_opener(_NoRedirect(self._accept_redirect)).open
        self.limiter = limiter or SharedRateLimiter(spec.interval_seconds, name=f"{spec.provider}_public")
        self.transport = ResilientHttpTransport(
            HttpRequestPolicy(provider=f"{spec.provider}_public", timeout_seconds=45,
                              max_retries=2, retry_cap_seconds=60,
                              retryable_statuses=frozenset({408, 425, 500, 502, 503, 504})),
            limiter=self.limiter, opener=self._bounded_open, on_attempt=budget.claim,
        )

    def _accept_redirect(self, source_url: str, location: str) -> None:
        source, target = urlsplit(source_url), urlsplit(location)
        # Census reports rejected/unactivated data keys via a same-origin
        # redirect. Classify it without following it or persisting the key URL.
        if (self.spec.provider == "census" and source.hostname == "api.census.gov"
                and target.scheme == "https" and target.netloc == "api.census.gov"
                and target.path == "/data/invalid_key.html" and not target.query):
            raise CatalogError("credential_rejected")
        approved = urlsplit(FINNHUB_CDN_CATALOG)
        try:
            params = parse_qsl(target.query, keep_blank_values=True, strict_parsing=True)
        except ValueError:
            raise CatalogError("redirect_refused") from None
        if (self.spec.provider != "finnhub" or self._redirect_taken
                or source.scheme != "https" or source.netloc != "api.finnhub.io"
                or source.path != "/api/v1/stock/symbol"
                or target.scheme != "https" or target.netloc != approved.netloc
                or target.path != approved.path or target.fragment
                or len(params) != 1 or params[0][0] != "Authorization"
                or not params[0][1] or len(location) > 8192
                or any(ord(char) < 32 or ord(char) == 127 for char in location)):
            raise CatalogError("redirect_refused")
        token = params[0][1]
        if any(secret in token or secret in unquote(token) for secret in _secret_variants(self.secrets)):
            raise CatalogError("redirect_refused")
        self._redirect_taken = True
        # The opaque delivery token is distinct from the original API key.
        # Keep both decoded and exact encoded forms in memory-only redaction.
        self.secrets = (*self.secrets, token, target.query.partition("=")[2])
        self._approved_location = location
        raise CatalogError("approved_cdn_redirect")

    def _bounded_open(self, request, *, timeout):
        expected = urlsplit(self.spec.endpoint or "")
        actual = urlsplit(request.full_url)
        if ((actual.scheme, actual.hostname, actual.port) != ("https", expected.hostname, expected.port)
                or actual.username is not None or actual.password is not None or actual.fragment):
            raise CatalogError("request_origin_refused")
        try:
            return _BoundedResponse(self._open(request, timeout=timeout), self.maximum_bytes)
        except HTTPError as exc:
            if 300 <= exc.code < 400:
                location = str(exc.headers.get("Location", "")) if exc.headers else ""
                exc.close()
                self._accept_redirect(request.full_url, location)
            try:
                raw = exc.read(self.maximum_bytes + 1)
            finally:
                exc.close()
            if len(raw) > self.maximum_bytes:
                raise CatalogError("response_byte_limit") from None
            # Preserve only Retry-After for canonical retry handling. Never let
            # response headers echo credentials into the generic transport.
            headers = {"Retry-After": _redact_text(str(exc.headers.get("Retry-After", "")), self.secrets)} if exc.headers else {}
            self.retry_after_seconds = parse_retry_after_seconds(headers.get("Retry-After")) or 0.0
            # A quota response is not a transient transport failure. Persist its
            # cooldown without tying up all the independent providers in a sleep.
            if self.retry_after_seconds > 60 and exc.code != 429:
                self.limiter.defer(self.retry_after_seconds)
                raise CatalogError("provider_cooldown") from None
            safe = _redact_text(raw.decode("utf-8", errors="replace"), self.secrets).encode()
            raise HTTPError("https://redacted.invalid/", exc.code, "provider_http_error",
                            headers, io.BytesIO(safe)) from None

    def fetch(self, url: str, headers: dict[str, str]) -> bytes:
        try:
            response = self.transport.request_bytes(url, headers=headers)
            if response.status != 200:
                raise CatalogError(f"http_{response.status}")
            return response.body
        except CatalogError as exc:
            if str(exc) == "approved_cdn_redirect" and self._approved_location is not None:
                # No original header, cookie, API query or response header is
                # reused. A second redirect is rejected at either boundary.
                delivery = SafeCatalogTransport(
                    replace(self.spec, endpoint=FINNHUB_CDN_CATALOG), self.budget,
                    maximum_bytes=self.maximum_bytes, secrets=self.secrets,
                    opener=self._open, limiter=self.limiter,
                )
                try:
                    return delivery.fetch(self._approved_location, {
                        "User-Agent": "stockAgent-public-catalog-research/1.0", "Accept": "application/json"})
                finally:
                    self.retry_after_seconds = delivery.retry_after_seconds
                    self._approved_location = None
            raise
        except HttpStatusError as exc:
            # Canonical sanitized_url strips queries, not FIRMS' key path.
            # Never forward its message/url/body/cause into logs or receipts.
            if exc.status == 429:
                self.retry_after_seconds = max(60.0, self.retry_after_seconds)
                self.limiter.defer(self.retry_after_seconds)
            raise CatalogError(f"http_{exc.status}") from None
        except Exception:
            raise CatalogError("transport_failed") from None


def request_for(spec: CatalogSpec, key: str) -> tuple[str, dict[str, str]]:
    headers = {"User-Agent": "stockAgent-public-catalog-research/1.0", "Accept": "application/json"}
    endpoint = spec.endpoint or ""
    params: dict[str, str | int] = {}
    if spec.provider == "noaa_cdo":
        headers["token"] = key
        params = {"limit": 1000, "offset": 1}
    elif spec.provider == "bea":
        params = {"UserID": key, "method": "GetDatasetList", "ResultFormat": "JSON"}
    elif spec.provider == "finnhub":
        headers["X-Finnhub-Token"] = key
        params = {"exchange": "US"}
    elif spec.provider == "nasa_firms":
        endpoint = endpoint.replace("{credential}", quote(key, safe=""))
        headers["Accept"] = "text/csv"
    elif spec.provider == "cwa":
        params = {"Authorization": key, "format": "JSON", "limit": 1000}
    elif spec.provider == "moenv":
        params = {"api_key": key, "format": "json", "limit": 1000}
    return endpoint + ("?" + urlencode(params) if params else ""), headers


def parse_payload(spec: CatalogSpec, body: bytes, secrets: tuple[str, ...]) -> tuple[bytes, dict]:
    try:
        if spec.provider == "nasa_firms":
            records = list(csv.DictReader(io.StringIO(body.decode("utf-8-sig"))))
            if not records or not {"data_id", "min_date", "max_date"} <= records[0].keys():
                raise CatalogError("invalid_availability_schema")
            if any(not row.get("data_id") or not row.get("min_date") or not row.get("max_date") for row in records):
                raise CatalogError("invalid_availability_schema")
            for row in records:
                first = datetime.strptime(row["min_date"], "%Y-%m-%d")
                last = datetime.strptime(row["max_date"], "%Y-%m-%d")
                if first > last:
                    raise CatalogError("invalid_availability_schema")
            payload = {"records": records}
        else:
            payload = json.loads(body)
            if spec.provider == "noaa_cdo":
                records = payload.get("results")
            elif spec.provider == "bea":
                results = payload.get("BEAAPI", {}).get("Results", {})
                if "Error" in results:
                    error = results["Error"]
                    if isinstance(error, dict):
                        code = str(error.get("APIErrorCode"))
                        # BEA code 4 is inactive registration; the UserID echo
                        # and opaque description must never reach artifacts.
                        if code in {"1", "4", "7"}:
                            raise CatalogError({"1": "credential_rejected", "4": "credential_activation_required",
                                                "7": "provider_throttled"}[code])
                    raise CatalogError("provider_application_error")
                records = results.get("Dataset")
            elif spec.provider == "census":
                records = payload.get("dataset")
            elif spec.provider == "finnhub":
                records = payload
            elif spec.provider == "cwa":
                if str(payload.get("success", "")).lower() != "true":
                    raise CatalogError("provider_application_error")
                records = payload.get("records", {}).get("Station")
            elif spec.provider == "moenv":
                # Official 2026-01-14 change moved envelope metadata to
                # /status. Retain compatibility with archived older responses.
                records = payload if isinstance(payload, list) else payload.get("records")
            else:
                raise CatalogError("unsupported_adapter")
        if not isinstance(records, list) or not records or any(not isinstance(row, dict) for row in records):
            raise CatalogError("empty_or_invalid_records")
        identity_field = {"noaa_cdo": "id", "bea": "DatasetName", "census": "c_dataset",
                          "finnhub": "symbol", "cwa": "StationId", "moenv": "siteid",
                          "nasa_firms": "data_id"}[spec.provider]
        if any(not row.get(identity_field) for row in records):
            raise CatalogError("invalid_payload_schema")
    except CatalogError:
        raise
    except Exception:
        raise CatalogError("invalid_payload_schema") from None
    # Decode first to remove JSON unicode-escaped credential echoes as well.
    clean = _redact_value(payload, secrets)
    encoded = json.dumps(clean, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if any(secret.encode() in encoded for secret in _secret_variants(secrets)):
        raise CatalogError("credential_echo_rejected")
    metadata = {"rows": len(records), "kind": spec.kind, "history_complete": False,
                "catalog_complete": spec.kind == "metadata_catalog", "source_event_start": None,
                "source_event_end": None, "advertised_history_start": None,
                "advertised_history_end": None, "payload_sanitized": clean != payload,
                "retained_payload_format": "sanitized_json", "original_response_bytes": len(body)}
    metadata["source_event_time_kind"] = {"cwa": "observation", "moenv": "publication"}.get(spec.provider)
    if spec.provider == "noaa_cdo":
        count = payload.get("metadata", {}).get("resultset", {}).get("count")
        metadata["catalog_complete"] = type(count) is int and count == len(records)
        metadata["advertised_history_start"] = min((r["mindate"] for r in records if isinstance(r.get("mindate"), str)), default=None)
        metadata["advertised_history_end"] = max((r["maxdate"] for r in records if isinstance(r.get("maxdate"), str)), default=None)
    elif spec.provider == "nasa_firms":
        metadata["advertised_history_start"] = min(r["min_date"] for r in records)
        metadata["advertised_history_end"] = max(r["max_date"] for r in records)
    elif spec.kind == "prospective_snapshot":
        metadata["catalog_complete"] = False
        dates = [r.get("publishtime") for r in records] if spec.provider == "moenv" else [r.get("ObsTime", {}).get("DateTime") for r in records]
        dates = [value for value in dates if isinstance(value, str) and value]
        metadata["source_event_start"] = min(dates, default=None)
        metadata["source_event_end"] = max(dates, default=None)
    return encoded, _redact_value(metadata, secrets)


def _now() -> datetime:
    return datetime.now(UTC)


def _next_check(spec: CatalogSpec, observed: datetime) -> datetime:
    expires = observed + timedelta(seconds=spec.ttl_seconds)
    if spec.kind == "prospective_snapshot" and (spec.upstream_cadence or "").startswith("hourly"):
        # Sampling at :59 must not cause the following :00 release to be
        # skipped. Taiwan's UTC+08 offset preserves these hourly boundaries.
        boundary = observed.astimezone(UTC).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return min(expires, boundary)
    return expires


def _cached(root: Path, spec: CatalogSpec, *, now: datetime, maximum_bytes: int) -> dict | None:
    try:
        receipt = json.loads((root / "receipts" / f"{spec.provider}.json").read_text())
        stamp = datetime.fromisoformat(receipt["observed_at_utc"])
        relative = Path(receipt["object_path"])
        path = root / relative
        if (receipt.get("provider") != spec.provider or receipt.get("dataset") != spec.dataset
                or receipt.get("status") != "acquired" or stamp.tzinfo is None
                or now < stamp or now >= _next_check(spec, stamp)
                or relative.is_absolute() or ".." in relative.parts
                or relative.parts[:2] != ("objects", spec.provider)
                or not path.resolve().is_relative_to(root.resolve())
                or not 0 < path.stat().st_size <= maximum_bytes
                or path.stat().st_size != receipt["object_bytes"]
                or hashlib.sha256(path.read_bytes()).hexdigest() != receipt["object_sha256"]):
            return None
        return receipt
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _active_cooldown(root: Path, spec: CatalogSpec, now: datetime) -> str | None:
    try:
        attempt = json.loads((root / "attempts" / f"{spec.provider}.json").read_text())
        retry_at = datetime.fromisoformat(attempt["retry_at_utc"])
        if (attempt.get("provider") == spec.provider
                and attempt.get("status") in {"http_429", "provider_throttled", "provider_cooldown"}
                and retry_at.tzinfo is not None and retry_at > now):
            return retry_at.isoformat()
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return None


def plan(specs: list[CatalogSpec], credentials: dict[str, str], *, max_requests: int,
         maximum_bytes: int) -> dict:
    return {"mode": "plan", "network_requests": 0, "global_history_enabled": False,
            "max_requests_including_retries": max_requests,
            "max_response_bytes_each": maximum_bytes,
            "worst_case_response_bytes": max_requests * maximum_bytes,
            "history_size_estimate_bytes": None,
            "history_size_estimate_reason": "requires catalog cardinalities, selected grain/years and measured response bytes before history enablement",
            "providers": [{**asdict(spec), "credential_state": "not_required" if not spec.credential_name else
                           "configured" if credentials.get(spec.credential_name) else "missing"}
                          for spec in specs]}


def execute(root: Path, specs: list[CatalogSpec], credentials: dict[str, str], *,
            max_requests: int = 12, maximum_bytes: int = 16 * 1024**2,
            minimum_free_bytes: int = 2 * 1024**3, force: bool = False,
            transport_factory=SafeCatalogTransport) -> dict:
    if not 0 <= max_requests <= 100 or not 0 < maximum_bytes <= 64 * 1024**2 or minimum_free_bytes < 0:
        raise ValueError("invalid bounded request/storage budget")
    root.mkdir(parents=True, exist_ok=True)
    budget = RequestBudget(max_requests)
    results: list[dict] = []
    started = _now()
    secrets = tuple(value for value in credentials.values() if value)
    with (root / ".download.lock").open("a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        for spec in specs:
            checked = _now()
            row = {"provider": spec.provider, "dataset": spec.dataset,
                   "kind": spec.kind, "checked_at_utc": checked.isoformat(),
                   "history_complete": False, "scope": spec.scope,
                   "documentation": spec.documentation, "requests_this_run": 0}
            if spec.implementation != "implemented":
                row["status"] = spec.implementation
            elif spec.credential_name and not credentials.get(spec.credential_name):
                row["status"] = "credential_missing"
            elif not force and (cached := _cached(root, spec, now=checked, maximum_bytes=maximum_bytes)):
                row.update(cached)
                row["status"] = "current_cached"
                row["checked_at_utc"] = checked.isoformat()
                row["requests_this_run"] = 0
            elif retry_at := _active_cooldown(root, spec, checked):
                row.update(status="provider_cooldown", retry_at_utc=retry_at)
            elif budget.used >= budget.maximum:
                row["status"] = "request_budget_exhausted"
            elif shutil.disk_usage(root).free < minimum_free_bytes + maximum_bytes * 2:
                row["status"] = "disk_reserve_reached"
            else:
                requests_before = budget.used
                client = None
                try:
                    url, headers = request_for(spec, credentials.get(spec.credential_name or "", ""))
                    client = transport_factory(spec, budget, maximum_bytes=maximum_bytes, secrets=secrets)
                    body = client.fetch(url, headers)
                    observed = _now()
                    try:
                        encoded, metadata = parse_payload(spec, body, getattr(client, "secrets", secrets))
                    except CatalogError as exc:
                        if str(exc) == "provider_throttled":
                            client.retry_after_seconds = 60
                            client.limiter.defer(60)
                        raise
                    if len(encoded) > maximum_bytes:
                        raise CatalogError("normalized_byte_limit")
                    if shutil.disk_usage(root).free < minimum_free_bytes + len(encoded) * 2:
                        raise CatalogError("disk_reserve_reached")
                    digest = hashlib.sha256(encoded).hexdigest()
                    relative = Path("objects") / spec.provider / f"{digest}.json"
                    if (root / relative).exists():
                        if ((root / relative).stat().st_size != len(encoded)
                                or hashlib.sha256((root / relative).read_bytes()).hexdigest() != digest):
                            raise CatalogError("existing_object_hash_mismatch")
                    else:
                        atomic_write_bytes(root / relative, encoded)
                    row.update(metadata, status="acquired", object_path=str(relative),
                               object_sha256=digest, object_bytes=len(encoded),
                               observed_at_utc=observed.isoformat(),
                               next_check_at_utc=_next_check(spec, observed).isoformat(),
                               requests_this_run=budget.used - requests_before)
                    # A new receipt never discards the former version's capture clock.
                    atomic_write_json(root / "receipt_history" / spec.provider /
                                      f"{observed.strftime('%Y%m%dT%H%M%S%fZ')}.json", row)
                    atomic_write_json(root / "receipts" / f"{spec.provider}.json", row)
                except CatalogError as exc:
                    row["status"] = str(exc)
                    row["requests_this_run"] = budget.used - requests_before
                    if str(exc) in {"http_429", "provider_throttled", "provider_cooldown"}:
                        delay = max(60.0, getattr(client, "retry_after_seconds", 0.0))
                        row["retry_at_utc"] = (_now() + timedelta(seconds=delay)).isoformat()
                    atomic_write_json(root / "attempts" / f"{spec.provider}.json", row)
                except Exception:
                    row["status"] = "local_processing_failed"
                    row["requests_this_run"] = budget.used - requests_before
                    atomic_write_json(root / "attempts" / f"{spec.provider}.json", row)
                row["requests_this_run"] = budget.used - requests_before
            results.append(row)
        implemented_providers = {spec.provider for spec in specs if spec.implementation == "implemented"}
        implemented = [row for row in results if row["provider"] in implemented_providers]
        implemented_complete = sum(
            row["status"] in {"acquired", "current_cached"}
            and (row["kind"] != "metadata_catalog" or row.get("catalog_complete") is True)
            for row in implemented)
        summary = {"schema_version": 1, "mode": "bounded_catalog_or_snapshot",
                   "started_at_utc": started.isoformat(), "ended_at_utc": _now().isoformat(),
                   "requests": budget.used, "max_requests": max_requests,
                   "global_history_enabled": False, "history_complete": False,
                   "historical_coverage_state": "not_requested",
                   "implemented_adapters": len(implemented), "implemented_complete": implemented_complete,
                   "planned_non_work": [{"provider": row["provider"], "status": row["status"]}
                                        for row in results if row["provider"] not in implemented_providers],
                   "providers": results, "rows": None,
                   "catalog_entries": sum(int(row.get("rows", 0)) for row in results if row["kind"] == "metadata_catalog"),
                   "snapshot_observation_rows": sum(int(row.get("rows", 0)) for row in results if row["kind"] == "prospective_snapshot"),
                   "provider_row_counts": {row["provider"]: row.get("rows") for row in results},
                   "state": "bounded_scope_complete" if implemented_complete == len(implemented) else "partial"}
        atomic_write_json(root / "download_summary.json", summary)
        return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan", action="store_true", help="Default: zero API requests")
    mode.add_argument("--execute", action="store_true", help="Only bounded catalog/snapshot requests, never global history")
    parser.add_argument("--providers", nargs="+", choices=sorted(SPEC_BY_PROVIDER), default=list(SPEC_BY_PROVIDER))
    parser.add_argument("--output-root", type=Path, default=ROOT / "data_keyed_public_catalogs")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--max-requests", type=int, default=12)
    parser.add_argument("--max-response-mb", type=float, default=16)
    parser.add_argument("--min-free-gb", type=float, default=2)
    parser.add_argument("--force", action="store_true", help="Recheck selected bounded scope; budgets still apply")
    args = parser.parse_args(argv)
    if (not 0 <= args.max_requests <= 100 or not math.isfinite(args.max_response_mb)
            or not 0 < args.max_response_mb <= 64 or not math.isfinite(args.min_free_gb)
            or args.min_free_gb < 0):
        parser.error("invalid bounded request/storage budget")
    names = [spec.credential_name for spec in SPECS if spec.credential_name]
    load_env_file(args.env_file, allowed_names=names)
    credentials = {name: os.environ.get(name, "").strip() for name in names}
    specs = [SPEC_BY_PROVIDER[name] for name in dict.fromkeys(args.providers)]
    maximum_bytes = int(args.max_response_mb * 1024**2)
    if not args.execute:
        result = plan(specs, credentials, max_requests=args.max_requests, maximum_bytes=maximum_bytes)
    else:
        try:
            result = execute(args.output_root, specs, credentials, max_requests=args.max_requests,
                             maximum_bytes=maximum_bytes, minimum_free_bytes=int(args.min_free_gb * 1024**3),
                             force=args.force)
        except BlockingIOError:
            result = {"state": "worker_active", "requests": 0}
        except Exception:
            # Even local OSError paths must not be allowed to expose SDK/request text.
            result = {"state": "local_setup_failed", "requests": 0}
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return int(result.get("state") in {"partial", "local_setup_failed"})


if __name__ == "__main__":
    raise SystemExit(main())
