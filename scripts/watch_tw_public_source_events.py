#!/usr/bin/env python3
"""Continuously detect and ingest version changes in every TW public dataset.

Official TWSE/TPEx/TAIFEX/TDCC/data.gov.tw feeds do not expose one common
webhook or event stream.  This daemon provides event semantics at the only
reliable boundary they share: a change in the official HTTP representation.
It uses conditional GET when the publisher exposes validators and a stable
content fingerprint otherwise.  A newly observed version is not acknowledged
until the existing downloader has parsed, validated, and receipted it.

The daemon accelerates publication pickup.  It does not replace the 08:20 and
08:30 full model-safety acceptance jobs.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Mapping
from urllib.parse import quote, urlencode, urlparse, urlunparse, parse_qsl
import uuid
from zoneinfo import ZoneInfo

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import (  # noqa: E402
    SharedRateLimiter,
    resolve_request_interval,
)
from downloader.download_tw_public_data import (  # noqa: E402
    DATA_GOV_DATASET_API,
    DEFAULT_DATASETS,
    DatasetSpec,
)
from scripts.watch_tw_public_publication_group import (  # noqa: E402
    PublicationPhase,
    _changed_files,
    _download_command,
    _file_hashes,
    _latest_completed_taiex_session,
    _taiex_calendar_command,
)


TAIPEI = ZoneInfo("Asia/Taipei")
SCHEMA_VERSION = 1
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "Chrome/124.0 Safari/537.36 stockAgent-tw-public-event-monitor/1"
)
BLOCKING_DOWNLOAD_STATUSES = frozenset({"failed", "incomplete", "unsupported"})
FAST_TAGS = frozenset(
    {
        "calendar",
        "corporate_action",
        "daily",
        "day-trade",
        "disposal",
        "event",
        "flow",
        "index",
        "institutional",
        "lifecycle",
        "margin",
        "market_rule",
        "market_state",
        "material",
        "price",
        "settlement",
        "shorting",
        "universe",
    }
)
MEDIUM_TAGS = frozenset({"fundamental", "ownership", "dividend", "valuation"})
_STOP = threading.Event()


@dataclass(frozen=True, slots=True)
class ProbeResult:
    dataset: str
    status: str
    url: str
    checked_at_taipei: str
    http_status: int | None = None
    version: str | None = None
    body_sha256: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    content_length: str | None = None
    content_disposition: str | None = None
    not_modified: bool = False
    resources: dict[str, dict[str, Any]] | None = None
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        default=Path("artifacts/data_refresh/tw_public/events"),
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--date-workers", type=int, default=4)
    parser.add_argument("--probe-workers", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--heartbeat-seconds", type=float, default=30.0)
    parser.add_argument("--fast-interval-seconds", type=float, default=60.0)
    parser.add_argument("--medium-interval-seconds", type=float, default=300.0)
    parser.add_argument("--slow-interval-seconds", type=float, default=300.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument(
        "--no-bootstrap-refresh",
        action="store_true",
        help="establish source baselines without reconciling local parquet files",
    )
    return parser.parse_args()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_timestamp(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=TAIPEI)


def _canonical_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _canonical_json_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        normalized = [_canonical_json_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ),
        )
    return value


def _body_sha256(content: bytes, *, expected_json: bool) -> str:
    if not expected_json:
        return hashlib.sha256(content).hexdigest()
    decoded = json.loads(content)
    canonical = json.dumps(
        _canonical_json_value(decoded),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _response_body_sha256(
    content: bytes,
    *,
    expected_json: bool,
    content_type: str | None,
    content_disposition: str | None,
) -> str:
    """Fingerprint an official structured response without assuming one format.

    Several official OpenAPI endpoints negotiate either JSON or CSV at the same
    URL and may change the selected representation behind a CDN.  JSON remains
    canonicalized as before.  A response explicitly identified as CSV is parsed
    and canonicalized instead of being rejected as malformed JSON; unknown
    HTML or other 200 responses still fail closed.
    """

    if not expected_json:
        return hashlib.sha256(content).hexdigest()
    stripped = content.lstrip(b"\xef\xbb\xbf \t\r\n")
    if stripped.startswith((b"{", b"[")):
        return _body_sha256(content, expected_json=True)
    descriptor = f"{content_type or ''} {content_disposition or ''}".casefold()
    first_line = stripped.splitlines()[0] if stripped.splitlines() else b""
    csv_hint = "csv" in descriptor or (b"," in first_line and b"\n" in stripped)
    if csv_hint:
        decoded: str | None = None
        for encoding in ("utf-8-sig", "cp950", "big5"):
            try:
                decoded = content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        if decoded is None:
            raise ValueError("CSV response has no supported text encoding")
        rows = [row for row in csv.reader(decoded.splitlines()) if any(row)]
        if len(rows) < 2 or len(rows[0]) < 2:
            raise ValueError("CSV response has no header and data rows")
        width = len(rows[0])
        if any(len(row) != width for row in rows[1:]):
            raise ValueError("CSV response has inconsistent row widths")
        canonical = json.dumps(
            [rows[0], *sorted(rows[1:])],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()
    # Preserve fail-closed behavior for a publisher error page returned as 200.
    return _body_sha256(content, expected_json=True)


def _probe_interval(spec: DatasetSpec, args: argparse.Namespace) -> float:
    tags = set(spec.tags)
    if spec.kind == "data_gov":
        return (
            float(args.medium_interval_seconds)
            if "daily" in tags
            else float(args.slow_interval_seconds)
        )
    if spec.kind == "historical_json_table" or tags & FAST_TAGS:
        return float(args.fast_interval_seconds)
    if tags & MEDIUM_TAGS:
        return float(args.medium_interval_seconds)
    return float(args.slow_interval_seconds)


def _probe_url(spec: DatasetSpec, observed: datetime) -> str:
    local = observed.astimezone(TAIPEI)
    if spec.kind == "data_gov":
        assert spec.data_gov_id is not None
        return DATA_GOV_DATASET_API.format(dataset_id=quote(str(spec.data_gov_id)))
    if spec.url_template:
        return spec.url_template.format(date=local.strftime(spec.date_format))
    assert spec.url is not None
    if spec.name == "tpex_delisted_company":
        parsed = urlparse(spec.url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.update({"response": "json", "date": str(local.year), "cate": "1"})
        return urlunparse(parsed._replace(query=urlencode(query)))
    return spec.url


def _version(
    *,
    url: str,
    body_sha256: str,
    etag: str | None,
    last_modified: str | None,
    content_length: str | None,
    content_disposition: str | None,
) -> str:
    payload = {
        "url": url,
        "body_sha256": body_sha256,
        "etag": etag,
        "last_modified": last_modified,
        "content_length": content_length,
        "content_disposition": content_disposition,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _request(
    session: requests.Session,
    *,
    url: str,
    previous: Mapping[str, Any],
    expected_json: bool,
    limiter: SharedRateLimiter,
    args: argparse.Namespace,
) -> dict[str, Any]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/csv,application/xml,text/xml,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }
    if previous.get("etag"):
        headers["If-None-Match"] = str(previous["etag"])
    if previous.get("last_modified"):
        headers["If-Modified-Since"] = str(previous["last_modified"])

    last_error: Exception | None = None
    for attempt in range(int(args.retries) + 1):
        limiter.wait()
        try:
            try:
                response = session.get(
                    url,
                    headers=headers,
                    timeout=(int(args.timeout), int(args.timeout)),
                    allow_redirects=True,
                    verify=True,
                )
            except requests.exceptions.SSLError:
                # A small set of official DGBAS resource hosts serves an
                # incomplete certificate chain.  Match the established
                # downloader contract: strict verification is always tried
                # first and only that concrete TLS failure permits one
                # request-local fallback.
                requests.packages.urllib3.disable_warnings()
                limiter.wait()
                response = session.get(
                    url,
                    headers=headers,
                    timeout=(int(args.timeout), int(args.timeout)),
                    allow_redirects=True,
                    verify=False,
            )
            if response.status_code == 304:
                prior_version = previous.get("version") or previous.get(
                    "observed_version"
                )
                if not prior_version:
                    raise RuntimeError("source returned 304 without a stored version")
                return {
                    "http_status": 304,
                    "version": str(prior_version),
                    "body_sha256": previous.get("body_sha256"),
                    "etag": response.headers.get("etag") or previous.get("etag"),
                    "last_modified": response.headers.get("last-modified")
                    or previous.get("last_modified"),
                    "content_length": previous.get("content_length"),
                    "content_disposition": response.headers.get("content-disposition")
                    or previous.get("content_disposition"),
                    "not_modified": True,
                }
            if response.status_code in {307, 403, 408, 429, 500, 502, 503, 504}:
                raise RuntimeError(f"transient HTTP {response.status_code}")
            response.raise_for_status()
            etag = response.headers.get("etag")
            last_modified = response.headers.get("last-modified")
            content_length = response.headers.get("content-length")
            content_disposition = response.headers.get("content-disposition")
            body_hash = _response_body_sha256(
                response.content,
                expected_json=expected_json,
                content_type=response.headers.get("content-type"),
                content_disposition=content_disposition,
            )
            return {
                "http_status": int(response.status_code),
                "version": _version(
                    url=response.url,
                    body_sha256=body_hash,
                    etag=etag,
                    last_modified=last_modified,
                    content_length=content_length,
                    content_disposition=content_disposition,
                ),
                "body_sha256": body_hash,
                "etag": etag,
                "last_modified": last_modified,
                "content_length": content_length,
                "content_disposition": content_disposition,
                "not_modified": False,
                "content": response.content,
            }
        except (requests.RequestException, RuntimeError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt >= int(args.retries):
                break
            time.sleep(min(8.0, 0.5 * (2**attempt)))
    raise RuntimeError(str(last_error or "HTTP probe failed"))


def _probe_data_gov(
    spec: DatasetSpec,
    *,
    observed: datetime,
    previous: Mapping[str, Any],
    limiter: SharedRateLimiter,
    args: argparse.Namespace,
) -> ProbeResult:
    url = _probe_url(spec, observed)
    session = requests.Session()
    metadata_previous = previous.get("metadata")
    metadata_previous = (
        dict(metadata_previous) if isinstance(metadata_previous, Mapping) else {}
    )
    metadata = _request(
        session,
        url=url,
        previous=metadata_previous,
        expected_json=True,
        limiter=limiter,
        args=args,
    )
    resource_urls: list[str] = []
    if metadata.get("content") is not None:
        payload = json.loads(metadata["content"])
        result = payload.get("result") if isinstance(payload, dict) else None
        distributions = result.get("distribution") if isinstance(result, dict) else None
        if not isinstance(distributions, list):
            raise RuntimeError("data.gov.tw metadata has no distribution list")
        for distribution in distributions:
            if not isinstance(distribution, Mapping):
                continue
            resource = distribution.get("resourceDownloadUrl") or distribution.get(
                "resourceAPIUrl"
            )
            if resource:
                resource_urls.append(str(resource))
    else:
        saved_urls = previous.get("resource_urls")
        if isinstance(saved_urls, list):
            resource_urls = [str(item) for item in saved_urls if str(item)]
    if not resource_urls:
        raise RuntimeError("data.gov.tw dataset has no downloadable resource")

    prior_resources = previous.get("resources")
    prior_resources = (
        dict(prior_resources) if isinstance(prior_resources, Mapping) else {}
    )
    resources: dict[str, dict[str, Any]] = {}
    for resource_url in resource_urls:
        prior = prior_resources.get(resource_url)
        prior = dict(prior) if isinstance(prior, Mapping) else {}
        resource = _request(
            session,
            url=resource_url,
            previous=prior,
            expected_json=False,
            limiter=limiter,
            args=args,
        )
        resource.pop("content", None)
        resources[resource_url] = resource
    metadata.pop("content", None)
    combined = {
        "metadata_version": metadata["version"],
        "resources": {
            key: resources[key]["version"] for key in sorted(resources)
        },
    }
    version = hashlib.sha256(
        json.dumps(combined, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ProbeResult(
        dataset=spec.name,
        status="ok",
        url=url,
        checked_at_taipei=observed.astimezone(TAIPEI).isoformat(timespec="seconds"),
        http_status=int(metadata["http_status"]),
        version=version,
        body_sha256=str(metadata.get("body_sha256") or ""),
        etag=metadata.get("etag"),
        last_modified=metadata.get("last_modified"),
        content_length=metadata.get("content_length"),
        content_disposition=metadata.get("content_disposition"),
        not_modified=bool(metadata.get("not_modified")) and all(
            bool(row.get("not_modified")) for row in resources.values()
        ),
        resources={"__metadata__": metadata, **resources},
    )


def _probe_one(
    spec: DatasetSpec,
    *,
    observed: datetime,
    previous: Mapping[str, Any],
    limiter: SharedRateLimiter,
    args: argparse.Namespace,
) -> ProbeResult:
    url = _probe_url(spec, observed)
    try:
        if spec.kind == "data_gov":
            return _probe_data_gov(
                spec,
                observed=observed,
                previous=previous,
                limiter=limiter,
                args=args,
            )
        session = requests.Session()
        fetched = _request(
            session,
            url=url,
            previous=previous,
            expected_json=True,
            limiter=limiter,
            args=args,
        )
        fetched.pop("content", None)
        return ProbeResult(
            dataset=spec.name,
            status="ok",
            url=url,
            checked_at_taipei=observed.astimezone(TAIPEI).isoformat(
                timespec="seconds"
            ),
            **fetched,
        )
    except Exception as exc:
        return ProbeResult(
            dataset=spec.name,
            status="failed",
            url=url,
            checked_at_taipei=observed.astimezone(TAIPEI).isoformat(
                timespec="seconds"
            ),
            error=str(exc),
        )


def _probe_specs(
    specs: list[DatasetSpec],
    *,
    observed: datetime,
    rows: Mapping[str, Any],
    limiter: SharedRateLimiter,
    args: argparse.Namespace,
) -> list[ProbeResult]:
    # Host grouping is observable in receipts and bounds concurrent connection
    # pools, while the process-shared limiter remains the authoritative 10 rps
    # ceiling across this daemon and downloader subprocesses.
    groups: dict[str, list[DatasetSpec]] = defaultdict(list)
    for spec in specs:
        groups[urlparse(_probe_url(spec, observed)).netloc].append(spec)

    def run_group(group: list[DatasetSpec]) -> list[ProbeResult]:
        output: list[ProbeResult] = []
        for spec in group:
            previous = rows.get(spec.name)
            previous = dict(previous) if isinstance(previous, Mapping) else {}
            output.append(
                _probe_one(
                    spec,
                    observed=datetime.now(TAIPEI),
                    previous=previous,
                    limiter=limiter,
                    args=args,
                )
            )
        return output

    results: list[ProbeResult] = []
    with ThreadPoolExecutor(
        max_workers=min(int(args.probe_workers), max(1, len(groups)))
    ) as executor:
        futures = [executor.submit(run_group, group) for group in groups.values()]
        for future in as_completed(futures):
            results.extend(future.result())
    return sorted(results, key=lambda row: row.dataset)


def _registry(specs: list[DatasetSpec], args: argparse.Namespace) -> tuple[str, list[dict[str, Any]]]:
    rows = [
        {
            "dataset": spec.name,
            "kind": spec.kind,
            "source": spec.source,
            "tags": list(spec.tags),
            "probe_template": spec.url
            or spec.url_template
            or DATA_GOV_DATASET_API.format(dataset_id=spec.data_gov_id),
            "interval_seconds": _probe_interval(spec, args),
        }
        for spec in specs
    ]
    digest = hashlib.sha256(
        json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return digest, rows


def _new_state(specs: list[DatasetSpec], args: argparse.Namespace) -> dict[str, Any]:
    registry_sha256, registry = _registry(specs, args)
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "warming",
        "registry_sha256": registry_sha256,
        "registered_dataset_count": len(specs),
        "monitored_dataset_count": len(specs),
        "registry": registry,
        "datasets": {},
        "started_at_taipei": datetime.now(TAIPEI).isoformat(timespec="seconds"),
    }


def _load_state(path: Path, specs: list[DatasetSpec], args: argparse.Namespace) -> dict[str, Any]:
    expected_sha, registry = _registry(specs, args)
    state = _read_json(path)
    if state.get("schema_version") != SCHEMA_VERSION:
        return _new_state(specs, args)
    registry_changed = state.get("registry_sha256") != expected_sha
    prior_rows = state.get("datasets")
    prior_rows = dict(prior_rows) if isinstance(prior_rows, Mapping) else {}
    state["registry_sha256"] = expected_sha
    state["registry"] = registry
    state["registered_dataset_count"] = len(specs)
    state["monitored_dataset_count"] = len(specs)
    state["datasets"] = {
        spec.name: dict(prior_rows.get(spec.name, {}))
        for spec in specs
        if isinstance(prior_rows.get(spec.name, {}), Mapping)
    }
    if registry_changed:
        for row in state["datasets"].values():
            row.pop("next_probe_at_taipei", None)
    return state


def _apply_probe_results(
    state: dict[str, Any],
    results: list[ProbeResult],
    *,
    specs_by_name: Mapping[str, DatasetSpec],
    args: argparse.Namespace,
) -> list[str]:
    rows = state.setdefault("datasets", {})
    changed: list[str] = []
    for result in results:
        row = rows.setdefault(result.dataset, {})
        prior_version = row.get("observed_version")
        row.update(
            {
                "dataset": result.dataset,
                "kind": specs_by_name[result.dataset].kind,
                "source": specs_by_name[result.dataset].source,
                "tags": list(specs_by_name[result.dataset].tags),
                "probe_url": result.url,
                "interval_seconds": _probe_interval(
                    specs_by_name[result.dataset], args
                ),
                "last_checked_at_taipei": result.checked_at_taipei,
                "last_probe_status": result.status,
            }
        )
        checked = _parse_timestamp(result.checked_at_taipei) or datetime.now(TAIPEI)
        if result.status == "ok" and result.version:
            for key in (
                "http_status",
                "body_sha256",
                "etag",
                "last_modified",
                "content_length",
                "content_disposition",
                "not_modified",
            ):
                row[key] = getattr(result, key)
            if result.resources is not None:
                metadata = result.resources.get("__metadata__", {})
                row["metadata"] = metadata
                row["resources"] = {
                    key: value
                    for key, value in result.resources.items()
                    if key != "__metadata__"
                }
                row["resource_urls"] = sorted(row["resources"])
            row["observed_version"] = result.version
            row["last_error"] = None
            row["consecutive_probe_failures"] = 0
            row["next_probe_at_taipei"] = (
                checked
                + timedelta(seconds=_probe_interval(specs_by_name[result.dataset], args))
            ).isoformat(timespec="seconds")
            if prior_version is not None and prior_version != result.version:
                row["last_changed_at_taipei"] = result.checked_at_taipei
                row["change_count"] = int(row.get("change_count") or 0) + 1
                changed.append(result.dataset)
            if row.get("applied_version") != result.version:
                row.setdefault("pending_since_taipei", result.checked_at_taipei)
        else:
            failures = int(row.get("consecutive_probe_failures") or 0) + 1
            row["consecutive_probe_failures"] = failures
            row["last_error"] = result.error
            backoff = min(
                _probe_interval(specs_by_name[result.dataset], args),
                30.0 * (2 ** min(failures - 1, 4)),
            )
            row["next_probe_at_taipei"] = (
                checked + timedelta(seconds=max(5.0, backoff))
            ).isoformat(timespec="seconds")
    return changed


def _download_report(metadata_dir: Path) -> dict[str, dict[str, str]]:
    path = metadata_dir / "download_report.csv"
    output: dict[str, dict[str, str]] = {}
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("dataset"):
                    output[str(row["dataset"])] = dict(row)
    except (OSError, UnicodeError, csv.Error):
        return {}
    return output


def _resilient_download_args(args: argparse.Namespace) -> argparse.Namespace:
    """Keep fast probes while giving an observed change time to download."""

    resolved = argparse.Namespace(**vars(args))
    resolved.timeout = max(60, int(getattr(args, "timeout", 20)))
    resolved.retries = max(4, int(getattr(args, "retries", 2)))
    return resolved


def _refresh_pending(
    names: list[str],
    *,
    state: dict[str, Any],
    live_root: Path,
    state_root: Path,
    specs_by_name: Mapping[str, DatasetSpec],
    args: argparse.Namespace,
) -> dict[str, Any]:
    started = datetime.now(TAIPEI)
    run_id = started.strftime("%Y%m%dT%H%M%S%f")
    metadata_dir = state_root / "download_runs" / run_id
    phase = PublicationPhase(
        name="source_event",
        anchor=datetime_time(0, 0),
        selectors=tuple(names),
        official_basis="observed official HTTP representation version change",
    )
    historical = [
        name for name in names if specs_by_name[name].kind == "historical_json_table"
    ]
    download_args = _resilient_download_args(args)
    if historical and started.timetz().replace(tzinfo=None) < datetime_time(13, 30):
        try:
            end_date = _latest_completed_taiex_session(live_root, observed=started)
        except Exception as exc:
            # A transient calendar read/download failure is a dataset-level
            # blocker, not a daemon-fatal exception.  Persist it so the public
            # monitor can expose the failure and let the long-running process
            # retry with backoff instead of entering a systemd restart storm.
            completed_at = datetime.now(TAIPEI)
            rows = state.get("datasets")
            rows = dict(rows) if isinstance(rows, Mapping) else {}
            failed: list[dict[str, Any]] = []
            for name in names:
                row = rows[name]
                failures = int(row.get("consecutive_download_failures") or 0) + 1
                row["last_download_status"] = "blocked_taiex_session_calendar"
                row["last_download_error"] = str(exc)
                row["consecutive_download_failures"] = failures
                row["next_probe_at_taipei"] = (
                    completed_at
                    + timedelta(
                        seconds=min(60.0, 5.0 * (2 ** min(failures - 1, 4)))
                    )
                ).isoformat(timespec="seconds")
                failed.append(
                    {
                        "dataset": name,
                        "status": "blocked_taiex_session_calendar",
                        "message": "TAIFEX session calendar is temporarily unavailable",
                    }
                )
            payload = {
                "schema_version": SCHEMA_VERSION,
                "event_id": run_id,
                "status": "failed",
                "event_kind": (
                    "bootstrap"
                    if not state.get("bootstrap_completed_at_taipei")
                    else "version_change"
                ),
                "started_at_taipei": started.isoformat(timespec="seconds"),
                "completed_at_taipei": completed_at.isoformat(timespec="seconds"),
                "lock_wait_ms": 0.0,
                "triggered_dataset_count": len(names),
                "triggered_datasets": names,
                "accepted_dataset_count": 0,
                "accepted_datasets": [],
                "failed_dataset_count": len(failed),
                "failed_datasets": failed,
                "content_changed_datasets": [],
                "download_end_date": None,
                "commands": [],
                "return_codes": [],
                "metadata_dir": str(metadata_dir),
                "calendar_status": "unavailable",
                "calendar_error": str(exc),
            }
            _atomic_json(state_root / "events" / "latest.json", payload)
            _atomic_json(state_root / "events" / "runs" / f"{run_id}.json", payload)
            return payload
    else:
        end_date = "today"
    commands: list[list[str]] = []
    if historical and started.timetz().replace(tzinfo=None) >= datetime_time(13, 30):
        commands.append(
            _taiex_calendar_command(live_root=live_root, args=download_args)
        )
    commands.append(
        _download_command(
            live_root=live_root,
            metadata_dir=metadata_dir,
            phase=phase,
            args=download_args,
            end_date=end_date,
        )
    )
    lock_path = live_root.parent / ".locks" / "tw-public-refresh.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    before: dict[str, dict[str, object]] = {}
    after: dict[str, dict[str, object]] = {}
    return_codes: list[int] = []
    lock_started = time.perf_counter()
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        lock_wait_ms = (time.perf_counter() - lock_started) * 1000.0
        before = _file_hashes(live_root, names)
        for command in commands:
            completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
            return_codes.append(int(completed.returncode))
            # Calendar refresh failure must not suppress unrelated source-event
            # downloads; the downloader remains the semantic authority.
        after = _file_hashes(live_root, names)
    report = _download_report(metadata_dir)
    accepted: list[str] = []
    failed: list[dict[str, Any]] = []
    rows = state.get("datasets")
    rows = dict(rows) if isinstance(rows, Mapping) else {}
    for name in names:
        result = report.get(name)
        status = str((result or {}).get("status") or "missing_report")
        if result is not None and status not in BLOCKING_DOWNLOAD_STATUSES:
            accepted.append(name)
            row = rows[name]
            row["applied_version"] = row.get("observed_version")
            row["last_applied_at_taipei"] = datetime.now(TAIPEI).isoformat(
                timespec="seconds"
            )
            row["last_download_status"] = status
            row["last_download_error"] = None
            row["consecutive_download_failures"] = 0
            row["pending_since_taipei"] = None
        else:
            row = rows[name]
            row["last_download_status"] = status
            row["last_download_error"] = (result or {}).get("message")
            download_failures = int(row.get("consecutive_download_failures") or 0) + 1
            row["consecutive_download_failures"] = download_failures
            row["next_probe_at_taipei"] = (
                datetime.now(TAIPEI)
                + timedelta(seconds=min(60.0, 5.0 * (2 ** min(download_failures - 1, 4))))
            ).isoformat(timespec="seconds")
            failed.append(
                {
                    "dataset": name,
                    "status": status,
                    "message": (result or {}).get("message"),
                }
            )
    completed_at = datetime.now(TAIPEI)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": run_id,
        "status": "ok" if not failed else "partial" if accepted else "failed",
        "event_kind": "bootstrap" if not state.get("bootstrap_completed_at_taipei") else "version_change",
        "started_at_taipei": started.isoformat(timespec="seconds"),
        "completed_at_taipei": completed_at.isoformat(timespec="seconds"),
        "lock_wait_ms": lock_wait_ms,
        "triggered_dataset_count": len(names),
        "triggered_datasets": names,
        "accepted_dataset_count": len(accepted),
        "accepted_datasets": accepted,
        "failed_dataset_count": len(failed),
        "failed_datasets": failed,
        "content_changed_datasets": _changed_files(before, after),
        "download_end_date": end_date,
        "commands": commands,
        "return_codes": return_codes,
        "metadata_dir": str(metadata_dir),
    }
    _atomic_json(state_root / "events" / "latest.json", payload)
    _atomic_json(state_root / "events" / "runs" / f"{run_id}.json", payload)
    return payload


def _summarize_state(
    state: dict[str, Any],
    *,
    specs: list[DatasetSpec],
    changed: list[str],
) -> None:
    rows = state.get("datasets")
    rows = dict(rows) if isinstance(rows, Mapping) else {}
    observed = [name for name, row in rows.items() if row.get("observed_version")]
    failed_probes = [
        name for name, row in rows.items() if row.get("last_probe_status") == "failed"
    ]
    pending = [
        name
        for name, row in rows.items()
        if row.get("observed_version")
        and row.get("applied_version") != row.get("observed_version")
    ]
    coverage = len(rows) == len(specs) and len(observed) == len(specs)
    state.update(
        {
            "status": (
                "ok"
                if coverage and not failed_probes and not pending
                else "warming"
                if not coverage
                else "degraded"
            ),
            "coverage_complete": coverage,
            "registered_dataset_count": len(specs),
            "monitored_dataset_count": len(rows),
            "observed_dataset_count": len(observed),
            "failed_probe_count": len(failed_probes),
            "failed_probe_datasets": sorted(failed_probes),
            "unapplied_event_count": len(pending),
            "unapplied_event_datasets": sorted(pending),
            "last_cycle_changed_count": len(changed),
            "last_cycle_changed_datasets": sorted(changed),
            "source_counts": dict(
                sorted(Counter(spec.source for spec in specs).items())
            ),
            "updated_at_taipei": datetime.now(TAIPEI).isoformat(
                timespec="seconds"
            ),
        }
    )


def _due_specs(
    specs: list[DatasetSpec], state: Mapping[str, Any], observed: datetime
) -> list[DatasetSpec]:
    rows = state.get("datasets")
    rows = dict(rows) if isinstance(rows, Mapping) else {}
    due: list[DatasetSpec] = []
    for spec in specs:
        row = rows.get(spec.name)
        row = dict(row) if isinstance(row, Mapping) else {}
        next_at = _parse_timestamp(row.get("next_probe_at_taipei"))
        if next_at is None or next_at <= observed + timedelta(seconds=5):
            due.append(spec)
    return due


def _handle_stop(_signum: int, _frame: Any) -> None:
    _STOP.set()


def main() -> int:
    args = parse_args()
    for field in (
        "workers",
        "date_workers",
        "probe_workers",
        "timeout",
        "heartbeat_seconds",
        "fast_interval_seconds",
        "medium_interval_seconds",
        "slow_interval_seconds",
    ):
        if float(getattr(args, field)) <= 0:
            raise ValueError(f"--{field.replace('_', '-')} must be positive")
    if int(args.retries) < 0:
        raise ValueError("--retries must be non-negative")
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    live_root = args.live_root.expanduser().resolve(strict=True)
    state_root = (
        args.state_root
        if args.state_root.is_absolute()
        else REPO_ROOT / args.state_root
    ).resolve(strict=False)
    state_path = state_root / "latest.json"
    specs = list(DEFAULT_DATASETS.values())
    specs_by_name = {spec.name: spec for spec in specs}
    state = _load_state(state_path, specs, args)
    limiter = SharedRateLimiter(
        resolve_request_interval("tw_public", None), name="tw_public"
    )
    last_heartbeat = 0.0
    cycles = 0
    while not _STOP.is_set():
        observed = datetime.now(TAIPEI)
        due = _due_specs(specs, state, observed)
        changed: list[str] = []
        if due:
            state["cycle_started_at_taipei"] = observed.isoformat(timespec="seconds")
            state["cycle_due_dataset_count"] = len(due)
            _summarize_state(state, specs=specs, changed=[])
            _atomic_json(state_path, state)
            results = _probe_specs(
                due,
                observed=observed,
                rows=state.get("datasets", {}),
                limiter=limiter,
                args=args,
            )
            changed = _apply_probe_results(
                state,
                results,
                specs_by_name=specs_by_name,
                args=args,
            )
            rows = state.get("datasets", {})
            pending = sorted(
                name
                for name, row in rows.items()
                if row.get("observed_version")
                and row.get("applied_version") != row.get("observed_version")
            )
            if args.no_bootstrap_refresh:
                baseline_names = [
                    name
                    for name in pending
                    if rows[name].get("applied_version") is None
                    and int(rows[name].get("change_count") or 0) == 0
                ]
                for name in baseline_names:
                    rows[name]["applied_version"] = rows[name].get(
                        "observed_version"
                    )
                    rows[name]["pending_since_taipei"] = None
                if baseline_names:
                    state.setdefault(
                        "bootstrap_completed_at_taipei",
                        datetime.now(TAIPEI).isoformat(timespec="seconds"),
                    )
                pending = [name for name in pending if name not in baseline_names]
            if pending and not args.probe_only:
                state["refresh_in_progress"] = True
                _summarize_state(state, specs=specs, changed=changed)
                _atomic_json(state_path, state)
                refresh = _refresh_pending(
                    pending,
                    state=state,
                    live_root=live_root,
                    state_root=state_root,
                    specs_by_name=specs_by_name,
                    args=args,
                )
                state["last_refresh"] = refresh
                if refresh["failed_dataset_count"] == 0:
                    state.setdefault(
                        "bootstrap_completed_at_taipei",
                        refresh["completed_at_taipei"],
                    )
                state["refresh_in_progress"] = False
            state["cycle_completed_at_taipei"] = datetime.now(TAIPEI).isoformat(
                timespec="seconds"
            )
            cycles += 1
        if due:
            _summarize_state(state, specs=specs, changed=changed)
            _atomic_json(state_path, state)
        if time.monotonic() - last_heartbeat >= float(args.heartbeat_seconds):
            if not due:
                _summarize_state(state, specs=specs, changed=[])
                _atomic_json(state_path, state)
            print(
                json.dumps(
                    {
                        key: state.get(key)
                        for key in (
                            "status",
                            "coverage_complete",
                            "registered_dataset_count",
                            "observed_dataset_count",
                            "failed_probe_count",
                            "unapplied_event_count",
                            "last_cycle_changed_count",
                            "updated_at_taipei",
                        )
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )
            last_heartbeat = time.monotonic()
        if args.once and cycles >= 1:
            break
        _STOP.wait(1.0)
    _summarize_state(state, specs=specs, changed=[])
    state["stopped_at_taipei"] = datetime.now(TAIPEI).isoformat(
        timespec="seconds"
    )
    _atomic_json(state_path, state)
    return 0 if state.get("status") == "ok" or args.probe_only else 1


if __name__ == "__main__":
    raise SystemExit(main())
