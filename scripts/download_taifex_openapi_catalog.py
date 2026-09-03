#!/usr/bin/env python3
"""Snapshot all non-duplicated TAIFEX OpenAPI datasets.

TAIFEX exposes many useful state tables only as rolling/current OpenAPI views.
This collector discovers the official Swagger catalog on every run and starts
an append-only point-in-time archive.  Large time-and-sales feeds and datasets
owned by existing canonical collectors are recorded as delegated rather than
duplicated.
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import gzip
import io
import json
from pathlib import Path
import re
import sys
import time
from typing import Final

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import SharedRateLimiter  # noqa: E402
from scripts.download_taifex_public_history import (  # noqa: E402
    _atomic_write_bytes,
    _atomic_write_parquet,
    _relative,
    _sha256_bytes,
    _utc_now,
    _write_json,
)
from scripts.taifex_daily_download_common import sha256_path  # noqa: E402


CONTRACT_VERSION: Final[int] = 1
SWAGGER_URL: Final[str] = "https://openapi.taifex.com.tw/swagger.json"
API_BASE: Final[str] = "https://openapi.taifex.com.tw/v1"
USER_AGENT: Final[str] = "stockAgent/taifex-openapi-point-in-time-archive"

# These already have stronger canonical collectors and receipts.  The catalog
# manifest keeps the delegation explicit so absence here is not mistaken for a
# missing source.
DELEGATED_ENDPOINTS: Final[dict[str, str]] = {
    "/DailyMarketReportFut": "data_tw_public/taifex_daily_futures.parquet",
    "/DailyMarketReportOpt": "data_tw_public/taifex_daily_options.parquet",
    "/MarketDataOfMajorInstitutionalTradersGeneralBytheDate": (
        "data_tw_public/taifex_institutional_total.parquet"
    ),
    "/OpenInterestOfLargeTradersFutures": (
        "data_tw_public/taifex_large_trader_futures_oi.parquet"
    ),
    "/FinalSettlementPrice": "data_tw_public/taifex_final_settlement_price.parquet",
    "/TimeAndSalesData": "data_tw_shioaji_history plus official recent tick archive",
    "/OptionsTimeAndSalesData": (
        "data_tw_shioaji_history plus data_tw_index_derivatives_ticks"
    ),
    "/TimeAndSalesDataOnCalendarSpreadOrders": (
        "canonical transaction-tape collectors; intentionally not duplicated"
    ),
}


def _slug(path: str) -> str:
    value = re.sub(r"(?<!^)(?=[A-Z])", "_", path.strip("/"))
    return re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")


def _json_rows(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, list):
        values = payload
    elif isinstance(payload, dict):
        values = [payload]
    else:
        values = [{"value": payload}]
    rows: list[dict[str, object]] = []
    for value in values:
        source = value if isinstance(value, dict) else {"value": value}
        row: dict[str, object] = {}
        for key, cell in source.items():
            if cell is None:
                row[str(key)] = None
            elif isinstance(cell, (dict, list)):
                row[str(key)] = json.dumps(
                    cell, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            else:
                row[str(key)] = str(cell)
        rows.append(row)
    return rows


def _normalized_frame(
    payload: object,
    *,
    endpoint: str,
    captured_at: str,
    content_sha256: str,
) -> pd.DataFrame:
    rows = _json_rows(payload)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows, dtype="string")
    frame.insert(0, "_source_path", endpoint)
    frame.insert(0, "_captured_at_utc", captured_at)
    frame.insert(0, "_content_sha256", content_sha256)
    return frame


def _valid_receipt(root: Path, capture_date: date, endpoint: str) -> dict[str, object] | None:
    receipt_path = root / "receipts" / "openapi" / capture_date.isoformat() / f"{_slug(endpoint)}.json"
    if not receipt_path.is_file():
        return None
    try:
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        if (
            payload.get("contract_version") != CONTRACT_VERSION
            or payload.get("endpoint") != endpoint
            or payload.get("capture_date") != capture_date.isoformat()
            or payload.get("status") not in {"complete", "source_empty"}
        ):
            return None
        raw_path = root / str(payload["raw_path"])
        if (
            not raw_path.is_file()
            or raw_path.stat().st_size != int(payload["raw_bytes"])
            or sha256_path(raw_path) != payload["raw_sha256"]
        ):
            return None
        if payload.get("status") == "complete":
            normalized_path = root / str(payload["normalized_path"])
            if (
                not normalized_path.is_file()
                or normalized_path.stat().st_size != int(payload["normalized_bytes"])
                or sha256_path(normalized_path) != payload["normalized_sha256"]
            ):
                return None
        return payload
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _parse_structured_content(content: bytes, content_type: str) -> tuple[object, str]:
    probe = content.lstrip()
    if "json" in content_type.casefold() or probe.startswith((b"[", b"{")):
        return json.loads(content), "json"

    # Two endpoints advertised by the official Swagger catalog currently return
    # UTF-8 CSV with application/octet-stream. Sniff the body instead of
    # trusting the inconsistent Content-Type header.
    decoded = content.decode("utf-8-sig")
    sample = decoded[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",")
    except csv.Error as exc:
        raise ValueError(f"unsupported structured content type {content_type!r}") from exc
    reader = csv.DictReader(io.StringIO(decoded), dialect=dialect)
    if not reader.fieldnames:
        raise ValueError("CSV response has no header")
    return list(reader), "csv"


def _fetch_structured(
    session: requests.Session,
    limiter: SharedRateLimiter,
    endpoint: str,
    *,
    attempts: int,
) -> tuple[bytes, object, str, dict[str, str], str]:
    url = API_BASE + endpoint
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            limiter.wait()
            response = session.get(url, timeout=180)
            response.raise_for_status()
            content = response.content
            content_type = str(response.headers.get("content-type") or "")
            payload, payload_format = _parse_structured_content(content, content_type)
            return (
                content,
                payload,
                _utc_now(),
                {key.lower(): value for key, value in response.headers.items()},
                payload_format,
            )
        except Exception as exc:
            last_error = exc
            if attempt == attempts:
                break
            response = getattr(exc, "response", None)
            retry_after = None if response is None else response.headers.get("Retry-After")
            if response is not None and response.status_code == 429:
                try:
                    delay = max(60.0, float(retry_after or 0.0))
                except ValueError:
                    delay = 60.0
                print(
                    f"[rate-limit] TAIFEX OpenAPI HTTP 429; cooling down {delay:.0f}s "
                    f"before attempt {attempt + 1}/{attempts}",
                    flush=True,
                )
            else:
                delay = min(30.0, float(2**attempt))
            limiter.defer(delay)
            time.sleep(min(delay, 60.0))
    detail = "unknown error" if last_error is None else f"{type(last_error).__name__}: {last_error}"
    raise RuntimeError(f"failed to fetch OpenAPI endpoint {endpoint}: {detail}") from last_error


def _persist(
    root: Path,
    *,
    capture_date: date,
    endpoint: str,
    content: bytes,
    payload: object,
    captured_at: str,
    headers: dict[str, str],
    payload_format: str,
) -> dict[str, object]:
    digest = _sha256_bytes(content)
    slug = _slug(endpoint)
    stored = gzip.compress(content, compresslevel=9, mtime=0)
    raw_path = root / "raw" / "openapi" / slug / f"{digest}.{payload_format}.gz"
    receipt_path = root / "receipts" / "openapi" / capture_date.isoformat() / f"{slug}.json"
    _atomic_write_bytes(raw_path, stored)
    frame = _normalized_frame(
        payload,
        endpoint=endpoint,
        captured_at=captured_at,
        content_sha256=digest,
    )
    receipt: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "capture_date": capture_date.isoformat(),
        "endpoint": endpoint,
        "source_url": API_BASE + endpoint,
        "status": "complete" if not frame.empty else "source_empty",
        "rows": len(frame),
        "captured_at_utc": captured_at,
        "response_content_type": headers.get("content-type"),
        "response_format": payload_format,
        "response_bytes": len(content),
        "response_sha256": digest,
        "raw_path": _relative(raw_path, root),
        "raw_bytes": raw_path.stat().st_size,
        "raw_sha256": sha256_path(raw_path),
        "point_in_time_rule": "not_available_before_captured_at_utc",
    }
    if not frame.empty:
        normalized_path = root / "shards" / "openapi" / slug / f"{digest}.parquet"
        if not normalized_path.is_file():
            _atomic_write_parquet(frame, normalized_path)
        receipt.update(
            normalized_path=_relative(normalized_path, root),
            normalized_bytes=normalized_path.stat().st_size,
            normalized_sha256=sha256_path(normalized_path),
        )
    _write_json(receipt_path, receipt)
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data_taifex_public_history")
    parser.add_argument("--capture-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--request-interval", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=5)
    args = parser.parse_args()
    if args.request_interval < 0.1:
        parser.error("--request-interval must be at least 0.1 seconds")
    if args.attempts < 1:
        parser.error("--attempts must be positive")

    root = Path(args.output_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    limiter = SharedRateLimiter(args.request_interval, name="taifex_openapi_catalog")
    limiter.wait()
    swagger_response = session.get(SWAGGER_URL, timeout=120)
    swagger_response.raise_for_status()
    swagger = swagger_response.json()
    endpoints = sorted(
        path
        for path, operations in swagger.get("paths", {}).items()
        if isinstance(operations, dict) and "get" in operations
    )
    if not endpoints:
        raise RuntimeError("official TAIFEX Swagger catalog contains no GET endpoints")
    swagger_content = swagger_response.content
    swagger_digest = _sha256_bytes(swagger_content)
    swagger_path = root / "raw" / "openapi" / "swagger" / f"{swagger_digest}.json"
    _atomic_write_bytes(swagger_path, swagger_content)

    completed: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    for index, endpoint in enumerate(endpoints, start=1):
        if endpoint in DELEGATED_ENDPOINTS:
            continue
        receipt = _valid_receipt(root, args.capture_date, endpoint)
        if receipt is None:
            try:
                content, payload, captured_at, headers, payload_format = _fetch_structured(
                    session, limiter, endpoint, attempts=args.attempts
                )
                receipt = _persist(
                    root,
                    capture_date=args.capture_date,
                    endpoint=endpoint,
                    content=content,
                    payload=payload,
                    captured_at=captured_at,
                    headers=headers,
                    payload_format=payload_format,
                )
            except Exception as exc:
                failures.append({"endpoint": endpoint, "error": str(exc)})
                print(f"[openapi] failed {endpoint}: {exc}", flush=True)
                continue
        completed.append(
            {
                "endpoint": endpoint,
                "status": receipt["status"],
                "rows": receipt["rows"],
                "response_sha256": receipt["response_sha256"],
            }
        )
        print(
            f"[openapi] {index}/{len(endpoints)} {endpoint} "
            f"rows={receipt['rows']} status={receipt['status']}",
            flush=True,
        )

    manifest = {
        "contract_version": CONTRACT_VERSION,
        "dataset": "taifex_openapi_point_in_time_catalog",
        "status": "complete" if not failures else "partial",
        "capture_date": args.capture_date.isoformat(),
        "captured_at_utc": _utc_now(),
        "swagger_url": SWAGGER_URL,
        "swagger_path": _relative(swagger_path, root),
        "swagger_sha256": swagger_digest,
        "catalog_endpoints": len(endpoints),
        "captured_endpoints": len(completed),
        "delegated_endpoints": DELEGATED_ENDPOINTS,
        "failed_endpoints": failures,
        "datasets": completed,
        "point_in_time_rule": "not_available_before_receipt_captured_at_utc",
    }
    manifest_path = root / "manifests" / "openapi" / f"{args.capture_date.isoformat()}.json"
    _write_json(manifest_path, manifest)
    _write_json(root / "openapi_latest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    # Partial endpoint availability is visible in the manifest and retried on
    # the next run.  A total failure remains a service failure.
    return 0 if completed else 1


if __name__ == "__main__":
    raise SystemExit(main())
