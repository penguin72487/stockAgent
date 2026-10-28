"""Download FRED initial-release observations for causal crypto macro context."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

import polars as pl

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(SCRIPT_DIR.parent) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR.parent))

from artifact_io import (  # noqa: E402
    atomic_write_bytes as _atomic_bytes,
    atomic_write_json as _atomic_json,
    atomic_write_parquet as _atomic_parquet_zstd,
    sha256_bytes,
    sha256_file,
)
from common import (  # noqa: E402
    PersistentProgress,
    provider_rate_limit,
    retry_delay_seconds,
)
from http_transport import (  # noqa: E402
    HttpRequestPolicy,
    HttpStatusError,
    ResilientHttpTransport,
)
from openbb_credentials import load_openbb_environment  # noqa: E402
from stockagent.data_sync.retained_source import (  # noqa: E402
    RetainedRawSource, collector_owner, load_retained_source,
)
from downloader.fred_projection import (  # noqa: E402
    ClosedProjection, build_manifest as build_projection_manifest, normalizer_identity,
)


BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
DEFAULT_SERIES = (
    "DFF",
    "SOFR",
    "DGS2",
    "DGS10",
    "T10Y2Y",
    "DTWEXBGS",
    "VIXCLS",
    "BAMLH0A0HYM2",
    "WALCL",
    "RRPONTSYD",
    "NFCI",
)
SCHEMA_VERSION = 1
REQUEST_CONTRACT_VERSION = 1


@dataclass(frozen=True, slots=True)
class FredWindow:
    series_id: str
    realtime_start: str
    realtime_end: str


@dataclass(slots=True)
class FredWindowResult:
    window: FredWindow
    status: str
    rows: list[dict[str, object]]
    raw_path: str | None
    raw_sha256: str | None
    point_in_time_gap: str | None
    retrieved_at_utc: str | None
    error: str | None = None
    raw_locator: dict[str, object] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch FRED output_type=4 initial releases. Because the API exposes "
            "release dates but not intraday release times, values become usable "
            "at 00:00 UTC on the following calendar day."
        )
    )
    parser.add_argument("--output-dir", default="data_fred_crypto_macro")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--start-date", default="2000-01-01")
    parser.add_argument("--end-date", default="today")
    parser.add_argument("--series", nargs="*", default=list(DEFAULT_SERIES))
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--historical-recheck-days",
        type=int,
        default=30,
        help=(
            "Reuse a hash-verified closed vintage window for this many days. "
            "The current window is always refreshed."
        ),
    )
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    # Preserve the existing dataset codec while sharing collision-safe publish.
    _atomic_parquet_zstd(path, frame, compression="snappy")


def _configured_api_key(env_file: Path) -> str:
    configured = load_openbb_environment(env_file)
    value = str(os.environ.get("FRED_API_KEY", "")).strip()
    if "FRED_API_KEY" not in configured or not value:
        raise RuntimeError(
            "FRED_API_KEY is not configured in the process environment or allowlisted env file"
        )
    return value


def _fetch_series(
    series_id: str,
    *,
    api_key: str,
    start_date: str,
    end_date: str,
    realtime_start: str,
    realtime_end: str,
    max_retries: int,
    transport: ResilientHttpTransport | None = None,
) -> bytes:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "realtime_start": realtime_start,
        "realtime_end": realtime_end,
        "output_type": 4,
        "sort_order": "asc",
        "limit": 100000,
    }
    url = f"{BASE_URL}?{urlencode(params)}"
    client = transport or ResilientHttpTransport(
        HttpRequestPolicy(
            provider="fred_api",
            timeout_seconds=60,
            max_retries=max_retries,
            retry_base_seconds=1.0,
            retry_cap_seconds=30.0,
        ),
        opener=urlopen,
        sleeper=time.sleep,
        retry_delay=retry_delay_seconds,
    )
    response = client.request_bytes(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "stockAgent-research/1.0",
        },
        accepted_statuses=frozenset({400}),
    )
    if response.status == 200:
        return response.body
    gap = _point_in_time_gap_message(response.body)
    if gap is None:
        raise HttpStatusError(response.status, url, response.body)
    return json.dumps(
        {
            "observations": [],
            "point_in_time_gap": gap,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
    ).encode("utf-8")


def _point_in_time_gap_message(payload: bytes) -> str | None:
    text = payload.decode("utf-8", errors="replace").strip()
    try:
        document = json.loads(text)
    except json.JSONDecodeError:
        document = {}
    candidates = (
        str(document.get("error_message") or ""),
        str(document.get("message") or ""),
        text,
    )
    for candidate in candidates:
        normalized = " ".join(candidate.casefold().split())
        if (
            "does not exist in alfred" in normalized
            or "no vintage dates exist" in normalized
        ):
            return candidate.strip()
    return None


def _realtime_windows(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Keep each request below FRED's 2,000-vintage-date JSON ceiling."""

    cursor = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    windows: list[tuple[str, str]] = []
    while cursor <= final:
        window_end = min(final, cursor + timedelta(days=1824))
        windows.append((cursor.isoformat(), window_end.isoformat()))
        cursor = window_end + timedelta(days=1)
    return windows


def _resolve_end_date(
    requested: str, *, series_id: str, api_key: str,
    transport: ResilientHttpTransport,
) -> tuple[str, dict[str, str]]:
    """Pin the run to FRED's own current date, independent of host timezone.

    Omitting realtime_start/end uses the provider's documented current-date
    defaults. A one-row probe supplies that boundary without guessing its
    timezone or requesting a future vintage at the Taiwan daily schedule.
    """
    requested_day = None if requested in {"today", "now"} else date.fromisoformat(requested)
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json", "limit": 1}
    response = transport.request_bytes(
        f"{BASE_URL}?{urlencode(params)}", headers={"Accept": "application/json"},
    )
    document = json.loads(response.body)
    provider_day = date.fromisoformat(str(document["realtime_end"]))
    if provider_day == date.max:
        raise ValueError("FRED current-date probe returned an unbounded realtime_end")
    effective = min(requested_day, provider_day) if requested_day else provider_day
    return effective.isoformat(), {
        "requested_end_date": requested,
        "provider_current_date": provider_day.isoformat(),
        "effective_end_date": effective.isoformat(),
        "end_date_basis": "fred_default_realtime_end",
    }


def parse_initial_release_rows(
    series_id: str, payload: bytes, *, retrieved_at_utc: datetime
) -> list[dict[str, object]]:
    document = json.loads(payload)
    if not isinstance(document, dict) or not isinstance(document.get("observations"), list):
        raise ValueError("FRED payload must contain an observations array")
    observations = document["observations"]
    if "count" in document and int(document["count"]) != len(observations):
        raise ValueError("FRED observation response is incomplete; count differs from returned rows")
    rows: list[dict[str, object]] = []
    raw_sha256 = sha256_bytes(payload)
    for item in observations:
        raw_value = str(item.get("value") or "").strip()
        if raw_value in {"", "."}:
            continue
        observation_date = date.fromisoformat(str(item["date"]))
        release_date = date.fromisoformat(str(item["realtime_start"]))
        # The daily endpoint supplies release dates, not release timestamps.
        # Waiting until the next UTC boundary guarantees that the release was
        # already public without assuming a US-market release hour.
        available_at = datetime.combine(
            release_date + timedelta(days=1),
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        rows.append(
            {
                "series_id": series_id,
                "observation_date": observation_date.isoformat(),
                "value": float(raw_value),
                "realtime_start": release_date.isoformat(),
                "realtime_end": str(item.get("realtime_end") or ""),
                "available_at_utc": available_at.isoformat(),
                "retrieved_at_utc": retrieved_at_utc.isoformat(),
                "point_in_time_state": "fred_initial_release_next_utc_day",
                "raw_sha256": raw_sha256,
            }
        )
    return rows


def _empty_initial_release_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "series_id": pl.String,
            "observation_date": pl.String,
            "value": pl.Float64,
            "realtime_start": pl.String,
            "realtime_end": pl.String,
            "available_at_utc": pl.String,
            "retrieved_at_utc": pl.String,
            "point_in_time_state": pl.String,
            "raw_sha256": pl.String,
        }
    )


def _window_receipt_path(output_dir: Path, window: FredWindow) -> Path:
    return (
        output_dir
        / "receipts"
        / window.series_id
        / f"{window.realtime_start}_{window.realtime_end}.json"
    )


def _read_cached_window(
    output_dir: Path,
    window: FredWindow,
    *,
    observation_start: str,
    end_date: str,
    recheck_days: int,
    now: datetime,
    retained_raw: RetainedRawSource | None = None,
    projection: ClosedProjection | None = None,
) -> FredWindowResult | None:
    if window.realtime_end >= end_date or recheck_days <= 0:
        return None
    receipt_path = _window_receipt_path(output_dir, window)
    try:
        receipt_raw = receipt_path.read_bytes()
        receipt = json.loads(receipt_raw)
        if (
            receipt.get("status") not in {"complete", "complete_with_point_in_time_gap"}
            or receipt.get("series_id") != window.series_id
            or receipt.get("realtime_start") != window.realtime_start
            or receipt.get("realtime_end") != window.realtime_end
            or int(receipt.get("request_contract_version", -1)) != REQUEST_CONTRACT_VERSION
            or str(receipt.get("observation_start") or "") != observation_start
            or int(receipt.get("output_type", -1)) != 4
        ):
            return None
        completed = datetime.fromisoformat(
            str(receipt["completed_at_utc"]).replace("Z", "+00:00")
        )
        if completed.tzinfo is None:
            completed = completed.replace(tzinfo=timezone.utc)
        if now - completed.astimezone(timezone.utc) > timedelta(days=recheck_days):
            return None
        relative_raw = Path(str(receipt["raw_path"]))
        if relative_raw.is_absolute() or ".." in relative_raw.parts:
            return None
        raw_path = output_dir / relative_raw
        raw_locator = None
        projected = projection.lookup(window, receipt_raw, retained_raw=retained_raw) if projection else None
        if projected is not None:
            return FredWindowResult(window=window, status='cached_verified', rows=projected['rows'],
                raw_path=str(raw_path), raw_sha256=projected['raw_sha256'],
                point_in_time_gap=projected['point_in_time_gap'], retrieved_at_utc=projected['retrieved_at_utc'],
                raw_locator=(retained_raw.locator(relative_raw.as_posix())
                    if not raw_path.exists() and retained_raw is not None else None))
        try:
            compressed = raw_path.read_bytes()
        except FileNotFoundError:
            compressed = retained_raw.get(relative_raw.as_posix()) if retained_raw is not None else None
            if compressed is None:
                return None
            raw_locator = retained_raw.locator(relative_raw.as_posix())
        payload = gzip.decompress(compressed)
        expected_sha = str(receipt["raw_sha256"])
        if sha256_bytes(payload) != expected_sha:
            return None
        content_first_observed = datetime.fromisoformat(
            str(
                receipt.get("content_first_observed_at_utc")
                or receipt["completed_at_utc"]
            ).replace("Z", "+00:00")
        )
        if content_first_observed.tzinfo is None:
            content_first_observed = content_first_observed.replace(tzinfo=timezone.utc)
        rows = parse_initial_release_rows(
            window.series_id,
            payload,
            retrieved_at_utc=content_first_observed.astimezone(timezone.utc),
        )
    except (
        KeyError,
        OSError,
        ValueError,
        TypeError,
        json.JSONDecodeError,
        gzip.BadGzipFile,
        EOFError,
    ):
        return None
    document = json.loads(payload)
    return FredWindowResult(
        window=window,
        status="cached_verified",
        rows=rows,
        raw_path=str(raw_path),
        raw_sha256=expected_sha,
        point_in_time_gap=str(document.get("point_in_time_gap") or "").strip() or None,
        retrieved_at_utc=content_first_observed.astimezone(timezone.utc).isoformat(),
        raw_locator=raw_locator,
    )


def _fetch_window(
    output_dir: Path,
    window: FredWindow,
    *,
    api_key: str,
    observation_start: str,
    observation_end: str,
    max_retries: int,
    transport: ResilientHttpTransport,
    retained_raw: RetainedRawSource | None = None,
) -> FredWindowResult:
    retrieved = datetime.now(timezone.utc)
    receipt_path = _window_receipt_path(output_dir, window)
    try:
        payload = _fetch_series(
            window.series_id,
            api_key=api_key,
            start_date=observation_start,
            end_date=observation_end,
            realtime_start=window.realtime_start,
            realtime_end=window.realtime_end,
            max_retries=max_retries,
            transport=transport,
        )
        raw_sha256 = sha256_bytes(payload)
        relative_raw = (
            Path("raw")
            / window.series_id
            / (f"{window.realtime_start}_{window.realtime_end}_{raw_sha256}.json.gz")
        )
        raw_path = output_dir / relative_raw
        same_content = False
        raw_locator = None
        encoded = gzip.compress(payload, mtime=0)
        if raw_path.is_file():
            try:
                same_content = gzip.decompress(raw_path.read_bytes()) == payload
            except (OSError, gzip.BadGzipFile, EOFError):
                same_content = False
        elif retained_raw is not None:
            if retained_raw.matches_encoded(relative_raw.as_posix(), encoded):
                # Fresh API bytes exactly match the fixed encoded original.
                # Its binding is not a fresh cold-recovery proof; publication
                # still verifies every retained original before commitment.
                same_content = True
                raw_locator = retained_raw.locator(relative_raw.as_posix())
            else:
                compressed = retained_raw.get(relative_raw.as_posix())
                if compressed is not None:
                    same_content = gzip.decompress(compressed) == payload
                    if not same_content:
                        raise ValueError('fixed retained FRED raw differs from its content-addressed payload')
                    raw_locator = retained_raw.locator(relative_raw.as_posix())
        if not same_content:
            _atomic_bytes(raw_path, encoded)
        content_first_observed_at = retrieved
        if same_content:
            try:
                previous = json.loads(receipt_path.read_text(encoding="utf-8"))
                if str(previous.get("raw_sha256") or "") == raw_sha256:
                    content_first_observed_at = datetime.fromisoformat(
                        str(
                            previous.get("content_first_observed_at_utc")
                            or previous["completed_at_utc"]
                        ).replace("Z", "+00:00")
                    )
                    if content_first_observed_at.tzinfo is None:
                        content_first_observed_at = content_first_observed_at.replace(
                            tzinfo=timezone.utc
                        )
                    content_first_observed_at = content_first_observed_at.astimezone(
                        timezone.utc
                    )
            except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
                content_first_observed_at = retrieved
        document = json.loads(payload)
        gap = str(document.get("point_in_time_gap") or "").strip() or None
        rows = parse_initial_release_rows(
            window.series_id,
            payload,
            retrieved_at_utc=content_first_observed_at,
        )
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "request_contract_version": REQUEST_CONTRACT_VERSION,
            "status": "complete_with_point_in_time_gap" if gap else "complete",
            "series_id": window.series_id,
            "realtime_start": window.realtime_start,
            "realtime_end": window.realtime_end,
            "observation_start": observation_start,
            "observation_end_at_fetch": observation_end,
            "output_type": 4,
            "rows": len(rows),
            "raw_path": str(relative_raw),
            "raw_sha256": raw_sha256,
            "point_in_time_gap": gap,
            "content_first_observed_at_utc": content_first_observed_at.isoformat(),
            "completed_at_utc": retrieved.isoformat(),
            **({'raw_storage': raw_locator} if raw_locator is not None else {}),
        }
        _atomic_json(receipt_path, receipt)
        return FredWindowResult(
            window=window,
            status=str(receipt["status"]),
            rows=rows,
            raw_path=str(raw_path),
            raw_sha256=raw_sha256,
            point_in_time_gap=gap,
            retrieved_at_utc=content_first_observed_at.isoformat(),
            raw_locator=raw_locator,
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _atomic_json(
            receipt_path,
            {
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "series_id": window.series_id,
                "realtime_start": window.realtime_start,
                "realtime_end": window.realtime_end,
                "error": error,
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )
        return FredWindowResult(
            window=window,
            status="failed",
            rows=[],
            raw_path=None,
            raw_sha256=None,
            point_in_time_gap=None,
            retrieved_at_utc=None,
            error=error,
        )


def main() -> None:
    args = parse_args()
    with collector_owner(Path(args.output_dir), 'fred-crypto-macro'):
        # A malformed binding fails before credentials/provider calls; never
        # turn a missing/corrupt cold authority into thousands of API refetches.
        retained_raw = load_retained_source(Path(args.output_dir), 'fred-crypto-macro',
            read_originals=False, defer_originals=True)
        _run_collection(args, retained_raw=retained_raw)


def _run_collection(args: argparse.Namespace, *, retained_raw: RetainedRawSource | None = None,
                    reuse_closed_projection: bool = True) -> None:
    started = datetime.now(timezone.utc)
    output_dir = Path(args.output_dir)
    api_key = _configured_api_key(Path(args.env_file))
    try:
        start_day = date.fromisoformat(args.start_date)
        if args.end_date not in {"today", "now"}:
            date.fromisoformat(args.end_date)
    except ValueError as exc:
        raise ValueError("FRED start/end dates must use ISO YYYY-MM-DD") from exc
    series = tuple(
        dict.fromkeys(
            str(value).strip().upper() for value in args.series if str(value).strip()
        )
    )
    if not series:
        raise ValueError("at least one FRED series is required")
    progress: PersistentProgress | None = None
    transport = ResilientHttpTransport(
        HttpRequestPolicy(
            provider="fred_api", timeout_seconds=60, max_retries=args.max_retries,
            retry_base_seconds=1.0, retry_cap_seconds=30.0,
        ),
        on_attempt=lambda _provider: progress.observe(
            "fred_request", "http_attempts", publish_interval_seconds=1.0
        ) if progress is not None else None,
    )
    try:
        end_date, date_boundary = _resolve_end_date(
            args.end_date, series_id=series[0], api_key=api_key, transport=transport,
        )
    except Exception as exc:
        output_path = output_dir / "observations.parquet"
        _atomic_json(output_dir / "download_summary.json", {
            "schema_version": SCHEMA_VERSION, "state": "failed",
            "failed_stage": "provider_date_probe", "requested_end_date": args.end_date,
            "canonical_output_preserved": output_path.is_file(),
            "error": f"{type(exc).__name__}: {exc}",
            "started_at_utc": started.isoformat(),
            "ended_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        raise
    if date.fromisoformat(end_date) < start_day:
        raise ValueError("FRED effective end date precedes start date")
    windows = [
        FredWindow(series_id, realtime_start, realtime_end)
        for series_id in series
        for realtime_start, realtime_end in _realtime_windows(args.start_date, end_date)
    ]
    progress = PersistentProgress(
        output_dir / "progress.json",
        label="FRED crypto macro vintages",
        total=len(windows),
        unit="series-window",
        basis="hash-verified cached or completed FRED vintage windows",
        started_at=started,
    )
    results: list[FredWindowResult] = []
    pending: list[FredWindow] = []
    identity = normalizer_identity((parse_initial_release_rows, _empty_initial_release_frame,
        _realtime_windows, _read_cached_window, _fetch_window, _run_collection),
        parameters={'schema_version': SCHEMA_VERSION, 'request_contract_version': REQUEST_CONTRACT_VERSION})
    projection = (ClosedProjection.load(output_dir, identity=identity, schema=_empty_initial_release_frame().schema)
        if reuse_closed_projection and not args.refresh else None)
    now = datetime.now(timezone.utc)
    for window in windows:
        cached = (
            None
            if args.refresh
            else _read_cached_window(
                output_dir,
                window,
                observation_start=args.start_date,
                end_date=end_date,
                recheck_days=max(0, int(args.historical_recheck_days)),
                now=now,
                retained_raw=retained_raw,
                projection=projection,
            )
        )
        if cached is None:
            pending.append(window)
            continue
        results.append(cached)
        progress.update(f"{window.series_id}:{window.realtime_start}", cached.status)

    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _fetch_window,
                output_dir,
                window,
                api_key=api_key,
                observation_start=args.start_date,
                observation_end=end_date,
                max_retries=args.max_retries,
                transport=transport,
                retained_raw=retained_raw,
            ): window
            for window in pending
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            progress.update(
                f"{result.window.series_id}:{result.window.realtime_start}",
                result.status,
            )

    results.sort(
        key=lambda item: (
            item.window.series_id,
            item.window.realtime_start,
            item.window.realtime_end,
        )
    )
    failed = [item for item in results if item.status == "failed"]
    source_receipts: list[dict[str, object]] = []
    all_rows: list[dict[str, object]] = []
    for series_id in series:
        selected = [item for item in results if item.window.series_id == series_id]
        rows = [row for item in selected for row in item.rows]
        all_rows.extend(rows)
        gaps = [
            {
                "realtime_start": item.window.realtime_start,
                "realtime_end": item.window.realtime_end,
                "message": item.point_in_time_gap,
            }
            for item in selected
            if item.point_in_time_gap
        ]
        source_receipts.append(
            {
                "series_id": series_id,
                "status": (
                    "failed"
                    if any(item.status == "failed" for item in selected)
                    else "complete_with_point_in_time_gap_windows"
                    if gaps
                    else "complete"
                ),
                "rows": len(rows),
                "cached_windows": sum(
                    item.status == "cached_verified" for item in selected
                ),
                "fetched_windows": sum(
                    item.status != "cached_verified" for item in selected
                ),
                "raw_paths": [item.raw_path for item in selected if item.raw_path],
                "raw_sha256": [item.raw_sha256 for item in selected if item.raw_sha256],
                "raw_locations": [
                    {'raw_path': item.raw_path, 'storage': item.raw_locator or {'backend': 'workspace'}}
                    for item in selected if item.raw_path
                ],
                "point_in_time_gap_windows": gaps,
                "failed_windows": [
                    {
                        "realtime_start": item.window.realtime_start,
                        "realtime_end": item.window.realtime_end,
                        "error": item.error,
                    }
                    for item in selected
                    if item.status == "failed"
                ],
            }
        )
    output_path = output_dir / "observations.parquet"
    if retained_raw is not None:
        retained_raw.check_generation()
    if projection is not None:
        projection.check_generation()
    if failed:
        summary = {
            "schema_version": SCHEMA_VERSION,
            "state": "failed",
            "source": "FRED series observations output_type=4",
            "series": list(series),
            "start_date": args.start_date,
            "end_date": end_date,
            **date_boundary,
            "failed_windows": len(failed),
            "canonical_output_preserved": output_path.is_file(),
            "canonical_output_path": str(output_path)
            if output_path.is_file()
            else None,
            "canonical_output_sha256": sha256_file(output_path)
            if output_path.is_file()
            else None,
            "sources": source_receipts,
            "started_at_utc": started.isoformat(),
            "ended_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_json(output_dir / "download_summary.json", summary)
        progress.finish(failed=True, require_exact=True)
        print(json.dumps(summary, ensure_ascii=False))
        raise RuntimeError(f"FRED failed for {len(failed)} series-window requests")

    frame = (
        (
            pl.from_dicts(all_rows, infer_schema_length=None)
            if all_rows
            else _empty_initial_release_frame()
        )
        .unique(["series_id", "observation_date", "realtime_start"], keep="last")
        .sort(["series_id", "observation_date", "realtime_start"])
    )
    _atomic_parquet(output_path, frame)
    projection_manifest = (build_projection_manifest(output_dir, results, frame, identity=identity,
        end_date=end_date, retained_raw=retained_raw) if reuse_closed_projection else None)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "state": "complete",
        "source": "FRED series observations output_type=4",
        "series": list(series),
        "rows": frame.height,
        "start_date": args.start_date,
        "end_date": end_date,
        **date_boundary,
        "point_in_time_contract": (
            "Only initial-release values are retained; each is usable at the "
            "first UTC midnight after FRED realtime_start because no intraday "
            "release time is supplied. Later revisions are excluded."
        ),
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
        "window_count": len(results),
        "cached_windows": sum(item.status == "cached_verified" for item in results),
        "fetched_windows": sum(item.status != "cached_verified" for item in results),
        "closed_projection_reused_windows": projection.reused_windows if projection else 0,
        **({'closed_projection': projection_manifest} if projection_manifest is not None else {}),
        "rate_limit": {
            "requests_per_second": provider_rate_limit("fred_api").requests_per_second,
            "basis": provider_rate_limit("fred_api").basis,
        },
        "sources": source_receipts,
        "started_at_utc": started.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_json(output_dir / "download_summary.json", summary)
    progress.finish(require_exact=True)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
