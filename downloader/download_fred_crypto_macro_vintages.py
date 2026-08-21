"""Download FRED initial-release observations for causal crypto macro context."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import provider_rate_limit, retry_delay_seconds  # noqa: E402
from openbb_credentials import load_openbb_environment  # noqa: E402


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
    return parser.parse_args()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Any) -> None:
    _atomic_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(frame.to_arrow(), temporary, compression="snappy")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


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
    profile = provider_rate_limit("fred_api")
    for attempt in range(max(1, max_retries + 1)):
        if attempt:
            time.sleep(retry_delay_seconds(attempt, 1.0, maximum=30.0))
        try:
            request = Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "stockAgent-research/1.0",
                },
            )
            with urlopen(request, timeout=60) as response:
                payload = response.read()
            time.sleep(profile.interval_seconds)
            return payload
        except HTTPError as exc:
            if exc.code == 400:
                body = exc.read().decode("utf-8", errors="replace")
                if (
                    "does not exist in ALFRED" in body
                    or "No vintage dates exist" in body
                ):
                    return json.dumps(
                        {
                            "observations": [],
                            "point_in_time_gap": body,
                            "realtime_start": realtime_start,
                            "realtime_end": realtime_end,
                        }
                    ).encode("utf-8")
            if exc.code not in {429, 500, 502, 503, 504} or attempt >= max_retries:
                raise
        except URLError:
            if attempt >= max_retries:
                raise
    raise RuntimeError(f"FRED request exhausted retries for {series_id}")


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


def parse_initial_release_rows(
    series_id: str, payload: bytes, *, retrieved_at_utc: datetime
) -> list[dict[str, object]]:
    document = json.loads(payload)
    observations = document.get("observations") or []
    rows: list[dict[str, object]] = []
    raw_sha256 = hashlib.sha256(payload).hexdigest()
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


def main() -> None:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_dir = Path(args.output_dir)
    api_key = _configured_api_key(Path(args.env_file))
    end_date = (
        date.today().isoformat() if args.end_date in {"today", "now"} else args.end_date
    )
    series = tuple(
        dict.fromkeys(
            str(value).strip().upper() for value in args.series if str(value).strip()
        )
    )
    if not series:
        raise ValueError("at least one FRED series is required")
    all_rows: list[dict[str, object]] = []
    source_receipts: list[dict[str, object]] = []
    for series_id in series:
        rows: list[dict[str, object]] = []
        raw_paths: list[str] = []
        raw_hashes: list[str] = []
        point_in_time_gap_windows: list[dict[str, str]] = []
        for realtime_start, realtime_end in _realtime_windows(
            args.start_date, end_date
        ):
            retrieved = datetime.now(timezone.utc)
            payload = _fetch_series(
                series_id,
                api_key=api_key,
                start_date=args.start_date,
                end_date=end_date,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
                max_retries=args.max_retries,
            )
            raw_sha256 = hashlib.sha256(payload).hexdigest()
            raw_path = (
                output_dir
                / "raw"
                / retrieved.strftime("%Y%m%d")
                / (
                    f"{series_id}-{realtime_start}-{realtime_end}-"
                    f"{retrieved.strftime('%Y%m%dT%H%M%S.%fZ')}-"
                    f"{raw_sha256[:16]}.json.gz"
                )
            )
            _atomic_bytes(raw_path, gzip.compress(payload, mtime=0))
            raw_paths.append(str(raw_path))
            raw_hashes.append(raw_sha256)
            document = json.loads(payload)
            if gap := str(document.get("point_in_time_gap") or "").strip():
                point_in_time_gap_windows.append(
                    {
                        "realtime_start": realtime_start,
                        "realtime_end": realtime_end,
                        "message": gap,
                    }
                )
            rows.extend(
                parse_initial_release_rows(
                    series_id, payload, retrieved_at_utc=retrieved
                )
            )
        all_rows.extend(rows)
        source_receipts.append(
            {
                "series_id": series_id,
                "status": (
                    "complete_with_point_in_time_gap_windows"
                    if point_in_time_gap_windows
                    else "complete"
                ),
                "rows": len(rows),
                "raw_paths": raw_paths,
                "raw_sha256": raw_hashes,
                "point_in_time_gap_windows": point_in_time_gap_windows,
            }
        )
    frame = (
        pl.from_dicts(all_rows, infer_schema_length=None)
        .unique(["series_id", "observation_date", "realtime_start"], keep="last")
        .sort(["series_id", "observation_date", "realtime_start"])
    )
    output_path = output_dir / "observations.parquet"
    _atomic_parquet(output_path, frame)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "state": "complete",
        "source": "FRED series observations output_type=4",
        "series": list(series),
        "rows": frame.height,
        "start_date": args.start_date,
        "end_date": end_date,
        "point_in_time_contract": (
            "Only initial-release values are retained; each is usable at the "
            "first UTC midnight after FRED realtime_start because no intraday "
            "release time is supplied. Later revisions are excluded."
        ),
        "output_path": str(output_path),
        "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "rate_limit": {
            "requests_per_second": provider_rate_limit("fred_api").requests_per_second,
            "basis": provider_rate_limit("fred_api").basis,
        },
        "sources": source_receipts,
        "started_at_utc": started.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _atomic_json(output_dir / "download_summary.json", summary)
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
