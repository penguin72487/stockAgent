#!/usr/bin/env python3
"""Archive the official TAIFEX VIX daily files exposed by the recent page."""

from __future__ import annotations

import argparse
from datetime import date
import gzip
import json
from pathlib import Path
import re
import sys
from typing import Final

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.common import SharedRateLimiter  # noqa: E402
from scripts.download_taifex_public_history import (  # noqa: E402
    _atomic_write_bytes,
    _atomic_write_parquet,
    _load_sessions,
    _next_session_map,
    _relative,
    _sha256_bytes,
    _utc_now,
    _write_json,
)
from scripts.taifex_daily_download_common import sha256_path  # noqa: E402


CONTRACT_VERSION: Final[int] = 1
SOURCE_PAGE: Final[str] = "https://www.taifex.com.tw/cht/7/vixDaily3MNew"
VIX_URL_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"https://www\.taifex\.com\.tw/file/taifex/Dailydownload/vix/"
    r"log2data/(?P<month>\d{6})new\.txt"
)


def _parse_vix(content: bytes, next_sessions: dict[date, date]) -> pd.DataFrame:
    lines = content.decode("cp950", errors="strict").splitlines()
    rows: list[dict[str, object]] = []
    for line in lines:
        cells = line.split()
        if len(cells) != 4 or not re.fullmatch(r"\d{8}", cells[0]):
            continue
        observed = date.fromisoformat(
            f"{cells[0][0:4]}-{cells[0][4:6]}-{cells[0][6:8]}"
        )
        rows.append(
            {
                "date": pd.Timestamp(observed),
                "time_hhmmssff": cells[1],
                "taifex_vix": float(cells[2]),
                "preclose_1m_average_vix": float(cells[3]),
                "available_date": pd.Timestamp(next_sessions.get(observed)),
                "published_after_close": True,
                "availability_rule": "next_receipt_verified_taifex_session",
            }
        )
    if not rows:
        raise ValueError("TAIFEX VIX file contained no daily rows")
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _valid_receipt(root: Path, capture_date: date, month: str) -> dict[str, object] | None:
    path = root / "receipts" / "vix_daily" / capture_date.isoformat() / f"{month}.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("contract_version") != CONTRACT_VERSION
            or payload.get("capture_date") != capture_date.isoformat()
            or payload.get("month") != month
            or payload.get("status") != "complete"
        ):
            return None
        for prefix in ("raw", "normalized"):
            artifact = root / str(payload[f"{prefix}_path"])
            if (
                not artifact.is_file()
                or artifact.stat().st_size != int(payload[f"{prefix}_bytes"])
                or sha256_path(artifact) != payload[f"{prefix}_sha256"]
            ):
                return None
        return payload
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data_taifex_public_history")
    parser.add_argument(
        "--session-parquet",
        default="data_tw_index_futures/day_session_contracts.parquet",
    )
    parser.add_argument("--capture-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--request-interval", type=float, default=1.0)
    args = parser.parse_args()
    root = Path(args.output_dir).expanduser().resolve()
    session_path = Path(args.session_parquet).expanduser().resolve()
    sessions, calendar_sha256 = _load_sessions(session_path, date.max)
    next_sessions = _next_session_map(sessions)
    limiter = SharedRateLimiter(args.request_interval, name="taifex_vix_recent")
    session = requests.Session()
    session.headers.update({"User-Agent": "stockAgent/taifex-vix-recent-archive"})
    limiter.wait()
    page = session.get(SOURCE_PAGE, timeout=120)
    page.raise_for_status()
    urls = list(dict.fromkeys(match.group(0) for match in VIX_URL_PATTERN.finditer(page.text)))
    if not urls:
        raise RuntimeError("TAIFEX VIX page exposed no official monthly files")

    receipts: list[dict[str, object]] = []
    for url in urls:
        month_match = VIX_URL_PATTERN.fullmatch(url)
        if month_match is None:
            raise AssertionError(url)
        month = month_match.group("month")
        receipt = _valid_receipt(root, args.capture_date, month)
        if receipt is None:
            limiter.wait()
            response = session.get(url, timeout=120)
            response.raise_for_status()
            content = response.content
            frame = _parse_vix(content, next_sessions)
            digest = _sha256_bytes(content)
            raw_path = root / "raw" / "vix_daily" / month / f"{digest}.txt.gz"
            shard_path = root / "shards" / "vix_daily" / month / f"{digest}.parquet"
            _atomic_write_bytes(raw_path, gzip.compress(content, compresslevel=9, mtime=0))
            if not shard_path.is_file():
                _atomic_write_parquet(frame, shard_path)
            receipt = {
                "contract_version": CONTRACT_VERSION,
                "dataset": "taifex_vix_daily_recent",
                "capture_date": args.capture_date.isoformat(),
                "month": month,
                "status": "complete",
                "source_page": SOURCE_PAGE,
                "source_url": url,
                "captured_at_utc": _utc_now(),
                "rows": len(frame),
                "response_sha256": digest,
                "raw_path": _relative(raw_path, root),
                "raw_bytes": raw_path.stat().st_size,
                "raw_sha256": sha256_path(raw_path),
                "normalized_path": _relative(shard_path, root),
                "normalized_bytes": shard_path.stat().st_size,
                "normalized_sha256": sha256_path(shard_path),
                "published_after_close": True,
                "availability_rule": "next_receipt_verified_taifex_session",
            }
            receipt_path = (
                root
                / "receipts"
                / "vix_daily"
                / args.capture_date.isoformat()
                / f"{month}.json"
            )
            _write_json(receipt_path, receipt)
        receipts.append(receipt)
        print(f"[vix] {month} rows={receipt['rows']}", flush=True)

    unique_shards = sorted(
        {root / str(receipt["normalized_path"]) for receipt in receipts}
    )
    # Include every prior content-addressed shard so the merged file only grows
    # when the official rolling window exposes a new month or daily revision.
    unique_shards = sorted(set(unique_shards) | set((root / "shards" / "vix_daily").glob("*/*.parquet")))
    table = pa.concat_tables(
        [pq.read_table(path) for path in unique_shards], promote_options="default"
    )
    frame = (
        table.to_pandas()
        .sort_values("date", kind="stable")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    output = root / "normalized" / "taifex_vix_daily_recent.parquet"
    _atomic_write_parquet(frame, output)
    manifest = {
        "contract_version": CONTRACT_VERSION,
        "dataset": "taifex_vix_daily_recent",
        "status": "complete",
        "capture_date": args.capture_date.isoformat(),
        "source_page": SOURCE_PAGE,
        "official_public_boundary": "files_currently_listed_on_rolling_recent_page",
        "older_history": "not_exposed_by_this_free_page",
        "rows": len(frame),
        "first_date": frame["date"].min().date().isoformat(),
        "last_date": frame["date"].max().date().isoformat(),
        "session_calendar_sha256": calendar_sha256,
        "output_path": _relative(output, root),
        "output_bytes": output.stat().st_size,
        "output_sha256": sha256_path(output),
        "completed_at_utc": _utc_now(),
    }
    _write_json(root / "vix_latest.json", manifest)
    print(json.dumps(manifest, ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
