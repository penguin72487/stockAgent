#!/usr/bin/env python3
"""Read-only A/B for a stable TWSE/TPEx institutional-date refresh.

The latest raw body must represent the same accepted rows already in the
canonical Parquet. If the official source changed, report ineligibility instead
of measuring a fictitious cache hit or writing into the live dataset.
"""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import resource
import sys
import time

import polars as pl


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_tw_public_data import (  # noqa: E402
    DEFAULT_DATASETS,
    _append_common_columns,
    _merge_frames,
    _parse_historical_response_content,
    _read_existing,
    _unchanged_historical_overlap,
)


DATASETS = ("twse_institutional_trades", "tpex_institutional_trades")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=DATASETS, required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--live-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    name = args.dataset
    day = args.date
    root = args.live_root
    path = root / f"{name}.parquet"
    raw_path = root / "raw" / name / f"{day.isoformat()}.json"
    if not path.is_file() or not raw_path.is_file():
        raise FileNotFoundError(f"canonical parquet or accepted raw is absent: {path}, {raw_path}")

    spec = DEFAULT_DATASETS[name]
    parsed, _ = _parse_historical_response_content(
        spec, day, raw_path.read_bytes(), "json"
    )
    observed_urls = (
        pl.scan_parquet(path)
        .filter(pl.col("date") == day.isoformat())
        .select(pl.col("_url").unique())
        .collect()
        .get_column("_url")
        .to_list()
    )
    if len(observed_urls) != 1 or not isinstance(observed_urls[0], str):
        raise ValueError("date does not have exactly one accepted source URL")
    incoming = _append_common_columns(
        parsed,
        spec,
        fetched_at="benchmark-only-download-clock",
        url=observed_urls[0],
    )
    before = path.stat()
    started = time.perf_counter()
    fast_noop = _unchanged_historical_overlap(path, incoming, refresh=False)
    fast_seconds = time.perf_counter() - started
    fast_peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result: dict[str, object] = {
        "dataset": name,
        "date": day.isoformat(),
        "archive_rows": None,
        "incoming_rows": incoming.height,
        "fast_path_eligible": fast_noop,
        "fast_seconds": round(fast_seconds, 6),
        "fast_process_peak_rss_kib": fast_peak_kib,
        "old_seconds": None,
        "old_process_peak_rss_kib": None,
        "old_noop": None,
        "source_path": str(path),
        "read_only": True,
    }
    if fast_noop:
        started = time.perf_counter()
        existing = _read_existing(path)
        merged = _merge_frames(existing, incoming, refresh=False)
        stable = [column for column in merged.columns if column != "_downloaded_at_utc"]
        old_noop = (
            existing.columns == merged.columns
            and existing.height == merged.height
            and existing.select(stable).equals(
                merged.select(stable), null_equal=True
            )
        )
        result.update({
            "archive_rows": existing.height,
            "old_seconds": round(time.perf_counter() - started, 6),
            "old_process_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "old_noop": old_noop,
        })
        if not old_noop:
            raise AssertionError("fast no-op disagrees with the original full merge")
    after = path.stat()
    if (before.st_ino, before.st_mtime_ns, before.st_size) != (
        after.st_ino, after.st_mtime_ns, after.st_size
    ):
        raise RuntimeError("canonical parquet changed during read-only measurement")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
