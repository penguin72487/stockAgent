#!/usr/bin/env python3
"""Audit a panel's dated TX coverage against its pinned daily reference.

This checks the source reference and panel in both directions over an explicit
horizon. It does not certify that TX is an exhaustive calendar for every other
futures product, that prices are executable, or that any training is admitted.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import json

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.panel_cache import array_content_fingerprint
from stockagent.data.tw_futures_benchmark import load_tx_front_rolling_benchmark


def audit(panel_meta: Path, reference: Path, start: str, end: str) -> dict:
    panel_meta = panel_meta.resolve(strict=True)
    reference = reference.resolve(strict=True)
    reference_manifest = reference.with_name("manifest.json")
    module = Path(__file__).resolve(strict=True)
    import stockagent.data.tw_futures_benchmark as benchmark_module

    implementation = Path(benchmark_module.__file__).resolve(strict=True)
    meta = json.loads(panel_meta.read_bytes())
    relative = Path(meta["arrays"]["dates"]["file"])
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("panel date array escapes its cache generation")
    dates_path = (panel_meta.parent / relative).resolve(strict=True)
    if not dates_path.is_relative_to(panel_meta.parent):
        raise ValueError("panel date array escapes its cache generation")
    paths = (panel_meta, dates_path, reference, reference_manifest, module, implementation)
    before = {str(path): sha256_file(path) for path in paths}
    manifest = json.loads(reference_manifest.read_bytes())
    if (manifest.get("status") != "complete"
            or manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256")
            != before[str(reference)]):
        raise ValueError("TX reference does not match its complete manifest")
    original = np.load(dates_path, allow_pickle=False)
    if array_content_fingerprint(original) != meta["arrays"]["dates"]["content_fingerprint"]:
        raise ValueError("panel date array does not match its cache fingerprint")
    dates = np.asarray(original, dtype="datetime64[D]")
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("panel dates must be finite and strictly increasing")
    first, last = np.datetime64(start, "D"), np.datetime64(end, "D")
    if np.isnat(first) or np.isnat(last) or first > last or str(first) != start or str(last) != end:
        raise ValueError("audit requires an ordered ISO date horizon")
    selected = dates[(dates >= first) & (dates <= last)]
    front = (pl.scan_parquet(reference)
             .filter((pl.col("product") == "TX") & (pl.col("tenor_rank") == 1)
                     & pl.col("date").cast(pl.String).is_between(pl.lit(start), pl.lit(end)))
             .select("date", "source_row_observed").sort("date").collect())
    if (not selected.size or front.is_empty() or front["date"].is_duplicated().any()
            or front["source_row_observed"].null_count() or not front["source_row_observed"].all()):
        raise ValueError("TX reference needs one observed front row on each source date")
    source_dates = front["date"].to_numpy().astype("datetime64[D]")
    absent_panel = np.setdiff1d(source_dates, selected)
    absent_source = np.setdiff1d(selected, source_dates)
    if absent_panel.size or absent_source.size:
        raise ValueError(f"market dates differ: panel_missing={absent_panel.tolist()} "
                         f"tx_source_missing={absent_source.tolist()}")
    payload = load_tx_front_rolling_benchmark(reference, selected)
    if not payload["source_covered"].all():
        raise ValueError("the matched panel includes TX source-inception placeholders")
    if len(selected) < 3:
        raise ValueError("audit requires an interior session for omission detection")
    removed = selected[len(selected) // 2]
    try:
        load_tx_front_rolling_benchmark(reference, np.delete(selected, len(selected) // 2))
    except ValueError as error:
        if f"panel is missing a TX source session: {removed}" not in str(error):
            raise
    else:
        raise ValueError("benchmark failed to reject an omitted TX session")
    if any(sha256_file(Path(path)) != digest for path, digest in before.items()):
        raise ValueError("calendar audit input changed during readback")
    # NumPy epoch 1970-01-01 was Thursday; Monday is weekday zero.
    weekday = (selected.astype("int64") + 3) % 7
    return {
        "contract": "tw_futures_panel_tx_source_calendar_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "requested_start": start, "requested_end": end,
        "first_session": str(selected[0]), "last_session": str(selected[-1]),
        "panel_sessions": len(selected), "tx_front_observed_sessions": len(source_dates),
        "panel_only_sessions": 0, "tx_only_sessions": 0,
        "weekend_sessions": [str(value) for value in selected[weekday >= 5]],
        "omission_detection_checked": str(removed),
        "inputs_sha256": before,
        "scope": "matched panel and TX front reference within the requested horizon",
        "all_futures_product_calendars_certified": False,
        "historical_point_in_time_verified": False,
        "execution_or_training_admitted": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-meta", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError("preserve the earlier calendar audit")
    result = audit(args.panel_meta, args.reference, args.start, args.end)
    atomic_write_json(args.output, result)
    print(json.dumps({"output": str(args.output), "panel_sessions": result["panel_sessions"],
                      "weekend_sessions": len(result["weekend_sessions"])}))


if __name__ == "__main__":
    main()
