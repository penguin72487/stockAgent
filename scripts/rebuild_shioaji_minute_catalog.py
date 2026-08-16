from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime, timezone
import json
from pathlib import Path
import shutil
from typing import Any

import polars as pl

from downloader.download_shioaji_tw_minute_kbars import (
    DEFAULT_CHUNK_DAYS,
    DEFAULT_REQUESTS_PER_SECOND,
    SHIOAJI_QUOTE_LIMIT_REQUESTS,
    SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS,
    SHIOAJI_STOCK_HISTORY_START,
    SOURCE_NAME,
    STORAGE_FREQUENCY,
    SymbolResult,
    _atomic_write_json,
    _load_universe,
    completed_symbol_manifest_result,
    iter_date_chunks,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild a terminal Shioaji minute catalog from sealed manifests and "
            "a receipt-backed daily materialization report without broker access."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--minute-root", type=Path, default=Path("data_tw_minute/shioaji_1m")
    )
    parser.add_argument(
        "--daily-report",
        type=Path,
        default=Path("data_tw_public/shioaji/download_report.csv"),
    )
    parser.add_argument("--start-date", default=SHIOAJI_STOCK_HISTORY_START.isoformat())
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--simulation", action="store_true", default=True)
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(str(args.start_date))
    end = date.fromisoformat(str(args.end_date))
    chunks = list(iter_date_chunks(start, end, DEFAULT_CHUNK_DAYS))
    daily = pl.read_csv(args.daily_report, infer_schema_length=0)
    daily_map = {
        str(row["symbol"]).strip().upper(): row
        for row in daily.iter_rows(named=True)
    }
    universe = _load_universe(args.base_stock_root)
    selected = [
        row
        for row in universe
        if pl.read_parquet(row.base_path, columns=["date"])["date"].min() <= end
    ]
    results: list[SymbolResult] = []
    failures: list[str] = []
    for row in selected:
        sealed = completed_symbol_manifest_result(
            args.minute_root,
            row,
            chunks,
            requested_start=start,
            requested_end=end,
            simulation=bool(args.simulation),
        )
        if sealed is not None:
            results.append(sealed)
            continue
        daily_row = daily_map.get(row.symbol)
        if daily_row is not None and daily_row.get("status") == "contract_unavailable":
            results.append(
                SymbolResult(
                    symbol=row.symbol,
                    status="contract_unavailable",
                    chunks_total=len(chunks),
                    chunks_complete=0,
                    source_minute_rows=0,
                    daily_rows=0,
                    first_date=None,
                    last_date=None,
                    output_path="",
                    message=str(daily_row.get("message") or "contract_unavailable"),
                )
            )
            continue
        failures.append(row.symbol)
    if failures or len(results) != len(selected):
        raise RuntimeError(
            f"cannot reconstruct terminal minute catalog; missing={failures[:20]} "
            f"results={len(results)} selected={len(selected)}"
        )

    canonical_summary = args.minute_root / "download_summary.json"
    canonical_report = args.minute_root / "download_report.csv"
    prior = _read_json(canonical_summary)
    if prior is not None and prior.get("resumable_collection_complete") is not True:
        shutil.copy2(canonical_summary, args.minute_root / "latest_run_summary.json")
        if canonical_report.is_file():
            shutil.copy2(canonical_report, args.minute_root / "latest_run_report.csv")

    pl.DataFrame([asdict(item) for item in results]).sort("symbol").write_csv(
        canonical_report
    )
    collection_complete = all(
        item.status
        in {"complete", "complete_with_source_gaps", "contract_unavailable"}
        for item in results
    )
    _atomic_write_json(
        canonical_summary,
        {
            "schema_version": 1,
            "source": SOURCE_NAME,
            "storage_frequency": STORAGE_FREQUENCY,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "chunk_days": DEFAULT_CHUNK_DAYS,
            "simulation": bool(args.simulation),
            "workers": 0,
            "requests_per_second_limit": DEFAULT_REQUESTS_PER_SECOND,
            "quote_limit_requests": SHIOAJI_QUOTE_LIMIT_REQUESTS,
            "quote_limit_window_seconds": SHIOAJI_QUOTE_LIMIT_WINDOW_SECONDS,
            "api_requests_started_this_run": 0,
            "observed_request_start_rps": 0.0,
            "processed_chunks_this_run": 0,
            "queried_chunks_this_run": 0,
            "skipped_empty_chunks_this_run": 0,
            "selected_symbols": len(selected),
            "reported_symbols": len(results),
            "complete_symbols": sum(x.status == "complete" for x in results),
            "complete_with_source_gap_symbols": sum(
                x.status == "complete_with_source_gaps" for x in results
            ),
            "contract_unavailable_symbols": sum(
                x.status == "contract_unavailable" for x in results
            ),
            "failed_symbols": 0,
            "partial_symbols": 0,
            "selected_coverage_complete": all(
                item.status == "complete" for item in results
            ),
            "resumable_collection_complete": collection_complete,
            "published_terminal_catalog": collection_complete,
            "stopped_for_traffic": False,
            "stopped_for_market_hours": False,
            "fatal_error": None,
            "traffic_used_bytes": None,
            "traffic_limit_bytes": None,
            "report_path": str(canonical_report),
            "recovered_from_sealed_manifests": True,
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    print(
        f"[shioaji-minute-catalog] terminal=true symbols={len(results)} "
        f"complete={sum(x.status == 'complete' for x in results)} "
        f"gaps={sum(x.status == 'complete_with_source_gaps' for x in results)} "
        f"unavailable={sum(x.status == 'contract_unavailable' for x in results)} "
        f"end_date={end} api_requests_started=0",
        flush=True,
    )


if __name__ == "__main__":
    main()
