#!/usr/bin/env python3
"""Audit one receipt-backed Shioaji TAIFEX Tick/BidAsk capture."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.shioaji_capture_parts import (  # noqa: E402
    read_capture_manifests,
    select_capture_part_paths,
)
from downloader.stream_shioaji_tw_microstructure import _atomic_json  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--session", choices=("day", "night"), default=None)
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    manifests = read_capture_manifests(
        args.capture_root,
        args.trade_date.isoformat(),
        session=args.session,
    )
    if not manifests:
        raise RuntimeError("no FOP worker manifests found")
    expected_workers = {int(item.get("workers", 1)) for item in manifests}
    if len(expected_workers) != 1 or len(manifests) != next(iter(expected_workers)):
        raise RuntimeError(
            f"incomplete FOP worker manifests: got={len(manifests)} "
            f"declared={sorted(expected_workers)}"
        )
    capture_ids = {str(item.get("capture_id", "")) for item in manifests}
    if len(capture_ids) != 1 or "" in capture_ids:
        raise RuntimeError(f"FOP workers do not share one capture: {capture_ids}")
    for manifest in manifests:
        if manifest.get("source") != "shioaji_taifex_tick_bidask_v1":
            raise RuntimeError("unexpected capture source")
        if args.session is not None and manifest.get("capture_session") not in {
            None,
            args.session,
        }:
            raise RuntimeError("capture session does not match requested audit")
        if manifest.get("status") != "complete":
            raise RuntimeError(
                f"capture status is not complete: {manifest.get('status')}"
            )
        if int(manifest.get("dropped_events", -1)) != 0:
            raise RuntimeError("capture lost callback events")
    paths = select_capture_part_paths(
        capture_root=args.capture_root,
        kind="book_events",
        trade_date=args.trade_date.isoformat(),
        manifests=manifests,
    )
    books = pl.scan_parquet(paths).collect()
    valid = books.filter(
        (~pl.col("simtrade").fill_null(False))
        & (pl.col("bid_price_1") > 0.0)
        & (pl.col("ask_price_1") >= pl.col("bid_price_1"))
        & (pl.col("bid_volume_1") > 0)
        & (pl.col("ask_volume_1") > 0)
    )
    metadata = [
        row
        for manifest in manifests
        for row in manifest.get("contract_metadata", [])
    ]
    expected_codes = sorted(
        {
            str(row["code"])
            for row in metadata
            if isinstance(row, dict) and row.get("code")
        }
    )
    observed_codes = sorted(str(value) for value in books["code"].unique())
    missing_codes = sorted(set(expected_codes).difference(observed_codes))
    if missing_codes:
        raise RuntimeError(f"captured books miss subscribed contracts: {missing_codes}")
    if valid.is_empty():
        raise RuntimeError("capture has no valid non-crossed best BidAsk")
    transport_ms = (valid["receive_ts_ns"] - valid["exchange_ts_ns"]).cast(
        pl.Float64
    ) / 1e6
    summary = {
        "schema_version": 1,
        "status": "ok",
        "trade_date": args.trade_date.isoformat(),
        "session": args.session,
        "capture_id": next(iter(capture_ids)),
        "simulation_account_environment": all(
            bool(manifest.get("simulation")) for manifest in manifests
        ),
        "workers": len(manifests),
        "contracts": len(expected_codes),
        "book_rows": books.height,
        "valid_book_rows": valid.height,
        "valid_book_fraction": valid.height / books.height,
        "transport_delay_ms_p50": float(transport_ms.quantile(0.50, "nearest")),
        "transport_delay_ms_p99": float(transport_ms.quantile(0.99, "nearest")),
        "receive_ts_min_ns": int(valid["receive_ts_ns"].min()),
        "receive_ts_max_ns": int(valid["receive_ts_ns"].max()),
        "dropped_events": sum(int(item["dropped_events"]) for item in manifests),
        "queue_high_watermark": max(
            int(item["queue_high_watermark"]) for item in manifests
        ),
    }
    output_suffix = (
        f"-{args.session}" if args.session is not None else ""
    )
    output = args.output or (
        args.capture_root
        / "audits"
        / f"{args.trade_date.isoformat()}{output_suffix}.json"
    )
    _atomic_json(output, summary)
    print(
        f"[shioaji-taifex-audit] status=ok date={args.trade_date} "
        f"session={args.session or 'legacy'} "
        f"books={books.height} valid={valid.height} contracts={len(expected_codes)} "
        f"output={output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
