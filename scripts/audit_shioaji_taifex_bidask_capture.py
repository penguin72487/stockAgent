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
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    manifests = read_capture_manifests(args.capture_root, args.trade_date.isoformat())
    if len(manifests) != 1:
        raise RuntimeError(f"expected one FOP worker manifest, got {len(manifests)}")
    manifest = manifests[0]
    if manifest.get("source") != "shioaji_taifex_tick_bidask_v1":
        raise RuntimeError("unexpected capture source")
    if manifest.get("status") != "complete":
        raise RuntimeError(f"capture status is not complete: {manifest.get('status')}")
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
    metadata = manifest.get("contract_metadata", [])
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
        "capture_id": manifest["capture_id"],
        "simulation_account_environment": bool(manifest.get("simulation")),
        "contracts": len(expected_codes),
        "book_rows": books.height,
        "valid_book_rows": valid.height,
        "valid_book_fraction": valid.height / books.height,
        "transport_delay_ms_p50": float(transport_ms.quantile(0.50, "nearest")),
        "transport_delay_ms_p99": float(transport_ms.quantile(0.99, "nearest")),
        "receive_ts_min_ns": int(valid["receive_ts_ns"].min()),
        "receive_ts_max_ns": int(valid["receive_ts_ns"].max()),
        "dropped_events": int(manifest["dropped_events"]),
        "queue_high_watermark": int(manifest["queue_high_watermark"]),
    }
    output = args.output or (
        args.capture_root / "audits" / f"{args.trade_date.isoformat()}.json"
    )
    _atomic_json(output, summary)
    print(
        f"[shioaji-taifex-audit] status=ok date={args.trade_date} "
        f"books={books.height} valid={valid.height} contracts={len(expected_codes)} "
        f"output={output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
