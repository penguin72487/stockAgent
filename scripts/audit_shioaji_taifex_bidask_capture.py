#!/usr/bin/env python3
"""Audit one receipt-backed Shioaji TAIFEX Tick/BidAsk capture."""

from __future__ import annotations

import argparse
from datetime import date, datetime
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
from downloader.stream_shioaji_taifex_bidask import (  # noqa: E402
    BIDASK_STALE_TIMEOUT_SECONDS,
    EXPIRY_HEDGE_CLOSE,
)
from stockagent.data.taifex_sessions import TAIPEI  # noqa: E402


def strategy_hedge_tail_gap_seconds(
    manifest: dict[str, object], books: pl.DataFrame
) -> float | None:
    """Prove the strategy worker received hedge books through capture close."""

    simulation = manifest.get("strategy_simulation")
    if not isinstance(simulation, dict) or not simulation.get("enabled"):
        return None
    if int(manifest.get("worker_index", -1)) != 0:
        raise RuntimeError("strategy simulation must run on worker 0")
    selection = manifest.get("selection")
    if not isinstance(selection, dict):
        raise RuntimeError("strategy worker has no contract selection")
    hedge_code = str(selection.get("resolved_hedge_future_code") or "")
    if not hedge_code:
        raise RuntimeError("strategy worker has no resolved hedge future")
    metadata = manifest.get("contract_metadata")
    if not isinstance(metadata, list):
        raise RuntimeError("strategy worker has no contract metadata")
    hedge_metadata = [
        row
        for row in metadata
        if isinstance(row, dict) and row.get("code") == hedge_code
    ]
    if len(hedge_metadata) != 1:
        raise RuntimeError(f"strategy hedge metadata is not unique: {hedge_code}")
    last_trading_date = date.fromisoformat(
        str(hedge_metadata[0]["last_trading_date"])
    )
    hedge_books = books.filter(
        (pl.col("worker_index") == 0) & (pl.col("code") == hedge_code)
    )
    if hedge_books.is_empty():
        raise RuntimeError(f"strategy hedge has no BidAsk: {hedge_code}")
    finished_at = datetime.fromisoformat(str(manifest["finished_at_utc"]))
    if finished_at.tzinfo is None:
        raise RuntimeError("capture finish time must be timezone-aware")
    local_finished_at = finished_at.astimezone(TAIPEI)
    if (
        manifest.get("capture_session") == "day"
        and local_finished_at.date() == last_trading_date
    ):
        finished_at = min(
            finished_at,
            datetime.combine(
                last_trading_date, EXPIRY_HEDGE_CLOSE, tzinfo=TAIPEI
            ),
        )
    last_receive_ns = int(hedge_books["receive_ts_ns"].max())
    return max(0.0, finished_at.timestamp() - last_receive_ns / 1e9)


def capture_book_metrics(manifests: list[dict], books: pl.LazyFrame) -> dict:
    """Reduce projected columns, never materialize the full multi-level books.

    Exact nearest quantiles still need their complete one-column population;
    streaming does not make that state free. Contract/worker maxima and counts
    are small, and also preserve the original missing-code/hedge-tail checks.
    """
    needs_worker = any(
        isinstance(manifest.get("strategy_simulation"), dict)
        and manifest["strategy_simulation"].get("enabled")
        for manifest in manifests
    )
    # Legacy data-only frames need no worker axis, just as in the eager audit.
    group_keys = ["worker_index", "code"] if needs_worker else ["code"]
    tails = books.group_by(group_keys).agg(
        pl.col("receive_ts_ns").max(), pl.len().cast(pl.UInt64).alias("book_rows")
    ).collect(engine="streaming")
    expected_codes = sorted({
        str(row["code"])
        for manifest in manifests
        for row in manifest.get("contract_metadata", [])
        if isinstance(row, dict) and row.get("code")
    })
    observed_codes = {str(value) for value in tails["code"].unique()}
    missing_codes = sorted(set(expected_codes).difference(observed_codes))
    if missing_codes:
        raise RuntimeError(f"captured books miss subscribed contracts: {missing_codes}")

    valid = books.filter(
        (~pl.col("simtrade").fill_null(False))
        & (pl.col("bid_price_1") > 0.0)
        & (pl.col("ask_price_1") >= pl.col("bid_price_1"))
        & (pl.col("bid_volume_1") > 0)
        & (pl.col("ask_volume_1") > 0)
    )
    transport_ms = (pl.col("receive_ts_ns") - pl.col("exchange_ts_ns")).cast(pl.Float64) / 1e6
    stats = valid.select(
        pl.len().alias("valid_book_rows"),
        transport_ms.quantile(0.50, "nearest").alias("transport_delay_ms_p50"),
        transport_ms.quantile(0.99, "nearest").alias("transport_delay_ms_p99"),
        pl.col("receive_ts_ns").min().alias("receive_ts_min_ns"),
        pl.col("receive_ts_ns").max().alias("receive_ts_max_ns"),
    ).collect(engine="streaming").row(0, named=True)
    valid_rows = int(stats["valid_book_rows"])
    if not valid_rows:
        raise RuntimeError("capture has no valid non-crossed best BidAsk")
    strategy_hedge_tail_gaps = [
        gap for manifest in manifests
        if (gap := strategy_hedge_tail_gap_seconds(manifest, tails)) is not None
    ]
    if any(gap > BIDASK_STALE_TIMEOUT_SECONDS for gap in strategy_hedge_tail_gaps):
        raise RuntimeError(
            "strategy hedge BidAsk stream ended before capture close: "
            f"tail_gaps_seconds={strategy_hedge_tail_gaps}"
        )
    total_rows = sum(tails["book_rows"])
    return {
        "contracts": len(expected_codes), "book_rows": total_rows,
        "valid_book_rows": valid_rows, "valid_book_fraction": valid_rows / total_rows,
        "transport_delay_ms_p50": float(stats["transport_delay_ms_p50"]),
        "transport_delay_ms_p99": float(stats["transport_delay_ms_p99"]),
        "receive_ts_min_ns": int(stats["receive_ts_min_ns"]),
        "receive_ts_max_ns": int(stats["receive_ts_max_ns"]),
        "strategy_hedge_tail_gap_seconds": (
            max(strategy_hedge_tail_gaps) if strategy_hedge_tail_gaps else None
        ),
    }


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
    metrics = capture_book_metrics(manifests, pl.scan_parquet(paths))
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
        **metrics,
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
        f"books={metrics['book_rows']} valid={metrics['valid_book_rows']} contracts={metrics['contracts']} "
        f"output={output}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
