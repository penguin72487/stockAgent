from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.shioaji_capture_parts import (
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fail-closed reconciliation audit for Shioaji microstructure captures."
    )
    parser.add_argument(
        "--universe",
        type=Path,
        default=Path("data_tw_microstructure/universe/top_200.csv"),
    )
    parser.add_argument(
        "--capture-root", type=Path, default=Path("data_tw_microstructure/captures")
    )
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument(
        "--output", type=Path, default=Path("data_tw_microstructure/audits/latest.json")
    )
    return parser.parse_args()


def _scan(paths: list[Path], *, kind: str) -> pl.LazyFrame:
    if not paths:
        raise RuntimeError(f"no {kind} parquet files found")
    return pl.scan_parquet([str(path) for path in paths], missing_columns="raise")


def audit(
    *, universe_path: Path, capture_root: Path, trade_date: date
) -> dict[str, Any]:
    universe = pl.read_csv(
        universe_path,
        schema_overrides={"symbol": pl.String},
        infer_schema_length=0,
    )
    expected_symbols = set(universe["symbol"])
    universe_sha256 = hashlib.sha256(universe_path.read_bytes()).hexdigest()
    manifests = read_capture_manifests(capture_root, trade_date.isoformat())
    if len(manifests) != 2:
        raise RuntimeError(f"expected 2 worker manifests, found {len(manifests)}")
    capture_id = shared_capture_id(manifests)
    captured_symbols: list[str] = []
    universe_checksum_verified = True
    for manifest in manifests:
        schema_version = int(manifest.get("schema_version", -1))
        checksum_ok = (
            manifest.get("universe_sha256") == universe_sha256
            if schema_version >= 2
            else True
        )
        universe_checksum_verified &= schema_version >= 2 and checksum_ok
        if not (
            manifest.get("source") == "shioaji_streaming_v1"
            and manifest.get("status") == "complete"
            and int(manifest.get("workers", -1)) == 2
            and checksum_ok
            and int(manifest.get("dropped_events", -1)) == 0
            and int(manifest.get("subscriptions_requested", -1))
            == 2 * int(manifest.get("symbol_count", -1))
        ):
            raise RuntimeError(f"invalid capture manifest: {manifest}")
        captured_symbols.extend(str(value) for value in manifest.get("symbols", []))
    if len(captured_symbols) != len(set(captured_symbols)):
        raise RuntimeError("worker manifests contain duplicate symbols")
    if set(captured_symbols) != expected_symbols:
        raise RuntimeError(
            f"captured universe mismatch missing={sorted(expected_symbols-set(captured_symbols))[:20]} "
            f"extra={sorted(set(captured_symbols)-expected_symbols)[:20]}"
        )

    tick_paths = select_capture_part_paths(
        capture_root=capture_root,
        kind="ticks",
        trade_date=trade_date.isoformat(),
        manifests=manifests,
    )
    book_paths = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_events",
        trade_date=trade_date.isoformat(),
        manifests=manifests,
    )
    snapshot_paths = select_capture_part_paths(
        capture_root=capture_root,
        kind="book_1s",
        trade_date=trade_date.isoformat(),
        manifests=manifests,
    )
    ticks = _scan(tick_paths, kind="tick")
    books = _scan(book_paths, kind="book event")
    snapshots = _scan(snapshot_paths, kind="one-second book")
    tick_counts = {
        int(row["worker_index"]): int(row["len"])
        for row in ticks.group_by("worker_index").len().collect().iter_rows(named=True)
    }
    book_counts = {
        int(row["worker_index"]): int(row["len"])
        for row in books.group_by("worker_index").len().collect().iter_rows(named=True)
    }
    snapshot_counts = {
        int(row["worker_index"]): int(row["len"])
        for row in snapshots.group_by("worker_index").len().collect().iter_rows(named=True)
    }
    for manifest in manifests:
        worker = int(manifest["worker_index"])
        expected = {
            "tick_rows_written": tick_counts.get(worker, 0),
            "book_rows_written": book_counts.get(worker, 0),
            "book_1s_rows_written": snapshot_counts.get(worker, 0),
        }
        for key, actual in expected.items():
            if int(manifest.get(key, -1)) != actual:
                raise RuntimeError(
                    f"worker {worker} {key} mismatch manifest={manifest.get(key)} "
                    f"actual={actual}"
                )
    duplicate_snapshots = (
        snapshots.group_by("snapshot_ts_ns", "code")
        .len()
        .filter(pl.col("len") > 1)
        .select(pl.len())
        .collect()
        .item()
    )
    if duplicate_snapshots:
        raise RuntimeError(f"duplicate one-second book keys: {duplicate_snapshots}")
    invalid_asof = (
        snapshots.filter(
            (pl.col("book_receive_ts_ns") > pl.col("snapshot_ts_ns"))
            | (pl.col("book_age_ms") < 0.0)
        )
        .select(pl.len())
        .collect()
        .item()
    )
    if invalid_asof:
        raise RuntimeError(f"one-second books contain {invalid_asof} future observations")
    unknown_tick_symbols = set(
        ticks.select("code").unique().collect().get_column("code")
    ) - expected_symbols
    unknown_book_symbols = set(
        books.select("code").unique().collect().get_column("code")
    ) - expected_symbols
    if unknown_tick_symbols or unknown_book_symbols:
        raise RuntimeError(
            f"events outside universe ticks={sorted(unknown_tick_symbols)} "
            f"books={sorted(unknown_book_symbols)}"
        )
    age = snapshots.select(
        pl.col("book_age_ms").mean().alias("mean"),
        pl.col("book_age_ms").quantile(0.99).alias("p99"),
        pl.col("stale").sum().alias("stale"),
    ).collect().row(0, named=True)
    return {
        "schema_version": 1,
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "capture_id": capture_id,
        "symbols": len(expected_symbols),
        "universe_sha256": universe_sha256,
        "universe_checksum_verified": universe_checksum_verified,
        "tick_rows": sum(tick_counts.values()),
        "book_event_rows": sum(book_counts.values()),
        "book_1s_rows": sum(snapshot_counts.values()),
        "tick_parts": len(tick_paths),
        "book_event_parts": len(book_paths),
        "book_1s_parts": len(snapshot_paths),
        "book_age_ms_mean": float(age["mean"]),
        "book_age_ms_p99": float(age["p99"]),
        "stale_book_1s_rows": int(age["stale"]),
        "missed_snapshot_seconds": sum(
            int(manifest.get("missed_snapshot_seconds", 0)) for manifest in manifests
        ),
        "queue_high_watermark": max(
            int(manifest.get("queue_high_watermark", 0)) for manifest in manifests
        ),
    }


def main() -> None:
    args = parse_args()
    result = audit(
        universe_path=args.universe,
        capture_root=args.capture_root,
        trade_date=date.fromisoformat(str(args.trade_date)),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        f"[shioaji-micro-audit] status=ok date={result['trade_date']} "
        f"symbols={result['symbols']} ticks={result['tick_rows']} "
        f"books={result['book_event_rows']} book_1s={result['book_1s_rows']} "
        f"output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
