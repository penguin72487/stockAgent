from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from uuid import uuid4

import polars as pl
import pyarrow.dataset as pa_dataset
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.shioaji_capture_parts import (  # noqa: E402
    read_capture_manifests,
    select_capture_part_paths,
    shared_capture_id,
)


STREAMS = ("ticks", "book_events", "book_1s")
ORDER_COLUMN = {
    "ticks": "receive_ts_ns",
    "book_events": "receive_ts_ns",
    "book_1s": "snapshot_ts_ns",
}
SYMBOL_RE = re.compile(r"[A-Za-z0-9._-]+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compact completed Shioaji Tick/BidAsk/book_1s capture parts into "
            "exactly one Parquet file per symbol."
        )
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=Path("data_tw_microstructure/captures"),
    )
    parser.add_argument(
        "--selection-root",
        type=Path,
        default=Path("data_tw_microstructure/hft_dataset"),
        help="Audited HFT partitions whose capture_id selects source parts.",
    )
    parser.add_argument(
        "--audit-root",
        type=Path,
        default=Path("data_tw_microstructure/audits"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_tw_microstructure/symbol_dataset"),
    )
    parser.add_argument("--through-date", type=date.fromisoformat)
    parser.add_argument("--compression-level", type=int, default=7)
    parser.add_argument("--hash-workers", type=int, default=8)
    return parser.parse_args()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return value


def _completed_selections(
    capture_root: Path,
    selection_root: Path,
    audit_root: Path,
    through_date: date | None,
) -> tuple[
    dict[str, list[Path]],
    dict[str, dict[str, list[Path]]],
    list[dict[str, Any]],
    dict[str, int],
    set[str],
]:
    selected_paths = {stream: [] for stream in STREAMS}
    selected_paths_by_date: dict[str, dict[str, list[Path]]] = {}
    receipts: list[dict[str, Any]] = []
    expected_rows = {stream: 0 for stream in STREAMS}
    expected_symbols: set[str] = set()
    partitions = sorted(selection_root.glob("trade_date=*/summary.json"))
    for summary_path in partitions:
        trade_date = summary_path.parent.name.split("=", 1)[-1]
        parsed_date = date.fromisoformat(trade_date)
        if through_date is not None and parsed_date > through_date:
            continue
        summary = _read_json(summary_path)
        if summary.get("status") != "ok":
            raise RuntimeError(f"HFT selection is not complete: {summary_path}")
        audit_path = audit_root / f"hft_{trade_date}.json"
        audit = _read_json(audit_path)
        failures = audit.get("failures")
        if audit.get("status") != "ok" or not isinstance(failures, dict):
            raise RuntimeError(f"HFT audit is not valid: {audit_path}")
        if any(int(value) for value in failures.values()):
            raise RuntimeError(f"HFT audit has failures: {audit_path}")
        manifests = read_capture_manifests(capture_root, trade_date)
        if not manifests or any(item.get("status") != "complete" for item in manifests):
            raise RuntimeError(f"capture manifests are incomplete for {trade_date}")
        capture_id = shared_capture_id(manifests)
        summary_capture_id = str(summary.get("capture_id", ""))
        if capture_id is not None and capture_id != summary_capture_id:
            raise RuntimeError(
                f"HFT/capture provenance mismatch for {trade_date}: "
                f"summary={summary_capture_id} manifests={capture_id}"
            )
        for manifest in manifests:
            expected_symbols.update(str(value) for value in manifest.get("symbols", []))
            expected_rows["ticks"] += int(manifest.get("tick_rows_written", 0))
            expected_rows["book_events"] += int(
                manifest.get("book_rows_written", 0)
            )
            expected_rows["book_1s"] += int(
                manifest.get("book_1s_rows_written", 0)
            )
        files_for_date: dict[str, int] = {}
        paths_for_date: dict[str, list[Path]] = {}
        for stream in STREAMS:
            paths = select_capture_part_paths(
                capture_root=capture_root,
                kind=stream,
                trade_date=trade_date,
                manifests=manifests,
                verify_part_counts=True,
            )
            selected_paths[stream].extend(paths)
            paths_for_date[stream] = paths
            files_for_date[stream] = len(paths)
        selected_paths_by_date[trade_date] = paths_for_date
        receipts.append(
            {
                "trade_date": trade_date,
                "capture_id": capture_id,
                "hft_summary_sha256": _sha256(summary_path),
                "audit_sha256": _sha256(audit_path),
                "worker_manifest_sha256": [
                    _sha256(
                        capture_root
                        / "manifests"
                        / f"trade_date={trade_date}"
                        / f"worker={int(item['worker_index']):02d}.json"
                    )
                    for item in sorted(
                        manifests, key=lambda item: int(item["worker_index"])
                    )
                ],
                "source_files": files_for_date,
            }
        )
    if not receipts:
        raise RuntimeError("no completed audited capture dates were selected")
    if not expected_symbols:
        raise RuntimeError("selected capture manifests contain no symbols")
    return (
        selected_paths,
        selected_paths_by_date,
        receipts,
        expected_rows,
        expected_symbols,
    )


def _scan_stream(stream: str, paths: list[Path]) -> pl.LazyFrame:
    if not paths:
        raise RuntimeError(f"selected stream has no files: {stream}")
    order_column = ORDER_COLUMN[stream]
    frame = pl.scan_parquet(
        paths,
        hive_partitioning=False,
        rechunk=False,
        low_memory=True,
        cache=False,
    )
    schema = frame.collect_schema()
    if "code" not in schema or "trade_date" not in schema or order_column not in schema:
        raise RuntimeError(
            f"{stream} schema lacks code/trade_date/{order_column}: {schema.names()}"
        )
    return frame.with_columns(
        pl.lit(stream).alias("source_stream"),
        pl.col(order_column).cast(pl.Int64).alias("event_order_ts_ns"),
    )


def _symbol_file_summary(path: Path) -> dict[str, Any]:
    symbol = path.parent.name.split("=", 1)[-1]
    counts = (
        pl.scan_parquet(path)
        .group_by("source_stream")
        .len()
        .collect(engine="streaming")
    )
    stream_rows = {
        str(row["source_stream"]): int(row["len"])
        for row in counts.iter_rows(named=True)
    }
    codes = pl.read_parquet(path, columns=["code"]).get_column("code").unique()
    if codes.len() != 1 or str(codes.item()) != symbol:
        raise RuntimeError(f"symbol partition contains foreign rows: {path}")
    return {
        "symbol": symbol,
        "path": str(Path("symbols") / path.parent.name / path.name),
        "rows": sum(stream_rows.values()),
        "stream_rows": stream_rows,
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
    }


def _target_schema(selected: dict[str, list[Path]]) -> pl.Schema:
    frames = [
        _scan_stream(stream, [selected[stream][0]]) for stream in STREAMS
    ]
    return pl.concat(frames, how="diagonal_relaxed").collect_schema()


def _align_frame_to_schema(frame: pl.DataFrame, schema: pl.Schema) -> pl.DataFrame:
    names = set(frame.columns)
    expressions = []
    for name, dtype in schema.items():
        if name in names:
            expressions.append(pl.col(name).cast(dtype).alias(name))
        else:
            expressions.append(pl.lit(None, dtype=dtype).alias(name))
    return frame.select(expressions)


def _write_symbols_with_pyarrow(
    symbol_root: Path,
    selected_by_date: dict[str, dict[str, list[Path]]],
    selected: dict[str, list[Path]],
    *,
    compression_level: int,
) -> None:
    target_schema = _target_schema(selected)
    arrow_schema = pl.DataFrame(schema=target_schema).to_arrow().schema
    writers: dict[str, pq.ParquetWriter] = {}
    try:
        for date_position, (trade_date, date_paths) in enumerate(
            sorted(selected_by_date.items()), start=1
        ):
            for stream in STREAMS:
                paths = date_paths[stream]
                table = pa_dataset.dataset(
                    [str(path) for path in paths], format="parquet"
                ).to_table(use_threads=True)
                frame = pl.from_arrow(table).with_columns(
                    pl.lit(stream).alias("source_stream"),
                    pl.col(ORDER_COLUMN[stream])
                    .cast(pl.Int64)
                    .alias("event_order_ts_ns"),
                )
                frame = _align_frame_to_schema(frame, target_schema)
                partitions = frame.partition_by(
                    "code", as_dict=True, maintain_order=False
                )
                for key, partition in partitions.items():
                    symbol = str(key[0] if isinstance(key, tuple) else key)
                    if not SYMBOL_RE.fullmatch(symbol):
                        raise RuntimeError(
                            f"unsafe symbol in partition path: {symbol!r}"
                        )
                    writer = writers.get(symbol)
                    if writer is None:
                        output_path = (
                            symbol_root / f"symbol={symbol}" / "data.parquet"
                        )
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        writer = pq.ParquetWriter(
                            output_path,
                            arrow_schema,
                            compression="zstd",
                            compression_level=int(compression_level),
                            use_dictionary=True,
                            write_statistics=True,
                        )
                        writers[symbol] = writer
                    writer.write_table(
                        partition.to_arrow(), row_group_size=262_144
                    )
                print(
                    f"[microstructure-compact] direct_dates={date_position}/"
                    f"{len(selected_by_date)} trade_date={trade_date} "
                    f"stream={stream} rows={frame.height}",
                    flush=True,
                )
    finally:
        for writer in writers.values():
            writer.close()


def main() -> None:
    args = parse_args()
    capture_root = args.capture_root.resolve()
    selection_root = args.selection_root.resolve()
    audit_root = args.audit_root.resolve()
    output_root = args.output_root.resolve()
    if output_root.exists():
        raise RuntimeError(f"refusing to overwrite existing output: {output_root}")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    partial_root = output_root.parent / f".{output_root.name}.partial.{uuid4().hex}"
    symbol_root = partial_root / "symbols"
    (
        selected,
        selected_by_date,
        receipts,
        expected_rows,
        expected_symbols,
    ) = _completed_selections(
        capture_root,
        selection_root,
        audit_root,
        args.through_date,
    )
    source_file_count = sum(len(paths) for paths in selected.values())
    print(
        f"[microstructure-compact] dates={len(receipts)} "
        f"symbols={len(expected_symbols)} source_files={source_file_count}",
        flush=True,
    )
    _write_symbols_with_pyarrow(
        symbol_root,
        selected_by_date,
        selected,
        compression_level=int(args.compression_level),
    )
    output_files = sorted(symbol_root.glob("symbol=*/data.parquet"))
    if len(output_files) != len(expected_symbols):
        raise RuntimeError(
            f"symbol output count mismatch expected={len(expected_symbols)} "
            f"actual={len(output_files)}"
        )
    with ThreadPoolExecutor(max_workers=max(1, int(args.hash_workers))) as pool:
        symbol_summaries = list(pool.map(_symbol_file_summary, output_files))
    actual_rows = {stream: 0 for stream in STREAMS}
    for summary in symbol_summaries:
        for stream, count in summary["stream_rows"].items():
            if stream not in actual_rows:
                raise RuntimeError(f"unexpected compacted stream: {stream}")
            actual_rows[stream] += int(count)
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"compacted row accounting mismatch expected={expected_rows} "
            f"actual={actual_rows}"
        )
    actual_symbols = {str(item["symbol"]) for item in symbol_summaries}
    if actual_symbols != expected_symbols:
        raise RuntimeError("compacted symbol membership differs from capture manifests")
    manifest = {
        "schema_version": 1,
        "status": "complete",
        "source": "shioaji_streaming_v1",
        "layout": "one_parquet_per_symbol",
        "event_order_contract": (
            "event_order_ts_ns is receive_ts_ns for ticks/book_events and "
            "snapshot_ts_ns for book_1s; rows are not globally sorted"
        ),
        "streams": list(STREAMS),
        "trade_dates": [item["trade_date"] for item in receipts],
        "symbols": sorted(expected_symbols),
        "source_files": source_file_count,
        "rows": actual_rows,
        "total_rows": sum(actual_rows.values()),
        "partitions": sorted(symbol_summaries, key=lambda item: item["symbol"]),
        "source_receipts": receipts,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = partial_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(partial_root, output_root)
    print(
        f"[microstructure-compact] status=complete "
        f"symbols={len(symbol_summaries)} rows={manifest['total_rows']} "
        f"output={output_root}",
        flush=True,
    )


if __name__ == "__main__":
    main()
