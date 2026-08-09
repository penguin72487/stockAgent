#!/usr/bin/env python3
"""Materialize causal developing-daily-candle features into minute receipts.

The schema-4 Shioaji dataset remains immutable.  This one-way, resumable
upgrade writes a fresh schema-5 root containing the original ten minute
microstructure fields plus fifteen current-session-to-date daily-candle
fields.  Every row is computable at that completed minute.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_minute import (
    MINUTE_DATASET_SCHEMA_VERSION,
    MINUTE_DEVELOPING_CANDLE_CONTRACT,
    MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS,
    MINUTE_FEATURE_COLUMNS,
    MINUTE_FEATURE_STATISTICS_CONTRACT,
    MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS,
    add_developing_daily_candle_features,
    summarize_minute_sessions_for_next_day,
)


SOURCE_SCHEMA_VERSION = 4


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset_stats_v4"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset_developing_v5"),
    )
    parser.add_argument(
        "--daily-stock-root",
        type=Path,
        default=Path("data_tw_public/stocks"),
        help="Used only to seed the completed session before the first minute date.",
    )
    parser.add_argument("--seed-workers", type=int, default=32)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse only already-written partitions whose summary and SHA256 pass.",
    )
    parser.add_argument(
        "--max-dates",
        type=int,
        default=None,
        help="Bounded smoke/debug upgrade; the resulting manifest is not research-ready.",
    )
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return value


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _source_partition_path(root: Path, summary: dict[str, Any]) -> Path:
    raw = Path(str(summary.get("output", "")))
    candidates = (
        raw,
        root / raw,
        root / f"trade_date={summary.get('trade_date')}" / "data.parquet",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    raise RuntimeError(
        f"source minute partition is missing: date={summary.get('trade_date')}"
    )


def _feature_statistics(day_frame: pl.DataFrame) -> dict[str, Any]:
    valid = day_frame.filter(pl.col("feature_valid"))
    if valid.is_empty():
        return {
            "feature_counts": {name: 0 for name in MINUTE_FEATURE_COLUMNS},
            "feature_sums": {name: 0.0 for name in MINUTE_FEATURE_COLUMNS},
            "feature_sum_squares": {
                name: 0.0 for name in MINUTE_FEATURE_COLUMNS
            },
        }
    row = valid.select(
        *[
            pl.col(name).count().alias(f"{name}__count")
            for name in MINUTE_FEATURE_COLUMNS
        ],
        *[
            pl.col(name).cast(pl.Float64).sum().alias(f"{name}__sum")
            for name in MINUTE_FEATURE_COLUMNS
        ],
        *[
            (pl.col(name).cast(pl.Float64) ** 2)
            .sum()
            .alias(f"{name}__sum_square")
            for name in MINUTE_FEATURE_COLUMNS
        ],
    ).row(0, named=True)
    return {
        "feature_counts": {
            name: int(row[f"{name}__count"] or 0)
            for name in MINUTE_FEATURE_COLUMNS
        },
        "feature_sums": {
            name: float(row[f"{name}__sum"] or 0.0)
            for name in MINUTE_FEATURE_COLUMNS
        },
        "feature_sum_squares": {
            name: float(row[f"{name}__sum_square"] or 0.0)
            for name in MINUTE_FEATURE_COLUMNS
        },
    }


def _seed_date(
    daily_stock_root: Path,
    *,
    benchmark: str,
    first_minute_date: date,
) -> date:
    path = daily_stock_root / f"{benchmark}_features.parquet"
    if not path.is_file():
        raise RuntimeError(f"daily seed benchmark is missing: {path}")
    value = (
        pl.scan_parquet(path)
        .select("date")
        .filter(pl.col("date") < pl.lit(first_minute_date))
        .max()
        .collect()
        .item()
    )
    if not isinstance(value, date):
        raise RuntimeError("daily source has no completed seed session")
    return value


def _load_seed_sessions(
    daily_stock_root: Path,
    *,
    symbols: list[str],
    seed_date: date,
    workers: int,
) -> pl.DataFrame:
    columns = ["date", "open", "max", "min", "close", "Trading_Volume"]

    def load_one(symbol: str) -> dict[str, Any]:
        path = daily_stock_root / f"{symbol}_features.parquet"
        if not path.is_file():
            return {"symbol": symbol}
        table = pq.read_table(
            path,
            columns=columns,
            filters=[("date", "=", seed_date)],
        )
        if table.num_rows != 1:
            return {"symbol": symbol}
        values = table.to_pydict()
        return {
            "symbol": symbol,
            "Open": values["open"][0],
            "High": values["max"][0],
            "Low": values["min"][0],
            "Close": values["close"][0],
            "volume_shares": values["Trading_Volume"][0],
        }

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        rows = list(executor.map(load_one, symbols))
    raw = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.lit(datetime.combine(seed_date, time(13, 30))).alias("ts"),
        (
            pl.col("Open").is_not_null()
            & pl.col("High").is_not_null()
            & pl.col("Low").is_not_null()
            & pl.col("Close").is_not_null()
            & pl.col("volume_shares").is_not_null()
        ).alias("source_volume_unit_valid"),
        pl.lit(True).alias("session_exit_valid"),
    )
    return summarize_minute_sessions_for_next_day(raw)


def _reusable_partition(
    partition: Path,
    *,
    trade_date: str,
) -> dict[str, Any] | None:
    summary_path = partition / "summary.json"
    output = partition / "data.parquet"
    if not summary_path.is_file() or not output.is_file():
        return None
    summary = _read_json(summary_path)
    compatible = (
        summary.get("trade_date") == trade_date
        and summary.get("schema_version") == MINUTE_DATASET_SCHEMA_VERSION
        and summary.get("feature_statistics_contract")
        == MINUTE_FEATURE_STATISTICS_CONTRACT
        and summary.get("developing_candle_contract")
        == MINUTE_DEVELOPING_CANDLE_CONTRACT
        and tuple(summary.get("model_feature_columns", ()))
        == MINUTE_FEATURE_COLUMNS
        and summary.get("output_sha256") == _sha256(output)
    )
    return summary if compatible else None


def _advance_completed_symbol_sessions(
    previous_sessions: pl.DataFrame,
    current_sessions: pl.DataFrame,
) -> pl.DataFrame:
    """Replace baselines only for symbols with a verified completed close."""

    completed = current_sessions.filter(pl.col("previous_session_valid"))
    if completed.is_empty():
        return previous_sessions
    completed_symbols = completed.select("symbol")
    retained = previous_sessions.join(completed_symbols, on="symbol", how="anti")
    return pl.concat([retained, completed], how="vertical").sort("symbol")


def main() -> None:
    args = parse_args()
    source_root = args.source_root.resolve()
    output_root = args.output_root.resolve()
    daily_stock_root = args.daily_stock_root.resolve()
    source_manifest_path = source_root / "manifest.json"
    source_manifest = _read_json(source_manifest_path)
    if source_manifest.get("schema_version") != SOURCE_SCHEMA_VERSION:
        raise RuntimeError("developing-candle upgrade requires schema-4 input")
    if tuple(source_manifest.get("model_feature_columns", ())) != (
        MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS
    ):
        raise RuntimeError("schema-4 minute feature columns are incompatible")
    symbols = [str(value) for value in source_manifest.get("symbols", ())]
    dates = [str(value) for value in source_manifest.get("dates", ())]
    partitions = list(source_manifest.get("partitions", ()))
    if not symbols or not dates or len(partitions) != len(dates):
        raise RuntimeError("source minute manifest is incomplete")
    if output_root == source_root:
        raise RuntimeError("output root must differ from immutable schema-4 input")
    if output_root.exists() and any(output_root.iterdir()) and not args.resume:
        raise RuntimeError(
            f"output root already exists; pass --resume after inspection: {output_root}"
        )
    output_root.mkdir(parents=True, exist_ok=True)

    first_date = date.fromisoformat(dates[0])
    benchmark = "2330" if "2330" in symbols else symbols[0]
    seed_date = _seed_date(
        daily_stock_root,
        benchmark=benchmark,
        first_minute_date=first_date,
    )
    previous_sessions = _load_seed_sessions(
        daily_stock_root,
        symbols=symbols,
        seed_date=seed_date,
        workers=args.seed_workers,
    )
    print(
        f"[tw-minute-v5] seed_date={seed_date} "
        f"valid_symbols={previous_sessions['previous_session_valid'].sum()} "
        f"total_symbols={len(symbols)}",
        flush=True,
    )

    maximum = len(partitions) if args.max_dates is None else max(0, args.max_dates)
    selected_partitions = partitions[:maximum]
    output_summaries: list[dict[str, Any]] = []
    for position, source_summary in enumerate(selected_partitions, start=1):
        trade_date = str(source_summary.get("trade_date", ""))
        source_path = _source_partition_path(source_root, source_summary)
        raw = pl.read_parquet(source_path)
        current_sessions = summarize_minute_sessions_for_next_day(raw)
        next_previous_sessions = _advance_completed_symbol_sessions(
            previous_sessions, current_sessions
        )
        partition = output_root / f"trade_date={trade_date}"
        partition.mkdir(parents=True, exist_ok=True)
        summary = _reusable_partition(partition, trade_date=trade_date) if args.resume else None
        if summary is None:
            day_frame = add_developing_daily_candle_features(raw, previous_sessions)
            output = partition / "data.parquet"
            temporary = output.with_suffix(".parquet.tmp")
            day_frame.write_parquet(
                temporary,
                compression="zstd",
                compression_level=7,
                statistics=True,
                row_group_size=128_000,
            )
            os.replace(temporary, output)
            summary = {
                **source_summary,
                "schema_version": MINUTE_DATASET_SCHEMA_VERSION,
                "feature_statistics_contract": MINUTE_FEATURE_STATISTICS_CONTRACT,
                "developing_candle_contract": MINUTE_DEVELOPING_CANDLE_CONTRACT,
                "source_schema_version": SOURCE_SCHEMA_VERSION,
                "source_output_sha256": source_summary.get("output_sha256"),
                "feature_valid_rows": int(day_frame["feature_valid"].sum()),
                "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
                "output": str(output.relative_to(output_root)),
                "output_sha256": _sha256(output),
                **_feature_statistics(day_frame),
            }
            _atomic_json(partition / "summary.json", summary)
            action = "wrote"
        else:
            action = "reused"
        output_summaries.append(summary)
        previous_sessions = next_previous_sessions
        print(
            f"[tw-minute-v5] {action}={position}/{len(selected_partitions)} "
            f"date={trade_date} rows={raw.height} "
            f"valid={summary['feature_valid_rows']}",
            flush=True,
        )

    complete = len(selected_partitions) == len(partitions)
    output_manifest = {
        **source_manifest,
        "schema_version": MINUTE_DATASET_SCHEMA_VERSION,
        "feature_statistics_contract": MINUTE_FEATURE_STATISTICS_CONTRACT,
        "status": "research_ready" if complete else "research_subset",
        "research_ready": bool(complete and source_manifest.get("research_ready")),
        "dates": dates[: len(selected_partitions)],
        "partitions": output_summaries,
        "model_feature_columns": list(MINUTE_FEATURE_COLUMNS),
        "minute_microstructure_feature_columns": list(
            MINUTE_MICROSTRUCTURE_FEATURE_COLUMNS
        ),
        "developing_candle_feature_columns": list(
            MINUTE_DEVELOPING_CANDLE_FEATURE_COLUMNS
        ),
        "developing_candle_contract": MINUTE_DEVELOPING_CANDLE_CONTRACT,
        "developing_candle_seed_date": str(seed_date),
        "source_schema_version": SOURCE_SCHEMA_VERSION,
        "source_manifest_sha256": _sha256(source_manifest_path),
        "written_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }
    _atomic_json(output_root / "manifest.json", output_manifest)
    print(
        f"[tw-minute-v5] complete={complete} dates={len(output_summaries)} "
        f"output={output_root}",
        flush=True,
    )


if __name__ == "__main__":
    main()
