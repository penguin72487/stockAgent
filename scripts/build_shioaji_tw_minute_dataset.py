from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import polars as pl


NS_PER_MINUTE = 60_000_000_000
SCHEMA_VERSION = 3
VOLUME_NOTIONAL_TOLERANCE = 0.05

MODEL_FEATURE_COLUMNS = (
    "log_close_return_1m",
    "gap_log_return",
    "intrabar_log_return",
    "range_log",
    "close_location",
    "relative_volume_20",
    "realized_volatility_20",
    "minutes_from_open",
    "time_sin",
    "time_cos",
)

EXECUTOR_ONLY_COLUMNS = (
    "label_valid_1m",
    "execution_open_next_1m",
    "exit_close_next_1m",
    "future_high_next_1m",
    "future_low_next_1m",
    "future_volume_shares_next_1m",
    "long_gross_return_next_1m",
    "short_gross_return_next_1m",
    "session_last_ts",
    "session_close",
    "session_exit_valid",
    "long_gross_return_to_session_close",
    "short_gross_return_to_session_close",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build causal per-date minute strategy partitions from receipt-backed "
            "Shioaji api.kbars() chunks. No Tick or BidAsk capture is read."
        )
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("data_tw_minute/shioaji_1m"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset"),
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated completed symbols. Default: all manifests.",
    )
    parser.add_argument(
        "--download-summary",
        type=Path,
        default=None,
        help=(
            "Receipt-backed full-market download summary. Defaults to "
            "<input-root>/download_summary.json."
        ),
    )
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON root is not an object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def build_research_frame(frame: pl.LazyFrame) -> pl.LazyFrame:
    """Create completed-bar features and strictly next-bar execution labels."""

    positive_volume = (pl.col("Volume") > 0.0) & (pl.col("Amount") > 0.0)

    def volume_multiplier_matches(multiplier: pl.Expr | float) -> pl.Expr:
        candidate = (
            multiplier if isinstance(multiplier, pl.Expr) else pl.lit(multiplier)
        )
        notional = pl.col("Volume") * candidate
        tolerance = VOLUME_NOTIONAL_TOLERANCE
        return (
            positive_volume
            & (pl.col("Amount") >= notional * pl.col("Low") * (1.0 - tolerance))
            & (pl.col("Amount") <= notional * pl.col("High") * (1.0 + tolerance))
        )

    # Historical Shioaji stock Kbars mix round-lot and direct-share Volume
    # encodings. Amount and the bar's OHLC range identify the source multiplier
    # without manufacturing a price or an executable quantity. Unknown positive
    # volume rows deliberately produce null capacity.
    multiplier_candidates: tuple[pl.Expr | float, ...] = (
        pl.col("contract_unit"),
        1_000.0,
        100.0,
        10.0,
        1.0,
    )
    source_volume_multiplier = pl.coalesce(
        *[
            pl.when(volume_multiplier_matches(candidate))
            .then(candidate)
            .otherwise(None)
            for candidate in multiplier_candidates
        ]
    )

    ordered = (
        frame.with_columns(
            pl.col("ts").cast(pl.Datetime("ns")),
            pl.col("date").cast(pl.Date),
            *[
                pl.col(name).cast(pl.Float64)
                for name in (
                    "Open",
                    "High",
                    "Low",
                    "Close",
                    "Volume",
                    "Amount",
                    "contract_unit",
                )
            ],
        )
        .sort(["symbol", "date", "ts"])
        .with_columns(
            (
                pl.col("ts").dt.hour().cast(pl.Int16) * 60
                + pl.col("ts").dt.minute().cast(pl.Int16)
                - 9 * 60
            )
            .cast(pl.Int16)
            .alias("minutes_from_open"),
            pl.col("ts").shift(1).over(["symbol", "date"]).alias("previous_ts"),
            pl.col("Close").shift(1).over(["symbol", "date"]).alias("previous_close"),
            pl.when((pl.col("Volume") == 0.0) & (pl.col("Amount") == 0.0))
            .then(pl.col("contract_unit"))
            .otherwise(source_volume_multiplier)
            .cast(pl.Float64)
            .alias("source_volume_multiplier"),
        )
        .with_columns(
            (
                pl.col("ts").cast(pl.Int64) - pl.col("previous_ts").cast(pl.Int64)
                == NS_PER_MINUTE
            )
            .fill_null(False)
            .alias("has_exact_previous_minute"),
            (
                pl.col("ts").shift(-1).over(["symbol", "date"]).cast(pl.Int64)
                - pl.col("ts").cast(pl.Int64)
                == NS_PER_MINUTE
            )
            .fill_null(False)
            .alias("has_exact_next_minute"),
            pl.col("source_volume_multiplier")
            .is_not_null()
            .alias("source_volume_unit_valid"),
        )
        .with_columns(
            pl.when(
                pl.col("has_exact_previous_minute") & (pl.col("previous_close") > 0)
            )
            .then((pl.col("Close") / pl.col("previous_close")).log())
            .otherwise(None)
            .alias("log_close_return_1m"),
            pl.when(
                pl.col("has_exact_previous_minute") & (pl.col("previous_close") > 0)
            )
            .then((pl.col("Open") / pl.col("previous_close")).log())
            .otherwise(None)
            .alias("gap_log_return"),
            (pl.col("Close") / pl.col("Open")).log().alias("intrabar_log_return"),
            (pl.col("High") / pl.col("Low")).log().alias("range_log"),
            pl.when(pl.col("High") > pl.col("Low"))
            .then((pl.col("Close") - pl.col("Low")) / (pl.col("High") - pl.col("Low")))
            .otherwise(0.5)
            .alias("close_location"),
            pl.when(pl.col("source_volume_unit_valid"))
            .then(pl.col("Volume") * pl.col("source_volume_multiplier"))
            .otherwise(None)
            .alias("volume_shares"),
        )
        .with_columns(
            pl.col("volume_shares")
            .rolling_mean(window_size=20, min_samples=5)
            .over(["symbol", "date"])
            .alias("rolling_volume_mean_20"),
            pl.col("log_close_return_1m")
            .rolling_std(window_size=20, min_samples=5, ddof=1)
            .over(["symbol", "date"])
            .alias("realized_volatility_20"),
        )
        .with_columns(
            pl.when(pl.col("rolling_volume_mean_20") > 0)
            .then(pl.col("volume_shares") / pl.col("rolling_volume_mean_20"))
            .otherwise(None)
            .alias("relative_volume_20"),
            (pl.col("minutes_from_open").cast(pl.Float64) * (2.0 * math.pi / 270.0))
            .sin()
            .alias("time_sin"),
            (pl.col("minutes_from_open").cast(pl.Float64) * (2.0 * math.pi / 270.0))
            .cos()
            .alias("time_cos"),
        )
    )
    feature_valid = pl.all_horizontal(
        pl.col("has_exact_previous_minute"),
        *[
            pl.col(name).is_not_null() & pl.col(name).is_finite()
            for name in MODEL_FEATURE_COLUMNS
        ],
    ).fill_null(False)
    label_valid = (
        pl.col("has_exact_next_minute")
        & (pl.col("Open").shift(-1).over(["symbol", "date"]) > 0)
        & (pl.col("Close").shift(-1).over(["symbol", "date"]) > 0)
    ).fill_null(False)
    return (
        ordered.with_columns(
            feature_valid.alias("feature_valid"),
            label_valid.alias("label_valid_1m"),
            pl.when(label_valid)
            .then(pl.col("Open").shift(-1).over(["symbol", "date"]))
            .otherwise(None)
            .alias("execution_open_next_1m"),
            pl.when(label_valid)
            .then(pl.col("Close").shift(-1).over(["symbol", "date"]))
            .otherwise(None)
            .alias("exit_close_next_1m"),
            pl.when(label_valid)
            .then(pl.col("High").shift(-1).over(["symbol", "date"]))
            .otherwise(None)
            .alias("future_high_next_1m"),
            pl.when(label_valid)
            .then(pl.col("Low").shift(-1).over(["symbol", "date"]))
            .otherwise(None)
            .alias("future_low_next_1m"),
            pl.when(label_valid)
            .then(pl.col("volume_shares").shift(-1).over(["symbol", "date"]))
            .otherwise(None)
            .alias("future_volume_shares_next_1m"),
            pl.col("ts").max().over(["symbol", "date"]).alias("session_last_ts"),
            pl.col("Close").last().over(["symbol", "date"]).alias("session_close"),
        )
        .with_columns(
            pl.when(pl.col("label_valid_1m"))
            .then(pl.col("exit_close_next_1m") / pl.col("execution_open_next_1m") - 1.0)
            .otherwise(None)
            .alias("long_gross_return_next_1m"),
            pl.when(pl.col("label_valid_1m"))
            .then(pl.col("execution_open_next_1m") / pl.col("exit_close_next_1m") - 1.0)
            .otherwise(None)
            .alias("short_gross_return_next_1m"),
            (
                (
                    pl.col("session_last_ts").dt.hour().cast(pl.Int16) * 60
                    + pl.col("session_last_ts").dt.minute().cast(pl.Int16)
                    == 13 * 60 + 30
                )
                & (pl.col("session_close") > 0)
            )
            .fill_null(False)
            .alias("session_exit_valid"),
        )
        .with_columns(
            pl.when(pl.col("session_exit_valid") & pl.col("label_valid_1m"))
            .then(pl.col("session_close") / pl.col("execution_open_next_1m") - 1.0)
            .otherwise(None)
            .alias("long_gross_return_to_session_close"),
            pl.when(pl.col("session_exit_valid") & pl.col("label_valid_1m"))
            .then(pl.col("execution_open_next_1m") / pl.col("session_close") - 1.0)
            .otherwise(None)
            .alias("short_gross_return_to_session_close"),
        )
        .drop("rolling_volume_mean_20")
        .sort(["ts", "symbol"])
    )


def discover_chunk_groups(
    input_root: Path,
    symbols: set[str] | None = None,
) -> tuple[dict[tuple[str, str], list[Path]], list[str]]:
    groups: dict[tuple[str, str], list[Path]] = defaultdict(list)
    selected_symbols: list[str] = []
    manifest_paths = sorted((input_root / "symbols").glob("*.manifest.json"))
    if symbols is not None:
        present = {path.name.split(".", 1)[0] for path in manifest_paths}
        missing = sorted(symbols - present)
        if missing:
            raise RuntimeError(f"minute symbol manifests are missing: {missing}")
    for manifest_path in manifest_paths:
        payload = _read_json(manifest_path)
        symbol = str(payload.get("symbol", ""))
        if symbols is not None and symbol not in symbols:
            continue
        if not (
            payload.get("schema_version") == 1
            and payload.get("source") == "shioaji_kbars_1m"
            and payload.get("storage_frequency") == "minute"
        ):
            raise RuntimeError(f"invalid minute symbol manifest: {manifest_path}")
        selected_symbols.append(symbol)
        for chunk in payload.get("chunks", []):
            if chunk.get("status") not in {"ok", "source_gap"}:
                continue
            raw_path = chunk.get("data_path")
            if not raw_path:
                continue
            path = Path(str(raw_path))
            if not path.is_file():
                raise RuntimeError(f"minute chunk is missing: {path}")
            expected = str(chunk.get("data_sha256", ""))
            actual = _sha256(path)
            if not expected or expected != actual:
                raise RuntimeError(
                    f"minute chunk fingerprint mismatch: {path} "
                    f"expected={expected} actual={actual}"
                )
            key = (str(chunk["start_date"]), str(chunk["end_date"]))
            groups[key].append(path)
    if not selected_symbols:
        raise RuntimeError("no completed minute symbol manifests selected")
    return dict(sorted(groups.items())), sorted(selected_symbols)


def _validate_collection_gate(
    path: Path,
    *,
    selected_symbols: list[str],
    subset_requested: bool,
) -> dict[str, Any]:
    if not path.is_file():
        raise RuntimeError(f"minute download summary is missing: {path}")
    payload = _read_json(path)
    required_identity = (
        payload.get("schema_version") == 1
        and payload.get("source") == "shioaji_kbars_1m"
        and payload.get("storage_frequency") == "minute"
        and bool(payload.get("simulation"))
    )
    if not required_identity:
        raise RuntimeError(f"invalid minute download summary identity: {path}")
    if payload.get("fatal_error"):
        raise RuntimeError(
            "minute collection has a fatal error: " + str(payload["fatal_error"])
        )
    selected = int(payload.get("selected_symbols", 0))
    reported = int(payload.get("reported_symbols", 0))
    failed = int(payload.get("failed_symbols", 0))
    partial = int(payload.get("partial_symbols", 0))
    available = int(payload.get("complete_symbols", 0)) + int(
        payload.get("complete_with_source_gap_symbols", 0)
    )
    unavailable = int(payload.get("contract_unavailable_symbols", 0))
    accounted = available + unavailable + failed + partial
    if not bool(payload.get("resumable_collection_complete")):
        raise RuntimeError(
            "minute collection is not research-ready: "
            f"selected={selected} reported={reported} available={available} "
            f"unavailable={unavailable} failed={failed} partial={partial}"
        )
    if selected <= 0 or reported != selected or accounted != selected:
        raise RuntimeError(
            "minute collection accounting is inconsistent: "
            f"selected={selected} reported={reported} accounted={accounted}"
        )
    if failed or partial:
        raise RuntimeError(
            f"minute collection still has failed={failed} partial={partial}"
        )
    if not subset_requested and len(selected_symbols) != available:
        raise RuntimeError(
            "completed minute manifests do not match available-source count: "
            f"manifests={len(selected_symbols)} available={available}"
        )
    return payload


def _feature_statistics(day_frame: pl.DataFrame) -> dict[str, Any]:
    valid = day_frame.filter(pl.col("feature_valid"))
    if valid.is_empty():
        counts = {name: 0 for name in MODEL_FEATURE_COLUMNS}
        sums = {name: 0.0 for name in MODEL_FEATURE_COLUMNS}
        sum_squares = {name: 0.0 for name in MODEL_FEATURE_COLUMNS}
    else:
        row = valid.select(
            *[pl.col(name).count().alias(f"{name}__count") for name in MODEL_FEATURE_COLUMNS],
            *[pl.col(name).sum().alias(f"{name}__sum") for name in MODEL_FEATURE_COLUMNS],
            *[
                (pl.col(name) * pl.col(name)).sum().alias(f"{name}__sum_square")
                for name in MODEL_FEATURE_COLUMNS
            ],
        ).row(0, named=True)
        counts = {
            name: int(row[f"{name}__count"] or 0) for name in MODEL_FEATURE_COLUMNS
        }
        sums = {
            name: float(row[f"{name}__sum"] or 0.0) for name in MODEL_FEATURE_COLUMNS
        }
        sum_squares = {
            name: float(row[f"{name}__sum_square"] or 0.0)
            for name in MODEL_FEATURE_COLUMNS
        }
    return {
        "feature_counts": counts,
        "feature_sums": sums,
        "feature_sum_squares": sum_squares,
    }


def main() -> None:
    args = parse_args()
    requested = {
        item.strip().upper() for item in str(args.symbols).split(",") if item.strip()
    }
    groups, symbols = discover_chunk_groups(
        args.input_root,
        requested if requested else None,
    )
    download_summary_path = (
        args.download_summary
        if args.download_summary is not None
        else args.input_root / "download_summary.json"
    )
    collection = _validate_collection_gate(
        download_summary_path,
        selected_symbols=symbols,
        subset_requested=bool(requested),
    )
    date_summaries: dict[str, dict[str, Any]] = {}
    for (chunk_start, chunk_end), paths in groups.items():
        frame = build_research_frame(
            pl.scan_parquet([str(path) for path in paths])
        ).collect(engine="streaming")
        for key, day_frame in frame.partition_by(
            "date", as_dict=True, maintain_order=True
        ).items():
            trade_date = key[0] if isinstance(key, tuple) else key
            date_text = str(trade_date)
            partition = args.output_root / f"trade_date={date_text}"
            partition.mkdir(parents=True, exist_ok=True)
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
                "schema_version": SCHEMA_VERSION,
                "status": "ok",
                "source": "shioaji_kbars_1m",
                "trade_date": date_text,
                "input_chunk_start": chunk_start,
                "input_chunk_end": chunk_end,
                "rows": day_frame.height,
                "symbols": day_frame["symbol"].n_unique(),
                "bars": day_frame["ts"].n_unique(),
                "feature_valid_rows": int(day_frame["feature_valid"].sum()),
                "label_valid_rows": int(day_frame["label_valid_1m"].sum()),
                "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
                "executor_only_columns": list(EXECUTOR_ONLY_COLUMNS),
                "output": str(output.relative_to(args.output_root)),
                "output_sha256": _sha256(output),
                **_feature_statistics(day_frame),
            }
            _atomic_json(partition / "summary.json", summary)
            date_summaries[date_text] = summary
        print(
            f"[tw-minute-build] chunk={chunk_start}..{chunk_end} "
            f"inputs={len(paths)} rows={frame.height}",
            flush=True,
        )
    _atomic_json(
        args.output_root / "manifest.json",
        {
            "schema_version": SCHEMA_VERSION,
            "status": "research_ready" if not requested else "research_subset",
            "research_ready": not requested,
            "source": "shioaji_kbars_1m",
            "decision_clock": "completed_right_labelled_1m_bar",
            "execution_clock": "next_1m_bar_open_proxy",
            "timezone": "Asia/Taipei",
            "download_summary": str(download_summary_path),
            "download_start_date": collection.get("start_date"),
            "download_end_date": collection.get("end_date"),
            "full_market_selected_symbols": int(
                collection.get("selected_symbols", 0)
            ),
            "available_source_symbols": int(collection.get("complete_symbols", 0))
            + int(collection.get("complete_with_source_gap_symbols", 0)),
            "source_gap_symbols": int(
                collection.get("complete_with_source_gap_symbols", 0)
            ),
            "contract_unavailable_symbols": int(
                collection.get("contract_unavailable_symbols", 0)
            ),
            "failed_symbols": int(collection.get("failed_symbols", 0)),
            "partial_symbols": int(collection.get("partial_symbols", 0)),
            "symbols": symbols,
            "dates": sorted(date_summaries),
            "partitions": [date_summaries[key] for key in sorted(date_summaries)],
            "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
            "executor_only_columns": list(EXECUTOR_ONLY_COLUMNS),
            "written_at_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
    )
    print(
        f"[tw-minute-build] complete symbols={len(symbols)} "
        f"dates={len(date_summaries)} output={args.output_root}",
        flush=True,
    )


if __name__ == "__main__":
    main()
