from __future__ import annotations

import argparse
from datetime import date, datetime, time
import json
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

NS_PER_SECOND = 1_000_000_000
TAIPEI = ZoneInfo("Asia/Taipei")


def _session_ns(trade_date: date, raw_time: str) -> int:
    local = datetime.combine(trade_date, time.fromisoformat(raw_time), tzinfo=TAIPEI)
    return int(local.timestamp() * NS_PER_SECOND)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Independently audit a Shioaji per-second HFT dataset partition."
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data_tw_microstructure/hft_dataset"),
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--session-start", default="09:00:00")
    parser.add_argument("--session-end", default="13:30:00")
    parser.add_argument("--horizons", default="1,5,30,60")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_tw_microstructure/audits/hft_latest.json"),
    )
    return parser.parse_args()


def audit_frame(
    frame: pl.DataFrame,
    *,
    trade_date: date,
    session_start: str,
    session_end: str,
    horizons: tuple[int, ...],
) -> dict[str, Any]:
    start_ns = _session_ns(trade_date, session_start)
    end_ns = _session_ns(trade_date, session_end)
    duplicate_keys = frame.group_by("snapshot_ts_ns", "code").len().filter(
        pl.col("len") > 1
    ).height
    out_of_session = frame.filter(
        (pl.col("snapshot_ts_ns") < start_ns)
        | (pl.col("snapshot_ts_ns") >= end_ns)
    ).height
    future_features = frame.filter(
        (pl.col("book_receive_ts_ns") > pl.col("snapshot_ts_ns"))
        | (
            pl.col("last_trade_receive_ts_ns").is_not_null()
            & (pl.col("last_trade_receive_ts_ns") > pl.col("snapshot_ts_ns"))
        )
        | (
            pl.col("last_book_event_receive_ts_ns").is_not_null()
            & (pl.col("last_book_event_receive_ts_ns") > pl.col("snapshot_ts_ns"))
        )
    ).height
    ordered = frame.sort(["code", "snapshot_ts_ns"])
    label_results: dict[str, dict[str, int]] = {}
    label_errors = 0
    for horizon in horizons:
        actual = f"label_valid_{horizon}s"
        expected = (
            pl.col("feature_valid")
            & pl.col("feature_valid").shift(-horizon).over("code")
            & (
                pl.col("snapshot_ts_ns").shift(-horizon).over("code")
                - pl.col("snapshot_ts_ns")
                == horizon * NS_PER_SECOND
            )
        ).fill_null(False)
        return_name = f"future_mid_log_return_{horizon}s"
        future_mid_name = f"future_mid_price_{horizon}s"
        markout_names = (
            f"long_cross_spread_markout_bps_{horizon}s",
            f"short_cross_spread_markout_bps_{horizon}s",
        )
        checked = ordered.select(
            (pl.col(actual) != expected).sum().alias("mask_mismatch"),
            (
                ~pl.col(actual)
                & pl.any_horizontal(
                    [
                        pl.col(name).is_not_null()
                        for name in (future_mid_name, return_name, *markout_names)
                    ]
                )
            )
            .sum()
            .alias("invalid_rows_with_labels"),
            pl.col(actual).sum().alias("valid_rows"),
        ).row(0, named=True)
        result = {key: int(value) for key, value in checked.items()}
        label_results[f"{horizon}s"] = result
        label_errors += result["mask_mismatch"] + result["invalid_rows_with_labels"]
    failures = {
        "duplicate_keys": int(duplicate_keys),
        "out_of_session_rows": int(out_of_session),
        "future_feature_rows": int(future_features),
        "unknown_universe_rows": int(frame["market_cap_rank"].null_count()),
        "label_errors": int(label_errors),
    }
    if any(failures.values()):
        raise RuntimeError(f"HFT dataset audit failed: {failures}")
    return {
        "schema_version": 1,
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "grain": ["trade_date", "snapshot_ts_ns", "code"],
        "rows": frame.height,
        "columns": frame.width,
        "symbols": frame["code"].n_unique(),
        "feature_valid_rows": int(frame["feature_valid"].sum()),
        "feature_valid_rate": float(frame["feature_valid"].mean()),
        "first_snapshot": str(frame["snapshot_datetime"].min()),
        "last_snapshot": str(frame["snapshot_datetime"].max()),
        "failures": failures,
        "labels": label_results,
    }


def main() -> None:
    args = parse_args()
    trade_date = date.fromisoformat(args.trade_date)
    path = args.dataset_root / f"trade_date={trade_date.isoformat()}" / "data.parquet"
    if not path.exists():
        raise RuntimeError(f"dataset partition does not exist: {path}")
    frame = pl.read_parquet(path)
    horizons = tuple(sorted({int(value) for value in args.horizons.split(",")}))
    result = audit_frame(
        frame,
        trade_date=trade_date,
        session_start=args.session_start,
        session_end=args.session_end,
        horizons=horizons,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        f"[shioaji-hft-audit] status=ok date={trade_date} rows={result['rows']} "
        f"symbols={result['symbols']} output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
