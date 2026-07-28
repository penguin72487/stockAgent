from __future__ import annotations

import argparse
from datetime import date
import json
import os
from pathlib import Path
import sys
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_shioaji_tw_minute_dataset import (  # noqa: E402
    EXECUTOR_ONLY_COLUMNS,
    MODEL_FEATURE_COLUMNS,
    NS_PER_MINUTE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit one Shioaji api.kbars minute-research partition."
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data_tw_minute/research_dataset"),
    )
    parser.add_argument("--trade-date", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_tw_minute/audits/latest.json"),
    )
    return parser.parse_args()


def audit_frame(frame: pl.DataFrame, *, trade_date: date) -> dict[str, Any]:
    required = {
        "date",
        "ts",
        "symbol",
        "feature_valid",
        "label_valid_1m",
        "execution_open_next_1m",
        "exit_close_next_1m",
        *MODEL_FEATURE_COLUMNS,
        *EXECUTOR_ONLY_COLUMNS,
    }
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"minute dataset is missing columns: {sorted(missing)}")
    duplicates = frame.group_by("ts", "symbol").len().filter(pl.col("len") > 1).height
    minute_of_day = pl.col("ts").dt.hour().cast(pl.Int16) * 60 + pl.col(
        "ts"
    ).dt.minute().cast(pl.Int16)
    out_of_session = frame.filter(
        (minute_of_day < 9 * 60 + 1)
        | (minute_of_day > 13 * 60 + 30)
        | (pl.col("ts").dt.second() != 0)
    ).height
    wrong_date = frame.filter(pl.col("date") != pl.lit(trade_date)).height
    invalid_rows_with_labels = frame.filter(
        ~pl.col("label_valid_1m")
        & pl.any_horizontal(
            [
                pl.col(name).is_not_null()
                for name in (
                    "execution_open_next_1m",
                    "exit_close_next_1m",
                    "future_high_next_1m",
                    "future_low_next_1m",
                    "future_volume_shares_next_1m",
                    "long_gross_return_next_1m",
                    "short_gross_return_next_1m",
                )
            ]
        )
    ).height
    invalid_session_rows_with_labels = frame.filter(
        ~pl.col("session_exit_valid")
        & pl.any_horizontal(
            [
                pl.col("long_gross_return_to_session_close").is_not_null(),
                pl.col("short_gross_return_to_session_close").is_not_null(),
            ]
        )
    ).height
    ordered = frame.sort(["symbol", "date", "ts"])
    expected_next_ts = pl.col("ts").shift(-1).over(["symbol", "date"])
    bad_label_alignment = ordered.filter(
        pl.col("label_valid_1m")
        & (
            expected_next_ts.cast(pl.Int64) - pl.col("ts").cast(pl.Int64)
            != NS_PER_MINUTE
        )
    ).height
    bad_label_values = ordered.filter(
        pl.col("label_valid_1m")
        & (
            (
                pl.col("execution_open_next_1m")
                != pl.col("Open").shift(-1).over(["symbol", "date"])
            )
            | (
                pl.col("exit_close_next_1m")
                != pl.col("Close").shift(-1).over(["symbol", "date"])
            )
        )
    ).height
    failures = {
        "duplicate_keys": int(duplicates),
        "out_of_session_rows": int(out_of_session),
        "wrong_date_rows": int(wrong_date),
        "invalid_rows_with_labels": int(invalid_rows_with_labels),
        "invalid_session_rows_with_labels": int(invalid_session_rows_with_labels),
        "bad_label_alignment_rows": int(bad_label_alignment),
        "bad_label_value_rows": int(bad_label_values),
    }
    if any(failures.values()):
        raise RuntimeError(f"minute dataset audit failed: {failures}")
    return {
        "schema_version": 1,
        "status": "ok",
        "trade_date": trade_date.isoformat(),
        "rows": frame.height,
        "symbols": frame["symbol"].n_unique(),
        "bars": frame["ts"].n_unique(),
        "feature_valid_rows": int(frame["feature_valid"].sum()),
        "label_valid_rows": int(frame["label_valid_1m"].sum()),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "executor_only_columns": list(EXECUTOR_ONLY_COLUMNS),
        "failures": failures,
    }


def main() -> None:
    args = parse_args()
    trade_date = date.fromisoformat(args.trade_date)
    path = args.dataset_root / f"trade_date={trade_date}" / "data.parquet"
    if not path.is_file():
        raise RuntimeError(f"minute research partition does not exist: {path}")
    result = audit_frame(pl.read_parquet(path), trade_date=trade_date)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, args.output)
    print(
        f"[tw-minute-audit] status=ok date={trade_date} "
        f"rows={result['rows']} symbols={result['symbols']} output={args.output}",
        flush=True,
    )


if __name__ == "__main__":
    main()
