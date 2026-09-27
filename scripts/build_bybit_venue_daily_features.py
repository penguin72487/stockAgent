#!/usr/bin/env python3
"""Materialize only Bybit funding inputs for the 00:05 venue-only model."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from scripts.build_bybit_crypto_public_daily_features import (  # noqa: E402
    BYBIT_FEATURES,
    _bybit_funding_features,
    _feature_quality_rows,
    _sha256,
    _write_parquet_atomic,
    _write_text_atomic,
)


def build(daily_root: Path, output_path: Path) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    files = sorted(daily_root.glob("*_features.parquet"))
    if not files:
        raise FileNotFoundError(f"no Bybit daily symbol files in {daily_root}")
    summary_path = daily_root / "materialize_summary.json"
    materialization = json.loads(summary_path.read_text(encoding="utf-8"))
    if materialization.get("contract_version") != 6 or materialization.get("failed_symbols"):
        raise ValueError("Bybit 00:05/v6 materialization receipt is missing or failed")
    source_started = time.perf_counter()
    frames = [_bybit_funding_features(path, path.stem.removesuffix("_features")) for path in files]
    source_seconds = time.perf_counter() - source_started
    projection_started = time.perf_counter()
    output = pl.concat(frames, how="vertical").sort("date", "symbol")
    if output.select(pl.struct("date", "symbol").n_unique()).item() != output.height:
        raise ValueError("duplicate Bybit symbol-date funding rows")
    if output.columns != ["date", "symbol", *BYBIT_FEATURES]:
        raise ValueError("Bybit funding schema changed unexpectedly")
    # Compute both derived artifacts before replacing the canonical parquet.
    # A quality calculation failure must leave the previous output untouched.
    quality_csv = _feature_quality_rows(output).write_csv()
    projection_seconds = time.perf_counter() - projection_started
    # All values come from the prior completed session, not the forward funding label.
    write_started = time.perf_counter()
    _write_parquet_atomic(output, output_path)
    quality_path = output_path.with_name(f"{output_path.stem}_quality.csv")
    _write_text_atomic(quality_csv, quality_path)
    write_seconds = time.perf_counter() - write_started
    summary = {
        "contract_version": 1,
        "exchange_scope": "bybit",
        "decision_boundary_utc": "00:00",
        "execution_boundary_utc": "00:05",
        "source_daily_contract_version": 6,
        "started_at_utc": started_at.isoformat(),
        "ended_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds_before_summary": round(time.perf_counter() - started, 3),
        "stage_seconds": {
            "source_load": round(source_seconds, 3),
            "projection_and_quality": round(projection_seconds, 3),
            "artifact_write": round(write_seconds, 3),
        },
        "source_files": len(files),
        "output_rows": output.height,
        "output_columns": output.columns,
        "first_date": output["date"].min(),
        "last_date": output["date"].max(),
        "output_path": str(output_path),
        "output_sha256": _sha256(output_path),
        "quality_path": str(quality_path),
        "quality_sha256": _sha256(quality_path),
    }
    atomic_write_json(output_path.with_name(f"{output_path.stem}_summary.json"), summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daily-root", type=Path, default=ROOT / "data_bybit/perpetual_daily")
    parser.add_argument(
        "--output-path", type=Path,
        default=ROOT / "data_bybit/public_features/bybit_venue_daily.parquet",
    )
    args = parser.parse_args()
    print(json.dumps(build(args.daily_root, args.output_path), ensure_ascii=False))


if __name__ == "__main__":
    main()
