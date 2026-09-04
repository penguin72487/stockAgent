#!/usr/bin/env python3
"""Replace only the Bybit funding slice in a causal public feature table."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

try:
    from scripts.build_bybit_crypto_public_daily_features import (
        BYBIT_FEATURES,
        _bybit_funding_features,
        _write_text_atomic,
    )
except ModuleNotFoundError:  # direct `python scripts/...py` execution
    from build_bybit_crypto_public_daily_features import (
        BYBIT_FEATURES,
        _bybit_funding_features,
        _write_text_atomic,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet_atomic(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), temporary, compression="snappy", write_statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def rebase_bybit_funding_slice(
    base: pl.DataFrame,
    funding_slice: pl.DataFrame,
) -> pl.DataFrame:
    keys = ["date", "symbol"]
    for name, frame in (("base", base), ("funding_slice", funding_slice)):
        missing = set(keys) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} missing keys: {sorted(missing)}")
        if frame.select(pl.struct(keys).is_duplicated().any()).item():
            raise ValueError(f"{name} contains duplicate (date, symbol) rows")
    missing_features = set(BYBIT_FEATURES) - set(funding_slice.columns)
    if missing_features:
        raise ValueError(
            f"funding_slice missing Bybit features: {sorted(missing_features)}"
        )

    normalized_base = base.with_columns(
        pl.col("date").cast(pl.String), pl.col("symbol").cast(pl.String)
    )
    normalized_slice = funding_slice.select(
        pl.col("date").cast(pl.String),
        pl.col("symbol").cast(pl.String),
        *BYBIT_FEATURES,
    )
    retained = normalized_base.drop(
        [name for name in BYBIT_FEATURES if name in normalized_base.columns]
    )
    union_keys = pl.concat(
        [retained.select(keys), normalized_slice.select(keys)],
        how="vertical",
    ).unique(keys)
    output = (
        union_keys.join(retained, on=keys, how="left")
        .join(normalized_slice, on=keys, how="left")
        .sort(keys)
    )
    # Do not pass the result through the canonical feature selector: the base
    # table can contain source-availability audit timestamps in addition to the
    # model feature ABI, and this operation promises to preserve them exactly.
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preserve a verified public feature table while replacing its Bybit "
            "funding fields from a new daily execution-clock contract."
        )
    )
    parser.add_argument("--base-feature-path", required=True)
    parser.add_argument("--bybit-daily-dir", required=True)
    parser.add_argument("--output-path", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_path = Path(args.base_feature_path)
    daily_dir = Path(args.bybit_daily_dir)
    output_path = Path(args.output_path)
    symbols_path = daily_dir / "symbols.csv"
    if not base_path.is_file() or not symbols_path.is_file():
        raise FileNotFoundError("base feature table or Bybit symbols.csv is missing")

    symbols = pl.read_csv(symbols_path)["code"].cast(pl.String).to_list()
    frames = []
    for symbol in symbols:
        path = daily_dir / f"{symbol}_features.parquet"
        if not path.is_file():
            raise FileNotFoundError(f"missing v7 daily file: {path}")
        frames.append(_bybit_funding_features(path, symbol))
    funding_slice = pl.concat(frames, how="vertical_relaxed")
    base = pl.read_parquet(base_path)
    output = rebase_bybit_funding_slice(base, funding_slice)
    _write_parquet_atomic(output, output_path)

    summary = {
        "contract_version": 1,
        "operation": "replace_bybit_funding_slice_preserve_other_public_features",
        "decision_boundary_utc": "00:00",
        "execution_boundary_utc": "00:00",
        "base_feature_path": str(base_path),
        "base_feature_sha256": _sha256(base_path),
        "bybit_daily_dir": str(daily_dir),
        "bybit_daily_contract_version": 7,
        "symbols": len(symbols),
        "funding_slice_rows": funding_slice.height,
        "output_rows": output.height,
        "output_columns": len(output.columns),
        "output_path": str(output_path),
        "output_sha256": _sha256(output_path),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    summary_path = output_path.with_name(f"{output_path.stem}_summary.json")
    _write_text_atomic(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", summary_path
    )
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
