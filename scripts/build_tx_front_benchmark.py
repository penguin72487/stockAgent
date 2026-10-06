#!/usr/bin/env python3
"""Rebuild a hash-pinned TX reference from retained official observations.

The normalized source and every raw archive must pass their existing receipt.
Reuse canonical contract ranking and benchmark returns; never use strategy
results as price inputs. This does not change the tradable action universe.
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_benchmark import load_tx_front_rolling_benchmark
from stockagent.data.tw_futures_portfolio_daily import _contract_metadata, _stable_rank_map


def build_reference(*, source: Path, raw_root: Path, output: Path,
                    start: date, end: date, audit_paths: list[Path]) -> dict:
    if end < start:
        raise ValueError("benchmark end precedes start")
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("use a new empty output directory; references are immutable")
    source_manifest = source.with_name("manifest.json")
    receipt = json.loads(source_manifest.read_text())
    source_sha = sha256_file(source)
    if receipt.get("outputs", {}).get(source.name, {}).get("sha256") != source_sha:
        raise ValueError("official observation SHA-256 mismatch")
    raw_sources = []
    for item in receipt["sources"]:
        # Migration retained raw files under their content digest, including
        # the original extension. Old receipt paths are provenance, not aliases
        # to unverified or newly downloaded substitute observations.
        path = raw_root / (item["sha256"] + Path(item["path"]).suffix)
        if sha256_file(path) != item["sha256"]:
            raise ValueError(f"official raw archive SHA-256 mismatch: {path}")
        raw_sources.append({"original_path": item["path"], "path": str(path),
                            "sha256": item["sha256"]})
    if not raw_sources:
        raise ValueError("official observation receipt has no raw sources")
    frame = (pl.scan_parquet(source)
             .filter((pl.col("product") == "TX") & (pl.col("session") == "一般")
                     & pl.col("contract").str.contains(r"^\d{6}$")
                     & pl.col("date").is_between(start, end))
             .select("date", "product", "contract", "series_type", "close", "volume", "source_sha256")
             .collect())
    if frame.is_empty() or frame.select("date", "product", "contract").is_duplicated().any():
        raise ValueError("TX source is empty or has duplicate physical contract-days")
    if not set(frame["source_sha256"]) <= {s["sha256"] for s in raw_sources}:
        raise ValueError("TX price rows reference unverified raw sources")
    ranks = _stable_rank_map(frame, _contract_metadata(frame.lazy()))
    reference = (ranks.join(frame, on=["date", "product", "contract"], how="left", validate="1:1")
                 .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
                 .with_columns((pl.col("volume") > 0).fill_null(False).alias("source_row_observed"))
                 .select("date", "product", "contract", "tenor_rank", "close",
                         "source_row_observed", "source_sha256")
                 .sort("date", "contract"))
    # Do not fill no-print closes or promote a different contract to front.
    # Validate every source session, including own-contract marks on roll days.
    dates = frame["date"].unique().sort().to_numpy().astype("datetime64[D]")
    output.mkdir(parents=True, exist_ok=True)
    target = output / "continuous_daily.parquet"
    atomic_write_parquet(target, reference)
    payload = load_tx_front_rolling_benchmark(target, dates)
    parity = []
    for audit in audit_paths:
        with np.load(audit, allow_pickle=False) as previous:
            actual = load_tx_front_rolling_benchmark(target, previous["dates"])
            for key, values in actual.items():
                equal = (np.array_equal(values, previous[key], equal_nan=True)
                         if values.dtype.kind == "f" else np.array_equal(values, previous[key]))
                if not equal:
                    raise ValueError(f"rebuilt benchmark differs from retained audit: {audit} / {key}")
            parity.append({"path": str(audit), "sha256": sha256_file(audit),
                           "sessions": len(actual["dates"]), "fields": list(actual),
                           "exact_equal": True})
    result = {
        "schema_version": 1, "dataset": "tx_front_rolling_benchmark_reference",
        "status": "complete", "benchmark_contract": "tx_front_rolling_1x_gross",
        "selection": "TX monthly regular session; canonical active-contract tenor ranking",
        "roll_gap_treatment": "new_front_contract_own_previous_session_close",
        "cost_treatment": "gross_no_fees_or_tax", "action_universe_changed": False,
        "requested_start": str(start), "requested_end": str(end),
        "date_start": str(dates[0]), "date_end": str(dates[-1]),
        "rows": reference.height, "sessions": len(dates),
        "roll_events": int(payload["front_month_roll_mask"].sum()),
        "source": {"path": str(source), "sha256": source_sha,
                   "manifest_path": str(source_manifest), "manifest_sha256": sha256_file(source_manifest)},
        "raw_sources": raw_sources, "retained_audit_parity": parity,
        "builder_sha256": sha256_file(Path(__file__)),
        "canonical_ranking_sha256": sha256_file(ROOT / "stockagent/data/tw_futures_portfolio_daily.py"),
        "canonical_returns_sha256": sha256_file(ROOT / "stockagent/data/tw_futures_benchmark.py"),
        "outputs": {"continuous_daily": {"path": str(target), "sha256": sha256_file(target)}},
    }
    atomic_write_json(output / "manifest.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--compare-audit", type=Path, action="append", default=[])
    args = parser.parse_args()
    result = build_reference(source=args.source, raw_root=args.raw_root, output=args.output_dir,
                             start=args.start, end=args.end, audit_paths=args.compare_audit)
    print(json.dumps({k: result[k] for k in ("status", "date_start", "date_end", "rows",
                                          "sessions", "roll_events", "outputs")}, indent=2))


if __name__ == "__main__":
    main()
