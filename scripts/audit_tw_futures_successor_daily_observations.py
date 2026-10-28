"""Compare source-bound corporate successors with retained official daily rows.

This diagnostic never creates a fill, inventory transfer, terminal event, or
training input. A missing daily row does not prove that no such event existed.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--successors", required=True, type=Path)
    parser.add_argument("--successors-report", required=True, type=Path)
    parser.add_argument("--daily", required=True, type=Path)
    parser.add_argument("--daily-manifest", required=True, type=Path)
    parser.add_argument("--detail", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    if args.detail.exists() or args.report.exists():
        raise FileExistsError("preserve the existing independent readback")

    source_paths = [args.successors, args.successors_report, args.daily,
                    args.daily_manifest, Path(__file__).resolve()]
    pins = {str(path): sha256_file(path) for path in source_paths}
    successor_report = json.loads(args.successors_report.read_bytes())
    daily_manifest = json.loads(args.daily_manifest.read_bytes())
    if successor_report.get("detail_sha256") != pins[str(args.successors)]:
        raise ValueError("corporate successor readback SHA mismatch")
    if (daily_manifest.get("outputs", {}).get(args.daily.name, {}).get("sha256")
            != pins[str(args.daily)]):
        raise ValueError("official daily source manifest SHA mismatch")

    targets = pl.read_csv(args.successors, try_parse_dates=True,
                          schema_overrides={"successor_contract": pl.String})
    if (targets.height != successor_report["target_boundaries"]
            or targets["original_physical_contract"].n_unique() != targets.height):
        raise ValueError("successor target identities or denominator changed")
    targets = targets.with_columns(
        pl.col("original_physical_contract").str.split(":").list.first().alias("origin_product"),
        pl.col("conservative_end_exclusive").cast(pl.Date),
    )
    products = sorted(set(targets["origin_product"].to_list()) | set(targets["successor_product"].to_list()))
    months = targets["successor_contract"].unique().to_list()
    daily = (pl.scan_parquet(args.daily)
             .filter((pl.col("session") == "一般") & pl.col("product").is_in(products)
                     & pl.col("contract").is_in(months))
             .select("date", "product", "contract", "open_interest", "volume",
                     "settlement", "source_sha256")
             .collect())
    if daily.select("date", "product", "contract").is_duplicated().any():
        raise ValueError("official daily session has duplicate contract-date keys")

    origins = (targets.join(daily, left_on=["origin_product", "successor_contract", "boundary_date"],
                            right_on=["product", "contract", "date"], how="left")
               .rename({"open_interest": "origin_boundary_open_interest",
                        "volume": "origin_boundary_volume", "settlement": "origin_boundary_settlement",
                        "source_sha256": "origin_boundary_source_sha256"}))
    successors = (targets.join(daily, left_on=["successor_product", "successor_contract"],
                               right_on=["product", "contract"], how="inner")
                  .filter((pl.col("date") >= pl.col("conversion_effective_date"))
                          & (pl.col("conservative_end_exclusive").is_null()
                             | (pl.col("date") < pl.col("conservative_end_exclusive")))))
    successor_counts = successors.group_by("original_physical_contract").agg(
        pl.len().alias("successor_daily_rows"),
        (pl.col("open_interest") > 0).sum().alias("successor_positive_oi_rows"),
        (pl.col("volume") > 0).sum().alias("successor_positive_volume_rows"),
        pl.col("date").min().alias("successor_first_date"),
        pl.col("date").max().alias("successor_last_date"),
    )
    detail = (origins.join(successor_counts, on="original_physical_contract", how="left", validate="1:1")
              .with_columns(pl.col("successor_daily_rows", "successor_positive_oi_rows",
                                   "successor_positive_volume_rows").fill_null(0))
              .select("original_physical_contract", "boundary_date", "successor_product",
                      "successor_contract", "conversion_effective_date", "conservative_end_exclusive",
                      "origin_boundary_open_interest", "origin_boundary_volume",
                      "origin_boundary_settlement", "origin_boundary_source_sha256",
                      "successor_daily_rows", "successor_positive_oi_rows",
                      "successor_positive_volume_rows", "successor_first_date", "successor_last_date")
              .sort("boundary_date", "original_physical_contract"))
    if detail.height != targets.height or any(sha256_file(path) != pins[str(path)] for path in source_paths):
        raise ValueError("a bound source or target changed during readback")
    args.detail.parent.mkdir(parents=True, exist_ok=True)
    with args.detail.open("x", encoding="utf-8", newline="") as handle:
        detail.write_csv(handle)
    report = {
        "contract": "source_bound_futures_successor_daily_readback_v1",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "target_boundaries": detail.height,
        "origin_boundary_daily_rows": detail.filter(pl.col("origin_boundary_source_sha256").is_not_null()).height,
        "origin_boundary_zero_oi": detail.filter(pl.col("origin_boundary_open_interest") == 0).height,
        "origin_boundary_positive_oi": detail.filter(pl.col("origin_boundary_open_interest") > 0).height,
        "origin_boundary_missing_oi": detail.filter(pl.col("origin_boundary_open_interest").is_null()).height,
        "successor_boundaries_with_daily_rows": detail.filter(pl.col("successor_daily_rows") > 0).height,
        "successor_daily_rows": successors.height,
        "evidence_window": successor_report["evidence_window"],
        "scope": "retained official general-session observations only; no independent provider calls",
        "missing_source_row_is_not_zero": True,
        "actual_zero_oi_is_not_counterfactual_terminal_proof": True,
        "official_daily_settlement_is_not_a_trade": True,
        "inventory_transfer_or_valuation_admitted": False,
        "financial_or_training_auto_adopted": False,
        "detail_path": str(args.detail),
        "detail_sha256": sha256_file(args.detail),
        "input_pins": pins,
    }
    atomic_write_json(args.report, report)
    print(json.dumps({k: report[k] for k in ("target_boundaries", "origin_boundary_daily_rows",
          "origin_boundary_zero_oi", "origin_boundary_missing_oi",
          "successor_boundaries_with_daily_rows", "successor_daily_rows")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
