#!/usr/bin/env python3
"""Build the receipt-backed 09:00 single-stock-futures entry sidecar.

The input is an archive of official TAIFEX ``Daily_YYYY_MM_DD.zip`` futures
transaction files.  Every date in the requested daily-source interval must
have one ZIP before the manifest is marked complete.  A covered date may
legitimately produce no entry row for a selected contract; a missing ZIP is a
coverage failure and cannot be confused with no trading after 09:00.
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sys
import tempfile
from typing import Final

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip
from stockagent.data.tw_stock_futures_day_trade import (
    TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION,
    _REQUIRED_COLUMNS,
    select_causal_front_stock_futures,
)


DATASET: Final[str] = "taifex_stock_futures_0900_entry_v1"
ARCHIVE_RE: Final[re.Pattern[str]] = re.compile(
    r"^Daily_(\d{4})_(\d{2})_(\d{2})\.zip$"
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.write_parquet(temporary, compression="zstd", statistics=True)
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_write_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        dir=path.parent,
        prefix=path.name + ".",
        suffix=".tmp",
        mode="w",
        encoding="utf-8",
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)
    try:
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _archive_inventory(root: Path) -> dict[date, Path]:
    result: dict[date, Path] = {}
    for path in sorted(root.rglob("Daily_????_??_??.zip")):
        match = ARCHIVE_RE.match(path.name)
        if match is None:
            continue
        trading_date = date(
            int(match.group(1)), int(match.group(2)), int(match.group(3))
        )
        previous = result.setdefault(trading_date, path)
        if previous != path:
            raise ValueError(
                f"duplicate TAIFEX futures ZIPs for {trading_date}: "
                f"{previous}, {path}"
            )
    return result


def _first_strictly_later_entries(
    transactions: pl.DataFrame,
    selected: pl.DataFrame,
) -> pl.DataFrame:
    """Select one deterministic post-09:00 public trade row per contract."""

    selected_keys = selected.select("date", "physical_contract").rename(
        {"date": "trading_date"}
    )
    return (
        transactions.filter(
            (pl.col("session") == "day")
            & (pl.col("event_date") == pl.col("trading_date"))
            & (pl.col("event_time").cast(pl.Int32) > 90000)
            & (pl.col("event_time").cast(pl.Int32) <= 90059)
            & (~pl.col("delivery_month_week").str.contains("/", literal=True))
        )
        .with_columns(
            pl.concat_str(
                [pl.col("product"), pl.lit(":"), pl.col("delivery_month_week")]
            ).alias("physical_contract"),
            pl.col("event_time").cast(pl.Int32).alias("entry_time_hhmmss"),
        )
        .join(
            selected_keys,
            on=["trading_date", "physical_contract"],
            how="inner",
            validate="m:1",
        )
        .sort(
            ["trading_date", "physical_contract", "event_ts", "source_row_number"],
            maintain_order=True,
        )
        .unique(
            subset=["trading_date", "physical_contract"],
            keep="first",
            maintain_order=True,
        )
        .select(
            pl.col("trading_date").alias("date"),
            "physical_contract",
            "entry_time_hhmmss",
            pl.col("price").cast(pl.Float64).alias("entry_price"),
            pl.col("matched_quantity").cast(pl.Float64),
            pl.lit(True).alias("source_row_observed"),
            "source_sha256",
        )
        .rename({"source_sha256": "source_file_sha256"})
        .sort("date", "physical_contract")
    )


def _empty_output() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "date": pl.Date,
            "physical_contract": pl.String,
            "entry_time_hhmmss": pl.Int32,
            "entry_price": pl.Float64,
            "matched_quantity": pl.Float64,
            "source_row_observed": pl.Boolean,
            "source_file_sha256": pl.String,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ticks-root",
        type=Path,
        required=True,
        help="Archive root containing official Daily_YYYY_MM_DD.zip files.",
    )
    parser.add_argument(
        "--daily-data-path",
        type=Path,
        default=Path(
            "data_tw_futures/taifex_portfolio_daily_v4/continuous_daily.parquet"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_tw_futures/taifex_stock_futures_0900_v1"),
    )
    parser.add_argument("--start-date", default="2014-01-01")
    parser.add_argument("--end-date", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = date.fromisoformat(str(args.start_date))
    end = date.fromisoformat(str(args.end_date)) if args.end_date else date.max
    if start > end:
        raise ValueError("start-date must not be after end-date")
    if not args.daily_data_path.is_file():
        raise FileNotFoundError(args.daily_data_path)
    if not args.ticks_root.is_dir():
        raise FileNotFoundError(args.ticks_root)

    source = pl.from_arrow(
        pq.read_table(
            args.daily_data_path,
            columns=list(_REQUIRED_COLUMNS),
            filters=[("asset_class", "=", "stock_future")],
            memory_map=True,
        )
    ).filter(
        (pl.col("date") >= pl.lit(start)) & (pl.col("date") <= pl.lit(end))
    )
    selected = select_causal_front_stock_futures(source)
    if selected.is_empty():
        raise ValueError("daily source has no selected stock futures in range")
    expected_dates = selected["date"].unique().sort().to_list()
    products = tuple(sorted(str(value) for value in selected["product"].unique()))
    archives = _archive_inventory(args.ticks_root)
    missing_dates = [value for value in expected_dates if value not in archives]

    frames: list[pl.DataFrame] = []
    for trading_date in expected_dates:
        archive = archives.get(trading_date)
        if archive is None:
            continue
        source_sha256 = _sha256_file(archive)
        transactions = _parse_zip(
            archive,
            kind="futures",
            trading_date=trading_date,
            source_sha256=source_sha256,
            futures_products=products,
            futures_outright_contracts_only=True,
        )
        selected_date = selected.filter(pl.col("date") == pl.lit(trading_date))
        entries = _first_strictly_later_entries(transactions, selected_date)
        if not entries.is_empty():
            frames.append(entries)

    output = pl.concat(frames, how="vertical_relaxed") if frames else _empty_output()
    output_path = args.output_dir / "entry_0900.parquet"
    _atomic_write_parquet(output, output_path)
    status = "complete" if not missing_dates else "partial"
    manifest: dict[str, object] = {
        "dataset": DATASET,
        "contract_version": int(
            TAIFEX_STOCK_FUTURES_0900_ENTRY_DATA_CONTRACT_VERSION
        ),
        "status": status,
        "timezone": "Asia/Taipei",
        "decision_time": "09:00:00",
        "entry_rule": (
            "first_strictly_later_public_trade_row_through_09:00:59"
        ),
        "same_second_tie_break": (
            "official_source_row_order_because_public_file_has_no_subsecond_clock"
        ),
        "execution_claim": "historical_trade_price_proxy_not_quote_or_depth_fill",
        "coverage": {
            "start": expected_dates[0].isoformat(),
            "end": expected_dates[-1].isoformat(),
            "expected_trading_dates": len(expected_dates),
            "covered_trading_dates": len(expected_dates) - len(missing_dates),
            "missing_trading_dates": [value.isoformat() for value in missing_dates],
        },
        "selection": (
            "causal_nearby_physical_contract_from_taifex_portfolio_daily_v4"
        ),
        "source_daily_path": str(args.daily_data_path),
        "source_daily_sha256": _sha256_file(args.daily_data_path),
        "source_ticks_root": str(args.ticks_root),
        "rows": output.height,
        "outputs": {
            "entry_0900": {
                "path": str(output_path),
                "sha256": _sha256_file(output_path),
            }
        },
    }
    _atomic_write_json(manifest, args.output_dir / "manifest.json")
    print(
        f"[tw-stock-futures-0900] status={status} rows={output.height:,} "
        f"covered={len(expected_dates) - len(missing_dates)}/{len(expected_dates)} "
        f"output={output_path}",
        flush=True,
    )
    return 0 if status == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
