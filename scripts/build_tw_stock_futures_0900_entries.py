#!/usr/bin/env python3
"""Build receipt-backed single-stock-futures 09:00 entries or 08:46 minute tape.

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
    select_causal_front_stock_futures_candidates,
)
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from stockagent.data.tw_stock_futures_minute import (
    MINUTE_CONTRACT_VERSION, MINUTE_DATASET, build_futures_minute_bars,
)
from stockagent.data.tw_futures_portfolio_daily import TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION


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
    parser.add_argument("--execution-policy", choices=("post_0900", "scheduled_0846"), default="post_0900")
    parser.add_argument("--archive-override", type=Path, action="append", default=[],
                        help="Explicit immutable corrected Daily_YYYY_MM_DD.zip; original source is preserved.")
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

    daily_digest = sha256_file(args.daily_data_path)
    if args.execution_policy == "scheduled_0846":
        daily_manifest = json.loads(args.daily_data_path.with_name("manifest.json").read_text())
        if (daily_manifest.get("contract_version") != TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
                or daily_manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256") != daily_digest):
            raise ValueError("daily candidate source contract or SHA mismatch")
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
    minute_policy = args.execution_policy == "scheduled_0846"
    if minute_policy and args.output_dir == Path("data_tw_futures/taifex_stock_futures_0900_v1"):
        args.output_dir = Path("data_tw_futures/taifex_stock_futures_minute_v1")
    selected = (select_causal_front_stock_futures_candidates(source) if minute_policy
                else select_causal_front_stock_futures(source))
    if selected.is_empty():
        raise ValueError("daily source has no selected stock futures in range")
    expected_dates = source["date"].unique().sort().to_list()
    products = tuple(sorted(str(value) for value in selected["product"].unique()))
    archives = _archive_inventory(args.ticks_root)
    override_dates = set()
    for archive in args.archive_override:
        match = ARCHIVE_RE.fullmatch(archive.name)
        if not archive.is_file() or match is None:
            raise ValueError(f"invalid explicit archive revision: {archive}")
        revised_date = date(*(int(match.group(i)) for i in (1, 2, 3)))
        if revised_date not in expected_dates or revised_date in override_dates:
            raise ValueError(f"duplicate or out-of-range archive revision: {archive}")
        override_dates.add(revised_date)
        archives[revised_date] = archive
    missing_dates = [value for value in expected_dates if value not in archives]

    frames: list[pl.DataFrame] = []
    source_receipts: list[dict] = []
    incomplete_sessions: list[str] = []
    for index, trading_date in enumerate(expected_dates, 1):
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
        entries = (build_futures_minute_bars(transactions) if minute_policy
                   else _first_strictly_later_entries(transactions, selected_date))
        if sha256_file(archive) != source_sha256:
            raise ValueError(f"source archive changed while parsing: {archive}")
        day_rows = transactions.filter(
            (pl.col("session") == "day") & (pl.col("event_date") == pl.lit(trading_date))
        )
        last_time = int(day_rows["event_time"].cast(pl.Int32).max() or 0)
        if minute_policy and (day_rows.is_empty() or last_time < 133000):
            incomplete_sessions.append(str(trading_date))
        source_receipts.append({"date": str(trading_date), "path": str(archive), "sha256": source_sha256,
                                "day_session_rows": day_rows.height, "day_last_time": last_time})
        if minute_policy or not entries.is_empty():
            frames.append(entries)
        if minute_policy:
            print(f"[futures minutes] {index}/{len(expected_dates)} {trading_date} rows={entries.height}", flush=True)

    if minute_policy:
        if sha256_file(args.daily_data_path) != daily_digest:
            raise ValueError("daily candidate source changed during minute build")
        output_path = args.output_dir / "minutes.parquet"
        # An incomplete attempt must not replace an accepted data/manifest pair.
        if missing_dates or incomplete_sessions:
            atomic_write_json(args.output_dir / "build_failure.json", {
                "status": "partial", "missing_dates": list(map(str, missing_dates)),
                "incomplete_sessions": incomplete_sessions,
                "requested_start": str(start), "requested_end": str(end),
            })
            print(f"[futures minutes] rejected missing_dates={list(map(str, missing_dates))} incomplete_sessions={incomplete_sessions}", flush=True)
            return 2
        output = pl.concat(frames, how="vertical_relaxed")
        atomic_write_parquet(output_path, output)
        atomic_write_json(args.output_dir / "manifest.json", {
            "dataset": MINUTE_DATASET, "contract_version": MINUTE_CONTRACT_VERSION,
            "status": "complete", "timezone": "Asia/Taipei",
            "source_daily_path": str(args.daily_data_path), "source_daily_sha256": daily_digest,
            "covered_dates": [str(d) for d in expected_dates], "sources": source_receipts,
            "clock": "right_labelled_minutes_[label-1min,label)",
            "quantity": "official_matched_contracts_B_plus_S_divided_by_two",
            "execution_claim": "historical_minute_vwap_not_order_book_or_guaranteed_fill",
            "rows": output.height,
            "outputs": {"minutes": {"path": str(output_path), "sha256": sha256_file(output_path)}},
        })
        print(f"[futures minutes] complete dates={len(expected_dates)} rows={output.height} output={output_path}", flush=True)
        return 0

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
