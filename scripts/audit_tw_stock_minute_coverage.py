#!/usr/bin/env python3
"""Audit every regular-board stock/ETF minute from open through close.

One 09:01 right-labelled bar represents trades during 09:00:00-09:00:59.
The 270-point 09:01-13:30 clock includes four normal no-match auction slots
(13:26-13:29); report those separately from 266 potentially traded slots.
An absent trade bar is NOT proof of a downloader failure or a missing fill.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from downloader.download_shioaji_tw_kbars import _load_universe  # noqa: E402


ACTIVE_MINUTES = (*range(1, 266), 270)
FULL_GRID_MINUTES = tuple(range(1, 271))
STRUCTURAL_AUCTION_MINUTES = (266, 267, 268, 269)
PAIR_SCHEMA = {
    "trade_date": pl.Date,
    "symbol": pl.String,
    "market": pl.String,
    "security_type": pl.String,
    "reference_status": pl.String,
    "status": pl.String,
    "full_grid_physical_minutes": pl.UInt16,
    "missing_full_grid_minutes": pl.UInt16,
    "missing_full_grid_minutes_from_open": pl.List(pl.UInt16),
    "full_grid_observed_trade_minutes": pl.UInt16,
    "unobserved_full_grid_minutes": pl.UInt16,
    "closing_auction_interval_trade_minutes": pl.UInt16,
    "physical_active_minutes": pl.UInt16,
    "missing_physical_minutes": pl.UInt16,
    "missing_physical_minutes_from_open": pl.List(pl.UInt16),
    "observed_active_minutes": pl.UInt16,
    "missing_active_minutes": pl.UInt16,
    "missing_minutes_from_open": pl.List(pl.UInt16),
    "zero_volume_minute_rows": pl.UInt16,
    "first_observed_minute": pl.UInt16,
    "last_observed_minute": pl.UInt16,
    "duplicate_active_minute_slots": pl.UInt16,
    "invalid_rows": pl.UInt16,
}


def _taipei_today() -> date:
    return datetime.now(ZoneInfo("Asia/Taipei")).date()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _minute_clock(offset: int) -> str:
    minute_of_day = 9 * 60 + offset
    return f"{minute_of_day // 60:02d}:{minute_of_day % 60:02d}"


def _expected_by_date(stock_root: Path, start: date, end: date):
    universe = _load_universe(stock_root)
    rows_by_path = {str(row.base_path): row for row in universe}
    paths = list(rows_by_path)
    observed = (
        pl.scan_parquet(paths, include_file_paths="_source_path")
        .filter(
            pl.col("date").is_between(start, end)
            & pl.col("Trading_Volume").is_finite()
            & (pl.col("Trading_Volume") > 0)
        )
        .select("_source_path", "date")
        .collect(engine="streaming")
    )
    expected = defaultdict(dict)
    for path, session in observed.iter_rows():
        row = rows_by_path[path]
        expected[session][row.symbol] = row
    return universe, expected


def _verified_partition(partition: Path, session: date) -> str | None:
    summary_path = partition.with_name("summary.json")
    if not partition.is_file() or not summary_path.is_file():
        return "missing_partition_or_summary"
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        if (
            summary.get("status") != "ok"
            or summary.get("trade_date") != session.isoformat()
            or summary.get("output_sha256") != _sha256(partition)
        ):
            return "partition_receipt_mismatch"
    except (OSError, ValueError, json.JSONDecodeError):
        return "partition_receipt_unreadable"
    return None


def _partition_minutes(partition: Path, session: date):
    required = {
        "symbol", "date", "ts", "minutes_from_open", "Open", "High",
        "Low", "Close", "Volume", "Amount",
    }
    schema = pl.read_parquet_schema(partition)
    if not required.issubset(schema):
        raise ValueError(f"missing minute columns: {sorted(required - set(schema))}")
    frame = pl.read_parquet(partition, columns=sorted(required))
    offset = (
        pl.col("ts").dt.hour().cast(pl.Int32) * 60
        + pl.col("ts").dt.minute().cast(pl.Int32)
        - 9 * 60
    )
    aligned = (
        (pl.col("date") == session)
        & (pl.col("minutes_from_open") == offset)
        & pl.col("ts").dt.second().eq(0)
        & pl.col("ts").dt.nanosecond().eq(0)
    )
    valid_values = (
        pl.all_horizontal(
            *[
                pl.col(name).is_finite() & (pl.col(name) > 0)
                for name in ("Open", "High", "Low", "Close")
            ]
        )
        & (pl.col("High") >= pl.max_horizontal("Open", "Low", "Close"))
        & (pl.col("Low") <= pl.min_horizontal("Open", "High", "Close"))
        & pl.col("Volume").is_finite()
        & (pl.col("Volume") >= 0)
        & pl.col("Amount").is_finite()
        & (pl.col("Amount") >= 0)
    )
    active = pl.col("minutes_from_open").is_in(ACTIVE_MINUTES)
    full_grid = pl.col("minutes_from_open").is_in(FULL_GRID_MINUTES)
    structural = pl.col("minutes_from_open").is_in(STRUCTURAL_AUCTION_MINUTES)
    checked = frame.with_columns(
        (aligned & full_grid).fill_null(False).alias("_physical"),
        (aligned & full_grid & valid_values & (pl.col("Volume") > 0)).fill_null(False).alias("_trade"),
        (aligned & active & valid_values & (pl.col("Volume") == 0)).fill_null(False).alias("_zero_volume"),
        (aligned & structural).fill_null(False).alias("_structural"),
        (~(aligned & valid_values) & ~structural).fill_null(False).alias("_invalid"),
    )
    invalid_counts = Counter(checked.filter(pl.col("_invalid"))["symbol"].to_list())
    zero_counts = Counter(checked.filter(pl.col("_zero_volume"))["symbol"].to_list())
    present = checked.filter(pl.col("_physical")).select("symbol", "minutes_from_open")
    duplicates = Counter()
    for row in (present.group_by("symbol", "minutes_from_open").len()
                .filter(pl.col("len") > 1).iter_rows(named=True)):
        duplicates[str(row["symbol"])] += int(row["len"] - 1)
    physical = defaultdict(set)
    for symbol, minute in present.unique().iter_rows():
        physical[str(symbol)].add(int(minute))
    observed = defaultdict(set)
    for symbol, minute in (checked.filter(pl.col("_trade"))
                           .select("symbol", "minutes_from_open").unique().iter_rows()):
        observed[str(symbol)].add(int(minute))
    return (
        physical, observed, zero_counts, invalid_counts, duplicates,
        int(checked["_structural"].sum()), int(checked["_invalid"].sum()),
    )


def audit(stock_root: Path, minute_root: Path, start: date, end: date, output_dir: Path) -> dict:
    if start > end or end >= _taipei_today():
        raise ValueError("select completed sessions only, with start <= end")
    if output_dir.exists():
        raise FileExistsError(output_dir)
    universe, expected = _expected_by_date(stock_root, start, end)
    universe_by_symbol = {row.symbol: row for row in universe}
    if not expected:
        raise RuntimeError("official positive-volume reference has no sessions")
    import pyarrow.parquet as pq

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=output_dir.parent))
    writer = None
    totals = Counter()
    clock_missing = Counter()
    clock_physical_missing = Counter()
    clock_full_grid_missing = Counter()
    symbol_missing = Counter()
    source_errors = {}
    by_date = {}
    try:
        for session, rows in sorted(expected.items()):
            day = session.isoformat()
            partition = minute_root / f"trade_date={day}" / "data.parquet"
            error = _verified_partition(partition, session)
            physical, observed = defaultdict(set), defaultdict(set)
            zero, invalid, duplicates = Counter(), Counter(), Counter()
            structural_rows = invalid_rows = 0
            if error is None:
                try:
                    (physical, observed, zero, invalid, duplicates,
                     structural_rows, invalid_rows) = _partition_minutes(partition, session)
                except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
                    error = f"partition_scan_failed:{type(exc).__name__}:{exc}"
            if error:
                source_errors[day] = error
            pair_rows = []
            day_counts = Counter()
            minute_only_symbols = sorted(set(physical) - set(rows)) if not error else []
            all_symbols = sorted(set(rows) | set(minute_only_symbols))
            for symbol in all_symbols:
                meta = rows.get(symbol) or universe_by_symbol.get(symbol)
                reference_status = (
                    "official_positive_volume" if symbol in rows
                    else "minute_only_unverified_daily_reference"
                )
                full_physical_have = physical.get(symbol, set()) if not error else set()
                full_trade_have = observed.get(symbol, set()) if not error else set()
                full_missing = [minute for minute in FULL_GRID_MINUTES if minute not in full_physical_have] if not error else []
                full_unobserved = [minute for minute in FULL_GRID_MINUTES if minute not in full_trade_have] if not error else []
                physical_have = full_physical_have.intersection(ACTIVE_MINUTES)
                have = full_trade_have.intersection(ACTIVE_MINUTES)
                physical_missing = [minute for minute in ACTIVE_MINUTES if minute not in physical_have] if not error else []
                missing = [minute for minute in ACTIVE_MINUTES if minute not in have] if not error else []
                if error:
                    status = "source_unassessable"
                    day_counts["unassessable_symbol_days"] += 1
                else:
                    status = "assessed"
                    day_counts["full_grid_physical_minutes"] += len(full_physical_have)
                    day_counts["missing_full_grid_minutes"] += len(full_missing)
                    day_counts["full_grid_observed_trade_minutes"] += len(full_trade_have)
                    day_counts["unobserved_full_grid_minutes"] += len(full_unobserved)
                    day_counts["closing_auction_interval_trade_minutes"] += len(full_trade_have.intersection(STRUCTURAL_AUCTION_MINUTES))
                    day_counts["physical_active_minutes"] += len(physical_have)
                    day_counts["missing_physical_minutes"] += len(physical_missing)
                    day_counts["zero_volume_minute_rows"] += zero.get(symbol, 0)
                    day_counts["observed_active_minutes"] += len(have)
                    day_counts["unobserved_active_minutes"] += len(missing)
                    day_counts["symbol_days_missing_any_active_minute"] += bool(missing)
                    day_counts["symbol_days_with_no_active_bar"] += not have
                    day_counts["symbol_days_missing_open_0901"] += 1 not in have
                    day_counts["symbol_days_missing_close_1330"] += 270 not in have
                    clock_missing.update(missing)
                    clock_physical_missing.update(physical_missing)
                    clock_full_grid_missing.update(full_missing)
                    symbol_missing[symbol] += len(missing)
                pair_rows.append({
                    "trade_date": session,
                    "symbol": symbol,
                    "market": meta.market if meta is not None else "unknown",
                    "security_type": meta.security_type if meta is not None else "unknown",
                    "reference_status": reference_status,
                    "status": status,
                    "full_grid_physical_minutes": len(full_physical_have) if not error else None,
                    "missing_full_grid_minutes": len(full_missing) if not error else None,
                    "missing_full_grid_minutes_from_open": full_missing if not error else None,
                    "full_grid_observed_trade_minutes": len(full_trade_have) if not error else None,
                    "unobserved_full_grid_minutes": len(full_unobserved) if not error else None,
                    "closing_auction_interval_trade_minutes": len(full_trade_have.intersection(STRUCTURAL_AUCTION_MINUTES)) if not error else None,
                    "physical_active_minutes": len(physical_have) if not error else None,
                    "missing_physical_minutes": len(physical_missing) if not error else None,
                    "missing_physical_minutes_from_open": physical_missing if not error else None,
                    "observed_active_minutes": len(have) if not error else None,
                    "missing_active_minutes": len(missing) if not error else None,
                    "missing_minutes_from_open": missing if not error else None,
                    "zero_volume_minute_rows": zero.get(symbol, 0) if not error else None,
                    "first_observed_minute": min(have) if have else None,
                    "last_observed_minute": max(have) if have else None,
                    "duplicate_active_minute_slots": duplicates.get(symbol, 0) if not error else None,
                    "invalid_rows": invalid.get(symbol, 0) if not error else None,
                })
            frame = pl.DataFrame(pair_rows, schema=PAIR_SCHEMA)
            table = frame.to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(temporary / "pair_coverage.parquet", table.schema, compression="zstd")
            writer.write_table(table)
            day_counts["official_positive_volume_symbol_days"] = len(rows)
            day_counts["minute_only_unverified_symbol_days"] = len(minute_only_symbols)
            day_counts["assessed_or_unassessable_symbol_days"] = len(all_symbols)
            day_counts["structural_auction_slots_1326_to_1329"] = 4 * len(all_symbols)
            day_counts["structural_auction_rows_present"] = structural_rows
            day_counts["invalid_source_rows"] = invalid_rows
            by_date[day] = dict(day_counts)
            totals.update(day_counts)
            print(
                f"[minute-coverage] {day} symbols={len(rows)} "
                f"missing_grid={day_counts['missing_full_grid_minutes']} "
                f"unobserved_trade={day_counts['unobserved_active_minutes']} "
                f"status={'source_error' if error else 'assessed'}",
                flush=True,
            )
        if writer is not None:
            writer.close()
            writer = None
        report = {
            "schema_version": 1,
            "claim": "observed_trade_bar_coverage_not_proof_of_missing_source_or_fills",
            "created_at": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds"),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "stock_root": str(stock_root.resolve()),
            "minute_root": str(minute_root.resolve()),
            "universe_symbols_in_current_manifest": len(universe),
            "denominator_contract": "official_positive_volume_union_minute_partition_symbols_v1",
            "right_labelled_grid_slots_per_symbol_day": 270,
            "potentially_traded_slots_per_symbol_day": len(ACTIVE_MINUTES),
            "structural_no_match_clock_labels": [_minute_clock(x) for x in STRUCTURAL_AUCTION_MINUTES],
            "totals": dict(totals),
            "source_errors": source_errors,
            "by_date": by_date,
            "missing_full_grid_by_clock": {_minute_clock(x): clock_full_grid_missing[x] for x in FULL_GRID_MINUTES},
            "missing_physical_by_clock": {_minute_clock(x): clock_physical_missing[x] for x in ACTIVE_MINUTES},
            "unobserved_by_clock": {_minute_clock(x): clock_missing[x] for x in ACTIVE_MINUTES},
            "top_20_symbols_by_unobserved_minutes": symbol_missing.most_common(20),
            "pair_coverage_file": "pair_coverage.parquet",
            "pair_coverage_sha256": _sha256(temporary / "pair_coverage.parquet"),
        }
        (temporary / "summary.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output_dir)
        return report
    finally:
        if writer is not None:
            writer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stock-root", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--minute-root", type=Path, default=Path("data_tw_minute/research_dataset"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 2, 25))
    parser.add_argument("--end-date", type=date.fromisoformat, default=_taipei_today() - timedelta(days=1))
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.stock_root, args.minute_root, args.start_date, args.end_date, args.output_dir)
    print(f"{args.output_dir}: {report['totals']} source_errors={len(report['source_errors'])}")


if __name__ == "__main__":
    main()
