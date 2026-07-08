from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import polars as pl
from tqdm import tqdm

from downloader.download_yahoo_ohlcv import (
    _merge_feature_frames,
    _read_parquet_frame,
    _write_feature_parquet_atomic,
)
from stockagent.data.tw_public_features import _date_column_expr, _num_expr, _symbol_expr


@dataclass(slots=True)
class BackfillResult:
    symbol: str
    status: str
    rows_added_or_replaced: int
    first_fresh_date: str | None
    last_fresh_date: str | None
    output_path: str | None
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill data_yahoo/tw_stocks OHLCV parquet files from official TWSE/TPEx daily OHLCV parquet."
    )
    parser.add_argument("--input-dir", default="data_tw_public", help="Directory containing twse_daily_ohlcv.parquet and tpex_daily_ohlcv.parquet.")
    parser.add_argument("--symbols-root", default="data_yahoo/tw_stocks", help="Yahoo-layout TW stock parquet directory to update.")
    parser.add_argument("--start-date", default=None, help="Inclusive YYYY-MM-DD. Default: one day after current newest Yahoo TW date.")
    parser.add_argument("--end-date", default=None, help="Inclusive YYYY-MM-DD. Default: latest official date.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel symbol file writers.")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; do not write parquet files.")
    parser.add_argument("--summary-path", default=None, help="Optional summary JSON path.")
    parser.add_argument("--report-path", default=None, help="Optional report CSV path.")
    return parser.parse_args()


def _existing_symbols(root: Path) -> set[str]:
    return {path.name.removesuffix("_features.parquet").upper() for path in root.glob("*_features.parquet")}


def _max_existing_date(root: Path, *, sample_symbols: int = 128) -> date | None:
    newest: date | None = None
    paths = sorted(root.glob("*_features.parquet"))[: max(1, int(sample_symbols))]
    for path in paths:
        try:
            frame = pl.read_parquet(path, columns=["date"])
        except Exception:
            continue
        if frame.is_empty():
            continue
        value = frame.select(pl.col("date").max().alias("max_date")).item()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date) and (newest is None or value > newest):
            newest = value
    return newest


def _parse_date_arg(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def _public_ohlcv_frame(input_dir: Path, symbols: set[str], start: date | None, end: date | None) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    twse_path = input_dir / "twse_daily_ohlcv.parquet"
    if twse_path.exists():
        twse = pl.scan_parquet(twse_path)
        frames.append(
            twse.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("證券代號").alias("symbol"),
                    _num_expr("開盤價").alias("open"),
                    _num_expr("最高價").alias("max"),
                    _num_expr("最低價").alias("min"),
                    _num_expr("收盤價").alias("close"),
                    _num_expr("收盤價").alias("adjclose"),
                    _num_expr("成交股數").alias("Trading_Volume"),
                ]
            )
        )
    tpex_path = input_dir / "tpex_daily_ohlcv.parquet"
    if tpex_path.exists():
        tpex = pl.scan_parquet(tpex_path)
        frames.append(
            tpex.select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("symbol"),
                    _num_expr("開盤").alias("open"),
                    _num_expr("最高").alias("max"),
                    _num_expr("最低").alias("min"),
                    _num_expr("收盤").alias("close"),
                    _num_expr("收盤").alias("adjclose"),
                    _num_expr("成交股數").alias("Trading_Volume"),
                ]
            )
        )
    if not frames:
        return pl.DataFrame()
    lazy = pl.concat(frames, how="vertical_relaxed").drop_nulls(["date", "symbol", "close"])
    if symbols:
        lazy = lazy.filter(pl.col("symbol").is_in(sorted(symbols)))
    if start is not None:
        lazy = lazy.filter(pl.col("date") >= start)
    if end is not None:
        lazy = lazy.filter(pl.col("date") <= end)
    return (
        lazy.sort(["symbol", "date"])
        .unique(subset=["symbol", "date"], keep="last", maintain_order=True)
        .collect()
    )


def _official_symbol_name_frame(input_dir: Path) -> pl.DataFrame:
    frames: list[pl.LazyFrame] = []
    twse_path = input_dir / "twse_daily_ohlcv.parquet"
    if twse_path.exists():
        frames.append(
            pl.scan_parquet(twse_path).select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("證券代號").alias("code"),
                    pl.col("證券名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
                ]
            )
        )
    tpex_path = input_dir / "tpex_daily_ohlcv.parquet"
    if tpex_path.exists():
        frames.append(
            pl.scan_parquet(tpex_path).select(
                [
                    _date_column_expr("date").alias("date"),
                    _symbol_expr("代號").alias("code"),
                    pl.col("名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
                ]
            )
        )
    if not frames:
        return pl.DataFrame()
    return (
        pl.concat(frames, how="vertical_relaxed")
        .drop_nulls(["code", "name"])
        .filter((pl.col("code") != "") & (pl.col("name") != ""))
        .sort(["code", "date"])
        .unique(subset=["code"], keep="last", maintain_order=True)
        .select(["code", "name"])
        .collect()
    )


def _update_symbols_csv_names(input_dir: Path, root: Path, symbols: set[str], *, dry_run: bool) -> int:
    symbols_path = root / "symbols.csv"
    if not symbols_path.exists():
        return 0
    names = _official_symbol_name_frame(input_dir)
    if names.is_empty():
        return 0
    if symbols:
        names = names.filter(pl.col("code").is_in(sorted(symbols)))
    if names.is_empty():
        return 0
    symbols_frame = pl.read_csv(symbols_path, infer_schema_length=10000)
    if "code" not in symbols_frame.columns or "name" not in symbols_frame.columns:
        return 0
    joined = symbols_frame.join(names.rename({"name": "_official_name"}), on="code", how="left")
    changed = int(
        joined.select(
            ((pl.col("_official_name").is_not_null()) & (pl.col("_official_name") != "") & (pl.col("_official_name") != pl.col("name"))).sum()
        ).item()
    )
    updated = joined.with_columns(
        pl.when(pl.col("_official_name").is_not_null() & (pl.col("_official_name") != ""))
        .then(pl.col("_official_name"))
        .otherwise(pl.col("name"))
        .alias("name")
    )
    updated = updated.drop("_official_name")
    if changed and not dry_run:
        tmp_path = symbols_path.with_suffix(symbols_path.suffix + ".tmp")
        updated.write_csv(tmp_path)
        tmp_path.replace(symbols_path)
    return changed


def _official_date_bounds(input_dir: Path) -> tuple[date | None, date | None]:
    frames = []
    for name in ("twse_daily_ohlcv", "tpex_daily_ohlcv"):
        path = input_dir / f"{name}.parquet"
        if path.exists():
            frames.append(pl.scan_parquet(path).select(_date_column_expr("date").alias("date")))
    if not frames:
        return None, None
    bounds = (
        pl.concat(frames, how="vertical_relaxed")
        .drop_nulls("date")
        .select(pl.col("date").min().alias("min_date"), pl.col("date").max().alias("max_date"))
        .collect()
        .row(0, named=True)
    )
    return bounds.get("min_date"), bounds.get("max_date")


def _write_noop_summary(args: argparse.Namespace, root: Path, start: date, official_latest: date | None) -> None:
    summary = {
        "input_dir": str(args.input_dir),
        "symbols_root": str(root),
        "start_date": str(start),
        "end_date": str(official_latest) if official_latest else None,
        "source_rows": 0,
        "source_symbols": 0,
        "status_counts": {"noop_already_current": 1},
        "elapsed_seconds": 0.0,
        "dry_run": bool(args.dry_run),
    }
    summary_path = Path(args.summary_path or root / "tw_public_backfill_summary.json")
    report_path = Path(args.report_path or root / "tw_public_backfill_report.csv")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pl.DataFrame(
        [
            {
                "symbol": "",
                "status": "noop_already_current",
                "rows_added_or_replaced": 0,
                "first_fresh_date": None,
                "last_fresh_date": str(official_latest) if official_latest else None,
                "output_path": None,
                "message": "local Yahoo-layout TW parquet files are already current with official TWSE/TPEx OHLCV",
            }
        ]
    ).write_csv(report_path)
    print(
        f"[tw-public-backfill] noop already current start={start} official_latest={official_latest} "
        f"summary={summary_path} report={report_path}",
        flush=True,
    )


def _write_symbol(root: Path, symbol: str, fresh: pl.DataFrame, *, requested_end_date: str, dry_run: bool) -> BackfillResult:
    output_path = root / f"{symbol}_features.parquet"
    try:
        if not output_path.exists():
            return BackfillResult(symbol, "missing_existing_file", 0, None, None, str(output_path))
        first = fresh.select(pl.col("date").min()).item()
        last = fresh.select(pl.col("date").max()).item()
        existing = _read_parquet_frame(output_path)
        merged = _merge_feature_frames(existing, fresh, keep_zero_volume=True)
        if dry_run:
            return BackfillResult(symbol, "planned", int(fresh.height), str(first), str(last), str(output_path))
        _write_feature_parquet_atomic(merged, output_path, asset_class="tw_stocks", requested_end_date=requested_end_date)
        return BackfillResult(symbol, "updated", int(fresh.height), str(first), str(last), str(output_path))
    except Exception as exc:
        return BackfillResult(symbol, "failed", 0, None, None, str(output_path), message=str(exc))


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    root = Path(args.symbols_root)
    symbols = _existing_symbols(root)
    if not symbols:
        raise SystemExit(f"No existing Yahoo-layout TW parquet files found under {root}")
    names_updated = _update_symbols_csv_names(input_dir, root, symbols, dry_run=bool(args.dry_run))
    if names_updated:
        print(f"[tw-public-backfill] updated symbols.csv names={names_updated}", flush=True)
    start = _parse_date_arg(args.start_date)
    if start is None:
        newest = _max_existing_date(root)
        if newest is None:
            raise SystemExit(f"Could not infer newest existing date under {root}; pass --start-date")
        start = newest.fromordinal(newest.toordinal() + 1)
    end = _parse_date_arg(args.end_date)

    started = time.perf_counter()
    fresh = _public_ohlcv_frame(input_dir, symbols, start, end)
    if fresh.is_empty():
        _, official_latest = _official_date_bounds(input_dir)
        if official_latest is not None and start > official_latest:
            _write_noop_summary(args, root, start, official_latest)
            return
        raise SystemExit(f"No official TW OHLCV rows matched symbols={len(symbols)} start={start} end={end or 'latest'}")
    latest = fresh.select(pl.col("date").max()).item()
    earliest = fresh.select(pl.col("date").min()).item()
    latest_text = str(latest)
    print(
        f"[tw-public-backfill] source_rows={fresh.height} symbols={fresh.select(pl.col('symbol').n_unique()).item()} "
        f"range={earliest}..{latest} target={root}",
        flush=True,
    )

    groups = fresh.partition_by("symbol", as_dict=True, maintain_order=False)
    results: list[BackfillResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(_write_symbol, root, str(key[0] if isinstance(key, tuple) else key), frame, requested_end_date=latest_text, dry_run=bool(args.dry_run)): key
            for key, frame in groups.items()
        }
        progress = tqdm(total=len(futures), desc="tw-public->yahoo", unit="symbol")
        try:
            for future in as_completed(futures):
                results.append(future.result())
                progress.update(1)
        finally:
            progress.close()

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    elapsed = time.perf_counter() - started
    summary = {
        "input_dir": str(input_dir),
        "symbols_root": str(root),
        "start_date": str(start),
        "end_date": latest_text,
        "source_rows": int(fresh.height),
        "source_symbols": int(fresh.select(pl.col("symbol").n_unique()).item()),
        "status_counts": counts,
        "elapsed_seconds": elapsed,
        "dry_run": bool(args.dry_run),
    }
    summary_path = Path(args.summary_path or root / "tw_public_backfill_summary.json")
    report_path = Path(args.report_path or root / "tw_public_backfill_report.csv")
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    pl.DataFrame([asdict(result) for result in sorted(results, key=lambda item: item.symbol)]).write_csv(report_path)
    print(f"[tw-public-backfill] done counts={counts} elapsed={elapsed:.1f}s summary={summary_path} report={report_path}", flush=True)
    if counts.get("failed", 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
