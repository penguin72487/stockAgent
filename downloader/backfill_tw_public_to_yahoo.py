from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import polars as pl
from tqdm import tqdm

from downloader.download_yahoo_ohlcv import (
    _canonicalize_feature_frame,
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
    parser.add_argument(
        "--create-missing-delisted",
        action="store_true",
        help="Create canonical symbol files for official delisted companies missing from Yahoo output.",
    )
    parser.add_argument(
        "--normalize-delisted-archives",
        action="store_true",
        help="Merge successful *_TW/*_TWO Yahoo archive candidates into canonical symbol files.",
    )
    parser.add_argument("--summary-path", default=None, help="Optional summary JSON path.")
    parser.add_argument("--report-path", default=None, help="Optional report CSV path.")
    return parser.parse_args()


def _existing_symbols(root: Path) -> set[str]:
    return {path.name.removesuffix("_features.parquet").upper() for path in root.glob("*_features.parquet")}


_DELISTED_ARCHIVE_FILE_RE = re.compile(
    r"^(?P<symbol>[0-9A-Z]{4,6})_(?:TW|TWO)_features\.parquet$",
    re.IGNORECASE,
)


def _normalize_delisted_archive_files(root: Path, *, dry_run: bool) -> tuple[int, int]:
    grouped: dict[str, list[Path]] = {}
    for path in root.glob("*_features.parquet"):
        match = _DELISTED_ARCHIVE_FILE_RE.fullmatch(path.name)
        if match is not None:
            grouped.setdefault(match.group("symbol").upper(), []).append(path)
    normalized = 0
    removed = 0
    for symbol, archive_paths in sorted(grouped.items()):
        canonical_path = root / f"{symbol}_features.parquet"
        source_paths = [*sorted(archive_paths), *([canonical_path] if canonical_path.exists() else [])]
        merged = None
        for source_path in source_paths:
            frame = _read_parquet_frame(source_path)
            merged = frame if merged is None else _merge_feature_frames(
                merged,
                frame,
                keep_zero_volume=True,
            )
        if merged is None or merged.is_empty():
            continue
        normalized += 1
        removed += len(archive_paths)
        if dry_run:
            continue
        latest = merged.select(pl.col("date").max()).item()
        _write_feature_parquet_atomic(
            merged,
            canonical_path,
            asset_class="tw_stocks",
            requested_end_date=str(latest)[:10],
        )
        for archive_path in archive_paths:
            archive_path.unlink()
    return normalized, removed


def _official_delisted_symbols(input_dir: Path) -> set[str]:
    symbols: set[str] = set()
    for market in ("twse", "tpex"):
        path = input_dir / f"{market}_delisted_company.parquet"
        if not path.exists() or "symbol" not in pl.read_parquet_schema(path):
            continue
        values = pl.read_parquet(path, columns=["symbol"])["symbol"].cast(pl.Utf8, strict=False)
        symbols.update(
            value.strip().upper()
            for value in values.drop_nulls().to_list()
            if re.fullmatch(r"[0-9A-Z]{4,6}", str(value).strip().upper())
        )
    return symbols


def _merge_official_feature_frame(existing: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    official_columns = ("open", "max", "min", "close", "Trading_Volume")
    renamed = fresh.rename(
        {column: f"_official_{column}" for column in official_columns if column in fresh.columns}
    ).drop("adjclose", strict=False)
    merged = existing.join(renamed, on="date", how="full", coalesce=True)
    expressions: list[pl.Expr] = []
    for column in official_columns:
        official = f"_official_{column}"
        if official not in merged.columns:
            continue
        if column in merged.columns:
            expressions.append(pl.coalesce(official, column).alias(column))
        else:
            expressions.append(pl.col(official).alias(column))
    if "adjclose" not in merged.columns:
        expressions.append(pl.col("_official_close").alias("adjclose"))
    else:
        expressions.append(
            pl.coalesce("adjclose", "_official_close").alias("adjclose")
        )
    merged = merged.with_columns(expressions).drop(
        [column for column in merged.columns if column.startswith("_official_")]
    )
    return _canonicalize_feature_frame(merged, keep_zero_volume=True)


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


def _resolve_start_date(
    input_dir: Path,
    root: Path,
    requested_start: str | None,
    *,
    create_missing_delisted: bool,
) -> date:
    explicit = _parse_date_arg(requested_start)
    if explicit is not None:
        return explicit
    if create_missing_delisted:
        official_earliest, _ = _official_date_bounds(input_dir)
        if official_earliest is None:
            raise RuntimeError(
                "Could not infer the official OHLCV start date needed to create missing delisted symbols"
            )
        return official_earliest
    newest = _max_existing_date(root)
    if newest is None:
        raise RuntimeError(f"Could not infer newest existing date under {root}; pass --start-date")
    return newest.fromordinal(newest.toordinal() + 1)


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


def _upsert_created_symbols_manifest(
    input_dir: Path,
    root: Path,
    created_symbols: set[str],
    *,
    dry_run: bool,
) -> int:
    if not created_symbols:
        return 0
    candidates: list[pl.DataFrame] = []
    for market, yahoo_suffix in (("twse", ".TW"), ("tpex", ".TWO")):
        path = input_dir / f"{market}_delisted_company.parquet"
        if not path.exists():
            continue
        schema = set(pl.read_parquet_schema(path))
        if not {"symbol", "date"} <= schema:
            continue
        name_expr = (
            pl.col("company_name").cast(pl.Utf8, strict=False)
            if "company_name" in schema
            else pl.col("symbol").cast(pl.Utf8, strict=False)
        )
        candidates.append(
            pl.read_parquet(path)
            .select(
                _date_column_expr("date").alias("date"),
                pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars().alias("code"),
                name_expr.str.strip_chars().alias("name"),
            )
            .filter(pl.col("code").is_in(sorted(created_symbols)))
            .with_columns(
                pl.lit("tw_delisted").alias("market"),
                (pl.col("code") + pl.lit(yahoo_suffix)).alias("yahoo_symbol"),
            )
        )
    if not candidates:
        return 0
    additions = (
        pl.concat(candidates, how="vertical_relaxed")
        .sort(["code", "date"])
        .unique("code", keep="last", maintain_order=True)
        .select(["code", "name", "market", "yahoo_symbol"])
    )
    symbols_path = root / "symbols.csv"
    if symbols_path.exists():
        existing = pl.read_csv(symbols_path, infer_schema_length=None).select(
            ["code", "name", "market", "yahoo_symbol"]
        )
        new_rows = additions.filter(~pl.col("code").is_in(existing["code"]))
        combined = pl.concat([existing, new_rows], how="vertical_relaxed")
    else:
        new_rows = additions
        combined = additions
    added = int(new_rows.height)
    if added and not dry_run:
        temporary = symbols_path.with_suffix(symbols_path.suffix + ".tmp")
        combined.sort("code").write_csv(temporary)
        temporary.replace(symbols_path)
    return added


def _update_return_price_provenance(
    root: Path,
    symbols: set[str],
    *,
    kind: str,
    source: str,
    dry_run: bool,
) -> int:
    if not symbols:
        return 0
    path = root / "return_price_provenance.json"
    payload: dict[str, Any] = {"schema_version": 1, "symbols": {}}
    if path.exists():
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict) or not isinstance(loaded.get("symbols", {}), dict):
            raise ValueError(f"invalid return-price provenance manifest: {path}")
        payload = loaded
        payload.setdefault("schema_version", 1)
        payload.setdefault("symbols", {})
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for symbol in sorted(symbols):
        payload["symbols"][symbol] = {
            "kind": str(kind),
            "source": str(source),
            "updated_at_utc": updated_at,
        }
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    return len(symbols)


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


def _write_symbol(
    root: Path,
    symbol: str,
    fresh: pl.DataFrame,
    *,
    requested_end_date: str,
    dry_run: bool,
    create_missing: bool,
) -> BackfillResult:
    output_path = root / f"{symbol}_features.parquet"
    try:
        exists = output_path.exists()
        if not exists and not create_missing:
            return BackfillResult(symbol, "missing_existing_file", 0, None, None, str(output_path))
        first = fresh.select(pl.col("date").min()).item()
        last = fresh.select(pl.col("date").max()).item()
        merged = fresh
        if exists:
            existing = _read_parquet_frame(output_path)
            merged = _merge_official_feature_frame(existing, fresh)
        if dry_run:
            status = "planned" if exists else "planned_create"
            return BackfillResult(symbol, status, int(fresh.height), str(first), str(last), str(output_path))
        _write_feature_parquet_atomic(merged, output_path, asset_class="tw_stocks", requested_end_date=requested_end_date)
        status = "updated" if exists else "created_delisted"
        return BackfillResult(symbol, status, int(fresh.height), str(first), str(last), str(output_path))
    except Exception as exc:
        return BackfillResult(symbol, "failed", 0, None, None, str(output_path), message=str(exc))


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    root = Path(args.symbols_root)
    normalized_archives = 0
    removed_archive_files = 0
    if args.normalize_delisted_archives:
        normalized_archives, removed_archive_files = _normalize_delisted_archive_files(
            root,
            dry_run=bool(args.dry_run),
        )
    existing_symbols = _existing_symbols(root)
    if not existing_symbols:
        raise SystemExit(f"No existing Yahoo-layout TW parquet files found under {root}")
    target_symbols = set(existing_symbols)
    if args.create_missing_delisted:
        target_symbols.update(_official_delisted_symbols(input_dir))
    names_updated = _update_symbols_csv_names(
        input_dir,
        root,
        existing_symbols,
        dry_run=bool(args.dry_run),
    )
    if names_updated:
        print(f"[tw-public-backfill] updated symbols.csv names={names_updated}", flush=True)
    try:
        start = _resolve_start_date(
            input_dir,
            root,
            args.start_date,
            create_missing_delisted=bool(args.create_missing_delisted),
        )
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc
    end = _parse_date_arg(args.end_date)

    started = time.perf_counter()
    fresh = _public_ohlcv_frame(input_dir, target_symbols, start, end)
    if fresh.is_empty():
        _, official_latest = _official_date_bounds(input_dir)
        if official_latest is not None and start > official_latest:
            _write_noop_summary(args, root, start, official_latest)
            return
        raise SystemExit(
            f"No official TW OHLCV rows matched symbols={len(target_symbols)} "
            f"start={start} end={end or 'latest'}"
        )
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
            executor.submit(
                _write_symbol,
                root,
                str(key[0] if isinstance(key, tuple) else key),
                frame,
                requested_end_date=latest_text,
                dry_run=bool(args.dry_run),
                create_missing=bool(args.create_missing_delisted),
            ): key
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
    created_symbols = {
        result.symbol for result in results if result.status == "created_delisted"
    }
    manifest_symbols_added = _upsert_created_symbols_manifest(
        input_dir,
        root,
        created_symbols,
        dry_run=bool(args.dry_run),
    )
    raw_return_provenance_added = _update_return_price_provenance(
        root,
        created_symbols,
        kind="official_raw_close",
        source="tw_public_ohlcv",
        dry_run=bool(args.dry_run),
    )
    elapsed = time.perf_counter() - started
    summary = {
        "input_dir": str(input_dir),
        "symbols_root": str(root),
        "start_date": str(start),
        "end_date": latest_text,
        "source_rows": int(fresh.height),
        "source_symbols": int(fresh.select(pl.col("symbol").n_unique()).item()),
        "existing_symbols": len(existing_symbols),
        "target_symbols": len(target_symbols),
        "official_delisted_targets": len(_official_delisted_symbols(input_dir)),
        "normalized_archive_symbols": normalized_archives,
        "removed_archive_files": removed_archive_files,
        "manifest_symbols_added": manifest_symbols_added,
        "raw_return_provenance_added": raw_return_provenance_added,
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
