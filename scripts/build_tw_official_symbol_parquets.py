from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
from typing import Any

import numpy as np
import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_public_features import (
    _date_column_expr,
    _file_content_receipt,
    _num_expr,
    _symbol_expr,
)
from stockagent.data.tw_security import (
    TW_ETF_SYMBOL_PATTERN,
    TW_STOCK_SYMBOL_PATTERN,
)


OFFICIAL_SOURCE_NAME = "twse_tpex_official"
LEGACY_COLUMN_ALIASES = {
    "code": "symbol",
    "security_code": "symbol",
    "venue": "market",
    "exchange": "market",
    "high": "max",
    "low": "min",
    "volume": "Trading_Volume",
    "change": "signed_change",
    "price_change": "signed_change",
    "adjusted_close": "source_adjclose",
    "adj_close": "source_adjclose",
    "adjclose": "source_adjclose",
    "reference_price": "source_reference",
    "opening_reference_price": "source_reference",
}
PARQUET_METADATA = {
    "source": b"stockagent.source",
    "asset_class": b"stockagent.asset_class",
    "checked_through": b"stockagent.official_checked_through",
    "first_date": b"stockagent.first_date",
    "last_date": b"stockagent.last_date",
    "write_ts_utc": b"stockagent.write_ts_utc",
}


@dataclass(slots=True)
class BuildResult:
    symbol: str
    security_type: str
    market: str
    name: str
    status: str
    rows: int
    adjusted_rows: int
    missing_adjustment_rows: int
    first_date: str | None
    last_date: str | None
    output_path: str
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build canonical per-symbol TW parquet files exclusively from official TWSE/TPEx daily data. "
            "adjclose is a reference-return index normalized to 10 at each symbol's first row."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_tw_public/stocks"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument(
        "--legacy-official-ohlcv",
        type=Path,
        action="append",
        default=[],
        help=(
            "Provenance-backed TWSE/TPEx archive to merge before index construction; "
            "repeatable and may provide adjclose, signed_change, or reference_price."
        ),
    )
    parser.add_argument("--legacy-source-name", default=None)
    parser.add_argument("--summary-path", type=Path, default=None)
    parser.add_argument("--report-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--allow-incomplete-source",
        action="store_true",
        help="Diagnostics only: permit non-full public download receipts.",
    )
    return parser.parse_args()


def _validate_download_receipts(input_dir: Path, *, allow_incomplete: bool) -> None:
    public_summary_path = input_dir / "download_summary.json"
    corporate_summary_path = input_dir / "tw_corporate_action_reference.summary.json"
    problems: list[str] = []
    try:
        public_summary = json.loads(public_summary_path.read_text(encoding="utf-8"))
        if public_summary.get("mode") != "full":
            problems.append(f"public download mode={public_summary.get('mode')!r}")
        if int(public_summary.get("failed_count", -1)) != 0:
            problems.append(f"public failed_count={public_summary.get('failed_count')!r}")
    except Exception as exc:
        problems.append(f"invalid public receipt: {type(exc).__name__}: {exc}")
    try:
        corporate_summary = json.loads(corporate_summary_path.read_text(encoding="utf-8"))
        if int(corporate_summary.get("failure_count", -1)) != 0:
            problems.append(
                f"corporate-action failure_count={corporate_summary.get('failure_count')!r}"
            )
        output_path = input_dir / "tw_corporate_action_reference.parquet"
        if corporate_summary.get("output_receipt") != _receipt(output_path):
            problems.append("corporate-action output receipt mismatch")
    except Exception as exc:
        problems.append(f"invalid corporate-action receipt: {type(exc).__name__}: {exc}")
    if problems and not allow_incomplete:
        raise RuntimeError(
            "official symbol build requires complete source receipts: " + "; ".join(problems)
        )


def _receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path), "size": int(path.stat().st_size), "sha256": digest.hexdigest()}


def _fsync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_official_quote_parquet(
    frame: pl.DataFrame,
    output_path: Path,
    *,
    checked_through: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    first_date = str(frame["date"].min())
    last_date = str(frame["date"].max())
    table = frame.to_arrow()
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            PARQUET_METADATA["source"]: OFFICIAL_SOURCE_NAME.encode("utf-8"),
            PARQUET_METADATA["asset_class"]: b"tw_stocks",
            PARQUET_METADATA["checked_through"]: str(checked_through).encode("utf-8"),
            PARQUET_METADATA["first_date"]: first_date.encode("utf-8"),
            PARQUET_METADATA["last_date"]: last_date.encode("utf-8"),
            PARQUET_METADATA["write_ts_utc"]: (
                datetime.now(timezone.utc).replace(microsecond=0).isoformat().encode("utf-8")
            ),
        }
    )
    table = table.replace_schema_metadata(metadata)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        pq.write_table(
            table,
            temporary,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
            row_group_size=128_000,
        )
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, output_path)
        _fsync_directory(output_path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _update_return_price_provenance(
    root: Path,
    symbols: set[str],
    *,
    dry_run: bool,
) -> None:
    path = root / "return_price_provenance.json"
    payload: dict[str, Any] = {"schema_version": 1, "symbols": {}}
    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for symbol in sorted(symbols):
        payload["symbols"][symbol] = {
            "kind": "official_reference_index_10",
            "source": OFFICIAL_SOURCE_NAME,
            "updated_at_utc": updated_at,
        }
    if dry_run:
        return
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _remove_stale_official_symbol_files(
    output_dir: Path,
    target_symbols: set[str],
    *,
    dry_run: bool,
) -> list[str]:
    removed: list[str] = []
    for path in sorted(output_dir.glob("*_features.parquet")):
        symbol = path.name.removesuffix("_features.parquet")
        if symbol in target_symbols:
            continue
        metadata = pq.read_schema(path).metadata or {}
        source = metadata.get(PARQUET_METADATA["source"], b"").decode("utf-8", errors="replace")
        if source != OFFICIAL_SOURCE_NAME:
            raise RuntimeError(f"refusing to remove non-official stale symbol file: {path}")
        removed.append(symbol)
        if not dry_run:
            path.unlink()
    if removed and not dry_run:
        _fsync_directory(output_dir)
    return removed


def _signed_number_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.Utf8, strict=False)
        .str.replace_all(r"[\s,]", "")
        .str.replace_all("−", "-")
        .str.replace_all("＋", "+")
        .cast(pl.Float64, strict=False)
    )


def _twse_frame(path: Path) -> pl.DataFrame:
    schema = set(pl.read_parquet_schema(path))
    required = {
        "date",
        "證券代號",
        "證券名稱",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
        "成交股數",
        "漲跌(+/-)",
        "漲跌價差",
    }
    missing = sorted(required - schema)
    if missing:
        raise ValueError(f"{path} is missing official TWSE columns: {missing}")
    direction = pl.col("漲跌(+/-)").cast(pl.Utf8, strict=False).str.strip_chars()
    magnitude = _num_expr("漲跌價差").abs()
    signed_change = (
        pl.when(direction == "-")
        .then(-magnitude)
        .when(direction == "+")
        .then(magnitude)
        .when(magnitude.fill_null(0.0) == 0.0)
        .then(0.0)
        .otherwise(None)
    )
    return (
        pl.read_parquet(path)
        .select(
            _date_column_expr("date").alias("date"),
            _symbol_expr("證券代號").str.to_uppercase().alias("symbol"),
            pl.col("證券名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
            pl.lit("twse").alias("market"),
            _num_expr("開盤價").alias("open"),
            _num_expr("最高價").alias("max"),
            _num_expr("最低價").alias("min"),
            _num_expr("收盤價").alias("close"),
            _num_expr("成交股數").alias("Trading_Volume"),
            signed_change.alias("signed_change"),
        )
    )


def _tpex_frame(path: Path) -> pl.DataFrame:
    schema = set(pl.read_parquet_schema(path))
    required = {"date", "代號", "名稱", "開盤", "最高", "最低", "收盤", "成交股數", "漲跌"}
    missing = sorted(required - schema)
    if missing:
        raise ValueError(f"{path} is missing official TPEx columns: {missing}")
    change_text = pl.col("漲跌").cast(pl.Utf8, strict=False).str.strip_chars()
    return (
        pl.read_parquet(path)
        .select(
            _date_column_expr("date").alias("date"),
            _symbol_expr("代號").str.to_uppercase().alias("symbol"),
            pl.col("名稱").cast(pl.Utf8, strict=False).str.strip_chars().alias("name"),
            pl.lit("tpex").alias("market"),
            _num_expr("開盤").alias("open"),
            _num_expr("最高").alias("max"),
            _num_expr("最低").alias("min"),
            _num_expr("收盤").alias("close"),
            _num_expr("成交股數").alias("Trading_Volume"),
            pl.when(change_text.is_in(["", "---", "--", "-"]))
            .then(None)
            .otherwise(_signed_number_expr("漲跌"))
            .alias("signed_change"),
        )
    )


def _legacy_official_frame(path: Path) -> pl.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        frame = pl.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        frame = pl.read_csv(path, infer_schema_length=0)
    else:
        raise ValueError(f"unsupported official archive format: {path}")
    rename = {
        source: target
        for source, target in LEGACY_COLUMN_ALIASES.items()
        if source in frame.columns and target not in frame.columns
    }
    if rename:
        frame = frame.rename(rename)
    required = {"date", "symbol", "market", "open", "max", "min", "close", "Trading_Volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{path} is missing official archive columns: {missing}")
    name = (
        pl.col("name").cast(pl.Utf8, strict=False).str.strip_chars()
        if "name" in frame.columns
        else pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars()
    )
    signed_change = (
        pl.col("signed_change").cast(pl.Float64, strict=False).fill_nan(None)
        if "signed_change" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_reference = (
        pl.col("source_reference").cast(pl.Float64, strict=False).fill_nan(None)
        if "source_reference" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    source_adjclose = (
        pl.col("source_adjclose").cast(pl.Float64, strict=False).fill_nan(None)
        if "source_adjclose" in frame.columns
        else pl.lit(None, dtype=pl.Float64)
    )
    normalized_market = (
        pl.col("market")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.to_lowercase()
        .replace(
            {
                "listed": "twse",
                "tse": "twse",
                "otc": "tpex",
                "gtsm": "tpex",
            }
        )
    )
    normalized = frame.select(
        _date_column_expr("date").alias("date"),
        pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase(),
        name.alias("name"),
        normalized_market.alias("market"),
        *[
            pl.col(column).cast(pl.Float64, strict=False).fill_nan(None).alias(column)
            for column in ("open", "max", "min", "close", "Trading_Volume")
        ],
        signed_change.alias("signed_change"),
        source_reference.alias("source_reference"),
        source_adjclose.alias("source_adjclose"),
    )
    invalid_market = normalized.filter(~pl.col("market").is_in(["twse", "tpex"]))
    if invalid_market.height:
        raise ValueError(f"{path} has {invalid_market.height} rows outside TWSE/TPEx")
    duplicate_keys = (
        normalized.group_by(["date", "symbol"])
        .len()
        .filter(pl.col("len") > 1)
        .height
    )
    if duplicate_keys:
        raise ValueError(f"{path} has {duplicate_keys} duplicate date-symbol keys")
    return normalized


def _official_frame(
    input_dir: Path,
    legacy_paths: list[Path] | None = None,
) -> tuple[pl.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    paths = [
        input_dir / "twse_daily_ohlcv.parquet",
        input_dir / "tpex_daily_ohlcv.parquet",
        input_dir / "tw_corporate_action_reference.parquet",
    ]
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"official daily OHLCV sources are missing: {missing}")
    legacy_paths = list(legacy_paths or [])
    missing_legacy = [str(path) for path in legacy_paths if not path.is_file()]
    if missing_legacy:
        raise FileNotFoundError(f"official legacy archives are missing: {missing_legacy}")
    before = [_file_content_receipt(path) for path in paths]
    legacy_before = [_receipt(path) for path in legacy_paths]
    current = pl.concat([_twse_frame(paths[0]), _tpex_frame(paths[1])], how="vertical_relaxed").with_columns(
        pl.lit(None, dtype=pl.Float64).alias("source_reference"),
        pl.lit(None, dtype=pl.Float64).alias("source_adjclose"),
        pl.lit(-1, dtype=pl.Int32).alias("_legacy_source_id"),
        pl.lit(1, dtype=pl.Int8).alias("_source_priority"),
    )
    legacy_frames = [
        _legacy_official_frame(path).with_columns(
            pl.lit(source_id, dtype=pl.Int32).alias("_legacy_source_id"),
            pl.lit(0, dtype=pl.Int8).alias("_source_priority"),
        )
        for source_id, path in enumerate(legacy_paths)
    ]
    if len(legacy_frames) > 1:
        duplicate_legacy = (
            pl.concat(legacy_frames, how="vertical_relaxed")
            .group_by(["date", "symbol"])
            .len()
            .filter(pl.col("len") > 1)
        )
        if duplicate_legacy.height:
            raise ValueError(
                f"legacy official archives overlap on {duplicate_legacy.height} date-symbol keys"
            )
    frame = pl.concat([*legacy_frames, current], how="vertical_relaxed")
    reference = (
        pl.read_parquet(paths[2])
        .select(
            _date_column_expr("date").alias("date"),
            pl.col("symbol").cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase(),
            pl.coalesce(
                pl.col("reference_price").cast(pl.Float64, strict=False),
                pl.col("opening_reference_price").cast(pl.Float64, strict=False),
            ).alias("reference_override"),
        )
        .drop_nulls(["date", "symbol", "reference_override"])
        .unique(["date", "symbol"], keep="last", maintain_order=True)
    )
    frame = frame.join(reference, on=["date", "symbol"], how="left").with_columns(
        pl.coalesce("reference_override", "source_reference").alias("reference_override")
    )
    after = [_file_content_receipt(path) for path in paths]
    legacy_after = [_receipt(path) for path in legacy_paths]
    if before != after or legacy_before != legacy_after:
        raise RuntimeError("official OHLCV source changed during symbol build")
    stock_symbol = pl.col("symbol").str.contains(TW_STOCK_SYMBOL_PATTERN).fill_null(False)
    etf_symbol = pl.col("symbol").str.contains(TW_ETF_SYMBOL_PATTERN).fill_null(False)
    frame = (
        frame.with_columns(
            pl.when(stock_symbol)
            .then(pl.lit("stock"))
            .when(etf_symbol)
            .then(pl.lit("etf"))
            .otherwise(None)
            .alias("security_type")
        )
        .drop_nulls("security_type")
        .drop_nulls(["date", "symbol", "close"])
        .filter(pl.col("close").is_finite() & (pl.col("close") > 0))
        .sort(["symbol", "date", "_source_priority", "market"])
        .unique(["symbol", "date"], keep="last", maintain_order=True)
    )
    if frame.is_empty():
        raise RuntimeError("official TWSE/TPEx sources produced no valid stock rows")
    return frame, before, legacy_before


def _normalized_reference_index(
    close: np.ndarray,
    signed_change: np.ndarray,
    volume: np.ndarray,
    reference_override: np.ndarray | None = None,
    factor_override: np.ndarray | None = None,
) -> tuple[np.ndarray, int]:
    close = np.asarray(close, dtype=np.float64)
    change = np.asarray(signed_change, dtype=np.float64)
    volume = np.asarray(volume, dtype=np.float64)
    reference_override = (
        np.asarray(reference_override, dtype=np.float64)
        if reference_override is not None
        else np.full(close.shape, np.nan, dtype=np.float64)
    )
    factor_override = (
        np.asarray(factor_override, dtype=np.float64)
        if factor_override is not None
        else np.full(close.shape, np.nan, dtype=np.float64)
    )
    output = np.full(close.shape, np.nan, dtype=np.float64)
    if close.size == 0:
        return output, 0
    state = 10.0
    output[0] = state
    missing = 0
    for idx in range(1, close.size):
        if not math.isfinite(close[idx]) or close[idx] <= 0:
            missing += 1
            continue
        if math.isfinite(factor_override[idx]) and factor_override[idx] > 0:
            factor = factor_override[idx]
        elif math.isfinite(reference_override[idx]) and reference_override[idx] > 0:
            factor = close[idx] / reference_override[idx]
        elif math.isfinite(change[idx]):
            reference = close[idx] - change[idx]
            factor = close[idx] / reference if reference > 0 else math.nan
        elif math.isfinite(volume[idx]) and volume[idx] == 0:
            factor = 1.0
        else:
            factor = math.nan
        if not math.isfinite(factor) or factor <= 0:
            missing += 1
            continue
        state *= factor
        output[idx] = state
    return output, missing


def _source_adjustment_factors(
    source_adjclose: np.ndarray,
    source_ids: np.ndarray | None = None,
) -> np.ndarray:
    values = np.asarray(source_adjclose, dtype=np.float64)
    groups = (
        np.asarray(source_ids)
        if source_ids is not None
        else np.zeros(values.shape, dtype=np.int64)
    )
    if groups.shape != values.shape:
        raise ValueError("source_ids must have the same shape as source_adjclose")
    factors = np.full(values.shape, np.nan, dtype=np.float64)
    if values.size < 2:
        return factors
    valid = (
        np.isfinite(values[1:])
        & np.isfinite(values[:-1])
        & (values[1:] > 0)
        & (values[:-1] > 0)
        & (groups[1:] == groups[:-1])
    )
    factors[1:] = np.where(valid, values[1:] / values[:-1], np.nan)
    return factors


def _write_symbol(
    output_dir: Path,
    symbol: str,
    frame: pl.DataFrame,
    *,
    requested_end_date: str,
    dry_run: bool,
) -> BuildResult:
    path = output_dir / f"{symbol}_features.parquet"
    try:
        frame = frame.sort("date")
        adjusted, missing = _normalized_reference_index(
            frame["close"].to_numpy(),
            frame["signed_change"].to_numpy(),
            frame["Trading_Volume"].to_numpy(),
            frame["reference_override"].to_numpy(),
            _source_adjustment_factors(
                frame["source_adjclose"].to_numpy(),
                frame["_legacy_source_id"].to_numpy(),
            ),
        )
        quotes = frame.select(
            ["date", "open", "max", "min", "close", "Trading_Volume"]
        ).with_columns(pl.Series("adjclose", adjusted))
        first = frame["date"].min()
        last = frame["date"].max()
        market = str(frame["market"][-1])
        name = str(frame["name"][-1] or symbol)
        if not dry_run:
            _write_official_quote_parquet(
                quotes,
                path,
                checked_through=requested_end_date,
            )
        return BuildResult(
            symbol=symbol,
            security_type=str(frame["security_type"][-1]),
            market=market,
            name=name,
            status="planned" if dry_run else "created",
            rows=frame.height,
            adjusted_rows=int(np.count_nonzero(np.isfinite(adjusted))),
            missing_adjustment_rows=missing,
            first_date=str(first),
            last_date=str(last),
            output_path=str(path),
        )
    except Exception as exc:
        return BuildResult(
            symbol=symbol,
            security_type="",
            market="",
            name=symbol,
            status="failed",
            rows=0,
            adjusted_rows=0,
            missing_adjustment_rows=0,
            first_date=None,
            last_date=None,
            output_path=str(path),
            message=f"{type(exc).__name__}: {exc}",
        )


def main() -> None:
    args = parse_args()
    if args.legacy_official_ohlcv and not args.legacy_source_name:
        raise ValueError("--legacy-source-name is required with --legacy-official-ohlcv")
    if args.legacy_source_name and not args.legacy_official_ohlcv:
        raise ValueError("--legacy-official-ohlcv is required with --legacy-source-name")
    _validate_download_receipts(
        args.input_dir,
        allow_incomplete=bool(args.allow_incomplete_source),
    )
    frame, receipts, legacy_receipts = _official_frame(
        args.input_dir,
        list(args.legacy_official_ohlcv),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    requested_end_date = str(frame["date"].max())
    groups = frame.partition_by("symbol", as_dict=True, maintain_order=False)
    results: list[BuildResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _write_symbol,
                args.output_dir,
                str(key[0] if isinstance(key, tuple) else key),
                symbol_frame,
                requested_end_date=requested_end_date,
                dry_run=bool(args.dry_run),
            ): key
            for key, symbol_frame in groups.items()
        }
        for future in as_completed(futures):
            results.append(future.result())
    failed = [result for result in results if result.status == "failed"]
    if failed:
        raise RuntimeError(f"official symbol parquet writes failed: {[asdict(item) for item in failed[:10]]}")

    target_symbols = {result.symbol for result in results}
    stale_symbols_removed = _remove_stale_official_symbol_files(
        args.output_dir,
        target_symbols,
        dry_run=bool(args.dry_run),
    )
    manifest = pl.DataFrame(
        {
            "code": [result.symbol for result in results],
            "name": [result.name for result in results],
            "market": [result.market for result in results],
            "security_type": [result.security_type for result in results],
            "source": [OFFICIAL_SOURCE_NAME] * len(results),
        }
    ).sort("code")
    if not args.dry_run:
        temporary = args.output_dir / "symbols.csv.tmp"
        manifest.write_csv(temporary)
        temporary.replace(args.output_dir / "symbols.csv")
    _update_return_price_provenance(
        args.output_dir,
        target_symbols,
        dry_run=bool(args.dry_run),
    )
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    summary = {
        "schema_version": 1,
        "source": OFFICIAL_SOURCE_NAME,
        "adjusted_price_method": (
            "first=10; next=previous_index*(close/reference); "
            "reference=corporate_action_reference else close-signed_change"
        ),
        "source_receipts": receipts,
        "legacy_source_name": str(args.legacy_source_name or ""),
        "legacy_source_receipts": legacy_receipts,
        "rows": int(frame.height),
        "symbols": len(results),
        "security_type_counts": {
            security_type: sum(result.security_type == security_type for result in results)
            for security_type in ("stock", "etf")
        },
        "stale_symbols_removed": stale_symbols_removed,
        "first_date": str(frame["date"].min()),
        "last_date": str(frame["date"].max()),
        "adjusted_rows": sum(result.adjusted_rows for result in results),
        "missing_adjustment_rows": sum(result.missing_adjustment_rows for result in results),
        "status_counts": status_counts,
        "dry_run": bool(args.dry_run),
    }
    summary_path = args.summary_path or args.output_dir / "official_symbol_build_summary.json"
    report_path = args.report_path or args.output_dir / "official_symbol_build_report.csv"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pl.DataFrame([asdict(result) for result in sorted(results, key=lambda item: item.symbol)]).write_csv(
        report_path
    )
    print(
        f"[tw-official-symbols] rows={frame.height} symbols={len(results)} "
        f"missing_adjustments={summary['missing_adjustment_rows']} summary={summary_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
