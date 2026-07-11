from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import Any

import polars as pl
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_public_features import _date_column_expr
from stockagent.data.tw_security import classify_tw_stock_or_etf


YAHOO_SOURCE_METADATA_KEY = b"stockagent.source"
FEATURE_FILE_PATTERN = re.compile(
    r"^(?P<symbol>[0-9A-Z]{4,6})(?:_(?P<venue>TW|TWO))?_features\.parquet$",
    re.IGNORECASE,
)
OUTPUT_COLUMNS = (
    "date",
    "symbol",
    "name",
    "market",
    "open",
    "max",
    "min",
    "close",
    "Trading_Volume",
    "source_adjclose",
    "source_factor",
    "quote_source",
)


@dataclass(slots=True)
class FallbackBuildResult:
    symbol: str
    status: str
    rows: int
    first_date: str | None
    last_date: str | None
    source_files: str
    message: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Project Yahoo TW stock/ETF files into an auditable OHLCV fallback archive. "
            "The archive is lower priority than TWSE/TPEx rows in the canonical builder."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data_tw_public/fallback/yahoo_tw_stocks"))
    parser.add_argument("--official-input-dir", type=Path, default=Path("data_tw_public"))
    parser.add_argument(
        "--output-path",
        type=Path,
        default=Path("data_tw_public/fallback/yahoo_tw_ohlcv.parquet"),
    )
    parser.add_argument("--start-date", default="2000-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--summary-path", type=Path, default=None)
    parser.add_argument("--report-path", type=Path, default=None)
    return parser.parse_args()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _file_receipt(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "path": str(path),
        "size": int(path.stat().st_size),
        "sha256": digest.hexdigest(),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _canonical_symbol(path: Path) -> tuple[str, str | None] | None:
    match = FEATURE_FILE_PATTERN.fullmatch(path.name)
    if match is None:
        return None
    symbol = match.group("symbol").upper()
    if classify_tw_stock_or_etf(symbol) is None:
        return None
    venue = match.group("venue")
    return symbol, venue.upper() if venue else None


def _manifest_metadata(path: Path) -> dict[str, tuple[str, str | None]]:
    if not path.exists():
        return {}
    frame = pl.read_csv(path, infer_schema_length=10000)
    if "code" not in frame.columns:
        return {}
    output: dict[str, tuple[str, str | None]] = {}
    for row in frame.iter_rows(named=True):
        raw_code = str(row.get("code") or "").strip().upper()
        match = re.fullmatch(r"(?P<symbol>[0-9A-Z]{4,6})(?:_(?:TW|TWO))?", raw_code)
        if match is None:
            continue
        symbol = match.group("symbol")
        if classify_tw_stock_or_etf(symbol) is None:
            continue
        name = str(row.get("name") or symbol).strip() or symbol
        yahoo_symbol = str(row.get("yahoo_symbol") or "").strip().upper()
        market: str | None
        if "," not in yahoo_symbol and yahoo_symbol.endswith(".TWO"):
            market = "tpex"
        elif "," not in yahoo_symbol and yahoo_symbol.endswith(".TW"):
            market = "twse"
        else:
            market = None
        current = output.get(symbol)
        if current is None or market is not None:
            output[symbol] = (name, market)
    return output


def _whitelist_markets(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    candidates: dict[str, set[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        yahoo_symbol = line.strip().upper()
        match = re.fullmatch(r"(?P<symbol>[0-9A-Z]{4,6})\.(?P<venue>TW|TWO)", yahoo_symbol)
        if match is None:
            continue
        symbol = match.group("symbol")
        if classify_tw_stock_or_etf(symbol) is None:
            continue
        market = "twse" if match.group("venue") == "TW" else "tpex"
        candidates.setdefault(symbol, set()).add(market)
    return {
        symbol: next(iter(markets))
        for symbol, markets in candidates.items()
        if len(markets) == 1
    }


def _official_market_lookup(root: Path) -> dict[str, str]:
    frames: list[pl.DataFrame] = []
    specs = (
        (root / "twse_daily_ohlcv.parquet", "證券代號", "twse"),
        (root / "tpex_daily_ohlcv.parquet", "代號", "tpex"),
    )
    for path, symbol_column, market in specs:
        if not path.exists() or symbol_column not in pq.read_schema(path).names:
            continue
        frames.append(
            pl.read_parquet(path, columns=["date", symbol_column]).select(
                _date_column_expr("date").alias("date"),
                pl.col(symbol_column)
                .cast(pl.Utf8, strict=False)
                .str.strip_chars()
                .str.to_uppercase()
                .alias("symbol"),
                pl.lit(market).alias("market"),
            )
        )
    if not frames:
        return {}
    latest = (
        pl.concat(frames, how="vertical_relaxed")
        .drop_nulls(["date", "symbol"])
        .sort(["symbol", "date", "market"])
        .unique("symbol", keep="last", maintain_order=True)
    )
    return dict(latest.select(["symbol", "market"]).iter_rows())


def _source_file_groups(root: Path) -> dict[str, list[tuple[Path, str | None]]]:
    groups: dict[str, list[tuple[Path, str | None]]] = {}
    for path in sorted(root.glob("*_features.parquet")):
        parsed = _canonical_symbol(path)
        if parsed is None:
            continue
        symbol, venue = parsed
        groups.setdefault(symbol, []).append((path, venue))

    selected: dict[str, list[tuple[Path, str | None]]] = {}
    for symbol, paths in groups.items():
        canonical = [(path, venue) for path, venue in paths if venue is None]
        selected[symbol] = canonical[:1] if canonical else paths
    return selected


def _number(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Float64, strict=False).fill_nan(None)


def _read_symbol_fallback(
    symbol: str,
    sources: list[tuple[Path, str | None]],
    *,
    manifest: dict[str, tuple[str, str | None]],
    official_markets: dict[str, str],
    start: date,
    end: date,
) -> tuple[FallbackBuildResult, pl.DataFrame | None]:
    source_names = ",".join(str(path) for path, _ in sources)
    try:
        frames: list[pl.DataFrame] = []
        manifest_name, manifest_market = manifest.get(symbol, (symbol, None))
        for path, venue in sources:
            schema = pq.read_schema(path)
            columns = set(schema.names)
            required = {"date", "open", "max", "min", "close", "Trading_Volume"}
            missing = sorted(required - columns)
            if missing:
                raise ValueError(f"{path} missing Yahoo OHLCV columns: {missing}")
            source_metadata = schema.metadata or {}
            source_name = source_metadata.get(YAHOO_SOURCE_METADATA_KEY, b"").decode(
                "utf-8", errors="replace"
            )
            if source_name and source_name != "yahoo":
                raise ValueError(f"{path} has unexpected source metadata: {source_name!r}")
            if venue == "TWO":
                market = "tpex"
            elif venue == "TW":
                market = "twse"
            else:
                market = manifest_market or official_markets.get(symbol)
            if market not in {"twse", "tpex"}:
                raise ValueError(f"cannot resolve TWSE/TPEx venue for Yahoo symbol {symbol}")
            adjclose = (
                pl.coalesce(_number("adjclose"), _number("close"))
                if "adjclose" in columns
                else _number("close")
            )
            frame = (
                pl.read_parquet(
                    path,
                    columns=[
                        column
                        for column in (
                            "date",
                            "open",
                            "max",
                            "min",
                            "close",
                            "Trading_Volume",
                            "adjclose",
                        )
                        if column in columns
                    ],
                )
                .select(
                    _date_column_expr("date").alias("date"),
                    pl.lit(symbol).alias("symbol"),
                    pl.lit(manifest_name).alias("name"),
                    pl.lit(market).alias("market"),
                    _number("open").alias("open"),
                    _number("max").alias("max"),
                    _number("min").alias("min"),
                    _number("close").alias("close"),
                    _number("Trading_Volume").fill_null(0.0).alias("Trading_Volume"),
                    adjclose.alias("source_adjclose"),
                    pl.lit("yahoo_fallback").alias("quote_source"),
                )
                .filter(pl.col("date").is_between(start, end))
                .drop_nulls(["date", "open", "max", "min", "close", "source_adjclose"])
                .filter(
                    pl.all_horizontal(
                        [
                            pl.col(column).is_finite() & (pl.col(column) > 0.0)
                            for column in ("open", "max", "min", "close", "source_adjclose")
                        ]
                    )
                    & (pl.col("max") >= pl.max_horizontal("open", "close"))
                    & (pl.col("min") <= pl.min_horizontal("open", "close"))
                    & (pl.col("max") >= pl.col("min"))
                    & pl.col("Trading_Volume").is_finite()
                    & (pl.col("Trading_Volume") >= 0.0)
                )
                .sort("date")
                .with_columns(
                    (
                        pl.col("source_adjclose")
                        / pl.col("source_adjclose").shift(1)
                    ).alias("source_factor")
                )
            )
            if not frame.is_empty():
                frames.append(frame)
        if not frames:
            return (
                FallbackBuildResult(symbol, "empty", 0, None, None, source_names),
                None,
            )
        combined = pl.concat(frames, how="vertical_relaxed")
        conflicts = (
            combined.group_by(["date", "symbol"])
            .agg(pl.col("close").drop_nulls().n_unique().alias("close_values"))
            .filter(pl.col("close_values") > 1)
        )
        if conflicts.height:
            raise ValueError(
                f"Yahoo .TW/.TWO candidates conflict on {conflicts.height} date-symbol keys"
            )
        combined = (
            combined.sort(["symbol", "date", "market"])
            .unique(["symbol", "date"], keep="last", maintain_order=True)
            .select(OUTPUT_COLUMNS)
        )
        return (
            FallbackBuildResult(
                symbol=symbol,
                status="ok",
                rows=int(combined.height),
                first_date=str(combined["date"].min()),
                last_date=str(combined["date"].max()),
                source_files=source_names,
            ),
            combined,
        )
    except Exception as exc:
        return (
            FallbackBuildResult(
                symbol=symbol,
                status="failed",
                rows=0,
                first_date=None,
                last_date=None,
                source_files=source_names,
                message=f"{type(exc).__name__}: {exc}",
            ),
            None,
        )


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start < date(2000, 1, 1):
        raise ValueError("Yahoo fallback is restricted to 2000-01-01 and later")
    if end < start:
        raise ValueError("--end-date must be on or after --start-date")
    if not args.input_dir.is_dir():
        raise FileNotFoundError(f"Yahoo fallback directory is missing: {args.input_dir}")

    groups = _source_file_groups(args.input_dir)
    if not groups:
        raise RuntimeError(f"no Yahoo stock/ETF parquet files found under {args.input_dir}")
    manifest = _manifest_metadata(args.input_dir / "symbols.csv")
    for symbol, market in _whitelist_markets(
        args.input_dir / "yahoo_whitelist.txt"
    ).items():
        name, manifest_market = manifest.get(symbol, (symbol, None))
        if manifest_market is None:
            manifest[symbol] = (name, market)
    official_markets = _official_market_lookup(args.official_input_dir)
    results: list[FallbackBuildResult] = []
    frames: list[pl.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _read_symbol_fallback,
                symbol,
                sources,
                manifest=manifest,
                official_markets=official_markets,
                start=start,
                end=end,
            ): symbol
            for symbol, sources in groups.items()
        }
        for future in as_completed(futures):
            result, frame = future.result()
            results.append(result)
            if frame is not None:
                frames.append(frame)

    results.sort(key=lambda item: item.symbol)
    failures = [item for item in results if item.status == "failed"]
    summary_path = args.summary_path or args.output_path.with_suffix(".summary.json")
    report_path = args.report_path or args.output_path.with_suffix(".report.csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [asdict(item) for item in results], infer_schema_length=None
    ).write_csv(report_path)
    summary: dict[str, Any] = {
        "schema_version": 1,
        "generated_at_utc": _utc_now(),
        "source": "yahoo_fallback",
        "input_dir": str(args.input_dir),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "source_symbol_count": len(groups),
        "ok_symbol_count": sum(item.status == "ok" for item in results),
        "empty_symbol_count": sum(item.status == "empty" for item in results),
        "failed_symbol_count": len(failures),
        "rows": sum(item.rows for item in results),
        "output_path": str(args.output_path),
        "report_path": str(report_path),
    }
    if failures:
        summary["failure_examples"] = [asdict(item) for item in failures[:20]]
        _write_json(summary_path, summary)
        raise RuntimeError(
            f"Yahoo fallback archive build failed for {len(failures)} symbols; report={report_path}"
        )
    if not frames:
        _write_json(summary_path, summary)
        raise RuntimeError("Yahoo fallback archive contains no usable rows")

    archive = (
        pl.concat(frames, how="vertical_relaxed")
        .sort(["symbol", "date", "market"])
        .unique(["symbol", "date"], keep="last", maintain_order=True)
    )
    duplicate_keys = (
        archive.group_by(["symbol", "date"]).len().filter(pl.col("len") > 1).height
    )
    if duplicate_keys:
        raise RuntimeError(f"Yahoo fallback archive has {duplicate_keys} duplicate keys")
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{args.output_path.name}.",
        suffix=".tmp",
        dir=args.output_path.parent,
        delete=False,
    )
    temporary = Path(handle.name)
    handle.close()
    try:
        archive.write_parquet(temporary, compression="zstd", statistics=True)
        os.replace(temporary, args.output_path)
    finally:
        temporary.unlink(missing_ok=True)
    summary.update(
        {
            "rows": int(archive.height),
            "symbols": int(archive["symbol"].n_unique()),
            "first_date": str(archive["date"].min()),
            "last_date": str(archive["date"].max()),
            "output_receipt": _file_receipt(args.output_path),
        }
    )
    _write_json(summary_path, summary)
    print(
        f"[tw-yahoo-fallback] rows={archive.height} symbols={summary['symbols']} "
        f"output={args.output_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
