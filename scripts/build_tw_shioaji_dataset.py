from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any

import polars as pl
import pyarrow.parquet as pq


SOURCE_NAME = "tw_public_before_shioaji_after"
SHIOAJI_SOURCE_NAME = "shioaji_kbars_1m"
SHIOAJI_STORAGE_FREQUENCY = "daily"
DEFAULT_CUTOVER = date(2020, 3, 2)


@dataclass(slots=True)
class BuildResult:
    symbol: str
    status: str
    rows: int
    public_rows: int
    shioaji_rows: int
    dropped_public_rows_after_cutover: int
    shioaji_only_rows: int
    quarantined_transitions: int
    first_date: str | None
    last_date: str | None
    output_path: str
    public_source_gap_fallback_rows: int = 0
    message: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a separate TW dataset that inherits public rows before each "
            "symbol's first verified Shioaji daily bar and uses only Shioaji quotes "
            "from that date onward. Public point-in-time adjustment evidence is kept."
        )
    )
    parser.add_argument(
        "--base-stock-root", type=Path, default=Path("data_tw_public/stocks")
    )
    parser.add_argument(
        "--shioaji-root", type=Path, default=Path("data_tw_public/shioaji")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_tw_public/shioaji/stocks")
    )
    parser.add_argument("--cutover-date", default=DEFAULT_CUTOVER.isoformat())
    parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Diagnostics/pilot only: build from a subset download receipt.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(
            f"invalid JSON receipt {path}: {type(exc).__name__}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSON receipt is not an object: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_download_summary(root: Path, *, allow_incomplete: bool) -> dict[str, Any]:
    path = root / "download_summary.json"
    summary = _read_json(path)
    checks = {
        "source": summary.get("source") == SHIOAJI_SOURCE_NAME,
        "storage_frequency": (
            summary.get("storage_frequency") == SHIOAJI_STORAGE_FREQUENCY
        ),
        "history_start": summary.get("historical_start") == DEFAULT_CUTOVER.isoformat(),
        "coverage": summary.get("universe_coverage_complete") is True,
        "no_failures": int(summary.get("failed_symbols", -1)) == 0,
        "no_partials": int(summary.get("partial_symbols", -1)) == 0,
    }
    if allow_incomplete:
        checks.pop("coverage")
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"Shioaji download summary failed checks {failed}: {path}")
    return summary


def _load_report(root: Path) -> dict[str, dict[str, Any]]:
    path = root / "download_report.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Shioaji download report is missing: {path}")
    frame = pl.read_csv(path, infer_schema_length=0)
    required = {"symbol", "status"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{path} is missing columns: {missing}")
    return {
        str(row["symbol"]).strip().upper(): row
        for row in frame.iter_rows(named=True)
    }


def _validate_daily_receipt(path: Path) -> dict[str, Any]:
    summary_path = path.with_suffix(".summary.json")
    summary = _read_json(summary_path)
    output = summary.get("output_receipt")
    checks = {
        "source": summary.get("source") == SHIOAJI_SOURCE_NAME,
        "storage_frequency": (
            summary.get("storage_frequency") == SHIOAJI_STORAGE_FREQUENCY
        ),
        "symbol": summary.get("symbol") == path.stem,
        "output_object": isinstance(output, dict),
        "path": isinstance(output, dict) and Path(str(output.get("path", ""))) == path,
        "size": isinstance(output, dict)
        and path.is_file()
        and int(output.get("size", -1)) == int(path.stat().st_size),
        "sha256": isinstance(output, dict)
        and path.is_file()
        and str(output.get("sha256", "")) == _sha256(path),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"Shioaji daily receipt failed checks {failed}: {summary_path}")
    return summary


def _finite_positive(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def _public_adjustment_multiplier(
    previous: dict[str, Any] | None,
    current: dict[str, Any] | None,
) -> float | None:
    if previous is None or current is None:
        return None
    if bool(previous.get("return_quarantined")):
        return None
    values = (
        previous.get("adjclose"),
        current.get("adjclose"),
        previous.get("close"),
        current.get("close"),
    )
    if not all(_finite_positive(value) for value in values):
        return None
    previous_adj, current_adj, previous_close, current_close = map(float, values)
    raw_ratio = current_close / previous_close
    adjusted_ratio = current_adj / previous_adj
    multiplier = adjusted_ratio / raw_ratio
    return multiplier if math.isfinite(multiplier) and multiplier > 0.0 else None


def merge_symbol_frames(
    base: pl.DataFrame,
    shioaji: pl.DataFrame,
    *,
    cutover: date,
    declared_source_gap_dates: set[date] | None = None,
) -> tuple[pl.DataFrame, dict[str, int]]:
    required_base = {
        "date",
        "open",
        "max",
        "min",
        "close",
        "Trading_Volume",
        "adjclose",
        "data_source",
    }
    required_shioaji = {
        "date",
        "open",
        "max",
        "min",
        "close",
        "Trading_Volume",
        "Trading_Value",
        "shioaji_volume_lots",
        "shioaji_minute_bars",
        "data_source",
    }
    missing_base = sorted(required_base - set(base.columns))
    missing_shioaji = sorted(required_shioaji - set(shioaji.columns))
    if missing_base:
        raise ValueError(f"base symbol frame is missing columns: {missing_base}")
    if missing_shioaji:
        raise ValueError(f"Shioaji daily frame is missing columns: {missing_shioaji}")
    base = base.with_columns(pl.col("date").cast(pl.Date, strict=False)).sort("date")
    shioaji = (
        shioaji.with_columns(pl.col("date").cast(pl.Date, strict=False))
        .filter(pl.col("date") >= pl.lit(cutover))
        .sort("date")
    )
    if shioaji.is_empty():
        raise ValueError(f"Shioaji daily frame has no rows on or after {cutover}")
    if shioaji.get_column("date").n_unique() != shioaji.height:
        raise ValueError("Shioaji daily frame contains duplicate dates")
    valid_shioaji = (
        pl.all_horizontal(
            *[
                pl.col(column).is_not_null()
                & pl.col(column).is_finite()
                & (pl.col(column) > 0.0)
                for column in ("open", "max", "min", "close")
            ]
        )
        & (pl.col("max") >= pl.max_horizontal("open", "min", "close"))
        & (pl.col("min") <= pl.min_horizontal("open", "max", "close"))
        & pl.col("Trading_Volume").is_not_null()
        & pl.col("Trading_Volume").is_finite()
        & (pl.col("Trading_Volume") >= 0.0)
    )
    invalid = int(shioaji.select((~valid_shioaji).fill_null(True).sum()).item() or 0)
    if invalid:
        raise ValueError(f"Shioaji daily frame has {invalid} invalid rows")

    effective_cutover = shioaji.get_column("date").min()
    base_rows = {row["date"]: row for row in base.iter_rows(named=True)}
    output_rows = [
        dict(row)
        for row in base.filter(pl.col("date") < pl.lit(effective_cutover)).iter_rows(
            named=True
        )
    ]
    base_dates_after = {
        value for value in base.get_column("date") if value >= effective_cutover
    }
    shioaji_dates = set(shioaji.get_column("date"))
    declared_source_gap_dates = declared_source_gap_dates or set()
    fallback_dates = {
        value
        for value in declared_source_gap_dates
        if value >= effective_cutover
        and value in base_rows
        and value not in shioaji_dates
    }
    shioaji_rows_by_date = {
        row["date"]: row for row in shioaji.iter_rows(named=True)
    }
    shioaji_only_rows = 0
    quarantined_transitions = 0
    for quote_date in sorted(shioaji_dates | fallback_dates):
        quote = shioaji_rows_by_date.get(quote_date)
        public_row = base_rows.get(quote_date)
        row = (
            dict(public_row)
            if public_row is not None
            else {column: None for column in base.columns}
        )
        if quote is not None:
            if public_row is None:
                shioaji_only_rows += 1
            row.update(
                {
                    "date": quote_date,
                    "open": float(quote["open"]),
                    "max": float(quote["max"]),
                    "min": float(quote["min"]),
                    "close": float(quote["close"]),
                    "Trading_Volume": float(quote["Trading_Volume"]),
                    "data_source": SHIOAJI_SOURCE_NAME,
                    "fallback_reason": None,
                    "shioaji_trading_value": float(quote["Trading_Value"]),
                    "shioaji_volume_lots": float(quote["shioaji_volume_lots"]),
                    "shioaji_minute_bars": int(quote["shioaji_minute_bars"]),
                }
            )
        else:
            assert public_row is not None
            row.update(
                {
                    "date": quote_date,
                    "fallback_reason": "shioaji_declared_source_gap",
                    "shioaji_trading_value": None,
                    "shioaji_volume_lots": None,
                    "shioaji_minute_bars": None,
                }
            )
        if not output_rows:
            row["adjclose"] = 10.0
            row["adjustment_source"] = (
                "shioaji_first_observation"
                if quote is not None
                else "public_source_gap_first_observation"
            )
        else:
            previous = output_rows[-1]
            previous_public = base_rows.get(previous["date"])
            multiplier = _public_adjustment_multiplier(previous_public, public_row)
            if (
                multiplier is not None
                and _finite_positive(previous.get("adjclose"))
                and _finite_positive(previous.get("close"))
            ):
                shioaji_raw_ratio = float(row["close"]) / float(previous["close"])
                row["adjclose"] = (
                    float(previous["adjclose"]) * shioaji_raw_ratio * multiplier
                )
                row["adjustment_source"] = (
                    "shioaji_close_with_public_corporate_action_factor"
                    if quote is not None
                    else "public_source_gap_with_public_corporate_action_factor"
                )
            else:
                if _finite_positive(previous.get("adjclose")) and _finite_positive(
                    previous.get("close")
                ):
                    row["adjclose"] = float(previous["adjclose"]) * (
                        float(row["close"]) / float(previous["close"])
                    )
                else:
                    row["adjclose"] = 10.0
                row["adjustment_source"] = (
                    "shioaji_unadjusted_close_quarantined"
                    if quote is not None
                    else "public_source_gap_unadjusted_close_quarantined"
                )
                if not bool(previous.get("return_quarantined")):
                    quarantined_transitions += 1
                previous["return_quarantined"] = True
                previous["return_quarantine_reason"] = (
                    "shioaji_missing_public_adjustment_evidence"
                )
        row.setdefault("return_quarantined", False)
        row.setdefault("return_quarantine_reason", None)
        output_rows.append(row)

    schema = dict(base.schema)
    schema.setdefault("return_quarantined", pl.Boolean)
    # Small/pilot frames often infer all-null lineage columns as Null. The
    # hybrid builder must still be able to write an explicit quarantine reason.
    schema["return_quarantine_reason"] = pl.String
    schema["adjustment_source"] = pl.String
    schema["fallback_reason"] = pl.String
    schema.update(
        {
            "shioaji_trading_value": pl.Float64,
            "shioaji_volume_lots": pl.Float64,
            "shioaji_minute_bars": pl.Int64,
        }
    )
    output = pl.from_dicts(output_rows, schema=schema, strict=False).sort("date")
    if output.get_column("date").n_unique() != output.height:
        raise RuntimeError("hybrid symbol output contains duplicate dates")
    public_after = output.filter(
        (pl.col("date") >= pl.lit(effective_cutover))
        & (pl.col("data_source") != SHIOAJI_SOURCE_NAME)
    )
    invalid_public_after = public_after.filter(
        (pl.col("fallback_reason") != "shioaji_declared_source_gap")
        | ~pl.col("date").is_in(sorted(fallback_dates))
    )
    if invalid_public_after.height or public_after.height != len(fallback_dates):
        raise RuntimeError(
            "hybrid output retained public rows without declared Shioaji source gaps"
        )
    stats = {
        "public_rows": int(output.height - shioaji.height),
        "shioaji_rows": int(shioaji.height),
        "dropped_public_rows_after_cutover": int(
            len(base_dates_after - shioaji_dates - fallback_dates)
        ),
        "shioaji_only_rows": int(shioaji_only_rows),
        "quarantined_transitions": int(quarantined_transitions),
    }
    if fallback_dates:
        stats["public_source_gap_fallback_rows"] = len(fallback_dates)
    return output, stats


def _write_symbol(frame: pl.DataFrame, path: Path, *, cutover: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = frame.to_arrow()
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"stockagent.source": SOURCE_NAME.encode(),
            b"stockagent.asset_class": b"tw_stocks",
            b"stockagent.shioaji_cutover": cutover.isoformat().encode(),
            b"stockagent.first_date": str(frame["date"].min()).encode(),
            b"stockagent.last_date": str(frame["date"].max()).encode(),
            b"stockagent.write_ts_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .encode(),
        }
    )
    table = table.replace_schema_metadata(metadata)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
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
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def main() -> None:
    args = parse_args()
    cutover = date.fromisoformat(str(args.cutover_date))
    if cutover < DEFAULT_CUTOVER:
        raise ValueError(
            f"Shioaji stock history starts at {DEFAULT_CUTOVER}; got {cutover}"
        )
    symbols_path = args.base_stock_root / "symbols.csv"
    symbols = pl.read_csv(symbols_path, infer_schema_length=0)
    required = {"code", "security_type"}
    missing = sorted(required - set(symbols.columns))
    if missing:
        raise ValueError(f"{symbols_path} is missing columns: {missing}")
    target_symbols = [
        str(row["code"]).strip().upper()
        for row in symbols.iter_rows(named=True)
        if str(row["security_type"]).strip().lower() in {"stock", "etf"}
    ]
    if args.dry_run:
        print(
            f"[tw-shioaji-build] dry_run symbols={len(target_symbols)} "
            f"cutover={cutover} output={args.output_dir}",
            flush=True,
        )
        return
    download_summary = _validate_download_summary(
        args.shioaji_root, allow_incomplete=bool(args.allow_incomplete)
    )
    download_report = _load_report(args.shioaji_root)
    if not args.allow_incomplete:
        unaccounted = sorted(set(target_symbols) - set(download_report))
        if unaccounted:
            raise RuntimeError(
                f"Shioaji full-universe report is missing {len(unaccounted)} symbols: "
                f"{unaccounted[:20]}"
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results: list[BuildResult] = []
    for symbol in target_symbols:
        base_path = args.base_stock_root / f"{symbol}_features.parquet"
        output_path = args.output_dir / base_path.name
        report = download_report.get(symbol)
        daily_path = args.shioaji_root / "daily" / f"{symbol}.parquet"
        try:
            if report is None or str(report.get("status")) != "complete":
                if report is not None and str(report.get("status")) not in {
                    "contract_unavailable",
                    "not_yet_listed",
                    "outside_source_window",
                    "",
                }:
                    raise RuntimeError(
                        f"Shioaji symbol status is not buildable: {report.get('status')}"
                    )
                public_only_status = (
                    "public_only_not_yet_listed"
                    if report is not None
                    and str(report.get("status")) == "not_yet_listed"
                    else "public_only_outside_source_window"
                    if report is not None
                    and str(report.get("status")) == "outside_source_window"
                    else "public_only_contract_unavailable"
                )
                shutil.copy2(base_path, output_path)
                base = pl.read_parquet(base_path, columns=["date"])
                results.append(
                    BuildResult(
                        symbol=symbol,
                        status=public_only_status,
                        rows=base.height,
                        public_rows=base.height,
                        shioaji_rows=0,
                        dropped_public_rows_after_cutover=0,
                        shioaji_only_rows=0,
                        quarantined_transitions=0,
                        first_date=str(base["date"].min()),
                        last_date=str(base["date"].max()),
                        output_path=str(output_path),
                        message=(
                            str(report.get("message") or "") if report is not None else ""
                        ),
                    )
                )
                continue
            daily_summary = _validate_daily_receipt(daily_path)
            base = pl.read_parquet(base_path)
            shioaji = pl.read_parquet(daily_path)
            declared_source_gap_dates = {
                date.fromisoformat(str(value))
                for value in daily_summary.get("declared_source_gap_dates", [])
            }
            merged, stats = merge_symbol_frames(
                base,
                shioaji,
                cutover=cutover,
                declared_source_gap_dates=declared_source_gap_dates,
            )
            _write_symbol(merged, output_path, cutover=cutover)
            results.append(
                BuildResult(
                    symbol=symbol,
                    status="hybrid",
                    rows=merged.height,
                    first_date=str(merged["date"].min()),
                    last_date=str(merged["date"].max()),
                    output_path=str(output_path),
                    **stats,
                )
            )
        except Exception as exc:
            results.append(
                BuildResult(
                    symbol=symbol,
                    status="failed",
                    rows=0,
                    public_rows=0,
                    shioaji_rows=0,
                    dropped_public_rows_after_cutover=0,
                    shioaji_only_rows=0,
                    quarantined_transitions=0,
                    first_date=None,
                    last_date=None,
                    output_path=str(output_path),
                    message=f"{type(exc).__name__}: {exc}",
                )
            )
    failed = [item for item in results if item.status == "failed"]
    report_path = args.output_dir / "shioaji_dataset_report.csv"
    pl.DataFrame([asdict(item) for item in results]).sort("symbol").write_csv(report_path)
    if failed:
        raise RuntimeError(
            f"TW Shioaji dataset build failed for {len(failed)} symbols: "
            f"{[asdict(item) for item in failed[:10]]}"
        )
    shutil.copy2(symbols_path, args.output_dir / "symbols.csv")
    provenance = {
        "schema_version": 1,
        "symbols": {
            item.symbol: {
                "kind": (
                    "public_adjustment_factor_with_shioaji_quotes"
                    if item.status == "hybrid"
                    else "public_reference_index_unchanged_not_yet_listed"
                    if item.status == "public_only_not_yet_listed"
                    else "public_reference_index_unchanged_outside_source_window"
                    if item.status == "public_only_outside_source_window"
                    else "public_reference_index_unchanged_contract_unavailable"
                ),
                "source": SOURCE_NAME if item.status == "hybrid" else "tw_public",
                "updated_at_utc": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat(),
            }
            for item in results
        },
    }
    _atomic_write_json(args.output_dir / "return_price_provenance.json", provenance)
    summary = {
        "schema_version": 1,
        "source": SOURCE_NAME,
        "cutover_date": cutover.isoformat(),
        "quote_contract": (
            "For each covered symbol, public rows precede its first Shioaji daily "
            "bar; from that date onward only Shioaji quote rows remain."
        ),
        "feature_contract": (
            "Point-in-time fundamentals, execution rules, and corporate-action "
            "evidence remain inherited from tw_public."
        ),
        "symbols": len(results),
        "hybrid_symbols": sum(item.status == "hybrid" for item in results),
        "public_only_contract_unavailable_symbols": sum(
            item.status == "public_only_contract_unavailable" for item in results
        ),
        "public_only_not_yet_listed_symbols": sum(
            item.status == "public_only_not_yet_listed" for item in results
        ),
        "public_only_outside_source_window_symbols": sum(
            item.status == "public_only_outside_source_window" for item in results
        ),
        "rows": sum(item.rows for item in results),
        "public_rows": sum(item.public_rows for item in results),
        "shioaji_rows": sum(item.shioaji_rows for item in results),
        "dropped_public_rows_after_cutover": sum(
            item.dropped_public_rows_after_cutover for item in results
        ),
        "shioaji_only_rows": sum(item.shioaji_only_rows for item in results),
        "quarantined_transitions": sum(
            item.quarantined_transitions for item in results
        ),
        "public_source_gap_fallback_rows": sum(
            item.public_source_gap_fallback_rows for item in results
        ),
        "download_summary_receipt": {
            "path": str(args.shioaji_root / "download_summary.json"),
            "size": int((args.shioaji_root / "download_summary.json").stat().st_size),
            "sha256": _sha256(args.shioaji_root / "download_summary.json"),
        },
        "download_end_date": download_summary.get("end_date"),
        "report_path": str(report_path),
        "written_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    _atomic_write_json(args.output_dir / "shioaji_dataset_summary.json", summary)
    print(
        f"[tw-shioaji-build] symbols={len(results)} "
        f"hybrid={summary['hybrid_symbols']} "
        f"public_only_unavailable={summary['public_only_contract_unavailable_symbols']} "
        f"public_only_not_yet_listed={summary['public_only_not_yet_listed_symbols']} "
        f"public_only_outside_window="
        f"{summary['public_only_outside_source_window_symbols']} "
        f"source_gap_fallback_rows={summary['public_source_gap_fallback_rows']} "
        f"shioaji_rows={summary['shioaji_rows']} "
        f"summary={args.output_dir / 'shioaji_dataset_summary.json'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
