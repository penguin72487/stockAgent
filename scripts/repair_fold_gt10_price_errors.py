from __future__ import annotations

import argparse
import math
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import polars as pl
import pyarrow.parquet as pq


PRICE_COLUMNS = ("open", "max", "min", "close", "adjclose")
CORPORATE_ACTION_COLUMNS = ("Dividends", "Stock Splits", "Capital Gains")


@dataclass(frozen=True)
class RepairAction:
    symbol: str
    method: str
    reason: str
    dates: tuple[str, ...] = ()
    factor: float | None = None
    start_date: str | None = None
    end_date: str | None = None
    columns: tuple[str, ...] = PRICE_COLUMNS


AUTO_REPAIRS: tuple[RepairAction, ...] = (
    RepairAction(
        symbol="1752",
        method="scale_decimal_shift_rows",
        dates=("2025-01-10",),
        factor=0.01,
        reason="OHLC/AdjClose is 100x the neighboring price level; scale the bad row back by 0.01.",
    ),
    RepairAction(
        symbol="3114",
        method="scale_decimal_shift_rows",
        dates=("2025-04-25",),
        factor=0.01,
        reason="OHLC/AdjClose is 100x the neighboring price level; scale the bad row back by 0.01.",
    ),
    RepairAction(
        symbol="00636K",
        method="scale_short_bad_segment",
        dates=("2017-11-06", "2017-11-07"),
        factor=1.0 / 3.0,
        reason="Two-day segment is near exactly 3x surrounding ETF prices and then reverts; scale segment by one third.",
    ),
    RepairAction(
        symbol="00657K",
        method="scale_short_bad_segment",
        dates=("2017-11-06", "2017-11-07"),
        factor=1.0 / 3.0,
        reason="Two-day segment is near exactly 3x surrounding ETF prices and then reverts; scale segment by one third.",
    ),
    RepairAction(
        symbol="2528",
        method="interpolate_zero_volume_single_row",
        dates=("2009-01-12",),
        reason="Zero-volume singleton price is far below identical neighboring adjusted levels; replace by adjacent-row interpolation.",
    ),
    RepairAction(
        symbol="3555",
        method="interpolate_single_day_spike",
        dates=("2009-12-03",),
        reason="Single-day price spike is inconsistent with both adjacent days and has no corporate-action marker; interpolate from neighbors.",
    ),
    RepairAction(
        symbol="6225",
        method="interpolate_single_day_spike",
        dates=("2006-10-26",),
        reason="Single-day price spike is inconsistent with both adjacent days and has no corporate-action marker; interpolate from neighbors.",
    ),
    RepairAction(
        symbol="6225",
        method="scale_short_bad_segment",
        dates=("2007-01-29", "2007-01-30"),
        factor=0.60,
        reason="Two-day segment jumps to about 1.7x surrounding prices and reverts; scale the short bad segment back near local continuity.",
    ),
)

MANUAL_REVIEW_SYMBOLS: dict[str, str] = {
    "00887": (
        "ETF had an extreme multi-day 2024 move with very large volume. It is suspicious, "
        "but not a clean decimal-point or short bad-segment repair; verify against exchange/vendor data first."
    ),
    "2540": (
        "The 2005-09-28 -> 2005-09-29 break looks like a missing split/capital-reduction adjustment, "
        "but older history has additional scale problems. Rebuild this symbol from a trusted corporate-action source."
    ),
    "4989": (
        "The 2014-08-14 -> 2014-08-15 break may be a missing adjustment boundary, but the surrounding period is highly volatile; "
        "verify source data before applying a historical range factor."
    ),
    "6283": (
        "Symbol has repeated alternating price regimes around 2008-2010. The safe repair is a symbol-level "
        "source refetch or corporate-action reconstruction, not local interpolation."
    ),
    "8066": (
        "The 2012-09-07 -> 2012-09-10 break looks like a missing reverse adjustment with zero-volume stale prices; "
        "verify/reconstruct the symbol history before applying a range factor."
    ),
}


def _safe_float(value: object) -> float:
    try:
        if _is_null(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _is_null(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(np.isnan(value))  # type: ignore[arg-type]
    except TypeError:
        return False


def _format_date(value: object) -> str | None:
    if _is_null(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return text[:10]


def _read_parquet(path: Path) -> pl.DataFrame:
    return pl.from_arrow(pq.read_table(path))


def _write_parquet(frame: pl.DataFrame, path: Path) -> None:
    pq.write_table(frame.to_arrow(), path)


def _method_counts(frame: pl.DataFrame) -> dict[str, int]:
    if frame.is_empty() or "repair_method" not in frame.columns:
        return {}
    counts = frame.group_by("repair_method").len(name="count").sort("repair_method")
    return {str(row["repair_method"]): int(row["count"]) for row in counts.iter_rows(named=True)}


def _candidate_rows(investigation: pl.DataFrame, min_abs_log_return: float) -> pl.DataFrame:
    raw_expr = pl.col("raw_close_logret").cast(pl.Float64, strict=False) if "raw_close_logret" in investigation.columns else pl.lit(None)
    adj_expr = pl.col("adjclose_logret").cast(pl.Float64, strict=False) if "adjclose_logret" in investigation.columns else pl.lit(None)
    work = investigation.with_columns(
        [
            raw_expr.alias("__raw_close_logret"),
            adj_expr.alias("__adjclose_logret"),
        ]
    )
    extreme = (
        pl.col("__raw_close_logret").abs().ge(min_abs_log_return).fill_null(False)
        | pl.col("__adjclose_logret").abs().ge(min_abs_log_return).fill_null(False)
    )

    has_action = pl.lit(False)
    for col in ("dividends", "stock_splits", "capital_gains"):
        if col in work.columns:
            has_action = has_action | pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0).abs().gt(1e-12)

    return work.filter(extreme & ~has_action).drop(["__raw_close_logret", "__adjclose_logret"])


def _collect_event_targets(candidates: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in candidates.iter_rows(named=True):
        symbol = str(row["symbol"])
        for endpoint in ("raw_date", "raw_next_date"):
            date_value = row.get(endpoint)
            date = _format_date(date_value)
            if date is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "date": date,
                    "source_fold": row.get("fold"),
                    "source_event_date": row.get("date"),
                    "endpoint": endpoint,
                    "portfolio_log_return": row.get("portfolio_log_return"),
                    "portfolio_simple_return": row.get("portfolio_simple_return"),
                    "panel_return_1d_log": row.get("panel_return_1d_log"),
                    "raw_close_logret": row.get("raw_close_logret"),
                    "adjclose_logret": row.get("adjclose_logret"),
                    "price_anomaly_reason": row.get("price_anomaly_reason"),
                    "raw_event_flags": row.get("raw_event_flags"),
                }
            )
    if not rows:
        return pl.DataFrame()
    targets = pl.DataFrame(rows)
    return targets.unique(subset=["symbol", "date"], maintain_order=True).sort(["symbol", "date"])


def _date_values(frame: pl.DataFrame) -> list[str | None]:
    if "date" not in frame.columns:
        return []
    return [_format_date(value) for value in frame.get_column("date").to_list()]


def _row_indices(frame: pl.DataFrame, dates: Iterable[str]) -> list[int]:
    wanted = {str(d) for d in dates}
    return [idx for idx, value in enumerate(_date_values(frame)) if value in wanted]


def _range_indices(frame: pl.DataFrame, start_date: str | None, end_date: str | None) -> list[int]:
    out: list[int] = []
    for idx, value in enumerate(_date_values(frame)):
        if value is None:
            continue
        if start_date is not None and value < start_date:
            continue
        if end_date is not None and value > end_date:
            continue
        out.append(idx)
    return out


def _record_values(
    frame: pl.DataFrame,
    idx: int,
    *,
    action: RepairAction,
    old_values: dict[str, object],
    new_values: dict[str, object],
) -> dict[str, object]:
    row = frame.row(idx, named=True)
    date = _format_date(row.get("date")) or str(row.get("date"))
    record: dict[str, object] = {
        "symbol": action.symbol,
        "date": date,
        "row_index": idx,
        "repair_method": action.method,
        "repair_reason": action.reason,
        "factor": action.factor,
        "start_date": action.start_date,
        "end_date": action.end_date,
        "applied_columns": ",".join(action.columns),
    }
    for col in PRICE_COLUMNS:
        if col in old_values:
            record[f"old_{col}"] = old_values[col]
        if col in new_values:
            record[f"new_{col}"] = new_values[col]
    for col in CORPORATE_ACTION_COLUMNS:
        if col in frame.columns:
            record[col] = row.get(col)
    if "Trading_Volume" in frame.columns:
        record["Trading_Volume"] = row.get("Trading_Volume")
    return record


def _interpolated_values(frame: pl.DataFrame, idx: int, columns: Iterable[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    prev_idx = idx - 1
    next_idx = idx + 1
    for col in columns:
        if col not in frame.columns:
            continue
        prev_val = _safe_float(frame.row(prev_idx, named=True).get(col)) if prev_idx >= 0 else float("nan")
        next_val = _safe_float(frame.row(next_idx, named=True).get(col)) if next_idx < frame.height else float("nan")
        vals = [v for v in (prev_val, next_val) if np.isfinite(v)]
        out[col] = float(np.mean(vals)) if vals else float("nan")
    return out


def _apply_action(frame: pl.DataFrame, action: RepairAction, *, apply: bool) -> tuple[list[dict[str, object]], pl.DataFrame]:
    if "date" not in frame.columns:
        return [
            {
                "symbol": action.symbol,
                "repair_method": "missing_date_column_no_change",
                "repair_reason": action.reason,
            }
        ], frame

    if action.dates:
        indices = _row_indices(frame, action.dates)
    else:
        indices = _range_indices(frame, action.start_date, action.end_date)
    if not indices:
        return [
            {
                "symbol": action.symbol,
                "repair_method": "target_dates_missing_no_change",
                "repair_reason": action.reason,
                "target_dates": ",".join(action.dates),
                "start_date": action.start_date,
                "end_date": action.end_date,
            }
        ], frame

    records: list[dict[str, object]] = []
    rows = frame.to_dicts() if apply else []
    for idx in indices:
        row = frame.row(idx, named=True)
        old_values = {col: row.get(col) for col in action.columns if col in frame.columns}
        if action.method.startswith("interpolate_"):
            new_values = _interpolated_values(frame, idx, action.columns)
        elif action.factor is not None:
            new_values = {
                col: (_safe_float(row.get(col)) * float(action.factor))
                for col in action.columns
                if col in frame.columns
            }
        else:
            new_values = {}

        records.append(_record_values(frame, idx, action=action, old_values=old_values, new_values=new_values))
        if apply:
            for col, value in new_values.items():
                rows[idx][col] = value
    repaired = pl.DataFrame(rows, schema=frame.schema, orient="row") if apply else frame
    return records, repaired


def _manual_review_records(targets: pl.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    auto_symbols = {a.symbol for a in AUTO_REPAIRS}
    if targets.is_empty() or "symbol" not in targets.columns:
        return records
    for symbol in sorted(str(value) for value in targets.get_column("symbol").unique().to_list()):
        group = targets.filter(pl.col("symbol").cast(pl.String) == symbol)
        if symbol not in MANUAL_REVIEW_SYMBOLS:
            continue
        reason = MANUAL_REVIEW_SYMBOLS[symbol]
        for row in group.iter_rows(named=True):
            rec = dict(row)
            rec["repair_method"] = "manual_review_no_change"
            rec["repair_reason"] = reason
            records.append(rec)

    known = auto_symbols | set(MANUAL_REVIEW_SYMBOLS)
    for symbol in sorted(str(value) for value in targets.get_column("symbol").unique().to_list()):
        group = targets.filter(pl.col("symbol").cast(pl.String) == symbol)
        if symbol in known:
            continue
        for row in group.iter_rows(named=True):
            rec = dict(row)
            rec["repair_method"] = "unclassified_no_change"
            rec["repair_reason"] = "No curated repair rule yet; inspect before mutating source data."
            records.append(rec)
    return records


def repair(
    *,
    data_root: Path,
    investigation_path: Path,
    output_dir: Path,
    backup_dir: Path,
    min_abs_log_return: float,
    apply: bool,
) -> Path:
    investigation = pl.read_csv(investigation_path, infer_schema=False).with_columns(pl.col("symbol").cast(pl.String))
    candidates = _candidate_rows(investigation, min_abs_log_return)
    targets = _collect_event_targets(candidates)
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ledger_path = output_dir / f"fold_gt10_price_error_repairs_{timestamp}.csv"
    summary_path = output_dir / f"fold_gt10_price_error_repairs_{timestamp}.md"
    symbol_backup_dir = backup_dir / timestamp

    records: list[dict[str, object]] = []
    touched_symbols: list[str] = []
    for action in AUTO_REPAIRS:
        parquet_path = data_root / f"{action.symbol}_features.parquet"
        if not parquet_path.exists():
            records.append(
                {
                    "symbol": action.symbol,
                    "repair_method": "missing_symbol_parquet_no_change",
                    "repair_reason": action.reason,
                }
            )
            continue

        frame = _read_parquet(parquet_path)
        action_records, repaired = _apply_action(frame, action, apply=apply)
        records.extend(action_records)
        changed = apply and any(
            rec.get("repair_method") == action.method and any(str(k).startswith("new_") for k in rec)
            for rec in action_records
        )
        if changed:
            symbol_backup_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_path, symbol_backup_dir / parquet_path.name)
            _write_parquet(repaired, parquet_path)
            touched_symbols.append(action.symbol)

    records.extend(_manual_review_records(targets))
    ledger = pl.DataFrame(records) if records else pl.DataFrame()
    ledger.write_csv(ledger_path)

    method_counts = _method_counts(ledger)
    lines = [
        "# Fold >10% Daily Return Price Repair Ledger",
        "",
        f"- apply: `{apply}`",
        f"- investigation: `{investigation_path}`",
        f"- min_abs_symbol_log_return: `{min_abs_log_return:.6f}`",
        f"- candidate extreme contribution rows: `{candidates.height}`",
        f"- unique symbol/date event targets: `{targets.height}`",
        f"- touched parquet symbols: `{len(set(touched_symbols))}`",
        f"- backup_dir: `{symbol_backup_dir}`",
        "",
        "## Method",
        "",
        "This repair is value-preserving where the error is diagnosable: decimal-shift rows are scaled, "
        "short bad segments are scaled as a group, isolated single-day spikes are interpolated from adjacent rows, "
        "and missing adjustment boundaries scale the historical side of the boundary so returns stay continuous. "
        "Ambiguous symbols are explicitly logged as manual review and left unchanged.",
        "",
        "## Method Counts",
        "",
    ]
    for method, count in sorted(method_counts.items()):
        lines.append(f"- `{method}`: `{count}`")
    lines.extend(
        [
            "",
            "## Touched Symbols",
            "",
            ", ".join(sorted(set(touched_symbols))) if touched_symbols else "(none)",
            "",
            "## Manual Review Symbols",
            "",
        ]
    )
    for symbol, reason in sorted(MANUAL_REVIEW_SYMBOLS.items()):
        lines.append(f"- `{symbol}`: {reason}")
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"ledger={ledger_path}")
    print(f"summary={summary_path}")
    print(f"event_targets={len(targets)} touched_symbols={len(set(touched_symbols))} apply={apply}")
    return ledger_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair diagnosable fold >10% daily-return price data errors.")
    parser.add_argument("--data-root", type=Path, default=Path("data_yahoo/tw_stocks"))
    parser.add_argument("--investigation", type=Path, default=Path("artifacts/daily_return_gt10_investigation.csv"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_logs"))
    parser.add_argument("--backup-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_backups"))
    parser.add_argument("--min-abs-log-return", type=float, default=math.log(1.5))
    parser.add_argument("--apply", action="store_true", help="Mutate parquet files. Without this flag only writes a repair plan.")
    args = parser.parse_args()
    repair(
        data_root=args.data_root,
        investigation_path=args.investigation,
        output_dir=args.output_dir,
        backup_dir=args.backup_dir,
        min_abs_log_return=float(args.min_abs_log_return),
        apply=bool(args.apply),
    )


if __name__ == "__main__":
    main()
