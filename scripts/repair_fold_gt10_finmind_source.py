from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import requests


PRICE_COLUMNS = ("open", "max", "min", "close", "adjclose")


@dataclass(frozen=True)
class SourceRepair:
    symbol: str
    start_date: str
    end_date: str
    mode: str
    reason: str


SOURCE_REPAIRS: tuple[SourceRepair, ...] = (
    SourceRepair(
        symbol="2540",
        start_date="2000-01-01",
        end_date="2026-06-10",
        mode="replace_positive_finmind_ohlc_full_history",
        reason=(
            "Yahoo source has large scale discontinuities around 2005-09-28/2005-09-29. "
            "FinMind provides continuous exchange OHLC; replace positive FinMind rows and use FinMind close as adjclose."
        ),
    ),
    SourceRepair(
        symbol="6283",
        start_date="2007-09-01",
        end_date="2026-06-10",
        mode="replace_positive_finmind_ohlc_full_history",
        reason=(
            "Yahoo source alternates between incompatible price regimes during 2008-2010. "
            "FinMind provides continuous exchange OHLC; replace positive FinMind rows and use FinMind close as adjclose."
        ),
    ),
    SourceRepair(
        symbol="8066",
        start_date="2012-09-01",
        end_date="2012-09-18",
        mode="nan_finmind_zero_price_rows",
        reason=(
            "FinMind shows 2012-09-10 to 2012-09-12 as zero-price rows, so Yahoo stale nonzero prices should not "
            "create tradable forward returns."
        ),
    ),
)


def _is_null(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(np.isnan(value))  # type: ignore[arg-type]
    except TypeError:
        return False


def _safe_float(value: object) -> float:
    if _is_null(value):
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


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


def _fetch_finmind(symbol: str, start_date: str, end_date: str) -> pl.DataFrame:
    response = requests.get(
        "https://api.finmindtrade.com/api/v4/data",
        params={
            "dataset": "TaiwanStockPrice",
            "data_id": symbol,
            "start_date": start_date,
            "end_date": end_date,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") not in (None, 200) and payload.get("msg") != "success":
        raise RuntimeError(f"FinMind returned non-success payload for {symbol}: {payload!r}")
    data = payload.get("data") or []
    frame = pl.DataFrame(data)
    if frame.is_empty():
        return frame
    if "date" in frame.columns:
        frame = frame.with_columns(pl.col("date").cast(pl.String).str.slice(0, 10).alias("date"))
    numeric_exprs = []
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        if col in frame.columns:
            numeric_exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
    return frame.with_columns(numeric_exprs) if numeric_exprs else frame


def _record_change(
    *,
    symbol: str,
    date: str,
    mode: str,
    reason: str,
    old_row: dict[str, object],
    new_values: dict[str, float],
    finmind_row: dict[str, object],
) -> dict[str, object]:
    rec: dict[str, object] = {
        "symbol": symbol,
        "date": date,
        "repair_method": mode,
        "repair_reason": reason,
        "source": "FinMind TaiwanStockPrice",
    }
    for col in PRICE_COLUMNS:
        if col in old_row:
            rec[f"old_{col}"] = old_row.get(col)
        if col in new_values:
            rec[f"new_{col}"] = new_values[col]
    if "Trading_Volume" in old_row:
        rec["old_Trading_Volume"] = old_row.get("Trading_Volume")
    if "Trading_Volume" in new_values:
        rec["new_Trading_Volume"] = new_values["Trading_Volume"]
    for col in ("open", "max", "min", "close", "Trading_Volume"):
        if col in finmind_row:
            rec[f"finmind_{col}"] = finmind_row.get(col)
    return rec


def _apply_source_repair(frame: pl.DataFrame, finmind: pl.DataFrame, action: SourceRepair, *, apply: bool) -> tuple[list[dict[str, object]], pl.DataFrame]:
    if finmind.is_empty():
        return [
            {
                "symbol": action.symbol,
                "repair_method": "finmind_empty_no_change",
                "repair_reason": action.reason,
            }
        ], frame
    frame = frame.sort("date") if "date" in frame.columns else frame
    frame_rows = frame.to_dicts()
    frame_dates = [_format_date(row.get("date")) for row in frame_rows]
    index_by_date = {date: int(idx) for idx, date in enumerate(frame_dates) if date is not None}

    records: list[dict[str, object]] = []
    repaired_rows = [dict(row) for row in frame_rows] if apply else []
    for src in finmind.iter_rows(named=True):
        date = str(src["date"])
        idx = index_by_date.get(date)
        if idx is None:
            continue

        open_px = _safe_float(src.get("open"))
        high_px = _safe_float(src.get("max"))
        low_px = _safe_float(src.get("min"))
        close_px = _safe_float(src.get("close"))
        volume = _safe_float(src.get("Trading_Volume"))

        if action.mode == "replace_positive_finmind_ohlc_full_history":
            if not all(np.isfinite(v) and v > 0.0 for v in (open_px, high_px, low_px, close_px)):
                continue
            new_values = {
                "open": open_px,
                "max": high_px,
                "min": low_px,
                "close": close_px,
                "adjclose": close_px,
                "Trading_Volume": volume,
            }
        elif action.mode == "nan_finmind_zero_price_rows":
            if not all(np.isfinite(v) and abs(v) <= 1e-12 for v in (open_px, high_px, low_px, close_px)):
                continue
            new_values = {
                "open": np.nan,
                "max": np.nan,
                "min": np.nan,
                "close": np.nan,
                "adjclose": np.nan,
                "Trading_Volume": volume,
            }
        else:
            raise ValueError(f"Unknown mode: {action.mode}")

        old_row = frame_rows[idx]
        changed = False
        for col, value in new_values.items():
            if col not in frame.columns:
                continue
            old_value = old_row.get(col)
            if _is_null(old_value) and _is_null(value):
                continue
            if not np.isclose(float(old_value), float(value), rtol=0.0, atol=1e-8) if not _is_null(old_value) and not _is_null(value) else True:
                changed = True
                break
        if not changed:
            continue

        records.append(
            _record_change(
                symbol=action.symbol,
                date=date,
                mode=action.mode,
                reason=action.reason,
                old_row=old_row,
                new_values=new_values,
                finmind_row=src,
            )
        )
        if apply:
            for col, value in new_values.items():
                if col in frame.columns:
                    repaired_rows[idx][col] = value
    repaired = pl.DataFrame(repaired_rows, schema=frame.schema, orient="row") if apply else frame
    return records, repaired


def repair(*, data_root: Path, output_dir: Path, backup_dir: Path, apply: bool) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    backup_target = backup_dir / timestamp
    ledger_path = output_dir / f"fold_gt10_finmind_source_repairs_{timestamp}.csv"
    summary_path = output_dir / f"fold_gt10_finmind_source_repairs_{timestamp}.md"

    all_records: list[dict[str, object]] = []
    touched: list[str] = []
    for action in SOURCE_REPAIRS:
        parquet_path = data_root / f"{action.symbol}_features.parquet"
        if not parquet_path.exists():
            all_records.append(
                {
                    "symbol": action.symbol,
                    "repair_method": "missing_symbol_parquet_no_change",
                    "repair_reason": action.reason,
                }
            )
            continue
        frame = _read_parquet(parquet_path)
        finmind = _fetch_finmind(action.symbol, action.start_date, action.end_date)
        records, repaired = _apply_source_repair(frame, finmind, action, apply=apply)
        all_records.extend(records)
        if apply and records:
            backup_target.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_path, backup_target / parquet_path.name)
            _write_parquet(repaired, parquet_path)
            touched.append(action.symbol)

    ledger = pl.DataFrame(all_records) if all_records else pl.DataFrame()
    ledger.write_csv(ledger_path)
    method_counts = _method_counts(ledger)
    lines = [
        "# FinMind Source Repairs For Fold >10% Events",
        "",
        f"- apply: `{apply}`",
        f"- touched_symbols: `{len(set(touched))}`",
        f"- backup_dir: `{backup_target}`",
        "",
        "## Method Counts",
        "",
    ]
    for method, count in sorted(method_counts.items()):
        lines.append(f"- `{method}`: `{count}`")
    lines.extend(
        [
            "",
            "## Note",
            "",
            "FinMind TaiwanStockPrice provides exchange OHLC, not a fully reconstructed total-return adjusted close. "
            "For symbols where Yahoo adjclose is corrupted, this repair uses FinMind close as adjclose to remove fake "
            "training labels. A later dividend/split total-return reconstruction can improve this further.",
        ]
    )
    summary_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"ledger={ledger_path}")
    print(f"summary={summary_path}")
    print(f"records={len(all_records)} touched_symbols={len(set(touched))} apply={apply}")
    return ledger_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair selected fold >10% source rows using FinMind.")
    parser.add_argument("--data-root", type=Path, default=Path("data_yahoo/tw_stocks"))
    parser.add_argument("--output-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_logs"))
    parser.add_argument("--backup-dir", type=Path, default=Path("data_yahoo/tw_stocks/repair_backups/finmind_source"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    repair(data_root=args.data_root, output_dir=args.output_dir, backup_dir=args.backup_dir, apply=bool(args.apply))


if __name__ == "__main__":
    main()
