from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class CapitalScale:
    scale: float = 1.0
    mode: str = "artifact"
    capital: float | None = None
    reference_nav: float | None = None
    reference_date: str | None = None


def positive_float_or_none(value: Any) -> float | None:
    try:
        number = float(value)
    except Exception:
        return None
    if number > 0.0:
        return number
    return None


def resolve_capital_scale_from_nav(
    nav_rows: list[dict[str, Any]],
    *,
    initial_capital: float | None = None,
    current_capital: float | None = None,
) -> CapitalScale:
    if not nav_rows:
        return CapitalScale()
    rows = sorted(nav_rows, key=lambda row: str(row.get("date") or ""))
    current = positive_float_or_none(current_capital)
    initial = positive_float_or_none(initial_capital)
    if current is not None:
        row = rows[-1]
        nav = positive_float_or_none(row.get("nav"))
        if nav is not None:
            return CapitalScale(
                scale=current / nav,
                mode="current_capital",
                capital=current,
                reference_nav=nav,
                reference_date=str(row.get("date") or ""),
            )
    if initial is not None:
        row = rows[0]
        nav = positive_float_or_none(row.get("nav"))
        if nav is not None:
            return CapitalScale(
                scale=initial / nav,
                mode="initial_capital",
                capital=initial,
                reference_nav=nav,
                reference_date=str(row.get("date") or ""),
            )
    return CapitalScale()


def _artifact_path(fold_dir: str | Path, stem: str) -> Path | None:
    root = Path(fold_dir)
    for suffix in (".parquet", ".csv"):
        path = root / f"{stem}{suffix}"
        if path.exists():
            return path
    return None


def load_fold_nav_rows(fold_dir: str | Path) -> list[dict[str, Any]]:
    path = _artifact_path(fold_dir, "holdings")
    if path is None:
        return []
    import polars as pl

    frame = pl.read_parquet(path) if path.suffix.lower() == ".parquet" else pl.read_csv(path, infer_schema_length=10000)
    if "date" not in frame.columns or "market_value" not in frame.columns:
        return []
    return (
        frame.select(
            [
                pl.col("date").cast(pl.Utf8).str.slice(0, 10).alias("date"),
                pl.col("market_value").cast(pl.Float64, strict=False).fill_null(0.0).alias("market_value"),
            ]
        )
        .group_by("date")
        .agg(pl.col("market_value").sum().alias("nav"))
        .sort("date")
        .to_dicts()
    )


def resolve_fold_capital_scale(
    fold_dir: str | Path,
    *,
    initial_capital: float | None = None,
    current_capital: float | None = None,
) -> CapitalScale:
    return resolve_capital_scale_from_nav(
        load_fold_nav_rows(fold_dir),
        initial_capital=initial_capital,
        current_capital=current_capital,
    )
