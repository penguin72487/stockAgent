#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import math
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.panel import (  # noqa: E402
    EPSILON,
    FEATURE_FILE_SUFFIX,
    LOG_RETURN_FEATURE_COLUMNS,
    PanelData,
    build_panel,
    _load_symbol_frame_cudf,
    _prepare_symbol_frame,
    _price_decimals_for_path,
    _resolve_benchmark_index,
    _symbol_name_from_path,
)
from stockagent.data.panel_cache import save_panel_cache_v2  # noqa: E402


SCAN_PATTERNS: dict[str, list[str]] = {
    "panel_ingest": [
        "read_parquet",
        "build_panel",
        "_prepare_symbol_frame",
        "panel_backend",
        "panel_load_workers",
    ],
    "table_output": [
        "to_parquet",
        "to_csv",
        "_write_dataframe_table",
        "table_output_format",
        "save_daily_weights",
    ],
    "downloader_merge": [
        "merge_existing",
        "pd.concat",
        "drop_duplicates",
        "read_csv",
        "read_html",
    ],
    "dataframe_compute": [
        "DataFrame",
        "groupby",
        "merge(",
        "concat(",
        "pd.to_numeric",
        "pd.to_datetime",
    ],
}


@dataclass(slots=True)
class Hotspot:
    category: str
    path: str
    line: int
    text: str


@dataclass(slots=True)
class SymbolStats:
    rows: int = 0
    feature_sum: float = 0.0
    return_sum: float = 0.0
    tradable_count: int = 0

    def add(self, other: "SymbolStats") -> None:
        self.rows += int(other.rows)
        self.feature_sum += float(other.feature_sum)
        self.return_sum += float(other.return_sum)
        self.tradable_count += int(other.tradable_count)

    @property
    def checksum(self) -> float:
        return float(self.feature_sum + self.return_sum * 7.0 + self.tradable_count * 1e-6)


@dataclass(slots=True)
class BenchmarkResult:
    workload: str
    backend: str
    available: bool
    files: int = 0
    rows: int = 0
    repeat: int = 1
    elapsed_s: float | None = None
    rows_per_s: float | None = None
    files_per_s: float | None = None
    checksum: float | None = None
    output_bytes: int | None = None
    error: str | None = None


def scan_data_processing_hotspots(root: Path) -> list[Hotspot]:
    regex_by_category = {
        category: re.compile("|".join(re.escape(pattern) for pattern in patterns), re.IGNORECASE)
        for category, patterns in SCAN_PATTERNS.items()
    }
    hotspots: list[Hotspot] = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] in {".git", "__pycache__"}:
            continue
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for idx, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            for category, regex in regex_by_category.items():
                if regex.search(stripped):
                    hotspots.append(
                        Hotspot(
                            category=category,
                            path=str(rel),
                            line=idx,
                            text=stripped[:180],
                        )
                    )
                    break
    return hotspots


def _module_available(name: str) -> bool:
    try:
        __import__(name)
    except Exception:
        return False
    return True


def _stats_from_pandas_frame(frame: pd.DataFrame) -> SymbolStats:
    feature_cols = [col for col in LOG_RETURN_FEATURE_COLUMNS if col in frame.columns]
    feature_sum = 0.0
    if feature_cols:
        values = frame[feature_cols].to_numpy(dtype=np.float64, copy=False)
        feature_sum = float(np.nansum(values))
    return SymbolStats(
        rows=int(len(frame)),
        feature_sum=feature_sum,
        return_sum=float(np.nansum(frame.get("return_1d", pd.Series(dtype=float)).to_numpy(dtype=np.float64))),
        tradable_count=int(np.asarray(frame.get("tradable", pd.Series(dtype=bool)), dtype=bool).sum()),
    )


def _bench_pandas_file(path: Path) -> SymbolStats:
    frame = _prepare_symbol_frame(pd.read_parquet(path), path)
    return _stats_from_pandas_frame(frame)


def _bench_cudf_file(path: Path) -> SymbolStats:
    frame = _load_symbol_frame_cudf(path)
    return _stats_from_pandas_frame(frame)


def _safe_log_np(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    num = np.asarray(numerator, dtype=np.float64)
    den = np.asarray(denominator, dtype=np.float64)
    out = np.full(num.shape, np.nan, dtype=np.float64)
    valid = np.isfinite(num) & np.isfinite(den) & (num > 0.0) & (den > 0.0)
    np.divide(num, den, out=out, where=valid)
    np.log(out, out=out, where=valid)
    out[~valid] = np.nan
    return out


def _shift_np(values: np.ndarray, offset: int) -> np.ndarray:
    out = np.full(values.shape, np.nan, dtype=np.float64)
    if offset > 0:
        out[offset:] = values[:-offset]
    elif offset < 0:
        out[:offset] = values[-offset:]
    else:
        out[:] = values
    return out


def _numeric_arrow_column(table: Any, name: str, rows: int) -> np.ndarray:
    if name not in table.column_names:
        return np.full(rows, np.nan, dtype=np.float64)
    values = table[name].combine_chunks().to_numpy(zero_copy_only=False)
    try:
        return np.asarray(values, dtype=np.float64)
    except (TypeError, ValueError):
        return pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype=np.float64)


def _bench_pyarrow_file(path: Path) -> SymbolStats:
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    rows = int(table.num_rows)
    if rows == 0:
        return SymbolStats()

    order: np.ndarray | None = None
    if "date" in table.column_names:
        date_values = table["date"].combine_chunks().to_numpy(zero_copy_only=False)
        try:
            order = np.argsort(np.asarray(date_values, dtype="datetime64[ns]"))
        except (TypeError, ValueError):
            order = np.argsort(pd.to_datetime(pd.Series(date_values), errors="coerce").to_numpy())

    def col(name: str) -> np.ndarray:
        values = _numeric_arrow_column(table, name, rows)
        return values[order] if order is not None else values

    decimals = _price_decimals_for_path(path)
    open_px = np.round(col("open"), decimals=decimals)
    high_px = np.round(col("max"), decimals=decimals)
    low_px = np.round(col("min"), decimals=decimals)
    close_px = np.round(col("close"), decimals=decimals)
    adjclose = np.round(col("adjclose"), decimals=decimals)
    volume = col("Trading_Volume")

    spread = np.clip(high_px - low_px, 0.0, None)
    denom = spread + EPSILON
    intraday_return_co = _safe_log_np(close_px, open_px)
    body_ratio = np.abs(close_px - open_px) / denom
    signed_body_ratio = (close_px - open_px) / denom
    clv = (close_px - low_px) / denom
    clv_centered = clv - 0.5
    upper_shadow = (high_px - np.maximum(open_px, close_px)) / denom
    lower_shadow = (np.minimum(open_px, close_px) - low_px) / denom
    shadow_imbalance = upper_shadow - lower_shadow
    delta_clv = clv - _shift_np(clv, 1)
    delta_body_ratio = body_ratio - _shift_np(body_ratio, 1)

    return_price = adjclose if np.isfinite(adjclose).any() else close_px
    return_1d = _safe_log_np(_shift_np(return_price, -1), return_price)
    open_logret_1d = _safe_log_np(open_px, _shift_np(open_px, 1))
    max_logret_1d = _safe_log_np(high_px, _shift_np(high_px, 1))
    min_logret_1d = _safe_log_np(low_px, _shift_np(low_px, 1))
    close_logret_1d = _safe_log_np(close_px, _shift_np(close_px, 1))
    trading_volume_logret_1d = _safe_log_np(volume, _shift_np(volume, 1))
    signed_vol = np.sign(intraday_return_co) * trading_volume_logret_1d

    feature_map = {
        "open_logret_1d": open_logret_1d,
        "max_logret_1d": max_logret_1d,
        "min_logret_1d": min_logret_1d,
        "close_logret_1d": close_logret_1d,
        "trading_volume_logret_1d": trading_volume_logret_1d,
        "signed_vol": signed_vol,
        "body_ratio": body_ratio,
        "signed_body_ratio": signed_body_ratio,
        "delta_body_ratio": delta_body_ratio,
        "clv": clv,
        "clv_centered": clv_centered,
        "delta_clv": delta_clv,
        "upper_shadow": upper_shadow,
        "lower_shadow": lower_shadow,
        "shadow_imbalance": shadow_imbalance,
    }
    volume_exists = "Trading_Volume" in table.column_names
    close_valid = np.isfinite(close_px)
    if volume_exists:
        tradable = close_valid & ((np.nan_to_num(volume, nan=0.0) > 0.0) | ~np.isfinite(volume))
    else:
        tradable = close_valid

    return SymbolStats(
        rows=rows,
        feature_sum=float(sum(np.nansum(feature_map[name]) for name in LOG_RETURN_FEATURE_COLUMNS)),
        return_sum=float(np.nansum(return_1d)),
        tradable_count=int(tradable.sum()),
    )


def _polars_log_ratio(num: Any, den: Any) -> Any:
    import polars as pl

    return (
        pl.when(num.is_finite() & den.is_finite() & (num > 0.0) & (den > 0.0))
        .then((num / den).log())
        .otherwise(None)
    )


def _bench_polars_file(path: Path) -> SymbolStats:
    import polars as pl

    df = pl.read_parquet(path)
    if "date" in df.columns:
        df = df.sort("date")
    cols = set(df.columns)
    decimals = _price_decimals_for_path(path)

    def num(name: str) -> Any:
        if name in cols:
            return pl.col(name).cast(pl.Float64, strict=False)
        return pl.lit(None, dtype=pl.Float64)

    df = df.with_columns(
        [
            num("open").round(decimals).alias("_open"),
            num("max").round(decimals).alias("_max"),
            num("min").round(decimals).alias("_min"),
            num("close").round(decimals).alias("_close"),
            num("adjclose").round(decimals).alias("_adjclose"),
            num("Trading_Volume").alias("_volume"),
        ]
    )
    spread = (pl.col("_max") - pl.col("_min")).clip(0.0, None)
    denom = spread + EPSILON
    df = df.with_columns(
        [
            _polars_log_ratio(pl.col("_close"), pl.col("_open")).alias("intraday_return_co"),
            (pl.col("_close") - pl.col("_open")).abs().truediv(denom).alias("body_ratio"),
            ((pl.col("_close") - pl.col("_open")) / denom).alias("signed_body_ratio"),
            ((pl.col("_close") - pl.col("_min")) / denom).alias("clv"),
            (pl.max_horizontal("_open", "_close")).alias("_max_oc"),
            (pl.min_horizontal("_open", "_close")).alias("_min_oc"),
        ]
    )
    return_price = pl.col("_adjclose") if "adjclose" in cols else pl.col("_close")
    df = df.with_columns(
        [
            (pl.col("clv") - 0.5).alias("clv_centered"),
            ((pl.col("_max") - pl.col("_max_oc")) / denom).alias("upper_shadow"),
            ((pl.col("_min_oc") - pl.col("_min")) / denom).alias("lower_shadow"),
            _polars_log_ratio(return_price.shift(-1), return_price).alias("return_1d"),
            _polars_log_ratio(pl.col("_open"), pl.col("_open").shift(1)).alias("open_logret_1d"),
            _polars_log_ratio(pl.col("_max"), pl.col("_max").shift(1)).alias("max_logret_1d"),
            _polars_log_ratio(pl.col("_min"), pl.col("_min").shift(1)).alias("min_logret_1d"),
            _polars_log_ratio(pl.col("_close"), pl.col("_close").shift(1)).alias("close_logret_1d"),
            _polars_log_ratio(pl.col("_volume"), pl.col("_volume").shift(1)).alias("trading_volume_logret_1d"),
        ]
    )
    if "Trading_Volume" in cols:
        tradable = pl.col("_close").is_not_null() & ((pl.col("_volume").fill_null(0.0) > 0.0) | pl.col("_volume").is_null())
    else:
        tradable = pl.col("_close").is_not_null()
    df = df.with_columns(
        [
            (pl.col("upper_shadow") - pl.col("lower_shadow")).alias("shadow_imbalance"),
            (pl.col("clv") - pl.col("clv").shift(1)).alias("delta_clv"),
            (pl.col("body_ratio") - pl.col("body_ratio").shift(1)).alias("delta_body_ratio"),
            (pl.col("intraday_return_co").sign() * pl.col("trading_volume_logret_1d")).alias("signed_vol"),
            tradable.alias("tradable"),
        ]
    )
    for feature in LOG_RETURN_FEATURE_COLUMNS:
        if feature not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(feature))

    out = df.select(
        [
            pl.len().alias("rows"),
            pl.sum_horizontal(
                [pl.col(name).fill_nan(0.0).fill_null(0.0) for name in LOG_RETURN_FEATURE_COLUMNS]
            )
            .sum()
            .alias("feature_sum"),
            pl.col("return_1d").fill_nan(0.0).fill_null(0.0).sum().alias("return_sum"),
            pl.col("tradable").cast(pl.Int64).sum().alias("tradable_count"),
        ]
    ).row(0, named=True)
    return SymbolStats(
        rows=int(out["rows"]),
        feature_sum=float(out["feature_sum"] or 0.0),
        return_sum=float(out["return_sum"] or 0.0),
        tradable_count=int(out["tradable_count"] or 0),
    )


def _duckdb_quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _duckdb_safe_log(num: str, den: str) -> str:
    return f"CASE WHEN {num} > 0.0 AND {den} > 0.0 THEN ln({num} / {den}) ELSE NULL END"


def _bench_duckdb_file(path: Path) -> SymbolStats:
    import duckdb
    import pyarrow.parquet as pq

    names = set(pq.read_schema(path).names)

    def col(name: str) -> str:
        if name in names:
            return f"try_cast({_duckdb_quote(name)} AS DOUBLE)"
        return "NULL::DOUBLE"

    decimals = _price_decimals_for_path(path)
    return_source = "adjclose_px" if "adjclose" in names else "close_px"
    volume_expr = "volume_px"
    tradable_expr = (
        "close_px IS NOT NULL AND (volume_px > 0.0 OR volume_px IS NULL)"
        if "Trading_Volume" in names
        else "close_px IS NOT NULL"
    )
    feature_terms = " + ".join(f"coalesce({name}, 0.0)" for name in LOG_RETURN_FEATURE_COLUMNS)
    sql = f"""
    WITH base AS (
        SELECT
            CAST(date AS TIMESTAMP) AS date_key,
            round({col("open")}, {decimals}) AS open_px,
            round({col("max")}, {decimals}) AS max_px,
            round({col("min")}, {decimals}) AS min_px,
            round({col("close")}, {decimals}) AS close_px,
            round({col("adjclose")}, {decimals}) AS adjclose_px,
            {col("Trading_Volume")} AS volume_px
        FROM read_parquet(?)
    ),
    calc1 AS (
        SELECT
            *,
            CASE
                WHEN max_px IS NULL OR min_px IS NULL THEN NULL
                ELSE greatest(max_px - min_px, 0.0) + {EPSILON}
            END AS denom,
            {_duckdb_safe_log("close_px", "open_px")} AS intraday_return_co
        FROM base
    ),
    calc2 AS (
        SELECT
            *,
            abs(close_px - open_px) / denom AS body_ratio,
            (close_px - open_px) / denom AS signed_body_ratio,
            (close_px - min_px) / denom AS clv,
            (max_px - greatest(open_px, close_px)) / denom AS upper_shadow,
            (least(open_px, close_px) - min_px) / denom AS lower_shadow,
            {_duckdb_safe_log(f"lead({return_source}) OVER (ORDER BY date_key)", return_source)} AS return_1d,
            {_duckdb_safe_log("open_px", "lag(open_px) OVER (ORDER BY date_key)")} AS open_logret_1d,
            {_duckdb_safe_log("max_px", "lag(max_px) OVER (ORDER BY date_key)")} AS max_logret_1d,
            {_duckdb_safe_log("min_px", "lag(min_px) OVER (ORDER BY date_key)")} AS min_logret_1d,
            {_duckdb_safe_log("close_px", "lag(close_px) OVER (ORDER BY date_key)")} AS close_logret_1d,
            {_duckdb_safe_log(volume_expr, f"lag({volume_expr}) OVER (ORDER BY date_key)")} AS trading_volume_logret_1d,
            {tradable_expr} AS tradable
        FROM calc1
    ),
    calc3 AS (
        SELECT
            *,
            clv - 0.5 AS clv_centered,
            upper_shadow - lower_shadow AS shadow_imbalance,
            clv - lag(clv) OVER (ORDER BY date_key) AS delta_clv,
            body_ratio - lag(body_ratio) OVER (ORDER BY date_key) AS delta_body_ratio,
            sign(intraday_return_co) * trading_volume_logret_1d AS signed_vol
        FROM calc2
    )
    SELECT
        count(*) AS rows,
        sum({feature_terms}) AS feature_sum,
        sum(coalesce(return_1d, 0.0)) AS return_sum,
        sum(CASE WHEN tradable THEN 1 ELSE 0 END) AS tradable_count
    FROM calc3
    """
    con = duckdb.connect(database=":memory:")
    try:
        row = con.execute(sql, [str(path)]).fetchone()
    finally:
        con.close()
    return SymbolStats(
        rows=int(row[0] or 0),
        feature_sum=float(row[1] or 0.0),
        return_sum=float(row[2] or 0.0),
        tradable_count=int(row[3] or 0),
    )


def _duckdb_union_schema_names(paths: list[Path]) -> set[str]:
    import pyarrow.parquet as pq

    names: set[str] = set()
    for path in paths:
        names.update(pq.read_schema(path).names)
    return names


def _duckdb_arrow_column(table: Any, name: str, dtype: Any) -> np.ndarray:
    values = table[name].combine_chunks().to_numpy(zero_copy_only=False)
    return np.asarray(values, dtype=dtype)


def _build_panel_duckdb(
    parquet_root: Path,
    paths: list[Path],
    *,
    benchmark_name: str,
    threads: int,
) -> PanelData:
    import duckdb

    symbols = [_symbol_name_from_path(path) for path in paths]
    symbol_map = pd.DataFrame(
        {
            "symbol": symbols,
            "symbol_idx": np.arange(len(symbols), dtype=np.int32),
        }
    )
    schema_names = _duckdb_union_schema_names(paths)
    decimals_by_path = {_price_decimals_for_path(path) for path in paths}
    if len(decimals_by_path) != 1:
        raise RuntimeError("DuckDB panel benchmark currently requires one market price precision")
    decimals = int(next(iter(decimals_by_path)))
    return_source = "adjclose_px" if "adjclose" in schema_names else "close_px"
    volume_expr = "volume_px"
    tradable_expr = (
        "close_px IS NOT NULL AND (volume_px > 0.0 OR volume_px IS NULL)"
        if "Trading_Volume" in schema_names
        else "close_px IS NOT NULL"
    )
    feature_names_sql = ",\n            ".join(LOG_RETURN_FEATURE_COLUMNS)
    pattern = (parquet_root / f"*{FEATURE_FILE_SUFFIX}").as_posix()

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"PRAGMA threads={max(1, int(threads))}")
        con.register("symbol_map_df", symbol_map)
        con.execute("CREATE TEMP TABLE symbol_map AS SELECT * FROM symbol_map_df")
        sql = f"""
        CREATE TEMP TABLE computed AS
        WITH raw AS (
            SELECT
                m.symbol_idx,
                m.symbol,
                CAST(date AS TIMESTAMP) AS date_key,
                round({_duckdb_column_or_null(schema_names, "open")}, {decimals}) AS open_px,
                round({_duckdb_column_or_null(schema_names, "max")}, {decimals}) AS max_px,
                round({_duckdb_column_or_null(schema_names, "min")}, {decimals}) AS min_px,
                round({_duckdb_column_or_null(schema_names, "close")}, {decimals}) AS close_px,
                round({_duckdb_column_or_null(schema_names, "adjclose")}, {decimals}) AS adjclose_px,
                {_duckdb_column_or_null(schema_names, "Trading_Volume")} AS volume_px
            FROM read_parquet(?, filename=true, union_by_name=true) AS r
            JOIN symbol_map AS m
              ON m.symbol = regexp_extract(r.filename, '([^/\\\\]+)_features\\.parquet$', 1)
        ),
        calc1 AS (
            SELECT
                *,
                CASE
                    WHEN max_px IS NULL OR min_px IS NULL THEN NULL
                    ELSE greatest(max_px - min_px, 0.0) + {EPSILON}
                END AS denom,
                {_duckdb_safe_log("close_px", "open_px")} AS intraday_return_co
            FROM raw
            WHERE date_key IS NOT NULL
        ),
        calc2 AS (
            SELECT
                *,
                abs(close_px - open_px) / denom AS body_ratio,
                (close_px - open_px) / denom AS signed_body_ratio,
                (close_px - min_px) / denom AS clv,
                (max_px - greatest(open_px, close_px)) / denom AS upper_shadow,
                (least(open_px, close_px) - min_px) / denom AS lower_shadow,
                {_duckdb_safe_log(f"lead({return_source}) OVER (PARTITION BY symbol_idx ORDER BY date_key)", return_source)} AS return_1d,
                {_duckdb_safe_log("open_px", "lag(open_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS open_logret_1d,
                {_duckdb_safe_log("max_px", "lag(max_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS max_logret_1d,
                {_duckdb_safe_log("min_px", "lag(min_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS min_logret_1d,
                {_duckdb_safe_log("close_px", "lag(close_px) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS close_logret_1d,
                {_duckdb_safe_log(volume_expr, f"lag({volume_expr}) OVER (PARTITION BY symbol_idx ORDER BY date_key)")} AS trading_volume_logret_1d,
                {tradable_expr} AS tradable,
                close_px IS NOT NULL AS alive
            FROM calc1
        ),
        calc3 AS (
            SELECT
                symbol_idx,
                symbol,
                date_key,
                close_px,
                return_1d,
                tradable,
                alive,
                open_logret_1d,
                max_logret_1d,
                min_logret_1d,
                close_logret_1d,
                trading_volume_logret_1d,
                sign(intraday_return_co) * trading_volume_logret_1d AS signed_vol,
                body_ratio,
                signed_body_ratio,
                body_ratio - lag(body_ratio) OVER (PARTITION BY symbol_idx ORDER BY date_key) AS delta_body_ratio,
                clv,
                clv - 0.5 AS clv_centered,
                clv - lag(clv) OVER (PARTITION BY symbol_idx ORDER BY date_key) AS delta_clv,
                upper_shadow,
                lower_shadow,
                upper_shadow - lower_shadow AS shadow_imbalance
            FROM calc2
        )
        SELECT * FROM calc3
        """
        con.execute(sql, [pattern])
        con.execute(
            """
            CREATE TEMP TABLE date_map AS
            SELECT date_key, row_number() OVER (ORDER BY date_key) - 1 AS date_idx
            FROM (SELECT DISTINCT date_key FROM computed)
            """
        )
        dates_table = con.execute("SELECT date_key FROM date_map ORDER BY date_idx").to_arrow_table()
        dates = _duckdb_arrow_column(dates_table, "date_key", "datetime64[ns]")
        num_dates = int(dates.size)
        num_symbols = len(symbols)
        num_features = len(LOG_RETURN_FEATURE_COLUMNS)
        features = np.full((num_dates, num_symbols, num_features), np.nan, dtype=np.float32)
        returns_1d = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
        close_prices = np.full((num_dates, num_symbols), np.nan, dtype=np.float32)
        tradable_mask = np.zeros((num_dates, num_symbols), dtype=bool)
        alive_mask = np.zeros((num_dates, num_symbols), dtype=bool)

        select_cols = ", ".join(
            [
                "dm.date_idx",
                "c.symbol_idx",
                "c.return_1d",
                "c.close_px",
                "c.tradable",
                "c.alive",
                *[f"c.{name}" for name in LOG_RETURN_FEATURE_COLUMNS],
            ]
        )
        final_table = con.execute(
            f"""
            SELECT {select_cols}
            FROM computed AS c
            JOIN date_map AS dm USING (date_key)
            ORDER BY c.symbol_idx, dm.date_idx
            """
        ).to_arrow_table()
    finally:
        con.close()

    row_idx = _duckdb_arrow_column(final_table, "date_idx", np.int64)
    sym_idx = _duckdb_arrow_column(final_table, "symbol_idx", np.int64)
    returns_1d[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "return_1d", np.float32)
    close_prices[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "close_px", np.float32)
    tradable_mask[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "tradable", bool)
    alive_mask[row_idx, sym_idx] = _duckdb_arrow_column(final_table, "alive", bool)
    for feat_idx, name in enumerate(LOG_RETURN_FEATURE_COLUMNS):
        features[row_idx, sym_idx, feat_idx] = _duckdb_arrow_column(final_table, name, np.float32)
    features = np.nan_to_num(features, nan=0.0, posinf=0.0, neginf=0.0)

    benchmark_symbol_index = _resolve_benchmark_index(symbols, benchmark_name)
    if benchmark_symbol_index is None:
        valid_returns = np.isfinite(returns_1d)
        n_valid = valid_returns.sum(axis=1)
        sum_ret = np.nansum(np.where(valid_returns, returns_1d, 0.0), axis=1)
        benchmark_returns = np.zeros_like(sum_ret, dtype=np.float32)
        np.divide(sum_ret, n_valid, out=benchmark_returns, where=n_valid > 0)
    else:
        benchmark_returns = np.nan_to_num(
            returns_1d[:, benchmark_symbol_index],
            nan=0.0,
            posinf=0.0,
            neginf=0.0,
        ).astype(np.float32, copy=False)

    return PanelData(
        dates=np.asarray(dates, dtype="datetime64[ns]"),
        symbols=symbols,
        feature_names=list(LOG_RETURN_FEATURE_COLUMNS),
        features=features,
        returns_1d=returns_1d,
        tradable_mask=tradable_mask,
        can_buy_mask=tradable_mask.copy(),
        can_sell_mask=tradable_mask.copy(),
        alive_mask=alive_mask,
        benchmark_returns=benchmark_returns,
        close_prices=close_prices,
    )


def _duckdb_column_or_null(schema_names: set[str], name: str) -> str:
    if name in schema_names:
        return f"try_cast({_duckdb_quote(name)} AS DOUBLE)"
    return "NULL::DOUBLE"


FEATURE_PREP_BACKENDS: dict[str, tuple[str, Callable[[Path], SymbolStats]]] = {
    "pandas": ("pandas", _bench_pandas_file),
    "polars": ("polars", _bench_polars_file),
    "duckdb": ("duckdb", _bench_duckdb_file),
    "pyarrow": ("pyarrow", _bench_pyarrow_file),
    "cudf": ("cudf", _bench_cudf_file),
}


def _select_paths(paths: list[Path], max_symbols: int | None) -> list[Path]:
    if max_symbols is None or max_symbols <= 0 or max_symbols >= len(paths):
        return list(paths)
    if max_symbols == 1:
        return [paths[0]]
    indices = np.linspace(0, len(paths) - 1, num=int(max_symbols), dtype=np.int64)
    return [paths[int(idx)] for idx in indices]


def benchmark_feature_prep(
    parquet_root: Path,
    *,
    backends: list[str],
    max_symbols: int | None,
    repeat: int,
) -> list[BenchmarkResult]:
    all_paths = sorted(Path(parquet_root).glob(f"*{FEATURE_FILE_SUFFIX}"))
    paths = _select_paths(all_paths, max_symbols)
    results: list[BenchmarkResult] = []
    if not paths:
        return [
            BenchmarkResult(
                workload="feature_prep",
                backend=backend,
                available=False,
                error=f"no *{FEATURE_FILE_SUFFIX} files under {parquet_root}",
            )
            for backend in backends
        ]

    for backend in backends:
        if backend not in FEATURE_PREP_BACKENDS:
            results.append(
                BenchmarkResult(
                    workload="feature_prep",
                    backend=backend,
                    available=False,
                    error=f"unknown backend {backend!r}",
                )
            )
            continue
        module_name, fn = FEATURE_PREP_BACKENDS[backend]
        if not _module_available(module_name):
            results.append(
                BenchmarkResult(
                    workload="feature_prep",
                    backend=backend,
                    available=False,
                    error=f"missing module {module_name}",
                )
            )
            continue
        total = SymbolStats()
        start = time.perf_counter()
        try:
            for _ in range(max(1, int(repeat))):
                for path in paths:
                    total.add(fn(path))
            elapsed = time.perf_counter() - start
            rows = int(total.rows)
            files = int(len(paths) * max(1, int(repeat)))
            results.append(
                BenchmarkResult(
                    workload="feature_prep",
                    backend=backend,
                    available=True,
                    files=files,
                    rows=rows,
                    repeat=max(1, int(repeat)),
                    elapsed_s=elapsed,
                    rows_per_s=(rows / elapsed) if elapsed > 0 else math.inf,
                    files_per_s=(files / elapsed) if elapsed > 0 else math.inf,
                    checksum=total.checksum,
                )
            )
        except Exception as exc:
            results.append(
                BenchmarkResult(
                    workload="feature_prep",
                    backend=backend,
                    available=True,
                    files=int(len(paths) * max(1, int(repeat))),
                    repeat=max(1, int(repeat)),
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
    return results


def _link_or_copy(src: Path, dst: Path) -> None:
    try:
        dst.symlink_to(src)
    except OSError:
        shutil.copy2(src, dst)


def _bench_temp_dir(prefix: str) -> tempfile.TemporaryDirectory[str]:
    root = Path(os.environ.get("STOCKAGENT_BENCH_TMPDIR", "/tmp"))
    root.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(prefix=prefix, dir=str(root))


def benchmark_panel_build(
    parquet_root: Path,
    *,
    backends: list[str],
    max_symbols: int | None,
    panel_load_workers: int,
    benchmark_name: str,
) -> list[BenchmarkResult]:
    all_paths = sorted(Path(parquet_root).glob(f"*{FEATURE_FILE_SUFFIX}"))
    paths = _select_paths(all_paths, max_symbols)
    results: list[BenchmarkResult] = []
    if not paths:
        return [
            BenchmarkResult(
                workload="panel_build",
                backend=backend,
                available=False,
                error=f"no *{FEATURE_FILE_SUFFIX} files under {parquet_root}",
            )
            for backend in backends
        ]

    with _bench_temp_dir("stockagent-panel-build-bench-") as tmp:
        tmp_dir = Path(tmp)
        for src in paths:
            _link_or_copy(src.resolve(), tmp_dir / src.name)
        bench_paths = sorted(tmp_dir.glob(f"*{FEATURE_FILE_SUFFIX}"))

        for backend in backends:
            if backend == "duckdb" and not _module_available("duckdb"):
                results.append(
                    BenchmarkResult(
                        workload="panel_build",
                        backend=backend,
                        available=False,
                        files=len(paths),
                        error="missing module duckdb",
                    )
                )
                continue
            if backend == "cudf" and not _module_available("cudf"):
                results.append(
                    BenchmarkResult(
                        workload="panel_build",
                        backend=backend,
                        available=False,
                        files=len(paths),
                        error="missing module cudf",
                    )
                )
                continue
            if backend in {"polars", "pyarrow"} and not _module_available(backend):
                results.append(
                    BenchmarkResult(
                        workload="panel_build",
                        backend=backend,
                        available=False,
                        files=len(paths),
                        error=f"missing module {backend}",
                    )
                )
                continue

            start = time.perf_counter()
            try:
                if backend == "duckdb":
                    panel = _build_panel_duckdb(
                        tmp_dir,
                        bench_paths,
                        benchmark_name=benchmark_name,
                        threads=max(1, int(panel_load_workers)),
                    )
                    save_panel_cache_v2(
                        tmp_dir,
                        panel,
                        source_hash="benchmark",
                        backend_key=f"duckdb|benchmark={benchmark_name}",
                        version=0,
                    )
                else:
                    panel = build_panel(
                        tmp_dir,
                        use_rapids=(backend == "cudf"),
                        benchmark_name=benchmark_name,
                        panel_backend="auto" if backend == "cudf" else backend,
                        panel_load_workers=max(0, int(panel_load_workers)),
                    )
                elapsed = time.perf_counter() - start
                dense_cells = int(panel.num_dates * panel.num_symbols)
                checksum = float(
                    np.sum(panel.features, dtype=np.float64)
                    + np.nansum(panel.returns_1d, dtype=np.float64) * 7.0
                    + int(panel.tradable_mask.sum()) * 1e-6
                )
                results.append(
                    BenchmarkResult(
                        workload="panel_build",
                        backend=backend,
                        available=True,
                        files=len(paths),
                        rows=dense_cells,
                        elapsed_s=elapsed,
                        rows_per_s=(dense_cells / elapsed) if elapsed > 0 else math.inf,
                        files_per_s=(len(paths) / elapsed) if elapsed > 0 else math.inf,
                        checksum=checksum,
                    )
                )
                del panel
                gc.collect()
            except Exception as exc:
                results.append(
                    BenchmarkResult(
                        workload="panel_build",
                        backend=backend,
                        available=True,
                        files=len(paths),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
    return results


def _wide_data(rows: int, symbols: int, seed: int) -> tuple[np.ndarray, np.ndarray, list[str]]:
    rng = np.random.default_rng(seed)
    dates = np.arange(np.datetime64("2020-01-01"), np.datetime64("2020-01-01") + rows)
    weights = rng.normal(0.0, 0.01, size=(rows, symbols)).astype(np.float32)
    names = [f"S{i:05d}" for i in range(symbols)]
    return dates, weights, names


def _bench_wide_write_pandas(path: Path, dates: np.ndarray, weights: np.ndarray, symbols: list[str]) -> int:
    df = pd.DataFrame(weights, columns=symbols)
    df.insert(0, "date", dates)
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")
    return path.stat().st_size


def _bench_wide_write_pyarrow(path: Path, dates: np.ndarray, weights: np.ndarray, symbols: list[str]) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    arrays = [pa.array(dates)]
    arrays.extend(pa.array(weights[:, idx]) for idx in range(weights.shape[1]))
    table = pa.Table.from_arrays(arrays, names=["date", *symbols])
    pq.write_table(table, path, compression="snappy")
    return path.stat().st_size


def _bench_wide_write_polars(path: Path, dates: np.ndarray, weights: np.ndarray, symbols: list[str]) -> int:
    import polars as pl

    data = {"date": dates}
    data.update({symbol: weights[:, idx] for idx, symbol in enumerate(symbols)})
    pl.DataFrame(data).write_parquet(path, compression="snappy")
    return path.stat().st_size


def _bench_wide_write_duckdb(path: Path, dates: np.ndarray, weights: np.ndarray, symbols: list[str]) -> int:
    import duckdb
    import pyarrow as pa

    arrays = [pa.array(dates)]
    arrays.extend(pa.array(weights[:, idx]) for idx in range(weights.shape[1]))
    table = pa.Table.from_arrays(arrays, names=["date", *symbols])
    con = duckdb.connect(database=":memory:")
    try:
        con.register("weights_arrow", table)
        con.execute(f"COPY weights_arrow TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)")
    finally:
        con.close()
    return path.stat().st_size


def _bench_wide_write_cudf(path: Path, dates: np.ndarray, weights: np.ndarray, symbols: list[str]) -> int:
    import cudf

    # Current runtime table output is CPU-originated. This benchmarks the cost of
    # moving the same CPU arrays into cuDF before parquet write.
    data = {"date": dates}
    data.update({symbol: weights[:, idx] for idx, symbol in enumerate(symbols)})
    cudf.DataFrame(data).to_parquet(path, compression="snappy", index=False)
    return path.stat().st_size


WIDE_WRITE_BACKENDS: dict[str, tuple[str, Callable[[Path, np.ndarray, np.ndarray, list[str]], int]]] = {
    "pandas": ("pandas", _bench_wide_write_pandas),
    "polars": ("polars", _bench_wide_write_polars),
    "duckdb": ("duckdb", _bench_wide_write_duckdb),
    "pyarrow": ("pyarrow", _bench_wide_write_pyarrow),
    "cudf": ("cudf", _bench_wide_write_cudf),
}


def benchmark_wide_write(
    *,
    backends: list[str],
    rows: int,
    symbols: int,
    repeat: int,
    seed: int,
) -> list[BenchmarkResult]:
    dates, weights, names = _wide_data(rows, symbols, seed)
    results: list[BenchmarkResult] = []
    with _bench_temp_dir("stockagent-data-bench-") as tmp:
        tmp_dir = Path(tmp)
        for backend in backends:
            if backend not in WIDE_WRITE_BACKENDS:
                continue
            module_name, fn = WIDE_WRITE_BACKENDS[backend]
            if not _module_available(module_name):
                results.append(
                    BenchmarkResult(
                        workload="wide_parquet_write",
                        backend=backend,
                        available=False,
                        rows=rows * max(1, int(repeat)),
                        repeat=max(1, int(repeat)),
                        error=f"missing module {module_name}",
                    )
                )
                continue
            output_bytes = 0
            start = time.perf_counter()
            try:
                for idx in range(max(1, int(repeat))):
                    output_bytes += int(fn(tmp_dir / f"{backend}_{idx}.parquet", dates, weights, names))
                elapsed = time.perf_counter() - start
                out_rows = rows * max(1, int(repeat))
                results.append(
                    BenchmarkResult(
                        workload="wide_parquet_write",
                        backend=backend,
                        available=True,
                        rows=out_rows,
                        repeat=max(1, int(repeat)),
                        elapsed_s=elapsed,
                        rows_per_s=(out_rows / elapsed) if elapsed > 0 else math.inf,
                        output_bytes=output_bytes,
                    )
                )
            except Exception as exc:
                results.append(
                    BenchmarkResult(
                        workload="wide_parquet_write",
                        backend=backend,
                        available=True,
                        rows=rows * max(1, int(repeat)),
                        repeat=max(1, int(repeat)),
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
    return results


def _fastest(results: list[BenchmarkResult]) -> dict[str, str]:
    fastest: dict[str, str] = {}
    workloads = sorted({result.workload for result in results})
    for workload in workloads:
        candidates = [
            result
            for result in results
            if result.workload == workload and result.available and result.elapsed_s is not None and result.error is None
        ]
        if not candidates:
            continue
        fastest[workload] = min(candidates, key=lambda item: float(item.elapsed_s)).backend
    return fastest


def _write_outputs(payload: dict[str, Any], output_json: Path | None, output_csv: Path | None) -> None:
    if output_json is not None:
        output_json.parent.mkdir(parents=True, exist_ok=True)
        output_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    if output_csv is not None:
        rows = payload.get("results", [])
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        if rows:
            import csv

            with output_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark stockAgent data-processing backends.")
    parser.add_argument("--parquet-root", default="data_yahoo/us_stocks")
    parser.add_argument("--backends", default="pandas,polars,duckdb,pyarrow,cudf")
    parser.add_argument("--max-symbols", type=int, default=256, help="0 means all parquet files.")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--skip-feature-prep", action="store_true")
    parser.add_argument("--skip-panel-build", action="store_true")
    parser.add_argument("--skip-wide-write", action="store_true")
    parser.add_argument("--panel-load-workers", type=int, default=16)
    parser.add_argument("--benchmark-name", default="universe_average_return")
    parser.add_argument("--wide-rows", type=int, default=512)
    parser.add_argument("--wide-symbols", type=int, default=2048)
    parser.add_argument("--seed", type=int, default=23)
    parser.add_argument("--scan", action="store_true", help="Include static data-processing hotspot scan.")
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-csv", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    backends = [item.strip().lower() for item in str(args.backends).split(",") if item.strip()]
    results: list[BenchmarkResult] = []
    parquet_root = (REPO_ROOT / args.parquet_root).resolve() if not Path(args.parquet_root).is_absolute() else Path(args.parquet_root)
    if not args.skip_feature_prep:
        results.extend(
            benchmark_feature_prep(
                parquet_root,
                backends=backends,
                max_symbols=None if int(args.max_symbols) <= 0 else int(args.max_symbols),
                repeat=max(1, int(args.repeat)),
            )
        )
    if not args.skip_panel_build:
        results.extend(
            benchmark_panel_build(
                parquet_root,
                backends=backends,
                max_symbols=None if int(args.max_symbols) <= 0 else int(args.max_symbols),
                panel_load_workers=int(args.panel_load_workers),
                benchmark_name=str(args.benchmark_name),
            )
        )
    if not args.skip_wide_write:
        results.extend(
            benchmark_wide_write(
                backends=backends,
                rows=max(1, int(args.wide_rows)),
                symbols=max(1, int(args.wide_symbols)),
                repeat=max(1, int(args.repeat)),
                seed=int(args.seed),
            )
        )

    payload: dict[str, Any] = {
        "parquet_root": str(parquet_root),
        "max_symbols": int(args.max_symbols),
        "repeat": max(1, int(args.repeat)),
        "backends": backends,
        "results": [asdict(result) for result in results],
        "fastest": _fastest(results),
    }
    if args.scan:
        payload["hotspots"] = [asdict(item) for item in scan_data_processing_hotspots(REPO_ROOT)]

    _write_outputs(payload, args.output_json, args.output_csv)
    print(json.dumps(payload, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
