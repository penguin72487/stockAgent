from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stockagent.live.capital import CapitalScale, resolve_fold_capital_scale

_ROW_INDEX_COL = "__stockagent_row_index"


def is_bar_frequency(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"bar", "intraday", "15m", "15min", "interval"}


@dataclass(slots=True)
class StockHistoryResult:
    requested_symbol: str
    symbol: str
    name: str
    fold_dir: Path
    rows: list[dict[str, Any]]
    source_paths: tuple[Path, ...]
    changes_only: bool
    fell_back_to_all_rows: bool = False
    capital: CapitalScale | None = None


def normalize_symbol_query(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = text.upper()
    for suffix in (".TW", ".TWO", ".TPE", ".TSE"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def _artifact_path(fold_dir: str | Path, stem: str) -> Path | None:
    root = Path(fold_dir)
    for suffix in (".parquet", ".csv"):
        path = root / f"{stem}{suffix}"
        if path.exists():
            return path
    return None


def _read_table(path: Path):
    import polars as pl

    if path.suffix.lower() == ".parquet":
        return pl.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pl.read_csv(path, infer_schema_length=10000)
    raise ValueError(f"Unsupported table format: {path}")


def _date_string_expr(frequency: str | None = "daily"):
    import polars as pl

    text = pl.col("date").cast(pl.Utf8).str.replace("T", " ").str.replace(r"\.\d+$", "")
    if is_bar_frequency(frequency):
        return text.str.slice(0, 19).alias("date")
    return text.str.slice(0, 10).alias("date")


def _day_string_expr(source: str = "date"):
    import polars as pl

    return pl.col(source).cast(pl.Utf8).str.replace("T", " ").str.slice(0, 10).alias("__day")


def _last_daily_snapshot(frame, value_columns: list[str]):
    """Collapse intraday artifacts to one end-of-day row per date."""
    import polars as pl

    if _ROW_INDEX_COL not in frame.columns:
        frame = frame.with_row_index(_ROW_INDEX_COL)
    return (
        frame.sort(_ROW_INDEX_COL)
        .group_by("date", maintain_order=True)
        .agg([pl.col(name).last().alias(name) for name in value_columns])
    )


def _returns_from_frame(frame, columns: list[str], *, frequency: str | None):
    import polars as pl

    if _ROW_INDEX_COL not in frame.columns:
        frame = frame.with_row_index(_ROW_INDEX_COL)
    selected = frame.select(
        [
            _date_string_expr(frequency),
            pl.col(_ROW_INDEX_COL),
            *[pl.col(name).cast(pl.Float64, strict=False).alias(name) for name in columns],
        ]
    )
    if is_bar_frequency(frequency):
        return selected.drop(_ROW_INDEX_COL)

    aggregations = []
    for name in ("portfolio_return", "benchmark_return"):
        if name in columns:
            value = pl.col(name)
            aggregations.append(
                pl.when(value.is_not_null().any())
                .then((value.drop_nulls() + 1.0).product() - 1.0)
                .otherwise(None)
                .alias(name)
            )
    if "turnover" in columns:
        turnover = pl.col("turnover")
        aggregations.append(
            pl.when(turnover.is_not_null().any()).then(turnover.sum()).otherwise(None).alias("turnover")
        )
    if not aggregations:
        return selected.select("date").unique(maintain_order=True)
    return selected.sort(_ROW_INDEX_COL).group_by("date", maintain_order=True).agg(aggregations)


def _resolve_symbol_value(values: list[str], requested: str) -> str | None:
    query = str(requested or "").strip()
    if not query:
        return None
    if query in values:
        return query
    query_upper = query.upper()
    for value in values:
        if str(value).upper() == query_upper:
            return str(value)
    query_norm = normalize_symbol_query(query)
    for value in values:
        if normalize_symbol_query(str(value)) == query_norm:
            return str(value)
    return None


def _resolve_symbol_column(columns: list[str], requested: str) -> str | None:
    return _resolve_symbol_value([name for name in columns if name != "date"], requested)


def _read_wide_symbol_series(
    fold_dir: Path,
    stem: str,
    symbol: str,
    alias: str,
    *,
    frequency: str | None = "daily",
):
    import polars as pl

    path = _artifact_path(fold_dir, stem)
    if path is None:
        return None, None, None
    frame = _read_table(path)
    if "date" not in frame.columns:
        raise ValueError(f"{path} missing date column")
    column = _resolve_symbol_column(frame.columns, symbol)
    if column is None:
        return None, None, path
    selected = frame.with_row_index(_ROW_INDEX_COL).select(
        [
            _date_string_expr(frequency),
            pl.col(_ROW_INDEX_COL),
            pl.col(column).cast(pl.Float64, strict=False).alias(alias),
        ]
    )
    if is_bar_frequency(frequency):
        return selected.drop(_ROW_INDEX_COL), column, path
    return (_last_daily_snapshot(selected, [alias]), column, path)


def _timeline_for_nonzero_symbol(frame, weight_column: str):
    import polars as pl

    if frame is None or weight_column not in frame.columns:
        return None
    return (
        frame.select(["date", weight_column])
        .with_columns(_day_string_expr())
        .filter(pl.col(weight_column).abs() > 1e-12)
        .with_columns(pl.arange(0, pl.len()).over("__day").alias("__seq"))
        .select(["__day", "__seq", pl.col("date").alias("__bar_date")])
    )


def _read_holdings_symbol(
    fold_dir: Path,
    symbol: str,
    *,
    frequency: str | None = "daily",
    timeline_frame=None,
    timeline_weight_column: str = "actual_weight",
):
    import polars as pl

    path = _artifact_path(fold_dir, "holdings")
    if path is None:
        return None, None, None
    frame = _read_table(path)
    required = {"date", "symbol", "shares", "price", "market_value", "holding_ratio"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{path} missing columns: {', '.join(missing)}")
    symbols = [str(item) for item in frame.select("symbol").unique().to_series().to_list()]
    resolved = _resolve_symbol_value(symbols, symbol)
    if resolved is None:
        return None, None, path
    selected = (
        frame.with_row_index(_ROW_INDEX_COL)
        .filter(pl.col("symbol").cast(pl.Utf8) == resolved)
        .select(
            [
                _date_string_expr("daily"),
                pl.col(_ROW_INDEX_COL),
                pl.col("shares").cast(pl.Int64, strict=False).alias("shares"),
                pl.col("price").cast(pl.Float64, strict=False).alias("price"),
                pl.col("market_value").cast(pl.Float64, strict=False).alias("market_value"),
                pl.col("holding_ratio").cast(pl.Float64, strict=False).alias("holding_ratio_from_holdings"),
            ]
        )
    )
    if is_bar_frequency(frequency):
        timeline = _timeline_for_nonzero_symbol(timeline_frame, timeline_weight_column)
        if timeline is not None:
            selected = (
                selected.with_columns(
                    [
                        pl.col("date").alias("__day"),
                        pl.arange(0, pl.len()).over("date").alias("__seq"),
                    ]
                )
                .join(timeline, on=["__day", "__seq"], how="left")
                .with_columns(pl.coalesce([pl.col("__bar_date"), pl.col("date")]).alias("date"))
                .drop(["__day", "__seq", "__bar_date", _ROW_INDEX_COL])
            )
            return selected, resolved, path
        return selected.drop(_ROW_INDEX_COL), resolved, path
    return (
        _last_daily_snapshot(
            selected,
            ["shares", "price", "market_value", "holding_ratio_from_holdings"],
        ),
        resolved,
        path,
    )


def _read_returns(fold_dir: Path, *, frequency: str | None = "daily"):
    import polars as pl

    path = _artifact_path(fold_dir, "integer_share_daily_portfolio_returns")
    if path is None:
        path = _artifact_path(fold_dir, "daily_portfolio_returns")
    if path is None:
        return None, None
    frame = _read_table(path)
    if "date" not in frame.columns:
        raise ValueError(f"{path} missing date column")
    columns = ["date"]
    for name in ("portfolio_return", "benchmark_return", "turnover"):
        if name in frame.columns:
            columns.append(name)
    return _returns_from_frame(frame, [name for name in columns if name != "date"], frequency=frequency), path


def _symbol_name(symbol_names: dict[str, str] | None, symbol: str) -> str:
    if not symbol_names:
        return ""
    exact = symbol_names.get(symbol)
    if exact:
        return str(exact)
    normalized = normalize_symbol_query(symbol)
    for key, value in symbol_names.items():
        if normalize_symbol_query(str(key)) == normalized and str(value).strip():
            return str(value).strip()
    return ""


def classify_stock_history_action(
    prev_shares: int,
    shares: int,
    *,
    holding_delta: float = 0.0,
    model_delta: float = 0.0,
    actual_delta: float = 0.0,
    eps: float = 1e-9,
) -> str:
    if shares != prev_shares:
        if prev_shares == 0:
            return "OPEN_LONG" if shares > 0 else "OPEN_SHORT"
        if shares == 0:
            return "EXIT_LONG" if prev_shares > 0 else "EXIT_SHORT"
        if prev_shares > 0 and shares < 0:
            return "FLIP_TO_SHORT"
        if prev_shares < 0 and shares > 0:
            return "FLIP_TO_LONG"
        if shares > 0:
            return "ADD_LONG" if shares > prev_shares else "REDUCE_LONG"
        return "ADD_SHORT" if shares < prev_shares else "REDUCE_SHORT"
    if abs(holding_delta) > eps or abs(model_delta) > eps or abs(actual_delta) > eps:
        return "ADJUST_WEIGHT"
    return "HOLD"


def _coalesce_stock_history_columns(frame):
    import polars as pl

    defaults = {
        "shares": pl.lit(None, dtype=pl.Int64),
        "price": pl.lit(None, dtype=pl.Float64),
        "market_value": pl.lit(None, dtype=pl.Float64),
        "holding_ratio_from_holdings": pl.lit(None, dtype=pl.Float64),
        "model_weight": pl.lit(None, dtype=pl.Float64),
        "actual_weight": pl.lit(None, dtype=pl.Float64),
        "portfolio_return": pl.lit(None, dtype=pl.Float64),
        "benchmark_return": pl.lit(None, dtype=pl.Float64),
        "turnover": pl.lit(None, dtype=pl.Float64),
    }
    missing_exprs = [expr.alias(name) for name, expr in defaults.items() if name not in frame.columns]
    if missing_exprs:
        frame = frame.with_columns(missing_exprs)

    return (
        frame.with_columns(
            [
                pl.coalesce([pl.col("shares"), pl.lit(0)]).cast(pl.Int64).alias("shares"),
                pl.coalesce([pl.col("market_value"), pl.lit(0.0)]).cast(pl.Float64).alias("market_value"),
                pl.coalesce([pl.col("model_weight"), pl.lit(0.0)]).cast(pl.Float64).alias("model_weight"),
                pl.coalesce([pl.col("actual_weight"), pl.lit(0.0)]).cast(pl.Float64).alias("actual_weight"),
                pl.coalesce(
                    [
                        pl.col("holding_ratio_from_holdings"),
                        pl.col("actual_weight"),
                        pl.lit(0.0),
                    ]
                )
                .cast(pl.Float64)
                .alias("holding_ratio"),
            ]
        )
        .sort("date")
        .with_columns(
            [
                pl.col("shares").shift(1).fill_null(0).alias("prev_shares"),
                pl.col("holding_ratio").shift(1).fill_null(0.0).alias("prev_holding_ratio"),
                pl.col("actual_weight").shift(1).fill_null(0.0).alias("prev_actual_weight"),
                pl.col("model_weight").shift(1).fill_null(0.0).alias("prev_model_weight"),
            ]
        )
        .with_columns(
            [
                (pl.col("shares") - pl.col("prev_shares")).alias("share_delta"),
                (pl.col("holding_ratio") - pl.col("prev_holding_ratio")).alias("holding_ratio_delta"),
                (pl.col("actual_weight") - pl.col("prev_actual_weight")).alias("actual_weight_delta"),
                (pl.col("model_weight") - pl.col("prev_model_weight")).alias("model_weight_delta"),
            ]
        )
    )


def load_stock_history(
    fold_dir: str | Path,
    symbol: str,
    *,
    limit: int = 32,
    changes_only: bool = True,
    initial_capital: float | None = None,
    current_capital: float | None = None,
    symbol_names: dict[str, str] | None = None,
    frequency: str | None = "daily",
) -> StockHistoryResult:
    root = Path(fold_dir)
    if not root.exists():
        raise FileNotFoundError(root)

    frames = []
    source_paths: list[Path] = []
    resolved_symbol: str | None = None

    model_frame, model_symbol, model_path = _read_wide_symbol_series(
        root,
        "daily_weights",
        symbol,
        "model_weight",
        frequency=frequency,
    )
    if model_path is not None:
        source_paths.append(model_path)
    if model_frame is not None:
        frames.append(model_frame)
        resolved_symbol = model_symbol or resolved_symbol

    actual_frame, actual_symbol, actual_path = _read_wide_symbol_series(
        root,
        "integer_share_daily_weights",
        symbol,
        "actual_weight",
        frequency=frequency,
    )
    if actual_path is not None:
        source_paths.append(actual_path)
    if actual_frame is not None:
        frames.append(actual_frame)
        resolved_symbol = resolved_symbol or actual_symbol

    holdings_frame, holdings_symbol, holdings_path = _read_holdings_symbol(
        root,
        symbol,
        frequency=frequency,
        timeline_frame=actual_frame,
        timeline_weight_column="actual_weight",
    )
    if holdings_path is not None:
        source_paths.append(holdings_path)
    if holdings_frame is not None:
        frames.append(holdings_frame)
        resolved_symbol = resolved_symbol or holdings_symbol

    returns_frame, returns_path = _read_returns(root, frequency=frequency)
    if returns_path is not None:
        source_paths.append(returns_path)
    if returns_frame is not None:
        frames.append(returns_frame)

    if resolved_symbol is None:
        available_hint = ""
        if model_path is not None:
            try:
                columns = [name for name in _read_table(model_path).columns if name != "date"]
                available_hint = f"; sample symbols={', '.join(columns[:8])}"
            except Exception:
                available_hint = ""
        raise ValueError(f"symbol `{symbol}` not found in {root}{available_hint}")
    if not frames:
        raise ValueError(f"no stock history tables found in {root}")

    frame = frames[0]
    for other in frames[1:]:
        frame = frame.join(other, on="date", how="full", coalesce=True)

    import polars as pl

    frame = _coalesce_stock_history_columns(frame)
    capital = resolve_fold_capital_scale(root, initial_capital=initial_capital, current_capital=current_capital)
    if capital.scale != 1.0:
        frame = frame.with_columns((pl.col("market_value") * capital.scale).alias("market_value"))
    frame = frame.with_columns(
        [
            pl.col("market_value").shift(1).fill_null(0.0).alias("prev_market_value"),
            (pl.col("market_value") - pl.col("market_value").shift(1).fill_null(0.0)).alias("market_value_delta"),
        ]
    )
    rows = frame.to_dicts()
    for row in rows:
        row["symbol"] = resolved_symbol
        row["name"] = _symbol_name(symbol_names, resolved_symbol)
        row["action"] = classify_stock_history_action(
            int(row.get("prev_shares") or 0),
            int(row.get("shares") or 0),
            holding_delta=float(row.get("holding_ratio_delta") or 0.0),
            model_delta=float(row.get("model_weight_delta") or 0.0),
            actual_delta=float(row.get("actual_weight_delta") or 0.0),
        )

    rows_desc = sorted(rows, key=lambda item: str(item.get("date") or ""), reverse=True)
    fell_back = False
    if changes_only:
        filtered = [row for row in rows_desc if str(row.get("action") or "HOLD") != "HOLD"]
        if filtered:
            rows_desc = filtered
        else:
            fell_back = True
    try:
        row_limit = int(limit)
    except Exception:
        row_limit = 32
    if row_limit > 0:
        rows_desc = rows_desc[:row_limit]

    return StockHistoryResult(
        requested_symbol=str(symbol),
        symbol=resolved_symbol,
        name=_symbol_name(symbol_names, resolved_symbol),
        fold_dir=root,
        rows=rows_desc,
        source_paths=tuple(dict.fromkeys(source_paths)),
        changes_only=bool(changes_only),
        fell_back_to_all_rows=fell_back,
        capital=capital,
    )
