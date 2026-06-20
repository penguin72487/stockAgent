from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stockagent.live.capital import CapitalScale, resolve_capital_scale_from_nav
from stockagent.live.stock_history import (
    _artifact_path,
    _date_string_expr,
    _read_returns,
    _read_table,
    _symbol_name,
    classify_stock_history_action,
)


@dataclass(slots=True)
class PortfolioHistoryResult:
    fold_dir: Path
    rows: list[dict[str, Any]]
    source_paths: tuple[Path, ...]
    days: int
    top_changes: int
    start_date: str | None
    end_date: str | None
    period_return: float | None
    benchmark_return: float | None
    profit_value: float | None
    capital: CapitalScale


def _daily_holdings_summary(holdings):
    import polars as pl

    non_cash = ~pl.col("is_cash")
    long_leg = non_cash & (pl.col("market_value") > 0)
    short_leg = non_cash & (pl.col("market_value") < 0)
    active = non_cash & (pl.col("shares") != 0)
    return (
        holdings.group_by("date")
        .agg(
            [
                pl.col("market_value").sum().alias("nav"),
                pl.col("market_value").filter(pl.col("is_cash")).sum().alias("cash_value"),
                pl.col("market_value").filter(non_cash).abs().sum().alias("gross_exposure"),
                pl.col("market_value").filter(non_cash).sum().alias("net_exposure"),
                pl.col("market_value").filter(long_leg).sum().alias("long_exposure"),
                (-pl.col("market_value").filter(short_leg).sum()).alias("short_exposure"),
                pl.col("symbol").filter(active).count().alias("position_count"),
                pl.col("symbol").filter(long_leg).count().alias("long_count"),
                pl.col("symbol").filter(short_leg).count().alias("short_count"),
            ]
        )
        .sort("date")
        .with_columns(
            [
                (pl.col("cash_value") / pl.col("nav")).alias("cash_ratio"),
                (pl.col("gross_exposure") / pl.col("nav")).alias("gross_ratio"),
                (pl.col("net_exposure") / pl.col("nav")).alias("net_ratio"),
                (pl.col("long_exposure") / pl.col("nav")).alias("long_ratio"),
                (pl.col("short_exposure") / pl.col("nav")).alias("short_ratio"),
            ]
        )
    )


def _join_daily_returns(daily, returns):
    import polars as pl

    if returns is not None:
        daily = daily.join(returns, on="date", how="left")
    for name in ("portfolio_return", "benchmark_return", "turnover"):
        if name not in daily.columns:
            daily = daily.with_columns(pl.lit(None, dtype=pl.Float64).alias(name))
    return daily.sort("date")


def _with_profit_estimates(daily):
    import polars as pl

    return daily.with_columns(pl.col("nav").shift(1).alias("prev_nav")).with_columns(
        [
            pl.when(pl.col("portfolio_return").is_null())
            .then(None)
            .when(pl.col("prev_nav").is_not_null())
            .then(pl.col("prev_nav") * pl.col("portfolio_return"))
            .otherwise(pl.col("nav") * pl.col("portfolio_return") / (1.0 + pl.col("portfolio_return")))
            .alias("profit_value")
        ]
    )


def _change_row(
    symbol: str,
    current: dict[str, Any] | None,
    previous: dict[str, Any] | None,
    *,
    symbol_names: dict[str, str] | None,
) -> dict[str, Any]:
    shares = int((current or {}).get("shares") or 0)
    prev_shares = int((previous or {}).get("shares") or 0)
    holding_ratio = float((current or {}).get("holding_ratio") or 0.0)
    prev_holding_ratio = float((previous or {}).get("holding_ratio") or 0.0)
    market_value = float((current or {}).get("market_value") or 0.0)
    prev_market_value = float((previous or {}).get("market_value") or 0.0)
    action = classify_stock_history_action(
        prev_shares,
        shares,
        holding_delta=holding_ratio - prev_holding_ratio,
        actual_delta=holding_ratio - prev_holding_ratio,
    )
    return {
        "symbol": symbol,
        "name": _symbol_name(symbol_names, symbol),
        "action": action,
        "shares": shares,
        "prev_shares": prev_shares,
        "share_delta": shares - prev_shares,
        "price": (current or previous or {}).get("price"),
        "market_value": market_value,
        "prev_market_value": prev_market_value,
        "market_value_delta": market_value - prev_market_value,
        "holding_ratio": holding_ratio,
        "prev_holding_ratio": prev_holding_ratio,
        "holding_ratio_delta": holding_ratio - prev_holding_ratio,
    }


def _records_by_date(holdings) -> dict[str, dict[str, dict[str, Any]]]:
    rows = (
        holdings.filter(~holdings["is_cash"])
        .select(["date", "symbol", "shares", "price", "market_value", "holding_ratio"])
        .to_dicts()
    )
    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for row in rows:
        date = str(row.get("date"))
        symbol = str(row.get("symbol"))
        by_date.setdefault(date, {})[symbol] = row
    return by_date


def _daily_changes(
    *,
    date: str,
    previous_date: str | None,
    holdings_by_date: dict[str, dict[str, dict[str, Any]]],
    symbol_names: dict[str, str] | None,
    min_abs_change: float,
    top_changes: int,
    capital_scale: float,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    current = holdings_by_date.get(date, {})
    previous = holdings_by_date.get(previous_date or "", {})
    symbols = set(current) | set(previous)
    changes: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for symbol in symbols:
        row = _change_row(symbol, current.get(symbol), previous.get(symbol), symbol_names=symbol_names)
        action = str(row["action"])
        if action == "HOLD":
            continue
        if abs(float(row["holding_ratio_delta"])) < min_abs_change and int(row["share_delta"]) == 0:
            continue
        for key in ("market_value", "prev_market_value", "market_value_delta"):
            row[key] = float(row.get(key) or 0.0) * capital_scale
        counts[action] = counts.get(action, 0) + 1
        changes.append(row)
    changes.sort(
        key=lambda item: (
            abs(int(item.get("share_delta") or 0)) > 0,
            abs(float(item.get("holding_ratio_delta") or 0.0)),
            abs(float(item.get("market_value") or 0.0)),
        ),
        reverse=True,
    )
    return changes[: max(0, int(top_changes))], counts


def _period_total_return(rows: list[dict[str, Any]], key: str) -> float | None:
    total = 1.0
    seen = False
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        total *= 1.0 + float(value)
        seen = True
    return total - 1.0 if seen else None


def load_portfolio_history(
    fold_dir: str | Path,
    *,
    days: int = 32,
    top_changes: int = 5,
    min_abs_change: float = 0.0,
    initial_capital: float | None = None,
    current_capital: float | None = None,
    symbol_names: dict[str, str] | None = None,
) -> PortfolioHistoryResult:
    root = Path(fold_dir)
    holdings_path = _artifact_path(root, "holdings")
    if holdings_path is None:
        raise FileNotFoundError(root / "holdings.parquet")

    import polars as pl

    holdings = _read_table(holdings_path)
    required = {"date", "symbol", "shares", "price", "market_value", "holding_ratio", "is_cash"}
    missing = sorted(required - set(holdings.columns))
    if missing:
        raise ValueError(f"{holdings_path} missing columns: {', '.join(missing)}")
    holdings = holdings.select(
        [
            _date_string_expr(),
            pl.col("symbol").cast(pl.Utf8).alias("symbol"),
            pl.col("shares").cast(pl.Int64, strict=False).fill_null(0).alias("shares"),
            pl.col("price").cast(pl.Float64, strict=False).alias("price"),
            pl.col("market_value").cast(pl.Float64, strict=False).fill_null(0.0).alias("market_value"),
            pl.col("holding_ratio").cast(pl.Float64, strict=False).fill_null(0.0).alias("holding_ratio"),
            pl.col("is_cash").cast(pl.Boolean).fill_null(False).alias("is_cash"),
        ]
    )

    returns, returns_path = _read_returns(root)
    daily = _with_profit_estimates(_join_daily_returns(_daily_holdings_summary(holdings), returns))
    capital = resolve_capital_scale_from_nav(
        daily.select(["date", "nav"]).to_dicts(),
        initial_capital=initial_capital,
        current_capital=current_capital,
    )
    money_columns = [
        "nav",
        "prev_nav",
        "cash_value",
        "gross_exposure",
        "net_exposure",
        "long_exposure",
        "short_exposure",
        "profit_value",
    ]
    if capital.scale != 1.0:
        daily = daily.with_columns([(pl.col(name) * capital.scale).alias(name) for name in money_columns if name in daily.columns])
    dates = daily["date"].to_list()
    try:
        day_count = int(days)
    except Exception:
        day_count = 32
    if day_count <= 0:
        selected_dates = dates
    else:
        selected_dates = dates[-day_count:]

    selected = daily.filter(pl.col("date").is_in(selected_dates)).sort("date")
    selected_rows = selected.to_dicts()
    period_return = _period_total_return(selected_rows, "portfolio_return")
    benchmark_return = _period_total_return(selected_rows, "benchmark_return")
    profit_value = sum(float(row.get("profit_value") or 0.0) for row in selected_rows)

    holdings_by_date = _records_by_date(holdings)
    date_index = {str(date): idx for idx, date in enumerate(dates)}
    output_rows: list[dict[str, Any]] = []
    cumulative = 1.0
    cumulative_values: dict[str, float | None] = {}
    for row in selected_rows:
        value = row.get("portfolio_return")
        if value is None:
            cumulative_values[str(row["date"])] = None
        else:
            cumulative *= 1.0 + float(value)
            cumulative_values[str(row["date"])] = cumulative - 1.0

    for row in reversed(selected_rows):
        date = str(row["date"])
        idx = date_index.get(date, 0)
        previous_date = str(dates[idx - 1]) if idx > 0 else None
        changes, change_counts = _daily_changes(
            date=date,
            previous_date=previous_date,
            holdings_by_date=holdings_by_date,
            symbol_names=symbol_names,
            min_abs_change=float(min_abs_change),
            top_changes=top_changes,
            capital_scale=capital.scale,
        )
        row = dict(row)
        row["cumulative_return"] = cumulative_values.get(date)
        row["changes"] = changes
        row["change_counts"] = change_counts
        row["change_count"] = sum(change_counts.values())
        output_rows.append(row)

    source_paths = [holdings_path]
    if returns_path is not None:
        source_paths.append(returns_path)
    return PortfolioHistoryResult(
        fold_dir=root,
        rows=output_rows,
        source_paths=tuple(source_paths),
        days=len(selected_rows),
        top_changes=int(top_changes),
        start_date=str(selected_dates[0]) if selected_dates else None,
        end_date=str(selected_dates[-1]) if selected_dates else None,
        period_return=period_return,
        benchmark_return=benchmark_return,
        profit_value=profit_value,
        capital=capital,
    )
