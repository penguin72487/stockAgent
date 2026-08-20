#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.discord_bot.bot import (
    _load_portfolio_history_for_market,
    _market_fold_dir,
    _market_price_root,
    _portfolio_history_pages,
    _resolve_market,
)


DEFAULT_MARKETS = (
    "tw_day_trade_multi_basis",
    "tw_day_trade_100m",
    "tw_day_trade_multi_basis_projection_l1_gelu",
)
OPEN_PRICE_TOLERANCE = 1e-4
TOP_CHANGES_AUDIT_COUNT = 20
DISCORD_PAGE_MAX_CHARS = 4000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit every executed TW day-trade /portfolio_history row against fold artifacts and official OHLC."
    )
    parser.add_argument("--market", action="append", dest="markets", help="Market id; repeat as needed.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/discord_bot/portfolio_history_audit.json"),
    )
    return parser.parse_args()


def _price_frame(price_root: Path, start_date: str, end_date: str) -> pl.DataFrame:
    pattern = str(price_root / "*_features.parquet")
    return (
        pl.scan_parquet(
            pattern,
            include_file_paths="__path",
            extra_columns="ignore",
        )
        .filter(
            pl.col("date")
            .cast(pl.Date, strict=False)
            .is_between(pl.lit(start_date).str.to_date(), pl.lit(end_date).str.to_date())
        )
        .select(
            pl.col("date").cast(pl.Utf8).str.slice(0, 10).alias("date"),
            pl.col("__path")
            .str.extract(r"/([^/]+)_features\.parquet$", 1)
            .alias("symbol"),
            pl.col("open").cast(pl.Float64, strict=False).alias("official_open"),
            pl.col("close").cast(pl.Float64, strict=False).alias("official_close"),
        )
        .collect(engine="streaming")
    )


def _return_parity(fold_dir: Path) -> dict[str, Any]:
    returns_path = fold_dir / "integer_share_daily_portfolio_returns.parquet"
    archive_path = fold_dir / "test_integer_share_backtest.npz"
    if not returns_path.exists() or not archive_path.exists():
        return {
            "status": "fail",
            "reason": "missing integer-share return parquet or canonical NPZ",
        }
    frame = pl.read_parquet(returns_path).sort("date")
    with np.load(archive_path, allow_pickle=False) as archive:
        expected_dates = np.asarray(archive["dates"]).astype("datetime64[D]")
        expected_returns = np.asarray(archive["strategy_returns"], dtype=np.float64)
        expected_benchmark = np.asarray(archive["benchmark_returns"], dtype=np.float64)
        expected_turnover = np.asarray(archive["turnovers"], dtype=np.float64)
    actual_dates = frame["date"].to_numpy().astype("datetime64[D]")
    actual_returns = frame["portfolio_return"].to_numpy()
    actual_benchmark = frame["benchmark_return"].to_numpy()
    actual_turnover = frame["turnover"].to_numpy()
    same_dates = bool(np.array_equal(actual_dates, expected_dates))

    def max_diff(actual: np.ndarray, expected: np.ndarray) -> float | None:
        if actual.shape != expected.shape:
            return None
        return float(np.nanmax(np.abs(actual - expected))) if actual.size else 0.0

    strategy_diff = max_diff(actual_returns, expected_returns)
    benchmark_diff = max_diff(actual_benchmark, expected_benchmark)
    turnover_diff = max_diff(actual_turnover, expected_turnover)
    passed = bool(
        same_dates
        and strategy_diff is not None
        and benchmark_diff is not None
        and turnover_diff is not None
        and strategy_diff <= 1e-12
        and benchmark_diff <= 1e-7
        and turnover_diff <= 1e-12
    )
    return {
        "status": "pass" if passed else "fail",
        "same_dates": same_dates,
        "rows": int(frame.height),
        "max_abs_strategy_return_diff": strategy_diff,
        "max_abs_benchmark_return_diff": benchmark_diff,
        "max_abs_turnover_diff": turnover_diff,
        "returns_path": str(returns_path),
        "archive_path": str(archive_path),
    }


def audit_market(market: str) -> dict[str, Any]:
    cfg = _resolve_market(market)
    fold_dir = _market_fold_dir(cfg)
    holdings_path = fold_dir / "holdings.parquet"
    if not holdings_path.exists():
        raise FileNotFoundError(holdings_path)
    holdings = pl.read_parquet(holdings_path).with_columns(
        pl.col("date").cast(pl.Utf8).str.slice(0, 10).alias("date")
    )
    dates = holdings["date"].unique().sort()
    start_date = str(dates[0])
    end_date = str(dates[-1])
    non_cash = holdings.filter(~pl.col("is_cash").cast(pl.Boolean).fill_null(False))
    prices = _price_frame(Path(_market_price_root(cfg)), start_date, end_date)
    joined = non_cash.join(prices, on=["date", "symbol"], how="left").with_columns(
        (pl.col("price") - pl.col("official_open")).abs().alias("open_price_abs_diff")
    )
    missing_open = int(joined["official_open"].null_count())
    mismatched_open = int(
        joined.filter(
            pl.col("official_open").is_not_null()
            & (pl.col("open_price_abs_diff") > OPEN_PRICE_TOLERANCE)
        ).height
    )
    max_open_diff = joined["open_price_abs_diff"].max()
    invalid_record_types = 0
    if "record_type" in non_cash.columns:
        invalid_record_types = int(
            non_cash.filter(
                pl.col("record_type")
                .cast(pl.Utf8, strict=False)
                .fill_null("")
                .str.to_lowercase()
                != "day_trade_open"
            ).height
        )

    history = _load_portfolio_history_for_market(
        cfg,
        0,
        TOP_CHANGES_AUDIT_COUNT,
        0.0,
        None,
        None,
    )
    executed_rows = [
        row for row in history.rows if row.get("position_source") == "executed_history"
    ]
    bad_history_contracts = sum(
        1
        for row in executed_rows
        if row.get("price_contract") != "session_open_to_close"
        or row.get("intraday_price_included") is not False
        or row.get("source") != "integer_share_backtest"
    )
    pending_rows = [row for row in history.rows if row.get("position_source") == "signal_target"]
    bad_pending_rows = sum(
        1
        for row in pending_rows
        if str(row.get("date") or "")[11:19] != "09:00:00"
        or row.get("portfolio_return") is not None
        or row.get("benchmark_return") is not None
        or row.get("profit_value") is not None
        or row.get("price_contract") != "session_open_target"
        or row.get("intraday_price_included") is not False
    )
    pages = _portfolio_history_pages(cfg, history)
    bad_page_dates = sum(
        1
        for row, page in zip(history.rows, pages)
        if str(row.get("display_date") or "") not in page
    )
    missing_displayed_top_changes = sum(
        1
        for row, page in zip(history.rows, pages)
        for change in row.get("changes", [])
        if isinstance(change, dict) and str(change.get("symbol") or "") not in page
    )
    max_page_chars = max((len(page) for page in pages), default=0)
    one_period_per_page = len(pages) == len(history.rows)
    parity = _return_parity(fold_dir)
    passed = bool(
        missing_open == 0
        and mismatched_open == 0
        and invalid_record_types == 0
        and len(executed_rows) == int(dates.len())
        and bad_history_contracts == 0
        and bad_pending_rows == 0
        and one_period_per_page
        and bad_page_dates == 0
        and missing_displayed_top_changes == 0
        and max_page_chars <= DISCORD_PAGE_MAX_CHARS
        and parity.get("status") == "pass"
    )
    return {
        "market": market,
        "label": cfg.label,
        "fold_id": cfg.fold_id,
        "initial_capital": cfg.initial_capital,
        "status": "pass" if passed else "fail",
        "date_range": {"start": start_date, "end": end_date},
        "executed_history_rows": len(executed_rows),
        "pending_open_signal_rows": len(pending_rows),
        "non_cash_holding_rows": int(non_cash.height),
        "non_cash_symbols": int(non_cash["symbol"].n_unique()),
        "missing_official_open_rows": missing_open,
        "open_price_mismatch_rows": mismatched_open,
        "max_abs_open_price_diff": float(max_open_diff or 0.0),
        "open_price_tolerance": OPEN_PRICE_TOLERANCE,
        "invalid_holding_record_types": invalid_record_types,
        "bad_executed_history_contracts": bad_history_contracts,
        "bad_pending_open_rows": bad_pending_rows,
        "discord_pages": len(pages),
        "top_changes_audited": TOP_CHANGES_AUDIT_COUNT,
        "one_period_per_page": one_period_per_page,
        "bad_page_dates": bad_page_dates,
        "missing_displayed_top_changes": missing_displayed_top_changes,
        "max_page_chars": max_page_chars,
        "discord_page_max_chars": DISCORD_PAGE_MAX_CHARS,
        "return_artifact_parity": parity,
        "fold_dir": str(fold_dir),
        "holdings_path": str(holdings_path),
        "price_root": str(_market_price_root(cfg)),
    }


def main() -> None:
    args = _parse_args()
    markets = tuple(args.markets or DEFAULT_MARKETS)
    results = [audit_market(market) for market in markets]
    report = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "contract": "tw_day_trade_portfolio_history_session_open_to_close",
        "status": "pass" if all(item["status"] == "pass" for item in results) else "fail",
        "markets": results,
    }
    output = args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
