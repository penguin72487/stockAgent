#!/usr/bin/env python3
"""Backfill the read-only 0050, 2330, and continuous-TX dashboard baselines.

No broker order or Shioaji request is made.  Completed stock sessions come
from the retained official daily files, the current stock open comes from the
same-session snapshot parquet, and TX uses the locally captured executable
front-month book.  The output is a small immutable origin/mark file consumed
by the dashboard; it never mutates the running paper-execution state.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.run_tw_day_trade_simulation import _mode_specs  # noqa: E402
from stockagent.live.tw_day_trade_dashboard import (  # noqa: E402
    BENCHMARK_HISTORY_FILENAME,
)
from stockagent.live.tw_day_trade_simulation import (  # noqa: E402
    DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH,
    STOCK_BENCHMARKS,
    TX_CONTINUOUS_BENCHMARK_ID,
    TwDayTradeSimulationEngine,
)
from stockagent.data.tw_index_futures import (  # noqa: E402
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
)
from stockagent.research.taifex_capital_returns import (  # noqa: E402
    taifex_initial_margin_twd,
)
from stockagent.research.taifex_transaction_tax import (  # noqa: E402
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


TAIPEI = ZoneInfo("Asia/Taipei")
TX_FEE_PER_SIDE_TWD = 60.0
DEFAULT_TWSE_DAILY_OHLCV_PATH = Path(
    "/srv/stockagent-live/data_tw_public/twse_daily_ohlcv.parquet"
)
DEFAULT_TPEX_DAILY_OHLCV_PATH = Path(
    "/srv/stockagent-live/data_tw_public/tpex_daily_ohlcv.parquet"
)
DEFAULT_TX_HISTORY_ROOT = Path("data_tw_index_futures/shioaji_history/TXFR1")
TX_SESSION_MINUTES = 300


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _minute(value: datetime) -> str:
    return value.replace(second=0, microsecond=0).isoformat(timespec="minutes")


def _mark_base(
    *,
    benchmark_id: str,
    label: str,
    instrument_type: str,
    observed: datetime,
) -> dict[str, Any]:
    return {
        "benchmark_id": benchmark_id,
        "label": label,
        "instrument_type": instrument_type,
        "recorded_at": observed.isoformat(timespec="seconds"),
        "minute": _minute(observed),
        "session_date": observed.date().isoformat(),
        "benchmark_origin_rebased": True,
        "benchmark_origin_session_date": observed.date().isoformat(),
        "counterfactual_open_replay": True,
        "replay_basis": "actual_session_open_to_recorded_executable_marks",
        "valuation_stale": False,
    }


def _stock_daily_row(
    path: Path,
    trading_date: date,
    *,
    symbol: str,
    twse_daily_ohlcv_path: Path,
    tpex_daily_ohlcv_path: Path,
) -> dict[str, Any] | None:
    if path.is_file():
        frame = (
            pl.scan_parquet(path)
            .filter(pl.col("date") == trading_date)
            .select("date", "open", "close")
            .collect()
        )
        if frame.height > 1:
            raise ValueError(
                f"duplicate per-symbol daily row for {symbol} on {trading_date}"
            )
        if frame.height == 1:
            return frame.row(0, named=True)

    def _price(column: str, alias: str) -> pl.Expr:
        return (
            pl.col(column)
            .cast(pl.String)
            .str.strip_chars()
            .str.replace_all(",", "")
            .cast(pl.Float64, strict=False)
            .alias(alias)
        )

    venue_specs = (
        (twse_daily_ohlcv_path, "證券代號", "開盤價", "收盤價"),
        (tpex_daily_ohlcv_path, "代號", "開盤", "收盤"),
    )
    matches: list[dict[str, Any]] = []
    for aggregate_path, symbol_column, open_column, close_column in venue_specs:
        if not aggregate_path.is_file():
            continue
        frame = (
            pl.scan_parquet(aggregate_path)
            .filter(
                (pl.col("date").cast(pl.String) == trading_date.isoformat())
                & (pl.col(symbol_column).cast(pl.String).str.strip_chars() == symbol)
            )
            .select(
                _price(open_column, "open"),
                _price(close_column, "close"),
            )
            .collect(engine="streaming")
        )
        matches.extend(frame.to_dicts())
    if len(matches) > 1:
        raise ValueError(
            f"duplicate official aggregate row for {symbol} on {trading_date}"
        )
    if matches:
        return {"date": trading_date, **matches[0]}
    return None


def _current_open_rows(path: Path, trading_date: date) -> dict[str, float]:
    if not path.is_file():
        return {}
    frame = pl.read_parquet(path).filter(pl.col("trading_date") == trading_date)
    return {
        str(row["symbol"]): float(row["open_price"])
        for row in frame.to_dicts()
        if bool(row.get("snapshot_available"))
        and _finite(row.get("open_price")) is not None
        and float(row["open_price"]) > 0.0
    }


def _stock_mark(
    *,
    benchmark_id: str,
    symbol: str,
    label: str,
    quantity: int,
    entry_price: float,
    entry_at: datetime,
    entry_fee: float,
    mark_price: float,
    observed: datetime,
    buy_rate: float,
    sell_rate: float,
    fee_schedule: Any,
    source: str,
    corporate_action_factor: float,
    corporate_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    adjusted_quantity = quantity * corporate_action_factor
    liquidation_notional = adjusted_quantity * mark_price
    commission, tax = TwDayTradeSimulationEngine._stock_benchmark_order_cost(
        notional=liquidation_notional,
        commission_rate=buy_rate,
        tax_rate=max(0.0, sell_rate - buy_rate),
        fee_schedule=fee_schedule,
    )
    initial_capital = quantity * entry_price
    net_pnl = liquidation_notional - initial_capital - entry_fee - commission - tax
    total_equity = initial_capital + net_pnl
    row = _mark_base(
        benchmark_id=benchmark_id,
        label=label,
        instrument_type="stock_buy_and_hold",
        observed=observed,
    )
    row.update(
        {
            "benchmark_origin_session_date": entry_at.date().isoformat(),
            "symbol": symbol,
            "quantity": quantity,
            "adjusted_quantity": adjusted_quantity,
            "entry_price": entry_price,
            "entry_at": entry_at.isoformat(timespec="seconds"),
            "initial_capital_twd": initial_capital,
            "capital_basis": "one_board_lot_actual_open_notional",
            "initial_fixed_fees_twd": entry_fee,
            "fixed_fees_twd": entry_fee,
            "transaction_tax_twd": 0.0,
            "last_mark_price": mark_price,
            "last_mark_at": observed.isoformat(timespec="seconds"),
            "last_quote_at": observed.isoformat(timespec="seconds"),
            "liquidation_cost_twd": commission + tax,
            "net_pnl_twd": net_pnl,
            "total_equity_twd": total_equity,
            "return_fraction": net_pnl / initial_capital,
            "return_pct": net_pnl / initial_capital * 100.0,
            "return_type": "total_return",
            "total_return_contract": "official_ex_date_reference_reinvestment_v1",
            "corporate_action_factor": corporate_action_factor,
            "corporate_action_count": len(corporate_actions),
            "last_corporate_action_date": (
                corporate_actions[-1]["date"] if corporate_actions else None
            ),
            "corporate_action_coverage": True,
            "corporate_action_status": "official_reference_complete",
            "applied_corporate_actions": corporate_actions,
            "source": source,
            "valuation_source": (
                "total_return_units_marked_at_recorded_bid_after_tw_cash_costs"
            ),
        }
    )
    return row


def _tx_tax(price: float, trading_date: date) -> float:
    return taifex_tax_per_contract_twd(
        price,
        multiplier_twd_per_point=TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
        tax_rate=stock_index_futures_tax_rate(trading_date),
    )


def _tx_day_books(
    *,
    capture_root: Path,
    trading_date: date,
    contract_code: str,
    end_at: datetime,
) -> pl.DataFrame:
    partition = capture_root / "book_1s" / f"trade_date={trading_date.isoformat()}"
    if not partition.is_dir():
        raise FileNotFoundError(partition)
    start = datetime.combine(trading_date, time(8, 45), tzinfo=TAIPEI)
    start_ns = int(start.timestamp() * 1_000_000_000)
    end_ns = int(end_at.timestamp() * 1_000_000_000)
    frame = (
        pl.scan_parquet(str(partition / "hour=*" / "*.parquet"))
        .filter(
            (pl.col("code") == contract_code)
            & (pl.col("snapshot_ts_ns") >= start_ns)
            & (pl.col("snapshot_ts_ns") <= end_ns)
            & (~pl.col("stale"))
            & (~pl.col("suspend"))
            & (~pl.col("simtrade"))
            & (pl.col("bid_price_1") > 0.0)
            & (pl.col("ask_price_1") > 0.0)
            & (pl.col("bid_price_1") <= pl.col("ask_price_1"))
        )
        .select("snapshot_ts_ns", "bid_price_1", "ask_price_1")
        .sort("snapshot_ts_ns")
        .collect(engine="streaming")
    )
    if frame.is_empty():
        raise ValueError(
            f"no valid local TX book for {contract_code} on {trading_date}"
        )
    return frame


def _tx_front_contract_metadata(
    *,
    capture_root: Path,
    trading_date: date,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_root = (
        capture_root / "manifests" / f"trade_date={trading_date.isoformat()}"
    )
    manifest_paths = sorted(manifest_root.glob("worker=*.json"))
    if not manifest_paths:
        raise FileNotFoundError(manifest_root)
    matches: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    for path in manifest_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if str(payload.get("status") or "") != "complete":
            raise ValueError(f"incomplete FOP capture manifest: {path}")
        receipts.append({"path": str(path.resolve()), "sha256": _sha256(path)})
        matches.extend(
            dict(row)
            for row in payload.get("contract_metadata") or ()
            if str(row.get("logical_code") or "").strip().upper() == "TXFR1"
        )
    identities = {
        (
            str(row.get("code") or "").strip().upper(),
            str(row.get("delivery_month") or "").strip(),
            str(row.get("last_trading_date") or "").strip(),
        )
        for row in matches
    }
    if len(identities) != 1:
        raise ValueError(
            f"ambiguous TXFR1 capture metadata on {trading_date}: {sorted(identities)}"
        )
    code, delivery_month, last_trading_date = next(iter(identities))
    if not code or len(delivery_month) != 6 or not last_trading_date:
        raise ValueError(
            f"incomplete TXFR1 capture metadata on {trading_date}: {identities}"
        )
    return {
        "code": code,
        "delivery_month": delivery_month,
        "last_trading_date": last_trading_date,
    }, receipts


def _tx_contract_code(delivery_month: str) -> str:
    month_codes = "ABCDEFGHIJKL"
    if len(delivery_month) != 6 or not delivery_month.isdigit():
        raise ValueError(f"invalid TX delivery month: {delivery_month!r}")
    month = int(delivery_month[4:])
    if not 1 <= month <= 12:
        raise ValueError(f"invalid TX delivery month: {delivery_month!r}")
    return f"TXF{month_codes[month - 1]}{delivery_month[3]}"


def _tx_historical_contract_metadata(
    *,
    final_settlement_path: Path,
    trading_date: date,
) -> dict[str, Any]:
    """Resolve the held monthly TX contract from official expiry history."""

    rows = (
        pl.scan_parquet(final_settlement_path)
        .filter(
            (pl.col("product") == "TXO")
            & pl.col("option_series").cast(pl.String).str.contains(r"^\d{6}$")
            & (pl.col("settlement_date") >= trading_date)
        )
        .select("settlement_date", "option_series")
        .sort("settlement_date")
        .limit(1)
        .collect()
    )
    if rows.height != 1:
        raise RuntimeError(
            f"official monthly TX expiry is unavailable for {trading_date}"
        )
    row = rows.row(0, named=True)
    delivery_month = str(row["option_series"])
    return {
        "code": _tx_contract_code(delivery_month),
        "delivery_month": delivery_month,
        "last_trading_date": row["settlement_date"].isoformat(),
    }


def _tx_historical_day_books(
    *,
    history_root: Path,
    trading_date: date,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    receipt_path = (
        history_root / "receipts" / (f"trading_date={trading_date.isoformat()}.json")
    )
    if not receipt_path.is_file():
        raise FileNotFoundError(receipt_path)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if (
        str(receipt.get("status") or "") != "complete"
        or str(receipt.get("contract") or "").upper() != "TXFR1"
        or str(receipt.get("trading_date") or "") != trading_date.isoformat()
    ):
        raise ValueError(f"invalid TXFR1 historical receipt: {receipt_path}")
    data_path = Path(str(receipt.get("path") or ""))
    if not data_path.is_absolute():
        data_path = REPO_ROOT / data_path
    if not data_path.is_file():
        raise FileNotFoundError(data_path)
    if _sha256(data_path) != str(receipt.get("sha256") or ""):
        raise RuntimeError(f"TXFR1 historical hash mismatch: {data_path}")
    frame = (
        pl.scan_parquet(data_path)
        .filter(
            (pl.col("event_ts").dt.time() >= time(8, 45))
            & (pl.col("event_ts").dt.time() < time(13, 45))
            & (pl.col("bid_price") > 0.0)
            & (pl.col("ask_price") > 0.0)
            & (pl.col("bid_price") <= pl.col("ask_price"))
        )
        .select(
            "event_ts",
            pl.col("bid_price").alias("bid_price_1"),
            pl.col("ask_price").alias("ask_price_1"),
        )
        .sort("event_ts")
        .collect(engine="streaming")
    )
    if frame.is_empty():
        raise RuntimeError(
            f"no valid receipt-backed TXFR1 historical quote on {trading_date}"
        )
    return frame, {
        "path": str(receipt_path.resolve()),
        "sha256": _sha256(receipt_path),
        "data_path": str(data_path.resolve()),
        "data_sha256": _sha256(data_path),
        "rows": int(receipt.get("rows") or 0),
        "source": receipt.get("source"),
    }


def _tx_complete_minute_books(
    books: pl.DataFrame,
    *,
    trading_date: date,
    timestamp_column: str,
    epoch_utc: bool,
) -> list[tuple[datetime, dict[str, Any], bool]]:
    """Return exactly 08:45..13:44, carrying only an observed prior quote."""

    by_minute: dict[str, dict[str, Any]] = {}
    for book in books.iter_rows(named=True):
        if epoch_utc:
            observed = datetime.fromtimestamp(
                int(book[timestamp_column]) / 1e9, tz=timezone.utc
            ).astimezone(TAIPEI)
        else:
            raw = book[timestamp_column]
            if not isinstance(raw, datetime):
                raise TypeError(f"invalid TX history timestamp: {raw!r}")
            observed = raw.replace(tzinfo=TAIPEI)
        by_minute[_minute(observed)] = book

    start = datetime.combine(trading_date, time(8, 45), tzinfo=TAIPEI)
    current: dict[str, Any] | None = None
    output: list[tuple[datetime, dict[str, Any], bool]] = []
    for offset in range(TX_SESSION_MINUTES):
        observed = start + timedelta(minutes=offset)
        fresh = _minute(observed) in by_minute
        if fresh:
            current = by_minute[_minute(observed)]
        if current is None:
            raise RuntimeError(f"TX first minute has no valid quote on {trading_date}")
        output.append((observed, dict(current), fresh))
    return output


def _tx_engine_history_mark(
    engine: TwDayTradeSimulationEngine,
    *,
    observed: datetime,
) -> dict[str, Any]:
    source = dict(engine.state["benchmarks"][TX_CONTINUOUS_BENCHMARK_ID])
    row = _mark_base(
        benchmark_id=TX_CONTINUOUS_BENCHMARK_ID,
        label="台指期無限轉倉（大台一口）",
        instrument_type="continuous_long_future",
        observed=observed,
    )
    row.update(source)
    entry_at = datetime.fromisoformat(str(source["origin_entry_at"])).astimezone(TAIPEI)
    row.update(
        {
            "recorded_at": observed.isoformat(timespec="seconds"),
            "minute": _minute(observed),
            "session_date": observed.date().isoformat(),
            "benchmark_origin_rebased": True,
            "benchmark_origin_session_date": entry_at.date().isoformat(),
            "counterfactual_open_replay": True,
            "replay_basis": (
                "retained_executable_front_month_books_with_official_expiry_settlement"
            ),
        }
    )
    return row


def _tx_mark(
    *,
    entry_price: float,
    entry_at: datetime,
    initial_capital: float,
    initial_tax: float,
    mark_price: float,
    observed: datetime,
    contract_code: str,
) -> dict[str, Any]:
    multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"]
    liquidation_tax = _tx_tax(mark_price, observed.date())
    liquidation_cost = TX_FEE_PER_SIDE_TWD + liquidation_tax
    net_pnl = (
        (mark_price - entry_price) * multiplier
        - TX_FEE_PER_SIDE_TWD
        - initial_tax
        - liquidation_cost
    )
    total_equity = initial_capital + net_pnl
    row = _mark_base(
        benchmark_id=TX_CONTINUOUS_BENCHMARK_ID,
        label="台指期無限轉倉（大台一口）",
        instrument_type="continuous_long_future",
        observed=observed,
    )
    row.update(
        {
            "benchmark_origin_session_date": entry_at.date().isoformat(),
            "logical_code": "TXFR1",
            "contract_code": contract_code,
            "multiplier_twd_per_point": multiplier,
            "entry_price": entry_price,
            "entry_at": entry_at.isoformat(timespec="seconds"),
            "initial_capital_twd": initial_capital,
            "capital_basis": "official_taifex_initial_margin_at_actual_open",
            "initial_fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
            "initial_transaction_tax_twd": initial_tax,
            "fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
            "transaction_tax_twd": initial_tax,
            "realized_gross_pnl_twd": 0.0,
            "last_mark_price": mark_price,
            "last_mark_at": observed.isoformat(timespec="seconds"),
            "last_quote_at": observed.isoformat(timespec="seconds"),
            "liquidation_cost_twd": liquidation_cost,
            "net_pnl_twd": net_pnl,
            "total_equity_twd": total_equity,
            "return_fraction": net_pnl / initial_capital,
            "return_pct": net_pnl / initial_capital * 100.0,
            "roll_count": 0,
            "source": "retained_shioaji_fop_book_1s",
            "valuation_source": (
                "actual_front_month_ask_entry_bid_liquidation_with_fee_and_tax"
            ),
        }
    )
    return row


def _iter_dates(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _completed_stock_benchmark_sessions(
    *,
    start: date,
    end: date,
    stock_parquet_root: Path,
    twse_daily_ohlcv_path: Path,
    tpex_daily_ohlcv_path: Path,
) -> list[date]:
    """Resolve completed sessions from each benchmark's official price source.

    Benchmark history is independently reproducible and must not be capped by
    the strategy replay receipt.  A date is admitted only when both stock
    benchmarks have valid official OHLC endpoints; partial coverage fails
    closed instead of silently dropping one benchmark.
    """

    sessions: list[date] = []
    for trading_date in _iter_dates(start, end):
        rows: dict[str, dict[str, Any] | None] = {}
        for _benchmark_id, symbol, _label, _security_type in STOCK_BENCHMARKS:
            rows[symbol] = _stock_daily_row(
                stock_parquet_root / f"{symbol}_features.parquet",
                trading_date,
                symbol=symbol,
                twse_daily_ohlcv_path=twse_daily_ohlcv_path,
                tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
            )
        available = {symbol: row is not None for symbol, row in rows.items()}
        if not any(available.values()):
            continue
        if not all(available.values()):
            raise RuntimeError(
                "partial official stock benchmark session coverage on "
                f"{trading_date}: {available}"
            )
        for symbol, row in rows.items():
            assert row is not None
            for column in ("open", "close"):
                value = _finite(row.get(column))
                if value is None or value <= 0.0:
                    raise RuntimeError(
                        f"invalid official {symbol} {column} on {trading_date}"
                    )
        sessions.append(trading_date)
    return sessions


def _required_stock_adjustment(
    engine: TwDayTradeSimulationEngine,
    *,
    symbol: str,
    entry_at: datetime,
    mark_date: date,
    current_session_reference: dict[str, Any] | None = None,
) -> tuple[float, list[dict[str, Any]]]:
    current_kwargs: dict[str, Any] = {}
    if (
        isinstance(current_session_reference, dict)
        and str(current_session_reference.get("corporate_action_coverage_end") or "")
        == mark_date.isoformat()
    ):
        current_kwargs = {
            "current_reference_price": current_session_reference.get(
                "current_session_reference_price"
            ),
            "current_reference_source": current_session_reference.get(
                "current_session_reference_source"
            ),
            "previous_close": current_session_reference.get("previous_official_close"),
            "previous_close_date": current_session_reference.get(
                "previous_official_close_date"
            ),
            "previous_close_source": current_session_reference.get(
                "previous_official_close_source"
            ),
        }
    factor, actions, status = engine._stock_total_return_adjustment(
        symbol=symbol,
        entry_at=entry_at,
        mark_date=mark_date,
        **current_kwargs,
    )
    if factor is None:
        raise RuntimeError(
            f"{symbol} corporate-action coverage failed at {mark_date}: {status}"
        )
    return factor, actions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--markets-dir", type=Path, default=Path("services/discord_bot/markets")
    )
    parser.add_argument(
        "--stock-parquet-root",
        type=Path,
        default=Path("/srv/stockagent-live/data_tw_public/stocks"),
    )
    parser.add_argument(
        "--corporate-action-reference",
        type=Path,
        default=Path(
            "/srv/stockagent-live/data_tw_public/tw_corporate_action_reference.parquet"
        ),
    )
    parser.add_argument(
        "--twse-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TWSE_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--tpex-daily-ohlcv-path",
        type=Path,
        default=DEFAULT_TPEX_DAILY_OHLCV_PATH,
    )
    parser.add_argument(
        "--fop-capture-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks/shioaji_fop_captures"),
    )
    parser.add_argument(
        "--tx-history-root",
        type=Path,
        default=DEFAULT_TX_HISTORY_ROOT,
        help=(
            "Receipt-backed Shioaji TXFR1 historical Tick root used before "
            "retained realtime book_1s capture coverage begins."
        ),
    )
    parser.add_argument(
        "--final-settlement-path",
        type=Path,
        default=DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH,
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    state_dir = args.state_dir.resolve()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    state_path = state_dir / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    replay_receipt_path = state_dir / "rebuild_receipt.json"
    replay_receipt = json.loads(replay_receipt_path.read_text(encoding="utf-8"))
    replay_session_dates = [
        date.fromisoformat(str(session["session_date"]))
        for session in replay_receipt.get("sessions") or ()
        if start <= date.fromisoformat(str(session["session_date"])) <= end
    ]
    live_benchmarks = state.get("benchmarks") or {}
    required_ids = {
        *(benchmark_id for benchmark_id, *_rest in STOCK_BENCHMARKS),
        TX_CONTINUOUS_BENCHMARK_ID,
    }
    if not required_ids.issubset(live_benchmarks):
        raise ValueError(
            f"live benchmark state is incomplete: {sorted(required_ids - set(live_benchmarks))}"
        )

    specs, _configs, errors = _mode_specs(args.markets_dir)
    if errors or not specs:
        raise RuntimeError(f"cannot resolve stock fee schedule: {errors}")
    fee_schedule = specs[0].fee_schedule
    adjustment_engine = TwDayTradeSimulationEngine(state_dir)
    corporate_action_path = args.corporate_action_reference.resolve()
    adjustment_engine._load_corporate_actions(corporate_action_path)
    if adjustment_engine._corporate_action_load_error is not None:
        raise RuntimeError(
            "corporate-action reference is required for total-return benchmarks: "
            f"{adjustment_engine._corporate_action_load_error}"
        )
    now = datetime.now(TAIPEI)
    current_session_unclosed = end == now.date() and now.timetz().replace(
        tzinfo=None
    ) < time(13, 30)
    current_open_path = state_dir / "replay_open_data" / f"{end.isoformat()}.parquet"
    current_opens = (
        _current_open_rows(current_open_path, end) if current_session_unclosed else {}
    )
    twse_daily_ohlcv_path = args.twse_daily_ohlcv_path.resolve()
    tpex_daily_ohlcv_path = args.tpex_daily_ohlcv_path.resolve()
    for official_path in (twse_daily_ohlcv_path, tpex_daily_ohlcv_path):
        if not official_path.is_file():
            raise FileNotFoundError(official_path)
    completed_end = end - timedelta(days=1) if current_session_unclosed else end
    session_dates = (
        _completed_stock_benchmark_sessions(
            start=start,
            end=completed_end,
            stock_parquet_root=args.stock_parquet_root.resolve(),
            twse_daily_ohlcv_path=twse_daily_ohlcv_path,
            tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
        )
        if completed_end >= start
        else []
    )
    if current_session_unclosed:
        if end not in replay_session_dates or not current_opens:
            raise ValueError(
                "unclosed benchmark session requires retained replay receipt and opens"
            )
        session_dates.append(end)
    if not session_dates or session_dates[0] != start or session_dates[-1] != end:
        raise ValueError(
            "benchmark range must exactly match official benchmark session boundaries"
        )
    unknown_replay_sessions = sorted(set(replay_session_dates) - set(session_dates))
    if unknown_replay_sessions:
        raise ValueError(
            "strategy replay receipt contains sessions absent from official benchmark "
            f"sources: {[day.isoformat() for day in unknown_replay_sessions]}"
        )

    origins: dict[str, dict[str, Any]] = {}
    marks: list[dict[str, Any]] = []
    provenance: dict[str, Any] = {
        "state_path": str(state_path),
        "state_sha256": _sha256(state_path),
        "replay_receipt_path": str(replay_receipt_path),
        "replay_receipt_sha256": _sha256(replay_receipt_path),
        "stock_files": {},
        "current_open_path": (
            str(current_open_path) if current_session_unclosed else None
        ),
        "current_open_sha256": (
            _sha256(current_open_path) if current_session_unclosed else None
        ),
        "twse_daily_ohlcv_path": str(twse_daily_ohlcv_path),
        "twse_daily_ohlcv_sha256": _sha256(twse_daily_ohlcv_path),
        "tpex_daily_ohlcv_path": str(tpex_daily_ohlcv_path),
        "tpex_daily_ohlcv_sha256": _sha256(tpex_daily_ohlcv_path),
        "fop_capture_root": str(args.fop_capture_root.resolve()),
        "tx_history_root": str(args.tx_history_root.resolve()),
        "corporate_action_reference_path": str(corporate_action_path),
        "corporate_action_reference_sha256": _sha256(corporate_action_path),
        "corporate_action_reference_summary_path": str(
            corporate_action_path.with_suffix(".summary.json")
        ),
        "corporate_action_reference_summary_sha256": _sha256(
            corporate_action_path.with_suffix(".summary.json")
        ),
        "benchmark_session_authority": (
            "official 0050 and 2330 daily endpoints plus complete retained TXFR1 "
            "capture manifests; strategy replay receipt does not cap benchmark dates"
        ),
        "strategy_replay_sessions": [
            trading_date.isoformat() for trading_date in replay_session_dates
        ],
        "benchmark_only_sessions": [
            trading_date.isoformat()
            for trading_date in session_dates
            if trading_date not in set(replay_session_dates)
        ],
    }

    for benchmark_id, symbol, label, security_type in STOCK_BENCHMARKS:
        daily_path = (args.stock_parquet_root / f"{symbol}_features.parquet").resolve()
        first_daily = _stock_daily_row(
            daily_path,
            start,
            symbol=symbol,
            twse_daily_ohlcv_path=twse_daily_ohlcv_path,
            tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
        )
        entry_price = _finite((first_daily or {}).get("open"))
        if entry_price is None or entry_price <= 0.0:
            raise ValueError(f"official {symbol} open is unavailable for {start}")
        entry_at = datetime.combine(start, time(9, 0), tzinfo=TAIPEI)
        quantity = 1_000
        buy_rate, sell_rate = TwDayTradeSimulationEngine._stock_benchmark_fee_rates(
            symbol=symbol,
            security_type=security_type,
            fee_schedule=fee_schedule,
        )
        entry_fee, _ = TwDayTradeSimulationEngine._stock_benchmark_order_cost(
            notional=quantity * entry_price,
            commission_rate=buy_rate,
            tax_rate=0.0,
            fee_schedule=fee_schedule,
        )
        live = live_benchmarks[benchmark_id]
        origins[benchmark_id] = {
            "benchmark_id": benchmark_id,
            "session_date": start.isoformat(),
            "entry_at": entry_at.isoformat(timespec="seconds"),
            "entry_price": entry_price,
            "initial_capital_twd": quantity * entry_price,
            "initial_fixed_fees_twd": entry_fee,
            "initial_transaction_tax_twd": 0.0,
            "gross_pnl_multiplier": quantity,
            "return_type": "total_return",
            "total_return_contract": "official_ex_date_reference_reinvestment_v1",
            "source": "official_daily_session_open",
            "live_origin": {
                "entry_at": live.get("entry_at"),
                "entry_price": live.get("entry_price"),
                "initial_fixed_fees_twd": live.get("fixed_fees_twd"),
                "initial_transaction_tax_twd": 0.0,
            },
        }
        provenance["stock_files"][symbol] = {
            "path": str(daily_path),
            "sha256": _sha256(daily_path),
        }
        for trading_date in session_dates:
            daily = _stock_daily_row(
                daily_path,
                trading_date,
                symbol=symbol,
                twse_daily_ohlcv_path=twse_daily_ohlcv_path,
                tpex_daily_ohlcv_path=tpex_daily_ohlcv_path,
            )
            if trading_date == end and current_session_unclosed:
                opening = current_opens.get(symbol)
                if opening is None:
                    raise ValueError(
                        f"retained same-session {symbol} open is unavailable for {trading_date}"
                    )
                action_factor, actions = _required_stock_adjustment(
                    adjustment_engine,
                    symbol=symbol,
                    entry_at=entry_at,
                    mark_date=trading_date,
                    current_session_reference=live,
                )
                marks.append(
                    _stock_mark(
                        benchmark_id=benchmark_id,
                        symbol=symbol,
                        label=label,
                        quantity=quantity,
                        entry_price=entry_price,
                        entry_at=entry_at,
                        entry_fee=entry_fee,
                        mark_price=opening,
                        observed=datetime.combine(
                            trading_date, time(9, 0), tzinfo=TAIPEI
                        ),
                        buy_rate=buy_rate,
                        sell_rate=sell_rate,
                        fee_schedule=fee_schedule,
                        source="retained_same_session_shioaji_snapshot_open",
                        corporate_action_factor=action_factor,
                        corporate_actions=actions,
                    )
                )
                continue
            if daily is None:
                raise ValueError(
                    f"official {symbol} daily row is unavailable for {trading_date}"
                )
            for at, price_key, source in (
                (time(9, 0), "open", "official_daily_session_open"),
                (time(13, 30), "close", "official_daily_session_close"),
            ):
                price = _finite(daily.get(price_key))
                if price is None or price <= 0.0:
                    raise ValueError(
                        f"official {symbol} {price_key} is unavailable for {trading_date}"
                    )
                action_factor, actions = _required_stock_adjustment(
                    adjustment_engine,
                    symbol=symbol,
                    entry_at=entry_at,
                    mark_date=trading_date,
                    current_session_reference=live,
                )
                marks.append(
                    _stock_mark(
                        benchmark_id=benchmark_id,
                        symbol=symbol,
                        label=label,
                        quantity=quantity,
                        entry_price=entry_price,
                        entry_at=entry_at,
                        entry_fee=entry_fee,
                        mark_price=price,
                        observed=datetime.combine(trading_date, at, tzinfo=TAIPEI),
                        buy_rate=buy_rate,
                        sell_rate=sell_rate,
                        fee_schedule=fee_schedule,
                        source=source,
                        corporate_action_factor=action_factor,
                        corporate_actions=actions,
                    )
                )

        live_entry_at = datetime.fromisoformat(str(live["entry_at"])).astimezone(TAIPEI)
        pre_live_factor, pre_live_actions = _required_stock_adjustment(
            adjustment_engine,
            symbol=symbol,
            entry_at=entry_at,
            mark_date=live_entry_at.date(),
            current_session_reference=live,
        )
        origins[benchmark_id]["corporate_action_factor_to_live_entry"] = pre_live_factor
        origins[benchmark_id]["corporate_action_count_to_live_entry"] = len(
            pre_live_actions
        )
        origins[benchmark_id]["corporate_action_coverage_end"] = (
            adjustment_engine._corporate_action_coverage_end.isoformat()
            if adjustment_engine._corporate_action_coverage_end is not None
            else None
        )

    live_tx = live_benchmarks[TX_CONTINUOUS_BENCHMARK_ID]
    final_settlement_path = args.final_settlement_path.resolve()
    if not final_settlement_path.is_file():
        raise FileNotFoundError(final_settlement_path)
    provenance["tx_final_settlement_path"] = str(final_settlement_path)
    provenance["tx_final_settlement_sha256"] = _sha256(final_settlement_path)
    provenance["tx_capture_manifests"] = {}
    provenance["tx_history_receipts"] = {}
    tx_rows = 0
    with tempfile.TemporaryDirectory(prefix="stockagent-tx-benchmark-replay-") as temp:
        tx_engine = TwDayTradeSimulationEngine(
            Path(temp),
            final_settlement_path=final_settlement_path,
        )
        for trading_date in session_dates:
            manifest_root = (
                args.fop_capture_root
                / "manifests"
                / f"trade_date={trading_date.isoformat()}"
            )
            if any(manifest_root.glob("worker=*.json")):
                metadata, manifest_receipts = _tx_front_contract_metadata(
                    capture_root=args.fop_capture_root,
                    trading_date=trading_date,
                )
                provenance["tx_capture_manifests"][trading_date.isoformat()] = {
                    "contract": metadata,
                    "receipts": manifest_receipts,
                }
                end_at = datetime.combine(trading_date, time(13, 45), tzinfo=TAIPEI)
                if trading_date == now.date() and current_session_unclosed:
                    end_at = now
                books = _tx_day_books(
                    capture_root=args.fop_capture_root,
                    trading_date=trading_date,
                    contract_code=str(metadata["code"]),
                    end_at=end_at,
                )
                try:
                    minute_books = _tx_complete_minute_books(
                        books,
                        trading_date=trading_date,
                        timestamp_column="snapshot_ts_ns",
                        epoch_utc=True,
                    )
                    quote_source = "retained_shioaji_fop_book_1s"
                except RuntimeError as exc:
                    # A reboot can leave a truthful partial realtime capture.
                    # Prefer it when it covers the open, otherwise fall back to
                    # the independently receipt-verified historical TXFR1 Tick
                    # partition for the same completed session.
                    provenance["tx_capture_manifests"][trading_date.isoformat()][
                        "minute_coverage_error"
                    ] = str(exc)
                    books, history_receipt = _tx_historical_day_books(
                        history_root=args.tx_history_root.resolve(),
                        trading_date=trading_date,
                    )
                    provenance["tx_history_receipts"][trading_date.isoformat()] = {
                        "contract": metadata,
                        "receipt": history_receipt,
                        "fallback_reason": "realtime_capture_missing_session_open",
                    }
                    minute_books = _tx_complete_minute_books(
                        books,
                        trading_date=trading_date,
                        timestamp_column="event_ts",
                        epoch_utc=False,
                    )
                    quote_source = "receipt_backed_shioaji_txfr1_historical_tick_l1"
            else:
                metadata = _tx_historical_contract_metadata(
                    final_settlement_path=final_settlement_path,
                    trading_date=trading_date,
                )
                books, history_receipt = _tx_historical_day_books(
                    history_root=args.tx_history_root.resolve(),
                    trading_date=trading_date,
                )
                provenance["tx_history_receipts"][trading_date.isoformat()] = {
                    "contract": metadata,
                    "receipt": history_receipt,
                }
                minute_books = _tx_complete_minute_books(
                    books,
                    trading_date=trading_date,
                    timestamp_column="event_ts",
                    epoch_utc=False,
                )
                quote_source = "receipt_backed_shioaji_txfr1_historical_tick_l1"
            for observed, book, fresh in minute_books:
                quote = {
                    "bid": float(book["bid_price_1"]),
                    "ask": float(book["ask_price_1"]),
                    "quote_at": observed.isoformat(timespec="seconds"),
                    "source": quote_source,
                    "delivery_month": metadata["delivery_month"],
                    "last_trading_date": metadata["last_trading_date"],
                }
                tx_engine._mark_tx_continuous_benchmark(
                    current_contract_code=str(metadata["code"]),
                    current_quote=quote,
                    previous_contract_quote={},
                    now=observed,
                )
                tx_mark = _tx_engine_history_mark(tx_engine, observed=observed)
                tx_mark["source"] = quote_source
                tx_mark["valuation_stale"] = not fresh
                tx_mark["fresh_quote_in_minute"] = bool(fresh)
                tx_mark["last_quote_carried"] = not fresh
                if bool(tx_mark.get("valuation_stale")):
                    tx_mark["valuation_source"] = (
                        "last_observed_best_bid_carried_without_interpolation"
                    )
                marks.append(tx_mark)
                tx_rows += 1
        tx_replayed = dict(tx_engine.state["benchmarks"][TX_CONTINUOUS_BENCHMARK_ID])

    entry_at = datetime.fromisoformat(str(tx_replayed["origin_entry_at"])).astimezone(
        TAIPEI
    )
    entry_price = float(tx_replayed["origin_entry_price"])
    live_tx_entry_at = datetime.fromisoformat(str(live_tx["entry_at"])).astimezone(
        TAIPEI
    )
    live_tx_entry_price = float(live_tx["entry_price"])
    tx_multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"]
    live_initial_tax = _tx_tax(live_tx_entry_price, live_tx_entry_at.date())
    canonical_fixed_fees = float(tx_replayed.get("fixed_fees_twd") or 0.0)
    canonical_transaction_tax = float(tx_replayed.get("transaction_tax_twd") or 0.0)
    live_net_pnl_offset = (
        float(tx_replayed.get("realized_gross_pnl_twd") or 0.0)
        + (live_tx_entry_price - float(tx_replayed["current_contract_entry_price"]))
        * tx_multiplier
        - canonical_fixed_fees
        - canonical_transaction_tax
        + TX_FEE_PER_SIDE_TWD
        + live_initial_tax
    )
    origins[TX_CONTINUOUS_BENCHMARK_ID] = {
        "benchmark_id": TX_CONTINUOUS_BENCHMARK_ID,
        "session_date": start.isoformat(),
        "entry_at": entry_at.isoformat(timespec="seconds"),
        "entry_price": entry_price,
        "initial_capital_twd": float(tx_replayed["initial_capital_twd"]),
        "initial_fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
        "initial_transaction_tax_twd": _tx_tax(entry_price, start),
        "gross_pnl_multiplier": tx_multiplier,
        "source": "receipt_backed_shioaji_front_month_best_ask_at_day_open",
        "contract_code": (
            provenance["tx_capture_manifests"][start.isoformat()]["contract"]["code"]
            if start.isoformat() in provenance["tx_capture_manifests"]
            else provenance["tx_history_receipts"][start.isoformat()]["contract"][
                "code"
            ]
        ),
        "roll_contract": (
            "official final settlement after expiry; new front-month ask; "
            "calendar spread never booked as return"
        ),
        "live_net_pnl_offset_twd": live_net_pnl_offset,
        "fixed_fees_twd_to_live_origin": canonical_fixed_fees,
        "transaction_tax_twd_to_live_origin": canonical_transaction_tax,
        "current_contract_entry_price_at_live_origin": float(
            tx_replayed["current_contract_entry_price"]
        ),
        "realized_gross_pnl_twd_to_live_origin": float(
            tx_replayed.get("realized_gross_pnl_twd") or 0.0
        ),
        "live_origin": {
            "entry_at": live_tx.get("entry_at"),
            "entry_price": live_tx.get("entry_price"),
            "initial_fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
            "initial_transaction_tax_twd": live_initial_tax,
        },
    }

    marks.sort(key=lambda row: (str(row["recorded_at"]), str(row["benchmark_id"])))
    output = {
        "schema_version": 1,
        "created_at": now.isoformat(timespec="seconds"),
        "simulation_only": True,
        "production_order_possible": False,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "origin_contract": {
            "stocks": (
                "one board lot entered at retained actual 09:00 open; official "
                "close is used only for completed sessions; official ex-date "
                "reference factors reinvest cash distributions and adjust splits"
            ),
            "tx": (
                "one front-month TX entered at the first valid receipt-backed "
                "08:45 ask; later minute marks use the observed bid or carry the "
                "latest observed bid without interpolation; an expired old month "
                "uses official final settlement before the new month opens at ask"
            ),
            "calendar_spread_return": "never booked as investment return",
            "missing_data": "fail closed; no synthetic price",
            "additional_shioaji_requests": 0,
        },
        "origins": origins,
        "marks": marks,
        "roll_history": list(tx_replayed.get("roll_history") or ()),
        "counts": {
            "marks": len(marks),
            "tx_minute_marks": tx_rows,
            "stock_marks": len(marks) - tx_rows,
        },
        "provenance": provenance,
    }
    expected_tx_rows = len(session_dates) * TX_SESSION_MINUTES
    if tx_rows != expected_tx_rows:
        raise RuntimeError(
            f"TX benchmark minute cardinality mismatch: {tx_rows} != {expected_tx_rows}"
        )
    destination = state_dir / BENCHMARK_HISTORY_FILENAME
    _atomic_json(destination, output)
    print(
        json.dumps(
            {
                "destination": str(destination),
                "sha256": _sha256(destination),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "contract_codes": sorted(
                    {
                        item["contract"]["code"]
                        for item in provenance["tx_capture_manifests"].values()
                    }
                    | {
                        item["contract"]["code"]
                        for item in provenance["tx_history_receipts"].values()
                    }
                ),
                **output["counts"],
                "additional_shioaji_requests": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
