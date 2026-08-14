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


def _stock_daily_row(path: Path, trading_date: date) -> dict[str, Any] | None:
    frame = (
        pl.scan_parquet(path)
        .filter(pl.col("date") == trading_date)
        .select("date", "open", "close")
        .collect()
    )
    return frame.row(0, named=True) if frame.height == 1 else None


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


def _required_stock_adjustment(
    engine: TwDayTradeSimulationEngine,
    *,
    symbol: str,
    entry_at: datetime,
    mark_date: date,
) -> tuple[float, list[dict[str, Any]]]:
    factor, actions, status = engine._stock_total_return_adjustment(
        symbol=symbol,
        entry_at=entry_at,
        mark_date=mark_date,
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
            "/srv/stockagent-live/data_tw_public/"
            "tw_corporate_action_reference.parquet"
        ),
    )
    parser.add_argument(
        "--fop-capture-root",
        type=Path,
        default=Path(
            "data_tw_index_derivatives_ticks/shioaji_fop_captures"
        ),
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    state_dir = args.state_dir.resolve()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("--start-date must not be after --end-date")
    if any(day.weekday() >= 5 for day in _iter_dates(start, end)):
        raise ValueError("benchmark history cannot synthesize weekend sessions")

    state_path = state_dir / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
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
    current_open_path = state_dir / "replay_open_data" / f"{end.isoformat()}.parquet"
    current_opens = _current_open_rows(current_open_path, end)

    origins: dict[str, dict[str, Any]] = {}
    marks: list[dict[str, Any]] = []
    provenance: dict[str, Any] = {
        "state_path": str(state_path),
        "state_sha256": _sha256(state_path),
        "stock_files": {},
        "current_open_path": str(current_open_path),
        "current_open_sha256": _sha256(current_open_path),
        "fop_capture_root": str(args.fop_capture_root.resolve()),
        "corporate_action_reference_path": str(corporate_action_path),
        "corporate_action_reference_sha256": _sha256(corporate_action_path),
        "corporate_action_reference_summary_path": str(
            corporate_action_path.with_suffix(".summary.json")
        ),
        "corporate_action_reference_summary_sha256": _sha256(
            corporate_action_path.with_suffix(".summary.json")
        ),
    }

    for benchmark_id, symbol, label, security_type in STOCK_BENCHMARKS:
        daily_path = (args.stock_parquet_root / f"{symbol}_features.parquet").resolve()
        first_daily = _stock_daily_row(daily_path, start)
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
        for trading_date in _iter_dates(start, end):
            daily = _stock_daily_row(daily_path, trading_date)
            if trading_date == end and trading_date >= now.date():
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

        live_entry_at = datetime.fromisoformat(str(live["entry_at"])).astimezone(
            TAIPEI
        )
        pre_live_factor, pre_live_actions = _required_stock_adjustment(
            adjustment_engine,
            symbol=symbol,
            entry_at=entry_at,
            mark_date=live_entry_at.date(),
        )
        origins[benchmark_id]["corporate_action_factor_to_live_entry"] = (
            pre_live_factor
        )
        origins[benchmark_id]["corporate_action_count_to_live_entry"] = len(
            pre_live_actions
        )
        origins[benchmark_id]["corporate_action_coverage_end"] = (
            adjustment_engine._corporate_action_coverage_end.isoformat()
            if adjustment_engine._corporate_action_coverage_end is not None
            else None
        )

    live_tx = live_benchmarks[TX_CONTINUOUS_BENCHMARK_ID]
    contract_code = str(live_tx.get("contract_code") or "").strip().upper()
    if not contract_code:
        raise ValueError("live TX front-month contract code is unavailable")
    start_close = datetime.combine(start, time(13, 45), tzinfo=TAIPEI)
    start_books = _tx_day_books(
        capture_root=args.fop_capture_root,
        trading_date=start,
        contract_code=contract_code,
        end_at=start_close,
    )
    first_book = start_books.row(0, named=True)
    entry_price = float(first_book["ask_price_1"])
    entry_at = datetime.fromtimestamp(
        int(first_book["snapshot_ts_ns"]) / 1e9, tz=timezone.utc
    ).astimezone(TAIPEI)
    initial_capital = taifex_initial_margin_twd("TX", start)
    initial_tax = _tx_tax(entry_price, start)
    live_tx_entry_at = datetime.fromisoformat(str(live_tx["entry_at"])).astimezone(
        TAIPEI
    )
    live_tx_entry_price = float(live_tx["entry_price"])
    origins[TX_CONTINUOUS_BENCHMARK_ID] = {
        "benchmark_id": TX_CONTINUOUS_BENCHMARK_ID,
        "session_date": start.isoformat(),
        "entry_at": entry_at.isoformat(timespec="seconds"),
        "entry_price": entry_price,
        "initial_capital_twd": initial_capital,
        "initial_fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
        "initial_transaction_tax_twd": initial_tax,
        "gross_pnl_multiplier": TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
        "source": "retained_shioaji_front_month_book_at_day_open",
        "contract_code": contract_code,
        "live_origin": {
            "entry_at": live_tx.get("entry_at"),
            "entry_price": live_tx.get("entry_price"),
            "initial_fixed_fees_twd": TX_FEE_PER_SIDE_TWD,
            "initial_transaction_tax_twd": _tx_tax(
                live_tx_entry_price, live_tx_entry_at.date()
            ),
        },
    }

    live_tx_entry = live_tx_entry_at
    tx_rows = 0
    for trading_date in _iter_dates(start, end):
        end_at = datetime.combine(trading_date, time(13, 45), tzinfo=TAIPEI)
        if trading_date == now.date():
            end_at = min(
                now,
                live_tx_entry.replace(second=0, microsecond=0)
                - timedelta(seconds=1),
            )
        books = _tx_day_books(
            capture_root=args.fop_capture_root,
            trading_date=trading_date,
            contract_code=contract_code,
            end_at=end_at,
        )
        by_minute: dict[str, dict[str, Any]] = {}
        for book in books.iter_rows(named=True):
            observed = datetime.fromtimestamp(
                int(book["snapshot_ts_ns"]) / 1e9, tz=timezone.utc
            ).astimezone(TAIPEI)
            by_minute[_minute(observed)] = book
        for minute_key, book in sorted(by_minute.items()):
            observed = datetime.fromisoformat(minute_key)
            marks.append(
                _tx_mark(
                    entry_price=entry_price,
                    entry_at=entry_at,
                    initial_capital=initial_capital,
                    initial_tax=initial_tax,
                    mark_price=float(book["bid_price_1"]),
                    observed=observed,
                    contract_code=contract_code,
                )
            )
            tx_rows += 1

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
                "one real front-month TX entered at the first valid retained "
                "08:45 ask; later marks use retained bids; forward live rolls "
                "require simultaneous old bid and new ask"
            ),
            "calendar_spread_return": "never booked as investment return",
            "missing_data": "fail closed; no synthetic price",
            "additional_shioaji_requests": 0,
        },
        "origins": origins,
        "marks": marks,
        "roll_history": list(live_tx.get("roll_history") or ()),
        "counts": {
            "marks": len(marks),
            "tx_minute_marks": tx_rows,
            "stock_marks": len(marks) - tx_rows,
        },
        "provenance": provenance,
    }
    destination = state_dir / BENCHMARK_HISTORY_FILENAME
    _atomic_json(destination, output)
    print(
        json.dumps(
            {
                "destination": str(destination),
                "sha256": _sha256(destination),
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "contract_code": contract_code,
                **output["counts"],
                "additional_shioaji_requests": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
