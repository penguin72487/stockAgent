#!/usr/bin/env python3
"""Backtest causal intraday ATM TXO straddles with upward rolling policies.

The recent TAIFEX transaction archive contains whole-second trade prints, not
historical bid/ask books.  Entry and rolling fills therefore use the existing
StockAgent contract: select contracts only from completed observations, then
use the matched-volume-weighted price from the first strictly later second.
The daily terminal mark uses the last observed trade by 13:45 and is reported
explicitly as a mark proxy; a stricter 13:40 decision/next-trade alternative is
calculated separately when both remaining legs trade before the close.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Final, Iterable

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_index_derivatives_tick import (  # noqa: E402
    TAIFEX_TRADE_PROXY_SOURCE,
    TAIPEI,
    _atomic_json,
    _atomic_parquet,
    _select_causal_atm_option_pair,
    taifex_front_month,
    taifex_option_expiry,
)
from scripts.download_taifex_recent_index_derivatives_ticks import _parse_zip  # noqa: E402


STRATEGY_ROLL_OTM: Final[str] = "roll_otm_put_keep_itm_call"
STRATEGY_ROLL_ITM: Final[str] = "roll_itm_call_keep_otm_put"
STRATEGY_CLASSIC: Final[str] = "classic_opening_straddle"
ROLLING_STRATEGIES: Final[tuple[str, str]] = (
    STRATEGY_ROLL_OTM,
    STRATEGY_ROLL_ITM,
)
STRATEGIES: Final[tuple[str, str, str]] = (
    STRATEGY_CLASSIC,
    *ROLLING_STRATEGIES,
)
CLASSIC_NO_ROLL_THRESHOLD: Final[int] = 0
OUTPUT_SCHEMA_VERSION: Final[int] = 2


@dataclass(frozen=True, slots=True)
class OptionContract:
    series: str
    strike: float
    right: str


@dataclass(frozen=True, slots=True)
class Fill:
    event_ns: int
    price: float


@dataclass(frozen=True, slots=True)
class DayMarket:
    trading_date: date
    tx_times_ns: np.ndarray
    tx_prices: np.ndarray
    option_events: dict[OptionContract, tuple[np.ndarray, np.ndarray]]
    pair_availability: tuple[tuple[int, date, str, float], ...]
    futures_events: dict[str, tuple[np.ndarray, np.ndarray]] = field(
        default_factory=dict
    )

    def underlying_at_or_before(self, event_ns: int) -> float | None:
        index = int(np.searchsorted(self.tx_times_ns, event_ns, side="right")) - 1
        if index < 0:
            return None
        return float(self.tx_prices[index])

    def first_option_trade_after(
        self,
        contract: OptionContract,
        event_ns: int,
        *,
        before_ns: int | None = None,
    ) -> Fill | None:
        payload = self.option_events.get(contract)
        if payload is None:
            return None
        times, prices = payload
        index = int(np.searchsorted(times, event_ns, side="right"))
        if index >= len(times):
            return None
        selected_ns = int(times[index])
        if before_ns is not None and selected_ns >= before_ns:
            return None
        return Fill(event_ns=selected_ns, price=float(prices[index]))

    def last_option_trade_at_or_before(
        self,
        contract: OptionContract,
        event_ns: int,
    ) -> Fill | None:
        payload = self.option_events.get(contract)
        if payload is None:
            return None
        times, prices = payload
        index = int(np.searchsorted(times, event_ns, side="right")) - 1
        if index < 0:
            return None
        return Fill(event_ns=int(times[index]), price=float(prices[index]))

    def last_option_trade_before(
        self,
        contract: OptionContract,
        event_ns: int,
    ) -> Fill | None:
        payload = self.option_events.get(contract)
        if payload is None:
            return None
        times, prices = payload
        index = int(np.searchsorted(times, event_ns, side="left")) - 1
        if index < 0:
            return None
        return Fill(event_ns=int(times[index]), price=float(prices[index]))

    def select_option_contract(
        self,
        *,
        decision_ns: int,
        series: str,
        right: str,
        target_strike: float,
    ) -> OptionContract:
        candidates = [
            contract
            for contract, (times, _prices) in self.option_events.items()
            if contract.series == series
            and contract.right == right
            and len(times)
            and int(times[0]) <= decision_ns
        ]
        if not candidates:
            raise ValueError(
                f"{self.trading_date}: no causal {series} {right} contract by "
                f"{_datetime_from_ns(decision_ns)}"
            )
        return min(
            candidates,
            key=lambda contract: (
                abs(contract.strike - float(target_strike)),
                contract.strike,
            ),
        )

    def _future_payload(self, product: str) -> tuple[np.ndarray, np.ndarray]:
        normalized = str(product).strip().upper()
        payload = self.futures_events.get(normalized)
        if payload is not None:
            return payload
        if normalized == "TX":
            return self.tx_times_ns, self.tx_prices
        raise ValueError(
            f"{self.trading_date}: no {normalized} front-month futures events"
        )

    def future_at_or_before(self, product: str, event_ns: int) -> Fill | None:
        times, prices = self._future_payload(product)
        index = int(np.searchsorted(times, event_ns, side="right")) - 1
        if index < 0:
            return None
        return Fill(event_ns=int(times[index]), price=float(prices[index]))

    def first_future_trade_after(
        self,
        product: str,
        event_ns: int,
        *,
        before_ns: int | None = None,
    ) -> Fill | None:
        times, prices = self._future_payload(product)
        index = int(np.searchsorted(times, event_ns, side="right"))
        if index >= len(times):
            return None
        selected_ns = int(times[index])
        if before_ns is not None and selected_ns >= before_ns:
            return None
        return Fill(event_ns=selected_ns, price=float(prices[index]))

    def last_future_trade_before(self, product: str, event_ns: int) -> Fill | None:
        times, prices = self._future_payload(product)
        index = int(np.searchsorted(times, event_ns, side="left")) - 1
        if index < 0:
            return None
        return Fill(event_ns=int(times[index]), price=float(prices[index]))

    def select_atm_pair(
        self,
        *,
        decision_ns: int,
        underlying_price: float,
        required_series: str | None = None,
    ) -> tuple[str, date, float]:
        candidates = [
            row
            for row in self.pair_availability
            if row[0] <= decision_ns
            and (required_series is None or row[2] == required_series)
        ]
        if not candidates:
            raise ValueError(
                f"{self.trading_date}: no causal Call/Put pair by "
                f"{_datetime_from_ns(decision_ns)}"
            )
        nearest_expiry, nearest_series = min((row[1], row[2]) for row in candidates)
        expiry_candidates = [
            row
            for row in candidates
            if row[1] == nearest_expiry and row[2] == nearest_series
        ]
        selected = min(
            expiry_candidates,
            key=lambda row: (abs(float(row[3]) - underlying_price), float(row[3])),
        )
        return selected[2], selected[1], float(selected[3])


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _ns(value: datetime) -> int:
    return int(round(value.timestamp() * 1_000_000_000))


def _datetime_from_ns(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=TAIPEI)


def _parse_time(value: str) -> time:
    return time.fromisoformat(str(value))


def _verify_manifest(raw_root: Path) -> tuple[dict[str, Any], str, list[date]]:
    path = raw_root / "manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError(f"source manifest is not complete: {path}")
    if manifest.get("parser_contract_version") != 1:
        raise ValueError("unsupported TAIFEX transaction parser contract")
    trading_dates = [date.fromisoformat(value) for value in manifest["trading_dates"]]
    if not trading_dates or trading_dates != sorted(set(trading_dates)):
        raise ValueError("source manifest trading_dates are empty, duplicate, or unsorted")
    products = set(manifest.get("products", []))
    if not {"TX", "TXO"} <= products:
        raise ValueError(f"source manifest lacks TX/TXO products: {sorted(products)}")
    expected = {(kind, day.isoformat()) for day in trading_dates for kind in ("TX", "TXO")}
    observed: set[tuple[str, str]] = set()
    for receipt in manifest.get("partitions", []):
        product = str(receipt.get("product"))
        trading_date = str(receipt.get("trading_date"))
        key = (product, trading_date)
        if key not in expected or key in observed:
            raise ValueError(f"unexpected or duplicate partition receipt: {key}")
        output = Path(str(receipt["output_path"]))
        if not output.is_file():
            raise FileNotFoundError(output)
        if output.stat().st_size != int(receipt["output_bytes"]):
            raise ValueError(f"partition byte mismatch: {output}")
        if _sha256_path(output) != str(receipt["output_sha256"]):
            raise ValueError(f"partition SHA-256 mismatch: {output}")
        observed.add(key)
    if observed != expected:
        raise ValueError(f"partition coverage mismatch: {len(observed)}/{len(expected)}")
    return manifest, _sha256_path(path), trading_dates


def _build_day_market(
    raw_root: Path,
    trading_date: date,
    *,
    futures_products: tuple[str, ...] = ("TX",),
) -> DayMarket:
    tx_path = raw_root / "tx" / f"trading_date={trading_date}" / "transactions.parquet"
    txo_path = raw_root / "txo" / f"trading_date={trading_date}" / "transactions.parquet"
    front_month = taifex_front_month(trading_date)
    tx = (
        pl.read_parquet(tx_path)
        .filter(
            (pl.col("session") == "day")
            & (pl.col("delivery_month_week") == front_month)
        )
        .sort(["event_ts", "source_row_number"])
        .group_by("event_ts", maintain_order=True)
        .agg(pl.col("price").last().alias("price"))
        .with_columns(pl.col("event_ts").dt.epoch("ns").alias("event_ns"))
    )
    if tx.is_empty():
        raise ValueError(f"{trading_date}: no day-session front-month TX trades")
    option_rows = pl.read_parquet(txo_path).filter(pl.col("session") == "day")
    if option_rows.is_empty():
        raise ValueError(f"{trading_date}: no day-session TXO trades")
    option_seconds = (
        option_rows.with_columns(
            (pl.col("price") * pl.col("matched_quantity_equivalent")).alias(
                "premium_notional"
            )
        )
        .group_by(
            ["delivery_month_week", "strike_price", "option_right", "event_ts"]
        )
        .agg(
            pl.col("premium_notional").sum(),
            pl.col("matched_quantity_equivalent").sum().alias("quantity"),
        )
        .with_columns(
            (pl.col("premium_notional") / pl.col("quantity")).alias("price"),
            pl.col("event_ts").dt.epoch("ns").alias("event_ns"),
        )
        .sort(["delivery_month_week", "strike_price", "option_right", "event_ns"])
    )
    option_events: dict[OptionContract, tuple[np.ndarray, np.ndarray]] = {}
    for key, frame in option_seconds.partition_by(
        ["delivery_month_week", "strike_price", "option_right"],
        as_dict=True,
        maintain_order=True,
    ).items():
        series, strike, right = key
        contract = OptionContract(str(series), float(strike), str(right))
        option_events[contract] = (
            frame["event_ns"].to_numpy().astype(np.int64, copy=False),
            frame["price"].to_numpy().astype(np.float64, copy=False),
        )
    first_seen: dict[tuple[str, float], dict[str, int]] = {}
    for contract, (times, _prices) in option_events.items():
        first_seen.setdefault((contract.series, contract.strike), {})[
            contract.right
        ] = int(times[0])
    pair_availability: list[tuple[int, date, str, float]] = []
    for (series, strike), rights in first_seen.items():
        if not {"C", "P"} <= rights.keys():
            continue
        try:
            expiry = taifex_option_expiry(series)
        except ValueError:
            continue
        if expiry < trading_date:
            continue
        pair_availability.append(
            (max(rights["C"], rights["P"]), expiry, series, strike)
        )
    if not pair_availability:
        raise ValueError(f"{trading_date}: no unexpired causal TXO Call/Put pairs")
    normalized_futures = tuple(
        dict.fromkeys(str(value).strip().upper() for value in futures_products)
    )
    if not normalized_futures or "TX" not in normalized_futures:
        raise ValueError("futures_products must include TX")
    futures_events: dict[str, tuple[np.ndarray, np.ndarray]] = {
        "TX": (
            tx["event_ns"].to_numpy().astype(np.int64, copy=False),
            tx["price"].to_numpy().astype(np.float64, copy=False),
        )
    }
    extra_products = tuple(value for value in normalized_futures if value != "TX")
    if extra_products:
        receipt_path = tx_path.with_name("transactions.receipt.json")
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        source_path = Path(str(receipt["source_path"]))
        source_sha = str(receipt["source_sha256"])
        if not source_path.is_file() or _sha256_path(source_path) != source_sha:
            raise ValueError(f"{trading_date}: raw futures ZIP receipt mismatch")
        futures = _parse_zip(
            source_path,
            kind="futures",
            trading_date=trading_date,
            source_sha256=source_sha,
            futures_products=normalized_futures,
            futures_outright_contracts_only=True,
        )
        for product in extra_products:
            seconds = (
                futures.filter(
                    (pl.col("session") == "day")
                    & (pl.col("product") == product)
                    & (pl.col("delivery_month_week") == front_month)
                )
                .sort(["event_ts", "source_row_number"])
                .group_by("event_ts", maintain_order=True)
                .agg(pl.col("price").last().alias("price"))
                .with_columns(pl.col("event_ts").dt.epoch("ns").alias("event_ns"))
            )
            if seconds.is_empty():
                raise ValueError(
                    f"{trading_date}: no day-session front-month {product} trades"
                )
            futures_events[product] = (
                seconds["event_ns"].to_numpy().astype(np.int64, copy=False),
                seconds["price"].to_numpy().astype(np.float64, copy=False),
            )
    market = DayMarket(
        trading_date=trading_date,
        tx_times_ns=tx["event_ns"].to_numpy().astype(np.int64, copy=False),
        tx_prices=tx["price"].to_numpy().astype(np.float64, copy=False),
        option_events=option_events,
        pair_availability=tuple(sorted(pair_availability)),
        futures_events=futures_events,
    )
    # Keep the strategy cache semantically tied to the canonical selector.
    open_ns = _ns(datetime.combine(trading_date, time(8, 50), tzinfo=TAIPEI))
    underlying = market.underlying_at_or_before(open_ns)
    if underlying is not None:
        canonical = _select_causal_atm_option_pair(
            option_rows,
            trading_date=trading_date,
            selection_ts=_datetime_from_ns(open_ns),
            underlying_price=underlying,
        )
        cached = market.select_atm_pair(
            decision_ns=open_ns,
            underlying_price=underlying,
        )
        if canonical != cached:
            raise ValueError(
                f"{trading_date}: cached option-chain selection diverged from canonical "
                f"selector: cached={cached}, canonical={canonical}"
            )
    return market


def _trade_row(
    *,
    trading_date: date,
    strategy: str,
    rolling_points: int,
    contract: OptionContract,
    fill: Fill,
    delta_contracts: int,
    reason: str,
    decision_ns: int,
    multiplier: float,
    fee_per_side: float,
    tax_rate: float,
    slippage_points: float,
    terminal_mark_proxy: bool = False,
) -> dict[str, Any]:
    quantity = abs(int(delta_contracts))
    gross_cash_flow = -float(delta_contracts) * fill.price * multiplier
    fixed_fee = quantity * fee_per_side
    transaction_tax = quantity * fill.price * multiplier * tax_rate
    slippage_cost = quantity * slippage_points * multiplier
    return {
        "trading_date": trading_date,
        "strategy": strategy,
        "rolling_points": rolling_points,
        "series": contract.series,
        "strike": contract.strike,
        "option_right": contract.right,
        "decision_ts": _datetime_from_ns(decision_ns),
        "fill_ts": _datetime_from_ns(fill.event_ns),
        "fill_delay_seconds": (fill.event_ns - decision_ns) / 1_000_000_000.0,
        "price_points": fill.price,
        "delta_contracts": int(delta_contracts),
        "reason": reason,
        "terminal_mark_proxy": terminal_mark_proxy,
        "gross_cash_flow_twd": gross_cash_flow,
        "fixed_fee_twd": fixed_fee,
        "transaction_tax_twd": transaction_tax,
        "slippage_cost_twd": slippage_cost,
        "net_cash_flow_twd": (
            gross_cash_flow - fixed_fee - transaction_tax - slippage_cost
        ),
    }


def _summarize_trade_rows(rows: Iterable[dict[str, Any]]) -> dict[str, float]:
    materialized = list(rows)
    gross = sum(float(row["gross_cash_flow_twd"]) for row in materialized)
    fixed_fee = sum(float(row["fixed_fee_twd"]) for row in materialized)
    tax = sum(float(row["transaction_tax_twd"]) for row in materialized)
    slippage = sum(float(row["slippage_cost_twd"]) for row in materialized)
    return {
        "gross_pnl_twd": gross,
        "fixed_fees_twd": fixed_fee,
        "transaction_tax_twd": tax,
        "slippage_cost_twd": slippage,
        "net_after_fee_twd": gross - fixed_fee,
        "net_after_fee_tax_twd": gross - fixed_fee - tax,
        "net_pnl_twd": gross - fixed_fee - tax - slippage,
    }


def _simulate_variant(
    market: DayMarket,
    *,
    strategy: str,
    rolling_points: int,
    selection_time: time,
    close_decision_time: time,
    session_end_time: time,
    multiplier: float,
    fee_per_side: float,
    tax_rate: float,
    slippage_points: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if strategy not in STRATEGIES:
        raise ValueError(f"unsupported strategy: {strategy}")
    is_classic = strategy == STRATEGY_CLASSIC
    if is_classic and rolling_points != CLASSIC_NO_ROLL_THRESHOLD:
        raise ValueError("classic opening straddle must use the no-roll threshold")
    if not is_classic and rolling_points <= 0:
        raise ValueError("rolling strategy points must be positive")
    selection_ns = _ns(
        datetime.combine(market.trading_date, selection_time, tzinfo=TAIPEI)
    )
    close_decision_ns = _ns(
        datetime.combine(market.trading_date, close_decision_time, tzinfo=TAIPEI)
    )
    session_end_ns = _ns(
        datetime.combine(market.trading_date, session_end_time, tzinfo=TAIPEI)
    )
    underlying = market.underlying_at_or_before(selection_ns)
    if underlying is None:
        raise ValueError(f"{market.trading_date}: no TX price by opening selection")
    series, expiry, strike = market.select_atm_pair(
        decision_ns=selection_ns,
        underlying_price=underlying,
    )
    positions = {
        "C": OptionContract(series, strike, "C"),
        "P": OptionContract(series, strike, "P"),
    }
    opening_fills = {
        right: market.first_option_trade_after(
            contract,
            selection_ns,
            before_ns=close_decision_ns,
        )
        for right, contract in positions.items()
    }
    if any(fill is None for fill in opening_fills.values()):
        raise ValueError(f"{market.trading_date}: opening straddle could not fill")
    trades: list[dict[str, Any]] = []
    for right in ("C", "P"):
        fill = opening_fills[right]
        assert fill is not None
        trades.append(
            _trade_row(
                trading_date=market.trading_date,
                strategy=strategy,
                rolling_points=rolling_points,
                contract=positions[right],
                fill=fill,
                delta_contracts=1,
                reason="open_atm_straddle",
                decision_ns=selection_ns,
                multiplier=multiplier,
                fee_per_side=fee_per_side,
                tax_rate=tax_rate,
                slippage_points=slippage_points,
            )
        )
    position_open_ns = {
        right: opening_fills[right].event_ns
        for right in ("C", "P")
        if opening_fills[right] is not None
    }
    available_after = max(fill.event_ns for fill in opening_fills.values() if fill)
    anchor_price = underlying
    rolled_right = (
        None
        if is_classic
        else ("P" if strategy == STRATEGY_ROLL_OTM else "C")
    )
    roll_count = 0
    same_strike_signals = 0
    unfilled_roll_signals = 0
    search_after = available_after
    while rolled_right is not None and roll_count < 100:
        start = int(np.searchsorted(market.tx_times_ns, search_after, side="right"))
        stop = int(
            np.searchsorted(market.tx_times_ns, close_decision_ns, side="left")
        )
        if start >= stop:
            break
        matches = np.flatnonzero(
            market.tx_prices[start:stop] >= anchor_price + float(rolling_points)
        )
        if len(matches) == 0:
            break
        trigger_index = start + int(matches[0])
        decision_ns = int(market.tx_times_ns[trigger_index])
        trigger_price = float(market.tx_prices[trigger_index])
        _selected_series, _selected_expiry, target_strike = market.select_atm_pair(
            decision_ns=decision_ns,
            underlying_price=trigger_price,
            required_series=series,
        )
        current = positions[rolled_right]
        if math.isclose(current.strike, target_strike, rel_tol=0.0, abs_tol=1e-9):
            same_strike_signals += 1
            search_after = decision_ns
            continue
        target = OptionContract(series, target_strike, rolled_right)
        close_fill = market.first_option_trade_after(
            current,
            decision_ns,
            before_ns=close_decision_ns,
        )
        open_fill = market.first_option_trade_after(
            target,
            decision_ns,
            before_ns=close_decision_ns,
        )
        if close_fill is None or open_fill is None:
            unfilled_roll_signals += 1
            break
        trades.append(
            _trade_row(
                trading_date=market.trading_date,
                strategy=strategy,
                rolling_points=rolling_points,
                contract=current,
                fill=close_fill,
                delta_contracts=-1,
                reason=f"roll_close_{rolled_right}",
                decision_ns=decision_ns,
                multiplier=multiplier,
                fee_per_side=fee_per_side,
                tax_rate=tax_rate,
                slippage_points=slippage_points,
            )
        )
        trades.append(
            _trade_row(
                trading_date=market.trading_date,
                strategy=strategy,
                rolling_points=rolling_points,
                contract=target,
                fill=open_fill,
                delta_contracts=1,
                reason=f"roll_open_atm_{rolled_right}",
                decision_ns=decision_ns,
                multiplier=multiplier,
                fee_per_side=fee_per_side,
                tax_rate=tax_rate,
                slippage_points=slippage_points,
            )
        )
        positions[rolled_right] = target
        position_open_ns[rolled_right] = open_fill.event_ns
        roll_count += 1
        anchor_price = trigger_price
        available_after = max(close_fill.event_ns, open_fill.event_ns)
        search_after = available_after
    pre_terminal_trades = list(trades)
    terminal_mark_rows: list[dict[str, Any]] = []
    strict_terminal_rows: list[dict[str, Any]] = []
    terminal_staleness: list[float] = []
    strict_terminal_executable = True
    for right in ("C", "P"):
        contract = positions[right]
        mark = market.last_option_trade_before(contract, session_end_ns)
        if mark is None or mark.event_ns < position_open_ns[right]:
            raise ValueError(
                f"{market.trading_date}: terminal mark unavailable after final roll"
            )
        terminal_staleness.append((session_end_ns - mark.event_ns) / 1_000_000_000.0)
        terminal_mark_rows.append(
            _trade_row(
                trading_date=market.trading_date,
                strategy=strategy,
                rolling_points=rolling_points,
                contract=contract,
                fill=mark,
                delta_contracts=-1,
                reason="daily_terminal_last_trade_mark",
                decision_ns=session_end_ns,
                multiplier=multiplier,
                fee_per_side=fee_per_side,
                tax_rate=tax_rate,
                slippage_points=slippage_points,
                terminal_mark_proxy=True,
            )
        )
        strict_fill = market.first_option_trade_after(
            contract,
            close_decision_ns,
            before_ns=session_end_ns,
        )
        if strict_fill is None:
            strict_terminal_executable = False
        else:
            strict_terminal_rows.append(
                _trade_row(
                    trading_date=market.trading_date,
                    strategy=strategy,
                    rolling_points=rolling_points,
                    contract=contract,
                    fill=strict_fill,
                    delta_contracts=-1,
                    reason="strict_terminal_next_trade",
                    decision_ns=close_decision_ns,
                    multiplier=multiplier,
                    fee_per_side=fee_per_side,
                    tax_rate=tax_rate,
                    slippage_points=slippage_points,
                )
            )
    trades.extend(terminal_mark_rows)
    summary = _summarize_trade_rows(trades)
    strict_summary = (
        _summarize_trade_rows([*pre_terminal_trades, *strict_terminal_rows])
        if strict_terminal_executable and len(strict_terminal_rows) == 2
        else None
    )
    opening_premium = sum(
        float(row["price_points"]) * multiplier
        for row in trades
        if row["reason"] == "open_atm_straddle"
    )
    output: dict[str, Any] = {
        "trading_date": market.trading_date,
        "strategy": strategy,
        "rolling_points": rolling_points,
        "option_series": series,
        "option_expiry": expiry,
        "opening_underlying_price": underlying,
        "opening_strike": strike,
        "opening_abs_moneyness_points": abs(strike - underlying),
        "opening_premium_twd": opening_premium,
        "roll_count": roll_count,
        "same_strike_signals": same_strike_signals,
        "unfilled_roll_signals": unfilled_roll_signals,
        "trade_sides": len(trades),
        "strict_terminal_executable": strict_terminal_executable,
        "terminal_mark_max_staleness_seconds": max(terminal_staleness),
        "return_on_opening_premium": (
            summary["net_pnl_twd"] / opening_premium if opening_premium > 0 else None
        ),
        **summary,
        "strict_terminal_gross_pnl_twd": (
            strict_summary["gross_pnl_twd"] if strict_summary else None
        ),
        "strict_terminal_net_after_fee_twd": (
            strict_summary["net_after_fee_twd"] if strict_summary else None
        ),
        "strict_terminal_net_after_fee_tax_twd": (
            strict_summary["net_after_fee_tax_twd"] if strict_summary else None
        ),
        "strict_terminal_net_pnl_twd": (
            strict_summary["net_pnl_twd"] if strict_summary else None
        ),
    }
    return output, trades


def _aggregate(daily: pl.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for key, frame in daily.partition_by(
        ["strategy", "rolling_points"], as_dict=True, maintain_order=True
    ).items():
        strategy, rolling_points = key
        pnl = frame["net_pnl_twd"].to_numpy().astype(np.float64, copy=False)
        fee_pnl = frame["net_after_fee_twd"].to_numpy().astype(
            np.float64, copy=False
        )
        fee_tax_pnl = frame["net_after_fee_tax_twd"].to_numpy().astype(
            np.float64, copy=False
        )
        gross = frame["gross_pnl_twd"].to_numpy().astype(np.float64, copy=False)
        cumulative = np.cumsum(pnl)
        running_peak = np.maximum.accumulate(np.r_[0.0, cumulative])
        drawdown = np.r_[0.0, cumulative] - running_peak
        fee_cumulative = np.cumsum(fee_pnl)
        fee_running_peak = np.maximum.accumulate(np.r_[0.0, fee_cumulative])
        fee_drawdown = np.r_[0.0, fee_cumulative] - fee_running_peak
        fee_tax_cumulative = np.cumsum(fee_tax_pnl)
        fee_tax_running_peak = np.maximum.accumulate(
            np.r_[0.0, fee_tax_cumulative]
        )
        fee_tax_drawdown = (
            np.r_[0.0, fee_tax_cumulative] - fee_tax_running_peak
        )
        positive = pnl[pnl > 0.0].sum()
        negative = -pnl[pnl < 0.0].sum()
        fee_positive = fee_pnl[fee_pnl > 0.0].sum()
        fee_negative = -fee_pnl[fee_pnl < 0.0].sum()
        strict = frame["strict_terminal_net_pnl_twd"].drop_nulls().to_numpy()
        strict_fee = (
            frame["strict_terminal_net_after_fee_twd"].drop_nulls().to_numpy()
        )
        strict_fee_tax = (
            frame["strict_terminal_net_after_fee_tax_twd"].drop_nulls().to_numpy()
        )
        results.append(
            {
                "strategy": str(strategy),
                "rolling_points": int(rolling_points),
                "days": frame.height,
                "total_rolls": int(frame["roll_count"].sum()),
                "total_trade_sides": int(frame["trade_sides"].sum()),
                "gross_pnl_twd": float(gross.sum()),
                "fixed_fees_twd": float(frame["fixed_fees_twd"].sum()),
                "transaction_tax_twd": float(frame["transaction_tax_twd"].sum()),
                "slippage_cost_twd": float(frame["slippage_cost_twd"].sum()),
                "net_after_fee_twd": float(frame["net_after_fee_twd"].sum()),
                "net_after_fee_tax_twd": float(
                    frame["net_after_fee_tax_twd"].sum()
                ),
                "net_pnl_twd": float(pnl.sum()),
                "average_daily_after_fee_twd": float(fee_pnl.mean()),
                "median_daily_after_fee_twd": float(np.median(fee_pnl)),
                "win_rate_after_fee": float(np.mean(fee_pnl > 0.0)),
                "profit_factor_after_fee": (
                    float(fee_positive / fee_negative)
                    if fee_negative > 0.0
                    else None
                ),
                "maximum_drawdown_after_fee_twd": float(fee_drawdown.min()),
                "best_day_after_fee_twd": float(fee_pnl.max()),
                "worst_day_after_fee_twd": float(fee_pnl.min()),
                "median_daily_after_fee_tax_twd": float(np.median(fee_tax_pnl)),
                "win_rate_after_fee_tax": float(np.mean(fee_tax_pnl > 0.0)),
                "maximum_drawdown_after_fee_tax_twd": float(
                    fee_tax_drawdown.min()
                ),
                "average_daily_pnl_twd": float(pnl.mean()),
                "median_daily_pnl_twd": float(np.median(pnl)),
                "win_rate": float(np.mean(pnl > 0.0)),
                "profit_factor": (
                    float(positive / negative) if negative > 0.0 else None
                ),
                "maximum_drawdown_twd": float(drawdown.min()),
                "best_day_twd": float(pnl.max()),
                "worst_day_twd": float(pnl.min()),
                "mean_return_on_opening_premium": float(
                    frame["return_on_opening_premium"].mean()
                ),
                "strict_terminal_days": int(len(strict)),
                "strict_terminal_coverage": float(len(strict) / frame.height),
                "strict_terminal_net_pnl_twd": (
                    float(strict.sum()) if len(strict) else None
                ),
                "strict_terminal_net_after_fee_twd": (
                    float(strict_fee.sum()) if len(strict_fee) else None
                ),
                "strict_terminal_average_daily_after_fee_twd": (
                    float(strict_fee.mean()) if len(strict_fee) else None
                ),
                "strict_terminal_win_rate_after_fee": (
                    float(np.mean(strict_fee > 0.0)) if len(strict_fee) else None
                ),
                "strict_terminal_net_after_fee_tax_twd": (
                    float(strict_fee_tax.sum()) if len(strict_fee_tax) else None
                ),
                "strict_terminal_average_daily_pnl_twd": (
                    float(strict.mean()) if len(strict) else None
                ),
                "strict_terminal_win_rate": (
                    float(np.mean(strict > 0.0)) if len(strict) else None
                ),
                "terminal_mark_staleness_median_seconds": float(
                    frame["terminal_mark_max_staleness_seconds"].median()
                ),
                "terminal_mark_staleness_max_seconds": float(
                    frame["terminal_mark_max_staleness_seconds"].max()
                ),
            }
        )
    return sorted(results, key=lambda row: (row["strategy"], row["rolling_points"]))


def _aggregate_common_strict(daily: pl.DataFrame) -> tuple[list[str], list[dict[str, Any]]]:
    variant_count = daily.select(["strategy", "rolling_points"]).unique().height
    complete_dates = (
        daily.group_by("trading_date")
        .agg(pl.col("strict_terminal_executable").sum().alias("variants"))
        .filter(pl.col("variants") == variant_count)["trading_date"]
        .sort()
        .to_list()
    )
    if not complete_dates:
        return [], []
    common = daily.filter(pl.col("trading_date").is_in(complete_dates))
    results: list[dict[str, Any]] = []
    for key, frame in common.partition_by(
        ["strategy", "rolling_points"], as_dict=True, maintain_order=True
    ).items():
        strategy, threshold = key
        fee_pnl = frame["strict_terminal_net_after_fee_twd"].to_numpy().astype(
            np.float64
        )
        fee_tax_pnl = frame[
            "strict_terminal_net_after_fee_tax_twd"
        ].to_numpy().astype(np.float64)
        pnl = frame["strict_terminal_net_pnl_twd"].to_numpy().astype(np.float64)
        cumulative = np.cumsum(fee_pnl)
        peak = np.maximum.accumulate(np.r_[0.0, cumulative])
        drawdown = np.r_[0.0, cumulative] - peak
        results.append(
            {
                "strategy": str(strategy),
                "rolling_points": int(threshold),
                "days": len(fee_pnl),
                "net_after_fee_twd": float(fee_pnl.sum()),
                "net_after_fee_tax_twd": float(fee_tax_pnl.sum()),
                "net_pnl_twd": float(pnl.sum()),
                "average_daily_after_fee_twd": float(fee_pnl.mean()),
                "median_daily_after_fee_twd": float(np.median(fee_pnl)),
                "win_rate_after_fee": float(np.mean(fee_pnl > 0.0)),
                "maximum_drawdown_twd": float(drawdown.min()),
                "best_day_after_fee_twd": float(fee_pnl.max()),
                "worst_day_after_fee_twd": float(fee_pnl.min()),
            }
        )
    return (
        [value.isoformat() for value in complete_dates],
        sorted(results, key=lambda row: (row["strategy"], row["rolling_points"])),
    )


def _execution_delay_summary(trades: pl.DataFrame) -> dict[str, Any]:
    causal = trades.filter(~pl.col("terminal_mark_proxy"))
    delay = causal["fill_delay_seconds"]
    return {
        "causal_trade_sides": causal.height,
        "median_seconds": float(delay.median()),
        "p95_seconds": float(delay.quantile(0.95, interpolation="nearest")),
        "maximum_seconds": float(delay.max()),
        "over_60_seconds": int((delay > 60.0).sum()),
        "over_300_seconds": int((delay > 300.0).sum()),
    }


def run_backtest(
    *,
    raw_root: Path,
    output_dir: Path,
    rolling_points: Iterable[int],
    selection_time: time,
    close_decision_time: time,
    session_end_time: time,
    multiplier: float,
    fee_per_side: float,
    tax_rate: float,
    slippage_points: float,
) -> dict[str, Any]:
    source_manifest, source_sha, trading_dates = _verify_manifest(raw_root)
    thresholds = tuple(sorted(set(int(value) for value in rolling_points)))
    if not thresholds or any(value <= 0 for value in thresholds):
        raise ValueError("rolling_points must contain positive integers")
    variants = (
        (STRATEGY_CLASSIC, CLASSIC_NO_ROLL_THRESHOLD),
        *(
            (strategy, threshold)
            for strategy in ROLLING_STRATEGIES
            for threshold in thresholds
        ),
    )
    daily_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for index, trading_date in enumerate(trading_dates, start=1):
        print(
            f"[straddle-rolling] date={trading_date} progress={index}/{len(trading_dates)}",
            flush=True,
        )
        market = _build_day_market(raw_root, trading_date)
        for strategy, threshold in variants:
            try:
                daily, trades = _simulate_variant(
                    market,
                    strategy=strategy,
                    rolling_points=threshold,
                    selection_time=selection_time,
                    close_decision_time=close_decision_time,
                    session_end_time=session_end_time,
                    multiplier=multiplier,
                    fee_per_side=fee_per_side,
                    tax_rate=tax_rate,
                    slippage_points=slippage_points,
                )
            except Exception as exc:
                failures.append(
                    {
                        "trading_date": trading_date.isoformat(),
                        "strategy": strategy,
                        "rolling_points": str(threshold),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                continue
            daily_rows.append(daily)
            trade_rows.extend(trades)
    if failures:
        raise RuntimeError(
            f"{len(failures)} strategy-days failed; first failures={failures[:5]}"
        )
    daily = pl.DataFrame(daily_rows).sort(
        ["strategy", "rolling_points", "trading_date"]
    )
    trades = pl.DataFrame(trade_rows).sort(
        ["strategy", "rolling_points", "trading_date", "fill_ts", "option_right"]
    )
    expected_rows = len(trading_dates) * len(variants)
    if daily.height != expected_rows:
        raise ValueError(f"daily result coverage mismatch: {daily.height}/{expected_rows}")
    noncausal = trades.filter(
        (~pl.col("terminal_mark_proxy"))
        & (pl.col("fill_ts") <= pl.col("decision_ts"))
    )
    if noncausal.height:
        raise ValueError(f"found {noncausal.height} non-causal entry/rolling trades")
    open_positions = (
        trades.group_by(
            [
                "trading_date",
                "strategy",
                "rolling_points",
                "series",
                "strike",
                "option_right",
            ]
        )
        .agg(pl.col("delta_contracts").sum().alias("contracts"))
        .filter(pl.col("contracts") != 0)
    )
    if open_positions.height:
        raise ValueError(f"terminal positions are not flat: {open_positions.head(5)}")
    expected_fees = daily["trade_sides"].cast(pl.Float64) * float(fee_per_side)
    if not np.allclose(expected_fees.to_numpy(), daily["fixed_fees_twd"].to_numpy()):
        raise ValueError("fixed fees do not reconcile to trade sides")
    results = _aggregate(daily)
    common_strict_dates, common_strict_results = _aggregate_common_strict(daily)
    delay_summary = _execution_delay_summary(trades)
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = output_dir / "daily_results.parquet"
    trades_path = output_dir / "trades.parquet"
    _atomic_parquet(daily, daily_path)
    _atomic_parquet(trades, trades_path)
    summary: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA_VERSION,
        "status": "complete",
        "generated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "source": TAIFEX_TRADE_PROXY_SOURCE,
        "source_manifest": str(raw_root / "manifest.json"),
        "source_manifest_sha256": source_sha,
        "source_parser_contract_version": source_manifest["parser_contract_version"],
        "date_start": trading_dates[0].isoformat(),
        "date_end": trading_dates[-1].isoformat(),
        "trading_days": len(trading_dates),
        "daily_result_rows": daily.height,
        "trade_rows": trades.height,
        "parameters": {
            "strategies": list(STRATEGIES),
            "rolling_points": list(thresholds),
            "classic_baseline": {
                "strategy": STRATEGY_CLASSIC,
                "rolling_points": CLASSIC_NO_ROLL_THRESHOLD,
                "description": "opening ATM straddle held without intraday rolling",
            },
            "selection_time": selection_time.isoformat(),
            "close_decision_time": close_decision_time.isoformat(),
            "session_end_time": session_end_time.isoformat(),
            "contract_multiplier_twd_per_point": multiplier,
            "fixed_fee_per_contract_per_side_twd": fee_per_side,
            "transaction_tax_rate": tax_rate,
            "slippage_points_per_side": slippage_points,
            "contracts_per_leg": 1,
            "rolling_direction": "upward_only_from_last_successful_roll_underlying",
        },
        "evaluation_stages": {
            "primary": {
                "name": "fixed_fee_only",
                "metric": "net_after_fee_twd",
                "fixed_fee_per_contract_per_side_twd": fee_per_side,
            },
            "next_if_primary_profitable": {
                "name": "fixed_fee_plus_statutory_transaction_tax",
                "metric": "net_after_fee_tax_twd",
                "transaction_tax_rate": tax_rate,
            },
            "artificial_slippage": {
                "enabled": bool(slippage_points > 0.0),
                "points_per_side": slippage_points,
            },
            "historical_bidask": {
                "status": "unavailable_in_taifex_transaction_archive",
            },
        },
        "execution_contract": {
            "entry_and_roll": (
                "completed-second causal selection; matched-volume-weighted first "
                "strictly later whole-second TAIFEX trade print"
            ),
            "liquidity_assumption": (
                "one contract per leg is guaranteed to fill; historical trade "
                "quantity and displayed book depth do not cap execution"
            ),
            "matched_quantity_usage": (
                "used only to aggregate multiple prints into one representative "
                "whole-second price; never used as a fill-capacity constraint"
            ),
            "terminal_primary": (
                "last whole-second trade print before 13:45 mark proxy; not a "
                "historically executable bid"
            ),
            "terminal_strict": (
                "13:40 completed-second decision and first strictly later trade "
                "before 13:45, available only when both held legs trade"
            ),
            "historical_bidask_available": False,
            "daily_flattened_for_pnl": True,
        },
        "validation": {
            "assessment": "needs_bidask_capture_for_profitability_decision",
            "primary_30_day_results_are_executable": False,
            "reason": (
                "historical books are unavailable; primary terminal values are last-"
                "trade marks and causal trade-print fills can be very delayed"
            ),
            "execution_delay": delay_summary,
            "common_strict_terminal_dates": common_strict_dates,
            "common_strict_terminal_days": len(common_strict_dates),
            "common_strict_terminal_results": common_strict_results,
        },
        "artifacts": {
            "daily_results": str(daily_path),
            "daily_results_sha256": _sha256_path(daily_path),
            "trades": str(trades_path),
            "trades_sha256": _sha256_path(trades_path),
        },
        "results": results,
    }
    _atomic_json(output_dir / "summary.json", summary)
    print(json.dumps(results, ensure_ascii=False, indent=2), flush=True)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=Path("data_tw_index_derivatives_ticks"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/research/taifex_atm_straddle_rolling"),
    )
    parser.add_argument(
        "--rolling-points",
        nargs="+",
        type=int,
        default=list(range(50, 1001, 50)),
    )
    parser.add_argument("--selection-time", default="08:50:00")
    parser.add_argument("--close-decision-time", default="13:40:00")
    parser.add_argument("--session-end-time", default="13:45:00")
    parser.add_argument("--contract-multiplier", type=float, default=50.0)
    parser.add_argument("--fee-per-side", type=float, default=22.0)
    parser.add_argument("--transaction-tax-rate", type=float, default=0.001)
    parser.add_argument("--slippage-points-per-side", type=float, default=0.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    for name, value in (
        ("contract_multiplier", args.contract_multiplier),
        ("fee_per_side", args.fee_per_side),
        ("transaction_tax_rate", args.transaction_tax_rate),
        ("slippage_points_per_side", args.slippage_points_per_side),
    ):
        if not math.isfinite(float(value)) or float(value) < 0.0:
            raise ValueError(f"{name} must be finite and non-negative")
    if args.contract_multiplier <= 0.0:
        raise ValueError("contract_multiplier must be positive")
    run_backtest(
        raw_root=args.raw_root,
        output_dir=args.output_dir,
        rolling_points=args.rolling_points,
        selection_time=_parse_time(args.selection_time),
        close_decision_time=_parse_time(args.close_decision_time),
        session_end_time=_parse_time(args.session_end_time),
        multiplier=float(args.contract_multiplier),
        fee_per_side=float(args.fee_per_side),
        tax_rate=float(args.transaction_tax_rate),
        slippage_points=float(args.slippage_points_per_side),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
