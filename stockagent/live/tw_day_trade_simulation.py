"""Durable multi-mode Taiwan stock day-trade paper execution.

The signal producer and this executor deliberately remain separate.  A signal
artifact is an immutable model decision; this module observes a causally later
TWSE/TPEx best quote, converts target weights to board lots, and owns the only
paper order/fill/position ledger used by the dashboard.

This module never calls a broker order API.  ``simulation_only`` and
``production_order_possible`` are persisted in every status snapshot so a
paper fill cannot be mistaken for a real exchange fill.
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
import json
import math
import os
from pathlib import Path
from typing import Any, Final, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np

from stockagent.backtest.tw_execution import (
    TaiwanFeeSchedule,
    commission_rebate_rate_vector,
    effective_fee_rate_vectors,
    gross_fee_rate_vectors,
    preserve_directional_lot_mix,
)
from stockagent.backtest.tw_integer_execution import (
    _commission_fees_by_symbol,
    _tax_fees_by_symbol,
)
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD,
    TAIFEX_INDEX_FUTURES_MULTIPLIERS,
)
from stockagent.data.tw_price_rules import limit_price_numpy, move_price_ticks_numpy
from stockagent.live.quote_provider import PriceSnapshot
from stockagent.research.taifex_capital_returns import taifex_initial_margin_twd
from stockagent.research.taifex_transaction_tax import (
    stock_index_futures_tax_rate,
    taifex_tax_per_contract_twd,
)


TAIPEI: Final[ZoneInfo] = ZoneInfo("Asia/Taipei")
SIMULATION_SCHEMA_VERSION: Final[int] = 3
TX_CONTINUOUS_ROLL_CONTRACT_VERSION: Final[int] = 2
ENTRY_GATE: Final[time] = time(9, 0)
FIRST_MINUTE_EXECUTION_TIME: Final[time] = time(9, 1)
EXIT_LIMIT_TIME: Final[time] = time(13, 20)
FORCE_EXIT_TIME: Final[time] = time(13, 24)
CLOSING_AUCTION_TIME: Final[time] = time(13, 25)
SESSION_CLOSE: Final[time] = time(13, 30)
MINUTE_VOLUME_PARTICIPATION: Final[float] = 0.50
# Conservative paper assumptions.  The 7% is the TWSE cap for an unpaid
# day-trade securities shortfall and the extra 10% is the cap on the borrower
# handling charge as a fraction of that borrowing fee.  Margin financing has
# no exchange-wide tariff, so 16% is deliberately a stress assumption rather
# than a claim about a broker's current customer rate.
DAY_TRADE_SHORTFALL_BORROW_FEE_RATE: Final[float] = 0.07
DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION: Final[float] = 0.10
MARGIN_FINANCING_ANNUAL_RATE: Final[float] = 0.16
MARGIN_FINANCING_RATIO: Final[float] = 0.60
MARGIN_SHORT_INITIAL_MARGIN_RATE: Final[float] = 0.90
DEFAULT_LIVE_RULE_DATA_DIR: Final[Path] = Path("/srv/stockagent-live/data_tw_public")
STOCK_BENCHMARKS: Final[tuple[tuple[str, str, str, str], ...]] = (
    ("benchmark_0050", "0050", "0050 元大台灣50", "etf"),
    ("benchmark_2330", "2330", "2330 台積電", "stock"),
)
TX_CONTINUOUS_BENCHMARK_ID: Final[str] = "benchmark_tx_continuous"
TX_CONTINUOUS_LOGICAL_CODE: Final[str] = "TXFR1"
DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "data_tw_index_options_daily/txo_final_settlement_history.parquet"
)


def _now_taipei(now: datetime | None = None) -> datetime:
    observed = now or datetime.now(TAIPEI)
    if observed.tzinfo is None:
        raise ValueError("simulation timestamps must be timezone-aware")
    return observed.astimezone(TAIPEI)


def _iso(now: datetime | None = None) -> str:
    return _now_taipei(now).isoformat(timespec="seconds")


def _parse_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TAIPEI)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=TAIPEI)
    return parsed.astimezone(TAIPEI)


def _timestamp_is_for_session(value: object, session_date: str) -> bool:
    """Return whether a lifecycle marker belongs to the active session.

    Persisted modes span trading days.  A truthy timestamp from yesterday must
    never suppress today's closing-auction submission or terminal settlement.
    """

    parsed = _parse_timestamp(value)
    return parsed is not None and parsed.date().isoformat() == str(session_date)


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def _date_value(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _top_book_capacity_shares(
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
) -> int:
    """Return executable shares visible at the relevant best quote.

    TWSE MIS ``f``/``g`` best-five quantities are board-lot counts.  This
    simulation intentionally consumes only level one: deeper prices and queue
    position are unknown, so the remainder must stay unfilled instead of being
    fabricated at the top price.
    """

    if transaction_side not in {"buy", "sell"}:
        raise ValueError("transaction_side must be 'buy' or 'sell'")
    displayed_lots = _finite(
        quote.get("ask_volume" if transaction_side == "buy" else "bid_volume")
    )
    if displayed_lots is None:
        return 0
    return int(math.floor(displayed_lots)) * int(lot_size)


def _minute_kbar_capacity_shares(
    quote: Mapping[str, Any],
    *,
    lot_size: int,
    participation: float = MINUTE_VOLUME_PARTICIPATION,
) -> int:
    """Return whole-lot capacity from one completed regular-session minute.

    Shioaji regular-board snapshot volume is denominated in board lots.  A
    missing or non-isolated minute is not evidence of liquidity and therefore
    has zero capacity.  Flooring before conversion to shares prevents a half
    lot from leaking into this board-lot-only simulator.
    """

    minute_lots = _finite(quote.get("minute_volume_lots"))
    if minute_lots is None:
        return 0
    return int(math.floor(minute_lots * float(participation))) * int(lot_size)


def _executable_capacity_shares(
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
) -> int:
    """Require both displayed level-one depth and 50% minute-K liquidity."""

    return min(
        _top_book_capacity_shares(
            quote,
            transaction_side=transaction_side,
            lot_size=lot_size,
        ),
        _minute_kbar_capacity_shares(quote, lot_size=lot_size),
    )


def _force_exit_retry_capacity_shares(
    position: dict[str, Any],
    quote: Mapping[str, Any],
    *,
    transaction_side: str,
    lot_size: int,
    now: datetime,
) -> int:
    """Return remaining same-minute capacity for repeated market exits.

    A fresh best quote may be retried every service poll from 13:24 to 13:25,
    but the completed-minute participation budget may only be consumed once.
    This prevents a two-second retry loop from multiplying the same one-minute
    volume evidence into fabricated liquidity.
    """

    minute_key = now.replace(second=0, microsecond=0).isoformat(timespec="minutes")
    observed_capacity = _minute_kbar_capacity_shares(quote, lot_size=lot_size)
    if position.get("force_exit_liquidity_minute") != minute_key:
        position["force_exit_liquidity_minute"] = minute_key
        position["force_exit_minute_capacity_shares"] = observed_capacity
        position["force_exit_minute_consumed_shares"] = 0
    else:
        position["force_exit_minute_capacity_shares"] = max(
            int(position.get("force_exit_minute_capacity_shares") or 0),
            observed_capacity,
        )
    remaining_volume_capacity = max(
        0,
        int(position.get("force_exit_minute_capacity_shares") or 0)
        - int(position.get("force_exit_minute_consumed_shares") or 0),
    )
    return min(
        _top_book_capacity_shares(
            quote,
            transaction_side=transaction_side,
            lot_size=lot_size,
        ),
        remaining_volume_capacity,
    )


def _prepare_entry_plan(
    raw_row: Mapping[str, Any],
    *,
    quote: Mapping[str, Any],
    evidence: LiveEligibility | None,
    signal_at: datetime,
    observation_at: datetime,
    spec: ModeSpec,
    allow_quote_at_signal: bool = False,
) -> dict[str, Any]:
    """Resolve one admissible entry before portfolio-level direction balancing."""

    row = dict(raw_row)
    symbol = str(row.get("symbol") or "")
    target_weight = float(row.get("target_weight") or 0.0)
    side = "long" if target_weight > 0.0 else "short" if target_weight < 0.0 else "flat"
    quote_values = dict(quote)
    status = "ready"
    reason: str | None = None
    sizing_price = _finite(quote_values.get("open")) or _finite(row.get("open_price"))
    entry_price = _finite(quote_values.get("ask" if side == "long" else "bid"))
    quote_at = _parse_timestamp(quote_values.get("quote_at"))
    upper = _finite(quote_values.get("upper_limit"))
    lower = _finite(quote_values.get("lower_limit"))
    if side == "flat":
        status, reason = "hold", "zero_target_weight"
    elif not bool(row.get("tradable")):
        status, reason = "blocked", "model_tradable_mask_false"
    elif side == "long" and not bool(row.get("can_buy")):
        status, reason = "blocked", "cannot_buy_open"
    elif side == "short" and not bool(row.get("can_sell")):
        status, reason = "blocked", "cannot_sell_open"
    elif evidence is None or not evidence.covered:
        status, reason = "blocked", "exact_session_eligibility_missing"
    elif not evidence.eligible:
        status, reason = "blocked", "not_day_trade_eligible"
    elif side == "short" and not evidence.short_open:
        status, reason = "blocked", "sell_first_suspended"
    elif sizing_price is None:
        status, reason = "blocked", "official_open_price_unavailable"

    requested_shares = 0
    filled_shares = 0
    top_book_capacity_shares = 0
    minute_kbar_capacity_shares = 0
    if status == "ready" and sizing_price is not None:
        requested_shares = int(
            math.floor(
                abs(target_weight)
                * float(spec.initial_capital_twd)
                / sizing_price
                / int(spec.lot_size)
            )
        ) * int(spec.lot_size)
        if requested_shares <= 0:
            status, reason = "skipped", "below_one_board_lot"
    if status == "ready":
        if entry_price is None:
            status, reason = "blocked", "no_executable_best_quote"
        elif quote_at is None or (
            quote_at < signal_at
            if allow_quote_at_signal
            else quote_at <= signal_at
        ):
            status, reason = "blocked", "quote_not_after_signal"
        elif quote_at > observation_at:
            status, reason = "blocked", "quote_after_local_observation"
        elif upper is None or lower is None:
            status, reason = "blocked", "price_limit_unavailable"
        else:
            top_book_capacity_shares = _top_book_capacity_shares(
                quote_values,
                transaction_side="buy" if side == "long" else "sell",
                lot_size=spec.lot_size,
            )
            minute_kbar_capacity_shares = _minute_kbar_capacity_shares(
                quote_values,
                lot_size=spec.lot_size,
            )
            quote_wall_time = (
                quote_at.astimezone(TAIPEI).timetz().replace(tzinfo=None)
                if quote_at is not None
                else None
            )
            # At 09:00 no completed one-minute K bar exists yet. Requiring one
            # delayed every entry until 09:01 even when a causally newer,
            # executable best quote was already available. Opening orders use
            # only fresh displayed level-one depth; later orders retain the
            # conservative completed-minute participation cap.
            if quote_wall_time is not None and quote_wall_time < time(9, 1):
                filled_shares = min(requested_shares, top_book_capacity_shares)
            else:
                filled_shares = min(
                    requested_shares,
                    top_book_capacity_shares,
                    minute_kbar_capacity_shares,
                )
            if filled_shares <= 0:
                status, reason = "blocked", "marketable_depth_unavailable"
            elif filled_shares < requested_shares:
                status, reason = "partial_depth", "marketable_depth_exhausted"
    return {
        "row": row,
        "symbol": symbol,
        "target_weight": target_weight,
        "side": side,
        "quote": quote_values,
        "evidence": evidence,
        "status": status,
        "reason": reason,
        "entry_price": entry_price,
        "sizing_price": sizing_price,
        "upper": upper,
        "lower": lower,
        "requested_shares": requested_shares,
        "filled_shares": filled_shares,
        "pre_balance_filled_shares": filled_shares,
        "top_book_capacity_shares": top_book_capacity_shares,
        "minute_kbar_capacity_shares": minute_kbar_capacity_shares,
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    _append_jsonl_many(path, (payload,))


def _append_jsonl_many(
    path: Path,
    payloads: Sequence[Mapping[str, Any]],
) -> None:
    """Durably append one logical ledger batch with a single fsync.

    A full-universe signal contains thousands of audit rows. Syncing every
    row separately made ledger persistence several seconds slower without
    improving the durability boundary of the logical signal transaction.
    Encode the complete batch first, then append and fsync it once.
    """

    if not payloads:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = "".join(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        for payload in payloads
    ).encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError(f"short append to {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ModeSpec:
    market: str
    label: str
    initial_capital_twd: float
    config_path: str
    checkpoint_path: str | None
    parquet_root: Path
    live_output_dir: Path
    fee_schedule: TaiwanFeeSchedule
    lot_size: int = 1_000
    signal_market: str | None = None
    price_limit_offset_ticks: int = 0

    def __post_init__(self) -> None:
        if (
            isinstance(self.price_limit_offset_ticks, bool)
            or int(self.price_limit_offset_ticks) != self.price_limit_offset_ticks
            or int(self.price_limit_offset_ticks) < 0
        ):
            raise ValueError("price_limit_offset_ticks must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class LiveEligibility:
    symbol: str
    venue: str | None
    security_type: str | None
    eligible: bool
    short_open: bool
    covered: bool
    source_date: str | None
    reason: str | None = None


def resolve_day_trade_rule_data_dir(
    configured: str | Path | None,
    *,
    parquet_root: Path,
    repo_root: Path,
) -> Path:
    """Resolve the mutable same-session rule source shared by live consumers."""

    raw = configured or os.getenv("STOCKAGENT_TW_DAY_TRADE_RULE_DATA_DIR")
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else Path(repo_root) / path
    if DEFAULT_LIVE_RULE_DATA_DIR.is_dir():
        return DEFAULT_LIVE_RULE_DATA_DIR
    return Path(parquet_root).parent


def load_symbol_metadata(parquet_root: Path) -> dict[str, dict[str, str]]:
    path = Path(parquet_root) / "symbols.csv"
    if not path.is_file():
        return {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        return {
            str(row.get("code") or "").strip(): {
                "venue": str(row.get("market") or "").strip().casefold(),
                "security_type": str(row.get("security_type") or "").strip().casefold(),
                "name": str(row.get("name") or "").strip(),
            }
            for row in csv.DictReader(handle)
            if str(row.get("code") or "").strip()
        }


def load_live_eligibility(
    *,
    rule_data_dir: Path,
    parquet_root: Path,
    symbols: Sequence[str],
    trading_date: date,
) -> tuple[dict[str, LiveEligibility], dict[str, Any]]:
    """Resolve exact-session membership; a missing venue/date fails closed."""

    import polars as pl

    metadata = load_symbol_metadata(parquet_root)
    target_date = trading_date.isoformat()
    members: dict[str, dict[str, bool]] = {}
    coverage: dict[str, dict[str, Any]] = {}
    for venue, dataset in (
        ("twse", "twse_day_trade_eligibility"),
        ("tpex", "tpex_day_trade_eligibility"),
    ):
        path = Path(rule_data_dir) / f"{dataset}.parquet"
        venue_members: dict[str, bool] = {}
        latest_date: str | None = None
        error: str | None = None
        if path.is_file():
            try:
                lazy = pl.scan_parquet(path)
                latest_date = lazy.select(pl.col("date").max()).collect().item()
                rows = (
                    lazy.filter(pl.col("date") == target_date)
                    .select(
                        pl.col("證券代號")
                        .cast(pl.String)
                        .str.strip_chars()
                        .alias("symbol"),
                        pl.col("暫停現股賣出後現款買進當沖註記")
                        .cast(pl.String)
                        .fill_null("")
                        .str.strip_chars()
                        .alias("suspension"),
                    )
                    .collect()
                )
                venue_members = {
                    str(row["symbol"]): str(row["suspension"] or "") == ""
                    for row in rows.iter_rows(named=True)
                    if str(row["symbol"] or "")
                }
            except Exception as exc:  # fail closed and surface provenance
                error = f"{type(exc).__name__}: {exc}"
        else:
            error = f"missing {path}"
        members[venue] = venue_members
        coverage[venue] = {
            "dataset": dataset,
            "path": str(path),
            "target_date": target_date,
            "latest_date": latest_date,
            "covered": bool(venue_members) and latest_date == target_date,
            "member_count": len(venue_members),
            "error": error,
        }

    resolved: dict[str, LiveEligibility] = {}
    for raw_symbol in symbols:
        symbol = str(raw_symbol)
        item = metadata.get(symbol, {})
        venue = str(item.get("venue") or "").casefold() or None
        security_type = str(item.get("security_type") or "").casefold() or None
        venue_coverage = coverage.get(str(venue), {})
        covered = bool(venue_coverage.get("covered"))
        eligible = covered and symbol in members.get(str(venue), {})
        short_open = eligible and bool(members[str(venue)][symbol])
        if venue not in {"twse", "tpex"}:
            reason = "unknown_venue"
        elif not covered:
            reason = "exact_session_eligibility_missing"
        elif not eligible:
            reason = "not_day_trade_eligible"
        elif not short_open:
            reason = "sell_first_suspended"
        else:
            reason = None
        resolved[symbol] = LiveEligibility(
            symbol=symbol,
            venue=venue,
            security_type=security_type,
            eligible=eligible,
            short_open=short_open,
            covered=covered,
            source_date=target_date if covered else venue_coverage.get("latest_date"),
            reason=reason,
        )
    return resolved, coverage


def require_exact_session_eligibility(
    *,
    rule_data_dir: Path,
    parquet_root: Path,
    trading_date: date,
) -> dict[str, Any]:
    """Return official same-session coverage or fail before READY/trading."""

    _resolved, coverage = load_live_eligibility(
        rule_data_dir=rule_data_dir,
        parquet_root=parquet_root,
        symbols=(),
        trading_date=trading_date,
    )
    missing = {
        venue: row for venue, row in coverage.items() if not bool(row.get("covered"))
    }
    if missing:
        details = "; ".join(
            f"{venue} target={row.get('target_date')} "
            f"latest={row.get('latest_date') or 'missing'}"
            + (f" error={row.get('error')}" if row.get("error") else "")
            for venue, row in sorted(missing.items())
        )
        raise RuntimeError(
            f"exact-session day-trade eligibility unavailable: {details}"
        )
    return coverage


def quote_map_from_snapshot(
    symbols: Sequence[str],
    snapshot: PriceSnapshot,
    *,
    trading_date: date,
) -> dict[str, dict[str, Any]]:
    count = len(symbols)

    def values(source: np.ndarray | None) -> np.ndarray:
        if source is None:
            return np.full((count,), np.nan, dtype=np.float64)
        array = np.asarray(source, dtype=np.float64)
        if array.shape != (count,):
            raise ValueError(f"quote array shape {array.shape} != {(count,)}")
        return array

    last = values(snapshot.prices)
    open_prices = values(snapshot.open_prices)
    cumulative_volume_lots = values(snapshot.volumes)
    bid = values(snapshot.bid_prices)
    ask = values(snapshot.ask_prices)
    bid_volume = values(snapshot.bid_volumes)
    ask_volume = values(snapshot.ask_volumes)
    upper = values(snapshot.upper_limit_prices)
    lower = values(snapshot.lower_limit_prices)
    reference = values(snapshot.reference_prices)
    timestamps = (
        np.asarray(snapshot.timestamps_ms, dtype=np.int64)
        if snapshot.timestamps_ms is not None
        else np.zeros((count,), dtype=np.int64)
    )
    date_values = np.full((count,), np.datetime64(trading_date.isoformat(), "D"))
    computed_upper = limit_price_numpy(reference, 1.10, date_values)
    computed_lower = limit_price_numpy(reference, 0.90, date_values)
    upper = np.where(np.isfinite(upper), upper, computed_upper)
    lower = np.where(np.isfinite(lower), lower, computed_lower)

    output: dict[str, dict[str, Any]] = {}
    for idx, raw_symbol in enumerate(symbols):
        timestamp = None
        if int(timestamps[idx]) > 0:
            timestamp = (
                datetime.fromtimestamp(int(timestamps[idx]) / 1000.0, tz=timezone.utc)
                .astimezone(TAIPEI)
                .isoformat(timespec="milliseconds")
            )
        output[str(raw_symbol)] = {
            "symbol": str(raw_symbol),
            "last": _finite(last[idx]),
            "open": _finite(open_prices[idx]),
            "cumulative_volume_lots": _finite(cumulative_volume_lots[idx]),
            "bid": _finite(bid[idx]),
            "ask": _finite(ask[idx]),
            "bid_volume": _finite(bid_volume[idx]),
            "ask_volume": _finite(ask_volume[idx]),
            "upper_limit": _finite(upper[idx]),
            "lower_limit": _finite(lower[idx]),
            "reference_price": _finite(reference[idx]),
            "quote_at": timestamp,
            "source": snapshot.source,
        }
    return output


class TwDayTradeSimulationEngine:
    """One durable simulation ledger shared by all stock day-trade modes."""

    def __init__(
        self,
        state_dir: Path,
        *,
        final_settlement_path: str | Path = DEFAULT_TAIFEX_INDEX_FINAL_SETTLEMENT_PATH,
    ) -> None:
        self.state_dir = Path(state_dir)
        self.state_path = self.state_dir / "state.json"
        self.status_path = self.state_dir / "status.json"
        self.positions_path = self.state_dir / "positions.json"
        self.position_history_dir = self.state_dir / "position_history"
        self.signals_path = self.state_dir / "signals.jsonl"
        self.orders_path = self.state_dir / "orders.jsonl"
        self.fills_path = self.state_dir / "fills.jsonl"
        self.marks_path = self.state_dir / "marks.jsonl"
        self.benchmark_marks_path = self.state_dir / "benchmark_marks.jsonl"
        self.events_path = self.state_dir / "events.jsonl"
        self.latency_path = self.state_dir / "latency.jsonl"
        self.final_settlement_path = Path(final_settlement_path)
        self._tx_final_settlement_error: str | None = None
        self._corporate_action_reference_path: Path | None = None
        self._corporate_action_cache_signature: tuple[int, ...] | None = None
        self._corporate_actions_by_symbol: dict[str, list[dict[str, Any]]] = {}
        self._corporate_action_load_error: str | None = None
        self._corporate_action_coverage_end: date | None = None
        self.state = self._load_state()
        tx_benchmark_migrated = self._migrate_tx_continuous_benchmark_contract()
        self._reconcile_daily_duplicate_signal_ids()
        self._restore_position_artifact_paths()
        if tx_benchmark_migrated:
            _atomic_json(self.state_path, self.state)

    def _migrate_tx_continuous_benchmark_contract(self) -> bool:
        row = (self.state.get("benchmarks") or {}).get(
            TX_CONTINUOUS_BENCHMARK_ID
        )
        if not isinstance(row, dict):
            return False
        row["roll_contract_version"] = TX_CONTINUOUS_ROLL_CONTRACT_VERSION
        row["official_final_settlement_path"] = str(self.final_settlement_path)
        if int(row.get("roll_count") or 0) == 0:
            row.setdefault("origin_entry_price", row.get("entry_price"))
            row.setdefault("origin_entry_at", row.get("entry_at"))
            row.setdefault("current_contract_entry_price", row.get("entry_price"))
            row.setdefault("current_contract_entry_at", row.get("entry_at"))
        elif row.get("origin_entry_price") is None:
            # A legacy already-rolled row cannot prove its immutable origin
            # from the current-contract basis alone.  Keep it visible but do
            # not manufacture a rebase identity.
            row["roll_contract_migration_error"] = (
                "legacy_rolled_benchmark_origin_unavailable"
            )
        return True

    def _official_tx_final_settlement(
        self,
        *,
        delivery_date: date,
        delivery_month: str,
    ) -> dict[str, Any] | None:
        """Resolve one monthly TX terminal value from the official index FSP file.

        The retained file is produced from TAIFEX's index-option table, but a
        monthly TXO row and TX/MTX/TMF use the same underlying-index final
        settlement formula and value.  Requiring both date and six-digit month
        prevents a weekly option settlement from being mistaken for the
        expiring monthly future.
        """

        self._tx_final_settlement_error = None
        month = str(delivery_month or "").strip()
        if len(month) != 6 or not month.isdigit():
            self._tx_final_settlement_error = (
                f"invalid_contract_delivery_month:{delivery_month}"
            )
            return None
        if not self.final_settlement_path.is_file():
            self._tx_final_settlement_error = (
                f"missing_official_final_settlement_file:{self.final_settlement_path}"
            )
            return None
        try:
            import polars as pl

            frame = pl.read_parquet(self.final_settlement_path)
            required = {"settlement_date", "final_settlement_price"}
            if not required <= set(frame.columns):
                raise ValueError(
                    "official final-settlement file lacks settlement_date or price"
                )
            series_column = (
                "option_series"
                if "option_series" in frame.columns
                else "contract_delivery_month"
                if "contract_delivery_month" in frame.columns
                else None
            )
            if series_column is None:
                raise ValueError(
                    "official final-settlement file lacks a contract month column"
                )
            matched = frame.filter(
                (pl.col("settlement_date") == delivery_date)
                & (pl.col(series_column).cast(pl.String) == month)
            )
            if matched.height != 1:
                self._tx_final_settlement_error = (
                    "missing_official_tx_final_settlement:"
                    f"{delivery_date.isoformat()}:{month}:rows={matched.height}"
                )
                return None
            row = matched.row(0, named=True)
            price = _finite(row.get("final_settlement_price"))
            if price is None:
                raise ValueError("official TX final settlement is not positive finite")
        except Exception as exc:
            self._tx_final_settlement_error = (
                f"invalid_official_final_settlement:{type(exc).__name__}:{exc}"
            )
            return None
        return {
            "price": price,
            "settlement_date": delivery_date.isoformat(),
            "delivery_month": month,
            "source_file": str(row.get("source_file") or self.final_settlement_path),
            "source_sha256": str(row.get("source_sha256") or ""),
            "source_url": str(row.get("source_url") or ""),
            "price_source": "official_taifex_index_final_settlement",
        }

    def _reconcile_daily_duplicate_signal_ids(self) -> None:
        """Restore the accepted signal identity after a blocked duplicate.

        Older state writers recorded a later ``daily_signal_already_consumed``
        candidate as the mode's active signal even though the accepted signal,
        positions, and execution ledger were left untouched.  The append-only
        event ledger is authoritative for repairing that display-only mismatch.
        """

        if not self.events_path.is_file():
            return
        latest_registered: dict[str, str] = {}
        latest_event: dict[str, tuple[str, str, str | None]] = {}
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                except (TypeError, ValueError):
                    continue
                market = str(event.get("market") or "")
                if not market:
                    continue
                event_name = str(event.get("event") or "")
                signal_id = str(event.get("signal_id") or "")
                reason = (
                    str(event.get("reason")) if event.get("reason") is not None else None
                )
                if event_name == "signal_registered" and signal_id:
                    latest_registered[market] = signal_id
                if event_name in {"signal_registered", "signal_blocked"}:
                    latest_event[market] = (event_name, signal_id, reason)

        for market, mode in (self.state.get("modes") or {}).items():
            if not isinstance(mode, dict) or not mode.get("entry_completed_at"):
                continue
            registered_id = latest_registered.get(str(market))
            event_name, duplicate_id, reason = latest_event.get(
                str(market), ("", "", None)
            )
            if (
                registered_id
                and event_name == "signal_blocked"
                and reason == "daily_signal_already_consumed"
                and duplicate_id
                and str(mode.get("signal_id") or "") == duplicate_id
            ):
                mode["signal_id"] = registered_id
                mode["last_duplicate_signal_id"] = duplicate_id
                mode["last_duplicate_signal_reason"] = reason

    def _restore_position_artifact_paths(self) -> None:
        """Backfill position artifact pointers for signals accepted before schema 1."""

        for mode in (self.state.get("modes") or {}).values():
            if not isinstance(mode, dict):
                continue
            mode["executed_positions_path"] = str(self.positions_path)
            if mode.get("target_weights_path") and mode.get("target_positions_path"):
                continue
            source_path = str(mode.get("signal_source_path") or "").strip()
            if not source_path:
                continue
            try:
                summary = json.loads(Path(source_path).read_text(encoding="utf-8"))
            except (OSError, TypeError, ValueError):
                continue
            if not isinstance(summary, Mapping) or str(
                summary.get("signal_id") or ""
            ) != str(mode.get("signal_id") or ""):
                continue
            mode["target_weights_path"] = summary.get("weights_path")
            mode["target_positions_path"] = (
                summary.get("positions_markdown_path") or summary.get("weights_path")
            )
            mode["target_symbol_count"] = summary.get("symbol_count")
            mode["target_risk"] = summary.get("target_risk") or {}

    @staticmethod
    def _position_history_filename(market: object) -> str:
        normalized = "".join(
            character
            if character.isalnum() or character in {"-", "_"}
            else "_"
            for character in str(market or "unknown")
        )
        return f"{normalized or 'unknown'}.json"

    def _archive_mode_positions(
        self,
        mode: Mapping[str, Any],
        *,
        archived_at: datetime,
    ) -> None:
        """Persist the completed prior-session position lifecycle by date.

        ``state.json`` intentionally owns only the latest session.  Without a
        dated archive the dashboard date selector could retain marks and fills
        but lose the corresponding closed position rows as soon as the next
        signal arrived.
        """

        positions = [
            dict(position)
            for position in (mode.get("positions") or {}).values()
            if isinstance(position, Mapping)
        ]
        session_date = str(mode.get("session_date") or "").strip()
        if not session_date or not positions:
            return
        destination = (
            self.position_history_dir
            / session_date
            / self._position_history_filename(mode.get("market"))
        )
        _atomic_json(
            destination,
            {
                "schema_version": 1,
                "session_date": session_date,
                "market": mode.get("market"),
                "signal_id": mode.get("signal_id"),
                "archived_at": archived_at.isoformat(timespec="seconds"),
                "simulation_only": True,
                "production_order_possible": False,
                "positions": positions,
            },
        )

    def _load_state(self) -> dict[str, Any]:
        if self.state_path.is_file():
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and int(payload.get("schema_version", 0)) in {
                2,
                SIMULATION_SCHEMA_VERSION,
            }:
                prior_schema = int(payload.get("schema_version", 0))
                payload["schema_version"] = SIMULATION_SCHEMA_VERSION
                payload.setdefault("modes", {})
                payload.setdefault("benchmarks", {})
                payload.setdefault("minute_liquidity", {})
                if prior_schema < SIMULATION_SCHEMA_VERSION:
                    payload["migrated_from_schema_version"] = prior_schema
                    for mode in payload["modes"].values():
                        open_legacy = any(
                            int(position.get("signed_shares") or 0) != 0
                            for position in (mode.get("positions") or {}).values()
                        )
                        if open_legacy:
                            mode["engine_status"] = (
                                "critical_legacy_position_requires_reconciliation"
                            )
                            mode["legacy_execution_contract"] = True
                return payload
        return {
            "schema_version": SIMULATION_SCHEMA_VERSION,
            "simulation_only": True,
            "production_order_possible": False,
            "created_at": _iso(),
            "modes": {},
            "benchmarks": {},
            "minute_liquidity": {},
        }

    def prepare_minute_quotes(
        self,
        quotes: Mapping[str, Mapping[str, Any]],
        *,
        now: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Attach one-minute K-bar volume inferred from cumulative snapshots.

        A delta is accepted only between adjacent wall-clock minutes in the
        same session.  The 09:01 observation is the sole exception: its
        cumulative regular-board volume represents the completed opening
        auction plus first minute.  Service gaps fail closed instead of
        smearing several minutes of volume into one oversized fill budget.
        Repeated consumers in the same minute receive the cached same delta.
        """

        observed = _now_taipei(now)
        minute = observed.replace(second=0, microsecond=0)
        minute_key = minute.isoformat(timespec="minutes")
        session_date = observed.date().isoformat()
        ledger = self.state.setdefault("minute_liquidity", {})
        prepared: dict[str, dict[str, Any]] = {}
        for raw_symbol, raw_quote in quotes.items():
            symbol = str(raw_symbol)
            quote = dict(raw_quote)
            raw_cumulative = quote.get("cumulative_volume_lots")
            try:
                cumulative = float(raw_cumulative)
            except (TypeError, ValueError):
                cumulative = math.nan
            if not math.isfinite(cumulative) or cumulative < 0.0:
                cumulative = math.nan

            previous = ledger.get(symbol) or {}
            minute_lots: float | None = None
            if (
                str(previous.get("session_date") or "") == session_date
                and str(previous.get("minute") or "") == minute_key
            ):
                cached = previous.get("minute_volume_lots")
                if cached is not None:
                    minute_lots = float(cached)
            elif math.isfinite(cumulative):
                if minute.timetz().replace(tzinfo=None) == FIRST_MINUTE_EXECUTION_TIME:
                    minute_lots = cumulative
                elif str(previous.get("session_date") or "") == session_date:
                    previous_at = _parse_timestamp(previous.get("minute"))
                    previous_cumulative = previous.get("cumulative_volume_lots")
                    if (
                        previous_at is not None
                        and (minute - previous_at).total_seconds() == 60.0
                        and previous_cumulative is not None
                    ):
                        delta = cumulative - float(previous_cumulative)
                        if math.isfinite(delta) and delta >= 0.0:
                            minute_lots = delta
                ledger[symbol] = {
                    "session_date": session_date,
                    "minute": minute_key,
                    "cumulative_volume_lots": cumulative,
                    "minute_volume_lots": minute_lots,
                }
            quote["minute_volume_lots"] = minute_lots
            quote["minute_volume_source"] = (
                "adjacent_cumulative_snapshot_delta"
                if minute_lots is not None
                else "unavailable_non_adjacent_or_missing_snapshot"
            )
            prepared[symbol] = quote
        return prepared

    def benchmark_fallback_prices(self) -> dict[str, float]:
        """Return last executable stock marks for the next snapshot fallback."""

        output: dict[str, float] = {}
        benchmarks = self.state.get("benchmarks") or {}
        for benchmark_id, symbol, _label, _security_type in STOCK_BENCHMARKS:
            row = benchmarks.get(benchmark_id) or {}
            price = _finite(row.get("last_mark_price") or row.get("entry_price"))
            output[symbol] = price or 1.0
        return output

    def benchmark_tx_contract(self) -> str | None:
        row = (self.state.get("benchmarks") or {}).get(TX_CONTINUOUS_BENCHMARK_ID) or {}
        code = str(row.get("contract_code") or "").strip().upper()
        return code or None

    @staticmethod
    def _stock_benchmark_fee_rates(
        *,
        symbol: str,
        security_type: str,
        fee_schedule: TaiwanFeeSchedule,
    ) -> tuple[float, float]:
        buy, sell = effective_fee_rate_vectors(
            [symbol],
            "tw_cash",
            fee_schedule=fee_schedule,
            security_types=[security_type],
        )
        return float(buy[0]), float(sell[0])

    @staticmethod
    def _stock_benchmark_order_cost(
        *,
        notional: float,
        commission_rate: float,
        tax_rate: float,
        fee_schedule: TaiwanFeeSchedule,
    ) -> tuple[float, float]:
        commission = float(
            _commission_fees_by_symbol(
                np.asarray([notional], dtype=np.float64),
                np.asarray([commission_rate], dtype=np.float64),
                minimum_commission=float(fee_schedule.minimum_commission),
                rounding=str(fee_schedule.commission_rounding),
            )[0]
        )
        tax = float(
            _tax_fees_by_symbol(
                np.asarray([notional], dtype=np.float64),
                np.asarray([tax_rate], dtype=np.float64),
                rounding=str(fee_schedule.tax_rounding),
            )[0]
        )
        return commission, tax

    def _append_benchmark_mark(
        self,
        benchmark: Mapping[str, Any],
        *,
        now: datetime,
    ) -> None:
        _append_jsonl(
            self.benchmark_marks_path,
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "minute": now.replace(second=0, microsecond=0).isoformat(
                    timespec="minutes"
                ),
                "session_date": now.date().isoformat(),
                **{
                    key: benchmark.get(key)
                    for key in (
                        "benchmark_id",
                        "label",
                        "instrument_type",
                        "symbol",
                        "logical_code",
                        "contract_code",
                        "initial_capital_twd",
                        "total_equity_twd",
                        "net_pnl_twd",
                        "return_fraction",
                        "return_pct",
                        "last_mark_price",
                        "last_quote_at",
                        "valuation_stale",
                        "valuation_source",
                        "entry_at",
                        "entry_price",
                        "roll_count",
                        "previous_contract_code",
                        "last_roll_at",
                        "last_roll_old_bid",
                        "last_roll_new_ask",
                        "fixed_fees_twd",
                        "transaction_tax_twd",
                        "total_return_contract",
                        "corporate_action_factor",
                        "adjusted_quantity",
                        "corporate_action_count",
                        "last_corporate_action_date",
                        "corporate_action_coverage",
                        "corporate_action_status",
                        "corporate_action_coverage_end",
                    )
                },
            },
        )

    def _load_corporate_actions(
        self, reference_path: str | Path | None
    ) -> None:
        """Cache the official ex-right/ex-dividend reference by file identity."""

        if reference_path is None:
            return
        path = Path(reference_path).expanduser().resolve()
        self._corporate_action_reference_path = path
        try:
            stat = path.stat()
            summary_path = path.with_suffix(".summary.json")
            summary_stat = summary_path.stat()
            signature = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                summary_stat.st_dev,
                summary_stat.st_ino,
                summary_stat.st_size,
                summary_stat.st_mtime_ns,
            )
            if signature == self._corporate_action_cache_signature:
                return
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if (
                not isinstance(summary, Mapping)
                or not bool(summary.get("coverage_complete"))
                or int(summary.get("failure_count") or 0) != 0
            ):
                raise ValueError("corporate-action completeness receipt failed")
            coverage_end = date.fromisoformat(str(summary.get("end_date") or ""))
            import polars as pl

            frame = (
                pl.scan_parquet(path)
                .filter(pl.col("symbol").is_in([row[1] for row in STOCK_BENCHMARKS]))
                .select(
                    pl.col("date").cast(pl.Date),
                    pl.col("symbol").cast(pl.String),
                    pl.col("previous_close").cast(pl.Float64),
                    pl.col("reference_price").cast(pl.Float64),
                    pl.col("event_type").cast(pl.String),
                )
                .sort(["symbol", "date"])
                .collect()
            )
            by_symbol: dict[str, list[dict[str, Any]]] = {}
            for item in frame.iter_rows(named=True):
                by_symbol.setdefault(str(item["symbol"]), []).append(dict(item))
            self._corporate_actions_by_symbol = by_symbol
            self._corporate_action_cache_signature = signature
            self._corporate_action_load_error = None
            self._corporate_action_coverage_end = coverage_end
        except Exception as exc:
            self._corporate_actions_by_symbol = {}
            self._corporate_action_cache_signature = None
            self._corporate_action_load_error = f"{type(exc).__name__}: {exc}"
            self._corporate_action_coverage_end = None

    def _stock_total_return_adjustment(
        self,
        *,
        symbol: str,
        entry_at: object,
        mark_date: date,
    ) -> tuple[float | None, list[dict[str, Any]], str]:
        """Return the reinvested-unit factor implied by official reference prices.

        A holder crossing an ex-date receives equivalent economic value when
        units are multiplied by ``previous_close / reference_price``.  The
        official factor handles cash distributions, stock dividends, splits,
        reverse splits, and mixed capital actions without guessing their type.
        """

        if self._corporate_action_reference_path is None:
            return 1.0, [], "not_configured"
        if self._corporate_action_load_error is not None:
            return None, [], "reference_unavailable"
        parsed_entry = _parse_timestamp(entry_at)
        if parsed_entry is None:
            return None, [], "entry_timestamp_invalid"
        # Both prices in a same-session benchmark are already on the same side
        # of that session's ex-right/ex-dividend boundary.  No distribution or
        # split can be crossed between the opening ask and a later bid, so the
        # exact total-return factor is 1 even while the official daily archive
        # still ends at the preceding completed session.  Cross-session marks
        # continue to fail closed until the reference covers the mark date.
        if parsed_entry.date() == mark_date:
            return 1.0, [], "same_session_no_action_boundary"
        if (
            self._corporate_action_coverage_end is None
            or self._corporate_action_coverage_end < mark_date
        ):
            return None, [], "reference_coverage_incomplete"
        factor = 1.0
        applied: list[dict[str, Any]] = []
        for item in self._corporate_actions_by_symbol.get(str(symbol), ()):
            action_date = item.get("date")
            if not isinstance(action_date, date):
                return None, [], "reference_date_invalid"
            # The opening ask on the entry date is already ex-action.  Only
            # actions crossed while holding the benchmark belong in return.
            if not (parsed_entry.date() < action_date <= mark_date):
                continue
            previous_close = _finite(item.get("previous_close"))
            reference_price = _finite(item.get("reference_price"))
            if (
                previous_close is None
                or reference_price is None
                or previous_close <= 0.0
                or reference_price <= 0.0
            ):
                return None, [], "reference_factor_invalid"
            action_factor = previous_close / reference_price
            factor *= action_factor
            applied.append(
                {
                    "date": action_date.isoformat(),
                    "event_type": item.get("event_type"),
                    "previous_close": previous_close,
                    "reference_price": reference_price,
                    "factor": action_factor,
                }
            )
        if not math.isfinite(factor) or factor <= 0.0:
            return None, [], "cumulative_factor_invalid"
        return factor, applied, "official_reference_complete"

    def _mark_stock_benchmark(
        self,
        *,
        benchmark_id: str,
        symbol: str,
        label: str,
        security_type: str,
        quote: Mapping[str, Any],
        fee_schedule: TaiwanFeeSchedule,
        now: datetime,
    ) -> None:
        benchmarks = self.state.setdefault("benchmarks", {})
        row = benchmarks.setdefault(
            benchmark_id,
            {
                "benchmark_id": benchmark_id,
                "label": label,
                "instrument_type": "stock_buy_and_hold",
                "symbol": symbol,
                "quantity": 1_000,
                "fixed_fees_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "valuation_stale": True,
            },
        )
        buy_rate, sell_rate = self._stock_benchmark_fee_rates(
            symbol=symbol,
            security_type=security_type,
            fee_schedule=fee_schedule,
        )
        ask = _finite(quote.get("ask"))
        bid = _finite(quote.get("bid"))
        quantity = int(row.get("quantity") or 1_000)
        if row.get("entry_price") is None and ask is not None:
            entry_notional = quantity * ask
            entry_commission, _entry_tax = self._stock_benchmark_order_cost(
                notional=entry_notional,
                commission_rate=buy_rate,
                tax_rate=0.0,
                fee_schedule=fee_schedule,
            )
            row.update(
                {
                    "entry_price": ask,
                    "entry_at": now.isoformat(timespec="seconds"),
                    "initial_capital_twd": entry_notional,
                    "fixed_fees_twd": entry_commission,
                    "capital_basis": "one_board_lot_entry_notional",
                }
            )
        entry = _finite(row.get("entry_price"))
        adjustment_factor, corporate_actions, action_status = (
            self._stock_total_return_adjustment(
                symbol=symbol,
                entry_at=row.get("entry_at"),
                mark_date=now.date(),
            )
        )
        row.update(
            {
                "total_return_contract": (
                    "official_ex_date_reference_reinvestment_v1"
                ),
                "corporate_action_coverage": action_status
                == "official_reference_complete",
                "corporate_action_status": action_status,
                "corporate_action_coverage_end": (
                    self._corporate_action_coverage_end.isoformat()
                    if self._corporate_action_coverage_end is not None
                    else None
                ),
            }
        )
        if entry is not None and bid is not None and adjustment_factor is not None:
            adjusted_quantity = quantity * adjustment_factor
            gross_value = adjusted_quantity * bid
            entry_notional = quantity * entry
            gross_pnl = gross_value - entry_notional
            liquidation_notional = gross_value
            liquidation_commission, liquidation_tax = self._stock_benchmark_order_cost(
                notional=liquidation_notional,
                commission_rate=buy_rate,
                tax_rate=max(0.0, sell_rate - buy_rate),
                fee_schedule=fee_schedule,
            )
            liquidation_cost = liquidation_commission + liquidation_tax
            net_pnl = (
                gross_pnl - float(row.get("fixed_fees_twd") or 0.0) - liquidation_cost
            )
            initial_capital = float(row.get("initial_capital_twd") or 0.0)
            return_fraction = (
                net_pnl / initial_capital if initial_capital > 0.0 else None
            )
            row.update(
                {
                    "last_mark_price": bid,
                    "last_quote_at": quote.get("quote_at"),
                    "last_mark_at": now.isoformat(timespec="seconds"),
                    "liquidation_cost_twd": liquidation_cost,
                    "net_pnl_twd": net_pnl,
                    "total_equity_twd": initial_capital + net_pnl,
                    "return_fraction": return_fraction,
                    "return_pct": None
                    if return_fraction is None
                    else return_fraction * 100.0,
                    "corporate_action_factor": adjustment_factor,
                    "adjusted_quantity": adjusted_quantity,
                    "corporate_action_count": len(corporate_actions),
                    "last_corporate_action_date": (
                        corporate_actions[-1]["date"] if corporate_actions else None
                    ),
                    "applied_corporate_actions": corporate_actions,
                    "valuation_stale": False,
                    "valuation_source": "total_return_units_at_best_bid_after_tw_cash_costs",
                    "source": quote.get("source"),
                }
            )
        elif adjustment_factor is None:
            row["valuation_stale"] = True
            row["valuation_source"] = (
                "corporate_action_reference_unavailable_fail_closed"
            )
        elif row.get("total_equity_twd") is not None:
            row["valuation_stale"] = True
            row["valuation_source"] = "carried_forward_last_complete_mark"
        self._append_benchmark_mark(row, now=now)

    @staticmethod
    def _tx_trade_tax(price: float, *, trading_date: date) -> float:
        return taifex_tax_per_contract_twd(
            price,
            multiplier_twd_per_point=TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
            tax_rate=stock_index_futures_tax_rate(trading_date),
        )

    def _mark_tx_continuous_benchmark(
        self,
        *,
        current_contract_code: str | None,
        current_quote: Mapping[str, Any],
        previous_contract_quote: Mapping[str, Any],
        now: datetime,
    ) -> None:
        benchmarks = self.state.setdefault("benchmarks", {})
        row = benchmarks.setdefault(
            TX_CONTINUOUS_BENCHMARK_ID,
            {
                "benchmark_id": TX_CONTINUOUS_BENCHMARK_ID,
                "label": "台指期無限轉倉（大台一口）",
                "instrument_type": "continuous_long_future",
                "logical_code": TX_CONTINUOUS_LOGICAL_CODE,
                "multiplier_twd_per_point": TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"],
                "realized_gross_pnl_twd": 0.0,
                "fixed_fees_twd": 0.0,
                "transaction_tax_twd": 0.0,
                "roll_count": 0,
                "roll_contract_version": TX_CONTINUOUS_ROLL_CONTRACT_VERSION,
                "valuation_stale": True,
            },
        )
        row["roll_contract_version"] = TX_CONTINUOUS_ROLL_CONTRACT_VERSION
        row["official_final_settlement_path"] = str(self.final_settlement_path)
        current_code = str(current_contract_code or "").strip().upper()
        held_code = str(row.get("contract_code") or "").strip().upper()
        current_ask = _finite(current_quote.get("ask"))
        current_bid = _finite(current_quote.get("bid"))
        fee_per_side = TAIFEX_INDEX_FUTURES_FEE_PER_SIDE_TWD["TX"]
        multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS["TX"]

        def update_contract_identity(
            target: dict[str, Any], quote: Mapping[str, Any]
        ) -> None:
            delivery_month = str(quote.get("delivery_month") or "").strip()
            delivery_date = _date_value(
                quote.get("last_trading_date") or quote.get("delivery_date")
            )
            if delivery_month:
                target["contract_delivery_month"] = delivery_month
            if delivery_date is not None:
                target["contract_last_trading_date"] = delivery_date.isoformat()

        if row.get("entry_price") is None and current_code and current_ask is not None:
            row.update(
                {
                    "contract_code": current_code,
                    "entry_price": current_ask,
                    "entry_at": now.isoformat(timespec="seconds"),
                    "origin_entry_price": current_ask,
                    "origin_entry_at": now.isoformat(timespec="seconds"),
                    "current_contract_entry_price": current_ask,
                    "current_contract_entry_at": now.isoformat(timespec="seconds"),
                    "initial_capital_twd": taifex_initial_margin_twd("TX", now.date()),
                    "fixed_fees_twd": fee_per_side,
                    "transaction_tax_twd": self._tx_trade_tax(
                        current_ask, trading_date=now.date()
                    ),
                    "capital_basis": "official_taifex_initial_margin_at_entry",
                }
            )
            update_contract_identity(row, current_quote)
            held_code = current_code

        if row.get("entry_price") is not None:
            row.setdefault("origin_entry_price", row.get("entry_price"))
            row.setdefault("origin_entry_at", row.get("entry_at"))
            row["current_contract_entry_price"] = row.get("entry_price")
            row.setdefault("current_contract_entry_at", row.get("entry_at"))

        if held_code == current_code:
            update_contract_identity(row, current_quote)
        elif held_code:
            update_contract_identity(row, previous_contract_quote)

        if (
            row.get("entry_price") is not None
            and current_code
            and held_code != current_code
        ):
            old_bid = _finite(previous_contract_quote.get("bid"))
            held_last_trading_date = _date_value(
                row.get("contract_last_trading_date")
            )
            held_delivery_month = str(
                row.get("contract_delivery_month") or ""
            ).strip()
            expired = (
                held_last_trading_date is not None
                and (
                    now.date() > held_last_trading_date
                    or (
                        now.date() == held_last_trading_date
                        and now.time() >= SESSION_CLOSE
                    )
                )
            )
            official_settlement = (
                self._official_tx_final_settlement(
                    delivery_date=held_last_trading_date,
                    delivery_month=held_delivery_month,
                )
                if expired and held_last_trading_date is not None
                else None
            )
            old_exit_price = (
                float(official_settlement["price"])
                if official_settlement is not None
                else old_bid
                if not expired
                else None
            )
            if old_exit_price is not None and current_ask is not None:
                entry = float(row["entry_price"])
                row["realized_gross_pnl_twd"] = (
                    float(row.get("realized_gross_pnl_twd") or 0.0)
                    + (old_exit_price - entry) * multiplier
                )
                old_exit_fee = 0.0 if official_settlement is not None else fee_per_side
                row["fixed_fees_twd"] = (
                    float(row.get("fixed_fees_twd") or 0.0)
                    + old_exit_fee
                    + fee_per_side
                )
                row["transaction_tax_twd"] = (
                    float(row.get("transaction_tax_twd") or 0.0)
                    + self._tx_trade_tax(
                        old_exit_price,
                        trading_date=held_last_trading_date or now.date(),
                    )
                    + self._tx_trade_tax(current_ask, trading_date=now.date())
                )
                row.update(
                    {
                        "previous_contract_code": held_code,
                        "contract_code": current_code,
                        "entry_price": current_ask,
                        "current_contract_entry_price": current_ask,
                        "current_contract_entry_at": now.isoformat(
                            timespec="seconds"
                        ),
                        "last_roll_at": now.isoformat(timespec="seconds"),
                        "last_roll_old_bid": (
                            old_bid if official_settlement is None else None
                        ),
                        "last_roll_old_price": old_exit_price,
                        "last_roll_old_price_source": (
                            "official_taifex_index_final_settlement"
                            if official_settlement is not None
                            else "executable_old_contract_bid"
                        ),
                        "last_roll_official_final_settlement": (
                            official_settlement["price"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_official_settlement_source_file": (
                            official_settlement["source_file"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_official_settlement_source_sha256": (
                            official_settlement["source_sha256"]
                            if official_settlement is not None
                            else None
                        ),
                        "last_roll_new_ask": current_ask,
                        "roll_count": int(row.get("roll_count") or 0) + 1,
                    }
                )
                update_contract_identity(row, current_quote)
                roll_history = list(row.get("roll_history") or ())
                roll_history.append(
                    {
                        "rolled_at": now.isoformat(timespec="seconds"),
                        "from_contract": held_code,
                        "to_contract": current_code,
                        "old_bid": old_bid if official_settlement is None else None,
                        "old_exit_price": old_exit_price,
                        "old_exit_price_source": (
                            "official_taifex_index_final_settlement"
                            if official_settlement is not None
                            else "executable_old_contract_bid"
                        ),
                        "official_final_settlement": official_settlement,
                        "new_ask": current_ask,
                        "old_exit_fee_twd": old_exit_fee,
                        "new_entry_fee_twd": fee_per_side,
                    }
                )
                row["roll_history"] = roll_history[-100:]
                row["roll_blocked_reason"] = None
                _append_jsonl(
                    self.events_path,
                    {
                        "event": "benchmark_tx_continuous_rolled",
                        "recorded_at": now.isoformat(timespec="seconds"),
                        "from_contract": held_code,
                        "to_contract": current_code,
                        "old_exit_price": old_exit_price,
                        "old_exit_price_source": row[
                            "last_roll_old_price_source"
                        ],
                        "new_entry_ask": current_ask,
                        "official_final_settlement": official_settlement,
                        "realized_gross_pnl_twd": row[
                            "realized_gross_pnl_twd"
                        ],
                        "roll_contract_version": (
                            TX_CONTINUOUS_ROLL_CONTRACT_VERSION
                        ),
                    },
                )
                held_code = current_code
            else:
                row["valuation_stale"] = True
                if expired and official_settlement is None:
                    row["roll_blocked_reason"] = self._tx_final_settlement_error
                    row["valuation_source"] = (
                        "roll_waiting_for_official_final_settlement_and_new_ask"
                    )
                else:
                    row["roll_blocked_reason"] = (
                        "missing_old_bid" if old_bid is None else "missing_new_ask"
                    )
                    row["valuation_source"] = "roll_waiting_for_old_bid_and_new_ask"
                self._append_benchmark_mark(row, now=now)
                return

        if (
            row.get("entry_price") is not None
            and held_code == current_code
            and current_bid is not None
        ):
            open_gross_pnl = (current_bid - float(row["entry_price"])) * multiplier
            liquidation_fee = fee_per_side
            liquidation_tax = self._tx_trade_tax(current_bid, trading_date=now.date())
            net_pnl = (
                float(row.get("realized_gross_pnl_twd") or 0.0)
                + open_gross_pnl
                - float(row.get("fixed_fees_twd") or 0.0)
                - float(row.get("transaction_tax_twd") or 0.0)
                - liquidation_fee
                - liquidation_tax
            )
            initial_capital = float(row.get("initial_capital_twd") or 0.0)
            return_fraction = (
                net_pnl / initial_capital if initial_capital > 0.0 else None
            )
            row.update(
                {
                    "last_mark_price": current_bid,
                    "last_quote_at": current_quote.get("quote_at"),
                    "last_mark_at": now.isoformat(timespec="seconds"),
                    "liquidation_cost_twd": liquidation_fee + liquidation_tax,
                    "net_pnl_twd": net_pnl,
                    "total_equity_twd": initial_capital + net_pnl,
                    "return_fraction": return_fraction,
                    "return_pct": None
                    if return_fraction is None
                    else return_fraction * 100.0,
                    "valuation_stale": False,
                    "valuation_source": (
                        "one_tx_ask_entry_bid_liquidation_with_official_expiry_"
                        "settlement_roll_fee_and_tax"
                    ),
                    "source": current_quote.get("source"),
                }
            )
        elif row.get("total_equity_twd") is not None:
            row["valuation_stale"] = True
            row["valuation_source"] = "carried_forward_last_complete_mark"
        self._append_benchmark_mark(row, now=now)

    def process_benchmarks(
        self,
        *,
        stock_quotes: Mapping[str, Mapping[str, Any]],
        stock_fee_schedule: TaiwanFeeSchedule,
        current_future_contract_code: str | None,
        current_future_quote: Mapping[str, Any] | None,
        previous_future_quote: Mapping[str, Any] | None = None,
        corporate_action_reference_path: str | Path | None = None,
        now: datetime | None = None,
    ) -> None:
        """Advance three independent, executable-price comparison ledgers."""

        observed = _now_taipei(now)
        self._load_corporate_actions(corporate_action_reference_path)
        for benchmark_id, symbol, label, security_type in STOCK_BENCHMARKS:
            self._mark_stock_benchmark(
                benchmark_id=benchmark_id,
                symbol=symbol,
                label=label,
                security_type=security_type,
                quote=stock_quotes.get(symbol) or {},
                fee_schedule=stock_fee_schedule,
                now=observed,
            )
        self._mark_tx_continuous_benchmark(
            current_contract_code=current_future_contract_code,
            current_quote=current_future_quote or {},
            previous_contract_quote=previous_future_quote or {},
            now=observed,
        )
        self._persist(observed)

    def _mode(self, spec: ModeSpec) -> dict[str, Any]:
        mode = self.state.setdefault("modes", {}).setdefault(spec.market, {})
        mode["market"] = spec.market
        mode["label"] = spec.label
        mode.setdefault("initial_capital_twd", float(spec.initial_capital_twd))
        mode.setdefault("cumulative_realized_net_pnl_twd", 0.0)
        mode.setdefault("cumulative_commission_rebate_accrued_twd", 0.0)
        mode.setdefault("open_net_liquidation_pnl_twd", 0.0)
        mode.setdefault("total_equity_twd", float(spec.initial_capital_twd))
        mode.setdefault("open_position_count", 0)
        mode.setdefault("stale_position_count", 0)
        mode.setdefault("force_exit_failures", 0)
        mode.setdefault("positions", {})
        mode.setdefault("processed_signal_ids", [])
        mode["config_path"] = spec.config_path
        mode["checkpoint_path"] = spec.checkpoint_path
        mode["live_output_dir"] = str(spec.live_output_dir)
        mode["signal_market"] = spec.signal_market or spec.market
        mode["executed_positions_path"] = str(self.positions_path)
        mode["price_limit_offset_ticks"] = int(spec.price_limit_offset_ticks)
        mode["bracket_price_policy"] = (
            "inside_daily_limits_by_ticks"
            if int(spec.price_limit_offset_ticks) > 0
            else "full_daily_limits"
        )
        mode["fill_guaranteed"] = False
        return mode

    def update_readiness(
        self,
        specs: Sequence[ModeSpec],
        *,
        now: datetime | None = None,
        errors: Mapping[str, str] | None = None,
        current_eligibility_coverage: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        observed = _now_taipei(now)
        for spec in specs:
            mode = self._mode(spec)
            checkpoint = Path(spec.checkpoint_path) if spec.checkpoint_path else None
            checkpoint_ready = bool(checkpoint and checkpoint.is_file())
            mode["checkpoint_ready"] = checkpoint_ready
            mode["readiness_error"] = (errors or {}).get(spec.market)
            if current_eligibility_coverage is not None:
                mode["current_eligibility_coverage"] = dict(
                    current_eligibility_coverage.get(spec.market) or {}
                )
            has_open_position = bool(mode.get("positions")) and any(
                int(item.get("signed_shares") or 0) != 0
                for item in mode["positions"].values()
            )
            if bool(mode.get("legacy_execution_contract")) and has_open_position:
                mode["engine_status"] = (
                    "critical_legacy_position_requires_reconciliation"
                )
            elif has_open_position:
                mode["engine_status"] = (
                    "critical_unflattened_after_13_24"
                    if observed.timetz().replace(tzinfo=None) >= FORCE_EXIT_TIME
                    else "active"
                )
            elif (
                mode.get("session_valid") is False
                and str(mode.get("session_date") or "")
                == observed.date().isoformat()
                and mode.get("non_session_invalidated_at")
            ):
                mode["engine_status"] = "invalid_non_trading_session"
            elif not checkpoint_ready:
                mode["engine_status"] = "blocked_missing_checkpoint"
            elif str(
                mode.get("session_date") or ""
            ) == observed.date().isoformat() and mode.get("entry_completed_at"):
                projection = mode.get("execution_projection") or {}
                if bool(projection.get("collapsed_to_flat")):
                    mode["engine_status"] = "flat_directional_mix_unexecutable"
                elif mode.get("positions"):
                    mode["engine_status"] = "session_flat_after_exit"
                else:
                    mode["engine_status"] = "flat_no_executable_signal"
            elif observed.weekday() >= 5:
                mode["engine_status"] = "waiting_trading_day"
            elif observed.timetz().replace(tzinfo=None) < ENTRY_GATE:
                mode["engine_status"] = "waiting_09_00_signal"
            elif observed.timetz().replace(tzinfo=None) >= SESSION_CLOSE:
                mode["engine_status"] = "session_complete"
            else:
                mode["engine_status"] = "waiting_signal"
        self._persist(observed)

    def rearm_flat_session(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Allow one replacement signal only when the session never filled.

        This is an operational recovery path for a signal that was consumed
        while an exact-session prerequisite was unavailable.  It deliberately
        preserves processed signal IDs and the append-only signal ledger, and
        refuses to rearm after any position or fill so it cannot become a
        same-day double-entry bypass.
        """

        observed = _now_taipei(now)
        wall_time = observed.timetz().replace(tzinfo=None)
        if wall_time < ENTRY_GATE or wall_time >= EXIT_LIMIT_TIME:
            raise RuntimeError("flat-session rearm is outside the entry window")
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("flat-session rearm requires an audit reason")
        mode = (self.state.get("modes") or {}).get(str(market))
        if not isinstance(mode, dict):
            raise KeyError(f"unknown simulation market: {market}")
        session_date = observed.date().isoformat()
        if str(mode.get("session_date") or "") != session_date:
            raise RuntimeError(
                f"{market} has no consumed signal for current session {session_date}"
            )
        if not mode.get("entry_completed_at"):
            return "already_armed"
        if mode.get("positions"):
            raise RuntimeError(f"{market} has position history and cannot be rearmed")
        if self._session_has_fill(str(market), session_date):
            raise RuntimeError(f"{market} has fills and cannot be rearmed")

        previous_signal_id = mode.get("signal_id")
        previous_entry_completed_at = mode.get("entry_completed_at")
        previous_signal_counts = dict(mode.get("signal_counts") or {})
        mode["entry_completed_at"] = None
        mode["pending_signal_id"] = None
        mode["pending_signal_at"] = None
        mode["exit_limit_submitted_at"] = None
        mode["force_exit_started_at"] = None
        mode["closing_auction_submitted_at"] = None
        mode["closing_auction_settled_at"] = None
        mode["residual_conversion_completed_at"] = None
        mode["engine_status"] = "waiting_signal"
        mode["blocked_reason"] = None
        mode["signal_counts"] = {}
        mode["rearmed_at"] = observed.isoformat(timespec="seconds")
        mode["rearm_reason"] = normalized_reason
        mode["rearm_count"] = int(mode.get("rearm_count") or 0) + 1
        self._event(
            "flat_session_rearmed",
            recorded_at=observed,
            market=market,
            session_date=session_date,
            reason=normalized_reason,
            previous_signal_id=previous_signal_id,
            previous_entry_completed_at=previous_entry_completed_at,
            previous_signal_counts=previous_signal_counts,
        )
        self._persist(observed)
        return "rearmed"

    def invalidate_non_session_flat_signal(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Void an impossible non-session signal without deleting its audit trail."""

        observed = _now_taipei(now)
        normalized_reason = str(reason or "").strip()
        if not normalized_reason:
            raise ValueError("non-session invalidation requires an audit reason")
        mode = (self.state.get("modes") or {}).get(str(market))
        if not isinstance(mode, dict):
            raise KeyError(f"unknown simulation market: {market}")
        session_date = str(mode.get("session_date") or "")
        if session_date != observed.date().isoformat():
            return "no_current_session_signal"
        if mode.get("non_session_invalidated_at"):
            return "already_invalidated"
        if mode.get("positions"):
            raise RuntimeError(
                f"{market} has positions; non-session signal cannot be auto-invalidated"
            )
        if self._session_has_fill(str(market), session_date):
            raise RuntimeError(
                f"{market} has fills; non-session signal cannot be auto-invalidated"
            )
        previous_signal_id = mode.get("signal_id")
        previous_entry_completed_at = mode.get("entry_completed_at")
        mode["entry_completed_at"] = None
        mode["pending_signal_id"] = None
        mode["pending_signal_at"] = None
        mode["engine_status"] = "invalid_non_trading_session"
        mode["blocked_reason"] = "non_trading_session"
        mode["session_valid"] = False
        mode["non_session_invalidated_at"] = observed.isoformat(timespec="seconds")
        mode["non_session_invalidation_reason"] = normalized_reason
        self._event(
            "non_session_signal_invalidated",
            recorded_at=observed,
            market=market,
            session_date=session_date,
            signal_id=previous_signal_id,
            reason=normalized_reason,
            previous_entry_completed_at=previous_entry_completed_at,
            positions=0,
            fills=0,
        )
        self._persist(observed)
        return "invalidated"

    def retire_flat_mode(
        self,
        market: str,
        *,
        now: datetime | None = None,
        reason: str,
    ) -> str:
        """Remove a mistakenly configured flat mode while preserving its logs."""

        observed = _now_taipei(now)
        normalized_market = str(market or "").strip()
        normalized_reason = str(reason or "").strip()
        if not normalized_market or not normalized_reason:
            raise ValueError("retiring a simulation mode requires market and reason")
        modes = self.state.get("modes") or {}
        mode = modes.get(normalized_market)
        if not isinstance(mode, dict):
            return "absent"
        if any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            raise RuntimeError(f"{normalized_market} has an open position")
        session_date = str(mode.get("session_date") or observed.date().isoformat())
        if self._session_has_fill(normalized_market, session_date):
            raise RuntimeError(f"{normalized_market} has fills and cannot be retired")
        modes.pop(normalized_market)
        self._event(
            "simulation_mode_retired",
            recorded_at=observed,
            market=normalized_market,
            reason=normalized_reason,
        )
        self._persist(observed)
        return "retired"

    def _session_has_fill(self, market: str, session_date: str) -> bool:
        if not self.fills_path.is_file():
            return False
        with self.fills_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if (
                    str(row.get("market") or "") == market
                    and str(row.get("session_date") or "") == session_date
                    and int(row.get("quantity") or 0) > 0
                ):
                    return True
        return False

    def _event(
        self,
        event: str,
        *,
        recorded_at: datetime | None = None,
        **payload: Any,
    ) -> None:
        _append_jsonl(
            self.events_path,
            {"recorded_at": _iso(recorded_at), "event": event, **payload},
        )

    def _order(self, payload: Mapping[str, Any]) -> None:
        _append_jsonl(self.orders_path, payload)

    def _fill(self, payload: Mapping[str, Any]) -> None:
        _append_jsonl(self.fills_path, payload)

    def record_latency_sample(
        self,
        *,
        market: str,
        signal_id: str,
        result: str,
        summary: Mapping[str, Any],
        consumer_detected_at: datetime,
        ledger_persisted_at: datetime,
        executor_quote_fetch_ms: float,
        eligibility_load_ms: float,
        ledger_compute_persist_ms: float,
    ) -> None:
        """Persist one measured input-to-ledger sample for the public panel."""

        started_at = _parse_timestamp(
            summary.get("signal_started_at") or summary.get("generated_at")
        )
        ready_at = _parse_timestamp(
            summary.get("signal_ready_at")
            or summary.get("artifact_published_at")
            or summary.get("generated_at")
        )
        published_at = _parse_timestamp(
            summary.get("artifact_published_at") or summary.get("signal_ready_at")
        )
        live_latency = dict(summary.get("live_latency") or {})
        signal_quote_ms = _finite(live_latency.get("quote_fetch_ms"))
        model_inference_ms = _finite(live_latency.get("model_inference_ms"))
        signal_total_ms = _finite(live_latency.get("compute_before_publish_ms"))
        detailed_signal_stages = {
            "signal_pre_quote_prepare_ms": _finite(
                live_latency.get("pre_quote_prepare_ms")
            ),
            "signal_pre_inference_prepare_ms": _finite(
                live_latency.get("pre_inference_prepare_ms")
            ),
            "signal_post_inference_format_ms": _finite(
                live_latency.get("post_inference_format_ms")
            ),
        }
        has_detailed_signal_stages = any(
            value is not None for value in detailed_signal_stages.values()
        )
        signal_other_ms = None
        if signal_total_ms is not None and not has_detailed_signal_stages:
            signal_other_ms = max(
                0.0,
                signal_total_ms
                - float(signal_quote_ms or 0.0)
                - float(model_inference_ms or 0.0),
            )

        def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
            if start is None or end is None:
                return None
            return round(max(0.0, (end - start).total_seconds() * 1000.0), 3)

        stages = {
            **detailed_signal_stages,
            "signal_quote_fetch_ms": signal_quote_ms,
            "model_inference_ms": model_inference_ms,
            "signal_other_compute_ms": signal_other_ms,
            "artifact_publish_ms": _finite(
                live_latency.get("artifact_publish_ms")
            ),
            "artifact_discovery_ms": elapsed_ms(
                published_at, consumer_detected_at
            ),
            "eligibility_load_ms": round(max(0.0, eligibility_load_ms), 3),
            "executor_quote_fetch_ms": round(max(0.0, executor_quote_fetch_ms), 3),
            "ledger_compute_persist_ms": round(
                max(0.0, ledger_compute_persist_ms), 3
            ),
        }
        finite_stages = {
            key: float(value)
            for key, value in stages.items()
            if value is not None and math.isfinite(float(value))
        }
        bottleneck = (
            max(finite_stages, key=finite_stages.get) if finite_stages else None
        )
        _append_jsonl(
            self.latency_path,
            {
                "schema_version": 1,
                "recorded_at": ledger_persisted_at.isoformat(timespec="microseconds"),
                "session_date": ledger_persisted_at.astimezone(TAIPEI)
                .date()
                .isoformat(),
                "market": market,
                "signal_id": signal_id,
                "result": result,
                "simulation_only": True,
                "measurement_boundary": "signal_input_to_simulation_ledger_persisted",
                "signal_started_at": started_at.isoformat(timespec="microseconds")
                if started_at
                else None,
                "signal_ready_at": ready_at.isoformat(timespec="microseconds")
                if ready_at
                else None,
                "artifact_published_at": published_at.isoformat(
                    timespec="microseconds"
                )
                if published_at
                else None,
                "consumer_detected_at": consumer_detected_at.isoformat(
                    timespec="microseconds"
                ),
                "ledger_persisted_at": ledger_persisted_at.isoformat(
                    timespec="microseconds"
                ),
                "input_to_ledger_ms": elapsed_ms(started_at, ledger_persisted_at),
                "ready_to_ledger_ms": elapsed_ms(ready_at, ledger_persisted_at),
                "signal_compute_total_ms": signal_total_ms,
                "stages": stages,
                "bottleneck_stage": bottleneck,
                "bottleneck_ms": finite_stages.get(bottleneck)
                if bottleneck
                else None,
            },
        )

    @staticmethod
    def _entry_and_exit_rates(
        *,
        symbols: Sequence[str],
        security_types: Sequence[str],
        fee_schedule: TaiwanFeeSchedule,
    ) -> dict[str, tuple[float, float, float, float, float]]:
        buy, sell = gross_fee_rate_vectors(
            symbols,
            "tw_day_trade",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        rebate = commission_rebate_rate_vector(
            symbols,
            "tw_day_trade",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        cash_buy, cash_sell = gross_fee_rate_vectors(
            symbols,
            "tw_cash",
            fee_schedule=fee_schedule,
            security_types=security_types,
        )
        return {
            str(symbol): (
                float(buy[idx]),
                float(sell[idx]),
                float(rebate[idx]),
                float(cash_buy[idx]),
                float(cash_sell[idx]),
            )
            for idx, symbol in enumerate(symbols)
        }

    def register_signal(
        self,
        *,
        spec: ModeSpec,
        summary: Mapping[str, Any],
        signal_rows: Sequence[Mapping[str, Any]],
        quotes: Mapping[str, Mapping[str, Any]],
        eligibility: Mapping[str, LiveEligibility],
        eligibility_coverage: Mapping[str, Any],
        now: datetime | None = None,
        counterfactual_open_replay: bool = False,
    ) -> str:
        observed = _now_taipei(now)
        mode = self._mode(spec)
        signal_id = str(summary.get("signal_id") or "").strip()
        if not signal_id:
            raise ValueError("signal summary has no signal_id")
        if signal_id in set(mode.get("processed_signal_ids") or ()):
            return "already_processed"
        if str(summary.get("execution_mode") or "") != "tw_day_trade":
            return self._block_signal(mode, signal_id, "not_tw_day_trade", observed)
        source_signal_at = _parse_timestamp(
            summary.get("signal_started_at")
            or summary.get("generated_at")
            or summary.get("asof_date")
        )
        if source_signal_at is None:
            return self._block_signal(
                mode, signal_id, "invalid_signal_timestamp", observed
            )
        if source_signal_at.date() != observed.date():
            return self._block_signal(
                mode, signal_id, "signal_not_current_session", observed
            )
        wall_time = observed.timetz().replace(tzinfo=None)
        if counterfactual_open_replay:
            if not bool(summary.get("simulation_replay")):
                raise ValueError(
                    "counterfactual open replay requires simulation_replay=true"
                )
            if str(summary.get("entry_fill_contract") or "") != (
                "retrospective_actual_session_open_price_counterfactual"
            ):
                raise ValueError(
                    "counterfactual open replay requires the retrospective "
                    "session-open fill contract"
                )
            if wall_time != ENTRY_GATE:
                raise ValueError(
                    "counterfactual open replay must be recorded exactly at 09:00"
                )
            signal_at = observed
        else:
            signal_at = _parse_timestamp(
                summary.get("signal_ready_at")
                or summary.get("artifact_published_at")
                or summary.get("generated_at")
            )
            if signal_at is None:
                return self._block_signal(
                    mode, signal_id, "invalid_signal_ready_timestamp", observed
                )
        if wall_time < ENTRY_GATE or wall_time >= EXIT_LIMIT_TIME:
            return self._block_signal(mode, signal_id, "outside_entry_window", observed)
        if not bool(summary.get("live_session_open_feature_applied")):
            return self._block_signal(
                mode, signal_id, "open_feature_not_observed", observed
            )
        if str(
            mode.get("session_date") or ""
        ) == observed.date().isoformat() and mode.get("entry_completed_at"):
            return self._block_signal(
                mode, signal_id, "daily_signal_already_consumed", observed
            )
        if any(
            int(position.get("signed_shares") or 0) != 0
            for position in (mode.get("positions") or {}).values()
        ):
            return self._block_signal(
                mode,
                signal_id,
                "prior_position_unflattened",
                observed,
                engine_status="critical_prior_position_unflattened",
            )

        actionable_symbols: list[str] = []
        for row in signal_rows:
            symbol = str(row.get("symbol") or "")
            weight = float(row.get("target_weight") or 0.0)
            side_allowed = bool(row.get("can_buy")) if weight > 0.0 else bool(
                row.get("can_sell")
            )
            evidence = eligibility.get(symbol)
            sizing_price = _finite(row.get("open_price")) or _finite(
                quotes.get(symbol, {}).get("open")
            )
            if (
                not symbol
                or weight == 0.0
                or not bool(row.get("tradable"))
                or not side_allowed
                or evidence is None
                or not evidence.covered
                or not evidence.eligible
                or (weight < 0.0 and not evidence.short_open)
                or sizing_price is None
            ):
                continue
            requested_shares = int(
                math.floor(
                    abs(weight)
                    * float(spec.initial_capital_twd)
                    / sizing_price
                    / int(spec.lot_size)
                )
            ) * int(spec.lot_size)
            if requested_shares > 0:
                actionable_symbols.append(symbol)
        later_quote_found = any(
            (quote_at := _parse_timestamp(quotes.get(symbol, {}).get("quote_at")))
            is not None
            and quote_at <= observed
            and (
                quote_at >= signal_at
                if counterfactual_open_replay
                else quote_at > signal_at
            )
            for symbol in actionable_symbols
        )
        if actionable_symbols and not later_quote_found:
            mode["pending_signal_id"] = signal_id
            mode["pending_signal_at"] = signal_at.isoformat(timespec="seconds")
            mode["engine_status"] = "waiting_causally_later_quote"
            self._persist(observed)
            return "waiting_quote"

        security_types = []
        symbols = []
        for row in signal_rows:
            symbol = str(row.get("symbol") or "")
            if not symbol:
                continue
            evidence = eligibility.get(symbol)
            security_type = evidence.security_type if evidence else None
            symbols.append(symbol)
            security_types.append(
                security_type if security_type in {"stock", "etf"} else "stock"
            )
        fee_rates = self._entry_and_exit_rates(
            symbols=symbols,
            security_types=security_types,
            fee_schedule=spec.fee_schedule,
        )

        prior_session_date = str(mode.get("session_date") or "")
        if prior_session_date and prior_session_date != observed.date().isoformat():
            self._archive_mode_positions(mode, archived_at=observed)

        mode["session_date"] = observed.date().isoformat()
        mode["session_valid"] = True
        mode.pop("non_session_invalidated_at", None)
        mode.pop("non_session_invalidation_reason", None)
        mode["blocked_reason"] = None
        mode["signal_id"] = signal_id
        mode["signal_at"] = signal_at.isoformat(timespec="seconds")
        mode["source_signal_at"] = source_signal_at.isoformat(timespec="seconds")
        mode["counterfactual_open_replay"] = bool(counterfactual_open_replay)
        mode["signal_source_path"] = summary.get("summary_path")
        mode["target_weights_path"] = summary.get("weights_path")
        mode["target_positions_path"] = (
            summary.get("positions_markdown_path") or summary.get("weights_path")
        )
        mode["target_symbol_count"] = summary.get("symbol_count") or len(signal_rows)
        mode["target_risk"] = dict(summary.get("target_risk") or {})
        mode["feature_cutoff_date"] = summary.get("feature_cutoff_date")
        mode["checkpoint_fingerprint"] = summary.get("checkpoint_fingerprint")
        mode["config_fingerprint"] = summary.get("config_fingerprint")
        mode["eligibility_coverage"] = dict(eligibility_coverage)
        mode["simulation_replay"] = bool(summary.get("simulation_replay", False))
        mode["replay_basis"] = summary.get("replay_basis")
        mode["replay_source"] = summary.get("replay_source")
        mode["entry_fill_contract"] = summary.get("entry_fill_contract") or (
            "best_ask_for_buy_best_bid_for_sell"
        )
        mode["entry_liquidity_assumption"] = summary.get(
            "entry_liquidity_assumption"
        ) or "09:00_fresh_level_one_depth_then_minimum_with_50pct_completed_minute_volume"
        mode["positions"] = {}
        mode["entry_completed_at"] = observed.isoformat(timespec="seconds")
        mode["exit_limit_submitted_at"] = None
        mode["force_exit_started_at"] = None
        mode["closing_auction_submitted_at"] = None
        mode["closing_auction_settled_at"] = None
        mode["residual_conversion_completed_at"] = None
        mode["force_exit_failures"] = 0
        mode["terminal_flatten_count"] = 0
        mode["terminal_flatten_degraded_count"] = 0
        counts: dict[str, int] = {}
        signal_records: list[dict[str, Any]] = []
        order_records: list[dict[str, Any]] = []
        fill_records: list[dict[str, Any]] = []

        plans = [
            _prepare_entry_plan(
                row,
                quote=quotes.get(str(row.get("symbol") or "")) or {},
                evidence=eligibility.get(str(row.get("symbol") or "")),
                signal_at=signal_at,
                observation_at=observed,
                spec=spec,
                allow_quote_at_signal=counterfactual_open_replay,
            )
            for row in signal_rows
            if str(row.get("symbol") or "")
        ]
        balanced_shares, directional_mix = preserve_directional_lot_mix(
            np.asarray([plan["target_weight"] for plan in plans], dtype=np.float64),
            np.asarray([plan["filled_shares"] for plan in plans], dtype=np.int64),
            np.asarray(
                [
                    plan["entry_price"] if plan["entry_price"] is not None else np.nan
                    for plan in plans
                ],
                dtype=np.float64,
            ),
            np.full((len(plans),), int(spec.lot_size), dtype=np.int64),
            capital=float(spec.initial_capital_twd),
        )
        for plan, adjusted_shares in zip(plans, balanced_shares, strict=True):
            previous_filled = int(plan["filled_shares"])
            plan["filled_shares"] = int(adjusted_shares)
            plan["directional_mix_adjusted"] = int(adjusted_shares) != previous_filled
            if int(adjusted_shares) < previous_filled:
                plan["pre_balance_status"] = plan["status"]
                plan["pre_balance_reason"] = plan["reason"]
                if int(adjusted_shares) == 0:
                    plan["status"] = "skipped"
                    plan["reason"] = "directional_mix_preserved"
                else:
                    plan["status"] = "partial_directional_mix"
                    plan["reason"] = "directional_mix_scaled"
        mode["execution_projection"] = asdict(directional_mix)

        for plan in plans:
            row = plan["row"]
            symbol = plan["symbol"]
            target_weight = float(plan["target_weight"])
            side = plan["side"]
            quote = plan["quote"]
            evidence = plan["evidence"]
            status = plan["status"]
            reason = plan["reason"]
            entry_price = plan["entry_price"]
            sizing_price = plan["sizing_price"]
            upper = plan["upper"]
            lower = plan["lower"]
            requested_shares = int(plan["requested_shares"])
            filled_shares = int(plan["filled_shares"])
            top_book_capacity_shares = int(plan["top_book_capacity_shares"])
            minute_kbar_capacity_shares = int(plan["minute_kbar_capacity_shares"])
            offset_ticks = int(spec.price_limit_offset_ticks)
            filled_weight = (
                (1.0 if side == "long" else -1.0)
                * filled_shares
                * entry_price
                / float(spec.initial_capital_twd)
                if filled_shares > 0 and entry_price is not None
                else 0.0
            )
            pre_balance_filled_weight = (
                (1.0 if side == "long" else -1.0)
                * int(plan["pre_balance_filled_shares"])
                * entry_price
                / float(spec.initial_capital_twd)
                if int(plan["pre_balance_filled_shares"]) > 0
                and entry_price is not None
                else 0.0
            )

            signal_record = {
                "recorded_at": observed.isoformat(timespec="seconds"),
                "session_date": observed.date().isoformat(),
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "price_limit_offset_ticks": offset_ticks,
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": source_signal_at.isoformat(timespec="seconds"),
                "symbol": symbol,
                "name": row.get("name"),
                "side": side,
                "action": row.get("action"),
                "score": row.get("score"),
                "raw_score": row.get("raw_score"),
                "target_weight": target_weight,
                "requested_shares": requested_shares,
                "executable_order_shares": filled_shares,
                "target_unsubmitted_shares": max(0, requested_shares - filled_shares),
                "sizing_open_price": sizing_price,
                "execution_price": entry_price,
                "filled_shares": filled_shares,
                "filled_weight": filled_weight,
                "pre_balance_filled_shares": int(plan["pre_balance_filled_shares"]),
                "pre_balance_filled_weight": pre_balance_filled_weight,
                "directional_mix_adjusted": bool(plan.get("directional_mix_adjusted")),
                "pre_balance_status": plan.get("pre_balance_status"),
                "pre_balance_reason": plan.get("pre_balance_reason"),
                "top_book_capacity_shares": top_book_capacity_shares,
                "minute_kbar_volume_lots": quote.get("minute_volume_lots"),
                "minute_kbar_capacity_shares": minute_kbar_capacity_shares,
                "minute_volume_participation": MINUTE_VOLUME_PARTICIPATION,
                "status": status,
                "reason": reason,
                "quote_at": quote.get("quote_at"),
                "bid": quote.get("bid"),
                "ask": quote.get("ask"),
                "bid_volume_lots": quote.get("bid_volume"),
                "ask_volume_lots": quote.get("ask_volume"),
                "upper_limit": upper,
                "lower_limit": lower,
                "eligibility_covered": bool(evidence and evidence.covered),
                "day_trade_eligible": bool(evidence and evidence.eligible),
                "sell_first_allowed": bool(evidence and evidence.short_open),
                "quote_source": quote.get("source"),
                "simulation_replay": bool(mode.get("simulation_replay")),
                "replay_basis": mode.get("replay_basis"),
                "counterfactual_open_replay": bool(counterfactual_open_replay),
            }
            signal_records.append(signal_record)
            counts[status] = counts.get(status, 0) + 1
            if (
                status
                not in {
                    "ready",
                    "partial_depth",
                    "partial_directional_mix",
                }
                or entry_price is None
            ):
                continue

            upper_bracket = float(
                move_price_ticks_numpy(
                    np.asarray([upper], dtype=np.float64),
                    -offset_ticks,
                    np.asarray([observed.date()]),
                )[0]
            )
            lower_bracket = float(
                move_price_ticks_numpy(
                    np.asarray([lower], dtype=np.float64),
                    offset_ticks,
                    np.asarray([observed.date()]),
                )[0]
            )

            signed_shares = filled_shares if side == "long" else -filled_shares
            buy_rate, sell_rate, rebate_rate, cash_buy_rate, cash_sell_rate = fee_rates[
                symbol
            ]
            entry_rate = buy_rate if side == "long" else sell_rate
            entry_gross_fee = filled_shares * entry_price * entry_rate
            entry_rebate = filled_shares * entry_price * rebate_rate
            entry_fee = entry_gross_fee - entry_rebate
            position_id = f"{spec.market}:{observed.date().isoformat()}:{symbol}"
            entry_order_id = f"{position_id}:entry"
            position = {
                "position_id": position_id,
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "session_date": observed.date().isoformat(),
                "signal_id": signal_id,
                "signal_at": signal_at.isoformat(timespec="seconds"),
                "source_signal_at": source_signal_at.isoformat(timespec="seconds"),
                "symbol": symbol,
                "name": row.get("name"),
                "side": side,
                "target_weight": target_weight,
                "requested_shares": requested_shares,
                "executable_order_shares": filled_shares,
                "target_unsubmitted_shares": max(0, requested_shares - filled_shares),
                "pre_balance_filled_shares": int(plan["pre_balance_filled_shares"]),
                "filled_shares": filled_shares,
                "signed_shares": signed_shares,
                "lot_size": int(spec.lot_size),
                "entry_order_id": entry_order_id,
                "entry_at": observed.isoformat(timespec="seconds"),
                "entry_quote_at": quote.get("quote_at"),
                "entry_price": entry_price,
                "sizing_open_price": sizing_price,
                "entry_fee_twd": entry_fee,
                "remaining_entry_fee_twd": entry_fee,
                "entry_gross_fee_and_tax_twd": entry_gross_fee,
                "entry_commission_rebate_accrued_twd": entry_rebate,
                "buy_fee_rate": buy_rate,
                "sell_fee_rate": sell_rate,
                "commission_rebate_rate": rebate_rate,
                "cash_buy_fee_rate": cash_buy_rate,
                "cash_sell_fee_rate": cash_sell_rate,
                "security_type": (
                    evidence.security_type
                    if evidence and evidence.security_type in {"stock", "etf"}
                    else "stock"
                ),
                "upper_limit": upper,
                "lower_limit": lower,
                "take_profit_price": upper_bracket if side == "long" else lower_bracket,
                "stop_trigger_price": lower_bracket if side == "long" else upper_bracket,
                "price_limit_offset_ticks": offset_ticks,
                "bracket_price_policy": (
                    "inside_daily_limits_by_ticks"
                    if offset_ticks > 0
                    else "full_daily_limits"
                ),
                "fill_guaranteed": False,
                "take_profit_order_status": "working",
                "stop_order_status": "armed_local_trigger",
                "eod_limit_order_status": None,
                "status": "open",
                "last_mark_price": entry_price,
                "last_complete_net_pnl_twd": -entry_fee,
                "realized_gross_pnl_twd": 0.0,
                "realized_exit_fee_twd": 0.0,
                "realized_net_pnl_twd": 0.0,
                "valuation_stale": False,
                "simulation_replay": bool(mode.get("simulation_replay")),
                "replay_basis": mode.get("replay_basis"),
                "replay_source": mode.get("replay_source"),
                "counterfactual_open_replay": bool(counterfactual_open_replay),
            }
            mode["positions"][position_id] = position
            mode["cumulative_commission_rebate_accrued_twd"] = (
                float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
                + entry_rebate
            )
            order_base = {
                "recorded_at": observed.isoformat(timespec="seconds"),
                "session_date": observed.date().isoformat(),
                "market": spec.market,
                "signal_market": spec.signal_market or spec.market,
                "price_limit_offset_ticks": offset_ticks,
                "position_id": position_id,
                "symbol": symbol,
                "quantity": filled_shares,
                "simulation_only": True,
            }
            order_records.append(
                {
                    **order_base,
                    "order_id": entry_order_id,
                    "purpose": "entry",
                    "side": "buy" if side == "long" else "sell_short",
                    "order_type": "MKT",
                    "status": "filled",
                    "filled_quantity": filled_shares,
                    "unfilled_quantity": 0,
                    "model_requested_quantity": requested_shares,
                    "target_unsubmitted_quantity": max(
                        0, requested_shares - filled_shares
                    ),
                }
            )
            fill_records.append(
                {
                    **order_base,
                    "order_id": entry_order_id,
                    "purpose": "entry",
                    "fill_at": observed.isoformat(timespec="seconds"),
                    "quote_at": quote.get("quote_at"),
                    "quantity": filled_shares,
                    "price": entry_price,
                    "fee_and_tax_twd": entry_fee,
                    "gross_fee_and_tax_twd": entry_gross_fee,
                    "commission_rebate_accrued_twd": entry_rebate,
                    "fill_contract": mode.get("entry_fill_contract"),
                    "depth_assumption": mode.get("entry_liquidity_assumption"),
                    "simulation_replay": bool(mode.get("simulation_replay")),
                    "replay_basis": mode.get("replay_basis"),
                }
            )
            for purpose, order_type, price, order_status in (
                ("take_profit", "LMT", position["take_profit_price"], "working"),
                (
                    "stop_loss",
                    "LOCAL_STOP_MKT",
                    position["stop_trigger_price"],
                    "armed",
                ),
            ):
                order_records.append(
                    {
                        **order_base,
                        "order_id": f"{position_id}:{purpose}",
                        "purpose": purpose,
                        "side": "sell" if side == "long" else "buy_to_cover",
                        "order_type": order_type,
                        "price": price,
                        "quantity": filled_shares,
                        "status": order_status,
                    }
                )

        # One signal is one logical append transaction per ledger. This keeps
        # every row durable before state.json points at the accepted signal,
        # while avoiding thousands of per-row fsync calls on the 09:00 path.
        _append_jsonl_many(self.signals_path, signal_records)
        _append_jsonl_many(self.orders_path, order_records)
        _append_jsonl_many(self.fills_path, fill_records)

        processed = list(mode.get("processed_signal_ids") or ())
        processed.append(signal_id)
        mode["processed_signal_ids"] = processed[-32:]
        mode["pending_signal_id"] = None
        mode["signal_counts"] = counts
        mode["engine_status"] = (
            "active"
            if any(
                int(item.get("signed_shares") or 0) != 0
                for item in mode["positions"].values()
            )
            else (
                "flat_directional_mix_unexecutable"
                if directional_mix.collapsed_to_flat
                else "flat_no_executable_signal"
            )
        )
        self._event(
            "signal_registered",
            recorded_at=observed,
            market=spec.market,
            signal_id=signal_id,
            counts=counts,
            execution_projection=asdict(directional_mix),
            simulation_replay=bool(mode.get("simulation_replay")),
            replay_basis=mode.get("replay_basis"),
            source_signal_at=source_signal_at.isoformat(timespec="seconds"),
            counterfactual_open_replay=bool(counterfactual_open_replay),
        )
        self._mark_mode(spec.market, observed, quotes)
        self._persist(observed)
        return "registered"

    def _block_signal(
        self,
        mode: dict[str, Any],
        signal_id: str,
        reason: str,
        now: datetime,
        *,
        engine_status: str = "blocked_signal",
    ) -> str:
        processed = list(mode.get("processed_signal_ids") or ())
        processed.append(signal_id)
        mode["processed_signal_ids"] = processed[-32:]
        if reason == "daily_signal_already_consumed":
            mode["last_duplicate_signal_id"] = signal_id
            mode["last_duplicate_signal_reason"] = reason
            mode["last_duplicate_signal_blocked_at"] = now.isoformat(
                timespec="seconds"
            )
            self._event(
                "signal_blocked",
                recorded_at=now,
                market=mode.get("market"),
                signal_id=signal_id,
                reason=reason,
            )
            self._persist(now)
            return "blocked"
        mode["signal_id"] = signal_id
        mode["engine_status"] = engine_status
        mode["blocked_reason"] = reason
        self._event(
            "signal_blocked",
            recorded_at=now,
            market=mode.get("market"),
            signal_id=signal_id,
            reason=reason,
        )
        self._persist(now)
        return "blocked"

    def process_quotes(
        self,
        *,
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime | None = None,
        append_mark_history: bool = True,
    ) -> None:
        observed = _now_taipei(now)
        wall_time = observed.timetz().replace(tzinfo=None)
        for market, mode in self.state.get("modes", {}).items():
            positions = mode.get("positions") or {}
            if bool(mode.get("legacy_execution_contract")) and any(
                int(position.get("signed_shares") or 0) != 0
                for position in positions.values()
            ):
                # Schema-migrated positions may only be reduced.  Reuse the
                # ordinary bracket close path once a real executable quote and
                # completed-minute capacity exist; never open or enlarge a
                # legacy position merely to unblock the next session.
                for position in positions.values():
                    if int(position.get("signed_shares") or 0) == 0:
                        continue
                    quote = quotes.get(str(position.get("symbol"))) or {}
                    self._apply_bracket(position, mode, quote, observed)
                self._mark_mode(
                    market,
                    observed,
                    quotes,
                    append_history=append_mark_history,
                )
                still_open = any(
                    int(position.get("signed_shares") or 0) != 0
                    for position in positions.values()
                )
                if still_open:
                    mode["engine_status"] = (
                        "critical_legacy_position_requires_reconciliation"
                    )
                else:
                    mode["legacy_execution_contract"] = False
                    mode["legacy_reconciled_at"] = observed.isoformat(
                        timespec="seconds"
                    )
                    mode["engine_status"] = "waiting_signal"
                    self._event(
                        "legacy_positions_reconciled",
                        recorded_at=observed,
                        market=market,
                        session_date=observed.date().isoformat(),
                    )
                continue
            if wall_time < EXIT_LIMIT_TIME:
                for position in positions.values():
                    if int(position.get("signed_shares") or 0) == 0:
                        continue
                    quote = quotes.get(str(position.get("symbol"))) or {}
                    self._apply_bracket(position, mode, quote, observed)
            elif not mode.get("exit_limit_submitted_at"):
                self._submit_exit_limits(market, mode, quotes, observed)
            if EXIT_LIMIT_TIME <= wall_time < FORCE_EXIT_TIME:
                self._fill_crossed_exit_limits(mode, quotes, observed)
            if FORCE_EXIT_TIME <= wall_time < CLOSING_AUCTION_TIME:
                self._force_exit(market, mode, quotes, observed)
            if CLOSING_AUCTION_TIME <= wall_time < SESSION_CLOSE:
                self._submit_closing_auction_limits(market, mode, observed)
            if wall_time >= SESSION_CLOSE:
                self._submit_closing_auction_limits(market, mode, observed)
                self._settle_closing_auction(market, mode, quotes, observed)
                self._convert_residual_to_carry(market, mode, quotes, observed)
            self._mark_mode(
                market,
                observed,
                quotes,
                append_history=append_mark_history,
            )
        self._persist(observed)

    def _apply_bracket(
        self,
        position: dict[str, Any],
        mode: dict[str, Any],
        quote: Mapping[str, Any],
        now: datetime,
    ) -> None:
        signed = int(position.get("signed_shares") or 0)
        if signed == 0:
            return
        bid = _finite(quote.get("bid"))
        ask = _finite(quote.get("ask"))
        last = _finite(quote.get("last"))
        side = str(position.get("side"))
        take_profit = float(position["take_profit_price"])
        stop = float(position["stop_trigger_price"])
        tp_hit = (side == "long" and bid is not None and bid >= take_profit) or (
            side == "short" and ask is not None and ask <= take_profit
        )
        if tp_hit:
            capacity = _executable_capacity_shares(
                quote,
                transaction_side="sell" if side == "long" else "buy",
                lot_size=int(position.get("lot_size") or 1_000),
            )
            if capacity <= 0:
                position["take_profit_order_status"] = "working_no_displayed_volume"
                return
            self._close_position(
                position,
                mode,
                price=take_profit,
                quote=quote,
                now=now,
                reason=(
                    "take_profit_inside_daily_limit_"
                    f"{int(position.get('price_limit_offset_ticks') or 0)}_tick"
                    if int(position.get("price_limit_offset_ticks") or 0) > 0
                    else "take_profit_full_price_limit"
                ),
                order_type="LMT",
                quantity=capacity,
            )
            return
        stop_reference = last if last is not None else bid if side == "long" else ask
        stop_hit = stop_reference is not None and (
            (side == "long" and stop_reference <= stop)
            or (side == "short" and stop_reference >= stop)
        )
        if not stop_hit and not str(position.get("stop_order_status") or "").startswith(
            "triggered"
        ):
            return
        position["stop_order_status"] = "triggered_waiting_liquidity"
        executable = bid if side == "long" else ask
        capacity = _executable_capacity_shares(
            quote,
            transaction_side="sell" if side == "long" else "buy",
            lot_size=int(position.get("lot_size") or 1_000),
        )
        if executable is None or capacity <= 0:
            position["status"] = "stop_triggered_waiting_liquidity"
            return
        self._close_position(
            position,
            mode,
            price=executable,
            quote=quote,
            now=now,
            reason=(
                "stop_loss_inside_daily_limit_"
                f"{int(position.get('price_limit_offset_ticks') or 0)}_tick_trigger"
                if int(position.get("price_limit_offset_ticks") or 0) > 0
                else "stop_loss_full_price_limit_trigger"
            ),
            order_type="MKT",
            quantity=capacity,
        )

    def _submit_exit_limits(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        mode["exit_limit_submitted_at"] = now.isoformat(timespec="seconds")
        for position in (mode.get("positions") or {}).values():
            quote = quotes.get(str(position.get("symbol"))) or {}
            self._place_exit_limit(
                market=market,
                mode=mode,
                position=position,
                quote=quote,
                now=now,
                purpose="13_20_exit_limit",
            )
        self._event("exit_limits_submitted", recorded_at=now, market=market)

    def _place_exit_limit(
        self,
        *,
        market: str,
        mode: dict[str, Any],
        position: dict[str, Any],
        quote: Mapping[str, Any],
        now: datetime,
        purpose: str,
    ) -> bool:
        signed = int(position.get("signed_shares") or 0)
        if signed == 0:
            return False
        side = str(position.get("side"))
        passive_price = _finite(quote.get("ask" if side == "long" else "bid"))
        position["take_profit_order_status"] = "cancelled_replaced_at_13_20"
        if passive_price is None:
            position["eod_limit_order_status"] = "not_submitted_no_quote"
            return False
        position["eod_limit_price"] = passive_price
        position["eod_limit_submitted_at"] = now.isoformat(timespec="seconds")
        position["eod_limit_order_status"] = "working"
        self._order(
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "session_date": mode.get("session_date"),
                "market": market,
                "position_id": position.get("position_id"),
                "symbol": position.get("symbol"),
                "order_id": f"{position.get('position_id')}:eod_limit",
                "purpose": purpose,
                "side": "sell" if side == "long" else "buy_to_cover",
                "order_type": "LMT",
                "price": passive_price,
                "quantity": abs(signed),
                "status": "working",
                "simulation_only": True,
                "pricing_rule": "passive_best_ask_for_sell_best_bid_for_buy",
            }
        )
        return True

    def _fill_crossed_exit_limits(
        self,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            if position.get("eod_limit_order_status") == "not_submitted_no_quote":
                self._place_exit_limit(
                    market=str(mode.get("market")),
                    mode=mode,
                    position=position,
                    quote=quote,
                    now=now,
                    purpose="13_20_exit_limit_late_quote",
                )
            if position.get("eod_limit_order_status") not in {"working", "part_filled"}:
                continue
            side = str(position.get("side"))
            limit_price = float(position["eod_limit_price"])
            bid = _finite(quote.get("bid"))
            ask = _finite(quote.get("ask"))
            crossed = (side == "long" and bid is not None and bid >= limit_price) or (
                side == "short" and ask is not None and ask <= limit_price
            )
            if crossed:
                capacity = _executable_capacity_shares(
                    quote,
                    transaction_side="sell" if side == "long" else "buy",
                    lot_size=int(position.get("lot_size") or 1_000),
                )
                if capacity <= 0:
                    position["eod_limit_liquidity_status"] = "no_displayed_volume"
                    continue
                self._close_position(
                    position,
                    mode,
                    price=limit_price,
                    quote=quote,
                    now=now,
                    reason="13_20_limit_filled",
                    order_type="LMT",
                    quantity=capacity,
                )
                continue
            # The user's 13:20 contract is to remain at the passive best quote,
            # not merely to preserve the price observed exactly at 13:20. Once
            # per new minute, an unfilled order is cancel-replaced to the
            # current ask for a sell or bid for a buy-to-cover.
            passive_price = _finite(quote.get("ask" if side == "long" else "bid"))
            if passive_price is None or math.isclose(
                passive_price,
                limit_price,
                rel_tol=0.0,
                abs_tol=1e-12,
            ):
                continue
            previous_price = limit_price
            position["eod_limit_price"] = passive_price
            position["eod_limit_submitted_at"] = now.isoformat(timespec="seconds")
            position["eod_limit_order_status"] = "working"
            position["eod_limit_reprice_count"] = int(
                position.get("eod_limit_reprice_count") or 0
            ) + 1
            self._order(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": mode.get("session_date"),
                    "market": mode.get("market"),
                    "position_id": position.get("position_id"),
                    "symbol": position.get("symbol"),
                    "order_id": (
                        f"{position.get('position_id')}:eod_limit:"
                        f"reprice:{int(position['eod_limit_reprice_count'])}"
                    ),
                    "replaces_order_id": f"{position.get('position_id')}:eod_limit",
                    "purpose": "13_20_exit_limit_reprice",
                    "side": "sell" if side == "long" else "buy_to_cover",
                    "order_type": "LMT",
                    "previous_price": previous_price,
                    "price": passive_price,
                    "quantity": abs(int(position.get("signed_shares") or 0)),
                    "status": "working",
                    "simulation_only": True,
                    "pricing_rule": "follow_passive_best_until_13_24",
                }
            )

    def _force_exit(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        if not mode.get("force_exit_started_at"):
            mode["force_exit_started_at"] = now.isoformat(timespec="seconds")
            self._event("force_exit_started", recorded_at=now, market=market)
        failures = 0
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            side = str(position.get("side"))
            executable = _finite(quote.get("bid" if side == "long" else "ask"))
            capacity = _force_exit_retry_capacity_shares(
                position,
                quote,
                transaction_side="sell" if side == "long" else "buy",
                lot_size=int(position.get("lot_size") or 1_000),
                now=now,
            )
            if executable is None or capacity <= 0:
                failures += 1
                position["status"] = "force_exit_unfilled_no_executable_depth"
                position["eod_limit_order_status"] = "cancelled_at_13_24"
                continue
            before = abs(int(position.get("signed_shares") or 0))
            self._close_position(
                position,
                mode,
                price=executable,
                quote=quote,
                now=now,
                reason="13_24_market_force_exit",
                order_type="MKT",
                quantity=capacity,
            )
            filled = before - abs(int(position.get("signed_shares") or 0))
            position["force_exit_minute_consumed_shares"] = (
                int(position.get("force_exit_minute_consumed_shares") or 0) + filled
            )
            if int(position.get("signed_shares") or 0) != 0:
                failures += 1
        mode["force_exit_failures"] = failures
        if failures:
            mode["engine_status"] = "critical_unflattened_after_13_24"

    def _submit_closing_auction_limits(
        self,
        market: str,
        mode: dict[str, Any],
        now: datetime,
    ) -> None:
        """Replace residuals with maximally marketable Limit ROD orders.

        TWSE/TPEx closing call auction does not accept market orders.  A long
        liquidation is therefore priced at the legal lower limit and a short
        cover at the legal upper limit.  The simulation still waits for the
        13:30 auction result and never treats the indicative book as a fill.
        """

        session_date = str(mode.get("session_date") or now.date().isoformat())
        if _timestamp_is_for_session(
            mode.get("closing_auction_submitted_at"), session_date
        ):
            return
        mode["closing_auction_submitted_at"] = now.isoformat(timespec="seconds")
        submitted = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            side = str(position.get("side"))
            limit_price = _finite(
                position.get("lower_limit" if side == "long" else "upper_limit")
            )
            position["eod_limit_order_status"] = "cancelled_replaced_at_13_25"
            if limit_price is None:
                position["closing_auction_order_status"] = (
                    "not_submitted_price_limit_unavailable"
                )
                continue
            position["closing_auction_limit_price"] = limit_price
            position["closing_auction_order_status"] = "working"
            submitted += 1
            self._order(
                {
                    "recorded_at": now.isoformat(timespec="seconds"),
                    "session_date": mode.get("session_date"),
                    "market": market,
                    "position_id": position.get("position_id"),
                    "symbol": position.get("symbol"),
                    "order_id": f"{position.get('position_id')}:closing_auction",
                    "purpose": "13_25_closing_auction_force_exit",
                    "side": "sell" if side == "long" else "buy_to_cover",
                    "order_type": "LMT_ROD",
                    "price": limit_price,
                    "quantity": abs(signed),
                    "status": "working",
                    "simulation_only": True,
                    "pricing_rule": (
                        "lower_limit_for_sell_upper_limit_for_buy_during_call_auction"
                    ),
                }
            )
        self._event(
            "closing_auction_limits_submitted",
            recorded_at=now,
            market=market,
            submitted=submitted,
        )

    def _settle_closing_auction(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        session_date = str(mode.get("session_date") or now.date().isoformat())
        if _timestamp_is_for_session(
            mode.get("closing_auction_settled_at"), session_date
        ):
            return
        mode["closing_auction_settled_at"] = now.isoformat(timespec="seconds")
        for position in (mode.get("positions") or {}).values():
            if int(position.get("signed_shares") or 0) == 0:
                continue
            if position.get("closing_auction_order_status") != "working":
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            close_price = _finite(quote.get("last"))
            capacity = _minute_kbar_capacity_shares(
                quote,
                lot_size=int(position.get("lot_size") or 1_000),
            )
            if close_price is None or capacity <= 0:
                position["closing_auction_order_status"] = (
                    "unfilled_no_close_or_minute_volume"
                )
                continue
            self._close_position(
                position,
                mode,
                price=close_price,
                quote=quote,
                now=now,
                reason="13_30_closing_auction_fill",
                order_type="LMT_ROD",
                quantity=capacity,
            )
            if int(position.get("signed_shares") or 0) == 0:
                position["closing_auction_order_status"] = "filled"
            else:
                position["closing_auction_order_status"] = "part_filled"
        self._event("closing_auction_settled", recorded_at=now, market=market)

    def _convert_residual_to_carry(
        self,
        market: str,
        mode: dict[str, Any],
        quotes: Mapping[str, Mapping[str, Any]],
        now: datetime,
    ) -> None:
        """Close every residual in the simulation's terminal ledger pass.

        The executable auction path remains separately audited above.  This
        final simulation-only pass enforces the product contract requested by
        the operator: a day-trade mode cannot carry an open position past the
        session close.  Residuals are valued at the observed close when
        available, otherwise at a conservative adverse legal limit, the last
        complete mark, or finally the entry price.  These rows are explicitly
        labelled as terminal ledger settlement rather than exchange fills.
        """

        session_date = str(mode.get("session_date") or now.date().isoformat())
        if _timestamp_is_for_session(
            mode.get("residual_conversion_completed_at"), session_date
        ):
            return
        flattened = 0
        degraded = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            quote = quotes.get(str(position.get("symbol"))) or {}
            close_price = _finite(quote.get("last"))
            side = str(position.get("side"))
            price_source = "observed_session_close"
            if close_price is None:
                close_price = _finite(
                    position.get("lower_limit" if side == "long" else "upper_limit")
                )
                price_source = "adverse_daily_limit_fallback"
                degraded += 1
            if close_price is None:
                close_price = _finite(position.get("last_mark_price"))
                price_source = "last_complete_mark_fallback"
            if close_price is None:
                close_price = _finite(position.get("entry_price"))
                price_source = "entry_price_last_resort"
            if close_price is None:
                raise RuntimeError(
                    "terminal day-trade flatten has no positive ledger price "
                    f"for {market}:{position.get('symbol')}"
                )
            position["terminal_flatten_price_source"] = price_source
            position["terminal_flatten_simulation_only"] = True
            self._close_position(
                position,
                mode,
                price=close_price,
                quote=quote,
                now=now,
                reason="13_30_terminal_ledger_flatten",
                order_type="SIM_TERMINAL",
                quantity=abs(signed),
            )
            position["closing_auction_order_status"] = "terminal_ledger_flattened"
            flattened += 1
        mode["residual_conversion_completed_at"] = now.isoformat(timespec="seconds")
        mode["terminal_flatten_count"] = flattened
        mode["terminal_flatten_degraded_count"] = degraded
        mode["force_exit_failures"] = 0
        self._event(
            "residual_positions_terminal_flattened",
            recorded_at=now,
            market=market,
            flattened=flattened,
            degraded=degraded,
            simulation_only=True,
        )

    def _close_position(
        self,
        position: dict[str, Any],
        mode: dict[str, Any],
        *,
        price: float,
        quote: Mapping[str, Any],
        now: datetime,
        reason: str,
        order_type: str,
        quantity: int,
    ) -> None:
        signed = int(position.get("signed_shares") or 0)
        if signed == 0:
            return
        remaining_before = abs(signed)
        fill_quantity = min(max(int(quantity), 0), remaining_before)
        if fill_quantity <= 0:
            return
        side = str(position.get("side"))
        direction = 1 if signed > 0 else -1
        exit_rate = float(
            position["sell_fee_rate"] if side == "long" else position["buy_fee_rate"]
        )
        rebate_rate = float(position.get("commission_rebate_rate") or 0.0)
        exit_gross_fee = fill_quantity * float(price) * exit_rate
        exit_rebate = fill_quantity * float(price) * rebate_rate
        exit_fee = exit_gross_fee - exit_rebate
        gross_pnl = (
            direction * fill_quantity * (float(price) - float(position["entry_price"]))
        )
        remaining_entry_fee = float(
            position.get("remaining_entry_fee_twd", position.get("entry_fee_twd", 0.0))
            or 0.0
        )
        entry_fee_allocated = (
            remaining_entry_fee
            if fill_quantity == remaining_before
            else remaining_entry_fee * fill_quantity / remaining_before
        )
        net_pnl = gross_pnl - entry_fee_allocated - exit_fee
        remaining_after = remaining_before - fill_quantity
        realized_gross = (
            float(position.get("realized_gross_pnl_twd") or 0.0) + gross_pnl
        )
        realized_exit_fee = (
            float(position.get("realized_exit_fee_twd") or 0.0) + exit_fee
        )
        realized_net = float(position.get("realized_net_pnl_twd") or 0.0) + net_pnl
        fully_closed = remaining_after == 0
        position.update(
            {
                "signed_shares": direction * remaining_after,
                "status": "closed" if fully_closed else "partially_closed",
                "last_exit_at": now.isoformat(timespec="seconds"),
                "last_exit_quote_at": quote.get("quote_at"),
                "last_exit_price": float(price),
                "last_exit_quantity": fill_quantity,
                "remaining_entry_fee_twd": remaining_entry_fee - entry_fee_allocated,
                "exit_fee_twd": realized_exit_fee,
                "gross_pnl_twd": realized_gross,
                "net_pnl_twd": realized_net,
                "realized_gross_pnl_twd": realized_gross,
                "realized_exit_fee_twd": realized_exit_fee,
                "realized_net_pnl_twd": realized_net,
                "exit_reason": reason,
                "last_complete_net_pnl_twd": 0.0
                if fully_closed
                else position.get("last_complete_net_pnl_twd", 0.0),
                "valuation_stale": False,
            }
        )
        if fully_closed:
            position.update(
                {
                    "exit_at": now.isoformat(timespec="seconds"),
                    "exit_quote_at": quote.get("quote_at"),
                    "exit_price": float(price),
                    "take_profit_order_status": "filled"
                    if reason.startswith("take_profit")
                    else "cancelled_oco",
                    "stop_order_status": "filled"
                    if reason.startswith("stop_loss")
                    else "cancelled_oco",
                    "eod_limit_order_status": "filled"
                    if reason == "13_20_limit_filled"
                    else "cancelled_oco",
                    "closing_auction_order_status": "filled"
                    if reason == "13_30_closing_auction_fill"
                    else position.get("closing_auction_order_status"),
                    # A flat position has no remaining liquidation component.
                    # Keep the cached total aligned with the final realized
                    # result instead of leaving the preceding minute mark.
                    "total_net_pnl_twd": realized_net,
                }
            )
        elif reason.startswith("take_profit"):
            position["take_profit_order_status"] = "part_filled"
        elif reason.startswith("stop_loss"):
            position["stop_order_status"] = "triggered_part_filled"
            position["status"] = "stop_triggered_partially_filled"
        elif reason == "13_20_limit_filled":
            position["eod_limit_order_status"] = "part_filled"
        elif reason == "13_24_market_force_exit":
            position["eod_limit_order_status"] = "cancelled_at_13_24"
            position["status"] = "force_exit_partially_filled"
        elif reason == "13_30_closing_auction_fill":
            position["closing_auction_order_status"] = "part_filled"
            position["status"] = "closing_auction_partially_filled"
        mode["cumulative_realized_net_pnl_twd"] = (
            float(mode.get("cumulative_realized_net_pnl_twd") or 0.0) + net_pnl
        )
        mode["cumulative_commission_rebate_accrued_twd"] = (
            float(mode.get("cumulative_commission_rebate_accrued_twd") or 0.0)
            + exit_rebate
        )
        order_id = f"{position.get('position_id')}:{reason}:{remaining_before}"
        common = {
            "recorded_at": now.isoformat(timespec="seconds"),
            "session_date": mode.get("session_date"),
            "market": position.get("market"),
            "position_id": position.get("position_id"),
            "symbol": position.get("symbol"),
            "order_id": order_id,
            "purpose": reason,
            "quantity": fill_quantity,
            "requested_quantity": remaining_before,
            "remaining_quantity": remaining_after,
            "simulation_only": True,
            "synthetic_terminal_ledger": (
                reason == "13_30_terminal_ledger_flatten"
            ),
        }
        self._order(
            {
                **common,
                "side": "sell" if side == "long" else "buy_to_cover",
                "order_type": order_type,
                "price": None if order_type == "MKT" else float(price),
                "status": "filled" if fully_closed else "part_filled",
            }
        )
        self._fill(
            {
                **common,
                "fill_at": now.isoformat(timespec="seconds"),
                "quote_at": quote.get("quote_at"),
                "price": float(price),
                "fee_and_tax_twd": exit_fee,
                "gross_fee_and_tax_twd": exit_gross_fee,
                "commission_rebate_accrued_twd": exit_rebate,
                "entry_fee_allocated_twd": entry_fee_allocated,
                "gross_pnl_twd": gross_pnl,
                "net_pnl_twd": net_pnl,
                "fill_contract": (
                    "simulation_terminal_ledger_not_exchange_fill"
                    if reason == "13_30_terminal_ledger_flatten"
                    else "best_bid_for_sell_best_ask_for_buy"
                ),
                "depth_assumption": (
                    "full_residual_ledger_close_ignores_displayed_depth"
                    if reason == "13_30_terminal_ledger_flatten"
                    else "minimum_of_level_one_and_50pct_minute_volume_except_closing_auction_which_uses_50pct_auction_minute_volume"
                ),
            }
        )

    def _mark_mode(
        self,
        market: str,
        now: datetime,
        quotes: Mapping[str, Mapping[str, Any]],
        *,
        append_history: bool = True,
    ) -> None:
        mode = self.state.get("modes", {}).get(market)
        if not isinstance(mode, dict):
            return
        open_net = 0.0
        stale_count = 0
        open_count = 0
        for position in (mode.get("positions") or {}).values():
            signed = int(position.get("signed_shares") or 0)
            if signed == 0:
                continue
            open_count += 1
            quote = quotes.get(str(position.get("symbol"))) or {}
            side = str(position.get("side"))
            liquidation = _finite(quote.get("bid" if side == "long" else "ask"))
            if liquidation is None:
                stale_count += 1
                position["valuation_stale"] = True
                open_net += float(position.get("last_complete_net_pnl_twd") or 0.0)
                continue
            exit_rate = float(
                position["sell_fee_rate"]
                if side == "long"
                else position["buy_fee_rate"]
            )
            rebate_rate = float(position.get("commission_rebate_rate") or 0.0)
            exit_fee = abs(signed) * liquidation * (exit_rate - rebate_rate)
            net_pnl = (
                signed * (liquidation - float(position["entry_price"]))
                - float(
                    position.get(
                        "remaining_entry_fee_twd",
                        position.get("entry_fee_twd", 0.0),
                    )
                )
                - exit_fee
            )
            position["last_mark_at"] = now.isoformat(timespec="seconds")
            position["last_quote_at"] = quote.get("quote_at")
            position["last_mark_price"] = liquidation
            position["last_complete_net_pnl_twd"] = net_pnl
            position["total_net_pnl_twd"] = (
                float(position.get("realized_net_pnl_twd") or 0.0) + net_pnl
            )
            position["valuation_stale"] = False
            open_net += net_pnl
        cumulative = float(mode.get("cumulative_realized_net_pnl_twd") or 0.0)
        total_equity = (
            float(mode.get("initial_capital_twd") or 0.0) + cumulative + open_net
        )
        flat_status = "flat"
        if str(mode.get("session_date") or "") == now.date().isoformat() and mode.get(
            "entry_completed_at"
        ):
            if bool((mode.get("execution_projection") or {}).get("collapsed_to_flat")):
                flat_status = "flat_directional_mix_unexecutable"
            elif mode.get("positions"):
                flat_status = "session_flat_after_exit"
            else:
                flat_status = "flat_no_executable_signal"
        mode.update(
            {
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "last_mark_at": now.isoformat(timespec="seconds"),
                "valuation_stale": stale_count > 0,
                "engine_status": (
                    "critical_residual_carried_after_13_30"
                    if open_count
                    and any(
                        position.get("carry_type")
                        for position in (mode.get("positions") or {}).values()
                        if int(position.get("signed_shares") or 0) != 0
                    )
                    else "critical_unflattened_after_13_24"
                    if now.timetz().replace(tzinfo=None) >= FORCE_EXIT_TIME and open_count
                    else "active"
                    if open_count
                    else flat_status
                ),
            }
        )
        if not append_history:
            return
        _append_jsonl(
            self.marks_path,
            {
                "recorded_at": now.isoformat(timespec="seconds"),
                "minute": now.replace(second=0, microsecond=0).isoformat(
                    timespec="minutes"
                ),
                "session_date": mode.get("session_date") or now.date().isoformat(),
                "market": market,
                "initial_capital_twd": mode.get("initial_capital_twd"),
                "cumulative_realized_net_pnl_twd": cumulative,
                "open_net_liquidation_pnl_twd": open_net,
                "total_equity_twd": total_equity,
                "open_position_count": open_count,
                "stale_position_count": stale_count,
                "valuation_stale": stale_count > 0,
            },
        )

    def _persist(self, now: datetime | None = None) -> None:
        observed = _now_taipei(now)
        self.state["updated_at"] = observed.isoformat(timespec="seconds")
        _atomic_json(self.state_path, self.state)
        mode_rows = list(self.state.get("modes", {}).values())
        _atomic_json(
            self.positions_path,
            {
                "schema_version": 1,
                "generated_at": observed.isoformat(timespec="seconds"),
                "simulation_only": True,
                "production_order_possible": False,
                "position_contract": {
                    "target": (
                        "complete model targets are stored at target_weights_path "
                        "and target_positions_path"
                    ),
                    "executed": (
                        "positions contains paper-simulation fills; an empty list "
                        "is an explicit flat position"
                    ),
                },
                "modes": {
                    str(item.get("market")): {
                        "market": item.get("market"),
                        "label": item.get("label"),
                        "session_date": item.get("session_date"),
                        "signal_id": item.get("signal_id"),
                        "signal_at": item.get("signal_at"),
                        "engine_status": item.get("engine_status"),
                        "target_weights_path": item.get("target_weights_path"),
                        "target_positions_path": item.get("target_positions_path"),
                        "target_symbol_count": item.get("target_symbol_count"),
                        "target_risk": item.get("target_risk") or {},
                        "open_position_count": int(item.get("open_position_count") or 0),
                        "positions": [
                            dict(position)
                            for position in (item.get("positions") or {}).values()
                            if isinstance(position, Mapping)
                            and int(position.get("signed_shares") or 0) != 0
                        ],
                    }
                    for item in mode_rows
                },
            },
        )
        critical = any(
            str(item.get("engine_status") or "").startswith("critical")
            for item in mode_rows
        )
        blocked = any(
            str(item.get("engine_status") or "").startswith("blocked")
            for item in mode_rows
        )
        active = any(item.get("engine_status") == "active" for item in mode_rows)
        health = (
            "critical"
            if critical
            else "active"
            if active
            else "blocked"
            if blocked
            else "waiting"
        )
        _atomic_json(
            self.status_path,
            {
                "schema_version": SIMULATION_SCHEMA_VERSION,
                "updated_at": observed.isoformat(timespec="seconds"),
                "health": health,
                "simulation_only": True,
                "production_order_possible": False,
                "schedule": {
                    "signal_gate": "09:00",
                    "entry": "size_at_official_open_then_execute_at_a_causally_later_best_ask_bid",
                    "liquidity": "min(level_one_depth, 50pct_completed_minute_kbar_volume)",
                    "take_profit": "mode-specific daily-limit LMT: full limit or configured ticks inside",
                    "stop_loss": "mode-specific local trigger: full limit or configured ticks inside, then market",
                    "fill_guarantee": False,
                    "fill_caveat": "inside-limit prices improve fill probability but cannot guarantee a fill without executable counterparty volume",
                    "exit_limit": "13:20 passive top-of-book limit",
                    "continuous_force_exit": "13:24<=t<13:25 market retry every service poll while residual exists",
                    "closing_auction": "13:25 long sell at lower limit and short cover at upper limit using LMT_ROD; settle at 13:30 call auction",
                    "residual": "13:30 simulation-only terminal ledger flatten; no overnight position",
                    "stress_rates": {
                        "financing_annual_rate": MARGIN_FINANCING_ANNUAL_RATE,
                        "shortfall_borrow_fee_rate_per_day": DAY_TRADE_SHORTFALL_BORROW_FEE_RATE,
                        "shortfall_handling_fee_fraction": DAY_TRADE_SHORTFALL_HANDLING_FEE_FRACTION,
                    },
                    "decision_and_mark_interval_seconds": 60,
                },
                "mode_count": len(mode_rows),
                "modes": {
                    str(item.get("market")): {
                        key: item.get(key)
                        for key in (
                            "market",
                            "label",
                            "signal_market",
                            "price_limit_offset_ticks",
                            "bracket_price_policy",
                            "fill_guaranteed",
                            "engine_status",
                            "checkpoint_ready",
                            "readiness_error",
                            "session_date",
                            "signal_id",
                            "signal_at",
                            "target_weights_path",
                            "target_positions_path",
                            "executed_positions_path",
                            "target_symbol_count",
                            "target_risk",
                            "initial_capital_twd",
                            "total_equity_twd",
                            "cumulative_realized_net_pnl_twd",
                            "open_net_liquidation_pnl_twd",
                            "open_position_count",
                            "stale_position_count",
                            "force_exit_failures",
                            "terminal_flatten_count",
                            "terminal_flatten_degraded_count",
                            "cumulative_carry_cost_twd",
                        )
                    }
                    for item in mode_rows
                },
            },
        )


__all__ = [
    "CLOSING_AUCTION_TIME",
    "ENTRY_GATE",
    "EXIT_LIMIT_TIME",
    "FIRST_MINUTE_EXECUTION_TIME",
    "FORCE_EXIT_TIME",
    "LiveEligibility",
    "ModeSpec",
    "TwDayTradeSimulationEngine",
    "load_live_eligibility",
    "load_symbol_metadata",
    "quote_map_from_snapshot",
]
