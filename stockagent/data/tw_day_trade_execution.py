"""Historical minute-K execution tapes for the daily day-trade model.

The model still makes exactly one decision per exchange session.  This module
builds either a compressed event tape or the full right-labelled minute path.
Both are execution labels and must never be appended to model features.

Before the first canonical minute-K partition, a compressed tape can carry an explicitly
labelled daily-bar proxy.  It is deliberately adverse on both legs: long trades
buy one dated legal tick above open and sell one tick below close; short trades
sell one tick below open and buy one tick above close.  Missing minute
partitions on or after the canonical minute-data start remain fail-closed and
are never silently replaced by this proxy.
"""

from __future__ import annotations

from enum import IntEnum
from pathlib import Path
import hashlib
import json
import os
from typing import Final

import numpy as np

from stockagent.data.tw_price_rules import move_price_ticks_numpy

try:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - validated by the public loader
    pc = None
    pq = None


DAY_TRADE_MINUTE_EXECUTION_CONTRACT_VERSION = 6
# A regular session is represented by minute labels 0..270 in the canonical
# archive.  Without historical minute bars, allocating the full daily volume
# to one synthetic bar would violate the user's 50%-of-minute-K capacity rule
# by orders of magnitude.  The pre-history proxy therefore uses the uniform
# no-lookahead estimate daily_volume / 271 as each entry/exit bar's volume;
# the executor subsequently applies its ordinary 50% whole-lot cap.
DAILY_PROXY_SESSION_MINUTE_BARS = 271.0
DAY_TRADE_MINUTE_SOURCE_SCHEMA_VERSION = 4
DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED: Final[str] = "scheduled_events_50pct"
DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME: Final[str] = (
    "full_session_volume_100pct"
)
DAY_TRADE_MINUTE_EXECUTION_POLICIES: Final[tuple[str, ...]] = (
    DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
    DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
)

# Full-session tensors use minute 0 for the official opening price used only
# for sizing.  Right-labelled executable bars occupy minutes 1..270.  The last
# axis is [VWAP_OR_AUCTION_PRICE, VOLUME_SHARES].
DAY_TRADE_FULL_SESSION_MINUTES = 271
DAY_TRADE_FULL_SESSION_FIELDS = 2
FULL_SESSION_PRICE = 0
FULL_SESSION_VOLUME_SHARES = 1


def normalize_day_trade_minute_execution_policy(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(
            "day_trade_minute_execution_policy must be one of "
            f"{DAY_TRADE_MINUTE_EXECUTION_POLICIES}"
        )
    normalized = "_".join(value.strip().casefold().replace("-", "_").split())
    aliases = {
        "scheduled": DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
        "scheduled_events": DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
        "scheduled_events_50pct": DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
        "legacy": DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
        "full_session": DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
        "full_session_volume": DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
        "full_session_volume_100pct": DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
        "minute_volume_100pct": DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME,
    }
    result = aliases.get(normalized)
    if result is None:
        raise ValueError(
            "day_trade_minute_execution_policy must be one of "
            f"{DAY_TRADE_MINUTE_EXECUTION_POLICIES}"
        )
    return result


class DayTradeExecutionField(IntEnum):
    OFFICIAL_OPEN = 0
    ENTRY_VWAP_0901 = 1
    ENTRY_VOLUME_0901 = 2
    LIMIT_PRICE_1320 = 3
    HIGH_1321 = 4
    LOW_1321 = 5
    VOLUME_1321 = 6
    HIGH_1322 = 7
    LOW_1322 = 8
    VOLUME_1322 = 9
    HIGH_1323 = 10
    LOW_1323 = 11
    VOLUME_1323 = 12
    MARKET_VWAP_1324 = 13
    MARKET_VOLUME_1324 = 14
    AUCTION_PRICE_1330 = 15
    AUCTION_VOLUME_1330 = 16
    DAILY_PROXY_LONG_ENTRY_PRICE = 17
    DAILY_PROXY_SHORT_ENTRY_PRICE = 18
    DAILY_PROXY_LONG_EXIT_PRICE = 19
    DAILY_PROXY_SHORT_EXIT_PRICE = 20
    DAILY_PROXY_VOLUME = 21
    DAILY_PROXY_FLAG = 22
    # Official daily close is a valuation source for a residual position when
    # the minute archive has no 13:30 auction row.  It is not an exchange fill:
    # the executor still classifies the residual as margin financing/borrowing
    # and charges the normal close-side fee plus the configured highest carry
    # costs.  Keeping this field separate from AUCTION_PRICE_1330 prevents a
    # daily close from being misreported as observed auction liquidity.
    OFFICIAL_CLOSE = 23
    MARKET_VWAP_1325 = 24
    MARKET_VOLUME_1325 = 25


DAY_TRADE_EXECUTION_FIELD_COUNT = len(DayTradeExecutionField)
_EVENT_MINUTES = (1, 260, 261, 262, 263, 264, 265, 270)
# The executor can consume at most half of a one-minute bar and Taiwan cash
# equities trade in 1,000-share board lots.  A smaller entry bar can never
# produce a legal forward fill, regardless of model output.
MIN_EXECUTABLE_MINUTE_VOLUME_SHARES = 2_000.0


def _vwap(amount: float, volume_shares: float, fallback: float) -> float:
    if np.isfinite(amount) and np.isfinite(volume_shares) and volume_shares > 0.0:
        value = amount / volume_shares
        if np.isfinite(value) and value > 0.0:
            return float(value)
    return float(fallback) if np.isfinite(fallback) and fallback > 0.0 else np.nan


def tw_day_trade_minute_round_trip_masks(
    tape: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Return cells supporting at least one exact long/short board-lot exit."""

    if tape.ndim != 3 or tape.shape[2] != DAY_TRADE_EXECUTION_FIELD_COUNT:
        raise ValueError(
            "minute execution tape must have shape [T,S,"
            f"{DAY_TRADE_EXECUTION_FIELD_COUNT}]"
        )
    official_open = tape[:, :, DayTradeExecutionField.OFFICIAL_OPEN]
    proxy = tape[:, :, DayTradeExecutionField.DAILY_PROXY_FLAG] > 0.5
    entry_price = tape[:, :, DayTradeExecutionField.ENTRY_VWAP_0901]
    entry_volume = tape[:, :, DayTradeExecutionField.ENTRY_VOLUME_0901]
    valid_minute_entry = (
        ~proxy
        & np.isfinite(official_open)
        & (official_open > 0.0)
        & np.isfinite(entry_price)
        & (entry_price > 0.0)
        & np.isfinite(entry_volume)
        & (entry_volume >= MIN_EXECUTABLE_MINUTE_VOLUME_SHARES)
    )
    limit_price = tape[:, :, DayTradeExecutionField.LIMIT_PRICE_1320]
    valid_limit = np.isfinite(limit_price) & (limit_price > 0.0)
    long_limit_exit = np.zeros_like(valid_minute_entry)
    short_limit_exit = np.zeros_like(valid_minute_entry)
    for high_field in (
        DayTradeExecutionField.HIGH_1321,
        DayTradeExecutionField.HIGH_1322,
        DayTradeExecutionField.HIGH_1323,
    ):
        high = tape[:, :, high_field]
        low = tape[:, :, int(high_field) + 1]
        volume = tape[:, :, int(high_field) + 2]
        capacity = (
            np.isfinite(volume)
            & (volume >= MIN_EXECUTABLE_MINUTE_VOLUME_SHARES)
        )
        long_limit_exit |= (
            valid_limit & np.isfinite(high) & (high > limit_price) & capacity
        )
        short_limit_exit |= (
            valid_limit & np.isfinite(low) & (low < limit_price) & capacity
        )
    market_exit = np.zeros_like(valid_minute_entry)
    for price_field, volume_field in (
        (
            DayTradeExecutionField.MARKET_VWAP_1324,
            DayTradeExecutionField.MARKET_VOLUME_1324,
        ),
        (
            DayTradeExecutionField.MARKET_VWAP_1325,
            DayTradeExecutionField.MARKET_VOLUME_1325,
        ),
    ):
        market_price = tape[:, :, price_field]
        market_volume = tape[:, :, volume_field]
        market_exit |= (
            np.isfinite(market_price)
            & (market_price > 0.0)
            & np.isfinite(market_volume)
            & (market_volume >= MIN_EXECUTABLE_MINUTE_VOLUME_SHARES)
        )
    auction_close = tape[:, :, DayTradeExecutionField.AUCTION_PRICE_1330]
    official_close = tape[:, :, DayTradeExecutionField.OFFICIAL_CLOSE]
    margin_close = (
        (np.isfinite(auction_close) & (auction_close > 0.0))
        | (np.isfinite(official_close) & (official_close > 0.0))
    )
    proxy_volume = tape[:, :, DayTradeExecutionField.DAILY_PROXY_VOLUME]
    valid_proxy_capacity = (
        np.isfinite(proxy_volume)
        & (proxy_volume >= MIN_EXECUTABLE_MINUTE_VOLUME_SHARES)
    )

    def valid_proxy_round_trip(entry_field: int, exit_field: int) -> np.ndarray:
        proxy_entry = tape[:, :, entry_field]
        proxy_exit = tape[:, :, exit_field]
        return (
            proxy
            & np.isfinite(official_open)
            & (official_open > 0.0)
            & np.isfinite(proxy_entry)
            & (proxy_entry > 0.0)
            & np.isfinite(proxy_exit)
            & (proxy_exit > 0.0)
            & valid_proxy_capacity
        )

    return (
        (
            valid_minute_entry
            & (long_limit_exit | market_exit | margin_close)
        )
        | valid_proxy_round_trip(
            DayTradeExecutionField.DAILY_PROXY_LONG_ENTRY_PRICE,
            DayTradeExecutionField.DAILY_PROXY_LONG_EXIT_PRICE,
        ),
        (
            valid_minute_entry
            & (short_limit_exit | market_exit | margin_close)
        )
        | valid_proxy_round_trip(
            DayTradeExecutionField.DAILY_PROXY_SHORT_ENTRY_PRICE,
            DayTradeExecutionField.DAILY_PROXY_SHORT_EXIT_PRICE,
        ),
    )


def _require_executable_tape_coverage(
    tape: np.ndarray,
    *,
    source: Path,
) -> None:
    """Reject a label tensor whose exact minute executor is identically flat."""

    long_round_trip, short_round_trip = tw_day_trade_minute_round_trip_masks(tape)
    if np.any(long_round_trip | short_round_trip):
        return
    raise ValueError(
        "daily hybrid execution tape has zero executable 1,000-share round "
        f"trips after 50% volume participation: source={source}. Refusing to "
        "train a constant zero loss with zero model gradients. Verify the "
        "canonical tw-minute-train materialization and its date/symbol overlap."
    )


def load_tw_day_trade_execution_tape(
    root: str | Path,
    *,
    panel_dates: np.ndarray,
    panel_symbols: list[str],
    official_open_prices: np.ndarray,
    official_close_prices: np.ndarray | None = None,
    daily_volume_shares: np.ndarray | None = None,
    cache_dir: str | Path | None = None,
    allow_daily_proxy: bool = True,
    policy: str = DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED,
) -> np.ndarray:
    """Align executor-only minute facts to the daily panel.

    ``scheduled_events_50pct`` emits the compressed event tape and can use a
    direction-specific adverse daily proxy before the first minute partition
    when ``allow_daily_proxy`` is true. Strict mode rejects those earlier rows.
    ``full_session_volume_100pct`` emits ``[T,S,271,2]`` with minute 0 holding
    only the official sizing open and minutes 1..270 holding right-labelled
    price/volume facts. Missing partitions, symbols, bars, prices, or volume
    remain fail-closed: prices are NaN and capacity is zero. ``Amount /
    volume_shares`` is the observable minute VWAP used for continuous-market
    historical execution; minute 270 uses the official close-auction price.
    """

    if pq is None or pc is None:
        raise RuntimeError("PyArrow is required for day-trade minute execution")
    root_path = Path(root)
    if not root_path.is_dir():
        raise FileNotFoundError(
            "daily minute-execution root does not exist or is not a directory: "
            f"{root_path}. Materialize the canonical tw-minute-train release "
            "before training."
        )
    partition_paths = tuple(root_path.glob("trade_date=*/data.parquet"))
    if not partition_paths:
        raise FileNotFoundError(
            "daily minute-execution root contains no trade_date=*/data.parquet "
            f"partitions: {root_path}"
        )
    minute_partition_dates: list[np.datetime64] = []
    for partition_path in partition_paths:
        day_text = partition_path.parent.name.removeprefix("trade_date=")
        try:
            minute_partition_dates.append(np.datetime64(day_text, "D"))
        except ValueError:
            continue
    if not minute_partition_dates:
        raise ValueError(
            "daily minute-execution root has no parseable ISO trade_date "
            f"partitions: {root_path}"
        )
    first_minute_date = min(minute_partition_dates)

    dates = np.asarray(panel_dates, dtype="datetime64[D]").reshape(-1)
    opens = np.asarray(official_open_prices, dtype=np.float64)
    normalized_policy = normalize_day_trade_minute_execution_policy(policy)
    expected = (int(dates.size), len(panel_symbols))
    if opens.shape != expected:
        raise ValueError("official_open_prices must align with panel [T,S]")
    closes: np.ndarray | None = None
    daily_volumes: np.ndarray | None = None
    if normalized_policy == DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED:
        if official_close_prices is None or daily_volume_shares is None:
            raise ValueError(
                "scheduled minute execution requires official close prices "
                "and daily share volume"
            )
        closes = np.asarray(official_close_prices, dtype=np.float64)
        daily_volumes = np.asarray(daily_volume_shares, dtype=np.float64)
        if closes.shape != expected:
            raise ValueError("official_close_prices must align with panel [T,S]")
        if daily_volumes.shape != expected:
            raise ValueError("daily_volume_shares must align with panel [T,S]")
    cache_path: Path | None = None
    if cache_dir is not None:
        digest = hashlib.sha256()
        digest.update(
            f"contract={DAY_TRADE_MINUTE_EXECUTION_CONTRACT_VERSION}\0"
            f"policy={normalized_policy}\0"
            f"allow_daily_proxy={bool(allow_daily_proxy)}\0".encode("utf-8")
        )
        digest.update(str(root_path.resolve()).encode("utf-8"))
        manifest = root_path / "manifest.json"
        if manifest.is_file():
            digest.update(manifest.read_bytes())
        digest.update(dates.astype("datetime64[D]").astype(np.int64).tobytes())
        digest.update("\0".join(map(str, panel_symbols)).encode("utf-8"))
        digest.update(np.ascontiguousarray(opens, dtype=np.float32).tobytes())
        if closes is not None and daily_volumes is not None:
            digest.update(np.ascontiguousarray(closes, dtype=np.float32).tobytes())
            digest.update(
                np.ascontiguousarray(daily_volumes, dtype=np.float32).tobytes()
            )
        resolved_cache = Path(cache_dir)
        resolved_cache.mkdir(parents=True, exist_ok=True)
        cache_path = resolved_cache / f"tape-{digest.hexdigest()}.npy"
        if cache_path.is_file():
            cached = np.load(cache_path, allow_pickle=False, mmap_mode="c")
            expected_shape = (
                (*expected, DAY_TRADE_EXECUTION_FIELD_COUNT)
                if normalized_policy
                == DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED
                else (
                    *expected,
                    DAY_TRADE_FULL_SESSION_MINUTES,
                    DAY_TRADE_FULL_SESSION_FIELDS,
                )
            )
            if cached.shape != expected_shape or cached.dtype != np.dtype(np.float32):
                raise RuntimeError(f"invalid cached day-trade execution tape: {cache_path}")
            if normalized_policy == DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED:
                cached = np.asarray(cached, dtype=np.float32)
                if not allow_daily_proxy and bool(
                    np.any(
                        cached[:, :, DayTradeExecutionField.DAILY_PROXY_FLAG] > 0.5
                    )
                ):
                    raise RuntimeError(
                        "strict minute execution cache contains daily proxy rows: "
                        f"{cache_path}"
                    )
                _require_executable_tape_coverage(cached, source=cache_path)
            return cached
    if normalized_policy == DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME:
        return _load_full_session_volume_tape(
            root=root,
            dates=dates,
            panel_symbols=panel_symbols,
            opens=opens,
            cache_path=cache_path,
        )
    assert closes is not None and daily_volumes is not None
    tape = np.full((*expected, DAY_TRADE_EXECUTION_FIELD_COUNT), np.nan, dtype=np.float32)
    tape[:, :, DayTradeExecutionField.ENTRY_VOLUME_0901] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1321] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1322] = 0.0
    tape[:, :, DayTradeExecutionField.VOLUME_1323] = 0.0
    tape[:, :, DayTradeExecutionField.MARKET_VOLUME_1324] = 0.0
    tape[:, :, DayTradeExecutionField.MARKET_VOLUME_1325] = 0.0
    tape[:, :, DayTradeExecutionField.AUCTION_VOLUME_1330] = 0.0
    tape[:, :, DayTradeExecutionField.DAILY_PROXY_VOLUME] = 0.0
    tape[:, :, DayTradeExecutionField.DAILY_PROXY_FLAG] = 0.0
    tape[:, :, DayTradeExecutionField.OFFICIAL_OPEN] = opens.astype(np.float32)
    tape[:, :, DayTradeExecutionField.OFFICIAL_CLOSE] = closes.astype(np.float32)

    # This is a labelled research proxy, not fabricated minute data.  It is
    # allowed only before the first canonical minute partition.  The dated
    # tick mover handles bucket-boundary asymmetry (for example 100 -> 100.5
    # upward but 100 -> 99.9 downward) and historical rule versions.
    proxy_rows = dates < first_minute_date
    if np.any(proxy_rows) and not allow_daily_proxy:
        first_requested = np.datetime_as_string(dates[proxy_rows][0], unit="D")
        first_exact = np.datetime_as_string(first_minute_date, unit="D")
        raise ValueError(
            "strict minute execution forbids the daily-bar proxy: "
            f"first_requested_date={first_requested} predates "
            f"first_minute_partition={first_exact}. Set panel_start_date on or "
            "after the first canonical minute partition."
        )
    if np.any(proxy_rows):
        proxy_opens = opens[proxy_rows]
        proxy_closes = closes[proxy_rows]
        proxy_volumes = daily_volumes[proxy_rows]
        proxy_shape = proxy_opens.shape
        proxy_date_grid = np.broadcast_to(dates[proxy_rows, None], proxy_shape)
        valid_proxy_prices = (
            np.isfinite(proxy_opens)
            & (proxy_opens > 0.0)
            & np.isfinite(proxy_closes)
            & (proxy_closes > 0.0)
        )
        for field, values in (
            (
                DayTradeExecutionField.DAILY_PROXY_LONG_ENTRY_PRICE,
                move_price_ticks_numpy(proxy_opens, 1, proxy_date_grid),
            ),
            (
                DayTradeExecutionField.DAILY_PROXY_SHORT_ENTRY_PRICE,
                move_price_ticks_numpy(proxy_opens, -1, proxy_date_grid),
            ),
            (
                DayTradeExecutionField.DAILY_PROXY_LONG_EXIT_PRICE,
                move_price_ticks_numpy(proxy_closes, -1, proxy_date_grid),
            ),
            (
                DayTradeExecutionField.DAILY_PROXY_SHORT_EXIT_PRICE,
                move_price_ticks_numpy(proxy_closes, 1, proxy_date_grid),
            ),
        ):
            tape[proxy_rows, :, field] = np.where(
                valid_proxy_prices, values, np.nan
            ).astype(np.float32)
        clean_daily_volume = np.where(
            np.isfinite(proxy_volumes) & (proxy_volumes > 0.0),
            proxy_volumes,
            0.0,
        )
        tape[proxy_rows, :, DayTradeExecutionField.DAILY_PROXY_VOLUME] = (
            np.where(
                valid_proxy_prices,
                clean_daily_volume / DAILY_PROXY_SESSION_MINUTE_BARS,
                0.0,
            ).astype(np.float32)
        )
        tape[proxy_rows, :, DayTradeExecutionField.DAILY_PROXY_FLAG] = (
            valid_proxy_prices.astype(np.float32)
        )
    symbol_index = {str(symbol): idx for idx, symbol in enumerate(panel_symbols)}
    columns = [
        "symbol", "minutes_from_open", "High", "Low", "Close", "Amount",
        "volume_shares",
    ]
    for date_idx, day in enumerate(dates):
        day_text = np.datetime_as_string(day, unit="D")
        path = root_path / f"trade_date={day_text}" / "data.parquet"
        if not path.is_file():
            continue
        table = pq.read_table(
            path,
            columns=columns,
            filters=[("minutes_from_open", "in", list(_EVENT_MINUTES))],
        )
        payload = table.to_pydict()
        for row in range(table.num_rows):
            sym_idx = symbol_index.get(str(payload["symbol"][row]))
            if sym_idx is None:
                continue
            minute = int(payload["minutes_from_open"][row])
            high = float(payload["High"][row])
            low = float(payload["Low"][row])
            close = float(payload["Close"][row])
            volume = float(payload["volume_shares"][row])
            amount = float(payload["Amount"][row])
            if minute == 1:
                tape[date_idx, sym_idx, DayTradeExecutionField.ENTRY_VWAP_0901] = _vwap(amount, volume, close)
                tape[date_idx, sym_idx, DayTradeExecutionField.ENTRY_VOLUME_0901] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 260:
                tape[date_idx, sym_idx, DayTradeExecutionField.LIMIT_PRICE_1320] = close
            elif minute in (261, 262, 263):
                base = {
                    261: DayTradeExecutionField.HIGH_1321,
                    262: DayTradeExecutionField.HIGH_1322,
                    263: DayTradeExecutionField.HIGH_1323,
                }[minute]
                tape[date_idx, sym_idx, base] = high
                tape[date_idx, sym_idx, int(base) + 1] = low
                tape[date_idx, sym_idx, int(base) + 2] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 264:
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VWAP_1324] = _vwap(amount, volume, close)
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VOLUME_1324] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 265:
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VWAP_1325] = _vwap(amount, volume, close)
                tape[date_idx, sym_idx, DayTradeExecutionField.MARKET_VOLUME_1325] = max(volume, 0.0) if np.isfinite(volume) else 0.0
            elif minute == 270:
                tape[date_idx, sym_idx, DayTradeExecutionField.AUCTION_PRICE_1330] = close
                tape[date_idx, sym_idx, DayTradeExecutionField.AUCTION_VOLUME_1330] = max(volume, 0.0) if np.isfinite(volume) else 0.0
    _require_executable_tape_coverage(tape, source=root_path)
    if cache_path is not None:
        temporary = cache_path.with_name(
            f".{cache_path.name}.{os.getpid()}.tmp"
        )
        with temporary.open("wb") as handle:
            np.save(handle, tape, allow_pickle=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, cache_path)
    return tape


def _load_full_session_volume_tape(
    *,
    root: str | Path,
    dates: np.ndarray,
    panel_symbols: list[str],
    opens: np.ndarray,
    cache_path: Path | None,
) -> np.ndarray:
    """Build the full right-labelled market-volume path without a RAM copy."""

    manifest_path = Path(root) / "manifest.json"
    if not manifest_path.is_file():
        raise RuntimeError(
            "full-session day-trade execution requires the receipt-backed "
            f"minute manifest: {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not (
        isinstance(manifest, dict)
        and manifest.get("schema_version")
        == DAY_TRADE_MINUTE_SOURCE_SCHEMA_VERSION
        and manifest.get("source") == "shioaji_kbars_1m"
        and manifest.get("research_ready") is True
        and manifest.get("status") == "research_ready"
        and isinstance(manifest.get("partitions"), list)
        and len(manifest["partitions"]) == len(manifest.get("dates", []))
    ):
        raise RuntimeError(
            "full-session day-trade execution requires a complete "
            "research_ready minute manifest"
        )

    shape = (
        int(dates.size),
        len(panel_symbols),
        DAY_TRADE_FULL_SESSION_MINUTES,
        DAY_TRADE_FULL_SESSION_FIELDS,
    )
    temporary: Path | None = None
    if cache_path is None:
        tape = np.full(shape, np.nan, dtype=np.float32)
    else:
        temporary = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.tmp")
        tape = np.lib.format.open_memmap(
            temporary,
            mode="w+",
            dtype=np.float32,
            shape=shape,
        )
        tape[:] = np.nan
    tape[:, :, :, FULL_SESSION_VOLUME_SHARES] = 0.0
    tape[:, :, 0, FULL_SESSION_PRICE] = opens.astype(np.float32)

    symbol_index = {str(symbol): idx for idx, symbol in enumerate(panel_symbols)}
    root_path = Path(root)
    columns = [
        "symbol",
        "minutes_from_open",
        "Close",
        "Amount",
        "volume_shares",
    ]
    for date_idx, day in enumerate(dates):
        day_text = np.datetime_as_string(day, unit="D")
        path = root_path / f"trade_date={day_text}" / "data.parquet"
        if not path.is_file():
            continue
        table = pq.read_table(path, columns=columns)
        payload = table.to_pydict()
        row_symbols = payload["symbol"]
        row_minutes = np.asarray(payload["minutes_from_open"], dtype=np.int16)
        row_close = np.asarray(payload["Close"], dtype=np.float64)
        row_amount = np.asarray(payload["Amount"], dtype=np.float64)
        row_volume = np.asarray(payload["volume_shares"], dtype=np.float64)
        row_symbol_indices = np.fromiter(
            (symbol_index.get(str(symbol), -1) for symbol in row_symbols),
            dtype=np.int64,
            count=len(row_symbols),
        )
        selected = (
            (row_symbol_indices >= 0)
            & (row_minutes >= 1)
            & (row_minutes < DAY_TRADE_FULL_SESSION_MINUTES)
        )
        if not bool(selected.any()):
            continue
        selected_indices = np.flatnonzero(selected)
        symbol_slots = row_symbol_indices[selected_indices]
        minute_slots = row_minutes[selected_indices].astype(np.int64, copy=False)
        volumes = row_volume[selected_indices]
        closes = row_close[selected_indices]
        amounts = row_amount[selected_indices]
        valid_volume = np.isfinite(volumes) & (volumes > 0.0)
        valid_vwap = valid_volume & np.isfinite(amounts) & (amounts > 0.0)
        prices = closes.copy()
        np.divide(amounts, volumes, out=prices, where=valid_vwap)
        # The 13:30 right-labelled row is the closing auction. Its official
        # close is the execution price; treating its Amount/Volume as an
        # ordinary continuous-market VWAP would obscure that boundary.
        prices = np.where(minute_slots == 270, closes, prices)
        valid_price = np.isfinite(prices) & (prices > 0.0)
        tape[date_idx, symbol_slots, minute_slots, FULL_SESSION_PRICE] = np.where(
            valid_price, prices, np.nan
        ).astype(np.float32)
        tape[
            date_idx,
            symbol_slots,
            minute_slots,
            FULL_SESSION_VOLUME_SHARES,
        ] = np.where(valid_volume, volumes, 0.0).astype(np.float32)

    if temporary is None or cache_path is None:
        return tape
    tape.flush()
    del tape
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, cache_path)
    return np.load(cache_path, allow_pickle=False, mmap_mode="c")


__all__ = [
    "DAILY_PROXY_SESSION_MINUTE_BARS",
    "DAY_TRADE_EXECUTION_FIELD_COUNT",
    "DAY_TRADE_FULL_SESSION_FIELDS",
    "DAY_TRADE_FULL_SESSION_MINUTES",
    "DAY_TRADE_MINUTE_EXECUTION_CONTRACT_VERSION",
    "DAY_TRADE_MINUTE_EXECUTION_POLICIES",
    "DAY_TRADE_MINUTE_EXECUTION_POLICY_FULL_VOLUME",
    "DAY_TRADE_MINUTE_EXECUTION_POLICY_SCHEDULED",
    "DAY_TRADE_MINUTE_SOURCE_SCHEMA_VERSION",
    "DayTradeExecutionField",
    "MIN_EXECUTABLE_MINUTE_VOLUME_SHARES",
    "FULL_SESSION_PRICE",
    "FULL_SESSION_VOLUME_SHARES",
    "load_tw_day_trade_execution_tape",
    "normalize_day_trade_minute_execution_policy",
    "tw_day_trade_minute_round_trip_masks",
]
