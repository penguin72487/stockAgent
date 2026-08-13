"""Official TAIFEX daily TXO rows normalized to causal opening slots.

The official daily file has one row per date/series/strike/right/session.  Its
``open`` and ``close`` fields are the first and last transaction prices of each
leg, not simultaneous executable quotes.  This module preserves that boundary
for both the legacy opening-ATM pair dataset and the complete structured chain.
The full chain uses the official front-month TX open only to assign a stable
relative-to-ATM strike rank; it never filters the chain by future liquidity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import csv
import math
from pathlib import Path
import re
from typing import Final, Iterable, Iterator, Literal, Mapping, Sequence

import numpy as np

from stockagent.data.tw_index_futures import (
    TAIFEX_DAY_SESSION_ALIASES,
    iter_taifex_daily_csv_streams,
    load_taifex_index_futures_day_session,
    parse_taifex_daily_price,
    parse_taifex_daily_volume,
    parse_taifex_trading_date,
)
from stockagent.data.tw_index_derivatives_tick import taifex_option_expiry


TAIFEX_TXO_PRODUCT: Final[str] = "TXO"
TAIFEX_TXO_MULTIPLIER: Final[float] = 50.0
TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION: Final[int] = 4
TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION: Final[int] = 1
TAIFEX_OPTIONS_DAILY_PRICE_SOURCE: Final[str] = "taifex_daily_first_last_trade_proxy"
TAIFEX_OPTION_SERIES_SCOPES: Final[tuple[str, str]] = ("monthly", "weekly")
TaifexOptionSeriesScope = Literal["monthly", "weekly"]
_MONTHLY_SERIES_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]{6}$")
_WEEKLY_SERIES_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?P<month>[0-9]{6})(?P<weekday>[WF])(?P<week>[1-5])$",
    re.IGNORECASE,
)
_DATASET_NAMES: Final[Mapping[str, str]] = {
    "monthly": "taifex_monthly_opening_atm_straddles",
    "weekly": "taifex_nearest_expiry_weekly_opening_atm_straddles",
}
_RIGHT_ALIASES: Final[Mapping[str, str]] = {
    "買權": "C",
    "CALL": "C",
    "C": "C",
    "賣權": "P",
    "PUT": "P",
    "P": "P",
}
_REQUIRED_SOURCE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "交易日期",
        "契約",
        "到期月份(週別)",
        "履約價",
        "買賣權",
        "開盤價",
        "收盤價",
        "結算價",
        "成交量",
    }
)
_NORMALIZED_COLUMNS: Final[tuple[str, ...]] = (
    "date",
    "tx_contract_month",
    "tx_open",
    "option_series",
    "strike",
    "opening_abs_moneyness_points",
    "call_open",
    "call_close",
    "call_settlement",
    "call_volume",
    "call_last_bid",
    "call_last_ask",
    "put_open",
    "put_close",
    "put_settlement",
    "put_volume",
    "put_last_bid",
    "put_last_ask",
    "executable",
    "exclusion_reason",
    "call_source_file",
    "call_source_sha256",
    "put_source_file",
    "put_source_sha256",
)

# These bounds are the exact extrema measured from every official receipt from
# 2001 through 2026-08-11.  A new receipt outside this envelope fails closed in
# the normalizer: silently clipping a newly listed contract would change the
# model/checkpoint action schema.
TAIFEX_OPTION_EXPIRY_SLOTS: Final[int] = 5
TAIFEX_MONTHLY_MONEYNESS_RANK_MIN: Final[int] = -268
TAIFEX_MONTHLY_MONEYNESS_RANK_MAX: Final[int] = 161
TAIFEX_WEEKLY_MONEYNESS_RANK_MIN: Final[int] = -186
TAIFEX_WEEKLY_MONEYNESS_RANK_MAX: Final[int] = 172


def option_scope_slot_count(series_scope: TaifexOptionSeriesScope) -> int:
    scope = _normalize_series_scope(series_scope)
    low, high = (
        (TAIFEX_MONTHLY_MONEYNESS_RANK_MIN, TAIFEX_MONTHLY_MONEYNESS_RANK_MAX)
        if scope == "monthly"
        else (TAIFEX_WEEKLY_MONEYNESS_RANK_MIN, TAIFEX_WEEKLY_MONEYNESS_RANK_MAX)
    )
    return TAIFEX_OPTION_EXPIRY_SLOTS * (high - low + 1) * 2


TAIFEX_MONTHLY_OPTION_SLOT_COUNT: Final[int] = (
    TAIFEX_OPTION_EXPIRY_SLOTS
    * (TAIFEX_MONTHLY_MONEYNESS_RANK_MAX - TAIFEX_MONTHLY_MONEYNESS_RANK_MIN + 1)
    * 2
)
TAIFEX_WEEKLY_OPTION_SLOT_COUNT: Final[int] = (
    TAIFEX_OPTION_EXPIRY_SLOTS
    * (TAIFEX_WEEKLY_MONEYNESS_RANK_MAX - TAIFEX_WEEKLY_MONEYNESS_RANK_MIN + 1)
    * 2
)
TAIFEX_ALL_OPTION_SLOT_COUNT: Final[int] = (
    TAIFEX_MONTHLY_OPTION_SLOT_COUNT + TAIFEX_WEEKLY_OPTION_SLOT_COUNT
)
TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT: Final[int] = 1 + TAIFEX_ALL_OPTION_SLOT_COUNT


def option_slot_index(
    series_scope: TaifexOptionSeriesScope,
    expiry_rank: int,
    moneyness_rank: int,
    right: str,
) -> int:
    """Map structured TXO identity to the stable combined option axis."""

    scope = _normalize_series_scope(series_scope)
    expiry = int(expiry_rank)
    if not 0 <= expiry < TAIFEX_OPTION_EXPIRY_SLOTS:
        raise ValueError(f"expiry_rank outside [0,{TAIFEX_OPTION_EXPIRY_SLOTS}): {expiry}")
    low, high, offset = (
        (
            TAIFEX_MONTHLY_MONEYNESS_RANK_MIN,
            TAIFEX_MONTHLY_MONEYNESS_RANK_MAX,
            0,
        )
        if scope == "monthly"
        else (
            TAIFEX_WEEKLY_MONEYNESS_RANK_MIN,
            TAIFEX_WEEKLY_MONEYNESS_RANK_MAX,
            TAIFEX_MONTHLY_OPTION_SLOT_COUNT,
        )
    )
    rank = int(moneyness_rank)
    if not low <= rank <= high:
        raise ValueError(
            f"{scope} moneyness_rank outside measured [{low},{high}]: {rank}"
        )
    normalized_right = str(right).strip().upper()
    if normalized_right not in {"C", "P"}:
        raise ValueError(f"unsupported option right {right!r}")
    width = high - low + 1
    return offset + ((expiry * width + (rank - low)) * 2) + (normalized_right == "P")


def option_slot_labels() -> tuple[str, ...]:
    labels: list[str] = []
    for scope, low, high in (
        ("monthly", TAIFEX_MONTHLY_MONEYNESS_RANK_MIN, TAIFEX_MONTHLY_MONEYNESS_RANK_MAX),
        ("weekly", TAIFEX_WEEKLY_MONEYNESS_RANK_MIN, TAIFEX_WEEKLY_MONEYNESS_RANK_MAX),
    ):
        for expiry_rank in range(TAIFEX_OPTION_EXPIRY_SLOTS):
            for moneyness_rank in range(low, high + 1):
                for right in ("C", "P"):
                    labels.append(
                        f"TXO_{scope.upper()}_E{expiry_rank + 1}_M{moneyness_rank:+d}_{right}"
                    )
    if len(labels) != TAIFEX_ALL_OPTION_SLOT_COUNT:
        raise RuntimeError("internal TXO slot-axis construction mismatch")
    return tuple(labels)


@dataclass(frozen=True, slots=True)
class _OptionDailyRow:
    trading_date: date
    series: str
    strike: float
    right: str
    open: float
    close: float
    settlement: float
    volume: int
    last_bid: float
    last_ask: float
    source_file: str
    source_sha256: str


@dataclass(frozen=True, slots=True)
class TaiwanIndexOptionPairDaySession:
    """One causally selected opening-ATM Call/Put pair per stock-panel date.

    The prices remain an official daily first/last-trade proxy.  ``executable``
    therefore means that both daily legs have finite positive fields and
    positive reported volume; it does not claim simultaneous historical
    bid/ask liquidity.
    """

    dates: np.ndarray
    series_scope: TaifexOptionSeriesScope
    option_series: np.ndarray
    strikes: np.ndarray
    call_open: np.ndarray
    call_close: np.ndarray
    put_open: np.ndarray
    put_close: np.ndarray
    executable: np.ndarray
    exclusion_reason: np.ndarray
    multiplier: float = TAIFEX_TXO_MULTIPLIER

    def __post_init__(self) -> None:
        scope = _normalize_series_scope(self.series_scope)
        object.__setattr__(self, "series_scope", scope)
        dates = np.asarray(self.dates, dtype="datetime64[D]")
        if dates.ndim != 1 or dates.size == 0:
            raise ValueError("option pair session dates must be a non-empty vector")
        if bool(np.any(dates[1:] <= dates[:-1])):
            raise ValueError("option pair session dates must be strictly increasing")
        rows = int(dates.size)
        object.__setattr__(self, "dates", dates)
        for name in (
            "option_series",
            "strikes",
            "call_open",
            "call_close",
            "put_open",
            "put_close",
            "executable",
            "exclusion_reason",
        ):
            values = np.asarray(getattr(self, name))
            if values.shape != (rows,):
                raise ValueError(f"{name} must have shape ({rows},)")
        if not math.isfinite(float(self.multiplier)) or float(self.multiplier) <= 0.0:
            raise ValueError("option multiplier must be finite and positive")

    def long_net_log_returns(
        self,
        *,
        fixed_fee_per_contract_per_side_twd: float,
        transaction_tax_rate: float,
        slippage_points_per_side: float,
    ) -> np.ndarray:
        """Return fee-adjusted per-premium Call/Put log returns ``[T,2]``.

        A continuous option budget ``b`` buys ``b*equity/open_premium`` units.
        Fixed per-contract charges therefore become a deterministic rate per
        opening premium.  Non-executable legs stay NaN so the executor can
        leave that requested sleeve in cash without fabricating a flat trade.
        """

        fee = float(fixed_fee_per_contract_per_side_twd)
        tax = float(transaction_tax_rate)
        slip = float(slippage_points_per_side)
        if any(not math.isfinite(value) or value < 0.0 for value in (fee, tax, slip)):
            raise ValueError("option costs must be finite and non-negative")
        output = np.full((self.dates.size, 2), np.nan, dtype=np.float32)
        for column, (opens, closes) in enumerate(
            ((self.call_open, self.call_close), (self.put_open, self.put_close))
        ):
            open_values = np.asarray(opens, dtype=np.float64)
            close_values = np.asarray(closes, dtype=np.float64)
            valid = (
                np.asarray(self.executable, dtype=bool)
                & np.isfinite(open_values)
                & (open_values > 0.0)
                & np.isfinite(close_values)
                & (close_values > 0.0)
            )
            close_over_open = np.ones_like(open_values)
            np.divide(close_values, open_values, out=close_over_open, where=valid)
            net_simple = (
                close_over_open
                - 1.0
                - (2.0 * fee) / (open_values * float(self.multiplier))
                # Long option: only the closing sale premium is taxable.
                - tax * close_over_open
                - (2.0 * slip) / open_values
            )
            safe = np.clip(net_simple, -0.999999, None)
            output[valid, column] = np.log1p(safe[valid]).astype(np.float32)
        return output


@dataclass(frozen=True, slots=True)
class TaiwanIndexOptionChainDaySession:
    """Sparse daily TXO chain mapped to one stable direct action axis.

    ``row_offsets`` partitions the remaining arrays by panel date.  A slot is
    an expiry-rank, relative-to-opening-ATM strike-rank, and Call/Put identity;
    ``option_series`` and ``strikes`` retain the actual listed contract needed
    by the integer executor.  Stocks never appear on this axis.
    """

    dates: np.ndarray
    row_offsets: np.ndarray
    slot_indices: np.ndarray
    option_series: np.ndarray
    strikes: np.ndarray
    option_rights: np.ndarray
    open_prices: np.ndarray
    close_prices: np.ndarray
    volumes: np.ndarray
    executable: np.ndarray
    multiplier: float = TAIFEX_TXO_MULTIPLIER

    def __post_init__(self) -> None:
        dates = np.asarray(self.dates, dtype="datetime64[D]")
        if dates.ndim != 1 or dates.size == 0:
            raise ValueError("option chain dates must be a non-empty vector")
        if bool(np.any(dates[1:] <= dates[:-1])):
            raise ValueError("option chain dates must be strictly increasing")
        offsets = np.asarray(self.row_offsets, dtype=np.int64)
        if offsets.shape != (dates.size + 1,) or offsets[0] != 0:
            raise ValueError("row_offsets must have shape [T+1] and start at zero")
        if bool(np.any(offsets[1:] < offsets[:-1])):
            raise ValueError("row_offsets must be non-decreasing")
        count = int(offsets[-1])
        if count != len(self.slot_indices):
            raise ValueError("row_offsets terminal value must equal sparse row count")
        for name in (
            "slot_indices",
            "option_series",
            "strikes",
            "option_rights",
            "open_prices",
            "close_prices",
            "volumes",
            "executable",
        ):
            if np.asarray(getattr(self, name)).shape != (count,):
                raise ValueError(f"{name} must have shape ({count},)")
        slots = np.asarray(self.slot_indices, dtype=np.int32)
        if count and (int(slots.min()) < 0 or int(slots.max()) >= TAIFEX_ALL_OPTION_SLOT_COUNT):
            raise ValueError("option chain contains a slot outside the fixed action axis")
        for row in range(dates.size):
            row_slots = slots[offsets[row] : offsets[row + 1]]
            if row_slots.size > 1 and bool(np.any(row_slots[1:] <= row_slots[:-1])):
                raise ValueError("each option-chain date must have unique sorted slots")
        if not math.isfinite(float(self.multiplier)) or float(self.multiplier) <= 0.0:
            raise ValueError("option multiplier must be finite and positive")
        object.__setattr__(self, "dates", dates)
        object.__setattr__(self, "row_offsets", offsets)
        object.__setattr__(self, "slot_indices", slots)

    @property
    def num_option_slots(self) -> int:
        return TAIFEX_ALL_OPTION_SLOT_COUNT

    def row_slice(self, row: int) -> slice:
        index = int(row)
        if not 0 <= index < self.dates.size:
            raise IndexError(index)
        return slice(int(self.row_offsets[index]), int(self.row_offsets[index + 1]))

    def long_net_log_returns(
        self,
        *,
        fixed_fee_per_contract_per_side_twd: float,
        transaction_tax_rate: float,
        slippage_points_per_side: float,
    ) -> np.ndarray:
        """Materialize fee-adjusted direct option returns as ``[T,7890]``."""

        fee = float(fixed_fee_per_contract_per_side_twd)
        tax = float(transaction_tax_rate)
        slip = float(slippage_points_per_side)
        if any(not math.isfinite(value) or value < 0.0 for value in (fee, tax, slip)):
            raise ValueError("option costs must be finite and non-negative")
        output = np.full(
            (self.dates.size, TAIFEX_ALL_OPTION_SLOT_COUNT),
            np.nan,
            dtype=np.float32,
        )
        opens = np.asarray(self.open_prices, dtype=np.float64)
        closes = np.asarray(self.close_prices, dtype=np.float64)
        valid = (
            np.asarray(self.executable, dtype=bool)
            & np.isfinite(opens)
            & (opens > 0.0)
            & np.isfinite(closes)
            & (closes > 0.0)
        )
        ratio = np.ones_like(opens)
        np.divide(closes, opens, out=ratio, where=valid)
        simple = (
            ratio
            - 1.0
            - (2.0 * fee) / (opens * float(self.multiplier))
            # Long option: only the closing sale premium is taxable.
            - tax * ratio
            - (2.0 * slip) / opens
        )
        values = np.log1p(np.clip(simple, -0.999999, None))
        for row in range(self.dates.size):
            selection = self.row_slice(row)
            row_valid = valid[selection]
            if bool(row_valid.any()):
                row_slots = self.slot_indices[selection][row_valid]
                output[row, row_slots] = values[selection][row_valid].astype(np.float32)
        return output

    def select_dates(self, requested_dates: Iterable[object]) -> "TaiwanIndexOptionChainDaySession":
        requested = np.asarray(list(requested_dates), dtype="datetime64[D]")
        if requested.ndim != 1 or requested.size == 0:
            raise ValueError("requested_dates must be a non-empty vector")
        indices = np.searchsorted(self.dates, requested)
        if bool(np.any(indices >= self.dates.size)) or not np.array_equal(
            self.dates[indices], requested
        ):
            raise ValueError("option chain does not cover every requested date")
        sparse_parts = [
            np.arange(self.row_offsets[i], self.row_offsets[i + 1], dtype=np.int64)
            for i in indices
        ]
        sparse_indices = (
            np.concatenate(sparse_parts)
            if sparse_parts
            else np.empty(0, dtype=np.int64)
        )
        counts = np.asarray([part.size for part in sparse_parts], dtype=np.int64)
        offsets = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(counts)))
        return TaiwanIndexOptionChainDaySession(
            dates=requested,
            row_offsets=offsets,
            slot_indices=self.slot_indices[sparse_indices],
            option_series=np.asarray(self.option_series)[sparse_indices],
            strikes=np.asarray(self.strikes)[sparse_indices],
            option_rights=np.asarray(self.option_rights)[sparse_indices],
            open_prices=np.asarray(self.open_prices)[sparse_indices],
            close_prices=np.asarray(self.close_prices)[sparse_indices],
            volumes=np.asarray(self.volumes)[sparse_indices],
            executable=np.asarray(self.executable)[sparse_indices],
            multiplier=self.multiplier,
        )


def _finite_positive(value: float) -> bool:
    return math.isfinite(value) and value > 0.0


def _same_number(left: float, right: float) -> bool:
    return (math.isnan(left) and math.isnan(right)) or left == right


def _same_option_row(left: _OptionDailyRow, right: _OptionDailyRow) -> bool:
    return (
        left.trading_date == right.trading_date
        and left.series == right.series
        and left.strike == right.strike
        and left.right == right.right
        and _same_number(left.open, right.open)
        and _same_number(left.close, right.close)
        and _same_number(left.settlement, right.settlement)
        and left.volume == right.volume
        and _same_number(left.last_bid, right.last_bid)
        and _same_number(left.last_ask, right.last_ask)
    )


def _parse_right(value: object) -> str | None:
    return _RIGHT_ALIASES.get(str(value or "").strip().upper())


def _normalize_series_scope(value: object) -> TaifexOptionSeriesScope:
    normalized = str(value).strip().casefold()
    if normalized not in TAIFEX_OPTION_SERIES_SCOPES:
        raise ValueError(
            f"unsupported TAIFEX option series scope {value!r}; "
            f"expected one of {TAIFEX_OPTION_SERIES_SCOPES}"
        )
    return normalized  # type: ignore[return-value]


def _series_matches(series: str, scope: TaifexOptionSeriesScope) -> bool:
    pattern = _MONTHLY_SERIES_RE if scope == "monthly" else _WEEKLY_SERIES_RE
    return pattern.fullmatch(series) is not None


def _series_scope(series: str) -> TaifexOptionSeriesScope | None:
    if _MONTHLY_SERIES_RE.fullmatch(series) is not None:
        return "monthly"
    if _WEEKLY_SERIES_RE.fullmatch(series) is not None:
        return "weekly"
    return None


def _series_sort_key(
    series: str,
    scope: TaifexOptionSeriesScope,
) -> tuple[int, int, str]:
    if scope == "monthly":
        return int(series), 0, series
    match = _WEEKLY_SERIES_RE.fullmatch(series)
    if match is None:
        raise ValueError(f"invalid weekly TXO series {series!r}")
    expiry = taifex_option_expiry(series)
    weekday_order = 0 if match.group("weekday").upper() == "W" else 1
    return expiry.toordinal(), weekday_order, series


def _read_txo_rows(
    source_path: Path,
    *,
    series_scope: TaifexOptionSeriesScope | None,
) -> tuple[
    dict[date, dict[tuple[str, float, str], _OptionDailyRow]],
    set[date],
]:
    by_date: dict[date, dict[tuple[str, float, str], _OptionDailyRow]] = {}
    all_txo_dates: set[date] = set()
    for stream, source_name, source_sha256 in iter_taifex_daily_csv_streams(
        source_path
    ):
        reader = csv.DictReader(stream)
        if reader.fieldnames is None:
            raise ValueError(f"{source_name} has no CSV header")
        reader.fieldnames = [
            str(name or "").lstrip("\ufeff").strip() for name in reader.fieldnames
        ]
        missing = sorted(_REQUIRED_SOURCE_COLUMNS - set(reader.fieldnames))
        if missing:
            raise ValueError(f"{source_name} is missing TAIFEX columns: {missing}")
        has_session = "交易時段" in reader.fieldnames
        for raw in reader:
            if str(raw.get("契約") or "").strip().upper() != TAIFEX_TXO_PRODUCT:
                continue
            if has_session:
                session = str(raw.get("交易時段") or "").strip().casefold()
                if session not in TAIFEX_DAY_SESSION_ALIASES:
                    continue
            parsed_date = parse_taifex_trading_date(raw.get("交易日期"))
            if parsed_date is None:
                continue
            trading_date = date.fromisoformat(str(parsed_date))
            all_txo_dates.add(trading_date)
            series = str(raw.get("到期月份(週別)") or "").strip()
            actual_scope = _series_scope(series)
            if actual_scope is None or (
                series_scope is not None and actual_scope != series_scope
            ):
                continue
            right = _parse_right(raw.get("買賣權"))
            if right is None:
                continue
            strike = parse_taifex_daily_price(raw.get("履約價"))
            if not _finite_positive(strike):
                continue
            row = _OptionDailyRow(
                trading_date=trading_date,
                series=series,
                strike=strike,
                right=right,
                open=parse_taifex_daily_price(raw.get("開盤價")),
                close=parse_taifex_daily_price(raw.get("收盤價")),
                settlement=parse_taifex_daily_price(raw.get("結算價")),
                volume=parse_taifex_daily_volume(raw.get("成交量")),
                last_bid=parse_taifex_daily_price(raw.get("最後最佳買價")),
                last_ask=parse_taifex_daily_price(raw.get("最後最佳賣價")),
                source_file=source_name,
                source_sha256=source_sha256,
            )
            key = (series, strike, right)
            previous = by_date.setdefault(trading_date, {}).get(key)
            if previous is not None and not _same_option_row(previous, row):
                raise ValueError(
                    "conflicting TAIFEX option rows for "
                    f"{trading_date}/{series}/{strike}/{right}: "
                    f"{previous.source_file} vs {source_name}"
                )
            by_date[trading_date][key] = row
    return by_date, all_txo_dates


def iter_taifex_option_daily_rows(
    option_source_paths: Iterable[str | Path],
    *,
    trading_dates: Iterable[date] | None = None,
    series_scopes: Sequence[TaifexOptionSeriesScope] = ("monthly", "weekly"),
) -> Iterator[dict[str, object]]:
    """Yield normalized official TXO day-session rows without a second parser.

    The iterator reads one receipt at a time, so callers can build a filtered
    surface cache without materializing the complete multi-year option chain.
    It intentionally preserves each contract's own first/last trade and final
    quote fields; none of those fields are treated as synchronized quotes.
    """

    normalized_scopes = tuple(
        _normalize_series_scope(scope) for scope in series_scopes
    )
    if not normalized_scopes:
        raise ValueError("series_scopes must not be empty")
    if len(set(normalized_scopes)) != len(normalized_scopes):
        raise ValueError(f"series_scopes contains duplicates: {normalized_scopes}")
    requested_dates = None if trading_dates is None else set(trading_dates)
    for raw_path in option_source_paths:
        source_path = Path(raw_path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"TAIFEX option source does not exist: {source_path}")
        rows_by_date, _all_txo_dates = _read_txo_rows(
            source_path,
            series_scope=None,
        )
        for trading_date in sorted(rows_by_date):
            if requested_dates is not None and trading_date not in requested_dates:
                continue
            for row in rows_by_date[trading_date].values():
                scope = _series_scope(row.series)
                if scope not in normalized_scopes:
                    continue
                yield {
                    "date": row.trading_date,
                    "option_series": row.series,
                    "series_scope": scope,
                    "strike": row.strike,
                    "option_right": row.right,
                    "open": row.open if math.isfinite(row.open) else None,
                    "close": row.close if math.isfinite(row.close) else None,
                    "settlement": (
                        row.settlement if math.isfinite(row.settlement) else None
                    ),
                    "volume": row.volume,
                    "last_bid": (
                        row.last_bid if math.isfinite(row.last_bid) else None
                    ),
                    "last_ask": (
                        row.last_ask if math.isfinite(row.last_ask) else None
                    ),
                    "source_file": row.source_file,
                    "source_sha256": row.source_sha256,
                }


def _none_row(
    trading_date: date,
    reason: str,
    *,
    tx_contract_month: str | None = None,
    tx_open: float | None = None,
) -> dict[str, object]:
    return {
        "date": trading_date,
        "tx_contract_month": tx_contract_month,
        "tx_open": tx_open,
        "option_series": None,
        "strike": None,
        "opening_abs_moneyness_points": None,
        "call_open": None,
        "call_close": None,
        "call_settlement": None,
        "call_volume": None,
        "call_last_bid": None,
        "call_last_ask": None,
        "put_open": None,
        "put_close": None,
        "put_settlement": None,
        "put_volume": None,
        "put_last_bid": None,
        "put_last_ask": None,
        "executable": False,
        "exclusion_reason": reason,
        "call_source_file": None,
        "call_source_sha256": None,
        "put_source_file": None,
        "put_source_sha256": None,
    }


def _select_atm_pair(
    trading_date: date,
    rows: Mapping[tuple[str, float, str], _OptionDailyRow],
    *,
    series_scope: TaifexOptionSeriesScope,
    tx_contract_month: str | None,
    tx_open: float | None,
) -> dict[str, object]:
    if tx_open is None or not _finite_positive(tx_open):
        return _none_row(
            trading_date,
            "missing_front_month_tx_open",
            tx_contract_month=tx_contract_month,
            tx_open=tx_open,
        )
    series_values = {series for series, _strike, _right in rows}
    if series_scope == "monthly":
        calendar_month = trading_date.strftime("%Y%m")
        series_values = {series for series in series_values if series >= calendar_month}
    if not series_values:
        return _none_row(
            trading_date,
            f"no_{series_scope}_txo_series",
            tx_contract_month=tx_contract_month,
            tx_open=tx_open,
        )
    series = min(
        series_values,
        key=lambda candidate: _series_sort_key(candidate, series_scope),
    )
    call_strikes = {
        strike for candidate_series, strike, right in rows
        if candidate_series == series and right == "C"
    }
    put_strikes = {
        strike for candidate_series, strike, right in rows
        if candidate_series == series and right == "P"
    }
    paired_strikes = call_strikes & put_strikes
    if not paired_strikes:
        return _none_row(
            trading_date,
            "no_paired_call_put_strike",
            tx_contract_month=tx_contract_month,
            tx_open=tx_open,
        )
    strike = min(paired_strikes, key=lambda value: (abs(value - tx_open), value))
    call = rows[(series, strike, "C")]
    put = rows[(series, strike, "P")]
    failures: list[str] = []
    for label, row in (("call", call), ("put", put)):
        if not _finite_positive(row.open):
            failures.append(f"missing_{label}_open")
        if not _finite_positive(row.close):
            failures.append(f"missing_{label}_close")
        if row.volume <= 0:
            failures.append(f"nonpositive_{label}_volume")
    return {
        "date": trading_date,
        "tx_contract_month": tx_contract_month,
        "tx_open": tx_open,
        "option_series": series,
        "strike": strike,
        "opening_abs_moneyness_points": abs(strike - tx_open),
        "call_open": call.open if math.isfinite(call.open) else None,
        "call_close": call.close if math.isfinite(call.close) else None,
        "call_settlement": (
            call.settlement if math.isfinite(call.settlement) else None
        ),
        "call_volume": call.volume,
        "call_last_bid": call.last_bid if math.isfinite(call.last_bid) else None,
        "call_last_ask": call.last_ask if math.isfinite(call.last_ask) else None,
        "put_open": put.open if math.isfinite(put.open) else None,
        "put_close": put.close if math.isfinite(put.close) else None,
        "put_settlement": (
            put.settlement if math.isfinite(put.settlement) else None
        ),
        "put_volume": put.volume,
        "put_last_bid": put.last_bid if math.isfinite(put.last_bid) else None,
        "put_last_ask": put.last_ask if math.isfinite(put.last_ask) else None,
        "executable": not failures,
        "exclusion_reason": "|".join(failures) if failures else None,
        "call_source_file": call.source_file,
        "call_source_sha256": call.source_sha256,
        "put_source_file": put.source_file,
        "put_source_sha256": put.source_sha256,
    }


def _same_selected_row(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    comparable = [
        column
        for column in _NORMALIZED_COLUMNS
        if not column.endswith(("source_file", "source_sha256"))
    ]
    for column in comparable:
        left_value = left.get(column)
        right_value = right.get(column)
        if isinstance(left_value, float) and isinstance(right_value, float):
            if math.isnan(left_value) and math.isnan(right_value):
                continue
        if left_value != right_value:
            return False
    return True


def build_taifex_opening_atm_straddles(
    option_source_paths: Iterable[str | Path],
    futures_path: str | Path,
    output_path: str | Path,
    *,
    series_scope: TaifexOptionSeriesScope,
) -> Path:
    """Build one official daily opening-ATM TXO candidate per session."""

    series_scope = _normalize_series_scope(series_scope)

    futures = load_taifex_index_futures_day_session(
        futures_path,
        products=("TX",),
    )
    tx_by_date: dict[date, tuple[str, float]] = {}
    for index, raw_date in enumerate(futures.dates):
        if not bool(futures.tradable_mask[index, 0]):
            continue
        tx_by_date[date.fromisoformat(str(raw_date))] = (
            str(futures.contract_months[index, 0]),
            float(futures.open_prices[index, 0]),
        )

    selected: dict[date, dict[str, object]] = {}
    option_dates: set[date] = set()
    all_txo_dates: set[date] = set()
    for raw_path in option_source_paths:
        source_path = Path(raw_path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"TAIFEX option source does not exist: {source_path}")
        source_rows, source_txo_dates = _read_txo_rows(
            source_path,
            series_scope=series_scope,
        )
        all_txo_dates.update(source_txo_dates)
        for trading_date, rows in source_rows.items():
            option_dates.add(trading_date)
            tx_payload = tx_by_date.get(trading_date)
            current = _select_atm_pair(
                trading_date,
                rows,
                series_scope=series_scope,
                tx_contract_month=tx_payload[0] if tx_payload else None,
                tx_open=tx_payload[1] if tx_payload else None,
            )
            previous = selected.get(trading_date)
            if previous is not None and not _same_selected_row(previous, current):
                raise ValueError(
                    f"conflicting selected ATM rows for {trading_date}: "
                    f"{previous.get('call_source_file')} vs {current.get('call_source_file')}"
                )
            selected[trading_date] = current

    if not option_dates:
        raise ValueError(f"no TXO {series_scope} daily rows were found")
    start = min(option_dates)
    end = max(option_dates)
    for trading_date in tx_by_date:
        if start <= trading_date <= end and trading_date not in selected:
            tx_contract_month, tx_open = tx_by_date[trading_date]
            selected[trading_date] = _none_row(
                trading_date,
                (
                    "no_weekly_txo_listing"
                    if series_scope == "weekly" and trading_date in all_txo_dates
                    else "missing_txo_daily_partition"
                ),
                tx_contract_month=tx_contract_month,
                tx_open=tx_open,
            )

    import pyarrow as pa
    import pyarrow.parquet as pq

    ordered = [selected[key] for key in sorted(selected)]
    schema = pa.schema(
        [
            ("date", pa.date32()),
            ("tx_contract_month", pa.string()),
            ("tx_open", pa.float64()),
            ("option_series", pa.string()),
            ("strike", pa.float64()),
            ("opening_abs_moneyness_points", pa.float64()),
            ("call_open", pa.float64()),
            ("call_close", pa.float64()),
            ("call_settlement", pa.float64()),
            ("call_volume", pa.int64()),
            ("call_last_bid", pa.float64()),
            ("call_last_ask", pa.float64()),
            ("put_open", pa.float64()),
            ("put_close", pa.float64()),
            ("put_settlement", pa.float64()),
            ("put_volume", pa.int64()),
            ("put_last_bid", pa.float64()),
            ("put_last_ask", pa.float64()),
            ("executable", pa.bool_()),
            ("exclusion_reason", pa.string()),
            ("call_source_file", pa.string()),
            ("call_source_sha256", pa.string()),
            ("put_source_file", pa.string()),
            ("put_source_sha256", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(ordered, schema=schema)
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"stockagent.dataset": _DATASET_NAMES[series_scope].encode("ascii"),
            b"stockagent.contract_version": str(
                TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION
            ).encode("ascii"),
            b"stockagent.product": TAIFEX_TXO_PRODUCT.encode("ascii"),
            b"stockagent.session": b"day",
            b"stockagent.series_scope": series_scope.encode("ascii"),
            b"stockagent.price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE.encode(
                "ascii"
            ),
        }
    )
    table = table.replace_schema_metadata(metadata)
    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    pq.write_table(table, temporary, compression="zstd")
    temporary.replace(target)
    return target


def build_taifex_monthly_atm_straddles(
    option_source_paths: Iterable[str | Path],
    futures_path: str | Path,
    output_path: str | Path,
) -> Path:
    return build_taifex_opening_atm_straddles(
        option_source_paths,
        futures_path,
        output_path,
        series_scope="monthly",
    )


def build_taifex_weekly_atm_straddles(
    option_source_paths: Iterable[str | Path],
    futures_path: str | Path,
    output_path: str | Path,
) -> Path:
    return build_taifex_opening_atm_straddles(
        option_source_paths,
        futures_path,
        output_path,
        series_scope="weekly",
    )


def build_taifex_option_full_chain(
    option_source_paths: Iterable[str | Path],
    futures_path: str | Path,
    output_path: str | Path,
    *,
    series_scope: TaifexOptionSeriesScope,
) -> Path:
    """Normalize every listed unexpired TXO leg to the fixed direct axis."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    scope = _normalize_series_scope(series_scope)
    futures = load_taifex_index_futures_day_session(futures_path, products=("TX",))
    tx_by_date = {
        date.fromisoformat(str(raw_date)): float(futures.open_prices[index, 0])
        for index, raw_date in enumerate(futures.dates)
        if bool(futures.tradable_mask[index, 0])
    }
    schema = pa.schema(
        [
            ("date", pa.date32()),
            ("series_scope", pa.string()),
            ("expiry_rank", pa.int8()),
            ("moneyness_rank", pa.int16()),
            ("option_slot", pa.int32()),
            ("option_series", pa.string()),
            ("expiry", pa.date32()),
            ("strike", pa.float64()),
            ("option_right", pa.string()),
            ("tx_open", pa.float64()),
            ("open", pa.float64()),
            ("close", pa.float64()),
            ("settlement", pa.float64()),
            ("volume", pa.int64()),
            ("last_bid", pa.float64()),
            ("last_ask", pa.float64()),
            ("executable", pa.bool_()),
            ("exclusion_reason", pa.string()),
            ("source_file", pa.string()),
            ("source_sha256", pa.string()),
        ],
        metadata={
            b"stockagent.dataset": f"taifex_{scope}_full_option_chain".encode("ascii"),
            b"stockagent.contract_version": str(
                TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION
            ).encode("ascii"),
            b"stockagent.product": TAIFEX_TXO_PRODUCT.encode("ascii"),
            b"stockagent.session": b"day",
            b"stockagent.series_scope": scope.encode("ascii"),
            b"stockagent.price_source": TAIFEX_OPTIONS_DAILY_PRICE_SOURCE.encode("ascii"),
            b"stockagent.option_axis_size": str(TAIFEX_ALL_OPTION_SLOT_COUNT).encode("ascii"),
        },
    )
    target = Path(output_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    writer = pq.ParquetWriter(temporary, schema, compression="zstd")
    seen_dates: set[date] = set()
    total_rows = 0
    try:
        for raw_path in option_source_paths:
            source_path = Path(raw_path).expanduser().resolve()
            if not source_path.is_file():
                raise FileNotFoundError(
                    f"TAIFEX option source does not exist: {source_path}"
                )
            rows_by_date, _all_txo_dates = _read_txo_rows(
                source_path,
                series_scope=scope,
            )
            normalized_rows: list[dict[str, object]] = []
            for trading_date in sorted(rows_by_date):
                if trading_date in seen_dates:
                    raise ValueError(
                        f"overlapping full-chain receipts contain {trading_date}"
                    )
                seen_dates.add(trading_date)
                tx_open = tx_by_date.get(trading_date)
                if tx_open is None or not _finite_positive(tx_open):
                    continue
                source_rows = rows_by_date[trading_date]
                candidates = {series for series, _strike, _right in source_rows}
                candidates = {
                    series
                    for series in candidates
                    if taifex_option_expiry(series) >= trading_date
                }
                ordered_series = sorted(
                    candidates,
                    key=lambda value: _series_sort_key(value, scope),
                )
                if len(ordered_series) > TAIFEX_OPTION_EXPIRY_SLOTS:
                    raise ValueError(
                        f"{trading_date} {scope} has {len(ordered_series)} unexpired "
                        f"series, exceeding action schema {TAIFEX_OPTION_EXPIRY_SLOTS}"
                    )
                for expiry_rank, series in enumerate(ordered_series):
                    strikes = sorted(
                        {
                            strike
                            for candidate, strike, _right in source_rows
                            if candidate == series
                        }
                    )
                    if not strikes:
                        continue
                    atm_index = min(
                        range(len(strikes)),
                        key=lambda index: (abs(strikes[index] - tx_open), strikes[index]),
                    )
                    rank_by_strike = {
                        strike: index - atm_index for index, strike in enumerate(strikes)
                    }
                    for (candidate, strike, right), row in sorted(source_rows.items()):
                        if candidate != series:
                            continue
                        moneyness_rank = rank_by_strike[strike]
                        slot = option_slot_index(
                            scope,
                            expiry_rank,
                            moneyness_rank,
                            right,
                        )
                        failures: list[str] = []
                        if not _finite_positive(row.open):
                            failures.append("missing_open")
                        if not _finite_positive(row.close):
                            failures.append("missing_close")
                        if row.volume <= 0:
                            failures.append("nonpositive_volume")
                        normalized_rows.append(
                            {
                                "date": trading_date,
                                "series_scope": scope,
                                "expiry_rank": expiry_rank,
                                "moneyness_rank": moneyness_rank,
                                "option_slot": slot,
                                "option_series": series,
                                "expiry": taifex_option_expiry(series),
                                "strike": strike,
                                "option_right": right,
                                "tx_open": tx_open,
                                "open": row.open if math.isfinite(row.open) else None,
                                "close": row.close if math.isfinite(row.close) else None,
                                "settlement": (
                                    row.settlement if math.isfinite(row.settlement) else None
                                ),
                                "volume": row.volume,
                                "last_bid": row.last_bid if math.isfinite(row.last_bid) else None,
                                "last_ask": row.last_ask if math.isfinite(row.last_ask) else None,
                                "executable": not failures,
                                "exclusion_reason": "|".join(failures) if failures else None,
                                "source_file": row.source_file,
                                "source_sha256": row.source_sha256,
                            }
                        )
            if normalized_rows:
                normalized_rows.sort(key=lambda item: (item["date"], item["option_slot"]))
                writer.write_table(pa.Table.from_pylist(normalized_rows, schema=schema))
                total_rows += len(normalized_rows)
    finally:
        writer.close()
    if total_rows == 0:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"no normalized TXO {scope} full-chain rows were found")
    temporary.replace(target)
    return target


def load_taifex_option_full_chain(
    path: str | Path,
    *,
    expected_series_scope: TaifexOptionSeriesScope,
    panel_dates: Iterable[object],
) -> TaiwanIndexOptionChainDaySession:
    """Load one normalized scope and align its sparse rows to panel dates."""

    import pyarrow.parquet as pq

    scope = _normalize_series_scope(expected_series_scope)
    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"TAIFEX full option-chain parquet does not exist: {source}")
    table = pq.read_table(
        source,
        columns=[
            "date",
            "option_slot",
            "option_series",
            "strike",
            "option_right",
            "open",
            "close",
            "volume",
            "executable",
        ],
    )
    metadata = table.schema.metadata or {}
    if int(metadata.get(b"stockagent.contract_version", b"-1")) != (
        TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION
    ):
        raise ValueError(f"{source} has unsupported full-chain contract metadata")
    if metadata.get(b"stockagent.series_scope", b"").decode("ascii") != scope:
        raise ValueError(f"{source} does not contain expected {scope} option scope")
    requested = np.asarray(list(panel_dates), dtype="datetime64[D]")
    if requested.ndim != 1 or requested.size == 0 or bool(np.any(requested[1:] <= requested[:-1])):
        raise ValueError("panel_dates must be a non-empty strictly increasing vector")
    source_dates = np.asarray(table.column("date").to_numpy(), dtype="datetime64[D]")
    slots = np.asarray(table.column("option_slot").to_numpy(), dtype=np.int32)
    order = np.lexsort((slots, source_dates.astype(np.int64)))
    source_dates = source_dates[order]
    slots = slots[order]
    panel_rows = np.searchsorted(requested, source_dates)
    matched = panel_rows < requested.size
    matched[matched] &= requested[panel_rows[matched]] == source_dates[matched]
    selected = order[matched]
    selected_panel_rows = panel_rows[matched]
    counts = np.bincount(selected_panel_rows, minlength=requested.size).astype(np.int64)
    offsets = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(counts)))

    def numeric(name: str, dtype: np.dtype) -> np.ndarray:
        values = np.asarray(
            table.column(name).to_numpy(zero_copy_only=False), dtype=dtype
        )
        return values[selected]

    def text_values(name: str, width: int) -> np.ndarray:
        values = np.asarray(
            ["" if value is None else str(value) for value in table.column(name).to_pylist()],
            dtype=f"U{width}",
        )
        return values[selected]

    return TaiwanIndexOptionChainDaySession(
        dates=requested,
        row_offsets=offsets,
        slot_indices=slots[matched],
        option_series=text_values("option_series", 12),
        strikes=numeric("strike", np.float64),
        option_rights=text_values("option_right", 1),
        open_prices=numeric("open", np.float64),
        close_prices=numeric("close", np.float64),
        volumes=numeric("volume", np.int64),
        executable=numeric("executable", np.bool_),
    )


def combine_taifex_option_chains(
    monthly: TaiwanIndexOptionChainDaySession,
    weekly: TaiwanIndexOptionChainDaySession,
) -> TaiwanIndexOptionChainDaySession:
    """Merge aligned monthly/weekly sparse rows into their shared slot axis."""

    if not np.array_equal(monthly.dates, weekly.dates):
        raise ValueError("monthly and weekly option-chain calendars must align")
    fields = (
        "slot_indices",
        "option_series",
        "strikes",
        "option_rights",
        "open_prices",
        "close_prices",
        "volumes",
        "executable",
    )
    parts: dict[str, list[np.ndarray]] = {name: [] for name in fields}
    counts = np.zeros(monthly.dates.size, dtype=np.int64)
    for row in range(monthly.dates.size):
        month_slice = monthly.row_slice(row)
        week_slice = weekly.row_slice(row)
        counts[row] = (month_slice.stop - month_slice.start) + (week_slice.stop - week_slice.start)
        for name in fields:
            parts[name].append(np.asarray(getattr(monthly, name))[month_slice])
            parts[name].append(np.asarray(getattr(weekly, name))[week_slice])
    merged = {
        name: np.concatenate(values) if values else np.empty(0)
        for name, values in parts.items()
    }
    return TaiwanIndexOptionChainDaySession(
        dates=monthly.dates.copy(),
        row_offsets=np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(counts))),
        multiplier=monthly.multiplier,
        **merged,
    )


def load_taifex_option_daily_contract_rows(
    option_source_paths: Iterable[str | Path],
    targets: Iterable[tuple[date, str, float, str]],
    *,
    series_scope: TaifexOptionSeriesScope,
) -> dict[tuple[date, str, float, str], dict[str, object]]:
    """Load only requested official daily option contract rows.

    This reuses the canonical CSV/ZIP parser and duplicate-conflict checks so
    multi-session research can follow fixed contracts without materializing a
    second raw-chain format.
    """

    scope = _normalize_series_scope(series_scope)
    normalized_targets = {
        (trading_date, str(series).strip().upper(), float(strike), str(right).upper())
        for trading_date, series, strike, right in targets
    }
    invalid_rights = sorted(
        {right for _date, _series, _strike, right in normalized_targets}
        - set(_RIGHT_ALIASES.values())
    )
    if invalid_rights:
        raise ValueError(f"unsupported option rights in targets: {invalid_rights}")
    targets_by_date: dict[date, set[tuple[str, float, str]]] = {}
    for trading_date, series, strike, right in normalized_targets:
        targets_by_date.setdefault(trading_date, set()).add((series, strike, right))

    selected: dict[tuple[date, str, float, str], _OptionDailyRow] = {}
    for raw_path in option_source_paths:
        source_path = Path(raw_path).expanduser().resolve()
        if not source_path.is_file():
            raise FileNotFoundError(f"TAIFEX option source does not exist: {source_path}")
        rows_by_date, _all_txo_dates = _read_txo_rows(
            source_path,
            series_scope=scope,
        )
        for trading_date, contract_keys in targets_by_date.items():
            source_rows = rows_by_date.get(trading_date)
            if source_rows is None:
                continue
            for series, strike, right in contract_keys:
                row = source_rows.get((series, strike, right))
                if row is None:
                    continue
                key = (trading_date, series, strike, right)
                previous = selected.get(key)
                if previous is not None and not _same_option_row(previous, row):
                    raise ValueError(
                        "conflicting TAIFEX target option rows for "
                        f"{trading_date}/{series}/{strike}/{right}: "
                        f"{previous.source_file} vs {row.source_file}"
                    )
                selected[key] = row

    return {
        key: {
            "date": row.trading_date,
            "option_series": row.series,
            "strike": row.strike,
            "option_right": row.right,
            "open": row.open if math.isfinite(row.open) else None,
            "close": row.close if math.isfinite(row.close) else None,
            "settlement": (
                row.settlement if math.isfinite(row.settlement) else None
            ),
            "volume": row.volume,
            "last_bid": row.last_bid if math.isfinite(row.last_bid) else None,
            "last_ask": row.last_ask if math.isfinite(row.last_ask) else None,
            "source_file": row.source_file,
            "source_sha256": row.source_sha256,
        }
        for key, row in selected.items()
    }


def load_taifex_opening_atm_straddles(
    path: str | Path,
    *,
    expected_series_scope: TaifexOptionSeriesScope | None = None,
):
    import pyarrow.parquet as pq

    source = Path(path).expanduser().resolve()
    if not source.is_file():
        raise FileNotFoundError(f"TAIFEX ATM straddle parquet does not exist: {source}")
    table = pq.read_table(source)
    metadata = table.schema.metadata or {}
    version = metadata.get(b"stockagent.contract_version")
    if version is None or int(version) not in {
        1,
        2,
        3,
        TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION,
    }:
        raise ValueError(f"{source} has unsupported option daily contract {version!r}")
    missing = sorted(set(_NORMALIZED_COLUMNS) - set(table.column_names))
    legacy_settlement_columns = {"call_settlement", "put_settlement"}
    if set(missing).issubset(legacy_settlement_columns) and int(version) < 4:
        import pyarrow as pa

        for column in sorted(missing):
            table = table.append_column(
                column,
                pa.nulls(table.num_rows, type=pa.float64()),
            )
        missing = []
    if missing:
        raise ValueError(f"{source} is missing normalized columns: {missing}")
    raw_scope = metadata.get(b"stockagent.series_scope")
    actual_scope = (
        raw_scope.decode("ascii")
        if raw_scope is not None
        else "monthly"
    )
    if expected_series_scope is not None:
        expected = _normalize_series_scope(expected_series_scope)
        if actual_scope != expected:
            raise ValueError(
                f"{source} has option series scope {actual_scope!r}, expected {expected!r}"
            )
    return table


def load_taifex_monthly_atm_straddles(path: str | Path):
    return load_taifex_opening_atm_straddles(
        path,
        expected_series_scope="monthly",
    )


def load_taifex_weekly_atm_straddles(path: str | Path):
    return load_taifex_opening_atm_straddles(
        path,
        expected_series_scope="weekly",
    )


def load_taifex_option_pair_day_session(
    path: str | Path,
    *,
    series_scope: TaifexOptionSeriesScope,
    panel_dates: Iterable[object] | None = None,
) -> TaiwanIndexOptionPairDaySession:
    """Load one normalized TXO pair and align it to the stock-panel calendar."""

    scope = _normalize_series_scope(series_scope)
    table = load_taifex_opening_atm_straddles(
        path,
        expected_series_scope=scope,
    )
    source_dates = np.asarray(table.column("date").to_numpy(), dtype="datetime64[D]")
    if source_dates.ndim != 1 or source_dates.size == 0:
        raise ValueError(f"{path} contains no normalized option dates")
    if np.unique(source_dates).size != source_dates.size:
        raise ValueError(f"{path} contains duplicate normalized option dates")
    order = np.argsort(source_dates, kind="stable")
    source_dates = source_dates[order]
    requested_dates = (
        source_dates.copy()
        if panel_dates is None
        else np.asarray(list(panel_dates), dtype="datetime64[D]")
    )
    if requested_dates.ndim != 1 or requested_dates.size == 0:
        raise ValueError("panel_dates must be a non-empty one-dimensional sequence")
    if bool(np.any(requested_dates[1:] <= requested_dates[:-1])):
        raise ValueError("panel_dates must be strictly increasing")

    row_indices = np.searchsorted(source_dates, requested_dates)
    candidate = np.flatnonzero(row_indices < source_dates.size)
    matched = np.zeros(requested_dates.shape, dtype=bool)
    matched[candidate] = (
        source_dates[row_indices[candidate]] == requested_dates[candidate]
    )
    rows = int(requested_dates.size)

    def numeric(name: str) -> np.ndarray:
        source = np.asarray(table.column(name).to_numpy(zero_copy_only=False), dtype=np.float64)[order]
        out = np.full(rows, np.nan, dtype=np.float64)
        out[matched] = source[row_indices[matched]]
        return out

    def text_values(name: str, *, missing: str = "") -> np.ndarray:
        source = np.asarray(
            ["" if value is None else str(value) for value in table.column(name).to_pylist()],
            dtype="U160",
        )[order]
        out = np.full(rows, missing, dtype="U160")
        out[matched] = source[row_indices[matched]]
        return out

    source_executable = np.asarray(
        table.column("executable").to_numpy(zero_copy_only=False),
        dtype=bool,
    )[order]
    executable = np.zeros(rows, dtype=bool)
    executable[matched] = source_executable[row_indices[matched]]
    reasons = text_values("exclusion_reason", missing="missing_normalized_option_date")
    reasons[~matched] = "missing_normalized_option_date"
    return TaiwanIndexOptionPairDaySession(
        dates=requested_dates,
        series_scope=scope,
        option_series=text_values("option_series"),
        strikes=numeric("strike"),
        call_open=numeric("call_open"),
        call_close=numeric("call_close"),
        put_open=numeric("put_open"),
        put_close=numeric("put_close"),
        executable=executable,
        exclusion_reason=reasons,
    )


__all__ = [
    "TAIFEX_ALL_OPTION_SLOT_COUNT",
    "TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT",
    "TAIFEX_MONTHLY_MONEYNESS_RANK_MAX",
    "TAIFEX_MONTHLY_MONEYNESS_RANK_MIN",
    "TAIFEX_MONTHLY_OPTION_SLOT_COUNT",
    "TAIFEX_OPTION_EXPIRY_SLOTS",
    "TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION",
    "TAIFEX_OPTIONS_FULL_CHAIN_DATA_CONTRACT_VERSION",
    "TAIFEX_OPTIONS_DAILY_PRICE_SOURCE",
    "TAIFEX_OPTION_SERIES_SCOPES",
    "TAIFEX_TXO_MULTIPLIER",
    "TAIFEX_TXO_PRODUCT",
    "TAIFEX_WEEKLY_MONEYNESS_RANK_MAX",
    "TAIFEX_WEEKLY_MONEYNESS_RANK_MIN",
    "TAIFEX_WEEKLY_OPTION_SLOT_COUNT",
    "TaiwanIndexOptionChainDaySession",
    "TaiwanIndexOptionPairDaySession",
    "TaifexOptionSeriesScope",
    "build_taifex_monthly_atm_straddles",
    "build_taifex_option_full_chain",
    "build_taifex_opening_atm_straddles",
    "build_taifex_weekly_atm_straddles",
    "combine_taifex_option_chains",
    "iter_taifex_option_daily_rows",
    "load_taifex_monthly_atm_straddles",
    "load_taifex_option_daily_contract_rows",
    "load_taifex_option_full_chain",
    "load_taifex_opening_atm_straddles",
    "load_taifex_option_pair_day_session",
    "load_taifex_weekly_atm_straddles",
    "option_scope_slot_count",
    "option_slot_index",
    "option_slot_labels",
]
