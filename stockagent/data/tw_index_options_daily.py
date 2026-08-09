"""Official TAIFEX daily TXO rows normalized to opening-ATM pairs.

The official daily file has one row per date/series/strike/right/session.  Its
``open`` and ``close`` fields are the first and last transaction prices of each
leg, not simultaneous executable quotes.  This module preserves that boundary:
it uses the official front-month TX day-session open only to choose the ATM
strike, retains the two option-leg daily fields, and records unexecutable days
instead of silently selecting a different strike using full-day liquidity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import csv
import math
from pathlib import Path
import re
from typing import Final, Iterable, Literal, Mapping

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
    series_scope: TaifexOptionSeriesScope,
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
            if not _series_matches(series, series_scope):
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


__all__ = [
    "TAIFEX_OPTIONS_DAILY_DATA_CONTRACT_VERSION",
    "TAIFEX_OPTIONS_DAILY_PRICE_SOURCE",
    "TAIFEX_OPTION_SERIES_SCOPES",
    "TAIFEX_TXO_MULTIPLIER",
    "TAIFEX_TXO_PRODUCT",
    "TaifexOptionSeriesScope",
    "build_taifex_monthly_atm_straddles",
    "build_taifex_opening_atm_straddles",
    "build_taifex_weekly_atm_straddles",
    "load_taifex_monthly_atm_straddles",
    "load_taifex_option_daily_contract_rows",
    "load_taifex_opening_atm_straddles",
    "load_taifex_weekly_atm_straddles",
]
