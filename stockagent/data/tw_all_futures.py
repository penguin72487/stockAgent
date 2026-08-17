"""Causal all-product TAIFEX regular and after-hours futures context.

This module is deliberately input-only.  Official daily files provide prices
and volume for hundreds of futures roots but do not contain the historical
contract multiplier, adjusted contract unit, or broker fee schedule required
for an exact cash ledger.  Every root can therefore inform the joint model,
while only products with a separately verified execution specification may be
traded by the canonical integer executor.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import csv
from datetime import date
from pathlib import Path
import re
from typing import Final

import numpy as np

from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM,
    iter_taifex_daily_csv_streams,
    parse_taifex_daily_price,
    parse_taifex_daily_volume,
    parse_taifex_trading_date,
)


TAIFEX_ALL_FUTURES_CONTEXT_CONTRACT_VERSION: Final[int] = 2
_OUTRIGHT_SERIES_RE = re.compile(r"^(?P<month>[0-9]{6})(?:W(?P<week>[1-5]))?$")
_REGULAR_SESSION_ALIASES: Final[frozenset[str]] = frozenset(
    {"一般", "一般交易時段", "day", "day_session", "regular"}
)
_AFTERHOURS_SESSION_ALIASES: Final[frozenset[str]] = frozenset(
    {"盤後", "盤後交易時段", "night", "night_session", "afterhours", "after_hours"}
)


def _normalize_session_kind(value: str) -> str:
    normalized = str(value).strip().lower().replace("-", "_")
    if normalized in _REGULAR_SESSION_ALIASES:
        return "regular"
    if normalized in _AFTERHOURS_SESSION_ALIASES:
        return "afterhours"
    raise ValueError(f"unsupported TAIFEX session kind: {value!r}")


def _row_session_kind(value: str) -> str | None:
    normalized = str(value).strip().lower().replace("-", "_")
    if normalized in _REGULAR_SESSION_ALIASES:
        return "regular"
    if normalized in _AFTERHOURS_SESSION_ALIASES:
        return "afterhours"
    return None


def _series_sort_key(series: str) -> tuple[int, int]:
    match = _OUTRIGHT_SERIES_RE.fullmatch(series)
    if match is None:
        raise ValueError(f"not an outright futures series: {series!r}")
    month = int(match.group("month"))
    week_raw = match.group("week")
    # Monthly contracts expire on the third Wednesday. Weekly products omit
    # that collision, so this ordering is causal and stable without guessing
    # a concrete holiday-adjusted expiry date.
    within_month = 3 if week_raw is None else int(week_raw)
    return month, within_month


def build_taifex_all_futures_front_panel(
    source_paths: Iterable[str | Path],
    output_path: str | Path,
    *,
    session: str = "regular",
) -> Path:
    """Normalize the first valid outright contract for every date/root."""

    session_kind = _normalize_session_kind(session)
    return build_taifex_all_futures_front_panels(
        source_paths,
        {session_kind: output_path},
    )[session_kind]


def build_taifex_all_futures_front_panels(
    source_paths: Iterable[str | Path],
    output_paths: Mapping[str, str | Path],
) -> dict[str, Path]:
    """Build one or more session panels while scanning every raw file once."""

    import pyarrow as pa
    import pyarrow.parquet as pq

    sources = [Path(value).expanduser().resolve() for value in source_paths]
    if not sources:
        raise ValueError("source_paths must not be empty")
    missing = [str(path) for path in sources if not path.exists()]
    if missing:
        raise FileNotFoundError("missing TAIFEX source paths: " + ", ".join(missing))
    outputs: dict[str, Path] = {}
    for raw_session, raw_path in output_paths.items():
        session_kind = _normalize_session_kind(raw_session)
        if session_kind in outputs:
            raise ValueError(f"duplicate output for TAIFEX session {session_kind}")
        output = Path(raw_path).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        outputs[session_kind] = output
    if not outputs:
        raise ValueError("output_paths must not be empty")
    if len(set(outputs.values())) != len(outputs):
        raise ValueError("TAIFEX session outputs must use distinct paths")

    fields = [
        ("date", pa.date32()),
        ("root", pa.string()),
        ("series", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
        ("log_return", pa.float64()),
        ("source_file", pa.string()),
    ]
    schemas = {
        session_kind: pa.schema(
            fields,
            metadata={
                b"stockagent.dataset": (
                    f"taifex_all_futures_{session_kind}_front".encode("ascii")
                ),
                b"stockagent.contract_version": str(
                    TAIFEX_ALL_FUTURES_CONTEXT_CONTRACT_VERSION
                ).encode("ascii"),
                b"stockagent.scope": (
                    f"{session_kind}_session_valid_outright_front_per_root".encode(
                        "ascii"
                    )
                ),
            },
        )
        for session_kind in outputs
    }
    writers = {
        session_kind: pq.ParquetWriter(
            outputs[session_kind], schemas[session_kind], compression="zstd"
        )
        for session_kind in outputs
    }
    try:
        for path in sources:
            best_by_session: dict[
                str,
                dict[
                    tuple[np.datetime64, str],
                    tuple[tuple[int, int], dict[str, object]],
                ],
            ] = {session_kind: {} for session_kind in outputs}
            for stream, source_name, _source_sha in iter_taifex_daily_csv_streams(path):
                for raw in csv.DictReader(stream):
                    row = {
                        str(key).strip(): "" if value is None else str(value).strip()
                        for key, value in raw.items()
                    }
                    session_kind = _row_session_kind(
                        row.get("交易時段", "一般") or "一般"
                    )
                    if session_kind not in outputs:
                        continue
                    root = row.get("契約", "").upper()
                    series = row.get("到期月份(週別)", "").upper()
                    if not root or _OUTRIGHT_SERIES_RE.fullmatch(series) is None:
                        continue
                    trading_date = parse_taifex_trading_date(row.get("交易日期", ""))
                    if trading_date is None:
                        continue
                    open_price = parse_taifex_daily_price(row.get("開盤價", ""))
                    high_price = parse_taifex_daily_price(row.get("最高價", ""))
                    low_price = parse_taifex_daily_price(row.get("最低價", ""))
                    close_price = parse_taifex_daily_price(row.get("收盤價", ""))
                    volume = parse_taifex_daily_volume(row.get("成交量", ""))
                    if not (
                        np.isfinite(open_price)
                        and open_price > 0.0
                        and np.isfinite(high_price)
                        and high_price > 0.0
                        and np.isfinite(low_price)
                        and low_price > 0.0
                        and np.isfinite(close_price)
                        and close_price > 0.0
                        and volume > 0
                    ):
                        continue
                    key = (trading_date, root)
                    sort_key = _series_sort_key(series)
                    payload = {
                        "date": date.fromisoformat(str(trading_date)),
                        "root": root,
                        "series": series,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                        "log_return": float(np.log(close_price / open_price)),
                        "source_file": source_name,
                    }
                    best = best_by_session[session_kind]
                    prior = best.get(key)
                    if prior is None or sort_key < prior[0]:
                        best[key] = (sort_key, payload)
                    elif sort_key == prior[0] and payload != prior[1]:
                        raise ValueError(
                            f"conflicting TAIFEX all-futures row {trading_date}/{root}/{series}"
                        )
            for session_kind, best in best_by_session.items():
                rows = [value[1] for _, value in sorted(best.items())]
                if rows:
                    writers[session_kind].write_table(
                        pa.Table.from_pylist(rows, schema=schemas[session_kind])
                    )
    finally:
        for writer in writers.values():
            writer.close()
    return outputs


def _rolling_moments(
    values: np.ndarray,
    valid: np.ndarray,
    window: int,
) -> tuple[np.ndarray, np.ndarray]:
    masked = np.where(valid, values, 0.0)
    counts = np.cumsum(valid.astype(np.int64), axis=0)
    sums = np.cumsum(masked, axis=0)
    squares = np.cumsum(np.square(masked), axis=0)
    if window < values.shape[0]:
        counts[window:] -= np.cumsum(valid.astype(np.int64), axis=0)[:-window]
        sums[window:] -= np.cumsum(masked, axis=0)[:-window]
        squares[window:] -= np.cumsum(np.square(masked), axis=0)[:-window]
    mean = np.divide(
        sums,
        counts,
        out=np.zeros_like(sums),
        where=counts > 0,
    )
    second = np.divide(
        squares,
        counts,
        out=np.zeros_like(squares),
        where=counts > 0,
    )
    return mean, np.sqrt(np.maximum(second - np.square(mean), 0.0))


def _load_taifex_all_futures_front_context(
    path: str | Path,
    *,
    panel_dates: Sequence[object] | np.ndarray,
    shift_one_session: bool,
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    """Load all roots under an explicit exchange-session availability clock."""

    import pyarrow.parquet as pq

    source = Path(path).expanduser().resolve()
    table = pq.read_table(
        source,
        columns=["date", "root", "open", "high", "low", "close", "volume", "log_return"],
    )
    payload = table.to_pydict()
    source_dates = np.asarray(payload["date"], dtype="datetime64[D]")
    dates = np.asarray(panel_dates, dtype="datetime64[D]")
    if dates.ndim != 1 or dates.size == 0:
        raise ValueError("panel_dates must be a non-empty one-dimensional array")
    in_range = (source_dates >= dates[0]) & (source_dates <= dates[-1])
    roots = tuple(sorted({str(payload["root"][i]).strip().upper() for i in np.flatnonzero(in_range)}))
    if not roots:
        raise ValueError("all-futures context has no roots in the panel date range")
    rows, columns = int(dates.size), len(roots)
    date_index = {value: index for index, value in enumerate(dates)}
    root_index = {value: index for index, value in enumerate(roots)}
    opens = np.full((rows, columns), np.nan, dtype=np.float64)
    highs = np.full_like(opens, np.nan)
    lows = np.full_like(opens, np.nan)
    closes = np.full_like(opens, np.nan)
    volumes = np.zeros((rows, columns), dtype=np.float64)
    log_returns = np.full_like(opens, np.nan)
    seen: set[tuple[int, int]] = set()
    for source_row in np.flatnonzero(in_range):
        date_value = source_dates[source_row]
        row = date_index.get(date_value)
        root = str(payload["root"][source_row]).strip().upper()
        column = root_index.get(root)
        if row is None or column is None:
            continue
        key = (row, column)
        if key in seen:
            raise ValueError(f"duplicate all-futures context row {date_value}/{root}")
        seen.add(key)
        opens[key] = float(payload["open"][source_row])
        highs[key] = float(payload["high"][source_row])
        lows[key] = float(payload["low"][source_row])
        closes[key] = float(payload["close"][source_row])
        volumes[key] = float(payload["volume"][source_row])
        log_returns[key] = float(payload["log_return"][source_row])
    valid = (
        np.isfinite(opens)
        & (opens > 0.0)
        & np.isfinite(highs)
        & (highs > 0.0)
        & np.isfinite(lows)
        & (lows > 0.0)
        & np.isfinite(closes)
        & (closes > 0.0)
        & np.isfinite(log_returns)
        & (volumes > 0.0)
    )
    safe_open = np.where(valid, opens, 1.0)
    safe_high = np.where(valid, highs, safe_open)
    safe_low = np.where(valid, lows, safe_open)
    safe_close = np.where(valid, closes, safe_open)
    clean_return = np.where(valid, log_returns, 0.0)
    price_range = np.maximum(safe_high - safe_low, 0.0)
    mean_5, vol_5 = _rolling_moments(clean_return, valid, 5)
    mean_20, vol_20 = _rolling_moments(clean_return, valid, 20)
    raw = np.stack(
        (
            clean_return,
            np.log(safe_high / safe_open),
            np.log(safe_low / safe_open),
            np.log(safe_high / safe_low),
            np.where(
                price_range > 0.0,
                2.0 * (safe_close - safe_low) / np.maximum(price_range, 1e-12) - 1.0,
                0.0,
            ),
            np.log1p(np.where(valid, volumes, 0.0)) / 20.0,
            np.log1p(safe_open * np.where(valid, volumes, 0.0)) / 30.0,
            mean_5,
            vol_5,
            mean_20,
            vol_20,
            np.zeros_like(clean_return),
            np.zeros_like(clean_return),
        ),
        axis=-1,
    )
    raw = np.where(valid[..., None], raw, 0.0)
    if shift_one_session:
        context = np.zeros(
            (rows, columns, TAIFEX_INDEX_FUTURES_CONTEXT_FEATURE_DIM),
            dtype=np.float32,
        )
        context_mask = np.zeros((rows, columns), dtype=bool)
        if rows > 1:
            context[1:] = raw[:-1].astype(np.float32, copy=False)
            context_mask[1:] = valid[:-1]
    else:
        context = raw.astype(np.float32, copy=False)
        context_mask = valid
    return context, context_mask, roots


def load_taifex_all_futures_front_context(
    path: str | Path,
    *,
    panel_dates: Sequence[object] | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    """Return completed general-session features on the next panel session."""

    return _load_taifex_all_futures_front_context(
        path,
        panel_dates=panel_dates,
        shift_one_session=True,
    )


def load_taifex_all_futures_afterhours_context(
    path: str | Path,
    *,
    panel_dates: Sequence[object] | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    """Return after-hours features at their attributed day-session open.

    TAIFEX labels an after-hours row by its volume-attribution date.  That
    session ends at 05:00 and belongs to the general session that opens at
    08:45 on the same labelled date, so shifting it again would add a false
    one-session delay.
    """

    return _load_taifex_all_futures_front_context(
        path,
        panel_dates=panel_dates,
        shift_one_session=False,
    )


__all__ = [
    "TAIFEX_ALL_FUTURES_CONTEXT_CONTRACT_VERSION",
    "build_taifex_all_futures_front_panel",
    "build_taifex_all_futures_front_panels",
    "load_taifex_all_futures_afterhours_context",
    "load_taifex_all_futures_front_context",
]
