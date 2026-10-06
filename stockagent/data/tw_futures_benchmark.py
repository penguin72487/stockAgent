"""Independent, hash-pinned TX reference; never add benchmark rows to the action universe."""
from __future__ import annotations
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file


def resolve_tx_benchmark_path(trading) -> Path:
    path = Path(trading.tw_futures_portfolio_benchmark_data_path or trading.tw_futures_portfolio_data_path)
    expected = trading.tw_futures_portfolio_benchmark_sha256
    if expected is not None and sha256_file(path) != expected:
        raise ValueError("TX benchmark source SHA-256 mismatch")
    return path


def load_tx_front_rolling_benchmark(
    data_path: str | Path,
    panel_dates: np.ndarray,
) -> dict[str, np.ndarray]:
    """Gross 1x TX front-month close-to-close return with a prior-close roll.

    On a roll, use the *new* front contract's own previous TX-session close.
    A missing/unobserved mark is an error, not a zero return or a calendar-spread
    jump. Dates preceding source inception are context only and receive zero.
    """

    dates = np.asarray(panel_dates, dtype="datetime64[D]")
    if dates.ndim != 1 or not dates.size or np.isnat(dates).any() or np.any(dates[1:] <= dates[:-1]):
        raise ValueError("TX benchmark panel dates must be finite and increasing")
    table = pq.read_table(
        data_path,
        columns=["date", "product", "contract", "tenor_rank", "close", "source_row_observed"],
        filters=[("product", "==", "TX"), ("date", "<=", date.fromisoformat(str(dates[-1])))],
        memory_map=True,
    )
    tx = pl.from_arrow(table).sort(["date", "contract"])
    if not tx.height:
        raise ValueError("TX benchmark has no source rows")
    source_dates = np.unique(tx["date"].to_numpy().astype("datetime64[D]"))
    front = tx.filter(pl.col("tenor_rank") == 1).sort("date")
    front_dates = front["date"].to_numpy().astype("datetime64[D]")
    if front_dates.size != source_dates.size or not np.array_equal(front_dates, source_dates):
        raise ValueError("TX benchmark requires exactly one front contract per source session")
    if dates[-1] > source_dates[-1]:
        raise ValueError(
            f"TX benchmark source ends {source_dates[-1]} before panel {dates[-1]}; "
            "trim the panel to the verified source first"
        )
    covered = dates >= source_dates[0]
    missing = np.setdiff1d(dates[covered], front_dates)
    if missing.size:
        raise ValueError(f"TX benchmark is missing a panel session: {missing[0]}")
    all_dates = tx["date"].to_numpy().astype("datetime64[D]")
    contracts = tx["contract"].to_numpy().astype(str)
    closes = tx["close"].to_numpy().astype(np.float64)
    observed = tx["source_row_observed"].to_numpy().astype(bool)
    marks: dict[tuple[np.datetime64, str], tuple[float, bool]] = {}
    for day, contract, close, is_observed in zip(all_dates, contracts, closes, observed):
        key = (day, contract)
        if key in marks or not np.isfinite(close) or close <= 0:
            raise ValueError(f"TX benchmark has duplicate or invalid close: {key}")
        marks[key] = (float(close), bool(is_observed))
    front_contracts = front["contract"].to_numpy().astype(str)
    count = len(dates)
    result: dict[str, np.ndarray] = {
        "dates": dates,
        "benchmark_log_returns": np.zeros(count, dtype=np.float32),
        "contract_months": np.full(count, "", dtype="U16"),
        "front_month_roll_mask": np.zeros(count, dtype=bool),
        "front_month_close": np.full(count, np.nan, dtype=np.float64),
        "prior_same_contract_close": np.full(count, np.nan, dtype=np.float64),
        "source_covered": covered,
    }
    for index, (day, contract) in enumerate(zip(front_dates, front_contracts)):
        if day < dates[0]:
            continue
        position = int(np.searchsorted(dates, day))
        if position == count or dates[position] != day:
            continue
        current_close, current_observed = marks[(day, contract)]
        if not current_observed:
            raise ValueError(f"TX front close was not observed on {day}")
        result["contract_months"][position] = contract
        result["front_month_close"][position] = current_close
        if index == 0:
            continue  # benchmark inception has no preceding source session
        prior_day = front_dates[index - 1]
        prior_mark = marks.get((prior_day, contract))
        if prior_mark is None or not prior_mark[1]:
            raise ValueError(f"TX {contract} lacks its observed own close on {prior_day}")
        prior_close = prior_mark[0]
        result["prior_same_contract_close"][position] = prior_close
        result["front_month_roll_mask"][position] = contract != front_contracts[index - 1]
        result["benchmark_log_returns"][position] = np.float32(np.log(current_close / prior_close))
    return result


def tx_front_benchmark_source_end(data_path: str | Path) -> np.datetime64:
    """Last observed front-month TX session in the pinned futures release."""

    table = pq.read_table(
        data_path,
        columns=["date"],
        filters=[("product", "==", "TX"), ("tenor_rank", "==", 1),
                 ("source_row_observed", "==", True)],
        memory_map=True,
    )
    if not table.num_rows:
        raise ValueError("verified TX front-month benchmark source is empty")
    return np.max(np.asarray(table["date"].to_numpy(), dtype="datetime64[D]"))
