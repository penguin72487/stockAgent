"""Strict all-futures adapter for the canonical scheduled minute executor."""
from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path
import json

import numpy as np
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_stock_futures_minute import (
    BAR_FIELDS, EVENT_MINUTES, TAPE_FIELDS, validate_futures_minute_data,
)
from stockagent.research.taifex_transaction_tax import stock_index_futures_tax_rate

def select_intraday_contracts(frame: pl.DataFrame) -> pl.DataFrame:
    """Calendar-front contracts; no same-session volume/return selection."""
    return frame.filter(
        (pl.col("tenor_rank") == 1)
        & pl.col("same_contract_as_previous_session")
        & pl.col("fixed_fee_research_supported")
        & pl.col("asset_class").is_in(["stock_future", "etf_future", "index_future"])
    )


def validate_all_futures_intraday_data(
    daily_path: str | Path, minute_path: str | Path, *, participation: float,
    dates: np.ndarray | None = None, start_date: str | None = None,
):
    """Use canonical minute receipts; add only all-futures universe coverage."""
    path = Path(minute_path)
    manifest = json.loads(path.with_name("manifest.json").read_text())
    digest = sha256_file(Path(daily_path))
    if manifest.get("source_daily_sha256") != digest:
        raise ValueError("intraday source belongs to a different daily release")
    coverage_path = path.with_name("coverage.parquet")
    if manifest["outputs"]["coverage"]["sha256"] != sha256_file(coverage_path):
        raise ValueError("intraday coverage SHA mismatch")
    if not np.isfinite(participation) or not 0 < participation <= 1:
        raise ValueError("minute participation must be in (0,1]")
    source = pl.read_parquet(daily_path)
    if dates is not None:
        source = source.filter(pl.col("date").is_in(np.asarray(dates, dtype="datetime64[D]").tolist()))
    if start_date is not None:
        source = source.filter(pl.col("date") >= date.fromisoformat(start_date))
    selected = select_intraday_contracts(source)
    keys = ["date", "physical_contract"]
    coverage = pl.read_parquet(coverage_path)
    if coverage.select(keys).is_duplicated().any():
        raise ValueError("duplicate physical-contract coverage keys")
    accepted = coverage.filter(pl.col("status").is_in(
        ["minute_verified", "official_no_outright_trades", "official_subcontract_capacity"]
    ))
    missing = selected.join(accepted.select(keys), on=keys, how="anti")
    if missing.height:
        by_class = missing.group_by("asset_class").len().sort("asset_class").to_dicts()
        raise ValueError(f"intraday minute source misses {missing.height} physical contract-days: {by_class}")
    bars, _ = validate_futures_minute_data(
        path, daily_sha256=digest,
        dates=np.asarray(source["date"].unique().to_list(), dtype="datetime64[D]"),
        daily_proxy_before=manifest.get("daily_proxy_before"),
        participation=participation, capacity_rounding="floor",
        quarantine_dates=manifest.get("quarantined_dates", []),
        quarantine_contract_days=manifest.get("quarantined_contract_days", []),
    )
    if bars.filter(pl.col("volume") != pl.col("volume").floor()).height:
        raise ValueError("futures volume must use whole contract quantities")
    return source, selected, bars


def attach_all_futures_intraday(panel, minute_path: str | Path, *, participation: float):
    daily = panel.stock_context_futures_portfolio_daily
    if daily is None or daily.integer_execution is None:
        raise ValueError("all-futures intraday requires the exact futures sidecar")
    # Canonical stock panels store datetime64[ns]; source identities are
    # trading dates. Normalize the join key without changing the panel axis.
    panel_dates = np.asarray(panel.dates, dtype="datetime64[D]")
    source, selected, bars = validate_all_futures_intraday_data(
        daily.source_path, minute_path, participation=participation, dates=panel_dates,
    )
    keys = ["date", "physical_contract"]
    dates = {str(d): i for i, d in enumerate(panel_dates)}
    slots = {s: i for i, s in enumerate(daily.symbols)}
    shape = daily.candidate_mask.shape
    # Slot 0 owns one physical contract. Slot 1 stays unavailable. This reuses
    # the existing standard/mini basket without inventing a second instrument.
    tape = np.zeros((*shape, 2, TAPE_FIELDS), dtype=np.float32)
    eligible = np.zeros(shape, dtype=bool)
    for row in selected.iter_rows(named=True):
        d, s = dates[str(row["date"])], slots[row["symbol"]]
        eligible[d, s] = daily.candidate_mask[d, s]
        tape[d, s, 0, :3] = (
            row["contract_multiplier"], daily.integer_execution[d, s, 5],
            stock_index_futures_tax_rate(row["date"]),
        )
    event_offsets = {m: 3 + i * len(BAR_FIELDS) for i, m in enumerate(EVENT_MINUTES)}
    joined = bars.join(selected.select(*keys, "symbol"), on=keys, how="inner", validate="m:1")
    for row in joined.iter_rows(named=True):
        d, s = dates[str(row["date"])], slots[row["symbol"]]
        offset = event_offsets[row["minute"]]
        tape[d, s, 0, offset:offset + 5] = (
            row["vwap"], row["high"], row["low"], row["close"],
            np.floor(row["volume"] * participation),
        )
    panel.stock_context_futures_portfolio_daily = replace(
        daily, intraday_execution=tape, candidate_mask=eligible,
        intraday_session_mask=np.isin(
            panel_dates,
            np.asarray(source["date"].unique().to_list(), dtype="datetime64[D]"),
        ),
        must_liquidate_mask=eligible.copy(), can_hold_overnight_mask=np.zeros(shape, dtype=bool),
        executable_mask=tape[:, :, 0, 7] > 0,
        # The daily sidecar's TX benchmark carries overnight. Intraday uses
        # explicit flat TWD cash rather than mixing incompatible hold clocks.
        benchmark_log_returns=np.zeros(len(panel.dates), dtype=np.float32),
    )
    return panel
