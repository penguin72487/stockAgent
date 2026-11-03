#!/usr/bin/env python3
"""Diagnose the configured futures attachment without authorizing training.

The purpose is to expose integration errors behind an earlier account-calendar
admission failure. This intentionally does not call the admission wrapper and
must never be used as evidence that any calendar, source or training is ready.
The market axes come from the existing panel cache; the account-only nodes come
from the configured daily source, including any omissions it may contain.
"""
from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import sys
import time

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.config import load_config  # noqa: E402
from stockagent.data.tw_futures_account_attachment import _AttachmentAxes  # noqa: E402
from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar  # noqa: E402
from stockagent.data.tw_futures_margin import CAN_BUY, CAN_SELL  # noqa: E402
from stockagent.data.tw_stock_context_futures_portfolio import (  # noqa: E402
    attach_configured_stock_context_futures_portfolio,
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def entry_capacity_gates(candidate, executable, execution) -> tuple[np.ndarray, np.ndarray]:
    """Expose necessary one-contract gates, not a promise of an order fill."""
    candidate = np.asarray(candidate)
    executable = np.asarray(executable)
    execution = np.asarray(execution)
    if (candidate.ndim != 2 or executable.shape != candidate.shape
            or candidate.dtype != np.bool_ or executable.dtype != np.bool_
            or execution.ndim != 3 or execution.shape[:2] != candidate.shape
            or execution.shape[2] <= max(8, CAN_BUY, CAN_SELL)):
        raise ValueError("entry gates require Boolean masks and the matching margin execution tape")
    joint = candidate & executable
    capacity = execution[..., 8]
    capacity_gate = joint & np.isfinite(capacity) & (capacity >= 1)
    direction_gate = capacity_gate & ((execution[..., CAN_BUY] > 0.5)
                                      | (execution[..., CAN_SELL] > 0.5))
    return capacity_gate, direction_gate


def summarize_masks(calendar, candidate, executable, capacity_gate, direction_gate) -> dict:
    """Separate decision eligibility from execution, including their overlap.

    A slot can be executable without being a new-entry candidate (for example,
    when existing inventory requires attention). The count of either mask alone
    therefore cannot establish the fraction of candidates that can execute.
    """
    candidate = np.asarray(candidate)
    executable = np.asarray(executable)
    capacity_gate = np.asarray(capacity_gate)
    direction_gate = np.asarray(direction_gate)
    dates = np.asarray(calendar.dates, dtype="datetime64[D]")
    account_only = np.asarray(calendar.account_only_mask)
    if (candidate.ndim != 2 or candidate.shape != executable.shape
            or candidate.shape[0] != len(dates)
            or candidate.dtype != np.bool_ or executable.dtype != np.bool_
            or capacity_gate.shape != candidate.shape or direction_gate.shape != candidate.shape
            or capacity_gate.dtype != np.bool_ or direction_gate.dtype != np.bool_
            or account_only.shape != (len(dates),) or account_only.dtype != np.bool_):
        raise ValueError("diagnostic masks and account calendar must share a Boolean [date, slot] axis")
    if np.any(candidate[account_only]) or np.any(executable[account_only]):
        raise ValueError("account-only dates cannot be decision or execution candidates")
    joint = candidate & executable
    if np.any(capacity_gate & ~joint) or np.any(direction_gate & ~capacity_gate):
        raise ValueError("capacity and direction gates must be nested within the joint mask")
    market = ~account_only
    years = dates.astype("datetime64[Y]").astype(np.int64) + 1970
    per_year = []
    for year in np.unique(years[market]):
        selected = market & (years == year)
        per_year.append({
            "year": int(year),
            "market_sessions": int(np.count_nonzero(selected)),
            "candidate_positions": int(np.count_nonzero(candidate[selected])),
            "executable_positions": int(np.count_nonzero(executable[selected])),
            "candidate_and_executable_positions": int(np.count_nonzero(joint[selected])),
            "candidate_executable_one_contract_capacity_positions": int(np.count_nonzero(capacity_gate[selected])),
            "candidate_executable_one_contract_capacity_any_direction_positions": int(np.count_nonzero(direction_gate[selected])),
            "market_sessions_without_candidate_and_executable": int(
                np.count_nonzero(~np.any(joint[selected], axis=1))),
        })
    market_daily = np.count_nonzero(joint[market], axis=1)
    direction_daily = np.count_nonzero(direction_gate[market], axis=1)
    return {
        "candidate_and_executable_positions": int(np.count_nonzero(joint)),
        "candidate_executable_one_contract_capacity_positions": int(np.count_nonzero(capacity_gate)),
        "candidate_executable_one_contract_capacity_any_direction_positions": int(np.count_nonzero(direction_gate)),
        "joint_without_one_contract_capacity_positions": int(np.count_nonzero(joint & ~capacity_gate)),
        "capacity_without_any_direction_positions": int(np.count_nonzero(capacity_gate & ~direction_gate)),
        "candidate_without_executable_positions": int(np.count_nonzero(candidate & ~executable)),
        "executable_without_candidate_positions": int(np.count_nonzero(executable & ~candidate)),
        "market_sessions_without_candidate_and_executable": int(np.count_nonzero(market_daily == 0)),
        "joint_positions_per_market_session": {
            "minimum": int(np.min(market_daily)),
            "median": float(np.median(market_daily)),
            "maximum": int(np.max(market_daily)),
        },
        "necessary_entry_gates_per_market_session": {
            "minimum": int(np.min(direction_daily)),
            "median": float(np.median(direction_daily)),
            "maximum": int(np.max(direction_daily)),
            "sessions_without_any": int(np.count_nonzero(direction_daily == 0)),
        },
        "per_calendar_year": per_year,
    }


def summarize_products(calendar, candidate, executable, direction_gate, source: pl.DataFrame) -> dict:
    """Map dated fixed slots back to source products before counting the union."""
    dates = np.asarray(calendar.dates, dtype="datetime64[D]")
    if source.select(pl.any_horizontal(pl.all().is_null()).any()).item():
        raise ValueError("source product mapping has missing identity fields")
    source_dates = source["date"].to_numpy().astype("datetime64[D]")
    di = np.searchsorted(dates, source_dates)
    slots = source["symbol"].str.extract(r"^TAIFEX_SLOT_(\d+)$", 1).cast(pl.Int64)
    if slots.null_count():
        raise ValueError("source product mapping has an invalid slot label")
    si = slots.to_numpy() - 1
    if (np.any(di >= len(dates)) or not np.array_equal(dates[di], source_dates)
            or np.any(si < 0) or np.any(si >= candidate.shape[1])):
        raise ValueError("source product mapping lies outside the diagnostic axis")
    if np.unique(di * candidate.shape[1] + si).size != source.height:
        raise ValueError("source product mapping has duplicate date/slot identities")
    mapped_candidate = candidate[di, si]
    mapped_executable = executable[di, si]
    mapped_direction_gate = direction_gate[di, si]
    if (np.count_nonzero(mapped_candidate) != np.count_nonzero(candidate)
            or np.count_nonzero(mapped_executable) != np.count_nonzero(executable)):
        raise ValueError("source product mapping does not cover all flagged date/slots")
    mapped = source.with_columns(
        pl.Series("candidate_flag", mapped_candidate),
        pl.Series("executable_flag", mapped_executable),
        pl.Series("joint_flag", mapped_candidate & mapped_executable),
        pl.Series("necessary_entry_gate_flag", mapped_direction_gate),
    )
    summary = mapped.group_by("asset_class").agg(
        pl.col("product").n_unique().alias("products_with_rows"),
        pl.col("product").filter(pl.col("candidate_flag")).n_unique().alias("products_with_candidate"),
        pl.col("product").filter(pl.col("executable_flag")).n_unique().alias("products_with_executable"),
        pl.col("product").filter(pl.col("joint_flag")).n_unique().alias("products_with_candidate_and_executable"),
        pl.col("joint_flag").sum().alias("candidate_and_executable_positions"),
        pl.col("product").filter(pl.col("necessary_entry_gate_flag")).n_unique().alias("products_with_necessary_entry_gates"),
        pl.col("necessary_entry_gate_flag").sum().alias("necessary_entry_gate_positions"),
    ).sort("asset_class")
    products = mapped.group_by("asset_class", "product").agg(
        pl.col("daily_executable").sum().alias("daily_executable_positions"),
        pl.col("candidate_flag").sum().alias("candidate_positions"),
        pl.col("executable_flag").sum().alias("clock_executable_positions"),
        pl.col("joint_flag").sum().alias("candidate_and_executable_positions"),
        pl.col("necessary_entry_gate_flag").sum().alias("necessary_entry_gate_positions"),
        pl.col("date").filter(pl.col("daily_executable")).min().dt.to_string("%Y-%m-%d")
          .alias("first_daily_executable_date"),
        pl.col("date").filter(pl.col("executable_flag")).min().dt.to_string("%Y-%m-%d")
          .alias("first_clock_executable_date"),
        pl.col("status").filter(pl.col("daily_executable")).unique().sort()
          .alias("clock_statuses_on_daily_executable_dates"),
    )
    missing = products.filter((pl.col("daily_executable_positions") > 0)
                              & (pl.col("candidate_and_executable_positions") == 0))
    missing = missing.sort("asset_class", "product")
    missing_entry_gates = products.filter((pl.col("daily_executable_positions") > 0)
                                          & (pl.col("necessary_entry_gate_positions") == 0))
    return {
        "mapped_source_rows": source.height,
        "products_with_candidate_and_executable": mapped.filter(pl.col("joint_flag"))["product"].n_unique(),
        "products_with_necessary_entry_gates": mapped.filter(pl.col("necessary_entry_gate_flag"))["product"].n_unique(),
        "by_asset_class": summary.to_dicts(),
        "products_with_daily_executable_but_no_candidate_and_clock_executable": missing.to_dicts(),
        "products_with_daily_executable_but_no_necessary_entry_gates": missing_entry_gates.sort("asset_class", "product").to_dicts(),
    }


def audit(config_path: Path, panel_generation: Path) -> dict:
    started = time.monotonic()
    config = load_config(config_path)
    clock_path = Path(config.trading.tw_futures_portfolio_entry_clock_path)
    clock_manifest = json.loads(clock_path.with_name("manifest.json").read_text())
    start = np.datetime64(clock_manifest["start_date"], "D")
    end = np.datetime64(clock_manifest["end_date"], "D")
    dates_path = panel_generation / "dates.npy"
    symbols_path = panel_generation / "symbols.json"
    panel_dates = np.asarray(np.load(dates_path, allow_pickle=False), dtype="datetime64[D]")
    dates = panel_dates[(panel_dates >= start) & (panel_dates <= end)]
    symbols = json.loads(symbols_path.read_text())
    if not isinstance(symbols, list) or not symbols or not all(isinstance(s, str) for s in symbols):
        raise ValueError("stock panel symbol axis is invalid")
    source_path = Path(config.trading.tw_futures_portfolio_data_path)
    days = (pl.scan_parquet(source_path)
            .select("date", "accounting_only_date").unique().sort("date").collect())
    if days["date"].is_duplicated().any() or days["accounting_only_date"].null_count():
        raise ValueError("source has ambiguous accounting calendar rows")
    calendar = build_futures_account_calendar(
        dates, days["date"].to_numpy(), days["accounting_only_date"].to_numpy(),
    )
    panel = _AttachmentAxes(dates=calendar.dates, symbols=symbols)
    panel = attach_configured_stock_context_futures_portfolio(
        panel, config, futures_account_calendar=calendar,
    )
    daily = panel.stock_context_futures_portfolio_daily
    if daily is None or daily.integer_execution is None:
        raise ValueError("configured attachment did not return its integer execution tape")
    if not np.array_equal(np.asarray(daily.dates, dtype="datetime64[D]"), calendar.dates):
        raise ValueError("diagnostic output and account calendar date axes do not match")
    capacity_gate, direction_gate = entry_capacity_gates(
        daily.candidate_mask, daily.executable_mask, daily.integer_execution)
    overlap = summarize_masks(calendar, daily.candidate_mask, daily.executable_mask,
                              capacity_gate, direction_gate)
    source_products = (pl.scan_parquet(source_path)
                       .filter(pl.col("date").is_between(date.fromisoformat(str(dates[0])),
                                                         date.fromisoformat(str(dates[-1]))))
                       .select("date", "symbol", "product", "asset_class",
                               pl.col("executable").alias("daily_executable"))
                       .join(pl.scan_parquet(clock_path).select("date", "symbol", "status"),
                             on=["date", "symbol"], how="left", validate="1:1").collect())
    product_overlap = summarize_products(calendar, daily.candidate_mask, daily.executable_mask,
                                         direction_gate, source_products)
    return {
        "contract": "tw_futures_core_attachment_diagnostic_v4",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "configuration": str(config_path),
        "configuration_sha256": sha256(config_path),
        "panel_dates_sha256": sha256(dates_path),
        "panel_symbols_sha256": sha256(symbols_path),
        "source_clock": str(clock_path),
        "source_clock_manifest_sha256": sha256(clock_path.with_name("manifest.json")),
        "market_sessions": int(dates.size),
        "account_only_nodes_present_in_source": int(calendar.account_only_indices.size),
        "first_market_session": str(dates[0]),
        "last_market_session": str(dates[-1]),
        "stock_symbols": len(symbols),
        "candidate_shape": list(daily.candidate_mask.shape),
        "candidate_positions": int(np.count_nonzero(daily.candidate_mask)),
        "executable_positions": int(np.count_nonzero(daily.executable_mask)),
        "mask_overlap": overlap,
        "dated_product_mapping": product_overlap,
        "integer_execution_shape": list(daily.integer_execution.shape),
        "attachment_completed": True,
        "holiday_calendar_reviewed": False,
        "financial_sources_admitted": False,
        "training_admitted": False,
        "training_started": False,
        "limitations": [
            "The training admission wrapper was intentionally not called for this diagnostic.",
            "The supplied source calendar can omit actual account-only events.",
            "Counts are slot-days, not unique products, actual fills or strategy performance.",
            "Executable without candidate can serve an existing position; it is not evidence of new-entry eligibility.",
            "Product counts are period unions, not the number simultaneously available on a given date.",
            "Necessary entry gates use at least one whole contract of capped volume and at least one permitted direction; they do not prove account capital, position limits, price depth, order submission or fills.",
            "Observed overlap does not by itself identify the reasons for missing execution or prove complete sources.",
        ],
        "wall_seconds": round(time.monotonic() - started, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--panel-generation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.config.resolve(), args.panel_generation.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
