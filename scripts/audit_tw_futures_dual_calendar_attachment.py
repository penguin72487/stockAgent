#!/usr/bin/env python3
"""Read back configured stock/futures attachment on its two real calendars.

The stock feature array is intentionally empty: this checks the canonical
futures data and execution attachment, not model training or full feature PIT.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import polars as pl
import stockagent

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_account_attachment import attach_configured_futures_account_calendar
from stockagent.data.tw_futures_benchmark import resolve_tx_benchmark_path, tx_front_benchmark_source_end
from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS


def audit(config_path: Path, output: Path) -> dict:
    from stockagent.data import (
        tw_futures_account_attachment, tw_futures_account_calendar, tw_futures_benchmark,
        tw_futures_margin, tw_futures_margin_release, tw_stock_context_futures_portfolio,
    )
    config = load_config(config_path)
    trading, data = config.trading, config.data
    axes_path = Path(data.factorized_feature_manifest)
    daily_path = Path(trading.tw_futures_portfolio_data_path)
    rules_path = Path(trading.tw_futures_portfolio_margin_rules_path)
    clock_path = Path(trading.tw_futures_portfolio_entry_clock_path)
    benchmark_path = resolve_tx_benchmark_path(trading)
    paths = [config_path, axes_path, daily_path, rules_path, clock_path,
             daily_path.with_name("manifest.json"), rules_path.with_name("manifest.json"),
             clock_path.with_name("manifest.json"), benchmark_path,
             Path(trading.tw_futures_portfolio_final_settlement_path)]
    if trading.tw_futures_portfolio_opening_reference_path:
        paths.append(Path(trading.tw_futures_portfolio_opening_reference_path))
    implementation_paths = [Path(__file__), Path(stockagent.__file__)] + [Path(module.__file__) for module in (
        tw_futures_account_attachment, tw_futures_account_calendar, tw_futures_benchmark,
        tw_futures_margin, tw_futures_margin_release, tw_stock_context_futures_portfolio,
    )]
    paths.extend(implementation_paths)
    before = {str(path): sha256_file(path) for path in paths}
    axes = json.loads(axes_path.read_text())
    dates = np.asarray(axes["dates"], dtype="datetime64[D]")
    if data.panel_start_date:
        dates = dates[dates >= np.datetime64(data.panel_start_date, "D")]
    dates = dates[dates <= tx_front_benchmark_source_end(benchmark_path)]
    symbols = list(axes["symbols"])
    shape = (len(dates), len(symbols))
    # Real identities only; these empty placeholders cannot train a model.
    zeros = np.zeros(shape, dtype=np.float32)
    mask = np.zeros(shape, dtype=bool)
    panel = PanelData(dates=dates, symbols=symbols, feature_names=[],
        features=np.empty((*shape, 0), dtype=np.float32), returns_1d=zeros,
        tradable_mask=mask, alive_mask=mask, benchmark_returns=np.zeros(len(dates), dtype=np.float32),
        close_prices=zeros)
    attached = attach_configured_futures_account_calendar(panel, config)
    daily = attached.stock_context_futures_portfolio_daily
    calendar = daily.futures_account_calendar
    events = daily.account_only_integer_execution
    if not np.array_equal(daily.dates, dates) or not np.array_equal(attached.dates, dates):
        raise ValueError("attachment changed the real stock decision sessions")
    cash_counts = np.count_nonzero(events[..., margin.CASH_SETTLEMENT] > .5, axis=1)
    checks = []
    feature_name = "taifex_settlement_logret_1d"
    feature_index = FUTURES_MODEL_FEATURE_COLUMNS.index(feature_name)
    raw = pl.scan_parquet(daily_path)
    for node in calendar.account_only_indices:
        prev_index = int(calendar.prior_decision_index[node])
        if prev_index < 0 or prev_index + 1 >= len(dates):
            raise ValueError("event is outside bounded real decision sessions")
        prior_day, next_day = dates[prev_index], dates[prev_index + 1]
        previous = raw.filter(pl.col("date") == prior_day.astype(object)).select(
            "physical_contract", "symbol", pl.col(feature_name).alias("prior_feature"))
        following = raw.filter(pl.col("date") == next_day.astype(object)).select("physical_contract", "symbol")
        matched = previous.join(following, on=["physical_contract", "symbol"], how="inner").collect()
        slots = matched["symbol"].str.extract(r"(\d+)$").cast(pl.Int64).to_numpy() - 1
        eligible = daily.candidate_mask[prev_index + 1, slots]
        values = np.nan_to_num(matched["prior_feature"].to_numpy().astype(np.float32), nan=0., posinf=0., neginf=0.)
        actual = daily.candidate_features[prev_index + 1, slots, feature_index]
        if not np.allclose(actual[eligible], values[eligible], atol=1e-7, rtol=0):
            raise ValueError("post-event feature differs from the prior real market session")
        checks.append({"account_date": str(calendar.dates[node]), "previous_decision": str(prior_day),
            "next_decision": str(next_day), "matched_same_physical_slots": len(slots),
            "eligible_candidates_checked": int(eligible.sum())})
    if any(sha256_file(Path(path)) != digest for path, digest in before.items()):
        raise ValueError("source changed during dual-calendar attachment")
    result = {"contract": "configured_futures_dual_calendar_attachment_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision_start": str(dates[0]), "decision_end": str(dates[-1]), "decision_sessions": len(dates),
        "stock_symbol_count": len(symbols), "fixed_futures_slots": len(daily.symbols),
        "account_only_dates": [str(day) for day in calendar.dates[calendar.account_only_indices]],
        "account_only_cash_settlements_by_date": cash_counts.tolist(),
        "calendar_fingerprint": calendar.fingerprint, "previous_market_feature_checks": checks,
        "implementation_sha256": {str(path): before[str(path)] for path in implementation_paths},
        "inputs_sha256": before, "valuation_research_contract": trading.tw_futures_portfolio_valuation_research_contract,
        "scope": "canonical futures source, TX benchmark, clock and sparse account tape; stock feature payload absent",
        "training_dataset_event_integration_verified": False, "model_training_started": False,
        "actual_orders_or_fills_verified": False}
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.config, args.output)
    print(json.dumps({key: result[key] for key in ("decision_sessions", "fixed_futures_slots",
        "account_only_dates", "account_only_cash_settlements_by_date",
        "previous_market_feature_checks", "training_dataset_event_integration_verified",
        "model_training_started")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
