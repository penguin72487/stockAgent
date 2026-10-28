#!/usr/bin/env python3
"""Read back a configured futures/stock dual calendar without training.

The receipt identifies source-backed accounting dates and terminal rules. It
does not certify model attachment, actual fills, GPU execution or profitability.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import polars as pl

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.config import load_config
from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
from stockagent.data.tw_futures_benchmark import (
    load_tx_front_rolling_benchmark, resolve_tx_benchmark_path, tx_front_benchmark_source_end,
)


def audit(config_path: Path, output: Path) -> dict:
    config = load_config(config_path)
    trading = config.trading
    if (trading.execution_mode != "tw_stock_context_futures_portfolio"
            or trading.tw_futures_portfolio_capital_basis != "initial_margin"):
        raise ValueError("calendar audit requires the configured carrying margin account")
    axes_path = Path(config.data.factorized_feature_manifest)
    daily_path = Path(trading.tw_futures_portfolio_data_path)
    rules_path = Path(trading.tw_futures_portfolio_margin_rules_path)
    daily_manifest = daily_path.with_name("manifest.json")
    rules_manifest = rules_path.with_name("manifest.json")
    paths = [config_path, axes_path, daily_path, rules_path, daily_manifest, rules_manifest]
    before = {str(path): sha256_file(path) for path in paths}
    daily_info = json.loads(daily_manifest.read_text())
    rules_info = json.loads(rules_manifest.read_text())
    if (daily_info.get("status") != "complete" or rules_info.get("status") != "complete"
            or daily_info.get("outputs", {}).get("continuous_daily", {}).get("sha256") != before[str(daily_path)]
            or daily_info.get("execution_rules_sha256") != before[str(rules_path)]
            or rules_info.get("outputs", {}).get("rules", {}).get("sha256") != before[str(rules_path)]
            or rules_info.get("source_daily_sha256") != before[str(daily_path)]):
        raise ValueError("daily and rule release identity does not match its manifests")
    axes = json.loads(axes_path.read_text())
    decision_dates = np.asarray(axes["dates"], dtype="datetime64[D]")
    start = config.data.panel_start_date
    if start:
        decision_dates = decision_dates[decision_dates >= np.datetime64(start, "D")]
    if trading.tw_futures_portfolio_benchmark_mode == "tx_front_rolling_1x_gross":
        benchmark_path = resolve_tx_benchmark_path(trading)
        before[str(benchmark_path)] = sha256_file(benchmark_path)
        decision_dates = decision_dates[decision_dates <= tx_front_benchmark_source_end(benchmark_path)]

    daily = pl.scan_parquet(daily_path)
    days = daily.select("date", "accounting_only_date").unique().sort("date").collect()
    if days["date"].is_duplicated().any() or days["accounting_only_date"].null_count():
        raise ValueError("source has ambiguous or missing account-calendar classification")
    calendar = build_futures_account_calendar(
        decision_dates, days["date"].to_numpy(), days["accounting_only_date"].to_numpy(),
    )
    account_dates = calendar.dates[calendar.account_only_mask].tolist()
    benchmark_account_only = None
    if trading.tw_futures_portfolio_benchmark_mode == "tx_front_rolling_1x_gross":
        benchmark_facts = load_tx_front_rolling_benchmark(
            benchmark_path, calendar.dates, account_only_mask=calendar.account_only_mask,
        )
        event_mask = calendar.account_only_mask
        if (benchmark_facts["source_covered"][event_mask].any()
                or np.isfinite(benchmark_facts["front_month_close"][event_mask]).any()
                or np.any(benchmark_facts["benchmark_log_returns"][event_mask] != 0)):
            raise ValueError("account-only benchmark nodes cannot claim observed TX prices or trades")
        benchmark_account_only = {
            "verified": True, "account_nodes": int(event_mask.sum()),
            "observed_tx_marks": 0, "trade_or_quote_fabricated": False,
        }
    rows = daily.filter(pl.col("date").is_in(account_dates)).select(
        "date", "product", "physical_contract", "symbol", "cash_settlement", "executable",
    ).collect()
    if rows.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("account-day source contains duplicate physical identities")
    if (rows["cash_settlement"].null_count() or rows["executable"].null_count()
            or rows["executable"].any()):
        raise ValueError("account-only source must have explicit settlement and no executable trades")
    if set(rows["date"].unique().to_list()) != set(account_dates):
        raise ValueError("one or more declared account-only dates have no source rows")
    rules = pl.scan_parquet(rules_path).filter(pl.col("date").is_in(account_dates)).select(
        "date", "physical_contract", "terminal_event", "carry_from_date",
        "carry_from_physical_contract", "inventory_entry_reachable",
        "opening_contract_value_twd", "terminal_contract_value_twd", "terminal_tax_twd",
    ).collect()
    if rules.select("date", "physical_contract").is_duplicated().any():
        raise ValueError("account-day rules contain duplicate physical identities")
    joined = rows.join(rules, on=["date", "physical_contract"], how="left", validate="1:1")
    if joined["terminal_event"].null_count():
        raise ValueError("account-day physical identity lacks a matching terminal rule")
    cash = joined.filter(pl.col("cash_settlement"))
    if cash.filter(pl.col("terminal_event") != "cash_settlement").height:
        raise ValueError("a source cash settlement lacks the matching terminal rule")
    if joined.filter(pl.col("terminal_event") == "market_close_required").height:
        raise ValueError("an account-only day cannot invent a required market close")
    details = cash.sort("date", "product", "physical_contract").to_dicts()
    details = [{key: str(value) if hasattr(value, "isoformat") else value
                for key, value in row.items()} for row in details]
    if any(sha256_file(Path(path)) != digest for path, digest in before.items()):
        raise ValueError("source changed during the calendar audit")
    result = {
        "contract": "stock_decision_futures_account_calendar_source_audit_v1",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision_start": str(decision_dates[0]), "decision_end": str(decision_dates[-1]),
        "decision_sessions": len(decision_dates), "account_nodes": len(calendar.dates),
        "account_only_dates": [str(value) for value in calendar.dates[calendar.account_only_mask]],
        "account_only_contract_rows": rows.height, "cash_settlement_rows": cash.height,
        "benchmark_account_only": benchmark_account_only,
        "cash_settlement_details": details, "calendar_fingerprint": calendar.fingerprint,
        "inputs_sha256": before,
        "valuation_research_contract": trading.tw_futures_portfolio_valuation_research_contract,
        "source_receipt_scope": "manifest hashes and account-day rows, not complete source or PIT verification",
        "model_attachment_verified": False, "actual_orders_or_fills_verified": False,
        "training_started": False,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.config, args.output)
    print(json.dumps({key: result[key] for key in (
        "decision_sessions", "account_only_dates", "account_only_contract_rows", "cash_settlement_rows",
        "calendar_fingerprint", "model_attachment_verified", "training_started")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
