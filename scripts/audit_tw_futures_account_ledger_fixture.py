#!/usr/bin/env python3
"""Replay one source-backed off-session cash settlement through the ledger.

This is a counterfactual one-contract engineering fixture, not model training,
an actual order, a fill, or a full-source acceptance.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

import numpy as np
import polars as pl
import torch

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.backtest.tw_futures_portfolio import run_tw_futures_portfolio_integer_torch
from stockagent.config import load_config
from stockagent.data import tw_futures_margin as margin
from stockagent.data.panel import PanelData
from stockagent.data.tw_futures_account_calendar import (
    build_futures_account_calendar, interleave_account_only_ledger_rows,
    slice_futures_account_calendar,
)
from stockagent.data.tw_stock_context_futures_portfolio import attach_stock_context_futures_portfolio_daily


def audit(config_path: Path, event_date: str, physical_contract: str, output: Path) -> dict:
    config = load_config(config_path)
    trading, data = config.trading, config.data
    if (trading.execution_mode != "tw_stock_context_futures_portfolio"
            or trading.tw_futures_portfolio_capital_basis != "initial_margin"
            or trading.tw_futures_portfolio_holding_policy != "carry"):
        raise ValueError("fixture requires the configured exact futures carrying account")
    daily_path = Path(trading.tw_futures_portfolio_data_path)
    rules_path = Path(trading.tw_futures_portfolio_margin_rules_path)
    final_settlement_path = Path(trading.tw_futures_portfolio_final_settlement_path)
    axes_path = Path(data.factorized_feature_manifest)
    pin_paths = [config_path, daily_path, rules_path, final_settlement_path, axes_path]
    pins = {str(path): sha256_file(path) for path in pin_paths}
    event = np.datetime64(event_date, "D")
    decisions = np.asarray(json.loads(axes_path.read_text())["dates"], dtype="datetime64[D]")
    before = decisions[decisions < event]
    after = decisions[decisions > event]
    if not len(before) or not len(after) or event in decisions:
        raise ValueError("fixture requires a non-stock day bounded by real decision sessions")
    dates = np.asarray([before[-1], event, after[0]], dtype="datetime64[D]")
    raw = pl.scan_parquet(daily_path).filter(
        (pl.col("date") == event.astype(object)) & (pl.col("physical_contract") == physical_contract)
    ).select("date", "product", "physical_contract", "symbol", "cash_settlement", "executable",
             "accounting_only_date").collect()
    if (raw.height != 1 or raw["accounting_only_date"][0] is not True
            or raw["cash_settlement"][0] is not True or raw["executable"][0] is not False):
        raise ValueError("fixture target must be one non-executable source cash settlement")
    slot = int(raw["symbol"][0].split("_")[-1]) - 1
    shape = (len(dates), 1)
    zeros = np.zeros(shape, dtype=np.float32)
    mask = np.zeros(shape, dtype=bool)
    panel = PanelData(dates=dates, symbols=["2330"], feature_names=[],
        features=np.empty((*shape, 0), dtype=np.float32), returns_1d=zeros,
        tradable_mask=mask, alive_mask=mask, benchmark_returns=np.zeros(len(dates), dtype=np.float32),
        close_prices=zeros)
    validated = margin.ValidatedMarginSource.load(rules_path, daily_path)
    panel = attach_stock_context_futures_portfolio_daily(
        panel, daily_path, fee_per_side_twd_by_group={
            "large": trading.tw_futures_portfolio_fee_large_twd,
            "standard": trading.tw_futures_portfolio_fee_standard_twd,
            "stock": trading.tw_futures_portfolio_fee_stock_twd,
            "micro": trading.tw_futures_portfolio_fee_micro_twd},
        integer_contracts=True, current_open_feature=False,
        denomination_context_basis=data.tw_futures_denomination_context_basis,
        futures_slot_count=data.tw_futures_portfolio_slot_count,
        margin_rules_path=rules_path, valuation_research_contract=trading.tw_futures_portfolio_valuation_research_contract,
        final_settlement_path=final_settlement_path,
        require_prior_capacity=data.tw_futures_require_prior_capacity,
        validated_margin_source=validated,
        integer_fee_per_contract_per_side_twd=trading.tw_futures_portfolio_integer_fee_per_contract_per_side_twd,
        max_volume_participation=trading.max_volume_participation,
        # The three-date fixture has no fake TX observation on the closed day.
        benchmark_mode="legacy_front_holding_return",
    )
    panel = margin.attach_futures_margin_rules(
        panel, rules_path, broker_multiplier=trading.tw_futures_portfolio_broker_margin_multiplier,
        liquidation_ratio=trading.tw_futures_portfolio_margin_liquidation_ratio,
        participation=trading.max_volume_participation, validated_source=validated,
    )
    source_execution = panel.stock_context_futures_portfolio_daily.integer_execution
    event_execution = torch.from_numpy(source_execution[1:2].copy())
    if bool((event_execution[..., 1] != 0).any()):
        raise ValueError("canonical event tape unexpectedly contains executable rows")
    # Previous-session volume is not an order on a closed day. The canonical
    # clock later applies this same no-execution gate to the base capacity.
    event_execution[..., 8] = 0
    source_slot = int(event_execution[0, slot, margin.CARRY_SOURCE_SLOT].item()) - 1
    numerator = event_execution[0, slot, margin.CARRY_QUANTITY_NUMERATOR].item()
    denominator = event_execution[0, slot, margin.CARRY_QUANTITY_DENOMINATOR].item()
    if source_slot < 0 or numerator != 1 or denominator != 1:
        raise ValueError("fixture requires a source-proven one-for-one incoming contract")
    if not bool(event_execution[0, slot, margin.CASH_SETTLEMENT] > .5):
        raise ValueError("canonical event tape lacks official cash settlement")
    calendar = build_futures_account_calendar(dates[[0, 2]], dates, [False, True, False])
    following = slice_futures_account_calendar(calendar, 1, 2)
    next_execution = torch.from_numpy(source_execution[2:3].copy())
    capital = 1_000_000.
    results = []
    for sign in (1, -1):
        quantities = torch.zeros(source_execution.shape[1])
        quantities[source_slot] = sign
        actions = torch.zeros((1, source_execution.shape[1]))
        expanded_actions, expanded_execution = interleave_account_only_ledger_rows(
            following, actions, next_execution, event_execution,
        )
        result = run_tw_futures_portfolio_integer_torch(
            expanded_actions, expanded_execution, initial_capital=capital, initial_quantities=quantities,
        )
        event_row = event_execution[0, slot]
        gross = sign * (event_row[margin.TERMINAL_MARK] - event_row[margin.PREVIOUS_MARK]
                        + event_row[margin.CARRY_CASH]).item()
        costs = (event_row[5] + event_row[7]).item()
        expected = capital + gross - costs
        equity = result.margin_audit_history[0, 2].item()
        if abs(equity - expected) > .05 or result.residual_contract_quantities_history[0, slot].item() != 0:
            raise ValueError("canonical event ledger disagrees with one-contract settlement arithmetic")
        results.append(dict(direction="long" if sign > 0 else "short", initial_contracts=sign,
            gross_pnl_twd=gross, terminal_fees_and_tax_twd=costs,
            expected_equity_twd=expected, ledger_equity_twd=equity,
            residual_contracts=int(result.residual_contract_quantities_history[0, slot].item())))
    if any(sha256_file(Path(path)) != digest for path, digest in pins.items()):
        raise ValueError("fixture input changed during replay")
    summary = dict(contract="off_session_cash_settlement_exact_ledger_fixture_v1",
        observed_at_utc=datetime.now(timezone.utc).isoformat(), event_date=event_date,
        preceding_stock_decision=str(dates[0]), next_stock_decision=str(dates[2]),
        physical_contract=physical_contract, symbol=raw["symbol"][0],
        event_is_executable=False, event_has_model_decision=False, cases=results,
        inputs_sha256=pins, valuation_research_contract=trading.tw_futures_portfolio_valuation_research_contract,
        scope="one-contract counterfactual fixture using canonical margin preparation and integer ledger",
        decision_clock_or_full_model_attachment_verified=False, actual_order_or_fill=False,
        gpu_training_started=False)
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(output, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--event-date", required=True)
    parser.add_argument("--physical-contract", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = audit(args.config, args.event_date, args.physical_contract, args.output)
    print(json.dumps({key: result[key] for key in ("event_date", "physical_contract", "cases",
        "decision_clock_or_full_model_attachment_verified", "gpu_training_started")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
