"""Distinguish a valid cash policy from the inactive tail of a failed account."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
from stockagent.backtest.futures_data_validity import CARRY_DATA_REASONS


def futures_minute_execution_status(
    result: Any, dates: np.ndarray, symbols: Sequence[str]
) -> dict[str, Any]:
    if result.execution_mode != "tw_stock_futures_day_trade_0845_minute":
        raise ValueError("execution status requires the stock-futures minute ledger")
    dates = np.asarray(dates).astype("datetime64[D]")
    quantities = np.asarray(result.futures_contract_quantities_history)
    residuals = np.asarray(result.futures_residual_contract_quantities_history)
    defaults = np.asarray(result.settlement_default, dtype=bool)
    carry = getattr(result, "final_futures_carry_state", None)
    shape = (len(dates), len(symbols), 2 if carry is None else np.asarray(carry).shape[1])
    if quantities.shape != shape or residuals.shape != shape or defaults.shape != (len(dates),):
        raise ValueError("execution status requires complete date/symbol/contract histories")
    if not all(np.issubdtype(x.dtype, np.integer) for x in (quantities, residuals)):
        raise ValueError("execution status requires whole-contract quantities")
    if carry is None and np.any(np.any(residuals != 0, axis=(1, 2)) & ~defaults):
        raise ValueError("residual contracts cannot be reported as a valid flat close")
    reasons = getattr(result, "default_reason_history", None)
    if carry is not None and (reasons is None or np.asarray(reasons).shape != (len(dates),)):
        raise ValueError("carry execution status requires complete failure reasons")
    failures = np.flatnonzero(defaults)
    first = int(failures[0]) if failures.size else None
    traded = (np.any(quantities != 0, axis=(1, 2)) if carry is None
              else np.asarray(result.turnovers) > 0)
    alive = bool(result.final_alive)
    if first is not None:
        status = "execution_contract_failed"
        if carry is not None and int(np.asarray(reasons)[first]) in CARRY_DATA_REASONS:
            status = "data_invalid"
    elif not alive:
        status = "invalid_terminal_state"
    else:
        status = "valid_traded" if traded.any() else "valid_cash"
        if carry is not None and np.any(residuals[-1] != 0):
            status = "valid_marked_open_positions"
    first_residuals = []
    if first is not None:
        for si, slot in np.argwhere(residuals[first] != 0):
            item = {
                "symbol": str(symbols[si]),
                "candidate_slot": int(slot),
                "signed_contracts": int(residuals[first, si, slot]),
            }
            if carry is not None:
                item["physical_id"] = int(result.futures_carry_state_history[first, si, slot, 2])
            first_residuals.append(item)
    return {
        "schema_version": 1,
        "residual_policy": "fail" if carry is None else "carry",
        "overnight_position_days": int(np.any(residuals != 0, axis=(1, 2)).sum()) if carry is not None else 0,
        "terminal_open_contracts": int(np.abs(residuals[-1]).sum()) if carry is not None else 0,
        "status": status,
        "final_alive": alive,
        "evaluated_days": len(dates),
        "traded_days": int(traded.sum()),
        "entry_contracts": int(np.abs(quantities).sum()) if carry is None else None,
        "opening_position_contract_days": int(np.abs(quantities).sum()) if carry is not None else None,
        "first_failure_date": None if first is None else str(dates[first]),
        "first_failure_row": first,
        "first_failure_reason": (None if first is None else
            ({1: "physical_identity_mismatch", 2: "held_minute_source_missing",
              3: "held_settlement_missing", 4: "nonpositive_equity", 5: "nonfinite_account_equity",
              6: "unresolved_corporate_contract_transition"}.get(
                int(np.asarray(reasons)[first]), "unknown")
             if carry is not None else "unfilled_1330_or_invalid_equity")),
        "first_failure_residuals": first_residuals,
        "cash_days_before_failure": int((~traded[:first]).sum()),
        "inactive_days_after_failure": (
            0 if first is None else int((~traded[first + 1:]).sum())
        ),
        "failure_return_interpretation": (
            None if first is None else
            "undefined_account_value_not_a_realized_loss" if status == "data_invalid" else
            "economic_insolvency_ruin_floor" if carry is not None and int(np.asarray(reasons)[first]) == 4 else
            "execution_contract_failure_marker_not_realized_account_loss"
        ),
    }


def save_futures_minute_execution_status(
    path: Path, result: Any, dates: np.ndarray, symbols: Sequence[str]
) -> dict[str, Any]:
    status = futures_minute_execution_status(result, dates, symbols)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(status, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)
    return status
