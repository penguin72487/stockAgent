"""An unknown futures account value is a data error, never a utility penalty."""
from __future__ import annotations

import json


CARRY_DATA_REASONS = {
    1: "physical_identity_mismatch",
    2: "held_minute_source_missing",
    3: "held_settlement_missing",
    5: "nonfinite_account_equity",
    6: "unresolved_corporate_contract_transition",
}


class FuturesCarryDataError(ValueError):
    """Serializable evidence for a rejected, unevaluable account trajectory."""

    def __init__(self, evidence: dict):
        self.evidence = evidence
        super().__init__(
            "Futures carry data invalid; no loss, optimizer update or performance "
            "claim is permitted for this trajectory: "
            + json.dumps(evidence, ensure_ascii=False, allow_nan=False)
        )


def map_carry_failure_symbols(evidence: dict, symbol_indices=None) -> dict:
    """Retain local indices and add panel indices for subset/DDP diagnostics."""
    result = dict(evidence)
    positions = []
    for source in evidence.get('positions', []):
        position = dict(source)
        index = int(position['symbol_index'])
        position['panel_symbol_index'] = index if symbol_indices is None else int(symbol_indices[index])
        positions.append(position)
    result['positions'] = positions
    return result


def reject_invalid_carry_artifact(result) -> None:
    """Prevent legacy finite failure markers from entering new financial reports."""
    if getattr(result, "final_futures_carry_state", None) is None:
        return
    import numpy as np

    reasons = getattr(result, "default_reason_history", None)
    if reasons is None:
        raise ValueError("carry result has no data-validity evidence")
    invalid = np.flatnonzero(np.isin(np.asarray(reasons), list(CARRY_DATA_REASONS)))
    if len(invalid):
        row = int(invalid[0])
        raise FuturesCarryDataError({
            "scope": "artifact", "row": row,
            "reason": CARRY_DATA_REASONS[int(reasons[row])],
        })
