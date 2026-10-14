"""Read-only price movement metrics, calculated only after publication checks.

This is a price-axis distance, not a side-adjusted return, net P&L or order
execution count. VWAP/average prices retain their precision and produce an
explicitly approximate fractional tick distance.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from functools import lru_cache
import math
from typing import Any

import numpy as np

from stockagent.data.tw_price_rules import TW_PRICE_RULE_CONTRACT_VERSION, tick_size_numpy
from stockagent.data.tw_security import classify_tw_stock_or_etf


CONTRACT = "public_entry_price_distance_v1"
# Union of the canonical regular stock's historical boundaries. Tick values
# come from tw_price_rules, not a second frontend/execution-rule registry.
_BOUNDARIES = (0.0, 5.0, 10.0, 15.0, 50.0, 100.0, 150.0, 500.0, 1000.0)


@lru_cache(maxsize=256)
def _bands(kind: str, session: str) -> tuple[tuple[float, float, float], ...]:
    ends = (*_BOUNDARIES[1:], math.inf)
    probes = np.array([(lower + upper) / 2 if math.isfinite(upper) else lower + 1
                       for lower, upper in zip(_BOUNDARIES, ends)], dtype=np.float64)
    ticks = tick_size_numpy(probes, [session], security_types=kind)
    return tuple((lower, upper, float(tick)) for lower, upper, tick
                 in zip(_BOUNDARIES, ends, ticks))


def _positive(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value > 0 else None


def _grid_aligned(price: float, bands: tuple[tuple[float, float, float], ...]) -> bool:
    tick = next(tick for lower, upper, tick in bands if lower <= price < upper)
    return math.isclose(price / tick, round(price / tick), rel_tol=0, abs_tol=1e-7)


def public_entry_distances(row: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Derive metrics from public values with their own publication evidence.

    Missing/withheld prices cannot be recovered through derived percentages.
    A venue/product we do not support gets a percentage but no guessed ticks.
    Inputs are never mutated and no collector, ledger or network is consulted.
    """
    display = row.get("public_price_display") or {}
    proofs = display.get("visible_fields") or {}
    withheld = display.get("withheld_fields") or []
    entry = _positive(row.get("entry_price"))
    if entry is None or "entry_price" not in proofs or "entry_price" in withheld:
        return {}
    symbol = str(row.get("symbol") or "").strip().upper()
    kind = row.get("security_type") or classify_tw_stock_or_etf(symbol)
    # The foreign-counter suffix cannot identify the actual quote currency.
    foreign_counter = len(symbol) == 6 and symbol[-1] in "KMSC"
    supported = kind in {"stock", "etf"} and not foreign_counter
    result: dict[str, dict[str, Any]] = {}
    for field in ("last_mark_price", "exit_price", "last_exit_price"):
        price = _positive(row.get(field))
        if price is None or field not in proofs or field in withheld:
            continue
        clock = (row.get("last_quote_at") if field == "last_mark_price" else
                 row.get("last_exit_at") if field == "last_exit_price" else
                 row.get("exit_exchange_at") or row.get("exit_at"))
        session = str(clock or row.get("exit_session_date") or row.get("session_date") or "")[:10]
        change = (price - entry) / entry * 100
        if not math.isfinite(change):
            continue
        value = {"contract": CONTRACT, "change_pct": change,
                 "ticks": None, "approximate_ticks": False, "tick_session_date": session,
                 "tick_rule_version": TW_PRICE_RULE_CONTRACT_VERSION,
                 "semantics": "price_change_not_net_pnl"}
        try:
            parsed = date.fromisoformat(session)
            if parsed.isoformat() != session or parsed < date(2000, 1, 1):
                raise ValueError("unverified historical date")
        except ValueError:
            result[field] = value
            continue
        if supported:
            bands = _bands(kind, session)
            low, high = sorted((entry, price))
            count = math.fsum(max(0, min(high, upper) - max(low, lower)) / tick
                             for lower, upper, tick in bands)
            approximate = not (_grid_aligned(entry, bands) and _grid_aligned(price, bands))
            signed = count if price >= entry else -count
            value["ticks"] = signed if approximate else float(round(signed))
            value["approximate_ticks"] = approximate
        result[field] = value
    return result
