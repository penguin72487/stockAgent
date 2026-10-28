"""Receipt-only opening transport proof shared by runtime and acceptance.

This proves a responding connection, never opening prices, model coverage,
market eligibility, a paper fill, or the opening latency SLO.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def opening_transport_proven(receipt: Mapping[str, Any]) -> bool:
    if receipt.get("ready") is not True:
        return False
    if receipt.get("source") == "twse_tpex:mis":
        return True
    if (receipt.get("source") != "parallel_opening_sources"
            or receipt.get("proof") != "responding_transport_not_opening_price_readiness"):
        return False
    providers = receipt.get("providers")
    return isinstance(providers, Mapping) and any(
        isinstance(row, Mapping) and row.get("state") == "complete" and row.get("ready") is True
        for row in providers.values()
    )
