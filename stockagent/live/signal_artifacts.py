"""Readers for the immutable execution payload published by signal_engine."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any


def read_execution_weight_rows(
    path: Path, *, market: str, signal_id: str,
) -> list[dict[str, Any]]:
    """Read schema-1 compact weights without recreating a Parquet artifact.

    This is the producer's actual execution payload, not a top-positions
    preview. Keep the same identity/schema checks in live and replay readers;
    replay callers separately pin and recheck the original file bytes.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"execution weights must be an object: {path}")
    rows = payload.get("rows")
    if (not market or not signal_id
            or int(payload.get("schema_version") or 0) != 1
            or str(payload.get("market") or "") != market
            or str(payload.get("signal_id") or "") != signal_id
            or not isinstance(rows, list)
            or not all(isinstance(row, dict) for row in rows)):
        raise ValueError(f"execution weights identity/schema mismatch: {path}")
    # Every consumer must see one unambiguous finite target per symbol.  Do
    # not coerce missing values or booleans to zero: that can silently remove
    # or reverse a paper position while still looking like a valid artifact.
    seen: set[str] = set()
    for row in rows:
        symbol, weight = row.get("symbol"), row.get("target_weight")
        if (not isinstance(symbol, str) or not symbol or len(symbol) > 32
                or symbol in seen or type(weight) not in (int, float)):
            raise ValueError(f"invalid execution target row: {path}")
        try:
            finite = math.isfinite(weight)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError(f"invalid execution target row: {path}")
        seen.add(symbol)
    return [dict(row) for row in rows]
