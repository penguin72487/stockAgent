"""Canonical local minute-source defaults, without loading data or accounting.

The replay/curve producers retain their public constant names as aliases.
Paths stay relative and ordered exactly as before; callers still own resolution,
receipt checks and prices. Importing this declaration must not load a provider,
NumPy/Polars/Torch, or a replay/execution engine.
"""

from __future__ import annotations

from pathlib import Path


DEFAULT_LOCAL_MINUTE_ROOTS = (
    Path("artifacts/data_repair/tw_day_trade_minute_curve/maintenance/current/fetched_kbars"),
    Path("artifacts/data_repair/tw_day_trade_minute_curve/kbars"),
    Path("data_tw_minute/research_dataset"),
    Path("data_tw_minute/shioaji_1m"),
)
DEFAULT_LOCAL_MINUTE_CACHE_ROOTS = (
    Path("artifacts/data_repair/tw_day_trade_minute_curve/rebuilt/tick_minutes"),
)
