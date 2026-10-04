"""Pure Taiwan index-derivative cost schedules without tensor runtime imports.

The clock, prices, fills, and ledger remain owned by the existing backtests.
These validated value objects are also used while parsing ordinary configs.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from numbers import Real
from typing import Final

import numpy as np

from stockagent.data.tw_index_futures import TAIFEX_INDEX_FUTURES_PRODUCTS


TW_INDEX_FUTURES_TRANSACTION_TAX_RATE: Final[float] = 0.00002


@dataclass(frozen=True, slots=True)
class FuturesCostSchedule:
    # Tax, fixed fees, and slippage apply to both transactions in a daily-flat
    # round trip. ``tax_rate`` is the statutory per-transaction rate.
    tax_rate: float = TW_INDEX_FUTURES_TRANSACTION_TAX_RATE
    exchange_and_clearing_fee_per_side_twd: tuple[float, ...] = (20.0, 12.5, 8.0)
    broker_fee_per_side_twd: tuple[float, ...] = (0.0, 0.0, 0.0)
    slippage_points_per_side: tuple[float, ...] = (0.0, 0.0, 0.0)
    basket_fee_penalty: float = 1.0

    def __post_init__(self) -> None:
        if (
            not isinstance(self.tax_rate, Real)
            or isinstance(self.tax_rate, bool)
            or not math.isfinite(float(self.tax_rate))
            or float(self.tax_rate) < 0.0
        ):
            raise ValueError("tax_rate must be a finite non-negative real")
        for name in (
            "exchange_and_clearing_fee_per_side_twd",
            "broker_fee_per_side_twd",
            "slippage_points_per_side",
        ):
            values = tuple(getattr(self, name))
            if len(values) != len(TAIFEX_INDEX_FUTURES_PRODUCTS):
                raise ValueError(f"{name} must contain TX, MTX, and TMF values")
            if any(
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(value) < 0.0
                for value in values
            ):
                raise ValueError(f"{name} must contain finite non-negative values")
        if (
            not isinstance(self.basket_fee_penalty, Real)
            or isinstance(self.basket_fee_penalty, bool)
            or not math.isfinite(float(self.basket_fee_penalty))
            or float(self.basket_fee_penalty) < 0.0
        ):
            raise ValueError("basket_fee_penalty must be a finite non-negative real")

    @property
    def fixed_fee_per_side_twd(self) -> np.ndarray:
        return np.asarray(
            self.exchange_and_clearing_fee_per_side_twd,
            dtype=np.float64,
        ) + np.asarray(self.broker_fee_per_side_twd, dtype=np.float64)


@dataclass(frozen=True, slots=True)
class OptionDayCostSchedule:
    fixed_fee_per_contract_per_side_twd: float = 22.0
    # Statutory TXO premium tax per transaction. A daily-flat position has an
    # opening and a closing transaction, regardless of long/short direction.
    transaction_tax_rate: float = 0.001
    slippage_points_per_side: float = 0.5

    def __post_init__(self) -> None:
        for name in (
            "fixed_fee_per_contract_per_side_twd",
            "transaction_tax_rate",
            "slippage_points_per_side",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, Real)
                or not math.isfinite(float(value))
                or float(value) < 0.0
            ):
                raise ValueError(f"{name} must be finite and non-negative")


# Keep pickle/checkpoint class identities and import paths unchanged. The old
# modules re-export these exact class objects, so existing serialized values
# still resolve without a semantic checkpoint migration.
FuturesCostSchedule.__module__ = "stockagent.backtest.tw_index_futures"
OptionDayCostSchedule.__module__ = "stockagent.backtest.tw_index_derivatives_day"
