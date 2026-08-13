"""Causal relative-tenor candidates for daily-flat Taiwan index derivatives.

The model never owns a permanent historical contract id.  Futures occupy six
economic expiry slots (E1..E6).  Options are packed from the preceding
exchange session's known contract set and carry their own causal metadata.
Today's open/close/volume are labels and executor facts only; they never decide
which candidates the model can see.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math
import re
from typing import Final, Iterable

import numpy as np

from stockagent.data.tw_index_derivatives_tick import taifex_option_expiry
from stockagent.data.tw_index_futures import (
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS,
    TaiwanIndexFuturesDaySession,
)
from stockagent.data.tw_index_options_daily import TaiwanIndexOptionChainDaySession


TAIFEX_OPTION_CANDIDATE_CAPACITY: Final[int] = 4096
TAIFEX_OPTION_CANDIDATE_FEATURE_DIM: Final[int] = 9
TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4: Final[int] = (
    TAIFEX_INDEX_FUTURES_TENOR_SLOTS + TAIFEX_OPTION_CANDIDATE_CAPACITY
)
TAIFEX_DERIVATIVE_CANDIDATE_CONTRACT_VERSION: Final[int] = 1
TAIFEX_OPTION_EXPIRY_SLOTS_BY_FAMILY: Final[tuple[int, int, int]] = (5, 3, 3)

_MONTHLY_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]{6}$")
_WED_WEEKLY_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]{6}W[1-5]$", re.I)
_FRI_WEEKLY_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]{6}F[1-5]$", re.I)


def _option_family(series: str) -> int:
    normalized = str(series).strip().upper()
    if _MONTHLY_RE.fullmatch(normalized):
        return 0
    if _WED_WEEKLY_RE.fullmatch(normalized):
        return 1
    if _FRI_WEEKLY_RE.fullmatch(normalized):
        return 2
    raise ValueError(f"unsupported TXO series in causal candidate set: {series!r}")


def _as_python_date(value: np.datetime64) -> date:
    return date.fromisoformat(str(np.datetime64(value, "D")))


@dataclass(frozen=True, slots=True)
class TaiwanIndexDerivativeDayCandidates:
    """Fixed-envelope tensors with date-local concrete contract mappings."""

    dates: np.ndarray
    futures_contract_months: np.ndarray
    futures_candidate_mask: np.ndarray
    futures_simple_returns: np.ndarray
    option_candidate_mask: np.ndarray
    option_candidate_features: np.ndarray
    option_simple_returns: np.ndarray
    option_sparse_indices: np.ndarray
    source_chain: TaiwanIndexOptionChainDaySession

    def __post_init__(self) -> None:
        dates = np.asarray(self.dates, dtype="datetime64[D]")
        rows = int(dates.size)
        if dates.ndim != 1 or rows == 0 or bool(np.any(dates[1:] <= dates[:-1])):
            raise ValueError("derivative candidate dates must be non-empty and increasing")
        expected_futures = (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS)
        expected_options = (rows, TAIFEX_OPTION_CANDIDATE_CAPACITY)
        if np.asarray(self.futures_contract_months).shape != expected_futures:
            raise ValueError("futures_contract_months must have shape [T,6]")
        if np.asarray(self.futures_candidate_mask).shape != expected_futures:
            raise ValueError("futures_candidate_mask must have shape [T,6]")
        if np.asarray(self.futures_simple_returns).shape != expected_futures:
            raise ValueError("futures_simple_returns must have shape [T,6]")
        if np.asarray(self.option_candidate_mask).shape != expected_options:
            raise ValueError("option_candidate_mask must have shape [T,4096]")
        if np.asarray(self.option_simple_returns).shape != expected_options:
            raise ValueError("option_simple_returns must have shape [T,4096]")
        if np.asarray(self.option_sparse_indices).shape != expected_options:
            raise ValueError("option_sparse_indices must have shape [T,4096]")
        if np.asarray(self.option_candidate_features).shape != (
            rows,
            TAIFEX_OPTION_CANDIDATE_CAPACITY,
            TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
        ):
            raise ValueError("option_candidate_features must have shape [T,4096,9]")
        if not np.array_equal(dates, np.asarray(self.source_chain.dates, dtype="datetime64[D]")):
            raise ValueError("candidate and source-chain dates must align")
        option_mask = np.asarray(self.option_candidate_mask, dtype=bool)
        features = np.asarray(self.option_candidate_features)
        if bool((~np.isfinite(features[option_mask])).any()):
            raise ValueError("visible option candidate features must be finite")
        sparse = np.asarray(self.option_sparse_indices, dtype=np.int64)
        if bool(((sparse >= 0) & ~option_mask).any()):
            raise ValueError("an invisible option candidate cannot map to a contract")
        option_returns = np.asarray(self.option_simple_returns)
        if bool((np.isfinite(option_returns) & ((sparse < 0) | ~option_mask)).any()):
            raise ValueError("finite option returns require a visible mapped contract")
        object.__setattr__(self, "dates", dates)

    @property
    def action_count(self) -> int:
        return TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4

    def candidate_mask(self) -> np.ndarray:
        return np.concatenate(
            (
                np.asarray(self.futures_candidate_mask, dtype=bool),
                np.asarray(self.option_candidate_mask, dtype=bool),
            ),
            axis=1,
        )

    def simple_returns(self) -> np.ndarray:
        return np.concatenate(
            (
                np.asarray(self.futures_simple_returns, dtype=np.float32),
                np.asarray(self.option_simple_returns, dtype=np.float32),
            ),
            axis=1,
        )

    def select_dates(
        self, requested_dates: Iterable[object]
    ) -> "TaiwanIndexDerivativeDayCandidates":
        requested = np.asarray(list(requested_dates), dtype="datetime64[D]")
        if requested.ndim != 1 or requested.size == 0:
            raise ValueError("requested_dates must be a non-empty vector")
        indices = np.searchsorted(self.dates, requested)
        if bool(np.any(indices >= self.dates.size)) or not np.array_equal(
            self.dates[indices], requested
        ):
            raise ValueError("candidate panel does not cover every requested date")
        selected_chain = self.source_chain.select_dates(requested)
        remapped = np.full(
            (requested.size, TAIFEX_OPTION_CANDIDATE_CAPACITY),
            -1,
            dtype=np.int32,
        )
        original_sparse = np.asarray(self.option_sparse_indices, dtype=np.int64)
        for selected_row, original_row in enumerate(indices):
            old_start = int(self.source_chain.row_offsets[original_row])
            old_stop = int(self.source_chain.row_offsets[original_row + 1])
            new_start = int(selected_chain.row_offsets[selected_row])
            row_sparse = original_sparse[original_row]
            valid = (row_sparse >= old_start) & (row_sparse < old_stop)
            remapped[selected_row, valid] = (
                new_start + row_sparse[valid] - old_start
            ).astype(np.int32)
        return TaiwanIndexDerivativeDayCandidates(
            dates=requested,
            futures_contract_months=np.asarray(self.futures_contract_months)[indices].copy(),
            futures_candidate_mask=np.asarray(self.futures_candidate_mask)[indices].copy(),
            futures_simple_returns=np.asarray(self.futures_simple_returns)[indices].copy(),
            option_candidate_mask=np.asarray(self.option_candidate_mask)[indices].copy(),
            option_candidate_features=np.asarray(self.option_candidate_features)[indices].copy(),
            option_simple_returns=np.asarray(self.option_simple_returns)[indices].copy(),
            option_sparse_indices=remapped,
            source_chain=selected_chain,
        )


def build_causal_derivative_day_candidates(
    futures: TaiwanIndexFuturesDaySession,
    options: TaiwanIndexOptionChainDaySession,
    *,
    fixed_fee_per_contract_per_side_twd: float,
    transaction_tax_rate: float,
    slippage_points_per_side: float,
    reference_product: str = "TX",
) -> TaiwanIndexDerivativeDayCandidates:
    """Build E1..E6 plus prior-session TXO candidates without future masks."""

    if not np.array_equal(futures.dates, options.dates):
        raise ValueError("futures and options calendars must align")
    fee = float(fixed_fee_per_contract_per_side_twd)
    tax = float(transaction_tax_rate)
    slip = float(slippage_points_per_side)
    if any(not math.isfinite(value) or value < 0.0 for value in (fee, tax, slip)):
        raise ValueError("candidate cost inputs must be finite and non-negative")
    (
        tenor_months,
        _tenor_open,
        _tenor_high,
        _tenor_low,
        _tenor_close,
        _tenor_volume,
        tenor_log_returns,
        tenor_tradable,
    ) = futures.require_tenor_panel()
    rows = int(futures.dates.size)
    futures_months = np.full(
        (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS), "", dtype="U6"
    )
    futures_mask = np.zeros(
        (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS), dtype=bool
    )
    futures_returns = np.full(
        (rows, TAIFEX_INDEX_FUTURES_TENOR_SLOTS), np.nan, dtype=np.float32
    )
    option_mask = np.zeros(
        (rows, TAIFEX_OPTION_CANDIDATE_CAPACITY), dtype=bool
    )
    option_features = np.zeros(
        (
            rows,
            TAIFEX_OPTION_CANDIDATE_CAPACITY,
            TAIFEX_OPTION_CANDIDATE_FEATURE_DIM,
        ),
        dtype=np.float32,
    )
    option_returns = np.full(
        (rows, TAIFEX_OPTION_CANDIDATE_CAPACITY), np.nan, dtype=np.float32
    )
    option_sparse = np.full(
        (rows, TAIFEX_OPTION_CANDIDATE_CAPACITY), -1, dtype=np.int32
    )
    reference_col = futures.product_index(reference_product)
    reference_close = np.asarray(futures.close_prices)[:, reference_col]
    option_series = np.asarray(options.option_series)
    option_strikes = np.asarray(options.strikes, dtype=np.float64)
    option_rights = np.asarray(options.option_rights)
    option_open = np.asarray(options.open_prices, dtype=np.float64)
    option_close = np.asarray(options.close_prices, dtype=np.float64)
    option_volume = np.asarray(options.volumes, dtype=np.int64)
    option_executable = np.asarray(options.executable, dtype=bool)

    for row in range(1, rows):
        prior_months = [
            str(value)
            for value in np.asarray(tenor_months[row - 1])
            if str(value)
        ]
        current_month_to_tenor = {
            str(value): index
            for index, value in enumerate(np.asarray(tenor_months[row]))
            if str(value)
        }
        for slot, month in enumerate(prior_months[:TAIFEX_INDEX_FUTURES_TENOR_SLOTS]):
            futures_months[row, slot] = month
            futures_mask[row, slot] = True
            current_tenor = current_month_to_tenor.get(month)
            if current_tenor is None:
                continue
            product_valid = np.asarray(tenor_tradable[row, current_tenor], dtype=bool)
            for product_col in range(len(futures.products)):
                if product_valid[product_col]:
                    futures_returns[row, slot] = np.float32(
                        math.expm1(float(tenor_log_returns[row, current_tenor, product_col]))
                    )
                    break

        prior_reference = float(reference_close[row - 1])
        if not math.isfinite(prior_reference) or prior_reference <= 0.0:
            continue
        prior_selection = options.row_slice(row - 1)
        current_selection = options.row_slice(row)
        current_lookup = {
            (
                str(option_series[index]).strip().upper(),
                float(option_strikes[index]),
                str(option_rights[index]).strip().upper(),
            ): index
            for index in range(current_selection.start, current_selection.stop)
        }
        trading_date = _as_python_date(futures.dates[row])
        candidates: list[tuple[int, date, str, float, str, int]] = []
        for sparse_index in range(prior_selection.start, prior_selection.stop):
            series = str(option_series[sparse_index]).strip().upper()
            expiry = taifex_option_expiry(series)
            if expiry < trading_date:
                continue
            candidates.append(
                (
                    _option_family(series),
                    expiry,
                    series,
                    float(option_strikes[sparse_index]),
                    str(option_rights[sparse_index]).strip().upper(),
                    sparse_index,
                )
            )
        candidates.sort(key=lambda item: (item[0], item[1], item[3], item[4]))
        if len(candidates) > TAIFEX_OPTION_CANDIDATE_CAPACITY:
            raise ValueError(
                f"{futures.dates[row]} has {len(candidates)} prior-known TXO legs, "
                f"exceeding capacity {TAIFEX_OPTION_CANDIDATE_CAPACITY}"
            )
        expiries_by_family = {
            family: sorted({item[1] for item in candidates if item[0] == family})
            for family in range(3)
        }
        for family, expiries in expiries_by_family.items():
            allowed = TAIFEX_OPTION_EXPIRY_SLOTS_BY_FAMILY[family]
            if len(expiries) > allowed:
                family_name = ("monthly", "wednesday_weekly", "friday_weekly")[family]
                raise ValueError(
                    f"{futures.dates[row]} has {len(expiries)} {family_name} "
                    f"expiries, exceeding normalized E1..E{allowed} contract"
                )
        rank_by_family_expiry = {
            (family, expiry): rank
            for family, expiries in expiries_by_family.items()
            for rank, expiry in enumerate(expiries)
        }
        for slot, (family, expiry, series, strike, right, prior_sparse) in enumerate(candidates):
            expiry_rank = rank_by_family_expiry[(family, expiry)]
            dte = max(0, (expiry - trading_date).days)
            option_mask[row, slot] = True
            option_features[row, slot, family] = 1.0
            option_features[row, slot, 3] = float(expiry_rank) / 4.0
            option_features[row, slot, 4] = min(float(dte) / 365.0, 2.0)
            option_features[row, slot, 5] = float(
                np.clip(math.log(strike / prior_reference), -2.0, 2.0)
            )
            option_features[row, slot, 6] = 1.0 if right == "C" else -1.0
            prior_close = float(option_close[prior_sparse])
            prior_volume = int(option_volume[prior_sparse])
            option_features[row, slot, 7] = (
                float(math.log1p(prior_close)) / 10.0
                if math.isfinite(prior_close) and prior_close >= 0.0
                else 0.0
            )
            option_features[row, slot, 8] = float(math.log1p(max(prior_volume, 0))) / 15.0
            current_sparse = current_lookup.get((series, strike, right))
            if current_sparse is None:
                continue
            option_sparse[row, slot] = int(current_sparse)
            open_price = float(option_open[current_sparse])
            close_price = float(option_close[current_sparse])
            if not (
                option_executable[current_sparse]
                and math.isfinite(open_price)
                and open_price > 0.0
                and math.isfinite(close_price)
                and close_price > 0.0
            ):
                continue
            ratio = close_price / open_price
            # Keep the exact unbounded simple P&L.  Fixed costs can exceed a
            # cheap option's premium; clipping here would give the optimizer a
            # free put on transaction costs.
            option_returns[row, slot] = np.float32(
                ratio
                - 1.0
                - (2.0 * fee) / (open_price * float(options.multiplier))
                - tax * ratio
                - (2.0 * slip) / open_price
            )

    return TaiwanIndexDerivativeDayCandidates(
        dates=np.asarray(futures.dates, dtype="datetime64[D]").copy(),
        futures_contract_months=futures_months,
        futures_candidate_mask=futures_mask,
        futures_simple_returns=futures_returns,
        option_candidate_mask=option_mask,
        option_candidate_features=option_features,
        option_simple_returns=option_returns,
        option_sparse_indices=option_sparse,
        source_chain=options,
    )


__all__ = [
    "TAIFEX_DERIVATIVE_CANDIDATE_CONTRACT_VERSION",
    "TAIFEX_INDEX_DERIVATIVE_ACTION_COUNT_V4",
    "TAIFEX_OPTION_CANDIDATE_CAPACITY",
    "TAIFEX_OPTION_CANDIDATE_FEATURE_DIM",
    "TAIFEX_OPTION_EXPIRY_SLOTS_BY_FAMILY",
    "TaiwanIndexDerivativeDayCandidates",
    "build_causal_derivative_day_candidates",
]
