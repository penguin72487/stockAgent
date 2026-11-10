"""Explicit inference decisions, separate from immutable feature history."""
from __future__ import annotations

import hashlib
import numpy as np

CONTRACT = "tw_current_year_formal_history_v1"



def year_decision_mask(dates, year: int | None) -> np.ndarray:
    days = np.asarray(dates, dtype="datetime64[D]")
    if days.ndim != 1 or not days.size or np.isnat(days).any() or np.any(days[1:] <= days[:-1]):
        raise ValueError("inference scope requires ordered unique real session dates")
    if year is None:
        return np.ones(days.size, dtype=bool)
    if type(year) is not int or not 1900 <= year <= 9999:
        raise ValueError("inference year must be a canonical calendar year")
    mask = days.astype("datetime64[Y]").astype(int) + 1970 == year
    if not mask.any():
        raise ValueError(f"inference year {year} has no observed sessions")
    return mask


def year_scope_receipt(dates, year: int) -> dict:
    days = np.asarray(dates, dtype="datetime64[D]")
    mask = year_decision_mask(days, year)
    return {"contract": CONTRACT, "calendar_year": year,
            "first_materialized_session": str(days[mask][0]),
            "last_materialized_session": str(days[mask][-1]),
            "materialized_sessions": int(mask.sum()), "reference_calendar_sessions": int(days.size),
            "unrequested_sessions_are_zero_trades": False,
            "account_initialization": "fresh_owned_test_split_only"}


def require_complete_year_split(dates, year: int, indices, test_years) -> dict:
    mask = year_decision_mask(dates, year)
    requested = np.asarray(indices)
    if (list(test_years) != [year] or requested.dtype.kind not in "iu"
            or not np.array_equal(requested, np.flatnonzero(mask))):
        raise ValueError("year-only inference requires the complete fresh owned test-year split")
    return year_scope_receipt(dates, year)


def scoped_source_digest(digest: str, year: int) -> str:
    return hashlib.sha256(f"{CONTRACT}:{year}:{digest}".encode()).hexdigest()
