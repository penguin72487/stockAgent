from __future__ import annotations

import numpy as np
import pytest

from stockagent.backtest.tw_commission_rebate import commission_rebate_calendar


def test_commission_rebate_calendar_tracks_months_across_year_boundary() -> None:
    month_ids, payment_eligible = commission_rebate_calendar(
        np.asarray(
            [
                "2025-12-12",
                "2025-12-15",
                "2025-12-31",
                "2026-01-02",
                "2026-01-16",
            ],
            dtype="datetime64[D]",
        )
    )

    assert month_ids.dtype == np.int64
    assert payment_eligible.dtype == np.bool_
    np.testing.assert_array_equal(
        month_ids,
        [
            2025 * 12 + 12,
            2025 * 12 + 12,
            2025 * 12 + 12,
            2026 * 12 + 1,
            2026 * 12 + 1,
        ],
    )
    np.testing.assert_array_equal(
        payment_eligible,
        [False, True, True, False, True],
    )


def test_monthly_payment_eligibility_defers_to_first_observed_session_after_15th() -> None:
    # 2025-06-15 was a Sunday.  The executor sees exchange sessions only, so
    # the first eligible row is Monday 2025-06-16.
    _, payment_eligible = commission_rebate_calendar(
        ["2025-06-13", "2025-06-16", "2025-06-17"]
    )

    np.testing.assert_array_equal(payment_eligible, [False, True, True])


def test_commission_rebate_calendar_accepts_empty_input() -> None:
    month_ids, payment_eligible = commission_rebate_calendar(
        np.asarray([], dtype="datetime64[D]")
    )

    assert month_ids.shape == (0,)
    assert month_ids.dtype == np.int64
    assert payment_eligible.shape == (0,)
    assert payment_eligible.dtype == np.bool_


@pytest.mark.parametrize(
    "bad_dates",
    [
        np.asarray([["2025-01-02"]], dtype="datetime64[D]"),
        ["2025-01-02", "2025-01-02"],
        ["2025-01-03", "2025-01-02"],
        np.asarray(["2025-01-02", "NaT"], dtype="datetime64[D]"),
        ["not-a-date"],
    ],
)
def test_commission_rebate_calendar_rejects_ambiguous_or_invalid_dates(
    bad_dates: object,
) -> None:
    with pytest.raises(ValueError, match="dates|NaT|increasing|calendar"):
        commission_rebate_calendar(bad_dates)  # type: ignore[arg-type]
