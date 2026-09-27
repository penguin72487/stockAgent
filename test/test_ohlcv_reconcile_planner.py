"""Local-only contracts for bounded source requests; never fill absent candles."""
from datetime import datetime, timedelta, timezone
from itertools import combinations

import polars as pl
import pytest

from downloader import ohlcv_hot_tail as hot

START = 1_767_225_600_000
MINUTE = 60_000


def frame(minutes):
    return pl.DataFrame({"date": [
        (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        for i in minutes
    ]})


def plan(minutes, start=0, end=12):
    return hot.plan_candle_reconcile_windows(
        frame(minutes), earliest_ms=START + min(minutes) * MINUTE,
        latest_ms=START + max(minutes) * MINUTE,
        start_ms=START + start * MINUTE, end_ms=START + end * MINUTE,
        interval_ms=MINUTE,
    )


def test_every_absent_minute_is_requested_across_255_sparse_tapes_and_clipped_ranges():
    for length in range(1, 9):
        for minutes in combinations(range(8), length):
            for start, end in [(0, 12), (2, 5), (6, 9), (9, 8)]:
                windows = plan(minutes, start, end)
                assert windows is not None
                covered = set()
                for lo, hi in windows:
                    assert START + start * MINUTE <= lo <= hi <= START + end * MINUTE
                    assert lo % MINUTE == hi % MINUTE == 0
                    covered.update(range((lo - START) // MINUTE, (hi - START) // MINUTE + 1))
                assert set(range(start, end + 1)) - set(minutes) <= covered
                assert all(b[0] > a[1] + MINUTE for a, b in zip(windows, windows[1:]))


def test_adjacent_gaps_and_revision_tail_are_coalesced_without_duplicate_work():
    assert plan([0, 2, 4, 6], end=8) == [(START, START + 8 * MINUTE)]


def test_listing_between_minutes_uses_first_legal_candle_boundary():
    result = hot.plan_candle_reconcile_windows(
        frame([1, 2, 3]), earliest_ms=START + MINUTE, latest_ms=START + 3 * MINUTE,
        start_ms=START + 59_000, end_ms=START + 4 * MINUTE + 59_999,
        interval_ms=MINUTE,
    )
    assert result == [(START + 2 * MINUTE, START + 4 * MINUTE)]


@pytest.mark.parametrize("damage", ["duplicate", "null", "invalid", "nanosecond", "off_grid", "bounds", "missing_date", "empty"])
def test_invalid_dates_never_authorize_skipping_existing_middle(damage):
    data = frame([1, 2, 3])
    if damage == "duplicate":
        data = pl.concat([data, data.slice(1, 1)])
    elif damage in {"null", "invalid", "nanosecond", "off_grid"}:
        values = data["date"].to_list()
        values[1] = {"null": None, "invalid": "invalid", "nanosecond": values[1] + ".000000001", "off_grid": "2026-01-01 00:02:30"}[damage]
        data = pl.DataFrame({"date": values})
    elif damage == "missing_date":
        data = data.rename({"date": "other"})
    elif damage == "empty":
        data = data.head(0)
    bounds = dict(earliest_ms=START + MINUTE, latest_ms=START + (4 if damage == "bounds" else 3) * MINUTE)
    assert hot.plan_candle_reconcile_windows(
        data, **bounds, start_ms=START, end_ms=START + 10 * MINUTE, interval_ms=MINUTE,
    ) is None
    assert not hot.has_contiguous_timestamps(data, **bounds, interval_ms=MINUTE)


def test_bybit_strict_middle_contract_does_not_accept_gap_aware_okx_tape():
    data = frame([0, 1, 3, 4])
    assert not hot.has_contiguous_timestamps(data, earliest_ms=START, latest_ms=START + 4 * MINUTE, interval_ms=MINUTE)
    assert plan([0, 1, 3, 4], end=5) == [(START + MINUTE, START + 5 * MINUTE)]
