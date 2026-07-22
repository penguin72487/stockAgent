from __future__ import annotations

from datetime import date

import polars as pl

from scripts.build_shioaji_hft_dataset import NS_PER_SECOND, build_hft_frame


def _books(base_ns: int) -> pl.LazyFrame:
    rows = []
    for offset, stale in ((1, False), (2, False), (3, True)):
        row: dict[str, object] = {
            "snapshot_ts_ns": base_ns + offset * NS_PER_SECOND,
            "exchange": "TSE",
            "code": "2330",
            "trade_date": date(2026, 7, 20),
            "book_exchange_ts_ns": base_ns + offset * NS_PER_SECOND - 2,
            "book_receive_ts_ns": base_ns + offset * NS_PER_SECOND - 1,
            "book_age_ms": 1.0 if not stale else 6_000.0,
            "stale": stale,
            "suspend": False,
        }
        for level in range(1, 6):
            row[f"bid_price_{level}"] = 99.0 + offset - (level - 1)
            row[f"ask_price_{level}"] = 101.0 + offset + (level - 1)
            row[f"bid_volume_{level}"] = 10 + level
            row[f"ask_volume_{level}"] = 5 + level
        rows.append(row)
    return pl.DataFrame(rows).lazy()


def _ticks(base_ns: int) -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "code": ["2330"],
            "event_seq": [1],
            "exchange_ts_ns": [base_ns + NS_PER_SECOND + 100],
            # This event is unavailable at snapshot +1s and becomes causal at +2s.
            "receive_ts_ns": [base_ns + NS_PER_SECOND + 200_000_000],
            "close": [101.0],
            "volume": [2],
            "amount": [202_000.0],
            "tick_type": [1],
            "simtrade": [False],
            "intraday_odd": [False],
        }
    ).lazy()


def _book_events(base_ns: int) -> pl.LazyFrame:
    row: dict[str, object] = {
        "code": "2330",
        "receive_ts_ns": base_ns + NS_PER_SECOND + 300_000_000,
        "simtrade": False,
        "intraday_odd": False,
    }
    for level in range(1, 6):
        row[f"diff_bid_vol_{level}"] = level
        row[f"diff_ask_vol_{level}"] = -level
    return pl.DataFrame([row]).lazy()


def test_hft_features_are_receive_time_causal_and_labels_require_fresh_future() -> None:
    base_ns = 1_800_000_000 * NS_PER_SECOND
    universe = pl.DataFrame(
        {
            "code": ["2330"],
            "market_cap_rank": [1],
            "name": ["台積電"],
            "market": ["twse"],
            "universe_as_of_date": [date(2026, 7, 17)],
        }
    ).lazy()
    result = build_hft_frame(
        books=_books(base_ns),
        ticks=_ticks(base_ns),
        book_events=_book_events(base_ns),
        universe=universe,
        session_start_ns=base_ns,
        session_end_ns=base_ns + 4 * NS_PER_SECOND,
        max_book_age_ms=5_000.0,
        horizons=(1,),
    ).collect()

    assert result.height == 3
    first, second, third = result.iter_rows(named=True)
    assert first["trade_count_1s"] == 0
    assert second["trade_count_1s"] == 1
    assert second["book_update_count_1s"] == 1
    assert second["last_trade_receive_ts_ns"] <= second["snapshot_ts_ns"]
    assert first["feature_valid"] is True
    assert second["feature_valid"] is True
    assert third["feature_valid"] is False
    assert first["label_valid_1s"] is True
    assert second["label_valid_1s"] is False
    assert first["future_mid_log_return_1s"] is not None
    assert second["future_mid_log_return_1s"] is None


def test_hft_builder_excludes_session_end_snapshot() -> None:
    base_ns = 1_800_000_000 * NS_PER_SECOND
    universe = pl.DataFrame(
        {
            "code": ["2330"],
            "market_cap_rank": [1],
            "name": ["台積電"],
            "market": ["twse"],
            "universe_as_of_date": [date(2026, 7, 17)],
        }
    ).lazy()
    result = build_hft_frame(
        books=_books(base_ns),
        ticks=_ticks(base_ns),
        book_events=_book_events(base_ns),
        universe=universe,
        session_start_ns=base_ns,
        session_end_ns=base_ns + 3 * NS_PER_SECOND,
        max_book_age_ms=5_000.0,
        horizons=(1,),
    ).collect()
    assert result["snapshot_ts_ns"].to_list() == [
        base_ns + NS_PER_SECOND,
        base_ns + 2 * NS_PER_SECOND,
    ]
