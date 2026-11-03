"""Candidate partitions preserve the expiry date and next physical month."""
from datetime import date

import polars as pl
import pytest

from scripts.plan_tx_settlement_reopen_priority import candidate_items


def inputs():
    days = [date(2011, 1, 3), date(2011, 1, 19), date(2011, 1, 20), date(2012, 1, 2)]
    reference = pl.DataFrame({"date": days, "contract": ["201101", "201101", "201102", "201102"],
                              "front_month_roll": [False, False, True, False],
                              "cash_settlement_event": [False, True, False, False]})
    raw = pl.DataFrame({"date": [*days, days[1]], "product": ["TX"] * 5,
                        "contract": ["201101", "201101", "201102", "201102", "201102"],
                        "source_row_observed": [True] * 5, "close": [100.] * 5})
    return reference, raw


def test_exact_expiry_and_yearly_entry_candidates_are_not_receipts():
    reference, raw = inputs()
    items, summary = candidate_items(reference, raw)
    assert items == [
        {"dataset": "tw_futures_minute:TX_201101", "partition": "2011-01-03"},
        {"dataset": "tw_futures_minute:TX_201102", "partition": "2011-01-19"},
        {"dataset": "tw_futures_minute:TX_201102", "partition": "2012-01-02"},
    ]
    assert summary["settlement_reopen_candidate_days"] == 1
    assert summary["yearly_first_entry_candidate_days"] == 2


def test_missing_new_contract_on_expiry_is_not_invented():
    reference, raw = inputs()
    with pytest.raises(ValueError, match="observed next-month"):
        candidate_items(reference, raw.head(4))


def test_roll_without_expiry_or_with_duplicate_raw_identity_is_rejected():
    reference, raw = inputs()
    with pytest.raises(ValueError, match="same-day cash-settlement"):
        candidate_items(reference.with_columns(pl.lit(False).alias("cash_settlement_event")), raw)
    with pytest.raises(ValueError, match="duplicate"):
        candidate_items(reference, pl.concat([raw, raw.head(1)]))
