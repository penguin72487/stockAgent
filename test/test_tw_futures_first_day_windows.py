from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_first_day_windows import inspect_windows
from scripts.verify_tw_futures_first_day_windows import raw_counts, verify_rows


def _targets():
    return pl.DataFrame([{"date": date(2020, 1, 1), "physical_contract": "MTX:202001W1",
                          "product": "MTX", "contract": "202001W1"}])


def _ticks():
    return pl.DataFrame([
        {"date": "2020-01-01 08:45:59", "futures_id": "MTX", "contract_date": "202001W1", "price": 100., "volume": 2},
        {"date": "2020-01-01 08:46:00", "futures_id": "MTX", "contract_date": "202001W1", "price": 101., "volume": 2},
        {"date": "2020-01-01 08:47:00", "futures_id": "MTX", "contract_date": "202001W1", "price": 102., "volume": 2},
        {"date": "2020-01-01 09:01:59", "futures_id": "MTX", "contract_date": "202001W1", "price": 103., "volume": 3},
        {"date": "2020-01-01 09:01:00", "futures_id": "TX", "contract_date": "202001W1", "price": 99., "volume": 2},
    ])


def test_exact_physical_identity_and_left_closed_windows():
    row = inspect_windows(_ticks(), _targets(), date(2020, 1, 1)).row(0, named=True)
    assert row["0845_raw_print_rows"] == 1
    assert row["0845_invalid_raw_rows"] == 0
    assert row["0900_raw_print_rows"] == 1
    assert row["0900_invalid_raw_rows"] == 1


def test_missing_raw_print_is_not_invented():
    row = inspect_windows(_ticks().clear(), _targets(), date(2020, 1, 1)).row(0, named=True)
    assert row["0845_raw_print_rows"] == row["0900_raw_print_rows"] == 0
    assert row["0845_first_raw_timestamp"] is None


def test_ambiguous_contract_and_wrong_receipt_date_fail():
    with pytest.raises(ValueError, match="ambiguous"):
        inspect_windows(_ticks(), pl.concat([_targets(), _targets()]), date(2020, 1, 1))
    with pytest.raises(ValueError, match="receipt calendar"):
        inspect_windows(_ticks().with_columns(pl.col("date").str.replace("2020-01-01", "2020-01-02")),
                        _targets(), date(2020, 1, 1))


def test_independent_arrow_readback_detects_changed_counts(tmp_path):
    path = tmp_path / "ticks.parquet"
    _ticks().write_parquet(path)
    row = inspect_windows(_ticks(), _targets(), date(2020, 1, 1)).row(0, named=True)
    counts = raw_counts(path, "2020-01-01", {("MTX", "202001W1")})
    assert verify_rows([row], counts) == []
    row["0845_raw_print_rows"] += 1
    assert verify_rows([row], counts)[0]["reason"] == "0845_count_or_time"
