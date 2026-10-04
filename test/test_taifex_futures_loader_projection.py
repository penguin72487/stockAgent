"""Product pushdown must retain the full calendar and all selected tenors."""
from dataclasses import fields
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.data.tw_index_futures import (
    build_taifex_index_futures_day_session,
    load_taifex_index_futures_day_session as load,
)
from test_tw_index_futures_data import _row, _write_csv


@pytest.fixture
def source(tmp_path: Path):
    raw = tmp_path / "futures.csv"
    _write_csv(raw, [
        _row("2025/01/02", "MTX", "202501", 19000, 19100),
        _row("2025/01/03", "TX", "202501", 20000, 20100),
        _row("2025/01/03", "TX", "202502", 20500, 20600),
        _row("2025/01/03", "MTX", "202501", 20000, 20100),
        _row("2025/01/06", "TX", "202501", 22000, 22100),
    ])
    return build_taifex_index_futures_day_session([raw], tmp_path / "futures.parquet")


def equal(left, right):
    for field in fields(left):
        a, b = getattr(left, field.name), getattr(right, field.name)
        if isinstance(a, np.ndarray):
            assert a.dtype == b.dtype, field.name
            np.testing.assert_array_equal(a, b, err_msg=field.name)
        else:
            assert a == b, field.name


def replace_column(source, table, name, values):
    pq.write_table(table.set_column(table.schema.get_field_index(name), name, values), source)


def test_selected_product_keeps_other_only_date_and_full_tenors(source):
    result = load(source, products=("TX",))
    np.testing.assert_array_equal(result.dates, np.array(["2025-01-02", "2025-01-03", "2025-01-06"], dtype="datetime64[D]"))
    np.testing.assert_array_equal(result.open_prices[:, 0], [np.nan, 20000, 22000])
    assert result.tenor_contract_months[1, :2].tolist() == ["202501", "202502"]
    np.testing.assert_array_equal(result.tenor_open_prices[1, :2, 0], [20000, 20500])
    assert result.require_tenor_panel()


@pytest.mark.parametrize("kind", ["string", "large_string", "dictionary"])
@pytest.mark.parametrize("padding", ["", " \t", "\x1c", "\u2003"])
def test_python_product_normalization_and_chunked_column_are_preserved(source, kind, padding):
    expected = load(source, products=("TX",))
    table = pq.read_table(source)
    values = [padding + value.lower() + padding for value in table["product"].to_pylist()]
    dtype = pa.large_string() if kind == "large_string" else pa.string()
    array = pa.chunked_array([pa.array(values[:2], type=dtype), pa.array(values[2:], type=dtype)])
    if kind == "dictionary":
        array = array.dictionary_encode()
    replace_column(source, table, "product", array)
    equal(expected, load(source, products=("TX",)))


@pytest.mark.parametrize("kind", ["binary", "integer", "null", "missing_selected", "nullable"])
def test_empty_selection_and_noncanonical_types_keep_python_behavior(source, kind):
    table = pq.read_table(source)
    if kind == "binary":
        values = pa.array([x.encode() for x in table["product"].to_pylist()])
    elif kind == "integer":
        values = pa.array([1] * table.num_rows)
    elif kind == "null":
        values = pa.nulls(table.num_rows)
    elif kind == "nullable":
        values = pa.array([None if x == "MTX" else x for x in table["product"].to_pylist()])
    else:
        values = pa.array(["OTHER"] * table.num_rows)
    replace_column(source, table, "product", values)
    result = load(source, products=("TX",))
    assert len(result.dates) == 3
    if kind == "nullable":
        np.testing.assert_array_equal(result.open_prices[:, 0], [np.nan, 20000, 22000])
    else:
        assert np.isnan(result.open_prices).all()
        assert not result.tradable_mask.any()
        assert not result.tenor_tradable_mask.any()


@pytest.mark.parametrize("which", ["front", "tenor", "other"])
def test_selected_duplicate_checks_are_not_skipped(source, which):
    table = pq.read_table(source)
    values = table.to_pylist()
    idx = next(i for i, row in enumerate(values) if (
        row["product"] == ("MTX" if which == "other" else "TX")
        and bool(row["is_front_month"]) == (which != "tenor")))
    pq.write_table(pa.concat_tables([table, table.slice(idx, 1)]), source)
    if which == "other":
        assert len(load(source, products=("TX",)).dates) == 3
    else:
        with pytest.raises(ValueError, match="duplicate normalized row" if which == "front" else "duplicate tenor row"):
            load(source, products=("TX",))


@pytest.mark.parametrize("failure", ["multiplier", "metadata", "missing_column", "duplicate_panel_date", "unsorted_panel_date"])
def test_original_source_and_calendar_errors_remain_visible(source, failure):
    table = pq.read_table(source)
    kwargs = {"products": ("TX",)}
    if failure == "multiplier":
        replace_column(source, table, "multiplier", pa.array([1] * table.num_rows))
    elif failure == "metadata":
        pq.write_table(table.replace_schema_metadata({}), source)
    elif failure == "missing_column":
        pq.write_table(table.drop(["volume"]), source)
    elif failure == "duplicate_panel_date":
        kwargs["panel_dates"] = np.array(["2025-01-03", "2025-01-03"], dtype="datetime64[D]")
    else:
        kwargs["panel_dates"] = np.array(["2025-01-06", "2025-01-03"], dtype="datetime64[D]")
    with pytest.raises(ValueError):
        load(source, **kwargs)


def test_panel_date_selection_keeps_requested_missing_rows(source):
    dates = np.array(["2025-01-01", "2025-01-03", "2025-01-07"], dtype="datetime64[D]")
    result = load(source, products=("TX",), panel_dates=dates)
    np.testing.assert_array_equal(result.dates, dates)
    np.testing.assert_array_equal(result.open_prices[:, 0], [np.nan, 20000, np.nan])
    assert result.tenor_open_prices[1, 1, 0] == 20500


def test_unselected_invalid_date_is_not_hidden_by_panel_dates(source):
    table = pq.read_table(source)
    dates = ["invalid-date" if row["product"] == "MTX" else str(row["date"])
             for row in table.to_pylist()]
    replace_column(source, table, "date", pa.array(dates))
    with pytest.raises(ValueError):
        load(source, products=("TX",), panel_dates=np.array(["2025-01-03"], dtype="datetime64[D]"))


def test_front_month_truthy_strings_retain_python_semantics(source):
    expected = load(source, products=("TX",))
    table = pq.read_table(source)
    # Nonempty "False" is truthy in the existing Python contract, not Arrow false.
    values = ["False" if value else "" for value in table["is_front_month"].to_pylist()]
    replace_column(source, table, "is_front_month", pa.array(values))
    equal(expected, load(source, products=("TX",)))
