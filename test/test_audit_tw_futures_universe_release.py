from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_universe_release import inventory_scope, validate_clock_join


def _facts(rows):
    return pl.DataFrame(rows, schema={"date": pl.Date, "physical_contract": pl.String,
                                     "symbol": pl.String}, orient="row")


def _entries(rows):
    return pl.DataFrame(rows, schema={"date": pl.Date, "physical_contract": pl.String,
                                     "clock_symbol": pl.String, "status": pl.String}, orient="row")


def test_universe_scope_reports_omitted_and_extra_products():
    catalog = pl.DataFrame({"product": ["AA", "BB", "CC"]})
    assert inventory_scope(catalog, ["BB", "DD"]) == (["AA", "CC"], ["DD"])


@pytest.mark.parametrize("catalog,requested", [
    (["AA", "AA"], ["AA"]),
    (["AA"], ["AA", "AA"]),
    (["AA"], []),
    (["AA", ""], ["AA"]),
])
def test_universe_scope_rejects_missing_or_ambiguous_identities(catalog, requested):
    with pytest.raises(ValueError, match="unique nonempty"):
        inventory_scope(pl.DataFrame({"product": catalog}), requested)


def test_clock_join_keeps_zero_fill_status_with_matching_identity():
    day = date(2020, 1, 2)
    facts = _facts([(day, "AA:202001", "AA")])
    entries = _entries([(day, "AA:202001", "AA", "verified_no_window_trade")])
    assert validate_clock_join(facts, entries)["status"].to_list() == ["verified_no_window_trade"]


@pytest.mark.parametrize("entries", [
    [],
    [(date(2020, 1, 3), "AA:202001", "AA", "observed_minute")],
    [(date(2020, 1, 2), "AA:202002", "AA", "observed_minute")],
    [(date(2020, 1, 2), "AA:202001", "BB", "observed_minute")],
    [(date(2020, 1, 2), "AA:202001", None, "observed_minute")],
    [(date(2020, 1, 2), "AA:202001", "AA", None)],
    [(date(2020, 1, 2), "AA:202001", "AA", "observed_minute"),
     (date(2020, 1, 3), "BB:202001", "BB", "observed_minute")],
])
def test_clock_join_rejects_missing_extra_or_mismatched_rows(entries):
    facts = _facts([(date(2020, 1, 2), "AA:202001", "AA")])
    with pytest.raises(ValueError, match="axes differ"):
        validate_clock_join(facts, _entries(entries))


def test_clock_join_rejects_duplicate_physical_contracts():
    day = date(2020, 1, 2)
    facts = _facts([(day, "AA:202001", "AA")])
    entry = (day, "AA:202001", "AA", "observed_minute")
    with pytest.raises(ValueError, match="duplicated"):
        validate_clock_join(facts, _entries([entry, entry]))
