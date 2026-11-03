from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_weekly_history_scope import summarize


def _row(day, physical, contract, first, *, volume=1, observed=True, executable=True, account=False):
    return {"date": day, "physical_contract": physical, "product": "MTX", "contract": contract,
            "series_type": "weekly", "first_observed_date": first, "source_row_observed": observed,
            "executable": executable, "outright_volume": volume, "accounting_only_date": account}


def _fixture():
    first, second = date(2020, 1, 1), date(2020, 1, 2)
    warmup = pl.DataFrame([_row(first, "one", "202001W1", first, volume=3)])
    daily = pl.DataFrame([_row(second, "one", "202001W1", first)])
    entry = pl.DataFrame([{"date": second, "physical_contract": "one", "status": "observed_minute",
                           "entry_volume": 1.0}])
    return daily, warmup, entry


def test_first_observation_and_window_are_separate_facts():
    daily, warmup, entry = _fixture()
    result = summarize(daily, warmup, {"08:45": entry, "09:00": entry.clear()})
    first = result["first_observation"]
    assert first["first_observation_also_has_later_release"] == 1
    assert first["positive_outright_volume_on_first_day"] == 1
    assert first["release_first_row_delay_calendar_days"] == [{"delay_calendar_days": 1, "len": 1}]
    assert result["clocks"][0]["status_counts"][0]["positive_window_prints"] == 1
    assert result["clocks"][1]["status_counts"][0]["status"] == "missing_from_clock"


def test_duplicate_release_and_entry_identities_fail():
    daily, warmup, entry = _fixture()
    with pytest.raises(ValueError, match="duplicate weekly"):
        summarize(pl.concat([daily, daily]), warmup, {"08:45": entry})
    with pytest.raises(ValueError, match="duplicate entry"):
        summarize(daily, warmup, {"08:45": pl.concat([entry, entry])})


def test_weekly_label_must_match_delivery_format_and_warmup_must_not_overlap():
    daily, warmup, entry = _fixture()
    with pytest.raises(ValueError, match="contract format"):
        summarize(daily.with_columns(pl.lit("202001").alias("contract")), warmup, {"08:45": entry})
    with pytest.raises(ValueError, match="overlap"):
        summarize(daily, pl.concat([warmup, daily]), {"08:45": entry})
