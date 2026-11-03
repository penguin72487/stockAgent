from datetime import date

import polars as pl
import pytest

from scripts.audit_tw_futures_priority_release import WINDOWS, reconcile, target_frame


def _daily(rows):
    schema = {"date": pl.Date, "product": pl.String, "contract": pl.String,
              "physical_contract": pl.String, "official_volume": pl.Int64,
              "outright_volume": pl.Int64, "official_reason": pl.String,
              "executable": pl.Boolean, "official_source_sha256": pl.String}
    return pl.DataFrame(rows, schema=schema, orient="row")


def _clocks(rows):
    schema = {"date": pl.Date, "physical_contract": pl.String, "status": pl.String,
              "entry_quote": pl.Float64, "entry_volume": pl.Float64}
    return {name: pl.DataFrame(rows, schema=schema, orient="row") for name in WINDOWS}


def test_daily_volume_does_not_imply_a_one_minute_execution():
    day = date(2018, 7, 3)
    targets = target_frame({"contract": "provider_priority_query_v1", "provider": "finlab",
                            "items": [{"dataset": "tw_futures_minute:BRF_201809",
                                       "partition": str(day)}]})
    row = (day, "BRF", "201809", "BRF:first", 12, 10, "outright_volume", True, "a" * 64)
    clocks = _clocks([(day, "BRF:first", "verified_no_window_trade", None, 0.0)])
    clocks["0845_300m"] = _clocks([(day, "BRF:first", "observed_minute", 100.0, 10.0)])["0845_300m"]
    result = reconcile(targets, _daily([row]), _daily([]), clocks)[0]
    assert result["classification"] == "daily"
    assert result["official_volume"] == 12
    assert result["clocks"]["0845_1m"]["status"] == "verified_no_window_trade"
    assert result["clocks"]["0845_300m"]["entry_volume"] == 10.0


def test_warmup_absence_and_ambiguous_physical_generations_are_not_conflated():
    day = date(2018, 7, 2)
    plan = {"contract": "provider_priority_query_v1", "provider": "finlab", "items": [
        {"dataset": f"tw_futures_minute:BRF_{month}", "partition": str(day)}
        for month in ("201809", "201810", "201811")]}
    targets = target_frame(plan)
    def row(month, identity):
        return (day, "BRF", month, identity, 2, 2, "outright_volume", True, "a" * 64)
    result = reconcile(targets, _daily([row("201809", "generation-a"),
                                        row("201809", "generation-b")]),
                       _daily([row("201810", "warmup")]), _clocks([]))
    assert [(r["contract"], r["classification"]) for r in result] == [
        ("201809", "ambiguous_physical_generation"), ("201810", "warmup"), ("201811", "absent")]
    assert result[1]["clocks"]["0845_1m"] is None


def test_duplicate_clock_or_priority_identity_fails_closed():
    day = date(2020, 1, 2)
    item = {"dataset": "tw_futures_minute:TX_202001", "partition": str(day)}
    plan = {"contract": "provider_priority_query_v1", "provider": "finlab", "items": [item, item]}
    with pytest.raises(ValueError, match="unique"):
        target_frame(plan)
    plan["items"] = [item]
    targets = target_frame(plan)
    duplicate = (day, "TX:first", "observed_minute", 100.0, 1.0)
    with pytest.raises(ValueError, match="ambiguous clock"):
        reconcile(targets, _daily([]), _daily([]), _clocks([duplicate, duplicate]))
