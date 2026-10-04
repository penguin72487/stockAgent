"""Research limits must unblock rules without changing financial observations."""
from datetime import date
import json
from pathlib import Path

import polars as pl
import pytest

from stockagent.data.tw_futures_execution_terms import compile_execution_terms
from stockagent.data.tw_futures_position_research import validate_position_research_policy
from test_tw_futures_execution_terms import inputs, specification, SPEC_FIELDS, CORPORATE_SCHEMA, STAMP, DAYS


POLICY = json.loads((Path(__file__).parents[1] / "configs/markets/tw_futures_position_research_v1.json").read_text())


def stock_inputs(*, adjusted=True):
    data = inputs()
    for name in ("frame", "margins", "positions"):
        data[name] = data[name].with_columns(pl.lit("CAF").alias("product"))
    data["frame"] = data["frame"].with_columns(pl.lit("CAF:202603#g0").alias("physical_contract"),
        pl.lit(100.).alias("volume"))
    data["positions"] = data["positions"].with_columns(pl.lit("CAF").alias("position_root_product"),
        pl.lit("contracts").alias("unit"))
    specs = [specification("CAF", contract_multiplier=2000., tick_kind="stock_future")]
    if adjusted:
        extra = data["frame"].with_columns(pl.lit("CA1").alias("product"),
            pl.lit("CA1:202603#g0").alias("physical_contract"))
        data["frame"] = pl.concat([data["frame"], extra])
        data["margins"] = pl.concat([data["margins"], data["margins"].with_columns(pl.lit("CA1").alias("product"))])
        pos = data["positions"].with_columns(pl.lit("CA1").alias("product"),
            pl.lit(False).alias("position_numeric_inputs_resolved"),
            pl.lit(None, dtype=pl.Float64).alias("position_unit"),
            pl.lit(None, dtype=pl.Float64).alias("position_limit"))
        data["positions"] = pl.concat([data["positions"], pos])
        specs.append(specification("CA1", contract_multiplier=2000., tick_kind="stock_future"))
        data["corporate_terms"] = pl.DataFrame([dict(product="CA1", contract="202603",
            effective_date="2025-12-02", valid_until_exclusive=None, from_product="CAF",
            contract_multiplier=2200., deliverable_cash_twd=0., fixed_subscription_rights_twd=None,
            subscription_rights_at_final_settlement=False, known_at=STAMP, source_content_sha256s=["d"*64],
            effective_at="2025-12-02T08:45:00+08:00", equity_cash_credit_twd=0.,
            has_equity_credit_fields=False, carry_quantity_numerator=1, carry_quantity_denominator=1)], schema=CORPORATE_SCHEMA)
    data["specifications"] = pl.DataFrame(specs, schema=SPEC_FIELDS)
    return data


def test_research_repairs_whole_group_in_actual_shares_without_financial_changes():
    data = stock_inputs()
    original, blocked = compile_execution_terms(**data)
    result, after = compile_execution_terms(**data, position_research_policy=POLICY)
    assert blocked.filter(~pl.col("is_warmup") & pl.col("missing_position_limit")).height == 3
    assert not after.filter(~pl.col("is_warmup"))["missing_position_limit"].any()
    assert result["position_research_applied"].all()
    assert result["position_limit"].to_list() == [20000.] * 6
    assert set(result.filter(pl.col("product") == "CA1")["position_unit"]) == {2200.}
    assert set(result.filter(pl.col("product") == "CAF")["position_unit"]) == {2000.}
    allowed = {"position_unit", "position_limit", "position_group", "second_position_unit",
               "second_position_limit", "second_position_group", "known_at", "effective_at"}
    common = [c for c in original.columns if c not in allowed]
    assert original.select(common).equals(result.select(common))
    flags = [c for c in blocked.columns if c not in {"missing_position_limit", "missing_position_clock", "has_blocker"}]
    assert blocked.select(flags).equals(after.select(flags))


def test_previous_cap_is_causal_and_keeps_a_known_tightening():
    data = stock_inputs()
    data["positions"] = data["positions"].with_columns(
        pl.when(pl.col("date") == DAYS[1]).then(pl.lit(str(DAYS[1]) + "T23:59:59+08:00"))
          .otherwise(pl.col("known_at")).alias("known_at"),
        pl.when((pl.col("date") == DAYS[2]) & (pl.col("product") == "CAF")).then(8.)
          .otherwise(pl.col("position_limit")).alias("position_limit"))
    before, _ = compile_execution_terms(**data, position_research_policy=POLICY)
    first = before.filter(pl.col("date") == DAYS[1])
    assert set(first["position_limit"]) == {20000.}
    assert set(first["position_research_seed_date"]) == {DAYS[0]}
    assert set(first["position_research_method"]) == {"previous_known_group_capacity"}
    assert set(before.filter(pl.col("date") == DAYS[2])["position_limit"]) == {16000.}
    data["positions"] = data["positions"].with_columns(
        pl.when(pl.col("date") == DAYS[3]).then(999999.).otherwise(pl.col("position_limit")).alias("position_limit"))
    after, _ = compile_execution_terms(**data, position_research_policy=POLICY)
    assert before.filter(pl.col("date") < DAYS[3]).equals(after.filter(pl.col("date") < DAYS[3]))


def test_initial_assumption_does_not_fabricate_a_missing_quote_or_margin():
    data = stock_inputs(adjusted=False)
    data["positions"] = data["positions"].with_columns(pl.lit(False).alias("position_numeric_inputs_resolved"))
    data["frame"] = data["frame"].with_columns(
        pl.when(pl.col("date") == DAYS[2]).then(None).otherwise(pl.col("settlement")).alias("settlement"))
    data["margins"] = data["margins"].with_columns(
        pl.when(pl.col("date") == DAYS[1]).then(pl.lit("no_prior_interval"))
          .otherwise(pl.col("opening_binding_status")).alias("opening_binding_status"))
    result, flags = compile_execution_terms(**data, position_research_policy=POLICY)
    assert set(result["position_limit"]) == {2500000.}
    assert set(result["position_research_method"]) == {"initial_ordinary_grade_assumption"}
    assert flags.filter(pl.col("date") == DAYS[2])["missing_settlement_value"].all()
    assert flags.filter(pl.col("date") == DAYS[1])["missing_opening_margin"].all()


def test_non_stock_and_valid_strict_rules_are_unchanged():
    data = inputs()
    data["positions"] = data["positions"].with_columns(pl.lit(False).alias("position_numeric_inputs_resolved"))
    old, flags = compile_execution_terms(**data)
    new, after = compile_execution_terms(**data, position_research_policy=POLICY)
    assert old.equals(new.select(old.columns)) and flags.equals(after)
    assert not new["position_research_applied"].any()
    data = stock_inputs(adjusted=False)
    old, flags = compile_execution_terms(**data)
    new, after = compile_execution_terms(**data, position_research_policy=POLICY)
    assert old.equals(new.select(old.columns)) and flags.equals(after)
    assert not new["position_research_applied"].any()


def test_conflicting_capacity_is_floored_and_independent_month_constraints_remain():
    data = stock_inputs()
    data["positions"] = data["positions"].with_columns(
        pl.when(pl.col("product") == "CA1").then(2000.)
          .otherwise(pl.col("position_unit")).alias("position_unit"),
        pl.when(pl.col("product") == "CA1").then(18182.2)
          .otherwise(pl.col("position_limit")).alias("position_limit"),
        pl.when(pl.col("product") == "CA1").then(True)
          .otherwise(pl.col("position_numeric_inputs_resolved")).alias("position_numeric_inputs_resolved"),
        pl.when(pl.col("product") == "CA1").then(None)
          .otherwise(pl.col("monthly_position_limit")).alias("monthly_position_limit"),
        pl.when(pl.col("product") == "CA1").then(2.).otherwise(None).alias("independent_contract_limit"))
    result, flags = compile_execution_terms(**data, position_research_policy=POLICY)
    # The known 7 standard-contract monthly bound intersects the known
    # independent 2-contract CA1 cap, despite the two-axis runtime layout.
    assert set(result["position_limit"]) == {14000.}
    assert set(result.filter(pl.col("product") == "CA1")["second_position_limit"]) == {2.}
    assert not flags.filter(~pl.col("is_warmup"))["missing_position_limit"].any()


@pytest.mark.parametrize("patch", [{"research_only": False}, {"fallback_known_at": "2010-01-15"},
    {"fallback_standard_contracts": float("inf")}, {"standard_shares_per_contract": 1000}])
def test_research_policy_rejects_unversioned_or_unproven_fallbacks(patch):
    with pytest.raises(ValueError):
        validate_position_research_policy(dict(POLICY, **patch))
