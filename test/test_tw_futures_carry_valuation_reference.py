from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_release import bind_margin_carry_opening_valuation


def inputs():
    day = date(2011, 5, 19)
    frame = pl.DataFrame(dict(date=[day], physical_contract=["JI1:201106"], product=["JI1"],
        contract=["201106"], open=[None], observed_open=[None], executable=[False]),
        schema_overrides={"open": pl.Float64, "observed_open": pl.Float64})
    rules = pl.DataFrame(dict(date=[day], physical_contract=["JI1:201106"],
        contract_multiplier=[2102.4327], opening_time=["08:45:00"], opening_contract_value_twd=[113782.],
        carry_from_physical_contract=["JIF:201106"], carry_previous_value_twd=[123000.],
        carry_cash_twd=[9218.], carry_quantity_numerator=[1], carry_quantity_denominator=[1]))
    terms = pl.DataFrame(dict(product=["JI1"], contract=["201106"], effective_date=["2011-05-19"],
        valid_until_exclusive=["2011-06-16"], contract_multiplier=[2102.4327], deliverable_cash_twd=[500.],
        known_at=["2011-05-18T12:00:00+08:00"], source_content_sha256s=[["a"*64]],
        subscription_rights_at_final_settlement=[False]))
    return frame, rules, terms


def test_known_inventory_value_subtracts_deliverable_cash_without_a_fill():
    frame, rules, terms = inputs()
    frame = frame.with_columns(pl.lit(None, dtype=pl.Float64).alias("contract_multiplier"))
    result = bind_margin_carry_opening_valuation(frame, rules, terms)
    assert result["open"][0] == pytest.approx((123000.-9218.-500.)/2102.4327)
    assert result["observed_open"][0] is None and not result["executable"][0]
    assert result["opening_is_carry_valuation"][0]
    assert frame["open"][0] is None


@pytest.mark.parametrize("mutation", ["late", "wrong_units", "wrong_value"])
def test_late_or_inconsistent_inventory_evidence_is_rejected(mutation):
    frame, rules, terms = inputs()
    if mutation == "late":
        terms = terms.with_columns(pl.lit("2011-05-19T09:00:00+08:00").alias("known_at"))
    elif mutation == "wrong_units":
        terms = terms.with_columns(pl.lit(2000.).alias("contract_multiplier"))
    else:
        rules = rules.with_columns(pl.lit(120000.).alias("opening_contract_value_twd"))
    with pytest.raises(ValueError, match="prior-known"):
        bind_margin_carry_opening_valuation(frame, rules, terms)


def test_execution_price_and_unowned_null_are_never_replaced():
    frame, rules, terms = inputs()
    quoted = frame.with_columns(pl.lit(55.).alias("open"), pl.lit(True).alias("executable"))
    assert bind_margin_carry_opening_valuation(quoted, rules, terms).equals(quoted)
    unowned = rules.with_columns(pl.lit("").alias("carry_from_physical_contract"))
    assert bind_margin_carry_opening_valuation(frame, unowned, terms).equals(frame)


@pytest.mark.parametrize("reachable", [False, True])
def test_unowned_reference_requires_the_compiler_proof_of_no_reachable_inventory(reachable):
    frame, rules, terms = inputs()
    rules = rules.with_columns(pl.lit("").alias("carry_from_physical_contract"),
        pl.lit(0.).alias("carry_cash_twd"), pl.lit(reachable).alias("inventory_entry_reachable"),
        pl.lit(False).alias("inventory_origin_unresolved"),
        pl.col("opening_contract_value_twd").alias("known_margin_contract_value_twd"))
    result = bind_margin_carry_opening_valuation(frame, rules, terms)
    assert result["open"][0] is None if reachable else result["open"][0] > 0
    assert not result["executable"][0] and result["observed_open"][0] is None
