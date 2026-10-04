"""Counterexamples for the rule compiler, separate from source admission."""
from datetime import date, timedelta
import json

import polars as pl
import pytest

from stockagent.data.tw_futures_execution_terms import (
    SPEC_FIELDS, bind_product_specifications, compile_execution_terms,
)
from stockagent.data.tw_futures_margin import (
    validate_margin_carry_rules, validate_margin_value_bases,
    validate_margin_second_position_limit, validate_margin_grandfather_rules,
)


STAMP = "2025-12-01T12:00:00+08:00"
DAYS = [date(2026, 1, 5) + timedelta(days=i) for i in range(4)]
CORPORATE_SCHEMA = {
    "product": pl.String, "contract": pl.String, "effective_date": pl.String,
    "valid_until_exclusive": pl.String, "from_product": pl.String,
    "contract_multiplier": pl.Float64, "deliverable_cash_twd": pl.Float64,
    "fixed_subscription_rights_twd": pl.Float64,
    "subscription_rights_at_final_settlement": pl.Boolean,
    "known_at": pl.String, "source_content_sha256s": pl.List(pl.String),
    "effective_at": pl.String, "equity_cash_credit_twd": pl.Float64,
    "has_equity_credit_fields": pl.Boolean, "carry_quantity_numerator": pl.Int64,
    "carry_quantity_denominator": pl.Int64,
}


def specification(product="ABC", **updates):
    row = dict(product=product, effective_date=date(2025, 12, 2), valid_until_exclusive=None,
        known_at=STAMP, source_content_sha256s=["a" * 64], contract_multiplier=100.,
        opening_time="08:45:00", settlement_time="13:45:00", expiry_time="13:30:00",
        tick_kind="fixed", tick_size=1., limit_up_ratio=1.1, limit_down_ratio=.9,
        margin_value_kind="quoted_value", tax_value_kind="quoted_value", tax_fixed_value_twd=None,
        tax_class="stock", settlement_method="cash_settlement",
        position_grandfather_existing=False, second_position_grandfather_existing=False)
    return dict(row, **updates)


def inputs():
    rows = []
    for i, day in enumerate(DAYS):
        rows.append(dict(date=day, product="ABC", contract="202603", physical_contract="ABC:202603#g0",
            previous_market_date=DAYS[i-1] if i else None, next_market_date=DAYS[i+1] if i<3 else None,
            previous_symbol_date=DAYS[i-1] if i else None, open=100.+i, close=102.+i, settlement=102.+i,
            executable=True, cash_settlement=False, official_expiry=date(2026, 3, 18)))
    frame = pl.DataFrame(rows)
    margins = frame.select("date", "product").with_columns(
        *[pl.lit("bound_prior_publication").alias(p + "binding_status") for p in ("opening_", "settlement_")],
        *[pl.lit(0, dtype=pl.UInt32).alias(p + "margin_interval_id") for p in ("opening_", "settlement_")],
        *[pl.lit("notional_rate").alias(p + "margin_kind") for p in ("opening_", "settlement_")],
        *[pl.lit(.1).alias(p + "initial") for p in ("opening_", "settlement_")],
        *[pl.lit(.08).alias(p + "maintenance") for p in ("opening_", "settlement_")],
        *[pl.lit(STAMP).alias(p + "known_at") for p in ("opening_", "settlement_")])
    positions = frame.select("date", "product", "contract").with_columns(
        pl.lit(1.).alias("position_unit"), pl.lit(10.).alias("position_limit"),
        pl.lit(7.).alias("monthly_position_limit"), pl.lit("ABC").alias("position_root_product"),
        pl.lit(True).alias("position_numeric_inputs_resolved"), pl.lit(STAMP).alias("known_at"),
        pl.lit("2025-12-02").alias("effective_date"), pl.lit(0).alias("effective_phase"),
        pl.lit(["b"*64]).alias("source_content_sha256s"))
    levels = pl.DataFrame(dict(effective_date=["2025-12-02"], effective_phase=[0],
        source_content_sha256s=[["c"*64]], known_at=[STAMP], margin_kind=["notional_rate"],
        initial=[.1], maintenance=[.08]))
    terminal = pl.DataFrame(schema={"date":pl.Date, "product":pl.String, "contract":pl.String,
                                  "terminal_value_input_twd":pl.Float64})
    return dict(frame=frame, margins=margins, positions=positions,
        corporate_terms=pl.DataFrame(schema=CORPORATE_SCHEMA), margin_intervals=levels,
        specifications=pl.DataFrame([specification()],schema=SPEC_FIELDS), terminal_values=terminal)


def test_rate_margin_and_carry_keep_different_account_instants():
    rules, blockers = compile_execution_terms(**inputs())
    assert rules.height == 3
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    # No inventory existed during the first source observation/warmup.
    assert rules["carry_from_physical_contract"].to_list() == ["", "ABC:202603#g0", "ABC:202603#g0"]
    row = rules.row(1, named=True)
    assert row["carry_previous_value_twd"] == 10300.
    assert row["carry_previous_initial_twd"] == 1030.
    assert row["carry_previous_maintenance_twd"] == 824.
    assert row["known_margin_value_twd"] == 10300.
    assert row["opening_margin_value_twd"] == 10200.
    assert row["position_group"] == "ABC"
    assert row["second_position_group"] == "ABC:202603"
    assert row["second_position_limit"] == 7.
    for validate in (validate_margin_carry_rules, validate_margin_value_bases,
                     validate_margin_second_position_limit, validate_margin_grandfather_rules):
        validate(rules)


@pytest.mark.parametrize('expiry_time', ['13:30:00', '13:45:00', '14:00:00'])
@pytest.mark.parametrize('post_close_missing', [False, True])
def test_cash_expiry_does_not_move_the_regular_margin_boundary(expiry_time, post_close_missing):
    kwargs = inputs()
    last = DAYS[-1]
    kwargs['frame'] = kwargs['frame'].with_columns(
        (pl.col('date') == last).alias('cash_settlement'),
        pl.lit(last).alias('official_expiry'))
    kwargs['specifications'] = pl.DataFrame([specification(expiry_time=expiry_time)], schema=SPEC_FIELDS)
    old = kwargs['margin_intervals'].row(0, named=True)
    old['effective_phase'] = 1
    new = dict(old, effective_date=str(last), initial=.2, maintenance=.16,
        source_content_sha256s=['d' * 64], known_at=f'{DAYS[-2]}T23:59:59+08:00')
    kwargs['margin_intervals'] = pl.DataFrame([old, new])
    target = pl.col('date') == last
    kwargs['margins'] = kwargs['margins'].with_columns(
        pl.when(target).then(pl.lit(None if post_close_missing else 1, dtype=pl.UInt32))
          .otherwise(pl.col('settlement_margin_interval_id')).alias('settlement_margin_interval_id'),
        pl.when(target).then(pl.lit('missing_numeric_rule' if post_close_missing else 'bound_prior_publication'))
          .otherwise(pl.col('settlement_binding_status')).alias('settlement_binding_status'),
        pl.when(target).then(pl.lit(None if post_close_missing else .2, dtype=pl.Float64))
          .otherwise(pl.col('settlement_initial')).alias('settlement_initial'),
        pl.when(target).then(pl.lit(None if post_close_missing else .16, dtype=pl.Float64))
          .otherwise(pl.col('settlement_maintenance')).alias('settlement_maintenance'))
    rules, flags = compile_execution_terms(**kwargs)
    row = rules.filter(pl.col('date') == last).row(0, named=True)
    blocked = flags.filter(pl.col('date') == last).row(0, named=True)
    assert row['settlement_time'] == expiry_time
    assert row['initial'] == .1
    if expiry_time < '13:45:00':
        assert row['settlement_initial'] == .1
        assert row['settlement_effective_at'] == '2025-12-02T13:45:00+08:00'
        assert not blocked['missing_settlement_margin']
    elif post_close_missing:
        assert blocked['missing_settlement_margin']
    else:
        assert row['settlement_initial'] == .2
        assert row['settlement_effective_at'] == f'{last}T13:45:00+08:00'
        assert not blocked['missing_settlement_margin']


def test_early_cash_event_cannot_admit_a_late_opening_rule_or_unknown_specification():
    kwargs = inputs()
    last = DAYS[-1]
    kwargs['frame'] = kwargs['frame'].with_columns(
        (pl.col('date') == last).alias('cash_settlement'), pl.lit(last).alias('official_expiry'))
    kwargs['margin_intervals'] = kwargs['margin_intervals'].with_columns(
        pl.lit(str(last)).alias('effective_date'),
        pl.lit(f'{last}T13:40:00+08:00').alias('known_at'))
    kwargs['margins'] = kwargs['margins'].with_columns(
        *[pl.lit('same_day_clock_review').alias(p + 'binding_status')
          for p in ['opening_', 'settlement_']],
        *[pl.lit(None, dtype=pl.Float64).alias(p + c)
          for p in ['opening_', 'settlement_'] for c in ['initial', 'maintenance']])
    _, flags = compile_execution_terms(**kwargs)
    blocked = flags.filter(pl.col('date') == last).row(0, named=True)
    assert blocked['missing_opening_margin'] and blocked['missing_settlement_margin']
    kwargs = inputs()
    kwargs['frame'] = kwargs['frame'].with_columns(
        (pl.col('date') == last).alias('cash_settlement'), pl.lit(last).alias('official_expiry'))
    kwargs['specifications'] = kwargs['specifications'].with_columns(pl.lit(last).alias('valid_until_exclusive'))
    _, flags = compile_execution_terms(**kwargs)
    assert flags.filter(pl.col('date') == last)['missing_product_specification'].all()


def test_independent_contract_and_combined_shares_use_existing_two_ledger_slots():
    kwargs=inputs()
    kwargs['positions']=kwargs['positions'].with_columns(
        pl.lit(2000.).alias('position_unit'),pl.lit(2500000.).alias('position_limit'),
        pl.lit(None,dtype=pl.Float64).alias('monthly_position_limit'),
        pl.lit(350.).alias('independent_contract_limit'))
    rules,blockers=compile_execution_terms(**kwargs)
    assert not blockers.filter(~pl.col('is_warmup'))['has_blocker'].any()
    assert rules['position_group'].unique().to_list()==['ABC']
    assert rules['position_limit'].unique().to_list()==[2500000.]
    assert rules['second_position_group'].unique().to_list()==['POSITION_SINGLE:ABC']
    assert rules['second_position_unit'].unique().to_list()==[1.]
    assert rules['second_position_limit'].unique().to_list()==[350.]
    validate_margin_second_position_limit(rules)
    # Each printed obligation excludes a different otherwise feasible order.
    row=rules.row(0,named=True)
    feasible=lambda standard,adjusted: (standard*row['position_unit']+adjusted*2200<=row['position_limit']
                                       and standard*row['second_position_unit']<=row['second_position_limit'])
    assert not feasible(351,0) and not feasible(0,1137)
    assert feasible(350,0) and feasible(0,1136) and feasible(300,800)
    # A monthly constraint would be a third obligation, not a value to lose.
    kwargs['positions']=kwargs['positions'].with_columns(pl.lit(100.).alias('monthly_position_limit'))
    _,rejected=compile_execution_terms(**kwargs)
    assert rejected.filter(~pl.col('is_warmup'))['missing_position_limit'].all()


@pytest.mark.parametrize('cap',[0.,-1.,float('nan'),float('inf')])
def test_invalid_independent_contract_cap_remains_blocked(cap):
    kwargs=inputs()
    kwargs['positions']=kwargs['positions'].with_columns(
        pl.lit(None,dtype=pl.Float64).alias('monthly_position_limit'),
        pl.lit(cap).alias('independent_contract_limit'))
    _,blocked=compile_execution_terms(**kwargs)
    assert blocked.filter(~pl.col('is_warmup'))['missing_position_limit'].all()


@pytest.mark.parametrize('conflict',['cap','unit','monthly','none','unresolved_peer','equivalent'])
def test_combined_group_cannot_admit_conflicting_limits_or_measurement_units(conflict):
    kwargs=inputs()
    for name in ['frame','margins','positions','specifications']:
        extra=kwargs[name].with_columns(pl.lit('DEF').alias('product'))
        if name=='frame':
            extra=extra.with_columns(pl.lit('DEF:202603#g0').alias('physical_contract'))
        if name=='positions':
            extra=extra.with_columns(pl.lit(1000. if conflict=='equivalent' else
                20. if conflict=='cap' else 10.).alias('position_limit'),
                pl.lit(700. if conflict=='equivalent' else 4. if conflict=='monthly' else 7.).alias('monthly_position_limit'),
                pl.lit(100. if conflict=='equivalent' else 1.).alias('position_unit'),
                pl.lit('shares' if conflict in ['unit','equivalent'] else 'contracts').alias('unit'),
                pl.lit(conflict!='unresolved_peer').alias('position_numeric_inputs_resolved'))
            kwargs[name]=kwargs[name].with_columns(pl.lit('contracts').alias('unit'))
        kwargs[name]=pl.concat([kwargs[name],extra])
    rules,blocked=compile_execution_terms(**kwargs)
    active=blocked.filter(~pl.col('is_warmup'))
    if conflict in ['cap','unit','monthly']:
        assert active['missing_position_limit'].all()
    else:
        assert not active.filter(pl.col('product')=='ABC')['missing_position_limit'].any()
        assert active.filter(pl.col('product')=='DEF')['missing_position_limit'].all()==(conflict=='unresolved_peer')
        if conflict=='equivalent':
            assert rules['position_unit'].to_list()==[100.]*rules.height
            assert rules['position_limit'].to_list()==[1000.]*rules.height
            assert rules['second_position_limit'].to_list()==[700.]*rules.height
            validate_margin_second_position_limit(rules)


def test_future_price_does_not_change_known_features_or_prior_rule_rows():
    kwargs = inputs()
    first, _ = compile_execution_terms(**kwargs)
    kwargs["frame"] = kwargs["frame"].with_columns(
        *[pl.when(pl.col("date") == DAYS[-1]).then(999.).otherwise(pl.col(c)).alias(c)
          for c in ("open", "close", "settlement")])
    second, _ = compile_execution_terms(**kwargs)
    assert first.filter(pl.col("date") < DAYS[-1]).equals(second.filter(pl.col("date") < DAYS[-1]))
    for col in ("known_margin_value_twd", "known_tax_twd", "known_margin_contract_value_twd"):
        assert first[col].equals(second[col])


def test_price_limit_uses_tick_bucket_at_limit_not_reference():
    kwargs = inputs()
    kwargs["frame"] = kwargs["frame"].with_columns(pl.lit(49.99).alias("settlement"))
    kwargs["specifications"] = pl.DataFrame([specification(tick_kind="stock_future", tick_size=None)],schema=SPEC_FIELDS)
    rules, _ = compile_execution_terms(**kwargs)
    # 49.99 * 1.1 = 54.989: using the reference's 0.05 tick would produce 54.95.
    assert rules["upper_limit"][0] == pytest.approx(54.9)
    assert rules["lower_limit"][0] == pytest.approx(45.)


def test_ended_or_unannounced_spec_does_not_backfill():
    days = inputs()["frame"].select("date","product")
    specs = pl.DataFrame([specification(valid_until_exclusive=DAYS[2])],schema=SPEC_FIELDS)
    bound = bind_product_specifications(days, specs).sort("date")
    assert bound["specification_bound"].to_list() == [True,True,False,False]
    assert bound["spec_contract_multiplier"].to_list() == [100.,100.,None,None]
    specs = pl.DataFrame([specification(known_at="2026-01-06T09:00:00+08:00")],schema=SPEC_FIELDS)
    bound = bind_product_specifications(days,specs).sort("date")
    assert bound["specification_bound"].to_list() == [False,False,True,True]


def test_missing_position_is_reported_not_filled_with_a_large_number():
    kwargs = inputs()
    kwargs["positions"] = kwargs["positions"].with_columns(pl.lit(False).alias("position_numeric_inputs_resolved"),
                                                           pl.lit(None,dtype=pl.Float64).alias("position_limit"))
    rules, blockers = compile_execution_terms(**kwargs)
    assert rules["position_limit"].null_count() == rules.height
    assert blockers["missing_position_limit"].all()


@pytest.mark.parametrize("announcement,expected", [("07:00:00", False), ("09:00:00", True)])
def test_same_day_margin_uses_the_actual_opening_clock(announcement, expected):
    kwargs = inputs()
    target = pl.col("date") == DAYS[1]
    kwargs["margins"] = kwargs["margins"].with_columns(
        pl.when(target).then(pl.lit("same_day_clock_review")).otherwise(pl.col("opening_binding_status"))
          .alias("opening_binding_status"),
        pl.when(target).then(None).otherwise(pl.col("opening_initial")).alias("opening_initial"),
        pl.when(target).then(None).otherwise(pl.col("opening_maintenance")).alias("opening_maintenance"))
    kwargs["margin_intervals"] = kwargs["margin_intervals"].with_columns(
        pl.lit(str(DAYS[1]) + "T" + announcement + "+08:00").alias("known_at"))
    rules, blockers = compile_execution_terms(**kwargs)
    row = blockers.filter(target).row(0, named=True)
    assert row["missing_opening_margin"] == expected
    assert rules.filter(target)["initial"][0] == (None if expected else .1)


def test_quantity_split_keeps_old_total_value_and_margin_once():
    kwargs = inputs()
    kwargs["specifications"] = pl.DataFrame([
        specification(valid_until_exclusive=DAYS[2], contract_multiplier=15.),
        specification(effective_date=DAYS[2], contract_multiplier=1.)],schema=SPEC_FIELDS)
    kwargs["corporate_terms"] = pl.DataFrame([dict(product="ABC",contract="202603", effective_date=str(DAYS[2]),
        valid_until_exclusive=None,from_product="ABC",contract_multiplier=1.,deliverable_cash_twd=0.,
        fixed_subscription_rights_twd=0.,subscription_rights_at_final_settlement=False,known_at=STAMP,
        source_content_sha256s=["d"*64],effective_at=None,equity_cash_credit_twd=None,
        has_equity_credit_fields=False,carry_quantity_numerator=15,carry_quantity_denominator=1)],schema=CORPORATE_SCHEMA)
    rules, blockers = compile_execution_terms(**kwargs)
    row = rules.filter(pl.col("date")==DAYS[2]).row(0,named=True)
    assert row["carry_quantity_numerator"] == 15
    assert row["carry_previous_value_twd"] == 103. * 15
    assert row["known_margin_contract_value_twd"] == 103.
    assert row["carry_previous_initial_twd"] == 155.
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    validate_margin_carry_rules(rules)


def test_fixed_point_interest_limit_and_face_value_tax_are_separate():
    kw = inputs()
    # CPF's official listing terms: 0.005 quoted points = TWD 411;
    # daily band is +/- 0.5 points, taxable face value is TWD 100 million.
    kw["specifications"] = pl.DataFrame([specification(
        contract_multiplier=82200., tick_size=.005, limit_up_ratio=None,
        limit_down_ratio=None, tax_class="commercial_paper", tax_value_kind="face_value",
        tax_fixed_value_twd=100_000_000.)], schema=SPEC_FIELDS).with_columns(
            pl.lit("points").alias("limit_kind"), pl.lit(.5).alias("limit_up_points"),
            pl.lit(.5).alias("limit_down_points"))
    kw["frame"] = kw["frame"].with_columns(pl.lit(98.125).alias("settlement"))
    rules, blockers = compile_execution_terms(**kw)
    assert rules["upper_limit"][0] == pytest.approx(98.625)
    assert rules["lower_limit"][0] == pytest.approx(97.625)
    assert rules["opening_tax_twd"][0] == 13.
    assert rules["opening_contract_value_twd"][0] == 101. * 82200.
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    kw["specifications"] = kw["specifications"].with_columns(pl.lit(1.1).alias("limit_up_ratio"))
    with pytest.raises(ValueError, match="invalid dated product specification"):
        compile_execution_terms(**kw)


def test_product_wide_split_preserves_value_without_month_specific_corporate_event():
    kw = inputs()
    kw["specifications"] = pl.DataFrame([
        specification(valid_until_exclusive=DAYS[2], contract_multiplier=100.),
        specification(effective_date=DAYS[2], contract_multiplier=10.)], schema=SPEC_FIELDS).with_columns(
            pl.Series("carry_split_numerator", [1, 10]), pl.lit(1).alias("carry_split_denominator"))
    rules, blockers = compile_execution_terms(**kw)
    row = rules.filter(pl.col("date") == DAYS[2]).row(0, named=True)
    assert row["carry_quantity_numerator"] == 10
    assert row["carry_previous_value_twd"] == 10300.
    assert row["known_margin_contract_value_twd"] == 1030.
    assert row["carry_known_at"] == STAMP
    assert row["upper_limit"] == 113.
    assert rules["carry_quantity_numerator"].to_list() == [1, 10, 1]
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    validate_margin_carry_rules(rules)
    # A source-bound split still has to conserve the old position's value.
    kw["specifications"] = kw["specifications"].with_columns(
        pl.when(pl.col("effective_date") == DAYS[2]).then(5).otherwise(1).alias("carry_split_numerator"))
    _, blocked = compile_execution_terms(**kw)
    assert blocked.filter(pl.col("date") == DAYS[2])["inconsistent_product_split"][0]


def test_newly_listed_month_does_not_receive_old_inventory_from_product_split():
    kw = inputs()
    kw["specifications"] = pl.DataFrame([
        specification(effective_date=DAYS[2], contract_multiplier=10.)], schema=SPEC_FIELDS).with_columns(
            pl.lit(10).alias("carry_split_numerator"), pl.lit(1).alias("carry_split_denominator"))
    kw["frame"] = kw["frame"].filter(pl.col("date") >= DAYS[2]).with_columns(
        pl.when(pl.col("date") == DAYS[2]).then(None).otherwise(pl.col("previous_symbol_date"))
        .alias("previous_symbol_date"))
    rules, blockers = compile_execution_terms(**kw)
    assert blockers.filter(pl.col("date") == DAYS[2])["is_warmup"][0]
    assert rules["carry_from_physical_contract"].to_list() == [""]
    assert rules["carry_quantity_numerator"].to_list() == [1]


def test_missing_intermediate_origin_cannot_be_disguised_as_warmup():
    kwargs = inputs()
    kwargs["frame"] = kwargs["frame"].filter(pl.col("date")!=DAYS[1])
    _, blockers = compile_execution_terms(**kwargs)
    row = blockers.filter(pl.col("date")==DAYS[2]).row(0,named=True)
    assert not row["is_warmup"]
    assert row["missing_prior_valuation"]


def test_a_nonterminal_position_cannot_disappear_from_the_next_session():
    kwargs = inputs()
    kwargs["frame"] = kwargs["frame"].filter(pl.col("date")!=DAYS[2])
    _, blockers = compile_execution_terms(**kwargs)
    assert blockers.filter(pl.col("date")==DAYS[1])["lost_inventory_continuation"][0]


def test_cash_credit_and_adjusted_identity_are_applied_once_to_old_inventory():
    kwargs = inputs()
    change = pl.col("date") >= DAYS[2]
    kwargs["frame"] = kwargs["frame"].with_columns(
        pl.when(change).then(pl.lit("AB1")).otherwise(pl.col("product")).alias("product"),
        pl.when(change).then(pl.lit("AB1:202603#g0")).otherwise(pl.col("physical_contract")).alias("physical_contract"))
    for key in ("margins", "positions"):
        kwargs[key] = kwargs[key].with_columns(pl.when(change).then(pl.lit("AB1"))
                                               .otherwise(pl.col("product")).alias("product"))
    kwargs["specifications"] = pl.DataFrame([specification(),specification("AB1")],schema=SPEC_FIELDS)
    kwargs["corporate_terms"] = pl.DataFrame([dict(product="AB1",contract="202603", effective_date=str(DAYS[2]),
        valid_until_exclusive=None,from_product="ABC",contract_multiplier=120.,deliverable_cash_twd=200.,
        fixed_subscription_rights_twd=0.,subscription_rights_at_final_settlement=False,known_at=STAMP,
        source_content_sha256s=["d"*64],effective_at=None,equity_cash_credit_twd=50.,
        has_equity_credit_fields=True,carry_quantity_numerator=1,carry_quantity_denominator=1)],schema=CORPORATE_SCHEMA)
    rules, blockers = compile_execution_terms(**kwargs)
    adjusted = rules.filter(pl.col("product")=="AB1").sort("date")
    assert adjusted["carry_cash_twd"].to_list()==[50.,0.]
    assert adjusted["carry_from_physical_contract"].to_list()==["ABC:202603#g0","AB1:202603#g0"]
    assert adjusted["carry_previous_value_twd"][0]==10300.
    assert adjusted["known_margin_contract_value_twd"][0]==10250.
    assert adjusted["opening_contract_value_twd"][0]==12440.
    assert adjusted["opening_margin_value_twd"][0]==12240.
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    validate_margin_carry_rules(rules)


@pytest.mark.parametrize("fixed_rights", [0., 70000.])
def test_subscription_rights_enter_only_final_value_never_daily_quotes(fixed_rights):
    kwargs = inputs()
    for key in ("frame", "margins", "positions"):
        kwargs[key] = kwargs[key].with_columns(pl.lit("AB1").alias("product"))
    kwargs["frame"] = kwargs["frame"].with_columns(
        pl.lit("AB1:202603#g0").alias("physical_contract"),
        (pl.col("date") == DAYS[-1]).alias("cash_settlement"))
    kwargs["specifications"] = pl.DataFrame([specification("AB1")], schema=SPEC_FIELDS)
    kwargs["corporate_terms"] = pl.DataFrame([dict(product="AB1",contract="202603",
        effective_date="2025-12-02", valid_until_exclusive=None, from_product="ABC",
        contract_multiplier=2000., deliverable_cash_twd=200., fixed_subscription_rights_twd=fixed_rights,
        subscription_rights_at_final_settlement=fixed_rights == 0., known_at=STAMP,
        source_content_sha256s=["d"*64], effective_at=None, equity_cash_credit_twd=None,
        has_equity_credit_fields=False, carry_quantity_numerator=1, carry_quantity_denominator=1)],
        schema=CORPORATE_SCHEMA)
    # The terminal binder supplies an official full deliverable; changing it
    # must not change any earlier mark, cash, margin or tax input.
    kwargs["terminal_values"] = pl.DataFrame(dict(date=[DAYS[-1]], product=["AB1"],
        contract=["202603"], terminal_value_input_twd=[280200.]))
    rules, blockers = compile_execution_terms(**kwargs)
    assert rules["settlement_contract_value_twd"].to_list() == [206200.,208200.,210200.]
    assert rules["carry_previous_value_twd"].to_list() == [204200.,206200.,208200.]
    assert rules["terminal_contract_value_twd"].to_list() == [206200.,208200.,280200.]
    assert rules["terminal_tax_twd"][-1] == 4.  # 210000 quoted value, not 280000
    assert not blockers.filter(~pl.col("is_warmup"))["has_blocker"].any()
    kwargs["terminal_values"] = kwargs["terminal_values"].with_columns(
        pl.lit(380200.).alias("terminal_value_input_twd"))
    changed, _ = compile_execution_terms(**kwargs)
    assert rules.filter(pl.col("date") < DAYS[-1]).equals(changed.filter(pl.col("date") < DAYS[-1]))
    kwargs["terminal_values"] = kwargs["terminal_values"].with_columns(
        pl.lit(None, dtype=pl.Float64).alias("terminal_value_input_twd"))
    _, missing = compile_execution_terms(**kwargs)
    assert not missing.filter(pl.col("date") < DAYS[-1])["unvalued_subscription_rights"].any()
    assert missing.filter(pl.col("date") == DAYS[-1])["missing_terminal_value"][0]
    assert missing.filter(pl.col("date") == DAYS[-1])["unvalued_subscription_rights"][0] == (fixed_rights == 0.)


def test_real_builder_writes_complete_worklist_without_publishing_candidates(tmp_path):
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_margin_release import build_all_twd_execution_terms
    kw = inputs()
    material, numeric, candidates, output = [tmp_path/name for name in ("market","numeric","candidates","terms")]
    for root in (material,numeric,candidates):
        root.mkdir()
    def write(root, name, frame):
        path=root/name; frame.write_parquet(path)
        return {"sha256":sha256_file(path)}
    numeric_outputs={name:write(numeric,name,kw[key]) for name,key in (
        ("product_day_margin_inputs.parquet","margins"),
        ("contract_day_position_inputs.parquet","positions"),
        ("adjusted_terminal_value_inputs.parquet","terminal_values"))}
    candidate_outputs={name:write(candidates,name,kw[key]) for name,key in (
        ("margin_level_intervals.parquet","margin_intervals"),
        ("corporate_terms_intervals.parquet","corporate_terms"))}
    source_sha={str(candidates/name):value['sha256'] for name,value in candidate_outputs.items()}
    (numeric/'manifest.json').write_text(json.dumps(dict(outputs=numeric_outputs,source_sha256s=source_sha)))
    (candidates/'manifest.json').write_text(json.dumps(dict(outputs=candidate_outputs)))
    daily=kw['frame'].with_columns(pl.lit(10.).alias('volume'),pl.lit('observed_at_dataset_boundary').alias('lifetime_status'))
    deps=daily.select('date','physical_contract').with_columns(
        *[pl.lit(False).alias(c) for c in ('missing_valuation','intermediate_calendar_gap','unresolved_lifetime')])
    material_outputs={'continuous_daily.parquet':write(material,'continuous_daily.parquet',daily),
                      'execution_dependencies.parquet':write(material,'execution_dependencies.parquet',deps)}
    (material/'manifest.json').write_text(json.dumps(dict(outputs=material_outputs,requested_products=['ABC'],
        source_sha256s={str(numeric/name):value['sha256'] for name,value in numeric_outputs.items()})))
    result=build_all_twd_execution_terms(materialization=material,margin_inputs=numeric,
        rule_candidates=candidates,specifications=None,output=output)
    assert result['status']=='blocked'
    assert not result['point_in_time_verified']
    assert result['source_rows']==4 and result['account_rows']==3
    assert result['blocker_counts']['missing_product_specification']==3
    assert result['outputs']['rules']['sha256']==sha256_file(output/'rules.parquet')
    with pytest.raises(FileExistsError):
        build_all_twd_execution_terms(materialization=material,margin_inputs=numeric,
            rule_candidates=candidates,specifications=None,output=output)
