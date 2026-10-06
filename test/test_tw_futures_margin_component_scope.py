from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_release import select_complete_margin_components


def fixture():
    dates = [date(2020, 1, d) for d in (2, 3, 6, 7)]
    rows, rules, flags = [], [], []
    for identity, offset, count in [("A", 0, 2), ("B", 2, 2), ("C", 0, 4)]:
        for j in range(count):
            i = offset + j
            final = identity != "A" and i == 3
            rows.append(dict(date=dates[i], physical_contract=identity, product=identity,
                next_market_date=dates[i+1] if i < 3 else None, cash_settlement=final,
                lifetime_status="corporate_transfer_candidate" if identity == "A" else "official_final"))
            origin = ("A" if identity == "B" and j == 0 else identity) if (j or identity == "B") else ""
            rules.append(dict(date=dates[i], physical_contract=identity,
                terminal_event="cash_settlement" if final else "mark_only",
                carry_from_date=dates[i-1] if origin else None,
                carry_from_physical_contract=origin))
            flags.append(dict(date=dates[i], physical_contract=identity,
                is_warmup=False, has_blocker=False))
    return pl.DataFrame(rows), pl.DataFrame(rules), pl.DataFrame(flags)


@pytest.mark.parametrize("failed", ["A", "B"])
def test_one_bad_day_removes_predecessor_and_successor_not_only_the_bad_row(failed):
    frame, rules, flags = fixture()
    flags = flags.with_columns((pl.col("physical_contract") == failed).alias("has_blocker"))
    selected, chosen, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert set(selected["physical_contract"]) == {"C"}
    assert set(chosen["physical_contract"]) == {"C"}
    assert scope.filter(pl.col("physical_contract").is_in(["A", "B"]))["selected"].to_list() == [False, False]


def test_complete_conversion_keeps_both_owners_and_exact_source_rows():
    frame, rules, flags = fixture()
    selected, chosen, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert selected.sort("date", "physical_contract").equals(frame.sort("date", "physical_contract"))
    assert chosen.sort("date", "physical_contract").equals(rules.sort("date", "physical_contract"))
    assert scope["selected"].all()


def test_endpoint_is_mark_only_without_synthetic_cash_event():
    frame, rules, flags = fixture()
    selected, chosen, _ = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 6))
    tail = selected.filter(pl.col("date") == date(2020, 1, 6))
    assert tail["next_market_date"].null_count() == tail.height
    assert not tail["cash_settlement"].any()
    assert chosen.filter(pl.col("date") == date(2020, 1, 6))["terminal_event"].eq("mark_only").all()


def test_missing_diagnostic_is_not_silently_treated_as_complete():
    frame, rules, flags = fixture()
    with pytest.raises(ValueError, match="diagnostics"):
        select_complete_margin_components(frame, rules, flags.head(1),
            start=date(2020, 1, 1), end=date(2020, 1, 7))


def test_context_only_product_is_not_advertised_as_a_trainable_account():
    frame, rules, flags = fixture()
    flags = flags.with_columns((pl.col("physical_contract") == "C").alias("is_warmup"))
    rules = rules.filter(pl.col("physical_contract") != "C")
    selected, _, scope = select_complete_margin_components(frame, rules, flags,
        start=date(2020, 1, 1), end=date(2020, 1, 7))
    assert set(selected["product"]) == {"A", "B"}
    assert not scope.filter(pl.col("product") == "C")["selected"].any()


def test_early_cpf_gap_excludes_whole_lifetime_but_keeps_verified_later_cpf():
    frame, rules, flags = fixture()
    frame = frame.with_columns(pl.lit('CPF').alias('product'))
    flags = flags.with_columns((pl.col('physical_contract') == 'A').alias('has_blocker'))
    selected, chosen, scope = select_complete_margin_components(frame,rules,flags,
        start=date(2020,1,1),end=date(2020,1,7))
    # The failed early owner and its transferred successor stay together;
    # a separate complete later month of the SAME product remains admitted.
    assert set(selected['physical_contract']) == {'C'}
    assert set(chosen['physical_contract']) == {'C'}
    assert set(selected['product']) == {'CPF'}
    assert not scope.filter(pl.col('physical_contract').is_in(['A','B']))['selected'].any()


def test_empty_context_keeps_all_predecessors_without_admitting_unrelated_early_cpf():
    from scripts.prepare_tw_futures_margin_training import _selected_empty_context_keys
    day=date(2020,1,2)
    selected=pl.DataFrame([dict(date=day,physical_contract='CURRENT')])
    suppressed=pl.DataFrame([dict(date=day,physical_contract='CURRENT',carry_from_physical_contract='PREVIOUS')])
    empty=pl.DataFrame([dict(date=day,physical_contract=p,carry_from_physical_contract=origin)
        for p,origin in [('PREVIOUS','GRANDPARENT'),('GRANDPARENT','ROOT'),('ROOT',''),('EARLY_CPF','')]])
    keys=_selected_empty_context_keys(selected,empty,suppressed)
    assert set(keys['physical_contract'])=={'PREVIOUS','GRANDPARENT','ROOT'}
