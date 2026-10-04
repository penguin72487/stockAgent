from datetime import date, timedelta

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_parquet, sha256_file
from stockagent.data.tw_futures_execution_terms import inventory_entry_reachability
from stockagent.data.tw_futures_margin_release import (
    CONTEXT_ONLY_PREFIX_CONTRACT,
    omit_unreachable_account_prefix,
    validate_context_only_prefix_sources,
    WHOLE_CONTRACT_PREFIX_CONTRACT,
    omit_whole_contract_empty_prefix,
    validate_whole_contract_prefix_sources,
    whole_contract_entry_frame,
)
from stockagent.data.tw_futures_entry_capacity import whole_contract_trade_capacity


def capacity_account_rows(prior=(0., 1., 2., 0.)):
    frame, rules, flags = account_rows()
    frame = frame.with_columns(pl.lit(True).alias("executable"), pl.lit(100.).alias("volume"),
        pl.Series("previous_volume", prior, dtype=pl.Float64),
        pl.lit(True).alias("same_contract_as_previous_session"))
    rules = rules.drop("inventory_entry_reachable").join(inventory_entry_reachability(frame, rules),
        on=["date", "physical_contract"])
    return frame, rules, flags


def test_causal_whole_capacity_matches_the_prior_loader_calculation():
    import numpy as np
    previous = np.array([np.nan, np.inf, -np.inf, -4., 0., .99, 1., 1.9999, 2., 100.1, 16_777_219.])
    original = previous.copy()
    for participation in (.01, .5, 1.):
        old = np.floor(np.clip(np.nan_to_num(previous, nan=0., posinf=0., neginf=0.), 0., None) * participation)
        assert np.array_equal(whole_contract_trade_capacity(previous, participation), old)
    assert np.array_equal(previous, original, equal_nan=True)


@pytest.mark.parametrize("bound", [0., -1., 1.01, float("nan"), float("inf")])
def test_invalid_capacity_cannot_prove_an_empty_prefix(bound):
    frame, rules, flags = capacity_account_rows()
    with pytest.raises(ValueError, match="participation"):
        omit_whole_contract_empty_prefix(frame, rules, flags, maximum_volume_participation=bound)


def test_whole_contract_prefix_does_not_use_today_volume_or_erase_a_held_halt():
    frame, rules, flags = capacity_account_rows()
    before = frame.clone()
    kept, marked, empty, suppressed = omit_whole_contract_empty_prefix(frame, rules, flags,
        maximum_volume_participation=.5)
    assert empty.height == 2 and suppressed.height == 1
    assert kept["date"].to_list() == [date(2020, 1, 8), date(2020, 1, 9)]
    assert kept["inventory_entry_reachable"].to_list() == [True, True]
    assert empty["opening_contract_value_twd"].null_count() == 2
    assert frame.equals(before) and frame["executable"].all()
    assert marked["is_warmup"].to_list() == [True, True, False, False]
    # Tomorrow's zero capacity does not erase yesterday's established holding.
    assert date(2020, 1, 9) in kept["date"]


def test_whole_contract_bound_is_conservative_for_higher_supported_participation():
    frame, rules, flags = capacity_account_rows()
    kept, _, empty, _ = omit_whole_contract_empty_prefix(frame, rules, flags,
        maximum_volume_participation=1.)
    assert empty.height == 1 and date(2020, 1, 7) in kept["date"]


@pytest.mark.parametrize("field", ["previous_volume", "same_contract_as_previous_session"])
def test_whole_contract_prefix_requires_causal_capacity_identity(field):
    frame, rules, flags = capacity_account_rows()
    with pytest.raises(ValueError):
        omit_whole_contract_empty_prefix(frame.drop(field), rules, flags)


def test_zero_own_capacity_does_not_erase_a_cross_contract_position():
    frame, rules, flags = capacity_account_rows(prior=(2., 2., 2., 2.))
    target = frame.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"),
        pl.lit(0.).alias("previous_volume"))
    destination = rules.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"))
    target_flags = flags.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"))
    kept, _, empty, _ = omit_whole_contract_empty_prefix(pl.concat([frame, target]),
        pl.concat([rules, destination]), pl.concat([flags, target_flags]), maximum_volume_participation=.5)
    assert "BB1:202003#g1" in kept["physical_contract"] and empty.is_empty()


def test_whole_contract_unknown_origin_remains_admitted():
    frame, rules, flags = capacity_account_rows(prior=(0., 0., 0., 0.))
    rules = rules.with_columns(pl.lit(True).alias("inventory_origin_unresolved"))
    kept, _, empty, _ = omit_whole_contract_empty_prefix(frame, rules, flags)
    assert kept.height == rules.height and empty.is_empty()


@pytest.mark.parametrize("changed", [None, "bound", "source_volume", "identity", "held_omission"])
def test_whole_contract_publication_recomputes_capacity_and_all_owners(tmp_path, changed):
    frame, rules, flags = capacity_account_rows()
    kept, _, empty, suppressed = omit_whole_contract_empty_prefix(frame, rules, flags,
        maximum_volume_participation=.5)
    sources, proof = [], dict(contract=WHOLE_CONTRACT_PREFIX_CONTRACT,
        maximum_volume_participation=.5, rows=empty.height)
    for key, data in [("excluded_rules", empty), ("suppressed_carries", suppressed)]:
        name = key + ".parquet"
        atomic_write_parquet(tmp_path / name, data)
        receipt = dict(path=name, sha256=sha256_file(tmp_path / name))
        sources.append(receipt); proof[key] = receipt
    if changed == "bound":
        proof["maximum_volume_participation"] = 1.
    elif changed == "source_volume":
        frame = frame.with_columns(pl.lit(2.).alias("previous_volume"))
    elif changed == "identity":
        frame = frame.with_columns(pl.lit(False).alias("same_contract_as_previous_session"))
    elif changed == "held_omission":
        kept = kept.head(1)
    manifest = dict(context_only_prefix=proof, sources=sources)
    if changed is not None:
        with pytest.raises(ValueError):
            validate_whole_contract_prefix_sources(frame, kept, manifest, tmp_path)
    else:
        assert validate_whole_contract_prefix_sources(frame, kept, manifest, tmp_path).height == 2


def account_rows():
    start = date(2020, 1, 6)
    frame = pl.DataFrame([
        dict(date=start + timedelta(days=i), physical_contract="AA1:202003#g1",
             executable=i == 2, volume=5. if i == 2 else 0.,
             settlement=None if i < 2 else 20.) for i in range(4)
    ])
    rules = pl.DataFrame([
        dict(date=start + timedelta(days=i), physical_contract="AA1:202003#g1",
             carry_from_date=start + timedelta(days=i-1) if i else None,
             carry_from_physical_contract="AA1:202003#g1" if i else "",
             inventory_origin_unresolved=False,
             carry_cash_twd=0., carry_quantity_numerator=1, carry_quantity_denominator=1,
             opening_contract_value_twd=None if i < 2 else 40_000.,
             settlement_contract_value_twd=None if i < 2 else 40_000.) for i in range(4)
    ])
    rules = rules.join(inventory_entry_reachability(frame, rules),
        on=["date", "physical_contract"], validate="1:1")
    blockers = frame.select("date", "physical_contract").with_columns(
        pl.lit(False).alias("is_warmup"),
        (pl.col("date") < start + timedelta(days=2)).alias("missing_valuation"),
    )
    return frame, rules, blockers


def test_empty_prefix_preserves_source_and_all_retained_financial_values():
    frame, rules, flags = account_rows()
    before_frame, before_rules = frame.clone(), rules.clone()
    kept, marked, empty, suppressed = omit_unreachable_account_prefix(frame, rules, flags)
    assert empty.height == 2 and kept.height == 2 and suppressed.height == 1
    assert frame.equals(before_frame) and rules.equals(before_rules)
    assert empty["settlement_contract_value_twd"].null_count() == 2
    assert kept["settlement_contract_value_twd"].to_list() == [40_000., 40_000.]
    assert kept["carry_from_physical_contract"].to_list() == ["", "AA1:202003#g1"]
    assert kept["carry_from_date"].to_list() == [None, date(2020, 1, 8)]
    assert marked["is_warmup"].to_list() == [True, True, False, False]
    assert marked["missing_valuation"].to_list() == [True, True, False, False]
    # The halted row after an executable entry is still a required account day.
    assert date(2020, 1, 9) in kept["date"]


def test_context_prefix_reapplication_does_not_remove_a_held_halt():
    frame, rules, flags = account_rows()
    kept, marked, _, _ = omit_unreachable_account_prefix(frame, rules, flags)
    repeated, _, empty, suppressed = omit_unreachable_account_prefix(frame, kept, marked)
    assert repeated.equals(kept) and empty.is_empty() and suppressed.is_empty()


def test_reachable_cross_contract_owner_cannot_be_omitted():
    frame, rules, flags = account_rows()
    target = frame.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"))
    destination = rules.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"))
    frames = pl.concat([frame, target])
    all_rules = pl.concat([rules, destination])
    all_flags = pl.concat([flags, flags.tail(1).with_columns(pl.lit("BB1:202003#g1").alias("physical_contract"))])
    kept, _, empty, suppressed = omit_unreachable_account_prefix(frames, all_rules, all_flags)
    assert "BB1:202003#g1" in kept["physical_contract"]
    assert "BB1:202003#g1" not in empty["physical_contract"]
    assert "BB1:202003#g1" not in suppressed["physical_contract"]


def test_reused_quoted_month_has_separate_generation_proof():
    frame, rules, flags = account_rows()
    target = frame.tail(1).with_columns(pl.lit("AA1:202003#g2").alias("physical_contract"))
    destination = rules.tail(1).with_columns(
        pl.lit("AA1:202003#g2").alias("physical_contract"),
        pl.lit("").alias("carry_from_physical_contract"),
        pl.lit(None, dtype=pl.Date).alias("carry_from_date"),
        pl.lit(False).alias("inventory_entry_reachable"))
    all_flags = pl.concat([flags, flags.tail(1).with_columns(pl.lit("AA1:202003#g2").alias("physical_contract"))])
    kept, _, empty, _ = omit_unreachable_account_prefix(pl.concat([frame, target]),
        pl.concat([rules, destination]), all_flags)
    assert "AA1:202003#g2" in empty["physical_contract"]
    assert "AA1:202003#g1" in kept["physical_contract"]


def test_unknown_origin_remains_a_required_account_day():
    frame, rules, flags = account_rows()
    rules = rules.drop("inventory_entry_reachable").with_columns(
        pl.lit(True).alias("inventory_origin_unresolved"))
    rules = rules.join(inventory_entry_reachability(frame, rules), on=["date", "physical_contract"])
    kept, _, empty, _ = omit_unreachable_account_prefix(frame, rules, flags)
    assert kept.height == rules.height and empty.is_empty()


@pytest.mark.parametrize("kind", ["stale", "null", "string", "missing_source", "duplicate", "missing_flag"])
def test_unproved_context_omissions_are_rejected(kind):
    frame, rules, flags = account_rows()
    if kind == "stale":
        rules = rules.with_columns(pl.lit(False).alias("inventory_entry_reachable"))
    elif kind == "null":
        rules = rules.with_columns(pl.lit(None, dtype=pl.Boolean).alias("inventory_entry_reachable"))
    elif kind == "string":
        rules = rules.with_columns(pl.col("inventory_entry_reachable").cast(pl.String))
    elif kind == "missing_source":
        frame = frame.tail(3)
    elif kind == "duplicate":
        rules = pl.concat([rules, rules.head(1)])
    else:
        flags = flags.tail(3)
    with pytest.raises(ValueError):
        omit_unreachable_account_prefix(frame, rules, flags)


def publication_proof(tmp_path):
    frame, rules, flags = account_rows()
    kept, _, empty, suppressed = omit_unreachable_account_prefix(frame, rules, flags)
    sources, proof = [], dict(contract=CONTEXT_ONLY_PREFIX_CONTRACT, rows=empty.height)
    for key, data in [("excluded_rules", empty), ("suppressed_carries", suppressed)]:
        name = key + ".parquet"
        atomic_write_parquet(tmp_path / name, data)
        receipt = dict(path=name, sha256=sha256_file(tmp_path / name))
        sources.append(receipt); proof[key] = receipt
    return frame, kept, dict(context_only_prefix=proof, sources=sources)


def test_publication_recomputes_the_entire_empty_prefix_proof(tmp_path):
    frame, kept, manifest = publication_proof(tmp_path)
    result = validate_context_only_prefix_sources(frame, kept, manifest, tmp_path)
    assert result.height == 2


@pytest.mark.parametrize("kind", ["no_contract", "changed_source", "wrong_count", "held_omission", "altered_retained_value"])
def test_publication_rejects_missing_or_inconsistent_prefix_evidence(tmp_path, kind):
    frame, kept, manifest = publication_proof(tmp_path)
    if kind == "no_contract":
        manifest.pop("context_only_prefix")
    elif kind == "changed_source":
        (tmp_path / "excluded_rules.parquet").write_bytes(b"changed")
    elif kind == "wrong_count":
        manifest["context_only_prefix"]["rows"] += 1
    elif kind == "held_omission":
        kept = kept.head(1)
    else:
        kept = kept.with_columns(pl.col("opening_contract_value_twd") + 1.)
    with pytest.raises(ValueError):
        validate_context_only_prefix_sources(frame, kept, manifest, tmp_path)
