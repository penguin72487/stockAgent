from decimal import Decimal
import json

import pytest


def _native_lifecycle():
    return {'Future Code': 'CJ1201403 華南金期 2014/03(1)', '_query_symbol': 'CJ1201403',
            'Listed Date': '2013/08/15', 'Trade Date_End': '2013/09/17',
            'Settle Date_End': '2014/03/19', 'Settle Price_End': '(null)'}


def test_native_early_last_trade_does_not_invent_a_price_publication_or_generation():
    from scripts.audit_tw_futures_tej_gap_operands import lifecycle_from_rows
    row = lifecycle_from_rows([_native_lifecycle()])[0]
    assert (row['product'], row['contract']) == ('CJ1', '201403')
    assert row['native_last_trade_date'] == '2013-09-17'
    assert row['native_settlement_date'] == '2014-03-19'
    assert row['native_last_trade_before_settlement']
    assert not row['native_final_price_present']
    assert row['native_final_price_cell'] == '(null)'
    assert not any(row[k] for k in ('source_values_admitted', 'publication_verified', 'generation_specific'))


@pytest.mark.parametrize('change', [
    {'_query_symbol': 'CJF201403'}, {'Future Code': 'CJ1201413 華南金'},
    {'Listed Date': '2013/10/01'}, {'Settle Date_End': '2013/09/16'},
    {'Trade Date_End': '2013/09'}, {'Trade Date_End': '2013/02/30'},
])
def test_native_lifecycle_rejects_foreign_keys_incomplete_dates_and_invalid_chronology(change):
    from scripts.audit_tw_futures_tej_gap_operands import lifecycle_from_rows
    with pytest.raises(ValueError):
        lifecycle_from_rows([_native_lifecycle() | change])


def test_native_final_price_is_a_terminal_cell_and_never_a_daily_carry_price():
    from scripts.audit_tw_futures_tej_gap_operands import lifecycle_from_rows
    row = lifecycle_from_rows([_native_lifecycle() | {'Settle Price_End': '17.20'}])[0]
    assert row['native_final_price_present']
    assert row['native_final_price_cell'] == '17.20'
    assert not row['source_values_admitted']

from scripts.audit_tw_futures_tej_gap_operands import (
    adjustment_role, audit, compare_adjustment, number, halt_events_from_rows,
)
from downloader.artifact_io import sha256_file


def test_adjusted_reference_does_not_replace_economic_carry_or_daily_clearing():
    row = compare_adjustment({"Reasons for Contract Adjustment": "減資", "Shares per Contract": "1600", "Reference Price": "15.55", "Cash Dividends per Unit": "0"},
        {"contract_multiplier": 1600, "equity_cash_credit_twd": 4000, "deliverable_cash_twd": 0},
        {"settlement": 14.45, "units": 2000})
    assert row["shares_match_at_display_precision"]
    assert Decimal(row["prior_economic_value_after_adjustment"]) == 24900
    assert Decimal(row["reference_price_value"]) == 24880
    assert Decimal(row["reference_rounding_difference_twd"]) == 20
    assert not row["reference_is_daily_settlement"] and not row["source_values_admitted"]


def test_small_share_recognition_error_is_not_hidden_by_relative_tolerance():
    row = compare_adjustment({"Reasons for Contract Adjustment": "減資", "Shares per Contract": "2000", "Reference Price": "12"}, {"contract_multiplier": 2})
    assert not row["shares_match_at_display_precision"]
    assert row["quantity_comparison"] == "quantity_conflict"


def test_native_display_rounding_keeps_more_precise_official_quantity():
    row = compare_adjustment({"Reasons for Contract Adjustment": "減資", "Shares per Contract": "1897.69231", "Reference Price": "12.5"},
        {"contract_multiplier": 1897.69230769231})
    assert row["shares_match_at_display_precision"]
    assert row["expected_shares"] != row["native_shares"]


@pytest.mark.parametrize("native,official", [("1897.69231", 1897.6923), ("1794.11765", 1794.1176), ("1130.63889", 1130.6389)])
def test_more_precise_native_value_does_not_rewrite_official_display(native, official):
    row = compare_adjustment({"Reasons for Contract Adjustment": "減資", "Shares per Contract": native},
                             {"contract_multiplier": official})
    assert row["quantity_comparison"] == "display_precision_difference"
    assert row["shares_match_at_display_precision"] and not row["source_values_admitted"]
    assert Decimal(row["expected_shares"]) == Decimal(str(official))


@pytest.mark.parametrize("flag", ["Y", "N"])
def test_relisted_standard_contract_does_not_compare_or_value_the_old_adjusted_contract(flag):
    native = {"Reasons for Contract Adjustment": "契約調整重新推出標準契約",
              "Failure to Contract Adjustment": flag, "Shares per Contract": "2000", "Reference Price": "72.9"}
    row = compare_adjustment(native, {"contract_multiplier": 20000}, {"settlement": 12, "units": 2000})
    assert adjustment_role(native) == "standard_contract_relisting"
    assert not row["adjusted_quantity_comparison_applicable"]
    assert row["shares_match_at_display_precision"] is None
    assert row["quantity_comparison"] == "not_comparable_source_role"
    assert "prior_economic_value_after_adjustment" not in row


def test_unknown_source_role_and_flag_are_not_inferred_as_a_financial_adjustment():
    row = compare_adjustment({"Failure to Contract Adjustment": "Y", "Shares per Contract": "2000"},
                             {"contract_multiplier": 2000})
    assert row["native_role"] == "unverified_adjustment_role"
    assert not row["adjusted_quantity_comparison_applicable"]
    assert not row["native_adjustment_flag_semantics_verified"]
    assert not row["source_values_admitted"]


def test_existing_interpretation_requires_explicit_refresh(tmp_path):
    output = tmp_path / "report"
    output.mkdir()
    with pytest.raises(ValueError, match="explicit interpretation refresh"):
        audit(tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, tmp_path, output)


@pytest.mark.parametrize("fault", ["different_input", "changed_bound_source"])
def test_interpretation_refresh_does_not_adopt_a_different_or_mutated_source(tmp_path, fault):
    output = tmp_path / "report"
    output.mkdir()
    paths = [tmp_path / n for n in ("registration.json", "receipt.json", "manifest.json", "terms.parquet", "source_manifest.json")]
    for p in paths:
        p.write_text("retained source")
    previous = dict(source_values_admitted=False,
                    sources=[dict(path=str(p), sha256=sha256_file(p)) for p in paths])
    (output / "manifest.json").write_text(json.dumps(previous))
    registration = paths[0]
    if fault == "different_input":
        registration = tmp_path / "different.json"
        registration.write_bytes(paths[0].read_bytes())
    else:
        paths[0].write_text("mutated source")
    with pytest.raises(ValueError, match="exact source inventory|sources changed"):
        audit(tmp_path, tmp_path, registration, paths[3], tmp_path, paths[1], output, refresh=True)


@pytest.mark.parametrize("value", [None, "", "(null)"])
def test_missing_source_cell_stays_missing(value):
    assert number(value) is None


@pytest.mark.parametrize("value", ["2,000", "unknown", "2000 shares", True, "NaN"])
def test_unknown_source_numeric_semantics_fail_closed(value):
    with pytest.raises(ValueError): number(value)


def test_native_halt_dates_are_event_evidence_and_not_a_quote_or_publication_clock():
    row = {"Co_id": "2311 日月光", "Suspended Date_Begin": "2018/04/18",
           "Suspended Re-trading": "2018/04/30", "_query_symbol": "2311"}
    event = halt_events_from_rows([row])[0]
    assert event["halt_start"] == "2018-04-18" and event["resume_date"] == "2018-04-30"
    assert not event["publication_verified"] and not event["source_values_admitted"]
    assert not any("price" in key or "value_twd" in key for key in event)
    row["Suspended Re-trading"] = None
    assert halt_events_from_rows([row])[0]["resume_date"] is None


@pytest.mark.parametrize("changes", [
    {"_query_symbol": "2312"}, {"Suspended Date_Begin": "2018/04"},
    {"Suspended Re-trading": "2018/04/17"}, {"Suspended Re-trading": "unknown"},
])
def test_unknown_halt_identity_or_date_cannot_be_reinterpreted(changes):
    row = {"Co_id": "2311 日月光", "Suspended Date_Begin": "2018/04/18",
           "Suspended Re-trading": "2018/04/30", "_query_symbol": "2311"} | changes
    with pytest.raises(ValueError, match="owner|native date|resumption"):
        halt_events_from_rows([row])
