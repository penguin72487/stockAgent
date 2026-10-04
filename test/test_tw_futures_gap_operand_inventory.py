"""Missing accounting inputs must be queried on their actual causal keys."""
from datetime import date

import polars as pl
import pytest

from scripts.plan_tw_futures_tej_gap_priority import (
    inventory_operands, operand_query_requests, halt_event_requests, build_halt_inventory,
    OPERAND_CONTRACT, SUSPENDED,
)


@pytest.fixture
def inputs():
    before, today, tomorrow = date(2022, 10, 7), date(2022, 10, 11), date(2022, 10, 12)
    frame = pl.DataFrame([
        dict(date=today, product="CH1", contract="202210", physical_contract="CH1:202210#g1",
             previous_market_date=before, next_market_date=tomorrow, settlement=16.4,
             executable=True, cash_settlement=False, corporate_transfer_target=None),
        dict(date=before, product="CHF", contract="202210", physical_contract="CHF:202210#g0",
             previous_market_date=date(2022, 10, 6), next_market_date=today, settlement=None,
             executable=False, cash_settlement=False, corporate_transfer_target="CH1"),
    ])
    gaps = frame.head(1).select("date", "product", "contract", "physical_contract").with_columns(
        pl.lit(True).alias("missing_prior_valuation"), pl.lit(True).alias("missing_price_limits"),
        pl.lit(True).alias("has_blocker"), pl.lit(False).alias("is_warmup"))
    rules = frame.head(1).select("date", "physical_contract").with_columns(
        pl.lit(True).alias("inventory_entry_reachable"))
    raw = pl.DataFrame([
        dict(date=date(2022, 9, 26), product="CHF", contract="202210", settlement=13.5, source_sha256="a" * 64),
        dict(date=today, product="CH1", contract="202210", settlement=16.4, source_sha256="b" * 64),
        dict(date=before, product="CHF", contract="202211", settlement=13.6, source_sha256="c" * 64),
    ])
    terms = pl.DataFrame([dict(product="CH1", contract="202210", from_product="CHF", effective_date=today.isoformat(),
                              source_content_sha256s=["d" * 64])])
    universe = pl.DataFrame([dict(product="CH1", underlying_symbol="2409"), dict(product="CHF", underlying_symbol="2409")])
    return gaps, frame, rules, raw, terms, universe


def test_prior_input_uses_source_product_previous_market_day_and_same_month(inputs):
    row = inventory_operands(*inputs).to_dicts()[0]
    assert row["required_product"] == "CHF"
    assert row["required_date"] == "2022-10-07"
    assert row["required_contract"] == "202210"
    assert row["required_physical_contract"] == "CHF:202210#g0"
    assert row["dependency_raw_settlement"] is None
    assert row["prior_positive_observation_date"] == "2022-09-26"
    assert not row["source_values_admitted"]
    assert row["candidate_adjustment_date"] == "2022-10-11"
    assert row["local_status"] == "missing_observation_or_legal_halt_input"


def test_existing_actual_dependency_is_binding_issue_and_future_value_is_not_borrowed(inputs):
    gaps, frame, rules, raw, terms, universe = inputs
    raw = pl.concat([raw, pl.DataFrame([dict(date=date(2022, 10, 7), product="CHF", contract="202210",
                                           settlement=13.5, source_sha256="e" * 64)])])
    row = inventory_operands(gaps, frame, rules, raw, terms, universe).to_dicts()[0]
    assert row["dependency_raw_settlement"] == 13.5
    assert row["local_status"] == "local_observation_present_binding_review_required"
    assert not row["source_values_admitted"]


def test_daily_value_uses_current_own_product_and_does_not_convert_prior_quote_into_value(inputs):
    gaps, frame, rules, raw, terms, universe = inputs
    gaps = frame.tail(1).select("date", "product", "contract", "physical_contract").with_columns(
        pl.lit(True).alias("missing_valuation"), pl.lit(True).alias("missing_settlement_value"))
    row = inventory_operands(gaps, frame, rules, raw, terms, universe).to_dicts()[0]
    assert row["required_product"] == "CHF" and row["required_date"] == "2022-10-07"
    assert row["dependency_raw_settlement"] is None
    assert row["prior_positive_observation_date"] == "2022-09-26"
    assert row["candidate_adjustment_product"] == "CH1"


def test_no_corporate_boundary_keeps_same_physical_product(inputs):
    gaps, frame, rules, raw, terms, universe = inputs
    terms = terms.with_columns(pl.lit("2022-10-06").alias("effective_date"))
    row = inventory_operands(gaps, frame, rules, raw, terms, universe).to_dicts()[0]
    assert row["required_product"] == "CH1"
    assert row["required_date"] == "2022-10-07"


@pytest.mark.parametrize("change,match", [
    ("duplicate_context", "Ambiguous"), ("missing_context", "Every blocked"),
    ("unknown_flag", "Uninventoried"), ("duplicate_raw", "Raw daily"),
])
def test_inventory_fails_closed_on_untraceable_operands(inputs, change, match):
    gaps, frame, rules, raw, terms, universe = inputs
    if change == "duplicate_context":
        frame = pl.concat([frame, frame.head(1)])
    elif change == "missing_context":
        frame = frame.tail(1)
    elif change == "unknown_flag":
        gaps = gaps.with_columns(pl.lit(True).alias("missing_new_financial_operand"))
    elif change == "duplicate_raw":
        raw = pl.concat([raw, raw.head(1)])
    with pytest.raises(ValueError, match=match):
        inventory_operands(gaps, frame, rules, raw, terms, universe)


def test_nonfinite_quote_is_not_local_settlement_coverage(inputs):
    gaps, frame, rules, raw, terms, universe = inputs
    raw = pl.concat([raw, pl.DataFrame([dict(date=date(2022, 10, 7), product="CHF", contract="202210",
                                           settlement=float("nan"), source_sha256="e" * 64)])])
    row = inventory_operands(gaps, frame, rules, raw, terms, universe).to_dicts()[0]
    assert row["local_status"] == "missing_observation_or_legal_halt_input"


def test_event_queries_collapse_halted_days_but_price_inputs_keep_actual_dates(inputs):
    row = inventory_operands(*inputs).to_dicts()[0]
    rows = [dict(row, gap_date=f"2022-10-{day:02}", required_date=f"2022-10-{day:02}") for day in (3, 4, 5, 6, 7)]
    requests = operand_query_requests(pl.DataFrame(rows))
    event = requests.filter(pl.col("requested_fields") == "corporate_adjustment_reference").to_dicts()[0]
    assert event["product"] == "CH1" and event["contract"] == "202210"
    assert event["exact_dates"] == "2022-10-03;2022-10-11"
    prices = requests.filter(pl.col("requested_fields") == "own_month_open_and_clearing_value").to_dicts()[0]
    assert prices["product"] == "CHF"
    assert prices["exact_dates"] == "2022-10-03;2022-10-04;2022-10-05;2022-10-06;2022-10-07"


def test_local_observation_is_not_queried_again(inputs):
    row = inventory_operands(*inputs).to_dicts()[0]
    row["local_status"] = "local_observation_present_binding_review_required"
    request = operand_query_requests(pl.DataFrame([row]))
    assert request.filter(pl.col("requested_fields") == "own_month_open_and_clearing_value").is_empty()


def test_halt_events_deduplicate_months_and_keep_only_underlying_event_scope(inputs):
    row = inventory_operands(*inputs).to_dicts()[0]
    rows = [dict(row, contract=month, required_date=day, next_positive_observation_date="2022-10-11")
            for month in ("202210", "202211") for day in ("2022-10-03", "2022-10-07")]
    native = {"2022-10-03", "2022-10-04", "2022-10-05", "2022-10-06", "2022-10-07", "2022-10-11", "2022-09-26"}
    result = halt_event_requests(pl.DataFrame(rows), native).to_dicts()
    assert len(result) == 1
    assert result[0]["product"] == "2409" and result[0]["contract"] == ""
    assert set(result[0]["exact_dates"].split(";")) == {"2022-09-26", "2022-10-03", "2022-10-07", "2022-10-11"}
    assert result[0]["requested_fields"] == "equity_halt_event"
    assert all(not r["source_values_admitted"] for r in rows)
    # An uncaptured input date must be diagnosed by the planner, not dropped.
    assert "2022-10-07" in halt_event_requests(pl.DataFrame(rows), native - {"2022-10-07"})["exact_dates"][0]


@pytest.mark.parametrize("fault", [None, "changed_terms", "changed_links", "uncovered_flag", "wrong_owner"])
def test_halt_subset_covers_current_flags_and_rejects_stale_context(inputs, tmp_path, fault):
    import json
    import sqlite3
    from downloader.artifact_io import sha256_file
    gaps, *_ = inputs
    old = tmp_path / "old"; old.mkdir()
    latest = tmp_path / "latest"; latest.mkdir()
    root = tmp_path / "tej"; root.mkdir()
    terms = tmp_path / "terms.parquet"; inputs[4].write_parquet(terms)
    links = old / "gap_operand_links.parquet"
    operands = inventory_operands(*inputs)
    if fault == "wrong_owner": operands = operands.with_columns(pl.lit("XX1").alias("product"))
    operands.write_parquet(links)
    (old / "manifest.json").write_text(json.dumps(dict(contract=OPERAND_CONTRACT,
        sources=[dict(path=str(terms), sha256=sha256_file(terms))],
        outputs={links.name: dict(sha256=sha256_file(links))})))
    if fault == "uncovered_flag": gaps = gaps.with_columns(pl.lit(True).alias("missing_cash_valuation"))
    gap = latest / "remaining_source_gaps.csv"; gaps.write_csv(gap)
    (latest / "manifest.json").write_text(json.dumps(dict(outputs={gap.name: dict(sha256=sha256_file(gap))})))
    axes = root / "axes.json"; axes.write_text(json.dumps(dict(date_labels=["20221007", "20221011"])))
    with sqlite3.connect(root / "queue.sqlite3") as con:
        con.execute("CREATE TABLE tables(table_id,name,category,fields_json,state,discovery_path,source_key_mode)")
        con.execute("INSERT INTO tables VALUES(?,?,'equity',?,'backfilling','axes.json',NULL)",
                    (SUSPENDED, "Company Suspended Records", json.dumps(["Suspended Re-trading"])))
    if fault == "changed_terms": terms.write_text("different financial terms")
    if fault == "changed_links": links.write_text("changed input coordinates")
    output = tmp_path / "result"
    if fault:
        with pytest.raises(ValueError, match="terms change|coordinates changed|not covered|owner differs"):
            build_halt_inventory(root, gap, old, terms, output)
        assert not output.exists()
    else:
        result = build_halt_inventory(root, gap, old, terms, output)
        assert result["remaining_coordinates"] == result["covered_coordinates"] == 1
        assert result["company_halt_request_groups"] == 1
        assert result["full_accounting_rebuilds"] == 0 and not result["source_values_admitted"]
