from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from scripts.build_tw_release_schedule_dataset import normalize_source, validate_base_unchanged
from stockagent.data.tw_public_release_schedule import (
    RULES, align_observations, feature_name, feature_category, next_session, publication_proxy, rule_for,
    schedule_lookup,
)


def calendar(start="2013-01-01", end="2026-12-31"):
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [a + timedelta(days=i) for i in range((b-a).days + 1)
            if (a + timedelta(days=i)).weekday() < 5]


def test_deadline_rolls_holiday_then_waits_until_next_session():
    sessions = [date(2026, 5, 29), date(2026, 6, 1), date(2026, 6, 2)]
    assert next_session(date(2026, 5, 31), sessions, roll_deadline=True) == date(2026, 6, 2)
    assert next_session(date(2026, 5, 29), sessions) == date(2026, 6, 1)
    assert next_session(date(2026, 6, 2), sessions) is None


def test_revenue_period_label_and_already_shifted_provider_date_are_distinct():
    c, r = calendar(), RULES["revenue"]
    assert publication_proxy("2025-M12", r, c) == date(2026, 1, 10)
    assert publication_proxy("2026-01-12 00:00:00", r, c) == date(2026, 1, 12)
    assert publication_proxy("2025-09-10 00:00:00", r, c) == date(2025, 9, 10)
    with pytest.raises(ValueError, match="period end"):
        publication_proxy("2026-01-31", r, c)
    with pytest.raises(ValueError):
        publication_proxy("2026-M13", r, c)


@pytest.mark.parametrize("period,expected", [
    ("2025-Q1", "2025-05-15"), ("2025-Q2", "2025-08-14"),
    ("2025-Q3", "2025-11-14"), ("2025-Q4", "2026-03-31"),
])
def test_quarter_envelope_not_quarter_end(period, expected):
    assert publication_proxy(period, RULES["quarter"], calendar()) == date.fromisoformat(expected)


def test_pmi_uses_third_session_and_calendar_holiday():
    c = [date(2026, 9, n) for n in [1, 3, 4, 7]]  # simulated closure on Sep 2
    lookup = schedule_lookup(["2026-09-01"], RULES["pmi"], c)
    assert lookup["estimated_published_on"].item() == date(2026, 9, 4)
    assert lookup["date"].item() == date(2026, 9, 7)


def test_pmi_boundary_window_is_deferred_without_inventing_future_sessions():
    c = [date(2026,9,n) for n in (1,2,3,4)] + [date(2026,10,1),date(2026,10,2)]
    lookup = schedule_lookup(["2026-08-01", "2026-09-01", "2026-10-01", "2026-11-01"], RULES["pmi"], c)
    assert lookup["source_index"].to_list() == ["2026-09-01"]
    assert lookup["date"].item() == date(2026,9,4)


def test_pmi_hole_inside_calendar_is_not_silently_a_pending_release():
    c = [date(2026,8,n) for n in (3,4,5,6)] + [date(2026,10,n) for n in (1,2,5,6)]
    with pytest.raises(ValueError, match="calendar does not cover"):
        schedule_lookup(["2026-09-01"], RULES["pmi"], c)


def test_ndc_and_tdcc_have_no_blanket_week_or_month_end_padding():
    c = calendar()
    assert publication_proxy("2026-09-27", RULES["business"], c) == date(2026, 9, 27)
    assert publication_proxy("2026-09-18", RULES["weekly"], c) == date(2026, 9, 18)
    assert rule_for("company_basic_info") is None
    assert rule_for("financial_statements_upload_detail:upload_date") is None


def test_nmi_retrospective_history_not_available_before_first_public_launch(tmp_path):
    key = "tw_total_nmi:臺灣非製造業NMI"
    p = tmp_path / "nmi.parquet"
    pl.DataFrame({"source_index": ["2014-08-01", "2015-01-05", "2015-02-02", "2015-03-02"],
                  "tw_total_nmi": [54.6, 53.6, 53.9, 50.8]}).write_parquet(p)
    t, schedule = normalize_source(p, key, calendar(), ["2330"])
    assert t["date"].min() == date(2015, 2, 3)
    assert t[feature_name(key)].to_list() == [53.9, 50.8]
    first = schedule.filter(pl.col("source_index") == "2015-02-02").row(0, named=True)
    assert first["estimated_published_on"] == date(2015, 2, 2)
    assert first["extra_delay_days"] == 1
    # An authoritative launch is not shifted into a later truncated calendar.
    later, _ = normalize_source(p, key, calendar("2016-01-01", "2016-12-31"), ["2330"])
    assert later.is_empty()


def test_etf_impossible_discount_is_explicit_quality_barrier(tmp_path):
    key = "tw_etf_nav_daily:折溢價(%)"
    p = tmp_path / "etf.parquet"
    pl.DataFrame({"source_index": ["2026-09-01", "2026-09-02", "2026-09-03"],
                  "0050": [-.3, -999.99, 1.2]}).write_parquet(p)
    q = []
    t, _ = normalize_source(p, key, calendar(), ["0050"], quality_rows=q)
    assert t[feature_name(key)].to_list() == [-.3, None, 1.2]
    assert q[0]["reason"] == "discount_impossible_for_positive_price_and_nav"


def test_missing_history_not_squeezed_onto_calendar_first_date():
    c = calendar("2014-01-01", "2014-02-01")
    lookup = schedule_lookup(["1960-01-27", "2014-01-27"], RULES["business"], c)
    assert lookup.height == 1
    assert lookup["date"].item() == date(2014, 1, 28)


@pytest.mark.parametrize("kind,index", [("quarter", "2026-Q1"), ("revenue", "2026-M06"),
    ("daily", "2026-09-18"), ("weekly", "2026-09-18"),
    ("pmi", "2026-09-01"), ("business", "2026-09-27")])
def test_every_rule_has_at_most_one_extra_calendar_day(kind, index):
    row = schedule_lookup([index], RULES[kind], calendar()).row(0, named=True)
    assert (row["safety_ready_on"] - row["estimated_published_on"]).days == 1
    assert row["extra_delay_days"] == 1
    assert row["date"] >= row["safety_ready_on"]
    assert row["nontrading_wait_days"] == (row["date"] - row["safety_ready_on"]).days


def test_alignment_preserves_zeros_no_backfill_bounded_state_and_no_daily_fill():
    keys = pl.DataFrame({"date": [date(2026, 9, 3), date(2026, 9, 1), date(2026, 9, 2), date(2026, 11, 9)],
                         "symbol": ["2330"] * 4})
    obs = pl.DataFrame({"date": [date(2026, 9, 2)], "symbol": ["2330"], "x": [0.]})
    assert align_observations(keys, obs, "x", RULES["daily"]).to_list() == [None, None, 0., None]
    assert align_observations(keys, obs, "x", RULES["revenue"]).to_list() == [0., None, 0., None]
    duplicate = pl.concat([obs, obs])
    with pytest.raises(ValueError, match="duplicate"):
        align_observations(keys, duplicate, "x", RULES["daily"])


def test_tdcc_historical_monthly_cadence_does_not_expire_mid_month():
    k = pl.DataFrame({"date": [date(2014, 4, 22), date(2016, 4, 22)], "symbol": ["2330"] * 2})
    o = pl.DataFrame({"date": [date(2014, 4, 1), date(2016, 4, 1)], "symbol": ["2330"] * 2, "x": [60., 70.]})
    assert align_observations(k, o, "x", RULES["weekly"]).to_list() == [60., None]


def test_tdcc_monthly_carry_still_expires_and_respects_invalid_barrier():
    k = pl.DataFrame({"date": [date(2014, 4, 22), date(2014, 7, 1)], "symbol": ["2330"] * 2})
    o = pl.DataFrame({"date": [date(2014, 4, 1), date(2014, 4, 15)], "symbol": ["2330"] * 2, "x": [60., None]})
    assert align_observations(k, o, "x", RULES["weekly"]).to_list() == [None, None]
    assert align_observations(k, o.head(1), "x", RULES["weekly"]).to_list() == [60., None]


def test_known_late_upload_does_not_override_newer_quarter(tmp_path: Path):
    key = "financial_statement:每股盈餘"
    p = tmp_path / "raw.parquet"
    pl.DataFrame({"source_index": ["2025-Q4", "2026-Q1", "2026-Q2"],
                  "2330": [1., 2., 3.], "9105": [1., 2., 3.]}).write_parquet(p)
    uploads = pl.DataFrame({"source_index": ["2025-Q4", "2026-Q1", "2026-Q2"],
        "symbol": ["2330"] * 3, "known_upload_on": [date(2026, 7, 2), date(2026, 5, 20), date(2027, 1, 2)]})
    table, _ = normalize_source(p, key, calendar(), ["2330", "9105"], uploads)
    assert table["symbol"].to_list() == ["2330"]
    assert table[feature_name(key)].to_list() == [2.]
    assert table["date"].item() == date(2026, 5, 21)


def test_known_early_upload_is_not_held_until_deadline(tmp_path):
    key = "financial_statement:每股盈餘"
    p = tmp_path / "early.parquet"
    pl.DataFrame({"source_index": ["2025-Q4"], "2330": [1.]}).write_parquet(p)
    uploads = pl.DataFrame({"source_index": ["2025-Q4"], "symbol": ["2330"],
                           "known_upload_on": [date(2026, 2, 26)]})
    table, _ = normalize_source(p, key, calendar(), ["2330"], uploads)
    assert table["date"].item() == date(2026, 2, 27)
    short_calendar = calendar("2026-02-01", "2026-03-01")
    table, _ = normalize_source(p, key, short_calendar, ["2330"], uploads)
    assert table["date"].item() == date(2026, 2, 27)


def test_reject_text_schema_and_mask_only_invalid_percent(tmp_path):
    p = tmp_path / "raw.parquet"
    pl.DataFrame({"source_index": ["2026-09-01"], "2330": ["missing"]}).write_parquet(p)
    with pytest.raises(ValueError, match="non-numeric"):
        normalize_source(p, "security_lending:借券", calendar(), ["2330"])
    pl.DataFrame({"source_index": ["2026-09-01"], "2330": [101.]}).write_parquet(p)
    masks = []
    key = "foreign_investors_shareholding:全體外資及陸資持股比率"
    table, _ = normalize_source(p, key, calendar(), ["2330"], quality_rows=masks)
    assert table[feature_name(key)].to_list() == [None]
    assert masks[0]["original_value"] == 101.
    assert masks[0]["date"] == "2026-09-02"


def test_invalid_weekly_report_is_a_null_barrier_not_previous_value_fill(tmp_path):
    key = "etl:inventory:大於四百張佔比"
    p = tmp_path / "weekly.parquet"
    pl.DataFrame({"source_index": ["2026-09-04", "2026-09-11", "2026-09-18"],
                  "2330": [60., 115.58, 70.]}).write_parquet(p)
    masks = []
    table, _ = normalize_source(p, key, calendar(), ["2330"], quality_rows=masks)
    keys = pl.DataFrame({"date": [date(2026, 9, 8), date(2026, 9, 15), date(2026, 9, 22)],
                         "symbol": ["2330"] * 3})
    values = align_observations(keys, table, feature_name(key), RULES["weekly"])
    assert values.to_list() == [60., None, 70.]
    assert len(masks) == 1


def test_strict_base_preservation_detects_mask_or_value_change(tmp_path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    frame = pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["2330"],
        "x": pl.Series([0.], dtype=pl.Float32), "x__available": [True]})
    frame.write_parquet(a)
    frame.with_columns(pl.lit(3.).alias("extra")).write_parquet(b)
    assert validate_base_unchanged(a, b, ["x"]) == 1
    frame.with_columns(pl.lit(False).alias("x__available")).write_parquet(b)
    with pytest.raises(ValueError, match="changed"):
        validate_base_unchanged(a, b, ["x"])


def test_source_family_classification_overrides_ambiguous_field_nouns():
    assert feature_category("financial_statement:銀行借款_非流動") == "fundamentals"
    assert feature_category("tw_total_nmi:人力僱用") == "macro"
    assert feature_category("tw_total_pmi:供應商交貨時間") == "macro"


def test_end_to_end_serializes_masks_and_preserves_strict_dataset(tmp_path):
    import json
    from scripts.build_tw_release_schedule_dataset import build
    from scripts.prepare_tw_day_trade_feature_catalog import sha256

    base, finlab, out = tmp_path / "base", tmp_path / "finlab", tmp_path / "out"
    base.mkdir()
    days = [date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 31), date(2026, 9, 7)]
    matrix = base / "model_inputs.parquet"
    import pyarrow as pa
    import pyarrow.parquet as pq
    pq.write_table(pa.table({"date": pa.array(days, type=pa.date32()),
        "symbol": pa.array(["2330"] * 4, type=pa.string()),
        "x": pa.array([1., 2., None, 0.], type=pa.float32()),
        "x__available": [True, True, False, True]}), matrix)
    digest = sha256(matrix)
    (base / "dataset_manifest.json").write_text(json.dumps({
        "training_features_ready": True, "matrix": {"sha256": digest}}))
    (base / "feature_columns.json").write_text(json.dumps({"value_features": ["x"]}))
    cal = tmp_path / "calendar.parquet"
    pl.DataFrame({"date": calendar("2026-08-01", "2026-09-30")}).write_parquet(cal)
    key = "etl:inventory:大於四百張佔比"
    (finlab / "datasets").mkdir(parents=True)
    (finlab / "receipts").mkdir()
    source = finlab / "datasets/weekly.parquet"
    pl.DataFrame({"source_index": ["2026-08-14", "2026-08-21", "2026-08-28"],
        "2330": [40., 50., 115.58]}).write_parquet(source)
    (finlab / "receipts/weekly.json").write_text(json.dumps({"dataset": key,
        "parquet_path": "datasets/weekly.parquet", "sha256": sha256(source),
        "publication_time_status": "not_verified"}))
    catalog = tmp_path / "catalog.csv"
    pl.DataFrame({"provider": ["FinLab"], "dataset_id": [key], "field": [key],
        "catalog_id": [key], "admission": ["research_only_needs_adapter"]}).write_csv(catalog)
    result = build(base=base, finlab=finlab, calendar=cal, catalog=catalog, out=out)
    assert result["research_training_features_ready"]
    assert not result["historical_point_in_time"]
    assert result["invalid_source_cells_masked"] == 1
    assert sha256(matrix) == digest
    assert pl.read_parquet(out / "model_inputs.parquet")[feature_name(key)].to_list() == [50., 50., None, None]
    assert json.loads((out / "quality_masks.json").read_text())[0]["date"] == "2026-08-31"
    with pytest.raises(FileExistsError):
        build(base=base, finlab=finlab, calendar=cal, catalog=catalog, out=out)
