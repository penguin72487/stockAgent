from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_feature_semantics import (
    BLOCK_PARTS, accepted_identity_fill, collapse_period_observations,
    event_zero_patch, identity_candidate, missingness_reasons, reconcile_block_reports,
)
from stockagent.data.tw_public_release_schedule import RULES, align_observations


def reports(total=100., missing_component=False):
    return pl.DataFrame({"source_index": ["2026-09-01"], "market": ["上市"],
        **{n: [total if i == 0 else None if missing_component and i == 1 else 0.] for i, n in enumerate(BLOCK_PARTS)}})


def test_reconcile_uses_summary_not_merely_nonempty_report():
    a = pl.DataFrame({"source_index": ["2026-09-01"], "symbol": ["2330"], "value": [100.]})
    markets = a.select("source_index", "symbol").with_columns(pl.lit("上市").alias("market"))
    assert reconcile_block_reports(a, markets, reports())["absence_zero_verified"].item()
    assert not reconcile_block_reports(a, markets, reports(101.))["absence_zero_verified"].item()
    assert not reconcile_block_reports(a, markets, reports(missing_component=True))["absence_zero_verified"].item()
    bad = markets.with_columns(pl.lit(None, dtype=pl.String).alias("market"))
    assert not reconcile_block_reports(a, bad, reports())["absence_zero_verified"].item()
    assert not reconcile_block_reports(a.with_columns(pl.lit(-100.).alias("value")), markets, reports())["absence_zero_verified"].item()


def test_zero_total_can_certify_empty_day_positive_total_cannot():
    a = pl.DataFrame(schema={"source_index": pl.String, "symbol": pl.String, "value": pl.Float64})
    m = pl.DataFrame(schema={"source_index": pl.String, "symbol": pl.String, "market": pl.String})
    assert reconcile_block_reports(a, m, reports(0.))["absence_zero_verified"].item()
    assert not reconcile_block_reports(a, m, reports(1.))["absence_zero_verified"].item()


def test_event_zero_respects_venue_date_and_null_barrier():
    d = date(2026, 9, 2)
    k = pl.DataFrame({"date": [d] * 5, "symbol": ["a", "b", "c", "d", "e"], "market": ["上市", "上市", "上櫃", "上市", None]})
    r = pl.DataFrame({"date": [d, d], "market": ["上市", "上櫃"], "absence_zero_verified": [True, False]})
    e = pl.DataFrame({"date": [d, d], "symbol": ["a", "d"], "x": [100., None]})
    assert event_zero_patch(k, r, e, "x").to_dicts() == [{"date": d, "symbol": "b", "x": 0.}]
    assert event_zero_patch(k.with_columns(pl.lit(date(2026, 9, 3)).alias("date")), r, e, "x").is_empty()
    with pytest.raises(ValueError, match="duplicate"):
        event_zero_patch(k, pl.concat([r, r]), e, "x")


def test_identity_requires_exact_period_and_latest_operand_clock():
    a = pl.DataFrame({"period": ["2026-Q1", "2026-Q2"], "symbol": ["2330"] * 2,
        "date": [date(2026, 5, 1), date(2026, 8, 1)], "value": [100., 200.]})
    b = a.head(1).with_columns(pl.lit(date(2026, 5, 20)).alias("date"), pl.lit(40.).alias("value"))
    c = identity_candidate({"a": a, "b": b}, [("a", 1), ("b", -1)])
    assert c.to_dicts() == [{"period": "2026-Q1", "symbol": "2330", "date": date(2026, 5, 20), "value": 60.}]


def test_identity_gate_and_invalid_observation_barrier():
    p = pl.DataFrame({"period": ["p1", "p2"], "symbol": ["s"] * 2, "value": [5., None]})
    c = pl.DataFrame({"period": ["p1", "p2", "p3"], "symbol": ["s"] * 3,
        "date": [date(2026, 9, 1)] * 3, "value": [5., 6., 7.]})
    f, a = accepted_identity_fill(p, c, min_overlap=1)
    assert a["accepted"] and f["period"].to_list() == ["p3"]
    f, a = accepted_identity_fill(p, c.with_columns((pl.col("value") * 1000).alias("value")), min_overlap=1)
    assert not a["accepted"] and f.is_empty()


def test_late_old_derived_period_cannot_replace_newer_report():
    t = pl.DataFrame({"source_index": ["2026-Q1", "2026-Q2"], "symbol": ["s"] * 2,
        "date": [date(2026, 9, 1), date(2026, 8, 1)], "x": [1., 2.]})
    assert collapse_period_observations(t, "x")["x"].to_list() == [2.]


def test_monthly_state_carry_is_not_missing_and_daily_flow_is_not_carried():
    k = pl.DataFrame({"date": [date(2026, 9, 11)], "symbol": ["2330"]})
    o = pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["2330"], "x": [0.]})
    assert align_observations(k, o, "x", RULES["revenue"]).to_list() == [0.]
    r = missingness_reasons(k, o, "x", RULES["daily"], ["2330"], "security_lending:借券")
    assert r["reason"].item() == "daily_report_has_no_symbol_value"


def test_early_history_not_applicable_and_ttl_are_not_source_corruption():
    k = pl.DataFrame({"date": [date(2026, 8, 1), date(2026, 9, 2), date(2027, 9, 2)], "symbol": ["2330", "0050", "2330"]})
    o = pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["2330"], "x": [1.]})
    r = missingness_reasons(k, o, "x", RULES["quarter"], ["2330"], "financial_statement:資產總額")
    assert set(r["reason"]) == {"before_first_observation_in_archive", "not_applicable_company_report_to_etf", "state_older_than_carry_limit"}
    assert r["cells"].sum() == 3


def test_report_exists_but_optional_line_missing_is_not_invented_zero():
    k = pl.DataFrame({"date": [date(2026, 9, 2)], "symbol": ["2330"]})
    o = pl.DataFrame({"date": [date(2025, 9, 1)], "symbol": ["2330"], "x": [1.], "source_index": ["2025-Q2"]})
    c = pl.DataFrame({"_report_date": [date(2026, 8, 15)], "symbol": ["2330"], "_report_period": ["2026-Q2"]})
    r = missingness_reasons(k, o, "x", RULES["quarter"], ["2330"], "financial_statement:特別股負債_流動", c)
    assert r["reason"].item() == "account_not_reported_in_available_company_report"
    assert align_observations(k, o, "x", RULES["quarter"]).to_list() == [None]


def test_warmup_reuses_previous_session_not_first_day_future_price(tmp_path):
    import json
    from scripts.repair_tw_feature_semantics import warmup_patch
    from scripts.prepare_tw_day_trade_feature_catalog import sha256
    from stockagent.data.tw_day_trade_feature_admission import DAILY_PUBLIC_FEATURES
    stocks = tmp_path / "stocks"
    stocks.mkdir()
    days = [date(2026, 9, i) for i in (1, 2, 3)]
    pl.DataFrame({"date": days, "open": [10., 10., 100.], "max": [12., 12., 150.],
        "min": [9., 9., 80.], "close": [10., 11., 120.], "Trading_Volume": [100., 120., 800.],
        "adjclose": [10., 11., 120.], "lifecycle_episode_id": [0, 0, 0]}).write_parquet(stocks / "2330_features.parquet")
    f = tmp_path / "public.parquet"
    pl.DataFrame({"date": [days[1]], "symbol": ["2330"], **{n: [3.] for n in DAILY_PUBLIC_FEATURES}}).write_parquet(f)
    (tmp_path / "quality_masks.json").write_text(json.dumps({"cells": [{"date": str(days[-1]), "symbol": "2330", "feature": "twpub_pe_raw"}]}))
    m = {"source": {"feature_path": str(f), "feature_sha256": sha256(f)}}
    k = pl.DataFrame({"date": [days[-1]], "symbol": ["2330"]})
    t = warmup_patch(stocks, days, k, tmp_path, m, tmp_path)
    assert t["body_ratio"].item() == pytest.approx(1/3)
    assert t["twpub_pb_raw"].item() == 3.
    assert t["twpub_pe_raw"].item() is None
    assert t["date"].item() == days[-1]


def test_independent_tdcc_verifier_checks_both_eras_and_null_barriers():
    from scripts.verify_tw_feature_semantics import verify_tdcc_state
    obs = pl.DataFrame({"date": [date(2014, 1, 2), date(2014, 2, 1), date(2016, 1, 1)],
        "symbol": ["s"] * 3, "x": [10., None, 20.]})
    actual = pl.DataFrame({"date": [date(2014, 1, 25), date(2014, 2, 2), date(2016, 1, 20)],
        "symbol": ["s"] * 3, "x": [10., None, None]})
    result = verify_tdcc_state(actual, obs, "x")
    assert result["historical_monthly_observed_rows"] == 1
    assert result["expired_state_rows"] == 1
    with pytest.raises(ValueError, match="dated state"):
        verify_tdcc_state(actual.with_columns(pl.col("x").fill_null(10.)), obs, "x")


def semantic_audit_fixture(root):
    import json
    from scripts.prepare_tw_day_trade_feature_catalog import sha256
    root.mkdir()
    pl.DataFrame({"date": [date(2026, 9, 1)], "symbol": ["2330"], "x": [1.]}).write_parquet(root / "model_inputs.parquet")
    (root / "feature_columns.json").write_text(json.dumps({"value_features": ["x"]}))
    (root / "validation.json").write_text(json.dumps({"features": {"x": {"missing": 0}}}))
    pl.DataFrame({"feature": ["x"], "remaining_missing": [0]}).write_csv(root / "remaining_feature_worklist.csv")
    pl.DataFrame(schema={"feature": pl.String, "reason": pl.String, "cells": pl.UInt32}).write_csv(root / "missingness_causes.csv")
    m = {"contract": "tw_feature_semantics_research_v1", "historical_point_in_time": False,
        "strict_training_eligible": False, "live_eligible": False,
        "matrix": {"sha256": sha256(root / "model_inputs.parquet"), "rows": 1}}
    for filename, field in (("feature_columns.json", "feature_schema_sha256"), ("validation.json", "validation_sha256"),
        ("remaining_feature_worklist.csv", "worklist_sha256"), ("missingness_causes.csv", "missingness_causes_sha256")):
        m[field] = sha256(root / filename)
    (root / "dataset_manifest.json").write_text(json.dumps(m))
    (root / "independent_acceptance.json").write_text(json.dumps({"status": "passed",
        "matrix_sha256": m["matrix"]["sha256"], "rows_compared": 1, "missingness_partition_total": 0}))


def test_semantic_worklist_publication_keeps_historical_audit(tmp_path):
    from pathlib import Path
    from scripts.audit_tw_feature_gap_repair import publish_semantic_audit
    root, out = tmp_path / "dataset", tmp_path / "audit"
    semantic_audit_fixture(root)
    out.mkdir()
    old = b"adapter_status,not_a_corruption_verdict\n"
    (out / "remaining_feature_worklist.csv").write_bytes(old)
    r = publish_semantic_audit(root, out)
    assert r["source_corruption_not_inferred_from_missingness"]
    assert any(Path(p).read_bytes() == old for p in r["prior_audits_preserved"])
    assert (out / "remaining_feature_worklist.csv").read_bytes() == (root / "remaining_feature_worklist.csv").read_bytes()


@pytest.mark.parametrize("broken", ["acceptance", "worklist", "counts"])
def test_semantic_worklist_rejects_unaccepted_or_changed_artifacts(tmp_path, broken):
    import json
    from scripts.audit_tw_feature_gap_repair import publish_semantic_audit
    root, out = tmp_path / "dataset", tmp_path / "audit"
    semantic_audit_fixture(root)
    if broken == "worklist":
        (root / "remaining_feature_worklist.csv").write_text("feature,remaining_missing\nx,1\n")
    else:
        p = root / "independent_acceptance.json"
        m = json.loads(p.read_text())
        m["status" if broken == "acceptance" else "rows_compared"] = "failed" if broken == "acceptance" else 2
        p.write_text(json.dumps(m))
    with pytest.raises(ValueError):
        publish_semantic_audit(root, out)
    assert not out.exists()
