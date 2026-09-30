from datetime import date

import polars as pl
import pytest

from scripts.build_tw_cross_source_fill_bundle import revenue_candidates
from scripts.build_tw_release_schedule_dataset import normalize_source
from stockagent.data.tw_public_cross_source_fill import (
    MAPPINGS, missing_only, normalize_finmind,
)
from stockagent.data.tw_public_release_schedule import feature_name


def test_missing_only_never_overwrites_valid_zero_or_disagreement():
    a = pl.DataFrame({"period": ["2026Q1"] * 3, "symbol": ["1", "2", "3"], "value": [0., 2., None]})
    b = pl.DataFrame({"period": ["2026Q1"] * 3, "symbol": ["1", "2", "3"], "value": [0., 200., 3.]})
    fills, audit, conflicts = missing_only(a, b, min_overlap=2, min_agreement=.5)
    assert fills["symbol"].to_list() == ["3"]
    assert audit["conflicts"] == 1
    assert conflicts["symbol"].to_list() == ["2"]
    assert missing_only(a, b, min_overlap=2)[0].is_empty()


def test_duplicate_keys_are_never_resolved_by_file_order():
    a = pl.DataFrame({"period": ["2026Q1"] * 2, "symbol": ["1"] * 2, "value": [1., 2.]})
    with pytest.raises(ValueError, match="duplicate economic"):
        missing_only(a, a)


def test_cf_ytd_conversion_requires_exact_previous_quarter_and_resets_year():
    a = pl.DataFrame({"date": ["2025-03-31", "2025-06-30", "2025-12-31", "2026-03-31"],
        "stock_id": ["2330"] * 4, "type": ["CashFlowsFromOperatingActivities"] * 4,
        "value": [1000., 4000., 10000., 2000.]})
    t, _ = normalize_finmind(a, MAPPINGS["financial_statement:營業活動之淨現金流入_流出"])
    assert dict(t.select("period", "value").iter_rows()) == {"2025Q1": 1., "2025Q2": 3., "2026Q1": 2.}


def test_tdcc_excludes_total_adjustment_and_requires_all_four_levels():
    a = pl.DataFrame({"date": ["2026-09-18"] * 6, "stock_id": ["2330"] * 6,
        "HoldingSharesLevel": ["400,001-600,000", "600,001-800,000", "800,001-1,000,000",
                               "more than 1,000,001", "total", "差異數調整（說明4）"],
        "percent": [10., 20., 20., 40., 100., 5.]})
    spec = MAPPINGS["etl:inventory:大於四百張佔比"]
    assert normalize_finmind(a, spec)[0]["value"].item() == 90.
    assert normalize_finmind(a.slice(1), spec)[0].is_empty()


def test_revenue_no_zero_denominator_no_missing_month_positional_shift():
    a = pl.DataFrame({"date": ["2026-02-01", "2026-03-01", "2026-05-01"],
        "stock_id": ["2330"] * 3, "country": ["Taiwan"] * 3,
        "revenue_year": [2026] * 3, "revenue_month": [1, 2, 4], "revenue": [0, 1000, 4000]})
    tables, _ = revenue_candidates(a)
    assert tables["monthly_revenue:上月比較增減(%)"].is_empty()
    assert dict(tables["monthly_revenue:當月累計營收"].select("period", "value").iter_rows()) == {
        "2026-01": 0., "2026-02": 1.}


def test_late_known_monthly_release_is_not_advanced_to_provider_deadline(tmp_path):
    p = tmp_path / "month.parquet"
    key = "monthly_revenue:當月營收"
    pl.DataFrame({"source_index": ["2026-09-10 00:00:00"], "2330": [1.]}).write_parquet(p)
    sessions = [date(2026, 9, n) for n in [9, 10, 11, 14, 15, 16, 17]]
    bounds = pl.DataFrame({"dataset": [key], "source_index": ["2026-09-10 00:00:00"],
        "symbol": ["2330"], "known_published_on": [date(2026, 9, 15)]})
    t, _ = normalize_source(p, key, sessions, ["2330"], publication_bounds=bounds)
    assert t["date"].item() == date(2026, 9, 16)
    assert t[feature_name(key)].item() == 1.


def test_daily_shift_uses_exchange_session_not_next_stock_observation():
    from scripts.build_tw_daily_feature_fill_bundle import next_session_values
    a = pl.DataFrame({"date": ["2026-09-18", "2026-09-22"], "stock_id": ["2330"] * 2,
                      "volume": [0., 3.]})
    table, conflicts = next_session_values(a, pl.col("volume"), [date(2026, 9, x) for x in [18, 21, 22, 23]])
    assert dict(table.select("period", "value").iter_rows()) == {"2026-09-21": 0., "2026-09-23": 3.}
    assert conflicts == 0


def test_turnover_supplements_shares_without_switching_volume_definition():
    from scripts.build_tw_daily_feature_fill_bundle import official_volume_turnover
    primary = pl.DataFrame({"period":["2026-09-21"], "symbol":["2330"],
                            "twpub_official_trading_volume_raw":[1000.]})
    fm = pl.DataFrame({"date":["2026-09-18"], "stock_id":["2330"],
        "NumberOfSharesIssued":[10000], "Trading_Volume":[1123]})
    t,_ = official_volume_turnover(primary,fm,[date(2026,9,18),date(2026,9,21)])
    assert t["value"].item() == .1


def test_official_market_context_is_previous_session_including_year_boundary():
    from scripts.build_tw_daily_feature_fill_bundle import official_market_index
    primary = pl.DataFrame({"period":["2026-01-02"], "symbol":["2330"]})
    calendar = pl.DataFrame({"date":[date(2025,12,31),date(2026,1,2)],"closing_index":[100.,999.]})
    assert official_market_index(primary,calendar)["value"].item() == 100.


def test_dealer_total_and_components_cannot_be_double_counted():
    from scripts.build_tw_daily_feature_fill_bundle import make_candidates
    categories = ["Foreign_Investor", "Foreign_Dealer_Self", "Investment_Trust",
                  "Dealer", "Dealer_self", "Dealer_Hedging"]
    raw = {"date": ["2026-09-18"], "stock_id": ["2330"],
           **{c + "_" + side: [0] for c in categories for side in ("buy", "sell")}}
    raw["Dealer_buy"] = [1000]
    raw["Dealer_self_buy"] = [1000]
    rows = list(make_candidates({"TaiwanStockInstitutionalInvestorsBuySellWide": pl.DataFrame(raw)},
        [date(2026, 9, 18), date(2026, 9, 21)]))
    dealer = next(t for n,_,t,_ in rows if n == "twpub_dealer_net_buy_flow")
    assert dealer.is_empty()


def test_unresolved_invalid_barrier_survives_a_missing_only_overlay():
    from scripts.build_tw_release_schedule_dataset import retain_invalid_barriers
    a = pl.DataFrame({"date": [date(2026, 9, x) for x in [7, 14, 21]],
        "symbol": ["2330"] * 3, "x": [1., None, 3.]})
    # A sparse provider overlay drops NULL cells; do not carry 1 through Sep14.
    b = a.filter(pl.col("x").is_not_null())
    restored = retain_invalid_barriers(a, b, "x")
    assert restored["x"].to_list() == [1., None, 3.]
    b = a.with_columns(pl.col("x").fill_null(2.))
    assert retain_invalid_barriers(a, b, "x")["x"].to_list() == [1., 2., 3.]


def test_apply_daily_patch_end_to_end_and_observed_value_protection(tmp_path, monkeypatch):
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from scripts.apply_tw_daily_feature_fill_bundle import build, verify_observed_unchanged
    from scripts.prepare_tw_day_trade_feature_catalog import sha256
    base, parent, bundle, out = [tmp_path / x for x in ("base", "parent", "bundle", "out")]
    for p in [base, parent, bundle]:
        p.mkdir()
    (base / "quality_masks.json").write_text(json.dumps({"cells": []}))
    t = pa.table({"date":pa.array([date(2026, 9, 1),date(2026, 9, 2)], type=pa.date32()),
        "symbol":["2330","2330"],"x":pa.array([0., None],type=pa.float32()),"x__available":[True,False]})
    pq.write_table(t, parent / "model_inputs.parquet")
    digest = sha256(parent / "model_inputs.parquet")
    (parent / "dataset_manifest.json").write_text(json.dumps({"matrix":{"sha256":digest},
        "base_matrix_sha256":"pinned", "base_matrix":str(base / "model_inputs.parquet"), "limitations":[]}))
    (parent / "feature_columns.json").write_text(json.dumps({"value_features":["x"],"strict_base_features":["x"]}))
    (parent / "validation.json").write_text(json.dumps({"features":{"x":{"missing":1}}}))
    f = bundle / "x.parquet"
    pl.DataFrame({"date":[date(2026,9,2)],"symbol":["2330"],"x":[2.]}).write_parquet(f)
    (bundle / "bundle_manifest.json").write_text(json.dumps({"contract":"tw_daily_feature_missing_only_v1",
        "base_sha256":"pinned", "quality_masks_sha256":sha256(base / "quality_masks.json"),
        "fills":{"x":{"path":str(f),"sha256":sha256(f)}}, "filled_cells":1}))
    result = build(parent, bundle, out)
    assert result["missing_only_daily_fills"] == 1
    assert pl.read_parquet(out / "model_inputs.parquet")["x"].to_list() == [0., 2.]
    assert sha256(parent / "model_inputs.parquet") == digest
    bad = tmp_path / "bad.parquet"
    corrupt = pl.read_parquet(out / "model_inputs.parquet").with_columns(pl.lit(5.,dtype=pl.Float32).alias("x"))
    pq.write_table(corrupt.to_arrow().cast(t.schema), bad)
    with pytest.raises(ValueError,match="changed existing observed"):
        verify_observed_unchanged(parent / "model_inputs.parquet",bad,["x"])
    import scripts.apply_tw_daily_feature_fill_bundle as module
    original_writer = module.atomic_write_json
    def fail_companion(path, value):
        if path.suffix == ".ipynb":
            raise OSError("simulated companion write failure")
        return original_writer(path, value)
    monkeypatch.setattr(module,"atomic_write_json",fail_companion)
    interrupted = tmp_path / "interrupted"
    with pytest.raises(OSError,match="companion"):
        module.build(parent,bundle,interrupted)
    assert not (interrupted / "dataset_manifest.json").exists()
