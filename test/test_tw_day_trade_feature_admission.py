from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from stockagent.config import load_config
from stockagent.data.tw_day_trade_feature_admission import (
    VALUE_FEATURES, SHIFT_FEATURES, CHIP_FEATURES, RELEASE_FEATURES,
    LEGACY_POSITIVE_ONLY_LOG_FEATURES,
    classify_candidate, category_for, feature_clock, validate_feature_names,
)
from scripts.curate_tw_day_trade_training_dataset import (
    apply_quality_masks, conflicting_feature_cells, feature_columns, validate_matrix,
)
from scripts.audit_tw_public_data_layer import (
    _audit_input_signatures, audit_feature_availability_contract, audit_panel_feature_schema,
)


def candidate(field, *, provider="test", dataset="test", admission="needs_adapter_and_pit_evidence", role="numeric_candidate"):
    return {"catalog_id": f"{dataset}::{field}", "field": field, "dataset_id": dataset,
            "provider": provider, "admission": admission, "role": role}


@pytest.mark.parametrize("field", ["sha256", "archive_sha256", "_payload_sha256", "file_path", "url", "downloaded_at_utc", "error_message"])
def test_provenance_never_becomes_numeric_feature(field):
    row = classify_candidate(candidate(field))
    assert row["decision"] == "excluded_metadata"
    assert not row["included_as_raw_column"]


@pytest.mark.parametrize("provider,dataset,field", [
    ("moi", "public_economic:115S2", "總價元"),
    ("census", "public_economic:hv", "cell_value"),
    ("census", "public_economic:resconst", "cell_value"),
    ("census", "public_economic:mhs2", "cell_value"),
    ("census", "public_economic:vip", "cell_value"),
    ("FinMind", "complement:TaiwanHousePriceIndex", "value"),
    ("FinLab", "house_price:國泰房價指數", "house_price:國泰房價指數"),
])
def test_housing_is_excluded_by_semantics_not_only_dtype(provider, dataset, field):
    row = classify_candidate(candidate(field, provider=provider, dataset=dataset))
    assert row["decision"] == "excluded_housing"
    assert row["category"] == "housing"


def test_no_housing_false_positive_on_current_or_shareholder():
    for field in ["current_ratio", "shareholder_ratio", "parent_equity", "interest_rate"]:
        assert classify_candidate(candidate(field))["decision"] != "excluded_housing"


@pytest.mark.parametrize("field", ["symbol", "date", "cftc_commodity_code_quotes", "SecuritiesCompanyCode"])
def test_dimension_keys_are_not_model_inputs(field):
    assert classify_candidate(candidate(field))["decision"] == "excluded_identifier"


@pytest.mark.parametrize("field", ["_twpub_day_trade_eligible", "return_1d", "raw_ohlc_scale_factor", "future_high_next_1m"])
def test_labels_and_rule_or_current_open_facts_are_separate(field):
    assert classify_candidate(candidate(field))["decision"] == "execution_only"


def test_current_open_feature_is_not_mislabelled_as_permanently_useless():
    row = classify_candidate(candidate("next_session_open_gap_logret"))
    assert row["decision"] == "separate_open_quote_feature"
    assert "phase_aware" in row["reason"]


def test_source_success_and_same_name_do_not_bypass_pit_or_quality():
    assert classify_candidate(candidate("price", provider="FinLab"))["decision"] == "quarantined_pit"
    assert classify_candidate(candidate("decimal_value", role="text_or_unparsed"))["decision"] == "quarantined_adapter"
    assert classify_candidate(candidate("volume", admission="missing_values"))["decision"] == "excluded_all_null"
    assert classify_candidate(candidate("twpub_tdcc_large_holder_ratio"))["decision"] == "quarantined_quality"
    field = "twpub_foreign_net_buy_flow"
    assert classify_candidate(candidate(field, dataset="physical:tw-futures:model-features"))["decision"] == "excluded_duplicate_representation"
    assert classify_candidate(candidate(field, dataset="physical:tw-public:training-features"))["decision"] == "canonical_feature_selected"
    assert classify_candidate(candidate(field, dataset="physical:tw-public:training-features", admission="incomplete_source"))["decision"] == "quarantined_quality"


def test_curated_abi_is_exact_and_every_feature_has_a_single_clock():
    names = feature_columns()
    validate_feature_names(names)
    assert len(VALUE_FEATURES) == 46
    assert len(names) == 92
    assert len(set(VALUE_FEATURES)) == 46
    assert not set(SHIFT_FEATURES) & (set(CHIP_FEATURES) | set(RELEASE_FEATURES))
    for feature in VALUE_FEATURES:
        assert feature_clock(feature)
        assert category_for("", feature) not in {"housing", "metadata", "unclassified", "execution_rules"}
    for bad in [names + ["sha256"], names[:-1], names + [names[0]], list(reversed(names))]:
        with pytest.raises(ValueError, match="ABI"):
            validate_feature_names(bad)
    with pytest.raises(ValueError, match="clock"):
        feature_clock("twfl_monthly_revenue_raw")


def test_zero_destroying_legacy_transforms_use_raw_channels_in_new_dataset_only():
    assert not set(VALUE_FEATURES) & LEGACY_POSITIVE_ONLY_LOG_FEATURES
    assert {"twpub_official_trading_volume_raw", "twpub_official_trading_value_raw",
            "twpub_official_trades_raw", "twpub_margin_balance_lots_raw",
            "twpub_short_balance_lots_raw"} <= set(VALUE_FEATURES)
    for name in LEGACY_POSITIVE_ONLY_LOG_FEATURES:
        assert classify_candidate(candidate(name))["decision"] == "excluded_lossy_transform"


def test_canonical_clock_audit_accepts_exact_selection_and_rejects_double_shift():
    config = load_config("configs/markets/tw_public_preopen_pit.yaml")
    config = replace(config, data=replace(config.data, feature_include=list(VALUE_FEATURES),
                                          feature_shift_next_session=list(SHIFT_FEATURES)))
    summary, findings = audit_feature_availability_contract(config)
    assert not findings
    assert not summary["unclassified_active_features"]
    config = replace(config, data=replace(config.data, feature_shift_next_session=list(VALUE_FEATURES)))
    _, findings = audit_feature_availability_contract(config)
    assert any(f.code == "unexpected_feature_availability_shift" for f in findings)


def test_canonical_schema_audit_accounts_for_derived_indicators_without_weakening_order():
    config = load_config("configs/markets/tw_public_preopen_pit.yaml")
    config = replace(config, data=replace(config.data, feature_include=["clv", "twpub_pe_raw"],
                                          feature_availability_indicators=["twpub_*"]))
    panel = SimpleNamespace(feature_names=["clv", "twpub_pe_raw", "twpub_pe_raw__available"],
                            features=np.zeros((2, 3, 3), dtype=np.float32))
    panel.features[0, :, 2] = 1
    assert not audit_panel_feature_schema(panel, config)
    panel.features[0, 1, 2] = 0.5
    assert audit_panel_feature_schema(panel, config)[0].code == "invalid_feature_availability_indicator"
    panel.feature_names = ["twpub_pe_raw", "clv", "twpub_pe_raw__available"]
    assert audit_panel_feature_schema(panel, config)[0].code == "panel_feature_schema_mismatch"
    panel.feature_names = ["clv", "twpub_pe_raw", "sha256"]
    assert audit_panel_feature_schema(panel, config)[0].code == "panel_feature_schema_mismatch"


def test_audit_detects_release_receipt_changes_without_value_changes(tmp_path):
    public = tmp_path / "public"
    stocks = public / "stocks"
    state = public / "state"
    stocks.mkdir(parents=True)
    state.mkdir()
    feature = public / "features.parquet"
    feature.write_bytes(b"unchanged feature bytes")
    receipt = state / "cbc_money_release_vintages.json"
    receipt.write_text('{"status":"complete"}')
    before = _audit_input_signatures(stocks, public, feature)
    receipt.write_text('{"status":"running"}')
    assert _audit_input_signatures(stocks, public, feature) != before


def test_premerge_conflicts_distinguish_zero_from_null_and_do_not_mask_agreement():
    import polars as pl
    frame = pl.DataFrame({"date": ["2026-09-24"] * 4, "symbol": ["2330"] * 2 + ["2317"] * 2,
                          "twpub_short_balance_lots_raw": [0., 1., 0., 0.],
                          "twpub_margin_balance_lots_raw": [None, 3., 4., 4.]})
    result = conflicting_feature_cells(frame, ["twpub_short_balance_lots_raw", "twpub_margin_balance_lots_raw"], "margin")
    assert len(result) == 1
    assert result[0]["symbol"] == "2330"
    assert result[0]["feature"] == "twpub_short_balance_lots_raw"
    assert result[0]["conflicting_values"] == "[0.0, 1.0]"


def test_quality_mask_only_changes_ambiguous_cell_without_mutating_panel_cache():
    from stockagent.data.panel import PanelData
    names = feature_columns()
    feature = "twpub_short_balance_lots_raw"
    panel = PanelData(dates=np.array(["2026-09-24"], dtype="datetime64[D]"), symbols=["2330"],
                      feature_names=names, features=np.ones((1, 1, len(names)), dtype=np.float32),
                      returns_1d=np.zeros((1, 1)), tradable_mask=np.ones((1, 1), dtype=bool),
                      alive_mask=np.ones((1, 1), dtype=bool), benchmark_returns=np.zeros(1),
                      close_prices=np.ones((1, 1)))
    rows = [{"date": "2026-09-24", "symbol": "2330", "feature": feature},
            {"date": "2013-01-01", "symbol": "2330", "feature": feature}]
    masked, report = apply_quality_masks(panel, rows)
    assert report["masked_export_cells"] == 1
    assert report["outside_export_scope"] == 1
    assert np.all(panel.features == 1)
    assert not np.shares_memory(panel.features, masked.features)
    assert masked.features[0, 0, names.index(feature)] == 0
    assert masked.features[0, 0, names.index(feature + "__available")] == 0
    assert masked.features[0, 0, names.index("twpub_margin_balance_lots_raw")] == 1
    with pytest.raises(ValueError, match="non-selected"):
        apply_quality_masks(panel, [{**rows[0], "feature": "sha256"}])


def test_conflict_mask_clock_shifts_daily_valuation_but_not_already_shifted_chips(monkeypatch, tmp_path):
    import polars as pl
    import scripts.curate_tw_day_trade_training_dataset as curator
    receipt = [{"name": "verified-source", "sha256": "test"}]
    monkeypatch.setattr(curator, "read_json", lambda _: {"source_receipts": receipt})
    monkeypatch.setattr(curator, "_source_content_receipts", lambda _: receipt)
    margin = pl.DataFrame({"date": ["2026-09-24"] * 2, "symbol": ["2330"] * 2,
                           "twpub_short_balance_lots_raw": [0., 1.]})
    valuation = pl.DataFrame({"date": ["2026-09-23"] * 2, "symbol": ["2330"] * 2,
                              "twpub_pe_raw": [3., 4.]})
    monkeypatch.setattr(curator, "_build_margin_features", lambda _: margin)
    monkeypatch.setattr(curator, "_build_institutional_features", lambda _: pl.DataFrame())
    monkeypatch.setattr(curator, "_build_valuation_features", lambda _: valuation)
    monkeypatch.setattr(curator, "_next_exchange_session_lookup", lambda _: pl.DataFrame({
        "_source_date": ["2026-09-23", "2026-09-24"],
        "_available_date": ["2026-09-24", "2026-09-29"],
    }))
    rows = curator.build_quality_masks(tmp_path, tmp_path / "features.parquet")
    assert len(rows) == 2
    assert {r["date"] for r in rows} == {"2026-09-24"}
    monkeypatch.setattr(curator, "_source_content_receipts", lambda _: [])
    with pytest.raises(ValueError, match="sources differ"):
        curator.build_quality_masks(tmp_path, tmp_path / "features.parquet")


def matrix_file(tmp_path: Path, values, flags, *, dates=None, extra=False):
    n = len(values)
    dates = dates or [f"2026-09-{i + 1:02d}" for i in range(n)]
    table = pa.table({"date": pa.array(np.asarray(dates, dtype="datetime64[D]")),
                      "symbol": pa.array(["2330"] * n),
                      "signal": pa.array(values, type=pa.float32()),
                      "signal__available": pa.array(flags, type=pa.bool_())})
    if extra:
        table = table.append_column("sha256", pa.array(["not a feature"] * n))
    path = tmp_path / "matrix.parquet"
    pq.write_table(table, path, row_group_size=1)
    return path


def test_readback_keeps_observed_zero_distinct_from_missing(tmp_path):
    p = matrix_file(tmp_path, [None, 0.0, -0.3], [False, True, True])
    quality, annual = validate_matrix(p, ["signal"])
    assert quality["features"]["signal"] == {"observed": 2, "missing": 1, "observed_zero": 1,
                                                "min": float(np.float32(-0.3)), "max": 0.0}
    assert quality["duplicate_keys"] == 0
    assert annual[0]["observed_fraction"] == 2 / 3


@pytest.mark.parametrize("values,flags", [([None, 1.0], [True, True]), ([1.0, 0.0], [False, True]),
                                         ([float("inf"), 1.0], [True, True]),
                                         ([1.0, 1.0], [None, True])])
def test_bad_mask_or_nonfinite_values_fail_closed(tmp_path, values, flags):
    with pytest.raises(ValueError):
        validate_matrix(matrix_file(tmp_path, values, flags), ["signal"])


def test_all_null_duplicate_and_extra_column_fail_closed(tmp_path):
    with pytest.raises(ValueError, match="all-null"):
        validate_matrix(matrix_file(tmp_path, [None], [False]), ["signal"])
    with pytest.raises(ValueError, match="duplicate"):
        validate_matrix(matrix_file(tmp_path, [1.0, 2.0], [True, True], dates=["2026-09-01"] * 2), ["signal"])
    with pytest.raises(ValueError, match="extra"):
        validate_matrix(matrix_file(tmp_path, [1.0], [True], extra=True), ["signal"])


def test_non_session_or_unknown_symbol_is_not_silently_joined(tmp_path):
    p = matrix_file(tmp_path, [1.0], [True])
    with pytest.raises(ValueError, match="noncanonical"):
        validate_matrix(p, ["signal"], dates=np.asarray(["2026-09-02"], dtype="datetime64[D]"), symbols=["2330"])
    with pytest.raises(ValueError, match="entire canonical session"):
        validate_matrix(p, ["signal"], dates=np.asarray(["2026-09-01", "2026-09-02"], dtype="datetime64[D]"), symbols=["2330"])
