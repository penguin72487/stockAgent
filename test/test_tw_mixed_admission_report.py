from collections import Counter
import pytest
from scripts.report_tw_day_trade_mixed_admission import classify, coverage_fields
from stockagent.data.tw_day_trade_mixed_frequency import public_spec


def row(provider="FinMind", dataset="sponsor:TaiwanStockFinancialStatements", field="value", admission="research_only_needs_adapter"):
    return {"catalog_id": dataset+"::"+field, "provider": provider, "dataset_id": dataset,
            "field": field, "admission": admission, "role": "numeric_candidate"}


def test_registered_finmind_mapping_is_not_admitted_without_value_comparison():
    result = classify(row(), [], {})
    assert result["decision"] == "quarantined_missing_only_adapter"
    assert not result["admitted_as_model_value"]
    assert not result["selected_canonical_feature"]


def test_housing_and_hashes_stay_excluded():
    assert classify(row(field="sha256"), [], {})["decision"] == "excluded_metadata"
    assert classify(row(dataset="housing", field="value"), [], {})["decision"] == "excluded_housing"


def test_only_exact_new_source_specs_override_legacy_admission():
    specs = [public_spec("twpub_pe_raw")]
    result = classify(row(provider="TWSE", dataset="physical:tw-public:training-features", field="twpub_pe_raw"), specs, {})
    assert result["decision"] == "selected_private_research_observation"
    assert result["selected_canonical_feature"] == "twcad_pe_raw"
    other = classify(row(provider="TAIFEX", dataset="physical:tw-futures:model-features", field="twpub_pe_raw"), specs, {})
    assert other["decision"] == "excluded_duplicate_representation"


def test_tdcc_investor_count_is_a_measure_not_an_identifier():
    r = row(dataset="sponsor:TaiwanStockHoldingSharesPer", field="investors")
    r["role"] = "key_or_provenance"
    result = classify(r, [], {})
    assert result["decision"] == "quarantined_adapter"
    assert "tier" in result["reason"]


def test_coverage_labels_row_denominator_and_does_not_confuse_missingness_with_corruption():
    proof = {"decision_feature_rows": 100, "feature_available_cells": {"quarter": 20}}
    definition = {"observations": "3", "first_usable": "2014-01-06", "last_usable": "2026-09-01", "unmapped_release_periods": "1"}
    result = coverage_fields("quarter", proof, definition, Counter({"quarter": 1}))
    assert result["availability_ratio_over_all_stock_rows"] == .2
    assert result["quality_barrier_observation_rows"] == 1
    assert "not_applicable_issuer" in result["coverage_basis"]
    proof["feature_available_cells"]["quarter"] = 101
    with pytest.raises(ValueError, match="availability counts"):
        coverage_fields("quarter", proof, definition, Counter())
