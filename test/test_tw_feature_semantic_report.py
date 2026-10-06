from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_text
from stockagent.data.tw_feature_semantic_report import (
    aggregate_long_files, anchor, classify, digest, enrich_report_rows, label_unit,
    CONTRACT, report_summary, reuse_source_snapshot, verify_report,
    write_feature_pages, write_source_pages,
)


def row(field, dataset="sponsor:TaiwanStockBalanceSheet", provider="FinMind", **kwargs):
    return dict(catalog_id=f"{provider}:{dataset}::{field}", provider=provider,
                dataset_id=dataset, field=field, **kwargs)


@pytest.mark.parametrize("field,dataset,provider,category,role", [
    ("Loan Amount(1000 NTD)", "tej:Banking Detail", "TEJ", "fundamentals", "economic_measure"),
    ("資產總額", "financial_statement:資產總額", "FinLab", "fundamentals", "economic_measure"),
    ("本益比", "price_earning_ratio:本益比", "FinLab", "valuation", "economic_measure"),
    ("投資性不動產", "financial_statement:投資性不動產", "FinLab", "fundamentals", "economic_measure"),
    ("investors", "sponsor:TaiwanStockHoldingSharesPer", "FinMind", "shareholding", "economic_measure"),
    ("Traders_Tot_All", "physical:cftc:legacy", "CFTC", "derivatives", "economic_measure"),
    ("strike_price", "sponsor:TaiwanOptionDaily", "FinMind", "derivatives", "economic_measure"),
    ("Industry", "sponsor:TaiwanStockInfo", "FinMind", "company_profile", "economic_category"),
    ("stock_id", "sponsor:TaiwanStockBalanceSheet", "FinMind", "fundamentals", "dimension_key"),
    ("archive_sha256", "tw-public:mops_xbrl_quarterly", "MOPS / 公開資訊觀測站", "fundamentals", "provenance"),
    ("next_session_open_gap_logret", "physical:tw-stock:features", "TWSE", "price_technical", "economic_measure"),
    ("return_1d", "physical:tw-stock:features", "TWSE", "execution_rules", "execution_or_label"),
    ("value", "sponsor:TaiwanStockFinancialStatements", "FinMind", "fundamentals", "measure_container"),
    ("type", "sponsor:TaiwanStockFinancialStatements", "FinMind", "fundamentals", "dimension_key"),
    ("資料來源", "price:資料來源", "FinLab", "price_technical", "provenance"),
])
def test_semantics_are_not_training_admission(field, dataset, provider, category, role):
    extra = {"tej_category": "financial"} if provider == "TEJ" else {}
    result = classify(row(field, dataset, provider, **extra))
    assert result["category"] == category
    assert result["semantic_role"] == role
    assert result["training_state"] != "research_value_wired"


def test_financial_source_context_not_housing_or_margin():
    result = classify(row("Mortgage Loans", "tej:Balance Sheet", "TEJ", tej_category="financial"))
    assert result["category"] == "fundamentals"
    assert result["scope"] == "tw_equity"
    assert result["meaningful_in_selected_scope"]


@pytest.mark.parametrize("concept,numeric,role,state", [
    ("tifrs-notes:Assets", 4, "economic_measure", "needs_adapter_clock_units"),
    ("{http://xbrl.example}DirectorName", 0, "dimension_key", "not_selected_input"),
    ("tifrs-notes:PublicationDate", 0, "dimension_key", "not_selected_input"),
    ("tifrs-notes:AuditReportTextBlock", 0, "economic_text", "needs_text_or_parse_adapter"),
    ("tifrs-notes:ObscureUnparsedItem", 0, "unknown_definition", "needs_semantic_definition"),
    ("tifrs-notes:BusinessSegmentAxis", 0, "dimension_key", "not_selected_input"),
])
def test_mops_taxonomy_type_without_numeric_evidence_is_not_numeric(concept, numeric, role, state):
    result = classify(row(concept, "tw-public:mops_xbrl_quarterly", "MOPS / 公開資訊觀測站",
                          expansion_kind="mops_xbrl_concept", numeric_observations=numeric))
    assert result["semantic_role"] == role
    assert result["training_state"] == state


@pytest.mark.parametrize("field", ["備註", "註記", "note_ref", "comments"])
def test_administrative_notes_are_not_financial_measures(field):
    result = classify(row(field))
    assert result["semantic_role"] == "provenance"
    assert not result["meaningful_in_selected_scope"]


@pytest.mark.parametrize("decision", ["quarantined_pit", "quarantined_adapter", "excluded_duplicate_representation", "excluded_lossy_transform"])
def test_unknown_clock_or_duplicate_representation_is_not_meaningless(decision):
    result = classify(row("Assets"), {"decision": decision})
    assert result["meaningful_in_selected_scope"]
    assert result["training_state"] == "needs_adapter_clock_units"
    assert result["prior_admission_decision"] == decision


def test_selected_research_value_not_formal_or_pit_ready():
    result = classify(row("資產總額", "financial_statement:資產總額", "FinLab"),
                      {"admitted_as_model_value": "True", "selected_canonical_feature": "assets"},
                      {"update_frequency": "quarterly", "publication_time_estimated": "True", "carry_days": "200"})
    assert result["training_state"] == "research_value_wired"
    assert result["update_frequency"] == "quarterly"
    assert "原生期間" not in result["rights"]
    assert "不等於正式訓練" in __import__("stockagent.data.tw_feature_semantic_report", fromlist=["STATE_LABELS"]).STATE_LABELS[result["training_state"]]


def test_zero_cells_vs_no_observation_and_tej_query_axis():
    empty = classify(row("EPS", "tej:Income", "TEJ", tej_category="financial", tej_exported_non_null_cells=None))
    found = classify(row("EPS", "tej:Income2", "TEJ", tej_category="financial", tej_exported_non_null_cells=3))
    assert empty["training_state"] == "tej_catalog_only_or_not_yet_acquired"
    assert found["training_state"] == "tej_values_acquired_unwired"
    assert empty["meaningful_in_selected_scope"]


@pytest.mark.parametrize("field,expected", [
    ("Loan Amount(1000 NTD)", "thousand_TWD"), ("Sales(NTD1000)", "thousand_TWD"),
    ("Capital(NTD MN)", "million_TWD"), ("Volume(1000S)", "thousand_shares"),
    ("ROE(%)", "percent"), ("現金(元)", "TWD"),
])
def test_explicit_scale_not_old_registry_shortcut(field, expected):
    assert label_unit(field).startswith(expected)


def test_long_projection_preserves_null_zero_and_native_concepts(tmp_path):
    path = tmp_path / "financial.parquet"
    pq.write_table(pa.table({"type": ["EPS", "EPS", "EPS", "Assets"],
                             "origin_name": ["每股盈餘"] * 3 + ["資產"],
                             "value": [0.0, None, float("nan"), 8.0],
                             "date": ["2024-03-31", "2024-06-30", "2024-09-30", "2024-03-31"]}), path)
    jobs = [dict(provider="FinMind", dataset_id="sponsor:TaiwanStockFinancialStatements", source_path=str(path),
                 dimensions=["type", "origin_name"], value_column="value", date_column="date",
                 expansion_kind="finmind_financial_code", declared_sha256=digest(path))]
    rows, proof = aggregate_long_files(jobs, batch_size=2)
    eps = next(r for r in rows if r["concept_code"] == "EPS")
    assert eps["rows"] == 3
    assert eps["non_null_count"] == 2
    assert eps["numeric_observations"] == 1
    assert eps["observed_zero_count"] == 1
    assert eps["source_last"] == "2024-09-30"
    assert eps["update_frequency"] == "quarterly"
    assert proof[0]["expected_sha256_matched"]
    assert len(rows) == 2
    assert classify(eps)["semantic_role"] == "economic_measure"


def test_long_projection_rejects_corrupt_receipt_sha(tmp_path):
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"k": ["x"], "v": [1.0], "d": ["2024"]}), path)
    job = dict(provider="bea", dataset_id="x", source_path=str(path), dimensions=["k"],
               value_column="v", date_column="d", expansion_kind="bea_series", declared_sha256="wrong")
    with pytest.raises(ValueError, match="SHA"):
        aggregate_long_files([job])


def test_same_names_are_not_merged_and_invalid_dates_not_certified():
    rows = [classify(row("Assets", "one", "TEJ", tej_category="financial", source_first="1911-00-07")),
            classify(row("Assets", "two", "TEJ", tej_category="financial", source_first="2020-01-01"))]
    enrich_report_rows(rows)
    assert rows[0]["same_label_occurrences"] == 2
    assert rows[0]["same_label_group"] == rows[1]["same_label_group"]
    assert "非法日期" in rows[0]["bounds_review"]
    assert report_summary(rows, 0, 2, 0)["feature_entries"] == 2


def test_markdown_all_id_detail_coverage_escaping_and_tamper(tmp_path):
    rows = [classify(row("Cash [ratio]|<value>*", "A", "TEJ", tej_category="financial")),
            classify(row("EPS", "B", "TEJ", tej_category="financial"))]
    enrich_report_rows(rows)
    pages = write_feature_pages(rows, tmp_path, page_size=1)
    atomic_write_text(tmp_path / "README.md", "# 摘要\n\n[分類](fundamentals/index.md)\n")
    write_source_pages([{"id": "x", "provider": "TEJ", "title": "原來源"}], tmp_path, 1)
    atomic_write_text(tmp_path / "feature_catalog.jsonl", "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n")
    atomic_write_json(tmp_path / "report_manifest.json", {"summary": report_summary(rows, 0, 2, 1),
                                                        "feature_pages": pages,
                                                        "feature_catalog_sha256": digest(tmp_path / "feature_catalog.jsonl")})
    proof = verify_report(tmp_path, {r["catalog_id"] for r in rows})
    assert proof["markdown_feature_details"] == 2
    assert proof["local_links_verified"] > 2
    assert anchor(rows[0]["catalog_id"]) in (tmp_path / rows[0]["report_file"]).read_text()
    atomic_write_text(tmp_path / pages[0]["file"], "lost feature")
    with pytest.raises(ValueError, match="hash"):
        verify_report(tmp_path)


@pytest.fixture
def accepted_source_report(tmp_path):
    inputs = [{"path": "catalog.csv", "sha256": "fixed-input"}]
    tej = row("Assets", "tej:Financial", "TEJ", tej_category="financial")
    expanded = row("EPS", expansion_kind="finmind_financial_code", numeric_observations=3)
    proof = {"mode": "sqlite_read_transaction", "fields": 1, "read_at_utc": "2026-10-04T00:00:00+00:00"}
    atomic_write_json(tmp_path / "tej_feature_read_snapshot.json", {"proof": proof, "features": [tej]})
    atomic_write_text(tmp_path / "expanded_source_snapshot.jsonl", json.dumps(expanded) + "\n")
    atomic_write_json(tmp_path / "source_projection_evidence.json", {"full_projection": [], "footer_only": []})
    final = [classify(tej), classify(expanded)]
    enrich_report_rows(final)
    atomic_write_text(tmp_path / "feature_catalog.jsonl", "\n".join(json.dumps(r) for r in final) + "\n")
    atomic_write_json(tmp_path / "report_manifest.json", {
        "contract": CONTRACT, "inputs": inputs,
        "summary": {"feature_entries": 2, "tej_read_snapshot": proof},
        "feature_catalog_sha256": digest(tmp_path / "feature_catalog.jsonl"),
        "source_projection_evidence_sha256": digest(tmp_path / "source_projection_evidence.json"),
    })
    atomic_write_json(tmp_path / "report_acceptance.json", {
        "state": "accepted_report_scope", "report_manifest_sha256": digest(tmp_path / "report_manifest.json"),
    })
    return tmp_path, inputs


def test_report_reclassification_reuses_only_hash_bound_source_fields(accepted_source_report):
    root, inputs = accepted_source_report
    tej, proof, expanded, projection = reuse_source_snapshot(root, inputs)
    assert tej[0]["field"] == "Assets"
    assert expanded[0]["numeric_observations"] == 3
    assert proof["read_at_utc"] == "2026-10-04T00:00:00+00:00"
    assert projection["reuse_proof"]["raw_source_fields_reconciled"] == 2
    assert projection["reuse_proof"]["not_a_new_source_download_or_freshness_check"]


@pytest.mark.parametrize("which", ["raw_values", "catalog_hash", "input_identity", "projection_hash", "acceptance"])
def test_report_reclassification_rejects_mixed_or_tampered_evidence(accepted_source_report, which):
    root, inputs = accepted_source_report
    if which == "raw_values":
        atomic_write_text(root / "expanded_source_snapshot.jsonl", json.dumps(
            row("EPS", expansion_kind="finmind_financial_code", numeric_observations=4)) + "\n")
    elif which == "catalog_hash":
        atomic_write_text(root / "feature_catalog.jsonl", "{}\n")
    elif which == "input_identity":
        inputs = [{"path": "catalog.csv", "sha256": "different-input"}]
    elif which == "projection_hash":
        atomic_write_json(root / "source_projection_evidence.json", {})
    else:
        atomic_write_json(root / "report_acceptance.json", {"state": "accepted_report_scope", "report_manifest_sha256": "wrong"})
    with pytest.raises(ValueError):
        reuse_source_snapshot(root, inputs)
