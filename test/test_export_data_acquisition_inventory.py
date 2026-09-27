from __future__ import annotations

import json

import pytest

from scripts.export_data_acquisition_inventory import (
    acquisition_role, api_rows, enrich_finmind_receipts, policy_summary,
    rows_from_snapshot, write_bundle,
)


def test_export_preserves_receipt_uncertainty_and_explicit_roles() -> None:
    snapshot = {
        "generated_at_utc": "2026-09-26T01:00:00Z",
        "sources": [
            {"id": "finmind:sponsor:TaiwanStockPrice",
             "endpoint_id": "finmind:sponsor:TaiwanStockPrice", "registry_alias": False,
             "in_active_scope": True, "provider": "FinMind",
             "coverage": {"current": 1, "total": 10, "unit": "partitions"},
             "record_stats": {"count": None, "first": None, "state": "unverified"}},
            {"id": "finmind:sponsor:TaiwanStockInstitutionalInvestorsBuySellWide",
             "provider": "FinMind"},
        ],
    }
    rows = rows_from_snapshot(snapshot)
    assert len(rows) == 2
    assert rows[0]["acquisition_role"] == "early_history_and_gap_fill_then_validation"
    assert rows[0]["endpoint_id"] == snapshot["sources"][0]["endpoint_id"]
    assert rows[0]["registry_alias"] is False
    assert rows[0]["record_count"] is None
    assert rows[0]["record_evidence"] == "unverified"
    assert rows[1]["acquisition_role"] == "derived_from_finmind_long_no_api"
    assert acquisition_role({"id": "unknown", "provider": "FinLab"}) == (
        "independent_research_source_unreconciled")


def test_export_rejects_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="duplicate source id"):
        rows_from_snapshot({"sources": [{"id": "a"}, {"id": "a"}]})


def test_export_routing_is_not_duplicate_or_completeness_proof():
    rows = rows_from_snapshot({"sources": [
        {"id": "finmind:price", "provider": "FinMind", "in_active_scope": True},
        {"id": "finmind:alias", "provider": "FinMind", "registry_alias": True},
        {"id": "crypto:okx", "provider": "OKX", "in_active_scope": True},
    ]})
    assert rows[0]["quota_scope"].startswith("finmind-v4-data")
    assert rows[0]["acquisition_blocker"] == "acquisition_proof_missing"
    assert rows[1]["work_class"] == "reference_not_work"
    assert "instrument" in rows[2]["quota_scope"]
    summary = policy_summary(rows)
    assert summary["providers"]["FinMind"]["required_endpoints_not_ready"] == 1
    assert summary["providers"]["FinMind"]["registered_rows"] == 2


def test_schedule_does_not_become_source_publication():
    row = rows_from_snapshot({"sources": [{"id": "x", "data_through": "2099-01-01",
        "automation": {"next_run_at_utc": "2026-09-28T01:00:00Z", "service_keys": ["x"]},
        "publication": {"expected_release_at_utc": "2026-09-29T10:00:00Z", "schedule_kind": "inferred"}}]})[0]
    assert row["first_observed"] is None
    assert row["last_observed"] is None
    assert row["next_run_at_utc"] != row["expected_release_at_utc"]
    assert row["publication_clock_kind"] == "inferred"


def test_finmind_receipt_dates_are_not_query_bounds_or_verified_unique_rows(tmp_path):
    path = tmp_path / "data_finmind/sponsor/status.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"series": {"TaiwanStockPrice": {"rows": 100,
        "first_data_date": "2015-01-01", "last_data_date": "2020-01-01",
        "last_checked_partition": "2026-09-27"}}}))
    rows = rows_from_snapshot({"sources": [{"id": "finmind:sponsor:TaiwanStockPrice"}]})
    enrich_finmind_receipts(rows, tmp_path)
    row = rows[0]
    assert row["record_count"] is None
    assert row["receipt_reported_rows"] == 100
    assert row["receipt_last_data_date"] == "2020-01-01"
    assert row["configured_request_start"] == "1994-10-01"
    assert "not_verified" in row["history_start_basis"]


def test_api_export_is_allowlisted_and_credential_gate_not_data():
    credential = {"secret_values_included": False, "providers": [{"id": "noaa_cdo",
        "state": "configured", "required_names": ["NOAA_CDO_TOKEN"], "unexpected_secret": "DO_NOT_EXPORT"}]}
    rows = rows_from_snapshot({"sources": [{"id": "credential:noaa_cdo"},
        {"id": "keyed-public:noaa_cdo:datasets", "in_active_scope": True}]})
    result = api_rows(credential, rows)[0]
    assert result["registered_data_rows"] == 1
    assert result["authentication_verified"] is False
    assert "DO_NOT_EXPORT" not in json.dumps(result)
    with pytest.raises(ValueError, match="exclude secret"):
        api_rows({"providers": []}, rows)


def test_complement_uses_actual_two_part_source_id(tmp_path):
    path = tmp_path / "data_finmind/complement/status.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"series": {"TaiwanStockInfo": {
        "rows": 4000, "first_data_date": "2020-06-03", "last_data_date": "2026-09-27"}}}))
    rows = rows_from_snapshot({"sources": [{"id": "finmind:TaiwanStockInfo"}]})
    enrich_finmind_receipts(rows, tmp_path)
    assert rows[0]["receipt_first_data_date"] == "2020-06-03"
    assert rows[0]["receipt_reported_rows"] == 4000
    assert "configured_request_start" not in rows[0]  # Current identity snapshot, not historical price coverage.


def test_bundle_does_not_sum_aliases_or_claim_global_complete(tmp_path):
    rows = rows_from_snapshot({"sources": [{"id": "finlab:x", "registry_alias": True}]})
    summary = write_bundle(tmp_path, rows,
        credentials={"secret_values_included": False, "providers": []}, registry={}, taifex={})
    assert summary["registered_rows"] == 1
    assert summary["global_bulk_downloads_authorized_now"] is False
    assert summary["exhaustive_all_internet_data"] is False
    assert (tmp_path / "data_inventory.csv").exists()
    assert "不是零" in (tmp_path / "README.md").read_text()
