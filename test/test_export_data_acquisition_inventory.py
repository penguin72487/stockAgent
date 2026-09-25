from __future__ import annotations

import pytest

from scripts.export_data_acquisition_inventory import acquisition_role, rows_from_snapshot


def test_export_preserves_receipt_uncertainty_and_explicit_roles() -> None:
    snapshot = {
        "generated_at_utc": "2026-09-26T01:00:00Z",
        "sources": [
            {"id": "finmind:sponsor:TaiwanStockPrice", "provider": "FinMind",
             "coverage": {"current": 1, "total": 10, "unit": "partitions"},
             "record_stats": {"count": None, "first": None, "state": "unverified"}},
            {"id": "finmind:sponsor:TaiwanStockInstitutionalInvestorsBuySellWide",
             "provider": "FinMind"},
        ],
    }
    rows = rows_from_snapshot(snapshot)
    assert len(rows) == 2
    assert rows[0]["acquisition_role"] == "early_history_and_gap_fill_then_validation"
    assert rows[0]["record_count"] is None
    assert rows[0]["record_evidence"] == "unverified"
    assert rows[1]["acquisition_role"] == "derived_from_finmind_long_no_api"
    assert acquisition_role({"id": "unknown", "provider": "FinLab"}) == (
        "independent_research_source_unreconciled")


def test_export_rejects_duplicate_ids() -> None:
    with pytest.raises(ValueError, match="duplicate source id"):
        rows_from_snapshot({"sources": [{"id": "a"}, {"id": "a"}]})
