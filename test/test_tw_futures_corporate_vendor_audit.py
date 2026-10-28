from copy import deepcopy

import pytest

from scripts.audit_tw_futures_corporate_vendor import reconcile, verify_raw_vendor_cells


def _fixture():
    candidate = {"physical_contract": "ODF:202103@unsettled#generation=0",
                 "successor_product": "OD1", "contract_month": "202103",
                 "adjusted_date": "2020-07-09", "tej_shares": "2041.1252",
                 "receipt": "data_tej/receipts/a.json", "task_id": "a"}
    official = [{"product": "OD1", "from_product": "ODF", "contract": "202103",
                 "effective_date": "2020-07-09", "contract_multiplier": 2041.1254,
                 "has_equity_credit_fields": True, "equity_cash_credit_twd": 13982.0,
                 "source_content_sha256s": ["original"], "source_urls": ["official"]}]
    vendor = [{"_query_symbol": "OD1202103", "_query_period": "2020-07-09",
               "Adjusted Date": "2020/07/09", "Future Code": "OD1202103 為升",
               "Shares per Contract": "2041.1252", "Cash Dividends per Unit": "13983"}]
    return candidate, official, vendor


def test_source_identity_is_matched_before_reporting_two_distinct_conflicts():
    result = reconcile(*_fixture())
    assert result["shares_per_contract"] == {"official": "2041.1254", "vendor": "2041.1252", "status": "conflict"}
    assert result["cash_credit_per_contract_twd"]["status"] == "conflict"
    assert not result["inventory_transfer_or_valuation_verified"]


def test_absent_official_equity_field_is_unknown_not_zero():
    candidate, official, vendor = _fixture()
    official[0]["has_equity_credit_fields"] = False
    official[0]["equity_cash_credit_twd"] = None
    vendor[0]["Cash Dividends per Unit"] = "0"
    assert reconcile(candidate, official, vendor)["cash_credit_per_contract_twd"]["status"] == "unknown"


def test_ambiguous_source_or_changed_vendor_cell_is_rejected():
    candidate, official, vendor = _fixture()
    with pytest.raises(ValueError, match="ambiguous"):
        reconcile(candidate, official + deepcopy(official), vendor)
    vendor[0]["Shares per Contract"] = "2041.1254"
    with pytest.raises(ValueError, match="differ"):
        reconcile(candidate, official, vendor)


def test_raw_vendor_grid_must_match_exact_parquet_cells():
    candidate, _, vendor = _fixture()
    names = ["Future Code", "Adjusted Date", "Shares per Contract", "Cash Dividends per Unit"]
    raw = {"cells": [names, [vendor[0][name] for name in names]]}
    verify_raw_vendor_cells(candidate, raw, vendor)
    raw["cells"][1][-1] = "13982"
    with pytest.raises(ValueError, match="differs"):
        verify_raw_vendor_cells(candidate, raw, vendor)
