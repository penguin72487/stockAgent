from copy import deepcopy
import json

import pytest

from scripts import audit_public_source_display as audit


@pytest.mark.parametrize("value,expected", [
    ([{"name": "收盤", "description": "not part of the name", "uri": "private"},
      {"name": " 開盤 "}, None, {"description": "no field name"}], ["收盤", "開盤"]),
    ("收盤, 開盤，次日漲停價；次日跌停價", ["收盤", "開盤", "次日漲停價", "次日跌停價"]),
    (None, []), ({"name": "not a list"}, []),
])
def test_government_field_metadata_is_parsed_without_leaking_other_keys(value, expected):
    assert audit.field_names(value) == expected


def fixtures():
    policy = json.loads((audit.ROOT / "configs/public_quote_display.json").read_text())
    metadata = {gid: {"state": "verified_open_v1", "fields": list(audit.FIELD_NAMES[name].values())}
                for name, gid in audit.GOV_IDS.items()}
    return policy, metadata


def test_existing_source_registry_is_inventoried_without_authorizing_all_sources(tmp_path):
    policy, metadata = fixtures()
    catalog = json.loads((audit.ROOT / "configs/free_public_data_sources.json").read_text())
    result = audit.build_inventory(policy=policy, catalog=catalog, metadata=metadata, root=tmp_path)
    assert result["status"] == "accepted_current_public_grants"
    assert result["summary"]["registered_families"] == len(catalog["sources"])
    assert len(result["official_datasets"]) == len(audit.EXTENDED_DATASETS)
    assert result["all_source_licenses_verified"] is False
    assert result["vendor_raw_prices_enabled"] is False
    assert result["ledger_writes"] is False
    assert result["public_request_provider_calls"] is False
    assert not any(row["local_materialized"] for row in result["official_datasets"])


def test_a_twse_daily_license_cannot_expand_to_unlisted_final_bid_ask(tmp_path):
    policy, metadata = fixtures()
    policy = deepcopy(policy)
    policy["authorized_sources"][0]["daily_price_roles"]["last_bid"] = ["last_bid"]
    result = audit.build_inventory(policy=policy, catalog={"sources": []}, metadata=metadata, root=tmp_path)
    assert result["status"] == "grant_verification_failed"
    assert result["summary"]["grant_errors"][0]["unverified_fields"] == ["last_bid"]


@pytest.mark.parametrize("change", [
    {"state": "metadata_unavailable"}, {"state": "license_needs_review"},
    {"state": "verified_open_v1", "fields": []},
])
def test_missing_source_or_unverified_field_does_not_pass_audit(tmp_path, change):
    policy, metadata = fixtures()
    metadata["11370"].update(change)
    result = audit.build_inventory(policy=policy, catalog={"sources": []}, metadata=metadata, root=tmp_path)
    assert result["status"] == "grant_verification_failed"
    assert {row["source_id"] for row in result["summary"]["grant_errors"]} == {"tpex_daily_ohlcv"}
