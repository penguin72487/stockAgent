import json

import pytest

from scripts.snapshot_eodhd_quota import public_quota


def test_eodhd_quota_contains_neither_account_identity_nor_token():
    payload = dict(dailyRateLimit=20, apiRequests=0, extraLimit=500,
                   apiRequestsDate="1970-01-01", subscriptionType="free",
                   email="PRIVATE", name="PRIVATE", apiKey="SECRET", other={"token": "SECRET"})
    result = public_quota(payload)
    assert "PRIVATE" not in json.dumps(result) and "SECRET" not in json.dumps(result)
    assert result["dailyRateLimit"] == 20 and result["usage_endpoint_call_cost"] == 0
    assert not result["counter_is_for_today"] and not result["extra_calls_auto_spend"]
    assert not result["data_endpoints_verified"]


@pytest.mark.parametrize("value", [True, -1, "NaN", None, 1.5])
def test_eodhd_invalid_counter_rejected(value):
    with pytest.raises(ValueError):
        public_quota(dict(dailyRateLimit=value, apiRequests=0, extraLimit=500,
                         apiRequestsDate="2026-09-27", subscriptionType="free"))
