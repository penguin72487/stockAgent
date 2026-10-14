from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from scripts import audit_public_ipv6 as module


HOST = "dashboard.example.com"
PATH = "/tw-day-trade/"
ADDRESSES = {4: ["198.51.100.1"], 6: ["2001:db8::10"]}


def _measurement(family: int, created: datetime | None = None) -> dict:
    return {
        "id": f"fresh{family}", "type": "http", "status": "finished", "target": HOST,
        "createdAt": (created or datetime.now(UTC)).isoformat(),
        "measurementOptions": {"port": 443, "ipVersion": family, "request": {"path": PATH, "method": "GET"}},
        "results": [
            {"probe": {"country": country, "city": country}, "result": {
                "status": "finished", "resolvedAddress": ADDRESSES[family][0],
                "statusCode": 200, "tls": {"authorized": True}, "timings": {"total": 50},
            }} for country in ("TW", "JP", "SG")
        ],
    }


def _evidence(measurement: dict, family: int = 6) -> dict:
    return module._https_measurement_evidence(
        measurement, hostname=HOST, family=family, addresses=ADDRESSES[family],
        path=PATH, countries=("TW", "JP", "SG"), started_at=datetime.now(UTC),
    )


def test_https_family_certificate_address_and_all_locations_are_required() -> None:
    assert _evidence(_measurement(6))["passed"] is True
    assert _evidence(_measurement(4), family=4)["passed"] is True
    default_ipv4 = _measurement(4)
    del default_ipv4["measurementOptions"]["ipVersion"]
    assert _evidence(default_ipv4, family=4)["passed"] is True


@pytest.mark.parametrize("broken", [
    "old_job", "future_job", "pending", "wrong_host", "wrong_path", "wrong_port", "wrong_family",
    "wrong_address", "invalid_address", "invalid_tls", "missing_tls", "not_200", "missing_country",
    "duplicate_country", "invalid_options", "invalid_results", "invalid_probe",
])
def test_probe_acceptance_or_partial_success_never_means_dual_stack_success(broken: str) -> None:
    measurement = _measurement(6)
    first = measurement["results"][0]
    if broken == "old_job":
        measurement["createdAt"] = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    elif broken == "future_job":
        measurement["createdAt"] = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    elif broken == "pending": measurement["status"] = "in-progress"
    elif broken == "wrong_host": measurement["target"] = "wrong.example.com"
    elif broken == "wrong_path": measurement["measurementOptions"]["request"]["path"] = "/"
    elif broken == "wrong_port": measurement["measurementOptions"]["port"] = 80
    elif broken == "wrong_family": first["result"]["resolvedAddress"] = ADDRESSES[4][0]
    elif broken == "wrong_address": first["result"]["resolvedAddress"] = "2001:db8::99"
    elif broken == "invalid_address": first["result"]["resolvedAddress"] = "not-an-address"
    elif broken == "invalid_tls": first["result"]["tls"]["authorized"] = False
    elif broken == "missing_tls": first["result"].pop("tls")
    elif broken == "not_200": first["result"]["statusCode"] = 503
    elif broken == "missing_country": measurement["results"].pop()
    elif broken == "duplicate_country": measurement["results"][1] = deepcopy(first)
    elif broken == "invalid_options": measurement["measurementOptions"] = "accepted"
    elif broken == "invalid_results": measurement["results"] = None
    elif broken == "invalid_probe": first["probe"] = "TW"
    assert _evidence(measurement)["passed"] is False


def test_both_families_start_before_polling_and_successes_are_retained() -> None:
    calls = []

    def request(url, *, payload=None, timeout_seconds):
        calls.append((url, payload))
        if payload:
            family = payload["measurementOptions"]["ipVersion"]
            assert payload["measurementOptions"]["protocol"] == "HTTPS"
            return {"id": f"fresh{family}"}
        family = int(url[-1])
        measurement = _measurement(family)
        if family == 6:
            measurement["results"][0]["result"]["statusCode"] = 503
        return measurement

    receipt = module.audit_public_dual_stack_https(
        HOST, resolve_family=lambda host, family: ADDRESSES[family], request_json=request,
    )
    assert [payload["measurementOptions"]["ipVersion"] for _, payload in calls[:2]] == [4, 6]
    assert receipt["families"]["4"]["passed"] is True
    assert receipt["families"]["6"]["passed"] is False
    assert receipt["passed"] is False
    assert receipt["state"] == "not_verified"


def test_missing_provider_or_dns_is_inconclusive_not_manufactured_outage() -> None:
    def request(url, *, payload=None, timeout_seconds):
        raise OSError("provider temporarily unavailable")

    receipt = module.audit_public_dual_stack_https(
        HOST, resolve_family=lambda host, family: ADDRESSES[family], request_json=request,
    )
    assert receipt["passed"] is False
    assert all(state["state"] == "inconclusive" for state in receipt["families"].values())
    assert all("provider temporarily unavailable" in state["error"] for state in receipt["families"].values())


def test_dns_change_during_measurement_is_not_accepted_against_old_address() -> None:
    counts = {4: 0, 6: 0}

    def resolve(host, family):
        counts[family] += 1
        return ADDRESSES[family] if counts[family] == 1 or family == 4 else ["2001:db8::99"]

    def request(url, *, payload=None, timeout_seconds):
        if payload:
            return {"id": f"fresh{payload['measurementOptions']['ipVersion']}"}
        return _measurement(int(url[-1]))

    receipt = module.audit_public_dual_stack_https(HOST, resolve_family=resolve, request_json=request)
    assert receipt["families"]["4"]["passed"] is True
    assert receipt["families"]["6"]["passed"] is False
    assert receipt["families"]["6"]["initial_dns_addresses"] == ["2001:db8::10"]
    assert receipt["families"]["6"]["dns_addresses"] == ["2001:db8::99"]


@pytest.mark.parametrize("kwargs", [
    {"path": "https://unrelated.example.com"}, {"path": "//unrelated.example.com"},
    {"path": "/\r\nInjected: yes"}, {"countries": ("TW", "TW")},
    {"countries": ("TW", "JP", "SG", "US")}, {"timeout_seconds": 0},
])
def test_invalid_requests_are_rejected_without_external_calls(kwargs) -> None:
    with pytest.raises(ValueError):
        module.audit_public_dual_stack_https(HOST, **kwargs)
