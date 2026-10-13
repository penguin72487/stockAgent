from __future__ import annotations

import pytest

from scripts import audit_public_ipv6 as module
from scripts.audit_public_ipv6 import audit_public_ipv6


def _healthy_report(address: str = "2001:db8::10", reach_status: int = 1) -> dict:
    return {
        "done": True,
        "name": "siteipv6",
        "details_set": [["", "", {
            "web_aaaa": {
                "status": 1,
                "tech_data": [["dashboard.example.com", [address], ["198.51.100.1"]]],
            },
            "web_reach": {"status": reach_status},
        }]],
    }


def test_external_probe_and_aaaa_are_both_required() -> None:
    triggered: list[tuple[str, float]] = []

    receipt = audit_public_ipv6(
        "dashboard.example.com",
        timeout_seconds=1.0,
        poll_seconds=0.01,
        resolve_ipv6=lambda _host: ["2001:db8::10"],
        trigger_probe=lambda host, *, timeout_seconds: triggered.append(
            (host, timeout_seconds)
        ),
        read_json=lambda _url, *, timeout_seconds: _healthy_report() if "/site/ipv6/" in _url else [
            {"name": "ipv6", "done": True, "success": True},
            {"name": "tls", "done": True, "success": False},
        ],
    )

    assert receipt["passed"] is True
    assert receipt["dns"]["aaaa"] == ["2001:db8::10"]
    assert receipt["external_probe"]["ipv6"]["success"] is True
    assert receipt["external_probe"]["probe_request_issued"] is True
    assert receipt["state"] == "passed"
    assert receipt["external_probe"]["web_evidence"]["dns_matches_report"] is True
    assert triggered == [("dashboard.example.com", 1.0)]
    assert "Same-host WSL curl" in receipt["evidence_boundary"]


@pytest.mark.parametrize(
    ("addresses", "probe_success"),
    [([], True), (["2001:db8::10"], False)],
)
def test_missing_aaaa_or_failed_external_probe_fails_closed(
    addresses: list[str], probe_success: bool
) -> None:
    receipt = audit_public_ipv6(
        "dashboard.example.com",
        start_probe=False,
        timeout_seconds=1.0,
        poll_seconds=0.01,
        resolve_ipv6=lambda _host: addresses,
        read_json=lambda _url, *, timeout_seconds: [
            {"name": "ipv6", "done": True, "success": probe_success}
        ],
    )

    assert receipt["passed"] is False
    assert receipt["state"] == "failed"


def test_unfinished_external_probe_is_inconclusive_not_a_network_failure() -> None:
    receipt = audit_public_ipv6(
        "dashboard.example.com",
        start_probe=False,
        timeout_seconds=0.001,
        poll_seconds=0.001,
        resolve_ipv6=lambda _host: ["2001:db8::10"],
        read_json=lambda _url, *, timeout_seconds: [
            {"name": "ipv6", "done": False, "success": False}
        ],
    )

    assert receipt["passed"] is False
    assert receipt["state"] == "inconclusive_timeout"
    assert receipt["external_probe"]["probe_request_issued"] is False


def test_hostname_validation_prevents_arbitrary_probe_urls() -> None:
    with pytest.raises(ValueError, match="invalid hostname"):
        audit_public_ipv6(
            "https://example.com/path",
            resolve_ipv6=lambda _host: [],
            read_json=lambda _url, *, timeout_seconds: [],
        )


def test_each_poll_has_a_fresh_nonce_instead_of_reusing_pending_http_cache() -> None:
    urls: list[str] = []

    def read(url: str, *, timeout_seconds: float) -> list[dict]:
        if "/site/ipv6/" in url:
            return _healthy_report()
        urls.append(url)
        return [{"name": "ipv6", "done": len(urls) == 2, "success": True}]

    receipt = audit_public_ipv6(
        "dashboard.example.com",
        start_probe=False,
        timeout_seconds=1,
        poll_seconds=0.001,
        resolve_ipv6=lambda _host: ["2001:db8::10"],
        read_json=read,
    )

    assert receipt["passed"] is True
    assert len(urls) == 2
    assert urls[0] != urls[1]
    assert all(url.startswith("https://internet.nl/site/probes/dashboard.example.com/?_=") for url in urls)
    assert receipt["external_probe"]["poll_cache_busted"] is True
    assert receipt["external_probe"]["probe_request_issued"] is False


def test_probe_trigger_and_json_reader_ask_for_uncached_responses(monkeypatch) -> None:
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, *_args):
            return b"[]"

    def open_request(request, *, timeout):
        requests.append(request)
        return Response()

    monkeypatch.setattr(module, "urlopen", open_request)
    module._start_probe("dashboard.example.com", timeout_seconds=1)
    assert module._read_json("https://internet.nl/site/probes/dashboard.example.com/?_=1", timeout_seconds=1) == []
    assert requests[0].full_url.startswith("https://internet.nl/site/dashboard.example.com/?_=")
    assert all(request.get_header("Cache-control") == "no-cache" for request in requests)


@pytest.mark.parametrize(
    "report",
    [None, {}, {"done": True}, _healthy_report(reach_status=0), _healthy_report(address="2001:db8::99")],
)
def test_successful_probe_job_is_not_enough_to_claim_current_website_reachable(report) -> None:
    receipt = audit_public_ipv6(
        "dashboard.example.com",
        start_probe=False,
        timeout_seconds=1,
        poll_seconds=0.01,
        resolve_ipv6=lambda _host: ["2001:db8::10"],
        read_json=lambda url, *, timeout_seconds: report if "/site/ipv6/" in url else [
            {"name": "ipv6", "done": True, "success": True}
        ],
    )
    assert receipt["passed"] is False
    assert receipt["external_probe"]["ipv6"]["success"] is True
    assert receipt["schema_version"] == 2


def test_ipv6_report_addresses_are_compared_as_addresses_not_text() -> None:
    evidence = module._web_ipv6_evidence(
        _healthy_report(address="2001:0db8:0000:0000:0000:0000:0000:0010"),
        ["2001:db8::10"],
    )
    assert evidence["dns_matches_report"] is True
