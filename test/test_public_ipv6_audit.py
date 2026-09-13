from __future__ import annotations

import pytest

from scripts.audit_public_ipv6 import audit_public_ipv6


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
        read_json=lambda _url, *, timeout_seconds: [
            {"name": "ipv6", "done": True, "success": True},
            {"name": "tls", "done": True, "success": False},
        ],
    )

    assert receipt["passed"] is True
    assert receipt["dns"]["aaaa"] == ["2001:db8::10"]
    assert receipt["external_probe"]["ipv6"]["success"] is True
    assert triggered == [("dashboard.example.com", 1.0)]
    assert "same-host WSL curl" in receipt["evidence_boundary"]


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


def test_hostname_validation_prevents_arbitrary_probe_urls() -> None:
    with pytest.raises(ValueError, match="invalid hostname"):
        audit_public_ipv6(
            "https://example.com/path",
            resolve_ipv6=lambda _host: [],
            read_json=lambda _url, *, timeout_seconds: [],
        )
