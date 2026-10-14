#!/usr/bin/env python3
"""Verify public IPv6 reachability with an independent Internet probe.

The dashboard host itself is not an independent WAN vantage point.  In
particular, WSL/Windows mirrored networking and router hairpin policy can make a
same-host ``curl -6`` fail even while the public IPv6 listener is reachable.
This audit therefore combines DNS resolution with Internet.nl's external IPv6
probe and writes a machine-readable receipt.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import re
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen


_HOSTNAME_RE = re.compile(
    r"(?=.{1,253}\Z)(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\Z"
)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _read_json(url: str, *, timeout_seconds: float) -> Any:
    request = Request(
        url,
        headers={
            "User-Agent": "StockAgent-public-IPv6-audit/1",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _start_probe(hostname: str, *, timeout_seconds: float) -> None:
    request = Request(
        f"https://internet.nl/site/{hostname}/?_={time.time_ns()}",
        headers={
            "User-Agent": "StockAgent-public-IPv6-audit/1",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        response.read(1)


def _resolve_ipv6(hostname: str) -> list[str]:
    values = {
        str(item[4][0])
        for item in socket.getaddrinfo(hostname, 443, socket.AF_INET6, socket.SOCK_STREAM)
    }
    return sorted(values)


def _web_ipv6_evidence(report: Any, addresses: list[str]) -> dict[str, Any]:
    """Assess rated web checks; Celery task success alone is not reachability."""
    evidence: dict[str, Any] = {
        "report_done": False,
        "web_aaaa_good": False,
        "web_reachable": False,
        "dns_matches_report": False,
        "report_addresses": [],
    }
    if not isinstance(report, dict) or report.get("name") != "siteipv6":
        return evidence
    evidence["report_done"] = report.get("done") is True
    sections = report.get("details_set")
    if not isinstance(sections, list) or len(sections) != 1:
        return evidence
    section = sections[0]
    if not isinstance(section, list) or len(section) < 3 or not isinstance(section[2], dict):
        return evidence
    checks = section[2]
    aaaa = checks.get("web_aaaa", {})
    reach = checks.get("web_reach", {})
    if not isinstance(aaaa, dict) or not isinstance(reach, dict):
        return evidence
    evidence["web_aaaa_good"] = type(aaaa.get("status")) is int and aaaa["status"] == 1
    evidence["web_reachable"] = type(reach.get("status")) is int and reach["status"] == 1
    rows = aaaa.get("tech_data")
    if not isinstance(rows, list):
        return evidence
    try:
        observed: set[str] = set()
        for row in rows:
            if not isinstance(row, list) or len(row) < 2 or not isinstance(row[1], list):
                return evidence
            observed.update(str(ipaddress.IPv6Address(address)) for address in row[1])
        current = {str(ipaddress.IPv6Address(address)) for address in addresses}
    except (ValueError, TypeError):
        return evidence
    evidence["report_addresses"] = sorted(observed)
    evidence["dns_matches_report"] = bool(current) and current == observed
    return evidence


def audit_public_ipv6(
    hostname: str,
    *,
    start_probe: bool = True,
    timeout_seconds: float = 90.0,
    poll_seconds: float = 2.0,
    resolve_ipv6: Callable[[str], list[str]] = _resolve_ipv6,
    read_json: Callable[..., Any] = _read_json,
    trigger_probe: Callable[..., None] = _start_probe,
) -> dict[str, Any]:
    """Return an external IPv6 audit receipt without treating hairpin as WAN proof."""

    if not _HOSTNAME_RE.fullmatch(hostname):
        raise ValueError(f"invalid hostname: {hostname!r}")
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("timeouts must be positive")

    started_at = _utc_now()
    addresses = resolve_ipv6(hostname)
    probe_url = f"https://internet.nl/site/probes/{hostname}/"
    if start_probe:
        trigger_probe(hostname, timeout_seconds=min(timeout_seconds, 30.0))

    deadline = time.monotonic() + timeout_seconds
    probes: list[dict[str, Any]] = []
    timed_out = False
    while True:
        # Internet.nl's own client uses jQuery cache:false. Without an equivalent
        # nonce, its HTTP max-age=60 response can repeat the initial done:false
        # after the external work has finished and falsely time out this audit.
        payload = read_json(
            f"{probe_url}?_={time.time_ns()}",
            timeout_seconds=min(poll_seconds + 5.0, 30.0),
        )
        if not isinstance(payload, list):
            raise RuntimeError("Internet.nl returned a non-list probe payload")
        probes = [dict(item) for item in payload if isinstance(item, dict)]
        ipv6 = next((item for item in probes if item.get("name") == "ipv6"), None)
        if ipv6 is not None and bool(ipv6.get("done")):
            break
        if time.monotonic() >= deadline:
            timed_out = True
            break
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

    ipv6 = next((item for item in probes if item.get("name") == "ipv6"), {})
    details_url = f"https://internet.nl/site/ipv6/{hostname}/"
    details = None
    if addresses and ipv6.get("done") is True and ipv6.get("success") is True:
        details = read_json(
            f"{details_url}?_={time.time_ns()}",
            timeout_seconds=min(poll_seconds + 5.0, 30.0),
        )
    web_evidence = _web_ipv6_evidence(details, addresses)
    passed = all(
        web_evidence[key]
        for key in ("report_done", "web_aaaa_good", "web_reachable", "dns_matches_report")
    )
    return {
        "schema_version": 2,
        "hostname": hostname,
        "started_at_utc": started_at,
        "completed_at_utc": _utc_now(),
        "state": (
            "passed" if passed else "inconclusive_timeout" if timed_out else "failed"
        ),
        "dns": {"aaaa": addresses, "has_aaaa": bool(addresses)},
        "external_probe": {
            "provider": "Internet.nl",
            "probe_request_issued": bool(start_probe),
            "result_url": f"https://internet.nl/site/{hostname}/",
            "probe_url": probe_url,
            "poll_cache_busted": True,
            "ipv6": ipv6,
            "ipv6_report_url": details_url,
            "ipv6_report": details,
            "web_evidence": web_evidence,
        },
        "passed": passed,
        "evidence_boundary": (
            "Rated external web AAAA/reachability checks must match current public "
            "DNS. Probe task done/success alone is not website reachability. "
            "Same-host WSL curl is not WAN evidence; issuing a probe request does "
            "not guarantee a fresh provider job rather than a reused provider result."
        ),
    }


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _globalping_request(
    url: str, *, payload: dict[str, Any] | None = None, timeout_seconds: float
) -> Any:
    request = Request(
        url,
        data=None if payload is None else json.dumps(payload).encode("utf-8"),
        headers={
            "User-Agent": "StockAgent-public-dual-stack-audit/1",
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _https_measurement_evidence(
    measurement: Any, *, hostname: str, family: int, addresses: list[str],
    path: str, countries: tuple[str, ...], started_at: datetime,
) -> dict[str, Any]:
    """Require fresh, family-specific HTTPS results, not job acceptance."""
    evidence: dict[str, Any] = {"passed": False, "fresh": False, "results": []}
    if not isinstance(measurement, dict):
        return evidence
    evidence["measurement_id"] = measurement.get("id")
    evidence["created_at_utc"] = measurement.get("createdAt")
    try:
        created = datetime.fromisoformat(measurement["createdAt"].replace("Z", "+00:00"))
        age = (created - started_at).total_seconds()
        evidence["fresh"] = -5 <= age <= 120
        current = {str(ipaddress.ip_address(item)) for item in addresses}
    except (KeyError, AttributeError, TypeError, ValueError):
        return evidence
    options = measurement.get("measurementOptions")
    if not isinstance(options, dict):
        return evidence
    request = options.get("request")
    if not isinstance(request, dict):
        return evidence
    if (
        not evidence["fresh"] or not current or measurement.get("target") != hostname
        or measurement.get("type") != "http" or measurement.get("status") != "finished"
        or options.get("port") != 443 or options.get("ipVersion", 4) != family
        or request.get("path") != path or request.get("method", "GET") != "GET"
    ):
        return evidence
    seen: set[str] = set()
    results = measurement.get("results")
    if not isinstance(results, list):
        return evidence
    for item in results:
        if not isinstance(item, dict):
            return evidence
        probe = item.get("probe")
        result = item.get("result")
        if not isinstance(probe, dict) or not isinstance(result, dict):
            return evidence
        tls = result.get("tls")
        if not isinstance(tls, dict):
            tls = {}
        address = result.get("resolvedAddress")
        try:
            resolved = ipaddress.ip_address(address)
            address_matches = resolved.version == family and str(resolved) in current
        except (ValueError, TypeError):
            address_matches = False
        passed = (
            result.get("status") == "finished" and result.get("statusCode") == 200
            and tls.get("authorized") is True and address_matches
            and probe.get("country") in countries
        )
        evidence["results"].append({
            "country": probe.get("country"), "city": probe.get("city"),
            "resolved_address": address, "dns_and_family_match": address_matches,
            "status_code": result.get("statusCode"), "tls_authorized": tls.get("authorized"),
            "timings_ms": result.get("timings"), "passed": passed,
            "error": result.get("error"),
        })
        if passed:
            seen.add(probe["country"])
    evidence["passed"] = (
        len(evidence["results"]) == len(countries) and seen == set(countries)
        and all(item["passed"] for item in evidence["results"])
    )
    return evidence


def _resolve_family(hostname: str, family: int) -> list[str]:
    af = socket.AF_INET6 if family == 6 else socket.AF_INET
    return sorted({str(item[4][0]) for item in socket.getaddrinfo(hostname, 443, af, socket.SOCK_STREAM)})


def audit_public_dual_stack_https(
    hostname: str, *, path: str = "/tw-day-trade/", countries: tuple[str, ...] = ("TW", "JP", "SG"),
    timeout_seconds: float = 55, poll_seconds: float = 2,
    resolve_family: Callable[..., list[str]] = _resolve_family,
    request_json: Callable[..., Any] = _globalping_request,
) -> dict[str, Any]:
    if not _HOSTNAME_RE.fullmatch(hostname):
        raise ValueError("invalid hostname")
    if not path.startswith("/") or path.startswith("//") or len(path) > 1024 or any(c in path for c in "\r\n"):
        raise ValueError("invalid request path")
    if not 1 <= len(countries) <= 3 or len(set(countries)) != len(countries) or any(
        not re.fullmatch(r"[A-Z]{2}", country) for country in countries
    ):
        raise ValueError("one to three distinct country codes are required")
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("timeouts must be positive")
    started = datetime.now(UTC)
    deadline = time.monotonic() + timeout_seconds
    families: dict[str, Any] = {}
    ids: dict[int, str] = {}
    api = "https://api.globalping.io/v1/measurements"

    def remaining() -> float:
        return max(0.001, min(15, deadline - time.monotonic()))

    # Start BOTH measurements before polling either; slow IPv6 does not prevent
    # retaining successful IPv4 evidence. Requests force each protocol family.
    for family in (4, 6):
        state = {"passed": False, "dns_addresses": [], "state": "inconclusive", "error": None}
        families[str(family)] = state
        try:
            if time.monotonic() >= deadline:
                raise TimeoutError("audit deadline exceeded before measurement submission")
            state["dns_addresses"] = resolve_family(hostname, family)
            state["initial_dns_addresses"] = list(state["dns_addresses"])
            response = request_json(api, payload={
                "type": "http", "target": hostname,
                "measurementOptions": {"protocol": "HTTPS", "port": 443, "ipVersion": family,
                                       "request": {"path": path, "method": "GET"}},
                "locations": [{"country": country} for country in countries], "limit": len(countries),
            }, timeout_seconds=remaining())
            mid = response.get("id") if isinstance(response, dict) else None
            if not isinstance(mid, str) or not re.fullmatch(r"[a-zA-Z0-9]{1,64}", mid):
                raise RuntimeError("Globalping did not accept a measurement")
            ids[family] = mid
        except Exception as exc:  # DNS/probe-provider failure is not proof of a WAN outage.
            state["error"] = f"{type(exc).__name__}: {exc}"

    pending = dict(ids)
    while pending and time.monotonic() < deadline:
        for family, mid in list(pending.items()):
            state = families[str(family)]
            try:
                if time.monotonic() >= deadline:
                    break
                measurement = request_json(f"{api}/{mid}", timeout_seconds=remaining())
                if not isinstance(measurement, dict) or measurement.get("id") != mid:
                    raise RuntimeError("Globalping returned an unexpected measurement")
                if measurement.get("status") != "finished":
                    continue
                state["dns_addresses"] = resolve_family(hostname, family)
                state["evidence"] = _https_measurement_evidence(
                    measurement, hostname=hostname, family=family,
                    addresses=state["dns_addresses"], path=path, countries=countries, started_at=started,
                )
                state["passed"] = state["evidence"]["passed"]
                state["state"] = "passed" if state["passed"] else "failed_validation"
                state["error"] = None
                del pending[family]
            except Exception as exc:
                state["error"] = f"{type(exc).__name__}: {exc}"
        if pending:
            time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
    passed = all(state["passed"] for state in families.values())
    return {
        "schema_version": 3, "hostname": hostname, "path": path,
        "started_at_utc": started.isoformat(), "completed_at_utc": _utc_now(),
        "provider": "Globalping", "countries": list(countries), "families": families,
        "measurement_ids": {str(family): mid for family, mid in ids.items()},
        "passed": passed, "state": "passed" if passed else "not_verified",
        "evidence_boundary": (
            "Fresh external HTTPS 200 with authorized TLS, requested family, current DNS and all "
            "requested countries is required. Probe acceptance and same-host WSL curl are not WAN "
            "proof. Provider/DNS failures are inconclusive; this audit never changes DDNS or services. "
            "Reachability is proved only at the recorded time and locations, not for every client forever."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("hostname")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/benchmarks/dashboards/public-ipv6-audit.json"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--provider", choices=("internet-nl", "globalping"), default="internet-nl")
    parser.add_argument("--url-path", default="/tw-day-trade/")
    parser.add_argument(
        "--reuse-latest",
        action="store_true",
        help="Poll the latest result without asking Internet.nl to start a fresh probe.",
    )
    args = parser.parse_args()
    if args.provider == "globalping":
        if args.reuse_latest:
            parser.error("Globalping HTTPS acceptance requires a fresh measurement, not --reuse-latest")
        receipt = audit_public_dual_stack_https(
            args.hostname, path=args.url_path, timeout_seconds=args.timeout_seconds, poll_seconds=args.poll_seconds,
        )
    else:
        receipt = audit_public_ipv6(
            args.hostname, start_probe=not args.reuse_latest,
            timeout_seconds=args.timeout_seconds, poll_seconds=args.poll_seconds,
        )
    _write_receipt(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
