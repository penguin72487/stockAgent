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
    request = Request(url, headers={"User-Agent": "StockAgent-public-IPv6-audit/1"})
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        return json.loads(response.read().decode("utf-8"))


def _start_probe(hostname: str, *, timeout_seconds: float) -> None:
    request = Request(
        f"https://internet.nl/site/{hostname}/",
        headers={"User-Agent": "StockAgent-public-IPv6-audit/1"},
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        response.read(1)


def _resolve_ipv6(hostname: str) -> list[str]:
    values = {
        str(item[4][0])
        for item in socket.getaddrinfo(hostname, 443, socket.AF_INET6, socket.SOCK_STREAM)
    }
    return sorted(values)


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
    while True:
        payload = read_json(probe_url, timeout_seconds=min(poll_seconds + 5.0, 30.0))
        if not isinstance(payload, list):
            raise RuntimeError("Internet.nl returned a non-list probe payload")
        probes = [dict(item) for item in payload if isinstance(item, dict)]
        ipv6 = next((item for item in probes if item.get("name") == "ipv6"), None)
        if ipv6 is not None and bool(ipv6.get("done")):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("Internet.nl IPv6 probe did not finish before the deadline")
        time.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))

    ipv6 = next((item for item in probes if item.get("name") == "ipv6"), {})
    passed = bool(addresses) and bool(ipv6.get("done")) and bool(ipv6.get("success"))
    return {
        "schema_version": 1,
        "hostname": hostname,
        "started_at_utc": started_at,
        "completed_at_utc": _utc_now(),
        "dns": {"aaaa": addresses, "has_aaaa": bool(addresses)},
        "external_probe": {
            "provider": "Internet.nl",
            "result_url": f"https://internet.nl/site/{hostname}/",
            "probe_url": probe_url,
            "ipv6": ipv6,
        },
        "passed": passed,
        "evidence_boundary": (
            "Independent external probe plus public DNS; same-host WSL curl is not "
            "used as WAN or router-hairpin evidence."
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
    parser.add_argument(
        "--reuse-latest",
        action="store_true",
        help="Poll the latest result without asking Internet.nl to start a fresh probe.",
    )
    args = parser.parse_args()
    receipt = audit_public_ipv6(
        args.hostname,
        start_probe=not args.reuse_latest,
        timeout_seconds=args.timeout_seconds,
        poll_seconds=args.poll_seconds,
    )
    _write_receipt(args.output, receipt)
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
