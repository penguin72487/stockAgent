#!/usr/bin/env python3
"""Native public-browser acceptance for an added paper mode; no broker orders."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlencode, urljoin, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.probe_browser_runtime import browser_profile_launch_options
from scripts.promote_tw_day_trade_replay import _atomic_json


def _signal_url(base_url, *, market, day):
    # urljoin discards an empty query suffix; add '?' after resolving the path.
    return urljoin(base_url, "api/signals") + "?" + urlencode(
        {"mode": market, "date": day, "offset": 0, "limit": 1})


def _is_cloudflare_beacon(url):
    parsed = urlsplit(url)
    return (parsed.scheme == "https" and parsed.netloc == "static.cloudflareinsights.com"
            and (parsed.path == "/beacon.min.js" or parsed.path.startswith("/beacon.min.js/")))


def _expected_beacon_csp_denial(message):
    match = re.match(r"Loading the script '([^']+)' violates the following Content Security Policy directive:", message)
    return bool(match and _is_cloudflare_beacon(match[1])
                and "script-src 'self'" in message and "The action has been blocked." in message)


def _verify_signal_boundary(payload, *, market, day, private_features=False):
    rows = payload.get("rows") or []
    if (payload.get("simulation_only") is not True
            or payload.get("production_order_possible") is not False
            or int(payload.get("total") or 0) <= 0 or not rows
            or any(row.get("market") != market or row.get("session_date") != day for row in rows)):
        raise ValueError("public signal boundary does not match the requested mode/session")
    research_notice_required = False
    if private_features:
        records = payload.get("feature_drivers_by_signal") or {}
        for row in rows:
            key = "|".join(str(row.get(field) or "") for field in ("session_date", "market", "symbol"))
            if row.get("signal_at"):
                key += "|" + str(row["signal_at"])
            record = records.get(key) or {}
            if record.get("feature_proxy_publication_allowed") is not False or record.get("drivers") != []:
                raise ValueError("private feature publication policy is missing or violated")
            research_notice_required |= (record.get("research_only") is True
                                         and record.get("historical_point_in_time_proven") is False)
    return {"session_date": day, "total": payload["total"], "returned": len(rows),
            "private_feature_policy_verified": private_features,
            "research_notice_required": research_notice_required}


def main():
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://www.penguin72487.dpdns.org/tw-day-trade/")
    parser.add_argument("--market", required=True)
    parser.add_argument("--checkpoint-fingerprint", required=True)
    parser.add_argument("--identity-url", default="http://127.0.0.1:8766/api/status",
                        help="Trusted local runtime proof; public gateways may intentionally redact checkpoint hashes.")
    parser.add_argument("--expected-market", action="append", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--expect-private-features", action="store_true")
    parser.add_argument("--browser-profile", choices=("default", "cpu-2d"), default="default")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    receipt = {"schema_version": 1, "observed_at_utc": datetime.now(UTC).isoformat(),
               "url": args.url, "market": args.market, "status": "running",
               "browser_profile": args.browser_profile,
               "gpu_webgl_verified": False,
               "broker_execution_verified": False, "viewports": []}
    output = args.output_dir / "receipt.json"
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(**browser_profile_launch_options(args.browser_profile))
            try:
                for name, width, height, ratio in (
                    ("laptop_1080", 1366, 768, 1), ("laptop_2k_scaled", 1280, 720, 2),
                    ("desktop_2k", 2560, 1440, 1), ("tablet", 820, 1180, 2),
                    ("phone", 390, 844, 3),
                ):
                    context = browser.new_context(viewport={"width": width, "height": height},
                                                  device_scale_factor=ratio)
                    try:
                        page = context.new_page()
                        errors, aborted, blocked_beacons = [], [], []
                        page.on("pageerror", lambda exc: errors.append(str(exc)))
                        page.on("console", lambda msg: (
                            blocked_beacons if _expected_beacon_csp_denial(msg.text) else errors
                        ).append(msg.text) if msg.type == "error" else None)
                        page.on("requestfailed", lambda req: (
                            blocked_beacons if req.failure == "csp" and _is_cloudflare_beacon(req.url)
                            else aborted if "ERR_ABORTED" in str(req.failure) else errors
                        ).append(f"{req.url}: {req.failure}"))
                        page.on("response", lambda response: errors.append(f"HTTP {response.status}: {response.url}")
                                if response.status >= 400 else None)
                        status_response = context.request.get(urljoin(args.url, "api/status"), timeout=60_000)
                        if not status_response.ok:
                            raise ValueError(f"public status HTTP {status_response.status}")
                        status = status_response.json()
                        modes = {row["market"]: row for row in status.get("modes", [])}
                        if set(modes) != set(args.expected_market):
                            raise ValueError("public enabled-mode set differs")
                        identity_response = context.request.get(args.identity_url, timeout=60_000)
                        if not identity_response.ok:
                            raise ValueError("trusted runtime identity endpoint unavailable")
                        identity = identity_response.json()
                        identity_modes = {row["market"]: row for row in identity.get("modes", [])}
                        if (set(identity_modes) != set(args.expected_market)
                                or identity_modes[args.market].get("checkpoint_fingerprint") != args.checkpoint_fingerprint
                                or identity.get("ledger_integrity", {}).get("ready") is not True):
                            raise ValueError("trusted runtime checkpoint/mode/integrity identity differs")
                        disclosed = modes[args.market].get("checkpoint_fingerprint")
                        if disclosed is not None and disclosed != args.checkpoint_fingerprint:
                            raise ValueError("disclosed public checkpoint identity differs")
                        if not (status.get("simulation_only") is True and status.get("production_order_possible") is False):
                            raise ValueError("not an exclusively paper dashboard")
                        boundaries = []
                        for day in dict.fromkeys((args.start_date, args.end_date)):
                            response = context.request.get(
                                _signal_url(args.url, market=args.market, day=day), timeout=60_000)
                            if not response.ok:
                                raise ValueError(f"public signal boundary HTTP {response.status}")
                            boundaries.append(_verify_signal_boundary(response.json(), market=args.market,
                                day=day, private_features=args.expect_private_features))
                        page.goto(args.url, wait_until="domcontentloaded", timeout=60_000)
                        page.locator(f'#mode-filter option[value="{args.market}"]').wait_for(state="attached", timeout=60_000)
                        if page.locator("#mode-cards article").count() != len(args.expected_market):
                            raise ValueError("rendered mode-card count differs")
                        page.locator("#mode-filter").select_option(args.market)
                        page.locator("#detail-start-date").fill(args.start_date)
                        page.locator("#detail-start-date").press("Tab")
                        page.locator("#detail-end-date").fill(args.end_date)
                        page.locator("#detail-end-date").press("Tab")
                        page.locator("#signal-body tr[data-signal-key]").first.wait_for(timeout=60_000)
                        # Inspect rendered state, never synthesize a click or bypass actionability.
                        page.wait_for_function("""market => {
                            const rows = [...document.querySelectorAll('#signal-body tr[data-signal-key]')];
                            return rows.length > 0 && rows.every(r => r.dataset.signalKey.includes(market));
                        }""", arg=args.market, timeout=60_000)
                        page.locator("#signal-body tr[data-signal-key]").first.click()
                        page.locator("#signal-feature-panel").wait_for(state="visible")
                        if args.expect_private_features:
                            page.wait_for_function("""() => document.querySelector('#signal-feature-empty')
                                ?.textContent.includes('此模式不公開私有原始訓練特徵')""", timeout=60_000)
                        if any(proof["research_notice_required"] for proof in boundaries):
                            page.wait_for_function("""() => document.querySelector('#signal-feature-empty')
                                ?.textContent.includes('歷史資料未證明完整 point-in-time')""", timeout=60_000)
                        page.locator("#chart-legend button[data-series-id]").first.wait_for(timeout=60_000)
                        page.wait_for_function("""() => {
                            const host = document.querySelector('#equity-chart');
                            return host && host.querySelector('canvas') &&
                                   !document.querySelector('#chart-empty')?.checkVisibility();
                        }""", timeout=60_000)
                        button = page.locator("#chart-legend button[data-series-id]").first
                        series_id = button.get_attribute("data-series-id")
                        pressed = button.get_attribute("aria-pressed")
                        button.click()
                        button = page.locator(f'#chart-legend button[data-series-id="{series_id}"]')
                        if button.get_attribute("aria-pressed") == pressed:
                            raise ValueError("legend click did not change curve visibility")
                        button.click()
                        if button.get_attribute("aria-pressed") != pressed:
                            raise ValueError("legend restore failed")
                        dimensions = page.evaluate("""() => ({width: innerWidth,
                            root: document.documentElement.scrollWidth, body: document.body.scrollWidth,
                            chart: document.querySelector('#equity-chart').getBoundingClientRect().toJSON()})""")
                        if max(dimensions["root"], dimensions["body"]) > dimensions["width"] + 1:
                            raise ValueError(f"page horizontal overflow: {dimensions}")
                        page.locator("#equity-chart").scroll_into_view_if_needed()
                        chart_image = args.output_dir / f"{name}_chart.png"
                        page.screenshot(path=str(chart_image))
                        page.locator("#mode-cards").scroll_into_view_if_needed()
                        cards_image = args.output_dir / f"{name}_cards.png"
                        page.screenshot(path=str(cards_image))
                        if errors:
                            raise ValueError(f"browser/runtime errors: {errors[:8]}")
                        receipt["viewports"].append({"name": name, "css_width": width, "height": height,
                            "device_pixel_ratio": ratio, "dimensions": dimensions, "status": "passed",
                            "source_health": status.get("health"), "native_mode_date_signal_legend_interactions": "passed",
                            "checkpoint_identity_verified_at": args.identity_url,
                            "public_checkpoint_disclosed": disclosed is not None,
                            "public_first_last_signal_boundaries": boundaries,
                            "signal_count": page.locator("#signal-count").inner_text(),
                            "blocked_optional_third_party_beacons": blocked_beacons,
                            "intentional_request_aborts": aborted, "screenshots": [str(chart_image), str(cards_image)]})
                        _atomic_json(output, receipt)
                    finally:
                        context.close()
            finally:
                browser.close()
        receipt["status"] = "passed"
        _atomic_json(output, receipt)
        print(json.dumps(receipt, ensure_ascii=False))
    except Exception as exc:
        receipt.update(status="failed", error_type=type(exc).__name__, error=str(exc))
        _atomic_json(output, receipt)
        raise


if __name__ == "__main__":
    main()
