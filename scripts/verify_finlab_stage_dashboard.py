#!/usr/bin/env python3
"""Rendered, read-only FinLab acceptance at desktop/tablet/mobile widths.

The explicit software-compositor profile checks this 2D page, not GPU/WebGL.
No provider API, credentials, frame data, forced click or retry is involved.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.download_finlab_history import _atomic_json  # noqa: E402
from stockagent.data.finlab_acquisition_contract import (  # noqa: E402
    QUOTA_POLICY_VERSION, incremental_quota_exempt, WORKLOAD_CONTRACT_VERSION,
)


def verify(url: str, output: Path) -> dict:
    import re
    from playwright.sync_api import expect, sync_playwright

    output.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, timeout=10000,
                                             args=["--disable-gpu", "--disable-software-rasterizer"])
        try:
            context = browser.new_context()
            try:
                response = context.request.get(urljoin(url, "api/status"), timeout=30000)
                assert response.status == 200
                status = response.json()
            finally:
                context.close()
            workload = status["workload"]
            assert workload["state"] == "available"
            assert workload["contract_version"] == WORKLOAD_CONTRACT_VERSION
            assert workload["quota_policy_version"] == QUOTA_POLICY_VERSION
            assert workload["incremental_quota_policy"] == "provider_enforced_no_local_reserve"
            rows = [row["workload"] for row in status["datasets"] if "workload" in row]
            assert rows
            for row in rows:
                assert row["incremental_quota_exempt"] is incremental_quota_exempt(
                    row["key"], downloaded=row["downloaded"] is True)
            policy = {"contract_version": workload["contract_version"],
                      "quota_policy_version": workload["quota_policy_version"],
                      "incremental_quota_policy": workload["incremental_quota_policy"],
                      "workload_observed_at_utc": workload["generated_at_utc"],
                      "catalog_rows": len(rows),
                      "incremental_quota_exempt_keys": sum(row["incremental_quota_exempt"] for row in rows),
                      "quota_managed_keys": sum(not row["incremental_quota_exempt"] for row in rows)}
            assert "owner" not in status.get("execution", {}).get("tick", {})
            for name, width in (("desktop", 1440), ("tablet", 1024), ("mobile", 390)):
                page = browser.new_page(viewport={"width": width, "height": 1000}, device_scale_factor=1)
                errors = []
                page.on("pageerror", lambda exc: errors.append(type(exc).__name__))
                response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                expect(page.locator("#workload-stages article")).to_have_count(6, timeout=30000)
                expect(page.locator("#volume-progress-label")).to_have_text(re.compile(r"%"), timeout=10000)
                # The shared acquisition layout keeps long sections collapsed.
                # Open them through normal navigation, just as a reader would.
                progress_link = page.locator('a[href="#acq-progress"]')
                if progress_link.count():
                    progress_link.click(timeout=10000)
                    expect(page.locator("#acq-progress")).to_have_attribute("open", "")
                else:
                    page.get_by_role("link", name="流量與完成時間", exact=True).click(timeout=10000)
                facts = page.evaluate("""() => ({
                    viewport: innerWidth,
                    document_width: document.documentElement.scrollWidth,
                    stage_count: document.querySelectorAll('#workload-stages article').length,
                    stage_labels: [...document.querySelectorAll('#workload-stages h4')].map(el => el.textContent),
                    stage_estimates: [...document.querySelectorAll('#workload-stages .finlab-current-eta dd')].map(el => el.textContent),
                    next_wave_estimates: [...document.querySelectorAll('#workload-stages .finlab-next-wave dd')].map(el => el.textContent),
                    stage_states: state.latest.workload.stages.map(stage =>
                        ['fast', 'reference', 'slow'].map(name => stage.scenarios?.[name]?.state)),
                    progress: document.querySelector('#volume-progress-label').textContent,
                    quota: document.querySelector('#quota-observed').textContent,
                    quota_reserve_description: document.querySelector('#quota-reserve-basis').textContent,
                    global_eta: document.querySelector('#download-global-eta').textContent,
                    execution_phase: state.latest.execution?.phase,
                    capture_state: document.querySelector('#capture-state').textContent
                })""")
                assert response and response.status == 200
                assert not errors, errors
                assert facts["document_width"] <= width, facts
                assert facts["stage_count"] == 6
                assert len(facts["stage_estimates"]) == 18
                assert len(facts["next_wave_estimates"]) >= 3
                assert "來源條件未解決" in facts["stage_estimates"]
                assert "缺全市場樣本，未知" in facts["stage_estimates"]
                assert "追新不受此本機門檻限制" in facts["quota_reserve_description"]
                if facts["execution_phase"] == "tick":
                    assert "Tick" in facts["capture_state"]
                for index, states in enumerate(facts["stage_states"]):
                    for scenario, value in enumerate(states):
                        if value == "complete":
                            assert facts["stage_estimates"][index*3+scenario] == "本次已查核"
                page.locator("#workload").screenshot(path=str(output / (name+".png")), timeout=10000)
                quota_link = page.locator('a[href="#acq-quota"]')
                if quota_link.count():
                    quota_link.click(timeout=10000)
                    expect(page.locator("#acq-quota")).to_have_attribute("open", "")
                page.locator("#traffic").screenshot(path=str(output / (name+"-quota.png")), timeout=10000)
                # A tall element screenshot can capture the sticky nav in its
                # middle. Inspect the affected policy card at normal bounds.
                policy_card = page.locator("#traffic .kpi-grid").first.locator(".kpi").nth(2)
                policy_card.evaluate("el => el.scrollIntoView({block: 'center', behavior: 'instant'})")
                policy_card.screenshot(path=str(output / (name+"-quota-policy.png")), timeout=10000)
                results.append({"profile": name, "http_status": response.status, "errors": errors, **facts})
                page.close()
        finally:
            browser.close()
    receipt = {"observed_at_utc": datetime.now(UTC).isoformat(), "url": url,
               "profile": "explicit_cpu_2d_no_gpu_webgl_coverage", "accepted": True,
               "quota_policy": policy, "viewports": results}
    _atomic_json(output / "rendered_acceptance.json", receipt)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://penguin72487.ddnsgeek.com/finlab/")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/data_quality/finlab_stage_eta_2026-10-01/public")
    args = parser.parse_args()
    print(json.dumps(verify(args.url, args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
