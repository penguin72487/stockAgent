#!/usr/bin/env python3
"""Exercise the member storage explorer in real Chromium without GPU work."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from playwright.sync_api import sync_playwright
from scripts.probe_browser_runtime import browser_profile_launch_options
from stockagent.data_sync.storage_monitor import public_status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:8772")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    cases = []
    with urlopen(args.base + "/data-monitor/api/storage", timeout=20) as response:
        actual = json.load(response)
    with sync_playwright() as runtime:
        browser = runtime.chromium.launch(**browser_profile_launch_options("cpu-2d"))
        try:
            for width in (1440, 390):
                page = browser.new_page(viewport={"width": width, "height": 960})
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                with page.expect_response(lambda response: response.url.endswith('/data-monitor/api/storage')) as response:
                    page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                native = response.value.json()
                page.wait_for_function("() => document.querySelectorAll('.storage-disk').length === 6", timeout=30000)
                page.locator("#storage-inventory").scroll_into_view_if_needed()
                assert page.locator("#storage-error").is_hidden()
                assert page.locator(".storage-node").count() == 4
                cases.append({"case": "native_storage_cards", "width": width, "volumes": 6, "nodes": 4})
                wsl = next(volume for node in native['nodes'] for volume in node['volumes']
                           if volume['id'] == 'penguin-wsl')
                group = wsl['classification']
                if type(group.get('roots_completed')) is int and type(group.get('roots_total')) is int:
                    page.locator('[data-storage-select="penguin-wsl"]').click()
                    text = page.locator('#storage-classification-state').inner_text()
                    assert f"已完成 {group['roots_completed']}／{group['roots_total']} 個根目錄" in text
                    cases[-1]['native_WSL_root_coverage'] = {
                        key: group[key] for key in ('roots_completed', 'roots_total', 'state', 'observed_at_utc')}
                    page.locator('#storage-inventory').screenshot(path=args.output / f'WSL-progress-{width}.png')
                button = page.locator('[data-storage-select="vastai1T-disk"]')
                button.click()
                assert button.get_attribute("aria-expanded") == "true"
                page.wait_for_function("() => document.querySelectorAll('#storage-honeycomb button:not(:disabled)').length > 0", timeout=30000)
                before = page.locator("#storage-honeycomb").inner_text()
                page.locator("#storage-honeycomb button:not(:disabled)").first.click()
                assert page.locator("#storage-breadcrumbs button").count() == 2
                assert page.locator("#storage-honeycomb").inner_text() != before
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                page.locator(".storage-tree-details > summary").click()
                page.wait_for_function("() => document.querySelectorAll('#storage-tree details').length > 0", timeout=10000)
                page.locator("#storage-tree details > summary").first.click()
                page.wait_for_function("() => document.querySelector('#storage-tree .storage-tree-children')?.children.length > 0")
                page.locator(".storage-tree-details > summary").click()
                page.locator("#storage-inventory").screenshot(path=args.output / f"storage-{width}.png")
                page.locator("#storage-close").click()
                assert page.locator("#storage-explorer").is_hidden()
                assert button.get_attribute("aria-expanded") == "false"
                page.locator('[data-storage-select="nas-disk"]').click()
                nas = next(volume for node in native["nodes"] for volume in node["volumes"]
                           if volume["id"] == "nas-disk")
                if not nas["classification"]["tree"]:
                    assert "未取得分類" in page.locator("#storage-classification-state").inner_text()
                    assert "尚未取得目錄分類" in page.locator("#storage-direct").inner_text()
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                cases.append({"case": "honeycomb_drill_collapse_and_lazy_tree", "width": width, "horizontal_overflow": False})
                assert not errors, errors
                page.close()
            for mode in ("missing", "stale", "markup", "partial", "scanning", "cold_tiers"):
                data = public_status({}) if mode == "missing" else json.loads(json.dumps(actual))
                if mode == "stale":
                    data["nodes"][0]["volumes"][0]["capacity"]["state"] = "stale"
                if mode == "cold_tiers":
                    data["cold_tiers"] = {"state": "outside_daily_window", "stale": False,
                        "schedule": {"start_hour": 3, "end_hour": 7, "stable_after_hours": 6},
                        "last_work_cycle": {"observed_at_utc": actual["generated_at_utc"],
                            "objects_completed": 2, "logical_bytes_processed": 25644291,
                            "allocation_bytes_saved": 19451139}}
                if mode in {"markup", "partial", "scanning"}:
                    data["nodes"][1]["volumes"][0]["classification"] = {
                        "state": "scanning" if mode == "scanning" else "partial" if mode == "partial" else "complete", "tree": [
                        {"id": "x", "label": "已完成子目錄" if mode in {"partial", "scanning"} else '<img src=x onerror="alert(1)">',
                         "allocated_bytes": 10, "allocated_bytes_is_lower_bound": mode in {"partial", "scanning"},
                         "kind": "source", "children": []}], "allocated_bytes": 10,
                        "allocated_bytes_is_lower_bound": mode in {"partial", "scanning"},
                        "roots_total": 4, "roots_completed": 2}
                page = browser.new_page(viewport={"width": 390, "height": 960})
                page.route("**/data-monitor/api/storage", lambda route, request, payload=data: route.fulfill(json=payload))
                page.goto(args.base + "/data-monitor/#storage-inventory", wait_until="domcontentloaded")
                page.wait_for_function("() => document.querySelectorAll('.storage-disk').length === 6", timeout=30000)
                if mode == "missing":
                    assert page.locator(".storage-size").all_inner_texts()[0].startswith("尚未量測")
                    assert page.locator(".storage-bar-fill").count() == 0
                elif mode == "stale":
                    assert "保留上次實測值" in page.locator('[data-volume="penguin-wsl"]').inner_text()
                elif mode == "markup":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    assert page.locator("#storage-honeycomb img").count() == 0
                    assert "<img" in page.locator("#storage-honeycomb").inner_text()
                elif mode == "partial":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    assert "≥ " in page.locator("#storage-honeycomb").inner_text()
                    assert "未掃描不當作零" in page.locator("#storage-classification-state").inner_text()
                elif mode == "scanning":
                    page.locator('[data-storage-select="vastai1T-disk"]').click()
                    text = page.locator("#storage-classification-state").inner_text()
                    assert "背景清點中" in text and "已完成 2／4 個根目錄" in text
                    assert "未掃描不當作零" in text
                else:
                    text = page.locator("#storage-tiers").inner_text()
                    assert "03:00–07:00" in text and "2 個物件" in text
                    assert "非全庫壓縮率" in text
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                cases.append({"case": mode + "_fixture", "width": 390, "fixture_is_not_live_evidence": True})
                page.close()
        finally:
            browser.close()
    receipt = {"checked_at_utc": datetime.now(timezone.utc).isoformat(), "state": "passed",
        "renderer_profile": "cpu-2d", "cases": cases, "provider_calls": 0}
    (args.output / "browser_acceptance.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(receipt, ensure_ascii=False))


if __name__ == "__main__":
    main()
