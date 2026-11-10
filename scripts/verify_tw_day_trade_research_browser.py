#!/usr/bin/env python3
"""Read-only real-browser acceptance for the public research preview."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.probe_browser_runtime import browser_profile_launch_options


def main() -> None:
    from playwright.sync_api import sync_playwright

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="https://penguin72487.dpdns.org/tw-day-trade/")
    parser.add_argument("--screenshot-dir", type=Path, required=True)
    parser.add_argument("--counterfactual-date")
    parser.add_argument("--counterfactual-count", type=int)
    args = parser.parse_args()
    directory = args.screenshot_dir.resolve(strict=True)
    if not directory.is_dir() or any(directory.iterdir()):
        raise ValueError("screenshots require an existing empty directory")
    results = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(**browser_profile_launch_options("cpu-2d"))
        try:
            for name, width, height in (("laptop", 1366, 768), ("phone", 390, 844)):
                context = browser.new_context(viewport={"width": width, "height": height}, device_scale_factor=1)
                page = context.new_page()
                errors, http_errors = [], []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.on("response", lambda response: http_errors.append({"status": response.status,
                    "path": urlsplit(response.url).path}) if response.status >= 500 else None)
                response = page.goto(args.url, wait_until="domcontentloaded", timeout=30_000)
                page.get_by_text("今年暫定反事實回放（非即時帳本）", exact=True).wait_for(timeout=30_000)
                panel = page.locator("#research-candidates-content")
                # The mobile layout can skip rendering off-screen sections via
                # content-visibility; inspect it after a real-user scroll.
                panel.scroll_into_view_if_needed(timeout=15_000)
                page.wait_for_function(
                    "() => document.getElementById('research-candidates-content')?.innerText.includes('尚未啟用紙上執行')",
                    timeout=15_000,
                )
                text = panel.inner_text()
                checks = {
                    "page_http_200": response is not None and response.status == 200,
                    "paper_disabled": "尚未啟用紙上執行" in text,
                    "uncertainty": "資料可能不完整、結果可能改變" in text,
                    "not_broker_execution": "不是當時已發布、盤中逐版再平衡、交易所或券商成交" in text,
                }
                if not all(checks.values()):
                    page.screenshot(path=str(directory / f"{name}-failure.png"))
                    raise ValueError(f"{name} research preview missing required disclosures: {checks}, panel={text[:1200]!r}")
                signal_count = None
                if args.counterfactual_date:
                    if ("盤後重建訊號（非即時訊號）" not in text or args.counterfactual_date not in text
                            or "不是當時已發布的訊號" not in text or "尚未確認該日分鐘回放與模擬成交" not in text):
                        raise ValueError("counterfactual signal or disclosure is not visible")
                    signal_section = panel.locator("details").filter(
                        has=page.get_by_text("查看全部重建訊號", exact=True))
                    signal_section.locator("summary").click()
                    signal_rows = signal_section.locator("tbody tr")
                    signal_count = signal_rows.count()
                    if args.counterfactual_count is not None and signal_count != args.counterfactual_count:
                        raise ValueError(f"counterfactual signal count differs: {signal_count}")
                    weights = [abs(float(x.strip().rstrip("%"))) for x in signal_rows.locator("td:nth-child(3)").all_inner_texts()]
                    if weights != sorted(weights, reverse=True):
                        raise ValueError("counterfactual signals are not ordered by absolute target")
                page.get_by_text("查看暫定回放每日報酬", exact=True).click()
                table = page.locator("#research-candidates-content details").filter(
                    has=page.get_by_text("查看暫定回放每日報酬", exact=True)).locator("tbody tr")
                count = table.count()
                if count != 203:
                    raise ValueError(f"expected 203 audited daily rows; got {count}")
                page.get_by_text("查看暫定每分鐘資產曲線", exact=True).click()
                minutes = panel.locator("[data-research-minutes]")
                minutes.locator("[data-research-minute-chart] .uplot").wait_for(timeout=30_000)
                minutes.get_by_text("共 270 點", exact=False).wait_for(timeout=30_000)
                layout = panel.evaluate("""el => {
                    const table = el.querySelector('.research-return-table');
                    const box = table.closest('.table-scroll');
                    const next = el.querySelector('[data-research-minutes]');
                    return {overflow: getComputedStyle(box).overflowY,
                            visibleTableBottom: box.getBoundingClientRect().bottom,
                            nextTop: next.getBoundingClientRect().top};
                }""")
                if layout["overflow"] not in {"auto", "scroll"} or layout["visibleTableBottom"] > layout["nextTop"] + 1:
                    raise ValueError(f"daily rows overlap the minute controls: {layout}")
                day = minutes.locator("[data-research-day]")
                if day.locator("option").count() != 203:
                    raise ValueError("minute selector does not cover audited sessions")
                day.select_option(index=0)
                page.wait_for_function("""() => {
                    const p = document.querySelector('[data-research-minutes]');
                    return p?.querySelector('[data-research-day]')?.value === '2026-01-02'
                        && p?.querySelector('[data-research-minute-chart] .uplot')
                        && p?.querySelector('[data-research-minute-status]')?.textContent.includes('共 270 點');
                }""", timeout=30_000)
                dims = page.evaluate("""() => ({viewport: document.documentElement.clientWidth,
                    document: document.documentElement.scrollWidth})""")
                if dims["document"] > dims["viewport"]:
                    raise ValueError(f"horizontal page overflow: {dims}")
                minutes.locator("[data-research-minute-chart]").scroll_into_view_if_needed(timeout=15_000)
                page.screenshot(path=str(directory / f"{name}-minutes.png"), timeout=15_000)
                # Scrolling to a large off-screen section can change preceding
                # content-visibility estimates. Let the layout settle, then
                # capture the actual viewport a person sees, not an old clip.
                for _ in range(3):
                    panel.evaluate("el => el.scrollIntoView({block: 'start', behavior: 'instant'})")
                    page.wait_for_timeout(400)
                    in_view = panel.evaluate("el => { const r = el.getBoundingClientRect(); return r.top >= 0 && r.top < innerHeight; }")
                    if in_view:
                        break
                else:
                    raise ValueError("research panel moved out of the viewport during capture")
                page.screenshot(path=str(directory / f"{name}.png"), timeout=15_000)
                results.append({"viewport": name, "width": width, "daily_rows": count, "minute_points": 270,
                    "counterfactual_signal_rows": signal_count,
                    "first_session_selectable": True,
                    "page_horizontal_overflow": False, "page_errors": errors, "http_5xx": http_errors})
                context.close()
        finally:
            browser.close()
    print(json.dumps({"url": args.url, "results": results, "scope": "research preview only; not live account or complete website acceptance"}, ensure_ascii=False))
    if any(row["page_errors"] for row in results):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
