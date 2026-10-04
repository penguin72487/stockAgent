#!/usr/bin/env python3
"""Real read-only UI acceptance; source health is not download completeness.

Uses the repository CPU-2D browser profile. Records original API requests,
summary/source parity, indeterminate bars, disclosure/deep-link behavior and
viewport screenshots. --all-providers checks every currently registered label.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, nullcontext
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from urllib.parse import quote, unquote, urlsplit

from playwright.sync_api import sync_playwright

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.probe_browser_runtime import browser_profile_launch_options

PAGES = [
    ("overview", "/", "/api/overview"),
    ("shioaji", "/shioaji/", "/shioaji/api/status"),
    ("finlab", "/finlab/", "/finlab/api/status"),
    ("finmind", "/finmind/", "/finmind/api/status"),
    ("tej", "/tej/", "/tej/api/status"),
    ("openbb", "/openbb/", "/openbb/api/status"),
    ("data-monitor", "/data-monitor/", "/data-monitor/api/summary"),
    ("provider", "/data-monitor/providers/TAIFEX/", "/data-monitor/api/provider"),
    ("traffic", "/traffic/", "/traffic/api/status"),
]
PROFILES = [("desktop", 1366, 768), ("phone", 390, 844), ("compact", 320, 568)]
QUOTA_APIS = {"FinLab":"/finlab/api/status", "FinMind":"/finmind/api/status",
              "永豐 Shioaji":"/shioaji/api/status"}
PROVIDER_EXAMPLES = ["FinLab", "FinMind", "TEJ", "永豐 Shioaji", "OpenBB", "Binance", "MOPS / 公開資訊觀測站"]
READINESS_HEALTH = {"shioaji":"connection-status", "finlab":"finlab-health", "finmind":"finmind-health",
                    "tej":"tej-health", "openbb":"connection-status", "data-monitor":"overall-health"}


SNAPSHOT_SCRIPT = """strict => {
      const bars = [...document.querySelectorAll('.acq-progress-bar')];
      const metrics = [...document.querySelectorAll('.acq-metric-value')];
      const details = [...document.querySelectorAll('main > details.acq-details')];
      const result = {
        title:document.querySelector('h1')?.textContent,
        kind:document.body.dataset.acquisitionPage,
        documentHeight:document.documentElement.scrollHeight,
        summaryTop:document.getElementById('acq-overview')?.getBoundingClientRect().top ?? null,
        overflow:Math.max(0,document.documentElement.scrollWidth-innerWidth),
        metricCount:metrics.length,
        metrics:metrics.map(value => ({source:value.dataset.acqSource,value:value.textContent,
          original:document.getElementById(value.dataset.acqSource)?.textContent.trim(),
          width:value.getBoundingClientRect().width})),
        bars:bars.map(bar => ({source:bar.dataset.acqSource,value:bar.getAttribute('value'),max:bar.getAttribute('max'),
          originalValue:document.getElementById(bar.dataset.acqSource)?.getAttribute('value'),
          originalMax:document.getElementById(bar.dataset.acqSource)?.getAttribute('max') || '1'})),
        details:details.map(node=>({id:node.id,open:node.open})),
        allIdsUnique:new Set([...document.querySelectorAll('[id]')].map(node=>node.id)).size===document.querySelectorAll('[id]').length,
        firstHubLink:document.querySelector('#dashboards > a')?.getAttribute('href') ?? null,
        navigationLinks:[...document.querySelectorAll('.jump-nav a')].map(a=>a.getAttribute('href')),
      };
      const coherent = result.metrics.every(item => item.value === item.original) &&
        result.bars.every(item => item.value === item.originalValue && item.max === item.originalMax);
      return strict && !coherent ? false : result;
    }"""


def verify_case(context, base, kind, path, api, output, screenshot=True, exercise=True, without_io=False):
    page = context.new_page()
    requests, errors, failed = [], [], []
    if without_io:
        page.add_init_script("delete window.IntersectionObserver")
    page.on("request", lambda req: requests.append(req.url))
    page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    page.on("pageerror", lambda err: errors.append(str(err)))
    page.on("response", lambda res: failed.append({"url":res.url,"status":res.status})
            if "/api/" in res.url and res.status >= 400 else None)
    row = {"path":path,"without_intersection_observer":without_io}
    try:
        quota_api = QUOTA_APIS.get(unquote(path.strip("/").rsplit("/", 1)[-1])) if kind == "provider" else None
        with ExitStack() as responses:
            for expected in [api, *([quota_api] if quota_api else [])]:
                responses.enter_context(page.expect_response(
                    lambda res, expected=expected: urlsplit(res.url).path == expected, timeout=30000))
            page.goto(base + path, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_function("() => document.body.dataset.acquisitionPage === 'overview' || document.querySelector('main.acq-mounted')", timeout=10000)
        if kind in READINESS_HEALTH:
            page.locator(f"#{READINESS_HEALTH[kind]}:not(.loading)").wait_for(state="attached", timeout=20000)
        elif kind == "provider":
            page.wait_for_function("() => state.loaded && !state.quotaInFlight", timeout=20000)
        elif kind == "traffic":
            page.wait_for_function("() => document.getElementById('live-status').textContent !== '連線中'", timeout=20000)
        elif kind == "overview":
            page.wait_for_function("() => document.querySelectorAll('#dashboards .health.loading').length === 0", timeout=20000)
        page.evaluate("new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
        if kind != "overview":
            page.wait_for_function("""() => [...document.querySelectorAll('.acq-metric-value')].every(node =>
              node.textContent === document.getElementById(node.dataset.acqSource).textContent.trim())""", timeout=10000)
        # Capture parity and values atomically after the coalesced observer has
        # caught up; a sample spanning the source-write frame isn't a UI defect.
        row.update(page.wait_for_function(SNAPSHOT_SCRIPT, arg=True, timeout=10000).json_value())
        assert row["overflow"] == 0
        assert row["allIdsUnique"]
        if kind == "overview":
            assert row["firstHubLink"] == "data-monitor/"
        else:
            assert row["metricCount"] == 4
            assert row["summaryTop"] < 650
            assert all(not item["open"] for item in row["details"])
            assert all(item["value"] == item["original"] for item in row["metrics"])
            assert all(item["value"] == item["originalValue"] and item["max"] == item["originalMax"] for item in row["bars"])
        initial = [urlsplit(url).path for url in requests if "/api/" in url]
        row["initial_api_paths"] = initial
        if kind == "data-monitor":
            assert "/data-monitor/api/status" not in initial and "/data-monitor/api/details" not in initial
            assert not any("features" in url for url in initial)
        if screenshot:
            page.screenshot(path=str(output / "summary.png"))
        if exercise and kind != "overview":
            summary = page.locator("main > details.acq-details > summary").first
            summary.focus()
            summary.press("Enter")
            assert page.locator("main > details.acq-details[open]").count() == 1
            if screenshot:
                page.screenshot(path=str(output / "details.png"))
            # Deep links into original sections must open all native ancestors.
            target = {"finmind":"pipelines", "finlab":"backfill", "shioaji":"pipelines",
                      "tej":"tables", "openbb":"providers", "provider":"provider-sources",
                      "data-monitor":"source-list", "traffic":"history-title"}[kind]
            with page.expect_response(lambda res: urlsplit(res.url).path == "/data-monitor/api/details", timeout=30000) if kind == "data-monitor" else nullcontext():
                page.evaluate("id => { location.hash=id; }", target)
                page.wait_for_function("id => !document.getElementById(id).closest('details:not([open])')", arg=target, timeout=10000)
            row["deep_link_opened"] = target
            if kind == "data-monitor":
                # Response headers aren't DOM/data readiness. Confirm actual
                # source rows and the completed anchor frame before navigating.
                page.locator("#source-rows tr").first.wait_for(state="attached", timeout=20000)
                page.evaluate("new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
                with page.expect_response(lambda res: "features" in urlsplit(res.url).path, timeout=30000):
                    page.evaluate("() => { StockAgentAcquisition.reveal('feature-list'); document.getElementById('feature-list').scrollIntoView({block:'start',behavior:'instant'}); }")
                page.locator("#feature-rows tr").first.wait_for(state="attached", timeout=20000)
                row["lazy_source_and_features_activated"] = True
        row["console_errors"] = errors
        row["failed_apis"] = failed
        row["external_requests"] = [url for url in requests if urlsplit(url).netloc != urlsplit(base).netloc]
        assert not errors and not failed and not row["external_requests"]
        row["ok"] = True
    except Exception as exc:
        row.update(ok=False, error=f"{type(exc).__name__}: {exc}", console_errors=errors, failed_apis=failed)
    finally:
        row["all_api_paths"] = [urlsplit(url).path for url in requests if "/api/" in url]
        if kind == "data-monitor":
            try:
                row["lazy_state"] = page.evaluate("({details:state.detailsActivated,features:state.featureActivated,inView:state.featureInView})")
            except Exception as error:
                row["diagnostic_error"] = str(error)
        page.close()
    print(json.dumps({"path":path,"ok":row["ok"],"error":row.get("error")}, ensure_ascii=False), flush=True)
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8770")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--all-providers", action="store_true")
    parser.add_argument("--provider-examples", action="store_true")
    parser.add_argument("--profiles", default="desktop,phone,compact")
    parser.add_argument("--cdp-port", type=int)
    parser.add_argument("--responsive-audit", action="store_true")
    parser.add_argument("--engineering", action="store_true", help="Measure hidden detail work and reopen/query behavior.")
    parser.add_argument("--responsive-pages", default="", help="Canonical audit page selection; empty means all pages.")
    args = parser.parse_args()
    base = args.base_url.rstrip("/")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = []
    with sync_playwright() as runtime:
        options = browser_profile_launch_options("cpu-2d")
        if args.cdp_port:
            options["args"] = [*options["args"], f"--remote-debugging-port={args.cdp_port}"]
        browser = runtime.chromium.launch(**options)
        context = browser.new_context(viewport={"width":1366,"height":768})
        response = context.request.get(base + "/data-monitor/api/summary", timeout=30000)
        assert response.ok
        providers = [entry["provider"] for entry in response.json()["provider_summaries"]]
        for name, width, height in PROFILES:
            if name not in args.profiles.split(","): continue
            context.close()
            context = browser.new_context(viewport={"width":width,"height":height}, is_mobile=name != "desktop")
            for kind, path, api in PAGES:
                directory = args.output_dir / name / kind
                directory.mkdir(parents=True, exist_ok=True)
                row = verify_case(context, base, kind, path, api, directory)
                row["profile"] = name
                results.append(row)
            if args.provider_examples:
                for index, provider in enumerate(PROVIDER_EXAMPLES):
                    if provider not in providers: continue
                    directory = args.output_dir / name / f"provider-example-{index}"
                    directory.mkdir(parents=True, exist_ok=True)
                    path = f"/data-monitor/providers/{quote(provider, safe='')}/"
                    row = verify_case(context, base, "provider", path, "/data-monitor/api/provider", directory)
                    row["profile"] = name
                    results.append(row)
        # No-IO progressive fallback is a real compatibility behavior, not a skip.
        directory = args.output_dir / "no-io"
        directory.mkdir(parents=True, exist_ok=True)
        results.append(verify_case(context, base, "data-monitor", "/data-monitor/", "/data-monitor/api/summary", directory, without_io=True))
        if args.all_providers:
            context.close()
            context = browser.new_context(viewport={"width":1366,"height":768})
            for index, provider in enumerate(providers):
                path = f"/data-monitor/providers/{quote(provider, safe='')}/"
                results.append(verify_case(context, base, "provider", path, "/data-monitor/api/provider", args.output_dir,
                                           screenshot=False, exercise=False))
        if args.engineering:
            for kind, path, api in PAGES:
                if kind == "overview": continue
                results.append(verify_engineering_case(context, base, kind, path, api))
            for kind in ["tej", "openbb", "data-monitor", "traffic"]:
                _, path, api = next(entry for entry in PAGES if entry[0] == kind)
                results.append(verify_engineering_case(context, base, kind, path, api, without_io=True))
        audit_code = None
        if args.responsive_audit:
            import subprocess
            assert args.cdp_port
            command = ["node", "scripts/audit_public_dashboards_responsive.mjs", str(args.cdp_port), base,
                       str(args.output_dir / "responsive"), args.responsive_pages]
            audit_code = subprocess.run(command, check=False).returncode
        context.close()
        browser.close()
    report = {"schema_version":1,"generated_at_utc":datetime.now(UTC).isoformat(),"base_url":base,
              "browser_profile":"cpu-2d","registered_provider_labels":providers,"provider_count":len(providers),
              "cases":results,"responsive_audit_exit_code":audit_code,
              "ok":all(row["ok"] for row in results) and audit_code in (None, 0),
              "boundary":"Read-only UI acceptance, not source completeness or GPU/WebGL proof."}
    path = args.output_dir / "acceptance.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"report":str(path),"ok":report["ok"],"cases":len(results),"providers":len(providers)}, ensure_ascii=False))
    return 0 if report["ok"] else 1


def verify_engineering_case(context, base, kind, path, api, *, without_io=False):
    targets = {
        "shioaji": ("pipelines", "#pipeline-grid .pipeline-card:not(.skeleton)", ["pipeline-grid", "traffic-chart", "storage-body", "contracts-body"]),
        "finlab": ("backfill", "#dataset-rows td:not([colspan])", ["pipeline-grid", "dataset-rows", "market-rows", "quota-chart", "quota-history-body"]),
        "finmind": ("backfill", "#dataset-rows td:not([colspan])", ["pipeline-grid", "dataset-rows", "quota-chart", "storage-bars"]),
        "tej": ("tables", "#table-rows td:not([colspan])", ["table-rows", "feature-rows"]),
        "openbb": ("providers", "#provider-body tr", ["chart-lines", "category-grid", "provider-body"]),
        "data-monitor": ("source-list", "#source-rows tr", ["source-rows", "feature-rows", "provider-grid"]),
        "provider": ("provider-sources", "#provider-source-rows tr", ["provider-source-rows"]),
        "traffic": ("traffic-routes", "#route-rows tr", ["chart-lines", "route-rows", "window-rows", "browser-action-rows", "history-chart-lines"]),
    }
    lazy = {"tej":"/tej/api/features", "openbb":"/openbb/api/history",
            "data-monitor":"/data-monitor/api/details", "traffic":"/traffic/api/history"}
    target, selector, heavy_ids = targets[kind]
    page = context.new_page()
    requests, errors = [], []
    if without_io: page.add_init_script("delete window.IntersectionObserver")
    page.add_init_script("""(() => {
          const ids = IDS;
          window.acquisitionDetailWrites = Object.fromEntries(ids.map(id => [id,0]));
          document.addEventListener('DOMContentLoaded', () => {
            for (const id of ids) {
              const target = document.getElementById(id);
              if (target) new MutationObserver(records => {
                acquisitionDetailWrites[id] += records.length;
              }).observe(target, {childList:true, subtree:true, characterData:true});
            }
          }, {once:true});
        })()""".replace("IDS", json.dumps(heavy_ids)))
    page.on("request", lambda req: requests.append(urlsplit(req.url).path) if "/api/" in req.url else None)
    page.on("console", lambda message: errors.append(message.text) if message.type == "error" else None)
    page.on("pageerror", lambda error: errors.append(str(error)))
    row = {"path":path,"kind":kind,"engineering":True,"viewport":page.viewport_size,
           "without_intersection_observer":without_io}
    try:
        with page.expect_response(lambda response:urlsplit(response.url).path == api, timeout=30000):
            page.goto(base + path, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_function("() => document.querySelector('main.acq-mounted')", timeout=10000)
        if kind == "provider":
            page.wait_for_function("() => state.loaded && !state.inFlight", timeout=20000)
        elif kind in READINESS_HEALTH:
            page.locator(f"#{READINESS_HEALTH[kind]}:not(.loading)").wait_for(state="attached", timeout=20000)
        else:
            page.wait_for_function("() => document.getElementById('live-status').textContent !== '連線中'", timeout=20000)
        # Exercise the real refresh entrypoint with real, sanitized status DTOs.
        for _ in range(3): page.evaluate("async () => { await refresh(); }")
        page.evaluate("new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
        row["hidden_detail_writes"] = page.evaluate("acquisitionDetailWrites")
        row["hidden_api_paths"] = list(requests)
        assert all(value == 0 for value in row["hidden_detail_writes"].values())
        if kind in lazy: assert lazy[kind] not in requests
        if kind == "data-monitor": assert "/data-monitor/api/features/page" not in requests
        page.evaluate("id => {StockAgentAcquisition.reveal(id);document.getElementById(id).scrollIntoView({block:'start',behavior:'instant'});}", target)
        page.locator(selector).first.wait_for(state="attached", timeout=20000)
        page.wait_for_function("selector => document.querySelectorAll(selector).length > 0", arg=selector, timeout=20000)
        row["opened_target"] = target
        row["opened_detail_nodes"] = page.locator(selector).count()
        if kind == "data-monitor":
            before = page.locator("#source-rows tr").count()
            page.locator("#load-more").click()
            expanded = page.locator("#source-rows tr").count()
            assert expanded > before
            # Force the changed-generation repaint path without fabricating a
            # source observation or modifying any backend state.
            page.evaluate("async () => {state.heavyRevision=''; await refresh({details:true});}")
            assert page.locator("#source-rows tr").count() == expanded
            row["expanded_source_rows_preserved"] = expanded
        if kind in lazy:
            history_target = {"tej":"features", "openbb":"trend", "data-monitor":"feature-list", "traffic":"traffic-history"}[kind]
            expected = "/data-monitor/api/features/page" if kind == "data-monitor" else lazy[kind]
            with page.expect_response(lambda response:urlsplit(response.url).path == expected, timeout=30000):
                if kind == "tej":
                    page.locator("#table-rows .tej-table-button").first.click()
                    row["table_to_features_navigation"] = True
                else:
                    page.evaluate("id => {StockAgentAcquisition.reveal(id);document.getElementById(id).scrollIntoView({block:'start',behavior:'instant'});}", history_target)
            page.wait_for_function("() => !document.querySelector('[aria-busy=true]')", timeout=20000)
            page.evaluate("() => document.querySelectorAll('main > details.acq-details').forEach(node => {node.open=false;})")
            page.evaluate("new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
            before = requests.count(expected)
            for _ in range(3): page.evaluate("async () => { await refresh(); }")
            row["reclosed_detail_requests"] = requests.count(expected) - before
            assert row["reclosed_detail_requests"] == 0
        assert not errors
        row["ok"] = True
    except Exception as error:
        row.update(ok=False, error=f"{type(error).__name__}: {error}")
    finally:
        row["api_paths"] = list(requests)
        row["page_errors"] = errors
        page.close()
    print(json.dumps({"engineering":kind,"without_io":without_io,"ok":row["ok"],"error":row.get("error")},ensure_ascii=False),flush=True)
    return row


if __name__ == "__main__":
    raise SystemExit(main())
