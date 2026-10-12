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
from urllib.parse import urljoin, urlparse

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
            worker = status.get("execution", {}).get("tick", {})
            assert worker.get("eta_measurement_contract_version") == 2
            expected_basis = ("current_pool_owner_wall_per_completed_stock_day"
                              if worker.get("processing_engine") in {"bounded_range_process_pool", "rolling_range_process_pool"}
                              and worker.get("completed_attempts", 0) > 0
                              else "rolling_mean_end_to_end_attempt_cycle")
            assert worker.get("eta_measurement_basis") == expected_basis
            if worker.get("processing_engine") in {"bounded_range_process_pool", "rolling_range_process_pool"}:
                assert worker.get("kind") == "tw_minute"
                assert 1 <= worker.get("pipeline_workers", 0) <= 32
                assert 1 <= worker.get("download_threads", 1) <= 5
                assert 1 <= worker.get("queue_depth", 1) <= 4
                assert 1 <= worker.get("authorization_slots", worker["pipeline_workers"]) <= worker["pipeline_workers"]
                assert 1 <= worker.get("range_calendar_days", 0) <= 31
                assert worker.get("inflight_partitions", -1) >= 0
            inventory = status.get("history_inventory", {})
            assert inventory.get("state") == "available"
            assert inventory.get("general_total") == len(rows)
            assert sum(inventory.get("mode_counts", {}).values()) == len(rows)
            assert sum(group["total_keys"] for group in inventory.get("groups", [])) == len(rows)
            market = status.get("intraday_market", {})
            assert isinstance(market.get("fresh"), bool)
            if not market["fresh"]:
                # Normal higher-priority source preemption retains historical
                # counts. Accept that UI only with an honest stale clock, not
                # by pretending the acquisition snapshot is still current.
                from stockagent.data.finlab_tick_eta import timestamp
                market_at = timestamp(market.get("observed_at_utc"))
                assert market_at and (datetime.now(UTC)-market_at).total_seconds() > 180
            assert market.get("progress_contract_version") == 1
            assert {phase["id"] for phase in market.get("coverage_phases", [])} == {
                "latest_session", "observed_history", "older_discovery"}
            assert all(phase["finish_at_utc"] is None for phase in market["coverage_phases"])
            tick_progress = status["tick_progress"]
            assert tick_progress["contract_version"] == 1
            assert tick_progress["all_published_history_finish_at_utc"] is None
            assert tick_progress["measurement"]["data_batches"] >= 2
            assert len(tick_progress["phases"]) == 3
            assert all(set(phase["scenarios"]) == {"fast", "reference", "slow"}
                       for phase in tick_progress["phases"])
            assert tick_progress["phases"][2]["estimate_scope"] == "first_pass_candidate_queries"
            assert tick_progress["phases"][2]["remaining_rows_estimate"] is None
            minute_progress = status.get("minute_progress", {})
            minute_first = market.get("acquisition_mode") == "minute_first_tick_residual"
            if minute_first:
                assert len(minute_progress["phases"]) == 3
                assert minute_progress["measurement"]["kind"] == "tw_minute"
                assert tick_progress["measurement"]["kind"] == "tw_tick"
                if not market["minute_priority"]["tick_residual_allowed"]:
                    assert all(row["finish_at_utc"] is None for row in tick_progress["phases"])
            expected_stages = len(workload["stages"]) + bool(minute_progress.get("phases"))
            for name, width in (("desktop", 1440), ("tablet", 1024), ("mobile", 390)):
                page = browser.new_page(viewport={"width": width, "height": 1000}, device_scale_factor=1)
                errors = []
                external_requests = []
                page.on("pageerror", lambda exc: errors.append(type(exc).__name__))
                page.on("request", lambda request: external_requests.append(request.url)
                        if urlparse(request.url).scheme in {"http", "https"}
                        and urlparse(request.url).netloc != urlparse(url).netloc else None)
                response = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                expect(page.locator("#workload-stages article")).to_have_count(expected_stages, timeout=30000)
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
                    minute_pipeline: document.querySelector('#minute-priority-now').textContent,
                    execution_phase: state.latest.execution?.phase,
                    capture_state: document.querySelector('#capture-state').textContent
                })""")
                assert response and response.status == 200
                assert not errors, errors
                assert facts["document_width"] <= width, facts
                assert facts["stage_count"] == expected_stages
                assert len(facts["stage_estimates"]) == expected_stages*3
                assert len(facts["next_wave_estimates"]) >= 3
                assert "來源條件未解決" in facts["stage_estimates"]
                if tick_progress.get("state") == "waiting_higher_priority_minutes":
                    assert facts["stage_estimates"][-3:] == ["先完成分鐘回補"]*3
                else:
                    assert all(re.search(r"20\d\d/\d\d/\d\d", value)
                               for value in facts["stage_estimates"][-3:])
                assert "追新不受此本機門檻限制" in facts["quota_reserve_description"]
                if facts["execution_phase"] == "tick":
                    assert "Tick" in facts["capture_state"]
                if facts["execution_phase"] == "minute":
                    assert "分鐘" in facts["capture_state"]
                for index, states in enumerate(facts["stage_states"]):
                    for scenario, value in enumerate(states):
                        if value == "complete":
                            offset = 1 if minute_progress.get("phases") and workload["stages"][index]["id"] == "tick" else 0
                            assert facts["stage_estimates"][(index+offset)*3+scenario] == "本次已查核"
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
                page.locator('a[href="#acq-catalog"]').click(timeout=10000)
                expect(page.locator("#backfill")).to_be_visible()
                # select_option does not scroll like a physical click. Bring
                # the normal controls/table into view before checking its
                # intentionally deferred DOM, rather than counting a placeholder.
                page.locator("#dataset-filter").scroll_into_view_if_needed(timeout=10000)
                expect(page.locator("#dataset-rows tr").first.locator("td")).to_have_count(14)
                page.locator("#dataset-filter").select_option("incremental")
                expect(page.locator("#dataset-rows tr").first.locator("td").nth(1)).to_contain_text(
                    re.compile("追新|正在抓取"))
                incremental = page.evaluate("""() => ({
                    filtered: state.datasets.filter(row => row.collection_mode?.startsWith('incremental')).length,
                    rendered: document.querySelector('#dataset-result-count').textContent,
                    modes: [...document.querySelectorAll('#dataset-rows tr')].map(row => row.children[1]?.textContent)
                })""")
                assert incremental["filtered"] == sum(count for mode, count in inventory["mode_counts"].items()
                                                      if mode.startswith("incremental"))
                assert all("追新" in mode or "正在抓取" in mode for mode in incremental["modes"])
                page.locator("#dataset-filter").select_option("all")
                page.locator("#dataset-group").select_option("fundamentals")
                page.locator("#dataset-sort").select_option("importance")
                expect(page.locator("#dataset-rows tr").first.locator("td").nth(2)).to_contain_text("營收與財報")
                page.locator("#dataset-group").select_option("all")
                expect(page.locator("#dataset-rows tr").first.locator("td").nth(2)).to_contain_text("1. 行情")
                page.locator("#dataset-filter").select_option("source_wait")
                waiting = page.evaluate("""() => ({
                    count: state.datasets.filter(row => row.collection_mode === 'source_wait').length,
                    rows: [...document.querySelectorAll('#dataset-rows tr')].map(row => row.textContent)
                })""")
                assert waiting["count"] == inventory["mode_counts"].get("source_wait", 0)
                if waiting["count"]:
                    assert all("來源待修復" in row or "正在抓取" in row for row in waiting["rows"])
                page.locator("#dataset-filter").select_option("all")
                page.locator("#dataset-sort").select_option("queue")
                page.locator('a[href="#acq-progress"]').click(timeout=10000)
                # Navigation opens the section then aligns its hash on the
                # next animation frame. Wait for that visible navigation
                # result before scrolling to a nested, deferred Tick panel.
                expect(page.locator("#acq-progress > summary")).to_be_in_viewport()
                page.locator("#market-title").scroll_into_view_if_needed(timeout=10000)
                expect(page.locator("#market-title")).to_be_in_viewport()
                expect(page.locator("#intraday-phases tr")).to_have_count(3)
                expect(page.locator("#intraday-phases .tick-scenarios dd")).to_have_count(9)
                if minute_first:
                    expect(page.locator("#minute-phases tr")).to_have_count(3)
                    expect(page.locator("#minute-phases .tick-scenarios dd")).to_have_count(9)
                    if minute_progress["measurement"]["data_batches"] < 2:
                        expect(page.locator("#market-eta")).to_have_text("完整批次樣本不足")
                    expect(page.locator("#market-basis")).to_contain_text("分鐘回補")
                    if not market["fresh"]:
                        expect(page.locator("#tick-estimation-basis")).to_contain_text("舊快照")
                        assert page.locator("#market-minute-progress").get_attribute("value") is None
                        assert page.locator("#market-tick-progress").get_attribute("value") is None
                    minute_scroll = page.locator(".finlab-tick-progress").first
                    minute_scroll.screenshot(path=str(output / (name+"-minute-progress.png")), timeout=10000)
                    minute_scroll.evaluate("el => { el.scrollLeft = el.scrollWidth; }")
                    expect(page.locator("#minute-phases tr").last.locator("td").last).to_contain_text("每日有界探查")
                    minute_scroll.screenshot(path=str(output / (name+"-minute-eta.png")), timeout=10000)
                    minute_scroll.evaluate("el => { el.scrollLeft = 0; }")
                else:
                    expect(page.locator("#market-eta")).to_contain_text(re.compile(r"20\d\d/\d\d/\d\d"))
                scroll = page.locator(".finlab-tick-progress").last
                scroll.screenshot(path=str(output / (name+"-tick-progress.png")), timeout=10000)
                scroll.evaluate("el => { el.scrollLeft = el.scrollWidth; }")
                expect(page.locator("#intraday-phases tr").last.locator("td").last).to_contain_text("首次查完候選")
                scroll.screenshot(path=str(output / (name+"-tick-eta.png")), timeout=10000)
                scroll.evaluate("el => { el.scrollLeft = 0; }")
                page.locator("#market-search").fill("2330")
                expect(page.locator("#market-rows tr")).to_have_count(2)
                reconciliation = page.evaluate("""() => {
                    const data = state.latest;
                    return data.datasets.filter(row => ['tw_tick:2330', 'tw_minute:2330'].includes(row.key)).map(row => {
                        const market = data.intraday_market.symbols.find(item => item.key === row.key);
                        return {key: row.key, rows_equal: row.rows === market.rows,
                            bounds_equal: row.first === market.first_data_date && row.last === market.last_data_date};
                    });
                }""")
                assert len(reconciliation) == 2
                assert all(row["rows_equal"] and row["bounds_equal"] for row in reconciliation)
                page.evaluate("() => document.activeElement?.blur()")
                page.mouse.move(width-10, 10)  # screenshot the unfocused reading view
                scroll.screenshot(path=str(output / (name+"-intraday.png")), timeout=10000)
                expect(page.locator("#market-tick-count")).to_contain_text("成交列")
                expect(page.locator("#market-minute-count")).to_contain_text("根K棒")
                assert page.evaluate("document.documentElement.scrollWidth") <= width
                assert not errors, errors
                assert not external_requests, external_requests
                facts.update(incremental=incremental, source_wait=waiting, exemplar_reconciliation=reconciliation)
                results.append({"profile": name, "http_status": response.status, "errors": errors, **facts})
                page.close()
        finally:
            browser.close()
    receipt = {"observed_at_utc": datetime.now(UTC).isoformat(), "url": url,
               "profile": "explicit_cpu_2d_no_gpu_webgl_coverage", "accepted": True,
               "quota_policy": policy, "history_inventory": inventory,
               "intraday_progress_contract_version": market["progress_contract_version"],
               "batch_eta_measurement_contract_version": worker["eta_measurement_contract_version"],
               "tick_eta_contract_version": tick_progress["contract_version"],
               "tick_eta_measurement": tick_progress["measurement"],
               "minute_eta_measurement": minute_progress.get("measurement"),
               "acquisition_mode": market.get("acquisition_mode"),
               "market_snapshot_fresh": market.get("fresh"),
               "viewports": results}
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
