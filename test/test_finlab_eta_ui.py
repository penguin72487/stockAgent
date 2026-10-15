"""Existing FinLab UI distinguishes local history reserve and source failures."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def render(function, payload):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node runtime not installed")
    script = r"""
const fs = require('fs'), vm = require('vm'), nodes = new Map();
const element = () => ({textContent: '', children: [],
  replaceChildren() { this.children = []; }, append(...values) { this.children.push(...values); },
  setAttribute() {}, removeAttribute() {}, querySelectorAll() { return []; }});
const byId = id => { if (!nodes.has(id)) nodes.set(id, element()); return nodes.get(id); };
const context = {window: {StockAgentDashboard: {createJsonFetcher: () => () => {}, byId,
  formatAge: () => '已觀測'}}, document: {createElement: element, createElementNS: element,
  querySelectorAll: () => []}};
vm.createContext(context);
const source = fs.readFileSync(process.argv[1], 'utf8').split('function renderStorage')[0];
vm.runInContext(source, context);
context[process.argv[2]](JSON.parse(process.argv[3]));
const snapshot = node => ({text: node.textContent, value: node.value,
  children: node.children.map(snapshot)});
process.stdout.write(JSON.stringify(Object.fromEntries([...nodes].map(([k, v]) => [k, snapshot(v)]))));
"""
    result = subprocess.run([node, "-e", script, str(ROOT/"services/finlab_dashboard/app.js"),
                             function, json.dumps(payload)], check=True, capture_output=True,
                            text=True, timeout=10)
    return json.loads(result.stdout)


def test_zero_history_room_does_not_claim_incremental_refresh_is_quota_blocked():
    view = render("renderQuota", {"quota": {"used_mb": 4970, "limit_mb": 5000, "remaining_mb": 30},
                                 "acquisition": {"quota_reserve_mb": 50},
                                 "workload": {"state": "available", "incremental_quota_policy":
                                              "provider_enforced_no_local_reserve"}})
    assert view["quota-safe-remaining"]["text"] == "0 MB"
    assert view["quota-remaining"]["text"] == "30 MB"
    assert "歷史回補／整表" in view["quota-reserve-basis"]["text"]
    assert "追新不受此本機門檻限制" in view["quota-reserve-basis"]["text"]
    assert "每分鐘取樣" in view["quota-reserve-basis"]["text"]


def test_minute_pipeline_exposes_inflight_without_claiming_all_history_completed():
    view = render("renderTickSummary", {"execution": {"phase": "minute", "intraday": {
        "owner_alive": True, "state": "running", "successful": 69, "attempted": 69,
        "processing_engine": "bounded_range_process_pool", "pipeline_workers": 4,
        "range_calendar_days": 31, "inflight_partitions": 23,
        "estimated_batch_finish_at_utc": "2026-10-10T00:00:00+00:00"}},
        "intraday_market": {"acquisition_mode": "minute_first_tick_residual"}})
    text = view["minute-priority-now"]["text"]
    assert "4 個下載／驗證程序" in text and "授權最多 4 路" in text
    assert "在途 23 個股票日" in text and "非全歷史" in text


def test_rolling_pool_displays_separate_control_concurrency():
    view = render("renderTickSummary", {"execution": {"phase": "minute", "intraday": {
        "owner_alive": True, "state": "running", "successful": 69, "attempted": 69,
        "processing_engine": "rolling_range_process_pool", "pipeline_workers": 32,
        "authorization_slots": 16, "range_calendar_days": 31, "inflight_partitions": 400}},
        "intraday_market": {"acquisition_mode": "minute_first_tick_residual"}})
    text = view["minute-priority-now"]["text"]
    assert "32 個下載／驗證程序" in text and "授權最多 16 路" in text and "空位立即派工" in text


@pytest.mark.parametrize("mode,floor,available", [("normal", 50, "650 MB"), ("pre_reset_drain", 0, "700 MB")])
def test_minute_usable_quota_is_not_reduced_by_tick_floor(mode, floor, available):
    view = render("renderQuota", {"quota": {"used_mb": 4300, "limit_mb": 5000, "remaining_mb": 700},
        "acquisition": {"quota_reserve_mb": 800}, "intraday_quota_policy": {"state": "available",
            "minute": {"mode": mode, "effective_reserve_mb": floor, "drain_before_reset_minutes": 60}}})
    assert view["quota-safe-remaining"]["text"] == available
    assert "每分鐘取樣" in view["quota-reserve-basis"]["text"]
    assert "500" not in view["quota-reserve-basis"]["text"]


def test_completed_stage_remains_done_when_another_stage_has_unresolved_source_errors():
    scenarios = {name: {"state": "blocked"} for name in ("fast", "reference", "slow")}
    done = {name: {"state": "complete"} for name in scenarios}
    view = render("renderWorkload", {"state": "available", "scenarios": scenarios,
                                    "stages": [{"label": "行情", "state": "awaiting_release", "scenarios": done},
                                               {"label": "來源問題", "state": "blocked", "scenarios": scenarios}]})
    assert view["download-global-eta"]["text"] == "來源條件未解決"
    cards = view["workload-stages"]["children"]
    assert [n["text"] for n in cards[0]["children"][4]["children"]][1::2] == ["本次已查核"]*3
    assert [n["text"] for n in cards[1]["children"][4]["children"]][1::2] == ["來源條件未解決"]*3


def test_stale_snapshot_does_not_claim_completed_stage_is_current():
    view = render("renderWorkload", {"state": "stale", "stages": [{"label": "行情", "state": "awaiting_release",
                                        "scenarios": {name: {"state": "complete"} for name in ("fast", "reference", "slow")}}]})
    times = view["workload-stages"]["children"][0]["children"][4]["children"]
    assert [n["text"] for n in times][1::2] == ["快照待更新"]*3


def test_general_table_progress_never_claims_minute_history_is_complete():
    view = render("renderWorkload", {"state": "available", "transfer": {"ratio": 1},
        "actionable_complete": True, "blocked_count": 1})
    assert "一般表" in view["volume-progress-label"]["text"] and "分鐘另列" in view["volume-progress-label"]["text"]
    assert view["download-global-eta"]["text"] == "一般表本輪已查核；另有 1 個來源問題"
