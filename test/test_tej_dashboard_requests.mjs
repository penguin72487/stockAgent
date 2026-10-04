import assert from "node:assert/strict";
import {readFileSync} from "node:fs";
import test from "node:test";
import vm from "node:vm";

function dashboard() {
  const nodes = new Map(), requests = [], rendered = [];
  const node = () => ({
    value:"",textContent:"",children:[],dataset:{},attrs:{},
    append(...children) { this.children.push(...children); },
    replaceChildren(...children) { this.children = children; },
    setAttribute(k,v) { this.attrs[k]=v; },removeAttribute(k) { delete this.attrs[k];if(k==="value")this.value=""; },
  });
  const byId = (id) => {
    if (!nodes.has(id)) nodes.set(id, {
      value: id.endsWith("phase") ? "all" : "", disabled: false, textContent: "previous",
      ...node(),value:id.endsWith("phase")?"all":"",textContent:"previous",addEventListener() {},
    });
    return nodes.get(id);
  };
  const fetchJson = (url, options) => new Promise((resolve, reject) => {
    requests.push({url, options, resolve, reject});
    options.signal.addEventListener("abort", () => reject(new DOMException("cancelled", "AbortError")), {once:true});
  });
  const context = vm.createContext({
    window: {StockAgentDashboard:{byId, createJsonFetcher:() => fetchJson, scheduleRefresh() {},formatBytes:v=>String(v)+" B"}},
    document: {hidden:false,createElement:() => node()}, URLSearchParams, AbortController, Intl, setTimeout, clearTimeout,
  });
  vm.runInContext(readFileSync(new URL("../services/tej_dashboard/app.js", import.meta.url), "utf8"), context);
  // Test the actual request coordinator without making a DOM/rendered-page
  // claim. Browser acceptance separately verifies the real rendering.
  context.record = (data) => {
    rendered.push(data);
    byId("feature-prev").disabled = data.page.offset === 0;
    byId("feature-next").disabled = !data.page.has_more;
  };
  vm.runInContext("renderFeatures = record", context);
  return {context, byId, requests, rendered,
    load:() => vm.runInContext("loadFeatures()", context),
    offset:(value) => vm.runInContext(`state.featureOffset=${value}`, context)};
}

function forecastFixture() {
  return {
    contract:"tej_staged_query_scenarios_v1",execution_state:"not_running",
    global_scenarios:Object.fromEntries(["fast","middle","slow"].map((s,i)=>[s,{
      remaining_seconds:(i+1)*86400,remaining_queries:1000,remaining_work_rows:10000,
      remaining_export_rows:100,remaining_local_bytes:10000,
    }])),known_remaining_queries:10,known_remaining_seconds:{middle:100},
    discovery_remaining_tasks:5,discovery_only_seconds:{middle:20},
    modeled_axis_tables:5,download_timing_samples:20,download_timing_tables:3,
    phases:["P1","P2","P3"].map((p,i)=>({phase:p,candidate_fields:10,dependency_tables:1,
      bundled_into_earlier_tables:i?1:0,undiscovered_dependency_tables:1,parent_phase_barrier:"P1",
      dependency_resolved_grid_rows:5,dependency_exported_rows:2,dependency_recorded_bytes:200,
      scenarios:Object.fromEntries(["fast","middle","slow"].map((s,j)=>[s,{
        additional_queries:i?0:1000,dependency_remaining_work_rows:10000,
        dependency_remaining_export_rows:100,dependency_remaining_local_bytes:10000,
        dependency_remaining_seconds:(j+1)*3600,
        if_started_now_complete_at_utc:"2026-10-03T00:00:00+00:00",
      }])),
    })),assumptions:["有限批次並非持續排程"],
  };
}

function descendantsText(n) { return [n.textContent,...n.children.map(descendantsText)].join(" "); }

test("bundled stages retain nonzero dependency time and conditional dates, not completed or scheduled", () => {
  const d=dashboard();d.context.eta={forecast:forecastFixture()};d.context.work={resolved_grid_rows:5};
  vm.runInContext("renderForecast(eta,work)",d.context);
  assert.match(d.byId("forecast-execution").textContent,/尚未排定.*批次已停止/);
  assert.ok(d.byId("forecast-progress").value>0 && d.byId("forecast-progress").value<1);
  assert.equal(d.byId("phase-rows").children.length,3);
  const p2=d.byId("phase-rows").children[1],p3=d.byId("phase-rows").children[2];
  assert.equal(p2.children[2].textContent,"0");
  assert.match(descendantsText(p2),/較快：.*1.0 小時/);
  assert.match(descendantsText(p2),/已回覆 5／預估 10,005 格點/);
  assert.match(descendantsText(p3),/真正多源校驗工時未知/);
  assert.match(d.byId("forecast-progress-label").textContent,/不是歷史完整率/);
});

test("unknown estimate does not leave an old 100% bar or stale capacity visible", () => {
  const d=dashboard();d.byId("forecast-progress").value=1;d.byId("forecast-total-bytes").textContent="old estimate";
  vm.runInContext("renderForecast({}, {})",d.context);
  assert.equal(d.byId("forecast-progress").value,"");
  assert.equal(d.byId("forecast-total-bytes").textContent,"未知");
  assert.equal(d.byId("phase-rows").children.length,0);
});

test("finite running batch does not imply a full continuously scheduled completion", () => {
  const d=dashboard();d.context.eta={forecast:{...forecastFixture(),execution_state:"finite_batch_running"}};
  vm.runInContext("renderForecast(eta,{})",d.context);
  assert.match(d.byId("forecast-execution").textContent,/有限批次.*未排定/);
});

test("automatic supervision and recovery waits are separate from query activity", () => {
  const d=dashboard();d.context.eta={forecast:{...forecastFixture(),execution_state:"automatic_running"}};
  vm.runInContext("renderForecast(eta,{})",d.context);
  assert.match(d.byId("forecast-execution").textContent,/自動排程持續運行.*非保證完成日/);
  d.context.eta.forecast.execution_state="automatic_waiting_recovery";
  vm.runInContext("renderForecast(eta,{})",d.context);
  assert.match(d.byId("forecast-execution").textContent,/正在等待安全恢復.*尚未排定/);
});

test("same page refresh joins one request and disables pagination until completion", async () => {
  const d = dashboard(), first = d.load();
  assert.equal(d.load(), first);
  assert.equal(d.requests.length, 1);
  assert.equal(d.byId("feature-next").disabled, true);
  assert.equal(d.byId("features").attrs["aria-busy"], "true");
  d.requests[0].resolve({page:{offset:0,has_more:true}});
  await first;
  assert.equal(d.rendered.length, 1);
  assert.equal(d.byId("feature-next").disabled, false);
  assert.equal(d.byId("features").attrs["aria-busy"], "false");
});

test("new selection cancels old request and only the latest page can render", async () => {
  const d = dashboard(), old = d.load();
  d.offset(50);
  const latest = d.load();
  assert.equal(d.requests[0].options.signal.aborted, true);
  assert.match(d.requests[1].url, /offset=50/);
  await old;
  assert.equal(d.byId("features").attrs["aria-busy"], "true");
  assert.equal(d.byId("feature-count").textContent, "previous");
  d.requests[1].resolve({page:{offset:50,has_more:true}});
  await latest;
  assert.equal(d.rendered.length, 1);
  assert.equal(d.rendered[0].page.offset, 50);
});

test("failed metadata read preserves prior rows and permits the next bounded refresh", async () => {
  const d = dashboard(), failed = d.load();
  d.requests[0].reject(new Error("metadata unavailable"));
  await failed;
  assert.equal(d.rendered.length, 0);
  assert.equal(d.byId("feature-prev").disabled, true);
  assert.equal(d.byId("feature-next").disabled, true);
  assert.match(d.byId("feature-count").textContent, /保留上次顯示/);
  const next = d.load();
  assert.equal(d.requests.length, 2);
  d.requests[1].resolve({page:{offset:0,has_more:true}});
  await next;
  assert.equal(d.rendered.length, 1);
});
