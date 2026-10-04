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
    options?.signal?.addEventListener("abort", () => reject(new DOMException("cancelled", "AbortError")), {once:true});
  });
  const context = vm.createContext({
    window: {StockAgentDashboard:{byId, createJsonFetcher:() => fetchJson, scheduleRefresh() {},formatBytes:v=>String(v)+" B",
      isElementVisible:() => context.visible !== false, observeVisibility() {}, createDeferredRenderer:(_target,render) => render}},
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

function featurePage(offset = 0) {
  return {read_only:true, raw_values_exposed:false, features:Array.from({length:50}, (_, i) => ({name:`field-${offset+i}`})),
    page:{offset, limit:50, matched_total:offset+51, has_more:true}};
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
  d.requests[0].resolve(featurePage());
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
  d.requests[1].resolve(featurePage(50));
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
  assert.match(d.byId("feature-count").textContent, /保留同篩選上次資料/);
  const next = d.load();
  assert.equal(d.requests.length, 2);
  d.requests[1].resolve(featurePage());
  await next;
  assert.equal(d.rendered.length, 1);
});

test("live readback is separate from cumulative committed work and clears during recovery wait", () => {
  const d=dashboard();d.context.data={activity:{contract:"tej_download_activity_v1",completed_download_tasks:103,completed_discovery_tasks:63,
    current_task:{table_name:"Source table",phase:"P1",kind:"download",stage:"reading_preview",elapsed_seconds:5,
      readback_scanned_row_slots:2,readback_total_row_slots:4,readback_ratio:.5},
    last_successful_download:{table_name:"Last table",actual_rows:375,actual_bytes:100},blocked_tasks:[]}};
  vm.runInContext("renderActivity(data)",d.context);
  assert.equal(d.byId("active-table").textContent,"Source table");
  assert.equal(d.byId("active-readback").value,.5);
  assert.equal(d.byId("completed-downloads").textContent,"103");
  assert.match(d.byId("active-readback-label").textContent,/尚未驗證入庫.*不計入/);
  d.context.data.activity.current_task=null;
  d.context.data.scheduler={alive:true,state:"waiting_recovery",paused_reason:"source_validation_failed"};
  d.context.data.activity.blocked_tasks_total=1;
  d.context.data.activity.blocked_tasks=[{table_name:"Blocked table",last_error_code:"source_validation_failed"}];
  vm.runInContext("renderActivity(data)",d.context);
  assert.equal(d.byId("active-readback-card").hidden,true);
  assert.equal(d.byId("active-readback").value,"");
  assert.match(d.byId("active-stage").textContent,/等待安全恢復/);
  assert.match(descendantsText(d.byId("blocker-list")),/Blocked table.*目前阻擋新查詢/);
  assert.match(d.byId("last-success-detail").textContent,/Last table.*375/);
});

test("byte and result-row bars use committed actuals plus remaining estimates, not task counts", () => {
  const d=dashboard();d.context.eta={forecast:forecastFixture()};
  d.context.work={exported_rows:100,recorded_bytes:10000,resolved_grid_rows:10000};
  vm.runInContext("renderForecast(eta,work)",d.context);
  assert.equal(d.byId("byte-progress").value,.5);
  assert.equal(d.byId("result-row-progress").value,.5);
  assert.match(d.byId("byte-progress-label").textContent,/10,000 B\/預估 20,000 B|10000 B／預估 20000 B/);
  vm.runInContext("renderForecast({},work)",d.context);
  assert.equal(d.byId("byte-progress").value,"");
  assert.equal(d.byId("result-row-progress").value,"");
  assert.match(d.byId("byte-progress-label").textContent,/預估 未知/);
});

test("table retries show actual next attempt without inventing completion or pausing other tables", () => {
  const d=dashboard();d.context.data={scheduler:{alive:true,state:"executing",paused_reason:null},
    activity:{contract:"tej_download_activity_v1",blocked_tasks_total:0,blocked_tasks:[],
      completed_download_tasks:150,deferred_tasks_total:1,deferred_tasks:[{
        table_name:"Deferred table",error_code:"list_selection_prequery_needs_review",
        consecutive_failures:2,next_attempt_at_utc:"2026-10-03T01:00:00+00:00",retry_after_seconds:120,
      }]}};
  vm.runInContext("renderActivity(data)",d.context);
  assert.match(d.byId("blocker-summary").textContent,/1 個逐表重試/);
  assert.doesNotMatch(d.byId("blocker-summary").textContent,/排程已安全暫停/);
  assert.match(descendantsText(d.byId("blocker-list")),/Deferred table.*120 秒後.*其他表可續抓/);
  assert.equal(d.byId("completed-downloads").textContent,"150");
  d.context.data.activity.deferred_tasks[0].retry_after_seconds=null;
  vm.runInContext("renderActivity(data)",d.context);
  assert.match(descendantsText(d.byId("blocker-list")),/未知 秒後/);
  assert.doesNotMatch(descendantsText(d.byId("blocker-list")),/0 秒後/);
  d.context.data.activity.deferred_tasks[0].retry_after_seconds=0;
  vm.runInContext("renderActivity(data)",d.context);
  assert.match(descendantsText(d.byId("blocker-list")),/下次嘗試最早.*已到期，等待桌面與先行工作/);
});

test("status polling does not wait on features or repeatedly fetch unchanged detail pages", async () => {
  const d=dashboard();d.context.recordStatus=data=>d.context.newStatus=data;
  vm.runInContext("renderStatus = data => {state.latest=data;recordStatus(data);}",d.context);
  const status={activity:{completion_revision:"one"}};
  const refresh=vm.runInContext("refresh()",d.context);
  d.requests[0].resolve(status);
  await refresh;
  assert.equal(d.requests.length,2);
  assert.equal(d.byId("refresh-now").disabled,false);
  d.requests[1].resolve(featurePage());
  await d.load();
  const again=vm.runInContext("refresh()",d.context);
  d.requests[2].resolve(status);await again;
  assert.equal(d.requests.length,3);
  const changed=vm.runInContext("refresh()",d.context);
  d.requests[3].resolve({activity:{completion_revision:"two"}});await changed;
  assert.equal(d.requests.length,5);
  assert.match(d.requests[4].url,/api\/features/);
  d.requests[4].resolve(featurePage());await d.load();
});

test("hidden details issue no feature requests, and changed input rejects a response before debounce", async () => {
  const d = dashboard(); d.context.visible = false;
  d.load(); assert.equal(d.requests.length, 0);
  d.context.visible = true;
  const request = d.load();
  d.byId("feature-search").value = "new scope";
  d.requests[0].resolve(featurePage()); await request;
  assert.equal(d.rendered.length, 0);
});

test("untrusted or mismatched feature envelopes never commit", async () => {
  for (const patch of [{read_only:false}, {raw_values_exposed:true}, {features:[]},
    {page:{offset:50,limit:50,matched_total:100,has_more:true}}]) {
    const d = dashboard(), request = d.load();
    d.requests[0].resolve({...featurePage(), ...patch}); await request;
    assert.equal(d.rendered.length, 0);
    assert.match(d.byId("feature-count").textContent, /暫不可讀/);
  }
});
