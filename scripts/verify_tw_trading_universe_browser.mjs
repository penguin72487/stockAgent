#!/usr/bin/env node
/** Bounded real-browser acceptance of the official universe KPI, not all features. */
import fs from "node:fs";
import path from "node:path";

const port = Number(process.argv[2] || 9244);
const base = (process.argv[3] || "http://127.0.0.1:8770").replace(/\/$/, "");
const output = process.argv[4] || "artifacts/operations/tw_trading_universe_browser";
const width = Number(process.argv[5] || 1366);
const height = Number(process.argv[6] || 768);
const mobile = width < 600;
const expected = await (await fetch(`${base}/tw-day-trade/api/summary`, {signal: AbortSignal.timeout(15000)})).json();
const universe = expected.current_trading_universe;
if (universe?.status !== "ready") throw Error("Official receipt is not ready");
const target = await (await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, {method: "PUT"})).json();
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  socket.addEventListener("open", resolve, {once:true});
  socket.addEventListener("error", reject, {once:true});
});
let sequence = 0;
const pending = new Map();
const errors = [];
socket.addEventListener("message", event => {
  const message = JSON.parse(event.data);
  if (pending.has(message.id)) {
    pending.get(message.id)(message);
    pending.delete(message.id);
  }
  if (message.method === "Runtime.exceptionThrown") errors.push(message.params.exceptionDetails.text);
});
const send = (method, params = {}) => new Promise((resolve, reject) => {
  const id = ++sequence;
  const timer = setTimeout(() => reject(Error(`CDP timeout: ${method}`)), 15000);
  pending.set(id, message => {
    clearTimeout(timer);
    if (message.error) reject(Error(JSON.stringify(message.error)));
    else resolve(message.result);
  });
  socket.send(JSON.stringify({id, method, params}));
});
try {
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Emulation.setDeviceMetricsOverride", {width, height, deviceScaleFactor: mobile ? 3 : 1, mobile});
  await send("Page.bringToFront");
  const started = performance.now();
  await send("Page.navigate", {url:`${base}/tw-day-trade/`});
  let actual;
  const deadline = Date.now() + 20000;
  do {
    const result = await send("Runtime.evaluate", {
      expression: `(() => {
        const item = [...document.querySelectorAll('#operation-kpis .operation-kpi')]
          .find(row => row.querySelector('span')?.textContent === '當前官方可交易標的');
        return {state:document.readyState, value:item?.querySelector('strong')?.textContent,
          note:item?.querySelector('small')?.textContent,
          horizontalOverflow:document.documentElement.scrollWidth>innerWidth,
          viewport:{width:innerWidth,height:innerHeight},
          samples:(window.StockAgentDashboard?.performanceSnapshot?.() || [])
            .filter(row=>row.path.endsWith('/api/status')).slice(-1)};
      })()`, returnByValue:true,
    });
    actual = result.result?.value;
    if (actual?.value) break;
    await new Promise(resolve=>setTimeout(resolve,100));
  } while (Date.now() < deadline);
  const value = `${new Intl.NumberFormat('zh-TW').format(universe.trading_count)} 檔`;
  if (actual?.value !== value || !actual.note?.includes("非當日原始訊號覆蓋率") || actual.horizontalOverflow || errors.length) {
    throw Error(`Official KPI did not match accepted receipt: ${JSON.stringify({actual, value, errors})}`);
  }
  const observedMs = performance.now()-started;
  await send("Runtime.evaluate", {expression:`(() => {
    [...document.querySelectorAll('#operation-kpis .operation-kpi')]
      .find(row=>row.querySelector('span')?.textContent==='當前官方可交易標的')
      ?.scrollIntoView({behavior:'instant',block:'center'});
    return new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));
  })()`,awaitPromise:true});
  const image = await send("Page.captureScreenshot", {format:"png",captureBeyondViewport:false});
  fs.mkdirSync(output,{recursive:true});
  const imagePath = path.join(output,`official-universe-${width}x${height}.png`);
  fs.writeFileSync(imagePath,Buffer.from(image.data,"base64"));
  const report = {status:"accepted_bounded_official_universe_ui",observed_at:new Date().toISOString(),
    base_url:base, official_universe:universe, ...actual, console_errors:errors,
    navigation_to_kpi_observed_ms:observedMs,
    navigation_to_verified_kpi_capture_ms:performance.now()-started, screenshot:imagePath,
    boundary:"Official membership KPI, core status response, paint and viewport only; not full history, positions, signals or broker execution"};
  fs.writeFileSync(path.join(output,`official-universe-${width}x${height}.json`),JSON.stringify(report,null,2)+"\n");
  console.log(JSON.stringify(report));
} finally {
  socket.close();
  await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`,{signal:AbortSignal.timeout(5000)}).catch(()=>{});
}
