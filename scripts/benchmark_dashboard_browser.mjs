#!/usr/bin/env node
// Read-only real-browser acceptance. Optional full-history selection changes
// only this temporary tab, never the engine, receipts, or execution ledger.
import fs from "node:fs";

const port = Number(process.argv[2] || 9224);
const url = process.argv[3] || "https://penguin72487.ddnsgeek.com/tw-day-trade/";
const output = process.argv[4];
const target = await (await fetch(`http://127.0.0.1:${port}/json/new?about:blank`, {method:"PUT"})).json();
const socket = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve) => socket.addEventListener("open", resolve, {once:true}));
let id = 0;
const pending = new Map(), errors = [], messages = [];
socket.addEventListener("message", (event) => {
  const message = JSON.parse(event.data);
  if (message.id) {
    pending.get(message.id)?.(message);
    pending.delete(message.id);
  } else if (message.method === "Runtime.exceptionThrown") errors.push(message.params.exceptionDetails.text);
  else if (message.method === "Network.responseReceived" && message.params.response.status >= 400) errors.push({url:message.params.response.url,status:message.params.response.status});
  else if (message.method === "Network.eventSourceMessageReceived") messages.push({receivedAt:Date.now(),data:JSON.parse(message.params.data)});
});
const send = (method, params = {}) => new Promise((resolve) => {
  pending.set(++id, resolve);
  socket.send(JSON.stringify({id,method,params}));
});
const evaluate = async (expression, awaitPromise = false) => {
  const reply = await send("Runtime.evaluate", {expression, returnByValue:true, awaitPromise});
  if (reply.error || reply.result?.exceptionDetails) throw new Error(JSON.stringify(reply));
  return reply.result.result.value;
};
try {
  for (const name of ["Page", "Runtime", "Network"]) await send(`${name}.enable`);
  await send("Network.setCacheDisabled", {cacheDisabled:true});
  await send("Page.navigate", {url});
  await evaluate(`new Promise((resolve, reject) => {
    const deadline = performance.now() + 60000;
    const check = () => {
      if (typeof signalRows !== 'undefined' && signalRows.length && chartHistoryMatchesSelection() && !historyInFlight) return resolve(true);
      if (performance.now() > deadline) return reject(new Error('initial view deadline'));
      setTimeout(check, 100);
    }; check();
  })`, true);
  const initial = await evaluate(`({
    signals:signalRows.length, points:chartHistory.history.length, revision:lastServiceRevision,
    resources:performance.getEntriesByType('resource').filter(x=>x.name.includes('/api/')).map(x=>({path:x.name.replace(location.origin,''),startMs:x.startTime,endMs:x.responseEnd,durationMs:x.duration})),
    renderMs:(()=>{const t=performance.now();render({heavy:true});return performance.now()-t;})(),
    memoizedChartMs:(()=>{const t=performance.now();renderChart(snapshot);return performance.now()-t;})()
  })`);
  const fullHistory = await evaluate(`new Promise((resolve,reject)=>{
    const select = document.getElementById('detail-start-date');
    const dates = [...availableDetailDates].filter(Boolean).sort();
    select.value = dates[0];
    const started = performance.now();
    select.dispatchEvent(new Event('change'));
    const check = () => {
      if (chartHistoryMatchesSelection() && !historyInFlight) {
        const before = document.getElementById('equity-chart').innerHTML;
        const hot = performance.now();renderChart(snapshot);const memoizedMs=performance.now()-hot;
        renderedChartHistory=null;
        const cold=performance.now();renderChart(snapshot);const rebuildMs=performance.now()-cold;
        return resolve({start:dates[0],points:chartHistory.history.length,loadMs:performance.now()-started,memoizedMs,rebuildMs,identicalSvg:before===document.getElementById('equity-chart').innerHTML,historyError:historyLoadError});
      }
      if (performance.now()-started>60000) return reject(new Error('full history deadline'));
      setTimeout(check,100);
    }; setTimeout(check,250);
  })`, true);
  await new Promise((resolve)=>setTimeout(resolve,3500));
  const final = await evaluate(`({signals:signalRows.length,revision:lastServiceRevision,sourceRevision:snapshot.service_sync.revision_token,hidden:document.hidden,overflow:document.documentElement.scrollWidth-document.documentElement.clientWidth,resources:performance.getEntriesByType('resource').filter(x=>x.name.includes('/api/')&&!x.name.endsWith('/revision')).map(x=>({path:x.name.replace(location.origin,''),startMs:x.startTime,endMs:x.responseEnd,durationMs:x.duration}))})`);
  const result = {url,initial,fullHistory,final,sseMessages:messages.length,errors};
  console.log(JSON.stringify(result,null,2));
  if (output) fs.writeFileSync(output,JSON.stringify(result,null,2)+"\n");
  if (errors.length || !messages.length || !fullHistory.identicalSvg || final.overflow) process.exitCode=1;
} finally {
  socket.close();
  await fetch(`http://127.0.0.1:${port}/json/close/${target.id}`);
}
