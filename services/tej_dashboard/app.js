"use strict";
const Dashboard = window.StockAgentDashboard;
const fetchJson = Dashboard.createJsonFetcher({timeoutMs:15000,cache:"no-store",expectedRoot:"object"});
const $ = Dashboard.byId;
const state = {latest:null,statusPending:false,tableOffset:0,featureOffset:0,featureRevision:0,featureRequest:null,tableCatalogKey:null,featureUpdatedAt:0,featureCompletionRevision:null};
const PAGE = 50;
const labels = {running:"正在匯出",backfilling:"歷史回補中",pending_discovery:"待清點歷史軸",query_grid_exported:"格點匯出完成 · 非歷史驗證",query_scope_checked:"查詢範圍完成 · 含來源空回",needs_review:"需檢查",stalled_requires_recovery:"工作中斷 · 待收據恢復",empty_field_menu:"目錄欄位為空 · 待核對",empty_query_axis_unverified:"查詢軸空回 · 待核對",queued:"已排工作 · 未執行"};
const order = {running:0,backfilling:1,pending_discovery:2,query_grid_exported:3,query_scope_checked:3,needs_review:4,stalled_requires_recovery:4,empty_query_axis_unverified:5,empty_field_menu:6};
const errors={vendor_metadata_allocation_failed_deferred:"來源選單配置失敗 · 已隔離，仍待修復",metadata_preparation_failed_deferred:"選單準備失敗 · 已隔離，未冒充清點完成",unknown_outcome_no_auto_retry:"結果未明 · 禁止自動重送",source_validation_failed:"資料鍵／schema 驗證未過 · 原始檔保留",date_input_prequery_needs_review:"日期輸入需檢查 · 未送 Preview",list_selection_prequery_needs_review:"欄位／公司／日期選取需檢查 · 未送 Preview",query_activation_prequery_needs_review:"查詢視窗啟用需檢查 · 未送 Preview",source_binding_prequery_needs_review:"來源選單尚未載入 · 自動修復，未送 Preview",query_preparation_prequery_needs_review:"查詢準備失敗 · 有界重試，未送 Preview",preview_column_limit_repartition_required:"預覽超過 30 欄 · 已拒絕，待安全分片",source_key_layout_replan_required:"來源鍵格式不符 · 未送 Preview，待保留粒度重排"};
const waitLabels={waiting_recovery:"等待安全恢復",recovering_response:"自動核對原查詢結果 · 不重送",replaying_authorized:"依使用者授權核對／重排 · 保留原紀錄",waiting_storage:"等待磁碟空間",waiting_owner:"等待桌面操作鎖",waiting_desktop:"等待互動桌面",waiting_metadata:"等待本機中繼資料寫入",waiting_local_retry:"等待本機輸入重試",waiting_queue:"待命 · 暫無到期工作",between_tasks:"工作間隔",executing:"正在核對工作",starting:"自動排程啟動中"};
const stageLabels={awaiting_progress:"工作執行中 · 等待步驟回報",preparing_scope:"核對並準備查詢範圍",awaiting_preview:"等待來源回覆",reading_preview:"讀回來源資料格",response_saved:"原始回覆已保存 · 待驗證",validating_and_saving:"驗證與寫入資料／收據"};
errors.source_validation_failed_deferred="來源結果驗證未過 · 此表隔離，其他表可續抓；原始檔保留";
labels.waiting_local_retry="此表等待重試 · 其他表可續抓";
order.waiting_local_retry=2;
errors.prequery_failure_deferred="已證明未送查詢 · 此表排定重試";
errors.unknown_outcome_no_auto_retry="原查詢結果未明 · 核對後依授權重排";
const int = new Intl.NumberFormat("zh-TW",{maximumFractionDigits:0});
function num(v){return v===null||v===undefined||v===""?null:Number.isFinite(Number(v))?Number(v):null;}
function count(v){return num(v)===null?"未知":int.format(v);}
function bytes(v){return num(v)===null?"未知":Dashboard.formatBytes(v,{maximumFractionDigits:1});}
function time(v){const d=new Date(v||"");return Number.isNaN(d.getTime())?"未核實":d.toLocaleString("zh-TW",{timeZone:"Asia/Taipei",hour12:false});}
function scenarioDate(v){const d=new Date(v||"");return Number.isNaN(d.getTime())?"尚無估算":`約 ${d.toLocaleDateString("zh-TW",{timeZone:"Asia/Taipei"})}`;}
function percentage(v){return v===null||!Number.isFinite(v)?"未知":v>0&&v<.0001?"低於 0.01%":`${(v*100).toFixed(2)}%`;}
function duration(v){return num(v)===null?"未知":v===0?"無新增工時":v>=365.25*86400?`約 ${(v/(365.25*86400)).toFixed(1)} 年`:v>=86400?`約 ${(v/86400).toFixed(1)} 天`:v>=3600?`約 ${(v/3600).toFixed(1)} 小時`:`約 ${Math.ceil(v/60)} 分鐘`;}
function text(id,v){$(id).textContent=v;}
function el(tag,v,cls){const e=document.createElement(tag);if(v!==undefined)e.textContent=v;if(cls)e.className=cls;return e;}
function cell(tr,main,detail){const td=el("td",main);if(detail)td.append(el("small",detail));tr.append(td);return td;}
function scenarioCell(tr,scenarios,key,format){
  const td=el("td"),stack=el("div",undefined,"tej-scenario-stack");
  for(const [id,label] of [["fast","較快"],["middle","中間"],["slow","較慢"]]){
    const line=el("span",`${label}：${format(scenarios?.[id]?.[key])}`,`tej-scenario ${id}`);stack.append(line);
  }
  td.append(stack);
  tr.append(td);return td;
}
function renderActivity(data){
  const a=data.activity||{},current=a.current_task,scheduler=data.scheduler||{},last=a.last_successful_download;
  const available=a.contract==="tej_download_activity_v1";
  text("active-table",current?.table_name||(available?"目前沒有正在下載的資料表":"下載現況暫不可讀"));
  text("active-task",current?`${current.phase} · ${current.kind==="discover"?"歷史軸清點":"歷史回補"} · 本次工作 ${count(current.query_work_rows)} 格點（非預估結果列數）`:scheduler.alive?`${waitLabels[scheduler.state]||"狀態待核實"}${errors[scheduler.paused_reason]?` · ${errors[scheduler.paused_reason]}`:""}`:"下載器未運行；既有資料與收據保留");
  text("active-stage",current?stageLabels[current.stage]||"步驟尚未回報":scheduler.alive?waitLabels[scheduler.state]||"狀態待核實":"未執行");
  text("active-elapsed",current?`本次已耗 ${count(num(current.elapsed_seconds)===null?null:Math.floor(current.elapsed_seconds))} 秒 · ${current.progress_stale?"步驟回報較舊；不延長工作截止時間 · ":""}步驟觀測 ${time(current.progress_observed_at_utc||current.started_at_utc)}`:scheduler.next_check_at_utc?`下次本機檢查 ${time(scheduler.next_check_at_utc)}（不是重新送查詢）`:"尚未排定新的查詢時間");
  text("completed-downloads",count(a.completed_download_tasks));
  text("completed-discoveries",`累計清點成功 ${count(a.completed_discovery_tasks)} 次；成功下載包含明確來源空回，不等於全歷史完成`);
  text("last-success",last?time(last.completed_at_utc):"尚無成功下載收據");
  text("last-success-detail",last?`${last.table_name} · ${count(last.actual_rows)} 分片列／${bytes(last.actual_bytes)}`:"不以排程 heartbeat 或批次結束冒充下載成功");
  const ratio=num(current?.readback_ratio);
  $("active-readback-card").hidden=!current||ratio===null;
  if(ratio===null)$("active-readback").removeAttribute("value");else $("active-readback").value=Math.min(1,Math.max(0,ratio));
  text("active-readback-label",current?`讀回 ${count(current.readback_scanned_row_slots)}／${count(current.readback_total_row_slots)} 個來源列槽（含空列） · ${percentage(ratio)}；尚未驗證入庫，不計入累計完成量。`:"目前沒有來源讀回工作");
  const blockers=a.blocked_tasks||[],body=$("blocker-list");body.replaceChildren();
  const deferred=a.deferred_tasks||[],deferredCount=a.deferred_tasks_total||0;
  $("activity-blockers").hidden=!(a.blocked_tasks_total>0||deferredCount>0||scheduler.paused_reason);
  text("blocker-summary",`${count(a.blocked_tasks_total)} 個工作需檢查 · ${count(deferredCount)} 個逐表重試${scheduler.paused_reason?" · 排程已安全暫停":""}`);
  for(const task of blockers){const li=el("li",task.table_name);li.append(el("small",`${errors[task.last_error_code]||"需要核對的來源／本機狀態"}${task.last_error_code===scheduler.paused_reason?" · 目前阻擋新查詢":""} · 最後嘗試 ${time(task.attempted_at_utc)}`));body.append(li);}
  if(a.blocked_tasks_total>blockers.length)body.append(el("li",`另有 ${count(a.blocked_tasks_total-blockers.length)} 個工作；請至逐表清單查看。`));
  for(const task of deferred){const li=el("li",task.table_name),seconds=num(task.retry_after_seconds);
    const waiting=seconds===0?"已到期，等待桌面與先行工作":`${count(seconds===null?null:Math.ceil(seconds))} 秒後`;
    li.append(el("small",`${errors[task.error_code]||"查詢前本機失敗"} · 第 ${count(task.consecutive_failures)} 次退避 · 下次嘗試最早 ${time(task.next_attempt_at_utc)}（${waiting}）；不是已下載完成，其他表可續抓。`));body.append(li);}
  if(deferredCount>deferred.length)body.append(el("li",`另有 ${count(deferredCount-deferred.length)} 個逐表重試；請至逐表清單查看。`));
  if(!available){text("active-task","等待可驗證的本機工作狀態；不將未知當作停止或完成");text("active-stage","未知");}
}
function volumeProgress(id,labelId,actual,remaining,format,label){
  const done=num(actual),left=num(remaining),total=done===null||left===null?null:done+left;
  const ratio=total!==null&&total>0?done/total:null;
  if(ratio===null)$(id).removeAttribute("value");else $(id).value=Math.min(1,Math.max(0,ratio));
  text(labelId,`${label}：${format(done)}／預估 ${format(total)} · ${percentage(ratio)}（中間情境；不是歷史完整率）`);
}
function renderForecast(eta,w){
  const f=eta?.forecast,body=$("phase-rows");body.replaceChildren();
  if(f?.contract!=="tej_staged_query_scenarios_v1"){
    text("global-eta",duration(eta?.global_remaining_seconds));text("global-eta-basis",eta?.reason||"估時計算暫不可讀");
    text("forecast-execution","分階段情境尚未提供；不把未知當成完成");
    for(const id of ["forecast-known","forecast-discovery","forecast-queries","forecast-total-bytes","forecast-total-rows"])text(id,"未知");
    text("forecast-model","估算資料暫不可讀；未採用上次的數字");
    text("forecast-storage","本機容量餘裕待觀測");text("forecast-assumptions","估算依據暫不可讀");
    $("forecast-progress").removeAttribute("value");
    volumeProgress("result-row-progress","result-row-progress-label",w.exported_rows,null,count,"已入庫分片列");
    volumeProgress("byte-progress","byte-progress-label",w.recorded_bytes,null,bytes,"收據本機容量");
    text("forecast-progress-label","分母未知");return;
  }
  const scenario=f.global_scenarios||{},middle=scenario.middle||{};
  volumeProgress("result-row-progress","result-row-progress-label",w.exported_rows,middle.remaining_export_rows,count,"已入庫分片列");
  volumeProgress("byte-progress","byte-progress-label",w.recorded_bytes,middle.remaining_local_bytes,bytes,"收據本機容量");
  text("global-eta",duration(middle.remaining_seconds));
  text("global-eta-basis",`下載較快 ${duration(scenario.fast?.remaining_seconds)}／較慢 ${duration(scenario.slow?.remaining_seconds)}；連續 24 小時情境，不含真正的多源校驗。`);
  text("forecast-execution",f.execution_state==="automatic_running"?"自動排程持續運行；下列日期仍是假設無故障／配額等待的情境，非保證完成日。":f.execution_state==="automatic_waiting_recovery"?"自動排程存活，但正在等待安全恢復；修復等待時間未知，尚未排定完成日。":f.execution_state==="automatic_waiting"?"自動排程存活，但正在等待桌面、磁碟或到期工作，不代表正在下載；等待時間未知，尚未排定完成日。":f.execution_state==="not_running"?"實際完成時間尚未排定：批次已停止。下列日期假設從估算時點連續執行。":"有限批次正在執行；未排定全程持續執行，下列日期仍是連續執行情境。");
  text("forecast-known",`${count(f.known_remaining_queries)} 次已核實剩餘查詢 · 中間情境 ${duration(f.known_remaining_seconds?.middle)}`);
  text("forecast-discovery",`${count(f.discovery_remaining_tasks)} 張待清點（含失敗） · 若只清點約 ${duration(f.discovery_only_seconds?.middle)}`);
  text("forecast-queries",count(middle.remaining_queries));
  text("forecast-model",`${count(f.modeled_axis_tables)} 張歷史軸待核實 · ${count(f.download_timing_samples)} 次完整下載／${count(f.download_timing_tables)} 張表實測 · 截止 ${f.history_cutoff||"未知"}`);
  text("forecast-total-bytes",num(middle.remaining_local_bytes)===null?"未知":bytes((w.recorded_bytes||0)+middle.remaining_local_bytes));
  text("forecast-total-rows",num(middle.remaining_export_rows)===null?"未知":count((w.exported_rows||0)+middle.remaining_export_rows));
  const storage=f.local_storage||{};
  text("forecast-storage",`本機可用 ${bytes(storage.free_bytes)}；保留 ${bytes(storage.minimum_reserve_bytes)} 後可下載 ${bytes(storage.available_for_download_bytes)}。${storage.middle_scenario_fits_current_budget===false?"中間情境超出目前容量，需增加空間；不會自動刪資料。":"樣本外推可能低估，實際持續按磁碟餘裕停抓；不保證全量放得下。"}`);
  const denominator=num(middle.remaining_work_rows),ratio=denominator===null?null:(w.resolved_grid_rows||0)/((w.resolved_grid_rows||0)+denominator);
  if(ratio===null||!Number.isFinite(ratio))$("forecast-progress").removeAttribute("value");else $("forecast-progress").value=Math.min(1,Math.max(0,ratio));
  const percent=percentage(ratio);
  text("forecast-progress-label",`${percent} · 已回覆 ${count(w.resolved_grid_rows)}／預估 ${denominator===null?"未知":count((w.resolved_grid_rows||0)+denominator)} 個查詢工作格點（含欄位分片）；不是歷史完整率`);
  text("forecast-assumptions",f.assumptions?.join("；")||"估算依據暫不可讀");
  for(const p of f.phases||[]){
    const tr=el("tr");tr.dataset.phase=p.phase;
    cell(tr,p.phase,p.phase==="P1"?"新增特徵資料":p.phase==="P2"?"補齊歷史／口徑":"取得校驗候選；校驗本身尚未排定");
    const dependency=cell(tr,`${count(p.candidate_fields)} 欄／${count(p.dependency_tables)} 張依賴表`,`${count(p.bundled_into_earlier_tables)} 張併入前階段，不重複下載；${count(p.undiscovered_dependency_tables)} 張待清點`);
    const work=num(p.scenarios?.middle?.dependency_remaining_work_rows),resolved=num(p.dependency_resolved_grid_rows),progress=el("progress");
    progress.max=1;progress.className="progress-track tej-stage-progress";progress.setAttribute("aria-label",`${p.phase} 共用表查詢工作進度（預估），非特徵歷史完整率`);
    const fraction=work!==null&&resolved!==null&&work+resolved>0?Math.min(1,Math.max(0,resolved/(work+resolved))):null;
    if(fraction!==null)progress.value=fraction;
    dependency.append(progress);
    dependency.append(el("small",`${percentage(fraction)} · 已回覆 ${count(resolved)}／預估 ${fraction===null?"未知":count(work+resolved)} 格點；非歷史完整率`));
    cell(tr,count(p.scenarios?.middle?.additional_queries),`此階段新增查詢；完成仍依賴 ${p.parent_phase_barrier}`);
    cell(tr,count(p.dependency_exported_rows),`預估總分片列 ${num(p.scenarios?.middle?.dependency_remaining_export_rows)===null?"未知":count(p.dependency_exported_rows+p.scenarios.middle.dependency_remaining_export_rows)}；稀疏樣本外推`);
    cell(tr,bytes(p.dependency_recorded_bytes),`預估總容量 ${num(p.scenarios?.middle?.dependency_remaining_local_bytes)===null?"未知":bytes(p.dependency_recorded_bytes+p.scenarios.middle.dependency_remaining_local_bytes)}`);
    scenarioCell(tr,p.scenarios,"dependency_remaining_seconds",duration);
    scenarioCell(tr,p.scenarios,"if_started_now_complete_at_utc",scenarioDate);
    cell(tr,p.blocked_dependency_tasks||p.deferred_dependency_tasks?`${count(p.blocked_dependency_tasks||0)} 個待修復 · ${count(p.deferred_dependency_tasks||0)} 個等待重試`:"無來源故障工作",p.phase==="P3"?"只是下載依賴日期；真正多源校驗工時未知":"日期是假設故障已修復、沒有配額或桌面等待；非已排定");
    body.append(tr);
  }
}
function renderStatus(data){
  if(data.read_only!==true||data.raw_values_exposed!==false)throw new Error("invalid metadata contract");
  state.latest=data;const c=data.catalog||{},w=data.workload||{},q=data.quota||{},traffic=data.traffic||{};
  renderActivity(data);
  text("tej-health",({running:"桌面回補進行中",needs_review:"下載需檢查",paused_for_storage:"磁碟餘裕不足 · 已停抓",queued:"已排工作 · 待執行",not_registered:"清冊未註冊",metadata_unavailable:"本機中繼資料不可讀",query_grid_exported:"已匯出目前格點",query_scope_checked:"查詢範圍完成 · 含來源空回"})[data.state]||"狀態待核實");
  if(data.state==="running")text("tej-health",data.worker?.kind==="discover"?"歷史範圍清點中":"歷史資料下載中");
  if(data.state==="running"&&data.worker?.state==="between_tasks")text("tej-health","批次執行中 · 工作間隔");
  if(data.state==="running"&&data.worker?.state==="waiting_local_retry")text("tej-health","本機輸入重試等待 · 未送 Preview");
  if(data.state==="running"&&data.worker?.state==="recovering_metadata")text("tej-health","安全核對選單失敗 · 未重送資料查詢");
  if(data.worker?.state==="desktop_interface_recovery_required")text("tej-health","桌面查詢介面停用 · 等待安全恢復");
  if(data.worker?.state==="batch_finished"&&["queued","needs_review"].includes(data.state))text("tej-health",data.state==="needs_review"?"批次已結束 · 仍有工作需檢查":"有限批次已結束 · 仍有待抓歷史");
  const scheduler=data.scheduler||{};
  if(scheduler.alive&&!data.worker?.alive)text("tej-health",({starting:"自動排程啟動中",executing:"自動排程正在核對工作",between_tasks:"自動排程運行中 · 工作間隔",waiting_queue:"自動排程待命 · 暫無到期工作",waiting_storage:"自動排程等待磁碟空間",waiting_owner:"自動排程等待桌面操作鎖",waiting_desktop:"自動排程等待互動桌面 · 未送 Preview",waiting_metadata:"自動排程等待本機中繼資料寫入",waiting_local_retry:"自動排程等待本機輸入重試",waiting_recovery:"自動排程等待安全恢復 · 未送新查詢",recovering_response:"自動修復中 · 核對原查詢結果，未重送"})[scheduler.state]||"自動排程存活 · 狀態待核對");
  if(scheduler.alive&&!data.worker?.alive&&scheduler.state==="replaying_authorized")text("tej-health","依使用者授權核對／重排 · 保留原紀錄");
  $("tej-health").className=`status ${data.state==="running"?"updating":data.state==="needs_review"?"degraded":"waiting"}`;
  text("tej-freshness",`面板 ${time(data.observed_at_utc)} · 工作觀測 ${time(data.worker?.observed_at_utc)}（臺北）`);
  if(data.worker?.state==="batch_finished")text("tej-freshness",`面板 ${time(data.observed_at_utc)} · 批次結束 ${time(data.worker.observed_at_utc)} · 本批嘗試 ${count(data.worker.attempted_tasks)} 個工作；不代表全歷史完成（臺北）`);
  text("tej-automation",scheduler.alive?`自動排程存活 · heartbeat ${time(scheduler.observed_at_utc)}${scheduler.next_check_at_utc?` · 下次檢查 ${time(scheduler.next_check_at_utc)}`:""}（臺北）；排程存活不等於正在下載。`:"自動排程未運行；已下載來源與收據保留。");
  const startup=data.desktop_startup||{};
  if(startup.enabled){
    const startupLabels={desktop_ready:startup.ready?"桌面已核對可用":"桌面或互動通道尚待重新核對",interactive_session_required:"等待 Windows 使用者登入",interactive_windows_transport_unavailable:"等待互動 Windows 通道恢復",desktop_writer_active:"保留目前下載操作",owned_query_needs_reconciliation:"等待原查詢結果恢復",retired_addin_still_present:"舊外掛仍在，未另開查詢",startup_launcher_outcome_unresolved:"查詢視窗開啟結果待核對，未重複開啟",desktop_restart_cooldown:"短時間已恢復三次；保留原查詢，稍後再核對",tej_addin_not_ready:"等待 Excel／TEJ 外掛就緒",desktop_waiting_login_or_addin:"等待 TEJ 登入／外掛就緒"};
    $("tej-automation").textContent+=` 開機恢復已設定：Windows 登入後每 ${count(startup.check_interval_seconds)} 秒核對 · ${startupLabels[startup.state]||"專用桌面恢復狀態待核對"} · 觀測 ${time(startup.observed_at_utc)}（臺北）。`;
  }
  const replay=scheduler.authorized_unknown_replay||{};
  if(replay.enabled){
    const reasons={authorized_replay_cooldown:"重排冷卻中",authorized_replay_task_budget:"此筆重排額度等待",authorized_replay_global_budget:"全域重排額度等待",authorized_replay_context_unverified:"介面／原範圍尚未可驗證",checking_authorized_replay:"核對授權重排條件"};
    $("tej-automation").textContent+=` 已授權有界自動重排：每筆滾動一小時最多 ${count(replay.max_replays_per_task_hour)} 次、全域 ${count(replay.max_replays_per_hour)} 次（本機恢復上限，非官方配額）；${reasons[replay.state]||"先核對原結果"}${replay.next_check_at_utc?` · 重排條件下次到期 ${time(replay.next_check_at_utc)}（臺北）`:""}。`;
  }
  const observed=new Date(data.observed_at_utc||""),age=Date.now()-observed.getTime();
  if(!Number.isFinite(age)||age>30000){text("tej-health","觀測已過期 · 不代表目前正在下載");$("tej-health").className="status degraded";$("active-readback-card").hidden=true;}
  text("activity-sync",`每 5 秒原位更新 · 最新資料觀測 ${time(data.observed_at_utc)} · ${age>30000?"觀測已過期，等待新狀態":"切回此頁立即同步"}`);
  text("catalog-types",count(c.types_scanned));text("catalog-verified",c.catalog_scan_complete?"全部分類有明確巡檢完成收據":"沒有完整巡檢證據");
  text("catalog-tables",count(c.tables));text("catalog-empty",`含 ${count(c.empty_field_menus)} 張目前無可見欄位的表`);
  text("catalog-fields",count(c.fields));text("fields-obtained",count(c.fields_with_non_null_exports));
  text("exported-rows",count(w.exported_rows));text("exported-cells",count(w.exported_non_null_cells));text("recorded-bytes",bytes(w.recorded_bytes));
  const legacy=data.legacy_exports||{};
  $("legacy-exports").hidden=!(legacy.completed_download_tasks>0);
  if(!$("legacy-exports").hidden)text("legacy-exports",`另保留舊範圍的 ${count(legacy.exported_rows)} 列／${count(legacy.exported_non_null_cells)} 個非空值（${bytes(legacy.recorded_bytes)}）。其公司／日期範圍需重驗，未算入本版完成率；原始檔與收據未刪除。`);
  text("total-rows",count(w.total_rows));text("discovery-progress",`${count(w.discovered_tables)} 張表已有查詢軸；已知 ${count(w.known_grid_rows)} 個格點，尚未涵蓋全部表`);
  const fraction=num(w.global_row_ratio);if(fraction===null)$("row-progress").removeAttribute("value");else $("row-progress").value=Math.min(1,Math.max(0,fraction));
  text("row-progress-label",fraction===null?"全域分母未知；不以欄位完成數代替":`${(fraction*100).toFixed(2)}% · ${count(w.exported_rows)} / ${count(w.total_rows)} 查詢格點`);
  const scopeFraction=num(w.global_query_scope_ratio);
  if(scopeFraction===null)$("scope-progress").removeAttribute("value");else $("scope-progress").value=Math.min(1,Math.max(0,scopeFraction));
  text("scope-progress-label",`${count(w.resolved_grid_rows)} 個格點已有來源回覆 · ${count(w.source_empty_tasks)} 次明確空回${scopeFraction===null?"；全域分母未知":` · ${(scopeFraction*100).toFixed(2)}%`}`);
  renderForecast(data.eta,w);
  text("desktop-operations",count(traffic.local_desktop_operations_60m));text("traffic-sampled",`取樣 ${time(traffic.sampled_at_utc)}；不是官方帳號用量`);
  text("quota-second",count(q.smart_wizard_requests_per_second));text("quota-day-calls",count(q.smart_wizard_requests_per_day));text("quota-day-rows",count(q.smart_wizard_rows_per_day));
  const planning=data.planning||{};
  text("local-capacity",`本機批次：最多 ${count(planning.local_max_rows)} 列／${count(planning.local_max_cells)} 儲存格／${count(planning.local_max_companies)} 個代號，預覽最多 ${count(planning.preview_max_columns)} 欄；${planning.query_interval_contract==="minimum_query_start_interval_v1"?"查詢開始間距（包含工作耗時）":"完成後間距"} ${count(planning.minimum_query_interval_seconds)} 秒。這些是本機容量與限流設定，不是官方配額；已完成與隔離的舊批次保留原範圍。`);
  const interfaceBlocked=data.worker?.state==="desktop_interface_recovery_required";
  const alert=$("page-alert");alert.hidden=!(interfaceBlocked||w.blocked_tasks>0||["metadata_unavailable","paused_for_storage"].includes(data.state));
  if(!alert.hidden){text("alert-title",interfaceBlocked?"桌面查詢介面需要恢復":data.state==="paused_for_storage"?"磁碟餘裕不足":`${count(w.blocked_tasks)} 個工作需檢查`);text("alert-copy",interfaceBlocked?"TEJ 查詢視窗不可用，排程未送新查詢。只在明確允許後重開指定查詢；Excel、工作簿、原始來源與失敗收據都保留，恢復後仍須核對來源介面與實際下載。":data.state==="paused_for_storage"?"保留至少 5 GiB 空間；沒有送新查詢，也不刪除既有來源與收據。":replay.enabled?"已取得的來源檔與收據保留。先收回原結果，再依授權有界重排；登入／配額／來源範圍不明仍暫停，不以重送掩蓋資料錯誤。":"已取得的來源檔與收據保留；失敗或未知操作結果不自動重送。原始資料問題與介面／本機處理問題分開。");}
  const select=$("feature-table"),selected=select.value;
  const tableCatalogKey=(data.tables||[]).map(t=>t.table_id).join("|");
  if(state.tableCatalogKey!==tableCatalogKey){
    select.replaceChildren(el("option","全部資料表"));select.firstChild.value="";
    for(const t of data.tables||[]){const opt=el("option",`${t.name} · ${count(t.fields)} 欄`);opt.value=t.table_id;select.append(opt);}
    select.value=selected;state.tableCatalogKey=tableCatalogKey;
  }
  renderTables();
}
function renderTables(){
  const q=$("table-search").value.trim().toLocaleLowerCase(),phase=$("table-phase").value;
  const rows=(state.latest?.tables||[]).filter(t=>(phase==="all"||(t.field_phase_counts?.[phase]||0)>0)&&(!q||[t.name,t.category,t.smart_id].some(v=>String(v).toLocaleLowerCase().includes(q))))
    .sort((a,b)=>(order[a.state]??9)-(order[b.state]??9)||a.phase.localeCompare(b.phase)||a.name.localeCompare(b.name));
  state.tableOffset=Math.min(state.tableOffset,Math.max(0,Math.ceil(rows.length/PAGE)-1)*PAGE);
  const body=$("table-rows");body.replaceChildren();
  for(const t of rows.slice(state.tableOffset,state.tableOffset+PAGE)){
    const tr=el("tr"),td=cell(tr,"",`${t.smart_id} · ${t.category}`),button=el("button",t.name,"tej-table-button");button.type="button";
    button.addEventListener("click",()=>{
      $("feature-table").value=t.table_id;state.featureOffset=0;
      invalidateFeatures();
      window.StockAgentAcquisition?.reveal("features");
      $("features").scrollIntoView({block:"start",behavior:"auto"});
      void loadFeatures();
    });td.prepend(button);
    cell(tr,t.state==="running"&&t.active_work_kind==="discover"?"清點歷史範圍中":labels[t.state]||"待核實",`${t.phase} · ${t.blocked_tasks||0} 個需檢查${errors[t.last_error_code]?` · ${errors[t.last_error_code]}`:""}${t.local_retry_window?` · 最早可重試 ${time(t.local_retry_window.next_attempt_at_utc)}`:""}`);
    cell(tr,`${count(t.fields)} 欄／${count(t.universe_count)} 個代號`,`${t.frequency==="snapshot"?"單鍵目前快照，不是歷史":`${t.frequency} 查詢格點`}${t.field_batches?` · ${count(t.field_batches)} 組欄位分片`:""}，不是已證明原生頻率`);
    cell(tr,t.first_query_period||"未知",`選單起點 ${t.first_available_query_period||"待核對"}；非觀測／發布日`);
    cell(tr,t.last_query_period||"未知",`選單終點 ${t.last_available_query_period||"待核對"}；非觀測／發布日`);
    cell(tr,`${count(t.exported_rows)}／${count(t.grid_rows)}`,num(t.query_scope_ratio)===null?"分母未知":`估計結果 ${count(t.estimated_total_export_rows)} 列 · 查詢覆蓋 ${(t.query_scope_ratio*100).toFixed(2)}% · ${count(t.source_empty_tasks)} 次明確空回；不是歷史完整率`);
    const predicted=t.forecast||{},estimatedBytes=num(predicted.remaining_local_bytes?.middle);
    cell(tr,`${bytes(t.recorded_bytes)}／${bytes(estimatedBytes===null?t.estimated_total_bytes:t.recorded_bytes+estimatedBytes)}`);
    cell(tr,duration(predicted.remaining_seconds?.middle??t.remaining_seconds),predicted.timing_samples?`較快 ${duration(predicted.remaining_seconds?.fast)}／較慢 ${duration(predicted.remaining_seconds?.slow)}；${count(predicted.remaining_queries?.middle)} 次剩餘查詢${t.grid_rows===null?"（歷史軸外推）":""}；本表工時，不含其他表等待`:t.eta_basis);body.append(tr);
  }
  if(!body.children.length){const tr=el("tr");const td=cell(tr,"沒有符合篩選的資料表");td.colSpan=8;body.append(tr);}
  text("table-count",`${count(rows.length)} 張符合篩選`);text("table-page",`${rows.length?state.tableOffset+1:0}–${Math.min(rows.length,state.tableOffset+PAGE)}／${count(rows.length)}`);
  $("table-prev").disabled=state.tableOffset===0;$("table-next").disabled=state.tableOffset+PAGE>=rows.length;
}
function renderFeatures(data){
  if(data.read_only!==true||data.raw_values_exposed!==false)throw new Error("invalid feature metadata contract");
  const body=$("feature-rows");body.replaceChildren();
  for(const f of data.features||[]){const tr=el("tr");cell(tr,f.name,f.table_name);cell(tr,f.phase,labels[f.table_state]||"待核實");
    cell(tr,f.unit,f.unit==="thousand_shares"?"欄名明示千股；原值保留，顯示換算尺度仍待核實":"欄名提示不是全表單位驗證；未知不猜測");
    cell(tr,String(f.raw_input_policy).includes("do_not_use_as_raw_input")?"來源 log 衍生 · 禁作原值輸入":"保留來源顯示值／比率",f.role);
    cell(tr,count(f.exported_non_null_cells));cell(tr,`${f.first_query_period||"未知"} → ${f.last_query_period||"未知"}`);
    cell(tr,count(f.local_count),`${f.local_first||"未知"} → ${f.local_last||"未知"}；同概念對照，非逐鍵完整證明`);body.append(tr);}
  if(!body.children.length){const tr=el("tr");const td=cell(tr,"沒有符合篩選的欄位");td.colSpan=7;body.append(tr);}
  const p=data.page||{};text("feature-count",`${count(p.matched_total)} 個符合篩選的表內欄位`);text("feature-page",`${data.features.length?p.offset+1:0}–${p.offset+data.features.length}／${count(p.matched_total)}`);
  $("feature-prev").disabled=p.offset===0;$("feature-next").disabled=!p.has_more;
}
function featureScope() {
  return new URLSearchParams({offset:String(state.featureOffset),limit:String(PAGE),
    q:$("feature-search").value,table:$("feature-table").value,phase:$("feature-phase").value}).toString();
}

function validateFeaturePage(data, offset) {
  const page = data.page;
  if (data.read_only !== true || data.raw_values_exposed !== false
      || !Array.isArray(data.features) || !page || page.offset !== offset || page.limit !== PAGE
      || !Number.isSafeInteger(page.matched_total) || page.matched_total < 0
      || data.features.length !== Math.min(PAGE, Math.max(0, page.matched_total - offset))
      || page.has_more !== (offset + data.features.length < page.matched_total)) {
    throw new Error("Invalid TEJ feature page");
  }
}

function invalidateFeatures() {
  state.featureRevision += 1;
  state.featureRequest?.controller.abort();
  state.featureRequest = null;
  state.featureUpdatedAt = 0;
  $("features").setAttribute("aria-busy", "false");
  $("feature-rows").replaceChildren();
  text("feature-count", "選擇已變更；等待符合目前篩選的欄位清冊。");
  text("feature-page", "—");
  $("feature-prev").disabled = true; $("feature-next").disabled = true;
}

function loadFeatures(){
  if (!Dashboard.isElementVisible("features")) return;
  const key = featureScope(), offset = state.featureOffset;
  if(state.featureRequest?.key===key)return state.featureRequest.promise;
  state.featureRequest?.controller.abort();
  const revision=++state.featureRevision,controller=new AbortController();
  $("features").setAttribute("aria-busy","true");
  $("feature-prev").disabled=true;$("feature-next").disabled=true;
  const request={key,controller,promise:null,completionRevision:state.latest?.activity?.completion_revision};
  state.featureRequest=request;
  request.promise=(async()=>{
    try {
      const data = await fetchJson(`api/features?${key}`, {signal:controller.signal});
      if (revision !== state.featureRevision || key !== featureScope()) return;
      validateFeaturePage(data, offset);
      renderFeatures(data);
      state.featureUpdatedAt=Date.now();state.featureCompletionRevision=request.completionRevision;
    } catch {
      if (revision === state.featureRevision && key === featureScope()) {
        text("feature-count", "欄位清冊暫不可讀；僅保留同篩選上次資料，不代表已更新。");
      }
    }
    finally{if(revision===state.featureRevision){$("features").setAttribute("aria-busy","false");state.featureRequest=null;}}
  })();
  return request.promise;
}
async function refresh(){
  if(document.hidden||state.statusPending)return;state.statusPending=true;
  $("refresh-now").disabled=true;
  try{renderStatus(await fetchJson("api/status"));if(Date.now()-state.featureUpdatedAt>=30000||state.featureCompletionRevision!==state.latest.activity?.completion_revision)void loadFeatures();}
  catch{text("tej-health",state.latest?"更新暫停 · 上次觀測":"面板暫時無法讀取");$("tej-health").className="status degraded";$("active-readback-card").hidden=true;text("activity-sync","同步暫時失敗；保留上次資料量，不代表目前仍在下載。");text("tej-freshness","稍後重試；不會呼叫 TEJ 或操作 Excel 填補顯示。");}
  finally{state.statusPending=false;$("refresh-now").disabled=false;}
}
let searchTimer=null;
for(const id of ["table-search","table-phase"]){$(id).addEventListener("input",()=>{state.tableOffset=0;renderTables();});}
for(const id of ["feature-search","feature-table","feature-phase"]){$(id).addEventListener("input",()=>{
  state.featureOffset=0;invalidateFeatures();clearTimeout(searchTimer);searchTimer=setTimeout(loadFeatures,250);
});}
$("table-prev").addEventListener("click",()=>{state.tableOffset=Math.max(0,state.tableOffset-PAGE);renderTables();});
$("table-next").addEventListener("click",()=>{state.tableOffset+=PAGE;renderTables();});
$("feature-prev").addEventListener("click",()=>{state.featureOffset=Math.max(0,state.featureOffset-PAGE);loadFeatures();});
$("feature-next").addEventListener("click",()=>{state.featureOffset+=PAGE;loadFeatures();});
$("refresh-now").addEventListener("click",refresh);
renderTables = Dashboard.createDeferredRenderer("tables", renderTables);
Dashboard.observeVisibility("features", (visible) => { if (visible) void loadFeatures(); });
Dashboard.scheduleRefresh(refresh,{intervalMs:5000});
