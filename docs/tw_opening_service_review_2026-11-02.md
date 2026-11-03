# 2026-11-02 當沖、Discord 與公開面板修復

## 範圍與判準

任務沿用 `tw-opening-latest-standard-latency-20261022`。當沖優先；來源、模型訊號、
模擬帳本、公網資料與瀏覽器呈現分開驗收。六模式一秒目標不能由盤前暖機、HTTP 200、
回放時間或某個熱快取測試代替。本輪不送券商委託，不改原帳本，不刪原始資料。

本輪沒有重啟當沖引擎、Discord、來源監控或公開 gateway，也沒有改帳本。
前端 app 已由原靜態服務提供，公網獨立讀回與本機 SHA 相等；後端修正仍為候選，
不能當作原生程序已載入。既有其他工作目錄變更予以保留。

## 第一性原理拆解與工作順序

總延遲的因果鏈是「正確交易日／清冊 → 可用來源與盤前輸入 → 新價／有來源的最近成交
參考 → 六模式推論 → 持久發布 → 模擬執行提交 → 公開 DTO → 瀏覽器套用」。平行化
只能移除獨立工作的等待，不能把缺資料、舊版本或尚未發生的成交當作完成。

| 階段 | 可解決的障礙與措施 | 必須保留的限制／驗收 |
| --- | --- | --- |
| 盤前輸入 | 移除新模式首次觀察造成的跨模式全域清除；沿用原共用 cache | 已知來源版本真正改變仍失效，final arm 需同程序／同版本 |
| 來源探測 | 失敗回應保存當次 HTTP、格式、長度與 SHA；原 owner 重試 | HTTP 200 HTML 不算資料；最後成功的 304 不代表本次失敗 HTTP 狀態 |
| 分鐘修復 | 本機先查，缺少的精確日期交原優先下載 owner | 整天有資料不等於 09:01 有成交；無首筆參考不是流動性或成交證據 |
| 模擬執行 | 核對原始發布、逐版修訂、持久提交及完整目標 | 不回填假的券商成交，不合併盤中所有版本為單一開盤訊號 |
| 網站讀取 | 先驗證模式／日期／明細；修正特徵快取的讀取競態及 manifest 子物件驗證 | 缺資料與真實告警不能用 UI 隱藏；公網傳輸、DOM 另測 |
| 盤後維護 | 分開 Gateway、快取 worker、來源等待及開盤保護 | `waiting_source` 不等於 Gateway 斷線；不搶占開盤資源硬啟維護 |

## 今天的盤前事實

- Discord 原生 run `140066-1793418641307486585`，08:07 六模式暖機完成；初次冷暖機
  93.914 秒，不是開盤推論耗時。08:25 六模式盤前資料準備均 ready。
- 08:45 final arm 六模式均完成；Factorized 3.529 秒、MGDA 2.523 秒、Multi-Basis
  37.091 秒、V8 43.347 秒、多基底22 62.021 秒、1億 62.755 秒。不同模式有共用來源與
  排隊，不能相加當 CPU 工時，也不能僅憑 ready 宣稱開盤一秒。
- 08:42 來源監控失敗項包含 `twse_api_opendata_t187ap11_l`。只讀測試先觀察到
  HTTP 200、`text/html`、947 bytes，下一次來源回應恢復 JSON。不能由舊狀態列上的
  `http_status=304` 推斷失敗原因。原來源監控後續自然恢復；08:47:05 官方 08:30
  驗收完成、`status=ok`、`failures=[]`，沒有人工改寫通過旗標。但 08:55／08:57
  重試後再因上游 JSON 失敗，不能沿用 08:47 的成功宣稱當前來源驗收通過。
  08:57 那次於 09:00:19.738 完成，這不等於開盤模型等到這個時間才開始。
- 08:47 與失敗重試的 final audit dependency SHA 相同（`ded8cc35…`）。失敗分支
  跳過 audit、留下空 dependency，卻被誤報為「audit 後版本變動」。這是診斷錯誤，
  沒有證據可據此宣稱開盤 freeze 失效；上游資料失敗本身仍真實保留。
- 原官方交易清冊：listed 2,321、暫停 5、可交易分母 2,316；歷史模型軸 2,771
  不是當日分母。盤前 transport probe：MIS 與 FinMind 曾證明可回應；Fugle HTTPError、
  共享 Shioaji 08:05 的 1.5 秒 probe timeout 後已恢復。08:55:49 引擎盤前 ready，
  eligibility 72.56 ms、共享報價 125.671 ms、stream armed 46.067 ms；08:58 仍 ready。
- 08:55 late arm 六模式皆 ready，耗時約 15.093～18.318 秒，於 08:55:15～18 完成。
- 08:52 Discord `discord_connected=true`、`core_health=ready`。盤後快取 worker
  最後收據為 08:07 `waiting_source`、6 attempts、0 failures、1 deferred；目前不是
  `LoadedImplementationChanged` 或 Gateway 中斷。原生盤後 dispatch 保護至 13:40，
  不為清除告警在開盤前硬啟動來源重建。
- 當沖引擎與 Discord CPUWeight/IOWeight 均 10000、Nice=-5；已讀服務及
  `system.slice` 的 `cpu.max=max`、`memory.max=max`。未增加 CPU、記憶體或併發硬上限。
- 最後 canonical exact ACK 讀回：引擎 revision 139771，Discord 同 run／revision，
  lag=0、connected=true、core ready、ledger_integrity_ready=true。這是同步／存活證據，
  不抵銷下述未完成模擬目標、公網錯誤或資料缺口。

## 今天原生開盤：來源、訊號、成交分開核對

| 模式 | 首次持久發布（台北時間） | 自 09:00 起 |
| --- | --- | --- |
| Factorized | 09:00:03.593904 | 3.594 秒 |
| MGDA | 09:00:04.793153 | 4.793 秒 |
| V8 | 09:00:05.807845 | 5.808 秒 |
| 多基底22 | 09:00:10.634228 | 10.634 秒 |
| Multi-Basis | 09:00:11.807072 | 11.807 秒 |
| 1億 | 09:00:11.809466 | 11.809 秒 |

這是原生 Discord publication journal 的結果，不是本輪候選加速效果，也不是券商
成交。六模式發布通過 20 秒，但未通過 11 秒／1 秒。原生 daily audit 的總狀態仍為
`missed_or_unverified`，含歷史 `malformed_attempt_rows:1`；沒有刪除這項證據來改成通過。

- 第 1／5／10／11／20／30 秒，真正有效開盤價分別為 1／26／315／478／1191／1430。
  第 11 秒另有 6 檔來源觀察到無首筆、1832 檔未知；未知不能直接當成已證明無首筆。
- FinMind 首輪 888.761 ms，有效 JSON 2894 rows，但當日要求的 2316 檔全部仍是
  其他交易日。HTTP headers/body/decode/validation 合計 493.487 ms；不能把舊日改成
  今日。原生來源宣告更新間隔 10 秒，開盤輪詢最小間隔已為 1 秒，兩者不同。
- Shioaji 首輪 1929.533 ms，13 個有效開盤價，其餘 2303 是舊／盤前時間。
  共享連線存在不等於所有股票已發生當日首筆；沒有增開 SDK login。
- MIS 首輪 10108.129 ms：13 個 HTTP requests，12 個失敗，錯誤鏈為 read timeout；
  當次使用 4 workers、4 秒 request timeout。稍後一輪有 86 requests／10 failures，
  錯誤鏈則是 HTTPError／RemoteDisconnected 等，不是全部同一種 timeout。
  原生 soft hedge／保留在途接收／批次減半已存在，不宣稱是本輪新完成。
- Fugle TSE／OTC 首輪均 HTTP 403，約 153／172 ms。這是權限／存取障礙，不以無限
  重送或調 timeout 冒充已修復；沒有更動憑證。
- 原排程 wake 約晚 129～210 ms。多基底22 的 `signal_inference_ms` 約 4225 ms，
  六模式 consumer 仍逐一呼叫，模型共用鎖含格式化與發布。舊
  `mode_dispatch_queue_ms` 混合輸入等待及 consumer 排隊，不能全歸因 CPU queue。
  多個 span 重疊／巢狀，不將它們直接相加當作總時間。
- 09:30:18 原生帳本完整性 ready、divergence=0，但 1億／MGDA／V8 仍有未完成目標。
  其餘 `no_order` 不代表每版都完全成交；盤中再平衡需逐版核對。
- 原生公網 observer 首次記錄六模式 status DOM 套用約 09:00:28.955～28.957，
  notice→paint 約 859 ms，不是開盤→成交→呈現 859 ms。完整 signal key 與首版對應仍
  需核對；double-RAF 是呈現機會觀測，不是實體像素／交易所時鐘證明。
- 09:37 另沿用 canonical symbol audit，讀原 journal prefix，沒有新 provider request。
  `opening-symbol-deadlines.json`／`.csv` 列出全部 2316 檔在 1／5／11／30／60 秒
  的來源狀態、供應商與後來首個有效價時間。當次 retained receipt 的 253 檔未知全為
  `listed_for_regular_trading`，沒有證據可以說成下市並直接排除。第 30 秒新分析的
  fresh no-first-print 為 234（另 652 unknown），與原 audit 的累計觀察 240 不同，
  差異來自 absence proof 時效；兩份原始結果都保留，不改原時鐘。

## 已實作的修正及採用邊界

1. `signal_engine._observe_completed_panel_identity`：首次看到另一個設定不再清除已
   準備的其他模式輸入；已觀察設定真正變更（含版本回復）仍清除並推進 warm epoch。
   不改來源 fingerprint、模型數值、開盤參考或推論 ABI。
2. `watch_tw_public_source_events.SourceProbeError`：保留最後一次失敗回應的狀態、
   content type、bytes、SHA 與錯誤類別。只有失敗時另算 raw SHA；成功路徑沒有多算
   一次大回應 hash。`last_probe_*` 與最後成功版本分開；重試後 timeout 不借用前次
   回應證據；HTML 不會被承認成來源版本。
3. 特徵摘要 cache 只接受 canonical JSON reader 已認證的穩定版本；讀取競態不會
   把新內容塞入舊 metadata key，manifest 子物件驗證也不會被父檔快取跳過。
4. 分鐘來源盤點支援原生 daily audit 的實際契約及其原始 publication 子契約，嚴格
   核對模式清冊。保留 audit evidence errors，不把來源 inventory 升格為 SLO／回放驗收。
5. 官方 08:30 gate 的 source-blocked 分支只在原共享鎖下，讀取完整且 SHA 相符的既有
   audit；不呼叫重建。缺 audit 明確標為未驗證，不再把空 dependency 報為版本改變。
   真正 SHA 變更仍失效，上游失敗仍令整體 gate failed。
6. Discord 排程新增 producer input-ready wall／monotonic clocks，分開
   `input_ready_from_open_ms` 與 `input_ready_dispatch_wait_ms`。不把缺時鐘填成 0，
   不拿不同程序 monotonic 相減，沒有移除共用模型鎖或改動推論順序。也保留 producer
   已有的 quote 前後細項與 dense／Factorized input transport，不為測量另做 CUDA 同步。
7. 前端相同模式／日期／執行器的明細與曲線請求不再被每個新版 content revision
   中止：先接受該次一致結果，再合併補讀最新版。改模式／日期／owner 仍取消舊請求，
   分頁不混合不同版本，失敗保留既有畫面並退避。原每分鐘價格更新節奏不變。
   公網 app SHA `af774f5edcfe18d7edecbdb5daacb9059618b277b434be8d0fc8c4618c4c545e`
   與本機相等；這一項前端已採用，沒有重啟 gateway。
8. 後端曲線 guard 改依真正依賴的 product／模式清冊／marks／benchmark journals
   驗證；pin 住已提交 state header，不為無關 order/fill commit 失效。owner、清冊、
   head 移除、未提交 append／實際曲線變更仍拒絕。明細維持原完整 commit guard。
   **這不足以解決持續追加 marks 的冷重建**，尚未部署，也未放寬 prefix integrity。

## 網站實測與未採用的最佳化

證據根目錄：`artifacts/operations/dashboard_recovery_20261102/`（產物不進 Git）。

- 1366×768 公網當沖／隔日沖：水平溢出 0；重複 ID、未命名控制、未標註欄位、
  控制項裁切／重疊均 0。當沖六模式均載入訊號及持倉首頁，各 100 rows，沒有讀取錯誤。
  特徵明細成功載入 99 features；官方來源監控 196 項有資料。
- 六模式切換仍約 0.723～6.757 秒；全年分鐘曲線約 7.360 秒，不能宣稱延遲已解決。
- 開盤後 390×844、DPR 3 真實手機 Chromium 重測：當沖仍有 20 個 API 503／timeout，
  六模式明細未通過，不能把「沒有 signal rows」視為合理空資料。隔日沖該次沒有 API
  timing errors，空訊號屬允許狀態；不以此證明隔日沖收盤執行通過。兩頁水平溢出皆 0。
  原生 opening browser watcher 也留有 232 個 errors。Gateway journal 指向
  `PaperReadChanged`／`DashboardSnapshotChanged`：journals 超前 commit、durable ACK
  切換或 projection 中途版本更新。前端修正不能讓這些後端錯誤自動消失。
- 新 history guard 真實只讀探測仍失敗：11959.479 ms 期間 relevant marks 改變，
  `PaperReadChanged` 如實保留於 `history-scope-live-probe.json`。未拿 metadata／檔案增長
  當作 prefix hash 證明，也未刪分鐘、降採樣或移除 end guard 來湊快。
- 保留瀏覽器失敗證據：Cloudflare 注入 script 被既有 CSP 擋下；一次 QUIC 錯誤。
  不為消除 log 加入 unsafe-inline。08:40 單次 benchmark 要求非空訊號，因盤前正確
  空資料而逾時；這不是今天開盤服務驗收。
- 同一份 3,332,445-byte 真實 summary、10 次／variant、ABBA 純解碼：full p50
  79.926 ms，投影 76.137 ms，內容相等。收益不足解釋數秒延遲，**未保留這條額外
  解碼路徑**；只留下獨立必要的快取一致性修正。這不是完整網頁或開盤加速證據。

## 分鐘來源恢復

10/30 首次發布的六模式非零訊號中，454 個 distinct symbols 缺原開盤參考，本機
找到其中 8 個 09:01 列，其餘 446 登記原 FinLab 分鐘 owner 的精確 10/30 分區。
request `7eedeb95516acf97a2d70c6b27390267efb28815944a897aaea88676a007a2a1`，
11/03 08:52 到期；09:30 左右原 registry 讀回 446 項仍為 `pending_existing_owner`，
查詢 provider calls=0，不宣稱下載完成。舊 market_status 的 running 時鐘停於 10/28，
不拿舊 running 狀態冒充目前有下載進度。

09:32～09:33 沿用 `run_finlab_finite_priority_drain.py --max-passes 1 --kind tw_minute`
嘗試原 owner 的有限補抓；實際 `physical_backing_reserve`、exit 75、attempted=0。
Windows backing 可用 32,680,464,384 bytes（約 30.4 GiB），該有限容量例外需要
52,613,349,376 bytes（49 GiB）；WSL 虛擬可用 206 GB 不能取代此證據。未繞過原保留
值、未刪資料、未另開下載路徑；至少還需約 18.6 GiB 加上執行期間寫入餘裕。

這 446 是「缺所需 09:01 列」而非已證明「整天來源缺失」。例如 0057 原 Shioaji
月 chunk 有 17 個交易日、129 rows、10/30 最後成交列 12:38、source_gap_dates=[]；
需區分沒有首分鐘成交與資料缺漏。來源比對、最近真實成交參考、逐版回放、費率／成交
及分鐘曲線仍分開驗證。此次沒有套用回放，也沒有改寫 10/30 或今天帳本。

## 回歸與剩餘驗收

- 跨模式輸入／singleflight／prepared input：167 passed。
- 官方來源／排程／publication／reconcile：172 passed。
- Discord 排程與 latency helpers：214 passed。
- 最終 journal／recovery／mode snapshot／gateway／dashboard／private JSON／當沖及
  隔日沖共用回歸：897 passed。曾有一個舊 source-string assertion 失敗，已改成新
  scope coalescing 契約並全組重跑；不是忽略失敗。
- 分鐘盤點契約：4 passed；前端四組 Node modules：94 passed。不是全庫測試通過。
- 開盤日誌／逐檔來源證據 reader：149 passed；history guard export 補查：48 passed
  （包含於前述 897 的重跑，不重複計入）。
- 否定結果與原始 browser／provider／測試紀錄均保留於上述 evidence root。

下一步依序：

1. 原分鐘 owner 有限補抓、查來源收據，區分真缺資料與無首筆，不動原帳本。
2. 完成 canonical immutable committed read view：同一 commit pin state／mode objects
   與 journal head／watermark，讀者可完成舊版，再追最新版；不能要求 writer 在整段
   冷重建期間停止，也不能跳過雜湊。沿用既有 journal／mode snapshot owner，不另造帳本。
3. 按新增排隊 clocks 實測多基底22／六模式完整輸入與推論；先移出獨立的格式化／持久化
   等待，再評估可證明 thread-safe 的運算平行。先量測，不以盲目移除模型鎖冒險。
4. 盤後 canonical source accepted 後，驗收六模式 cache 同版本 readback 及 Discord
   maintenance；目前 Gateway ready 不等於維護完成。受控發布後端，不在持倉中強制
   重啟執行器，也不把大量其他 dirty code 隨手一併載入。
5. 保留逐版訊號／模擬成交／費率／每分鐘曲線及 browser correlation，下一交易日再做
   原生端到端。今天未達一秒，下一交易日成功與零錯誤尚不能保證。
