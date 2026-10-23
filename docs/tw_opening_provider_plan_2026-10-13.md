# 台股 09:00 開盤行情：來源、異常與主備方案

初版查核日：2026-10-13；來源清冊與三時段測速續查：2026-10-22。時間均為 Asia/Taipei。
這是行情取得與公開展示規劃，不是投資建議或券商成交驗收。

## 最新查核：2026-10-22

**尚未達到全市場一秒／十一秒，也不能宣稱只剩 API 成本或零錯誤。** 今日官方普通股／ETF
交易軸為 **2,309 檔**；08:55 的原生 roster 保留六個模式，不縮行情分母或模型的既有準備條件。
開盤原生日誌、獨立全市場 probe 與受控制的本機比較分開驗收。
下列最新查核為目前結論；較早日期各節保留歷史條件與失敗證據，不是現行權限／容量保證。

| 時段／原始時點 | 實際觀察 | 邊界 |
| --- | --- | --- |
| 10/22 09:00 原生開盤 | 第 1 秒 3、第 5 秒 57、第 11 秒 **434／2,309**；第 30 秒 1,256 | 未達標。11 秒仍 1,875 未知；MIS／Fugle 失敗沒有轉成「無成交」 |
| 10/22 09:49 盤中獨立 probe | probe 入口至最後報告寫入前 **1,458.719 ms**；2,019 有開盤價、0 已證明未首筆、290 未知 | 空收據起跑；290 檔 FinMind 回應仍是其他交易日，不能稱完整報價 |
| 10/22 09:58 盤中續測 | 入口至報告寫入前 **1,390.644 ms** | 不同時間的實網結果，不拿它與 09:49 當固定輸入的加速比較 |
| 10/22 10:05 盤中續測 | **9,624.638 ms**；2,087 真開盤、178 已證明未首筆、44 未知 | MIS 恢復部分回覆但 29 請求中 5 個失敗；90% quorum 976.097 ms 不等於完整取得，不能只挑較快的先前樣本 |
| 10/22 13:53 盤後獨立重測 | 入口至報告寫入前 **1,442.906 ms**；2,275 真開盤、0 有效無首筆證據、34 未知 | 原 90% quorum 767.602 ms、全部 provider 工作持久完成 1,320.166 ms；MIS 單檔 prewarm 連線被關閉，沒有送全市場突發。不代表全量價格齊備或 09:00 SLA |
| 10/21 13:40 已完成盤後排程歷史對照 | **2,133.566 ms**；2,282／2,308 有開盤價、26 未知 | 保留昨天分母；不是今天盤後或全量成功，不與不同時點實網樣本當固定輸入 A/B |

最新口徑的完整本機傳輸、持久完成邊界、負向併發測試與重測指令見
[10/22 延遲續修](tw_opening_latency_stages_2026-10-14.md#最新口徑重測與本機傳輸續修2026-10-22)。
本輪沒有降低 quorum、把未知視為零成交或略過來源驗證；新常駐程式的部署計畫
仍被 **17 筆未平倉／0 筆 pending orders** 拒絕，未重啟 owner 或操作交易帳本。

現有來源逐項核實：

| 來源 | 本機實際權限／結果 | 低延遲定位 |
| --- | --- | --- |
| 永豐 Shioaji | 共用 owner 已接通；不另登入。今日前 11 秒觀察到 428 檔合格開盤，與其他來源重疊另計 | 官方即時訂閱上限 200，查詢上限 50／10 秒；不以高頻 snapshots 輪詢或多登入繞過限制。[官方限制](https://sinotrade.github.io/zh/tutor/limit/) |
| 官方 MIS | 開盤前四批約 4.2 秒逾時且未到回應標頭；09:49 單檔 probe 又在回應前被關閉；10:05 已恢復部分回覆，29 請求仍有 5 個在回應前失敗 | 不只是開盤尖峰，也不是永久完全斷線；持續使用既有連線重用、局部併發、熔斷與單檔 half-open 恢復。無證據把根因確定為 WAF／ISP，也不盲增併發 |
| FinMind | 本機全市場快照權限有效；首輪 09:00:00.811 已收到完整 2,309 軸，但當輪 **0 檔同日有效開盤** | 官方約十秒更新；Sponsor 999／月、Pro 3,330／月。本機已拆 HTTP／body／JSON／驗證；來源回完不等於新價格齊備。[即時文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)、[費用](https://finmind.github.io/Pricing/) |
| Fugle REST | 今日 TSE／OTC 全市場 snapshot 均 HTTP 403 | 不能列成可用全市場備援；403 不等於 API key 所有介面均失效 |
| Fugle WebSocket | **10:01 以既有 key 成功認證、訂閱 2330 aggregates 並收到 snapshot**；冷連線到首筆 **166.557 ms**，隨即關閉測試連線 | 只證明單檔，非全市場／開盤 SLA。免費 5 訂閱；進階 2,999／月、每連線 2,000 訂閱、最多兩連線，容量需實際帳號與完整 ACK 驗收。[方案](https://developer.fugle.tw/docs/pricing/)、[欄位](https://developer.fugle.tw/docs/data/websocket-api/market-data-channels/aggregates/) |
| 其他券商／授權 feed | 元大、富邦、玉山、群益、台新、凱基、國票、IBKR，以及交易所／國內外資訊商，完整分類見下表 | 沒有本機正式憑證、全量訂閱 ACK 或來源時鐘證據的，不冒稱已接通；不自行購買付費方案 |

### 無首筆參考與 44 檔未知逐檔核對：2026-10-22

模型採「有同日首筆就用首筆，尚無首筆則沿用已知最後真實成交」；最後成交
可能是昨日收盤，也可能更早。新契約
`last_real_completed_trade_no_first_print_model_only_v2` 沿用 canonical panel
的正成交量、有效收盤價與來源日期，不使用 bid/ask 中點、試撮或零成交日的調整參考值。
普通股無首筆時沿用歷史成交僅是這個模型的參考政策：
[TWSE 開盤價定義](https://investoredu.twse.com.tw/Pages/TWSE_InvestmentQA.aspx?ID=14)
仍是當日第一筆成交，不能把歷史收盤改寫成交易所當日已成交的開盤價；
[開盤競價基準](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW)
也有無收盤、除權息等例外，不等同一律複製昨收。

- `model_input_price`、`model_input_price_basis`、`model_input_price_is_carry`、
  `model_input_price_source_date` 與原 `open_price`／成交／資金帳本分離。真正首筆
  一經觀測即優先；來源未知不假裝零成交，也不靠 carry 製造成交量或成交能力。
- 連續零成交日往前找最後有效成交，不只讀前一天；`alive_mask` 只是 close
  覆蓋，缺一天資料不代表下市。投影按不可變 panel／row 身分快取，可盤前預備，
  不在每次 tick 重掃全歷史；dense 與 Factorized 共用同一覆寫邏輯。
- 無首筆證據是有期限的觀察，不是整天不變的事實。即時共享快取和來源修復
  依證據本身時間檢查 15 秒期限；後續收到舊永豐行情不能延長舊 MIS 證據。
  舊證據仍保留，診斷列出證據時間與年齡，不改原始資料或帳本。

10:05 probe 的 44 未知已與**同日、同 fingerprint、原檔 SHA 驗證通過**的官方名冊
逐檔比對，44 檔全是 `listed_for_regular_trading`，沒有任何一檔可僅因這次缺價
排除成停牌／下市。10:49 讀回累積來源時，17 檔已有真開盤，27 檔僅保留
10:05:30 的無首筆證據；後者是過期證據，**不能宣稱 10:49 仍沒成交**。

| 原 44 未知的後續狀態 | 代號 |
| --- | --- |
| 已有真正開盤價：17 檔 | 6844、6859、7705、7708、7728、7743、7819、7820、7842、8059、8097、8284、8409、8416、8435、8446、8937 |
| 尚待新證據：27 檔 | 6762、6767、6791、6843、6887、6899、6904、6958、6965、7713、7753、7767、8077、8080、8087、8101、8342、8354、8367、8411、8444、8455、8917、8928、9931、9949、9962 |

[逐檔姓名、官方狀態、後續時間與 SHA](../artifacts/operations/tw_opening_last_trade_20261022/unknown44_final_readback.json)；
[MIS 補查失敗原始測速](../artifacts/operations/tw_opening_last_trade_20261022/unknown44.json)：
44 檔合為一批，首次加一次重試共 11,750.593 ms，兩次均未取得回應標頭，
分別為連線被關閉與讀取逾時。不能把這個網路失敗變成「無成交」或已修好 MIS。
稽核入口 `scripts/audit_tw_opening_symbol_status.py` 只沿用既有 MIS adapter，
不另登入、不下單、不寫交易帳本；預設完全離線，補查需明確加 `--refresh-unknown-mis`。
輸入 JSON 與 SHA 由同一份讀取 bytes 計算，避免比較可變 live receipt 時雜湊錯版。

本輪盤中只改實作與驗證；未重啟／部署交易 owner，未回寫原訊號或成交。
正式生效仍須原有盤後、flat／pending order 與部署 admission 驗收，不能把磁碟修改宣稱已上線。

驗證：[943 項核心回歸](../artifacts/operations/agent-workflow/runs/last-trade-acceptance-v2-20261022T024807-ba81377b/run.json)
通過；[另 151 項展示／稽核測試及來源讀回](../artifacts/operations/agent-workflow/runs/last-trade-display-audit-20261022T024924-92d77356/run.json)
通過（兩組部分重疊）。首輪的兩個失敗是把 quorum 返回當成所有背景來源完成：
測試須等既有 `wait_idle` 再查 job 或關閉 generation，不減少任何來源或持久性斷言。
保留首輪失敗收據；熱路徑來源 journal 的 compact v1 row schema 不變，
證據年齡只在明確 as-of 稽核的 v2 診斷附加，避免破壞既有無損緊湊記錄。

### 開盤才會遇到的問題與方案

1. **真正首筆尚未發生**：上述 Fugle 單檔資料回報今日 2330 `openTime` 為
   **09:00:08.980736**，即使本機零處理成本，也不能在 09:00:01 取得這個尚未發生的真實開盤。
   這是供應者所載成交事件鐘，不是已證明的交易所發布／網路到達鐘。
   `lastPrice`／試撮／昨收／未回應都不能冒充這個開盤價或可成交量。
2. **跨日／盤前快照切換**：保留 session/open gate/PID/token fence、正式日期、trial 與成交量檢查；
   未觀察不能推論成交量為零。已證明無首筆時的模型參考值，仍與真實開盤、可執行行情及成交帳務分開。
3. **來源發布批次**：FinMind 約十秒更新不保證每檔第一輪就齊。保留全量 response、同日正開盤、
   官方已證明未首筆與未知四種狀態；健康來源並行，既有 request-start 短輪詢不繞過錯誤／配額退避。
4. **盤前就建立足夠容量的推播**：最低延遲優先考慮已授權的全市場 aggregates／完整報價串流；
   開盤前完成官方清冊、兩連線分片、全部訂閱 ACK、時鐘／trial／日期重組及可追蹤 gap 恢復。
   Fugle 進階或元大 SPARK 是容量候選；需另以真實帳號驗收，不把本輪單檔成功推成全市場能力。
5. **單源故障不能拖住其他源**：維持來源局部 single-flight、成功事實保留、bounded retry／熔斷／half-open。
   這次移除完全沒觀察來源的空收據搶寫；部分成功後失敗仍落盤，消費端完整 scope／輸入 fsync 不減。

### 本輪已實作與可重測證據

- 在既有測速 owner 補 **raw response／驗證後 observation／正開盤／resolved** 分層，
  開盤、盤中、盤後各自記錄首筆、50／90／95／99／100% 與包含失敗的 P50／P95／P99。
  權限／配額、無 HTTP 回應、body／validation、未分類失敗分開；舊資料缺測不是零耗時。
  即使來源 job 是 `complete`，有個別失敗請求仍保留 failure 分類；merged 消費端不能反向冒充來源觀測。
- 大批逐標的來源日誌共用欄位名稱，保留所有原始數值、整數時鐘、未知 flag、事件順序與同步 fsync；
  小批／舊格式仍可讀。非法／未知 layout 視為缺損證據，不靜默截短。這不是刪分鐘、刪股票或新建 collector。
- 同一個 **1,032,677 bytes 真實 FinMind 回應／2,309 軸**，加入明示的兩個受控失敗來源，
  四輪交錯 A/B/B/A 的完整本機 ingestion→驗證→journal→輸入耐久寫入，中位數
  **149.184 → 109.671 ms（減少 26.5%，約 1.36×）**；全 financial arrays、時鐘、
  durable rows、完整 observation journal 逐值 SHA 一致。此時 2,060 正開盤、249 未解，未縮分母。
  日誌約 508 KB → 257 KB；測試不含網路、模型或成交，不能外推成 09:00 全程加速。
- 另測 native JSON 加上通用遞迴驗證為 22.24 ms、原 strict stdlib 為 16.26 ms，**變慢的方案未採用**。
- 既有原生被動開盤 audit 保留 08:55 roster 與首次 deadline 收據；10:30／13:40 完整來源 probe、
  10:31／15:10 被動驗收 timer 皆保留且核對為 active。開盤不另啟測速登入或額外券商快照輪詢。
- 固定候選版本 **842 項相關測試通過**；10:13:54 完成 qualified Discord-only 採用，
  10:14:29 獨立讀回六模式 ACK lag 0、唯讀版本、六個受保護 owner 與帳本既有前綴均一致。
  未重啟交易引擎，沒有操作員帳本寫入／券商下單；這不是 GPU 暖機或 09:00 SLO 驗收。

證據：[原生開盤分段](../artifacts/operations/tw_opening_latency_20261022/phase_native_classified.json)、
[09:49 盤中](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/intraday_094928562390.json)、
[09:58 盤中](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/intraday_095833224601.json)、
[10:05 部分 MIS 恢復](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-22/intraday_100524023512.json)、
[單檔串流](../artifacts/operations/tw_opening_latency_20261022/fugle_stream_capability.json)、
[完整本機 A/B](../artifacts/operations/tw_opening_latency_20261022/combined_local_abba.json)、
[部署後獨立讀回](../artifacts/operations/tw_opening_latency_20261022/phase_independent_readback.json)。
這些原始行情／內部證據不改變公開網站的資料展示授權隔離。

## 歷史結論：2026-10-19 盤中

**已有接入與權限的主要組合仍是永豐共用 owner、MIS、FinMind；不等於今天三家均健康或全量。**
10/19 原生開盤第 11 秒只有 591／當日官方 2,301 檔有效開盤價，五模式原始發布為 0／5，未達標。
MIS 首四個請求逾時，盤中單檔診斷又遇到遠端關閉連線。Fugle 的 TSE／OTC 全市場快照
今天重新實測仍為 HTTP 403；單檔成功只保留 10/18 的證據，不算第四家可用全市場備援。

| 來源／候選 | 開盤來源與取得方式 | 本機可用性／成本邊界 |
| --- | --- | --- |
| 永豐 Shioaji | 既有串流＋共用批次查詢，保留正式成交時間及 trial 狀態 | 已接通；盤中即時使用訂閱。快照每批最多 500，不另登入或用高頻快照充當全市場推播；行情流量與訂閱額度依帳號 |
| TWSE／TPEx MIS | 官方開盤欄；按完整官方 scope 分批平行取價 | 已接通；網站免費但沒有全市場逐筆／一秒 SLA，也不能推定不限量 API 或公網再散布權利 |
| FinMind | sponsor 全市場快照，`data_id=""` | 已有權限；付費方案，官方說約每十秒更新，不是保證每檔 09:00:10 之前已有首筆 |
| Fugle | 單檔 REST、依市場快照、WebSocket | 10/18 單檔 HTTP 200；10/19 整批 TSE／OTC 仍 HTTP 403。快照每五秒，須開發者／進階權限；不靠逐檔數千 HTTP 偽裝可用全市場 |
| 元大 SPARK、富邦 Neo、玉山、台新 Nova／MLAPI、群益 SKCOM、凱基、國票、IBKR | 券商 quote／snapshot／串流，依各自 SDK、帳號與容量 | 官方候選清冊已收錄；尚無本機全市場帳號／訂閱 ACK／容量／延遲驗收，不算已接通 |
| TWSE／TPEx 授權 feed、國內資訊公司、LSEG／Bloomberg／ICE 等 | 交易所或授權企業介面 | 需合約、授權、網路與詢價；完整官方長名單及來源相關性見下文，不能把名冊列數當可用 API 數 |
| 本機一分鐘 K、官方盤後、FinLab、TEJ | 歷史開盤與補漏 | 可作可追溯歷史／盤後證據；不是 09:00 同步即時來源。MOPS 不是行情 feed |

本次依官方 schema 重新確認 [Shioaji 快照](https://sinotrade.github.io/tutor/market_data/snapshot/)、
[FinMind 即時資料](https://finmind.github.io/en/tutor/TaiwanMarket/RealTime/)、
[Fugle Snapshot](https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/)與
[Fugle 容量／方案](https://developer.fugle.tw/docs/pricing/)。
已接通不等於有公開網站展示授權；沿用原公開價格來源隔離，不修改內部策略或帳本。

這次實作將 MIS 下一批 HTTP 提前至同步驗證／journal 前送出，保持原本來源局部併發與熔斷，
並移除普通股／ETF scope 完全不會使用的 Fugle TIB 自動請求；TIB adapter 仍保留供明確選用。
不刪股票、不縮分母、不丟成功資料、不跳過 fsync。新增逐請求 row decode、每批 assembly／callback
及分時段安全投影；缺少歷史測速欄位仍是未測得，不記為零。

盤前全量保留資料重測、同版本控制、602 項測試及當時正式 Discord-only 換版，見
[10/19 完整實作驗收](tw_opening_latency_stages_2026-10-14.md#2026-10-19http-與本機驗證重疊三時段測速續修)。
本輪另完成 **683 項相關回歸**，修正開始 journal 阻擋送請求、MIS 多一個無用網頁往返、
測速預熱的全域串行等待，以及程式已失配卻仍做昂貴準備的問題。
詳見 [10/19 交易日實測與續修](tw_opening_latency_stages_2026-10-14.md#2026-10-19交易日開盤未達標與盤中續修)。

09:21 的獨立盤中 probe，從 probe 函式入口到最後報告落盤為 **982.523 ms**，
但只有 **1,927 正開盤／0 已證明未首筆／374 未解**；374 檔在 FinMind 回應仍屬其他交易日，
MIS 及 Fugle 故障沒有被改寫為成功。補上「清空程序快取後獨立重讀收據」的最終 09:37 實測，
則為 **1,476.442 ms、2,019／2,301、282 未解**；原 983 ms 保留為較早版本結果，不挑較快樣本。
兩者時間均不包含 Python 程序啟動，也不代表完整報價或成交。
09:42 再以 FinMind 同一連線發兩筆新 HTTP，每輪從空收據獨立重讀：第一輪 672.714 ms、
第二輪 332.813 ms；都是 2,039／2,301，262 未解。不是把已解碼快取當成快 API，
也不能把不同時間的來源狀態當成受控制的程式加速比較。
10:30／13:40 的原生 probe 已在盤後核對，分別是 2,154／2,301 與 2,272／2,301；
前者 MIS 單檔預熱成功後四批逾時，後者單檔預熱即斷線。沒有把有限來源回應寫成完整報價。
09:24 qualified plan 曾因原生 Factorized catch-up 在途而拒絕；09:38 原生 watchdog／systemd
自行恢復後，09:40 獨立讀回已確認採用當時程式、Gateway／exact engine ACK 正常、暖機 5／5。
09:41 新 qualified plan 通過，當時無需再人工重啟；這不是當日全五模式原始訊號／成交驗收。
不能宣稱已到理論極限、只剩 API 成本、永遠零錯誤或所有標的一秒成交。

### 10/19 盤後續修與 29 檔的解釋

17:50 真實全市場測速得到 **2,272 有正式開盤價＋29 由當日 MIS 證明尚無首筆＋0 未知**；
`resolved_complete=true` 不等於每檔都有開盤價。18:04 再測，MIS 單檔遭連線重設，
只能確認 2,272 有價／29 未知，未把另一輪的證據灌入新空收據。兩輪分別約 2.136／1.157 秒，
覆蓋不同，不是受控制的加速比較，也不是 09:00 訊號或成交驗收。

這 29 檔均在 08:55 官方正常交易清冊；依 **2026-10-19 當沖資格** 再分為：

- 16 檔不具當沖資格：00625K、00643K、1213、1538、3085、3259、3432、4183、4198、
  5703、6242、6597、7708、7842、8067、8921。應由當沖執行資格遮罩排除，不代表停止一般交易。
- 13 檔具當沖資格：00707R、1593、2926、2939、4305、4530、4581、4728、5455、5601、
  7782、8342、8923。無首筆不能永久刪掉；沒有有效開盤價不建立假的開盤模擬成交。

18:11 單批核對 29 檔及 2330 對照，原始 MIS 日期為當日，29 檔 `o="-"`／`v="0"`；
聚合資料曾看到的高低價對應舊 FinMind 觀察，不是這批原始 MIS 的當日高低價，也不是今天成交的證據。
以上都是盤後的來源觀察，不能回填成 09:00 已知的排除理由。官方行情分母仍為 2,301，
模型軸、當沖執行資格、已觀察有價／無首筆／未知分開保存。

[原始 MIS 查核](../artifacts/operations/tw_opening_latency_20261019/mis_no_print_raw_audit.json)、
[當日資格逐檔核對](../artifacts/operations/tw_opening_latency_20261019/no_print_day_trade_eligibility_audit.json)。
本輪的 Session 獨佔移交、向量化、完整重測與換版邊界，見
[盤後續修](tw_opening_latency_stages_2026-10-14.md#2026-10-19盤後連線重用與完整欄位投影續修)。

## 每日可交易清冊：來源與獨立更新（2026-10-18）

本節的「可交易」是**指定日期、時點允許交易**，不是「今天已經有成交」。
盤前不能拿今日成交量、provider 回價筆數或模型歷史 alive mask 當分母：
當日有效掛牌與商品範圍，扣除已生效的終止上市櫃、停止／暫停交易，再套用該時點的復牌事件。
當沖資格、可放空資格與券商可接受委託，仍是另外的交集，不能混成同一欄。

| 官方來源 | 用途／限制 |
| --- | --- |
| [TWSE ISIN 上市](https://isin.twse.com.tw/isin/C_public.jsp?strMode=2)、[上櫃](https://isin.twse.com.tw/isin/C_public.jsp?strMode=4) | 代號、商品種類、掛牌日的免費基礎清冊；不能單獨證明目前仍交易，已確認保留已終止 ETF |
| [證交所暫停／恢復交易](https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU)、[停止買賣](https://www.twse.com.tw/rwd/zh/violation/stop?response=json)、[櫃買交易狀態](https://www.tpex.org.tw/openapi/v1/tpex_cmode)、[櫃買暫停／恢復歷史](https://www.tpex.org.tw/openapi/v1/tpex_spendi_history) | 按有效日期及時間排除／恢復；公司換股等停止交易仍由既有、收據驗證的生命週期公告參考檔補齊 |
| [證交所 ETF 商品目錄](https://www.twse.com.tw/rwd/zh/ETF/list?response=json)、[櫃買 ETF 商品目錄](https://www.tpex.org.tw/www/zh-tw/ETF/list?type=domestic)與官方終止公告 | 上市目錄及上櫃七類聯集決定當前 ETF 成員；嚴格驗證來源，不以缺報價推論終止交易 |
| [證交所全日成交](https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL)、[櫃買收盤行情](https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes) | 如需「今天確實成交過」的另一份清冊，再核對來源交易日及正成交量；不是盤前可交易主檔 |
| [證交所證券主檔 BFI85U](https://eshop.twse.com.tw/zh/product/detail/000000006f6a5e34017033452c8d0070) | 付費結構化候選，官方列每交易日 22:00 提供次日參考；本輪未訂購，亦不能取代之後發布的臨時停復牌 |

公開資訊觀測站的公司基本資料不是 ETF 全清冊；基金基本資料亦可能保留已終止商品，
不宜直接拿來當當日可交易名單。Shioaji Contracts 是券商映射／交易條件的交叉核對，
也不取代交易所生效公告。現有 `v3` 產品範圍是上市櫃普通股與 ETF，
**不是全部有價證券**；創新板、TDR、特別股、ETN、權證、興櫃等不能不分種類直接塞進模型。

### 已部署的每日更新

- `scripts/refresh_tw_trading_universe.py --scheduled` 呼叫既有 canonical universe owner，
  不另建券商連線、不依賴 Discord 或模型暖機。與既有公開資料抓取共用 provider-global cadence／backoff。
- 獨立 `stockagent-tw-trading-universe.timer` 每天 07:30、08:00、08:15、08:30、08:45、08:55 檢查；
  09:15 只補缺失的當日收據，不重發布既有開盤清冊。官方日曆決定休市，不硬寫週一至週五。
- 開機 90 秒後亦檢查；08:57–09:10 保護開盤，不送清冊 HTTP。
  單次 120 秒期限、暫時失敗 30 秒後重試、10 分鐘最多三次；有效的目錄衝突不盲目重試。
- 原始 bytes、抓取時間與 SHA 保留；來源不完整不產生較小的假名單，已成功的來源可重用，
  失敗來源再取。正式 membership policy 已納入完整商品目錄；來源差異與政策排除分別保留，
  不修改既有歷史訊號或帳本，也不把已驗收的排除寫成「原始 ISIN 已無差異」。
- 安裝沿用 `scripts/install_tw_public_publication_services.sh --universe-only`；
  獨立排程安裝不連動重啟交易、行情或網站 owner。本次後續僅對 Discord 做合格的 code-only 換版，見下方驗收。

狀態入口為 [每日更新收據](../artifacts/live/tw_trading_universe/daily_update_status.json)、
[商品來源交叉驗證](../artifacts/live/tw_trading_universe/catalog_audit_status.json)。
原先獨立排程驗收為五模組 **200 passed／11.43 秒**，保留於
[排程安裝驗收](../artifacts/operations/tw_opening_latency_20261018/daily_universe_acceptance.json)。
本次分母、重試、讀取相容性與服務換版整合回歸為九模組 **445 passed／17.58 秒**；不是全庫測試。
09:50 的原生排程執行成功並回報 `closed`，因 10/18 為週日；下一次為 10/19 07:30，
下一交易日的實際盤前驗收與開盤仍待發生。

### 正式分母修復與原生讀回（2026-10-18）

09:14 的即時目錄實測為 ISIN 上櫃 ETF 119 檔、七類商品聯集 118 檔，差異僅 `00838B`。
已找到[櫃買原始公告](https://dsp.tpex.org.tw/web/announcement/announcement_detail.php?content_number=MTExMDAwMzYzMjE%3D&content_file=MTExMDAwMzYzMjEuaHRtbA%3D%3D)：
證櫃債字第 11100036321 號，111/04/27 公告、**2022-05-31 起終止櫃檯買賣**，
最後交易日為 2022-05-30。
[原始 HTML／雜湊與解析收據](../artifacts/operations/tw_opening_latency_20261018/tpex_00838b_termination.json)
保留此證據；不是由三家行情缺價推定終止交易。

09:47 的正式現況清冊及運行中網站 API 均已讀回：**ISIN 2,312 → 有效掛牌 2,311 → 扣除停牌 11 → 可交易 2,300**，
其中上市可交易 1,296、上櫃 1,004；排除名單僅 `00838B`。
這是休市日的當前官方資料觀察，不是假造 10/18 有開盤，也不是下一交易日固定不變的數字。

修復沿用 `tw_official_current_regular_stock_etf_universe_v3` 的相容讀取封套，
新增獨立版本 `tw_official_active_stock_etf_membership_v1`，綁定原始來源與有效掛牌名單的雜湊。
不是硬寫代號黑名單：上市目錄完整處理多幣別交易代號、尚未生效掛牌日與合法 NULL 指數欄；
上櫃七類先完整驗收再取聯集。目錄新增、ISIN 尚未跟上的商品會保留待驗收並重抓清冊，
不能遺漏該商品後宣稱完整；任何必要來源失敗均保留最後驗收版本與已成功來源。
失敗嘗試亦會解除「直接回傳有效舊清冊」的早退，真正重試失敗項目。

生命週期驗收未放寬日期條件：既有正式歷史參考檔仍只到 10/16，
本次重用 canonical 官方收集器隔離取得 10/01–10/18 的當前換股停牌證據，
兩市場 16 個事件與 33 份 raw receipts 通過既有驗證器；只供本次現況清冊的原始證據封存，
不把隔離尾段改名為完整歷史、也未修改特徵來源或歷史帳本。
正常交易日仍由既有 06:00 execution-actions 排程及歷史目錄合併供應，再由獨立清冊排程驗收。

效能上將完整來源解析留在盤前寫入端。初始候選讀取端重解析 HTML 約 4,892 ms，
已改為驗證全部原始資料 SHA 與政策綁定的名單投影，首次讀取實測 **52.53 ms**，
既有五模式測速工具另測 **66.55 ms**；兩者都不是強制清除 OS 檔案快取的磁碟冷讀。
五模式各 500 次完整模型遮罩的 p50 為 **0.162–0.172 ms**，請求識別 p50 為 **0.361–0.375 ms**。
名單、數量及模型遮罩有獨立來源重算比對，未刪標的、未省略 raw SHA 驗證；09:00 讀取不新增清冊 HTTP。
正式來源完整抓取與投影本次約 11.34 秒，仍屬盤前工作，不能冒充開盤至成交的總延遲。

部署採既有 `plan → apply → verify` 的 Discord-only code admission；
以換版前保留的實際讀取程式驗證其能讀取正式新收據，不為資料修正重啟交易引擎。
六個受保護 owner、五模式**引擎已接受**訊號 ID、三份帳本前綴完整 SHA 均未變，兩筆真實未完成模擬委託保留。
Discord 原生盤後工作可能發布新的研究 artifact 指標；這不等於改寫引擎已接受訊號或歷史成交。
換版驗收是 Gateway／載入程式／引擎 ACK，不是 GPU 暖機或 09:00 成交 SLA。

證據與可重測入口：

- [完整修復與原生 API／owner／帳本驗收](../artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/acceptance.json)
- [原始來源、名單不變與讀取相容性／測速](../artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/reader_projection_acceptance.json)
- [五模式完整模型遮罩與請求識別測速](../artifacts/operations/tw_opening_latency_20261018/universe_membership_repair/five_mode_benchmark.json)
- 既有 `scripts/benchmark_tw_trading_universe.py --signal-pointer <latest_signal.json> --rounds 500` 可重測；
  每個模式各傳一次 `--signal-pointer`，讀取當日已驗收清冊，不抓行情、不推論或下單。

尚未解除的邊界：網站仍忠實回報先前回放的開盤／09:01 缺價，Discord 盤後 Factorized artifact maintenance 仍 `degraded`。
這些不是名單分母錯誤，不能以本次清冊修正宣稱已補價、已重建帳本、全部服務已健康或下次開盤必定一秒成交。

## 最新權限讀回（2026-10-18）

**目前可用的全市場組合仍是永豐共用連線、MIS、FinMind；新增確認 Fugle 單檔可用，
但整批快照尚未獲准。** 不是四家均已支援全市場，也不是六條獨立行情：Fugle 的
TSE／OTC／TIB 三個 route 是同一供應商。

[本機權限收據](../artifacts/operations/tw_opening_latency_20261018/provider_access.json)
記錄 08:55:59 的有限唯讀檢查：

| 本機管道 | 可用性證據 | 對開盤目標的影響 |
| --- | --- | --- |
| 永豐共用 session | 已有原生 owner 與 10/16 全軸讀回；本輪沒有額外登入／定期 snapshot | 沿用既有推播、批次與執行路徑；200 訂閱不能直接當作全市場推播 |
| MIS | 已有完整 29 請求原始回應與全軸測速 | 平行補缺；沒有全市場逐筆 SLA，不保證開盤端點不逾時 |
| FinMind | 已有 sponsor 快照權限與全市場讀回 | 約 10 秒來源更新，增加 client 頻率不會變成逐筆 feed |
| Fugle 單檔 | `2330` HTTP 200，21.50 ms；原始資料日期 10/16 | 證明該端點有權限；今天週日，不是 09:00 實測，也不能外推 2,301 檔耗時 |
| Fugle 全市場快照 | TSE／OTC／TIB 均 HTTP 403；191.78／19.69／17.33 ms | 保留失敗與來源退避；不把存在 key 當成開發者／進階方案權限 |
| 富邦、元大、玉山、台新、群益、凱基、國票、IBKR、企業 feed | 尚無本機帳號／容量驗收 | 候選，不冒稱已開通、已測速或已訂閱 |

Fugle 官方定價已明列「**每個連線**最多 2,000 訂閱、最多兩條連線」，每檔股票×channel
占一個訂閱；因此進階方案的文件容量可分片容納目前完整 scope 的單一 trades channel，
但實際帳號方案、訂閱 ACK、重連與完整事件仍未驗收；不能把它當成已取得權限。
同時全量 trades＋books 約需 4,602 槽，超過這個文件容量。
[Fugle 定價](https://developer.fugle.tw/docs/pricing/)、
[Snapshot 的開發者／進階資格與五秒更新](https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/)。
本輪沒有替使用者購買、升級或新增常駐訂閱。

10/18 另重新讀回[官方資訊公司清冊](../artifacts/operations/tw_opening_latency_20261018/official_vendor_roster.json)：
國內 23、國外 25、即時指數 9、延遲資訊 20 列。下方長名單與官方原始表是詢價與
權限確認範圍，不把分組列數當成可直接使用的 API 數，也不宣稱已逐一登入實測。

本輪本機處理、故障隔離、完整分母對照與換版結果見
[10/18 實作驗收](tw_opening_latency_stages_2026-10-14.md#2026-10-18完整來源處理的增量投影與無等待批次合併)。
開盤／盤中／盤後的原生測速排程仍沿用下節，週日沒有發生的 09:00 樣本保持未驗收。

## 最新盤點與實測（2026-10-16）

**現在有權限且已實際讀回的仍只有永豐共用連線、官方 MIS、FinMind 三路。**
富果 REST adapter 已存在，但本機未設定 Fugle key；其他券商／企業 feed 尚未驗證帳號、
授權容量與正式連線。文件可查到不等於這個帳號已可用，不新增登入、不購買、不送券商委託。
下表與既有長名單涵蓋本輪能由官方文件查核的管道；未公開的機構合約不宣稱已窮盡。

新增查核的候選：

| 管道 | 正式開盤欄／介面與限制 | 本機狀態與判定 |
| --- | --- | --- |
| 玉山行情 API | REST `openPrice`／`openTime`、WebSocket；日內與快照各 600 次／分鐘，WebSocket 文件列 300 訂閱、2 連線 | 未開通驗證。基本與開發者權限的免費條件見官方 FAQ；不足以直接宣稱全市場推播。資料／技術同時報、群馥／Fugle，與富果、富邦不是完全獨立的主備。[行情](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/http-api/intraday/quote/)、[限制](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/rate-limit/)、[來源](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/intro/)、[申請](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/faq/intro/) |
| 台新 Nova API | Python／Node.js，跨平台 REST、WebSocket `trades`／`books`／`aggregates` | 官方已有股票行情；未取得權限、完整開盤 schema、容量／費用及獨立故障域證據，不宣稱已接通或全市場合格。[官方平台](https://ml-fugle-api.tssco.com.tw/FugleSDK/landingpage/) |
| 台新 MLAPI（原元富介面） | Python／C# 行情，`OpenPrice`、成交／五檔獨立時間、序號與試撮旗標；需完成線上行情驗證 | 未取得帳號與容量；文件列 TWSE／TAIFEX，TPEx 覆蓋須另確認。與 Nova 是同券商不同介面，不計為獨立供應商備援。[Python 行情手冊](https://mlapi.tssco.com.tw/web_api/service/document/python-quote) |
| 國票 API | 官方手冊提供股票報價與五檔 callback，Windows 64-bit／.NET 元件 | 手冊版本為 2023，現行服務、開盤欄、全市場容量及費用待券商確認；本機未實測。[官方手冊](https://www.ibfs.com.tw/wealth-ap/download/API_trade/ibfs-API.pdf?ver=20230426) |
| TWMD | 頁面標題稱接近即時，但 coverage 為 coming，注意事項又明列不提供即時報價／盤中分 K；回覆例也沒有正式開盤欄 | **規格互相衝突，排除於目前可用開盤來源**，不能只因 endpoint 名稱含 realtime 就加入模型。[供應商文件](https://twmarketdata.com/zh-TW/docs/api/market-prices/realtime-prices) |

其他已查核的來源仍包括：富果 REST／WebSocket、富邦 Neo、元大 SPARK、群益 SKCOM、
凱基、IBKR、TWSE／TPEx 授權 feed、國內資訊公司、LSEG、Bloomberg、ICE 等。
本機分鐘 K、FinLab `tw_tick`／`tw_minute` 與 TEJ 日開盤欄屬於盤後／歷史證據，
不能因為有 `open` 欄就當成 09:00 即時來源；MOPS 不是股價 feed。
[FinLab 定義](https://finlab.finance/docs/reference/data/)、[TEJ API](https://api.tej.com.tw/document_rest.html)。

已重新下載[10/16 官方簽約資訊公司清冊](../artifacts/operations/tw_opening_latency_20261016/official_vendor_roster.json)：
國內 23、國外 25、即時指數 9、延遲資訊 20 列。這不是 77 個可買的低延遲 API，
不把同公司跨組重複、展示商或延遲來源冒充獨立即時供應商。

### 同一完整分母的盤後網路實測

[18:15 三路實測](../artifacts/operations/tw_opening_latency_20261016/quote_phase_full_market_network.json)
均從空的隔離 receipt 開始，請求同一官方 scope 2,301 檔，保留失敗與缺價：

| 來源 | 首批有效開盤 ms | 來源工作完成 ms | 含來源保存 ms | 真實開盤／已證無首筆／未知 |
| --- | ---: | ---: | ---: | --- |
| MIS | 462.96 | 2,129.11 | 2,218.64 | 2,274／26／1 |
| 永豐共用連線 | 502.42 | 2,674.60 | 2,976.43 | 2,274／0／27 |
| FinMind | 1,169.34 | 1,283.94 | 1,622.53 | 2,274／0／27 |

合併 90% quorum 約 1,447.44 ms；完整排空來源、保存與最後讀回約 3,185.32 ms。
「來源工作完成」不是 100% 真正開盤價，也不是券商成交；不同來源沒有提供 no-first-print
證據時不能借用別家的數字列為自身完整。這是盤後取得**當日開盤欄**的查詢成本，
不是盤中最新成交／可執行五檔 freshness，也不能回推 09:00 第一輪供價。

原生測速單元的[18:23 兩路讀回](../artifacts/operations/tw_quote_phase_benchmarks/2026-10-16/postclose_182316141202.json)
也成功執行：MIS 首批 141.46 ms／來源完成 1,226.62 ms；FinMind 765.65／873.77 ms；
完整工作 1,367.39 ms。樣本條件不同，不能將兩次網路波動全算成程式加速或據此推導長期 P99。

### 完整率分母與剩餘邊界

本輪三路均缺的 `00838B`，ISIN 名冊仍列為上櫃；但已驗證官方七類 ETF 商品表的
日期、schema、每表 totalCount 與實際列數，聯集 118 檔，ISIN ETF 為 119 檔，唯一差異
正是此標的。[七類原始來源與雜湊](../artifacts/operations/tw_opening_latency_20261016/official_etf_scope_audit.json)。
這證明現行 ISIN-only 分母存在商品目錄差異，**尚未將新的生命週期規則部署到交易 owner**；
本輪沒有直接刪分母、回寫今日盤前名冊或重算原訊號，所以正式測速仍忠實保留 1 檔未知。
需要後續在同版本、可安全換版的 canonical universe owner 整合 ETF 目錄與原始保存，
不能用 provider 沒回價作為下市判定。完整 scope 修復與來源性能是不同驗收。

### 已啟用的三時段測速

- 開盤：原生流程被動記錄，observer 在 08:55、09:00:11、09:00:31、09:01 驗收；
  不另登入或增加一輪開盤 API 競爭。沒有新 schema 的舊工作明確列為未測得，不寫 0 ms。
- 盤中：交易日 10:30 做一次 MIS／FinMind 完整 scope 的空快取探測，10:31 被動彙整。
- 盤後：交易日 13:40 做一次探測；observer 在 13:40、15:10 續收原生證據。
  兩個 13:40 單元獨立，因此不能要求同秒 observer 立刻看見尚未結束的探測；探測自身即保存完整報告。
- 每個實際 job 有獨立 ID、完整分母 SHA、首批、50／90／95／99／100% 正開盤與 resolved 里程碑，
  排隊、HTTP／body／JSON、驗證合併、落盤、錯誤與逾時；按時段與來源彙整並保留失敗樣本。
- 永豐不加入定期 snapshot 輪詢；沿用原生 owner 的被動記錄。probe 的 missed timer 不補跑成
  假的歷史開盤測速；休市、日曆未驗收或 scope 不完整時留下 skip／waiting 收據。

資料保存在 `artifacts/live/tw_opening_snapshots/*.acquisition.jsonl`、
`artifacts/live/tw_day_trade_simulation/opening_audits/` 與 `artifacts/operations/tw_quote_phase_benchmarks/`。
上述 timings 是 opening-input 取得與處理，不能當成市場撮合、可成交 book 或 broker fill 證明。
10/19 開盤未達標與兩次盤中樣本見頂部及續修報告；今日 10:30／13:40 原生探測仍待發生。
不宣稱已達完整一秒或外部 API 永遠零錯誤。

## 10/13 原始結論與驗收

14:33 續作已在四模式清倉後部署；Discord connected／core ready，訊號、委託與成交帳本 SHA 不變。
現有三路仍是永豐共用串流優先、MIS／FinMind 並行補缺；**並未接通新的全市場串流，亦未達成一秒全量 SLA**。
本輪 2,303 檔完整離線對照（5 組交錯順序）保留所有價格／時鐘／旗標與進度；本機呼叫至第一份來源檔保存中位數由 285.95 ms 降至 150.26 ms，同時補上檔案與目錄 fsync。
詳見[現有三路續作與測速](#現有三路續作與測速2026-10-13-1433)及[工程／部署收據](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/acceptance.json)。

目前三路的「全市場 HTTP 快照＋缺口重試」不足以保證一秒內取得開盤輸入。
優先評估 **元大 SPARK 全市場預先訂閱**；其次為 **確認帳號總額度後的富果 WebSocket／企業方案**。
永豐保留交易標的即時行情與既有執行介面；MIS、FinMind 保留為補洞來源，不作一秒目標的必要前置條件。
這是依文件容量與資料語義的候選排序，**不是已證明哪家 09:00 最快／最穩**。

使用者新增要求已納入：受限來源可繼續供內部模型使用，但不可直接對外顯示價格。
公開價格應有獨立來源、展示授權與行情時間；缺少證據時先空白／等待，不改內部帳本或模型輸入。
「付費 API 權限」不等於「公開網站展示權限」。

## 今日異常：已知機制，不猜測封鎖原因

開盤原始 evidence：
[來源事件](../artifacts/live/tw_opening_snapshots/2026-10-13.acquisition.jsonl)。

| 今日時間 | 原始觀察 | 可以推論／不能推論 |
| --- | --- | --- |
| 09:00:00.129 起 | 永豐、MIS、FinMind 同時開始 | 並非依序等待三家；起始官方 scope 2,293，後續加入 10 個標的至 2,303 |
| 09:00:01.133 | 永豐首批收到 500 行，只有 12 筆有效開盤價 | HTTP／SDK 回覆成功不等於正式開盤輸入更新完成；不是證明另外 488 檔都沒有成交 |
| 09:00:01.676 | FinMind 首批有效開盤數 0 | 需分辨資料更新節奏、交易日、開盤欄與正式成交狀態，不能以「有價格數字」直接放行 |
| 09:00:04.773 | 合併有 92 筆正開盤、94 筆已解來源觀察／2,303 | 仍未達既有模型輸入門檻；未知不能冒充官方無首筆成交 |
| 12:39 有界診斷 | MIS 80 檔單次 2,692.566 ms | 原 1.5 秒逾時短於實際來源成本；單檔快不能外推全市場 |
| 13:07 有界重測 | MIS 單檔仍 `RemoteDisconnected` | 對端關閉 HTTP 連線；沒有足夠證據判定 IP 封鎖、WAF 或伺服器故障 |

MIS 原逾時／批次／缺口重試已在共用來源層修正，來源供價尚未恢復；部署與剩餘邊界見
[MIS 修復證據](tw_opening_latency_resilience_2026-10-13.md#1013-午後mis-全部失敗的分層診斷與修正)。
這份報告不把外部供價失敗改寫成「已修好」。

13:07 本輪有限唯讀測試：

| 管道 | 測試邊界 | 本次結果 |
| --- | --- | --- |
| 永豐共用連線 | 一檔 2330；不新增登入 | 140.811 ms；1／1 有效開盤 |
| FinMind | 一次全市場快照；與官方 scope 對齊 | 645.752 ms；2,264／2,303 有效開盤，39 未解 |
| MIS | 一檔 probe；失敗後不送全市場突發 | 1,374.153 ms；`ConnectionError → ProtocolError → RemoteDisconnected` |

[實測收據](../artifacts/operations/tw_opening_provider_plan_20261013/access_probe.json)
保留 Taiwan 時間、請求範圍與延遲。這是單次午盤存取測試，不是 09:00 覆蓋／P95／P99、長期可用率或成交 SLA。
其他需帳號的來源僅查官方文件，未冒稱實測，未購買、註冊、寄信或券商下單。

## 可以取得行情的管道清單

以下「全市場」必須以當日官方交易清單為準，不能固定寫死 2,303，亦不能拿 API 回應行數當分母。
ETF、特別股、創新板等須按實際官方 scope 對照；只選普通股可能漏標的。
分項查核日期不同：Shioaji、FinMind、Fugle、富邦、元大、玉山、群益規格已於 10/22 重讀；
其餘費用是 10/13 查核觀察，未重測現價，不當成現在報價。未列開戶、憑證、專線、
非展示使用、再散布或另外的交易所費用；需依正式權限／報價確認。

| 管道 | 介面／容量或更新限制 | 費用／權限 | 本專案定位與穩定性證據 |
| --- | --- | --- | --- |
| 官方 MIS | 網站 HTTP 行情；官方歷史介紹描述五秒快照，未找到全市場逐筆開發 API 的 SLA | 網站免費；API 大量取用與對外轉送不能由免費瀏覽推定 | 補洞／交叉核對。今日先能讀回、後被對端關閉；不能當唯一主源。[官方介紹](https://www.twse.com.tw/staticFiles/product/publication/twse60/P8.pdf)、[使用問答](https://www.twse.com.tw/zh/products/information/qa.html) |
| 永豐 Shioaji | 即時訂閱上限 200；行情查詢共 50 次／10 秒；官方明確不允許以反覆 snapshots／ticks／kbars 輪詢替代即時行情 | 券商帳戶／API 權限；查詢流量依成交級距，推播不計流量 | 已有共用連線；適合實際交易標的，不足單獨推播當日官方全量。不得靠多登入規避限制。[使用限制](https://sinotrade.github.io/zh/tutor/limit/) |
| FinMind 股票快照 | 全市場一次請求；約 10 秒更新 | Sponsor NT$999／月；Sponsor Pro NT$3,330／月；免費／Backer 不含此股票即時權限 | 已有可用權限；補洞／稍晚交叉核對。提高請求額度不會把 10 秒更新變成逐筆推播。[資料說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)、[價格與授權](https://finmind.github.io/Pricing/) |
| 富果 REST Snapshot | 按市場整批；每五秒更新 | 開發者或進階方案 | 比逐檔請求省成本，但不足保證一秒目標；不是串流替代品。[Snapshot](https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/) |
| 富果 WebSocket | 免費 5 訂閱／1 連線；開發者 300／2 連線；進階每連線最多 2,000、最多 2 連線；股票×channel 占一訂閱 | 開發者 NT$1,499／月；進階 NT$2,999／月；企業方案另詢價 | 10/18 重讀官方每連線定義，文件容量可覆蓋目前 scope 的單一 channel；實際帳號方案、全量 ACK 與開盤事件仍待驗收。本機 key 不等於已買此方案。[定價](https://developer.fugle.tw/docs/pricing/) |
| 富邦 Neo | WebSocket 單連線 300、同時 7 條：最多 2,100 訂閱槽；REST 日內／快照 300 次／分鐘 | 需券商 API 開通；未確認獨立行情月費，不能宣稱免費 | 可作部分備援；預設額度不足 10/22 的 2,309 全量。Speed mode 是性能候選，需確認保留必需欄位後才測；未登入測試。[限制](https://www.fbs.com.tw/TradeAPI/docs/market-data/rate-limit/)、[模式](https://www.fbs.com.tw/TradeAPI/docs/market-data/websocket-api/upgrade-guide/) |
| 元大 SPARK | 單連線 2,000、帳號合計 3,000，最多 10 連線；指定欄位訂閱按股票×欄位計數 | 需帳戶、API 申請與正式權限；官網無資金／交易量門檻，但不能因此推定所有費用為零 | **優先測試全市場候選**：兩條授權連線分片，若 `WatchlistAll` 每檔只占一額度，容量可涵蓋目前 scope。須實測正式開盤欄、delta 重組及 ACK。[限制](https://www.yuanta.com.tw/file-repository/content/sparkapi_docs/1.%E5%89%8D%E8%A8%80/3.%E4%BD%BF%E7%94%A8%E9%99%90%E5%88%B6%E8%AA%AA%E6%98%8E/index.html)、[平台](https://www.yuanta.com.tw/file-repository/content/API/page/index.html) |
| 玉山行情 Web API／Socket | 日內／快照 600 次／分鐘、歷史 60 次／分鐘；官網列 WebSocket 300 訂閱、2 連線 | 需帳戶與 API 行情權限；費用及每連線／帳號計額仍待正式確認 | 有聚合開盤欄／市場快照，預設容量沒有全市場證明；本機未登入測試。[限制](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/rate-limit/)、[API](https://www.esunsec.com.tw/trading-platforms/api-trading/docs/market-data/http-api/getting-started/) |
| 群益 SKCOM 等 | 國內行情初始 100 檔、五檔 10 檔、兩條連線；可洽營業員調整。SKCOM 涉及 Windows 元件 | 帳戶、API／行情權限；擴大額度、費用待正式核准 | 預設不能當成全市場串流；未取得擴額、來源時鐘及實測，不列作已達標來源。[官方行情物件與限制](https://www.capital.com.tw/apiforum/docs/apiCOM/quoteObj_SKQuoteLib) |
| 台新 Nova／MLAPI、國票與其他券商 SDK | 行情功能、目前 SDK 名稱及全量容量須分別由官方技術文件／帳號確認 | 帳戶、API 開通、行情使用權限；不因有下單 API 推定有完整報價 API | 候選，未取得正式授權／訂閱 ACK，不列為可用來源；沒有以本機不存在的憑證假測延遲 |
| 凱基與其他券商 API | 券商提供 API 不一定等於有全市場逐筆行情訂閱 | 按券商方案確認 | 未確認的容量、價格、來源語義列待查，不以名稱推定可用。[凱基平台](https://www.kgi.com.tw/zh-tw/support-index/trading-platforms-overview/individual-trading-platform7) |
| IBKR TWS／Gateway API | 行情線數由帳號權限決定；桌面與 API 共用，不能以多 API 連線倍增 | 10/13 查核價目曾列 TWSE L1 USD1／月、TPEx L1 TWD15／月；不是 10/22 即時報價，申請前重核目前 API/off-platform 費用、權限及線數 | 有台灣市場候選，但低單價不等於全市場容量；未登入實測。[費用](https://www.interactivebrokers.com/en/pricing/market-data-pricing.php)、[線數](https://www.interactivebrokers.com/docs/general/market-data-subscriptions/market-data-lines/introduction)、[API 權限](https://www.interactivebrokers.com/docs/general/market-data-subscriptions/tws-data-vs-api-data) |
| 證交所／櫃買中心直接資訊連線 | 官方機器資訊 feed、主備連線；須分別涵蓋 TWSE／TPEx | TWSE 官網非券商傳輸授權費 NT$60,000／月；**不是全包個人方案**，另有資訊費、網路、用途與 TPEx 契約 | 可控制更多傳輸環節的企業級候選；仍需量測，不保證全標的有首筆成交或一秒成交。[TWSE](https://www.twse.com.tw/zh/products/information/real-time.html)、[TPEx](https://www.tpex.org.tw/zh-tw/service/data/product/regulation.html) |
| 國內資訊公司 | 三竹、嘉實／XQ、精誠、時報資訊、奇唯、凱衛、寶碩、CMoney 等；是否提供可購買機器介面須逐一確認 | 企業授權、SDK、專線或專案詢價 | 官方名冊可作完整詢價長名單，但不能把「名列資訊公司」當成「已有全市場低延遲 API」。[名冊](https://www.twse.com.tw/zh/products/information/list.html) |
| LSEG Real-Time／Bloomberg B-PIPE | 企業 push feed／SDK／雲端或專線 | 依股票市場、非展示用途、容量、對外展示權利詢價 | 有企業機器行情產品，需分別確認 TWSE＋TPEx 包含於合約與是否逐筆、是否合併節流。[LSEG](https://www.lseg.com/en/data-analytics/market-data/data-feeds/real-time-full-tick-data)、[B-PIPE](https://professional.bloomberg.com/products/data/enterprise-catalog/real-time-data-feed/) |
| ICE Consolidated Feed／其他國際供應商 | 訂閱推播，容量由帳號／契約決定；ICE 曾公告台灣在地 Shared Farm | 企業報價；在地方案的現行可售範圍與 SLA 待確認 | 企業候選，不把 2022 公告當成 2026 實測。[API](https://developer.theice.com/hc/en-us/articles/200717514-About-the-Consolidated-Feed-API)、[台灣服務公告](https://ir.theice.com/press/news-details/2022/ICE-Launches-Plug-and-Play-Connectivity-to-Managed-Data-Feed-in-Taiwan/default.aspx) |
| 桌面報價／DDE | GUI／Windows session／DDE 才能送資料；能力與掛牌來源各異 | 桌面軟體與對外使用權限須確認 | 可為備援候選，重開機／登出／GUI 故障面較大，不作首選；未量測 |
| Yahoo／Google／TradingView／新聞看盤頁 | 可顯示行情，不等於供全市場機器抓取的授權 API；可能即時或延遲，依產品而異 | 顯示、取用、再散布權利不同 | 不將未公開授權的網頁端點或爬蟲當一秒主源，亦不籠統宣稱各頁全部延遲 |
| 本機分鐘 K、TEJ／FinLab 歷史、官方日資料／OpenAPI | 保存歷史、參考檔與已完成分鐘 | 各資料授權 | 有助盤前準備／盤後驗證／明確回放，但不能用未來完成分鐘 K 冒充 09:00 當時即時觀察 |

完整官方長名單已直接讀回：[簽約資訊公司收據](../artifacts/operations/tw_opening_provider_plan_20261013/official_vendor_roster.json)，
13:15:15 有 23 家國內資訊公司、25 家國外資訊公司，另列 9 家即時指數與 20 家延遲資訊。
這些是分組列數，不是去重後 77 個逐筆 API；電視台／資訊展示商也在名冊內。
國際長名單包括 LSEG／Refinitiv、Quick、Bloomberg、ICE、SIX、FactSet、Morningstar、Pico、TNS 等。
**私有方案無公開規格的部分保留待確認，不宣稱所有廠商已測試或永不失敗。**

### 多來源不是同等獨立

富果由時報資訊與 Fugle 團隊提供；富邦證券行情也使用時報資訊／群馥，並由 Fugle 團隊開發。
因此富邦＋富果可提供不同入口，但不算完全獨立的供應商雙備援。
不同供應商最後仍可能共用同一交易所、網路節點或資料分發平台；要查中間故障面。
[富果來源](https://developer.fugle.tw/docs/data/intro/)、[富邦來源](https://www.fbs.com.tw/TradeAPI/docs/market-data/intro/)。

## 第一性原理：要縮短的是哪一段

```text
交易所正式首筆成交
  → 資料發布／供應商轉送 → 本機收到 → 驗證與合併
  → 原模型輸入條件達成 → 推論／持久發布
  → 執行介面受理 → 交易所撮合／券商 StockDeal
```

「09:00 到完成」包含首筆成交何時產生；「資料發布到本機」則是另一個可比較的傳輸延遲。
開盤延後、停牌、未成交時，正式開盤價還不存在，任何 API 都不能提供不存在的事實。
試撮價、昨收／參考價、bid／ask 中點也不是正式開盤成交價。
重連後收到的第一筆普通成交，不一定是該股票今天第一筆，需明確開盤欄／旗標或可追溯補缺。

定期快照還有更新等待：若更新週期是 Δ，理想化任意相位可能多等約 0～Δ；均勻相位下均值 Δ／2。
這是機制分析，不是這次實測的延遲分布。增加並行只能減少本地排隊，不能把來源的 10 秒更新變成逐筆。
一秒目標必須以 **盤前已建立的正式串流**為主，而不是 09:00 才登入、下載合約或掃描全部檔案。

## 分階段方案與採購／測試順序

| 階段 | 做法 | 完成證據／邊界 |
| --- | --- | --- |
| 現有權限 | 保持共用永豐連線，成功事實不丟失；MIS 限缺口重試／來源局部退避；FinMind 按來源節奏補缺 | 三路仍有價值，但不承諾全市場一秒；避免官方禁止的 snapshot 輪詢與反覆登入 |
| 優先帳戶候選 | 元大 SPARK：盤前兩條合法連線分片；優先完整 quote table，不把每欄重複訂閱放大額度 | 要先取得 API 權限、SDK／.NET8 相容性、正式訂閱 ACK 與已訂閱清單；不是尚無憑證時冒稱已接通 |
| 訂閱式候選 | 富果進階先確認跨連線帳號總額度；不足則詢價企業方案 | 一檔一個 `aggregates` channel 有開盤欄、時間、買賣盤與試撮／延開訊號，需測推送更新方式及容量；不可用多帳號繞限制 |
| 強主備 | 一個有全量額度的主推播＋不同供應商的全量備援；持續並行接收、先達原輸入條件者釋放模型 | 富邦＋富果不是完全獨立；逐筆、訂閱容量、來源時鐘、缺序補發與權限均須驗證 |
| 企業極低延遲 | 比較 TWSE＋TPEx 直接 feed、國內 licensed feed 與國際在地 feed | 依正式報價／非展示使用與公開展示授權／09:00 P99 決定，不能先宣稱最貴最快 |

候選串流接入時，沿現有共用 collector／fan-in 擴充，不另建一套模型／帳本：

- 08:30 前準備當日官方 universe、資格、除權息／參考檔版本、price grid、模型與資金，並完成登入與訂閱。
- 訂閱容量至少覆蓋整個官方 scope，保留合理成長餘量；成交與五檔是否各占額度按實際帳號驗證。
- callback 僅解析／更新本機事實／喚醒 consumer，不同步做網路、模型、日誌逐筆 fsync；模型訊號與帳本仍保留原持久性、冪等性與因果契約。
- 保存股票別正式開盤事實、source event clock、received clock、序號、試撮／延開／停牌狀態；價格與成交量單位不可混同張、股、口。
- 分片 delta 來源先重組欄位，不能把舊欄位跟新時鐘拼成假的完整快照。
- 多源衝突保留原值與證據，不平均；來源慢／失敗不取消其他來源已成功的結果。
- 斷線重連要完成 auth／訂閱 ACK／清單讀回與缺序補洞；socket connected 不等於資料完整。權限／429／對端拒絕做該來源退避，不讓全系統進入重試風暴。
- 照既有模型的價格／mask／quorum 契約釋放推論，不為達標偷減分母、拿中點替代正式開盤、改模型或補造成交。

## 09:00 驗收與持續測速

至少跨 10 個真實交易日測試，先 shadow／只讀，不新增真實委託；測試環境不能替代正式開盤節點性能。
每檔記錄交易所事件、來源發布、收到、驗證，另記 quorum、推論、原子發布、執行受理、真實成交回報。
本機耗時用 monotonic clock；跨主機／交易所時鐘誤差要單獨量測，不把兩個不同邊界相減冒充 CPU 耗時。

驗收表至少含：

- 每源與合併 09:00＋100／250／500／1,000／2,000／5,000 ms 的正開盤數、已解狀態數與未知數；同一官方 scope。
- 可取得報價的 source-to-receive／quorum／inference／publication P50、P95、P99；失敗與超時也納入，不只統計成功樣本。
- 確認已正式成交的標的覆蓋，並把延開／停牌／未成交與未知分開，不能把 provider 缺價全部叫做市場沒成交。
- 訂閱 ACK 完整率、丟序／補缺、斷線恢復、權限到期、重試量、CPU／記憶體、時鐘偏移。
- 若要宣稱一秒「送出／受理」，只對輸入已足夠且可下單的原模型目標統計；「全市場都有開盤價」和「所有券商成交」不是相同 SLA。

目前未取得新來源的實測與正式帳號／價格確認，不宣稱毫秒極限或一秒已達成。

## 公開展示邊界：模型與網站分離

FinMind 本日條款明列：即使 Sponsor Pro 也不能再散布原始資料／建立鏡像 API，亦不能把即時資料直接顯示於對外 web／App。
因此不能把購買 Pro 當公開面板行情展示的解法；其他供應商的 API／非展示／再散布權限也分別確認。
[FinMind 授權](https://finmind.github.io/Pricing/)。

本輪共用實作：

- [公開價格政策](../stockagent/live/public_quote_display.py)與[設定](../configs/public_quote_display.json)：預設無已確認的公開來源，價格先不傳送。
- [公開投影](../stockagent/live/public_dashboards.py)：當沖／隔日沖的 status、signals、positions、events，巢狀 signal position 及 TAIFEX status 一起處理，不只用 CSS 隱藏。
- 保留訊號、股數、執行狀態、既有損益與分鐘曲線；來源原值不被修改，內部模型／帳本不讀公開替代行情。
- 價格待補不能當作零名目金額，也不能讓已平倉顯示成持倉中。

將來授權來源以 canonical collector 的 **獨立公開價格觀察**補值，不能把受限價格重新貼來源標籤。
`public_price_observations[field]` 必須含 `value`、`price_role`、`symbol`、`venue`、含時區的 `observed_at`、`source_id`、`grant_id`。
每個 source grant 在設定內有 `source_id`、`grant_id`、`public_label`、`license_reference`、`public_display=true`、
ISO `valid_from`／`valid_until`、`venues`；只有本機可信設定能授權，payload 的 `public_display_authorized=true` 無效。

公開來源的角色／股票／交易日／venue／時間／價格均要匹配；來源與時間一同顯示。
記錄成交／進出場／委託／計價價格不能被另一筆不同價格替換，只有公開觀察驗證相同值才揭露；
另源的現價只是公開行情，不改內部估值與損益。
授權到期或政策變更須重新載入公開讀取服務、清除舊編碼快取；不要重啟交易引擎或登入券商。
這是技術揭露邊界，不代替供應商確認模型／商業／非展示用途的許可。

本輪部署與驗收收據：
[公開價格邊界驗收](../artifacts/operations/public_quote_display_boundary_20261013/acceptance.json)。
分析任務與公開展示修改分開記錄；完成這兩者不等於 MIS 已恢復、全部上游健康或新的 09:00 SLA 已通過。

### 本輪工程驗收

- 181 個公開投影／價格政策測試、87 個頁面／歷史／生命週期測試通過；47 個 Node 共用元件測試通過。這不是全庫測試。
- [API 回讀](../artifacts/operations/public_quote_display_boundary_20261013/api_readback.json)：12 個本機出口與 DDNS IPv4 HTTPS 訊號頁均未傳出受限價格；包含巢狀持倉、事件及歷史。DDNS 測試由本機發起，不代表獨立外網或 IPv6 驗收。
- 真實私有訊號／持倉 DTO 比較：原值未被 sanitizer 修改，股數及損益保留。每 2 筆資料、10 次局部序列化樣本的中位數分別為 0.839 ms／0.171 ms；不是全市場或開盤端到端延遲。
- [瀏覽器回讀](../artifacts/operations/public_quote_display_boundary_20261013/browser/readback.json)：當沖、隔日沖與 TAIFEX 各測 1366×768／390×844，6 組均無 JavaScript 錯誤與 body 橫向溢出；查看價格待補與原有狀態，不把 degraded／blocked 改成正常。使用明確 `cpu-2d` profile，不宣稱 GPU／WebGL 驗收。
- 手機版原先把新提示當一般副標隱藏；已改共用獨立 disclosure 樣式並加回歸測試。CSP 保持不允許 `unsafe-eval`；瀏覽器測試用實際函式與正常載入／繪製完成條件。
- [部署回讀](../artifacts/operations/public_quote_display_boundary_20261013/runtime_after.json)：只重新載入公開 reader；當沖、隔日沖、Discord invocation 不變，未新增券商登入或委託、未修復／重寫帳本。當前公開價格授權設定為空，**新公開行情來源尚未接通**。

## 現有三路續作與測速：2026-10-13 14:33

### 可以立即運作的方案

盤前沿用官方當日名冊、資格／參考檔、共享永豐登入、合約與訂閱準備；
09:00 三來源沿同一 fan-in 並行，正式來源觀察先累積，達原模型門檻後持久保存再推論，慢來源不阻擋已成功來源。
缺口、正式零成交觀察與未知分開；剩餘缺口繼續由原來源 owner 修復，不能變成假的價格、假的現金持倉或新的模型權重。

| 現有來源 | 正確用途 | 實際限制／目前狀態 |
| --- | --- | --- |
| 永豐共用連線 | 盤前訂閱、原生正式 quote 優先；不另外登入 | 推播額度 200，不足整個名冊；Snapshot 是查詢型來源，不得用高頻輪詢替代全市場推播。本輪沒有提高查詢頻率 |
| MIS | 官方公開欄與明確 no-first-print 的補缺／核對 | 14:33 單檔 probe 仍被關閉連線；只對這個來源判失敗，診斷不再送全市場突發 |
| FinMind | 現有權限一次全市場快照，按來源更新節奏補缺 | 約 10 秒更新；提高本機並行度不會改變更新頻率，且原始即時價仍不對外展示 |

200 訂閱與每 10 秒 50 行情查詢的限制／合理使用規範：
[永豐官方文件](https://sinotrade.github.io/zh/tutor/limit/)。
全市場快照與約 10 秒更新：[FinMind 官方文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)。
若要「已發布的全部標的開盤價」穩定次秒接收，仍要另取得完整官方 scope 容量的合法串流；
延開／停牌／尚未首筆成交的股票沒有正式開盤價，不能承諾在 09:00 取得不存在的值。

### 共用實作修正

- 正式開盤事實與最新 book 分開保存；後續試撮、缺欄、輪轉、同日重連不再擦掉開盤證據。保留原接收／市場時鐘；新連線不會把舊 book 變成新鮮可成交報價。真正的開盤價變更仍被觀察，canonical cache 保留衝突，不平均價格。
- Snapshot 原生 callback 仍只交付結果，解析在 worker；每列解碼一次，累積進度與最終輸出共用解碼結果。亂序批次仍保持原批次優先次序，失敗／超時／late callback fence 不移除。
- 正式新來源欄位、接收時間或請求範圍更新仍保存；完全無變更的重試不再重寫整個來源檔。
- 修正原來源檔僅 atomic rename 的斷電缺口：沿用 `downloader.artifact_io`，成功回傳前完成 file fsync 與 parent directory fsync。同步失敗保持 dirty、拒絕成功宣告並可重試。
- `np.float64` 直接轉為同位元 Python float，避免逐格字串來回轉換；bool、float32、Decimal 與其他來源型別仍維持既有語義。以極小值、最大有限值、隨機值與 float32 反例檢驗。
- 保存解碼、來源診斷、快取合併、來源總耗時等巢狀測速；新增 100／250／500／1,000／2,000／5,000 ms 覆蓋稽核。今天早上只有舊聚合日誌、逐檔 schema 12:53 才開始，輸出 `insufficient_evidence` 與 null，**不會把缺少逐檔證據寫成零報價**。
- 正常 `--runtime` 不掃描 5.2 GB 原始帳本；深度重載比對必須明確指定 `--ledger-fingerprints`。

本輪沿共用取價／collector 做修改，不另建模型、帳本或登入流程；原模型 ABI、官方分母、quorum、資金與成交政策保持不變。
內部取價與公開展示分離的政策持續有效。

### 完整軸離線對照

[測速與逐組 SHA](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/comparison.json)
載入實際修改前的程式副本，不以自造慢路徑充當 baseline。
2,303 檔、5 組前後交錯順序，每組全部 batch 都完成；SDK／來源為明確 synthetic、零網路、零生產帳本寫入。

| 中位數／工作量 | 修改前 | 修改後 |
| --- | ---: | ---: |
| 本機呼叫到最終 source DTO | 175.84 ms | 71.72 ms |
| 本機呼叫到第一份來源檔保存 | 285.95 ms | 150.26 ms |
| 5 組相同進度＋既有 scope seed，共 10 次 | 960.20 ms | 161.30 ms |
| 每輪原始 row 解碼次數 | 9,606 | 2,303 |
| 無變更來源檔重寫 | 10 | 0 |
| 首份來源檔 file／directory 同步 | 0／0 | 1／1 |

所有價格、量欄、時間戳、trial flag、每批進度、來源識別與保存 row 值 SHA 完全一致。
before 原本沒有 fsync，after 的測速已包含新增持久性成本；不把減少同步假裝成加速。
這不是市場傳輸延遲、推論／委託／券商成交耗時、P99、理論極限或明日 09:00 SLA。

### 14:33 真實來源／部署驗收

[唯讀存取證據](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/access_readback.json)：
使用同一官方 2,303 軸與全新空暫存，正常現有共用 broker，不新增測試登入、不發布訊號、不送券商委託。

- 單檔 MIS probe 失敗隔離後，兩個健康來源的 fan-in 於 1,143.04 ms 回傳原 quorum，完成於 1,569.87 ms；此計時不含前置 MIS probe、模型或委託。最終有 2,276 個正式開盤值、27 未知、0 值衝突，不冒稱 100%。
- 永豐 2,302 來源回應，2,276 有效；其餘 26 市場時間陳舊／開盤前、1 無觀察。沒有證據把 27 檔一概稱為「當日沒成交」。Snapshot 的 2,302 筆 trial flag 仍 unknown，不升格成可成交 book。
- FinMind 2,275 有效、28 無觀察。MIS probe 仍 `ConnectionError → ProtocolError → RemoteDisconnected`；沒有足夠證據宣稱封鎖原因。
- IPC 明確讀回 `single_decode_per_callback_row_v1`、`formal_open_separate_from_latest_book_v1` 與 Taipei wall-clock normalization，證明不是僅改磁碟檔、未載入服務。
- 盤後這次 IPC queue 442.36 ms、永豐 server provider 875.07 ms；這是盤後一般 polling 邊界，不能當成 09:00 hot gate 的排隊量。其他來源與原 quorum 仍先完成。
- 285 個 Python 來源、恢復、持久性、排程、benchmark 與期貨回歸通過；不宣稱全庫測試。
- 14:31:44 通過既有四模式全清倉 gate 後，重新載入 Discord／當沖共用取價 owner；14:33 Discord connected、core ready、ledger integrity ready／divergence 0。正常休市／盤後 waiting 保留，不改成 ready。
- [重載前](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/runtime_before.json)與[重載後](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/runtime_after.json)三份 signals／orders／fills 的 SHA 與大小完全相同；未重算／替換帳本、未改當日原訊號或 broker fill 語義。
- 公開當沖 status／頁面依現有 reader 回讀 HTTP 200；只證明可連線，不能代替明日來源完整性。前一節限制價格隔離仍保留。

重測入口（不推論、不交易）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-14 --runtime \
  --output artifacts/operations/tw_opening_20261014_audit.json
```

明日須用新日誌驗收當日官方 scope 與真實 09:00 分位數／覆蓋；目前多交易日 SLA、MIS 外部恢復與完整市場推播權限仍未完成。
