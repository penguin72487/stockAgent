# TW 當沖開盤延遲與故障隔離修復：2026-10-13

## 結論與範圍

本輪從 10/13 凌晨檢查最近一次 10/12 真實開盤，修正「取價→四模式訊號→本機模擬帳本」的排隊、測速歸因、等待超時與故障恢復。沒有重新訓練、切換模型、修改歷史訊號／成交／委託帳本、放寬資料驗收或送出券商委託。

凌晨階段有 402 個受影響 Python 測試通過。新版 Discord 與當沖引擎已在盤前部署；Gateway 連線、帳本完整性及跨服務同步均有讀回證據。重啟前後三份帳本 SHA 與經濟狀態投影完全相同。盤中續修後擴大驗證為 **502 passed**，詳見下方「10/13 真實開盤與盤中續修」。

**整段 1 毫秒或真實開盤後 1 秒內全部完成仍未達成。** 本機回呼喚醒已達亞毫秒中位數，但完整四模式訊號發布約 1 秒，合成紙上帳本探針約 1.89 秒，外部價格等待與券商／交易所成交不包含在這些離線結果內。凌晨驗收時 10/13 尚未開盤；現在已有真實盤中樣本，顯示資料覆蓋等待仍是主要阻塞，不能用凌晨離線結果宣稱今日開盤成功。

## 從證據定位瓶頸

第一性原理的依賴順序：合法同日資料可取得 → 接收並驗證 → 不可變模型輸出 → 原子訊號發布 → 取得符合訊號後時鐘的可執行報價 → 模擬帳本落盤。後一步不能替前一步製造證據。

10/12 原始來源檔：[2026-10-12.acquisition.jsonl](../artifacts/live/tw_opening_snapshots/2026-10-12.acquisition.jsonl)。

| 10/12 時鐘／階段 | 實際證據 | 意義 |
| --- | --- | --- |
| 09:00:00.074 左右 | 永豐、MIS、FinMind 已同時啟動 | 並不是三路最初被串行發出 |
| 09:00:00.869 | 永豐首批 500 回應，但只有 6 個有效開盤價 | HTTP／SDK 回應數不是有效價格覆蓋 |
| 09:00:01.870 左右 | 永豐第一輪 2,299 回應、114 個有效開盤價 | 單一路仍不足以達到既定模型輸入契約 |
| 09:00:01.484 | FinMind 4 個有效開盤價 | 此次來源沒有提供全市場覆蓋 |
| 09:00:04.613 | MIS 0 回應、0 有效價 | 主要來源缺口，不能當作市場沒有成交 |
| 09:30:11.502 | 2,071／2,299 個有效價，門檻 2,070 | 當次價格 quorum 到這裡才通過 |
| 09:30:12～16 | 四模式發布 | 單次模型計算是數十毫秒，不是 30 分鐘 |
| 09:38:55.294 | 1 億模式登記帳本 | 超過 09:00:15 後進入既有 09:01 回放，需要補齊逐檔歷史價格 |

舊 `artifact_discovery_ms=520439.542` 混入逐檔補價等待，並非真的花 8 分多鐘發現訊號檔案。本輪修正歸因，不改寫歷史測速，也不宣稱消除了外部歷史資料查詢成本。

## 已實作

| 共用元件 | 修正 | 不變的邊界 |
| --- | --- | --- |
| `services/discord_bot/bot.py` | 資料準備和 canonical 取價可跨模式並行；準備好的模式先進模型，不等慢來源；每個失敗保留自己的重試。模型 owner 重驗完整價格 fingerprint 與 age。 | 模型全域 runtime 仍互斥；每日不可變訊號、完整輸出與報表都保留 |
| 同上 | 盤前／開盤並行 input stage 也納入既有無進展 watchdog；取消時清理 pending tasks。 | 沿用 60 秒 hot／180 秒 cold 下限，沒有在正常有界 I/O 中途任意重啟 |
| `stockagent/live/opening_price_acquisition.py` | 來源個別退避 0.5→1→2→4→8→15 秒；權限／配額／429 錯誤退避 60 秒；FinMind 10 秒、Fugle 5 秒刷新下限仍保留。 | 不清掉健康來源已取得的資料；不新增全域 rate limit 或全域 32 併發上限 |
| 同上 | 永豐／MIS／其他來源獨立進展；將來源落盤移出 session condition lock，保存前仍做 generation fence；測速 JSONL flush＋fsync。 | 入模前接受的輸入仍由 canonical receipt 持久化；來源衝突不平均 |
| `stockagent/live/quote_provider.py` | MIS probe 必須包含要求的 2330 列；空 HTTP 200 不再續期準備完成狀態；失敗的 forced probe 清掉舊 transport proof；破損 HTTP worker connection pool 可重建。 | 盤前 transport-ready 不是當日開盤價-ready；收到前日行情不冒充當日價格 |
| 同上 | 本機 IPC 客戶端與 partial progress 等待改用 monotonic deadline。 | 跨程序請求仍帶 wall-clock TTL；報價來源／交易所時鐘不變 |
| 同上、`scripts/run_tw_day_trade_simulation.py` | 有待成交標的的新非試撮報價，透過合併的 nonblocking pipe 喚醒單一執行 owner；包括缺價尚未算股數的等待。 | 回呼不做網路、落盤、下單；執行仍驗證價格、時鐘、漲跌停、股數及資金；原有定時檢查仍為備援；回放不借用 native book |
| 模擬引擎／runner | 固定第一次看到 pointer 的時間；增加 `signal_commit_attempt_at`、`source_recovery_wait_ms` 和等待原因。 | 慢等待仍計入真實總延遲，不移掉失敗樣本或把等待重新歸零 |
| `stockagent/live/signal_engine.py` | 完整 JSON 改 compact，去掉非資料的縮排成本；共用 writer、唯一 owned temp、檔案及 Linux receiving directory 同步後發布。 | 所有欄位、浮點精度與每個 caller 原有 NaN 政策不變；嚴格 execution JSON 仍拒絕 NaN；fsync 失敗不得覆蓋舊 artifact |

仍然區分正常 `causal_market_full_target_at_best_quote` 與超時後明示的 09:01 分鐘成交量回放。沒有以優化之名刪除 eligibility、PIT、費用、整張、價格界線、missingness 或來源證據；沒有新增 50／50 多空要求。

## 測速：每個邊界分開報告

主 benchmark 固定同一份 10/12 receipt、`feature-date=2026-10-08`、四個目前部署的完整模型。每個 variant 都做模型／價格／解釋 digest parity、完整 Parquet／Markdown artifact 驗收及獨立程序的紙上帳本探針。receipt SHA：`afcc617b332ce2010b2a4288e7753b3c8a5f0a28fc4efa322921ae263cdf9c9c`。

| 邊界 | 控制組 | 新版／候選 | 樣本與限制 |
| --- | --- | --- | --- |
| 純熱快取，順序／並行 input | 866.22 ms | 951.41 ms | 各 4 次；並行慢約 85 ms，**沒有宣稱此快取情境加速**。並行的作用是隔離慢 I/O，非消除 GIL／prepare 開銷 |
| 四模式發布：pretty／compact，同樣落盤保障 | 中位數 1116.57 ms | 中位數 1013.34 ms | 各 10 次平衡順序；中位數改善 9.25%，全樣本平均 1147.12→1077.99 ms |
| 同一測試發布 P95／最大值 | 1471.92 ms | 1510.03 ms | 新版尾部仍略慢；只有 10 次／組，不能推論生產 P99 或硬即時保證 |
| 四模式完整 artifacts | 中位數 2621.23 ms | 中位數 2375.74 ms | 沒有省略報表、Parquet 或驗收 |
| 合成 book 的全部紙上帳本探針 | 中位數 1971.20 ms | 中位數 1891.45 ms | 包含實際 sizing／費用／canonical ledger persistence；books 是 engineering fixture，**不是即時行情或券商成交** |
| native callback→執行 thread 喚醒 | 無對等舊事件路徑 | 中位數 0.0714 ms；P95 0.2890；P99 1.3472；最大 3.6867 | 500 次合成回呼，本機 thread／pipe；不含模型、來源請求或帳本完成 |

第一次 publication 比較另保留 2,288.93 ms 的較慢候選樣本於 `publication.json`，沒有用加長測試刪掉它。`baseline.prof` 的 2,804.78 ms 基準有 cProfile 開銷，不能拿來與無 profiler 結果做百分比比較。初始完整 panel 重建約 28～30 秒出現在 logs、位於 warm-round 之外；沒有把冷啟動偽裝成熱快取測速。測試時本機 16 CPU、load average 約 11，未中止其他服務／agents／資料工作以美化結果。

證據：

- [完整測速摘要](../artifacts/operations/tw_opening_latency_20261013/performance_summary.json)
- [20 次平衡測試與各階段](../artifacts/operations/tw_opening_latency_20261013/publication_long.json)
- [首輪較慢樣本](../artifacts/operations/tw_opening_latency_20261013/publication.json)
- [input 排程控制／候選](../artifacts/operations/tw_opening_latency_20261013/parallel_inputs.json)
- [500 個喚醒樣本](../artifacts/operations/tw_opening_latency_20261013/quote_wakeup.json)

## 檢驗與部署

凌晨階段最終受影響回歸：402 passed，14.29 秒，run `opening-complete-final-verificat-20261012T174442-15e715b0`。包括 source fan-in、來源失敗退避與恢復、互斥模型、慢來源超車、任務取消、空 MIS probe、凍結 wall clock、partial IPC、試撮／舊報價、inotify fallback、未算股數等待、回放隔離、嚴格 JSON、原子落盤失敗及帳本契約。沒有宣稱整個 repository 全測試通過。

兩次 targeted engine 部署與一次 Discord 部署皆在 10/13 凌晨平倉後執行；第一次做共用變更，第二次補進未算股數等待的喚醒目標。沒有執行主機重開機、改路由器或送單。最終 SHA 和服務讀回在 [部署驗收](../artifacts/operations/tw_opening_latency_20261013/deployment_acceptance.json)。

核對項目：

- 4 個 scheduled day-trade modes；Gateway `discord_connected=true`、core `ready`。
- 當沖 `ledger_integrity_ready=true`，Discord 和 engine 的 run identity／revision 對齊。
- 四模式剩餘持倉 0 股，經濟狀態投影沒有改變。
- `signals.jsonl`、`orders.jsonl`、`fills.jsonl` SHA 在重啟前後完全相同。
- 凌晨讀回當時 `session_date=2026-10-13`、latency sample count 0；頁面保留真實 `degraded`。盤前 readiness 留存的 10/12 receipt 不當作 10/13 驗收。這是盤前觀測，不是現在的樣本數。

保留的失敗：第一輪離線 benchmark 未同步 source-owner 測試時鐘；舊測試以凌晨 timestamp 假裝開盤正例；一次錯誤全域 stdlib wall-clock freeze 使 IPC timeout／檔案 mtime 不一致而停止**本輪自有測試 unit**。修正 scoped fixture 與 monotonic wait 後重新驗證，失敗 receipt/log 不刪除。這些不是生產成功證據，也沒有重寫當時的 source/ledger 紀錄。

## 10/13 真實開盤與盤中續修

### 有效開盤價究竟代表什麼

目前 canonical opening input 使用來源明示的 `open`，不是最後成交價、前日收盤、委買／委賣中點或把中點取整至 tick。共用驗證要求價格有限且大於零、標的有來源資料、行情時鐘屬於當日 09:00 之後，並排除來源明示的試撮。Shioaji 的本機收到時間不能代替其 `Snapshot.ts` 所代表的行情時鐘；未知試撮標記不等於已證明非試撮，因此這個 opening mask 是模型輸入契約，**不是成交證明**。

來源差異必須保留：

- Shioaji snapshot 的開盤輸入驗證沒有「成交量必須大於零」的獨立硬門檻；使用 `open`、行情時鐘及明示試撮標記。
- FinMind adapter 額外要求 `total_volume > 0`，且明示 `isTrial=true` 的列不接受。FinMind 日期仍須是當日開盤後，不能因數字非零就接受昨日列。
- 同日原始 MIS 明示 `volume=0`、沒有正開盤價且非明示試撮，才能形成「尚無首筆成交」證據；來源連不上、回應缺成交量或昨日快照只代表未知，不能推論股票沒有成交。
- `_shioaji_snapshot_values` 的一般 `price` 在缺最後成交時確有 Bid／Ask 中點備援；獨立的 `open_prices` 不會使用它。正常紙上進場另驗訊號後非試撮的可執行 Ask／Bid；不把中點或 opening input 描述成券商成交。

例如本輪 FinMind 單次讀回的 0057 有 `open=338.55`、`total_volume=4`，但來源日期是 **10/12 14:30**，因此仍不是 10/13 的有效開盤輸入。「尚未取得有效開盤價」不等於「股票沒有交易」。

### 今日實際阻塞

來源仍是既有永豐／官方 MIS／FinMind，四模式沒有切換模型。原始四個排程喚醒是 09:00:00.002711～09:00:00.005020；第一筆永豐有效開盤價於 09:00:01.131809 收到，FinMind 第一筆有效價於 09:00:15.703698 收到。這些是實際來源時鐘，不是離線喚醒 benchmark。

09:04 約有 1,750／2,303 個有效輸入；09:23 約有 1,986 個。**09:30:40** 的 union 是 **2,054／2,303**，仍低於既定 **2,073（90%）**，四模式當日訊號仍未發布。各模式 active universe 不同，當時個別缺口約 19～20 個；沒有用 response count 冒充有效價，也沒有降低 quorum。

MIS 在 WSL 與 Windows 的有限單筆讀取都得到 HTTP 000／empty reply；生產來源亦有 ConnectionError／ReadTimeout。這支持「官方來源目前未正常供價」，**不能僅憑這些探針判定是 IP 封鎖、路由器或防火牆哪一項根因，更不能宣稱已修好外部 MIS**。FinMind 一次合法 bulk read 回傳 2,878 列，含 2,101 列當日日期、714 列昨日日期；當日列數也不等於模型 universe 的有效覆蓋。

### 盤中新增的共用修正

| 元件 | 續修內容 | 驗證邊界 |
| --- | --- | --- |
| `opening_price_acquisition.py` | 對「有回應但沒有任何有效當日輸入」新增 provider-local 0.5→1 秒短退避，避免四模式反覆空轉；新有效輸入立即清除無進展計數。傳輸失敗仍為最高 15 秒，權限／429 為 60 秒，FinMind 既有刷新下限不變。 | 健康來源繼續；已有有效價格不清除；不新增全域上限。MIS metadata 中的 429 等 HTTP 錯誤現在也納入相同恢復規則。 |
| `quote_provider.py` | 原始 MIS「沒有首筆成交」判定在 telemetry／cache 使用同一函式；禁止把本次缺成交量欄位與前次 cached zero 拼成證據。 | stale、trial、broker rows、shared aggregate 不能冒充原始 MIS 缺成交證據。 |
| `signal_engine.py`、Discord | canonical quorum 尚未準備完成時，在取價結果處先回到 waiting，保留 coverage／source／時鐘／耗時，避免先進模型 mutex 再重做必然失敗的準備。 | ready 分支仍做原有完整驗證；模型、權重、資金與成交規則未改。 |
| Discord setup／latency | 啟動時讀一次 append-only attempts，恢復當日原始 first scheduler observation；重啟不能把排程喚醒誤算成 17 分鐘。實際本次 attempt 時鐘不改。 | malformed、錯日期、未帶時區與未來時鐘不採用；無歷史證據時不製造 09:00 時鐘。 |
| Discord warmup | 既有開盤保護窗結束後，若排程確實在 retry 且沒有 active opening attempt，允許冷 owner 做既有 no-quote/no-publication warmup。 | 不在 09:00 關鍵窗搶模型、不取新行情、不發布額外訊號，仍使用既有模型 mutex。 |

背景來源修復本來已使用 shared fan-in，本輪沒有另建平行補價 pipeline。

### 新增測速與回歸

完整 2,303 標的的無進展來源比較為 **離線合成 stale responses**，3 組平衡順序，每組 12 次 observer 呼叫，全部 financial/source arrays SHA 相同：

- 每組 provider 呼叫 **12→2**、request rows **27,636→4,606**，減少 83.33% 重複查詢。
- 12 次觀測整段中位數 **2,006.657→1,530.619 ms**，減少 **23.72%**；不是單次 quote latency，也不是真實開盤或券商成交 SLA。
- 每個原始樣本與時鐘邊界保留在 [no_progress_comparison_final.json](../artifacts/operations/tw_opening_latency_20261013/no_progress_comparison_final.json)。沒有凍結全域 wall／monotonic clock，沒有 network request 或 Shioaji login。

最終 **502 passed in 13.04s**，run `opening-recovery-clock-regressio-20261013T012231-c73b9091`，覆蓋 source fan-in、source repair、quote provider、pending-quorum model-lock exclusion、Discord restart/warmup、完整開盤／當沖／回放契約與 benchmark guard。先前 `opening-full-source-semantic-gat-20261013T011258-67f405bf` 的 436 passed／1 failed 保留：舊 fixture 要求無進展來源立即再次呼叫，已改為驗證 cooldown 與期限後恢復，後續 502 個回歸覆蓋該情境。

### 盤中部署與獨立讀回

盤中只 targeted restart Discord bot，**沒有重啟模擬執行引擎、改寫任何帳本或送出券商委託**。第一次部署後發現 restart clock／warmup recovery 缺陷，修正並測試後做第二次部署；沒有把第一次不完整 recovery 當成最終驗收。

最終 Discord invocation：`2fb5d611495342e9a457237d09f36efa`，owner PID 955833，Python run `955871-1791854628111519597`。09:25:47 獨立讀回為 Gateway connected、core ready，四個模型 warmup **4／4 ready，15.112 秒**。這是新 bot owner 加保留資料快取的實測，不是主機完整冷開機，也不是與不同情境 66 秒數字的公平比較。一次 22.94 MB attempts clock recovery 約 79～85 ms，位於 setup，不增加每次開盤熱路徑成本。

執行引擎 invocation 仍為 `34227ee9d55a401f9471c032d43bc9e9`，PID 208392，engine run `87de6ae919854689a548d1fc6168cd7a`；ledger integrity ready、divergence 0，但 overall health 仍是 waiting，不能因服務 active 就宣稱當日訊號已完成。

盤中最終讀回 run `opening-live-runtime-readback-20261013T012543-a9cfd5b0` 成功，三份 journal SHA 及經濟投影與部署前相同：

| 證據 | SHA-256 |
| --- | --- |
| signals | `6f3ebcdc541a27051be746edb538b30a562c05f6888f428a54927a843a88173a` |
| orders | `99ba713a8c514d345ff160bfe0ca8bf3dbdad25663af6bbf3fe3d42f94a13ce5` |
| fills | `7f5d41cda1782890e8a5add248c02d979669be17dcfce652071c1d59b52c8a4a` |
| 經濟投影 | `9008cc6f905ecd7e352337143eb969e99cf9a28d754aa8b4db4bbefc0c55c8aa` |

部署前後狀態與真實 attempts 保留在 [daytime_runtime_before_final.json](../artifacts/operations/tw_opening_latency_20261013/daytime_runtime_before_final.json) 與 [daytime_runtime_after.json](../artifacts/operations/tw_opening_latency_20261013/daytime_runtime_after.json)。來源工作會沿既有 scheduler 自動繼續重試；外部 MIS 供價恢復、今日 quorum、四模式發布及紙上執行必須各自讀回驗收，尚不能列為完成。

## 持續真實開盤驗收與不能承諾的部分

現在的價源仍是使用者選定的永豐／MIS／FinMind；沒有擅自增加其他券商登入或訂閱購買。來源健康可能在 09:00 改變；未成交、停牌、試撮與未取得資料必須維持不同狀態。不能把無法得知的價格填成零、前日收盤或假開盤價，只為通過 1 秒目標。

每天現有服務會繼續記錄：

1. `artifacts/live/tw_opening_snapshots/YYYY-MM-DD.acquisition.jsonl`：各來源啟動、首批／首有效價、有效覆蓋、失敗／重試、quorum 及落盤狀態。
2. `artifacts/live/tw_day_trade_simulation/opening_signal_latency.jsonl`：排程喚醒、獨立 input、model lock queue、準備／推論／格式化、pointer 發布。
3. `artifacts/live/tw_day_trade_simulation/latency.jsonl`：第一次 pointer 發現、來源補價等待、執行取價、eligibility、帳本計算／持久化及開盤→帳本完成總時鐘。

今日後續與下一次 09:00 都要以這三層實測證明改善，不能用本機事件 benchmark 或 `/healthz` 代替。

永豐 [Snapshot 官方文件](https://sinotrade.github.io/tutor/market_data/snapshot/) 是請求式報價，單次最大 500 商品；[使用限制](https://sinotrade.github.io/zh/tutor/limit/) 對訂閱／請求另有邊界。不能未經權限或驗證把全市場 source SLA 變成 1 毫秒。開盤交易本身也有各股撮合／延緩狀態，[TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW) 不保證每檔 09:00 同時出現首筆成交。

本輪更快的本機部分不等於「外部資料全面正常」「全部成交」「零故障」「達到理論全局最優」。若再發生來源等待，新的時鐘會如實顯示來源、等待原因及總成本，而不是把它誤算成模型時間或消除告警。

## 重測指令

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_tw_opening_price_fan_in.py test/test_discord_intraday_latency.py \
  test/test_quote_provider.py test/test_tw_opening_source_recovery.py \
  test/test_tw_day_trade_simulation.py \
  test/test_live_signal_helpers.py test/test_opening_signal_pipeline.py \
  test/test_opening_signal_benchmark.py test/test_day_trade_intraday_discipline.py \
  test/test_opening_source_benchmark.py test/test_discord_bot_helpers.py

run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-13.json \
  --compare-no-progress --rounds 3 \
  --output artifacts/operations/tw_opening_latency_20261013/retest_retry_comparison.json

run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-13 --runtime \
  --output artifacts/operations/tw_opening_latency_20261013/retest_runtime.json

run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-12.json \
  --feature-date 2026-10-08 --repeats 10 --defer-reports --simulate-entry \
  --compare-publication \
  --output artifacts/operations/tw_opening_latency_20261013/retest_publication.json

run_fintech_python scripts/benchmark_tw_opening_quote_wakeup.py \
  --repeats 500 \
  --output artifacts/operations/tw_opening_latency_20261013/retest_quote_wakeup.json
```

## 10/13 日間續作：官方交易名冊取代模型 alive-mask 分母

已修正官方名冊、分列停復牌、現行停止買賣與 canonical 減資停牌的成分契約。
目前 2,311 掛牌 − 8 停止交易 = 2,303 可交易，與舊總數碰巧相同，
但移除 30／加入 30 個標的；當日不可變訊號與模擬 execution engine 保持不重算／不重啟。
最新證據、測速、來源時鐘與尚未解決邊界見
[官方交易名冊修正與驗收](tw_official_trading_universe_2026-10-13.md)。

## 10/13 午間：全市場低延遲取價方案與實作

### 先界定可保證的事情

全市場分母沿用當日官方 regular stock／ETF 名冊與停復牌證據，不固定寫死 2,303，
也不由模型歷史 alive-mask 或當沖資格反推。**交易名冊、收到行情、正式開盤價、
訊號後可執行 Bid／Ask、紙上記帳、券商成交，是不同驗收。**

正式開盤價必須先有首筆成交。暫緩開盤、無成交或單邊無對手報價時，
程式不能製造 09:00 價格；只能留下交易狀態／缺口證據。
[TWSE 交易制度](https://www.twse.com.tw/zh/products/system/trading.html?hl=zh-TW)
包含暫緩開盤機制，因此「09:00 整市場都有正式開盤成交價」不是可保證的工程 SLO。

永豐目前[官方限制](https://sinotrade.github.io/zh/tutor/limit/)為 200 個行情訂閱、
行情查詢合計 50 次／10 秒、同 person_id 最多 5 連線；
[Snapshot](https://sinotrade.github.io/zh/tutor/market_data/snapshot/) 每請求最多 500 商品，
不是全市場即時推播。不能靠額外登入、輪替訂閱或持續加快 Snapshot 輪詢，
宣稱取得全市場低延遲串流權限。授權不夠時，MIS／FinMind 可提供獨立補充證據，
但本輪沒有來源證明能在真正 09:00 穩定供應全市場次秒價。

### 選定方案

| 階段 | 共用路徑與動作 | 驗收與邊界 |
| --- | --- | --- |
| 盤前資料準備 | 現有官方名冊、當日參考價／漲跌停、資格及模型準備，各自保留 fingerprint／receipt | 取價來源名冊不受下游當沖權限閘門綁住；權限仍在執行層檢查 |
| 08:55～08:59:30 | 同一永豐 client 驗證登入並預先接通原生 Quote 訂閱；缺合約不阻擋已驗證的 client | 完整官方來源 axis；訂閱上限仍 200，`armed` 不代表 2,303 檔已有價格 |
| 08:59:20 起 | 凍結當前訂閱 target、完成剩餘 25-name reconciliation；準備中的 loop 暫時用 10 Hz | 08:59:30～09:00:15 不發新的全市場 rotation／usage probe；既有持倉與待執行訂單優先 |
| 09:00 初始 fan-in | 永豐／MIS／FinMind 全來源 axis 獨立並行；已有合格 native open 立即 progress；Snapshot 只查未覆蓋名字 | 不等慢來源完成；Snapshot 可用額度內各 batch 非阻塞提交；每日模型仍只推論一次 |
| 入模 | 沿用原先 90% coverage、同日來源與 missingness ABI；接受的輸入由 canonical receipt 持久化 | 不降低 quorum；不把昨日價、試撮或 Bid／Ask 中點換成 `open` |
| 缺口與恢復 | 成功事實保留，失敗來源局部退避／重試；無執行需求時於 09:00:15～09:10 繼續原生 target rotation | 不將未知誤作「未成交」；不另開登入或平行帳本；故障不取消健康來源 |
| 長期嚴格目標 | 合法授權的全市場成交＋Bid／Ask＋交易狀態串流，盤前連線與訂閱完成；現有三路作交叉驗證／恢復 | 須另確認授權、完整名冊覆蓋、來源時鐘、sequence／重連補送及真實 09:00 P95/P99；尚未購買或接入新供應商 |

目前實作仍使用使用者指定的三路；全市場授權串流是需求推導出的後續方案，
不是本機已具備的服務。即使有全市場串流，價格驗收也只能從該股實際首筆成交／
合法書簿產生後計算，不能把來源尚未產生的時間算成程式優化可消除的延遲。

### 已實作的共用修正

- `quote_provider.py`：開盤用途明確使用 `proven_native_open_then_snapshot_gap_v1`。
  Native 只接受非試撮、非暫停、正開盤價、正累積成交量、同日 09:00 後的交易所及
  本機接收時鐘，保留原始 timestamp；時鐘超過接收時間 1 秒以上不採用此 native proof。
  Snapshot 原有驗收 ABI 沒有額外加成交量 gate。正常執行的 book／成交規則不變。
- native callback 只存觀察值／喚醒，無模型、檔案或委託 I/O。已有同一正式 `open`
  的後續成交 tick 不再重複喚醒全市場 source worker；新增或改變的 opening proof 才喚醒。
  worker 會保留 request 內首次成功 native observation，後來的試撮／缺欄位不會清掉它。
  最終 Snapshot DTO 保留已知合約參考價與法定上下界，未知的界線不製造。
- Snapshot 與同一 client 的歷史讀取共用實際 provider window；歷史保留既有 20% 額度預留。
  第一輪冷取價的 5 batch 在空 window 內仍可立即全部發出，不加均勻 0.2 秒排隊。
  增加 `snapshot_query_admission_ms`。**此 local window 不證明同帳號其他程序的總量安全**；
  其他 downloader 的 host-global 排程仍須一併協調。這不是新增網站／全域併發限制。
- runner 的取價盤前 warmup 改用完整官方名冊，不再依賴執行資格；
  `authenticated_session_ready` 與完整 contract readiness 分開。
  readiness receipt 分列官方 fingerprint、訂閱數、已收到新鮮 quote 數及全市場覆蓋是否成立。
  已移除不再被引用的資格篩選取價 helper；實際執行層的資格驗收沒有刪除。
  缺合約的 usage-probe 重試只在盤前進行；已證明 client 的盤中／盤後執行迴圈
  不會每五秒重做登入探針。冷啟動恢復仍可在保護窗外建立第一份連線證據。
- fan-in 每個請求有獨立 `job_id`，全市場 batch 的 requested/response/valid 數量不再被
  同來源 one-symbol delta 覆寫。provider aggregate 使用 unique symbol union，
  並另留 `last_response_count`／`last_valid_open_count`，不混用累積與最新批次。
- 每來源保存 `observations_delta`：逐檔區分有效開盤、明示尚無首筆成交、缺開盤價、
  舊／盤前時鐘、試撮、缺市場時鐘及未收到來源；unknown trial flag 留作 unknown。
  這是 decoded observation，不冒充原始 vendor bytes。來源 JSON 序列化移出共用 append lock，
  小型 quorum receipt 不需等待另一來源的大量 Python JSON 編碼。

### 測速、正確性與真實邊界

最後完整 2,303 名稱、5 組平衡順序、空 canonical receipt 的合成比較：
native fixture 185 個正式價、MIS fixture 1,888 個互不重疊正式價、SDK callbacks 人為延後
100 ms、第三來源故障。**價格是合成資料，不是真實開盤、網路 SLA 或券商成交。**

| 邊界 | 控制組 | Native 優先 | 不能推論的內容 |
| --- | --- | --- | --- |
| 永豐來源第一個可用輸入，中位數 | 123.337 ms | 14.532 ms | Native fixture 在開始前已產生，不包含交易所到 feed 的傳播 |
| 既有 90% quorum 返回，中位數 | 240.803 ms | 123.736 ms | 約改善 48.61%；不是 100% 行情或全模式下單／成交完成 |
| 全部來源完成與驗收，中位數 | 388.721 ms | 344.980 ms | 每組都 drain sources，不省略後續來源完成成本 |
| Snapshot request rows | 2,303 | 2,118 | 只省去已證明的 185 native 名稱；仍未宣稱全市場串流 |
| 最終正式開盤價 | 2,303／2,303 | 2,303／2,303 | 最終價格 SHA 完全相同；不宣稱決策瞬間 missing-mask／來源時鐘／訊號完全一致 |

全部原始樣本與逐來源 receipts：
[stream_priority_acceptance.json](../artifacts/operations/tw_opening_quote_plan_20261013/stream_priority_acceptance.json)、
其同名 `.sources/` 目錄。先前兩組比較未刪除；單組負載差異與尾部較慢樣本仍保留。
只有 5 個樣本／variant，不能當作正式 P99 或全局理論最優。

真實網路探針皆由空測試 receipt 起步，借用仍在運行的舊 engine broker，不新登入：

- 11:53 基線：quorum 742.118 ms，最終 2,237 正開盤價＋18 個官方無首筆成交證據。
- 12:20 讀回：quorum 795.347 ms，最終 2,254 正開盤價、49 個仍未驗收；
  MIS 當輪全部失敗，永豐與 FinMind 繼續成功。沒有把較慢的 795.347 ms 報成加速。
- 這些是不同盤中來源狀態，不是公平真實 09:00 控制／候選比較；
  新版 native engine 尚未載入，不能拿它們宣稱盤前串流部署已驗收。

對應[基線](../artifacts/operations/tw_opening_quote_plan_20261013/source_before.json)、
[實際來源讀回](../artifacts/operations/tw_opening_quote_plan_20261013/source_after.json)。

相關最終回歸 **561 passed in 20.82s**，run `opening-quote-final-gate-20261013T043209-0f9c0ff7`。
先前 554 passed 的 run `opening-stream-acceptance-20261013T042351-9963edad` 亦保留。
涵蓋 native 與 SDK 競賽、後續 trial 不抹去成功、原始時鐘、預備窗／凍結窗、200-topic capacity、
完整官方 scope、缺合約不阻擋 healthy login、provider 50／10s shared window、來源故障隔離、
並行大批／delta job metrics、逐檔品質、共用恢復及訊號／fees／paper ledger 契約。
沒有宣稱全庫測試已執行。失敗的 fixture clock、benchmark evidence 路徑，以及新增停用模式造成的
舊四模式固定測試假設皆保留原始 logs；修正 tests 明確驗證 canonical enabled／all-configured 集合，
沒有關閉 production feature-source gate。

### 部署狀態與下一次驗收

**程式與回歸完成，尚未重啟盤中 engine；全市場次秒目標尚未達成。**
12:24 獨立讀回 engine PID 208392、invocation `34227ee9d55a401f9471c032d43bc9e9`、
engine run `87de6ae919854689a548d1fc6168cd7a`，與本輪之前相同；
四模式仍有 786／27／1／12 個持倉，因此本輪沒有為部署而中斷執行。
12:25 ledger integrity ready、divergence 0。這只證明當次帳本一致性，不證明來源全面正常。
[獨立 runtime 讀回](../artifacts/operations/tw_opening_quote_plan_20261013/runtime_readback.json)
另保存 Gateway、各 mode 原始訊號與目前 engine run identity，不把已存在的 09:01 回放說成正常開盤。
新 engine 須在安全收盤窗口更新並讀回已載入版本；此後再以真正 09:00 receipt 驗收。

工程 task `tw-opening-quote-evidence-stream-20261013` 保留 active，下一步為安全部署＋
真實開盤 coverage／各段延遲驗收。未更動模型、quorum、歷史帳本或券商委託，
也未購買任何新 feed／連線權限。

重測：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-13.json \
  --compare-stream-priority --rounds 5 \
  --output artifacts/operations/tw_opening_quote_plan_20261013/retest_stream_priority.json
run_fintech_python -m pytest -q -s \
  test/test_tw_opening_stream_acquisition.py test/test_tw_opening_price_fan_in.py \
  test/test_shioaji_stock_quote_provider.py test/test_quote_provider.py \
  test/test_tw_day_trade_recovery_responsiveness.py test/test_opening_source_benchmark.py
```

## 10/13 午後：MIS 全部失敗的分層診斷與修正

### 原因與證據

12:20 的全市場 MIS 工作不是「收到行情但開盤價不合格」：29 個 HTTP 批次
全數 `ReadTimeout`，0 個 response rows。原開盤設定同時使用 **80 channels／batch**
與 **1.5 秒 request timeout**，與來源的實際回應成本不一致。

12:39 使用同一官方 2,303-symbol scope 做有限單次探測；不登入券商、不發布訊號：

| channels | HTTP / rows | 單次 wall time |
| --- | --- | --- |
| 1 | 200 / 1 | 63.500 ms |
| 20 | 200 / 20 | 584.876 ms |
| 40 | 200 / 40 | 1,243.619 ms |
| 80 | 200 / 80 | 2,692.566 ms |

run `mis-batch-axis-diagnostic-20261013T043942-86548e7d` 保留原始 stdout。
這不是公平 P95/P99 或真正 09:00 實驗，但直接證明「單檔可達」不代表
80 檔能在 1.5 秒完成。另一有限 stock-only 控制中，20 檔先花 2,361.010 ms，
同樣 20 檔稍後只花 85.158 ms，80 檔超過 4 秒；上游回應亦有波動，不能只縮短 timeout。

短預算候選（20 channels、1.5 秒、16 workers）完整請求同一 2,303 標的，
讀回 **1,698 個來源觀察＝1,673 個正開盤價＋25 個官方無首筆成交證據**；
147 次實際 HTTP request（含只查缺口的重試）後段出現 502 與 `RemoteDisconnected`。
因此沒有採用「用更多小請求就保證全市場成功」的結論。
[候選完整 receipt](../artifacts/operations/tw_mis_source_recovery_20261013/short_budget_readback.json)
與同名 `.round1.receipt.json`、`.acquisition.jsonl` 保留成功、缺口及失敗。

之後 curl 和 requests 的單檔請求也無 HTTP 回應。DNS／TCP／TLS 可以完成，
exception chain 為 `ConnectionError → ProtocolError → RemoteDisconnected`。
這是對端在 HTTP headers 前關閉連線的證據，**尚不能判定是 IP 封鎖、WAF、
服務過載或特定後端故障**；沒有關閉 TLS 驗證、輪換代理或繞過來源限制。
12:50 的 [最終 transport probe](../artifacts/operations/tw_mis_source_recovery_20261013/final_transport_readback.json)
忠實是 `waiting_source / data_complete=false`，且不在失敗單檔 probe 後再送全市場 burst。

### 已實作的共用修正

- canonical MIS HTTP collector 依 timeout 選批次：短預算最多 20 channels，
  更短預算還會縮小；一般較長預算仍最多 80。完整 symbol scope 不減少。
- 開盤 fan-in 的 MIS read budget 預設 **4 秒／80 channels**，避免把全市場
  HTTP request 數由 29 倍增到 116。這是來源 worker 的預算，**不是四秒模型固定等待**；
  永豐／FinMind 的已驗證輸入仍可先達成原 90% quorum，慢來源獨立繼續。
- 部分回應也補缺檔，不再只重試「完全空」的批次；重試新增 result slots，
  不覆蓋已取得行或把舊行的 receipt clock 更新成重試時間。
- HTTP 401／402／403／429 或連續四次對端關閉／502／503 等 endpoint failures
  停止派送該輪尚未開始的 MIS 批次，收回並保留已經在跑的成功回應。
  剩餘標的維持未解，由既有 shared source owner 接續，無全域網站 rate limit。
- 即使有部分成功，來源已拒絕該輪派送時仍為 `degraded`，不清掉失敗狀態；
  endpoint quiet interval 為 15 秒，權限／429 仍 60 秒。成功事實、其他 provider
  與 consumer 不被取消，未知不冒充 no-first-trade。
- 每個實際 HTTP request 留 channels／rows、headers／total ms、HTTP status、
  安全 exception class chain、有效 batch size、timeout、停止與未派送數量。
  不記 Cookie、token、exception text；非成功 `rtcode` 不採為行情。
- 原測速入口新增 `--mis-only`，沿既有 collector／fan-in 保存完整 evidence；
  single-symbol prewarm 失敗時輸出 `waiting_source`，不發全市場 burst。

### 驗收與部署邊界

最終 **575 passed in 26.95s**，run
`mis-final-contract-regression-20261013T045209-7c470c3f`，包括新 timeout/batch、
partial-only retry、原始時鐘、破損 worker pool、拒絕停止、部分成功後 quiet interval，
以及既有 opening／Shioaji／模型訊號／回放／帳本／fees／Discord／supervisor 回歸。
先前 fixture 對 thread-local session 的錯誤數量假設和診斷欄位 `KeyError` 的原 logs
仍保留，後續實測與最終回歸已覆蓋；沒有宣稱全 repository 測試通過。

12:53:47 只 targeted restart `stockagent-discord-bot.service`；重啟前互動工作
active／queued／running 都為 0，四個 scheduled markets 未變。12:55:23 獨立讀回：

- 新 Discord invocation `f5578b64ebec4ccf8fb569e98e70f032`、run
  `1328352-1791867234850993917`，Gateway connected、core ready。
- engine invocation `34227ee9d55a401f9471c032d43bc9e9`、PID 208392、run
  `87de6ae919854689a548d1fc6168cd7a` 與之前相同，未重啟執行引擎。
- 四個原始 signal IDs 未變，ledger integrity ready、divergence 0；沒有手改帳本
  或券商下單。當時仍有 827 個持倉，engine 內版本留待安全清倉窗口更新。
- 12:55 生產 MIS `finished` 已實際讀回 `mis_request_timeout_seconds=4.0`、
  `deadline_compatible_channels_v1` 和 nested error classes，證明新訊號取價 owner
  已載入修正；該請求仍由對端關閉。不是只用 process active 宣稱新功能已生效。

[部署前](../artifacts/operations/tw_mis_source_recovery_20261013/runtime_before.json)、
[部署後](../artifacts/operations/tw_mis_source_recovery_20261013/runtime_after.json)
分開保存。Gateway-ready 不證明 inference warmup 已完成，也不證明 MIS 或所有資料健康。
task `tw-mis-source-recovery-20261013` 保留 active：尚需外部 MIS 供價恢復後的
完整官方 scope 讀回、清倉後 engine 載入，以及真正 09:00 的來源／quorum／執行驗收。

證交所的[官方 MIS 介紹](https://www.twse.com.tw/staticFiles/product/publication/twse60/P8.pdf)
描述五秒行情快照；其 HTTP transport 修好也不等於合法全市場逐筆串流或一秒成交 SLA。

低請求量重測（單檔 probe 失敗會停止，不會為了測速強行重送）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_tw_opening_sources.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-13.json \
  --mis-only --mis-chunk-size 80 --mis-request-timeout 4 --mis-parallel-requests 4 \
  --rounds 1 --output artifacts/operations/tw_mis_source_recovery_20261013/retest.json
```
