# 當沖開盤延遲：2026-10-05 根因與修正

本次範圍是 09:00 → 開盤行情 → 四模式訊號 → 模擬市價進場帳本。
毫秒是測量單位與優化方向，不是已達成的交易所端到端承諾。
系統仍是紙上模擬；Shioaji 提供行情，沒有把紙上成交宣稱為券商回報。

## 今天真正慢在哪裡

來源為 `artifacts/live/shioaji_traffic/daily/2026-10-05.jsonl`、
Discord 開盤測速紀錄、模擬引擎測速紀錄及當日 `latest_signal.json`。
不重寫今天的測速、不回填假的 callback、不改動成交帳本。

| 區段 | 當日實測 | 解讀 |
| --- | ---: | --- |
| 排程醒來 | 2.230–8.533 ms | 不是主要瓶頸 |
| 第一模式資料準備／等待 | 約 636 ms | 開盤後仍有檢查工作 |
| 引擎取價前 `api.usage()` | 5,001.348 ms | 失敗／未取得用量，仍阻塞行情 |
| 2,306 檔、五批原生快照 | 1,000.626 ms | 請求與 callback 整理，不是純交易所 RTT |
| 引擎取價後 `api.usage()` | 3,658.599 ms | 不是產生價格或訊號的必要依賴 |
| 模型推論 | 58.749–140.326 ms／模式 | 與取價阻塞分開 |
| 原子指標發布後至帳本落盤 | 約數十毫秒到數分鐘 | 包含執行端排隊、取價、等待與回補，不是單純檔案發現 |

引擎早盤流量事件的 PID 為 270，五批全部完成、沒有 Snapshot batch timeout；
兩次同步流量查詢合計 **8,659.947 ms**。
共享行情回應直到 09:00:10.715 才送出，超過訊號端 4 秒等待界線，
因此走 MIS／另一程序 Shioaji 備援。不能把顯示的整段取價等待都怪給上游行情。

| 模式 | 訊號 ready | 原子發布 | 實際帳本落盤 |
| --- | --- | --- | --- |
| V8 年度 Log Cash | 09:00:13.649877 | 09:00:13.684604 | 09:00:14.327299 |
| 多基底 22 | 09:00:14.018650 | 09:00:14.040418 | 09:00:16.032017 |
| Multi-Basis | 09:00:14.364883 | 09:00:14.393355 | 09:00:15.267483 |
| 一億元 | 09:00:14.870606 | 09:00:15.010614 | 09:04:52.643220 |

一億元模式的原子發布晚於既有 09:00:15 執行界線，觸發 09:01 價格重建，
不是行情通知漏了四分多鐘。回補的模擬時間不等於實際完成時間。
不延長截止時間來掩蓋失敗；也不把回補改寫成即時成交。

## 修正的依賴與運算

1. `shioaji_query(..., observe_usage=False)`：即時股票／期貨 Snapshot
   不再同步查剩餘流量。歷史 Ticks／KBars 等既有呼叫預設維持原本用量觀測。
   每次仍持久記錄請求數、列數、失敗與耗時；未測得位元組量為 `null`，不是零或免費。
   帳戶配額仍以獨立觀測的真實 `api.usage()` 收據為準。
2. 原生數字解析：float／int 不再先轉字串、去逗號、再轉浮點。
   bool、Decimal、numpy scalar、字串及特殊 wrapper 保留既有轉換路徑；
   NaN、inf、零及負價格仍拒絕。不改價格精度、張／股、成交量、漲跌停或手續費。
3. YAML bool 契約：每個 Python schema class 只解析一次靜態型別定義，
   但每次載入都重新驗證當次 YAML 值；不快取合法／非法判斷，不略過檢查。
4. 逐段測速：股票連線鎖、合約／批次準備、請求送出、首／末批 callback、
   回傳等待、解析、向量組裝、合法價格界線、流量紀錄、共享行情排隊與回應編碼。
   本機 callback receipt 與交易所時間仍分開。這些是巢狀分段，不能全部相加。
5. 面板保留 8 張摘要卡，新增「09:00 → 全模式帳本落盤」：
   只有完整觀測所有模式才顯示全部完成時間；舊紀錄缺欄位顯示未知。
   `artifact_discovery_ms` 改為「發布至註冊（含等待／回補）」。
   首個與最後訊號改用毫秒顯示，不再把 13.650 秒四捨五入成 14 秒。
6. 09:00:15 的驗收門檻改讀同日、同模式、同 `signal_id` 的成功
   `latency.jsonl` 落盤紀錄，不再拿秒級／回補的 `entry_completed_at`
   冒充實際完成時間。缺紀錄是未知／未達標，不是零延遲；15,001 ms 不會
   截成 15 秒而通過。重用面板的 bounded tail reader，盤前不多讀歷史或查 API。

保留同一條已登入共享行情連線、500 檔原生批次、原模型、完整標的輸出、
因果價格、資格、資金及帳本限制。沒有為測速另建登入或減少候選標的。
[官方 Snapshot 文件](https://sinotrade.github.io/tutor/market_data/snapshot/)
說明請求批次及流量成本；
[非阻塞呼叫文件](https://sinotrade.github.io/tutor/advanced/nonblock/)
不等於交易所接受或完成成交的證據。

## 可重測的對照與邊界

當日完整開盤收據 SHA-256：
`538c10747470d173a369e8851380cf3e4d35ad30d2205653818a65a6e395f664`。

`scripts/benchmark_opening_quote_path.py` 使用所有 2,306 個標的、空價格快取，
正反交替測三個候選；真實共用解析、價格界線與持久流量紀錄仍執行。
Fake SDK 使用固定收據，流量查詢每次注入 25 ms，**沒有真實網路延遲**。

| 離線取價候選，10 次／候選 | 中位數 |
| --- | ---: |
| 舊解析＋同步流量觀測 | 165.067 ms |
| 舊解析＋不查流量 | 114.578 ms |
| 新解析＋不查流量 | 47.725 ms |

只比較解析修正時，完整本機路徑減少約 58.35%；三候選價格／數量／界線
摘要相同，30 個流量事件全部寫出。不是開盤總延遲從 17 秒變成 47 ms。
實際 8.66 秒阻塞來自今天原始流量紀錄，不是上述注入的 25 ms。

### 真實四模型與完整產物

最後同一程序交替對照 8 次／候選，16 個樣本的 panel／checkpoint／model／
alignment 全部命中；四模型完整權重、決策與模型解釋 digest 均相同。
完整報告、Parquet 和完成指標都驗證，分開記錄「執行指標發布」與「完整產物完成」。

| 靜態 bool schema 對照 | 四模式執行指標發布中位數 | 完整產物完成中位數 |
| --- | ---: | ---: |
| 每次重新解析 schema | 768.556 ms | 1,137.712 ms |
| 重用靜態 schema，逐次驗證值 | 730.675 ms | 1,109.841 ms |

同次對照發布區段約減少 4.93%，不等於整段開盤固定改善 4.93%。
收據是 `artifacts/benchmarks/opening-model-validation-final-paired-20261005.json`。
較早對照中的一個 24.744 秒樣本重建 panel，原始資料沒有刪除，不能拿它
直接對比暖樣本。新的測速摘要明列冷／暖樣本數與兩個完成邊界。
不同時段 CPU／GPU 與其他資料工作負載不同，不把跨時段差異歸功本輪修正。

C-order 預熱候選也做四模型對照且輸出同值，但原始發布中位數
2,491.129 ms，對照 K-order 為 2,260.213 ms，沒有證明更快。
正式推論的該候選已撤回，僅保留隔離測速選項與原始對照收據。

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/benchmark_opening_quote_path.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-05.json \
  --repeats 10 --usage-delay-ms 25 --compare-scalar \
  --output artifacts/benchmarks/opening-quote-retest.json

run_fintech_python scripts/benchmark_opening_signal_pipeline.py \
  --receipt artifacts/live/tw_opening_snapshots/2026-10-05.json \
  --feature-date 2026-10-02 --repeats 8 --defer-reports --compare-validation \
  --output artifacts/benchmarks/opening-model-retest.json
```

GPU 實際模型測速前仍需 strict CUDA preflight。模型測速不查即時行情、
不發 Discord、不下單；訊號及 panel cache 都寫入隔離暫存目錄。
完整報告、Parquet 與原子完成指標照常驗證，不能只測 inference 就稱流程加速。
`--profile` 的 profiler 會明顯增加 CPU 耗時；只能找熱點，不能直接作速度驗收。

## 本輪驗收與待驗證

- 受影響 Python 整合測試 513 項通過；公開 gateway 另 122 項通過、
  2 項未選入；Node 54 項通過。最後測速摘要、實際 commit gate 與公開資料
  排程回歸 88 項通過，
  不把重複執行的測試累加成全庫測試數。
- 14:46 盤後重載當沖引擎、Discord 與唯讀 gateway，三者 active/running、
  `NRestarts=0`。Discord 連線恢復並完成 22 個指令同步，runtime revision lag 為 0。
- 14:49:29 透過引擎同一條已登入連線做真實 Snapshot 驗收：
  全部 2,306／2,306 檔可用，呼叫總計 **725.533 ms**，provider 703.629 ms，
  首批 callback 150.419 ms、末批 654.705 ms、解析 14.318 ms、
  流量紀錄 10.189 ms，兩次 usage 觀測皆未執行。
  巢狀 spans 不可相加；這是**盤後取價**，不是開盤總延遲或券商成交。
  收據：`artifacts/benchmarks/opening-live-shared-quote-20261005.json`。
- 重載後實際 localhost 頁面在 1366×768、390×844 完成渲染，四模式測速均存在，
  沒有水平溢出與 page error；瀏覽器明確選 cpu-2d，不是 GPU／WebGL 驗收。
  顯示今天訊號全數 ready 14,870.606 ms、實際全模式落盤 292,643.220 ms，
  不把 09:01 回補時間當成 60,000 ms 實際完成。
- 公網 IPv4 HTTPS 回應成功，網站與 Discord 同步；沒有宣稱 IPv6、所有上游
  資料或所有服務已修好。今日 `health=degraded` 及開盤 SLO 失敗保持真實。
- 重載前後 `orders.jsonl`／`fills.jsonl` SHA-256 相同、ledger divergence 為 0，
  四模式均已平倉；沒有重寫訊號、價格、張股數、委託或成交。
  orders：`696813978b949b8662e9b3d07829d12d058dbadcc1b955cf2455da4126b59ed1`；
  fills：`d313eaf36d4f345242cc0827ee8fb60aa192f9d4427a41426d796bed23e98b2c`。
- 新 gate 使用現有原始收據驗證：V8 實際落盤 14,327.299 ms 通過 15 秒門檻；
  Multi-Basis 15,267.483 ms、多基底 22 16,032.017 ms、一億元 292,643.220 ms
  都未達標。與面板一致，不再沿用舊 gate 的整秒／模擬進場時間判定。
  本輪獨立驗收不覆寫今天既有 gate 紀錄；排程下次執行會載入新邏輯。
- 擴大公開面板測試曾出現 FinLab 的 browser lazy-pipeline 等待逾時，
  單獨重測仍失敗；沒有把此未修正問題略過後宣稱全庫通過。
- 冷模型診斷曾因公司行動 reference 與 entitlement receipt 身分不同正確拒絕。
  既有官方來源維護在 14:24:56 發布新 entitlement，來源驗收之後已可通過。
  本輪沒有替換公司行動原始資料或略過驗證。整體 publication sweep 後續仍失敗，
  這個單一來源驗收不能宣稱全資料健康／明日資料已備齊。

重載 invocation：engine `a809f258b9634d1386c691768d8bbc58`、
Discord `f997df148adc49f29fcdc07ffcfad747`、
gateway `cc415d0a978847b1a863e88e96b93970`。
部署後驗收與剩餘邊界另存 `artifacts/benchmarks/opening-latency-acceptance-20261005.json`。

下一個實際開盤才能驗證行情來源、四模式訊號、模擬進場與落盤的新總時間。
若行情尚未到達或標的尚未實際開盤，不能用盤前試撮、昨天價、捏造到達時間
來達成毫秒 KPI。優化後仍依實測繼續定位模型、格式化與帳本成本。
盤後實際取價 725.5 ms、離線暖四模型發布 730.7 ms 不可直接相加當成
下一次開盤預測；即使個別環節低於一秒，也沒有證明完整開盤到進場低於一秒。
